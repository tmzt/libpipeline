//! Composing stages into a graph.

use std::sync::{Arc, Mutex, PoisonError};
use std::task::Context;

use libpipelinedata::{EffectPoll, MemoKey, StageAnswer, StageId};

use crate::Stage;

/// Two stages, one after the other - and itself a [`Stage`].
///
/// That last part is the design: a graph is not a second kind of thing that a
/// driver must know how to walk. A four-level lowering chain is three of these
/// nested, and the two drivers drive the composite with the same two methods
/// they drive a leaf with.
///
/// **`Pending` propagates without special handling.** If the first stage has
/// not landed, the chain is `Pending` and the second is never polled - and
/// since both are handed the SAME [`Context`], the waker the first registered
/// is the one that wakes the whole chain. There is no dependency graph in this
/// type, which is the point: the wake path is the poll path run backwards.
///
/// **`Unchanged` is where the ladder's cheapest rung is spent.** When `first`
/// answers [`StageAnswer::Unchanged`], `second`'s input is the value it was
/// handed last time, so polling it could only produce the answer it already
/// gave: the chain answers `Unchanged` too and `second` is never entered. That
/// is the saving - not a refcount avoided, a stage skipped - and it is why
/// `Unchanged` carries no value.
///
/// **Except while the join owes an answer**, which is the one piece of state
/// this type holds. A `second` that answered `Pending` has not produced
/// anything, so there is no answer for the chain to stand on: skipping it on
/// the next poll would make the landed value LOST rather than late, which is
/// the defect the wake half of the version gate exists to prevent, arriving
/// through the new variant. So the join remembers what it last handed on, and
/// re-polls `second` over it until `second` answers `Ready`. That state is
/// `Joint`, private to this module: it is the join's own memory, not a thing a
/// composed graph is assembled out of.
pub struct Chain<A: Stage, B> {
    first: A,
    second: B,
    id: StageId,
    joint: Mutex<Joint<A::Output>>,
}

/// What the join is holding between `first` and `second`.
///
/// Three states, and the middle one is the whole reason this exists: an
/// `Unchanged` from `first` may only skip `second` if `second` has settled over
/// what it was last handed.
enum Joint<T> {
    /// `second` has never been handed anything.
    Cold,
    /// `second` was handed this and has not answered `Ready` over it - it is
    /// `Pending`, or it failed. Either way the chain has no answer of its own,
    /// so an `Unchanged` upstream re-polls `second` over this rather than
    /// skipping it.
    Owing(Arc<T>),
    /// `second` answered `Ready` over what it was last handed. The chain has an
    /// answer, so an `Unchanged` upstream can be passed straight on.
    Settled,
}

impl<A: Stage, B> Chain<A, B> {
    /// Feed `first`'s output to `second`.
    ///
    /// The composite takes an id of its own because memo keys are `(stage id,
    /// inputs)` and two different chains over the same input type must not be
    /// confusable. It is unused while [`Chain::memo_key`] refuses to key -
    /// see there - but the id belongs to the composite either way, and asking
    /// for it now is cheaper than adding a required argument later.
    pub fn new(id: StageId, first: A, second: B) -> Self {
        Self {
            first,
            second,
            id,
            joint: Mutex::new(Joint::Cold),
        }
    }

    /// The first stage.
    pub fn first(&self) -> &A {
        &self.first
    }

    /// The second stage.
    pub fn second(&self) -> &B {
        &self.second
    }
}

/// **Both halves share one error type, and a join propagates rather than
/// retypes.**
///
/// An earlier revision tagged each failure with the half it came from and
/// nested once per join, so five stages read a type nobody writes in a
/// signature and "which stage failed" was answered by counting layers. The
/// position is stamped where it is known - at registration - and the tag is
/// gone (`DESIGN.md`, "One error type, flat and positioned"): here a failure
/// travels out unchanged, which is why this impl has no `map_err` in it.
/// **The join is spelled on the VALUE, not on the share.** `B` consumes what
/// `A` produces - `B::Input = A::Output` - even though what travels between
/// them is an `Arc` of it, because the share lives in the poll's return type
/// rather than in `Output` (`Stage`'s doc). The chain dereferences the share it
/// is holding and hands the second stage the value; there is no adapter
/// between two stages and nothing for a stage author to unwrap.
impl<A, B> Stage for Chain<A, B>
where
    A: Stage,
    B: Stage<Input = A::Output, Error = A::Error>,
{
    type Input = A::Input;
    type Output = B::Output;
    type Error = A::Error;

    fn id(&self) -> StageId {
        self.id
    }

    /// Always `None`: **a chain is not separately memoized, its parts are.**
    ///
    /// The composite's key would be the derived-hash fold - `H(stage_id,
    /// key(inputs))` - which is not built yet (`PLAN.md`, "Not built yet").
    /// Until it is, the honest answer is the one [`Stage::memo_key`] documents:
    /// refuse to key rather than invent one. Nothing is lost meanwhile, because
    /// the cheapness argument is about hitting at the FIRST level - wrap the halves in
    /// [`Memo`](crate::memo::Memo) and an unchanged input never reaches the second.
    fn memo_key(&self, _input: &Self::Input) -> Option<MemoKey> {
        None
    }

    fn poll_stage(
        &self,
        input: &Self::Input,
        cx: &mut Context<'_>,
    ) -> EffectPoll<StageAnswer<Arc<Self::Output>>, Self::Error> {
        match self.first.poll_stage(input, cx) {
            EffectPoll::Ready(StageAnswer::Computed(intermediate)) => {
                let polled = self.second.poll_stage(&intermediate, cx);
                self.settle(intermediate, &polled);
                polled
            }
            EffectPoll::Ready(StageAnswer::Unchanged) => match self.owed() {
                // `second` settled over what it was last handed, and what it
                // would be handed now is that same value. It is skipped
                // entirely - this is the rung.
                None => EffectPoll::Ready(StageAnswer::Unchanged),
                // `second` still owes an answer over that value. Re-poll it
                // rather than pass the `Unchanged` on: the chain has no answer
                // of its own for an `Unchanged` to refer to.
                Some(owed) => {
                    let polled = self.second.poll_stage(&owed, cx);
                    self.settle(owed, &polled);
                    polled
                }
            },
            EffectPoll::Pending => EffectPoll::Pending,
            EffectPoll::Failed(e) => EffectPoll::Failed(e),
        }
    }
}

impl<A, B> Chain<A, B>
where
    A: Stage,
    B: Stage<Input = A::Output, Error = A::Error>,
{
    /// What `second` still owes an answer over, if it owes one.
    ///
    /// `None` covers both "settled" and "cold". A cold join cannot be reached
    /// from an `Unchanged` first stage anyway: the cold-slot invariant
    /// ([`Memo`](crate::memo::Memo)) means a first stage answering `Unchanged`
    /// has answered `Computed` before, which is what filled this.
    fn owed(&self) -> Option<Arc<A::Output>> {
        match &*self.joint.lock().unwrap_or_else(PoisonError::into_inner) {
            Joint::Owing(handed) => Some(Arc::clone(handed)),
            Joint::Cold | Joint::Settled => None,
        }
    }

    /// Record what `second` was handed and whether it answered over it.
    ///
    /// `Ready` settles, whichever answer it carried: an `Unchanged` from
    /// `second` IS an answer, and the chain refers to it exactly as it refers
    /// to a computed one. `Pending` and `Failed` do not settle - a failure is
    /// retried on the next poll rather than remembered (`DESIGN.md`, "The four
    /// outcomes"), so the join must still be able to hand `second` its input.
    fn settle(
        &self,
        handed: Arc<A::Output>,
        polled: &EffectPoll<StageAnswer<Arc<B::Output>>, B::Error>,
    ) {
        *self.joint.lock().unwrap_or_else(PoisonError::into_inner) = match polled {
            EffectPoll::Ready(_) => Joint::Settled,
            EffectPoll::Pending | EffectPoll::Failed(_) => Joint::Owing(handed),
        };
    }
}
