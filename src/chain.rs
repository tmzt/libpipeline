//! Composing stages into a graph.

// **Dead in the shipped library, alive in the test build - and that is the
// flip's honest arithmetic rather than an oversight.**
//
// With the flat exports gone (`DESIGN.md`, "Migration plan") nothing outside
// this crate can name what is below, and the builder has no spelling for it
// yet, so the only callers are this module's own `#[cfg(test)]` tests. The
// allow is `not(test)` ON PURPOSE: under `cargo test` the lint stays fully
// armed, so code that becomes genuinely unused still fails the gate. It comes
// off the day the builder grows the spelling the findings name, because the
// builder will then be the caller.
#![cfg_attr(not(test), allow(dead_code))]


use std::task::Context;

use libpipelinedata::{EffectPoll, MemoKey, Stage, StageId};

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
/// is the one that wakes the whole chain. There is no edge bookkeeping in this
/// type, which is the point: the wake path is the poll path run backwards.
pub(crate) struct Chain<A, B> {
    first: A,
    second: B,
    id: StageId,
}

impl<A, B> Chain<A, B> {
    /// Feed `first`'s output to `second`.
    ///
    /// The composite takes an id of its own because memo keys are `(stage id,
    /// inputs)` and two different chains over the same input type must not be
    /// confusable. It is unused while [`Chain::memo_key`] refuses to key -
    /// see there - but the id belongs to the composite either way, and asking
    /// for it now is cheaper than adding a required argument later.
    pub(crate) fn new(id: StageId, first: A, second: B) -> Self {
        Self { first, second, id }
    }

    /// The first stage.
    pub(crate) fn first(&self) -> &A {
        &self.first
    }

    /// The second stage.
    pub(crate) fn second(&self) -> &B {
        &self.second
    }
}

/// A failure from one of a chain's two halves, tagged with which.
///
/// This is the boundary rule at its smallest scope: a failure that the inner stage did
/// not handle bubbles to the containing scope, which retypes it into its own
/// error channel rather than flattening it. Nesting chains nests this type, so
/// the path a failure took out is recoverable from its type - and an error
/// boundary that wants to catch only its own half can match on the tag.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum ChainError<A, B> {
    /// The first stage failed; the second never ran.
    First(A),
    /// The first stage produced a value and the second failed on it.
    Second(B),
}

impl<A, B> Stage for Chain<A, B>
where
    A: Stage,
    B: Stage<Input = A::Output>,
{
    type Input = A::Input;
    type Output = B::Output;
    type Error = ChainError<A::Error, B::Error>;

    fn id(&self) -> StageId {
        self.id
    }

    /// Always `None`: **a chain is not separately memoized, its parts are.**
    ///
    /// The composite's key would be the derived-hash fold - `H(stage_id,
    /// key(inputs))` - which is not built yet (`DESIGN.md`, "Not built yet").
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
    ) -> EffectPoll<Self::Output, Self::Error> {
        match self.first.poll_stage(input, cx) {
            EffectPoll::Ready(intermediate) => self
                .second
                .poll_stage(&intermediate, cx)
                .map_err(ChainError::Second),
            EffectPoll::Pending => EffectPoll::Pending,
            EffectPoll::Failed(e) => EffectPoll::Failed(ChainError::First(e)),
        }
    }
}
