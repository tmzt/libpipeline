//! Memoization: the lookup precedes the work.

use std::sync::{Arc, Mutex, PoisonError};
use std::task::Context;

use libpipelinedata::{EffectPoll, MemoKey, MemoStore, StageAnswer, StageId};

use crate::Stage;

use crate::track::revalidating;

/// A stage with a memo in front of it.
///
/// **The lookup precedes the work** (`DESIGN.md`, "The lookup precedes the
/// work"): the key is
/// computable from the input alone, so the cache is consulted before the stage
/// is polled rather than to validate what it produced. That is what makes a
/// four-level lowering chain cheap - an unchanged source hits at the first
/// level and the remaining three are never entered.
///
/// **The ledger outranks the store.** A key built from the input describes the
/// stage's ARGUMENTS; a stage that also reads tracked state has an ambient input
/// no such key can see, and a store consulted on the key alone will answer with
/// a value [`Ledger`](crate::track::Ledger) has already marked stale. So the lookup is
/// skipped entirely while [`revalidating`] is true - while this poll is running
/// inside the scope of a node the ledger ruled out - and the stage runs, which
/// is what a poll of a stale node is FOR.
///
/// Three properties come out of taking it here rather than asking each stage to
/// fold a revision into its key:
///
/// * **Nothing has to be declared, so nothing can be forgotten.** There is no
///   `Memo` that opts out; the check is not a constructor argument, a builder
///   flag or a trait bound the author supplies. The ledger records edges by
///   observing reads rather than accepting a declared list, and this is the
///   same rule applied to the correction.
/// * **`NoMemo` stays a legitimate implementation** - the property its own doc
///   names, "a pipeline whose ANSWERS change when the cache is disabled has a
///   bug the cache was hiding". Without this the tracked case failed exactly
///   that check; `invalidation_marks_dependents.rs`'s
///   `the_memo_over_tracked_state_changes_speed_and_not_answers` is that
///   control run over a graph which reads tracked state.
/// * **A pipeline with no tracking in it is untouched.** With no run scope open
///   `revalidating` is false, so a pure lowering chain pays one thread-local
///   read per lookup and behaves as it did.
///
/// **Wrap the memo in the tracking, not the tracking in the memo.**
/// `Tracked::new(&ledger, "x", Memo::new(stage, store))` puts the lookup inside
/// the node's scope, which is what lets it see the node's staleness.
/// `Memo::new(Tracked::new(..), store)` puts a cache OUTSIDE the node it should
/// be deferring to: the store answers before any scope opens, the tracked stage
/// is never polled, and the ledger's mark goes unread. That order is pinned as
/// this rule's known-bad twin
/// (`a_cache_outside_the_tracking_is_a_cache_the_ledger_cannot_reach`); it is a
/// composition this type cannot detect, because a memo is generic over a stage
/// and can no more inspect its inner one than a driver can.
///
/// **Only `Ready` is recorded, and the exclusions are the interesting part.**
///
/// * `Pending` is not a value; there is nothing to remember.
/// * `Failed` is deliberately not cached. The standing rule - memoize only
///   pure constructors with equal input versions; effects are never replayed
///   by an implicit cache - is exactly what caching a failure would break: a transient failure (a network that was down, a file not yet
///   written) would be served back as a settled fact under a key that says it
///   is fresh. An effect's result becomes a replay input by being RECORDED
///   deliberately, which is a different act from a cache remembering it.
///
///   **The exclusion is defeatable from outside**, and by the one construct
///   the boundary layer adds: an error boundary turns that `Failed` into a `Ready`, so a memo
///   above one is offered a fallback with the exclusion already spent. That is
///   why [`Guarded`](crate::boundary::Guarded) - the stage-level boundary - answers
///   `memo_key -> None`, and why the composition rule it states matters even
///   though breaking it now costs only speed.
/// * An input the stage refuses to key (`memo_key` says `None`) is neither
///   looked up nor recorded. See that method's doc for why refusing beats
///   inventing a key, and `Guarded` for the third and least obvious reason a
///   stage refuses: not that its input cannot be addressed, but that its
///   ANSWER may not be the one the input implies.
///
/// **The slot: what this POSITION last answered.**
///
/// Beside the store, and a different thing from it. The store is keyed by the
/// INPUT - "what did this stage answer for that argument" - and answers for any
/// input it has seen. The slot is keyed by nothing: it is the single last
/// [`Arc`] this position handed out, whether that came from the stage or from a
/// store hit, and it is what [`StageAnswer::Unchanged`] refers to when it
/// carries no value.
///
/// It is here, and not in a layer of its own, because it is the same question
/// the store answers one dimension smaller - and because `PLAN.md`'s step 8
/// collapses the two: a store indexed by position IS this slot, with the key
/// beside it. Putting it OUTSIDE the store lookup rather than inside the stage
/// is what keeps it honest: a poll served from the store is still an answer
/// this position gave, and a slot that missed those would refer to a value the
/// consumer no longer holds.
///
/// **What it enforces.** A cold slot has nothing to refer to, so a stage may
/// answer `Unchanged` only where this position has answered before. That holds
/// inductively - back to a first stage with no upstream, which must compute on
/// a cold pipeline - so it is an invariant to ASSERT rather than a type to
/// encode, and the assertion names the position. What it CANNOT check is
/// whether an `Unchanged` was true; see [`StageAnswer`]'s own doc, and
/// `PLAN.md` step 10 for the detector a key would make possible.
///
/// This type is the engine's machinery and not part of any consumer-facing
/// vocabulary: the facade's builder wraps every registration in one, and
/// nothing outside this crate can name it.
pub struct Memo<S: Stage, St> {
    stage: S,
    store: St,
    /// The last share this position answered with. Safe interior mutability,
    /// for the reason everything in this crate is: a poll holds `&self` all the
    /// way down.
    slot: Mutex<Option<Arc<S::Output>>>,
}

impl<S: Stage, St> Memo<S, St> {
    /// Put `store` in front of `stage`, with an empty slot.
    pub fn new(stage: S, store: St) -> Self {
        Self {
            stage,
            store,
            slot: Mutex::new(None),
        }
    }

    /// The share this position last answered with, if it has answered.
    ///
    /// What `Unchanged` refers to. `None` is a cold slot, which is the one
    /// state in which `Unchanged` is a defect rather than an answer.
    pub fn slot(&self) -> Option<Arc<S::Output>> {
        self.slot
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Record what this position is about to answer with.
    fn fill(&self, value: &Arc<S::Output>) {
        *self.slot.lock().unwrap_or_else(PoisonError::into_inner) = Some(Arc::clone(value));
    }

    /// The stage behind the memo.
    pub fn stage(&self) -> &S {
        &self.stage
    }

    /// The store behind the memo.
    pub fn store(&self) -> &St {
        &self.store
    }
}

impl<S, St> Stage for Memo<S, St>
where
    S: Stage,
    St: MemoStore<S::Output>,
{
    type Input = S::Input;
    /// **The stage's output, unchanged.** The share is in the poll's RETURN
    /// (`Stage::poll_stage`), not in this type: a memo is transparent about
    /// what a stage produces, and a `Stage<Output = Arc<T>>` would make every
    /// consumer of a memoized stage spell the engine's storage decision in its
    /// own constraint.
    ///
    /// What the memo holds and answers with is the share the stage below it
    /// already made, recorded on a miss and refcount-bumped on every hit after.
    /// No stage author writes `Arc` to be memoized cheaply and none can forget
    /// to.
    type Output = S::Output;
    type Error = S::Error;

    /// The inner stage's id. Memoization is transparent: it must not change
    /// what anything is keyed by, or the memo would be part of the semantics
    /// rather than an optimization over them.
    fn id(&self) -> StageId {
        self.stage.id()
    }

    fn memo_key(&self, input: &Self::Input) -> Option<MemoKey> {
        self.stage.memo_key(input)
    }

    fn poll_stage(
        &self,
        input: &Self::Input,
        cx: &mut Context<'_>,
    ) -> EffectPoll<StageAnswer<Arc<Self::Output>>, Self::Error> {
        let key = self.stage.memo_key(input);
        if let Some(key) = &key
            && !revalidating()
            && let Some(hit) = self.store.lookup(key)
        {
            // A hit is an answer this position gave, so the slot records it.
            // A store hit is NOT an `Unchanged`: the store answers for an
            // INPUT, and an input that oscillates hits on a value the consumer
            // stopped holding two polls ago. Answering `Unchanged` off a hit
            // needs the key compared against the SLOT's, which is `PLAN.md`
            // steps 9 and 10.
            self.fill(&hit);
            return EffectPoll::Ready(StageAnswer::Computed(hit));
        }
        match self.stage.poll_stage(input, cx) {
            EffectPoll::Ready(StageAnswer::Computed(held)) => {
                // Nothing is wrapped here: the poll already answered with the
                // share, so the row and the answer are one allocation and
                // recording is a refcount bump - as is every hit it serves
                // afterwards.
                //
                // Recorded even when the lookup was skipped, and under the same
                // key: the store then holds the value the stage has just
                // produced, so the next poll of an unstale node hits on
                // something current rather than on the entry the ledger ruled
                // out.
                if let Some(key) = &key {
                    self.store.record(key, Arc::clone(&held));
                }
                // **Where the detector goes**, when there is a key to build it
                // from. `Unchanged` is a claim the engine cannot check, and its
                // opposite is checkable at exactly this point: a stage that
                // answers `Computed` under a key EQUAL to the one the slot was
                // filled at has recomputed to the same address, which is the
                // defect `Unchanged` exists to let it report. Comparing them
                // and complaining under `debug_assertions` would be a detector
                // - it must not substitute an `Unchanged`, because the stage's
                // word is the mechanism and second-guessing it puts the
                // decision back in the engine. It waits on the per-stage key
                // (`PLAN.md`, steps 9 and 10): today's `MemoKey` is built by
                // the stage's own key function and is not kept beside the slot.
                self.fill(&held);
                EffectPoll::Ready(StageAnswer::Computed(held))
            }
            EffectPoll::Ready(StageAnswer::Unchanged) => {
                // **The cold-slot invariant.** `Unchanged` says "the answer you
                // have from me still stands"; with nothing in the slot there is
                // no such answer, and passing it on would tell the caller to
                // keep a value it was never given.
                assert!(
                    self.slot
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .is_some(),
                    "stage at position {} answered Unchanged before it had \
                     answered at all: Unchanged carries no value and refers to \
                     the one this position last gave, and this position has \
                     given none",
                    self.stage.id().index(),
                );
                // Nothing is recorded. The store holds what a poll PRODUCED and
                // this poll produced nothing. Recording the slot's value under
                // this input's key would in fact be sound - that IS this
                // input's answer - but the store keyed by input is the
                // dimension `PLAN.md` step 8 removes, and a write aimed at a
                // type that is going away buys a hit nobody has asked for.
                EffectPoll::Ready(StageAnswer::Unchanged)
            }
            EffectPoll::Pending => EffectPoll::Pending,
            EffectPoll::Failed(error) => EffectPoll::Failed(error),
        }
    }
}
