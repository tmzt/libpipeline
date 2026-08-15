//! Memoization (`PIPELINE_PLAN.md` §3).

use std::task::Context;

use libpipelinedata::{EffectPoll, MemoKey, MemoStore, Stage, StageId};

use crate::revalidating;

/// A stage with a memo in front of it.
///
/// **The lookup precedes the work** (`PIPELINE_PLAN.md` §3): the key is
/// computable from the input alone, so the cache is consulted before the stage
/// is polled rather than to validate what it produced. That is what makes a
/// four-level lowering chain cheap - an unchanged source hits at the first
/// level and the remaining three are never entered.
///
/// **The ledger outranks the store.** A key built from the input describes the
/// stage's ARGUMENTS; a stage that also reads tracked state has an ambient input
/// no such key can see, and a store consulted on the key alone will answer with
/// a value [`Ledger`](crate::Ledger) has already marked stale. So the lookup is
/// skipped entirely while [`revalidating`] is true - while this poll is running
/// inside the scope of a node the ledger ruled out - and the stage runs, which
/// is what a poll of a stale node is FOR.
///
/// Three properties come out of taking it here rather than asking each stage to
/// fold a revision into its key:
///
/// * **Nothing has to be declared, so nothing can be forgotten.** There is no
///   `Memo` that opts out; the check is not a constructor argument, a builder
///   flag or a trait bound the author supplies. §3 records edges by observing
///   reads rather than accepting a declared list, and this is the same rule
///   applied to the correction.
/// * **`NoMemo` stays a legitimate implementation** - the property its own doc
///   names, "a pipeline whose ANSWERS change when the cache is disabled has a
///   bug the cache was hiding". Without this the tracked case failed exactly
///   that check; `invalidation_marks_dependents.rs`'s
///   `the_memo_over_tracked_state_changes_speed_and_not_answers` is that
///   control run over a graph which reads tracked state.
/// * **A pipeline with no tracking in it is untouched.** With no run scope open
///   `revalidating` is false, so the pure-lowering chains of §4 pay one thread-
///   local read per lookup and behave as they did.
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
/// * `Failed` is deliberately not cached. `OBJECTS_PLAN_PI.md:707`'s rule -
///   "memoize only pure constructors with equal input versions; effects are
///   never replayed by an implicit cache" - is exactly what caching a failure
///   would break: a transient failure (a network that was down, a file not yet
///   written) would be served back as a settled fact under a key that says it
///   is fresh. An effect's result becomes a replay input by being RECORDED
///   deliberately, which is a different act from a cache remembering it.
/// * An input the stage refuses to key (`memo_key` says `None`) is neither
///   looked up nor recorded. See that method's doc for why refusing beats
///   inventing a key.
///
/// This type is `libpipeline`'s and not `libpipelinedata`'s because it is
/// machinery: a crate that only implements a stage should not link it
/// (`PIPELINE_PLAN.md`:517-531).
pub struct Memo<S, St> {
    stage: S,
    store: St,
}

impl<S, St> Memo<S, St> {
    /// Put `store` in front of `stage`.
    pub fn new(stage: S, store: St) -> Self {
        Self { stage, store }
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
    S::Output: Clone,
    St: MemoStore<S::Output>,
{
    type Input = S::Input;
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
    ) -> EffectPoll<Self::Output, Self::Error> {
        let key = self.stage.memo_key(input);
        if let Some(key) = &key
            && !revalidating()
            && let Some(hit) = self.store.lookup(key)
        {
            return EffectPoll::Ready(hit);
        }
        let polled = self.stage.poll_stage(input, cx);
        // Recorded even when the lookup was skipped, and under the same key:
        // the store then holds the value the stage has just produced, so the
        // next poll of an unstale node hits on something current rather than on
        // the entry the ledger ruled out.
        if let (EffectPoll::Ready(value), Some(key)) = (&polled, &key) {
            self.store.record(key, value.clone());
        }
        polled
    }
}
