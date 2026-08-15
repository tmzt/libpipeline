//! Memoization (`PIPELINE_PLAN.md` §3).

use std::task::Context;

use libpipelinedata::{EffectPoll, MemoKey, MemoStore, Stage, StageId};

/// A stage with a memo in front of it.
///
/// **The lookup precedes the work** (`PIPELINE_PLAN.md` §3): the key is
/// computable from the input alone, so the cache is consulted before the stage
/// is polled rather than to validate what it produced. That is what makes a
/// four-level lowering chain cheap - an unchanged source hits at the first
/// level and the remaining three are never entered.
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
            && let Some(hit) = self.store.lookup(key)
        {
            return EffectPoll::Ready(hit);
        }
        let polled = self.stage.poll_stage(input, cx);
        if let (EffectPoll::Ready(value), Some(key)) = (&polled, &key) {
            self.store.record(key, value.clone());
        }
        polled
    }
}
