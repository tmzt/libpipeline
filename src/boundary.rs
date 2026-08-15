//! An error boundary around a STAGE (`PIPELINE_PLAN.md` §7), and the memo key
//! it refuses.
//!
//! The mechanism is `libeffects`' and is not repeated here. [`Recover`],
//! [`Boundary`], `Fallback` and `Arms` landed there in `9b42cd6`, with the scope
//! as a WRAPPER so that which boundary catches is decided by composition; this
//! module wraps a [`Stage`] and hands the failure to that machinery unchanged.
//!
//! What this layer adds is the one thing `libeffects` could not say, because it
//! has no key vocabulary and must not learn one: **a boundary refuses to be
//! memoized**. [`Guarded`]'s `memo_key` answers `None`, which is where the seam
//! `libeffects::Boundary`'s doc could only state becomes structural.

use std::sync::{Mutex, PoisonError};
use std::task::Context;

use libeffects::{Boundary, Effect, Recover};
use libpipelinedata::{BoundStage, EffectPoll, MemoKey, Stage, StageId};

/// A stage with an error boundary around it - §7's scope, at stage level.
///
/// # What it is, and what it delegates
///
/// A [`Stage`] whose failures are handed to a [`Recover`] handler: `Ready` and
/// `Pending` pass through untouched, and a `Failed` becomes whatever the
/// handler says - a substituted value, a still-loading fallback, or a bubble
/// into this scope's own error channel ([`Recover`]'s three cases, which are
/// `EffectPoll`'s three states for exactly that reason).
///
/// **None of that is implemented here.** The poll builds a
/// [`BoundStage`] - a stage with its input bound, which IS an
/// [`Effect`] - wraps it in `libeffects`' [`Boundary`] and polls that. So the
/// arms, the ordering, the pass-through and the substitution count are the same
/// code an effect-level boundary runs, and a change to §7's semantics has one
/// place to land rather than two that agree by convention.
///
/// **The boundary is built per poll, and that is a fact about `Stage` rather
/// than a shortcut.** A stage is handed its input by the caller on each poll, so
/// the effect a stage-level boundary guards does not exist between polls: the
/// `BoundStage` borrows both the stage and that poll's input and lives exactly
/// as long as the poll. What must outlive the poll is the substitution count, so
/// that is what this type holds - it accumulates what each poll's boundary
/// counted.
///
/// # It refuses to be memoized, and that is the point of it living here
///
/// **A boundary LAUNDERS an uncacheable answer into a cacheable-looking one.**
/// [`Memo`](crate::Memo) already refuses to record `Failed`, for
/// `OBJECTS_PLAN_PI.md:707`'s reason - "effects are never replayed by an
/// implicit cache", so a transient failure is not served back as a settled fact
/// (`memo.rs:54-63`). A boundary turns exactly that `Failed` into a `Ready`.
/// Cache it and the key says "input X, value V" while V is the fallback; the
/// input never moved, so the key never moves, and **the real value is never
/// computed again** - a permanent fallback that is indistinguishable, from the
/// value channel, from a correct answer.
///
/// `libeffects::Boundary`'s doc lists three things that keep that from
/// happening and is candid that only the first is structural there: a boundary
/// holds no store; the boundary must be composed OUTSIDE the memo; and
/// [`substitutions`](Guarded::substitutions) is the channel by which a
/// substituted `Ready` differs from a real one. The middle one is a composition
/// a generic type cannot check about itself - the same shape as `Memo`'s own
/// "wrap the memo in the tracking, not the tracking in the memo"
/// (`memo.rs:43-52`).
///
/// **Answering `memo_key` with `None` is what makes that seam structural on
/// this side.** `Memo` neither looks up nor records for an input
/// it has no key for (`memo.rs:119-133`), so the forbidden order stops being
/// able to poison anything: a cache placed above a boundary is a cache with
/// nothing to say. The rule is still worth composing correctly - the prescribed
/// order also memoizes the REAL answers, which the forbidden one cannot - but
/// getting it wrong now costs speed rather than correctness.
///
/// That is the third use of the `Option` step 1 put on
/// [`Stage::memo_key`], after [`Chain`](crate::Chain) - a composite whose
/// derived key needs §9's fold - and `highbay_elements`' unattributable
/// registry, where a pass no contributor claims has no honest content value
/// (`crates/highbay_elements/src/pipeline.rs:401-407` **\[read\]**). Three
/// unrelated reasons to refuse, one answer: "Refusing to key is the safe
/// answer; faking one is not."
///
/// # The second thing a laundered `Ready` poisons: the ledger
///
/// A memo remembers a VALUE. [`Ledger`](crate::Ledger) remembers something
/// else about the same poll - that the node is up to date - and a substituted
/// `Ready` is just as false there. So the composition rule has a second half,
/// stated here because no type can check it either:
///
/// > **A boundary goes outside the tracking, not inside it.**
/// > `Guarded::new(id, Tracked::new(&ledger, "n", stage), handler)`, never
/// > `Tracked::new(&ledger, "n", Guarded::new(id, stage, handler))`.
///
/// Inside, the tracked node's poll answers `Ready(fallback)`, its staleness is
/// cleared, and the node drops out of [`Ledger::schedule`](crate::Ledger) - so a
/// driver that polls what the schedule names polls nothing when the failure
/// clears, and the fallback is permanent. Outside, the node answers `Failed`,
/// stays stale because it still owes a value, and the next pass re-polls it. The
/// twin `a_boundary_inside_the_tracking_keeps_its_fallback_after_the_failure_clears`
/// measures the difference rather than asserting it.
///
/// `memo_key -> None` cannot close this half: the node id belongs to
/// [`Tracked`](crate::Tracked), and a boundary that reached for one would be
/// deciding which ledger it belongs to - which is the wrapper's job, and the
/// direction the composition already runs in.
///
/// # Its id is its own
///
/// [`Memo`](crate::Memo) and [`Tracked`](crate::Tracked) delegate `id()` because
/// they are transparent: neither changes what the stage answers. A boundary is
/// not transparent - same stage, same input, different answer, depending on the
/// handler - so delegating would let a caller build a key that says "this is
/// that stage's answer for that input" over a fallback, which is the laundering
/// again, one level up and out of this type's reach. So it takes an id of its
/// own, for the reason [`Chain::new`](crate::Chain::new) does, and it is unused
/// while `memo_key` refuses to key.
pub struct Guarded<S, H> {
    id: StageId,
    stage: S,
    handler: H,
    /// Safe interior mutability (`CLAUDE.md`): a poll holds `&self`, exactly as
    /// the ledger's own lock and `libeffects::Boundary`'s counter do.
    substitutions: Mutex<usize>,
}

impl<S, H> Guarded<S, H> {
    /// Guard `stage` with `handler`.
    ///
    /// Runs nothing: a boundary is a description of what a scope does about
    /// failure, in the sense [`Dormant`](libeffects::Dormant) means it.
    pub fn new(id: StageId, stage: S, handler: H) -> Self {
        Self {
            id,
            stage,
            handler,
            substitutions: Mutex::new(0),
        }
    }

    /// The stage inside the boundary - the scope's contents.
    pub fn stage(&self) -> &S {
        &self.stage
    }

    /// The handler - what this scope says about failure.
    pub fn handler(&self) -> &H {
        &self.handler
    }

    /// How many polls of this boundary have answered `Ready` with a SUBSTITUTED
    /// value rather than the stage's own.
    ///
    /// Monotone and never reset, so a caller asking "did this pass substitute"
    /// takes a difference across the pass rather than trusting a last-write-wins
    /// flag - and the answer stays meaningful when a boundary is polled from
    /// more than one consumer.
    ///
    /// **Why it exists.** Substituting is designed to be invisible in the value
    /// channel, so something else has to carry it. `libeffects::Boundary`'s doc
    /// names three readers; the one this crate owns is §5's offline driver,
    /// where `run_to_completion` returns `Ok(value)` for a graph that
    /// substituted every one of its answers - right for a frame, wrong for a
    /// build.
    pub fn substitutions(&self) -> usize {
        *self
            .substitutions
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

impl<S, H> Stage for Guarded<S, H>
where
    S: Stage,
    H: Recover<S::Error, Value = S::Output>,
{
    type Input = S::Input;
    type Output = S::Output;
    /// What this scope raises when its handler declines - §7's bubble, retyped
    /// into the containing scope's channel by the handler's choice.
    type Error = H::Escalated;

    fn id(&self) -> StageId {
        self.id
    }

    /// Always `None`: **a substituted answer must not be cached, and a stage
    /// that cannot say which of its answers were substituted must not be cached
    /// at all.** See the type doc for what caching one costs, and
    /// `a_memo_over_a_boundary_neither_looks_up_nor_records` /
    /// `a_boundary_that_keys_poisons_the_memo_it_is_under` for the measurement.
    ///
    /// Note what is NOT proposed instead: keying only the polls that did not
    /// substitute. The key is required to be computable from the input BEFORE
    /// the stage runs (`Stage::memo_key`), and whether this poll will substitute
    /// is not knowable then - it depends on whether the guarded stage fails,
    /// which is the thing being polled for. A key that is honest only in
    /// retrospect is not a key.
    fn memo_key(&self, _input: &Self::Input) -> Option<MemoKey> {
        None
    }

    fn poll_stage(
        &self,
        input: &Self::Input,
        cx: &mut Context<'_>,
    ) -> EffectPoll<Self::Output, Self::Error> {
        // `cx` goes through untouched, so a fallback that awaits registers on
        // the same waker the guarded stage would have - there is no second wake
        // path to keep in sync (`libeffects::Recover::recover`).
        let boundary = Boundary::new(BoundStage::new(&self.stage, input), Borrowed(&self.handler));
        let polled = boundary.poll_effect(cx);
        *self
            .substitutions
            .lock()
            .unwrap_or_else(PoisonError::into_inner) += boundary.substitutions();
        polled
    }
}

/// A borrowed handler is a handler.
///
/// [`Boundary`] takes its handler BY VALUE, and the boundary here is built per
/// poll (see [`Guarded`]) while the handler is owned by the stage and outlives
/// every poll of it. Without this, each poll would have to clone the handler -
/// which would require `H: Clone` for no reason, and would give a stateful
/// handler a fresh copy of its state per poll.
///
/// It is a local newtype rather than a blanket `impl Recover for &H` because
/// that impl belongs to `libeffects`, beside the trait; this crate can only
/// write it over a type of its own.
struct Borrowed<'a, H>(&'a H);

impl<E, H: Recover<E>> Recover<E> for Borrowed<'_, H> {
    type Value = H::Value;
    type Escalated = H::Escalated;

    fn recover(
        &self,
        failure: E,
        cx: &mut Context<'_>,
    ) -> EffectPoll<Self::Value, Self::Escalated> {
        self.0.recover(failure, cx)
    }
}
