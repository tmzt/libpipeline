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


use std::sync::{Arc, Mutex, PoisonError};
use std::task::Context;

use libeffects::{Boundary, Effect, Recover};
use libpipelinedata::{BoundStage, EffectPoll, MemoKey, Stage, StageId};

use crate::driver::{DriveError, PendingWork, run_to_completion};

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
/// [`Memo`](crate::memo::Memo) already refuses to record `Failed`, for
/// `OBJECTS_PLAN_PI.md:707`'s reason - "effects are never replayed by an
/// implicit cache", so a transient failure is not served back as a settled fact
/// (`memo.rs:57-63`). A boundary turns exactly that `Failed` into a `Ready`.
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
/// it has no key for (`memo.rs:128-142`), so the forbidden order stops being
/// able to poison anything: a cache placed above a boundary is a cache with
/// nothing to say. The rule is still worth composing correctly - the prescribed
/// order also memoizes the REAL answers, which the forbidden one cannot - but
/// getting it wrong now costs speed rather than correctness.
///
/// That is the third use of the `Option` step 1 put on
/// [`Stage::memo_key`], after [`Chain`](crate::chain::Chain) - a composite whose
/// derived key needs §9's fold - and `highbay_elements`' unattributable
/// registry, where a pass no contributor claims has no honest content value
/// (`crates/highbay_elements/src/pipeline.rs:401-407` **\[read\]**). Three
/// unrelated reasons to refuse, one answer: "Refusing to key is the safe
/// answer; faking one is not."
///
/// # The second thing a laundered `Ready` poisons: the ledger
///
/// A memo remembers a VALUE. [`Ledger`](crate::track::Ledger) remembers something
/// else about the same poll - that the node is up to date - and a substituted
/// `Ready` is just as false there. So the composition rule has a second half,
/// stated here because no type can check it either:
///
/// > **A boundary goes outside the tracking, not inside it.**
/// > `Guarded::new(id, Tracked::new(&ledger, "n", stage), handler)`, never
/// > `Tracked::new(&ledger, "n", Guarded::new(id, stage, handler))`.
///
/// Inside, the tracked node's poll answers `Ready(fallback)`, its staleness is
/// cleared, and the node drops out of [`Ledger::schedule`](crate::track::Ledger) - so a
/// driver that polls what the schedule names polls nothing when the failure
/// clears, and the fallback is permanent. Outside, the node answers `Failed`,
/// stays stale because it still owes a value, and the next pass re-polls it. The
/// twin `a_boundary_inside_the_tracking_keeps_its_fallback_after_the_failure_clears`
/// measures the difference rather than asserting it.
///
/// **And it does not stop at the node.** [`Backdated`](crate::track::Backdated)
/// addresses each `Ready` output and, when the address repeats, tells the
/// ledger this node produced nothing new - so a fallback that repeats retracts
/// the staleness of everything READING it, and the consumers are told nothing
/// moved about a node whose real answer has never been computed. Same
/// composition, one step further out;
/// `a_repeated_fallback_inside_backdating_retracts_what_its_consumers_owe`
/// measures it, and its prescribed-order twin shows a `Failed` node addressing
/// nothing and retracting nothing.
///
/// `memo_key -> None` cannot close this half: the node id belongs to
/// [`Tracked`](crate::track::Tracked), and a boundary that reached for one would be
/// deciding which ledger it belongs to - which is the wrapper's job, and the
/// direction the composition already runs in.
///
/// # Its id is its own
///
/// [`Memo`](crate::memo::Memo) and [`Tracked`](crate::track::Tracked) delegate `id()` because
/// they are transparent: neither changes what the stage answers. A boundary is
/// not transparent - same stage, same input, different answer, depending on the
/// handler - so delegating would let a caller build a key that says "this is
/// that stage's answer for that input" over a fallback, which is the laundering
/// again, one level up and out of this type's reach. So it takes an id of its
/// own, for the reason [`Chain::new`](crate::chain::Chain::new) does, and it is unused
/// while `memo_key` refuses to key.
pub(crate) struct Guarded<S, H> {
    id: StageId,
    stage: S,
    handler: H,
    substitutions: Arc<Substitutions>,
}

impl<S, H> Guarded<S, H> {
    /// Guard `stage` with `handler`, counting its substitutions alone.
    ///
    /// Runs nothing: a boundary is a description of what a scope does about
    /// failure, in the sense [`Dormant`](libeffects::Dormant) means it.
    pub(crate) fn new(id: StageId, stage: S, handler: H) -> Self {
        Self::tallied(id, stage, handler, &Substitutions::new())
    }

    /// Guard `stage` with `handler`, counting its substitutions into a tally
    /// this boundary SHARES - see [`Substitutions`] for why a build wants one
    /// number over a graph rather than one per scope.
    pub(crate) fn tallied(
        id: StageId,
        stage: S,
        handler: H,
        substitutions: &Arc<Substitutions>,
    ) -> Self {
        Self {
            id,
            stage,
            handler,
            substitutions: Arc::clone(substitutions),
        }
    }

    /// The stage inside the boundary - the scope's contents.
    pub(crate) fn stage(&self) -> &S {
        &self.stage
    }

    /// How many polls have answered `Ready` with a SUBSTITUTED value rather
    /// than the stage's own - **this boundary's**, for one built by
    /// [`new`](Guarded::new), or the shared total for one built by
    /// [`tallied`](Guarded::tallied).
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
    /// build. [`run_to_completion_counted`] is that reader.
    pub(crate) fn substitutions(&self) -> usize {
        self.substitutions.count()
    }
}

/// How many answers were substituted, counted across every boundary that shares
/// one of these - §7's "built" versus "built on fallbacks".
///
/// **Why a shared type rather than one counter per boundary.** The question a
/// build asks is about the GRAPH ("did anything I am about to ship stand on a
/// fallback?"), and the outermost boundary's own count cannot answer it: a
/// boundary counts what IT substituted, and a scope further in that recovered
/// and handed a value up leaves the outer one with nothing to report. Handing
/// several boundaries one tally makes the number the build's, which is the
/// scope the question has.
///
/// **Monotone, never reset**, for [`Guarded::substitutions`]'s reason: a caller
/// takes a difference across the pass it cares about, which composes when two
/// passes share a graph and a last-write-wins flag does not.
///
/// **It counts substituting POLLS, not substituted nodes.** A graph polled
/// twice by the offline driver's pump loop counts a persistent fallback twice,
/// and two consumers of one shared boundary count it once each. The question it
/// answers exactly is the yes/no one - `count() == 0` means nothing was
/// substituted - and the magnitude is a poll count, not a census of the graph.
/// Nothing here can do better: the engine holds no node identity for a stage
/// (`PIPELINE_PLAN.md`:579-583), which is the same reason
/// [`Schedule`](crate::schedule::Schedule) deals in ids and not work.
#[derive(Debug, Default)]
pub(crate) struct Substitutions {
    /// Safe interior mutability (`CLAUDE.md`): a poll holds `&self` all the way
    /// down, exactly as the ledger's own lock and `libeffects::Boundary`'s
    /// counter do.
    count: Mutex<usize>,
}

impl Substitutions {
    /// A fresh tally at zero.
    ///
    /// `Arc`-wrapped because the boundaries that share one outlive any single
    /// poll of any of them - the same reason [`Ledger::new`](crate::track::Ledger::new)
    /// hands one back.
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// How many substitutions have been counted here.
    pub(crate) fn count(&self) -> usize {
        *self.count.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn add(&self, polls: usize) {
        *self.count.lock().unwrap_or_else(PoisonError::into_inner) += polls;
    }
}

/// [`run_to_completion`], reporting how many of the answers it drove through
/// were SUBSTITUTED.
///
/// **The finding this closes**, in `PIPELINE_PLAN.md` §7's words: "a build that
/// silently ships fallbacks is the failure mode this section exists to
/// prevent". `run_to_completion` returns `Ok(value)` for a graph whose every
/// answer was a fallback, which is right for a frame - the pane draws the
/// stand-in and the wake brings the real thing - and wrong for a build, which
/// has nowhere to put a value that will be correct later. The count is what
/// separates the two, and until this function existed nothing said the CLI
/// should ask.
///
/// **It is the same drive, not a second driver**, and that is literal: it calls
/// [`run_to_completion`] and returns exactly what that returns. §5's rule is
/// that a stage cannot tell which driver polls it, so an observation must ride
/// ALONGSIDE the result rather than change its type - the shape
/// [`run_to_completion_watched`](crate::watch::run_to_completion_watched) already
/// takes for the wake report. Nothing is asked of the stage; the tally is the
/// caller's, because only a boundary can substitute and which boundaries belong
/// to this build is a fact about the caller's composition, not about `S`.
///
/// **The frame driver needs no counterpart.** A frame loop holds the tally and
/// takes a difference across the frame it just drew - which is the same
/// measurement, spelled where a frame loop can spell it.
///
/// The returned count is this drive's alone: a tally reused across passes
/// reports per pass.
pub(crate) fn run_to_completion_counted<S, W>(
    stage: &S,
    input: &S::Input,
    work: &W,
    substitutions: &Substitutions,
) -> (Result<S::Output, DriveError<S::Error>>, usize)
where
    S: Stage,
    W: PendingWork + ?Sized,
{
    let before = substitutions.count();
    let driven = run_to_completion(stage, input, work);
    (driven, substitutions.count() - before)
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
        self.substitutions.add(boundary.substitutions());
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

#[cfg(test)]
mod a_boundary_is_not_a_cacheable_answer {
    //! **Moved in from `tests/a_boundary_is_not_a_cacheable_answer.rs`** at the
    //! visibility flip. It places an error boundary by hand
    //! (`Guarded`, `Memo`), and the builder has no spelling for one -
    //! `DESIGN.md`'s finding 2. A test in `tests/` proves the PUBLIC API can
    //! express something; a test in `src/` admits it cannot yet, and lives beside
    //! the code it pins. Every assertion is the one it arrived with; when finding
    //! 2 lands this migrates back out unchanged but for its imports.
    //!
    //! Gate: **a boundary refuses to be memoized, and a boundary that did not
    //! would poison the memo it sits under** (`PIPELINE_PLAN.md` §7, §3).
    //!
    //! The hazard, stated on [`Guarded`](crate::boundary::Guarded) and on
    //! `libeffects::Boundary` before it: a boundary LAUNDERS an uncacheable answer
    //! into a cacheable-looking one. [`Memo`] already refuses to record `Failed`,
    //! for `OBJECTS_PLAN_PI.md:707`'s reason - effects are never replayed by an
    //! implicit cache - and a boundary turns exactly that `Failed` into a `Ready`.
    //! Cache it and the key says "input X, value V" while V is the fallback; the
    //! input never moved, so the key never moves, and the real value is never
    //! computed again.
    //!
    //! **What is measured here is that the refusal is LOAD-BEARING, not that it
    //! happens.** Asserting `memo_key == None` proves the line is present; it does
    //! not prove anything would go wrong without it. So the twin,
    //! `a_boundary_that_keys_poisons_the_memo_it_is_under`, is the same boundary
    //! with ONE line changed - `memo_key` delegates to the guarded stage instead of
    //! refusing - and it demonstrates the stale fallback rather than asserting it
    //! cannot happen. `a_keyed_boundary_answers_identically_with_no_memo_in_front`
    //! is what makes that a fair comparison: the two differ in the key and in
    //! nothing else.
    //!
    //! **The composition is the FORBIDDEN one throughout.**
    //! `libeffects::Boundary`'s rule is that a boundary goes outside the memo, and
    //! the gates below put a memo outside a boundary on purpose. That is the point:
    //! with `memo_key -> None` the forbidden order is merely useless - a cache with
    //! nothing to say - where before it was dangerous. The prescribed order is
    //! gated too (`a_boundary_outside_a_memo_caches_only_real_answers`), because
    //! what it buys is the reason to keep composing that way.
    //!
    //! **Every type here is a stand-in** (`PIPELINE_PLAN.md`:584-589).

    use std::sync::{Arc, Mutex};
    use std::task::{Context, Waker};

    use libeffects::{Fallback, Recover};
    use crate::boundary::Guarded;
    use crate::memo::Memo;
    use libpipelinedata::{ContentKey, EffectPoll, MemoKey, MemoMap, MemoStore, Stage, StageId};

    // ---------------------------------------------------------------- stand-ins

    /// Stand-in for whatever an author wrote.
    #[derive(Clone, PartialEq, Eq, Debug)]
    struct Text(String);

    /// Transient in the sense the memo rule cares about: it can clear.
    #[derive(Clone, Copy, PartialEq, Eq, Debug)]
    struct Boom;

    /// A stage that fails until its slot is filled, registering the waker on the
    /// way out so the failure CAN clear - which is the whole hazard: a fallback
    /// that could never be replaced would be no worse for being cached.
    struct Flaky {
        slot: Mutex<Option<&'static str>>,
        waiting: Mutex<Vec<Waker>>,
        polls: Mutex<usize>,
    }

    impl Flaky {
        const ID: StageId = StageId::new("test.flaky", 1);

        fn failing() -> Arc<Self> {
            Arc::new(Self {
                slot: Mutex::new(None),
                waiting: Mutex::new(Vec::new()),
                polls: Mutex::new(0),
            })
        }

        fn land(&self, value: &'static str) {
            *self.slot.lock().unwrap() = Some(value);
            for waker in self.waiting.lock().unwrap().drain(..) {
                waker.wake();
            }
        }

        /// How many times this was polled - which is how many times it RAN.
        fn polls(&self) -> usize {
            *self.polls.lock().unwrap()
        }
    }

    impl Stage for Flaky {
        type Input = Text;
        type Output = String;
        type Error = Boom;

        fn id(&self) -> StageId {
            Self::ID
        }

        /// **It keys, and keys honestly.** What refuses below is the BOUNDARY, not
        /// a stage that could not have keyed anyway.
        fn memo_key(&self, input: &Text) -> Option<MemoKey> {
            Some(MemoKey::new(Self::ID, [ContentKey::of(&input.0)]))
        }

        fn poll_stage(&self, input: &Text, cx: &mut Context<'_>) -> EffectPoll<String, Boom> {
            *self.polls.lock().unwrap() += 1;
            let Some(filled) = *self.slot.lock().unwrap() else {
                self.waiting.lock().unwrap().push(cx.waker().clone());
                return EffectPoll::Failed(Boom);
            };
            EffectPoll::Ready(format!("{}:{filled}", input.0))
        }
    }

    /// A shared stage is a stage. See the note in
    /// `a_stage_boundary_catches_what_its_stage_raises.rs`: `Stage` has no
    /// forwarding impl for `Arc<S>` and `libeffects::Effect` does.
    #[derive(Clone)]
    struct Shared<S>(Arc<S>);

    impl<S: Stage> Stage for Shared<S> {
        type Input = S::Input;
        type Output = S::Output;
        type Error = S::Error;

        fn id(&self) -> StageId {
            self.0.id()
        }

        fn memo_key(&self, input: &Self::Input) -> Option<MemoKey> {
            self.0.memo_key(input)
        }

        fn poll_stage(
            &self,
            input: &Self::Input,
            cx: &mut Context<'_>,
        ) -> EffectPoll<Self::Output, Self::Error> {
            self.0.poll_stage(input, cx)
        }
    }

    /// **THE TWIN: [`Guarded`] with one line changed.**
    ///
    /// `memo_key` delegates to the guarded stage - which is the obvious, wrong
    /// thing to write, and exactly what [`Memo`] and
    /// [`Tracked`](crate::track::Tracked) DO write, because those two are
    /// transparent and this is not. Everything else is `Guarded`'s: the same poll,
    /// the same handler, the same id.
    struct Keyed<S, H>(Guarded<S, H>);

    impl<S, H> Stage for Keyed<S, H>
    where
        S: Stage,
        H: Recover<S::Error, Value = S::Output>,
    {
        type Input = S::Input;
        type Output = S::Output;
        type Error = H::Escalated;

        fn id(&self) -> StageId {
            self.0.id()
        }

        /// The one line. `Guarded`'s answers `None`.
        fn memo_key(&self, input: &Self::Input) -> Option<MemoKey> {
            self.0.stage().memo_key(input)
        }

        fn poll_stage(
            &self,
            input: &Self::Input,
            cx: &mut Context<'_>,
        ) -> EffectPoll<Self::Output, Self::Error> {
            self.0.poll_stage(input, cx)
        }
    }

    /// A store that answers exactly as [`MemoMap`] does and counts what it was
    /// asked - so "neither looks up nor records" is measured at the store rather
    /// than inferred from an answer.
    struct Watching<V> {
        rows: MemoMap<V>,
        lookups: Mutex<usize>,
        records: Mutex<usize>,
    }

    impl<V> Watching<V> {
        fn new() -> Self {
            Self {
                rows: MemoMap::new(),
                lookups: Mutex::new(0),
                records: Mutex::new(0),
            }
        }

        fn lookups(&self) -> usize {
            *self.lookups.lock().unwrap()
        }

        fn records(&self) -> usize {
            *self.records.lock().unwrap()
        }
    }

    impl<V: Clone> MemoStore<V> for Watching<V> {
        fn lookup(&self, key: &MemoKey) -> Option<V> {
            *self.lookups.lock().unwrap() += 1;
            self.rows.lookup(key)
        }

        fn record(&self, key: &MemoKey, value: V) {
            *self.records.lock().unwrap() += 1;
            self.rows.record(key, value);
        }
    }

    const GUARD: StageId = StageId::new("test.guard", 1);

    fn guard(flaky: &Arc<Flaky>) -> Guarded<Shared<Flaky>, Fallback<String>> {
        Guarded::new(
            GUARD,
            Shared(Arc::clone(flaky)),
            Fallback::new("fallback".to_string()),
        )
    }

    /// Poll once with a waker of no consequence - the offline driver's shape.
    fn value_of<S: Stage<Output = String>>(stage: &S, input: &S::Input) -> String {
        match stage.poll_stage(input, &mut Context::from_waker(Waker::noop())) {
            EffectPoll::Ready(value) => value,
            EffectPoll::Pending => panic!("nothing here is pending"),
            EffectPoll::Failed(_) => panic!("this boundary catches everything"),
        }
    }

    // ---------------------------------------------------------------------------
    // Gate 1: the refusal itself.
    // ---------------------------------------------------------------------------

    #[test]
    fn a_boundary_refuses_to_key_even_when_its_stage_keys() {
        let flaky = Flaky::failing();
        let guarded = guard(&flaky);

        for source in ["src", "other", ""] {
            let input = Text(source.into());
            assert!(
                guarded.stage().memo_key(&input).is_some(),
                "the guarded stage keys this input",
            );
            assert_eq!(
                guarded.memo_key(&input),
                None,
                "and the boundary over it does not: whether this poll will \
                 substitute is not knowable before the stage runs, so there is no \
                 key that is honest in advance",
            );
        }
    }

    // ---------------------------------------------------------------------------
    // Gate 2: the forbidden order, made harmless.
    // ---------------------------------------------------------------------------

    #[test]
    fn a_memo_over_a_boundary_neither_looks_up_nor_records() {
        // `Memo::new(Boundary::new(..), ..)` - the composition `libeffects` forbids
        // - built on purpose. With `memo_key -> None` the store is never consulted
        // and never written, so the cache has nothing to serve back and nothing to
        // poison.
        let flaky = Flaky::failing();
        let poisonable = Memo::new(guard(&flaky), Watching::new());
        let input = Text("src".into());

        assert_eq!(value_of(&poisonable, &input), "fallback");
        assert_eq!(value_of(&poisonable, &input), "fallback");
        assert_eq!(poisonable.store().lookups(), 0, "no key, no lookup");
        assert_eq!(poisonable.store().records(), 0, "no key, no record");

        // The failure clears. The input never moved - so a key built from it never
        // moved either, and what makes the real answer reachable is that there was
        // never an entry to hit.
        flaky.land("v1");
        assert_eq!(value_of(&poisonable, &input), "src:v1");
        assert_eq!(
            value_of(&poisonable, &input),
            "src:v1",
            "and the real answer is not cached either: a stage that cannot say \
             which of its answers were substituted is not cached at all",
        );
        assert_eq!(poisonable.store().lookups(), 0);
        assert_eq!(poisonable.store().records(), 0);
    }

    #[test]
    fn a_boundary_that_keys_poisons_the_memo_it_is_under() {
        // THE TWIN. One line different from the gate above, and it does not merely
        // fail an assertion - it demonstrates the defect.
        let flaky = Flaky::failing();
        let poisoned = Memo::new(Keyed(guard(&flaky)), Watching::new());
        let input = Text("src".into());

        assert_eq!(value_of(&poisoned, &input), "fallback");
        assert_eq!(
            poisoned.store().records(),
            1,
            "the substituted value was recorded as if it were the real answer",
        );

        let polls_before = flaky.polls();
        flaky.land("v1");
        assert_eq!(
            value_of(&poisoned, &input),
            "fallback",
            "the failure cleared and the cached fallback did not: this is the \
             poisoning, and from the value channel it is indistinguishable from a \
             correct answer",
        );
        assert_eq!(
            flaky.polls(),
            polls_before,
            "and the real value is never even computed - the memo hits first, so \
             the guarded stage is never polled again",
        );
        assert!(poisoned.store().lookups() > 0);

        // It is permanent, not slow: the input is what the key is built from, and
        // the input is what did not move.
        for _ in 0..5 {
            assert_eq!(value_of(&poisoned, &input), "fallback");
        }
    }

    #[test]
    fn a_keyed_boundary_answers_identically_with_no_memo_in_front() {
        // What makes the twin FAIR. Without a memo the two boundaries are the same
        // boundary: same substitution, same real answer once the failure clears.
        // The key is the only difference between them, so the poisoning above is
        // attributable to the key and to nothing else.
        let sequence = |keyed: bool| {
            let flaky = Flaky::failing();
            let input = Text("src".into());
            let mut seen = Vec::new();
            if keyed {
                let stage = Keyed(guard(&flaky));
                seen.push(value_of(&stage, &input));
                flaky.land("v1");
                seen.push(value_of(&stage, &input));
            } else {
                let stage = guard(&flaky);
                seen.push(value_of(&stage, &input));
                flaky.land("v1");
                seen.push(value_of(&stage, &input));
            }
            seen
        };

        assert_eq!(sequence(true), sequence(false));
        assert_eq!(sequence(true), vec!["fallback", "src:v1"]);
    }

    // ---------------------------------------------------------------------------
    // Gate 3: what the prescribed order buys.
    // ---------------------------------------------------------------------------

    #[test]
    fn a_boundary_outside_a_memo_caches_only_real_answers() {
        // `Boundary::new(Memo::new(..), ..)` - the prescribed order. The memo's
        // inner stage answers `Failed`, which the memo passes through unrecorded,
        // and the substitution happens above it where no key reaches. So the store
        // holds real answers and only real answers.
        let flaky = Flaky::failing();
        let guarded = Guarded::new(
            GUARD,
            Memo::new(Shared(Arc::clone(&flaky)), Watching::new()),
            Fallback::new("fallback".to_string()),
        );
        let input = Text("src".into());

        assert_eq!(value_of(&guarded, &input), "fallback");
        assert_eq!(
            guarded.stage().store().records(),
            0,
            "a failure is not a value, so the memo has nothing to remember - and \
             the fallback was never offered to it",
        );

        flaky.land("v1");
        assert_eq!(value_of(&guarded, &input), "src:v1");
        assert_eq!(guarded.stage().store().records(), 1);

        let polls = flaky.polls();
        assert_eq!(value_of(&guarded, &input), "src:v1");
        assert_eq!(
            flaky.polls(),
            polls,
            "and THIS hit is the one worth having: the real answer, under a key \
             that moves when the input does",
        );
    }
}

#[cfg(test)]
mod a_stage_boundary_catches_what_its_stage_raises {
    //! **Moved in from `tests/a_stage_boundary_catches_what_its_stage_raises.rs`** at the
    //! visibility flip. It places an error boundary by hand
    //! (`DriveError`, `FrameDriver`, `Guarded`, `Memo`, `NoPendingWork`, `WakePath`, `poll_watched`, `run_to_completion`), and the builder has no spelling for one -
    //! `DESIGN.md`'s finding 2. A test in `tests/` proves the PUBLIC API can
    //! express something; a test in `src/` admits it cannot yet, and lives beside
    //! the code it pins. Every assertion is the one it arrived with; when finding
    //! 2 lands this migrates back out unchanged but for its imports.
    //!
    //! Gate: **a stage-level boundary is `libeffects`' boundary, applied to a
    //! [`Stage`]** (`PIPELINE_PLAN.md` §7).
    //!
    //! `libeffects` already gates the mechanism - caught, declined, bubbled,
    //! pending-fallback, first-match-wins, and the composition twins (`9b42cd6`).
    //! This file does not re-gate any of that. It gates the seam:
    //! [`Guarded`](crate::boundary::Guarded) hands a `Stage`'s failure to that machinery
    //! and hands its answer back, so what a scope does about failure is the same at
    //! both scales and there is one place for §7's semantics to live.
    //!
    //! Four claims, each of which could fail on its own:
    //!
    //! 1. **A failure the handler catches is substituted, and one it declines
    //!    bubbles into this scope's channel.** The handler's three answers are
    //!    `Recover`'s three, unchanged by the trip through a stage.
    //! 2. **A boundary is not in the path of a value.** `Ready` and `Pending` pass
    //!    through untouched - including the [`Context`], which the pending gates
    //!    measure through this crate's own wake probe rather than by inspection.
    //! 3. **Both drivers see the same thing** (§5: a stage cannot tell which driver
    //!    polls it). A boundary is where that would be easiest to break, since it is
    //!    the first stage type whose answer depends on a failure.
    //! 4. **The answers do not change when the cache is disabled.** `NoMemo` is a
    //!    legitimate implementation, and a boundary must not be what breaks it.
    //!
    //! **Every type here is a stand-in** (`PIPELINE_PLAN.md`:584-589).

    use std::convert::Infallible;
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Waker};

    use libeffects::{Arms, Fallback};
    use crate::driver::DriveError;
    use crate::driver::FrameDriver;
    use crate::boundary::Guarded;
    use crate::memo::Memo;
    use crate::driver::NoPendingWork;
    use crate::watch::WakePath;
    use crate::watch::poll_watched;
    use crate::driver::run_to_completion;
    use libpipelinedata::{ContentKey, EffectPoll, MemoKey, MemoMap, NoMemo, Stage, StageId};

    // ---------------------------------------------------------------- stand-ins

    /// Stand-in for whatever an author wrote.
    #[derive(Clone, PartialEq, Eq, Debug)]
    struct Text(String);

    /// Two failures, so an arm that names one can decline the other. A single
    /// variant would let a handler that catches unconditionally pass every gate.
    #[derive(Clone, Copy, PartialEq, Eq, Debug)]
    enum Boom {
        /// Transient in the sense §7 cares about: it can clear.
        Network,
        /// A different failure, used to make declining observable.
        Malformed,
    }

    /// How a failure looks once it has left a scope that named its own error
    /// channel - `Chain`'s tagging (`chain.rs:48-54`), reduced to one arm.
    #[derive(Clone, Copy, PartialEq, Eq, Debug)]
    struct Escaped(Boom);

    /// What a stage does until its slot is filled.
    #[derive(Clone, Copy, PartialEq, Eq, Debug)]
    enum WhileEmpty {
        /// Fail, and register the waker on the way out - a real effect whose
        /// failure can clear must arrange a wake for when it does.
        Failing(Boom),
        /// Park.
        Parked,
    }

    /// A stage whose answer is whatever its slot currently holds, and which can be
    /// told to land a value later.
    struct Flaky {
        while_empty: WhileEmpty,
        slot: Mutex<Option<&'static str>>,
        waiting: Mutex<Vec<Waker>>,
        polls: Mutex<usize>,
    }

    impl Flaky {
        const ID: StageId = StageId::new("test.flaky", 1);

        fn new(while_empty: WhileEmpty) -> Arc<Self> {
            Arc::new(Self {
                while_empty,
                slot: Mutex::new(None),
                waiting: Mutex::new(Vec::new()),
                polls: Mutex::new(0),
            })
        }

        fn failing(boom: Boom) -> Arc<Self> {
            Self::new(WhileEmpty::Failing(boom))
        }

        /// The value arrives, and whoever was waiting is told to poll again.
        fn land(&self, value: &'static str) {
            *self.slot.lock().unwrap() = Some(value);
            for waker in self.waiting.lock().unwrap().drain(..) {
                waker.wake();
            }
        }

        /// How many times this was polled - which is how many times it RAN, since
        /// the poll is the run.
        fn polls(&self) -> usize {
            *self.polls.lock().unwrap()
        }
    }

    impl Stage for Flaky {
        type Input = Text;
        type Output = String;
        type Error = Boom;

        fn id(&self) -> StageId {
            Self::ID
        }

        /// It keys, and keys honestly. That matters for the memo gates in
        /// `a_boundary_is_not_a_cacheable_answer.rs`: what refuses to key there is
        /// the BOUNDARY, not a stage that could not have keyed anyway.
        fn memo_key(&self, input: &Text) -> Option<MemoKey> {
            Some(MemoKey::new(Self::ID, [ContentKey::of(&input.0)]))
        }

        fn poll_stage(&self, input: &Text, cx: &mut Context<'_>) -> EffectPoll<String, Boom> {
            *self.polls.lock().unwrap() += 1;
            let Some(filled) = *self.slot.lock().unwrap() else {
                self.waiting.lock().unwrap().push(cx.waker().clone());
                return match self.while_empty {
                    WhileEmpty::Failing(boom) => EffectPoll::Failed(boom),
                    WhileEmpty::Parked => EffectPoll::Pending,
                };
            };
            EffectPoll::Ready(format!("{}:{filled}", input.0))
        }
    }

    /// A shared stage is a stage - written here because [`Stage`] has no forwarding
    /// impl for `Arc<S>`, and this crate cannot add one.
    ///
    /// **[`Effect`](libeffects::Effect) HAS that impl**, with an argument that
    /// transfers word for word: "a node with more than one consumer is the case
    /// error boundaries are interesting in, and without an impl like this one every
    /// consumer would have to OWN its guarded node, which is precisely the graph
    /// shape that cannot arise" (`libeffects/src/effect.rs:59-84` **[read]**, which
    /// gives both `&T` and `Arc<T>`). A DAG's nodes are stages, so the same shape
    /// is wanted here and is unspellable except as a newtype per test crate. Its
    /// existence is the finding; the fix belongs in `libpipelinedata` beside the
    /// trait.
    #[derive(Clone)]
    struct Shared<S>(Arc<S>);

    impl<S: Stage> Stage for Shared<S> {
        type Input = S::Input;
        type Output = S::Output;
        type Error = S::Error;

        fn id(&self) -> StageId {
            self.0.id()
        }

        fn memo_key(&self, input: &Self::Input) -> Option<MemoKey> {
            self.0.memo_key(input)
        }

        fn poll_stage(
            &self,
            input: &Self::Input,
            cx: &mut Context<'_>,
        ) -> EffectPoll<Self::Output, Self::Error> {
            self.0.poll_stage(input, cx)
        }
    }

    const GUARD: StageId = StageId::new("test.guard", 1);

    /// Poll once with a waker of no consequence - the offline driver's shape
    /// (`driver.rs:80-81`). What this returns is what a driver sees.
    fn driven<S: Stage>(stage: &S, input: &S::Input) -> EffectPoll<S::Output, S::Error> {
        stage.poll_stage(input, &mut Context::from_waker(Waker::noop()))
    }

    /// `driven`, insisting on a value.
    fn value_of<S: Stage>(stage: &S, input: &S::Input) -> S::Output
    where
        S::Error: std::fmt::Debug,
    {
        match driven(stage, input) {
            EffectPoll::Ready(value) => value,
            EffectPoll::Pending => panic!("expected a value, got Pending"),
            EffectPoll::Failed(e) => panic!("expected a value, got {e:?}"),
        }
    }

    // ---------------------------------------------------------------------------
    // Gate 1: caught, declined, bubbled.
    // ---------------------------------------------------------------------------

    #[test]
    fn a_failure_the_handler_catches_is_substituted_for_the_stages_answer() {
        let flaky = Flaky::failing(Boom::Network);
        let guarded = Guarded::new(GUARD, Shared(Arc::clone(&flaky)), Fallback::new("fallback".into()));

        assert_eq!(value_of(&guarded, &Text("src".into())), "fallback");
        assert_eq!(
            guarded.substitutions(),
            1,
            "the value channel cannot say a fallback was substituted, so this is \
             what says it",
        );

        // The failure clears, and the real answer replaces the fallback - nothing
        // in the boundary remembers the substitution it made.
        flaky.land("v1");
        assert_eq!(value_of(&guarded, &Text("src".into())), "src:v1");
        assert_eq!(guarded.substitutions(), 1, "no second substitution");
    }

    #[test]
    fn a_handler_that_declines_bubbles_into_this_scopes_own_channel() {
        // An arm for `Network` only: the scope has expressed one failure case and
        // says nothing about the other, which per §7 is what bubbling IS.
        let arms = || {
            Arms::escalating(Escaped)
                .catching(|boom: &Boom| matches!(boom, Boom::Network), "fallback".to_string())
        };

        let caught = Guarded::new(GUARD, Shared(Flaky::failing(Boom::Network)), arms());
        assert_eq!(value_of(&caught, &Text("src".into())), "fallback");

        let declined = Guarded::new(GUARD, Shared(Flaky::failing(Boom::Malformed)), arms());
        assert_eq!(
            driven(&declined, &Text("src".into())),
            EffectPoll::Failed(Escaped(Boom::Malformed)),
            "an unhandled failure bubbles, retyped into the containing scope's \
             channel - and the path out is recoverable from the value",
        );
        assert_eq!(
            declined.substitutions(),
            0,
            "declining is not substituting: nothing took the stage's place",
        );
    }

    #[test]
    fn an_outermost_stage_boundary_makes_an_unhandled_failure_impossible_by_type() {
        // `Fallback::Escalated = Infallible`, so `Guarded`'s error channel is
        // `Infallible` and "nothing bubbles past here" is checked by the compiler
        // rather than claimed by a comment. Writing the failed arm costs
        // `match never {}`.
        let guarded: Guarded<_, Fallback<String>> = Guarded::new(
            GUARD,
            Shared(Flaky::failing(Boom::Malformed)),
            Fallback::new("the last resort".into()),
        );
        let value = match driven(&guarded, &Text("src".into())) {
            EffectPoll::Ready(value) => value,
            EffectPoll::Pending => panic!("nothing here is pending"),
            EffectPoll::Failed(never) => match never {},
        };
        assert_eq!(value, "the last resort");

        let _: Result<String, DriveError<Infallible>> =
            run_to_completion(&guarded, &Text("src".into()), &NoPendingWork);
    }

    // ---------------------------------------------------------------------------
    // Gate 2: a boundary is not in the path of a value - `cx` included.
    // ---------------------------------------------------------------------------

    #[test]
    fn a_value_and_a_park_pass_through_untouched() {
        let flaky = Flaky::new(WhileEmpty::Parked);
        let guarded = Guarded::new(GUARD, Shared(Arc::clone(&flaky)), Fallback::new("fallback".into()));

        let (polled, path) = poll_watched(&guarded, &Text("src".into()), Waker::noop());
        assert_eq!(polled, EffectPoll::Pending, "a park is not a failure");
        assert_eq!(
            path,
            Some(WakePath::Registered),
            "the guarded stage registered on the SAME context the boundary was \
             handed: there is no second wake path to keep in sync",
        );
        assert_eq!(guarded.substitutions(), 0);

        flaky.land("v1");
        assert_eq!(value_of(&guarded, &Text("src".into())), "src:v1");
        assert_eq!(guarded.substitutions(), 0, "a value is nobody's fallback");
    }

    #[test]
    fn a_fallback_that_is_still_loading_is_pending_rather_than_a_value() {
        // The handler's second answer: this scope HAS a case for the failure, and
        // that case is itself still computing. §3's rule then applies to it
        // unchanged - and the waker it registers is the one the boundary was
        // handed, which is what the probe measures.
        let parked: Arc<Mutex<Vec<Waker>>> = Arc::new(Mutex::new(Vec::new()));
        let stashing = Arc::clone(&parked);
        let arms = Arms::<Boom, String, Boom>::bubbling().arm(move |_boom, cx: &mut Context<'_>| {
            stashing.lock().unwrap().push(cx.waker().clone());
            Some(EffectPoll::Pending)
        });
        let guarded = Guarded::new(GUARD, Shared(Flaky::failing(Boom::Network)), arms);

        let (polled, path) = poll_watched(&guarded, &Text("src".into()), Waker::noop());
        assert_eq!(polled, EffectPoll::Pending);
        assert_eq!(
            path,
            Some(WakePath::Registered),
            "a pending FALLBACK owes the same wake a pending stage does, and it \
             registers on the context the boundary passed straight through",
        );
        assert_eq!(parked.lock().unwrap().len(), 1);
        assert_eq!(
            guarded.substitutions(),
            0,
            "a fallback that has not produced a value has substituted nothing yet",
        );
    }

    // ---------------------------------------------------------------------------
    // Gate 3: both drivers, one answer (§5).
    // ---------------------------------------------------------------------------

    #[test]
    fn both_drivers_see_the_same_answer_through_a_boundary() {
        let input = Text("src".into());

        // Caught: the offline driver reaches a value for a graph whose stage never
        // produced one, and so does the frame.
        let flaky = Flaky::failing(Boom::Network);
        let caught = Guarded::new(GUARD, Shared(Arc::clone(&flaky)), Fallback::new("fallback".into()));
        assert_eq!(
            run_to_completion(&caught, &input, &NoPendingWork).map_err(|_| "failed"),
            Ok("fallback".to_string()),
        );
        assert_eq!(
            FrameDriver::new().poll_frame(&caught, &input),
            EffectPoll::Ready("fallback".to_string()),
        );

        // Declined: the failure reaches BOTH drivers, carrying the same path.
        let declined = Guarded::new(
            GUARD,
            Shared(Flaky::failing(Boom::Malformed)),
            Arms::escalating(Escaped)
                .catching(|boom: &Boom| matches!(boom, Boom::Network), "fallback".to_string()),
        );
        assert_eq!(
            run_to_completion(&declined, &input, &NoPendingWork),
            Err(DriveError::Failed(Escaped(Boom::Malformed))),
        );
        assert_eq!(
            FrameDriver::new().poll_frame(&declined, &input),
            EffectPoll::Failed(Escaped(Boom::Malformed)),
        );

        // And the substitution count is the same measurement under either driver:
        // two polls above, both substituted.
        assert_eq!(caught.substitutions(), 2);
        assert_eq!(declined.substitutions(), 0);
    }

    // ---------------------------------------------------------------------------
    // Gate 4: the control run - `NoMemo` must stay legitimate.
    // ---------------------------------------------------------------------------

    #[test]
    fn the_answers_through_a_boundary_do_not_change_when_the_cache_is_disabled() {
        // The prescribed composition, both ways round on the STORE only: boundary
        // outside the memo, and the memo either remembering or not. A pipeline
        // whose answers change when the cache is disabled has a bug the cache was
        // hiding - and a boundary, which turns a `Failed` into a `Ready`, is
        // exactly where such a bug would live.
        let sequence = |remembering: bool| {
            let flaky = Flaky::failing(Boom::Network);
            let input = Text("src".into());
            let mut seen = Vec::new();
            if remembering {
                let guarded = Guarded::new(
                    GUARD,
                    Memo::new(Shared(Arc::clone(&flaky)), MemoMap::new()),
                    Fallback::new("fallback".to_string()),
                );
                seen.push(value_of(&guarded, &input));
                seen.push(value_of(&guarded, &input));
                flaky.land("v1");
                seen.push(value_of(&guarded, &input));
                seen.push(value_of(&guarded, &input));
            } else {
                let guarded = Guarded::new(
                    GUARD,
                    Memo::new(Shared(Arc::clone(&flaky)), NoMemo),
                    Fallback::new("fallback".to_string()),
                );
                seen.push(value_of(&guarded, &input));
                seen.push(value_of(&guarded, &input));
                flaky.land("v1");
                seen.push(value_of(&guarded, &input));
                seen.push(value_of(&guarded, &input));
            }
            seen
        };

        assert_eq!(sequence(true), sequence(false));
        assert_eq!(
            sequence(false),
            vec![
                "fallback".to_string(),
                "fallback".to_string(),
                "src:v1".to_string(),
                "src:v1".to_string(),
            ],
            "and the sequence is the one the pipeline should have: the fallback \
             only while the stage is failing",
        );
    }

    #[test]
    fn a_memoized_stage_under_a_boundary_still_serves_its_real_answers() {
        // The other half of the control: the prescribed order does not merely stay
        // correct, it keeps the memo working. Once the stage has produced a value,
        // the store answers and the stage is not polled again.
        let flaky = Flaky::failing(Boom::Network);
        let guarded = Guarded::new(
            GUARD,
            Memo::new(Shared(Arc::clone(&flaky)), MemoMap::new()),
            Fallback::new("fallback".to_string()),
        );
        let input = Text("src".into());

        assert_eq!(value_of(&guarded, &input), "fallback");
        flaky.land("v1");
        assert_eq!(value_of(&guarded, &input), "src:v1");

        let polls = flaky.polls();
        assert_eq!(value_of(&guarded, &input), "src:v1");
        assert_eq!(
            flaky.polls(),
            polls,
            "the REAL answer is cached, under a key that moves with the input - \
             which is what the forbidden order gives up",
        );
    }
}

#[cfg(test)]
mod a_build_can_ask_whether_it_stood_on_a_fallback {
    //! **Moved in from `tests/a_build_can_ask_whether_it_stood_on_a_fallback.rs`** at the
    //! visibility flip. It places an error boundary by hand
    //! (`Chain`, `Guarded`, `NoPendingWork`, `Substitutions`, `run_to_completion`, `run_to_completion_counted`), and the builder has no spelling for one -
    //! `DESIGN.md`'s finding 2. A test in `tests/` proves the PUBLIC API can
    //! express something; a test in `src/` admits it cannot yet, and lives beside
    //! the code it pins. Every assertion is the one it arrived with; when finding
    //! 2 lands this migrates back out unchanged but for its imports.
    //!
    //! Gate: **the offline driver can say whether it built on fallbacks, without
    //! the two drivers answering differently** (`PIPELINE_PLAN.md` §7, §5).
    //!
    //! §7's finding, verbatim: "`run_to_completion` returns `Ok(value)` for a graph
    //! that substituted EVERY answer - right for a frame, wrong for a build.
    //! `substitutions()` separates 'built' from 'built on fallbacks' without giving
    //! the two drivers different return types, which §5 forbids - but that means
    //! the CLI must ASK, and until this sentence nothing said it should. **A build
    //! that silently ships fallbacks is the failure mode this section exists to
    //! prevent.**"
    //!
    //! Three claims:
    //!
    //! 1. **The ambiguity is real**, so it is demonstrated rather than described:
    //!    `run_to_completion` answers `Ok` identically for a graph that computed its
    //!    value and one that substituted it, and nothing in the returned value
    //!    tells them apart.
    //! 2. **The count resolves it and changes nothing else.** Same drive, same
    //!    result, one more observation - the shape
    //!    [`run_to_completion_watched`](crate::watch::run_to_completion_watched)
    //!    already took for wake paths.
    //! 3. **One tally covers the graph.** A boundary counts what it substituted, so
    //!    the outermost one cannot answer for a scope further in that recovered on
    //!    its own; sharing a [`Substitutions`] is what makes the number the build's.
    //!
    //! **Every type here is a stand-in** (`PIPELINE_PLAN.md`:584-589).

    use std::sync::Mutex;
    use std::task::{Context, Waker};

    use libeffects::Fallback;
    use crate::chain::Chain;
    use crate::boundary::Guarded;
    use crate::driver::NoPendingWork;
    use crate::boundary::Substitutions;
    use crate::driver::run_to_completion;
    use crate::boundary::run_to_completion_counted;
    use libpipelinedata::{EffectPoll, MemoKey, Stage, StageId};

    // ---------------------------------------------------------------- stand-ins

    /// Stand-in for whatever an author wrote.
    #[derive(Clone, PartialEq, Eq, Debug)]
    struct Text(String);

    #[derive(Clone, Copy, PartialEq, Eq, Debug)]
    struct Boom;

    /// A stage that fails until its slot is filled.
    struct Flaky {
        slot: Mutex<Option<&'static str>>,
        waiting: Mutex<Vec<Waker>>,
    }

    impl Flaky {
        const ID: StageId = StageId::new("test.flaky", 1);

        fn failing() -> Self {
            Self {
                slot: Mutex::new(None),
                waiting: Mutex::new(Vec::new()),
            }
        }

        fn holding(value: &'static str) -> Self {
            Self {
                slot: Mutex::new(Some(value)),
                waiting: Mutex::new(Vec::new()),
            }
        }
    }

    impl Stage for Flaky {
        type Input = Text;
        type Output = String;
        type Error = Boom;

        fn id(&self) -> StageId {
            Self::ID
        }

        fn memo_key(&self, _input: &Text) -> Option<MemoKey> {
            None
        }

        fn poll_stage(&self, input: &Text, cx: &mut Context<'_>) -> EffectPoll<String, Boom> {
            let Some(filled) = *self.slot.lock().unwrap() else {
                self.waiting.lock().unwrap().push(cx.waker().clone());
                return EffectPoll::Failed(Boom);
            };
            EffectPoll::Ready(format!("{}:{filled}", input.0))
        }
    }

    /// The second half of a chain: it consumes what the first produced and, like
    /// the first, may fail. Nothing about it is interesting except that it is a
    /// SECOND scope, so a graph can have two boundaries in it.
    struct Appends {
        slot: Mutex<Option<&'static str>>,
    }

    impl Appends {
        const ID: StageId = StageId::new("test.appends", 1);

        fn failing() -> Self {
            Self {
                slot: Mutex::new(None),
            }
        }
    }

    impl Stage for Appends {
        type Input = String;
        type Output = String;
        type Error = Boom;

        fn id(&self) -> StageId {
            Self::ID
        }

        fn memo_key(&self, _input: &String) -> Option<MemoKey> {
            None
        }

        fn poll_stage(&self, input: &String, _cx: &mut Context<'_>) -> EffectPoll<String, Boom> {
            match *self.slot.lock().unwrap() {
                Some(filled) => EffectPoll::Ready(format!("{input}/{filled}")),
                None => EffectPoll::Failed(Boom),
            }
        }
    }

    const GUARD: StageId = StageId::new("test.guard", 1);
    const CHAIN: StageId = StageId::new("test.chain", 1);

    // ---------------------------------------------------------------------------
    // Gate 1: the ambiguity, and its resolution.
    // ---------------------------------------------------------------------------

    #[test]
    fn the_plain_offline_driver_cannot_tell_a_fallback_from_an_answer() {
        // Two graphs, one that computed its answer and one that substituted every
        // answer it had. `run_to_completion` reports `Ok` for both, and the value
        // channel is designed not to carry the difference - that is what
        // substituting IS.
        let input = Text("src".into());

        let real = Guarded::new(
            GUARD,
            Flaky::holding("v1"),
            Fallback::new("fallback".to_string()),
        );
        let substituted = Guarded::new(
            GUARD,
            Flaky::failing(),
            Fallback::new("fallback".to_string()),
        );

        let built = run_to_completion(&real, &input, &NoPendingWork);
        let built_on_fallbacks = run_to_completion(&substituted, &input, &NoPendingWork);

        assert!(built.is_ok());
        assert!(
            built_on_fallbacks.is_ok(),
            "the drive SUCCEEDED on a graph that produced none of its own answers, \
             which is right for a frame and wrong for a build",
        );
    }

    #[test]
    fn the_counted_drive_says_which_it_was_and_returns_the_same_result() {
        let input = Text("src".into());

        let tally = Substitutions::new();
        let real = Guarded::tallied(
            GUARD,
            Flaky::holding("v1"),
            Fallback::new("fallback".to_string()),
            &tally,
        );
        let (driven, substitutions) =
            run_to_completion_counted(&real, &input, &NoPendingWork, &tally);
        assert_eq!(driven.map_err(|_| "failed"), Ok("src:v1".to_string()));
        assert_eq!(substitutions, 0, "nothing stood in for anything");

        let tally = Substitutions::new();
        let substituted = Guarded::tallied(
            GUARD,
            Flaky::failing(),
            Fallback::new("fallback".to_string()),
            &tally,
        );
        let (driven, substitutions) =
            run_to_completion_counted(&substituted, &input, &NoPendingWork, &tally);
        assert_eq!(
            driven.map_err(|_| "failed"),
            Ok("fallback".to_string()),
            "the RESULT is unchanged - a driver that failed a graph the plain one \
             completes would break §5 in order to report on it",
        );
        assert_eq!(
            substitutions, 1,
            "and the finding rides alongside it, where a build can act on it",
        );
    }

    // ---------------------------------------------------------------------------
    // Gate 2: one tally covers the graph.
    // ---------------------------------------------------------------------------

    #[test]
    fn one_tally_counts_every_boundary_in_the_graph() {
        // Two scopes, one build. Both substitute, and the OUTER one substituted
        // nothing itself - its stage handed it a value, because the inner scope
        // had already recovered. A count taken from the outermost boundary would
        // report zero for a build that stood entirely on fallbacks.
        let tally = Substitutions::new();
        let inner = Guarded::tallied(
            GUARD,
            Flaky::failing(),
            Fallback::new("first fallback".to_string()),
            &tally,
        );
        let outer = Guarded::tallied(
            GUARD,
            Appends::failing(),
            Fallback::new("second fallback".to_string()),
            &tally,
        );
        let graph = Chain::new(CHAIN, inner, outer);

        let (driven, substitutions) =
            run_to_completion_counted(&graph, &Text("src".into()), &NoPendingWork, &tally);
        assert_eq!(
            driven.map_err(|_| "failed"),
            Ok("second fallback".to_string()),
        );
        assert_eq!(
            substitutions, 2,
            "both scopes substituted, and the build's question is about the graph",
        );
        assert_eq!(
            graph.second().substitutions(),
            2,
            "a shared tally reads the same from either boundary: it is the tally's \
             count, not the boundary's",
        );
    }

    #[test]
    fn a_private_tally_stays_the_boundarys_own() {
        // The default. `Guarded::new` gives each scope its own counter, so the
        // per-scope question - which one substituted - is still askable, and a
        // caller who wants the graph's number opts into sharing one.
        let first = Guarded::new(
            GUARD,
            Flaky::failing(),
            Fallback::new("first fallback".to_string()),
        );
        let second = Guarded::new(
            GUARD,
            Appends::failing(),
            Fallback::new("second fallback".to_string()),
        );
        let graph = Chain::new(CHAIN, first, second);

        assert!(run_to_completion(&graph, &Text("src".into()), &NoPendingWork).is_ok());
        assert_eq!(graph.first().substitutions(), 1);
        assert_eq!(graph.second().substitutions(), 1);
    }

    // ---------------------------------------------------------------------------
    // Gate 3: the count is this drive's.
    // ---------------------------------------------------------------------------

    #[test]
    fn a_reused_tally_reports_per_drive_rather_than_forever() {
        // The counter is monotone and never reset - which is what makes it safe to
        // share - so the driver takes a difference across the drive it ran. A
        // second build of the same graph must not inherit the first one's number.
        let tally = Substitutions::new();
        let guarded = Guarded::tallied(
            GUARD,
            Flaky::failing(),
            Fallback::new("fallback".to_string()),
            &tally,
        );
        let input = Text("src".into());

        let (_, first) = run_to_completion_counted(&guarded, &input, &NoPendingWork, &tally);
        let (_, second) = run_to_completion_counted(&guarded, &input, &NoPendingWork, &tally);
        assert_eq!((first, second), (1, 1));
        assert_eq!(
            tally.count(),
            2,
            "while the tally itself keeps the running total, which is what a frame \
             loop differences across a frame",
        );
    }

    #[test]
    fn a_drive_that_recovers_between_passes_reports_the_change() {
        // What the number is FOR: a build that stood on a fallback, run again once
        // the failure cleared, reports zero - so "did this build on fallbacks" is
        // answerable per build rather than per process.
        let tally = Substitutions::new();
        let guarded = Guarded::tallied(
            GUARD,
            Flaky::failing(),
            Fallback::new("fallback".to_string()),
            &tally,
        );
        let input = Text("src".into());

        let (driven, substitutions) =
            run_to_completion_counted(&guarded, &input, &NoPendingWork, &tally);
        assert_eq!(driven.map_err(|_| "failed"), Ok("fallback".to_string()));
        assert_eq!(substitutions, 1);

        *guarded.stage().slot.lock().unwrap() = Some("v1");
        let (driven, substitutions) =
            run_to_completion_counted(&guarded, &input, &NoPendingWork, &tally);
        assert_eq!(driven.map_err(|_| "failed"), Ok("src:v1".to_string()));
        assert_eq!(substitutions, 0, "this build is not standing on anything");
    }

    // ---------------------------------------------------------------------------
    // Gate 4: the frame driver needs no counterpart.
    // ---------------------------------------------------------------------------

    #[test]
    fn a_frame_loop_takes_the_same_measurement_by_differencing_the_tally() {
        // §5 forbids giving the two drivers different return types, and this is
        // the other end of that: there is no counted FRAME driver, because a frame
        // loop already holds the tally and a frame is a difference across it. The
        // same graph, the same measurement, spelled where a frame loop can spell
        // it.
        let tally = Substitutions::new();
        let guarded = Guarded::tallied(
            GUARD,
            Flaky::failing(),
            Fallback::new("fallback".to_string()),
            &tally,
        );
        let driver = crate::driver::FrameDriver::new();
        let input = Text("src".into());

        let before = tally.count();
        assert_eq!(
            driver.poll_frame(&guarded, &input),
            EffectPoll::Ready("fallback".to_string()),
        );
        assert_eq!(
            tally.count() - before,
            1,
            "this frame drew a fallback, and the pane that wants to draw it \
             differently can know that",
        );
    }
}
