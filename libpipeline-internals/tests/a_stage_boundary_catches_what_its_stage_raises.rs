//! **Moved in from `tests/a_stage_boundary_catches_what_its_stage_raises.rs`** at the
//! visibility flip. It places an error boundary by hand
//! (`DriveError`, `FrameDriver`, `Guarded`, `Memo`, `NoPendingWork`, `WakePath`, `poll_watched`, `run_to_completion`), and the builder has no spelling for one -
//! `PLAN.md`'s finding 2. A test in `tests/` proves the PUBLIC API can
//! express something; a test in `src/` admits it cannot yet, and lives beside
//! the code it pins. Every assertion is the one it arrived with; when finding
//! 2 lands this migrates back out unchanged but for its imports.
//!
//! Gate: **a stage-level boundary is `libeffects`' boundary, applied to a
//! [`Stage`].**
//!
//! `libeffects` already gates the mechanism - caught, declined, bubbled,
//! pending-fallback, first-match-wins, and the composition twins.
//! This file does not re-gate any of that. It gates the seam:
//! [`Guarded`](libpipeline_internals::boundary::Guarded) hands a `Stage`'s failure to that machinery
//! and hands its answer back, so what a scope does about failure is the same at
//! both scales and there is one place for the failure semantics to live.
//!
//! Four claims, each of which could fail on its own:
//!
//! 1. **A failure the handler catches is substituted, and one it declines
//!    bubbles into this scope's channel.** The handler's three answers are
//!    `Recover`'s three, unchanged by the trip through a stage.
//! 2. **A boundary is not in the path of a value.** `Ready` and `Pending` pass
//!    through untouched - including the [`Context`], which the pending gates
//!    measure through this crate's own wake probe rather than by inspection.
//! 3. **Both drivers see the same thing** (the two-driver rule: a stage
//!    cannot tell which driver polls it). A boundary is where that would be easiest to break, since it is
//!    the first stage type whose answer depends on a failure.
//! 4. **The answers do not change when the cache is disabled.** `NoMemo` is a
//!    legitimate implementation, and a boundary must not be what breaks it.
//!
//! **Every type here is a stand-in** (`DESIGN.md`, "The engine stays
//! generic").

use std::convert::Infallible;
use std::sync::{Arc, Mutex};
use std::task::{Context, Waker};

use libeffects::{Arms, Fallback};
use libpipeline_internals::driver::DriveError;
use libpipeline_internals::driver::FrameDriver;
use libpipeline_internals::boundary::Guarded;
use libpipeline_internals::memo::Memo;
use libpipeline_internals::driver::NoPendingWork;
use libpipeline_internals::watch::WakePath;
use libpipeline_internals::watch::poll_watched;
use libpipeline_internals::driver::run_to_completion;
use libpipelinedata::{ContentKey, EffectPoll, MemoKey, MemoMap, NoMemo, StageId};
use libpipeline_internals::{Stage};

// ---------------------------------------------------------------- stand-ins

/// Stand-in for whatever an author wrote.
#[derive(Clone, PartialEq, Eq, Debug)]
struct Text(String);

/// Two failures, so an arm that names one can decline the other. A single
/// variant would let a handler that catches unconditionally pass every gate.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Boom {
    /// Transient in the sense a boundary cares about: it can clear.
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
    const ID: StageId = StageId::at(0);

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

    fn poll_stage(&self, input: &Text, cx: &mut Context<'_>) -> EffectPoll<Arc<String>, Boom> {
        *self.polls.lock().unwrap() += 1;
        let Some(filled) = *self.slot.lock().unwrap() else {
            self.waiting.lock().unwrap().push(cx.waker().clone());
            return match self.while_empty {
                WhileEmpty::Failing(boom) => EffectPoll::Failed(boom),
                WhileEmpty::Parked => EffectPoll::Pending,
            };
        };
        EffectPoll::Ready(Arc::new(format!("{}:{filled}", input.0)))
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
    ) -> EffectPoll<Arc<Self::Output>, Self::Error> {
        self.0.poll_stage(input, cx)
    }
}

const GUARD: StageId = StageId::at(1);

/// Poll once with a waker of no consequence - the offline driver's shape
/// (`driver.rs:80-81`). What this returns is what a driver sees.
fn driven<S: Stage>(stage: &S, input: &S::Input) -> EffectPoll<Arc<S::Output>, S::Error> {
    stage.poll_stage(input, &mut Context::from_waker(Waker::noop()))
}

/// `driven`, insisting on a value.
fn value_of<S: Stage>(stage: &S, input: &S::Input) -> Arc<S::Output>
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
    let guarded = Guarded::new(GUARD, Shared(Arc::clone(&flaky)), Fallback::new(Arc::new("fallback".to_string())));

    assert_eq!(*value_of(&guarded, &Text("src".into())), "fallback");
    assert_eq!(
        guarded.substitutions(),
        1,
        "the value channel cannot say a fallback was substituted, so this is \
         what says it",
    );

    // The failure clears, and the real answer replaces the fallback - nothing
    // in the boundary remembers the substitution it made.
    flaky.land("v1");
    assert_eq!(*value_of(&guarded, &Text("src".into())), "src:v1");
    assert_eq!(guarded.substitutions(), 1, "no second substitution");
}

#[test]
fn a_handler_that_declines_bubbles_into_this_scopes_own_channel() {
    // An arm for `Network` only: the scope has expressed one failure case and
    // says nothing about the other, which is what bubbling IS.
    let arms = || {
        Arms::escalating(Escaped)
            .catching(|boom: &Boom| matches!(boom, Boom::Network), Arc::new("fallback".to_string()))
    };

    let caught = Guarded::new(GUARD, Shared(Flaky::failing(Boom::Network)), arms());
    assert_eq!(*value_of(&caught, &Text("src".into())), "fallback");

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
    let guarded: Guarded<_, Fallback<Arc<String>>> = Guarded::new(
        GUARD,
        Shared(Flaky::failing(Boom::Malformed)),
        Fallback::new(Arc::new("the last resort".to_string())),
    );
    let value = match driven(&guarded, &Text("src".into())) {
        EffectPoll::Ready(value) => value,
        EffectPoll::Pending => panic!("nothing here is pending"),
        EffectPoll::Failed(never) => match never {},
    };
    assert_eq!(*value, "the last resort");

    let _: Result<Arc<String>, DriveError<Infallible>> =
        run_to_completion(&guarded, &Text("src".into()), &NoPendingWork);
}

// ---------------------------------------------------------------------------
// Gate 2: a boundary is not in the path of a value - `cx` included.
// ---------------------------------------------------------------------------

#[test]
fn a_value_and_a_park_pass_through_untouched() {
    let flaky = Flaky::new(WhileEmpty::Parked);
    let guarded = Guarded::new(GUARD, Shared(Arc::clone(&flaky)), Fallback::new(Arc::new("fallback".to_string())));

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
    assert_eq!(*value_of(&guarded, &Text("src".into())), "src:v1");
    assert_eq!(guarded.substitutions(), 0, "a value is nobody's fallback");
}

#[test]
fn a_fallback_that_is_still_loading_is_pending_rather_than_a_value() {
    // The handler's second answer: this scope HAS a case for the failure, and
    // that case is itself still computing. The no-cached-failures rule then applies to it
    // unchanged - and the waker it registers is the one the boundary was
    // handed, which is what the probe measures.
    let parked: Arc<Mutex<Vec<Waker>>> = Arc::new(Mutex::new(Vec::new()));
    let stashing = Arc::clone(&parked);
    let arms = Arms::<Boom, Arc<String>, Boom>::bubbling().arm(move |_boom, cx: &mut Context<'_>| {
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
// Gate 3: both drivers, one answer.
// ---------------------------------------------------------------------------

#[test]
fn both_drivers_see_the_same_answer_through_a_boundary() {
    let input = Text("src".into());

    // Caught: the offline driver reaches a value for a graph whose stage never
    // produced one, and so does the frame.
    let flaky = Flaky::failing(Boom::Network);
    let caught = Guarded::new(GUARD, Shared(Arc::clone(&flaky)), Fallback::new(Arc::new("fallback".to_string())));
    assert_eq!(
        run_to_completion(&caught, &input, &NoPendingWork).map_err(|_| "failed"),
        Ok(Arc::new("fallback".to_string())),
    );
    assert_eq!(
        FrameDriver::new().poll_frame(&caught, &input),
        EffectPoll::Ready(Arc::new("fallback".to_string())),
    );

    // Declined: the failure reaches BOTH drivers, carrying the same path.
    let declined = Guarded::new(
        GUARD,
        Shared(Flaky::failing(Boom::Malformed)),
        Arms::escalating(Escaped)
            .catching(|boom: &Boom| matches!(boom, Boom::Network), Arc::new("fallback".to_string())),
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
                // A memo answers a SHARE of its stage's output, and
                // `Recover::Value` is whatever the stage under the boundary
                // answers - so the substitute is a share too.
                Fallback::new(Arc::new("fallback".to_string())),
            );
            seen.push(value_of(&guarded, &input).to_string());
            seen.push(value_of(&guarded, &input).to_string());
            flaky.land("v1");
            seen.push(value_of(&guarded, &input).to_string());
            seen.push(value_of(&guarded, &input).to_string());
        } else {
            let guarded = Guarded::new(
                GUARD,
                Memo::new(Shared(Arc::clone(&flaky)), NoMemo),
                Fallback::new(Arc::new("fallback".to_string())),
            );
            seen.push(value_of(&guarded, &input).to_string());
            seen.push(value_of(&guarded, &input).to_string());
            flaky.land("v1");
            seen.push(value_of(&guarded, &input).to_string());
            seen.push(value_of(&guarded, &input).to_string());
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
        Fallback::new(Arc::new("fallback".to_string())),
    );
    let input = Text("src".into());

    assert_eq!(*value_of(&guarded, &input), "fallback");
    flaky.land("v1");
    assert_eq!(*value_of(&guarded, &input), "src:v1");

    let polls = flaky.polls();
    assert_eq!(*value_of(&guarded, &input), "src:v1");
    assert_eq!(
        flaky.polls(),
        polls,
        "the REAL answer is cached, under a key that moves with the input - \
         which is what the forbidden order gives up",
    );
}
