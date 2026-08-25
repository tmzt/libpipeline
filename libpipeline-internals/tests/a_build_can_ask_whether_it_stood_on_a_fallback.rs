//! **Moved in from `tests/a_build_can_ask_whether_it_stood_on_a_fallback.rs`** at the
//! visibility flip. It places an error boundary by hand
//! (`Chain`, `Guarded`, `NoPendingWork`, `Substitutions`, `run_to_completion`, `run_to_completion_counted`), and the builder has no spelling for one -
//! `PLAN.md`'s finding 2. A test in `tests/` proves the PUBLIC API can
//! express something; a test in `src/` admits it cannot yet, and lives beside
//! the code it pins. Every assertion is the one it arrived with; when finding
//! 2 lands this migrates back out unchanged but for its imports.
//!
//! Gate: **the offline driver can say whether it built on fallbacks, without
//! the two drivers answering differently.**
//!
//! The finding: `run_to_completion` returns `Ok(value)` for a graph
//! that substituted EVERY answer - right for a frame, wrong for a build.
//! `substitutions()` separates "built" from "built on fallbacks" without giving
//! the two drivers different return types, which the two-driver rule forbids
//! - but that means the batch run must ASK, and until this function existed
//! nothing said it should. **A build that silently ships fallbacks is the
//! failure mode this layer exists to prevent.**
//!
//! Three claims:
//!
//! 1. **The ambiguity is real**, so it is demonstrated rather than described:
//!    `run_to_completion` answers `Ok` identically for a graph that computed its
//!    value and one that substituted it, and nothing in the returned value
//!    tells them apart.
//! 2. **The count resolves it and changes nothing else.** Same drive, same
//!    result, one more observation - the shape
//!    [`run_to_completion_watched`](libpipeline_internals::watch::run_to_completion_watched)
//!    already took for wake paths.
//! 3. **One tally covers the graph.** A boundary counts what it substituted, so
//!    the outermost one cannot answer for a scope further in that recovered on
//!    its own; sharing a [`Substitutions`] is what makes the number the build's.
//!
//! **Every type here is a stand-in** (`DESIGN.md`, "The engine stays
//! generic").

use std::sync::Mutex;
use std::task::{Context, Waker};

use libeffects::Fallback;
use libpipeline_internals::chain::Chain;
use libpipeline_internals::boundary::Guarded;
use libpipeline_internals::driver::NoPendingWork;
use libpipeline_internals::boundary::Substitutions;
use libpipeline_internals::driver::run_to_completion;
use libpipeline_internals::boundary::run_to_completion_counted;
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
    const ID: StageId = StageId::at(0);

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
    const ID: StageId = StageId::at(1);

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

const GUARD: StageId = StageId::at(2);
const CHAIN: StageId = StageId::at(3);

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
         completes would break the two-driver rule in order to report on it",
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
    // The two-driver rule forbids giving the two drivers different return types, and this is
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
    let driver = libpipeline_internals::driver::FrameDriver::new();
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
