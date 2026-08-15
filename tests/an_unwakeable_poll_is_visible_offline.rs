//! Gate: **a `Pending` poll that leaves no wake path is detectable from the
//! offline driver** (`PIPELINE_PLAN.md` §3, §5).
//!
//! Step 1's finding, in its own words: a `Pending` stage that registers no
//! waker is "invisible to the blocking/CLI driver and fatal to the frame/IDE
//! driver - the value is lost rather than late, and the offline path cannot
//! detect it". This file is that sentence's last clause tested again with the
//! watching driver in place, and it now fails: the CLI run reaches the same
//! value it always did AND reports the defect.
//!
//! **Two claims, and they pull against each other, so both are gated.**
//!
//! 1. The report is accurate: a stage that registers is not named, one that
//!    forgets is, and one that yields by waking before returning `Pending` is
//!    told apart from both.
//! 2. Watching changes nothing else. Same answers as the plain driver on the
//!    same graphs, and a wake that arrives through the probe is passed on
//!    rather than swallowed - a diagnostic that could stall a working graph
//!    would be worse than the defect it looks for.
//!
//! **Every type here is a stand-in** (`PIPELINE_PLAN.md`:563-568).

use std::sync::{Arc, Mutex};
use std::task::{Context, Waker};

use libeffects::WakeFlag;
use libpipeline::{
    DriveError, NoPendingWork, PendingWork, WakePath, poll_watched, run_to_completion,
    run_to_completion_watched,
};
use libpipelinedata::{EffectPoll, MemoKey, Stage, StageId};

// ---------------------------------------------------------------- stand-ins

/// Stand-in for whatever an author wrote.
#[derive(Clone, PartialEq, Eq, Debug)]
struct Text(String);

/// What a stage does with the waker it is handed, on the poll where it parks.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum OnPark {
    /// Stash a clone, as `Effect::poll_effect`'s doc obliges.
    Register,
    /// Take a clone and drop it before returning - registering into a place
    /// that does not outlive the poll, which is the same defect wearing a
    /// clone.
    RegisterAndDrop,
    /// Wake before returning `Pending`: a yield, not a park.
    Yield,
    /// Nothing at all - step 1's `ForgetfulEmit`.
    Forget,
}

/// A stage that is `Pending` until its slot is filled, and treats the waker
/// according to `on_park`.
struct Parks {
    on_park: OnPark,
    slot: Mutex<Option<&'static str>>,
    waiting: Mutex<Vec<Waker>>,
}

impl Parks {
    fn new(on_park: OnPark) -> Arc<Self> {
        Arc::new(Self {
            on_park,
            slot: Mutex::new(None),
            waiting: Mutex::new(Vec::new()),
        })
    }

    /// The value arrives, and everyone who parked on it is told to poll again.
    fn land(&self, value: &'static str) {
        *self.slot.lock().unwrap() = Some(value);
        for waker in self.waiting.lock().unwrap().drain(..) {
            waker.wake();
        }
    }
}

impl Stage for Parks {
    type Input = Text;
    type Output = String;
    type Error = &'static str;

    fn id(&self) -> StageId {
        StageId::new("test.parks", 1)
    }

    fn memo_key(&self, _input: &Text) -> Option<MemoKey> {
        None
    }

    fn poll_stage(&self, input: &Text, cx: &mut Context<'_>) -> EffectPoll<String, &'static str> {
        let Some(landed) = *self.slot.lock().unwrap() else {
            match self.on_park {
                OnPark::Register => self.waiting.lock().unwrap().push(cx.waker().clone()),
                OnPark::RegisterAndDrop => drop(cx.waker().clone()),
                OnPark::Yield => cx.waker().wake_by_ref(),
                OnPark::Forget => {}
            }
            return EffectPoll::Pending;
        };
        EffectPoll::Ready(format!("{}::{landed}", input.0))
    }
}

/// The blocking driver's executor: it lands the slot the first time it is
/// pumped, then has nothing left to do.
struct LandsOnFirstPump {
    stage: Arc<Parks>,
    landed: Mutex<bool>,
}

impl LandsOnFirstPump {
    fn for_stage(stage: &Arc<Parks>) -> Self {
        Self {
            stage: Arc::clone(stage),
            landed: Mutex::new(false),
        }
    }
}

impl PendingWork for LandsOnFirstPump {
    fn run_once(&self) -> bool {
        let mut landed = self.landed.lock().unwrap();
        if *landed {
            return false;
        }
        *landed = true;
        self.stage.land("built");
        true
    }
}

fn watch(on_park: OnPark) -> Option<WakePath> {
    let stage = Parks::new(on_park);
    let (polled, path) = poll_watched(&*stage, &Text("hi".to_string()), Waker::noop());
    assert!(polled.is_pending(), "the slot is empty, so this parks");
    path
}

// --------------------------------------------------------------------- gate

#[test]
fn a_stage_that_forgets_the_waker_is_reported() {
    assert_eq!(watch(OnPark::Forget), Some(WakePath::Missing));
}

#[test]
fn a_stage_that_registers_is_not_reported() {
    // This is also the check on the mechanism itself: it holds only because
    // cloning a Waker built from an Arc increments that Arc's strong count. If
    // std ever stopped doing that, this test fails rather than the gate quietly
    // reporting every stage as broken.
    assert_eq!(watch(OnPark::Register), Some(WakePath::Registered));
}

#[test]
fn a_clone_that_does_not_outlive_the_poll_is_reported() {
    // Registering into somewhere that does not outlive the poll is the same
    // defect with a clone in it, and it is caught for the same reason: what is
    // measured is what was KEPT, not what was taken.
    assert_eq!(watch(OnPark::RegisterAndDrop), Some(WakePath::Missing));
}

#[test]
fn a_yield_is_told_apart_from_a_park() {
    // Waking before returning Pending is a legitimate "poll me again" and must
    // not be reported as the defect - a diagnostic with a false positive in it
    // gets switched off.
    assert_eq!(watch(OnPark::Yield), Some(WakePath::Woken));
}

#[test]
fn a_poll_that_produced_a_value_owes_no_wake() {
    let stage = Parks::new(OnPark::Forget);
    stage.land("built");
    let (polled, path) = poll_watched(&*stage, &Text("hi".to_string()), Waker::noop());
    assert_eq!(polled, EffectPoll::Ready("hi::built".to_string()));
    assert_eq!(path, None);
}

#[test]
fn forwards_a_registered_wake_rather_than_swallowing_it() {
    // The property that makes this a diagnostic rather than a second driver:
    // the probe is a waker in front of the caller's, not instead of it. A
    // watched frame loop must still be woken.
    let flag = WakeFlag::new();
    let stage = Parks::new(OnPark::Register);

    let (polled, path) = poll_watched(&*stage, &Text("hi".to_string()), &flag.waker());
    assert!(polled.is_pending());
    assert_eq!(path, Some(WakePath::Registered));
    assert!(!flag.is_stale(), "a Pending poll is not itself a wake");

    stage.land("built");
    assert!(
        flag.take_stale(),
        "the wake travelled through the probe to the frame loop",
    );
}

#[test]
fn the_offline_driver_reports_the_defect_without_changing_its_answer() {
    // Step 1's finding, closed. The same graph, the same drive, the same value
    // - and now the run says that a frame driver would have lost it.
    let stage = Parks::new(OnPark::Forget);
    let (out, report) = run_to_completion_watched(
        &*stage,
        &Text("hi".to_string()),
        &LandsOnFirstPump::for_stage(&stage),
    );

    assert_eq!(
        out,
        Ok("hi::built".to_string()),
        "the offline driver still completes, because it re-polls without \
         being asked - that is what made the defect invisible",
    );
    assert_eq!(report.pending_polls(), 1);
    assert_eq!(report.unwakeable_polls(), 1);
    assert!(!report.is_clean());

    // And the plain driver agrees on the answer, which is §5's claim: the
    // watching is an observation, not a different drive.
    let plain_stage = Parks::new(OnPark::Forget);
    assert_eq!(
        run_to_completion(
            &*plain_stage,
            &Text("hi".to_string()),
            &LandsOnFirstPump::for_stage(&plain_stage),
        ),
        out,
    );
}

#[test]
fn a_graph_that_registers_reports_clean() {
    let stage = Parks::new(OnPark::Register);
    let (out, report) = run_to_completion_watched(
        &*stage,
        &Text("hi".to_string()),
        &LandsOnFirstPump::for_stage(&stage),
    );
    assert_eq!(out, Ok("hi::built".to_string()));
    assert_eq!(report.pending_polls(), 1, "it did park once");
    assert!(report.is_clean(), "and left a wake path when it did");
}

#[test]
fn a_stalled_graph_still_stalls_and_says_why() {
    // Nothing to pump, so the drive ends where the plain one ends - and the
    // report distinguishes the two reasons a drive can stall: an effect that
    // never lands (clean) from a stage that could never have been woken.
    let forgetful = Parks::new(OnPark::Forget);
    let (out, report) =
        run_to_completion_watched(&*forgetful, &Text("hi".to_string()), &NoPendingWork);
    assert_eq!(out, Err(DriveError::Stalled));
    assert_eq!(report.unwakeable_polls(), 1);

    let registering = Parks::new(OnPark::Register);
    let (out, report) =
        run_to_completion_watched(&*registering, &Text("hi".to_string()), &NoPendingWork);
    assert_eq!(
        out,
        Err(DriveError::Stalled),
        "the same end state, reached for a different reason",
    );
    assert!(report.is_clean());
}
