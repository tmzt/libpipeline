//! Gate: **a `Pending` poll that leaves no wake path is detectable from a
//! blocking drive.**
//!
//! The finding this drive exists to close: a `Pending` stage that registers
//! no waker is invisible to a caller that loops and re-polls, and fatal to a
//! frame caller - the value is lost rather than late, and the looping path
//! cannot detect it (`libpipeline/tests/one_door_two_patterns.rs`'s
//! `a_pending_stage_that_registers_no_waker_is_a_value_lost_rather_than_late`
//! measures exactly that). This file is that sentence's last clause tested
//! again with the watching drive in place, and it now fails: the drive reaches
//! the same value it always did AND reports the defect.
//!
//! **Two claims, and they pull against each other, so both are gated.**
//!
//! 1. The report is accurate: a graph that registers reports clean, one that
//!    forgets is counted.
//! 2. Watching changes nothing else. Same answers as the plain drive on the
//!    same graphs - a diagnostic that could stall a working graph would be
//!    worse than the defect it looks for.
//!
//! # Why this file is here rather than in `libpipeline/tests/`
//!
//! It was a public test until the flip. The runner has ONE door now -
//! `run(version, &input)`, a single poll - and no watched door beside it, so
//! `run_to_completion_watched` has no public expression and neither do the
//! properties below. `DESIGN.md`'s rule is that a test which has to reach the
//! internals is a finding about the builder's reach, recorded and left visible,
//! rather than papered over with a re-export - and a test in this crate is how
//! it stays visible. `tests.rs` beside it holds the finer, per-POLL half for
//! the same reason.
//!
//! The plan for `Delayed`'s promise (`PLAN.md`, step 6) is the debug-build
//! check `run` itself will make through `poll_watched`; when that lands, the
//! drive-level properties here stay where they are, because the drive is still
//! not a door.
//!
//! **Every type here is a stand-in** (`DESIGN.md`, "The engine stays
//! generic").

use std::sync::{Arc, Mutex};
use std::task::{Context, Waker};

use libpipelinedata::{EffectPoll, MemoKey, StageAnswer, StageId};
use libpipeline_internals::{Stage};

use libpipeline_internals::driver::{DriveError, NoPendingWork, PendingWork};
use libpipeline_internals::watch::run_to_completion_watched;

// ---------------------------------------------------------------- stand-ins

/// Stand-in for whatever an author wrote.
#[derive(Clone, PartialEq, Eq, Debug)]
struct Text(String);

/// What a stage does with the waker it is handed, on the poll where it parks.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum OnPark {
    /// Stash a clone, as `Effect::poll_effect`'s doc obliges.
    Register,
    /// Nothing at all - `one_door_two_patterns.rs`'s `ForgetfulEmit`.
    Forget,
}

/// A stage that is `Pending` until its slot is filled, and treats the waker
/// according to `on_park`.
struct Parks {
    id: StageId,
    on_park: OnPark,
    slot: Mutex<Option<&'static str>>,
    waiting: Mutex<Vec<Waker>>,
}

impl Parks {
    fn new(on_park: OnPark) -> Arc<Self> {
        Arc::new(Self {
            // The position a builder would have minted for a single
            // registration. Nothing here is keyed, so it is only an identity.
            id: StageId::at(0),
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
        self.id
    }

    fn memo_key(&self, _input: &Text) -> Option<MemoKey> {
        None
    }

    fn poll_stage(&self, input: &Text, cx: &mut Context<'_>) -> EffectPoll<StageAnswer<Arc<String>>, &'static str> {
        let Some(landed) = *self.slot.lock().unwrap() else {
            match self.on_park {
                OnPark::Register => self.waiting.lock().unwrap().push(cx.waker().clone()),
                OnPark::Forget => {}
            }
            return EffectPoll::Pending;
        };
        StageAnswer::computed(Arc::new(format!("{}::{landed}", input.0)))
    }
}

/// The blocking caller's executor: it lands the slot the first time it is
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

// --------------------------------------------------------------------- gate

#[test]
fn the_blocking_drive_reports_the_defect_without_changing_its_answer() {
    // The lost-wake finding, closed. The same graph, the same drive, the same
    // value - and now the run says that a frame caller would have lost it.
    let stage = Parks::new(OnPark::Forget);
    let input = Text("hi".to_string());
    let (out, report) =
        run_to_completion_watched(&stage, &input, &LandsOnFirstPump::for_stage(&stage));

    assert_eq!(
        out,
        Ok(StageAnswer::Computed(Arc::new("hi::built".to_string()))),
        "the drive still completes, because it re-polls without being asked - \
         that is what made the defect invisible",
    );
    assert_eq!(report.pending_polls(), 1);
    assert_eq!(report.unwakeable_polls(), 1);
    assert!(!report.is_clean());

    // And the plain drive agrees on the answer, which is the claim: the
    // watching is an observation, not a different drive.
    let plain_stage = Parks::new(OnPark::Forget);
    assert_eq!(
        libpipeline_internals::driver::run_to_completion(
            &plain_stage,
            &input,
            &LandsOnFirstPump::for_stage(&plain_stage),
        ),
        out,
    );
}

#[test]
fn a_graph_that_registers_reports_clean() {
    let stage = Parks::new(OnPark::Register);
    let (out, report) = run_to_completion_watched(
        &stage,
        &Text("hi".to_string()),
        &LandsOnFirstPump::for_stage(&stage),
    );
    assert_eq!(out, Ok(StageAnswer::Computed(Arc::new("hi::built".to_string()))));
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
        run_to_completion_watched(&forgetful, &Text("hi".to_string()), &NoPendingWork);
    assert_eq!(out, Err(DriveError::Stalled));
    assert_eq!(report.unwakeable_polls(), 1);

    let registering = Parks::new(OnPark::Register);
    let (out, report) =
        run_to_completion_watched(&registering, &Text("hi".to_string()), &NoPendingWork);
    assert_eq!(
        out,
        Err(DriveError::Stalled),
        "the same end state, reached for a different reason",
    );
    assert!(report.is_clean());
}
