//! Gate: **a `Pending` poll that leaves no wake path is detectable from the
//! offline driver.**
//!
//! The finding this driver exists to close: a `Pending` stage that registers
//! no waker is invisible to the blocking driver and fatal to the frame driver
//! - the value is lost rather than late, and the offline path cannot detect
//! it (`tests/two_drivers_one_graph.rs` measures exactly that). This file is
//! that sentence's last clause tested again with the watching driver in
//! place, and it now fails: the blocking run reaches the same value it always
//! did AND reports the defect.
//!
//! **Two claims, and they pull against each other, so both are gated.**
//!
//! 1. The report is accurate: a graph that registers reports clean, one that
//!    forgets is counted.
//! 2. Watching changes nothing else. Same answers as the plain driver on the
//!    same graphs - a diagnostic that could stall a working graph would be
//!    worse than the defect it looks for.
//!
//! # What is here and what is in `src/watch.rs`
//!
//! `Pipeline::run_watched` is the public door onto the watched drive, so the
//! three DRIVE-level properties are here, through the builder. The finer
//! measurements - [`WakePath`](libpipeline::WakePath) per poll, telling a yield
//! apart from a park, and the probe forwarding a wake rather than swallowing it
//! - are made by `poll_watched`, which is a single poll and is internal: the
//! runner has no watched single-poll door (`Pipeline::poll_frame` is unwatched).
//! Those six tests live beside `poll_watched` in `src/watch.rs` and migrate back
//! here if the runner ever grows one.
//!
//! **Every type here is a stand-in** (`DESIGN.md`, "The engine stays
//! generic").

use std::sync::{Arc, Mutex};
use std::task::{Context, Waker};

use libpipeline::{DriveError, PendingWork, Pipeline, PipelineBuilder};
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
    /// Nothing at all - `two_drivers_one_graph.rs`'s `ForgetfulEmit`.
    Forget,
}

/// A stage that is `Pending` until its slot is filled, and treats the waker
/// according to `on_park`.
struct Parks {
    id: Mutex<Option<StageId>>,
    on_park: OnPark,
    slot: Mutex<Option<&'static str>>,
    waiting: Mutex<Vec<Waker>>,
}

impl Parks {
    fn new(on_park: OnPark) -> Arc<Self> {
        Arc::new(Self {
            id: Mutex::new(None),
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
            .lock()
            .unwrap()
            .expect("the builder minted this stage's id at registration")
    }

    fn memo_key(&self, _input: &Text) -> Option<MemoKey> {
        None
    }

    fn poll_stage(&self, input: &Text, cx: &mut Context<'_>) -> EffectPoll<String, &'static str> {
        let Some(landed) = *self.slot.lock().unwrap() else {
            match self.on_park {
                OnPark::Register => self.waiting.lock().unwrap().push(cx.waker().clone()),
                OnPark::Forget => {}
            }
            return EffectPoll::Pending;
        };
        EffectPoll::Ready(format!("{}::{landed}", input.0))
    }
}

/// The stage, and the pipeline it is registered in.
///
/// The stage is handed back as well as registered because the executor below
/// has to be able to LAND its value, and the builder owns what it registers -
/// there is no reaching back through `Pipeline` for it. The id is stamped into
/// the stage by the builder's `make`, so `Stage::id` answers the id it was
/// registered under rather than a constant that could drift from it.
fn parking(
    on_park: OnPark,
) -> (
    Arc<Parks>,
    Pipeline<impl Stage<Input = Text, Output = String, Error = &'static str>>,
) {
    let stage = Parks::new(on_park);
    let registered = Arc::clone(&stage);
    let pipeline = PipelineBuilder::new()
        .stage("test.parks", 1, move |id| {
            *registered.id.lock().unwrap() = Some(id);
            registered
        })
        .build();
    (stage, pipeline)
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

// --------------------------------------------------------------------- gate

#[test]
fn the_offline_driver_reports_the_defect_without_changing_its_answer() {
    // The lost-wake finding, closed. The same graph, the same drive, the same
    // value - and now the run says that a frame driver would have lost it.
    let (stage, pipeline) = parking(OnPark::Forget);
    let (out, report) = pipeline.run_watched(&Text("hi".to_string()), &LandsOnFirstPump::for_stage(&stage));

    assert_eq!(
        out,
        Ok("hi::built".to_string()),
        "the offline driver still completes, because it re-polls without \
         being asked - that is what made the defect invisible",
    );
    assert_eq!(report.pending_polls(), 1);
    assert_eq!(report.unwakeable_polls(), 1);
    assert!(!report.is_clean());

    // And the plain driver agrees on the answer, which is the two-driver
    // rule's claim: the watching is an observation, not a different drive.
    let (plain_stage, plain) = parking(OnPark::Forget);
    assert_eq!(
        plain.run(
            &Text("hi".to_string()),
            &LandsOnFirstPump::for_stage(&plain_stage),
        ),
        out,
    );
}

#[test]
fn a_graph_that_registers_reports_clean() {
    let (stage, pipeline) = parking(OnPark::Register);
    let (out, report) = pipeline.run_watched(&Text("hi".to_string()), &LandsOnFirstPump::for_stage(&stage));
    assert_eq!(out, Ok("hi::built".to_string()));
    assert_eq!(report.pending_polls(), 1, "it did park once");
    assert!(report.is_clean(), "and left a wake path when it did");
}

#[test]
fn a_stalled_graph_still_stalls_and_says_why() {
    // Nothing to pump, so the drive ends where the plain one ends - and the
    // report distinguishes the two reasons a drive can stall: an effect that
    // never lands (clean) from a stage that could never have been woken.
    let (_forgetful, pipeline) = parking(OnPark::Forget);
    let (out, report) = pipeline.run_watched(&Text("hi".to_string()), &libpipeline::NoPendingWork);
    assert_eq!(out, Err(DriveError::Stalled));
    assert_eq!(report.unwakeable_polls(), 1);

    let (_registering, pipeline) = parking(OnPark::Register);
    let (out, report) = pipeline.run_watched(&Text("hi".to_string()), &libpipeline::NoPendingWork);
    assert_eq!(
        out,
        Err(DriveError::Stalled),
        "the same end state, reached for a different reason",
    );
    assert!(report.is_clean());
}
