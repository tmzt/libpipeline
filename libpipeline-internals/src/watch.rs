//! Watching a poll for the wake it owes.
//!
//! **The defect this makes visible.** `Effect::poll_effect`'s doc makes the
//! obligation explicit: a poll that answers `Pending` MUST have arranged for
//! `cx`'s waker to be woken, "otherwise the frame that drew a stand-in is never
//! told to redraw and the value is lost rather than late". What breaking it
//! costs is measured, and worse, WHERE: a stage that forgets is fatal to the
//! frame driver and INVISIBLE to the blocking one, because the offline driver
//! re-polls without being asked
//! (`libpipeline/tests/one_door_two_patterns.rs`'s
//! `a_pending_stage_that_registers_no_waker_is_a_value_lost_rather_than_late`).
//! A defect that only the harder-to-run driver can find is a defect that ships.
//!
//! **How it is caught, in safe code.** The waker handed to the poll is built
//! from an `Arc` this module holds. Registering a waker means CLONING it -
//! there is no other way to keep one past the poll - and cloning a
//! `Waker` built by [`Waker::from`] over an `Arc` increments that `Arc`'s
//! strong count. So the count, read after the poll returns, says whether a
//! clone was left behind. `forwards_a_registered_wake_rather_than_swallowing_it`
//! and `a_stage_that_registers_is_not_reported` pin that mechanism against std
//! rather than assuming it.
//!
//! **What it cannot say is which stage.** The measurement is per POLL, so on a
//! composed graph it reports that the poll as a whole left no wake path, not
//! which half owed it. Narrowing is bisection: drive a subgraph and watch that.
//! No per-node version is offered here, because it would mean a fresh waker per
//! node per frame in the frame drive's hot path for a diagnostic.

use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Wake, Waker};

use libpipelinedata::{EffectPoll, StageAnswer};

use crate::Stage;

use crate::driver::{DriveError, PendingWork};

/// What a `Pending` poll left behind for whoever lands the value.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum WakePath {
    /// A clone of the waker outlived the poll. Something can wake this.
    ///
    /// It does NOT say the clone was registered somewhere useful - a stage that
    /// stashes a waker on a slot nothing ever fills is indistinguishable from
    /// one that stashes it correctly, and no mechanical check reaches that.
    Registered,
    /// The poll woke the waker before returning `Pending` - a yield rather than
    /// a park. Legitimate, and reported separately so it is not confused with
    /// the defect.
    Woken,
    /// Neither. Nothing can wake this poll: the value is lost rather than late.
    Missing,
}

/// The wake target a watched poll hands out.
#[derive(Debug)]
struct Probe {
    forward: Waker,
    woken: Mutex<bool>,
}

impl Wake for Probe {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        *self.woken.lock().unwrap_or_else(PoisonError::into_inner) = true;
        self.forward.wake_by_ref();
    }
}

/// Poll `stage`, and report what a `Pending` answer left behind.
///
/// `wake` is the waker the caller would have passed anyway - [`Waker::noop`]
/// for the offline driver, a frame loop's waker for the real-time one. Every
/// wake reaching the probe is passed straight on to it, so watching a poll
/// cannot turn a working graph into a stalled one. That property is the
/// difference between a diagnostic and a second driver, and it is gated.
///
/// `None` is returned for a poll that did not answer `Pending`: there is no
/// wake to owe.
pub fn poll_watched<S: Stage>(
    stage: &S,
    input: &S::Input,
    wake: &Waker,
) -> (
    EffectPoll<StageAnswer<Arc<S::Output>>, S::Error>,
    Option<WakePath>,
) {
    let probe = Arc::new(Probe {
        forward: wake.clone(),
        woken: Mutex::new(false),
    });
    // Two references exist now: `probe`, and the one inside the waker. A clone
    // taken by the stage and kept is a third.
    let waker = Waker::from(Arc::clone(&probe));
    let polled = stage.poll_stage(input, &mut Context::from_waker(&waker));
    let held = Arc::strong_count(&probe);
    let woken = *probe.woken.lock().unwrap_or_else(PoisonError::into_inner);
    drop(waker);

    if !polled.is_pending() {
        return (polled, None);
    }
    let path = if woken {
        WakePath::Woken
    } else if held > 2 {
        WakePath::Registered
    } else {
        WakePath::Missing
    };
    (polled, Some(path))
}

/// What the polls of one watched drive owed and left.
///
/// Counted per poll rather than collected per node - see the module doc for
/// why there is no node here to name.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Default)]
pub struct WakeReport {
    pending: usize,
    unwakeable: usize,
}

impl WakeReport {
    /// How many polls answered `Pending`.
    pub fn pending_polls(&self) -> usize {
        self.pending
    }

    /// How many of those left no wake path at all - each one a value that
    /// would be lost rather than late under a frame driver.
    pub fn unwakeable_polls(&self) -> usize {
        self.unwakeable
    }

    /// Nothing owed a wake and failed to leave one.
    pub fn is_clean(&self) -> bool {
        self.unwakeable == 0
    }
}

/// [`run_to_completion`](crate::driver::run_to_completion), reporting what its
/// `Pending` polls left behind.
///
/// **The answer is not affected, and that is the point.** This drives exactly
/// as the offline driver does and returns exactly what it returns - including
/// for a graph whose stages forget, which the offline driver reaches a value
/// for regardless. The two-driver rule's claim is that a stage cannot tell
/// which driver is polling it, and a driver that failed a graph the plain one completes would
/// break that claim in order to report on it. So the finding rides ALONGSIDE
/// the result, in the same shape `NoMemo` gives the memo layer: the control and
/// the real thing must agree on answers and differ only in what they observe.
///
/// Which makes this the batch tool's answer to a defect it could not
/// previously see: run the offline driver, read the report, and a stage that
/// would strand a frame drive is counted before it gets there.
pub fn run_to_completion_watched<S, W>(
    stage: &S,
    input: &S::Input,
    work: &W,
) -> (
    Result<StageAnswer<Arc<S::Output>>, DriveError<S::Error>>,
    WakeReport,
)
where
    S: Stage,
    W: PendingWork + ?Sized,
{
    let mut report = WakeReport::default();
    loop {
        let (polled, path) = poll_watched(stage, input, Waker::noop());
        match polled {
            EffectPoll::Ready(value) => return (Ok(value), report),
            EffectPoll::Failed(e) => return (Err(DriveError::Failed(e)), report),
            EffectPoll::Pending => {
                report.pending += 1;
                if path == Some(WakePath::Missing) {
                    report.unwakeable += 1;
                }
                if !work.run_once() {
                    return (Err(DriveError::Stalled), report);
                }
            }
        }
    }
}
