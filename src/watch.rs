//! Watching a poll for the wake it owes.
//!
//! **The defect this makes visible.** `Effect::poll_effect`'s doc makes the
//! obligation explicit: a poll that answers `Pending` MUST have arranged for
//! `cx`'s waker to be woken, "otherwise the frame that drew a stand-in is never
//! told to redraw and the value is lost rather than late". What breaking it
//! costs is measured, and worse, WHERE: a stage that forgets is fatal to the
//! frame driver and INVISIBLE to the blocking one, because the offline driver
//! re-polls without being asked
//! (`tests/two_drivers_one_graph.rs`'s
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

use libpipelinedata::{EffectPoll, Stage};

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
pub(crate) fn poll_watched<S: Stage>(
    stage: &S,
    input: &S::Input,
    wake: &Waker,
) -> (EffectPoll<S::Output, S::Error>, Option<WakePath>) {
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
pub(crate) fn run_to_completion_watched<S, W>(
    stage: &S,
    input: &S::Input,
    work: &W,
) -> (Result<S::Output, DriveError<S::Error>>, WakeReport)
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

#[cfg(test)]
mod tests {
    //! **The per-POLL half of `an_unwakeable_poll_is_visible_offline.rs`**,
    //! hosted here because [`poll_watched`] is internal.
    //!
    //! `Pipeline::run_watched` is the public door onto the watched DRIVE, and
    //! the three drive-level properties are gated through it in `tests/`. These
    //! six are finer than a drive: they read the [`WakePath`] of a single poll,
    //! which is what tells a stage that registers apart from one that forgets
    //! and both apart from one that yields. The runner has no watched
    //! single-poll door (`Pipeline::poll_frame` is unwatched), so there is no
    //! public expression for them - `DESIGN.md`'s rule is that a test needing
    //! internals is a finding about the builder's reach, recorded rather than
    //! papered over with a re-export.
    //!
    //! They migrate outward to `tests/` unchanged, minus the `poll_watched`
    //! call, the day the runner grows one.
    //!
    //! **Every type here is a stand-in** (`DESIGN.md`, "The engine stays
    //! generic").

    use std::sync::{Arc, Mutex};
    use std::task::{Context, Waker};

    use libeffects::WakeFlag;
    use libpipelinedata::{EffectPoll, MemoKey, Stage, StageId};

    use super::{WakePath, poll_watched};

    /// Stand-in for whatever an author wrote.
    #[derive(Clone, PartialEq, Eq, Debug)]
    struct Text(String);

    /// What a stage does with the waker it is handed, on the poll where it
    /// parks.
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
        /// Nothing at all - `two_drivers_one_graph.rs`'s `ForgetfulEmit`.
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

        /// The value arrives, and everyone who parked on it is told to poll
        /// again.
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

        fn poll_stage(
            &self,
            input: &Text,
            cx: &mut Context<'_>,
        ) -> EffectPoll<String, &'static str> {
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

    fn watch(on_park: OnPark) -> Option<WakePath> {
        let stage = Parks::new(on_park);
        let (polled, path) = poll_watched(&*stage, &Text("hi".to_string()), Waker::noop());
        assert!(polled.is_pending(), "the slot is empty, so this parks");
        path
    }

    #[test]
    fn a_stage_that_forgets_the_waker_is_reported() {
        assert_eq!(watch(OnPark::Forget), Some(WakePath::Missing));
    }

    #[test]
    fn a_stage_that_registers_is_not_reported() {
        // This is also the check on the mechanism itself: it holds only because
        // cloning a Waker built from an Arc increments that Arc's strong count.
        // If std ever stopped doing that, this test fails rather than the gate
        // quietly reporting every stage as broken.
        assert_eq!(watch(OnPark::Register), Some(WakePath::Registered));
    }

    #[test]
    fn a_clone_that_does_not_outlive_the_poll_is_reported() {
        // Registering into somewhere that does not outlive the poll is the same
        // defect with a clone in it, and it is caught for the same reason: what
        // is measured is what was KEPT, not what was taken.
        assert_eq!(watch(OnPark::RegisterAndDrop), Some(WakePath::Missing));
    }

    #[test]
    fn a_yield_is_told_apart_from_a_park() {
        // Waking before returning Pending is a legitimate "poll me again" and
        // must not be reported as the defect - a diagnostic with a false
        // positive in it gets switched off.
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
        // The property that makes this a diagnostic rather than a second
        // driver: the probe is a waker in front of the caller's, not instead of
        // it. A watched frame loop must still be woken.
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
}
