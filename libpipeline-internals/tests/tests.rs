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

use libpipeline_internals::watch::{WakePath, poll_watched};

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
