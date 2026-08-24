//! The two drivers.
//!
//! **Same stages, same keys, different driver** - that is the whole claim of
//! the two-driver rule (`DESIGN.md`, "Two drivers, one graph"), and this
//! module is where it either holds or does not. Both drivers below
//! take an arbitrary `S: Stage` and touch nothing else; neither has a method
//! the other's stages would need. A stage cannot tell which one is polling it,
//! and that is the property that makes an interactive host and a batch tool
//! one API rather than two implementations that agree by convention.

use std::sync::Arc;
use std::task::{Context, Waker};

use libeffects::WakeFlag;
use libpipelinedata::{EffectPoll, Stage};

/// How a blocking drive ended other than with a value.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum DriveError<E> {
    /// The stage's own typed error channel.
    Failed(E),
    /// The graph answered `Pending` and there was no outstanding work left to
    /// run, so re-polling could only produce `Pending` again.
    ///
    /// This is a real end state, not a timeout: it means something registered a
    /// waker for an input nothing was ever going to land. Offline, that is a
    /// bug in the graph; under a frame drive the same situation is normal and simply
    /// means the frame keeps its stand-in until a user action lands the value.
    Stalled,
}

/// What the blocking driver pumps when a poll answers `Pending`.
///
/// The blocking half of the contract: when `Pending`, run the pending
/// effect's work on a plain executor and poll again. This trait is that
/// executor's seam, kept abstract because which executor - and whether there
/// is a real one at all - is the caller's, and because a real one must never
/// be linked by the engine.
pub trait PendingWork {
    /// Make progress on something a `Pending` poll is waiting for.
    ///
    /// Returns `false` when there is nothing left to run. Termination is then
    /// the graph's: the DAG's acyclicity plus effect completion. An implementation that
    /// returns `true` forever will loop forever, and no budget here would make
    /// that correct - it would only turn a hang into a wrong answer.
    fn run_once(&self) -> bool;
}

/// Nothing to pump: for a graph of pure stages, where `Pending` can only mean
/// the graph is stalled.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoPendingWork;

impl PendingWork for NoPendingWork {
    fn run_once(&self) -> bool {
        false
    }
}

/// The offline driver: poll until the graph produces a value (the blocking
/// half of the two-driver rule).
///
/// **The waker is deliberately a no-op** - a waker of no consequence, because
/// there is no frame to keep alive. The loop re-polls unconditionally
/// after pumping, so nothing depends on being woken - and a stage that registers
/// [`Waker::noop`] and is never woken behaves identically to one that is. That
/// the same stage works under both drivers *because* it cannot observe the
/// difference is the claim, not a coincidence to be re-checked per stage.
///
/// A batch run against an unchanged tree is all cache hits, because the memo
/// keys are the same ones the interactive host used.
pub(crate) fn run_to_completion<S, W>(
    stage: &S,
    input: &S::Input,
    work: &W,
) -> Result<S::Output, DriveError<S::Error>>
where
    S: Stage,
    W: PendingWork + ?Sized,
{
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    loop {
        match stage.poll_stage(input, &mut cx) {
            EffectPoll::Ready(value) => return Ok(value),
            EffectPoll::Failed(e) => return Err(DriveError::Failed(e)),
            EffectPoll::Pending => {
                if !work.run_once() {
                    return Err(DriveError::Stalled);
                }
            }
        }
    }
}

/// The real-time driver: poll once per frame and return whatever the graph says
/// (the interactive half of the two-driver rule).
///
/// **Nothing waits inside a frame, ever.** A poll that answers `Pending` is a
/// frame that draws its stand-in and returns; the waker left behind schedules
/// the redraw; the next frame polls and gets `Ready`. There is no method here
/// that blocks, which is the mechanical form of that rule.
///
/// **On a single-threaded host the wake target is an event-loop message.** In
/// a browser or windowing event loop, the loop itself is the only pump, and an
/// async result re-enters it by posting an event to a proxy. This driver holds
/// a [`WakeFlag`] instead, which records staleness without knowing how a
/// redraw is requested - so such a host is this driver plus a proxy send
/// beside the flag, not a different driver.
pub(crate) struct FrameDriver {
    stale: Arc<WakeFlag>,
}

impl Default for FrameDriver {
    fn default() -> Self {
        Self::new()
    }
}

impl FrameDriver {
    /// A driver with a fresh, unstale flag.
    pub(crate) fn new() -> Self {
        Self {
            stale: WakeFlag::new(),
        }
    }

    /// The waker this driver hands to every poll. Exposed so whatever lands a
    /// value out of band can wake the frame loop directly.
    pub(crate) fn waker(&self) -> Waker {
        self.stale.waker()
    }

    /// Whether a wake has arrived since this was last called - "stale, poll
    /// again". Reading clears it.
    pub(crate) fn take_stale(&self) -> bool {
        self.stale.take_stale()
    }

    /// Poll the graph once. Returns immediately, whatever the answer.
    pub(crate) fn poll_frame<S: Stage>(
        &self,
        stage: &S,
        input: &S::Input,
    ) -> EffectPoll<S::Output, S::Error> {
        let waker = self.stale.waker();
        stage.poll_stage(input, &mut Context::from_waker(&waker))
    }
}
