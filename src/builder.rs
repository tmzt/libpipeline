//! The builder: the one public door onto composition, memoization and driving.
//!
//! See `DESIGN.md`. Three rules are enforced here rather than remembered:
//!
//! * **Memoization is intrinsic to registration.** Every stage registered
//!   through [`PipelineBuilder::stage`] is wrapped in the memo layer; there is
//!   no un-memoized registration to forget. A stage that must not be served
//!   from cache says so through `memo_key -> None`.
//! * **The version is declared at the call site.** `stage(name, version,
//!   make)` mints the [`StageId`] and hands it to `make`, so the number lives
//!   in the same lexical scope as the behaviour it versions.
//! * **A registered stage answers the id it was registered under.** Checked at
//!   registration; a mismatch panics at construction rather than serving stale
//!   values at poll time.

use std::task::Waker;

use libpipelinedata::{EffectPoll, MemoKey, MemoMap, MemoStore, Stage, StageId};

use crate::chain::{Chain, ChainError};
use crate::driver::{DriveError, FrameDriver, NoPendingWork, PendingWork, run_to_completion};
use crate::memo::Memo;
use crate::watch::{WakeReport, run_to_completion_watched};

/// The store the builder wraps around each registered stage.
///
/// Internal on purpose: which store a stage runs against is the builder's
/// decision (owned map by default, caller-given via `stage_in`, off under
/// `uncached`), not something assembled at call sites.
pub(crate) enum BuilderStore<V, St> {
    /// A fresh map owned by this pipeline.
    Own(MemoMap<V>),
    /// A store the caller provided, so the cache can outlive one build.
    Given(St),
    /// The control case: remember nothing (`PipelineBuilder::uncached`).
    Off,
}

impl<V: Clone, St: MemoStore<V>> MemoStore<V> for BuilderStore<V, St> {
    fn lookup(&self, key: &MemoKey) -> Option<V> {
        match self {
            BuilderStore::Own(map) => map.lookup(key),
            BuilderStore::Given(store) => store.lookup(key),
            BuilderStore::Off => None,
        }
    }

    fn record(&self, key: &MemoKey, value: V) {
        match self {
            BuilderStore::Own(map) => map.record(key, value),
            BuilderStore::Given(store) => store.record(key, value),
            BuilderStore::Off => {}
        }
    }
}

/// The id every internal chain composite answers. A chain never keys
/// (`Chain::memo_key` is `None`; its parts are memoized instead), so the id is
/// never part of a memo key and one shared spelling cannot collide.
const CHAIN_ID: StageId = StageId::new("libpipeline.builder.chain", 0);

fn checked<S: Stage>(id: StageId, stage: S) -> S {
    assert!(
        stage.id() == id,
        "stage registered as {:?} answers id {:?}; the version at the \
         registration call site must be the one the stage's keys carry",
        id,
        stage.id(),
    );
    stage
}

/// The empty builder. Entry point: [`PipelineBuilder::new`].
#[derive(Default)]
pub struct PipelineBuilder {
    uncached: bool,
}

impl PipelineBuilder {
    /// Start a new, empty builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Disable every store: the control run. Answers must not change, only
    /// speed; a pipeline whose answers change when the cache is disabled has a
    /// bug the cache was hiding.
    pub fn uncached(mut self) -> Self {
        self.uncached = true;
        self
    }

    /// Register the first stage. `make` receives the [`StageId`] minted from
    /// `(name, version)`; the stage must answer it from [`Stage::id`], and a
    /// mismatch panics here rather than misfiling memo entries later.
    pub fn stage<S, F>(
        self,
        name: &'static str,
        version: u32,
        make: F,
    ) -> StagedPipelineBuilder<impl Stage<Input = S::Input, Output = S::Output, Error = S::Error>>
    where
        S: Stage,
        S::Output: Clone,
        F: FnOnce(StageId) -> S,
    {
        let id = StageId::new(name, version);
        let store: BuilderStore<S::Output, libpipelinedata::NoMemo> = if self.uncached {
            BuilderStore::Off
        } else {
            BuilderStore::Own(MemoMap::new())
        };
        StagedPipelineBuilder {
            graph: Memo::new(checked(id, make(id)), store),
            uncached: self.uncached,
        }
    }

    /// [`Self::stage`], memoized in a store the caller provides - the door for
    /// a cache that outlives one build of the pipeline. Under
    /// [`Self::uncached`] the given store is ignored: the control run controls
    /// for every store.
    pub fn stage_in<S, St, F>(
        self,
        name: &'static str,
        version: u32,
        store: St,
        make: F,
    ) -> StagedPipelineBuilder<impl Stage<Input = S::Input, Output = S::Output, Error = S::Error>>
    where
        S: Stage,
        S::Output: Clone,
        St: MemoStore<S::Output>,
        F: FnOnce(StageId) -> S,
    {
        let id = StageId::new(name, version);
        let store = if self.uncached {
            BuilderStore::Off
        } else {
            BuilderStore::Given(store)
        };
        StagedPipelineBuilder {
            graph: Memo::new(checked(id, make(id)), store),
            uncached: self.uncached,
        }
    }
}

/// A builder holding at least one stage. Chain more with
/// [`stage`](Self::stage), finish with [`build`](Self::build).
pub struct StagedPipelineBuilder<S> {
    graph: S,
    uncached: bool,
}

impl<S: Stage> StagedPipelineBuilder<S> {
    /// Register the next stage; its `Input` is the previous stage's `Output`.
    /// Same contract as [`PipelineBuilder::stage`].
    pub fn stage<S2, F>(
        self,
        name: &'static str,
        version: u32,
        make: F,
    ) -> StagedPipelineBuilder<
        impl Stage<Input = S::Input, Output = S2::Output, Error = ChainError<S::Error, S2::Error>>,
    >
    where
        S2: Stage<Input = S::Output>,
        S2::Output: Clone,
        F: FnOnce(StageId) -> S2,
    {
        let id = StageId::new(name, version);
        let store: BuilderStore<S2::Output, libpipelinedata::NoMemo> = if self.uncached {
            BuilderStore::Off
        } else {
            BuilderStore::Own(MemoMap::new())
        };
        StagedPipelineBuilder {
            graph: Chain::new(CHAIN_ID, self.graph, Memo::new(checked(id, make(id)), store)),
            uncached: self.uncached,
        }
    }

    /// [`Self::stage`] with a caller-provided store; see
    /// [`PipelineBuilder::stage_in`].
    pub fn stage_in<S2, St, F>(
        self,
        name: &'static str,
        version: u32,
        store: St,
        make: F,
    ) -> StagedPipelineBuilder<
        impl Stage<Input = S::Input, Output = S2::Output, Error = ChainError<S::Error, S2::Error>>,
    >
    where
        S2: Stage<Input = S::Output>,
        S2::Output: Clone,
        St: MemoStore<S2::Output>,
        F: FnOnce(StageId) -> S2,
    {
        let id = StageId::new(name, version);
        let store = if self.uncached {
            BuilderStore::Off
        } else {
            BuilderStore::Given(store)
        };
        StagedPipelineBuilder {
            graph: Chain::new(CHAIN_ID, self.graph, Memo::new(checked(id, make(id)), store)),
            uncached: self.uncached,
        }
    }

    /// Finish: a [`Pipeline`] over the assembled graph.
    pub fn build(self) -> Pipeline<S> {
        Pipeline {
            graph: self.graph,
            frame: FrameDriver::new(),
        }
    }
}

/// A built pipeline: the runner, and the only thing that drives.
///
/// The graph parameter is opaque (`impl Stage` out of the builder); consumers
/// hold a `Pipeline` by inference and cannot reach the machinery inside. Both
/// drive modes are here (`DESIGN.md`, "Two drivers, one graph") - same graph,
/// same keys, and a stage cannot tell which one is polling it.
pub struct Pipeline<S> {
    graph: S,
    frame: FrameDriver,
}

impl<S: Stage> Pipeline<S> {
    /// The blocking (offline) drive: poll until a value or a typed failure,
    /// pumping `work` while polls answer `Pending`. `Stalled` means something
    /// waited for an input nothing was going to land.
    pub fn run<W>(&self, input: &S::Input, work: &W) -> Result<S::Output, DriveError<S::Error>>
    where
        W: PendingWork + ?Sized,
    {
        run_to_completion(&self.graph, input, work)
    }

    /// [`Self::run`] with nothing to pump, for graphs of pure stages.
    pub fn run_pure(&self, input: &S::Input) -> Result<S::Output, DriveError<S::Error>> {
        self.run(input, &NoPendingWork)
    }

    /// [`Self::run`], reporting what its `Pending` polls left behind: each
    /// unwakeable poll is a value a frame driver would lose rather than
    /// receive late. Answers are identical to `run`'s; only the observation is
    /// added.
    pub fn run_watched<W>(
        &self,
        input: &S::Input,
        work: &W,
    ) -> (Result<S::Output, DriveError<S::Error>>, WakeReport)
    where
        W: PendingWork + ?Sized,
    {
        run_to_completion_watched(&self.graph, input, work)
    }

    /// The real-time drive: poll once, return immediately, whatever the
    /// answer. A `Pending` frame draws its stand-in; the waker left behind
    /// schedules the next poll.
    pub fn poll_frame(&self, input: &S::Input) -> EffectPoll<S::Output, S::Error> {
        self.frame.poll_frame(&self.graph, input)
    }

    /// Whether a wake arrived since this was last asked - "stale, poll
    /// again". Reading clears it.
    pub fn take_stale(&self) -> bool {
        self.frame.take_stale()
    }

    /// The waker the frame drive hands to every poll, for landing values out
    /// of band.
    pub fn waker(&self) -> Waker {
        self.frame.waker()
    }
}
