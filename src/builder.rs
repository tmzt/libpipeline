//! The builder: the one public door onto composition, memoization and driving.
//!
//! See `DESIGN.md`. Four rules are enforced here rather than remembered:
//!
//! * **Memoization is intrinsic to registration.** Every stage registered
//!   through [`PipelineBuilder::stage`] is wrapped in the memo layer; there is
//!   no un-memoized registration to forget. A stage that must not be served
//!   from cache says so through `memo_key -> None`.
//! * **Identity is a position.** The builder mints the [`StageId`] from the
//!   order it sees registrations in and hands it to `make`; a stage never
//!   declares one, so there is no second id for an honest author to answer
//!   with and no construction-time check to run (`DESIGN.md`, "Identity is a
//!   position"). The `name` beside it is a diagnostic label: it enters no key,
//!   nothing is looked up or compared by it, and two stages may share one with
//!   no consequence.
//! * **One store, chosen once.** Where the pipeline remembers is the builder's
//!   decision about the whole pipeline - a map it owns by default,
//!   [`PipelineBuilder::store`] to override, [`PipelineBuilder::uncached`] to
//!   remember nothing. Rows are erased (`Arc<dyn Any + Send + Sync>`), so one
//!   store serves stages of differing output types.
//! * **One error type, flat and positioned.** Every stage of one pipeline
//!   shares an error type; registration stamps the position, and a chain
//!   propagates the [`Failure`] unchanged rather than retyping it per join.

use std::any::Any;
use std::marker::PhantomData;
use std::sync::Arc;
use std::task::{Context, Waker};

use libpipelinedata::{EffectPoll, MemoKey, MemoMap, MemoStore, Stage, StageId};

use libpipeline_internals::chain::Chain;
use libpipeline_internals::driver::{
    DriveError, FrameDriver, NoPendingWork, PendingWork, run_to_completion,
};
use libpipeline_internals::memo::Memo;
use libpipeline_internals::watch::{WakeReport, run_to_completion_watched};

/// What one store holds so it can serve stages of differing output types: a
/// stage's output, erased (`DESIGN.md`, "The erased row").
///
/// `Arc` because [`MemoStore::lookup`] returns owned values on purpose and
/// [`MemoMap`] is bounded `V: Clone`, so the row has to be cheap to hand out;
/// `Any` because that is the erasure, costing `V: 'static`; `Send + Sync`
/// because the store is shared across threads, which is the same reason
/// `MemoMap` holds a `Mutex` rather than a `RefCell`.
type Row = Arc<dyn Any + Send + Sync>;

/// The one store the builder chose, in the three shapes that choice has.
///
/// Internal on purpose: where the pipeline remembers is the builder's decision
/// about the WHOLE pipeline (`DESIGN.md`, "One store, at the builder"), not
/// something assembled at call sites. It is consulted once, at the first
/// registration, and every stage after that shares the answer.
pub(crate) enum BuilderStore {
    /// The default: a map owned by this pipeline, living exactly as long as it
    /// does.
    Own(MemoMap<Row>),
    /// A store the caller handed over with [`PipelineBuilder::store`].
    Given(Arc<dyn MemoStore<Row> + Send + Sync>),
    /// The control case: remember nothing ([`PipelineBuilder::uncached`]).
    Off,
}

impl MemoStore<Row> for BuilderStore {
    fn lookup(&self, key: &MemoKey) -> Option<Row> {
        match self {
            BuilderStore::Own(map) => map.lookup(key),
            BuilderStore::Given(store) => store.lookup(key),
            BuilderStore::Off => None,
        }
    }

    fn record(&self, key: &MemoKey, value: Row) {
        match self {
            BuilderStore::Own(map) => map.record(key, value),
            BuilderStore::Given(store) => store.record(key, value),
            BuilderStore::Off => {}
        }
    }
}

/// One stage's view of the builder's store: the typed seam the memo layer
/// needs, over the erased rows the store holds.
///
/// **The downcast cannot legitimately fail, so it is not handled.** A row's key
/// carries the stage's [`StageId`], an id is a position, and positions are
/// minted one per registration by a single builder - so within one pipeline no
/// two stages can share one, and a row recorded under a stage's key can only
/// hold that stage's output type. A failure here would mean an identity
/// collision, which the builder cannot mint; the message says so rather than
/// the code pretending it is a miss.
///
/// **Why this lives in the facade.** Nothing the machinery uses may be DEFINED
/// here - `libpipeline-internals` cannot depend on `libpipeline`, so a type it
/// names has to be somewhere it can reach. Nothing is defined here that it
/// names: the seam is [`MemoStore`], which is `libpipelinedata`'s, and `Memo`
/// is generic over it. The erased row is constructed by `record` below and
/// consumed by `lookup` below - both ends of the pair are in this file - and
/// the internals see only `Option<V>` at the type the stage answers. No new
/// trait exists anywhere in this crate.
///
/// **What the erased row costs and buys.** A hit is a lookup, a downcast and a
/// clone of the output - and the clone is what the memo's promise rests on: for
/// an `Arc`-shaped output it is a refcount bump, and for an output that is
/// itself a large owned structure it is still the deep copy
/// [`MemoStore::lookup`]'s owned-value contract implies. The erasure removes
/// the second copy (the store's own), not the one the seam is defined in terms
/// of.
struct Erased<V> {
    store: Arc<BuilderStore>,
    /// The position this seam serves - for the message above, nothing else.
    at: usize,
    /// The registration's diagnostic label - for the message above, nothing
    /// else. It enters no key: a key's stage half is [`StageId`], which has no
    /// room for a name.
    label: &'static str,
    _value: PhantomData<fn() -> V>,
}

impl<V> Erased<V> {
    fn new(store: Arc<BuilderStore>, at: usize, label: &'static str) -> Self {
        Self {
            store,
            at,
            label,
            _value: PhantomData,
        }
    }
}

impl<V: Clone + Send + Sync + 'static> MemoStore<V> for Erased<V> {
    fn lookup(&self, key: &MemoKey) -> Option<V> {
        let row = self.store.lookup(key)?;
        let held: Arc<V> = row.downcast::<V>().unwrap_or_else(|_| {
            panic!(
                "the memo row keyed to stage {} ({}) holds another stage's \
                 output type; that would mean two stages minted the same \
                 identity, which one builder cannot do - the row is keyed by a \
                 position and positions are minted one per registration",
                self.at, self.label,
            )
        });
        Some((*held).clone())
    }

    fn record(&self, key: &MemoKey, value: V) {
        self.store.record(key, Arc::new(value) as Row);
    }
}

/// A failure, with the position of the stage that raised it.
///
/// One `Failure` type serves a whole pipeline: a chain of five steps has the
/// same error type as a chain of two, so `?` propagates it through any depth of
/// assembly and a caller that matches it matches once (`DESIGN.md`, "One error
/// type, flat and positioned"). "Which stage failed" is [`at`](Self::at),
/// answered in one call at any length of chain - which is only spellable
/// because identity is a position.
///
/// The fields are private and there is no constructor: a position is meaningful
/// only if the builder that minted it stamped it, and a `Failure` a caller
/// could build would be a position a caller invented.
///
/// **Why this lives in the facade.** It is constructed here, at registration
/// ([`Positioned`]), and consumed by the caller through [`at`](Self::at) and
/// [`error`](Self::error) - both ends of that pair are the facade's.
/// `libpipeline-internals` never names it: [`Chain`] propagates `A::Error`
/// unchanged, and the memo layer and the drivers are generic over `S::Error`,
/// so a positioned failure travels through the machinery as an opaque type
/// parameter. Were any of them to construct or unwrap one, this type would
/// have to move to `libpipelinedata`, where the machinery could reach it.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Failure<E> {
    at: usize,
    error: E,
}

impl<E> Failure<E> {
    /// Stamped at registration, the only place the index is available.
    fn new(at: usize, error: E) -> Self {
        Self { at, error }
    }

    /// The position of the stage that raised this - the same number the
    /// builder minted its identity from, counting registrations from zero.
    pub fn at(&self) -> usize {
        self.at
    }

    /// The stage's own error, beside the position.
    pub fn error(&self) -> &E {
        &self.error
    }
}

/// A registered stage, with its position stamped onto whatever it raises.
///
/// This is where the flat error is made: the stage answers its own error type
/// and this turns it into the pipeline's one, so [`Chain`] has nothing to
/// retype and propagates instead (`DESIGN.md`, "One error type, flat and
/// positioned"). Everything else forwards - a wrapper that changed the id or
/// the key would be part of the semantics rather than a stamp on the error
/// channel.
struct Positioned<S> {
    at: usize,
    stage: S,
}

impl<S: Stage> Stage for Positioned<S> {
    type Input = S::Input;
    type Output = S::Output;
    type Error = Failure<S::Error>;

    fn id(&self) -> StageId {
        self.stage.id()
    }

    fn memo_key(&self, input: &Self::Input) -> Option<MemoKey> {
        self.stage.memo_key(input)
    }

    fn poll_stage(
        &self,
        input: &Self::Input,
        cx: &mut Context<'_>,
    ) -> EffectPoll<Self::Output, Self::Error> {
        self.stage
            .poll_stage(input, cx)
            .map_err(|error| Failure::new(self.at, error))
    }
}

/// The id every internal chain composite answers. A chain never keys
/// (`Chain::memo_key` is `None`; its parts are memoized instead), so the id is
/// never part of a memo key and one shared spelling cannot collide.
///
/// `usize::MAX` is deliberately not a position: a builder would have to see
/// `usize::MAX` registrations before it minted this one, so a composite's id
/// cannot be confused with any stage's.
const CHAIN_ID: StageId = StageId::at(usize::MAX);

/// The empty builder. Entry point: [`PipelineBuilder::new`].
#[derive(Default)]
pub struct PipelineBuilder {
    uncached: bool,
    store: Option<Arc<dyn MemoStore<Row> + Send + Sync>>,
}

impl PipelineBuilder {
    /// Start a new, empty builder, remembering into a map of its own.
    pub fn new() -> Self {
        Self::default()
    }

    /// Disable the store: the control run. Answers must not change, only
    /// speed; a pipeline whose answers change when the cache is disabled has a
    /// bug the cache was hiding.
    ///
    /// This wins over [`store`](Self::store) whichever order the two are called
    /// in: the control run controls for every store.
    pub fn uncached(mut self) -> Self {
        self.uncached = true;
        self
    }

    /// Remember into `store` rather than into a map of the builder's own - one
    /// store, one decision, taken once, for the whole pipeline.
    ///
    /// The store holds ERASED rows, which is what lets one store serve stages
    /// of differing output types (`DESIGN.md`, "The erased row"): an
    /// implementation generic over its value type slots in unchanged, and the
    /// value type it is instantiated at is `Arc<dyn Any + Send + Sync>`.
    ///
    /// The store is handed over here and the pipeline holds it, so it lives
    /// exactly as long as the pipeline does. A store that outlived one is a
    /// closed door and `DESIGN.md`'s "Rejected alternatives" says why.
    pub fn store<St>(mut self, store: St) -> Self
    where
        St: MemoStore<Arc<dyn Any + Send + Sync>> + Send + Sync + 'static,
    {
        self.store = Some(Arc::new(store));
        self
    }

    /// The one store, resolved: the three answers of [`BuilderStore`] taken
    /// once, at the first registration.
    fn resolve(self) -> Arc<BuilderStore> {
        Arc::new(if self.uncached {
            BuilderStore::Off
        } else {
            match self.store {
                Some(store) => BuilderStore::Given(store),
                None => BuilderStore::Own(MemoMap::new()),
            }
        })
    }

    /// Register the first stage. `make` receives the [`StageId`] the builder
    /// mints for it - position zero - and the stage answers it from
    /// [`Stage::id`].
    ///
    /// `name` is a diagnostic label, held for messages. It enters no key,
    /// nothing is looked up or compared by it, and a second stage may carry the
    /// same one with no consequence.
    pub fn stage<S, F>(
        self,
        name: &'static str,
        make: F,
    ) -> StagedPipelineBuilder<
        impl Stage<Input = S::Input, Output = S::Output, Error = Failure<S::Error>>,
        S::Error,
    >
    where
        S: Stage,
        S::Output: Clone + Send + Sync + 'static,
        F: FnOnce(StageId) -> S,
    {
        let at = 0;
        let store = self.resolve();
        let stage = Positioned {
            at,
            stage: make(StageId::at(at)),
        };
        StagedPipelineBuilder {
            graph: Memo::new(stage, Erased::new(Arc::clone(&store), at, name)),
            store,
            next: at + 1,
            _error: PhantomData,
        }
    }
}

/// A builder holding at least one stage. Chain more with
/// [`stage`](Self::stage), finish with [`build`](Self::build).
///
/// The `E` parameter is the pipeline's one error type, which every stage
/// shares; the graph carries it as [`Failure<E>`], already positioned.
pub struct StagedPipelineBuilder<S, E> {
    graph: S,
    store: Arc<BuilderStore>,
    next: usize,
    _error: PhantomData<fn() -> E>,
}

impl<S, E> StagedPipelineBuilder<S, E>
where
    S: Stage<Error = Failure<E>>,
{
    /// Register the next stage; its `Input` is the previous stage's `Output`
    /// and its `Error` is the pipeline's one error type. Same contract as
    /// [`PipelineBuilder::stage`].
    ///
    /// The shared error type is the cost `DESIGN.md` names rather than hides: a
    /// consumer whose steps fail in genuinely disjoint ways writes one enum
    /// unifying them, which is what such a consumer would write anyway to match
    /// on the result.
    pub fn stage<S2, F>(
        self,
        name: &'static str,
        make: F,
    ) -> StagedPipelineBuilder<
        impl Stage<Input = S::Input, Output = S2::Output, Error = Failure<E>>,
        E,
    >
    where
        S2: Stage<Input = S::Output, Error = E>,
        S2::Output: Clone + Send + Sync + 'static,
        F: FnOnce(StageId) -> S2,
    {
        let at = self.next;
        let stage = Positioned {
            at,
            stage: make(StageId::at(at)),
        };
        let memo = Memo::new(stage, Erased::new(Arc::clone(&self.store), at, name));
        StagedPipelineBuilder {
            graph: Chain::new(CHAIN_ID, self.graph, memo),
            store: self.store,
            next: at + 1,
            _error: PhantomData,
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
/// drive modes are here (`PLAN.md`, "Two drivers, one graph") - same graph,
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
