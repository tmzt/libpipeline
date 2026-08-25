//! The builder and the runner: the whole public surface, and the one door onto
//! composition, memoization and running.
//!
//! See `DESIGN.md`. Five rules are enforced here rather than remembered:
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
//!   remember nothing. Rows are erased shares (`Arc<dyn Any + Send + Sync>`),
//!   so one store serves stages of differing output types.
//! * **One error type, flat and positioned.** Every stage of one pipeline
//!   shares an error type; registration stamps the position, and a chain
//!   propagates the [`Failure`] unchanged rather than retyping it per join.
//! * **One door onto running.** [`Pipeline::run`] polls once, gates on the
//!   version AND the stale flag, and returns immediately. Blocking and frame
//!   driving are caller patterns over that one call, so there is no second
//!   method for a caller to pick wrong and no state that means opposite things
//!   at two doors.

use std::any::Any;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::{Context, Waker};

use libpipelinedata::{EffectPoll, MemoKey, MemoMap, MemoStore, Stage, StageId};

use libpipeline_internals::chain::Chain;
use libpipeline_internals::driver::FrameDriver;
use libpipeline_internals::memo::Memo;

/// What one store holds so it can serve stages of differing output types: a
/// stage's output, erased (`DESIGN.md`, "The erased row").
///
/// `Any` is the erasure, costing `V: 'static`; `Send + Sync` because the store
/// is shared across threads, which is the same reason `MemoMap` holds a `Mutex`
/// rather than a `RefCell`.
///
/// **It is the unsized type, not a handle to one, and that is what makes the
/// erasure free.** [`MemoStore`]'s contract is `Arc` on both sides, so a store
/// at this value type holds `Arc<Row>` already: recording is an unsizing
/// coercion of the share the memo layer minted when it missed, and a lookup is
/// [`Arc::downcast`] plus a refcount bump. One allocation per miss, none per
/// hit, and nothing wrapped twice on the way in.
type Row = dyn Any + Send + Sync;

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
    fn lookup(&self, key: &MemoKey) -> Option<Arc<Row>> {
        match self {
            BuilderStore::Own(map) => map.lookup(key),
            BuilderStore::Given(store) => store.lookup(key),
            BuilderStore::Off => None,
        }
    }

    fn record(&self, key: &MemoKey, value: Arc<Row>) {
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
/// **What the erased row costs and buys.** A miss allocates once - the memo
/// layer's `Arc::new` over the value the stage produced - and recording it here
/// is an unsizing coercion of that same allocation. A hit is a lookup, an
/// [`Arc::downcast`] and a refcount bump: nothing about the output is copied,
/// whatever it is. That is the memo's promise held for the values this engine
/// actually carries - lowered documents, node graphs, bundles - rather than
/// held only for the ones a stage author remembered to shape as an `Arc`.
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

impl<V: Send + Sync + 'static> MemoStore<V> for Erased<V> {
    fn lookup(&self, key: &MemoKey) -> Option<Arc<V>> {
        let row = self.store.lookup(key)?;
        Some(row.downcast::<V>().unwrap_or_else(|_| {
            panic!(
                "the memo row keyed to stage {} ({}) holds another stage's \
                 output type; that would mean two stages minted the same \
                 identity, which one builder cannot do - the row is keyed by a \
                 position and positions are minted one per registration",
                self.at, self.label,
            )
        }))
    }

    /// A pure coercion: `Arc<V>` unsizes to `Arc<Row>` with no second
    /// allocation, so the row IS the share the memo layer is about to answer
    /// with.
    fn record(&self, key: &MemoKey, value: Arc<V>) {
        self.store.record(key, value);
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

/// A registered stage whose input arrives as the share the stage before it
/// answered with.
///
/// The memo layer answers `Arc<Output>` (`DESIGN.md`, "A memo hit is cheap"),
/// so the chain carries shares between stages. This hands the stage the value
/// behind the share and nothing else changes: a stage author writes
/// `type Input = Lowered`, not `type Input = Arc<Lowered>`, and the `Arc` the
/// engine uses to keep a hit free stays the engine's business. Symmetry with
/// the output side, where the memo wraps rather than the author: nobody
/// remembers to wrap, so nobody can forget to.
///
/// It forwards the id and the key unchanged - the key is computed from the same
/// value the stage sees, so sharing the input cannot move a memo key.
struct Shared<S> {
    stage: S,
}

impl<S: Stage> Stage for Shared<S> {
    type Input = Arc<S::Input>;
    type Output = S::Output;
    type Error = S::Error;

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
        self.stage.poll_stage(input, cx)
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
    /// of differing output types (`DESIGN.md`, "The erased row"): the value
    /// type it is instantiated at is `dyn Any + Send + Sync`, so what it holds
    /// and hands back is `Arc<dyn Any + Send + Sync>` - the share the memo
    /// layer already made, unsized, never wrapped a second time. An
    /// implementation generic over its value type slots in unchanged provided
    /// that parameter is `?Sized`; one written for this builder alone can name
    /// the erased type directly and needs nothing generic at all.
    ///
    /// The store is handed over here and the pipeline holds it, so it lives
    /// exactly as long as the pipeline does. A store that outlived one is a
    /// closed door and `DESIGN.md`'s "Rejected alternatives" says why.
    pub fn store<St>(mut self, store: St) -> Self
    where
        St: MemoStore<Row> + Send + Sync + 'static,
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
        impl Stage<Input = S::Input, Output = Arc<S::Output>, Error = Failure<S::Error>>,
        S::Error,
    >
    where
        S: Stage,
        S::Output: Send + Sync + 'static,
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
/// **Not a fifth thing to learn.** Its fields are private and it has no
/// constructor: a consumer receives one from `.stage()`, calls a method on it,
/// and with method chaining never writes its name. That is the same category
/// [`Failure`] is in - a public type with private fields and a private `new` -
/// and the two are a pattern this crate uses rather than exceptions to
/// `DESIGN.md`'s count, which counts the things a consumer CONSTRUCTS OR
/// MATCHES ON.
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
        impl Stage<Input = S::Input, Output = Arc<S2::Output>, Error = Failure<E>>,
        E,
    >
    where
        S2: Stage<Error = E>,
        S2::Output: Send + Sync + 'static,
        // The step consumes what the step before it produced. What the graph
        // hands on is the SHARE the memo layer holds, and [`Shared`] is what
        // lets the stage go on being written against the value.
        S: Stage<Output = Arc<S2::Input>>,
        F: FnOnce(StageId) -> S2,
    {
        let at = self.next;
        let stage = Positioned {
            at,
            stage: Shared {
                stage: make(StageId::at(at)),
            },
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
    ///
    /// The pipeline's VERSION type is fixed here, as part of the pipeline's own
    /// type: it is whatever the caller hands [`Pipeline::run`], bounded
    /// `Copy + Eq` because the gate copies it and compares it for identity and
    /// does nothing else with it. Where a version comes from is the consumer's
    /// business - an edit store's cursor, a build number, a git sha.
    pub fn build<V: Copy + Eq>(self) -> Pipeline<V, S> {
        Pipeline {
            graph: self.graph,
            frame: FrameDriver::new(),
            last: Mutex::new(None),
        }
    }
}

/// What one run answered.
///
/// The outcome of the one door, and the whole of it (`DESIGN.md`, "The four
/// outcomes" - the fourth is the failure on the other side of the
/// [`Result`](RunResult)).
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Run<Output> {
    /// Work happened; take the new value.
    ///
    /// **A share, not sole ownership, and the type says so.** The memo still
    /// holds this value after answering with it - that is what a memo is - so
    /// what a caller receives is an [`Arc`] of it. It is also what makes a
    /// large output cheap without anyone remembering to wrap it: the engine
    /// wraps once, on a miss, at the point it records (`DESIGN.md`, "A memo hit
    /// is cheap"), and every hit after that is a refcount bump.
    Computed(Arc<Output>),
    /// The value already held derives from exactly this state; keep it.
    ///
    /// Read it as **the value is finished**: not a report that nothing
    /// happened, but a statement that nothing needs to. The readable was not
    /// dereferenced, no memo key was computed and no stage was polled.
    Unchanged,
    /// Not ready; a wake is coming.
    ///
    /// The run arranged for the pipeline's [waker](Pipeline::waker) to be woken
    /// when the answer becomes possible, so wait to be woken rather than
    /// re-polling in a spin. Where the wake comes from - the original input, or
    /// a later stage internally - is unspecified, because the caller's
    /// obligation is identical either way.
    Delayed,
}

/// What a run answers: an outcome, or the failure that stopped it.
///
/// The error side is [`Failure<E>`] rather than a bare `E` because a pipeline
/// is a sequence of steps and "it failed" is half an answer: WHICH step failed
/// is what a caller acts on, and [`Failure::at`] is that, in one call, at any
/// length of chain.
pub type RunResult<T, E> = Result<Run<T>, Failure<E>>;

/// A built pipeline: the runner, and the only thing that drives.
///
/// The graph parameter is opaque (`impl Stage` out of the builder); consumers
/// hold a `Pipeline` by inference and cannot reach the machinery inside.
///
/// **There is one door.** [`run`](Self::run) polls once and returns
/// immediately, whatever the answer; nothing inside it waits, ever. Blocking
/// and frame-driving are CALLER patterns over that one call - a frame caller
/// runs once per frame, a blocking caller loops on [`Run::Delayed`] pumping its
/// own executor - and a stage cannot tell which is asking (`DESIGN.md`,
/// "Blocking and frame are what a caller does"). Two doors would make waiting
/// the pipeline's job, and the same state would then mean opposite things at
/// each: a poll that cannot progress is a defect to one caller and an ordinary
/// frame to the other, and only the caller can tell which, because only the
/// caller can see whether its queue is empty.
pub struct Pipeline<V, S> {
    graph: S,
    frame: FrameDriver,
    /// The version the last [`Run::Computed`] answered for, which is the whole
    /// of the version gate's state. Safe interior mutability, as everywhere in
    /// this crate: a run keeps `&self` because a poll holds `&self` all the way
    /// down.
    last: Mutex<Option<V>>,
}

impl<V, S> Pipeline<V, S> {
    /// Whether a wake arrived since this was last asked - "stale, run again".
    /// Reading clears it.
    ///
    /// **[`run`](Self::run) consumes this too, on every run** (see there), so
    /// the two readers race: a caller that reads it and then calls `run` has
    /// already taken the wake, and `run` will see none. A frame caller does not
    /// need it - call `run` every frame and let [`Run::Unchanged`] be the cheap
    /// answer, which is what that variant is for.
    pub fn take_stale(&self) -> bool {
        self.frame.take_stale()
    }

    /// The waker every poll is handed, for landing values out of band.
    pub fn waker(&self) -> Waker {
        self.frame.waker()
    }

    /// The gate's state, recovering from a poisoned lock rather than
    /// panicking: a run that panicked elsewhere should cost this pipeline a
    /// recompute, not every run after it.
    fn last(&self) -> MutexGuard<'_, Option<V>> {
        self.last.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl<V, S, O, E> Pipeline<V, S>
where
    V: Copy + Eq,
    S: Stage<Output = Arc<O>, Error = Failure<E>>,
{
    /// Run once: the version gate, one poll, and the outcome.
    ///
    /// `version` says WHICH STATE the readable is; it is the only version in
    /// the API, and the pipeline never computes one - it compares the ones it
    /// is handed. That pairing is the point: the version costs a comparison,
    /// and the readable may be a large snapshot that a matching version never
    /// touches.
    ///
    /// **The gate consumes the stale flag on EVERY run, and answers
    /// [`Run::Unchanged`] only when the version matches AND no wake was
    /// pending** (`DESIGN.md`, "The version gate and the one door"). Two
    /// different things mean "something happened" and only one of them moves
    /// the version: the input version moves when the source changes, and a wake
    /// arrives when a value some stage was waiting on has landed. A landed
    /// effect does not move the input version, so a gate that checked the
    /// version alone would take a pipeline sitting on [`Run::Delayed`], receive
    /// the wake, re-poll, short-circuit on the unchanged version and answer
    /// `Unchanged` - forever, with the caller holding a value permanently one
    /// step stale and nothing reporting it. The flag is read FIRST and
    /// unconditionally because it clears on read: a run that polled for a
    /// version change and left an unread wake behind would be the same defect
    /// one step displaced.
    ///
    /// Only [`Run::Computed`] records the version. A `Delayed` run does not, so
    /// asking again with the same version polls again; a failure does not
    /// either, so a later run with the same version retries.
    pub fn run(&self, version: V, input: &S::Input) -> RunResult<O, E> {
        let woken = self.frame.take_stale();
        if !woken && *self.last() == Some(version) {
            return Ok(Run::Unchanged);
        }
        match self.frame.poll_frame(&self.graph, input) {
            EffectPoll::Ready(value) => {
                *self.last() = Some(version);
                Ok(Run::Computed(value))
            }
            EffectPoll::Pending => Ok(Run::Delayed),
            // A re-wrap, not a construction: the position was stamped at
            // registration, so the graph's own error type IS the flat
            // `Failure` and the door only moves it onto `Result`'s error side.
            EffectPoll::Failed(failure) => Err(failure),
        }
    }
}
