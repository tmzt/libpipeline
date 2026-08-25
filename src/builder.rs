//! The builder and the runner: the whole public surface, and the one door onto
//! composition, memoization and running.
//!
//! See `DESIGN.md`. Six rules are enforced here rather than remembered:
//!
//! * **A stage is a pair of FUNCTIONS, registered as `fn` pointers.**
//!   [`PipelineBuilder::stage_fn`] takes a key function and a poll function,
//!   both `fn` and not `impl Fn`, so a non-capturing closure coerces and a
//!   capturing one does not compile. That is the whole of `DESIGN.md`'s "a door
//!   typed on a trait hands back a struct, and structs accrete fields - each one
//!   a candidate input that moves the output without moving the key": the `fn`
//!   door makes the field impossible rather than reviewable, and `impl Fn` fails
//!   the same way one increment earlier.
//! * **Everything a stage is handed comes through [`Ctx`].** Today that is its
//!   identity and the waker it must arrange a wake on; the read log and the
//!   in-flight store `DESIGN.md` describes join it there rather than beside it,
//!   so the registration signature does not move when they do.
//! * **Memoization is intrinsic to registration.** Every stage registered
//!   through [`PipelineBuilder::stage_fn`] is wrapped in the memo layer; there
//!   is no un-memoized registration to forget. A stage that must not be served
//!   from cache says so by answering `None` from its key function.
//! * **Identity is a position.** The builder mints the `StageId` from the order
//!   it sees registrations in and hands it to both functions through [`Ctx`]; a
//!   stage never declares one, so there is no second id for an honest author to
//!   answer with and no construction-time check to run (`DESIGN.md`, "Identity
//!   is a position"). The `name` beside it is a diagnostic label: it enters no
//!   key, nothing is looked up or compared by it, and two stages may share one
//!   with no consequence.
//! * **One store, chosen once.** Where the pipeline remembers is the builder's
//!   decision about the whole pipeline - a map it owns by default,
//!   [`PipelineBuilder::store`] to override, [`PipelineBuilder::uncached`] to
//!   remember nothing. Rows are erased shares (`Arc<dyn Any + Send + Sync>`),
//!   so one store serves stages of differing output types.
//! * **One error type, flat and positioned.** Every stage of one pipeline
//!   shares an error type; registration stamps the position, and a chain
//!   propagates the [`Failure`] unchanged rather than retyping it per join.
//! * **One door onto running.** [`Pipeline::poll`] polls once and returns
//!   immediately - the name says what it does. Blocking and frame driving are
//!   caller patterns over that one call: [`run_blocking`] is the blocking one,
//!   shipped as a free function whose whole body is a loop over `poll`, so
//!   there is no second way INTO the engine and no state that means opposite
//!   things at two doors.
//!
//! **No public signature here names a trait of the machinery.** The graph is
//! held as a boxed stage behind private fields, so `Pipeline` and
//! `StagedPipelineBuilder` are spelled in the CONSUMER's types - input, output,
//! error, version - and the stage contract stays inside
//! `libpipeline-internals` where it now lives. One indirect call per stage per
//! poll is what that costs, against a stage that is polled at all only when its
//! memo missed.

use std::any::Any;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::{Context, Waker};

use libpipelinedata::{
    ContentKey, EffectPoll, MemoKey, MemoMap, MemoStore, StageAnswer, StageId,
};

use libpipeline_internals::Stage;
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
/// coercion of the share the stage answered with, and a lookup is
/// [`Arc::downcast`] plus a refcount bump. One allocation per miss, none per
/// hit, and nothing wrapped twice on the way in.
type Row = dyn Any + Send + Sync;

/// The assembled graph, erased.
///
/// **Why it is a box and not a type parameter.** The stage contract is
/// `libpipeline-internals`', and a `Pipeline<V, S>` generic over the graph would
/// put `impl Stage<..>` in every public return type - which is the machinery
/// reappearing in the facade's signatures under a different spelling. Erased
/// here, the public types are spelled in the consumer's own: the input, the
/// output, the error and the version.
type Graph<I, O, E> = Box<dyn Stage<Input = I, Output = O, Error = Failure<E>> + Send + Sync>;

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
/// the internals see only `Option<Arc<V>>` at the type the stage answers.
///
/// **What the erased row costs and buys.** A miss allocates once - the wrapping
/// [`StageFn`] does over the value the poll function produced - and recording
/// it here is an unsizing coercion of that same allocation. A hit is a lookup,
/// an [`Arc::downcast`] and a refcount bump: nothing about the output is
/// copied, whatever it is. That is the memo's promise held for the values this
/// engine actually carries - lowered documents, node graphs, bundles - rather
/// than held only for the ones a stage author remembered to shape as an `Arc`.
struct Erased<V> {
    store: Arc<BuilderStore>,
    /// The position this seam serves - for the message above, nothing else.
    at: usize,
    /// The registration's diagnostic label - for the message above, nothing
    /// else. It enters no key: a key's stage half is [`StageId`], which has no
    /// room for a name.
    label: &'static str,
    _value: std::marker::PhantomData<fn() -> V>,
}

impl<V> Erased<V> {
    fn new(store: Arc<BuilderStore>, at: usize, label: &'static str) -> Self {
        Self {
            store,
            at,
            label,
            _value: std::marker::PhantomData,
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
    /// allocation, so the row IS the share the stage is about to answer with.
    fn record(&self, key: &MemoKey, value: Arc<V>) {
        self.store.record(key, value);
    }
}

/// What a stage function is handed: everything the engine gives a stage, and
/// the whole of it.
///
/// **It exists so that the registration signature does not move when the engine
/// gives a stage more.** `DESIGN.md`'s "The intended stage shape" puts reads,
/// in-flight state and identity through one argument for that reason; today it
/// carries the two a poll cannot do without.
///
/// * **The identity the builder minted**, which is this registration's position
///   and nothing else. [`key`](Ctx::key) is the only spelling a stage needs:
///   the id it would otherwise pass is already here, so a key built through it
///   cannot be built under another stage's identity.
/// * **The waker**, which is what makes [`EffectPoll::Pending`] an honest
///   answer. `Pending` promises a wake is coming, and
///   `ctx.waker().clone()` stashed where the value will land is how that
///   promise is kept.
///
/// **What it does not carry yet, and what that costs**, is recorded in
/// `DESIGN.md` rather than implied by silence: the read log (so that a stage's
/// ambient reads enter the read-set) and in-flight state addressed by
/// `(PipelineId, position)` (so that a stage can hold something between a
/// `Pending` and the `Ready` that answers it). Until they exist, a stage that
/// needs either reaches a `static` or a `thread_local` - which the `fn` door
/// permits and the type system cannot distinguish from an honest one.
///
/// **The key function's waker is a no-op**, deliberately: a key is computed
/// before the stage runs, so there is nothing for it to be woken about, and a
/// waker that did anything there would be arranging a wake for work that has
/// not started.
pub struct Ctx<'a> {
    id: StageId,
    waker: &'a Waker,
}

impl<'a> Ctx<'a> {
    /// Built by the engine, per poll, and never by a consumer.
    fn new(id: StageId, waker: &'a Waker) -> Self {
        Self { id, waker }
    }

    /// This stage's identity: the position the builder minted for it.
    ///
    /// Held, never constructed. Prefer [`key`](Ctx::key), which is the only
    /// thing an id is for on this side.
    pub fn id(&self) -> StageId {
        self.id
    }

    /// The memo key for this stage over `inputs`, in argument order.
    ///
    /// The stage half is this registration's identity, supplied here rather
    /// than by the caller: a key is `(stage id, input content keys)`, and the
    /// id is the half a stage has no business choosing.
    pub fn key(&self, inputs: impl IntoIterator<Item = ContentKey>) -> MemoKey {
        MemoKey::new(self.id, inputs)
    }

    /// The wake target for this poll.
    ///
    /// Answering [`EffectPoll::Pending`] obliges a stage to arrange for this to
    /// be woken when the answer becomes possible. A `Pending` that leaves no
    /// wake path has made its value LOST rather than late (`DESIGN.md`,
    /// "Delayed keeps its promise").
    pub fn waker(&self) -> &Waker {
        self.waker
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
/// ([`StageFn`]), and consumed by the caller through [`at`](Self::at) and
/// [`error`](Self::error) - both ends of that pair are the facade's.
/// `libpipeline-internals` never names it: [`Chain`] propagates `A::Error`
/// unchanged, and the memo layer and the drivers are generic over `S::Error`,
/// so a positioned failure travels through the machinery as an opaque type
/// parameter.
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

/// A registration: the two functions, the identity minted for them, and the
/// position stamped onto whatever they raise.
///
/// **The `fn` pointers are the whole enforcement.** There is no room in this
/// struct for state a stage brought with it, because the type of what a
/// registration accepts has no room for it: a non-capturing closure coerces to
/// `fn` and a capturing one does not. Everything the poll can see, the key can
/// see, which is what makes a memo key an honest address for the answer.
///
/// This is also where the flat error is made: the poll answers its own error
/// type and this turns it into the pipeline's one, so [`Chain`] has nothing to
/// retype and propagates instead (`DESIGN.md`, "One error type, flat and
/// positioned").
struct StageFn<I, O, E> {
    at: usize,
    id: StageId,
    key: fn(&I, &Ctx<'_>) -> Option<MemoKey>,
    poll: fn(&I, &Ctx<'_>) -> EffectPoll<StageAnswer<O>, E>,
}

impl<I, O, E> Stage for StageFn<I, O, E> {
    type Input = I;
    type Output = O;
    type Error = Failure<E>;

    fn id(&self) -> StageId {
        self.id
    }

    fn memo_key(&self, input: &Self::Input) -> Option<MemoKey> {
        (self.key)(input, &Ctx::new(self.id, Waker::noop()))
    }

    fn poll_stage(
        &self,
        input: &Self::Input,
        cx: &mut Context<'_>,
    ) -> EffectPoll<StageAnswer<Arc<Self::Output>>, Self::Error> {
        match (self.poll)(input, &Ctx::new(self.id, cx.waker())) {
            // The one allocation a miss costs: what the memo records and what
            // the caller is eventually handed are this same share.
            EffectPoll::Ready(StageAnswer::Computed(value)) => {
                EffectPoll::Ready(StageAnswer::Computed(Arc::new(value)))
            }
            // Nothing to wrap: the value this refers to is the one the memo
            // layer's slot already holds, which is what makes the answer free.
            EffectPoll::Ready(StageAnswer::Unchanged) => {
                EffectPoll::Ready(StageAnswer::Unchanged)
            }
            EffectPoll::Pending => EffectPoll::Pending,
            EffectPoll::Failed(error) => EffectPoll::Failed(Failure::new(self.at, error)),
        }
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
    /// and hands back is `Arc<dyn Any + Send + Sync>` - the share the stage
    /// already answered with, unsized, never wrapped a second time. An
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

    /// Register the first stage: a key function and a poll function, both `fn`
    /// pointers.
    ///
    /// * `key` answers this input's memo key WITHOUT running the stage, which
    ///   is what lets the lookup precede the work. `None` means "this input
    ///   cannot be honestly addressed": the stage is then neither looked up nor
    ///   recorded, which is how a registered stage opts out of the memoization
    ///   registration applies. Build the key through [`Ctx::key`], which
    ///   supplies the identity half.
    /// * `poll` answers [`EffectPoll`]: `Ready` with a [`StageAnswer`],
    ///   `Pending` when an awaited input has not landed - having stashed a
    ///   clone of [`Ctx::waker`] where it will be woken - or `Failed` with the
    ///   stage's own error, which the pipeline positions.
    ///
    ///   A `Ready` says WHICH answer. `StageAnswer::computed(value)` is the
    ///   ordinary one. `StageAnswer::unchanged()` says the answer this stage
    ///   last gave still stands - it carries no value, and the stages after
    ///   this one are then never entered at all, which is the whole saving. A
    ///   stage may only say it where it has answered before; on a cold
    ///   position the engine panics rather than telling a caller to keep a
    ///   value it was never given.
    ///
    /// **Both are `fn` and not `impl Fn`, and that is the design rather than a
    /// stylistic preference.** A non-capturing closure coerces to `fn` and a
    /// capturing one does not compile, so a stage cannot carry state the key
    /// function cannot see. `impl Fn` would permit exactly that, and a trait
    /// would permit it one increment further along (`DESIGN.md`, "A
    /// trait-taking stage door").
    ///
    /// `name` is a diagnostic label, held for messages. It enters no key,
    /// nothing is looked up or compared by it, and a second stage may carry the
    /// same one with no consequence.
    pub fn stage_fn<I, O, E>(
        self,
        name: &'static str,
        key: fn(&I, &Ctx<'_>) -> Option<MemoKey>,
        poll: fn(&I, &Ctx<'_>) -> EffectPoll<StageAnswer<O>, E>,
    ) -> StagedPipelineBuilder<I, O, E>
    where
        I: 'static,
        O: Send + Sync + 'static,
        E: 'static,
    {
        let at = 0;
        let store = self.resolve();
        let stage = StageFn {
            at,
            id: StageId::at(at),
            key,
            poll,
        };
        StagedPipelineBuilder {
            graph: Box::new(Memo::new(stage, Erased::new(Arc::clone(&store), at, name))),
            store,
            next: at + 1,
        }
    }
}

/// A builder holding at least one stage. Chain more with
/// [`stage_fn`](Self::stage_fn), finish with [`build`](Self::build).
///
/// **Not a fifth thing to learn.** Its fields are private and it has no
/// constructor: a consumer receives one from `.stage_fn()`, calls a method on
/// it, and with method chaining never writes its name. That is the same
/// category [`Failure`] is in - a public type with private fields and a private
/// `new` - and the two are a pattern this crate uses rather than exceptions to
/// `DESIGN.md`'s count, which counts the things a consumer CONSTRUCTS OR
/// MATCHES ON.
///
/// Its parameters are the consumer's own types and no others: `I` is what the
/// first registration consumes, `O` what the last one produced, `E` the one
/// error type every stage of this pipeline shares.
pub struct StagedPipelineBuilder<I, O, E> {
    graph: Graph<I, O, E>,
    store: Arc<BuilderStore>,
    next: usize,
}

impl<I, O, E> StagedPipelineBuilder<I, O, E>
where
    I: 'static,
    O: Send + Sync + 'static,
    E: 'static,
{
    /// Register the next stage; it consumes what the previous one produced and
    /// raises the pipeline's one error type. Same contract as
    /// [`PipelineBuilder::stage_fn`].
    ///
    /// The shared error type is the cost `DESIGN.md` names rather than hides: a
    /// consumer whose steps fail in genuinely disjoint ways writes one enum
    /// unifying them, which is what such a consumer would write anyway to match
    /// on the result.
    pub fn stage_fn<O2>(
        self,
        name: &'static str,
        key: fn(&O, &Ctx<'_>) -> Option<MemoKey>,
        poll: fn(&O, &Ctx<'_>) -> EffectPoll<StageAnswer<O2>, E>,
    ) -> StagedPipelineBuilder<I, O2, E>
    where
        O2: Send + Sync + 'static,
    {
        let at = self.next;
        let stage = StageFn {
            at,
            id: StageId::at(at),
            key,
            poll,
        };
        let memo = Memo::new(stage, Erased::new(Arc::clone(&self.store), at, name));
        StagedPipelineBuilder {
            // The join is spelled on the VALUE - the next stage's input is the
            // previous stage's output - because the share lives in the poll's
            // return type rather than in `Output`. Nothing adapts between two
            // stages.
            graph: Box::new(Chain::new(CHAIN_ID, self.graph, memo)),
            store: self.store,
            next: at + 1,
        }
    }

    /// Finish: a [`Pipeline`] over the assembled graph.
    ///
    /// The pipeline's VERSION type is fixed here, as part of the pipeline's own
    /// type: it is whatever the caller hands [`Pipeline::run`], bounded
    /// `Copy + Eq` because the gate copies it and compares it for identity and
    /// does nothing else with it. Where a version comes from is the consumer's
    /// business - an edit store's cursor, a build number, a git sha.
    pub fn build<V: Copy + Eq>(self) -> Pipeline<V, I, O, E> {
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
    /// wraps once, where a poll function's value enters the graph, and every
    /// hit after that is a refcount bump.
    Computed(Arc<Output>),
    /// The value already held derives from exactly this state; keep it.
    ///
    /// Read it as **the value is finished**: not a report that nothing
    /// happened, but a statement that nothing needs to.
    ///
    /// Two things answer it and the caller's obligation is identical for both.
    /// The version gate answers it when the version it is handed is the one it
    /// last recorded and no wake is pending - the readable is not dereferenced,
    /// no memo key is computed and no stage is polled. And the GRAPH answers it
    /// when a stage that was entered rewrote nothing: the stages after that one
    /// are never entered, the answer travels to the root, and the door records
    /// the version for it. Which of the two happened is the pipeline's own
    /// business, and a caller that could tell them apart would have nothing
    /// different to do about it.
    Unchanged,
    /// Not ready; a wake is coming.
    ///
    /// The poll arranged for the pipeline's [waker](Pipeline::waker) to be
    /// woken when the answer becomes possible, so wait to be woken rather than
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
/// Its parameters are the consumer's own: the version type the gate compares,
/// the first stage's input, the last stage's output, and the one error type
/// every stage shares. The graph itself is erased behind a private field, so
/// nothing about the machinery is spellable from a `Pipeline`'s type.
///
/// **There is one door.** [`poll`](Self::poll) polls once and returns
/// immediately, whatever the answer; nothing inside it waits, ever. Blocking
/// and frame-driving are CALLER patterns over that one call - a frame caller
/// polls once per frame, a blocking caller loops on [`Run::Delayed`] pumping
/// its own executor, which [`run_blocking`] is - and a stage cannot tell which
/// is asking (`DESIGN.md`,
/// "Blocking and frame are what a caller does"). Two doors would make waiting
/// the pipeline's job, and the same state would then mean opposite things at
/// each: a poll that cannot progress is a defect to one caller and an ordinary
/// frame to the other, and only the caller can tell which, because only the
/// caller can see whether its queue is empty.
pub struct Pipeline<V, I, O, E> {
    graph: Graph<I, O, E>,
    frame: FrameDriver,
    /// The version the last [`Run::Computed`] answered for, which is the whole
    /// of the version gate's state. Safe interior mutability, as everywhere in
    /// this crate: a run keeps `&self` because a poll holds `&self` all the way
    /// down.
    last: Mutex<Option<V>>,
}

impl<V, I, O, E> Pipeline<V, I, O, E> {
    /// The waker every poll is handed, for landing values out of band.
    ///
    /// Waking it tells the pipeline that something it cannot see has moved, so
    /// the next [`poll`](Self::poll) at an unchanged version polls the graph
    /// rather than answering [`Run::Unchanged`] off the version alone. A stage
    /// that parks does this for itself through [`Ctx::waker`]; this is the same
    /// target, for whoever lands a value from outside the graph entirely.
    ///
    /// **There is no "has a wake arrived" accessor**, and its absence is the
    /// design: an answer that clears on read is a fact two readers race for,
    /// and the gate is the reader that must not lose. A caller with nothing
    /// else to ask polls every frame and lets `Unchanged` be the cheap answer,
    /// which is what that variant is for.
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

impl<V, I, O, E> Pipeline<V, I, O, E>
where
    V: Copy + Eq,
{
    /// Run once: the version gate, one poll, and the outcome.
    ///
    /// `version` says WHICH STATE the readable is; it is the only version in
    /// the API, and the pipeline never computes one - it compares the ones it
    /// is handed. That pairing is the point: the version costs a comparison,
    /// and the readable may be a large snapshot that a matching version never
    /// touches.
    ///
    /// **The gate answers [`Run::Unchanged`] only when the version matches AND
    /// no wake is pending** (`DESIGN.md`, "The version gate and the one door").
    /// Two different things mean "something happened" and only one of them
    /// moves the version: the input version moves when the source changes, and
    /// a wake arrives when a value some stage was waiting on has landed. A
    /// landed effect does not move the input version, so a gate that checked
    /// the version alone would take a pipeline that had answered at this
    /// version, receive the wake, and go on answering `Unchanged` forever -
    /// with the caller holding a value permanently one step stale and nothing
    /// reporting it.
    ///
    /// **The wake half is internal and has no accessor**, which is what keeps
    /// it honest: the flag clears when it is read, so a second reader is a
    /// second claimant on one wake. This method is the only reader, and it
    /// reads FIRST and unconditionally - a poll that entered the graph for a
    /// version change and left an unread wake behind would be the same defect
    /// one step displaced.
    ///
    /// Only [`Run::Computed`] records the version. A `Delayed` poll does not,
    /// so asking again with the same version polls again; a failure does not
    /// either, so a later poll with the same version retries.
    pub fn poll(&self, version: V, input: &I) -> RunResult<O, E> {
        let woken = self.frame.take_stale();
        if !woken && *self.last() == Some(version) {
            return Ok(Run::Unchanged);
        }
        match self.frame.poll_frame(&self.graph, input) {
            EffectPoll::Ready(StageAnswer::Computed(value)) => {
                *self.last() = Some(version);
                Ok(Run::Computed(value))
            }
            // **The graph reached the root without rewriting anything**, which
            // is the ladder's answer arriving at the door: the value the caller
            // holds derives from exactly this state, so the version is recorded
            // for it exactly as a computed one is. This is the second source of
            // `Unchanged` the version gate has always anticipated, and it is
            // the one that does not need the version to have stayed still.
            EffectPoll::Ready(StageAnswer::Unchanged) => {
                *self.last() = Some(version);
                Ok(Run::Unchanged)
            }
            EffectPoll::Pending => Ok(Run::Delayed),
            // A re-wrap, not a construction: the position was stamped at
            // registration, so the graph's own error type IS the flat
            // `Failure` and the door only moves it onto `Result`'s error side.
            EffectPoll::Failed(failure) => Err(failure),
        }
    }
}

/// The blocking caller pattern, as a function: poll, and while the answer is
/// [`Run::Delayed`], make the caller's own progress and poll again.
///
/// **It is not a second door**, and its body is the evidence: it calls
/// [`Pipeline::poll`] and nothing else. Two doors INTO the engine would make
/// waiting the pipeline's job, and the same state would then mean opposite
/// things at each - a poll that cannot progress is a defect to one caller and
/// an ordinary frame to the other. This function does not decide which; it
/// hands back the plain `Ok(Run::Delayed)` when `pump` says there is nothing
/// left to run, because a stall is a fact about the CALLER's queue, and only
/// the caller can see that it is empty.
///
/// `pump` makes progress on something a `Delayed` poll is waiting for and
/// answers `false` when there is nothing left to run. What it pumps is the
/// caller's executor, deliberately: the engine must never link one.
///
/// ```rust,ignore
/// let outcome = run_blocking(&pipeline, version, &document, || executor.run_once());
/// ```
pub fn run_blocking<V, I, O, E>(
    pipeline: &Pipeline<V, I, O, E>,
    version: V,
    input: &I,
    mut pump: impl FnMut() -> bool,
) -> RunResult<O, E>
where
    V: Copy + Eq,
{
    loop {
        match pipeline.poll(version, input) {
            Ok(Run::Delayed) if pump() => continue,
            done => break done,
        }
    }
}
