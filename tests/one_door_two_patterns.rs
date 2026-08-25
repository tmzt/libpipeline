//! Gate: **one door, two caller patterns, over the same graph.**
//!
//! There is one way to run a pipeline - `run(version, &input)`, which polls
//! once and returns immediately, whatever the answer. Blocking and frame
//! driving are things a CALLER does with that one call: a blocking caller loops
//! on `Run::Delayed` pumping its own executor, a frame caller runs once per
//! frame and draws its stand-in when the answer is `Delayed`. The graph below
//! is built once, from stages that know nothing about who is asking, and is run
//! both ways. If the two disagree about the answer, the claim is false.
//!
//! **Why it is one door and not two.** Two doors would make waiting the
//! pipeline's job, and the same state would then mean opposite things at each:
//! a poll that cannot progress is a defect to one caller and an ordinary frame
//! to the other. Only the caller can tell which, because only the caller can
//! see whether its queue is empty - which is why `a_stalled_graph_ends_rather_than_spinning`
//! below reads the stall off the caller's own condition rather than off a
//! variant the pipeline invented.
//!
//! **Every type the graph carries is a stand-in** (`DESIGN.md`, "The engine
//! stays generic"). `Source`, `Lowered` and `Emitted` are invented for this
//! file. That is the standing requirement, not a convenience: if the engine's
//! tests could not be written without a consumer's real types, the engine
//! would have learned something it must not know.
//!
//! # Everything here goes through the builder, and that is a second gate
//!
//! `DESIGN.md`: the builder is the only public way to compose, memoize or run.
//! This file names `PipelineBuilder`, `Pipeline`, `Run`, `RunResult` and
//! `Failure` and nothing else from the crate - which is now the WHOLE of what
//! the crate exports. So it is also the measurement that these properties are
//! expressible through the public door. A test in `tests/` proves the public
//! API reaches something; a test in `libpipeline-internals/tests/` admits it
//! does not yet.
//!
//! Four things changed shape in the conversions this file has been through, and
//! each is a consequence of the design rather than of this file:
//!
//! * **The stages' ids are minted at registration**, not declared as
//!   associated consts: an id is the position the builder counted to, and
//!   `Lower::new` and `Emit::new` take the one they are handed and answer it
//!   from `Stage::id`. There is no second id for a stage to disagree with, so
//!   nothing checks for a disagreement.
//! * **The run counts are handed IN.** The builder owns what it registers, so a
//!   test cannot reach back through the opaque graph for a counter a stage
//!   holds. [`Runs`] is that counter, shared.
//! * **Memoization is not composed here at all.** There is no `Memo::new` to
//!   write: registering a stage memoizes it. `.uncached()` is the control.
//! * **The executor seam is the caller's own.** `PendingWork` left the crate
//!   with the blocking door; [`Executor`] below is this file's, which is the
//!   point - a blocking caller owns its queue, and the engine never sees it.

use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::task::Waker;

use libpipeline::{Failure, Pipeline, PipelineBuilder, Run, RunResult};
use libpipelinedata::{ContentKey, EffectPoll, MemoKey, MemoStore, Stage, StageId};
use std::task::Context;

// ---------------------------------------------------------------- stand-ins

/// Stand-in for whatever an author wrote.
#[derive(Clone, PartialEq, Eq, Debug)]
struct Source(&'static str);

/// Stand-in for the first lowering's output.
#[derive(Clone, PartialEq, Eq, Debug)]
struct Lowered(Vec<String>);

/// Stand-in for the second lowering's output.
#[derive(Clone, PartialEq, Eq, Debug)]
struct Emitted(String);

/// How many times a stage was actually polled, readable after the builder has
/// taken the stage.
///
/// The builder owns what it registers - `.stage(..)` moves the stage into an
/// opaque graph - so a count kept inside the stage is unreachable once the
/// pipeline is built. Handing the cell in keeps the measurement without giving
/// the graph a door back out, which is the same trade the opaque graph type is
/// there to make.
#[derive(Clone, Default)]
struct Runs(Arc<Mutex<usize>>);

impl Runs {
    fn get(&self) -> usize {
        *self.0.lock().unwrap()
    }

    fn bump(&self) {
        *self.0.lock().unwrap() += 1;
    }
}

// -------------------------------------------------------------------- store

/// A memo store for the tests, and deliberately still its own implementation.
///
/// `libpipelinedata` ships real ones - `MemoMap`, and an optional ECS-backed
/// store, keyed by content hash - so this could now be swapped for `MemoMap`.
/// It is not, on purpose: an INDEPENDENT implementation on the other side of
/// the seam is the only thing here that shows `MemoStore` is implementable by
/// someone who did not write it, which is what a seam is for. `MemoMap` cannot
/// prove that about itself.
///
/// It reaches the graph through `PipelineBuilder::store` - the builder's one
/// door for a caller-provided store, taken once for the whole pipeline - so the
/// seam is still exercised through the public API rather than by hand-composing
/// a memo. What the builder instantiates it at is the ERASED row type
/// (`dyn Any + Send + Sync`), which is how one store serves stages of differing
/// output types.
///
/// **`V: ?Sized` is what it costs a generic store to serve the builder**, and
/// it is one word: the seam accepts and returns `Arc<V>`, so the erased row is
/// the unsized `dyn Any + Send + Sync` itself rather than a handle wrapped
/// around one, and recording is a coercion of the share the memo layer already
/// made. A store written for this builder alone would name that type directly
/// and need nothing generic at all.
struct MapStore<V: ?Sized> {
    rows: Mutex<HashMap<MemoKey, Arc<V>>>,
}

impl<V: ?Sized> MapStore<V> {
    fn new() -> Self {
        Self {
            rows: Mutex::new(HashMap::new()),
        }
    }
}

impl<V: ?Sized> MemoStore<V> for MapStore<V> {
    fn lookup(&self, key: &MemoKey) -> Option<Arc<V>> {
        self.rows.lock().unwrap().get(key).map(Arc::clone)
    }

    fn record(&self, key: &MemoKey, value: Arc<V>) {
        self.rows.lock().unwrap().insert(key.clone(), value);
    }
}

/// The store the builder is handed, at the type the builder instantiates it at.
fn erased_store() -> MapStore<dyn Any + Send + Sync> {
    MapStore::new()
}

// ------------------------------------------------------------------- stage 1

/// A pure stage: `Ready` on the first poll, keyed, never `Pending` - the shape
/// most stages have; only effectful ones ever park.
struct Lower {
    id: StageId,
    runs: Runs,
}

impl Lower {
    /// Written to be the builder's `make`: it takes the id the builder minted
    /// from this registration's position, and answers it from [`Stage::id`].
    fn new(id: StageId, runs: Runs) -> Self {
        Self { id, runs }
    }
}

impl Stage for Lower {
    type Input = Source;
    type Output = Lowered;
    type Error = &'static str;

    fn id(&self) -> StageId {
        self.id
    }

    fn memo_key(&self, input: &Source) -> Option<MemoKey> {
        // A stand-in for the real content hash (`ContentKey::of`, which streams
        // the value's fields through a hasher). What matters to this file is only
        // that equal inputs give equal keys and the key costs no run.
        Some(MemoKey::new(self.id, [content_key_of(input.0)]))
    }

    fn poll_stage(
        &self,
        input: &Source,
        _cx: &mut Context<'_>,
    ) -> EffectPoll<Lowered, &'static str> {
        self.runs.bump();
        if input.0.is_empty() {
            return EffectPoll::Failed("nothing to lower");
        }
        EffectPoll::Ready(Lowered(input.0.split('.').map(str::to_string).collect()))
    }
}

// ------------------------------------------------------------------- stage 2

/// The thing stage 2 is waiting on: an input that has not landed yet, holding
/// the wakers of everyone who asked for it early. The effectful,
/// pending-then-ready shape in miniature.
#[derive(Default)]
struct Upstream {
    value: Mutex<Option<&'static str>>,
    waiting: Mutex<Vec<Waker>>,
}

impl Upstream {
    /// The value arrives, and everyone who parked on it is told to run again.
    fn land(&self, value: &'static str) {
        *self.value.lock().unwrap() = Some(value);
        for waker in self.waiting.lock().unwrap().drain(..) {
            waker.wake();
        }
    }
}

/// An effectful stage: `Pending` until its upstream lands.
///
/// Its `Input` is `Lowered` - the value the stage before it produced, not the
/// share the graph carries it in. The engine wraps a stage's output once, where
/// it records it, and hands the next stage the value behind the share; neither
/// side of that is written by a stage author.
struct Emit {
    id: StageId,
    upstream: Arc<Upstream>,
    runs: Runs,
}

impl Emit {
    fn new(id: StageId, upstream: Arc<Upstream>, runs: Runs) -> Self {
        Self { id, upstream, runs }
    }
}

impl Stage for Emit {
    type Input = Lowered;
    type Output = Emitted;
    type Error = &'static str;

    fn id(&self) -> StageId {
        self.id
    }

    fn memo_key(&self, input: &Lowered) -> Option<MemoKey> {
        Some(MemoKey::new(self.id, [content_key_of(&input.0.join("."))]))
    }

    fn poll_stage(
        &self,
        input: &Lowered,
        cx: &mut Context<'_>,
    ) -> EffectPoll<Emitted, &'static str> {
        let Some(landed) = *self.upstream.value.lock().unwrap() else {
            self.upstream.waiting.lock().unwrap().push(cx.waker().clone());
            return EffectPoll::Pending;
        };
        self.runs.bump();
        EffectPoll::Ready(Emitted(format!("{landed}::{}", input.0.join("/"))))
    }
}

/// Stand-in for the streaming content hash - FNV over the bytes. Not a content
/// address and not claiming to be one; the property this file needs is that
/// equal inputs key equally.
fn content_key_of(text: &str) -> ContentKey {
    let mut h: u128 = 0x6c62_272e_07bb_0142_62b8_2175_6295_c58du128;
    for byte in text.as_bytes() {
        h ^= u128::from(*byte);
        h = h.wrapping_mul(0x0000_0000_0100_0000_0000_0000_0000_013bu128);
    }
    ContentKey::from_u128(h)
}

// -------------------------------------------------------------------- graphs

/// The failure type a registration produces, whatever the length of the chain:
/// the pipeline's one error type, with the failing stage's position stamped on
/// it. A five-stage graph spells this the same way a two-stage one does.
type PipelineError = Failure<&'static str>;

/// The version type these pipelines are built at. Where a version comes from is
/// the consumer's business - a cursor, a build number, a git sha - and the
/// engine only ever compares the ones it is handed.
type Version = u64;

/// The position and error a run ended on.
///
/// [`Failure`]'s fields are private and it has no constructor - a position is
/// the builder's to stamp - so a test READS one rather than building one to
/// compare against.
fn failed<T: std::fmt::Debug>(outcome: RunResult<T, &'static str>) -> (usize, &'static str) {
    match outcome {
        Err(failure) => (failure.at(), *failure.error()),
        other => panic!("expected a failed run, got {other:?}"),
    }
}

/// The value a run computed, or a panic naming what it answered instead.
fn computed<T: std::fmt::Debug>(outcome: RunResult<T, &'static str>) -> Arc<T> {
    match outcome {
        Ok(Run::Computed(value)) => value,
        other => panic!("expected a computed run, got {other:?}"),
    }
}

/// The two-stage graph, registered: `lower` then `emit`, both memoized in the
/// one [`MapStore`] the caller hands the builder.
///
/// **The store is chosen once**, before any registration, and the two stages
/// share it - the whole-pipeline decision, taken where `DESIGN.md` puts it.
/// The names are labels: nothing here is looked up by one.
fn pipeline(
    upstream: Arc<Upstream>,
    lower_runs: Runs,
    emit_runs: Runs,
) -> Pipeline<Version, impl Stage<Input = Source, Output = Arc<Emitted>, Error = PipelineError>> {
    PipelineBuilder::new()
        .store(erased_store())
        .stage("test.lower", move |id| Lower::new(id, lower_runs))
        .stage("test.emit", move |id| Emit::new(id, upstream, emit_runs))
        .build()
}

/// [`pipeline`] with the counts thrown away, for the tests that measure answers
/// rather than runs.
fn graph(
    upstream: Arc<Upstream>,
) -> Pipeline<Version, impl Stage<Input = Source, Output = Arc<Emitted>, Error = PipelineError>> {
    pipeline(upstream, Runs::default(), Runs::default())
}

/// Just the first stage, registered on its own - for the memo properties, which
/// are about ONE stage's store and would be reported by a chain only
/// indirectly.
fn lowering(
    runs: Runs,
    cached: bool,
) -> Pipeline<Version, impl Stage<Input = Source, Output = Arc<Lowered>, Error = PipelineError>> {
    let builder = if cached {
        PipelineBuilder::new()
    } else {
        // `uncached` wins over `store` whichever order they are called in: the
        // control run controls for every store.
        PipelineBuilder::new().uncached()
    };
    builder
        .store(erased_store())
        .stage("test.lower", move |id| Lower::new(id, runs))
        .build()
}

// ------------------------------------------------------------------ patterns

/// The caller's own executor. It left the crate with the blocking door: what a
/// blocking caller pumps between runs is its queue, not the engine's, and the
/// engine must never link one.
trait Executor {
    /// Make progress on something a `Delayed` run is waiting for. `false` means
    /// there is nothing left to run.
    fn run_once(&self) -> bool;
}

/// The blocking caller pattern, exactly as `DESIGN.md` writes it: loop on
/// `Delayed`, making the caller's own progress between runs.
///
/// The second arm is what makes this the caller's decision rather than the
/// pipeline's: `Delayed` when there is nothing left to run means something
/// waited for an input nothing was going to land, and only the caller that owns
/// the executor can see that its queue is empty. The answer this hands back in
/// that case is the plain `Ok(Run::Delayed)` - no `Stalled` variant, because a
/// stall is a fact about the caller, not about the graph.
fn blocking<S, O, W>(
    pipeline: &Pipeline<Version, S>,
    version: Version,
    input: &S::Input,
    work: &W,
) -> RunResult<O, &'static str>
where
    S: Stage<Output = Arc<O>, Error = PipelineError>,
    W: Executor + ?Sized,
{
    loop {
        match pipeline.run(version, input) {
            Ok(Run::Delayed) if work.run_once() => continue,
            done => break done,
        }
    }
}

/// Nothing to pump: for a graph of pure stages, where `Delayed` can only mean
/// the graph is waiting for something nobody will land.
struct NoWork;

impl Executor for NoWork {
    fn run_once(&self) -> bool {
        false
    }
}

/// The frame caller pattern, AFTER a `Delayed`, and the reason it is written
/// this way: **it runs only because a wake said stale.**
///
/// Every run here is guarded by `take_stale`, so if the waker were torn out this
/// returns `None` no matter how many frames it is given. Asserting that a wake
/// arrived and then running anyway would have proved nothing: the test would be
/// re-running on a hunch and reading the wake as decoration. This helper's first
/// draft did exactly that - it opened with an unguarded run - and
/// `a_pending_stage_that_registers_no_waker_is_a_value_lost_rather_than_late`
/// caught it, which is the whole reason that control test is here.
///
/// **The guard consumes the wake, and `run` would have consumed it too.** The
/// flag clears on read and both of them read it, so a caller that asks must not
/// then expect `run` to notice the same wake; here it does not matter, because
/// the pipeline has never computed for this version and the gate has nothing to
/// mistake for `Unchanged`. A frame caller with no such question to ask should
/// simply run every frame and let `Unchanged` be the cheap answer.
///
/// `None` means "still delayed, nobody woke us". That is a legitimate outcome
/// for a real frame - it keeps its stand-in - and a failure for a test that
/// expected a value.
fn frames_when_woken<S, O>(
    pipeline: &Pipeline<Version, S>,
    version: Version,
    input: &S::Input,
    max_frames: usize,
) -> Option<Arc<O>>
where
    S: Stage<Output = Arc<O>, Error = PipelineError>,
{
    for _ in 0..max_frames {
        if !pipeline.take_stale() {
            return None;
        }
        if let Ok(Run::Computed(value)) = pipeline.run(version, input) {
            return Some(value);
        }
    }
    None
}

/// The blocking caller's executor: it lands the upstream the first time it is
/// pumped, then has nothing left to do.
struct LandsOnFirstPump {
    upstream: Arc<Upstream>,
    landed: Mutex<bool>,
}

impl LandsOnFirstPump {
    fn new(upstream: Arc<Upstream>) -> Self {
        Self {
            upstream,
            landed: Mutex::new(false),
        }
    }
}

impl Executor for LandsOnFirstPump {
    fn run_once(&self) -> bool {
        let mut landed = self.landed.lock().unwrap();
        if *landed {
            return false;
        }
        *landed = true;
        self.upstream.land("built");
        true
    }
}

// --------------------------------------------------------------------- gate

#[test]
fn a_blocking_caller_loops_on_delayed_until_the_graph_answers() {
    let upstream = Arc::new(Upstream::default());
    let graph = graph(Arc::clone(&upstream));
    let work = LandsOnFirstPump::new(upstream);

    let out = blocking(&graph, 1, &Source("doc.title"), &work);
    assert_eq!(*computed(out), Emitted("built::doc/title".to_string()));
}

#[test]
fn a_frame_caller_is_delayed_wakes_and_runs_again_to_computed() {
    let upstream = Arc::new(Upstream::default());
    let graph = graph(Arc::clone(&upstream));
    let input = Source("doc.title");

    // Frame 1: the upstream has not landed. The frame does not block; it would
    // draw a stand-in and return.
    assert_eq!(graph.run(1, &input), Ok(Run::Delayed));
    assert!(
        !graph.take_stale(),
        "a Delayed run is not itself a wake - something has to land",
    );

    // Between frames, the upstream lands and wakes what parked on it.
    upstream.land("built");
    assert!(graph.take_stale(), "the wake reached the frame loop");

    // Frame 2: the same graph, run again, now answers.
    assert_eq!(
        graph.run(1, &input),
        Ok(Run::Computed(Arc::new(Emitted("built::doc/title".to_string())))),
    );
}

#[test]
fn both_patterns_give_the_same_answer_for_the_same_graph() {
    // The one-door rule, stated as an equality. The two graphs are built by the
    // same function from the same input; only what the CALLER does with `run`
    // differs.
    let input = Source("doc.section.title");

    let offline_upstream = Arc::new(Upstream::default());
    let offline_graph = graph(Arc::clone(&offline_upstream));
    let offline = computed(blocking(
        &offline_graph,
        1,
        &input,
        &LandsOnFirstPump::new(offline_upstream),
    ));

    let live_upstream = Arc::new(Upstream::default());
    let live_graph = graph(Arc::clone(&live_upstream));

    // Frame 1 is delayed. Between frames the upstream lands and wakes; every
    // later frame happens only because of that wake - see frames_when_woken.
    assert_eq!(live_graph.run(1, &input), Ok(Run::Delayed));
    live_upstream.land("built");
    let live = frames_when_woken(&live_graph, 1, &input, 4).expect("the woken frame reaches a value");

    assert_eq!(offline, live);
}

/// The same stage as [`Emit`], minus the one line that registers the waker.
///
/// It exists to measure, rather than reason about, what `frames_when_woken`
/// depends on: with this in the graph the value LANDS and the frame loop still
/// never sees it. That turns "the wake is load-bearing" from a claim about the
/// test into an observation of it.
struct ForgetfulEmit {
    id: StageId,
    upstream: Arc<Upstream>,
}

impl Stage for ForgetfulEmit {
    type Input = Lowered;
    type Output = Emitted;
    type Error = &'static str;

    fn id(&self) -> StageId {
        self.id
    }

    /// Refuses to key, which is also how a registered stage opts OUT of the
    /// memoization registration applies: `DESIGN.md`'s "a stage that must not be
    /// served from cache says so through `memo_key -> None`".
    fn memo_key(&self, _input: &Lowered) -> Option<MemoKey> {
        None
    }

    fn poll_stage(
        &self,
        input: &Lowered,
        _cx: &mut Context<'_>,
    ) -> EffectPoll<Emitted, &'static str> {
        let Some(landed) = *self.upstream.value.lock().unwrap() else {
            return EffectPoll::Pending;
        };
        EffectPoll::Ready(Emitted(format!("{landed}::{}", input.0.join("/"))))
    }
}

#[test]
fn a_pending_stage_that_registers_no_waker_is_a_value_lost_rather_than_late() {
    // `Stage::poll_stage`'s doc makes registering an obligation. This is what
    // breaking it costs, and it is the control that gives the wake in
    // `both_patterns_give_the_same_answer_for_the_same_graph` its meaning: the
    // upstream lands, the value is there for the asking, and no frame loop will
    // ever ask because nothing told it to.
    //
    // The measurement is `take_stale` staying FALSE. That is the whole of what
    // a frame loop has to go on: `Run::Delayed` promises a wake is coming, and
    // the flag is where the promise is either kept or not.
    let upstream = Arc::new(Upstream::default());
    let forgetful = Arc::clone(&upstream);
    let graph = PipelineBuilder::new()
        .store(erased_store())
        .stage("test.lower", |id| Lower::new(id, Runs::default()))
        .stage("test.emit_without_registering", move |id| ForgetfulEmit {
            id,
            upstream: forgetful,
        })
        .build();
    let input = Source("doc.title");

    assert_eq!(graph.run(1, &input), Ok(Run::Delayed));
    upstream.land("built");
    assert!(
        !graph.take_stale(),
        "the value landed and nothing can tell the frame loop; it is \
         unreachable, not merely late",
    );
    assert_eq!(
        frames_when_woken(&graph, 1, &input, 100),
        None,
        "so no number of frames reaches it",
    );

    // A blocking caller is unaffected, because it runs again without being
    // asked - which is exactly why a missing registration is invisible to a CLI
    // run and fatal in the IDE. Worth knowing before a real stage forgets.
    let out = blocking(&graph, 1, &input, &LandsOnFirstPump::new(upstream));
    assert_eq!(*computed(out), Emitted("built::doc/title".to_string()));
}

#[test]
fn the_frame_loop_cannot_advance_without_a_wake() {
    // The control for frames_when_woken: nothing lands, so nothing wakes, so no
    // number of frames produces a value.
    let graph = graph(Arc::new(Upstream::default()));
    let input = Source("doc.title");
    assert_eq!(graph.run(1, &input), Ok(Run::Delayed));
    assert_eq!(frames_when_woken(&graph, 1, &input, 100), None);
}

#[test]
fn a_stalled_graph_ends_rather_than_spinning() {
    // Nothing lands, so the blocking caller has no work left after the first
    // `Delayed`. For that caller this is a graph bug; for a frame loop the same
    // state is the ordinary "keep the stand-in" case - which is why the
    // pipeline answers the same thing to both and the caller draws its own
    // conclusion.
    let graph = graph(Arc::new(Upstream::default()));
    let out = blocking(&graph, 1, &Source("doc.title"), &NoWork);
    assert_eq!(out, Ok(Run::Delayed));
}

#[test]
fn a_failure_bubbles_out_positioned_at_the_stage_that_raised_it() {
    let upstream = Arc::new(Upstream::default());
    upstream.land("built");
    let graph = graph(upstream);
    // The first stage failed and the second never ran. "Which one" is a
    // position read in one call, at any length of chain - not a count of
    // `First`/`Second` layers.
    assert_eq!(
        failed(blocking(&graph, 1, &Source(""), &NoWork)),
        (0, "nothing to lower"),
    );
}

#[test]
fn the_memo_serves_the_second_run_without_re_running_the_stage() {
    // Registration memoizes - there is no `Memo` to compose here - so a run
    // over the same CONTENT at a moved version is served by the lookup that
    // precedes the work. The version has to move, or the gate above the graph
    // would answer first and the memo would never be asked.
    let runs = Runs::default();
    let stage = lowering(runs.clone(), true);
    let input = Source("doc.title");

    let first = computed(blocking(&stage, 1, &input, &NoWork));
    let second = computed(blocking(&stage, 2, &input, &NoWork));

    assert_eq!(first, second);
    assert_eq!(
        runs.get(),
        1,
        "the second run was served by the lookup that precedes the work",
    );
}

#[test]
fn one_memo_serves_both_patterns_and_the_stage_runs_once() {
    // The two patterns over ONE registered stage, which
    // `both_patterns_give_the_same_answer_for_the_same_graph` does not cover:
    // it builds a graph per pattern, so each has its own store and neither hits
    // on the other's work. Here the blocking run fills the store and the frame
    // run is served by it - a batch run against an unchanged tree is all cache
    // hits, because the memo keys are the same ones the interactive host used -
    // measured rather than asserted about.
    //
    // **This is the public-API expression of what the retired ledger test
    // measured** (`PLAN.md`, "The ledger test, measured"). That test wrapped the
    // same shape in a `Ledger` scope over a stage that read no tracked state,
    // and mutation testing showed the scope was inert in it: the known-bad
    // `Memo::new(Tracked::new(..), ..)` order passed it, and so did deleting the
    // tracking outright. What it actually held is below, and the builder can say
    // all of it.
    let input = Source("doc.title");
    let runs = Runs::default();
    let stage = lowering(runs.clone(), true);

    let first = computed(blocking(&stage, 1, &input, &NoWork));
    // The frame pattern, at a moved version so the graph is actually entered.
    let second = computed(stage.run(2, &input));
    assert!(
        !stage.take_stale(),
        "the stage answered without waiting, so nothing should have asked for a \
         redraw",
    );

    assert_eq!(
        runs.get(),
        1,
        "the frame run was served by what the blocking run recorded - one \
         store, one key, either caller",
    );
    assert_eq!(first, second, "and the caller must not change what a stage answers");

    // The control run: same answer with every store off. A pipeline whose
    // ANSWERS move when the cache is removed has a bug the cache was hiding.
    let control_runs = Runs::default();
    let control = lowering(control_runs.clone(), false);
    assert_eq!(
        computed(blocking(&control, 1, &input, &NoWork)),
        first,
        "nor may the memo differ from what the uncached stage says",
    );
    assert_eq!(control_runs.get(), 1, "the control ran, rather than hitting");
}

#[test]
fn the_memo_changes_speed_and_not_answers() {
    // `uncached` is the control case (DESIGN.md): if the answers move when the
    // cache is removed, the cache was part of the semantics.
    let input = Source("doc.title");
    let cached_runs = Runs::default();
    let uncached_runs = Runs::default();
    let cached = lowering(cached_runs.clone(), true);
    let uncached = lowering(uncached_runs.clone(), false);

    // Each round moves the version, so the answers below are the GRAPH's rather
    // than the gate's.
    for version in 1..=3 {
        assert_eq!(
            computed(blocking(&cached, version, &input, &NoWork)),
            computed(blocking(&uncached, version, &input, &NoWork)),
        );
    }
    assert_eq!(cached_runs.get(), 1);
    assert_eq!(uncached_runs.get(), 3, "the control ran every time");
}

#[test]
fn a_failure_is_never_cached() {
    // The standing rule: effects are never replayed by an implicit cache. A
    // transient failure served back from a memo would be exactly that. Both
    // runs move the version, so what is measured is the store rather than the
    // gate - and a failure records no version either, which is why the second
    // run would reach the graph even at the same one.
    let runs = Runs::default();
    let stage = lowering(runs.clone(), true);

    assert_eq!(
        failed(blocking(&stage, 1, &Source(""), &NoWork)),
        (0, "nothing to lower"),
    );
    assert_eq!(
        failed(blocking(&stage, 2, &Source(""), &NoWork)),
        (0, "nothing to lower"),
    );
    assert_eq!(
        runs.get(),
        2,
        "a cached failure would have made the second run free, and a transient \
         failure would then outlive its cause",
    );
}

#[test]
fn the_effectful_stage_runs_once_across_a_park_and_a_wake() {
    // The park/wake round trip must not double-run the effect: the Delayed run
    // did not run it, and the memo means the woken run runs it exactly once.
    let upstream = Arc::new(Upstream::default());
    let lower_runs = Runs::default();
    let emit_runs = Runs::default();
    let graph = pipeline(
        Arc::clone(&upstream),
        lower_runs.clone(),
        emit_runs.clone(),
    );
    let input = Source("doc.title");

    assert_eq!(graph.run(1, &input), Ok(Run::Delayed));
    upstream.land("built");
    assert!(matches!(graph.run(1, &input), Ok(Run::Computed(_))));
    // A moved version, so the run reaches the graph rather than the gate: the
    // memo is what keeps the effect from running a second time.
    assert!(matches!(graph.run(2, &input), Ok(Run::Computed(_))));

    assert_eq!(emit_runs.get(), 1);
    assert_eq!(
        lower_runs.get(),
        1,
        "the pure first stage ran on the Delayed run and was memoized for the rest",
    );
}
