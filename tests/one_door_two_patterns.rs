//! Gate: **one door, two caller patterns, over the same graph.**
//!
//! There is one way to run a pipeline - `poll(version, &input)`, which polls
//! once and returns immediately, whatever the answer. Blocking and frame
//! driving are things a CALLER does with that one call: a blocking caller loops
//! on `Run::Delayed` pumping its own executor, which is exactly what the
//! crate-level `run_blocking` is; a frame caller polls once per frame and draws
//! its stand-in when the answer is `Delayed`. The graph below is built once,
//! from stages that know nothing about who is asking, and is run both ways. If
//! the two disagree about the answer, the claim is false.
//!
//! **Why `run_blocking` is not a second door.** Its body is a loop over `poll`
//! and nothing else. Two doors INTO the engine would make waiting the
//! pipeline's job, and the same state would then mean opposite things at each:
//! a poll that cannot progress is a defect to one caller and an ordinary frame
//! to the other. Only the caller can tell which, because only the caller can
//! see whether its queue is empty - which is why `a_stalled_graph_ends_rather_than_spinning`
//! below reads the stall off the caller's own pump rather than off a variant
//! the pipeline invented.
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
//! This file names `PipelineBuilder`, `Pipeline`, `Ctx`, `Run`, `RunResult`,
//! `Failure`'s reader and `run_blocking` and nothing else from the crate - which is now
//! the WHOLE of what the crate exports. So it is also the measurement that
//! these properties are expressible through the public door. A test in `tests/`
//! proves the public API reaches something; a test in
//! `libpipeline-internals/tests/` admits it does not yet.
//!
//! Five things changed shape in the conversions this file has been through, and
//! each is a consequence of the design rather than of this file:
//!
//! * **A stage is two `fn` pointers**, not a struct implementing a trait: a key
//!   function and a poll function, neither able to capture. `Lower` and `Emit`
//!   below are pairs of free functions.
//! * **The stages' ids are minted at registration** and reach the functions
//!   through `Ctx`. There is no second id for a stage to disagree with, so
//!   nothing checks for a disagreement.
//! * **The run counts and the upstream are thread-locals.** A `fn` cannot carry
//!   a field, and libtest gives each test its own thread. What a real consumer
//!   would need instead is a route through `Ctx`, which `DESIGN.md` describes
//!   and the crate does not have; that gap is the finding, not this file's
//!   arrangement.
//! * **Memoization is not composed here at all.** There is no `Memo::new` to
//!   write: registering a stage memoizes it. `.uncached()` is the control.
//! * **The executor seam is the caller's own.** `run_blocking` takes a closure
//!   that pumps whatever the caller owns, and the engine never sees it.
//!
//! **What this file cannot state, and where it is stated instead.** That a
//! CAPTURING closure is refused is a compile-fail property, and a compile-fail
//! harness (`trybuild`) is a dependency `tests/engine_stays_generic.rs`
//! forbids this stack from taking. `a_capturing_closure_is_not_a_stage` in
//! `tests/builder_is_the_only_door.rs` holds the half that compiles - a
//! non-capturing closure coerces - and the refusal is recorded rather than
//! measured.

use std::any::Any;
use std::cell::Cell;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::task::Waker;

use libpipeline::{Ctx, Pipeline, PipelineBuilder, Run, RunResult, run_blocking};
use libpipelinedata::{ContentKey, EffectPoll, MemoKey, MemoStore, StageAnswer};

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

thread_local! {
    /// How many times the first stage actually ran.
    static LOWER_RUNS: Cell<usize> = const { Cell::new(0) };
    /// How many times the second stage actually ran.
    static EMIT_RUNS: Cell<usize> = const { Cell::new(0) };
    /// The thing stage 2 is waiting on.
    static UPSTREAM: Arc<Upstream> = Arc::new(Upstream::default());
}

fn lower_runs() -> usize {
    LOWER_RUNS.with(Cell::get)
}

fn emit_runs() -> usize {
    EMIT_RUNS.with(Cell::get)
}

fn upstream() -> Arc<Upstream> {
    UPSTREAM.with(Arc::clone)
}

// -------------------------------------------------------------------- store

/// A memo store for the tests, and deliberately still its own implementation.
///
/// `libpipelinedata` ships real ones - `MemoMap`, and an optional ECS-backed
/// store - so this could be swapped for `MemoMap`. It is not, on purpose: an
/// INDEPENDENT implementation on the other side of the seam is the only thing
/// here that shows `MemoStore` is implementable by someone who did not write
/// it, which is what a seam is for. `MemoMap` cannot prove that about itself.
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
/// around one, and recording is a coercion of the share the stage already
/// answered with. A store written for this builder alone would name that type
/// directly and need nothing generic at all.
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
///
/// The key is built through `Ctx::key`, which supplies the identity half: a
/// stage never mints its own position and has no way to key under another's.
fn lower_key(input: &Source, ctx: &Ctx<'_>) -> Option<MemoKey> {
    // A stand-in for a real content address. What matters to this file is only
    // that equal inputs give equal keys and the key costs no run.
    Some(ctx.key([content_key_of(input.0)]))
}

fn lower_poll(input: &Source, _ctx: &Ctx<'_>) -> EffectPoll<StageAnswer<Lowered>, &'static str> {
    LOWER_RUNS.with(|c| c.set(c.get() + 1));
    if input.0.is_empty() {
        return EffectPoll::Failed("nothing to lower");
    }
    StageAnswer::computed(Lowered(input.0.split('.').map(str::to_string).collect()))
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
    /// The value arrives, and everyone who parked on it is told to poll again.
    fn land(&self, value: &'static str) {
        *self.value.lock().unwrap() = Some(value);
        for waker in self.waiting.lock().unwrap().drain(..) {
            waker.wake();
        }
    }
}

/// An effectful stage: `Pending` until its upstream lands.
///
/// Its input is `Lowered` - the value the stage before it produced, not the
/// share the graph carries it in. The engine wraps a stage's output once, where
/// the value enters the graph, and hands the next stage the value behind the
/// share; neither side of that is written by a stage author.
fn emit_key(input: &Lowered, ctx: &Ctx<'_>) -> Option<MemoKey> {
    Some(ctx.key([content_key_of(&input.0.join("."))]))
}

fn emit_poll(input: &Lowered, ctx: &Ctx<'_>) -> EffectPoll<StageAnswer<Emitted>, &'static str> {
    let upstream = upstream();
    let Some(landed) = *upstream.value.lock().unwrap() else {
        // Answering `Pending` obliges the stage to arrange a wake, and
        // `Ctx::waker` is the target.
        upstream.waiting.lock().unwrap().push(ctx.waker().clone());
        return EffectPoll::Pending;
    };
    EMIT_RUNS.with(|c| c.set(c.get() + 1));
    StageAnswer::computed(Emitted(format!("{landed}::{}", input.0.join("/"))))
}

/// The same stage minus the one line that registers the waker.
///
/// It exists to measure, rather than reason about, what a wake is worth to each
/// caller: a blocking caller polls again without being asked and never notices,
/// which is exactly why a missing registration is invisible to a CLI run and
/// fatal in an interactive host.
fn forgetful_emit_key(_input: &Lowered, _ctx: &Ctx<'_>) -> Option<MemoKey> {
    // Refusing to key is also how a registered stage opts OUT of the
    // memoization registration applies.
    None
}

fn forgetful_emit_poll(input: &Lowered, _ctx: &Ctx<'_>) -> EffectPoll<StageAnswer<Emitted>, &'static str> {
    let Some(landed) = *upstream().value.lock().unwrap() else {
        return EffectPoll::Pending;
    };
    StageAnswer::computed(Emitted(format!("{landed}::{}", input.0.join("/"))))
}

/// Stand-in for a streaming content hash - FNV over the bytes. Not a content
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

/// The version type these pipelines are built at. Where a version comes from is
/// the consumer's business - a cursor, a build number, a git sha - and the
/// engine only ever compares the ones it is handed.
type Version = u64;

/// The position and error a poll ended on.
///
/// [`Failure`]'s fields are private and it has no constructor - a position is
/// the builder's to stamp - so a test READS one rather than building one to
/// compare against.
fn failed<T: std::fmt::Debug>(outcome: RunResult<T, &'static str>) -> (usize, &'static str) {
    match outcome {
        Err(failure) => (failure.at(), *failure.error()),
        other => panic!("expected a failed poll, got {other:?}"),
    }
}

/// The value a poll computed, or a panic naming what it answered instead.
fn computed<T: std::fmt::Debug>(outcome: RunResult<T, &'static str>) -> Arc<T> {
    match outcome {
        Ok(Run::Computed(value)) => value,
        other => panic!("expected a computed poll, got {other:?}"),
    }
}

/// The two-stage graph, registered: `lower` then `emit`, both memoized in the
/// one [`MapStore`] the caller hands the builder.
///
/// **The store is chosen once**, before any registration, and the two stages
/// share it - the whole-pipeline decision, taken where `DESIGN.md` puts it.
/// The names are labels: nothing here is looked up by one.
///
/// **The return type names no trait**, which is the other half of this wave:
/// the pipeline is spelled in the consumer's own types, because the stage
/// contract is `libpipeline-internals`' and a public signature may not reach
/// for it.
fn pipeline() -> Pipeline<Version, Source, Emitted, &'static str> {
    PipelineBuilder::new()
        .store(erased_store())
        .stage_fn("test.lower", lower_key, lower_poll)
        .stage_fn("test.emit", emit_key, emit_poll)
        .build()
}

/// Just the first stage, registered on its own - for the memo properties, which
/// are about ONE stage's store and would be reported by a chain only
/// indirectly.
fn lowering(cached: bool) -> Pipeline<Version, Source, Lowered, &'static str> {
    let builder = if cached {
        PipelineBuilder::new()
    } else {
        // `uncached` wins over `store` whichever order they are called in: the
        // control run controls for every store.
        PipelineBuilder::new().uncached()
    };
    builder
        .store(erased_store())
        .stage_fn("test.lower", lower_key, lower_poll)
        .build()
}

// ------------------------------------------------------------------ patterns

/// A pump that lands the upstream the first time it is called, then has nothing
/// left to do. The blocking caller's executor, in miniature.
fn lands_once() -> impl FnMut() -> bool {
    let mut landed = false;
    move || {
        if landed {
            return false;
        }
        landed = true;
        upstream().land("built");
        true
    }
}

/// Nothing to pump: for a graph whose `Delayed` can only mean it is waiting for
/// something nobody will land.
fn no_work() -> impl FnMut() -> bool {
    || false
}

/// The frame caller pattern: poll once per frame, up to `max_frames`, and stop
/// at the first value.
///
/// `None` means "still delayed". That is a legitimate outcome for a real frame
/// - it keeps its stand-in - and a failure for a test that expected a value.
fn frames<O>(
    pipeline: &Pipeline<Version, Source, O, &'static str>,
    version: Version,
    input: &Source,
    max_frames: usize,
) -> Option<Arc<O>> {
    for _ in 0..max_frames {
        if let Ok(Run::Computed(value)) = pipeline.poll(version, input) {
            return Some(value);
        }
    }
    None
}

// --------------------------------------------------------------------- gate

#[test]
fn a_blocking_caller_loops_on_delayed_until_the_graph_answers() {
    let graph = pipeline();
    let out = run_blocking(&graph, 1, &Source("doc.title"), lands_once());
    assert_eq!(*computed(out), Emitted("built::doc/title".to_string()));
}

#[test]
fn a_frame_caller_is_delayed_wakes_and_polls_again_to_computed() {
    let graph = pipeline();
    let input = Source("doc.title");

    // Frame 1: the upstream has not landed. The frame does not block; it would
    // draw a stand-in and return.
    assert_eq!(graph.poll(1, &input), Ok(Run::Delayed));

    // Between frames, the upstream lands and wakes what parked on it.
    upstream().land("built");

    // Frame 2: the same graph, polled again, now answers.
    assert_eq!(
        graph.poll(1, &input),
        Ok(Run::Computed(Arc::new(Emitted("built::doc/title".to_string())))),
    );
}

#[test]
fn both_patterns_give_the_same_answer_for_the_same_graph() {
    // The one-door rule, stated as an equality. The two graphs are built by the
    // same function; only what the CALLER does with `poll` differs.
    let input = Source("doc.section.title");

    let offline_graph = pipeline();
    let offline = computed(run_blocking(&offline_graph, 1, &input, lands_once()));

    // A fresh upstream for the live half, so the frame caller starts from the
    // same "nothing has landed" state the blocking one did.
    UPSTREAM.with(|slot| *slot.value.lock().unwrap() = None);
    let live_graph = pipeline();

    // Frame 1 is delayed. Between frames the upstream lands and wakes.
    assert_eq!(live_graph.poll(1, &input), Ok(Run::Delayed));
    upstream().land("built");
    let live = frames(&live_graph, 1, &input, 4).expect("the woken frame reaches a value");

    assert_eq!(offline, live);
}

#[test]
fn a_missing_wake_is_invisible_to_a_blocking_caller() {
    // A `Delayed` that owes a wake and leaves none is a value LOST rather than
    // late - and the loss is a FRAME caller's, not a blocking one's, because a
    // blocking caller polls again without being asked. That asymmetry is why a
    // missing registration is invisible to a CLI run and fatal in the IDE.
    //
    // What the loss looks like from the frame side is
    // `a_stage_that_forgets_its_waker_makes_its_value_lost_rather_than_late`
    // (`tests/builder_is_the_only_door.rs`), which measures it in the OUTCOME -
    // the only place a caller sees anything now that no flag is exposed.
    let graph = PipelineBuilder::new()
        .store(erased_store())
        .stage_fn("test.lower", lower_key, lower_poll)
        .stage_fn("test.emit_without_registering", forgetful_emit_key, forgetful_emit_poll)
        .build();
    let input = Source("doc.title");

    assert_eq!(graph.poll(1, &input), Ok(Run::Delayed));
    let out = run_blocking(&graph, 1, &input, lands_once());
    assert_eq!(*computed(out), Emitted("built::doc/title".to_string()));
}

#[test]
fn a_frame_caller_keeps_its_stand_in_while_nothing_lands() {
    // Nothing lands, so no number of frames produces a value: `Delayed` is a
    // standing answer, not a transient one, and a frame loop is expected to
    // draw its stand-in for as long as it lasts.
    let graph = pipeline();
    let input = Source("doc.title");
    assert_eq!(graph.poll(1, &input), Ok(Run::Delayed));
    assert_eq!(frames(&graph, 1, &input, 100), None);
}

#[test]
fn a_stalled_graph_ends_rather_than_spinning() {
    // Nothing lands, so the blocking caller has no work left after the first
    // `Delayed`. For that caller this is a graph bug; for a frame loop the same
    // state is the ordinary "keep the stand-in" case - which is why the
    // pipeline answers the same thing to both and the caller draws its own
    // conclusion.
    let graph = pipeline();
    let out = run_blocking(&graph, 1, &Source("doc.title"), no_work());
    assert_eq!(out, Ok(Run::Delayed));
}

#[test]
fn a_failure_bubbles_out_positioned_at_the_stage_that_raised_it() {
    upstream().land("built");
    let graph = pipeline();
    // The first stage failed and the second never ran. "Which one" is a
    // position read in one call, at any length of chain - not a count of
    // `First`/`Second` layers.
    assert_eq!(
        failed(run_blocking(&graph, 1, &Source(""), no_work())),
        (0, "nothing to lower"),
    );
}

#[test]
fn the_memo_serves_the_second_poll_without_re_running_the_stage() {
    // Registration memoizes - there is no `Memo` to compose here - so a poll
    // over the same CONTENT at a moved version is served by the lookup that
    // precedes the work. The version has to move, or the gate above the graph
    // would answer first and the memo would never be asked.
    let stage = lowering(true);
    let input = Source("doc.title");

    let first = computed(run_blocking(&stage, 1, &input, no_work()));
    let second = computed(run_blocking(&stage, 2, &input, no_work()));

    assert_eq!(first, second);
    assert_eq!(
        lower_runs(),
        1,
        "the second poll was served by the lookup that precedes the work",
    );
}

#[test]
fn one_memo_serves_both_patterns_and_the_stage_runs_once() {
    // The two patterns over ONE registered stage, which
    // `both_patterns_give_the_same_answer_for_the_same_graph` does not cover:
    // it builds a graph per pattern, so each has its own store and neither hits
    // on the other's work. Here the blocking poll fills the store and the frame
    // poll is served by it - a batch run against an unchanged tree is all cache
    // hits, because the memo keys are the same ones the interactive host used -
    // measured rather than asserted about.
    let input = Source("doc.title");
    let stage = lowering(true);

    let first = computed(run_blocking(&stage, 1, &input, no_work()));
    // The frame pattern, at a moved version so the graph is actually entered.
    let second = computed(stage.poll(2, &input));

    assert_eq!(
        lower_runs(),
        1,
        "the frame poll was served by what the blocking poll recorded - one \
         store, one key, either caller",
    );
    assert_eq!(first, second, "and the caller must not change what a stage answers");

    // The control run: same answer with every store off. A pipeline whose
    // ANSWERS move when the cache is removed has a bug the cache was hiding.
    let before = lower_runs();
    let control = lowering(false);
    assert_eq!(
        computed(run_blocking(&control, 1, &input, no_work())),
        first,
        "nor may the memo differ from what the uncached stage says",
    );
    assert_eq!(lower_runs() - before, 1, "the control ran, rather than hitting");
}

#[test]
fn the_memo_changes_speed_and_not_answers() {
    // `uncached` is the control case (DESIGN.md): if the answers move when the
    // cache is removed, the cache was part of the semantics.
    let input = Source("doc.title");
    let cached = lowering(true);

    // Each round moves the version, so the answers below are the GRAPH's rather
    // than the gate's.
    let mut cached_answers = Vec::new();
    for version in 1..=3 {
        cached_answers.push(computed(run_blocking(&cached, version, &input, no_work())));
    }
    let cached_runs = lower_runs();

    let uncached = lowering(false);
    for (round, version) in (1..=3).enumerate() {
        assert_eq!(
            computed(run_blocking(&uncached, version, &input, no_work())),
            cached_answers[round],
        );
    }

    assert_eq!(cached_runs, 1);
    assert_eq!(lower_runs() - cached_runs, 3, "the control ran every time");
}

#[test]
fn a_failure_is_never_cached() {
    // The standing rule: effects are never replayed by an implicit cache. A
    // transient failure served back from a memo would be exactly that. Both
    // polls move the version, so what is measured is the store rather than the
    // gate - and a failure records no version either, which is why the second
    // poll would reach the graph even at the same one.
    let stage = lowering(true);

    assert_eq!(
        failed(run_blocking(&stage, 1, &Source(""), no_work())),
        (0, "nothing to lower"),
    );
    assert_eq!(
        failed(run_blocking(&stage, 2, &Source(""), no_work())),
        (0, "nothing to lower"),
    );
    assert_eq!(
        lower_runs(),
        2,
        "a cached failure would have made the second poll free, and a transient \
         failure would then outlive its cause",
    );
}

#[test]
fn the_effectful_stage_runs_once_across_a_park_and_a_wake() {
    // The park/wake round trip must not double-run the effect: the Delayed poll
    // did not run it, and the memo means the woken poll runs it exactly once.
    let graph = pipeline();
    let input = Source("doc.title");

    assert_eq!(graph.poll(1, &input), Ok(Run::Delayed));
    upstream().land("built");
    assert!(matches!(graph.poll(1, &input), Ok(Run::Computed(_))));
    // A moved version, so the poll reaches the graph rather than the gate: the
    // memo is what keeps the effect from running a second time.
    assert!(matches!(graph.poll(2, &input), Ok(Run::Computed(_))));

    assert_eq!(emit_runs(), 1);
    assert_eq!(
        lower_runs(),
        1,
        "the pure first stage ran on the Delayed poll and was memoized for the rest",
    );
}
