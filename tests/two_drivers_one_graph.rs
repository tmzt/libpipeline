//! Gate 1 of `PIPELINE_PLAN.md` §9 step 1: **the same two-stage graph runs
//! under both drivers.**
//!
//! §5's claim is "same stages, same keys, different driver". The graph below is
//! built once, from stages that know nothing about who polls them, and is
//! driven twice: to completion by the offline driver, and one frame at a time
//! by the real-time one, where a `Pending` stage parks, is woken, and re-polls
//! to `Ready`. If those two disagree about the answer, the claim is false.
//!
//! **Every type the graph carries is a stand-in** (`PIPELINE_PLAN.md`:584-589).
//! `Source`, `Lowered` and `Emitted` are invented for this file. That is the
//! standing requirement, not a convenience: if the engine's tests could not be
//! written without a real IR, the engine would have learned something it must
//! not know.
//!
//! # Everything here goes through the builder, and that is a second gate
//!
//! `DESIGN.md`: the builder is the only public way to compose, memoize or
//! drive. This file names `PipelineBuilder`, `Pipeline`, `DriveError`,
//! `ChainError` and `PendingWork` and nothing else from the crate - so it is
//! also the measurement that the two-driver property is EXPRESSIBLE through the
//! public door. A test in `tests/` proves the public API reaches something; a
//! test in `src/` admits it does not yet.
//!
//! Three things changed shape in the conversion and each is a consequence of
//! the design rather than of this file:
//!
//! * **The stages' ids are minted at registration**, not declared as
//!   associated consts, so the version sits beside the closure that builds the
//!   behaviour it versions. `Lower::new` and `Emit::new` take the id the
//!   builder hands them and answer it from `Stage::id`; a mismatch panics
//!   there.
//! * **The run counts are handed IN.** The builder owns what it registers, so a
//!   test cannot reach back through the opaque graph for a counter a stage
//!   holds. [`Runs`] is that counter, shared.
//! * **Memoization is not composed here at all.** There is no `Memo::new` to
//!   write: registering a stage memoizes it. `.uncached()` is the control.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::task::Waker;

use libpipeline::{ChainError, DriveError, PendingWork, Pipeline, PipelineBuilder};
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
/// Step 3 has since landed the real ones - `libpipelinedata`'s `MemoMap` and
/// the hecs-backed `EcsMemoStore`, keyed by content hash - so this could now
/// be swapped for `MemoMap`. It is not, on purpose: an INDEPENDENT
/// implementation on the other side of the seam is the only thing here that
/// shows `MemoStore` is implementable by someone who did not write it, which
/// is what a seam is for. `MemoMap` cannot prove that about itself.
///
/// It reaches the graph through `PipelineBuilder::stage_in`, which is the
/// builder's door for a caller-provided store - so the seam is still exercised
/// after the flip, through the public API rather than by hand-composing a memo.
struct MapStore<V> {
    rows: Mutex<HashMap<MemoKey, V>>,
}

impl<V> MapStore<V> {
    fn new() -> Self {
        Self {
            rows: Mutex::new(HashMap::new()),
        }
    }
}

impl<V: Clone> MemoStore<V> for MapStore<V> {
    fn lookup(&self, key: &MemoKey) -> Option<V> {
        self.rows.lock().unwrap().get(key).cloned()
    }

    fn record(&self, key: &MemoKey, value: V) {
        self.rows.lock().unwrap().insert(key.clone(), value);
    }
}

// ------------------------------------------------------------------- stage 1

/// A pure stage: `Ready` on the first poll, keyed, never `Pending` - the shape
/// every row of §4's table has except rows 10 and 11.
struct Lower {
    id: StageId,
    runs: Runs,
}

impl Lower {
    /// Written to be the builder's `make`: it takes the id minted from the name
    /// and version at the registration call site, and answers it from
    /// [`Stage::id`].
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
        // A stand-in for step 2's content hash: a real one hashes the value's
        // fields through a streaming hasher. What matters to this file is only
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
/// the wakers of everyone who asked for it early. §4's rows 10 and 11 in
/// miniature.
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

/// Stand-in for step 2's content hash - FNV over the bytes. Not a content
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

/// The failure type a two-stage registration produces, tagged with the half it
/// came from - the vocabulary a caller matching on which stage failed needs.
type TwoStageError = ChainError<&'static str, &'static str>;

/// The two-stage graph, registered: `lower` then `emit`, each memoized in a
/// [`MapStore`] the caller provides.
///
/// **The versions are here**, at the registration call sites, which is the whole
/// of why the builder takes them there.
fn pipeline(
    upstream: Arc<Upstream>,
    lower_runs: Runs,
    emit_runs: Runs,
) -> Pipeline<impl Stage<Input = Source, Output = Emitted, Error = TwoStageError>> {
    PipelineBuilder::new()
        .stage_in("test.lower", 1, MapStore::new(), move |id| {
            Lower::new(id, lower_runs)
        })
        .stage_in("test.emit", 1, MapStore::new(), move |id| {
            Emit::new(id, upstream, emit_runs)
        })
        .build()
}

/// [`pipeline`] with the counts thrown away, for the tests that measure answers
/// rather than runs.
fn graph(
    upstream: Arc<Upstream>,
) -> Pipeline<impl Stage<Input = Source, Output = Emitted, Error = TwoStageError>> {
    pipeline(upstream, Runs::default(), Runs::default())
}

/// Just the first stage, registered on its own - for the memo properties, which
/// are about ONE stage's store and would be reported by a chain only
/// indirectly.
fn lowering(
    runs: Runs,
    cached: bool,
) -> Pipeline<impl Stage<Input = Source, Output = Lowered, Error = &'static str>> {
    let builder = if cached {
        PipelineBuilder::new()
    } else {
        PipelineBuilder::new().uncached()
    };
    builder
        .stage_in("test.lower", 1, MapStore::new(), move |id| {
            Lower::new(id, runs)
        })
        .build()
}

/// The blocking driver's executor: it lands the upstream the first time it is
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

impl PendingWork for LandsOnFirstPump {
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

/// Frames AFTER a park, and the reason it is written this way: **it polls only
/// because a wake said stale.**
///
/// Every poll here is guarded by `take_stale`, so if the waker were torn out
/// this returns `None` no matter how many frames it is given. Asserting that a
/// wake arrived and then re-polling anyway would have proved nothing: the test
/// would be re-polling on a hunch and reading the wake as decoration. This
/// helper's first draft did exactly that - it opened with an unguarded poll -
/// and `a_pending_stage_that_registers_no_waker_is_a_value_lost_rather_than_late`
/// caught it, which is the whole reason that control test is here.
///
/// `None` means "still pending, nobody woke us". That is a legitimate outcome
/// for a real frame - it keeps its stand-in - and a failure for a test that
/// expected a value.
fn resume_when_woken<S: Stage>(
    pipeline: &Pipeline<S>,
    input: &S::Input,
    max_frames: usize,
) -> Option<S::Output> {
    for _ in 0..max_frames {
        if !pipeline.take_stale() {
            return None;
        }
        if let Some(value) = pipeline.poll_frame(input).ready() {
            return Some(value);
        }
    }
    None
}

// --------------------------------------------------------------------- gate

#[test]
fn the_offline_driver_runs_the_graph_to_completion() {
    let upstream = Arc::new(Upstream::default());
    let graph = graph(Arc::clone(&upstream));
    let work = LandsOnFirstPump::new(upstream);

    let out = graph.run(&Source("props.title"), &work);
    assert_eq!(out, Ok(Emitted("built::props/title".to_string())));
}

#[test]
fn the_real_time_driver_parks_wakes_and_re_polls_to_ready() {
    let upstream = Arc::new(Upstream::default());
    let graph = graph(Arc::clone(&upstream));
    let input = Source("props.title");

    // Frame 1: the upstream has not landed. The frame does not block; it would
    // draw a stand-in and return.
    assert_eq!(graph.poll_frame(&input), EffectPoll::Pending);
    assert!(
        !graph.take_stale(),
        "a Pending poll is not itself a wake - something has to land",
    );

    // Between frames, the upstream lands and wakes what parked on it.
    upstream.land("built");
    assert!(graph.take_stale(), "the wake reached the frame loop");

    // Frame 2: the same graph, re-polled, now answers.
    assert_eq!(
        graph.poll_frame(&input),
        EffectPoll::Ready(Emitted("built::props/title".to_string())),
    );
}

#[test]
fn both_drivers_give_the_same_answer_for_the_same_graph() {
    // PIPELINE_PLAN.md §5's claim, stated as an equality. The two graphs are
    // built by the same function from the same input; only the driver differs.
    let input = Source("app.screen.title");

    let offline_upstream = Arc::new(Upstream::default());
    let offline = graph(Arc::clone(&offline_upstream))
        .run(&input, &LandsOnFirstPump::new(offline_upstream))
        .expect("the offline driver reaches a value");

    let live_upstream = Arc::new(Upstream::default());
    let live_graph = graph(Arc::clone(&live_upstream));

    // Frame 1 parks. Between frames the upstream lands and wakes; every later
    // frame happens only because of that wake - see resume_when_woken.
    assert!(live_graph.poll_frame(&input).is_pending());
    live_upstream.land("built");
    let live = resume_when_woken(&live_graph, &input, 4).expect("the woken frame reaches a value");

    assert_eq!(offline, live);
}

/// The same stage as [`Emit`], minus the one line that registers the waker.
///
/// It exists to measure, rather than reason about, what `resume_when_woken`
/// depends on: with this in the graph the value LANDS and the frame loop still
/// never sees it. That turns "the wake is load-bearing" from a claim about the test
/// into an observation of it.
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
    // Stage::poll_stage's doc makes registering an obligation. This is what
    // breaking it costs, and it is the control that gives the wake in
    // `both_drivers_give_the_same_answer_for_the_same_graph` its meaning: the
    // upstream lands, the value is there for the asking, and the frame loop
    // never asks because nothing told it to.
    let upstream = Arc::new(Upstream::default());
    let forgetful = Arc::clone(&upstream);
    let graph = PipelineBuilder::new()
        .stage_in("test.lower", 1, MapStore::new(), |id| {
            Lower::new(id, Runs::default())
        })
        .stage("test.emit_without_registering", 1, move |id| ForgetfulEmit {
            id,
            upstream: forgetful,
        })
        .build();
    let input = Source("props.title");

    assert!(graph.poll_frame(&input).is_pending());
    upstream.land("built");
    assert_eq!(
        resume_when_woken(&graph, &input, 100),
        None,
        "the value landed and the frame loop never learned of it",
    );

    // The offline driver is unaffected, because it re-polls without being
    // asked - which is exactly why a missing registration is invisible to a CLI
    // run and fatal in the IDE. Worth knowing before a real stage forgets.
    let out = graph.run(&input, &LandsOnFirstPump::new(upstream));
    assert_eq!(out, Ok(Emitted("built::props/title".to_string())));
}

#[test]
fn the_frame_loop_cannot_advance_without_a_wake() {
    // The control for resume_when_woken: nothing lands, so nothing wakes, so no
    // number of frames produces a value.
    let graph = graph(Arc::new(Upstream::default()));
    let input = Source("props.title");
    assert!(graph.poll_frame(&input).is_pending());
    assert_eq!(resume_when_woken(&graph, &input, 100), None);
}

#[test]
fn a_stalled_graph_ends_rather_than_spinning() {
    // Nothing lands, so the offline driver has no work left after the first
    // Pending. Offline that is a graph bug; in a frame loop the same state is
    // the ordinary "keep the stand-in" case, which is exactly why the two
    // drivers differ here and nowhere else.
    let graph = graph(Arc::new(Upstream::default()));
    let out = graph.run_pure(&Source("props.title"));
    assert_eq!(out, Err(DriveError::Stalled));
}

#[test]
fn a_failure_bubbles_out_tagged_with_the_half_it_came_from() {
    let upstream = Arc::new(Upstream::default());
    upstream.land("built");
    let graph = graph(upstream);
    let out = graph.run_pure(&Source(""));
    assert_eq!(
        out,
        Err(DriveError::Failed(ChainError::First("nothing to lower"))),
    );
}

#[test]
fn the_memo_serves_the_second_poll_without_re_running_the_stage() {
    // Registration memoizes - there is no `Memo` to compose here - so a second
    // drive over the same input is served by the lookup that precedes the work.
    let runs = Runs::default();
    let stage = lowering(runs.clone(), true);
    let input = Source("props.title");

    let first = stage.run_pure(&input);
    let second = stage.run_pure(&input);

    assert_eq!(first, second);
    assert_eq!(
        runs.get(),
        1,
        "the second poll was served by the lookup that precedes the work",
    );
}

#[test]
fn one_memo_serves_both_drivers_and_the_stage_runs_once() {
    // The two drives of section 5 over ONE registered stage, which
    // `both_drivers_give_the_same_answer_for_the_same_graph` does not cover: it
    // builds a graph per driver, so each has its own store and neither hits on
    // the other's work. Here the offline drive fills the store and the frame
    // drive is served by it - "a CLI run against an unchanged tree is all cache
    // hits, because the memo keys are the same ones the IDE used" (section 5),
    // measured rather than asserted about.
    //
    // **This is the public-API expression of what `src/track.rs`'s
    // `the_ledger_scope_changes_speed_and_not_answers` measured** (DESIGN.md,
    // finding 1's note). That test wrapped the same shape in a `Ledger` scope
    // over a stage that read no tracked state, and mutation testing showed the
    // scope was inert in it: the known-bad `Memo::new(Tracked::new(..), ..)`
    // order passed it, and so did deleting the tracking outright. What it
    // actually held is below, and the builder can say all of it.
    let input = Source("props.title");
    let runs = Runs::default();
    let stage = lowering(runs.clone(), true);

    let first = stage.run_pure(&input).expect("a pure stage answers");
    let second = stage
        .poll_frame(&input)
        .ready()
        .expect("a pure stage answers on the first poll");
    assert!(
        !stage.take_stale(),
        "the stage answered without waiting, so nothing should have asked for a \
         redraw",
    );

    assert_eq!(
        runs.get(),
        1,
        "the frame drive was served by what the offline drive recorded - one \
         store, one key, either driver",
    );
    assert_eq!(first, second, "and the driver must not change what a stage answers");

    // The control run: same answer with every store off. A pipeline whose
    // ANSWERS move when the cache is removed has a bug the cache was hiding.
    let control_runs = Runs::default();
    let control = lowering(control_runs.clone(), false);
    assert_eq!(
        control.run_pure(&input).expect("a pure stage answers"),
        first,
        "nor may the memo differ from what the uncached stage says",
    );
    assert_eq!(control_runs.get(), 1, "the control ran, rather than hitting");
}

#[test]
fn the_memo_changes_speed_and_not_answers() {
    // `uncached` is the control case (DESIGN.md): if the answers move when the
    // cache is removed, the cache was part of the semantics.
    let input = Source("props.title");
    let cached_runs = Runs::default();
    let uncached_runs = Runs::default();
    let cached = lowering(cached_runs.clone(), true);
    let uncached = lowering(uncached_runs.clone(), false);

    for _ in 0..3 {
        assert_eq!(cached.run_pure(&input), uncached.run_pure(&input));
    }
    assert_eq!(cached_runs.get(), 1);
    assert_eq!(uncached_runs.get(), 3, "the control ran every time");
}

#[test]
fn a_failure_is_never_cached() {
    // §3's rule: effects are never replayed by an implicit cache. A transient
    // failure served back from a memo would be exactly that.
    let runs = Runs::default();
    let stage = lowering(runs.clone(), true);

    assert_eq!(
        stage.run_pure(&Source("")),
        Err(DriveError::Failed("nothing to lower")),
    );
    assert_eq!(
        stage.run_pure(&Source("")),
        Err(DriveError::Failed("nothing to lower")),
    );
    assert_eq!(
        runs.get(),
        2,
        "a cached Failed would have made the second poll free, and a transient \
         failure would then outlive its cause",
    );
}

#[test]
fn the_effectful_stage_runs_once_across_a_park_and_a_wake() {
    // The park/wake round trip must not double-run the effect: the Pending poll
    // did not run it, and the memo means the woken poll runs it exactly once.
    let upstream = Arc::new(Upstream::default());
    let lower_runs = Runs::default();
    let emit_runs = Runs::default();
    let graph = pipeline(
        Arc::clone(&upstream),
        lower_runs.clone(),
        emit_runs.clone(),
    );
    let input = Source("props.title");

    assert!(graph.poll_frame(&input).is_pending());
    upstream.land("built");
    assert!(graph.poll_frame(&input).ready().is_some());
    assert!(graph.poll_frame(&input).ready().is_some());

    assert_eq!(emit_runs.get(), 1);
    assert_eq!(
        lower_runs.get(),
        1,
        "the pure first stage ran on the Pending frame and was memoized for the rest",
    );
}
