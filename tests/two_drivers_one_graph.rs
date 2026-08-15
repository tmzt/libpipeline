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

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::task::{Context, Waker};

use libpipeline::{Chain, ChainError, DriveError, FrameDriver, Memo, PendingWork, run_to_completion};
use libpipelinedata::{ContentKey, EffectPoll, MemoKey, MemoStore, NoMemo, Stage, StageId};

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

// -------------------------------------------------------------------- store

/// A memo store for the tests, and deliberately still its own implementation.
///
/// Step 3 has since landed the real ones - `libpipelinedata`'s `MemoMap` and
/// the hecs-backed `EcsMemoStore`, keyed by content hash - so this could now
/// be swapped for `MemoMap`. It is not, on purpose: an INDEPENDENT
/// implementation on the other side of the seam is the only thing here that
/// shows `MemoStore` is implementable by someone who did not write it, which
/// is what a seam is for. `MemoMap` cannot prove that about itself.
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
    runs: Mutex<usize>,
}

impl Lower {
    const ID: StageId = StageId::new("test.lower", 1);

    fn new() -> Self {
        Self {
            runs: Mutex::new(0),
        }
    }

    fn runs(&self) -> usize {
        *self.runs.lock().unwrap()
    }
}

impl Stage for Lower {
    type Input = Source;
    type Output = Lowered;
    type Error = &'static str;

    fn id(&self) -> StageId {
        Self::ID
    }

    fn memo_key(&self, input: &Source) -> Option<MemoKey> {
        // A stand-in for step 2's content hash: a real one hashes the value's
        // fields through a streaming hasher. What matters to this file is only
        // that equal inputs give equal keys and the key costs no run.
        Some(MemoKey::new(Self::ID, [content_key_of(input.0)]))
    }

    fn poll_stage(
        &self,
        input: &Source,
        _cx: &mut Context<'_>,
    ) -> EffectPoll<Lowered, &'static str> {
        *self.runs.lock().unwrap() += 1;
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
    upstream: Arc<Upstream>,
    runs: Mutex<usize>,
}

impl Emit {
    const ID: StageId = StageId::new("test.emit", 1);

    fn new(upstream: Arc<Upstream>) -> Self {
        Self {
            upstream,
            runs: Mutex::new(0),
        }
    }

    fn runs(&self) -> usize {
        *self.runs.lock().unwrap()
    }
}

impl Stage for Emit {
    type Input = Lowered;
    type Output = Emitted;
    type Error = &'static str;

    fn id(&self) -> StageId {
        Self::ID
    }

    fn memo_key(&self, input: &Lowered) -> Option<MemoKey> {
        Some(MemoKey::new(Self::ID, [content_key_of(&input.0.join("."))]))
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
        *self.runs.lock().unwrap() += 1;
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

type TwoStage = Chain<Memo<Lower, MapStore<Lowered>>, Memo<Emit, MapStore<Emitted>>>;

const GRAPH_ID: StageId = StageId::new("test.lower_then_emit", 1);

fn graph(upstream: Arc<Upstream>) -> TwoStage {
    Chain::new(
        GRAPH_ID,
        Memo::new(Lower::new(), MapStore::new()),
        Memo::new(Emit::new(upstream), MapStore::new()),
    )
}

/// The blocking driver's executor: it lands the upstream the first time it is
/// pumped, then has nothing left to do.
struct LandsOnFirstPump {
    upstream: Arc<Upstream>,
    landed: Mutex<bool>,
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
    driver: &FrameDriver,
    stage: &S,
    input: &S::Input,
    max_frames: usize,
) -> Option<S::Output> {
    for _ in 0..max_frames {
        if !driver.take_stale() {
            return None;
        }
        if let Some(value) = driver.poll_frame(stage, input).ready() {
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
    let work = LandsOnFirstPump {
        upstream,
        landed: Mutex::new(false),
    };

    let out = run_to_completion(&graph, &Source("props.title"), &work);
    assert_eq!(out, Ok(Emitted("built::props/title".to_string())));
}

#[test]
fn the_real_time_driver_parks_wakes_and_re_polls_to_ready() {
    let upstream = Arc::new(Upstream::default());
    let graph = graph(Arc::clone(&upstream));
    let driver = FrameDriver::new();
    let input = Source("props.title");

    // Frame 1: the upstream has not landed. The frame does not block; it would
    // draw a stand-in and return.
    assert_eq!(driver.poll_frame(&graph, &input), EffectPoll::Pending);
    assert!(
        !driver.take_stale(),
        "a Pending poll is not itself a wake - something has to land",
    );

    // Between frames, the upstream lands and wakes what parked on it.
    upstream.land("built");
    assert!(driver.take_stale(), "the wake reached the frame loop");

    // Frame 2: the same graph, re-polled, now answers.
    assert_eq!(
        driver.poll_frame(&graph, &input),
        EffectPoll::Ready(Emitted("built::props/title".to_string())),
    );
}

#[test]
fn both_drivers_give_the_same_answer_for_the_same_graph() {
    // PIPELINE_PLAN.md §5's claim, stated as an equality. The two graphs are
    // built by the same function from the same input; only the driver differs.
    let input = Source("app.screen.title");

    let offline_upstream = Arc::new(Upstream::default());
    let offline = run_to_completion(
        &graph(Arc::clone(&offline_upstream)),
        &input,
        &LandsOnFirstPump {
            upstream: offline_upstream,
            landed: Mutex::new(false),
        },
    )
    .expect("the offline driver reaches a value");

    let live_upstream = Arc::new(Upstream::default());
    let live_graph = graph(Arc::clone(&live_upstream));
    let driver = FrameDriver::new();

    // Frame 1 parks. Between frames the upstream lands and wakes; every later
    // frame happens only because of that wake - see resume_when_woken.
    assert!(driver.poll_frame(&live_graph, &input).is_pending());
    live_upstream.land("built");
    let live = resume_when_woken(&driver, &live_graph, &input, 4)
        .expect("the woken frame reaches a value");

    assert_eq!(offline, live);
}

/// The same stage as [`Emit`], minus the one line that registers the waker.
///
/// It exists to measure, rather than reason about, what `resume_when_woken`
/// depends on: with this in the graph the value LANDS and the frame loop still
/// never sees it. That turns "the wake is load-bearing" from a claim about the test
/// into an observation of it.
struct ForgetfulEmit {
    upstream: Arc<Upstream>,
}

impl Stage for ForgetfulEmit {
    type Input = Lowered;
    type Output = Emitted;
    type Error = &'static str;

    fn id(&self) -> StageId {
        StageId::new("test.emit_without_registering", 1)
    }

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
    let graph = Chain::new(
        StageId::new("test.forgetful", 1),
        Memo::new(Lower::new(), MapStore::new()),
        ForgetfulEmit {
            upstream: Arc::clone(&upstream),
        },
    );
    let driver = FrameDriver::new();
    let input = Source("props.title");

    assert!(driver.poll_frame(&graph, &input).is_pending());
    upstream.land("built");
    assert_eq!(
        resume_when_woken(&driver, &graph, &input, 100),
        None,
        "the value landed and the frame loop never learned of it",
    );

    // The offline driver is unaffected, because it re-polls without being
    // asked - which is exactly why a missing registration is invisible to a CLI
    // run and fatal in the IDE. Worth knowing before a real stage forgets.
    let out = run_to_completion(
        &graph,
        &input,
        &LandsOnFirstPump {
            upstream,
            landed: Mutex::new(false),
        },
    );
    assert_eq!(out, Ok(Emitted("built::props/title".to_string())));
}

#[test]
fn the_frame_loop_cannot_advance_without_a_wake() {
    // The control for resume_when_woken: nothing lands, so nothing wakes, so no
    // number of frames produces a value.
    let graph = graph(Arc::new(Upstream::default()));
    let driver = FrameDriver::new();
    let input = Source("props.title");
    assert!(driver.poll_frame(&graph, &input).is_pending());
    assert_eq!(resume_when_woken(&driver, &graph, &input, 100), None);
}

#[test]
fn a_stalled_graph_ends_rather_than_spinning() {
    // Nothing lands, so the offline driver has no work left after the first
    // Pending. Offline that is a graph bug; in a frame loop the same state is
    // the ordinary "keep the stand-in" case, which is exactly why the two
    // drivers differ here and nowhere else.
    let graph = graph(Arc::new(Upstream::default()));
    let out = run_to_completion(&graph, &Source("props.title"), &libpipeline::NoPendingWork);
    assert_eq!(out, Err(DriveError::Stalled));
}

#[test]
fn a_failure_bubbles_out_tagged_with_the_half_it_came_from() {
    let upstream = Arc::new(Upstream::default());
    upstream.land("built");
    let graph = graph(upstream);
    let out = run_to_completion(&graph, &Source(""), &libpipeline::NoPendingWork);
    assert_eq!(
        out,
        Err(DriveError::Failed(ChainError::First("nothing to lower"))),
    );
}

#[test]
fn the_memo_serves_the_second_poll_without_re_running_the_stage() {
    let upstream = Arc::new(Upstream::default());
    upstream.land("built");
    let stage = Memo::new(Lower::new(), MapStore::new());
    let input = Source("props.title");

    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    let first = stage.poll_stage(&input, &mut cx);
    let second = stage.poll_stage(&input, &mut cx);

    assert_eq!(first, second);
    assert_eq!(
        stage.stage().runs(),
        1,
        "the second poll was served by the lookup that precedes the work",
    );
}

#[test]
fn the_memo_changes_speed_and_not_answers() {
    // NoMemo is the control case (see its doc): if the answers move when the
    // cache is removed, the cache was part of the semantics.
    let input = Source("props.title");
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);

    let cached = Memo::new(Lower::new(), MapStore::new());
    let uncached = Memo::new(Lower::new(), NoMemo);
    for _ in 0..3 {
        assert_eq!(
            cached.poll_stage(&input, &mut cx),
            uncached.poll_stage(&input, &mut cx),
        );
    }
    assert_eq!(cached.stage().runs(), 1);
    assert_eq!(uncached.stage().runs(), 3, "the control ran every time");
}

#[test]
fn a_failure_is_never_cached() {
    // §3's rule: effects are never replayed by an implicit cache. A transient
    // failure served back from a memo would be exactly that.
    let stage = Memo::new(Lower::new(), MapStore::new());
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);

    assert_eq!(
        stage.poll_stage(&Source(""), &mut cx),
        EffectPoll::Failed("nothing to lower"),
    );
    assert_eq!(
        stage.poll_stage(&Source(""), &mut cx),
        EffectPoll::Failed("nothing to lower"),
    );
    assert_eq!(
        stage.stage().runs(),
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
    let graph = graph(Arc::clone(&upstream));
    let driver = FrameDriver::new();
    let input = Source("props.title");

    assert!(driver.poll_frame(&graph, &input).is_pending());
    upstream.land("built");
    assert!(driver.poll_frame(&graph, &input).ready().is_some());
    assert!(driver.poll_frame(&graph, &input).ready().is_some());

    assert_eq!(graph.second().stage().runs(), 1);
    assert_eq!(
        graph.first().stage().runs(),
        1,
        "the pure first stage ran on the Pending frame and was memoized for the rest",
    );
}
