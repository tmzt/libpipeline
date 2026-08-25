//! The builder as the only door (DESIGN.md).
//!
//! Every test here goes through the PUBLIC builder API and the stage-author
//! contract (`libpipelinedata`) - never through `Chain`, `Memo`, `FrameDriver`
//! or `run_to_completion` directly. That restriction is the point: if a
//! property below could not be expressed this way, the builder would be
//! missing something a consumer needs, and DESIGN.md's findings section would
//! get a new entry instead of this file getting an internal import.
//!
//! The last two tests re-hold `two_drivers_one_graph.rs`'s headline property
//! through the builder: a pending stage that registers no waker is a value
//! LOST rather than late - fatal to the frame drive and invisible to the
//! blocking one.

use std::any::Any;
use std::sync::{Arc, Mutex};
use std::task::{Context, Waker};

use libpipeline::{DriveError, Failure, PendingWork, PipelineBuilder};
use libpipelinedata::{ContentKey, EffectPoll, MemoKey, MemoMap, Stage, StageId};

/// A deterministic content key for test inputs. What matters here is only
/// that distinct inputs get distinct keys; real stages use the hash module.
fn key_of_bytes(bytes: &[u8]) -> ContentKey {
    let mut h: u128 = 0xcbf2_9ce4_8422_2325;
    for b in bytes {
        h ^= *b as u128;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    ContentKey::from_u128(h)
}

/// First stage: length of a string, counting its runs.
///
/// Its error type is the pipeline's one error type, shared with every stage it
/// is registered beside - `DESIGN.md`'s named cost of the flat error. `Len`
/// never fails; it still spells the type its neighbours raise.
struct Len {
    id: StageId,
    runs: Arc<Mutex<usize>>,
}

impl Stage for Len {
    type Input = String;
    type Output = usize;
    type Error = &'static str;

    fn id(&self) -> StageId {
        self.id
    }

    fn memo_key(&self, input: &Self::Input) -> Option<MemoKey> {
        Some(MemoKey::new(self.id, [key_of_bytes(input.as_bytes())]))
    }

    fn poll_stage(
        &self,
        input: &Self::Input,
        _cx: &mut Context<'_>,
    ) -> EffectPoll<Self::Output, Self::Error> {
        *self.runs.lock().unwrap() += 1;
        EffectPoll::Ready(input.len())
    }
}

/// Second stage: doubles, counting its runs.
struct Double {
    id: StageId,
    runs: Arc<Mutex<usize>>,
}

impl Stage for Double {
    type Input = usize;
    type Output = usize;
    type Error = &'static str;

    fn id(&self) -> StageId {
        self.id
    }

    fn memo_key(&self, input: &Self::Input) -> Option<MemoKey> {
        Some(MemoKey::new(self.id, [ContentKey::from_u128(*input as u128)]))
    }

    fn poll_stage(
        &self,
        input: &Self::Input,
        _cx: &mut Context<'_>,
    ) -> EffectPoll<Self::Output, Self::Error> {
        *self.runs.lock().unwrap() += 1;
        EffectPoll::Ready(input * 2)
    }
}

/// Third stage: renders the count, so a pipeline can hold two stages whose
/// outputs are different types.
struct Render {
    id: StageId,
    runs: Arc<Mutex<usize>>,
}

impl Stage for Render {
    type Input = usize;
    type Output = String;
    type Error = &'static str;

    fn id(&self) -> StageId {
        self.id
    }

    fn memo_key(&self, input: &Self::Input) -> Option<MemoKey> {
        Some(MemoKey::new(self.id, [ContentKey::from_u128(*input as u128)]))
    }

    fn poll_stage(
        &self,
        input: &Self::Input,
        _cx: &mut Context<'_>,
    ) -> EffectPoll<Self::Output, Self::Error> {
        *self.runs.lock().unwrap() += 1;
        EffectPoll::Ready(input.to_string())
    }
}

fn counter() -> Arc<Mutex<usize>> {
    Arc::new(Mutex::new(0))
}

fn count(c: &Arc<Mutex<usize>>) -> usize {
    *c.lock().unwrap()
}

#[test]
fn two_stages_compose_and_answer() {
    let pipeline = PipelineBuilder::new()
        .stage("len", |id| Len { id, runs: counter() })
        .stage("double", |id| Double { id, runs: counter() })
        .build();
    assert_eq!(pipeline.run_pure(&"abcd".to_string()), Ok(8));
}

#[test]
fn an_unchanged_input_hits_at_the_first_stage() {
    let (len_runs, double_runs) = (counter(), counter());
    let pipeline = PipelineBuilder::new()
        .stage("len", |id| Len { id, runs: Arc::clone(&len_runs) })
        .stage("double", |id| Double { id, runs: Arc::clone(&double_runs) })
        .build();

    assert_eq!(pipeline.run_pure(&"abcd".to_string()), Ok(8));
    assert_eq!(pipeline.run_pure(&"abcd".to_string()), Ok(8));
    // Registration memoized both stages without anyone asking: the repeat is
    // all cache hits and neither stage ran again.
    assert_eq!((count(&len_runs), count(&double_runs)), (1, 1));

    // A different input reaches both stages...
    assert_eq!(pipeline.run_pure(&"xy".to_string()), Ok(4));
    assert_eq!((count(&len_runs), count(&double_runs)), (2, 2));
    // ...but one that lowers to an already-seen intermediate stops there:
    // "dcba" has the length "abcd" had, so `double` hits.
    assert_eq!(pipeline.run_pure(&"dcba".to_string()), Ok(8));
    assert_eq!((count(&len_runs), count(&double_runs)), (3, 2));
}

#[test]
fn two_stages_may_share_a_name_with_no_consequence() {
    // The test `DESIGN.md` names for the label discipline: "a label two steps
    // can share without consequence is a label nothing depends on". Both
    // stages here are registered under one name, and nothing about the
    // pipeline moves - identity is the position, which the builder minted
    // separately for each.
    let (len_runs, double_runs) = (counter(), counter());
    let shared = PipelineBuilder::new()
        .stage("same", |id| Len { id, runs: Arc::clone(&len_runs) })
        .stage("same", |id| Double { id, runs: Arc::clone(&double_runs) })
        .build();

    assert_eq!(shared.run_pure(&"abcd".to_string()), Ok(8));
    assert_eq!(shared.run_pure(&"abcd".to_string()), Ok(8));
    // Two rows in one store under one name: each stage was looked up and hit
    // on its own identity, so neither served the other its answer and neither
    // ran twice.
    assert_eq!((count(&len_runs), count(&double_runs)), (1, 1));

    // And a different input still reaches both, which is what says the first
    // repeat was a hit rather than a stage that had stopped running.
    assert_eq!(shared.run_pure(&"xy".to_string()), Ok(4));
    assert_eq!((count(&len_runs), count(&double_runs)), (2, 2));
}

#[test]
fn one_store_serves_stages_of_different_output_types() {
    // Where the pipeline remembers is one decision, taken once, about the
    // whole pipeline - and the rows are erased, so one store serves stages
    // that do not agree about their output type. These two produce a `usize`
    // and a `String`, into the one store the builder was handed.
    let store: Arc<MemoMap<Arc<dyn Any + Send + Sync>>> = Arc::new(MemoMap::new());
    let (len_runs, render_runs) = (counter(), counter());
    let pipeline = PipelineBuilder::new()
        .store(Arc::clone(&store))
        .stage("len", |id| Len { id, runs: Arc::clone(&len_runs) })
        .stage("render", |id| Render { id, runs: Arc::clone(&render_runs) })
        .build();

    assert_eq!(pipeline.run_pure(&"abcd".to_string()), Ok("4".to_string()));
    assert_eq!(store.len(), 2, "one store, one row per stage");

    // Each stage got its OWN answer back: the repeat is two hits, and the row
    // each one hit is the row it recorded. A row holding the other stage's
    // type would be an identity collision, which one builder cannot mint -
    // which is why the lookup asserts the invariant rather than reporting a
    // miss and quietly recomputing.
    assert_eq!(pipeline.run_pure(&"abcd".to_string()), Ok("4".to_string()));
    assert_eq!((count(&len_runs), count(&render_runs)), (1, 1));
    assert_eq!(store.len(), 2);
}

#[test]
fn uncached_is_the_control_run_answers_hold_speed_does_not() {
    let input = "abcd".to_string();

    let cached_runs = counter();
    let cached = PipelineBuilder::new()
        .stage("len", |id| Len { id, runs: Arc::clone(&cached_runs) })
        .build();
    let uncached_runs = counter();
    let uncached = PipelineBuilder::new()
        .uncached()
        .stage("len", |id| Len { id, runs: Arc::clone(&uncached_runs) })
        .build();

    assert_eq!(cached.run_pure(&input), Ok(4));
    assert_eq!(cached.run_pure(&input), Ok(4));
    assert_eq!(uncached.run_pure(&input), Ok(4));
    assert_eq!(uncached.run_pure(&input), Ok(4));
    assert_eq!(count(&cached_runs), 1);
    assert_eq!(count(&uncached_runs), 2);
}

/// A second-stage failure, for the position the pipeline stamps on it.
struct Reject {
    id: StageId,
}

impl Stage for Reject {
    type Input = usize;
    type Output = usize;
    type Error = &'static str;

    fn id(&self) -> StageId {
        self.id
    }

    fn memo_key(&self, _input: &Self::Input) -> Option<MemoKey> {
        None
    }

    fn poll_stage(
        &self,
        _input: &Self::Input,
        _cx: &mut Context<'_>,
    ) -> EffectPoll<Self::Output, Self::Error> {
        EffectPoll::Failed("rejected")
    }
}

#[test]
fn a_failure_names_the_stage_that_raised_it() {
    let pipeline = PipelineBuilder::new()
        .stage("len", |id| Len { id, runs: counter() })
        .stage("reject", |id| Reject { id })
        .build();
    // Which stage failed is a position, answered in one call - not a count of
    // `First`/`Second` layers read off a nested type.
    let Err(DriveError::Failed(failure)) = pipeline.run_pure(&"abcd".to_string()) else {
        panic!("the second stage rejects, so the drive must fail");
    };
    assert_eq!(failure.at(), 1);
    assert_eq!(*failure.error(), "rejected");
}

// --- the two-drivers property, through the builder -------------------------

/// Where an out-of-band value lands, and where a wakeful stage leaves its
/// waker.
#[derive(Default)]
struct Slot {
    value: Mutex<Option<u64>>,
    waker: Mutex<Option<Waker>>,
}

impl Slot {
    fn land(&self, value: u64) {
        *self.value.lock().unwrap() = Some(value);
        if let Some(waker) = self.waker.lock().unwrap().take() {
            waker.wake();
        }
    }
}

/// An effectful fetch: `Pending` until the slot holds a value. `registers`
/// decides whether a `Pending` poll leaves a waker behind - the difference
/// between a value that arrives LATE and one that is LOST.
struct Fetch {
    id: StageId,
    slot: Arc<Slot>,
    registers: bool,
}

impl Stage for Fetch {
    type Input = ();
    type Output = u64;
    type Error = ();

    fn id(&self) -> StageId {
        self.id
    }

    /// An effect's result is not a cacheable fact; refuse to key.
    fn memo_key(&self, _input: &Self::Input) -> Option<MemoKey> {
        None
    }

    fn poll_stage(
        &self,
        _input: &Self::Input,
        cx: &mut Context<'_>,
    ) -> EffectPoll<Self::Output, Self::Error> {
        if let Some(value) = *self.slot.value.lock().unwrap() {
            return EffectPoll::Ready(value);
        }
        if self.registers {
            *self.slot.waker.lock().unwrap() = Some(cx.waker().clone());
        }
        EffectPoll::Pending
    }
}

/// A pump that lands the value on its first call.
struct LandOnPump {
    slot: Arc<Slot>,
    landed: Mutex<bool>,
}

impl PendingWork for LandOnPump {
    fn run_once(&self) -> bool {
        let mut landed = self.landed.lock().unwrap();
        if *landed {
            return false;
        }
        *landed = true;
        self.slot.land(7);
        true
    }
}

fn fetch_pipeline(
    slot: &Arc<Slot>,
    registers: bool,
) -> libpipeline::Pipeline<impl Stage<Input = (), Output = u64, Error = Failure<()>>> {
    let slot = Arc::clone(slot);
    PipelineBuilder::new()
        .stage("fetch", move |id| Fetch { id, slot, registers })
        .build()
}

#[test]
fn the_blocking_drive_cannot_see_a_forgotten_waker() {
    // Both variants complete offline: the loop re-polls unconditionally after
    // pumping, so nothing there depends on being woken. That is exactly why
    // the defect below is invisible to the blocking drive.
    for registers in [true, false] {
        let slot = Arc::<Slot>::default();
        let pipeline = fetch_pipeline(&slot, registers);
        let work = LandOnPump { slot: Arc::clone(&slot), landed: Mutex::new(false) };
        assert_eq!(pipeline.run(&(), &work), Ok(7));
    }

    // And with nothing to pump, an empty slot is a STALLED drive, not a hang.
    let slot = Arc::<Slot>::default();
    let pipeline = fetch_pipeline(&slot, true);
    assert_eq!(pipeline.run_pure(&()), Err(DriveError::Stalled));
}

#[test]
fn a_pending_stage_that_registers_no_waker_is_a_value_lost_rather_than_late() {
    // The wakeful stage: the value arrives LATE. Pending frame, value lands
    // out of band, the wake marks the pipeline stale, the next frame has it.
    let slot = Arc::<Slot>::default();
    let pipeline = fetch_pipeline(&slot, true);
    assert!(pipeline.poll_frame(&()).is_pending());
    slot.land(7);
    assert!(pipeline.take_stale(), "the landing must schedule a re-poll");
    assert_eq!(pipeline.poll_frame(&()), EffectPoll::Ready(7));

    // The forgetful stage: the value is LOST. It is sitting in the slot, a
    // poll would find it - but no wake ever marks the pipeline stale, so no
    // frame loop will ever issue that poll.
    let slot = Arc::<Slot>::default();
    let pipeline = fetch_pipeline(&slot, false);
    assert!(pipeline.poll_frame(&()).is_pending());
    slot.land(7);
    assert!(
        !pipeline.take_stale(),
        "no waker was registered, so nothing can tell the frame loop; \
         the landed value is unreachable, not merely late"
    );
}
