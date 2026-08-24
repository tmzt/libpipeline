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

use std::sync::{Arc, Mutex};
use std::task::{Context, Waker};

use libpipeline::{ChainError, DriveError, PendingWork, PipelineBuilder};
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
struct Len {
    id: StageId,
    runs: Arc<Mutex<usize>>,
}

impl Stage for Len {
    type Input = String;
    type Output = usize;
    type Error = ();

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
    type Error = ();

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

fn counter() -> Arc<Mutex<usize>> {
    Arc::new(Mutex::new(0))
}

fn count(c: &Arc<Mutex<usize>>) -> usize {
    *c.lock().unwrap()
}

#[test]
fn two_stages_compose_and_answer() {
    let pipeline = PipelineBuilder::new()
        .stage("len", 1, |id| Len { id, runs: counter() })
        .stage("double", 1, |id| Double { id, runs: counter() })
        .build();
    assert_eq!(pipeline.run_pure(&"abcd".to_string()), Ok(8));
}

#[test]
fn an_unchanged_input_hits_at_the_first_stage() {
    let (len_runs, double_runs) = (counter(), counter());
    let pipeline = PipelineBuilder::new()
        .stage("len", 1, |id| Len { id, runs: Arc::clone(&len_runs) })
        .stage("double", 1, |id| Double { id, runs: Arc::clone(&double_runs) })
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
fn a_version_bump_at_the_call_site_is_a_cold_cache() {
    let store: Arc<MemoMap<usize>> = Arc::new(MemoMap::new());
    let runs = counter();
    let run_once = |version: u32| {
        let pipeline = PipelineBuilder::new()
            .stage_in("len", version, Arc::clone(&store), |id| Len {
                id,
                runs: Arc::clone(&runs),
            })
            .build();
        pipeline.run_pure(&"abcd".to_string())
    };

    // The store outlives each build, so a rebuild at the SAME version is a
    // hit across pipelines...
    assert_eq!(run_once(1), Ok(4));
    assert_eq!(run_once(1), Ok(4));
    assert_eq!(count(&runs), 1);
    // ...and bumping the version at the one call site that declares it makes
    // every old entry unreachable: the id is half of the key.
    assert_eq!(run_once(2), Ok(4));
    assert_eq!(count(&runs), 2);
}

#[test]
#[should_panic(expected = "registered as")]
fn a_stage_that_answers_a_different_id_than_registered_panics() {
    let _ = PipelineBuilder::new().stage("len", 3, |_id| Len {
        // The defect the check exists for: keys built from an id that is not
        // the one the call site declares. It must die at registration.
        id: StageId::new("len", 1),
        runs: counter(),
    });
}

#[test]
fn uncached_is_the_control_run_answers_hold_speed_does_not() {
    let input = "abcd".to_string();

    let cached_runs = counter();
    let cached = PipelineBuilder::new()
        .stage("len", 1, |id| Len { id, runs: Arc::clone(&cached_runs) })
        .build();
    let uncached_runs = counter();
    let uncached = PipelineBuilder::new()
        .uncached()
        .stage("len", 1, |id| Len { id, runs: Arc::clone(&uncached_runs) })
        .build();

    assert_eq!(cached.run_pure(&input), Ok(4));
    assert_eq!(cached.run_pure(&input), Ok(4));
    assert_eq!(uncached.run_pure(&input), Ok(4));
    assert_eq!(uncached.run_pure(&input), Ok(4));
    assert_eq!(count(&cached_runs), 1);
    assert_eq!(count(&uncached_runs), 2);
}

/// A second-stage failure arrives tagged with which half raised it.
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
        .stage("len", 1, |id| Len { id, runs: counter() })
        .stage("reject", 1, |id| Reject { id })
        .build();
    assert_eq!(
        pipeline.run_pure(&"abcd".to_string()),
        Err(DriveError::Failed(ChainError::Second("rejected")))
    );
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
) -> libpipeline::Pipeline<impl Stage<Input = (), Output = u64, Error = ()>> {
    let slot = Arc::clone(slot);
    PipelineBuilder::new()
        .stage("fetch", 1, move |id| Fetch { id, slot, registers })
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
