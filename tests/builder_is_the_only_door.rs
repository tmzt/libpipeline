//! The builder as the only door (DESIGN.md).
//!
//! Every test here goes through the PUBLIC builder API and the stage-author
//! contract (`libpipelinedata`) - never through `Chain`, `Memo`, `FrameDriver`
//! or `run_to_completion` directly. That restriction is the point: if a
//! property below could not be expressed this way, the builder would be
//! missing something a consumer needs, and `PLAN.md`'s findings section would
//! get a new entry instead of this file getting an internal import.
//!
//! Since the flip there is exactly one way to run a pipeline - `run(version,
//! &input)` - so this file also measures what the one door answers: `Computed`
//! with a share of the value, `Unchanged` when the state it was handed is the
//! state it last computed for, `Delayed` when a wake is owed, and a positioned
//! `Failure` on the error side.

use std::any::Any;
use std::sync::{Arc, Mutex};
use std::task::{Context, Waker};

use libpipeline::{Failure, PipelineBuilder, Run, RunResult};
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

/// The value a run computed, or a panic naming what it answered instead.
///
/// It is a SHARE: the memo still holds the value it just answered with, which
/// is what a memo is, and `Run::Computed` says so in its type rather than
/// handing out a copy that pretends otherwise.
fn computed<T: std::fmt::Debug, E: std::fmt::Debug>(outcome: RunResult<T, E>) -> Arc<T> {
    match outcome {
        Ok(Run::Computed(value)) => value,
        other => panic!("expected a computed run, got {other:?}"),
    }
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
///
/// Its `Input` is the previous stage's `Output` - the value, not the share the
/// graph carries it in. The engine wraps on the way out of a stage and unwraps
/// on the way into the next; a stage author writes neither.
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
    assert_eq!(*computed(pipeline.run(1, &"abcd".to_string())), 8);
}

#[test]
fn an_unchanged_input_hits_at_the_first_stage() {
    let (len_runs, double_runs) = (counter(), counter());
    let pipeline = PipelineBuilder::new()
        .stage("len", |id| Len { id, runs: Arc::clone(&len_runs) })
        .stage("double", |id| Double { id, runs: Arc::clone(&double_runs) })
        .build();

    assert_eq!(*computed(pipeline.run(1, &"abcd".to_string())), 8);
    // A NEW version over the same content: the version gate cannot answer, so
    // the graph is entered - and every stage is a cache hit. That is the memo's
    // headline, and stating it at a moved version is what keeps this a test of
    // the memo rather than of the gate above it.
    assert_eq!(*computed(pipeline.run(2, &"abcd".to_string())), 8);
    assert_eq!((count(&len_runs), count(&double_runs)), (1, 1));

    // A different input reaches both stages...
    assert_eq!(*computed(pipeline.run(3, &"xy".to_string())), 4);
    assert_eq!((count(&len_runs), count(&double_runs)), (2, 2));
    // ...but one that lowers to an already-seen intermediate stops there:
    // "dcba" has the length "abcd" had, so `double` hits.
    assert_eq!(*computed(pipeline.run(4, &"dcba".to_string())), 8);
    assert_eq!((count(&len_runs), count(&double_runs)), (3, 2));
}

#[test]
fn the_same_version_answers_unchanged_without_touching_the_graph() {
    // The gate, on its own: the version the pipeline last computed for, handed
    // back. The readable is never dereferenced and no stage is polled - which
    // is measurable here, because the input handed to the second run is a
    // DIFFERENT string. A run that reached the graph would answer 2, not 4.
    let runs = counter();
    let pipeline = PipelineBuilder::new()
        .stage("len", |id| Len { id, runs: Arc::clone(&runs) })
        .build();

    assert_eq!(*computed(pipeline.run(7, &"abcd".to_string())), 4);
    assert_eq!(pipeline.run(7, &"xy".to_string()), Ok(Run::Unchanged));
    assert_eq!(count(&runs), 1, "nothing was polled");

    // And the gate is not a latch: a version it has not computed for reaches
    // the graph, whatever it answered a moment ago.
    assert_eq!(*computed(pipeline.run(8, &"xy".to_string())), 2);
    assert_eq!(count(&runs), 2);
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

    assert_eq!(*computed(shared.run(1, &"abcd".to_string())), 8);
    assert_eq!(*computed(shared.run(2, &"abcd".to_string())), 8);
    // Two rows in one store under one name: each stage was looked up and hit
    // on its own identity, so neither served the other its answer and neither
    // ran twice.
    assert_eq!((count(&len_runs), count(&double_runs)), (1, 1));

    // And a different input still reaches both, which is what says the first
    // repeat was a hit rather than a stage that had stopped running.
    assert_eq!(*computed(shared.run(3, &"xy".to_string())), 4);
    assert_eq!((count(&len_runs), count(&double_runs)), (2, 2));
}

#[test]
fn one_store_serves_stages_of_different_output_types() {
    // Where the pipeline remembers is one decision, taken once, about the
    // whole pipeline - and the rows are erased, so one store serves stages
    // that do not agree about their output type. These two produce a `usize`
    // and a `String`, into the one store the builder was handed.
    //
    // The store is instantiated at the UNSIZED erased type, which is what makes
    // recording a coercion of the share the memo layer already holds rather
    // than a second wrapping of it.
    let store: Arc<MemoMap<dyn Any + Send + Sync>> = Arc::new(MemoMap::new());
    let (len_runs, render_runs) = (counter(), counter());
    let pipeline = PipelineBuilder::new()
        .store(Arc::clone(&store))
        .stage("len", |id| Len { id, runs: Arc::clone(&len_runs) })
        .stage("render", |id| Render { id, runs: Arc::clone(&render_runs) })
        .build();

    assert_eq!(*computed(pipeline.run(1, &"abcd".to_string())), "4");
    assert_eq!(store.len(), 2, "one store, one row per stage");

    // Each stage got its OWN answer back: the repeat is two hits, and the row
    // each one hit is the row it recorded. A row holding the other stage's
    // type would be an identity collision, which one builder cannot mint -
    // which is why the lookup asserts the invariant rather than reporting a
    // miss and quietly recomputing.
    assert_eq!(*computed(pipeline.run(2, &"abcd".to_string())), "4");
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

    // The versions move so that the gate never answers: what differs below is
    // the STORE, which is the thing being controlled for.
    for version in 1..=2 {
        assert_eq!(*computed(cached.run(version, &input)), 4);
        assert_eq!(*computed(uncached.run(version, &input)), 4);
    }
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
    let Err(failure) = pipeline.run(1, &"abcd".to_string()) else {
        panic!("the second stage rejects, so the run must fail");
    };
    assert_eq!(failure.at(), 1);
    assert_eq!(*failure.error(), "rejected");

    // A failure is this run's answer, not the pipeline's verdict: nothing was
    // recorded, so the same version retries rather than being answered
    // `Unchanged` off a version the run never earned.
    let Err(again) = pipeline.run(1, &"abcd".to_string()) else {
        panic!("a failure records nothing, so the same version runs again");
    };
    assert_eq!(again.at(), 1);
}

// --- the version gate's wake half ------------------------------------------

/// Where an out-of-band value lands, and where a parked poll leaves its waker.
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

/// A stage over something that keeps changing, which KEEPS its subscription: it
/// stashes a fresh waker on every poll, ready or not, so a value landing after
/// it has already answered still reaches the pipeline.
///
/// That is the ordinary shape of a stage over a watched file, a socket or an
/// effect that lands more than once - and it is the shape that makes the wake
/// half of the version gate observable, because the second landing moves the
/// answer without moving the input version.
struct Watches {
    id: StageId,
    slot: Arc<Slot>,
}

impl Stage for Watches {
    type Input = ();
    type Output = u64;
    type Error = &'static str;

    fn id(&self) -> StageId {
        self.id
    }

    /// The slot is an ambient input no key over the ARGUMENT can address, so
    /// this refuses to key rather than inventing one.
    fn memo_key(&self, _input: &Self::Input) -> Option<MemoKey> {
        None
    }

    fn poll_stage(
        &self,
        _input: &Self::Input,
        cx: &mut Context<'_>,
    ) -> EffectPoll<Self::Output, Self::Error> {
        *self.slot.waker.lock().unwrap() = Some(cx.waker().clone());
        match *self.slot.value.lock().unwrap() {
            Some(value) => EffectPoll::Ready(value),
            None => EffectPoll::Pending,
        }
    }
}

#[test]
fn a_wake_at_an_unchanged_version_computes_rather_than_answering_unchanged() {
    // `DESIGN.md`, "The version gate and the one door": two different things
    // mean "something happened", and only one of them moves the version. The
    // input version moves when the source changes; a wake arrives when a value
    // some stage was waiting on has landed, and a landed effect does not move
    // the input version at all.
    let slot = Arc::<Slot>::default();
    let watching = Arc::clone(&slot);
    let pipeline = PipelineBuilder::new()
        .stage("watches", move |id| Watches { id, slot: watching })
        .build();

    // Nothing has landed: Delayed, and a Delayed run records no version.
    assert_eq!(pipeline.run(1, &()), Ok(Run::Delayed));

    // A value lands and wakes. The run it prompts computes, and THAT is what
    // records version 1.
    slot.land(7);
    assert_eq!(pipeline.run(1, &()), Ok(Run::Computed(Arc::new(7))));

    // Version 1 again with nothing having happened: the gate answers without
    // polling. This assertion is here so the one below cannot pass merely
    // because the gate never short-circuits at all.
    assert_eq!(pipeline.run(1, &()), Ok(Run::Unchanged));

    // And now the load-bearing one. A SECOND value lands out of band. The
    // version has not moved - the source did not change, an awaited value
    // simply arrived - so a gate that compared the version alone would answer
    // `Unchanged` here, and go on answering it forever while the caller held 7
    // and nothing reported the staleness.
    //
    // **Delete `!woken &&` from `Pipeline::run`'s gate and this is the
    // assertion that fails**, which is the only way to know the wake half is
    // doing anything: every other assertion in this file passes without it.
    slot.land(9);
    assert_eq!(
        pipeline.run(1, &()),
        Ok(Run::Computed(Arc::new(9))),
        "a wake is the other thing that means something happened, and the \
         stale flag is the only thing carrying it - the version cannot",
    );
}

#[test]
fn a_failure_type_is_spellable_in_one_line_at_any_length_of_chain() {
    // The flat error, seen from the signature side: three stages, one error
    // type, written out in full. A nested one would read
    // `ChainError<ChainError<..>, ..>` here and would grow with the chain.
    fn three_stages()
    -> libpipeline::Pipeline<u64, impl Stage<Input = String, Output = Arc<String>, Error = Failure<&'static str>>>
    {
        PipelineBuilder::new()
            .stage("len", |id| Len { id, runs: counter() })
            .stage("double", |id| Double { id, runs: counter() })
            .stage("render", |id| Render { id, runs: counter() })
            .build()
    }
    assert_eq!(*computed(three_stages().run(1, &"abcd".to_string())), "8");
}
