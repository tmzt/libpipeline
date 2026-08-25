//! The builder as the only door (DESIGN.md).
//!
//! Every test here goes through the PUBLIC builder API - never through `Chain`,
//! `Memo`, `FrameDriver` or the stage contract directly. That restriction is
//! the point: if a property below could not be expressed this way, the builder
//! would be missing something a consumer needs, and `PLAN.md`'s findings
//! section would get a new entry instead of this file getting an internal
//! import.
//!
//! Since the door flip there is exactly one way to run a pipeline -
//! `poll(version, &input)` - so this file also measures what it answers:
//! `Computed` with a share of the value, `Unchanged` when the state it was
//! handed is the state it last computed for, `Delayed` when a wake is owed, and
//! a positioned `Failure` on the error side.
//!
//! # A stage is two functions, and the counters say why that is visible here
//!
//! Registration takes `fn` pointers, so a stage carries NO state of its own -
//! a capturing closure does not compile. Where an earlier draft of this file
//! handed each stage an `Arc<Mutex<usize>>` to count its runs, the counters are
//! now thread-locals: libtest gives each test its own thread, so each test
//! reads its own counts, and a `fn` can reach a thread-local where it cannot
//! reach a field.
//!
//! **That substitution is the finding, not a workaround.** A stage that needs
//! something from outside itself - a counter here, a font atlas or a module
//! runtime in earnest - has exactly one honest route today, and it is a
//! `static`. `Ctx` is where such a route belongs (`DESIGN.md`, "The intended
//! stage shape"), and it does not carry one yet.

use std::any::Any;
use std::cell::Cell;
use std::sync::{Arc, Mutex};
use std::task::Waker;

use libpipeline::{Ctx, Failure, Pipeline, PipelineBuilder, Run, RunResult};
use libpipelinedata::{ContentKey, EffectPoll, MemoKey, MemoMap};

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

/// The value a poll computed, or a panic naming what it answered instead.
///
/// It is a SHARE: the memo still holds the value it just answered with, which
/// is what a memo is, and `Run::Computed` says so in its type rather than
/// handing out a copy that pretends otherwise.
fn computed<T: std::fmt::Debug, E: std::fmt::Debug>(outcome: RunResult<T, E>) -> Arc<T> {
    match outcome {
        Ok(Run::Computed(value)) => value,
        other => panic!("expected a computed poll, got {other:?}"),
    }
}

thread_local! {
    static LEN_RUNS: Cell<usize> = const { Cell::new(0) };
    static DOUBLE_RUNS: Cell<usize> = const { Cell::new(0) };
    static RENDER_RUNS: Cell<usize> = const { Cell::new(0) };
}

fn bump(counter: &'static std::thread::LocalKey<Cell<usize>>) {
    counter.with(|c| c.set(c.get() + 1));
}

fn count(counter: &'static std::thread::LocalKey<Cell<usize>>) -> usize {
    counter.with(Cell::get)
}

// ----------------------------------------------------------------- stage fns

/// First stage: length of a string, counting its runs.
///
/// Its error type is the pipeline's one error type, shared with every stage it
/// is registered beside - `DESIGN.md`'s named cost of the flat error. `len`
/// never fails; it still spells the type its neighbours raise.
fn len_key(input: &String, ctx: &Ctx<'_>) -> Option<MemoKey> {
    Some(ctx.key([key_of_bytes(input.as_bytes())]))
}

fn len_poll(input: &String, _ctx: &Ctx<'_>) -> EffectPoll<usize, &'static str> {
    bump(&LEN_RUNS);
    EffectPoll::Ready(input.len())
}

/// Second stage: doubles, counting its runs. Its input is the previous stage's
/// output - the value, not the share the graph carries it in.
fn double_key(input: &usize, ctx: &Ctx<'_>) -> Option<MemoKey> {
    Some(ctx.key([ContentKey::from_u128(*input as u128)]))
}

fn double_poll(input: &usize, _ctx: &Ctx<'_>) -> EffectPoll<usize, &'static str> {
    bump(&DOUBLE_RUNS);
    EffectPoll::Ready(input * 2)
}

/// Third stage: renders the count, so a pipeline can hold two stages whose
/// outputs are different types.
fn render_key(input: &usize, ctx: &Ctx<'_>) -> Option<MemoKey> {
    Some(ctx.key([ContentKey::from_u128(*input as u128)]))
}

fn render_poll(input: &usize, _ctx: &Ctx<'_>) -> EffectPoll<String, &'static str> {
    bump(&RENDER_RUNS);
    EffectPoll::Ready(input.to_string())
}

// ---------------------------------------------------------------------- gates

#[test]
fn two_stages_compose_and_answer() {
    let pipeline = PipelineBuilder::new()
        .stage_fn("len", len_key, len_poll)
        .stage_fn("double", double_key, double_poll)
        .build();
    assert_eq!(*computed(pipeline.poll(1, &"abcd".to_string())), 8);
}

#[test]
fn a_capturing_closure_is_not_a_stage() {
    // The `fn` door, stated as the thing it refuses. Both functions below are
    // non-capturing closures, which coerce; a closure that captured the counter
    // would not compile, which is the whole enforcement (`DESIGN.md`, "A
    // trait-taking stage door" - `impl Fn` fails the same way one increment
    // earlier, by permitting capture).
    //
    // `tests/one_door_two_patterns.rs`'s module doc carries the compile-fail
    // twin this cannot state in-line: a `trybuild` fixture is a dependency this
    // stack's manifest gate forbids, so the refusal is recorded rather than
    // measured.
    let pipeline = PipelineBuilder::new()
        .stage_fn(
            "len",
            |input: &String, ctx: &Ctx<'_>| Some(ctx.key([key_of_bytes(input.as_bytes())])),
            |input: &String, _ctx: &Ctx<'_>| -> EffectPoll<usize, &'static str> {
                EffectPoll::Ready(input.len())
            },
        )
        .build();
    assert_eq!(*computed(pipeline.poll(1, &"abcd".to_string())), 4);
}

#[test]
fn an_unchanged_input_hits_at_the_first_stage() {
    let pipeline = PipelineBuilder::new()
        .stage_fn("len", len_key, len_poll)
        .stage_fn("double", double_key, double_poll)
        .build();

    assert_eq!(*computed(pipeline.poll(1, &"abcd".to_string())), 8);
    // A NEW version over the same content: the version gate cannot answer, so
    // the graph is entered - and every stage is a cache hit. That is the memo's
    // headline, and stating it at a moved version is what keeps this a test of
    // the memo rather than of the gate above it.
    assert_eq!(*computed(pipeline.poll(2, &"abcd".to_string())), 8);
    assert_eq!((count(&LEN_RUNS), count(&DOUBLE_RUNS)), (1, 1));

    // A different input reaches both stages...
    assert_eq!(*computed(pipeline.poll(3, &"xy".to_string())), 4);
    assert_eq!((count(&LEN_RUNS), count(&DOUBLE_RUNS)), (2, 2));
    // ...but one that lowers to an already-seen intermediate stops there:
    // "dcba" has the length "abcd" had, so `double` hits.
    assert_eq!(*computed(pipeline.poll(4, &"dcba".to_string())), 8);
    assert_eq!((count(&LEN_RUNS), count(&DOUBLE_RUNS)), (3, 2));
}

#[test]
fn the_same_version_answers_unchanged_without_touching_the_graph() {
    // The gate, on its own: the version the pipeline last computed for, handed
    // back. The readable is never dereferenced and no stage is polled - which
    // is measurable here, because the input handed to the second poll is a
    // DIFFERENT string. A poll that reached the graph would answer 2, not 4.
    let pipeline = PipelineBuilder::new()
        .stage_fn("len", len_key, len_poll)
        .build();

    assert_eq!(*computed(pipeline.poll(7, &"abcd".to_string())), 4);
    assert_eq!(pipeline.poll(7, &"xy".to_string()), Ok(Run::Unchanged));
    assert_eq!(count(&LEN_RUNS), 1, "nothing was polled");

    // And the gate is not a latch: a version it has not computed for reaches
    // the graph, whatever it answered a moment ago.
    assert_eq!(*computed(pipeline.poll(8, &"xy".to_string())), 2);
    assert_eq!(count(&LEN_RUNS), 2);
}

#[test]
fn two_stages_may_share_a_name_with_no_consequence() {
    // The test `DESIGN.md` names for the label discipline: "a label two steps
    // can share without consequence is a label nothing depends on". Both
    // stages here are registered under one name, and nothing about the
    // pipeline moves - identity is the position, which the builder minted
    // separately for each and handed to each function through `Ctx`.
    let shared = PipelineBuilder::new()
        .stage_fn("same", len_key, len_poll)
        .stage_fn("same", double_key, double_poll)
        .build();

    assert_eq!(*computed(shared.poll(1, &"abcd".to_string())), 8);
    assert_eq!(*computed(shared.poll(2, &"abcd".to_string())), 8);
    // Two rows in one store under one name: each stage was looked up and hit
    // on its own identity, so neither served the other its answer and neither
    // ran twice.
    assert_eq!((count(&LEN_RUNS), count(&DOUBLE_RUNS)), (1, 1));

    // And a different input still reaches both, which is what says the first
    // repeat was a hit rather than a stage that had stopped running.
    assert_eq!(*computed(shared.poll(3, &"xy".to_string())), 4);
    assert_eq!((count(&LEN_RUNS), count(&DOUBLE_RUNS)), (2, 2));
}

#[test]
fn one_store_serves_stages_of_different_output_types() {
    // Where the pipeline remembers is one decision, taken once, about the
    // whole pipeline - and the rows are erased, so one store serves stages
    // that do not agree about their output type. These two produce a `usize`
    // and a `String`, into the one store the builder was handed.
    //
    // The store is instantiated at the UNSIZED erased type, which is what makes
    // recording a coercion of the share the stage already answered with rather
    // than a second wrapping of it.
    let store: Arc<MemoMap<dyn Any + Send + Sync>> = Arc::new(MemoMap::new());
    let pipeline = PipelineBuilder::new()
        .store(Arc::clone(&store))
        .stage_fn("len", len_key, len_poll)
        .stage_fn("render", render_key, render_poll)
        .build();

    assert_eq!(*computed(pipeline.poll(1, &"abcd".to_string())), "4");
    assert_eq!(store.len(), 2, "one store, one row per stage");

    // Each stage got its OWN answer back: the repeat is two hits, and the row
    // each one hit is the row it recorded. A row holding the other stage's
    // type would be an identity collision, which one builder cannot mint -
    // which is why the lookup asserts the invariant rather than reporting a
    // miss and quietly recomputing.
    assert_eq!(*computed(pipeline.poll(2, &"abcd".to_string())), "4");
    assert_eq!((count(&LEN_RUNS), count(&RENDER_RUNS)), (1, 1));
    assert_eq!(store.len(), 2);
}

#[test]
fn uncached_is_the_control_run_answers_hold_speed_does_not() {
    let input = "abcd".to_string();

    let cached = PipelineBuilder::new()
        .stage_fn("len", len_key, len_poll)
        .build();
    // The versions move so that the gate never answers: what differs below is
    // the STORE, which is the thing being controlled for.
    for version in 1..=2 {
        assert_eq!(*computed(cached.poll(version, &input)), 4);
    }
    let cached_runs = count(&LEN_RUNS);

    let uncached = PipelineBuilder::new()
        .uncached()
        .stage_fn("len", len_key, len_poll)
        .build();
    for version in 1..=2 {
        assert_eq!(*computed(uncached.poll(version, &input)), 4);
    }

    assert_eq!(cached_runs, 1);
    assert_eq!(count(&LEN_RUNS) - cached_runs, 2, "the control ran every time");
}

/// A second-stage failure, for the position the pipeline stamps on it.
fn reject_key(_input: &usize, _ctx: &Ctx<'_>) -> Option<MemoKey> {
    None
}

fn reject_poll(_input: &usize, _ctx: &Ctx<'_>) -> EffectPoll<usize, &'static str> {
    EffectPoll::Failed("rejected")
}

#[test]
fn a_failure_names_the_stage_that_raised_it() {
    let pipeline = PipelineBuilder::new()
        .stage_fn("len", len_key, len_poll)
        .stage_fn("reject", reject_key, reject_poll)
        .build();
    // Which stage failed is a position, answered in one call - not a count of
    // `First`/`Second` layers read off a nested type.
    let Err(failure) = pipeline.poll(1, &"abcd".to_string()) else {
        panic!("the second stage rejects, so the poll must fail");
    };
    assert_eq!(failure.at(), 1);
    assert_eq!(*failure.error(), "rejected");

    // A failure is this poll's answer, not the pipeline's verdict: nothing was
    // recorded, so the same version retries rather than being answered
    // `Unchanged` off a version the poll never earned.
    let Err(again) = pipeline.poll(1, &"abcd".to_string()) else {
        panic!("a failure records nothing, so the same version polls again");
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

thread_local! {
    /// The slot the watching stages below read.
    ///
    /// It is a thread-local because a stage is a `fn` and cannot capture one.
    /// libtest runs each test on its own thread, so each test has its own slot;
    /// a real consumer would reach its upstream through `Ctx` if `Ctx` carried
    /// a route to one, and this file's module doc records that it does not.
    static SLOT: Arc<Slot> = Arc::new(Slot::default());
}

fn slot() -> Arc<Slot> {
    SLOT.with(Arc::clone)
}

fn watches_key(_input: &(), _ctx: &Ctx<'_>) -> Option<MemoKey> {
    // The slot is an ambient input no key over the ARGUMENT can address, so
    // this refuses to key rather than inventing one.
    None
}

/// A stage over something that keeps changing, which KEEPS its subscription: it
/// stashes a fresh waker on every poll, ready or not, so a value landing after
/// it has already answered still reaches the pipeline.
///
/// That is the ordinary shape of a stage over a watched file, a socket or an
/// effect that lands more than once - and it is the shape that makes the wake
/// half of the version gate observable, because the second landing moves the
/// answer without moving the input version.
fn watches_poll(_input: &(), ctx: &Ctx<'_>) -> EffectPoll<u64, &'static str> {
    let slot = slot();
    *slot.waker.lock().unwrap() = Some(ctx.waker().clone());
    match *slot.value.lock().unwrap() {
        Some(value) => EffectPoll::Ready(value),
        None => EffectPoll::Pending,
    }
}

/// The same stage minus the one line that keeps the subscription.
fn forgetful_poll(_input: &(), _ctx: &Ctx<'_>) -> EffectPoll<u64, &'static str> {
    match *slot().value.lock().unwrap() {
        Some(value) => EffectPoll::Ready(value),
        None => EffectPoll::Pending,
    }
}

#[test]
fn a_wake_at_an_unchanged_version_computes_rather_than_answering_unchanged() {
    // `DESIGN.md`, "The version gate and the one door": two different things
    // mean "something happened", and only one of them moves the version. The
    // input version moves when the source changes; a wake arrives when a value
    // some stage was waiting on has landed, and a landed effect does not move
    // the input version at all.
    let pipeline = PipelineBuilder::new()
        .stage_fn("watches", watches_key, watches_poll)
        .build();

    // Nothing has landed: Delayed, and a Delayed poll records no version.
    assert_eq!(pipeline.poll(1, &()), Ok(Run::Delayed));

    // A value lands and wakes. The poll it prompts computes, and THAT is what
    // records version 1.
    slot().land(7);
    assert_eq!(pipeline.poll(1, &()), Ok(Run::Computed(Arc::new(7))));

    // Version 1 again with nothing having happened: the gate answers without
    // polling. This assertion is here so the one below cannot pass merely
    // because the gate never short-circuits at all.
    assert_eq!(pipeline.poll(1, &()), Ok(Run::Unchanged));

    // And now the load-bearing one. A SECOND value lands out of band. The
    // version has not moved - the source did not change, an awaited value
    // simply arrived - so a gate that compared the version alone would answer
    // `Unchanged` here, and go on answering it forever while the caller held 7
    // and nothing reported the staleness.
    //
    // **Delete `!woken &&` from `Pipeline::poll`'s gate and this is the
    // assertion that fails**, which is the only way to know the wake half is
    // doing anything: every other assertion in this file passes without it.
    slot().land(9);
    assert_eq!(
        pipeline.poll(1, &()),
        Ok(Run::Computed(Arc::new(9))),
        "a wake is the other thing that means something happened, and the \
         version cannot carry it",
    );
}

#[test]
fn a_stage_that_forgets_its_waker_makes_its_value_lost_rather_than_late() {
    // The control for the gate test above, and what a broken wake promise costs
    // once there is no `take_stale` to ask about it: the defect is visible in
    // the OUTCOME, which is the only place a caller ever sees anything.
    //
    // `forgetful_poll` is `watches_poll` minus the line that keeps the
    // subscription. The first landing is picked up anyway - a Delayed poll
    // records no version, so the next poll enters the graph regardless. The
    // SECOND landing is the one that needs a wake, and never gets one.
    let pipeline = PipelineBuilder::new()
        .stage_fn("forgets", watches_key, forgetful_poll)
        .build();

    assert_eq!(pipeline.poll(1, &()), Ok(Run::Delayed));
    slot().land(7);
    assert_eq!(pipeline.poll(1, &()), Ok(Run::Computed(Arc::new(7))));

    slot().land(9);
    assert_eq!(
        pipeline.poll(1, &()),
        Ok(Run::Unchanged),
        "the value landed and nothing told the pipeline; the caller holds 7 \
         forever and no outcome ever says otherwise - which is what \
         `Stage::poll`'s wake obligation exists to prevent",
    );
}

#[test]
fn a_pipeline_type_is_spellable_in_the_consumers_own_types() {
    // The flat error, seen from the signature side: three stages, one error
    // type, written out in full. A nested one would read
    // `ChainError<ChainError<..>, ..>` here and would grow with the chain.
    //
    // And the other half, which is this wave's: the pipeline's type names NO
    // trait of the machinery. `Stage` is `libpipeline-internals`' now, and a
    // return type that had to say `impl Stage<..>` would have put it back in
    // the facade's public vocabulary under a different spelling.
    fn three_stages() -> Pipeline<u64, String, String, &'static str> {
        PipelineBuilder::new()
            .stage_fn("len", len_key, len_poll)
            .stage_fn("double", double_key, double_poll)
            .stage_fn("render", render_key, render_poll)
            .build()
    }
    assert_eq!(*computed(three_stages().poll(1, &"abcd".to_string())), "8");

    // The failure type, spelled once, whatever the length of the chain.
    let _: fn(Failure<&'static str>) -> usize = |failure| failure.at();
}
