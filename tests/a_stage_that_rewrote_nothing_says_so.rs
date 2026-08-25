//! A stage answers `Unchanged` or `Computed`, and `Unchanged` costs the stages
//! after it nothing.
//!
//! The ladder's cheapest rung, through the public door (`DESIGN.md`, and
//! `PLAN.md`'s "What the session settled"). Early cutoff is OBSERVED here
//! rather than reconstructed: a pass that walked its input and rewrote nothing
//! says so at its return, and because `StageAnswer::Unchanged` carries NO
//! VALUE, the stage after it has no input to be polled over and is never
//! entered at all. That is the saving. A variant carrying the value would have
//! saved a refcount bump; this saves the rest of the chain.
//!
//! Every test goes through the public builder, as
//! `builder_is_the_only_door.rs` requires of itself and for the same reason.
//!
//! # What is measured, and what could not be
//!
//! * **That the downstream is not entered** - a poll counter the downstream
//!   increments, read after three polls. Nothing else in this file can move
//!   that number, which is what makes it the gate: an implementation that
//!   passed the `Unchanged` along as a value would leave it at three.
//! * **That a cold slot is a defect** - `Unchanged` refers to the answer this
//!   position last gave, so a position that has given none is claiming
//!   something that does not exist, and the engine panics naming the position.
//! * **That an `Unchanged` upstream does not strand a downstream that owes a
//!   wake.** A downstream which answered `Pending` has produced nothing, so
//!   there is no answer for the chain to stand on and it must be re-polled.
//!   This is the same "lost rather than late" class as a forgotten waker,
//!   arriving through the new variant, and it is invisible in exactly the same
//!   way - a caller sees a legitimate-looking `Unchanged` and holds nothing.
//! * **What no test here can see**: whether an `Unchanged` was TRUE. A stage
//!   that answers it while its output moved makes the pipeline serve a stale
//!   value forever, and answers do not change - only which answer is served
//!   does. Catching that needs the key comparison of `PLAN.md`'s step 10.
//!
//! Counters are thread-locals because a stage is a `fn` pointer and cannot
//! capture one; `builder_is_the_only_door.rs`'s module doc records why that
//! substitution is a finding about `Ctx` rather than a workaround.

use std::cell::Cell;
use std::sync::{Arc, Mutex};
use std::task::Waker;

use libpipeline::{Ctx, PipelineBuilder, Run};
use libpipelinedata::{EffectPoll, MemoKey, StageAnswer};

thread_local! {
    /// How many times the FIRST stage's poll function was entered.
    static SETTLES_RUNS: Cell<usize> = const { Cell::new(0) };
    /// How many times the SECOND stage's poll function was entered. The gate.
    static DOWNSTREAM_RUNS: Cell<usize> = const { Cell::new(0) };
}

fn bump(counter: &'static std::thread::LocalKey<Cell<usize>>) {
    counter.with(|c| c.set(c.get() + 1));
}

fn count(counter: &'static std::thread::LocalKey<Cell<usize>>) -> usize {
    counter.with(Cell::get)
}

/// Neither stage keys.
///
/// **Deliberate, and load-bearing.** A stage with a key function is served from
/// the store before its poll function is entered, so it would never get the
/// chance to answer `Unchanged` at all - the memo would answer `Computed` with
/// the hit. Refusing to key is what `PLAN.md`'s step 10 makes the default, and
/// it is the shape this rung is for: enter the stage, let it walk, let it
/// answer for itself.
fn unkeyed<I>(_input: &I, _ctx: &Ctx<'_>) -> Option<MemoKey> {
    None
}

/// Computes on its first poll and answers `Unchanged` on every one after.
///
/// The stand-in for a domain pass whose tree declared nothing it rewrites,
/// which its own vocabulary already knows at its return.
fn settles_poll(input: &String, _ctx: &Ctx<'_>) -> EffectPoll<StageAnswer<usize>, &'static str> {
    bump(&SETTLES_RUNS);
    if count(&SETTLES_RUNS) == 1 {
        StageAnswer::computed(input.len())
    } else {
        StageAnswer::unchanged()
    }
}

/// The stage after it, which exists to be counted.
fn downstream_poll(input: &usize, _ctx: &Ctx<'_>) -> EffectPoll<StageAnswer<String>, &'static str> {
    bump(&DOWNSTREAM_RUNS);
    StageAnswer::computed(input.to_string())
}

#[test]
fn an_unchanged_upstream_means_the_downstream_is_never_polled() {
    let pipeline = PipelineBuilder::new()
        .stage_fn("settles", unkeyed, settles_poll)
        .stage_fn("downstream", unkeyed, downstream_poll)
        .build();

    // Poll 1, cold. The first stage computes, so the second is handed a value
    // and computes over it.
    assert_eq!(
        pipeline.poll(1, &"abcd".to_string()),
        Ok(Run::Computed(Arc::new("4".to_string()))),
    );
    assert_eq!((count(&SETTLES_RUNS), count(&DOWNSTREAM_RUNS)), (1, 1));

    // Polls 2 and 3, at versions that MOVED - so the version gate does not
    // answer for them and the graph really is entered each time. Without the
    // moving version this test would pass off the gate alone and measure
    // nothing.
    assert_eq!(pipeline.poll(2, &"abcd".to_string()), Ok(Run::Unchanged));
    assert_eq!(pipeline.poll(3, &"abcd".to_string()), Ok(Run::Unchanged));

    assert_eq!(
        count(&SETTLES_RUNS),
        3,
        "the first stage was entered on every poll - it is unkeyed, so nothing \
         answers ahead of it",
    );
    assert_eq!(
        count(&DOWNSTREAM_RUNS),
        1,
        "the second stage was entered ONCE, on the poll where its input was \
         computed. `Unchanged` carries no value, so there was nothing to poll \
         it over - which is the whole of the rung",
    );
}

#[test]
fn the_control_run_changes_speed_and_not_answers() {
    // `DESIGN.md`'s standing rule for `.uncached()`: a pipeline whose ANSWERS
    // change when the store is switched off has a bug the store was hiding.
    //
    // It applies here with a twist worth pinning: the SLOT is not the store.
    // The slot is what `Unchanged` refers to and it exists whether or not the
    // pipeline remembers anything, so an implementation that put the slot in
    // the store would change this pipeline's answers - and this is the
    // assertion that would say so.
    let pipeline = PipelineBuilder::new()
        .uncached()
        .stage_fn("settles", unkeyed, settles_poll)
        .stage_fn("downstream", unkeyed, downstream_poll)
        .build();

    assert_eq!(
        pipeline.poll(1, &"abcd".to_string()),
        Ok(Run::Computed(Arc::new("4".to_string()))),
    );
    assert_eq!(pipeline.poll(2, &"abcd".to_string()), Ok(Run::Unchanged));
    assert_eq!((count(&SETTLES_RUNS), count(&DOWNSTREAM_RUNS)), (2, 1));
}

// --- the cold slot ---------------------------------------------------------

/// Answers `Unchanged` immediately, having answered nothing before.
fn premature_poll<I>(_input: &I, _ctx: &Ctx<'_>) -> EffectPoll<StageAnswer<usize>, &'static str> {
    StageAnswer::unchanged()
}

#[test]
#[should_panic(expected = "stage at position 0 answered Unchanged before it had answered at all")]
fn a_first_stage_cannot_answer_unchanged_on_a_cold_pipeline() {
    // The inductive base of the invariant. Every `Unchanged` refers to an
    // earlier answer at the same position; follow the chain of references back
    // and it ends at a first stage on a cold pipeline, which has no upstream to
    // have told it anything and must compute.
    //
    // The caller is what the panic protects: `Run::Unchanged` means "keep what
    // you hold", and this caller holds nothing.
    let pipeline = PipelineBuilder::new()
        .stage_fn("premature", unkeyed, premature_poll::<String>)
        .build();
    let _ = pipeline.poll(1, &"abcd".to_string());
}

#[test]
#[should_panic(expected = "stage at position 1 answered Unchanged before it had answered at all")]
fn a_later_stage_cannot_answer_unchanged_before_it_has_answered() {
    // The same invariant one position along, which is what says the panic
    // NAMES the offending stage rather than printing a constant. Position 1's
    // slot is cold on this poll even though position 0's is not.
    let pipeline = PipelineBuilder::new()
        .stage_fn("settles", unkeyed, settles_poll)
        .stage_fn("premature", unkeyed, premature_poll::<usize>)
        .build();
    let _ = pipeline.poll(1, &"abcd".to_string());
}

// --- an unchanged upstream over a downstream that owes a wake ---------------

/// Where an out-of-band value lands, and where a parked poll leaves its waker.
#[derive(Default)]
struct Landing {
    value: Mutex<Option<u64>>,
    waker: Mutex<Option<Waker>>,
}

impl Landing {
    fn land(&self, value: u64) {
        *self.value.lock().unwrap() = Some(value);
        if let Some(waker) = self.waker.lock().unwrap().take() {
            waker.wake();
        }
    }
}

thread_local! {
    static LANDING: Arc<Landing> = Arc::new(Landing::default());
}

fn landing() -> Arc<Landing> {
    LANDING.with(Arc::clone)
}

/// A downstream that parks until a value lands, keeping its subscription.
fn awaits_poll(_input: &usize, ctx: &Ctx<'_>) -> EffectPoll<StageAnswer<u64>, &'static str> {
    bump(&DOWNSTREAM_RUNS);
    let landing = landing();
    *landing.waker.lock().unwrap() = Some(ctx.waker().clone());
    match *landing.value.lock().unwrap() {
        Some(value) => StageAnswer::computed(value),
        None => EffectPoll::Pending,
    }
}

#[test]
fn an_unchanged_upstream_does_not_skip_a_downstream_that_owes_a_wake() {
    // **The rung is not unconditional, and this is the condition.** Skipping
    // the downstream is sound because its answer over that input already
    // stands - but a downstream that answered `Pending` has no answer at all.
    // Skipping it would leave the pipeline reporting `Unchanged` at a caller
    // that holds nothing, for ever, with the landed value never delivered:
    // exactly the "lost rather than late" defect the wake half of the version
    // gate exists to prevent, arriving through the new variant instead.
    let pipeline = PipelineBuilder::new()
        .stage_fn("settles", unkeyed, settles_poll)
        .stage_fn("awaits", unkeyed, awaits_poll)
        .build();

    // Poll 1: the first stage computes; the second parks.
    assert_eq!(pipeline.poll(1, &"abcd".to_string()), Ok(Run::Delayed));
    assert_eq!((count(&SETTLES_RUNS), count(&DOWNSTREAM_RUNS)), (1, 1));

    // The awaited value lands and wakes the pipeline. The first stage now
    // answers `Unchanged` - truthfully; its own input has not moved.
    landing().land(7);
    assert_eq!(
        pipeline.poll(1, &"abcd".to_string()),
        Ok(Run::Computed(Arc::new(7))),
        "the join owed an answer over what it last handed on, so the \
         `Unchanged` upstream re-polls the downstream rather than standing on \
         an answer that was never given",
    );
    assert_eq!((count(&SETTLES_RUNS), count(&DOWNSTREAM_RUNS)), (2, 2));

    // And once it HAS settled, the rung applies again: the next moved version
    // skips the downstream.
    assert_eq!(pipeline.poll(2, &"abcd".to_string()), Ok(Run::Unchanged));
    assert_eq!((count(&SETTLES_RUNS), count(&DOWNSTREAM_RUNS)), (3, 2));
}
