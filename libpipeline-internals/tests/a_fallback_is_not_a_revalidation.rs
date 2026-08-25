//! **Moved in from `tests/a_fallback_is_not_a_revalidation.rs`** at the
//! visibility flip. It composes the tracked layer by hand
//! (`Guarded`, `Ledger`, `Tracked`, `TrackedInput`), and the builder has no spelling for a tracked
//! graph - `PLAN.md`'s finding 1. A test in `tests/` proves the PUBLIC API
//! can express something; a test in `src/` admits it cannot yet, and lives
//! beside the code it pins so that a reshape of that code sees it. Every
//! assertion is the one it arrived with; when finding 1 lands this migrates
//! back out unchanged but for its imports.
//!
//! Gate: **a poll that produced no value leaves its node owing one - `Failed`
//! exactly as much as `Pending`.**
//!
//! # The second thing a boundary launders
//!
//! `a_boundary_is_not_a_cacheable_answer.rs` gates the first: a substituted
//! `Ready` must not be recorded by a [`Memo`](libpipeline_internals::memo::Memo), which
//! [`Guarded::memo_key`](libpipeline_internals::boundary::Guarded) closes structurally. A memo
//! remembers a VALUE. The [`Ledger`] remembers something else about the same
//! poll - **that the node is up to date** - and a substituted `Ready` is just as
//! false there.
//!
//! Two claims come out of that, and they are separate:
//!
//! 1. **`Tracked` treated `Failed` as a revalidation.**
//!    [`Ledger::run`] clears a node's staleness on the way IN, which is right
//!    for a poll that produces a value and wrong for one that does not - and
//!    `Tracked` re-marked only after `Pending`. A `Failed` poll cleared the
//!    node and left it clear, so a node that has never produced a value was
//!    reported valid by [`Ledger::is_stale`], dropped from
//!    [`Ledger::schedule`], and never polled again by anything that asks the
//!    ledger what to poll.
//!
//!    That state was **unreachable before the boundary layer**: a failure used to end the
//!    drive, so what the ledger thought about the failed node afterwards did not
//!    matter. A boundary is what lets the drive continue past it.
//!
//! 2. **A boundary therefore belongs OUTSIDE the tracking**, the same way it
//!    belongs outside the memo, and for the same reason. Inside, the tracked
//!    node's own poll answers `Ready(fallback)` - so the ledger is told the
//!    node is up to date, by a poll that substituted, and no fix to the
//!    `Failed` path can see that. The twin below measures what it costs.
//!
//! # What this can and cannot see
//!
//! The defect is invisible to a driver that re-polls unconditionally. The
//! offline driver loops until it gets a value and `FrameDriver` polls its root
//! every frame; both would reach the real answer regardless. What loses it is a
//! driver that polls what the LEDGER says needs polling, which is what
//! [`Schedule`](libpipeline_internals::schedule::Schedule) exists to answer. So `frame_if_scheduled`
//! below is that driver, in the smallest honest form - and the same shape as
//! `watch.rs`'s finding: a defect only one of the drivers can even express is a
//! defect that ships.
//!
//! **Every type here is a stand-in** (`DESIGN.md`, "The engine stays
//! generic").

use std::sync::{Arc, Mutex};
use std::task::{Context, Waker};

use libeffects::Fallback;
use libpipeline_internals::boundary::Guarded;
use libpipeline_internals::track::Ledger;
use libpipeline_internals::track::Tracked;
use libpipeline_internals::track::TrackedInput;
use libpipelinedata::{EffectPoll, MemoKey, StageAnswer, StageId};
use libpipeline_internals::{Stage};

// ---------------------------------------------------------------- stand-ins

/// Stand-in for whatever an author wrote.
#[derive(Clone, PartialEq, Eq, Debug)]
struct Text(String);

/// Transient in the sense a boundary cares about: it can clear.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct Boom;

/// What a stage does until its slot is filled.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum WhileEmpty {
    Failing,
    Parked,
}

/// A stage that reads a tracked input - observably, so the node has an edge -
/// and then answers according to its slot.
struct Flaky {
    while_empty: WhileEmpty,
    from: Arc<TrackedInput<String>>,
    slot: Mutex<Option<&'static str>>,
    waiting: Mutex<Vec<Waker>>,
    polls: Mutex<usize>,
}

impl Flaky {
    const ID: StageId = StageId::at(0);

    fn new(while_empty: WhileEmpty, from: &Arc<TrackedInput<String>>) -> Self {
        Self {
            while_empty,
            from: Arc::clone(from),
            slot: Mutex::new(None),
            waiting: Mutex::new(Vec::new()),
            polls: Mutex::new(0),
        }
    }

    /// The value arrives, and whoever was waiting is told to poll again.
    ///
    /// Note what this does NOT do: touch the ledger. Landing an effect's value
    /// is not a tracked write, so nothing here marks anything stale - the whole
    /// question is what the node's own poll left behind.
    fn land(&self, value: &'static str) {
        *self.slot.lock().unwrap() = Some(value);
        for waker in self.waiting.lock().unwrap().drain(..) {
            waker.wake();
        }
    }

    fn polls(&self) -> usize {
        *self.polls.lock().unwrap()
    }
}

impl Stage for Flaky {
    type Input = Text;
    type Output = String;
    type Error = Boom;

    fn id(&self) -> StageId {
        Self::ID
    }

    fn memo_key(&self, _input: &Text) -> Option<MemoKey> {
        None
    }

    fn poll_stage(&self, input: &Text, cx: &mut Context<'_>) -> EffectPoll<StageAnswer<Arc<String>>, Boom> {
        *self.polls.lock().unwrap() += 1;
        let read = self.from.get();
        let Some(filled) = *self.slot.lock().unwrap() else {
            self.waiting.lock().unwrap().push(cx.waker().clone());
            return match self.while_empty {
                WhileEmpty::Failing => EffectPoll::Failed(Boom),
                WhileEmpty::Parked => EffectPoll::Pending,
            };
        };
        StageAnswer::computed(Arc::new(format!("{}:{read}:{filled}", input.0)))
    }
}

const GUARD: StageId = StageId::at(1);

/// Poll unconditionally, as both drivers do.
fn driven<S: Stage>(stage: &S, input: &S::Input) -> EffectPoll<StageAnswer<Arc<S::Output>>, S::Error> {
    stage.poll_stage(input, &mut Context::from_waker(Waker::noop()))
}

/// **The driver that asks the ledger what is worth polling** - `Schedule`'s
/// intended consumer, in the smallest form that is still honest.
///
/// `None` means the schedule was empty: this driver had no reason to poll and
/// did not. Everything the ledger is wrong about is invisible to a driver that
/// polls anyway.
fn frame_if_scheduled<S: Stage>(
    ledger: &Ledger,
    root: &S,
    input: &S::Input,
) -> Option<EffectPoll<StageAnswer<Arc<S::Output>>, S::Error>> {
    let schedule = ledger.schedule().expect("no cycles in a one-node graph");
    if schedule.is_empty() {
        return None;
    }
    Some(driven(root, input))
}

// ---------------------------------------------------------------------------
// Gate 1: a poll that produced no value owes one, whichever way it failed to.
// ---------------------------------------------------------------------------

#[test]
fn a_failed_poll_leaves_its_node_owing_a_value() {
    let ledger = Ledger::new();
    let src = Arc::new(TrackedInput::new(&ledger, "src", "a".to_string()));
    let node = Tracked::new(&ledger, "n", Flaky::new(WhileEmpty::Failing, &src));

    assert!(!ledger.is_stale(node.node()), "nothing has happened yet");
    assert_eq!(driven(&node, &Text("src".into())), EffectPoll::Failed(Boom));
    assert!(
        ledger.is_stale(node.node()),
        "the run scope cleared this node on the way in, which is right for a \
         poll that produces a value; this one produced none, so the node still \
         owes one",
    );
    assert_eq!(ledger.stale_nodes(), vec![node.node()]);
}

#[test]
fn a_pending_poll_and_a_failed_poll_leave_the_same_debt() {
    // The symmetry is the whole argument. `Pending` has always re-marked, for
    // the reason stated in `Tracked`: a poll that did not produce a value has
    // not revalidated anything. `Failed` did not produce a value either.
    let ledger = Ledger::new();
    let src = Arc::new(TrackedInput::new(&ledger, "src", "a".to_string()));
    let parked = Tracked::new(&ledger, "parked", Flaky::new(WhileEmpty::Parked, &src));
    let failing = Tracked::new(&ledger, "failing", Flaky::new(WhileEmpty::Failing, &src));

    assert_eq!(driven(&parked, &Text("src".into())), EffectPoll::Pending);
    assert_eq!(driven(&failing, &Text("src".into())), EffectPoll::Failed(Boom));

    assert!(ledger.is_stale(parked.node()));
    assert!(ledger.is_stale(failing.node()));
}

#[test]
fn a_value_still_clears_the_debt() {
    // The control: this must not become "a tracked node is always stale".
    let ledger = Ledger::new();
    let src = Arc::new(TrackedInput::new(&ledger, "src", "a".to_string()));
    let node = Tracked::new(&ledger, "n", Flaky::new(WhileEmpty::Failing, &src));

    assert_eq!(driven(&node, &Text("src".into())), EffectPoll::Failed(Boom));
    assert!(ledger.is_stale(node.node()));

    node.stage().land("v1");
    assert_eq!(
        driven(&node, &Text("src".into())),
        StageAnswer::computed(Arc::new("src:a:v1".to_string())),
    );
    assert!(
        !ledger.is_stale(node.node()),
        "a poll that produced a value revalidated the node, which is what the \
         run scope's clear was for",
    );
}

// ---------------------------------------------------------------------------
// Gate 2: the composition - a boundary outside the tracking, and its twin.
// ---------------------------------------------------------------------------

#[test]
fn a_boundary_outside_the_tracking_replaces_its_fallback_when_the_failure_clears() {
    // The prescribed order: the ledger sees the FAILURE, and the substitution
    // happens above it - where no node id reaches.
    let ledger = Ledger::new();
    let src = Arc::new(TrackedInput::new(&ledger, "src", "a".to_string()));
    let node = Tracked::new(&ledger, "n", Flaky::new(WhileEmpty::Failing, &src));
    let watched = node.node();
    let guarded = Guarded::new(GUARD, node, Fallback::new(Arc::new("fallback".to_string())));
    let input = Text("src".into());

    // First frame: drawn with a stand-in, and the node still owes its answer.
    assert_eq!(driven(&guarded, &input), StageAnswer::computed(Arc::new("fallback".to_string())));
    assert_eq!(guarded.substitutions(), 1);
    assert!(
        ledger.is_stale(watched),
        "a fallback is a value the frame can draw AND a node that owes its \
         real one - which is exactly the distinction the value channel cannot \
         carry",
    );

    // The scheduled driver still has work, and still gets the fallback.
    assert_eq!(
        frame_if_scheduled(&ledger, &guarded, &input),
        Some(StageAnswer::computed(Arc::new("fallback".to_string()))),
    );

    // The failure clears out of band. Nothing marks anything stale - the
    // node's own debt is what keeps it in the schedule.
    guarded.stage().stage().land("v1");
    assert_eq!(
        frame_if_scheduled(&ledger, &guarded, &input),
        Some(StageAnswer::computed(Arc::new("src:a:v1".to_string()))),
        "the fallback is replaced by the real answer, by a driver that polls \
         only what the ledger names",
    );
    assert_eq!(guarded.substitutions(), 2, "and no third substitution");
    assert_eq!(
        frame_if_scheduled(&ledger, &guarded, &input),
        None,
        "the debt is settled, so there is nothing left to poll",
    );
}

#[test]
fn a_boundary_inside_the_tracking_keeps_its_fallback_after_the_failure_clears() {
    // THE TWIN. Same parts, one composition step reversed: the boundary is
    // INSIDE the node, so the node's own poll answers `Ready(fallback)` and the
    // ledger is told the node is up to date by a poll that substituted.
    //
    // No fix to the `Failed` path reaches this, which is why the rule is stated
    // on `Guarded` rather than enforced: the tracked node never sees a failure.
    let ledger = Ledger::new();
    let src = Arc::new(TrackedInput::new(&ledger, "src", "a".to_string()));
    let flaky = Flaky::new(WhileEmpty::Failing, &src);
    let node = Tracked::new(
        &ledger,
        "n",
        Guarded::new(GUARD, flaky, Fallback::new(Arc::new("fallback".to_string()))),
    );
    let input = Text("src".into());

    assert_eq!(driven(&node, &input), StageAnswer::computed(Arc::new("fallback".to_string())));
    assert!(
        !ledger.is_stale(node.node()),
        "the laundering: a substituted value cleared the node's debt, and the \
         ledger has no way to tell it was substituted",
    );

    // The failure clears - and nobody is told, because as far as the ledger is
    // concerned this node is up to date.
    node.stage().stage().land("v1");
    let polls = node.stage().stage().polls();
    assert_eq!(
        frame_if_scheduled(&ledger, &node, &input),
        None,
        "nothing is scheduled, so the driver polls nothing: the fallback is \
         permanent",
    );
    assert_eq!(node.stage().stage().polls(), polls, "and no work is done");

    // The real answer was available the whole time. What was lost is the
    // instruction to go and get it.
    assert_eq!(
        driven(&node, &input),
        StageAnswer::computed(Arc::new("src:a:v1".to_string())),
        "polled anyway, the graph answers correctly - which is why a driver \
         that re-polls unconditionally cannot see this defect at all",
    );
}

// ---------------------------------------------------------------------------
// Gate 3: what backdating does with a fallback - the amplification.
// ---------------------------------------------------------------------------

/// Open a scope for `reader` and poll `root` inside it, so the read is
/// attributed and `reader` becomes a consumer of the polled node.
///
/// A consumer written as a stage would need to hold the node it reads, and
/// `Stage` has no forwarding impl for `Arc<S>` (see the note in
/// `a_stage_boundary_catches_what_its_stage_raises.rs`). The ledger's own
/// public surface does the same job here: `run` is what a `Tracked` poll opens,
/// and `observe_read` is what a poll inside it logs.
fn read_as<S: Stage>(ledger: &Ledger, reader: libpipeline_internals::track::NodeId, root: &S, input: &S::Input) {
    ledger.run(reader, || {
        driven(root, input);
    });
}

#[test]
fn a_repeated_fallback_inside_backdating_retracts_what_its_consumers_owe() {
    // THE AMPLIFICATION, and it is the SAME forbidden composition: a boundary
    // inside the node. `Backdated` addresses each `Ready` output and, when the
    // address repeats, tells the ledger this node produced nothing new - so a
    // fallback that repeats retracts the staleness of everything reading it.
    // The consumers are then told "nothing moved" about a node that has never
    // produced its real answer at all.
    let ledger = Ledger::new();
    let src = Arc::new(TrackedInput::new(&ledger, "src", "a".to_string()));
    let node = libpipeline_internals::track::Backdated::new(
        &ledger,
        "n",
        Guarded::new(
            GUARD,
            Flaky::new(WhileEmpty::Failing, &src),
            Fallback::new(Arc::new("fallback".to_string())),
        ),
    );
    let consumer = ledger.node("c");
    let input = Text("src".into());

    // The consumer reads the node once, which is what makes it a consumer.
    read_as(&ledger, consumer, &node, &input);
    assert_eq!(ledger.readers_of(node.node()), vec![consumer]);

    // Something upstream moves, so both are stale.
    assert!(src.set("b".to_string()));
    assert!(ledger.is_stale(node.node()) && ledger.is_stale(consumer));

    // The node re-runs and substitutes the same fallback as last time.
    assert_eq!(driven(&node, &input), StageAnswer::computed(Arc::new("fallback".to_string())));
    assert!(
        !ledger.is_stale(consumer),
        "the fallback repeated, so backdating retracted the consumer's reason \
         to re-run - about a node whose real answer has never been computed",
    );
}

#[test]
fn a_repeated_failure_outside_backdating_retracts_nothing() {
    // The prescribed order, same parts. The node answers `Failed`, so there is
    // no output to address, nothing repeats, and nobody is told anything was
    // unchanged - and the node itself still owes a value.
    let ledger = Ledger::new();
    let src = Arc::new(TrackedInput::new(&ledger, "src", "a".to_string()));
    let node = libpipeline_internals::track::Backdated::new(&ledger, "n", Flaky::new(WhileEmpty::Failing, &src));
    let watched = node.node();
    let guarded = Guarded::new(GUARD, node, Fallback::new(Arc::new("fallback".to_string())));
    let consumer = ledger.node("c");
    let input = Text("src".into());

    read_as(&ledger, consumer, &guarded, &input);
    assert_eq!(ledger.readers_of(watched), vec![consumer]);

    assert!(src.set("b".to_string()));
    assert!(ledger.is_stale(watched) && ledger.is_stale(consumer));

    assert_eq!(driven(&guarded, &input), StageAnswer::computed(Arc::new("fallback".to_string())));
    assert!(
        ledger.is_stale(consumer),
        "no address was recorded for a failure, so nothing was retracted",
    );
    assert!(ledger.is_stale(watched), "and the node still owes its value");
}

// ---------------------------------------------------------------------------
// Gate 4: the debt is the node's own, and backdating cannot retract it.
// ---------------------------------------------------------------------------

#[test]
fn no_equality_below_a_failed_node_retracts_what_it_owes() {
    // `Ledger::unchanged` retracts a reason of the form "a dependency moved".
    // A node that owes a value is not stale for that reason, and an input that
    // recomputes to the same thing does not answer for a value that was never
    // produced. Same rule `Pending` has always had; `Reason::Owed` is why both
    // get it for free.
    let ledger = Ledger::new();
    let src = Arc::new(TrackedInput::new(&ledger, "src", "a".to_string()));
    let node = Tracked::new(&ledger, "n", Flaky::new(WhileEmpty::Failing, &src));

    assert_eq!(driven(&node, &Text("src".into())), EffectPoll::Failed(Boom));
    assert!(ledger.is_stale(node.node()));

    ledger.unchanged(src.node());
    assert!(
        ledger.is_stale(node.node()),
        "the input turning out not to have moved says nothing about a value \
         this node never produced",
    );
}
