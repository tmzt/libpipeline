//! **Moved in from `tests/an_equal_recompute_stops_at_its_node.rs`** at the
//! visibility flip. It composes the tracked layer by hand
//! (`Backdated`, `FrameDriver`, `Ledger`, `NodeId`, `Tracked`, `TrackedInput`), and the builder has no spelling for a tracked
//! graph - `PLAN.md`'s finding 1. A test in `tests/` proves the PUBLIC API
//! can express something; a test in `src/` admits it cannot yet, and lives
//! beside the code it pins so that a reshape of that code sees it. Every
//! assertion is the one it arrived with; when finding 1 lands this migrates
//! back out unchanged but for its imports.
//!
//! Gate: **backdating above the leaf** - a derived node that recomputes to the
//! value it already had leaves its consumers fresh.
//!
//! The design wants both halves - constructive keys give the LOOKUP,
//! backdating gives the CUTOFF, and a live interactive host needs both -
//! and until now only the leaf had one: [`TrackedInput::set`] refuses to
//! invalidate on a write of an equal value, exactly and for one comparison.
//! That does nothing for the named case, "without cutoff every keystroke
//! invalidates the whole pipeline",
//! because the keystroke DOES move the source. What saves the pipeline is the
//! first stage above it whose output ignores the difference - a formatter fed a
//! re-indented file, a lowering fed a renamed local - and that stage's
//! consumers were re-run anyway.
//!
//! The missing half needed "somewhere to keep the last output and an equality to
//! trust". `ContentHash` supplies the equality, so what is kept is
//! its 128-bit address rather than the output; the retraction is
//! [`Ledger::unchanged`], and staleness carries REASONS so that there is
//! something to retract.
//!
//! **Every type here is a stand-in** (`DESIGN.md`, "The engine stays
//! generic").

use std::sync::{Arc, Mutex};
use std::task::{Context, Waker};

use libpipeline_internals::track::Backdated;
use libpipeline_internals::driver::FrameDriver;
use libpipeline_internals::track::Ledger;
use libpipeline_internals::track::NodeId;
use libpipeline_internals::track::Tracked;
use libpipeline_internals::track::TrackedInput;
use libpipelinedata::{EffectPoll, MemoKey, StageId};
use libpipeline_internals::{Stage};

// ---------------------------------------------------------------- stand-ins

/// Stand-in for whatever an author wrote.
#[derive(Clone, PartialEq, Eq, Debug)]
struct Text(String);

/// A stage whose output IGNORES part of its input - the shape early cutoff
/// exists for. Trimming stands in for every lowering that discards formatting.
struct Normalizes {
    from: Arc<TrackedInput<String>>,
    runs: Mutex<usize>,
}

impl Normalizes {
    fn new(from: &Arc<TrackedInput<String>>) -> Self {
        Self {
            from: Arc::clone(from),
            runs: Mutex::new(0),
        }
    }

    fn runs(&self) -> usize {
        *self.runs.lock().unwrap()
    }
}

impl Stage for Normalizes {
    type Input = Text;
    type Output = String;
    type Error = &'static str;

    fn id(&self) -> StageId {
        StageId::at(0)
    }

    fn memo_key(&self, _input: &Text) -> Option<MemoKey> {
        None
    }

    fn poll_stage(&self, _input: &Text, _cx: &mut Context<'_>) -> EffectPoll<Arc<String>, &'static str> {
        *self.runs.lock().unwrap() += 1;
        EffectPoll::Ready(Arc::new(self.from.get().trim().to_string()))
    }
}

/// A consumer: polls another stage, reads nothing itself. Its run count is what
/// the cutoff is supposed to hold down.
struct Relays<S> {
    inner: Arc<S>,
    runs: Mutex<usize>,
}

impl<S> Relays<S> {
    fn new(inner: &Arc<S>) -> Self {
        Self {
            inner: Arc::clone(inner),
            runs: Mutex::new(0),
        }
    }

    fn runs(&self) -> usize {
        *self.runs.lock().unwrap()
    }
}

impl<S: Stage<Input = Text, Output = String, Error = &'static str>> Stage for Relays<S> {
    type Input = Text;
    type Output = String;
    type Error = &'static str;

    fn id(&self) -> StageId {
        StageId::at(1)
    }

    fn memo_key(&self, _input: &Text) -> Option<MemoKey> {
        None
    }

    fn poll_stage(&self, input: &Text, cx: &mut Context<'_>) -> EffectPoll<Arc<String>, &'static str> {
        *self.runs.lock().unwrap() += 1;
        self.inner.poll_stage(input, cx)
    }
}

/// A consumer that polls its inner stage - so the edge is real - and then never
/// lands. The effectful, pending-then-ready shape in miniature.
struct ParksAfterReading<S> {
    inner: Arc<S>,
}

impl<S: Stage<Input = Text, Output = String, Error = &'static str>> Stage for ParksAfterReading<S> {
    type Input = Text;
    type Output = String;
    type Error = &'static str;

    fn id(&self) -> StageId {
        StageId::at(2)
    }

    fn memo_key(&self, _input: &Text) -> Option<MemoKey> {
        None
    }

    fn poll_stage(&self, input: &Text, cx: &mut Context<'_>) -> EffectPoll<Arc<String>, &'static str> {
        let _ = self.inner.poll_stage(input, cx);
        let _ = cx.waker().clone();
        EffectPoll::Pending
    }
}

fn poll<S: Stage>(stage: &S, input: &S::Input) -> EffectPoll<Arc<S::Output>, S::Error> {
    stage.poll_stage(input, &mut Context::from_waker(Waker::noop()))
}

fn labels(ledger: &Ledger, nodes: &[NodeId]) -> Vec<&'static str> {
    nodes.iter().map(|n| ledger.label(*n)).collect()
}

fn input() -> Text {
    Text(String::new())
}

// --------------------------------------------------------------------- gate

#[test]
fn an_equal_recompute_leaves_every_consumer_above_it_fresh() {
    // The payoff, over a three-deep chain so the retraction has to travel:
    // `source` -> `a` (normalizes) -> `b` -> `c`. The write moves the source and
    // does not move `a`'s output.
    let ledger = Ledger::new();
    let source = Arc::new(TrackedInput::new(&ledger, "source", "A".to_string()));
    let a = Arc::new(Backdated::new(&ledger, "a", Normalizes::new(&source)));
    let b = Arc::new(Tracked::new(&ledger, "b", Relays::new(&a)));
    let c = Arc::new(Tracked::new(&ledger, "c", Relays::new(&b)));

    assert_eq!(poll(&*c, &input()), EffectPoll::Ready(Arc::new("A".to_string())));
    assert!(ledger.stale_nodes().is_empty(), "a poll is not a change");

    assert!(source.set("  A  ".to_string()), "the source really moved");
    assert_eq!(
        labels(&ledger, &ledger.stale_nodes()),
        ["a", "b", "c"],
        "and the ledger has to assume the worst: nothing can know `a`'s output \
         held still until `a` has run",
    );

    // Revalidating the bottom of the stale set is all it takes. This is
    // `Schedule::order()`'s shape - "the order in which a node's inputs are
    // known fresh" - and the point is what happens to the rest of that order.
    assert_eq!(poll(&*a, &input()), EffectPoll::Ready(Arc::new("A".to_string())));

    assert!(
        ledger.stale_nodes().is_empty(),
        "`a` produced what it produced last time, so `b` and `c` have nothing \
         to recompute - and the retraction reached `c`, two edges up",
    );
    assert!(
        ledger.schedule().expect("acyclic").is_empty(),
        "which is the form a driver reads it in: the next pass has no work",
    );
    assert_eq!(a.stage().runs(), 2, "`a` ran, which is how it was found out");
    assert_eq!(b.stage().runs(), 1, "and `b` did not");
    assert_eq!(c.stage().runs(), 1, "nor `c`");
}

#[test]
fn without_backdating_the_same_chain_recomputes_to_the_top() {
    // The known-bad twin: one word different - `Tracked` where the test above
    // has `Backdated` - and the same equal recompute leaves the whole chain
    // waiting, which is the state of things the wake contract calls out.
    let ledger = Ledger::new();
    let source = Arc::new(TrackedInput::new(&ledger, "source", "A".to_string()));
    let a = Arc::new(Tracked::new(&ledger, "a", Normalizes::new(&source)));
    let b = Arc::new(Tracked::new(&ledger, "b", Relays::new(&a)));
    let c = Arc::new(Tracked::new(&ledger, "c", Relays::new(&b)));

    poll(&*c, &input());
    source.set("  A  ".to_string());
    assert_eq!(poll(&*a, &input()), EffectPoll::Ready(Arc::new("A".to_string())));

    assert_eq!(
        labels(&ledger, &ledger.stale_nodes()),
        ["b", "c"],
        "`a` recomputed the same answer and nothing was allowed to notice",
    );
    poll(&*c, &input());
    assert_eq!(b.stage().runs(), 2, "so `b` re-ran for nothing");
    assert_eq!(c.stage().runs(), 2, "and so did `c`");
}

#[test]
fn a_recompute_that_moves_the_value_still_marks_its_consumers() {
    // The direction that must NOT be cut off, and the reason the address is
    // recorded on every `Ready` rather than only when it repeats.
    let ledger = Ledger::new();
    let source = Arc::new(TrackedInput::new(&ledger, "source", "A".to_string()));
    let a = Arc::new(Backdated::new(&ledger, "a", Normalizes::new(&source)));
    let b = Arc::new(Tracked::new(&ledger, "b", Relays::new(&a)));

    poll(&*b, &input());
    source.set("B".to_string());
    assert_eq!(poll(&*a, &input()), EffectPoll::Ready(Arc::new("B".to_string())));
    assert_eq!(
        labels(&ledger, &ledger.stale_nodes()),
        ["b"],
        "the output moved, so the consumer still has work",
    );

    // And the node it cut off from is remembered, so a LATER equal recompute
    // cuts off against `B` rather than against the value before it.
    source.set(" B ".to_string());
    assert_eq!(poll(&*a, &input()), EffectPoll::Ready(Arc::new("B".to_string())));
    assert!(!ledger.is_stale(b.node()));
}

#[test]
fn the_first_poll_of_a_node_is_never_a_cutoff() {
    // There is no equality to appeal to with nothing recorded, and a consumer
    // that has never had this node's value cannot be spared having it.
    let ledger = Ledger::new();
    let source = Arc::new(TrackedInput::new(&ledger, "source", "A".to_string()));
    let a = Arc::new(Backdated::new(&ledger, "a", Normalizes::new(&source)));

    assert_eq!(a.last_address(), None);
    poll(&*a, &input());
    assert_eq!(a.last_address(), Some(libpipelinedata::ContentKey::of("A")));
}

#[test]
fn a_consumer_reached_by_two_changed_paths_needs_both_to_cut_off() {
    // Why staleness carries reasons rather than a bit. `b` polls two normalizing
    // nodes; both sources move without moving either output. After the first
    // cutoff `b` is still waiting on the second, and a bare stale set could not
    // have said so - it would either have cleared `b` early (wrong) or never
    // (no cutoff at all).
    let ledger = Ledger::new();
    let left_source = Arc::new(TrackedInput::new(&ledger, "left_source", "L".to_string()));
    let right_source = Arc::new(TrackedInput::new(&ledger, "right_source", "R".to_string()));
    let left = Arc::new(Backdated::new(&ledger, "left", Normalizes::new(&left_source)));
    let right = Arc::new(Backdated::new(
        &ledger,
        "right",
        Normalizes::new(&right_source),
    ));
    let both = Arc::new(Tracked::new(
        &ledger,
        "both",
        Joins {
            left: Arc::clone(&left),
            right: Arc::clone(&right),
            runs: Mutex::new(0),
        },
    ));

    assert_eq!(poll(&*both, &input()), EffectPoll::Ready(Arc::new("LR".to_string())));
    left_source.set(" L ".to_string());
    right_source.set(" R ".to_string());
    assert_eq!(
        labels(&ledger, &ledger.stale_nodes()),
        ["left", "right", "both"],
    );

    poll(&*left, &input());
    assert_eq!(
        labels(&ledger, &ledger.stale_nodes()),
        ["right", "both"],
        "one path cut off; `both` is still waiting on the other",
    );

    poll(&*right, &input());
    assert!(
        ledger.stale_nodes().is_empty(),
        "and now every reason it had is retracted",
    );
    assert_eq!(both.stage().runs(), 1);
}

/// A consumer of two nodes - the diamond's foot, for the two-reason case.
struct Joins {
    left: Arc<Backdated<Normalizes>>,
    right: Arc<Backdated<Normalizes>>,
    runs: Mutex<usize>,
}

impl Joins {
    fn runs(&self) -> usize {
        *self.runs.lock().unwrap()
    }
}

impl Stage for Joins {
    type Input = Text;
    type Output = String;
    type Error = &'static str;

    fn id(&self) -> StageId {
        StageId::at(3)
    }

    fn memo_key(&self, _input: &Text) -> Option<MemoKey> {
        None
    }

    fn poll_stage(&self, input: &Text, cx: &mut Context<'_>) -> EffectPoll<Arc<String>, &'static str> {
        *self.runs.lock().unwrap() += 1;
        let (EffectPoll::Ready(left), EffectPoll::Ready(right)) = (
            self.left.poll_stage(input, cx),
            self.right.poll_stage(input, cx),
        ) else {
            return EffectPoll::Pending;
        };
        EffectPoll::Ready(Arc::new(format!("{left}{right}")))
    }
}

#[test]
fn a_node_that_owes_a_value_is_not_retracted_by_the_node_below_it() {
    // The reason that cannot be taken back. A `Pending` poll marks its own node
    // - it produced no value, so it revalidated nothing - and no equality below
    // it answers for a value that was never produced. A cutoff that cleared this
    // would drop the parked node out of the schedule, which is "lost
    // rather than late" arriving by a new road.
    let ledger = Ledger::new();
    let source = Arc::new(TrackedInput::new(&ledger, "source", "A".to_string()));
    let a = Arc::new(Backdated::new(&ledger, "a", Normalizes::new(&source)));
    let parked = Arc::new(Tracked::new(
        &ledger,
        "parked",
        ParksAfterReading {
            inner: Arc::clone(&a),
        },
    ));

    assert!(poll(&*parked, &input()).is_pending());
    assert_eq!(labels(&ledger, &ledger.stale_nodes()), ["parked"]);

    source.set(" A ".to_string());
    assert_eq!(labels(&ledger, &ledger.stale_nodes()), ["a", "parked"]);

    poll(&*a, &input());
    assert_eq!(
        labels(&ledger, &ledger.stale_nodes()),
        ["parked"],
        "the dependency's reason was retracted and the node's own was not",
    );
}

#[test]
fn the_cutoff_saves_the_work_and_not_the_wake() {
    // The honest boundary, stated so it is not mistaken for a defect: finding
    // out that `a`'s output held still REQUIRED running `a`, which required a
    // driver to have been woken and to have polled it. What backdating saves is
    // everything above that node - which is where a pipeline's cost is.
    let ledger = Ledger::new();
    let source = Arc::new(TrackedInput::new(&ledger, "source", "A".to_string()));
    let a = Arc::new(Backdated::new(&ledger, "a", Normalizes::new(&source)));
    let b = Arc::new(Tracked::new(&ledger, "b", Relays::new(&a)));
    let driver = FrameDriver::new();
    ledger.subscribe(driver.waker());

    driver.poll_frame(&*b, &input());
    assert!(!driver.take_stale());

    source.set(" A ".to_string());
    assert!(
        driver.take_stale(),
        "the frame loop is woken by the write, before anyone can know what it \
         will come to",
    );

    driver.poll_frame(&*a, &input());
    assert!(
        ledger.schedule().expect("acyclic").is_empty(),
        "and the frame it woke for finds nothing left to do",
    );
}
