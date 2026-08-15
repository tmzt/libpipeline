//! Gate: **a stage's reads are recorded as edges while it runs**
//! (`PIPELINE_PLAN.md` §3:225-230).
//!
//! The sentence being checked is "while a stage runs, every keyed read is
//! logged as an edge; the set is re-logged on every run so it follows
//! conditionals (a branch not taken this run contributes no edge)". Each test
//! below is one clause of it, asserted on the ledger's own answer rather than
//! on a consequence of it - what the graph does with the edges is the next
//! gate's subject, and mixing the two would leave neither pinned.
//!
//! **Every type here is a stand-in** (`PIPELINE_PLAN.md`:584-589). `Text` and
//! `Shout` are invented for this file; the engine has no expression type to
//! offer and this test suite is where that is proved rather than asserted.
//!
//! **The known-bad input is `Untracked`** - the same stage, minus the wrapper
//! that opens the run scope. It reads exactly what the tracked one reads and
//! the ledger records nothing, which is what makes the passing assertions
//! evidence rather than a scanner reporting on an empty graph (step 1's
//! precedent: `engine_stays_generic.rs`'s `A_MANIFEST_THAT_MUST_NOT_PASS`).

use std::sync::{Arc, Mutex};
use std::task::{Context, Waker};

use libpipeline::{Ledger, NodeId, Tracked, TrackedInput};
use libpipelinedata::{EffectPoll, MemoKey, Stage, StageId};

// ---------------------------------------------------------------- stand-ins

/// Stand-in for whatever an author wrote.
#[derive(Clone, PartialEq, Eq, Debug)]
struct Text(String);

/// Stand-in for a lowering's output.
#[derive(Clone, PartialEq, Eq, Debug)]
struct Shout(String);

/// A stage that reads one tracked input and nothing else.
struct Reads {
    from: Arc<TrackedInput<String>>,
}

impl Stage for Reads {
    type Input = Text;
    type Output = Shout;
    type Error = &'static str;

    fn id(&self) -> StageId {
        StageId::new("test.reads", 1)
    }

    fn memo_key(&self, _input: &Text) -> Option<MemoKey> {
        None
    }

    fn poll_stage(&self, input: &Text, _cx: &mut Context<'_>) -> EffectPoll<Shout, &'static str> {
        EffectPoll::Ready(Shout(format!("{}{}", input.0, self.from.get())))
    }
}

/// A stage that reads ONE of two inputs, chosen per run. §3's conditional: the
/// branch not taken contributes no edge.
struct ReadsEither {
    left: Arc<TrackedInput<String>>,
    right: Arc<TrackedInput<String>>,
    take_left: Mutex<bool>,
}

impl Stage for ReadsEither {
    type Input = Text;
    type Output = Shout;
    type Error = &'static str;

    fn id(&self) -> StageId {
        StageId::new("test.reads_either", 1)
    }

    fn memo_key(&self, _input: &Text) -> Option<MemoKey> {
        None
    }

    fn poll_stage(&self, _input: &Text, _cx: &mut Context<'_>) -> EffectPoll<Shout, &'static str> {
        let read = if *self.take_left.lock().unwrap() {
            self.left.get()
        } else {
            self.right.get()
        };
        EffectPoll::Ready(Shout(read))
    }
}

/// A stage that reads nothing at all - the "returned a value without reading"
/// shape a memo hit also has.
struct ReadsNothing;

impl Stage for ReadsNothing {
    type Input = Text;
    type Output = Shout;
    type Error = &'static str;

    fn id(&self) -> StageId {
        StageId::new("test.reads_nothing", 1)
    }

    fn memo_key(&self, _input: &Text) -> Option<MemoKey> {
        None
    }

    fn poll_stage(&self, input: &Text, _cx: &mut Context<'_>) -> EffectPoll<Shout, &'static str> {
        EffectPoll::Ready(Shout(input.0.clone()))
    }
}

/// A stage that polls another stage, so the ledger has two open scopes to
/// attribute reads between.
struct PollsAnother<S> {
    inner: S,
}

impl<S: Stage<Input = Text, Output = Shout, Error = &'static str>> Stage for PollsAnother<S> {
    type Input = Text;
    type Output = Shout;
    type Error = &'static str;

    fn id(&self) -> StageId {
        StageId::new("test.polls_another", 1)
    }

    fn memo_key(&self, _input: &Text) -> Option<MemoKey> {
        None
    }

    fn poll_stage(&self, input: &Text, cx: &mut Context<'_>) -> EffectPoll<Shout, &'static str> {
        self.inner.poll_stage(input, cx).map(|s| Shout(s.0))
    }
}

fn poll<S: Stage>(stage: &S, input: &S::Input) -> EffectPoll<S::Output, S::Error> {
    stage.poll_stage(input, &mut Context::from_waker(Waker::noop()))
}

fn labels(ledger: &Ledger, nodes: &[NodeId]) -> Vec<&'static str> {
    nodes.iter().map(|n| ledger.label(*n)).collect()
}

// --------------------------------------------------------------------- gate

#[test]
fn a_stage_that_reads_an_input_records_that_edge() {
    let ledger = Ledger::new();
    let title = Arc::new(TrackedInput::new(&ledger, "title", "!".to_string()));
    let stage = Tracked::new(
        &ledger,
        "reads",
        Reads {
            from: Arc::clone(&title),
        },
    );

    assert_eq!(
        poll(&stage, &Text("hi".to_string())),
        EffectPoll::Ready(Shout("hi!".to_string())),
    );

    assert_eq!(labels(&ledger, &ledger.reads_of(stage.node())), ["title"]);
    assert_eq!(labels(&ledger, &ledger.readers_of(title.node())), ["reads"]);
}

#[test]
fn the_same_read_without_a_run_scope_records_nothing() {
    // The known-bad input. `Untracked` is `Reads` with the wrapper removed and
    // nothing else changed: same input, same read, same answer. If this
    // recorded an edge anyway, the assertions above would be measuring
    // something other than observation - so this is what makes them mean what
    // they say.
    let ledger = Ledger::new();
    let title = Arc::new(TrackedInput::new(&ledger, "title", "!".to_string()));
    let untracked = Reads {
        from: Arc::clone(&title),
    };

    assert_eq!(
        poll(&untracked, &Text("hi".to_string())),
        EffectPoll::Ready(Shout("hi!".to_string())),
        "the answer is the same - only the observation is missing",
    );
    assert!(ledger.readers_of(title.node()).is_empty());
    assert_eq!(ledger.running(), None);
}

#[test]
fn a_read_outside_any_run_scope_belongs_to_nobody() {
    let ledger = Ledger::new();
    let title = TrackedInput::new(&ledger, "title", "!".to_string());
    assert_eq!(title.get(), "!", "a bare read still answers");
    assert!(
        ledger.readers_of(title.node()).is_empty(),
        "nothing was running, so nothing depends on it",
    );
}

#[test]
fn the_read_set_is_re_logged_on_every_run_so_a_branch_not_taken_contributes_no_edge() {
    // §3's conditional clause, which is the whole reason the set is re-logged
    // rather than accumulated: "a change behind it wakes nothing".
    let ledger = Ledger::new();
    let left = Arc::new(TrackedInput::new(&ledger, "left", "L".to_string()));
    let right = Arc::new(TrackedInput::new(&ledger, "right", "R".to_string()));
    let stage = Tracked::new(
        &ledger,
        "either",
        ReadsEither {
            left: Arc::clone(&left),
            right: Arc::clone(&right),
            take_left: Mutex::new(true),
        },
    );

    poll(&stage, &Text(String::new()));
    assert_eq!(labels(&ledger, &ledger.reads_of(stage.node())), ["left"]);
    assert_eq!(labels(&ledger, &ledger.readers_of(right.node())), [] as [&str; 0]);

    *stage.stage().take_left.lock().unwrap() = false;
    poll(&stage, &Text(String::new()));
    assert_eq!(
        labels(&ledger, &ledger.reads_of(stage.node())),
        ["right"],
        "the set is replaced, not accumulated",
    );
    assert_eq!(
        labels(&ledger, &ledger.readers_of(left.node())),
        [] as [&str; 0],
        "the reverse index moved with it - a stale reader here would wake a \
         node that no longer reads this input",
    );
}

#[test]
fn a_run_that_reads_nothing_keeps_the_edges_of_its_last_real_run() {
    // The conservative rule, stated in Ledger::run's doc and measured here so
    // it is a decision rather than an accident. A scope that observes no reads
    // is indistinguishable from a memo hit, and clearing the set would leave
    // the node permanently un-invalidatable.
    let ledger = Ledger::new();
    let left = Arc::new(TrackedInput::new(&ledger, "left", "L".to_string()));
    let right = Arc::new(TrackedInput::new(&ledger, "right", "R".to_string()));
    let reading = Tracked::new(
        &ledger,
        "either",
        ReadsEither {
            left: Arc::clone(&left),
            right,
            take_left: Mutex::new(true),
        },
    );
    poll(&reading, &Text(String::new()));
    assert_eq!(labels(&ledger, &ledger.reads_of(reading.node())), ["left"]);

    // A second node that never reads anything starts and stays empty: the rule
    // keeps a previous set, it does not invent one.
    let silent = Tracked::new(&ledger, "silent", ReadsNothing);
    poll(&silent, &Text("x".to_string()));
    assert!(ledger.reads_of(silent.node()).is_empty());

    // And the node that did read keeps its edge across a scope that read
    // nothing, which is the case that matters.
    ledger.run(reading.node(), || {});
    assert_eq!(
        labels(&ledger, &ledger.reads_of(reading.node())),
        ["left"],
        "a scope that observed nothing must not be read as `depends on nothing`",
    );
}

#[test]
fn a_read_is_attributed_to_the_innermost_stage_that_ran_it() {
    // Two open scopes: the outer stage polls the inner one, the inner one
    // reads. The outer must NOT acquire the inner's input as its own edge -
    // it depends on the inner node, which depends on the input. Flattening
    // that would make invalidation wake the outer stage for reasons it cannot
    // see, and would lose the intermediate node's own cutoff.
    let ledger = Ledger::new();
    let title = Arc::new(TrackedInput::new(&ledger, "title", "!".to_string()));
    let inner = Tracked::new(
        &ledger,
        "inner",
        Reads {
            from: Arc::clone(&title),
        },
    );
    let inner_node = inner.node();
    let outer = Tracked::new(&ledger, "outer", PollsAnother { inner });

    poll(&outer, &Text("hi".to_string()));

    assert_eq!(labels(&ledger, &ledger.reads_of(outer.node())), ["inner"]);
    assert_eq!(labels(&ledger, &ledger.reads_of(inner_node)), ["title"]);
    assert_eq!(
        labels(&ledger, &ledger.readers_of(title.node())),
        ["inner"],
        "the outer stage never read the title; it read the stage that did",
    );
}

#[test]
fn a_polled_node_is_recorded_as_read_even_when_its_own_run_reads_nothing() {
    // The memo-hit shape at the node-to-node edge: `Tracked` records that it
    // was READ before it opens its scope, so a consumer's edge survives a poll
    // that does no work. Without this ordering a memoized node would be
    // invisible to its own consumer on exactly the polls where it is cheapest.
    let ledger = Ledger::new();
    let inner = Tracked::new(&ledger, "inner", ReadsNothing);
    let inner_node = inner.node();
    let outer = Tracked::new(&ledger, "outer", PollsAnother { inner });

    poll(&outer, &Text("x".to_string()));

    assert_eq!(labels(&ledger, &ledger.reads_of(outer.node())), ["inner"]);
    assert!(ledger.reads_of(inner_node).is_empty());
}

#[test]
fn the_scope_closes_even_when_the_stage_panics() {
    // A poll that unwinds must not leave the ledger attributing later reads to
    // a stage that is no longer running - that is silent mistracking of exactly
    // the kind this layer exists to prevent.
    let ledger = Ledger::new();
    let node = ledger.node("panics");
    let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        ledger.run(node, || panic!("the stage gave up"));
    }));
    assert!(caught.is_err());
    assert_eq!(ledger.running(), None, "the scope closed on the way out");
}

#[test]
#[should_panic(expected = "was used against ledger")]
fn a_node_from_another_ledger_is_refused() {
    // Mistracking is silent by nature, so the seam is checked rather than
    // trusted: two ledgers in one process must not share an id space by
    // accident of both counting from zero.
    let one = Ledger::new();
    let two = Ledger::new();
    let node = one.node("mine");
    let _ = two.reads_of(node);
}
