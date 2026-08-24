//! Scheduling: what a driver polls next, given the stale set.
//!
//! **In a pull graph the schedule is not what makes the answer right - the pull
//! is.** Polling a node polls what it reads, so any stale node reached from a
//! poll is revalidated on the way. What the schedule decides is how much work
//! that costs: which nodes the driver polls DIRECTLY, and in what order, so
//! that nothing is recomputed twice and nothing untouched is polled at all.
//!
//! That makes [`Schedule::to_poll`] the headline and [`Schedule::order`] the
//! supporting fact. A stale node that another stale node reads does not need
//! polling by the driver - its consumer will pull it - so the set worth polling
//! is the stale nodes with no stale reader. On the diamond graph (one input,
//! two readers, one joiner) that set is one node, and the shared consumer runs
//! once instead of once per stale path.

// **Dead in the shipped library, alive in the test build - and that is the
// flip's honest arithmetic rather than an oversight.**
//
// With the flat exports gone (`DESIGN.md`, "Migration plan") nothing outside
// this crate can name what is below, and the builder has no spelling for it
// yet, so the only callers are this module's own `#[cfg(test)]` tests. The
// allow is `not(test)` ON PURPOSE: under `cargo test` the lint stays fully
// armed, so code that becomes genuinely unused still fails the gate. It comes
// off the day the builder grows the spelling the findings name, because the
// builder will then be the caller.
#![cfg_attr(not(test), allow(dead_code))]


use std::collections::{BTreeMap, BTreeSet};

use crate::track::{Ledger, NodeId};

/// What to poll, and in what order, to revalidate everything currently stale.
///
/// **A snapshot.** It is computed from the ledger's stale set and edges as they
/// are, and a change landing afterwards is not in it. That is the right
/// lifetime for the thing that consumes it: a frame loop takes one per frame, a
/// CLI run takes one per pass.
///
/// **Ids, not work.** A `Schedule` says which NODES; mapping a node to the
/// typed stage that polls it is the caller's, because the engine is generic
/// over every payload type and the stages of one graph do not share an
/// `Input` or `Output` (`DESIGN.md`, "The engine stays generic"). A registry of type-erased
/// closures could live here, but it would be the caller's map with the types
/// thrown away rather than something the engine knows.
#[derive(Clone, PartialEq, Eq, Debug)]
pub(crate) struct Schedule {
    to_poll: Vec<NodeId>,
    order: Vec<NodeId>,
}

impl Schedule {
    /// The nodes a driver should poll, in dependency order - the stale nodes
    /// that no stale node reads.
    ///
    /// Everything else stale is reachable from one of these by a poll, so
    /// polling them and nothing else revalidates the whole stale set with each
    /// node run once. Polling the full stale set instead is not wrong, it is
    /// just work done twice: a node polled directly and again through its
    /// consumer.
    pub(crate) fn to_poll(&self) -> &[NodeId] {
        &self.to_poll
    }

    /// Every stale node, dependencies before dependents, each appearing once.
    ///
    /// The order a driver would use if it revalidated bottom-up rather than by
    /// pulling from the top - and the order in which a node's inputs are known
    /// fresh, which is what a memo needs to hit rather than recompute.
    pub(crate) fn order(&self) -> &[NodeId] {
        &self.order
    }

    /// Nothing is stale.
    pub(crate) fn is_empty(&self) -> bool {
        self.order.is_empty()
    }
}

/// The stale set could not be ordered: some of it reads itself, directly or
/// through others.
///
/// The drivers' termination argument is the DAG's acyclicity plus effect
/// completion, so a cycle is a graph bug rather than a case to schedule around - and it is
/// reported rather than dropped or looped on, because the two silent answers
/// are worse than the loud one: dropping the nodes loses the work, and walking
/// the cycle would not stop.
#[derive(Clone, PartialEq, Eq, Debug)]
pub(crate) struct Cycle {
    nodes: Vec<NodeId>,
}

impl Cycle {
    /// The stale nodes that could not be ordered - the cycle and anything
    /// downstream of it.
    pub(crate) fn nodes(&self) -> &[NodeId] {
        &self.nodes
    }
}

impl Ledger {
    /// What to poll next, given what is currently stale.
    ///
    /// Built from the ledger's public answers only - the stale set and the two
    /// directions of its edges. Nothing here reaches into the ledger's
    /// internals, which is the check that scheduling is a consumer of tracking
    /// rather than a second place the graph lives.
    pub(crate) fn schedule(&self) -> Result<Schedule, Cycle> {
        let stale: BTreeSet<NodeId> = self.stale_nodes().into_iter().collect();

        // Within the stale set only: what each node must have fresh before it
        // is worth polling, and who is waiting on it. An edge to a node that is
        // NOT stale is not a prerequisite - that node is already valid.
        let mut waiting_on: BTreeMap<NodeId, BTreeSet<NodeId>> = BTreeMap::new();
        let mut blocks: BTreeMap<NodeId, BTreeSet<NodeId>> = BTreeMap::new();
        for node in &stale {
            let reads: BTreeSet<NodeId> = self
                .reads_of(*node)
                .into_iter()
                .filter(|read| stale.contains(read))
                .collect();
            for read in &reads {
                blocks.entry(*read).or_default().insert(*node);
            }
            waiting_on.insert(*node, reads);
        }

        // Kahn, taking ready nodes in mint order so the answer is
        // deterministic - two runs of the same graph must schedule the same
        // way or nothing built on this is reproducible.
        let mut order = Vec::with_capacity(stale.len());
        let mut ready: BTreeSet<NodeId> = waiting_on
            .iter()
            .filter(|(_, reads)| reads.is_empty())
            .map(|(node, _)| *node)
            .collect();
        while let Some(node) = ready.iter().next().copied() {
            ready.remove(&node);
            order.push(node);
            for dependent in blocks.get(&node).cloned().unwrap_or_default() {
                let reads = waiting_on.get_mut(&dependent).expect("stale by construction");
                reads.remove(&node);
                if reads.is_empty() {
                    ready.insert(dependent);
                }
            }
        }

        if order.len() != stale.len() {
            let ordered: BTreeSet<NodeId> = order.iter().copied().collect();
            return Err(Cycle {
                nodes: stale.difference(&ordered).copied().collect(),
            });
        }

        let to_poll = order
            .iter()
            .copied()
            .filter(|node| !blocks.contains_key(node))
            .collect();
        Ok(Schedule { to_poll, order })
    }
}

#[cfg(test)]
mod the_schedule_polls_each_node_once {
    //! **Moved in from `tests/the_schedule_polls_each_node_once.rs`** at the
    //! visibility flip. It composes the tracked layer and reads
    //! [`Ledger::schedule`] (`Ledger`, `NodeId`, `Schedule`, `Tracked`, `TrackedInput`), and the builder has no
    //! spelling for either - `DESIGN.md`'s findings 1 and 4. A test in `tests/`
    //! proves the PUBLIC API can express something; a test in `src/` admits it
    //! cannot yet, and lives beside the code it pins. Every assertion is the one
    //! it arrived with; when the findings land this migrates back out unchanged
    //! but for its imports.
    //!
    //! Gate: **what the driver polls next, given the stale set.**
    //!
    //! The graph is the diamond - one input, two stages that read it, one stage
    //! that reads both - because it is the smallest shape where the answer is not
    //! "poll everything": the shared consumer is reachable by two stale paths and
    //! must run once.
    //!
    //! **The known-bad input is the naive driver.** `poll_every_stale_node` is the
    //! same graph, the same change, and the schedule ignored; it re-runs the two
    //! middle stages twice each. Both drivers reach the same answers - what differs
    //! is the work - which is the same control shape `NoMemo` gives the memo layer
    //! ("a pipeline whose ANSWERS change when the cache is disabled has a bug
    //! the cache was hiding").
    //!
    //! **Every type here is a stand-in** (`DESIGN.md`, "The engine stays
    //! generic").

    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Waker};

    use crate::track::Ledger;
    use crate::track::NodeId;
    use crate::schedule::Schedule;
    use crate::track::Tracked;
    use crate::track::TrackedInput;
    use libpipelinedata::{EffectPoll, MemoKey, Stage, StageId};

    // ---------------------------------------------------------------- stand-ins

    /// Stand-in for whatever an author wrote.
    #[derive(Clone, PartialEq, Eq, Debug)]
    struct Text(String);

    /// A stage that reads one tracked input and counts its runs.
    struct Reads {
        from: Arc<TrackedInput<String>>,
        tag: &'static str,
        runs: Mutex<usize>,
    }

    impl Stage for Reads {
        type Input = Text;
        type Output = String;
        type Error = &'static str;

        fn id(&self) -> StageId {
            StageId::new("test.reads", 1)
        }

        fn memo_key(&self, _input: &Text) -> Option<MemoKey> {
            None
        }

        fn poll_stage(&self, _input: &Text, _cx: &mut Context<'_>) -> EffectPoll<String, &'static str> {
            *self.runs.lock().unwrap() += 1;
            EffectPoll::Ready(format!("{}{}", self.tag, self.from.get()))
        }
    }

    /// A stage that polls two others and joins their answers - the diamond's foot.
    struct Joins<A, B> {
        left: A,
        right: B,
        runs: Mutex<usize>,
    }

    // Over `Arc<A>` rather than `A`: the halves of a diamond are SHARED - the same
    // node is polled by its consumer and held by the driver's dispatcher - and
    // `Stage` cannot be implemented for `Arc<S>` here, since both the trait and
    // `Arc` are foreign to this crate.
    impl<A, B> Stage for Joins<Arc<A>, Arc<B>>
    where
        A: Stage<Input = Text, Output = String, Error = &'static str>,
        B: Stage<Input = Text, Output = String, Error = &'static str>,
    {
        type Input = Text;
        type Output = String;
        type Error = &'static str;

        fn id(&self) -> StageId {
            StageId::new("test.joins", 1)
        }

        fn memo_key(&self, _input: &Text) -> Option<MemoKey> {
            None
        }

        fn poll_stage(&self, input: &Text, cx: &mut Context<'_>) -> EffectPoll<String, &'static str> {
            *self.runs.lock().unwrap() += 1;
            let left = match self.left.poll_stage(input, cx) {
                EffectPoll::Ready(value) => value,
                other => return other,
            };
            let right = match self.right.poll_stage(input, cx) {
                EffectPoll::Ready(value) => value,
                other => return other,
            };
            EffectPoll::Ready(format!("{left}+{right}"))
        }
    }

    type Middle = Tracked<Reads>;
    type Foot = Tracked<Joins<Arc<Middle>, Arc<Middle>>>;

    /// The diamond: `source` read by `left` and `right`, both read by `foot`.
    struct Diamond {
        ledger: Arc<Ledger>,
        source: Arc<TrackedInput<String>>,
        left: Arc<Middle>,
        right: Arc<Middle>,
        foot: Arc<Foot>,
    }

    impl Diamond {
        fn new() -> Self {
            let ledger = Ledger::new();
            let source = Arc::new(TrackedInput::new(&ledger, "source", "A".to_string()));
            let middle = |tag| {
                Arc::new(Tracked::new(
                    &ledger,
                    tag,
                    Reads {
                        from: Arc::clone(&source),
                        tag,
                        runs: Mutex::new(0),
                    },
                ))
            };
            let left = middle("left");
            let right = middle("right");
            let foot = Arc::new(Tracked::new(
                &ledger,
                "foot",
                Joins {
                    left: Arc::clone(&left),
                    right: Arc::clone(&right),
                    runs: Mutex::new(0),
                },
            ));
            Self {
                ledger,
                source,
                left,
                right,
                foot,
            }
        }

        /// Everything the ledger knows how to poll, by node - the caller's map from
        /// an id to typed work, which is the half a generic engine cannot hold.
        fn dispatcher(&self) -> HashMap<NodeId, Box<dyn Fn() + '_>> {
            let mut by_node: HashMap<NodeId, Box<dyn Fn() + '_>> = HashMap::new();
            for stage in [&self.left, &self.right] {
                let stage = Arc::clone(stage);
                by_node.insert(stage.node(), Box::new(move || poll_once(&*stage)));
            }
            let foot = Arc::clone(&self.foot);
            by_node.insert(self.foot.node(), Box::new(move || poll_once(&*foot)));
            by_node
        }

        fn runs(&self) -> (usize, usize, usize) {
            (
                *self.left.stage().runs.lock().unwrap(),
                *self.right.stage().runs.lock().unwrap(),
                *self.foot.stage().runs.lock().unwrap(),
            )
        }
    }

    fn poll_once<S: Stage<Input = Text>>(stage: &S) {
        let _ = stage.poll_stage(&Text("x".to_string()), &mut Context::from_waker(Waker::noop()));
    }

    fn labels(ledger: &Ledger, nodes: &[NodeId]) -> Vec<&'static str> {
        nodes.iter().map(|n| ledger.label(*n)).collect()
    }

    fn schedule(ledger: &Ledger) -> Schedule {
        ledger.schedule().expect("the graph is acyclic")
    }

    // --------------------------------------------------------------------- gate

    #[test]
    fn a_diamond_schedules_its_shared_consumer_once_and_last() {
        let d = Diamond::new();
        poll_once(&*d.foot);
        assert!(schedule(&d.ledger).is_empty(), "a poll is not a change");

        d.source.set("B".to_string());
        let schedule = schedule(&d.ledger);

        assert_eq!(
            labels(&d.ledger, schedule.order()),
            ["left", "right", "foot"],
            "each stale node once, dependencies before dependents",
        );
        assert_eq!(
            labels(&d.ledger, schedule.to_poll()),
            ["foot"],
            "the middles have a stale reader, so the driver does not poll them - \
             the pull does",
        );
    }

    #[test]
    fn polling_the_schedule_runs_each_node_once() {
        let d = Diamond::new();
        poll_once(&*d.foot);
        assert_eq!(d.runs(), (1, 1, 1));

        d.source.set("B".to_string());
        let dispatcher = d.dispatcher();
        for node in schedule(&d.ledger).to_poll() {
            dispatcher[node]();
        }

        assert_eq!(
            d.runs(),
            (2, 2, 2),
            "one re-run each: the foot was polled, and it pulled the two middles",
        );
        assert!(
            schedule(&d.ledger).is_empty(),
            "and the stale set is empty afterwards, so the pass is finished",
        );
    }

    #[test]
    fn polling_every_stale_node_instead_runs_the_middles_twice() {
        // The known-bad input: the same graph and the same change with the schedule
        // ignored. It is not WRONG - the answers are identical, which is the point
        // - it is the work the schedule exists to remove.
        let d = Diamond::new();
        poll_once(&*d.foot);
        d.source.set("B".to_string());

        let dispatcher = d.dispatcher();
        for node in d.ledger.stale_nodes() {
            dispatcher[&node]();
        }

        assert_eq!(
            d.runs(),
            (3, 3, 2),
            "each middle ran on its own poll AND again when the foot pulled it",
        );
    }

    #[test]
    fn an_unchanged_input_schedules_nothing_and_nothing_re_runs() {
        let d = Diamond::new();
        poll_once(&*d.foot);
        let before = d.runs();

        assert!(
            !d.source.set("A".to_string()),
            "the value did not move, so this is not a change",
        );

        let schedule = schedule(&d.ledger);
        assert!(schedule.is_empty());
        assert!(schedule.to_poll().is_empty());

        let dispatcher = d.dispatcher();
        for node in schedule.to_poll() {
            dispatcher[node]();
        }
        assert_eq!(d.runs(), before, "a driver following the schedule did nothing");
    }

    #[test]
    fn only_the_graph_that_read_the_changed_input_is_scheduled() {
        // Two independent diamonds over one ledger, which is the IDE's shape: many
        // panes, one change. The schedule is what keeps the other panes from being
        // re-polled - the stale set is per node, not per ledger.
        let first = Diamond::new();
        let second = Diamond::new();
        poll_once(&*first.foot);
        poll_once(&*second.foot);

        first.source.set("B".to_string());

        assert_eq!(labels(&first.ledger, schedule(&first.ledger).to_poll()), ["foot"]);
        assert!(
            schedule(&second.ledger).is_empty(),
            "the second graph read a different input and is untouched",
        );
    }

    #[test]
    fn a_pending_node_stays_in_the_schedule_after_it_is_polled() {
        // A node that answered Pending has produced no value, so the pass is not
        // finished with it. This is the scheduling half of Tracked's re-mark: a
        // driver that took the poll as revalidation would drop the node and the
        // value would be lost rather than late.
        struct Parks;
        impl Stage for Parks {
            type Input = Text;
            type Output = String;
            type Error = &'static str;
            fn id(&self) -> StageId {
                StageId::new("test.parks", 1)
            }
            fn memo_key(&self, _input: &Text) -> Option<MemoKey> {
                None
            }
            fn poll_stage(
                &self,
                _input: &Text,
                cx: &mut Context<'_>,
            ) -> EffectPoll<String, &'static str> {
                let _ = cx.waker().clone();
                EffectPoll::Pending
            }
        }

        let ledger = Ledger::new();
        let parks = Tracked::new(&ledger, "parks", Parks);
        poll_once(&parks);

        assert_eq!(
            labels(&ledger, schedule(&ledger).to_poll()),
            ["parks"],
            "polled, still pending, still work",
        );
    }

    #[test]
    fn a_cycle_in_the_stale_set_is_reported_rather_than_dropped() {
        // The termination argument is the DAG's acyclicity, so a cycle is a graph
        // bug. The ledger records what it observes and cannot refuse one, which is
        // why the scheduler has to answer for it: dropping the nodes would lose the
        // work silently and walking the cycle would not stop.
        let ledger = Ledger::new();
        let each_reads_the_other = (ledger.node("x"), ledger.node("y"));
        let (x, y) = each_reads_the_other;
        ledger.run(x, || ledger.observe_read(y));
        ledger.run(y, || ledger.observe_read(x));
        ledger.mark_stale(x);
        ledger.mark_stale(y);

        let cycle = ledger.schedule().expect_err("x and y read each other");
        assert_eq!(labels(&ledger, cycle.nodes()), ["x", "y"]);
    }

    #[test]
    fn a_node_whose_only_stale_reader_is_itself_out_of_the_set_is_polled_directly() {
        // The middles become tops as soon as nothing stale reads them. Without this
        // the schedule would answer "poll nothing" while work remained - the
        // failure mode that is invisible until a value never updates.
        let d = Diamond::new();
        poll_once(&*d.foot);
        d.source.set("B".to_string());
        d.ledger.clear_stale(d.foot.node());

        assert_eq!(
            labels(&d.ledger, schedule(&d.ledger).to_poll()),
            ["left", "right"],
        );
    }
}
