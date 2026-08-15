//! Scheduling: what a driver polls next, given the stale set
//! (`PIPELINE_PLAN.md` §6:484-485).
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
//! is the stale nodes with no stale reader. On §3's diamond that set is one
//! node, and the shared consumer runs once instead of once per stale path.

use std::collections::{BTreeMap, BTreeSet};

use crate::{Ledger, NodeId};

/// What to poll, and in what order, to revalidate everything currently stale.
///
/// **A snapshot.** It is computed from the ledger's stale set and edges as they
/// are, and a change landing afterwards is not in it. That is the right
/// lifetime for the thing that consumes it: a frame loop takes one per frame, a
/// CLI run takes one per pass.
///
/// **Ids, not work.** A `Schedule` says which NODES; mapping a node to the
/// typed stage that polls it is the caller's, because the engine is generic
/// over every expression type and the stages of one graph do not share an
/// `Input` or `Output` (`PIPELINE_PLAN.md`:558-568). A registry of type-erased
/// closures could live here, but it would be the caller's map with the types
/// thrown away rather than something the engine knows.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Schedule {
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
    pub fn to_poll(&self) -> &[NodeId] {
        &self.to_poll
    }

    /// Every stale node, dependencies before dependents, each appearing once.
    ///
    /// The order a driver would use if it revalidated bottom-up rather than by
    /// pulling from the top - and the order in which a node's inputs are known
    /// fresh, which is what a memo needs to hit rather than recompute.
    pub fn order(&self) -> &[NodeId] {
        &self.order
    }

    /// Nothing is stale.
    pub fn is_empty(&self) -> bool {
        self.order.is_empty()
    }
}

/// The stale set could not be ordered: some of it reads itself, directly or
/// through others.
///
/// §5's termination argument is "the DAG's acyclicity plus effect completion",
/// so a cycle is a graph bug rather than a case to schedule around - and it is
/// reported rather than dropped or looped on, because the two silent answers
/// are worse than the loud one: dropping the nodes loses the work, and walking
/// the cycle would not stop.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Cycle {
    nodes: Vec<NodeId>,
}

impl Cycle {
    /// The stale nodes that could not be ordered - the cycle and anything
    /// downstream of it.
    pub fn nodes(&self) -> &[NodeId] {
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
    pub fn schedule(&self) -> Result<Schedule, Cycle> {
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
