//! The read-observation ledger (`PIPELINE_PLAN.md` §3:225-230).
//!
//! §3's rule, verbatim: "edges are **recorded by observing reads, not
//! declared** - the salsa / MobX / Vue / Solid mechanism: while a stage runs,
//! every keyed read is logged as an edge; the set is re-logged on every run so
//! it follows conditionals (a branch not taken this run contributes no edge, so
//! a change behind it wakes nothing). Declared dependency lists are what this
//! design explicitly does not have."
//!
//! Three pieces implement exactly that sentence and nothing more:
//!
//! * [`Ledger`] - the ledger itself: who is running, what they read, and the
//!   reverse index that invalidation walks.
//! * [`Tracked`] - a [`Stage`] wrapper that opens a run scope around a poll, so
//!   "while a stage runs" has a beginning and an end the ledger can see.
//! * [`TrackedInput`] - a value whose reads are observable. Reading one inside
//!   a run scope is what logs an edge.
//!
//! **The engine still names no IR** (`PIPELINE_PLAN.md`:558-568). A node is an
//! opaque [`NodeId`] and a tracked value is an opaque `T`; nothing here matches
//! on an expression type, because none is in scope.

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::Context;

use libpipelinedata::{EffectPoll, MemoKey, Stage, StageId};

/// Distinguishes one [`Ledger`] from another, so a [`NodeId`] minted by one is
/// refused by the other rather than silently addressing a different node.
static LEDGERS: AtomicU64 = AtomicU64::new(0);

/// A node in the dependency graph: a tracked input, or a stage that reads them.
///
/// **One id space for both, deliberately.** Invalidation is transitive - a
/// changed input marks its readers stale, and their readers, and so on - so a
/// derived node has to be addressable as something that was READ, exactly as an
/// input is. Two id spaces would need a rule for crossing between them at every
/// step of that walk. salsa makes the same choice (inputs are leaf queries).
///
/// The `ledger` half is not decoration: an id used against the wrong ledger
/// would otherwise index a real but unrelated node, and mistracking is silent
/// by nature. It is checked on every call that takes one.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct NodeId {
    ledger: u64,
    index: u32,
}

impl NodeId {
    /// The minting ledger's identity. Only useful for diagnostics.
    pub fn ledger(self) -> u64 {
        self.ledger
    }
}

/// What a stage read while it ran, and who reads whom
/// (`PIPELINE_PLAN.md` §3:225-230).
///
/// **Reads are observed, never declared.** Nothing here accepts a dependency
/// list. An edge exists because [`TrackedInput::get`] (or a poll of a
/// [`Tracked`] stage) happened while a run scope was open, which is why the set
/// follows a conditional for free: the branch not taken this run did not read,
/// so it logged nothing.
///
/// **Safe interior mutability** (`CLAUDE.md`): one `Mutex` behind `&self`,
/// because reads are observed DURING a poll and a poll holds `&self` all the
/// way down - the same reason `MemoStore` takes `&self` on its recording side.
/// No `UnsafeCell`, and no lock is ever held across the stage's own poll.
///
/// **Single-threaded per drive.** The running-node stack is per-ledger, so two
/// threads polling different graphs through ONE ledger would interleave their
/// scopes and attribute reads to each other. §5's drivers are each one loop - a
/// frame loop and a CLI loop - so the shape that is supported is one ledger per
/// drive. Sharing one across concurrent drives is a misuse this type does not
/// try to detect.
#[derive(Debug)]
pub struct Ledger {
    id: u64,
    inner: Mutex<Inner>,
}

#[derive(Debug, Default)]
struct Inner {
    /// Per node, in mint order. `labels.len()` is the node count.
    labels: Vec<&'static str>,
    /// The open run scopes, innermost last: `(node, reads observed so far)`.
    running: Vec<(u32, BTreeSet<u32>)>,
    /// Per node: what it read on its last run that read anything.
    reads: Vec<BTreeSet<u32>>,
    /// The reverse index of `reads`, maintained in step with it. Invalidation
    /// walks this one, so it is not a convenience - it is what makes marking
    /// dependents cheap rather than a scan of every node's read set.
    readers: Vec<BTreeSet<u32>>,
}

impl Ledger {
    /// A fresh ledger with no nodes.
    ///
    /// `Arc`-wrapped because every [`Tracked`] stage and [`TrackedInput`] holds
    /// one and they outlive any single poll.
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            id: LEDGERS.fetch_add(1, Ordering::Relaxed),
            inner: Mutex::new(Inner::default()),
        })
    }

    /// Mint a node id. `label` is for diagnostics only - two nodes may share
    /// one, and nothing is keyed by it.
    pub fn node(&self, label: &'static str) -> NodeId {
        let mut inner = self.lock();
        let index = u32::try_from(inner.labels.len()).expect("a ledger holds fewer than 4B nodes");
        inner.labels.push(label);
        inner.reads.push(BTreeSet::new());
        inner.readers.push(BTreeSet::new());
        NodeId {
            ledger: self.id,
            index,
        }
    }

    /// The label `node` was minted with.
    pub fn label(&self, node: NodeId) -> &'static str {
        let inner = self.lock();
        inner.labels[self.check(node)]
    }

    /// The innermost node currently running, if any.
    ///
    /// A read observed with nothing running belongs to nobody - see
    /// [`observe_read`](Self::observe_read).
    pub fn running(&self) -> Option<NodeId> {
        let inner = self.lock();
        inner.running.last().map(|(node, _)| NodeId {
            ledger: self.id,
            index: *node,
        })
    }

    /// Log a read of `node` by whatever is running.
    ///
    /// **This is the whole mechanism.** The caller says only "I was read"; who
    /// depended on that is the ledger's answer, taken from the run scope that
    /// happens to be open. That is what makes the edge OBSERVED rather than
    /// declared: the reader is never named at the read site.
    ///
    /// A read with no scope open records nothing. That is the honest answer
    /// rather than an error - a driver, a test or a UI inspector may read a
    /// tracked value outside any stage, and nothing depends on that read.
    pub fn observe_read(&self, node: NodeId) {
        let index = self.check(node);
        let mut inner = self.lock();
        let index = u32::try_from(index).expect("checked above");
        if let Some((_, scratch)) = inner.running.last_mut() {
            scratch.insert(index);
        }
    }

    /// Run `f` as `node`, observing every read it makes.
    ///
    /// **What is committed, and when.** Reads land in a scratch set for the
    /// duration and replace the node's recorded set when the scope closes -
    /// "the set is re-logged on every run" (§3). Two deliberate exceptions:
    ///
    /// * **A scope that observed NO reads leaves the previous set standing.**
    ///   The ledger cannot distinguish "ran and read nothing" from "returned a
    ///   memoized value without running", and those want opposite treatment: the
    ///   first should clear the edges, the second must keep them or the node
    ///   would never be invalidated again. Keeping them costs an extra poll for
    ///   a node whose reads legitimately drop to zero; clearing them would cost
    ///   a value that is never recomputed. The cheap error is the one taken.
    /// * **A scope unwound by a panic commits nothing.** A partial read set is
    ///   a set with edges MISSING, which is the unsafe direction.
    pub fn run<R>(&self, node: NodeId, f: impl FnOnce() -> R) -> R {
        let index = u32::try_from(self.check(node)).expect("checked above");
        self.lock().running.push((index, BTreeSet::new()));
        let scope = Scope {
            ledger: self,
            index,
        };
        let out = f();
        drop(scope);
        out
    }

    /// What `node` read on its last run that read anything, in mint order.
    pub fn reads_of(&self, node: NodeId) -> Vec<NodeId> {
        let index = self.check(node);
        let inner = self.lock();
        self.ids(inner.reads[index].iter().copied())
    }

    /// Who read `node` on their last run that read anything, in mint order.
    /// This is the edge invalidation walks.
    pub fn readers_of(&self, node: NodeId) -> Vec<NodeId> {
        let index = self.check(node);
        let inner = self.lock();
        self.ids(inner.readers[index].iter().copied())
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The index `node` addresses, refusing an id from another ledger.
    ///
    /// A panic rather than a silent no-op: mistracking produces wrong answers
    /// far from its cause, and this is a wiring mistake, not a runtime
    /// condition - it is wrong on the first call or never.
    fn check(&self, node: NodeId) -> usize {
        assert_eq!(
            node.ledger, self.id,
            "a NodeId minted by ledger {} was used against ledger {}",
            node.ledger, self.id,
        );
        node.index as usize
    }

    fn ids(&self, indices: impl Iterator<Item = u32>) -> Vec<NodeId> {
        indices
            .map(|index| NodeId {
                ledger: self.id,
                index,
            })
            .collect()
    }
}

/// Closes a run scope even if the stage's poll panics. See [`Ledger::run`].
struct Scope<'a> {
    ledger: &'a Ledger,
    index: u32,
}

impl Drop for Scope<'_> {
    fn drop(&mut self) {
        let mut inner = self.ledger.lock();
        let Some((index, scratch)) = inner.running.pop() else {
            return;
        };
        debug_assert_eq!(index, self.index, "run scopes closed out of order");
        if scratch.is_empty() || std::thread::panicking() {
            return;
        }
        let node = index as usize;
        for was in std::mem::take(&mut inner.reads[node]) {
            inner.readers[was as usize].remove(&index);
        }
        for now in &scratch {
            inner.readers[*now as usize].insert(index);
        }
        inner.reads[node] = scratch;
    }
}

/// A stage whose reads are observed (`PIPELINE_PLAN.md` §3:225-230).
///
/// **Transparent by construction.** Id and memo key delegate, exactly as
/// [`Memo`](crate::Memo)'s do, for the same reason: tracking must not change
/// what anything is keyed by, or it would be part of the semantics rather than
/// an observation of them.
///
/// **Two things happen per poll, in this order and not the other.**
///
/// 1. The node records that it was READ, so a stage polling this one from
///    inside its own scope gets the edge - whether or not this poll does any
///    work. A memo hit must still tell its consumer who it depends on.
/// 2. The node opens its run scope, so everything the inner stage reads is
///    attributed here.
///
/// **What it does not do is register a waker.** That is the difference this
/// layer makes: a `TrackedInput` change reaches the driver because the read was
/// OBSERVED, not because the stage remembered to stash the waker it was handed
/// (`PIPELINE_PLAN.md` §3, and the step-1 finding pinned by
/// `two_drivers_one_graph.rs`'s
/// `a_pending_stage_that_registers_no_waker_is_a_value_lost_rather_than_late`).
pub struct Tracked<S> {
    stage: S,
    ledger: Arc<Ledger>,
    node: NodeId,
}

impl<S> Tracked<S> {
    /// Wrap `stage` as a node of `ledger`, minting its id.
    pub fn new(ledger: &Arc<Ledger>, label: &'static str, stage: S) -> Self {
        let node = ledger.node(label);
        Self {
            stage,
            ledger: Arc::clone(ledger),
            node,
        }
    }

    /// This stage's node.
    pub fn node(&self) -> NodeId {
        self.node
    }

    /// The stage behind the tracking.
    pub fn stage(&self) -> &S {
        &self.stage
    }

    /// The ledger this node belongs to.
    pub fn ledger(&self) -> &Arc<Ledger> {
        &self.ledger
    }
}

impl<S: Stage> Stage for Tracked<S> {
    type Input = S::Input;
    type Output = S::Output;
    type Error = S::Error;

    fn id(&self) -> StageId {
        self.stage.id()
    }

    fn memo_key(&self, input: &Self::Input) -> Option<MemoKey> {
        self.stage.memo_key(input)
    }

    fn poll_stage(
        &self,
        input: &Self::Input,
        cx: &mut Context<'_>,
    ) -> EffectPoll<Self::Output, Self::Error> {
        self.ledger.observe_read(self.node);
        self.ledger
            .run(self.node, || self.stage.poll_stage(input, cx))
    }
}

/// A value whose reads are observable - the leaf of the dependency graph.
///
/// **`get` is the read that logs the edge**, which is why it exists at all
/// rather than the value being handed to a stage as an argument: an argument is
/// a declared dependency, and §3's design "explicitly does not have" those. A
/// stage holds the input and reads it when it needs it, including not at all on
/// a run that takes the other branch.
///
/// `T: Clone` because [`get`](Self::get) hands back an owned value - a read
/// that returned a borrow would tie the stage's poll to this type's lock, which
/// is the same argument `MemoStore::lookup`'s doc makes.
pub struct TrackedInput<T> {
    ledger: Arc<Ledger>,
    node: NodeId,
    value: Mutex<T>,
}

impl<T> TrackedInput<T> {
    /// A tracked value, holding `value`, as a node of `ledger`.
    pub fn new(ledger: &Arc<Ledger>, label: &'static str, value: T) -> Self {
        let node = ledger.node(label);
        Self {
            ledger: Arc::clone(ledger),
            node,
            value: Mutex::new(value),
        }
    }

    /// This value's node.
    pub fn node(&self) -> NodeId {
        self.node
    }

    /// The ledger this value belongs to.
    pub fn ledger(&self) -> &Arc<Ledger> {
        &self.ledger
    }
}

impl<T: Clone> TrackedInput<T> {
    /// Read the value, logging the edge from whatever is running.
    pub fn get(&self) -> T {
        self.ledger.observe_read(self.node);
        self.value
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Read the value WITHOUT logging anything.
    ///
    /// For a caller that is not a stage - a driver deciding what to draw, a
    /// test asserting what a value is. Named so that using it inside a stage
    /// reads as the mistake it would be: a stage that peeks is a stage whose
    /// dependency is invisible, and §3's tracking cannot see what did not
    /// announce itself.
    pub fn peek(&self) -> T {
        self.value
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}
