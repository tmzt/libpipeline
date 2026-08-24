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
//!   a run scope is what logs an edge, and changing one marks every node that
//!   read it - transitively - stale.
//!
//! **The engine still names no IR** (`PIPELINE_PLAN.md`:579-583). A node is an
//! opaque [`NodeId`] and a tracked value is an opaque `T`; nothing here matches
//! on an expression type, because none is in scope.

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


use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::{Context, Waker};

use libpipelinedata::{ContentHash, ContentKey, EffectPoll, MemoKey, Stage, StageId};

/// Distinguishes one [`Ledger`] from another, so a [`NodeId`] minted by one is
/// refused by the other rather than silently addressing a different node.
static LEDGERS: AtomicU64 = AtomicU64::new(0);

thread_local! {
    /// Per open run scope on this thread, innermost last: whether the node was
    /// STALE when its scope opened. See [`revalidating`].
    ///
    /// **Per thread rather than per ledger, because the consumer has no
    /// ledger.** A cache layer sits INSIDE a stage's poll and is handed nothing
    /// but the input and a [`Context`]; a run scope is exactly the dynamic
    /// extent of that poll, so the scope stack is the only channel that reaches
    /// it without the author wiring one - and a wiring the author must remember
    /// is the thing being fixed, not a fix. It follows the ledger's stated
    /// shape (one ledger per drive, one drive per loop) and is strictly
    /// narrower than the ledger's own running stack: two threads polling
    /// through one ledger already interleave their scopes there, and their
    /// revalidation flags do not.
    static REVALIDATING: RefCell<Vec<bool>> = const { RefCell::new(Vec::new()) };
}

/// Whether the innermost open run scope is REVALIDATING: its node was stale
/// when [`Ledger::run`] opened the scope, so this poll exists to replace a
/// value the ledger has already ruled out.
///
/// **This is what stops a cache from contradicting the ledger** - the defect
/// pinned by `invalidation_marks_dependents.rs`'s
/// `a_memo_over_a_tracked_read_cannot_serve_what_the_ledger_ruled_stale`.
/// [`Stage::memo_key`] is built from the stage's INPUT argument, so a stage
/// that also reads tracked state has an ambient input the key cannot see, and a
/// cache keyed on that alone will serve a value the ledger knows is stale.
/// [`Memo`](crate::memo::Memo) consults this and does not answer from its store while
/// it is true; any other cache layer must do the same, which is why this is
/// public rather than private to that type.
///
/// **Why observation rather than declaration.** The other available fix is for
/// the stage to fold [`Ledger::revision`] of everything it reads into its own
/// key. That works and is exact, and it is a DECLARED dependency list - the one
/// thing §3 says this design explicitly does not have, and it fails the way
/// declarations fail: silently, when someone forgets. The ambient inputs are
/// already observed; the correction belongs on the same channel as the
/// observation.
///
/// **False when no scope is open**, which is the honest answer: with no run
/// scope there is no tracked node, nothing was ever marked stale on its behalf,
/// and a cache has nothing to defer to. That is what keeps a pipeline with no
/// tracking in it behaving exactly as before.
pub(crate) fn revalidating() -> bool {
    REVALIDATING.with(|stack| stack.borrow().last().copied().unwrap_or(false))
}

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
pub(crate) struct NodeId {
    ledger: u64,
    index: u32,
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
pub(crate) struct Ledger {
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
    /// Per node: how many times it has been declared changed. See
    /// [`Ledger::revision`].
    revisions: Vec<u64>,
    /// Who has been marked stale and not yet revalidated, and WHY.
    ///
    /// A node is stale exactly when it has an entry here, and the entry is
    /// never empty. The reasons are what makes staleness RETRACTABLE, which is
    /// the whole of [`Ledger::unchanged`]: a bare set could record that a node
    /// must re-run and could not answer "does it still have to, now that this
    /// dependency turned out not to have moved?" - and clearing on "none of my
    /// reads is stale" is not that answer, since a changed INPUT is never
    /// itself stale.
    stale: BTreeMap<u32, BTreeSet<Reason>>,
    /// Whom to tell that something went stale. Not drained by waking - see
    /// [`Ledger::subscribe`].
    subscribers: Vec<Waker>,
}

/// Why a node is stale (see [`Inner::stale`]).
///
/// Two kinds, because only one of them can be taken back.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
enum Reason {
    /// A change reached this node through the named dependency. Retractable:
    /// if that dependency recomputes to the same value it stops being a reason
    /// ([`Ledger::unchanged`]).
    Read(u32),
    /// The node itself owes a value - it answered `Pending` or `Failed`, or a
    /// caller marked it directly ([`Ledger::mark_stale`]). Nothing a dependency
    /// does retracts this; only running the node to a value clears it.
    Owed,
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
        inner.revisions.push(0);
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
    /// **Entering the scope clears the node's staleness**, before `f` runs and
    /// not after. That is [`WakeFlag::take_stale`](libeffects::WakeFlag)'s
    /// discipline for the same reason: a change that lands DURING the run must
    /// leave the node stale, and clearing afterwards would swallow it. A run
    /// that ends up answering `Pending` has not produced the new value and is
    /// marked again by [`Tracked`], which is where the answer is visible.
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
    ///
    /// **The staleness the entry clears is not merely discarded - it is carried
    /// for the duration of the scope**, where [`revalidating`] answers it. The
    /// clear alone would leave a cache inside `f` unable to tell a poll that
    /// exists to REPLACE a ruled-out value from a poll of a node nothing has
    /// touched, and those want opposite answers from a store.
    pub fn run<R>(&self, node: NodeId, f: impl FnOnce() -> R) -> R {
        let index = u32::try_from(self.check(node)).expect("checked above");
        let entered_stale = {
            let mut inner = self.lock();
            // Every reason at once, retractable or not: the node is about to
            // produce a value, which is what all of them were waiting for.
            let was_stale = inner.stale.remove(&index).is_some();
            inner.running.push((index, BTreeSet::new()));
            was_stale
        };
        REVALIDATING.with(|stack| stack.borrow_mut().push(entered_stale));
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

    /// Declare that `node`'s value has changed: bump its revision, mark every
    /// node that read it stale - transitively - and tell the subscribers.
    ///
    /// Returns how many nodes were marked.
    ///
    /// **The changed node is not itself marked.** Staleness means "revalidate
    /// this by running it"; an input has nothing to run, and a derived node
    /// whose own value someone declares changed has just produced it. Marking
    /// the source would put a node in the stale set that no poll can clear.
    ///
    /// **The walk is over the reverse index and it is complete**, not stopped
    /// by nodes that were already stale. Stopping there would be an easy
    /// dedupe and a wrong one: a node can be stale while a node that reads it
    /// has since been revalidated, and the second change has to reach that far
    /// again. Visiting each node once is what bounds the walk - the same
    /// property, taken from the right place.
    ///
    /// **Each node is marked with the dependency the change reached it
    /// through**, and a node reached along two edges is marked twice. That is
    /// not bookkeeping for its own sake: it is what [`unchanged`](Self::unchanged)
    /// retracts, one edge at a time. The dedupe above is therefore on the
    /// EXPANSION only - a node already visited still records the new reason.
    ///
    /// **Subscribers are woken only if something was marked.** A change nobody
    /// read wakes nothing, which is §3's conditional clause seen from the other
    /// end: "a branch not taken this run contributes no edge, so a change
    /// behind it wakes nothing".
    pub fn changed(&self, node: NodeId) -> usize {
        let index = u32::try_from(self.check(node)).expect("checked above");
        let mut inner = self.lock();
        inner.revisions[index as usize] += 1;

        let mut visited = BTreeSet::new();
        // `(node, the dependency the change reached it through)` - the reason,
        // carried along the walk so it can be retracted one edge at a time.
        let mut queue: VecDeque<(u32, u32)> = inner.readers[index as usize]
            .iter()
            .map(|reader| (*reader, index))
            .collect();
        let mut marked = 0;
        while let Some((next, through)) = queue.pop_front() {
            // The reason is recorded on every arrival, including at a node
            // already visited: a node reached through two dependencies is stale
            // for two reasons and needs both retracted. Only the EXPANSION is
            // deduped, which is what bounds the walk.
            let reasons = inner.stale.entry(next).or_default();
            let was_stale = !reasons.is_empty();
            reasons.insert(Reason::Read(through));
            if !was_stale {
                marked += 1;
            }
            if !visited.insert(next) {
                continue;
            }
            queue.extend(
                inner.readers[next as usize]
                    .iter()
                    .map(|reader| (*reader, next)),
            );
        }

        if !visited.is_empty() {
            let wakers = inner.subscribers.clone();
            drop(inner);
            for waker in wakers {
                waker.wake();
            }
        }
        marked
    }

    /// Declare that `node` has just recomputed to the value it already had, so
    /// its readers may stop counting it as a reason to be stale - §3's
    /// **backdating**, one level above the leaf.
    ///
    /// Returns how many nodes stopped being stale.
    ///
    /// **This is the half [`TrackedInput::set`] could not reach.** The leaf
    /// cutoff is exact and costs one comparison, and it does nothing for a
    /// DERIVED node: a stage whose output ignores part of its input - a
    /// formatter fed a re-indented file, a lowering fed a renamed local - would
    /// recompute the same answer and still make every consumer re-run.
    /// §3: "without cutoff every keystroke invalidates the whole pipeline".
    ///
    /// **The retraction is per edge, and that is why staleness carries
    /// reasons.** A reader stops being stale only when EVERY dependency whose
    /// change reached it has been retracted; a reader waiting on two changed
    /// paths is still waiting after one of them cuts off. When a reader does go
    /// fresh the retraction continues past it - it was itself a reason for ITS
    /// readers, and it has now produced nothing new for them either.
    ///
    /// **A node that owes a value of its own is not retracted.** A `Pending`
    /// poll marks its node through [`mark_stale`](Self::mark_stale), and no
    /// dependency's equality answers for a value that was never produced.
    ///
    /// **The wake cannot be taken back, and should not be.** Discovering the
    /// equality required running `node`, which required something to have woken
    /// the driver that polled it. What backdating saves is the WORK above that
    /// node, which is where the pipeline's cost is: one stage re-ran and its
    /// consumers did not.
    ///
    /// It is deliberately the caller's to declare rather than something the
    /// ledger detects, for the reason it cannot: the ledger holds `NodeId`s and
    /// never sees a value. [`Backdated`] is the wrapper that does see one.
    pub fn unchanged(&self, node: NodeId) -> usize {
        let index = u32::try_from(self.check(node)).expect("checked above");
        let mut inner = self.lock();

        let mut cleared = 0;
        let mut queue: VecDeque<u32> = VecDeque::from([index]);
        while let Some(next) = queue.pop_front() {
            for reader in inner.readers[next as usize].clone() {
                let Some(reasons) = inner.stale.get_mut(&reader) else {
                    continue;
                };
                reasons.remove(&Reason::Read(next));
                if reasons.is_empty() {
                    inner.stale.remove(&reader);
                    cleared += 1;
                    // This reader produced nothing new either, so it is no
                    // longer a reason for the nodes that read IT.
                    queue.push_back(reader);
                }
            }
        }
        cleared
    }

    /// How many times `node` has been declared changed, starting at 0.
    ///
    /// **A stage may fold this into its own key, and no longer has to.**
    /// `ContentKey`'s doc states the rule an ambient input must satisfy: "an
    /// ambient input either becomes a real input with a content key, or it
    /// moves the version. There is no third option that leaves the cache
    /// correct." A revision folded into [`Stage::memo_key`] is the first
    /// branch, spelled by the stage. It is exact, it lets a hit be served
    /// straight from the store, and it is a DECLARATION - so it is also
    /// forgettable, and forgetting it is silent.
    ///
    /// The second branch is the one the engine now takes on every stage's
    /// behalf, because it needs nothing declared: staleness IS the version, and
    /// [`revalidating`] carries it into the poll where a cache can defer to it.
    /// So this is a sharpening a stage may buy - one that turns an extra run
    /// into a hit under a moved key - rather than the thing that stands between
    /// the cache and a wrong answer.
    ///
    /// Note the direction of the difference. Folding the revision distinguishes
    /// "this input moved" from "some input I read moved"; the ledger's stale bit
    /// is per NODE, so a node stale for any reason re-runs. The engine's rule is
    /// the conservative one, which is the same direction [`run`](Self::run)
    /// takes with a read set it cannot classify.
    pub fn revision(&self, node: NodeId) -> u64 {
        let index = self.check(node);
        self.lock().revisions[index]
    }

    /// Whether `node` has been marked stale and not yet revalidated.
    pub fn is_stale(&self, node: NodeId) -> bool {
        let index = u32::try_from(self.check(node)).expect("checked above");
        self.lock().stale.contains_key(&index)
    }

    /// Everything currently stale, in mint order.
    pub fn stale_nodes(&self) -> Vec<NodeId> {
        let inner = self.lock();
        self.ids(inner.stale.keys().copied())
    }

    /// Mark `node` alone stale, without walking to its readers.
    ///
    /// For a node that must be revalidated for a reason the ledger cannot
    /// observe - [`Tracked`] uses it to keep a `Pending` OR `Failed` poll
    /// stale, since a poll that did not produce a value has not revalidated
    /// anything, whichever way it declined to.
    ///
    /// The mark is the node's OWN, not a dependency's, so no amount of
    /// backdating below it takes it back. See [`unchanged`](Self::unchanged).
    pub fn mark_stale(&self, node: NodeId) {
        let index = u32::try_from(self.check(node)).expect("checked above");
        self.lock().stale.entry(index).or_default().insert(Reason::Owed);
    }

    /// Clear `node`'s staleness. [`run`](Self::run) does this on entry; this is
    /// for a caller revalidating a node some other way.
    pub fn clear_stale(&self, node: NodeId) {
        let index = u32::try_from(self.check(node)).expect("checked above");
        self.lock().stale.remove(&index);
    }

    /// Be told when something goes stale.
    ///
    /// **This is the layer's payoff.** A driver that subscribes is woken
    /// because a read was OBSERVED, not because the stage remembered to stash
    /// the waker it was handed - which is the defect class step 1 measured
    /// (`two_drivers_one_graph.rs`'s
    /// `a_pending_stage_that_registers_no_waker_is_a_value_lost_rather_than_late`).
    /// A stage that reads a [`TrackedInput`] cannot forget, because it never
    /// had to remember.
    ///
    /// **Subscription is not one-shot** and waking does not drain it: §3's wake
    /// means "stale, poll again", not "the thing you waited for has arrived".
    /// A frame loop subscribes once and stays subscribed - and may re-subscribe
    /// every frame without accumulating, because a waker that would wake the
    /// same target as one already held is not added twice.
    pub fn subscribe(&self, waker: Waker) {
        let mut inner = self.lock();
        if inner.subscribers.iter().any(|held| held.will_wake(&waker)) {
            return;
        }
        inner.subscribers.push(waker);
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
        // Popped first and unconditionally: every path out of this function
        // below is a `return`, and a revalidation flag left on the stack would
        // outlive its poll and be read by the NEXT one.
        REVALIDATING.with(|stack| {
            stack.borrow_mut().pop();
        });
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
/// [`Memo`](crate::memo::Memo)'s do, for the same reason: tracking must not change
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
pub(crate) struct Tracked<S> {
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
        let polled = self
            .ledger
            .run(self.node, || self.stage.poll_stage(input, cx));
        if !matches!(polled, EffectPoll::Ready(_)) {
            // The scope cleared this node's staleness on the way in, which is
            // right for a poll that produced a value and wrong for one that did
            // not: a `Pending` stage has not revalidated anything, and a
            // scheduler that took the clear at face value would drop it from
            // the work it has left.
            //
            // **`Failed` is the same case and used to be missed.** A failure
            // produces no value either, and the state was unreachable until §7:
            // a failure ended the drive, so what the ledger thought about the
            // failed node afterwards did not matter. An error boundary is what
            // lets the drive continue past one - and then a node that has never
            // produced a value was reported valid by
            // [`is_stale`](Ledger::is_stale), dropped from
            // [`schedule`](Ledger::schedule), and never polled again by anything
            // that asks the ledger what to poll.
            // `a_fallback_is_not_a_revalidation.rs` is that measurement.
            self.ledger.mark_stale(self.node);
        }
        polled
    }
}

/// A [`Tracked`] node whose consumers are spared when its output does not move
/// - §3's **early cutoff**, above the leaf.
///
/// **What it adds to `Tracked`, and only that.** It IS a `Tracked` inside, so
/// the reads, the scope, the `Pending` re-mark and the delegated id and memo key
/// are the same ones; the addition is one comparison after a `Ready` poll. If
/// the output addresses to what it addressed to last time, the node calls
/// [`Ledger::unchanged`] and its consumers stop being stale.
///
/// **Why `ContentHash` and not `PartialEq`.** §3's equality for this purpose is
/// the content address (§9's step 2), and it is what a memo already trusts
/// "INSTEAD of comparing the value"
/// ([`ContentAddressHasher`](libpipelinedata::ContentAddressHasher)'s doc). It also
/// answers the storage half of the problem cheaply: what has to be kept between
/// polls is 128 bits, not the last output, so a node whose output is a whole
/// lowered tree does not double its footprint to get a cutoff. The cost is the
/// one that doc states - a collision here is a wrong answer, not a slow cache -
/// which is why the hasher is the seam it is and why this takes the address
/// rather than a `Hash`.
///
/// **The first poll is never a cutoff.** With nothing recorded there is no
/// equality to appeal to, and a node that has never run has consumers that have
/// never had its value.
///
/// **A `Pending` poll records nothing and retracts nothing.** There is no
/// output to address, and the node is stale on its OWN account after one -
/// which [`Ledger::unchanged`] does not touch.
///
/// **What this does not do is un-wake the driver**; see
/// [`Ledger::unchanged`]. The saving is the work above this node.
pub(crate) struct Backdated<S> {
    tracked: Tracked<S>,
    /// The address of the last `Ready` output. Safe interior mutability, for
    /// the reason the ledger's own lock is one: a poll holds `&self`.
    last: Mutex<Option<ContentKey>>,
}

impl<S> Backdated<S> {
    /// Wrap `stage` as a node of `ledger` that cuts off when its output repeats.
    pub fn new(ledger: &Arc<Ledger>, label: &'static str, stage: S) -> Self {
        Self {
            tracked: Tracked::new(ledger, label, stage),
            last: Mutex::new(None),
        }
    }

    /// This stage's node.
    pub fn node(&self) -> NodeId {
        self.tracked.node()
    }

    /// The stage behind the tracking.
    pub fn stage(&self) -> &S {
        self.tracked.stage()
    }

    /// The address of the last `Ready` output, if there has been one.
    pub fn last_address(&self) -> Option<ContentKey> {
        *self.last.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl<S: Stage> Stage for Backdated<S>
where
    S::Output: ContentHash,
{
    type Input = S::Input;
    type Output = S::Output;
    type Error = S::Error;

    fn id(&self) -> StageId {
        self.tracked.id()
    }

    fn memo_key(&self, input: &Self::Input) -> Option<MemoKey> {
        self.tracked.memo_key(input)
    }

    fn poll_stage(
        &self,
        input: &Self::Input,
        cx: &mut Context<'_>,
    ) -> EffectPoll<Self::Output, Self::Error> {
        let polled = self.tracked.poll_stage(input, cx);
        if let EffectPoll::Ready(value) = &polled {
            // After the scope has closed, deliberately: `unchanged` walks this
            // node's READERS, and doing it from inside the node's own run would
            // retract a reason from a consumer that is at that moment part-way
            // through the poll which pulled us.
            let address = ContentKey::of(value);
            let repeated = self
                .last
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .replace(address)
                == Some(address);
            if repeated {
                self.tracked.ledger().unchanged(self.tracked.node());
            }
        }
        polled
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
pub(crate) struct TrackedInput<T> {
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

impl<T: Clone + PartialEq> TrackedInput<T> {
    /// Store `value`, marking every node that read this one stale -
    /// transitively - and waking the ledger's subscribers.
    ///
    /// Returns whether the value moved.
    ///
    /// **A write of an equal value is not a change, and marks nothing.** This
    /// is §3's backdating ("early cutoff" in the build-systems literature) at
    /// the leaf, where it is exact and costs one comparison: "without cutoff
    /// every keystroke invalidates the whole pipeline". Taking it here does not
    /// pre-empt the harder half - a DERIVED value that recomputes to something
    /// equal still propagates, because nothing at this layer compares outputs -
    /// but it removes the case a live IDE hits constantly, an editor writing
    /// back a value the user did not actually change.
    ///
    /// `T: PartialEq` is therefore load-bearing rather than a convenience. A
    /// type that cannot be compared cannot have this cutoff, and the honest
    /// spelling for one would be a separate constructor, not a silent
    /// invalidation on every write.
    pub fn set(&self, value: T) -> bool {
        {
            let mut held = self.value.lock().unwrap_or_else(PoisonError::into_inner);
            if *held == value {
                return false;
            }
            *held = value;
        }
        self.ledger.changed(self.node);
        true
    }
}

#[cfg(test)]
mod invalidation_marks_dependents {
    //! **Moved in from `tests/invalidation_marks_dependents.rs`** at the
    //! visibility flip. It composes the tracked layer by hand
    //! (`FrameDriver`, `Ledger`, `Memo`, `NodeId`, `Tracked`, `TrackedInput`, `revalidating`), and the builder has no spelling for a tracked
    //! graph - `DESIGN.md`'s finding 1. A test in `tests/` proves the PUBLIC API
    //! can express something; a test in `src/` admits it cannot yet, and lives
    //! beside the code it pins so that a reshape of that code sees it. Every
    //! assertion is the one it arrived with; when finding 1 lands this migrates
    //! back out unchanged but for its imports.
    //!
    //! Gate: **a changed input marks its dependents stale, transitively - and
    //! nothing else** (`PIPELINE_PLAN.md` §3:225-230).
    //!
    //! The edges are the previous gate's subject (`reads_become_edges.rs`); this
    //! one is what they are FOR. Four clauses are pinned separately because they
    //! fail separately: who gets marked, how far the marking travels, what a write
    //! that changes nothing does, and how the mark reaches a driver.
    //!
    //! **The payoff clause is the last one.** Step 1 measured that staleness
    //! reaches a driver only because a stage registered the waker it was handed
    //! (`two_drivers_one_graph.rs`'s
    //! `a_pending_stage_that_registers_no_waker_is_a_value_lost_rather_than_late`).
    //! Here the stage registers nothing, holds no waker and has no idea a driver
    //! exists - and the frame loop still learns, because the READ was observed.
    //! Its known-bad twin is the same stage reading through
    //! [`TrackedInput::peek`](crate::track::TrackedInput::peek) instead of `get`:
    //! same value, same answer, no edge, and the frame loop is never told.
    //!
    //! **Every type here is a stand-in** (`PIPELINE_PLAN.md`:584-589).

    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Waker};

    use crate::driver::FrameDriver;
    use crate::track::Ledger;
    use crate::memo::Memo;
    use crate::track::NodeId;
    use crate::track::Tracked;
    use crate::track::TrackedInput;
    use crate::track::revalidating;
    use libpipelinedata::{ContentKey, EffectPoll, MemoKey, MemoStore, NoMemo, Stage, StageId};

    // ---------------------------------------------------------------- stand-ins

    /// Stand-in for whatever an author wrote.
    #[derive(Clone, PartialEq, Eq, Debug)]
    struct Text(String);

    /// A stage that reads one tracked input, observably or not.
    struct Reads {
        from: Arc<TrackedInput<String>>,
        /// When false the read goes through `peek`, which logs no edge. This is the
        /// gate's known-bad input: everything else about the stage is identical.
        observed: bool,
        runs: Mutex<usize>,
    }

    impl Reads {
        fn new(from: &Arc<TrackedInput<String>>, observed: bool) -> Self {
            Self {
                from: Arc::clone(from),
                observed,
                runs: Mutex::new(0),
            }
        }

        fn runs(&self) -> usize {
            *self.runs.lock().unwrap()
        }
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

        fn poll_stage(&self, input: &Text, _cx: &mut Context<'_>) -> EffectPoll<String, &'static str> {
            *self.runs.lock().unwrap() += 1;
            let read = if self.observed {
                self.from.get()
            } else {
                self.from.peek()
            };
            EffectPoll::Ready(format!("{}{read}", input.0))
        }
    }

    /// A stage that reads ONE of two inputs, chosen per run - §3's conditional,
    /// which is where "the set is re-logged on every run" earns its keep.
    struct ReadsEither {
        left: Arc<TrackedInput<String>>,
        right: Arc<TrackedInput<String>>,
        take_left: Mutex<bool>,
    }

    impl Stage for ReadsEither {
        type Input = Text;
        type Output = String;
        type Error = &'static str;

        fn id(&self) -> StageId {
            StageId::new("test.reads_either", 1)
        }

        fn memo_key(&self, _input: &Text) -> Option<MemoKey> {
            None
        }

        fn poll_stage(&self, _input: &Text, _cx: &mut Context<'_>) -> EffectPoll<String, &'static str> {
            let read = if *self.take_left.lock().unwrap() {
                self.left.get()
            } else {
                self.right.get()
            };
            EffectPoll::Ready(read)
        }
    }

    /// A stage that polls another and reads nothing itself - a middle of a chain.
    struct Relays<S> {
        inner: S,
    }

    impl<S: Stage<Input = Text, Output = String, Error = &'static str>> Stage for Relays<S> {
        type Input = Text;
        type Output = String;
        type Error = &'static str;

        fn id(&self) -> StageId {
            StageId::new("test.relays", 1)
        }

        fn memo_key(&self, _input: &Text) -> Option<MemoKey> {
            None
        }

        fn poll_stage(&self, input: &Text, cx: &mut Context<'_>) -> EffectPoll<String, &'static str> {
            self.inner.poll_stage(input, cx)
        }
    }

    /// A stage that never lands - §4's rows 10 and 11 in miniature, minus the
    /// upstream, because what this file needs from it is only the `Pending`.
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

        fn poll_stage(&self, _input: &Text, cx: &mut Context<'_>) -> EffectPoll<String, &'static str> {
            let _ = cx.waker().clone();
            EffectPoll::Pending
        }
    }

    /// A memoized stage that reads tracked state, with both spellings of "account
    /// for the ambient input" switchable: whether the read is OBSERVED (so the
    /// ledger can rule the node stale) and whether the stage DECLARES it by folding
    /// the revision into its own key.
    struct Composes {
        ledger: Arc<Ledger>,
        from: Arc<TrackedInput<String>>,
        /// When false the read goes through `peek` and logs no edge - the same
        /// known-bad input `Reads` carries, one word different.
        observes: bool,
        folds_revision: bool,
        runs: Mutex<usize>,
        /// What `revalidating()` said on each run, in order. The mechanism itself,
        /// read from inside the poll it governs.
        saw_revalidating: Mutex<Vec<bool>>,
    }

    impl Composes {
        const ID: StageId = StageId::new("test.composes", 1);

        fn new(
            ledger: &Arc<Ledger>,
            from: &Arc<TrackedInput<String>>,
            observes: bool,
            folds_revision: bool,
        ) -> Self {
            Self {
                ledger: Arc::clone(ledger),
                from: Arc::clone(from),
                observes,
                folds_revision,
                runs: Mutex::new(0),
                saw_revalidating: Mutex::new(Vec::new()),
            }
        }

        fn runs(&self) -> usize {
            *self.runs.lock().unwrap()
        }

        fn saw_revalidating(&self) -> Vec<bool> {
            self.saw_revalidating.lock().unwrap().clone()
        }
    }

    impl Stage for Composes {
        type Input = Text;
        type Output = String;
        type Error = &'static str;

        fn id(&self) -> StageId {
            Self::ID
        }

        fn memo_key(&self, input: &Text) -> Option<MemoKey> {
            let mut inputs = vec![content_key_of(&input.0)];
            if self.folds_revision {
                // ContentKey's doc: "an ambient input either becomes a real input
                // with a content key, or it moves the version." This is the first
                // branch, and the revision is what it costs.
                inputs.push(ContentKey::from_u128(u128::from(
                    self.ledger.revision(self.from.node()),
                )));
            }
            Some(MemoKey::new(Self::ID, inputs))
        }

        fn poll_stage(&self, input: &Text, _cx: &mut Context<'_>) -> EffectPoll<String, &'static str> {
            *self.runs.lock().unwrap() += 1;
            self.saw_revalidating.lock().unwrap().push(revalidating());
            let read = if self.observes {
                self.from.get()
            } else {
                self.from.peek()
            };
            EffectPoll::Ready(format!("{}{read}", input.0))
        }
    }

    /// Stand-in for step 2's content hash - FNV over the bytes, as in
    /// `two_drivers_one_graph.rs`. Equal inputs key equally; nothing more is
    /// claimed.
    fn content_key_of(text: &str) -> ContentKey {
        let mut h: u128 = 0x6c62_272e_07bb_0142_62b8_2175_6295_c58du128;
        for byte in text.as_bytes() {
            h ^= u128::from(*byte);
            h = h.wrapping_mul(0x0000_0000_0100_0000_0000_0000_0000_013bu128);
        }
        ContentKey::from_u128(h)
    }

    /// A store that remembers, so a memo hit is a real hit.
    struct MapStore<V> {
        rows: Mutex<HashMap<MemoKey, V>>,
    }

    impl<V: Clone> MemoStore<V> for MapStore<V> {
        fn lookup(&self, key: &MemoKey) -> Option<V> {
            self.rows.lock().unwrap().get(key).cloned()
        }

        fn record(&self, key: &MemoKey, value: V) {
            self.rows.lock().unwrap().insert(key.clone(), value);
        }
    }

    impl<V> MapStore<V> {
        fn new() -> Self {
            Self {
                rows: Mutex::new(HashMap::new()),
            }
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
    fn changing_an_input_marks_its_dependents_and_nothing_else() {
        let ledger = Ledger::new();
        let read = Arc::new(TrackedInput::new(&ledger, "read", "A".to_string()));
        let ignored = Arc::new(TrackedInput::new(&ledger, "ignored", "Z".to_string()));
        let reader = Tracked::new(&ledger, "reader", Reads::new(&read, true));
        let bystander = Tracked::new(&ledger, "bystander", Reads::new(&ignored, true));

        poll(&reader, &Text("hi".to_string()));
        poll(&bystander, &Text("hi".to_string()));
        assert!(ledger.stale_nodes().is_empty(), "a poll is not a change");

        assert!(read.set("B".to_string()));
        assert_eq!(
            labels(&ledger, &ledger.stale_nodes()),
            ["reader"],
            "the bystander reads a different input and must not be woken by this one",
        );
    }

    #[test]
    fn staleness_is_transitive() {
        // A read by `inner`, `inner` polled by `middle`, `middle` polled by
        // `outer`. Only `inner` reads the input; the other two depend on it through
        // nodes, which is the edge the reverse index has to walk.
        let ledger = Ledger::new();
        let read = Arc::new(TrackedInput::new(&ledger, "read", "A".to_string()));
        let inner = Tracked::new(&ledger, "inner", Reads::new(&read, true));
        let middle = Tracked::new(&ledger, "middle", Relays { inner });
        let outer = Tracked::new(&ledger, "outer", Relays { inner: middle });

        poll(&outer, &Text("hi".to_string()));
        read.set("B".to_string());

        assert_eq!(
            labels(&ledger, &ledger.stale_nodes()),
            ["inner", "middle", "outer"],
            "invalidation reaches every node that transitively read the input",
        );
    }

    #[test]
    fn an_unchanged_input_marks_nothing_and_moves_no_revision() {
        // §3's backdating at the leaf: "without cutoff every keystroke invalidates
        // the whole pipeline". An editor writing back the value it already held is
        // that keystroke.
        let ledger = Ledger::new();
        let read = Arc::new(TrackedInput::new(&ledger, "read", "A".to_string()));
        let reader = Tracked::new(&ledger, "reader", Reads::new(&read, true));
        poll(&reader, &Text("hi".to_string()));

        assert!(!read.set("A".to_string()), "the value did not move");
        assert!(ledger.stale_nodes().is_empty());
        assert_eq!(ledger.revision(read.node()), 0, "and neither did the revision");

        assert!(read.set("B".to_string()));
        assert_eq!(ledger.revision(read.node()), 1);
    }

    #[test]
    fn an_input_a_stage_no_longer_reads_marks_nothing() {
        // The invalidation half of §3's conditional clause: "a branch not taken
        // this run contributes no edge, so a change behind it wakes nothing".
        // `reads_become_edges.rs` shows the edge going away; this shows what that
        // buys.
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
        assert!(left.set("L2".to_string()));
        assert_eq!(labels(&ledger, &ledger.stale_nodes()), ["either"]);

        // The stage takes the other branch, and the run re-logs its reads.
        *stage.stage().take_left.lock().unwrap() = false;
        poll(&stage, &Text(String::new()));
        assert!(!ledger.is_stale(stage.node()), "the run revalidated it");

        assert!(left.set("L3".to_string()), "the value moved");
        assert!(
            ledger.stale_nodes().is_empty(),
            "nothing reads `left` any more, so a change behind the branch not \
             taken wakes nothing",
        );

        assert!(right.set("R2".to_string()));
        assert_eq!(
            labels(&ledger, &ledger.stale_nodes()),
            ["either"],
            "and the branch it DID take still invalidates",
        );
    }

    #[test]
    fn polling_a_node_clears_its_staleness_and_a_pending_poll_does_not() {
        let ledger = Ledger::new();
        let read = Arc::new(TrackedInput::new(&ledger, "read", "A".to_string()));
        let reader = Tracked::new(&ledger, "reader", Reads::new(&read, true));
        poll(&reader, &Text("hi".to_string()));
        read.set("B".to_string());
        assert!(ledger.is_stale(reader.node()));

        poll(&reader, &Text("hi".to_string()));
        assert!(
            !ledger.is_stale(reader.node()),
            "the run revalidated it, and the clear happened on the way IN so a \
             change landing during the run would survive",
        );

        let parks = Tracked::new(&ledger, "parks", Parks);
        assert!(poll(&parks, &Text("hi".to_string())).is_pending());
        assert!(
            ledger.is_stale(parks.node()),
            "a Pending poll produced no value, so it revalidated nothing",
        );
    }

    #[test]
    fn the_frame_driver_learns_of_a_change_because_the_read_was_observed() {
        // The payoff. `Reads` registers no waker, holds none, and knows nothing
        // about a driver; the ledger is what connects the change to the frame loop,
        // and it knows where to send it because it watched the read happen.
        let ledger = Ledger::new();
        let title = Arc::new(TrackedInput::new(&ledger, "title", "A".to_string()));
        let stage = Tracked::new(&ledger, "reader", Reads::new(&title, true));
        let driver = FrameDriver::new();
        ledger.subscribe(driver.waker());

        assert_eq!(
            driver.poll_frame(&stage, &Text("hi".to_string())),
            EffectPoll::Ready("hiA".to_string()),
        );
        assert!(!driver.take_stale(), "a poll is not a wake");

        title.set("B".to_string());
        assert!(driver.take_stale(), "the change reached the frame loop");
        assert_eq!(
            driver.poll_frame(&stage, &Text("hi".to_string())),
            EffectPoll::Ready("hiB".to_string()),
        );
        assert_eq!(stage.stage().runs(), 2);
    }

    #[test]
    fn the_same_stage_reading_unobserved_never_reaches_the_frame_loop() {
        // The known-bad input, and the exact shape of step 1's finding: the value
        // is there for the asking and the frame loop never asks, because nothing
        // told it to. `peek` is the one line that differs.
        let ledger = Ledger::new();
        let title = Arc::new(TrackedInput::new(&ledger, "title", "A".to_string()));
        let stage = Tracked::new(&ledger, "reader", Reads::new(&title, false));
        let driver = FrameDriver::new();
        ledger.subscribe(driver.waker());

        assert_eq!(
            driver.poll_frame(&stage, &Text("hi".to_string())),
            EffectPoll::Ready("hiA".to_string()),
        );
        title.set("B".to_string());
        assert!(
            !driver.take_stale(),
            "no edge was recorded, so the change woke nobody",
        );
        assert!(ledger.stale_nodes().is_empty());
        assert_eq!(
            driver.poll_frame(&stage, &Text("hi".to_string())),
            EffectPoll::Ready("hiB".to_string()),
            "the new value was available all along - it is the TELLING that was lost",
        );
    }

    #[test]
    fn a_wake_subscription_is_not_one_shot_and_does_not_accumulate() {
        let ledger = Ledger::new();
        let title = Arc::new(TrackedInput::new(&ledger, "title", "A".to_string()));
        let stage = Tracked::new(&ledger, "reader", Reads::new(&title, true));
        let driver = FrameDriver::new();
        for _ in 0..3 {
            // A frame loop subscribing every frame is the expected usage.
            ledger.subscribe(driver.waker());
        }

        poll(&stage, &Text("hi".to_string()));
        title.set("B".to_string());
        assert!(driver.take_stale());

        poll(&stage, &Text("hi".to_string()));
        title.set("C".to_string());
        assert!(
            driver.take_stale(),
            "the subscription survives the wake - §3's wake means `stale, poll \
             again`, not `the thing you waited for arrived`",
        );
    }

    #[test]
    fn a_memo_over_a_tracked_read_cannot_serve_what_the_ledger_ruled_stale() {
        // The finding this test was written to pin: TRACKING SEES A DEPENDENCY THE
        // MEMO KEY DOES NOT. `Stage::memo_key` is built from the stage's INPUT
        // argument, so a stage that also reads tracked state has an ambient input
        // the key cannot see - and the memo would happily serve a value the ledger
        // had already marked stale.
        //
        // It is now the ENGINE's business rather than the stage's. `Memo` does not
        // consult its store while `revalidating()` - while the poll is running
        // inside the scope of a node the ledger ruled out - so `folds_revision`
        // buys speed under a moved key and no longer stands between the cache and a
        // wrong answer. Both settings answer the same, which is the claim.
        for folds_revision in [true, false] {
            let ledger = Ledger::new();
            let from = Arc::new(TrackedInput::new(&ledger, "from", "A".to_string()));
            let stage = Tracked::new(
                &ledger,
                "composes",
                Memo::new(
                    Composes::new(&ledger, &from, true, folds_revision),
                    MapStore::new(),
                ),
            );
            let input = Text("hi".to_string());

            assert_eq!(poll(&stage, &input), EffectPoll::Ready("hiA".to_string()));
            from.set("B".to_string());
            assert!(ledger.is_stale(stage.node()), "the ledger saw the read");

            assert_eq!(
                poll(&stage, &input),
                EffectPoll::Ready("hiB".to_string()),
                "the store held `hiA` under a key that had not moved; the ledger's \
                 mark outranks it",
            );
            assert_eq!(stage.stage().stage().runs(), 2);
            assert_eq!(
                stage.stage().stage().saw_revalidating(),
                [false, true],
                "and the mechanism is the scope's own flag, not a coincidence of \
                 the key: fresh on the first run, revalidating on the second",
            );

            // The third poll re-establishes that this is still a cache. Nothing is
            // stale now, so the store answers and the stage does not run - and it
            // answers with what the second run recorded, not the entry the ledger
            // ruled out.
            assert_eq!(poll(&stage, &input), EffectPoll::Ready("hiB".to_string()));
            assert_eq!(stage.stage().stage().runs(), 2, "the third poll was a hit");
        }
    }

    #[test]
    fn a_memo_over_an_unobserved_read_is_the_case_only_a_declared_key_saves() {
        // The known-bad twin, and the boundary of the rule above: the gate is the
        // LEDGER's mark, so it can only fire where the ledger was told. This stage
        // reads through `peek`, exactly as `Reads` does in this file's first twin -
        // no edge, nothing marked, `revalidating()` false on every run - and the
        // store answers with the value from before the change.
        let ledger = Ledger::new();
        let from = Arc::new(TrackedInput::new(&ledger, "from", "A".to_string()));
        let stage = Tracked::new(
            &ledger,
            "composes",
            Memo::new(Composes::new(&ledger, &from, false, false), MapStore::new()),
        );
        let input = Text("hi".to_string());

        assert_eq!(poll(&stage, &input), EffectPoll::Ready("hiA".to_string()));
        from.set("B".to_string());
        assert!(!ledger.is_stale(stage.node()), "no edge, so nothing was marked");
        assert_eq!(
            poll(&stage, &input),
            EffectPoll::Ready("hiA".to_string()),
            "the memo served a value that had moved - and no layer here was ever \
             told it had",
        );
        assert_eq!(stage.stage().stage().saw_revalidating(), [false]);

        // The same stage that declares the ambient input in its key survives it.
        // This is `ContentKey`'s first branch, and where it earns its keep: it is
        // the only one of the two that works when tracking cannot see the read.
        let ledger = Ledger::new();
        let from = Arc::new(TrackedInput::new(&ledger, "from", "A".to_string()));
        let declared = Tracked::new(
            &ledger,
            "composes",
            Memo::new(Composes::new(&ledger, &from, false, true), MapStore::new()),
        );
        assert_eq!(poll(&declared, &input), EffectPoll::Ready("hiA".to_string()));
        from.set("B".to_string());
        assert_eq!(
            poll(&declared, &input),
            EffectPoll::Ready("hiB".to_string()),
            "the revision moved the key, so the lookup missed on its own",
        );
    }

    #[test]
    fn a_cache_outside_the_tracking_is_a_cache_the_ledger_cannot_reach() {
        // The other known-bad twin: the same two layers, composed the other way
        // round. `Memo::new(Tracked::new(..), store)` puts the lookup OUTSIDE the
        // node's scope, so it happens before any scope opens, `revalidating()` is
        // false, and the hit means the tracked stage is never polled at all. The
        // ledger's mark is right there and unread.
        //
        // `Memo` cannot detect this - it is generic over a stage and can no more
        // inspect its inner one than a driver can - so the rule is the composition
        // order, stated in `Memo`'s doc and measured here.
        let ledger = Ledger::new();
        let from = Arc::new(TrackedInput::new(&ledger, "from", "A".to_string()));
        let stage = Memo::new(
            Tracked::new(
                &ledger,
                "composes",
                Composes::new(&ledger, &from, true, false),
            ),
            MapStore::new(),
        );
        let input = Text("hi".to_string());

        assert_eq!(poll(&stage, &input), EffectPoll::Ready("hiA".to_string()));
        from.set("B".to_string());
        assert!(
            ledger.is_stale(stage.stage().node()),
            "the read WAS observed and the node IS stale",
        );
        assert_eq!(
            poll(&stage, &input),
            EffectPoll::Ready("hiA".to_string()),
            "and the outer store answered anyway - the contradiction the correct \
             order removes",
        );
        assert_eq!(stage.stage().stage().runs(), 1);
    }

    #[test]
    fn the_memo_over_tracked_state_changes_speed_and_not_answers() {
        // `NoMemo` is the control case its own doc describes: "a pipeline whose
        // ANSWERS change when the cache is disabled has a bug the cache was
        // hiding". Over UNTRACKED stages that check already passed
        // (`two_drivers_one_graph.rs`'s `the_memo_changes_speed_and_not_answers`);
        // over a stage that reads tracked state it did not, and that failure is
        // what the revalidation gate exists to remove.
        //
        // The run counts are the other half and are not decoration: a gate that
        // simply stopped the store from ever answering would pass the equality
        // above and fail here.
        let (cached_answers, cached_runs) = drive_over_tracked_state(MapStore::new());
        let (uncached_answers, uncached_runs) = drive_over_tracked_state(NoMemo);

        assert_eq!(
            cached_answers,
            ["hiA", "hiB", "hiB", "hiA"],
            "the answers follow the tracked value, cache or no cache",
        );
        assert_eq!(cached_answers, uncached_answers);
        assert!(
            cached_runs < uncached_runs,
            "and the cache still hits where nothing moved: {cached_runs} runs \
             against {uncached_runs}",
        );
    }

    /// Poll one memoized, tracked stage through a sequence of writes - one of which
    /// changes nothing - and report the answers and how many times it ran.
    fn drive_over_tracked_state<St: MemoStore<String>>(store: St) -> (Vec<String>, usize) {
        let ledger = Ledger::new();
        let from = Arc::new(TrackedInput::new(&ledger, "from", "A".to_string()));
        let stage = Tracked::new(
            &ledger,
            "composes",
            Memo::new(Composes::new(&ledger, &from, true, false), store),
        );
        let input = Text("hi".to_string());

        let answers = ["A", "B", "B", "A"]
            .into_iter()
            .map(|value| {
                from.set(value.to_string());
                match poll(&stage, &input) {
                    EffectPoll::Ready(value) => value,
                    other => panic!("a pure stage answered {other:?}"),
                }
            })
            .collect();
        (answers, stage.stage().stage().runs())
    }

    #[test]
    fn a_memo_hit_does_not_cost_the_node_its_edges() {
        // The conservative rule of `Ledger::run` seen from the case it was written
        // for: the memo answered without running the stage, so no read was
        // observed, and the previous set has to stand or this node is never
        // invalidated again.
        let ledger = Ledger::new();
        let from = Arc::new(TrackedInput::new(&ledger, "from", "A".to_string()));
        let stage = Tracked::new(
            &ledger,
            "composes",
            Memo::new(
                Composes::new(&ledger, &from, true, false),
                MapStore::new(),
            ),
        );
        let input = Text("hi".to_string());

        poll(&stage, &input);
        assert_eq!(labels(&ledger, &ledger.reads_of(stage.node())), ["from"]);

        poll(&stage, &input);
        assert_eq!(stage.stage().stage().runs(), 1, "the second poll was a hit");
        assert_eq!(
            labels(&ledger, &ledger.reads_of(stage.node())),
            ["from"],
            "the hit observed no reads, and the edge survived it",
        );

        assert!(from.set("B".to_string()));
        assert!(ledger.is_stale(stage.node()));
    }
}

#[cfg(test)]
mod an_equal_recompute_stops_at_its_node {
    //! **Moved in from `tests/an_equal_recompute_stops_at_its_node.rs`** at the
    //! visibility flip. It composes the tracked layer by hand
    //! (`Backdated`, `FrameDriver`, `Ledger`, `NodeId`, `Tracked`, `TrackedInput`), and the builder has no spelling for a tracked
    //! graph - `DESIGN.md`'s finding 1. A test in `tests/` proves the PUBLIC API
    //! can express something; a test in `src/` admits it cannot yet, and lives
    //! beside the code it pins so that a reshape of that code sees it. Every
    //! assertion is the one it arrived with; when finding 1 lands this migrates
    //! back out unchanged but for its imports.
    //!
    //! Gate: **backdating above the leaf** - a derived node that recomputes to the
    //! value it already had leaves its consumers fresh (`PIPELINE_PLAN.md` §3).
    //!
    //! §3 wants both halves - "constructive keys give the *lookup*, backdating gives
    //! the *cutoff*, and a live IDE needs both" - and until now only the leaf had
    //! one: [`TrackedInput::set`] refuses to invalidate on a write of an equal
    //! value, exactly and for one comparison. That does nothing for the case §3
    //! names, "without cutoff every keystroke invalidates the whole pipeline",
    //! because the keystroke DOES move the source. What saves the pipeline is the
    //! first stage above it whose output ignores the difference - a formatter fed a
    //! re-indented file, a lowering fed a renamed local - and that stage's
    //! consumers were re-run anyway.
    //!
    //! The missing half needed "somewhere to keep the last output and an equality to
    //! trust". §9's step 2 supplied the equality (`ContentHash`), so what is kept is
    //! its 128-bit address rather than the output; the retraction is
    //! [`Ledger::unchanged`], and staleness carries REASONS so that there is
    //! something to retract.
    //!
    //! **Every type here is a stand-in** (`PIPELINE_PLAN.md`:584-589).

    use std::sync::{Arc, Mutex};
    use std::task::{Context, Waker};

    use crate::track::Backdated;
    use crate::driver::FrameDriver;
    use crate::track::Ledger;
    use crate::track::NodeId;
    use crate::track::Tracked;
    use crate::track::TrackedInput;
    use libpipelinedata::{EffectPoll, MemoKey, Stage, StageId};

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
            StageId::new("test.normalizes", 1)
        }

        fn memo_key(&self, _input: &Text) -> Option<MemoKey> {
            None
        }

        fn poll_stage(&self, _input: &Text, _cx: &mut Context<'_>) -> EffectPoll<String, &'static str> {
            *self.runs.lock().unwrap() += 1;
            EffectPoll::Ready(self.from.get().trim().to_string())
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
            StageId::new("test.relays", 1)
        }

        fn memo_key(&self, _input: &Text) -> Option<MemoKey> {
            None
        }

        fn poll_stage(&self, input: &Text, cx: &mut Context<'_>) -> EffectPoll<String, &'static str> {
            *self.runs.lock().unwrap() += 1;
            self.inner.poll_stage(input, cx)
        }
    }

    /// A consumer that polls its inner stage - so the edge is real - and then never
    /// lands. §4's rows 10 and 11 in miniature.
    struct ParksAfterReading<S> {
        inner: Arc<S>,
    }

    impl<S: Stage<Input = Text, Output = String, Error = &'static str>> Stage for ParksAfterReading<S> {
        type Input = Text;
        type Output = String;
        type Error = &'static str;

        fn id(&self) -> StageId {
            StageId::new("test.parks_after_reading", 1)
        }

        fn memo_key(&self, _input: &Text) -> Option<MemoKey> {
            None
        }

        fn poll_stage(&self, input: &Text, cx: &mut Context<'_>) -> EffectPoll<String, &'static str> {
            let _ = self.inner.poll_stage(input, cx);
            let _ = cx.waker().clone();
            EffectPoll::Pending
        }
    }

    fn poll<S: Stage>(stage: &S, input: &S::Input) -> EffectPoll<S::Output, S::Error> {
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

        assert_eq!(poll(&*c, &input()), EffectPoll::Ready("A".to_string()));
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
        assert_eq!(poll(&*a, &input()), EffectPoll::Ready("A".to_string()));

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
        // waiting, which is the state of things §3 calls out.
        let ledger = Ledger::new();
        let source = Arc::new(TrackedInput::new(&ledger, "source", "A".to_string()));
        let a = Arc::new(Tracked::new(&ledger, "a", Normalizes::new(&source)));
        let b = Arc::new(Tracked::new(&ledger, "b", Relays::new(&a)));
        let c = Arc::new(Tracked::new(&ledger, "c", Relays::new(&b)));

        poll(&*c, &input());
        source.set("  A  ".to_string());
        assert_eq!(poll(&*a, &input()), EffectPoll::Ready("A".to_string()));

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
        assert_eq!(poll(&*a, &input()), EffectPoll::Ready("B".to_string()));
        assert_eq!(
            labels(&ledger, &ledger.stale_nodes()),
            ["b"],
            "the output moved, so the consumer still has work",
        );

        // And the node it cut off from is remembered, so a LATER equal recompute
        // cuts off against `B` rather than against the value before it.
        source.set(" B ".to_string());
        assert_eq!(poll(&*a, &input()), EffectPoll::Ready("B".to_string()));
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

        assert_eq!(poll(&*both, &input()), EffectPoll::Ready("LR".to_string()));
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
            StageId::new("test.joins", 1)
        }

        fn memo_key(&self, _input: &Text) -> Option<MemoKey> {
            None
        }

        fn poll_stage(&self, input: &Text, cx: &mut Context<'_>) -> EffectPoll<String, &'static str> {
            *self.runs.lock().unwrap() += 1;
            let (EffectPoll::Ready(left), EffectPoll::Ready(right)) = (
                self.left.poll_stage(input, cx),
                self.right.poll_stage(input, cx),
            ) else {
                return EffectPoll::Pending;
            };
            EffectPoll::Ready(format!("{left}{right}"))
        }
    }

    #[test]
    fn a_node_that_owes_a_value_is_not_retracted_by_the_node_below_it() {
        // The reason that cannot be taken back. A `Pending` poll marks its own node
        // - it produced no value, so it revalidated nothing - and no equality below
        // it answers for a value that was never produced. A cutoff that cleared this
        // would drop the parked node out of the schedule, which is step 1's "lost
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
}

#[cfg(test)]
mod reads_become_edges {
    //! **Moved in from `tests/reads_become_edges.rs`** at the
    //! visibility flip. It composes the tracked layer by hand
    //! (`Ledger`, `NodeId`, `Tracked`, `TrackedInput`), and the builder has no spelling for a tracked
    //! graph - `DESIGN.md`'s finding 1. A test in `tests/` proves the PUBLIC API
    //! can express something; a test in `src/` admits it cannot yet, and lives
    //! beside the code it pins so that a reshape of that code sees it. Every
    //! assertion is the one it arrived with; when finding 1 lands this migrates
    //! back out unchanged but for its imports.
    //!
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

    use crate::track::Ledger;
    use crate::track::NodeId;
    use crate::track::Tracked;
    use crate::track::TrackedInput;
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
}

#[cfg(test)]
mod a_fallback_is_not_a_revalidation {
    //! **Moved in from `tests/a_fallback_is_not_a_revalidation.rs`** at the
    //! visibility flip. It composes the tracked layer by hand
    //! (`Guarded`, `Ledger`, `Tracked`, `TrackedInput`), and the builder has no spelling for a tracked
    //! graph - `DESIGN.md`'s finding 1. A test in `tests/` proves the PUBLIC API
    //! can express something; a test in `src/` admits it cannot yet, and lives
    //! beside the code it pins so that a reshape of that code sees it. Every
    //! assertion is the one it arrived with; when finding 1 lands this migrates
    //! back out unchanged but for its imports.
    //!
    //! Gate: **a poll that produced no value leaves its node owing one - `Failed`
    //! exactly as much as `Pending`** (`PIPELINE_PLAN.md` §3, §7).
    //!
    //! # The second thing a boundary launders
    //!
    //! `a_boundary_is_not_a_cacheable_answer.rs` gates the first: a substituted
    //! `Ready` must not be recorded by a [`Memo`](crate::memo::Memo), which
    //! [`Guarded::memo_key`](crate::boundary::Guarded) closes structurally. A memo
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
    //!    That state was **unreachable before §7**: a failure used to end the
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
    //! The defect is invisible to a driver that re-polls unconditionally. §5's
    //! offline driver loops until it gets a value and `FrameDriver` polls its root
    //! every frame; both would reach the real answer regardless. What loses it is a
    //! driver that polls what the LEDGER says needs polling, which is what
    //! [`Schedule`](crate::schedule::Schedule) exists to answer. So `frame_if_scheduled`
    //! below is that driver, in the smallest honest form - and the same shape as
    //! `watch.rs`'s finding: a defect only one of the drivers can even express is a
    //! defect that ships.
    //!
    //! **Every type here is a stand-in** (`PIPELINE_PLAN.md`:584-589).

    use std::sync::{Arc, Mutex};
    use std::task::{Context, Waker};

    use libeffects::Fallback;
    use crate::boundary::Guarded;
    use crate::track::Ledger;
    use crate::track::Tracked;
    use crate::track::TrackedInput;
    use libpipelinedata::{EffectPoll, MemoKey, Stage, StageId};

    // ---------------------------------------------------------------- stand-ins

    /// Stand-in for whatever an author wrote.
    #[derive(Clone, PartialEq, Eq, Debug)]
    struct Text(String);

    /// Transient in the sense §7 cares about: it can clear.
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
        const ID: StageId = StageId::new("test.flaky", 1);

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

        fn poll_stage(&self, input: &Text, cx: &mut Context<'_>) -> EffectPoll<String, Boom> {
            *self.polls.lock().unwrap() += 1;
            let read = self.from.get();
            let Some(filled) = *self.slot.lock().unwrap() else {
                self.waiting.lock().unwrap().push(cx.waker().clone());
                return match self.while_empty {
                    WhileEmpty::Failing => EffectPoll::Failed(Boom),
                    WhileEmpty::Parked => EffectPoll::Pending,
                };
            };
            EffectPoll::Ready(format!("{}:{read}:{filled}", input.0))
        }
    }

    const GUARD: StageId = StageId::new("test.guard", 1);

    /// Poll unconditionally, as both of §5's drivers do.
    fn driven<S: Stage>(stage: &S, input: &S::Input) -> EffectPoll<S::Output, S::Error> {
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
    ) -> Option<EffectPoll<S::Output, S::Error>> {
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
            EffectPoll::Ready("src:a:v1".to_string()),
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
        let guarded = Guarded::new(GUARD, node, Fallback::new("fallback".to_string()));
        let input = Text("src".into());

        // First frame: drawn with a stand-in, and the node still owes its answer.
        assert_eq!(driven(&guarded, &input), EffectPoll::Ready("fallback".into()));
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
            Some(EffectPoll::Ready("fallback".into())),
        );

        // The failure clears out of band. Nothing marks anything stale - the
        // node's own debt is what keeps it in the schedule.
        guarded.stage().stage().land("v1");
        assert_eq!(
            frame_if_scheduled(&ledger, &guarded, &input),
            Some(EffectPoll::Ready("src:a:v1".into())),
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
            Guarded::new(GUARD, flaky, Fallback::new("fallback".to_string())),
        );
        let input = Text("src".into());

        assert_eq!(driven(&node, &input), EffectPoll::Ready("fallback".into()));
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
            EffectPoll::Ready("src:a:v1".into()),
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
    fn read_as<S: Stage>(ledger: &Ledger, reader: crate::track::NodeId, root: &S, input: &S::Input) {
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
        let node = crate::track::Backdated::new(
            &ledger,
            "n",
            Guarded::new(
                GUARD,
                Flaky::new(WhileEmpty::Failing, &src),
                Fallback::new("fallback".to_string()),
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
        assert_eq!(driven(&node, &input), EffectPoll::Ready("fallback".into()));
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
        let node = crate::track::Backdated::new(&ledger, "n", Flaky::new(WhileEmpty::Failing, &src));
        let watched = node.node();
        let guarded = Guarded::new(GUARD, node, Fallback::new("fallback".to_string()));
        let consumer = ledger.node("c");
        let input = Text("src".into());

        read_as(&ledger, consumer, &guarded, &input);
        assert_eq!(ledger.readers_of(watched), vec![consumer]);

        assert!(src.set("b".to_string()));
        assert!(ledger.is_stale(watched) && ledger.is_stale(consumer));

        assert_eq!(driven(&guarded, &input), EffectPoll::Ready("fallback".into()));
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
}
