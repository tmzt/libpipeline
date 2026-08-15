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
/// [`Memo`](crate::Memo) consults this and does not answer from its store while
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
pub fn revalidating() -> bool {
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
pub struct Backdated<S> {
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

    /// The ledger this node belongs to.
    pub fn ledger(&self) -> &Arc<Ledger> {
        self.tracked.ledger()
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
