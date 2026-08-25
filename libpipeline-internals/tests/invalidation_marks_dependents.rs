//! **Moved in from `tests/invalidation_marks_dependents.rs`** at the
//! visibility flip. It composes the tracked layer by hand
//! (`FrameDriver`, `Ledger`, `Memo`, `NodeId`, `Tracked`, `TrackedInput`, `revalidating`), and the builder has no spelling for a tracked
//! graph - `PLAN.md`'s finding 1. A test in `tests/` proves the PUBLIC API
//! can express something; a test in `src/` admits it cannot yet, and lives
//! beside the code it pins so that a reshape of that code sees it. Every
//! assertion is the one it arrived with; when finding 1 lands this migrates
//! back out unchanged but for its imports.
//!
//! Gate: **a changed input marks its dependents stale, transitively - and
//! nothing else.**
//!
//! The edges are the previous gate's subject (`reads_become_edges.rs`); this
//! one is what they are FOR. Four clauses are pinned separately because they
//! fail separately: who gets marked, how far the marking travels, what a write
//! that changes nothing does, and how the mark reaches a driver.
//!
//! **The payoff clause is the last one.** `libpipeline/tests/one_door_two_patterns.rs`
//! measures that staleness ordinarily reaches a driver only because a
//! stage registered the waker it was handed
//! (`libpipeline/tests/one_door_two_patterns.rs`'s
//! `a_pending_stage_that_registers_no_waker_is_a_value_lost_rather_than_late`).
//! Here the stage registers nothing, holds no waker and has no idea a driver
//! exists - and the frame loop still learns, because the READ was observed.
//! Its known-bad twin is the same stage reading through
//! [`TrackedInput::peek`](libpipeline_internals::track::TrackedInput::peek) instead of `get`:
//! same value, same answer, no edge, and the frame loop is never told.
//!
//! **Every type here is a stand-in** (`DESIGN.md`, "The engine stays
//! generic").

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::task::{Context, Waker};

use libpipeline_internals::driver::FrameDriver;
use libpipeline_internals::track::Ledger;
use libpipeline_internals::memo::Memo;
use libpipeline_internals::track::NodeId;
use libpipeline_internals::track::Tracked;
use libpipeline_internals::track::TrackedInput;
use libpipeline_internals::track::revalidating;
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
        StageId::at(0)
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

/// A stage that reads ONE of two inputs, chosen per run - the conditional case,
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
        StageId::at(1)
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
        StageId::at(2)
    }

    fn memo_key(&self, _input: &Text) -> Option<MemoKey> {
        None
    }

    fn poll_stage(&self, input: &Text, cx: &mut Context<'_>) -> EffectPoll<String, &'static str> {
        self.inner.poll_stage(input, cx)
    }
}

/// A stage that never lands - the effectful, pending shape in miniature, minus the
/// upstream, because what this file needs from it is only the `Pending`.
struct Parks;

impl Stage for Parks {
    type Input = Text;
    type Output = String;
    type Error = &'static str;

    fn id(&self) -> StageId {
        StageId::at(3)
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
    const ID: StageId = StageId::at(4);

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
            // An ambient input either becomes a real input with a content
            // key, or the engine has to notice it moved (`Ledger::revision`'s
            // doc). This is the first branch, and the revision is what it
            // costs.
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

/// Stand-in for the streaming content hash - FNV over the bytes, as in
/// `libpipeline/tests/one_door_two_patterns.rs`. Equal inputs key equally; nothing more is
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
    rows: Mutex<HashMap<MemoKey, Arc<V>>>,
}

impl<V> MemoStore<V> for MapStore<V> {
    fn lookup(&self, key: &MemoKey) -> Option<Arc<V>> {
        self.rows.lock().unwrap().get(key).map(Arc::clone)
    }

    fn record(&self, key: &MemoKey, value: Arc<V>) {
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

/// What a MEMOIZED stage answers: a share of its output, not the output.
///
/// `Memo` wraps once, on a miss, where it records - so both the value it hands
/// back and the row it kept are that one allocation, and every hit after it is
/// a refcount bump. Every expectation in this file that is stated against a
/// memoized stage goes through here.
fn shared(text: &str) -> EffectPoll<Arc<String>, &'static str> {
    EffectPoll::Ready(Arc::new(text.to_string()))
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
    // Backdating at the leaf: "without cutoff every keystroke invalidates
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
    // The invalidation half of the conditional clause: "a branch not taken
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
    // The known-bad input, and the exact shape of the lost-wake finding: the value
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
        "the subscription survives the wake - a wake means `stale, poll \
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

        assert_eq!(poll(&stage, &input), shared("hiA"));
        from.set("B".to_string());
        assert!(ledger.is_stale(stage.node()), "the ledger saw the read");

        assert_eq!(
            poll(&stage, &input),
            shared("hiB"),
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
        assert_eq!(poll(&stage, &input), shared("hiB"));
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

    assert_eq!(poll(&stage, &input), shared("hiA"));
    from.set("B".to_string());
    assert!(!ledger.is_stale(stage.node()), "no edge, so nothing was marked");
    assert_eq!(
        poll(&stage, &input),
        shared("hiA"),
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
    assert_eq!(poll(&declared, &input), shared("hiA"));
    from.set("B".to_string());
    assert_eq!(
        poll(&declared, &input),
        shared("hiB"),
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

    assert_eq!(poll(&stage, &input), shared("hiA"));
    from.set("B".to_string());
    assert!(
        ledger.is_stale(stage.stage().node()),
        "the read WAS observed and the node IS stale",
    );
    assert_eq!(
        poll(&stage, &input),
        shared("hiA"),
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
    // (`libpipeline/tests/one_door_two_patterns.rs`'s `the_memo_changes_speed_and_not_answers`);
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
        ["hiA", "hiB", "hiB", "hiA"].map(|text| Arc::new(text.to_string())),
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
fn drive_over_tracked_state<St: MemoStore<String>>(store: St) -> (Vec<Arc<String>>, usize) {
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
