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
//! [`TrackedInput::peek`](libpipeline::TrackedInput::peek) instead of `get`:
//! same value, same answer, no edge, and the frame loop is never told.
//!
//! **Every type here is a stand-in** (`PIPELINE_PLAN.md`:563-568).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::task::{Context, Waker};

use libpipeline::{FrameDriver, Ledger, Memo, NodeId, Tracked, TrackedInput};
use libpipelinedata::{ContentKey, EffectPoll, MemoKey, MemoStore, Stage, StageId};

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

/// A memoized stage that reads tracked state, with the fold that makes its key
/// honest switchable - see `a_memo_over_a_tracked_read_needs_the_revision_in_its_key`.
struct Composes {
    ledger: Arc<Ledger>,
    from: Arc<TrackedInput<String>>,
    folds_revision: bool,
    runs: Mutex<usize>,
}

impl Composes {
    const ID: StageId = StageId::new("test.composes", 1);

    fn runs(&self) -> usize {
        *self.runs.lock().unwrap()
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
        EffectPoll::Ready(format!("{}{}", input.0, self.from.get()))
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
fn a_memo_over_a_tracked_read_needs_the_revision_in_its_key() {
    // The finding this test exists to pin: TRACKING SEES A DEPENDENCY THE MEMO
    // KEY DOES NOT. `Stage::memo_key` is built from the stage's INPUT argument,
    // so a stage that also reads tracked state has an ambient input the key
    // cannot see - and the memo will happily serve a value the ledger has
    // already marked stale. ContentKey's doc states the rule ("an ambient input
    // either becomes a real input with a content key, or it moves the version.
    // There is no third option that leaves the cache correct"); the ledger's
    // revision is what makes the first branch available before §9's step 2
    // gives values content hashes.
    for folds_revision in [true, false] {
        let ledger = Ledger::new();
        let from = Arc::new(TrackedInput::new(&ledger, "from", "A".to_string()));
        let stage = Tracked::new(
            &ledger,
            "composes",
            Memo::new(
                Composes {
                    ledger: Arc::clone(&ledger),
                    from: Arc::clone(&from),
                    folds_revision,
                    runs: Mutex::new(0),
                },
                MapStore::new(),
            ),
        );
        let input = Text("hi".to_string());

        assert_eq!(poll(&stage, &input), EffectPoll::Ready("hiA".to_string()));
        from.set("B".to_string());
        assert!(ledger.is_stale(stage.node()), "the ledger saw the read");

        let second = poll(&stage, &input);
        if folds_revision {
            assert_eq!(second, EffectPoll::Ready("hiB".to_string()));
            assert_eq!(stage.stage().stage().runs(), 2);
        } else {
            assert_eq!(
                second,
                EffectPoll::Ready("hiA".to_string()),
                "the memo served a value the ledger knew was stale - this is \
                 the defect, measured rather than described",
            );
            assert_eq!(stage.stage().stage().runs(), 1);
        }
    }
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
            Composes {
                ledger: Arc::clone(&ledger),
                from: Arc::clone(&from),
                folds_revision: false,
                runs: Mutex::new(0),
            },
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
