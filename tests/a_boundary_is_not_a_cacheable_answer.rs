//! Gate: **a boundary refuses to be memoized, and a boundary that did not
//! would poison the memo it sits under** (`PIPELINE_PLAN.md` §7, §3).
//!
//! The hazard, stated on [`Guarded`](libpipeline::Guarded) and on
//! `libeffects::Boundary` before it: a boundary LAUNDERS an uncacheable answer
//! into a cacheable-looking one. [`Memo`] already refuses to record `Failed`,
//! for `OBJECTS_PLAN_PI.md:707`'s reason - effects are never replayed by an
//! implicit cache - and a boundary turns exactly that `Failed` into a `Ready`.
//! Cache it and the key says "input X, value V" while V is the fallback; the
//! input never moved, so the key never moves, and the real value is never
//! computed again.
//!
//! **What is measured here is that the refusal is LOAD-BEARING, not that it
//! happens.** Asserting `memo_key == None` proves the line is present; it does
//! not prove anything would go wrong without it. So the twin,
//! `a_boundary_that_keys_poisons_the_memo_it_is_under`, is the same boundary
//! with ONE line changed - `memo_key` delegates to the guarded stage instead of
//! refusing - and it demonstrates the stale fallback rather than asserting it
//! cannot happen. `a_keyed_boundary_answers_identically_with_no_memo_in_front`
//! is what makes that a fair comparison: the two differ in the key and in
//! nothing else.
//!
//! **The composition is the FORBIDDEN one throughout.**
//! `libeffects::Boundary`'s rule is that a boundary goes outside the memo, and
//! the gates below put a memo outside a boundary on purpose. That is the point:
//! with `memo_key -> None` the forbidden order is merely useless - a cache with
//! nothing to say - where before it was dangerous. The prescribed order is
//! gated too (`a_boundary_outside_a_memo_caches_only_real_answers`), because
//! what it buys is the reason to keep composing that way.
//!
//! **Every type here is a stand-in** (`PIPELINE_PLAN.md`:584-589).

use std::sync::{Arc, Mutex};
use std::task::{Context, Waker};

use libeffects::{Fallback, Recover};
use libpipeline::{Guarded, Memo};
use libpipelinedata::{ContentKey, EffectPoll, MemoKey, MemoMap, MemoStore, Stage, StageId};

// ---------------------------------------------------------------- stand-ins

/// Stand-in for whatever an author wrote.
#[derive(Clone, PartialEq, Eq, Debug)]
struct Text(String);

/// Transient in the sense the memo rule cares about: it can clear.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct Boom;

/// A stage that fails until its slot is filled, registering the waker on the
/// way out so the failure CAN clear - which is the whole hazard: a fallback
/// that could never be replaced would be no worse for being cached.
struct Flaky {
    slot: Mutex<Option<&'static str>>,
    waiting: Mutex<Vec<Waker>>,
    polls: Mutex<usize>,
}

impl Flaky {
    const ID: StageId = StageId::new("test.flaky", 1);

    fn failing() -> Arc<Self> {
        Arc::new(Self {
            slot: Mutex::new(None),
            waiting: Mutex::new(Vec::new()),
            polls: Mutex::new(0),
        })
    }

    fn land(&self, value: &'static str) {
        *self.slot.lock().unwrap() = Some(value);
        for waker in self.waiting.lock().unwrap().drain(..) {
            waker.wake();
        }
    }

    /// How many times this was polled - which is how many times it RAN.
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

    /// **It keys, and keys honestly.** What refuses below is the BOUNDARY, not
    /// a stage that could not have keyed anyway.
    fn memo_key(&self, input: &Text) -> Option<MemoKey> {
        Some(MemoKey::new(Self::ID, [ContentKey::of(&input.0)]))
    }

    fn poll_stage(&self, input: &Text, cx: &mut Context<'_>) -> EffectPoll<String, Boom> {
        *self.polls.lock().unwrap() += 1;
        let Some(filled) = *self.slot.lock().unwrap() else {
            self.waiting.lock().unwrap().push(cx.waker().clone());
            return EffectPoll::Failed(Boom);
        };
        EffectPoll::Ready(format!("{}:{filled}", input.0))
    }
}

/// A shared stage is a stage. See the note in
/// `a_stage_boundary_catches_what_its_stage_raises.rs`: `Stage` has no
/// forwarding impl for `Arc<S>` and `libeffects::Effect` does.
#[derive(Clone)]
struct Shared<S>(Arc<S>);

impl<S: Stage> Stage for Shared<S> {
    type Input = S::Input;
    type Output = S::Output;
    type Error = S::Error;

    fn id(&self) -> StageId {
        self.0.id()
    }

    fn memo_key(&self, input: &Self::Input) -> Option<MemoKey> {
        self.0.memo_key(input)
    }

    fn poll_stage(
        &self,
        input: &Self::Input,
        cx: &mut Context<'_>,
    ) -> EffectPoll<Self::Output, Self::Error> {
        self.0.poll_stage(input, cx)
    }
}

/// **THE TWIN: [`Guarded`] with one line changed.**
///
/// `memo_key` delegates to the guarded stage - which is the obvious, wrong
/// thing to write, and exactly what [`Memo`] and
/// [`Tracked`](libpipeline::Tracked) DO write, because those two are
/// transparent and this is not. Everything else is `Guarded`'s: the same poll,
/// the same handler, the same id.
struct Keyed<S, H>(Guarded<S, H>);

impl<S, H> Stage for Keyed<S, H>
where
    S: Stage,
    H: Recover<S::Error, Value = S::Output>,
{
    type Input = S::Input;
    type Output = S::Output;
    type Error = H::Escalated;

    fn id(&self) -> StageId {
        self.0.id()
    }

    /// The one line. `Guarded`'s answers `None`.
    fn memo_key(&self, input: &Self::Input) -> Option<MemoKey> {
        self.0.stage().memo_key(input)
    }

    fn poll_stage(
        &self,
        input: &Self::Input,
        cx: &mut Context<'_>,
    ) -> EffectPoll<Self::Output, Self::Error> {
        self.0.poll_stage(input, cx)
    }
}

/// A store that answers exactly as [`MemoMap`] does and counts what it was
/// asked - so "neither looks up nor records" is measured at the store rather
/// than inferred from an answer.
struct Watching<V> {
    rows: MemoMap<V>,
    lookups: Mutex<usize>,
    records: Mutex<usize>,
}

impl<V> Watching<V> {
    fn new() -> Self {
        Self {
            rows: MemoMap::new(),
            lookups: Mutex::new(0),
            records: Mutex::new(0),
        }
    }

    fn lookups(&self) -> usize {
        *self.lookups.lock().unwrap()
    }

    fn records(&self) -> usize {
        *self.records.lock().unwrap()
    }
}

impl<V: Clone> MemoStore<V> for Watching<V> {
    fn lookup(&self, key: &MemoKey) -> Option<V> {
        *self.lookups.lock().unwrap() += 1;
        self.rows.lookup(key)
    }

    fn record(&self, key: &MemoKey, value: V) {
        *self.records.lock().unwrap() += 1;
        self.rows.record(key, value);
    }
}

const GUARD: StageId = StageId::new("test.guard", 1);

fn guard(flaky: &Arc<Flaky>) -> Guarded<Shared<Flaky>, Fallback<String>> {
    Guarded::new(
        GUARD,
        Shared(Arc::clone(flaky)),
        Fallback::new("fallback".to_string()),
    )
}

/// Poll once with a waker of no consequence - the offline driver's shape.
fn value_of<S: Stage<Output = String>>(stage: &S, input: &S::Input) -> String {
    match stage.poll_stage(input, &mut Context::from_waker(Waker::noop())) {
        EffectPoll::Ready(value) => value,
        EffectPoll::Pending => panic!("nothing here is pending"),
        EffectPoll::Failed(_) => panic!("this boundary catches everything"),
    }
}

// ---------------------------------------------------------------------------
// Gate 1: the refusal itself.
// ---------------------------------------------------------------------------

#[test]
fn a_boundary_refuses_to_key_even_when_its_stage_keys() {
    let flaky = Flaky::failing();
    let guarded = guard(&flaky);

    for source in ["src", "other", ""] {
        let input = Text(source.into());
        assert!(
            guarded.stage().memo_key(&input).is_some(),
            "the guarded stage keys this input",
        );
        assert_eq!(
            guarded.memo_key(&input),
            None,
            "and the boundary over it does not: whether this poll will \
             substitute is not knowable before the stage runs, so there is no \
             key that is honest in advance",
        );
    }
}

// ---------------------------------------------------------------------------
// Gate 2: the forbidden order, made harmless.
// ---------------------------------------------------------------------------

#[test]
fn a_memo_over_a_boundary_neither_looks_up_nor_records() {
    // `Memo::new(Boundary::new(..), ..)` - the composition `libeffects` forbids
    // - built on purpose. With `memo_key -> None` the store is never consulted
    // and never written, so the cache has nothing to serve back and nothing to
    // poison.
    let flaky = Flaky::failing();
    let poisonable = Memo::new(guard(&flaky), Watching::new());
    let input = Text("src".into());

    assert_eq!(value_of(&poisonable, &input), "fallback");
    assert_eq!(value_of(&poisonable, &input), "fallback");
    assert_eq!(poisonable.store().lookups(), 0, "no key, no lookup");
    assert_eq!(poisonable.store().records(), 0, "no key, no record");

    // The failure clears. The input never moved - so a key built from it never
    // moved either, and what makes the real answer reachable is that there was
    // never an entry to hit.
    flaky.land("v1");
    assert_eq!(value_of(&poisonable, &input), "src:v1");
    assert_eq!(
        value_of(&poisonable, &input),
        "src:v1",
        "and the real answer is not cached either: a stage that cannot say \
         which of its answers were substituted is not cached at all",
    );
    assert_eq!(poisonable.store().lookups(), 0);
    assert_eq!(poisonable.store().records(), 0);
}

#[test]
fn a_boundary_that_keys_poisons_the_memo_it_is_under() {
    // THE TWIN. One line different from the gate above, and it does not merely
    // fail an assertion - it demonstrates the defect.
    let flaky = Flaky::failing();
    let poisoned = Memo::new(Keyed(guard(&flaky)), Watching::new());
    let input = Text("src".into());

    assert_eq!(value_of(&poisoned, &input), "fallback");
    assert_eq!(
        poisoned.store().records(),
        1,
        "the substituted value was recorded as if it were the real answer",
    );

    let polls_before = flaky.polls();
    flaky.land("v1");
    assert_eq!(
        value_of(&poisoned, &input),
        "fallback",
        "the failure cleared and the cached fallback did not: this is the \
         poisoning, and from the value channel it is indistinguishable from a \
         correct answer",
    );
    assert_eq!(
        flaky.polls(),
        polls_before,
        "and the real value is never even computed - the memo hits first, so \
         the guarded stage is never polled again",
    );
    assert!(poisoned.store().lookups() > 0);

    // It is permanent, not slow: the input is what the key is built from, and
    // the input is what did not move.
    for _ in 0..5 {
        assert_eq!(value_of(&poisoned, &input), "fallback");
    }
}

#[test]
fn a_keyed_boundary_answers_identically_with_no_memo_in_front() {
    // What makes the twin FAIR. Without a memo the two boundaries are the same
    // boundary: same substitution, same real answer once the failure clears.
    // The key is the only difference between them, so the poisoning above is
    // attributable to the key and to nothing else.
    let sequence = |keyed: bool| {
        let flaky = Flaky::failing();
        let input = Text("src".into());
        let mut seen = Vec::new();
        if keyed {
            let stage = Keyed(guard(&flaky));
            seen.push(value_of(&stage, &input));
            flaky.land("v1");
            seen.push(value_of(&stage, &input));
        } else {
            let stage = guard(&flaky);
            seen.push(value_of(&stage, &input));
            flaky.land("v1");
            seen.push(value_of(&stage, &input));
        }
        seen
    };

    assert_eq!(sequence(true), sequence(false));
    assert_eq!(sequence(true), vec!["fallback", "src:v1"]);
}

// ---------------------------------------------------------------------------
// Gate 3: what the prescribed order buys.
// ---------------------------------------------------------------------------

#[test]
fn a_boundary_outside_a_memo_caches_only_real_answers() {
    // `Boundary::new(Memo::new(..), ..)` - the prescribed order. The memo's
    // inner stage answers `Failed`, which the memo passes through unrecorded,
    // and the substitution happens above it where no key reaches. So the store
    // holds real answers and only real answers.
    let flaky = Flaky::failing();
    let guarded = Guarded::new(
        GUARD,
        Memo::new(Shared(Arc::clone(&flaky)), Watching::new()),
        Fallback::new("fallback".to_string()),
    );
    let input = Text("src".into());

    assert_eq!(value_of(&guarded, &input), "fallback");
    assert_eq!(
        guarded.stage().store().records(),
        0,
        "a failure is not a value, so the memo has nothing to remember - and \
         the fallback was never offered to it",
    );

    flaky.land("v1");
    assert_eq!(value_of(&guarded, &input), "src:v1");
    assert_eq!(guarded.stage().store().records(), 1);

    let polls = flaky.polls();
    assert_eq!(value_of(&guarded, &input), "src:v1");
    assert_eq!(
        flaky.polls(),
        polls,
        "and THIS hit is the one worth having: the real answer, under a key \
         that moves when the input does",
    );
}
