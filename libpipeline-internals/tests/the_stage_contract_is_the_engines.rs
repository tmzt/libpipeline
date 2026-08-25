//! The stage contract's own properties, where the contract now lives.
//!
//! These three moved from `libpipelinedata/tests/stage_without_engine.rs` when
//! `Stage` moved. That file held them under a claim that is gone - "a consumer
//! implements a stage without linking the engine" - but the properties
//! themselves are about the trait, not about which crate a consumer links, so
//! they travel with it rather than being deleted:
//!
//! * **the key precedes the work** - `memo_key` computes without polling, which
//!   is what lets a lookup precede the work rather than validate it afterwards;
//! * **a shared stage is the SAME stage** - `&S` and `Arc<S>` forward identity,
//!   key and poll rather than standing in for a second stage;
//! * **the poll answers a SHARE** - which is this wave's change, and is
//!   measured here rather than assumed: the value the engine carries between
//!   layers is `Arc<Output>` while `Output` stays the plain `T`.
//!
//! Every type below is a stand-in. The engine may never learn a consumer's
//! types, so a test that needed a real one would be evidence of a defect.

use std::sync::{Arc, Mutex};
use std::task::{Context, Waker};

use libpipeline_internals::Stage;
use libpipelinedata::{ContentKey, EffectPoll, MemoKey, StageAnswer, StageId};

/// A stand-in for an authored expression.
#[derive(Clone, PartialEq, Eq, Debug)]
struct Source(&'static str);

/// A stand-in for what lowering it produces.
#[derive(Clone, PartialEq, Eq, Debug)]
struct Lowered(Vec<String>);

/// A pure stage: `Ready` on the first poll, keyed, never `Pending` - the shape
/// most stages have.
struct Lower {
    polls: Mutex<usize>,
}

impl Lower {
    const ID: StageId = StageId::at(0);

    fn new() -> Self {
        Self {
            polls: Mutex::new(0),
        }
    }
}

impl Stage for Lower {
    type Input = Source;
    type Output = Lowered;
    type Error = &'static str;

    fn id(&self) -> StageId {
        Self::ID
    }

    fn memo_key(&self, input: &Source) -> Option<MemoKey> {
        Some(MemoKey::new(
            Self::ID,
            [ContentKey::from_u128(input.0.len() as u128)],
        ))
    }

    fn poll_stage(
        &self,
        input: &Source,
        _cx: &mut Context<'_>,
    ) -> EffectPoll<StageAnswer<Arc<Lowered>>, &'static str> {
        *self.polls.lock().unwrap() += 1;
        if input.0.is_empty() {
            return EffectPoll::Failed("nothing to lower");
        }
        StageAnswer::computed(Arc::new(Lowered(
            input.0.split('.').map(str::to_string).collect::<Vec<_>>(),
        )))
    }
}

#[test]
fn a_stage_polls_to_a_share_of_its_output() {
    let stage = Lower::new();
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);

    let out = stage.poll_stage(&Source("props.title"), &mut cx);
    assert_eq!(
        out,
        StageAnswer::computed(Arc::new(Lowered(vec![
            "props".to_string(),
            "title".to_string()
        ]))),
        "the poll answers Arc<Output> while Output stays the plain value - the \
         share is in the RETURN, which is what lets a chain say \
         `B: Stage<Input = A::Output>`",
    );
    assert_eq!(*stage.polls.lock().unwrap(), 1);

    // The typed error channel is reachable too, and is not wrapped: a failure
    // is not a value the memo holds.
    assert_eq!(
        stage.poll_stage(&Source(""), &mut cx),
        EffectPoll::Failed("nothing to lower"),
    );
}

#[test]
fn the_key_is_computable_without_running_the_stage() {
    let stage = Lower::new();
    let key = stage.memo_key(&Source("props.title")).expect("keyable");
    assert_eq!(key.stage(), Lower::ID);
    assert_eq!(key.inputs().len(), 1);
    assert_eq!(
        *stage.polls.lock().unwrap(),
        0,
        "computing a key must not run the stage - that is what lets a lookup \
         precede the work rather than validate it afterwards",
    );
}

/// **A shared stage is still the SAME stage** - `&S` and `Arc<S>` forward
/// identity, key and poll rather than standing in for a second stage.
///
/// The forwarding impls exist because a node with more than one consumer is the
/// case error boundaries are interesting in, and without them every consumer
/// must OWN its guarded node - the graph shape that cannot arise. They were
/// found missing when `Guarded` landed and two of its test files had to define
/// a local sharing newtype to work around it.
///
/// The poll count is what makes this a real check rather than a type-check: if
/// a forwarding impl duplicated the stage instead of borrowing it, two
/// consumers would each poll their own copy and the count would read 1, not 2.
#[test]
fn a_shared_stage_is_the_same_stage_and_not_a_copy_of_it() {
    let stage = Arc::new(Lower::new());
    let waker = Waker::noop();
    let mut cx = Context::from_waker(&waker);
    let input = Source("a.b.c");

    // identity and key must not change under sharing - a reference to a stage
    // is not a different stage
    assert_eq!(Stage::id(&stage), Lower::ID, "Arc must forward the id");
    assert_eq!(
        Stage::memo_key(&stage, &input),
        stage.as_ref().memo_key(&input),
        "Arc must forward the key it would answer unshared",
    );

    // two consumers, one shared node
    let one = Arc::clone(&stage);
    let two = Arc::clone(&stage);
    let a = Stage::poll_stage(&one, &input, &mut cx);
    let b = Stage::poll_stage(&two, &input, &mut cx);

    assert!(matches!(a, EffectPoll::Ready(_)));
    assert!(matches!(b, EffectPoll::Ready(_)));
    assert_eq!(
        *stage.polls.lock().unwrap(),
        2,
        "both consumers must have polled ONE stage; 1 would mean a copy was made",
    );

    // and the borrowed form agrees with the owned one
    let by_ref: &Lower = stage.as_ref();
    assert_eq!(Stage::id(&by_ref), Lower::ID);

    // the boxed form is the one the builder holds once a registration is
    // erased, and it forwards the same way
    let boxed: Box<dyn Stage<Input = Source, Output = Lowered, Error = &'static str>> =
        Box::new(Lower::new());
    assert_eq!(boxed.id(), Lower::ID);
    assert!(matches!(
        boxed.poll_stage(&input, &mut cx),
        EffectPoll::Ready(_)
    ));
}
