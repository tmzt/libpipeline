//! The stage contract: the engine's own trait, and the shape every layer here
//! composes.
//!
//! **It is INTERNAL, and that is the change this module records.** The contract
//! lived in `libpipelinedata` while a consumer implemented it directly, whose
//! stated purpose was letting a crate declare a stage without linking the
//! engine. With registration taking a `fn` pointer
//! (`libpipeline`'s `PipelineBuilder::stage_fn`) no consumer implements it: the
//! builder is the only thing that ever constructs one, from the two functions a
//! registration hands it. So the trait is machinery, and machinery lives here.
//!
//! What a consumer still writes is the pair of functions - a key and a poll -
//! and the vocabulary those name (`EffectPoll`, `MemoKey`, `ContentKey`) is
//! `libpipelinedata`'s still.

use std::sync::Arc;
use std::task::Context;

use libeffects::{Effect, EffectPoll};
use libpipelinedata::{MemoKey, StageAnswer, StageId};

/// One step of a pipeline.
///
/// **The contract is `libeffects`' protocol, not a parallel one.** A stage
/// answers [`EffectPoll`] and is handed a [`Context`] exactly as an
/// [`Effect`] is, and [`BoundStage`] makes that literal: bind a stage to an
/// input and what you have IS an `Effect`. A stage is a function-shaped effect,
/// with an input in front.
///
/// **Most stages never answer `Pending`.** A pure lowering step returns `Ready`
/// on the first poll; `Pending` exists for the effectful ones, and the protocol
/// is uniform so that a driver does not need to know which kind it is holding.
/// A pipeline of pure stages that ends in one effectful one is `Pending` all
/// the way up, and that costs the pure stages nothing.
///
/// **The output is a SHARE, and the associated type is the value.** The memo
/// keeps what it answers with - that is what a memo is - so what travels
/// between layers is `Arc<Self::Output>`, while `Self::Output` stays the plain
/// `T`. Putting the share in the poll's return rather than in the associated
/// type is what lets [`Chain`](crate::chain::Chain) say
/// `B: Stage<Input = A::Output>` instead of `A: Stage<Output = Arc<B::Input>>`,
/// and it is why no adapter exists to unwrap between two stages: the chain
/// dereferences the share it already holds.
///
/// **A `Ready` says WHICH answer** ([`StageAnswer`]): the new value, or that the
/// one this position last gave still stands. `Unchanged` carries nothing, so a
/// stage answering it hands its consumer no input to be polled over - which is
/// the whole saving, and why the variant lives in the `Ready` channel rather
/// than beside it. [`StageAnswer`]'s doc carries the two spellings that were
/// rejected. `BoundStage` below still implements [`Effect`] over it, which is
/// what keeps "the stage contract IS the effect protocol" a claim the compiler
/// checks.
///
/// **`&self`.** A stage is a description of work, shareable and re-pollable;
/// anything it must remember across polls goes through safe interior mutability
/// (`CLAUDE.md`).
pub trait Stage {
    /// What the stage consumes. Deliberately an associated type: the engine is
    /// generic over every payload type and may never name one.
    type Input;
    /// What the stage produces - the VALUE, not the share the poll answers
    /// with.
    type Output;
    /// The typed error channel.
    type Error;

    /// This stage's identity: the position a builder minted for it. See
    /// [`StageId`] - a stage answers the id it was handed and never invents
    /// one.
    fn id(&self) -> StageId;

    /// The memo key for this input, computed WITHOUT running the stage.
    ///
    /// `None` means "this input cannot be cheaply keyed, so do not memoize by
    /// lookup". It is `Option` rather than absent because the alternative is
    /// worse than a missing optimization: a stage that invents a key for an
    /// input it cannot address will serve a stale value under a key that says
    /// it is fresh, and nothing downstream can detect that. Refusing to key is
    /// the safe answer; faking one is not.
    fn memo_key(&self, input: &Self::Input) -> Option<MemoKey>;

    /// Poll for the current output given `input`.
    ///
    /// Same rules as [`Effect::poll_effect`]: answering `Pending` obliges the
    /// implementation to arrange a wake on `cx`'s waker, and `Ready` is "here
    /// is the current value", not "finished".
    fn poll_stage(
        &self,
        input: &Self::Input,
        cx: &mut Context<'_>,
    ) -> EffectPoll<StageAnswer<Arc<Self::Output>>, Self::Error>;
}

/// A shared stage is still a stage.
///
/// The mirror of [`Effect for &T`](libeffects::Effect), and the argument
/// transfers word for word: a node with more than one consumer is the case
/// error boundaries are interesting in, and without an impl like this one every
/// consumer must OWN its guarded node - precisely the graph shape that cannot
/// arise. Found missing when `Guarded` landed and two of its test files had to
/// define a local sharing newtype to work around it.
///
/// `id` and `memo_key` forward too: a reference to a stage is not a different
/// stage, so it must not answer with a different identity or key.
impl<T: Stage + ?Sized> Stage for &T {
    type Input = T::Input;
    type Output = T::Output;
    type Error = T::Error;

    fn id(&self) -> StageId {
        (**self).id()
    }

    fn memo_key(&self, input: &Self::Input) -> Option<MemoKey> {
        (**self).memo_key(input)
    }

    fn poll_stage(
        &self,
        input: &Self::Input,
        cx: &mut Context<'_>,
    ) -> EffectPoll<StageAnswer<Arc<Self::Output>>, Self::Error> {
        (**self).poll_stage(input, cx)
    }
}

/// The owning form of the impl above - what a real graph shares a node with.
impl<T: Stage + ?Sized> Stage for Arc<T> {
    type Input = T::Input;
    type Output = T::Output;
    type Error = T::Error;

    fn id(&self) -> StageId {
        (**self).id()
    }

    fn memo_key(&self, input: &Self::Input) -> Option<MemoKey> {
        (**self).memo_key(input)
    }

    fn poll_stage(
        &self,
        input: &Self::Input,
        cx: &mut Context<'_>,
    ) -> EffectPoll<StageAnswer<Arc<Self::Output>>, Self::Error> {
        (**self).poll_stage(input, cx)
    }
}

/// The boxed form - what the builder holds once a registration is erased.
///
/// The facade's builder keeps its graph as a `Box<dyn Stage<..>>` so that no
/// public signature names this trait (`DESIGN.md`, "Public API"). One indirect
/// call per stage per poll is what that costs, against a stage that is only
/// polled at all when its memo missed.
impl<T: Stage + ?Sized> Stage for Box<T> {
    type Input = T::Input;
    type Output = T::Output;
    type Error = T::Error;

    fn id(&self) -> StageId {
        (**self).id()
    }

    fn memo_key(&self, input: &Self::Input) -> Option<MemoKey> {
        (**self).memo_key(input)
    }

    fn poll_stage(
        &self,
        input: &Self::Input,
        cx: &mut Context<'_>,
    ) -> EffectPoll<StageAnswer<Arc<Self::Output>>, Self::Error> {
        (**self).poll_stage(input, cx)
    }
}

/// A stage with its input bound - which is an [`Effect`].
///
/// This exists to make one claim checkable by the compiler rather than by
/// reading: the stage contract and the effect protocol are the same protocol.
/// If they ever drift, this type stops compiling.
pub struct BoundStage<'a, S: Stage> {
    stage: &'a S,
    input: &'a S::Input,
}

impl<'a, S: Stage> BoundStage<'a, S> {
    /// Bind `input` to `stage`. Runs nothing - the result is a description,
    /// pollable later, in the sense [`Dormant`](libeffects::Dormant) means it.
    pub fn new(stage: &'a S, input: &'a S::Input) -> Self {
        Self { stage, input }
    }

    /// The memo key of the bound pair, if it has one.
    pub fn memo_key(&self) -> Option<MemoKey> {
        self.stage.memo_key(self.input)
    }
}

impl<S: Stage> Effect for BoundStage<'_, S> {
    type Output = StageAnswer<Arc<S::Output>>;
    type Error = S::Error;

    fn poll_effect(&self, cx: &mut Context<'_>) -> EffectPoll<Self::Output, Self::Error> {
        self.stage.poll_stage(self.input, cx)
    }
}
