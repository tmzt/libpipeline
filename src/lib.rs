#![doc = include_str!("../README.md")]
//!
//! # Inside the crate (an inventory, not an API)
//!
//! * `Stage` - the contract every layer below composes, and the thing the
//!   builder constructs from a registration's two functions. It moved here
//!   from `libpipelinedata` when the door became a `fn` door: with nothing
//!   outside the engine implementing it, it is machinery like the rest of this
//!   list.
//! * `Memo` - the memo layer. The lookup precedes the work, only `Ready` is
//!   recorded, and the store is not consulted at all while `revalidating` -
//!   so a key built from a stage's arguments cannot serve a value the ledger
//!   has ruled out on account of an ambient one.
//! * `Chain` - two stages composed, which is itself a `Stage`, so a graph is
//!   not a second kind of thing a driver must know how to walk.
//! * `Guarded` - the error boundary at stage level, delegating to
//!   `libeffects`' `Boundary` for the mechanism and adding the one thing that
//!   crate has no vocabulary for: a boundary refuses to be memoized, because a
//!   substituted fallback cached under a key that never moves is a permanent
//!   fallback indistinguishable from a correct answer.
//! * `FrameDriver` and `run_to_completion` - the single poll the one door
//!   makes, and the loop a blocking caller would otherwise write by hand,
//!   over the same graph, the same contract and the same keys.
//! * `Ledger`, `Tracked`, `TrackedInput` - the read-observation ledger:
//!   edges recorded by observing reads rather than declared, re-logged on every
//!   run so they follow conditionals; and invalidation over those edges, where
//!   a changed input marks its dependents stale transitively and wakes whoever
//!   subscribed.
//! * `Backdated` - early cutoff above the leaf: a node whose output
//!   addresses to what it addressed to last time retracts itself as a reason
//!   for its consumers to re-run.
//! * `Schedule` - what a driver polls next given the stale set: the stale
//!   nodes no stale node reads, since the pull revalidates the rest.
//! * `run_to_completion_watched` - the blocking driver, reporting `Pending`
//!   polls that left no wake path. Same answers, one more observation.
//! * `run_to_completion_counted` - the same drive again, reporting how many
//!   of its answers a boundary SUBSTITUTED, which is what separates a build
//!   from a build that silently shipped fallbacks.
//!
//! **The engine never learns a consumer's types, and the proof is the
//! manifests.** Everything here is generic over `S: Stage`; nothing matches on
//! a concrete payload type, because there is no concrete payload type in scope
//! to match on (`DESIGN.md`, "The engine stays generic").
//! `tests/engine_stays_generic.rs` walks the stack's manifests - this crate's
//! and, through its path dependencies, every crate under it - and fails if a
//! crate outside the stack's own closed allowlist appears ANYWHERE in the
//! tree. The check is TRANSITIVE because nothing in the stack reaches outside
//! the allowlist at any step, and a rule that holds transitively cannot be
//! evaded by routing an edge through a sibling.
//!
//! **What is not here yet.** `Backdated` cuts off where a node's output
//! REPEATS, which needs the node to have run. A node whose consumers could be
//! spared before it runs at all - salsa's deep verify, where an unchanged
//! dependency set is enough - is not here, and neither is any policy for which
//! nodes are worth addressing on every poll: `Backdated` is opt-in per node
//! because the address costs a traversal of the output, and a chain that
//! backdates at every level pays for it at every level.
//!
//! **The seams that are stated rather than enforced.** Three rules hold by
//! composition and cannot be checked from inside the type that needs them, so
//! each is stated in a doc and pinned by a known-bad twin: a cache belongs
//! INSIDE the tracking (`Memo`'s doc); a stage's tracked reads must go through
//! `TrackedInput::get` rather than `peek` or the ledger sees nothing to defer
//! to; and a boundary belongs OUTSIDE the tracking (`Guarded`'s doc), because
//! a substituted `Ready` tells the ledger the node is up to date when it is
//! still owed its real answer.
//!
//! The fourth of that family is no longer stated: a boundary belongs outside
//! the MEMO, which `Guarded`'s `memo_key` closes structurally by refusing to
//! key at all.
//!
//! **Read the list above as an inventory, not as an API.** Every item named in
//! it lives in `libpipeline-internals` and is reached only by this crate. The
//! whole of what this crate exports is [`PipelineBuilder`], [`Pipeline`],
//! [`Run`], [`Failure`], the [`RunResult`] alias over the last two, [`Ctx`] and
//! [`run_blocking`]. `DESIGN.md` says why, and `PLAN.md` says what to do when a
//! consumer needs one of the internals: that is a FINDING about the builder's
//! reach, recorded there, not a re-export.
//!
//! [`Ctx`] and [`run_blocking`] are the two additions the door flip forced. A
//! `fn` door's signature must name what it hands the function, so `Ctx` is
//! public because the registration types say so; `run_blocking` is the
//! blocking caller's loop, which is the same few lines in every caller and is
//! a loop over [`Pipeline::poll`] rather than a second way into the engine.
//!
//! [`StagedPipelineBuilder`] is exported beside them and is not a further thing
//! to learn: it is what `.stage_fn()` hands back, its fields are private and it
//! has no constructor, so a consumer receives one, calls a method on it, and
//! with method chaining never writes its name. `Failure` is the same category -
//! public type, private fields, private `new` - and the two are the pattern
//! rather than exceptions to it.

#![forbid(unsafe_code)]

mod builder;

/// **The public door, and the whole of it.** See `DESIGN.md`: composition,
/// memoization and driving go through the builder, and a stage is registered
/// as two `fn` pointers rather than implemented as a trait - so there is no
/// implement-side contract in this list either.
///
/// Everything the builder assembles - the chain, the memo layer, the frame
/// driver, the drive functions, the tracked layer, the boundary layer, the
/// scheduler and the builder's own store - is `libpipeline-internals`', and a
/// consumer that takes only this crate's manifest edge cannot name it. A
/// consumer that needs one is not missing an export; it has found something the
/// builder cannot express, which `PLAN.md` records as a finding.
// NOTHING IS RE-EXPORTED FROM THE INTERNALS, and that is now checkable rather
// than intended. Six names were: `ChainError` went when a join stopped
// retyping, and `DriveError`, `PendingWork`, `NoPendingWork`, `WakePath` and
// `WakeReport` went with the four doors - every one of them was here because
// some door's SIGNATURE named it, and with one door that returns `RunResult`
// no signature does. A consumer that finds itself needing one has found
// something the builder cannot express (`PLAN.md`'s findings), not an export
// this list forgot.
pub use builder::{
    Ctx, Failure, Pipeline, PipelineBuilder, Run, RunResult, StagedPipelineBuilder, run_blocking,
};
