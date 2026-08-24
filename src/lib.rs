//! The pipeline engine and its two drivers (`PIPELINE_PLAN.md` §5, §6).
//!
//! * [`Memo`] - the memo layer. The lookup precedes the work, only `Ready` is
//!   recorded, and the store is not consulted at all while [`revalidating`] -
//!   so a key built from a stage's arguments cannot serve a value the ledger
//!   has ruled out on account of an ambient one.
//! * [`Chain`] - two stages composed, which is itself a `Stage`, so a graph is
//!   not a second kind of thing a driver must know how to walk.
//! * [`Guarded`] - §7's error boundary at stage level, delegating to
//!   `libeffects`' `Boundary` for the mechanism and adding the one thing that
//!   crate has no vocabulary for: a boundary refuses to be memoized, because a
//!   substituted fallback cached under a key that never moves is a permanent
//!   fallback indistinguishable from a correct answer.
//! * [`run_to_completion`] and [`FrameDriver`] - §5's two drivers over the same
//!   graph, the same contract and the same keys.
//! * [`Ledger`], [`Tracked`], [`TrackedInput`] - §3's read-observation ledger:
//!   edges recorded by observing reads rather than declared, re-logged on every
//!   run so they follow conditionals; and invalidation over those edges, where
//!   a changed input marks its dependents stale transitively and wakes whoever
//!   subscribed.
//! * [`Backdated`] - §3's early cutoff above the leaf: a node whose output
//!   addresses to what it addressed to last time retracts itself as a reason
//!   for its consumers to re-run.
//! * [`Schedule`] - what a driver polls next given the stale set: the stale
//!   nodes no stale node reads, since the pull revalidates the rest.
//! * [`run_to_completion_watched`] - the offline driver, reporting `Pending`
//!   polls that left no wake path. Same answers, one more observation.
//! * [`run_to_completion_counted`] - the same drive again, reporting how many
//!   of its answers a boundary SUBSTITUTED, which is what separates a build
//!   from a build that silently shipped fallbacks.
//!
//! **The engine never learns an IR, and the proof is the manifests.**
//! Everything here is generic over `S: Stage`; nothing matches on a concrete
//! expression type, because there is no concrete expression type in scope to
//! match on (`PIPELINE_PLAN.md`:579-583). `tests/engine_stays_generic.rs` walks
//! the stack's manifests - this crate's and, through its path dependencies,
//! every crate under it - and fails if `libtsx` or a Highbay crate appears
//! anywhere in the tree (§6:591-604). That check is TRANSITIVE as of
//! `52b6562`, which moved `PipelineExpr` into `highbay_data` and so left
//! nothing in the stack depending on `libtsx` at any step; the earlier
//! direct-edge check was sufficient, since a crate reachable only transitively
//! cannot be named in a `use`, but a rule that holds transitively cannot be
//! evaded by routing an edge through a sibling.
//!
//! **What is not here yet.** [`Backdated`] cuts off where a node's output
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
//! [`TrackedInput::get`] rather than `peek` or the ledger sees nothing to defer
//! to; and a boundary belongs OUTSIDE the tracking ([`Guarded`]'s doc), because
//! a substituted `Ready` tells the ledger the node is up to date when it is
//! still owed its real answer.
//!
//! The fourth of that family is no longer stated: a boundary belongs outside
//! the MEMO, which [`Guarded`]'s `memo_key` closes structurally by refusing to
//! key at all.

#![forbid(unsafe_code)]

mod boundary;
mod builder;
mod chain;
mod driver;
mod memo;
mod schedule;
mod track;
mod watch;

/// The public door. See `DESIGN.md`: composition, memoization and driving go
/// through the builder; `Stage` stays public to IMPLEMENT (via
/// `libpipelinedata`) but not to assemble by hand.
pub use builder::{Pipeline, PipelineBuilder, StagedPipelineBuilder};

// Result and report vocabulary - named in the runner's signatures, so a
// caller matching on them needs the names (DESIGN.md, "What else stays
// public").
pub use chain::ChainError;
pub use driver::{DriveError, NoPendingWork, PendingWork};
pub use watch::{WakePath, WakeReport};

// SCHEDULED FOR REMOVAL (DESIGN.md, "Migration plan"): the flat assembly
// surface, kept exported only until the tracked/boundary layers have builder
// spellings and the consumers plus the tests over them are converted. New
// code composes through `Pipeline::builder()`, never through these.
pub use boundary::{Guarded, Substitutions, run_to_completion_counted};
pub use chain::Chain;
pub use driver::{FrameDriver, run_to_completion};
pub use memo::Memo;
pub use schedule::{Cycle, Schedule};
pub use track::{Backdated, Ledger, NodeId, Tracked, TrackedInput, revalidating};
pub use watch::{poll_watched, run_to_completion_watched};
