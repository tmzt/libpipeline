//! The pipeline engine and its two drivers (`PIPELINE_PLAN.md` §5, §6).
//!
//! * [`Memo`] - the memo layer. The lookup precedes the work, only `Ready` is
//!   recorded, and the store is not consulted at all while [`revalidating`] -
//!   so a key built from a stage's arguments cannot serve a value the ledger
//!   has ruled out on account of an ambient one.
//! * [`Chain`] - two stages composed, which is itself a `Stage`, so a graph is
//!   not a second kind of thing a driver must know how to walk.
//! * [`run_to_completion`] and [`FrameDriver`] - §5's two drivers over the same
//!   graph, the same contract and the same keys.
//! * [`Ledger`], [`Tracked`], [`TrackedInput`] - §3's read-observation ledger:
//!   edges recorded by observing reads rather than declared, re-logged on every
//!   run so they follow conditionals; and invalidation over those edges, where
//!   a changed input marks its dependents stale transitively and wakes whoever
//!   subscribed.
//! * [`Schedule`] - what a driver polls next given the stale set: the stale
//!   nodes no stale node reads, since the pull revalidates the rest.
//! * [`run_to_completion_watched`] - the offline driver, reporting `Pending`
//!   polls that left no wake path. Same answers, one more observation.
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
//! **What is not here yet.** Backdating (§3's "early cutoff") exists only at
//! the leaf: [`TrackedInput::set`] refuses to invalidate on a write of an equal
//! value, but a DERIVED node that recomputes to something equal still wakes its
//! consumers, because nothing here compares a stage's output to its last one.
//! §3 wants both halves - "constructive keys give the *lookup*, backdating
//! gives the *cutoff*, and a live IDE needs both" - and the missing half needs
//! somewhere to keep the last output and an equality it can trust, which is
//! §9's step 2 (content keys) rather than more machinery here.

#![forbid(unsafe_code)]

mod chain;
mod driver;
mod memo;
mod schedule;
mod track;
mod watch;

pub use chain::{Chain, ChainError};
pub use driver::{DriveError, FrameDriver, NoPendingWork, PendingWork, run_to_completion};
pub use memo::Memo;
pub use schedule::{Cycle, Schedule};
pub use track::{Ledger, NodeId, Tracked, TrackedInput, revalidating};
pub use watch::{WakePath, WakeReport, poll_watched, run_to_completion_watched};
