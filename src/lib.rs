//! The pipeline engine and its two drivers (`PIPELINE_PLAN.md` §5, §6).
//!
//! * [`Memo`] - the memo layer. The lookup precedes the work, and only `Ready`
//!   is recorded.
//! * [`Chain`] - two stages composed, which is itself a `Stage`, so a graph is
//!   not a second kind of thing a driver must know how to walk.
//! * [`run_to_completion`] and [`FrameDriver`] - §5's two drivers over the same
//!   graph, the same contract and the same keys.
//! * [`Ledger`], [`Tracked`], [`TrackedInput`] - §3's read-observation ledger:
//!   edges recorded by observing reads rather than declared, re-logged on every
//!   run so they follow conditionals.
//!
//! **The engine never learns an IR, and the proof is the manifest.** Everything
//! here is generic over `S: Stage`; nothing matches on a concrete expression
//! type, because there is no concrete expression type in scope to match on
//! (`PIPELINE_PLAN.md`:558-568). `tests/engine_stays_generic.rs` reads
//! `Cargo.toml` and fails if `libtsx` or a Highbay crate ever appears there,
//! and Rust makes that check sufficient rather than merely suggestive: a crate
//! reachable only transitively cannot be named in a `use`, so an engine that
//! wanted to match on a real IR would have to add the edge in the open. That
//! matters for a reason already visible in the plan - §6b:698-706 has
//! `libpipelinedata` depending on `libtsx` from step 6 onward, so from then on
//! `libtsx` IS transitively present here and only the direct-edge check
//! distinguishes "linked through the data crate" from "known to the engine".
//!
//! **What is not here yet.** §6 assigns this crate three things beyond the
//! memo: the read-observation ledger, invalidation, and scheduling. The ledger
//! is here ([`track`](self::Ledger)); invalidation and scheduling are not. Until
//! they are, an observed read is a recorded edge and nothing more - staleness
//! still reaches a driver only because a stage registered the waker it was
//! handed, which is enough for a graph whose effects know their own consumers
//! and not enough for the live IDE §3 describes.

#![forbid(unsafe_code)]

mod chain;
mod driver;
mod memo;
mod track;

pub use chain::{Chain, ChainError};
pub use driver::{DriveError, FrameDriver, NoPendingWork, PendingWork, run_to_completion};
pub use memo::Memo;
pub use track::{Ledger, NodeId, Tracked, TrackedInput};
