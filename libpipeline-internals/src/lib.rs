//! The machinery `libpipeline`'s builder assembles, in a crate of its own.
//!
//! **This is not the door.** A consumer composes, memoizes and drives through
//! `libpipeline`'s builder; what is here is what the builder wires up on its
//! behalf. Nothing in this crate is a stable surface, and a consumer that
//! reaches for one of these types has found something the builder cannot
//! express - which `PLAN.md` records as a finding rather than a re-export.
//!
//! An inventory, not an API:
//!
//! * [`memo`] - the memo layer. The lookup precedes the work, only `Ready` is
//!   recorded, and the store is not consulted at all while
//!   [`revalidating`](track::revalidating) - so a key built from a stage's
//!   arguments cannot serve a value the ledger has ruled out on account of an
//!   ambient one.
//! * [`chain`] - two stages composed, which is itself a `Stage`, so a graph is
//!   not a second kind of thing a driver must know how to walk.
//! * [`boundary`] - the error boundary at stage level, delegating to
//!   `libeffects`' `Boundary` for the mechanism and adding the one thing that
//!   crate has no vocabulary for: a boundary refuses to be memoized, because a
//!   substituted fallback cached under a key that never moves is a permanent
//!   fallback indistinguishable from a correct answer.
//! * [`driver`] - `FrameDriver`, the single poll the facade's one door makes,
//!   and `run_to_completion`, the loop a blocking caller would otherwise write
//!   by hand: the same graph, the same contract and the same keys, and a stage
//!   cannot tell which is asking.
//! * [`watch`] - that loop again, reporting `Pending` polls that left no wake
//!   path. Same answers, one more observation.
//! * [`track`] - the read-observation ledger: edges recorded by observing
//!   reads rather than declared, re-logged on every run so they follow
//!   conditionals; invalidation over those edges; and `Backdated`, the early
//!   cutoff above the leaf.
//! * [`schedule`] - what a driver polls next given the stale set: the stale
//!   nodes no stale node reads, since the pull revalidates the rest.
//!
//! **The engine never learns a consumer's types, and the proof is the
//! manifests.** Everything here is generic over `S: Stage`; nothing matches on
//! a concrete payload type, because there is no concrete payload type in scope
//! to match on (`DESIGN.md`, "The engine stays generic"). The facade's
//! `tests/engine_stays_generic.rs` walks this manifest along with the rest of
//! the stack's.
//!
//! **Where the tests are.** Every suite over the machinery lives in this
//! crate's `tests/`, against the public surface below. That is the whole point
//! of the split: a test in `libpipeline/tests/` proves the PUBLIC API can
//! express something, and a test here admits it cannot yet.

#![forbid(unsafe_code)]

pub mod boundary;
pub mod chain;
pub mod driver;
pub mod memo;
pub mod schedule;
pub mod track;
pub mod watch;
