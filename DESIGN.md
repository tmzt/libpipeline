# libpipeline: the builder is the only door

Status: design landed 2026-08-24; implementation partial (see "State of the
implementation" at the bottom). This document separates the DESIRED PUBLIC API
from the INTERNALS on purpose: the public section is the contract consumers
and tests are allowed to touch; everything in the internals section is
machinery the builder assembles and may reorganize without notice.

## Why this document exists

`src/lib.rs` exported 23 items flat and no builder. An API that invites
assembly at each call site gets assembly at each call site: two consumers
(`crates/libhbui/src/charter.rs`'s expansion memo and
`highbay_data::elements::ReduceMemo`) hand-rolled their own memoization BESIDE
this crate rather than composing `Memo` themselves, and both are now known
defects. The same flat surface let a stage be used un-memoized (memoization
was something a caller remembered), and let a stage's version live hundreds of
lines from the behaviour it versions (`charter.rs` declares its `StageId` at
line 254 and builds the key it versions at line 1015 - 761 lines apart;
`highbay_data/src/elements.rs` declares `REDUCE` at 607 with its door at 825).
A stale version makes the memo serve a value computed by the OLD behaviour
under a key claiming to be current, and nothing downstream can detect that.

Three decisions follow, and the rest of this file is their consequences:

1. **The builder is the only public way to compose, memoize or drive.**
   `Stage` stays public to IMPLEMENT - a consumer must be able to write one -
   but not to compose, memoize or drive by hand.
2. **Memoization is intrinsic to registration.** There is no un-memoized
   `add_stage`. A stage that must not be served from cache says so through
   `memo_key -> None` (the vocabulary that already exists for exactly this),
   not by a caller forgetting a wrapper.
3. **The version is declared at the registration call site.** `stage(name,
   version, |id| ...)` puts the number in the same lexical scope as the
   closure that constructs the behaviour it versions. Most of this code is
   written by LLM agents, which do not scroll to a module-level `const` to ask
   whether it should move; the invariant has to be in the way, not remembered.

## Public API (the desired end state)

### The builder

```rust
use libpipeline::PipelineBuilder;

let pipeline = PipelineBuilder::new()
    .stage("parse", 3, |id| Parse::new(id))
    .stage("lower", 1, |id| Lower::new(id))
    .build();
```

* `PipelineBuilder::new()` - the empty builder. (Not `Pipeline::builder()`:
  `Pipeline` is generic over the graph, so an associated constructor there
  would demand a type the caller cannot yet name.)
* `.stage(name, version, make)` - register one stage. `make: FnOnce(StageId)
  -> S` receives the id the builder minted from `(name, version)`; the stage
  stores it and answers it from `Stage::id`. **The builder checks
  `stage.id() == id` at registration and panics on mismatch** - a stage
  registered under a version its keys do not carry is exactly the stale-memo
  defect this design exists to close, so it fails at construction, not at
  poll time. Each registered stage is memoized in a store the builder owns
  (fresh `MemoMap` per stage); this is why `S::Output: Clone` is required.
  Stages chain: the Nth stage's `Input` must equal the (N-1)th's `Output`.
* `.stage_in(name, version, store, make)` - same, but memoized in a store the
  CALLER provides (`impl MemoStore<S::Output>`). This is how a cache outlives
  one build of the pipeline (an IDE session rebuilding its graph), and it is
  what makes the version rule observable in a test: share a store across two
  builds, bump the version, and the second build must recompute.
* `.uncached()` - the control switch: every stage runs with a store that
  remembers nothing. Kept public because "a pipeline whose ANSWERS change when
  the cache is disabled has a bug the cache was hiding" is a property
  consumers should be able to assert about their own graphs.
* `.build() -> Pipeline<impl Stage<...>>` - the runner. The graph type is
  opaque (`impl Stage`); consumers hold it by inference and can never reach
  the machinery inside.

### The runner

`Pipeline<S>` is generic over the opaque graph and exposes the two drive
modes of PIPELINE_PLAN.md section 5 - same graph, same keys:

* `.run(&input, &work) -> Result<Output, DriveError<Error>>` - the blocking
  (offline/CLI) drive. `work: impl PendingWork` pumps whatever a `Pending`
  poll is waiting for.
* `.run_pure(&input)` - `run` with `NoPendingWork`, for graphs of pure stages.
* `.run_watched(&input, &work) -> (Result<...>, WakeReport)` - the same drive,
  reporting `Pending` polls that left no wake path (each one a value a frame
  driver would lose rather than receive late).
* `.poll_frame(&input) -> EffectPoll<Output, Error>` - the real-time drive:
  one poll, returns immediately, never blocks.
* `.take_stale() -> bool` - whether a wake arrived since last asked ("stale,
  poll again"); reading clears it.
* `.waker() -> Waker` - for landing values out of band.

### What else stays public, and why

* **`Stage` and its contract types** (`Stage`, `StageId`, `MemoKey`,
  `ContentKey`, `EffectPoll`, `MemoStore`, `MemoMap`, `NoMemo`) - these live
  in `libpipelinedata`, not here; a stage AUTHOR needs them and they are the
  implement-side contract, not composition machinery.
* **`PendingWork` / `NoPendingWork`** - the executor seam. Which executor (and
  whether there is one) is the caller's decision; the engine may not link one.
* **`DriveError`** - what `run` answers with; `Stalled` is a real end state.
* **`ChainError`** - result VOCABULARY, not machinery: the runner's error type
  for a multi-stage pipeline is nested `ChainError`, and a caller matching on
  which stage failed needs the name. (`Chain` itself - the type that builds
  such graphs - is internal.)
* **`WakePath` / `WakeReport`** - the watched drive's findings.

### What tests may touch

Only the above. A test that needs anything from the internals section is a
FINDING that the builder cannot express something a consumer will need; record
it here, do not re-export.

## Internals (assembled by the builder, never exported)

* **`Chain`** (`src/chain.rs`) - two stages composed, itself a `Stage`. The
  builder nests these; the composite id is a fixed internal `StageId` because
  a chain never keys (`memo_key -> None`, its parts are memoized instead).
* **`Memo`** (`src/memo.rs`) - the memo layer: lookup precedes the work, only
  `Ready` recorded, store skipped while `revalidating`. Merged into
  registration: every `stage()` call wraps its stage in one. Its module doc
  already argued this direction for the revalidation check ("not a
  constructor argument, a builder flag or a trait bound the author supplies");
  this design applies the same rule to memoization itself.
* **`FrameDriver`** (`src/driver.rs`) - held inside `Pipeline`, surfaced as
  `poll_frame`/`take_stale`/`waker`.
* **`run_to_completion`**, **`run_to_completion_watched`**, **`poll_watched`**
  (`src/driver.rs`, `src/watch.rs`) - surfaced as `run`/`run_watched`.
* **`BuilderStore`** (`src/builder.rs`) - the store the builder wraps around
  each stage: owned map / caller-given / off (the `.uncached()` control).
* **The tracked layer** (`src/track.rs`: `Ledger`, `Tracked`, `TrackedInput`,
  `Backdated`, `NodeId`, `revalidating`) and **`Schedule`/`Cycle`**
  (`src/schedule.rs`) - internal IN INTENT, but still exported today because
  the builder cannot yet express them (see findings).
* **`Guarded`/`Substitutions`/`run_to_completion_counted`**
  (`src/boundary.rs`) - same status as the tracked layer.

## What the builder cannot yet express (findings, in priority order)

1. **Tracked state graphs.** `Ledger`/`Tracked`/`TrackedInput` composition -
   including the load-bearing order "wrap the memo in the tracking, not the
   tracking in the memo" - is exactly the kind of assembly the builder exists
   to own, and it is not in the builder yet. Sketch: `.tracked_input(label,
   value)` and a `stage` variant taking a ledger label, with the builder
   owning the ledger and the wrap order so the known-bad composition becomes
   unwritable. Until then `Ledger` et al stay exported and the five tests
   over them stay on internals.
2. **Error boundaries.** `Guarded` placement ("outside the tracking") and the
   substitution tally are caller assembly today. Sketch: `.guarded_stage(name,
   version, handler, make)` plus `Pipeline::substitutions()`.
3. **Non-linear graphs.** The builder builds chains. A diamond (one producer,
   two consumers, one joiner) exists in the engine via `Arc<S>: Stage` but has
   no builder spelling.
4. **Scheduling.** `Ledger::schedule` has no builder-level door; it rides on
   finding 1.
5. **Store lifecycle.** `.stage_in` lets a cache outlive a build, but there is
   no whole-pipeline store policy (a single backend serving every stage needs
   per-output-type stores; the seam is per-stage on purpose, but a factory
   hook may be wanted).

## Migration plan and test disposition

This wave lands the builder beside the flat exports; the flip that deletes the
flat exports happens together with the consumer conversion (a separate wave,
per instruction). The workspace consumers to convert then: `charter.rs`'s
hand-rolled expansion memo and `highbay_data::elements::ReduceMemo`, both of
which are linear memoized chains the builder already expresses.

Existing tests, one line each (all still pass; "internals" means it imports
soon-internal items and must be converted or its property re-held before the
flip):

* `two_drivers_one_graph.rs` - internals (`Chain`, `Memo`, drivers). Its
  headline property (a pending stage that registers no waker is a value lost
  rather than late) is RE-HELD through the builder in
  `tests/builder_is_the_only_door.rs`; the rest is convertible with `.stage`
  + `run`/`poll_frame`.
* `invalidation_marks_dependents.rs`,
  `an_equal_recompute_stops_at_its_node.rs`, `reads_become_edges.rs`,
  `the_schedule_polls_each_node_once.rs`, `a_fallback_is_not_a_revalidation.rs`
  - BLOCKED on finding 1 (tracked layer has no builder spelling).
* `a_boundary_is_not_a_cacheable_answer.rs`,
  `a_stage_boundary_catches_what_its_stage_raises.rs`,
  `a_build_can_ask_whether_it_stood_on_a_fallback.rs` - BLOCKED on finding 2
  (boundaries have no builder spelling).
* `an_unwakeable_poll_is_visible_offline.rs` - convertible (`run_watched`
  exists on the runner); not yet converted.
* `engine_stays_generic.rs` - manifest walk, touches no API; keep as is.

## State of the implementation

DONE this wave: `src/builder.rs` (builder, runner, id check at registration,
intrinsic memoization with owned/given/off stores), exported from `lib.rs`;
`tests/builder_is_the_only_door.rs` exercising composition, first-level memo
hits, the version-bump-cold-cache rule via a shared store, the registration
panic, the uncached control, and the two-drivers/lost-value property - through
the public builder API only.

NOT done: the visibility flip (flat exports still present so the existing
suite passes); conversion of the convertible tests; findings 1-5.
