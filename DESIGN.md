# libpipeline: the builder is the only door

Status: design landed 2026-08-24; **the flip landed 2026-08-24** - the flat
exports are gone and the builder is the only door (see "State of the
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

`WakePath` is the one item on that list no public signature names, and it is
worth saying so rather than letting a reader assume otherwise: the runner's
watched door is `run_watched`, which reports a `WakeReport` (counts). It stays
exported as that report's vocabulary. The tests that read a per-poll path are
`poll_watched`'s, and they are unit tests in `src/watch.rs` for exactly that
reason - see finding 6.

### What tests may touch

Only the above. A test that needs anything from the internals section is a
FINDING that the builder cannot express something a consumer will need; record
it here, do not re-export.

**Which makes the count of tests in `tests/` the measurement.** It is what the
public API can reach: 29 tests in 4 binaries as of the flip, against 72 unit
tests in `src/` that admit it cannot. When a finding closes, its tests migrate
OUTWARD and that first number goes up. A test moved inward is placed in the
MODULE THAT OWNS THE INTERNAL it reaches for - not in one collecting `mod
tests` at the crate root - so it moves with that module when the module is
reshaped, and so that reaching an internal stays local and visible rather than
having a sanctioned home.

## Internals (assembled by the builder, never exported)

All of the below is `pub(crate)` as of the flip. `boundary.rs`, `schedule.rs`,
`track.rs`, `chain.rs` and `memo.rs` each carry
`#![cfg_attr(not(test), allow(dead_code))]`: with the exports gone, the parts
the builder has no spelling for have no caller but their own tests, and the
`not(test)` form keeps the lint fully armed under `cargo test` so anything that
becomes genuinely unused still fails the gate. The allow comes off when the
builder becomes the caller.

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
  (`src/schedule.rs`) - internal, and now private, even though the builder
  cannot yet express them (findings 1 and 4). The flip was not held hostage to
  the findings: the tests over these layers moved into `src/track.rs` and
  `src/schedule.rs` instead.
* **`Guarded`/`Substitutions`/`run_to_completion_counted`**
  (`src/boundary.rs`) - same, with their tests in `src/boundary.rs`
  (finding 2).
* **`poll_watched`** (`src/watch.rs`) - the watched SINGLE poll. `run_watched`
  is the public watched drive; there is no public single-poll counterpart
  (finding 6).

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
6. **A watched single poll.** `Pipeline::run_watched` reports a `WakeReport`
   over a whole DRIVE; `Pipeline::poll_frame` is unwatched. Nothing public
   answers "what did THIS poll leave behind", so `WakePath` - a public type -
   is named by no public signature, and the six tests that read one are
   `src/watch.rs`'s. Sketch: `Pipeline::poll_frame_watched(&input) ->
   (EffectPoll<..>, Option<WakePath>)`.
7. **A consumer's stage cannot be registered from the crate that declares it.**
   `highbay_elements` deliberately does not link `libpipeline`, in
   dependencies OR dev-dependencies (its `Cargo.toml` states the rule and the
   reason). So `ExpandStage` and `ExpandDefinitionStage` have no
   `PipelineBuilder::stage` call site anywhere in the workspace, and their
   tests construct them with `StageId::new(NAME, version)` at the call site
   instead - the same shape and the same placement of the version, without the
   builder's registration check behind it. Not a defect in the builder; a note
   that "the version lives at the registration site" is only as strong as
   there BEING one.

## Migration plan and test disposition

**Done.** The consumer conversion and the flip landed together, as planned.

The three workspace `Stage` implementors now carry the id the builder mints and
answer it from `Stage::id`; their module-level `StageId` consts are gone,
replaced by `*_STAGE_NAME: &str` (the name half, which is not a version and
cannot go stale), and the version is written at the construction call site:

| stage | file | where its version now lives |
|---|---|---|
| `AssembleStage` | `crates/highbay_data/src/pipeline.rs` | `assemble_pipeline` in `crates/highbay_data/tests/assemble_under_the_driver.rs` - a real `PipelineBuilder::stage` call |
| `ExpandStage` | `crates/highbay_elements/src/pipeline.rs` | `expand_stage()` in that crate's two stage tests - see finding 7 |
| `ExpandDefinitionStage` | same | `definition_stage()`, likewise |

The keys did not move: each `memo_key` now reads `self.id`, which is the same
`StageId` the deleted const held, so a converted consumer hits exactly where it
hit before (`one_memo_serves_both_drivers_and_the_stage_runs_once` still
measures one poll across two drives).

Test disposition, as it landed - 100 tests before, 101 after (the extra is one
relocated in from a consumer), and no assertion dropped:

* `builder_is_the_only_door.rs` (8) - unchanged, in `tests/`.
* `engine_stays_generic.rs` (7) - unchanged, in `tests/`.
* `two_drivers_one_graph.rs` (11) - CONVERTED to the builder, in `tests/`.
  `Chain`/`Memo`/the drivers are gone from it; `MapStore` reaches the graph
  through `.stage_in`, so the "a store is implementable from outside" seam is
  still exercised through the public door. The two-stage failure tag is
  `ChainError`, which is why that type stays public.
* `an_unwakeable_poll_is_visible_offline.rs` (9) - SPLIT. The three drive-level
  tests converted to `Pipeline::run_watched` and stayed in `tests/`; the six
  that read a per-poll `WakePath` moved to `src/watch.rs` (finding 6).
* `invalidation_marks_dependents.rs` (13), `an_equal_recompute_stops_at_its_node.rs`
  (7), `reads_become_edges.rs` (9), `a_fallback_is_not_a_revalidation.rs` (8) -
  MOVED to `src/track.rs` (finding 1). The last is placed there rather than in
  `boundary.rs` because what it pins is `Tracked`'s handling of a `Failed`
  poll; the boundary is the thing that makes that state reachable.
* `the_schedule_polls_each_node_once.rs` (8) - MOVED to `src/schedule.rs`
  (findings 1 and 4).
* `a_boundary_is_not_a_cacheable_answer.rs` (5),
  `a_stage_boundary_catches_what_its_stage_raises.rs` (8),
  `a_build_can_ask_whether_it_stood_on_a_fallback.rs` (7) - MOVED to
  `src/boundary.rs` (finding 2).
* `the_ledger_scope_changes_speed_and_not_answers` (1) - RELOCATED IN from
  `highbay_data`'s `tests/assemble_under_the_driver.rs`, into `src/track.rs`.
  It composed `Tracked::new(&ledger, label, Memo::new(stage, store))` by hand,
  which finding 1 makes unwritable from a consumer. The property is held here
  over a stand-in; what is parked is holding it over a REAL stage, and the
  consumer file carries a note saying so.

## State of the implementation

DONE: `src/builder.rs` (builder, runner, id check at registration, intrinsic
memoization with owned/given/off stores); `tests/builder_is_the_only_door.rs`;
the CONSUMER CONVERSION and the VISIBILITY FLIP, with every internals-reaching
test moved into the module that owns the internal it reaches for.

NOT done: findings 1-7. Each one is a test in `src/` that wants to be a test in
`tests/`.
