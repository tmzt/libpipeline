# libpipeline: plan

`DESIGN.md` describes the target crate. This document is the working record
for getting there: the crate as it stands at this revision, the evidence
and findings behind the design's subtractions, the verdict, and the ordered
migration steps. It is written for the agent executing the migration and is
consumed by the work - reasoning meant to outlive the migration lives in
the design document, not here.

Provenance of the decisions, for the record. The definition is Tim's
(2026-08-24): "it's the self-contained state tracker for the steps defined
via a builder pattern with one way to run it and three possible results" -
four outcomes once the error channel is counted, with `Computed` in place
of `Ok` "to match `Unchanged`". The rulings of the same day, in order:
"these aren't durable pipelines, it's not a concern"; "the data can be
durable, the runtime state isn't"; "the order can just be tracked in the
builder, we don't need a stageid apart from that"; "MemoStore is a trait we
provide to the builder" (singular); "this is just a simple pipeline with an
input version and read-state tracking at the edges"; "Unchanged is also a
successful result, meaning we didn't need to compute anything or delay
anything, the value is finished"; "we can also use standard result now";
"the wakers are registered on the original input (or on a later stage,
internally) but the result isn't ready yet"; "the stages are connected by
wakers"; "we should also use a subcrate boundary and explicit re-exports
where needed moving the internals tests to the subcrate". Naming was
settled 2026-08-25: the outcome is `RunResult<T, E> =
Result<Run<T>, Failure<E>>`, the error type is `Failure` (not `Failed`),
and the position is reached through `at()`, not a public field.

## How to read this

* Present tense describes the crate at this revision; every such claim
  names its file and is checkable against the source. The steps are
  imperative and describe changes that have not landed. That tense split is
  the whole convention: the `!!! PROPOSED` marker earlier revisions of the
  design document used per section is not used here (a plan is entirely
  about change), and the design document now carries a single
  document-level note instead.
* Vocabulary: today's surface speaks `EffectPoll`, `DriveError`,
  `ChainError` and four run doors. The target speaks
  `RunResult<T, E> = Result<Run<T>, Failure<E>>`,
  `Run::{Computed, Unchanged, Delayed}`, and `Failure::at()`. Steps use the
  target names for what they introduce and today's names for what they
  remove.

## What exists today

Everything in this section is checkable against the source as of this
revision. It is the ground the design has to be reached from.

### The builder (built)

`src/builder.rs`: `PipelineBuilder::new`, `.stage(name, version, make)`,
`.stage_in(name, version, store, make)`, `.uncached()`, and
`StagedPipelineBuilder::build`. Memoization is intrinsic to registration -
every `stage()` call wraps its stage in the memo layer with an owned map, a
caller-given store, or the off switch (`BuilderStore`: owned / given /
off). The version is declared at the registration call site, and `checked`
in `src/builder.rs` panics at construction when a stage answers a different
id than it was registered under.

Carries forward: the builder as the only door, and memoization as
intrinsic to registration. Changes: the `version` argument goes from both
registration methods (step 2), `stage_in` goes in favour of one store at
the builder (step 3), `checked` goes with the self-declared identity
(step 2), `build` acquires the run-version type parameter (step 5), and the
chained error type stops growing per join (step 4).

### The four doors (built, and what the design replaces)

`Pipeline` in `src/builder.rs` exposes four ways to run one graph, onto two
drives:

* `.run(&input, &work) -> Result<Output, DriveError<Error>>` - the
  blocking drive, pumping `work: impl PendingWork` while polls answer
  `Pending`.
* `.run_pure(&input)` - `run` with `NoPendingWork`.
* `.run_watched(&input, &work) -> (Result<..>, WakeReport)` - the same
  drive, additionally reporting `Pending` polls that left no wake path.
* `.poll_frame(&input) -> EffectPoll<Output, Error>` - the frame drive:
  one poll, returns immediately.

Plus `.take_stale()` and `.waker()`, which the design keeps. There is no
version gate, no `Unchanged`, and no outcome type: an unchanged input
produces the same output again - cheaply, via the memo, but produced and
handed over all the same.

### What a run answers today

Three result types, at three scopes:

* **`EffectPoll<A, E>`** (`libeffects`, re-exported by `libpipelinedata`) -
  what one poll answers: `Ready(value)`, `Pending` (which obliges the poll
  to have registered the supplied waker), `Failed(error)`. This is the
  stage contract's vocabulary and survives; it leaves the **door**
  signature only.
* **`DriveError<E>`** (`src/driver.rs`) - how a blocking run ends badly:
  `Failed(E)`, or `Stalled` - the graph answered `Pending` with no
  outstanding work left. `Stalled` is not a timeout, and the same state
  means opposite things under the two drives: offline it is a bug in the
  graph, under a frame drive it is normal and the frame keeps its stand-in.
  That asymmetry is why `run_watched` exists, and it is what the one door
  dissolves by making the waiting the caller's.
* **`ChainError<A, B>`** (`src/chain.rs`) - which stage failed: `First(A)`
  (the second never ran) or `Second(B)`, nesting once per join.

### What else stays public, and why

Today's full export list, from `src/lib.rs`: `PipelineBuilder`,
`StagedPipelineBuilder`, `Pipeline`, `ChainError`, `DriveError`,
`PendingWork`, `NoPendingWork`, `WakePath`, `WakeReport`. `WakePath` is
named by no public signature and is kept as `WakeReport`'s vocabulary. The
stage contract lives in `libpipelinedata` and is re-exported by nothing
here (finding 8).

Target surface, for contrast: `PipelineBuilder`, `Pipeline`, `Run`,
`Failure`, the `RunResult` alias, with `MemoStore` behind the default.
Everything else on today's list leaves: `StagedPipelineBuilder` is absorbed
by the builder's chaining, `ChainError` goes with the nesting (step 4), and
`DriveError`, `PendingWork`, `NoPendingWork`, `WakePath`, `WakeReport` go
with the doors (step 5).

### Where a consumer works, and what it may never name

A consumer of this crate implements `Stage`
(`libpipelinedata/src/stage.rs`) and assembles nothing; everything on the composition side
reaches a consumer's stage only through registration. The measurement of
how much of the crate the public API can express: `tests/` holds **30
tests in 4 binaries** (`an_unwakeable_poll_is_visible_offline.rs`,
`builder_is_the_only_door.rs`, `engine_stays_generic.rs`,
`two_drivers_one_graph.rs`), plus the README's 6 doctests - against **71
unit tests in `src/`** that admit it cannot (37 in `src/track.rs`, 20 in
`src/boundary.rs`, 8 in `src/schedule.rs`, 6 in `src/watch.rs`). The
subcrate split (step 1) is aimed at exactly this ratio.

Five modules carry `#![cfg_attr(not(test), allow(dead_code))]`
(`src/boundary.rs`, `src/chain.rs`, `src/memo.rs`, `src/schedule.rs`,
`src/track.rs`), armed under `cargo test`; step 1 deletes the attributes
with the visibility flip.

### Two drivers, one graph

The same set of stages runs under two drives, and a stage cannot tell
which one is polling it: the blocking drive (`run_to_completion`,
`src/driver.rs`) polls until a value or a typed failure, pumping the
executor seam, with a deliberately no-op waker; the frame drive
(`FrameDriver`, `src/driver.rs`) polls once, never waits, and records
wakes in a flag. The blocking drive's watched form (`src/watch.rs`)
reports `Pending` polls that left no wake path. Under the design the same
two loops survive as caller patterns around the one door, and the claim "a
stage cannot tell how it is being driven" is unchanged; the drive functions
remain internal machinery with their own tests and stay the reference
semantics for what a blocking caller's loop does.

### A boundary refuses the cache

An error boundary turns a `Failed` poll into a substituted `Ready`, which
launders an uncacheable answer into a cacheable-looking one - so the
stage-level boundary (`Guarded`, `src/boundary.rs`) answers
`memo_key -> None` structurally, and a substitution count rides alongside
the drive's result. Real, tested, and unwired (finding 2).

### A memo hit is a deep copy today

Registration requires `S::Output: Clone` (`src/builder.rs`),
`MemoStore::lookup` returns an owned value on purpose
(`libpipelinedata/src/store.rs`), and the memo layer clones on both sides:
`.cloned()` on the hit (`libpipelinedata/src/store.rs`) and
`value.clone()` on the record (`src/memo.rs`). For an output the size of a
whole bundle that is the opposite of the saving a memo exists for. Fixed by
step 3: the erased `Arc` row makes a hit a downcast plus a refcount bump,
so the store erasure and the cheap-output fix are one change, not two.

## Why the stage version goes

### What the version was for

Today `StageId` is `{ name: &'static str, version: u32 }`
(`libpipelinedata/src/key.rs`), and its doc gives the version one job: an
input that changes a stage's output without itself being an input has to
move the key, or the memo returns the old answer forever. The named
instance is element expansion reading a static registry table.

Two things about that job: the version is a compile-time constant, so it
can only distinguish one compilation of the code from another - an ambient
input that moves within a process was never covered by it, and belongs to
read observation (`src/track.rs`). So the version's entire domain is a memo
that outlives a rebuild.

### The window never opens

Nothing in this stack has a cache that outlives a rebuild, and after the
durability ruling nothing will. Checked rather than assumed:

* `MemoMap` is `Mutex<HashMap<MemoKey, V>>`, in memory, cleared by `clear`
  or by being dropped (`libpipelinedata/src/store.rs`).
* The `MemoStore` seam has two methods, `lookup` and `record`, over owned
  values. No serialization bound, no path, no handle, nothing that could
  reach a disk (`libpipelinedata/src/store.rs`).
* `libpipelinedata`'s optional `serde` dependency is not a persistence
  door: it is a `Serializer` that writes into the content hasher, whose
  `Ok` type is `()` (`libpipelinedata/src/serde_hash.rs`).

A memo that cannot leave the process cannot be read by a later build of the
binary.

### What this invalidates, said plainly

An earlier revision of the design document devoted a section, "Why the
builder is the only door", to three decisions, of which the third was the
version declared at the registration call site. Its evidence was a
measurement: in the flat-export era one consumer declared its `StageId` 761
lines from the key construction it governed. The reasoning was that much of
this code is written by LLM agents, which do not scroll to a module-level
`const` to ask whether a number should move, so the invariant has to be in
the way rather than remembered. Every part of that is still true except the
part that mattered: the discipline protected a window that never opens, so
the closing bought nothing. The measurement was sound; what it measured was
the cost of a discipline with no payoff. The other two decisions of that
section - the builder as the only door, and memoization intrinsic to
registration - are untouched and load-bearing.

A second, smaller lesson sits in the test that covers the version.
`a_version_bump_at_the_call_site_is_a_cold_cache`
(`tests/builder_is_the_only_door.rs`) shares one store across two builds
inside one process, bumps the number, and shows the old rows going
unreachable. It demonstrates the mechanism exactly. It cannot demonstrate
the scenario, because the scenario spans two compilations of the binary and
no test spans those. A mechanism that is easy to demonstrate beside a
scenario that is unreachable is what this shape of defect looks like from
the inside.

### What follows

* The only readers of `name()` and `version()` anywhere in the stack are
  two adjacent lines in `libpipelinedata/src/hash.rs` folding both into the
  key; the change is small because the parts being dropped are barely read.
* `checked` (`src/builder.rs`) dissolves: with the builder the only source
  of an identity there is no second id for an honest author to answer with.
* `PipelineId` loses its planned shape hash and keeps the serial.
* Finding 7 closes by dissolution.
* The label discipline the design adopts for stage names is already held by
  an internal layer for its own labels (`Ledger::node`'s label,
  `src/track.rs`) - a precedent to copy, not an invention.

## Why the per-registration store goes

`stage_in` takes a store per registration (`src/builder.rs`), and
`BuilderStore` in the same file is the three-way per-stage answer to "where
does this stage remember". That is the assembly-at-each-call-site pattern
the builder exists to remove, one level down. `stage_in`'s stated use is "a
cache that outlives one build of the pipeline" (`src/builder.rs`);
position-as-identity forces that question closed (see the design's
"Rejected alternatives": a store that outlives its pipeline).

Step 3 removes: two `stage_in` methods and their `St` type parameter across
four registration signatures, `BuilderStore::Given`, and the
per-registration store plumbing. `.uncached()` stays and becomes what it
always meant: one store that remembers nothing, chosen once.

## Why the nested error goes

`ChainError<A, B>` tags a failure with the half it came from and nests once
per join (`src/chain.rs`). Two stages read `ChainError::First(..)`. Five
stages read `ChainError<ChainError<ChainError<ChainError<A, B>, C>, D>, E>`
- a type nobody writes in a signature and nobody matches on twice. The
nesting is also how a caller answers "which stage failed" today: by
counting `First`/`Second` layers. The flat `Failure` with `at()` replaces
it (step 4); internally the tagging in `src/chain.rs` goes with it - with
one error type on both halves, a chain propagates rather than retypes, and
the position is stamped at registration, where the index is available.

## Why the doors and their vocabulary go

The two-drive split forces one state to mean two things: a `Pending` poll
with nothing left to pump is a defect offline (`DriveError::Stalled`,
`src/driver.rs`) and a normal frame under the frame drive. `run_watched`
exists to let an offline caller see what a frame drive would lose. The one
door dissolves the asymmetry by making the waiting the caller's; each
public name then goes where its job went:

| today | fate |
|---|---|
| `.poll_frame(&input)` | becomes the one door's poll, under the version gate (step 5) |
| `.run(&input, &work)` | becomes the caller's loop on `Run::Delayed`, pumping the caller's own executor |
| `.run_pure(&input)` | the same loop with nothing to pump; a pure graph answers `Computed` on the first run |
| `.run_watched(&input, &work)` | becomes debug-build enforcement inside the door (step 6) |
| `DriveError::Failed(E)` | `Failure<E>`, position through `at()` |
| `DriveError::Stalled` | the caller's own break condition - the caller that owns the executor sees its queue is empty |
| `PendingWork` / `NoPendingWork` | the caller's executor seam; the types stay in `libpipeline-internals` for the reference drive and its tests |
| `WakeReport` / `WakePath` | internal vocabulary of `src/watch.rs` |
| `ChainError` | `Failure` with `at()` (step 4) |
| `EffectPoll` in the door signature | mapped onto `RunResult`; `EffectPoll` itself stays the stage contract's poll answer |

## What the edge reduction displaces

The design keeps read tracking at the run's edges and rejects an internal
dependency graph (its "Rejected alternatives" carries the argument). The
evidence from the code as it stands:

`src/track.rs` is 2,831 lines. The parts that answer questions only a
graph can ask: the reverse index (`Inner::readers`), the transitive marking
that walks it (the `VecDeque` walks in `Ledger::changed` and
`Ledger::unchanged`), the per-node read sets maintained in step with them,
and the per-edge retractable reasons (`Reason::Read(node)` against
`Reason::Owed`). `Schedule`/`Cycle` (`src/schedule.rs`, 539 lines) goes
with them. What survives: `TrackedInput` and the run scope, the subscriber
list, `revalidating`, and `Backdated`'s content-address comparison applied
at the root.

Checked in three places, and the fan-out argument holds:

* `src/schedule.rs`'s own doc states its headline saving on "the diamond
  graph (one input, two readers, one joiner)". The module names a fan-out
  shape as the case it is for; on a chain the same computation returns the
  head of the chain, which is where a pull starts anyway.
* Node-to-node edges only arise from nesting. `Tracked::poll_stage` calls
  `observe_read(self.node)` before opening its own scope (`src/track.rs`),
  so an edge is recorded only when another node's scope is already open - a
  stage polling another stage inside its own poll. `Chain::poll_stage`
  polls its two halves at the same level and hands the value along
  (`src/chain.rs`), so a chain records no node-to-node edge at all.
  `staleness_is_transitive` (`src/track.rs`) has to build its graph out of
  stages that poll stages, because a chain will not produce one.
* Early cutoff at a node (`Backdated`, `src/track.rs`) buys, on a chain,
  what content-keyed memoization already buys: a recompute that reaches the
  same value leaves the next stage's input unmoved, and the lookup precedes
  the work, so the chain stops there anyway (`src/memo.rs`).

Nothing in `src/track.rs` or `src/schedule.rs` is deleted in this plan.
The reduction changes what the builder will eventually spell (finding 1 in
the edge shape), not what has to be unwound first; removal happens against
a spelling that exists, after finding 1 lands.

## The ledger test, measured

A test relocated from a consumer's suite
(`the_ledger_scope_changes_speed_and_not_answers`) was examined by mutation
- break one thing, run the suite, see who notices - and found to observe
nothing its name claimed: deleting the tracking layer from it changed no
assertion, and the known-bad wrap order passed all four. The reproduction
that remained does build through the public door, as
`one_memo_serves_both_drivers_and_the_stage_runs_once` in
`tests/two_drivers_one_graph.rs`; the empty test was deleted rather than
parked, and should not return when finding 1 lands. The method is recorded
as a standing rule in the design ("What a test holds").

## Not built yet (engine-level, distinct from the builder findings)

* **The derived-key fold for composites.** A chain's own memo key would be
  a fold over its parts; until that exists, `Chain` honestly refuses to key
  and its parts are memoized individually (`src/chain.rs`).
* **Deep verification.** `Backdated` cuts off where a node's output
  repeats, which needs the node to have run; sparing a node's consumers
  before it runs at all (salsa's deep verify) is not here, and neither is a
  policy for which nodes are worth addressing per poll (`src/track.rs`).

## What the builder cannot yet express (findings, in priority order)

The numbering is load-bearing: source comments cite these by number. Each
entry records its status under the one-door design.

1. **Tracked state graphs.** `Ledger`/`Tracked`/`TrackedInput` composition
   - including the load-bearing wrap order - is exactly the assembly the
   builder exists to own, and the builder has no spelling for it. The 45
   tests over the tracked and schedule layers stay on internals until it
   does (the suites in `src/track.rs` and `src/schedule.rs`). Status:
   **open**, unchanged by the one door - the version gate answers
   `Unchanged` from version identity, not from the ledger - and **reshaped**
   by the edges ruling: the spelling the builder grows is read observation
   at the input boundary and a cutoff at the output boundary, not a wiring
   of today's graph. When it lands, the wrap order becomes unwritable and
   its known-bad twin is deleted rather than migrated.
2. **Error boundaries.** `Guarded` placement and the substitution tally are
   caller assembly today (`src/boundary.rs`). Status: **open**; under the
   one door the tally would surface as an accessor on `Pipeline` rather
   than a counted drive variant.
3. **Non-linear graphs.** The builder builds chains; a diamond exists in
   the engine via `Arc<S>: Stage` (`libpipelinedata/src/stage.rs`) but has
   no builder spelling. Status: **open**, orthogonal - with one constraint
   added by position-as-identity: a non-linear builder must still hand out
   one identity per registration, and a node shared between two consumers
   must keep the single identity it was registered with, which is what the
   `Arc<S>` impl's forwarding of `id` already does.
4. **Scheduling.** `Ledger::schedule` (`src/schedule.rs`) has no
   builder-level door; rides on finding 1. Status: **open**, and likely to
   close by dissolution - a chain's schedule is the chain, and the module's
   own headline case is a diamond.
5. **Store lifecycle.** `.stage_in` lets a cache outlive a build; there is
   no whole-pipeline store policy. Status: **settled** - one store at the
   builder is the whole-pipeline store policy, and the store does not
   outlive the pipeline. Closed by step 3.
6. **A watched single poll.** Nothing public answers "what did this poll
   leave behind". Status: **subsumed** - the one door checks the wake path
   itself in debug builds (step 6), and the finding closes by deletion
   rather than by a new door.
7. **The registration-site guarantee protects only what is registered.** A
   stage-authoring crate that never links the engine carries its versions
   unchecked. Status: **closes by dissolution** - with no version and no
   self-declared identity there is nothing for an unlinked authoring crate
   to carry unchecked; the builder mints the identity at registration or it
   does not exist (step 2).
8. **Assembling a pipeline takes two manifest edges** (`libpipeline` plus
   `libpipelinedata`). This crate could re-export the port so one edge
   suffices without weakening the split. Status: **open**, deliberately
   undecided; the subcrate split makes the facade the natural place to
   decide it.

## Verdict: migrate, do not rewrite

The judgement was asked for plainly, so here it is plainly: **migrate**.
The distance from what exists to what is designed is almost entirely in the
facade, and the facade is the smallest part of the crate.

The evidence, weighed:

* **The one door is a thin total mapping over machinery that exists.**
  `run` is the version gate (new: one `Mutex<Option<V>>` and one
  comparison) plus `FrameDriver::poll_frame` (exists, `src/driver.rs`) plus
  a three-arm match from `EffectPoll` onto `RunResult`, under the gate's
  early return (new: one enum, one error type, one alias). The blocking
  drive it displaces becomes a documented caller loop whose reference
  semantics - `run_to_completion` and its watched and counted forms - stay
  as internals with their tests. No engine semantics change.
* **The rulings shorten the distance rather than lengthening it.** Every
  one is a subtraction. The stage version deletes an argument from four
  registration signatures and two fields from a key type
  (`libpipelinedata/src/key.rs`) whose only readers are two lines
  (`libpipelinedata/src/hash.rs`); `PipelineId` loses its hash before the
  hash is ever written; one store deletes two `stage_in` methods, the `St`
  type parameter, and half the registration surface; the shared error type
  stops the chained error growing per join. The port needs no edit to carry
  the erased store: erasure is a choice of `V`, and `MemoStore for Arc<S>`
  is already there (`libpipelinedata/src/store.rs`).
* **The four doors are facade, not engine.** The doors and their vocabulary
  re-exports total well under a hundred lines of `src/builder.rs` and
  `src/lib.rs`. Deleting doors is not a rewrite-scale event.
* **The 71 internals tests survive by motion, not reconstruction.** Their
  own module docs already promise an outward migration "unchanged but for
  the imports"; the subcrate split is that migration. A rewrite would
  forfeit 3,370 lines of tracked-layer implementation and its 45 tests
  (2,831 in `src/track.rs`, 539 in `src/schedule.rs`) - machinery the new
  definition does not even touch - to arrive back at the same `Stage`
  contract.
* **What genuinely must be rewritten is bounded and identified**: the 30
  public tests and the README's 6 doctests, which speak the four-door
  vocabulary. The two-drivers file translates property-for-property into
  one-door-two-patterns form (its central claims - same answers, memo
  shared, wake obligations - are door-independent). Two of the 30 do not
  translate and are deleted, both in `tests/builder_is_the_only_door.rs`:
  `a_version_bump_at_the_call_site_is_a_cold_cache` tests a number that
  will not exist, and
  `a_stage_that_answers_a_different_id_than_registered_panics` tests a
  check whose defect stops being reachable.
* **The run-version parameter threads as a type parameter, not a rewrite.**
  `Pipeline<S>` becomes `Pipeline<V, S>`; the builder's chaining types are
  untouched until `build`.
* **One ruling reaches below the facade, and it is still small.** The flat
  error changes `src/chain.rs`: a two-variant enum and the two `map_err`
  arms come out of a 115-line module, and a position stamp goes in at
  registration. That is the deepest any of this goes.

What would have tipped it the other way, and did not: if the outcome type
had needed the engine to distinguish `Computed` from `Unchanged` per stage,
the memo/track layers would have needed a new result channel throughout -
but the gate is at the root, and the engine below it already answers
everything the mapping needs. The tracked-layer reduction is the one item
that could look like a rewrite, and it is not one in this plan: nothing in
`src/track.rs` or `src/schedule.rs` is deleted here. Those modules have no
caller today beyond their own tests, so the reduction changes what the
builder will eventually spell, not what has to be unwound first.

## Migration plan

Ordered steps. Each step leaves `cargo test` green; the counts named are
the gates. Baseline at this revision: 101 tests (30 public in 4 binaries +
71 unit in `src/`) plus the README's 6 doctests, all green.

**Citation debt from the document split.** Source comments cite sections by
name in `DESIGN.md` that now live in this file: "Migration plan" (the
dead-code notes in `src/boundary.rs`, `src/chain.rs`, `src/memo.rs`,
`src/schedule.rs`, `src/track.rs`), "Two drivers, one graph"
(`src/builder.rs`, `src/driver.rs`, `tests/two_drivers_one_graph.rs`),
"What else stays public" (`src/lib.rs`), "Not built yet" (`src/chain.rs`),
"The ledger test, measured" (`tests/two_drivers_one_graph.rs`), "Where a
consumer works" (`src/memo.rs`), and the findings by number
(`src/track.rs`, `src/schedule.rs`, `src/boundary.rs`, `src/watch.rs`).
The section names are kept verbatim here so each citation still resolves by
name; step 1 updates the document half of each comment from `DESIGN.md` to
`PLAN.md` (comment-only edits, no behaviour).

**Step 1 - the subcrate split (motion only).**
Create `libpipeline-internals/` (manifest: `libeffects`,
`libpipelinedata` path deps; same license and edition). Move
`src/{chain,memo,driver,watch,boundary,track,schedule}.rs` into its `src/`,
flipping `pub(crate)` to `pub` and deleting the five
`#![cfg_attr(not(test), allow(dead_code))]` attributes. Move the nine
`#[cfg(test)]` modules to `libpipeline-internals/tests/`, one file per
module, changed only in their imports: `src/track.rs`'s four suites
(`invalidation_marks_dependents`, `an_equal_recompute_stops_at_its_node`,
`reads_become_edges`, `a_fallback_is_not_a_revalidation`),
`src/schedule.rs`'s `the_schedule_polls_each_node_once`,
`src/boundary.rs`'s three (`a_boundary_is_not_a_cacheable_answer`,
`a_stage_boundary_catches_what_its_stage_raises`,
`a_build_can_ask_whether_it_stood_on_a_fallback`), and `src/watch.rs`'s
`tests`. Facade `src/builder.rs` imports `Chain`, `Memo`, `FrameDriver`,
`run_to_completion`, `run_to_completion_watched` from the internals crate;
`src/lib.rs` re-exports `ChainError`, `DriveError`, `NoPendingWork`,
`PendingWork`, `WakePath`, `WakeReport` from it explicitly (a temporary
list: all six leave in later steps). Add `libpipeline-internals` to
`THE_STACK` in `tests/engine_stays_generic.rs`. Sweep the citation debt
above.
*Gate*: facade 30 tests + 6 doctests; internals 71 tests; zero test bodies
changed; `builder_is_the_only_door.rs` unchanged and green.

**Step 2 - identity becomes position, and the version goes.**
In `libpipelinedata`: `StageId` becomes the builder's index
(`libpipelinedata/src/key.rs`), with the two folds in
`libpipelinedata/src/hash.rs` following it; `StageId::new(name, version)`
leaves the surface. In the facade: drop the `version` argument from all
four registration methods and delete `checked` (`src/builder.rs`). Keep the
`name` argument as a diagnostic label, held for messages and `Debug`,
entering no key and compared by nothing - the discipline `Ledger::node`'s
label already states (`src/track.rs`). Delete
`a_version_bump_at_the_call_site_is_a_cold_cache` and
`a_stage_that_answers_a_different_id_than_registered_panics`
(`tests/builder_is_the_only_door.rs`). Update the README's version passage
(its "declares the stage's version right there" paragraph and the store
passage that leans on it) and its examples. Sweep the surviving test docs
for paragraphs that argue the retired discipline - among them
`tests/two_drivers_one_graph.rs`'s "the versions are here, at the
registration call sites, which is the whole of why the builder takes them
there", which becomes false the moment this step lands.
*Gate*: facade 28 tests + 6 doctests; internals 71;
`grep -rn "StageId::new\|\.version()"` finds nothing outside the key type
itself; no test asserts on a stage name as though it were an identity.

**Step 3 - one store at the builder, erased.**
Add `.store(store)` to `PipelineBuilder` and remove both `stage_in`
methods, taking the `St` type parameter with them and leaving two
registration signatures where there were four (`src/builder.rs`). The
builder holds one store; each registration takes a shared handle to it,
through the existing `MemoStore for Arc<S>` in
`libpipelinedata/src/store.rs`. Rows are `Arc<dyn Any + Send + Sync>`; a
lookup downcasts and `expect()`s, naming the identity-collision invariant.
`BuilderStore` keeps its three answers and is consulted once
(`.uncached()` becomes the store that remembers nothing). Port
`tests/two_drivers_one_graph.rs`'s four `stage_in` call sites to one
`.store(MapStore)` at the builder - the seam is still exercised by an
implementation this crate did not write, which is the property that file
names; its doc paragraph about reaching the graph "through
`PipelineBuilder::stage_in`" is rewritten with the call sites rather than
after them. Add one facade test: two stages of different output types
sharing one store, each getting its own answer back.
*Gate*: facade 29 tests + 6 doctests; internals 71; `grep -rn "stage_in"`
finds nothing; the new test fails if the downcast is made to swallow a
miss.

**Step 4 - one error type, flat and positioned.**
Add `Failure<E>` (`src/builder.rs`, exported from `src/lib.rs`): private
fields - the failing stage's position and its error - with `at() -> usize`
as the position accessor and an accessor for the error beside it.
Registration stamps the position; `Chain` propagates instead of retyping,
and `ChainError` and its two `map_err` arms come out of `src/chain.rs` and
out of `src/lib.rs`'s exports. Re-spell
`a_failure_names_the_stage_that_raised_it`
(`tests/builder_is_the_only_door.rs`) and
`a_failure_bubbles_out_tagged_with_the_half_it_came_from`
(`tests/two_drivers_one_graph.rs`) as assertions on `.at()`.
*Gate*: facade 29 tests + 6 doctests; internals 71;
`grep -rn "ChainError"` finds nothing in the facade; a three-stage
pipeline's error type is spellable in one line in a test signature, which
is the property the change is for.

**Step 5 - the outcome and the one door (the flip).**
In `src/builder.rs`: add `Run<Output>` and the alias
`pub type RunResult<T, E> = Result<Run<T>, Failure<E>>` (both exported from
`src/lib.rs`); give `Pipeline` the `V: Copy + Eq` parameter and the
`last: Mutex<Option<V>>` field; implement
`run(&self, version, &input) -> RunResult<Output, Error>` as gate +
`poll_frame` + mapping, recording the version only on `Ready`. The gate
consumes `take_stale` on **every** run and answers `Unchanged` only when
the version matches **and** no wake was pending - see the design's "The
version gate and the one door" for why the version alone is a silent
defect. This step owes a test for it: a pipeline left on `Delayed`, woken
out of band with the version unchanged, must answer `Computed` and not
`Unchanged`; it fails the moment the wake half is dropped, which is the
only way to know the half is doing anything. Delete `run`, `run_pure`,
`run_watched`, `poll_frame` (the doors, not the internals they call); keep
`take_stale` and `waker`. Drop the
`DriveError`/`PendingWork`/`NoPendingWork`/`WakePath`/`WakeReport`
re-exports from `src/lib.rs`. Port the public tests:
`tests/builder_is_the_only_door.rs` re-spells its tests through the one
door (pure graphs answer `Computed` first run, `Unchanged` second - which
finally lets the public suite assert the memo's headline directly);
`tests/two_drivers_one_graph.rs` becomes
`tests/one_door_two_patterns.rs`, its 12 properties translated (blocking
loop and wake-wait patterns, same answers, one memo, the lost-not-late wake
test spelled as `take_stale` staying false);
`tests/an_unwakeable_poll_is_visible_offline.rs` moves to
`libpipeline-internals/tests/` against `run_to_completion_watched`,
unchanged but for imports. Rewrite the README's examples (the doctests are
the gate). Sweep `libpipeline-internals` for citations of renamed public
tests and update them.
*Gate*: facade 26 tests + 6 doctests; internals 74;
`grep -rn "run_pure\|run_watched\|poll_frame\|PendingWork\|DriveError"`
finds nothing in facade `src/` public items or `tests/`; every test name
cited from internals docs exists (`grep` each cited name); ASCII check on
everything rendered or exported.

**Step 6 - Delayed keeps its promise.**
In the facade's `run`, on the `Pending` path under
`#[cfg(debug_assertions)]`, poll through
`libpipeline_internals::poll_watched` and panic on `WakePath::Missing` with
the lost-not-late diagnosis. Add a facade test
(`#[cfg(debug_assertions)]`, `#[should_panic]`) driving a stage that
forgets its waker. The open decision recorded in the design - a wake-debt
accessor instead of the debug panic - is Tim's to flip; the shape of the
check is the same either way.
*Gate*: facade 27 tests + 6 doctests, green in debug and `--release`.

**Step 7 - the documents catch up.**
As each step lands, strike or update the matching passages in this file's
"What exists today" and the findings' status lines (part of each step's
review, listed once here so it is nobody's afterthought). When step 6
lands, remove the document-level `!!! PROPOSED` note from `DESIGN.md` -
the design is then the crate - and retire this file: it is scaffolding, not
a long-term document. Whatever is still open (findings 1-4 and 8, the `Ctx`
stage shape, the wake-debt decision) moves with the work that takes it up.

Deliberately not in this plan: wiring the tracked layer (finding 1),
boundaries (finding 2), non-linear graphs (finding 3), the `Ctx` stage
shape, and `PipelineId`. Each becomes strictly easier after the split -
they are builder spellings over an internals crate that now has a public
API to compose - and none of them blocks, or is blocked by, the one door.
Also deliberately not in this plan: any deletion in `src/track.rs` or
`src/schedule.rs` ("What the edge reduction displaces" says why the order
is finding 1 first, deletion after).
