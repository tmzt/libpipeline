# libpipeline: design

`libpipeline` is an incremental computation engine for pipelines of pure
stages: a source value goes in, derived outputs come out, and everything
between is memoized and re-derivable on demand. The engine is generic over
every payload type - it never learns what a stage computes, only how to poll
it, key it, and remember its answers.

This revision of the document exists to capture one intended public shape -
the pipeline as a SELF-CONTAINED STATE TRACKER with a single way to run it
and four possible outcomes - and to measure the distance between that shape
and the crate as it stands. The definition is Tim's (2026-08-24): "it's the
self-contained state tracker for the steps defined via a builder pattern
with one way to run it and three possible results" - four outcomes once the
error channel is counted among them, and with `Computed` in place of `Ok`,
"to match `Unchanged`".

## How to read this

A section that describes code which DOES NOT EXIST carries a marker on a
line of its own, directly beneath the heading, exactly as written here and
with no leading whitespace:

```
!!! PROPOSED
```

The marker scopes to that entire section, subsections included. Everything
not so marked describes the crate as it is today.

The distinction carries more weight here than in most design documents,
because this one is read as a specification: an unmarked paragraph is a
claim you can check against the source, and a marked one is a claim about
intent. Confusing the two sends a reader looking in `src/` for something
nobody has written. Proposed behaviour is never written in the present
tense as if it existed; where a marked section must speak of today's code,
it names it as today's.

Two further conventions worth knowing before the first section:

* **Every claim about the code names the file that carries it.** A claim
  with no path is a claim about design rather than about this
  implementation.
* **"Public" and "internal" are drawn strictly.** The public-API section is
  the whole contract a consumer or a test may touch, and it names no
  internal type. The internals section is machinery the builder assembles,
  which may be reorganized without notice and is named there only so this
  crate can discuss itself.

## What is in it

Five parts. WHAT A PIPELINE IS and the PUBLIC API are the intended shape,
marked proposed almost throughout. WHAT EXISTS TODAY and THE MODEL are the
crate as it stands - the unmarked ground truth, including the current
four-door surface the proposal replaces. INTERNALS covers the layers, how
they compose, and where the proposed pieces land among them. The rest is
the ledger: the findings, the recorded stage-shape intent, the subcrate
boundary, and the document ends with a verdict and a migration plan.

## What a pipeline is

!!! PROPOSED

A pipeline is the self-contained state tracker for a sequence of steps
defined through a builder. Self-contained means the pipeline itself holds
what it last ran against and what it remembered along the way; none of that
state lives in the caller, and none of it lives in hand-composed wrappers
around the pipeline. A caller holds a pipeline, hands it the current state
of the world, and reads one of four answers.

* **Steps come from a builder.** Each step is registered once, with a name
  and a behaviour version, and the pipeline owns everything about how the
  steps compose and what they remember between runs.
* **There is ONE way to run it.** Not a blocking door and a frame door - one
  method. Whether a caller blocks until the answer or returns and waits for
  a wake is what the caller DOES with the `Delayed` outcome, not an API it
  chose up front.
* **A run has four outcomes.** `Computed(Output)` - work happened, here is
  the new value. `Unchanged` - nothing moved; the value the caller already
  holds still stands. `Delayed` - not ready yet; a wake is coming. And
  `Failed(Error)` - the stage's typed error.
* **Input is a `(version, readable)` pair, supplied per run.** The version
  says WHICH state this is; the readable IS that state. A version matching
  the previous run's answers `Unchanged` without reading the readable at
  all.

`Computed` rather than `Ok` is deliberate. `Ok` says only "not an error";
`Computed` says WORK HAPPENED, and contrasts exactly with `Unchanged`. Both
are success. The distinction between them is the entire point of a
memoizing pipeline: a caller that cannot tell "here is a new value" from
"keep what you have" cannot avoid re-consuming the value, and the work the
pipeline saved reappears one layer up.

## Public API

!!! PROPOSED

This section is the whole contract. A consumer learns here what a pipeline
is, how to build one, how to run it, and what the four outcomes mean - and
meets nothing else. The machinery behind it is described under "Internals"
and is deliberately absent here.

### Building a pipeline

```rust,ignore
use libpipeline::{PipelineBuilder, Run};

let pipeline = PipelineBuilder::new()
    .stage("parse", 3, |id| Parse::new(id))
    .stage("lower", 1, |id| Lower::new(id))
    .build();
```

* `PipelineBuilder::new()` - the empty builder.
* `.stage(name, version, make)` - register one step. The number is the
  STEP'S BEHAVIOUR VERSION - which code this is - declared at the
  registration call site so it lives beside the behaviour it versions; a
  stage constructed under a different identity than it answers is refused
  at build time, loudly. Steps chain: each consumes what the previous one
  produced. Every registered step remembers its answers; there is no
  un-remembering registration to forget.
* `.stage_in(name, version, store, make)` - the same, remembering into a
  store the caller provides, so what a step remembers can outlive one build
  of the pipeline.
* `.uncached()` - the control switch: the pipeline remembers nothing.
  Answers must not change, only speed; a pipeline whose answers change when
  remembering is disabled has a bug the remembering was hiding.
* `.build()` - the finished pipeline. Its input type is the first step's
  input; its output type is the last step's output; its version type is
  fixed here, as part of the pipeline's own type.

A consumer implements steps against the stage contract in
`libpipelinedata` (`Stage`, `StageId`, and the key vocabulary), which
exists so a step can be DECLARED without linking the engine that runs it.
A consumer implements stages and assembles nothing.

### Running it

```rust,ignore
match pipeline.run(version, &document) {
    Run::Computed(output) => held = Some(output),
    Run::Unchanged => { /* `held` still stands */ }
    Run::Delayed => { /* draw the stand-in; a wake is coming */ }
    Run::Failed(error) => { /* the stage's typed error */ }
}
```

`run(version, &readable)` is the one door. It polls once and returns
immediately, whatever the answer; nothing inside it waits, ever.

The `version` here is NOT the builder's per-step number. The step version
says which CODE a step is; the run version says which STATE the input is.
The two never meet: one is fixed at registration, the other arrives with
every run.

### The four outcomes, and what each answer means

| outcome | meaning |
|---|---|
| `Computed(Output)` | work happened; here is the new value |
| `Unchanged` | nothing moved; the value from the last `Computed` still stands |
| `Delayed` | not ready; a wake is coming |
| `Failed(Error)` | the stage's typed error channel |

**`Computed` and `Unchanged` are both success**, and the pipeline's memory
is what tells them apart. The pipeline records the version each `Computed`
answered for; a run handed that same version again answers `Unchanged`
without reading the readable. So `Unchanged` always means: the value I
last handed you derives from exactly this state - keep it.

**Only `Computed` records the version.** A `Delayed` run has not produced
the value for that version, so asking again with the same version polls
again rather than falsely answering `Unchanged`. A `Failed` run records
nothing either: a failure is this run's answer, not the pipeline's verdict,
and a later run with the same version retries rather than serving the
failure back as a settled fact. A consequence worth knowing: after a newer
version fails, re-running the OLD version still answers `Unchanged` - the
old value still stands, which is true.

**`Delayed` is a promise.** A run that answers it has arranged for the
pipeline's waker to be woken when the answer becomes possible. A step that
cannot keep that promise has made its value lost rather than late, and the
pipeline treats that as the defect it is (see "Delayed keeps its promise"
under Internals).

**Neither `Computed` nor `Failed` is terminal.** A pipeline is a standing
derivation over inputs that change; a later run over a changed version can
move `Failed` back to `Computed`, and `Computed` to a different value.

### The version

The version type is fixed when the pipeline is built and is bounded
`Copy + Eq`. `Eq` and not `PartialEq`, deliberately: `PartialEq` admits
`f64`, `NaN != NaN`, and a version that never equals itself makes the
`Unchanged` gate silently never fire.

The pipeline NEVER computes a version. It compares the ones it is handed.
Where a version comes from is the consumer's business - an edit store's
cursor, a build number, a git sha - anything cheap to copy and honest
about identity. That cheapness is the point of the pair: the version costs
a comparison, and the readable may be a large snapshot that a matching
version never touches. Versions are compared for identity, not order;
running an older state again is just another state.

### After `Delayed`: the wake

* `.take_stale() -> bool` - whether a wake arrived since last asked
  ("stale, run again"); reading clears it.
* `.waker() -> Waker` - the wake target, for landing values out of band.

### Blocking and frame are what a caller does, not what it chooses between

A frame-driven caller runs once per frame when there is reason to:

```rust,ignore
if pipeline.take_stale() || version != drawn_for {
    match pipeline.run(version, &document) { /* ... */ }
}
```

A blocking caller loops on `Delayed`, making its own progress between
runs:

```rust,ignore
let outcome = loop {
    match pipeline.run(version, &document) {
        Run::Delayed if executor.run_once() => continue,
        Run::Delayed => break stalled(), // the caller's own condition
        done => break done,
    }
};
```

The second arm is what used to be the engine's `Stalled` error: a
`Delayed` when the caller has nothing left to run means something waited
for an input nothing was going to land. Waiting stopped being the
pipeline's job, so having-nothing-left-to-wait-on stopped being its
vocabulary; the caller that owns the executor is the one that can see its
queue is empty.

### What else is public

* **The stage contract** (`Stage`, `StageId`, `MemoKey`, `ContentKey`,
  `EffectPoll`, `MemoStore`, `MemoMap`, `NoMemo`) - lives in
  `libpipelinedata`, not here; a stage AUTHOR needs it, and it is the
  implement-side contract, not composition machinery.
* **`Run`** - the outcome type above.
* **`ChainError`** - result vocabulary, not machinery: a pipeline of two or
  more steps fails with nested `ChainError`, and a caller matching on WHICH
  step failed needs the name. The type that composes such pipelines stays
  internal.

Nothing else. In particular, the executor seam (`PendingWork`), the
blocking drive's error type (`DriveError`), and the watched drive's report
(`WakeReport`, `WakePath`) - all public today - leave the surface with the
doors that named them.

## What exists today

Everything in this section is checkable against the source as of this
revision. It is the ground the proposal above has to be reached from.

### The builder (built)

`src/builder.rs`: `PipelineBuilder::new`, `.stage(name, version, make)`,
`.stage_in(name, version, store, make)`, `.uncached()`, and
`StagedPipelineBuilder::build`. Memoization is intrinsic to registration -
every `stage()` call wraps its stage in the memo layer with an owned map,
a caller-given store, or the off switch. The version is declared at the
registration call site, and `checked` in `src/builder.rs` panics at
construction when a stage answers a different id than it was registered
under - the stale-memo defect refused at build time rather than served at
poll time. All of this carries forward under the proposal unchanged except
for the version type parameter `build` acquires.

### The four doors (built, and what the proposal replaces)

`Pipeline` in `src/builder.rs` exposes four ways to run one graph, onto
two drives:

* `.run(&input, &work) -> Result<Output, DriveError<Error>>` - the
  blocking drive, pumping `work: impl PendingWork` while polls answer
  `Pending`.
* `.run_pure(&input)` - `run` with `NoPendingWork`.
* `.run_watched(&input, &work) -> (Result<..>, WakeReport)` - the same
  drive, additionally reporting `Pending` polls that left no wake path.
* `.poll_frame(&input) -> EffectPoll<Output, Error>` - the frame drive:
  one poll, returns immediately.

Plus `.take_stale()` and `.waker()`, which the proposal keeps. The public
result vocabulary these doors name is re-exported from `src/lib.rs`:
`ChainError`, `DriveError`, `PendingWork`/`NoPendingWork`,
`WakePath`/`WakeReport`.

There is no version gate, no `Unchanged`, and no outcome type: today a
caller cannot be told "keep what you have" - an unchanged input produces
the same output again (cheaply, via the memo, but produced and handed over
all the same).

### What a run answers today

Three result types, at three scopes, all replaced or resituated by the
proposal but true today:

* **`EffectPoll<A, E>`** (`libeffects`, re-exported by `libpipelinedata`) -
  what one poll answers: `Ready(value)` ("here is the current value", not
  "finished"), `Pending` ("I cannot answer yet" - which OBLIGES the poll to
  have registered the supplied waker), `Failed(error)`. Neither `Ready`
  nor `Failed` is terminal; only `Pending` carries an obligation.
* **`DriveError<E>`** (`src/driver.rs`) - how a blocking run ends badly:
  `Failed(E)`, or `Stalled` - the graph answered `Pending` with no
  outstanding work left, so re-polling could only answer `Pending` again.
  `Stalled` is not a timeout, and the same state means opposite things
  under the two drives: offline it is a bug in the graph, under a frame
  drive it is normal and the frame keeps its stand-in. That asymmetry is
  why `run_watched` exists today, and it is the asymmetry the one-door
  design dissolves by making the waiting the caller's.
* **`ChainError<A, B>`** (`src/chain.rs`) - WHICH stage failed: `First(A)`
  (the second never ran) or `Second(B)` (the first produced a value and
  the second failed on it), nesting once per join. This one survives the
  proposal unchanged.

### What else stays public, and why

Today's full export list, from `src/lib.rs`: `PipelineBuilder`,
`StagedPipelineBuilder`, `Pipeline`, `ChainError`, `DriveError`,
`PendingWork`, `NoPendingWork`, `WakePath`, `WakeReport`. `WakePath` is
named by no public signature and is kept as `WakeReport`'s vocabulary. The
stage contract lives in `libpipelinedata` and is re-exported by nothing
here (finding 8 records the open decision).

### Where a consumer works, and what it may never name

**A consumer of this crate implements `Stage` and assembles nothing.**
`Stage` is a trait in `libpipelinedata` (`src/stage.rs` there); the
builder takes `S: Stage`. Everything on the composition side - tracking,
memoization, chaining, scheduling, driving - belongs to whoever links the
engine, reaches a consumer's stage only through registration, and cannot
be named from outside this crate. A consumer-level test that hand-composes
the graph is testing the wrong layer whether or not it passes, and the
remedy is never a re-export: a property the builder cannot express is a
FINDING, recorded below.

**The measurement.** `tests/` holds what the public API can reach: **30
tests in 4 binaries** (`an_unwakeable_poll_is_visible_offline.rs`,
`builder_is_the_only_door.rs`, `engine_stays_generic.rs`,
`two_drivers_one_graph.rs`), plus the README's 6 doctests - against **71
unit tests in `src/`** that admit it cannot (37 in `src/track.rs`, 20 in
`src/boundary.rs`, 8 in `src/schedule.rs`, 6 in `src/watch.rs`). When a
finding closes, tests migrate outward. The subcrate boundary section below
is aimed at exactly this ratio.

## The model

The commitments the engine embodies, each carried by a named piece of the
crate. Source comments cite these section names rather than restating
them. All of this is true today, and all of it survives the proposal - the
proposal changes the door, not the engine.

### A pipeline is a chain of pure stages

A stage consumes one input type and produces one output type; stages
compose by the next stage's `Input` equaling the previous stage's
`Output`. The composite of two stages is itself a stage, so a graph is
never a second kind of thing a driver must know how to walk
(`src/chain.rs`). A stage is driven by a poll/waker protocol
(`libpipelinedata::Stage`, reusing `libeffects`' poll contract): `Ready`,
`Pending` - which obliges the stage to arrange a wake - or `Failed`.

### The lookup precedes the work

Memoization is keyed by `(stage identity, content keys of the inputs)` - a
key computable BEFORE the stage runs, so the cache can skip the work
rather than validate it afterwards. An unchanged input hits at the first
stage and the rest of the chain is never entered. Only `Ready` is
recorded: `Pending` is not a value, and `Failed` is deliberately never
cached - a transient failure served back under a key that says it is fresh
would be a settled fact that never was. A stage that cannot honestly key
an input answers `memo_key -> None` and is neither looked up nor recorded
(`src/memo.rs`). Content keys are streaming hashes; the vocabulary lives
in `libpipelinedata`, and WHERE answers are remembered is a seam
(`MemoStore`), not the engine's decision.

### Reads are observed, not declared

Dependency edges are RECORDED BY OBSERVING READS, never accepted as a
declared list. While a stage runs, every tracked read is logged as an
edge; the set is re-logged on every run so it follows conditionals.
Changing a tracked input marks every node that read it - transitively -
stale, and wakes whoever subscribed; a node that recomputes to the same
content address retracts itself as a reason for its consumers to re-run
(`src/track.rs`: `Ledger`, `Tracked`, `TrackedInput`, `Backdated`;
`src/schedule.rs`: what a driver polls next given the stale set). This
whole layer is real, tested, and UNWIRED: the builder has no spelling for
any of it (finding 1), so nothing a consumer can build today reaches it.

### Two drivers, one graph

The same set of stages runs under two drives, and a stage cannot tell
which one is polling it: the blocking drive (`run_to_completion`,
`src/driver.rs`) polls until a value or a typed failure, pumping the
executor seam, with a deliberately no-op waker; the frame drive
(`FrameDriver`, `src/driver.rs`) polls once, never waits, and records
wakes in a flag. The blocking drive's watched form (`src/watch.rs`)
reports `Pending` polls that left no wake path - each one a value a frame
drive would lose rather than receive late. Under the proposal the same two
loops survive as CALLER PATTERNS around the one door; the claim "a stage
cannot tell how it is being driven" is unchanged.

### A boundary refuses the cache

An error boundary turns a `Failed` poll into a substituted `Ready`, which
launders an uncacheable answer into a cacheable-looking one - so the
stage-level boundary (`Guarded`, `src/boundary.rs`) answers
`memo_key -> None` structurally, and a substitution count rides alongside
the drive's result, separating "built" from "built on fallbacks". Also
real, tested, and unwired (finding 2).

### The engine stays generic

The engine never learns a consumer's types. Everything is generic over
`S: Stage`; every test invents stand-in types of its own. The proof is
mechanical: `tests/engine_stays_generic.rs` walks this crate's manifest
and, through its path dependencies, every manifest under it, and fails if
the tree names a crate outside the stack's closed allowlist (`THE_STACK`
in that file). The stack is three crates, dependencies pointing strictly
downward:

| crate | role |
|---|---|
| `libpipeline` | the engine: memoization, tracking, scheduling, the drivers |
| `libpipelinedata` | the port: `Stage`, the key types, `ContentHash`, `MemoStore` |
| `libeffects` | the base: the poll/waker protocol, boundaries, wake flags |

## Internals

**Nothing in this section is reachable by a consumer, and nothing in it
appears in a public-API example.** The names exist so this crate's
maintainers and tests can talk about the machinery.

### The layers, and the wrap order

All internal machinery is `pub(crate)` today. Bottom-up:

* **`Chain`** (`src/chain.rs`) - two stages composed, itself a `Stage`.
  Refuses to key (`memo_key -> None`); its parts are memoized instead. The
  builder nests these under a fixed internal `StageId`.
* **`Memo`** (`src/memo.rs`) - the memo layer: lookup precedes the work,
  only `Ready` recorded, and the store is skipped entirely while
  `revalidating()` (`src/track.rs`) is true - the thread-local channel by
  which the ledger outranks the store without any stage declaring
  anything. Merged into registration: every `stage()` call wraps its stage
  in one (`src/builder.rs`).
* **The tracked layer** (`src/track.rs`: `Ledger`, `Tracked`,
  `TrackedInput`, `Backdated`, `NodeId`, `revalidating`) and
  **`Schedule`/`Cycle`** (`src/schedule.rs`) - observed reads, transitive
  invalidation, early cutoff, and what-to-poll-next. Unwired: private even
  though the builder cannot yet express them (findings 1 and 4).
* **`Guarded`/`Substitutions`/`run_to_completion_counted`**
  (`src/boundary.rs`) - the stage-level error boundary and its tally.
  Unwired (finding 2).
* **The drives** - `run_to_completion` and `FrameDriver`
  (`src/driver.rs`); `poll_watched` and `run_to_completion_watched`
  (`src/watch.rs`).
* **`BuilderStore`** (`src/builder.rs`) - owned map / caller-given / off,
  the three answers to "where does this stage remember".

Two composition rules hold by convention rather than by type, each stated
in the owning module's doc and pinned by a known-bad twin:

* **The cache goes INSIDE the tracking** - `Tracked::new(&ledger, label,
  Memo::new(stage, store))`, never `Memo::new(Tracked::new(..), store)`.
  A cache outside the tracking answers before any run scope opens, so the
  ledger's staleness mark goes unread (`src/memo.rs`'s doc; the twin lives
  in `src/track.rs`'s test modules). Memoization sits inside the node's
  scope precisely so the lookup can see the node's staleness - that is
  where tracking and memoization sit relative to each other, and the
  builder is intended to own this order so it becomes unwritable
  (finding 1).
* **The boundary goes OUTSIDE the tracking** - a substituted `Ready`
  inside the tracking tells the ledger a node is up to date while it is
  still owed its real answer (`src/boundary.rs`'s doc and twins). The
  third rule of the family - a boundary belongs outside the memo - is
  closed structurally by `Guarded` refusing to key.

The five modules whose machinery has no caller but its own tests carry
`#![cfg_attr(not(test), allow(dead_code))]` (`src/boundary.rs`,
`src/chain.rs`, `src/memo.rs`, `src/schedule.rs`, `src/track.rs`), armed
under `cargo test` so anything genuinely unused still fails the gate.

### The version gate and the one door

!!! PROPOSED

The one door is the frame drive plus a version gate plus an outcome
mapping - no new engine semantics anywhere in it.

`Pipeline` gains a type parameter and one field: `Pipeline<V, S>` holding
the graph, the existing `FrameDriver`, and `last: Mutex<Option<V>>` (safe
interior mutability, as everywhere in this crate; `run` keeps `&self`
because a poll holds `&self` all the way down). `run(version, input)`:

1. If `last` holds exactly `version`: return `Run::Unchanged`. The
   readable is not dereferenced, no memo key is computed, no stage is
   polled.
2. Otherwise poll once through the frame driver (today's
   `FrameDriver::poll_frame`, `src/driver.rs`) and map the answer:
   `Ready(v)` becomes `Run::Computed(v)` and records the version;
   `Pending` becomes `Run::Delayed`; `Failed(e)` becomes
   `Run::Failed(e)`. Nothing else records the version.

The blocking drive (`run_to_completion`, `src/driver.rs`) stops being a
public door and becomes the caller's loop; it and its watched and counted
forms remain internal machinery with their own tests, and they remain the
reference semantics for what such a loop does. `PendingWork`,
`NoPendingWork`, `DriveError`, `WakeReport` and `WakePath` leave the
public surface with the doors that named them; `ChainError` stays, because
the one door still fails with it.

The version gate sits ABOVE the whole graph, outermost: it is the pipeline
remembering what it last ran against, not a per-stage concern. Stage-level
memoization is untouched and still does its work on the version-mismatch
path - an input that moved its version but not its content still hits at
the first stage. The gate does not wire the tracking layer, and does not
need it; a later wiring of the ledger would let `Unchanged` also fire when
a recompute reaches the root with an unchanged content address
(root-level backdating), through the same variant, with no API change.

### Delayed keeps its promise

!!! PROPOSED

`Delayed` publicly promises "a wake is coming", and the engine can check
it: `poll_watched` (`src/watch.rs`) already measures, per poll and in safe
code, whether a `Pending` poll left a wake path. The one door polls
through it in debug builds and panics on `WakePath::Missing` with the
diagnosis (a stage answered `Pending` without arranging a wake - the value
is lost rather than late); release builds poll plain and trust the stage
contract, keeping the probe allocation out of the hot path. This subsumes
finding 6 and replaces the public `run_watched` door: what was an optional
offline diagnostic becomes enforcement of the outcome's meaning, at the
door itself. The alternative - an accessor exposing a wake-debt count
instead of a debug panic - is one decision Tim may flip; the shape of the
check is the same either way.

## Not built yet (engine-level, distinct from the builder findings)

* **The derived-key fold for composites.** A chain's own memo key would be
  a fold over its parts; until that exists, `Chain` honestly refuses to
  key and its parts are memoized individually (`src/chain.rs`).
* **Deep verification.** `Backdated` cuts off where a node's output
  REPEATS, which needs the node to have run; sparing a node's consumers
  before it runs at all (salsa's deep verify) is not here, and neither is
  a policy for which nodes are worth addressing per poll (`src/track.rs`).

## What the builder cannot yet express (findings, in priority order)

The numbering is load-bearing: source comments cite these by number.
Each entry now also records its status under the one-door design.

1. **Tracked state graphs.** `Ledger`/`Tracked`/`TrackedInput` composition
   - including the load-bearing wrap order - is exactly the assembly the
   builder exists to own, and the builder has no spelling for it. The 45
   tests over the tracked and schedule layers stay on internals until it
   does (the suites in `src/track.rs` and `src/schedule.rs`). Status:
   OPEN and unchanged by the one door - the version gate answers
   `Unchanged` from version identity, not from the ledger. When this
   lands, the wrap order becomes unwritable and its known-bad twin is
   deleted rather than migrated.
2. **Error boundaries.** `Guarded` placement and the substitution tally
   are caller assembly today (`src/boundary.rs`). Status: OPEN; under the
   one door the tally would surface as an accessor on `Pipeline` rather
   than a counted drive variant.
3. **Non-linear graphs.** The builder builds chains; a diamond exists in
   the engine via `Arc<S>: Stage` (`libpipelinedata/src/stage.rs`) but has
   no builder spelling. Status: OPEN, orthogonal.
4. **Scheduling.** `Ledger::schedule` (`src/schedule.rs`) has no
   builder-level door; rides on finding 1. Status: OPEN.
5. **Store lifecycle.** `.stage_in` lets a cache outlive a build; there is
   no whole-pipeline store policy. Status: OPEN, orthogonal.
6. **A watched single poll.** Nothing public answers "what did THIS poll
   leave behind". Status: SUBSUMED by "Delayed keeps its promise" - the
   one door checks the wake path itself, and the finding closes by
   deletion rather than by a new door.
7. **The registration-site guarantee protects only what is registered.**
   A stage-authoring crate that never links the engine carries its
   versions unchecked; the gap closes per consumer, by an assembler
   existing. Status: unchanged by anything here.
8. **Assembling a pipeline takes two manifest edges** (`libpipeline` plus
   `libpipelinedata`). This crate could re-export the port so one edge
   suffices without weakening the split. Status: OPEN; the subcrate
   boundary below makes the facade the natural place to decide it, and it
   remains deliberately undecided.

## The intended stage shape: a function, with everything through Ctx

!!! PROPOSED

Recorded intent from 2026-08-24, compressed from the previous revision of
this document (full form in git history); orthogonal to the one-door
design - registration shape and run shape are independent decisions, and
nothing in the one door forecloses this.

* **A stage is a pure closure taking `Ctx`, registered as a `fn` pointer**
  - not `impl Fn` (permits capture), not a trait (permits fields). The
  type refuses captured state at compile time. There is deliberately no
  trait-taking variant: a door typed on the trait hands back a struct, and
  structs accrete fields that move the output without moving the key.
* **Everything a stage touches comes through `Ctx`**: reads through
  `Ctx::observe_read` so they enter the read-set; in-flight state between
  a `Pending` and a `Ready` lives in a store the consumer provides through
  a trait seam (as `MemoStore` already is), addressed by
  `(PipelineId, StageId)` - never in a field, and the `Ctx` carries ACCESS
  TO A STORE, not a world.
* **`PipelineId` is a shape hash plus a serial**: the hash over the
  `StageId`s in order identifies the pipeline's shape (a version bump
  changes it for free); the serial, minted at build, distinguishes
  instances, so keyed state dies with its instance.

## The ledger test, measured (a lesson in what a test holds)

A test relocated from a consumer's suite
(`the_ledger_scope_changes_speed_and_not_answers`) was examined by
mutation - break one thing, run the suite, see who notices - and found to
observe nothing its name claimed: deleting the tracking layer from it
changed no assertion, and the known-bad wrap order passed all four. The
reproduction that remained does build through the public door, as
`one_memo_serves_both_drivers_and_the_stage_runs_once` in
`tests/two_drivers_one_graph.rs`; the empty test was deleted rather than
parked, and should not return when finding 1 lands. The lesson is the
method: a test's name and comments claim what it holds; only mutating the
code under it shows what it observes.

## The subcrate boundary

!!! PROPOSED

Tim, 2026-08-24: "we should also use a subcrate boundary and explicit
re-exports where needed moving the internals tests to the subcrate."

Today "public versus internal" is a `pub(crate)` boundary, and the
measurement it produces is the 71-versus-30 ratio above: 71 tests live in
`src/` because they reach machinery the public API cannot spell. The
subcrate boundary redraws the line as a CRATE boundary - unfakeable, and
it gives the internals what they have never had: an API of their own to be
integration-tested through.

### The shape

A nested subcrate, `libpipeline/libpipeline-internals/`, on the
workspace's own precedent: `libpipelinedata-macros` nests inside
`libpipelinedata` because that subrepo's root IS its crate, and a path
dependency inside the workspace directory becomes a workspace member on
its own (`../libpipelinedata/Cargo.toml` records the reasoning). The same
constraint holds here, so the same shape applies.

### What moves, what stays, what is re-exported

**Moves to `libpipeline-internals`** - the seven machinery modules,
verbatim, with `pub(crate)` becoming `pub`:

* `src/chain.rs`, `src/memo.rs`, `src/driver.rs`, `src/watch.rs`,
  `src/boundary.rs`, `src/track.rs`, `src/schedule.rs`.

**Moves with them** - all 71 internals tests, out of `#[cfg(test)]`
modules and into `libpipeline-internals/tests/`, one file per module,
changed only in their imports: `src/track.rs`'s four suites
(`invalidation_marks_dependents`, `an_equal_recompute_stops_at_its_node`,
`reads_become_edges`, `a_fallback_is_not_a_revalidation`),
`src/schedule.rs`'s `the_schedule_polls_each_node_once`,
`src/boundary.rs`'s three (`a_boundary_is_not_a_cacheable_answer`,
`a_stage_boundary_catches_what_its_stage_raises`,
`a_build_can_ask_whether_it_stood_on_a_fallback`), and `src/watch.rs`'s
`tests`. Most of these arrived in `src/` at the visibility flip with docs
promising they "migrate back out unchanged but for the imports"; this is
that migration, to the boundary that can actually take them.

**Stays in `libpipeline`** - the facade: `src/lib.rs` and `src/builder.rs`
(the builder, `Pipeline`, `BuilderStore`, the `checked` id panic), the
four public test binaries in `tests/`, and the README with its doctests.

**Re-exported, explicitly** - the facade re-exports exactly today's public
vocabulary from the internals crate: `ChainError`, `DriveError`,
`PendingWork`, `NoPendingWork`, `WakePath`, `WakeReport` (until the door
collapse removes the last five). Nothing else: no glob, no module
re-export, each name a visible decision in `src/lib.rs`.

**Manifests** - `libpipeline-internals` depends on `libeffects` and
`libpipelinedata` only; `libpipeline` adds the path edge to it.
`tests/engine_stays_generic.rs` adds `libpipeline-internals` to
`THE_STACK`; its walk already follows path dependencies transitively, so
the new crate's manifest is checked without new machinery.

### What it costs, honestly

* **The dead-code arithmetic dies.** Everything `pub` in the internals
  crate is "used" by definition, so the
  `#![cfg_attr(not(test), allow(dead_code))]` discipline - the lint armed
  under test, catching machinery that becomes genuinely unused - is lost
  with the attributes it justified. The replacement measurements are the
  facade's explicit re-export list and the internals crate's own test
  suite; a weaker signal, and named as such.
* **The boundary is unfakeable but not unreachable.** A consumer could add
  a manifest edge to `libpipeline-internals` directly. Nothing in this
  subrepo can police other crates' manifests; the guard is the same one
  `libpipelinedata-macros` has - the edge is glaring in review, and the
  internals crate's docs state it is not a supported surface. What the
  boundary DOES make impossible is the accidental leak: no `pub(crate)`
  mistake, no test import that quietly widens, can expose machinery
  through `libpipeline` itself.
* **Two crates to compile and version instead of one**, one more manifest
  in the generic-stack allowlist, and cross-module doc links inside the
  internals stay intact while the facade's links to them become
  cross-crate paths.
* **The measurement moves rather than disappears.** The 71 tests become
  integration tests - of the INTERNALS crate. The count of tests in
  `libpipeline/tests/` remains the measurement of the public API's reach,
  and finding-driven migration outward still means facade tests. The
  ratio stops being "tests that admit defeat in `src/`" and becomes
  "internals coverage versus public coverage", which is what it always
  measured, now enforced by the compiler.

## Verdict: migrate, do not rewrite

The judgement was asked for plainly, so here it is plainly: MIGRATE. The
crate should not be rewritten, because the distance from what exists to
what is proposed is almost entirely in the facade, and the facade is the
smallest part of the crate.

The evidence, weighed:

* **The one door is a thin total mapping over machinery that exists.**
  `run` is the version gate (new: one `Mutex<Option<V>>` and one
  comparison) plus `FrameDriver::poll_frame` (exists, `src/driver.rs`)
  plus a four-arm match from `EffectPoll` to `Run` (new: one enum). The
  blocking drive it displaces becomes a documented caller loop whose
  reference semantics - `run_to_completion` and its watched and counted
  forms - stay as internals with their tests. No engine semantics change.
* **The four doors are facade, not engine.** The doors and their
  vocabulary re-exports total well under a hundred lines of
  `src/builder.rs` and `src/lib.rs`. Deleting doors is not a
  rewrite-scale event.
* **The 71 internals tests survive by motion, not reconstruction.** Their
  own module docs already promise an outward migration "unchanged but for
  the imports"; the subcrate split is that migration. A rewrite would
  forfeit 2,831 lines of tracked-layer implementation and its 45 tests
  (`src/track.rs`, `src/schedule.rs`) - machinery the new definition does
  not even touch - to arrive back at the same `Stage` contract.
* **What genuinely must be rewritten is bounded and identified**: the 30
  public tests and the README's 6 doctests, which speak the four-door
  vocabulary; the two-drivers file translates property-for-property into
  one-door-two-patterns form (its central claims - same answers, memo
  shared, wake obligations - are door-independent).
* **The version parameter threads as a type parameter, not a rewrite.**
  `Pipeline<S>` becomes `Pipeline<V, S>`; the builder's chaining types are
  untouched until `build`.

What would have tipped it the other way, and did not: if the outcome type
had needed the engine to distinguish `Computed` from `Unchanged` per
stage, the memo/track layers would have needed a new result channel
throughout - but the gate is at the root, and the engine below it already
answers everything the mapping needs.

## Migration plan

(This heading deliberately restores a section name that five source
comments still cite - the `#![cfg_attr(not(test), allow(dead_code))]`
notes in `src/boundary.rs`, `src/chain.rs`, `src/memo.rs`,
`src/schedule.rs` and `src/track.rs` refer to "`DESIGN.md`, 'Migration
plan'" - a citation left dead by an earlier revision of this document.
Dead citations mark real defects; this one is closed by the section
existing again.)

Ordered steps. Each step leaves `cargo test` green; the counts named are
the gates.

**Step 1 - the subcrate split (motion only).**
Create `libpipeline-internals/` (manifest: `libeffects`,
`libpipelinedata` path deps; same license and edition). Move
`src/{chain,memo,driver,watch,boundary,track,schedule}.rs` into its
`src/`, flipping `pub(crate)` to `pub` and deleting the five
`#![cfg_attr(not(test), allow(dead_code))]` attributes. Move the nine
`#[cfg(test)]` modules listed under "The subcrate boundary" to
`libpipeline-internals/tests/`, imports only. Facade `src/builder.rs`
imports `Chain`, `Memo`, `FrameDriver`, `run_to_completion`,
`run_to_completion_watched` from the internals crate; `src/lib.rs`
re-exports `ChainError`, `DriveError`, `NoPendingWork`, `PendingWork`,
`WakePath`, `WakeReport` from it explicitly. Add `libpipeline-internals`
to `THE_STACK` in `tests/engine_stays_generic.rs`.
*Gate*: facade 30 tests + 6 doctests; internals 71 tests; zero test
bodies changed; `builder_is_the_only_door.rs` unchanged and green.

**Step 2 - the outcome and the one door (the flip).**
In `src/builder.rs`: add `Run<Output, Error>` (exported from
`src/lib.rs`); give `Pipeline` the `V: Copy + Eq` parameter and the
`last: Mutex<Option<V>>` field; implement `run(&self, version, &input)`
as gate + `poll_frame` + mapping, recording the version only on `Ready`.
Delete `run`, `run_pure`, `run_watched`, `poll_frame` (the door, not the
internals they call); keep `take_stale` and `waker`. Drop the
`DriveError`/`PendingWork`/`NoPendingWork`/`WakePath`/`WakeReport`
re-exports from `src/lib.rs`; keep `ChainError`. Port the public tests:
`tests/builder_is_the_only_door.rs` re-spells its 8 tests through the one
door (pure graphs answer `Computed` first run, `Unchanged` second - which
finally lets the public suite assert the memo's headline directly);
`tests/two_drivers_one_graph.rs` becomes
`tests/one_door_two_patterns.rs`, its 12 properties translated (blocking
loop and wake-wait patterns, same answers, one memo, the
lost-not-late wake test spelled as `take_stale` staying false);
`tests/an_unwakeable_poll_is_visible_offline.rs` moves to
`libpipeline-internals/tests/` against `run_to_completion_watched`,
unchanged but for imports. Rewrite the README's examples (the doctests
are the gate). Sweep `libpipeline-internals` for citations of renamed
public tests and update them.
*Gate*: full suite green; `grep -rn "run_pure\|run_watched\|poll_frame\|PendingWork\|DriveError"`
finds nothing in facade `src/` public items or `tests/`; every test name
cited from internals docs exists (`grep` each cited name); ASCII check on
everything rendered or exported.

**Step 3 - Delayed keeps its promise.**
In the facade's `run`, on the `Pending` path under
`#[cfg(debug_assertions)]`, poll through
`libpipeline_internals::poll_watched` and panic on `WakePath::Missing`
with the lost-not-late diagnosis. Add a facade test
(`#[cfg(debug_assertions)]`, `#[should_panic]`) driving a stage that
forgets its waker.
*Gate*: full suite green in debug and `--release`.

**Step 4 - the document catches up.**
Flip this document's landed sections from `!!! PROPOSED` to unmarked
present tense, section by section as each step lands (part of each step's
review, listed once here so it is nobody's afterthought). Update "What
exists today" and the findings' status lines; retire the four-door
description into git history.

Deliberately NOT in this plan: wiring the tracked layer (finding 1),
boundaries (finding 2), non-linear graphs (finding 3), the `Ctx` stage
shape, and `PipelineId`. Each becomes strictly easier after the split -
they are builder spellings over an internals crate that now has a public
API to compose - and none of them blocks, or is blocked by, the one door.
