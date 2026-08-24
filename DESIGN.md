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

### Where a consumer works, and what it may never name

**A consumer of this crate implements `Stage` and assembles nothing.** `Stage`
and its contract types live in `libpipelinedata`, which exists precisely so a
consumer can DECLARE a step - an id, a version, an input, an output, a memo key
- without linking the engine that runs it. That crate's own module doc says so:
"the pipeline's traits and types: the stage contract, the key types and the
storage seam", with `Stage` as "one step of a lowering pipeline". It is the
port; this crate is the implementation behind it.

Everything on the composition side - `Tracked`, `Memo`, `Chain`, the ledger,
the drivers - belongs to whoever LINKS the engine, and reaches a consumer's
stage only by that assembler registering it. A consumer that names
`Tracked::new(&ledger, label, Memo::new(stage, store))` is not merely reaching
past the builder; it is working at a layer it has no business at, and under the
correct split it cannot spell those types at all, since it does not depend on
this crate. That is the rule, and it is what makes the ledger test's old home
in `highbay_data` wrong independently of whether the test itself was any good:

* A consumer-level test that hand-composes the stage graph is testing the wrong
  LAYER, whether or not it passes.
* The remedy is never to re-export a type so such a test compiles. If a
  consumer-level property is genuinely wanted, it is expressible through the
  builder or it is a finding here.
* Where such a property is held over a stand-in stage inside this crate, that
  is the RIGHT shape rather than a lesser substitute for the real stage: the
  engine is generic, so a stand-in exercises exactly what a real stage would.

### What tests may touch

Only the above. A test that needs anything from the internals section is a
FINDING that the builder cannot express something a consumer will need; record
it here, do not re-export.

**Which makes the count of tests in `tests/` the measurement.** It is what the
public API can reach: **30 tests in 4 binaries** (29 at the flip), against **71
unit tests** in `src/` that admit it cannot. When a finding closes, its tests
migrate OUTWARD and that first number goes up. The first such migration was not
a finding closing but a test being measured and found empty - see "The ledger
test, measured" below. A test moved inward is placed in the
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

## The intended stage shape: a function, with everything through Ctx (PROPOSED)

None of this section is built. It is the shape the API is heading for,
recorded so the next wave does not re-derive it and so the trait-taking
doors are not widened by habit first.

### A stage is a FUNCTION, and Rust can enforce that

The default registration door takes a `fn` pointer, not `impl Fn` and not
a trait object:

```
stage_fn(name, version, f: fn(&I, &mut Ctx) -> O)
```

A non-capturing closure coerces to `fn`; a capturing one does not. So the
type REFUSES captured state at compile time - no lint, no convention, no
review. `impl Fn` would permit capture and a trait would permit fields;
`fn` permits neither.

Two forms, split by what they are for:

| form | holds | keyed by | for |
|---|---|---|---|
| `stage_fn(name, version, fn)` | nothing, provably | `StageId` alone | wrappers - a `Component` expansion, most steps |
| `stage(name, version, make)` | per-build config | `StageId` + a `ContentKey` of that config | the minority, e.g. `ExpandStage`'s `environment` |

**Losing the content key on the first form is the POINT, not a cost.** A
stage with no captured state has exactly one input; if the source's
version already covers it, hashing it is paying `O(input)` to discover
what the write path recorded. Those stages belong at the cheapest tier of
invalidation, which is where measurement already put them.

### Why the trait-taking door must not become the default

Tim, 2026-08-24: *"if we did have an add_tracked_stage taking the full
trait that might cause problems later on with local state getting added to
the structs."*

A door typed on the trait hands back a struct, and structs accrete: today
`{ id }`, later `{ id, cache, last_seen, config }`, each new field a
candidate input that moves the output without moving the key, and none of
it arriving as a visible decision. An exhaustive destructure in
`memo_key` catches that only if someone remembers to write it that way. A
`fn`-typed door makes the field IMPOSSIBLE rather than reviewable, which
is the difference this crate keeps choosing.

So when a tracked variant lands (finding 1), `tracked_stage_fn` comes
first and the trait-taking form exists only where per-build config
genuinely does.

### In-flight state lives in `Ctx`, addressed - not in a field

The objection to the `fn` form is that a stage which polls `Pending` then
`Ready` needs somewhere to keep its work between polls, and that somewhere
looks like a field.

It is not. Tim, 2026-08-24: *"it feels more like that's the role of
ctx/cx, and we already have the concept with ecs keyed on widget (in this
case, stageid and pipelineid)."*

The addressing pattern is the one to copy: state lives in a store and the
thing using it holds only an ADDRESS - `(PipelineId, StageId)` here, where
a widget id serves the same role in the consumer's world. What is NOT
copied is the backing. Tim, same date: *"MemoStore is a trait we provide
to the builder, the ecs is not libpipeline's concern."*

So the store is a SEAM, not a structure this crate owns. `MemoStore`
(`libpipelinedata`'s `src/store.rs`) is already exactly that - a trait,
with `stage_in` on the builder taking a caller-provided implementation -
and in-flight state should reach the `Ctx` the same way: through a
trait the consumer satisfies, addressed by `(PipelineId, StageId)`.
Whether a consumer backs it with an ECS, a map, or anything else is its
business and is invisible here.

**Correcting an earlier draft of this section**, which said in-flight work
lives "in the world the `Ctx` carries". That baked an ECS into the
engine, which is the opposite of what the seam exists for. The `Ctx`
carries ACCESS TO A STORE; it does not carry a world.

That collapses the design to one rule - **a stage is a function, and
everything it touches comes through `Ctx`**:

* reads through `Ctx::observe_read`, so they enter the read-set and
  invalidation stays precise;
* in-flight state in `Ctx`, addressed, so a pending poll resumes without
  a field;
* writes through the same door, so nothing escapes the log.

Purity stops being a convention and becomes structural: there is nowhere
else to reach. No captured environment (the type forbids it), no fields
(there is no struct), and the only handle in scope is the one that logs.

### `PipelineId` (PROPOSED): a shape hash plus a serial

Tim, 2026-08-24: *"I think it's a quick hash over the stageids in order,
along with a serial when the pipeline is constructed via the builder."*

Two halves answering two questions:

* **The hash over the `StageId`s IN ORDER** identifies the pipeline's
  SHAPE - which stages, in which order. Because a `StageId` is
  `(name, version)`, a version bump changes the shape hash automatically,
  so state keyed on a pipeline cannot survive a stage's behaviour
  changing underneath it. That property is free and worth naming.
* **The serial, minted by the builder at construction**, distinguishes
  INSTANCES. Two pipelines of identical shape built separately are
  different instances - which is exactly the authoring/runtime case: same
  stages, same shape hash, different serial, different memo stores, and
  both correct while holding different cached answers.

Keyed state therefore dies with its instance: rebuild the pipeline and the
serial changes, so nothing leaks across. Same instance and same stage
resumes. That is the load/unload rule one layer down, and it needs no new
vocabulary.

**Note the addressing question is separate from the store's backing.** A
memo store is addressed by a `MemoKey` - stage id plus content keys, a
CONTENT address. In-flight state is addressed by IDENTITY,
`(PipelineId, StageId)`. Two different questions over possibly the same
backing, and an implementation should say which of the two any given
store answers. That the backing might be one an implementor already has
(an ECS world, say) is a consumer's convenience and not a fact this crate
knows.

## What the builder cannot yet express (findings, in priority order)

1. **Tracked state graphs.** `Ledger`/`Tracked`/`TrackedInput` composition -
   including the load-bearing order "wrap the memo in the tracking, not the
   tracking in the memo" - is exactly the kind of assembly the builder exists
   to own, and it is not in the builder yet. Sketch: `.tracked_input(label,
   value)` and a `stage` variant taking a ledger label, with the builder
   owning the ledger and the wrap order so the known-bad composition becomes
   unwritable. Until then the tracked layer stays private and the 45 tests
   over it stay on internals (`invalidation_marks_dependents`,
   `an_equal_recompute_stops_at_its_node`, `reads_become_edges`,
   `a_fallback_is_not_a_revalidation`, `the_schedule_polls_each_node_once`).

   The exact expression a consumer cannot write today, and its error:

   ```text
   error[E0432]: unresolved imports `libpipeline::Ledger`, `libpipeline::Memo`,
                 `libpipeline::Tracked`
   ```

   **When this finding lands, the wrap order stops being testable, and that is
   the point.** A builder that owns the order makes `Memo::new(Tracked::new(..),
   store)` unspellable; a test asserting the order is then asserting about a
   composition nobody can construct, which is worse than no test because it
   implies the mistake is still reachable. `Memo`'s known-bad twin
   `a_cache_outside_the_tracking_is_a_cache_the_ledger_cannot_reach` is
   therefore scheduled for DELETION with this finding, not migration - it is
   the record of why the builder owns the order, and the builder will be that
   record.
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
7. **Nothing in the workspace assembles a pipeline from two of the three
   stages.** `ExpandStage` and `ExpandDefinitionStage` have no
   `PipelineBuilder::stage` call site anywhere, so their tests construct them
   with `StageId::new(NAME, version)` at the call site instead - the same shape
   and the same placement of the version, without the builder's registration
   check behind it. The version-at-the-registration-site guarantee is
   unenforced for them until an assembler exists.

   **The crate boundary is CORRECT and is not what this finding names.**
   `highbay_elements` links `libpipelinedata` and deliberately never
   `libpipeline`, stating the rule in its own manifest ("NO engine -
   `libpipeline` is deliberately not named here, and a stage that needed it
   would be evidence the data/engine cut is in the wrong place"). That is the
   port/adapter split working as designed, per "Where a consumer works" above:
   the consumer declares its stage against the port and does not assemble.
   A reader who takes this finding as "the boundary is wrong" will fix it by
   adding the dependency edge that crate refuses on purpose. The gap is in what
   has been BUILT - an assembly site on the authoring side - not in how the
   crates are split.

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
  `highbay_data`'s `tests/assemble_under_the_driver.rs`, into `src/track.rs`,
  and RETIRED on 2026-08-24 after being measured. See "The ledger test,
  measured" below: the ledger scope was inert in it, and what it actually held
  is now `tests/two_drivers_one_graph.rs`'s
  `one_memo_serves_both_drivers_and_the_stage_runs_once`, through the public
  builder. Net test count unchanged at 101; the split moved one test outward
  (30 in `tests/`, 71 in `src/`).

## The ledger test, measured (2026-08-24)

`the_ledger_scope_changes_speed_and_not_answers` was the one test the flip
relocated INTO this crate rather than moving between directories, and finding 1
was recorded as the reason it could not be written through the public API. It
was re-examined by asking, of each of its four assertions, what defect would
make it fail. The method was mutation: break one thing, run the suite, see who
notices.

```rust
let ledger = Ledger::new();
let tracked = Tracked::new(&ledger, "test.pure", Memo::new(Pure::new(), MemoMap::new()));
let first = offline(&tracked, &input);
let second = per_frame(&tracked, &input);
assert_eq!(tracked.stage().stage().runs(), 1);   // 1
assert_eq!(first, second);                       // 2
assert_eq!(first, offline(&Pure::new(), &input));// 3
assert!(!ledger.is_stale(tracked.node()));       // 4
```

| mutation | subject test | who else notices |
|---|---|---|
| `Memo::new(Tracked::new(..), store)` - the KNOWN-BAD order | **passes** | `a_cache_outside_the_tracking_is_a_cache_the_ledger_cannot_reach` |
| delete the `Tracked` wrapper outright | **passes** | nothing - it was inert |
| delete `Memo`'s `!revalidating()` gate | **passes** | 2 tests |
| `revalidating()` true whenever a scope is open | fails (1) | 4 tests, 3 with a real edge |
| `Tracked` marks stale after a `Ready` poll | fails (4) | 20 tests |

What that establishes:

* **The test contains nothing about the ledger.** Deleting the tracking layer
  from it changes no assertion, so the `Ledger` in its name and the "scope" in
  its comment were never observed. Its comment on assertion 1 - "the lookup
  sits inside the node's scope" - names the wrap order, and the inverted,
  known-bad order passes all four assertions.
* **The SPEED half is the memo's, not the ledger's.** With no `TrackedInput` in
  the test, nothing can go stale, so `revalidating()` is false either way and
  the scope cannot change a lookup's outcome. Deleting the gate the assertion
  appeals to leaves the test green.
* **Assertions 2 and 3 cannot fail here.** `Tracked` returns its inner poll
  verbatim; the only route by which tracking changes an ANSWER is a stale value
  served from a store, which needs a tracked input to move.
* **Assertion 4 is close to tautological** and owned elsewhere:
  `polling_a_node_clears_its_staleness_and_a_pending_poll_does_not` is the test
  for it, over a node that was actually stale first.
* **What the ledger is FOR is tested elsewhere and well** - dependency edges and
  selective invalidation live in `reads_become_edges`,
  `invalidation_marks_dependents` and `an_equal_recompute_stops_at_its_node`.
  This test was never part of that.

**Outcome: a defect in the TEST, not in the builder** - and the reproduction
that remains after the empty half is removed DOES build through the public
door, as `one_memo_serves_both_drivers_and_the_stage_runs_once`. It reuses the
file's existing stand-ins, so the cost was zero new machinery. It also covers a
gap: `both_drivers_give_the_same_answer_for_the_same_graph` builds one graph per
driver, so until now nothing measured ONE store serving both.

The test is therefore deleted rather than parked, and should not return when
finding 1 lands. Two independent reasons, either of which is sufficient:

1. It asserts a property the builder is meant to make UNWRITABLE (the wrap
   order) - and does not even assert it successfully. A test that survives its
   own fix by testing the now-unconstructible case implies the mistake is still
   reachable.
2. Its consumer-level form should not come back to `highbay_data` either, per
   "Where a consumer works": a consumer does not assemble the stage graph, so
   the "parked half" the flip recorded - holding the property over a REAL stage
   from the consumer - was never a thing to restore. A stand-in inside this
   crate is the right shape, not a substitute for one.

## State of the implementation

DONE: `src/builder.rs` (builder, runner, id check at registration, intrinsic
memoization with owned/given/off stores); `tests/builder_is_the_only_door.rs`;
the CONSUMER CONVERSION and the VISIBILITY FLIP, with every internals-reaching
test moved into the module that owns the internal it reaches for.

NOT done: findings 1-7. Each one is a test in `src/` that wants to be a test in
`tests/` - with the correction that finding 1's tests want to move outward as
INVALIDATION tests, and one of them (`a_cache_outside_the_tracking_..`) wants to
be deleted instead, because the builder will be the thing that holds what it
holds. `the_ledger_scope_changes_speed_and_not_answers` was never evidence for
finding 1 and is gone; the finding stands on the 45 tests that do exercise a
ledger.
