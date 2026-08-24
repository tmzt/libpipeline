# libpipeline: design

`libpipeline` is an incremental computation engine for pipelines of pure
stages: a source value goes in, derived outputs come out, and everything
between is memoized, dependency-tracked, and drivable either one poll per
frame or blocking to completion. The engine is generic over every payload
type - it never learns what a stage computes, only how to poll it, key it,
and remember its answers.

This document has three parts. THE MODEL is the set of ideas the engine
embodies; source comments cite its section names. THE CURRENT DESIGN is the
builder-only public API and why it is shaped that way. The rest is the
ledger: what is proposed, what the builder cannot yet express, and one
lesson about testing that was expensive to learn. The public-API section is
the contract consumers and tests are allowed to touch; everything in the
internals section is machinery the builder assembles and may reorganize
without notice.

## The model

Ten commitments. Each is carried by a specific piece of the crate, named in
parentheses.

### A pipeline is a chain of pure stages

A pipeline is a chain of stages from a source to a derived output. A stage
consumes one input type and produces one output type; stages compose by the
next stage's `Input` equaling the previous stage's `Output`. The composite
of two stages is itself a stage, so a graph is never a second kind of thing
a driver must know how to walk: a four-level lowering is three composites
nested, driven with the same two methods as a leaf (`src/chain.rs`).

### Poll and wake, not a one-shot future

A stage is driven by a poll/waker protocol rather than awaited as a one-shot
future. A poll answers one of three things: `Ready(value)` ("here is the
current value", not "finished"), `Failed(error)` (the typed error channel),
or `Pending` - "I cannot answer yet". Answering `Pending` OBLIGES the stage
to arrange for the supplied waker to be woken when the answer becomes
possible; a stage that forgets has made its value lost rather than late, and
`src/watch.rs` exists to catch exactly that. The contract lives in
`libpipelinedata::Stage`, which reuses `libeffects`' poll protocol: a stage
bound to an input IS an effect.

### The lookup precedes the work

Memoization is keyed by `(stage identity, content keys of the inputs)` - a
key computable BEFORE the stage runs. That ordering is the design's center
of gravity: a lookup that precedes the work can skip the work, rather than
validate it afterwards. An unchanged input hits at the first stage and the
rest of the chain is never entered.

Only `Ready` is recorded. `Pending` is not a value, and `Failed` is
deliberately never cached - the standing rule is that effects are never
replayed by an implicit cache, and a cached failure is exactly that: a
transient failure (a network that was down, a file not yet written) served
back as a settled fact under a key that says it is fresh. A stage that
cannot honestly key an input answers `memo_key -> None` and is neither
looked up nor recorded; refusing to key is the safe answer, faking one is
not (`src/memo.rs`).

### Content addressing is a streaming hash

A content key is produced by streaming a value's parts through a hasher -
never by serializing to a buffer and digesting the bytes. The vocabulary
(`ContentKey`, `ContentHash`, the streaming `ContentHasher` and the derive)
lives in `libpipelinedata`; this crate only trusts that equal values give
equal keys.

### The store is a seam

WHERE answers are remembered is a separate decision from the engine.
`MemoStore` (in `libpipelinedata`) is a two-method trait - `lookup` and
`record` - and the builder accepts any implementation per stage
(`stage_in`), owns a fresh map by default (`stage`), or turns every store
off (`uncached`). One map-backed store is enough for this crate's entire
test suite; heavier backends are a consumer's business and are invisible
here.

### Reads are observed, not declared

Dependency edges are RECORDED BY OBSERVING READS, never accepted as a
declared list - the salsa / MobX / Vue / Solid mechanism. While a stage
runs, every tracked read is logged as an edge; the set is re-logged on every
run so it follows conditionals (a branch not taken this run contributes no
edge, so a change behind it wakes nothing). Changing a tracked input marks
every node that read it - transitively - stale, and wakes whoever
subscribed. Invalidation is selective: what did not read the change is not
touched (`src/track.rs`, `src/schedule.rs`).

### An equal recompute stops at its node

Early cutoff, above the leaf: a node that recomputes to a value whose
content address equals what it addressed to last time RETRACTS itself as a
reason for its consumers to re-run. The recompute stops at its node rather
than cascading; without cutoff, every keystroke invalidates the whole
pipeline (`Backdated` in `src/track.rs`). At the leaf, setting a tracked
input to the value it already had marks nothing stale at all.

### Two drivers, one graph

The same set of stages runs under two drives, and A STAGE CANNOT TELL WHICH
ONE IS POLLING IT. That is what makes an interactive host and a batch tool
one API rather than two implementations that agree by convention.

* The FRAME drive polls once and returns immediately, whatever the answer. A
  `Pending` frame draws its stand-in; the registered waker marks the
  pipeline stale when the value lands; the next frame polls again. Nothing
  waits inside a frame, ever.
* The BLOCKING drive polls until a value or a typed failure, pumping an
  executor seam (`PendingWork`) while polls answer `Pending`. Its waker is
  deliberately a no-op - it re-polls unconditionally after pumping, so
  nothing depends on being woken. A `Pending` with nothing left to pump is a
  `Stalled` error, a real end state: something waited for an input nothing
  was going to land.

A batch run against an unchanged tree is all cache hits, because the memo
keys are the same ones the interactive host used. The blocking drive also
comes in a WATCHED form that reports, alongside an unchanged answer, every
`Pending` poll that left no wake path - each one a value a frame drive would
lose rather than receive late (`src/driver.rs`, `src/watch.rs`).

### A boundary refuses the cache

An error boundary turns a `Failed` poll into a substituted `Ready` - which
LAUNDERS an uncacheable answer into a cacheable-looking one. Cache it and
the key says "input X, value V" while V is the fallback; the input never
moves, so the key never moves, and the real value is never computed again.
So the stage-level boundary (`Guarded`, `src/boundary.rs`) answers
`memo_key -> None` structurally, and a substitution COUNT rides alongside
the drive's result, separating "built" from "built on fallbacks" without
giving the two drivers different return types. The recovery mechanism
itself is `libeffects`' and is not duplicated here.

### The engine stays generic

The engine never learns a consumer's types. Everything is generic over
`S: Stage`; nothing matches on a concrete payload type, because none is in
scope to match on. Every test in this crate invents STAND-IN types of its
own (`Source`, `Lowered`, `Text`, dotted-path strings) - if the suite could
not be written without importing a consumer's IR, the engine would have
learned something it must not know. The proof is mechanical:
`tests/engine_stays_generic.rs` walks this crate's manifest and, through its
path dependencies, every manifest under it, and fails if the tree names a
crate outside the stack's own closed allowlist. A rule that holds
transitively cannot be evaded by routing an edge through a sibling.

The stack is three crates, dependencies pointing strictly downward:

| crate | role |
|---|---|
| `libpipeline` | the engine: memoization, tracking, scheduling, the two drivers |
| `libpipelinedata` | the port: `Stage`, the key types, `ContentHash`, `MemoStore` |
| `libeffects` | the base: the poll/waker protocol, boundaries, wake flags |

## Why the builder is the only door

An earlier revision of this crate exported its machinery flat - 23 items,
no builder. An API that invites assembly at each call site gets assembly at
each call site: two consumers hand-rolled their own memoization BESIDE the
crate rather than composing the memo layer, and both were later found
defective. The same flat surface let a stage be used un-memoized
(memoization was something a caller remembered), and let a stage's version
live far from the behaviour it versions - one consumer declared its
`StageId` 761 lines from the key construction it governed. A stale version
makes the memo serve a value computed by the OLD behaviour under a key
claiming to be current, and nothing downstream can detect that.

Three decisions follow, and the current design is their consequences:

1. **The builder is the only public way to compose, memoize or drive.**
   `Stage` stays public to IMPLEMENT - a consumer must be able to write one -
   but not to compose, memoize or drive by hand.
2. **Memoization is intrinsic to registration.** There is no un-memoized
   `add_stage`. A stage that must not be served from cache says so through
   `memo_key -> None` (the vocabulary that already exists for exactly this),
   not by a caller forgetting a wrapper.
3. **The version is declared at the registration call site.** `stage(name,
   version, |id| ...)` puts the number in the same lexical scope as the
   closure that constructs the behaviour it versions. Much of this code is
   written by LLM agents, which do not scroll to a module-level `const` to
   ask whether it should move; the invariant has to be in the way, not
   remembered.

## Public API

### The builder

```rust,ignore
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
  (a fresh map per stage); this is why `S::Output: Clone` is required.
  Stages chain: the Nth stage's `Input` must equal the (N-1)th's `Output`.
* `.stage_in(name, version, store, make)` - same, but memoized in a store the
  CALLER provides (`impl MemoStore<S::Output>`). This is how a cache outlives
  one build of the pipeline (an interactive session rebuilding its graph),
  and it is what makes the version rule observable in a test: share a store
  across two builds, bump the version, and the second build must recompute.
* `.uncached()` - the control switch: every stage runs with a store that
  remembers nothing. Kept public because "a pipeline whose ANSWERS change
  when the cache is disabled has a bug the cache was hiding" is a property
  consumers should be able to assert about their own graphs.
* `.build() -> Pipeline<impl Stage<...>>` - the runner. The graph type is
  opaque (`impl Stage`); consumers hold it by inference and can never reach
  the machinery inside.

### The runner

`Pipeline<S>` is generic over the opaque graph and exposes the two drive
modes - same graph, same keys:

* `.run(&input, &work) -> Result<Output, DriveError<Error>>` - the blocking
  drive. `work: impl PendingWork` pumps whatever a `Pending` poll is waiting
  for.
* `.run_pure(&input)` - `run` with `NoPendingWork`, for graphs of pure
  stages.
* `.run_watched(&input, &work) -> (Result<...>, WakeReport)` - the same
  drive, reporting `Pending` polls that left no wake path (each one a value
  a frame drive would lose rather than receive late).
* `.poll_frame(&input) -> EffectPoll<Output, Error>` - the frame drive: one
  poll, returns immediately, never blocks.
* `.take_stale() -> bool` - whether a wake arrived since last asked ("stale,
  poll again"); reading clears it.
* `.waker() -> Waker` - for landing values out of band.

### What else stays public, and why

* **`Stage` and its contract types** (`Stage`, `StageId`, `MemoKey`,
  `ContentKey`, `EffectPoll`, `MemoStore`, `MemoMap`, `NoMemo`) - these live
  in `libpipelinedata`, not here; a stage AUTHOR needs them and they are the
  implement-side contract, not composition machinery.
* **`PendingWork` / `NoPendingWork`** - the executor seam. Which executor
  (and whether there is one) is the caller's decision; the engine may not
  link one.
* **`DriveError`** - what `run` answers with; `Stalled` is a real end state.
* **`ChainError`** - result VOCABULARY, not machinery: the runner's error
  type for a multi-stage pipeline is nested `ChainError`, and a caller
  matching on which stage failed needs the name. (`Chain` itself - the type
  that builds such graphs - is internal.)
* **`WakePath` / `WakeReport`** - the watched drive's findings.

`WakePath` is the one item on that list no public signature names, and it is
worth saying so rather than letting a reader assume otherwise: the runner's
watched door is `run_watched`, which reports a `WakeReport` (counts). It
stays exported as that report's vocabulary. The tests that read a per-poll
path are `poll_watched`'s, and they are unit tests in `src/watch.rs` for
exactly that reason - see finding 6.

### Where a consumer works, and what it may never name

**A consumer of this crate implements `Stage` and assembles nothing.**

`Stage` is a TRAIT, today and in every shipped version of this crate. It
lives in `libpipelinedata` (`src/stage.rs`), a consumer implements it on a
type of its own, and the builder takes `S: Stage`. The closure form
described later in this document is PROPOSED and does not exist; where the
two disagree, the trait is what is true now. Nothing below should be read
as saying a stage is already a function.
`Stage` and its contract types live in `libpipelinedata`, which exists
precisely so a consumer can DECLARE a step - an id, a version, an input, an
output, a memo key - without linking the engine that runs it. It is the
port; this crate is the implementation behind it.

Everything on the composition side - the tracked layer, the memo layer, the
chain, the ledger, the drivers - belongs to whoever LINKS the engine, and
reaches a consumer's stage only by that assembler registering it. A consumer
that names `Tracked::new(&ledger, label, Memo::new(stage, store))` is not
merely reaching past the builder; it is working at a layer it has no
business at, and under the correct split it cannot spell those types at all,
since it does not depend on this crate. Three consequences:

* A consumer-level test that hand-composes the stage graph is testing the
  wrong LAYER, whether or not it passes.
* The remedy is never to re-export a type so such a test compiles. If a
  consumer-level property is genuinely wanted, it is expressible through the
  builder or it is a finding here.
* Where such a property is held over a stand-in stage inside this crate,
  that is the RIGHT shape rather than a lesser substitute for the real
  stage: the engine is generic, so a stand-in exercises exactly what a real
  stage would.

### What tests may touch

Only the above. A test that needs anything from the internals section is a
FINDING that the builder cannot express something a consumer will need;
record it here, do not re-export.

**Which makes the count of tests in `tests/` the measurement.** It is what
the public API can reach: **30 tests in 4 binaries** (plus the README's
doctests), against **71 unit tests** in `src/` that admit it cannot. When a
finding closes, its tests migrate OUTWARD and that first number goes up. A
test moved inward is placed in the MODULE THAT OWNS THE INTERNAL it reaches
for - not in one collecting `mod tests` at the crate root - so it moves with
that module when the module is reshaped, and so that reaching an internal
stays local and visible rather than having a sanctioned home.

## Internals (assembled by the builder, never exported)

All of the below is `pub(crate)`. `boundary.rs`, `schedule.rs`, `track.rs`,
`chain.rs` and `memo.rs` each carry
`#![cfg_attr(not(test), allow(dead_code))]`: with no flat exports, the parts
the builder has no spelling for have no caller but their own tests, and the
`not(test)` form keeps the lint fully armed under `cargo test` so anything
that becomes genuinely unused still fails the gate. The allow comes off when
the builder becomes the caller.

* **`Chain`** (`src/chain.rs`) - two stages composed, itself a `Stage`. The
  builder nests these; the composite id is a fixed internal `StageId`
  because a chain never keys (`memo_key -> None`, its parts are memoized
  instead).
* **`Memo`** (`src/memo.rs`) - the memo layer: lookup precedes the work,
  only `Ready` recorded, store skipped while `revalidating`. Merged into
  registration: every `stage()` call wraps its stage in one.
* **`FrameDriver`** (`src/driver.rs`) - held inside `Pipeline`, surfaced as
  `poll_frame`/`take_stale`/`waker`.
* **`run_to_completion`**, **`run_to_completion_watched`**,
  **`poll_watched`** (`src/driver.rs`, `src/watch.rs`) - surfaced as
  `run`/`run_watched`.
* **`BuilderStore`** (`src/builder.rs`) - the store the builder wraps around
  each stage: owned map / caller-given / off (the `.uncached()` control).
* **The tracked layer** (`src/track.rs`: `Ledger`, `Tracked`,
  `TrackedInput`, `Backdated`, `NodeId`, `revalidating`) and
  **`Schedule`/`Cycle`** (`src/schedule.rs`) - internal, and private, even
  though the builder cannot yet express them (findings 1 and 4). The tests
  over these layers live in `src/track.rs` and `src/schedule.rs`.
* **`Guarded`/`Substitutions`/`run_to_completion_counted`**
  (`src/boundary.rs`) - same, with their tests in `src/boundary.rs`
  (finding 2).
* **`poll_watched`** (`src/watch.rs`) - the watched SINGLE poll.
  `run_watched` is the public watched drive; there is no public single-poll
  counterpart (finding 6).

Two composition rules hold by convention rather than by type, and each is
stated in the owning module's doc and pinned by a known-bad twin: a cache
belongs INSIDE the tracking (`Memo`'s doc - a cache outside the tracking is
a cache the ledger cannot reach), and a boundary belongs OUTSIDE the
tracking (`Guarded`'s doc - a substituted `Ready` tells the ledger the node
is up to date when it is still owed its real answer). The third of that
family - a boundary belongs outside the MEMO - is closed structurally by
`Guarded` refusing to key at all.

## Not built yet (engine-level, distinct from the builder findings)

* **The derived-key fold for composites.** A chain's own memo key would be a
  fold `H(stage_id, key(inputs))` over its parts; until that exists, `Chain`
  honestly refuses to key and its parts are memoized individually. Nothing
  is lost meanwhile - the cheapness argument is about hitting at the FIRST
  level.
* **Deep verification.** `Backdated` cuts off where a node's output REPEATS,
  which needs the node to have run. A node whose consumers could be spared
  before it runs at all - an unchanged dependency set being enough, as in
  salsa's deep verify - is not here. Neither is any policy for which nodes
  are worth addressing on every poll: `Backdated` is opt-in per node because
  the address costs a traversal of the output, and a chain that backdates at
  every level pays for it at every level.

## The intended stage shape: a function, with everything through Ctx (PROPOSED)

None of this section is built. It is the shape the API is heading for,
recorded so the next wave does not re-derive it - and so that the
trait-taking door the builder has TODAY (`stage`/`stage_in`, taking
`S: Stage`) is understood as the thing being replaced rather than as the
general case with a convenience beside it.

### PROPOSED: a stage WOULD BE a pure closure taking `Ctx`, with no other form

The registration door takes a `fn` pointer - not `impl Fn`, not a trait:

```text
stage_fn(name, version, f: fn(&I, &mut Ctx) -> O)
```

A non-capturing closure coerces to `fn`; a capturing one does not. So the
TYPE refuses captured state at compile time - no lint, no convention, no
review. `impl Fn` would permit capture and a trait would permit fields;
`fn` permits neither.

**There is no trait-taking variant, and that is the decision rather than
an omission.** Tim, 2026-08-24: *"we just dismissed the trait variant as
something that grows local state we don't want. the pipeline has pure
closures as stages taking cx: Ctx."*

An earlier draft of this section kept a second door for stages with
"genuine per-build config", citing one consumer's expansion stage - it held
an `environment` field, a `ContentKey` over the definition tables it
expands against, computed once at construction and folded into every memo
key. That was preserving an existing shape rather than following the design
through.

**A resource a stage reads comes through `Ctx`, and the read-set covers
it.** The definition tables are things the expansion READS. Reaching them
through `cx` puts them in the read log (`Ctx::observe_read`), which is
strictly better than the field:

* nothing is hashed at construction, so a stage that never consults the
  table on a given run does not carry it in that run's read-set;
* the set is re-addressed per run rather than frozen when the stage was
  built, which closes the staleness the field's own doc had to assume away
  (the table could not change mid-build, and the field was only correct
  because of that);
* and the operand stops being something an author must remember to fold
  into `memo_key` - the door that logs it is the only door there is.

The manual key operand existed because there was a struct to hang it on.
Remove the struct and the reason goes with it.

**What a stage is keyed by, then**: its `StageId` - name and version, the
version declared at the registration call site - plus whatever its run
actually read, as the read-set records it. No `ContentKey` computed over a
whole input to discover what the source's version already says, and no
per-stage decision about which fields belong in the key, because there are
no fields.

**This binds every door that comes later, including the tracked one.**
When finding 1 lands it is a CLOSURE form - a tracked registration taking
`fn`, with the builder owning the ledger and the wrap order. Tim,
2026-08-24, on the alternative: *"if we did have an add_tracked_stage
taking the full trait that might cause problems later on with local state
getting added to the structs."* A door typed on the trait hands back a
struct, and structs accrete: today `{ id }`, later
`{ id, cache, last_seen, config }`, each field a candidate input that
moves the output without moving the key, none of it arriving as a visible
decision. The point of the `fn` door is that the field is IMPOSSIBLE
rather than reviewable, and a second door typed on the trait would give
that back for whichever stage was written through it.

### PROPOSED: in-flight state would live in `Ctx`, addressed - not in a field

The objection to the `fn` form is that a stage which polls `Pending` then
`Ready` needs somewhere to keep its work between polls, and that somewhere
looks like a field.

It is not. Tim, 2026-08-24: *"it feels more like that's the role of
ctx/cx, and we already have the concept with ecs keyed on widget (in this
case, stageid and pipelineid)."*

The addressing pattern is the one to copy: state lives in a store and the
thing using it holds only an ADDRESS - `(PipelineId, StageId)` here, where
a widget id serves the same role in a UI consumer's world. What is NOT
copied is the backing. Tim, same date: *"MemoStore is a trait we provide
to the builder, the ecs is not libpipeline's concern."*

So the store is a SEAM, not a structure this crate owns. `MemoStore` is
already exactly that - a trait, with `stage_in` on the builder taking a
caller-provided implementation - and in-flight state should reach the `Ctx`
the same way: through a trait the consumer satisfies, addressed by
`(PipelineId, StageId)`. Whether a consumer backs it with an ECS, a map, or
anything else is its business and is invisible here.

**Correcting an earlier draft of this section**, which said in-flight work
lives "in the world the `Ctx` carries". That baked a specific backing into
the engine, which is the opposite of what the seam exists for. The `Ctx`
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

## What a run answers, and what each answer means

Three result types, at three different scopes. A consumer meets all of
them, and their meanings are not guessable from their names.

### `EffectPoll<A, E>` - what ONE poll answers

This is what `poll_frame` returns and what a stage's own poll produces.

| variant | meaning |
|---|---|
| `Ready(A)` | a value |
| `Pending` | an upstream effect has not landed; a waker was registered, so expect a wake |
| `Failed(E)` | the stage's typed error channel |

**Neither `Ready` nor `Failed` is terminal**, and that is the part worth
saying out loud because it differs from `Result`. A pipeline is a standing
derivation over inputs that change, so a later poll over changed inputs
can move `Failed` back to `Ready` - and can move `Ready` to a different
value. A `Failed` is this poll's answer, not the pipeline's verdict. Only
`Pending` carries an obligation: the poll that returned it MUST have
registered a waker, or the value is lost rather than late.

### `DriveError<E>` - how a BLOCKING run ends badly

`run` and its variants return `Result<Output, DriveError<E>>`.

| variant | meaning |
|---|---|
| `Failed(E)` | a stage's own typed error, surfaced from its poll |
| `Stalled` | the graph answered `Pending` with no outstanding work left to run, so re-polling could only answer `Pending` again |

**`Stalled` is not a timeout**, and it means opposite things in the two
drives - which is the single most useful thing to know about this API.
Something registered a waker for an input nothing was ever going to land.
Under a BLOCKING drive that is a bug in the graph: nothing else was coming.
Under a FRAME drive the identical situation is normal - the frame keeps
its stand-in and a later user action lands the value. So the same state is
a defect offline and expected behaviour in an interactive host, and that
asymmetry is exactly why `run_watched` exists: it lets an offline run
report the unwakeable polls that a frame drive would silently lose.

### `ChainError<A, B>` - WHICH stage failed

A pipeline of two or more stages returns
`Result<_, DriveError<ChainError<..>>>`, nesting one `ChainError` per join.

| variant | meaning |
|---|---|
| `First(A)` | the first stage failed; the second never ran |
| `Second(B)` | the first produced a value and the second failed on it |

The types are the answer to "where did this break", so the error channel
stays typed all the way out rather than collapsing to a string. A
three-stage pipeline nests them - `ChainError<ChainError<A, B>, C>` - which
is verbose to name and precise to match on; a consumer usually matches the
outermost and only destructures further when it must.

### `PendingWork` and `NoPendingWork`

`run` takes something to pump while polls answer `Pending`: that is
`PendingWork`. `NoPendingWork` is the implementation for a graph of pure
stages, where nothing can be pumped because nothing is waiting on the
outside world. `run_pure` is `run` with it supplied.

## What the builder cannot yet express (findings, in priority order)

1. **Tracked state graphs.** `Ledger`/`Tracked`/`TrackedInput` composition -
   including the load-bearing order "wrap the memo in the tracking, not the
   tracking in the memo" - is exactly the kind of assembly the builder
   exists to own, and it is not in the builder yet. Sketch:
   `.tracked_input(label, value)` and a `stage` variant taking a ledger
   label, with the builder owning the ledger and the wrap order so the
   known-bad composition becomes unwritable. Until then the tracked layer
   stays private and the 45 tests over it stay on internals (the
   invalidation, cutoff, read-edge, fallback-revalidation and schedule
   suites in `src/track.rs` and `src/schedule.rs`).

   The exact expression a consumer cannot write today, and its error:

   ```text
   error[E0432]: unresolved imports `libpipeline::Ledger`, `libpipeline::Memo`,
                 `libpipeline::Tracked`
   ```

   **When this finding lands, the wrap order stops being testable, and that
   is the point.** A builder that owns the order makes
   `Memo::new(Tracked::new(..), store)` unspellable; a test asserting the
   order is then asserting about a composition nobody can construct, which
   is worse than no test because it implies the mistake is still reachable.
   `Memo`'s known-bad twin
   `a_cache_outside_the_tracking_is_a_cache_the_ledger_cannot_reach` is
   therefore scheduled for DELETION with this finding, not migration - it
   is the record of why the builder owns the order, and the builder will be
   that record.
2. **Error boundaries.** `Guarded` placement ("outside the tracking") and
   the substitution tally are caller assembly today. Sketch:
   `.guarded_stage(name, version, handler, make)` plus
   `Pipeline::substitutions()`.
3. **Non-linear graphs.** The builder builds chains. A diamond (one
   producer, two consumers, one joiner) exists in the engine via
   `Arc<S>: Stage` but has no builder spelling.
4. **Scheduling.** `Ledger::schedule` has no builder-level door; it rides
   on finding 1.
5. **Store lifecycle.** `.stage_in` lets a cache outlive a build, but there
   is no whole-pipeline store policy (a single backend serving every stage
   needs per-output-type stores; the seam is per-stage on purpose, but a
   factory hook may be wanted).
6. **A watched single poll.** `Pipeline::run_watched` reports a
   `WakeReport` over a whole DRIVE; `Pipeline::poll_frame` is unwatched.
   Nothing public answers "what did THIS poll leave behind", so `WakePath` -
   a public type - is named by no public signature, and the six tests that
   read one are `src/watch.rs`'s. Sketch:
   `Pipeline::poll_frame_watched(&input) ->
   (EffectPoll<..>, Option<WakePath>)`.
7. **The registration-site guarantee protects only what is registered.**
   The id check runs where a builder call constructs the stage. A crate
   that authors stages against the port (`libpipelinedata`) but is
   assembled nowhere carries its versions at its own construction sites,
   unchecked - the same placement of the version, without the panic behind
   it. That is a property of the port/adapter split, not a defect in it: a
   stage-authoring crate deliberately never links this engine, so the
   guarantee can only exist at an assembly site. The gap closes per
   consumer, by an assembler existing, not by anything this crate can add.
8. **Assembling a pipeline takes two manifest edges.** An assembler needs
   `libpipeline` for the builder and `libpipelinedata` for the vocabulary
   its closures must name (`Stage`, `StageId`, `MemoKey`, `ContentKey`,
   `EffectPoll`, `MemoStore`) - the README's every example imports both.
   This crate could re-export the port so one edge suffices, without
   weakening the split (stage authors would still depend on the port
   alone). Deliberately not done in the same wave that wrote the README;
   recorded here as the open decision it is.

## The ledger test, measured (a lesson in what a test holds)

`the_ledger_scope_changes_speed_and_not_answers` arrived in this crate
relocated from a consumer's suite, and finding 1 was recorded as the reason
it could not be written through the public API. It was re-examined by
asking, of each of its four assertions, what defect would make it fail. The
method was mutation: break one thing, run the suite, see who notices.

```rust,ignore
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

* **The test contains nothing about the ledger.** Deleting the tracking
  layer from it changes no assertion, so the `Ledger` in its name and the
  "scope" in its comment were never observed. Its comment on assertion 1 -
  "the lookup sits inside the node's scope" - names the wrap order, and the
  inverted, known-bad order passes all four assertions.
* **The SPEED half is the memo's, not the ledger's.** With no
  `TrackedInput` in the test, nothing can go stale, so `revalidating()` is
  false either way and the scope cannot change a lookup's outcome. Deleting
  the gate the assertion appeals to leaves the test green.
* **Assertions 2 and 3 cannot fail here.** `Tracked` returns its inner poll
  verbatim; the only route by which tracking changes an ANSWER is a stale
  value served from a store, which needs a tracked input to move.
* **Assertion 4 is close to tautological** and owned elsewhere:
  `polling_a_node_clears_its_staleness_and_a_pending_poll_does_not` is the
  test for it, over a node that was actually stale first.
* **What the ledger is FOR is tested elsewhere and well** - dependency
  edges and selective invalidation live in the read-edge, invalidation and
  cutoff suites. This test was never part of that.

**Outcome: a defect in the TEST, not in the builder** - and the
reproduction that remains after the empty half is removed DOES build
through the public door, as
`one_memo_serves_both_drivers_and_the_stage_runs_once` in
`tests/two_drivers_one_graph.rs`. It reuses that file's existing stand-ins,
so the cost was zero new machinery. It also covers a gap:
`both_drivers_give_the_same_answer_for_the_same_graph` builds one graph per
driver, so until then nothing measured ONE store serving both.

The test is therefore deleted rather than parked, and should not return
when finding 1 lands. Two independent reasons, either of which is
sufficient:

1. It asserts a property the builder is meant to make UNWRITABLE (the wrap
   order) - and does not even assert it successfully. A test that survives
   its own fix by testing the now-unconstructible case implies the mistake
   is still reachable.
2. Its consumer-level form should not come back in a consumer's suite
   either, per "Where a consumer works": a consumer does not assemble the
   stage graph, so holding the property over a REAL stage from the consumer
   was never a thing to restore. A stand-in inside this crate is the right
   shape, not a substitute for one.

The general lesson is the method: a test's name and comments claim what it
holds; only mutating the code under it shows what it observes. A test whose
every mutation is caught by some OTHER test is not redundant cover - it is
empty, and its name is misdirection.

## State of the implementation

DONE: `src/builder.rs` (builder, runner, id check at registration,
intrinsic memoization with owned/given/off stores); the visibility flip
that made the builder the only door, with every internals-reaching test
moved into the module that owns the internal it reaches for; the four
public-API test binaries in `tests/`; the README as the public API's front
door, its examples running as doctests.

NOT done: findings 1-6 (each one is a test in `src/` that wants to be a
test in `tests/` - with the correction that finding 1's tests want to move
outward as INVALIDATION tests, and one of them,
`a_cache_outside_the_tracking_..`, wants to be deleted instead, because the
builder will be the thing that holds what it holds); the `Ctx` closure form
above; the engine-level items under "Not built yet".
