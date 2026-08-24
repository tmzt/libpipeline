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

Seven rulings later the same day cut that shape down considerably, and this
revision carries them through rather than annotating around them. Tim, in the
order they were reached: "these aren't durable pipelines, it's not a concern",
then "the data can be durable, the runtime state isn't"; "the order can just
be tracked in the builder, we don't need a stageid apart from that";
"MemoStore is a trait we provide to the builder" - singular; "this is just a
simple pipeline with an input version and read-state tracking at the edges";
"Unchanged is also a successful result, meaning we didn't need to compute
anything or delay anything, the value is finished"; and "we can also use
standard result now, with the error side being Failed."

What follows from them: the pipeline's RUNTIME state is not durable (the DATA
is), the stage VERSION is therefore inert and goes, stage identity is
POSITION tracked by the builder, there is ONE store handed to the builder and
type-erased, the tracked layer reduces to read-state tracking at the EDGES,
and a run answers a standard `Result` whose error side is flat and carries the
position of the stage that failed.

Four sections carry the arguments - "Why the stage version goes", "One store,
at the builder", "One error type, flat and positioned", and "Read-state
tracking at the edges". Where a ruling retires an argument this document used
to make, it names the argument and says why it went, rather than deleting it
quietly.

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
marked proposed almost throughout, and three sections behind them - WHY THE
STAGE VERSION GOES, ONE STORE AT THE BUILDER, and ONE ERROR TYPE FLAT AND
POSITIONED - carry the arguments for the shape's three subtractions. WHAT
EXISTS TODAY and THE MODEL are the crate as it stands - the unmarked ground
truth, including the current four-door surface the proposal replaces.
INTERNALS covers the layers, how they compose, where the proposed pieces land
among them, and the reduction of the tracked layer to READ-STATE TRACKING AT
THE EDGES. The rest is the ledger: the findings, the recorded stage-shape
intent, the subcrate boundary, and the document ends with a verdict and a
migration plan.

## What a pipeline is

!!! PROPOSED

A pipeline is the self-contained state tracker for a sequence of steps
defined through a builder. Self-contained means the pipeline itself holds
what it last ran against and what it remembered along the way; none of that
state lives in the caller, and none of it lives in hand-composed wrappers
around the pipeline. A caller holds a pipeline, hands it the current state
of the world, and reads one of four answers.

* **Steps come from a builder.** Each step is registered once, and its
  identity is its POSITION in that registration order - tracked by the
  builder, never declared by the step. The pipeline owns everything about how
  the steps compose and what they remember between runs.
* **There is ONE way to run it.** Not a blocking door and a frame door - one
  method. Whether a caller blocks until the answer or returns and waits for
  a wake is what the caller DOES with the `Delayed` outcome, not an API it
  chose up front.
* **A run has four outcomes, split by kind.** Three of them mean the run
  happened, and they are the success side: `Computed(Output)` - work
  happened, here is the new value; `Unchanged` - no work was needed and none
  is outstanding, THE VALUE THE CALLER HOLDS IS FINISHED; `Delayed` - not
  ready yet, a wake is coming. The fourth means the run did not happen, and
  it is the error side: `Failed` - which stage, and its error.
* **Input is a `(version, readable)` pair, supplied per run.** The version
  says WHICH state this is; the readable IS that state. A version matching
  the previous run's answers `Unchanged` without reading the readable at
  all.
* **One store, chosen once.** Where the whole pipeline remembers is a single
  decision taken at the builder, not one taken per registration - and it has
  a default, so most consumers never take it at all.
* **The DATA is durable; the pipeline's runtime state is not.** The action
  store, the node-graph and a bundle persist. What the PIPELINE holds - its
  memo, the version it last ran against, whatever is in flight between a
  `Delayed` and its wake - does not, and is not meant to. On restart the data
  is there, the memo is empty, and everything recomputes once. That is the
  expected cost of a start rather than a gap to be closed, and several of the
  simplifications below are paid for by it.

`Computed` rather than `Ok` is deliberate, and it survives the move to a
standard `Result`: the success side carries a `Run` rather than a bare value,
because `Ok` says only "not an error" while `Computed` says WORK HAPPENED and
contrasts exactly with `Unchanged`. All three of `Computed`, `Unchanged` and
`Delayed` are successes, and each asks something different of the caller: use
the new value, keep the one you hold, wait to be woken. The distinction is the
entire point of a memoizing pipeline: a caller that cannot tell "here is a new
value" from "keep what you have" cannot avoid re-consuming the value, and the
work the pipeline saved reappears one layer up.

## Public API

!!! PROPOSED

This section is the whole contract. A consumer learns here what a pipeline
is, how to build one, how to run it, and what the four outcomes mean - and
meets nothing else. The machinery behind it is described under "Internals"
and is deliberately absent here.

**Four names are required reading**: `PipelineBuilder`, `Pipeline`, `Run` and
`Failed`. A fifth, `MemoStore`, is optional and has its own passage at the end
of this section; a consumer reaches for it only when the default is wrong. The
order is deliberate - everything up to that passage can be read, and a
pipeline built and run, without meeting a store at all.

### Building a pipeline

```rust,ignore
use libpipeline::{PipelineBuilder, Run};

let pipeline = PipelineBuilder::new()
    .stage("parse", |id| Parse::new(id))
    .stage("lower", |id| Lower::new(id))
    .build();
```

* `PipelineBuilder::new()` - the empty builder, remembering into a map of its
  own.
* `.stage(name, make)` - register one step. `make` receives the identity the
  builder MINTS for it, which is its position in this builder and nothing
  else; the `name` beside it is a DIAGNOSTIC label, not an identity (see "Why
  the stage version goes"). Steps chain: each consumes what the previous one
  produced. Every registered step remembers its answers; there is no
  un-remembering registration to forget.
* `.uncached()` - the control switch: the pipeline remembers nothing.
  Answers must not change, only speed; a pipeline whose answers change when
  remembering is disabled has a bug the remembering was hiding.
* `.build()` - the finished pipeline. Its input type is the first step's
  input; its output type is the last step's output; its error type is the one
  every step shares; its version type is fixed here, as part of the
  pipeline's own type.

A consumer implements steps against the stage contract in
`libpipelinedata` (`Stage`, `StageId`, and the key vocabulary), which
exists so a step can be DECLARED without linking the engine that runs it. A
consumer RECEIVES a `StageId` from the builder and never constructs one. A
consumer implements stages and assembles nothing.

### Running it

```rust,ignore
match pipeline.run(version, &document)? {
    Run::Computed(output) => held = Some(output),
    Run::Unchanged => { /* `held` is finished and current */ }
    Run::Delayed => { /* draw the stand-in; a wake is coming */ }
}
```

`run(version, &readable) -> Result<Run<Output>, Failed<Error>>` is the one
door. It polls once and returns immediately, whatever the answer; nothing
inside it waits, ever.

**A standard `Result`, with `Failed` on the error side.** The four outcomes
split by kind rather than sitting in one flat enum, and the split lands
exactly where the meaning already was: `Failed` means the run did not happen,
and the other three all mean it did, each demanding something different of the
caller. The practical gain is that `?` starts working - a caller that only
cares about failure propagates it and never matches it, which is the common
case in a lowering chain - and the match above is left with three arms that
are genuinely three decisions.

`Failed` is FLAT and carries the position of the stage that raised it, rather
than a tag nested once per join; see "One error type, flat and positioned".

There is only one number called a version here, and it is this one: the RUN
version, which says which STATE the input is. The per-step behaviour version
that used to sit beside it at each registration site goes (see "Why the stage
version goes"), so the two can no longer be confused for each other.

### The four outcomes, and what each answer means

| outcome | side | meaning |
|---|---|---|
| `Computed(Output)` | `Ok` | work happened; here is the new value |
| `Unchanged` | `Ok` | no work was needed and none is outstanding; the value the caller holds is finished |
| `Delayed` | `Ok` | not ready; a wake is coming |
| `Failed { at, error }` | `Err` | the run did not happen: which stage, and its error |

**`Computed` and `Unchanged` are both success**, and the pipeline's memory
is what tells them apart. The pipeline records the version each `Computed`
answered for; a run handed that same version again answers `Unchanged`
without reading the readable. So `Unchanged` always means: the value I
last handed you derives from exactly this state - keep it.

Tim's phrasing for it is the actionable one - THE VALUE IS FINISHED. It is
not a report that nothing happened; it is a statement that nothing needs to,
which is what lets a caller draw the value it holds and stop. "Nothing moved"
describes the pipeline; "your value is finished" describes the caller's
situation, and the caller is who the answer is for.

**Why `Unchanged` exists at all is a CALLER-side argument.** It is tempting to
say a pipeline that can hand the old value back cheaply does not need the
variant, and Tim put exactly that question: "it may not be needed if the cost
of providing the old result is low and the pipeline owns that, I'm thinking
about the full InMemoryBundle, do we need to hand it back if we didn't build
one?" The answer is that cheapness settles the PIPELINE's side and not the
caller's. A frame loop given `Unchanged` skips layout, render and diff
entirely. Given `Computed(same_value)` it must either redraw or compare what
it was handed against what it held - which is the pipeline's own knowledge,
reconstructed one layer up by whoever remembered to. `Computed` when nothing
was computed is also a small lie, and the name is the whole reason the variant
is not called `Ok`.

The question also uncovers a cost the design has not been paying attention
to: handing the old value back is not, in fact, cheap today. That measurement
is recorded under "One store, at the builder", where the fix for it lives.

**Only `Computed` records the version.** A `Delayed` run has not produced
the value for that version, so asking again with the same version polls
again rather than falsely answering `Unchanged`. A `Failed` run records
nothing either: a failure is this run's answer, not the pipeline's verdict,
and a later run with the same version retries rather than serving the
failure back as a settled fact. A consequence worth knowing: after a newer
version fails, re-running the OLD version still answers `Unchanged` - the
old value still stands, which is true.

**`Delayed` is a promise.** A run that answers it has arranged for the
pipeline's waker to be woken when the answer becomes possible. Where that
wake comes from is deliberately not part of the answer - Tim, 2026-08-24:
"the wakers are registered on the original input (or on a later stage,
internally) but the result isn't ready yet". Either way the caller's
obligation is identical, and that identity is the point of not saying: wait
to be woken, and do not re-poll in a spin. A step that cannot keep the
promise has made its value lost rather than late, and the pipeline treats
that as the defect it is (see "Delayed keeps its promise" under Internals).

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
        Ok(Run::Delayed) if executor.run_once() => continue,
        Ok(Run::Delayed) => break stalled(), // the caller's own condition
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

### Where answers live, if the default is wrong

This is the optional passage. A consumer whose answers can live in the map
the builder already owns can skip it entirely - which is the point of its
being here rather than back at `.stage`.

By default the builder remembers into a map it owns, and that default is why
nothing so far has mentioned a store. `.store(store)` overrides it for the
WHOLE pipeline - one store, one decision, taken once. Any `MemoStore`
implementation will do; the trait lives in `libpipelinedata` beside the stage
contract, and "One store, at the builder" gives the argument for its shape.

**Why a default rather than an explicit choice.** Where answers live is a
decision almost every consumer would answer the same way, and a required
parameter for a decision nobody varies is friction that teaches nothing: it
puts a name in front of every reader for the benefit of the few who will
change it. The seam earns its existence from that minority. Made a parameter,
it would stop being a seam and become part of what the crate is - and a seam
you must understand in order to use the crate at all is not a seam.

### What else is public

* **The stage contract** (`Stage`, `StageId`, `MemoKey`, `ContentKey`,
  `EffectPoll`, `MemoStore`, `MemoMap`, `NoMemo`) - lives in
  `libpipelinedata`, not here; a stage AUTHOR needs it, and it is the
  implement-side contract, not composition machinery. `StageId` is among the
  types a consumer HOLDS rather than constructs: the builder mints one per
  registration and hands it to `make`.
* **`Run`** - the success side above: `Computed`, `Unchanged`, `Delayed`.
* **`Failed`** - the error side: the position of the stage that raised it,
  and its error. Flat, one type for the whole pipeline, so a chain of five
  steps has the same error type as a chain of two.
* **`PipelineBuilder` and `Pipeline`** - the other two of the four required
  names, each with its own section above.

Nothing else. In particular, the executor seam (`PendingWork`), the
blocking drive's error type (`DriveError`), the watched drive's report
(`WakeReport`, `WakePath`), and the nested per-join failure tag
(`ChainError`) - all public today - leave the surface with the doors and the
nesting that named them.

## Why the stage version goes

!!! PROPOSED

Tim, 2026-08-24, in two steps. First on durability: "these aren't durable
pipelines, it's not a concern", then "the data can be durable, the runtime
state isn't." Then on identity: "the order can just be tracked in the builder,
we don't need a stageid apart from that."

### What the version was for

Today `StageId` is `{ name: &'static str, version: u32 }`
(`libpipelinedata/src/key.rs`), and its doc gives the version one job: an
input that changes a stage's OUTPUT without itself being an input has to move
the key, or the memo returns the old answer forever. The named instance is
element expansion reading a static registry table, which "versions as part of
the stage id".

Two things about that job:

* **The version is a compile-time constant**, so it can only distinguish one
  compilation of the code from another. An ambient input that moves WITHIN a
  process was never covered by it - no constant can be bumped at runtime -
  and that case belongs to read observation (`src/track.rs`), which sees the
  read rather than being told about it.
* **So the version's entire domain is a memo that outlives a rebuild.** The
  static registry table is the same window in other words: a `static` changes
  only by recompiling, so the answer it poisons is only reachable if the
  cache survived the recompilation that changed it.

### The window never opens

Nothing in this stack has a cache that outlives a rebuild, and after the
durability ruling nothing will. Checked rather than assumed:

* `MemoMap` is `Mutex<HashMap<MemoKey, V>>`, in memory, cleared by `clear` or
  by being dropped (`libpipelinedata/src/store.rs`).
* The `MemoStore` seam has two methods, `lookup` and `record`, over owned
  values. No serialization bound, no path, no handle, nothing that could
  reach a disk (`libpipelinedata/src/store.rs`).
* `libpipelinedata` does carry an optional `serde` dependency, and it is not
  a persistence door: it is a `Serializer` that writes into the content
  hasher, whose `Ok` type is `()` so a serialization through it produces no
  value at all (`libpipelinedata/src/serde_hash.rs`).

A memo that cannot leave the process cannot be read by a later build of the
binary. The version guards a transition that never happens.

### What this invalidates, said plainly

This document has argued the other way at length, and the argument should be
retired out loud rather than quietly deleted.

An earlier revision (in git history) devoted a section, "Why the builder is
the only door", to three decisions, of which the third was **the version is
declared at the registration call site**. Its evidence was a measurement: in
the flat-export era one consumer declared its `StageId` 761 lines from the key
construction it governed. The reasoning built on it was that much of this code
is written by LLM agents, which do not scroll to a module-level `const` to ask
whether a number should move, so the invariant has to be in the way rather
than remembered.

Every part of that is still true except the part that mattered. The distance
was real and was measured. Putting the number at the call site did close it.
What the discipline protected was a window that never opens, so the closing
bought nothing. THE MEASUREMENT WAS SOUND; WHAT IT MEASURED WAS THE COST OF A
DISCIPLINE WITH NO PAYOFF. The other two decisions of that section - the
builder as the only door, and memoization intrinsic to registration - are
untouched and are load-bearing throughout this document.

There is a second, smaller lesson in the test that covers the version.
`a_version_bump_at_the_call_site_is_a_cold_cache`
(`tests/builder_is_the_only_door.rs`) shares one store across two builds
inside one process, bumps the number, and shows the old rows going
unreachable. It demonstrates the MECHANISM exactly. It cannot demonstrate the
SCENARIO, because the scenario spans two compilations of the binary and no
test spans those. A mechanism that is easy to demonstrate and a scenario that
is unreachable is what this shape of defect looks like from the inside.

### Position, and what a name is for

Identity becomes the builder's index. The builder already sees every
registration in order; it mints the identity from that order and hands it to
`make`, and a step never declares one. `StageId` collapses to that index. The
change is small because the parts being dropped are barely read: the only
readers of `name()` and `version()` anywhere in the stack are two adjacent
lines in `libpipelinedata/src/hash.rs` folding both into the key.

**Names should survive, as diagnostics.** A failure that says "stage 3" is
worse than one that says "stage 3, lower", and the position that makes the
first possible is exactly what makes the second cheap. But a diagnostic name
is NOT an identity, and the difference has to be enforced rather than
intended:

* it must not enter a memo key, or it is an identity again;
* nothing may be looked up by it, keyed by it, or compared on it;
* two steps may share one with no consequence whatsoever.

That last property is the test: a label two steps can share without
consequence is a label nothing depends on. The discipline is not invented
here - an internal layer already holds the same line for its own labels,
in the same words (see "Internals"), so this is a precedent to copy.

### What follows

* **The build-time identity check dissolves.** `checked` in `src/builder.rs`
  panics when a stage answers a different id than it was registered under.
  With the builder as the only source of an identity there is no second id
  for an honest author to answer with, so the defect stops being reachable
  rather than being caught. That is the better outcome, and it is why the
  check is not carried forward.
* **`PipelineId` loses its shape hash.** Recorded intent has it as a shape
  hash over the `StageId`s in order, plus a serial. Over positions the hash
  is `0,1,2` for every three-stage pipeline in the program, which
  distinguishes nothing. The serial carries instance identity by itself, and
  a rebuild mints a new one so keyed state dies with the instance - which is
  what the shape hash was belt-and-braces for. Keep the serial, drop the hash.
* **Finding 7 closes by dissolution** rather than by being fixed; see the
  findings list.

## One store, at the builder

!!! PROPOSED

Tim, 2026-08-24: "MemoStore is a trait we provide to the builder" - singular.
Today `stage_in` takes one per registration (`src/builder.rs`), and
`BuilderStore` in the same file is the three-way answer to "where does THIS
stage remember": a map of its own, the caller's, or off.

That is the assembly-at-each-call-site pattern the builder exists to remove,
one level down. Registration already owns whether a stage is memoized;
asking each registration WHERE is the same question the builder was built to
stop asking repeatedly.

### The typed factory does not work

The obvious way to keep types and still have one thing at the builder is a
factory: hand the builder something that mints a `MemoStore<V>` per
registration. It cannot be done. Minting a store for arbitrary `V` needs a
generic method, a generic method is not object-safe, so the factory cannot be
`dyn` - which means it threads as a type parameter through `PipelineBuilder`,
through every intermediate builder state, and into the pipeline's own type.
The one type a consumer names would then carry the store implementation in it,
which is the opposite of a seam.

### So erasure, and `Any` is the right one

Erasure is what makes one store possible, and the choice of erasure is settled
by the durability ruling. `Any` needs only `V: 'static` and costs one
allocation and one downcast per lookup. Serde-erasure - values that can be
written out and read back - is the erasure a DURABLE store would need, and
durability is now explicitly not wanted; it would also put a serialization
bound on every stage output, which is a far heavier tax than `'static` for a
capability nothing asks for.

**The seam needs no change to carry it.** `MemoStore<V>` is generic over the
value type, so erasure is a choice of `V`, not a second trait: the builder's
store is a `MemoStore` whose `V` is an erased handle. Two things in
`libpipelinedata/src/store.rs` make this fit without editing that file - the
blanket `impl<V, S: MemoStore<V> + ?Sized> MemoStore<V> for Arc<S>`, which
exists precisely because "the natural way to put one store in front of several
stages is to share it", and `MemoMap`'s own impl, bounded `V: Clone`.

That `Clone` bound decides the shape of the handle. `Arc<dyn Any + Send + Sync>`
satisfies it; a `Box<dyn Any>` does not, and `lookup` returns owned values by
design. So the row is an `Arc` and the erasure is an unsizing coercion of one:
record an `Arc<Output>`, look up an `Arc<dyn Any + Send + Sync>`, downcast back
to `Arc<Output>`. `Send + Sync` follows from the store being shared across
threads under the blocking drive, which is the same reason `MemoMap` holds a
`Mutex` rather than a `RefCell` (its own doc).

### And this is where the deep clone goes

The cost flagged under "The four outcomes" is fixed by the same change. Today
a memo hit is a deep copy: registration requires `S::Output: Clone`
(`src/builder.rs`), `MemoStore::lookup` returns an owned value on purpose
(`libpipelinedata/src/store.rs`: a cache handing out borrows "ties the
caller's frame to the store's lock"), and the memo layer clones on both sides,
`.cloned()` on the hit and `value.clone()` on the record
(`libpipelinedata/src/store.rs` and `src/memo.rs`). For an output the size of
a whole bundle that is the opposite of the saving a memo exists for.

With the row already an `Arc`, an output that is itself `Arc`-shaped erases
into it with no second indirection, and a hit becomes a downcast plus a
refcount bump. The store erasure and the cheap-output fix are one change, not
two.

### One store means a store cannot outlive the build

`stage_in`'s stated use is "a cache that outlives one build of the pipeline"
(`src/builder.rs`). Position-as-identity forces that question to be answered
rather than left open, because two pipelines sharing a store both have a stage
0.

**The decision: the store belongs to the pipeline and does not outlive it.**
`.store(store)` hands it over at the builder, the pipeline holds it, and no
second pipeline shares it.

The alternative is to put the pipeline's serial in every key so two instances
cannot collide. It is self-defeating: instance-scoped keys mean two pipelines
sharing a store share no rows, and a rebuild - which mints a new serial -
inherits nothing. The shared store would save one allocation and nothing else,
which is not what "outlives a build" was ever asking for. Under the durability
ruling the case for it is weaker still: recomputing once after a restart is
the expected cost, and this is the same cost one build earlier.

That decision is also what keeps the downcast honest. A row's key carries the
stage's identity; identities are positions; positions are minted one per
registration by a single builder, so within one pipeline no two stages can
share one. A failed downcast would therefore mean an identity collision, which
cannot be constructed - so the lookup `expect()`s the invariant and names it,
rather than degrading into a silent miss (a cache quietly disabled) or a typed
error a consumer could do nothing about. Let a store be shared between
pipelines and that `expect` becomes reachable, which is the sharpest form of
the argument above.

### What it removes

Two `stage_in` methods and their `St` type parameter across four registration
signatures, `BuilderStore::Given`, and the per-registration store plumbing
(`src/builder.rs`). `.uncached()` stays as the control switch and becomes what
it always meant: one store that remembers nothing, chosen once, rather than a
flag consulted at every registration.

## One error type, flat and positioned

!!! PROPOSED

Tim, 2026-08-24: "we can also use standard result now, with the error side
being Failed."

`Failed` carries the position of the stage that raised the error, and the
error, and that is all. Every step of one pipeline shares an error type.

### What nesting costs

`ChainError<A, B>` tags a failure with the half it came from and nests once
per join (`src/chain.rs`). Two stages read `ChainError::First(..)`. Five
stages read `ChainError<ChainError<ChainError<ChainError<A, B>, C>, D>, E>`,
which is a type nobody writes in a signature and nobody matches on twice. The
nesting is also how a caller answers "which stage failed" today: by counting
`First`/`Second` layers, which is position arithmetic performed by hand
against a type shape.

Position is already the stage's identity, so `at: usize` answers that question
directly, in one field, at any length of chain. The two rulings meet here: the
flat error is only spellable because identity is a position.

### The cost, recorded

**Stages of one pipeline can no longer carry unrelated error types.** A
consumer with genuinely disjoint failure modes writes one enum unifying them.
That is what such a consumer would write anyway to match on the result, and
writing it once at the pipeline is better than having it assembled implicitly
by the chain's type - but it IS a constraint the current design does not
impose, and it should be named as one rather than presented as free.

`ChainError` leaves the public surface with `DriveError`. Internally the
tagging in `src/chain.rs` goes with it: with one error type on both halves, a
chain propagates rather than retypes, and the position is stamped where the
builder already knows it - at registration, which is the only place the index
is available.


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
poll time.

What carries forward, and what does not. The builder as the only door and
memoization as intrinsic to registration carry forward untouched; they are the
two decisions everything else here rests on. The rest of this paragraph is
what the 2026-08-24 rulings change: the `version` argument goes from both
registration methods ("Why the stage version goes"), `stage_in` goes in favour
of one store chosen once at the builder ("One store, at the builder"), and
`checked` goes because the defect it catches stops being reachable when the
builder is the only source of an identity. `build` acquires the run-version
type parameter, and the chained error type stops growing per join ("One error
type, flat and positioned").

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
  the second failed on it), nesting once per join. Earlier revisions of this
  document had it surviving the proposal unchanged; it does not. A flat
  `Failed` carrying the failing stage's position replaces it, and the reasons
  are under "One error type, flat and positioned".

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
(`MemoStore`), not the engine's decision. Under the proposal the seam is
unchanged and the QUESTION moves: it is asked once, at the builder, instead
of once per registration, and the stage half of the key becomes a position
rather than a name and a version.

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

The RULE - observed, never declared - is not in question and survives
everything below. What the proposal changes is the granularity at which it is
recorded: see "Read-state tracking at the edges" under Internals.

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
  builder nests these under a fixed internal `StageId`. Its error channel
  retypes each half into `ChainError`; under the proposal both halves share
  one error type and it propagates instead ("One error type, flat and
  positioned").
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
  the three answers to "where does this stage remember". Under the proposal
  the three answers stay and the question is asked ONCE, at the builder,
  about the whole pipeline ("One store, at the builder"); each registration
  then holds a shared handle to that one store, which
  `libpipelinedata/src/store.rs`'s `MemoStore for Arc<S>` impl exists to
  allow.

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

### Read-state tracking at the edges

!!! PROPOSED

Tim, 2026-08-24: "this is just a simple pipeline with an input version and
read-state tracking at the edges."

That is a large reduction of the layer described above, and it is stated here
as intent: the tracked layer becomes a record of WHAT A RUN READ at the input
boundary and WHAT CAME OUT at the output boundary, rather than a who-read-whom
graph maintained inside.

**And the strongest argument for it is that the wake topology already IS
the dependency structure.** Tim, 2026-08-24: *"the stages are connected by
wakers."* That is not only intent - the code composes this way today.
`Chain::poll_stage` (`src/chain.rs`) hands BOTH halves the same `Context`,
so one waker serves the whole chain, and a `Pending` anywhere makes the
chain `Pending`. A wake therefore re-polls from the top.

Re-polling everything sounds wasteful and is not, because MEMOIZATION IS
WHAT MAKES THE SHARED WAKER A WAKE GRAPH: on the re-poll every stage
before the pending one hits its memo and returns at once, so waking "all
of them" costs only the stage that was actually waiting. Which is the
point - **a dependency graph is not needed to work out who to wake,
because everyone is woken and the memo decides who had work to do.**

So the ledger's `readers` index, its transitive marking and its per-edge
retraction are maintaining, at cost, a structure the wake path expresses
for free. That is an argument from the code's present shape rather than
from the design's preference, and it stands beside the fan-out argument
above: a chain records no node-to-node edge, and it does not need one.

**What it displaces.** `src/track.rs` is 2,831 lines. The parts that answer
questions only a graph can ask are the reverse index (`Inner::readers`, a set
of readers per node), the transitive marking that walks it (the `VecDeque`
walks in `Ledger::changed` and `Ledger::unchanged`), the per-node read sets
those two are maintained in step with, and the per-edge retractable reasons
(`Reason::Read(node)` against `Reason::Owed`) that make one dependency's
non-movement retract one consumer's staleness. `Schedule`/`Cycle`
(`src/schedule.rs`) goes with them.

**Does the code bear the argument out?** The argument is that an internal
dependency graph earns itself when stages FAN OUT and share dependencies, so
an invalidation can reach some consumers and not others - and that a linear
chain, where each stage has exactly one predecessor, cannot ask that question.
Checked in three places, and it holds:

* `src/schedule.rs`'s own doc states its headline saving on "the diamond
  graph (one input, two readers, one joiner)", where the set worth polling is
  one node "and the shared consumer runs once instead of once per stale path".
  The module names a fan-out shape as the case it is for. On a chain the same
  computation returns the head of the chain, which is where a pull starts
  anyway.
* Node-to-node edges only arise from NESTING. `Tracked::poll_stage` calls
  `observe_read(self.node)` before opening its own scope (`src/track.rs`), so
  an edge is recorded only when another node's scope is already open - a stage
  polling another stage inside its own poll. `Chain::poll_stage` polls its two
  halves at the same level and hands the value along (`src/chain.rs`), so a
  chain records NO node-to-node edge at all. The transitive walk has nothing
  to walk; it degenerates to one hop, from the input to the stages that read
  it. `staleness_is_transitive` (`src/track.rs`) has to build its graph out of
  stages that poll stages, because a chain will not produce one.
* Early cutoff at a node (`Backdated`, `src/track.rs`) buys, on a chain, what
  content-keyed memoization already buys: a recompute that reaches the same
  value leaves the next stage's input unmoved, and the lookup precedes the
  work, so the chain stops there anyway (`src/memo.rs`).

**What survives the reduction, and why.** Not everything in that layer is
graph machinery:

* **The read observation itself** - `TrackedInput` and the run scope that
  gives "while a stage runs" a beginning and an end (`src/track.rs`). This IS
  edge tracking; recording reads at the boundary is the same mechanism with
  one scope instead of one per node.
* **The wake** - the subscriber list a change notifies, which is what reaches
  the frame drive's stale flag and therefore `take_stale`.
* **The memo-outranking channel** - `revalidating` (`src/track.rs`), read by
  `src/memo.rs` so that a store cannot answer for a run whose ambient input
  moved. A key built from a stage's ARGUMENTS cannot see an ambient read, and
  that is as true of an edge-scoped record as of a per-node one. What changes
  is scope: per run rather than per node.
* **Cutoff at the output edge** - the content-address comparison `Backdated`
  performs, applied at the root. That is exactly the root-level backdating the
  version gate below already anticipates as a second source of `Unchanged`.

**What the reduction costs, named.** Per-node staleness is finer than per-run
staleness in one case: when a stage recomputes to the same value, a per-node
ledger spares the stages after it, while a run-scoped `revalidating` does not,
because every stage in the run skips its store. The cheap consolation is that
the output-edge cutoff still spares the CALLER - the root answers `Unchanged`
even when intermediate stages re-ran. Whether the intra-run difference is
worth a graph is a measurement nobody has taken, and taking it is cheaper than
keeping the machinery against the possibility.

Nothing is deleted on the strength of this section. It records where the
design is going so that finding 1, when it is taken up, is taken up in this
shape rather than by wiring today's ledger to the builder.

### The version gate and the one door

!!! PROPOSED

The one door is the frame drive plus a version gate plus an outcome
mapping - no new engine semantics anywhere in it.

`Pipeline` gains a type parameter and one field: `Pipeline<V, S>` holding
the graph, the existing `FrameDriver`, and `last: Mutex<Option<V>>` (safe
interior mutability, as everywhere in this crate; `run` keeps `&self`
because a poll holds `&self` all the way down). `run(version, input)`:

1. If `last` holds exactly `version` AND NO WAKE IS PENDING: return
   `Ok(Run::Unchanged)`. The readable is not dereferenced, no memo key is
   computed, no stage is polled.
2. Otherwise poll once through the frame driver (today's
   `FrameDriver::poll_frame`, `src/driver.rs`) and map the answer:
   `Ready(v)` becomes `Ok(Run::Computed(v))` and records the version;
   `Pending` becomes `Ok(Run::Delayed)`; `Failed(e)` becomes `Err(e)`.
   Nothing else records the version.

**The wake half of that condition is not optional, and omitting it is a
silent defect.** Two different things mean "something happened", and only
one of them moves the version:

* the INPUT VERSION moved - the source changed;
* a WAKE arrived - a value some stage was waiting on has landed.

A landed effect does not move the input version. The source did not
change; an awaited value simply arrived. So a gate that checked the
version alone would take a pipeline sitting on `Delayed`, receive the
wake, re-poll, short-circuit on the unchanged version, and answer
`Unchanged` - forever. The delayed value is never delivered, and nothing
reports it: the caller sees a legitimate-looking `Unchanged` and holds a
value that is permanently one step stale. That is precisely the failure
this crate exists to prevent, arriving through the gate meant to prevent
it.

The mechanism already exists. `FrameDriver::take_stale` (`src/driver.rs`)
answers "has a wake arrived since this was last asked" and clears on read;
the gate consumes it rather than leaving it a separate question the caller
must remember to ask. Today's frame-drive callers ask both by hand - the
README's own loop reads `if pipeline.take_stale() || version != drawn_for`
- so the gate is absorbing a condition that is already load-bearing in
practice, not inventing one.

Ordering matters: `take_stale` clears when read, so the gate must consume
it on every run, not only when the version matches. A run that polls for a
version change and leaves an unread wake behind would answer `Unchanged`
on the NEXT run despite the wake, which is the same defect one step
displaced.

The error arm is a re-wrap and not a construction: with the position stamped
at registration, the graph's own error type IS the flat `Failed`, so the door
moves it from `EffectPoll`'s failure arm to `Result`'s and adds nothing.

The blocking drive (`run_to_completion`, `src/driver.rs`) stops being a
public door and becomes the caller's loop; it and its watched and counted
forms remain internal machinery with their own tests, and they remain the
reference semantics for what such a loop does. `PendingWork`,
`NoPendingWork`, `DriveError`, `WakeReport`, `WakePath` and `ChainError` all
leave the public surface - the first five with the doors that named them, the
last with the nesting it existed to express.

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
   deleted rather than migrated. RESHAPED, though, by the edges ruling: the
   spelling the builder grows is read observation at the input boundary and
   a cutoff at the output boundary, not a wiring of today's graph
   ("Read-state tracking at the edges").
2. **Error boundaries.** `Guarded` placement and the substitution tally
   are caller assembly today (`src/boundary.rs`). Status: OPEN; under the
   one door the tally would surface as an accessor on `Pipeline` rather
   than a counted drive variant.
3. **Non-linear graphs.** The builder builds chains; a diamond exists in
   the engine via `Arc<S>: Stage` (`libpipelinedata/src/stage.rs`) but has
   no builder spelling. Status: OPEN, orthogonal - with one constraint
   added by position-as-identity: a non-linear builder must still hand out
   one identity per registration, and a node shared between two consumers
   must keep the single identity it was registered with, which is what the
   `Arc<S>` impl's forwarding of `id` already does.
4. **Scheduling.** `Ledger::schedule` (`src/schedule.rs`) has no
   builder-level door; rides on finding 1. Status: OPEN, and likely to
   close by dissolution rather than by a door - a chain's schedule is the
   chain, and the module's own headline case is a diamond
   ("Read-state tracking at the edges").
5. **Store lifecycle.** `.stage_in` lets a cache outlive a build; there is
   no whole-pipeline store policy. Status: SETTLED, and no longer
   orthogonal - it is part of the design. One store at the builder IS the
   whole-pipeline store policy, `stage_in` per registration is the
   assembly-at-each-call-site pattern the builder exists to remove, and the
   store does not outlive the build. "One store, at the builder" gives the
   argument, including why a store shared across builds cannot be made to
   pay under position-as-identity.
6. **A watched single poll.** Nothing public answers "what did THIS poll
   leave behind". Status: SUBSUMED by "Delayed keeps its promise" - the
   one door checks the wake path itself, and the finding closes by
   deletion rather than by a new door.
7. **The registration-site guarantee protects only what is registered.**
   A stage-authoring crate that never links the engine carries its
   versions unchecked; the gap closes per consumer, by an assembler
   existing. Status: CLOSES BY DISSOLUTION. With no version and no
   self-declared identity there is nothing for an unlinked authoring crate
   to carry unchecked; the builder mints the identity at registration or it
   does not exist ("Why the stage version goes").
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
  `(PipelineId, position)` - never in a field, and the `Ctx` carries ACCESS
  TO A STORE, not a world. In-flight state is runtime state, so the
  durability ruling applies to it in full: it does not survive a restart,
  and the same `Any` erasure serves it. Whether it is the store the builder
  already holds or a second seam beside it is not settled here; both are
  "at the builder" in the sense the ruling meant.
* **`PipelineId` is the serial the builder mints, and nothing else.**
  Recorded intent had it as a shape hash over the `StageId`s in order plus
  that serial. The hash dies with the version: over positions it is
  `0,1,2` for every three-stage pipeline in the program and distinguishes
  none of them from each other. The serial carries instance identity by
  itself, and a rebuild mints a new one, so keyed state dies with the
  instance - which is what the shape hash was belt-and-braces for.

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
`PendingWork`, `NoPendingWork`, `WakePath`, `WakeReport`. All six go later:
five with the door collapse, and `ChainError` with the nesting it expressed
("One error type, flat and positioned"), leaving `Run` and `Failed` as the
facade's own vocabulary. Nothing else: no glob, no module re-export, each
name a visible decision in `src/lib.rs`.

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
  plus a three-arm match from `EffectPoll` onto `Result<Run<_>, _>`, under
  the gate's early return (new: one enum and one struct). The blocking drive
  it displaces becomes a documented caller loop whose reference semantics -
  `run_to_completion` and its watched and counted forms - stay as internals
  with their tests. No engine semantics change.
* **The 2026-08-24 rulings shorten the distance rather than lengthening
  it.** Every one of them is a subtraction. The stage version deletes an
  argument from four registration signatures and two fields from a key type
  (`libpipelinedata/src/key.rs`) whose only readers are two lines
  (`libpipelinedata/src/hash.rs`); `PipelineId` loses its hash before the
  hash is ever written; one store deletes two `stage_in` methods, the `St`
  type parameter they carried, and half the registration surface; and the
  shared error type stops the builder's chained error type growing per join.
  The port needs no edit to carry the erased store: erasure is a choice of
  `V`, and
  `MemoStore for Arc<S>` is already there for exactly this
  (`libpipelinedata/src/store.rs`).
* **The four doors are facade, not engine.** The doors and their
  vocabulary re-exports total well under a hundred lines of
  `src/builder.rs` and `src/lib.rs`. Deleting doors is not a
  rewrite-scale event.
* **The 71 internals tests survive by motion, not reconstruction.** Their
  own module docs already promise an outward migration "unchanged but for
  the imports"; the subcrate split is that migration. A rewrite would
  forfeit 3,370 lines of tracked-layer implementation and its 45 tests
  (2,831 in `src/track.rs`, 539 in `src/schedule.rs`) - machinery the new definition does
  not even touch - to arrive back at the same `Stage` contract.
* **What genuinely must be rewritten is bounded and identified**: the 30
  public tests and the README's 6 doctests, which speak the four-door
  vocabulary; the two-drivers file translates property-for-property into
  one-door-two-patterns form (its central claims - same answers, memo
  shared, wake obligations - are door-independent). Two of the 30 do not
  translate and are deleted, both in `tests/builder_is_the_only_door.rs`:
  `a_version_bump_at_the_call_site_is_a_cold_cache` tests a number that will
  not exist, and `a_stage_that_answers_a_different_id_than_registered_panics`
  tests a check whose defect stops being reachable.
* **The run-version parameter threads as a type parameter, not a rewrite.**
  `Pipeline<S>` becomes `Pipeline<V, S>`; the builder's chaining types are
  untouched until `build`.
* **One ruling does reach below the facade, and it is still small.** The
  flat error changes `src/chain.rs`: a two-variant enum and the two
  `map_err` arms that produce it come out of a 115-line module, and a
  position stamp goes in at registration, where the builder already knows
  the index. That is the deepest any of this goes.

What would have tipped it the other way, and did not: if the outcome type
had needed the engine to distinguish `Computed` from `Unchanged` per
stage, the memo/track layers would have needed a new result channel
throughout - but the gate is at the root, and the engine below it already
answers everything the mapping needs.

**The verdict is re-checked against the rulings and holds, more comfortably
than before.** The reduction of the tracked layer is the one item that could
look like a rewrite, and it is not one in this plan: nothing in `src/track.rs`
or `src/schedule.rs` is deleted here. Those two modules have no caller today
beyond their own tests, so the reduction changes what the builder will
eventually SPELL, not what has to be unwound first. A rewrite would still
forfeit them for nothing; a migration leaves them in place, unwired, while
the edge-shaped spelling is built.

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

**Step 2 - identity becomes position, and the version goes.**
In `libpipelinedata`: `StageId` becomes the builder's index
(`libpipelinedata/src/key.rs`), with the two folds in
`libpipelinedata/src/hash.rs` following it; `StageId::new(name, version)`
leaves the surface. In the facade: drop the `version` argument from
all four registration methods and delete `checked` (`src/builder.rs`), which
has nothing left to disagree with. Keep the `name` argument as a DIAGNOSTIC
label, held for messages and `Debug`, entering no key and compared by nothing
- the discipline `Ledger::node`'s label already states (`src/track.rs`).
Delete `a_version_bump_at_the_call_site_is_a_cold_cache` and
`a_stage_that_answers_a_different_id_than_registered_panics`
(`tests/builder_is_the_only_door.rs`): the first tests a number that no
longer exists, the second a check whose defect is no longer reachable. Update
the README's version passage (its "declares the stage's version right there"
paragraph and the store passage that leans on it) and its examples. Sweep the
surviving test docs for paragraphs that argue the discipline - among them
`tests/two_drivers_one_graph.rs`'s "the versions are here, at the registration
call sites, which is the whole of why the builder takes them there", which
becomes false the moment this step lands.
*Gate*: facade 28 tests + 6 doctests; internals 71;
`grep -rn "StageId::new\|\.version()"` finds nothing outside the key type
itself; no test asserts on a stage NAME as though it were an identity.

**Step 3 - one store at the builder, erased.**
Add `.store(store)` to `PipelineBuilder` and remove both `stage_in` methods,
taking the `St` type parameter with them and leaving two registration
signatures where there were four (`src/builder.rs`). The builder holds one
store; each registration takes a shared handle to it, through the existing
`MemoStore for Arc<S>` in `libpipelinedata/src/store.rs`. Rows are
`Arc<dyn Any + Send + Sync>`; a lookup downcasts and `expect()`s, naming the
identity-collision invariant.
`BuilderStore` keeps its three answers and is consulted once
(`.uncached()` becomes the store that remembers nothing). Port
`tests/two_drivers_one_graph.rs`'s four `stage_in` call sites to one
`.store(MapStore)` at the builder - the seam is still exercised by an
implementation this crate did not write, which is the property that file
names, and its doc paragraph about reaching the graph "through
`PipelineBuilder::stage_in`" is rewritten with the call sites rather than
after them. Add one facade test: two stages of DIFFERENT output types
sharing one store, each getting its own answer back.
*Gate*: facade 29 tests + 6 doctests; internals 71; `grep -rn "stage_in"`
finds nothing; the new test fails if the downcast is made to swallow a miss.

**Step 4 - one error type, flat and positioned.**
Add `Failed<E> { at: usize, error: E }` (`src/builder.rs`, exported from
`src/lib.rs`). Registration stamps the position; `Chain` propagates instead of
retyping, and `ChainError` and its two `map_err` arms come out of
`src/chain.rs` and out of `src/lib.rs`'s exports. Re-spell
`a_failure_names_the_stage_that_raised_it`
(`tests/builder_is_the_only_door.rs`) and
`a_failure_bubbles_out_tagged_with_the_half_it_came_from`
(`tests/two_drivers_one_graph.rs`) as assertions on `at`.
*Gate*: facade 29 tests + 6 doctests; internals 71; `grep -rn "ChainError"`
finds nothing in the facade; a three-stage pipeline's error type is spellable
in one line in a test signature, which is the property the change is for.

**Step 5 - the outcome and the one door (the flip).**
In `src/builder.rs`: add `Run<Output>` (exported from `src/lib.rs`); give
`Pipeline` the `V: Copy + Eq` parameter and the `last: Mutex<Option<V>>`
field; implement `run(&self, version, &input) -> Result<Run<Output>,
Failed<Error>>` as gate + `poll_frame` + mapping, recording the version only
on `Ready`. **The gate consumes `take_stale` on EVERY run** and answers
`Unchanged` only when the version matches AND no wake was pending - see
"The version gate and the one door" for why the version alone is a silent
defect. This step owes a test for it: a pipeline left on `Delayed`, woken
out of band with the version unchanged, must answer `Computed` and not
`Unchanged`. It fails the moment the wake half is dropped, which is the
only way to know the half is doing anything. Delete `run`, `run_pure`,
`run_watched`, `poll_frame` (the door, not the internals they call); keep
`take_stale` and `waker`. Drop the
`DriveError`/`PendingWork`/`NoPendingWork`/`WakePath`/`WakeReport`
re-exports from `src/lib.rs`. Port the public tests:
`tests/builder_is_the_only_door.rs` re-spells its tests through the one
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
*Gate*: facade 26 tests + 6 doctests; internals 74;
`grep -rn "run_pure\|run_watched\|poll_frame\|PendingWork\|DriveError"`
finds nothing in facade `src/` public items or `tests/`; every test name
cited from internals docs exists (`grep` each cited name); ASCII check on
everything rendered or exported.

**Step 6 - Delayed keeps its promise.**
In the facade's `run`, on the `Pending` path under
`#[cfg(debug_assertions)]`, poll through
`libpipeline_internals::poll_watched` and panic on `WakePath::Missing`
with the lost-not-late diagnosis. Add a facade test
(`#[cfg(debug_assertions)]`, `#[should_panic]`) driving a stage that
forgets its waker.
*Gate*: facade 27 tests + 6 doctests, green in debug and `--release`.

**Step 7 - the document catches up.**
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

Also deliberately not in this plan: any deletion in `src/track.rs` or
`src/schedule.rs`. "Read-state tracking at the edges" says where that layer
is going, and the way it gets there is by finding 1 being taken up in the
edge shape - reads observed at the input boundary, a content-address cutoff
at the output boundary - after which whatever the builder still does not
reach can be removed against a spelling that exists. Removing it first would
be deleting on the strength of a design statement instead of a caller, which
is the order this crate has already learned not to use.
