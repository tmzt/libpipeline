# libpipeline: design

`libpipeline` is an incremental computation engine for pipelines of pure
stages: a source value goes in, derived outputs come out, and everything
between is memoized and re-derivable on demand. The engine is generic over
every payload type - it never learns what a stage computes, only how to poll
it, key it, and remember its answers.

A pipeline is **the self-contained state tracker for the steps defined
through a builder, with one way to run it and four possible outcomes**.
This document describes that crate.

A step is **two functions**, registered as `fn` pointers - a key and a poll.
Not a trait, and not `impl Fn`: both of those admit captured state, which is
an input that moves a step's output without moving its key.

!!! MOSTLY BUILT

The public API below, the version gate, the one store, the flat error and the
`fn` registration door are built. What is not is collected in **"Ruled, not
yet built"**, at the end of Internals - read that section before acting on
anything here, because several of its entries change what a memo key IS. The
companion `PLAN.md` records the migration that got the crate here.

## How to read this

Three conventions carry through every section:

* **Every claim about code names the file that carries it.** Much of the
  machinery this design composes already exists, and where a section leans
  on it, the path is given. A claim with no path is a claim about design
  alone.
* **"Public" and "internal" are drawn strictly.** The public-API section is
  the whole contract a consumer or a test may touch, and it names no
  internal type. The internals section is machinery the builder assembles,
  which may be reorganized without notice and is named there only so this
  crate can discuss itself. A consumer that needs an internal is not
  missing a re-export; it has found something the builder cannot express,
  which is a finding to record, never a licence to widen the surface.
* **Sections state the design positively.** What it is, how it works, why
  it works. Every argument against an alternative lives in one place -
  "Rejected alternatives", at the end of Internals - as a list of closed
  doors, each a ruling with its reason.

The order: what a pipeline is; the public API that delivers it; the three
decisions behind the API's shape - identity as position, one store, one
flat error; the model the engine embodies; the internals, ending with the
rejected alternatives; and the subcrate boundary that keeps the internals
internal.

## What a pipeline is

A pipeline turns a source value into derived outputs through a sequence of
pure steps, and remembers enough along the way that asking again is cheap.
It is the tool to reach for when a value is derived - a document lowered to
a render tree, a bundle compiled from sources - and the input keeps
changing: the pipeline re-derives what moved, answers from memory for what
did not, and keeps all of the bookkeeping itself. Self-contained means
exactly that: the pipeline holds what it last ran against and what it
remembered along the way, so none of that state lives in the caller and
none of it lives in hand-composed wrappers around the pipeline.

Steps are registered once, through a builder, and chain: each consumes what
the previous one produced. A step's identity is its **position** in that
registration order - the builder tracks it, and a step never declares one.
Where the whole pipeline remembers is also the builder's: one store, chosen
once - and defaulted, so most consumers never choose at all.

A built pipeline has one way to run. A run is handed a
`(version, readable)` pair: the version says **which** state of the world
this is, and the readable **is** that state. The answer comes back one of
four ways - three successes and one failure. `Computed` means work happened
and carries the new value. `Unchanged` means no work was needed and none is
outstanding - the value the caller holds is **finished**, and a version
matching the previous run's is answered this way without the readable ever
being read. `Delayed` means not ready yet: a wake is coming. `Failure`
means the run did not happen, and says which stage raised the error. Each
success asks something different of the caller - use the new value, keep
the one you hold, wait to be woken - and that distinction is what a
memoizing pipeline is for: whether work happened is the pipeline's own
knowledge, and the outcome delivers it to the caller, who is the one that
can act on it. Whether a caller blocks until the answer or returns and
waits for the wake is what the caller **does** with `Delayed`; there is no
second door for it.

The data is durable; the pipeline's runtime state is not. The action store,
the node-graph and a bundle persist. What the pipeline holds - its memo,
the version it last ran against, whatever is in flight between a `Delayed`
and its wake - dies with the process, and is meant to. On restart the data
is there, the memo is empty, and everything recomputes once: the expected
cost of a start, and several of the simplifications below are paid for by
it.

## Public API

Four types - `PipelineBuilder`, `Pipeline`, `Run` and `Failure` - joined
by one alias, `RunResult`, and one free function, `run_blocking`. A fifth
type, `Ctx`, is what a step's functions are handed. A sixth, `MemoStore`,
matters only when the default store is wrong.

### Building a pipeline

```rust,ignore
use libpipeline::{Ctx, PipelineBuilder, Run};
use libpipelinedata::{ContentKey, EffectPoll, MemoKey};

fn parse_key(input: &Source, ctx: &Ctx<'_>) -> Option<MemoKey> {
    Some(ctx.key([ContentKey::of(input)]))
}

fn parse(input: &Source, _ctx: &Ctx<'_>) -> EffectPoll<Parsed, Error> {
    EffectPoll::Ready(Parsed::of(input))
}

let pipeline = PipelineBuilder::new()
    .stage_fn("parse", parse_key, parse)
    .stage_fn("lower", lower_key, lower)
    .build();
```

* `PipelineBuilder::new()` - the empty builder, remembering into a map of
  its own.
* `.stage_fn(name, key, poll)` - register one step, as **two `fn`
  pointers**. `key` answers this input's memo key without running anything,
  which is what lets a lookup precede the work; `None` there means the input
  cannot be honestly addressed, and the step is then neither looked up nor
  recorded. `poll` answers `EffectPoll` - `Ready` with the value, `Pending`
  having arranged a wake, or `Failed`. Both receive a `Ctx`, which carries
  the identity the builder **mints** for this registration - its position and
  nothing else; the `name` beside it is a diagnostic label, not an identity
  (see "Identity is a position"). Steps chain: each consumes what the
  previous one produced. Every registered step remembers its answers; there
  is no un-remembering registration to forget.

  **`fn` and not `impl Fn`, and not a trait.** A non-capturing closure
  coerces to `fn`; a capturing one does not compile. So everything the poll
  can see, the key can see - which is what makes a memo key an honest address
  for the answer rather than one that is honest until somebody adds a field.
  "A trait-taking stage door" under "Rejected alternatives" carries the
  argument in full.
* `.uncached()` - the control switch: the pipeline remembers nothing.
  Answers must not change, only speed; a pipeline whose answers change when
  remembering is disabled has a bug the remembering was hiding.
* `.build()` - the finished pipeline, `Pipeline<V, I, O, E>`. Its input type
  is the first step's input; its output type is the last step's output; its
  error type is the one every step shares; its version type is fixed here.
  **Four of the consumer's own types and no trait of the engine's**: the
  assembled graph is erased behind a private field, so nothing about the
  machinery is spellable from a pipeline's type.

`.stage_fn()` hands back a `StagedPipelineBuilder`, which is not a further
thing to learn: its fields are private and it has no constructor, so a
consumer receives one, calls a method on it, and with method chaining never
writes its name. The four names above are the four a consumer CONSTRUCTS OR
MATCHES ON; an opaque intermediate that only appears in a return type is not
a concept beside them. `Failure` is the same category - a public type with
private fields and a private constructor - so this is a pattern the design
uses twice rather than an exception to its own count.

**`Ctx` is the one argument through which the engine gives a step anything**,
and it exists so that the registration signature does not move when the
engine gives a step more. Today it carries the minted identity -
`ctx.key(inputs)` is the only spelling a step needs, and it supplies the
identity half so a key cannot be built under another step's - and the waker,
which is what makes `Pending` an honest answer. "The intended stage shape"
under Internals records what it does not carry yet.

A consumer writes functions and assembles nothing. The vocabulary those
functions name - `EffectPoll`, `MemoKey`, `ContentKey` - is
`libpipelinedata`'s. The stage CONTRACT is not: it is
`libpipeline-internals`', because with registration taking functions nothing
outside the engine implements it.

### Running it

A pipeline runs through one method, whose outcome is a standard
`Result` spelled by one alias:

```rust,ignore
pub type RunResult<T, E> = Result<Run<T>, Failure<E>>;
```

`poll(version, &readable) -> RunResult<Output, Error>` polls once and
returns immediately, whatever the answer; nothing inside it waits, ever. The
name is what it does.

```rust,ignore
match pipeline.poll(version, &document)? {
    Run::Computed(output) => held = Some(output),
    Run::Unchanged => { /* `held` is finished and current */ }
    Run::Delayed => { /* draw the stand-in; a wake is coming */ }
}
```

The error side is `Failure<E>` rather than a bare `E` because a pipeline
is a sequence of steps, and "it failed" is half an answer: **which** step
failed is what a caller acts on, and that is the state `Failure` tracks.
`at()` gives the failing stage's position, with the stage's error beside
it. One `Failure` type serves the whole pipeline - a chain of five steps
has the same error type as a chain of two - so `?` propagates it through
any depth of assembly, and a caller that does match it matches once. See
"One error type, flat and positioned".

The `version` argument is the run version: which state the input is. It is
the only version in the API.

### The four outcomes

`Run::Computed(output)` - work happened; take the new value. The pipeline
records the version each `Computed` answered for.

The value is an `Arc` of the output, because the memo still holds it after
answering with it - that is what a memo is, so the caller does not own it
exclusively and the type says so rather than a copy pretending otherwise.
It is also what makes a large output cheap without any stage author
remembering to wrap one: the engine wraps once, where a poll function's value
enters the graph, and every hit after that is a refcount bump.

`Run::Unchanged` - the value already held derives from exactly this state;
keep it. The pipeline compares the version it is handed against the one it
last computed for, and on a match answers without reading the readable at
all. Read it as **the value is finished**: not a report that nothing
happened, but a statement that nothing needs to, which is what lets a
caller draw the value it holds and stop.

`Run::Delayed` - not ready; a wake is coming. The poll has arranged for the
pipeline's waker to be woken when the answer becomes possible, so wait to
be woken rather than re-polling in a spin. Where the wake comes from - the
original input, or a later stage internally - is unspecified, because the
caller's obligation is identical either way. A `Delayed` run does not
record the version, so asking again with the same version polls again. A
step that cannot keep the wake promise has made its value lost rather than
late, and the pipeline treats that as the defect it is (see "Delayed keeps
its promise" under Internals).

`Failure` - the run did not happen: `at()` names the stage, and the error
rides beside it. A failure is this run's answer, not the pipeline's
verdict: nothing is recorded, and a later run with the same version
retries. Neither `Computed` nor `Failure` is terminal - a pipeline is a
standing derivation over inputs that change, and a later run over a
changed version can move a failure back to `Computed`, and `Computed` to a
different value. One consequence worth knowing: after a newer version
fails, re-running the **old** version still answers `Unchanged` - the old
value still stands, which is true.

### The version

The version type is fixed when the pipeline is built and is bounded
`Copy + Eq`.

The pipeline **never computes a version**. It compares the ones it is
handed. Where a version comes from is the consumer's business - an edit
store's cursor, a build number, a git sha - anything cheap to copy and
honest about identity. That cheapness is the point of the pair: the version
costs a comparison, and the readable may be a large snapshot that a
matching version never touches. Versions are compared for identity, not
order; running an older state again is just another state.

### After `Delayed`: the wake

`.waker() -> Waker` - the wake target, for landing values out of band. Waking
it tells the pipeline that something it cannot see has moved, so the next
`poll` at an unchanged version enters the graph rather than answering
`Unchanged` off the version alone. A step that parks does this for itself
through `Ctx::waker`; this is the same target, for whoever lands a value from
outside the graph entirely.

**There is no "has a wake arrived" accessor, and its absence is the design.**
An answer that clears when it is read is a fact two readers race for, and the
gate is the reader that must not lose: a caller that asked first would have
TAKEN the wake, leaving `poll` to find none, see the version unchanged, and
answer `Unchanged` - the exact defect the wake half of the gate exists to
prevent, reintroduced from the caller's side. The flag stays internal and the
gate is its only reader.

### Blocking and frame are what a caller does

A frame-driven caller runs once per frame, and does not have to decide
whether there is reason to - the gate decides, and `Unchanged` is the cheap
answer:

```rust,ignore
match pipeline.poll(version, &document) {
    Ok(Run::Computed(value)) => held = Some(value),
    Ok(Run::Unchanged) => { /* draw what is held */ }
    Ok(Run::Delayed) => { /* draw the stand-in */ }
    Err(failure) => report(failure),
}
```

A blocking caller loops on `Delayed`, making its own progress between polls -
and that loop is shipped, because every such caller writes the same one:

```rust,ignore
let outcome = run_blocking(&pipeline, version, &document, || executor.run_once());
```

**`run_blocking` is a free function and not a second door**, and its body is
the evidence: it calls `poll` and nothing else. What it does NOT do is decide
what a stall means. `Delayed` when the pump answers `false` comes back as the
plain `Ok(Run::Delayed)`, because that condition is the caller's own - the
caller that owns the executor is the one that can see its queue is empty, and
a door that guessed would need vocabulary for its own wrong guess.

### Where answers live, if the default is wrong

By default the builder remembers into a map it owns. `.store(store)`
overrides the default for the **whole pipeline** - one store, one
decision, taken once:

```rust,ignore
let pipeline = PipelineBuilder::new()
    .store(store)
    .stage_fn("parse", parse_key, parse)
    .build();
```

Any `MemoStore` implementation will do; the trait lives in
`libpipelinedata` beside the stage contract, and "One store, at the
builder" gives its shape. The builder instantiates it at the erased value
type, `dyn Any + Send + Sync`, which a store written for this purpose names
directly; one written generically over its value type declares that
parameter `?Sized` and is otherwise unchanged.

### What else is public

The whole public surface: `PipelineBuilder`, `Pipeline`, `Run`, `Failure`,
the `RunResult` alias, `Ctx`, the `run_blocking` function, and the
`MemoStore` seam. The vocabulary a step's functions name (`StageId`,
`MemoKey`, `ContentKey`, `EffectPoll`, `MemoStore`, `MemoMap`, `NoMemo`)
lives in `libpipelinedata`. `StageId` is held, never constructed - the
builder mints one per registration and hands it through `Ctx`.

`Ctx` and `run_blocking` are additions to the count this section used to
give, and each is forced rather than chosen: a `fn` door's signature must
name what it hands the function, and the blocking loop is the same few lines
in every caller. Neither is a way INTO the engine that `poll` is not.

Everything on the composition side - the stage contract itself, tracking,
memoization, chaining, scheduling, driving - reaches a consumer's functions
only through registration and cannot be named from outside this crate.

## Identity is a position

A step's identity is the index the builder mints at registration - nothing
else. The builder sees every registration in order, mints the identity from
that order, and hands it to `make`; a step never declares one.

**A name is a diagnostic.** A failure that says "stage 3, lower" is better
than one that says "stage 3", so registration takes a label - and the
discipline that keeps a label a label is enforced rather than intended:

* it never enters a memo key;
* nothing is looked up by it, keyed by it, or compared on it;
* two steps may share one with no consequence whatsoever.

That last property is the test: a label two steps can share without
consequence is a label nothing depends on.

Position is also what makes the flat error spellable: `at()` answers "which
stage failed" as an index, directly, at any length of chain.

## One store, at the builder

Where the pipeline remembers is one decision, taken once, at the builder,
about the whole pipeline - defaulted to a map the builder owns, overridden
by `.store(store)`. Registration owns **whether** a stage is memoized; the
builder owns **where**.

### The erased row

One store serves stages of many output types by erasure, and the erasure is
`Any`: it needs only `V: 'static`, and it costs a downcast per lookup and
nothing else.

**The seam accepts and returns `Arc`, on both sides, always**
(`libpipelinedata/src/store.rs`): `lookup` answers `Option<Arc<V>>` and
`record` takes `Arc<V>`. A store holds a stage's output for as long as it is
worth remembering, and what a lookup hands back is a SHARE of it - not a
copy, and not a borrow, because a cache handing out borrows "ties the
caller's frame to the store's lock" (that file's own doc).

It is unconditional, and that is the part worth defending. A contract that
shared large outputs and copied small ones would put the choice on the stage
author; that judgement gets made once, at the moment the output is small,
and then silently outlives the output growing. Nobody revisits it, and the
symptom is a copy per hit that no test can see - answers do not change, only
speed does, which is the one axis a memo is invisible on. Unconditional
means nobody chooses and nobody can choose wrong, which is the same rule
this design applies to memoization being intrinsic to registration.

The erasure follows from that shape. `MemoStore<V>` is generic over the
value type and `V` is `?Sized`, so the builder's store is a `MemoStore`
whose `V` is `dyn Any + Send + Sync` - the unsized type itself, not a handle
wrapped around one. Recording is then an unsizing coercion of the share the
memo layer already made, and a lookup is `Arc::downcast`. `Send + Sync`
follows from the store being shared across threads, which is the same reason
`MemoMap` holds a `Mutex` rather than a `RefCell` (its own doc). The blanket
`impl<V: ?Sized, S: MemoStore<V> + ?Sized> MemoStore<V> for Arc<S>` is what
lets one store sit in front of several stages, which is what it was written
for.

`V: Clone` appears nowhere: with the contract `Arc`-shaped there is nothing
to clone but a refcount.

### A memo hit is cheap

One allocation per miss, none per hit. The memo layer wraps a stage's output
once, at the point it records it, so the row and the value it answers with
are the same allocation; the erased store coerces that share rather than
wrapping it again; and every hit afterwards is a downcast plus a refcount
bump, whatever the output is. For an output the size of a whole bundle, that
is the saving a memo exists for - and it arrives without any stage author
having shaped their output as an `Arc` to get it.

### The store belongs to the pipeline

`.store(store)` hands the store over at the builder, the pipeline holds it,
and it lives exactly as long as the pipeline does.

That ownership keeps the downcast honest. A row's key carries the stage's
identity; identities are positions; positions are minted one per
registration by a single builder, so within one pipeline no two stages can
share one. A failed downcast would mean an identity collision, which cannot
be constructed - so the lookup `expect()`s the invariant and names it.

## One error type, flat and positioned

`Failure` carries the position of the stage that raised the error - read
through `at()` - and the error, and that is all. Every step of one pipeline
shares an error type, so a chain of five steps has the same error type as a
chain of two, spellable in one line of a test signature.

Position is already a stage's identity, so `at()` answers "which stage
failed" directly, in one call, at any length of chain. The flat error is
only spellable because identity is a position; the two decisions meet here.

**The cost, recorded.** Stages of one pipeline share an error type, so a
consumer with genuinely disjoint failure modes writes one enum unifying
them - which is what such a consumer would write anyway to match on the
result, and writing it once at the pipeline names the union in one place.
It **is** a constraint, and it is named as one rather than presented as
free.

Internally the position is stamped where the builder knows it - at
registration, the only place the index is available - and the composed
stages propagate the error unchanged.

## The model

The commitments the engine embodies, each carried by a named piece of the
crate. Source comments cite these section names rather than restating them.

### A pipeline is a chain of pure stages

A stage consumes one input type and produces one output type; stages
compose by the next stage's `Input` equaling the previous stage's `Output`.
The composite of two stages is itself a stage, so a graph is never a second
kind of thing the engine must know how to walk (`src/chain.rs`). A stage is
driven by a poll/waker protocol (`libpipelinedata::Stage`, reusing
`libeffects`' poll contract): `Ready`, `Pending` - which obliges the stage
to arrange a wake - or `Failed`. A stage cannot tell how it is being
driven: the same stages answer a blocking caller and a frame caller, and no
stage learns which is asking.

### The lookup precedes the work

Memoization is keyed by `(stage identity, content keys of the inputs)` - a
key computable **before** the stage runs, so the cache can skip the work
rather than validate it afterwards. An unchanged input hits at the first
stage and the rest of the chain is never entered. Only `Ready` is recorded:
`Pending` is not a value, and a failure is deliberately never cached - a
transient failure served back under a key that says it is fresh would be a
settled fact that never was. A stage that must not be served from cache
says so through `memo_key -> None` and is neither looked up nor recorded
(`src/memo.rs`). Content keys are streaming hashes; the vocabulary lives in
`libpipelinedata`. Where answers are remembered is a seam (`MemoStore`),
asked once, at the builder - and the stage half of every key is a position.

### Reads are observed, not declared

Dependency edges are **recorded by observing reads**, never accepted as a
declared list: while a stage runs, every tracked read is logged, and the
set is re-logged on every run so it follows conditionals. The record is
kept at the run's edges - what a run read at the input boundary, and the
content address of what came out at the output boundary ("Read-state
tracking at the edges" under Internals). The observation machinery lives in
`src/track.rs`. The rule itself is independent of scope and is not in
question.

### The engine stays generic

The engine never learns a consumer's types. Everything is generic over
`S: Stage`; every test invents stand-in types of its own. The proof is
mechanical: `tests/engine_stays_generic.rs` walks this crate's manifest
and, through its path dependencies, every manifest under it, and fails if
the tree names a crate outside the stack's closed allowlist (`THE_STACK` in
that file). The stack is four crates, dependencies pointing strictly
downward:

| crate | role |
|---|---|
| `libpipeline` | the facade: the builder, the pipeline, `Ctx`, the one door |
| `libpipeline-internals` | the machinery: the `Stage` contract, composition, memoization, tracking, the poll loops |
| `libpipelinedata` | the vocabulary: the key types, `ContentHash`, `MemoStore` |
| `libeffects` | the base: the poll/waker protocol, boundaries, wake flags |

## Internals

**Nothing in this section is reachable by a consumer, and nothing in it
appears in a public-API example.** The names exist so this crate's
maintainers and tests can talk about the machinery; "The subcrate boundary"
below is what keeps them internal.

### The layers

Bottom-up, the machinery the builder assembles:

* **Composition** (`src/chain.rs`) - two stages composed, itself a stage.
  Refuses to key (`memo_key -> None`); its parts are memoized instead. Both
  halves share the pipeline's one error type, so a join propagates the
  error unchanged, and the failing position is stamped at registration.
* **Memoization** (`src/memo.rs`) - the memo layer: lookup precedes the
  work, only `Ready` recorded, and the store is skipped entirely while
  `revalidating()` (`src/track.rs`) is true - the thread-local channel by
  which read tracking outranks the store without any stage declaring
  anything. Merged into registration: every `.stage_fn()` call wraps its
  stage in one, each holding a shared handle to the builder's one store.
* **Read tracking** (`src/track.rs`) - read observation at the edges, the
  wake subscription that reaches the gate's stale flag, and the output-edge
  cutoff.
* **The poll loop** (`src/driver.rs`) - the engine's single poll: one pass,
  returns immediately, records wakes in a flag.
* **Error boundaries** (`src/boundary.rs`) - a stage-level boundary that
  turns a failure into a substituted `Ready` and counts the substitution.
  Machinery awaiting a builder spelling; until it has one, no consumer
  reaches it.

Two composition rules hold by convention rather than by type, each stated
in the owning module's doc and pinned by a known-bad twin:

* **The cache goes inside the tracking** - a cache outside the tracking
  answers before any run scope opens, so the staleness mark goes unread
  (`src/memo.rs`'s doc). The builder owns this order so it is unwritable.
* **The boundary goes outside the tracking** - a substituted `Ready` inside
  the tracking reports a node up to date while it is still owed its real
  answer (`src/boundary.rs`'s doc). The third rule of the family - a
  boundary belongs outside the memo - is closed structurally by the
  boundary refusing to key.

### The version gate and the one door

The one door is the engine's single poll plus a version gate plus an
outcome mapping - no new engine semantics anywhere in it.

`Pipeline<V, I, O, E>` holds the erased graph, the poll machinery, and
`last: Mutex<Option<V>>` (safe interior mutability, as everywhere in this
crate; `poll` keeps `&self` because a poll holds `&self` all the way down).
`poll(version, input)`:

1. If `last` holds exactly `version` **and no wake is pending**: return
   `Ok(Run::Unchanged)`. The readable is not dereferenced, no memo key is
   computed, no stage is polled.
2. Otherwise poll once (`src/driver.rs`) and map the answer: `Ready(v)`
   becomes `Ok(Run::Computed(v))` and records the version; `Pending`
   becomes `Ok(Run::Delayed)`; a failed poll becomes `Err(failure)`.
   Nothing else records the version.

**The wake half of that condition is not optional, and omitting it is a
silent defect.** Two different things mean "something happened", and only
one of them moves the version:

* the **input version** moved - the source changed;
* a **wake** arrived - a value some stage was waiting on has landed.

A landed effect does not move the input version. The source did not change;
an awaited value simply arrived. So a gate that checked the version alone
would take a pipeline sitting on `Delayed`, receive the wake, re-poll,
short-circuit on the unchanged version, and answer `Unchanged` - forever.
The delayed value is never delivered, and nothing reports it: the caller
sees a legitimate-looking `Unchanged` and holds a value that is permanently
one step stale. That is precisely the failure this crate exists to prevent,
arriving through the gate meant to prevent it.

The mechanism is the stale flag itself: it answers "has a wake arrived since
this was last asked" and clears on read, and the gate is its only reader -
which is why no accessor exposes it (see "After `Delayed`: the wake").
Ordering matters: because it clears when read, the gate must consume it on
**every** poll, not only when the version matches. A poll that entered the
graph for a version change and left an unread wake behind would answer
`Unchanged` on the next poll despite the wake - the same defect one step
displaced.

The error arm is a re-wrap and not a construction: with the position
stamped at registration, the graph's own error type **is** the flat
`Failure`, so the door moves it onto `Result`'s error side and adds
nothing.

The version gate sits above the whole graph, outermost: it is the pipeline
remembering what it last ran against, not a per-stage concern. Stage-level
memoization still does its work on the version-mismatch path - an input
that moved its version but not its content still hits at the first stage. A
later wiring of read tracking to the gate would let `Unchanged` also fire
when a recompute reaches the root with an unchanged content address
(root-level backdating), through the same variant, with no API change.

### Read-state tracking at the edges

The tracked layer is a record kept at the run's edges: **what a run read**
at the input boundary, and **what came out** at the output boundary.

* **The read observation** - the tracked-input wrapper and the run scope
  that gives "while a stage runs" a beginning and an end (`src/track.rs`).
  Recording reads at the boundary is one scope's worth of the same
  mechanism that could serve many.
* **The wake** - the subscriber list a change notifies, which is what
  reaches the gate's stale flag.
* **The memo-outranking channel** - `revalidating` (`src/track.rs`), read
  by `src/memo.rs` so that a store cannot answer for a run whose ambient
  input moved. A key built from a stage's arguments cannot see an ambient
  read, and the run-scoped channel is what lets the engine see it anyway.
* **Cutoff at the output edge** - the content-address comparison, applied
  at the root: a recompute that reaches the same value spares the caller.
  This is the root-level backdating the version gate already anticipates as
  a second source of `Unchanged`.

When the builder grows a spelling for tracking, it grows this shape: reads
observed at the input boundary, a content-address cutoff at the output
boundary. Machinery is removed against a spelling that exists, never
against a design statement alone - deletion follows the caller.

### Delayed keeps its promise

`Delayed` publicly promises "a wake is coming", and the engine checks it.
The watched poll (`src/watch.rs`) measures, per poll and in safe code,
whether a poll that could not answer left a wake path. The one door polls
through it in debug builds and panics when the path is missing, with the
diagnosis: a stage answered `Pending` without arranging a wake - the value
is lost rather than late. Release builds poll plain and trust the stage
contract, keeping the probe allocation out of the hot path.

One decision here is genuinely open: an accessor exposing a wake-debt count
could replace the debug panic. The shape of the check is the same either
way.

### The intended stage shape: a function, with everything through Ctx

**The registration half is built.** A stage is a pair of `fn` pointers - a
key and a poll - each handed a `Ctx`, and the type refuses captured state at
compile time: everything the poll can see, the key can see. What `Ctx`
carries today is the minted identity and the waker, which is the minimum a
poll needs in order to answer `Pending` honestly.

**Two things it does not carry, and their absence has a cost that is being
paid now rather than avoided.**

* **The read log.** Reads should go through `Ctx` - `Ctx::observe_read` -
  so that a stage's ambient inputs enter the read-set and the memo can be
  outranked for them. The machinery exists (`src/track.rs`) and has no
  builder spelling, so nothing routes through it.
* **In-flight and ambient state.** State between a `Pending` and a `Ready`,
  and any handle a stage needs from the world - an upstream, an atlas, a
  module runtime - should live in a store the consumer provides through a
  trait seam (as `MemoStore` already is), addressed by
  `(PipelineId, position)`. The `Ctx` carries **access to a store**, not a
  world. In-flight state is runtime state, so the durability decision applies
  to it in full: it does not survive a restart, and the same `Any` erasure
  serves it. Whether it is the store the builder already holds or a second
  seam beside it is not settled here; both are "at the builder" in the sense
  the decision means.

  **Until it exists, a stage's only honest route to anything ambient is a
  `static` or a `thread_local`**, which the `fn` door permits and no type can
  distinguish from a captured field. That is the door's one real gap, and it
  is visible in this crate's own tests: every counter and every upstream in
  `tests/` is a thread-local because a `fn` cannot hold one. The gap is
  narrower than a captured field - a `static` is shared, named and greppable
  where a field is private and invisible - but it is a gap, and naming it
  here is the alternative to pretending the door is complete.
* **`PipelineId` is the serial the builder mints, and nothing else.** The
  serial carries instance identity by itself, and a rebuild mints a new one,
  so keyed state dies with the instance. Not built.

### Rejected alternatives

The closed doors. Each entry is a ruling with its reason - the sections
above state what the design is; this list is where every argument against
an alternative lives, in full.

* **A blocking door beside the frame door.** Not the design: there is one
  way to run a pipeline, and blocking is a caller pattern - a loop on
  `Delayed` pumping the caller's own executor. Two doors make waiting the
  pipeline's job, and the same state then means opposite things at each
  door: a poll that cannot progress is a defect to one caller and a normal
  frame to another. Only the caller can tell which, because only the caller
  can see whether its queue is empty; a door that decides for it must grow
  vocabulary for its own wrong guess.

* **A single four-variant outcome enum.** Not the design: the outcomes
  split by kind into `RunResult<T, E> = Result<Run<T>, Failure<E>>`,
  because `Failure` means the run did not happen and the three `Run`
  variants mean it did. Flat, the four variants would deny callers `?`: a
  caller that only cares about failure - the common case in a lowering
  chain - would match all four arms forever.

* **Dropping `Unchanged` and always answering `Computed`.** Not the design:
  cheaply handing the old value back settles the pipeline's side and not
  the caller's. A frame loop given `Unchanged` skips layout, render and
  diff entirely; given `Computed(same_value)` it must either redraw or
  compare what it was handed against what it held - the pipeline's own
  knowledge, reconstructed one layer up by whoever remembered to.
  `Computed` when nothing was computed is also untrue, and the contrast
  with `Unchanged` is the whole reason the variant is not called `Ok`.

* **`PartialEq` for the version bound.** Not the design: `PartialEq`
  admits `f64`, `NaN != NaN`, and a version that never equals itself makes
  the `Unchanged` gate silently never fire. `Eq` refuses the type instead
  of shipping the symptom.

* **A stage version.** Not the design: a version declared in code is a
  compile-time constant, so it can only distinguish one compilation from
  another - its entire domain is a memo that outlives a rebuild, and none
  does: runtime state is not durable, so no memo survives the recompilation
  that could poison it. The version would guard a window that never opens,
  and every discipline spent keeping it honest would be cost with no
  payoff. The other stale-key case - an ambient input that moves **within**
  a process - was never the version's to cover, because no constant can be
  bumped at runtime; it belongs to read observation.

* **A name as stage identity.** Not the design: identity is the position
  the builder mints, and a name is a diagnostic. A name that enters a memo
  key, or that anything looks up or compares on, is an identity again -
  self-declared, collidable, and carrying the stale-key defects declaration
  invites. And with the builder the only source of an identity, there is no
  second identity for an honest author to answer with, so no
  construction-time identity check exists either: the defect it would catch
  cannot be constructed, which is better than being caught.

* **A pipeline shape hash.** Not the design: over positions a shape hash
  reads `0,1,2` for every three-stage pipeline in the program and
  distinguishes nothing. The serial carries instance identity by itself,
  and a rebuild mints a new one, so keyed state already dies with the
  instance - everything the hash would have been belt-and-braces for.

* **A trait-taking stage door.** Not the design: a door typed on a trait
  hands back a struct, and structs accrete fields - each one a candidate
  input that moves the output without moving the key. The `fn` door makes
  the field impossible rather than reviewable. `impl Fn` fails the same
  way, one increment earlier: it permits capture.

  This crate SHIPPED the trait door for three waves, because the Public API
  section above demonstrated it - `.stage(name, |id| Parse::new(id))`, a
  closure constructing a struct that implements `Stage` - while this entry
  rejected it. The section was the half that was wrong, and the record of
  that is worth more than a silent correction: an example is a specification
  to whoever implements against it, and it outranks a paragraph three
  sections away every time.

* **A typed store factory at the builder.** Not the design: minting a
  `MemoStore<V>` for arbitrary `V` needs a generic method, a generic method
  is not object-safe, so the factory cannot be `dyn` - and it would then
  thread as a type parameter through `PipelineBuilder`, every intermediate
  builder state, and the pipeline's own type. The one type a consumer names
  would carry the store implementation in it, which is the opposite of a
  seam.

* **Serde-erasure of the store.** Not the design: values that can be
  written out and read back are the erasure a durable store needs, and the
  runtime state is not durable. It would also put a serialization bound on
  every stage output - a far heavier tax than `'static` for a capability
  nothing asks for.

* **Per-stage stores.** Not the design: where the pipeline remembers is one
  decision about the whole pipeline. Asking each registration where to
  remember is the assembly-at-each-call-site pattern the builder exists to
  remove, one level down - registration already owns whether a stage is
  memoized.

* **A required store choice.** Not the design: the store is defaulted.
  Where answers live is a decision almost every consumer answers the same
  way, and a required parameter for a decision nobody varies is friction
  that teaches nothing. Made a parameter, the seam would stop being a seam
  and become part of what the crate is - and a seam you must understand in
  order to use the crate at all is not a seam.

* **A store that outlives its pipeline.** Not the design: the store belongs
  to the pipeline. Two pipelines sharing a store would both hold a stage 0;
  scoping every key by a per-pipeline serial to prevent the collision is
  self-defeating, because instance-scoped keys share no rows and a rebuild
  - which mints a new serial - inherits nothing, so the shared store would
  save one allocation and nothing else. Recomputing once after a rebuild is
  the expected cost of a start, arriving one build earlier. Sharing is also
  what would make the lookup's identity-collision `expect` reachable.

* **Nested per-join errors.** Not the design: one error type per pipeline,
  flat, the position through `at()`. A per-join tag nests once per join:
  two stages read one layer, five stages read a type nobody writes in a
  signature, and "which stage failed" is answered by counting layers -
  position arithmetic performed by hand against a type shape. Position is
  already the stage's identity, so one call answers the question at any
  length of chain.

* **An internal dependency graph.** Not the design: the wake topology
  already is the dependency structure. Composition hands both halves of a
  join the same `Context` (`src/chain.rs`), so one waker serves the whole
  chain and a wake re-polls from the top - and memoization is what makes
  that a wake graph: on the re-poll every stage before the waiting one hits
  its memo and returns at once, so waking everyone costs only the stage
  that had work to do. A node-to-node graph earns itself only where stages
  fan out and share dependencies, so an invalidation can reach some
  consumers and not others; a linear chain, each stage with exactly one
  predecessor, cannot ask that question - a reverse index, transitive
  marking and per-edge retraction would maintain, at cost, a structure the
  wake path expresses for free. The cost acknowledged: per-node staleness
  is finer than per-run staleness when a stage recomputes to the same
  value - a per-node ledger spares the stages after it, the run-scoped
  channel does not. The output-edge cutoff still spares the caller, and
  whether the intra-run difference is worth a graph is a measurement nobody
  has taken; taking it is cheaper than keeping the machinery against the
  possibility.

* **A separate watched drive.** Not the design: the wake obligation is a
  property of `Delayed`, checked where the answer arises - the door itself
  polls through the watched probe in debug builds and panics when a poll
  leaves no wake path. An optional diagnostic a caller must remember to run
  is a contract nobody enforces.

### Ruled, not yet built

Decisions taken and not yet implemented, each recorded here so that no
example above is read as the last word. Nothing in this list is speculation:
each is a ruling.

* **The version is a hash, and the engine does not content-key.** A memo key
  today is `(position, content keys of the inputs)`, and the engine computes
  those keys by hashing what it is handed. It should not: the source already
  content-addresses its own writes at the boundary where that is cheap, and
  the version it hands `poll` IS that address. Hashing again to rediscover
  what the caller already told us is `O(input)` per poll for an answer already
  in hand.

  The layering this settles is worth stating on its own, because each layer
  hashes only what it is competent to hash: **the STORE addresses its writes
  and hands the pipeline a version; the DOMAIN addresses its own trees to
  decide whether it changed anything; the ENGINE addresses neither and trusts
  what it is handed.** A pipeline that knows nothing about what a tree is has
  no business deciding whether one moved.

  What this deletes from the engine is the CONTENT-KEY-AS-MEMO-KEY use, not
  the hashing vocabulary: `ContentKey` and its hashers are what the domain
  needs for its own early exit, and they relocate rather than die (see "What
  `libpipelinedata` is for" below).

* **A keyed variant beside the default.** With the version as the default
  key, a stage that can cheaply say what PART of its input it depends on
  should be able to say so, in the shape `sort_by_key` has: a key extractor
  supplied beside the poll function.

  ```rust,ignore
  .stage_fn(name, key, poll)          // proposed default: keyed by the version
  .stage_by_key(name, extract, poll)  // keyed by what `extract` says
  ```

  **The engine still never hashes** - the author's function produces the
  identity and the engine only compares it, which is the version rule applied
  one level down. It is opt-in and the default serves the majority, which is
  the same shape the store override has.

  Internally it is one `Option` field on the engine's own stage struct:
  `None` means "keyed by the version", `Some(f)` means "keyed by `f`". That
  is not the rejected trait door returning - the argument there was about
  CONSUMER-authored structs accreting fields the engine cannot see, and this
  struct is the engine's own, which knows what is in its key because it put
  it there.

  **What the extractor returns is the open question.** A type parameter `K`
  is awkward on an `Option`: with `None` there is nothing to infer it from, so
  it needs a phantom or a default, and it would have to reach `MemoKey`, which
  today carries `ContentKey` operands and is `Hash + Eq + Clone` because the
  default store is a `HashMap`. Erasing to a scalar the engine can compare -
  a `u64`, or the existing `ContentKey`'s `u128` - avoids the inference
  problem entirely and keeps the no-hashing rule intact. The alternative,
  a generic `K: Hash + Eq + 'static` erased into the key, buys arbitrary
  author-chosen identities at the cost of a second erasure beside the row's.

  **Nothing in the door as built forecloses it**: the shipped `.stage_fn`
  already takes a key function, so the variant is the DEFAULT changing rather
  than a new parameter appearing.

* **A stage answering `Unchanged`.** Early cutoff can be OBSERVED from a
  stage's own answer or RECONSTRUCTED by comparing outputs afterwards.
  `Backdated` (`src/track.rs`) is the second: it addresses each `Ready`
  output and retracts its consumers' staleness when the address repeats -
  work after the fact, and work the domain has often already done. The first
  is cheaper and is already present in the consumer's vocabulary: a domain
  pass that rewrote nothing says so at its return, and the distinction is
  then discarded because both paths return the same type.

  A stage-level `Unchanged` is a different shape from the caller-facing one:
  the caller's carries nothing, because the caller already holds the value; a
  stage's must still carry the value, because its consumers need one either
  way. "The same value, and I am telling you it is the same" - not "no
  value".

  A stage that can answer it needs nothing the door does not already pass:
  it has its input and, once the version is the key, the version. What it
  needs is somewhere to PUT the answer, which is the outcome enum gaining a
  variant.

* **What `libpipelinedata` is for.** With `Stage` internal, the crate's
  stated purpose - author a stage without linking the engine - has no
  consumer, because no crate's role is authoring stages: a domain crate
  implements its domain operation, and whoever ASSEMBLES wraps it in a
  registration and links the engine. What is left is reported in
  `PLAN.md`; the short form is that the hashing vocabulary relocates to
  where the domain needs it, `MemoStore`'s value as a seam is in doubt, and
  the residue is small.

## The subcrate boundary

"Public versus internal" is a **crate** boundary. The facade,
`libpipeline`, holds the builder, the pipeline, the public tests and the
README; the machinery lives in `libpipeline-internals`, public within its
own crate and integration-tested through an API of its own. The boundary is
unfakeable - no `pub(crate)` mistake, no test import that quietly widens,
can expose machinery through `libpipeline` itself.

The facade re-exports nothing from the internals crate: every public name -
`PipelineBuilder`, `Pipeline`, `Run`, `Failure`, `RunResult` - is the
facade's own vocabulary, each a visible decision in `src/lib.rs`. No glob,
no module re-export.

### The shape

A nested subcrate, `libpipeline/libpipeline-internals/`, on the workspace's
own precedent: `libpipelinedata-macros` nests inside `libpipelinedata`
because that subrepo's root **is** its crate, and a path dependency inside
the workspace directory becomes a workspace member on its own
(`../libpipelinedata/Cargo.toml` records the reasoning). The same
constraint holds here, so the same shape applies.
The stage contract itself lives in the internals crate, which is what makes
the boundary total: with no consumer implementing it, a trait the facade
cannot name is a trait a consumer cannot name. That is also why `Pipeline`
and `StagedPipelineBuilder` erase their graph - a public signature saying
`impl Stage<..>` would have put the machinery back in the facade's vocabulary
under a different spelling, and one indirect call per stage per poll is a
cheap price for not doing that.

`libpipeline-internals` depends on `libeffects` and `libpipelinedata` only;
`libpipeline` adds the path edge to it, and the generic-stack walk
(`tests/engine_stays_generic.rs`) checks its manifest through the same
transitive traversal it already performs.

### What it costs, honestly

* **The dead-code arithmetic dies.** Everything `pub` in the internals
  crate is "used" by definition, so a lint armed under test - catching
  machinery that becomes genuinely unused - is lost. The replacement
  measurements are the facade's explicit export list and the internals
  crate's own test suite; a weaker signal, and named as such.
* **The boundary is unfakeable but not unreachable.** A consumer could add
  a manifest edge to `libpipeline-internals` directly. Nothing in this
  subrepo can police other crates' manifests; the guard is the same one
  `libpipelinedata-macros` has - the edge is glaring in review, and the
  internals crate's docs state it is not a supported surface.
* **Two crates to compile and version instead of one**, one more manifest
  in the generic-stack allowlist, and the facade's doc links to internals
  become cross-crate paths.
* **The test ratio becomes a measurement of coverage.** The internals tests
  are integration tests of the internals crate; the count of tests in
  `libpipeline/tests/` remains the measurement of the public API's reach,
  and a property only an internals test can state remains a finding about
  the builder, recorded rather than papered over with a re-export.

## What a test holds

One rule about tests is load-bearing enough to state in the design rather
than leave in working notes: a test's name and comments **claim** what it
holds; only mutating the code under it - break one thing, run the suite,
see who notices - shows what it actually observes. This crate has already
had a test whose every named claim turned out to be unobserved: deleting
the layer it was named for changed no assertion. The method, not the
incident, is the rule. Apply it when a test is relocated, and whenever a
test's claims are load-bearing for a decision.
