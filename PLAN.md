# libpipeline: plan

`DESIGN.md` describes the crate and carries the reasoning; it is the durable
document. This one is scaffolding for the agent doing the work: where the
crate stands at `790863b`, what is left, and in what order. It is consumed by
the migration and retired with it. Nothing here is argued that `DESIGN.md`
already argues.

Verified against `libpipeline` at `790863b` (branch `highbay-clean`, clean)
and `libpipelinedata` at `db3c1eb` (branch `highbay-clean`, clean), on
2026-08-25. Every claim about code names its file; every count was run.

## How to read this

* Present tense describes the crate at this revision. Steps are imperative
  and have not landed. Landed steps are kept, marked LANDED with their
  commit, because a landed step's gate is the evidence the next one stands
  on.
* **Numbers are cited from source and are never reused.** `PLAN.md`'s
  finding 1 is cited from five internals test files, finding 2 from three,
  finding 4 from one, and **step 6** from
  `libpipeline-internals/tests/tests.rs:13` and
  `libpipeline-internals/tests/an_unwakeable_poll_is_visible_offline.rs:32`.
  Steps 1-5 landed, step 6 is still "Delayed keeps its promise", and new work
  starts at 7.
* Two section headings are cited by name and are kept verbatim: **"Two
  drivers, one graph"** (`libpipeline-internals/src/driver.rs:4`) and **"Not
  built yet"** (`libpipeline-internals/src/chain.rs:82`). Check
  `grep -rn "PLAN.md" src libpipeline-internals/src tests libpipeline-internals/tests`
  before renaming any heading.
* Vocabulary is the shipped vocabulary: `stage_fn`, `Ctx`, `poll`,
  `RunResult<T, E> = Result<Run<T>, Failure<E>>`, `run_blocking`. The
  four-door and `Stage`-trait spellings are gone and are not translated here.

## Where the crate stands

### The landed record

| step | commit(s) | what it left |
|---|---|---|
| 1 - the subcrate split | `251f1c9` | `libpipeline-internals/` as a nested crate; the nine `#[cfg(test)]` modules moved to its `tests/`; five `#![cfg_attr(not(test), allow(dead_code))]` deleted |
| 2 - identity is a position | `ac2299a`, data half `85c3cd1` | `StageId` is `{ index: usize }` (`libpipelinedata/src/key.rs:29`), minted by the builder; `version` and `checked` gone; `StageId::new(name, version)` gone |
| 3 - one store at the builder | `ac2299a` | `.store()` as the override, `.uncached()` as the control; `stage_in` and the `St` per-registration parameter gone; rows erased to `dyn Any + Send + Sync` (`src/builder.rs:76`, `src/builder.rs:148`) |
| 4 - one flat error | `ac2299a` | `Failure<E>` with private fields and `at()` (`src/builder.rs:280`); `ChainError` and its two `map_err` arms gone |
| 5 - the outcome and the one door | `7a20d63`, data half `402099c` | `Run`/`RunResult`, the version gate with its wake half; `MemoStore` is `Arc` on both sides with `V: ?Sized` |
| the door flip | `790863b`, data half `db3c1eb` | `stage_fn(name, key, poll)` taking two `fn` pointers; `Ctx` carrying `StageId` and waker; `Stage` moved to `libpipeline-internals/src/stage.rs`; the graph boxed behind private fields (`src/builder.rs:86`); `poll` replacing `run`; `take_stale` off the API; `run_blocking` a free function; the `Arc` out of the associated type; `Shared` deleted |
| 7 - the ladder | this commit, data half this commit | `StageAnswer` in `libpipelinedata` (`src/answer.rs`), inside `EffectPoll::Ready`; `Chain` skips its second half on an `Unchanged` first half and holds a `Joint` for what it owes; `Memo` holds the slot and asserts the cold-slot invariant; `Pipeline::poll` maps a root `Unchanged` and records the version for it |

The door flip is deliberately unnumbered: step 6's number is cited from
source and could not be taken.

### Gates as they stand

Run at this revision, all green:

* facade: **35 tests + 7 doctests** - `builder_is_the_only_door.rs` 11,
  `engine_stays_generic.rs` 7, `one_door_two_patterns.rs` 12,
  `a_stage_that_rewrote_nothing_says_so.rs` 5 (step 7); one doctest ignored
  (`run_blocking`'s `rust,ignore` sketch). The seventh doctest is the
  README's `unchanged` example, added with step 7.
* internals: **77 tests** in 11 files -
  `a_boundary_is_not_a_cacheable_answer` 5,
  `a_build_can_ask_whether_it_stood_on_a_fallback` 7,
  `a_fallback_is_not_a_revalidation` 8,
  `a_stage_boundary_catches_what_its_stage_raises` 8,
  `an_equal_recompute_stops_at_its_node` 7,
  `an_unwakeable_poll_is_visible_offline` 3,
  `invalidation_marks_dependents` 13, `reads_become_edges` 9, `tests` 6,
  `the_schedule_polls_each_node_once` 8,
  `the_stage_contract_is_the_engines` 3.
* `libpipelinedata`: **39 tests + 1 doctest** (default features); three
  doctests ignored, the third being `StageAnswer`'s `rust,ignore` sketch.

`cargo doc --no-deps -p libpipeline` is NOT clean: 5 warnings, two of them
real (see "Found stale at this revision"). No step below has taken rustdoc
as a gate; step 13 does.

### The public surface

`src/lib.rs:116`: `PipelineBuilder`, `StagedPipelineBuilder`, `Pipeline`,
`Run`, `RunResult`, `Failure`, `Ctx`, `run_blocking`. Nothing is re-exported
from `libpipeline-internals`, and that is checkable rather than intended.

A consumer still takes **two manifest edges**: a key function names
`MemoKey`, `ContentKey` and `EffectPoll`, all `libpipelinedata`'s, and the
facade re-exports none of them. 13 `ctx.key(..)` sites across `tests/` and
`README.md` prove it. That is finding 8, and it is now sharper than when it
was written: see the decision points.

### Two drivers, one graph

The same graph runs under two drives and a stage cannot tell which is
polling it. Both are internals with their own tests, and both are now
CALLER patterns rather than doors: `FrameDriver::poll_frame`
(`libpipeline-internals/src/driver.rs:143`) is what `Pipeline::poll`
(`src/builder.rs:676`) calls once; `run_to_completion`
(`libpipeline-internals/src/driver.rs:74`) is the reference semantics for
what `run_blocking` (`src/builder.rs:714`) does over `poll`.
`run_to_completion_watched` (`libpipeline-internals/src/watch.rs:156`) is the
same loop reporting `Pending` polls that left no wake path. The claim is
unchanged by the flip; only who owns the loop moved.

### Not built yet (engine-level, distinct from the builder findings)

* **The derived-key fold for composites.** A chain's own memo key would be a
  fold over its parts; until that exists `Chain` honestly refuses to key
  (`libpipeline-internals/src/chain.rs:82`) and its parts are memoized
  individually. `CHAIN_ID` is `StageId::at(usize::MAX)`
  (`src/builder.rs:358`), which a builder cannot mint, so a composite's id
  cannot collide with a stage's.
* **Deep verification.** `Backdated`
  (`libpipeline-internals/src/track.rs:701`) cuts off where a node's output
  repeats, which needs the node to have run. Sparing a node's consumers
  before it runs at all is not here, and neither is a policy for which nodes
  are worth addressing per poll.
* ~~**Root-level backdating.**~~ **CLOSED by step 7, and not in the shape it
  was written.** `Unchanged` does fire when a recompute reaches the root - but
  not by addressing the output there and comparing. The STAGE says so, the
  stages after it are never entered, and the answer travels to the root, where
  `Pipeline::poll` records the version for it. No content address is taken.

## What the builder cannot yet express (findings, in priority order)

The numbering is load-bearing: internals test docs cite findings 1, 2 and 4
by number.

1. **Tracked state graphs.** `Ledger`/`Tracked`/`TrackedInput` composition -
   including the load-bearing wrap order - is exactly the assembly the
   builder exists to own, and the builder has no spelling for it. 45 tests
   over the tracked and schedule layers stay on internals until it does.
   Status: **open**, and now split by shape: its INPUT half is step 8
   (`Ctx::observe_read`), its OUTPUT half is the content-address cutoff,
   which step 9 may make unnecessary at the root.
2. **Error boundaries.** `Guarded` placement and the substitution tally are
   caller assembly (`libpipeline-internals/src/boundary.rs`). Status:
   **open**; the tally would surface as an accessor on `Pipeline`.
3. **Non-linear graphs.** The builder builds chains; a diamond exists in the
   engine via `Arc<S>: Stage`
   (`libpipeline-internals/src/stage.rs:119`) but has no builder spelling.
   Status: **open**, orthogonal. One constraint from position identity: a
   node shared between two consumers keeps the single identity it was
   registered with, which the `Arc<S>` impl's forwarding of `id` already
   does.
4. **Scheduling.** `Ledger::schedule`
   (`libpipeline-internals/src/schedule.rs:95`) has no builder-level door;
   rides on finding 1. Status: **open**, likely to close by dissolution - a
   chain's schedule is the chain, and the module's own headline case is a
   diamond.
5. **Store lifecycle.** Status: **closed** (step 3) - one store at the
   builder, living exactly as long as the pipeline.
6. **A watched single poll.** Status: **closed by deletion** (the door flip)
   - there is no public watched door and there will not be one. The
   MECHANISM is still owed as an internal check: that is step 6, which is a
   different thing from this finding and must not be read as reopening it.
7. **The registration-site guarantee protects only what is registered.**
   Status: **closed by dissolution** (step 2).
8. **Assembling a pipeline takes two manifest edges.** Status: **open**, and
   the door flip sharpened it. Before the flip a consumer took the second
   edge to implement `Stage`; now it takes the second edge to spell a key
   function's TYPES, which is a smaller thing to re-export. Bound to the
   `libpipelinedata` fate decision.
9. **`take_stale` and the gate are two readers of one clear-on-read flag.**
   Status: **closed by deletion** (the door flip) - `take_stale` is off the
   public API, `FrameDriver::take_stale`
   (`libpipeline-internals/src/driver.rs:138`) is internal, and
   `Pipeline::poll` is its only reader. `Pipeline::waker`'s doc
   (`src/builder.rs:631`) states the absence of an accessor as the design.

## The remaining steps

Each step names what it touches, its gate, its halt condition and its
dependencies. A halt condition is a discovery that makes the step bigger than
it was scoped as: stop, record, and ask, rather than widening the step.

**Renumbered at this revision.** Step 6 keeps its number - it is cited from
`tests/tests.rs:13` and `an_unwakeable_poll_is_visible_offline.rs:32` - and 7
onward were rewritten after the design session of 2026-08-25, which dissolved
the old 7 and 8 (`Ctx` growing a read log and in-flight state) and promoted the
old 10 to the centre. See "What the session settled" below.

6 is independent. 7 is the spine and 8 follows it. 9 is independent of both. 10
needs 9. 11 is independent. 12 needs the API to have stopped moving, so it is
last before the documents.

### What the session settled

The invalidation ladder, cheapest rung first, each tried only if the one above
does not answer:

| # | condition | cost | what happens |
|---|---|---|---|
| 1 | upstream answered `Unchanged` | nothing | the input is the same `Arc`; return the slot without looking at a key |
| 2 | no upstream signal, stage HAS a `key_fn` | one key | compute, compare to the slot's; equal -> return the slot, never entering the stage |
| 3 | no `key_fn`, or the key moved | the work | call the compute function, which may still answer `Unchanged` from its own walk |

Rulings that follow from it, each now a step or a deletion:

* **`key_fn` is optional per stage, and its presence is ONE cost question**: is
  computing the key cheaper than doing the work? This repo's one measurement -
  `PIPELINE_IMPLEMENTATION.md:177`, hashing cost 15x the work it avoided - says
  the default answer is "just do it". A `key_fn` is the exception, added on
  evidence.
* **A `key_fn` is NOT for catching an upstream that answers `Computed` with a
  value equal to last time's.** Tim: *"that's a defect in the computation, are
  we adding special handling for it?"* That is a bug in that stage, fixed
  there.
* **`K` is `Copy + Eq`, passed to `new`, defaulting to `u128`.** Not `Hash`:
  the store stops being a map.
* **`ReadSetMemo` retires** - see "Outside these repos" below. Both its
  consumers are recorded as not paying, and the key function plus the enum
  cover everything it did.
* **`Ctx` does NOT grow a read log or in-flight state.** The read log dies with
  the read-set memo; in-flight state was justified by a `Pending` no production
  stage answers. It gains an ENVIRONMENT instead (step 11).

### Step 6 - Delayed keeps its promise

Reassessed and it stands, in the shape it was written. What the flip closed
was finding 6 (a public watched door) and the way the property is
DEMONSTRATED: `a_stage_that_forgets_its_waker_makes_its_value_lost_rather_than_late`
(`tests/builder_is_the_only_door.rs:412`) now measures the loss in the
OUTCOME, and asserts that the caller holds a stale value forever with nothing
reporting it. That is a demonstration of the defect, not a check against it -
and the flip made a check MORE owed, not less: with `take_stale` off the API
a caller has no way to detect a forgotten waker at all.

Touches: `src/builder.rs` (`Pipeline::poll`, the `Pending` arm) and one new
facade test. In `poll`, under `#[cfg(debug_assertions)]`, poll through
`libpipeline_internals::watch::poll_watched`
(`libpipeline-internals/src/watch.rs:83`) with `self.frame.waker()` as the
forwarded waker, and panic on `WakePath::Missing`
(`libpipeline-internals/src/watch.rs:40`) with the lost-not-late diagnosis.
`Box<T>: Stage` (`libpipeline-internals/src/stage.rs:147`) already makes the
boxed graph a legal argument. Release builds poll plain.

Add a facade test (`#[cfg(debug_assertions)]`, `#[should_panic]`) driving a
stage that forgets its waker - `forgetful_poll`
(`tests/builder_is_the_only_door.rs:362`) is that stage already.

*Depends on*: nothing.
*Gate*: facade 31 tests + 6 doctests, green in debug AND under `--release`;
internals 77 unchanged. The existing loss-demonstration test must keep
passing in release and must be re-sited or re-spelled if debug now panics on
it - decide which, do not delete it.
*Halt if*: the probe's forwarded waker changes an existing test's answer.
`poll_watched`'s contract is that watching cannot turn a working graph into a
stalled one; if it does here, that is a defect in the probe, not a reason to
skip the check.
*Open decision*: a wake-debt accessor instead of the debug panic (D4 below).
The shape of the check is the same either way, so the step does not wait on
it.

### Step 7 - the ladder: a stage answers `Unchanged` or `Computed` - LANDED

**Landed as described, with the variant in the `Ready` channel** (option 2
done differently from how it was costed - see "Where the variant went",
below) and with one mechanism the plan did not anticipate (see "What the
join owes"). The rest of this section is kept as written, because the next
steps cite it.

**The spine of everything else.** Early cutoff is OBSERVED rather than
reconstructed: `Expansion::untouched`
(`crates/highbay_data/src/elements.rs:239`) is documented as "what a pass
answers for a tree that declared none of its element, which is most trees", and
both paths return the same `Expansion`. `Backdated`
(`libpipeline-internals/src/track.rs:701`) pays a traversal of the output per
node per poll to learn the same fact afterwards.

`Unchanged` carries NOTHING - the value is in the slot. `Computed` carries the
new `Arc`. That is what makes rung 1 free.

**It does not contradict `EffectPoll`'s standing ruling.**
`deps/libeffects/src/poll.rs:22` rules out a variant for STALENESS - *"making
staleness a variant would require something to be polled in order to learn it
should be polled"* - because a wake is out of band by nature. `Unchanged` is an
ANSWER, which presupposes having been polled.

**Where the variant goes.** Three spellings, costed in the previous revision of
this plan: a fourth `EffectPoll` variant (crosses into `deps/libeffects`, a
third repo); an engine-side enum (breaks the compiler-checked "the stage
contract IS the effect protocol" claim); or a flag on `Ctx` read by
`StageFn::poll_stage`. The third was recommended for crossing no repo boundary.
**Re-examine that recommendation before building**: it was made when
`Unchanged` was a peripheral optimisation, and it is now the central mechanism.
A flag on `Ctx` expresses "the stage's answer" as a side channel, which is the
shape this design keeps removing.

**A cold slot has nothing to return.** A stage may answer `Unchanged` only if
it has answered before at this position. That holds inductively - back to a
first stage with no upstream, which must answer `Computed` on a cold pipeline -
so it is an invariant to ASSERT, not a type to encode: the engine checks "slot
empty and stage answered `Unchanged`" and panics with the position.

**`Unchanged` is a claim the engine cannot check.** A stage that answers it
wrongly makes the pipeline serve a stale `Arc` forever, invisibly - the same
class as a forgotten waker. Where a stage has a `key_fn`, the engine CAN catch
it: compare the new key against the slot's when the stage answers `Computed`
and complain in debug if they are equal. A detector, not a compensator.

**Where the variant went, and why not the recommended spelling.** Inside
`EffectPoll::Ready`, as `libpipelinedata::StageAnswer<T>`. The `Ctx` flag was
re-examined and is not merely inelegant - it CANNOT EXPRESS THIS SHAPE. The
poll function's return type demands a value on the `Ready` path, and a stage
answering `Unchanged` has none: the value is in a slot the stage cannot reach.
The flag was costed while `Unchanged` still carried the value; with the value
gone there is nothing for a flag to accompany. The engine-side enum was costed
as one BESIDE `EffectPoll`, which is what would break the compiler-checked
"the stage contract IS the effect protocol" claim. Nested inside `Ready` it
breaks nothing: `BoundStage` still implements `Effect`, over
`StageAnswer<Arc<S::Output>>`. `deps/libeffects` is untouched.

**What the join owes - a mechanism this plan did not have.** "Upstream answered
`Unchanged`, so the downstream is never entered" is UNSOUND on its own. A
downstream that answered `Pending` last poll has produced nothing, so there is
no answer for the chain to stand on: skipping it would answer `Unchanged` at a
caller holding nothing, for ever, with the landed value never delivered - the
"lost rather than late" class, arriving through the new variant. So `Chain`
holds one piece of state, `Joint`: what it last handed on, and whether the
second half settled over it. `Unchanged` skips only a settled join; an owing
one is re-polled over what it was handed. `Failed` does not settle either, since
a failure retries. This is reachable for any stage whose key function answers
`None` - which step 10 makes the DEFAULT - so it is not a corner.

*Depends on*: nothing.
*Gate*: a two-stage pipeline where the second is never polled because the first
answered `Unchanged`, asserted by a poll counter the second stage increments.
Plus the cold-slot panic, asserted by message.
**Met** by `tests/a_stage_that_rewrote_nothing_says_so.rs`, 5 tests: the poll
counter, the `.uncached()` control (the slot is not the store), the cold-slot
panic at position 0 and at position 1, and the owing join. Mutation-checked
three ways - the skip removed, the assertion removed, the owing state removed -
each failing exactly the tests that name it and nothing else.
*Halt if*: the variant cannot be expressed without editing `deps/libeffects`.
That is a third repo and a gitlink; stop and ask. **Not reached.**

### Step 8 - the store becomes one slot per position

`MemoMap` is `Mutex<HashMap<MemoKey, Arc<V>>>`
(`libpipelinedata/src/store.rs:142`) whose own `len()` doc admits *"the growth
this type does not bound"*. Positions are minted one per registration, so the
store is a `Vec` indexed by `StageId::index()`, each slot
`Option<(Option<K>, Arc<Row>)>` - **the last `Arc` this position answered**,
which is what rung 1 returns. The `K` beside it is rung 2 reaching the same
slot, and is `None` for a stage with no `key_fn`.

Bounded by construction, O(1), and it needs exactly `Copy + Eq`.

**What it costs, stated**: a stage polled with alternating inputs misses every
time where a map would hit. A pipeline stage has no dimension to key on - one
position, one chain, one input per poll - so one slot is right here. The
dimensioned store is the domain's, and
`crates/highbay_data/src/elements.rs:670` already draws the line: *"The node is
a DIMENSION of the store, not an input to the hash."*

**This is where D2 gets answered.** `EcsMemoStore` is verified at zero
consumers and `ecs` is enabled by no manifest. If the store is a `Vec` indexed
by position, `MemoStore`/`MemoMap`/`NoMemo` go with it, along with `.store()`,
`BuilderStore::Given` and `Erased`. Read D2's cost list before deleting: four
independent-implementation demonstrations go too.

*Depends on*: step 7, which built the slot where it could live today - one
`Option<Arc<S::Output>>` per `Memo`, filled by every answer that position gives
including a store hit. This step moves it into the store and puts `K` beside
it; nothing else about it changes.
*Gate*: a stage's second poll at an unchanged input does not enter it; the
store's memory does not grow across N polls of the same pipeline.
*Halt if*: two pipelines must share a store. That is the dimension this step
removes and it invalidates the shape.

### Step 9 - `K` at the builder: `Copy + Eq`, passed to `new`, default `u128`

Tim: *"it can be passed as K to `PipelineBuilder::new` (or `new_with_key`)"*,
*"it only needs to be `Copy + Eq` which `struct(u128)` is"*, *"K can default to
u128 itself if not provided"*.

The key closure returns `Option<K>`, not `Option<MemoKey>`. **The engine folds
in the position**, because only the builder mints positions - so `Ctx::key`
(`src/builder.rs:244`) and a public `MemoKey::new` both leave the surface, and
`MemoKey` becomes internal or stops existing.

`PipelineBuilder::<ContentKey>::new()` is the spelling. One Rust detail to know
rather than discover: a default type parameter does NOT drive inference in
expression position on stable (that is `default_type_parameter_fallback`,
unstable), so a bare `new()` resolves `K` only if a later registration's key
closure constrains it - which it usually does, the return type being concrete
under the `fn` door. The `K = u128` default earns its place when WRITING the
type, not when calling `new`.

**`Eq`, not `PartialEq`.** A key whose equality is reflexively false (a float,
NaN) makes the memo silently never hit, and no test sees it because answers do
not change - only speed does.

**The engine still never hashes.** The author's function produces the identity;
the engine compares it.

*Depends on*: nothing; blocks 10.
*Gate*: a readable that panics on deref survives a poll at an unchanged key.
*Halt if*: `K` needs a bound beyond `Copy + Eq` to reach the store. That would
mean step 8's shape is wrong.

### Step 10 - `key_fn` becomes optional per stage

```
.stage_fn(name, poll)               // no key: always entered, answers for itself
.stage_by_key(name, extract, poll)  // keyed by what `extract` says
```

Internally one `Option` field on `StageFn` (`src/builder.rs:316`). `None` is
rung 3; `Some(f)` is rung 2.

**The `None` case is now the DEFAULT and the common one**, which inverts how
the previous revision scoped this step. It framed `None` as "keyed by the
version" - a stage with no key still had one. Under the ladder a stage with no
`key_fn` simply has no rung 2, is always entered, and answers for itself. That
is cheaper and it is what the 15x measurement points at.

*Depends on*: step 9.
*Gate*: a keyed stage hits when the rest of the input moved; its unkeyed
sibling in the same pipeline is entered on every poll and answers `Unchanged`.
*Halt if*: the `Option` forces `K` to be inferred where there is nothing to
infer it from. Step 9's default is the intended answer; if it is not enough,
stop.

### Step 11 - `Ctx` carries the environment

**Not the read log, and not in-flight state.** Both justifications are gone:
the read log dies with the read-set memo, and in-flight state was for a
`Pending` all three production stages document as never answered.

What a stage genuinely needs from outside is an ENVIRONMENT - the world it
reads through and mints into. It cannot ride in `I`/`O`: a later stage's input
is an earlier stage's output, outputs are held as `Arc<O>` in the slot, and the
environment is borrowed, so it cannot live in an `Arc` that outlives the frame.
Threading it would also put it under `K`, invalidating stages that never read
it.

```rust
pub struct Ctx<'a, E> { id: StageId, waker: &'a Waker, env: &'a E }
poll: fn(&I, &Ctx<'_, E>) -> EffectPoll<O, Err>
```

`E` defaults to `()`. **Uniform across the pipeline**: a stage needing less
narrows it INSIDE the poll function, not in the type. Two stages needing
disjoint environments means `E` is the consumer's product type. Per-stage `E`
at the type level would force the builder to carry a heterogeneous tuple and
buys nothing a product does not.

**`E` must not enter the key** - it is a destination, not an input. Sound only
while `E` is stable for the pipeline's life (one pipeline per world), which is
also what step 8's single slot rests on. See D5.

The evidence this is the real shape: `highbay_data::elements::Ctx`
(`crates/highbay_data/src/elements.rs:468`) is `{ node, reqs, memo }`. `memo`
dies with the read-set memo; `reqs` is the environment; `node` is the
per-reduction address, which varies WITHIN one poll as the walk descends and
never reaches the pipeline.

*Depends on*: nothing.
*Gate*: a stage reads a value from `env` that no argument carries, and a
pipeline built with `E = ()` still compiles with the door unspelled.
*Halt if*: two stages need environments with no common product. That is
per-stage `E` and it is a different step.

### Step 12 - the consumer conversion, and the gitlink bump

**Re-scoped, and larger than "convert three `Stage` impls".** Tim's ruling is
that an element crate authors COMPONENTS, not stages, so
`crates/highbay_elements` should name neither `libpipeline` nor
`libpipelinedata`; its stage wrappers and its test doubles belong to whoever
assembles. Nothing in the workspace assembles today - the only assembly sites
are `crates/highbay_data/tests/assemble_under_the_driver.rs:205,207`, in a
test - so the conversion must also decide WHERE a registration lives.

The inventory, verified:

* `impl Stage` in `src/`: **three** - `ExpandStage`
  (`crates/highbay_elements/src/pipeline.rs:265`), `ExpandDefinitionStage`
  (`:399`), `AssembleStage` (`crates/highbay_data/src/pipeline.rs:192`).
* `impl Stage` in tests: **four** - `Counted` and `CountedDefinition`, once
  each in `crates/highbay_elements/tests/expand_stage_memo.rs:146,402` and
  `crates/highbay_elements/tests/expand_stage_corpus.rs:104,195`.
* `impl MemoStore` doubles: **four impls, two names** - `TreeMemo` and
  `DefinitionMemo`, in `expand_stage_memo.rs:77,372` and
  `expand_stage_corpus.rs:80,171`. All four count lookups.
* Gated test files and their counts: `expand_stage_memo.rs` 14 tests
  (`#![cfg(feature = "pipeline")]`, `:24`), `expand_stage_corpus.rs` 8
  (`#![cfg(all(feature = "pipeline", feature = "tsx"))]`, `:28`),
  `assemble_under_the_driver.rs` 10
  (`#![cfg(all(feature = "tsx", feature = "pipeline"))]`, `:49`), plus
  `hash_element_is_the_serde_encoding.rs` (`:30`) and
  `the_ir_this_door_can_hash.rs` (`:31`), which are gated on `pipeline` but
  name only `ContentKey`. `crates/highbay_elements/src/pipeline.rs` also
  carries 5 unit tests.

**The claim that only `--features pipeline` breaks is FALSE at this
revision.** Measured with `cargo check --workspace --keep-going` on
2026-08-25, against the subrepo working trees (which are already at the tips
the bump would record): the DEFAULT build fails in two crates.

* `crates/libhbui/src/charter.rs:254` -
  `const EXPANSION: StageId = StageId::new("libhbui::expand_lists", 2);`
* `crates/highbay_data/src/elements.rs:607` -
  `pub const REDUCE: StageId = StageId::new("highbay_data::elements::reduce", 1);`

Both are library code, neither is feature-gated, and `StageId::new` left with
step 2. Everything downstream (`highbay_elements`, `highbay_ui`, the root
binary) fails as a consequence. A third site,
`crates/libhbdata/src/memo.rs:481`, is test-only and fails the same way under
`--tests`.

Neither constant is a builder-minted position: both are the stage half of a
DOMAIN memo key, outside any pipeline. That is the relocation's territory,
which makes the relocation a **prerequisite** for the bump rather than a
parallel change. The cheap alternative - re-spelling them as `StageId::at(N)`
with hand-picked numbers - is an invented position and is exactly the
self-declared identity step 2 removed; if it is taken as a stopgap, it must
be recorded as one.

Making `highbay_elements` name neither crate additionally requires
`crates/highbay_elements/src/host.rs:39`
(`use libpipelinedata::ContentKey;`, ungated, feeding
`Requirements::read_key`) and the manifest edge at
`crates/highbay_elements/Cargo.toml:122` to go, both of which are the
relocation again.

Touches: `crates/highbay_elements/src/pipeline.rs` (823 lines),
`crates/highbay_data/src/pipeline.rs` (250), the three gated test files
(2078 lines), the two manifests, and `deps/libpipeline` +
`deps/libpipelinedata` gitlinks in the outer repo.

**The gitlinks move in the same commit as the conversion** (Tim: "move the
gitlink when we do that commit, we'll worry about merging later"), pointing
at the `highbay-clean` tips: `deps/libpipeline` from `867fbcb` to `790863b`
(18 commits), `deps/libpipelinedata` from `ffe4a1b` to `db3c1eb` (3
commits). `deps/libeffects` does not move (`d5c5a55`, matching).

*Depends on*: steps 9-11 landing first, or the conversion is done twice - the
key function is what a converted stage mostly IS, and step 9 changes what a
key function says. And on the relocation for the default build, which is
outside these repos.
*Gate*: `cargo check --workspace` clean; `cargo test -p highbay_elements
--features pipeline`, `-p highbay_data --features pipeline,tsx` green at 32
gated tests plus 5 unit tests;
`grep -rn "libpipeline" crates/highbay_elements/` finds nothing outside
comments; the gitlink bump and the conversion in one commit.
*Halt if*: the assembly has no home. If no crate is the right place for a
registration, that is a finding about the workspace's shape and belongs to
whoever owns it, not to this plan.

### Step 13 - the documents catch up

As each step lands, strike or update the matching passages in "Where the
crate stands" and the findings' status lines - part of each step's review,
listed once so it is nobody's afterthought. When the last step lands, retire
this file; whatever is still open moves with the work that takes it up.

Also owed and not yet anybody's: `cargo doc --no-deps -p libpipeline` clean
(2 real warnings today), and the ASCII rule applied to
`libpipelinedata/Cargo.toml`, which uses em-dashes in comments.

*Depends on*: everything.
*Gate*: rustdoc clean in all three crates; `grep -rn "PLAN.md"` over both
source trees resolves every cited section name and number.

## Decision points, carried rather than sequenced

Each is Tim's, and each is written with what the options cost rather than
with a recommendation dressed as a step.

**D1 - `libpipelinedata`'s fate.** The residue report stands: nothing
consumer-facing remains that `libpipeline` could not serve by re-exporting
three types. What is in the crate today, by fate:

* **Relocating, not dying**: `ContentHash`, `ContentHasher`,
  `ContentAddressHasher`, `Fnv1a128`, `ContentKey::of` (`src/hash.rs:313`),
  the `serde_hash` door and `libpipelinedata-macros`' derive. The domain
  needs them for its own early exit; see the relocation section.
* **Engine-internal if the seam goes**: `StageId`, `MemoKey`, and
  `ContentKey` as an opaque 128-bit operand (`ContentKey::from_u128`,
  `src/key.rs:82`, survives the hashing leaving).
* **The `libeffects` re-export and `EffectPoll`** - the one thing a key or
  poll function still reaches for through this crate. Could equally be a
  direct `libeffects` edge or a facade re-export; that choice is finding 8.

Folding it into `libpipeline` costs: the `the_port_without_the_engine.rs`
suite (3 tests) loses its subject, the `ecs` and `serde` features move or
die, and `tests/engine_stays_generic.rs`'s `THE_STACK` (`:44`) shrinks by
two names. Keeping it standalone costs: it still owes the treatment
`libpipeline` got at `7417fb9` - **40 citations of `PIPELINE_PLAN.md` across
22 files**, a document that lives in the OUTER repo root and not in this
subrepo, so every one of them points outside its own crate.

**D2 - the `MemoStore` seam. ANSWERED BY STEP 8** - if the store is a `Vec`
indexed by position, the seam has nothing left to swap. Read the cost list
below before deleting, then delete.

Original framing: `EcsMemoStore` has zero consumers anywhere -
verified across `crates/` and both subrepos; the `ecs` feature is enabled by
no manifest in the workspace. The only external implementations are the four
test doubles of step 12, which count lookups. So the trait abstracts over one
real backend (`MemoMap`) for the benefit of mocks. Removing it costs:
`.store()` (`src/builder.rs:399`) and `BuilderStore::Given`
(`src/builder.rs:99`) go, `Erased` (`src/builder.rs:148`) collapses into a
direct `MemoMap` view, and four demonstrations of an independent
implementation lose their subject - `README.md:404` (a doctest),
`tests/one_door_two_patterns.rs:143`,
`libpipeline-internals/tests/invalidation_marks_dependents.rs:273`,
`libpipeline-internals/tests/a_boundary_is_not_a_cacheable_answer.rs:210`.
What the doubles need is "count the lookups", which a `MemoMap` with a
counter beside it serves without a trait.

**D3 - `stage_by_key`'s return type. ANSWERED**: `K`, `Copy + Eq`, passed to
`new`, defaulting to `u128` (step 9). Tim ruled it directly. The original
framing, kept because it names the cost that ruling accepts:

Written into the old step 11 with a
recommendation, because the step cannot be written without an answer. If the
generic `K` is chosen instead, step 11 grows a second erasure and its gate
grows a test that two stages with different `K` share one store.

**D5 - is one pipeline per world true?** Step 11's environment-not-in-the-key
rule and step 8's single slot per position BOTH rest on it. If two worlds can
share one pipeline instance, both need a dimension back, and both steps are
shaped wrong. Cheap to check now, expensive to discover after. **This is the
one to answer before building.**

**D4 - the debug panic or a wake-debt accessor.** Step 6's open decision, as
`DESIGN.md`'s "Delayed keeps its promise" records it. The check is the same
either way; only who is told differs. An accessor reintroduces the
clear-on-read hazard finding 9 closed unless it is cumulative rather than
clearing.

## Outside these repos: the read-set memo retires, and the build is broken

**The outer workspace does not build.** Verified with `cargo check -p libhbui`:

```
error[E0599]: no associated function named `new` found for struct `StageId`
  --> crates/libhbui/src/charter.rs:254
254 | const EXPANSION: StageId = StageId::new("libhbui::expand_lists", 2);
```

Same at `crates/highbay_data/src/elements.rs:607`. `StageId` became positional
when identity became a position; both sites are DOMAIN memo keys that were
never using the pipeline's notion. Each const feeds exactly ONE call site -
`charter.rs:727` and `elements.rs:834` - and both are read-set memos.

**`ReadSetMemo` is superseded.** `PIPELINE_IMPLEMENTATION.md:168` records five
memo boundaries. The three that pay are boundary keys. The two that do not are
its only two consumers:

| # | boundary | pays? |
|---|---|---|
| 4 | `ReduceMemo` under `Expansion::then` | **NO - measured 15x slower** |
| 5 | `libhbui::expand_lists` | *"the per-frame hash IS the polling cost this plan removes"* |

It has no remaining unique capability. Deciding BEFORE the stage is entered is
the key function (rung 2); deciding after, from the walk, is `Unchanged` (rung
3). Its last distinguishing feature - folding AMBIENT reads rather than only
the input - is something `Ctx` can hand a key function directly, including the
prediction trick: hand it the last run's read set for that position and it
folds those names' current keys into `K`. That is `ReadSetMemo` expressed once,
in the engine, instead of duplicated at the domain layer with its own stage
identity.

`ReadLog::observe` survives as an observation point; the memo type does not.

**Ordering.** #4 can go now - a straight win, dependent on nothing, with its
own `addressings` measurement to check the result against. #5's entire benefit
is a per-frame hash that push-not-pull removes, so deleting it before the waker
gates the frame means re-expanding every frame; give it a local identity in
`libhbdata` to unbreak the build and leave the memo standing.

**`ContentKey` does NOT move.** An earlier revision scoped a relocation to
`crates/libhbdata` as a prerequisite of step 12. Three narrowings later there
is nothing left in it for the pipeline: the engine never hashes, `K` is the
consumer's type, and `expand`'s early exit is structural rather than addressed.
What remains is that the data layer content-addresses in its own right - the
derives at `crates/libhbui/src/{id.rs:444, scope.rs:246, site.rs:74,
app.rs:190}` and `ecs.rs:875` - which is a `crates/` concern with its own
timing and no bearing on this crate.

## Found stale at this revision

Recorded because a wrong citation is a defect, not a typo. None of these were
fixed here - this revision changed no code.

1. **`Expansion::untouched` was cited at the wrong path.** The previous
   PLAN.md put it in `crates/highbay_elements/src/component.rs`; it is
   `crates/highbay_data/src/elements.rs:239`. `Component::expand` IS at
   `crates/highbay_elements/src/component.rs:215`, which is probably how the
   two merged.
2. **The trybuild blocker does not exist.** The door flip's finding 2 says a
   compile-fail test for the capturing closure cannot be written because
   `tests/engine_stays_generic.rs` would reject the dependency; `trybuild` is
   already in `PERMITTED_REGISTRY` (`tests/engine_stays_generic.rs:54`, added
   at `7417fb9`). `tests/builder_is_the_only_door.rs:134` repeats the false
   claim in a comment. The test is writable today; only the fixture is
   missing.
3. **Three source comments still call the door `run`.** It is `poll`:
   `libpipeline-internals/tests/tests.rs:5` and `:14`,
   `libpipeline-internals/tests/an_unwakeable_poll_is_visible_offline.rs:24`.
4. **Two real rustdoc warnings.** `src/builder.rs:536` links to
   `Pipeline::run`, which no longer exists (unresolved intra-doc link);
   `src/builder.rs:273` links public `Failure` documentation to the private
   `StageFn`.
5. **The previous plan's step 7 was already done.** It instructed removing a
   document-level proposed-section marker from `DESIGN.md`; `DESIGN.md` has
   carried no such marker since `02af6ae`.
6. **The previous plan's citation-debt list overstated what is cited.** Of
   the seven section names it listed, only two are cited from source today -
   "Two drivers, one graph" and "Not built yet". "Migration plan", "What else
   stays public", "The ledger test, measured" and "Where a consumer works"
   are cited from nowhere; the files that cited them were rewritten by later
   steps. Only the two above are protected here.
7. **`libpipelinedata/Cargo.toml`'s without-the-engine claim names a file
   that does not exist.** It cites `tests/stage_without_engine.rs`; the file
   is `tests/the_port_without_the_engine.rs`. The claim it makes is also the
   one D1 says has no consumer left.
