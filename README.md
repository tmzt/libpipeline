# libpipeline

An incremental pipeline engine. You register pure, poll-driven stages with a
builder; the engine memoizes every stage under keys computed before the work,
chains the stages into one graph, and runs it through one door - `poll(version,
&input)`, which polls once and returns immediately, whatever the answer.
Blocking to completion (a build tool) and one poll per frame (an interactive
host) are things a CALLER does with that one call: the same stages, the same
cache, either caller.

**A stage is two functions, and the builder takes them as `fn` pointers.** A
key function that says what this input is, and a poll function that produces
the output. Neither may capture - a non-capturing closure coerces to `fn` and a
capturing one does not compile - so a stage cannot carry hidden state that
moves its output without moving its key. Everything the engine gives a stage
arrives through one argument, `Ctx`.

`libpipeline` is the engine half of a pair. Its companion `libpipelinedata`
holds the shared vocabulary the two sides speak: the key types, the content
hash, and the `MemoStore` seam. The examples below use both.

Every example on this page is a doctest: `cargo test` compiles and runs them.

## The smallest working pipeline

A stage is a `(key, poll)` pair. The key is computable from the input alone -
before the stage runs - which is what lets a lookup precede the work. The
builder is the only way to compose, memoize, or drive; you assemble nothing by
hand.

```rust
use libpipeline::{Ctx, PipelineBuilder, Run};
use libpipelinedata::{ContentKey, EffectPoll, MemoKey};

/// What "doc.title" IS, as a key - computable without splitting anything.
///
/// `Ctx::key` supplies the identity half: a key is `(stage id, input content
/// keys)`, and the id is the half a stage has no business choosing.
fn split_key(input: &String, ctx: &Ctx<'_>) -> Option<MemoKey> {
    Some(ctx.key([ContentKey::of(input)]))
}

/// Splits a dotted path like "doc.title" into its segments.
fn split(input: &String, _ctx: &Ctx<'_>) -> EffectPoll<Vec<String>, &'static str> {
    if input.is_empty() {
        return EffectPoll::Failed("nothing to split");
    }
    EffectPoll::Ready(input.split('.').map(str::to_string).collect())
}

let pipeline = PipelineBuilder::new()
    .stage_fn("split", split_key, split)
    .build();

// `1` is the run VERSION: which state the input is. Where it comes from is
// yours - an edit store's cursor, a build number, a git sha.
let Ok(Run::Computed(segments)) = pipeline.poll(1, &"doc.title".to_string()) else {
    panic!("a pure stage answers on the first poll");
};
assert_eq!(*segments, vec!["doc".to_string(), "title".to_string()]);
```

`Run::Computed` hands back an `Arc` of the output, because the memo still
holds the value it just answered with - that is what a memo is. It also means
a large output costs a refcount bump per cache hit and nothing else, without
any stage author remembering to wrap anything: the engine wraps once, where the
value enters the graph.

Three things to notice at the registration call site:

* **The builder mints the identity, and the identity is a position.** The
  `StageId` inside the `Ctx` is this registration's index and nothing else; a
  stage never declares one, so there is no second id it could answer with and
  nothing to check. `"split"` beside it is a diagnostic label: it enters no
  key, nothing is looked up or compared by it, and a second stage may carry the
  same one with no consequence.
* **The functions are `fn` pointers, not closures the builder boxes.** Free
  functions like these coerce, and so does a non-capturing closure written
  inline. A closure that captured a counter, a handle or a config would not,
  and that refusal is the design: a captured field is an input that moves the
  output without moving the key, and no review catches every one.
* There is no separate "memoize" step. **Registering a stage memoizes it**;
  there is no un-memoized registration to reach for.

## Adding stages

Chain another `.stage_fn(..)` call; the new stage's input must be the previous
stage's output - the value, not the share the graph carries it in; unwrapping
between stages is the engine's job too - and its error is the pipeline's one
error type, which every stage of one pipeline shares. A failure comes back
carrying the POSITION of the stage that raised it.

```rust
use libpipeline::{Ctx, PipelineBuilder, Run};
use libpipelinedata::{ContentKey, EffectPoll, MemoKey};

# fn split_key(input: &String, ctx: &Ctx<'_>) -> Option<MemoKey> {
#     Some(ctx.key([ContentKey::of(input)]))
# }
# fn split(input: &String, _ctx: &Ctx<'_>) -> EffectPoll<Vec<String>, &'static str> {
#     if input.is_empty() {
#         return EffectPoll::Failed("nothing to split");
#     }
#     EffectPoll::Ready(input.split('.').map(str::to_string).collect())
# }
/// Counts the segments the first stage produced.
fn count_key(input: &Vec<String>, ctx: &Ctx<'_>) -> Option<MemoKey> {
    Some(ctx.key(input.iter().map(ContentKey::of)))
}

fn count(input: &Vec<String>, _ctx: &Ctx<'_>) -> EffectPoll<usize, &'static str> {
    EffectPoll::Ready(input.len())
}

let pipeline = PipelineBuilder::new()
    .stage_fn("split", split_key, split)
    .stage_fn("count", count_key, count)
    .build();

let Ok(Run::Computed(count)) = pipeline.poll(1, &"doc.section.title".to_string()) else {
    panic!("both stages are pure");
};
assert_eq!(*count, 3);

// A failure names the stage that raised it, as a position: stage 0 here,
// because `split` failed and `count` never ran. One `at()` call answers that
// at any length of chain.
let Err(failure) = pipeline.poll(2, &String::new()) else {
    panic!("an empty source cannot be split");
};
assert_eq!(failure.at(), 0);
assert_eq!(*failure.error(), "nothing to split");
```

(The `#`-prefixed lines in this and later examples are hidden doctest lines
repeating a definition from an earlier example.)

## What memoization does for you

Every registered stage is looked up before it is polled: the key is
`(stage id, content keys of the inputs)`, so it costs no run to compute.
An unchanged input is a cache hit that never enters the stage at all.

The versions below MOVE while the content stays the same, which is how a poll
reaches the graph at all: the version gate above it answers first when the
version repeats (the next section is about that), so a test of the memo has to
hand the pipeline a state it has not computed for yet.

**Note where the run counter lives**, because it is the one thing a `fn` door
changes about writing a stage: a stage cannot hold a field, so anything ambient
it needs - a counter here, a font atlas or a module runtime in earnest - is
reached through a `static`. `Ctx` is where such a route belongs and does not
carry one yet; `DESIGN.md`'s "The intended stage shape" is the record of that.

```rust
use std::sync::atomic::{AtomicUsize, Ordering};

use libpipeline::{Ctx, PipelineBuilder, Run};
use libpipelinedata::{ContentKey, EffectPoll, MemoKey};

static LEN_RUNS: AtomicUsize = AtomicUsize::new(0);

fn len_key(input: &String, ctx: &Ctx<'_>) -> Option<MemoKey> {
    Some(ctx.key([ContentKey::of(input)]))
}

/// Measures a string, counting how many times it actually ran.
fn len(input: &String, _ctx: &Ctx<'_>) -> EffectPoll<usize, &'static str> {
    LEN_RUNS.fetch_add(1, Ordering::Relaxed);
    EffectPoll::Ready(input.len())
}

let pipeline = PipelineBuilder::new()
    .stage_fn("len", len_key, len)
    .build();

assert_eq!(pipeline.poll(1, &"abcd".to_string()), Ok(Run::Computed(Arc::new(4))));
assert_eq!(pipeline.poll(2, &"abcd".to_string()), Ok(Run::Computed(Arc::new(4))));
assert_eq!(LEN_RUNS.load(Ordering::Relaxed), 1); // the repeat was served by the lookup

// `.uncached()` turns the store off: the control run. Answers must not
// change, only speed - a pipeline whose answers change when the cache is
// disabled has a bug the cache was hiding.
let control = PipelineBuilder::new()
    .uncached()
    .stage_fn("len", len_key, len)
    .build();

assert_eq!(control.poll(1, &"abcd".to_string()), Ok(Run::Computed(Arc::new(4))));
assert_eq!(control.poll(2, &"abcd".to_string()), Ok(Run::Computed(Arc::new(4))));
assert_eq!(LEN_RUNS.load(Ordering::Relaxed), 3); // the control ran every time
# use std::sync::Arc;
```

**Opting out.** A stage whose answer is not a cacheable fact - an effect, a
read of something no key can address - answers `None` from its key function. It
is then neither looked up nor recorded, and everything else about it is
unchanged. Failures are never cached regardless: a transient failure served
back from a memo would outlive its cause.

## One door, and what it answers

`poll(version, &readable)` polls once and returns immediately, whatever the
answer. Nothing inside it waits, ever. There are four answers:

* **`Run::Computed(value)`** - work happened; take the new value. The pipeline
  records the version it answered for.
* **`Run::Unchanged`** - the value you already hold derives from exactly this
  state; keep it. Read it as *the value is finished*: not a report that nothing
  happened, but a statement that nothing needs to. The readable is not
  dereferenced, no memo key is computed and no stage is polled.
* **`Run::Delayed`** - not ready; a wake is coming. The poll arranged for the
  pipeline's waker to be woken when the answer becomes possible, so wait to be
  woken rather than re-polling in a spin.
* **`Err(Failure)`** - the poll did not happen. `at()` names the stage, and the
  error rides beside it. Nothing is recorded, so a later poll with the same
  version retries.

The `version` argument says WHICH STATE the readable is; it is the only version
in the API, and the pipeline never computes one - it compares the ones it is
handed. That pairing is the point: the version costs a comparison, and the
readable may be a large snapshot that a matching version never touches.

```rust
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use libpipeline::{Ctx, PipelineBuilder, Run};
use libpipelinedata::{ContentKey, EffectPoll, MemoKey};

# static LEN_RUNS: AtomicUsize = AtomicUsize::new(0);
# fn len_key(input: &String, ctx: &Ctx<'_>) -> Option<MemoKey> {
#     Some(ctx.key([ContentKey::of(input)]))
# }
# fn len(input: &String, _ctx: &Ctx<'_>) -> EffectPoll<usize, &'static str> {
#     LEN_RUNS.fetch_add(1, Ordering::Relaxed);
#     EffectPoll::Ready(input.len())
# }
let pipeline = PipelineBuilder::new()
    .stage_fn("len", len_key, len)
    .build();

let doc = "doc.title".to_string();
assert_eq!(pipeline.poll(1, &doc), Ok(Run::Computed(Arc::new(9))));

// The same version again: the gate answers, and the graph is not entered. The
// readable is not even looked at - which is measurable, because this one is a
// different string.
assert_eq!(pipeline.poll(1, &"anything at all".to_string()), Ok(Run::Unchanged));
assert_eq!(LEN_RUNS.load(Ordering::Relaxed), 1);

// A version it has not computed for reaches the graph.
assert_eq!(pipeline.poll(2, &"hi".to_string()), Ok(Run::Computed(Arc::new(2))));
assert_eq!(LEN_RUNS.load(Ordering::Relaxed), 2);
```

**A wake counts as much as a version, and the gate knows it.** Two different
things mean "something happened": the input version moves when the source
changes, and a wake arrives when a value some stage was waiting on has landed.
A landed effect does not move the input version, so the gate answers
`Unchanged` only when the version matches AND no wake is pending. Without that
half, a pipeline sitting on `Delayed` would receive its wake, poll again,
short-circuit on the unchanged version and answer `Unchanged` forever, with the
caller holding a value one step stale and nothing reporting it.

**There is no "has a wake arrived" accessor.** The flag clears when it is read,
so a second reader is a second claimant on one wake, and the gate is the reader
that must not lose. `waker()` hands out the wake target for landing values out
of band; a caller with nothing else to ask polls every frame and lets
`Unchanged` be the cheap answer, which is what that variant is for.

## Two caller patterns

Blocking and frame driving are things a caller does with `poll`, not doors the
pipeline provides. A stage cannot tell which is asking.

* **A frame caller** polls once per frame. A `Delayed` poll draws its stand-in;
  the waker the stage registered wakes the pipeline when the value lands, and
  the next frame's poll picks it up.
* **A blocking caller** loops on `Delayed`, pumping its own executor between
  polls. `run_blocking` is that loop, shipped as a free function whose body is
  a loop over `poll` and nothing else. `Delayed` when the caller has nothing
  left to run means something waited for an input nothing was going to land -
  and that is the CALLER's condition, because only the caller can see that its
  queue is empty, so `run_blocking` hands back the plain `Delayed` rather than
  deciding.

```rust
use std::sync::{Arc, LazyLock, Mutex};
use std::task::Waker;

use libpipeline::{Ctx, PipelineBuilder, Run, run_blocking};
use libpipelinedata::{EffectPoll, MemoKey};

/// Where a value lands out of band, and where a delayed poll leaves its waker.
#[derive(Default)]
struct Slot {
    value: Mutex<Option<u32>>,
    waker: Mutex<Option<Waker>>,
}

impl Slot {
    fn land(&self, value: u32) {
        *self.value.lock().unwrap() = Some(value);
        if let Some(waker) = self.waker.lock().unwrap().take() {
            waker.wake();
        }
    }
}

// A stage is a `fn` and cannot capture, so what it waits on is reached through
// a `static`. That is the shape today; see the note above.
static SLOT: LazyLock<Slot> = LazyLock::new(Slot::default);

/// An effect's answer is not a cacheable fact, so this refuses to key.
fn fetch_key(_input: &(), _ctx: &Ctx<'_>) -> Option<MemoKey> {
    None
}

/// `Pending` until the slot is filled.
fn fetch(_input: &(), ctx: &Ctx<'_>) -> EffectPoll<u32, &'static str> {
    match *SLOT.value.lock().unwrap() {
        Some(value) => EffectPoll::Ready(value),
        None => {
            // Answering `Pending` obliges the stage to arrange a wake.
            *SLOT.waker.lock().unwrap() = Some(ctx.waker().clone());
            EffectPoll::Pending
        }
    }
}

// The frame pattern: poll, delayed, wake, poll again.
let pipeline = PipelineBuilder::new()
    .stage_fn("fetch", fetch_key, fetch)
    .build();

assert_eq!(pipeline.poll(1, &()), Ok(Run::Delayed)); // frame 1: draw a stand-in
SLOT.land(7);                                        // the value arrives out of band
assert_eq!(pipeline.poll(1, &()), Ok(Run::Computed(Arc::new(7))));

// The blocking pattern: the same stage, the caller's own pump.
let pipeline = PipelineBuilder::new()
    .stage_fn("fetch", fetch_key, fetch)
    .build();

let mut pumped = false;
let outcome = run_blocking(&pipeline, 1, &(), || {
    if pumped {
        return false; // nothing left to run: the caller's own stall condition
    }
    pumped = true;
    SLOT.land(7);
    true
});
assert_eq!(outcome, Ok(Run::Computed(Arc::new(7))));
```

**A `Delayed` that owes a wake and leaves none is a value LOST rather than
late.** It is invisible to a blocking caller, which polls again without being
asked, and fatal to a frame caller, which never learns there is anything to ask
for. Answering `Pending` makes registering an obligation for that reason, and
what the defect looks like from outside is a pipeline answering `Unchanged`
forever over a value that has already moved.

## The storage seam

Where answers are remembered is a separate decision from the engine, and it is
ONE decision about the whole pipeline, taken once, at the builder. By default
the pipeline remembers into a map it owns; `.store(..)` takes any `MemoStore`
implementation you provide instead. The store is handed over at the builder and
lives exactly as long as the pipeline does.

**The seam accepts and returns `Arc`, on both sides, always.** A store holds a
stage's output for as long as it is worth remembering, and a lookup hands back a
SHARE of it - so a miss costs one allocation and every hit after it is a refcount
bump. It is unconditional on purpose: a contract that shared large outputs and
copied small ones would put that judgement on the stage author, it would get made
once at the moment the output was small, and the copy per hit it left behind would
outlive the output growing without any test being able to see it (answers do not
change; only speed does).

One store serves every stage, whatever their outputs are, because the rows are
erased: the store the builder holds is instantiated at `dyn Any + Send + Sync`,
so what it records is the share the stage already answered with, unsized -
nothing is wrapped twice - and a lookup is `Arc::downcast`. `V: ?Sized` on the
store below is what that costs an implementation generic over its value type; a
store written for this builder alone would name the erased type directly and
need nothing generic at all.

```rust
use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};

use libpipeline::{Ctx, PipelineBuilder, Run};
use libpipelinedata::{ContentKey, EffectPoll, MemoKey, MemoStore};

/// A store of your own: any `lookup`/`record` pair over `MemoKey` will do.
struct MapStore<V: ?Sized> {
    rows: Mutex<HashMap<MemoKey, Arc<V>>>,
}

impl<V: ?Sized> MemoStore<V> for MapStore<V> {
    fn lookup(&self, key: &MemoKey) -> Option<Arc<V>> {
        self.rows.lock().unwrap().get(key).map(Arc::clone)
    }

    fn record(&self, key: &MemoKey, value: Arc<V>) {
        self.rows.lock().unwrap().insert(key.clone(), value);
    }
}

# static LEN_RUNS: AtomicUsize = AtomicUsize::new(0);
# fn len_key(input: &String, ctx: &Ctx<'_>) -> Option<MemoKey> {
#     Some(ctx.key([ContentKey::of(input)]))
# }
# fn len(input: &String, _ctx: &Ctx<'_>) -> EffectPoll<usize, &'static str> {
#     LEN_RUNS.fetch_add(1, Ordering::Relaxed);
#     EffectPoll::Ready(input.len())
# }
/// Renders the count, so the pipeline below holds two stages whose outputs
/// are different types - and still one store.
fn render_key(input: &usize, ctx: &Ctx<'_>) -> Option<MemoKey> {
    Some(ctx.key([ContentKey::of(input)]))
}

fn render(input: &usize, _ctx: &Ctx<'_>) -> EffectPoll<String, &'static str> {
    EffectPoll::Ready(input.to_string())
}

let store: Arc<MapStore<dyn Any + Send + Sync>> = Arc::new(MapStore {
    rows: Mutex::new(HashMap::new()),
});

let pipeline = PipelineBuilder::new()
    .store(Arc::clone(&store))
    .stage_fn("len", len_key, len)
    .stage_fn("render", render_key, render)
    .build();

let Ok(Run::Computed(rendered)) = pipeline.poll(1, &"abcd".to_string()) else {
    panic!("both stages are pure");
};
assert_eq!(*rendered, "4");

// One store, two stages, two output types: each row is keyed by the stage's
// identity - its position - so each stage gets its own answer back.
assert_eq!(store.rows.lock().unwrap().len(), 2);

// And the repeat is served from it: neither stage ran again. The version moves,
// so what answers is the store rather than the gate above it.
assert_eq!(pipeline.poll(2, &"abcd".to_string()), Ok(Run::Computed(Arc::new("4".to_string()))));
assert_eq!(LEN_RUNS.load(Ordering::Relaxed), 1);
```

## Where to look next

* `DESIGN.md` - the design: the model the engine embodies, why the builder is
  the only public door, why registration takes `fn` pointers rather than a
  trait, why there is one door onto running rather than two, why identity is a
  position, and where the one store and the one error type come from. Its
  "Ruled, not yet built" section is the shortest route to what is about to
  change.
* `libpipelinedata` - the shared vocabulary: `StageId`, `MemoKey`/`ContentKey`,
  the `ContentHash` streaming hasher and derive, and the `MemoStore` seam.
* The tests in `tests/` are written exclusively against the public API shown
  on this page, and are a good second read.

## License

Licensed under either of

 * Apache License, Version 2.0
   ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
 * MIT license
   ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.
