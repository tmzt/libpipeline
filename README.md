# libpipeline

An incremental pipeline engine. You register pure, poll-driven stages with a
builder; the engine memoizes every stage under content-addressed keys, chains
the stages into one graph, and runs it through one door - `run(version,
&input)`, which polls once and returns immediately, whatever the answer.
Blocking to completion (a build tool) and one run per frame (an interactive
host) are things a CALLER does with that one call: the same stages, the same
cache, either caller.

`libpipeline` is the engine half of a two-crate pair. Its companion
`libpipelinedata` is the port: the `Stage` trait, the key types, and the
storage seam. A crate that only *authors* stages depends on `libpipelinedata`
alone and never links the engine; the crate that *assembles and drives* them
adds `libpipeline`. The examples below are assembly-side code, so they use
both.

Every example on this page is a doctest: `cargo test` compiles and runs them.

## The smallest working pipeline

A stage implements `Stage` from `libpipelinedata`: an identity, a memo key
computable from the input alone, and a poll. The builder is the only way to
compose, memoize, or drive - you assemble nothing by hand.

```rust
use std::task::Context;

use libpipeline::{PipelineBuilder, Run};
use libpipelinedata::{ContentKey, EffectPoll, MemoKey, Stage, StageId};

/// Splits a dotted path like "doc.title" into its segments.
struct Split {
    id: StageId,
}

impl Stage for Split {
    type Input = String;
    type Output = Vec<String>;
    type Error = &'static str;

    fn id(&self) -> StageId {
        self.id
    }

    /// The memo key, computable from the input alone - before the stage runs.
    fn memo_key(&self, input: &String) -> Option<MemoKey> {
        Some(MemoKey::new(self.id, [ContentKey::of(input)]))
    }

    fn poll_stage(
        &self,
        input: &String,
        _cx: &mut Context<'_>,
    ) -> EffectPoll<Vec<String>, &'static str> {
        if input.is_empty() {
            return EffectPoll::Failed("nothing to split");
        }
        EffectPoll::Ready(input.split('.').map(str::to_string).collect())
    }
}

let pipeline = PipelineBuilder::new()
    .stage("split", |id| Split { id })
    .build();

// `1` is the run VERSION: which state the input is. Where it comes from is
// yours - an edit store's cursor, a build number, a git sha.
let Ok(Run::Computed(segments)) = pipeline.run(1, &"doc.title".to_string()) else {
    panic!("a pure stage answers on the first run");
};
assert_eq!(*segments, vec!["doc".to_string(), "title".to_string()]);
```

`Run::Computed` hands back an `Arc` of the output, because the memo still
holds the value it just answered with - that is what a memo is. It also means
a large output costs a refcount bump per cache hit and nothing else, without
any stage author remembering to wrap anything: the engine wraps once, on a
miss, where it records.

Two things to notice at the registration call site:

* **The builder mints the identity, and the identity is a position.** The
  `StageId` handed to the closure is this registration's index and nothing
  else; a stage never declares one, so there is no second id it could answer
  with and nothing to check. `"split"` beside it is a diagnostic label: it
  enters no key, nothing is looked up or compared by it, and a second stage
  may carry the same one with no consequence.
* There is no separate "memoize" step. **Registering a stage memoizes it**;
  there is no un-memoized registration to reach for.

## Adding stages

Chain another `.stage(..)` call; the new stage's `Input` must equal the
previous stage's `Output` - the value, not the share the graph carries it in;
unwrapping between stages is the engine's job too - and its `Error` is the
pipeline's one error type, which every stage of one pipeline shares. A failure
comes back carrying the POSITION of the stage that raised it.

```rust
use std::task::Context;

use libpipeline::{PipelineBuilder, Run};
use libpipelinedata::{ContentKey, EffectPoll, MemoKey, Stage, StageId};

# struct Split { id: StageId }
# impl Stage for Split {
#     type Input = String;
#     type Output = Vec<String>;
#     type Error = &'static str;
#     fn id(&self) -> StageId { self.id }
#     fn memo_key(&self, input: &String) -> Option<MemoKey> {
#         Some(MemoKey::new(self.id, [ContentKey::of(input)]))
#     }
#     fn poll_stage(&self, input: &String, _cx: &mut Context<'_>)
#         -> EffectPoll<Vec<String>, &'static str>
#     {
#         if input.is_empty() {
#             return EffectPoll::Failed("nothing to split");
#         }
#         EffectPoll::Ready(input.split('.').map(str::to_string).collect())
#     }
# }
/// Counts the segments the first stage produced.
struct Count {
    id: StageId,
}

impl Stage for Count {
    type Input = Vec<String>;
    type Output = usize;
    type Error = &'static str;

    fn id(&self) -> StageId {
        self.id
    }

    fn memo_key(&self, input: &Vec<String>) -> Option<MemoKey> {
        Some(MemoKey::new(self.id, input.iter().map(ContentKey::of)))
    }

    fn poll_stage(
        &self,
        input: &Vec<String>,
        _cx: &mut Context<'_>,
    ) -> EffectPoll<usize, &'static str> {
        EffectPoll::Ready(input.len())
    }
}

let pipeline = PipelineBuilder::new()
    .stage("split", |id| Split { id })
    .stage("count", |id| Count { id })
    .build();

let Ok(Run::Computed(count)) = pipeline.run(1, &"doc.section.title".to_string()) else {
    panic!("both stages are pure");
};
assert_eq!(*count, 3);

// A failure names the stage that raised it, as a position: stage 0 here,
// because `Split` failed and `Count` never ran. One `at()` call answers that
// at any length of chain.
let Err(failure) = pipeline.run(2, &String::new()) else {
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

The versions below MOVE while the content stays the same, which is how a run
reaches the graph at all: the version gate above it answers first when the
version repeats (the next section is about that), so a test of the memo has to
hand the pipeline a state it has not computed for yet.

```rust
use std::sync::{Arc, Mutex};
use std::task::Context;

use libpipeline::{PipelineBuilder, Run};
use libpipelinedata::{ContentKey, EffectPoll, MemoKey, Stage, StageId};

/// Measures a string, counting how many times it actually ran.
struct Len {
    id: StageId,
    runs: Arc<Mutex<usize>>,
}

impl Stage for Len {
    type Input = String;
    type Output = usize;
    type Error = &'static str;

    fn id(&self) -> StageId {
        self.id
    }

    fn memo_key(&self, input: &String) -> Option<MemoKey> {
        Some(MemoKey::new(self.id, [ContentKey::of(input)]))
    }

    fn poll_stage(
        &self,
        input: &String,
        _cx: &mut Context<'_>,
    ) -> EffectPoll<usize, &'static str> {
        *self.runs.lock().unwrap() += 1;
        EffectPoll::Ready(input.len())
    }
}

let runs = Arc::new(Mutex::new(0));
let counted = Arc::clone(&runs);
let pipeline = PipelineBuilder::new()
    .stage("len", move |id| Len { id, runs: counted })
    .build();

assert_eq!(pipeline.run(1, &"abcd".to_string()), Ok(Run::Computed(Arc::new(4))));
assert_eq!(pipeline.run(2, &"abcd".to_string()), Ok(Run::Computed(Arc::new(4))));
assert_eq!(*runs.lock().unwrap(), 1); // the repeat was served by the lookup

// `.uncached()` turns the store off: the control run. Answers must not
// change, only speed - a pipeline whose answers change when the cache is
// disabled has a bug the cache was hiding.
let runs = Arc::new(Mutex::new(0));
let counted = Arc::clone(&runs);
let control = PipelineBuilder::new()
    .uncached()
    .stage("len", move |id| Len { id, runs: counted })
    .build();

assert_eq!(control.run(1, &"abcd".to_string()), Ok(Run::Computed(Arc::new(4))));
assert_eq!(control.run(2, &"abcd".to_string()), Ok(Run::Computed(Arc::new(4))));
assert_eq!(*runs.lock().unwrap(), 2); // the control ran every time
```

**Opting out.** A stage whose answer is not a cacheable fact - an effect, a
read of something no key can address - answers `memo_key -> None`. It is then
neither looked up nor recorded, and everything else about it is unchanged.
Failures are never cached regardless: a transient failure served back from a
memo would outlive its cause.

## One door, and what it answers

`run(version, &readable)` polls once and returns immediately, whatever the
answer. Nothing inside it waits, ever. There are four answers:

* **`Run::Computed(value)`** - work happened; take the new value. The pipeline
  records the version it answered for.
* **`Run::Unchanged`** - the value you already hold derives from exactly this
  state; keep it. Read it as *the value is finished*: not a report that nothing
  happened, but a statement that nothing needs to. The readable is not
  dereferenced, no memo key is computed and no stage is polled.
* **`Run::Delayed`** - not ready; a wake is coming. The run arranged for the
  pipeline's waker to be woken when the answer becomes possible, so wait to be
  woken rather than re-polling in a spin.
* **`Err(Failure)`** - the run did not happen. `at()` names the stage, and the
  error rides beside it. Nothing is recorded, so a later run with the same
  version retries.

The `version` argument says WHICH STATE the readable is; it is the only version
in the API, and the pipeline never computes one - it compares the ones it is
handed. That pairing is the point: the version costs a comparison, and the
readable may be a large snapshot that a matching version never touches.

```rust
use std::sync::{Arc, Mutex};
use std::task::Context;

use libpipeline::{PipelineBuilder, Run};
use libpipelinedata::{ContentKey, EffectPoll, MemoKey, Stage, StageId};

# struct Len { id: StageId, runs: Arc<Mutex<usize>> }
# impl Stage for Len {
#     type Input = String;
#     type Output = usize;
#     type Error = &'static str;
#     fn id(&self) -> StageId { self.id }
#     fn memo_key(&self, input: &String) -> Option<MemoKey> {
#         Some(MemoKey::new(self.id, [ContentKey::of(input)]))
#     }
#     fn poll_stage(&self, input: &String, _cx: &mut Context<'_>)
#         -> EffectPoll<usize, &'static str>
#     {
#         *self.runs.lock().unwrap() += 1;
#         EffectPoll::Ready(input.len())
#     }
# }
let runs = Arc::new(Mutex::new(0));
let counted = Arc::clone(&runs);
let pipeline = PipelineBuilder::new()
    .stage("len", move |id| Len { id, runs: counted })
    .build();

let doc = "doc.title".to_string();
assert_eq!(pipeline.run(1, &doc), Ok(Run::Computed(Arc::new(9))));

// The same version again: the gate answers, and the graph is not entered. The
// readable is not even looked at - which is measurable, because this one is a
// different string.
assert_eq!(pipeline.run(1, &"anything at all".to_string()), Ok(Run::Unchanged));
assert_eq!(*runs.lock().unwrap(), 1);

// A version it has not computed for reaches the graph.
assert_eq!(pipeline.run(2, &"hi".to_string()), Ok(Run::Computed(Arc::new(2))));
assert_eq!(*runs.lock().unwrap(), 2);
```

**A wake counts as much as a version, and the gate knows it.** Two different
things mean "something happened": the input version moves when the source
changes, and a wake arrives when a value some stage was waiting on has landed.
A landed effect does not move the input version, so the gate consumes the
pipeline's stale flag on every run and answers `Unchanged` only when the
version matches AND no wake was pending. Without that half, a pipeline sitting
on `Delayed` would receive its wake, re-run, short-circuit on the unchanged
version and answer `Unchanged` forever, with the caller holding a value one
step stale and nothing reporting it.

`take_stale()` asks the same question directly ("has a wake arrived since I
last asked"; reading clears it) and `waker()` hands out the wake target for
landing values out of band. Because the flag clears on read and `run` reads it
too, a frame caller with nothing else to ask should simply run every frame:
`Unchanged` is the cheap answer, and it is what that variant is for.

## Two caller patterns

Blocking and frame driving are things a caller does with `run`, not doors the
pipeline provides. A stage cannot tell which is asking.

* **A frame caller** runs once per frame. A `Delayed` run draws its stand-in;
  the waker the stage registered marks the pipeline stale when the value lands,
  and the next frame's run picks it up.
* **A blocking caller** loops on `Delayed`, pumping its own executor between
  runs. `Delayed` when the caller has nothing left to run means something
  waited for an input nothing was going to land - and that is the CALLER's
  condition, because only the caller can see that its queue is empty.

```rust
use std::sync::{Arc, Mutex};
use std::task::{Context, Waker};

use libpipeline::{PipelineBuilder, Run};
use libpipelinedata::{EffectPoll, MemoKey, Stage, StageId};

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

/// `Pending` until the slot is filled. An effect's answer is not a cacheable
/// fact, so it refuses to key.
struct Fetch {
    id: StageId,
    slot: Arc<Slot>,
}

impl Stage for Fetch {
    type Input = ();
    type Output = u32;
    type Error = &'static str;

    fn id(&self) -> StageId {
        self.id
    }

    fn memo_key(&self, _input: &()) -> Option<MemoKey> {
        None
    }

    fn poll_stage(&self, _input: &(), cx: &mut Context<'_>) -> EffectPoll<u32, &'static str> {
        match *self.slot.value.lock().unwrap() {
            Some(value) => EffectPoll::Ready(value),
            None => {
                // Answering `Pending` obliges the stage to arrange a wake.
                *self.slot.waker.lock().unwrap() = Some(cx.waker().clone());
                EffectPoll::Pending
            }
        }
    }
}

// The frame pattern: run, delayed, wake, run again.
let slot = Arc::new(Slot::default());
let registered = Arc::clone(&slot);
let pipeline = PipelineBuilder::new()
    .stage("fetch", move |id| Fetch { id, slot: registered })
    .build();

assert_eq!(pipeline.run(1, &()), Ok(Run::Delayed)); // frame 1: draw a stand-in
slot.land(7);                                       // the value arrives out of band
assert_eq!(pipeline.run(1, &()), Ok(Run::Computed(Arc::new(7))));

// The blocking pattern: the same stage, the caller's own loop and executor.
let slot = Arc::new(Slot::default());
let registered = Arc::clone(&slot);
let pipeline = PipelineBuilder::new()
    .stage("fetch", move |id| Fetch { id, slot: registered })
    .build();

let landed = Mutex::new(false);
let mut pump_once = || {
    let mut landed = landed.lock().unwrap();
    if *landed {
        return false; // nothing left to run: the caller's own stall condition
    }
    *landed = true;
    slot.land(7);
    true
};

let outcome = loop {
    match pipeline.run(1, &()) {
        Ok(Run::Delayed) if pump_once() => continue,
        done => break done,
    }
};
assert_eq!(outcome, Ok(Run::Computed(Arc::new(7))));
```

**A `Delayed` that owes a wake and leaves none is a value LOST rather than
late.** It is invisible to a blocking caller, which runs again without being
asked, and fatal to a frame caller, which never learns there is anything to ask
for. `Stage::poll_stage` makes registering an obligation for that reason, and
`take_stale()` staying false after the value lands is what the defect looks
like from outside.

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
so what it records is the share the memo layer already made, unsized - nothing is
wrapped twice - and a lookup is `Arc::downcast`. `V: ?Sized` on the store below is
what that costs an implementation generic over its value type; a store written for
this builder alone would name the erased type directly and need nothing generic at
all.

```rust
use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
# use std::task::Context;
# use libpipelinedata::{ContentKey, EffectPoll, Stage, StageId};

use libpipeline::{PipelineBuilder, Run};
use libpipelinedata::{MemoKey, MemoStore};

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

# struct Len {
#     id: StageId,
#     runs: Arc<Mutex<usize>>,
# }
# impl Stage for Len {
#     type Input = String;
#     type Output = usize;
#     type Error = &'static str;
#     fn id(&self) -> StageId { self.id }
#     fn memo_key(&self, input: &String) -> Option<MemoKey> {
#         Some(MemoKey::new(self.id, [ContentKey::of(input)]))
#     }
#     fn poll_stage(&self, input: &String, _cx: &mut Context<'_>)
#         -> EffectPoll<usize, &'static str>
#     {
#         *self.runs.lock().unwrap() += 1;
#         EffectPoll::Ready(input.len())
#     }
# }
/// Renders the count, so the pipeline below holds two stages whose outputs
/// are different types - and still one store.
struct Render {
    id: StageId,
}

impl Stage for Render {
    type Input = usize;
    type Output = String;
    type Error = &'static str;

    fn id(&self) -> StageId {
        self.id
    }

    fn memo_key(&self, input: &usize) -> Option<MemoKey> {
        Some(MemoKey::new(self.id, [ContentKey::of(input)]))
    }

    fn poll_stage(&self, input: &usize, _cx: &mut Context<'_>)
        -> EffectPoll<String, &'static str>
    {
        EffectPoll::Ready(input.to_string())
    }
}

let store: Arc<MapStore<dyn Any + Send + Sync>> = Arc::new(MapStore {
    rows: Mutex::new(HashMap::new()),
});
let runs = Arc::new(Mutex::new(0));
let counted = Arc::clone(&runs);

let pipeline = PipelineBuilder::new()
    .store(Arc::clone(&store))
    .stage("len", move |id| Len { id, runs: counted })
    .stage("render", |id| Render { id })
    .build();

let Ok(Run::Computed(rendered)) = pipeline.run(1, &"abcd".to_string()) else {
    panic!("both stages are pure");
};
assert_eq!(*rendered, "4");

// One store, two stages, two output types: each row is keyed by the stage's
// identity - its position - so each stage gets its own answer back.
assert_eq!(store.rows.lock().unwrap().len(), 2);

// And the repeat is served from it: neither stage ran again. The version moves,
// so what answers is the store rather than the gate above it.
assert_eq!(pipeline.run(2, &"abcd".to_string()), Ok(Run::Computed(Arc::new("4".to_string()))));
assert_eq!(*runs.lock().unwrap(), 1);
```

## Where to look next

* `DESIGN.md` - the design: the model the engine embodies, why the builder is
  the only public door, why there is one door onto running rather than two, why
  identity is a position, and where the one store and the one error type come
  from.
* `libpipelinedata` - the stage author's crate: `Stage`, `StageId`,
  `MemoKey`/`ContentKey`, the `ContentHash` streaming hasher and derive, and
  the `MemoStore` seam.
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
