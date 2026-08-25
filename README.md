# libpipeline

An incremental pipeline engine. You register pure, poll-driven stages with a
builder; the engine memoizes every stage under content-addressed keys, chains
the stages into one graph, and drives that graph either blocking to
completion (a build tool) or one poll per frame (an interactive host) - the
same stages, the same cache, either driver.

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

use libpipeline::PipelineBuilder;
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

assert_eq!(
    pipeline.run_pure(&"doc.title".to_string()),
    Ok(vec!["doc".to_string(), "title".to_string()]),
);
```

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
previous stage's `Output`, and its `Error` is the pipeline's one error type -
every stage of one pipeline shares it. A failure comes back carrying the
POSITION of the stage that raised it.

```rust
use std::task::Context;

use libpipeline::{DriveError, PipelineBuilder};
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

assert_eq!(pipeline.run_pure(&"doc.section.title".to_string()), Ok(3));

// A failure names the stage that raised it, as a position: stage 0 here,
// because `Split` failed and `Count` never ran. One `at()` call answers that
// at any length of chain.
let Err(DriveError::Failed(failure)) = pipeline.run_pure(&String::new()) else {
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

```rust
use std::sync::{Arc, Mutex};
use std::task::Context;

use libpipeline::PipelineBuilder;
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

assert_eq!(pipeline.run_pure(&"abcd".to_string()), Ok(4));
assert_eq!(pipeline.run_pure(&"abcd".to_string()), Ok(4));
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

assert_eq!(control.run_pure(&"abcd".to_string()), Ok(4));
assert_eq!(control.run_pure(&"abcd".to_string()), Ok(4));
assert_eq!(*runs.lock().unwrap(), 2); // the control ran every time
```

**Opting out.** A stage whose answer is not a cacheable fact - an effect, a
read of something no key can address - answers `memo_key -> None`. It is then
neither looked up nor recorded, and everything else about it is unchanged.
Failures are never cached regardless: a transient failure served back from a
memo would outlive its cause.

## The two drivers

The same graph runs under two drives, and a stage cannot tell which one is
polling it. That is what makes an interactive host and a batch tool one API
rather than two implementations that agree by convention.

* **The frame drive** (`poll_frame`) polls once and returns immediately,
  whatever the answer. A `Pending` frame draws its stand-in; the waker the
  stage registered marks the pipeline stale when the value lands, and
  `take_stale` tells the frame loop to poll again.
* **The blocking drive** (`run`, `run_pure`) polls until a value or a typed
  failure, pumping a `PendingWork` implementation while polls answer
  `Pending`. `run_pure` is `run` with nothing to pump, for graphs of pure
  stages; a `Pending` there is a `Stalled` error, not a hang.

```rust
use std::sync::{Arc, Mutex};
use std::task::{Context, Waker};

use libpipeline::{PendingWork, PipelineBuilder};
use libpipelinedata::{EffectPoll, MemoKey, Stage, StageId};

/// Where a value lands out of band, and where a parked poll leaves its waker.
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

// The frame drive: poll, park, wake, re-poll.
let slot = Arc::new(Slot::default());
let registered = Arc::clone(&slot);
let pipeline = PipelineBuilder::new()
    .stage("fetch", move |id| Fetch { id, slot: registered })
    .build();

assert!(pipeline.poll_frame(&()).is_pending()); // frame 1: draw a stand-in
slot.land(7);                                   // the value arrives out of band
assert!(pipeline.take_stale());                 // the wake asked for a re-poll
assert_eq!(pipeline.poll_frame(&()), EffectPoll::Ready(7));

// The blocking drive: the same stage, pumped by a `PendingWork` impl.
struct LandsSeven(Arc<Slot>, Mutex<bool>);

impl PendingWork for LandsSeven {
    fn run_once(&self) -> bool {
        let mut landed = self.1.lock().unwrap();
        if *landed {
            return false;
        }
        *landed = true;
        self.0.land(7);
        true
    }
}

let slot = Arc::new(Slot::default());
let registered = Arc::clone(&slot);
let pipeline = PipelineBuilder::new()
    .stage("fetch", move |id| Fetch { id, slot: registered })
    .build();

assert_eq!(pipeline.run(&(), &LandsSeven(slot, Mutex::new(false))), Ok(7));
```

**Watching a drive.** A `Pending` poll that registers no waker is a defect
the blocking drive cannot feel - it re-polls without being asked - and the
frame drive cannot survive: the value is lost rather than late.
`run_watched` is the blocking drive with that observation riding alongside
the (unchanged) answer:

```rust
# use std::sync::{Arc, Mutex};
# use std::task::{Context, Waker};
# use libpipeline::{PendingWork, PipelineBuilder};
# use libpipelinedata::{EffectPoll, MemoKey, Stage, StageId};
# #[derive(Default)]
# struct Slot {
#     value: Mutex<Option<u32>>,
# }
# impl Slot {
#     fn land(&self, value: u32) {
#         *self.value.lock().unwrap() = Some(value);
#     }
# }
/// The defect: `Pending` with the waker ignored. No frame loop would ever
/// learn the value landed.
struct Forgets {
    id: StageId,
    slot: Arc<Slot>,
}

impl Stage for Forgets {
    type Input = ();
    type Output = u32;
    type Error = &'static str;

    fn id(&self) -> StageId {
        self.id
    }

    fn memo_key(&self, _input: &()) -> Option<MemoKey> {
        None
    }

    fn poll_stage(&self, _input: &(), _cx: &mut Context<'_>) -> EffectPoll<u32, &'static str> {
        match *self.slot.value.lock().unwrap() {
            Some(value) => EffectPoll::Ready(value),
            None => EffectPoll::Pending, // no waker registered!
        }
    }
}
#
# struct LandsSeven(Arc<Slot>, Mutex<bool>);
# impl PendingWork for LandsSeven {
#     fn run_once(&self) -> bool {
#         let mut landed = self.1.lock().unwrap();
#         if *landed {
#             return false;
#         }
#         *landed = true;
#         self.0.land(7);
#         true
#     }
# }

let slot = Arc::new(Slot::default());
let registered = Arc::clone(&slot);
let pipeline = PipelineBuilder::new()
    .stage("fetch", move |id| Forgets { id, slot: registered })
    .build();

let (out, report) = pipeline.run_watched(&(), &LandsSeven(slot, Mutex::new(false)));
assert_eq!(out, Ok(7));          // the blocking drive still completes...
assert!(!report.is_clean());     // ...and now says a frame drive would not have
assert_eq!(report.unwakeable_polls(), 1);
```

## The storage seam

Where answers are remembered is a separate decision from the engine, and it is
ONE decision about the whole pipeline, taken once, at the builder. By default
the pipeline remembers into a map it owns; `.store(..)` takes any `MemoStore`
implementation you provide instead. The store is handed over at the builder and
lives exactly as long as the pipeline does.

One store serves every stage, whatever their outputs are, because the rows are
erased: a store records `Arc<dyn Any + Send + Sync>` and a lookup downcasts back.
That is why the store below is instantiated at that type rather than at any one
stage's output - and why an implementation generic over its value type, like this
one, needs no change to serve a whole pipeline.

```rust
use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
# use std::task::Context;
# use libpipelinedata::{ContentKey, EffectPoll, Stage, StageId};

use libpipeline::PipelineBuilder;
use libpipelinedata::{MemoKey, MemoStore};

/// A store of your own: any `lookup`/`record` pair over `MemoKey` will do.
struct MapStore<V> {
    rows: Mutex<HashMap<MemoKey, V>>,
}

impl<V: Clone> MemoStore<V> for MapStore<V> {
    fn lookup(&self, key: &MemoKey) -> Option<V> {
        self.rows.lock().unwrap().get(key).cloned()
    }

    fn record(&self, key: &MemoKey, value: V) {
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

let store: Arc<MapStore<Arc<dyn Any + Send + Sync>>> = Arc::new(MapStore {
    rows: Mutex::new(HashMap::new()),
});
let runs = Arc::new(Mutex::new(0));
let counted = Arc::clone(&runs);

let pipeline = PipelineBuilder::new()
    .store(Arc::clone(&store))
    .stage("len", move |id| Len { id, runs: counted })
    .stage("render", |id| Render { id })
    .build();

assert_eq!(pipeline.run_pure(&"abcd".to_string()), Ok("4".to_string()));

// One store, two stages, two output types: each row is keyed by the stage's
// identity - its position - so each stage gets its own answer back.
assert_eq!(store.rows.lock().unwrap().len(), 2);

// And the repeat is served from it: neither stage ran again.
assert_eq!(pipeline.run_pure(&"abcd".to_string()), Ok("4".to_string()));
assert_eq!(*runs.lock().unwrap(), 1);
```

## Where to look next

* `DESIGN.md` - the design: the model the engine embodies, why the builder is
  the only public door, why identity is a position, and where the one store
  and the one error type come from.
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
