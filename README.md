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
    .stage("split", 1, |id| Split { id })
    .build();

assert_eq!(
    pipeline.run_pure(&"doc.title".to_string()),
    Ok(vec!["doc".to_string(), "title".to_string()]),
);
```

Two things to notice at the registration call site:

* `stage("split", 1, ...)` declares the stage's **version** right there, in
  the same lexical scope as the closure that builds the behaviour it
  versions. The builder mints a `StageId` from `(name, version)` and hands it
  to the closure; if the constructed stage answers a different id, the
  builder panics at construction rather than misfiling cache entries later.
* There is no separate "memoize" step. **Registering a stage memoizes it**;
  there is no un-memoized registration to reach for.

## Adding stages

Chain another `.stage(..)` call; the new stage's `Input` must equal the
previous stage's `Output`. A failure comes back tagged with which stage
raised it.

```rust
use std::task::Context;

use libpipeline::{ChainError, DriveError, PipelineBuilder};
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
    .stage("split", 1, |id| Split { id })
    .stage("count", 1, |id| Count { id })
    .build();

assert_eq!(pipeline.run_pure(&"doc.section.title".to_string()), Ok(3));

// A failure names the stage that raised it: `First` here, because `Split`
// failed and `Count` never ran.
assert_eq!(
    pipeline.run_pure(&String::new()),
    Err(DriveError::Failed(ChainError::First("nothing to split"))),
);
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
    .stage("len", 1, move |id| Len { id, runs: counted })
    .build();

assert_eq!(pipeline.run_pure(&"abcd".to_string()), Ok(4));
assert_eq!(pipeline.run_pure(&"abcd".to_string()), Ok(4));
assert_eq!(*runs.lock().unwrap(), 1); // the repeat was served by the lookup

// `.uncached()` disables every store: the control run. Answers must not
// change, only speed - a pipeline whose answers change when the cache is
// disabled has a bug the cache was hiding.
let runs = Arc::new(Mutex::new(0));
let counted = Arc::clone(&runs);
let control = PipelineBuilder::new()
    .uncached()
    .stage("len", 1, move |id| Len { id, runs: counted })
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
    .stage("fetch", 1, move |id| Fetch { id, slot: registered })
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
    .stage("fetch", 1, move |id| Fetch { id, slot: registered })
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
    .stage("fetch", 1, move |id| Forgets { id, slot: registered })
    .build();

let (out, report) = pipeline.run_watched(&(), &LandsSeven(slot, Mutex::new(false)));
assert_eq!(out, Ok(7));          // the blocking drive still completes...
assert!(!report.is_clean());     // ...and now says a frame drive would not have
assert_eq!(report.unwakeable_polls(), 1);
```

## The storage seam

Where answers are remembered is a separate decision from the engine. By
default each registered stage gets a fresh store owned by the pipeline;
`stage_in` takes any `MemoStore` implementation you provide instead, which is
how a cache outlives one build of the pipeline - and how you can see the
version rule work: share a store across two builds, bump the version, and the
old entries become unreachable, because the stage id is half of every key.

```rust
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
let store: Arc<MapStore<usize>> = Arc::new(MapStore {
    rows: Mutex::new(HashMap::new()),
});
let runs = Arc::new(Mutex::new(0));

let run_once = |version: u32| {
    let counted = Arc::clone(&runs);
    let pipeline = PipelineBuilder::new()
        .stage_in("len", version, Arc::clone(&store), move |id| Len {
            id,
            runs: counted,
        })
        .build();
    pipeline.run_pure(&"abcd".to_string())
};

// The store outlives each pipeline: a rebuild at the same version hits.
assert_eq!(run_once(1), Ok(4));
assert_eq!(run_once(1), Ok(4));
assert_eq!(*runs.lock().unwrap(), 1);

// Bumping the version at the one call site that declares it: a cold cache.
assert_eq!(run_once(2), Ok(4));
assert_eq!(*runs.lock().unwrap(), 2);
```

## Where to look next

* `DESIGN.md` - the design: the model the engine embodies, why the builder is
  the only public door, the proposed closure-shaped stage registration, and
  the honest list of what the builder cannot yet express.
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
