<!-- llm-restriction: disallow-edit[claude] -->
# libpipeline

`libpipeline` is an engine for incremental computation. Complex software systems—such as compilers, interactive document editors, and real-time renderers—often process data through a lengthy sequence of dependent steps. When the source data changes incrementally over time, executing the entire sequence from scratch becomes prohibitively expensive.

To solve this, `libpipeline` allows developers to define computations as a sequence of independent stages, each consuming the output of the one before it. Before executing any computational work, the engine calculates a content hash (key) for the pending input. If the engine recognizes this key from a previous execution, it immediately retrieves the cached output, preempting redundant computation entirely.

The engine exposes a single, unified entry point to drive this process: `poll(version, &input)`. Execution is strictly non-blocking and completely decoupled from any specific polling strategy. Whether you are blocking a background thread to completion for a batch build tool, or polling once per frame in an interactive host application, you rely on the exact same stage definitions, cache semantics, and API boundaries.

## Core Design Philosophy

The primary objective of `libpipeline` is to make incremental, memoized computation reliable and deterministic. Doing so requires strict guarantees about how data flows and how state is preserved. If intermediate computational steps hide mutable state, or if side effects go untracked, the cache logic fails and the pipeline serves stale data.

To guarantee cache correctness across the chain of stages, the architecture enforces several strict design invariants:

### Architectural Design Invariants

#### Compilation-Enforced Statelessness

A stage is defined strictly by two function pointers: a key function for input identification and a poll function for output generation. The API enforces statelessness at compile time by requiring raw `fn` pointers; capturing closures fail to compile. This statically prevents hidden state mutations from invalidating cache guarantees. Identity and the waker are passed via `Ctx`; anything else ambient must live in a `static` today - see State Management Considerations.

Non-capturing closures do not carry state and coerce safely to raw `fn` pointers:

```rust
use libpipeline::{Ctx, PipelineBuilder};
use libpipelinedata::{ContentKey, EffectPoll, StageAnswer};

let pipeline = PipelineBuilder::new()
    .stage_fn(
        "scale",
        /* Key Function: identifies the input */
        |input: &i32, ctx| Some(ctx.key([ContentKey::of(input)])),
        /* Compute / Poll Function: produces the output strictly from the input */
        |input: &i32, _ctx| -> EffectPoll<StageAnswer<i32>, &'static str> {
            StageAnswer::computed(input * 2)
        },
    )
    .build();

let _ = pipeline.poll(1, &4);
```

Capturing variables from the surrounding environment prevents coercion to a `fn` pointer, resulting in a compiler error:

```rust,compile_fail
use libpipeline::{Ctx, PipelineBuilder};
use libpipelinedata::{ContentKey, EffectPoll, StageAnswer};

let ambient_multiplier = 3;

let pipeline = PipelineBuilder::new()
    .stage_fn(
        "scale_dynamic",
        |input: &i32, ctx| Some(ctx.key([ContentKey::of(input)])),
        /* Fails to compile because it captures `ambient_multiplier` */
        |input: &i32,
         _ctx|
         -> EffectPoll<StageAnswer<i32>, &'static str> {
            StageAnswer::computed(input * ambient_multiplier)
        },
    )
    .build();
```

#### Cache-Lookup Preemption

Keys are computed purely from inputs and the `Ctx` prior to stage execution. This enables the engine to preempt execution entirely upon a cache hit, avoiding even the setup cost of the stage.

#### Poll-Driven Execution & Wakers

The engine never blocks. Stages performing asynchronous operations yield a pending state and register a waker. When out-of-band data arrives, the waker signals the engine, prompting the caller's next poll to retrieve the finalized value.

`libpipeline` handles the core execution and memoization logic, while its companion crate, `libpipelinedata`, defines the shared data structures, including key types, content hashes, and the `MemoStore` interface.

## Developer Integration Guide

Every example provided below is an executable doctest.

### Constructing the Pipeline

A pipeline stage fundamentally pairs a key function with a poll function. The builder interface is the exclusive mechanism for composition, implicit memoization, and pipeline execution.

```rust
use libpipeline::{Ctx, PipelineBuilder, Run};
use libpipelinedata::{ContentKey, EffectPoll, MemoKey, StageAnswer};

// What "doc.title" IS, as a key - computable without splitting anything.
//
// `Ctx::key` supplies the identity half: a key is `(stage id, input content
// keys)`, and the id is the half a stage has no business choosing.
let split_key = |input: &String, ctx: &Ctx<'_>| -> Option<MemoKey> {
    Some(ctx.key([ContentKey::of(input)]))
};

// Splits a dotted path like "doc.title" into its segments.
let split = |input: &String,
             _ctx: &Ctx<'_>|
 -> EffectPoll<StageAnswer<Vec<String>>, &'static str> {
    if input.is_empty() {
        return EffectPoll::Failed("nothing to split");
    }

    StageAnswer::computed(
        input
            .split('.')
            .map(str::to_string)
            .collect(),
    )
};

let pipeline = PipelineBuilder::new()
    .stage_fn("split", split_key, split)
    .build();

// `1` is the run VERSION: which state the input is. Where it comes from is
// yours - an edit store's cursor, a build number, a git sha.
let Ok(Run::Computed(segments)) =
    pipeline.poll(1, &"doc.title".to_string())
else {
    panic!("a pure stage answers on the first poll");
};

assert_eq!(*segments, vec!["doc".to_string(), "title".to_string()]);
```

When a stage completes, `Run::Computed` returns an `Arc` containing the output. The cache retains this exact value. Subsequent cache hits incur only a reference count increment, avoiding large data copies. The engine handles all value wrapping implicitly at the graph boundary.

### Key Registration Behavior

#### Positional Identity

The engine assigns a `StageId` internally based on registration order. Stages do not declare intrinsic IDs. Diagnostic string labels (e.g., `"split"`) are excluded from cache keys and identity comparisons.

#### Stateless Pointers

Registration accepts only `fn` pointers or non-capturing closures. Preventing environment capture guarantees that outputs cannot shift independently of the input key, preserving cache integrity.

#### Implicit Memoization

Memoization is not opt-in per stage: a registered stage is memoized unless its key function declines (`None`) or the whole pipeline is built `.uncached()`.

#### Explicit State Returns

A stage signals new output via `StageAnswer::computed(value)`, or signals an unchanged state via `StageAnswer::unchanged()`.

### Stage Chaining

Pipelines scale by chaining subsequent `.stage_fn(..)` calls. Downstream stages consume the unwrapped output of their immediate predecessors. The pipeline enforces a single, unified error type across all stages. When an error occurs, the pipeline surfaces the failure alongside the numerical position of the originating stage.

```rust
use libpipeline::{Ctx, PipelineBuilder, Run};
use libpipelinedata::{ContentKey, EffectPoll, MemoKey, StageAnswer};

# let split_key = |input: &String, ctx: &Ctx<'_>| -> Option<MemoKey> {
#     Some(ctx.key([ContentKey::of(input)]))
# };
# let split = |input: &String, _ctx: &Ctx<'_>|
#     -> EffectPoll<StageAnswer<Vec<String>>, &'static str>
# {
#     if input.is_empty() {
#         return EffectPoll::Failed("nothing to split");
#     }
#
#     StageAnswer::computed(input.split('.').map(str::to_string).collect())
# };
// Counts the segments the first stage produced.
let count_key = |input: &Vec<String>, ctx: &Ctx<'_>| -> Option<MemoKey> {
    Some(
        ctx.key(
            input
                .iter()
                .map(ContentKey::of),
        ),
    )
};

let count = |input: &Vec<String>,
             _ctx: &Ctx<'_>|
 -> EffectPoll<StageAnswer<usize>, &'static str> {
    StageAnswer::computed(input.len())
};

let pipeline = PipelineBuilder::new()
    .stage_fn("split", split_key, split)
    .stage_fn("count", count_key, count)
    .build();

let Ok(Run::Computed(count)) =
    pipeline.poll(1, &"doc.section.title".to_string())
else {
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

*(Note: `#`-prefixed lines within doctests duplicate earlier definitions to satisfy compilation requirements.)*

### Incremental Memoization

The engine executes a cache lookup for every stage it enters. A stage whose upstream answered `unchanged` is not entered. The cache key combines the internal `stage id` and the content keys of the provided inputs. If the input remains unchanged, the cache lookup succeeds, and stage execution is entirely bypassed.

In the following example, advancing the version while maintaining identical content forces the poll past the fast-path version gate, engaging the memo cache.

#### State Management Considerations

Because stages are raw `fn` pointers without internal fields, required external state (e.g., metrics counters, texture atlases) must reside in static memory. Future revisions of the API intend to expose such ambient context natively through the `Ctx` parameter (refer to `DESIGN.md`).

```rust
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use libpipeline::{Ctx, PipelineBuilder, Run};
use libpipelinedata::{ContentKey, EffectPoll, MemoKey, StageAnswer};

// A closure that captures nothing coerces to the `fn` the door takes - which
// is also why the run counter is a `static` rather than a local it closes over.
static LEN_RUNS: AtomicUsize = AtomicUsize::new(0);

let len_key = |input: &String, ctx: &Ctx<'_>| -> Option<MemoKey> {
    Some(ctx.key([ContentKey::of(input)]))
};

// Measures a string, counting how many times it actually ran.
let len = |input: &String,
           _ctx: &Ctx<'_>|
 -> EffectPoll<StageAnswer<usize>, &'static str> {
    LEN_RUNS.fetch_add(1, Ordering::Relaxed);

    StageAnswer::computed(input.len())
};

let pipeline = PipelineBuilder::new()
    .stage_fn("len", len_key, len)
    .build();

assert_eq!(
    pipeline.poll(1, &"abcd".to_string()),
    Ok(Run::Computed(Arc::new(4)))
);

assert_eq!(
    pipeline.poll(2, &"abcd".to_string()),
    Ok(Run::Computed(Arc::new(4)))
);

assert_eq!(LEN_RUNS.load(Ordering::Relaxed), 1); // the repeat was served by the lookup

// `.uncached()` turns the store off: the control run. Answers must not
// change, only speed - a pipeline whose answers change when the cache is
// disabled has a bug the cache was hiding.
let control = PipelineBuilder::new()
    .uncached()
    .stage_fn("len", len_key, len)
    .build();

assert_eq!(
    control.poll(1, &"abcd".to_string()),
    Ok(Run::Computed(Arc::new(4)))
);

assert_eq!(
    control.poll(2, &"abcd".to_string()),
    Ok(Run::Computed(Arc::new(4)))
);

assert_eq!(LEN_RUNS.load(Ordering::Relaxed), 3); // the control ran every time
```

#### Uncacheable Operations

Stages performing side effects or reading state that cannot be keyed must return `None` from the key function. The engine bypasses cache lookups and storage for these stages without affecting the rest of the pipeline. Failures are never cached; storing a transient error would indefinitely mask recovery.

### State Unchanged Optimization

Stages that process input without enacting modifications can return `StageAnswer::unchanged()`.

This signal carries no payload. The engine retains the output from the stage's previous execution. Downstream stages perceive no input changes, validate against their own cached states, and are completely preempted. This optimizes away execution for the entire downstream chain.

```rust
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use libpipeline::{Ctx, PipelineBuilder, Run};
use libpipelinedata::{EffectPoll, MemoKey, StageAnswer};

static UPPER_RUNS: AtomicUsize = AtomicUsize::new(0);
static SHOUT_RUNS: AtomicUsize = AtomicUsize::new(0);

// Neither stage keys, so neither is served from the store: each is entered and
// answers for itself. That is what lets the first one say `unchanged`.
let unkeyed =
    |_input: &String, _ctx: &Ctx<'_>| -> Option<MemoKey> { None };

// Upper-cases - and says so when there was nothing to upper-case.
let upper = |input: &String,
             _ctx: &Ctx<'_>|
 -> EffectPoll<StageAnswer<String>, &'static str> {
    UPPER_RUNS.fetch_add(1, Ordering::Relaxed);

    match input
        .chars()
        .any(|c| c.is_lowercase())
    {
        true => StageAnswer::computed(input.to_uppercase()),
        false => StageAnswer::unchanged(),
    }
};

let shout = |input: &String,
             _ctx: &Ctx<'_>|
 -> EffectPoll<StageAnswer<String>, &'static str> {
    SHOUT_RUNS.fetch_add(1, Ordering::Relaxed);

    StageAnswer::computed(format!("{input}!"))
};

let pipeline = PipelineBuilder::new()
    .stage_fn("upper", unkeyed, upper)
    .stage_fn("shout", unkeyed, shout)
    .build();

// Something to do: both stages run.

assert_eq!(
    pipeline.poll(1, &"hi".to_string()),
    Ok(Run::Computed(Arc::new("HI!".to_string()))),
);

// A new version, and nothing to do. `upper` is entered - it is unkeyed, so
// nothing answers ahead of it - and says `unchanged`. `shout` is not entered.

assert_eq!(pipeline.poll(2, &"HI".to_string()), Ok(Run::Unchanged));

assert_eq!(UPPER_RUNS.load(Ordering::Relaxed), 2);
assert_eq!(SHOUT_RUNS.load(Ordering::Relaxed), 1);
```

#### Usage Constraints

`unchanged` fundamentally references a prior output. Emitting `unchanged` during a stage's initial execution triggers a panic, as no previous value exists to reference.

The correct sequence requires executing a compute pass before returning `unchanged`:

```rust
use libpipeline::{Ctx, PipelineBuilder, Run};
use libpipelinedata::{EffectPoll, MemoKey, StageAnswer};

let unkeyed =
    |_input: &String, _ctx: &Ctx<'_>| -> Option<MemoKey> { None };
let upper = |input: &String,
             _ctx: &Ctx<'_>|
 -> EffectPoll<StageAnswer<String>, &'static str> {
    match input
        .chars()
        .any(|c| c.is_lowercase())
    {
        true => StageAnswer::computed(input.to_uppercase()),
        false => StageAnswer::unchanged(),
    }
};

let pipeline = PipelineBuilder::new()
    .stage_fn("upper", unkeyed, upper)
    .build();

// Initial poll performs computation

assert!(pipeline
    .poll(1, &"hi".to_string())
    .is_ok());

// Subsequent poll can now safely return Unchanged

assert_eq!(pipeline.poll(2, &"HI".to_string()), Ok(Run::Unchanged));
```

Triggering `unchanged` on a cold pipeline without a prior cached value results in a panic:

```rust,should_panic(expected = "stage at position 0 answered Unchanged before it had answered at all")
use libpipeline::{Ctx, PipelineBuilder};
use libpipelinedata::{EffectPoll, MemoKey, StageAnswer};

let unkeyed =
    |_input: &String, _ctx: &Ctx<'_>| -> Option<MemoKey> { None };
let premature = |_input: &String,
                 _ctx: &Ctx<'_>|
 -> EffectPoll<StageAnswer<String>, &'static str> {
    StageAnswer::unchanged()
};

let pipeline = PipelineBuilder::new()
    .stage_fn("premature", unkeyed, premature)
    .build();

// Panics: stage at position 0 answered Unchanged before it had answered at all
let _ = pipeline.poll(1, &"hi".to_string());
```

### Contractual Trust

The engine does not verify the accuracy of `unchanged`. If a stage mutates data but emits `unchanged`, the pipeline permanently serves stale data. This contract, similar to a `Pending` future promising a subsequent wake, must be upheld by the stage logic.

### Execution and the Polling Interface

The `poll(version, &readable)` method executes a single, non-blocking pipeline iteration. It yields one of four outcomes:

* `Run::Computed(value)`: Execution occurred; returns the newly computed value and records the processed version.
* `Run::Unchanged`: The existing output is accurate and up-to-date. This variant asserts that no work is necessary. It occurs either when the input version matches the recorded version (preempting key computation and stage polling entirely) or when a stage explicitly returns `unchanged`.
* `Run::Delayed`: The pipeline is blocked on pending asynchronous operations. A waker has been registered. The caller must await waking rather than spin-polling.
* `Err(Failure)`: Pipeline execution failed. The error payload identifies the originating stage. Failures are uncached, permitting immediate retry on subsequent polls.

The `version` parameter defines the input state. The pipeline compares the provided version against its internal record to bypass heavy snapshot dereferencing on unmodified inputs.

```rust
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use libpipeline::{Ctx, PipelineBuilder, Run};
use libpipelinedata::{ContentKey, EffectPoll, MemoKey, StageAnswer};

# static LEN_RUNS: AtomicUsize = AtomicUsize::new(0);
# let len_key = |input: &String, ctx: &Ctx<'_>| -> Option<MemoKey> {
#     Some(ctx.key([ContentKey::of(input)]))
# };
# let len = |input: &String, _ctx: &Ctx<'_>|
#     -> EffectPoll<StageAnswer<usize>, &'static str>
# {
#     LEN_RUNS.fetch_add(1, Ordering::Relaxed);
#
#     StageAnswer::computed(input.len())
# };
let pipeline = PipelineBuilder::new()
    .stage_fn("len", len_key, len)
    .build();

let doc = "doc.title".to_string();

assert_eq!(pipeline.poll(1, &doc), Ok(Run::Computed(Arc::new(9))));

// The same version again: the gate answers, and the graph is not entered. The
// readable is not even looked at - which is measurable, because this one is a
// different string.

assert_eq!(
    pipeline.poll(1, &"anything at all".to_string()),
    Ok(Run::Unchanged)
);

assert_eq!(LEN_RUNS.load(Ordering::Relaxed), 1);

// A version it has not computed for reaches the graph.

assert_eq!(
    pipeline.poll(2, &"hi".to_string()),
    Ok(Run::Computed(Arc::new(2)))
);

assert_eq!(LEN_RUNS.load(Ordering::Relaxed), 2);
```

#### Waker Preemption

The pipeline must execute if either the input version advances or an asynchronous waker signals a pending effect. Because effects resolve asynchronously, the early-exit version gate returns `Unchanged` only if the version matches *and* no wakes are pending.

The internal wake flag clears immediately upon read. Exposing this flag via a public API would steal the signal from the version gate, causing deadlocks. Out-of-band resolutions obtain wakers via `Ctx::waker()`. For frame-continuous applications, frequent polling handles these updates optimally, as `Unchanged` returns incur negligible overhead.

### Polling Strategies

The pipeline exposes only `poll`. It remains agnostic to the caller's execution strategy.

#### Frame-Synchronous Polling

The caller polls exactly once per frame. A `Delayed` result typically prompts rendering a placeholder. Asynchronous completion triggers the waker, allowing the subsequent frame's poll to retrieve the finalized data.

#### Executor-Blocked Polling

The caller loops on `poll` within an asynchronous executor. The provided `run_blocking` utility implements this loop, yielding to a custom executor pump. If the pump exhausts its queue while the pipeline remains `Delayed`, a permanent deadlock has occurred, and `run_blocking` surfaces the `Delayed` variant.

```rust
use std::sync::{Arc, LazyLock, Mutex};
use std::task::Waker;

use libpipeline::{run_blocking, Ctx, PipelineBuilder, Run};
use libpipelinedata::{EffectPoll, MemoKey, StageAnswer};

/// Where a value lands out of band, and where a delayed poll leaves its waker.
#[derive(Default)]
struct Slot {
    value: Mutex<Option<u32>>,
    waker: Mutex<Option<Waker>>,
}

impl Slot {
    fn land(&self, value: u32) {
        *self
            .value
            .lock()
            .unwrap() = Some(value);
        if let Some(waker) = self
            .waker
            .lock()
            .unwrap()
            .take()
        {
            waker.wake();
        }
    }
}

// A stage is a `fn` and cannot capture, so what it waits on is reached through
// a `static`. That is the shape today; see the note above.
static SLOT: LazyLock<Slot> = LazyLock::new(Slot::default);

// An effect's answer is not a cacheable fact, so this refuses to key.
let fetch_key = |_input: &(), _ctx: &Ctx<'_>| -> Option<MemoKey> { None };

// `Pending` until the slot is filled.
let fetch = |_input: &(),
             ctx: &Ctx<'_>|
 -> EffectPoll<StageAnswer<u32>, &'static str> {
    match *SLOT
        .value
        .lock()
        .unwrap()
    {
        Some(value) => StageAnswer::computed(value),
        None => {
            // Answering `Pending` obliges the stage to arrange a wake.
            *SLOT
                .waker
                .lock()
                .unwrap() = Some(
                ctx.waker()
                    .clone(),
            );

            EffectPoll::Pending
        }
    }
};

// The frame pattern: poll, delayed, wake, poll again.
let pipeline = PipelineBuilder::new()
    .stage_fn("fetch", fetch_key, fetch)
    .build();

assert_eq!(pipeline.poll(1, &()), Ok(Run::Delayed)); // frame 1: draw a stand-in
SLOT.land(7); // the value arrives out of band

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

#### Waker Obligations

Emitting a `Delayed` response without registering a corresponding waker constitutes a critical defect. It silently deadlocks frame-driven callers and induces infinite loops in blocking callers. Stages returning `Pending` are strictly obligated to register a waker.

### Storage Interface Seam

Computation is structurally decoupled from storage. The cache backend is configured globally during builder initialization. While the pipeline defaults to an internal hash map, custom `MemoStore` implementations (e.g., for LRU eviction policies or disk persistence) can be injected via `.store(..)`. A caller can retain a shared reference to the store, and the pipeline holds its own share for its lifecycle.

#### Shared Reference Mandate

The storage interface exclusively handles `Arc` pointers. Cache misses allocate once; cache hits incur only a reference count increment. This strictly prevents stage authors from inadvertently duplicating large structures on cache hits.

A unified store manages all stages despite disparate return types. Output types are erased to `dyn Any + Send + Sync`. Retrieving a value requires a lightweight `Arc::downcast`. Generic store implementations require a `V: ?Sized` bound.

```rust
use std::any::Any;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use libpipeline::{Ctx, PipelineBuilder, Run};
use libpipelinedata::{
    ContentKey, EffectPoll, MemoKey, MemoStore, StageAnswer,
};

/// A store of your own: any `lookup`/`record` pair over `MemoKey` will do.
struct MapStore<V: ?Sized> {
    rows: Mutex<HashMap<MemoKey, Arc<V>>>,
}

impl<V: ?Sized> MemoStore<V> for MapStore<V> {
    fn lookup(&self, key: &MemoKey) -> Option<Arc<V>> {
        self.rows
            .lock()
            .unwrap()
            .get(key)
            .map(Arc::clone)
    }

    fn record(&self, key: &MemoKey, value: Arc<V>) {
        self.rows
            .lock()
            .unwrap()
            .insert(key.clone(), value);
    }
}

# static LEN_RUNS: AtomicUsize = AtomicUsize::new(0);
# let len_key = |input: &String, ctx: &Ctx<'_>| -> Option<MemoKey> {
#     Some(ctx.key([ContentKey::of(input)]))
# };
# let len = |input: &String, _ctx: &Ctx<'_>|
#     -> EffectPoll<StageAnswer<usize>, &'static str>
# {
#     LEN_RUNS.fetch_add(1, Ordering::Relaxed);
#
#     StageAnswer::computed(input.len())
# };
// Renders the count, so the pipeline below holds two stages whose outputs
// are different types - and still one store.
let render_key = |input: &usize, ctx: &Ctx<'_>| -> Option<MemoKey> {
    Some(ctx.key([ContentKey::of(input)]))
};

let render = |input: &usize,
              _ctx: &Ctx<'_>|
 -> EffectPoll<StageAnswer<String>, &'static str> {
    StageAnswer::computed(input.to_string())
};

let store: Arc<MapStore<dyn Any + Send + Sync>> = Arc::new(MapStore {
    rows: Mutex::new(HashMap::new()),
});

let pipeline = PipelineBuilder::new()
    .store(Arc::clone(&store))
    .stage_fn("len", len_key, len)
    .stage_fn("render", render_key, render)
    .build();

let Ok(Run::Computed(rendered)) = pipeline.poll(1, &"abcd".to_string())
else {
    panic!("both stages are pure");
};

assert_eq!(*rendered, "4");

// One store, two stages, two output types: each row is keyed by the stage's
// identity - its position - so each stage gets its own answer back.

assert_eq!(
    store
        .rows
        .lock()
        .unwrap()
        .len(),
    2
);

// And the repeat is served from it: neither stage ran again. The version moves,
// so what answers is the store rather than the gate above it.

assert_eq!(
    pipeline.poll(2, &"abcd".to_string()),
    Ok(Run::Computed(Arc::new("4".to_string()))),
);

assert_eq!(LEN_RUNS.load(Ordering::Relaxed), 1);
```

## Further Documentation

* `DESIGN.md` — Architectural deep-dive covering builder exclusivity, pointer enforcement, positional identity, and single-store philosophies.
* `libpipelinedata` — Documentation for shared primitives: `StageId`, `MemoKey`, `ContentHash`, and `MemoStore`.
* `tests/` — Test suites validating the public API behaviors described herein.

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
