# mayu

[![PyPI](https://img.shields.io/pypi/v/mayu.svg)](https://pypi.org/project/mayu/)
[![Python versions](https://img.shields.io/pypi/pyversions/mayu.svg)](https://pypi.org/project/mayu/)
[![CI](https://github.com/Siddid-Soni/mayu/actions/workflows/ci.yml/badge.svg)](https://github.com/Siddid-Soni/mayu/actions/workflows/ci.yml)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)

**mayu is a fast, Redis-backed Python task queue with a Rust core, built for free-threaded
(no-GIL) Python.** Its workers run tasks on real parallel OS threads under CPython 3.14t, so a
few processes can do the work that Celery needs dozens of forked processes for.

> Status: alpha. Delivery is at-least-once: **tasks must be idempotent.**

- **Fast:** about 31× Celery's no-op throughput and 5× on a realistic task, with an eighth of the
  memory ([benchmarks](https://github.com/Siddid-Soni/mayu/blob/main/bench/RESULTS.md)).
- **Free-threaded first:** thread mode on Python 3.14t, with automatic fallback to process mode on
  GIL builds (3.13, 3.14).
- **Reliable:** acks after the task finishes, crash recovery, poison-task detection, dead letters
  and replay, and it survives Redis restarts.
- **Familiar API:** `@app.task`, `.delay()`, `.send()`, retries, countdown/ETA, rate limits, time
  limits, queue priority, `link=` callbacks, sync and `async def` tasks.
- **Operable:** Prometheus metrics, JSON logs, OpenTelemetry tracing, and an `inspect` CLI.

## Install

```bash
pip install mayu            # or: uv add mayu
pip install "mayu[otel]"    # with OpenTelemetry tracing
pip install "mayu[gpu]"     # with NVIDIA GPU monitoring (NVML)
```

Requires Python 3.13+ and Redis. Use a free-threaded build (`python3.14t`, e.g.
`uv venv --python 3.14t`) to get thread mode.

## Quickstart

```python
# tasks.py
from mayu import App, Retry

app = App(broker="redis://localhost:6379/0", result_ttl=3600)

@app.task(max_retries=5, retry_on=(TimeoutError,))
def add(a: int, b: int) -> int:
    return a + b

@app.task
async def fetch(url: str) -> str:
    ...
```

```bash
mayu worker tasks:app --threads 32
```

```python
from tasks import add
add.delay(2, 3)
h = add.send(2, 3, countdown=10)
h.result(timeout=30)   # 5
```

Configuration comes from constructor arguments, overridden by `MAYU_BROKER`,
`MAYU_RESULT_TTL` (`0` disables results), `MAYU_DEFAULT_QUEUE`, `MAYU_SERIALIZER`.
`countdown`, `eta`, `queue`, `link` and `expires` are reserved option names and cannot be task parameters.

## Reliability

mayu delivers **at least once**: a task is acknowledged only after it finishes, so a crash
means the task runs again, never that it is lost. **Write idempotent tasks.**

- Each worker heartbeats to `mayu:workers:{worker_id}` (TTL = 3 × `--heartbeat-interval`).
- A worker keeps ownership of tasks it is running by re-claiming them every half
  `--visibility-timeout` (default 30 s).
- When a worker dies (`kill -9`, OOM, segfault), the other workers reclaim its tasks once they
  have been silent for `--visibility-timeout`.
- A task whose worker dies `--max-deliveries` times in a row (default 5) is treated as poison
  and dead-lettered instead of being retried forever.
- Redis restarts are survived: connections reconnect, and outcomes produced while Redis is
  down are recorded when it returns. Use Redis persistence (AOF) if queued tasks must survive
  a Redis restart.
- Python threads cannot be killed: set timeouts on network clients inside tasks.

Chaos test (needs Docker): `.venv/bin/pytest tests/chaos -v`
(`MAYU_CHAOS_N` / `MAYU_CHAOS_SECONDS` scale it).

## Task expiry

```python
add.send(2, 3, expires=30)                          # drop if not started within 30 seconds
add.send(2, 3, expires=some_tz_aware_datetime)      # or an absolute deadline
```

A task with `expires=` is dropped when a worker claims it after the deadline has passed: it is
acked without running, settled as `outcome="expired"`, and (when results are enabled) a "task
expired" failure is stored, so `handle.result()` raises `TaskFailed`. The check is at claim time,
so a task already claimed into a worker's prefetch buffer still runs. The deadline is `expires`
seconds from when it is sent, or an absolute timezone-aware `datetime`. The deadline travels in an
optional envelope field that older workers ignore, so upgrade workers before relying on it.

- **The deadline is absolute and spans retries.** A retry (or a rate-limit deferral) keeps the
  original `expires`, so a task whose backoff or `countdown` lands after the deadline is dropped as
  expired on its next claim, and that "task expired" result replaces the error that caused the
  retry.
- **Clock skew.** `expires` is set from the sender's clock and checked against the worker's, so
  keep their clocks in sync (NTP); a worker running behind the sender enforces the deadline late.

## Time limits

```python
@app.task(soft_time_limit=30, hard_time_limit=60)
def crunch(): ...
```

- **Soft limit:** `mayu.SoftTimeLimitExceeded` is raised inside the task at its next Python
  bytecode, so it can be caught to clean up. It **cannot interrupt blocking C calls or socket
  reads**: set timeouts on network clients.
- On Python 3.13 a soft limit that lands in a `while` loop with an empty body can escape the
  task's `try`/`except` (a CPython 3.13 bug, fixed in 3.14). The task then fails with
  `SoftTimeLimitExceeded` without running its cleanup.
- **Hard limit:** Python threads cannot be killed. The task is recorded as failed with
  `mayu.TimeLimitExceeded` (retried if it is in `retry_on`), its thread is abandoned as a
  *zombie*, and a replacement thread takes over. Whatever the zombie produces later is discarded.
- Async tasks are cancelled at their limit instead.
- `@app.task(threadsafe=False)` runs a task on one dedicated thread, one call at a time, for
  legacy code that is not thread-safe.

The worker recycles itself (graceful stop, then a fresh interpreter in the same process) when
zombies exceed `--max-zombie-threads` (default 4), after `--max-tasks-per-process` tasks, or
above `--max-memory-per-process` MB of RSS.

## Rate limits

```python
@app.task(rate_limit="100/s")   # also "10/m", "5/h"; a bare number means per second
def call_api(): ...
```

Starts are spaced evenly (no bursts). The limit is **per worker process**: with
`--processes 4`, a `10/s` task can start 40 times a second across them. Tasks that would wait
more than a second for their turn go back to Redis until then, so a rate-limited backlog never
fills a worker's prefetch buffer or delays other tasks. A deferred task returns to Redis as a
fresh entry, so its crash-redelivery count (`--max-deliveries`) starts over. Deferred tasks are
spaced for the worker that deferred them, so adding workers doesn't speed up a backlog that is
already deferred.

## Resources

```python
@app.task(resources={"gpu": 0.3, "vram_gb": 1.2})
def embed(texts): ...
```

```bash
mayu worker tasks:app --resources gpu=1,vram_gb=7.5     # or MAYU_RESOURCES
mayu worker tasks:app --gpus 0,1 --resources gpu=1      # one worker process per GPU
```

A worker starts a task only when everything it requests fits in what is left of what the worker
declared, so three of the tasks above run side by side on one GPU and a fourth waits for a
finisher. Amounts are in whatever unit you pick (GPUs, GB, cores), down to 0.001. A worker
refuses to start if a task on a queue it serves requests a resource it didn't declare, or more
than it has; tasks on other queues are ignored. Tasks without `resources=` are never held back.

- **No starvation:** if the oldest, highest-priority waiting task has been due for 5 seconds,
  later tasks may start only if they request none of the resources it is waiting for.
- **Per process:** each worker process counts only its own tasks. Several processes sharing a
  GPU each think they own it, so run one process per GPU and size `--resources` for that
  process. `--gpus 0,1` does this: it starts one process per id with `CUDA_VISIBLE_DEVICES`
  set to it, before your app is imported (ids are opaque, so `GPU-abc...` UUIDs work).
  `--processes`, if given, must equal the number of ids.
- **Zombies:** resources are released when a task's outcome is reported. A task past its hard
  time limit is reported as failed while its thread keeps running, so until the worker
  recycles, a zombie can use a GPU that a new task has already been given.

## Model pools

```python
@app.pool("gemma", vram_gb=5.8)             # resources are per instance
def gemma(ctx):                             # ctx.name, ctx.index in range(ctx.size), ctx.size
    return load_gemma(device="cuda")

@gemma.close
def _(model):
    model.unload()

@app.task(queue="gpu")
def encode(text, model=app.use("gemma")): ...   # model is one instance, yours alone until the task ends
```

```bash
mayu worker tasks:app --queues gpu --pools gemma=2,tts=3 --resources gpu=1,vram_gb=40   # or MAYU_POOLS
```

Code says how to build an instance; the operator says how many. A worker builds every instance
before it fetches anything, one at a time and logging each load time, and keeps them for its life.
At most N tasks that use a pool of N run at once, so a task always finds a free instance and never
waits inside the task body: it waits in the worker's queue, like any task held back by
`resources=`. A task gets its instances before its time limits start and gives them back before
its outcome is reported.

- **Reservations:** a pool's resources are reserved for the worker's life, N × the amount, and
  come off what `--resources` leaves for tasks. `gemma=2` above holds 11.6 GB of `vram_gb`; a
  task asking for more than the remaining 28.4 makes the worker refuse to start. Resource names
  starting with `pool:` are reserved.
- **Startup checks:** a worker refuses to start if a task it serves uses a pool that the process
  doesn't register with `@app.pool` or that `--pools` doesn't size, if `--pools` names an
  unregistered pool, or if the reservations exceed what it declares. A pool sized but unused by the
  queues it serves is not loaded (a warning is logged). If a factory raises while loading, the
  instances built so far are closed and the worker exits.
- **Broken instances:** raise `DiscardInstance` (`DiscardInstance(pool="gemma")` to discard only
  that pool's instance, otherwise all the task holds), or list exception classes in
  `@app.pool(..., discard_on=CudaError)`. The task still fails or retries by its usual rules
  (`retry_on=(DiscardInstance,)` retries it on another instance). After the outcome is reported,
  the instance is closed and a replacement built with the same `ctx.index`; the pool runs one
  instance short meanwhile. If the rebuild fails, the pool stays smaller and
  `mayu_pool_size` shows it. A pool with no instances left makes the worker recycle.
- **Close:** `@pool.close` runs on discarded instances and, at shutdown, on every instance that is
  back in the pool. Instances still held by threads that outlive the shutdown grace period are not
  closed. Errors in `close` are logged and ignored.
- **Zombies:** a task past its hard time limit is reported as failed while its thread keeps its
  instance, so the pool shrinks by one for good: the instance is never reused or closed, because
  the thread may still be using it. Recycling brings the pool back to full size.
- **Per process:** each worker process loads its own instances, in process mode and with `--gpus`
  too, so size pools for one process or one GPU. A recycle restarts the process and reloads every
  instance, so recycling a worker with big models costs their load time.
- **Sync tasks only:** an `async def` task can't use a pool (`TypeError` at registration). A task
  can use each pool in only one parameter, and the parameter must be usable by keyword.
- **Producers need no pool code:** `app.use("gemma")` is only a name, resolved when a worker
  starts, so a producer can declare the task (see below) without torch or `@app.pool`. Passing
  the pool's parameter to `send()`, `wf.add()` or a `link=` raises `TypeError`. A payload that
  carries it anyway, only possible with `app.send` by name, has its keyword value overwritten by
  the instance, and a positional one fails the task with a duplicate-argument `TypeError`.

### Lazy pools (models that don't fit together)

Eager pools (the default, described above) load everything at startup and keep it. If the models
don't fit in memory together, run the worker in lazy mode: it loads a model when a task needs it and
closes idle ones to make room.

```bash
mayu worker tasks:app --queues gpu --pools gemma=1,tts=1 --pool-mode lazy --schedule throughput \
    --preload gemma --resources gpu=1,vram_gb=7.5 --prefetch-multiplier 1
# or MAYU_POOL_MODE=lazy, MAYU_SCHEDULE, MAYU_PRELOAD
```

Here `gemma` and `tts` don't fit in 7.5 GB together, so only one is in memory at a time. The pool
code and the tasks are the same as for eager pools; `--pools gemma=1` is now the most instances
loaded at once, and nothing is reserved for it at startup. `--pool-mode` applies to every pool of
the worker.

- **Loading and eviction:** a pool's resources are held only while an instance is loaded. A task
  that finds no idle instance of its pool has the worker build one, after closing idle instances
  (least recently used first) until it fits. Only idle instances are closed, never one a task is
  using. A task without a pool may close an idle model too, if it has no room beside it. Loading and
  closing happen before the task's time limits start.
- **`--preload gemma,tts`:** load one instance of each at startup, in that order. They must fit
  together, or the worker refuses to start. Without it the first task for a model pays the load.
- **`--schedule`:** a worker holds back tasks that don't fit, and lazy mode lets it prefer ones whose
  model is already loaded. Queue priority still comes first; this only orders tasks within a
  priority. `throughput` (fewest swaps) lets a task with a loaded model run ahead of other work that
  arrived up to 60 seconds before it. `oldest` (the default in lazy mode) orders by the start of the
  workflow, and by arrival for tasks outside one, so a workflow already under way finishes first; a
  loaded model is worth 10 seconds, about one model load. A task that needs a swap is passed only by
  loaded-model tasks that arrived (or started) less than that bound after it, so none waits forever.
  The bounds are not configurable.
- **Only prefetched tasks are reordered:** the worker chooses among tasks it has already claimed
  from Redis. Run GPU queues with a small `--prefetch-multiplier` (the default is 2): a larger
  buffer gives it more to choose from but claims more tasks before they can run.
- **Startup checks:** `--schedule` and `--preload` need `--pool-mode lazy`. A worker refuses to start
  if a task's own resources plus one instance of each pool it uses exceed what `--resources`
  declares, or if the preloaded pools don't fit together.
- **Broken instances:** `DiscardInstance` and `discard_on` close the instance after the outcome is
  reported, as with eager pools, but nothing is rebuilt: the next task that needs the model loads it.
- **Failed loads:** if a factory raises while loading for a task, that task fails with
  `RuntimeError("pool 'gemma' failed to load: ...")`, so `retry_on` applies, and the worker keeps
  running. Instances closed to make room for it stay closed. A failing `--preload` stops the worker
  at startup, as in eager mode.
- **Zombies:** as with eager pools, a task past its hard time limit keeps its instance, so the pool
  shrinks by one for good. Its memory also stays counted as in use until the worker recycles.
- **Close:** `@pool.close` runs on evicted and discarded instances and at shutdown, as above.
- **`linger=`:** `@app.pool("gemma", vram_gb=5.8, linger=30)` keeps an idle instance from being closed
  for room for up to 30 seconds while a workflow still has a task to come that uses the pool, so a
  workflow that returns to a model after a step on another doesn't pay its load twice. The time counts
  from when the instance went idle, not from its last use. Only workflows this worker has fetched a task of
  count, and once the time is up, or no such task is left, the instance is closed like any idle one.
  Tasks that need the room wait meanwhile. `mayu_pool_demand` shows the count. `linger` must be above 0
  and is ignored in eager mode.
- **`--lookahead`:** while a workflow's current task runs, load the model its next task needs
  (`mayu worker ... --pool-mode lazy --lookahead`, or `MAYU_LOOKAHEAD=1`), so the next task finds it
  loaded. It is a low-priority guess: it loads only when the model fits without closing anything,
  the pool has no idle instance and is below its `--pools` size, and the worker has nothing due to
  run. Like `linger`, it knows only the workflows this worker has fetched a task of. A load that fails
  is logged and the task loads the model itself later; that pool isn't tried ahead again for 60 seconds.
  `mayu_pool_lookahead_loads_total` counts the loads. `--lookahead` needs `--pool-mode lazy`. With parked
  pools it also moves parked models back and stages them, see below.

#### Parking models in host RAM

Closing an idle model for room means loading it again from disk later. If the machine has spare RAM, a pool
can instead *park* the idle instance there and bring it back in about a second.

```python
@app.pool("tts", vram_gb=6.9, parked={"ram_gb": 6.6})   # what one parked instance holds
def tts(ctx):
    return load_tts(device="cuda")

@tts.park                       # GPU -> host RAM; the instance object is kept
def _(model): ...

@tts.unpark                     # host RAM -> GPU, in place
def _(model): ...

@tts.stage                      # optional: build straight into the parked state, without using the GPU
def _(ctx): ...                 # returns an instance that is already parked
```

```bash
mayu worker tasks:app --queues gpu --pools gemma=1,tts=1 --pool-mode lazy --schedule throughput --lookahead \
    --resources gpu=1,vram_gb=7.5,ram_gb=16
```

- **Declaring:** `parked=` is what one parked instance holds instead of its normal resources, like
  `{"ram_gb": 6.6}`. Every resource it names must be in `--resources`, so host RAM has an explicit budget
  (mayu counts the amounts you declare and doesn't read host memory). The worker refuses to start if one is
  missing or `parked=` asks for more than the worker has. `parked=` needs both `@pool.park` and `@pool.unpark`,
  and the hooks need `parked=`; each registers once. `--pools tts=1` bounds loaded and parked instances
  together, and `mayu_pool_loaded` counts only the loaded ones. Eager mode ignores all of it, like `linger`.
- **Close:** `@pool.close` also gets parked instances (when one is closed for room, and at shutdown), so it
  must handle both states.
- **Parking and unparking:** a task whose pool has a parked instance unparks it instead of building one. When a
  task needs the room an idle instance of a parkable pool holds, the worker parks that instance if its
  `parked=` amount fits, and otherwise closes it as before. To make that room it may close parked instances
  (picked in the order below), never the one the task is about to unpark. A parked instance is also closed when a task
  needs the resources it holds. Parks and unparks happen before the task's time limits start, like loads. A
  hook that raises is logged: a failed park closes the instance (counted in `mayu_pool_evictions_total`) and a
  failed unpark closes it and builds a fresh one with the factory, so the task runs either way.
- **Which goes first:** with `--lookahead` and at least one pool with `parked=`, idle instances are chosen in
  order of when their pool is next needed (see Estimates): the one needed furthest ahead goes first, and a pool
  no workflow needs goes before any of them, ties by least recently used. Without them it is least recently
  used, as before. Under `--schedule`, a
  task whose missing models are all parked counts a quarter of the loaded-model bonus (15 s under `throughput`,
  2.5 s under `oldest`).
- **Staging:** with `--lookahead`, a pool with `@pool.stage` is built straight into host RAM before a
  workflow needs it, even while the worker is busy, since it uses no GPU. It happens when the pool is next
  needed within its lead time: the stage and unpark times this worker measured plus 5 seconds, or 15 seconds
  until it has measured a stage. The pool must have a workflow task to come, no idle, loading or parked
  instance, room below its `--pools` size, and its `parked=` amount free without closing anything. The usual
  `--lookahead` rule then moves it to the GPU once the worker has nothing due and it fits. A failed stage is
  logged and the task builds the model itself; that pool isn't staged again for 60 seconds.
- **Estimates:** when a pool is next needed comes from the workflow DAG and this worker's own average run time
  of each task (successful runs, weighted towards recent ones). A node that is running counts what is left of
  its average, or what its own [progress reports](#reporting-progress) say is left when it has made enough of
  them (on another worker too); a waiting node starts when its slowest unfinished parent ends, and a task this
  worker has never run counts as instant, which only makes a stage earlier. Like `linger`, it knows only the
  workflows this worker has fetched a task of, and another worker's speed is not known unless its tasks report
  progress. `mayu_pool_next_need_seconds` shows the estimate. When staging starts the worker logs it, like
  `staging pool tts into host RAM: needed in ~38 s (9/12 of its tasks done)`.
- **Metrics:** `mayu_pool_parked`, `mayu_pool_parks_total`, `mayu_pool_unparks_total`,
  `mayu_pool_drops_total`, `mayu_pool_stages_total`, `mayu_pool_move_seconds` and
  `mayu_pool_next_need_seconds` (see [Metrics](#metrics)). They exist only for pools with `parked=`, so a
  worker without them has the same metrics, scheduling and logs as before.

**Torch recipe.** mayu doesn't know about torch; the hooks do the moving. What worked best on an RTX 3070 Ti
laptop (6 GB models, PCIe about 12.5 GB/s): keep a host mirror of every CUDA tensor the model owns, in one
pageable slab registered with `cudaHostRegister`. The first park takes about 1 s, later ones 0.3 s and an
unpark 0.5 s, against 1 to 5 s to reload from a warm page cache and more from a cold disk. Closing the mirror
returns its RAM; tensors from `pin_memory=True` stay in torch's host cache after the model is gone, and a
pinned slab is rounded up to 8 GiB for 6 GB of weights.

```python
import gc
import torch

ALIGN = 512  # every tensor starts on an aligned offset of the slab, so a typed view of it is valid

class Mirror:
    """Host copies of the CUDA tensors a model owns, in one slab. The weights must not change while parked."""

    def __init__(self, device, *modules):
        seen, self.tensors = set(), []
        def add(t):
            if isinstance(t, torch.Tensor) and t.device.type == device and id(t) not in seen:
                seen.add(id(t))
                self.tensors.append(t)
        for m in (m for module in modules for m in module.modules()):
            for t in (*m._parameters.values(), *m._buffers.values()):
                add(t)
                qs = getattr(t, "quant_state", None)  # bitsandbytes 4-bit: its tables are tensors too
                while qs is not None:
                    for name in ("absmax", "code", "offset"):
                        add(getattr(qs, name, None))
                    qs = getattr(qs, "state2", None)
        offsets, size = [], 0
        for t in self.tensors:
            offsets.append(size)
            size += -(-t.numel() * t.element_size() // ALIGN) * ALIGN
        self.slab = torch.zeros(size, dtype=torch.uint8)  # pageable, so release() really gives it back
        torch.cuda.cudart().cudaHostRegister(self.slab.data_ptr(), size, 0)  # page-locked: fast async copies
        self.host = [self.slab[o:o + t.numel() * t.element_size()].view(t.dtype).view(t.shape)
                     for t, o in zip(self.tensors, offsets)]
        for h, t in zip(self.host, self.tensors):
            h.copy_(t, non_blocking=True)
        torch.cuda.synchronize()

    def park(self):
        for t, h in zip(self.tensors, self.host):
            t.data = h
        gc.collect()
        torch.cuda.empty_cache()

    def unpark(self):
        for t, h in zip(self.tensors, self.host):
            t.data = h.to("cuda", non_blocking=True)
        torch.cuda.synchronize()

    def release(self):
        torch.cuda.cudart().cudaHostUnregister(self.slab.data_ptr())
        self.host = self.slab = None

@app.pool("tts", vram_gb=6.9, parked={"ram_gb": 6.6})
def tts(ctx):
    model = load_tts(device="cuda")
    model.mirror = None
    return model

@tts.park
def _(model):
    if model.mirror is None:                      # the first park pays for the copy; later ones just repoint
        model.mirror = Mirror("cuda", model)
    model.mirror.park()

@tts.unpark
def _(model):
    model.mirror.unpark()

@tts.stage
def _(ctx):
    model = load_tts(device="cpu")                # no GPU memory is used while this builds
    model.mirror = Mirror("cpu", model)
    model.mirror.park()                           # the CPU-built weights are dropped; one host copy remains
    return model

@tts.close
def _(model):                                     # runs on idle, parked and discarded instances
    if model.mirror is not None:
        model.mirror.release()
    model.unload()
```

- Park drops the GPU copy only if nothing else holds the weights (a CUDA graph, a cache, an output that still
  has a `grad_fn`), so run inference under `torch.no_grad()`.
- To stage a transformers model, build it on the CPU (`device_map={"": "cpu"}`; pre-quantized bitsandbytes
  4-bit checkpoints load that way too) and pass `Mirror` only the submodules that will live on the GPU.

## GPU monitoring and self-tuning

```bash
pip install "mayu[gpu]"
mayu worker tasks:app --resources gpu=auto,vram_gb=auto --autotune gpu
```

With `mayu[gpu]` installed, the worker reads the GPUs it can see through NVIDIA's NVML every 2
seconds. That feeds the `mayu_gpu_*` metrics, a guard that keeps `vram_gb` honest when other programs
use the card, and optionally the tuner below. Without the extra, or on a machine without a GPU,
nothing changes.

- **`--resources gpu=auto,vram_gb=auto`:** read the amounts from the visible GPUs instead of typing
  them: `gpu` is their number and `vram_gb` their memory in GiB minus 0.5 GiB per GPU, for the CUDA
  context and the desktop (an 8 GiB card gives 7.5). Either can be mixed with plain numbers, like
  `gpu=1,vram_gb=auto`. The worker refuses to start if no GPU is visible.
- **`--gpu-monitor auto|on|off`** (`MAYU_GPU_MONITOR`): `auto` (the default) monitors when NVML works and
  a GPU is visible, `on` refuses to start if it can't, `off` never loads NVML. `off` can't be combined with
  `auto` resources or `--autotune`, which need the readings. `--gpu-interval SECONDS`
  (`MAYU_GPU_INTERVAL`, default 2) sets how often the GPUs are read.
- **Which GPUs:** only the ones `CUDA_VISIBLE_DEVICES` shows, so `--gpus 0,1` gives each process its own
  GPU to monitor. It takes indexes and `GPU-...` UUIDs. NVML numbers GPUs as `nvidia-smi` does, which is
  CUDA's order only with `CUDA_DEVICE_ORDER=PCI_BUS_ID`: on a machine with several different GPUs, set it
  or use UUIDs, or an index can name a different card in CUDA than in the monitor. Other forms (`MIG-...`)
  are an error unless monitoring is off.
- **VRAM guard:** with `vram_gb` declared, the worker compares what the GPUs have in use with what its own
  processes (and their children) use, and treats the difference as other programs' memory. The GPUs'
  memory beyond `vram_gb` is slack that absorbs a small foreign user at no cost; past that, the guard
  takes the excess out of `vram_gb`, so tasks that no longer fit wait instead of running out of memory.
  It shrinks at once and gives memory back only after 3 samples in a row ask for less, so a desktop app
  that comes and goes can't make admission flap. Running tasks and loaded pools are never evicted for it:
  it only holds back new ones. Each change is logged. If NVML can't say which memory is the worker's,
  the guard turns itself off with a warning.
- **Under-declared warning:** if the worker's processes use more VRAM than its running tasks and loaded
  pools declare (by more than 0.5 GiB or 10%) for 3 samples in a row, a warning says to raise `vram_gb=`
  on them, at most once every 5 minutes. mayu can't stop a task from using more than it declared.
- **`--autotune gpu`** (`MAYU_AUTOTUNE=gpu`): run only as many GPU tasks as help. The configured `gpu` is
  the ceiling, the smallest `gpu` request among the tasks the worker serves is the step, and the largest is
  the floor, so the biggest task always fits. Every 5 samples (10 seconds by default) the tuner takes the
  mean GPU utilization. At 90% or more it removes a step of `gpu`, since more concurrency on a saturated GPU
  only adds latency, unless it saw the GPU below 90% one step lower (with tasks waiting, in the last 5
  minutes): then this level gets more done, and it stays. Below 70%, with tasks waiting for `gpu` and
  neither the guard nor the cards' memory (95% in use) under pressure, it adds a step back. After a shrink
  it waits 6 windows before it grows again. Each change is logged. So it settles on the fewest GPU tasks
  that keep the GPU busy.
  It needs a `gpu` capacity, a served task that requests `gpu`, and monitoring.

The GPU metrics are labelled `gpu` (NVML index) and `uuid`. With several GPUs they are per GPU, and the
tuner and the guard use their mean and sum.

## Queue priority

```bash
mayu worker tasks:app --queues high,default,low
```

The order of `--queues` is a strict priority: whenever a worker picks the next task to start,
it prefers `high` tasks over `default` ones. Tasks already claimed into its prefetch buffer, or
already running, are not preempted, and a constantly busy high queue still starves the ones
after it. Without `--queues`, a worker consumes every queue your app uses, at equal priority.

## Callbacks

```python
add.send(2, 3, link=notify.s("sum"))      # after add succeeds: notify(5, "sum")
add.send(2, 3, link=[audit, notify.s("sum")])
```

A `link=` callback runs only after its parent **succeeds**, with the parent's result as its first
argument (even when results are disabled). The callback is enqueued in the same atomic step
that acks the parent, so it is enqueued exactly once per successful parent. Like every task, it
can still run more than once if a worker crashes. Callbacks run on the callback task's queue
and do not chain further; for a DAG of tasks see [Workflows](#workflows).

## Workflows

A DAG of tasks, submitted as one unit. A node runs once all its parents have succeeded.

```python
wf = app.workflow(name="video:42")
plan = wf.add(plan_run.s(prompt, run))
voice = wf.add(voice_lines.s(run), after=[plan])
scenes = [wf.add(render_scene.s(run, s), after=[voice]) for s in ("S1", "S2", "S3")]
wf.add(finalize.s(run), after=scenes)
handle = wf.send()            # WorkflowHandle
handle.status()               # {"state": "running", "waiting": 4, "queued": 1, "done": 1, "dead": 0, ...}
handle.nodes[-1].result(60)   # a TaskHandle per node, in add order
handle.wait(timeout=600)      # "done", "failed", "cancelled" or "expired"
handle.cancel()
```

`wf.add` takes a task or `task.s(...)` with exactly the arguments the task needs; `after=` takes a node
or a list of nodes. `send()` writes the whole DAG and enqueues the nodes with no parents in one atomic
step. When a node succeeds, the step that acks it also enqueues each child whose parents are now all
done, so each child is released exactly once. Nodes still run at least once, like any task.

- Nodes do not receive their parents' results; pass data through your own storage. Nodes can't use
  `link=`, `countdown` or `eta`.
- A node that fails for good (dead-lettered) blocks its descendants and the workflow reports `failed`.
  `mayu dead replay` the node and, once it succeeds, the rest of the DAG continues.
- `handle.cancel()` stops waiting nodes from being released and skips nodes that are queued but not yet
  started (they are acked with a "workflow cancelled" failure). A node already running finishes.
- `app.workflow(name, ttl=7 * 86400)` sets how long Redis keeps the workflow after its last progress.
  If it expires, running nodes still finish but release nothing.
- Release computes stream keys at run time, so workflows, like acks with results, are not
  Redis-Cluster-safe.
- Upgrade workers before submitting workflows: an older worker acks a node but never releases its children.

## Reporting progress

A long task can say how far it is, and mayu turns that into a fraction and a time left.

```python
@app.task(queue="gpu")
def voice_lines(lines, tts=app.use("tts")):
    for i, line in enumerate(lines):
        tts.voice(line)
        mayu.progress(i + 1, len(lines))      # or mayu.progress(0.4) for a fraction

h = voice_lines.send(lines)
h.progress()    # Progress(done=7.0, total=25.0, fraction=0.28, remaining=250.0, worker="laptop-1", updated=...)
```

```console
$ mayu inspect tasks:app
RUNNING (1)
TASK         ID    WORKER    PROGRESS     LEFT    WORKFLOW
voice_lines  01JB  laptop-1  7/25 (28 %)  ~4m10s  01JA
```

- **Reporting:** `mayu.progress(done, total)` needs `0 <= done <= total` and `total > 0`; `mayu.progress(fraction)`
  takes a number from 0 to 1. It returns `True` when recorded, and does nothing and returns `False` outside a
  running task, so library code can call it unconditionally. A wrong call raises `ValueError` (the message names
  the rule) or `TypeError` for a non-number, running or not. Progress may go backwards (a retry inside the task, a
  new phase); the estimate restarts from there. It is for the task body: a task that finishes, however it ends,
  has no progress afterwards.
- **Threads and async:** `mayu.progress` finds its task from the thread it runs on, so a thread the task starts
  can't use it. Call `mayu.progress_reporter()` in the task and give the result to the thread: it has the same
  signature as `progress`, is bound to that task, and does nothing after the task ends. It returns `None` outside
  a task. `async def` tasks call `mayu.progress` directly, each in its own context. Process mode needs nothing
  extra.
- **Reading it:** `TaskHandle.progress()` returns a `Progress` (`done`, `total`, `fraction`, `remaining` in seconds,
  `worker`, `updated` as `time.time()`) or `None` when the task isn't running (not started, finished, or its
  worker is gone). A running task that hasn't reported gives a `Progress` whose numbers are `None`; `total` is
  `None` after a bare fraction. `mayu inspect` lists running tasks in a `RUNNING` section, only while something
  runs, and `--json` has a `running` key with the same fields (`task`, `id`, `worker`, `done`, `total`,
  `fraction`, `remaining_s`, `workflow_id`, `node`, `started_ms`). Each worker entry in `workers` also has its
  `tasks`.
- **Time left:** from the pace between the first report and the latest, once two reports differ by at least 1 %
  and are at least 1 s apart, so model loading before the loop doesn't count as slow work. Before that, from
  the time since the task started, once it is 5 % done. Otherwise it is unknown (`None`, `-` in `inspect`).
  100 % is 0 s.
- **Cost:** `mayu.progress` does no I/O and never blocks; it only updates a small record in the worker. The worker
  publishes the latest values with its heartbeat (every `--heartbeat-interval`, 5 s by default), so a reader is
  up to one interval behind. A worker publishes at most 64 running tasks, oldest started first. Nothing is
  added to tasks that never report. `TaskHandle.progress()` scans the live workers, so use it for an admin
  page or a UI, not in a tight loop; poll no faster than the heartbeat.
- **Metrics:** `mayu_task_progress_reports_total{queue,task}` counts the reports, and shows up after the first
  one. There is no per-task gauge, since task ids would make the label set grow without end. A worker whose
  `tasks` field can't be read is skipped and counted in `mayu_broker_errors_total{op="progress"}`.
- **Lookahead:** with `--lookahead` and a pool with `parked=`, a running workflow node's reported progress
  replaces its average run time in the estimate of when a pool is next needed (see
  [Estimates](#parking-models-in-host-ram)), so staging follows the real position in a loop instead of a guess.
  That includes nodes running on other workers, read from their heartbeats at most once per heartbeat interval,
  and only while a workflow has a queued node this worker isn't running. Without progress reports the estimate
  is the average, as before.

The only addition to the Redis layout is a `tasks` field in the worker's heartbeat hash, removed when the worker
is idle; it is stored as hex text and older workers ignore it.

## Declaring tasks without their code

A producer often can't import a task's dependencies (a GPU model, a heavy library). Declare the task
there with only its name, options and signature, and attach the code in the worker:

```python
# shared module, imported by producers and workers
@app.declare(name="ml.embed", queue="gpu")
def embed(text: str, *, normalize: bool = True): ...

# worker module, imported only by workers that serve the "gpu" queue
@embed.implement
def _(text: str, *, normalize: bool = True):
    return model.encode(text, normalize=normalize)

embed.send("hello")                     # producer: arguments are checked against the declaration
app.send("ml.embed", "hello")           # by name: checked if registered here, sent as-is if not
```

`implement` must match the declaration's parameter names, kinds and defaults (annotations are ignored)
and returns the task. Calling a declared task that isn't implemented raises `TypeError`.

A worker without `--queues` serves only queues where it implements every task (the default queue
included), so it never takes work meant for another worker. With `--queues`, naming a queue that has
an unimplemented task is an error (`mayu: queue 'gpu' has tasks declared but not
implemented in this process: ...`, exit 2). If a delivery for an unimplemented task still arrives, it
fails with "not implemented" instead of running the stub.

## Modes

```bash
mayu worker tasks:app --threads 32 --processes 2      # thread mode: 2 processes × 32 threads
mayu worker tasks:app --mode process --processes 8    # process mode: 8 processes × 1 task each
```

`--mode auto` (default) uses thread mode when the GIL is off after importing your app, and
process mode otherwise, naming the module that re-enabled the GIL. With more than one process,
a supervising parent keeps the workers running, restarts any that crash (with backoff), and
shuts them all down gracefully on SIGTERM. In process mode, a task that exceeds its hard time
limit takes its process down with it, and the process is replaced.

Prefer **a few processes × many threads**: a segfault in a C extension takes down every task in
its process.

## Memory and huge pages

On Linux with transparent huge pages set to `always` (the default on Arch, Fedora and RHEL), a
free-threaded worker can use about 4x the memory it needs: every thread's slice of CPython's
object heap is backed by a 2 MB huge page. Measured with 2 processes × 32 threads, that was
525 MB PSS against 135 MB with huge pages off, at the same throughput. Process mode on a GIL
build is not affected.

```bash
mayu worker tasks:app --thp python    # default; or MAYU_THP=python
```

- `python` (default): on a free-threaded build, opt only Python's own object heap out of huge
  pages, before your app is imported. Memory your tasks allocate themselves (numpy, torch and
  other `malloc`/`mmap` buffers) keeps the system setting.
- `keep`: change nothing.
- `off`: disable huge pages for the whole process, which child processes inherit, including
  tasks that start subprocesses and, under the supervisor, the worker processes.

`WorkerRunner(..., thp="keep")` takes the same modes. A failed system call is logged as a
warning and the worker starts anyway. Linux only; elsewhere every mode does nothing. Limitation:
CPython reserves further heap arenas only after about 1 GB of live Python objects, and those
keep the system setting.

## Metrics

```bash
mayu worker tasks:app --metrics-port 9100      # GET http://host:9100/metrics (Prometheus)
```

Each worker process serves its own metrics: with `--processes 4 --metrics-port 9100`, the
processes listen on 9100–9103, so scrape all four. `MAYU_METRICS_PORT` sets the default;
`--metrics-host` picks the interface (default `0.0.0.0`).

| Metric | Type | Labels |
|---|---|---|
| `mayu_tasks_received_total` | counter | `queue`, `task` |
| `mayu_tasks_settled_total` | counter | `queue`, `task`, `outcome` (`success`, `retry`, `dead`, `cancelled`, `expired`) |
| `mayu_task_duration_seconds` | histogram | `queue`, `task` |
| `mayu_tasks_deferred_total` | counter | `queue`, `task` (rate-limited tasks sent back to Redis) |
| `mayu_tasks_reclaimed_total` | counter | `queue` (taken over from a dead worker) |
| `mayu_broker_errors_total` | counter | `op` |
| `mayu_worker_concurrency`, `mayu_tasks_inflight`, `mayu_tasks_running` | gauge | |
| `mayu_queue_ready`, `mayu_queue_pending`, `mayu_queue_delayed`, `mayu_queue_dead` | gauge | `queue` |
| `mayu_resource_capacity`, `mayu_resource_in_use` | gauge | `resource` (only with `--resources`) |
| `mayu_pool_size`, `mayu_pool_busy` | gauge | `pool` (only with `--pools`; instances in the pool, and held by running tasks) |
| `mayu_pool_rebuilds_total` | counter | `pool` (instances replaced after `DiscardInstance` or `discard_on`) |
| `mayu_pool_loaded` | gauge | `pool` (only with `--pool-mode lazy`; instances in memory, idle or busy) |
| `mayu_pool_loads_total`, `mayu_pool_evictions_total` | counter | `pool` (lazy instances built, preloads included, and closed to make room) |
| `mayu_pool_demand` | gauge | `pool` (only in lazy mode with `--lookahead` or `linger=`; tasks still to come in workflows this worker has seen) |
| `mayu_pool_lookahead_loads_total` | counter | `pool` (lazy instances built by `--lookahead` before their task) |
| `mayu_pool_parked` | gauge | `pool` (only for pools with `parked=`; instances held in host RAM, ones being staged included) |
| `mayu_pool_parks_total`, `mayu_pool_unparks_total` | counter | `pool` (instances moved to host RAM, and moved back to the GPU, ahead of a task or for it) |
| `mayu_pool_drops_total`, `mayu_pool_stages_total` | counter | `pool` (parked instances closed to make room, and instances built straight into host RAM) |
| `mayu_pool_move_seconds` | summary | `pool`, `kind` (`build`, `stage`, `park`, `unpark`; only for pools with `parked=`; `_sum` and `_count`) |
| `mayu_pool_next_need_seconds` | gauge | `pool` (only with `parked=` and `--lookahead`; estimated seconds until the pool is next needed, omitted when no workflow has a task to come for it) |
| `mayu_task_progress_reports_total` | counter | `queue`, `task` (only after a task first calls `mayu.progress`) |
| `mayu_gpu_utilization_ratio` | gauge | `gpu`, `uuid` (0 to 1) |
| `mayu_gpu_memory_used_bytes`, `mayu_gpu_memory_total_bytes` | gauge | `gpu`, `uuid` (used by all processes, and installed) |
| `mayu_gpu_process_memory_bytes` | gauge | `gpu`, `uuid` (used by this worker and its children; omitted if NVML can't say) |
| `mayu_gpu_power_watts`, `mayu_gpu_temperature_celsius` | gauge | `gpu`, `uuid` (omitted if no GPU reports them) |

Pools are counted in the resource ledger as `pool:<name>`, but they appear only as `mayu_pool_*`, never under
`mayu_resource_*`. The `mayu_gpu_*` metrics appear only while GPU monitoring runs (see above).

Queue gauges are read from Redis on each scrape, so every worker reports the same values: use
`max by (queue) (mayu_queue_ready)`. If Redis cannot be read, the scrape still answers without them.

## Logging

```bash
mayu worker tasks:app --log-format json --log-level INFO
```

`text` (default) writes readable lines; `json` writes one object per line on stderr, from both the
Rust core and Python, with `timestamp`, `level`, `message`, `target` and event fields such as
`task_id`, `task`, `queue` and `error`. `MAYU_LOG_FORMAT` and `MAYU_LOG_LEVEL` set the defaults. Failed
tasks are logged at `WARN`, retries at `INFO`, successes at `DEBUG`. When you run `WorkerRunner`
from your own code, call `mayu.configure_logging("json")` first.

## Inspecting and dead letters

```bash
mayu inspect tasks:app                      # queue depths (ready, claimed, delayed, dead), live workers, running tasks, unfinished workflows
mayu inspect tasks:app --json
mayu dead list tasks:app --queue emails     # oldest first: when, task id, task, attempt, error
mayu dead replay tasks:app --queue emails   # run them again
mayu dead replay tasks:app --queue emails --id 01J... --limit 10
```

The running table appears only while a task runs: its progress and time left if it reports any (see
[Reporting progress](#reporting-progress)), as fresh as each worker's last heartbeat. The workflows table
lists those not yet done (id, name, state, node counts, age); finished ones are left out and expire on
their `ttl`.

A replayed task keeps its id, starts again at attempt 0, and its stored failure is cleared, so
`handle.result()` waits for the new outcome. Each dead letter is moved back atomically, so
concurrent replays never enqueue a task twice. A replay only takes the dead letters that exist
when it starts: tasks that fail again during the replay stay dead. Entries whose envelope could
not be decoded are listed but never replayed.

## Tracing

```bash
pip install "mayu[otel]"
```

With OpenTelemetry installed and a tracer provider configured, `send()` creates a `send <task>`
producer span and stores its W3C `traceparent` in the task envelope. The worker runs the task inside a
`process <task>` consumer span that is its child, so spans your task creates (HTTP calls, database
queries) join the caller's trace. Retries, rate-limit deferrals and `link=` callbacks keep the original
trace context. A failed attempt marks its span as an error. Without OpenTelemetry installed, nothing
changes.

## Benchmarks

mayu vs Celery, RQ, Dramatiq and streaQ at 64 concurrent tasks: no-op throughput, a realistic task
(JSON decode + 5 ms I/O + 1 ms CPU), memory at equal concurrency, and workers on Python 3.14t, 3.14 and
3.13 serving the same queues at once. Results and method:
[`bench/RESULTS.md`](https://github.com/Siddid-Soni/mayu/blob/main/bench/RESULTS.md). Reproduce with `bench/setup.sh && bench/all.sh` (needs Docker and uv).

## Development

See [CONTRIBUTING.md](https://github.com/Siddid-Soni/mayu/blob/main/CONTRIBUTING.md) for setup, the test matrix and the pull request checklist.
Coding agents should also read [AGENTS.md](https://github.com/Siddid-Soni/mayu/blob/main/AGENTS.md). The design is in
[`docs/specs/2026-10-11-mayu-architecture-design.md`](https://github.com/Siddid-Soni/mayu/blob/main/docs/specs/2026-10-11-mayu-architecture-design.md).

## License

Licensed under either of [Apache License, Version 2.0](https://github.com/Siddid-Soni/mayu/blob/main/LICENSE-APACHE) or [MIT license](https://github.com/Siddid-Soni/mayu/blob/main/LICENSE-MIT)
at your option. Unless you explicitly state otherwise, any contribution intentionally submitted for
inclusion in mayu by you, as defined in the Apache-2.0 license, shall be dual licensed as above,
without any additional terms or conditions.
