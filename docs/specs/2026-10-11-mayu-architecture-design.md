# mayu — Architecture Design

**Status:** Draft for review
**Date:** 2026-10-11
**Name:** mayu (繭, "silk cocoon" — the source of silk thread)

---

## 1. Intent

### What we're building
An open-source Python task queue whose runtime is written in Rust and whose
workers execute tasks on **real parallel OS threads** under free-threaded
CPython (3.14t+). One worker process replaces many prefork processes.

### Why
Every mainstream Python task queue (Celery, RQ, Dramatiq, TaskIQ, streaQ) is
pure Python and assumes one process per unit of concurrency, or async-only
concurrency. Free-threaded Python makes a new model possible: one process,
many threads, one shared connection pool, one copy of loaded state. No
existing project combines a Rust core with free-threaded thread workers.

### Who it's for
Python teams that run Celery/RQ today and pay for it in memory, worker
start-up time, and operational complexity — and teams with heavy in-process
state (ML models, large caches) that today is duplicated per worker process.

### Success criteria
1. **Correctness:** at-least-once delivery holds under chaos testing
   (`kill -9` of workers, Redis restarts): zero lost tasks.
2. **Memory:** at equal concurrency, materially lower memory than Celery
   prefork (target: ≥5× less for 64-way concurrency).
3. **Throughput:** no-op task throughput competitive with or better than
   Celery/Dramatiq/streaQ on the same Redis.
4. **Adoption:** a Celery user can migrate a typical app mostly by renaming
   imports and decorators.

### Decisions made during brainstorming
| Decision | Choice |
|---|---|
| Goal | Open-source library, wide adoption |
| Broker (v1) | Redis Streams, behind a pluggable `Broker` trait |
| Celery compatibility | Familiar API (`@task`, `.delay`, retries, countdown); own wire protocol |
| Task model | Sync-first on free threads; `async def` supported |
| Rust/Python split | **Rust core runtime** — Rust owns everything except running user code |
| GIL builds | Supported via process-mode fallback |

---

## 2. Architecture overview

```
┌──────────────────────── one worker process (Python 3.14t) ────────────────────────┐
│                                                                                   │
│  Rust core (tokio runtime, a few OS threads)                                      │
│  ┌──────────────┐   ┌─────────────┐   ┌──────────────┐   ┌──────────────────┐     │
│  │ Broker driver│──▶│  Prefetch   │──▶│  Dispatcher  │──▶│ Ready queue      │     │
│  │ (Redis       │   │  buffer +   │   │ (per-queue   │   │ (MPMC channel,   │     │
│  │  Streams)    │◀──│ backpressure│   │  priority,   │   │  bounded)        │     │
│  └──────┬───────┘   └─────────────┘   │  rate limits)│   └────────┬─────────┘     │
│         │ ack / nack / retry          └──────────────┘            │               │
│  ┌──────┴───────┐   ┌─────────────┐   ┌──────────────┐            ▼               │
│  │ Result sink  │◀──│ Completion  │◀──│ Delay/retry  │   Python thread pool       │
│  │              │   │  channel    │   │ scheduler    │   ┌──────────────────┐     │
│  └──────────────┘   └─────────────┘   │ (timer wheel)│   │ T1 T2 T3 … Tn    │     │
│  ┌──────────────┐                     └──────────────┘   │ (sync tasks)     │     │
│  │ Heartbeat +  │                                        │ + 1 asyncio loop │     │
│  │ reclaimer    │                                        │ (async tasks)    │     │
│  └──────────────┘                                        └──────────────────┘     │
└───────────────────────────────────────────────────────────────────────────────────┘
```

**Data flow:** Redis → broker driver → prefetch buffer → dispatcher → ready
queue → Python thread executes → completion channel → result sink → ack/result
written to Redis.

The guiding rule: **Python runs only user code.** Everything else — I/O,
scheduling, acks, retries, timers, recovery — lives in Rust.

---

## 3. Components

### 3.1 Rust core (`mayu-core`, no PyO3 dependency)

| Component | Responsibility | Interface |
|---|---|---|
| **Broker trait** | Abstracts fetch / ack / retry / dead-letter / promote / reclaim / heartbeat. The only component aware of the broker. | `trait Broker` (async) |
| **Prefetch buffer** | Caps in-flight messages at `threads × prefetch_multiplier`; applies backpressure so busy workers stop claiming. | Pulls from `Broker`, feeds dispatcher |
| **Dispatcher** | Decodes envelope **header only**; applies queue priority and per-task rate limits (token bucket); routes to ready queue (sync) or async queue. | Consumes prefetch, produces `ReadyTask` |
| **Ready queue** | Bounded MPMC channel of `ReadyTask` (header + raw payload bytes). | `recv()` callable from Python with GIL detached |
| **Completion channel** | Carries `Outcome { id, Success(bytes) \| Failure(err, tb) \| Retry(countdown) }` from threads to Rust. | `send()` from Python |
| **Result sink** | Translates outcomes into atomic broker operations (ack+result, ack+retry, ack+dead-letter). | Consumes completion channel |
| **Delay/retry scheduler** | Timer wheel driving promotion of due delayed tasks and backoff timing. | Internal |
| **Heartbeat + reclaimer** | Publishes worker liveness; extends ownership of long-running messages; reclaims messages abandoned by dead workers. | Internal |
| **Supervisor** | Thread mode: manages Python thread pool, replaces zombie threads. Process mode: forks/monitors child processes. | Internal |

### 3.2 Redis driver (`mayu-redis`)
Implements `Broker` with Redis Streams + consumer groups. All multi-step state
transitions are Lua scripts so they are atomic.

### 3.2a In-process broker (`memory://`, embedded mode)
`mayu-core`'s `MemoryBroker` (an `Arc<Mutex<State>>` plus a `Notify` that wakes blocked fetches) is a faithful
in-memory model of the Redis driver: the same atomic transitions, each under one lock instead of one Lua script,
the same dead-letter entry ids and replay, and result, heartbeat and workflow TTLs that expire as the Redis keys
would (lazily on read, plus a sweep when the map is touched). Besides testing the runtime, it backs embedded mode:
`mayu-py` holds a `Backend` enum (`Redis(RedisBroker) | Memory(MemoryBroker)`) that implements `Broker` by static
dispatch, and a `memory://` or `memory://<name>` URL picks a `MemoryBroker` from a process-wide registry keyed by
name, so a producer `Client` and a thread-mode `Worker` in the same process share its state. Nothing crosses the
process boundary: `mayu worker`, `inspect` and `dead` refuse such an app, process mode is refused, and queued
work is lost when the process exits. Other URLs take the Redis path unchanged.

### 3.3 Python bindings (`mayu-py`, PyO3)
- Module declared free-threading safe (PyO3 ≥ 0.28 default; no `gil_used`).
- Exposes the ready queue `recv()` (detaches from the interpreter while
  blocking), completion `send()`, and worker bootstrap.
- Wraps all Rust entry points in `catch_unwind`.

### 3.4 Python package (`python/mayu`)
`App`, `@app.task`, `TaskHandle`, serializers, CLI (`mayu worker`,
`mayu inspect`, `mayu dead replay`), the thread-worker loop, and the asyncio
loop host.

**Thread worker loop (conceptual):**
```python
while running:
    task = ready.recv()                 # blocks in Rust, interpreter detached
    fn = registry[task.name]
    try:
        result = fn(*decode(task.payload))
        complete.send(Success(task.id, encode(result)))
    except Retry as r:
        complete.send(RetryReq(task.id, r.countdown))
    except BaseException as e:
        complete.send(Failure(task.id, e, traceback))
```

Async tasks are dispatched to a single dedicated asyncio loop thread per
worker process via `run_coroutine_threadsafe`; their outcomes use the same
completion channel.

---

## 4. Message lifecycle & delivery guarantees

### 4.1 Guarantee
**At-least-once.** A message is acked only after the task finishes. Crashes
cause re-execution, never loss. Tasks must be idempotent; the docs state this
prominently. Exactly-once is explicitly not offered.

### 4.2 Envelope (msgpack)
- **Header:** `id` (ULID), `task`, `queue`, `attempt`, `max_retries`, `eta`,
  `created_at`, `trace_ctx` (W3C traceparent), `content_type`.
- **Payload:** args + kwargs encoded with a pluggable serializer — msgpack
  (default), JSON, or pickle (**opt-in only**; documented as unsafe with
  untrusted producers).
- Rust decodes only the header. Payload stays bytes until a Python thread
  decodes it.

### 4.3 Redis layout (per queue `q`)
| Key | Type | Purpose |
|---|---|---|
| `mayu:{q}:stream` | Stream + consumer group `mayu` | Ready tasks |
| `mayu:{q}:delayed` | Sorted set, score = eta (ms) | Countdown / ETA / retry backoff |
| `mayu:{q}:dead` | Stream | Dead letters (retries exhausted, poison) |
| `mayu:result:{id}` | String, TTL = `result_ttl` | Result or error (only if results enabled) |
| `mayu:workers:{worker_id}` | Hash, TTL = 3 × heartbeat interval | Liveness + stats |

### 4.4 Lifecycle
1. **Enqueue:** `XADD` to stream, or `ZADD` to delayed set if `eta > now`.
2. **Promote:** a Rust loop runs a Lua script that atomically moves due
   entries from `delayed` to `stream` (`ZRANGEBYSCORE` + `XADD` + `ZREM` in
   one script), so concurrent workers never double-promote.
3. **Claim:** `XREADGROUP` places the message in this consumer's pending
   entries list (PEL).
4. **Execute:** a Python thread runs the task. For tasks still running at
   `visibility_timeout / 2`, Rust re-claims them to itself
   (`XCLAIM … JUSTID`) to reset idle time so they are not stolen.
5. **Complete** (one Lua script each):
   - success → `XACK` (+ `SET result EX ttl` if enabled)
   - retry → `XACK` + `ZADD delayed` with `attempt+1`, exponential backoff
     with full jitter
   - exhausted → `XACK` + `XADD dead`
   - Each script first checks that the settling consumer still owns the entry
     (it is in the PEL under that consumer). If another worker reclaimed it, the
     script changes nothing and returns 0, and the worker counts a stale settle.
     (A stale ack still stores its result, but only if none exists: `SET NX`.)
6. **Recover:** each worker's reclaimer periodically runs `XAUTOCLAIM` with
   `min-idle-time = visibility_timeout` and re-executes what it claims.

### 4.5 Ordering
No ordering guarantee across workers. Not attempted in v1.

### 4.6 Graceful shutdown
On SIGTERM: stop fetching → wait for in-flight tasks up to `shutdown_grace` →
exit. Unacked messages stay in the PEL and are reclaimed by other workers.

---

## 5. Failure handling

### 5.1 Time limits
Python threads cannot be killed. The design is explicit about this.

- **Soft limit:** Rust triggers `SoftTimeLimitExceeded` in the task thread
  via `PyThreadState_SetAsyncExc`. Delivered at the next bytecode boundary —
  **cannot interrupt blocking C calls or socket reads.** Tasks may catch it to
  clean up.
- **Hard limit, thread mode:** the thread is abandoned as a *zombie*. The
  message is recorded as failed (retried or dead-lettered per policy) and a
  replacement thread is started. If the zombie later finishes, its outcome is
  discarded (the completion channel drops outcomes for ids no longer tracked).
  When zombies exceed `max_zombie_threads` (default 4), the worker performs a
  graceful restart.
- **Hard limit, process mode:** the child process is killed (Celery parity).
- Documentation directs users to set timeouts on network clients, which is
  the real fix in thread mode.

### 5.2 Failure matrix
| Failure | Response |
|---|---|
| Task raises | Retry if exception matches `retry_on` and attempts remain; else dead-letter. Traceback stored in result. |
| Undecodable envelope (poison) | Dead-letter immediately, no retry. |
| Redis disconnect | Pause fetching; reconnect with backoff; in-flight tasks continue; completions buffered (bounded) and flushed on reconnect. Lost acks → duplicate execution (allowed by at-least-once). |
| Worker crash (segfault, OOM, `kill -9`) | In-flight messages reclaimed by other workers after `visibility_timeout`. |
| Rust panic | `catch_unwind` at PyO3 boundary; core panics fail fast — process exits non-zero for the supervisor/orchestrator to restart. |
| Memory growth | `max_tasks_per_process` / `max_memory_per_process` trigger graceful restart. |
| GIL re-enabled by an import | `--mode auto` falls back to process mode and logs the offending module. |

### 5.3 Blast radius
In thread mode a segfault in a C extension kills every in-flight task in that
process. Recommended deployment: **a few processes × many threads** (e.g.
2 × 32 instead of 1 × 64).

### 5.4 Thread-unsafe user code
`@app.task(threadsafe=False)` routes the task to a single dedicated thread,
executing serially. Escape hatch for legacy code.

---

## 6. Public API

```python
from mayu import App

app = App(broker="redis://localhost:6379/0", result_ttl=3600)

@app.task(queue="emails", max_retries=5, retry_on=(TimeoutError,),
          soft_time_limit=30, hard_time_limit=60, rate_limit="100/s")
def send_email(to: str, subject: str) -> str:
    ...

@app.task
async def fetch(url: str) -> bytes:          # runs on the worker's asyncio loop
    ...

send_email.delay("a@b.com", "hi")                       # Celery-style alias
h = send_email.send("a@b.com", "hi", countdown=10)      # options as kwargs
h.result(timeout=5)                                     # only if results enabled
```

**Deliberate differences from Celery**
- Arguments are validated against the function signature at enqueue time.
- No canvas (chain/chord/group) in v1; simple `link=` success callback only.
- `send(..., **options)` replaces `apply_async`; `.delay` kept as alias.
- Retrying from inside a task: `raise mayu.Retry(countdown=...)`.

**CLI**
```bash
mayu worker myapp:app --threads 32 --processes 2 --queues emails,default
mayu worker myapp:app --mode process --processes 8   # GIL builds
mayu inspect myapp:app                               # depth, delayed, dead, workers
mayu dead replay myapp:app --queue emails
```
`--mode` is `auto` (default), `thread`, or `process`. `auto` selects thread
mode iff the GIL is disabled after importing the app module.

**Configuration:** constructor arguments, overridable by `MAYU_*` environment
variables. No settings-module discovery.

**Observability:** structured JSON logs; Prometheus metrics endpoint
(`--metrics-port`); OpenTelemetry trace context propagated in the envelope.

---

## 7. Scope

### In v1
Redis Streams broker; sync + async tasks; thread and process modes; retries
with backoff; countdown/ETA; dead letters + replay; results with TTL; soft and
hard time limits; rate limits; queue priority across queues; metrics, logs,
tracing; `inspect` CLI.

### Out of v1
Canvas workflows; periodic/beat scheduling (planned v1.1); other brokers
(Postgres planned next); Celery wire compatibility; web dashboard;
exactly-once semantics; ordering guarantees.

---

## 8. Repository layout

```
mayu/
├── crates/
│   ├── mayu-core/        # broker trait, prefetch, dispatcher, scheduler, reclaimer
│   ├── mayu-redis/       # Redis Streams driver + Lua scripts
│   └── mayu-py/          # PyO3 bindings
├── python/mayu/          # App, @task, handles, CLI, serializers
├── tests/
│   ├── rust/             # unit + property tests
│   ├── python/           # API + end-to-end tests
│   └── chaos/            # kill -9, Redis restart, partitions
├── bench/                # vs Celery / RQ / Dramatiq / streaQ
└── docs/
```
Built with maturin. `mayu-core` has no PyO3 dependency so it is testable in
plain Rust and reusable by future bindings.

---

## 9. Testing strategy

- **Rust unit + property tests** (`proptest`): timer wheel, backoff, prefetch
  accounting, envelope decoding.
- **Driver tests against real Redis** (testcontainers): every Lua script,
  `XAUTOCLAIM` reclaim, promotion races between 2+ workers.
- **Python end-to-end:** enqueue → execute → result; retries; dead letters;
  async tasks; soft/hard limits; zombie replacement.
- **Free-threading stress:** full suite on 3.14t with `PYTHON_GIL=0` under
  `pytest-run-parallel`; CI fails if any import re-enables the GIL.
- **Chaos (the guarantee test):** enqueue 100k tasks, randomly `kill -9`
  workers and restart Redis; assert every task executed ≥1 time and none are
  lost.
- **CI matrix:** 3.14t (thread mode), 3.14 and 3.13 (process mode);
  Linux + macOS.

### Published benchmarks
1. No-op task throughput.
2. Realistic task throughput (JSON decode + simulated I/O + CPU work).
3. Memory at equal concurrency: mayu 2 × 32 threads vs Celery 64 prefork.

---

## 10. Key risks

| Risk | Mitigation |
|---|---|
| Ecosystem C extensions not free-threading safe | `auto` mode fallback; GIL-reenable detection; process-mode parity |
| Users' code not thread-safe | Docs; `threadsafe=False` escape hatch; process mode |
| Uninterruptible threads | Soft limits + zombie replacement + restart cap; process mode for hard kills |
| Redis Streams memory growth | Periodic `XTRIM MINID` to just below the oldest pending entry (from `XPENDING`), so unacked messages are never trimmed |
| Celery users expect canvas/beat | Clear roadmap; beat in v1.1 |
