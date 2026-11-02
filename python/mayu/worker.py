import asyncio
import collections
import logging
import math
import queue
import signal
import threading
import time
import traceback

from . import _native, otel
from . import gpu as gpu_mod
from . import thp as thp_mod
from .exceptions import DiscardInstance, Retry, SoftTimeLimitExceeded, TimeLimitExceeded
from .limits import TimeLimits
from .pool import PoolContext, check_pool_name
from .progress import _async_current, _slot_local
from .recycle import rss_mb
from .serializers import for_content_type
from .task import check_resources

log = logging.getLogger("mayu.worker")

_RECV_TIMEOUT_MS = 100
_SUPERVISOR_TICK = 0.05
LOOKAHEAD_POLL = 0.25  # seconds between asks for a model to load ahead; the dispatcher backs a failed pool off itself
# How long a task waits for a parked instance the ledger counts but another thread is still parking.
PARKED_WAIT = 30.0


def _hostport(host: str, port: int) -> str:
    return f"[{host}]:{port}" if ":" in host else f"{host}:{port}"


def _ms(seconds: float) -> int:
    return max(1, int(seconds * 1000))


async def _in_span(span, coro, native, token):
    _async_current.set((native, token))  # in the task's own context: `mayu.progress` finds its task
    with otel.activate(span):
        return await coro


async def _with_time_limits(coro, soft: float | None, hard: float | None):
    limit = soft if soft is not None else hard
    if limit is None:
        return await coro
    cm = asyncio.timeout(limit)
    try:
        async with cm:
            return await coro
    except TimeoutError:
        if not cm.expired():
            raise  # the task's own TimeoutError
        if soft is not None:
            raise SoftTimeLimitExceeded(f"soft time limit ({soft}s) exceeded") from None
        raise TimeLimitExceeded(f"hard time limit ({hard}s) exceeded") from None


class _BadLink(Exception):
    """A link entry is malformed; the original error is the cause."""


class _Slot:
    """One task-running OS thread. A zombie's thread is abandoned: never joined, given no new work."""

    def __init__(self, name: str, serial: bool = False, native=None):
        self.name = name
        self.serial = serial
        self.native = native  # what `mayu.progress` reports through
        self.token = None  # the task body running on this thread, set only around the call
        self.thread: threading.Thread | None = None
        self.ident: int | None = None
        self.zombie = False


class _PoolState:
    """The live instances of one pool. The lock guards only the free lists, never user code.

    An instance is in exactly one place: busy (held by a task), `free` (idle), `parked`, or in the hands of the
    one thread that is moving it (building, parking, unparking, closing). Lazy pools only: `parked`."""

    def __init__(self, pool, size: int, lazy: bool = False):
        self.pool = pool
        self.lazy = lazy
        self.target = size  # what ctx.size says
        self.size = size  # instances the pool is expected to have; shrinks when one is lost for good
        self.free: collections.deque = collections.deque()  # (index, instance), least recently returned first
        self.indexes = set(range(size)) if lazy else set()  # lazy pools: indexes no instance has right now
        self.lock = threading.Lock()
        self.parked: collections.deque = collections.deque()  # (index, instance), least recently parked first
        self.parked_added = threading.Condition(self.lock)

    def push(self, index: int, instance) -> None:
        with self.lock:
            self.free.append((index, instance))

    def take(self):
        with self.lock:
            return self.free.popleft() if self.free else None

    def take_recent(self):
        """Lazy pools: the instance returned last (its weights are the most likely to still be warm)."""
        with self.lock:
            return self.free.pop() if self.free else None

    def take_lru(self):
        """Lazy pools: the instance returned longest ago, the one to evict."""
        with self.lock:
            return self.free.popleft() if self.free else None

    def push_parked(self, index: int, instance) -> None:
        with self.parked_added:
            self.parked.append((index, instance))
            self.parked_added.notify_all()

    def take_parked(self, wait: float = 0.0):
        """The instance parked last (the one to unpark). The ledger counts an instance as parked when a task is
        planned to park it, so one may still be on its way: wait up to `wait` seconds for it."""
        with self.parked_added:
            self.parked_added.wait_for(lambda: self.parked, wait)
            return self.parked.pop() if self.parked else None

    def take_parked_lru(self, wait: float = 0.0):
        """The instance parked longest ago (the one to drop); see `take_parked`."""
        with self.parked_added:
            self.parked_added.wait_for(lambda: self.parked, wait)
            return self.parked.popleft() if self.parked else None

    def take_index(self) -> int | None:
        with self.lock:
            if not self.indexes:
                return None
            index = min(self.indexes)
            self.indexes.discard(index)
            return index

    def give_index(self, index: int) -> None:
        with self.lock:
            self.indexes.add(index)

    def drain(self) -> list:
        with self.lock:
            out = list(self.free)
            self.free.clear()
        return out

    def drain_parked(self) -> list:
        with self.lock:
            out = list(self.parked)
            self.parked.clear()
        return out


class _Plan(collections.namedtuple("_Plan", "loads evicts drops parks unparks")):
    """What the dispatcher decided to do to the pools for one task (lazy mode), in the order it is carried out:
    drops, evicts, parks, unparks, loads. One entry per instance."""

    __slots__ = ()

    def __bool__(self) -> bool:
        return any(self)


_NO_PLAN = _Plan((), (), (), (), ())


class _Held(collections.namedtuple("_Held", "param state index instance")):
    """A pool instance checked out by a task."""

    __slots__ = ()


class WorkerRunner:
    """Runs tasks on real OS threads; Rust owns fetching, acks, retries and timers."""

    def __init__(self, app, *, threads: int, queues: list[str] | None = None,
                 prefetch_multiplier: int = 2, shutdown_grace: float = 30.0,
                 visibility_timeout: float = 30.0, heartbeat_interval: float = 5.0,
                 max_deliveries: int = 5, max_zombie_threads: int = 4,
                 max_tasks_per_process: int | None = None, max_memory_per_process: float | None = None,
                 metrics_port: int | None = None, metrics_host: str = "0.0.0.0",
                 resources: dict[str, float] | None = None, pools: dict[str, int] | None = None,
                 thp: str = "python", pool_mode: str = "eager", schedule: str | None = None,
                 preload: list[str] | None = None, gpu_monitor: str = "auto", gpu_interval: float = 2.0,
                 autotune: str | None = None, lookahead: bool = False):
        if threads < 1:
            raise ValueError("threads must be >= 1")
        if visibility_timeout <= 0 or heartbeat_interval <= 0:
            raise ValueError("visibility_timeout and heartbeat_interval must be positive")
        if max_deliveries < 1:
            raise ValueError("max_deliveries must be >= 1")
        if max_zombie_threads < 0:
            raise ValueError("max_zombie_threads must be >= 0")
        if max_tasks_per_process is not None and max_tasks_per_process < 1:
            raise ValueError("max_tasks_per_process must be >= 1")
        if max_memory_per_process is not None and max_memory_per_process <= 0:
            raise ValueError("max_memory_per_process must be positive (MB)")
        if metrics_port is not None and not 0 <= metrics_port <= 65535:
            raise ValueError("metrics_port must be between 0 and 65535")
        if pool_mode not in ("eager", "lazy"):
            raise ValueError(f"pool_mode must be 'eager' or 'lazy', got {pool_mode!r}")
        if schedule not in (None, "throughput", "oldest"):
            raise ValueError(f"schedule must be 'throughput' or 'oldest', got {schedule!r}")
        if pool_mode != "lazy" and schedule is not None:
            raise ValueError("--schedule needs --pool-mode lazy")
        if pool_mode != "lazy" and preload:
            raise ValueError("--preload needs --pool-mode lazy")
        if pool_mode != "lazy" and lookahead:
            raise ValueError("--lookahead needs --pool-mode lazy")
        if gpu_monitor not in ("auto", "on", "off"):
            raise ValueError(f"gpu_monitor must be 'auto', 'on' or 'off', got {gpu_monitor!r}")
        if isinstance(gpu_interval, bool) or not isinstance(gpu_interval, (int, float)) or not gpu_interval > 0 \
                or math.isinf(gpu_interval):
            raise ValueError(f"gpu_interval must be a positive number of seconds, got {gpu_interval!r}")
        if autotune not in (None, "gpu"):
            raise ValueError(f"autotune must be 'gpu' (the only resource mayu can tune), got {autotune!r}")
        if autotune and gpu_monitor == "off":
            raise ValueError("--autotune gpu reads the GPU's utilization through NVML, which --gpu-monitor off "
                             "turns away; use --gpu-monitor auto or on, or drop --autotune")
        self.thp = thp_mod.check(thp)
        self.app = app
        self.threads = threads
        if queues:
            for q in queues:
                stubs = sorted(t.name for t in app.registry.values() if t.queue == q and not t.implemented)
                if stubs:
                    raise ValueError(f"queue {q!r} has tasks declared but not implemented in this process: "
                                     f"{', '.join(stubs)}; import their implementations or don't serve that queue")
        self.queues = list(queues or app.queues(implemented=True))
        if not self.queues:
            raise ValueError("no queue to serve: every queue has tasks declared but not implemented in this process; "
                             "import their implementations or pass queues")
        self.gpu_interval = float(gpu_interval)
        self._gpu_backend = self._plan_gpu_monitor(gpu_monitor, resources)
        if autotune and self._gpu_backend is None:
            if gpu_mod.backend() is None:
                raise ValueError("--autotune gpu needs NVIDIA's NVML: pip install 'mayu[gpu]'")
            raise ValueError("--autotune gpu found no readable GPU; check CUDA_VISIBLE_DEVICES")
        if gpu_mod.has_auto(resources):
            resources = gpu_mod.resolve_auto(resources)
            log.info("resolved resources from the visible GPUs: %s",
                     ", ".join(f"{n}={v:g}" for n, v in resources.items() if n in gpu_mod.AUTO_RESOURCES))
        self.resources = check_resources("resources", resources, allow_zero=True)
        self.lazy = pool_mode == "lazy"
        left = self._plan_pools(pools)
        self.lazy_pools = self._plan_preload(preload)
        self.lazy = self.lazy and bool(self._loaded)  # a lazy worker with no pools is an ordinary one
        self.schedule = (schedule or "oldest") if self.lazy else None
        self.lookahead = bool(lookahead) and self.lazy  # like schedule: nothing to do for a worker without pools
        parkable = [self.app.pools[p] for p in self.lazy_pools if self.app.pools[p].parked] if self.lazy else []
        self._parkable = bool(parkable)  # false: no park, unpark or stage is ever asked of, or reported by, this worker
        self._stageable = any(p.stager is not None for p in parkable)
        self.task_resources = self._served_requests(left)
        self.capacity = {**left, **{f"pool:{p}": float(n) for p, n in self._loaded.items()}}
        self._tuner = self._plan_autotune(autotune)
        self.strict_priority = bool(queues)  # only a non-empty explicit list is a priority order
        self.prefetch_multiplier = prefetch_multiplier
        self.shutdown_grace = shutdown_grace
        self.visibility_timeout = visibility_timeout
        self.heartbeat_interval = heartbeat_interval
        self.max_deliveries = max_deliveries
        self.max_zombie_threads = max_zombie_threads
        self.max_tasks_per_process = max_tasks_per_process
        self.max_memory_per_process = max_memory_per_process
        self.metrics_port_requested = metrics_port
        self.metrics_host = metrics_host
        self.restart_reason: str | None = None
        self._restart_lock = threading.Lock()
        self._settled_count = 0
        self._settled_lock = threading.Lock()
        self._next_memory_check = 0.0
        self.native = None
        self._sampler: gpu_mod.Sampler | None = None
        self._lookahead_stop = threading.Event()
        self._lookahead_thread: threading.Thread | None = None
        self._running = threading.Event()
        self._stopped = threading.Event()
        self._halt = threading.Event()
        self._slots: list[_Slot] = []
        self._slots_lock = threading.Lock()
        self._slot_seq = 0
        self._serial_q: queue.Queue = queue.Queue()
        self._serial_lock = threading.Lock()
        self._limits = TimeLimits()
        self._supervisor: threading.Thread | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._loop_thread: threading.Thread | None = None
        self._pending_async: set = set()
        self._pending_lock = threading.Lock()
        self._spans: dict = {}
        self._spans_lock = threading.Lock()
        self._states: dict[str, _PoolState] = {}  # loaded pools in load order; only start() adds to it
        self._checked_out: dict = {}  # token -> [_Held], so the hard limit can find what a zombie holds
        self._held_lock = threading.Lock()

    def _plan_gpu_monitor(self, mode: str, resources):
        """The backend the GPU sampler will read, or None when monitoring is off or unavailable (nothing is
        imported then). `auto` resources need the GPUs, so they turn `auto` into `on` and clash with `off`."""
        if mode == "off":
            if gpu_mod.has_auto(resources):
                raise ValueError("resources with 'auto' read the GPUs through NVML, which --gpu-monitor off turns "
                                 "away; use --gpu-monitor auto or on, or give the resources numbers")
            return None
        be = gpu_mod.backend()
        if be is None:
            if mode == "on":
                raise ValueError("--gpu-monitor on needs NVIDIA's NVML: pip install 'mayu[gpu]'")
            return None
        try:
            found = bool(be.devices())
        except Exception as e:
            if mode == "on":
                raise ValueError(f"--gpu-monitor on could not read the GPUs: {e!r}") from e
            log.debug("GPU monitoring is off: could not read the GPUs: %r", e)
            return None
        if not found:
            if mode == "on":
                raise ValueError("--gpu-monitor on found no visible GPU; check CUDA_VISIBLE_DEVICES")
            return None
        return be

    def _plan_autotune(self, autotune: str | None) -> gpu_mod.Tuner | None:
        """The `gpu` tuner: the capacity the worker has is the ceiling, the smallest served request the step and the
        largest the floor (the biggest task must still fit)."""
        if not autotune:
            return None
        asks = [r["gpu"] for r in self.task_resources.values() if "gpu" in r]
        if "gpu" not in self.capacity:
            raise ValueError("--autotune gpu needs a gpu resource to tune: pass --resources gpu=...")
        if not asks:
            raise ValueError("--autotune gpu needs a served task that requests gpu: declare "
                             "@app.task(resources={'gpu': ...}) or drop --autotune")
        return gpu_mod.Tuner(self.capacity["gpu"], max(asks), min(asks))

    def _plan_pools(self, pools) -> dict[str, float]:
        """Checks `pools` against the served tasks; returns what each declared resource has left after reserving
        room for the loaded pools. Sets `self._loaded` ({pool: instances}, in `pools` order) and `self._reserved`."""
        if pools is None:
            pools = {}
        shape = "pools must be a dict like {'gemma': 2} (pool name -> number of instances)"
        if not isinstance(pools, dict):
            raise TypeError(f"{shape}, got {type(pools).__name__}")
        for name, n in pools.items():
            check_pool_name(name)
            if isinstance(n, bool) or not isinstance(n, int):
                raise TypeError(f"{shape}; {name!r} must be a whole number, got {n!r}")
            if n < 0:
                raise ValueError(f"pool {name!r} cannot have {n} instances")
        registered = self.app.pools
        used: set[str] = set()
        for t in sorted(self.app.registry.values(), key=lambda t: t.name):
            if t.queue not in self.queues:
                continue
            for p in sorted(set(t.pools.values())):
                if p not in registered:
                    raise ValueError(f"task {t.name!r} uses pool {p!r}, which this process does not define; "
                                     f"import the module with @app.pool({p!r})")
                if not pools.get(p):
                    raise ValueError(f"task {t.name!r} uses pool {p!r} but this worker has no instances of it; "
                                     f"pass --pools {p}=N")
                used.add(p)
        for name, n in pools.items():
            if n and name not in registered:
                raise ValueError(f"--pools names pool {name!r}, which this process does not define; "
                                 f"import the module with @app.pool({name!r})")
            if n and name not in used:
                log.warning("pool %r is sized but no task on the served queues uses it; not loading it", name)
        self._sized = {p: n for p, n in pools.items() if n}
        self._loaded = {p: n for p, n in pools.items() if n and p in used}
        reserved: dict[str, int] = {}  # milli-units, like the dispatcher
        parts: dict[str, list[str]] = {}
        for p, n in self._loaded.items():
            for r, amount in registered[p].resources.items():
                if r not in self.resources:
                    raise ValueError(f"pool {p!r} needs {r!r} per instance but this worker declares no such "
                                     f"resource; pass --resources {r}=...")
                if not self.lazy:  # a lazy pool holds its resources only while an instance is loaded
                    reserved[r] = reserved.get(r, 0) + n * round(amount * 1000)
                    parts.setdefault(r, []).append(f"{p} {n} × {amount:g}")
        left = {}
        for r, cap in self.resources.items():
            milli = round(cap * 1000) - reserved.get(r, 0)
            if milli < 0:
                raise ValueError(f"pools reserve {r}={reserved[r] / 1000:g} ({', '.join(parts[r])}) but this "
                                 f"worker has {cap:g}; lower --pools or raise --resources {r}=...")
            left[r] = milli / 1000
        self._reserved = reserved
        return left

    def _plan_preload(self, preload) -> dict[str, tuple[dict[str, float], int]]:
        """Checks `preload`; returns the lazy pools as {pool: (resources per instance, instances preloaded)}."""
        if preload is None:
            preload = []
        if isinstance(preload, str) or not all(isinstance(n, str) for n in preload):
            raise TypeError(f"preload must be a list of pool names like ['gemma'], got {preload!r}")
        names = list(preload)
        registered = self.app.pools
        for name in names:
            check_pool_name(name)
            if names.count(name) > 1:
                raise ValueError(f"--preload lists pool {name!r} more than once")
            if name not in registered:
                raise ValueError(f"--preload names pool {name!r}, which this process does not define; "
                                 f"import the module with @app.pool({name!r})")
            if name not in self._sized:
                raise ValueError(f"--preload names pool {name!r} but this worker has no instances of it; "
                                 f"pass --pools {name}=N")
            if name not in self._loaded:
                raise ValueError(f"--preload names pool {name!r}, which no task on the served queues uses")
        total: dict[str, int] = {}  # milli-units
        for name in names:
            for r, amount in registered[name].resources.items():
                total[r] = total.get(r, 0) + round(amount * 1000)
        for r, milli in sorted(total.items()):
            if milli > round(self.resources[r] * 1000):
                raise ValueError(f"preloading {', '.join(names)} needs {r}={milli / 1000:g} but this worker has "
                                 f"{r}={self.resources[r]:g}; preload fewer pools or raise --resources {r}=...")
        self._preload = names
        if not self.lazy:
            return {}
        self._check_parked()
        return {p: (dict(registered[p].resources), 1 if p in names else 0) for p in self._loaded}

    def _check_parked(self) -> None:
        """Lazy mode: a pool with `parked=` needs its hooks and room declared, and hooks need `parked=`."""
        for name in self._loaded:
            pool = self.app.pools[name]
            if pool.parked is None:
                for hook, fn in (("park", pool.parker), ("unpark", pool.unparker), ("stage", pool.stager)):
                    if fn is not None:
                        raise ValueError(f"pool {name!r} has a {hook} function but no parked=; declare what a "
                                         f"parked instance holds, like app.pool({name!r}, parked={{'ram_gb': 6.6}})")
                continue
            for hook, fn in (("park", pool.parker), ("unpark", pool.unparker)):
                if fn is None:
                    raise ValueError(f"pool {name!r} has parked= but no {hook} function; register one with "
                                     f"the pool's @{hook} decorator")
            for r in pool.parked:
                if r not in self.resources:
                    raise ValueError(f"pool {name!r} parks into {r} but this worker has no {r}; "
                                     f"add --resources {r}=...")
                if round(pool.parked[r] * 1000) > round(self.resources[r] * 1000):
                    raise ValueError(f"pool {name!r} parks {r}={pool.parked[r]:g} but this worker has "
                                     f"{r}={self.resources[r]:g}; raise --resources {r}=... or park less")

    def _served_requests(self, left: dict[str, float]) -> dict[str, dict[str, float]]:
        """Requests of the tasks on served queues (plus one `pool:` slot per pool used), checked against `left`."""
        out = {}
        for t in sorted(self.app.registry.values(), key=lambda t: t.name):
            if t.queue not in self.queues or not (t.resources or t.pools):
                continue
            for name, amount in sorted(t.resources.items()):
                if name not in self.resources:
                    raise ValueError(f"task {t.name!r} requests {name!r} but this worker declares no such resource; "
                                     f"pass --resources {name}=...")
                if round(amount * 1000) > round(left[name] * 1000):
                    after = (f" left after pools reserve {self._reserved[name] / 1000:g}"
                             if name in self._reserved else "")
                    raise ValueError(
                        f"task {t.name!r} requests {name}={amount:g} but this worker has "
                        f"{name}={left[name]:g}{after}; raise --resources {name}=... "
                        f"or route the task to a bigger worker")
            if self.lazy and t.pools:  # its own request plus every model it needs in memory at once
                self._check_lazy_fit(t)
            out[t.name] = {**t.resources, **{f"pool:{p}": 1.0 for p in t.pools.values()}}
        return out

    def _check_lazy_fit(self, t) -> None:
        need: dict[str, int] = {r: round(a * 1000) for r, a in t.resources.items()}
        parts: dict[str, list[str]] = {}
        for p in sorted(set(t.pools.values())):
            for r, amount in self.app.pools[p].resources.items():
                need[r] = need.get(r, 0) + round(amount * 1000)
                parts.setdefault(r, []).append(f"{p} {amount:g}")
        for r, milli in sorted(need.items()):
            if milli > round(self.resources[r] * 1000):
                if r in t.resources:
                    parts.setdefault(r, []).append(f"{t.resources[r]:g} of its own")
                raise ValueError(
                    f"task {t.name!r} needs {r}={milli / 1000:g} ({' + '.join(parts[r])}) but this worker has "
                    f"{r}={self.resources[r]:g}; raise --resources {r}=... or route the task to a bigger worker")

    @property
    def worker_id(self) -> str | None:
        return None if self.native is None else self.native.worker_id

    @property
    def metrics_port(self) -> int | None:
        """The port `/metrics` is served on once started (the real one when 0 was requested)."""
        return None if self.native is None else self.native.metrics_port

    def start(self) -> None:
        thp_mod.apply(self.thp)  # before any thread exists, so every heap starts in the advised arena
        rate_limits = {t.name: t.rate_per_sec for t in self.app.registry.values() if t.rate_per_sec is not None}
        self._load_pools()  # before the native worker: it starts fetching as soon as it exists
        try:
            self.native = self._start_native(rate_limits)
        except BaseException:
            self._close_pools()
            raise
        for name in self._preload if self.lazy else ():  # the dispatcher already counts them (lazy_pools)
            self.native.pool_loaded(name)
        if self._gpu_backend is not None:
            self._sampler = gpu_mod.Sampler(self.native, self._gpu_backend, self.gpu_interval,
                                            vram_gb=self.resources.get("vram_gb"), tuner=self._tuner)
            self._sampler.start()
        if self.lookahead:
            self._lookahead_thread = threading.Thread(target=self._lookahead_main, name="mayu-lookahead", daemon=True)
            self._lookahead_thread.start()
        self._loop = asyncio.new_event_loop()
        self._loop_thread = threading.Thread(target=self._loop.run_forever, name="mayu-async", daemon=True)
        self._loop_thread.start()
        self._running.set()
        for _ in range(self.threads):
            self._start_slot()
        self._supervisor = threading.Thread(target=self._supervise, name="mayu-supervisor", daemon=True)
        self._supervisor.start()
        log.info("worker started", extra={"worker_id": self.worker_id, "queues": self.queues, "threads": self.threads})

    def __enter__(self) -> "WorkerRunner":
        self.start()
        return self

    def __exit__(self, *exc) -> None:
        self.stop()
        self.join()

    def _start_native(self, rate_limits):
        return _native.Worker(
            self.app.broker_url, self.queues, self.threads, self.prefetch_multiplier, self.app.result_ttl,
            visibility_timeout_ms=_ms(self.visibility_timeout),
            reclaim_interval_ms=_ms(min(5.0, self.visibility_timeout / 2)),
            heartbeat_interval_ms=_ms(self.heartbeat_interval),
            max_deliveries=self.max_deliveries,
            strict_priority=self.strict_priority,
            rate_limits=rate_limits,
            resources=self.capacity or None,
            task_resources=self.task_resources or None,
            lazy_pools={p: self._lazy_args(self.app.pools[p], claim, loaded)
                        for p, (claim, loaded) in self.lazy_pools.items()} or None,
            schedule=self.schedule,
            lookahead=self.lookahead,
            metrics_addr=(None if self.metrics_port_requested is None
                          else _hostport(self.metrics_host, self.metrics_port_requested)),
        )

    @staticmethod
    def _lazy_args(pool, claim, loaded) -> tuple:
        if not pool.parked:  # the plain form: a pool without parked= is told nothing new
            return claim, loaded, pool.linger
        return claim, loaded, pool.linger, dict(pool.parked), pool.stager is not None

    def stop(self) -> None:
        self._running.clear()
        self._lookahead_stop.set()
        if self._sampler is not None:
            self._sampler.stop()
        if self.native is not None:
            self.native.stop_fetching()
        self._stopped.set()

    def join(self) -> None:
        deadline = time.monotonic() + self.shutdown_grace
        # Zombie threads are never waited for; a thread may turn zombie while we wait.
        while time.monotonic() < deadline:
            busy = [s for s in self._live_slots() if s.thread.is_alive()]
            if not busy:
                break
            busy[0].thread.join(min(0.05, max(0.0, deadline - time.monotonic())))
        while time.monotonic() < deadline:
            with self._pending_lock:
                if not self._pending_async:
                    break
            time.sleep(0.01)
        self._halt.set()
        self._supervisor.join(1.0)
        if self._sampler is not None:
            self._sampler.stop()
            self._sampler.join()  # before the native worker goes: a tick uses it
        if self._lookahead_thread is not None:  # what it is building or staging still gets closed by _close_pools
            self._lookahead_stop.set()
            self._lookahead_thread.join(max(0.0, deadline - time.monotonic()))
        remaining_ms = int(max(0.0, deadline - time.monotonic()) * 1000)
        self.native.shutdown(max(remaining_ms, 1000))
        self._close_pools()  # what threads past the grace period still hold is not closed
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._loop_thread.join(1.0)
        if not self._loop_thread.is_alive():
            self._loop.close()
        log.info("worker stopped", extra={"worker_id": self.worker_id})

    def run(self) -> str | None:
        """Runs until stopped. Returns why the worker stopped to recycle itself, or None."""
        self.start()
        if threading.current_thread() is threading.main_thread():
            for sig in (signal.SIGTERM, signal.SIGINT):
                signal.signal(sig, lambda *_: self.stop())
        while not self._stopped.wait(0.5):
            pass
        self.join()
        return self.restart_reason

    # -- pools ---------------------------------------------------------------

    def _load_pools(self) -> None:
        """Builds every instance of every loaded pool, one at a time, in `pools` order then index order.

        A lazy worker builds only the `preload` pools, one instance each, in the order given."""
        if self.lazy:
            for name, n in self._loaded.items():
                self._states[name] = _PoolState(self.app.pools[name], n, lazy=True)
            for name in self._preload:
                state = self._states[name]
                index = state.take_index()
                started = time.monotonic()
                try:
                    instance = state.pool.factory(PoolContext(name, index, state.target))
                except Exception as e:
                    self._close_pools()
                    raise RuntimeError(f"pool {name!r} instance {index} failed to load: {e!r}") from e
                state.push(index, instance)
                log.info("pool instance loaded", extra={"pool": name, "index": index, "size": state.target,
                                                        "load_seconds": round(time.monotonic() - started, 3)})
            return
        for name, n in self._loaded.items():
            pool = self.app.pools[name]
            state = self._states[name] = _PoolState(pool, n)
            for index in range(n):
                started = time.monotonic()
                try:
                    instance = pool.factory(PoolContext(name, index, n))
                except Exception as e:
                    self._close_pools()
                    raise RuntimeError(f"pool {name!r} instance {index} failed to load: {e!r}") from e
                state.push(index, instance)
                log.info("pool instance loaded", extra={"pool": name, "index": index, "size": n,
                                                        "load_seconds": round(time.monotonic() - started, 3)})

    def _close_pools(self) -> None:
        """Closes the idle and parked instances, last loaded first."""
        for state in reversed(list(self._states.values())):
            for _, instance in sorted(state.drain() + state.drain_parked(), key=lambda entry: entry[0], reverse=True):
                self._close_instance(state.pool, instance)

    @staticmethod
    def _close_instance(pool, instance) -> None:
        if pool.closer is None:
            return
        try:
            pool.closer(instance)
        except Exception:
            log.exception("closing an instance of pool %r failed", pool.name)

    def _checkout(self, token, task, plan=_NO_PLAN) -> list[_Held]:
        if self.lazy:
            return self._checkout_lazy(token, task, plan)
        held: list[_Held] = []
        try:
            for param, name in task.pools.items():
                state = self._states[name]
                entry = state.take()
                if entry is None:  # the ledger admitted this task, so an instance must be free
                    raise RuntimeError(f"pool {name!r} has no free instance; this is a mayu bug")
                held.append(_Held(param, state, *entry))
        except Exception:
            for h in held:
                h.state.push(h.index, h.instance)
            raise
        with self._held_lock:
            self._checked_out[token] = held
        return held

    def _checkout_lazy(self, token, task, plan) -> list[_Held]:
        """Carries out the dispatcher's plan for this task: drops, evictions, parks, unparks, then loads, and takes
        the resident instances.

        The dispatcher already counts every move as done, so on failure each load that was not built is undone
        (`unload_begin` + `unload_done`), and what was built, unparked or taken goes back to its pool: the
        dispatcher's counts always equal the instances this process has.
        """
        pending = list(plan.loads)  # loads the dispatcher counted that are not built yet
        built: dict[str, tuple] = {}  # pool -> (index, instance) built or unparked for this task, not taken yet
        held: list[_Held] = []
        try:
            failed = None
            for closing, names in ((self._drop, plan.drops), (self._evict, plan.evicts)):
                for name in names:  # all of them, even after a failure: the dispatcher counts them as gone
                    try:
                        closing(name)
                    except Exception as e:
                        log.exception("could not close an instance of pool %r to make room", name)
                        failed = failed or e
            for name in plan.parks:
                self._park(name)  # a failed park closes the instance instead; the task is not affected
            for name in plan.unparks:
                entry = self._unpark(name)
                if entry is None:  # closed; the dispatcher counts a loaded instance, so build a fresh one
                    self._unpark_failed(name)
                    pending.append(name)
                else:
                    built[name] = entry
            if failed is not None:
                raise failed
            while pending:
                built[pending[0]] = self._load(pending[0])
                pending.pop(0)
            for param, name in task.pools.items():
                state = self._states[name]
                entry = built.pop(name, None) or state.take_recent()
                if entry is None:  # the ledger admitted this task, so an idle instance must exist
                    raise RuntimeError(f"pool {name!r} has no free instance; this is a mayu bug")
                held.append(_Held(param, state, *entry))
        except BaseException:
            for name in pending:
                self._unload_unbuilt(name)
            for name, (index, instance) in built.items():
                self._states[name].push(index, instance)
            for h in held:
                h.state.push(h.index, h.instance)
            raise
        if held:
            with self._held_lock:
                self._checked_out[token] = held
        return held

    def _evict(self, name: str) -> None:
        """Closes the idle instance of `name` that was returned longest ago."""
        state = self._states[name]
        entry = state.take_lru()
        if entry is None:
            raise RuntimeError(f"pool {name!r} has no idle instance to evict; this is a mayu bug")
        index, instance = entry
        self._close_instance(state.pool, instance)
        state.give_index(index)  # after the close: nothing may reuse the index while the old instance lives
        self.native.pool_evicted(name)
        log.info("pool instance evicted", extra={"pool": name, "index": index})

    def _drop(self, name: str) -> None:
        """Closes the parked instance of `name` that was parked longest ago."""
        state = self._states[name]
        entry = state.take_parked_lru(PARKED_WAIT)
        if entry is None:
            raise RuntimeError(f"pool {name!r} has no parked instance to drop; this is a mayu bug")
        index, instance = entry
        self._close_instance(state.pool, instance)
        state.give_index(index)
        self.native.pool_dropped(name)
        log.info("pool instance dropped from host RAM", extra={"pool": name, "index": index})

    def _park(self, name: str) -> None:
        """Moves the idle instance of `name` that was returned longest ago to the parked state. Never raises: a
        park hook that fails gets the instance closed instead."""
        state = self._states[name]
        entry = state.take_lru()
        if entry is None:
            log.error("pool %r has no idle instance to park; this is a mayu bug", name, extra={"pool": name})
            self._park_failed(name)
            return
        index, instance = entry
        started = time.monotonic()
        try:
            state.pool.parker(instance)
        except Exception:
            log.exception("parking an instance of pool %r failed; closing it instead", name, extra={"pool": name})
            self._close_instance(state.pool, instance)
            state.give_index(index)
            self._park_failed(name)
            self.native.pool_evicted(name)
            return
        secs = time.monotonic() - started
        state.push_parked(index, instance)
        self.native.pool_parked(name)
        self._report_move(name, "park", secs)
        log.info("pool %s parked in %.1f s", name, secs, extra={"pool": name, "index": index,
                                                               "park_seconds": round(secs, 3)})

    def _unpark(self, name: str):
        """Moves a parked instance of `name` back; returns (index, instance), or None if that failed (the instance
        is closed and its index given back; the caller tells the dispatcher)."""
        state = self._states[name]
        entry = state.take_parked(PARKED_WAIT)
        if entry is None:
            log.error("pool %r has no parked instance to unpark; this is a mayu bug", name, extra={"pool": name})
            return None
        index, instance = entry
        started = time.monotonic()
        try:
            state.pool.unparker(instance)
        except Exception:
            log.exception("unparking an instance of pool %r failed; closing it", name, extra={"pool": name})
            self._close_instance(state.pool, instance)
            state.give_index(index)
            return None
        secs = time.monotonic() - started
        self.native.pool_unparked(name)
        self._report_move(name, "unpark", secs)
        log.info("pool %s unparked in %.1f s", name, secs, extra={"pool": name, "index": index,
                                                                 "unpark_seconds": round(secs, 3)})
        return entry

    def _report_move(self, name: str, kind: str, secs: float) -> None:
        """Tells the dispatcher how long a move took (parkable pools only: it sets how early a stage is due)."""
        if not self.app.pools[name].parked:
            return
        try:
            self.native.pool_timing(name, kind, secs)
        except Exception:
            log.exception("could not report the %s time of pool %r", kind, name)

    def _park_failed(self, name: str) -> None:
        try:
            self.native.park_failed(name)
        except Exception:
            log.exception("could not report a failed park of pool %r", name)

    def _unpark_failed(self, name: str) -> None:
        try:
            self.native.unpark_failed(name)
        except Exception:
            log.exception("could not report a failed unpark of pool %r", name)

    def _load(self, name: str) -> tuple:
        state = self._states[name]
        index = state.take_index()
        if index is None:
            raise RuntimeError(f"pool {name!r} has no free index to load into; this is a mayu bug")
        try:
            return index, self._build(name, state, index)
        except Exception as e:
            raise RuntimeError(f"pool {name!r} failed to load: {e!r}") from e

    def _build(self, name: str, state: _PoolState, index: int):
        """Builds one lazy instance into `index` and tells the dispatcher; on failure the index is given back."""
        started = time.monotonic()
        try:
            instance = state.pool.factory(PoolContext(name, index, state.target))
        except BaseException:
            state.give_index(index)
            raise
        self.native.pool_loaded(name)
        secs = time.monotonic() - started
        self._report_move(name, "build", secs)
        log.info("pool instance loaded", extra={"pool": name, "index": index, "size": state.target,
                                                "load_seconds": round(secs, 3)})
        return instance

    def _lookahead_main(self) -> None:
        """Stages the models the dispatcher says are due, and builds or unparks the ones it grants ahead of the
        tasks that will need them."""
        while not self._lookahead_stop.is_set():
            if self._stageable:  # before a load: staging uses no GPU, so it goes ahead even when the GPU is busy
                try:
                    staged = self.native.stage_begin()
                except Exception:
                    log.exception("asking for a model to stage failed")
                    staged = None
                if staged is not None:
                    self._lookahead_stage(staged)
                    continue
            try:
                grant = self.native.lookahead_begin()
            except Exception:
                log.exception("asking for a model to load ahead failed")
                grant = None
            if grant is None:
                self._lookahead_stop.wait(LOOKAHEAD_POLL)
                continue
            kind, name = grant
            if kind == "build":
                self._lookahead_load(name)
            elif kind == "unpark":
                self._lookahead_unpark(name)
            else:
                log.error("lookahead granted a %r of pool %r, which this worker cannot do; this is a mayu bug",
                          kind, name, extra={"pool": name})
                self._lookahead_done(name, False)

    def _lookahead_stage(self, name: str) -> None:
        """Builds an instance of `name` straight into the parked state (host RAM, no GPU)."""
        state = self._states[name]
        index = state.take_index()
        if index is None:  # the ledger granted a stage that has no index to go into
            log.error("staging pool %r has no free index to build into; this is a mayu bug", name,
                      extra={"pool": name})
            self._stage_done(name, False)
            return
        need = self.native.pool_need(name)
        if need is None:
            log.info("staging pool %s into host RAM", name, extra={"pool": name, "index": index})
        else:
            secs, done, total = need
            log.info("staging pool %s into host RAM: needed in ~%.0f s (%d/%d of its tasks done)",
                     name, secs, done, total, extra={"pool": name, "index": index, "needed_in": round(secs, 1)})
        started = time.monotonic()
        try:
            instance = state.pool.stager(PoolContext(name, index, state.target))
        except Exception as e:
            log.warning("staging pool %r failed (%r); the task that needs it will load it itself", name, e,
                        extra={"pool": name})
            state.give_index(index)
            self._stage_done(name, False)
            return
        secs = time.monotonic() - started
        state.push_parked(index, instance)  # before the ledger counts it usable: a task admitted then must find it
        self.native.pool_staged(name)
        self._report_move(name, "stage", secs)
        log.info("pool %s staged in %.1f s", name, secs, extra={"pool": name, "index": index,
                                                               "stage_seconds": round(secs, 3)})
        self._stage_done(name, True)

    def _stage_done(self, name: str, ok: bool) -> None:
        try:
            self.native.stage_done(name, ok)
        except Exception:
            log.exception("could not report the staging of pool %r", name)

    def _lookahead_unpark(self, name: str) -> None:
        entry = self._unpark(name)
        if entry is None:
            log.warning("lookahead unpark of pool %r failed; the task that needs it will load it itself", name,
                        extra={"pool": name})
            self._lookahead_done(name, False)
            return
        self._states[name].push(*entry)  # before the ledger counts it idle: a task admitted then must find it
        self._lookahead_done(name, True)

    def _lookahead_load(self, name: str) -> None:
        state = self._states[name]
        index = state.take_index()
        if index is None:  # the ledger granted a load that has no index to go into
            log.error("lookahead load of pool %r has no free index to load into; this is a mayu bug", name,
                      extra={"pool": name})
            self._lookahead_done(name, False)
            return
        try:
            instance = self._build(name, state, index)
        except Exception as e:
            log.warning("lookahead load of pool %r failed (%r); the task that needs it will load it itself", name, e,
                        extra={"pool": name})
            self._lookahead_done(name, False)
            return
        self.native.pool_lookahead_loaded(name)
        state.push(index, instance)  # before the ledger counts it idle: a task admitted then must find it
        self._lookahead_done(name, True)

    def _lookahead_done(self, name: str, ok: bool) -> None:
        try:
            self.native.lookahead_done(name, ok)
        except Exception:
            log.exception("could not report the lookahead load of pool %r", name)

    def _unload_unbuilt(self, name: str) -> None:
        """Takes back a load the dispatcher counted but nothing was built for."""
        try:
            self.native.unload_begin(name)
            self.native.unload_done(name)
        except Exception:
            log.exception("could not undo a planned load of pool %r", name)

    def _abandon(self, plan) -> None:
        """A planned task failed before it could run: still carry out its drops, evictions and parks, move its
        unparked instances back (the dispatcher counts them as loaded), and undo its loads."""
        for closing, names in ((self._drop, plan.drops), (self._evict, plan.evicts)):
            for name in names:
                try:
                    closing(name)
                except Exception:
                    log.exception("could not close an instance of pool %r to make room", name)
        for name in plan.parks:
            self._park(name)
        for name in plan.unparks:
            entry = self._unpark(name)
            if entry is None:
                self._unpark_failed(name)
                self._unload_unbuilt(name)
            else:
                self._states[name].push(*entry)
        for name in plan.loads:
            self._unload_unbuilt(name)

    @staticmethod
    def _is_broken(h: _Held, error: BaseException) -> bool:
        if isinstance(error, DiscardInstance) and error.pool in (None, h.state.pool.name):
            return True
        return isinstance(error, h.state.pool.discard_on)

    def _release(self, token, held: list[_Held], outcome) -> list[_Held]:
        """Returns the healthy instances to their pools and takes the broken ones' slots out of the ledger.

        Runs before the outcome is reported: reporting releases the task's `pool:` slots, so the instances
        must already be back and the broken ones' slots already gone, or another task could be admitted
        with no instance to take.
        """
        with self._held_lock:
            self._checked_out.pop(token, None)
        broken = [] if outcome[0] else [h for h in held if self._is_broken(h, outcome[1])]
        for h in held:
            if not any(h is b for b in broken):
                h.state.push(h.index, h.instance)
        for h in broken:
            if self.lazy:  # the instance stays in memory until it is closed, after the report
                self._unload_begin(h.state.pool.name)
            else:
                self._adjust(f"pool:{h.state.pool.name}", -1.0)
        return broken

    def _unload_begin(self, name: str) -> bool:
        try:
            self.native.unload_begin(name)
        except Exception:
            log.exception("could not mark an instance of pool %r as unloading", name)
            return False
        return True

    def _rebuild(self, h: _Held) -> None:
        """Closes a broken instance and builds its replacement; runs after the outcome was reported."""
        pool, state = h.state.pool, h.state
        self._close_instance(pool, h.instance)
        if self.lazy:  # no replacement: the next task that needs this model loads one
            try:
                self.native.unload_done(pool.name)
            except Exception:
                log.exception("could not release the memory of a discarded instance of pool %r", pool.name)
            state.give_index(h.index)
            log.info("pool instance unloaded", extra={"pool": pool.name, "index": h.index})
            return
        started = time.monotonic()
        try:
            fresh = pool.factory(PoolContext(pool.name, h.index, state.target))
        except Exception:
            log.exception("rebuilding instance %d of pool %r failed; the pool is one smaller", h.index, pool.name)
            self._pool_shrunk(state)
            return
        state.push(h.index, fresh)  # before growing the ledger, so checkout never finds the list empty
        self._adjust(f"pool:{pool.name}", 1.0)
        self.native.pool_rebuilt(pool.name)
        log.info("pool instance rebuilt", extra={"pool": pool.name, "index": h.index,
                                                 "load_seconds": round(time.monotonic() - started, 3)})

    def _forfeit(self, token) -> None:
        """A zombie keeps its instances (its thread may still use them): the pool just gets smaller."""
        with self._held_lock:
            held = self._checked_out.pop(token, None)
        for h in held or ():
            self._adjust(f"pool:{h.state.pool.name}", -1.0)
            if self.lazy:  # never unload_done: the zombie may still be using the memory
                self._unload_begin(h.state.pool.name)
            self._pool_shrunk(h.state)

    def _pool_shrunk(self, state: _PoolState) -> None:
        with state.lock:
            state.size -= 1
            size = state.size
        if size == 0:
            self._request_restart(f"pool {state.pool.name!r} has no instances left")

    def _adjust(self, resource: str, delta: float) -> None:
        try:
            self.native.adjust_capacity(resource, delta)
        except Exception:
            log.exception("could not adjust the capacity of %s by %+g", resource, delta)

    # -- recycling -----------------------------------------------------------

    def _request_restart(self, reason: str) -> None:
        with self._restart_lock:
            if self.restart_reason is not None:
                return
            self.restart_reason = reason
        log.warning("recycling worker: %s", reason, extra={"reason": reason})
        self.stop()

    def _settled(self) -> None:
        if self.max_tasks_per_process is None:
            return
        with self._settled_lock:
            self._settled_count += 1
            n = self._settled_count
        if n >= self.max_tasks_per_process:
            self._request_restart(f"max_tasks_per_process ({self.max_tasks_per_process}) reached")

    def _check_recycle(self) -> None:
        with self._slots_lock:
            zombies = sum(1 for s in self._slots if s.zombie and s.thread is not None and s.thread.is_alive())
        if zombies > self.max_zombie_threads:
            self._request_restart(f"{zombies} zombie threads exceed max_zombie_threads ({self.max_zombie_threads})")
        now = time.monotonic()
        if self.max_memory_per_process is not None and now >= self._next_memory_check:
            self._next_memory_check = now + 1.0
            rss = rss_mb()
            if rss is not None and rss > self.max_memory_per_process:
                self._request_restart(
                    f"memory {rss:.0f} MB exceeds max_memory_per_process ({self.max_memory_per_process:g} MB)")

    # -- threads -------------------------------------------------------------

    def _start_slot(self, serial: bool = False) -> _Slot:
        with self._slots_lock:
            kind = "serial" if serial else "worker"
            slot = _Slot(f"mayu-{kind}-{self._slot_seq}", serial=serial, native=self.native)
            self._slot_seq += 1
            self._slots.append(slot)
        target = self._serial_main if serial else self._thread_main
        slot.thread = threading.Thread(target=target, args=(slot,), name=slot.name, daemon=True)
        slot.thread.start()
        return slot

    def _ensure_serial(self) -> None:
        """Starts the serial thread unless a live one exists (never two at once)."""
        with self._serial_lock:
            with self._slots_lock:
                alive = any(s.serial and not s.zombie for s in self._slots)
            if not alive:
                self._start_slot(serial=True)

    def _live_slots(self) -> list[_Slot]:
        with self._slots_lock:
            return [s for s in self._slots if not s.zombie and s.thread is not None]

    def _thread_main(self, slot: _Slot) -> None:
        slot.ident = threading.get_ident()
        _slot_local.slot = slot
        native = self.native
        while self._running.is_set() and not slot.zombie:
            ready = native.recv(_RECV_TIMEOUT_MS)
            if ready is not None:
                self._execute(ready, slot)

    def _serial_main(self, slot: _Slot) -> None:
        slot.ident = threading.get_ident()
        _slot_local.slot = slot
        while not slot.zombie:
            try:
                token, task, serializer, args, kwargs, links, plan = self._serial_q.get(
                    timeout=_RECV_TIMEOUT_MS / 1000)
            except queue.Empty:
                if not self._running.is_set():
                    return  # drained after stop
                continue
            self._run_sync(token, task, serializer, args, kwargs, slot, links, plan)

    def _supervise(self) -> None:
        while not self._halt.wait(_SUPERVISOR_TICK):
            try:
                for run in self._limits.tick():
                    try:
                        self._on_hard_limit(run)
                    except Exception:
                        log.exception("worker supervisor step failed")  # one failed report must not stop the others
                self._check_recycle()
            except Exception:
                log.exception("worker supervisor step failed")  # the supervisor must outlive a bad tick

    def _on_hard_limit(self, run) -> None:
        task, slot = run.context
        slot.zombie = True
        if task.pools:
            try:
                self._forfeit(run.token)  # before the failure is reported, as for a rebuild
            except Exception:
                log.exception("worker supervisor step failed")
        try:
            # Decide before reporting: once clients see the failure they may submit new work, and a
            # worker that is about to recycle must already have stopped fetching.
            self._check_recycle()
        except Exception:
            log.exception("worker supervisor step failed")
        try:
            self._report_error(run.token, task, TimeLimitExceeded(f"hard time limit ({run.hard}s) exceeded"))
        finally:
            if self._running.is_set():
                # keep capacity even if the report failed
                if slot.serial:
                    self._ensure_serial()
                else:
                    self._start_slot()

    # -- execution -----------------------------------------------------------

    def _plan_of(self, ready) -> _Plan:
        if not self._parkable:  # nothing can park here: don't even ask
            return _Plan(ready.loads, ready.evicts, (), (), ())
        return _Plan(ready.loads, ready.evicts, ready.drops, ready.parks, ready.unparks)

    def _execute(self, ready, slot: _Slot) -> None:
        # what the dispatcher decided to do to the pools for this task (lazy pools only)
        plan = self._plan_of(ready) if self.lazy else _NO_PLAN
        task = self.app.registry.get(ready.task)
        if task is None:
            self._abandon(plan)
            self.native.complete_failure(ready.token, f"unknown task {ready.task!r}", "")
            self._settled()
            return
        if not task.implemented:  # declared elsewhere; running the stub would record a bogus success
            self._abandon(plan)
            self.native.complete_failure(
                ready.token, f"task {ready.task!r} is declared but not implemented in this worker", "")
            self._settled()
            return
        span = otel.start_process(ready)
        if span is not None:
            with self._spans_lock:
                self._spans[ready.token] = span
        planned = bool(plan)  # true until something carries the plan out
        try:
            serializer = for_content_type(ready.content_type, allow_pickle=self.app.allow_pickle)
            args, kwargs, *extra = serializer.loads(ready.payload)
            links = extra[0].get("links", ()) if extra else ()
            if task.is_async:
                if planned:  # an async task uses no pool, but it may have to make room
                    planned = False
                    self._checkout(ready.token, task, plan)
                self._submit_async(ready.token, task, serializer, args, kwargs, links)
                return
        except BaseException as e:
            if planned:
                self._abandon(plan)
            self._report_error(ready.token, task, e)
            return
        if not task.threadsafe:
            self._ensure_serial()
            self._serial_q.put((ready.token, task, serializer, args, kwargs, links, plan))
            return
        self._run_sync(ready.token, task, serializer, args, kwargs, slot, links, plan)

    def _span(self, token):
        with self._spans_lock:
            return self._spans.get(token)

    def _end_span(self, token, error: BaseException | None = None) -> None:
        with self._spans_lock:
            span = self._spans.pop(token, None)
        otel.end(span, error)

    def _run_sync(self, token, task, serializer, args, kwargs, slot: _Slot, links=(), plan=_NO_PLAN) -> None:
        """Exactly one outcome per task, and a soft-limit exception never leaves this method.

        The soft-limit exception can land on any bytecode of this thread from the moment the run is
        armed until `disarm` returns (which also clears one that is still pending). It is delivered
        at most once per run, so one catch-and-resume loop covers that whole span; every step in it
        is idempotent (`arm` returns the registered run, `outcome` is kept, `disarm` can be re-run).
        """
        run = outcome = owned = held = None
        armed = settled = False
        # before the limits are armed: waiting for or building an instance is not the task's time
        if task.pools or plan:  # in lazy mode a task without pools can still evict one to make room
            try:
                held = self._checkout(token, task, plan)
            except Exception as e:
                self._report_error(token, task, e)
                return
            kwargs = {**kwargs, **{h.param: h.instance for h in held}}
        # The span is attached before the run is armed and detached after it is disarmed, so the
        # soft-limit exception (only delivered while armed) never lands inside attach/detach.
        with otel.activate(self._span(token)):
            while not settled:
                try:
                    if not armed:
                        run = self._limits.arm(token, slot.ident, task.soft_time_limit, task.hard_time_limit,
                                               context=(task, slot))
                        armed = True
                    if outcome is None:
                        slot.token = token
                        try:
                            outcome = (True, task.fn(*args, **kwargs))
                        except BaseException as e:
                            outcome = (False, e)
                        slot.token = None
                    owned = run is None or self._limits.disarm(run)
                    settled = True
                except SoftTimeLimitExceeded as e:
                    slot.token = None  # the limit can land between the body and the clear above
                    if outcome is None:
                        outcome = (False, e)  # the limit fired before/around the task body
                    # otherwise it landed after the task finished: drop it and finish settling
        if not owned:
            slot.zombie = True  # the hard limit already settled this task; this thread retires
            return
        broken = () if held is None else self._release(token, held, outcome)
        ok, value = outcome
        if ok:
            self._report_success(token, serializer, value, links)
        else:
            self._report_error(token, task, value)
        for h in broken:
            self._rebuild(h)

    def _submit_async(self, token, task, serializer, args, kwargs, links=()) -> None:
        coro = _in_span(self._span(token),
                        _with_time_limits(task.fn(*args, **kwargs), task.soft_time_limit, task.hard_time_limit),
                        self.native, token)
        fut = asyncio.run_coroutine_threadsafe(coro, self._loop)
        with self._pending_lock:
            self._pending_async.add(fut)
        fut.add_done_callback(lambda f: self._async_done(token, task, serializer, links, f))

    def _async_done(self, token, task, serializer, links, fut) -> None:
        with self._pending_lock:
            self._pending_async.discard(fut)
        try:
            result = fut.result()
        except BaseException as e:
            self._report_error(token, task, e)
        else:
            self._report_success(token, serializer, result, links)

    def _report_success(self, token, serializer, result, links=()) -> None:
        try:
            data = None if self.app.result_ttl is None else serializer.dumps(result)
        except BaseException as e:
            self._end_span(token, e)
            self.native.complete_failure(token, f"result not serializable: {e!r}", traceback.format_exc())
            self._settled()
            return
        try:
            # link callbacks get the result first; Rust enqueues them atomically with the ack
            follow_ups = [self._follow_up(serializer, result, cb) for cb in links]
        except _BadLink as e:
            self._end_span(token, e)
            self.native.complete_failure(token, f"bad link callback: {e.__cause__!r}", traceback.format_exc())
            self._settled()
            return
        except BaseException as e:
            self._end_span(token, e)
            self.native.complete_failure(token, f"result not serializable: {e!r}", traceback.format_exc())
            self._settled()
            return
        self._end_span(token)
        try:
            self.native.complete_success(token, data, follow_ups)
        except (TypeError, ValueError, OverflowError) as e:
            # argument extraction failed before reaching Rust, so no outcome was recorded yet
            self.native.complete_failure(token, f"bad link callback: {e!r}", traceback.format_exc())
        self._settled()

    @staticmethod
    def _follow_up(serializer, result, cb) -> tuple:
        try:
            task, queue = str(cb["task"]), str(cb["queue"])
            max_retries = int(cb["max_retries"])
            if not 0 <= max_retries <= 2**32 - 1:
                raise ValueError(f"link max_retries out of range: {max_retries}")
            args, kwargs = cb["args"], cb["kwargs"]
            args = [result, *args]
        except (KeyError, TypeError, ValueError, OverflowError) as e:
            raise _BadLink() from e
        payload = bytes(serializer.dumps([args, kwargs]))
        return (task, queue, payload, max_retries)

    def _report_error(self, token, task, e: BaseException) -> None:
        self._end_span(token, e)
        if isinstance(e, Retry):
            ms = None if e.countdown is None else max(0, int(e.countdown * 1000))
            self.native.complete_retry(token, ms, repr(e.exc) if e.exc is not None else "Retry requested")
            self._settled()
        elif task.retry_on and isinstance(e, task.retry_on):
            self.native.complete_retry(token, None, repr(e))
            self._settled()
        else:
            self.native.complete_failure(token, repr(e), "".join(traceback.format_exception(e)))
            self._settled()
