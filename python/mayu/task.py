import datetime as dt
import functools
import inspect
import math
import time

from . import otel
from .handle import TaskHandle
from .pool import Use

RESERVED_OPTIONS = frozenset({"countdown", "eta", "queue", "link", "expires"})


_RATE_UNITS = {"s": 1.0, "m": 60.0, "h": 3600.0}


def parse_rate(rate) -> float:
    """'100/s', '10/m', '5/h' or a bare number (per second) -> task starts per second."""
    if isinstance(rate, bool):
        raise ValueError(f"bad rate_limit {rate!r}")
    if isinstance(rate, (int, float)):
        try:
            n, per = float(rate), 1.0
        except OverflowError:
            raise ValueError(f"bad rate_limit {rate!r}") from None
    elif isinstance(rate, str):
        num, _, unit = rate.strip().partition("/")
        try:
            n, per = float(num), _RATE_UNITS[unit.strip() or "s"]
        except (ValueError, KeyError):
            raise ValueError(f"bad rate_limit {rate!r}: use '<number>/s', '<number>/m' or '<number>/h'") from None
    else:
        raise TypeError(f"rate_limit must be a string or number, got {type(rate).__name__}")
    if not (math.isfinite(n) and n > 0):
        raise ValueError(f"bad rate_limit {rate!r}: must be a positive number")
    return n / per


def check_resources(what: str, resources, *, allow_zero: bool = False) -> dict[str, float]:
    """Validates ``{name: amount}`` for a task's requests (amount > 0) or a worker's capacity (amount >= 0)."""
    if resources is None:
        return {}
    shape = f"{what} must be a dict like {{'gpu': 0.5, 'vram_gb': 1.2}} (resource name -> amount)"
    if not isinstance(resources, dict):
        raise TypeError(f"{shape}, got {type(resources).__name__}")
    out = {}
    for name, amount in resources.items():
        if not isinstance(name, str):
            raise TypeError(f"{shape}; got a key {name!r}")
        if not name:
            raise ValueError(f"{shape}; resource names cannot be empty")
        if name.startswith("pool:"):
            raise ValueError(f"{shape}; resource names starting with 'pool:' are reserved for pools, got {name!r}")
        if isinstance(amount, bool) or not isinstance(amount, (int, float)):
            raise TypeError(f"{shape}; {name!r} must be a number, got {amount!r}")
        try:
            milli = round(amount * 1000)
        except (OverflowError, ValueError):  # inf, nan
            milli = None
        if milli is None or milli < (0 if allow_zero else 1):
            raise ValueError(f"{shape}; {name!r} must be a finite number " +
                             ("of at least 0" if allow_zero else "of at least 0.001") + f", got {amount!r}")
        out[name] = float(amount)
    return out


_RESULT = object()  # stands in for the parent's result when checking a callback's arguments


class Signature:
    """A task plus arguments. As ``link=`` the parent's result is passed first; as ``wf.add`` it is called as given.

    The arguments are checked where the signature is used, since the two uses bind them differently.
    """

    def __init__(self, task, args=(), kwargs=None):
        self.task = task
        self.args = tuple(args)
        self.kwargs = dict(kwargs or {})

    def __repr__(self) -> str:
        return f"<Signature {self.task.name} args={self.args!r} kwargs={self.kwargs!r}>"

    def _check_link(self) -> None:
        try:
            bound = self.task.signature.bind(_RESULT, *self.args, **self.kwargs)
        except TypeError as e:
            raise TypeError(f"{self.task.name} cannot be a link callback with these arguments "
                            f"(it receives the parent's result first): {e}") from None
        _reject_pool_arguments(self.task, bound, f"{self.task.name} cannot be a link callback")

    def _check_node(self) -> None:
        try:
            bound = self.task.signature.bind(*self.args, **self.kwargs)
        except TypeError as e:
            raise TypeError(f"{self.task.name} cannot be a workflow node with these arguments: {e}") from None
        _reject_pool_arguments(self.task, bound, f"{self.task.name} cannot be a workflow node")

    def _wire(self) -> dict:
        return {"task": self.task.name, "queue": self.task.queue, "max_retries": self.task.max_retries,
                "args": list(self.args), "kwargs": self.kwargs}


def _reject_pool_arguments(task, bound: inspect.BoundArguments, prefix: str) -> None:
    for param, pool in task.pools.items():
        if param in bound.arguments:
            raise TypeError(f"{prefix}: parameter {param!r} is filled from pool {pool!r} on the worker; "
                            f"don't pass it")


def _as_links(link) -> list[Signature]:
    if link is None:
        return []
    items = link if isinstance(link, (list, tuple)) else [link]
    out = []
    for item in items:
        if isinstance(item, Signature):
            sig = item
        elif isinstance(item, Task):
            sig = Signature(item)
        else:
            raise TypeError(f"link must be a task, task.s(...), or a list of them; got {item!r}")
        sig._check_link()
        out.append(sig)
    return out


def _expires_ms(expires) -> int | None:
    """Seconds from now, or an absolute timezone-aware datetime, as absolute ms. None stays None."""
    if expires is None:
        return None
    if isinstance(expires, dt.datetime):
        if expires.tzinfo is None:
            raise ValueError("expires must be a timezone-aware datetime")
        return max(0, int(expires.timestamp() * 1000))  # a pre-1970 deadline is already past
    if isinstance(expires, bool) or not isinstance(expires, (int, float)):
        raise TypeError(f"expires must be a number of seconds or a datetime, got {type(expires).__name__}")
    if not math.isfinite(expires):
        raise ValueError(f"expires must be a finite number of seconds, got {expires!r}")
    return int(time.time() * 1000) + max(0, int(expires * 1000))


def _eta_ms(countdown: float | None, eta: dt.datetime | None) -> int | None:
    if countdown is not None and eta is not None:
        raise ValueError("pass countdown or eta, not both")
    if countdown is not None:
        return int(time.time() * 1000) + max(0, int(countdown * 1000))
    if eta is not None:
        if eta.tzinfo is None:
            raise ValueError("eta must be a timezone-aware datetime")
        return int(eta.timestamp() * 1000)
    return None


def _shape(sig: inspect.Signature) -> list[tuple]:
    return [(p.name, p.kind, p.default) for p in sig.parameters.values()]


def _find_pools(name: str, sig: inspect.Signature, is_async: bool) -> dict[str, str]:
    """``{parameter: pool name}`` for the parameters whose default is ``app.use(...)``."""
    pools: dict[str, str] = {}
    for p in sig.parameters.values():
        if not isinstance(p.default, Use):
            continue
        if p.kind not in (p.POSITIONAL_OR_KEYWORD, p.KEYWORD_ONLY):
            raise TypeError(f"task {name!r}: parameter {p.name!r} takes a pool instance, so it cannot be "
                            f"positional-only; the worker passes it by keyword")
        if p.default.name in pools.values():
            raise TypeError(f"task {name!r} uses pool {p.default.name!r} in two parameters; "
                            f"a task gets one instance per pool")
        pools[p.name] = p.default.name
    if pools and is_async:
        raise TypeError(f"task {name!r} is async and uses pool(s) {sorted(set(pools.values()))}; "
                        f"pools are for sync tasks")
    return pools


def _enqueue(app, name: str, queue: str, max_retries: int, args, kwargs, countdown, eta, link,
             expires=None) -> TaskHandle:
    links = _as_links(link)
    eta_ms = _eta_ms(countdown, eta)
    expires_ms = _expires_ms(expires)
    serializer = app.serializer
    body = [list(args), kwargs]
    if links:
        body.append({"links": [s._wire() for s in links]})
    payload = serializer.dumps(body)
    span, traceparent = otel.start_send(name, queue)
    try:
        task_id = app.client.enqueue(name, queue, payload, serializer.content_type, max_retries, eta_ms,
                                     trace_ctx=traceparent, expires_ms=expires_ms)
    except BaseException as e:
        otel.end(span, e)
        raise
    otel.set_message_id(span, task_id)
    otel.end(span)
    return TaskHandle(app, task_id)


class Task:
    def __init__(self, app, fn, *, name: str, queue: str, max_retries: int, retry_on: tuple,
                 soft_time_limit: float | None = None, hard_time_limit: float | None = None, threadsafe: bool = True,
                 rate_limit=None, implemented: bool = True, resources=None):
        self.app = app
        self.fn = fn
        self.name = name
        self.queue = queue
        self.max_retries = max_retries
        self.retry_on = retry_on
        self.signature = inspect.signature(fn)
        clash = RESERVED_OPTIONS & set(self.signature.parameters)
        if clash:
            raise TypeError(f"task {name!r} uses reserved parameter name(s) {sorted(clash)}")
        for label, value in (("soft_time_limit", soft_time_limit), ("hard_time_limit", hard_time_limit)):
            if value is not None and value <= 0:
                raise ValueError(f"{label} must be a positive number of seconds")
        if soft_time_limit is not None and hard_time_limit is not None and soft_time_limit >= hard_time_limit:
            raise ValueError("soft_time_limit must be less than hard_time_limit")
        self.soft_time_limit = soft_time_limit
        self.hard_time_limit = hard_time_limit
        self.is_async = inspect.iscoroutinefunction(fn)
        if not threadsafe and self.is_async:
            raise ValueError("threadsafe=False is for sync tasks; async tasks already run on one event-loop thread")
        self.pools = _find_pools(name, self.signature, self.is_async)  # {parameter: pool name}
        self.threadsafe = threadsafe
        self.rate_limit = rate_limit
        self.rate_per_sec = None if rate_limit is None else parse_rate(rate_limit)
        self.resources = check_resources(f"resources of task {name!r}", resources)
        self.implemented = implemented
        self.declared = not implemented
        functools.update_wrapper(self, fn)

    def __repr__(self) -> str:
        return f"<Task {self.name}>"

    def __call__(self, *args, **kwargs):
        if not self.implemented:
            raise TypeError(f"task {self.name!r} is declared but not implemented in this process; "
                            f"import the module that calls {self.name}.implement(...)")
        return self.fn(*args, **kwargs)

    def implement(self, fn) -> "Task":
        """Attach the code of a task made with ``app.declare``; usable as a decorator, returns the task."""
        with self.app._registry_lock:
            if self.implemented:
                raise ValueError(f"task {self.name!r} is already implemented" if self.declared else
                                 f"task {self.name!r} was made with app.task; only app.declare tasks take an "
                                 f"implementation")
            sig = inspect.signature(fn)
            if _shape(sig) != _shape(self.signature):
                raise TypeError(f"implementation of {self.name!r} must match its declaration: "
                                f"declared {self.signature}, got {sig}")
            is_async = inspect.iscoroutinefunction(fn)
            if not self.threadsafe and is_async:
                raise ValueError("threadsafe=False is for sync tasks; async tasks already run on one event-loop thread")
            pools = _find_pools(self.name, sig, is_async)
            self.fn = fn
            self.is_async = is_async
            self.pools = pools
            functools.update_wrapper(self, fn)
            self.implemented = True
        return self

    def send(self, *args, countdown: float | None = None, eta: dt.datetime | None = None,
             queue: str | None = None, link=None, expires: float | dt.datetime | None = None,
             **kwargs) -> TaskHandle:
        bound = self.signature.bind(*args, **kwargs)
        if self.pools:
            _reject_pool_arguments(self, bound, self.name)
        return _enqueue(self.app, self.name, queue or self.queue, self.max_retries, args, kwargs, countdown, eta,
                        link, expires)

    def s(self, *args, **kwargs) -> Signature:
        """This task with arguments, for ``link=`` (the parent's result comes first) or ``wf.add``."""
        return Signature(self, args, kwargs)

    delay = send
