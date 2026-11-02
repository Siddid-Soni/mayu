import argparse
import importlib
import json
import logging
import math
import os
import sys
import time

from .app import App

log = logging.getLogger("mayu.cli")


def load_app(path: str) -> App:
    module_name, sep, attr = path.partition(":")
    if not sep or not module_name or not attr:
        raise ValueError(f"app path must look like 'package.module:app', got {path!r}")
    if os.getcwd() not in sys.path:
        sys.path.insert(0, os.getcwd())
    obj = getattr(importlib.import_module(module_name), attr)
    if not isinstance(obj, App):
        raise TypeError(f"{path!r} is a {type(obj).__name__}, not a mayu.App")
    return obj


def _refuse_embedded(app: App, command: str) -> bool:
    """Prints why `command` can't reach a memory:// broker and returns True; False for any other broker."""
    if not app._embedded:
        return False
    if command == "worker":
        hint = "start the worker from the program that sends the tasks, e.g. `with app.worker(): ...`"
    else:
        hint = "read it from inside that program"
    print(f"mayu: {app.broker_url} is a broker inside one Python process, so `mayu {command}` (another process) "
          f"can't reach it; {hint} (see 'Embedded mode' in the README)", file=sys.stderr)
    return True


def parse_resources(text: str | None) -> dict[str, float | str] | None:
    """'gpu=1,vram_gb=7.5' -> {'gpu': 1.0, 'vram_gb': 7.5}; None or '' -> None. gpu and vram_gb also take 'auto'."""
    if not text or not text.strip():
        return None
    out: dict[str, float | str] = {}
    for item in text.split(","):
        name, sep, amount = item.partition("=")
        name = name.strip()
        if sep and amount.strip() == "auto":
            if name not in ("gpu", "vram_gb"):
                raise ValueError(f"bad --resources entry {item.strip()!r}: only gpu and vram_gb can be auto; "
                                 f"give {name or 'it'} a number")
            if name in out:
                raise ValueError(f"bad --resources: {name!r} given twice")
            out[name] = "auto"
            continue
        try:
            value = float(amount)
        except ValueError:
            value = float("nan")
        if not sep or not name or not math.isfinite(value) or value < 0:
            raise ValueError(f"bad --resources entry {item.strip()!r}: use name=amount with an amount >= 0, "
                             f"like gpu=1,vram_gb=7.5")
        if name in out:
            raise ValueError(f"bad --resources: {name!r} given twice")
        out[name] = value
    return out


def parse_pools(text: str | None) -> dict[str, int] | None:
    """'gemma=2,tts=3' -> {'gemma': 2, 'tts': 3}; None or '' -> None."""
    if not text or not text.strip():
        return None
    out: dict[str, int] = {}
    for item in text.split(","):
        name, sep, count = item.partition("=")
        name, count = name.strip(), count.strip()
        if not sep or not name or not (count.isascii() and count.isdigit()) or any(c.isspace() for c in name):
            raise ValueError(f"bad --pools entry {item.strip()!r}: use name=count with a whole number >= 0, "
                             f"like gemma=2,tts=3")
        if name in out:
            raise ValueError(f"bad --pools: {name!r} given twice")
        out[name] = int(count)
    return out


def parse_preload(text: str | None) -> list[str] | None:
    """'gemma,tts' -> ['gemma', 'tts'] (order kept); None or '' -> None."""
    if not text or not text.strip():
        return None
    names = [n.strip() for n in text.split(",")]
    if not all(names) or any(any(c.isspace() for c in n) or "=" in n for n in names):
        raise ValueError(f"bad --preload {text!r}: use comma-separated pool names like gemma,tts")
    dupes = sorted({n for n in names if names.count(n) > 1})
    if dupes:
        raise ValueError(f"bad --preload {text!r}: {', '.join(dupes)} listed more than once")
    return names


def parse_gpus(text: str | None) -> list[str] | None:
    """'0,1' -> ['0', '1']. Ids are opaque (indexes or UUIDs like 'GPU-abc'): they go to CUDA_VISIBLE_DEVICES."""
    if not text or not text.strip():
        return None
    ids = [i.strip() for i in text.split(",")]
    if not all(ids):
        raise ValueError(f"bad --gpus {text!r}: use comma-separated GPU ids like 0,1")
    dupes = sorted({i for i in ids if ids.count(i) > 1})
    if dupes:
        raise ValueError(f"bad --gpus {text!r}: {', '.join(dupes)} listed more than once")
    return ids


def _build_runner(app, args, threads: int):
    from .worker import WorkerRunner

    queues = [q for q in args.queues.split(",") if q] if args.queues else None
    return WorkerRunner(app, threads=threads, queues=queues,
                        prefetch_multiplier=args.prefetch_multiplier,
                        shutdown_grace=args.shutdown_grace,
                        visibility_timeout=args.visibility_timeout,
                        heartbeat_interval=args.heartbeat_interval,
                        max_deliveries=args.max_deliveries,
                        max_zombie_threads=args.max_zombie_threads,
                        max_tasks_per_process=args.max_tasks_per_process,
                        max_memory_per_process=args.max_memory_per_process,
                        metrics_port=args.metrics_port, metrics_host=args.metrics_host,
                        resources=args.resources, pools=args.pools, thp=args.thp,
                        pool_mode=args.pool_mode, schedule=args.schedule, preload=args.preload,
                        gpu_monitor=args.gpu_monitor, gpu_interval=args.gpu_interval,
                        autotune=args.autotune, lookahead=args.lookahead)


def _cmd_worker(args) -> int:
    from . import thp
    from .log import configure_logging
    try:
        configure_logging(args.log_format, args.log_level)
    except ValueError as e:  # argparse does not check `choices` against env-provided defaults
        print(f"mayu: {e}", file=sys.stderr)
        return 2
    try:
        args.resources = parse_resources(args.resources)
    except ValueError as e:
        print(f"mayu: {e}", file=sys.stderr)
        return 2
    try:
        args.pools = parse_pools(args.pools)
    except ValueError as e:
        print(f"mayu: {e}", file=sys.stderr)
        return 2
    try:
        args.preload = parse_preload(args.preload)
    except ValueError as e:
        print(f"mayu: {e}", file=sys.stderr)
        return 2
    try:
        thp.check(args.thp)
    except ValueError as e:  # argparse does not check `choices` against env-provided defaults
        print(f"mayu: {e}", file=sys.stderr)
        return 2
    try:
        gpus = parse_gpus(args.gpus)
    except ValueError as e:
        print(f"mayu: {e}", file=sys.stderr)
        return 2
    if gpus and not args._child:  # a child already has its GPU in its environment
        if args.processes is not None and args.processes != len(gpus):
            print(f"mayu: --gpus lists {len(gpus)} GPU(s), one process each; --processes {args.processes} "
                  f"does not match (drop --processes or list {args.processes} GPUs)", file=sys.stderr)
            return 2
        if len(gpus) == 1:
            # Before the app is imported: user code may initialise CUDA at import time.
            os.environ["CUDA_VISIBLE_DEVICES"] = gpus[0]
    thp.apply(args.thp)  # also before the import: objects the app creates at import time are already in the arena
    from .modes import gil_enabled, import_app_detecting_gil, resolve_mode
    from .supervisor import RESTART_EXIT_CODE, ProcessSupervisor, watch_parent

    try:
        app, offenders = import_app_detecting_gil(args.app)
    except (ImportError, AttributeError, ValueError, TypeError) as e:
        print(f"mayu: cannot load app {args.app!r}: {e}", file=sys.stderr)
        return 2
    if _refuse_embedded(app, "worker"):
        return 2
    gil = gil_enabled()
    mode = resolve_mode(args.mode, gil)
    if mode == "process" and args.threads not in (None, 1):
        print("mayu: --threads applies to thread mode; process mode runs one task per process "
              "(use --processes)", file=sys.stderr)
        return 2
    threads = 1 if mode == "process" else (args.threads or os.cpu_count() or 4)
    processes = args.processes or ((os.cpu_count() or 2) if mode == "process" else 1)
    if gpus and not args._child:
        processes = len(gpus)
    if processes < 1:
        print("mayu: --processes must be >= 1", file=sys.stderr)
        return 2
    if args.metrics_port is not None and not args._child and processes > 1:
        if args.metrics_port == 0:
            print("mayu: --metrics-port 0 cannot be used with several processes; pass a port",
                  file=sys.stderr)
            return 2
        if args.metrics_port + processes - 1 > 65535:
            print("mayu: --metrics-port + --processes exceeds 65535", file=sys.stderr)
            return 2

    if args._child:
        try:
            runner = _build_runner(app, args, threads)
        except ValueError as e:
            print(f"mayu: {e}", file=sys.stderr)
            return 2
        watch_parent(runner.stop)
        try:
            reason = runner.run()
        except (RuntimeError, ValueError) as e:
            print(f"mayu: cannot start worker: {e}", file=sys.stderr)
            return 1
        sys.stdout.flush()
        sys.stderr.flush()
        if reason is not None:
            os._exit(RESTART_EXIT_CODE)  # the supervisor replaces us; skip waiting on zombie threads
        return 0

    if args.mode == "auto" and mode == "process":
        why = (f"re-enabled by importing {', '.join(offenders)}" if offenders
               else "this Python build has a GIL")
        log.warning("the GIL is enabled (%s); using process mode with %s processes", why, processes)
    elif mode == "thread" and gil:
        log.warning("the GIL is enabled; threads will not run Python code in parallel. "
                    "Use a free-threaded build (python3.14t) or --mode process.")

    if mode == "thread" and processes == 1:
        try:
            runner = _build_runner(app, args, threads)
        except ValueError as e:
            print(f"mayu: {e}", file=sys.stderr)
            return 2
        try:
            reason = runner.run()
        except (RuntimeError, ValueError) as e:
            print(f"mayu: cannot start worker: {e}", file=sys.stderr)
            return 1
        if reason is not None:
            sys.stdout.flush()
            sys.stderr.flush()
            os.execv(sys.executable, [sys.executable, "-m", "mayu", *args.argv])
        return 0

    try:
        _build_runner(app, args, threads)  # validate runner options; does not connect to Redis
    except ValueError as e:
        print(f"mayu: {e}", file=sys.stderr)
        return 2
    child = [sys.executable, "-m", "mayu", *args.argv, "--mode", mode, "--processes", "1", "--_child"]
    if mode == "process":
        child += ["--threads", "1", "--max-zombie-threads", "0"]
    per_child = None
    if args.metrics_port is not None:
        base = args.metrics_port
        per_child = lambda i: ["--metrics-port", str(base + i)]  # noqa: E731
    per_child_env = None
    if gpus and len(gpus) > 1:
        per_child_env = lambda i: {"CUDA_VISIBLE_DEVICES": gpus[i]}  # noqa: E731
    return ProcessSupervisor(child, processes, shutdown_grace=args.shutdown_grace,
                             per_child_args=per_child, per_child_env=per_child_env).run()


def _load_or_exit(path: str):
    try:
        return load_app(path)
    except (ImportError, AttributeError, ValueError, TypeError) as e:
        print(f"mayu: cannot load app {path!r}: {e}", file=sys.stderr)
        return None


def _cmd_inspect(args) -> int:
    from . import admin

    app = _load_or_exit(args.app)
    if app is None or _refuse_embedded(app, "inspect"):
        return 2
    queues = [q for q in args.queues.split(",") if q] if args.queues else None
    try:
        data = admin.inspect(app, queues)
    except RuntimeError as e:
        print(f"mayu: {e}", file=sys.stderr)
        return 1
    print(json.dumps(data, indent=2) if args.json else admin.format_inspect(data, int(time.time() * 1000)))
    return 0


def _cmd_dead(args) -> int:
    from . import admin

    if args.limit is not None and args.limit < 1:
        print("mayu: --limit must be >= 1", file=sys.stderr)
        return 2
    app = _load_or_exit(args.app)
    if app is None or _refuse_embedded(app, f"dead {args.dead_cmd}"):
        return 2
    queue = args.queue or app.default_queue
    try:
        if args.dead_cmd == "list":
            entries = admin.dead_list(app, queue, args.limit)
            if args.json:
                print(json.dumps([{k: v for k, v in e.items() if k != "payload"} for e in entries], indent=2))
            else:
                print(admin.format_dead(entries))
            return 0
        replayed, skipped = admin.dead_replay(app, queue, limit=args.limit, ids=args.ids)
    except RuntimeError as e:
        print(f"mayu: {e}", file=sys.stderr)
        return 1
    note = f" ({skipped} undecodable left in place)" if skipped else ""
    print(f"replayed {replayed} task(s) from the dead letters of {queue!r}{note}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="mayu")
    sub = parser.add_subparsers(dest="cmd", required=True)
    w = sub.add_parser("worker", help="run a worker process")
    w.add_argument("app", help="import path of the App, e.g. myproject.tasks:app")
    w.add_argument("--threads", type=int, default=None,
                   help="threads per process (thread mode; default: CPU count)")
    w.add_argument("--mode", choices=("auto", "thread", "process"), default="auto",
                   help="auto: thread mode if the GIL is off after importing the app, else process mode")
    w.add_argument("--processes", type=int, default=None,
                   help="worker processes (default: 1 in thread mode, CPU count in process mode)")
    w.add_argument("--_child", action="store_true", help=argparse.SUPPRESS)
    w.add_argument("--queues", default=None, help="comma-separated, highest priority first; default: every queue the app's tasks use, at equal priority")
    w.add_argument("--prefetch-multiplier", type=int, default=2)
    w.add_argument("--shutdown-grace", type=float, default=30.0)
    w.add_argument("--visibility-timeout", type=float, default=30.0,
                   help="seconds before a silent worker's tasks are reclaimed by others")
    w.add_argument("--heartbeat-interval", type=float, default=5.0)
    w.add_argument("--max-deliveries", type=int, default=5,
                   help="dead-letter a task after this many crash-redeliveries")
    w.add_argument("--max-zombie-threads", type=int, default=4,
                   help="recycle the worker when more threads than this are stuck past their hard limit")
    w.add_argument("--max-tasks-per-process", type=int, default=None)
    w.add_argument("--max-memory-per-process", type=float, default=None, metavar="MB")
    w.add_argument("--metrics-port", type=int, default=os.environ.get("MAYU_METRICS_PORT") or None,
                   help="serve Prometheus metrics on this port (process i of --processes uses port+i)")
    w.add_argument("--metrics-host", default="0.0.0.0", help="address to serve metrics on")
    w.add_argument("--resources", default=os.environ.get("MAYU_RESOURCES") or None, metavar="NAME=AMOUNT,...",
                   help="what each worker process has, e.g. gpu=1,vram_gb=7.5,cpu=12; a task starts only when "
                        "its resources= requests fit in what is left. gpu=auto and vram_gb=auto read it from the "
                        "visible GPUs (needs mayu[gpu])")
    w.add_argument("--pools", default=os.environ.get("MAYU_POOLS") or None, metavar="NAME=COUNT,...",
                   help="instances of each model pool to load in each worker process, e.g. gemma=2,tts=3 "
                        "(pools are declared in code with @app.pool)")
    w.add_argument("--pool-mode", default=os.environ.get("MAYU_POOL_MODE") or "eager", metavar="{eager,lazy}",
                   choices=("eager", "lazy"),
                   help="eager (default) loads every pool instance at startup and holds its resources for good; "
                        "lazy loads a model when a task needs it and evicts idle ones (least recently used) "
                        "when another task needs the room")
    w.add_argument("--schedule", default=os.environ.get("MAYU_SCHEDULE") or None, metavar="{throughput,oldest}",
                   choices=("throughput", "oldest"),
                   help="with --pool-mode lazy: prefer tasks whose model is loaded. throughput: fewest model swaps; "
                        "oldest: oldest workflow first (default in lazy mode)")
    w.add_argument("--preload", default=os.environ.get("MAYU_PRELOAD") or None, metavar="NAME,...",
                   help="with --pool-mode lazy: pools to load one instance of at startup, e.g. gemma")
    w.add_argument("--lookahead", action="store_true",
                   default=(os.environ.get("MAYU_LOOKAHEAD") or "").lower() in ("1", "true", "yes"),
                   help="with --pool-mode lazy: load the model a running workflow will need next while the "
                        "current node runs, when it fits without evicting anything")
    w.add_argument("--gpus", default=None, metavar="ID,...",
                   help="one worker process per listed GPU, each with CUDA_VISIBLE_DEVICES set to its id")
    w.add_argument("--gpu-monitor", default=os.environ.get("MAYU_GPU_MONITOR") or "auto", metavar="{auto,on,off}",
                   choices=("auto", "on", "off"),
                   help="read the GPUs with NVML (needs mayu[gpu]) for the mayu_gpu_* metrics and the VRAM guard. "
                        "auto (default): on when NVML works and a GPU is visible; on: error if it can't; off")
    w.add_argument("--gpu-interval", type=float, default=os.environ.get("MAYU_GPU_INTERVAL") or 2.0, metavar="SECONDS",
                   help="how often to read the GPUs (default 2)")
    w.add_argument("--autotune", default=os.environ.get("MAYU_AUTOTUNE") or None, metavar="RESOURCE",
                   help="adapt concurrency to the hardware. gpu: with --resources gpu=N as the ceiling, run fewer "
                        "GPU tasks while the GPU is saturated and more again when it idles with tasks waiting "
                        "(needs mayu[gpu])")
    w.add_argument("--thp", default=os.environ.get("MAYU_THP") or "python", metavar="{python,keep,off}",
                   help="transparent huge pages: python (default) opts only Python's own heap out, keep leaves "
                        "the system setting, off disables them for the whole process and its children")
    w.add_argument("--log-format", choices=("text", "json"), default=os.environ.get("MAYU_LOG_FORMAT") or "text")
    w.add_argument("--log-level", choices=("DEBUG", "INFO", "WARNING", "ERROR"),
                   type=str.upper, default=(os.environ.get("MAYU_LOG_LEVEL") or "INFO").upper())
    w.set_defaults(func=_cmd_worker)

    i = sub.add_parser("inspect", help="show queue depths, live workers and unfinished workflows")
    i.add_argument("app")
    i.add_argument("--queues", default=None, help="comma-separated; default: the app's queues and any found in Redis")
    i.add_argument("--json", action="store_true")
    i.set_defaults(func=_cmd_inspect)

    d = sub.add_parser("dead", help="list or replay dead-lettered tasks")
    dsub = d.add_subparsers(dest="dead_cmd", required=True)
    dl = dsub.add_parser("list", help="show dead letters, oldest first")
    dl.add_argument("app")
    dl.add_argument("--queue", default=None, help="default: the app's default queue")
    dl.add_argument("--limit", type=int, default=20)
    dl.add_argument("--json", action="store_true")
    dr = dsub.add_parser("replay", help="run dead-lettered tasks again (attempts reset)")
    dr.add_argument("app")
    dr.add_argument("--queue", default=None, help="default: the app's default queue")
    dr.add_argument("--limit", type=int, default=None)
    dr.add_argument("--id", action="append", dest="ids", metavar="TASK_ID", help="only this task (repeatable)")
    for p in (dl, dr):
        p.set_defaults(func=_cmd_dead)
    argv = sys.argv[1:] if argv is None else list(argv)
    args = parser.parse_args(argv)
    args.argv = argv
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
