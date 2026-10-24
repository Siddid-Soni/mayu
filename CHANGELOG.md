# Changelog

All notable changes to mayu are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- Task expiry: `send(..., expires=30)` (seconds from now, or a timezone-aware `datetime`) drops a task at claim time if no worker started it by the deadline, settling it as `outcome="expired"` in `mayu_tasks_settled_total` and storing a "task expired" failure (when results are enabled). `expires` is now a reserved option name. Opt-in: tasks sent without it behave exactly as before. The deadline rides in a new optional envelope field, so older workers ignore it (they won't enforce expiry); upgrade workers first.

## [0.2.5] - 2026-10-22

### Added

- Parked models: `@app.pool("tts", vram_gb=6.9, parked={"ram_gb": 6.6})` with `@tts.park` and `@tts.unpark` hooks lets a lazy pool move an idle instance to host RAM instead of closing it, and move it back in about a second instead of rebuilding it. The worker parks a victim when its `parked=` amount fits in `--resources` (every resource named must be declared there), unparks a parked model before loading one, closes parked instances to make room, and scores a swap from RAM at a quarter of the loaded-model bonus. With `--lookahead`, victims go in order of when their pool is next needed, `--lookahead` unparks a parked model ahead of its task, and an optional `@tts.stage` hook builds the model straight into host RAM, even while the worker is busy, shortly before a workflow needs it (estimated from this worker's own run times and the workflow DAG). `close` runs on parked instances too. The README has a torch recipe.
- Metrics `mayu_pool_parked`, `mayu_pool_parks_total`, `mayu_pool_unparks_total`, `mayu_pool_drops_total`, `mayu_pool_stages_total`, `mayu_pool_move_seconds` and `mayu_pool_next_need_seconds`, exported only for pools with `parked=`. Nothing changes for a worker whose pools don't use it: same scheduling, eviction order, logs and metrics. No wire or key-layout change.
- Task progress: `mayu.progress(7, 25)` (or `mayu.progress(0.28)`) inside a running task reports how far it is, `mayu.progress_reporter()` does the same from threads the task starts, and both do nothing outside a task. `TaskHandle.progress()` returns a `Progress` (done, total, fraction, seconds left, worker) or `None`, `mayu inspect` gains a `RUNNING` section (shown only while something runs) and `inspect --json` a `running` key. Reports cost no I/O and are published with the worker heartbeat; the time left comes from the pace since the first report. With `--lookahead` and a pool with `parked=`, a running node's reported progress replaces its average run time in the estimate of when a pool is next needed, including nodes on other workers, and the staging log line says how soon. Adds `mayu_task_progress_reports_total`, exported once a task has reported. Nothing changes for tasks and workers that don't report.
- Worker heartbeat key: the `mayu:workers:{id}` hash gains a `tasks` field (the running tasks and their progress, msgpack as hex text, removed when the worker is idle). This is additive: older readers ignore it and older workers never write it, so 0.2.0 workers and tools keep working during a rolling deploy.

## [0.2.0] - 2026-10-22

### Added

- `app.declare` / `task.implement` and `app.send(name, ...)` to enqueue tasks without importing their code; workers serve only queues whose tasks they implement.
- Resource admission: `resources={"gpu": 0.3}` on tasks, `--resources` on workers, `--gpus` for one worker process per GPU, and `mayu_resource_capacity` / `mayu_resource_in_use` gauges.
- Workflows: `app.workflow()` builds a DAG of tasks (`wf.add(sig, after=[...])`) that `send()` submits atomically; each node's ack releases the children whose parents are all done. `WorkflowHandle` has `status()`, `wait()` and `cancel()`, a dead-lettered node blocks its descendants until `mayu dead replay`, `mayu inspect` lists unfinished workflows, and skipped nodes count as `outcome="cancelled"` in `mayu_tasks_settled_total`.
- Model pools: `@app.pool("gemma", vram_gb=5.8)` builds a fixed number of instances of an expensive object once per worker (`--pools gemma=2` / `MAYU_POOLS`), tasks borrow one with `model=app.use("gemma")`, and `DiscardInstance` / `discard_on` replace broken ones. Adds `mayu_pool_size`, `mayu_pool_busy` and `mayu_pool_rebuilds_total`; resource names starting with `pool:` are reserved.
- Lazy model pools: `--pool-mode lazy` loads a model when a task needs it and closes idle ones (least recently used) to make room, for models that don't fit in memory together. `--preload` loads pools at startup, `--schedule throughput|oldest` prefers tasks whose model is loaded (bounded to 60 s / 10 s, queue priority first), and `MAYU_POOL_MODE`, `MAYU_SCHEDULE` and `MAYU_PRELOAD` set the defaults. Adds `mayu_pool_loaded`, `mayu_pool_loads_total` and `mayu_pool_evictions_total`. Eager mode is unchanged.
- GPU monitoring with the optional `mayu[gpu]` extra (NVML): `--resources gpu=auto,vram_gb=auto` reads the amounts from the visible GPUs, `--gpu-monitor auto|on|off` and `--gpu-interval` control it (`MAYU_GPU_MONITOR`, `MAYU_GPU_INTERVAL`), and it respects `CUDA_VISIBLE_DEVICES`.
- VRAM guard: other processes' GPU memory beyond the slack above `vram_gb` is withheld from admission (never evicting anything running), and a warning is logged when the worker's processes use more VRAM than its tasks and pools declare.
- `--autotune gpu` (`MAYU_AUTOTUNE`) lowers the `gpu` capacity one task's worth at a time while the GPU is saturated and raises it again when tasks wait on an idle GPU, between the largest `gpu` request and the configured amount.
- `linger=` on `@app.pool` keeps an idle lazy instance loaded for up to that many seconds while a running workflow still has a task to come that uses it, and `--lookahead` (`MAYU_LOOKAHEAD`) loads the model a workflow needs next while its current task runs, when it fits without evicting anything.
- Metrics `mayu_gpu_utilization_ratio`, `mayu_gpu_memory_used_bytes`, `mayu_gpu_memory_total_bytes`, `mayu_gpu_process_memory_bytes`, `mayu_gpu_power_watts`, `mayu_gpu_temperature_celsius`, `mayu_pool_demand` and `mayu_pool_lookahead_loads_total`.

### Changed

- `Signature` arguments are checked when used (at `send()` for `link=`, at `wf.add()` for workflows), not in `task.s()`.
- The envelope header gains optional `workflow_id` and `node` fields. Old workers ignore them but can't release workflow children: upgrade workers before submitting workflows.
- Free-threaded workers no longer use about 4x the memory on Linux when transparent huge pages are `always`: `--thp python` (default, `MAYU_THP`) opts only Python's own heap out of huge pages, `keep` changes nothing and `off` disables them for the whole process. Measured at 2 × 32 threads: 525 MB down to 135 MB PSS, same throughput.

## [0.1.0] - 2026-10-12

First public release.

- Redis broker with at-least-once delivery, crash recovery, poison-task detection, dead letters and replay.
- Thread mode for free-threaded CPython 3.14t and process mode for GIL builds, with a supervising parent.
- Retries with backoff, countdown/ETA, soft and hard time limits, rate limits, queue priority and `link=` callbacks.
- Sync and `async def` tasks.
- Prometheus metrics, JSON logging, OpenTelemetry tracing and the `mayu inspect` / `mayu dead` CLI.
- Benchmarks against Celery, RQ, Dramatiq and streaQ.

[Unreleased]: https://github.com/Siddid-Soni/mayu/compare/v0.2.5...HEAD
[0.2.5]: https://github.com/Siddid-Soni/mayu/compare/v0.2.0...v0.2.5
[0.2.0]: https://github.com/Siddid-Soni/mayu/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/Siddid-Soni/mayu/releases/tag/v0.1.0
