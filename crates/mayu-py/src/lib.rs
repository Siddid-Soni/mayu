use std::collections::HashMap;
use std::time::Duration;

use mayu_core::{
    Broker, GpuStat, Header, LazyPool, Link, MoveKind, Outcome, ReadyTask, Resources, RunningTask, Runtime,
    RuntimeConfig, Schedule,
    WorkflowNode, WorkflowSpec, WorkflowState,
};
use mayu_redis::RedisBroker;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};

fn rt_err(e: impl std::fmt::Display) -> PyErr {
    PyRuntimeError::new_err(e.to_string())
}

/// Bad input (a spec the core rejects) is a `ValueError`; anything else is a `RuntimeError`.
fn core_err(e: mayu_core::Error) -> PyErr {
    match e {
        mayu_core::Error::Config(m) => PyValueError::new_err(m),
        e => rt_err(e),
    }
}

/// (resources one loaded instance holds, instances loaded at start, idle seconds to linger)
type LazyArgs = (HashMap<String, f64>, u32, Option<f64>);

/// `LazyArgs`, then what one parked instance holds and whether an instance can be staged.
type ParkableArgs = (HashMap<String, f64>, u32, Option<f64>, HashMap<String, f64>, bool);

/// A pool's settings: `LazyArgs`, and optionally what one parked instance holds and whether it can be staged.
/// The shorter form is what a worker without parked pools passes.
#[derive(FromPyObject)]
enum LazyPoolArg {
    Parkable(ParkableArgs),
    Plain(LazyArgs),
}

/// pool name -> its settings
type LazyPoolArgs = HashMap<String, LazyPoolArg>;

/// (index, uuid, utilization, memory used, memory total, process memory, power watts, temperature C)
type GpuArgs = (u32, String, f64, u64, u64, Option<u64>, Option<f64>, Option<f64>);

fn lazy_pool(name: &str, arg: LazyPoolArg) -> PyResult<LazyPool> {
    let ((claim, loaded, linger), parked_claim, stageable) = match arg {
        LazyPoolArg::Parkable((claim, loaded, linger, parked_claim, stageable)) => {
            ((claim, loaded, linger), parked_claim, stageable)
        }
        LazyPoolArg::Plain(args) => (args, HashMap::new(), false),
    };
    let linger = linger
        .map(|secs| match Duration::try_from_secs_f64(secs) {
            Ok(d) if !d.is_zero() => Ok(d),
            _ => Err(PyValueError::new_err(format!(
                "linger for pool '{name}' must be a finite number of seconds above 0, got {secs}"
            ))),
        })
        .transpose()?;
    Ok(LazyPool { claim, loaded, linger, parked_claim, stageable })
}

/// (task, queue, payload, content_type, max_retries, parents, trace_ctx)
type NodeArgs = (String, String, Vec<u8>, String, u32, Vec<usize>, Option<String>);

/// Producer-side handle: enqueue tasks and read results.
#[pyclass(frozen, module = "mayu._native")]
struct Client {
    rt: tokio::runtime::Runtime,
    broker: RedisBroker,
}

fn running_task_dict<'py>(py: Python<'py>, t: RunningTask) -> PyResult<Bound<'py, PyDict>> {
    let d = PyDict::new(py);
    d.set_item("id", t.id)?;
    d.set_item("task", t.task)?;
    d.set_item("queue", t.queue)?;
    d.set_item("workflow_id", t.workflow_id)?;
    d.set_item("node", t.node)?;
    d.set_item("started_ms", t.started_ms)?;
    d.set_item("done", t.done)?;
    d.set_item("total", t.total)?;
    d.set_item("fraction", t.fraction)?;
    d.set_item("remaining_s", t.remaining_s)?;
    Ok(d)
}

fn worker_dict<'py>(py: Python<'py>, w: mayu_core::WorkerInfo) -> PyResult<Bound<'py, PyDict>> {
    let d = PyDict::new(py);
    d.set_item("worker_id", w.worker_id)?;
    d.set_item("hostname", w.hostname)?;
    d.set_item("pid", w.pid)?;
    d.set_item("queues", w.queues)?;
    d.set_item("concurrency", w.concurrency)?;
    d.set_item("inflight", w.inflight)?;
    d.set_item("started_at", w.started_at)?;
    d.set_item("last_seen", w.last_seen)?;
    let tasks = w.tasks.into_iter().map(|t| running_task_dict(py, t)).collect::<PyResult<Vec<_>>>()?;
    d.set_item("tasks", tasks)?;
    Ok(d)
}

fn workflow_dict<'py>(py: Python<'py>, w: WorkflowState) -> PyResult<Bound<'py, PyDict>> {
    let sum = w.summary();
    let d = PyDict::new(py);
    d.set_item("id", &w.id)?;
    d.set_item("name", &w.name)?;
    d.set_item("created_at", w.created_at)?;
    d.set_item("cancelled", w.cancelled)?;
    d.set_item("state", sum.state)?;
    d.set_item("waiting", sum.waiting)?;
    d.set_item("queued", sum.queued)?;
    d.set_item("done", sum.done)?;
    d.set_item("dead", sum.dead)?;
    d.set_item("cancelled_nodes", sum.cancelled)?;
    let nodes = w.nodes.iter().map(|n| {
        let nd = PyDict::new(py);
        nd.set_item("node", &n.node)?;
        nd.set_item("id", &n.task_id)?;
        nd.set_item("task", &n.task)?;
        nd.set_item("queue", &n.queue)?;
        nd.set_item("state", n.state.as_str())?;
        Ok(nd)
    }).collect::<PyResult<Vec<_>>>()?;
    d.set_item("nodes", nodes)?;
    Ok(d)
}

#[pymethods]
impl Client {
    #[new]
    fn new(py: Python<'_>, url: String) -> PyResult<Self> {
        py.detach(|| {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .enable_all()
                .build()
                .map_err(rt_err)?;
            let broker = rt.block_on(RedisBroker::connect(&url, "producer")).map_err(rt_err)?;
            Ok(Client { rt, broker })
        })
    }

    #[pyo3(signature = (task, queue, payload, content_type, max_retries, eta_ms=None, trace_ctx=None, expires_ms=None))]
    #[allow(clippy::too_many_arguments)]
    fn enqueue(
        &self,
        py: Python<'_>,
        task: String,
        queue: String,
        payload: Vec<u8>,
        content_type: String,
        max_retries: u32,
        eta_ms: Option<u64>,
        trace_ctx: Option<String>,
        expires_ms: Option<u64>,
    ) -> PyResult<String> {
        let mut header = Header::new(task, queue, max_retries, content_type);
        header.eta = eta_ms;
        header.trace_ctx = trace_ctx;
        header.expires = expires_ms;
        py.detach(|| self.rt.block_on(self.broker.enqueue(&header, &payload))).map_err(rt_err)?;
        Ok(header.id)
    }

    fn get_result<'py>(&self, py: Python<'py>, id: &str, timeout_ms: u64) -> PyResult<Option<Bound<'py, PyBytes>>> {
        let found = py.detach(|| {
            self.rt.block_on(mayu_core::wait_result(&self.broker, id, Duration::from_millis(timeout_ms)))
        });
        Ok(found.map_err(rt_err)?.map(|v| PyBytes::new(py, &v)))
    }

    fn worker_info<'py>(&self, py: Python<'py>, worker_id: &str) -> PyResult<Option<Bound<'py, PyDict>>> {
        let info = py.detach(|| self.rt.block_on(self.broker.worker_info(worker_id))).map_err(rt_err)?;
        info.map(|w| worker_dict(py, w)).transpose()
    }

    fn workers<'py>(&self, py: Python<'py>) -> PyResult<Vec<Bound<'py, PyDict>>> {
        let ws = py.detach(|| self.rt.block_on(self.broker.workers())).map_err(rt_err)?;
        ws.into_iter().map(|w| worker_dict(py, w)).collect()
    }

    /// Tasks running on every live worker, each with its `worker` id; as fresh as the last heartbeat.
    fn running_tasks<'py>(&self, py: Python<'py>) -> PyResult<Vec<Bound<'py, PyDict>>> {
        let ws = py.detach(|| self.rt.block_on(self.broker.workers())).map_err(rt_err)?;
        let mut out = Vec::new();
        for w in ws {
            for t in w.tasks {
                let d = running_task_dict(py, t)?;
                d.set_item("worker", &w.worker_id)?;
                out.push(d);
            }
        }
        Ok(out)
    }

    fn known_queues(&self, py: Python<'_>) -> PyResult<Vec<String>> {
        py.detach(|| self.rt.block_on(self.broker.known_queues())).map_err(rt_err)
    }

    #[pyo3(signature = (queue, after=None, until=None, count=100))]
    fn dead_letters<'py>(&self, py: Python<'py>, queue: &str, after: Option<String>, until: Option<String>,
                         count: usize) -> PyResult<Vec<Bound<'py, PyDict>>> {
        let items = py.detach(|| self.rt.block_on(
            self.broker.dead_letters(queue, after.as_deref(), until.as_deref(), count))).map_err(rt_err)?;
        items.into_iter().map(|e| {
            let d = PyDict::new(py);
            d.set_item("entry_id", &e.entry_id)?;
            d.set_item("dead_at_ms", e.dead_at_ms())?;
            d.set_item("error", &e.error)?;
            d.set_item("payload", PyBytes::new(py, &e.payload))?;
            let h = e.header.as_ref();
            d.set_item("id", h.map(|h| h.id.clone()))?;
            d.set_item("task", h.map(|h| h.task.clone()))?;
            d.set_item("attempt", h.map(|h| h.attempt))?;
            d.set_item("max_retries", h.map(|h| h.max_retries))?;
            d.set_item("content_type", h.map(|h| h.content_type.clone()))?;
            d.set_item("created_at", h.map(|h| h.created_at))?;
            Ok(d)
        }).collect()
    }

    fn last_dead_id(&self, py: Python<'_>, queue: &str) -> PyResult<Option<String>> {
        py.detach(|| self.rt.block_on(self.broker.last_dead_id(queue))).map_err(rt_err)
    }

    fn replay_dead(&self, py: Python<'_>, queue: &str, entry_ids: Vec<String>) -> PyResult<usize> {
        py.detach(|| self.rt.block_on(self.broker.replay_dead(queue, &entry_ids))).map_err(rt_err)
    }

    /// Writes the DAG and enqueues its roots in one step; returns the workflow id and each node's task id.
    fn submit_workflow(&self, py: Python<'_>, name: String, ttl_secs: u64, nodes: Vec<NodeArgs>)
        -> PyResult<(String, Vec<String>)> {
        let nodes = nodes.into_iter().map(|(task, queue, payload, content_type, max_retries, parents, trace_ctx)| {
            let mut header = Header::new(task, queue, max_retries, content_type);
            header.trace_ctx = trace_ctx;
            WorkflowNode { header, payload, parents }
        }).collect();
        let wf = WorkflowSpec::new(name, ttl_secs, nodes);
        wf.validate().map_err(core_err)?;
        py.detach(|| self.rt.block_on(self.broker.submit_workflow(&wf))).map_err(core_err)?;
        Ok((wf.id.clone(), wf.nodes.iter().map(|n| n.header.id.clone()).collect()))
    }

    fn workflow<'py>(&self, py: Python<'py>, id: &str) -> PyResult<Option<Bound<'py, PyDict>>> {
        let w = py.detach(|| self.rt.block_on(self.broker.workflow(id))).map_err(core_err)?;
        w.map(|w| workflow_dict(py, w)).transpose()
    }

    fn cancel_workflow(&self, py: Python<'_>, id: &str) -> PyResult<bool> {
        py.detach(|| self.rt.block_on(self.broker.cancel_workflow(id))).map_err(core_err)
    }

    fn workflows<'py>(&self, py: Python<'py>) -> PyResult<Vec<Bound<'py, PyDict>>> {
        let ws = py.detach(|| self.rt.block_on(self.broker.workflows())).map_err(core_err)?;
        ws.into_iter().map(|w| workflow_dict(py, w)).collect()
    }

    fn queue_stats<'py>(&self, py: Python<'py>, queue: &str) -> PyResult<Bound<'py, PyDict>> {
        let s = py.detach(|| self.rt.block_on(self.broker.queue_stats(queue))).map_err(rt_err)?;
        let d = PyDict::new(py);
        d.set_item("stream", s.stream)?;
        d.set_item("pending", s.pending)?;
        d.set_item("delayed", s.delayed)?;
        d.set_item("dead", s.dead)?;
        Ok(d)
    }
}

/// Worker-side runtime: Python threads pull tasks and report outcomes.
#[pyclass(frozen, module = "mayu._native")]
struct Worker {
    inner: Runtime<RedisBroker>,
}

#[pymethods]
impl Worker {
    #[new]
    #[pyo3(signature = (url, queues, concurrency, prefetch_multiplier=2, result_ttl_secs=None, consumer=None, visibility_timeout_ms=None, reclaim_interval_ms=None, heartbeat_interval_ms=None, max_deliveries=None, strict_priority=false, rate_limits=None, metrics_addr=None, resources=None, task_resources=None, lazy_pools=None, schedule=None, lookahead=false))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        py: Python<'_>,
        url: String,
        queues: Vec<String>,
        concurrency: usize,
        prefetch_multiplier: usize,
        result_ttl_secs: Option<u64>,
        consumer: Option<String>,
        visibility_timeout_ms: Option<u64>,
        reclaim_interval_ms: Option<u64>,
        heartbeat_interval_ms: Option<u64>,
        max_deliveries: Option<u64>,
        strict_priority: bool,
        rate_limits: Option<HashMap<String, f64>>,
        metrics_addr: Option<String>,
        resources: Option<HashMap<String, f64>>,
        task_resources: Option<HashMap<String, HashMap<String, f64>>>,
        lazy_pools: Option<LazyPoolArgs>,
        schedule: Option<String>,
        lookahead: bool,
    ) -> PyResult<Self> {
        let schedule = match schedule.as_deref() {
            None => Schedule::Fifo,
            Some("throughput") => Schedule::Throughput,
            Some("oldest") => Schedule::Oldest,
            Some(other) => {
                return Err(PyValueError::new_err(format!(
                    "schedule must be 'throughput' or 'oldest', got {other:?}"
                )));
            }
        };
        let lazy = lazy_pools.unwrap_or_default().into_iter()
            .map(|(name, args)| lazy_pool(&name, args).map(|p| (name, p)))
            .collect::<PyResult<HashMap<_, _>>>()?;
        let consumer = consumer.unwrap_or_else(|| format!("w-{}-{}", std::process::id(), ulid::Ulid::generate()));
        let mut cfg = RuntimeConfig {
            queues, concurrency, prefetch_multiplier, result_ttl_secs,
            worker_id: consumer.clone(),
            strict_priority,
            rate_limits: rate_limits.unwrap_or_default(),
            resources: Resources {
                capacity: resources.unwrap_or_default(),
                requests: task_resources.unwrap_or_default(),
                lazy,
            },
            schedule,
            lookahead,
            ..RuntimeConfig::default()
        };
        if let Some(v) = visibility_timeout_ms { cfg.visibility_timeout_ms = v; }
        if let Some(v) = reclaim_interval_ms { cfg.reclaim_interval_ms = v; }
        if let Some(v) = heartbeat_interval_ms { cfg.heartbeat_interval_ms = v; }
        if let Some(v) = max_deliveries { cfg.max_deliveries = v; }
        if let Some(a) = metrics_addr {
            use std::net::ToSocketAddrs;
            let bad = |e: String| PyValueError::new_err(format!("bad metrics address {a:?}: {e}"));
            cfg.metrics_addr = Some(
                a.to_socket_addrs().map_err(|e| bad(e.to_string()))?
                    .next().ok_or_else(|| bad("resolves to no address".into()))?,
            );
        }
        let inner = py
            .detach(move || Runtime::start(move || async move { RedisBroker::connect(&url, consumer).await }, cfg))
            .map_err(rt_err)?;
        Ok(Worker { inner })
    }

    #[getter]
    fn worker_id(&self) -> &str {
        self.inner.worker_id()
    }

    #[getter]
    fn metrics_port(&self) -> Option<u16> {
        self.inner.metrics_addr().map(|a| a.port())
    }

    fn recv(&self, py: Python<'_>, timeout_ms: u64) -> Option<PyReadyTask> {
        py.detach(|| self.inner.recv_timeout(Duration::from_millis(timeout_ms))).map(PyReadyTask)
    }

    #[pyo3(signature = (token, result=None, links=None))]
    fn complete_success(&self, token: u64, result: Option<Vec<u8>>, links: Option<Vec<(String, String, Vec<u8>, u32)>>) {
        let links = links
            .unwrap_or_default()
            .into_iter()
            .map(|(task, queue, payload, max_retries)| Link { task, queue, max_retries, payload })
            .collect();
        self.inner.complete(token, Outcome::Success { result, links });
    }

    fn complete_failure(&self, token: u64, error: String, traceback: String) {
        self.inner.complete(token, Outcome::Failure { error, traceback });
    }

    #[pyo3(signature = (token, countdown_ms, error))]
    fn complete_retry(&self, token: u64, countdown_ms: Option<u64>, error: String) {
        self.inner.complete(token, Outcome::Retry { countdown_ms, error });
    }

    /// Pools resize their `pool:<name>` resource as instances are discarded and rebuilt.
    fn adjust_capacity(&self, resource: &str, delta: f64) -> PyResult<()> {
        self.inner.adjust_capacity(resource, delta).map_err(core_err)
    }

    fn pool_rebuilt(&self, name: &str) {
        self.inner.pool_rebuilt(name);
    }

    /// Lazy pools: an instance is being discarded; its memory stays held until `unload_done`.
    fn unload_begin(&self, name: &str) -> PyResult<()> {
        self.inner.unload_begin(name).map_err(core_err)
    }

    /// Lazy pools: the instance from `unload_begin` is closed.
    fn unload_done(&self, name: &str) -> PyResult<()> {
        self.inner.unload_done(name).map_err(core_err)
    }

    fn pool_loaded(&self, name: &str) {
        self.inner.pool_loaded(name);
    }

    fn pool_evicted(&self, name: &str) {
        self.inner.pool_evicted(name);
    }

    /// Lazy pools: an instance was built ahead of need (also call `pool_loaded`).
    fn pool_lookahead_loaded(&self, name: &str) {
        self.inner.pool_lookahead_loaded(name);
    }

    /// Lazy pools: `(kind, pool)` means the ledger already counts one more instance of the pool as loading;
    /// build it (kind "build") or move the parked one back (kind "unpark") and call `lookahead_done`. None when
    /// there is nothing to load ahead.
    fn lookahead_begin(&self) -> Option<(&'static str, String)> {
        self.inner.lookahead_begin().map(|(kind, name)| (kind.as_str(), name))
    }

    /// Lazy pools: the instance from `lookahead_begin` is built (`ok`) or its build failed.
    fn lookahead_done(&self, name: &str, ok: bool) -> PyResult<()> {
        self.inner.lookahead_done(name, ok).map_err(core_err)
    }

    /// Lazy pools: a pool name means the ledger already counts a new parked instance of it as staging; build it
    /// straight into the parked state and call `stage_done`. None when no stage is due.
    fn stage_begin(&self) -> Option<String> {
        self.inner.stage_begin()
    }

    /// Lazy pools: `(seconds until it is next needed, workflow nodes using it that are done, all of them)` as the
    /// lookahead loop last estimated, for a log line. None when `--lookahead` isn't estimating or nothing needs it.
    fn pool_need(&self, name: &str) -> Option<(f64, u32, u32)> {
        self.inner.pool_need(name)
    }

    /// Lazy pools: the instance from `stage_begin` is built and parked (`ok`) or its build failed.
    fn stage_done(&self, name: &str, ok: bool) -> PyResult<()> {
        self.inner.stage_done(name, ok).map_err(core_err)
    }

    /// Lazy pools: the park hook of an instance a task was told to park raised; the instance is closed instead.
    fn park_failed(&self, name: &str) -> PyResult<()> {
        self.inner.park_failed(name).map_err(core_err)
    }

    /// Lazy pools: the unpark hook of an instance a task was told to unpark raised; the instance is closed and
    /// the task builds a fresh one.
    fn unpark_failed(&self, name: &str) -> PyResult<()> {
        self.inner.unpark_failed(name).map_err(core_err)
    }

    /// Lazy pools: how long a "build", "stage", "park" or "unpark" of an instance took, in seconds.
    fn pool_timing(&self, name: &str, kind: &str, secs: f64) -> PyResult<()> {
        let kind = MoveKind::parse(kind).ok_or_else(|| {
            PyValueError::new_err(format!("pool timing kind must be build, stage, park or unpark, got {kind:?}"))
        })?;
        self.inner.pool_timing(name, kind, secs).map_err(core_err)
    }

    /// Lazy pools: an instance was moved to host RAM.
    fn pool_parked(&self, name: &str) {
        self.inner.pool_parked(name);
    }

    /// Lazy pools: an instance was moved back from host RAM.
    fn pool_unparked(&self, name: &str) {
        self.inner.pool_unparked(name);
    }

    /// Lazy pools: a parked instance was closed to make room.
    fn pool_dropped(&self, name: &str) {
        self.inner.pool_dropped(name);
    }

    /// Lazy pools: an instance was built straight into host RAM.
    fn pool_staged(&self, name: &str) {
        self.inner.pool_staged(name);
    }

    /// (pool, instances parked or being staged) for each lazy pool.
    fn parked_usage(&self) -> Vec<(String, u32)> {
        self.inner.parked_usage()
    }

    /// Due tasks that need `resource` and don't fit now.
    fn blocked(&self, resource: &str) -> usize {
        self.inner.blocked(resource)
    }

    /// (resource, capacity, in use) for each resource, pools included as `pool:<name>`.
    fn resource_usage(&self) -> Vec<(String, f64, f64)> {
        self.inner.usage()
    }

    /// The GPU samples `/metrics` reports; each push replaces the last.
    fn set_gpu_stats(&self, stats: Vec<GpuArgs>) {
        let stats = stats.into_iter()
            .map(|(index, uuid, utilization, memory_used, memory_total, process_memory, power_watts, temperature_c)| {
                GpuStat { index, uuid, utilization, memory_used, memory_total, process_memory, power_watts, temperature_c }
            })
            .collect();
        self.inner.set_gpu_stats(stats);
    }

    fn stop_fetching(&self) {
        self.inner.stop_fetching();
    }

    /// Records a running task's progress; False if the task is gone or the numbers are unusable. No I/O.
    #[pyo3(signature = (token, done, total=None))]
    fn progress(&self, token: u64, done: f64, total: Option<f64>) -> bool {
        self.inner.progress(token, done, total)
    }

    fn inflight(&self) -> usize {
        self.inner.inflight()
    }

    fn shutdown(&self, py: Python<'_>, grace_ms: u64) {
        py.detach(|| self.inner.shutdown(Duration::from_millis(grace_ms)));
    }
}

#[pyclass(frozen, name = "ReadyTask", module = "mayu._native")]
struct PyReadyTask(ReadyTask);

#[pymethods]
impl PyReadyTask {
    #[getter]
    fn token(&self) -> u64 {
        self.0.token
    }
    #[getter]
    fn id(&self) -> &str {
        &self.0.id
    }
    #[getter]
    fn task(&self) -> &str {
        &self.0.task
    }
    #[getter]
    fn queue(&self) -> &str {
        &self.0.queue
    }
    #[getter]
    fn attempt(&self) -> u32 {
        self.0.attempt
    }
    #[getter]
    fn max_retries(&self) -> u32 {
        self.0.max_retries
    }
    #[getter]
    fn content_type(&self) -> &str {
        &self.0.content_type
    }
    #[getter]
    fn payload<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &self.0.payload)
    }
    #[getter]
    fn trace_ctx(&self) -> Option<&str> {
        self.0.trace_ctx.as_deref()
    }
    /// Lazy pools to build an instance of before running, one entry per instance.
    #[getter]
    fn loads(&self) -> Vec<String> {
        self.0.loads.clone()
    }
    /// Lazy pools to close an idle instance of first, one entry per instance.
    #[getter]
    fn evicts(&self) -> Vec<String> {
        self.0.evicts.clone()
    }
    /// Lazy pools to move an idle instance of to host RAM, after `evicts`, one entry per instance.
    #[getter]
    fn parks(&self) -> Vec<String> {
        self.0.parks.clone()
    }
    /// Lazy pools to move a parked instance of back instead of building one, after `parks`.
    #[getter]
    fn unparks(&self) -> Vec<String> {
        self.0.unparks.clone()
    }
    /// Lazy pools to close a parked instance of before anything else, one entry per instance.
    #[getter]
    fn drops(&self) -> Vec<String> {
        self.0.drops.clone()
    }
    fn __repr__(&self) -> String {
        format!("ReadyTask(id={:?}, task={:?}, attempt={})", self.0.id, self.0.task, self.0.attempt)
    }
}

/// Schedules `exc` to be raised in the thread `thread_id` (a `threading.get_ident()` value) at its
/// next bytecode boundary; `None` clears a pending one. Returns the number of threads affected.
#[pyfunction]
#[pyo3(signature = (thread_id, exc=None))]
fn async_raise(thread_id: u64, exc: Option<Bound<'_, PyAny>>) -> i32 {
    let ptr = exc.as_ref().map_or(std::ptr::null_mut(), |e| e.as_ptr());
    // SAFETY: the interpreter is attached (we hold `Bound`s); the call only reads `ptr` and takes
    // its own reference. pyo3-ffi declares the id as c_long; the cast preserves the bits.
    unsafe { pyo3::ffi::PyThreadState_SetAsyncExc(thread_id as std::os::raw::c_long, ptr) }
}

/// Routes the Rust core's log events to stderr as text or JSON lines. Only the first call installs
/// the subscriber; later calls return `false`.
#[pyfunction]
fn init_logging(format: &str, level: &str) -> PyResult<bool> {
    let level: tracing::Level = match level {
        "debug" => tracing::Level::DEBUG,
        "info" => tracing::Level::INFO,
        "warn" => tracing::Level::WARN,
        "error" => tracing::Level::ERROR,
        _ => return Err(PyValueError::new_err(format!("bad log level {level:?}"))),
    };
    let base = tracing_subscriber::fmt().with_writer(std::io::stderr).with_max_level(level).with_ansi(false);
    let installed = match format {
        "json" => base.json().flatten_event(true).with_current_span(false).with_span_list(false).try_init(),
        "text" => base.with_target(false).try_init(),
        _ => return Err(PyValueError::new_err(format!("bad log format {format:?}"))),
    };
    Ok(installed.is_ok())
}

#[pymodule]
#[pyo3(name = "_native")]
fn mayu_native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Client>()?;
    m.add_class::<Worker>()?;
    m.add_class::<PyReadyTask>()?;
    m.add_function(wrap_pyfunction!(async_raise, m)?)?;
    m.add_function(wrap_pyfunction!(init_logging, m)?)?;
    Ok(())
}
