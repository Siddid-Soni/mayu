use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use tokio::sync::{Notify, Semaphore, mpsc, watch};
use tokio::task::{JoinError, JoinHandle, JoinSet};
use tokio::time::MissedTickBehavior;

use crate::dispatch::{Admit, Dispatcher, ReadyQueue, Resources, Schedule};
use crate::broker::{Broker, Delivery, PublishedTasks, StoredResult, WorkerInfo};
use crate::envelope::{Header, now_ms};
use crate::error::{Error, Result};
use crate::eta::{self, Durations, MoveKind, PoolEta, Running};
use crate::metrics::{Gauges, GpuStat, Metrics, Settled};
use crate::policy::{Action, Outcome, RetryPolicy, decide, full_jitter};
use crate::progress::{MAX_PUBLISHED, Progress, RunningTask};
use crate::result::ResultRecord;
use crate::workflow::NodeState;

/// Heartbeats between consumer-pruning passes.
const PRUNE_EVERY_TICKS: u64 = 12;
/// Exit code when a core loop panics (EX_SOFTWARE).
const PANIC_EXIT_CODE: i32 = 70;
/// Bound on reading queue depths for one scrape.
const SCRAPE_BROKER_TIMEOUT: Duration = Duration::from_secs(2);
/// Workflows remembered for demand; past this the one seen longest ago is forgotten.
const MAX_ACTIVE_WORKFLOWS: usize = 256;
/// How often the lookahead loop re-reads its workflows, absent a settled workflow node.
const LOOKAHEAD_INTERVAL: Duration = Duration::from_secs(2);

#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    pub queues: Vec<String>,
    pub concurrency: usize,
    pub prefetch_multiplier: usize,
    pub result_ttl_secs: Option<u64>,
    pub fetch_block_ms: u64,
    pub promote_interval_ms: u64,
    pub retry: RetryPolicy,
    pub worker_id: String,
    pub visibility_timeout_ms: u64,
    pub reclaim_interval_ms: u64,
    pub heartbeat_interval_ms: u64,
    pub max_deliveries: u64,
    /// Earlier `queues` run first (strict). Otherwise all queues rank equally.
    pub strict_priority: bool,
    /// Task name -> starts per second, per worker process.
    pub rate_limits: HashMap<String, f64>,
    /// What this worker process has and what its tasks use; a task starts only when its request fits.
    pub resources: Resources,
    /// How tasks of a worker with lazy pools are ordered; `Fifo` exactly when there are none.
    pub schedule: Schedule,
    /// Track the workflows this worker runs and publish how much of each lazy pool they still need.
    /// The loop also runs, regardless of this, when a lazy pool has `linger`.
    pub lookahead: bool,
    /// Serve Prometheus metrics here (`GET /metrics`). Port 0 picks a free port.
    pub metrics_addr: Option<SocketAddr>,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        RuntimeConfig {
            queues: vec!["default".into()],
            concurrency: 8,
            prefetch_multiplier: 2,
            result_ttl_secs: None,
            fetch_block_ms: 1_000,
            promote_interval_ms: 100,
            retry: RetryPolicy::default(),
            worker_id: format!("w-{}-{}", std::process::id(), ulid::Ulid::generate()),
            visibility_timeout_ms: 30_000,
            reclaim_interval_ms: 5_000,
            heartbeat_interval_ms: 5_000,
            max_deliveries: 5,
            strict_priority: false,
            rate_limits: HashMap::new(),
            resources: Resources::default(),
            schedule: Schedule::Fifo,
            lookahead: false,
            metrics_addr: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ReadyTask {
    pub token: u64,
    pub id: String,
    pub task: String,
    pub queue: String,
    pub attempt: u32,
    pub max_retries: u32,
    pub content_type: String,
    pub payload: Vec<u8>,
    pub trace_ctx: Option<String>,
    /// Lazy pools only: model pools to build an instance of before the task runs, one entry per instance.
    pub loads: Vec<String>,
    /// Lazy pools only: model pools to close an idle instance of first (before `loads`), one entry per instance.
    pub evicts: Vec<String>,
    /// Lazy pools only: model pools to park an idle instance of (move to host RAM), after `evicts`, one entry per instance.
    pub parks: Vec<String>,
    /// Lazy pools only: model pools to unpark an instance of, after `parks`, instead of building one.
    pub unparks: Vec<String>,
    /// Lazy pools only: model pools to close a parked instance of, first of all, one entry per instance.
    pub drops: Vec<String>,
    /// Unix ms the task's workflow started, from its ULID id; None outside workflows or for other id formats.
    pub workflow_started_ms: Option<u64>,
}

impl ReadyTask {
    fn from_delivery(token: u64, d: &Delivery) -> Self {
        ReadyTask {
            token,
            id: d.header.id.clone(),
            task: d.header.task.clone(),
            queue: d.queue.clone(),
            attempt: d.header.attempt,
            max_retries: d.header.max_retries,
            content_type: d.header.content_type.clone(),
            payload: d.payload.clone(),
            trace_ctx: d.header.trace_ctx.clone(),
            loads: Vec::new(),
            evicts: Vec::new(),
            parks: Vec::new(),
            unparks: Vec::new(),
            drops: Vec::new(),
            workflow_started_ms: d.header.workflow_id.as_deref()
                .and_then(|id| ulid::Ulid::from_string(id).ok())
                .map(|u| u.timestamp_ms()),
        }
    }
}

/// A message this worker has claimed and not yet settled.
struct InFlight {
    delivery: Delivery,
    last_claim: Instant,
    /// Another worker reclaimed it; stop extending.
    lost: bool,
    /// When a Python thread took it; None while it waits in the dispatcher.
    started: Option<Instant>,
    /// What the running code last reported through `Runtime::progress`.
    progress: Option<Progress>,
}

type Inflight = Arc<Mutex<HashMap<u64, InFlight>>>;

struct Handles {
    fetch: JoinHandle<()>,
    complete: JoinHandle<()>,
    promote: JoinHandle<()>,
    keepalive: JoinHandle<()>,
    heartbeat: JoinHandle<()>,
    lookahead: Option<JoinHandle<()>>,
    metrics: Option<JoinHandle<()>>,
}

/// Workflows this worker has fetched a task of, for the lookahead loop.
struct Active {
    seen: Mutex<HashMap<String, Instant>>,
    /// Poked when the loop should read the workflows now: a new one appeared or a node settled.
    wake: Notify,
}

impl Active {
    fn new() -> Self {
        Active { seen: Mutex::new(HashMap::new()), wake: Notify::new() }
    }

    fn touch(&self, id: &str) {
        let mut seen = self.seen.lock().unwrap();
        let new = seen.insert(id.to_string(), Instant::now()).is_none();
        if new && seen.len() > MAX_ACTIVE_WORKFLOWS {
            let oldest = seen.iter().min_by_key(|(_, at)| **at).map(|(id, _)| id.clone());
            if let Some(oldest) = oldest {
                seen.remove(&oldest);
            }
        }
        drop(seen);
        if new {
            self.wake.notify_one();
        }
    }
}

/// What the progress estimate knows. Exists when a lazy pool can park; only used for ordering and staging
/// when `--lookahead` is on too, so every other worker never touches it.
struct EtaState {
    durations: Mutex<Durations>,
    /// (pool, seconds until next needed) as last computed by the lookahead loop; pools with no need left out.
    needs: Mutex<Vec<(String, f64)>>,
    /// (pool, (workflow nodes using it that are done, all of them)), for the pools in `needs`.
    counts: Mutex<HashMap<String, (u32, u32)>>,
}

pub struct Runtime<B: Broker> {
    rt: tokio::runtime::Runtime,
    broker: Arc<B>,
    worker_id: String,
    ready: Arc<ReadyQueue>,
    complete_tx: RwLock<Option<mpsc::UnboundedSender<(u64, Outcome)>>>,
    stop_tx: watch::Sender<bool>,
    inflight: Inflight,
    metrics: Arc<Metrics>,
    metrics_addr: Option<SocketAddr>,
    handles: Mutex<Option<Handles>>,
    /// Lazy pools, and the ones that can park (a parked claim); pool moves are only recorded for the latter.
    pools: HashSet<String>,
    parkable: HashSet<String>,
    eta: Option<Arc<EtaState>>,
}

impl<B: Broker> Runtime<B> {
    pub fn start<F, Fut>(connect: F, cfg: RuntimeConfig) -> Result<Self>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<B>>,
    {
        if cfg.concurrency == 0 || cfg.prefetch_multiplier == 0 {
            return Err(Error::Config("concurrency and prefetch_multiplier must be >= 1".into()));
        }
        if cfg.queues.is_empty() {
            return Err(Error::Config("at least one queue is required".into()));
        }
        if cfg.visibility_timeout_ms == 0 || cfg.reclaim_interval_ms == 0 || cfg.heartbeat_interval_ms == 0 {
            return Err(Error::Config("visibility, reclaim and heartbeat intervals must be > 0".into()));
        }
        let dispatcher = Dispatcher::new(&cfg.queues, cfg.strict_priority, &cfg.rate_limits, &cfg.resources, cfg.schedule)?;
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("mayu-rt")
            .enable_all()
            .build()
            .map_err(|e| Error::Config(e.to_string()))?;
        let listener = match cfg.metrics_addr {
            None => None,
            Some(addr) => {
                let l = std::net::TcpListener::bind(addr)
                    .map_err(|e| Error::Config(format!("cannot serve metrics on {addr}: {e}")))?;
                l.set_nonblocking(true).map_err(|e| Error::Config(e.to_string()))?;
                Some(l)
            }
        };
        let broker = Arc::new(rt.block_on(async {
            let b = connect().await?;
            b.ensure_queues(&cfg.queues).await?;
            Ok::<_, Error>(b)
        })?);

        let cap = cfg.concurrency * cfg.prefetch_multiplier;
        let ready = Arc::new(ReadyQueue::new(dispatcher));
        let (complete_tx, complete_rx) = mpsc::unbounded_channel();
        let (stop_tx, stop_rx) = watch::channel(false);
        let sem = Arc::new(Semaphore::new(cap));
        let inflight: Inflight = Arc::default();
        let metrics = Arc::new(Metrics::default());
        let lookahead = !cfg.resources.lazy.is_empty()
            && (cfg.lookahead || cfg.resources.lazy.values().any(|p| p.linger.is_some()));
        let active = lookahead.then(|| Arc::new(Active::new()));
        let pools: HashSet<String> = cfg.resources.lazy.keys().cloned().collect();
        let mut parkable: Vec<String> = cfg.resources.lazy.iter()
            .filter(|(_, p)| !p.parked_claim.is_empty()).map(|(n, _)| n.clone()).collect();
        parkable.sort();
        let eta = (!parkable.is_empty()).then(|| Arc::new(EtaState {
            durations: Mutex::new(Durations::default()),
            needs: Mutex::new(Vec::new()),
            counts: Mutex::new(HashMap::new()),
        }));
        // Run times are only gathered, and needs only computed, for a worker that can use them to park or stage.
        let eta_in_use = if cfg.lookahead && lookahead { eta.clone() } else { None };

        let fetch = rt.spawn(critical("fetch", fetch_loop(FetchCtx {
            broker: broker.clone(),
            queues: cfg.queues.clone(),
            sem: sem.clone(),
            batch: cap,
            block_ms: cfg.fetch_block_ms,
            inflight: inflight.clone(),
            ready: ready.clone(),
            strict_priority: cfg.strict_priority,
            stop: stop_rx.clone(),
            visibility_ms: cfg.visibility_timeout_ms,
            reclaim_interval: Duration::from_millis(cfg.reclaim_interval_ms),
            max_deliveries: cfg.max_deliveries,
            result_ttl: cfg.result_ttl_secs,
            metrics: metrics.clone(),
            active: active.clone(),
        })));
        let complete = rt.spawn(critical("completion", completion_loop(
            broker.clone(),
            complete_rx,
            inflight.clone(),
            sem,
            cfg.retry.clone(),
            cfg.result_ttl_secs,
            Duration::from_millis(cfg.visibility_timeout_ms),
            metrics.clone(),
            active.clone(),
            eta_in_use.clone(),
        )));
        let promote = rt.spawn(critical("promote",
            promote_loop(broker.clone(), cfg.queues.clone(), cfg.promote_interval_ms, stop_rx.clone(), metrics.clone())));
        let lookahead_task = active.map(|active| {
            rt.spawn(critical("lookahead", lookahead_loop(LookaheadCtx {
                broker: broker.clone(),
                active,
                ready: ready.clone(),
                task_pools: task_pools(&cfg.resources),
                pools: cfg.resources.lazy.keys().cloned().collect(),
                stop: stop_rx,
                metrics: metrics.clone(),
                inflight: inflight.clone(),
                eta: eta_in_use,
                worker_id: cfg.worker_id.clone(),
                heartbeat: Duration::from_millis(cfg.heartbeat_interval_ms.max(10)),
            })))
        });
        let keepalive = rt.spawn(critical("keepalive",
            keepalive_loop(broker.clone(), inflight.clone(), cfg.visibility_timeout_ms, metrics.clone())));
        let info = WorkerInfo {
            worker_id: cfg.worker_id.clone(),
            hostname: hostname(),
            pid: std::process::id(),
            queues: cfg.queues.clone(),
            concurrency: cfg.concurrency,
            inflight: 0,
            started_at: now_ms(),
            last_seen: 0,
            tasks: Vec::new(),
        };
        let prune_idle_ms = cfg.visibility_timeout_ms.max(cfg.heartbeat_interval_ms.saturating_mul(3)).saturating_mul(2);
        let heartbeat = rt.spawn(critical("heartbeat",
            heartbeat_loop(broker.clone(), info, inflight.clone(), cfg.heartbeat_interval_ms, prune_idle_ms, metrics.clone())));
        let (metrics_task, metrics_addr) = match listener {
            None => (None, None),
            Some(l) => {
                let addr = l.local_addr().map_err(|e| Error::Config(e.to_string()))?;
                let l = {
                    let _guard = rt.enter();
                    tokio::net::TcpListener::from_std(l).map_err(|e| Error::Config(e.to_string()))?
                };
                let (b, m, inf, queues, concurrency) =
                    (broker.clone(), metrics.clone(), inflight.clone(), cfg.queues.clone(), cfg.concurrency);
                let ready = ready.clone();
                let pv = Arc::new(PoolView { parkable: parkable.clone(), eta: eta.clone() });
                let render: crate::http::Render = Arc::new(move || {
                    let (b, m, inf, queues, ready) = (b.clone(), m.clone(), inf.clone(), queues.clone(), ready.clone());
                    let pv = pv.clone();
                    Box::pin(async move { scrape(&*b, &m, &inf, &ready, &pv, &queues, concurrency, lookahead).await })
                });
                // Not `critical`: a broken metrics endpoint must not take the worker down.
                (Some(rt.spawn(crate::http::serve(l, render))), Some(addr))
            }
        };

        Ok(Runtime {
            rt,
            broker,
            worker_id: cfg.worker_id,
            ready,
            complete_tx: RwLock::new(Some(complete_tx)),
            stop_tx,
            inflight,
            metrics,
            metrics_addr,
            handles: Mutex::new(Some(Handles {
                fetch, complete, promote, keepalive, heartbeat, lookahead: lookahead_task, metrics: metrics_task,
            })),
            pools,
            parkable: parkable.into_iter().collect(),
            eta,
        })
    }

    pub fn worker_id(&self) -> &str {
        &self.worker_id
    }

    /// Where `/metrics` is served (the bound port, also when configured with port 0).
    pub fn metrics_addr(&self) -> Option<SocketAddr> {
        self.metrics_addr
    }

    pub fn recv_timeout(&self, timeout: Duration) -> Option<ReadyTask> {
        let task = self.ready.recv_timeout(timeout)?;
        if *self.stop_tx.borrow() {
            self.ready.release(task.token);
            // Fetched by a read that was already in flight when fetching stopped: don't start new
            // work while stopping; give it back to the queue (as `shutdown` does for the rest).
            let f = self.inflight.lock().unwrap().remove(&task.token);
            if let Some(f) = f {
                if let Err(e) = self.rt.block_on(self.broker.requeue(&f.delivery)) {
                    self.metrics.broker_error("requeue");
                    tracing::warn!(error = %e, task_id = %f.delivery.header.id, "requeue after stop failed");
                }
            }
            return None;
        }
        if let Some(f) = self.inflight.lock().unwrap().get_mut(&task.token) {
            f.started = Some(Instant::now());
        }
        Some(task)
    }

    /// Records a running task's progress. False if the token is no longer in flight or the numbers are unusable.
    pub fn progress(&self, token: u64, done: f64, total: Option<f64>) -> bool {
        let now = Instant::now();
        let mut m = self.inflight.lock().unwrap();
        let Some(f) = m.get_mut(&token) else { return false };
        let ok = match &mut f.progress {
            Some(p) => p.report(now, done, total),
            none => match Progress::new(now, done, total) {
                Some(p) => {
                    *none = Some(p);
                    true
                }
                None => false,
            },
        };
        if ok {
            self.metrics.progress_reported(&f.delivery.queue, &f.delivery.header.task);
        }
        ok
    }

    /// Tasks running now, oldest first, at most `MAX_PUBLISHED`.
    pub fn running(&self) -> Vec<RunningTask> {
        running_snapshot(&self.inflight, Instant::now(), now_ms())
    }

    pub fn complete(&self, token: u64, outcome: Outcome) {
        // Before the outcome is sent: the code has stopped running, so its resources are free now.
        self.ready.release(token);
        if let Some(tx) = self.complete_tx.read().unwrap().as_ref() {
            let _ = tx.send((token, outcome));
        }
    }

    /// Changes a resource's capacity while running (pools shrink and regrow it); see `Dispatcher::adjust_capacity`.
    pub fn adjust_capacity(&self, resource: &str, delta: f64) -> Result<()> {
        self.ready.adjust_capacity(resource, delta)
    }

    pub fn pool_rebuilt(&self, pool: &str) {
        self.metrics.pool_rebuilt(pool);
    }

    /// See `Dispatcher::unload_begin`.
    pub fn unload_begin(&self, pool: &str) -> Result<()> {
        self.ready.unload_begin(pool)
    }

    /// See `Dispatcher::unload_done`.
    pub fn unload_done(&self, pool: &str) -> Result<()> {
        self.ready.unload_done(pool)
    }

    /// A lazy pool instance was built.
    pub fn pool_loaded(&self, pool: &str) {
        self.metrics.pool_loaded(pool);
    }

    /// A lazy pool instance was closed to make room (after the close).
    pub fn pool_evicted(&self, pool: &str) {
        self.metrics.pool_evicted(pool);
    }

    /// A lazy pool instance was built ahead of need, in addition to `pool_loaded`.
    pub fn pool_lookahead_loaded(&self, pool: &str) {
        self.metrics.pool_lookahead_loaded(pool);
    }

    /// See `Dispatcher::lookahead_begin`.
    pub fn lookahead_begin(&self) -> Option<(crate::dispatch::LookaheadKind, String)> {
        self.ready.lookahead_begin()
    }

    /// See `Dispatcher::lookahead_done`.
    pub fn lookahead_done(&self, pool: &str, ok: bool) -> Result<()> {
        self.ready.lookahead_done(pool, ok)
    }

    /// See `Dispatcher::stage_begin`.
    pub fn stage_begin(&self) -> Option<String> {
        self.ready.stage_begin()
    }

    /// See `Dispatcher::stage_done`.
    pub fn stage_done(&self, pool: &str, ok: bool) -> Result<()> {
        self.ready.stage_done(pool, ok)
    }

    /// See `Dispatcher::park_failed`.
    pub fn park_failed(&self, pool: &str) -> Result<()> {
        self.ready.park_failed(pool)
    }

    /// See `Dispatcher::unpark_failed`.
    pub fn unpark_failed(&self, pool: &str) -> Result<()> {
        self.ready.unpark_failed(pool)
    }

    /// (pool, instances parked or being staged) for each lazy pool.
    pub fn parked_usage(&self) -> Vec<(String, u32)> {
        self.ready.parked_usage()
    }

    /// A lazy pool instance was moved to host RAM (after the move).
    pub fn pool_parked(&self, pool: &str) {
        self.metrics.pool_parked(pool);
    }

    /// A lazy pool instance was moved back from host RAM (after the move).
    pub fn pool_unparked(&self, pool: &str) {
        self.metrics.pool_unparked(pool);
    }

    /// A parked lazy pool instance was closed to make room (after the close).
    pub fn pool_dropped(&self, pool: &str) {
        self.metrics.pool_dropped(pool);
    }

    /// A lazy pool instance was built straight into host RAM (after the build).
    pub fn pool_staged(&self, pool: &str) {
        self.metrics.pool_staged(pool);
    }

    /// Reports how long a `build`, `stage`, `park` or `unpark` of a pool instance took. Stage and unpark times
    /// set how early a stage is due (`Dispatcher::set_move_times`). Ignored for a pool that can't park, so a
    /// worker without parked pools reports nothing new. An unknown pool is an error.
    pub fn pool_timing(&self, pool: &str, kind: MoveKind, secs: f64) -> Result<()> {
        if !self.pools.contains(pool) {
            return Err(Error::Config(format!("{pool:?} is not a lazy pool")));
        }
        let Some(eta) = self.eta.as_ref().filter(|_| self.parkable.contains(pool)) else { return Ok(()) };
        let (stage, unpark) = {
            let mut d = eta.durations.lock().unwrap();
            if d.record_move(pool, kind, secs).is_none() {
                return Ok(());
            }
            (d.move_secs(pool, MoveKind::Stage), d.move_secs(pool, MoveKind::Unpark))
        };
        self.metrics.pool_move(pool, kind.as_str(), secs);
        if matches!(kind, MoveKind::Stage | MoveKind::Unpark) {
            self.ready.set_move_times(pool, stage, unpark)?;
        }
        Ok(())
    }

    /// Seeds the run time the progress estimate assumes for `task`, as if it had run for `secs`. For tests.
    #[doc(hidden)]
    pub fn seed_task_secs(&self, task: &str, secs: f64) {
        if let Some(eta) = &self.eta {
            eta.durations.lock().unwrap().record_task(task, secs);
        }
    }

    /// When `pool` is next needed, as the lookahead loop last estimated: (seconds from now, workflow nodes using
    /// it that are done, all of them). None when `--lookahead` isn't estimating, or nothing needs the pool.
    pub fn pool_need(&self, pool: &str) -> Option<(f64, u32, u32)> {
        let eta = self.eta.as_ref()?;
        let secs = eta.needs.lock().unwrap().iter().find(|(p, _)| p == pool)?.1;
        let (done, total) = eta.counts.lock().unwrap().get(pool).copied().unwrap_or((0, 0));
        Some((secs, done, total))
    }

    /// See `Dispatcher::blocked`.
    pub fn blocked(&self, resource: &str) -> usize {
        self.ready.blocked(resource)
    }

    /// (resource, capacity, in use), as `/metrics` reports them.
    pub fn usage(&self) -> Vec<(String, f64, f64)> {
        self.ready.usage()
    }

    /// Replaces the GPU samples `/metrics` reports.
    pub fn set_gpu_stats(&self, stats: Vec<GpuStat>) {
        self.metrics.set_gpus(stats);
    }

    pub fn stop_fetching(&self) {
        self.stop_tx.send_replace(true);
    }

    pub fn inflight(&self) -> usize {
        self.inflight.lock().unwrap().len()
    }

    /// Tasks the dispatcher holds that no thread has taken yet. For tests.
    #[doc(hidden)]
    pub fn queued(&self) -> usize {
        self.ready.len()
    }

    pub fn shutdown(&self, grace: Duration) {
        self.stop_fetching();
        let Some(h) = self.handles.lock().unwrap().take() else { return };
        self.rt.block_on(async {
            let _ = h.fetch.await;
            let _ = h.promote.await;
            if let Some(l) = h.lookahead {
                let _ = l.await; // stops on the same signal
            }
            let leftovers = self.ready.drain();
            for t in leftovers {
                let f = self.inflight.lock().unwrap().remove(&t.token);
                if let Some(f) = f {
                    if let Err(e) = self.broker.requeue(&f.delivery).await {
                        self.metrics.broker_error("requeue");
                        tracing::warn!(error = %e, task_id = %f.delivery.header.id, "requeue on shutdown failed");
                    }
                }
            }
            self.complete_tx.write().unwrap().take();
            if tokio::time::timeout(grace, h.complete).await.is_err() {
                tracing::warn!("completion loop did not drain within shutdown grace");
            }
            // Tasks still running past the grace period are reclaimed by other workers.
            h.keepalive.abort();
            h.heartbeat.abort();
            let _ = h.keepalive.await;
            let _ = h.heartbeat.await;
            if let Some(m) = h.metrics {
                m.abort();
                let _ = m.await;
            }
            if let Err(e) = self.broker.unregister(&self.worker_id).await {
                tracing::warn!(error = %e, "unregistering worker failed");
            }
        });
    }
}

/// Runs a core loop; a panic inside it terminates the process (fail fast, spec §5.2).
/// Cancellation (abort, runtime drop) is not a panic and exits quietly.
async fn critical<F: Future<Output = ()>>(name: &'static str, fut: F) {
    struct ExitOnPanic(&'static str);
    impl Drop for ExitOnPanic {
        fn drop(&mut self) {
            if std::thread::panicking() {
                eprintln!("mayu: fatal: panic in the {} loop; exiting", self.0);
                std::process::exit(PANIC_EXIT_CODE);
            }
        }
    }
    let _guard = ExitOnPanic(name);
    fut.await;
}

fn hostname() -> String {
    std::fs::read_to_string("/proc/sys/kernel/hostname")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .or_else(|| std::env::var("HOSTNAME").ok())
        .unwrap_or_else(|| "unknown".into())
}

struct FetchCtx<B> {
    broker: Arc<B>,
    queues: Vec<String>,
    sem: Arc<Semaphore>,
    batch: usize,
    block_ms: u64,
    inflight: Inflight,
    ready: Arc<ReadyQueue>,
    strict_priority: bool,
    stop: watch::Receiver<bool>,
    visibility_ms: u64,
    reclaim_interval: Duration,
    max_deliveries: u64,
    result_ttl: Option<u64>,
    metrics: Arc<Metrics>,
    /// Some when the lookahead loop runs.
    active: Option<Arc<Active>>,
}

/// How long shutdown waits for the acks of skipped (cancelled-workflow) deliveries still being written.
const SKIP_DRAIN: Duration = Duration::from_secs(5);

async fn fetch_loop<B: Broker>(mut c: FetchCtx<B>) {
    let mut skips = JoinSet::new();
    fetch_batches(&mut c, &mut skips).await;
    let drain = async {
        while let Some(done) = skips.join_next().await {
            reap(done);
        }
    };
    if tokio::time::timeout(SKIP_DRAIN, drain).await.is_err() {
        tracing::warn!("acks of skipped workflow tasks did not finish at shutdown; they will be reclaimed");
    }
}

async fn fetch_batches<B: Broker>(c: &mut FetchCtx<B>, skips: &mut JoinSet<()>) {
    let mut next_token: u64 = 1;
    let mut next_reclaim = Instant::now();
    let mut cursors: HashMap<String, String> = HashMap::new();
    let mut order = c.queues.clone();
    loop {
        while let Some(done) = skips.try_join_next() {
            reap(done);
        }
        let permit = tokio::select! {
            p = c.sem.clone().acquire_owned() => match p { Ok(p) => p, Err(_) => return },
            _ = c.stop.wait_for(|s| *s) => return,
        };
        permit.forget();
        let mut n = 1;
        while n < c.batch {
            match c.sem.try_acquire() {
                Ok(p) => { p.forget(); n += 1; }
                Err(_) => break,
            }
        }
        let mut got: Vec<Delivery> = Vec::new();
        if Instant::now() >= next_reclaim {
            next_reclaim = Instant::now() + c.reclaim_interval;
            got = reclaim_due(c, &mut cursors, n).await;
        }
        if got.is_empty() {
            if !c.strict_priority {
                order.rotate_left(1); // equal priority: queues take turns being claimed from first
            }
            // Not cancelled on stop: a cancelled XREADGROUP could claim messages we never see.
            match c.broker.fetch(&order, n, c.block_ms).await {
                Ok(d) => got = d,
                Err(e) => {
                    c.metrics.broker_error("fetch");
                    tracing::warn!(error = %e, "fetch failed; backing off");
                    c.sem.add_permits(n);
                    tokio::select! {
                        _ = tokio::time::sleep(Duration::from_millis(500)) => {}
                        _ = c.stop.wait_for(|s| *s) => return,
                    }
                    continue;
                }
            }
        }
        c.sem.add_permits(n.saturating_sub(got.len()));
        let overshoot = got.len().saturating_sub(n);
        // Only a batch with workflow nodes pays for the cancelled check.
        let got = if got.iter().any(|d| d.header.workflow_id.is_some()) {
            skip_cancelled(c, skips, got).await
        } else {
            got
        };
        // Only a batch with an expiring task pays for the deadline check.
        let got = if got.iter().any(|d| d.header.expires.is_some()) {
            skip_expired(c, skips, got)
        } else {
            got
        };
        for d in got {
            c.metrics.received(&d.queue, &d.header.task);
            if let (Some(active), Some(id)) = (&c.active, &d.header.workflow_id) {
                active.touch(id);
            }
            let token = next_token;
            next_token += 1;
            let task = ReadyTask::from_delivery(token, &d);
            c.inflight.lock().unwrap().insert(token, InFlight { delivery: d, last_claim: Instant::now(), lost: false, started: None, progress: None });
            if let Admit::Defer { task, delay } = c.ready.push(task, Instant::now()) {
                defer(c, *task, delay).await;
            }
        }
        if overshoot > 0 {
            // A blocking read across queues may claim up to one entry per queue beyond `n`: take
            // those permits back before claiming more, so in-flight work stays bounded.
            tokio::select! {
                p = c.sem.clone().acquire_many_owned(overshoot as u32) => match p {
                    Ok(p) => p.forget(),
                    Err(_) => return,
                },
                _ = c.stop.wait_for(|s| *s) => return,
            }
        }
        if *c.stop.borrow() { return; }
    }
}

/// Splits off the deliveries of cancelled workflows and acks them without running them. If the
/// cancelled check fails, everything is dispatched (running a cancelled node is allowed; losing one
/// is not). A skipped delivery keeps its prefetch permit until its ack is written, like a completion.
async fn skip_cancelled<B: Broker>(c: &FetchCtx<B>, skips: &mut JoinSet<()>, got: Vec<Delivery>) -> Vec<Delivery> {
    let mut ids: Vec<String> = Vec::new();
    for id in got.iter().filter_map(|d| d.header.workflow_id.as_ref()) {
        if !ids.contains(id) {
            ids.push(id.clone());
        }
    }
    let cancelled = match c.broker.cancelled_workflows(&ids).await {
        Ok(v) if v.is_empty() => return got,
        Ok(v) => v,
        Err(e) => {
            c.metrics.broker_error("cancelled");
            tracing::warn!(error = %e, "checking for cancelled workflows failed; dispatching normally");
            return got;
        }
    };
    let (skip, run): (Vec<_>, Vec<_>) =
        got.into_iter().partition(|d| d.header.workflow_id.as_ref().is_some_and(|id| cancelled.contains(id)));
    for d in skip {
        ack_skipped(c, skips, d, "workflow cancelled", Settled::Cancelled, "skipped a task of a cancelled workflow");
    }
    run
}

/// Splits off deliveries whose `expires` deadline has already passed and acks them without running
/// them, settled as `expired`. Like `skip_cancelled`, a skipped delivery keeps its prefetch permit
/// until its ack is written, so the permit is returned by the spawned task, not the caller.
fn skip_expired<B: Broker>(c: &FetchCtx<B>, skips: &mut JoinSet<()>, got: Vec<Delivery>) -> Vec<Delivery> {
    let now = now_ms();
    let (skip, run): (Vec<_>, Vec<_>) =
        got.into_iter().partition(|d| d.header.expires.is_some_and(|e| now >= e));
    for d in skip {
        ack_skipped(c, skips, d, "task expired", Settled::Expired, "dropped an expired task");
    }
    run
}

/// Acks a claimed delivery without running it, on `skips` so it keeps its prefetch permit until the
/// ack is written (the spawned task returns the permit). Shared by every claim-time skip reason.
fn ack_skipped<B: Broker>(c: &FetchCtx<B>, skips: &mut JoinSet<()>, d: Delivery, error: &'static str,
                          how: Settled, log: &'static str) {
    c.metrics.received(&d.queue, &d.header.task);
    let stored = c.result_ttl.map(|ttl_secs| StoredResult {
        id: d.header.id.clone(),
        record: ResultRecord::failure(error, "", &d.header.content_type).encode(),
        ttl_secs,
    });
    let (broker, metrics, sem) = (c.broker.clone(), c.metrics.clone(), c.sem.clone());
    let deadline = Duration::from_millis(c.visibility_ms);
    skips.spawn(async move {
        let rec = Record::Ack(stored, Vec::new());
        if record_retrying(&*broker, &d, &rec, deadline, &metrics).await {
            metrics.settled(&d.queue, &d.header.task, how, None);
            tracing::info!(task_id = %d.header.id, task = %d.header.task, "{}", log);
        }
        sem.add_permits(1);
    });
}

/// Returns a throttled task to the broker's delayed set (attempt unchanged), freeing its slot.
/// If the broker is unreachable it is held here until its slot instead.
async fn defer<B: Broker>(c: &FetchCtx<B>, task: ReadyTask, delay: Duration) {
    let Some(f) = c.inflight.lock().unwrap().remove(&task.token) else { return };
    let mut header = f.delivery.header.clone();
    header.eta = Some(now_ms().saturating_add(delay.as_millis() as u64));
    match c.broker.retry(&f.delivery, &header).await {
        Ok(()) => {
            c.metrics.deferred(&task.queue, &task.task);
            c.sem.add_permits(1)
        }
        Err(e) => {
            c.metrics.broker_error("defer");
            tracing::warn!(error = %e, task_id = %header.id, "deferring a rate-limited task failed; holding it");
            c.inflight.lock().unwrap().insert(task.token, f);
            c.ready.hold(task, Instant::now() + delay);
        }
    }
}

/// Claims messages idle longer than the visibility timeout (their worker died).
/// Messages delivered more than `max_deliveries` times are dead-lettered instead.
async fn reclaim_due<B: Broker>(c: &FetchCtx<B>, cursors: &mut HashMap<String, String>, limit: usize) -> Vec<Delivery> {
    let mut got = Vec::new();
    for q in &c.queues {
        if got.len() >= limit { break; }
        let cursor = cursors.get(q).cloned().unwrap_or_else(|| "0-0".to_string());
        match c.broker.reclaim(q, c.visibility_ms, &cursor, limit - got.len()).await {
            Ok(r) => {
                cursors.insert(q.clone(), r.next_cursor);
                for d in r.deliveries {
                    if d.delivery_count > c.max_deliveries {
                        bury_poison(c, &d).await;
                    } else {
                        tracing::info!(task_id = %d.header.id, deliveries = d.delivery_count, "reclaimed abandoned task");
                        c.metrics.reclaimed(q);
                        got.push(d);
                    }
                }
            }
            Err(e) => {
                c.metrics.broker_error("reclaim");
                tracing::warn!(error = %e, queue = %q, "reclaim failed");
            }
        }
    }
    got
}

async fn bury_poison<B: Broker>(c: &FetchCtx<B>, d: &Delivery) {
    let error = format!("poison message: delivered {} times without completing", d.delivery_count);
    let stored = c.result_ttl.map(|ttl_secs| StoredResult {
        id: d.header.id.clone(),
        record: ResultRecord::failure(&error, "", &d.header.content_type).encode(),
        ttl_secs,
    });
    match c.broker.dead_letter(d, &error, stored).await {
        Ok(()) => c.metrics.settled(&d.queue, &d.header.task, Settled::Dead, None),
        Err(e) => {
            c.metrics.broker_error("dead_letter");
            tracing::error!(error = %e, task_id = %d.header.id, "failed to dead-letter poison message");
        }
    }
}

/// How an outcome is written to the broker.
enum Record {
    Ack(Option<StoredResult>, Vec<(Header, Vec<u8>)>),
    Retry(Header),
    Dead(String, Option<StoredResult>),
}

async fn record<B: Broker>(broker: &B, d: &Delivery, r: &Record) -> Result<()> {
    match r {
        Record::Ack(stored, follow_ups) => broker.ack(d, stored.clone(), follow_ups).await,
        Record::Retry(header) => broker.retry(d, header).await,
        Record::Dead(error, stored) => broker.dead_letter(d, error, stored.clone()).await,
    }
}

/// Writes `r`, retrying with backoff while the broker errors, for at most `deadline`. False if it
/// gave up: the message is then reclaimed and runs again.
async fn record_retrying<B: Broker>(broker: &B, d: &Delivery, r: &Record, deadline: Duration, metrics: &Metrics) -> bool {
    let began = Instant::now();
    let mut delay = Duration::from_millis(100);
    loop {
        match record(broker, d, r).await {
            Ok(()) => return true,
            Err(e) if began.elapsed() + delay < deadline => {
                metrics.broker_error("record");
                tracing::warn!(error = %e, task_id = %d.header.id, "recording outcome failed; retrying");
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(Duration::from_secs(5));
            }
            Err(e) => {
                metrics.broker_error("record");
                tracing::error!(error = %e, task_id = %d.header.id,
                    "giving up recording outcome; the message will be reclaimed and run again");
                return false;
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn completion_loop<B: Broker>(
    broker: Arc<B>,
    mut rx: mpsc::UnboundedReceiver<(u64, Outcome)>,
    inflight: Inflight,
    sem: Arc<Semaphore>,
    retry: RetryPolicy,
    ttl: Option<u64>,
    record_deadline: Duration,
    metrics: Arc<Metrics>,
    active: Option<Arc<Active>>,
    eta: Option<Arc<EtaState>>,
) {
    // Outcomes are decided here, in order, but written concurrently: a write is one broker round
    // trip, and awaiting each before taking the next capped a worker at 1 / RTT. Each write holds
    // its message's prefetch permit until `add_permits(1)`, so at most concurrency * prefetch
    // writes are outstanding and no further bound is needed.
    let mut writes = JoinSet::new();
    loop {
        let (token, outcome) = tokio::select! {
            got = rx.recv() => match got {
                Some(c) => c,
                None => break,
            },
            Some(done) = writes.join_next(), if !writes.is_empty() => {
                reap(done);
                continue;
            }
        };
        // Unknown token: a zombie finished after its message was settled. Drop it.
        let Some(f) = inflight.lock().unwrap().remove(&token) else { continue };
        let started = f.started;
        let d = f.delivery;
        let ct = d.header.content_type.clone();
        let rec = match decide(&d.header, outcome, &retry, now_ms(), full_jitter) {
            Action::Ack { result, links } => {
                let stored = match (result, ttl) {
                    (Some(v), Some(ttl_secs)) => Some(StoredResult {
                        id: d.header.id.clone(),
                        record: ResultRecord::success(v, &ct).encode(),
                        ttl_secs,
                    }),
                    _ => None,
                };
                // Built once, so retried records reuse the same callback ids.
                let follow_ups = links.into_iter().map(|l| {
                    let mut h = Header::new(l.task, l.queue, l.max_retries, ct.clone());
                    h.trace_ctx = d.header.trace_ctx.clone();
                    (h, l.payload)
                }).collect();
                Record::Ack(stored, follow_ups)
            }
            Action::Retry { header } => Record::Retry(header),
            Action::Dead { error, traceback } => {
                let stored = ttl.map(|ttl_secs| StoredResult {
                    id: d.header.id.clone(),
                    record: ResultRecord::failure(&error, &traceback, &ct).encode(),
                    ttl_secs,
                });
                Record::Dead(error, stored)
            }
        };
        let how = match &rec {
            Record::Ack(..) => Settled::Success,
            Record::Retry(_) => Settled::Retry,
            Record::Dead(..) => Settled::Dead,
        };
        if let (Some(eta), Settled::Success, Some(started)) = (&eta, how, started) {
            eta.durations.lock().unwrap().record_task(&d.header.task, started.elapsed().as_secs_f64());
        }
        // Buffer through broker outages (bounded by the prefetch permits this message holds).
        let (broker, metrics, sem) = (broker.clone(), metrics.clone(), sem.clone());
        let active = active.clone().filter(|_| d.header.workflow_id.is_some());
        writes.spawn(async move {
            if record_retrying(&*broker, &d, &rec, record_deadline, &metrics).await {
                metrics.settled(&d.queue, &d.header.task, how, started.map(|s| s.elapsed()));
                log_settled(&d, &rec);
                // The workflow just changed; its remaining demand with it.
                if let Some(active) = active {
                    active.wake.notify_one();
                }
            }
            sem.add_permits(1);
        });
    }
    // The channel is closed (shutdown): finish the writes still in flight before returning.
    while let Some(done) = writes.join_next().await {
        reap(done);
    }
}

/// A write task only ends in an error by panicking; take the worker down, as a panic in the
/// loop itself would.
fn reap(done: std::result::Result<(), JoinError>) {
    if let Err(e) = done {
        if let Ok(p) = e.try_into_panic() {
            std::panic::resume_unwind(p);
        }
    }
}

fn log_settled(d: &Delivery, rec: &Record) {
    let (id, task, queue) = (&d.header.id, &d.header.task, &d.queue);
    match rec {
        Record::Ack(..) => tracing::debug!(task_id = %id, task = %task, queue = %queue, "task succeeded"),
        Record::Retry(h) => tracing::info!(task_id = %id, task = %task, queue = %queue, attempt = h.attempt, "task will retry"),
        Record::Dead(error, _) => tracing::warn!(task_id = %id, task = %task, queue = %queue, error = %error, "task failed; dead-lettered"),
    }
}

async fn promote_loop<B: Broker>(
    broker: Arc<B>,
    queues: Vec<String>,
    interval_ms: u64,
    mut stop: watch::Receiver<bool>,
    metrics: Arc<Metrics>,
) {
    let mut tick = tokio::time::interval(Duration::from_millis(interval_ms.max(1)));
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = tick.tick() => {}
            _ = stop.wait_for(|s| *s) => return,
        }
        for q in &queues {
            if let Err(e) = broker.promote(q, now_ms(), 1_000).await {
                metrics.broker_error("promote");
                tracing::warn!(error = %e, queue = %q, "promote failed");
            }
        }
    }
}

struct LookaheadCtx<B> {
    broker: Arc<B>,
    active: Arc<Active>,
    ready: Arc<ReadyQueue>,
    /// Served task -> the lazy pools it uses.
    task_pools: HashMap<String, Vec<String>>,
    /// Every lazy pool, so a pool with no demand is set to 0.
    pools: Vec<String>,
    stop: watch::Receiver<bool>,
    metrics: Arc<Metrics>,
    inflight: Inflight,
    /// Some when pools park and `--lookahead` is on: the loop also estimates when each pool is next needed.
    eta: Option<Arc<EtaState>>,
    worker_id: String,
    /// How long a read of the other workers' running tasks stays good: they publish once per heartbeat.
    heartbeat: Duration,
}

/// What other workers' last heartbeats said about the tasks running there, with when it was read.
#[derive(Default)]
struct RemoteRunning {
    read_at: Option<Instant>,
    /// Task id -> seconds left as of `read_at`, for tasks that report a usable estimate.
    remaining: HashMap<String, f64>,
}

impl RemoteRunning {
    fn stale(&self, max_age: Duration) -> bool {
        self.read_at.is_none_or(|at| at.elapsed() >= max_age)
    }

    /// Reads the other workers' published tasks. A failed read leaves them unknown (the estimate falls back to
    /// run-time averages) and is not retried before the next heartbeat. Every worker whose field can't be parsed
    /// counts as a `progress` broker error per read, so a bad writer shows up on `/metrics` until it goes away.
    async fn refresh<B: Broker>(&mut self, broker: &B, own: &str, metrics: &Metrics) {
        self.read_at = Some(Instant::now());
        self.remaining.clear();
        match broker.running_all().await {
            Ok(PublishedTasks { tasks, malformed }) => {
                for _ in 0..malformed {
                    metrics.broker_error("progress");
                }
                for (worker, t) in tasks {
                    if let Some(r) = t.remaining_s.filter(|r| r.is_finite() && worker != own) {
                        self.remaining.insert(t.id, r);
                    }
                }
            }
            Err(e) => {
                metrics.broker_error("progress");
                tracing::warn!(error = %e, "reading other workers' running tasks failed");
            }
        }
    }

    /// The estimates as of now: each has aged by the time since the read.
    fn running(&self) -> impl Iterator<Item = (String, Running)> + '_ {
        let age = self.read_at.map_or(0.0, |at| at.elapsed().as_secs_f64());
        self.remaining.iter().map(move |(id, r)| {
            (id.clone(), Running { elapsed: 0.0, remaining: Some((r - age).max(0.0)) })
        })
    }
}

/// Served task -> the lazy pools its request names as `pool:<name>`; tasks using none are left out.
fn task_pools(res: &Resources) -> HashMap<String, Vec<String>> {
    res.requests.iter()
        .filter_map(|(task, req)| {
            let mut pools: Vec<String> = res.lazy.keys()
                .filter(|p| req.contains_key(&format!("pool:{p}")))
                .cloned()
                .collect();
            pools.sort();
            (!pools.is_empty()).then(|| (task.clone(), pools))
        })
        .collect()
}

/// Keeps the dispatcher's per-pool demand current: workflow nodes this worker's workflows have not run yet
/// and that use the pool. Reads workflows one at a time, only those this worker has fetched a task of.
async fn lookahead_loop<B: Broker>(mut c: LookaheadCtx<B>) {
    let mut tick = tokio::time::interval(LOOKAHEAD_INTERVAL);
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut remote = RemoteRunning::default();
    loop {
        tokio::select! {
            _ = tick.tick() => {}
            _ = c.active.wake.notified() => {}
            _ = c.stop.wait_for(|s| *s) => return,
        }
        let ids: Vec<String> = c.active.seen.lock().unwrap().keys().cloned().collect();
        let mut demand: HashMap<&str, u32> = c.pools.iter().map(|p| (p.as_str(), 0)).collect();
        let mut finished = Vec::new();
        let mut running_wfs = Vec::new();
        for id in ids {
            match c.broker.workflow(&id).await {
                Ok(Some(wf)) if wf.summary().state == "running" => {
                    for n in wf.nodes.iter().filter(|n| matches!(n.state, NodeState::Waiting | NodeState::Queued)) {
                        for p in c.task_pools.get(&n.task).into_iter().flatten() {
                            if let Some(d) = demand.get_mut(p.as_str()) {
                                *d += 1;
                            }
                        }
                    }
                    if c.eta.is_some() {
                        running_wfs.push(wf);
                    }
                }
                Ok(_) => finished.push(id),
                Err(e) => {
                    c.metrics.broker_error("workflow");
                    tracing::warn!(error = %e, workflow = %id, "reading a workflow for pool demand failed");
                }
            }
        }
        // Run times so far, and the tasks running now (by task id): the ones on this worker from the in-flight
        // map, the ones on other workers from their heartbeats.
        let mut etas: HashMap<String, PoolEta> = HashMap::new();
        if let Some(eta) = &c.eta {
            let durations = eta.durations.lock().unwrap().clone();
            let now = Instant::now();
            let local: HashMap<String, Running> = c.inflight.lock().unwrap().values()
                .filter_map(|f| {
                    let started = f.started?;
                    Some((f.delivery.header.id.clone(), Running {
                        elapsed: now.saturating_duration_since(started).as_secs_f64(),
                        remaining: f.progress.as_ref().and_then(|p| p.remaining_s(started, now)),
                    }))
                })
                .collect();
            // Only worth a read while a pool's workflow has a queued node that isn't running here: that one may be
            // running elsewhere.
            let elsewhere = running_wfs.iter().any(|wf| {
                wf.nodes.iter().any(|n| c.task_pools.contains_key(&n.task))
                    && wf.nodes.iter().any(|n| n.state == NodeState::Queued && !local.contains_key(&n.task_id))
            });
            let mut running = HashMap::new();
            if elsewhere {
                if remote.stale(c.heartbeat) {
                    remote.refresh(&*c.broker, &c.worker_id, &c.metrics).await;
                }
                running.extend(remote.running());
            }
            running.extend(local); // this worker knows its own tasks best
            for wf in &running_wfs {
                eta::add_workflow(&mut etas, wf, &durations, &running, &c.task_pools);
            }
        }
        if !finished.is_empty() {
            let mut seen = c.active.seen.lock().unwrap();
            for id in &finished {
                seen.remove(id);
            }
        }
        let demand: Vec<(String, u32)> = demand.into_iter().map(|(p, n)| (p.to_string(), n)).collect();
        let Some(eta) = &c.eta else {
            c.ready.set_demand(&demand);
            continue;
        };
        let mut counts = HashMap::new();
        let needs: Vec<(String, u32, Option<f64>)> = demand.into_iter().map(|(p, n)| {
            let e = etas.get(&p);
            if let Some(e) = e.filter(|_| n > 0) {
                tracing::debug!(pool = %p, next_need = ?e.next_need, done = e.done, total = e.total, "pool progress");
            }
            let next = e.and_then(|e| e.next_need).filter(|_| n > 0);
            if let (Some(e), Some(_)) = (e, next) {
                counts.insert(p.clone(), (e.done, e.total));
            }
            (p, n, next)
        }).collect();
        let mut known: Vec<(String, f64)> = needs.iter().filter_map(|(p, _, s)| Some((p.clone(), (*s)?))).collect();
        known.sort_by(|a, b| a.0.cmp(&b.0));
        *eta.needs.lock().unwrap() = known;
        *eta.counts.lock().unwrap() = counts;
        c.ready.set_needs(&needs);
    }
}

/// Re-claims messages this worker holds before they look abandoned (spec §4.4 step 4).
async fn keepalive_loop<B: Broker>(broker: Arc<B>, inflight: Inflight, visibility_ms: u64, metrics: Arc<Metrics>) {
    let threshold = Duration::from_millis(visibility_ms / 2);
    let mut tick = tokio::time::interval(Duration::from_millis((visibility_ms / 4).max(10)));
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        tick.tick().await;
        let due: HashMap<String, Vec<(u64, String)>> = {
            let map = inflight.lock().unwrap();
            let mut due: HashMap<String, Vec<(u64, String)>> = HashMap::new();
            for (token, f) in map.iter() {
                if !f.lost && f.last_claim.elapsed() >= threshold {
                    due.entry(f.delivery.queue.clone()).or_default().push((*token, f.delivery.stream_id.clone()));
                }
            }
            due
        };
        for (queue, items) in due {
            let ids: Vec<String> = items.iter().map(|(_, id)| id.clone()).collect();
            match broker.extend(&queue, &ids).await {
                Ok(lost) => {
                    let now = Instant::now();
                    let mut map = inflight.lock().unwrap();
                    for (token, id) in &items {
                        let Some(f) = map.get_mut(token) else { continue };
                        if lost.contains(id) {
                            f.lost = true;
                            tracing::warn!(task_id = %f.delivery.header.id,
                                "lost ownership of a running task; another worker may run it too");
                        } else {
                            f.last_claim = now;
                        }
                    }
                }
                Err(e) => {
                    metrics.broker_error("extend");
                    tracing::warn!(error = %e, queue = %queue, "extending ownership failed");
                }
            }
        }
    }
}

async fn heartbeat_loop<B: Broker>(
    broker: Arc<B>,
    base: WorkerInfo,
    inflight: Inflight,
    interval_ms: u64,
    prune_idle_ms: u64,
    metrics: Arc<Metrics>,
) {
    let ttl_ms = interval_ms.saturating_mul(3);
    let mut tick = tokio::time::interval(Duration::from_millis(interval_ms.max(10)));
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut beats: u64 = 0;
    loop {
        tick.tick().await;
        let mut info = base.clone();
        info.inflight = inflight.lock().unwrap().len();
        info.last_seen = now_ms();
        info.tasks = running_snapshot(&inflight, Instant::now(), info.last_seen);
        if let Err(e) = broker.heartbeat(&info, ttl_ms).await {
            metrics.broker_error("heartbeat");
            tracing::warn!(error = %e, "heartbeat failed");
        }
        if beats % PRUNE_EVERY_TICKS == 0 {
            for q in &info.queues {
                if let Err(e) = broker.prune_consumers(q, prune_idle_ms).await {
                    metrics.broker_error("prune");
                    tracing::warn!(error = %e, queue = %q, "pruning consumers failed");
                }
            }
        }
        beats += 1;
    }
}

/// What a scrape needs to report about parkable pools.
struct PoolView {
    /// Sorted.
    parkable: Vec<String>,
    eta: Option<Arc<EtaState>>,
}

/// Started tasks, oldest first (ties by claim order), capped; clones only the ones it returns.
fn running_snapshot(inflight: &Inflight, now: Instant, wall_ms: u64) -> Vec<RunningTask> {
    let m = inflight.lock().unwrap();
    let mut started: Vec<(Instant, u64, &InFlight)> =
        m.iter().filter_map(|(token, f)| f.started.map(|s| (s, *token, f))).collect();
    started.sort_unstable_by_key(|(s, token, _)| (*s, *token));
    started.truncate(MAX_PUBLISHED);
    started
        .into_iter()
        .map(|(s, _, f)| {
            let h = &f.delivery.header;
            let p = f.progress.as_ref();
            RunningTask {
                id: h.id.clone(),
                task: h.task.clone(),
                queue: f.delivery.queue.clone(),
                workflow_id: h.workflow_id.clone(),
                node: h.node.clone(),
                started_ms: wall_ms.saturating_sub(now.saturating_duration_since(s).as_millis() as u64),
                done: p.map(|p| p.done),
                total: p.and_then(|p| p.total),
                fraction: p.map(|p| p.fraction),
                remaining_s: p.and_then(|p| p.remaining_s(s, now)),
            }
        })
        .collect()
}

/// One `/metrics` body: counters, process gauges, and queue depths if the broker answers in time.
#[allow(clippy::too_many_arguments)]
async fn scrape<B: Broker>(
    broker: &B,
    metrics: &Metrics,
    inflight: &Inflight,
    ready: &ReadyQueue,
    pools: &PoolView,
    queues: &[String],
    concurrency: usize,
    lookahead: bool,
) -> String {
    let (n, running) = {
        let m = inflight.lock().unwrap();
        (m.len(), m.values().filter(|f| f.started.is_some()).count())
    };
    let mut stats = Vec::with_capacity(queues.len());
    for q in queues {
        match tokio::time::timeout(SCRAPE_BROKER_TIMEOUT, broker.queue_stats(q)).await {
            Ok(Ok(s)) => stats.push((q.clone(), s)),
            failed => {
                metrics.broker_error("stats");
                match failed {
                    Ok(Err(e)) => tracing::warn!(error = %e, queue = %q, "reading queue depth for metrics failed"),
                    _ => tracing::warn!(queue = %q, "reading queue depth for metrics timed out"),
                }
                stats.clear(); // all or nothing: a partial set of queues would mislead
                break;
            }
        }
    }
    let demand = if lookahead { ready.demand() } else { Vec::new() };
    // Only pools that can park report parked instances and needs, so other workers' output is unchanged.
    let parked = if pools.parkable.is_empty() {
        Vec::new()
    } else {
        ready.parked_usage().into_iter().filter(|(p, _)| pools.parkable.contains(p)).collect()
    };
    let next_need = pools.eta.as_ref().map(|e| e.needs.lock().unwrap().clone()).unwrap_or_default();
    metrics.render(&Gauges {
        concurrency, inflight: n, running, queues: stats, resources: ready.usage(), loaded: ready.lazy_usage(), demand,
        parked, next_need,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn delivery(workflow_id: Option<&str>) -> Delivery {
        let mut header = Header::new("t", "q", 0, "ct");
        header.workflow_id = workflow_id.map(String::from);
        Delivery { queue: "q".into(), stream_id: "1-0".into(), header, raw_header: vec![], payload: vec![], delivery_count: 1 }
    }

    #[test]
    fn workflow_start_comes_from_the_ulid_timestamp() {
        let ms = 1_700_000_000_123;
        let id = ulid::Ulid::from_parts(ms, 42).to_string();
        assert_eq!(ReadyTask::from_delivery(1, &delivery(Some(&id))).workflow_started_ms, Some(ms));
    }

    fn inflight_with(entries: Vec<(u64, Option<Instant>, Option<Progress>)>) -> Inflight {
        let m = entries
            .into_iter()
            .map(|(token, started, progress)| {
                let mut d = delivery(None);
                d.header.id = format!("id{token}");
                (token, InFlight { delivery: d, last_claim: Instant::now(), lost: false, started, progress })
            })
            .collect();
        Arc::new(Mutex::new(m))
    }

    #[test]
    fn snapshot_lists_started_tasks_oldest_first() {
        let now = Instant::now();
        let ago = |s| Some(now - Duration::from_secs(s));
        let p = Progress::new(now, 5.0, Some(10.0));
        let inflight = inflight_with(vec![(1, ago(10), None), (2, ago(30), p), (3, None, None), (4, ago(20), None)]);
        let snap = running_snapshot(&inflight, now, 1_000_000);
        let ids: Vec<_> = snap.iter().map(|t| t.id.as_str()).collect();
        assert_eq!(ids, ["id2", "id4", "id1"]);
        assert_eq!(snap[0].started_ms, 970_000);
        assert_eq!((snap[0].done, snap[0].total, snap[0].fraction), (Some(5.0), Some(10.0), Some(0.5)));
        assert_eq!(snap[0].remaining_s, Some(30.0 * 0.5 / 0.5));
        assert_eq!((snap[1].done, snap[1].fraction, snap[1].remaining_s), (None, None, None));
        assert_eq!((snap[1].task.as_str(), snap[1].queue.as_str()), ("t", "q"));
    }

    #[test]
    fn snapshot_is_capped() {
        let now = Instant::now();
        let entries = (0..MAX_PUBLISHED as u64 + 10).map(|i| (i, Some(now - Duration::from_millis(1000 - i)), None));
        let snap = running_snapshot(&inflight_with(entries.collect()), now, 10_000);
        assert_eq!(snap.len(), MAX_PUBLISHED);
        assert_eq!(snap[0].id, "id0");
        assert_eq!(snap[MAX_PUBLISHED - 1].id, format!("id{}", MAX_PUBLISHED - 1));
    }

    #[test]
    fn no_workflow_start_without_a_workflow_or_a_ulid() {
        assert_eq!(ReadyTask::from_delivery(1, &delivery(None)).workflow_started_ms, None);
        for bad in ["01WF", "", "not-a-ulid-at-all-not-a-ulid"] {
            assert_eq!(ReadyTask::from_delivery(1, &delivery(Some(bad))).workflow_started_ms, None, "{bad:?}");
        }
    }
}
