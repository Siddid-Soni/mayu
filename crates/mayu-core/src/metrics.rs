//! Worker metrics in the Prometheus text exposition format (0.0.4).
//!
//! Counters and the duration histogram are updated by the runtime's loops; gauges are sampled when
//! `/metrics` is scraped. Everything is per worker process.
use std::collections::BTreeMap;
use std::fmt::{Display, Write as _};
use std::sync::Mutex;
use std::time::Duration;

use crate::broker::QueueStats;

/// Upper bounds (seconds) of the task duration histogram; `+Inf` is implied.
pub const DURATION_BUCKETS: [f64; 14] =
    [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 300.0];

/// Ledger resources named `pool:<name>` are model pools, reported separately from real resources.
const POOL_PREFIX: &str = "pool:";

/// How a claimed task was settled.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Settled {
    Success,
    Retry,
    Dead,
    /// Acked without running: the task's workflow was cancelled.
    Cancelled,
    /// Acked without running: the task's `expires` deadline had passed when it was claimed.
    Expired,
}

impl Settled {
    pub fn as_str(self) -> &'static str {
        match self {
            Settled::Success => "success",
            Settled::Retry => "retry",
            Settled::Dead => "dead",
            Settled::Cancelled => "cancelled",
            Settled::Expired => "expired",
        }
    }
}

#[derive(Default)]
struct Histogram {
    /// Cumulative: `buckets[i]` counts observations `<= DURATION_BUCKETS[i]`.
    buckets: [u64; DURATION_BUCKETS.len()],
    count: u64,
    sum: f64,
}

/// (queue, task)
type Key = (String, String);

#[derive(Default)]
struct Counters {
    received: BTreeMap<Key, u64>,
    settled: BTreeMap<(String, String, Settled), u64>,
    durations: BTreeMap<Key, Histogram>,
    deferred: BTreeMap<Key, u64>,
    reclaimed: BTreeMap<String, u64>,
    broker_errors: BTreeMap<&'static str, u64>,
    pool_rebuilds: BTreeMap<String, u64>,
    pool_loads: BTreeMap<String, u64>,
    pool_evictions: BTreeMap<String, u64>,
    pool_lookahead_loads: BTreeMap<String, u64>,
    pool_parks: BTreeMap<String, u64>,
    pool_unparks: BTreeMap<String, u64>,
    pool_drops: BTreeMap<String, u64>,
    pool_stages: BTreeMap<String, u64>,
    /// (pool, kind) -> (moves, seconds); only pools that can park report these.
    pool_moves: BTreeMap<(String, &'static str), (u64, f64)>,
    progress_reports: BTreeMap<Key, u64>,
}

/// Sampled when `/metrics` is scraped.
#[derive(Debug, Clone, Default)]
pub struct Gauges {
    pub concurrency: usize,
    /// Claimed and not yet settled (prefetched or running).
    pub inflight: usize,
    /// Handed to a Python thread and not yet settled.
    pub running: usize,
    /// Per-queue broker state; empty when the broker could not be read.
    pub queues: Vec<(String, QueueStats)>,
    /// (resource, capacity, in use); empty when the worker declares no resources.
    pub resources: Vec<(String, f64, f64)>,
    /// (pool, instances loaded) for each lazy pool; empty unless the worker runs pools lazily.
    pub loaded: Vec<(String, u32)>,
    /// (pool, queued or waiting workflow nodes that use it) for each lazy pool; empty unless lookahead runs.
    pub demand: Vec<(String, u32)>,
    /// (pool, instances parked in host RAM, staging ones included) for each lazy pool that can park.
    pub parked: Vec<(String, u32)>,
    /// (pool, seconds until its next need) for pools the progress estimate has a need for; empty without one.
    pub next_need: Vec<(String, f64)>,
}

/// One GPU as last sampled by the Python side (NVML), pushed with `Runtime::set_gpu_stats`.
#[derive(Debug, Clone, PartialEq)]
pub struct GpuStat {
    /// NVML index, as `nvidia-smi` numbers it.
    pub index: u32,
    pub uuid: String,
    /// 0 to 1.
    pub utilization: f64,
    /// Bytes in use by all processes.
    pub memory_used: u64,
    pub memory_total: u64,
    /// Bytes this worker's process tree uses; None if the driver can't attribute it.
    pub process_memory: Option<u64>,
    pub power_watts: Option<f64>,
    pub temperature_c: Option<f64>,
}

#[derive(Default)]
pub struct Metrics {
    c: Mutex<Counters>,
    /// The last push; empty until the first.
    gpus: Mutex<Vec<GpuStat>>,
}

fn key(queue: &str, task: &str) -> Key {
    (queue.to_string(), task.to_string())
}

impl Metrics {
    pub fn received(&self, queue: &str, task: &str) {
        *self.c.lock().unwrap().received.entry(key(queue, task)).or_default() += 1;
    }

    pub fn settled(&self, queue: &str, task: &str, how: Settled, duration: Option<Duration>) {
        let mut c = self.c.lock().unwrap();
        *c.settled.entry((queue.to_string(), task.to_string(), how)).or_default() += 1;
        if let Some(d) = duration {
            let secs = d.as_secs_f64();
            let h = c.durations.entry(key(queue, task)).or_default();
            for (i, le) in DURATION_BUCKETS.iter().enumerate() {
                if secs <= *le {
                    h.buckets[i] += 1;
                }
            }
            h.count += 1;
            h.sum += secs;
        }
    }

    pub fn deferred(&self, queue: &str, task: &str) {
        *self.c.lock().unwrap().deferred.entry(key(queue, task)).or_default() += 1;
    }

    pub fn reclaimed(&self, queue: &str) {
        *self.c.lock().unwrap().reclaimed.entry(queue.to_string()).or_default() += 1;
    }

    pub fn broker_error(&self, op: &'static str) {
        *self.c.lock().unwrap().broker_errors.entry(op).or_default() += 1;
    }

    /// A running task's progress report was recorded.
    pub fn progress_reported(&self, queue: &str, task: &str) {
        *self.c.lock().unwrap().progress_reports.entry(key(queue, task)).or_default() += 1;
    }

    pub fn pool_rebuilt(&self, pool: &str) {
        *self.c.lock().unwrap().pool_rebuilds.entry(pool.to_string()).or_default() += 1;
    }

    pub fn pool_loaded(&self, pool: &str) {
        *self.c.lock().unwrap().pool_loads.entry(pool.to_string()).or_default() += 1;
    }

    pub fn pool_evicted(&self, pool: &str) {
        *self.c.lock().unwrap().pool_evictions.entry(pool.to_string()).or_default() += 1;
    }

    /// A lazy pool instance was built ahead of any task needing it (also counted by `pool_loaded`).
    pub fn pool_lookahead_loaded(&self, pool: &str) {
        *self.c.lock().unwrap().pool_lookahead_loads.entry(pool.to_string()).or_default() += 1;
    }

    /// A lazy pool instance was moved to host RAM.
    pub fn pool_parked(&self, pool: &str) {
        *self.c.lock().unwrap().pool_parks.entry(pool.to_string()).or_default() += 1;
    }

    /// A lazy pool instance was moved back from host RAM (by a task or ahead of need).
    pub fn pool_unparked(&self, pool: &str) {
        *self.c.lock().unwrap().pool_unparks.entry(pool.to_string()).or_default() += 1;
    }

    /// A parked lazy pool instance was closed to make room.
    pub fn pool_dropped(&self, pool: &str) {
        *self.c.lock().unwrap().pool_drops.entry(pool.to_string()).or_default() += 1;
    }

    /// A lazy pool instance was built straight into host RAM ahead of need.
    pub fn pool_staged(&self, pool: &str) {
        *self.c.lock().unwrap().pool_stages.entry(pool.to_string()).or_default() += 1;
    }

    /// One timed move (`build`, `stage`, `park` or `unpark`) of a pool instance.
    pub fn pool_move(&self, pool: &str, kind: &'static str, secs: f64) {
        let mut c = self.c.lock().unwrap();
        let m = c.pool_moves.entry((pool.to_string(), kind)).or_default();
        m.0 += 1;
        m.1 += secs;
    }

    /// Replaces the GPU samples rendered by `render`.
    pub fn set_gpus(&self, gpus: Vec<GpuStat>) {
        *self.gpus.lock().unwrap() = gpus;
    }

    pub fn render(&self, g: &Gauges) -> String {
        let c = self.c.lock().unwrap();
        let mut out = String::with_capacity(4096);

        family(&mut out, "mayu_tasks_received_total", "counter", "Tasks claimed from the broker, new or reclaimed.");
        for ((q, t), n) in &c.received {
            sample(&mut out, "mayu_tasks_received_total", &[("queue", q), ("task", t)], n);
        }
        family(&mut out, "mayu_tasks_settled_total", "counter", "Task outcomes recorded in the broker.");
        for ((q, t, how), n) in &c.settled {
            sample(&mut out, "mayu_tasks_settled_total", &[("queue", q), ("task", t), ("outcome", how.as_str())], n);
        }
        family(&mut out, "mayu_task_duration_seconds", "histogram", "Time from a thread taking a task to its outcome being recorded.");
        for ((q, t), h) in &c.durations {
            for (i, le) in DURATION_BUCKETS.iter().enumerate() {
                let le = le.to_string();
                sample(&mut out, "mayu_task_duration_seconds_bucket", &[("queue", q), ("task", t), ("le", &le)], h.buckets[i]);
            }
            sample(&mut out, "mayu_task_duration_seconds_bucket", &[("queue", q), ("task", t), ("le", "+Inf")], h.count);
            sample(&mut out, "mayu_task_duration_seconds_sum", &[("queue", q), ("task", t)], h.sum);
            sample(&mut out, "mayu_task_duration_seconds_count", &[("queue", q), ("task", t)], h.count);
        }
        family(&mut out, "mayu_tasks_deferred_total", "counter", "Rate-limited tasks handed back to the broker until their slot.");
        for ((q, t), n) in &c.deferred {
            sample(&mut out, "mayu_tasks_deferred_total", &[("queue", q), ("task", t)], n);
        }
        family(&mut out, "mayu_tasks_reclaimed_total", "counter", "Tasks reclaimed from workers that stopped responding.");
        for (q, n) in &c.reclaimed {
            sample(&mut out, "mayu_tasks_reclaimed_total", &[("queue", q)], n);
        }
        family(&mut out, "mayu_broker_errors_total", "counter", "Failed broker operations, by operation.");
        for (op, n) in &c.broker_errors {
            sample(&mut out, "mayu_broker_errors_total", &[("op", op)], n);
        }
        if !c.pool_rebuilds.is_empty() {
            family(&mut out, "mayu_pool_rebuilds_total", "counter", "Pool instances replaced after being discarded.");
            for (p, n) in &c.pool_rebuilds {
                sample(&mut out, "mayu_pool_rebuilds_total", &[("pool", p)], n);
            }
        }
        if !c.pool_loads.is_empty() {
            family(&mut out, "mayu_pool_loads_total", "counter", "Lazy pool instances built, preloads included.");
            for (p, n) in &c.pool_loads {
                sample(&mut out, "mayu_pool_loads_total", &[("pool", p)], n);
            }
        }
        if !c.pool_evictions.is_empty() {
            family(&mut out, "mayu_pool_evictions_total", "counter", "Lazy pool instances closed to make room.");
            for (p, n) in &c.pool_evictions {
                sample(&mut out, "mayu_pool_evictions_total", &[("pool", p)], n);
            }
        }
        if !c.pool_lookahead_loads.is_empty() {
            family(&mut out, "mayu_pool_lookahead_loads_total", "counter", "Lazy pool instances built ahead of the task that needs them.");
            for (p, n) in &c.pool_lookahead_loads {
                sample(&mut out, "mayu_pool_lookahead_loads_total", &[("pool", p)], n);
            }
        }
        let counters: [(&str, &str, &BTreeMap<String, u64>); 4] = [
            ("mayu_pool_parks_total", "Lazy pool instances moved to host RAM.", &c.pool_parks),
            ("mayu_pool_unparks_total", "Lazy pool instances moved back from host RAM.", &c.pool_unparks),
            ("mayu_pool_drops_total", "Parked lazy pool instances closed to make room.", &c.pool_drops),
            ("mayu_pool_stages_total", "Lazy pool instances built straight into host RAM ahead of need.", &c.pool_stages),
        ];
        for (name, help, by_pool) in counters {
            if !by_pool.is_empty() {
                family(&mut out, name, "counter", help);
                for (p, n) in by_pool {
                    sample(&mut out, name, &[("pool", p)], n);
                }
            }
        }
        if !c.pool_moves.is_empty() {
            family(&mut out, "mayu_pool_move_seconds", "summary", "Time to build, stage, park or unpark a lazy pool instance.");
            for ((p, kind), (n, secs)) in &c.pool_moves {
                sample(&mut out, "mayu_pool_move_seconds_sum", &[("pool", p), ("kind", kind)], secs);
                sample(&mut out, "mayu_pool_move_seconds_count", &[("pool", p), ("kind", kind)], n);
            }
        }
        if !c.progress_reports.is_empty() {
            family(&mut out, "mayu_task_progress_reports_total", "counter", "Progress reports recorded from running tasks.");
            for ((q, t), n) in &c.progress_reports {
                sample(&mut out, "mayu_task_progress_reports_total", &[("queue", q), ("task", t)], n);
            }
        }
        drop(c);

        family(&mut out, "mayu_worker_concurrency", "gauge", "Task threads in this worker process.");
        sample(&mut out, "mayu_worker_concurrency", &[], g.concurrency);
        family(&mut out, "mayu_tasks_inflight", "gauge", "Tasks claimed by this process and not yet settled.");
        sample(&mut out, "mayu_tasks_inflight", &[], g.inflight);
        family(&mut out, "mayu_tasks_running", "gauge", "Tasks running on a thread of this process.");
        sample(&mut out, "mayu_tasks_running", &[], g.running);
        let per_queue: [(&str, &str, fn(&QueueStats) -> u64); 4] = [
            ("mayu_queue_ready", "Entries no worker has claimed yet.", |s| s.ready()),
            ("mayu_queue_pending", "Entries claimed by a worker and not yet settled.", |s| s.pending),
            ("mayu_queue_delayed", "Entries waiting for their countdown, eta or retry backoff.", |s| s.delayed),
            ("mayu_queue_dead", "Dead-lettered entries.", |s| s.dead),
        ];
        for (name, help, get) in per_queue {
            family(&mut out, name, "gauge", help);
            for (q, s) in &g.queues {
                sample(&mut out, name, &[("queue", q)], get(s));
            }
        }
        // Pool sizes ride in the resource ledger as `pool:<name>`; they get their own families.
        let (pools, resources): (Vec<_>, Vec<_>) = g.resources.iter().partition(|(r, ..)| r.starts_with(POOL_PREFIX));
        if !resources.is_empty() {
            family(&mut out, "mayu_resource_capacity", "gauge", "What this worker process declared for each resource.");
            for (r, cap, _) in &resources {
                sample(&mut out, "mayu_resource_capacity", &[("resource", r)], cap);
            }
            family(&mut out, "mayu_resource_in_use", "gauge", "Resource amounts held by tasks running in this process.");
            for (r, _, used) in &resources {
                sample(&mut out, "mayu_resource_in_use", &[("resource", r)], used);
            }
        }
        if !pools.is_empty() {
            family(&mut out, "mayu_pool_size", "gauge", "Instances in each model pool of this process.");
            for (r, size, _) in &pools {
                sample(&mut out, "mayu_pool_size", &[("pool", r.strip_prefix(POOL_PREFIX).unwrap_or(r))], size);
            }
            family(&mut out, "mayu_pool_busy", "gauge", "Pool instances held by running tasks.");
            for (r, _, busy) in &pools {
                sample(&mut out, "mayu_pool_busy", &[("pool", r.strip_prefix(POOL_PREFIX).unwrap_or(r))], busy);
            }
        }
        if !g.loaded.is_empty() {
            family(&mut out, "mayu_pool_loaded", "gauge", "Instances of each lazy pool in memory, idle or busy.");
            for (p, n) in &g.loaded {
                sample(&mut out, "mayu_pool_loaded", &[("pool", p)], n);
            }
        }
        if !g.demand.is_empty() {
            family(&mut out, "mayu_pool_demand", "gauge", "Waiting or queued workflow nodes that use each lazy pool.");
            for (p, n) in &g.demand {
                sample(&mut out, "mayu_pool_demand", &[("pool", p)], n);
            }
        }
        if !g.parked.is_empty() {
            family(&mut out, "mayu_pool_parked", "gauge", "Instances of each parkable lazy pool held in host RAM.");
            for (p, n) in &g.parked {
                sample(&mut out, "mayu_pool_parked", &[("pool", p)], n);
            }
        }
        if !g.next_need.is_empty() {
            family(&mut out, "mayu_pool_next_need_seconds", "gauge", "Estimated seconds until a lazy pool is next needed.");
            for (p, secs) in &g.next_need {
                sample(&mut out, "mayu_pool_next_need_seconds", &[("pool", p)], secs);
            }
        }
        self.render_gpus(&mut out);
        out
    }

    /// Nothing before the first push; a field the GPU doesn't report has no sample.
    fn render_gpus(&self, out: &mut String) {
        let gpus = self.gpus.lock().unwrap();
        if gpus.is_empty() {
            return;
        }
        type Family = (&'static str, &'static str, fn(&GpuStat) -> Option<f64>);
        let families: [Family; 6] = [
            ("mayu_gpu_utilization_ratio", "GPU compute utilization, 0 to 1.", |s| Some(s.utilization)),
            ("mayu_gpu_memory_used_bytes", "GPU memory in use by all processes.", |s| Some(s.memory_used as f64)),
            ("mayu_gpu_memory_total_bytes", "GPU memory installed.", |s| Some(s.memory_total as f64)),
            ("mayu_gpu_process_memory_bytes", "GPU memory used by this worker and its child processes.", |s| s.process_memory.map(|b| b as f64)),
            ("mayu_gpu_power_watts", "GPU power draw.", |s| s.power_watts),
            ("mayu_gpu_temperature_celsius", "GPU temperature.", |s| s.temperature_c),
        ];
        for (name, help, get) in families {
            if !gpus.iter().any(|s| get(s).is_some()) {
                continue;
            }
            family(out, name, "gauge", help);
            for s in gpus.iter() {
                if let Some(v) = get(s) {
                    sample(out, name, &[("gpu", &s.index.to_string()), ("uuid", &s.uuid)], v);
                }
            }
        }
    }
}

fn family(out: &mut String, name: &str, kind: &str, help: &str) {
    let _ = writeln!(out, "# HELP {name} {help}");
    let _ = writeln!(out, "# TYPE {name} {kind}");
}

fn sample(out: &mut String, name: &str, labels: &[(&str, &str)], value: impl Display) {
    out.push_str(name);
    if !labels.is_empty() {
        out.push('{');
        for (i, (k, v)) in labels.iter().enumerate() {
            if i > 0 {
                out.push(',');
            }
            let _ = write!(out, "{k}=\"{}\"", escape(v));
        }
        out.push('}');
    }
    let _ = writeln!(out, " {value}");
}

/// Label value escaping per the exposition format: backslash, double quote, newline.
fn escape(v: &str) -> String {
    v.replace('\\', "\\\\").replace('"', "\\\"").replace('\n', "\\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::broker::QueueStats;

    fn value(body: &str, series: &str) -> Option<f64> {
        body.lines().find_map(|l| l.strip_prefix(series)?.strip_prefix(' ')?.parse().ok())
    }

    #[test]
    fn counters_render_with_labels() {
        let m = Metrics::default();
        m.received("q", "t");
        m.received("q", "t");
        m.received("q2", "t");
        m.deferred("q", "t");
        m.reclaimed("q");
        m.broker_error("fetch");
        let b = m.render(&Gauges::default());
        assert_eq!(value(&b, r#"mayu_tasks_received_total{queue="q",task="t"}"#), Some(2.0));
        assert_eq!(value(&b, r#"mayu_tasks_received_total{queue="q2",task="t"}"#), Some(1.0));
        assert_eq!(value(&b, r#"mayu_tasks_deferred_total{queue="q",task="t"}"#), Some(1.0));
        assert_eq!(value(&b, r#"mayu_tasks_reclaimed_total{queue="q"}"#), Some(1.0));
        assert_eq!(value(&b, r#"mayu_broker_errors_total{op="fetch"}"#), Some(1.0));
    }

    #[test]
    fn progress_reports_are_counted_and_only_emitted_once_seen() {
        let m = Metrics::default();
        assert!(!m.render(&Gauges::default()).contains("mayu_task_progress_reports_total"));
        m.progress_reported("q", "t");
        m.progress_reported("q", "t");
        let b = m.render(&Gauges::default());
        assert_eq!(value(&b, r#"mayu_task_progress_reports_total{queue="q",task="t"}"#), Some(2.0));
    }

    #[test]
    fn settled_counts_by_outcome_and_observes_cumulative_durations() {
        let m = Metrics::default();
        m.settled("q", "t", Settled::Success, Some(Duration::from_millis(30)));
        m.settled("q", "t", Settled::Dead, Some(Duration::from_secs(2)));
        m.settled("q", "t", Settled::Dead, None); // poison: counted, not timed
        m.settled("q", "t", Settled::Expired, None); // dropped at claim: counted, not timed
        let b = m.render(&Gauges::default());
        assert_eq!(value(&b, r#"mayu_tasks_settled_total{queue="q",task="t",outcome="success"}"#), Some(1.0));
        assert_eq!(value(&b, r#"mayu_tasks_settled_total{queue="q",task="t",outcome="dead"}"#), Some(2.0));
        assert_eq!(value(&b, r#"mayu_tasks_settled_total{queue="q",task="t",outcome="expired"}"#), Some(1.0));
        let bucket = |le: &str| value(&b, &format!(r#"mayu_task_duration_seconds_bucket{{queue="q",task="t",le="{le}"}}"#));
        assert_eq!(bucket("0.025"), Some(0.0));
        assert_eq!(bucket("0.05"), Some(1.0));
        assert_eq!(bucket("1"), Some(1.0));
        assert_eq!(bucket("2.5"), Some(2.0));
        assert_eq!(bucket("+Inf"), Some(2.0));
        assert_eq!(value(&b, r#"mayu_task_duration_seconds_count{queue="q",task="t"}"#), Some(2.0));
        let sum = value(&b, r#"mayu_task_duration_seconds_sum{queue="q",task="t"}"#).unwrap();
        assert!((sum - 2.03).abs() < 1e-9, "sum was {sum}");
    }

    #[test]
    fn label_values_are_escaped() {
        let m = Metrics::default();
        m.received("q", "a\"b\\c\nd");
        let b = m.render(&Gauges::default());
        assert!(b.contains(r#"mayu_tasks_received_total{queue="q",task="a\"b\\c\nd"} 1"#), "{b}");
    }

    #[test]
    fn gauges_render_with_ready_derived_from_stream_and_pending() {
        let g = Gauges {
            concurrency: 8,
            inflight: 5,
            running: 3,
            queues: vec![
                ("a".into(), QueueStats { stream: 10, pending: 4, delayed: 2, dead: 1 }),
                ("b".into(), QueueStats { stream: 1, pending: 3, delayed: 0, dead: 0 }), // racy read
            ],
            resources: vec![("gpu".into(), 1.0, 0.5)],
            ..Gauges::default()
        };
        let b = Metrics::default().render(&g);
        assert_eq!(value(&b, "mayu_worker_concurrency"), Some(8.0));
        assert_eq!(value(&b, "mayu_tasks_inflight"), Some(5.0));
        assert_eq!(value(&b, "mayu_tasks_running"), Some(3.0));
        assert_eq!(value(&b, r#"mayu_queue_ready{queue="a"}"#), Some(6.0));
        assert_eq!(value(&b, r#"mayu_queue_pending{queue="a"}"#), Some(4.0));
        assert_eq!(value(&b, r#"mayu_queue_delayed{queue="a"}"#), Some(2.0));
        assert_eq!(value(&b, r#"mayu_queue_dead{queue="a"}"#), Some(1.0));
        assert_eq!(value(&b, r#"mayu_queue_ready{queue="b"}"#), Some(0.0));
        assert_eq!(value(&b, r#"mayu_resource_capacity{resource="gpu"}"#), Some(1.0));
        assert_eq!(value(&b, r#"mayu_resource_in_use{resource="gpu"}"#), Some(0.5));
    }

    #[test]
    fn resource_gauges_are_absent_without_resources() {
        let b = Metrics::default().render(&Gauges::default());
        assert!(!b.contains("mayu_resource"), "{b}");
    }

    #[test]
    fn every_sample_follows_its_type_line() {
        let m = Metrics::default();
        m.received("q", "t");
        m.settled("q", "t", Settled::Retry, Some(Duration::from_millis(1)));
        let g = Gauges { queues: vec![("q".into(), QueueStats::default())], ..Gauges::default() };
        let b = m.render(&g);
        let mut typed: Vec<String> = Vec::new();
        for line in b.lines() {
            if let Some(rest) = line.strip_prefix("# TYPE ") {
                typed.push(rest.split(' ').next().unwrap().to_string());
                continue;
            }
            if line.starts_with('#') || line.is_empty() { continue; }
            let name = line.split(['{', ' ']).next().unwrap();
            let family = ["_bucket", "_sum", "_count"].iter()
                .find_map(|s| name.strip_suffix(s).filter(|f| typed.iter().any(|t| t == f)))
                .unwrap_or(name);
            assert!(typed.iter().any(|t| t == family), "sample {line:?} has no preceding TYPE");
            assert!(line.rsplit(' ').next().unwrap().parse::<f64>().is_ok(), "bad value in {line:?}");
        }
    }

    #[test]
    fn pool_gauges_use_the_pool_label_and_stay_out_of_resource_families() {
        let g = Gauges {
            resources: vec![("gpu".into(), 1.0, 0.5), ("pool:gemma".into(), 2.0, 1.0), ("pool:tts".into(), 3.0, 0.0)],
            ..Gauges::default()
        };
        let b = Metrics::default().render(&g);
        assert_eq!(value(&b, r#"mayu_pool_size{pool="gemma"}"#), Some(2.0));
        assert_eq!(value(&b, r#"mayu_pool_busy{pool="gemma"}"#), Some(1.0));
        assert_eq!(value(&b, r#"mayu_pool_size{pool="tts"}"#), Some(3.0));
        assert_eq!(value(&b, r#"mayu_pool_busy{pool="tts"}"#), Some(0.0));
        assert_eq!(value(&b, r#"mayu_resource_capacity{resource="gpu"}"#), Some(1.0));
        assert!(!b.contains("pool:"), "{b}");
    }

    #[test]
    fn only_pool_entries_emit_no_resource_families() {
        let g = Gauges { resources: vec![("pool:gemma".into(), 2.0, 0.0)], ..Gauges::default() };
        let b = Metrics::default().render(&g);
        assert!(!b.contains("mayu_resource"), "{b}");
        assert!(b.contains("# TYPE mayu_pool_size gauge") && b.contains("# TYPE mayu_pool_busy gauge"), "{b}");
    }

    #[test]
    fn pool_rebuilds_are_counted_per_pool() {
        let m = Metrics::default();
        m.pool_rebuilt("gemma");
        m.pool_rebuilt("gemma");
        m.pool_rebuilt("tts");
        let b = m.render(&Gauges::default());
        assert_eq!(value(&b, r#"mayu_pool_rebuilds_total{pool="gemma"}"#), Some(2.0));
        assert_eq!(value(&b, r#"mayu_pool_rebuilds_total{pool="tts"}"#), Some(1.0));
    }

    #[test]
    fn no_pool_families_without_pool_entries() {
        let g = Gauges { resources: vec![("gpu".into(), 1.0, 0.0)], ..Gauges::default() };
        let b = Metrics::default().render(&g);
        assert!(!b.contains("mayu_pool"), "{b}");
    }

    #[test]
    fn loaded_gauge_is_rendered_for_lazy_pools_only() {
        let g = Gauges {
            resources: vec![("pool:gemma".into(), 1.0, 0.0), ("pool:tts".into(), 1.0, 0.0), ("pool:eager".into(), 1.0, 0.0)],
            loaded: vec![("gemma".into(), 1), ("tts".into(), 0)],
            ..Gauges::default()
        };
        let b = Metrics::default().render(&g);
        assert!(b.contains("# TYPE mayu_pool_loaded gauge"), "{b}");
        assert_eq!(value(&b, r#"mayu_pool_loaded{pool="gemma"}"#), Some(1.0));
        assert_eq!(value(&b, r#"mayu_pool_loaded{pool="tts"}"#), Some(0.0));
        assert_eq!(value(&b, r#"mayu_pool_loaded{pool="eager"}"#), None);
        let eager_only = Gauges { resources: vec![("pool:eager".into(), 1.0, 0.0)], ..Gauges::default() };
        assert!(!Metrics::default().render(&eager_only).contains("mayu_pool_loaded"));
    }

    #[test]
    fn loads_and_evictions_are_counted_per_pool_and_only_emitted_once_seen() {
        let m = Metrics::default();
        let b = m.render(&Gauges::default());
        assert!(!b.contains("mayu_pool_loads_total") && !b.contains("mayu_pool_evictions_total"), "{b}");
        m.pool_loaded("gemma");
        m.pool_loaded("gemma");
        m.pool_loaded("tts");
        m.pool_evicted("gemma");
        let b = m.render(&Gauges::default());
        assert_eq!(value(&b, r#"mayu_pool_loads_total{pool="gemma"}"#), Some(2.0));
        assert_eq!(value(&b, r#"mayu_pool_loads_total{pool="tts"}"#), Some(1.0));
        assert_eq!(value(&b, r#"mayu_pool_evictions_total{pool="gemma"}"#), Some(1.0));
        assert_eq!(value(&b, r#"mayu_pool_evictions_total{pool="tts"}"#), None);
    }

    fn gpu(index: u32) -> GpuStat {
        GpuStat {
            index,
            uuid: format!("GPU-{index}"),
            utilization: 0.5,
            memory_used: 2 * 1024 * 1024 * 1024,
            memory_total: 8 * 1024 * 1024 * 1024,
            process_memory: Some(1024 * 1024 * 1024),
            power_watts: Some(85.5),
            temperature_c: Some(61.0),
        }
    }

    #[test]
    fn no_gpu_families_before_a_push() {
        let b = Metrics::default().render(&Gauges::default());
        assert!(!b.contains("mayu_gpu"), "{b}");
    }

    #[test]
    fn gpu_families_render_per_device_with_index_and_uuid() {
        let m = Metrics::default();
        m.set_gpus(vec![gpu(0), GpuStat { utilization: 0.99, ..gpu(1) }]);
        let b = m.render(&Gauges::default());
        let l0 = r#"{gpu="0",uuid="GPU-0"}"#;
        assert_eq!(value(&b, &format!("mayu_gpu_utilization_ratio{l0}")), Some(0.5));
        assert_eq!(value(&b, r#"mayu_gpu_utilization_ratio{gpu="1",uuid="GPU-1"}"#), Some(0.99));
        assert_eq!(value(&b, &format!("mayu_gpu_memory_used_bytes{l0}")), Some(2147483648.0));
        assert_eq!(value(&b, &format!("mayu_gpu_memory_total_bytes{l0}")), Some(8589934592.0));
        assert_eq!(value(&b, &format!("mayu_gpu_process_memory_bytes{l0}")), Some(1073741824.0));
        assert_eq!(value(&b, &format!("mayu_gpu_power_watts{l0}")), Some(85.5));
        assert_eq!(value(&b, &format!("mayu_gpu_temperature_celsius{l0}")), Some(61.0));
        assert!(b.contains("# TYPE mayu_gpu_utilization_ratio gauge"), "{b}");
    }

    #[test]
    fn gpu_fields_the_device_does_not_report_have_no_sample() {
        let m = Metrics::default();
        m.set_gpus(vec![gpu(0), GpuStat { process_memory: None, power_watts: None, temperature_c: None, ..gpu(1) }]);
        let b = m.render(&Gauges::default());
        assert!(value(&b, r#"mayu_gpu_process_memory_bytes{gpu="0",uuid="GPU-0"}"#).is_some());
        assert!(!b.contains(r#"mayu_gpu_process_memory_bytes{gpu="1""#), "{b}");
        assert!(!b.contains(r#"mayu_gpu_power_watts{gpu="1""#), "{b}");
        assert!(!b.contains(r#"mayu_gpu_temperature_celsius{gpu="1""#), "{b}");
        assert!(value(&b, r#"mayu_gpu_memory_used_bytes{gpu="1",uuid="GPU-1"}"#).is_some());
    }

    #[test]
    fn a_later_push_replaces_the_earlier_one() {
        let m = Metrics::default();
        m.set_gpus(vec![gpu(0), gpu(1)]);
        m.set_gpus(vec![gpu(1)]);
        let b = m.render(&Gauges::default());
        assert!(!b.contains(r#"gpu="0""#) && b.contains(r#"gpu="1""#), "{b}");
    }

    #[test]
    fn lookahead_loads_are_counted_per_pool_and_only_emitted_once_seen() {
        let m = Metrics::default();
        assert!(!m.render(&Gauges::default()).contains("mayu_pool_lookahead_loads_total"));
        m.pool_lookahead_loaded("tts");
        m.pool_lookahead_loaded("tts");
        let b = m.render(&Gauges::default());
        assert_eq!(value(&b, r#"mayu_pool_lookahead_loads_total{pool="tts"}"#), Some(2.0));
        assert_eq!(value(&b, r#"mayu_pool_lookahead_loads_total{pool="gemma"}"#), None);
    }

    #[test]
    fn demand_gauge_is_rendered_only_when_given() {
        assert!(!Metrics::default().render(&Gauges::default()).contains("mayu_pool_demand"));
        let g = Gauges { demand: vec![("tts".into(), 2), ("gemma".into(), 0)], ..Gauges::default() };
        let b = Metrics::default().render(&g);
        assert_eq!(value(&b, r#"mayu_pool_demand{pool="tts"}"#), Some(2.0));
        assert_eq!(value(&b, r#"mayu_pool_demand{pool="gemma"}"#), Some(0.0));
    }
}
