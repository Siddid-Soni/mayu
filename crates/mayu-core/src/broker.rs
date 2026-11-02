use std::future::Future;

use crate::envelope::Header;
use crate::error::Result;
use crate::progress::RunningTask;
use crate::workflow::{WorkflowSpec, WorkflowState};

#[derive(Debug, Clone)]
pub struct Delivery {
    pub queue: String,
    pub stream_id: String,
    pub header: Header,
    pub raw_header: Vec<u8>,
    pub payload: Vec<u8>,
    pub delivery_count: u64,
}

/// A dead-lettered entry (`mayu:{q}:dead` in Redis).
#[derive(Debug, Clone, PartialEq)]
pub struct DeadLetter {
    /// Entry id in the dead stream; its millisecond part is when it was dead-lettered.
    pub entry_id: String,
    /// `None` if the header cannot be decoded (such entries are never replayed).
    pub header: Option<Header>,
    pub raw_header: Vec<u8>,
    pub payload: Vec<u8>,
    pub error: String,
}

impl DeadLetter {
    pub fn dead_at_ms(&self) -> u64 {
        self.entry_id.split('-').next().and_then(|ms| ms.parse().ok()).unwrap_or(0)
    }
}

#[derive(Debug, Clone)]
pub struct StoredResult {
    pub id: String,
    pub record: Vec<u8>,
    pub ttl_secs: u64,
}

/// A queue's state in the broker. `stream` counts unclaimed and claimed entries (acked entries
/// are deleted), so `ready()` is what no worker has claimed yet.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct QueueStats {
    pub stream: u64,
    pub pending: u64,
    pub delayed: u64,
    pub dead: u64,
}

impl QueueStats {
    pub fn ready(&self) -> u64 {
        self.stream.saturating_sub(self.pending)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct WorkerInfo {
    pub worker_id: String,
    pub hostname: String,
    pub pid: u32,
    pub queues: Vec<String>,
    pub concurrency: usize,
    pub inflight: usize,
    pub started_at: u64,
    pub last_seen: u64,
    /// Tasks running now, as of this heartbeat (at most `MAX_PUBLISHED`).
    pub tasks: Vec<RunningTask>,
}

/// What live workers published as running, from `Broker::running_all`.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct PublishedTasks {
    /// (worker id, task) for every task a live worker published.
    pub tasks: Vec<(String, RunningTask)>,
    /// Workers whose `tasks` field couldn't be read. The broker skips them (and logs); the caller counts them.
    pub malformed: usize,
}

#[derive(Debug, Clone, Default)]
pub struct Reclaimed {
    pub next_cursor: String,
    pub deliveries: Vec<Delivery>,
}

/// The only component aware of the broker. Every state transition must be atomic.
pub trait Broker: Send + Sync + 'static {
    fn ensure_queues(&self, queues: &[String]) -> impl Future<Output = Result<()>> + Send;
    fn enqueue(&self, header: &Header, payload: &[u8]) -> impl Future<Output = Result<()>> + Send;
    /// Claims new entries: at most `count` in total when entries are ready, earlier `queues` first.
    /// When none are ready it blocks up to `block_ms` and may return up to one entry per queue.
    fn fetch(&self, queues: &[String], count: usize, block_ms: u64)
        -> impl Future<Output = Result<Vec<Delivery>>> + Send;
    /// Acks `d` and enqueues `follow_ups` (link callbacks) in one atomic step. The follow-ups are
    /// enqueued only if this call is the one that acks `d`, so a repeated ack adds no duplicates.
    ///
    /// Returns `false` (stale) when this consumer no longer owns `d` (another worker reclaimed it, or it
    /// was already settled): nothing is acked, enqueued or released, and a result is stored only if none
    /// exists yet. The same holds for `retry`, `dead_letter` and `requeue`, which then change nothing.
    ///
    /// When `d.header.workflow_id` is set, `d` is a workflow node: the same step also marks the node
    /// done and enqueues every child whose parents are now all done, each at most once however many
    /// times the node is acked. `follow_ups` must be empty for a node (`Error::Broker` otherwise).
    /// In a cancelled workflow the node is marked cancelled and nothing is released.
    fn ack(&self, d: &Delivery, result: Option<StoredResult>, follow_ups: &[(Header, Vec<u8>)])
        -> impl Future<Output = Result<bool>> + Send;
    /// Schedules `d` to run again from `header`. `true` if applied, `false` if stale (see `ack`).
    fn retry(&self, d: &Delivery, header: &Header) -> impl Future<Output = Result<bool>> + Send;
    /// Moves `d` to the dead letters. A workflow node that isn't done yet is marked dead in the
    /// same step, which keeps its descendants waiting. `true` if applied, `false` if stale (see `ack`).
    fn dead_letter(&self, d: &Delivery, error: &str, result: Option<StoredResult>)
        -> impl Future<Output = Result<bool>> + Send;
    /// Puts `d` back on the stream. `true` if applied, `false` if stale (see `ack`).
    fn requeue(&self, d: &Delivery) -> impl Future<Output = Result<bool>> + Send;
    fn promote(&self, queue: &str, now_ms: u64, limit: usize) -> impl Future<Output = Result<usize>> + Send;
    fn get_result(&self, id: &str) -> impl Future<Output = Result<Option<Vec<u8>>>> + Send;
    /// Depth of `queue`: stream entries (unclaimed + claimed), claimed, delayed, dead.
    fn queue_stats(&self, queue: &str) -> impl Future<Output = Result<QueueStats>> + Send;
    /// Resets idle time on entries this consumer still owns; returns the ids it no longer owns.
    fn extend(&self, queue: &str, stream_ids: &[String]) -> impl Future<Output = Result<Vec<String>>> + Send;
    /// Claims entries idle for at least `min_idle_ms` (their worker died).
    fn reclaim(&self, queue: &str, min_idle_ms: u64, cursor: &str, count: usize)
        -> impl Future<Output = Result<Reclaimed>> + Send;
    fn heartbeat(&self, info: &WorkerInfo, ttl_ms: u64) -> impl Future<Output = Result<()>> + Send;
    /// The tasks `worker_id` published in its last live heartbeat; empty if it is unknown, expired or published none.
    fn running(&self, worker_id: &str) -> impl Future<Output = Result<Vec<RunningTask>>> + Send;
    /// The tasks every live worker published in its last heartbeat. A worker with a malformed field adds to
    /// `malformed` and nothing else; the call fails only when the broker does.
    fn running_all(&self) -> impl Future<Output = Result<PublishedTasks>> + Send;
    fn unregister(&self, worker_id: &str) -> impl Future<Output = Result<()>> + Send;
    /// Removes consumers with no pending entries, idle at least `min_idle_ms`, and no heartbeat.
    fn prune_consumers(&self, queue: &str, min_idle_ms: u64) -> impl Future<Output = Result<usize>> + Send;
    /// Writes the workflow's DAG and enqueues its root nodes in one atomic step. Fails if a
    /// workflow with this id exists.
    fn submit_workflow(&self, wf: &WorkflowSpec) -> impl Future<Output = Result<()>> + Send;
    fn workflow(&self, id: &str) -> impl Future<Output = Result<Option<WorkflowState>>> + Send;
    /// Stops the workflow from releasing further nodes. False if it is unknown or every node is done.
    fn cancel_workflow(&self, id: &str) -> impl Future<Output = Result<bool>> + Send;
    /// The subset of `ids` that are cancelled, in order.
    fn cancelled_workflows(&self, ids: &[String]) -> impl Future<Output = Result<Vec<String>>> + Send;
}
