//! The broker a `Client` or `Worker` talks to: Redis, or for a `memory://` URL an in-process `MemoryBroker`
//! shared by every client and worker in this process that names it.
use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};

use mayu_core::{
    Broker, DeadLetter, Delivery, Error, Header, MemoryBroker, PublishedTasks, QueueStats, Reclaimed, Result,
    RunningTask, StoredResult, WorkerInfo, WorkflowSpec, WorkflowState,
};
use mayu_redis::RedisBroker;

const MEMORY_SCHEME: &str = "memory://";

/// Brokers of `memory://<name>` URLs by name. Created on first use and kept for the life of the process.
static MEMORY: LazyLock<Mutex<HashMap<String, MemoryBroker>>> = LazyLock::new(Default::default);

/// Where a broker URL points.
pub enum Target {
    Redis(String),
    /// The name of an in-process broker (empty for plain `memory://`).
    Memory(String),
}

impl Target {
    /// `memory://` and `memory://<name>` are in-process brokers; any other URL goes to Redis unchanged.
    pub fn parse(url: &str) -> Result<Target> {
        let Some(rest) = url.get(..MEMORY_SCHEME.len())
            .filter(|s| s.eq_ignore_ascii_case(MEMORY_SCHEME))
            .map(|_| &url[MEMORY_SCHEME.len()..])
        else {
            return Ok(Target::Redis(url.to_string()));
        };
        if rest.contains(['/', '?', '#', '@', ':']) {
            return Err(Error::Config(format!(
                "a memory:// broker URL takes only a name, like memory:// or memory://jobs; got {url:?}"
            )));
        }
        Ok(Target::Memory(rest.to_string()))
    }
}

// One per client or worker; boxing the Redis variant would only add a pointer chase to every call.
#[allow(clippy::large_enum_variant)]
pub enum Backend {
    Redis(RedisBroker),
    Memory(MemoryBroker),
}

/// Calls the same method on whichever broker this is.
macro_rules! delegate {
    ($self:ident, $b:ident => $call:expr) => {
        match $self {
            Backend::Redis($b) => $call,
            Backend::Memory($b) => $call,
        }
    };
}

impl Backend {
    pub async fn connect(target: Target, consumer: impl Into<String>) -> Result<Self> {
        match target {
            Target::Redis(url) => RedisBroker::connect(&url, consumer).await.map(Backend::Redis),
            Target::Memory(name) => {
                let shared = MEMORY.lock().unwrap().entry(name).or_default().clone();
                Ok(Backend::Memory(shared.with_consumer(&consumer.into())))
            }
        }
    }

    pub async fn worker_info(&self, worker_id: &str) -> Result<Option<WorkerInfo>> {
        delegate!(self, b => b.worker_info(worker_id).await)
    }

    pub async fn workers(&self) -> Result<Vec<WorkerInfo>> {
        delegate!(self, b => b.workers().await)
    }

    pub async fn known_queues(&self) -> Result<Vec<String>> {
        delegate!(self, b => b.known_queues().await)
    }

    pub async fn dead_letters(&self, queue: &str, after: Option<&str>, until: Option<&str>, count: usize)
        -> Result<Vec<DeadLetter>> {
        delegate!(self, b => b.dead_letters(queue, after, until, count).await)
    }

    pub async fn last_dead_id(&self, queue: &str) -> Result<Option<String>> {
        delegate!(self, b => b.last_dead_id(queue).await)
    }

    pub async fn replay_dead(&self, queue: &str, entry_ids: &[String]) -> Result<usize> {
        delegate!(self, b => b.replay_dead(queue, entry_ids).await)
    }

    pub async fn workflows(&self) -> Result<Vec<WorkflowState>> {
        delegate!(self, b => b.workflows().await)
    }
}

impl Broker for Backend {
    async fn ensure_queues(&self, queues: &[String]) -> Result<()> {
        delegate!(self, b => b.ensure_queues(queues).await)
    }

    async fn enqueue(&self, header: &Header, payload: &[u8]) -> Result<()> {
        delegate!(self, b => b.enqueue(header, payload).await)
    }

    async fn fetch(&self, queues: &[String], count: usize, block_ms: u64) -> Result<Vec<Delivery>> {
        delegate!(self, b => b.fetch(queues, count, block_ms).await)
    }

    async fn ack(&self, d: &Delivery, result: Option<StoredResult>, follow_ups: &[(Header, Vec<u8>)]) -> Result<()> {
        delegate!(self, b => b.ack(d, result, follow_ups).await)
    }

    async fn retry(&self, d: &Delivery, header: &Header) -> Result<()> {
        delegate!(self, b => b.retry(d, header).await)
    }

    async fn dead_letter(&self, d: &Delivery, error: &str, result: Option<StoredResult>) -> Result<()> {
        delegate!(self, b => b.dead_letter(d, error, result).await)
    }

    async fn requeue(&self, d: &Delivery) -> Result<()> {
        delegate!(self, b => b.requeue(d).await)
    }

    async fn promote(&self, queue: &str, now_ms: u64, limit: usize) -> Result<usize> {
        delegate!(self, b => b.promote(queue, now_ms, limit).await)
    }

    async fn get_result(&self, id: &str) -> Result<Option<Vec<u8>>> {
        delegate!(self, b => b.get_result(id).await)
    }

    async fn queue_stats(&self, queue: &str) -> Result<QueueStats> {
        delegate!(self, b => b.queue_stats(queue).await)
    }

    async fn extend(&self, queue: &str, stream_ids: &[String]) -> Result<Vec<String>> {
        delegate!(self, b => b.extend(queue, stream_ids).await)
    }

    async fn reclaim(&self, queue: &str, min_idle_ms: u64, cursor: &str, count: usize) -> Result<Reclaimed> {
        delegate!(self, b => b.reclaim(queue, min_idle_ms, cursor, count).await)
    }

    async fn heartbeat(&self, info: &WorkerInfo, ttl_ms: u64) -> Result<()> {
        delegate!(self, b => b.heartbeat(info, ttl_ms).await)
    }

    async fn running(&self, worker_id: &str) -> Result<Vec<RunningTask>> {
        delegate!(self, b => b.running(worker_id).await)
    }

    async fn running_all(&self) -> Result<PublishedTasks> {
        delegate!(self, b => b.running_all().await)
    }

    async fn unregister(&self, worker_id: &str) -> Result<()> {
        delegate!(self, b => b.unregister(worker_id).await)
    }

    async fn prune_consumers(&self, queue: &str, min_idle_ms: u64) -> Result<usize> {
        delegate!(self, b => b.prune_consumers(queue, min_idle_ms).await)
    }

    async fn submit_workflow(&self, wf: &WorkflowSpec) -> Result<()> {
        delegate!(self, b => b.submit_workflow(wf).await)
    }

    async fn workflow(&self, id: &str) -> Result<Option<WorkflowState>> {
        delegate!(self, b => b.workflow(id).await)
    }

    async fn cancel_workflow(&self, id: &str) -> Result<bool> {
        delegate!(self, b => b.cancel_workflow(id).await)
    }

    async fn cancelled_workflows(&self, ids: &[String]) -> Result<Vec<String>> {
        delegate!(self, b => b.cancelled_workflows(ids).await)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn name(url: &str) -> Option<String> {
        match Target::parse(url).unwrap() {
            Target::Memory(n) => Some(n),
            Target::Redis(_) => None,
        }
    }

    #[test]
    fn memory_urls_name_a_broker() {
        assert_eq!(name("memory://"), Some(String::new()));
        assert_eq!(name("memory://jobs"), Some("jobs".into()));
        assert_eq!(name("MEMORY://jobs"), Some("jobs".into()));
        assert_eq!(name("redis://127.0.0.1:6390/0"), None);
        assert_eq!(name("rediss://h/1"), None);
        assert_eq!(name("mem"), None);
    }

    #[test]
    fn memory_urls_take_nothing_after_the_name() {
        for bad in ["memory://jobs/0", "memory://jobs?x=1", "memory:///jobs", "memory://h:1", "memory://a#b"] {
            let Err(Error::Config(m)) = Target::parse(bad) else { panic!("{bad} parsed") };
            assert!(m.contains("takes only a name"), "{m}");
        }
    }

    #[test]
    fn the_same_name_shares_state_and_other_names_do_not() {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        rt.block_on(async {
            let q = "q".to_string();
            let a = Backend::connect(Target::Memory("backend-test-a".into()), "p").await.unwrap();
            let a2 = Backend::connect(Target::Memory("backend-test-a".into()), "w").await.unwrap();
            let b = Backend::connect(Target::Memory("backend-test-b".into()), "w").await.unwrap();
            a.enqueue(&Header::new("t", &q, 0, "ct"), b"").await.unwrap();
            assert!(b.fetch(std::slice::from_ref(&q), 1, 10).await.unwrap().is_empty());
            assert_eq!(a2.fetch(std::slice::from_ref(&q), 1, 10).await.unwrap().len(), 1);
        });
    }
}
