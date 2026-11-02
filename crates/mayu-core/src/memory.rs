//! In-process `Broker`: a faithful model of `RedisBroker`, used to test the runtime without Redis and by
//! embedded mode (`memory://`).
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::Notify;

use crate::broker::{Broker, DeadLetter, Delivery, PublishedTasks, QueueStats, Reclaimed, StoredResult, WorkerInfo};
use crate::envelope::{Header, decode_header, encode_header, now_ms};
use crate::error::{Error, Result};
use crate::progress::RunningTask;
use crate::workflow::{NodeInfo, NodeState, WorkflowSpec, WorkflowState};

#[derive(Debug, Clone)]
pub struct DeadEntry {
    pub queue: String,
    pub header: Option<Header>,
    pub error: String,
}

#[derive(Clone)]
struct Entry {
    id: String,
    h: Vec<u8>,
    p: Vec<u8>,
}

struct Pending {
    queue: String,
    entry: Entry,
    owner: String,
    last_delivery: Instant,
    count: u64,
}

/// (eta ms, header, payload)
type DelayedEntry = (u64, Vec<u8>, Vec<u8>);

struct MemNode {
    task_id: String,
    task: String,
    queue: String,
    state: NodeState,
    parents: Vec<usize>,
    children: Vec<usize>,
    /// Encoded header and payload of a non-root node, enqueued when its parents are done.
    held: Option<(Vec<u8>, Vec<u8>)>,
}

struct MemWorkflow {
    name: String,
    created_at: u64,
    /// Like the Redis hash's TTL: set on submit, refreshed when a node is released or replayed.
    ttl: Duration,
    expires: Instant,
    cancelled: bool,
    /// Nodes in state `Done`; `cancel_workflow` is a no-op once it reaches the node count.
    done: usize,
    nodes: Vec<MemNode>,
}

/// A dead letter as the dead stream holds it.
struct DeadStored {
    /// `<ms>-<seq>`, increasing, like a Redis stream id.
    entry_id: String,
    id: (u64, u64),
    queue: String,
    /// What `dead()` reports: the delivery's header, or `None` for an undecodable entry.
    header: Option<Header>,
    raw_header: Vec<u8>,
    payload: Vec<u8>,
    error: String,
}

/// A stored result and when it expires (`None`: never, for `put_result`).
struct StoredRecord {
    record: Vec<u8>,
    expires: Option<Instant>,
}

/// Results are swept of expired records once the map grows past this, then past twice its live size.
const RESULT_SWEEP_MIN: usize = 1024;

#[derive(Default)]
struct State {
    seq: u64,
    streams: HashMap<String, VecDeque<Entry>>,
    pending: HashMap<String, Pending>,
    delayed: HashMap<String, Vec<DelayedEntry>>,
    dead: Vec<DeadStored>,
    /// Queues that have a dead stream; like Redis, it stays after its entries are replayed.
    dead_queues: HashSet<String>,
    last_dead_id: (u64, u64),
    results: HashMap<String, StoredRecord>,
    results_sweep_at: usize,
    workers: HashMap<String, (WorkerInfo, Instant)>,
    workflows: HashMap<String, MemWorkflow>,
}

/// Parses a stream id `ms-seq`, or `ms` with `seq` defaulting to `missing_seq` (as Redis completes a range bound).
fn parse_stream_id(id: &str, missing_seq: u64) -> Result<(u64, u64)> {
    let bad = || Error::Broker(format!("invalid stream id {id:?}"));
    let (ms, seq) = match id.split_once('-') {
        Some((ms, seq)) => (ms, Some(seq)),
        None => (id, None),
    };
    let ms = ms.parse().map_err(|_| bad())?;
    let seq = seq.map_or(Ok(missing_seq), |s| s.parse().map_err(|_| bad()))?;
    Ok((ms, seq))
}

impl State {
    fn push_dead(&mut self, queue: &str, header: Option<Header>, raw_header: Vec<u8>, payload: Vec<u8>, error: String) {
        let ms = now_ms();
        let id = if ms > self.last_dead_id.0 { (ms, 0) } else { (self.last_dead_id.0, self.last_dead_id.1 + 1) };
        self.last_dead_id = id;
        self.dead_queues.insert(queue.to_string());
        self.dead.push(DeadStored {
            entry_id: format!("{}-{}", id.0, id.1),
            id,
            queue: queue.to_string(),
            header,
            raw_header,
            payload,
            error,
        });
    }

    fn store_result(&mut self, r: StoredResult) {
        let expires = Instant::now() + Duration::from_secs(r.ttl_secs);
        self.results.insert(r.id, StoredRecord { record: r.record, expires: Some(expires) });
        if self.results.len() >= self.results_sweep_at.max(RESULT_SWEEP_MIN) {
            let now = Instant::now();
            self.results.retain(|_, r| r.expires.is_none_or(|e| e > now));
            self.results_sweep_at = self.results.len() * 2;
        }
    }

    fn result(&mut self, id: &str) -> Option<Vec<u8>> {
        let live = self.results.get(id)?.expires.is_none_or(|e| e > Instant::now());
        if !live {
            self.results.remove(id);
            return None;
        }
        self.results.get(id).map(|r| r.record.clone())
    }

    /// The workflow, unless it is unknown or its TTL ran out (then it is dropped, as Redis expires the hash).
    fn workflow_mut(&mut self, id: &str) -> Option<&mut MemWorkflow> {
        if self.workflows.get(id).is_some_and(|w| w.expires <= Instant::now()) {
            self.workflows.remove(id);
        }
        self.workflows.get_mut(id)
    }

    fn sweep_workflows(&mut self) {
        let now = Instant::now();
        self.workflows.retain(|_, w| w.expires > now);
    }

    fn push_stream(&mut self, queue: &str, h: Vec<u8>, p: Vec<u8>) {
        self.seq += 1;
        let id = format!("{}-0", self.seq);
        self.streams.entry(queue.to_string()).or_default().push_back(Entry { id, h, p });
    }

    /// Dead-letters an entry whose header can't be decoded.
    fn bury(&mut self, queue: &str, h: Vec<u8>, p: Vec<u8>, error: String) {
        self.push_dead(queue, None, h, p, error);
    }

    /// Marks node `i` done and enqueues the children it was the last missing parent of. Does
    /// nothing for a node already done or cancelled, so a duplicate run releases nothing, and in
    /// a cancelled workflow it marks the node cancelled instead. False when nothing was enqueued.
    /// An expired (unknown) workflow releases nothing, like `release.lua` finding no hash.
    fn release(&mut self, wf_id: &str, i: usize) -> Result<bool> {
        let Some(wf) = self.workflow_mut(wf_id) else { return Ok(false) };
        let Some(state) = wf.nodes.get(i).map(|n| n.state) else {
            return Err(Error::Broker(format!("workflow {wf_id} has no node {i}")));
        };
        if matches!(state, NodeState::Done | NodeState::Cancelled) {
            return Ok(false);
        }
        if wf.cancelled {
            wf.nodes[i].state = NodeState::Cancelled;
            wf.expires = Instant::now() + wf.ttl;
            return Ok(false);
        }
        let mut ready = Vec::new();
        for &c in &wf.nodes[i].children {
            let child = &wf.nodes[c];
            let parents_done = child.parents.iter().all(|&p| p == i || wf.nodes[p].state == NodeState::Done);
            if child.state == NodeState::Waiting && parents_done {
                if child.held.is_none() {
                    return Err(Error::Broker(format!("workflow {wf_id} node {c} has no stored task")));
                }
                ready.push(c);
            }
        }
        wf.nodes[i].state = NodeState::Done;
        wf.done += 1;
        wf.expires = Instant::now() + wf.ttl;
        let mut out = Vec::new();
        for c in ready {
            let child = &mut wf.nodes[c];
            child.state = NodeState::Queued;
            out.extend(child.held.take().map(|(h, p)| (child.queue.clone(), h, p)));
        }
        let released = !out.is_empty();
        for (q, h, p) in out {
            self.push_stream(&q, h, p);
        }
        Ok(released)
    }

    fn take(&mut self, queues: &[String], count: usize, consumer: &str) -> Vec<Delivery> {
        let mut out = Vec::new();
        for q in queues {
            while out.len() < count {
                let Some(e) = self.streams.get_mut(q).and_then(|s| s.pop_front()) else { break };
                match decode_header(&e.h) {
                    Ok(header) => {
                        out.push(Delivery {
                            queue: q.clone(),
                            stream_id: e.id.clone(),
                            header,
                            raw_header: e.h.clone(),
                            payload: e.p.clone(),
                            delivery_count: 1,
                        });
                        self.pending.insert(e.id.clone(), Pending {
                            queue: q.clone(),
                            entry: e,
                            owner: consumer.to_string(),
                            last_delivery: Instant::now(),
                            count: 1,
                        });
                    }
                    Err(err) => self.bury(q, e.h, e.p, err.to_string()),
                }
            }
        }
        out
    }
}

#[derive(Clone, Default)]
pub struct MemoryBroker {
    state: Arc<Mutex<State>>,
    notify: Arc<Notify>,
    failing: Arc<AtomicBool>,
    fail_retries: Arc<AtomicBool>,
    fail_cancelled: Arc<AtomicBool>,
    fail_running_all: Arc<AtomicBool>,
    running_all_calls: Arc<AtomicUsize>,
    fetch_extra: Arc<AtomicUsize>,
    record_delay_ns: Arc<AtomicU64>,
    consumer: String,
}

impl MemoryBroker {
    /// A handle on the same state that acts as a different consumer (worker).
    pub fn with_consumer(&self, consumer: &str) -> Self {
        MemoryBroker { consumer: consumer.to_string(), ..self.clone() }
    }

    /// While set, every `Broker` call fails, simulating an outage.
    pub fn set_failing(&self, on: bool) {
        self.failing.store(on, Ordering::SeqCst);
    }

    /// Test aid: while set, only `retry` fails (every other call keeps working), simulating a
    /// broker that rejects deferring a task.
    pub fn fail_retries(&self, on: bool) {
        self.fail_retries.store(on, Ordering::SeqCst);
    }

    /// Test aid: while set, only `running_all` fails.
    pub fn fail_running_all(&self, on: bool) {
        self.fail_running_all.store(on, Ordering::SeqCst);
    }

    /// Test aid: how many times `running_all` was called, on this broker and every handle sharing its state.
    pub fn running_all_calls(&self) -> usize {
        self.running_all_calls.load(Ordering::SeqCst)
    }

    /// Test aid: while set, only `cancelled_workflows` fails.
    pub fn fail_cancelled_checks(&self, on: bool) {
        self.fail_cancelled.store(on, Ordering::SeqCst);
    }

    /// Test aid: lets `fetch` return up to `n` entries beyond the requested `count`, like a
    /// blocking read across queues that claims one entry per queue past the limit.
    pub fn fetch_extra(&self, n: usize) {
        self.fetch_extra.store(n, Ordering::SeqCst);
    }

    /// Test aid: while set, `ack`, `retry` and `dead_letter` first wait `d`, like a broker with a
    /// slow round trip. `Duration::ZERO` clears it.
    pub fn set_record_delay(&self, d: Duration) {
        self.record_delay_ns.store(d.as_nanos().min(u64::MAX as u128) as u64, Ordering::SeqCst);
    }

    async fn record_delay(&self) {
        let ns = self.record_delay_ns.load(Ordering::SeqCst);
        if ns > 0 {
            tokio::time::sleep(Duration::from_nanos(ns)).await;
        }
    }

    fn check(&self) -> Result<()> {
        if self.failing.load(Ordering::SeqCst) {
            Err(Error::Broker("injected failure".into()))
        } else {
            Ok(())
        }
    }

    pub fn push(&self, header: &Header, payload: &[u8]) {
        let h = encode_header(header);
        {
            let mut s = self.state.lock().unwrap();
            match header.eta {
                Some(eta) if eta > now_ms() => {
                    s.delayed.entry(header.queue.clone()).or_default().push((eta, h, payload.to_vec()))
                }
                _ => s.push_stream(&header.queue, h, payload.to_vec()),
            }
        }
        self.notify.notify_waiters();
    }

    /// Test aid: appends a stream entry with these raw header bytes, like an `XADD` by another writer.
    pub fn push_raw(&self, queue: &str, h: Vec<u8>, p: Vec<u8>) {
        self.state.lock().unwrap().push_stream(queue, h, p);
        self.notify.notify_waiters();
    }

    pub fn pending_len(&self) -> usize { self.state.lock().unwrap().pending.len() }
    pub fn stream_len(&self, q: &str) -> usize {
        self.state.lock().unwrap().streams.get(q).map_or(0, |s| s.len())
    }
    pub fn delayed_len(&self, q: &str) -> usize {
        self.state.lock().unwrap().delayed.get(q).map_or(0, |d| d.len())
    }
    pub fn dead(&self) -> Vec<DeadEntry> {
        let s = self.state.lock().unwrap();
        s.dead.iter().map(|d| DeadEntry { queue: d.queue.clone(), header: d.header.clone(), error: d.error.clone() })
            .collect()
    }
    /// Stores a result record directly, as a finished task's ack would; it never expires.
    pub fn put_result(&self, id: &str, record: Vec<u8>) {
        self.state.lock().unwrap().results.insert(id.to_string(), StoredRecord { record, expires: None });
    }
    pub fn result(&self, id: &str) -> Option<Vec<u8>> { self.state.lock().unwrap().result(id) }
    pub fn delivery_count(&self, stream_id: &str) -> Option<u64> {
        self.state.lock().unwrap().pending.get(stream_id).map(|p| p.count)
    }
    /// The worker's heartbeat, if it has not expired.
    pub fn worker(&self, id: &str) -> Option<WorkerInfo> {
        let s = self.state.lock().unwrap();
        s.workers.get(id).filter(|(_, expires)| *expires > Instant::now()).map(|(w, _)| w.clone())
    }

    /// The workflow id and node index of a delivery, if it is a workflow node.
    fn workflow_node(d: &Delivery) -> Result<Option<(&str, usize)>> {
        let Some(wf) = d.header.workflow_id.as_deref() else { return Ok(None) };
        d.header.node.as_deref().and_then(|n| n.parse().ok()).map(|i| Some((wf, i))).ok_or_else(|| {
            Error::Broker(format!("task {} has a workflow id but no valid node", d.header.id))
        })
    }

    fn store(s: &mut State, r: Option<StoredResult>) {
        if let Some(r) = r { s.store_result(r); }
    }

    /// Whether this consumer still holds `d`'s pending entry (not reclaimed by another, not settled).
    fn owns(&self, s: &State, d: &Delivery) -> bool {
        matches!(s.pending.get(&d.stream_id), Some(p) if p.owner == self.consumer)
    }

    // The admin calls `RedisBroker` has besides the `Broker` trait, with the same semantics.

    /// The worker's last heartbeat, if it has not expired.
    pub async fn worker_info(&self, worker_id: &str) -> Result<Option<WorkerInfo>> {
        self.check()?;
        Ok(self.worker(worker_id))
    }

    /// Workers with a live heartbeat, sorted by id.
    pub async fn workers(&self) -> Result<Vec<WorkerInfo>> {
        self.check()?;
        let now = Instant::now();
        let s = self.state.lock().unwrap();
        let mut out: Vec<WorkerInfo> = s.workers.values().filter(|(_, e)| *e > now).map(|(w, _)| w.clone()).collect();
        out.sort_by(|a, b| a.worker_id.cmp(&b.worker_id));
        Ok(out)
    }

    /// Dead letters of `queue`, oldest first: strictly after `after`, up to and including `until`.
    pub async fn dead_letters(&self, queue: &str, after: Option<&str>, until: Option<&str>, count: usize)
        -> Result<Vec<DeadLetter>>
    {
        self.check()?;
        let after = after.map(|id| parse_stream_id(id, 0)).transpose()?;
        let until = until.map(|id| parse_stream_id(id, u64::MAX)).transpose()?;
        let s = self.state.lock().unwrap();
        Ok(s.dead.iter()
            .filter(|d| d.queue == queue && after.is_none_or(|a| d.id > a) && until.is_none_or(|u| d.id <= u))
            .take(count.max(1))
            .map(|d| DeadLetter {
                entry_id: d.entry_id.clone(),
                header: decode_header(&d.raw_header).ok(),
                raw_header: d.raw_header.clone(),
                payload: d.payload.clone(),
                error: d.error.clone(),
            })
            .collect())
    }

    /// Id of the newest dead letter of `queue`.
    pub async fn last_dead_id(&self, queue: &str) -> Result<Option<String>> {
        self.check()?;
        let s = self.state.lock().unwrap();
        Ok(s.dead.iter().rev().find(|d| d.queue == queue).map(|d| d.entry_id.clone()))
    }

    /// Moves dead letters back to `queue` to run again (attempt 0, no eta, same task id), deleting
    /// their stored failure results. Missing or undecodable entries are skipped. Returns how many moved.
    pub async fn replay_dead(&self, queue: &str, entry_ids: &[String]) -> Result<usize> {
        self.check()?;
        let ids = entry_ids.iter().map(|id| parse_stream_id(id, 0)).collect::<Result<Vec<_>>>()?;
        let moved = {
            let mut s = self.state.lock().unwrap();
            let mut moved = 0;
            for id in ids {
                let Some(pos) = s.dead.iter().position(|d| d.queue == queue && d.id == id) else { continue };
                let Ok(mut h) = decode_header(&s.dead[pos].raw_header) else { continue };
                let payload = s.dead.remove(pos).payload;
                h.attempt = 0;
                h.eta = None;
                s.push_stream(queue, encode_header(&h), payload);
                s.results.remove(&h.id);
                moved += 1;
                let node = h.node.as_deref().and_then(|n| n.parse::<usize>().ok());
                if let (Some(wf), Some(i)) = (h.workflow_id.as_deref(), node)
                    && let Some(w) = s.workflow_mut(wf)
                    && let Some(n) = w.nodes.get_mut(i)
                    && n.state == NodeState::Dead
                {
                    n.state = NodeState::Queued;
                    w.expires = Instant::now() + w.ttl;
                }
            }
            moved
        };
        if moved > 0 {
            self.notify.notify_waiters();
        }
        Ok(moved)
    }

    /// Workflows that haven't expired, oldest first.
    pub async fn workflows(&self) -> Result<Vec<WorkflowState>> {
        self.check()?;
        let mut s = self.state.lock().unwrap();
        s.sweep_workflows();
        let mut out: Vec<WorkflowState> = s.workflows.iter().map(|(id, w)| w.state(id)).collect();
        out.sort_by(|a, b| (a.created_at, &a.id).cmp(&(b.created_at, &b.id)));
        Ok(out)
    }

    /// Queues that have a stream, delayed entries or a dead stream, sorted.
    pub async fn known_queues(&self) -> Result<Vec<String>> {
        self.check()?;
        let s = self.state.lock().unwrap();
        let mut qs: Vec<String> = s.streams.keys()
            .chain(s.delayed.iter().filter(|(_, d)| !d.is_empty()).map(|(q, _)| q))
            .chain(s.dead_queues.iter())
            .cloned()
            .collect();
        qs.sort();
        qs.dedup();
        Ok(qs)
    }
}

impl MemWorkflow {
    fn state(&self, id: &str) -> WorkflowState {
        WorkflowState {
            id: id.to_string(),
            name: self.name.clone(),
            created_at: self.created_at,
            cancelled: self.cancelled,
            nodes: self.nodes.iter().enumerate().map(|(i, n)| NodeInfo {
                node: i.to_string(),
                task_id: n.task_id.clone(),
                task: n.task.clone(),
                queue: n.queue.clone(),
                state: n.state,
                parents: n.parents.clone(),
            }).collect(),
        }
    }
}

impl Broker for MemoryBroker {
    /// Creates each queue's (empty) stream, as `XGROUP CREATE ... MKSTREAM` does.
    async fn ensure_queues(&self, queues: &[String]) -> Result<()> {
        self.check()?;
        let mut s = self.state.lock().unwrap();
        for q in queues {
            s.streams.entry(q.clone()).or_default();
        }
        Ok(())
    }

    async fn enqueue(&self, header: &Header, payload: &[u8]) -> Result<()> {
        self.check()?;
        self.push(header, payload);
        Ok(())
    }

    async fn fetch(&self, queues: &[String], count: usize, block_ms: u64) -> Result<Vec<Delivery>> {
        self.check()?;
        let deadline = tokio::time::Instant::now() + Duration::from_millis(block_ms);
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let got = self.state.lock().unwrap().take(queues, count + self.fetch_extra.load(Ordering::SeqCst), &self.consumer);
            if !got.is_empty() { return Ok(got); }
            if tokio::time::timeout_at(deadline, notified).await.is_err() { return Ok(Vec::new()); }
        }
    }

    async fn ack(&self, d: &Delivery, result: Option<StoredResult>, follow_ups: &[(Header, Vec<u8>)]) -> Result<bool> {
        self.record_delay().await;
        self.check()?;
        let node = Self::workflow_node(d)?;
        if node.is_some() && !follow_ups.is_empty() {
            return Err(Error::Broker("a workflow node can't have link follow-ups".into()));
        }
        let wake = {
            let mut s = self.state.lock().unwrap();
            if !self.owns(&s, d) {
                // SET NX: an expired result counts as absent, as in Redis.
                if let Some(r) = result
                    && s.result(&r.id).is_none()
                {
                    s.store_result(r);
                }
                return Ok(false);
            }
            // First, because it is the only step that can fail and nothing has changed yet.
            let released = match node {
                Some((wf, i)) => s.release(wf, i)?,
                None => false,
            };
            let acked = s.pending.remove(&d.stream_id).is_some();
            Self::store(&mut s, result);
            if acked {
                for (h, p) in follow_ups {
                    s.push_stream(&h.queue, encode_header(h), p.clone());
                }
            }
            released || (acked && !follow_ups.is_empty())
        };
        if wake {
            self.notify.notify_waiters();
        }
        Ok(true)
    }

    async fn retry(&self, d: &Delivery, header: &Header) -> Result<bool> {
        self.record_delay().await;
        self.check()?;
        if self.fail_retries.load(Ordering::SeqCst) {
            return Err(Error::Broker("injected retry failure".into()));
        }
        let mut s = self.state.lock().unwrap();
        if !self.owns(&s, d) {
            return Ok(false);
        }
        s.pending.remove(&d.stream_id);
        s.delayed.entry(d.queue.clone()).or_default()
            .push((header.eta.unwrap_or(0), encode_header(header), d.payload.clone()));
        Ok(true)
    }

    async fn dead_letter(&self, d: &Delivery, error: &str, result: Option<StoredResult>) -> Result<bool> {
        self.record_delay().await;
        self.check()?;
        let mut s = self.state.lock().unwrap();
        if !self.owns(&s, d) {
            return Ok(false);
        }
        s.pending.remove(&d.stream_id);
        s.push_dead(&d.queue, Some(d.header.clone()), d.raw_header.clone(), d.payload.clone(), error.into());
        Self::store(&mut s, result);
        if let Ok(Some((wf, i))) = Self::workflow_node(d)
            && let Some(n) = s.workflow_mut(wf).and_then(|w| w.nodes.get_mut(i))
            && n.state != NodeState::Done
        {
            n.state = NodeState::Dead;
        }
        Ok(true)
    }

    async fn requeue(&self, d: &Delivery) -> Result<bool> {
        self.check()?;
        {
            let mut s = self.state.lock().unwrap();
            if !self.owns(&s, d) {
                return Ok(false);
            }
            s.pending.remove(&d.stream_id);
            s.push_stream(&d.queue, d.raw_header.clone(), d.payload.clone());
        }
        self.notify.notify_waiters();
        Ok(true)
    }

    async fn promote(&self, queue: &str, now_ms: u64, limit: usize) -> Result<usize> {
        self.check()?;
        let n = {
            let mut s = self.state.lock().unwrap();
            let delayed = s.delayed.entry(queue.to_string()).or_default();
            delayed.sort_by_key(|e| e.0);
            let due = delayed.iter().take(limit).take_while(|e| e.0 <= now_ms).count();
            let moved: Vec<_> = delayed.drain(..due).collect();
            for (_, h, p) in moved { s.push_stream(queue, h, p); }
            due
        };
        if n > 0 { self.notify.notify_waiters(); }
        Ok(n)
    }

    async fn get_result(&self, id: &str) -> Result<Option<Vec<u8>>> {
        self.check()?;
        Ok(self.result(id))
    }

    async fn queue_stats(&self, queue: &str) -> Result<QueueStats> {
        self.check()?;
        let s = self.state.lock().unwrap();
        let pending = s.pending.values().filter(|p| p.queue == queue).count() as u64;
        Ok(QueueStats {
            stream: s.streams.get(queue).map_or(0, |q| q.len() as u64) + pending,
            pending,
            delayed: s.delayed.get(queue).map_or(0, |d| d.len() as u64),
            dead: s.dead.iter().filter(|d| d.queue == queue).count() as u64,
        })
    }

    async fn extend(&self, _queue: &str, stream_ids: &[String]) -> Result<Vec<String>> {
        self.check()?;
        let mut s = self.state.lock().unwrap();
        let now = Instant::now();
        let mut lost = Vec::new();
        for id in stream_ids {
            match s.pending.get_mut(id) {
                Some(p) if p.owner == self.consumer => p.last_delivery = now,
                _ => lost.push(id.clone()),
            }
        }
        Ok(lost)
    }

    async fn reclaim(&self, queue: &str, min_idle_ms: u64, _cursor: &str, count: usize) -> Result<Reclaimed> {
        self.check()?;
        let mut s = self.state.lock().unwrap();
        let min_idle = Duration::from_millis(min_idle_ms);
        let mut ids: Vec<String> = s.pending.iter()
            .filter(|(_, p)| p.queue == queue && p.last_delivery.elapsed() >= min_idle)
            .map(|(id, _)| id.clone())
            .collect();
        ids.sort_by_key(|id| id.split('-').next().and_then(|n| n.parse::<u64>().ok()).unwrap_or(0));
        ids.truncate(count);
        let mut deliveries = Vec::new();
        for id in ids {
            let p = s.pending.get_mut(&id).expect("listed above");
            p.owner = self.consumer.clone();
            p.last_delivery = Instant::now();
            p.count += 1;
            let (entry, delivery_count) = (p.entry.clone(), p.count);
            match decode_header(&entry.h) {
                Ok(header) => deliveries.push(Delivery {
                    queue: queue.to_string(),
                    stream_id: id,
                    header,
                    raw_header: entry.h,
                    payload: entry.p,
                    delivery_count,
                }),
                Err(err) => {
                    s.pending.remove(&id);
                    s.bury(queue, entry.h, entry.p, err.to_string());
                }
            }
        }
        // The memory broker scans everything each call, so the cursor always wraps.
        Ok(Reclaimed { next_cursor: "0-0".into(), deliveries })
    }

    async fn heartbeat(&self, info: &WorkerInfo, ttl_ms: u64) -> Result<()> {
        self.check()?;
        let now = Instant::now();
        let mut s = self.state.lock().unwrap();
        s.workers.retain(|_, (_, expires)| *expires > now); // what Redis would have expired by now
        s.workers.insert(info.worker_id.clone(), (info.clone(), now + Duration::from_millis(ttl_ms)));
        Ok(())
    }

    async fn running(&self, worker_id: &str) -> Result<Vec<RunningTask>> {
        self.check()?;
        Ok(self.worker(worker_id).map(|w| w.tasks).unwrap_or_default())
    }

    async fn running_all(&self) -> Result<PublishedTasks> {
        self.running_all_calls.fetch_add(1, Ordering::SeqCst);
        self.check()?;
        if self.fail_running_all.load(Ordering::SeqCst) {
            return Err(Error::Broker("injected running_all failure".into()));
        }
        let now = Instant::now();
        let s = self.state.lock().unwrap();
        let mut workers: Vec<&(WorkerInfo, Instant)> = s.workers.values().filter(|(_, expires)| *expires > now).collect();
        workers.sort_by(|a, b| a.0.worker_id.cmp(&b.0.worker_id));
        // Typed in memory, so nothing here can be malformed.
        let tasks = workers.into_iter()
            .flat_map(|(w, _)| w.tasks.iter().map(|t| (w.worker_id.clone(), t.clone())))
            .collect();
        Ok(PublishedTasks { tasks, malformed: 0 })
    }

    async fn unregister(&self, worker_id: &str) -> Result<()> {
        self.check()?;
        self.state.lock().unwrap().workers.remove(worker_id);
        Ok(())
    }

    /// The memory broker keeps no consumer registry, so there is nothing to prune.
    async fn prune_consumers(&self, _queue: &str, _min_idle_ms: u64) -> Result<usize> {
        self.check()?;
        Ok(0)
    }

    async fn submit_workflow(&self, wf: &WorkflowSpec) -> Result<()> {
        self.check()?;
        wf.validate()?;
        {
            let mut s = self.state.lock().unwrap();
            if s.workflow_mut(&wf.id).is_some() {
                return Err(Error::Broker(format!("workflow {} already exists", wf.id)));
            }
            let children = wf.children();
            let mut nodes = Vec::with_capacity(wf.nodes.len());
            let mut roots = Vec::new();
            for (n, children) in wf.nodes.iter().zip(children) {
                let h = encode_header(&n.header);
                let root = n.parents.is_empty();
                let held = if root { None } else { Some((h.clone(), n.payload.clone())) };
                if root {
                    roots.push((n.header.queue.clone(), h, n.payload.clone()));
                }
                nodes.push(MemNode {
                    task_id: n.header.id.clone(),
                    task: n.header.task.clone(),
                    queue: n.header.queue.clone(),
                    state: if root { NodeState::Queued } else { NodeState::Waiting },
                    parents: n.parents.clone(),
                    children,
                    held,
                });
            }
            for (q, h, p) in roots {
                s.push_stream(&q, h, p);
            }
            let ttl = Duration::from_secs(wf.ttl_secs);
            s.workflows.insert(wf.id.clone(), MemWorkflow {
                name: wf.name.clone(),
                created_at: now_ms(),
                ttl,
                expires: Instant::now() + ttl,
                cancelled: false,
                done: 0,
                nodes,
            });
        }
        self.notify.notify_waiters();
        Ok(())
    }

    async fn workflow(&self, id: &str) -> Result<Option<WorkflowState>> {
        self.check()?;
        let mut s = self.state.lock().unwrap();
        Ok(s.workflow_mut(id).map(|w| w.state(id)))
    }

    async fn cancel_workflow(&self, id: &str) -> Result<bool> {
        self.check()?;
        let mut s = self.state.lock().unwrap();
        match s.workflow_mut(id) {
            Some(w) if w.done < w.nodes.len() => {
                w.cancelled = true;
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    async fn cancelled_workflows(&self, ids: &[String]) -> Result<Vec<String>> {
        self.check()?;
        if self.fail_cancelled.load(Ordering::SeqCst) {
            return Err(Error::Broker("injected cancelled-check failure".into()));
        }
        let mut s = self.state.lock().unwrap();
        Ok(ids.iter().filter(|id| s.workflow_mut(id).is_some_and(|w| w.cancelled)).cloned().collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rt() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap()
    }

    #[test]
    fn enqueue_fetch_ack() {
        rt().block_on(async {
            let b = MemoryBroker::default();
            let h = Header::new("t", "q", 3, "ct");
            b.enqueue(&h, b"p").await.unwrap();
            let ds = b.fetch(&["q".into()], 10, 10).await.unwrap();
            assert_eq!(ds.len(), 1);
            assert_eq!(ds[0].header, h);
            assert_eq!(ds[0].payload, b"p");
            assert_eq!(b.pending_len(), 1);
            b.ack(&ds[0], Some(StoredResult { id: h.id.clone(), record: b"r".to_vec(), ttl_secs: 9 }), &[]).await.unwrap();
            assert_eq!(b.pending_len(), 0);
            assert_eq!(b.result(&h.id), Some(b"r".to_vec()));
        });
    }

    #[test]
    fn fetch_times_out_empty() {
        rt().block_on(async {
            let b = MemoryBroker::default();
            let t = std::time::Instant::now();
            assert!(b.fetch(&["q".into()], 1, 50).await.unwrap().is_empty());
            assert!(t.elapsed() >= std::time::Duration::from_millis(45));
        });
    }

    #[test]
    fn fetch_wakes_on_enqueue() {
        rt().block_on(async {
            let b = MemoryBroker::default();
            let b2 = b.clone();
            let h = Header::new("t", "q", 0, "ct");
            let waiter = tokio::spawn(async move { b2.fetch(&["q".into()], 1, 5_000).await.unwrap() });
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            b.enqueue(&h, b"").await.unwrap();
            let got = tokio::time::timeout(std::time::Duration::from_secs(1), waiter).await.unwrap().unwrap();
            assert_eq!(got.len(), 1);
        });
    }

    #[test]
    fn delayed_then_promote() {
        rt().block_on(async {
            let b = MemoryBroker::default();
            let mut h = Header::new("t", "q", 0, "ct");
            h.eta = Some(now_ms() + 60_000);
            b.enqueue(&h, b"").await.unwrap();
            assert_eq!(b.delayed_len("q"), 1);
            assert_eq!(b.promote("q", now_ms(), 100).await.unwrap(), 0);
            assert_eq!(b.promote("q", now_ms() + 120_000, 100).await.unwrap(), 1);
            assert_eq!(b.stream_len("q"), 1);
        });
    }

    #[test]
    fn retry_dead_requeue() {
        rt().block_on(async {
            let b = MemoryBroker::default();
            for _ in 0..3 { b.enqueue(&Header::new("t", "q", 1, "ct"), b"").await.unwrap(); }
            let ds = b.fetch(&["q".into()], 3, 10).await.unwrap();
            let mut next = ds[0].header.clone();
            next.attempt = 1;
            next.eta = Some(0);
            b.retry(&ds[0], &next).await.unwrap();
            b.dead_letter(&ds[1], "bad", None).await.unwrap();
            b.requeue(&ds[2]).await.unwrap();
            assert_eq!(b.pending_len(), 0);
            assert_eq!(b.delayed_len("q"), 1);
            assert_eq!(b.dead().len(), 1);
            assert_eq!(b.dead()[0].error, "bad");
            assert_eq!(b.stream_len("q"), 1);
        });
    }

    #[test]
    fn failing_switch_fails_calls_until_cleared() {
        rt().block_on(async {
            let b = MemoryBroker::default();
            let h = Header::new("t", "q", 0, "ct");
            b.set_failing(true);
            assert!(b.enqueue(&h, b"").await.is_err());
            assert!(b.fetch(&["q".into()], 1, 10).await.is_err());
            b.set_failing(false);
            b.enqueue(&h, b"").await.unwrap();
            assert_eq!(b.fetch(&["q".into()], 1, 10).await.unwrap().len(), 1);
        });
    }

    #[test]
    fn extend_keeps_ownership_and_reports_lost() {
        rt().block_on(async {
            let a = MemoryBroker::default().with_consumer("a");
            let b = a.with_consumer("b");
            a.enqueue(&Header::new("t", "q", 0, "ct"), b"").await.unwrap();
            let d = a.fetch(&["q".into()], 1, 10).await.unwrap().remove(0);
            assert_eq!(d.delivery_count, 1);
            assert!(a.extend("q", &[d.stream_id.clone()]).await.unwrap().is_empty());
            let r = b.reclaim("q", 0, "0-0", 10).await.unwrap();
            assert_eq!(r.deliveries.len(), 1);
            assert_eq!(r.deliveries[0].delivery_count, 2);
            assert_eq!(a.delivery_count(&d.stream_id), Some(2));
            assert_eq!(a.extend("q", &[d.stream_id.clone()]).await.unwrap(), vec![d.stream_id.clone()]);
        });
    }

    #[test]
    fn reclaim_respects_min_idle_and_queue() {
        rt().block_on(async {
            let a = MemoryBroker::default().with_consumer("a");
            let b = a.with_consumer("b");
            a.enqueue(&Header::new("t", "q", 0, "ct"), b"").await.unwrap();
            a.fetch(&["q".into()], 1, 10).await.unwrap();
            assert!(b.reclaim("q", 60_000, "0-0", 10).await.unwrap().deliveries.is_empty());
            assert!(b.reclaim("other", 0, "0-0", 10).await.unwrap().deliveries.is_empty());
            tokio::time::sleep(Duration::from_millis(30)).await;
            assert_eq!(b.reclaim("q", 20, "0-0", 10).await.unwrap().deliveries.len(), 1);
        });
    }

    #[test]
    fn heartbeat_expires_and_unregisters() {
        rt().block_on(async {
            let b = MemoryBroker::default();
            let info = WorkerInfo {
                worker_id: "w1".into(), hostname: "h".into(), pid: 1, queues: vec!["q".into()],
                concurrency: 2, inflight: 0, started_at: 0, last_seen: 0, tasks: vec![],
            };
            b.heartbeat(&info, 50).await.unwrap();
            assert_eq!(b.worker("w1"), Some(info.clone()));
            tokio::time::sleep(Duration::from_millis(80)).await;
            assert_eq!(b.worker("w1"), None);
            b.heartbeat(&info, 60_000).await.unwrap();
            b.unregister("w1").await.unwrap();
            assert_eq!(b.worker("w1"), None);
        });
    }

    #[test]
    fn ack_enqueues_follow_ups_only_once() {
        rt().block_on(async {
            let b = MemoryBroker::default();
            b.enqueue(&Header::new("t", "q", 0, "ct"), b"").await.unwrap();
            let d = b.fetch(&["q".into()], 1, 10).await.unwrap().remove(0);
            let follow = [(Header::new("cb", "q2", 1, "ct"), b"x".to_vec())];
            b.ack(&d, None, &follow).await.unwrap();
            b.ack(&d, None, &follow).await.unwrap(); // a retried ack: already acked, adds nothing
            let got = b.fetch(&["q2".into()], 10, 10).await.unwrap();
            assert_eq!(got.len(), 1);
            assert_eq!(got[0].header, follow[0].0);
            assert_eq!(got[0].payload, b"x");
        });
    }
}
