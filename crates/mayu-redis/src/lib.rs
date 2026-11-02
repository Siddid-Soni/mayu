//! Redis Streams implementation of `mayu_core::Broker`.
use std::collections::HashMap;
use std::time::Duration;

pub use mayu_core::DeadLetter;
use mayu_core::{
    Broker, Delivery, Error, Header, NodeInfo, NodeState, PublishedTasks, QueueStats, Reclaimed, Result, RunningTask, StoredResult,
    WorkerInfo, WorkflowSpec, WorkflowState, decode_header, decode_running, encode_delayed_member, encode_header,
    encode_running, now_ms,
};
use redis::aio::{ConnectionManager, ConnectionManagerConfig};
use redis::streams::StreamReadReply;
use redis::{Script, Value};

pub const GROUP: &str = "mayu";
/// XREADGROUP BLOCK is clamped below the fetch connection's response timeout.
const MAX_BLOCK_MS: u64 = 10_000;
const COMMAND_TIMEOUT: Duration = Duration::from_secs(5);

pub fn stream_key(q: &str) -> String { format!("mayu:{{{q}}}:stream") }
pub fn delayed_key(q: &str) -> String { format!("mayu:{{{q}}}:delayed") }
pub fn dead_key(q: &str) -> String { format!("mayu:{{{q}}}:dead") }
pub fn result_key(id: &str) -> String { format!("mayu:result:{id}") }
const WORKER_KEY_PREFIX: &str = "mayu:workers:";
pub fn worker_key(id: &str) -> String { format!("{WORKER_KEY_PREFIX}{id}") }
const WORKFLOW_KEY_PREFIX: &str = "mayu:wf:";
pub fn workflow_key(id: &str) -> String { format!("{WORKFLOW_KEY_PREFIX}{id}") }

fn queue_from_stream_key(k: &str) -> &str {
    k.strip_prefix("mayu:{").and_then(|s| s.strip_suffix("}:stream")).unwrap_or(k)
}

fn be(e: impl std::fmt::Display) -> Error { Error::Broker(e.to_string()) }

/// The heartbeat's `tasks` field: set while tasks run, removed when idle so a reader never sees stale ones.
/// Stored as lowercase hex of the msgpack so the hash stays valid UTF-8 for readers of older versions
/// (they `HGETALL` it into strings).
fn tasks_cmd(key: &str, tasks: &[RunningTask]) -> redis::Cmd {
    if tasks.is_empty() {
        let mut c = redis::cmd("HDEL");
        c.arg(key).arg("tasks");
        c
    } else {
        let mut c = redis::cmd("HSET");
        c.arg(key).arg("tasks").arg(to_hex(&encode_running(tasks)));
        c
    }
}

fn to_hex(b: &[u8]) -> String {
    use std::fmt::Write;
    b.iter().fold(String::with_capacity(b.len() * 2), |mut s, x| {
        let _ = write!(s, "{x:02x}");
        s
    })
}

fn from_hex(s: &str) -> Option<Vec<u8>> {
    if s.len() % 2 != 0 || !s.is_ascii() {
        return None;
    }
    (0..s.len()).step_by(2).map(|i| u8::from_str_radix(&s[i..i + 2], 16).ok()).collect()
}

fn decode_tasks(raw: &str) -> std::result::Result<Vec<RunningTask>, String> {
    from_hex(raw).ok_or_else(|| "not hex".to_string()).and_then(|b| decode_running(&b).map_err(|e| e.to_string()))
}

/// A worker's `tasks` field; a malformed one reads as empty so a bad writer can't break `workers()` or inspect.
fn parse_tasks(worker_id: &str, raw: &str) -> Vec<RunningTask> {
    decode_tasks(raw).unwrap_or_else(|e| {
        tracing::warn!(worker = %worker_id, error = %e, "ignoring a malformed tasks field in a worker heartbeat");
        Vec::new()
    })
}

/// Splits the metadata `HMGET` of a workflow hash into (node count, name, created_at, status).
fn parse_workflow_meta(id: &str, meta: Vec<Option<String>>) -> Result<(usize, String, u64, String)> {
    let [Some(nodes), Some(name), Some(created_at), Some(status)] = <[Option<String>; 4]>::try_from(meta)
        .map_err(|_| be("malformed HMGET reply"))?
    else {
        return Err(be(format!("workflow {id} is missing its metadata")));
    };
    let bad = |what: &str| be(format!("workflow {id} has a malformed {what}"));
    let count: usize = nodes.parse().map_err(|_| bad("node count"))?;
    let created_at: u64 = created_at.parse().map_err(|_| bad("created_at"))?;
    Ok((count, name, created_at, status))
}

/// Builds a `WorkflowState` from the per-node `HMGET` (id, task, queue, state, up for each of `count` nodes).
/// `up` is absent in hashes written by older versions, which reads as no parents.
fn parse_workflow_nodes(
    id: &str, name: String, created_at: u64, cancelled: bool, count: usize, vals: &[Option<String>],
) -> Result<WorkflowState> {
    if vals.len() != count * 5 {
        return Err(be(format!("workflow {id} has a malformed node list")));
    }
    let mut nodes = Vec::with_capacity(count);
    for (i, f) in vals.chunks(5).enumerate() {
        let [Some(task_id), Some(task), Some(queue), Some(state), up] = f else {
            return Err(be(format!("workflow {id} node {i} is missing fields")));
        };
        nodes.push(NodeInfo {
            node: i.to_string(),
            task_id: task_id.clone(),
            task: task.clone(),
            queue: queue.clone(),
            state: NodeState::parse(state)?,
            parents: parse_parents(id, i, up.as_deref()),
        });
    }
    Ok(WorkflowState { id: id.to_string(), name, created_at, cancelled, nodes })
}

/// `n:<i>:up`: comma-separated parent indexes (empty for a root); `None` when the field is missing. Parents only
/// feed the lookahead estimate, so a malformed list reads as none (with a warning) instead of failing the read.
fn parse_parents(id: &str, node: usize, up: Option<&str>) -> Vec<usize> {
    let parsed: std::result::Result<Vec<usize>, _> =
        up.unwrap_or("").split(',').filter(|p| !p.is_empty()).map(str::parse).collect();
    parsed.unwrap_or_else(|_| {
        tracing::warn!(workflow = %id, node, "ignoring a malformed parent list in a workflow hash");
        Vec::new()
    })
}

struct Scripts {
    ack: Script,
    retry: Script,
    dead: Script,
    requeue: Script,
    promote: Script,
    extend: Script,
    reclaim: Script,
    prune: Script,
    fetch: Script,
    replay: Script,
    submit: Script,
    release: Script,
    cancel: Script,
}

async fn manager(client: &redis::Client, response_timeout: Duration) -> Result<ConnectionManager> {
    let cfg = ConnectionManagerConfig::new()
        .set_connection_timeout(Some(COMMAND_TIMEOUT))
        .set_response_timeout(Some(response_timeout))
        .set_number_of_retries(2);
    ConnectionManager::new_with_config(client.clone(), cfg).await.map_err(be)
}

/// A stream entry: (entry id, fields).
type RangeEntry = (String, HashMap<String, Vec<u8>>);

/// An `XRANGE`/`XREVRANGE` reply.
fn parse_range(v: Value) -> Result<Vec<RangeEntry>> {
    let bad = || be("malformed stream range reply");
    let Value::Array(entries) = v else { return Err(bad()) };
    let mut out = Vec::with_capacity(entries.len());
    for e in entries {
        let Value::Array(parts) = e else { return Err(bad()) };
        let mut parts = parts.into_iter();
        let (Some(id), Some(Value::Array(flat))) = (parts.next(), parts.next()) else { return Err(bad()) };
        let id = text(&id).ok_or_else(bad)?;
        let mut fields = HashMap::new();
        for pair in flat.chunks(2) {
            let [k, v] = pair else { return Err(bad()) };
            fields.insert(text(k).ok_or_else(bad)?, bulk(v).ok_or_else(bad)?);
        }
        out.push((id, fields));
    }
    Ok(out)
}

pub struct RedisBroker {
    conn: ConnectionManager,
    /// Dedicated connection: a blocking XREADGROUP would stall other commands pipelined behind it.
    fetch_conn: ConnectionManager,
    consumer: String,
    scripts: Scripts,
}

impl RedisBroker {
    pub async fn connect(url: &str, consumer: impl Into<String>) -> Result<Self> {
        let client = redis::Client::open(url).map_err(be)?;
        let conn = manager(&client, COMMAND_TIMEOUT).await?;
        // Dedicated: a blocking XREADGROUP would stall commands pipelined behind it.
        let fetch_conn = manager(&client, Duration::from_millis(MAX_BLOCK_MS) + COMMAND_TIMEOUT).await?;
        Ok(RedisBroker {
            conn,
            fetch_conn,
            consumer: consumer.into(),
            scripts: Scripts {
                ack: Script::new(include_str!("lua/ack.lua")),
                retry: Script::new(include_str!("lua/retry.lua")),
                dead: Script::new(include_str!("lua/dead.lua")),
                requeue: Script::new(include_str!("lua/requeue.lua")),
                promote: Script::new(include_str!("lua/promote.lua")),
                extend: Script::new(include_str!("lua/extend.lua")),
                reclaim: Script::new(include_str!("lua/reclaim.lua")),
                prune: Script::new(include_str!("lua/prune.lua")),
                fetch: Script::new(include_str!("lua/fetch.lua")),
                replay: Script::new(include_str!("lua/replay.lua")),
                submit: Script::new(include_str!("lua/submit.lua")),
                release: Script::new(include_str!("lua/release.lua")),
                cancel: Script::new(include_str!("lua/cancel.lua")),
            },
        })
    }

    async fn dead_raw(&self, queue: &str, stream_id: &str, h: &[u8], p: &[u8], error: &str,
                      result: Option<StoredResult>) -> Result<bool> {
        let (flag, id, record, ttl) = match result {
            Some(r) => ("1", r.id, r.record, r.ttl_secs),
            None => ("0", String::new(), Vec::new(), 0),
        };
        // A workflow node is marked dead in the same script; an undecodable header has none.
        let header = decode_header(h).ok();
        let node = header.as_ref().and_then(|h| h.workflow_id.as_deref().zip(h.node.as_deref()));
        let mut inv = self.scripts.dead.prepare_invoke();
        inv.key(stream_key(queue)).key(dead_key(queue)).key(result_key(&id));
        if let Some((wf, _)) = node { inv.key(workflow_key(wf)); }
        inv.arg(GROUP).arg(stream_id).arg(h).arg(p).arg(error).arg(flag).arg(record).arg(ttl);
        if let Some((_, n)) = node { inv.arg(n); }
        inv.arg(&self.consumer);
        let applied = inv.invoke_async::<i64>(&mut self.conn.clone()).await.map_err(be)?;
        Ok(applied == 1)
    }

    /// Non-blocking ordered claim of up to `count` entries (one round trip).
    async fn fetch_ordered(&self, queues: &[String], count: usize) -> Result<Vec<Delivery>> {
        let mut inv = self.scripts.fetch.prepare_invoke();
        for q in queues { inv.key(stream_key(q)); }
        inv.arg(GROUP).arg(&self.consumer).arg(count);
        let v: Value = match inv.invoke_async(&mut self.conn.clone()).await {
            Ok(v) => v,
            Err(e) if is_nogroup(&e) => {
                self.ensure_queues(queues).await?;
                return Ok(Vec::new());
            }
            Err(e) => return Err(be(e)),
        };
        let (claimed, missing) = parse_fetch(v)?;
        let mut out = Vec::new();
        for (i, id, mut fields) in claimed {
            let queue = queues.get(i).ok_or_else(|| be("fetch reply names an unknown queue"))?;
            let h = fields.remove("h");
            let p = fields.remove("p").unwrap_or_default();
            if let Some(d) = self.decode_or_bury(queue, id, h, p, 1).await? {
                out.push(d);
            }
        }
        if !missing.is_empty() {
            let gone = missing.iter().filter_map(|&i| queues.get(i).cloned()).collect::<Vec<_>>();
            self.ensure_queues(&gone).await?;
        }
        Ok(out)
    }

    /// Blocks on every stream until one has entries; `per_stream` caps each stream.
    async fn fetch_blocking(&self, queues: &[String], per_stream: usize, block_ms: u64) -> Result<Vec<Delivery>> {
        let mut cmd = redis::cmd("XREADGROUP");
        cmd.arg("GROUP").arg(GROUP).arg(&self.consumer)
            .arg("COUNT").arg(per_stream).arg("BLOCK").arg(block_ms.min(MAX_BLOCK_MS)).arg("STREAMS");
        for q in queues { cmd.arg(stream_key(q)); }
        for _ in queues { cmd.arg(">"); }
        let reply: Option<StreamReadReply> = match cmd.query_async(&mut self.fetch_conn.clone()).await {
            Ok(r) => r,
            Err(e) if is_nogroup(&e) => {
                self.ensure_queues(queues).await?;
                return Ok(Vec::new());
            }
            Err(e) => return Err(be(e)),
        };
        let mut out = Vec::new();
        for key in reply.map(|r| r.keys).unwrap_or_default() {
            let queue = queue_from_stream_key(&key.key).to_string();
            for entry in key.ids {
                let mut map = entry.map;
                let h = take_bytes(&mut map, "h");
                let p = take_bytes(&mut map, "p").unwrap_or_default();
                if let Some(d) = self.decode_or_bury(&queue, entry.id, h, p, 1).await? {
                    out.push(d);
                }
            }
        }
        Ok(out)
    }

    /// A stream entry as a `Delivery`, or dead-lettered if its header cannot be decoded.
    async fn decode_or_bury(&self, queue: &str, stream_id: String, h: Option<Vec<u8>>, p: Vec<u8>,
                            delivery_count: u64) -> Result<Option<Delivery>> {
        let decoded = h.as_deref().map(decode_header);
        match (h, decoded) {
            (Some(raw_header), Some(Ok(header))) => Ok(Some(Delivery {
                queue: queue.to_string(), stream_id, header, raw_header, payload: p, delivery_count,
            })),
            (h, _) => {
                // Just claimed by this consumer, so the settle is never stale.
                self.dead_raw(queue, &stream_id, &h.unwrap_or_default(), &p, "undecodable envelope header", None)
                    .await?;
                Ok(None)
            }
        }
    }

    pub async fn worker_info(&self, worker_id: &str) -> Result<Option<WorkerInfo>> {
        let map: HashMap<String, String> = redis::cmd("HGETALL").arg(worker_key(worker_id))
            .query_async(&mut self.conn.clone()).await.map_err(be)?;
        if map.is_empty() {
            return Ok(None);
        }
        let num = |k: &str| map.get(k).and_then(|v| v.parse::<u64>().ok()).unwrap_or(0);
        Ok(Some(WorkerInfo {
            worker_id: worker_id.to_string(),
            hostname: map.get("hostname").cloned().unwrap_or_default(),
            pid: num("pid") as u32,
            queues: map.get("queues")
                .map(|q| q.split(',').filter(|s| !s.is_empty()).map(String::from).collect())
                .unwrap_or_default(),
            concurrency: num("concurrency") as usize,
            inflight: num("inflight") as usize,
            started_at: num("started_at"),
            last_seen: num("last_seen"),
            tasks: map.get("tasks").map(|t| parse_tasks(worker_id, t)).unwrap_or_default(),
        }))
    }

    /// Dead letters of `queue`, oldest first: strictly after `after`, up to and including `until`.
    pub async fn dead_letters(&self, queue: &str, after: Option<&str>, until: Option<&str>, count: usize)
        -> Result<Vec<DeadLetter>>
    {
        let start = after.map_or_else(|| "-".to_string(), |id| format!("({id}"));
        let v: Value = redis::cmd("XRANGE").arg(dead_key(queue)).arg(start).arg(until.unwrap_or("+"))
            .arg("COUNT").arg(count.max(1))
            .query_async(&mut self.conn.clone()).await.map_err(be)?;
        Ok(parse_range(v)?.into_iter().map(|(entry_id, mut f)| {
            let raw_header = f.remove("h").unwrap_or_default();
            DeadLetter {
                entry_id,
                header: decode_header(&raw_header).ok(),
                raw_header,
                payload: f.remove("p").unwrap_or_default(),
                error: String::from_utf8_lossy(&f.remove("e").unwrap_or_default()).into_owned(),
            }
        }).collect())
    }

    /// Id of the newest dead letter of `queue`.
    pub async fn last_dead_id(&self, queue: &str) -> Result<Option<String>> {
        let v: Value = redis::cmd("XREVRANGE").arg(dead_key(queue)).arg("+").arg("-").arg("COUNT").arg(1)
            .query_async(&mut self.conn.clone()).await.map_err(be)?;
        Ok(parse_range(v)?.into_iter().next().map(|(id, _)| id))
    }

    /// Moves dead letters back to `queue` to run again (attempt 0, no eta, same task id), deleting
    /// their stored failure results. Missing or undecodable entries are skipped. Returns how many moved.
    pub async fn replay_dead(&self, queue: &str, entry_ids: &[String]) -> Result<usize> {
        let mut picked = Vec::new();
        for id in entry_ids {
            // Exactly this entry (XRANGE id id), not the oldest one up to it.
            let v: Value = redis::cmd("XRANGE").arg(dead_key(queue)).arg(id).arg(id).arg("COUNT").arg(1)
                .query_async(&mut self.conn.clone()).await.map_err(be)?;
            let Some((entry_id, mut f)) = parse_range(v)?.into_iter().next() else { continue };
            let Ok(mut h) = decode_header(&f.remove("h").unwrap_or_default()) else { continue };
            h.attempt = 0;
            h.eta = None;
            picked.push((entry_id, h, f.remove("p").unwrap_or_default()));
        }
        if picked.is_empty() {
            return Ok(0);
        }
        let mut inv = self.scripts.replay.prepare_invoke();
        inv.key(dead_key(queue)).key(stream_key(queue));
        for (_, h, _) in &picked { inv.key(result_key(&h.id)); }
        for (_, h, _) in &picked {
            if let (Some(wf), Some(_)) = (&h.workflow_id, &h.node) { inv.key(workflow_key(wf)); }
        }
        for (id, h, p) in &picked {
            let node = if h.workflow_id.is_some() { h.node.as_deref().unwrap_or("") } else { "" };
            inv.arg(id).arg(encode_header(h)).arg(p).arg(node);
        }
        inv.invoke_async::<usize>(&mut self.conn.clone()).await.map_err(be)
    }

    async fn scan_keys(&self, pattern: &str) -> Result<Vec<String>> {
        let mut c = self.conn.clone();
        let mut cursor: u64 = 0;
        let mut out = Vec::new();
        loop {
            let (next, keys): (u64, Vec<String>) = redis::cmd("SCAN").arg(cursor).arg("MATCH").arg(pattern)
                .arg("COUNT").arg(500).query_async(&mut c).await.map_err(be)?;
            out.extend(keys);
            if next == 0 { break; }
            cursor = next;
        }
        out.sort();
        out.dedup(); // SCAN may return a key more than once
        Ok(out)
    }

    /// Workers with a live heartbeat, sorted by id.
    pub async fn workers(&self) -> Result<Vec<WorkerInfo>> {
        let mut out = Vec::new();
        for key in self.scan_keys(&format!("{WORKER_KEY_PREFIX}*")).await? {
            let id = key.strip_prefix(WORKER_KEY_PREFIX).unwrap_or(&key);
            if let Some(w) = self.worker_info(id).await? { // gone if it expired since the scan
                out.push(w);
            }
        }
        Ok(out)
    }

    /// Workflows that have a hash in Redis, oldest first. Skips ones that expire mid-scan and ones whose hash
    /// is malformed (with a warning), so one bad key can't break the listing.
    pub async fn workflows(&self) -> Result<Vec<WorkflowState>> {
        let mut out = Vec::new();
        for key in self.scan_keys(&format!("{WORKFLOW_KEY_PREFIX}*")).await? {
            let id = key.strip_prefix(WORKFLOW_KEY_PREFIX).unwrap_or(&key);
            match self.read_workflow(id).await? {
                Some(Ok(w)) => out.push(w),
                Some(Err(e)) => tracing::warn!(key = %key, error = %e, "skipping a malformed workflow hash"),
                None => {}
            }
        }
        out.sort_by(|a, b| (a.created_at, &a.id).cmp(&(b.created_at, &b.id)));
        Ok(out)
    }

    /// Reads one workflow. The outer error is a Redis failure; the inner one means the hash is malformed.
    async fn read_workflow(&self, id: &str) -> Result<Option<Result<WorkflowState>>> {
        let key = workflow_key(id);
        let mut c = self.conn.clone();
        let meta: Vec<Option<String>> = redis::cmd("HMGET").arg(&key)
            .arg("nodes").arg("name").arg("created_at").arg("status")
            .query_async(&mut c).await.map_err(be)?;
        if meta.iter().all(Option::is_none) {
            return Ok(None);
        }
        let (count, name, created_at, status) = match parse_workflow_meta(id, meta) {
            Ok(m) => m,
            Err(e) => return Ok(Some(Err(e))),
        };
        let mut cmd = redis::cmd("HMGET");
        cmd.arg(&key);
        for i in 0..count {
            cmd.arg(format!("n:{i}:id")).arg(format!("n:{i}:t")).arg(format!("n:{i}:q")).arg(format!("n:{i}:s"))
                .arg(format!("n:{i}:up"));
        }
        let vals: Vec<Option<String>> = cmd.query_async(&mut c).await.map_err(be)?;
        if vals.len() == count * 5 && vals.iter().all(Option::is_none) {
            return Ok(None); // expired between the two reads
        }
        Ok(Some(parse_workflow_nodes(id, name, created_at, status == "cancelled", count, &vals)))
    }

    /// Queues that have a stream, delayed set or dead stream in Redis, sorted.
    pub async fn known_queues(&self) -> Result<Vec<String>> {
        let mut qs = Vec::new();
        for (suffix, pattern) in [("}:stream", "mayu:{*}:stream"), ("}:delayed", "mayu:{*}:delayed"), ("}:dead", "mayu:{*}:dead")] {
            for k in self.scan_keys(pattern).await? {
                if let Some(q) = k.strip_prefix("mayu:{").and_then(|s| s.strip_suffix(suffix)) {
                    qs.push(q.to_string());
                }
            }
        }
        qs.sort();
        qs.dedup();
        Ok(qs)
    }
}

fn take_bytes(map: &mut HashMap<String, Value>, k: &str) -> Option<Vec<u8>> {
    match map.remove(k)? {
        Value::BulkString(b) => Some(b),
        _ => None,
    }
}

fn bulk(v: &Value) -> Option<Vec<u8>> {
    match v {
        Value::BulkString(b) => Some(b.clone()),
        Value::SimpleString(s) => Some(s.clone().into_bytes()),
        _ => None,
    }
}

fn text(v: &Value) -> Option<String> {
    bulk(v).map(|b| String::from_utf8_lossy(&b).into_owned())
}

struct RawEntry {
    id: String,
    fields: HashMap<String, Vec<u8>>,
    delivery_count: u64,
}

fn parse_reclaim(v: Value) -> Result<(String, Vec<RawEntry>)> {
    let Value::Array(top) = v else { return Err(be("unexpected reclaim reply")) };
    let mut top = top.into_iter();
    let cursor = top.next().as_ref().and_then(text).ok_or_else(|| be("reclaim reply missing cursor"))?;
    let entries = match top.next() {
        Some(Value::Array(e)) => e,
        _ => Vec::new(),
    };
    let mut out = Vec::new();
    for e in entries {
        let Value::Array(parts) = e else { continue };
        let mut parts = parts.into_iter();
        let (Some(id), Some(Value::Array(flat)), Some(count)) = (parts.next(), parts.next(), parts.next()) else {
            continue;
        };
        let Some(id) = text(&id) else { continue };
        let delivery_count = match count { Value::Int(n) => n.max(1) as u64, _ => 1 };
        let mut fields = HashMap::new();
        for pair in flat.chunks(2) {
            if let [k, v] = pair {
                if let (Some(k), Some(v)) = (text(k), bulk(v)) {
                    fields.insert(k, v);
                }
            }
        }
        out.push(RawEntry { id, fields, delivery_count });
    }
    Ok((cursor, out))
}

fn is_nogroup(e: &redis::RedisError) -> bool {
    e.code() == Some("NOGROUP") || e.to_string().contains("NOGROUP")
}

/// A claimed entry: (queue index, entry id, fields).
type Claimed = (usize, String, HashMap<String, Vec<u8>>);

/// `fetch.lua` reply `{claimed, missing}` -> (claimed entries, indexes of streams without the group).
///
/// Every element of `claimed` is already claimed in Redis, so a malformed one is an error,
/// never silently skipped.
fn parse_fetch(v: Value) -> Result<(Vec<Claimed>, Vec<usize>)> {
    let bad = || be("malformed fetch reply");
    let Value::Array(top) = v else { return Err(bad()) };
    let mut top = top.into_iter();
    let (Some(Value::Array(entries)), Some(Value::Array(missing))) = (top.next(), top.next()) else {
        return Err(bad());
    };
    let index = |v: Value| match v {
        Value::Int(i) if i >= 1 => Ok(i as usize - 1),
        _ => Err(bad()),
    };
    let mut out = Vec::new();
    for e in entries {
        let Value::Array(parts) = e else { return Err(bad()) };
        let mut parts = parts.into_iter();
        let (Some(i), Some(id), Some(Value::Array(flat))) = (parts.next(), parts.next(), parts.next())
        else { return Err(bad()) };
        let i = index(i)?;
        let id = text(&id).ok_or_else(bad)?;
        let mut fields = HashMap::new();
        for pair in flat.chunks(2) {
            let [k, v] = pair else { return Err(bad()) };
            fields.insert(text(k).ok_or_else(bad)?, bulk(v).ok_or_else(bad)?);
        }
        out.push((i, id, fields));
    }
    let missing = missing.into_iter().map(index).collect::<Result<Vec<_>>>()?;
    Ok((out, missing))
}

impl Broker for RedisBroker {
    async fn ensure_queues(&self, queues: &[String]) -> Result<()> {
        let mut c = self.conn.clone();
        for q in queues {
            // Start at 0, not $, so messages enqueued before any worker existed are delivered.
            let r: redis::RedisResult<()> = redis::cmd("XGROUP").arg("CREATE").arg(stream_key(q))
                .arg(GROUP).arg("0").arg("MKSTREAM").query_async(&mut c).await;
            match r {
                Ok(()) => {}
                Err(e) if e.code() == Some("BUSYGROUP") => {}
                Err(e) => return Err(be(e)),
            }
        }
        Ok(())
    }

    async fn enqueue(&self, header: &Header, payload: &[u8]) -> Result<()> {
        let h = encode_header(header);
        let mut c = self.conn.clone();
        match header.eta {
            Some(eta) if eta > now_ms() => {
                redis::cmd("ZADD").arg(delayed_key(&header.queue)).arg(eta)
                    .arg(encode_delayed_member(&h, payload))
                    .query_async::<i64>(&mut c).await.map_err(be)?;
            }
            _ => {
                redis::cmd("XADD").arg(stream_key(&header.queue)).arg("*")
                    .arg("h").arg(&h).arg("p").arg(payload)
                    .query_async::<String>(&mut c).await.map_err(be)?;
            }
        }
        Ok(())
    }

    async fn fetch(&self, queues: &[String], count: usize, block_ms: u64) -> Result<Vec<Delivery>> {
        if count == 0 {
            return Ok(Vec::new());
        }
        let got = self.fetch_ordered(queues, count).await?;
        if !got.is_empty() || block_ms == 0 || queues.is_empty() {
            return Ok(got);
        }
        // COUNT is per stream: spread `count` so a multi-queue wake-up stays near it.
        self.fetch_blocking(queues, (count / queues.len()).max(1), block_ms).await
    }

    async fn ack(&self, d: &Delivery, result: Option<StoredResult>, follow_ups: &[(Header, Vec<u8>)]) -> Result<bool> {
        let (flag, id, record, ttl) = match result {
            Some(r) => ("1", r.id, r.record, r.ttl_secs),
            None => ("0", String::new(), Vec::new(), 0),
        };
        if let Some(wf) = &d.header.workflow_id {
            if !follow_ups.is_empty() {
                return Err(be("a workflow node can't have link follow-ups"));
            }
            let node = d.header.node.as_deref().and_then(|n| n.parse::<usize>().ok()).ok_or_else(|| {
                be(format!("task {} has a workflow id but no valid node", d.header.id))
            })?;
            let released = self.scripts.release
                .key(stream_key(&d.queue)).key(result_key(&id)).key(workflow_key(wf))
                .arg(GROUP).arg(&d.stream_id).arg(flag).arg(record).arg(ttl).arg(node).arg(&self.consumer)
                .invoke_async::<i64>(&mut self.conn.clone()).await.map_err(be)?;
            if released == -2 {
                return Ok(false);
            }
            if released < 0 {
                tracing::warn!(
                    task_id = %d.header.id, workflow_id = %wf,
                    "workflow state is gone (expired?); acked without releasing its children"
                );
            }
            return Ok(true);
        }
        let mut inv = self.scripts.ack.prepare_invoke();
        inv.key(stream_key(&d.queue)).key(result_key(&id));
        for (h, _) in follow_ups { inv.key(stream_key(&h.queue)); }
        inv.arg(GROUP).arg(&d.stream_id).arg(flag).arg(record).arg(ttl);
        for (h, p) in follow_ups { inv.arg(encode_header(h)).arg(p); }
        inv.arg(&self.consumer);
        let applied = inv.invoke_async::<i64>(&mut self.conn.clone()).await.map_err(be)?;
        Ok(applied == 1)
    }

    async fn retry(&self, d: &Delivery, header: &Header) -> Result<bool> {
        let member = encode_delayed_member(&encode_header(header), &d.payload);
        self.scripts.retry
            .key(stream_key(&d.queue)).key(delayed_key(&d.queue))
            .arg(GROUP).arg(&d.stream_id).arg(header.eta.unwrap_or(0)).arg(member).arg(&self.consumer)
            .invoke_async::<i64>(&mut self.conn.clone()).await.map_err(be).map(|applied| applied == 1)
    }

    async fn dead_letter(&self, d: &Delivery, error: &str, result: Option<StoredResult>) -> Result<bool> {
        self.dead_raw(&d.queue, &d.stream_id, &d.raw_header, &d.payload, error, result).await
    }

    async fn requeue(&self, d: &Delivery) -> Result<bool> {
        self.scripts.requeue
            .key(stream_key(&d.queue))
            .arg(GROUP).arg(&d.stream_id).arg(&d.raw_header).arg(&d.payload).arg(&self.consumer)
            .invoke_async::<i64>(&mut self.conn.clone()).await.map_err(be).map(|applied| applied == 1)
    }

    async fn promote(&self, queue: &str, now_ms: u64, limit: usize) -> Result<usize> {
        self.scripts.promote
            .key(delayed_key(queue)).key(stream_key(queue))
            .arg(now_ms).arg(limit)
            .invoke_async::<usize>(&mut self.conn.clone()).await.map_err(be)
    }

    async fn get_result(&self, id: &str) -> Result<Option<Vec<u8>>> {
        redis::cmd("GET").arg(result_key(id))
            .query_async(&mut self.conn.clone()).await.map_err(be)
    }

    async fn queue_stats(&self, queue: &str) -> Result<QueueStats> {
        let mut c = self.conn.clone();
        let stream: u64 = redis::cmd("XLEN").arg(stream_key(queue)).query_async(&mut c).await.map_err(be)?;
        let delayed: u64 = redis::cmd("ZCARD").arg(delayed_key(queue)).query_async(&mut c).await.map_err(be)?;
        let dead: u64 = redis::cmd("XLEN").arg(dead_key(queue)).query_async(&mut c).await.map_err(be)?;
        let pending = match redis::cmd("XPENDING").arg(stream_key(queue)).arg(GROUP)
            .query_async::<Value>(&mut c).await
        {
            Ok(Value::Array(v)) => match v.first() { Some(Value::Int(n)) => *n as u64, _ => 0 },
            _ => 0, // no group yet
        };
        Ok(QueueStats { stream, pending, delayed, dead })
    }

    async fn extend(&self, queue: &str, stream_ids: &[String]) -> Result<Vec<String>> {
        if stream_ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut inv = self.scripts.extend.prepare_invoke();
        inv.key(stream_key(queue)).arg(GROUP).arg(&self.consumer);
        for id in stream_ids {
            inv.arg(id);
        }
        inv.invoke_async(&mut self.conn.clone()).await.map_err(be)
    }

    async fn reclaim(&self, queue: &str, min_idle_ms: u64, cursor: &str, count: usize) -> Result<Reclaimed> {
        let v: Value = self.scripts.reclaim
            .key(stream_key(queue))
            .arg(GROUP).arg(&self.consumer).arg(min_idle_ms).arg(cursor).arg(count.max(1))
            .invoke_async(&mut self.conn.clone()).await.map_err(be)?;
        let (next_cursor, entries) = parse_reclaim(v)?;
        let mut deliveries = Vec::new();
        for mut e in entries {
            let h = e.fields.remove("h");
            let p = e.fields.remove("p").unwrap_or_default();
            if let Some(d) = self.decode_or_bury(queue, e.id, h, p, e.delivery_count).await? {
                deliveries.push(d);
            }
        }
        Ok(Reclaimed { next_cursor, deliveries })
    }

    async fn heartbeat(&self, info: &WorkerInfo, ttl_ms: u64) -> Result<()> {
        let key = worker_key(&info.worker_id);
        redis::pipe()
            .atomic()
            .cmd("HSET").arg(&key)
                .arg("hostname").arg(&info.hostname)
                .arg("pid").arg(info.pid)
                .arg("queues").arg(info.queues.join(","))
                .arg("concurrency").arg(info.concurrency)
                .arg("inflight").arg(info.inflight)
                .arg("started_at").arg(info.started_at)
                .arg("last_seen").arg(info.last_seen)
                .ignore()
            .add_command(tasks_cmd(&key, &info.tasks)).ignore()
            .cmd("PEXPIRE").arg(&key).arg(ttl_ms).ignore()
            .query_async::<()>(&mut self.conn.clone()).await.map_err(be)
    }

    async fn running(&self, worker_id: &str) -> Result<Vec<RunningTask>> {
        let raw: Option<String> = redis::cmd("HGET").arg(worker_key(worker_id)).arg("tasks")
            .query_async(&mut self.conn.clone()).await.map_err(be)?;
        Ok(raw.map(|t| parse_tasks(worker_id, &t)).unwrap_or_default())
    }

    async fn running_all(&self) -> Result<PublishedTasks> {
        let mut out = PublishedTasks::default();
        for key in self.scan_keys(&format!("{WORKER_KEY_PREFIX}*")).await? {
            let id = key.strip_prefix(WORKER_KEY_PREFIX).unwrap_or(&key);
            let raw: Option<String> = redis::cmd("HGET").arg(&key).arg("tasks")
                .query_async(&mut self.conn.clone()).await.map_err(be)?;
            let Some(raw) = raw else { continue }; // idle, expired since the scan, or an older version
            match decode_tasks(&raw) {
                Ok(tasks) => out.tasks.extend(tasks.into_iter().map(|t| (id.to_string(), t))),
                Err(e) => {
                    tracing::warn!(worker = %id, error = %e, "ignoring a malformed tasks field in a worker heartbeat");
                    out.malformed += 1;
                }
            }
        }
        Ok(out)
    }

    async fn unregister(&self, worker_id: &str) -> Result<()> {
        redis::cmd("DEL").arg(worker_key(worker_id))
            .query_async::<i64>(&mut self.conn.clone()).await.map_err(be)?;
        Ok(())
    }

    async fn prune_consumers(&self, queue: &str, min_idle_ms: u64) -> Result<usize> {
        self.scripts.prune
            .key(stream_key(queue))
            .arg(GROUP).arg(min_idle_ms).arg(WORKER_KEY_PREFIX)
            .invoke_async(&mut self.conn.clone()).await.map_err(be)
    }

    async fn submit_workflow(&self, wf: &WorkflowSpec) -> Result<()> {
        wf.validate()?;
        let children = wf.children();
        let mut fields: Vec<(String, Vec<u8>)> = vec![
            ("name".into(), wf.name.clone().into_bytes()),
            ("created_at".into(), now_ms().to_string().into_bytes()),
            ("ttl".into(), wf.ttl_secs.to_string().into_bytes()),
            ("status".into(), b"running".to_vec()),
            ("nodes".into(), wf.nodes.len().to_string().into_bytes()),
            ("done".into(), b"0".to_vec()),
        ];
        let list = |ids: &mut dyn Iterator<Item = usize>| {
            ids.map(|i| i.to_string()).collect::<Vec<_>>().join(",").into_bytes()
        };
        let mut roots = Vec::new();
        for (i, (n, down)) in wf.nodes.iter().zip(&children).enumerate() {
            let root = n.parents.is_empty();
            let h = encode_header(&n.header);
            fields.push((format!("n:{i}:id"), n.header.id.clone().into_bytes()));
            fields.push((format!("n:{i}:t"), n.header.task.clone().into_bytes()));
            fields.push((format!("n:{i}:q"), n.header.queue.clone().into_bytes()));
            fields.push((format!("n:{i}:s"), if root { b"queued".to_vec() } else { b"waiting".to_vec() }));
            fields.push((format!("n:{i}:up"), list(&mut n.parents.iter().copied())));
            fields.push((format!("n:{i}:down"), list(&mut down.iter().copied())));
            if root {
                roots.push((stream_key(&n.header.queue), h, &n.payload));
            } else {
                fields.push((format!("n:{i}:h"), h));
                fields.push((format!("n:{i}:b"), n.payload.clone()));
            }
        }
        let mut inv = self.scripts.submit.prepare_invoke();
        inv.key(workflow_key(&wf.id));
        for (k, _, _) in &roots { inv.key(k); }
        inv.arg(wf.ttl_secs).arg(fields.len());
        for (k, v) in &fields { inv.arg(k).arg(v); }
        for (_, h, p) in &roots { inv.arg(h).arg(*p); }
        let created: i64 = inv.invoke_async(&mut self.conn.clone()).await.map_err(be)?;
        if created == 0 {
            return Err(Error::Broker(format!("workflow {} already exists", wf.id)));
        }
        Ok(())
    }

    async fn workflow(&self, id: &str) -> Result<Option<WorkflowState>> {
        self.read_workflow(id).await?.transpose()
    }

    async fn cancel_workflow(&self, id: &str) -> Result<bool> {
        let r: i64 = self.scripts.cancel.key(workflow_key(id))
            .invoke_async(&mut self.conn.clone()).await.map_err(be)?;
        Ok(r == 1)
    }

    async fn cancelled_workflows(&self, ids: &[String]) -> Result<Vec<String>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut pipe = redis::pipe();
        for id in ids { pipe.cmd("HGET").arg(workflow_key(id)).arg("status"); }
        let statuses: Vec<Option<String>> = pipe.query_async(&mut self.conn.clone()).await.map_err(be)?;
        Ok(ids.iter().zip(statuses).filter(|(_, s)| s.as_deref() == Some("cancelled")).map(|(id, _)| id.clone()).collect())
    }
}
