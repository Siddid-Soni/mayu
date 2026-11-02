use mayu_core::{
    Broker, Delivery, Error, Header, NodeState, StoredResult, WorkflowNode, WorkflowSpec, WorkflowState, decode_header,
    encode_header,
};
use mayu_redis::{RedisBroker, stream_key, workflow_key};
use NodeState::{Cancelled, Dead, Done, Queued, Waiting};

fn url() -> String {
    std::env::var("MAYU_TEST_REDIS").unwrap_or_else(|_| "redis://127.0.0.1:6390/0".into())
}

fn uniq(prefix: &str) -> String { format!("{prefix}-{}", ulid::Ulid::generate()) }

async fn setup() -> (RedisBroker, String) {
    let q = uniq("q");
    let b = RedisBroker::connect(&url(), uniq("c")).await.expect("is Redis running? scripts/redis-up.sh");
    b.ensure_queues(std::slice::from_ref(&q)).await.unwrap();
    (b, q)
}

async fn raw() -> redis::aio::MultiplexedConnection {
    redis::Client::open(url()).unwrap().get_multiplexed_async_connection().await.unwrap()
}

fn node(task: &str, q: &str, parents: &[usize]) -> WorkflowNode {
    WorkflowNode {
        header: Header::new(task, q, 0, "ct"),
        payload: task.as_bytes().to_vec(),
        parents: parents.to_vec(),
    }
}

/// A -> (B, C) -> D, all on `q`.
fn diamond(q: &str) -> WorkflowSpec {
    WorkflowSpec::new("diamond", 3600, vec![
        node("a", q, &[]), node("b", q, &[0]), node("c", q, &[0]), node("d", q, &[1, 2]),
    ])
}

async fn fetch_all(b: &RedisBroker, q: &str) -> Vec<Delivery> {
    b.fetch(&[q.to_string()], 10, 50).await.unwrap()
}

fn states(s: &WorkflowState) -> Vec<NodeState> {
    s.nodes.iter().map(|n| n.state).collect()
}

fn tasks(ds: &[Delivery]) -> Vec<&str> {
    let mut t: Vec<&str> = ds.iter().map(|d| d.header.task.as_str()).collect();
    t.sort();
    t
}

fn by_task<'a>(ds: &'a [Delivery], task: &str) -> &'a Delivery {
    ds.iter().find(|d| d.header.task == task).unwrap()
}

/// Held by tests that corrupt a hash or scan every workflow, so one doesn't trip over the other's.
static SCAN: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn state_of(b: &RedisBroker, wf: &WorkflowSpec) -> WorkflowState {
    b.workflow(&wf.id).await.unwrap().unwrap()
}

#[tokio::test]
async fn diamond_runs_in_dependency_order() {
    let (b, q) = setup().await;
    let wf = diamond(&q);
    b.submit_workflow(&wf).await.unwrap();

    let s = state_of(&b, &wf).await;
    assert_eq!((s.id.as_str(), s.name.as_str(), s.cancelled), (wf.id.as_str(), "diamond", false));
    assert!(s.created_at > 0);
    assert_eq!(states(&s), vec![Queued, Waiting, Waiting, Waiting]);
    assert_eq!(
        s.nodes.iter().map(|n| (n.node.as_str(), n.task.as_str(), n.queue.as_str())).collect::<Vec<_>>(),
        vec![("0", "a", q.as_str()), ("1", "b", q.as_str()), ("2", "c", q.as_str()), ("3", "d", q.as_str())]
    );
    for (n, spec) in s.nodes.iter().zip(&wf.nodes) {
        assert_eq!(n.task_id, spec.header.id);
    }
    let sum = s.summary();
    assert_eq!((sum.state, sum.waiting, sum.queued, sum.done, sum.dead, sum.cancelled), ("running", 3, 1, 0, 0, 0));

    let ds = fetch_all(&b, &q).await;
    assert_eq!(tasks(&ds), vec!["a"]);
    assert_eq!(ds[0].payload, b"a");
    b.ack(&ds[0], None, &[]).await.unwrap();
    assert_eq!(states(&state_of(&b, &wf).await), vec![Done, Queued, Queued, Waiting]);

    let ds = fetch_all(&b, &q).await;
    assert_eq!(tasks(&ds), vec!["b", "c"]);
    let (db, dc) = (by_task(&ds, "b"), by_task(&ds, "c"));
    assert_eq!(db.payload, b"b");
    assert_eq!(db.header.id, wf.nodes[1].header.id, "released with the id the spec promised");
    assert_eq!(db.header.workflow_id.as_deref(), Some(wf.id.as_str()));
    assert_eq!(db.header.node.as_deref(), Some("1"));

    b.ack(db, None, &[]).await.unwrap();
    assert!(fetch_all(&b, &q).await.is_empty(), "D waits for C");
    assert_eq!(states(&state_of(&b, &wf).await), vec![Done, Done, Queued, Waiting]);

    b.ack(dc, None, &[]).await.unwrap();
    assert_eq!(states(&state_of(&b, &wf).await), vec![Done, Done, Done, Queued]);
    let ds = fetch_all(&b, &q).await;
    assert_eq!(tasks(&ds), vec!["d"]);
    b.ack(&ds[0], None, &[]).await.unwrap();

    let s = state_of(&b, &wf).await;
    assert_eq!(states(&s), vec![Done; 4]);
    let sum = s.summary();
    assert_eq!((sum.state, sum.waiting, sum.queued, sum.done), ("done", 0, 0, 4));
    let stats = b.queue_stats(&q).await.unwrap();
    assert_eq!((stats.stream, stats.pending), (0, 0));
}

#[tokio::test]
async fn ack_stores_the_result_and_release_still_happens() {
    let (b, q) = setup().await;
    let wf = diamond(&q);
    b.submit_workflow(&wf).await.unwrap();
    let d = fetch_all(&b, &q).await.remove(0);
    let r = StoredResult { id: d.header.id.clone(), record: b"r".to_vec(), ttl_secs: 9 };
    b.ack(&d, Some(r), &[]).await.unwrap();
    assert_eq!(b.get_result(&d.header.id).await.unwrap(), Some(b"r".to_vec()));
    assert_eq!(b.queue_stats(&q).await.unwrap().stream, 2);
}

#[tokio::test]
async fn duplicate_acks_and_duplicate_runs_release_once() {
    let (b, q) = setup().await;
    let wf = diamond(&q);
    b.submit_workflow(&wf).await.unwrap();
    let a = fetch_all(&b, &q).await.remove(0);
    // A second delivery of A, as after a visibility-timeout reclaim of a task that did finish.
    b.enqueue(&a.header, &a.payload).await.unwrap();
    let a2 = fetch_all(&b, &q).await.remove(0);

    b.ack(&a, None, &[]).await.unwrap();
    b.ack(&a, None, &[]).await.unwrap(); // a retried ack
    b.ack(&a2, None, &[]).await.unwrap(); // the duplicate run's ack

    assert_eq!(b.queue_stats(&q).await.unwrap().stream, 2, "B and C were enqueued once each");
    let ds = fetch_all(&b, &q).await;
    assert_eq!(tasks(&ds), vec!["b", "c"]);
    // Both ack, then re-ack B: D is released exactly once.
    for d in &ds {
        b.ack(d, None, &[]).await.unwrap();
    }
    b.ack(&ds[0], None, &[]).await.unwrap();
    assert_eq!(tasks(&fetch_all(&b, &q).await), vec!["d"]);
    assert!(fetch_all(&b, &q).await.is_empty());
}

#[tokio::test]
async fn dead_letter_blocks_descendants() {
    let (b, q) = setup().await;
    let wf = diamond(&q);
    b.submit_workflow(&wf).await.unwrap();
    let a = fetch_all(&b, &q).await.remove(0);
    b.ack(&a, None, &[]).await.unwrap();
    let ds = fetch_all(&b, &q).await;

    b.dead_letter(by_task(&ds, "b"), "boom", None).await.unwrap();
    b.ack(by_task(&ds, "c"), None, &[]).await.unwrap();

    let s = state_of(&b, &wf).await;
    assert_eq!(states(&s), vec![Done, Dead, Done, Waiting]);
    let sum = s.summary();
    assert_eq!((sum.state, sum.waiting, sum.dead, sum.done), ("failed", 1, 1, 2));
    assert!(fetch_all(&b, &q).await.is_empty(), "D is never released");
    assert_eq!(b.dead_letters(&q, None, None, 10).await.unwrap().len(), 1);
}

#[tokio::test]
async fn dead_letter_does_not_undo_a_done_node() {
    let (b, q) = setup().await;
    let wf = diamond(&q);
    b.submit_workflow(&wf).await.unwrap();
    let a = fetch_all(&b, &q).await.remove(0);
    b.enqueue(&a.header, &a.payload).await.unwrap();
    let a2 = fetch_all(&b, &q).await.remove(0);
    b.ack(&a, None, &[]).await.unwrap();
    b.dead_letter(&a2, "late duplicate failed", None).await.unwrap();
    assert_eq!(states(&state_of(&b, &wf).await)[0], Done);
}

#[tokio::test]
async fn cancel_stops_the_rest() {
    let (b, q) = setup().await;
    let wf = diamond(&q);
    b.submit_workflow(&wf).await.unwrap();
    let a = fetch_all(&b, &q).await.remove(0);
    b.ack(&a, None, &[]).await.unwrap();
    let ds = fetch_all(&b, &q).await; // B and C are claimed

    assert!(b.cancel_workflow(&wf.id).await.unwrap());
    let s = state_of(&b, &wf).await;
    assert!(s.cancelled);
    assert_eq!(s.summary().state, "cancelled");

    for d in &ds {
        b.ack(d, None, &[]).await.unwrap();
    }
    let s = state_of(&b, &wf).await;
    assert_eq!(states(&s), vec![Done, Cancelled, Cancelled, Waiting]);
    let sum = s.summary();
    assert_eq!((sum.state, sum.cancelled, sum.done, sum.waiting), ("cancelled", 2, 1, 1));
    assert!(fetch_all(&b, &q).await.is_empty(), "D is never queued");
    assert_eq!(b.queue_stats(&q).await.unwrap().pending, 0);
}

#[tokio::test]
async fn cancel_is_false_when_unknown_or_all_done() {
    let (b, q) = setup().await;
    assert!(!b.cancel_workflow(&uniq("nope")).await.unwrap());

    let wf = WorkflowSpec::new("one", 60, vec![node("a", &q, &[])]);
    b.submit_workflow(&wf).await.unwrap();
    let a = fetch_all(&b, &q).await.remove(0);
    b.ack(&a, None, &[]).await.unwrap();
    assert!(!b.cancel_workflow(&wf.id).await.unwrap());
    let s = state_of(&b, &wf).await;
    assert!(!s.cancelled);
    assert_eq!(s.summary().state, "done");
}

#[tokio::test]
async fn workflow_is_none_for_an_unknown_id() {
    let (b, _) = setup().await;
    assert!(b.workflow(&uniq("nope")).await.unwrap().is_none());
}

#[tokio::test]
async fn cancelled_workflows_returns_exactly_the_cancelled_ids() {
    let (b, q) = setup().await;
    let (w1, w2, w3) = (diamond(&q), diamond(&q), diamond(&q));
    for w in [&w1, &w2, &w3] {
        b.submit_workflow(w).await.unwrap();
    }
    b.cancel_workflow(&w1.id).await.unwrap();
    b.cancel_workflow(&w3.id).await.unwrap();
    let ids = vec![w1.id.clone(), w2.id.clone(), w3.id.clone(), "unknown".to_string()];
    assert_eq!(b.cancelled_workflows(&ids).await.unwrap(), vec![w1.id.clone(), w3.id.clone()]);
    assert!(b.cancelled_workflows(&[]).await.unwrap().is_empty());
}

#[tokio::test]
async fn ack_of_a_node_with_follow_ups_is_an_error() {
    let (b, q) = setup().await;
    let wf = diamond(&q);
    b.submit_workflow(&wf).await.unwrap();
    let a = fetch_all(&b, &q).await.remove(0);
    let follow = [(Header::new("cb", &q, 0, "ct"), b"x".to_vec())];
    assert!(matches!(b.ack(&a, None, &follow).await, Err(Error::Broker(_))));
    assert_eq!(b.queue_stats(&q).await.unwrap().pending, 1, "nothing was acked");
    assert_eq!(states(&state_of(&b, &wf).await)[0], Queued);
}

#[tokio::test]
async fn ack_of_a_node_missing_from_the_hash_is_an_error_and_acks_nothing() {
    let (b, q) = setup().await;
    let wf = diamond(&q);
    b.submit_workflow(&wf).await.unwrap();
    let mut a = fetch_all(&b, &q).await.remove(0);
    a.header.node = Some("99".into());
    assert!(matches!(b.ack(&a, None, &[]).await, Err(Error::Broker(_))));
    a.header.node = Some("x".into());
    assert!(matches!(b.ack(&a, None, &[]).await, Err(Error::Broker(_))));
    a.header.node = None;
    assert!(matches!(b.ack(&a, None, &[]).await, Err(Error::Broker(_))));
    assert_eq!(b.queue_stats(&q).await.unwrap().pending, 1, "nothing was acked");
    assert_eq!(states(&state_of(&b, &wf).await)[0], Queued);
}

#[tokio::test]
async fn submitting_the_same_workflow_twice_is_an_error() {
    let (b, q) = setup().await;
    let wf = diamond(&q);
    b.submit_workflow(&wf).await.unwrap();
    assert!(matches!(b.submit_workflow(&wf).await, Err(Error::Broker(_))));
    assert_eq!(b.queue_stats(&q).await.unwrap().stream, 1);
}

#[tokio::test]
async fn submit_rejects_an_invalid_spec() {
    let (b, _) = setup().await;
    let wf = WorkflowSpec::new("empty", 60, vec![]);
    assert!(matches!(b.submit_workflow(&wf).await, Err(Error::Config(_))));
}

#[tokio::test]
async fn several_roots_are_all_enqueued_and_comma_lists_may_be_empty() {
    let (b, q) = setup().await;
    let q2 = uniq("q");
    b.ensure_queues(std::slice::from_ref(&q2)).await.unwrap();
    let wf = WorkflowSpec::new("roots", 60, vec![node("a", &q, &[]), node("b", &q2, &[]), node("c", &q, &[0, 1])]);
    b.submit_workflow(&wf).await.unwrap();
    assert_eq!(tasks(&fetch_all(&b, &q).await), vec!["a"]);
    assert_eq!(tasks(&fetch_all(&b, &q2).await), vec!["b"]);
    assert_eq!(states(&state_of(&b, &wf).await), vec![Queued, Queued, Waiting]);
}

#[tokio::test]
async fn a_child_is_released_onto_its_own_queue() {
    let (b, q) = setup().await;
    let q2 = uniq("q");
    b.ensure_queues(std::slice::from_ref(&q2)).await.unwrap();
    let wf = WorkflowSpec::new("two queues", 60, vec![node("a", &q, &[]), node("b", &q2, &[0])]);
    b.submit_workflow(&wf).await.unwrap();
    let a = fetch_all(&b, &q).await.remove(0);
    b.ack(&a, None, &[]).await.unwrap();
    assert!(fetch_all(&b, &q).await.is_empty());
    assert_eq!(tasks(&fetch_all(&b, &q2).await), vec!["b"]);
}

#[tokio::test]
async fn workflows_lists_in_creation_order() {
    let _scan = SCAN.lock().await;
    let (b, q) = setup().await;
    let first = diamond(&q);
    b.submit_workflow(&first).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(5)).await; // created_at has ms resolution
    let second = diamond(&q);
    b.submit_workflow(&second).await.unwrap();
    let all = b.workflows().await.unwrap();
    let pos = |id: &str| all.iter().position(|w| w.id == id).unwrap();
    assert!(pos(&first.id) < pos(&second.id));
    assert!(all.windows(2).all(|w| w[0].created_at <= w[1].created_at));
}

#[tokio::test]
async fn malformed_hash_data_is_a_broker_error() {
    let _scan = SCAN.lock().await;
    let (b, q) = setup().await;
    let wf = diamond(&q);
    b.submit_workflow(&wf).await.unwrap();
    let mut c = raw().await;
    let key = workflow_key(&wf.id);
    let _: i64 = redis::cmd("HSET").arg(&key).arg("n:1:s").arg("bogus").query_async(&mut c).await.unwrap();
    assert!(matches!(b.workflow(&wf.id).await, Err(Error::Broker(_))));
    let _: i64 = redis::cmd("HSET").arg(&key).arg("n:1:s").arg("waiting").query_async(&mut c).await.unwrap();
    let _: i64 = redis::cmd("HDEL").arg(&key).arg("n:2:q").query_async(&mut c).await.unwrap();
    assert!(matches!(b.workflow(&wf.id).await, Err(Error::Broker(_))));
    let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut c).await.unwrap();
}

#[tokio::test]
async fn parents_read_back_and_a_missing_up_field_reads_empty() {
    let _scan = SCAN.lock().await;
    let (b, q) = setup().await;
    let wf = diamond(&q);
    b.submit_workflow(&wf).await.unwrap();
    let parents = |s: &WorkflowState| s.nodes.iter().map(|n| n.parents.clone()).collect::<Vec<_>>();
    assert_eq!(parents(&state_of(&b, &wf).await), vec![vec![], vec![0], vec![0], vec![1, 2]]);
    // A hash written before `up` existed: every node reads as a root.
    let mut c = raw().await;
    let key = workflow_key(&wf.id);
    for i in 0..4 {
        let _: i64 = redis::cmd("HDEL").arg(&key).arg(format!("n:{i}:up")).query_async(&mut c).await.unwrap();
    }
    assert_eq!(parents(&state_of(&b, &wf).await), vec![Vec::<usize>::new(); 4]);
    let _: i64 = redis::cmd("HSET").arg(&key).arg("n:3:up").arg("1,x").query_async(&mut c).await.unwrap();
    // Parents only feed the lookahead estimate: a malformed list reads as none rather than failing the read.
    assert_eq!(state_of(&b, &wf).await.nodes[3].parents, Vec::<usize>::new());
    assert_eq!(state_of(&b, &wf).await.nodes[1].task, wf.nodes[1].header.task);
}

#[tokio::test]
async fn listing_skips_a_malformed_workflow() {
    let _scan = SCAN.lock().await;
    let (b, q) = setup().await;
    let good = diamond(&q);
    let bad = diamond(&q);
    b.submit_workflow(&good).await.unwrap();
    b.submit_workflow(&bad).await.unwrap();
    let mut c = raw().await;
    let key = workflow_key(&bad.id);
    let _: i64 = redis::cmd("HSET").arg(&key).arg("n:1:s").arg("bogus").query_async(&mut c).await.unwrap();
    assert!(matches!(b.workflow(&bad.id).await, Err(Error::Broker(_))));
    let all = b.workflows().await.unwrap();
    assert!(all.iter().any(|w| w.id == good.id));
    assert!(all.iter().all(|w| w.id != bad.id));
    let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut c).await.unwrap();
}

#[tokio::test]
async fn submit_sets_a_ttl_and_a_release_refreshes_it() {
    let (b, q) = setup().await;
    let wf = WorkflowSpec::new("ttl", 1000, vec![node("a", &q, &[]), node("b", &q, &[0])]);
    b.submit_workflow(&wf).await.unwrap();
    let mut c = raw().await;
    let key = workflow_key(&wf.id);
    let ttl: i64 = redis::cmd("TTL").arg(&key).query_async(&mut c).await.unwrap();
    assert!(ttl > 0 && ttl <= 1000, "ttl {ttl}");

    let _: i64 = redis::cmd("EXPIRE").arg(&key).arg(5).query_async(&mut c).await.unwrap();
    let a = fetch_all(&b, &q).await.remove(0);
    b.ack(&a, None, &[]).await.unwrap();
    let ttl: i64 = redis::cmd("TTL").arg(&key).query_async(&mut c).await.unwrap();
    assert!(ttl > 900 && ttl <= 1000, "the release restored the full ttl, got {ttl}");
}

#[tokio::test]
async fn release_after_the_hash_expired_still_acks() {
    let (b, q) = setup().await;
    let wf = diamond(&q);
    b.submit_workflow(&wf).await.unwrap();
    let a = fetch_all(&b, &q).await.remove(0);
    let _: i64 = redis::cmd("DEL").arg(workflow_key(&wf.id)).query_async(&mut raw().await).await.unwrap();

    let r = StoredResult { id: a.header.id.clone(), record: b"r".to_vec(), ttl_secs: 60 };
    b.ack(&a, Some(r), &[]).await.unwrap();
    let s = b.queue_stats(&q).await.unwrap();
    assert_eq!((s.stream, s.pending), (0, 0), "acked, and nothing was released");
    assert_eq!(b.get_result(&a.header.id).await.unwrap(), Some(b"r".to_vec()));
    assert!(b.workflow(&wf.id).await.unwrap().is_none());
}

#[tokio::test]
async fn dead_letter_then_replay_resumes_the_workflow() {
    let (b, q) = setup().await;
    let wf = diamond(&q);
    b.submit_workflow(&wf).await.unwrap();
    let a = fetch_all(&b, &q).await.remove(0);
    b.ack(&a, None, &[]).await.unwrap();
    let ds = fetch_all(&b, &q).await;
    b.dead_letter(by_task(&ds, "b"), "boom", None).await.unwrap();
    b.ack(by_task(&ds, "c"), None, &[]).await.unwrap();
    assert_eq!(states(&state_of(&b, &wf).await), vec![Done, Dead, Done, Waiting]);

    let dl = b.dead_letters(&q, None, None, 10).await.unwrap();
    assert_eq!(dl.len(), 1);
    assert_eq!(b.replay_dead(&q, &[dl[0].entry_id.clone()]).await.unwrap(), 1);
    assert_eq!(states(&state_of(&b, &wf).await), vec![Done, Queued, Done, Waiting]);
    assert_eq!(b.replay_dead(&q, &[dl[0].entry_id.clone()]).await.unwrap(), 0);

    let ds = fetch_all(&b, &q).await;
    assert_eq!(tasks(&ds), vec!["b"]);
    assert_eq!(ds[0].header.node.as_deref(), Some("1"));
    b.ack(&ds[0], None, &[]).await.unwrap();
    assert_eq!(states(&state_of(&b, &wf).await), vec![Done, Done, Done, Queued]);
    assert_eq!(tasks(&fetch_all(&b, &q).await), vec!["d"]);
}

#[tokio::test]
async fn replay_leaves_a_done_node_and_plain_tasks_alone() {
    let (b, q) = setup().await;
    let wf = diamond(&q);
    b.submit_workflow(&wf).await.unwrap();
    let a = fetch_all(&b, &q).await.remove(0);
    // A dead copy of A, then A finishes elsewhere: replaying the copy must not rewind A.
    b.enqueue(&a.header, &a.payload).await.unwrap();
    let a2 = fetch_all(&b, &q).await.remove(0);
    b.dead_letter(&a2, "boom", None).await.unwrap();
    assert_eq!(states(&state_of(&b, &wf).await)[0], Dead);
    b.ack(&a, None, &[]).await.unwrap();
    assert_eq!(states(&state_of(&b, &wf).await)[0], Done);

    // A plain task in the same dead stream is replayed with no workflow bookkeeping.
    let plain = Header::new("plain", &q, 0, "ct");
    b.enqueue(&plain, b"").await.unwrap();
    let ds = fetch_all(&b, &q).await;
    b.dead_letter(by_task(&ds, "plain"), "boom", None).await.unwrap();
    let ids: Vec<String> = b.dead_letters(&q, None, None, 10).await.unwrap().into_iter().map(|d| d.entry_id).collect();
    assert_eq!(ids.len(), 2);
    assert_eq!(b.replay_dead(&q, &ids).await.unwrap(), 2);
    assert_eq!(states(&state_of(&b, &wf).await)[0], Done, "a done node stays done");
}

#[tokio::test]
async fn an_ordinary_header_is_stored_byte_for_byte_as_before() {
    let (b, q) = setup().await;
    let h = Header::new("plain", &q, 3, "application/msgpack");
    b.enqueue(&h, b"p").await.unwrap();
    let mut c = raw().await;
    let v: redis::Value = redis::cmd("XRANGE").arg(stream_key(&q)).arg("-").arg("+").query_async(&mut c).await.unwrap();
    let redis::Value::Array(entries) = v else { panic!("bad XRANGE reply") };
    let redis::Value::Array(parts) = &entries[0] else { panic!("bad entry") };
    let redis::Value::Array(flat) = &parts[1] else { panic!("bad fields") };
    let mut stored = None;
    for pair in flat.chunks(2) {
        if let [redis::Value::BulkString(k), redis::Value::BulkString(v)] = pair && k == b"h" {
            stored = Some(v.clone());
        }
    }
    let stored = stored.expect("an h field");
    assert_eq!(stored, encode_header(&h));
    let back = decode_header(&stored).unwrap();
    assert_eq!((back.workflow_id, back.node), (None, None));
}

#[tokio::test]
async fn a_workflow_expires_after_its_ttl_and_a_release_refreshes_it() {
    let (b, q) = setup().await;
    let wf = WorkflowSpec::new("ttl", 1, vec![node("a", &q, &[]), node("b", &q, &[0])]);
    b.submit_workflow(&wf).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(600)).await;
    let a = b.fetch(std::slice::from_ref(&q), 1, 100).await.unwrap().remove(0);
    b.ack(&a, None, &[]).await.unwrap(); // the release restores the full second
    tokio::time::sleep(std::time::Duration::from_millis(600)).await;
    let s = b.workflow(&wf.id).await.unwrap().expect("refreshed by the release");
    assert_eq!(s.nodes[0].state, Done);
    tokio::time::sleep(std::time::Duration::from_millis(700)).await;
    assert!(b.workflow(&wf.id).await.unwrap().is_none(), "expired");
    assert!(!b.workflows().await.unwrap().iter().any(|w| w.id == wf.id));
    assert!(!b.cancel_workflow(&wf.id).await.unwrap());
    // The released child still runs; its ack finds no workflow and releases nothing.
    let child = b.fetch(std::slice::from_ref(&q), 1, 100).await.unwrap().remove(0);
    b.ack(&child, None, &[]).await.unwrap();
    assert_eq!(b.queue_stats(&q).await.unwrap().stream, 0);
}
