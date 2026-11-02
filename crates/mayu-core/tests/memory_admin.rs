//! The admin calls `MemoryBroker` shares with `RedisBroker` (dead letters, replay, listings, TTLs). Each test
//! makes the same assertions as its twin in `crates/mayu-redis/tests`.
use std::time::Duration;

use mayu_core::{
    Broker, Header, MemoryBroker, NodeState, ResultRecord, StoredResult, WorkerInfo, WorkflowNode, WorkflowSpec,
    now_ms,
};

fn uniq(prefix: &str) -> String { format!("{prefix}-{}", ulid::Ulid::generate()) }

async fn setup() -> (MemoryBroker, String) {
    let q = uniq("q");
    let b = MemoryBroker::default().with_consumer(&uniq("c"));
    b.ensure_queues(std::slice::from_ref(&q)).await.unwrap();
    (b, q)
}

async fn dead_one(b: &MemoryBroker, q: &str, attempt: u32, error: &str) -> Header {
    let mut h = Header::new("t", q, 3, "ct");
    h.attempt = attempt;
    b.enqueue(&h, b"payload").await.unwrap();
    let d = b.fetch(&[q.to_string()], 1, 100).await.unwrap().remove(0);
    let stored = StoredResult { id: h.id.clone(), record: ResultRecord::failure(error, "", "ct").encode(), ttl_secs: 60 };
    b.dead_letter(&d, error, Some(stored)).await.unwrap();
    h
}

#[tokio::test]
async fn dead_letters_list_oldest_first_and_page() {
    let (b, q) = setup().await;
    let h1 = dead_one(&b, &q, 0, "first").await;
    let h2 = dead_one(&b, &q, 0, "second").await;
    let all = b.dead_letters(&q, None, None, 10).await.unwrap();
    assert_eq!(all.len(), 2);
    assert_eq!(all[0].header.as_ref().unwrap().id, h1.id);
    assert_eq!(all[0].error, "first");
    assert_eq!(all[0].payload, b"payload");
    assert!(all[0].dead_at_ms() > 0 && all[0].dead_at_ms() <= now_ms());
    let rest = b.dead_letters(&q, Some(&all[0].entry_id), None, 10).await.unwrap();
    assert_eq!(rest.len(), 1);
    assert_eq!(rest[0].header.as_ref().unwrap().id, h2.id);
    let upto = b.dead_letters(&q, None, Some(&all[0].entry_id), 10).await.unwrap();
    assert_eq!(upto.len(), 1);
    assert_eq!(b.last_dead_id(&q).await.unwrap(), Some(all[1].entry_id.clone()));
}

#[tokio::test]
async fn last_dead_id_of_empty_queue_is_none() {
    let (b, q) = setup().await;
    assert_eq!(b.last_dead_id(&q).await.unwrap(), None);
    assert!(b.dead_letters(&q, None, None, 10).await.unwrap().is_empty());
}

#[tokio::test]
async fn replay_resets_attempt_keeps_id_and_clears_the_failure_result() {
    let (b, q) = setup().await;
    let h = dead_one(&b, &q, 3, "boom").await;
    assert!(b.get_result(&h.id).await.unwrap().is_some());
    let dl = b.dead_letters(&q, None, None, 10).await.unwrap();
    assert_eq!(b.replay_dead(&q, &[dl[0].entry_id.clone()]).await.unwrap(), 1);
    assert!(b.dead_letters(&q, None, None, 10).await.unwrap().is_empty());
    assert_eq!(b.get_result(&h.id).await.unwrap(), None, "a waiting client must see the new outcome");
    let d = b.fetch(std::slice::from_ref(&q), 1, 100).await.unwrap().remove(0);
    assert_eq!(d.header.id, h.id);
    assert_eq!(d.header.attempt, 0);
    assert_eq!(d.header.eta, None);
    assert_eq!(d.payload, b"payload");
}

#[tokio::test]
async fn replay_starts_the_redelivery_count_over() {
    let (b, q) = setup().await;
    let mut h = Header::new("t", &q, 3, "ct");
    h.redeliveries = 4; // a poison task: its crash deliveries were carried across rate-limit deferrals
    b.enqueue(&h, b"payload").await.unwrap();
    let d = b.fetch(std::slice::from_ref(&q), 1, 100).await.unwrap().remove(0);
    b.dead_letter(&d, "poison", None).await.unwrap();
    let dl = b.dead_letters(&q, None, None, 10).await.unwrap();
    assert_eq!(b.replay_dead(&q, &[dl[0].entry_id.clone()]).await.unwrap(), 1);
    let d = b.fetch(std::slice::from_ref(&q), 1, 100).await.unwrap().remove(0);
    assert_eq!(d.header.redeliveries, 0, "a replayed task gets a fresh --max-deliveries budget");
}

#[tokio::test]
async fn replay_twice_moves_once() {
    let (b, q) = setup().await;
    dead_one(&b, &q, 0, "e").await;
    let id = b.dead_letters(&q, None, None, 1).await.unwrap()[0].entry_id.clone();
    assert_eq!(b.replay_dead(&q, std::slice::from_ref(&id)).await.unwrap(), 1);
    assert_eq!(b.replay_dead(&q, &[id]).await.unwrap(), 0);
    assert_eq!(b.queue_stats(&q).await.unwrap().stream, 1);
}

#[tokio::test]
async fn concurrent_replays_move_once() {
    let (b, q) = setup().await;
    for _ in 0..20 {
        dead_one(&b, &q, 0, "e").await;
    }
    let ids: Vec<String> = b.dead_letters(&q, None, None, 100).await.unwrap().into_iter().map(|d| d.entry_id).collect();
    let b2 = b.with_consumer(&uniq("c"));
    let (x, y) = tokio::join!(b.replay_dead(&q, &ids), b2.replay_dead(&q, &ids));
    assert_eq!(x.unwrap() + y.unwrap(), 20);
    assert_eq!(b.queue_stats(&q).await.unwrap().stream, 20);
}

#[tokio::test]
async fn replay_skips_missing_and_undecodable_entries() {
    let (b, q) = setup().await;
    b.push_raw(&q, b"garbage".to_vec(), Vec::new());
    assert!(b.fetch(std::slice::from_ref(&q), 10, 100).await.unwrap().is_empty(), "undecodable entries are buried on fetch");
    let dl = b.dead_letters(&q, None, None, 10).await.unwrap();
    assert_eq!(dl.len(), 1);
    assert!(dl[0].header.is_none());
    assert_eq!(dl[0].raw_header, b"garbage");
    assert_eq!(b.replay_dead(&q, &[dl[0].entry_id.clone(), "1-1".into()]).await.unwrap(), 0);
    assert_eq!(b.dead_letters(&q, None, None, 10).await.unwrap().len(), 1, "left in place");
}

fn worker(id: &str, q: &str) -> WorkerInfo {
    WorkerInfo { worker_id: id.into(), hostname: "h".into(), pid: 1, queues: vec![q.into()],
                 concurrency: 2, inflight: 0, started_at: 1, last_seen: 2, tasks: vec![] }
}

#[tokio::test]
async fn workers_lists_live_heartbeats() {
    let (b, q) = setup().await;
    let (a, z) = (uniq("w"), uniq("w"));
    for id in [&a, &z] {
        b.heartbeat(&worker(id, &q), 10_000).await.unwrap();
    }
    let ws = b.workers().await.unwrap();
    let mine: Vec<&WorkerInfo> = ws.iter().filter(|w| w.worker_id == a || w.worker_id == z).collect();
    assert_eq!(mine.len(), 2);
    assert!(ws.windows(2).all(|p| p[0].worker_id <= p[1].worker_id), "sorted");
    assert_eq!(mine[0].queues, vec![q.clone()]);
    b.unregister(&a).await.unwrap();
    assert!(!b.workers().await.unwrap().iter().any(|w| w.worker_id == a));
}

#[tokio::test]
async fn worker_info_roundtrip_and_expiry() {
    let (b, q) = setup().await;
    let info = worker(&uniq("w"), &q);
    b.heartbeat(&info, 300).await.unwrap();
    assert_eq!(b.worker_info(&info.worker_id).await.unwrap(), Some(info.clone()));
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(b.worker_info(&info.worker_id).await.unwrap(), None, "expired");
    assert!(!b.workers().await.unwrap().iter().any(|w| w.worker_id == info.worker_id));
}

#[tokio::test]
async fn known_queues_finds_queues_in_the_broker() {
    let (b, q) = setup().await;
    let delayed_only = uniq("q");
    let mut h = Header::new("t", &delayed_only, 0, "ct");
    h.eta = Some(now_ms() + 60_000);
    b.enqueue(&h, b"").await.unwrap();
    let qs = b.known_queues().await.unwrap();
    assert!(qs.contains(&q) && qs.contains(&delayed_only));
    assert!(qs.windows(2).all(|p| p[0] < p[1]), "sorted and unique");
}

#[tokio::test]
async fn known_queues_keeps_a_dead_stream_after_replay() {
    let (b, _) = setup().await;
    let q = uniq("q"); // never ensured: only its dead stream makes it known
    dead_one(&b, &q, 0, "e").await;
    let id = b.last_dead_id(&q).await.unwrap().unwrap();
    b.replay_dead(&q, &[id]).await.unwrap();
    assert!(b.known_queues().await.unwrap().contains(&q));
}

#[tokio::test]
async fn results_expire_after_their_ttl() {
    let (b, q) = setup().await;
    let h = Header::new("t", &q, 0, "ct");
    b.enqueue(&h, b"").await.unwrap();
    let d = b.fetch(std::slice::from_ref(&q), 1, 100).await.unwrap().remove(0);
    b.ack(&d, Some(StoredResult { id: h.id.clone(), record: b"r".to_vec(), ttl_secs: 1 }), &[]).await.unwrap();
    assert_eq!(b.get_result(&h.id).await.unwrap(), Some(b"r".to_vec()));
    tokio::time::sleep(Duration::from_millis(1_200)).await;
    assert_eq!(b.get_result(&h.id).await.unwrap(), None);
}

fn node(task: &str, q: &str, parents: &[usize]) -> WorkflowNode {
    WorkflowNode { header: Header::new(task, q, 0, "ct"), payload: task.as_bytes().to_vec(), parents: parents.to_vec() }
}

#[tokio::test]
async fn workflows_list_in_creation_order() {
    let (b, q) = setup().await;
    let first = WorkflowSpec::new("w", 3600, vec![node("a", &q, &[]), node("b", &q, &[0])]);
    b.submit_workflow(&first).await.unwrap();
    tokio::time::sleep(Duration::from_millis(5)).await; // created_at has ms resolution
    let second = WorkflowSpec::new("w", 3600, vec![node("a", &q, &[])]);
    b.submit_workflow(&second).await.unwrap();
    let all = b.workflows().await.unwrap();
    let pos = |id: &str| all.iter().position(|w| w.id == id).unwrap();
    assert!(pos(&first.id) < pos(&second.id));
    assert!(all.windows(2).all(|w| w[0].created_at <= w[1].created_at));
    assert_eq!(all[pos(&first.id)].nodes.len(), 2);
}

#[tokio::test]
async fn a_workflow_expires_after_its_ttl_and_a_release_refreshes_it() {
    let (b, q) = setup().await;
    let wf = WorkflowSpec::new("ttl", 1, vec![node("a", &q, &[]), node("b", &q, &[0])]);
    b.submit_workflow(&wf).await.unwrap();
    tokio::time::sleep(Duration::from_millis(600)).await;
    let a = b.fetch(std::slice::from_ref(&q), 1, 100).await.unwrap().remove(0);
    b.ack(&a, None, &[]).await.unwrap(); // the release restores the full second
    tokio::time::sleep(Duration::from_millis(600)).await;
    let s = b.workflow(&wf.id).await.unwrap().expect("refreshed by the release");
    assert_eq!(s.nodes[0].state, NodeState::Done);
    tokio::time::sleep(Duration::from_millis(700)).await;
    assert!(b.workflow(&wf.id).await.unwrap().is_none(), "expired");
    assert!(!b.workflows().await.unwrap().iter().any(|w| w.id == wf.id));
    assert!(!b.cancel_workflow(&wf.id).await.unwrap());
    // The released child still runs; its ack finds no workflow and releases nothing.
    let child = b.fetch(std::slice::from_ref(&q), 1, 100).await.unwrap().remove(0);
    b.ack(&child, None, &[]).await.unwrap();
    assert_eq!(b.queue_stats(&q).await.unwrap().stream, 0);
}

fn diamond(q: &str) -> WorkflowSpec {
    WorkflowSpec::new("diamond", 3600, vec![
        node("a", q, &[]), node("b", q, &[0]), node("c", q, &[0]), node("d", q, &[1, 2]),
    ])
}

async fn fetch_all(b: &MemoryBroker, q: &str) -> Vec<mayu_core::Delivery> {
    b.fetch(&[q.to_string()], 10, 50).await.unwrap()
}

async fn states(b: &MemoryBroker, wf: &WorkflowSpec) -> Vec<NodeState> {
    b.workflow(&wf.id).await.unwrap().unwrap().nodes.iter().map(|n| n.state).collect()
}

fn by_task<'a>(ds: &'a [mayu_core::Delivery], task: &str) -> &'a mayu_core::Delivery {
    ds.iter().find(|d| d.header.task == task).unwrap()
}

#[tokio::test]
async fn dead_letter_then_replay_resumes_the_workflow() {
    use NodeState::{Dead, Done, Queued, Waiting};
    let (b, q) = setup().await;
    let wf = diamond(&q);
    b.submit_workflow(&wf).await.unwrap();
    let a = fetch_all(&b, &q).await.remove(0);
    b.ack(&a, None, &[]).await.unwrap();
    let ds = fetch_all(&b, &q).await;
    b.dead_letter(by_task(&ds, "b"), "boom", None).await.unwrap();
    b.ack(by_task(&ds, "c"), None, &[]).await.unwrap();
    assert_eq!(states(&b, &wf).await, vec![Done, Dead, Done, Waiting]);

    let dl = b.dead_letters(&q, None, None, 10).await.unwrap();
    assert_eq!(dl.len(), 1);
    assert_eq!(b.replay_dead(&q, &[dl[0].entry_id.clone()]).await.unwrap(), 1);
    assert_eq!(states(&b, &wf).await, vec![Done, Queued, Done, Waiting]);
    assert_eq!(b.replay_dead(&q, &[dl[0].entry_id.clone()]).await.unwrap(), 0);

    let ds = fetch_all(&b, &q).await;
    assert_eq!(ds.len(), 1);
    assert_eq!(ds[0].header.task, "b");
    assert_eq!(ds[0].header.node.as_deref(), Some("1"));
    b.ack(&ds[0], None, &[]).await.unwrap();
    assert_eq!(states(&b, &wf).await, vec![Done, Done, Done, Queued]);
    assert_eq!(fetch_all(&b, &q).await[0].header.task, "d");
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
    assert_eq!(states(&b, &wf).await[0], NodeState::Dead);
    b.ack(&a, None, &[]).await.unwrap();
    assert_eq!(states(&b, &wf).await[0], NodeState::Done);

    // A plain task in the same dead stream is replayed with no workflow bookkeeping.
    let plain = Header::new("plain", &q, 0, "ct");
    b.enqueue(&plain, b"").await.unwrap();
    let ds = fetch_all(&b, &q).await;
    b.dead_letter(by_task(&ds, "plain"), "boom", None).await.unwrap();
    let ids: Vec<String> = b.dead_letters(&q, None, None, 10).await.unwrap().into_iter().map(|d| d.entry_id).collect();
    assert_eq!(ids.len(), 2);
    assert_eq!(b.replay_dead(&q, &ids).await.unwrap(), 2);
    assert_eq!(states(&b, &wf).await[0], NodeState::Done, "a done node stays done");
}
