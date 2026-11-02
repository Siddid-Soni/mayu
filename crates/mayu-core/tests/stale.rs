//! A worker that lost a task to a reclaim must not settle it: the worker that took it over decides the outcome.
//! Each test: A fetches, B reclaims, then A settles (stale) and, where relevant, B settles.
//! The Redis counterparts are in `crates/mayu-redis/tests/{redis_broker,workflow}.rs`.

use mayu_core::{
    Broker, Delivery, Header, MemoryBroker, NodeState, StoredResult, WorkflowNode, WorkflowSpec, now_ms,
};

const Q: &str = "q";

fn qs() -> Vec<String> { vec![Q.to_string()] }

fn stored(id: &str, record: &str) -> Option<StoredResult> {
    Some(StoredResult { id: id.to_string(), record: record.as_bytes().to_vec(), ttl_secs: 60 })
}

/// Consumers `a` and `b` on one shared state; `a` has fetched one task and `b` has reclaimed it.
async fn a_lost_to_b() -> (MemoryBroker, MemoryBroker, Delivery, Delivery) {
    let m = MemoryBroker::default();
    let (a, b) = (m.with_consumer("a"), m.with_consumer("b"));
    m.push(&Header::new("t", Q, 3, "ct"), b"x");
    let da = a.fetch(&qs(), 1, 10).await.unwrap().remove(0);
    let db = b.reclaim(Q, 0, "0-0", 10).await.unwrap().deliveries.remove(0);
    assert_eq!((da.stream_id.as_str(), db.delivery_count), (db.stream_id.as_str(), 2));
    (a, b, da, db)
}

async fn drain(b: &MemoryBroker, queue: &str) -> Vec<Delivery> {
    b.fetch(&[queue.to_string()], 10, 10).await.unwrap()
}

#[tokio::test]
async fn stale_retry_leaves_the_new_owners_entry() {
    let (a, b, da, _) = a_lost_to_b().await;
    let mut next = da.header.clone();
    next.attempt = 1;
    next.eta = Some(now_ms());
    assert!(!a.retry(&da, &next).await.unwrap());
    let s = b.queue_stats(Q).await.unwrap();
    assert_eq!((s.pending, s.delayed), (1, 0), "stale retry must not ack B's entry or schedule a second run");
}

#[tokio::test]
async fn stale_dead_letter_does_not_overwrite_the_new_owners_success() {
    let (a, b, da, db) = a_lost_to_b().await;
    let id = &da.header.id;
    assert!(!a.dead_letter(&da, "boom", stored(id, "failure")).await.unwrap());
    let s = b.queue_stats(Q).await.unwrap();
    assert_eq!((s.pending, s.dead), (1, 0), "stale dead_letter must leave B's entry pending");
    assert!(b.ack(&db, stored(id, "success"), &[]).await.unwrap());
    assert_eq!(b.queue_stats(Q).await.unwrap().dead, 0, "a task B completed must not be dead-lettered");
    assert_eq!(b.get_result(id).await.unwrap(), Some(b"success".to_vec()));
}

#[tokio::test]
async fn stale_requeue_adds_no_second_copy() {
    let (a, b, da, _) = a_lost_to_b().await;
    assert!(!a.requeue(&da).await.unwrap());
    let s = b.queue_stats(Q).await.unwrap();
    assert_eq!((s.stream, s.pending), (1, 1), "stale requeue must leave only B's pending entry");
    assert!(drain(&b, Q).await.is_empty(), "no second copy to fetch");
}

#[tokio::test]
async fn stale_ack_is_overwritten_by_the_owner_and_adds_no_follow_ups() {
    let (a, b, da, db) = a_lost_to_b().await;
    let id = &da.header.id;
    let (cb_a, cb_b) = (Header::new("cb-a", "cb", 0, "ct"), Header::new("cb-b", "cb", 0, "ct"));
    assert!(!a.ack(&da, stored(id, "a"), &[(cb_a, b"a".to_vec())]).await.unwrap());
    assert_eq!(b.queue_stats(Q).await.unwrap().pending, 1, "stale ack must leave B's entry pending");
    assert!(b.ack(&db, stored(id, "b"), &[(cb_b.clone(), b"b".to_vec())]).await.unwrap());
    assert_eq!(b.get_result(id).await.unwrap(), Some(b"b".to_vec()));
    let follow = drain(&b, "cb").await;
    assert_eq!(follow.iter().map(|d| d.header.task.as_str()).collect::<Vec<_>>(), ["cb-b"], "added once, by B");
}

#[tokio::test]
async fn stale_ack_after_the_owner_keeps_its_result_and_adds_no_follow_ups() {
    let (a, b, da, db) = a_lost_to_b().await;
    let id = &da.header.id;
    let (cb_a, cb_b) = (Header::new("cb-a", "cb", 0, "ct"), Header::new("cb-b", "cb", 0, "ct"));
    assert!(b.ack(&db, stored(id, "b"), &[(cb_b, b"b".to_vec())]).await.unwrap());
    assert!(!a.ack(&da, stored(id, "a"), &[(cb_a, b"a".to_vec())]).await.unwrap());
    assert_eq!(b.get_result(id).await.unwrap(), Some(b"b".to_vec()), "a late stale ack must not overwrite");
    let follow = drain(&b, "cb").await;
    assert_eq!(follow.iter().map(|d| d.header.task.as_str()).collect::<Vec<_>>(), ["cb-b"]);
}

fn node(task: &str, parents: &[usize]) -> WorkflowNode {
    WorkflowNode { header: Header::new(task, Q, 0, "ct"), payload: task.as_bytes().to_vec(), parents: parents.to_vec() }
}

#[tokio::test]
async fn stale_ack_of_a_workflow_node_releases_nothing() {
    let m = MemoryBroker::default();
    let (a, b) = (m.with_consumer("a"), m.with_consumer("b"));
    // a -> (b, c)
    let wf = WorkflowSpec::new("fan", 3600, vec![node("a", &[]), node("b", &[0]), node("c", &[0])]);
    m.submit_workflow(&wf).await.unwrap();
    let da = a.fetch(&qs(), 1, 10).await.unwrap().remove(0);
    let db = b.reclaim(Q, 0, "0-0", 10).await.unwrap().deliveries.remove(0);

    assert!(!a.ack(&da, None, &[]).await.unwrap());
    let st = b.workflow(&wf.id).await.unwrap().unwrap();
    let states: Vec<_> = st.nodes.iter().map(|n| n.state).collect();
    assert_eq!(states, [NodeState::Queued, NodeState::Waiting, NodeState::Waiting], "stale ack changed the node");
    assert!(drain(&b, Q).await.is_empty(), "stale ack released children");

    assert!(b.ack(&db, None, &[]).await.unwrap());
    let mut children: Vec<_> = drain(&b, Q).await.into_iter().map(|d| d.header.task).collect();
    children.sort();
    assert_eq!(children, ["b", "c"], "B's ack releases the children once");
}

#[tokio::test]
async fn retry_repeated_after_it_applied_schedules_once() {
    let m = MemoryBroker::default();
    m.push(&Header::new("t", Q, 3, "ct"), b"x");
    let d = m.fetch(&qs(), 1, 10).await.unwrap().remove(0);
    let mut next = d.header.clone();
    next.attempt = 1;
    next.eta = Some(now_ms() + 60_000);
    assert!(m.retry(&d, &next).await.unwrap());
    assert!(!m.retry(&d, &next).await.unwrap()); // the first reply was lost
    assert_eq!(m.queue_stats(Q).await.unwrap().delayed, 1);
}

#[tokio::test]
async fn dead_letter_repeated_after_it_applied_buries_once() {
    let m = MemoryBroker::default();
    m.push(&Header::new("t", Q, 0, "ct"), b"x");
    let d = m.fetch(&qs(), 1, 10).await.unwrap().remove(0);
    assert!(m.dead_letter(&d, "boom", None).await.unwrap());
    assert!(!m.dead_letter(&d, "boom", None).await.unwrap());
    assert_eq!(m.queue_stats(Q).await.unwrap().dead, 1);
}

#[tokio::test]
async fn requeue_repeated_after_it_applied_adds_one_copy() {
    let m = MemoryBroker::default();
    m.push(&Header::new("t", Q, 0, "ct"), b"x");
    let d = m.fetch(&qs(), 1, 10).await.unwrap().remove(0);
    assert!(m.requeue(&d).await.unwrap());
    assert!(!m.requeue(&d).await.unwrap());
    assert_eq!(m.queue_stats(Q).await.unwrap().stream, 1);
}
