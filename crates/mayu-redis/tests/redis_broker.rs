use std::time::Duration;
use mayu_core::{Broker, Header, ResultRecord, StoredResult, WorkerInfo, now_ms};
use mayu_redis::{RedisBroker, stream_key, worker_key};
use redis::Value;

fn url() -> String {
    std::env::var("MAYU_TEST_REDIS").unwrap_or_else(|_| "redis://127.0.0.1:6390/0".into())
}

fn uniq(prefix: &str) -> String { format!("{prefix}-{}", ulid::Ulid::generate()) }

async fn setup() -> (RedisBroker, String) {
    let q = uniq("q");
    let b = RedisBroker::connect(&url(), uniq("c")).await.expect("is Redis running? scripts/redis-up.sh");
    b.ensure_queues(&[q.clone()]).await.unwrap();
    (b, q)
}

#[tokio::test]
async fn enqueue_fetch_roundtrip() {
    let (b, q) = setup().await;
    let h = Header::new("t", &q, 3, "ct");
    b.enqueue(&h, b"payload").await.unwrap();
    let ds = b.fetch(&[q.clone()], 10, 100).await.unwrap();
    assert_eq!(ds.len(), 1);
    assert_eq!(ds[0].header, h);
    assert_eq!(ds[0].payload, b"payload");
    assert_eq!(ds[0].queue, q);
    assert_eq!(b.queue_stats(&q).await.unwrap().pending, 1);
}

#[tokio::test]
async fn enqueue_before_group_exists_is_delivered() {
    let q = uniq("q");
    let b = RedisBroker::connect(&url(), uniq("c")).await.unwrap();
    b.enqueue(&Header::new("t", &q, 0, "ct"), b"").await.unwrap();
    b.ensure_queues(&[q.clone()]).await.unwrap();
    assert_eq!(b.fetch(&[q.clone()], 10, 100).await.unwrap().len(), 1);
}

#[tokio::test]
async fn ensure_queues_is_idempotent() {
    let (b, q) = setup().await;
    b.ensure_queues(&[q.clone()]).await.unwrap();
}

#[tokio::test]
async fn fetch_empty_returns_after_block() {
    let (b, q) = setup().await;
    let t = std::time::Instant::now();
    assert!(b.fetch(&[q], 1, 100).await.unwrap().is_empty());
    assert!(t.elapsed() >= std::time::Duration::from_millis(90));
}

#[tokio::test]
async fn ack_removes_entry_and_stores_result() {
    let (b, q) = setup().await;
    let h = Header::new("t", &q, 0, "ct");
    b.enqueue(&h, b"").await.unwrap();
    let d = b.fetch(&[q.clone()], 1, 100).await.unwrap().remove(0);
    b.ack(&d, Some(StoredResult { id: h.id.clone(), record: b"rec".to_vec(), ttl_secs: 60 }), &[]).await.unwrap();
    let s = b.queue_stats(&q).await.unwrap();
    assert_eq!((s.stream, s.pending), (0, 0));
    assert_eq!(b.get_result(&h.id).await.unwrap(), Some(b"rec".to_vec()));
}

#[tokio::test]
async fn delayed_enqueue_and_promote() {
    let (b, q) = setup().await;
    let mut h = Header::new("t", &q, 0, "ct");
    h.eta = Some(now_ms() + 60_000);
    b.enqueue(&h, b"p:with:colons").await.unwrap();
    assert_eq!(b.queue_stats(&q).await.unwrap().delayed, 1);
    assert_eq!(b.promote(&q, now_ms(), 100).await.unwrap(), 0);
    assert_eq!(b.promote(&q, now_ms() + 120_000, 100).await.unwrap(), 1);
    let d = b.fetch(&[q.clone()], 1, 100).await.unwrap().remove(0);
    assert_eq!(d.header, h);
    assert_eq!(d.payload, b"p:with:colons");
}

#[tokio::test]
async fn retry_moves_to_delayed_with_new_attempt() {
    let (b, q) = setup().await;
    b.enqueue(&Header::new("t", &q, 3, "ct"), b"x").await.unwrap();
    let d = b.fetch(&[q.clone()], 1, 100).await.unwrap().remove(0);
    let mut next = d.header.clone();
    next.attempt = 1;
    next.eta = Some(now_ms());
    b.retry(&d, &next).await.unwrap();
    let s = b.queue_stats(&q).await.unwrap();
    assert_eq!((s.stream, s.pending, s.delayed), (0, 0, 1));
    b.promote(&q, now_ms() + 1, 10).await.unwrap();
    let d2 = b.fetch(&[q.clone()], 1, 100).await.unwrap().remove(0);
    assert_eq!(d2.header.attempt, 1);
    assert_eq!(d2.payload, b"x");
}

#[tokio::test]
async fn dead_letter_and_requeue() {
    let (b, q) = setup().await;
    let h = Header::new("t", &q, 0, "ct");
    b.enqueue(&h, b"").await.unwrap();
    b.enqueue(&Header::new("t", &q, 0, "ct"), b"").await.unwrap();
    let ds = b.fetch(&[q.clone()], 2, 100).await.unwrap();
    b.dead_letter(&ds[0], "boom", Some(StoredResult { id: h.id.clone(), record: b"f".to_vec(), ttl_secs: 60 }))
        .await.unwrap();
    b.requeue(&ds[1]).await.unwrap();
    let s = b.queue_stats(&q).await.unwrap();
    assert_eq!((s.dead, s.pending, s.stream), (1, 0, 1));
    assert_eq!(b.get_result(&h.id).await.unwrap(), Some(b"f".to_vec()));
    assert_eq!(b.fetch(&[q], 1, 100).await.unwrap()[0].header, ds[1].header);
}

#[tokio::test]
async fn corrupt_header_is_dead_lettered() {
    let (b, q) = setup().await;
    let client = redis::Client::open(url()).unwrap();
    let mut c = client.get_multiplexed_async_connection().await.unwrap();
    let _: String = redis::cmd("XADD").arg(stream_key(&q)).arg("*").arg("h").arg("garbage").arg("p").arg("x")
        .query_async(&mut c).await.unwrap();
    assert!(b.fetch(&[q.clone()], 10, 100).await.unwrap().is_empty());
    let s = b.queue_stats(&q).await.unwrap();
    assert_eq!((s.dead, s.pending, s.stream), (1, 0, 0));
}

#[tokio::test]
async fn fetch_recovers_from_deleted_stream() {
    let (b, q) = setup().await;
    let client = redis::Client::open(url()).unwrap();
    let mut c = client.get_multiplexed_async_connection().await.unwrap();
    let _: i64 = redis::cmd("DEL").arg(stream_key(&q)).query_async(&mut c).await.unwrap();
    assert!(b.fetch(&[q.clone()], 1, 50).await.unwrap().is_empty(), "NOGROUP handled");
    b.enqueue(&Header::new("t", &q, 0, "ct"), b"").await.unwrap();
    assert_eq!(b.fetch(&[q], 1, 100).await.unwrap().len(), 1);
}

#[tokio::test]
async fn concurrent_promote_never_duplicates() {
    let (b1, q) = setup().await;
    let b2 = RedisBroker::connect(&url(), uniq("c")).await.unwrap();
    for _ in 0..50 {
        let mut h = Header::new("t", &q, 0, "ct");
        h.eta = Some(now_ms() + 60_000);
        b1.enqueue(&h, b"").await.unwrap();
    }
    let later = now_ms() + 120_000;
    let (a, c) = tokio::join!(b1.promote(&q, later, 7), b2.promote(&q, later, 7));
    let mut total = a.unwrap() + c.unwrap();
    while total < 50 { total += b1.promote(&q, later, 7).await.unwrap(); }
    let s = b1.queue_stats(&q).await.unwrap();
    assert_eq!((s.stream, s.delayed, total), (50, 0, 50));
}

async fn raw() -> redis::aio::MultiplexedConnection {
    redis::Client::open(url()).unwrap().get_multiplexed_async_connection().await.unwrap()
}

async fn consumer_count(q: &str) -> usize {
    let v: Vec<Value> = redis::cmd("XINFO").arg("CONSUMERS").arg(stream_key(q)).arg("mayu")
        .query_async(&mut raw().await).await.unwrap();
    v.len()
}

#[tokio::test]
async fn fresh_fetch_has_delivery_count_one() {
    let (b, q) = setup().await;
    b.enqueue(&Header::new("t", &q, 0, "ct"), b"").await.unwrap();
    assert_eq!(b.fetch(&[q.clone()], 1, 100).await.unwrap()[0].delivery_count, 1);
}

#[tokio::test]
async fn extend_keeps_ownership_and_reports_lost() {
    let (a, q) = setup().await;
    let b = RedisBroker::connect(&url(), uniq("c")).await.unwrap();
    a.enqueue(&Header::new("t", &q, 0, "ct"), b"x").await.unwrap();
    let d = a.fetch(&[q.clone()], 1, 100).await.unwrap().remove(0);
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    assert!(a.extend(&q, &[d.stream_id.clone()]).await.unwrap().is_empty());
    assert!(b.reclaim(&q, 100, "0-0", 10).await.unwrap().deliveries.is_empty(), "extend reset idle time");
    let r = b.reclaim(&q, 0, "0-0", 10).await.unwrap();
    assert_eq!(r.deliveries.len(), 1);
    assert_eq!(r.deliveries[0].delivery_count, 2);
    assert_eq!(r.deliveries[0].payload, b"x");
    assert_eq!(a.extend(&q, &[d.stream_id.clone()]).await.unwrap(), vec![d.stream_id.clone()]);
    assert!(a.extend(&q, &[]).await.unwrap().is_empty());
}

#[tokio::test]
async fn reclaim_pages_with_cursor() {
    let (a, q) = setup().await;
    let b = RedisBroker::connect(&url(), uniq("c")).await.unwrap();
    for _ in 0..5 { a.enqueue(&Header::new("t", &q, 0, "ct"), b"").await.unwrap(); }
    assert_eq!(a.fetch(&[q.clone()], 5, 100).await.unwrap().len(), 5);
    let (mut cursor, mut total) = ("0-0".to_string(), 0);
    for _ in 0..10 {
        let r = b.reclaim(&q, 0, &cursor, 2).await.unwrap();
        total += r.deliveries.len();
        cursor = r.next_cursor;
        if cursor == "0-0" { break; }
    }
    assert_eq!(total, 5);
}

#[tokio::test]
async fn reclaim_dead_letters_corrupt_header() {
    let (b, q) = setup().await;
    let mut c = raw().await;
    let _: String = redis::cmd("XADD").arg(stream_key(&q)).arg("*").arg("h").arg("garbage")
        .query_async(&mut c).await.unwrap();
    let _: Value = redis::cmd("XREADGROUP").arg("GROUP").arg("mayu").arg("someone-else")
        .arg("COUNT").arg(1).arg("STREAMS").arg(stream_key(&q)).arg(">")
        .query_async(&mut c).await.unwrap();
    assert!(b.reclaim(&q, 0, "0-0", 10).await.unwrap().deliveries.is_empty());
    let s = b.queue_stats(&q).await.unwrap();
    assert_eq!((s.dead, s.pending), (1, 0));
}

#[tokio::test]
async fn heartbeat_roundtrip_ttl_and_unregister() {
    let (b, _q) = setup().await;
    let info = WorkerInfo {
        worker_id: uniq("w"), hostname: "host-1".into(), pid: 42, queues: vec!["a".into(), "b".into()],
        concurrency: 8, inflight: 3, started_at: 1_000, last_seen: 2_000, tasks: vec![],
    };
    b.heartbeat(&info, 300).await.unwrap();
    assert_eq!(b.worker_info(&info.worker_id).await.unwrap(), Some(info.clone()));
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    assert_eq!(b.worker_info(&info.worker_id).await.unwrap(), None, "expired");
    b.heartbeat(&info, 60_000).await.unwrap();
    b.unregister(&info.worker_id).await.unwrap();
    assert_eq!(b.worker_info(&info.worker_id).await.unwrap(), None);
    assert!(worker_key("x").starts_with("mayu:workers:"));
}

#[tokio::test]
async fn prune_removes_only_idle_consumers_without_heartbeat_or_pending() {
    let q = uniq("q");
    let ghost_name = uniq("ghost");
    let live_name = uniq("live");
    let busy_name = uniq("busy");
    let ghost = RedisBroker::connect(&url(), ghost_name.clone()).await.unwrap();
    let live = RedisBroker::connect(&url(), live_name.clone()).await.unwrap();
    let busy = RedisBroker::connect(&url(), busy_name.clone()).await.unwrap();
    ghost.ensure_queues(&[q.clone()]).await.unwrap();
    for _ in 0..3 { ghost.enqueue(&Header::new("t", &q, 0, "ct"), b"").await.unwrap(); }
    for b in [&ghost, &live] {
        let d = b.fetch(&[q.clone()], 1, 100).await.unwrap().remove(0);
        b.ack(&d, None, &[]).await.unwrap(); // consumer exists, 0 pending
    }
    busy.fetch(&[q.clone()], 1, 100).await.unwrap(); // 1 pending, no heartbeat
    live.heartbeat(&WorkerInfo {
        worker_id: live_name.clone(), hostname: "h".into(), pid: 1, queues: vec![q.clone()],
        concurrency: 1, inflight: 0, started_at: 0, last_seen: 0, tasks: vec![],
    }, 60_000).await.unwrap();
    assert_eq!(consumer_count(&q).await, 3);
    assert_eq!(ghost.prune_consumers(&q, 0).await.unwrap(), 1);
    assert_eq!(consumer_count(&q).await, 2, "live (heartbeat) and busy (pending) are kept");
    assert_eq!(ghost.prune_consumers(&q, 60_000).await.unwrap(), 0, "min idle respected");
    assert_eq!(ghost.prune_consumers(&uniq("nope"), 0).await.unwrap(), 0, "missing stream is fine");
}

#[tokio::test]
async fn fetch_takes_queues_in_order_and_caps_the_total() {
    let (b, q1) = setup().await;
    let q2 = uniq("q");
    b.ensure_queues(std::slice::from_ref(&q2)).await.unwrap();
    for q in [&q1, &q2] {
        for _ in 0..3 { b.enqueue(&Header::new("t", q, 0, "ct"), b"").await.unwrap(); }
    }
    let got = b.fetch(&[q2.clone(), q1.clone()], 4, 100).await.unwrap();
    let queues: Vec<&str> = got.iter().map(|d| d.queue.as_str()).collect();
    assert_eq!(queues, vec![q2.as_str(), q2.as_str(), q2.as_str(), q1.as_str()]);
    assert_eq!(b.queue_stats(&q1).await.unwrap().pending, 1);
    assert_eq!(b.queue_stats(&q2).await.unwrap().pending, 3);
}

#[tokio::test]
async fn blocking_fetch_across_queues_stays_within_count() {
    let (b, q1) = setup().await;
    let q2 = uniq("q");
    b.ensure_queues(std::slice::from_ref(&q2)).await.unwrap();
    let producer = RedisBroker::connect(&url(), uniq("p")).await.unwrap();
    let (qa, qb) = (q1.clone(), q2.clone());
    let push = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        for q in [&qa, &qb] {
            for _ in 0..3 { producer.enqueue(&Header::new("t", q, 0, "ct"), b"").await.unwrap(); }
        }
    });
    let got = b.fetch(&[q1.clone(), q2.clone()], 2, 2_000).await.unwrap();
    push.await.unwrap();
    assert!(!got.is_empty() && got.len() <= 2, "claimed {}", got.len());
}

#[tokio::test]
async fn fetch_keeps_claimed_entries_when_a_later_queue_lost_its_group() {
    let (b, q1) = setup().await;
    let q2 = uniq("q");
    b.ensure_queues(std::slice::from_ref(&q2)).await.unwrap();
    for q in [&q1, &q2] {
        b.enqueue(&Header::new("t", q, 0, "ct"), b"").await.unwrap();
    }
    // Deleting the stream drops its group; the next enqueue recreates the stream without one.
    let mut c = redis::Client::open(url().as_str()).unwrap().get_multiplexed_async_connection().await.unwrap();
    redis::cmd("DEL").arg(stream_key(&q2)).query_async::<i64>(&mut c).await.unwrap();
    b.enqueue(&Header::new("t", &q2, 0, "ct"), b"").await.unwrap();

    let got = b.fetch(&[q1.clone(), q2.clone()], 10, 100).await.unwrap();
    assert_eq!(got.iter().filter(|d| d.queue == q1).count(), 1, "q1's claimed entry must be returned");
    assert_eq!(b.queue_stats(&q1).await.unwrap().pending, 1);

    let got = b.fetch(std::slice::from_ref(&q2), 10, 100).await.unwrap();
    assert_eq!(got.len(), 1, "the recreated group delivers q2's entry");
    assert_eq!(got[0].queue, q2);
}

#[tokio::test]
async fn fetch_with_zero_count_returns_nothing() {
    let (b, q) = setup().await;
    b.enqueue(&Header::new("t", &q, 0, "ct"), b"").await.unwrap();
    assert!(b.fetch(std::slice::from_ref(&q), 0, 100).await.unwrap().is_empty());
    assert_eq!(b.queue_stats(&q).await.unwrap().pending, 0);
}

#[tokio::test]
async fn ack_enqueues_follow_ups_only_once() {
    let (b, q) = setup().await;
    let q2 = uniq("q");
    b.ensure_queues(std::slice::from_ref(&q2)).await.unwrap();
    b.enqueue(&Header::new("t", &q, 0, "ct"), b"").await.unwrap();
    let d = b.fetch(std::slice::from_ref(&q), 1, 100).await.unwrap().remove(0);
    let cb = Header::new("cb", &q2, 2, "ct");
    let follow = [(cb.clone(), b"x".to_vec())];
    b.ack(&d, None, &follow).await.unwrap();
    b.ack(&d, None, &follow).await.unwrap(); // a retried ack whose first reply was lost
    let got = b.fetch(std::slice::from_ref(&q2), 10, 100).await.unwrap();
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].header, cb);
    assert_eq!(got[0].payload, b"x");
}

async fn dead_one(b: &RedisBroker, q: &str, attempt: u32, error: &str) -> Header {
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
    let d = b.fetch(&[q.clone()], 1, 100).await.unwrap().remove(0);
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
    let d = b.fetch(&[q.clone()], 1, 100).await.unwrap().remove(0);
    b.dead_letter(&d, "poison", None).await.unwrap();
    let dl = b.dead_letters(&q, None, None, 10).await.unwrap();
    assert_eq!(b.replay_dead(&q, &[dl[0].entry_id.clone()]).await.unwrap(), 1);
    let d = b.fetch(&[q.clone()], 1, 100).await.unwrap().remove(0);
    assert_eq!(d.header.redeliveries, 0, "a replayed task gets a fresh --max-deliveries budget");
}

#[tokio::test]
async fn replay_twice_moves_once() {
    let (b, q) = setup().await;
    dead_one(&b, &q, 0, "e").await;
    let id = b.dead_letters(&q, None, None, 1).await.unwrap()[0].entry_id.clone();
    assert_eq!(b.replay_dead(&q, &[id.clone()]).await.unwrap(), 1);
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
    let b2 = RedisBroker::connect(&url(), uniq("c")).await.unwrap();
    let (x, y) = tokio::join!(b.replay_dead(&q, &ids), b2.replay_dead(&q, &ids));
    assert_eq!(x.unwrap() + y.unwrap(), 20);
    assert_eq!(b.queue_stats(&q).await.unwrap().stream, 20);
}

#[tokio::test]
async fn replay_skips_missing_and_undecodable_entries() {
    let (b, q) = setup().await;
    let mut c = redis::Client::open(url()).unwrap().get_multiplexed_async_connection().await.unwrap();
    let _: String = redis::cmd("XADD").arg(stream_key(&q)).arg("*").arg("h").arg(b"garbage".as_slice()).arg("p").arg(b"".as_slice())
        .query_async(&mut c).await.unwrap();
    assert!(b.fetch(&[q.clone()], 10, 100).await.unwrap().is_empty(), "undecodable entries are buried on fetch");
    let dl = b.dead_letters(&q, None, None, 10).await.unwrap();
    assert_eq!(dl.len(), 1);
    assert!(dl[0].header.is_none());
    assert_eq!(dl[0].raw_header, b"garbage");
    assert_eq!(b.replay_dead(&q, &[dl[0].entry_id.clone(), "1-1".into()]).await.unwrap(), 0);
    assert_eq!(b.dead_letters(&q, None, None, 10).await.unwrap().len(), 1, "left in place");
}

#[tokio::test]
async fn workers_lists_live_heartbeats() {
    let (b, q) = setup().await;
    let (a, z) = (uniq("w"), uniq("w"));
    for id in [&a, &z] {
        let info = WorkerInfo { worker_id: id.clone(), hostname: "h".into(), pid: 1, queues: vec![q.clone()],
                                concurrency: 2, inflight: 0, started_at: 1, last_seen: 2, tasks: vec![] };
        b.heartbeat(&info, 10_000).await.unwrap();
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
async fn known_queues_finds_queues_in_redis() {
    let (b, q) = setup().await;
    let delayed_only = uniq("q");
    let mut h = Header::new("t", &delayed_only, 0, "ct");
    h.eta = Some(now_ms() + 60_000);
    b.enqueue(&h, b"").await.unwrap();
    let qs = b.known_queues().await.unwrap();
    assert!(qs.contains(&q) && qs.contains(&delayed_only));
    assert!(qs.windows(2).all(|p| p[0] < p[1]), "sorted and unique");
}

fn running_task(id: &str, progress: bool) -> mayu_core::RunningTask {
    mayu_core::RunningTask {
        id: id.into(), task: "t".into(), queue: "q".into(), workflow_id: Some("wf".into()), node: Some("1".into()),
        started_ms: 1_000, done: progress.then_some(3.0), total: progress.then_some(12.0),
        fraction: progress.then_some(0.25), remaining_s: progress.then_some(90.5),
    }
}

fn worker(id: &str, tasks: Vec<mayu_core::RunningTask>) -> WorkerInfo {
    WorkerInfo {
        worker_id: id.into(), hostname: "h".into(), pid: 7, queues: vec!["q".into()], concurrency: 2,
        inflight: tasks.len(), started_at: 1, last_seen: 2, tasks,
    }
}

async fn has_tasks_field(id: &str) -> bool {
    redis::cmd("HEXISTS").arg(worker_key(id)).arg("tasks").query_async(&mut raw().await).await.unwrap()
}

#[tokio::test]
async fn heartbeat_publishes_running_tasks_and_removes_them_when_idle() {
    let (b, _q) = setup().await;
    let id = uniq("w");
    let tasks = vec![running_task("a", true), running_task("b", false)];
    let info = worker(&id, tasks.clone());
    b.heartbeat(&info, 60_000).await.unwrap();
    assert_eq!(b.running(&id).await.unwrap(), tasks);
    assert_eq!(b.worker_info(&id).await.unwrap(), Some(info));
    let listed = b.workers().await.unwrap().into_iter().find(|w| w.worker_id == id).unwrap();
    assert_eq!(listed.tasks, tasks);
    assert!(has_tasks_field(&id).await);
    // Idle: the field is deleted, not left stale, and the other fields are still refreshed.
    b.heartbeat(&worker(&id, vec![]), 60_000).await.unwrap();
    assert!(!has_tasks_field(&id).await);
    assert!(b.running(&id).await.unwrap().is_empty());
    assert_eq!(b.worker_info(&id).await.unwrap().unwrap().pid, 7);
    // Unknown and expired workers have none.
    assert!(b.running(&uniq("nope")).await.unwrap().is_empty());
    b.heartbeat(&worker(&id, vec![running_task("c", false)]), 100).await.unwrap();
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert!(b.running(&id).await.unwrap().is_empty());
}

#[tokio::test]
async fn heartbeat_with_tasks_is_readable_the_way_older_versions_read_it() {
    // 0.2.0 reads the worker hash with HGETALL into strings; a binary field would fail that conversion.
    let (b, _q) = setup().await;
    let id = uniq("w");
    b.heartbeat(&worker(&id, vec![running_task("a", true), running_task("b", false)]), 60_000).await.unwrap();
    let old: std::collections::HashMap<String, String> =
        redis::cmd("HGETALL").arg(worker_key(&id)).query_async(&mut raw().await).await.unwrap();
    assert_eq!(old["hostname"], "h");
    assert_eq!(old["pid"], "7");
    assert_eq!(old["queues"], "q");
    assert_eq!(old["concurrency"], "2");
    assert_eq!(old["inflight"], "2");
    assert_eq!(old["started_at"], "1");
    assert_eq!(old["last_seen"], "2");
    assert!(old.contains_key("tasks"));
}

#[tokio::test]
async fn publishes_a_full_batch_of_tasks() {
    let (b, _q) = setup().await;
    let id = uniq("w");
    let tasks: Vec<_> = (0..mayu_core::MAX_PUBLISHED).map(|i| running_task(&format!("id{i}"), i % 2 == 0)).collect();
    b.heartbeat(&worker(&id, tasks.clone()), 60_000).await.unwrap();
    assert_eq!(b.running(&id).await.unwrap(), tasks);
}

#[tokio::test]
async fn worker_hash_from_an_older_version_has_no_tasks() {
    let (b, _q) = setup().await;
    let id = uniq("w");
    let _: i64 = redis::cmd("HSET").arg(worker_key(&id))
        .arg("hostname").arg("old").arg("pid").arg(9).arg("queues").arg("a,b").arg("concurrency").arg(4)
        .arg("inflight").arg(1).arg("started_at").arg(10).arg("last_seen").arg(20)
        .query_async(&mut raw().await).await.unwrap();
    let w = b.workers().await.unwrap().into_iter().find(|w| w.worker_id == id).unwrap();
    assert_eq!((w.hostname.as_str(), w.pid, w.concurrency, w.inflight, w.started_at, w.last_seen),
               ("old", 9, 4, 1, 10, 20));
    assert_eq!(w.queues, vec!["a".to_string(), "b".to_string()]);
    assert!(w.tasks.is_empty());
    assert!(b.running(&id).await.unwrap().is_empty());
}

#[tokio::test]
async fn malformed_tasks_field_reads_as_empty_without_an_error() {
    let (b, _q) = setup().await;
    let id = uniq("w");
    b.heartbeat(&worker(&id, vec![]), 60_000).await.unwrap();
    // Not hex, odd length, empty, hex of garbage, hex of msgpack that is not a task list, non-ASCII text.
    for bad in ["not hex at all", "abc", "", "fffe00", "93010203", "caf\u{e9}"] {
        let _: i64 = redis::cmd("HSET").arg(worker_key(&id)).arg("tasks").arg(bad)
            .query_async(&mut raw().await).await.unwrap();
        assert!(b.running(&id).await.unwrap().is_empty());
        let w = b.worker_info(&id).await.unwrap().unwrap();
        assert!(w.tasks.is_empty());
        assert_eq!(w.pid, 7, "the other fields still parse");
        assert!(b.workers().await.unwrap().iter().any(|w| w.worker_id == id));
    }
    // The next heartbeat replaces it.
    b.heartbeat(&worker(&id, vec![running_task("a", false)]), 60_000).await.unwrap();
    assert_eq!(b.running(&id).await.unwrap().len(), 1);
}

#[tokio::test]
async fn running_all_lists_every_live_workers_tasks_and_survives_a_malformed_field() {
    let (b, _q) = setup().await;
    let (w1, w2, bad, idle) = (uniq("w1"), uniq("w2"), uniq("bad"), uniq("idle"));
    let (t1, t2) = (vec![running_task("a", true), running_task("b", false)], vec![running_task("c", true)]);
    b.heartbeat(&worker(&w1, t1.clone()), 60_000).await.unwrap();
    b.heartbeat(&worker(&w2, t2.clone()), 60_000).await.unwrap();
    b.heartbeat(&worker(&idle, vec![]), 60_000).await.unwrap();
    b.heartbeat(&worker(&bad, vec![]), 60_000).await.unwrap();
    let ours = |all: &mayu_core::PublishedTasks, w: &str| -> Vec<mayu_core::RunningTask> {
        all.tasks.iter().filter(|(id, _)| id == w).map(|(_, t)| t.clone()).collect()
    };

    let all = b.running_all().await.unwrap();
    assert_eq!(ours(&all, &w1), t1);
    assert_eq!(ours(&all, &w2), t2);
    assert!(ours(&all, &idle).is_empty() && ours(&all, &bad).is_empty());

    // One worker writes garbage: it is counted, and the others still read. (Workers other tests left behind
    // may add to the count.)
    let _: i64 = redis::cmd("HSET").arg(worker_key(&bad)).arg("tasks").arg("not hex at all")
        .query_async(&mut raw().await).await.unwrap();
    let again = b.running_all().await.unwrap();
    assert!(again.malformed > all.malformed, "{} vs {}", again.malformed, all.malformed);
    assert_eq!(ours(&again, &w1), t1);
    assert_eq!(ours(&again, &w2), t2);
    assert!(ours(&again, &bad).is_empty());

    // A worker that leaves, or whose beat lapses, is not listed.
    b.unregister(&w1).await.unwrap();
    assert!(ours(&b.running_all().await.unwrap(), &w1).is_empty());
}

// A worker that lost a task to a reclaim must not settle it (plan 30). Each test: A fetches, B reclaims,
// then A settles (stale) and, where relevant, B settles.

fn stored(id: &str, record: &str) -> Option<StoredResult> {
    Some(StoredResult { id: id.to_string(), record: record.as_bytes().to_vec(), ttl_secs: 60 })
}

/// Two workers on one queue; A has fetched a task and B has reclaimed it.
async fn a_lost_to_b() -> (RedisBroker, RedisBroker, String, mayu_core::Delivery, mayu_core::Delivery) {
    let (a, q) = setup().await;
    let b = RedisBroker::connect(&url(), uniq("c")).await.unwrap();
    a.enqueue(&Header::new("t", &q, 3, "ct"), b"x").await.unwrap();
    let da = a.fetch(std::slice::from_ref(&q), 1, 100).await.unwrap().remove(0);
    let db = b.reclaim(&q, 0, "0-0", 10).await.unwrap().deliveries.remove(0);
    assert_eq!((db.stream_id.as_str(), db.delivery_count), (da.stream_id.as_str(), 2));
    (a, b, q, da, db)
}

async fn drain(b: &RedisBroker, q: &str) -> Vec<mayu_core::Delivery> {
    b.fetch(&[q.to_string()], 10, 50).await.unwrap()
}

#[tokio::test]
async fn stale_retry_leaves_the_new_owners_entry() {
    let (a, b, q, da, _) = a_lost_to_b().await;
    let mut next = da.header.clone();
    next.attempt = 1;
    next.eta = Some(now_ms());
    assert!(!a.retry(&da, &next).await.unwrap());
    let s = b.queue_stats(&q).await.unwrap();
    assert_eq!((s.pending, s.delayed), (1, 0), "stale retry must not ack B's entry or schedule a second run");
}

#[tokio::test]
async fn stale_dead_letter_does_not_overwrite_the_new_owners_success() {
    let (a, b, q, da, db) = a_lost_to_b().await;
    let id = &da.header.id;
    assert!(!a.dead_letter(&da, "boom", stored(id, "failure")).await.unwrap());
    let s = b.queue_stats(&q).await.unwrap();
    assert_eq!((s.pending, s.dead), (1, 0), "stale dead_letter must leave B's entry pending");
    assert!(b.ack(&db, stored(id, "success"), &[]).await.unwrap());
    assert_eq!(b.queue_stats(&q).await.unwrap().dead, 0, "a task B completed must not be dead-lettered");
    assert_eq!(b.get_result(id).await.unwrap(), Some(b"success".to_vec()));
}

#[tokio::test]
async fn stale_requeue_adds_no_second_copy() {
    let (a, b, q, da, _) = a_lost_to_b().await;
    assert!(!a.requeue(&da).await.unwrap());
    let s = b.queue_stats(&q).await.unwrap();
    assert_eq!((s.stream, s.pending), (1, 1), "stale requeue must leave only B's pending entry");
    assert!(drain(&b, &q).await.is_empty(), "no second copy to fetch");
}

#[tokio::test]
async fn stale_ack_is_overwritten_by_the_owner_and_adds_no_follow_ups() {
    let (a, b, q, da, db) = a_lost_to_b().await;
    let id = &da.header.id;
    let cbq = uniq("q");
    b.ensure_queues(std::slice::from_ref(&cbq)).await.unwrap();
    let (cb_a, cb_b) = (Header::new("cb-a", &cbq, 0, "ct"), Header::new("cb-b", &cbq, 0, "ct"));
    assert!(!a.ack(&da, stored(id, "a"), &[(cb_a, b"a".to_vec())]).await.unwrap());
    assert_eq!(b.queue_stats(&q).await.unwrap().pending, 1, "stale ack must leave B's entry pending");
    assert!(b.ack(&db, stored(id, "b"), &[(cb_b, b"b".to_vec())]).await.unwrap());
    assert_eq!(b.get_result(id).await.unwrap(), Some(b"b".to_vec()));
    let follow = drain(&b, &cbq).await;
    assert_eq!(follow.iter().map(|d| d.header.task.as_str()).collect::<Vec<_>>(), ["cb-b"], "added once, by B");
}

#[tokio::test]
async fn stale_ack_after_the_owner_keeps_its_result_and_adds_no_follow_ups() {
    let (a, b, _, da, db) = a_lost_to_b().await;
    let id = &da.header.id;
    let cbq = uniq("q");
    b.ensure_queues(std::slice::from_ref(&cbq)).await.unwrap();
    let (cb_a, cb_b) = (Header::new("cb-a", &cbq, 0, "ct"), Header::new("cb-b", &cbq, 0, "ct"));
    assert!(b.ack(&db, stored(id, "b"), &[(cb_b, b"b".to_vec())]).await.unwrap());
    assert!(!a.ack(&da, stored(id, "a"), &[(cb_a, b"a".to_vec())]).await.unwrap());
    assert_eq!(b.get_result(id).await.unwrap(), Some(b"b".to_vec()), "a late stale ack must not overwrite");
    let follow = drain(&b, &cbq).await;
    assert_eq!(follow.iter().map(|d| d.header.task.as_str()).collect::<Vec<_>>(), ["cb-b"]);
}

#[tokio::test]
async fn retry_repeated_after_it_applied_schedules_once() {
    let (b, q) = setup().await;
    b.enqueue(&Header::new("t", &q, 3, "ct"), b"x").await.unwrap();
    let d = b.fetch(std::slice::from_ref(&q), 1, 100).await.unwrap().remove(0);
    let mut next = d.header.clone();
    next.attempt = 1;
    next.eta = Some(now_ms() + 60_000);
    assert!(b.retry(&d, &next).await.unwrap());
    assert!(!b.retry(&d, &next).await.unwrap()); // the first reply was lost
    assert_eq!(b.queue_stats(&q).await.unwrap().delayed, 1);
}

#[tokio::test]
async fn dead_letter_repeated_after_it_applied_buries_once() {
    let (b, q) = setup().await;
    b.enqueue(&Header::new("t", &q, 0, "ct"), b"x").await.unwrap();
    let d = b.fetch(std::slice::from_ref(&q), 1, 100).await.unwrap().remove(0);
    assert!(b.dead_letter(&d, "boom", None).await.unwrap());
    assert!(!b.dead_letter(&d, "boom", None).await.unwrap());
    assert_eq!(b.queue_stats(&q).await.unwrap().dead, 1);
}

#[tokio::test]
async fn requeue_repeated_after_it_applied_adds_one_copy() {
    let (b, q) = setup().await;
    b.enqueue(&Header::new("t", &q, 0, "ct"), b"x").await.unwrap();
    let d = b.fetch(std::slice::from_ref(&q), 1, 100).await.unwrap().remove(0);
    assert!(b.requeue(&d).await.unwrap());
    assert!(!b.requeue(&d).await.unwrap());
    assert_eq!(b.queue_stats(&q).await.unwrap().stream, 1);
}

#[tokio::test]
async fn known_queues_keeps_a_dead_stream_after_replay() {
    let (b, _) = setup().await;
    let q = uniq("q"); // never ensured: only its dead stream makes it known
    b.enqueue(&Header::new("t", &q, 0, "ct"), b"").await.unwrap();
    b.ensure_queues(std::slice::from_ref(&q)).await.unwrap();
    let d = b.fetch(std::slice::from_ref(&q), 1, 100).await.unwrap().remove(0);
    b.dead_letter(&d, "e", None).await.unwrap();
    let _: i64 = redis::cmd("DEL").arg(stream_key(&q)).query_async(&mut raw().await).await.unwrap();
    let id = b.last_dead_id(&q).await.unwrap().unwrap();
    b.replay_dead(&q, &[id]).await.unwrap();
    let _: i64 = redis::cmd("DEL").arg(stream_key(&q)).query_async(&mut raw().await).await.unwrap();
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
