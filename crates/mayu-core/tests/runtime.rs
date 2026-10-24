use std::time::{Duration, Instant};

use mayu_core::{
    Header, Link, MemoryBroker, Outcome, Resources, ResultRecord, RetryPolicy, Runtime, RuntimeConfig, now_ms,
};

fn cfg(concurrency: usize, prefetch: usize) -> RuntimeConfig {
    RuntimeConfig {
        queues: vec!["q".into()],
        concurrency,
        prefetch_multiplier: prefetch,
        result_ttl_secs: Some(60),
        fetch_block_ms: 50,
        promote_interval_ms: 20,
        retry: RetryPolicy { base_ms: 10, cap_ms: 50 },
        visibility_timeout_ms: 10_000,
        reclaim_interval_ms: 50,
        heartbeat_interval_ms: 50,
        max_deliveries: 5,
        ..RuntimeConfig::default()
    }
}

fn start(mem: &MemoryBroker, c: RuntimeConfig) -> Runtime<MemoryBroker> {
    let m = mem.clone();
    Runtime::start(move || async move { Ok(m) }, c).unwrap()
}

fn wait_until(mut f: impl FnMut() -> bool) {
    let t = Instant::now();
    while !f() {
        assert!(t.elapsed() < Duration::from_secs(3), "condition not reached in time");
        std::thread::sleep(Duration::from_millis(10));
    }
}

const RECV: Duration = Duration::from_secs(3);

#[test]
fn success_acks_and_stores_result() {
    let mem = MemoryBroker::default();
    let h = Header::new("t", "q", 3, "application/msgpack");
    mem.push(&h, b"p");
    let rt = start(&mem, cfg(2, 2));
    let t = rt.recv_timeout(RECV).expect("task");
    assert_eq!((t.id.as_str(), t.task.as_str(), t.payload.as_slice()), (h.id.as_str(), "t", &b"p"[..]));
    rt.complete(t.token, Outcome::Success { result: Some(b"r".to_vec()), links: vec![] });
    wait_until(|| mem.pending_len() == 0 && mem.result(&h.id).is_some());
    let rec = ResultRecord::decode(&mem.result(&h.id).unwrap()).unwrap();
    assert_eq!(rec.status, "success");
    assert_eq!(rec.value.as_deref(), Some(&b"r"[..]));
    rt.shutdown(Duration::from_secs(1));
}

#[test]
fn retry_redelivers_with_incremented_attempt() {
    let mem = MemoryBroker::default();
    mem.push(&Header::new("t", "q", 3, "ct"), b"");
    let rt = start(&mem, cfg(1, 1));
    let t = rt.recv_timeout(RECV).unwrap();
    rt.complete(t.token, Outcome::Retry { countdown_ms: Some(0), error: "e".into() });
    let t2 = rt.recv_timeout(RECV).expect("redelivered");
    assert_eq!(t2.id, t.id);
    assert_eq!(t2.attempt, 1);
    rt.shutdown(Duration::from_secs(1));
}

#[test]
fn retry_exhausted_dead_letters_and_stores_failure() {
    let mem = MemoryBroker::default();
    let h = Header::new("t", "q", 0, "ct");
    mem.push(&h, b"");
    let rt = start(&mem, cfg(1, 1));
    let t = rt.recv_timeout(RECV).unwrap();
    rt.complete(t.token, Outcome::Retry { countdown_ms: None, error: "boom".into() });
    wait_until(|| mem.dead().len() == 1);
    assert!(mem.dead()[0].error.contains("retries exhausted"));
    let rec = ResultRecord::decode(&mem.result(&h.id).unwrap()).unwrap();
    assert_eq!(rec.status, "failure");
    rt.shutdown(Duration::from_secs(1));
}

#[test]
fn expired_task_is_dropped_before_running() {
    let mem = MemoryBroker::default();
    let mut h = Header::new("t", "q", 3, "ct");
    h.expires = Some(now_ms().saturating_sub(1_000)); // deadline already past
    mem.push(&h, b"p");
    let rt = start(&mem, cfg(2, 2));
    // never handed to a worker thread
    assert!(rt.recv_timeout(Duration::from_millis(500)).is_none());
    // acked with a stored "task expired" failure, queue drained
    wait_until(|| mem.pending_len() == 0 && mem.result(&h.id).is_some());
    let rec = ResultRecord::decode(&mem.result(&h.id).unwrap()).unwrap();
    assert_eq!(rec.status, "failure");
    assert!(rec.error.as_deref().unwrap_or("").contains("task expired"), "got {:?}", rec.error);
    rt.shutdown(Duration::from_secs(1));
}

#[test]
fn not_yet_expired_task_runs_normally() {
    let mem = MemoryBroker::default();
    let mut h = Header::new("t", "q", 3, "ct");
    h.expires = Some(now_ms() + 60_000); // deadline well in the future
    mem.push(&h, b"p");
    let rt = start(&mem, cfg(2, 2));
    let t = rt.recv_timeout(RECV).expect("task should be delivered");
    assert_eq!(t.id, h.id);
    rt.complete(t.token, Outcome::Success { result: None, links: vec![] });
    wait_until(|| mem.pending_len() == 0);
    rt.shutdown(Duration::from_secs(1));
}

#[test]
fn failure_dead_letters() {
    let mem = MemoryBroker::default();
    mem.push(&Header::new("t", "q", 5, "ct"), b"");
    let rt = start(&mem, cfg(1, 1));
    let t = rt.recv_timeout(RECV).unwrap();
    rt.complete(t.token, Outcome::Failure { error: "E".into(), traceback: "tb".into() });
    wait_until(|| mem.dead().len() == 1 && mem.pending_len() == 0);
    rt.shutdown(Duration::from_secs(1));
}

#[test]
fn prefetch_caps_inflight() {
    let mem = MemoryBroker::default();
    for _ in 0..5 { mem.push(&Header::new("t", "q", 0, "ct"), b""); }
    let rt = start(&mem, cfg(1, 2));
    wait_until(|| mem.pending_len() == 2);
    std::thread::sleep(Duration::from_millis(150));
    assert_eq!(mem.pending_len(), 2, "must not claim beyond concurrency * prefetch");
    assert_eq!(mem.stream_len("q"), 3);
    let t = rt.recv_timeout(RECV).unwrap();
    rt.complete(t.token, Outcome::Success { result: None, links: vec![] });
    wait_until(|| mem.stream_len("q") == 2 && mem.pending_len() == 2);
    rt.shutdown(Duration::from_secs(1));
}

#[test]
fn unknown_token_is_ignored() {
    let mem = MemoryBroker::default();
    let rt = start(&mem, cfg(1, 1));
    rt.complete(999_999, Outcome::Success { result: None, links: vec![] });
    mem.push(&Header::new("t", "q", 0, "ct"), b"");
    assert!(rt.recv_timeout(RECV).is_some(), "runtime still works after a stray completion");
    rt.shutdown(Duration::from_secs(1));
}

#[test]
fn delayed_task_is_promoted_when_due() {
    let mem = MemoryBroker::default();
    let mut h = Header::new("t", "q", 0, "ct");
    h.eta = Some(now_ms() + 300);
    mem.push(&h, b"");
    let rt = start(&mem, cfg(1, 1));
    assert!(rt.recv_timeout(Duration::from_millis(100)).is_none(), "not due yet");
    assert_eq!(rt.recv_timeout(RECV).expect("promoted").id, h.id);
    rt.shutdown(Duration::from_secs(1));
}

#[test]
fn shutdown_requeues_unstarted() {
    let mem = MemoryBroker::default();
    for _ in 0..3 { mem.push(&Header::new("t", "q", 0, "ct"), b""); }
    let rt = start(&mem, cfg(1, 3));
    wait_until(|| mem.pending_len() == 3);
    let _running = rt.recv_timeout(RECV).unwrap(); // started, never completed
    rt.shutdown(Duration::from_millis(200));
    assert_eq!(mem.stream_len("q"), 2, "two unstarted tasks returned to the stream");
    assert_eq!(mem.pending_len(), 1, "the running task stays pending for reclaim");
    rt.shutdown(Duration::from_millis(10)); // idempotent
}

#[test]
fn rejects_zero_concurrency() {
    let mem = MemoryBroker::default();
    let m = mem.clone();
    assert!(Runtime::start(move || async move { Ok(m) }, cfg(0, 1)).is_err());
}

#[test]
fn shutdown_is_prompt_while_broker_is_failing() {
    let mem = MemoryBroker::default();
    let rt = start(&mem, cfg(1, 1));
    mem.set_failing(true);
    std::thread::sleep(Duration::from_millis(60)); // fetch loop is now in its 500ms error backoff
    let t = Instant::now();
    rt.shutdown(Duration::from_millis(100));
    assert!(t.elapsed() < Duration::from_millis(250), "shutdown took {:?}", t.elapsed());
}

use mayu_core::Broker;

fn rel_cfg(worker: &str, visibility_ms: u64, max_deliveries: u64) -> RuntimeConfig {
    RuntimeConfig {
        worker_id: worker.into(),
        visibility_timeout_ms: visibility_ms,
        reclaim_interval_ms: 30,
        heartbeat_interval_ms: 30,
        max_deliveries,
        ..cfg(1, 1)
    }
}

fn start_as(mem: &MemoryBroker, worker: &str, c: RuntimeConfig) -> Runtime<MemoryBroker> {
    let m = mem.with_consumer(worker);
    Runtime::start(move || async move { Ok(m) }, c).unwrap()
}

#[test]
fn running_task_is_kept_alive_and_not_stolen() {
    let mem = MemoryBroker::default();
    mem.push(&Header::new("t", "q", 0, "ct"), b"");
    let a = start_as(&mem, "a", rel_cfg("a", 200, 5));
    let t = a.recv_timeout(RECV).unwrap();
    let b = start_as(&mem, "b", rel_cfg("b", 200, 5));
    assert!(b.recv_timeout(Duration::from_millis(700)).is_none(), "b stole a task a is still running");
    a.complete(t.token, Outcome::Success { result: None, links: vec![] });
    wait_until(|| mem.pending_len() == 0);
    a.shutdown(Duration::from_secs(1));
    b.shutdown(Duration::from_secs(1));
}

#[test]
fn crashed_workers_task_is_reclaimed() {
    let mem = MemoryBroker::default();
    let h = Header::new("t", "q", 0, "ct");
    mem.push(&h, b"");
    let a = start_as(&mem, "a", rel_cfg("a", 200, 5));
    let _never_completed = a.recv_timeout(RECV).unwrap();
    drop(a); // crash: no ack, no keepalive, no unregister
    let b = start_as(&mem, "b", rel_cfg("b", 200, 5));
    let t = b.recv_timeout(RECV).expect("reclaimed after visibility timeout");
    assert_eq!(t.id, h.id);
    b.complete(t.token, Outcome::Success { result: None, links: vec![] });
    wait_until(|| mem.pending_len() == 0);
    b.shutdown(Duration::from_secs(1));
}

#[test]
fn poison_message_is_dead_lettered_without_running() {
    let mem = MemoryBroker::default();
    let h = Header::new("t", "q", 0, "ct");
    mem.push(&h, b"");
    let a = start_as(&mem, "a", rel_cfg("a", 200, 1));
    let _ = a.recv_timeout(RECV).unwrap();
    drop(a);
    let b = start_as(&mem, "b", rel_cfg("b", 200, 1));
    assert!(b.recv_timeout(Duration::from_millis(800)).is_none(), "poison message must not run");
    wait_until(|| mem.dead().len() == 1 && mem.pending_len() == 0);
    assert!(mem.dead()[0].error.contains("poison"));
    let rec = ResultRecord::decode(&mem.result(&h.id).unwrap()).unwrap();
    assert_eq!(rec.status, "failure");
    b.shutdown(Duration::from_secs(1));
}

#[test]
fn lost_ownership_still_completes() {
    let mem = MemoryBroker::default();
    mem.push(&Header::new("t", "q", 0, "ct"), b"");
    let a = start_as(&mem, "a", rel_cfg("a", 10_000, 5));
    let t = a.recv_timeout(RECV).unwrap();
    // Another worker steals the entry, as if `a` had stalled past the visibility timeout.
    let thief = mem.with_consumer("thief");
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    assert_eq!(rt.block_on(thief.reclaim("q", 0, "0-0", 10)).unwrap().deliveries.len(), 1);
    a.complete(t.token, Outcome::Success { result: None, links: vec![] });
    wait_until(|| mem.pending_len() == 0);
    a.shutdown(Duration::from_secs(1));
}

#[test]
fn heartbeat_registers_while_running_and_unregisters_on_shutdown() {
    let mem = MemoryBroker::default();
    let rt = start_as(&mem, "hb", rel_cfg("hb", 10_000, 5));
    assert_eq!(rt.worker_id(), "hb");
    wait_until(|| mem.worker("hb").is_some());
    let w = mem.worker("hb").unwrap();
    assert_eq!(w.queues, vec!["q".to_string()]);
    assert_eq!(w.pid, std::process::id());
    assert_eq!(w.concurrency, 1);
    rt.shutdown(Duration::from_secs(1));
    assert!(mem.worker("hb").is_none());
}

#[test]
fn outcome_is_recorded_after_transient_broker_failure() {
    let mem = MemoryBroker::default();
    let h = Header::new("t", "q", 0, "ct");
    mem.push(&h, b"");
    let rt = start(&mem, cfg(1, 1));
    let t = rt.recv_timeout(RECV).unwrap();
    mem.set_failing(true);
    rt.complete(t.token, Outcome::Success { result: Some(b"r".to_vec()), links: vec![] });
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(mem.pending_len(), 1, "not recorded while the broker is down");
    mem.set_failing(false);
    wait_until(|| mem.pending_len() == 0 && mem.result(&h.id).is_some());
    rt.shutdown(Duration::from_secs(1));
}

#[test]
fn rejects_zero_intervals() {
    let mem = MemoryBroker::default();
    let m = mem.clone();
    let c = RuntimeConfig { visibility_timeout_ms: 0, ..cfg(1, 1) };
    assert!(Runtime::start(move || async move { Ok(m) }, c).is_err());
}

fn two_queues(strict: bool, concurrency: usize, prefetch: usize) -> RuntimeConfig {
    RuntimeConfig { queues: vec!["hi".into(), "lo".into()], strict_priority: strict, ..cfg(concurrency, prefetch) }
}

fn limited(rate: f64, concurrency: usize, prefetch: usize) -> RuntimeConfig {
    RuntimeConfig { rate_limits: [("slow".to_string(), rate)].into(), ..cfg(concurrency, prefetch) }
}

#[test]
fn strict_priority_runs_a_later_claimed_higher_queue_first() {
    let mem = MemoryBroker::default();
    for _ in 0..3 { mem.push(&Header::new("t", "lo", 0, "ct"), b""); }
    let rt = start(&mem, two_queues(true, 1, 8));
    wait_until(|| mem.pending_len() == 3);
    mem.push(&Header::new("t", "hi", 0, "ct"), b"");
    // Claimed by the broker is not yet handed to the dispatcher: wait for the dispatcher to hold all four.
    wait_until(|| rt.queued() == 4);
    assert_eq!(rt.recv_timeout(RECV).unwrap().queue, "hi");
    rt.shutdown(Duration::from_secs(1));
}

#[test]
fn equal_priority_takes_turns_between_queues() {
    let mem = MemoryBroker::default();
    for _ in 0..4 {
        mem.push(&Header::new("t", "hi", 0, "ct"), b"");
        mem.push(&Header::new("t", "lo", 0, "ct"), b"");
    }
    let rt = start(&mem, two_queues(false, 1, 1));
    let mut seen = Vec::new();
    for _ in 0..4 {
        let t = rt.recv_timeout(RECV).unwrap();
        seen.push(t.queue.clone());
        rt.complete(t.token, Outcome::Success { result: None, links: vec![] });
    }
    assert!(seen.iter().any(|q| q == "hi") && seen.iter().any(|q| q == "lo"), "{seen:?}");
    rt.shutdown(Duration::from_secs(1));
}

#[test]
fn rate_limit_spaces_task_starts() {
    let mem = MemoryBroker::default();
    for _ in 0..3 { mem.push(&Header::new("slow", "q", 0, "ct"), b""); }
    let rt = start(&mem, limited(10.0, 4, 4));
    let mut at = Vec::new();
    for _ in 0..3 {
        let t = rt.recv_timeout(RECV).unwrap();
        at.push(Instant::now());
        rt.complete(t.token, Outcome::Success { result: None, links: vec![] });
    }
    for w in at.windows(2) {
        assert!(w[1] - w[0] >= Duration::from_millis(80), "starts {:?} apart", w[1] - w[0]);
    }
    rt.shutdown(Duration::from_secs(1));
}

#[test]
fn rate_limited_backlog_goes_back_to_the_broker_and_frees_prefetch() {
    let mem = MemoryBroker::default();
    for _ in 0..3 { mem.push(&Header::new("slow", "q", 0, "ct"), b""); }
    mem.push(&Header::new("fast", "q", 0, "ct"), b"");
    // one prefetch slot: without deferral the second "slow" would hold it for 2 s
    let rt = start(&mem, limited(0.5, 1, 1));
    let first = rt.recv_timeout(RECV).unwrap();
    assert_eq!(first.task, "slow");
    rt.complete(first.token, Outcome::Success { result: None, links: vec![] });
    let t = Instant::now();
    let next = rt.recv_timeout(RECV).unwrap();
    assert_eq!(next.task, "fast", "an unthrottled task must not wait behind the throttled backlog");
    assert!(t.elapsed() < Duration::from_secs(1));
    assert_eq!(mem.delayed_len("q"), 2);
    rt.shutdown(Duration::from_secs(1));
}

#[test]
fn shutdown_requeues_tasks_waiting_on_the_rate_limit() {
    let mem = MemoryBroker::default();
    for _ in 0..2 { mem.push(&Header::new("slow", "q", 0, "ct"), b""); }
    let rt = start(&mem, limited(2.0, 2, 2));
    let _running = rt.recv_timeout(RECV).unwrap();
    wait_until(|| mem.pending_len() == 2); // the second is claimed and waiting for its slot
    let t = Instant::now();
    rt.shutdown(Duration::from_secs(1));
    assert!(t.elapsed() < Duration::from_millis(900), "shutdown waited for the rate limit");
    assert_eq!(mem.stream_len("q"), 1, "the waiting task went back to the queue");
    assert_eq!(mem.pending_len(), 1, "the running one stays pending for reclaim");
}

#[test]
fn failed_defer_holds_the_task_until_its_slot() {
    let mem = MemoryBroker::default();
    mem.fail_retries(true); // deferring a task back to the broker is rejected
    for _ in 0..2 { mem.push(&Header::new("slow", "q", 0, "ct"), b""); }
    let rt = start(&mem, limited(0.5, 2, 2));
    let first = rt.recv_timeout(RECV).unwrap();
    let t = Instant::now();
    // the second is claimed and (its deferral having failed) held here, not in the delayed set
    wait_until(|| mem.pending_len() == 2);
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(mem.delayed_len("q"), 0, "a failed deferral must not reach the delayed set");
    assert_eq!(mem.pending_len(), 2, "the held task stays claimed");
    mem.fail_retries(false);
    // still delivered, once its ~2 s slot arrives
    let second = rt.recv_timeout(Duration::from_secs(4)).expect("held task is delivered at its slot");
    assert!(t.elapsed() >= Duration::from_millis(1500), "delivered {:?} after the first", t.elapsed());
    assert_ne!(first.token, second.token);
    rt.complete(first.token, Outcome::Success { result: None, links: vec![] });
    rt.complete(second.token, Outcome::Success { result: None, links: vec![] });
    wait_until(|| mem.pending_len() == 0);
    rt.shutdown(Duration::from_secs(1));
}

#[test]
fn blocking_fetch_overshoot_is_paid_back() {
    let mem = MemoryBroker::default();
    mem.fetch_extra(3); // a fetch for 1 returns 4
    for _ in 0..12 { mem.push(&Header::new("t", "q", 0, "ct"), b""); }
    let rt = start(&mem, cfg(1, 1)); // one permit
    wait_until(|| mem.pending_len() == 4);
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(mem.pending_len(), 4, "claims never exceed cap + extra");
    assert_eq!(mem.stream_len("q"), 8, "no further fetch while the overshoot is outstanding");
    // completions repay the 3 overshoot permits before any new claim
    for done in 1..=3 {
        let t = rt.recv_timeout(RECV).unwrap();
        rt.complete(t.token, Outcome::Success { result: None, links: vec![] });
        wait_until(|| mem.pending_len() == 4 - done);
        std::thread::sleep(Duration::from_millis(200));
        assert_eq!(mem.pending_len(), 4 - done, "no new fetch while permits are still owed");
        assert_eq!(mem.stream_len("q"), 8);
    }
    // the last claimed task holds the only permit; finishing it lets the next batch in
    let last = rt.recv_timeout(RECV).unwrap();
    rt.complete(last.token, Outcome::Success { result: None, links: vec![] });
    wait_until(|| mem.stream_len("q") == 4);
    assert!(mem.pending_len() <= 4);
    rt.shutdown(Duration::from_secs(1));
}

#[test]
fn rejects_unusable_rate_limits() {
    let mem = MemoryBroker::default();
    let m = mem.clone();
    let r = Runtime::start(move || async move { Ok(m) }, limited(0.0, 1, 1));
    assert!(matches!(r, Err(mayu_core::Error::Config(_))));
}

#[test]
fn link_callbacks_are_enqueued_with_the_ack() {
    let mem = MemoryBroker::default();
    let mut parent = Header::new("t", "q", 0, "application/json");
    parent.trace_ctx = Some("00-trace-01".into());
    mem.push(&parent, b"");
    let rt = start(&mem, cfg(1, 1));
    let t = rt.recv_timeout(RECV).unwrap();
    let link = Link { task: "cb".into(), queue: "q2".into(), max_retries: 2, payload: b"[[5],{}]".to_vec() };
    rt.complete(t.token, Outcome::Success { result: None, links: vec![link] });
    wait_until(|| mem.stream_len("q2") == 1 && mem.pending_len() == 0);
    let peek = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    let d = peek.block_on(mem.fetch(&["q2".into()], 1, 100)).unwrap().remove(0);
    assert_eq!((d.header.task.as_str(), d.header.queue.as_str()), ("cb", "q2"));
    assert_eq!((d.header.max_retries, d.header.attempt, d.header.eta), (2, 0, None));
    assert_eq!(d.header.content_type, "application/json", "callbacks use the parent's serializer");
    assert_eq!(d.header.trace_ctx.as_deref(), Some("00-trace-01"));
    assert_ne!(d.header.id, parent.id);
    assert_eq!(d.payload, b"[[5],{}]");
    rt.shutdown(Duration::from_secs(1));
}

fn ok() -> Outcome {
    Outcome::Success { result: None, links: vec![] }
}

#[test]
fn outcomes_are_recorded_concurrently() {
    let mem = MemoryBroker::default();
    mem.set_record_delay(Duration::from_millis(100));
    for _ in 0..16 { mem.push(&Header::new("t", "q", 0, "ct"), b""); }
    let rt = start(&mem, cfg(16, 1));
    let tasks: Vec<_> = (0..16).map(|_| rt.recv_timeout(RECV).expect("task")).collect();
    let first = Instant::now();
    for t in tasks { rt.complete(t.token, ok()); }
    // One at a time these take 16 * 100ms; concurrently about one delay.
    while !(mem.stream_len("q") == 0 && mem.pending_len() == 0) {
        assert!(first.elapsed() < Duration::from_millis(800), "only {} of 16 acked after {:?}", 16 - mem.pending_len(), first.elapsed());
        std::thread::sleep(Duration::from_millis(5));
    }
    rt.shutdown(Duration::from_secs(1));
}

#[test]
fn shutdown_drains_outstanding_writes() {
    let mem = MemoryBroker::default();
    mem.set_record_delay(Duration::from_millis(200));
    for _ in 0..8 { mem.push(&Header::new("t", "q", 0, "ct"), b""); }
    let rt = start(&mem, cfg(8, 1));
    let tasks: Vec<_> = (0..8).map(|_| rt.recv_timeout(RECV).expect("task")).collect();
    for t in tasks { rt.complete(t.token, ok()); }
    let t = Instant::now();
    rt.shutdown(Duration::from_secs(3));
    assert!(t.elapsed() < Duration::from_millis(1000), "drain took {:?}; writes are not concurrent", t.elapsed());
    assert_eq!(mem.pending_len(), 0, "shutdown returned with writes still outstanding");
    assert_eq!(mem.stream_len("q"), 0);
}

#[test]
fn failure_and_retry_outcomes_still_recorded_concurrently() {
    let mem = MemoryBroker::default();
    mem.set_record_delay(Duration::from_millis(50));
    let mut ok_ids = Vec::new();
    let mut failed_ids = Vec::new();
    for _ in 0..4 {
        let h = Header::new("t", "q", 3, "ct");
        ok_ids.push(h.id.clone());
        mem.push(&h, b"");
        let h = Header::new("t", "q", 3, "ct");
        failed_ids.push(h.id.clone());
        mem.push(&h, b"");
        mem.push(&Header::new("t", "q", 3, "ct"), b"");
    }
    let rt = start(&mem, cfg(12, 1));
    let tasks: Vec<_> = (0..12).map(|_| rt.recv_timeout(RECV).expect("task")).collect();
    for t in tasks {
        let outcome = if ok_ids.contains(&t.id) {
            Outcome::Success { result: Some(b"r".to_vec()), links: vec![] }
        } else if failed_ids.contains(&t.id) {
            Outcome::Failure { error: "E".into(), traceback: "tb".into() }
        } else {
            Outcome::Retry { countdown_ms: Some(60_000), error: "e".into() } // parked in the delayed set
        };
        rt.complete(t.token, outcome);
    }
    wait_until(|| mem.pending_len() == 0);
    // `wait_until` returns as soon as the last write lands; give any duplicate a chance to show.
    std::thread::sleep(Duration::from_millis(150));
    assert_eq!(mem.stream_len("q"), 0);
    assert_eq!(mem.pending_len(), 0);
    assert_eq!(mem.delayed_len("q"), 4, "each retry parked exactly once");
    assert_eq!(mem.dead().len(), 4, "each failure dead-lettered exactly once");
    for id in &ok_ids {
        let rec = ResultRecord::decode(&mem.result(id).expect("success result stored")).unwrap();
        assert_eq!(rec.status, "success");
    }
    for id in &failed_ids {
        let rec = ResultRecord::decode(&mem.result(id).expect("failure result stored")).unwrap();
        assert_eq!(rec.status, "failure");
    }
    rt.shutdown(Duration::from_secs(1));
}

fn gpu_cfg(concurrency: usize, prefetch: usize) -> RuntimeConfig {
    RuntimeConfig {
        resources: Resources {
            capacity: [("gpu".to_string(), 1.0)].into(),
            requests: [("g".to_string(), [("gpu".to_string(), 1.0)].into())].into(),
            ..Default::default()
        },
        ..cfg(concurrency, prefetch)
    }
}

#[test]
fn resource_request_holds_back_a_second_task_until_the_first_completes() {
    let mem = MemoryBroker::default();
    for _ in 0..2 { mem.push(&Header::new("g", "q", 0, "ct"), b""); }
    let rt = start(&mem, gpu_cfg(4, 2));
    let first = rt.recv_timeout(RECV).expect("first task");
    wait_until(|| rt.inflight() == 2); // the second is claimed and waiting, not missing
    assert!(rt.recv_timeout(Duration::from_millis(300)).is_none(), "the gpu is taken");
    rt.complete(first.token, Outcome::Success { result: None, links: vec![] });
    let second = rt.recv_timeout(RECV).expect("second task after release");
    assert_ne!(second.token, first.token);
    rt.complete(second.token, Outcome::Success { result: None, links: vec![] });
    wait_until(|| mem.pending_len() == 0);
    rt.shutdown(Duration::from_secs(1));
}

#[test]
fn tasks_without_a_request_run_while_the_resource_is_taken() {
    let mem = MemoryBroker::default();
    mem.push(&Header::new("g", "q", 0, "ct"), b"");
    mem.push(&Header::new("g", "q", 0, "ct"), b"");
    mem.push(&Header::new("free", "q", 0, "ct"), b"");
    let rt = start(&mem, gpu_cfg(4, 2));
    let mut seen = Vec::new();
    for _ in 0..2 {
        seen.push(rt.recv_timeout(RECV).expect("a task that fits").task);
    }
    seen.sort();
    assert_eq!(seen, vec!["free", "g"]);
    assert!(rt.recv_timeout(Duration::from_millis(200)).is_none());
    rt.shutdown(Duration::from_secs(1));
}

#[test]
fn progress_is_recorded_for_a_running_task_and_ignored_after() {
    let mem = MemoryBroker::default();
    mem.push(&Header::new("t", "q", 0, "ct"), b"");
    let rt = start(&mem, cfg(1, 1));
    assert!(!rt.progress(999, 1.0, None), "unknown token");
    let t = rt.recv_timeout(RECV).unwrap();
    let running = rt.running();
    assert_eq!(running.len(), 1);
    assert_eq!((running[0].id.as_str(), running[0].fraction), (t.id.as_str(), None));
    assert!(rt.progress(t.token, 25.0, Some(100.0)));
    assert!(!rt.progress(t.token, f64::NAN, None));
    let running = rt.running();
    assert_eq!((running[0].done, running[0].total, running[0].fraction), (Some(25.0), Some(100.0), Some(0.25)));
    rt.complete(t.token, Outcome::Success { result: None, links: vec![] });
    wait_until(|| mem.pending_len() == 0 && rt.running().is_empty());
    assert!(!rt.progress(t.token, 50.0, Some(100.0)), "settled task");
    rt.shutdown(Duration::from_secs(1));
}

#[test]
fn heartbeat_publishes_running_tasks_with_progress() {
    let mem = MemoryBroker::default();
    mem.push(&Header::new("a", "q", 0, "ct"), b"");
    mem.push(&Header::new("b", "q", 0, "ct"), b"");
    let rt = start(&mem, cfg(2, 2));
    let id = rt.worker_id().to_string();
    let reader = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    let (t1, t2) = (rt.recv_timeout(RECV).unwrap(), rt.recv_timeout(RECV).unwrap());
    assert!(rt.progress(t1.token, 5.0, Some(10.0)));
    let t = Instant::now();
    let tasks = loop {
        let tasks = reader.block_on(mayu_core::Broker::running(&mem, &id)).unwrap();
        if tasks.iter().any(|x| x.fraction.is_some()) {
            break tasks;
        }
        assert!(t.elapsed() < Duration::from_secs(3), "progress never reached a heartbeat");
        std::thread::sleep(Duration::from_millis(10));
    };
    assert_eq!(tasks.len(), 2);
    let p = tasks.iter().find(|x| x.id == t1.id).unwrap();
    assert_eq!((p.done, p.total, p.fraction), (Some(5.0), Some(10.0), Some(0.5)));
    assert_eq!((p.task.as_str(), p.queue.as_str()), (t1.task.as_str(), "q"));
    let other = tasks.iter().find(|x| x.id != t1.id).unwrap();
    assert_eq!((other.done, other.fraction, other.remaining_s), (None, None, None));
    // Once both finish, the next heartbeat publishes none.
    rt.complete(t1.token, Outcome::Success { result: None, links: vec![] });
    rt.complete(t2.token, Outcome::Success { result: None, links: vec![] });
    wait_until(|| mem.worker(&id).is_some_and(|w| w.tasks.is_empty()));
    assert!(reader.block_on(mayu_core::Broker::running(&mem, &id)).unwrap().is_empty());
    rt.shutdown(Duration::from_secs(1));
    assert!(reader.block_on(mayu_core::Broker::running(&mem, &id)).unwrap().is_empty(), "unregistered");
}

#[test]
fn heartbeat_publishes_at_most_the_cap() {
    let mem = MemoryBroker::default();
    let n = mayu_core::MAX_PUBLISHED + 6;
    for _ in 0..n {
        mem.push(&Header::new("t", "q", 0, "ct"), b"");
    }
    let rt = start(&mem, cfg(n, 1));
    let id = rt.worker_id().to_string();
    let tokens: Vec<_> = (0..n).map(|_| rt.recv_timeout(RECV).unwrap().token).collect();
    wait_until(|| mem.worker(&id).is_some_and(|w| w.inflight == n));
    assert_eq!(mem.worker(&id).unwrap().tasks.len(), mayu_core::MAX_PUBLISHED);
    for t in tokens {
        rt.complete(t, Outcome::Success { result: None, links: vec![] });
    }
    rt.shutdown(Duration::from_secs(1));
}
