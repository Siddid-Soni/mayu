use crate::envelope::Header;

/// A `link=` callback: a task enqueued in the same atomic step that acks its parent.
#[derive(Debug, Clone, PartialEq)]
pub struct Link {
    pub task: String,
    pub queue: String,
    pub max_retries: u32,
    /// Encoded by the worker with the parent's serializer; the parent's result comes first.
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Outcome {
    Success { result: Option<Vec<u8>>, links: Vec<Link> },
    Failure { error: String, traceback: String },
    Retry { countdown_ms: Option<u64>, error: String },
}

#[derive(Debug, Clone, PartialEq)]
pub enum Action {
    Ack { result: Option<Vec<u8>>, links: Vec<Link> },
    Retry { header: Header },
    Dead { error: String, traceback: String },
}

#[derive(Debug, Clone)]
pub struct RetryPolicy {
    pub base_ms: u64,
    pub cap_ms: u64,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        RetryPolicy { base_ms: 1_000, cap_ms: 600_000 }
    }
}

/// Exponential backoff `min(cap, base * 2^attempt)`, passed through `jitter`.
pub fn backoff_ms(attempt: u32, base_ms: u64, cap_ms: u64, jitter: impl FnOnce(u64) -> u64) -> u64 {
    let factor = 1u64.checked_shl(attempt.min(63)).unwrap_or(u64::MAX);
    let max = base_ms.saturating_mul(factor).min(cap_ms);
    jitter(max).min(max)
}

/// Full jitter: uniform in `0..=max`.
pub fn full_jitter(max: u64) -> u64 {
    rand::random_range(0..=max)
}

pub fn decide(
    header: &Header,
    outcome: Outcome,
    policy: &RetryPolicy,
    now_ms: u64,
    jitter: impl FnOnce(u64) -> u64,
) -> Action {
    match outcome {
        Outcome::Success { result, links } => Action::Ack { result, links },
        Outcome::Failure { error, traceback } => Action::Dead { error, traceback },
        Outcome::Retry { countdown_ms, error } => {
            if header.attempt >= header.max_retries {
                return Action::Dead {
                    error: format!("retries exhausted after {} attempts: {error}", header.attempt + 1),
                    traceback: String::new(),
                };
            }
            let delay = countdown_ms
                .unwrap_or_else(|| backoff_ms(header.attempt, policy.base_ms, policy.cap_ms, jitter));
            let mut next = header.clone();
            next.attempt += 1;
            next.eta = Some(now_ms.saturating_add(delay));
            Action::Retry { header: next }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn hdr(attempt: u32, max: u32) -> Header {
        let mut h = Header::new("t", "q", max, "ct");
        h.attempt = attempt;
        h
    }

    #[test]
    fn backoff_grows_exponentially_then_caps() {
        let id = |m| m; // no jitter: return the max
        assert_eq!(backoff_ms(0, 100, 10_000, id), 100);
        assert_eq!(backoff_ms(1, 100, 10_000, id), 200);
        assert_eq!(backoff_ms(3, 100, 10_000, id), 800);
        assert_eq!(backoff_ms(20, 100, 10_000, id), 10_000);
        assert_eq!(backoff_ms(200, 100, 10_000, id), 10_000, "huge attempt must not overflow");
    }

    proptest! {
        #[test]
        fn full_jitter_within_bounds(attempt in 0u32..100, base in 1u64..10_000, cap in 1u64..1_000_000) {
            let v = backoff_ms(attempt, base, cap, full_jitter);
            prop_assert!(v <= cap);
        }
    }

    #[test]
    fn success_carries_links_to_the_ack() {
        let link = Link { task: "cb".into(), queue: "q2".into(), max_retries: 1, payload: b"p".to_vec() };
        let a = decide(&hdr(0, 3), Outcome::Success { result: None, links: vec![link.clone()] },
                       &RetryPolicy::default(), 1_000, |m| m);
        assert_eq!(a, Action::Ack { result: None, links: vec![link] });
    }

    #[test]
    fn success_acks() {
        let a = decide(&hdr(0, 3), Outcome::Success { result: Some(b"r".to_vec()), links: vec![] },
                       &RetryPolicy::default(), 1_000, |m| m);
        assert!(matches!(a, Action::Ack { result: Some(ref r), .. } if r == b"r"));
    }

    #[test]
    fn failure_goes_dead() {
        let a = decide(&hdr(0, 3), Outcome::Failure { error: "E".into(), traceback: "tb".into() },
                       &RetryPolicy::default(), 1_000, |m| m);
        assert!(matches!(a, Action::Dead { ref error, ref traceback } if error == "E" && traceback == "tb"));
    }

    #[test]
    fn retry_with_countdown_schedules_next_attempt() {
        let h = hdr(1, 3);
        let a = decide(&h, Outcome::Retry { countdown_ms: Some(500), error: "x".into() },
                       &RetryPolicy::default(), 1_000, |m| m);
        match a {
            Action::Retry { header } => {
                assert_eq!(header.attempt, 2);
                assert_eq!(header.eta, Some(1_500));
                assert_eq!(header.id, h.id, "id is stable across retries");
            }
            other => panic!("expected retry, got {other:?}"),
        }
    }

    #[test]
    fn retry_without_countdown_uses_backoff() {
        let p = RetryPolicy { base_ms: 100, cap_ms: 10_000 };
        let a = decide(&hdr(2, 5), Outcome::Retry { countdown_ms: None, error: "x".into() }, &p, 1_000, |m| m);
        assert!(matches!(a, Action::Retry { ref header } if header.eta == Some(1_400)));
    }

    #[test]
    fn retry_keeps_the_expires_deadline() {
        // The retried header is cloned, so `expires` carries over: a retry whose backoff lands past
        // the deadline is later dropped as expired at claim time (README documents this).
        let mut h = hdr(0, 3);
        h.expires = Some(1_234);
        let a = decide(&h, Outcome::Retry { countdown_ms: Some(0), error: "x".into() },
                       &RetryPolicy::default(), 1_000, |m| m);
        assert!(matches!(a, Action::Retry { ref header } if header.expires == Some(1_234)));
    }

    #[test]
    fn retry_exhausted_goes_dead() {
        let a = decide(&hdr(3, 3), Outcome::Retry { countdown_ms: None, error: "boom".into() },
                       &RetryPolicy::default(), 1_000, |m| m);
        assert!(matches!(a, Action::Dead { ref error, .. } if error.contains("retries exhausted") && error.contains("boom")));
    }
}
