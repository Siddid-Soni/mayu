use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

/// Envelope header. Rust decodes only this; the payload stays opaque bytes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Header {
    pub id: String,
    pub task: String,
    pub queue: String,
    pub attempt: u32,
    pub max_retries: u32,
    pub eta: Option<u64>,
    pub created_at: u64,
    #[serde(default)]
    pub trace_ctx: Option<String>,
    pub content_type: String,
    /// Set on workflow nodes only; absent from every ordinary header's bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow_id: Option<String>,
    /// The node's index in its workflow, as a decimal string.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node: Option<String>,
    /// Absolute ms deadline. The worker drops the task at claim time if `now_ms()` is past it,
    /// settling it as `expired`. Absent from an ordinary task's bytes, so older workers ignore it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires: Option<u64>,
}

impl Header {
    pub fn new(
        task: impl Into<String>,
        queue: impl Into<String>,
        max_retries: u32,
        content_type: impl Into<String>,
    ) -> Self {
        Header {
            id: ulid::Ulid::generate().to_string(),
            task: task.into(),
            queue: queue.into(),
            attempt: 0,
            max_retries,
            eta: None,
            created_at: now_ms(),
            trace_ctx: None,
            content_type: content_type.into(),
            workflow_id: None,
            node: None,
            expires: None,
        }
    }
}

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

pub fn encode_header(h: &Header) -> Vec<u8> {
    rmp_serde::to_vec_named(h).expect("header serialization cannot fail")
}

pub fn decode_header(b: &[u8]) -> Result<Header> {
    rmp_serde::from_slice(b).map_err(|e| Error::Envelope(e.to_string()))
}

/// Delayed-set member: `"<header_len>:" ++ header ++ payload`.
pub fn encode_delayed_member(h: &[u8], p: &[u8]) -> Vec<u8> {
    let mut out = format!("{}:", h.len()).into_bytes();
    out.reserve(h.len() + p.len());
    out.extend_from_slice(h);
    out.extend_from_slice(p);
    out
}

pub fn decode_delayed_member(m: &[u8]) -> Result<(&[u8], &[u8])> {
    let colon = m
        .iter()
        .position(|&b| b == b':')
        .ok_or_else(|| Error::Envelope("delayed member missing length prefix".into()))?;
    let len: usize = std::str::from_utf8(&m[..colon])
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| Error::Envelope("delayed member has bad length prefix".into()))?;
    let rest = &m[colon + 1..];
    if len > rest.len() {
        return Err(Error::Envelope("delayed member length exceeds data".into()));
    }
    Ok(rest.split_at(len))
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn header_roundtrip() {
        let mut h = Header::new("app.add", "default", 3, "application/msgpack");
        h.eta = Some(123);
        h.trace_ctx = Some("00-abc-def-01".into());
        let back = decode_header(&encode_header(&h)).unwrap();
        assert_eq!(back, h);
    }

    /// `Header` as it was before workflows, to pin the wire format.
    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct OldHeader {
        id: String,
        task: String,
        queue: String,
        attempt: u32,
        max_retries: u32,
        eta: Option<u64>,
        created_at: u64,
        #[serde(default)]
        trace_ctx: Option<String>,
        content_type: String,
    }

    fn old_of(h: &Header) -> OldHeader {
        OldHeader {
            id: h.id.clone(),
            task: h.task.clone(),
            queue: h.queue.clone(),
            attempt: h.attempt,
            max_retries: h.max_retries,
            eta: h.eta,
            created_at: h.created_at,
            trace_ctx: h.trace_ctx.clone(),
            content_type: h.content_type.clone(),
        }
    }

    #[test]
    fn plain_header_bytes_match_the_old_layout() {
        let mut h = Header::new("app.add", "default", 3, "application/msgpack");
        h.trace_ctx = Some("00-abc-def-01".into());
        assert_eq!(encode_header(&h), rmp_serde::to_vec_named(&old_of(&h)).unwrap());
    }

    #[test]
    fn old_bytes_decode_without_workflow_fields() {
        let h = Header::new("t", "q", 1, "ct");
        let back = decode_header(&rmp_serde::to_vec_named(&old_of(&h)).unwrap()).unwrap();
        assert_eq!((back.workflow_id.as_deref(), back.node.as_deref()), (None, None));
        assert_eq!(back, h);
    }

    #[test]
    fn workflow_header_roundtrips_and_old_readers_ignore_it() {
        let mut h = Header::new("t", "q", 1, "ct");
        h.workflow_id = Some("01WF".into());
        h.node = Some("3".into());
        let bytes = encode_header(&h);
        assert_eq!(decode_header(&bytes).unwrap(), h);
        let old: OldHeader = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(old, old_of(&h));
    }

    #[test]
    fn expires_roundtrips_and_old_readers_ignore_it() {
        let mut h = Header::new("t", "q", 1, "ct");
        h.expires = Some(1_700_000_000_000);
        let bytes = encode_header(&h);
        assert_eq!(decode_header(&bytes).unwrap(), h);
        // A worker on an older version decodes the same bytes without the field and still sees the task.
        let old: OldHeader = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(old, old_of(&h));
    }

    #[test]
    fn plain_header_without_expires_matches_the_old_bytes() {
        // expires is skipped when None, so an ordinary task's bytes are unchanged for a rolling deploy.
        let h = Header::new("app.add", "default", 3, "application/msgpack");
        assert_eq!(h.expires, None);
        assert_eq!(encode_header(&h), rmp_serde::to_vec_named(&old_of(&h)).unwrap());
    }

    #[test]
    fn header_new_defaults() {
        let h = Header::new("t", "q", 5, "ct");
        assert_eq!(h.attempt, 0);
        assert_eq!(h.eta, None);
        assert_eq!((h.workflow_id, h.node, h.expires), (None, None, None));
        assert_eq!(h.id.len(), 26, "ULID string is 26 chars");
        assert!(h.created_at > 0);
        assert_ne!(Header::new("t", "q", 5, "ct").id, h.id);
    }

    #[test]
    fn decode_header_rejects_garbage() {
        assert!(matches!(decode_header(b"not msgpack"), Err(Error::Envelope(_))));
    }

    #[test]
    fn delayed_member_roundtrip_with_colons_in_data() {
        let m = encode_delayed_member(b"h:e:a:d", b"pay:load");
        let (h, p) = decode_delayed_member(&m).unwrap();
        assert_eq!(h, b"h:e:a:d");
        assert_eq!(p, b"pay:load");
    }

    #[test]
    fn delayed_member_rejects_bad_prefix() {
        assert!(decode_delayed_member(b"nocolon").is_err());
        assert!(decode_delayed_member(b"x:abc").is_err());
        assert!(decode_delayed_member(b"99:short").is_err());
    }

    proptest! {
        #[test]
        fn delayed_member_roundtrip_prop(h in proptest::collection::vec(any::<u8>(), 0..512),
                                         p in proptest::collection::vec(any::<u8>(), 0..512)) {
            let m = encode_delayed_member(&h, &p);
            let (h2, p2) = decode_delayed_member(&m).unwrap();
            prop_assert_eq!(h2, &h[..]);
            prop_assert_eq!(p2, &p[..]);
        }
    }
}
