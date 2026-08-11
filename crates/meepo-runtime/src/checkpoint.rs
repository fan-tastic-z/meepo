//! HistoryCompactCheckpoint — the durable compaction projection.
//!
//! A checkpoint covers an ordered prefix of RuntimeEvents with a lossy summary,
//! plus verifiable coverage metadata (event/turn counts, through-boundary, and
//! a SHA-256 source digest). It enables rolling compaction (each new checkpoint
//! builds on the previous summary + newly folded events) and replay validation
//! (the digest proves which source prefix the checkpoint claims to cover).

use std::collections::HashSet;

use sha2::{Digest, Sha256};

use meepo_core::RuntimeEvent;

/// A durable compaction checkpoint: a lossy summary of an ordered event prefix,
/// with verifiable coverage.
#[derive(Debug, Clone)]
pub struct HistoryCompactCheckpoint {
    pub checkpoint_id: String,
    pub session_id: String,
    pub created_at_ts: i64,
    /// Number of source events covered by this checkpoint.
    pub event_count: usize,
    /// Number of distinct turns covered.
    pub turn_count: usize,
    /// The boundary event (last covered event's identity).
    pub through_run_id: String,
    pub through_turn_id: String,
    pub through_event_id: String,
    /// SHA-256 over the canonical JSON of all covered events, joined by newline.
    /// Format: `sha256:<hex>`.
    pub source_digest: String,
    /// The lossy continuation summary.
    pub summary: String,
    /// Previous checkpoint in the rolling chain (None for the first).
    pub previous_checkpoint_id: Option<String>,
}

/// Build a checkpoint from covered events and a summary.
///
/// Validates: non-empty events, same session, no partial events, non-empty
/// summary. Computes the source digest over canonical JSON.
pub fn build_checkpoint(
    session_id: &str,
    covered_events: &[RuntimeEvent],
    summary: &str,
    previous_checkpoint_id: Option<&str>,
    now_ts: i64,
) -> HistoryCompactCheckpoint {
    assert!(
        !covered_events.is_empty(),
        "checkpoint requires at least one covered event"
    );
    assert!(
        covered_events.iter().all(|e| e.session_id == session_id),
        "all covered events must belong to the same session"
    );
    assert!(
        covered_events
            .iter()
            .all(|e| e.partial.unwrap_or(false) == false),
        "checkpoint coverage must not include partial events"
    );
    let trimmed = summary.trim();
    assert!(!trimmed.is_empty(), "checkpoint requires a non-empty summary");

    let last = covered_events.last().unwrap();
    let event_count = covered_events.len();
    let turn_count = covered_events
        .iter()
        .map(|e| &e.turn_id)
        .collect::<HashSet<_>>()
        .len();

    // Source digest: SHA-256 over canonical JSON of each event, joined by newline.
    let mut hasher = Sha256::new();
    for event in covered_events {
        let json = event.to_canonical_json().unwrap_or_default();
        hasher.update(json.as_bytes());
        hasher.update(b"\n");
    }
    let digest_hex = format!("{:x}", hasher.finalize());
    let source_digest = format!("sha256:{digest_hex}");

    HistoryCompactCheckpoint {
        checkpoint_id: format!("ckpt-{session_id}-{event_count}"),
        session_id: session_id.to_string(),
        created_at_ts: now_ts,
        event_count,
        turn_count,
        through_run_id: last.run_id.clone(),
        through_turn_id: last.turn_id.clone(),
        through_event_id: last.id.clone(),
        source_digest,
        summary: trimmed.to_string(),
        previous_checkpoint_id: previous_checkpoint_id.map(String::from),
    }
}

/// Verify that a checkpoint's coverage matches a source event prefix.
///
/// Returns `Ok(())` if the prefix's digest and through-boundary match the
/// checkpoint's coverage. Returns `Err(reason)` otherwise.
pub fn verify_checkpoint_prefix(
    checkpoint: &HistoryCompactCheckpoint,
    source_events: &[RuntimeEvent],
) -> Result<(), &'static str> {
    if source_events.len() < checkpoint.event_count {
        return Err("coverage_miss");
    }

    let prefix = &source_events[..checkpoint.event_count];
    let last = prefix.last().ok_or("coverage_miss")?;

    // Through-boundary must match.
    if last.run_id != checkpoint.through_run_id
        || last.turn_id != checkpoint.through_turn_id
        || last.id != checkpoint.through_event_id
    {
        return Err("coverage_miss");
    }

    // Recompute digest.
    let mut hasher = Sha256::new();
    for event in prefix {
        let json = event.to_canonical_json().unwrap_or_default();
        hasher.update(json.as_bytes());
        hasher.update(b"\n");
    }
    let digest_hex = format!("{:x}", hasher.finalize());
    let recomputed = format!("sha256:{digest_hex}");

    if recomputed != checkpoint.source_digest {
        return Err("source_hash_mismatch");
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use meepo_core::{Author, Content, Role};

    fn text_event(id: &str, role: Role, author: Author, text: &str) -> RuntimeEvent {
        RuntimeEvent {
            session_id: "s".into(),
            invocation_id: "inv".into(),
            run_id: "r".into(),
            turn_id: "t".into(),
            branch: None,
            id: id.into(),
            ts: 0,
            role,
            author,
            origin: None,
            model_visibility: None,
            status: None,
            content: Some(Content::Text {
                text: text.into(),
                provider_options: None,
                steering: None,
            }),
            actions: None,
            refs: None,
            partial: None,
        }
    }

    #[test]
    fn build_checkpoint_computes_digest_and_coverage() {
        let events = vec![
            text_event("e1", Role::User, Author::User, "hello"),
            text_event("e2", Role::Model, Author::Agent, "hi there"),
        ];
        let ckpt = build_checkpoint("s", &events, "User greeted, agent replied.", None, 1000);
        assert_eq!(ckpt.event_count, 2);
        assert_eq!(ckpt.turn_count, 1);
        assert_eq!(ckpt.through_event_id, "e2");
        assert!(ckpt.source_digest.starts_with("sha256:"));
        assert!(ckpt.summary.contains("User greeted"));
        assert!(ckpt.previous_checkpoint_id.is_none());
    }

    #[test]
    fn verify_matching_prefix() {
        let events = vec![
            text_event("e1", Role::User, Author::User, "hello"),
            text_event("e2", Role::Model, Author::Agent, "hi there"),
        ];
        let ckpt = build_checkpoint("s", &events, "summary", None, 0);
        assert!(verify_checkpoint_prefix(&ckpt, &events).is_ok());
    }

    #[test]
    fn verify_rejects_modified_prefix() {
        let events = vec![
            text_event("e1", Role::User, Author::User, "hello"),
            text_event("e2", Role::Model, Author::Agent, "hi there"),
        ];
        let ckpt = build_checkpoint("s", &events, "summary", None, 0);
        // Modify the second event — digest should mismatch.
        let mut modified = events.clone();
        modified[1].id = "e2-modified".into();
        assert_eq!(verify_checkpoint_prefix(&ckpt, &modified), Err("coverage_miss"));
    }

    #[test]
    fn rolling_chain_links_previous() {
        let events = vec![
            text_event("e1", Role::User, Author::User, "hello"),
            text_event("e2", Role::Model, Author::Agent, "hi there"),
        ];
        let ckpt1 = build_checkpoint("s", &events, "first summary", None, 0);
        let more = vec![
            text_event("e3", Role::User, Author::User, "what next"),
            text_event("e4", Role::Model, Author::Agent, "try this"),
        ];
        let all: Vec<RuntimeEvent> = events.into_iter().chain(more.into_iter()).collect();
        let ckpt2 = build_checkpoint(
            "s",
            &all,
            "updated summary with new context",
            Some(&ckpt1.checkpoint_id),
            1000,
        );
        assert_eq!(ckpt2.event_count, 4);
        assert_eq!(ckpt2.previous_checkpoint_id, Some(ckpt1.checkpoint_id));
        assert_ne!(ckpt1.source_digest, ckpt2.source_digest);
    }
}
