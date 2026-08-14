//! [`SessionContinuitySnapshot`] — the canonical, resumable projection of a
//! session. `schemaVersion` 3; capped at [`MAX_SNAPSHOT_BYTES`] (56 KiB). It is
//! rebuilt server-side and is the single source of truth a client re-syncs to
//! after any disconnection. Phase 6 populates the spine fields; phase 7 fills
//! `rootTurn` from a live turn and phase 9 fills `interactionsPending`.

use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const SNAPSHOT_SCHEMA_VERSION: u32 = 3;
/// Maximum serialized size of a snapshot on the wire.
pub const MAX_SNAPSHOT_BYTES: usize = 56 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct SessionContinuitySnapshot {
    pub schema_version: u32,
    pub session_id: String,
    /// Strictly increasing on every canonical transition.
    pub projection_revision: u64,
    /// Session status label (phase 6 placeholder; phase 9 uses the typed status).
    pub status: String,
    /// The active/terminal turn (`TurnSnapshot`); None when idle. Phase 7 fills.
    pub root_turn: Option<Value>,
    pub queue: MessageQueueProjection,
    /// Pending permission interactions. Phase 9 fills.
    pub interactions_pending: Vec<Value>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct MessageQueueProjection {
    pub queue_revision: u64,
    /// Messages injected into the current turn (steering).
    pub steering: Vec<Value>,
    /// Messages queued for the next turn (followup).
    pub followup: Vec<Value>,
}

impl SessionContinuitySnapshot {
    pub fn fresh(session_id: impl Into<String>) -> Self {
        Self {
            schema_version: SNAPSHOT_SCHEMA_VERSION,
            session_id: session_id.into(),
            projection_revision: 0,
            status: "ready".into(),
            root_turn: None,
            queue: MessageQueueProjection::default(),
            interactions_pending: Vec::new(),
        }
    }

    /// Serialized byte length (for the cap check).
    pub fn encoded_bytes(&self) -> usize {
        serde_json::to_vec(self).map(|b| b.len()).unwrap_or(usize::MAX)
    }

    pub fn over_cap(&self) -> bool {
        self.encoded_bytes() > MAX_SNAPSHOT_BYTES
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fresh_snapshot_round_trips_under_cap() {
        let snap = SessionContinuitySnapshot::fresh("s1");
        assert_eq!(snap.schema_version, 3);
        assert!(snap.projection_revision == 0);
        let v = serde_json::to_value(&snap).unwrap();
        assert_eq!(v["schemaVersion"], 3);
        assert_eq!(v["sessionId"], "s1");
        assert!(!snap.over_cap());
    }

    #[test]
    fn snapshot_serializes_with_camel_case_and_kind_free() {
        // No `kind` on a snapshot (kind is a frame attribute); closed schema.
        let snap = SessionContinuitySnapshot::fresh("s1");
        let s = serde_json::to_string(&snap).unwrap();
        assert!(s.contains("\"schemaVersion\""));
        assert!(s.contains("\"projectionRevision\""));
        // unknown field rejected
        let tampered = s.replace("\"status\"", "\"stray\":1,\"status\"");
        assert!(serde_json::from_str::<SessionContinuitySnapshot>(&tampered).is_err());
    }
}
