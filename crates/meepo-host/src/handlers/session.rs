//! Session lifecycle ops: create / catalog query / metadata + configuration +
//! cwd updates (optimistic concurrency) / read marker / archive / remove.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use meepo_storage::{SessionPatch, SessionRecord, SessionUpdate, SqliteStore};
use serde_json::{json, Value};

use crate::protocol::OpErrorCode;
use crate::server::dispatcher::{handler, Dispatcher, Outcome};

fn now() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs() as i64).unwrap_or(0)
}

fn str_field(input: &Value, key: &str) -> String {
    input.get(key).and_then(|v| v.as_str()).unwrap_or_default().to_string()
}

fn invalid(message: &str) -> Outcome {
    Outcome::Err { code: OpErrorCode::InvalidRequest, message: message.into() }
}

fn storage_err(e: impl std::fmt::Display) -> Outcome {
    Outcome::Err { code: OpErrorCode::PersistenceFailed, message: e.to_string() }
}

/// Apply a revision-checked patch; the result is the committed item or a
/// revision_conflict marker (a result union, not an op error).
async fn apply_patch(store: &SqliteStore, input: &Value, patch: SessionPatch) -> Outcome {
    let session_id = str_field(input, "sessionId");
    if session_id.is_empty() {
        return invalid("sessionId is required");
    }
    let expected = input.get("expectedRevision").and_then(|v| v.as_i64()).unwrap_or(0);
    match store.update_session(&session_id, expected, &patch).await {
        Ok(SessionUpdate::Committed(rec)) => Outcome::Ok(json!({
            "kind": "committed",
            "session": serde_json::to_value(&rec).expect("record serializes"),
        })),
        Ok(SessionUpdate::RevisionConflict { expected, current }) => Outcome::Ok(json!({
            "kind": "revision_conflict",
            "expectedRevision": expected,
            "currentRevision": current,
        })),
        Err(e) => storage_err(e),
    }
}

pub fn register(dispatcher: &mut Dispatcher, store: Arc<SqliteStore>) {
    // session.create
    let s = store.clone();
    dispatcher.register("session.create", handler(move |input, _ctx| {
        let store = s.clone();
        async move {
            let session_id = str_field(&input, "sessionId");
            if session_id.is_empty() {
                return invalid("sessionId is required");
            }
            let ts = now();
            let rec = SessionRecord {
                session_id,
                name: str_field(&input, "name"),
                labels: input
                    .get("labels")
                    .and_then(|v| v.as_array())
                    .map(|a| a.iter().filter_map(|l| l.as_str().map(String::from)).collect())
                    .unwrap_or_default(),
                revision: 1,
                is_archived: false,
                cwd: str_field(&input, "cwd"),
                config_json: "{}".into(),
                read_marker: None,
                created_at: ts,
                updated_at: ts,
            };
            match store.create_session(&rec).await {
                Ok(()) => Outcome::Ok(serde_json::to_value(&rec).expect("record serializes")),
                Err(e) => storage_err(e),
            }
        }
    }));

    // session.catalog.query (single page; cursor paging is a later refinement)
    let s = store.clone();
    dispatcher.register("session.catalog.query", handler(move |_input, _ctx| {
        let store = s.clone();
        async move {
            match store.list_sessions().await {
                Ok(items) => Outcome::Ok(json!({
                    "kind": "page",
                    "items": serde_json::to_value(&items).expect("records serialize"),
                })),
                Err(e) => storage_err(e),
            }
        }
    }));

    // session.metadata.update {patch: {name?, labels?}}
    let s = store.clone();
    dispatcher.register("session.metadata.update", handler(move |input, _ctx| {
        let store = s.clone();
        async move {
            let patch = SessionPatch {
                name: input.pointer("/patch/name").and_then(|v| v.as_str()).map(String::from),
                labels: input.pointer("/patch/labels").and_then(|v| v.as_array()).map(|a| {
                    a.iter().filter_map(|l| l.as_str().map(String::from)).collect()
                }),
                ..Default::default()
            };
            apply_patch(&store, &input, patch).await
        }
    }));

    // session.configuration.update {configuration: {...}}
    let s = store.clone();
    dispatcher.register("session.configuration.update", handler(move |input, _ctx| {
        let store = s.clone();
        async move {
            let config = match input.get("configuration") {
                Some(c) if c.is_object() => serde_json::to_string(c).unwrap_or_default(),
                _ => return invalid("configuration object is required"),
            };
            apply_patch(&store, &input, SessionPatch { config_json: Some(config), ..Default::default() })
                .await
        }
    }));

    // session.cwd.relocate {cwd}
    let s = store.clone();
    dispatcher.register("session.cwd.relocate", handler(move |input, _ctx| {
        let store = s.clone();
        async move {
            let cwd = str_field(&input, "cwd");
            if cwd.is_empty() {
                return invalid("cwd is required");
            }
            apply_patch(&store, &input, SessionPatch { cwd: Some(cwd), ..Default::default() }).await
        }
    }));

    // session.read_marker.set {readThroughMessageId}
    let s = store.clone();
    dispatcher.register("session.read_marker.set", handler(move |input, _ctx| {
        let store = s.clone();
        async move {
            let marker = str_field(&input, "readThroughMessageId");
            if marker.is_empty() {
                return invalid("readThroughMessageId is required");
            }
            // No expectedRevision in the op; rebase on the current revision.
            let session_id = str_field(&input, "sessionId");
            let current = match store.get_session(&session_id).await {
                Ok(Some(rec)) => rec,
                Ok(None) => return invalid(&format!("session '{session_id}' does not exist")),
                Err(e) => return storage_err(e),
            };
            let mut rebased = input.clone();
            rebased["expectedRevision"] = json!(current.revision);
            apply_patch(
                &store,
                &rebased,
                SessionPatch { read_marker: Some(marker), ..Default::default() },
            )
            .await
        }
    }));

    // session.lifecycle.set {lifecycle: "archived" | "active"}
    let s = store.clone();
    dispatcher.register("session.lifecycle.set", handler(move |input, _ctx| {
        let store = s.clone();
        async move {
            let archived = match str_field(&input, "lifecycle").as_str() {
                "archived" => true,
                "active" => false,
                _ => return invalid("lifecycle must be 'archived' or 'active'"),
            };
            let session_id = str_field(&input, "sessionId");
            let current = match store.get_session(&session_id).await {
                Ok(Some(rec)) => rec,
                Ok(None) => return invalid(&format!("session '{session_id}' does not exist")),
                Err(e) => return storage_err(e),
            };
            let mut rebased = input.clone();
            rebased["expectedRevision"] = json!(current.revision);
            apply_patch(
                &store,
                &rebased,
                SessionPatch { is_archived: Some(archived), ..Default::default() },
            )
            .await
        }
    }));

    // session.remove
    let s = store;
    dispatcher.register("session.remove", handler(move |input, _ctx| {
        let store = s.clone();
        async move {
            let session_id = str_field(&input, "sessionId");
            if session_id.is_empty() {
                return invalid("sessionId is required");
            }
            match store.remove_session(&session_id).await {
                Ok(removed) => Outcome::Ok(json!({ "removed": removed })),
                Err(e) => storage_err(e),
            }
        }
    }));
}
