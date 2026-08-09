//! SqliteStore — a persistent [`RuntimeEventStore`] backed by SQLite (rusqlite).
//!
//! The `runtime_events` table is byte-aligned with the upstream runtime.sqlite
//! layout (same columns, indexes, and payload_json = canonical RuntimeEvent),
//! so a database written by either side is readable by the other. `event_seq`
//! is allocated per-invocation (MAX+1); `committed_at` is the event ts;
//! `event_kind` is a derived label (the payload is authoritative, so this is
//! informational, not load-bearing for reads).
//!
//! Only the `runtime_events` table is implemented here; the upstream DB has 12
//! more tables (tool_operations, workspace authority, partial snapshots, ...)
//! that meepo adds as needed. The schema_migrations row records v11.

use std::path::Path;
use std::sync::Mutex;

use async_trait::async_trait;
use rusqlite::Connection;

use meepo_core::{Content, Durability, RuntimeEvent, RuntimeEventStore, StoreResult};

const SCHEMA_VERSION: i64 = 11;

const INIT_SQL: &str = r#"
    CREATE TABLE IF NOT EXISTS runtime_events (
        event_id       TEXT PRIMARY KEY,
        session_id     TEXT NOT NULL,
        invocation_id  TEXT NOT NULL,
        run_id         TEXT NOT NULL,
        turn_id        TEXT NOT NULL,
        event_seq      INTEGER NOT NULL CHECK (event_seq > 0),
        event_kind     TEXT NOT NULL,
        payload_json   TEXT NOT NULL,
        committed_at   INTEGER NOT NULL,
        UNIQUE (invocation_id, event_seq)
    );
    CREATE INDEX IF NOT EXISTS runtime_events_by_run
        ON runtime_events(session_id, run_id, event_seq);
    CREATE INDEX IF NOT EXISTS runtime_events_by_session
        ON runtime_events(session_id, committed_at, event_id);

    CREATE TABLE IF NOT EXISTS operational_schema_migrations (
        version INTEGER PRIMARY KEY
    );
"#;

pub struct SqliteStore {
    conn: Mutex<Connection>,
}

impl SqliteStore {
    /// Open (or create) a SQLite database file. Parent directories are created
    /// if missing. Existing upstream runtime.sqlite databases are read as-is.
    pub fn open(path: impl AsRef<Path>) -> StoreResult<Self> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        Self::init(Connection::open(path)?)
    }

    /// In-memory database, for tests.
    pub fn in_memory() -> StoreResult<Self> {
        Self::init(Connection::open_in_memory()?)
    }

    fn init(conn: Connection) -> StoreResult<Self> {
        conn.execute_batch(INIT_SQL)?;
        conn.execute(
            "INSERT OR IGNORE INTO operational_schema_migrations (version) VALUES (?1)",
            rusqlite::params![SCHEMA_VERSION],
        )?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    fn next_event_seq(conn: &Connection, invocation_id: &str) -> StoreResult<i64> {
        Ok(conn.query_row(
            "SELECT COALESCE(MAX(event_seq), 0) + 1 FROM runtime_events WHERE invocation_id = ?1",
            rusqlite::params![invocation_id],
            |row| row.get::<_, i64>(0),
        )?)
    }
}

/// Informational event_kind label. Reads use payload_json, so this does not
/// affect interop; it mirrors the upstream column shape.
fn runtime_event_kind(event: &RuntimeEvent) -> &'static str {
    match &event.content {
        Some(Content::Text { .. }) => "text",
        Some(Content::Thinking { .. }) => "thinking",
        Some(Content::FunctionCall { .. }) => "function_call",
        Some(Content::FunctionResponse { .. }) => "function_response",
        Some(Content::Error { .. }) => "error",
        None => match event.status {
            Some(_) => "terminal",
            None => "event",
        },
    }
}

#[async_trait]
impl RuntimeEventStore for SqliteStore {
    fn durability(&self) -> Durability {
        Durability::Canonical
    }

    async fn append_runtime_event(
        &self,
        _session_id: &str,
        _run_id: &str,
        event: RuntimeEvent,
        _durable: bool,
    ) -> StoreResult<()> {
        let payload = event.to_canonical_json()?;
        let kind = runtime_event_kind(&event);
        let committed_at = event.ts;
        let event_id = event.id.clone();
        let session_id = event.session_id.clone();
        let invocation_id = event.invocation_id.clone();
        let run_id = event.run_id.clone();
        let turn_id = event.turn_id.clone();
        let conn = self.conn.lock().expect("db lock poisoned");
        let next = Self::next_event_seq(&conn, &invocation_id)?;
        conn.execute(
            "INSERT INTO runtime_events \
             (event_id, session_id, invocation_id, run_id, turn_id, event_seq, event_kind, payload_json, committed_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            rusqlite::params![
                event_id, session_id, invocation_id, run_id, turn_id, next, kind, payload,
                committed_at
            ],
        )?;
        Ok(())
    }

    async fn ensure_terminal_runtime_event_durable(
        &self,
        session_id: &str,
        run_id: &str,
        event: RuntimeEvent,
    ) -> StoreResult<()> {
        // Idempotent: if this run already has a terminal event, do nothing.
        let existing = self.read_runtime_events(session_id, run_id).await?;
        let has_terminal = existing
            .iter()
            .any(|e| e.status.is_some_and(|s| s.is_terminal()));
        if !has_terminal {
            self.append_runtime_event(session_id, run_id, event, true)
                .await?;
        }
        Ok(())
    }

    async fn read_runtime_events(
        &self,
        session_id: &str,
        run_id: &str,
    ) -> StoreResult<Vec<RuntimeEvent>> {
        let conn = self.conn.lock().expect("db lock poisoned");
        read_ordered(
            &conn,
            "SELECT payload_json FROM runtime_events \
             WHERE session_id = ?1 AND run_id = ?2 \
             ORDER BY event_seq ASC, event_id ASC",
            rusqlite::params![session_id, run_id],
        )
    }

    async fn read_session_runtime_events(
        &self,
        session_id: &str,
    ) -> StoreResult<Vec<RuntimeEvent>> {
        let conn = self.conn.lock().expect("db lock poisoned");
        read_ordered(
            &conn,
            "SELECT payload_json FROM runtime_events \
             WHERE session_id = ?1 \
             ORDER BY committed_at ASC, event_id ASC",
            rusqlite::params![session_id],
        )
    }
}

fn read_ordered<P: rusqlite::Params>(
    conn: &Connection,
    sql: &str,
    params: P,
) -> StoreResult<Vec<RuntimeEvent>> {
    let mut stmt = conn.prepare(sql)?;
    let rows = stmt.query_map(params, |row| {
        let payload: String = row.get(0)?;
        Ok(payload)
    })?;
    let mut events = Vec::new();
    for row in rows {
        let payload = row?;
        events.push(serde_json::from_str(&payload)?);
    }
    Ok(events)
}
