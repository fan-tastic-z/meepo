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

use meepo_core::{Content, Durability, InteractionStore, RuntimeEvent, RuntimeEventStore, StoreResult};

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

    -- Interaction store (permission requests + outcomes). Byte-aligned with
    -- the upstream core_interaction_requests / core_interaction_outcomes schema.
    CREATE TABLE IF NOT EXISTS core_interaction_requests (
        request_id   TEXT PRIMARY KEY,
        session_id   TEXT NOT NULL,
        turn_id      TEXT NOT NULL,
        run_id       TEXT NOT NULL,
        request_kind TEXT NOT NULL,
        created_at   INTEGER NOT NULL,
        record_json  TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS core_interaction_pending
        ON core_interaction_requests(session_id, created_at, request_id);

    CREATE TABLE IF NOT EXISTS core_interaction_outcomes (
        request_id  TEXT PRIMARY KEY,
        record_json TEXT NOT NULL,
        FOREIGN KEY (request_id)
            REFERENCES core_interaction_requests(request_id)
            ON DELETE CASCADE
    );

    -- Tool operation ledger: one row per durable side-effect boundary a
    -- dispatch fact opens. Byte-aligned with the upstream tool_operations table.
    CREATE TABLE IF NOT EXISTS tool_operations (
        operation_id          TEXT PRIMARY KEY,
        invocation_id         TEXT NOT NULL,
        run_id                TEXT NOT NULL,
        turn_id               TEXT NOT NULL,
        provider_tool_call_id TEXT NOT NULL,
        tool_name             TEXT NOT NULL,
        canonical_args_hash   TEXT NOT NULL,
        recovery_mode         TEXT NOT NULL,
        current_state         TEXT NOT NULL,
        call_event_id         TEXT NOT NULL,
        result_event_id       TEXT,
        dispatch_event_id     TEXT,
        version               INTEGER NOT NULL CHECK (version > 0),
        UNIQUE (invocation_id, provider_tool_call_id)
    );

    -- Tool journal events: the state transitions of one operation, in order.
    CREATE TABLE IF NOT EXISTS tool_journal_events (
        journal_seq         INTEGER PRIMARY KEY AUTOINCREMENT,
        journal_event_id    TEXT NOT NULL UNIQUE,
        operation_id        TEXT NOT NULL,
        invocation_id       TEXT NOT NULL,
        run_id              TEXT NOT NULL,
        turn_id             TEXT NOT NULL,
        state               TEXT NOT NULL,
        runtime_event_id    TEXT,
        canonical_args_hash TEXT,
        recovery_mode       TEXT,
        external_handle     TEXT,
        metadata_json       TEXT,
        committed_at        INTEGER NOT NULL
    );
    CREATE INDEX IF NOT EXISTS tool_journal_events_by_operation
        ON tool_journal_events(operation_id, journal_seq);
"#;

pub struct SqliteStore {
    conn: Mutex<Connection>,
}

/// One durable tool side-effect boundary (a row in `tool_operations`).
#[derive(Debug, Clone, PartialEq)]
pub struct ToolOperation {
    pub operation_id: String,
    pub invocation_id: String,
    pub run_id: String,
    pub turn_id: String,
    pub provider_tool_call_id: String,
    pub tool_name: String,
    pub canonical_args_hash: String,
    pub recovery_mode: String,
    pub current_state: String,
    pub call_event_id: String,
    pub result_event_id: Option<String>,
    pub dispatch_event_id: Option<String>,
    pub version: i64,
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

    /// Upsert one tool operation row (`operation_id` is the primary key).
    pub async fn record_tool_operation(&self, op: &ToolOperation) -> StoreResult<()> {
        let conn = self.conn.lock().expect("db lock poisoned");
        conn.execute(
            "INSERT OR REPLACE INTO tool_operations \
             (operation_id, invocation_id, run_id, turn_id, provider_tool_call_id, \
              tool_name, canonical_args_hash, recovery_mode, current_state, \
              call_event_id, result_event_id, dispatch_event_id, version) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
            rusqlite::params![
                op.operation_id, op.invocation_id, op.run_id, op.turn_id,
                op.provider_tool_call_id, op.tool_name, op.canonical_args_hash,
                op.recovery_mode, op.current_state, op.call_event_id,
                op.result_event_id, op.dispatch_event_id, op.version,
            ],
        )?;
        Ok(())
    }

    /// Read one tool operation by its operation id.
    pub async fn read_tool_operation(
        &self,
        operation_id: &str,
    ) -> StoreResult<Option<ToolOperation>> {
        let conn = self.conn.lock().expect("db lock poisoned");
        let res = conn.query_row(
            "SELECT operation_id, invocation_id, run_id, turn_id, provider_tool_call_id, \
             tool_name, canonical_args_hash, recovery_mode, current_state, call_event_id, \
             result_event_id, dispatch_event_id, version \
             FROM tool_operations WHERE operation_id = ?1",
            rusqlite::params![operation_id],
            |row| {
                Ok(ToolOperation {
                    operation_id: row.get(0)?,
                    invocation_id: row.get(1)?,
                    run_id: row.get(2)?,
                    turn_id: row.get(3)?,
                    provider_tool_call_id: row.get(4)?,
                    tool_name: row.get(5)?,
                    canonical_args_hash: row.get(6)?,
                    recovery_mode: row.get(7)?,
                    current_state: row.get(8)?,
                    call_event_id: row.get(9)?,
                    result_event_id: row.get(10)?,
                    dispatch_event_id: row.get(11)?,
                    version: row.get(12)?,
                })
            },
        );
        match res {
            Ok(op) => Ok(Some(op)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
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
             ORDER BY rowid ASC",
            rusqlite::params![session_id],
        )
    }
}

#[async_trait]
impl InteractionStore for SqliteStore {
    async fn record_permission(
        &self,
        session_id: &str,
        run_id: &str,
        turn_id: &str,
        request_id: &str,
        created_at: i64,
        request_json: &str,
        outcome_json: &str,
    ) -> StoreResult<()> {
        let conn = self.conn.lock().expect("db lock poisoned");
        // Idempotent: a re-write of the same request_id is a no-op (the first
        // publication is authoritative), matching the upstream establish/commit.
        conn.execute(
            "INSERT OR IGNORE INTO core_interaction_requests \
             (request_id, session_id, turn_id, run_id, request_kind, created_at, record_json) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            rusqlite::params![
                request_id, session_id, turn_id, run_id, "permission", created_at, request_json
            ],
        )?;
        conn.execute(
            "INSERT OR IGNORE INTO core_interaction_outcomes (request_id, record_json) \
             VALUES (?1, ?2)",
            rusqlite::params![request_id, outcome_json],
        )?;
        Ok(())
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
