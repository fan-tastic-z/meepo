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

use meepo_core::{
    Content, Durability, InteractionStore, RuntimeEvent, RuntimeEventStore, StoreResult,
};
pub use meepo_core::{ToolOperation, ToolOperationStore};

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

    -- Crash-recovery continuation claims. Byte-aligned with the upstream
    -- runtime_continuation_claims table.
    CREATE TABLE IF NOT EXISTS runtime_continuation_claims (
        claim_id                    TEXT PRIMARY KEY,
        source_session_id           TEXT NOT NULL,
        source_invocation_id        TEXT NOT NULL,
        source_run_id               TEXT NOT NULL,
        source_turn_id              TEXT NOT NULL,
        source_event_high_water     INTEGER NOT NULL CHECK (source_event_high_water > 0),
        source_prefix_digest        TEXT NOT NULL,
        boundary_digest             TEXT NOT NULL UNIQUE,
        boundary_json               TEXT NOT NULL,
        provider_projection_version INTEGER NOT NULL CHECK (provider_projection_version = 1),
        provider_replay_digest      TEXT NOT NULL,
        target_session_id           TEXT NOT NULL,
        target_invocation_id        TEXT NOT NULL UNIQUE,
        target_run_id               TEXT NOT NULL UNIQUE,
        target_turn_id              TEXT NOT NULL,
        target_run_header_json      TEXT NOT NULL,
        claimed_at                  INTEGER NOT NULL,
        start_event_id              TEXT UNIQUE REFERENCES runtime_events(event_id),
        start_kind                  TEXT CHECK (
            start_kind IS NULL OR start_kind IN ('runtime_admission', 'claim_repair')
        ),
        protocol_version            INTEGER NOT NULL CHECK (protocol_version = 1),
        UNIQUE (source_session_id, source_run_id, source_event_high_water, source_prefix_digest),
        UNIQUE (target_session_id, target_turn_id)
    );

    -- Headless (durable task) event ledger. Byte-aligned with the upstream
    -- headless_task_run_events table.
    CREATE TABLE IF NOT EXISTS headless_task_run_events (
        task_run_id  TEXT NOT NULL,
        sequence     INTEGER NOT NULL CHECK (sequence >= 0),
        event_id     TEXT NOT NULL,
        record_json  TEXT NOT NULL,
        PRIMARY KEY (task_run_id, sequence)
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

#[async_trait]
impl ToolOperationStore for SqliteStore {
    async fn record_tool_operation(&self, op: &ToolOperation) -> StoreResult<()> {
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

    async fn read_tool_operation(
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

/// One crash-recovery continuation claim (a row in `runtime_continuation_claims`).
#[derive(Debug, Clone, PartialEq)]
pub struct ContinuationClaim {
    pub claim_id: String,
    pub source_session_id: String,
    pub source_invocation_id: String,
    pub source_run_id: String,
    pub source_turn_id: String,
    pub source_event_high_water: i64,
    pub source_prefix_digest: String,
    pub boundary_digest: String,
    pub boundary_json: String,
    pub provider_projection_version: i64,
    pub provider_replay_digest: String,
    pub target_session_id: String,
    pub target_invocation_id: String,
    pub target_run_id: String,
    pub target_turn_id: String,
    pub target_run_header_json: String,
    pub claimed_at: i64,
    pub start_event_id: Option<String>,
    pub start_kind: Option<String>,
    pub protocol_version: i64,
}

/// One event in a headless task run's event ledger (a row in
/// `headless_task_run_events`).
#[derive(Debug, Clone, PartialEq)]
pub struct TaskRunEvent {
    pub task_run_id: String,
    pub sequence: i64,
    pub event_id: String,
    pub record_json: String,
}

impl SqliteStore {
    pub async fn record_continuation_claim(&self, claim: &ContinuationClaim) -> StoreResult<()> {
        let conn = self.conn.lock().expect("db lock poisoned");
        conn.execute(
            "INSERT OR REPLACE INTO runtime_continuation_claims \
             (claim_id, source_session_id, source_invocation_id, source_run_id, source_turn_id, \
              source_event_high_water, source_prefix_digest, boundary_digest, boundary_json, \
              provider_projection_version, provider_replay_digest, target_session_id, \
              target_invocation_id, target_run_id, target_turn_id, target_run_header_json, \
              claimed_at, start_event_id, start_kind, protocol_version) \
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20)",
            rusqlite::params![
                claim.claim_id, claim.source_session_id, claim.source_invocation_id,
                claim.source_run_id, claim.source_turn_id, claim.source_event_high_water,
                claim.source_prefix_digest, claim.boundary_digest, claim.boundary_json,
                claim.provider_projection_version, claim.provider_replay_digest,
                claim.target_session_id, claim.target_invocation_id, claim.target_run_id,
                claim.target_turn_id, claim.target_run_header_json, claim.claimed_at,
                claim.start_event_id, claim.start_kind, claim.protocol_version,
            ],
        )?;
        Ok(())
    }

    pub async fn read_continuation_claim(
        &self,
        claim_id: &str,
    ) -> StoreResult<Option<ContinuationClaim>> {
        let conn = self.conn.lock().expect("db lock poisoned");
        let res = conn.query_row(
            "SELECT claim_id, source_session_id, source_invocation_id, source_run_id, \
             source_turn_id, source_event_high_water, source_prefix_digest, boundary_digest, \
             boundary_json, provider_projection_version, provider_replay_digest, \
             target_session_id, target_invocation_id, target_run_id, target_turn_id, \
             target_run_header_json, claimed_at, start_event_id, start_kind, protocol_version \
             FROM runtime_continuation_claims WHERE claim_id = ?1",
            rusqlite::params![claim_id],
            |row| {
                Ok(ContinuationClaim {
                    claim_id: row.get(0)?,
                    source_session_id: row.get(1)?,
                    source_invocation_id: row.get(2)?,
                    source_run_id: row.get(3)?,
                    source_turn_id: row.get(4)?,
                    source_event_high_water: row.get(5)?,
                    source_prefix_digest: row.get(6)?,
                    boundary_digest: row.get(7)?,
                    boundary_json: row.get(8)?,
                    provider_projection_version: row.get(9)?,
                    provider_replay_digest: row.get(10)?,
                    target_session_id: row.get(11)?,
                    target_invocation_id: row.get(12)?,
                    target_run_id: row.get(13)?,
                    target_turn_id: row.get(14)?,
                    target_run_header_json: row.get(15)?,
                    claimed_at: row.get(16)?,
                    start_event_id: row.get(17)?,
                    start_kind: row.get(18)?,
                    protocol_version: row.get(19)?,
                })
            },
        );
        match res {
            Ok(c) => Ok(Some(c)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn append_task_run_event(&self, event: &TaskRunEvent) -> StoreResult<()> {
        let conn = self.conn.lock().expect("db lock poisoned");
        conn.execute(
            "INSERT OR REPLACE INTO headless_task_run_events \
             (task_run_id, sequence, event_id, record_json) VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params![event.task_run_id, event.sequence, event.event_id, event.record_json],
        )?;
        Ok(())
    }

    pub async fn read_task_run_events(&self, task_run_id: &str) -> StoreResult<Vec<TaskRunEvent>> {
        let conn = self.conn.lock().expect("db lock poisoned");
        let mut stmt = conn.prepare(
            "SELECT task_run_id, sequence, event_id, record_json \
             FROM headless_task_run_events WHERE task_run_id = ?1 ORDER BY sequence ASC",
        )?;
        let rows = stmt.query_map(rusqlite::params![task_run_id], |row| {
            Ok(TaskRunEvent {
                task_run_id: row.get(0)?,
                sequence: row.get(1)?,
                event_id: row.get(2)?,
                record_json: row.get(3)?,
            })
        })?;
        let mut events = Vec::new();
        for row in rows {
            events.push(row?);
        }
        Ok(events)
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
