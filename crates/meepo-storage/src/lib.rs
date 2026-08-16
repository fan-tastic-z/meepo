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
use meepo_headless::{TaskEvent, TaskRunStore};

const SCHEMA_VERSION: i64 = 13;

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

    -- Durable root-turn admission chain, one row per admitted turn per session.
    -- Append-only; each admission is immutable once written. A poisoned session
    -- refuses further admissions. Survives host restart so the host can
    -- re-establish the live root turn.
    CREATE TABLE IF NOT EXISTS root_admission_chain (
        session_id            TEXT NOT NULL,
        turn_id               TEXT NOT NULL,
        run_id                TEXT NOT NULL,
        previous_root_turn_id TEXT,
        identity_json         TEXT NOT NULL,
        admitted_at           INTEGER NOT NULL,
        poisoned              INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (session_id, turn_id)
    );
    CREATE INDEX IF NOT EXISTS root_admission_chain_tip
        ON root_admission_chain(session_id, admitted_at DESC);

    -- Session catalog: one row per session, with optimistic concurrency via
    -- `revision` (writers pass expectedRevision; a clash is a no-op + conflict).
    CREATE TABLE IF NOT EXISTS sessions (
        session_id   TEXT PRIMARY KEY,
        name         TEXT NOT NULL DEFAULT '',
        labels_json  TEXT NOT NULL DEFAULT '[]',
        revision     INTEGER NOT NULL DEFAULT 1 CHECK (revision > 0),
        is_archived  INTEGER NOT NULL DEFAULT 0,
        cwd          TEXT NOT NULL DEFAULT '',
        config_json  TEXT NOT NULL DEFAULT '{}',
        read_marker  TEXT,
        created_at   INTEGER NOT NULL,
        updated_at   INTEGER NOT NULL
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

/// One entry in a session's durable root-turn admission chain (a row in
/// `root_admission_chain`). Append-only per session; immutable once written; a
/// poisoned session refuses further admissions.
#[derive(Debug, Clone, PartialEq)]
pub struct RootAdmissionRecord {
    pub session_id: String,
    pub turn_id: String,
    pub run_id: String,
    /// The previous turn in the chain (None for the first).
    pub previous_root_turn_id: Option<String>,
    /// Immutable identity blob (deep-equality checked for immutability).
    pub identity_json: String,
    pub admitted_at: i64,
    pub poisoned: bool,
}

/// One session catalog row.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionRecord {
    pub session_id: String,
    pub name: String,
    pub labels: Vec<String>,
    pub revision: i64,
    pub is_archived: bool,
    pub cwd: String,
    pub config_json: String,
    pub read_marker: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}

/// A partial session patch; None fields are left unchanged.
#[derive(Debug, Clone, Default)]
pub struct SessionPatch {
    pub name: Option<String>,
    pub labels: Option<Vec<String>>,
    pub is_archived: Option<bool>,
    pub cwd: Option<String>,
    pub config_json: Option<String>,
    pub read_marker: Option<String>,
}

/// Result of an optimistic-concurrency update.
#[derive(Debug, Clone, PartialEq)]
pub enum SessionUpdate {
    Committed(SessionRecord),
    RevisionConflict { expected: i64, current: i64 },
}

impl SqliteStore {
    /// Append one admission record. Chain extension (previousRootTurnId == tip)
    /// is enforced by the caller; this row is the durable fact.
    pub async fn extend_admission_chain(&self, rec: &RootAdmissionRecord) -> StoreResult<()> {
        let conn = self.conn.lock().expect("db lock poisoned");
        conn.execute(
            "INSERT OR REPLACE INTO root_admission_chain \
             (session_id, turn_id, run_id, previous_root_turn_id, identity_json, admitted_at, poisoned) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            rusqlite::params![
                rec.session_id,
                rec.turn_id,
                rec.run_id,
                rec.previous_root_turn_id,
                rec.identity_json,
                rec.admitted_at,
                rec.poisoned as i64,
            ],
        )?;
        Ok(())
    }

    /// The most-recently-admitted turn for a session (the chain tip), if any.
    pub async fn read_admission_tip(&self, session_id: &str) -> StoreResult<Option<RootAdmissionRecord>> {
        let conn = self.conn.lock().expect("db lock poisoned");
        let row = conn.query_row(
            "SELECT session_id, turn_id, run_id, previous_root_turn_id, identity_json, admitted_at, poisoned \
             FROM root_admission_chain WHERE session_id = ?1 ORDER BY admitted_at DESC LIMIT 1",
            rusqlite::params![session_id],
            |row| {
                Ok(RootAdmissionRecord {
                    session_id: row.get(0)?,
                    turn_id: row.get(1)?,
                    run_id: row.get(2)?,
                    previous_root_turn_id: row.get(3)?,
                    identity_json: row.get(4)?,
                    admitted_at: row.get(5)?,
                    poisoned: row.get::<_, i64>(6)? != 0,
                })
            },
        );
        match row {
            Ok(r) => Ok(Some(r)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Mark every admission for a session poisoned (fail-stop).
    pub async fn mark_admission_poisoned(&self, session_id: &str) -> StoreResult<()> {
        let conn = self.conn.lock().expect("db lock poisoned");
        conn.execute(
            "UPDATE root_admission_chain SET poisoned = 1 WHERE session_id = ?1",
            rusqlite::params![session_id],
        )?;
        Ok(())
    }

    /// Read the whole chain for a session in admission order (oldest first).
    pub async fn recover_admission_chain(
        &self,
        session_id: &str,
    ) -> StoreResult<Vec<RootAdmissionRecord>> {
        let conn = self.conn.lock().expect("db lock poisoned");
        let mut stmt = conn.prepare(
            "SELECT session_id, turn_id, run_id, previous_root_turn_id, identity_json, admitted_at, poisoned \
             FROM root_admission_chain WHERE session_id = ?1 ORDER BY admitted_at ASC",
        )?;
        let rows = stmt.query_map(rusqlite::params![session_id], |row| {
            Ok(RootAdmissionRecord {
                session_id: row.get(0)?,
                turn_id: row.get(1)?,
                run_id: row.get(2)?,
                previous_root_turn_id: row.get(3)?,
                identity_json: row.get(4)?,
                admitted_at: row.get(5)?,
                poisoned: row.get::<_, i64>(6)? != 0,
            })
        })?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r?);
        }
        Ok(out)
    }

    /// The durable RuntimeEvent high-water mark for a run: how many events were
    /// durably committed. The resume protocol keys continuation off this count.
    pub async fn read_runtime_event_high_water(
        &self,
        session_id: &str,
        run_id: &str,
    ) -> StoreResult<i64> {
        let conn = self.conn.lock().expect("db lock poisoned");
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM runtime_events WHERE session_id = ?1 AND run_id = ?2",
            rusqlite::params![session_id, run_id],
            |row| row.get(0),
        )?;
        Ok(count)
    }

    // ── session catalog ──

    const SESSION_COLS: &'static str =
        "session_id, name, labels_json, revision, is_archived, cwd, config_json, read_marker, created_at, updated_at";

    fn session_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<SessionRecord> {
        let labels_json: String = row.get(2)?;
        Ok(SessionRecord {
            session_id: row.get(0)?,
            name: row.get(1)?,
            labels: serde_json::from_str(&labels_json).unwrap_or_default(),
            revision: row.get(3)?,
            is_archived: row.get::<_, i64>(4)? != 0,
            cwd: row.get(5)?,
            config_json: row.get(6)?,
            read_marker: row.get(7)?,
            created_at: row.get(8)?,
            updated_at: row.get(9)?,
        })
    }

    /// Insert a session row (idempotent: an existing id is left as-is).
    pub async fn create_session(&self, rec: &SessionRecord) -> StoreResult<()> {
        let conn = self.conn.lock().expect("db lock poisoned");
        conn.execute(
            "INSERT OR IGNORE INTO sessions \
             (session_id, name, labels_json, revision, is_archived, cwd, config_json, read_marker, created_at, updated_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            rusqlite::params![
                rec.session_id,
                rec.name,
                serde_json::to_string(&rec.labels).unwrap_or_else(|_| "[]".into()),
                rec.revision,
                rec.is_archived as i64,
                rec.cwd,
                rec.config_json,
                rec.read_marker,
                rec.created_at,
                rec.updated_at,
            ],
        )?;
        Ok(())
    }

    pub async fn get_session(&self, session_id: &str) -> StoreResult<Option<SessionRecord>> {
        let conn = self.conn.lock().expect("db lock poisoned");
        let row = conn.query_row(
            &format!("SELECT {} FROM sessions WHERE session_id = ?1", Self::SESSION_COLS),
            rusqlite::params![session_id],
            Self::session_from_row,
        );
        match row {
            Ok(r) => Ok(Some(r)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn list_sessions(&self) -> StoreResult<Vec<SessionRecord>> {
        let conn = self.conn.lock().expect("db lock poisoned");
        let mut stmt = conn.prepare(&format!(
            "SELECT {} FROM sessions ORDER BY created_at ASC",
            Self::SESSION_COLS
        ))?;
        let rows = stmt.query_map([], Self::session_from_row)?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r?);
        }
        Ok(out)
    }

    /// Optimistic-concurrency update: applies `patch` only when the row's
    /// revision still equals `expected_revision` (bumping it by one).
    pub async fn update_session(
        &self,
        session_id: &str,
        expected_revision: i64,
        patch: &SessionPatch,
    ) -> StoreResult<SessionUpdate> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        let conn = self.conn.lock().expect("db lock poisoned");
        let current = match conn.query_row(
            &format!("SELECT {} FROM sessions WHERE session_id = ?1", Self::SESSION_COLS),
            rusqlite::params![session_id],
            Self::session_from_row,
        ) {
            Ok(r) => r,
            Err(rusqlite::Error::QueryReturnedNoRows) => {
                return Ok(SessionUpdate::RevisionConflict { expected: expected_revision, current: 0 })
            }
            Err(e) => return Err(e.into()),
        };
        let mut next = current.clone();
        if let Some(n) = &patch.name {
            next.name = n.clone();
        }
        if let Some(l) = &patch.labels {
            next.labels = l.clone();
        }
        if let Some(a) = patch.is_archived {
            next.is_archived = a;
        }
        if let Some(c) = &patch.cwd {
            next.cwd = c.clone();
        }
        if let Some(cfg) = &patch.config_json {
            next.config_json = cfg.clone();
        }
        if let Some(rm) = &patch.read_marker {
            next.read_marker = Some(rm.clone());
        }
        next.revision = current.revision + 1;
        next.updated_at = now;
        let changed = conn.execute(
            "UPDATE sessions SET name=?2, labels_json=?3, revision=?4, is_archived=?5, cwd=?6, \
             config_json=?7, read_marker=?8, updated_at=?9 \
             WHERE session_id = ?1 AND revision = ?10",
            rusqlite::params![
                session_id,
                next.name,
                serde_json::to_string(&next.labels).unwrap_or_else(|_| "[]".into()),
                next.revision,
                next.is_archived as i64,
                next.cwd,
                next.config_json,
                next.read_marker,
                next.updated_at,
                expected_revision,
            ],
        )?;
        if changed == 0 {
            return Ok(SessionUpdate::RevisionConflict {
                expected: expected_revision,
                current: current.revision,
            });
        }
        Ok(SessionUpdate::Committed(next))
    }

    pub async fn remove_session(&self, session_id: &str) -> StoreResult<bool> {
        let conn = self.conn.lock().expect("db lock poisoned");
        let n = conn.execute(
            "DELETE FROM sessions WHERE session_id = ?1",
            rusqlite::params![session_id],
        )?;
        Ok(n > 0)
    }
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

#[async_trait]
impl TaskRunStore for SqliteStore {
    async fn append_event(
        &self,
        task_run_id: &str,
        sequence: i64,
        event: &TaskEvent,
    ) -> StoreResult<()> {
        let record_json = serde_json::to_string(event)?;
        let event_id = format!("{task_run_id}-{sequence}");
        self.append_task_run_event(&TaskRunEvent {
            task_run_id: task_run_id.into(),
            sequence,
            event_id,
            record_json,
        })
        .await
    }

    async fn read_events(&self, task_run_id: &str) -> StoreResult<Vec<TaskEvent>> {
        let rows = self.read_task_run_events(task_run_id).await?;
        rows.into_iter()
            .map(|r| serde_json::from_str(&r.record_json).map_err(Into::into))
            .collect()
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
