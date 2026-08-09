//! SqliteStore — a persistent [`RuntimeEventStore`] backed by SQLite (rusqlite).
//!
//! Walking-skeleton schema (meepo's own): one append-only `runtime_events`
//! table keyed by autoincrement seq, storing canonical JSON payloads plus a
//! denormalized status column for terminal-event checks. Byte-compatible
//! alignment with the upstream `runtime.sqlite` layout arrives in a later
//! phase; this crate currently implements the same trait with its own schema.

use std::path::Path;
use std::sync::Mutex;

use async_trait::async_trait;
use rusqlite::Connection;

use meepo_core::{Durability, RuntimeEvent, RuntimeEventStore, StoreResult};

pub struct SqliteStore {
    conn: Mutex<Connection>,
}

impl SqliteStore {
    /// Open (or create) a SQLite database file.
    pub fn open(path: impl AsRef<Path>) -> StoreResult<Self> {
        let conn = Connection::open(path)?;
        Self::init(conn)
    }

    /// In-memory database, for tests.
    pub fn in_memory() -> StoreResult<Self> {
        let conn = Connection::open_in_memory()?;
        Self::init(conn)
    }

    fn init(conn: Connection) -> StoreResult<Self> {
        conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS runtime_events (
                seq        INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL,
                run_id     TEXT NOT NULL,
                status     TEXT,
                payload    TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_run     ON runtime_events(session_id, run_id, seq);
            CREATE INDEX IF NOT EXISTS idx_session ON runtime_events(session_id, seq);
            "#,
        )?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    fn status_string(event: &RuntimeEvent) -> Option<String> {
        event.status.and_then(|s| {
            serde_json::to_value(s)
                .ok()
                .and_then(|v| v.as_str().map(String::from))
        })
    }
}

#[async_trait]
impl RuntimeEventStore for SqliteStore {
    fn durability(&self) -> Durability {
        Durability::Canonical
    }

    async fn append_runtime_event(
        &self,
        session_id: &str,
        run_id: &str,
        event: RuntimeEvent,
        _durable: bool,
    ) -> StoreResult<()> {
        let payload = event.to_canonical_json()?;
        let status = Self::status_string(&event);
        let conn = self.conn.lock().expect("db lock poisoned");
        conn.execute(
            "INSERT INTO runtime_events (session_id, run_id, status, payload) \
             VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params![session_id, run_id, status, payload],
        )?;
        Ok(())
    }

    async fn ensure_terminal_runtime_event_durable(
        &self,
        session_id: &str,
        run_id: &str,
        event: RuntimeEvent,
    ) -> StoreResult<()> {
        // Drop the lock before awaiting append (append takes it again).
        let has_terminal: bool = {
            let conn = self.conn.lock().expect("db lock poisoned");
            conn.query_row(
                "SELECT EXISTS(SELECT 1 FROM runtime_events \
                 WHERE session_id = ?1 AND run_id = ?2 \
                 AND status IN ('completed','failed','aborted','cancelled'))",
                rusqlite::params![session_id, run_id],
                |row| row.get::<_, bool>(0),
            )?
        };
        if !has_terminal {
            self.append_runtime_event(session_id, run_id, event, true).await?;
        }
        Ok(())
    }

    async fn read_runtime_events(
        &self,
        session_id: &str,
        run_id: &str,
    ) -> StoreResult<Vec<RuntimeEvent>> {
        let conn = self.conn.lock().expect("db lock poisoned");
        let mut stmt = conn.prepare(
            "SELECT payload FROM runtime_events \
             WHERE session_id = ?1 AND run_id = ?2 ORDER BY seq",
        )?;
        let rows = stmt.query_map(rusqlite::params![session_id, run_id], |row| {
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

    async fn read_session_runtime_events(
        &self,
        session_id: &str,
    ) -> StoreResult<Vec<RuntimeEvent>> {
        let conn = self.conn.lock().expect("db lock poisoned");
        let mut stmt = conn.prepare(
            "SELECT payload FROM runtime_events \
             WHERE session_id = ?1 ORDER BY seq",
        )?;
        let rows = stmt.query_map(rusqlite::params![session_id], |row| {
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
}
