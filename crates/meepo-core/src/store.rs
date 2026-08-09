//! RuntimeEventStore port — the append-only ledger persistence seam.
//!
//! The runner appends canonical facts here and reads them back for replay and
//! recovery. Two durability tiers: best-effort (may drop on crash) and
//! canonical (the authority; a durable-write failure fails the run closed). A
//! walking-skeleton in-memory implementation is included; the SQLite canonical
//! store arrives in a later phase.

use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;

use crate::RuntimeEvent;

/// Persistence tier. Canonical stores are the authority; a durable-write
/// failure there fails the active run closed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Durability {
    #[default]
    BestEffort,
    Canonical,
}

/// Typed store error is deferred; opaque boxed error for now.
pub type StoreResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// Append-only ledger port. Implementations: in-memory (here), SQLite (later).
///
/// **Terminal-event invariant:** each `(session, run)` has at most one
/// accepted terminal [`RuntimeEvent`], and it must be durable before the run
/// header closes. Recovery relies on this.
#[async_trait]
pub trait RuntimeEventStore: Send + Sync {
    fn durability(&self) -> Durability {
        Durability::BestEffort
    }

    /// Append one fact. `durable` requests a stable-storage barrier (canonical
    /// stores may reject on a write failure; best-effort stores accept and
    /// ignore the flag).
    async fn append_runtime_event(
        &self,
        session_id: &str,
        run_id: &str,
        event: RuntimeEvent,
        durable: bool,
    ) -> StoreResult<()>;

    /// Ensure the terminal event is present and its barrier established.
    /// Idempotent: a no-op if a terminal event already exists for this run.
    async fn ensure_terminal_runtime_event_durable(
        &self,
        session_id: &str,
        run_id: &str,
        event: RuntimeEvent,
    ) -> StoreResult<()>;

    /// Read one run's events in commit order.
    async fn read_runtime_events(
        &self,
        session_id: &str,
        run_id: &str,
    ) -> StoreResult<Vec<RuntimeEvent>>;

    /// Read every event for a session (across all runs), in commit order.
    async fn read_session_runtime_events(
        &self,
        session_id: &str,
    ) -> StoreResult<Vec<RuntimeEvent>>;
}

/// In-memory store for the walking skeleton and tests. Not durable across
/// restarts; `durable` is accepted and ignored.
#[derive(Default)]
pub struct InMemoryRuntimeEventStore {
    durability: Durability,
    // (session_id, run_id) -> events in append order.
    runs: Mutex<HashMap<(String, String), Vec<RuntimeEvent>>>,
}

impl InMemoryRuntimeEventStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_durability(durability: Durability) -> Self {
        Self {
            durability,
            runs: Mutex::new(HashMap::new()),
        }
    }
}

#[async_trait]
impl RuntimeEventStore for InMemoryRuntimeEventStore {
    fn durability(&self) -> Durability {
        self.durability
    }

    async fn append_runtime_event(
        &self,
        session_id: &str,
        run_id: &str,
        event: RuntimeEvent,
        _durable: bool,
    ) -> StoreResult<()> {
        let mut runs = self.runs.lock().expect("ledger lock poisoned");
        runs.entry((session_id.into(), run_id.into()))
            .or_default()
            .push(event);
        Ok(())
    }

    async fn ensure_terminal_runtime_event_durable(
        &self,
        session_id: &str,
        run_id: &str,
        event: RuntimeEvent,
    ) -> StoreResult<()> {
        let mut runs = self.runs.lock().expect("ledger lock poisoned");
        let entry = runs
            .entry((session_id.into(), run_id.into()))
            .or_default();
        // Idempotent: a terminal event already exists -> no-op.
        let has_terminal = entry
            .iter()
            .any(|e| e.status.is_some_and(|s| s.is_terminal()));
        if !has_terminal {
            entry.push(event);
        }
        Ok(())
    }

    async fn read_runtime_events(
        &self,
        session_id: &str,
        run_id: &str,
    ) -> StoreResult<Vec<RuntimeEvent>> {
        let runs = self.runs.lock().expect("ledger lock poisoned");
        Ok(runs
            .get(&(session_id.into(), run_id.into()))
            .cloned()
            .unwrap_or_default())
    }

    async fn read_session_runtime_events(
        &self,
        session_id: &str,
    ) -> StoreResult<Vec<RuntimeEvent>> {
        let runs = self.runs.lock().expect("ledger lock poisoned");
        let mut all: Vec<&RuntimeEvent> = Vec::new();
        for ((sid, _rid), events) in runs.iter() {
            if sid == session_id {
                all.extend(events.iter());
            }
        }
        // Cross-run order by monotonic event id.
        all.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(all.into_iter().cloned().collect())
    }
}
