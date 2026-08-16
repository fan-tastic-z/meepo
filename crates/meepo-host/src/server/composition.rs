//! Composition — the host's domain services: the storage root, one
//! [`SessionManager`] per session, and the backend factory that builds an
//! [`AgentBackend`] for a turn. The binary wires a real provider factory; tests
//! inject a scripted one.

use std::collections::HashMap;
use std::sync::Arc;

use meepo_core::AgentBackend;
use meepo_runtime::SessionManager;
use meepo_storage::SqliteStore;
use tokio::sync::Mutex;

/// Builds a fresh backend for one turn of `session_id` (backends are
/// stateless; history lives in the SessionManager). The backend must be
/// `Send + Sync` so the drain task's turn stream is `Send`.
pub type BackendFactory =
    Arc<dyn Fn(&str) -> Box<dyn AgentBackend + Send + Sync> + Send + Sync>;

pub struct Composition {
    store: Arc<SqliteStore>,
    backend_factory: BackendFactory,
    system_prompt: Option<String>,
    sessions: Mutex<HashMap<String, Arc<Mutex<SessionManager>>>>,
}

impl Composition {
    pub fn new(
        store: Arc<SqliteStore>,
        backend_factory: BackendFactory,
        system_prompt: Option<String>,
    ) -> Self {
        Self {
            store,
            backend_factory,
            system_prompt,
            sessions: Mutex::new(HashMap::new()),
        }
    }

    pub fn store(&self) -> &Arc<SqliteStore> {
        &self.store
    }

    pub fn system_prompt(&self) -> Option<&str> {
        self.system_prompt.as_deref()
    }

    /// Get-or-resume the session manager for `session_id` (recovery scan runs
    /// on first touch after boot).
    pub async fn session(&self, session_id: &str) -> Arc<Mutex<SessionManager>> {
        let mut sessions = self.sessions.lock().await;
        if let Some(s) = sessions.get(session_id) {
            return s.clone();
        }
        let manager = SessionManager::resume(session_id, &*self.store).await;
        let shared = Arc::new(Mutex::new(manager));
        sessions.insert(session_id.to_string(), shared.clone());
        shared
    }

    /// Build a backend for one turn.
    pub fn build_backend(&self, session_id: &str) -> Box<dyn AgentBackend + Send + Sync> {
        (self.backend_factory)(session_id)
    }
}
