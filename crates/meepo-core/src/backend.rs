//! AgentBackend port — the execution-engine seam.
//!
//! The runner drives a turn by calling [`AgentBackend::send`] and consuming
//! the returned event stream, mapping SessionEvents into canonical
//! RuntimeEvents. Concrete backends: [`FakeBackend`] (here, for tests and the
//! walking skeleton) and a real provider backend (later phase).

use async_trait::async_trait;
use futures::stream::{BoxStream, StreamExt};
use serde::{Deserialize, Serialize};

use crate::SessionEvent;

/// Which backend implementation is driving the session. Wire values carry a
/// hyphen, so the two non-trivial variants are renamed explicitly.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BackendKind {
    #[serde(rename = "ai-sdk")]
    AiSdk,
    #[serde(rename = "fake")]
    Fake,
    #[serde(rename = "pi-agent")]
    PiAgent,
}

/// Why the caller asked to stop an in-flight turn.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendStopReason {
    UserStop,
    Redirect,
}

/// When the stop should take effect.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendStopMode {
    Immediate,
    AfterStep,
}

/// Input to one `send` invocation. Walking-skeleton subset; the full surface
/// (context, runtimeContext, steering, continuation, hosted interaction, ...)
/// is filled in across later phases.
#[derive(Debug, Clone)]
pub struct BackendSendInput {
    pub turn_id: String,
    pub text: String,
    pub run_id: Option<String>,
    pub invocation_id: Option<String>,
    pub max_steps: Option<u32>,
}

/// Opaque boxed error; a typed backend error is deferred.
pub type BackendResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// Execution-engine port. `send` returns a stream (not a future): it produces
/// a stream handle synchronously and the runner drives it asynchronously.
#[async_trait]
pub trait AgentBackend: Send {
    fn kind(&self) -> BackendKind;
    fn session_id(&self) -> &str;
    fn send<'a>(&'a mut self, input: &'a BackendSendInput) -> BoxStream<'a, SessionEvent>;
    async fn stop(
        &mut self,
        reason: BackendStopReason,
        mode: Option<BackendStopMode>,
    ) -> BackendResult<()>;
    async fn dispose(&mut self) -> BackendResult<()>;
}

/// Scripted backend for tests and the walking skeleton: `send` replays a
/// fixed event sequence, ignoring the input.
pub struct FakeBackend {
    session_id: String,
    script: Vec<SessionEvent>,
}

impl FakeBackend {
    pub fn new(session_id: impl Into<String>, script: Vec<SessionEvent>) -> Self {
        Self {
            session_id: session_id.into(),
            script,
        }
    }
}

#[async_trait]
impl AgentBackend for FakeBackend {
    fn kind(&self) -> BackendKind {
        BackendKind::Fake
    }

    fn session_id(&self) -> &str {
        &self.session_id
    }

    fn send<'a>(&'a mut self, _input: &'a BackendSendInput) -> BoxStream<'a, SessionEvent> {
        futures::stream::iter(self.script.clone()).boxed()
    }

    async fn stop(
        &mut self,
        _reason: BackendStopReason,
        _mode: Option<BackendStopMode>,
    ) -> BackendResult<()> {
        Ok(())
    }

    async fn dispose(&mut self) -> BackendResult<()> {
        Ok(())
    }
}
