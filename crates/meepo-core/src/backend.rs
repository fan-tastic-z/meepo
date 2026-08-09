//! AgentBackend port — the execution-engine seam.

use async_trait::async_trait;
use futures::stream::{BoxStream, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::SessionEvent;

/// Which backend implementation is driving the session.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BackendKind {
    #[serde(rename = "ai-sdk")]
    AiSdk,
    #[serde(rename = "fake")]
    Fake,
    #[serde(rename = "pi-agent")]
    PiAgent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendStopReason {
    UserStop,
    Redirect,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendStopMode {
    Immediate,
    AfterStep,
}

/// One tool call recorded in assistant history.
#[derive(Debug, Clone)]
pub struct AssistantToolCall {
    pub id: String,
    pub name: String,
    pub args: Value,
}

/// Conversation history crossing the backend boundary. The runner threads tool
/// results back in as `Tool` messages so the model can continue.
#[derive(Debug, Clone)]
pub enum ChatMessage {
    User { content: String },
    Assistant {
        content: Option<String>,
        tool_calls: Vec<AssistantToolCall>,
    },
    Tool { tool_call_id: String, content: String },
}

/// Input to one `send` invocation. `messages` is the full conversation history
/// (the runner appends assistant tool-calls and tool results between steps).
#[derive(Debug, Clone)]
pub struct BackendSendInput {
    pub turn_id: String,
    pub run_id: Option<String>,
    pub invocation_id: Option<String>,
    pub max_steps: Option<u32>,
    pub messages: Vec<ChatMessage>,
    /// OpenAI function-calling tool definitions (rendered by ToolRegistry).
    pub tools: Vec<Value>,
}

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

/// Scripted backend for tests and the walking skeleton.
///
/// `new(session, script)` replays one step on the first `send`. `new_stepped`
/// replays `steps[i]` on the i-th `send`, so a test can script a multi-step
/// tool loop (e.g. step 0 emits a tool_call, step 1 emits complete).
pub struct FakeBackend {
    session_id: String,
    steps: Vec<Vec<SessionEvent>>,
    calls: usize,
}

impl FakeBackend {
    pub fn new(session_id: impl Into<String>, script: Vec<SessionEvent>) -> Self {
        Self {
            session_id: session_id.into(),
            steps: vec![script],
            calls: 0,
        }
    }

    pub fn new_stepped(session_id: impl Into<String>, steps: Vec<Vec<SessionEvent>>) -> Self {
        Self {
            session_id: session_id.into(),
            steps,
            calls: 0,
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
        let step = self.steps.get(self.calls).cloned().unwrap_or_default();
        self.calls += 1;
        futures::stream::iter(step).boxed()
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
