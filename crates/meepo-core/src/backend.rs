//! AgentBackend port — the execution-engine seam.

use async_trait::async_trait;
use futures::stream::{BoxStream, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio_util::sync::CancellationToken;

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
#[derive(Debug, Clone, PartialEq)]
pub struct AssistantToolCall {
    pub id: String,
    pub name: String,
    pub args: Value,
}

/// One signed thinking block from an assistant message.
#[derive(Debug, Clone, PartialEq)]
pub struct ThinkingBlock {
    pub text: String,
    /// Anthropic signed thinking — MUST be re-sent verbatim on replay.
    pub signature: Option<String>,
}

/// Conversation history crossing the backend boundary. The runner threads tool
/// results back in as `Tool` messages so the model can continue.
#[derive(Debug, Clone, PartialEq)]
pub enum ChatMessage {
    User { content: String },
    Assistant {
        content: Option<String>,
        tool_calls: Vec<AssistantToolCall>,
        /// Signed thinking blocks (for extended thinking replay).
        thinking: Vec<ThinkingBlock>,
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
    /// Optional system prompt prepended to the request.
    pub system_prompt: Option<String>,
    /// OpenAI function-calling tool definitions (rendered by ToolRegistry).
    pub tools: Vec<Value>,
}

pub type BackendResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// Cooperative cancellation handle threaded into a backend `send`.
///
/// The host holds the originating [`CancellationToken`] and cancels it on
/// `turn.stop`; the backend observes the request via
/// [`cancelled`](StopToken::cancelled) / [`is_cancelled`](StopToken::is_cancelled)
/// at its await points, stops cleanly, and yields a terminal event so the
/// one-terminal-per-run invariant holds. Legacy/embedded callers pass
/// [`StopToken::never`].
#[derive(Debug, Clone)]
pub struct StopToken(CancellationToken);

impl StopToken {
    /// A token that can never be cancelled (the legacy embedded path).
    pub fn never() -> Self {
        Self(CancellationToken::new())
    }

    /// Wrap an existing token; the caller keeps a clone to call `cancel()`.
    pub fn from_token(token: CancellationToken) -> Self {
        Self(token)
    }

    /// The inner token (for callers that need to register child tokens etc.).
    pub fn inner(&self) -> &CancellationToken {
        &self.0
    }

    /// Whether cancellation has been requested.
    pub fn is_cancelled(&self) -> bool {
        self.0.is_cancelled()
    }

    /// Resolves when cancellation is requested. For `tokio::select!` arms.
    pub async fn cancelled(&self) {
        self.0.cancelled().await
    }
}

/// Port for executing tools, used by backends that drive an internal tool
/// loop. Concrete implementation lives in meepo-tools (ToolRegistry); this
/// trait keeps core decoupled.
#[async_trait]
pub trait ToolExecutor: Send + Sync {
    async fn execute(&self, name: &str, args: &Value) -> Result<String, String>;
    fn openai_functions(&self) -> Vec<Value>;
}

/// Execution-engine port. `send` returns a stream (not a future): it produces
/// a stream handle synchronously and the runner drives it asynchronously.
/// `compact_history` asks the model to summarize a prefix of messages so the
/// next request can carry a smaller continuation view (the ledger is not
/// mutated — compaction is a projection).
#[async_trait]
pub trait AgentBackend: Send {
    fn kind(&self) -> BackendKind;
    fn session_id(&self) -> &str;
    fn send<'a>(&'a mut self, input: &'a BackendSendInput) -> BoxStream<'a, SessionEvent>;

    /// As [`send`](AgentBackend::send) but with a cooperative [`StopToken`]. The
    /// default delegates to `send` (ignores the token); a backend that supports
    /// clean interruption overrides this to observe the token at its await
    /// points and yield a terminal event on cancel.
    fn send_cancellable<'a>(
        &'a mut self,
        input: &'a BackendSendInput,
        _stop: StopToken,
    ) -> BoxStream<'a, SessionEvent> {
        self.send(input)
    }
    async fn stop(
        &mut self,
        reason: BackendStopReason,
        mode: Option<BackendStopMode>,
    ) -> BackendResult<()>;
    async fn dispose(&mut self) -> BackendResult<()>;
    /// Produce a continuation summary of `messages` (the older prefix being
    /// folded out of the working context). Not a tool-bearing agent call.
    async fn compact_history(&self, messages: &[ChatMessage]) -> BackendResult<String>;
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

    async fn compact_history(&self, messages: &[ChatMessage]) -> BackendResult<String> {
        Ok(format!("[fake compact of {} messages]", messages.len()))
    }
}
