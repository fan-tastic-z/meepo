//! Meepo runtime — the agent execution layer.
//!
//! Sits above [`meepo-core`] contracts: drives a backend, maps the session
//! event stream into canonical RuntimeEvents, and enforces the per-invocation
//! terminal invariant. The runner intentionally does NOT own persistence — it
//! collects events and returns them; the orchestration layer above persists.

pub mod map_session_event;
pub mod projection;
pub mod runner;

pub use map_session_event::{map_session_event, InvocationContext};
pub use projection::messages_from_runtime_events;
pub use runner::{RunResult, RunStatus, RuntimeRunner, TurnEvent};

/// Default agent persona. CLI injects this unless `--system` overrides it.
pub const DEFAULT_SYSTEM_PROMPT: &str = "You are meepo, a local-first coding agent running on the user's machine. You have tools (read_file, write_file, edit, bash). Use them proactively: read before you change, run commands to verify, and never ask the user to do what a tool can do. Be concise and direct.";
