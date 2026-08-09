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
pub use runner::{RunResult, RunStatus, RuntimeRunner};
