//! Operation dispatcher — flat `op-name -> handler` registry.
//!
//! A handler is an async closure `(input, OpContext) -> Outcome`. Unknown ops
//! resolve to `operation_unavailable`. The full spine completeness check (every
//! op in the spec has exactly one handler, no dupes) lands when composition
//! wires all handlers in phase 5+.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use serde_json::Value;

use crate::protocol::{LifecycleState, OpErrorCode};

/// Per-request context handed to every handler. Cheap to clone (the host
/// snapshots lifecycle at dispatch; phase 5 makes it live).
#[derive(Clone)]
pub struct OpContext {
    pub host_epoch: String,
    pub lifecycle: LifecycleState,
}

/// Handler outcome — a successful result value, or a declared op error.
pub enum Outcome {
    Ok(Value),
    Err { code: OpErrorCode, message: String },
}

pub type HandlerFuture = Pin<Box<dyn Future<Output = Outcome> + Send>>;
pub type Handler = Arc<dyn Fn(Value, OpContext) -> HandlerFuture + Send + Sync>;

pub struct Dispatcher {
    handlers: HashMap<String, Handler>,
}

impl Dispatcher {
    pub fn new() -> Self {
        Self { handlers: HashMap::new() }
    }

    pub fn register(&mut self, op: impl Into<String>, handler: Handler) {
        self.handlers.insert(op.into(), handler);
    }

    pub async fn dispatch(&self, op: &str, input: Value, ctx: OpContext) -> Outcome {
        match self.handlers.get(op) {
            Some(h) => h(input, ctx).await,
            None => Outcome::Err {
                code: OpErrorCode::OperationUnavailable,
                message: format!("unknown operation '{op}'"),
            },
        }
    }
}

impl Default for Dispatcher {
    fn default() -> Self {
        Self::new()
    }
}

/// Helper to box a handler future (`Arc::new(handler_of(...))`).
pub fn handler<F, Fut>(f: F) -> Handler
where
    F: Fn(Value, OpContext) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Outcome> + Send + 'static,
{
    Arc::new(move |input, ctx| Box::pin(f(input, ctx)))
}
