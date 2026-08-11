//! Tool system — the agent's "hands".
//!
//! A [`Tool`] is a named, JSON-Schema-described, async operation the model can
//! invoke. The [`ToolRegistry`] holds tools, exposes them in the OpenAI
//! function-calling wire shape, and dispatches executions by name. Builtins
//! live in [`builtin`].

pub mod builtin;

use std::collections::HashMap;

use async_trait::async_trait;
use serde_json::{json, Value};

pub use builtin::{all, Bash, Edit, Glob, Grep, ReadFile, WriteFile};

/// One invocable tool. The default `openai_function` renders the standard
/// OpenAI function-calling tool definition.
#[async_trait]
pub trait Tool: Send + Sync {
    fn name(&self) -> &str;
    fn description(&self) -> &str;
    /// JSON Schema for the parameters object.
    fn parameters(&self) -> Value;
    /// Execute the tool; return its result as a model-facing string.
    async fn execute(&self, args: &Value) -> Result<String, ToolError>;

    /// Render the OpenAI function-calling tool definition:
    /// `{ "type":"function", "function": { name, description, parameters } }`.
    fn openai_function(&self) -> Value {
        json!({
            "type": "function",
            "function": {
                "name": self.name(),
                "description": self.description(),
                "parameters": self.parameters(),
            }
        })
    }
}

/// Why a tool call failed.
#[derive(Debug)]
pub enum ToolError {
    NotFound(String),
    BadArgs(String),
    Io(String),
    Other(String),
}

impl std::fmt::Display for ToolError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ToolError::NotFound(n) => write!(f, "tool not found: {n}"),
            ToolError::BadArgs(m) => write!(f, "bad arguments: {m}"),
            ToolError::Io(m) => write!(f, "io error: {m}"),
            ToolError::Other(m) => write!(f, "{m}"),
        }
    }
}

impl std::error::Error for ToolError {}

/// Owned collection of tools, dispatchable by name.
pub struct ToolRegistry {
    tools: HashMap<String, Box<dyn Tool>>,
}

impl ToolRegistry {
    pub fn new() -> Self {
        Self {
            tools: HashMap::new(),
        }
    }

    pub fn register(&mut self, tool: Box<dyn Tool>) {
        self.tools.insert(tool.name().to_string(), tool);
    }

    pub fn names(&self) -> Vec<&str> {
        self.tools.keys().map(|s| s.as_str()).collect()
    }

    /// All tool definitions in the OpenAI function-calling shape, for the
    /// `tools` request field.
    pub fn openai_functions(&self) -> Vec<Value> {
        self.tools.values().map(|t| t.openai_function()).collect()
    }

    pub async fn execute(&self, name: &str, args: &Value) -> Result<String, ToolError> {
        let tool = self
            .tools
            .get(name)
            .ok_or_else(|| ToolError::NotFound(name.to_string()))?;
        tool.execute(args).await
    }
}

impl Default for ToolRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl meepo_core::ToolExecutor for ToolRegistry {
    async fn execute(&self, name: &str, args: &Value) -> Result<String, String> {
        ToolRegistry::execute(self, name, args)
            .await
            .map_err(|e| e.to_string())
    }

    fn openai_functions(&self) -> Vec<Value> {
        ToolRegistry::openai_functions(self)
    }
}
