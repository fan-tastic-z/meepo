//! Built-in tools.

use async_trait::async_trait;
use serde_json::{json, Value};

use crate::{Tool, ToolError};

/// `read_file(path)` — read a file's text content from the local filesystem.
pub struct ReadFile;

#[async_trait]
impl Tool for ReadFile {
    fn name(&self) -> &str {
        "read_file"
    }

    fn description(&self) -> &str {
        "Read the UTF-8 text content of a file at the given path."
    }

    fn parameters(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Absolute or relative path to the file."
                }
            },
            "required": ["path"]
        })
    }

    async fn execute(&self, args: &Value) -> Result<String, ToolError> {
        let path = args
            .get("path")
            .and_then(|v| v.as_str())
            .ok_or_else(|| ToolError::BadArgs("missing or non-string 'path'".into()))?;
        tokio::fs::read_to_string(path)
            .await
            .map_err(|e| ToolError::Io(format!("{path}: {e}")))
    }
}
