//! Built-in tools.

use async_trait::async_trait;
use serde_json::{json, Value};

use crate::{Tool, ToolError};

/// All built-in tools, ready to register.
pub fn all() -> Vec<Box<dyn Tool>> {
    vec![
        Box::new(ReadFile),
        Box::new(WriteFile),
        Box::new(Edit),
        Box::new(Bash),
    ]
}

/// `read_file(path)` — read a file's text content.
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
            "properties": { "path": { "type": "string", "description": "Absolute or relative path." } },
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

/// `write_file(path, content)` — create or overwrite a file's text content.
pub struct WriteFile;

#[async_trait]
impl Tool for WriteFile {
    fn name(&self) -> &str {
        "write_file"
    }
    fn description(&self) -> &str {
        "Write UTF-8 text content to a file, creating or overwriting it."
    }
    fn parameters(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "path": { "type": "string" },
                "content": { "type": "string" }
            },
            "required": ["path", "content"]
        })
    }
    async fn execute(&self, args: &Value) -> Result<String, ToolError> {
        let path = args
            .get("path")
            .and_then(|v| v.as_str())
            .ok_or_else(|| ToolError::BadArgs("missing 'path'".into()))?;
        let content = args
            .get("content")
            .and_then(|v| v.as_str())
            .ok_or_else(|| ToolError::BadArgs("missing 'content'".into()))?;
        let n = content.len();
        tokio::fs::write(path, content)
            .await
            .map_err(|e| ToolError::Io(format!("{path}: {e}")))?;
        Ok(format!("wrote {n} bytes to {path}"))
    }
}

/// `edit(path, old_string, new_string)` — replace exactly one occurrence.
pub struct Edit;

#[async_trait]
impl Tool for Edit {
    fn name(&self) -> &str {
        "edit"
    }
    fn description(&self) -> &str {
        "Replace exactly one occurrence of old_string with new_string in a file. Errors if old_string is absent or not unique."
    }
    fn parameters(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "path": { "type": "string" },
                "old_string": { "type": "string" },
                "new_string": { "type": "string" }
            },
            "required": ["path", "old_string", "new_string"]
        })
    }
    async fn execute(&self, args: &Value) -> Result<String, ToolError> {
        let path = args
            .get("path")
            .and_then(|v| v.as_str())
            .ok_or_else(|| ToolError::BadArgs("missing 'path'".into()))?;
        let old_string = args
            .get("old_string")
            .and_then(|v| v.as_str())
            .ok_or_else(|| ToolError::BadArgs("missing 'old_string'".into()))?;
        let new_string = args
            .get("new_string")
            .and_then(|v| v.as_str())
            .ok_or_else(|| ToolError::BadArgs("missing 'new_string'".into()))?;

        let content = tokio::fs::read_to_string(path)
            .await
            .map_err(|e| ToolError::Io(format!("{path}: {e}")))?;
        match content.matches(old_string).count() {
            0 => Err(ToolError::Other("old_string not found".into())),
            1 => {
                let updated = content.replacen(old_string, new_string, 1);
                tokio::fs::write(path, &updated)
                    .await
                    .map_err(|e| ToolError::Io(format!("{path}: {e}")))?;
                Ok(format!("edited {path}"))
            }
            n => Err(ToolError::Other(format!(
                "old_string matches {n} times; must be unique"
            ))),
        }
    }
}

/// `bash(command)` — run a shell command via `sh -c`.
///
/// WARNING: unsandboxed in the walking skeleton — the agent can run arbitrary
/// commands. Production needs an OS-level sandbox with a request/approve
/// boundary (like the upstream sandbox-boundary model).
pub struct Bash;

#[async_trait]
impl Tool for Bash {
    fn name(&self) -> &str {
        "bash"
    }
    fn description(&self) -> &str {
        "Run a shell command via `sh -c` and return stdout, stderr, and exit code. (Unsandboxed.)"
    }
    fn parameters(&self) -> Value {
        json!({
            "type": "object",
            "properties": { "command": { "type": "string" } },
            "required": ["command"]
        })
    }
    async fn execute(&self, args: &Value) -> Result<String, ToolError> {
        let command = args
            .get("command")
            .and_then(|v| v.as_str())
            .ok_or_else(|| ToolError::BadArgs("missing 'command'".into()))?;
        let output = tokio::process::Command::new("sh")
            .arg("-c")
            .arg(command)
            .output()
            .await
            .map_err(|e| ToolError::Other(format!("spawn failed: {e}")))?;
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        let code = output.status.code().unwrap_or(-1);
        Ok(format!("exit {code}\nstdout:\n{stdout}stderr:\n{stderr}"))
    }
}
