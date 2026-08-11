//! Built-in tools.

use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{json, Value};

use crate::{Tool, ToolError};

/// All built-in tools, ready to register (bash unsandboxed).
pub fn all() -> Vec<Box<dyn Tool>> {
    vec![
        Box::new(ReadFile),
        Box::new(WriteFile),
        Box::new(Edit),
        Box::new(Bash { sandbox: None }),
        Box::new(Glob),
        Box::new(Grep),
    ]
}

/// All built-in tools, with bash sandboxed via the given manager.
pub fn all_with_sandbox(sandbox: Arc<meepo_sandbox::SandboxManager>) -> Vec<Box<dyn Tool>> {
    vec![
        Box::new(ReadFile),
        Box::new(WriteFile),
        Box::new(Edit),
        Box::new(Bash { sandbox: Some(sandbox) }),
        Box::new(Glob),
        Box::new(Grep),
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

/// `bash(command)` — run a shell command via `sh -c`, optionally sandboxed.
pub struct Bash {
    sandbox: Option<Arc<meepo_sandbox::SandboxManager>>,
}

impl Default for Bash {
    fn default() -> Self {
        Self { sandbox: None }
    }
}

#[async_trait]
impl Tool for Bash {
    fn name(&self) -> &str {
        "bash"
    }
    fn description(&self) -> &str {
        "Run a shell command via `sh -c` and return stdout, stderr, and exit code."
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

        let cwd = std::env::current_dir()
            .map(|p| p.to_string_lossy().into_owned())
            .unwrap_or_else(|_| ".".into());

        // If a sandbox manager is configured, transform the command through it.
        let (program, cmd_args) = if let Some(ref sandbox) = self.sandbox {
            let cmd = meepo_sandbox::SandboxCommand {
                program: "sh".into(),
                args: vec!["-c".into(), command.into()],
                cwd: cwd.clone(),
                env: vec![],
                profile: meepo_sandbox::workspace_managed_profile(&cwd),
                path_context: meepo_sandbox::SandboxPathContext {
                    workspace_roots: vec![cwd.clone()],
                    tmpdir: Some("/tmp".into()),
                    ..Default::default()
                },
            };
            match sandbox.transform(&cmd) {
                meepo_sandbox::SandboxTransformResult::Ok(req) => {
                    let program = req.argv[0].clone();
                    let cmd_args = req.argv[1..].to_vec();
                    (program, cmd_args)
                }
                meepo_sandbox::SandboxTransformResult::Failed { message, .. } => {
                    return Err(ToolError::Other(format!("sandbox denied: {message}")));
                }
            }
        } else {
            ("sh".into(), vec!["-c".into(), command.into()])
        };

        let output = tokio::process::Command::new(&program)
            .args(&cmd_args)
            .output()
            .await
            .map_err(|e| ToolError::Other(format!("spawn failed: {e}")))?;
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        let code = output.status.code().unwrap_or(-1);
        Ok(format!("exit {code}\nstdout:\n{stdout}stderr:\n{stderr}"))
    }
}

/// `glob(pattern, path?)` — find files matching a glob pattern.
pub struct Glob;

#[async_trait]
impl Tool for Glob {
    fn name(&self) -> &str {
        "glob"
    }
    fn description(&self) -> &str {
        "Find files whose path (relative to `path`, default cwd) matches a glob pattern, e.g. **/*.rs."
    }
    fn parameters(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "pattern": { "type": "string" },
                "path": { "type": "string", "description": "Base directory (default cwd)." }
            },
            "required": ["pattern"]
        })
    }
    async fn execute(&self, args: &Value) -> Result<String, ToolError> {
        let pattern = args
            .get("pattern")
            .and_then(|v| v.as_str())
            .ok_or_else(|| ToolError::BadArgs("missing 'pattern'".into()))?;
        let base = args.get("path").and_then(|v| v.as_str()).unwrap_or(".");
        let pat = glob::Pattern::new(pattern)
            .map_err(|e| ToolError::BadArgs(format!("bad pattern: {e}")))?;
        let base = Path::new(base);
        let mut hits = Vec::new();
        for entry in walkdir::WalkDir::new(base).into_iter().filter_map(|e| e.ok()) {
            let rel = entry.path().strip_prefix(base).unwrap_or(entry.path());
            let rel_str = rel.to_string_lossy();
            if !rel_str.is_empty() && pat.matches(rel_str.as_ref()) {
                hits.push(entry.path().to_string_lossy().into_owned());
            }
            if hits.len() >= 1000 {
                break;
            }
        }
        if hits.is_empty() {
            Ok("(no matches)".into())
        } else {
            Ok(hits.join("\n"))
        }
    }
}

/// `grep(pattern, path?, include?)` — search file contents with a regex.
pub struct Grep;

#[async_trait]
impl Tool for Grep {
    fn name(&self) -> &str {
        "grep"
    }
    fn description(&self) -> &str {
        "Search file contents under `path` (default cwd) for a regex. Optional `include` glob filters filenames (e.g. *.rs)."
    }
    fn parameters(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "pattern": { "type": "string" },
                "path": { "type": "string", "description": "Base directory (default cwd)." },
                "include": { "type": "string", "description": "Filename glob to include (e.g. *.rs)." }
            },
            "required": ["pattern"]
        })
    }
    async fn execute(&self, args: &Value) -> Result<String, ToolError> {
        let pattern = args
            .get("pattern")
            .and_then(|v| v.as_str())
            .ok_or_else(|| ToolError::BadArgs("missing 'pattern'".into()))?;
        let base = args.get("path").and_then(|v| v.as_str()).unwrap_or(".");
        let include = args.get("include").and_then(|v| v.as_str());
        let re = regex::Regex::new(pattern)
            .map_err(|e| ToolError::BadArgs(format!("bad regex: {e}")))?;
        let inc = match include {
            Some(s) => Some(
                glob::Pattern::new(s).map_err(|e| ToolError::BadArgs(format!("bad include: {e}")))?,
            ),
            None => None,
        };
        let base = Path::new(base);
        let mut hits = Vec::new();
        for entry in walkdir::WalkDir::new(base).into_iter().filter_map(|e| e.ok()) {
            if !entry.file_type().is_file() {
                continue;
            }
            let fname = entry.file_name().to_string_lossy();
            if let Some(inc) = &inc {
                if !inc.matches(&fname) {
                    continue;
                }
            }
            let content = match tokio::fs::read_to_string(entry.path()).await {
                Ok(c) => c,
                Err(_) => continue,
            };
            for (i, line) in content.lines().enumerate() {
                if re.is_match(line) {
                    hits.push(format!("{}:{}: {}", entry.path().display(), i + 1, line.trim()));
                    if hits.len() >= 200 {
                        break;
                    }
                }
            }
            if hits.len() >= 200 {
                break;
            }
        }
        if hits.is_empty() {
            Ok("(no matches)".into())
        } else {
            Ok(hits.join("\n"))
        }
    }
}
