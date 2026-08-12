//! Extra tools: ask_user_question, multi_edit, list_dir.

use std::path::Path;

use async_trait::async_trait;
use serde_json::{json, Value};

use crate::{Tool, ToolError};

// ── ask_user_question ──

/// `ask_user_question(questions)` — structured mid-turn user question.
///
/// Each question has a header, a prompt, and 2-4 options. The tool prints
/// to stderr and reads the answer from stdin. Works in CLI mode because the
/// main loop isn't reading stdin while the backend drives a turn.
pub struct AskUserQuestion;

#[async_trait]
impl Tool for AskUserQuestion {
    fn name(&self) -> &str {
        "ask_user_question"
    }

    fn description(&self) -> &str {
        "Ask the user 1-4 structured questions with labeled options. Each question has a header (≤12 chars), a question, and 2-4 options. Use when you need a decision or clarification before proceeding."
    }

    fn parameters(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "questions": {
                    "type": "array",
                    "minItems": 1,
                    "maxItems": 4,
                    "items": {
                        "type": "object",
                        "properties": {
                            "header": { "type": "string", "description": "Short label (≤12 chars)." },
                            "question": { "type": "string" },
                            "options": {
                                "type": "array",
                                "minItems": 2,
                                "maxItems": 4,
                                "items": { "type": "string" }
                            }
                        },
                        "required": ["header", "question", "options"]
                    }
                }
            },
            "required": ["questions"]
        })
    }

    async fn execute(&self, args: &Value) -> Result<String, ToolError> {
        let questions = args
            .get("questions")
            .and_then(|v| v.as_array())
            .ok_or_else(|| ToolError::BadArgs("missing 'questions' array".into()))?;

        if questions.is_empty() || questions.len() > 4 {
            return Err(ToolError::BadArgs("questions must have 1-4 items".into()));
        }

        let mut answers = Vec::new();

        for (qi, q) in questions.iter().enumerate() {
            let header = q.get("header").and_then(|v| v.as_str()).unwrap_or("");
            let question = q.get("question").and_then(|v| v.as_str()).unwrap_or("");
            let options = q.get("options").and_then(|v| v.as_array())
                .ok_or_else(|| ToolError::BadArgs("each question needs 'options'".into()))?;

            if options.len() < 2 {
                return Err(ToolError::BadArgs("each question needs 2+ options".into()));
            }

            // Print question to stderr.
            eprintln!("\n┌─ {header} ──────────────────────────────");
            eprintln!("│ {question}");
            for (oi, opt) in options.iter().enumerate() {
                let label = opt.as_str().unwrap_or("?");
                eprintln!("│  {}) {label}", oi + 1);
            }
            eprintln!("└─ enter number (or type your answer): ");

            // Read answer from stdin.
            let answer = read_stdin_line().await?;

            // Parse: try as a number index, else use as free text.
            let resolved = match answer.trim().parse::<usize>() {
                Ok(n) if n >= 1 && n <= options.len() => {
                    options[n - 1].as_str().unwrap_or("").to_string()
                }
                _ => answer.trim().to_string(),
            };

            answers.push(format!("Q{} [{}]: {resolved}", qi + 1, header));
        }

        Ok(answers.join("\n"))
    }
}

async fn read_stdin_line() -> Result<String, ToolError> {
    use std::io::BufRead;
    let mut line = String::new();
    std::io::stdin().read_line(&mut line)
        .map_err(|e| ToolError::Other(format!("stdin read: {e}")))?;
    Ok(line)
}

// ── multi_edit ──

/// `multi_edit(edits)` — apply multiple file edits atomically.
///
/// Each edit is {path, old_string, new_string}. All edits are validated
/// first (file exists, old_string found and unique). If any fails, NO file
/// is modified.
pub struct MultiEdit;

#[async_trait]
impl Tool for MultiEdit {
    fn name(&self) -> &str {
        "multi_edit"
    }

    fn description(&self) -> &str {
        "Apply multiple file edits in one call. All edits are validated before any write — if any edit fails, no files are changed. Each edit: {path, old_string, new_string}."
    }

    fn parameters(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "edits": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "path": { "type": "string" },
                            "old_string": { "type": "string" },
                            "new_string": { "type": "string" }
                        },
                        "required": ["path", "old_string", "new_string"]
                    }
                }
            },
            "required": ["edits"]
        })
    }

    async fn execute(&self, args: &Value) -> Result<String, ToolError> {
        let edits = args
            .get("edits")
            .and_then(|v| v.as_array())
            .ok_or_else(|| ToolError::BadArgs("missing 'edits' array".into()))?;

        if edits.is_empty() {
            return Err(ToolError::BadArgs("edits must have 1+ items".into()));
        }

        // Group edits by path (multiple edits to the same file are applied sequentially).
        use std::collections::HashMap;
        let mut by_path: HashMap<String, Vec<(String, String)>> = HashMap::new();
        for edit in edits {
            let path = edit.get("path").and_then(|v| v.as_str())
                .ok_or_else(|| ToolError::BadArgs("each edit needs 'path'".into()))?;
            let old = edit.get("old_string").and_then(|v| v.as_str())
                .ok_or_else(|| ToolError::BadArgs("each edit needs 'old_string'".into()))?;
            let new = edit.get("new_string").and_then(|v| v.as_str())
                .ok_or_else(|| ToolError::BadArgs("each edit needs 'new_string'".into()))?;
            by_path.entry(path.to_string()).or_default().push((old.to_string(), new.to_string()));
        }

        // Phase 1: validate all edits (read + check).
        let mut contents: HashMap<String, String> = HashMap::new();
        for (path, edits) in &by_path {
            let content = tokio::fs::read_to_string(path).await
                .map_err(|e| ToolError::Io(format!("{path}: {e}")))?;
            let mut current = content.clone();
            for (i, (old, _new)) in edits.iter().enumerate() {
                match current.matches(old).count() {
                    0 => return Err(ToolError::Other(format!("{path} edit {}: old_string not found", i + 1))),
                    1 => { current = current.replacen(old, "PLACEHOLDER", 1); }
                    n => return Err(ToolError::Other(format!("{path} edit {}: old_string matches {n} times", i + 1))),
                }
            }
            contents.insert(path.clone(), content);
        }

        // Phase 2: apply all edits (now safe).
        let mut modified = Vec::new();
        for (path, edits) in &by_path {
            let mut content = contents.remove(path).unwrap();
            for (old, new) in edits {
                content = content.replacen(old, new, 1);
            }
            tokio::fs::write(path, &content).await
                .map_err(|e| ToolError::Io(format!("{path}: {e}")))?;
            modified.push(path.clone());
        }

        Ok(format!("Applied {} edit(s) across {} file(s).", edits.len(), modified.len()))
    }
}

// ── list_dir ──

/// `list_dir(path?)` — list directory contents.
pub struct ListDir;

#[async_trait]
impl Tool for ListDir {
    fn name(&self) -> &str {
        "list_dir"
    }

    fn description(&self) -> &str {
        "List the contents of a directory. Returns entries with type indicators (d=file, /=dir, @=symlink). Default: current directory."
    }

    fn parameters(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "path": { "type": "string", "description": "Directory to list (default: '.')." }
            }
        })
    }

    async fn execute(&self, args: &Value) -> Result<String, ToolError> {
        let path = args.get("path").and_then(|v| v.as_str()).unwrap_or(".");
        let entries = tokio::fs::read_dir(path).await
            .map_err(|e| ToolError::Io(format!("{path}: {e}")))?;

        let mut lines = Vec::new();
        let mut entries = entries;
        loop {
            let entry = match entries.next_entry().await {
                Ok(Some(e)) => e,
                Ok(None) => break,
                Err(e) => return Err(ToolError::Io(format!("read entry: {e}"))),
            };
            let name = entry.file_name().to_string_lossy().into_owned();
            let ft = entry.file_type().await.ok();
            let prefix = match ft {
                Some(t) if t.is_dir() => "/",
                Some(t) if t.is_symlink() => "@",
                _ => " ",
            };
            lines.push(format!("{prefix}{name}"));
        }
        lines.sort();

        if lines.is_empty() {
            Ok("(empty directory)".into())
        } else {
            Ok(lines.join("\n"))
        }
    }
}
