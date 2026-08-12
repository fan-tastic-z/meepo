//! Permission contracts — tool categorization and the mode×category policy.
//!
//! Every tool call is classified into a [`ToolCategory`]; the session's
//! [`PermissionMode`] (the immutable creation-time ceiling) plus the category
//! yield a [`PolicyDecision`] (allow / prompt / block).
//!
//! **There is no `shell_safe` category.** A shell is Turing-complete, so its
//! runtime effect cannot be decided from a static string — every command is at
//! least [`ToolCategory::ShellUnsafe`] (prompt). [`categorize_bash`] only makes
//! the confirmation *reason* accurate (destructive vs privileged vs generic);
//! it never opens the allow-vs-prompt gate, which stays at prompt. A missed
//! pattern is therefore a wording nit, not a bypass.

use std::sync::OnceLock;

use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Immutable creation-time permission ceiling.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PermissionMode {
    /// Read-only: no writes, no shell, no network send.
    Explore,
    /// Read freely; writes and shell prompt.
    Ask,
    /// Reads and file writes allowed; shell and destructive prompt.
    Execute,
    /// Everything allowed (trusted automation / development).
    Bypass,
}

/// Canonical tool category. A subset of the 14-class taxonomy covering the
/// tools meepo currently ships.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolCategory {
    /// Read, Grep, Glob, ListDir.
    Read,
    /// WebFetch, WebSearch (GET-class).
    WebRead,
    /// Write, Edit, MultiEdit (create / append / overwrite).
    FileWrite,
    /// rm, dd, shred, mkfs, find -delete, ...
    FsDestructive,
    /// Default Bash bucket — never auto-allowed.
    ShellUnsafe,
    /// git reset --hard, push --force, branch -D, ...
    GitDestructive,
    /// sudo, chmod, chown, kill, systemctl.
    Privileged,
    /// Session-scoped tools without a stricter category hint.
    CustomTool,
    // Reserved for later phases: NetworkSend, Browser, ComputerUse,
    // ClientCapability, Subagent.
}

/// Policy verdict for one tool call under the current mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PolicyDecision {
    Allow,
    Prompt,
    Block,
}

/// Map a builtin tool name to its category, if known.
pub fn builtin_tool_category(tool_name: &str) -> Option<ToolCategory> {
    match tool_name {
        "read_file" | "Read" | "grep" | "Grep" | "glob" | "Glob" | "list_dir" => {
            Some(ToolCategory::Read)
        }
        "web_fetch" | "WebFetch" | "web_search" | "WebSearch" => Some(ToolCategory::WebRead),
        "write_file" | "Write" | "edit" | "Edit" | "multi_edit" | "MultiEdit" => {
            Some(ToolCategory::FileWrite)
        }
        "bash" | "Bash" => Some(ToolCategory::ShellUnsafe),
        _ => None,
    }
}

/// Classify a tool invocation: builtin mapping, with Bash arguments refined by
/// [`categorize_bash`].
pub fn classify_tool_use(tool_name: &str, args: &Value) -> ToolCategory {
    let category = builtin_tool_category(tool_name).unwrap_or(ToolCategory::CustomTool);
    if category == ToolCategory::ShellUnsafe {
        if let Some(cmd) = args.get("command").and_then(Value::as_str) {
            return categorize_bash(cmd);
        }
    }
    category
}

/// The mode × category policy matrix.
pub fn policy_decision(mode: PermissionMode, category: ToolCategory) -> PolicyDecision {
    use PermissionMode::*;
    use ToolCategory::*;
    match (mode, category) {
        (Bypass, _) => PolicyDecision::Allow,
        (Explore, Read) | (Explore, WebRead) => PolicyDecision::Allow,
        (Explore, _) => PolicyDecision::Block,
        (Ask, Read) | (Ask, WebRead) => PolicyDecision::Allow,
        (Ask, _) => PolicyDecision::Prompt,
        (Execute, Read) | (Execute, WebRead) | (Execute, FileWrite) => PolicyDecision::Allow,
        (Execute, _) => PolicyDecision::Prompt,
    }
}

// ===========================================================================
// Shell command categorization — faithful port of the upstream categorizeBash.
// ===========================================================================

/// Commands that defer to the real command later in the segment.
const WRAPPER_COMMANDS: &[&str] = &[
    "nohup", "nice", "time", "timeout", "env", "command", "exec", "stdbuf",
];

struct ShellPatterns {
    privileged_prefixes: Vec<String>,
    privileged: Vec<Regex>,
    fs_destructive: Vec<Regex>,
    pipe_destructive: Vec<Regex>,
    git_destructive: Vec<Regex>,
    /// Shell-in-shell heads whose literal payload is categorized recursively.
    /// (Interpreters like `python -c` are deliberately absent.)
    nested_shell: Vec<(Regex, Regex)>,
}

fn patterns() -> &'static ShellPatterns {
    static P: OnceLock<ShellPatterns> = OnceLock::new();
    P.get_or_init(|| {
        let re = |p: &str| Regex::new(p).expect("invalid shell pattern");
        ShellPatterns {
            privileged_prefixes: [
                "sudo ", "su ", "chmod ", "chown ", "chgrp ", "mount ", "umount ", "kill ",
                "killall ", "systemctl ", "launchctl ", "shutdown", "reboot",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect(),
            privileged: vec![
                re(r"(?i)^(kill|stop-process|spps|taskkill)\b"),
                re(r"(?i)(^|\s)-verb\s+runas\b"),
                re(r"(?i)^((start|stop|restart|set|new|remove|suspend|resume)-service|sasv|spsv)\b"),
                re(r"(?i)^sc\s+(stop|start|pause|continue|delete|config|create|failure|sdset)\b"),
                re(r"(?i)^net\s+(stop|start|pause|continue)\b"),
                re(r"(?i)^(stop-computer|restart-computer)\b"),
                re(r"(?i)^(icacls|takeown|set-acl|runas)\b"),
            ],
            fs_destructive: vec![
                re(r"^dd\s+"),
                re(r"^truncate\b"),
                re(r"^shred\b"),
                re(r"^mkfs\b"),
                re(r"^git\s+restore\s+(\.\s*$|--\s+\S+)"),
                re(r"^git\s+checkout\s+--\s+\S+"),
                re(r"^find\s+.*\s-delete\b"),
                re(r"^find\s+.*\s-exec\s+.*\b(rm|shred|truncate|dd)\b"),
                re(r"^xargs\s+.*\b(rm|shred|truncate|dd)\b"),
                re(r"(?i)^remove-item\b"),
                re(r"(?i)^(rm|rmdir|ri|del|erase|rd)\b"),
                re(r"(?i)^(clear-content|clc)\b"),
            ],
            pipe_destructive: vec![
                re(r"\|\s*xargs\b[^\n;&|]*\b(rm|shred|truncate|dd)\b"),
                re(r"\|\s*(sh|bash|zsh)\b"),
            ],
            git_destructive: vec![
                re(r"^git\s+reset\s+--hard\b"),
                re(r"^git\s+push\s+(--force|-f)\b"),
                re(r"^git\s+branch\s+-D\b"),
                re(r"^git\s+clean\s+-fd?\b"),
                re(r"^git\s+checkout\s+\.\s*$"),
                re(r"^git\s+rebase\s+-i\b"),
            ],
            nested_shell: vec![
                (re(r"^(sh|bash|zsh)$"), re(r"(?:^|\s)-\w*c\s+([\s\S]+)$")),
                (re(r"(?i)^(pwsh|powershell)$"), re(r"(?i)\s-c(?:ommand)?\s+([\s\S]+)$")),
                (re(r"(?i)^cmd$"), re(r"(?i)\s/[ck]\s+([\s\S]+)$")),
            ],
        }
    })
}

/// Categorize a shell command. Returns the most accurate confirmation reason:
/// `privileged` > `fs_destructive` > `git_destructive` > `shell_unsafe`.
/// Never returns a "safe" category — the gate stays at prompt.
pub fn categorize_bash(cmd: &str) -> ToolCategory {
    let p = patterns();
    let mut segments = scan_segments(cmd, 2);
    // Backtick is both a split boundary (bash substitution) and a PowerShell
    // in-name escape (R`M runs rm). Scan split segments AND a backtick-collapsed
    // variant so the PS halves are seen whole.
    if cmd.contains('`') {
        let collapsed: String = cmd.chars().filter(|&c| c != '`').collect();
        segments.extend(scan_segments(&collapsed, 2));
    }
    if segments.iter().any(|s| is_privileged_segment(s, p)) {
        return ToolCategory::Privileged;
    }
    if segments
        .iter()
        .any(|s| p.fs_destructive.iter().any(|re| re.is_match(s)))
    {
        return ToolCategory::FsDestructive;
    }
    if p.pipe_destructive.iter().any(|re| re.is_match(cmd.trim())) {
        return ToolCategory::FsDestructive;
    }
    if segments
        .iter()
        .any(|s| p.git_destructive.iter().any(|re| re.is_match(s)))
    {
        return ToolCategory::GitDestructive;
    }
    ToolCategory::ShellUnsafe
}

/// Split a command into statement/pipeline/scriptblock/substitution segments.
/// Deliberately quote-naive: `$(...)` and backticks expand inside double
/// quotes, so quote-aware splitting would hide `echo "$(rm x)"`. Naive
/// splitting never drops content — it only cuts it up.
fn command_segments(cmd: &str) -> Vec<String> {
    cmd.split(|c| matches!(c, '|' | ';' | '&' | '\n' | '(' | ')' | '{' | '}' | '`'))
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

/// Canonicalize a segment's first token: unwrap quotes, drop a leading escape
/// / path prefix / `.exe` suffix, and skip wrapper commands + their option-ish
/// arguments. Only the upgrade checks see this; categorize never auto-allows.
fn normalize_segment_head(segment: &str) -> String {
    let mut rest = segment.to_string();
    for _ in 0..5 {
        let (head, tail) = match split_head(&rest) {
            Some(ht) => ht,
            None => return rest,
        };
        let mut h = head;
        h = h.chars().filter(|&c| c != '\'' && c != '"' && c != '^').collect::<String>();
        if h.starts_with('\\') {
            h = h[1..].to_string();
        }
        if let Some(pos) = h.rfind(|c: char| c == '/' || c == '\\') {
            h = h[pos + 1..].to_string();
        }
        if h.to_ascii_lowercase().ends_with(".exe") {
            h = h[..h.len() - 4].to_string();
        }
        if WRAPPER_COMMANDS.contains(&h.to_ascii_lowercase().as_str()) {
            // Skip the wrapper's option-ish arguments (flags, KEY=VAL, durations).
            rest = strip_leading_options(&tail);
            continue;
        }
        return if tail.is_empty() { h } else { format!("{h} {tail}") };
    }
    rest
}

/// Split off the first token of `rest`, honoring a leading quoted form
/// `& 'Name' x` / `"Name" x`, else the first run of non-space.
fn split_head(rest: &str) -> Option<(String, String)> {
    let trimmed = rest.trim_start();
    if trimmed.is_empty() {
        return None;
    }
    let first = trimmed.chars().next().unwrap();
    if first == '\'' || first == '"' {
        // Quoted form: read until the matching close quote. `first` is ASCII so
        // byte indexing on the quote boundary is UTF-8 safe.
        let after_quote = &trimmed[first.len_utf8()..];
        let close = after_quote.find(first)?;
        let inner = &after_quote[..close];
        if inner.is_empty() {
            return None;
        }
        let tail = after_quote[close + first.len_utf8()..].trim_start().to_string();
        return Some((inner.to_string(), tail));
    }
    // Bare token: first run of non-space.
    let end = trimmed.find(char::is_whitespace).unwrap_or(trimmed.len());
    let head = trimmed[..end].to_string();
    let tail = trimmed[end..].trim_start().to_string();
    Some((head, tail))
}

/// Drop leading option-ish arguments a wrapper command carries: `-x`, `KEY=V`,
/// durations like `30s`. Returns the remainder.
fn strip_leading_options(tail: &str) -> String {
    let mut rest = tail.trim_start();
    while let Some(token) = rest.split_whitespace().next() {
        let is_flag = token.starts_with('-') && token.len() > 1;
        let is_assign = token.contains('=');
        // \d+[smhd]? — a bare number or number+unit (the `timeout 30` form).
        let is_duration = token
            .chars()
            .all(|c| c.is_ascii_digit() || matches!(c, 's' | 'm' | 'h' | 'd'))
            && token.chars().any(|c| c.is_ascii_digit());
        if is_flag || is_assign || is_duration {
            rest = rest[token.len()..].trim_start();
        } else {
            break;
        }
    }
    rest.to_string()
}

fn scan_segments(cmd: &str, depth: usize) -> Vec<String> {
    let p = patterns();
    let mut out = Vec::new();
    for raw in command_segments(cmd) {
        let segment = normalize_segment_head(&raw);
        out.push(segment.clone());
        if depth == 0 {
            continue;
        }
        if let Some(payload) = nested_shell_payload(&segment, p) {
            out.extend(scan_segments(&payload, depth - 1));
        }
    }
    out
}

fn nested_shell_payload(segment: &str, p: &ShellPatterns) -> Option<String> {
    let head = segment.split_whitespace().next()?;
    for (head_re, flag_re) in &p.nested_shell {
        if !head_re.is_match(head) {
            continue;
        }
        let payload = flag_re
            .captures(segment)
            .and_then(|c| c.get(1))
            .map(|m| m.as_str().trim().to_string())?;
        // Unquote if the whole payload is one quoted string.
        if payload.len() >= 2 {
            let b = payload.as_bytes();
            if (b[0] == b'\'' || b[0] == b'"') && b[0] == b[payload.len() - 1] {
                return Some(payload[1..payload.len() - 1].to_string());
            }
        }
        return Some(payload);
    }
    None
}

fn is_privileged_segment(segment: &str, p: &ShellPatterns) -> bool {
    let lower = segment.to_ascii_lowercase();
    p.privileged_prefixes.iter().any(|pre| lower.starts_with(pre))
        || p.privileged.iter().any(|re| re.is_match(segment))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn builtin_categories() {
        assert_eq!(builtin_tool_category("read_file"), Some(ToolCategory::Read));
        assert_eq!(builtin_tool_category("bash"), Some(ToolCategory::ShellUnsafe));
        assert_eq!(builtin_tool_category("write_file"), Some(ToolCategory::FileWrite));
        assert_eq!(builtin_tool_category("web_fetch"), Some(ToolCategory::WebRead));
        assert_eq!(builtin_tool_category("unknown"), None);
    }

    #[test]
    fn classify_bash_uses_command_arg() {
        let args = json!({ "command": "rm -rf /tmp/x" });
        assert_eq!(classify_tool_use("bash", &args), ToolCategory::FsDestructive);
    }

    #[test]
    fn classify_unknown_is_custom() {
        assert_eq!(
            classify_tool_use("my_tool", &json!({})),
            ToolCategory::CustomTool
        );
    }

    #[test]
    fn policy_matrix() {
        use PermissionMode::*;
        use ToolCategory::*;
        // Explore: read ok, everything else blocked.
        assert_eq!(policy_decision(Explore, Read), PolicyDecision::Allow);
        assert_eq!(policy_decision(Explore, FileWrite), PolicyDecision::Block);
        assert_eq!(policy_decision(Explore, ShellUnsafe), PolicyDecision::Block);
        // Ask: read ok, writes/shell prompt.
        assert_eq!(policy_decision(Ask, Read), PolicyDecision::Allow);
        assert_eq!(policy_decision(Ask, FileWrite), PolicyDecision::Prompt);
        assert_eq!(policy_decision(Ask, ShellUnsafe), PolicyDecision::Prompt);
        // Execute: read+write ok, shell prompts.
        assert_eq!(policy_decision(Execute, FileWrite), PolicyDecision::Allow);
        assert_eq!(policy_decision(Execute, ShellUnsafe), PolicyDecision::Prompt);
        // Bypass: all allow.
        assert_eq!(policy_decision(Bypass, Privileged), PolicyDecision::Allow);
    }

    #[test]
    fn bash_plain_is_shell_unsafe() {
        assert_eq!(categorize_bash("ls -la"), ToolCategory::ShellUnsafe);
        assert_eq!(categorize_bash("echo hello"), ToolCategory::ShellUnsafe);
        // Even "read-only" git is shell_unsafe — no shell is auto-safe.
        assert_eq!(categorize_bash("git status"), ToolCategory::ShellUnsafe);
    }

    #[test]
    fn bash_fs_destructive() {
        assert_eq!(categorize_bash("rm foo.txt"), ToolCategory::FsDestructive);
        assert_eq!(categorize_bash("rm -rf stuff"), ToolCategory::FsDestructive);
        assert_eq!(categorize_bash("dd if=/dev/zero of=/dev/sda"), ToolCategory::FsDestructive);
        assert_eq!(categorize_bash("find . -delete"), ToolCategory::FsDestructive);
    }

    #[test]
    fn bash_destructive_in_second_segment() {
        assert_eq!(
            categorize_bash("cd /tmp; rm -rf stuff"),
            ToolCategory::FsDestructive
        );
        assert_eq!(
            categorize_bash("echo hi && rm x"),
            ToolCategory::FsDestructive
        );
    }

    #[test]
    fn bash_privileged() {
        assert_eq!(categorize_bash("sudo apt update"), ToolCategory::Privileged);
        assert_eq!(categorize_bash("chmod 777 x"), ToolCategory::Privileged);
        assert_eq!(categorize_bash("kill -9 1234"), ToolCategory::Privileged);
    }

    #[test]
    fn bash_git_destructive() {
        assert_eq!(
            categorize_bash("git reset --hard origin/main"),
            ToolCategory::GitDestructive
        );
        assert_eq!(
            categorize_bash("git push --force"),
            ToolCategory::GitDestructive
        );
    }

    #[test]
    fn bash_nested_shell_payload_scanned() {
        // `sh -c 'rm x'` — the nested payload must be categorized.
        assert_eq!(categorize_bash("sh -c 'rm -rf x'"), ToolCategory::FsDestructive);
        assert_eq!(categorize_bash("bash -c rm"), ToolCategory::FsDestructive);
    }

    #[test]
    fn bash_pipe_destructive() {
        assert_eq!(
            categorize_bash("ls | xargs rm"),
            ToolCategory::FsDestructive
        );
        assert_eq!(
            categorize_bash("echo x | sh"),
            ToolCategory::FsDestructive
        );
    }

    #[test]
    fn bash_wrapper_command_unwrapped() {
        // timeout / nohup wrap the real command; the destructive intent shows.
        assert_eq!(
            categorize_bash("timeout 30 rm -rf /tmp/x"),
            ToolCategory::FsDestructive
        );
    }
}
