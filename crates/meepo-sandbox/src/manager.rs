//! Sandbox manager — decides whether to sandbox, selects a platform backend,
//! and delegates command transformation.

use crate::profile::{PermissionProfile, SandboxPathContext};

/// The type of sandbox enforcement selected for a command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SandboxType {
    None,
    MacosSeatbelt,
    Linux,
}

/// A command to be potentially sandboxed.
#[derive(Debug, Clone)]
pub struct SandboxCommand {
    pub program: String,
    pub args: Vec<String>,
    pub cwd: String,
    pub env: Vec<(String, String)>,
    pub profile: PermissionProfile,
    pub path_context: SandboxPathContext,
}

/// The result of sandbox transformation: the wrapped argv ready to execute.
#[derive(Debug, Clone)]
pub struct SandboxExecRequest {
    /// The full argv to execute (may include sandbox wrapper like sandbox-exec).
    pub argv: Vec<String>,
    pub cwd: String,
    pub env: Vec<(String, String)>,
    pub sandbox_type: SandboxType,
}

/// Result of a sandbox transform attempt.
#[derive(Debug, Clone)]
pub enum SandboxTransformResult {
    Ok(SandboxExecRequest),
    Failed {
        reason: String,
        message: String,
    },
}

/// A platform sandbox backend (macOS Seatbelt, Linux seccomp, ...).
/// Transforms a command into a sandbox-wrapped exec request.
pub trait SandboxBackend: Send + Sync {
    /// The sandbox type this backend implements.
    fn sandbox_type(&self) -> SandboxType;
    /// Whether this backend is available on the current platform.
    fn is_available(&self) -> bool;
    /// Transform a command into a sandbox-wrapped exec request.
    fn transform(&self, command: &SandboxCommand) -> SandboxTransformResult;
}

/// Decides whether to sandbox, selects a backend, and delegates.
pub struct SandboxManager {
    backends: Vec<Box<dyn SandboxBackend>>,
}

impl SandboxManager {
    /// Create with no backends (all sandboxed commands fail-closed).
    pub fn new() -> Self {
        Self { backends: Vec::new() }
    }

    /// Register a platform backend.
    pub fn register(&mut self, backend: Box<dyn SandboxBackend>) {
        self.backends.push(backend);
    }

    /// Transform a command: if the profile requires a sandbox, select a
    /// backend and wrap the command. If no sandbox is needed, return the
    /// command as-is. If a sandbox is needed but no backend is available,
    /// fail-closed.
    pub fn transform(&self, command: &SandboxCommand) -> SandboxTransformResult {
        if !command.profile.requires_sandbox() {
            return SandboxTransformResult::Ok(SandboxExecRequest {
                argv: std::iter::once(command.program.clone())
                    .chain(command.args.iter().cloned())
                    .collect(),
                cwd: command.cwd.clone(),
                env: command.env.clone(),
                sandbox_type: SandboxType::None,
            });
        }

        // Find an available backend for this platform.
        let backend = self.backends.iter().find(|b| b.is_available());
        match backend {
            Some(b) => b.transform(command),
            None => SandboxTransformResult::Failed {
                reason: "backend_not_available".into(),
                message: "Sandbox is required but no platform backend is available.".into(),
            },
        }
    }
}

impl Default for SandboxManager {
    fn default() -> Self {
        Self::new()
    }
}
