//! Permission profile — the platform-neutral boundary language.
//!
//! A PermissionProfile describes what a sandboxed process may access. The
//! sandbox manager and platform backends translate this into OS-level
//! enforcement (SBPL on macOS, seccomp on Linux).

use std::fmt;

/// What kind of sandbox policy is active.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PermissionProfile {
    /// A managed profile with explicit file-system and network rules.
    Managed {
        file_system: FileSystemPolicy,
        network: NetworkPolicy,
    },
    /// No restrictions — the process runs on the host directly.
    Unrestricted,
    /// Sandboxing is disabled by the user.
    Disabled,
}

/// File-system access policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FileSystemPolicy {
    /// Only the listed roots are accessible.
    Restricted {
        readable_roots: Vec<String>,
        writable_roots: Vec<String>,
    },
    /// Full file-system access.
    Unrestricted,
}

/// Network access policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NetworkPolicy {
    /// All network access is denied.
    Denied,
    /// Network access is allowed.
    Allowed,
}

/// Context describing the workspace and runtime paths that the sandbox
/// backend needs to parameterize its enforcement.
#[derive(Debug, Clone, Default)]
pub struct SandboxPathContext {
    /// Workspace directories the sandboxed process may read.
    pub workspace_roots: Vec<String>,
    /// Temp directory available inside the sandbox.
    pub tmpdir: Option<String>,
    /// Runtime directories the process may read (binaries, frameworks).
    pub runtime_readable_roots: Vec<String>,
    /// Runtime directories the process may write (logs, caches).
    pub runtime_writable_roots: Vec<String>,
}

impl PermissionProfile {
    /// Returns true if this profile requires a platform sandbox.
    pub fn requires_sandbox(&self) -> bool {
        match self {
            PermissionProfile::Managed { file_system, network } => {
                matches!(file_system, FileSystemPolicy::Restricted { .. })
                    || matches!(network, NetworkPolicy::Denied)
            }
            _ => false,
        }
    }
}

impl fmt::Display for PermissionProfile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Managed { .. } => write!(f, "managed"),
            Self::Unrestricted => write!(f, "unrestricted"),
            Self::Disabled => write!(f, "disabled"),
        }
    }
}

/// Convenience: a default managed profile for a workspace.
pub fn workspace_managed_profile(workspace_root: &str) -> PermissionProfile {
    PermissionProfile::Managed {
        file_system: FileSystemPolicy::Restricted {
            readable_roots: vec![workspace_root.to_string()],
            writable_roots: vec![workspace_root.to_string()],
        },
        network: NetworkPolicy::Denied,
    }
}
