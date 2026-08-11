//! OS-level sandbox boundary.
//!
//! Defines the platform-neutral permission profile language, the sandbox
//! manager that decides whether to sandbox and selects a platform backend,
//! and the SandboxBackend trait that each platform implements (macOS Seatbelt,
//! Linux seccomp).
//!
//! Design (mirrors maka):
//! - PermissionProfile describes what the sandboxed process may access.
//! - SandboxManager decides if a profile requires a sandbox, selects a
//!   platform backend, and delegates command transformation.
//! - The backend wraps the command argv with platform sandbox enforcement
//!   (e.g. /usr/bin/sandbox-exec on macOS).
//! - Fail-closed: if the backend is unavailable, the command fails rather
//!   than running unsandboxed.

pub mod manager;
pub mod profile;
#[cfg(target_os = "macos")]
pub mod macos;

pub use manager::{
    SandboxBackend, SandboxCommand, SandboxExecRequest, SandboxManager,
    SandboxTransformResult,
};
pub use profile::{
    FileSystemPolicy, NetworkPolicy, PermissionProfile, SandboxPathContext,
    workspace_managed_profile,
};
#[cfg(target_os = "macos")]
pub use macos::MacosSeatbeltBackend;
