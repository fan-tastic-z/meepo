//! macOS Seatbelt sandbox backend.
//!
//! Generates an SBPL (Seatbelt Policy Language) profile from the permission
//! profile and path context, then wraps the command with
//! `/usr/bin/sandbox-exec -p <profile> -- <program> <args>`.

use crate::manager::{SandboxBackend, SandboxCommand, SandboxExecRequest, SandboxTransformResult, SandboxType};
use crate::profile::{FileSystemPolicy, NetworkPolicy, PermissionProfile};

pub const SANDBOX_EXEC: &str = "/usr/bin/sandbox-exec";

/// The base SBPL policy: deny everything by default, then allow process
/// management, sysctl, and reading system directories.
const BASE_POLICY: &str = r#"(version 1)
(deny default)
(allow process*)
(allow signal (target same-sandbox))
(allow sysctl*)
(allow file-read-metadata)
(allow file-read*
  (subpath "/System")
  (subpath "/usr")
  (subpath "/bin")
  (subpath "/sbin")
  (subpath "/Library/Apple")
  (subpath "/Library/Filesystems/NetFSPlugins")
  (subpath "/private/var/db/timezone")
  (subpath "/private/var/db/DarwinDirectory")
  (literal "/dev/null")
  (literal "/dev/zero"))
(allow file-map-executable
  (subpath "/usr/lib")
  (subpath "/System/Library/Frameworks")
  (subpath "/System/Library/PrivateFrameworks"))"#;

/// Build the full SBPL profile string from a managed restricted profile.
pub fn build_seatbelt_policy(
    profile: &PermissionProfile,
    path_context: &crate::profile::SandboxPathContext,
) -> String {
    let mut sb = String::with_capacity(4096);
    sb.push_str(BASE_POLICY);
    sb.push('\n');

    if let PermissionProfile::Managed {
        file_system: FileSystemPolicy::Restricted { readable_roots, writable_roots },
        network,
    } = profile
    {
        for root in readable_roots {
            sb.push_str(&format!("(allow file-read* (subpath \"{root}\"))\n"));
        }
        for root in writable_roots {
            sb.push_str(&format!("(allow file-write* (subpath \"{root}\"))\n"));
        }
        match network {
            NetworkPolicy::Denied => sb.push_str("(deny network*)\n"),
            NetworkPolicy::Allowed => sb.push_str("(allow network*)\n"),
        }
    }

    for root in &path_context.runtime_readable_roots {
        sb.push_str(&format!("(allow file-read* (subpath \"{root}\"))\n"));
    }
    for root in &path_context.runtime_writable_roots {
        sb.push_str(&format!("(allow file-write* (subpath \"{root}\"))\n"));
    }
    if let Some(tmp) = &path_context.tmpdir {
        sb.push_str(&format!("(allow file-read* file-write* (subpath \"{tmp}\"))\n"));
    }

    sb
}

/// Wrap a command with sandbox-exec, producing the full argv.
pub fn create_exec_args(
    profile: &PermissionProfile,
    path_context: &crate::profile::SandboxPathContext,
    program: &str,
    args: &[String],
) -> Vec<String> {
    let policy = build_seatbelt_policy(profile, path_context);
    let mut argv = vec![
        SANDBOX_EXEC.to_string(),
        "-p".to_string(),
        policy,
        "--".to_string(),
        program.to_string(),
    ];
    argv.extend(args.iter().cloned());
    argv
}

/// macOS Seatbelt sandbox backend.
pub struct MacosSeatbeltBackend;

impl MacosSeatbeltBackend {
    pub fn new() -> Self {
        Self
    }
}

impl SandboxBackend for MacosSeatbeltBackend {
    fn sandbox_type(&self) -> SandboxType {
        SandboxType::MacosSeatbelt
    }

    fn is_available(&self) -> bool {
        cfg!(target_os = "macos") && std::path::Path::new(SANDBOX_EXEC).exists()
    }

    fn transform(&self, command: &SandboxCommand) -> SandboxTransformResult {
        // Only accept managed restricted profiles.
        match &command.profile {
            PermissionProfile::Managed {
                file_system: FileSystemPolicy::Restricted { .. },
                ..
            } => {}
            _ => {
                return SandboxTransformResult::Failed {
                    reason: "invalid_profile".into(),
                    message: "macOS Seatbelt backend only accepts managed restricted profiles.".into(),
                };
            }
        }

        let argv = create_exec_args(
            &command.profile,
            &command.path_context,
            &command.program,
            &command.args,
        );

        SandboxTransformResult::Ok(SandboxExecRequest {
            argv,
            cwd: command.cwd.clone(),
            env: command.env.clone(),
            sandbox_type: SandboxType::MacosSeatbelt,
        })
    }
}

impl Default for MacosSeatbeltBackend {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::profile::workspace_managed_profile;

    #[test]
    fn build_policy_contains_deny_default_and_workspace_roots() {
        let profile = workspace_managed_profile("/Users/test/project");
        let ctx = crate::profile::SandboxPathContext::default();
        let policy = build_seatbelt_policy(&profile, &ctx);
        assert!(policy.contains("(deny default)"));
        assert!(policy.contains("/Users/test/project"));
        assert!(policy.contains("(deny network*)"));
    }

    #[test]
    fn create_exec_args_wraps_with_sandbox_exec() {
        let profile = workspace_managed_profile("/workspace");
        let ctx = crate::profile::SandboxPathContext::default();
        let argv = create_exec_args(&profile, &ctx, "ls", &["-la".into()]);
        assert_eq!(argv[0], SANDBOX_EXEC);
        assert_eq!(argv[1], "-p");
        assert!(argv[2].contains("(version 1)"));
        assert_eq!(argv[3], "--");
        assert_eq!(argv[4], "ls");
        assert_eq!(argv[5], "-la");
    }

    #[test]
    fn backend_rejects_unrestricted_profile() {
        let backend = MacosSeatbeltBackend::new();
        let cmd = SandboxCommand {
            program: "ls".into(),
            args: vec![],
            cwd: "/tmp".into(),
            env: vec![],
            profile: PermissionProfile::Unrestricted,
            path_context: crate::profile::SandboxPathContext::default(),
        };
        match backend.transform(&cmd) {
            SandboxTransformResult::Failed { reason, .. } => assert_eq!(reason, "invalid_profile"),
            _ => panic!("expected failure for unrestricted profile"),
        }
    }

    #[test]
    fn backend_transforms_managed_restricted() {
        let backend = MacosSeatbeltBackend::new();
        let cmd = SandboxCommand {
            program: "cargo".into(),
            args: vec!["test".into()],
            cwd: "/workspace".into(),
            env: vec![],
            profile: workspace_managed_profile("/workspace"),
            path_context: crate::profile::SandboxPathContext {
                tmpdir: Some("/tmp".into()),
                ..Default::default()
            },
        };
        match backend.transform(&cmd) {
            SandboxTransformResult::Ok(req) => {
                assert_eq!(req.sandbox_type, SandboxType::MacosSeatbelt);
                assert_eq!(req.argv[0], SANDBOX_EXEC);
                assert!(req.argv[2].contains("/workspace"));
            }
            _ => panic!("expected ok for managed restricted"),
        }
    }
}
