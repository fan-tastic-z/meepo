//! macOS Seatbelt sandbox backend.
//!
//! Generates an SBPL (Seatbelt Policy Language) profile from the permission
//! profile and path context, then wraps the command with
//! `/usr/bin/sandbox-exec -p <profile> -D<param>=<value>... -- <program> <args>`.
//!
//! SBPL policies are a byte-for-byte port of the upstream maka implementation
//! (packages/runtime/src/sandbox/macos-seatbelt.ts). Two constant blocks
//! (BASE_POLICY + PLATFORM_DEFAULTS_POLICY) plus dynamic sections for
//! readable/writable/runtime roots and network policy.

use crate::manager::{SandboxBackend, SandboxCommand, SandboxExecRequest, SandboxTransformResult, SandboxType};
use crate::profile::{FileSystemPolicy, NetworkPolicy, PermissionProfile, SandboxPathContext};

pub const SANDBOX_EXEC: &str = "/usr/bin/sandbox-exec";

// ── Constant SBPL blocks (ported verbatim from maka) ──

/// Base policy: deny everything by default, allow process management, sysctl,
/// and reading core system directories. Matches maka's MACOS_SEATBELT_BASE_POLICY.
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
  (literal "/dev/null")
  (literal "/dev/zero"))"#;

/// Platform defaults: the extensive list of system paths that standard tools
/// (sh, cargo, node, etc.) need to start. Matches maka's
/// MACOS_SEATBELT_PLATFORM_DEFAULTS_POLICY (lines 28-182).
const PLATFORM_DEFAULTS_POLICY: &str = r#"; macOS platform defaults for launching standard system tools.
(allow file-read* file-test-existence
  (subpath "/Library/Apple")
  (subpath "/Library/Filesystems/NetFSPlugins")
  (subpath "/Library/Preferences")
  (subpath "/Library/Preferences/Logging")
  (subpath "/private/var/db")
  (subpath "/private/var/db/DarwinDirectory/local/recordStore.data")
  (subpath "/private/var/db/timezone")
  (subpath "/usr/lib")
  (subpath "/usr/share")
  (subpath "/var/db"))

(allow file-map-executable
  (subpath "/Library/Apple/System/Library/Frameworks")
  (subpath "/Library/Apple/System/Library/PrivateFrameworks")
  (subpath "/Library/Apple/usr/lib")
  (subpath "/System/Library/Extensions")
  (subpath "/System/Library/Frameworks")
  (subpath "/System/Library/PrivateFrameworks")
  (subpath "/System/Library/SubFrameworks")
  (subpath "/System/iOSSupport/System/Library/Frameworks")
  (subpath "/System/iOSSupport/System/Library/PrivateFrameworks")
  (subpath "/System/iOSSupport/System/Library/SubFrameworks")
  (subpath "/usr/lib"))

(allow file-read* file-test-existence
  (subpath "/Library/Apple/System/Library/Frameworks")
  (subpath "/Library/Apple/System/Library/PrivateFrameworks")
  (subpath "/Library/Apple/usr/lib")
  (subpath "/System/Library/Frameworks")
  (subpath "/System/Library/PrivateFrameworks")
  (subpath "/System/Library/SubFrameworks")
  (subpath "/System/iOSSupport/System/Library/Frameworks")
  (subpath "/System/iOSSupport/System/Library/PrivateFrameworks")
  (subpath "/System/iOSSupport/System/Library/SubFrameworks")
  (subpath "/usr/lib"))

(allow system-mac-syscall (mac-policy-name "vnguard"))
(allow system-mac-syscall
  (require-all
    (mac-policy-name "Sandbox")
    (mac-syscall-number 67)))

(allow file-read-metadata file-test-existence
  (literal "/etc")
  (literal "/tmp")
  (literal "/var")
  (literal "/private/etc/localtime"))

(allow file-read-metadata file-test-existence
  (path-ancestors "/System/Volumes/Data/private"))

(allow file-read* file-test-existence
  (literal "/"))

(allow system-fsctl (fsctl-command FSIOC_CAS_BSDFLAGS))

(allow file-read* file-test-existence
  (literal "/dev/autofs_nowait")
  (literal "/dev/random")
  (literal "/dev/urandom")
  (literal "/private/etc/master.passwd")
  (literal "/private/etc/passwd")
  (literal "/private/etc/protocols")
  (literal "/private/etc/services"))

(allow file-read* file-test-existence file-write-data
  (literal "/dev/null")
  (literal "/dev/zero"))

(allow file-read-data file-test-existence file-write-data
  (subpath "/dev/fd"))

(allow file-read* file-test-existence file-write-data file-ioctl
  (literal "/dev/dtracehelper"))

(allow file-read* (subpath "/etc"))
(allow file-read* (subpath "/private/etc"))

(allow file-read* file-test-existence
  (literal "/System/Library/CoreServices")
  (literal "/System/Library/CoreServices/.SystemVersionPlatform.plist")
  (literal "/System/Library/CoreServices/SystemVersion.plist"))

(allow file-read-metadata (subpath "/var"))
(allow file-read-metadata (subpath "/private/var"))

(allow mach-lookup
  (global-name "com.apple.analyticsd")
  (global-name "com.apple.analyticsd.messagetracer")
  (global-name "com.apple.appsleep")
  (global-name "com.apple.bsd.dirhelper")
  (global-name "com.apple.cfprefsd.agent")
  (global-name "com.apple.cfprefsd.daemon")
  (global-name "com.apple.diagnosticd")
  (global-name "com.apple.dt.automationmode.reader")
  (global-name "com.apple.espd")
  (global-name "com.apple.logd")
  (global-name "com.apple.logd.events")
  (global-name "com.apple.runningboard")
  (global-name "com.apple.secinitd")
  (global-name "com.apple.system.DirectoryService.libinfo_v1")
  (global-name "com.apple.system.logger")
  (global-name "com.apple.system.notification_center")
  (global-name "com.apple.system.opendirectoryd.membership")
  (global-name "com.apple.trustd")
  (global-name "com.apple.trustd.agent")
  (global-name "com.apple.xpc.activity.unmanaged")
  (local-name "com.apple.cfprefsd.agent"))

(allow ipc-posix-shm-read*
  (ipc-posix-name "apple.shm.notification_center"))

(allow file-read*
  (literal "/private/var/db/eligibilityd/eligibility.plist"))

(allow mach-lookup (global-name "com.apple.audio.audiohald"))
(allow mach-lookup (global-name "com.apple.audio.AudioComponentRegistrar"))

(allow file-read-data (subpath "/bin"))
(allow file-read-metadata (subpath "/bin"))
(allow file-read-data (subpath "/sbin"))
(allow file-read-metadata (subpath "/sbin"))
(allow file-read-data (subpath "/usr/bin"))
(allow file-read-metadata (subpath "/usr/bin"))
(allow file-read-data (subpath "/usr/sbin"))
(allow file-read-metadata (subpath "/usr/sbin"))
(allow file-read-data (subpath "/usr/libexec"))
(allow file-read-metadata (subpath "/usr/libexec"))

(allow file-read* (subpath "/opt/homebrew/lib"))
(allow file-read* (subpath "/usr/local/lib"))

(allow file-read* (regex "^/dev/fd/(0|1|2)$"))
(allow file-write* (regex "^/dev/fd/(1|2)$"))
(allow file-read* file-write* (literal "/dev/null"))
(allow file-read* file-write* (literal "/dev/tty"))
(allow file-read-metadata (literal "/dev"))
(allow file-read-metadata (regex "^/dev/.*$"))
(allow file-read-metadata (literal "/dev/stdin"))
(allow file-read-metadata (literal "/dev/stdout"))
(allow file-read-metadata (literal "/dev/stderr"))
(allow file-read-metadata (regex "^/dev/tty[^/]*$"))
(allow file-read-metadata (regex "^/dev/pty[^/]*$"))
(allow file-read* file-write* (regex "^/dev/ttys[0-9]+$"))
(allow file-read* file-write* (literal "/dev/ptmx"))
(allow file-ioctl (regex "^/dev/ttys[0-9]+$"))

(allow file-read-metadata (literal "/System/Volumes") (vnode-type DIRECTORY))
(allow file-read-metadata (literal "/System/Volumes/Data") (vnode-type DIRECTORY))
(allow file-read-metadata (literal "/System/Volumes/Data/Users") (vnode-type DIRECTORY))

(allow file-read* (extension "com.apple.app-sandbox.read"))
(allow file-read* file-write* (extension "com.apple.app-sandbox.read-write"))"#;

// ── Dynamic section builders ──

/// Build readable roots policy section using -D params (prevents SBPL injection).
fn build_readable_roots(readable: &[String]) -> (String, Vec<(String, String)>) {
    if readable.is_empty() {
        return (String::new(), Vec::new());
    }
    let mut params = Vec::new();
    let clauses: Vec<String> = readable
        .iter()
        .enumerate()
        .map(|(i, _)| {
            let name = format!("READABLE_ROOT_{i}");
            params.push((name.clone(), String::new())); // filled by caller
            format!("  (subpath (param \"{}\"))", name)
        })
        .collect();
    let policy = format!("(allow file-read*\n{})", clauses.join("\n"));
    // Fill param values.
    for (i, root) in readable.iter().enumerate() {
        params[i].1 = root.clone();
    }
    (policy, params)
}

/// Build writable roots policy section using -D params.
fn build_writable_roots(writable: &[String]) -> (String, Vec<(String, String)>) {
    if writable.is_empty() {
        return (String::new(), Vec::new());
    }
    let mut params = Vec::new();
    let clauses: Vec<String> = writable
        .iter()
        .enumerate()
        .map(|(i, _)| {
            let name = format!("WRITABLE_ROOT_{i}");
            params.push((name.clone(), String::new()));
            format!("  (subpath (param \"{}\"))", name)
        })
        .collect();
    let policy = format!("(allow file-write*\n{})", clauses.join("\n"));
    for (i, root) in writable.iter().enumerate() {
        params[i].1 = root.clone();
    }
    (policy, params)
}

/// Build runtime roots section (readable + executable).
fn build_runtime_roots(ctx: &SandboxPathContext) -> (String, Vec<(String, String)>) {
    let mut sections = Vec::new();
    let mut params = Vec::new();

    if !ctx.runtime_readable_roots.is_empty() {
        let clauses: Vec<String> = ctx
            .runtime_readable_roots
            .iter()
            .enumerate()
            .map(|(i, root)| {
                let name = format!("RUNTIME_READABLE_ROOT_{i}");
                params.push((name.clone(), root.clone()));
                format!("  (subpath (param \"{}\"))", name)
            })
            .collect();
        sections.push(format!(
            "(allow file-read* file-test-existence\n{})",
            clauses.join("\n")
        ));
    }

    if !ctx.runtime_writable_roots.is_empty() {
        let clauses: Vec<String> = ctx
            .runtime_writable_roots
            .iter()
            .enumerate()
            .map(|(i, root)| {
                let name = format!("RUNTIME_WRITABLE_ROOT_{i}");
                params.push((name.clone(), root.clone()));
                format!("  (subpath (param \"{}\"))", name)
            })
            .collect();
        sections.push(format!(
            "(allow file-write*\n{})",
            clauses.join("\n")
        ));
    }

    (sections.join("\n\n"), params)
}

/// Build network policy section.
fn build_network_policy(profile: &PermissionProfile) -> String {
    match profile {
        PermissionProfile::Managed {
            network: NetworkPolicy::Allowed,
            ..
        } => "(allow network*)".into(),
        _ => "(deny network*)".into(),
    }
}

// ── Full policy builder ──

/// Build the complete SBPL profile and its -D definition args.
pub fn build_seatbelt_policy(
    profile: &PermissionProfile,
    path_context: &SandboxPathContext,
) -> (String, Vec<(String, String)>) {
    let mut all_params = Vec::new();

    // Collect roots from profile.
    let (readable, writable): (Vec<String>, Vec<String>) = match profile {
        PermissionProfile::Managed {
            file_system: FileSystemPolicy::Restricted { readable_roots, writable_roots },
            ..
        } => (readable_roots.clone(), writable_roots.clone()),
        _ => (Vec::new(), Vec::new()),
    };

    // Add workspace_roots from path_context to readable.
    let mut all_readable = readable;
    all_readable.extend(path_context.workspace_roots.iter().cloned());
    if let Some(ref tmp) = path_context.tmpdir {
        all_readable.push(tmp.clone());
    }
    all_readable.dedup();

    let (readable_policy, readable_params) = build_readable_roots(&all_readable);
    all_params.extend(readable_params);

    let (writable_policy, writable_params) = build_writable_roots(&writable);
    all_params.extend(writable_params);

    let (runtime_policy, runtime_params) = build_runtime_roots(path_context);
    all_params.extend(runtime_params);

    let network_policy = build_network_policy(profile);

    let sections: Vec<&str> = vec![
        &readable_policy,
        &writable_policy,
        &runtime_policy,
        &network_policy,
    ];
    let dynamic: String = sections
        .iter()
        .filter(|s| !s.is_empty())
        .copied()
        .collect::<Vec<_>>()
        .join("\n\n");

    let full = format!("{BASE_POLICY}\n\n{PLATFORM_DEFAULTS_POLICY}\n\n{dynamic}\n");
    (full, all_params)
}

/// Wrap a command with sandbox-exec, producing the full argv with -D params.
pub fn create_exec_args(
    profile: &PermissionProfile,
    path_context: &SandboxPathContext,
    program: &str,
    args: &[String],
) -> Vec<String> {
    let (policy, params) = build_seatbelt_policy(profile, path_context);
    let mut argv = vec![SANDBOX_EXEC.to_string(), "-p".to_string(), policy];
    // Insert -D params between -p and --.
    for (name, value) in &params {
        argv.push(format!("-D{name}={value}"));
    }
    argv.push("--".to_string());
    argv.push(program.to_string());
    argv.extend(args.iter().cloned());
    argv
}

// ── Backend ──

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
    fn build_policy_contains_base_and_platform_defaults() {
        let profile = workspace_managed_profile("/Users/test/project");
        let ctx = SandboxPathContext::default();
        let (policy, params) = build_seatbelt_policy(&profile, &ctx);
        // Base policy
        assert!(policy.contains("(deny default)"));
        assert!(policy.contains("(allow process*)"));
        assert!(policy.contains("(subpath \"/System\")"));
        // Platform defaults
        assert!(policy.contains("(subpath \"/usr/lib\")"));
        assert!(policy.contains("com.apple.cfprefsd"));
        assert!(policy.contains("(regex \"^/dev/ttys[0-9]+$\")"));
        // Dynamic: workspace readable via param
        assert!(params.iter().any(|(n, v)| n.starts_with("READABLE_ROOT") && v == "/Users/test/project"));
        // Network denied
        assert!(policy.contains("(deny network*)"));
    }

    #[test]
    fn create_exec_args_includes_d_params() {
        let profile = workspace_managed_profile("/workspace");
        let ctx = SandboxPathContext::default();
        let argv = create_exec_args(&profile, &ctx, "ls", &["-la".into()]);
        assert_eq!(argv[0], SANDBOX_EXEC);
        assert_eq!(argv[1], "-p");
        assert!(argv[2].contains("(version 1)"));
        // -D params exist between -p and --
        assert!(argv.iter().any(|a| a.starts_with("-DREADABLE_ROOT")));
        assert!(argv.iter().any(|a| a.starts_with("-DWRITABLE_ROOT")));
        let dash_idx = argv.iter().position(|a| a == "--").unwrap();
        assert_eq!(argv[dash_idx + 1], "ls");
        assert_eq!(argv[dash_idx + 2], "-la");
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
            path_context: SandboxPathContext::default(),
        };
        match backend.transform(&cmd) {
            SandboxTransformResult::Failed { reason, .. } => assert_eq!(reason, "invalid_profile"),
            _ => panic!("expected failure"),
        }
    }

    #[test]
    fn backend_transforms_managed_restricted_with_params() {
        let backend = MacosSeatbeltBackend::new();
        let cmd = SandboxCommand {
            program: "cargo".into(),
            args: vec!["test".into()],
            cwd: "/workspace".into(),
            env: vec![],
            profile: workspace_managed_profile("/workspace"),
            path_context: SandboxPathContext {
                tmpdir: Some("/tmp".into()),
                ..Default::default()
            },
        };
        match backend.transform(&cmd) {
            SandboxTransformResult::Ok(req) => {
                assert_eq!(req.sandbox_type, SandboxType::MacosSeatbelt);
                assert_eq!(req.argv[0], SANDBOX_EXEC);
                // -D params include workspace path
                assert!(req.argv.iter().any(|a| a.contains("/workspace")));
                assert!(req.argv.iter().any(|a| a == "--"));
            }
            _ => panic!("expected ok"),
        }
    }

    #[test]
    fn readable_roots_use_params_not_inline() {
        let profile = workspace_managed_profile("/my/workspace");
        let ctx = SandboxPathContext::default();
        let (policy, _) = build_seatbelt_policy(&profile, &ctx);
        // Workspace path must NOT appear directly in policy text (it's in -D params)
        assert!(!policy.contains("/my/workspace"), "workspace path leaked into policy text");
        // But the param reference must be there
        assert!(policy.contains("(param \"READABLE_ROOT_"));
    }
}
