//! meepo-host daemon entry point.
//!
//! Phase 0 scaffold: parses the documented launch arguments and reports them.
//! Ownership (flock), the listening socket, and the protocol arrive in later
//! phases. The launcher (`meepo-host --root <root> --expected-root-id <id>
//! [--idle-grace-ms N]`) drives this binary.

use std::process::ExitCode;

struct HostArgs {
    root: Option<String>,
    expected_root_id: Option<String>,
    idle_grace_ms: Option<u64>,
}

fn parse_args(args: &[String]) -> Result<HostArgs, String> {
    let mut root = None;
    let mut expected_root_id = None;
    let mut idle_grace_ms = None;
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--root" if i + 1 < args.len() => { root = Some(args[i + 1].clone()); i += 2; }
            "--expected-root-id" if i + 1 < args.len() => { expected_root_id = Some(args[i + 1].clone()); i += 2; }
            "--idle-grace-ms" if i + 1 < args.len() => {
                idle_grace_ms = Some(args[i + 1].parse().map_err(|_| format!("invalid --idle-grace-ms '{}'", args[i + 1]))?);
                i += 2;
            }
            other => return Err(format!("unknown argument '{other}'")),
        }
    }
    Ok(HostArgs { root, expected_root_id, idle_grace_ms })
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let parsed = match parse_args(&args) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("meepo-host: {e}");
            eprintln!("usage: meepo-host --root <dir> --expected-root-id <id> [--idle-grace-ms N]");
            return ExitCode::from(2);
        }
    };
    eprintln!(
        "[meepo-host scaffold] root={:?} expected_root_id={:?} idle_grace_ms={:?}",
        parsed.root, parsed.expected_root_id, parsed.idle_grace_ms
    );
    eprintln!("[meepo-host scaffold] ownership/listen/protocol not implemented yet (phase 0)");
    ExitCode::SUCCESS
}
