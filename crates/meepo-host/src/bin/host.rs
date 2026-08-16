//! meepo-host daemon entry point.
//!
//! Phase 4: bind a Unix socket under `--root`, register the bootstrap host
//! handlers, and serve connections via [`HostKernel`]. Process ownership
//! (flock), registration discovery, idle daemon, and signal handling arrive in
//! phase 5.

use std::process::ExitCode;

#[tokio::main]
async fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut root: Option<String> = None;
    let mut idle_grace_ms: Option<u64> = None;
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--root" if i + 1 < args.len() => {
                root = Some(args[i + 1].clone());
                i += 2;
            }
            "--idle-grace-ms" if i + 1 < args.len() => {
                match args[i + 1].parse() {
                    Ok(v) => idle_grace_ms = Some(v),
                    Err(_) => {
                        eprintln!("meepo-host: invalid --idle-grace-ms '{}'", args[i + 1]);
                        return ExitCode::from(2);
                    }
                }
                i += 2;
            }
            "--expected-root-id" if i + 1 < args.len() => {
                // Reserved for phase 5 (flock ownership check).
                i += 2;
            }
            other => {
                eprintln!("meepo-host: unknown argument '{other}'");
                eprintln!(
                    "usage: meepo-host --root <dir> --expected-root-id <id> [--idle-grace-ms N]"
                );
                return ExitCode::from(2);
            }
        }
    }
    let _ = idle_grace_ms; // phase 5
    let root = match root {
        Some(r) => r,
        None => {
            eprintln!("meepo-host: --root is required");
            eprintln!("usage: meepo-host --root <dir> --expected-root-id <id> [--idle-grace-ms N]");
            return ExitCode::from(2);
        }
    };

    if let Err(e) = std::fs::create_dir_all(&root) {
        eprintln!("meepo-host: cannot create root '{root}': {e}");
        return ExitCode::from(2);
    }
    let sock = format!("{root}/host.sock");
    let listener = match meepo_host::transport::bind(&sock) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("meepo-host: bind {sock}: {e}");
            return ExitCode::from(2);
        }
    };

    let mut dispatcher = meepo_host::Dispatcher::new();
    meepo_host::handlers::host::register(&mut dispatcher);
    let epoch = uuid::Uuid::new_v4().to_string();
    let idle_grace = idle_grace_ms
        .map(std::time::Duration::from_millis)
        .unwrap_or(meepo_host::server::kernel::DEFAULT_IDLE_GRACE);

    let shutdown = tokio_util::sync::CancellationToken::new();
    install_signal_handler(shutdown.clone());
    let kernel = meepo_host::HostKernel::new(
        epoch,
        dispatcher,
        std::sync::Arc::new(meepo_host::SessionContinuityCoordinator::new()),
    );
    let root_path = std::path::Path::new(&root);
    match kernel.serve_owned(listener, root_path, &sock, idle_grace, shutdown).await {
        Ok(meepo_host::server::ServeOutcome::Loser) => {
            eprintln!("[meepo-host] another process owns '{root}' — exiting");
            ExitCode::SUCCESS
        }
        Ok(meepo_host::server::ServeOutcome::Done) => {
            eprintln!("[meepo-host] shut down");
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("[meepo-host] fatal: {e}");
            ExitCode::FAILURE
        }
    }
}

/// Cancel `shutdown` on SIGINT (ctrl-c) and SIGTERM so the kernel drains.
fn install_signal_handler(shutdown: tokio_util::sync::CancellationToken) {
    {
        let s = shutdown.clone();
        tokio::spawn(async move {
            if tokio::signal::ctrl_c().await.is_ok() {
                eprintln!("[meepo-host] interrupt received, draining");
                s.cancel();
            }
        });
    }
    #[cfg(unix)]
    {
        tokio::spawn(async move {
            let mut term = match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
                Ok(t) => t,
                Err(_) => return,
            };
            term.recv().await;
            eprintln!("[meepo-host] SIGTERM received, draining");
            shutdown.cancel();
        });
    }
}
