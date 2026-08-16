//! meepo-host daemon entry point.
//!
//! Boots the single-owner host: opens the storage root (`<root>/runtime.sqlite`),
//! wires the composition (provider backend factory + per-session managers), the
//! interaction hub, and the continuity/turn coordinators, then serves with
//! flock ownership, registration discovery, idle self-shutdown, and
//! SIGINT/SIGTERM draining.

use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use meepo_core::PermissionMode;

#[tokio::main]
async fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut root: Option<String> = None;
    let mut idle_grace_ms: Option<u64> = None;
    let mut provider = "fake".to_string();
    let mut model: Option<String> = None;
    let mut base_url: Option<String> = None;
    let mut mode = PermissionMode::Ask;
    let mut system: Option<String> = None;
    let mut i = 0;
    let usage = |msg: &str| -> ExitCode {
        eprintln!("meepo-host: {msg}");
        eprintln!(
            "usage: meepo-host --root <dir> [--provider fake|openai|anthropic] [--model M] \
             [--base-url U] [--mode explore|ask|execute|bypass] [--system S] [--idle-grace-ms N]"
        );
        ExitCode::from(2)
    };
    while i < args.len() {
        let arg = args[i].clone();
        let value = |i: &mut usize| -> Option<String> {
            *i += 1;
            args.get(*i).cloned()
        };
        match arg.as_str() {
            "--root" => match value(&mut i) {
                Some(v) => root = Some(v),
                None => return usage("--root needs a value"),
            },
            "--provider" => match value(&mut i) {
                Some(v) => provider = v,
                None => return usage("--provider needs a value"),
            },
            "--model" => match value(&mut i) {
                Some(v) => model = Some(v),
                None => return usage("--model needs a value"),
            },
            "--base-url" => match value(&mut i) {
                Some(v) => base_url = Some(v),
                None => return usage("--base-url needs a value"),
            },
            "--system" => match value(&mut i) {
                Some(v) => system = Some(v),
                None => return usage("--system needs a value"),
            },
            "--idle-grace-ms" => match value(&mut i).map(|v| v.parse::<u64>()) {
                Some(Ok(v)) => idle_grace_ms = Some(v),
                _ => return usage("--idle-grace-ms needs a number"),
            },
            "--mode" => match value(&mut i).as_deref() {
                Some("explore") => mode = PermissionMode::Explore,
                Some("ask") => mode = PermissionMode::Ask,
                Some("execute") => mode = PermissionMode::Execute,
                Some("bypass") => mode = PermissionMode::Bypass,
                _ => return usage("--mode must be explore|ask|execute|bypass"),
            },
            "--expected-root-id" => {
                // Reserved for a future strict root check (the flock is the authority).
                i += 1;
            }
            other => return usage(&format!("unknown argument '{other}'")),
        }
        i += 1;
    }
    let Some(root) = root else {
        return usage("--root is required");
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

    // Storage + domain services.
    let db = format!("{root}/runtime.sqlite");
    let store = match meepo_storage::SqliteStore::open(&db) {
        Ok(s) => Arc::new(s),
        Err(e) => {
            eprintln!("meepo-host: open {db}: {e}");
            return ExitCode::from(2);
        }
    };
    let epoch = uuid::Uuid::new_v4().to_string();
    let continuity = Arc::new(meepo_host::SessionContinuityCoordinator::new());
    let hub = Arc::new(meepo_host::server::InteractionHub::new(store.clone(), continuity.clone()));
    let factory = meepo_host::server::provider_factory::daemon_backend_factory(
        &provider,
        model,
        base_url,
        mode,
        hub.clone(),
        epoch.clone(),
        store.clone(),
    );
    let composition = Arc::new(meepo_host::Composition::new(store.clone(), factory, system));
    let turns = Arc::new(meepo_host::TurnCoordinator::new(composition, continuity.clone()));

    let mut dispatcher = meepo_host::Dispatcher::new();
    meepo_host::handlers::host::register(&mut dispatcher);
    meepo_host::handlers::session::register(&mut dispatcher, store);
    meepo_host::handlers::interaction::register(&mut dispatcher, hub);
    meepo_host::handlers::turn::register(&mut dispatcher, turns);

    let idle_grace = idle_grace_ms
        .map(Duration::from_millis)
        .unwrap_or(meepo_host::server::kernel::DEFAULT_IDLE_GRACE);
    let shutdown = tokio_util::sync::CancellationToken::new();
    install_signal_handler(shutdown.clone());

    let kernel = meepo_host::HostKernel::new(epoch, dispatcher, continuity);
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
