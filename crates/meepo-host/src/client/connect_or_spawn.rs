//! Connect-or-spawn election: connect to an existing owner of a root, or
//! launch a candidate and poll for its registration until the election
//! deadline.

use std::future::Future;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use crate::client::HostClient;
use crate::protocol::LifecycleState;
use crate::server::read_registration;

/// Default election deadline (the bounded window a candidate has to win the
/// flock and publish its registration).
pub const DEFAULT_ELECTION_DEADLINE: Duration = Duration::from_secs(45);

#[derive(Debug, thiserror::Error)]
pub enum ConnectError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("election deadline exceeded")]
    Timeout,
}

/// Connect to the owner of `root`, or launch a candidate via `launch` and poll
/// for its registration until `deadline`.
///
/// `launch` is injected so production spawns the `meepo-host` binary while tests
/// start an in-process kernel — the election logic is identical either way.
pub async fn connect_or_spawn_with<F, Fut>(
    root: &Path,
    deadline: Duration,
    launch: F,
) -> Result<(HostClient, String), ConnectError>
where
    F: FnOnce(PathBuf) -> Fut,
    Fut: Future<Output = ()>,
{
    // Fast path: an owner is already registered and not draining.
    if let Some(reg) = read_registration(root) {
        if reg.state != LifecycleState::Draining {
            if let Ok(pair) = HostClient::connect(&reg.endpoint).await {
                return Ok(pair);
            }
        }
    }

    // Launch a candidate, then poll for its registration.
    launch(root.to_path_buf()).await;
    let start = Instant::now();
    let mut backoff = Duration::from_millis(20);
    loop {
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_millis(250));
        if let Some(reg) = read_registration(root) {
            if reg.state != LifecycleState::Draining {
                if let Ok(pair) = HostClient::connect(&reg.endpoint).await {
                    return Ok(pair);
                }
            }
        }
        if start.elapsed() >= deadline {
            return Err(ConnectError::Timeout);
        }
    }
}

/// Connect-or-spawn with the default launcher: spawn `meepo-host` from the
/// directory of the current executable.
pub async fn connect_or_spawn(root: &Path) -> Result<(HostClient, String), ConnectError> {
    connect_or_spawn_with(root, DEFAULT_ELECTION_DEADLINE, |r| async move {
        let exe = std::env::current_exe()
            .ok()
            .and_then(|e| e.parent().map(|p| p.join("meepo-host")))
            .unwrap_or_else(|| PathBuf::from("meepo-host"));
        let _ = crate::client::launcher::spawn_candidate(&exe, &r);
    })
    .await
}
