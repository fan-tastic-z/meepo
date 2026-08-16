//! Host kernel — owns the listening socket, the operation dispatcher, and the
//! host lifecycle. `serve` is the plain accept loop (phase 4); `serve_owned`
//! is the production path: it acquires the per-root flock, publishes the
//! registration record, runs an idle daemon, and cleans up on shutdown.

use std::path::Path;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::net::UnixListener;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

use crate::continuity::SessionContinuityCoordinator;
use crate::protocol::LifecycleState;
use crate::server::connection::serve_connection;
use crate::server::dispatcher::{Dispatcher, OpContext};
use crate::server::registration::{self, HostRegistration, Ownership};
use crate::transport::FramedConn;

/// Default idle grace before an idle, quiescent host self-shuts down.
pub const DEFAULT_IDLE_GRACE: Duration = Duration::from_secs(30);

/// Why [`HostKernel::serve_owned`] returned.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServeOutcome {
    /// Served and shut down (idle timeout or listener closed).
    Done,
    /// Another process owns this root (flock lost); the caller should exit.
    Loser,
}

/// Shared kernel state observed by the accept loop and the idle timer.
struct Control {
    lifecycle: RwLock<LifecycleState>,
    connections: AtomicI64,
}

impl Control {
    fn new() -> Self {
        Self {
            lifecycle: RwLock::new(LifecycleState::Starting),
            connections: AtomicI64::new(0),
        }
    }

    /// True idle: Ready, no connections, no in-flight ops.
    fn is_idle(&self) -> bool {
        let st = self.lifecycle.try_read().map(|g| *g).unwrap_or(LifecycleState::Starting);
        st == LifecycleState::Ready && self.connections.load(Ordering::Relaxed) == 0
    }
}

pub struct HostKernel {
    host_epoch: String,
    dispatcher: Dispatcher,
    continuity: Arc<SessionContinuityCoordinator>,
}

impl HostKernel {
    pub fn new(
        host_epoch: impl Into<String>,
        dispatcher: Dispatcher,
        continuity: Arc<SessionContinuityCoordinator>,
    ) -> Self {
        Self {
            host_epoch: host_epoch.into(),
            dispatcher,
            continuity,
        }
    }

    /// Plain accept loop (phase 4): serve connections until the listener fails.
    pub async fn serve(self, listener: UnixListener) {
        let dispatcher = Arc::new(self.dispatcher);
        let continuity = self.continuity;
        let ctx = OpContext { host_epoch: self.host_epoch, lifecycle: LifecycleState::Ready };
        loop {
            match listener.accept().await {
                Ok((stream, _)) => {
                    let conn = FramedConn::new(stream);
                    let dispatcher = dispatcher.clone();
                    let continuity = continuity.clone();
                    let ctx = ctx.clone();
                    tokio::spawn(async move {
                        serve_connection(conn, ctx, dispatcher, continuity).await;
                    });
                }
                Err(_) => break,
            }
        }
    }

    /// Production serve: acquire the per-root flock, publish registration, run
    /// the accept loop with an idle daemon and an external shutdown signal,
    /// then clean up. Returns `Loser` if another process already owns the root.
    /// `shutdown` is cancelled on idle (and, from the binary, on SIGINT/SIGTERM).
    pub async fn serve_owned(
        self,
        listener: UnixListener,
        root: &Path,
        endpoint: &str,
        idle_grace: Duration,
        shutdown: CancellationToken,
    ) -> std::io::Result<ServeOutcome> {
        let ownership = match Ownership::try_acquire(root)? {
            Some(o) => o,
            None => return Ok(ServeOutcome::Loser),
        };
        let root_id = registration::root_id_of(root);
        let reg = HostRegistration::new(&root_id, &self.host_epoch, endpoint, LifecycleState::Ready);
        registration::write_registration(root, &reg)?;
        eprintln!(
            "[meepo-host] owner of {root_id} (epoch {}); listening on {endpoint}",
            self.host_epoch
        );

        let dispatcher = Arc::new(self.dispatcher);
        let continuity = self.continuity;
        let control = Arc::new(Control::new());
        *control.lifecycle.write().await = LifecycleState::Ready;

        // Idle daemon: after `idle_grace` of true idle, signal shutdown.
        let idle_control = control.clone();
        let idle_shutdown = shutdown.clone();
        let idle_handle = tokio::spawn(async move {
            loop {
                tokio::time::sleep(idle_grace).await;
                if idle_control.is_idle() {
                    idle_shutdown.cancel();
                    break;
                }
            }
        });

        // Accept loop, racing against the shared shutdown signal.
        loop {
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => break,
                accept = listener.accept() => match accept {
                    Ok((stream, _)) => {
                        control.connections.fetch_add(1, Ordering::Relaxed);
                        let conn = FramedConn::new(stream);
                        let dispatcher = dispatcher.clone();
                        let ctx = OpContext {
                            host_epoch: reg.host_epoch.clone(),
                            lifecycle: LifecycleState::Ready,
                        };
                        let ctrl = control.clone();
                        let continuity = continuity.clone();
                        tokio::spawn(async move {
                            serve_connection(conn, ctx, dispatcher, continuity).await;
                            ctrl.connections.fetch_sub(1, Ordering::Relaxed);
                        });
                    }
                    Err(_) => break,
                }
            }
        }

        // Drain: stop admitting, give in-flight connections a brief grace.
        drop(listener);
        *control.lifecycle.write().await = LifecycleState::Draining;
        idle_handle.abort();
        let drain_deadline = tokio::time::Instant::now() + Duration::from_millis(500);
        while control.connections.load(Ordering::Relaxed) > 0
            && tokio::time::Instant::now() < drain_deadline
        {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        // Clean up discovery (only if we still own it) and release the lock.
        registration::remove_registration(root, &reg.host_epoch)?;
        drop(ownership);
        Ok(ServeOutcome::Done)
    }
}
