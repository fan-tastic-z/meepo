//! Host kernel — owns the listening socket and spawns a connection task per
//! accepted client. Holds the host epoch and the operation dispatcher. Process
//! ownership (flock), registration discovery, idle daemon, and signal handling
//! arrive in phase 5.

use tokio::net::UnixListener;

use crate::protocol::LifecycleState;
use crate::server::connection::serve_connection;
use crate::server::dispatcher::{Dispatcher, OpContext};
use crate::transport::FramedConn;

pub struct HostKernel {
    host_epoch: String,
    dispatcher: Dispatcher,
}

impl HostKernel {
    pub fn new(host_epoch: impl Into<String>, dispatcher: Dispatcher) -> Self {
        Self { host_epoch: host_epoch.into(), dispatcher }
    }

    /// Accept connections until the listener fails. Each connection is served
    /// on its own task. The kernel's lifecycle is `Ready` for the spine (phase
    /// 5 adds the starting/recovering/draining transitions).
    pub async fn serve(self, listener: UnixListener) {
        let dispatcher = std::sync::Arc::new(self.dispatcher);
        let ctx = OpContext {
            host_epoch: self.host_epoch,
            lifecycle: LifecycleState::Ready,
        };
        loop {
            match listener.accept().await {
                Ok((stream, _)) => {
                    let conn = FramedConn::new(stream);
                    let dispatcher = dispatcher.clone();
                    let ctx = ctx.clone();
                    tokio::spawn(async move {
                        serve_connection(conn, ctx, dispatcher).await;
                    });
                }
                Err(_) => break,
            }
        }
    }
}
