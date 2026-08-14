//! Bootstrap handlers: `host.status` and `host.diagnostics.query`.
//!
//! Both are `availability: bootstrap` — callable before the host reaches
//! `Ready` — so a client can probe liveness and version negotiation.

use serde::Serialize;

use crate::protocol::LifecycleState;
use crate::server::dispatcher::{handler, Dispatcher, Outcome};

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct HostStatusOutput {
    host_epoch: String,
    state: LifecycleState,
}

fn status_outcome(host_epoch: String, state: LifecycleState) -> Outcome {
    Outcome::Ok(
        serde_json::to_value(HostStatusOutput { host_epoch, state })
            .expect("host status serializes"),
    )
}

/// Register the bootstrap host handlers on `dispatcher`.
pub fn register(dispatcher: &mut Dispatcher) {
    dispatcher.register(
        "host.status",
        handler(|_input, ctx| async move { status_outcome(ctx.host_epoch, ctx.lifecycle) }),
    );
    dispatcher.register(
        "host.diagnostics.query",
        handler(|_input, ctx| async move { status_outcome(ctx.host_epoch, ctx.lifecycle) }),
    );
}
