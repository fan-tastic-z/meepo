//! Host client — connect, handshake, drive request/response, and discover the
//! owner of a root via connect-or-spawn.

pub mod connect_or_spawn;
pub mod connection;
pub mod launcher;

pub use connect_or_spawn::{connect_or_spawn, connect_or_spawn_with, ConnectError};
pub use connection::{ClientError, HostClient};
