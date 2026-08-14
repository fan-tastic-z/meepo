//! Host server: kernel, connection loop, operation dispatcher.

pub mod connection;
pub mod dispatcher;
pub mod kernel;

pub use dispatcher::{Dispatcher, OpContext, Outcome};
pub use kernel::HostKernel;
