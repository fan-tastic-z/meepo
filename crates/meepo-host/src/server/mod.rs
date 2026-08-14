//! Host server: kernel, connection loop, operation dispatcher, ownership.

pub mod connection;
pub mod dispatcher;
pub mod kernel;
pub mod registration;

pub use dispatcher::{Dispatcher, OpContext, Outcome};
pub use kernel::{HostKernel, ServeOutcome};
pub use registration::{read_registration, HostRegistration, Ownership};
