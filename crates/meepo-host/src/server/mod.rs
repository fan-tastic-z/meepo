//! Host server: kernel, connection loop, operation dispatcher, ownership,
//! composition, and turn coordination.

pub mod composition;
pub mod connection;
pub mod dispatcher;
pub mod interaction;
pub mod kernel;
pub mod registration;
pub mod turn;

pub use composition::{BackendFactory, Composition};
pub use dispatcher::{Dispatcher, OpContext, Outcome};
pub use interaction::{AnswerError, HubPrompter, InteractionContext, InteractionHub};
pub use kernel::{HostKernel, ServeOutcome};
pub use registration::{read_registration, HostRegistration, Ownership};
pub use turn::{ResumePlan, ResumeStart, TurnCoordinator, TurnError, TurnStarted, TurnStopped};
