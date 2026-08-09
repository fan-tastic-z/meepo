//! Provider backends — concrete [`AgentBackend`] implementations.
//!
//! Each module is one provider. They depend on `meepo-core`'s port and stay
//! decoupled from the runner, so the runtime never imports provider-specific
//! code.

pub mod openai;

pub use openai::OpenAiBackend;
