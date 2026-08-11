//! Provider backends — concrete [`AgentBackend`] implementations.
//!
//! All real LLM access goes through [`AimuxBackend`], which wraps any
//! `aimux_core::LanguageModel` (325+ providers via the aimux library).
//! The hand-written OpenAI and Anthropic backends have been removed — aimux
//! handles all provider-specific HTTP, SSE parsing, and message formatting.

pub mod aimux_backend;

pub use aimux_backend::AimuxBackend;
