//! meepo-host — the single-owner host daemon.
//!
//! One process owns the runtime + storage and speaks a framed NDJSON
//! protocol over a Unix domain socket. Clients (CLI, headless, future TUI)
//! connect to it rather than embedding the runtime in-process.
//!
//! The crate is built up in phases: protocol wire grammar, framed transport,
//! the host kernel + dispatcher, per-session admission + turn coordination,
//! the subscription/continuity stream, and the client. Each phase lands as a
//! self-contained, compiling module under `protocol/`, `transport/`,
//! `server/`, and `client/`.

pub mod protocol;
