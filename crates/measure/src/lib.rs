//! measure: reach diagnostics over the bifrost overlay.
//!
//! Ping and speed are diagnostics on reach itself, the cheapest high-signal proofs of the thesis: an
//! RTT to a peer addressed by their public key, and throughput over the session that reaches them.
//! Both ride a tiny versioned protocol on bifrost streams and are transport-blind (generic over
//! `bifrost::Session`), so the identical test runs over iroh, mem, and any future transport. That is
//! the payoff: speed is iperf, but over any transport, a built-in transport dyno.
//!
//! ping and speed are TWO independent services, so a node may advertise one without the other: `ping`
//! (cheap RTT) and `speed` (bandwidth-eating throughput). The served entries are [`server::Ping`] /
//! [`server::Speed`] (family routes, owner limits, never public) and [`server::MeteredPing`] /
//! [`server::MeteredSpeed`] (public routes, the safety caps by construction), each behind its own gate
//! and each refusing the other's method at the wire. A client constructs a [`Ping`] or [`Speedtest`],
//! runs it against a session, and reads back a report.
//!
//! The owner entries are bound through the contract's typed adapter (`Serve(Ping::new(..))`), which
//! decodes the [`protocol::Opening`] frame and hands the stream halves on by value; the metered entries
//! are bound directly, because their wall clock has to cover the opening read. Either way the codec
//! touches the preamble only, never a payload byte.

pub mod ping;
pub mod protocol;
pub mod server;
pub mod speed;

mod payload;
mod responder;

#[cfg(test)]
#[path = "reach_tests.rs"]
mod reach_tests;

pub use ping::{Ping, PingReport, Probe};
pub use protocol::{MethodRefusal, ProtocolError, Refusal, WireVersion};
pub use speed::{Limit, Mode, Progress, SpeedReport, Speedtest, Throughput};
