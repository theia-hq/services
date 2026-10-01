//! transfer: push a file to a keyed node, and receive one there, off an admitted stream.
//!
//! [`wire`] is the frame both ends speak: a sender dials, opens one stream per file, and drives a
//! [`wire::Transfer`] to the receiver. The rest of this crate is that receiver's per-stream work: read one
//! blob off an admitted stream, check it against the root the sender sent, and save it under an output
//! directory, reducing the sender-supplied name to a safe relative path so a peer can never write outside
//! that directory. Each saved file is reported with the key the gate admitted its sender under, which is
//! what says who sent it; the root only says the bytes arrived as the sender read them.
//!
//! It is a service crate: it knows what to DO with an admitted stream, never how the peer was reached or
//! gated. The composing consumer binds [`Recv`](crate::Recv) into its route table, so every pushed file rides
//! the same family gate as every other service; the sender side (dial, expand directories, pipeline
//! concurrent streams) is a client verb driving [`wire::Transfer::send`].
//!
//! One stream carries one file. The exposer accepts a sender's per-file streams concurrently, so a
//! directory's files land in parallel with no fan-out logic here: each invocation is one file, start to
//! finish.

// An engine produces facts and never prints: a landed file leaves as a value through the sink its
// caller installed, and the caller owns every line a user sees. A print macro here would put
// peer-named bytes on a terminal past the one place that escapes them, so it fails the build.
#![deny(clippy::print_stdout, clippy::print_stderr, clippy::dbg_macro)]

mod handler;
mod serve;
pub mod wire;

pub use handler::{ReceivedSink, Recv};

pub use crate::serve::{ReceiveError, Received, safe_relative_path};
