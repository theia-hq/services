//! The engine's `Handler` impl: `recv:` as the contract sees it.
//!
//! The ceiling is [`Never`]: a stranger writing files into the node's sink directory has no legitimate public
//! use, so the gate is the only auth. The receive body stays crate-private; this impl is the entry.

use core::sync::atomic::{AtomicU64, Ordering};
use std::path::PathBuf;

use tightbeam_handler::open_policy::{Never, PublicUse};
use tightbeam_handler::{BoxRead, BoxWrite, Handler, ServeError, Served};

/// The `recv:` engine: receive pushed files into `out`, one per admitted stream.
///
/// Each instance owns its sink directory and its temp-file tag counter, so two receive services never share
/// a sink and concurrent pushes on one never contend for the same temp path.
pub struct Recv {
    out: PathBuf,
    next_tag: AtomicU64,
}

impl Recv {
    /// Receive pushed files into `out`.
    pub fn new(out: PathBuf) -> Self {
        Self {
            out,
            next_tag: AtomicU64::new(0),
        }
    }
}

impl Handler for Recv {
    /// NEVER: a receive service with no auth of its own would let anyone write files into the node's output
    /// directory; the gate IS its authentication.
    type Exposure = Never;

    async fn serve(
        &self,
        _served: Served<Self>,
        writer: BoxWrite,
        reader: BoxRead,
    ) -> Result<(), ServeError> {
        let tag = self.next_tag.fetch_add(1, Ordering::Relaxed);
        let received = crate::serve::receive_file(writer, reader, &self.out, tag)
            .await
            .map_err(|error| ServeError::Io(std::io::Error::other(format!("{error:#}"))))?;
        // The engine narrates at debug; the root renders its own user-facing line when it wires the route.
        tracing::debug!(
            path = %received.path.display(),
            bytes = received.bytes,
            "received a pushed file"
        );
        Ok(())
    }
}

/// The engine's ceiling, asserted at compile time: a `Never` flip here would let a stranger write into the
/// node's sink directory through an open gate.
const _: () = assert!(!<<Recv as Handler>::Exposure as PublicUse>::OPEN_SAFE);
