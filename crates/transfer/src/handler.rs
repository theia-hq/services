//! The engine's `Handler` impl: `recv:` as the contract sees it.
//!
//! The ceiling is [`Never`]: a stranger writing files into the node's output directory has no legitimate
//! public use, so the gate is the only auth. The receive body stays crate-private; this impl is the entry.

use core::sync::atomic::{AtomicU64, Ordering};
use std::path::PathBuf;

use tightbeam_handler::open_policy::{Never, PublicUse};
use tightbeam_handler::{BoxRead, BoxWrite, Handler, ServeError, Served};

use crate::serve::Received;

/// The `recv:` engine: receive pushed files into `out`, one per admitted stream.
///
/// Each instance owns its output directory and its temp-file tag counter, so two receive services never
/// share one and concurrent pushes on one never contend for the same temp path. It prints nothing: a landed
/// file is reported only as a [`Received`] value handed to the [`ReceivedSink`] its constructor installed,
/// and with no sink installed the engine is silent.
pub struct Recv {
    out: PathBuf,
    next_tag: AtomicU64,
    sink: Option<Box<dyn ReceivedSink>>,
}

impl Recv {
    /// Receive pushed files into `out`, reporting nothing.
    pub fn new(out: PathBuf) -> Self {
        Self {
            out,
            next_tag: AtomicU64::new(0),
            sink: None,
        }
    }

    /// Hand every landed file's [`Received`] to `sink`. The caller that constructs the engine owns what,
    /// if anything, a landed file becomes on a screen or in a log; the engine only produces the fact.
    #[must_use]
    pub fn with_sink(mut self, sink: impl ReceivedSink) -> Self {
        self.sink = Some(Box::new(sink));
        self
    }
}

/// Where a [`Recv`] reports each file it lands: once per file, after the verified bytes are in place, and
/// never for a transfer that failed (a failure is the serve call's error, reported once, by its caller).
///
/// The call runs on the stream's serve path, before the stream completes, so an implementation MUST NOT
/// block or wait: a stalled renderer would hold every sender's stream open behind it, and a sender can
/// time its stream. Queue the fact and return; drop it when the queue is full.
pub trait ReceivedSink: Send + Sync + 'static {
    /// One file landed. The path is the peer-named safe relative path, raw: it is safe to join under the
    /// output directory and NOT safe to print, so whoever renders it escapes it first.
    fn received(&self, file: Received);
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
        // The contract's stream-failure arm carries the typed cause as its source, so a consumer that
        // downcasts still sees the `ReceiveError`, and the rendered text is the arm's own message.
        let received = crate::serve::receive_file(writer, reader, &self.out, tag)
            .await
            .map_err(|error| ServeError::Io(std::io::Error::other(error)))?;
        // The arrival is a value, never a line: rendering, escaping, and whether to show it at all belong
        // to the caller that installed the sink.
        if let Some(sink) = &self.sink {
            sink.received(received);
        }
        Ok(())
    }
}

/// The engine's ceiling, asserted at compile time: a `Never` flip here would let a stranger write into the
/// node's output directory through an open gate.
const _: () = assert!(!<<Recv as Handler>::Exposure as PublicUse>::OPEN_SAFE);
