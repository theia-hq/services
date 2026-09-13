//! The engine's `Handler` impl: `recv:` as the contract sees it.
//!
//! The ceiling is [`Never`]: a stranger writing files into the node's sink directory has no legitimate public
//! use, so the gate is the only auth. The receive body stays crate-private; this impl is the entry.

use core::sync::atomic::{AtomicU64, Ordering};
use std::path::{Path, PathBuf};

use tightbeam_handler::open_policy::{Never, PublicUse};
use tightbeam_handler::{BoxRead, BoxWrite, Handler, ServeError, Served};

/// The longest a peer-supplied path may render in the activity event, in characters. The path is
/// peer-controlled and unbounded up to the wire frame, so the render caps what reaches the log.
const MAX_RENDERED_PATH: usize = 256;

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
        // The arrival as ONE structured event: the fields carry the facts (the safe relative path,
        // escaped and capped because a peer supplies it, and the verified byte count), and a composing
        // program's subscriber decides whether and how to render them. No user-facing prose lives here.
        let path = render_path(&received.path);
        tracing::info!(
            path = %path,
            bytes = received.bytes,
            "received"
        );
        Ok(())
    }
}

/// Render a peer-supplied path for the activity event: escape control characters and cap the rendered
/// length. A raw newline forges a log line, a carriage return rewrites one, and ESC drives a terminal, so
/// none may reach the event as-is. `char::escape_debug` escapes those and leaves printable text, including
/// non-ASCII names, alone.
///
/// INTERIM (delib-63): the engine renders here only because the root-owned typed sink does not exist yet;
/// once the root renderer lands it owns this rule, and this helper moves with it.
fn render_path(path: &Path) -> String {
    let raw = path.to_string_lossy();
    // The cap plus the `...` cut marker: allocation is bounded whatever the peer names.
    let mut rendered = String::with_capacity(MAX_RENDERED_PATH + 3);
    let mut written = 0usize;
    'chars: for ch in raw.chars() {
        for escaped in ch.escape_debug() {
            if written == MAX_RENDERED_PATH {
                rendered.push_str("...");
                break 'chars;
            }
            rendered.push(escaped);
            written += 1;
        }
    }
    rendered
}

/// The engine's ceiling, asserted at compile time: a `Never` flip here would let a stranger write into the
/// node's sink directory through an open gate.
const _: () = assert!(!<<Recv as Handler>::Exposure as PublicUse>::OPEN_SAFE);

#[cfg(test)]
#[path = "handler_tests.rs"]
mod handler_tests;
