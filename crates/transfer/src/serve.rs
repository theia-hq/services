//! The receive handler: take one admitted stream, receive one verified blob into
//! a temp file, then move it into place under the output directory, named by a peer-supplied header reduced
//! to a safe relative path.

use std::path::{Path, PathBuf};

use bifrost::wire::Transfer;
use tokio::io::{self, AsyncWriteExt as _};

/// Receive one pushed file over an admitted stream: stream it into a temp file under `out`, verify it end
/// to end (`bifrost-wire` checks every byte against the sender's BLAKE3 root), then move it into place at
/// the safe relative path the sender named. On any failure the temp file is removed, so a rejected or
/// truncated transfer never leaves a partial file behind.
///
/// `tag` distinguishes concurrent temp files on one node (the caller passes a per-stream value), so two
/// files arriving at once never contend for the same temp path.
///
/// Crate-private: the entry is the [`Recv`](crate::Recv) handler, the only public door, and the `Never`
/// ceiling it declares is the posture check.
pub(crate) async fn receive_file<W, R>(
    writer: W,
    reader: R,
    out: &Path,
    tag: u64,
) -> Result<Received, ReceiveError>
where
    W: io::AsyncWrite + Unpin,
    R: io::AsyncRead + Unpin,
{
    let temp = out.join(format!(".transfer-{}-{tag}.part", std::process::id()));
    let received = {
        let mut file =
            tokio::fs::File::create(&temp)
                .await
                .map_err(|source| ReceiveError::CreateTemp {
                    path: render_path(&temp),
                    source,
                })?;
        match Transfer::new(writer, reader).recv(&mut file).await {
            Ok(received) => {
                file.flush().await.map_err(|source| ReceiveError::Flush {
                    path: render_path(&temp),
                    source,
                })?;
                received
            }
            Err(err) => {
                drop(file);
                let _ = tokio::fs::remove_file(&temp).await;
                return Err(ReceiveError::Transfer(err));
            }
        }
    };

    let relative = safe_relative_path(&received.header);
    let final_path = out.join(&relative);
    if let Some(parent) = final_path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|source| ReceiveError::CreateDir {
                path: render_path(parent),
                source,
            })?;
    }
    // Every path in an error renders through `render_path`: the dispatcher logs the error text at warn,
    // and the sender names the final path, so a raw newline or ESC may not ride the line.
    tokio::fs::rename(&temp, &final_path)
        .await
        .map_err(|source| ReceiveError::Save {
            path: render_path(&final_path),
            source,
        })?;

    Ok(Received {
        path: relative,
        bytes: received.blob.len(),
    })
}

/// Why one pushed file was not saved. Each arm names the step that failed and the path it failed at,
/// already rendered safe for a log line; the transfer arm carries the wire's own typed failure (a bad
/// frame, a truncated stream, a blob that did not verify).
#[derive(Debug, thiserror::Error)]
pub enum ReceiveError {
    /// The temp file under the output directory could not be created.
    #[error("create the temp file {path}: {source}")]
    CreateTemp {
        /// The temp path, rendered for a log line.
        path: String,
        /// The filesystem failure.
        #[source]
        source: io::Error,
    },
    /// The wire transfer failed: a protocol fault, a truncated stream, or a blob that did not verify.
    #[error(transparent)]
    Transfer(#[from] bifrost::wire::Error),
    /// The verified bytes could not be flushed to the temp file.
    #[error("flush {path}: {source}")]
    Flush {
        /// The temp path, rendered for a log line.
        path: String,
        /// The filesystem failure.
        #[source]
        source: io::Error,
    },
    /// The destination's parent directory could not be created.
    #[error("create the directory {path}: {source}")]
    CreateDir {
        /// The directory, rendered for a log line.
        path: String,
        /// The filesystem failure.
        #[source]
        source: io::Error,
    },
    /// The verified temp file could not be moved onto the destination the sender named.
    #[error("save to {path}: {source}")]
    Save {
        /// The destination, rendered for a log line (the sender chose it).
        path: String,
        /// The filesystem failure.
        #[source]
        source: io::Error,
    },
}

/// One received file: the safe relative path it was saved at under the output directory, and its verified
/// byte length. The fact a [`ReceivedSink`](crate::ReceivedSink) is handed, so the caller can report what
/// landed. The path is raw and peer-named: safe to join under the output directory, never safe to print
/// unescaped.
#[derive(Debug, Clone)]
pub struct Received {
    /// The path the file was saved at, relative to the output directory.
    pub path: PathBuf,
    /// The verified length of the received bytes.
    pub bytes: u64,
}

/// Reduce a peer-supplied header to a safe relative path under the output directory: keep only normal
/// components, dropping roots, prefixes, and `..`, so a peer cannot write outside it. An all-stripped or
/// empty header falls back to `download`, so a pushed blob always lands somewhere nameable.
///
/// LOAD-BEARING: this is the path-traversal guard on the receive side. Without it a sender could name
/// `../../etc/authorized_keys` and write outside the output directory. Ported verbatim from iris; keep it.
pub fn safe_relative_path(header: &[u8]) -> PathBuf {
    let raw = String::from_utf8_lossy(header);
    let mut safe = PathBuf::new();
    for component in Path::new(raw.as_ref()).components() {
        if let std::path::Component::Normal(part) = component {
            safe.push(part);
        }
    }
    if safe.as_os_str().is_empty() {
        safe.push("download");
    }
    safe
}

/// Letters that render as blank space on a terminal. `char::escape_debug` treats them as printable and
/// passes them raw, so a path made only of them would print as nothing. They are escaped so a path in an
/// error is never invisible. A product rendering the engine's facts should escape the same set.
const BLANK_LETTERS: [char; 5] = ['\u{115f}', '\u{1160}', '\u{3164}', '\u{ffa0}', '\u{2800}'];

/// The longest a path may render in an error's text, in characters. The final path is peer-named and
/// unbounded up to the wire frame, so the render caps what reaches the log.
const MAX_RENDERED_PATH: usize = 256;

/// Render a path for an error's text: escape control characters and cap the rendered length. A raw
/// newline forges a log line, a carriage return rewrites one, and ESC drives a terminal, so none may reach
/// a line as-is. Escapes are rendered whole: when the next complete escape would pass the cap, the render
/// appends the cut marker and stops, so the cut never lands inside a sequence. `char::escape_debug` leaves
/// printable text alone except grapheme-extended marks, which it escapes (a combining accent renders as
/// `\u{...}`; an emoji passes raw). [`BLANK_LETTERS`] render as `\u{...}` too.
///
/// Errors need this here, in the engine, because their text is logged by the dispatcher that calls
/// `serve`, a channel no caller renders. A landed file is not rendered here at all: it leaves as a raw
/// [`Received`] value, and the caller that installed the sink escapes it where it prints it.
fn render_path(path: &Path) -> String {
    let raw = path.to_string_lossy();
    // The cap plus the `...` cut marker: allocation is bounded whatever the peer names.
    let mut rendered = String::with_capacity(MAX_RENDERED_PATH + 3);
    let mut written = 0usize;
    for ch in raw.chars() {
        let blank = BLANK_LETTERS.contains(&ch);
        // The width is the whole escape's, so the cap check below never admits half of one.
        let width = if blank {
            ch.escape_unicode().len()
        } else {
            ch.escape_debug().len()
        };
        if written + width > MAX_RENDERED_PATH {
            rendered.push_str("...");
            break;
        }
        if blank {
            rendered.extend(ch.escape_unicode());
        } else {
            rendered.extend(ch.escape_debug());
        }
        written += width;
    }
    rendered
}

#[cfg(test)]
#[path = "serve_tests.rs"]
mod serve_tests;
