//! The receive handler: take one admitted stream, receive one verified blob into
//! a temp file, then move it into place under the output directory, named by a peer-supplied header reduced
//! to a safe relative path.

use std::path::{Path, PathBuf};

use bifrost::wire::Transfer;
use tokio::io::{self, AsyncWriteExt as _};

use crate::handler::render_path;

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
        let mut sink =
            tokio::fs::File::create(&temp)
                .await
                .map_err(|source| ReceiveError::CreateTemp {
                    path: render_path(&temp),
                    source,
                })?;
        match Transfer::new(writer, reader).recv(&mut sink).await {
            Ok(received) => {
                sink.flush().await.map_err(|source| ReceiveError::Flush {
                    path: render_path(&temp),
                    source,
                })?;
                received
            }
            Err(err) => {
                drop(sink);
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
    // Every path in an error renders through the same escape/cap helper as the success event: the serve
    // loop logs the text at warn, and the sender names the final path, so a raw newline or ESC may not
    // ride the line.
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
/// byte length. Returned so the caller can report what landed.
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

#[cfg(test)]
#[path = "serve_tests.rs"]
mod serve_tests;
