//! The receive handler: take one admitted stream, receive one verified blob into
//! a fresh temp file, then land it under the output directory as a new file, named by a peer-supplied
//! header reduced to a safe relative path.
//!
//! A landing never replaces a file, never leaves the output directory through a symlinked directory, and
//! never targets the receiver's own temp files: a sender chooses the name, so every one of those would let
//! a push rewrite something the receiver did not offer.

use core::hash::BuildHasher as _;
use std::ffi::OsStr;
use std::hash::RandomState;
use std::path::{Path, PathBuf};

use bifrost::wire::Transfer;
use tokio::io::{self, AsyncWriteExt as _};

/// Receive one pushed file over an admitted stream: stream it into a fresh temp file under `out`, verify it
/// end to end (`bifrost-wire` checks every byte against the sender's BLAKE3 root), then land it at the safe
/// relative path the sender named. On any failure the temp file is removed, so a rejected or truncated
/// transfer never leaves a partial file behind.
///
/// `tag` is mixed into the temp file's random name; the temp is opened `create_new`, so two files arriving
/// at once never share one, and nothing already on disk is ever opened as a temp.
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
    let temp = out.join(TempName::fresh(tag).0);
    let received = {
        let mut file = tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp)
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

    let landed = land(&temp, out, &received.header).await;
    // The temp name goes either way: a landed file is already a second link to the same bytes, and a
    // refused one must not linger as a partial or stray file under the output directory.
    let _ = tokio::fs::remove_file(&temp).await;
    Ok(Received {
        path: landed?,
        bytes: received.blob.len(),
    })
}

/// Land the verified temp file at the path `header` names under `out`, as a new file, and return the safe
/// relative path it landed at.
///
/// Every path in an error renders through `render_path`: the dispatcher logs the error text at warn, and
/// the sender names the final path, so a raw newline or ESC may not ride the line.
async fn land(temp: &Path, out: &Path, header: &[u8]) -> Result<PathBuf, ReceiveError> {
    let relative = safe_relative_path(header);
    let final_path = out.join(&relative);
    // A header naming the temp pattern could target another stream's in-flight temp file, or plant a name
    // a later temp would collide with, so no component of a landing may look like one.
    if relative
        .components()
        .any(|component| TempName::matches(component.as_os_str()))
    {
        return Err(ReceiveError::TempName {
            path: render_path(&final_path),
        });
    }

    let landing = contained_path(out, &relative, &final_path).await?;

    // A hard link is the landing because it refuses an existing name, where a rename replaces it: a push
    // may only add a file, never rewrite one the receiver already holds. A symlink at the final name is an
    // existing name too, so it is refused rather than replaced or followed.
    match tokio::fs::hard_link(temp, &landing).await {
        Ok(()) => Ok(relative),
        Err(source) if source.kind() == io::ErrorKind::AlreadyExists => Err(ReceiveError::Exists {
            path: render_path(&final_path),
        }),
        Err(source) => Err(ReceiveError::Save {
            path: render_path(&final_path),
            source,
        }),
    }
}

/// Make the directories `relative` names under `out` and return where its file lands, never creating or
/// resolving anything outside `out`.
///
/// `safe_relative_path` strips `..` and roots, but a directory already inside `out` may be a symlink that
/// leads out of it, and the kernel follows it. So the walk goes one directory at a time: each is created
/// if missing (never through a symlink, which `mkdir` treats as an existing name), then canonicalized and
/// held under the canonical `out` before the next is made inside it. A symlink that leads out is refused
/// before anything is created under it, and the file lands under the last canonical directory, so no later
/// spelling of the path is followed again.
async fn contained_path(
    out: &Path,
    relative: &Path,
    final_path: &Path,
) -> Result<PathBuf, ReceiveError> {
    let root = canonical(out).await?;
    let mut directories = relative.components();
    // `safe_relative_path` never returns an empty path. Were it to, the landing would name `out` itself,
    // which the link refuses as existing.
    let Some(name) = directories.next_back() else {
        return Ok(root);
    };
    let mut dir = root.clone();
    for directory in directories {
        let next = dir.join(directory);
        match tokio::fs::create_dir(&next).await {
            Ok(()) => {}
            Err(source) if source.kind() == io::ErrorKind::AlreadyExists => {}
            Err(source) => {
                return Err(ReceiveError::CreateDir {
                    path: render_path(&next),
                    source,
                });
            }
        }
        let resolved = canonical(&next).await?;
        if !resolved.starts_with(&root) {
            return Err(ReceiveError::Escapes {
                path: render_path(final_path),
            });
        }
        dir = resolved;
    }
    Ok(dir.join(name))
}

/// Resolve `path` to its canonical form, every symlink followed, for the containment check.
async fn canonical(path: &Path) -> Result<PathBuf, ReceiveError> {
    tokio::fs::canonicalize(path)
        .await
        .map_err(|source| ReceiveError::Resolve {
            path: render_path(path),
            source,
        })
}

/// The receiver's own temp file name: `.transfer-<random>.part`. The name is unguessable so a sender
/// cannot aim at an in-flight temp, and the pattern is fixed so a landing that matches it can be refused.
struct TempName(String);

impl TempName {
    const PREFIX: &str = ".transfer-";
    const SUFFIX: &str = ".part";

    /// A fresh name. `RandomState` keys SipHash from the OS's randomness and moves its keys on every call,
    /// so hashing the tag yields a value a peer cannot predict, with no randomness dependency.
    fn fresh(tag: u64) -> Self {
        let random = RandomState::new().hash_one(tag);
        Self(format!("{}{random:016x}{}", Self::PREFIX, Self::SUFFIX))
    }

    /// Whether `name` has the temp pattern's shape, whatever its middle.
    fn matches(name: &OsStr) -> bool {
        name.to_str()
            .is_some_and(|name| name.starts_with(Self::PREFIX) && name.ends_with(Self::SUFFIX))
    }
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
    /// The output directory, or a directory on the way to the destination, could not be resolved to its
    /// canonical form.
    #[error("resolve {path}: {source}")]
    Resolve {
        /// The directory, rendered for a log line.
        path: String,
        /// The filesystem failure.
        #[source]
        source: io::Error,
    },
    /// The sender named a path whose directory resolves outside the output directory, through a symlinked
    /// directory inside it. Nothing is written there.
    #[error("{path} leads outside the output directory")]
    Escapes {
        /// The destination, rendered for a log line (the sender chose it).
        path: String,
    },
    /// The sender named a path in the receiver's temp file pattern, which could target another stream's
    /// in-flight file.
    #[error("{path} has the name of a receive temp file")]
    TempName {
        /// The destination, rendered for a log line (the sender chose it).
        path: String,
    },
    /// The sender named a path that already exists. A push only adds a file; it never replaces one.
    #[error("{path} already exists")]
    Exists {
        /// The destination, rendered for a log line (the sender chose it).
        path: String,
    },
    /// The verified temp file could not be linked at the destination the sender named.
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
