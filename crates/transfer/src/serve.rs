//! The receive handler: take one admitted stream, receive one verified blob into
//! a fresh temp file, then land it under the output directory as a new file, named by a peer-supplied
//! header reduced to a safe relative path.
//!
//! A landing never replaces a file, never passes through a symlink, and never targets the receiver's own
//! temp files: a sender chooses the name, so every one of those would let a push rewrite something the
//! receiver did not offer. Every step after opening the output directory works from a directory handle,
//! never a path string, so nothing on disk can be swapped between a check and the use it guards.

use core::hash::BuildHasher as _;
use std::ffi::OsStr;
use std::hash::RandomState;
use std::os::fd::{AsFd as _, BorrowedFd, OwnedFd};
use std::path::{Path, PathBuf};

use nauthy::VerifyKey;
use rustix::fs::{AtFlags, FileType, Mode, OFlags};
use rustix::io::Errno;
use tokio::io::{self, AsyncWriteExt as _};

use crate::wire::{Blob, Incoming, Transfer};

/// Receive one pushed file over an admitted stream from `from`: read the frame's head, stream the body into
/// a fresh temp file under `out`, check it against the root the sender sent, land it at the safe relative
/// path the sender named, and only then answer the sender. On any failure, and when the returned future is dropped mid-stream, the temp file is removed, so a
/// rejected, truncated or abandoned transfer never leaves a partial file behind.
///
/// Nothing is spent on a stream before its head is read: the output directory is opened and the temp made
/// only for a frame that is ours and names a landing the receiver could make. A stream that stays silent,
/// is not ours, or names a refused path costs no descriptor and no disk, and its body is never written.
///
/// The answer comes last because it is the sender's only word on the push: a sender told yes before the
/// landing would report a file as delivered that a refusal then threw away, while another push held its
/// name. So every refusal after the frame's head, from a bad name to an existing file, reaches the sender
/// as a refusal.
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
    from: VerifyKey,
) -> Result<Received, ReceiveError>
where
    W: io::AsyncWrite + Unpin,
    R: io::AsyncRead + Unpin,
{
    let mut incoming = Transfer::new(writer, reader).recv().await?;
    let landed = land(&mut incoming, out, tag, from).await;
    incoming.answer(landed).await
}

/// Every step between reading the frame's head and answering the sender, so that the answer is made from
/// one outcome: refuse the name, make the temp, take the body into it, then land it under `out`.
async fn land<W, R>(
    incoming: &mut Incoming<W, R>,
    out: &Path,
    tag: u64,
    from: VerifyKey,
) -> Result<Received, ReceiveError>
where
    W: io::AsyncWrite + Unpin,
    R: io::AsyncRead + Unpin,
{
    let landing = Landing::parse(incoming.header(), out)?;

    // Each filesystem step is a blocking syscall, so it runs on the blocking pool. Were this future dropped
    // while one runs, the step still finishes and its `Temp` drops there, which removes the temp.
    let opening = out.to_path_buf();
    let (temp, file) = tokio::task::spawn_blocking(move || Temp::create(&opening, tag))
        .await
        .map_err(|joined| ReceiveError::CreateTemp {
            path: render_path(out),
            source: io::Error::other(joined),
        })??;

    let mut file = tokio::fs::File::from_std(file);
    incoming.verify_into(&mut file).await?;
    file.flush().await.map_err(|source| ReceiveError::Flush {
        path: render_path(&out.join(temp.name.as_str())),
        source,
    })?;
    // The temp is closed before it moves, so no handle outlives the name it was opened under.
    drop(file);

    let blob = *incoming.blob();
    let final_path = out.join(&landing.relative);
    let path = tokio::task::spawn_blocking(move || temp.land(landing, &blob))
        .await
        .map_err(|joined| ReceiveError::Save {
            path: render_path(&final_path),
            source: io::Error::other(joined),
        })??;
    Ok(Received {
        path,
        bytes: blob.len(),
        from,
    })
}

/// Where a sender asked a file to land: its header reduced to a safe relative path, with every refusal
/// that depends only on the name already made. Parsing it first means a refused push creates nothing,
/// not even the temp, and its body is never written.
struct Landing {
    /// The safe relative path the file lands at under the output directory.
    relative: PathBuf,
    /// Where `relative` renders from, for an error's text.
    out: PathBuf,
}

impl Landing {
    /// Reduce `header` to a landing under `out`, refusing a name the receiver could never create.
    fn parse(header: &[u8], out: &Path) -> Result<Self, ReceiveError> {
        let landing = Self {
            relative: safe_relative_path(header),
            out: out.to_path_buf(),
        };
        // A header naming the temp pattern could target another stream's in-flight temp file, or plant a
        // name a later temp would collide with, so no component of a landing may look like one.
        if landing.relative.iter().any(TempName::matches) {
            return Err(ReceiveError::TempName {
                path: landing.render(),
            });
        }
        // A NUL cannot reach a syscall, which takes C strings, so the name fails here, before a directory
        // is made for it, with the error the syscall would have returned.
        if landing.relative.as_os_str().as_encoded_bytes().contains(&0) {
            return Err(ReceiveError::Save {
                path: landing.render(),
                source: Errno::INVAL.into(),
            });
        }
        Ok(landing)
    }

    /// The destination under the output directory, rendered for an error's text.
    fn render(&self) -> String {
        render_path(&self.out.join(&self.relative))
    }

    /// Open, or make, each directory the landing names under `out`, one handle at a time, and return the
    /// handle its file lands in (`None` for `out` itself).
    ///
    /// Each directory is opened `O_NOFOLLOW` through its parent's handle, so a symlink is refused rather
    /// than followed, whether it leads out of the output directory or not, and a directory swapped for a
    /// symlink after an earlier step is refused the same way. The existing directories are all opened before
    /// the first missing one is made, so a refusal on the way creates nothing.
    fn walk(&self, out: BorrowedFd<'_>) -> Result<Option<OwnedFd>, ReceiveError> {
        let mut directories = self.relative.parent().into_iter().flat_map(Path::iter);
        let mut dir: Option<OwnedFd> = None;
        let mut path = self.out.clone();
        for directory in directories.by_ref() {
            path.push(directory);
            let parent = dir.as_ref().map_or(out, |dir| dir.as_fd());
            match open_dir(parent, directory) {
                Ok(next) => dir = Some(next),
                Err(Errno::NOENT) => {
                    dir = Some(make_dir(parent, directory, &path)?);
                    break;
                }
                Err(Errno::LOOP | Errno::NOTDIR) if is_symlink(parent, directory) => {
                    return Err(ReceiveError::Escapes {
                        path: self.render(),
                    });
                }
                Err(errno) => {
                    return Err(ReceiveError::OpenDir {
                        path: render_path(&path),
                        source: errno.into(),
                    });
                }
            }
        }
        // Below a directory this landing just made, nothing exists yet, so the rest are made without a look.
        for directory in directories {
            path.push(directory);
            let parent = dir.as_ref().map_or(out, |dir| dir.as_fd());
            dir = Some(make_dir(parent, directory, &path)?);
        }
        Ok(dir)
    }

    /// The file name the landing ends in. `safe_relative_path` never returns an empty path, so there is
    /// always one; were there not, the empty name would fail the landing as a save error.
    fn name(&self) -> &OsStr {
        self.relative.file_name().unwrap_or_default()
    }
}

/// Open the directory `name` under `parent`, never following a symlink at `name`.
fn open_dir(parent: BorrowedFd<'_>, name: &OsStr) -> rustix::io::Result<OwnedFd> {
    rustix::fs::openat(
        parent,
        name,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
}

/// Make the directory `name` under `parent` and open it. Another stream making the same directory first is
/// not a failure: the open that follows refuses anything there but a real directory.
fn make_dir(parent: BorrowedFd<'_>, name: &OsStr, path: &Path) -> Result<OwnedFd, ReceiveError> {
    match rustix::fs::mkdirat(parent, name, DIR_MODE) {
        Ok(()) | Err(Errno::EXIST) => {}
        Err(errno) => {
            return Err(ReceiveError::CreateDir {
                path: render_path(path),
                source: errno.into(),
            });
        }
    }
    open_dir(parent, name).map_err(|errno| ReceiveError::OpenDir {
        path: render_path(path),
        source: errno.into(),
    })
}

/// Whether `name` under `parent` is a symlink. `O_NOFOLLOW` refuses one with `ELOOP` on macOS and, with
/// `O_DIRECTORY`, `ENOTDIR` on Linux, the same error a file in the way returns, so the refusal asks.
fn is_symlink(parent: BorrowedFd<'_>, name: &OsStr) -> bool {
    rustix::fs::statat(parent, name, AtFlags::SYMLINK_NOFOLLOW)
        .is_ok_and(|stat| FileType::from_raw_mode(stat.st_mode).is_symlink())
}

/// A file's creation mode before the umask, as `std::fs::File::create` uses.
const FILE_MODE: Mode = Mode::from_raw_mode(0o666);

/// A directory's creation mode before the umask, as `std::fs::create_dir` uses.
const DIR_MODE: Mode = Mode::from_raw_mode(0o777);

/// The receive's temp file at the top of the output directory, held through the directory's handle.
/// Dropping it removes the temp, so a stream that fails, a flush that fails, and a stream dropped at its
/// caller's deadline all leave nothing behind; landing it disarms the removal.
struct Temp {
    /// The output directory, opened once: the temp is made, landed and removed relative to it.
    out: OwnedFd,
    /// The temp's name in `out`.
    name: TempName,
    /// Whether the temp is still on disk under `name`, so dropping it must remove it.
    pending: bool,
}

impl Temp {
    /// Open `out` and make a fresh temp in it, returning the guard and the file to write.
    fn create(out: &Path, tag: u64) -> Result<(Self, std::fs::File), ReceiveError> {
        let dir = rustix::fs::open(
            out,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(|errno| ReceiveError::OpenDir {
            path: render_path(out),
            source: errno.into(),
        })?;
        let name = TempName::fresh(tag);
        let file = rustix::fs::openat(
            &dir,
            name.as_str(),
            OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::CLOEXEC,
            FILE_MODE,
        )
        .map_err(|errno| ReceiveError::CreateTemp {
            path: render_path(&out.join(name.as_str())),
            source: errno.into(),
        })?;
        let temp = Self {
            out: dir,
            name,
            pending: true,
        };
        Ok((temp, file.into()))
    }

    /// Move the verified temp to `landing` as a new file and return the safe relative path it landed at.
    ///
    /// A name that already holds exactly `blob` counts as landed, and the temp is removed: a sender whose
    /// file landed but whose answer was lost (a deadline, a dropped connection) retries, and a retry must
    /// not read as the refusal a squatted name gets. The held file is never written. A pusher learns
    /// from this only that a name holds bytes it already has.
    ///
    /// Every path in an error renders through `render_path`: the dispatcher logs the error text at warn,
    /// and the sender names the final path, so a raw newline or ESC may not ride the line.
    fn land(mut self, landing: Landing, blob: &Blob) -> Result<PathBuf, ReceiveError> {
        let dir = landing.walk(self.out.as_fd())?;
        let to = dir.as_ref().map_or(self.out.as_fd(), |dir| dir.as_fd());
        match place(self.out.as_fd(), self.name.as_str(), to, landing.name()) {
            Ok(()) => {
                self.pending = false;
                Ok(landing.relative)
            }
            Err(Errno::EXIST) if holds(to, landing.name(), blob) => Ok(landing.relative),
            Err(Errno::EXIST) => Err(ReceiveError::Exists {
                path: landing.render(),
            }),
            Err(errno) => Err(ReceiveError::Save {
                path: landing.render(),
                source: errno.into(),
            }),
        }
    }
}

impl Drop for Temp {
    fn drop(&mut self) {
        if self.pending {
            let _ = rustix::fs::unlinkat(&self.out, self.name.as_str(), AtFlags::empty());
        }
    }
}

/// Whether `name` under `dir` is a regular file holding exactly `blob`. A symlink is not followed, and the
/// open does not block, so a FIFO planted at the name cannot stall the check; anything but a regular file,
/// and any failure to read it, is a no.
fn holds(dir: BorrowedFd<'_>, name: &OsStr, blob: &Blob) -> bool {
    let Ok(held) = rustix::fs::openat(
        dir,
        name,
        OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
        Mode::empty(),
    ) else {
        return false;
    };
    let Ok(stat) = rustix::fs::fstat(&held) else {
        return false;
    };
    if !FileType::from_raw_mode(stat.st_mode).is_file() {
        return false;
    }
    let len = u64::try_from(stat.st_size).unwrap_or(u64::MAX);
    blob.describes(len, std::fs::File::from(held))
        .unwrap_or(false)
}

/// Move `temp` under `from` to `name` under `to`, refusing an existing name with `EEXIST`: a push may only
/// add a file, never rewrite one the receiver already holds. A symlink at `name` is an existing name too,
/// so it is refused rather than replaced or followed.
///
/// Linux and macOS have an exclusive rename, one atomic step. A filesystem that refuses its flag (exFAT on
/// macOS, and NFS by the Linux man page) gets [`reserve_then_rename`] instead.
fn place(
    from: BorrowedFd<'_>,
    temp: &str,
    to: BorrowedFd<'_>,
    name: &OsStr,
) -> rustix::io::Result<()> {
    #[cfg(any(target_os = "linux", target_vendor = "apple"))]
    match rustix::fs::renameat_with(from, temp, to, name, rustix::fs::RenameFlags::NOREPLACE) {
        Err(Errno::NOTSUP | Errno::INVAL | Errno::NOSYS) => {}
        placed => return placed,
    }
    reserve_then_rename(from, temp, to, name)
}

/// The landing where the filesystem has no exclusive rename: reserve `name` with an exclusive create, which
/// refuses an existing file and a symlink, then rename the temp over the empty reservation this landing just
/// made. If the rename fails, the reservation is removed.
fn reserve_then_rename(
    from: BorrowedFd<'_>,
    temp: &str,
    to: BorrowedFd<'_>,
    name: &OsStr,
) -> rustix::io::Result<()> {
    drop(rustix::fs::openat(
        to,
        name,
        OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::CLOEXEC,
        FILE_MODE,
    )?);
    rustix::fs::renameat(from, temp, to, name).inspect_err(|_| {
        let _ = rustix::fs::unlinkat(to, name, AtFlags::empty());
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

    fn as_str(&self) -> &str {
        &self.0
    }

    /// Whether `name` has the temp pattern's shape, whatever its middle and in any ASCII case: on a
    /// case-insensitive filesystem an upper-case spelling names the same file.
    fn matches(name: &OsStr) -> bool {
        let name = name.as_encoded_bytes();
        let (prefix, suffix) = (Self::PREFIX.as_bytes(), Self::SUFFIX.as_bytes());
        name.len() >= prefix.len() + suffix.len()
            && name[..prefix.len()].eq_ignore_ascii_case(prefix)
            && name[name.len() - suffix.len()..].eq_ignore_ascii_case(suffix)
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
    Transfer(#[from] crate::wire::Error),
    /// The verified bytes could not be flushed to the temp file.
    #[error("flush {path}: {source}")]
    Flush {
        /// The temp path, rendered for a log line.
        path: String,
        /// The filesystem failure.
        #[source]
        source: io::Error,
    },
    /// A directory on the way to the destination could not be created.
    #[error("create the directory {path}: {source}")]
    CreateDir {
        /// The directory, rendered for a log line.
        path: String,
        /// The filesystem failure.
        #[source]
        source: io::Error,
    },
    /// The output directory, or a directory on the way to the destination, could not be opened.
    #[error("open the directory {path}: {source}")]
    OpenDir {
        /// The directory, rendered for a log line.
        path: String,
        /// The filesystem failure.
        #[source]
        source: io::Error,
    },
    /// The sender named a path through a symlink inside the output directory. A symlink could lead out of
    /// it, so none is followed, and nothing is written past it.
    #[error("{path} passes through a symlink")]
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
    /// The verified temp file could not be moved to the destination the sender named.
    #[error("save to {path}: {source}")]
    Save {
        /// The destination, rendered for a log line (the sender chose it).
        path: String,
        /// The filesystem failure.
        #[source]
        source: io::Error,
    },
}

/// One received file: the safe relative path it was saved at under the output directory, its byte length,
/// and the key of the peer that sent it. The fact a [`ReceivedSink`](crate::ReceivedSink) is handed, so the
/// caller can report what landed and who sent it. The path is raw and peer-named: safe to join under the
/// output directory, never safe to print unescaped.
#[derive(Debug, Clone)]
pub struct Received {
    /// The path the file was saved at, relative to the output directory.
    pub path: PathBuf,
    /// The length of the received bytes.
    pub bytes: u64,
    /// The sender's key, as the gate admitted it: the transport proved the peer holds it. This, not the
    /// root, is what says who the bytes came from, since a sender names the root of whatever it sends.
    pub from: VerifyKey,
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
