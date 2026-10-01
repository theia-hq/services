//! The transfer wire: verified blob transfer over any byte stream.
//!
//! Pure bytes. A [`Transfer`] owns one bidirectional stream pair and is consumed to move a single
//! [`Blob`] across it, proving the bytes against their BLAKE3 root so a peer cannot lie about content
//! and a truncated transfer is rejected. It knows nothing about files, paths, filenames, iroh, QUIC,
//! or sockets: sources and sinks are any [`AsyncRead`]/[`AsyncWrite`] the caller supplies, and an
//! opaque `header` carries whatever application metadata the caller wants (a filename, a content
//! type), transmitted verbatim and never interpreted here.
//!
//! [`AsyncRead`]: tokio::io::AsyncRead
//! [`AsyncWrite`]: tokio::io::AsyncWrite

use core::time::Duration;

use tokio::io;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

/// The largest app header this wire carries, in bytes, enforced at both ends.
///
/// A header names one blob, so the largest legitimate one is a name: POSIX bounds a whole filesystem
/// path at 4 KiB (`PATH_MAX`), and 64 KiB leaves sixteen times that for a header that carries more than
/// a name (a content type, a small manifest beside it). It is also the size of the streaming buffer the
/// receive path already holds, so the header is never the largest allocation in a transfer.
///
/// The bound exists because the length prefix is a `u32`: without it a receiver allocates whatever the
/// peer declares, up to 4 GiB, before reading one byte of the header it is sizing for. The cap is
/// checked against the prefix, so an oversized claim costs the receiver nothing.
const MAX_HEADER_LEN: u32 = 64 * 1024;

/// This wire's identity: the bytes every frame opens with, at every version, forever. A stream that
/// does not open with these is not a stream of this wire, and that is the only thing an identity
/// mismatch is allowed to mean.
const IDENTITY: [u8; 3] = *b"BFW";

/// The frame grammar THIS build speaks, written after [`IDENTITY`] and parsed (never compared whole)
/// on read: together they are the four magic bytes `BFW1`. Bumped when the header layout changes.
const VERSION: WireVersion = WireVersion(*b"1");

/// The magic splits by RULE, not by a remembered offset: the identity is the leading run of capitals,
/// the version is the digits after it, four bytes in all. Held at build time so a magic that breaks the
/// rule fails to compile rather than splitting somewhere the next reader would not look. A digit is
/// never a capital, so "all capitals, then all digits" is exactly "the maximal leading capital run".
const _: () = assert!(
    all_between(&IDENTITY, b'A', b'Z')
        && all_between(VERSION.as_bytes(), b'0', b'9')
        && IDENTITY.len() + VERSION.as_bytes().len() == 4,
    "the magic must be four bytes: a run of capitals (the identity) then digits (the version)"
);

/// Whether `bytes` is non-empty and every byte falls in `lo..=hi`. `const` because its one caller is a
/// build-time claim about the magic.
const fn all_between(bytes: &[u8], lo: u8, hi: u8) -> bool {
    let mut at = 0;
    while at < bytes.len() {
        if bytes[at] < lo || bytes[at] > hi {
            return false;
        }
        at += 1;
    }
    !bytes.is_empty()
}

/// The version half of the frame magic: the byte after [`IDENTITY`], naming which frame grammar the
/// peer that wrote it speaks.
///
/// Parsed as a value rather than folded into one four-byte comparison, because the two halves of the
/// magic answer different questions. An IDENTITY mismatch says the stream is not ours. A VERSION
/// mismatch says a peer of this wire on another build, which is a different fact and reaches a
/// different reader: see [`Error::VersionMismatch`] for why this wire tells its own side rather than
/// the peer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct WireVersion([u8; 1]);

impl WireVersion {
    /// Read the version half, after the identity. The width of the field lives here, in the type that
    /// owns it, so the reader and the writer cannot drift apart.
    async fn read<R>(reader: &mut R) -> Result<Self>
    where
        R: io::AsyncRead + Unpin,
    {
        let mut bytes = [0u8; 1];
        reader
            .read_exact(&mut bytes)
            .await
            .map_err(|_| Error::Truncated)?;
        Ok(Self(bytes))
    }

    /// The bytes as they go on the wire.
    const fn as_bytes(&self) -> &[u8; 1] {
        &self.0
    }
}

impl core::fmt::Display for WireVersion {
    /// Renders the WHOLE four-byte tag (`BFW1`), because that is the form the source and the changelog
    /// use, so an operator holding one from a log line can match it against what they read. A peer's
    /// version byte is arbitrary and need not be printable, so it is escaped rather than trusted: this
    /// string reaches a terminal.
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{}{}", IDENTITY.escape_ascii(), self.0.escape_ascii())
    }
}
/// The receiver verified the blob and finished with it (a receiver that saves it has saved it).
const ACK_OK: u8 = 1;
/// The receiver refused the blob at some step: it did not verify, or the receiver would not keep it.
const ACK_ERR: u8 = 0;
/// Streaming buffer size.
const CHUNK: usize = 64 * 1024;

/// How long a sender waits for the receiver's answer once its whole frame is sent.
///
/// The receiver answers only after it is done with the blob (a receiver that saves it saves it first), and
/// it may still be draining the body the transport buffered when the sender finished, so the wait is
/// generous. It is bounded because a receiver that takes the body and never answers would otherwise hold
/// the sender, and every stream it has open, forever.
const ACK_DEADLINE: Duration = Duration::from_secs(60);

/// A content-addressed blob descriptor: its BLAKE3 root and length. The root names the bytes, so
/// anyone can verify what they received against it and the source cannot lie about content.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Blob {
    root: [u8; 32],
    len: u64,
}

impl Blob {
    /// Hash a byte source into a descriptor, in one streaming pass.
    pub async fn hash<R>(source: &mut R) -> io::Result<Self>
    where
        R: io::AsyncRead + Unpin,
    {
        let mut hasher = blake3::Hasher::new();
        let mut buf = vec![0u8; CHUNK];
        let mut len = 0u64;
        loop {
            let read = source.read(&mut buf).await?;
            if read == 0 {
                break;
            }
            hasher.update(&buf[..read]);
            len += read as u64;
        }
        Ok(Self {
            root: *hasher.finalize().as_bytes(),
            len,
        })
    }

    /// The blob length in bytes.
    pub fn len(&self) -> u64 {
        self.len
    }

    /// Whether the blob is empty.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Whether `bytes`, `len` bytes long, are exactly this blob. The length is compared first, so a
    /// short claim never makes the caller hash a large file; a reader that runs past `len` is cut there.
    pub(crate) fn describes(&self, len: u64, bytes: impl std::io::Read) -> std::io::Result<bool> {
        if len != self.len {
            return Ok(false);
        }
        let mut hasher = blake3::Hasher::new();
        let read = std::io::copy(&mut bytes.take(len), &mut hasher)?;
        Ok(read == len && hasher.finalize().as_bytes() == &self.root)
    }
}

/// A verified blob transfer over one bidirectional byte-stream pair.
///
/// Owns the stream halves and is consumed by [`Transfer::send`], or on the receiving side by this crate's
/// own receiver: one transfer, one blob, one pair.
pub struct Transfer<W, R> {
    writer: W,
    reader: R,
}

impl<W, R> Transfer<W, R>
where
    W: io::AsyncWrite + Unpin,
    R: io::AsyncRead + Unpin,
{
    /// Wrap a session's stream halves.
    pub fn new(writer: W, reader: R) -> Self {
        Self { writer, reader }
    }

    /// Send `blob`, streaming its bytes from `source`, with an opaque `header`; await the peer's ack.
    ///
    /// `blob` must describe `source` (produce it with [`Blob::hash`]); the receiver checks every byte
    /// against `blob`'s root. Exactly `blob`'s length is sent: a source that ends early or runs past it
    /// is [`Error::LengthMismatch`], and the receiver never gets a whole blob from it. An answer that
    /// does not come within a minute of the frame's end is [`Error::AckTimeout`].
    pub async fn send<Src>(mut self, header: &[u8], blob: &Blob, source: &mut Src) -> Result<()>
    where
        Src: io::AsyncRead + Unpin,
    {
        // Both ends hold the same bound, so an over-cap header fails here, named by the caller's own
        // error, rather than as a remote refusal partway through a frame the receiver was always going
        // to reject.
        let header_len = u32::try_from(header.len()).map_err(|_| Error::HeaderTooLong)?;
        if header_len > MAX_HEADER_LEN {
            return Err(Error::HeaderTooLong);
        }
        // An empty blob has no last chunk to hold back (see `copy_body`), so its source is proved empty
        // before any byte of the frame is written.
        if blob.len == 0 {
            source_ends(source).await?;
        }
        self.writer.write_all(&IDENTITY).await?;
        self.writer.write_all(VERSION.as_bytes()).await?;
        self.writer.write_all(&header_len.to_be_bytes()).await?;
        self.writer.write_all(header).await?;
        self.writer.write_all(&blob.len.to_be_bytes()).await?;
        self.writer.write_all(&blob.root).await?;

        self.copy_body(source, blob.len).await?;
        // Finish the send half now: on QUIC this flushes every buffered byte and signals end of blob.
        // Waiting for the ack before finishing would deadlock (the receiver blocks on the last bytes
        // that finish is what delivers); the recv half stays open for the ack.
        self.writer.shutdown().await?;

        let mut ack = [0u8; 1];
        tokio::time::timeout(ACK_DEADLINE, self.reader.read_exact(&mut ack))
            .await
            .map_err(|_| Error::AckTimeout)?
            .map_err(|_| Error::Truncated)?;
        if ack[0] != ACK_OK {
            return Err(Error::Rejected);
        }
        Ok(())
    }

    /// Copy exactly `len` bytes of `source` to the peer, refusing a source of any other length.
    ///
    /// The last chunk is held back until the source proves it ends there. Once the peer holds `len` bytes
    /// and the stream ends, it holds a whole blob, and a sender cannot take that back by returning an
    /// error: a transport may finish a send half that is dropped (QUIC does). So a source that runs on is
    /// refused while the peer is still short of the declared length, which the peer reads as truncated.
    async fn copy_body<Src>(&mut self, source: &mut Src, len: u64) -> Result<()>
    where
        Src: io::AsyncRead + Unpin,
    {
        let mut buf = vec![0u8; CHUNK];
        let mut remaining = len;
        while remaining > 0 {
            let want = remaining.min(buf.len() as u64) as usize;
            let read = source.read(&mut buf[..want]).await?;
            if read == 0 {
                return Err(Error::LengthMismatch);
            }
            remaining -= read as u64;
            if remaining == 0 {
                source_ends(source).await?;
            }
            self.writer.write_all(&buf[..read]).await?;
        }
        Ok(())
    }

    /// Read the head of a frame (its magic, header, length and root) and hand back the [`Incoming`] blob,
    /// whose body is still on the stream and whose sender is still waiting for an answer.
    ///
    /// The magic is parsed as [`IDENTITY`] plus a [`WireVersion`], never compared as four bytes, so
    /// that "not our protocol" and "our protocol, another build" stay two facts instead of one. Both
    /// end the transfer here and neither is written back: this wire is write-then-read, so the peer
    /// is still streaming its body and is not listening. The distinction is for THIS side's log, and
    /// that is the whole of what it buys ([`Error::VersionMismatch`]).
    pub(crate) async fn recv(mut self) -> Result<Incoming<W, R>> {
        let mut identity = [0u8; IDENTITY.len()];
        self.reader
            .read_exact(&mut identity)
            .await
            .map_err(|_| Error::Truncated)?;
        if identity != IDENTITY {
            return Err(Error::Foreign);
        }
        let version = WireVersion::read(&mut self.reader).await?;
        if version != VERSION {
            return Err(Error::VersionMismatch {
                peer: version.to_string(),
            });
        }

        let header = self.read_framed().await?;
        let len = self.read_u64().await?;
        let mut root = [0u8; 32];
        self.reader
            .read_exact(&mut root)
            .await
            .map_err(|_| Error::Truncated)?;
        Ok(Incoming {
            transfer: self,
            header,
            blob: Blob { root, len },
        })
    }

    /// Read the one length-prefixed field the layout has, the app header, bounded by
    /// [`MAX_HEADER_LEN`].
    ///
    /// The cap is checked against the PREFIX, before the buffer exists: the peer's `u32` is a claim, and
    /// sizing a buffer to an unverified claim is what lets one frame commit 4 GiB of the host. An
    /// over-cap claim is [`Error::OversizedHeader`], never [`Error::Truncated`]: the frame is well
    /// formed and too big, which is a different answer than a stream that ended.
    async fn read_framed(&mut self) -> Result<Vec<u8>> {
        let mut len = [0u8; 4];
        self.reader
            .read_exact(&mut len)
            .await
            .map_err(|_| Error::Truncated)?;
        let len = u32::from_be_bytes(len);
        if len > MAX_HEADER_LEN {
            return Err(Error::OversizedHeader { len });
        }
        let mut bytes = vec![0u8; len as usize];
        self.reader
            .read_exact(&mut bytes)
            .await
            .map_err(|_| Error::Truncated)?;
        Ok(bytes)
    }

    async fn read_u64(&mut self) -> Result<u64> {
        let mut bytes = [0u8; 8];
        self.reader
            .read_exact(&mut bytes)
            .await
            .map_err(|_| Error::Truncated)?;
        Ok(u64::from_be_bytes(bytes))
    }
}

/// A blob whose frame head has been read and whose sender is waiting for one answer.
///
/// The answer is the receiver's last word on the whole push, so it is written by [`answer`](Self::answer)
/// alone, from the outcome the caller hands it: a receiver that saves the blob somewhere answers only
/// once the blob is saved, and a push it refuses at any step reads as a refusal at the sender. Dropping
/// this without answering closes the stream, which the sender also reads as a failure.
pub(crate) struct Incoming<W, R> {
    transfer: Transfer<W, R>,
    header: Vec<u8>,
    blob: Blob,
}

impl<W, R> Incoming<W, R>
where
    W: io::AsyncWrite + Unpin,
    R: io::AsyncRead + Unpin,
{
    /// The opaque, app-defined header the sender attached.
    pub(crate) fn header(&self) -> &[u8] {
        &self.header
    }

    /// The descriptor the sender declared. Its length is checked by [`verify_into`](Self::verify_into).
    pub(crate) fn blob(&self) -> &Blob {
        &self.blob
    }

    /// Stream exactly the declared length from the peer into `sink`, require the stream to end there, then
    /// check it against the root.
    ///
    /// The bytes reach `sink` before the root is checked, since the root covers the whole body, so a
    /// caller that keeps the sink somewhere treats it as unchecked until this returns `Ok`.
    pub(crate) async fn verify_into<Sink>(&mut self, sink: &mut Sink) -> Result<()>
    where
        Sink: io::AsyncWrite + Unpin,
    {
        let reader = &mut self.transfer.reader;
        let mut hasher = blake3::Hasher::new();
        let mut buf = vec![0u8; CHUNK];
        let mut remaining = self.blob.len;
        while remaining > 0 {
            let want = remaining.min(buf.len() as u64) as usize;
            let read = reader.read(&mut buf[..want]).await?;
            if read == 0 {
                return Err(Error::Truncated);
            }
            hasher.update(&buf[..read]);
            sink.write_all(&buf[..read]).await?;
            remaining -= read as u64;
        }
        // A frame ends with its body, so a byte past the declared length means the two ends disagree on
        // what was sent, and the blob is refused rather than kept from a stream that is not done.
        if reader.read(&mut [0u8; 1]).await? != 0 {
            return Err(Error::Overrun);
        }
        sink.flush().await?;
        if hasher.finalize().as_bytes() != &self.blob.root {
            return Err(Error::IntegrityFailed);
        }
        Ok(())
    }

    /// Answer the sender from `outcome`, then hand `outcome` back: [`ACK_OK`] for `Ok`, [`ACK_ERR`] for
    /// any `Err`, with nothing about why, so a refusal tells a pusher nothing about the receiver's disk.
    ///
    /// A failed write is not an error here. The outcome already happened: a blob that was saved stays
    /// saved, and the sender, whose stream broke, reports a failure of its own.
    pub(crate) async fn answer<T, E>(
        mut self,
        outcome: core::result::Result<T, E>,
    ) -> core::result::Result<T, E> {
        let ack = if outcome.is_ok() { ACK_OK } else { ACK_ERR };
        let writer = &mut self.transfer.writer;
        if writer.write_all(&[ack]).await.is_ok() {
            let _ = writer.shutdown().await;
        }
        outcome
    }
}

/// Why a blob transfer failed.
///
/// Non-exhaustive: hardening this wire adds classes, and [`OversizedHeader`](Self::OversizedHeader) is
/// one. Match the classes you act on and keep a catch-all for the rest, so a class a newer peer can
/// earn never inherits whichever arm happened to be written first.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// An underlying read or write failed.
    #[error("io")]
    Io(#[from] std::io::Error),
    /// The stream did not open with this wire's identity, so it is not a stream of this wire. The wording
    /// is exactly true and covers only that: a peer of this wire on another version is never this.
    #[error("not a transfer stream")]
    Foreign,
    /// A stream of this wire from a build that speaks a different frame grammar.
    ///
    /// **This never reaches the peer, and cannot.** The wire is write-then-read: a sender writes the
    /// whole frame and the whole body and shuts its send half down BEFORE its first read, so when the
    /// receiver finds this mismatch the sender is still inside its body copy with nothing listening.
    /// Writing a refusal back would land on a peer that is not reading, and making it read first
    /// means a round trip in front of every transfer, forever. So the answer this condition owes is
    /// paid to the RECEIVING side, in this message, which reaches the host log of the machine that
    /// can act on it. The peer sees the transport failure it was always going to see.
    ///
    /// Naming both versions is the point: "bad frame magic" told an operator the peer sent garbage
    /// when the peer was a peer of this wire one release away, and sent them hunting a broken network
    /// instead of cutting a release.
    #[error(
        "transfer wire version mismatch: the frame is {peer}, this build speaks {VERSION}; run the \
         same release at both ends"
    )]
    VersionMismatch {
        /// The four-byte tag the peer's frame opened with, escaped for a log line: the peer's version
        /// byte is arbitrary and need not be printable.
        peer: String,
    },
    /// The app header handed to [`Transfer::send`] was over the 64 KiB header cap both ends hold.
    #[error("header too long")]
    HeaderTooLong,
    /// The peer declared a header over the 64 KiB header cap. Refused from the length prefix alone, so no
    /// buffer is ever sized to the claim.
    #[error("peer declared a {len} byte header, over the {MAX_HEADER_LEN} byte cap")]
    OversizedHeader {
        /// The header length the peer declared, in bytes.
        len: u32,
    },
    /// The source produced a different number of bytes than the blob declared.
    #[error("source length did not match the blob length")]
    LengthMismatch,
    /// The peer's stream went on past the blob's declared length.
    #[error("the stream ran past the declared length")]
    Overrun,
    /// The received bytes did not match their hash.
    #[error("integrity check failed: content did not match its hash")]
    IntegrityFailed,
    /// The peer closed the stream before the transfer completed.
    #[error("transfer truncated")]
    Truncated,
    /// The peer rejected the transfer.
    #[error("peer rejected the transfer")]
    Rejected,
    /// The peer did not answer within a minute of the frame's end.
    #[error("the peer did not answer within {} seconds", ACK_DEADLINE.as_secs())]
    AckTimeout,
}

/// Whether `source` is at its end, refusing one that still has bytes as [`Error::LengthMismatch`].
async fn source_ends<Src>(source: &mut Src) -> Result<()>
where
    Src: io::AsyncRead + Unpin,
{
    if source.read(&mut [0u8; 1]).await? != 0 {
        return Err(Error::LengthMismatch);
    }
    Ok(())
}

type Result<T> = core::result::Result<T, Error>;

#[cfg(test)]
#[path = "wire_tests.rs"]
mod wire_tests;
