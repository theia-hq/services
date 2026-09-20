//! The HTTP preamble (`TBH1`): the request/response framing for the origin-fetch egress service.
//!
//! It rides INSIDE an admitted stream: after the host admits the requester, the requester writes a
//! [`FetchRequest`], the host performs the origin request and writes a [`FetchResponse`], then streams the
//! body until the stream closes (EOF delimits the body, as the raw splice already relies on). Pure framing
//! here; the origin fetch lives in the crate-private `serve` body, behind the [`Fetch`](crate::Fetch) and
//! [`ScopedFetch`](crate::ScopedFetch) entries. Both ends of one fetch are meant to be one release, and
//! when they are not the host says so on the wire rather than closing the stream on a peer that cannot
//! see why.

use tokio::io::{self, AsyncReadExt as _, AsyncWriteExt as _};

/// The fetch wire's identity: the bytes every request frame opens with, at every version, forever. A
/// stream that does not open with these is not a fetch stream, and that is the only thing an identity
/// mismatch is allowed to mean.
const IDENTITY: [u8; 3] = *b"TBH";

/// The request grammar THIS build speaks, written after [`IDENTITY`] and parsed (never compared whole)
/// on read: together they are the four magic bytes `TBH1`.
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

/// The four bytes every RESPONSE frame opens with. FROZEN: they are these bytes at every version of the
/// request frame, and they are deliberately a literal rather than [`IDENTITY`] plus [`VERSION`].
///
/// A host answers a peer whose request it could not parse ([`RequestReadError::answer`]), and an answer
/// is only worth writing if a build that predates it can read it. A response tag derived from the
/// request grammar would change under the next version bump and make every such answer unreadable by
/// exactly the peer it was written for, which is the bare EOF this wire is getting away from. So the
/// request preamble MAY break with a version bump, and this tag may NOT, ever.
const RESPONSE_TAG: [u8; 4] = *b"TBH1";

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

/// The version half of a request frame: the byte after [`IDENTITY`], naming which request grammar the
/// peer that wrote it speaks.
///
/// Parsed as a value rather than folded into one four-byte comparison, because the two halves of the
/// magic answer different questions. An IDENTITY mismatch says the stream is not ours, and there is
/// nothing true we could say to whatever is on the other end. A VERSION mismatch says a fetch peer on
/// another build, which is a fact both ends can act on, so it is answered on the wire
/// ([`RequestReadError::answer`]) instead of dropped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WireVersion([u8; 1]);

impl WireVersion {
    /// Read the version half, after the identity. The width of the field lives here, in the type that
    /// owns it, so the reader and the writer cannot drift apart.
    async fn read<R: io::AsyncRead + Unpin>(reader: &mut R) -> io::Result<Self> {
        let mut bytes = [0u8; 1];
        reader.read_exact(&mut bytes).await?;
        Ok(Self(bytes))
    }

    /// The bytes as they go on the wire.
    const fn as_bytes(&self) -> &[u8; 1] {
        &self.0
    }
}

impl core::fmt::Display for WireVersion {
    /// Renders the WHOLE four-byte tag (`TBH1`), because that is the form the source and the changelog
    /// use, so a dialer handed one in a refusal can match it against what it reads. A peer's version
    /// byte is arbitrary and need not be printable, so it is escaped rather than trusted: this string
    /// reaches a terminal.
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{}{}", IDENTITY.escape_ascii(), self.0.escape_ascii())
    }
}

/// The most headers a fetch frame may carry. The wire count is a `u16` (up to 65 535) and each string is
/// up to 64 KiB, so an uncapped frame lets one post-gate request pin gigabytes; a real GET/HEAD needs a
/// few dozen. Bounds the allocation a single admitted stream can force. Applies to both the request and the
/// forwarded response headers, so it sits well above any real HTTP message (the largest top out ~100).
pub(crate) const MAX_HEADERS: usize = 128;

/// A requester's fetch: the method, the absolute origin URL, and the headers to forward verbatim
/// (including `Range`, which is the whole point: the origin returns `206` and a resumable download works).
#[derive(Debug, PartialEq, Eq)]
pub struct FetchRequest {
    /// The HTTP method. v1 accepts only `GET` and `HEAD`; the field is here for forward-compat.
    pub method: String,
    /// The absolute origin URL to fetch.
    pub url: String,
    /// Request headers, forwarded verbatim (hop-by-hop headers are stripped by the caller).
    pub headers: Vec<(String, String)>,
}

/// The host's reply, sent before the body: the origin status and headers forwarded verbatim, or an
/// error (origin unreachable, method refused, policy denied) as a human-readable string.
///
/// **This frame is FROZEN**, from its [`RESPONSE_TAG`] through the meaning of every tag byte and field
/// behind it. A host answers a peer whose request it could not parse ([`RequestReadError::answer`]),
/// and an answer is only worth writing if a build that predates it can read it. So the request frame
/// MAY break with a version bump, since it carries the evolving vocabulary, and this one may NOT.
/// Growth here is additive only, and an ADDED tag is legible only to a peer that already knows it,
/// because this reader rejects an unknown tag outright rather than reading it as something it cannot
/// name: a new tag may therefore serve new conditions and may never carry the version answer, which
/// has to ride the [`Error`](Self::Error) frame every shipped build already decodes.
#[derive(Debug, PartialEq, Eq)]
pub enum FetchResponse {
    /// The origin answered; its status and headers follow, then the body streams to EOF.
    Ok {
        /// The origin HTTP status, forwarded verbatim (200, 206, 301, 404, ...).
        status: u16,
        /// The origin response headers, forwarded verbatim (`Accept-Ranges`, `Content-Range`, ...).
        headers: Vec<(String, String)>,
    },
    /// The fetch could not be performed, with a reason.
    Error(String),
}

impl FetchRequest {
    /// Write the request frame.
    pub async fn write<W: io::AsyncWrite + Unpin>(&self, writer: &mut W) -> io::Result<()> {
        writer.write_all(&IDENTITY).await?;
        writer.write_all(VERSION.as_bytes()).await?;
        write_str(writer, &self.method).await?;
        write_str(writer, &self.url).await?;
        write_headers(writer, &self.headers).await
    }

    /// Read a request frame.
    ///
    /// The magic is parsed as [`IDENTITY`] plus a [`WireVersion`], never compared as four bytes, so
    /// that "not our protocol" and "our protocol, another build" stay two facts instead of one. Only
    /// the second is something the peer can act on, and [`RequestReadError::answer`] is where it gets
    /// answered rather than logged at the wrong end.
    pub async fn read<R: io::AsyncRead + Unpin>(reader: &mut R) -> Result<Self, RequestReadError> {
        let mut identity = [0u8; IDENTITY.len()];
        reader.read_exact(&mut identity).await?;
        if identity != IDENTITY {
            return Err(RequestReadError::Foreign);
        }
        let version = WireVersion::read(reader).await?;
        if version != VERSION {
            return Err(RequestReadError::Version { peer: version });
        }
        Ok(Self {
            method: read_str(reader).await?,
            url: read_str(reader).await?,
            headers: read_headers(reader).await?,
        })
    }
}

/// Why a request frame could not be read.
///
/// Three failures, kept apart on purpose: only one of them is a fact the peer can act on, and only that
/// one is answered on the wire (see [`answer`](Self::answer)). Collapsing them is how a version-skewed
/// requester used to get a bare EOF while the host logged a sentence nobody read.
#[derive(Debug, thiserror::Error)]
pub enum RequestReadError {
    /// The stream did not open with [`IDENTITY`], so it is not a fetch stream. The wording is now
    /// exactly true: it used to cover a fetch peer on another version as well, which it never was.
    #[error("not a fetch stream")]
    Foreign,
    /// A fetch stream from a build that speaks a different request grammar.
    ///
    /// This message goes ON THE WIRE via [`answer`](Self::answer), so it is FIXED text plus the two
    /// version tags and nothing else. Never interpolate host state here: the only host fact it may
    /// carry is this build's own wire version, which any peer learns by being served at all.
    #[error(
        "fetch wire version mismatch: the request is {peer}, this host speaks {VERSION}; run the \
         same release at both ends"
    )]
    Version {
        /// The version the peer's frame named.
        peer: WireVersion,
    },
    /// The frame could not be read.
    #[error(transparent)]
    Io(#[from] io::Error),
}

impl RequestReadError {
    /// The frame to write back, for the one unreadable request a peer can act on.
    ///
    /// A version mismatch is answerable because the identity already proved the peer speaks fetch:
    /// naming both versions tells them what happened and what to do about it. A foreign identity gets
    /// nothing, since we cannot know what would even be meaningful to whatever is on the other end,
    /// and an i/o failure has no readable stream left to answer into.
    ///
    /// This wire rides INSIDE a stream the host has already admitted, so the pre-gate disclosure
    /// question does not arise: the peer is authorized, and the only host fact the answer carries is
    /// a wire version it would learn from any served request.
    #[must_use]
    pub fn answer(&self) -> Option<FetchResponse> {
        match self {
            // The text is this variant's own rendering, which is the whole reason that string is held
            // to fixed text plus the two version tags.
            Self::Version { .. } => Some(FetchResponse::Error(self.to_string())),
            Self::Foreign | Self::Io(_) => None,
        }
    }
}

impl From<RequestReadError> for io::Error {
    /// Cross back into an `io::Result` boundary. An i/o failure passes through as ITSELF, so the cause
    /// chain stays one deep and a caller still reads the real kind; a framing failure becomes an
    /// `Other` carrying this typed error as its source.
    fn from(error: RequestReadError) -> Self {
        match error {
            RequestReadError::Io(error) => error,
            framing => io::Error::other(framing),
        }
    }
}

impl FetchResponse {
    /// Write the response frame (the body, if any, is streamed by the caller after this).
    pub async fn write<W: io::AsyncWrite + Unpin>(&self, writer: &mut W) -> io::Result<()> {
        writer.write_all(&RESPONSE_TAG).await?;
        match self {
            Self::Ok { status, headers } => {
                writer.write_all(&[0]).await?;
                writer.write_all(&status.to_be_bytes()).await?;
                write_headers(writer, headers).await
            }
            Self::Error(message) => {
                writer.write_all(&[1]).await?;
                write_str(writer, message).await
            }
        }
    }

    /// Read a response frame. The tag is [`RESPONSE_TAG`], not this build's magic, so a reply from a
    /// host on another request grammar still parses: that is what makes the version answer reach the
    /// peer it is for.
    pub async fn read<R: io::AsyncRead + Unpin>(reader: &mut R) -> io::Result<Self> {
        read_response_tag(reader).await?;
        let mut tag = [0u8; 1];
        reader.read_exact(&mut tag).await?;
        match tag[0] {
            0 => {
                let mut status = [0u8; 2];
                reader.read_exact(&mut status).await?;
                Ok(Self::Ok {
                    status: u16::from_be_bytes(status),
                    headers: read_headers(reader).await?,
                })
            }
            1 => Ok(Self::Error(read_str(reader).await?)),
            other => Err(io::Error::other(format!(
                "unknown fetch response tag {other:#04x}"
            ))),
        }
    }
}

/// Read and check the frozen response tag. A response is only ever read on a stream we opened, so a tag
/// mismatch here is a foreign or corrupt stream and there is no version to distinguish: the tag is the
/// same four bytes at every version by construction ([`RESPONSE_TAG`]).
async fn read_response_tag<R: io::AsyncRead + Unpin>(reader: &mut R) -> io::Result<()> {
    let mut tag = [0u8; RESPONSE_TAG.len()];
    reader.read_exact(&mut tag).await?;
    if tag != RESPONSE_TAG {
        return Err(io::Error::other("not a fetch stream"));
    }
    Ok(())
}

/// Write a header list as a `u16` count followed by that many `(name, value)` string pairs.
async fn write_headers<W: io::AsyncWrite + Unpin>(
    writer: &mut W,
    headers: &[(String, String)],
) -> io::Result<()> {
    let count = u16::try_from(headers.len()).map_err(|_| io::Error::other("too many headers"))?;
    writer.write_all(&count.to_be_bytes()).await?;
    for (name, value) in headers {
        write_str(writer, name).await?;
        write_str(writer, value).await?;
    }
    Ok(())
}

/// Read a header list written by [`write_headers`].
async fn read_headers<R: io::AsyncRead + Unpin>(
    reader: &mut R,
) -> io::Result<Vec<(String, String)>> {
    let mut count = [0u8; 2];
    reader.read_exact(&mut count).await?;
    let count = u16::from_be_bytes(count) as usize;
    // Reject an over-large count before allocating, so a hostile frame cannot pin memory or make the node
    // read gigabytes of header strings off an admitted stream.
    if count > MAX_HEADERS {
        return Err(io::Error::other(format!(
            "fetch frame declares {count} headers (max {MAX_HEADERS})"
        )));
    }
    let mut headers = Vec::with_capacity(count);
    for _ in 0..count {
        let name = read_str(reader).await?;
        let value = read_str(reader).await?;
        headers.push((name, value));
    }
    Ok(headers)
}

/// Write a string as a `u16` byte-length prefix followed by its UTF-8 bytes.
async fn write_str<W: io::AsyncWrite + Unpin>(writer: &mut W, value: &str) -> io::Result<()> {
    let bytes = value.as_bytes();
    let len = u16::try_from(bytes.len()).map_err(|_| io::Error::other("string too long"))?;
    writer.write_all(&len.to_be_bytes()).await?;
    writer.write_all(bytes).await
}

/// Read a length-prefixed string written by [`write_str`].
async fn read_str<R: io::AsyncRead + Unpin>(reader: &mut R) -> io::Result<String> {
    let mut len = [0u8; 2];
    reader.read_exact(&mut len).await?;
    let mut bytes = vec![0u8; u16::from_be_bytes(len) as usize];
    reader.read_exact(&mut bytes).await?;
    String::from_utf8(bytes).map_err(|_| io::Error::other("invalid utf-8 in string"))
}
