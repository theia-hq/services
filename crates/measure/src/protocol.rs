//! The measure stream protocol: a small, versioned frame that opens each diagnostic stream and selects
//! what the responder should do, before the measured bytes flow. A 4-byte magic guards every stream, then a
//! typed [`Request`], then a typed reply.
//!
//! Ping round-trips a whole frame (request then echoed reply). Speed sends the framed request, then a
//! counted byte stream flows in the chosen direction, then a framed reply reports the counted total.

use bifrost::{RefusalDetail, RefusalDetailError};
use tightbeam_handler::wire::{read_frame, write_frame};
use tokio::io::{self, AsyncReadExt as _, AsyncWriteExt as _};

mod opening;

pub use opening::{Opening, Unread};

/// measure's protocol identity: the bytes every request frame opens with, at every version, forever. A
/// stream that does not open with these is not a measure stream, and that is the only thing an identity
/// mismatch is allowed to mean.
const IDENTITY: [u8; 2] = *b"DG";

/// The request grammar THIS build speaks, written after [`IDENTITY`] and parsed (never compared whole)
/// on read: together they are the four magic bytes `DG02`.
const VERSION: WireVersion = WireVersion(*b"02");

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

/// The version half of a request frame: the bytes after [`IDENTITY`], naming which request grammar the
/// peer that wrote them speaks.
///
/// Parsed as a value rather than folded into one four-byte comparison, because the two halves of the
/// magic answer different questions. An IDENTITY mismatch says the stream is not ours, and there is
/// nothing true we could say to whatever is on the other end. A VERSION mismatch says a measure peer on
/// another build, which is a fact both ends can act on, so it is answered on the wire
/// ([`ProtocolError::answer`]) instead of dropped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WireVersion([u8; 2]);

impl WireVersion {
    /// The version half of a head, taken after the identity. The width of the field lives here, in the
    /// type that owns it, so the codec and the writer cannot drift apart.
    ///
    /// `None` for a capital where the version begins, because that is the identity RUN continuing into
    /// a longer name. An identity is the maximal leading run of capitals, so `DGX1` is identity `DGX`,
    /// a different wire, and not this one at version `X1`. Without this the reader answers a foreign
    /// protocol with this host's version, which is a fact it has no business handing out and a
    /// diagnosis the peer cannot use. The check lives in the type that owns the field, so a version
    /// cannot exist unless the run stopped before it.
    fn after_identity(bytes: [u8; 2]) -> Option<Self> {
        let [after_identity, ..] = bytes;
        if after_identity.is_ascii_uppercase() {
            return None;
        }
        Some(Self(bytes))
    }

    /// The bytes as they go on the wire.
    const fn as_bytes(&self) -> &[u8; 2] {
        &self.0
    }
}

impl core::fmt::Display for WireVersion {
    /// Renders the WHOLE four-byte tag (`DG02`), because that is the form the source and the changelog
    /// use, so a dialer handed one in a refusal can match it against what it reads. A peer's version
    /// bytes are arbitrary and need not be printable, so they are escaped rather than trusted: this
    /// string reaches a terminal.
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{}{}", IDENTITY.escape_ascii(), self.0.escape_ascii())
    }
}

/// What a client asks a responder to do on a freshly opened stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Request {
    /// Echo this frame back verbatim. `sent_unix_nanos` is an opaque client-chosen nonce the responder
    /// returns untouched; the client times the round trip locally with a monotonic clock and never
    /// trusts this stamp (two machines' clocks are not comparable).
    Ping {
        /// The client's sequence number for this probe.
        seq: u32,
        /// The client's send stamp, echoed back verbatim as a nonce.
        sent_unix_nanos: u64,
    },
    /// The client is about to send `limit_bytes` for the responder to drain and count (upload / sink).
    SpeedSink {
        /// How many payload bytes the client will send after this frame.
        limit_bytes: u64,
    },
    /// The responder should stream counted payload for the client to drain (download). `limit_bytes`
    /// is `Some(n)` for a byte-bounded run (send exactly `n`) or `None` for a time-bounded run, where
    /// the responder streams until the client stops reading and its wall-clock deadline is the sole
    /// terminator. Encoded with a [`UNBOUNDED`] sentinel so the wire stays a fixed-width `u64`.
    SpeedSource {
        /// How many payload bytes to send, or `None` to stream until the client closes the stream.
        limit_bytes: Option<u64>,
    },
    /// Full-duplex speed: both ends send and drain counted payload at once on this one stream, so it
    /// measures upload and download simultaneously and works over a single-stream transport (quirk).
    /// The responder mirrors the client: it drains the client's upload to EOF while sourcing its own
    /// download, `Some(n)` bytes for a byte bound or unbounded for a time bound, where the client's
    /// close of its read half ends the responder's source. Encoded with the [`UNBOUNDED`] sentinel like
    /// [`SpeedSource`](Self::SpeedSource), so the wire stays a fixed-width `u64`.
    SpeedBidir {
        /// How many payload bytes to move each direction, or `None` to run until the client stops.
        limit_bytes: Option<u64>,
    },
}

/// The wire value of an unbounded [`Request::SpeedSource`]. `u64::MAX` bytes is unreachable in any real
/// transfer, so it reads unambiguously as "stream until the client stops" rather than a byte count. A
/// time-bounded [`Request::SpeedSink`] writes the same value as its byte ceiling (`Limit::byte_ceiling`),
/// so a responder reads it there as "the client named no exact count", never as an over-cap ask.
pub(crate) const UNBOUNDED: u64 = u64::MAX;

/// Wire tags for the [`Request`] variants, kept next to the frame they select.
mod tag {
    pub const PING: u8 = 0;
    pub const SPEED_SINK: u8 = 1;
    pub const SPEED_SOURCE: u8 = 2;
    pub const SPEED_BIDIR: u8 = 3;
}

/// Wire tags for the [`Response`] variants. A response has its own tag namespace, independent of
/// [`tag`], so a new reply variant never has to dodge a request tag to stay legible.
mod resp_tag {
    pub const PONG: u8 = 0;
    pub const RECEIVED: u8 = 1;
    pub const SOURCING: u8 = 2;
    pub const UNSUPPORTED: u8 = 3;
}

/// A refused diagnostic run. One type for both layers, so a render site has exactly one refusal arm;
/// the layer is the variant, never a string prefix.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Refusal {
    /// Layer 1: the host's gate or its resource refused the stream itself, arriving as
    /// [`bifrost::Error::Refused`].
    #[error("{0}")]
    Stream(bifrost::Refusal),
    /// Layer 2: the host admitted the stream, then refused the method.
    #[error("{code}: {detail}")]
    Method {
        /// The typed method-level code.
        code: MethodRefusal,
        /// The responder's bounded detail.
        detail: RefusalDetail,
    },
}

/// The method-level refusal code: the client's branch key, distinct from the prose. A new code forces a
/// render decision at every match site.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum MethodRefusal {
    /// The service does not serve the requested method (a ping frame on `speed`).
    #[error("this service does not serve that method")]
    WrongMethod,
    /// The service serves the method but hit a responder-side bound on this run: per-caller rate
    /// limiting, the ping stream's byte ceiling or lifetime cap, or a speed request that asks for
    /// more bytes than the route's cap allows. The bounded detail names which; the code is the
    /// coarse branch (stop the run, report the refusal).
    #[error("rate limited")]
    RateLimited,
    /// The service serves the method but is busy right now (a transfer slot).
    #[error("busy")]
    Busy,
}

/// Wire tags for the [`MethodRefusal`] codes, beside the frame they select. A new code forces a tag
/// here and an arm in the reader.
mod refusal_tag {
    pub const WRONG_METHOD: u8 = 0;
    pub const RATE_LIMITED: u8 = 1;
    pub const BUSY: u8 = 2;
}

impl MethodRefusal {
    /// The wire tag for this code.
    fn tag(self) -> u8 {
        match self {
            Self::WrongMethod => refusal_tag::WRONG_METHOD,
            Self::RateLimited => refusal_tag::RATE_LIMITED,
            Self::Busy => refusal_tag::BUSY,
        }
    }

    /// Decode a wire tag, rejecting an unrecognized code rather than guessing.
    fn from_tag(tag: u8) -> Result<Self, ProtocolError> {
        match tag {
            refusal_tag::WRONG_METHOD => Ok(Self::WrongMethod),
            refusal_tag::RATE_LIMITED => Ok(Self::RateLimited),
            refusal_tag::BUSY => Ok(Self::Busy),
            other => Err(ProtocolError::UnknownRefusalCode(other)),
        }
    }
}

impl Request {
    /// The wire size of a [`Ping`](Self::Ping) request: identity, version, tag, sequence, and nonce. A
    /// ping frame is fixed-width, so a stream's counted bytes and its frame count bound the same run;
    /// the byte ceiling on a ping stream charges this per probe.
    pub(crate) const PING_BYTES: u64 =
        (IDENTITY.len() + VERSION.as_bytes().len() + 1 + 4 + 8) as u64;

    /// Write the framed request in ONE call: identity, version, tag, then the variant's fields.
    ///
    /// Byte for byte what the field-by-field writer emitted, and four calls became one. Every call is
    /// an allocation and a copy, and on a transport that seals per write it is a whole frame of
    /// overhead: a 17-byte ping frame paid 92 bytes of framing across four writes and pays 23 across
    /// one, on the latency path of the family's own diagnostic.
    pub async fn write<W: io::AsyncWrite + Unpin>(&self, writer: &mut W) -> io::Result<()> {
        write_frame(writer, &Opening::Spoken(*self))
            .await
            .map_err(opening::io_error)
    }

    /// Read a framed request, failing with whatever the opening turned out to be instead.
    ///
    /// The magic is parsed as [`IDENTITY`] plus a [`WireVersion`], never compared as four bytes, so
    /// that "not our protocol" and "our protocol, another build" stay two facts instead of one. Only
    /// the second is something the peer can act on, and [`ProtocolError::answer`] is where it gets
    /// answered rather than logged at the wrong end.
    ///
    /// This is the door for a caller that wants the typed error it always got: the client's own
    /// reader, and a responder that reads its own opening. A responder on the typed door takes an
    /// [`Opening`] instead, because an unreadable head is a frame it may still have something true to
    /// write back to.
    pub async fn read<R: io::AsyncRead + Unpin>(reader: &mut R) -> Result<Self, ProtocolError> {
        match read_frame::<Opening, _>(reader).await {
            Ok(Opening::Spoken(request)) => Ok(request),
            Ok(Opening::Unread(unread)) => Err(unread.cause()),
            Err(error) => Err(ProtocolError::from(error)),
        }
    }
}

/// A responder's typed reply, sent before (source, sourcing-ack) or after (ping, sink) the payload it
/// describes. Not `Copy`: [`Unsupported`](Self::Unsupported) carries an owned detail.
///
/// **This frame is FROZEN.** It carries no identity and no version of its own, and every tag, refusal
/// code, and field in it means the same thing at every version of the REQUEST frame. That is not
/// tidiness: a host answers a peer whose request it could not parse ([`ProtocolError::answer`]), and an
/// answer is only worth writing if a build that predates it can read it. Versioning this frame, or
/// changing what a shipped tag means, would take the answer away and put every future wire break back
/// to the bare EOF it used to be.
///
/// So: two stability classes on one wire. The request frame MAY break with a version bump, since it
/// carries the evolving vocabulary. This one may NOT. Growth here is additive only, and an ADDED tag or
/// refusal code is legible only to a peer that already knows it, because this reader rejects an unknown
/// one outright ([`ProtocolError::UnknownResponse`], [`ProtocolError::UnknownRefusalCode`]) rather than
/// reading it as a class it cannot name. A new code may therefore serve new conditions and may never
/// carry the version answer, which has to ride a code every shipped build already decodes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Response {
    /// The echoed ping, carrying the request's `seq` and nonce untouched.
    Pong {
        /// The sequence number from the matching [`Request::Ping`].
        seq: u32,
        /// The nonce from the matching [`Request::Ping`], returned verbatim.
        sent_unix_nanos: u64,
    },
    /// The responder drained and counted this many bytes (reply to [`Request::SpeedSink`]).
    Received {
        /// How many payload bytes the responder read before EOF.
        bytes: u64,
    },
    /// The responder accepts a download and is about to source payload (reply to
    /// [`Request::SpeedSource`] / [`Request::SpeedBidir`]). A leading go-ahead frame is what lets the
    /// download client tell "here comes the payload" from a refusal on the very first read, so a
    /// wrong-method [`Unsupported`](Self::Unsupported) can never be drained as if it were zero bytes.
    Sourcing,
    /// The gate admitted this stream, but the handler does not serve the requested method: a ping frame
    /// arrived on the `speed` service, or a speed frame on `ping`. This is a TYPED refusal on the wire, so a
    /// client can tell "refused" from "measured
    /// badly" instead of reading a silently dropped stream as loss or zero bytes. The typed
    /// [`MethodRefusal`] code is the client's branch key; the bounded [`RefusalDetail`] is the prose it
    /// renders. A responder writes it instead of dropping the stream; a client decodes it to
    /// [`ProtocolError::Refused`], which no report can be constructed from.
    Unsupported {
        /// The typed method-level code the client branches on.
        code: MethodRefusal,
        /// The responder's bounded explanation, for a loud client-side error naming the peer and method.
        detail: RefusalDetail,
    },
}

impl Response {
    /// The wire size of a [`Pong`](Self::Pong): tag, sequence, and nonce (a reply carries no magic).
    /// The byte ceiling on a ping stream charges this per echo.
    pub(crate) const PONG_BYTES: u64 = 1 + 4 + 8;

    /// Write the response frame (no magic: a response is only ever read on a stream we opened).
    pub async fn write<W: io::AsyncWrite + Unpin>(&self, writer: &mut W) -> io::Result<()> {
        match self {
            Response::Pong {
                seq,
                sent_unix_nanos,
            } => {
                writer.write_all(&[resp_tag::PONG]).await?;
                writer.write_all(&seq.to_be_bytes()).await?;
                writer.write_all(&sent_unix_nanos.to_be_bytes()).await
            }
            Response::Received { bytes } => {
                writer.write_all(&[resp_tag::RECEIVED]).await?;
                writer.write_all(&bytes.to_be_bytes()).await
            }
            Response::Sourcing => writer.write_all(&[resp_tag::SOURCING]).await,
            Response::Unsupported { code, detail } => {
                writer.write_all(&[resp_tag::UNSUPPORTED]).await?;
                writer.write_all(&[code.tag()]).await?;
                write_detail(writer, detail).await
            }
        }
    }

    /// Read a response frame.
    pub async fn read<R: io::AsyncRead + Unpin>(reader: &mut R) -> Result<Self, ProtocolError> {
        let mut tag = [0u8; 1];
        reader.read_exact(&mut tag).await?;
        match tag[0] {
            resp_tag::PONG => Ok(Response::Pong {
                seq: read_u32(reader).await?,
                sent_unix_nanos: read_u64(reader).await?,
            }),
            resp_tag::RECEIVED => Ok(Response::Received {
                bytes: read_u64(reader).await?,
            }),
            resp_tag::SOURCING => Ok(Response::Sourcing),
            resp_tag::UNSUPPORTED => Ok(Response::Unsupported {
                code: MethodRefusal::from_tag(read_u8(reader).await?)?,
                detail: read_detail(reader).await?,
            }),
            other => Err(ProtocolError::UnknownResponse(other)),
        }
    }
}

async fn read_u8<R: io::AsyncRead + Unpin>(reader: &mut R) -> io::Result<u8> {
    let mut byte = [0u8; 1];
    reader.read_exact(&mut byte).await?;
    Ok(byte[0])
}

async fn read_u32<R: io::AsyncRead + Unpin>(reader: &mut R) -> io::Result<u32> {
    let mut bytes = [0u8; 4];
    reader.read_exact(&mut bytes).await?;
    Ok(u32::from_be_bytes(bytes))
}

async fn read_u64<R: io::AsyncRead + Unpin>(reader: &mut R) -> io::Result<u64> {
    let mut bytes = [0u8; 8];
    reader.read_exact(&mut bytes).await?;
    Ok(u64::from_be_bytes(bytes))
}

/// Write a length-prefixed refusal detail: a `u32` byte count then the already-bounded UTF-8 bytes. The
/// value is a [`RefusalDetail`], bounded by construction, so the writer never truncates and can never cut
/// a codepoint; the checked conversion still fails the write rather than emitting a frame our reader
/// would reject.
async fn write_detail<W: io::AsyncWrite + Unpin>(
    writer: &mut W,
    detail: &RefusalDetail,
) -> io::Result<()> {
    let bytes = detail.as_str().as_bytes();
    let len =
        u32::try_from(bytes.len()).map_err(|_| io::Error::other("refusal detail too long"))?;
    writer.write_all(&len.to_be_bytes()).await?;
    writer.write_all(bytes).await
}

/// Read a length-prefixed refusal detail written by [`write_detail`]. An over-cap claim is rejected
/// BEFORE the reader allocates, and the bytes must be valid UTF-8: a corrupt or hostile frame is an
/// error, never repaired or lossily decoded.
async fn read_detail<R: io::AsyncRead + Unpin>(
    reader: &mut R,
) -> Result<RefusalDetail, ProtocolError> {
    let len = read_u32(reader).await?;
    if len > RefusalDetail::MAX_LEN as u32 {
        return Err(ProtocolError::BadDetail(RefusalDetailError::TooLong(len)));
    }
    let mut bytes = vec![0u8; len as usize];
    reader.read_exact(&mut bytes).await?;
    Ok(RefusalDetail::try_from(bytes)?)
}

/// Why a diagnostic frame could not be decoded.
#[derive(Debug, thiserror::Error)]
pub enum ProtocolError {
    /// The stream did not open with [`IDENTITY`], so it is not a measure stream. The wording is now
    /// exactly true: it used to cover a measure peer on another version as well, which it never was.
    #[error("not a measure stream")]
    Foreign,
    /// A measure stream from a build that speaks a different request grammar.
    ///
    /// This message goes ON THE WIRE via [`answer`](Self::answer), so it is FIXED text plus the two
    /// version tags and nothing else. Never interpolate host state here: the only host fact it may
    /// carry is this build's own wire version, which any peer learns by being served at all.
    #[error(
        "measure wire version mismatch: the request is {peer}, this host speaks {VERSION}; run the \
         same release at both ends"
    )]
    Version {
        /// The version the peer's frame named.
        peer: WireVersion,
    },
    /// The request tag was not recognized.
    #[error("unknown request tag {0:#04x}")]
    UnknownRequest(u8),
    /// The response tag was not recognized.
    #[error("unknown response tag {0:#04x}")]
    UnknownResponse(u8),
    /// A well-formed reply did not match the request it answered (wrong sequence or nonce).
    #[error("reply did not match the probe")]
    Mismatched,
    /// This service does not serve the requested method: a ping frame arrived on the `speed`
    /// service (or a speed frame on `ping`). These are two independent services with distinct
    /// gates, so the served method must match the service the gate admitted, at the wire, not just at
    /// the gate. Refusing here is what makes a `ping` grant unable to open a speed drain. The responder
    /// answers a wrong-method frame with [`Response::Unsupported`] and returns this so the stream task
    /// logs why it refused; a client decodes that frame to [`Refused`](Self::Refused).
    #[error("this service does not serve that method")]
    WrongService,
    /// The requested method was refused, at either layer: the host's gate or resource refused the whole
    /// stream (Layer 1, arriving as [`bifrost::Error::Refused`]), or the handler admitted the stream but
    /// does not serve this method (Layer 2, arriving as a [`Response::Unsupported`] frame). Distinct from
    /// [`Io`] and [`Mismatched`] on purpose: a refusal is NOT a measurement, so a report can never be
    /// built from it. A render site MUST surface this as a loud, distinct error, never as `0` /
    /// `100% loss` / `0.00 MiB/s`.
    ///
    /// [`Io`]: Self::Io
    #[error("refused: {0}")]
    Refused(Refusal),
    /// A byte-bounded run ended before the asked count with no refusal frame: the peer stopped
    /// sending (or taking) payload, whether from its own lifetime cap, a crash, or a stalled link.
    /// The client cannot tell those apart, so it names only that the stream ended early; `moved` is
    /// what it could account for. Distinct from [`Refused`](Self::Refused) (a typed refusal on the
    /// wire) and from [`Io`](Self::Io) (a stream failure): this is a measurement that stopped short,
    /// so it must surface as an error, never as a smaller throughput or a zero-rate report.
    #[error("stream ended early: {moved} of {asked} bytes moved")]
    EndedEarly {
        /// The payload bytes the client accounted for before the peer stopped.
        moved: u64,
        /// The byte count the client asked to move.
        asked: u64,
    },
    /// A refusal detail could not be trusted: the frame claimed a length over [`RefusalDetail::MAX_LEN`],
    /// or its bytes were not valid UTF-8. A corrupt or hostile stream, not a real refusal, so it is
    /// rejected rather than repaired.
    #[error("bad refusal detail")]
    BadDetail(#[from] RefusalDetailError),
    /// The refusal code was not recognized: a corrupt or future stream, never guessed at.
    #[error("unknown refusal code {0:#04x}")]
    UnknownRefusalCode(u8),
    /// The underlying stream failed. Every non-refusal session failure lands here, including one that
    /// happened while OPENING the stream or while closing it, so this variant must not name an operation
    /// of its own: it used to read `read frame`, which described a read to a caller whose stream had never
    /// opened. Transparent, so the rendered text is the failure's own and a client that chains the causes
    /// gets `stream: peer went away` rather than a fabricated outer half over it. The variant still
    /// carries the class for anyone matching on it.
    #[error(transparent)]
    Io(#[from] io::Error),
}

impl ProtocolError {
    /// The frame to write back, for the one unreadable request a peer can act on.
    ///
    /// A version mismatch is answerable because the identity already proved the peer speaks measure:
    /// naming both versions tells them what happened and what to do about it. A foreign identity gets
    /// nothing, since we cannot know what would even be meaningful to whatever is on the other end,
    /// and every other failure either has no readable frame left or is not a framing fact at all.
    ///
    /// The code is [`MethodRefusal::WrongMethod`] because the answer must ride a code that ALREADY
    /// ships. The peer it is for is by definition on another build, and a code that build has no tag
    /// for is rejected by its reader ([`ProtocolError::UnknownRefusalCode`]) BEFORE the detail is
    /// read, which would lose the one sentence the frame exists to carry. It is also the honest
    /// reading: a method named in a grammar this build does not speak is not a method it serves.
    #[must_use]
    pub fn answer(&self) -> Option<Response> {
        match self {
            // The detail is this variant's own rendering, which is the whole reason that string is
            // held to fixed text plus the two version tags.
            Self::Version { .. } => Some(Response::Unsupported {
                code: MethodRefusal::WrongMethod,
                detail: RefusalDetail::bounded(self.to_string()),
            }),
            Self::Foreign
            | Self::UnknownRequest(_)
            | Self::UnknownResponse(_)
            | Self::Mismatched
            | Self::WrongService
            | Self::Refused(_)
            | Self::EndedEarly { .. }
            | Self::BadDetail(_)
            | Self::UnknownRefusalCode(_)
            | Self::Io(_) => None,
        }
    }
}

/// Map a session-level failure onto a protocol error. A typed refusal ([`bifrost::Error::Refused`]) is a
/// REFUSAL, not an i/o failure, so it maps to [`ProtocolError::Refused`] with the dialer-class refusal
/// preserved; every other session failure is a genuine [`ProtocolError::Io`]. This is the seam that stops
/// a typed refusal from arriving at the render path indistinguishable from a read error.
impl From<bifrost::Error> for ProtocolError {
    fn from(error: bifrost::Error) -> Self {
        match error {
            bifrost::Error::Refused(refusal) => ProtocolError::Refused(Refusal::Stream(refusal)),
            other => ProtocolError::Io(io::Error::other(other)),
        }
    }
}

#[cfg(test)]
#[path = "protocol_tests.rs"]
mod protocol_tests;
