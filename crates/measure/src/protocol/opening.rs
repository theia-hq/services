//! The opening frame's codec: the one place the bytes that start a diagnostic stream are judged.
//!
//! Its own file because it is a CODEC and nothing else: no IO, no policy, no responder. A frame that
//! encodes into one buffer and decodes from one slice is testable with no stream and no runtime, which
//! is what lets this wire's octet layouts be pinned as the normative examples a third party reads. The
//! stream-facing shells in the parent ([`Request::write`](super::Request::write) /
//! [`Request::read`](super::Request::read)) are thin over it, so this wire has ONE codec rather than a
//! writer and a reader that drift.

use tightbeam_handler::wire::{Frame, WireError};
use tokio::io;

use super::{IDENTITY, ProtocolError, Request, UNBOUNDED, VERSION, WireVersion, tag};

/// The bytes a diagnostic stream opens with, before the reader knows whether it can read them: the
/// identity, the version, and the tag that selects the request. Fixed, because a reader must know how
/// much to take before it knows anything else, and the tag is what says how long the rest is.
pub(super) const OPENING_HEAD: usize = IDENTITY.len() + 2 + 1;

/// What the opening frame of a diagnostic stream turned out to BE.
///
/// Two cases, because only two things can happen to bytes: this build read them, or it did not. The
/// second is a VALUE rather than a decode failure, and that is the whole reason this type exists. One
/// of the three ways an opening can be unreadable is a fact the peer can act on (a measure peer on
/// another request grammar), and a responder ANSWERS it on the wire; a decode failure would reach that
/// peer as a bare closed stream, which is exactly what the version answer exists to stop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Opening {
    /// A request in this build's grammar.
    Spoken(Request),
    /// An opening this build cannot read, carried as the head it read.
    Unread(Unread),
}

/// An opening head this build could not read, kept as the bytes themselves.
///
/// The bytes rather than a verdict, for two reasons. The condition is then judged in exactly ONE place
/// ([`Head::of`]), so the length rule, the reader, and the refusal cannot disagree about what a head
/// means. And the frame re-encodes exactly as it arrived, so the codec round-trips the heads it refuses
/// as well as the ones it serves. The field is private: an `Unread` is something the reader produces,
/// never something a caller asserts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Unread([u8; OPENING_HEAD]);

impl Unread {
    /// Why this build could not read it. THREE conditions, not two, because only the middle one is a
    /// fact the peer can act on: a foreign identity is not our protocol and there is nothing true we
    /// could say to whatever is on the other end, our identity at a version we do not serve is a peer
    /// worth telling, and a tag we have no frame for is a corrupt or future stream.
    #[must_use]
    pub fn cause(&self) -> ProtocolError {
        match Head::of(&self.0) {
            Head::Foreign => ProtocolError::Foreign,
            Head::Skewed(peer) => ProtocolError::Version { peer },
            Head::Ours(unknown) => ProtocolError::UnknownRequest(unknown),
        }
    }
}

/// What a head names. Judged once, here, because three different readers ask the same question about
/// it: how long the frame is, what to decode, and what to refuse it with.
enum Head {
    /// This build's grammar, at the request tag it named (which this build may still have no frame for).
    Ours(u8),
    /// Not this wire. Either the identity is not ours, or it is a LONGER identity that merely opens
    /// with ours, which is a different wire and not this one at an odd version.
    Foreign,
    /// This wire, at a version this build does not serve.
    Skewed(WireVersion),
}

impl Head {
    /// Judge one head. The identity is the maximal leading run of capitals, so a capital where the
    /// version begins is the RUN CONTINUING into a longer name (`DGX1` is identity `DGX`), never this
    /// wire at version `X1`. Reading it the other way hands this host's version to a protocol it does
    /// not speak, which is a fact it has no business disclosing and a diagnosis the peer cannot use.
    fn of(head: &[u8; OPENING_HEAD]) -> Self {
        let (identity, rest) = head.split_at(IDENTITY.len());
        if identity != IDENTITY {
            return Self::Foreign;
        }
        let [first, second, selector] = rest else {
            return Self::Foreign;
        };
        // The version half judges itself: it does not exist at all when the identity run continued
        // into it, which is a LONGER identity and a different wire.
        let Some(version) = WireVersion::after_identity([*first, *second]) else {
            return Self::Foreign;
        };
        if version != VERSION {
            return Self::Skewed(version);
        }
        Self::Ours(*selector)
    }
}

impl Frame for Opening {
    /// A ping frame, the widest this grammar has: the magic, the tag, a sequence, and a nonce.
    const MAX: usize = Request::PING_BYTES as usize;

    const HEAD: usize = OPENING_HEAD;

    /// Only a frame in THIS build's grammar has a length this build can trust, so anything else stops
    /// at the head and [`decode`](Frame::decode) classifies exactly the bytes that were read. Guessing
    /// a length out of another grammar's tag is how a reader eats the payload behind the frame, and on
    /// this wire the payload is the measurement.
    fn rest(head: &[u8]) -> usize {
        let Some(head) = head.first_chunk::<OPENING_HEAD>() else {
            return 0;
        };
        match Head::of(head) {
            Head::Ours(tag::PING) => 4 + 8,
            Head::Ours(tag::SPEED_SINK | tag::SPEED_SOURCE | tag::SPEED_BIDIR) => 8,
            // A tag with no frame behind it, and both not-ours conditions: the head is all we read.
            Head::Ours(_) | Head::Foreign | Head::Skewed(_) => 0,
        }
    }

    /// The whole frame into one buffer, so the writer makes one call. Byte for byte what the
    /// field-by-field writer emitted: identity, version, tag, then the variant's fields, big-endian.
    fn encode(&self, out: &mut Vec<u8>) {
        let request = match self {
            // An unread opening re-encodes as the head it arrived on. This build writes one grammar, so
            // an unread opening is only ever something the reader produced, and the exactness is what
            // lets the round-trip cover the heads this codec refuses as well as the ones it serves.
            Self::Unread(Unread(head)) => return out.extend_from_slice(head),
            Self::Spoken(request) => request,
        };
        out.extend_from_slice(&IDENTITY);
        out.extend_from_slice(VERSION.as_bytes());
        match *request {
            Request::Ping {
                seq,
                sent_unix_nanos,
            } => {
                out.push(tag::PING);
                out.extend_from_slice(&seq.to_be_bytes());
                out.extend_from_slice(&sent_unix_nanos.to_be_bytes());
            }
            Request::SpeedSink { limit_bytes } => {
                out.push(tag::SPEED_SINK);
                out.extend_from_slice(&limit_bytes.to_be_bytes());
            }
            Request::SpeedSource { limit_bytes } => {
                out.push(tag::SPEED_SOURCE);
                out.extend_from_slice(&limit_bytes.unwrap_or(UNBOUNDED).to_be_bytes());
            }
            Request::SpeedBidir { limit_bytes } => {
                out.push(tag::SPEED_BIDIR);
                out.extend_from_slice(&limit_bytes.unwrap_or(UNBOUNDED).to_be_bytes());
            }
        }
    }

    fn decode(bytes: &[u8]) -> Result<Self, WireError> {
        let Some(head) = bytes.first_chunk::<OPENING_HEAD>() else {
            return Err(short_frame());
        };
        let Head::Ours(selector) = Head::of(head) else {
            return Ok(Self::Unread(Unread(*head)));
        };
        let request = match selector {
            tag::PING => Request::Ping {
                seq: u32::from_be_bytes(field(bytes, OPENING_HEAD)?),
                sent_unix_nanos: u64::from_be_bytes(field(bytes, OPENING_HEAD + 4)?),
            },
            tag::SPEED_SINK => Request::SpeedSink {
                limit_bytes: u64::from_be_bytes(field(bytes, OPENING_HEAD)?),
            },
            tag::SPEED_SOURCE => Request::SpeedSource {
                limit_bytes: counted(u64::from_be_bytes(field(bytes, OPENING_HEAD)?)),
            },
            tag::SPEED_BIDIR => Request::SpeedBidir {
                limit_bytes: counted(u64::from_be_bytes(field(bytes, OPENING_HEAD)?)),
            },
            _unknown => return Ok(Self::Unread(Unread(*head))),
        };
        Ok(Self::Spoken(request))
    }
}

/// Read a fixed-width field out of a decoded frame. The reader hands [`Frame::decode`] exactly one
/// frame's bytes, but `decode` is public and a caller can hand it anything, so a short slice is an
/// error rather than a slice index that panics.
fn field<const N: usize>(bytes: &[u8], at: usize) -> Result<[u8; N], WireError> {
    bytes
        .get(at..at + N)
        .and_then(|field| <[u8; N]>::try_from(field).ok())
        .ok_or_else(short_frame)
}

/// A frame that ended inside a field it declared. An end-of-file, so a caller that already tells a
/// clean stream end from a framing failure keeps reading it the same way.
fn short_frame() -> WireError {
    WireError::malformed(ProtocolError::Io(io::Error::from(
        io::ErrorKind::UnexpectedEof,
    )))
}

/// The wire's "no exact count" ceiling, back as the `None` the domain means by it.
fn counted(limit_bytes: u64) -> Option<u64> {
    (limit_bytes != UNBOUNDED).then_some(limit_bytes)
}

/// Cross a frame-writer failure back into an `io::Result` boundary. A stream failure passes through as
/// ITSELF, so the cause chain stays one deep and a caller still reads the real kind; a framing failure
/// becomes an `Other` carrying the typed error as its source.
pub(super) fn io_error(error: WireError) -> io::Error {
    match error {
        WireError::Io(error) => error,
        framing => io::Error::other(framing),
    }
}

/// Take measure's own decode failure back out of the frame reader, which erases it behind a boxed
/// source so one reader can serve every wire. Total rather than a catch-all: a caller keeps matching
/// the typed variants it always did, and only a cause that is not ours travels as a stream failure
/// carrying itself.
impl From<WireError> for ProtocolError {
    fn from(error: WireError) -> Self {
        match error {
            WireError::Io(error) => Self::Io(error),
            WireError::Malformed(cause) => match cause.downcast::<Self>() {
                Ok(protocol) => *protocol,
                Err(foreign) => Self::Io(io::Error::other(foreign)),
            },
            over_cap => Self::Io(io::Error::other(over_cap)),
        }
    }
}
