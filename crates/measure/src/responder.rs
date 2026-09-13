//! The diagnostic responder bodies: what an admitted diagnostic stream does, one method at a time.
//!
//! ping and speed are TWO independent services, not one: `ping` (cheap RTT) and `speed` (bandwidth-eating
//! throughput). A node may offer one without the other, and each carries its own gate, so the served
//! method MUST match the service that admitted the stream. [`crate::server::Ping`] and
//! [`crate::server::Speed`] are the public entries; the per-stream bodies here are crate-private and each
//! refuses the other's method at the wire ([`ProtocolError::WrongService`]), so a `ping` grant can never
//! open a speed drain even though both speak the same frame. `answer` is the union of both, for the
//! in-crate responder loop the reach tests drive.

use core::time::Duration;

use bifrost::RefusalDetail;
use tokio::{io, time};

use crate::payload::Payload;
use crate::protocol::{MethodRefusal, ProtocolError, Request, Response};

/// The responder-side bounds [`crate::server::Speed`] enforces on one speed stream: the largest payload it
/// will move per direction and the longest it will run. `None` on either is unbounded (the old
/// mirror-the-client behavior), which only the crate's test-only union body asks for: the public-capable
/// [`crate::server::Speed`] fills both caps from [`crate::server::Limits`], which has no unbounded
/// constructor.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct SpeedCaps {
    /// The largest payload one direction may move, or `None` for unbounded.
    pub(crate) max_bytes: Option<u64>,
    /// The longest the stream may run, or `None` for unbounded.
    pub(crate) max_duration: Option<Duration>,
}

/// The responder-side bounds [`crate::server::Ping`] enforces on one ping stream: the largest number of
/// bytes the stream may move (fixed-width requests plus their echoes) and the longest it may run, from
/// the opening read through the last echo. `None` on either is unbounded (the old mirror-the-client
/// behavior), which only the crate's test-only union body asks for: the public-capable
/// [`crate::server::Ping`] fills both caps from [`crate::server::Limits`], which has no unbounded
/// constructor.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct PingCaps {
    /// The largest number of stream bytes one run may move, or `None` for unbounded.
    pub(crate) max_bytes: Option<u64>,
    /// The longest the stream may run, or `None` for unbounded.
    pub(crate) max_duration: Option<Duration>,
}

/// The refusal a ping stream ends with when its byte ceiling stops it.
const PING_BYTE_CAP_DETAIL: &str = "ping stream reached its byte cap, reconnect to continue";
/// The refusal a ping stream ends with when its wall clock stops it.
const PING_TIME_CAP_DETAIL: &str = "ping stream reached its lifetime cap, reconnect to continue";

impl SpeedCaps {
    /// Bound a requested byte count: an explicit request is clamped to the cap, and an unbounded source
    /// (`None`) becomes the cap itself, so a metered run always carries its own byte bound.
    pub(crate) fn clamp(&self, limit_bytes: Option<u64>) -> Option<u64> {
        match (self.max_bytes, limit_bytes) {
            (Some(cap), Some(requested)) => Some(requested.min(cap)),
            (Some(cap), None) => Some(cap),
            (None, requested) => requested,
        }
    }
}

/// Answer one inbound stream on the `ping` service: echo the opening ping and every probe on it
/// (the client sends its whole run over one stream), bounded by `caps`. A non-ping frame is a wire-level
/// violation, not a silent widening: the outer `ping` gate admitted this stream for liveness only, so a
/// speed frame here is refused with [`ProtocolError::WrongService`].
///
/// One deadline covers the opening read and every echo, so a peer that opens a stream and stalls is
/// bounded like the run itself; a capped stream ends with the typed Layer-2 refusal when the stream
/// still carries a frame, and closes when the peer is no longer reading.
///
/// Crate-private: the entry is [`crate::server::Ping`], which applies the per-caller rate bound first.
pub(crate) async fn answer_ping<W, R>(
    mut writer: W,
    mut reader: R,
    caps: PingCaps,
) -> Result<(), ProtocolError>
where
    W: io::AsyncWrite + Unpin,
    R: io::AsyncRead + Unpin,
{
    let deadline = caps.max_duration.map(|cap| time::Instant::now() + cap);
    let opening = match deadline {
        Some(deadline) => match time::timeout_at(deadline, Request::read(&mut reader)).await {
            Ok(request) => request?,
            // The cap fired before a probe arrived; nothing was served, so the stream just closes.
            Err(_past_deadline) => return Ok(()),
        },
        None => Request::read(&mut reader).await?,
    };
    match opening {
        Request::Ping {
            seq,
            sent_unix_nanos,
        } => {
            echo_pings(
                &mut writer,
                &mut reader,
                seq,
                sent_unix_nanos,
                caps,
                deadline,
            )
            .await
        }
        _ => {
            refuse(
                &mut writer,
                MethodRefusal::WrongMethod,
                "this node serves ping, not speed",
            )
            .await
        }
    }
}

/// Answer one inbound stream on the `speed` service: run the requested transfer (sink / source /
/// bidir), one per stream, bounded by `caps`. A ping frame is refused with [`ProtocolError::WrongService`]
/// for symmetry, so a `speed` grant serves only throughput, never a liveness probe on the wrong wall.
///
/// Crate-private: the entry is [`crate::server::Speed`], which holds the transfer slot and the caps.
pub(crate) async fn answer_speed<W, R>(
    mut writer: W,
    mut reader: R,
    caps: SpeedCaps,
) -> Result<(), ProtocolError>
where
    W: io::AsyncWrite + Unpin,
    R: io::AsyncRead + Unpin,
{
    // One deadline for the whole stream: the request read and the transfer share the cap, so a caller
    // that stalls before naming a method is bounded too, never parked open until the client stops.
    let deadline = caps.max_duration.map(|cap| time::Instant::now() + cap);
    let request = match deadline {
        Some(deadline) => match time::timeout_at(deadline, Request::read(&mut reader)).await {
            Ok(request) => request?,
            // The cap fired before a method arrived; close cleanly, nothing was served.
            Err(_past_deadline) => return Ok(()),
        },
        None => Request::read(&mut reader).await?,
    };
    match request {
        Request::Ping { .. } => {
            refuse(
                &mut writer,
                MethodRefusal::WrongMethod,
                "this node serves speed, not ping",
            )
            .await
        }
        speed => serve_speed(&mut writer, &mut reader, speed, caps, deadline).await,
    }
}

/// Answer one inbound stream on the union of both methods, dispatching on its opening
/// request. The in-crate responder loop the reach tests drive uses this; a served node wires the split
/// [`crate::server::Ping`] / [`crate::server::Speed`] entries instead, each behind its own gate.
#[cfg(test)]
pub(crate) async fn answer<W, R>(mut writer: W, mut reader: R) -> Result<(), ProtocolError>
where
    W: io::AsyncWrite + Unpin,
    R: io::AsyncRead + Unpin,
{
    match Request::read(&mut reader).await? {
        Request::Ping {
            seq,
            sent_unix_nanos,
        } => {
            echo_pings(
                &mut writer,
                &mut reader,
                seq,
                sent_unix_nanos,
                PingCaps::default(),
                None,
            )
            .await
        }
        speed => serve_speed(&mut writer, &mut reader, speed, SpeedCaps::default(), None).await,
    }
}

/// Run one speed transfer for an already-read speed request: drain a sink, source a download, or mirror a
/// full-duplex run, each clamped to `caps` and stopped at `deadline` when one is set. Shared by
/// [`answer_speed`] and the union [`answer`], so the transfer engine has one home. A [`Request::Ping`] is
/// unreachable here (both callers peel it off first) and refused for completeness.
async fn serve_speed<W, R>(
    writer: &mut W,
    reader: &mut R,
    request: Request,
    caps: SpeedCaps,
    deadline: Option<time::Instant>,
) -> Result<(), ProtocolError>
where
    W: io::AsyncWrite + Unpin,
    R: io::AsyncRead + Unpin,
{
    match request {
        Request::Ping { .. } => {
            refuse(
                writer,
                MethodRefusal::WrongMethod,
                "this node serves speed, not ping",
            )
            .await
        }
        Request::SpeedSink { limit_bytes } => {
            // The sink drains the client's upload, clamped to the cap: a metered node never accepts more
            // than its configured bound, and the count it reports is the bytes it actually took. A
            // deadline stop is no exception: the drain returns its partial count and the reply carries
            // it, so a capped sink is a short count, never a dropped frame.
            let bounded = caps.clamp(Some(limit_bytes)).unwrap_or(limit_bytes);
            let bytes = drain(reader, Some(bounded), deadline).await?;
            Response::Received { bytes }
                .write(writer)
                .await
                .map_err(ProtocolError::from)
        }
        Request::SpeedSource { limit_bytes } => {
            // A leading go-ahead frame precedes the payload so the client can tell "here comes the
            // download" from a refusal on its first read; a wrong-method node writes `Unsupported`
            // instead (in `answer_ping`), so the download can never drain a refusal as zero bytes.
            Response::Sourcing.write(writer).await?;
            // A cap turns an unbounded source into a byte- or time-bounded one, so a metered run always
            // terminates on the responder's own terms; an unbounded one (the test-only union body)
            // sources until the client stops.
            source(writer, caps.clamp(limit_bytes), deadline).await?;
            Ok(())
        }
        Request::SpeedBidir { limit_bytes } => {
            // Lead with the go-ahead frame (as the source path does) so the client's download half reads
            // "sourcing" or a refusal deterministically before any payload, then run both halves.
            Response::Sourcing.write(writer).await?;
            // Full-duplex: drain the client's upload while sourcing our download at once, both clamped to
            // the same cap and stopped at the same deadline. Run both to completion.
            let bounded = caps.clamp(limit_bytes);
            let (sourced, drained) = tokio::join!(
                source(writer, bounded, deadline),
                drain(reader, bounded, deadline),
            );
            sourced?;
            drained?;
            Ok(())
        }
    }
}

/// Refuse a wrong-method or over-limit frame LOUDLY: write a typed [`Response::Unsupported`] frame carrying
/// `code` and a bounded detail off `reason`, so the client decodes a refusal (not a silently dropped stream
/// it would read as loss or zero bytes), then return [`ProtocolError::WrongService`] so the stream task logs
/// why it refused. This is the fix for the false-success class: a refused frame is a frame on the wire,
/// never a silent close.
pub(crate) async fn refuse<W: io::AsyncWrite + Unpin>(
    writer: &mut W,
    code: MethodRefusal,
    reason: &str,
) -> Result<(), ProtocolError> {
    Response::Unsupported {
        code,
        detail: RefusalDetail::bounded(reason),
    }
    .write(writer)
    .await?;
    Err(ProtocolError::WrongService)
}

/// Drain the client's upload to `limit_bytes` (or to EOF when `None`), stopping at `deadline` when one
/// is set and returning the count taken. A deadline stop keeps the partial count, so a capped sink
/// reports what it actually took rather than losing the count to the stop.
async fn drain<R: io::AsyncRead + Unpin>(
    reader: &mut R,
    limit_bytes: Option<u64>,
    deadline: Option<time::Instant>,
) -> Result<u64, ProtocolError> {
    Payload::of_or_until_peer(limit_bytes)
        .drain_within(reader, deadline)
        .await
        .map_err(ProtocolError::from)
}

/// Source counted download payload: an exact `Some(n)` bytes for a byte bound, or unbounded until the
/// client stops reading. Stops at `deadline` when one is set and closes the stream: a capped source is
/// a truncated close, never an unbounded run.
async fn source<W: io::AsyncWrite + Unpin>(
    writer: &mut W,
    limit_bytes: Option<u64>,
    deadline: Option<time::Instant>,
) -> io::Result<()> {
    let payload = match limit_bytes {
        Some(bytes) => Payload::of(bytes),
        None => Payload::until_peer_stops(),
    };
    match deadline {
        Some(deadline) => match time::timeout_at(deadline, payload.send(writer)).await {
            Ok(result) => result.map(|_| ()),
            // Cancelled at the cap: the stream closes behind a partial payload, so the client reads a
            // truncated stream, never a hang.
            Err(_past_deadline) => Ok(()),
        },
        None => payload.send(writer).await.map(|_| ()),
    }
}

/// End a ping stream that hit a cap: write the typed Layer-2 refusal while the stream still carries a
/// frame, then close. Best effort under the stream deadline, because a peer that stopped reading has no
/// room for the frame: the cap, not the write, owns the stream's end.
async fn refuse_capped<W: io::AsyncWrite + Unpin>(
    writer: &mut W,
    deadline: Option<time::Instant>,
    detail: &str,
) -> Result<(), ProtocolError> {
    let frame = Response::Unsupported {
        code: MethodRefusal::RateLimited,
        detail: RefusalDetail::bounded(detail),
    };
    match deadline {
        Some(deadline) => match time::timeout_at(deadline, frame.write(writer)).await {
            Ok(result) => result?,
            Err(_past_deadline) => {}
        },
        None => frame.write(writer).await?,
    }
    Ok(())
}

/// Echo the opening ping, then every subsequent ping on the same stream until the client closes it or
/// `caps` end the run.
///
/// The byte ceiling counts the whole stream (each fixed-width request and its echo), and the wall clock
/// covers the opening read through the last echo, whichever ends first. Either ending writes the typed
/// Layer-2 refusal where the stream still carries one, so a client reads a refusal, never a silent close
/// it folds into loss; a client that stopped reading sees only the close, because the write is best
/// effort at the cap. The wall clock is checked between probes as well as on the pending operations: a
/// peer that keeps every await ready would otherwise stream past the deadline without ever parking.
async fn echo_pings<W, R>(
    writer: &mut W,
    reader: &mut R,
    mut seq: u32,
    mut sent_unix_nanos: u64,
    caps: PingCaps,
    deadline: Option<time::Instant>,
) -> Result<(), ProtocolError>
where
    W: io::AsyncWrite + Unpin,
    R: io::AsyncRead + Unpin,
{
    // The opening probe was read by the caller; charge it here so the byte ceiling counts the whole
    // stream, not just the echoes.
    let mut moved = Request::PING_BYTES;
    loop {
        if deadline.is_some_and(|deadline| time::Instant::now() >= deadline) {
            return refuse_capped(writer, deadline, PING_TIME_CAP_DETAIL).await;
        }
        if caps
            .max_bytes
            .is_some_and(|max| moved + Response::PONG_BYTES > max)
        {
            return refuse_capped(writer, deadline, PING_BYTE_CAP_DETAIL).await;
        }

        let pong = Response::Pong {
            seq,
            sent_unix_nanos,
        };
        let echoed = match deadline {
            Some(deadline) => time::timeout_at(deadline, pong.write(writer)).await,
            None => Ok(pong.write(writer).await),
        };
        match echoed {
            Ok(result) => result?,
            // The echo parked on backpressure at the cap: the peer is not reading, so no frame can
            // reach it; close instead of holding the task past the cap.
            Err(_past_deadline) => return Ok(()),
        }
        moved += Response::PONG_BYTES;

        let next = match deadline {
            Some(deadline) => time::timeout_at(deadline, Request::read(reader)).await,
            None => Ok(Request::read(reader).await),
        };
        match next {
            Ok(Ok(Request::Ping {
                seq: next_seq,
                sent_unix_nanos: next_nonce,
            })) => {
                seq = next_seq;
                sent_unix_nanos = next_nonce;
                moved += Request::PING_BYTES;
            }
            // A clean EOF ends the probe run; any other outcome is a real stream error.
            Ok(Err(ProtocolError::Io(error))) if error.kind() == io::ErrorKind::UnexpectedEof => {
                return Ok(());
            }
            Ok(Err(error)) => return Err(error),
            Ok(Ok(_)) => return Err(ProtocolError::Mismatched),
            // An idle peer at the cap: the stream is between frames, so the refusal lands whole.
            Err(_past_deadline) => {
                return refuse_capped(writer, deadline, PING_TIME_CAP_DETAIL).await;
            }
        }
    }
}

#[cfg(test)]
#[path = "responder_tests.rs"]
mod responder_tests;
