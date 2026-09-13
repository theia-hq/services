//! The diagnostic responder bodies: what an admitted diagnostic stream does, one method at a time.
//!
//! ping and speed are TWO independent services, not one: `ping` (cheap RTT) and `speed` (bandwidth-eating
//! throughput). A node may offer one without the other, and each carries its own gate, so the served
//! method MUST match the service that admitted the stream. [`crate::server::Ping`] and
//! [`crate::server::Speed`] are the public entries; the per-stream bodies here are crate-private and each
//! refuses the other's method at the wire ([`ProtocolError::WrongService`]), so a `ping` grant can never
//! open a speed drain even though both speak the same frame. `answer` is the union of both, for the
//! in-crate responder loop the reach tests drive.

use bifrost::RefusalDetail;
use tokio::io;

use crate::payload::Payload;
use crate::protocol::{MethodRefusal, ProtocolError, Request, Response};

/// The responder-side bounds [`crate::server::Speed`] enforces on one speed stream: the largest payload it
/// will move per direction. `None` is unbounded (the old mirror-the-client behavior), which a caller may
/// choose only through [`crate::server::Limits::unmetered`] and then owns the banner caveat.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct SpeedCaps {
    /// The largest payload one direction may move, or `None` for unbounded.
    pub(crate) max_bytes: Option<u64>,
}

impl SpeedCaps {
    /// Bound a requested byte count: an explicit request is clamped to the cap, and an unbounded source
    /// (`None`) becomes the cap itself, so a metered run always terminates on a byte count.
    pub(crate) fn clamp(&self, limit_bytes: Option<u64>) -> Option<u64> {
        match (self.max_bytes, limit_bytes) {
            (Some(cap), Some(requested)) => Some(requested.min(cap)),
            (Some(cap), None) => Some(cap),
            (None, requested) => requested,
        }
    }
}

/// Answer one inbound stream on the `ping` service: echo the opening ping and every probe on it
/// (the client sends its whole run over one stream). A non-ping frame is a wire-level violation, not a
/// silent widening: the outer `ping` gate admitted this stream for liveness only, so a speed frame
/// here is refused with [`ProtocolError::WrongService`].
///
/// Crate-private: the entry is [`crate::server::Ping`], which applies the per-caller rate bound first.
pub(crate) async fn answer_ping<W, R>(mut writer: W, mut reader: R) -> Result<(), ProtocolError>
where
    W: io::AsyncWrite + Unpin,
    R: io::AsyncRead + Unpin,
{
    match Request::read(&mut reader).await? {
        Request::Ping {
            seq,
            sent_unix_nanos,
        } => echo_pings(&mut writer, &mut reader, seq, sent_unix_nanos).await,
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
    match Request::read(&mut reader).await? {
        Request::Ping { .. } => {
            refuse(
                &mut writer,
                MethodRefusal::WrongMethod,
                "this node serves speed, not ping",
            )
            .await
        }
        speed => serve_speed(&mut writer, &mut reader, speed, caps).await,
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
        } => echo_pings(&mut writer, &mut reader, seq, sent_unix_nanos).await,
        speed => serve_speed(&mut writer, &mut reader, speed, SpeedCaps::default()).await,
    }
}

/// Run one speed transfer for an already-read speed request: drain a sink, source a download, or mirror a
/// full-duplex run, each clamped to `caps`. Shared by [`answer_speed`] and the union [`answer`], so the
/// transfer engine has one home. A [`Request::Ping`] is unreachable here (both callers peel it off first)
/// and refused for completeness.
async fn serve_speed<W, R>(
    writer: &mut W,
    reader: &mut R,
    request: Request,
    caps: SpeedCaps,
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
            // than its configured bound, and the count it reports is the bytes it actually took.
            let bounded = caps.clamp(Some(limit_bytes)).unwrap_or(limit_bytes);
            let bytes = Payload::of(bounded).drain(reader).await?;
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
            // A cap turns an unbounded source into a byte-bounded one, so a metered run always terminates
            // on the responder's own terms; an unmetered one sources until the client stops.
            source(writer, caps.clamp(limit_bytes)).await?;
            Ok(())
        }
        Request::SpeedBidir { limit_bytes } => {
            // Lead with the go-ahead frame (as the source path does) so the client's download half reads
            // "sourcing" or a refusal deterministically before any payload, then run both halves.
            Response::Sourcing.write(writer).await?;
            // Full-duplex: drain the client's upload while sourcing our download at once, both clamped to
            // the same cap. Run both to completion.
            let bounded = caps.clamp(limit_bytes);
            let (sourced, drained) = tokio::join!(
                source(writer, bounded),
                Payload::of_or_until_peer(bounded).drain(reader),
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

/// Source counted download payload: an exact `Some(n)` bytes for a byte bound, or unbounded until the
/// client stops reading for a time bound (its deadline, not a byte count, is the terminator).
async fn source<W: io::AsyncWrite + Unpin>(
    writer: &mut W,
    limit_bytes: Option<u64>,
) -> io::Result<()> {
    let payload = match limit_bytes {
        Some(bytes) => Payload::of(bytes),
        None => Payload::until_peer_stops(),
    };
    payload.send(writer).await?;
    Ok(())
}

/// Echo the opening ping, then every subsequent ping on the same stream until the client closes it.
async fn echo_pings<W, R>(
    writer: &mut W,
    reader: &mut R,
    mut seq: u32,
    mut sent_unix_nanos: u64,
) -> Result<(), ProtocolError>
where
    W: io::AsyncWrite + Unpin,
    R: io::AsyncRead + Unpin,
{
    loop {
        Response::Pong {
            seq,
            sent_unix_nanos,
        }
        .write(writer)
        .await?;

        match Request::read(reader).await {
            Ok(Request::Ping {
                seq: next_seq,
                sent_unix_nanos: next_nonce,
            }) => {
                seq = next_seq;
                sent_unix_nanos = next_nonce;
            }
            // A clean EOF ends the probe run; any other outcome is a real stream error.
            Err(ProtocolError::Io(error)) if error.kind() == io::ErrorKind::UnexpectedEof => {
                return Ok(());
            }
            Ok(_) => return Err(ProtocolError::Mismatched),
            Err(error) => return Err(error),
        }
    }
}
