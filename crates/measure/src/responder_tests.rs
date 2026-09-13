//! Responder-side bounds: the ping stream's wall-clock and byte caps and the speed stream's wall-clock
//! cap, proven on a paused clock so the caps fire deterministically instead of after real seconds. The
//! bounded and partial-count paths are driven over one in-memory stream pair, the same [`answer_ping`] /
//! [`answer_speed`] bodies the responder wires.

use core::time::Duration;

use tokio::io::{self, AsyncReadExt as _, AsyncWriteExt as _, DuplexStream};
use tokio::time;

use super::{
    PING_BYTE_CAP_DETAIL, PING_TIME_CAP_DETAIL, PingCaps, SpeedCaps, answer_ping, answer_speed,
};
use crate::protocol::{MethodRefusal, ProtocolError, Request, Response};

/// The in-memory stream capacity. Small, so an unbounded source fills it and parks on backpressure.
const CAPACITY: usize = 1024;

/// The responder task's join handle: the answer body driving the server halves.
type Serving = tokio::task::JoinHandle<Result<(), ProtocolError>>;

/// Caps that never end a run on bytes, so the wall clock is the only terminator under test.
fn capped_at(duration: Duration) -> SpeedCaps {
    SpeedCaps {
        max_bytes: Some(u64::MAX),
        max_duration: Some(duration),
    }
}

/// An in-memory stream pair with the speed responder already running on the server end.
fn serve(caps: SpeedCaps) -> (DuplexStream, Serving) {
    let (client, server) = io::duplex(CAPACITY);
    let (mut server_read, mut server_write) = io::split(server);
    let task =
        tokio::spawn(async move { answer_speed(&mut server_write, &mut server_read, caps).await });
    (client, task)
}

/// An in-memory stream pair with the ping responder already running on the server end.
fn serve_ping(caps: PingCaps) -> (DuplexStream, Serving) {
    let (client, server) = io::duplex(CAPACITY);
    let (mut server_read, mut server_write) = io::split(server);
    let task =
        tokio::spawn(async move { answer_ping(&mut server_write, &mut server_read, caps).await });
    (client, task)
}

/// An unbounded source with a client that never reads and never closes must end at the cap. Without the
/// cap the responder parks on backpressure forever; the paused clock advances to the deadline while both
/// tasks idle, so the timing assertion is deterministic rather than a real 15-second wait.
#[tokio::test(start_paused = true)]
async fn an_unbounded_source_stops_at_the_duration_cap() {
    let cap = Duration::from_secs(15);
    let (mut client, serving) = serve(capped_at(cap));

    Request::SpeedSource { limit_bytes: None }
        .write(&mut client)
        .await
        .expect("the request frame fits the stream");
    assert_eq!(
        Response::read(&mut client)
            .await
            .expect("the go-ahead frame"),
        Response::Sourcing
    );

    let started = time::Instant::now();
    let result = serving.await.expect("the responder task does not panic");
    assert!(result.is_ok(), "a capped source closes cleanly: {result:?}");
    let elapsed = started.elapsed();
    assert!(
        elapsed <= cap + Duration::from_secs(1),
        "the run must end by the cap plus scheduling slack, took {elapsed:?}"
    );
    assert!(
        elapsed >= cap,
        "the unread payload, not an early close, must hold the stream to the cap, took {elapsed:?}"
    );
}

/// A bounded source is unaffected: it ends on its byte count well before the cap, with every byte
/// delivered and no truncation.
#[tokio::test(start_paused = true)]
async fn a_bounded_source_is_unaffected_by_the_duration_cap() {
    let cap = Duration::from_secs(15);
    let payload_bytes = 4 * 1024u64;
    let (mut client, serving) = serve(capped_at(cap));

    Request::SpeedSource {
        limit_bytes: Some(payload_bytes),
    }
    .write(&mut client)
    .await
    .expect("the request frame fits the stream");
    assert_eq!(
        Response::read(&mut client)
            .await
            .expect("the go-ahead frame"),
        Response::Sourcing
    );

    let started = time::Instant::now();
    let mut received = vec![0u8; payload_bytes as usize];
    client
        .read_exact(&mut received)
        .await
        .expect("a bounded source delivers every byte");

    let result = serving.await.expect("the responder task does not panic");
    assert!(result.is_ok(), "a bounded source completes: {result:?}");
    assert!(
        started.elapsed() < cap,
        "a bounded run ends on its byte count, not the cap, took {:?}",
        started.elapsed()
    );
}

/// A sink whose client goes silent must stop at the cap with the count it actually took, never zero and
/// never a dropped reply frame.
#[tokio::test(start_paused = true)]
async fn a_capped_sink_reports_the_bytes_it_took() {
    let cap = Duration::from_secs(15);
    let sent = 4 * 1024u64;
    let (mut client, serving) = serve(capped_at(cap));

    Request::SpeedSink {
        limit_bytes: u64::MAX,
    }
    .write(&mut client)
    .await
    .expect("the request frame fits the stream");
    client
        .write_all(&[0u8; 4 * 1024])
        .await
        .expect("the upload fits the stream");

    let result = serving.await.expect("the responder task does not panic");
    assert!(result.is_ok(), "a capped sink returns Ok: {result:?}");
    match Response::read(&mut client).await.expect("the count frame") {
        Response::Received { bytes } => assert_eq!(
            bytes, sent,
            "the sink reports the bytes it actually took, not zero"
        ),
        other => panic!("a capped sink must report its partial count, got {other:?}"),
    }
}

/// A ping stream whose client goes silent after the first probe must end at the wall-clock cap, not run
/// until the client closes or the job dies. The paused clock advances to the deadline while both ends
/// wait, so the assertion is deterministic. The client sees the cap as a typed refusal, never a silent
/// close it would fold into loss.
#[tokio::test(start_paused = true)]
async fn an_idle_ping_stream_stops_at_the_duration_cap() {
    let cap = Duration::from_secs(60);
    let (mut client, serving) = serve_ping(PingCaps {
        max_bytes: None,
        max_duration: Some(cap),
    });

    Request::Ping {
        seq: 0,
        sent_unix_nanos: 7,
    }
    .write(&mut client)
    .await
    .expect("the opening probe fits the stream");
    assert_eq!(
        Response::read(&mut client).await.expect("the opening pong"),
        Response::Pong {
            seq: 0,
            sent_unix_nanos: 7
        }
    );

    let started = time::Instant::now();
    let result = serving.await.expect("the responder task does not panic");
    assert!(
        result.is_ok(),
        "a capped ping stream closes cleanly: {result:?}"
    );
    let elapsed = started.elapsed();
    assert!(
        elapsed >= cap,
        "the idle stream must hold to the cap, took {elapsed:?}"
    );
    assert!(
        elapsed <= cap + Duration::from_secs(1),
        "the cap, not the client, must end the stream, took {elapsed:?}"
    );

    match Response::read(&mut client).await.expect("the cap refusal") {
        Response::Unsupported { code, detail } => {
            assert_eq!(code, MethodRefusal::RateLimited);
            assert_eq!(detail.as_str(), PING_TIME_CAP_DETAIL);
        }
        other => panic!("a capped stream must end with a typed refusal, got {other:?}"),
    }
}

/// A probing client that reads every echo still cannot move more bytes than the byte cap: the stream
/// ends with the typed refusal once the counted requests and echoes reach the ceiling, before the wall
/// clock (paused at zero elapsed here) can fire.
#[tokio::test(start_paused = true)]
async fn a_busy_ping_stream_stops_at_the_byte_cap() {
    // Room for exactly two round trips: the opening request plus its echo, then one more probe and
    // echo; the following echo would cross the ceiling and is refused instead.
    let round_trip = Request::PING_BYTES + Response::PONG_BYTES;
    let (mut client, serving) = serve_ping(PingCaps {
        max_bytes: Some(2 * round_trip),
        max_duration: Some(Duration::from_secs(60)),
    });

    let mut pongs = 0u32;
    let mut ending = None;
    for seq in 0..10 {
        Request::Ping {
            seq,
            sent_unix_nanos: u64::from(seq),
        }
        .write(&mut client)
        .await
        .expect("the probe fits the stream");
        match Response::read(&mut client).await.expect("a reply frame") {
            Response::Pong { .. } => pongs += 1,
            other => {
                ending = Some(other);
                break;
            }
        }
    }

    let result = serving.await.expect("the responder task does not panic");
    assert!(
        result.is_ok(),
        "a byte-capped ping stream closes cleanly: {result:?}"
    );
    assert_eq!(pongs, 2, "the cap allows exactly two echoes");
    match ending {
        Some(Response::Unsupported { code, detail }) => {
            assert_eq!(code, MethodRefusal::RateLimited);
            assert_eq!(detail.as_str(), PING_BYTE_CAP_DETAIL);
        }
        other => panic!("the stream must end with a typed refusal, got {other:?}"),
    }
}
