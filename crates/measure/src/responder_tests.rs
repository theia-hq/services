//! Responder-side bounds: the ping stream's wall-clock and byte caps and the speed stream's wall-clock
//! cap, proven on a paused clock so the caps fire deterministically instead of after real seconds. The
//! bounded and partial-count paths are driven over one in-memory stream pair, the same [`answer_ping`] /
//! [`answer_speed`] bodies the responder wires.

use core::time::Duration;

use tokio::io::{self, AsyncReadExt as _, AsyncWriteExt as _, DuplexStream};
use tokio::time;

use super::{
    PING_BYTE_CAP_DETAIL, PING_TIME_CAP_DETAIL, PingCaps, SPEED_BYTE_CAP_DETAIL, SpeedCaps,
    answer_ping, answer_speed, respond_ping, respond_speed,
};
use crate::protocol::{MethodRefusal, Opening, ProtocolError, Request, Response};

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

/// Caps with a real byte ceiling and a wall clock wide enough that only the byte cap decides, so an
/// over-cap ask and a full-size run are separated without advancing the clock.
fn capped_at_bytes(cap: u64) -> SpeedCaps {
    SpeedCaps {
        max_bytes: Some(cap),
        max_duration: Some(Duration::from_secs(15)),
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

/// An ask over the byte cap is refused BEFORE the payload: the reply to the request frame is the typed
/// Layer-2 refusal, never a `Sourcing` go-ahead followed by a truncated stream the client waits on or
/// counts as measured bytes.
#[tokio::test(start_paused = true)]
async fn an_over_cap_source_request_is_refused_before_the_payload() {
    let cap = 8 * 1024u64;
    for request in [
        Request::SpeedSource {
            limit_bytes: Some(cap + 1),
        },
        Request::SpeedBidir {
            limit_bytes: Some(cap + 1),
        },
    ] {
        let (mut client, serving) = serve(capped_at_bytes(cap));
        request
            .write(&mut client)
            .await
            .expect("the request frame fits the stream");
        match Response::read(&mut client)
            .await
            .expect("the refusal frame")
        {
            Response::Unsupported { code, detail } => {
                assert_eq!(code, MethodRefusal::RateLimited);
                assert_eq!(detail.as_str(), SPEED_BYTE_CAP_DETAIL);
            }
            other => {
                panic!("an over-cap request must be refused before the payload, got {other:?}")
            }
        }
        // `refuse` answers with the typed frame, then fails the stream task so the server logs why.
        let result = serving.await.expect("the responder task does not panic");
        assert!(
            matches!(result, Err(ProtocolError::WrongService)),
            "a refused request ends the stream task: {result:?}"
        );
    }
}

/// An over-cap sink ask is refused BEFORE the drain: the responder never reads the payload the client
/// would otherwise send into a stream it stopped reading, and the client reads a typed refusal where it
/// reads the count frame.
#[tokio::test(start_paused = true)]
async fn an_over_cap_sink_request_is_refused_before_the_drain() {
    let cap = 8 * 1024u64;
    let (mut client, serving) = serve(capped_at_bytes(cap));

    Request::SpeedSink {
        limit_bytes: cap + 1,
    }
    .write(&mut client)
    .await
    .expect("the request frame fits the stream");
    match Response::read(&mut client)
        .await
        .expect("the refusal frame")
    {
        Response::Unsupported { code, detail } => {
            assert_eq!(code, MethodRefusal::RateLimited);
            assert_eq!(detail.as_str(), SPEED_BYTE_CAP_DETAIL);
        }
        other => panic!("an over-cap sink must be refused before the drain, got {other:?}"),
    }

    // `refuse` answers with the typed frame, then fails the stream task so the server logs why.
    let result = serving.await.expect("the responder task does not panic");
    assert!(
        matches!(result, Err(ProtocolError::WrongService)),
        "a refused sink ends the stream task: {result:?}"
    );
}

/// The `u64::MAX` ceiling a time-bounded upload sends is not an over-cap ask: it reads as "no exact
/// count", so the drain clamps it to the cap instead (refusing it would refuse every `-t` upload).
#[tokio::test(start_paused = true)]
async fn a_time_bounded_sink_ceiling_is_not_an_over_cap_ask() {
    let cap = 8 * 1024u64;
    let (mut client, serving) = serve(capped_at_bytes(cap));

    Request::SpeedSink {
        limit_bytes: u64::MAX,
    }
    .write(&mut client)
    .await
    .expect("the request frame fits the stream");
    let sent = 4 * 1024u64;
    client
        .write_all(&vec![0u8; sent as usize])
        .await
        .expect("the upload fits the stream");

    let result = serving.await.expect("the responder task does not panic");
    assert!(result.is_ok(), "a clamped sink returns Ok: {result:?}");
    match Response::read(&mut client).await.expect("the count frame") {
        Response::Received { bytes } => assert_eq!(
            bytes, sent,
            "the sink reports the bytes it actually took, not a refusal"
        ),
        other => panic!("a time-bounded ceiling is served, not refused, got {other:?}"),
    }
}

/// A source request exactly at the cap is served: the cap is the largest ask that fits, and a full-size
/// run delivers every byte.
#[tokio::test(start_paused = true)]
async fn a_source_request_at_the_cap_is_served() {
    let cap = 4 * 1024u64;
    let (mut client, serving) = serve(capped_at_bytes(cap));

    Request::SpeedSource {
        limit_bytes: Some(cap),
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
    let mut received = vec![0u8; cap as usize];
    client
        .read_exact(&mut received)
        .await
        .expect("a full-size run delivers every byte");

    let result = serving.await.expect("the responder task does not panic");
    assert!(result.is_ok(), "a request at the cap completes: {result:?}");
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

/// A peer on another wire version is ANSWERED, not dropped: the refusal frame is on the wire before the
/// stream ends, so a dialer reads a sentence instead of a bare EOF. Propagate the read error with `?`
/// ahead of the write, as it used to be, and this goes red waiting for a frame that never comes.
#[tokio::test]
async fn a_version_skewed_peer_is_answered_not_dropped() {
    let (mut client, serving) = serve_ping(PingCaps::default());

    // A well-formed ping frame with one digit of the version changed: `DG03`.
    let mut frame = Vec::new();
    Request::Ping {
        seq: 1,
        sent_unix_nanos: 2,
    }
    .write(&mut frame)
    .await
    .expect("a request frame fits a vec");
    frame[3] = b'3';
    client
        .write_all(&frame)
        .await
        .expect("the frame fits the stream");

    let answer = Response::read(&mut client)
        .await
        .expect("the version answer is a frame, not an EOF");
    let Response::Unsupported { code, detail } = answer else {
        panic!("a frame this build cannot parse is refused, never served: {answer:?}");
    };
    assert_eq!(code, MethodRefusal::WrongMethod);
    assert!(
        detail.as_str().contains("DG03") && detail.as_str().contains("DG02"),
        "{detail}"
    );

    let result = serving.await.expect("the responder task does not panic");
    assert!(
        matches!(result, Err(ProtocolError::Version { .. })),
        "the host still fails the stream and logs why: {result:?}"
    );
}

/// The server halves, held by the responder task past the body so only the body's own `shutdown` can
/// end the client's read. Dropping a half closes the in-memory stream too, which would read as the same
/// clean end with or without a shutdown, so a row that let the halves drop could never go red.
type Held = tokio::task::JoinHandle<(
    Result<(), ProtocolError>,
    io::ReadHalf<DuplexStream>,
    io::WriteHalf<DuplexStream>,
)>;

/// Which entry a held responder runs: the raw door reads the opening frame off the stream, the typed
/// door is handed it already decoded. Both end their stream on their own, so each row runs both.
#[derive(Debug, Clone, Copy)]
enum Door {
    Raw,
    Typed,
}

/// A speed responder on `door` that has been asked `request`, its server halves outliving the body.
async fn open_speed(caps: SpeedCaps, door: Door, request: Request) -> (DuplexStream, Held) {
    let (mut client, server) = io::duplex(CAPACITY);
    let (mut server_read, mut server_write) = io::split(server);
    let task = tokio::spawn(async move {
        let served = match door {
            Door::Raw => answer_speed(&mut server_write, &mut server_read, caps).await,
            Door::Typed => {
                let opening = Opening::Spoken(request);
                respond_speed(opening, &mut server_write, &mut server_read, caps).await
            }
        };
        (served, server_read, server_write)
    });
    if let Door::Raw = door {
        request
            .write(&mut client)
            .await
            .expect("the request frame fits the stream");
    }
    (client, task)
}

/// A ping responder on `door` that has been sent `request`, its server halves outliving the body.
async fn open_ping(caps: PingCaps, door: Door, request: Request) -> (DuplexStream, Held) {
    let (mut client, server) = io::duplex(CAPACITY);
    let (mut server_read, mut server_write) = io::split(server);
    let task = tokio::spawn(async move {
        let served = match door {
            Door::Raw => answer_ping(&mut server_write, &mut server_read, caps).await,
            Door::Typed => {
                let opening = Opening::Spoken(request);
                respond_ping(opening, &mut server_write, &mut server_read, caps).await
            }
        };
        (served, server_read, server_write)
    });
    if let Door::Raw = door {
        request
            .write(&mut client)
            .await
            .expect("the ping frame fits the stream");
    }
    (client, task)
}

/// A raw-door ping responder that has been sent nothing yet, its server halves outliving the body.
fn silent_ping(caps: PingCaps) -> (DuplexStream, Held) {
    let (client, server) = io::duplex(CAPACITY);
    let (mut server_read, mut server_write) = io::split(server);
    let task = tokio::spawn(async move {
        let served = answer_ping(&mut server_write, &mut server_read, caps).await;
        (served, server_read, server_write)
    });
    (client, task)
}

/// A raw-door speed responder that has been sent nothing yet, its server halves outliving the body.
fn silent_speed(caps: SpeedCaps) -> (DuplexStream, Held) {
    let (client, server) = io::duplex(CAPACITY);
    let (mut server_read, mut server_write) = io::split(server);
    let task = tokio::spawn(async move {
        let served = answer_speed(&mut server_write, &mut server_read, caps).await;
        (served, server_read, server_write)
    });
    (client, task)
}

/// Read what is left of the stream, to its end. With the server halves held, the end arrives only
/// through the responder's `shutdown`; a stream left unshut stalls, and the bound turns that stall into
/// the failure.
async fn read_to_the_end(client: &mut DuplexStream) -> Vec<u8> {
    let mut rest = Vec::new();
    time::timeout(Duration::from_secs(5), client.read_to_end(&mut rest))
        .await
        .expect(
            "the responder ends its stream with shutdown, so the peer reads an end, not a stall",
        )
        .expect("a clean end, not a reset");
    rest
}

/// The ping a row opens with.
const PING: Request = Request::Ping {
    seq: 1,
    sent_unix_nanos: 2,
};

/// A ping run the client ends reads a clean end after its last echo, on either door. Drop the
/// `shutdown` on either door's success path and the read stalls behind the held halves.
#[tokio::test(start_paused = true)]
async fn a_finished_ping_run_ends_with_a_clean_end() {
    for door in [Door::Raw, Door::Typed] {
        let (mut client, serving) = open_ping(PingCaps::default(), door, PING).await;
        let pong = Response::read(&mut client).await.expect("the echo");
        assert!(
            matches!(pong, Response::Pong { seq: 1, .. }),
            "{door:?}: {pong:?}"
        );
        client.shutdown().await.expect("the client ends its run");

        let rest = read_to_the_end(&mut client).await;
        assert!(rest.is_empty(), "{door:?}: nothing follows the last echo");
        let (served, _read, _write) = serving.await.expect("the responder task does not panic");
        assert!(served.is_ok(), "{door:?}: {served:?}");
    }
}

/// A finished speed download reads its whole payload and then a clean end, on either door, so a client
/// counting to the end of the stream counts exactly what was sourced.
#[tokio::test(start_paused = true)]
async fn a_finished_speed_run_ends_with_a_clean_end() {
    let request = Request::SpeedSource {
        limit_bytes: Some(4096),
    };
    for door in [Door::Raw, Door::Typed] {
        let (mut client, serving) = open_speed(SpeedCaps::default(), door, request).await;
        let go = Response::read(&mut client).await.expect("the go-ahead");
        assert!(matches!(go, Response::Sourcing), "{door:?}: {go:?}");

        let rest = read_to_the_end(&mut client).await;
        assert_eq!(rest.len(), 4096, "{door:?}: the payload, then the end");
        let (served, _read, _write) = serving.await.expect("the responder task does not panic");
        assert!(served.is_ok(), "{door:?}: {served:?}");
    }
}

/// A stream whose cap fires before its opening frame arrives served nothing, and still ends cleanly:
/// the shutdown runs after the deadline has passed, and one try is all it needs. Drop the `shutdown`
/// on that early return and the read stalls.
#[tokio::test(start_paused = true)]
async fn a_stream_capped_before_its_opening_ends_with_a_clean_end() {
    let cap = Duration::from_secs(1);
    let (mut ping, ping_serving) = silent_ping(PingCaps {
        max_bytes: None,
        max_duration: Some(cap),
    });
    let (mut speed, speed_serving) = silent_speed(capped_at(cap));
    for (name, client, serving) in [
        ("ping", &mut ping, ping_serving),
        ("speed", &mut speed, speed_serving),
    ] {
        assert!(
            read_to_the_end(client).await.is_empty(),
            "{name}: nothing was served"
        );
        let (served, _read, _write) = serving.await.expect("the responder task does not panic");
        assert!(served.is_ok(), "{name}: {served:?}");
    }
}

/// A refusal is a clean end: the peer reads the typed frame and then the end of the stream, while the
/// host still fails the stream task so it logs why. Drop the `shutdown` in `refuse` and the read stalls.
#[tokio::test(start_paused = true)]
async fn a_refusal_ends_with_a_clean_end() {
    let cap = 8 * 1024u64;
    let request = Request::SpeedSource {
        limit_bytes: Some(cap + 1),
    };
    let (mut client, serving) = open_speed(capped_at_bytes(cap), Door::Raw, request).await;
    let refusal = Response::read(&mut client)
        .await
        .expect("the refusal frame");
    assert!(
        matches!(
            refusal,
            Response::Unsupported {
                code: MethodRefusal::RateLimited,
                ..
            }
        ),
        "{refusal:?}"
    );

    assert!(
        read_to_the_end(&mut client).await.is_empty(),
        "nothing follows the refusal"
    );
    let (served, _read, _write) = serving.await.expect("the responder task does not panic");
    assert!(
        matches!(served, Err(ProtocolError::WrongService)),
        "{served:?}"
    );
}

/// The answer to a version-skewed peer is a refusal too, so it ends cleanly behind its frame. Drop the
/// `shutdown` after the answer and the read stalls.
#[tokio::test(start_paused = true)]
async fn a_version_answer_ends_with_a_clean_end() {
    let (mut client, serving) = silent_ping(PingCaps::default());
    let mut frame = Vec::new();
    PING.write(&mut frame)
        .await
        .expect("a request frame fits a vec");
    frame[3] = b'3';
    client
        .write_all(&frame)
        .await
        .expect("the frame fits the stream");
    let answer = Response::read(&mut client)
        .await
        .expect("the version answer");
    assert!(matches!(answer, Response::Unsupported { .. }), "{answer:?}");

    assert!(
        read_to_the_end(&mut client).await.is_empty(),
        "nothing follows the answer"
    );
    let (served, _read, _write) = serving.await.expect("the responder task does not panic");
    assert!(
        matches!(served, Err(ProtocolError::Version { .. })),
        "{served:?}"
    );
}
