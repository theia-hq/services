//! Responder-side bounds: the speed stream's wall-clock cap, proven on a paused clock so the cap fires
//! deterministically instead of after real seconds. The bounded and partial-count paths are driven over
//! one in-memory stream pair, the same [`answer_speed`] body the responder wires.

use core::time::Duration;

use tokio::io::{self, AsyncReadExt as _, AsyncWriteExt as _, DuplexStream};
use tokio::time;

use super::{SpeedCaps, answer_speed};
use crate::protocol::{ProtocolError, Request, Response};

/// The in-memory stream capacity. Small, so an unbounded source fills it and parks on backpressure.
const CAPACITY: usize = 1024;

/// The responder task's join handle: [`answer_speed`] driving the server halves.
type Serving = tokio::task::JoinHandle<Result<(), ProtocolError>>;

/// Caps that never end a run on bytes, so the wall clock is the only terminator under test.
fn capped_at(duration: Duration) -> SpeedCaps {
    SpeedCaps {
        max_bytes: Some(u64::MAX),
        max_duration: Some(duration),
    }
}

/// An in-memory stream pair with the responder already running on the server end.
fn serve(caps: SpeedCaps) -> (DuplexStream, Serving) {
    let (client, server) = io::duplex(CAPACITY);
    let (mut server_read, mut server_write) = io::split(server);
    let task =
        tokio::spawn(async move { answer_speed(&mut server_write, &mut server_read, caps).await });
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
