//! Client-side bounds on a byte-bounded run: when the peer stops before the asked bytes, the run
//! ends with the typed [`ProtocolError::EndedEarly`] instead of parking or reporting a short count.
//! Driven over an in-memory duplex stream on a paused clock, so the stall bound fires
//! deterministically rather than after real seconds.

use core::time::Duration;
use std::time::Instant;

use bifrost::RefusalDetail;
use tokio::io::{self, AsyncReadExt as _, AsyncWriteExt as _, ReadHalf};
use tokio::time;

use super::{Limit, Mode, STALL_BOUND, Speedtest, bidir, download, upload};
use crate::protocol::{MethodRefusal, ProtocolError, Refusal, Request, Response};

/// A stalled byte-bounded download must end at the stall bound with the typed error, not park: the shape
/// a live run takes when a metered source stops at its lifetime cap and the close never reaches the
/// client.
#[tokio::test(start_paused = true)]
async fn a_stalled_download_ends_early_instead_of_parking() {
    let asked = 8 * 1024u64;
    let (client, server) = io::duplex(1024);
    let (mut client_read, mut client_write) = io::split(client);
    let (mut server_read, mut server_write) = io::split(server);
    let serving = tokio::spawn(async move {
        let _ = Request::read(&mut server_read).await;
        Response::Sourcing.write(&mut server_write).await?;
        server_write.write_all(&[0u8; 4 * 1024]).await?;
        // Hold the stream open: the peer stopped, but no close arrives.
        core::future::pending::<()>().await;
        Ok::<(), ProtocolError>(())
    });

    let started = time::Instant::now();
    let outcome = download(
        &mut client_write,
        &mut client_read,
        Limit::ByBytes(asked),
        Instant::now(),
        None,
    )
    .await;
    assert!(
        matches!(
            outcome,
            Err(ProtocolError::EndedEarly {
                moved: 4096,
                asked: 8192
            })
        ),
        "a stalled byte-bounded download must surface the typed short error: {outcome:?}"
    );
    assert!(
        started.elapsed() >= STALL_BOUND,
        "the stall bound, not an early close, must end the run"
    );
    serving.abort();
}

/// A clean early close (no stall) is short for the same reason: the peer stopped before the ask, so
/// the run is the typed error, never a short throughput that reads as measured.
#[tokio::test(start_paused = true)]
async fn a_clean_short_close_ends_early_not_as_a_smaller_throughput() {
    let asked = 8 * 1024u64;
    let (client, server) = io::duplex(1024);
    let (mut client_read, mut client_write) = io::split(client);
    let (mut server_read, mut server_write) = io::split(server);
    let serving = tokio::spawn(async move {
        let _ = Request::read(&mut server_read).await;
        Response::Sourcing.write(&mut server_write).await?;
        server_write.write_all(&[0u8; 4 * 1024]).await?;
        // A clean close: the client reads EOF, not a stall.
        drop(server_write);
        Ok::<(), ProtocolError>(())
    });

    let outcome = download(
        &mut client_write,
        &mut client_read,
        Limit::ByBytes(asked),
        Instant::now(),
        None,
    )
    .await;
    assert!(
        matches!(
            outcome,
            Err(ProtocolError::EndedEarly {
                moved: 4096,
                asked: 8192
            })
        ),
        "a clean short close must be named as the stream ending early: {outcome:?}"
    );
    serving
        .await
        .expect("the server task does not panic")
        .expect("the server wrote its half");
}

/// A sink that confirms fewer bytes than the ask (the metered sink stopped at its lifetime cap and
/// replied with the count it took) must surface the typed error, not exit 0 with a short upload.
#[tokio::test(start_paused = true)]
async fn a_short_sink_reply_ends_early_not_as_a_short_upload() {
    let asked = 8 * 1024u64;
    let (client, server) = io::duplex(1024);
    let (mut client_read, mut client_write) = io::split(client);
    let (mut server_read, mut server_write) = io::split(server);
    let serving = tokio::spawn(async move {
        let request = Request::read(&mut server_read).await?;
        assert!(
            matches!(request, Request::SpeedSink { limit_bytes } if limit_bytes == asked),
            "the client asks the sink for its byte count"
        );
        let mut taken = [0u8; 4 * 1024];
        server_read.read_exact(&mut taken).await?;
        Response::Received { bytes: 4 * 1024 }
            .write(&mut server_write)
            .await?;
        Ok::<(), ProtocolError>(())
    });

    let outcome = upload(
        &mut client_write,
        &mut client_read,
        Limit::ByBytes(asked),
        Instant::now(),
        None,
    )
    .await;
    assert!(
        matches!(
            outcome,
            Err(ProtocolError::EndedEarly {
                moved: 4096,
                asked: 8192
            })
        ),
        "a short sink reply must surface the typed error: {outcome:?}"
    );
    serving
        .await
        .expect("the server task does not panic")
        .expect("the server wrote its half");
}

/// A sink that stops reading and never replies parks the client's send on backpressure; the stall
/// bound ends the run with the typed error, and the count is what the client wrote before it.
#[tokio::test(start_paused = true)]
async fn a_stalled_upload_ends_early_instead_of_parking() {
    let asked = 512 * 1024u64;
    let (client, server) = io::duplex(128 * 1024);
    let (mut client_read, mut client_write) = io::split(client);
    let (mut server_read, server_write) = io::split(server);
    let serving = tokio::spawn(async move {
        let _ = Request::read(&mut server_read).await;
        // Hold the write half open (no close) and stop reading (no reply): the peer went silent.
        let _hold = server_write;
        core::future::pending::<()>().await;
        Ok::<(), ProtocolError>(())
    });

    let outcome = upload(
        &mut client_write,
        &mut client_read,
        Limit::ByBytes(asked),
        Instant::now(),
        None,
    )
    .await;
    match outcome {
        Err(ProtocolError::EndedEarly { moved, asked: a }) => {
            assert_eq!(a, asked);
            assert!(
                moved > 0 && moved < asked,
                "a stall stop is a short, counted run, moved {moved}"
            );
        }
        other => panic!("a stalled upload must end early with the typed error, got {other:?}"),
    }
    serving.abort();
}

/// A bidir run whose peer stops both directions parks both legs; the run ends at the stall bound
/// with the typed error, never a pair of partial throughputs.
#[tokio::test(start_paused = true)]
async fn a_stalled_bidir_ends_early_instead_of_parking() {
    let asked = 8 * 1024u64;
    let (client, server) = io::duplex(1024);
    let (mut client_read, mut client_write) = io::split(client);
    let (mut server_read, mut server_write) = io::split(server);
    let serving = tokio::spawn(async move {
        let _ = Request::read(&mut server_read).await;
        Response::Sourcing.write(&mut server_write).await?;
        server_write.write_all(&[0u8; 4 * 1024]).await?;
        core::future::pending::<()>().await;
        Ok::<(), ProtocolError>(())
    });

    let outcome = bidir(
        &mut client_write,
        &mut client_read,
        Limit::ByBytes(asked),
        Instant::now(),
        None,
    )
    .await;
    assert!(
        matches!(
            outcome,
            Err(ProtocolError::EndedEarly {
                moved: 4096,
                asked: 8192
            })
        ),
        "a stalled bidir run must surface the typed error: {outcome:?}"
    );
    serving.abort();
}

/// A time-bounded drain that closes early is not a shortfall: the run asked for a window, not a
/// count, so the bytes that arrived are the result.
#[tokio::test(start_paused = true)]
async fn a_time_bounded_short_close_is_not_a_shortfall() {
    let (client, server) = io::duplex(1024);
    let (mut client_read, mut client_write) = io::split(client);
    let (mut server_read, mut server_write) = io::split(server);
    let serving = tokio::spawn(async move {
        let _ = Request::read(&mut server_read).await;
        Response::Sourcing.write(&mut server_write).await?;
        server_write.write_all(&[0u8; 4 * 1024]).await?;
        drop(server_write);
        Ok::<(), ProtocolError>(())
    });

    let received = download(
        &mut client_write,
        &mut client_read,
        Limit::ByTime(Duration::from_secs(5)),
        Instant::now(),
        None,
    )
    .await;
    assert_eq!(
        received.expect("a time-bounded run reports the bytes that arrived"),
        4 * 1024
    );
    serving
        .await
        .expect("the server task does not panic")
        .expect("the server wrote its half");
}

/// How long a responder waits to read a failed run's clean end. The clock is paused, so this fires
/// only once every task is parked: the end never came.
const END_BOUND: Duration = Duration::from_secs(1);

/// The responder's side of a run's stream: it must read to a clean end, never park.
///
/// The double is a split duplex, whose write half closes nothing when dropped while the run's caller
/// still holds the read half. Each row's caller holds both halves past the run, so the end arrives
/// only if the run itself shut its writer: a run that left it unshut parks this read until the bound.
async fn reads_a_clean_end(mut server_read: ReadHalf<io::DuplexStream>) {
    let mut rest = Vec::new();
    time::timeout(END_BOUND, server_read.read_to_end(&mut rest))
        .await
        .expect("a run must end its write half, not leave the responder parked")
        .expect("a run's stream ends cleanly, never with an error");
}

/// A refused upload ends its stream: the responder that wrote the typed refusal reads the payload
/// already sent, then a clean end.
#[tokio::test(start_paused = true)]
async fn a_refused_run_still_ends_its_stream_cleanly() {
    let (client, server) = io::duplex(1024);
    let (mut client_read, mut client_write) = io::split(client);
    let (mut server_read, mut server_write) = io::split(server);
    let serving = tokio::spawn(async move {
        let _ = Request::read(&mut server_read).await;
        Response::Unsupported {
            code: MethodRefusal::Busy,
            detail: RefusalDetail::bounded("a transfer slot is busy"),
        }
        .write(&mut server_write)
        .await?;
        reads_a_clean_end(server_read).await;
        Ok::<(), ProtocolError>(())
    });

    let outcome = Speedtest::new(Mode::Up, Limit::ByBytes(64 * 1024))
        .over(&mut client_write, &mut client_read)
        .await;
    assert!(
        matches!(outcome, Err(ProtocolError::Refused(Refusal::Method { .. }))),
        "a refused upload surfaces the typed refusal: {outcome:?}"
    );
    serving
        .await
        .expect("the responder reads a clean end")
        .expect("the responder wrote its refusal");
}

/// An upload the sink answered early with a short count ends its stream: the responder reads a
/// clean end after its `Received`, not a stream left open behind the typed error.
#[tokio::test(start_paused = true)]
async fn an_early_received_still_ends_its_stream_cleanly() {
    let asked = 8 * 1024u64;
    let (client, server) = io::duplex(1024);
    let (mut client_read, mut client_write) = io::split(client);
    let (mut server_read, mut server_write) = io::split(server);
    let serving = tokio::spawn(async move {
        let _ = Request::read(&mut server_read).await;
        let mut taken = [0u8; 4 * 1024];
        server_read.read_exact(&mut taken).await?;
        Response::Received { bytes: 4 * 1024 }
            .write(&mut server_write)
            .await?;
        reads_a_clean_end(server_read).await;
        Ok::<(), ProtocolError>(())
    });

    let outcome = Speedtest::new(Mode::Up, Limit::ByBytes(asked))
        .over(&mut client_write, &mut client_read)
        .await;
    assert!(
        matches!(outcome, Err(ProtocolError::EndedEarly { .. })),
        "an early short count surfaces the typed error: {outcome:?}"
    );
    serving
        .await
        .expect("the responder reads a clean end")
        .expect("the responder wrote its count");
}

/// A download that came up short ends its stream: the client errors before the shutdown a full run
/// reaches, and the responder still reads a clean end.
#[tokio::test(start_paused = true)]
async fn a_short_download_still_ends_its_stream_cleanly() {
    let (client, server) = io::duplex(1024);
    let (mut client_read, mut client_write) = io::split(client);
    let (mut server_read, mut server_write) = io::split(server);
    let serving = tokio::spawn(async move {
        let _ = Request::read(&mut server_read).await;
        Response::Sourcing.write(&mut server_write).await?;
        server_write.write_all(&[0u8; 4 * 1024]).await?;
        server_write.shutdown().await?;
        reads_a_clean_end(server_read).await;
        Ok::<(), ProtocolError>(())
    });

    let outcome = Speedtest::new(Mode::Down, Limit::ByBytes(8 * 1024))
        .over(&mut client_write, &mut client_read)
        .await;
    assert!(
        matches!(
            outcome,
            Err(ProtocolError::EndedEarly {
                moved: 4096,
                asked: 8192
            })
        ),
        "a short download surfaces the typed error: {outcome:?}"
    );
    serving
        .await
        .expect("the responder reads a clean end")
        .expect("the responder sourced its bytes");
}

/// A time-bounded upload the sink answered early is a result, not an error, and it still ends its
/// stream: the responder reads a clean end after its `Received`.
#[tokio::test(start_paused = true)]
async fn an_early_answer_to_a_timed_upload_still_ends_its_stream_cleanly() {
    let (client, server) = io::duplex(1024);
    let (mut client_read, mut client_write) = io::split(client);
    let (mut server_read, mut server_write) = io::split(server);
    let serving = tokio::spawn(async move {
        let _ = Request::read(&mut server_read).await;
        let mut taken = [0u8; 4 * 1024];
        server_read.read_exact(&mut taken).await?;
        Response::Received { bytes: 4 * 1024 }
            .write(&mut server_write)
            .await?;
        reads_a_clean_end(server_read).await;
        Ok::<(), ProtocolError>(())
    });

    let outcome = Speedtest::new(Mode::Up, Limit::ByTime(Duration::from_secs(5)))
        .over(&mut client_write, &mut client_read)
        .await;
    assert_eq!(
        outcome
            .expect("a timed upload reports the count the sink confirmed")
            .up()
            .map(|up| up.bytes()),
        Some(4 * 1024)
    );
    serving
        .await
        .expect("the responder reads a clean end")
        .expect("the responder wrote its count");
}
