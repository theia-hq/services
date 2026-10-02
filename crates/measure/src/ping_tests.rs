//! The client's own terminator: a peer that admits the stream and then stops answering is counted as
//! lost probes rather than waited on. Driven over an in-memory duplex on a paused clock, so the probe
//! bound fires deterministically rather than after real seconds, and each run is wrapped in a
//! [`HANG_BOUND`] far above it, so deleting the guard fails the test instead of hanging the suite.
//!
//! What these cannot observe: a duplex is not a link. There is no flow control, no relay and no NAT
//! here, so nothing below proves what an honest round trip costs on a real path or that ten seconds is
//! the right patience for one. They prove only that the client ends a wait no peer will end for it,
//! and that it books the result as loss. A peer that stops READING (rather than stops answering) is
//! also not observable here, because a duplex takes every fixed-width request frame without parking
//! the writer; the bound covers the write half too, but nothing in this file exercises that.

use core::time::Duration;

use tokio::io::{self, AsyncWriteExt as _, DuplexStream};
use tokio::task::JoinHandle;
use tokio::time;

use super::{PROBE_TIMEOUT, Ping, PingReport};
use crate::protocol::{Request, Response};

/// The bound each run is wrapped in, so a missing [`PROBE_TIMEOUT`] surfaces as a failed test rather
/// than a hung suite. It is far above every bound the client applies and costs nothing on a paused
/// clock, so it can fire only when nothing inside the client ends the wait.
const HANG_BOUND: Duration = Duration::from_secs(600);

/// Serve probes over `server`, echoing exactly the sequence numbers `answered` names and silently
/// swallowing the rest: the wedged peer this module exists to bound. The stream is never closed and no
/// frame is ever malformed, so an unanswered probe reaches the client as nothing at all, which is the
/// point: on a reliable stream, silence is the one failure the stream itself cannot report.
fn answering(server: DuplexStream, answered: &'static [u32]) -> JoinHandle<()> {
    tokio::spawn(async move {
        let (mut reader, mut writer) = io::split(server);
        while let Ok(Request::Ping {
            seq,
            sent_unix_nanos,
        }) = Request::read(&mut reader).await
        {
            if !answered.contains(&seq) {
                continue;
            }
            let pong = Response::Pong {
                seq,
                sent_unix_nanos,
            };
            if pong.write(&mut writer).await.is_err() {
                return;
            }
        }
    })
}

/// Serve probes, but only after `delay`: the peer whose pong arrives once the client has already
/// counted that probe lost, so every reply lands during the wait of the probe after it. `delay` must
/// sit just PAST [`PROBE_TIMEOUT`] and well under twice it, or the next probe's own bound fires before
/// the stale pong lands and the run never reaches the reader that skips it.
fn answering_after(server: DuplexStream, delay: Duration) -> JoinHandle<()> {
    tokio::spawn(async move {
        let (mut reader, mut writer) = io::split(server);
        while let Ok(Request::Ping {
            seq,
            sent_unix_nanos,
        }) = Request::read(&mut reader).await
        {
            time::sleep(delay).await;
            let pong = Response::Pong {
                seq,
                sent_unix_nanos,
            };
            if pong.write(&mut writer).await.is_err() {
                return;
            }
        }
    })
}

/// The defect: a peer that admits the stream and then goes quiet held the client for ever. The run must
/// come back, and it must come back as the total loss a person already reads as "it stopped answering",
/// not as an error and not as a measurement.
#[tokio::test(start_paused = true)]
async fn a_peer_that_admits_and_then_goes_quiet_is_counted_as_loss_not_waited_on() {
    let (client, server) = io::duplex(1024);
    let (mut client_read, mut client_write) = io::split(client);
    let serving = answering(server, &[]);

    let started = time::Instant::now();
    let mut seen = Vec::new();
    let plan = Ping {
        count: 3,
        interval: Duration::ZERO,
    };
    let report = time::timeout(
        HANG_BOUND,
        plan.probes(&mut client_write, &mut client_read, |probe| {
            seen.push((probe.seq, probe.rtt))
        }),
    )
    .await
    .expect("the client's own probe bound, not the suite's, must end a run against a silent peer")
    .expect("a peer that says nothing is loss, never a protocol error: nothing failed on the wire");

    assert!(
        report.min().is_none()
            && report.avg().is_none()
            && report.max().is_none()
            && report.mdev().is_none(),
        "no reply arrived, so the run has no round trip to report: {report:?}"
    );
    assert!(
        seen.iter().all(|(_, rtt)| rtt.is_none()),
        "no probe may be observed as measured when none was answered: {seen:?}"
    );
    assert_eq!(report.sent(), 3);
    assert_eq!(report.received(), 0, "the peer answered nothing");
    assert_eq!(report.loss(), 1.0, "every probe is a lost probe");
    assert!(
        started.elapsed() >= PROBE_TIMEOUT,
        "each probe waits its bound before it is written off"
    );

    serving.abort();
}

/// The control: the bound must not fire on a peer that answers. A responsive run reports no loss and
/// spends none of its patience, so the fix costs a healthy peer nothing.
#[tokio::test(start_paused = true)]
async fn a_responsive_peer_spends_none_of_the_probe_bound_and_reports_no_loss() {
    let (client, server) = io::duplex(1024);
    let (mut client_read, mut client_write) = io::split(client);
    let serving = answering(server, &[0, 1, 2]);

    let started = time::Instant::now();
    let mut seen = Vec::new();
    let plan = Ping {
        count: 3,
        interval: Duration::ZERO,
    };
    let report = time::timeout(
        HANG_BOUND,
        plan.probes(&mut client_write, &mut client_read, |probe| {
            seen.push((probe.seq, probe.rtt.is_some()))
        }),
    )
    .await
    .expect("a responsive run must not reach the suite's bound")
    .expect("an answered run is not an error");

    assert!(
        started.elapsed() < PROBE_TIMEOUT,
        "a peer that answers must never wait on the bound"
    );
    assert_eq!(report.loss(), 0.0, "every probe was answered");
    assert_eq!(seen, vec![(0, true), (1, true), (2, true)]);

    serving.abort();
}

/// The case that proves the counting rather than a special case for "all failed": a peer that answers
/// some probes and not others reports exactly the probes it dropped, and still reports the round trips
/// of the ones it answered.
#[tokio::test(start_paused = true)]
async fn a_peer_that_answers_some_probes_reports_exactly_those_it_did_not() {
    let (client, server) = io::duplex(1024);
    let (mut client_read, mut client_write) = io::split(client);
    let serving = answering(server, &[0, 2]);

    let mut seen = Vec::new();
    let plan = Ping {
        count: 4,
        interval: Duration::ZERO,
    };
    let report = time::timeout(
        HANG_BOUND,
        plan.probes(&mut client_write, &mut client_read, |probe| {
            seen.push((probe.seq, probe.rtt.is_some()))
        }),
    )
    .await
    .expect("an unanswered probe must not park a run whose other probes came back")
    .expect("a partly answered run is not an error");

    assert!(
        report.loss() > 0.0 && report.loss() < 1.0,
        "a mixed run must collapse to neither extreme: {}",
        report.loss()
    );
    assert_eq!(
        seen,
        vec![(0, true), (1, false), (2, true), (3, false)],
        "each probe is booked by what that probe did, not by what the run did"
    );
    assert_eq!(report.sent(), 4);
    assert_eq!(report.received(), 2);
    assert_eq!(report.loss(), 0.5);
    assert!(
        report.avg().is_some(),
        "the probes that came back are still measurements"
    );

    serving.abort();
}

/// A pong that arrives after its probe's bound lands during the next probe's wait, where the reader
/// skips it. It must never be credited to the probe that was in flight when it landed: that would
/// report a peer this slow as one with a sub-millisecond round trip.
#[tokio::test(start_paused = true)]
async fn a_pong_that_arrives_after_its_bound_is_never_credited_to_the_next_probe() {
    let (client, server) = io::duplex(1024);
    let (mut client_read, mut client_write) = io::split(client);
    let serving = answering_after(server, PROBE_TIMEOUT + Duration::from_secs(1));

    let plan = Ping {
        count: 2,
        interval: Duration::ZERO,
    };
    let report = time::timeout(
        HANG_BOUND,
        plan.probes(&mut client_write, &mut client_read, |_probe| {}),
    )
    .await
    .expect("a peer answering past the bound must not park the run")
    .expect("a stale reply is loss, not a run-ending error");

    assert_eq!(
        report.received(),
        0,
        "a reply to an already-lost probe measures nothing"
    );
    assert!(
        report.avg().is_none(),
        "a stale pong must not become a round-trip time: {report:?}"
    );
    assert_eq!(report.loss(), 1.0);

    serving.abort();
}

/// Serve probes, answering probe 0 only after `first` and every later one after `rest`: a peer with one
/// slow reply on an otherwise steady run. The pong for 0 still arrives, during a later probe's wait,
/// which is the case the reader must skip without booking that later probe lost.
fn answering_late_once(server: DuplexStream, first: Duration, rest: Duration) -> JoinHandle<()> {
    tokio::spawn(async move {
        let (mut reader, mut writer) = io::split(server);
        while let Ok(Request::Ping {
            seq,
            sent_unix_nanos,
        }) = Request::read(&mut reader).await
        {
            time::sleep(if seq == 0 { first } else { rest }).await;
            let pong = Response::Pong {
                seq,
                sent_unix_nanos,
            };
            if pong.write(&mut writer).await.is_err() {
                return;
            }
        }
    })
}

/// The defect: one late pong booked every later probe lost, because each probe read the reply meant
/// for the one before it. Six probes, the first answered at 11 s and the rest at once, were answered
/// five times, and the run must say five.
#[tokio::test(start_paused = true)]
async fn a_late_pong_leaves_every_later_probe_answered() {
    let (client, server) = io::duplex(1024);
    let (mut client_read, mut client_write) = io::split(client);
    let serving = answering_late_once(server, Duration::from_secs(11), Duration::ZERO);

    let mut seen = Vec::new();
    let plan = Ping {
        count: 6,
        interval: Duration::from_secs(1),
    };
    let report = time::timeout(
        HANG_BOUND,
        plan.probes(&mut client_write, &mut client_read, |probe| {
            seen.push((probe.seq, probe.rtt.is_some()))
        }),
    )
    .await
    .expect("one slow reply must not park the run")
    .expect("a late reply is loss for its own probe, not a run-ending error");

    assert_eq!(
        seen,
        vec![
            (0, false),
            (1, true),
            (2, true),
            (3, true),
            (4, true),
            (5, true)
        ],
        "only the probe whose reply came past its bound is unanswered"
    );
    assert_eq!(report.sent(), 6);
    assert_eq!(report.received(), 5, "five of six probes were answered");

    serving.abort();
}

/// The reader's second fault: a bound that drops a reply read between its fields leaves the next read
/// starting inside that frame, so every later reply is misframed. Here probe 0's pong tag lands inside
/// its bound and its fields after it; probe 1, answered at once, must still be read and matched.
#[tokio::test(start_paused = true)]
async fn a_bound_that_lands_mid_frame_leaves_the_next_probe_matched() {
    let (client, server) = io::duplex(1024);
    let (mut client_read, mut client_write) = io::split(client);
    let serving = tokio::spawn(async move {
        let (mut reader, mut writer) = io::split(server);
        let Ok(Request::Ping {
            seq,
            sent_unix_nanos,
        }) = Request::read(&mut reader).await
        else {
            return;
        };
        let mut torn = Vec::new();
        Response::Pong {
            seq,
            sent_unix_nanos,
        }
        .write(&mut torn)
        .await
        .expect("a pong encodes into memory");
        let (tag, fields) = torn.split_at(1);

        time::sleep(PROBE_TIMEOUT / 2).await;
        writer.write_all(tag).await.expect("the client is reading");
        writer.flush().await.expect("the client is reading");
        time::sleep(PROBE_TIMEOUT).await;
        writer
            .write_all(fields)
            .await
            .expect("the client is reading");

        while let Ok(Request::Ping {
            seq,
            sent_unix_nanos,
        }) = Request::read(&mut reader).await
        {
            let pong = Response::Pong {
                seq,
                sent_unix_nanos,
            };
            if pong.write(&mut writer).await.is_err() {
                return;
            }
        }
    });

    let mut seen = Vec::new();
    let plan = Ping {
        count: 2,
        interval: Duration::ZERO,
    };
    let report = time::timeout(
        HANG_BOUND,
        plan.probes(&mut client_write, &mut client_read, |probe| {
            seen.push((probe.seq, probe.rtt.is_some()))
        }),
    )
    .await
    .expect("a torn reply must not park the run")
    .expect("a reply torn across a bound is loss, not a run-ending error");

    assert_eq!(
        seen,
        vec![(0, false), (1, true)],
        "the reply after a torn one is read whole and matched"
    );
    assert_eq!(report.received(), 1);

    serving.abort();
}

/// A late pong that the reader skips adds no sample. Probe 0 is answered at 11 s, past its bound, and
/// probe 1 three seconds after it was sent; the one round trip in the report is probe 1's 3 s. Had the
/// late pong been credited as it arrived, the run would hold a 1 s sample (probe 1 sent at 10 s, pong
/// 0 in at 11 s) or an 11 s one.
#[tokio::test(start_paused = true)]
async fn a_skipped_late_pong_is_never_an_rtt() {
    let (client, server) = io::duplex(1024);
    let (mut client_read, mut client_write) = io::split(client);
    let serving = answering_late_once(server, Duration::from_secs(11), Duration::from_secs(2));

    let plan = Ping {
        count: 2,
        interval: Duration::ZERO,
    };
    let report = time::timeout(
        HANG_BOUND,
        plan.probes(&mut client_write, &mut client_read, |_probe| {}),
    )
    .await
    .expect("one slow reply must not park the run")
    .expect("a late reply is loss for its own probe, not a run-ending error");

    let probe_one = Duration::from_secs(3);
    assert_eq!(report.received(), 1, "only probe 1 was answered in time");
    assert_eq!(
        (report.min(), report.max()),
        (Some(probe_one), Some(probe_one)),
        "the only sample is probe 1's own round trip: {report:?}"
    );

    serving.abort();
}

/// `mdev` is the standard deviation `ping(8)` prints under that label, not the mean absolute
/// deviation. One slow sample among three steady ones is where the two part: 17.3 ms against 15.0.
#[test]
fn mdev_is_the_standard_deviation() {
    let report = PingReport {
        sent: 4,
        rtts: [13.0, 13.0, 13.1, 53.1]
            .map(|ms| Duration::from_secs_f64(ms / 1000.0))
            .to_vec(),
    };

    let mdev_ms = report
        .mdev()
        .expect("four samples have a deviation")
        .as_secs_f64()
        * 1000.0;
    assert!(
        (mdev_ms - 17.349).abs() < 0.001,
        "ping(8) prints 17.349 ms for these samples, this printed {mdev_ms}"
    );
}

/// A peer that reads every probe but answers with `reply` once, then nothing; what it does to its own
/// half after that one write is `then`'s.
fn answering_once_with(server: DuplexStream, reply: &'static [u8], then: Then) -> JoinHandle<()> {
    tokio::spawn(async move {
        let (mut reader, mut writer) = io::split(server);
        let mut replied = false;
        while Request::read(&mut reader).await.is_ok() {
            if replied {
                continue;
            }
            replied = true;
            if writer.write_all(reply).await.is_err() {
                return;
            }
            if matches!(then, Then::Finish) && writer.shutdown().await.is_err() {
                return;
            }
        }
    })
}

/// What [`answering_once_with`]'s peer does with its send half after its one write.
#[derive(Clone, Copy)]
enum Then {
    /// Leave it open and say nothing more.
    KeepOpen,
    /// Finish it, so the client reads end of stream, while still reading probes.
    Finish,
}

/// Drive a run of `count` probes, one second apart, against `serving`'s peer, and return the report
/// with what each probe was booked as.
async fn booked(
    client: DuplexStream,
    serving: JoinHandle<()>,
    count: u32,
) -> (PingReport, Vec<(u32, bool)>) {
    let (mut client_read, mut client_write) = io::split(client);
    let mut seen = Vec::new();
    let plan = Ping {
        count,
        interval: Duration::from_secs(1),
    };
    let report = time::timeout(
        HANG_BOUND,
        plan.probes(&mut client_write, &mut client_read, |probe| {
            seen.push((probe.seq, probe.rtt.is_some()))
        }),
    )
    .await
    .expect("a reply stream that ended must not park the run")
    .expect("a reply stream that ended is loss, not a run-ending error");
    serving.abort();
    (report, seen)
}

/// A reply stream the peer ends, here with one byte that is no reply's tag, stays ended: the run
/// finishes and books every probe from the failure on as lost. Four probes, because each probe after
/// the end reads again, and the second read past the end is where a stream that is not safe to poll
/// past its end panics.
#[tokio::test(start_paused = true)]
async fn a_reply_that_fails_to_decode_books_every_later_probe_lost() {
    let (client, server) = io::duplex(1024);
    let serving = answering_once_with(server, &[0xEE], Then::KeepOpen);

    let (report, seen) = booked(client, serving, 4).await;

    assert_eq!(seen, vec![(0, false), (1, false), (2, false), (3, false)]);
    assert_eq!((report.sent(), report.received()), (4, 0));
}

/// The same for the ordinary way a reply stream ends: the peer finishes its send half and goes on
/// reading. Every probe is booked lost and the run still returns its report.
#[tokio::test(start_paused = true)]
async fn a_peer_that_finishes_its_replies_books_every_later_probe_lost() {
    let (client, server) = io::duplex(1024);
    let serving = answering_once_with(server, &[], Then::Finish);

    let (report, seen) = booked(client, serving, 3).await;

    assert_eq!(seen, vec![(0, false), (1, false), (2, false)]);
    assert_eq!((report.sent(), report.received()), (3, 0));
}

/// What a forging peer does to a pong's `(seq, nonce)` before it sends it.
type Forge = fn(u32, u64) -> (u32, u64);

/// Serve probes, answering probe 0 with `forge` applied to its pong and every later one as sent: a pong
/// that answers nothing the client asked.
fn answering_first_forged(server: DuplexStream, forge: Forge) -> JoinHandle<()> {
    tokio::spawn(async move {
        let (mut reader, mut writer) = io::split(server);
        while let Ok(Request::Ping {
            seq,
            sent_unix_nanos,
        }) = Request::read(&mut reader).await
        {
            let (seq, sent_unix_nanos) = if seq == 0 {
                forge(seq, sent_unix_nanos)
            } else {
                (seq, sent_unix_nanos)
            };
            let pong = Response::Pong {
                seq,
                sent_unix_nanos,
            };
            if pong.write(&mut writer).await.is_err() {
                return;
            }
        }
    })
}

/// A pong for a later probe, or for this probe with a nonce it did not send, is a lost probe at once:
/// never credited, and never skipped as late, which would hold the probe to its bound. Probe 1 is still
/// answered, so one bad pong costs one probe.
#[tokio::test(start_paused = true)]
async fn a_pong_for_a_later_probe_or_a_foreign_nonce_is_lost_at_once() {
    let forgeries: [(&str, Forge); 2] = [
        ("a later seq", |seq, nonce| (seq + 1, nonce)),
        ("a foreign nonce", |seq, nonce| (seq, !nonce)),
    ];
    for (forgery, forge) in forgeries {
        let (client, server) = io::duplex(1024);
        let (mut client_read, mut client_write) = io::split(client);
        let serving = answering_first_forged(server, forge);

        let started = time::Instant::now();
        let mut seen = Vec::new();
        let plan = Ping {
            count: 2,
            interval: Duration::ZERO,
        };
        let report = time::timeout(
            HANG_BOUND,
            plan.probes(&mut client_write, &mut client_read, |probe| {
                seen.push((probe.seq, probe.rtt.is_some()))
            }),
        )
        .await
        .expect("a forged pong must not park the run")
        .expect("a forged pong is a lost probe, not a run-ending error");

        assert_eq!(
            seen,
            vec![(0, false), (1, true)],
            "{forgery}: probe 0 is lost and probe 1 answered"
        );
        assert_eq!(report.received(), 1, "{forgery}");
        assert!(
            started.elapsed() < PROBE_TIMEOUT,
            "{forgery}: a pong that answers nothing ends its probe at once, not at the bound"
        );

        serving.abort();
    }
}
