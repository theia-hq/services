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

use tokio::io::{self, DuplexStream};
use tokio::task::JoinHandle;
use tokio::time;

use super::{PROBE_TIMEOUT, Ping};
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
/// counted that probe lost, so every reply lands on the read of the probe after it. `delay` must sit
/// just PAST [`PROBE_TIMEOUT`] and well under twice it, or the next probe's own bound fires before the
/// stale pong lands and the run never reaches the check that rejects it.
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

/// A pong that arrives after its probe's bound lands on the next probe's read, where the nonce and
/// sequence check rejects it. It must never be credited to the probe that was in flight when it
/// landed: that would report a peer this slow as one with a sub-millisecond round trip.
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
