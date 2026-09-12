//! Reach diagnostics between two in-process nodes: one ping, one speed test, no sockets.
//!
//! One node runs [`Responder`], which answers both diagnostics on every stream the client opens; the
//! other node is the client, pinging the responder and then running one speed test against it. The
//! transport is bifrost's in-process `MemTransport`, so the whole exchange happens in one process. The
//! same client and responder run unchanged over iroh or quirk.
//!
//! Run it:
//!
//! ```sh
//! cargo run --example reach
//! ```

use core::time::Duration;

use bifrost::{NoDiscovery, Node};
use bifrost_mem::MemTransport;
use measure::{Limit, Mode, Ping, Responder, Speedtest};

/// One mebibyte, so byte counts print as the unit the throughput uses.
const MIB: f64 = 1024.0 * 1024.0;

/// Bytes per direction in the speed run.
const SPEED_BYTES: u64 = 4 * 1024 * 1024;

#[tokio::main]
async fn main() -> Result<(), Box<dyn core::error::Error>> {
    // Two nodes in one process: the responder answers diagnostics, the client runs them against it.
    let responder = Node::new(MemTransport::bind(), NoDiscovery);
    let responder_id = responder.node_id();
    let client = Node::new(MemTransport::bind(), NoDiscovery);

    // Serve the accepted session in the background until the client drops it.
    let _serving = tokio::spawn(async move {
        if let Ok(session) = responder.accept().await {
            Responder::serve(session).await;
        }
    });

    let session = client.connect(responder_id).await?;

    // One ping run: three probes, 50 ms apart.
    let ping = Ping {
        count: 3,
        interval: Duration::from_millis(50),
    }
    .run(&session)
    .await?;
    println!(
        "ping: {} sent, {} received, {:.0}% loss, rtt min {} avg {} max {} mdev {}",
        ping.sent(),
        ping.received(),
        ping.loss() * 100.0,
        show(ping.min()),
        show(ping.avg()),
        show(ping.max()),
        show(ping.mdev()),
    );

    // One speed run: both directions at once, one run over one stream.
    let speed = Speedtest::new(Mode::Bidir, Limit::ByBytes(SPEED_BYTES))
        .run(&session)
        .await?;
    let elapsed_ms = speed.elapsed().as_secs_f64() * 1e3;
    for (label, leg) in [("up", speed.up()), ("down", speed.down())] {
        if let Some(leg) = leg {
            println!(
                "speed {label}: {:.2} MiB in {elapsed_ms:.1} ms at {:.1} MiB/s",
                leg.bytes() as f64 / MIB,
                leg.mib_per_sec(),
            );
        }
    }

    Ok(())
}

/// Format an optional duration for printing, or `none` when no reply came back.
fn show(duration: Option<Duration>) -> String {
    duration.map_or_else(|| "none".to_owned(), |d| format!("{d:?}"))
}
