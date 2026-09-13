//! The measure server engines: `ping:` and `speed:` as the contract sees them.
//!
//! Ping and speed are TWO independent services, so each gets its own handler and its own gate: a grant for
//! one never opens the other, and each refuses the other's frame at the wire. Both are [`OptIn`]: a public
//! responder is a use an operator may deliberately stand behind, unlike a shell. The per-stream protocol
//! bodies stay in [`crate::responder`]; these impls are the entries and they apply the responder-side bounds
//! the caller configured with [`Limits`].
//!
//! The bounds are service-owned (rate-limit-spec): ping spaces one caller's probes by a minimum interval,
//! speed admits one transfer at a time and clamps every direction to a byte cap. A bound that exists is
//! narrated through [`Handler::metering`], so a banner warns exactly when an open service is unbounded.

use core::time::Duration;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use nauthy::VerifyKey;
use tightbeam_handler::open_policy::{OptIn, PublicUse};
use tightbeam_handler::{BoxRead, BoxWrite, Handler, Metering, ServeError, Served};
use tokio::sync::{Semaphore, SemaphorePermit};

use crate::protocol::MethodRefusal;
use crate::responder::{self, SpeedCaps};

/// The minimum spacing between two probes from one caller, when the caller sets the ping bound.
const PING_MIN_INTERVAL: Duration = Duration::from_secs(1);
/// The largest number of distinct callers the ping limiter remembers before it prunes, bounding the map a
/// peer-churn flood can grow.
const PING_MAP_MAX: usize = 8192;
/// The largest payload a metered speed run moves per direction, when the caller sets the speed bound.
const SPEED_MAX_BYTES: u64 = 64 * 1024 * 1024;
/// The longest a metered speed stream may run before the responder stops it, when the caller sets the
/// speed bound. Sized above a normal diagnostic window (seconds, not minutes) so a bounded run completes,
/// while a caller-held stream still ends; the byte cap bounds volume, this bounds lifetime.
const SPEED_MAX_DURATION: Duration = Duration::from_secs(15);

/// The responder-side bounds a measurement service enforces, configured once by the assembly and read by
/// [`Ping`] and [`Speed`].
///
/// Two constructors: [`metered`](Self::metered) installs this node's default bounds (a one-second probe
/// interval, one transfer slot, a 64 MiB per-direction cap, a 15-second stream cap), and
/// [`unmetered`](Self::unmetered) installs none, which preserves the old mirror-the-client behavior and
/// carries the `Unmetered` banner caveat. The two are the operator's deliberate choice, never a default
/// that flips silently.
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    ping_interval: Option<Duration>,
    speed_slots: Option<usize>,
    speed_max_bytes: Option<u64>,
    speed_max_duration: Option<Duration>,
}

impl Limits {
    /// This node's default bounds: a metered responder.
    pub fn metered() -> Self {
        Self {
            ping_interval: Some(PING_MIN_INTERVAL),
            speed_slots: Some(1),
            speed_max_bytes: Some(SPEED_MAX_BYTES),
            speed_max_duration: Some(SPEED_MAX_DURATION),
        }
    }

    /// No responder-side bounds: every run mirrors the client until it stops.
    pub fn unmetered() -> Self {
        Self {
            ping_interval: None,
            speed_slots: None,
            speed_max_bytes: None,
            speed_max_duration: None,
        }
    }

    /// Whether this configuration bounds what a caller may consume.
    fn metering(&self) -> Metering {
        if self.ping_interval.is_some()
            || self.speed_slots.is_some()
            || self.speed_max_bytes.is_some()
            || self.speed_max_duration.is_some()
        {
            Metering::Metered
        } else {
            Metering::Unmetered
        }
    }

    /// The speed caps derived from this configuration.
    fn speed_caps(&self) -> SpeedCaps {
        SpeedCaps {
            max_bytes: self.speed_max_bytes,
            max_duration: self.speed_max_duration,
        }
    }
}

/// The `ping:` engine: echo a caller's probes, bounded to one probe per caller per interval.
///
/// The bound is per verified caller ([`Served::peer`]), so one stranger cannot probe-loop the node; the
/// eviction bound keeps the map a peer-churn flood can grow finite.
pub struct Ping {
    interval: Option<Duration>,
    metering: Metering,
    last: Mutex<HashMap<VerifyKey, Instant>>,
}

impl Ping {
    /// Serve probes under `limits`.
    pub fn new(limits: &Limits) -> Self {
        Self {
            interval: limits.ping_interval,
            metering: limits.metering(),
            last: Mutex::new(HashMap::new()),
        }
    }

    /// Whether this caller may probe now: records the probe when admitted, prunes stale entries on the
    /// refusal path and over the retention cap. A poisoned lock (a panic in another stream's check) admits
    /// rather than wedging the service closed; the per-caller spacing is a floor, not an authority.
    fn admits(&self, peer: VerifyKey) -> bool {
        let Some(interval) = self.interval else {
            return true;
        };
        let Ok(mut last) = self.last.lock() else {
            return true;
        };
        let now = Instant::now();
        if last
            .get(&peer)
            .is_some_and(|at| now.duration_since(*at) < interval)
        {
            last.retain(|_, at| now.duration_since(*at) < interval);
            return false;
        }
        if last.len() > PING_MAP_MAX {
            last.retain(|_, at| now.duration_since(*at) < interval);
        }
        last.insert(peer, now);
        true
    }
}

impl Handler for Ping {
    /// OPT-IN: a member is admitted whole-node, and an operator may deliberately stand behind a public
    /// ping responder; a metered one bounds the probe rate, and an unmetered one warns on the banner.
    type Exposure = OptIn;

    fn metering(&self) -> Metering {
        self.metering
    }

    async fn serve(
        &self,
        served: Served<Self>,
        mut writer: BoxWrite,
        reader: BoxRead,
    ) -> Result<(), ServeError> {
        if !self.admits(served.peer()) {
            return responder::refuse(
                &mut writer,
                MethodRefusal::RateLimited,
                "ping rate limited, try again shortly",
            )
            .await
            .map_err(contract_error);
        }
        responder::answer_ping(writer, reader)
            .await
            .map_err(contract_error)
    }
}

/// The `speed:` engine: run one transfer per stream, bounded to one concurrent transfer, a per-direction
/// byte cap, and a per-stream wall clock.
///
/// A second concurrent caller is refused with the typed busy frame, never queued or given a share of the
/// uplink; the caps bound what one run may move in either direction and how long it may take.
pub struct Speed {
    slot: Option<Arc<Semaphore>>,
    caps: SpeedCaps,
    metering: Metering,
}

impl Speed {
    /// Serve transfers under `limits`.
    pub fn new(limits: &Limits) -> Self {
        Self {
            slot: limits
                .speed_slots
                .map(|slots| Arc::new(Semaphore::new(slots))),
            caps: limits.speed_caps(),
            metering: limits.metering(),
        }
    }

    /// Take the one transfer slot, or refuse because another transfer holds it. `None` when the caller
    /// configured no slot bound.
    fn acquire_slot(&self) -> Result<Option<SemaphorePermit<'_>>, ()> {
        match &self.slot {
            Some(slot) => slot.try_acquire().map(Some).map_err(|_| ()),
            None => Ok(None),
        }
    }
}

impl Handler for Speed {
    /// OPT-IN: a member is admitted whole-node, and an operator may deliberately stand behind a public
    /// throughput responder; a metered one bounds concurrency, bytes, and stream lifetime, an unmetered one
    /// warns on the banner.
    type Exposure = OptIn;

    fn metering(&self) -> Metering {
        self.metering
    }

    async fn serve(
        &self,
        _served: Served<Self>,
        mut writer: BoxWrite,
        reader: BoxRead,
    ) -> Result<(), ServeError> {
        let _permit = match self.acquire_slot() {
            Ok(permit) => permit,
            Err(()) => {
                return responder::refuse(
                    &mut writer,
                    MethodRefusal::Busy,
                    "speed busy, try again shortly",
                )
                .await
                .map_err(contract_error);
            }
        };
        responder::answer_speed(writer, reader, self.caps)
            .await
            .map_err(contract_error)
    }
}

/// Map an engine failure into the contract's typed error. The contract has no engine arm, so the cause
/// travels in the `Io` arm (the same convention every engine uses) and a CLI renders it at the verb edge.
fn contract_error(error: impl core::error::Error + Send + Sync + 'static) -> ServeError {
    ServeError::Io(std::io::Error::other(error))
}

/// The two ceilings, asserted at compile time: both diagnostics are legitimately public when an operator
/// opts in, and neither may silently become gated by a marker flip.
const _: () = assert!(<<Ping as Handler>::Exposure as PublicUse>::OPEN_SAFE);
const _: () = assert!(<<Speed as Handler>::Exposure as PublicUse>::OPEN_SAFE);

#[cfg(test)]
mod server_tests {
    use tightbeam_handler::Metering;

    use super::{Limits, Ping, SPEED_MAX_DURATION, Speed};

    fn peer(byte: u8) -> nauthy::VerifyKey {
        nauthy::Identity::from_secret(&[byte; 32])
            .expect("valid secret")
            .verifying_key()
    }

    /// The metering a banner narrates is derived from the configured bounds, never a frozen flag.
    #[test]
    fn metering_reads_the_limits() {
        assert_eq!(Limits::metered().metering(), Metering::Metered);
        assert_eq!(Limits::unmetered().metering(), Metering::Unmetered);
    }

    /// The ping bound is per caller: the first probe is admitted, a second inside the interval is refused,
    /// and a different caller is unaffected.
    #[test]
    fn ping_spaces_one_callers_probes() {
        let ping = Ping::new(&Limits::metered());
        assert!(ping.admits(peer(1)), "the first probe is admitted");
        assert!(
            !ping.admits(peer(1)),
            "a second probe inside the interval is refused"
        );
        assert!(
            ping.admits(peer(2)),
            "another caller's probe is not blocked by the first caller"
        );
    }

    /// An unmetered ping admits every probe.
    #[test]
    fn unmetered_ping_admits_every_probe() {
        let ping = Ping::new(&Limits::unmetered());
        assert!(ping.admits(peer(1)));
        assert!(ping.admits(peer(1)));
    }

    /// The speed slot admits exactly one transfer at a time and refuses the second immediately.
    #[test]
    fn speed_slot_refuses_a_second_transfer() {
        let speed = Speed::new(&Limits::metered());
        let held = speed
            .acquire_slot()
            .expect("the first transfer takes the slot");
        assert!(held.is_some(), "a metered speed holds a slot");
        assert!(
            speed.acquire_slot().is_err(),
            "a second concurrent transfer is refused, never queued"
        );
        drop(held);
        assert!(
            speed.acquire_slot().is_ok(),
            "dropping the transfer frees the slot"
        );
        assert!(Speed::new(&Limits::unmetered()).acquire_slot().is_ok());
    }

    /// The byte cap clamps an explicit request and bounds an unbounded one, so a metered run terminates
    /// on the responder's own byte count.
    #[test]
    fn speed_caps_clamp_the_request() {
        let caps = Limits::metered().speed_caps();
        assert_eq!(caps.clamp(Some(u64::MAX)), Some(64 * 1024 * 1024));
        assert_eq!(caps.clamp(Some(1)), Some(1));
        assert_eq!(caps.clamp(None), Some(64 * 1024 * 1024));
        let unbounded = Limits::unmetered().speed_caps();
        assert_eq!(unbounded.clamp(None), None);
        assert_eq!(unbounded.clamp(Some(7)), Some(7));
    }

    /// The stream cap rides the limits with the byte cap: metered carries the wall clock, unmetered leaves
    /// the stream open so a run still mirrors the client.
    #[test]
    fn speed_stream_cap_rides_the_limits() {
        assert_eq!(
            Limits::metered().speed_caps().max_duration,
            Some(SPEED_MAX_DURATION)
        );
        assert_eq!(Limits::unmetered().speed_caps().max_duration, None);
    }
}
