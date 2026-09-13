//! The measure server engines: `ping:` and `speed:` as the contract sees them.
//!
//! Ping and speed are TWO independent services, so each gets its own handler and its own gate: a grant for
//! one never opens the other, and each refuses the other's frame at the wire. Both are [`OptIn`]: a public
//! responder is a use an operator may deliberately stand behind, unlike a shell. The per-stream protocol
//! bodies stay in [`crate::responder`]; these impls are the entries and they apply the responder-side bounds
//! from [`Limits`], the engine's one metered configuration.
//!
//! The bounds are service-owned (rate-limit-spec): ping spaces one caller's probes by a minimum interval
//! and caps the stream's bytes and lifetime, speed admits one transfer at a time and clamps every
//! direction to a byte cap. Both engines are metered by construction (see [`Limits`]), so they always
//! report [`Metering::Metered`] and the banner's unmetered caveat can never apply to them.

use core::time::Duration;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use nauthy::VerifyKey;
use tightbeam_handler::open_policy::{OptIn, PublicUse};
use tightbeam_handler::{BoxRead, BoxWrite, Handler, Metering, ServeError, Served};
use tokio::sync::{Semaphore, SemaphorePermit};

use crate::protocol::MethodRefusal;
use crate::responder::{self, PingCaps, SpeedCaps};

/// The minimum spacing between two probes from one caller.
const PING_MIN_INTERVAL: Duration = Duration::from_secs(1);
/// The largest number of stream bytes one ping run may move: the fixed-width requests plus their echoes,
/// whichever fills first. Sized at the G4 order of magnitude (about 1 GiB per public stream, the delib-49
/// guard list); a real run ends on the 60-second deadline first, so this is the pathological-rate
/// backstop, not the working bound.
const PING_MAX_BYTES: u64 = 1024 * 1024 * 1024;
/// The longest a ping stream may run before the responder stops it. Sized above an honest probe run (a
/// handful of probes at one per interval) while still ending a caller-held stream; the interval bounds
/// the probe rate, this bounds the stream lifetime.
const PING_MAX_DURATION: Duration = Duration::from_secs(60);
/// The largest number of distinct callers the ping limiter remembers before it prunes, bounding the map a
/// peer-churn flood can grow.
const PING_MAP_MAX: usize = 8192;
/// The largest payload a speed run moves per direction.
const SPEED_MAX_BYTES: u64 = 64 * 1024 * 1024;
/// The longest a speed stream may run before the responder stops it. Sized above a normal diagnostic
/// window (seconds, not minutes) so a bounded run completes, while a caller-held stream still ends; the
/// byte cap bounds volume, this bounds lifetime.
const SPEED_MAX_DURATION: Duration = Duration::from_secs(15);

/// The responder-side bounds a measurement service enforces, configured once by the assembly and read by
/// [`Ping`] and [`Speed`].
///
/// One constructor, [`metered`](Self::metered), installs this node's bounds: a one-second probe interval
/// plus a 60-second and 1 GiB ping stream cap, one transfer slot, a 64 MiB per-direction cap, and a
/// 15-second stream cap. Every field is a bound, never an `Option`, and there is no unbounded
/// constructor, so the public-capable diagnostics are metered by construction: no assembly can build a
/// `Ping` or `Speed` that mirrors the client, and the banner can never carry the unmetered caveat for
/// one. That is the structural guard, not an assembly choice.
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    ping_interval: Duration,
    ping_max_bytes: u64,
    ping_max_duration: Duration,
    speed_slots: usize,
    speed_max_bytes: u64,
    speed_max_duration: Duration,
}

impl Limits {
    /// This node's bounds: the one metered configuration a public-capable diagnostic may run under.
    pub fn metered() -> Self {
        Self {
            ping_interval: PING_MIN_INTERVAL,
            ping_max_bytes: PING_MAX_BYTES,
            ping_max_duration: PING_MAX_DURATION,
            speed_slots: 1,
            speed_max_bytes: SPEED_MAX_BYTES,
            speed_max_duration: SPEED_MAX_DURATION,
        }
    }

    /// The ping caps derived from this configuration. Both are always set: the fields are bounds, so a
    /// metered ping can never be built uncapped.
    fn ping_caps(&self) -> PingCaps {
        PingCaps {
            max_bytes: Some(self.ping_max_bytes),
            max_duration: Some(self.ping_max_duration),
        }
    }

    /// The speed caps derived from this configuration. Both are always set, as for
    /// [`ping_caps`](Self::ping_caps).
    fn speed_caps(&self) -> SpeedCaps {
        SpeedCaps {
            max_bytes: Some(self.speed_max_bytes),
            max_duration: Some(self.speed_max_duration),
        }
    }
}

/// The `ping:` engine: echo a caller's probes, bounded to one probe per caller per interval and to a
/// per-stream byte ceiling and wall clock.
///
/// Metered by construction: the interval, the byte ceiling, and the wall clock all come from [`Limits`],
/// which has no unbounded constructor, so no assembly can stand an uncapped ping responder. The interval
/// is per verified caller ([`Served::peer`]), so one stranger cannot probe-loop the node. The stream caps
/// bound what one admitted stream may move and how long it may run, so a peer that clears the interval
/// still cannot hold a responder task and one of the public stream permits indefinitely. The map the
/// interval needs is bounded by [`PING_MAP_MAX`]: stale entries are pruned, and a flood of fresh entries
/// inside one interval clears the map rather than growing it. The clear is a deliberate fail-open on rate
/// (memory stays bounded, spacing is forgotten for the cleared callers, so a churn flood that outpaces
/// the interval can admit more than one probe per caller per interval) and is the trade the rate-limit
/// spec records.
pub struct Ping {
    interval: Duration,
    caps: PingCaps,
    last: Mutex<HashMap<VerifyKey, Instant>>,
}

impl Ping {
    /// Serve probes under `limits`.
    pub fn new(limits: &Limits) -> Self {
        Self {
            interval: limits.ping_interval,
            caps: limits.ping_caps(),
            last: Mutex::new(HashMap::new()),
        }
    }

    /// Whether this caller may probe now: records the probe when admitted, prunes stale entries on the
    /// refusal path and over the retention cap. A poisoned lock (a panic in another stream's check) admits
    /// rather than wedging the service closed; the per-caller spacing is a floor, not an authority.
    ///
    /// Over-cap GC has two stages: prune entries older than the interval, then, if fresh entries alone
    /// still hold more than [`PING_MAP_MAX`] callers, clear the map. The clear is the memory bound of
    /// last resort, documented where [`Ping`] is: it trades the spacing those callers had earned for a
    /// map that cannot outgrow its cap.
    fn admits(&self, peer: VerifyKey) -> bool {
        let Ok(mut last) = self.last.lock() else {
            return true;
        };
        let now = Instant::now();
        if last
            .get(&peer)
            .is_some_and(|at| now.duration_since(*at) < self.interval)
        {
            last.retain(|_, at| now.duration_since(*at) < self.interval);
            return false;
        }
        last.insert(peer, now);
        if last.len() > PING_MAP_MAX {
            last.retain(|_, at| now.duration_since(*at) < self.interval);
            if last.len() > PING_MAP_MAX {
                last.clear();
            }
        }
        true
    }
}

impl Handler for Ping {
    /// OPT-IN: a member is admitted whole-node, and an operator may deliberately stand behind a public
    /// ping responder; the engine is metered by construction, so its probe rate and stream are always
    /// bounded.
    type Exposure = OptIn;

    fn metering(&self) -> Metering {
        Metering::Metered
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
        responder::answer_ping(writer, reader, self.caps)
            .await
            .map_err(contract_error)
    }
}

/// The `speed:` engine: run one transfer per stream, bounded to one concurrent transfer, a per-direction
/// byte cap, and a per-stream wall clock.
///
/// Metered by construction: the slot and both caps come from [`Limits`], which has no unbounded
/// constructor, so no assembly can stand an uncapped throughput responder. A second concurrent caller is
/// refused with the typed busy frame, never queued or given a share of the uplink; the caps bound what
/// one run may move in either direction and how long it may take.
pub struct Speed {
    slot: Arc<Semaphore>,
    caps: SpeedCaps,
}

impl Speed {
    /// Serve transfers under `limits`.
    pub fn new(limits: &Limits) -> Self {
        Self {
            slot: Arc::new(Semaphore::new(limits.speed_slots)),
            caps: limits.speed_caps(),
        }
    }

    /// Take the one transfer slot, or refuse because another transfer holds it.
    fn acquire_slot(&self) -> Result<SemaphorePermit<'_>, ()> {
        self.slot.try_acquire().map_err(|_| ())
    }
}

impl Handler for Speed {
    /// OPT-IN: a member is admitted whole-node, and an operator may deliberately stand behind a public
    /// throughput responder; the engine is metered by construction, so concurrency, bytes, and stream
    /// lifetime are always bounded.
    type Exposure = OptIn;

    fn metering(&self) -> Metering {
        Metering::Metered
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
    use tightbeam_handler::{Handler as _, Metering};

    use super::{
        Limits, PING_MAP_MAX, PING_MAX_BYTES, PING_MAX_DURATION, Ping, SPEED_MAX_DURATION, Speed,
    };
    use crate::responder::SpeedCaps;

    /// A distinct caller key: the secret's first eight bytes carry `n`, so each `n` mints a new identity.
    fn peer(n: u64) -> nauthy::VerifyKey {
        let mut secret = [0u8; 32];
        secret[..8].copy_from_slice(&n.to_be_bytes());
        nauthy::Identity::from_secret(&secret)
            .expect("valid secret")
            .verifying_key()
    }

    /// The public-capable engines report metered unconditionally: there is no configuration that could
    /// hand a banner the unmetered caveat for ping or speed.
    #[test]
    fn public_capable_engines_are_metered_by_construction() {
        assert_eq!(Ping::new(&Limits::metered()).metering(), Metering::Metered);
        assert_eq!(Speed::new(&Limits::metered()).metering(), Metering::Metered);
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

    /// The ping stream caps are engine-owned: the one metered limit set carries the wall clock and byte
    /// ceiling, and no constructor drops either.
    #[test]
    fn ping_stream_caps_are_engine_owned() {
        let caps = Limits::metered().ping_caps();
        assert_eq!(caps.max_bytes, Some(PING_MAX_BYTES));
        assert_eq!(caps.max_duration, Some(PING_MAX_DURATION));
    }

    /// A fresh-peer churn flood cannot grow the map past its cap, and the fallback does not disable the
    /// limiter: after the over-cap clear, a fresh caller is still admitted once and spaced on its next
    /// probe inside the interval. Without the clear, every fresh entry survives the retain and the map
    /// outgrows the documented bound.
    #[test]
    fn ping_map_stays_bounded_under_fresh_peer_churn() {
        let ping = Ping::new(&Limits::metered());
        for n in 0..(PING_MAP_MAX as u64 + 64) {
            assert!(
                ping.admits(peer(n)),
                "a fresh caller is admitted once, at {n}"
            );
        }
        let held = ping
            .last
            .lock()
            .expect("the map mutex is not poisoned")
            .len();
        assert!(
            held <= PING_MAP_MAX,
            "the map must stay at or under its cap, holds {held}"
        );

        let fresh = peer(u64::MAX);
        assert!(
            ping.admits(fresh),
            "a fresh caller after the flood is admitted"
        );
        assert!(
            !ping.admits(fresh),
            "and its next probe inside the interval is still refused"
        );
    }

    /// The speed slot admits exactly one transfer at a time and refuses the second immediately.
    #[test]
    fn speed_slot_refuses_a_second_transfer() {
        let speed = Speed::new(&Limits::metered());
        let held = speed
            .acquire_slot()
            .expect("the first transfer takes the slot");
        assert!(
            speed.acquire_slot().is_err(),
            "a second concurrent transfer is refused, never queued"
        );
        drop(held);
        assert!(
            speed.acquire_slot().is_ok(),
            "dropping the transfer frees the slot"
        );
    }

    /// The byte cap clamps an explicit request and bounds an unbounded one, so a metered run terminates
    /// on the responder's own byte count. The unbounded side is the crate-private cap the test-only
    /// union body uses; the public `Speed` engine always carries the metered cap.
    #[test]
    fn speed_caps_clamp_the_request() {
        let caps = Limits::metered().speed_caps();
        assert_eq!(caps.clamp(Some(u64::MAX)), Some(64 * 1024 * 1024));
        assert_eq!(caps.clamp(Some(1)), Some(1));
        assert_eq!(caps.clamp(None), Some(64 * 1024 * 1024));
        let unbounded = SpeedCaps {
            max_bytes: None,
            max_duration: None,
        };
        assert_eq!(unbounded.clamp(None), None);
        assert_eq!(unbounded.clamp(Some(7)), Some(7));
    }

    /// The stream cap is engine-owned with the byte cap: the one metered set carries the wall clock.
    #[test]
    fn speed_stream_cap_is_engine_owned() {
        assert_eq!(
            Limits::metered().speed_caps().max_duration,
            Some(SPEED_MAX_DURATION)
        );
    }
}
