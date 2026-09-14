//! The measure server engines: `ping:` and `speed:` as the contract sees them.
//!
//! Ping and speed are TWO independent services, so each gets its own handler and its own gate: a grant for
//! one never opens the other, and each refuses the other's frame at the wire. The per-stream protocol
//! bodies stay in [`crate::responder`]; these impls are the entries and they apply the responder-side
//! bounds from [`Limits`], the engine's two named profiles.
//!
//! Metering is exposure-coupled, and the coupling is enforced by TYPES, never by assembly convention. Each
//! service ships two engines: the owner engine ([`Ping`] / [`Speed`], `Exposure = Never`), a family route
//! bound at [`Limits::owner`] (effectively unbounded) that can never face an open gate; and the metered
//! engine ([`MeteredPing`] / [`MeteredSpeed`], `Exposure = OptIn`), built ONLY from [`Limits::metered`]
//! and therefore always carrying the public safety caps. The public proof refuses a `Never` route, so a
//! public diagnostic cannot be armed with an uncapped engine even by a hand-assembled router: the openable
//! engine is capped by construction, and the uncapped engine is structurally unopenable.

use core::time::Duration;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use nauthy::VerifyKey;
use tightbeam_handler::open_policy::{Never, OptIn, PublicUse};
use tightbeam_handler::{BoxRead, BoxWrite, Handler, Metering, ServeError, Served};
use tokio::sync::{Semaphore, SemaphorePermit};

use crate::protocol::MethodRefusal;
use crate::responder::{self, PingCaps, SpeedCaps};

/// The minimum spacing between two ping RUNS (one admitted stream each) from one caller under the metered
/// profile. The interval gates stream admission, not individual probes: every probe inside an admitted run
/// is echoed until the run ends at its stream caps.
const PING_MIN_INTERVAL: Duration = Duration::from_secs(1);
/// The largest number of stream bytes one ping run may move: the fixed-width requests plus their echoes,
/// whichever fills first. Sized at the G4 order of magnitude (about 1 GiB per public stream, the delib-49
/// guard list); a real run ends on the 60-second deadline first, so this is the pathological-rate
/// backstop, not the working bound.
const PING_MAX_BYTES: u64 = 1024 * 1024 * 1024;
/// The longest a ping stream may run before the responder stops it. Sized above an honest probe run while
/// still ending a caller-held stream: the per-caller interval bounds how often one caller can open runs,
/// this bounds how long one run can last.
const PING_MAX_DURATION: Duration = Duration::from_secs(60);
/// The largest number of distinct callers the ping limiter remembers before it prunes, bounding the map a
/// peer-churn flood can grow.
const PING_MAP_MAX: usize = 8192;
/// The largest payload a speed run moves per direction.
const SPEED_MAX_BYTES: u64 = 64 * 1024 * 1024;
/// The longest a speed stream may run before the responder stops it. Sized above a normal diagnostic
/// window (seconds, not minutes) so a bounded run completes, while a caller-held stream still ends; the
/// byte cap bounds volume, this bounds lifetime. The client's stall bound is derived from it
/// ([`crate::speed`]'s `STALL_BOUND`), because a responder stopped at its cap and a peer gone silent
/// look the same from the other end of the stream.
pub(crate) const SPEED_MAX_DURATION: Duration = Duration::from_secs(15);

/// The responder-side bounds a measurement engine enforces, as two named profiles.
///
/// [`metered`](Self::metered) is the public safety profile: a one-second ping-run interval per caller,
/// a 60-second and 1 GiB ping stream cap, one speed transfer slot, a 64 MiB per-direction cap, and a
/// 15-second speed stream cap. [`owner`](Self::owner) is the family profile: nothing bounded, so a run
/// mirrors the client until the client stops.
///
/// The engine TYPES pick the profile: [`MeteredPing`] / [`MeteredSpeed`] are built only from
/// [`metered`](Self::metered) and are the only engines the public proof opens; [`Ping`] / [`Speed`] carry
/// whatever profile a family bind hands them (the intended one is [`owner`](Self::owner)) and declare
/// `Never`, so no profile they carry can reach an open gate.
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    ping_interval: Option<Duration>,
    ping_max_bytes: Option<u64>,
    ping_max_duration: Option<Duration>,
    speed_slots: Option<usize>,
    speed_max_bytes: Option<u64>,
    speed_max_duration: Option<Duration>,
}

impl Limits {
    /// The public safety profile: the G4 caps an openable diagnostic runs under. Every field is a bound.
    pub fn metered() -> Self {
        Self {
            ping_interval: Some(PING_MIN_INTERVAL),
            ping_max_bytes: Some(PING_MAX_BYTES),
            ping_max_duration: Some(PING_MAX_DURATION),
            speed_slots: Some(1),
            speed_max_bytes: Some(SPEED_MAX_BYTES),
            speed_max_duration: Some(SPEED_MAX_DURATION),
        }
    }

    /// The owner profile: effectively unbounded. A family route's gate is the terminator, so the
    /// diagnostic mirrors the client: no ping-run interval, no stream caps, no slot bound. What remains
    /// is the serving transport's session and stream table (256 sessions, 256 streams per session), so an
    /// admitted member can hold every stream and drain the uplink. Bind the owner engines only behind a
    /// family gate; an open route binds a metered engine.
    pub fn owner() -> Self {
        Self {
            ping_interval: None,
            ping_max_bytes: None,
            ping_max_duration: None,
            speed_slots: None,
            speed_max_bytes: None,
            speed_max_duration: None,
        }
    }

    /// The ping caps derived from this profile: both set under [`metered`](Self::metered), both `None`
    /// under [`owner`](Self::owner).
    fn ping_caps(&self) -> PingCaps {
        PingCaps {
            max_bytes: self.ping_max_bytes,
            max_duration: self.ping_max_duration,
        }
    }

    /// The speed caps derived from this profile, as for [`ping_caps`](Self::ping_caps).
    fn speed_caps(&self) -> SpeedCaps {
        SpeedCaps {
            max_bytes: self.speed_max_bytes,
            max_duration: self.speed_max_duration,
        }
    }
}

/// The owner `ping:` engine: family routes at [`Limits::owner`], effectively unbounded, and never open.
///
/// The interval, when the profile sets one, is per verified caller ([`Served::peer`]), so one caller
/// cannot open runs back to back; each admitted run echoes every probe until it ends at its stream caps.
/// The map the interval needs is bounded by [`PING_MAP_MAX`]: stale entries are pruned, and a flood of
/// fresh entries inside one interval clears the map rather than growing it. The clear is a deliberate
/// fail-open on rate (memory stays bounded, spacing is forgotten for the cleared callers, so a churn
/// flood that outpaces the interval can admit more than one run per caller per interval) and is the trade
/// the rate-limit spec records.
pub struct Ping {
    interval: Option<Duration>,
    caps: PingCaps,
    last: Mutex<HashMap<VerifyKey, Instant>>,
}

impl Ping {
    /// Serve ping runs under `limits`. A family route binds [`Limits::owner`], so the run is unbounded and
    /// the `Never` ceiling keeps it off an open gate; a public route binds [`MeteredPing`] instead.
    pub fn new(limits: &Limits) -> Self {
        Self {
            interval: limits.ping_interval,
            caps: limits.ping_caps(),
            last: Mutex::new(HashMap::new()),
        }
    }

    /// Whether this caller may open a run now: records the run when admitted, prunes stale entries on the
    /// refusal path and over the retention cap. No interval (the owner profile) admits every run without
    /// touching the map. A poisoned lock (a panic in another stream's check) admits rather than wedging
    /// the service closed; the per-caller spacing is a floor, not an authority.
    ///
    /// Over-cap GC has two stages: prune entries older than the interval, then, if fresh entries alone
    /// still hold more than [`PING_MAP_MAX`] callers, clear the map. The clear is the memory bound of
    /// last resort, documented where [`Ping`] is: it trades the spacing those callers had earned for a
    /// map that cannot outgrow its cap.
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
        last.insert(peer, now);
        if last.len() > PING_MAP_MAX {
            last.retain(|_, at| now.duration_since(*at) < interval);
            if last.len() > PING_MAP_MAX {
                last.clear();
            }
        }
        true
    }

    /// The whole per-stream body, given the admitted peer: refuse an over-rate caller, else echo the run
    /// under this engine's caps. Shared with [`MeteredPing`], so the two engines cannot drift.
    async fn serve_peer(
        &self,
        peer: VerifyKey,
        mut writer: BoxWrite,
        reader: BoxRead,
    ) -> Result<(), ServeError> {
        if !self.admits(peer) {
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

impl Handler for Ping {
    /// NEVER: an owner-limit engine is effectively unbounded, so it must not face an open gate. A ping
    /// route an operator wants open binds [`MeteredPing`] instead, which is capped by construction.
    type Exposure = Never;

    /// What this engine's profile actually applies, so a banner narrates the running policy, never a
    /// frozen flag.
    fn metering(&self) -> Metering {
        if self.interval.is_some() || self.caps.is_metered() {
            Metering::Metered
        } else {
            Metering::Unmetered
        }
    }

    async fn serve(
        &self,
        served: Served<Self>,
        writer: BoxWrite,
        reader: BoxRead,
    ) -> Result<(), ServeError> {
        self.serve_peer(served.peer(), writer, reader).await
    }
}

/// The metered `ping:` engine: the public safety caps BY CONSTRUCTION, and the only ping engine the
/// public proof will open.
pub struct MeteredPing {
    ping: Ping,
}

impl MeteredPing {
    /// A public-capable ping responder: [`Limits::metered`] and nothing else. The constructor takes no
    /// profile, so no assembly can build an open ping route uncapped.
    pub fn new() -> Self {
        Self {
            ping: Ping::new(&Limits::metered()),
        }
    }
}

impl Default for MeteredPing {
    fn default() -> Self {
        Self::new()
    }
}

impl Handler for MeteredPing {
    /// OPT-IN: an operator may deliberately stand behind a public ping responder, and this engine always
    /// carries the metered caps (a one-second run interval, a 60-second and 1 GiB per-stream cap).
    type Exposure = OptIn;

    /// METERED by construction: the engine is built only from [`Limits::metered`], and no constructor
    /// takes another profile.
    fn metering(&self) -> Metering {
        Metering::Metered
    }

    async fn serve(
        &self,
        served: Served<Self>,
        writer: BoxWrite,
        reader: BoxRead,
    ) -> Result<(), ServeError> {
        self.ping.serve_peer(served.peer(), writer, reader).await
    }
}

/// The owner `speed:` engine: family routes at [`Limits::owner`], effectively unbounded, and never open.
///
/// A slot bound, when the profile sets one, admits one transfer at a time: a second concurrent caller is
/// refused with the typed busy frame, never queued or given a share of the uplink. The caps bound what one
/// run may move in either direction and how long it may take.
pub struct Speed {
    slot: Option<Arc<Semaphore>>,
    caps: SpeedCaps,
}

impl Speed {
    /// Serve transfers under `limits`. A family route binds [`Limits::owner`], so the transfer mirrors the
    /// client and the `Never` ceiling keeps it off an open gate; a public route binds [`MeteredSpeed`].
    pub fn new(limits: &Limits) -> Self {
        Self {
            slot: limits
                .speed_slots
                .map(|slots| Arc::new(Semaphore::new(slots))),
            caps: limits.speed_caps(),
        }
    }

    /// Take a transfer slot, or refuse because another transfer holds it. `None` (the owner profile) is no
    /// slot bound: every transfer is admitted.
    fn acquire_slot(&self) -> Result<Option<SemaphorePermit<'_>>, ()> {
        match &self.slot {
            Some(slot) => slot.try_acquire().map(Some).map_err(|_| ()),
            None => Ok(None),
        }
    }

    /// The whole per-stream body: take the slot, then run the transfer under this engine's caps. Shared
    /// with [`MeteredSpeed`], so the two engines cannot drift.
    async fn serve_stream(&self, mut writer: BoxWrite, reader: BoxRead) -> Result<(), ServeError> {
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

impl Handler for Speed {
    /// NEVER: an owner-limit engine is effectively unbounded, so it must not face an open gate. A speed
    /// route an operator wants open binds [`MeteredSpeed`] instead, which is capped by construction.
    type Exposure = Never;

    /// What this engine's profile actually applies, so a banner narrates the running policy, never a
    /// frozen flag.
    fn metering(&self) -> Metering {
        if self.slot.is_some() || self.caps.is_metered() {
            Metering::Metered
        } else {
            Metering::Unmetered
        }
    }

    async fn serve(
        &self,
        _served: Served<Self>,
        writer: BoxWrite,
        reader: BoxRead,
    ) -> Result<(), ServeError> {
        self.serve_stream(writer, reader).await
    }
}

/// The metered `speed:` engine: the public safety caps BY CONSTRUCTION, and the only speed engine the
/// public proof will open.
pub struct MeteredSpeed {
    speed: Speed,
}

impl MeteredSpeed {
    /// A public-capable throughput responder: [`Limits::metered`] and nothing else. The constructor takes
    /// no profile, so no assembly can build an open speed route uncapped.
    pub fn new() -> Self {
        Self {
            speed: Speed::new(&Limits::metered()),
        }
    }
}

impl Default for MeteredSpeed {
    fn default() -> Self {
        Self::new()
    }
}

impl Handler for MeteredSpeed {
    /// OPT-IN: an operator may deliberately stand behind a public throughput responder, and this engine
    /// always carries the metered caps (one transfer slot, a 64 MiB and 15-second per-stream cap).
    type Exposure = OptIn;

    /// METERED by construction: the engine is built only from [`Limits::metered`], and no constructor
    /// takes another profile.
    fn metering(&self) -> Metering {
        Metering::Metered
    }

    async fn serve(
        &self,
        _served: Served<Self>,
        writer: BoxWrite,
        reader: BoxRead,
    ) -> Result<(), ServeError> {
        self.speed.serve_stream(writer, reader).await
    }
}

/// Map an engine failure into the contract's typed error. The contract has no engine arm, so the cause
/// travels in the `Io` arm (the same convention every engine uses) and a CLI renders it at the verb edge.
fn contract_error(error: impl core::error::Error + Send + Sync + 'static) -> ServeError {
    ServeError::Io(std::io::Error::other(error))
}

/// The exposure coupling, asserted at compile time: the metered engines are openable, and the owner
/// engines are NOT. Flipping either would let an uncapped ping/speed face an open gate, or gate the
/// engines an operator may deliberately open.
const _: () = assert!(<<MeteredPing as Handler>::Exposure as PublicUse>::OPEN_SAFE);
const _: () = assert!(<<MeteredSpeed as Handler>::Exposure as PublicUse>::OPEN_SAFE);
const _: () = assert!(!<<Ping as Handler>::Exposure as PublicUse>::OPEN_SAFE);
const _: () = assert!(!<<Speed as Handler>::Exposure as PublicUse>::OPEN_SAFE);

#[cfg(test)]
mod server_tests {
    use tightbeam_handler::{Handler as _, Metering};

    use super::{
        Limits, MeteredPing, MeteredSpeed, PING_MAP_MAX, PING_MAX_BYTES, PING_MAX_DURATION, Ping,
        SPEED_MAX_DURATION, Speed,
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

    /// The exposure coupling, read at the value level too: the metered engines report `Metered` by
    /// construction, and the owner engines (owner limits) report `Unmetered`.
    #[test]
    fn the_openable_engines_are_metered_and_the_owner_engines_are_not() {
        assert_eq!(MeteredPing::new().metering(), Metering::Metered);
        assert_eq!(MeteredSpeed::new().metering(), Metering::Metered);
        assert_eq!(Ping::new(&Limits::owner()).metering(), Metering::Unmetered);
        assert_eq!(Speed::new(&Limits::owner()).metering(), Metering::Unmetered);
    }

    /// The public wrappers carry the metered caps through their own fields: the assertions read the
    /// wrapped engine and drive its observable bound, so swapping either wrapper's interior to owner
    /// limits fails here rather than only at the hardcoded `metering()` report.
    #[test]
    fn the_metered_wrappers_carry_the_caps_by_construction() {
        let ping = MeteredPing::new();
        assert_eq!(ping.metering(), Metering::Metered);
        assert!(
            ping.ping.interval.is_some(),
            "the metered wrapper carries the ping run interval"
        );
        assert!(
            ping.ping.caps.is_metered(),
            "the metered wrapper carries the ping stream caps"
        );
        assert!(ping.ping.admits(peer(1)), "the first run is admitted");
        assert!(
            !ping.ping.admits(peer(1)),
            "a second run inside the interval is refused through the wrapper"
        );

        let speed = MeteredSpeed::new();
        assert_eq!(speed.metering(), Metering::Metered);
        assert!(
            speed.speed.slot.is_some(),
            "the metered wrapper carries the speed transfer slot"
        );
        assert!(
            speed.speed.caps.is_metered(),
            "the metered wrapper carries the speed stream caps"
        );
        let held = speed
            .speed
            .acquire_slot()
            .expect("the first transfer takes the slot");
        assert!(held.is_some(), "the wrapper's slot is a real bound");
        assert!(
            speed.speed.acquire_slot().is_err(),
            "a second concurrent transfer is refused through the wrapper"
        );
    }

    /// The owner profile bounds nothing, on either service.
    #[test]
    fn owner_limits_bound_nothing() {
        let ping = Limits::owner().ping_caps();
        assert_eq!(ping.max_bytes, None);
        assert_eq!(ping.max_duration, None);
        let speed = Limits::owner().speed_caps();
        assert_eq!(speed.max_bytes, None);
        assert_eq!(speed.max_duration, None);
    }

    /// The ping bound is per caller: the first run is admitted, a second inside the interval is refused,
    /// and a different caller is unaffected.
    #[test]
    fn ping_spaces_one_callers_runs() {
        let ping = Ping::new(&Limits::metered());
        assert!(ping.admits(peer(1)), "the first run is admitted");
        assert!(
            !ping.admits(peer(1)),
            "a second run inside the interval is refused"
        );
        assert!(
            ping.admits(peer(2)),
            "another caller's run is not blocked by the first caller"
        );
    }

    /// The owner profile spaces nothing: a caller may open run after run, and the map is never touched.
    #[test]
    fn owner_ping_admits_every_run() {
        let ping = Ping::new(&Limits::owner());
        assert!(ping.admits(peer(1)), "the first run is admitted");
        assert!(ping.admits(peer(1)), "the next run is admitted too");
        assert!(
            ping.last
                .lock()
                .expect("the map mutex is not poisoned")
                .is_empty(),
            "an interval-free engine never records callers"
        );
    }

    /// The ping stream caps are engine-owned: the metered profile carries the wall clock and byte
    /// ceiling, and the metered engine takes no other profile.
    #[test]
    fn ping_stream_caps_are_engine_owned() {
        let caps = Limits::metered().ping_caps();
        assert_eq!(caps.max_bytes, Some(PING_MAX_BYTES));
        assert_eq!(caps.max_duration, Some(PING_MAX_DURATION));
    }

    /// A fresh-peer churn flood cannot grow the map past its cap, and the fallback does not disable the
    /// limiter: after the over-cap clear, a fresh caller is still admitted once and spaced on its next
    /// run inside the interval. Without the clear, every fresh entry survives the retain and the map
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
            "and its next run inside the interval is still refused"
        );
    }

    /// The speed slot admits exactly one transfer at a time and refuses the second immediately.
    #[test]
    fn speed_slot_refuses_a_second_transfer() {
        let speed = Speed::new(&Limits::metered());
        let held = speed
            .acquire_slot()
            .expect("the first transfer takes the slot");
        assert!(held.is_some(), "the metered profile carries a slot");
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

    /// The owner profile carries no slot bound: concurrent transfers are admitted, never refused busy.
    #[test]
    fn owner_speed_takes_no_slot() {
        let speed = Speed::new(&Limits::owner());
        let first = speed
            .acquire_slot()
            .expect("owner speed never refuses a transfer");
        assert!(first.is_none(), "no slot is bounded under owner limits");
        assert!(
            speed.acquire_slot().is_ok(),
            "a second concurrent transfer is admitted"
        );
    }

    /// The byte cap clamps an explicit request and bounds an unbounded one, so a metered run terminates
    /// on the responder's own byte count. The unbounded side is the owner profile's shape; the metered
    /// engine always carries the cap.
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

    /// The stream cap is engine-owned with the byte cap: the metered profile carries the wall clock.
    #[test]
    fn speed_stream_cap_is_engine_owned() {
        assert_eq!(
            Limits::metered().speed_caps().max_duration,
            Some(SPEED_MAX_DURATION)
        );
    }
}
