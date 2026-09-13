# measure

Ping and speed diagnostics for a session to a peer.

`Ping` sends a count of probes at an interval and reports min, average, max, mean deviation, and loss.
`Speedtest` moves counted bytes in one direction or both at once, bounded by a byte count or a wall-clock
window, and reports MiB/s per direction. Both clients are generic over `bifrost::Session`, so the same run
works over iroh, an in-process transport, or any future transport.

## Two services, one crate

`ping` and `speed` are independent services: a node can answer one without the other. Each has an owner
entry and a metered entry: `server::Ping` / `server::Speed` are `Handler` impls for family routes (owner
limits, never public), `server::MeteredPing` / `server::MeteredSpeed` are the public-capable ones (the
safety caps by construction). Each refuses the other's method with a typed refusal on the wire, so a
wrong-method dial surfaces as an error, never as `0` bytes or `100%` loss. The per-stream protocol bodies
are crate-private.

On the client side, `Ping { count, interval }` and `Speedtest::new(mode, limit)` each open one stream, run
the test, and return a report.

## The entry point

`server::Ping::new(&limits)` and `server::Speed::new(&limits)` are the family entries: bind them at
`Limits::owner()` (no responder-side bound; the gate is the terminator). `server::MeteredPing::new()` and
`server::MeteredSpeed::new()` are the public entries: they carry the safety caps by construction (a
one-second ping-run interval per caller, a 60-second and 1 GiB ping stream cap, one transfer slot, a
64 MiB per-direction speed cap, and a 15-second speed stream cap) and take no profile, so an open
`ping`/`speed` route cannot be armed uncapped. The owner engines declare `Never`, so the public proof
refuses them even if a hand-assembled router names one; the metered engines declare `OptIn` and are the
only ones the proof will open. The caller owns admission.

## Honest limits

- **Exposure-coupled metering, by type.** The metered engines are the only openable ones and always carry
  the caps; the owner engines are unbounded and can never be opened. That coupling is structural, not an
  assembly choice.
- **Metered bounds are per service instance.** A metered `speed` admits one transfer at a time, clamps
  each direction to the byte cap, and stops the stream at the wall-clock cap (a capped sink replies with
  the bytes it took; a capped source closes early). An owner `speed` bounds neither and takes no slot.
- **An owner diagnostic has no caps of its own.** The serving transport's session and stream table (256
  sessions, 256 streams per session) is the only bound, so a member admitted by the gate can drain the
  uplink. An open route binds the metered engines.
- **A metered ping stream ends at its cap.** At 60 seconds or the 1 GiB byte ceiling, whichever comes
  first, the responder writes a typed refusal and closes; a client reads that as a refusal, never a silent
  close folded into loss. A client that stopped reading sees only the close. The per-caller interval gates
  ping RUNS (one admitted stream each), not the probes inside a run; an owner ping spaces nothing.
- **Bidir upload is unconfirmed.** Full-duplex mode reports the upload bytes sent; it carries no
  confirmation frame for that leg, because a trailer would corrupt the download stream. A reliable stream
  delivers what was sent.
- **One run per client value.** `Ping` and `Speedtest` consume themselves and use one stream per run;
  a second run needs a new value.
- **A probe failure counts as loss, except a refusal.** A refused method short-circuits the run with a
  typed error rather than folding into the loss figure.
- **Experimental.** Version `0.0.0`, `publish = false`, consumed from git. The API changes
  without notice.

## License

Licensed under either of Apache-2.0 or MIT at your option.
