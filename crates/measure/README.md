# measure

Ping and speed diagnostics for a session to a peer.

`Ping` sends a count of probes at an interval and reports min, average, max, mean deviation, and loss.
`Speedtest` moves counted bytes in one direction or both at once, bounded by a byte count or a wall-clock
window, and reports MiB/s per direction. Both clients are generic over `bifrost::Session`, so the same run
works over iroh, an in-process transport, or any future transport.

## Two services, one crate

`ping` and `speed` are independent services: a node can answer one without the other. The served entries
are `server::Ping` and `server::Speed`, two `Handler` impls behind their own gates, each refusing the
other's method with a typed refusal on the wire, so a wrong-method dial surfaces as an error, never as `0`
bytes or `100%` loss. The per-stream protocol bodies are crate-private.

On the client side, `Ping { count, interval }` and `Speedtest::new(mode, limit)` each open one stream, run
the test, and return a report.

## The entry point

`server::Ping::new(&limits)` and `server::Speed::new(&limits)` are the whole server side. `limits` is built
with `Limits::metered()`: a one-second probe interval per caller, a 60-second and 1 GiB ping stream cap,
one transfer slot, a 64 MiB per-direction speed cap, and a 15-second speed stream cap. `Limits` has no
unbounded constructor and every bound is non-optional, so the public-capable engines are metered by
construction: no assembly can stand an uncapped `ping` or `speed`, and the banner never carries the
unmetered caveat for them. The caller owns admission and exposure.

## Honest limits

- **Ping and speed are metered by construction.** `Limits::metered()` is the only way to build either
  engine and every bound is non-optional, so the guard is structural, not an assembly choice: an open
  `ping`/`speed` route is always capped, and the unmetered banner caveat can never apply to one.
- **Metered bounds are per service instance.** A metered `speed` admits one transfer at a time, clamps
  each direction to the byte cap, and stops the stream at the wall-clock cap (a capped sink replies with
  the bytes it took; a capped source closes early).
- **A metered ping stream ends at its cap.** At 60 seconds or the 1 GiB byte ceiling, whichever comes
  first, the responder writes a typed refusal and closes; a client reads that as a refusal, never a silent
  close folded into loss. A client that stopped reading sees only the close.
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
