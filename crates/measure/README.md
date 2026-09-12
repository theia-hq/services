# measure

Ping and speed diagnostics for a session to a peer.

`Ping` sends a count of probes at an interval and reports min, average, max, mean deviation, and loss.
`Speedtest` moves counted bytes in one direction or both at once, bounded by a byte count or a wall-clock
window, and reports MiB/s per direction. Both clients are generic over `bifrost::Session`, so the same run
works over iroh, an in-process transport, or any future transport.

## Two services, one crate

`ping` and `speed` are independent services: a node can answer one without the other. `answer_ping` and
`answer_speed` each refuse the other's method with a typed refusal on the wire, so a wrong-method dial
surfaces as an error, never as `0` bytes or `100%` loss. `answer` serves both methods on one session, and
`Responder::serve` handles a session's streams concurrently and keeps the session alive when one stream
fails.

On the client side, `Ping { count, interval }` and `Speedtest::new(mode, limit)` each open one stream, run
the test, and return a report.

## The entry point

The server side is a plain async function over the stream halves: `answer_ping`, `answer_speed`, `answer`,
or `Responder::serve` on an accepted session. The caller owns admission and decides which methods a node
serves.

## Honest limits

- **No byte cap.** A time-bounded `speed` source streams until the client stops reading, and a sink drains
  up to the byte ceiling the client names. The caller's stream and session caps are the only bound. A
  node that advertises `speed` to strangers consents to that drain.
- **Bidir upload is unconfirmed.** Full-duplex mode reports the upload bytes sent; it carries no
  confirmation frame for that leg, because a trailer would corrupt the download stream. A reliable stream
  delivers what was sent.
- **One run per client value.** `Ping` and `Speedtest` consume themselves and use one stream per run;
  a second run needs a new value.
- **A probe failure counts as loss, except a refusal.** A refused method short-circuits the run with a
  typed error rather than folding into the loss figure.
- **Experimental.** Version `0.0.0`, `publish = false`, consumed by exact git revs. The API changes
  without notice.

## License

Licensed under either of Apache-2.0 or MIT at your option.
