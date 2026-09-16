# Changelog

All notable changes to services, newest first.

## Unreleased

### Changed
- **Every `fetch` and `transfer` failure is a typed error.** `Origin::parse`, `OriginAllowlist::parse`,
  and `compose_url` return `OriginError` and `ComposeError` instead of a message string, so a consumer
  matches the cause (a userinfo-bearing origin, a missing host, a bad join) rather than reading text; the
  refusal a requester sees on the wire renders the same words as before. `transfer`'s receive path returns
  `ReceiveError` naming the step that failed (temp file, wire transfer, flush, directory, save) with the
  path already rendered log-safe, and the crate drops its `eyre` dependency. All four engines now speak
  `thiserror` at their boundaries.

## v0.1.1

Sibling pins move to bifrost v0.1.1 and tightbeam-handler from tightbeam v0.5.1; no engine behavior change.

### Changed
- **The bifrost pin follows the v0.1.1 hotfix.** The bind-role split lands under the engines unchanged; the
  tightbeam-handler pin moves with it.

## v0.1.0

The first release of the four engines, consumed today only by swoosh: `fetch`, `measure`, `sshh`, and
`transfer`, each a library that serves one job on an already-admitted stream.

### New
- **`fetch`.** Perform an HTTP `GET`/`HEAD` at an origin for an admitted requester, with the target vetted
  against SSRF and the operator's `OriginAllowlist`; the scoped engine carries a 16 MiB body cap and a
  30-second total deadline by construction. The request URL is composed by URL-join, and an unparseable or
  userinfo-bearing origin is refused before it connects.
- **`measure`.** Answer `ping` (round-trip time) and `speed` (throughput) tests on a session, or run the
  same tests from the client side; a speed run stops at its byte or wall-clock bound in every direction,
  and the per-stream protocol bodies stay crate-private. A refusal is a typed error on the wire, never a
  measured value: a wrong-method dial surfaces as a refusal, not `0` bytes or `100%` loss, and an unknown
  refusal code decodes as an error, not as loss.
- **`sshh`.** Serve a shell over an admitted stream to a standard `ssh` client, with no SSH keys: the
  admission is the only credential, the host key derives from the node's identity, and a terminal resize
  propagates to the remote pty. It refuses a root shell and caps live shells per process at 64.
- **`transfer`.** Receive one pushed file off an admitted stream, verified end to end against the
  sender's BLAKE3 root and saved under an output directory; a peer-supplied name is reduced to a safe
  relative path, and a failed transfer leaves no partial file.
- **The `Handler` impl is the entry.** Each engine implements `tightbeam-handler`'s `Handler` directly,
  declaring its exposure ceiling (`Never`, or `OptIn` for the scoped fetch), and the public free
  functions are gone, so a service crate depends on the lean contract and not on tightbeam.

### Changed
- **Diagnostic metering is exposure-coupled.** `measure` ships two engines per service: the owner engine
  (`Ping`/`Speed`, `Limits::owner()`, effectively unbounded, `Exposure = Never`) and the metered engine
  (`MeteredPing`/`MeteredSpeed`, the safety caps by construction, `Exposure = OptIn`, the only engines the
  public proof opens). A public route therefore cannot be armed with an uncapped diagnostic, and a family
  route is no longer clamped to the public caps. An owner diagnostic carries no caps of its own, so the
  serving transport's session and stream table (256 sessions, 256 streams per session) is the only bound:
  a member admitted by the gate can drain the uplink.
- **The receive engine reports an arrival as a structured event.** `transfer` now emits `path` and `bytes`
  at info level when a pushed file lands (previously debug-only), so a composing program's subscriber can
  surface it. The path is peer-controlled, so it is escaped (control characters) and capped before it
  reaches the event, in the success and the rename-error path alike; no user-facing prose lives in the
  engine.

### Fixed
- **A byte-bounded `speed` run that stops short fails typed instead of hanging or reporting the short
  count.** A run at the metered cap parked at `0.00 MiB/s` with no totals (the responder's 15-second
  lifetime cap closed a source the client never saw close), and the upload direction exited 0 with a
  short count. The client now bounds each payload wait by the responder's lifetime cap plus a grace and
  ends a byte-bounded run that cannot move the asked bytes with a typed `EndedEarly` error in every mode
  (down, up, bidir); a short run is never a smaller throughput. Time-bounded runs and the wire are
  unchanged.
- **An over-cap `speed` request is refused before any payload, not truncated.** A metered responder
  answered an explicit request for more bytes than its byte cap by clamping the transfer and closing, so
  a client asking for more than the cap waited on bytes that would never arrive (public `speed -n 128MiB`
  against the 64 MiB cap hung) or counted the short run. The responder now answers that request with the
  typed Layer-2 `RateLimited` refusal in place of the go-ahead (or the drain), and the upload leg reads
  the reply while it sends, so every mode exits promptly with the refusal.
