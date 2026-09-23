# Changelog

The four engines here share no code with each other, so each carries its own version and its own
cadence: a release moves one engine and leaves the other three exactly where they were. Each
engine's notes live beside its manifest.

- [`fetch`](crates/fetch/CHANGELOG.md)
- [`measure`](crates/measure/CHANGELOG.md)
- [`sshh`](crates/sshh/CHANGELOG.md)
- [`transfer`](crates/transfer/CHANGELOG.md)

This file is the record of what came before, newest first, and it closes at v0.3.0. Up to and
including that release the four moved together under one number, so every entry below is about the
set: not one of them is about a single engine, which is why they stay here rather than being dealt
out four ways.

All four start from the v0.3.0 they hold today. That number was reached in lockstep rather than
earned separately, and it stands. A version that shipped is shipped, and an untidy starting point
is better than a rewritten history.

What lockstep cost is easiest to see in `sshh`, which has had no source change at all since its
first release and whose version was raised ten times anyway, 0.1.0 through 0.3.0. Ten releases of
code that did not move. Read the other way, the same numbering meant a fix in one engine arrived
as a new version of the other three, and a consumer comparing versions could not tell which.

The repository itself has no version. Its root manifest is a workspace with no package to carry
one, and the marker for the set that was tested together is the commit, which is what a consumer
pins.

## v0.3.0

Two wires learn to say which version they speak, and a ping stops hanging.

### Fixed
- **A peer one release out was indistinguishable from a foreign protocol, on both wires.** `measure`
  and `fetch` each compared four magic bytes for equality and then propagated the mismatch before
  any response could be written, so a dialer on another build received a closed stream and no
  explanation at all.

  Both now read the magic as an identity followed by a version, split by the rule the identity is
  defined by, the maximal run of capitals. One rule parses both a two-and-two magic and a
  three-and-one magic, which is why the two shapes were never two patterns. Each wire checks its own
  magic against that rule at compile time, walking the run rather than naming byte positions, so
  widening an identity later cannot leave a byte unchecked.

  A foreign identity is still refused in silence, and the wording that always said so is now true.
  A version mismatch is answered, naming what the peer sent and what this host speaks.

- **`swoosh ping` and `swoosh status` hung forever against a peer that admitted the stream and then
  went quiet.** A probe now waits ten seconds and a probe that times out counts as loss, which is
  what loss has always meant to a person. The wait covers writing as well as reading, because a peer
  that stops reading parks a client just as surely as one that stops answering.

  The bound is not shared with the speed client's stall bound, which is a duration cap plus grace
  for a sink that may honestly owe its count for its whole lifetime. Nothing holds a reply back, so
  deriving this one the same way would have answered a hang with a slower hang.

### Changed
- Advances to bifrost v0.4.0, nauthy v0.4.0 and tightbeam v0.11.0.
- **`fetch`'s response frame carries its own frozen tag**, no longer derived from the request
  version. Its response carries the magic, unlike the other wires in the family, so an answer
  written with a bumped version would be unreadable by exactly the peer it is for. No byte changes
  today; a future version bump can no longer silently break the answer.

## v0.2.0

Keeps up with a refusal type that stopped being a closed set.

### Changed
- **Picks up bifrost v0.3.0 and tightbeam v0.9.0.** bifrost's `Refusal` is now `#[non_exhaustive]`,
  and `measure` re-exports a `Refusal` that wraps it, so a consumer matching on the stream case
  inherits the new obligation: one more arm, for a class this build cannot name. That arm should
  say so rather than fold the unknown class onto a known one, because standing in for the
  not-admitted class invents an authorization ruling out of a message that carried none.

  No engine changed. The wrapper passes Display through and never matches on the inner value, so
  nothing inside this repo needed an arm. The release is minor because the obligation lands on
  this repo's public surface, not because anything here behaves differently.

## v0.1.8

Pins nauthy v0.3.1 and tightbeam v0.8.2.

### Changed
- **nauthy v0.3.1, tightbeam v0.8.2.** Takes `Cap::expiry()`, so a holder can answer when its own
  grant dies rather than learning it from a refusal that names nothing, and the gate that enforces
  nauthy's datalog budget funnel.

## v0.1.7

Pins bifrost v0.2.3 and tightbeam v0.8.1.

### Changed
- **bifrost v0.2.3, tightbeam v0.8.1.** Carries the temporary-address fix. A node no longer
  publishes or hands out an RFC 8981 temporary IPv6 address: the address was scoped `Internet` and
  so passed the advertisement's own filter, which meant a rotating privacy address went onto every
  network the node joined, and a consumer also handed one to a human, where it is deprecated within
  about a day.

## v0.1.6

The pins follow nauthy v0.3.0, where a busy host stops refusing valid capabilities.

### Changed
- **Pinned to nauthy v0.3.0, tightbeam v0.8.0 and bifrost v0.2.2.** No engine behaviour changes. The
  nauthy bump is the point: every capability check there ran on a one-millisecond wall-clock budget, so a
  loaded host refused a valid capability and reported it as a denial.

## v0.1.5

The sibling pins follow bifrost v0.2.1 and tightbeam v0.7.1.

### Changed
- **Pinned to bifrost v0.2.1 and tightbeam v0.7.1.** No engine behaviour changes. Both bumps are additive
  upstream, and the pins move here so a consumer pinning these engines alongside either sibling resolves
  one copy of each rather than two.

## v0.1.4

The handler pin follows tightbeam v0.7.0, where every target carries a scheme.

### Changed
- **`tightbeam-handler` moves to tightbeam v0.7.0.** No engine behavior changes. The bump has to happen
  here before a consumer can pin both: a consumer on the new tightbeam and these engines on the old one
  resolves two copies of the contract crate, and the witness types stop matching.

## v0.1.3

A stream failure says what failed instead of naming a read that never happened.

### Fixed
- **A non-refusal stream failure no longer claims a read.** Every session failure that is not a typed
  refusal lands in one error variant, including one raised while opening or closing the stream, and that
  variant rendered `read frame`. A client that chains the causes therefore printed `read frame: stream:
  peer went away` for a stream that never opened. The variant is transparent now, so the text is the
  failure's own: `stream: peer went away`.

## v0.1.2

Every failure in every engine is a typed error.

### Changed
- **Every `fetch` and `transfer` failure is a typed error.** `Origin::parse`, `OriginAllowlist::parse`,
  and `compose_url` return `OriginError` and `ComposeError` instead of a message string, so a consumer
  matches the cause (a userinfo-bearing origin, a missing host, a bad join) rather than reading text; the
  refusal a requester sees on the wire renders the same words as before. `transfer`'s receive path returns
  `ReceiveError` naming the step that failed (temp file, wire transfer, flush, directory, save) with the
  path already rendered log-safe, and the crate drops its `eyre` dependency. All four engines now speak
  `thiserror` at their boundaries.
- **The sibling pins follow bifrost, nauthy, and tightbeam.** No engine behavior changes; the bumps carry
  the required transport bind-truth accessor, the pointer-sized `Link`, and the router split.

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
