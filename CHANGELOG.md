# Changelog

All notable changes to services, newest first.

## Unreleased

### Fixed
- **An over-cap `speed` request is refused before any payload, not truncated.** A metered responder
  answered an explicit request for more bytes than its byte cap by clamping the transfer and closing, so
  a client asking for more than the cap waited on bytes that would never arrive (public `speed -n 128MiB`
  against the 64 MiB cap hung) or counted the short run. The responder now answers that request with the
  typed Layer-2 `RateLimited` refusal in place of the go-ahead (or the drain), and the upload leg reads
  the reply while it sends, so every mode exits promptly with the refusal.

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
  reaches the event; no user-facing prose lives in the engine.
