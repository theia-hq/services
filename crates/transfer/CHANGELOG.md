# Changelog: transfer

All notable changes to `transfer`, newest first. It carries its own version and its own cadence, so a
release here moves `transfer` and nothing else in this repository.

## v0.6.0

### Changed
- Builds against tightbeam v0.17.0's handler contract, nauthy v0.10.0 and bifrost v0.8.0. The
  contract itself is unchanged, but the sibling types it names come from the new releases, so a
  consumer must pin those same releases.

## v0.5.0

### Changed
- **Needs Rust 1.91 or newer.** The minimum was 1.85. tightbeam v0.16.0 and bifrost v0.7.0 need
  1.91.
- Builds against tightbeam v0.16.0's handler contract, nauthy v0.9.0 and bifrost v0.7.0. The
  contract itself is unchanged.

## v0.4.0

### Changed
- **`Recv` no longer logs a received file; it hands it to you.** Build it with
  `Recv::new(out).with_sink(sink)` and each landed file reaches your `ReceivedSink` as a `Received` value,
  once, after it is in place. The path is the sender's name, so escape it before printing it. A sink must
  not block. Without a sink the engine prints nothing.
- **A path in an error message also escapes letters that print as blank space**, so a name made only of
  them is never invisible in a log.

Nothing before v0.3.0 is recorded here. Until then the four engines released together under one
number, and those entries are about the set rather than about any one engine, so they stay in the
[repository changelog](../../CHANGELOG.md). `transfer` starts from the v0.3.0 it holds today, reached
in lockstep rather than earned alone, and that stands.
