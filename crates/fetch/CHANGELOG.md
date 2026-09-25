# Changelog: fetch

All notable changes to `fetch`, newest first. It carries its own version and its own cadence, so a
release here moves `fetch` and nothing else in this repository.

Nothing before v0.3.0 is recorded here. Until then the four engines released together under one
number, and those entries are about the set rather than about any one engine, so they stay in the
[repository changelog](../../CHANGELOG.md). `fetch` starts from the v0.3.0 it holds today, reached
in lockstep rather than earned alone, and that stands.

## Unreleased

### Changed
- Builds against tightbeam v0.17.1.

## v0.5.0

### Changed
- Builds against tightbeam v0.17.0's handler contract, nauthy v0.10.0 and bifrost v0.8.0. The
  contract itself is unchanged, but the sibling types it names come from the new releases, so a
  consumer must pin those same releases.

## v0.4.0

### Changed
- **Needs Rust 1.91 or newer.** The minimum was 1.85. tightbeam v0.16.0 and bifrost v0.7.0 need
  1.91.
- Builds against tightbeam v0.16.0's handler contract, nauthy v0.9.0 and bifrost v0.7.0. The
  contract itself is unchanged.
