# services

The general service engines: fetch, measure, sshh, and transfer, each serving one job on an
already-admitted stream, usable on their own over tightbeam. A node serves whichever ones it needs.

The engines are libraries, and your program keeps the transport, the identity, the gate, and the launcher.
Each engine ships its own entry: a `Handler` impl that declares the service's exposure ceiling (`Never`, or
`OptIn` for a service an operator may open deliberately) and its responder-side metering. An engine never
sees how the peer was reached or admitted; it gets a prepared proof and a stream and does its job.

## The engines

- **[fetch](crates/fetch/README.md)**: perform an HTTP `GET`/`HEAD` at an origin for an admitted requester, with the target vetted
  against SSRF and the operator's `OriginAllowlist`.
- **[measure](crates/measure/README.md)**: answer `ping` (round-trip time) and `speed` (throughput) tests on a session, or run the
  same tests from the client side.
- **[sshh](crates/sshh/README.md)**: serve a shell over an admitted stream to a standard `ssh` client, with no SSH keys; the
  stream's admission is the only credential.
- **[transfer](crates/transfer/README.md)**: receive one pushed file off an admitted stream, verified end to end with BLAKE3 and saved
  under an output directory.

This page describes the default branch.

## Build it and test it

Clone, then run the tests that cover each engine end to end, refusal paths included:

```sh
git clone https://github.com/theia-hq/services
cd services
cargo test --locked
```

## Embed an engine

Add the engine and the contract as git dependencies:

```toml
[dependencies]
tightbeam-handler = { git = "https://github.com/theia-hq/tightbeam" }
measure = { git = "https://github.com/theia-hq/services" }
```

The engine is the entry: bind the handler value, and a dispatcher (tightbeam's `Router`, or your own
dispatch over the same contract) proves the route and hands it prepared streams.

```rust
use measure::server::{Limits, Ping};

let limits = Limits::owner();
router.service("ping".parse()?, Ping::new(&limits))?;
```

A route an operator wants OPEN binds the metered entry instead (`measure::server::MeteredPing::new()`),
which carries the public safety caps by construction and takes no profile. The owner entry declares
`Never`, so the public proof refuses it; the metered entry declares `OptIn` and is the only one the proof
will open.

Each engine README names its handler types and the policy the caller keeps. Git is the only source today, so
pinning a rev is available if you want a fixed point; that choice is the embedder's.

## Honest limits

- **The engines ship the ceiling, the root ships the assembly.** Each engine's handler declares its
  exposure (`Never`, or `OptIn` where an operator may open it deliberately) and its metering. The
  diagnostics ship two entries per service: the owner entry is unbounded and `Never` (family routes), the
  metered entry is capped by construction and `OptIn` (the only one an open route can bind). Admission,
  the open decision, transport, and identity stay in the embedding program. This repo ships no gate, no
  registry, and no binary. Each engine README carries that engine's limits.
- **Experimental.** Version `0.0.0`, `publish = false`, consumed from git. The APIs change
  without notice.
- **One repo, one rev.** All four engines share a rev; a bump for one moves the pin for the others. A
  consumer depends on only the crate it needs.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the work
by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any additional
terms or conditions.
