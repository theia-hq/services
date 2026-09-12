# services

A collection of tightbeam services. A node serves whichever ones it needs, and each engine does one job on
an admitted stream.

The engines are libraries, and your program keeps the transport, the identity, and the access policy. An
engine never sees how the peer was reached or admitted; it gets a stream and does its job. Pick the crates
you want.

## The engines

- **[fetch](crates/fetch/README.md)**: perform an HTTP `GET`/`HEAD` at an origin for an admitted requester, with the target vetted
  against SSRF and the operator's `OriginAllowlist`.
- **[measure](crates/measure/README.md)**: answer `ping` (round-trip time) and `speed` (throughput) tests on a session, or run the
  same tests from the client side.
- **[sshh](crates/sshh/README.md)**: serve a shell over an admitted stream to a standard `ssh` client, with no SSH keys; the
  stream's admission is the only credential.
- **[transfer](crates/transfer/README.md)**: receive one pushed file off an admitted stream, verified end to end with BLAKE3 and saved
  under an output directory.

## Build it and run one engine

The engines are libraries. `measure` carries a runnable example (`crates/measure/examples/reach.rs`):
clone, then run it. Two in-process nodes over the mem transport, a ping and a speed test, with no sockets
involved.

```sh
git clone https://github.com/theia-hq/services
cd services
```

<!-- live-run: cargo run --example reach; the timing numbers vary run to run -->
```sh
cargo run --example reach
```

```
ping: 3 sent, 3 received, 0% loss, rtt min 50.125µs avg 94.736µs max 139.292µs mdev 29.741µs
speed up: 4.00 MiB in 1.8 ms at 2233.4 MiB/s
speed down: 4.00 MiB in 1.8 ms at 2233.4 MiB/s
```

The tests cover each engine end to end, refusal paths included:

```sh
cargo test --locked
```

## Embed an engine

Add the crate you need as a git dependency:

```toml
[dependencies]
fetch = { git = "https://github.com/theia-hq/services" }
```

Each engine README names its entry point and the policy the caller keeps. Git is the only source today, so
pinning a rev is available if you want a fixed point; that choice is the embedder's.

## Honest limits

- **The engines are the work, not the policy.** Admission, exposure, and public-use decisions stay in the
  embedding program. This repo ships no gate, no registry, and no binary.
- **Experimental.** Version `0.0.0`, `publish = false`, consumed from git. The APIs change
  without notice.
- **The shell is remote code execution by construction.** `sshh` refuses to run as root, caps live shells
  at 64 per process, and serves only a stream the caller proves was admitted. Who reaches it, and with
  what capability, is the embedder's policy.
- **`measure` and `transfer` set no byte caps, and `fetch` caps no response body.** An admitted peer can
  move bytes without a bound the engine sets; the embedder's stream and session caps are the only bound.
- **An empty `OriginAllowlist` is unconstrained.** The SSRF guard still holds, so only public origins
  pass, but any public origin does.
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
