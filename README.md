# services

The general theia service engines: the work a keyed node does for a peer on an already-admitted stream.

Each engine is a Rust library that does one job on a byte stream. An engine never sees how the peer was
reached or admitted. The embedding program supplies the transport, the identity, and the access policy,
then hands the engine a stream.

## The engines

- **fetch**: perform an HTTP `GET`/`HEAD` at an origin for an admitted requester, with the target vetted
  against SSRF and the operator's `OriginAllowlist`.
- **measure**: answer `ping` (round-trip time) and `speed` (throughput) tests on a session, or run the
  same tests from the client side.
- **sshh**: serve a shell over an admitted stream to a standard `ssh` client, with no SSH keys; the
  stream's admission is the only credential.
- **transfer**: receive one pushed file off an admitted stream, verified end to end with BLAKE3 and saved
  under an output directory.

## Build it and run one engine

No engine ships a binary; the tests are the runnable examples. Clone, build, and run the suite:

```sh
git clone https://github.com/theia-hq/services
cd services
cargo test --locked
```

The `measure` integration test is the end-to-end example: two in-process nodes, a `ping`, and a `speed`
test with no sockets involved. Run it alone:

<!-- capture: cargo test --locked -p measure --test reach -->
```sh
cargo test --locked -p measure --test reach
```

```
running 7 tests
test a_speed_frame_on_a_ping_only_node_carries_the_unsupported_refusal ... ok
test a_ping_frame_on_a_speed_only_node_carries_the_unsupported_refusal ... ok
test bidir_moves_bytes_in_both_directions_at_once ... ok
test ping_measures_round_trips_with_no_loss ... ok
test observing_reports_every_probe_in_order_as_it_lands ... ok
test speed_moves_bytes_in_each_direction ... ok
test time_bounded_speed_respects_the_duration_not_a_byte_count ... ok

test result: ok. 7 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.75s
```

## Embed an engine

Add the crate you need as a git dependency, pinned to an exact commit:

```toml
[dependencies]
fetch = { git = "https://github.com/theia-hq/services", rev = "<40-char commit>" }
```

Each engine README names its entry point and the policy the caller keeps.

## Honest limits

- **The engines are the work, not the policy.** Admission, exposure, and public-use decisions stay in the
  embedding program. This repo ships no gate, no registry, and no binary.
- **Experimental.** Version `0.0.0`, `publish = false`, consumed by exact git revs. The APIs change
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
