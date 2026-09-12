# services

Service engines a keyed node runs for a peer: fetch an origin, measure the link, run a shell, receive a file.

Each engine is a Rust library that does one job with an already-authorized byte stream. An engine does not know how the peer was reached or how it was gated; the program that embeds it supplies the transport, the identity, and the admission policy. [tightbeam](https://github.com/theia-hq/tightbeam) is the usual assembly, and [swoosh](https://github.com/theia-hq/swoosh) is one program built from these engines.

## What each engine does

- **fetch** performs an HTTP `GET`/`HEAD` at an origin on the requester's behalf. `serve_fetch` vets the target before connecting (http/https only, every resolved address public, the vetted address pinned into the client, redirects not followed) and streams the response back with `Range` intact. `OriginAllowlist` scopes the origins an instance may reach.
- **measure** answers reach diagnostics: `ping` (RTT) and `speed` (throughput). Both ride a small versioned protocol over a [bifrost](https://github.com/theia-hq/bifrost) stream and are transport-blind. A node answers with `answer_ping`/`answer_speed` or a `Responder`; a client runs a `Ping` or `Speedtest`.
- **sshh** runs a keyless SSH server when handed a stream a real gate already admitted. A standard `ssh` or `scp` client works unchanged, with no SSH keys to manage. `serve` consumes a [nauthy](https://github.com/theia-hq/nauthy) `Admitted` witness, so "authorize before serving" is a compile-time precondition.
- **transfer** receives a pushed file: one stream, one file, verified end to end with BLAKE3 by `bifrost-wire`, saved under a sink directory. A sender-supplied name is reduced to a safe relative path, so a peer cannot write outside that directory.

## Quickstart

Clone and run the tests:

```sh
git clone https://github.com/theia-hq/services
cd services
cargo test --locked
```

Add an engine to a program. Git-only for now, not published to crates.io; pin an exact rev the way the family does:

```toml
[dependencies]
fetch = { git = "https://github.com/theia-hq/services", rev = "<40-char commit>" }
```

## What is not here

The engines are the work a service does, not the policy around it.

- **No handlers, no registry, no gate.** The `Handler` implementations, their public-use markers, and the gate that admits a peer stay at the composition root. A program serves an engine by wrapping its `serve` function in a handler it owns.
- **No fetch policy wrapper.** `fetch` is the engine only. The scoped-allowlist wrapper that refuses to open a fetch service with an unconstrained allowlist lives in [swoosh](https://github.com/theia-hq/swoosh), the consumer, with the public-exposure proof it belongs to.
- **No shell route.** The `sshh` crate is the keyless SSH server engine. The product that arms and exposes a shell service owns that decision and its blast radius.

## Honest limits

- **Experimental.** `0.0.0`, `publish = false`, consumed by exact git revs. The APIs change without notice.
- **The shell is remote code execution by construction.** The engine refuses to run as root, caps live shells at 64 per process, and demands the `Admitted` witness. Who reaches it, and with what capability, is the embedding program's policy.
- **`ping`/`speed` answer with unbounded per-request bytes**, bounded only by the session and stream caps. A node that advertises them to strangers consents to that drain.
- **`transfer` has no byte cap.** An admitted peer can fill the sink directory. Bounding that is the operator's job.
- **An empty `OriginAllowlist` is unconstrained.** The SSRF guard still holds, so only public origins pass, but any public origin does.
- **One repo, one rev.** Four engines share a rev; a consumer depends on the crate it needs, and a bump for one engine moves that rev for all four.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions.
