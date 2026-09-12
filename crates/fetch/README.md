# fetch

An HTTP origin fetch a keyed node performs on an admitted requester's behalf.

`serve_fetch` reads a `FetchRequest` off an already-admitted stream, performs the `GET`/`HEAD` at the
origin, and streams the response back with `Range` intact, so a resumable download works. It is a fetch
scoped to one origin, not a general proxy or an open VPN.

## The origin is vetted before the connection

Only `http` or `https` targets pass, and the host must resolve entirely to public addresses. This stops an
admitted caller from turning the node into an SSRF pivot: reaching its loopback, its LAN, or the cloud
metadata endpoint (`169.254.169.254`) to steal instance credentials. The vetted address is pinned into the
client, so a DNS rebind between the check and the connect cannot swap a public answer for a private one.

## The operator scopes the origins

`OriginAllowlist` constrains the service to a fixed set of origins. The operator parses the list at setup
time; `serve_fetch` refuses a request whose origin is not on it, before any connection and in front of the
SSRF guard.

The check is over the normalized `(scheme, host, port)` triple, compared exactly. A request URL carrying
userinfo (`https://user@host/`) is rejected outright, so it can never be parsed around to a different
host.

## The entry point

`serve_fetch(writer, reader, &allow)` is the whole engine. The caller owns admission and exposure, and
decides who may use the service and whether it is open. The `http` module publishes the request and
response framing, so the caller's client side speaks the same wire.

## Honest limits

- **`GET` and `HEAD` only.** Other methods are refused. Redirects are forwarded to the requester, not
  followed here: the caller decides whether to follow.
- **No byte cap on the response body.** An admitted requester can pull as much as the origin serves,
  bounded only by the caller's stream and session caps.
- **An empty `OriginAllowlist` is unconstrained.** The SSRF guard still holds, so only public origins
  pass, but any public origin does.
- **The allowlist gates the origin only.** The path and query are the requester's to choose.
- **TLS terminates at the node.** The node handles the request and response in plaintext; the requester
  trusts the node, not the origin certificate.
- **Experimental.** Version `0.0.0`, `publish = false`, consumed from git. The API changes
  without notice.

## License

Licensed under either of Apache-2.0 or MIT at your option.
