# TBH1, the origin-fetch wire

The frame exchange inside an admitted stream when a requester asks a keyed node to perform one HTTP
`GET` or `HEAD` at an origin on its behalf: the requester names a method, an absolute URL and the
headers to forward, the node vets the target and performs the request, and the origin's status, headers
and body come back over the same stream.

This document is the wire, not the implementation of it. A second implementation built from this page
alone must interoperate with the one in `src/http.rs`, octet for octet. Where a statement here is
weaker than the code (a `MAY` the code happens to satisfy), the weaker statement is the contract. Where
the code is weaker than a `MUST` here, it is named in [Known gaps](#known-gaps) rather than left for a
reader to discover.

- [Conventions](#conventions)
- [Where this wire sits](#where-this-wire-sits)
- [The magic: identity and version](#the-magic-identity-and-version)
- [The request frame](#the-request-frame)
- [The response frame](#the-response-frame)
- [The response tag, and why it is a literal](#the-response-tag-and-why-it-is-a-literal)
- [The body](#the-body)
- [The state machine](#the-state-machine)
- [Unrecognised values](#unrecognised-values)
- [Refusal semantics](#refusal-semantics)
- [Caps](#caps)
- [Versioning and the frozen response](#versioning-and-the-frozen-response)
- [Known gaps](#known-gaps)
- [Test vectors](#test-vectors)
- [Regenerating the vectors](#regenerating-the-vectors)

## Conventions

`MUST`, `MUST NOT`, `SHOULD`, `SHOULD NOT` and `MAY` carry their RFC 2119 meanings. They are used only
where a divergent choice breaks interoperation or a security property; where an implementation is free,
this page says so.

An **octet** is 8 bits. Every multi-octet integer on this wire is **unsigned big-endian**. There are no
signed integers, no varints, and no alignment or padding anywhere: every field begins at the octet after
the previous one ends. Text fields are UTF-8 and are never NUL-terminated; their length is always
carried in front of them.

The **requester** is the side that opens a stream and asks for a fetch. The **host** is the keyed node
that performs it. HTTP terms (method, status, header, origin, `Range`) carry their HTTP meanings; this
wire carries them and, except where this page says otherwise, gives them no meaning of its own.

Octets are shown as lowercase hexadecimal pairs. Grouping and line breaks inside a vector block are
presentation only.

## Where this wire sits

TBH1 is carried on one bidirectional stream that the host's gate has **already admitted**. The tunnel
preamble on that stream has completed and the host has written its own success answer before the first
octet of this wire moves. That single fact shapes the rest of this page:

- **The peer is admitted before this wire begins.** TBH1 performs no authentication, carries no
  credential field, and has nothing to say about authorization. Admission happened one layer down.
- **The pre-gate disclosure question does not arise.** A wire whose first frame a stranger can provoke
  has to weigh every octet it writes back as a fingerprint. This one does not: anything this wire says
  is said to a peer the host already chose to serve, and the only host fact any refusal here carries is
  a wire version that any served request would reveal anyway.
- **Admission is not always membership.** The unscoped engine is member-only and can never face an open
  gate. The scoped engine carries a non-empty operator origin allowlist, and an operator `MAY` open it
  to callers holding no credential, in which case the peer is an anonymous stranger bounded by that
  allowlist and by the [caps](#caps). An implementation `MUST NOT` infer from arrival on this wire that
  the peer is a member.

Four rules bind this wire to the layer under it:

1. **The requester opens every stream, and one stream carries one fetch.** A host `MUST NOT` open a
   stream toward a requester and call it fetch. There is no pipelining, no second request on a served
   stream, and no connection reuse: a second fetch is a second stream.
2. **TLS terminates at the host, not at the requester.** The host is the HTTP client. A requester does
   not see the origin's certificate and `MUST NOT` claim end-to-end origin authentication from a
   successful fetch. What it gets is the host's assertion about what the origin said.
3. **The host bounds the first frame itself.** The tunnel's own pre-gate deadline stops at admission, so
   this wire carries its own deadline on the request frame; otherwise an admitted peer could hold a
   stream open by dribbling length prefixes. See [Caps](#caps).
4. **This wire defines no trailer, no keepalive and no close frame.** The body ends when the stream
   ends.

## The magic: identity and version

Every wire in this family opens with four ASCII octets that split into an **identity** and a **version**:

```text
identity = the maximal leading run of [A-Z]   (0x41 to 0x5a)
version  = the trailing digits                (0x30 to 0x39)
```

The rule is self-delimiting, so it parses a 2+2 magic and a 3+1 magic without being told which it is
looking at. For this wire:

| magic | identity | version |
| ----- | -------- | ------- |
| `TBH1` (`54 42 48 31`) | `TBH` | `1` |

The identity is **frozen forever**. A stream that does not open with `TBH` is not a fetch stream, and
that is the only thing an identity mismatch is ever allowed to mean. The version names the request
grammar, and only the request grammar: see [Versioning](#versioning-and-the-frozen-response).

A receiver `MUST` split the four octets by the rule above and compare the **identity** first, then the
version. Comparing all four at once collapses two different facts into one and makes the answer in the
next section impossible to write.

**`TB` is a prefix of `TBH`, and that collision is why the rule is a rule.** The tunnel wire this one
rides inside has identity `TB`, and a reader of that wire which compared a fixed two-octet prefix would
take a `TBH1` head for a tunnel stream on an unserved version and answer it, handing its own wire
version to something that does not speak it. The maximal-run rule is what closes that by parse rather
than by convention. **A new wire `MUST NOT` be given an identity that is a prefix of an existing one**,
or that an existing one is a prefix of; where such a pair already exists, every reader of the shorter
identity `MUST` require the capital run to END where its identity ends.

This wire does not need that run-end check, and the reason is worth stating so a reimplementer does not
add one and then wonder what it guards. `TBH` is three octets and the magic is four, so a longer
identity beginning with `TBH` would need at least four identity octets plus at least one digit, which
does not fit. No such wire can exist. The octet after `TBH` is therefore always the version, and it is
**not held to being a digit**: an unserved version is answered on its own terms whether or not it is
printable.

The three conditions a host can meet on the head of a stream are distinct, and each has exactly one
correct answer:

| condition | what it means | the host's answer |
| --------- | ------------- | ----------------- |
| identity is not `TBH` | not a fetch stream | **no octets at all**; close the stream |
| identity is `TBH`, version is not served | a fetch peer on another grammar | an **error frame** naming **both** versions |
| the frame cannot be read (short, closed, timed out, malformed) | nothing usable was received | no octets; close the stream |

A foreign identity `MUST` receive **zero** octets in reply. Nothing true can be said to a protocol we
cannot name, and a reply would be a guess at what is meaningful to it. On this wire that is a
correctness rule rather than a disclosure one: the peer is already admitted, so silence buys no privacy
here, it simply avoids writing octets into a stream whose grammar is unknown.

A served identity on an unserved version `MUST` be answered, because the peer has already proved it
speaks fetch, and both ends can act on the fact. This is the single reason the response frame is
frozen: a host that cannot parse a peer's request must still be able to write something that peer can
parse.

Note that the third condition is wider here than the answerable one: **every** failure past the version
check, including a truncated field, invalid UTF-8 and an over-count header block, closes the stream with
no octets. That is [known gap 3](#known-gaps).

## The request frame

Written by the requester, once, as the first octets of the stream.

| offset | field | width | encoding |
| ------ | ----- | ----- | -------- |
| 0 | identity | 3 | ASCII `TBH` (`54 42 48`) |
| 3 | version | 1 | ASCII `1` (`31`) |
| 4 | method length | 2 | u16 big-endian, octets of UTF-8 |
| 6 | method | *method length* | UTF-8, no terminator |
| ... | url length | 2 | u16 big-endian |
| ... | url | *url length* | UTF-8 |
| ... | header count | 2 | u16 big-endian, at most 128 |
| ... | headers | *see below* | *header count* repetitions |

Each header is two length-prefixed strings, back to back, with no separator and no terminator:

| field | width | encoding |
| ----- | ----- | -------- |
| name length | 2 | u16 big-endian |
| name | *name length* | UTF-8 |
| value length | 2 | u16 big-endian |
| value | *value length* | UTF-8 |

The frame ends after the last header. There is no frame-level length prefix, so a reader finds the end
by consuming the fields in order.

### method

`GET` or `HEAD`, and nothing else. The field is a length-prefixed string rather than a tag so the
grammar can grow without a version bump, but v1 serves exactly those two. A requester `MUST NOT` send
another method; a host that receives one `MUST` refuse with an error frame naming the method it was
given.

The comparison is **exact and case-sensitive**: `get` is not `GET` and is refused.

### url

The absolute URL to fetch, as a UTF-8 string. It `MUST` be absolute and `MUST` carry a scheme of `http`
or `https`. A host refuses, with an error frame and before any connection, a URL that:

- does not parse,
- has a scheme other than `http` or `https`,
- carries userinfo (`https://user@host/`), which is rejected entirely rather than stripped,
- has no host, or no port and no default port for its scheme,
- names an origin outside the service's operator allowlist, when the service carries one,
- names a host that does not resolve, resolves to nothing, or resolves to **any** non-public address.

Those last two are the SSRF guard and they are not advisory. A host `MUST` require **every** resolved
address to be globally routable unicast and `MUST` refuse the whole request if any one of them is not,
rather than picking a public one from a mixed answer. Loopback, private, link-local (which includes the
cloud metadata address `169.254.169.254`), shared/CGNAT, unspecified, broadcast, documentation and
multicast addresses are all non-public, and an IPv6 address that **embeds** an IPv4 address (v4-mapped,
the NAT64 well-known prefix `64:ff9b::/96`, or the deprecated v4-compatible form) `MUST` be unwrapped
and judged as that IPv4 address.

A host `MUST` pin the connection to the address it vetted. Resolving once for the check and again for
the connect is a DNS-rebinding hole, and the pin is what closes it.

A host that carries an operator allowlist `MUST` derive the request's origin from the **same** parse of
the URL that the SSRF guard reads its host from. Two parses of one URL is itself the evasion.

### headers

Forwarded to the origin verbatim, in order, which is the whole point: a `Range` header reaches the
origin, the origin answers `206` with a `Content-Range`, and a resumable download works.

A host `MUST NOT` forward the hop-by-hop headers, matched **case-insensitively**: `host`, `connection`,
`proxy-connection`, `proxy-authorization`, `keep-alive`, `transfer-encoding`, `upgrade`. They are
dropped silently rather than refused, because they describe the requester's hop and have no meaning at
the host's. `Host` is dropped because the host derives it from the URL.

Everything else passes through untouched. This wire does not canonicalise header names, does not
deduplicate them, and does not reorder them.

A reader `MUST` reject a header count above 128 **before** allocating for it. A requester `MUST NOT`
send more than 128 headers; the shipped encoder does not enforce that, which is
[known gap 2](#known-gaps).

## The response frame

Written by the host, once, before any body octet. **This frame is frozen**; see
[Versioning](#versioning-and-the-frozen-response).

| offset | field | width | encoding |
| ------ | ----- | ----- | -------- |
| 0 | response tag | 4 | ASCII `TBH1` (`54 42 48 31`), a frozen literal |
| 4 | tag | 1 | `00` ok, `01` error |
| 5 | *the tag's own fields* | *see below* | |

### `00` ok

| offset | field | width | encoding |
| ------ | ----- | ----- | -------- |
| 5 | status | 2 | u16 big-endian |
| 7 | header count | 2 | u16 big-endian, at most 128 |
| ... | headers | *as in the request frame* | *header count* repetitions |

The body follows immediately after the last header and runs to the end of the stream.

`status` is the origin's HTTP status forwarded verbatim: `200`, `206`, `301`, `404`, and any other the
origin returns. A host `MUST NOT` interpret it, rewrite it, or substitute one of its own, and in
particular `MUST NOT` follow redirects: a `3xx` and its `Location` header are forwarded so the requester
decides. A requester `MUST` treat any `u16` as a possible status.

Headers are the origin's response headers, forwarded verbatim, in the order the origin gave them, up to
the first 128. Two details a reimplementer will otherwise discover the hard way: a header whose value is
not representable as a string is **dropped silently** rather than refused or escaped, and the 128 cap is
applied after that drop, so it counts survivors.

### `01` error

| offset | field | width | encoding |
| ------ | ----- | ----- | -------- |
| 5 | message length | 2 | u16 big-endian |
| 7 | message | *message length* | UTF-8 |

The frame ends after the message. **No body follows an error frame**, and the host closes its write half
immediately after writing it.

The message is human-readable prose. A requester `MAY` show it to a person and `MUST NOT` parse it,
match on it, or branch on its content. There is no machine-readable class on this frame, which is
[known gap 4](#known-gaps) and the most consequential limit of this wire.

## The response tag, and why it is a literal

The four octets that open every response frame are the literal `TBH1`. They are **not** derived from
this wire's identity and version, and an implementation that derives them is wrong in a way that will
not show up until the day it matters most.

Today the two are the same four octets, which is exactly why this is easy to get wrong. The request
frame opens `TBH1` because its identity is `TBH` and its version is `1`. The response frame opens `TBH1`
because that is the constant. When the request grammar moves to `TBH2`, the request frame will open
`TBH2` and the response frame `MUST` still open `TBH1`.

The reason is the version answer. A host meeting a request it cannot parse writes an error frame naming
both versions, and the peer that frame is written for is by definition running another build. A derived
response tag would make that host write `TBH2` at the head of the answer; the `TBH1` peer's reader
compares the tag it knows, sees a mismatch, and rejects the whole frame as not a fetch stream. The one
sentence the frame exists to carry is lost, and the peer gets the bare closed stream that this wire's
version answer exists to replace. The tag is frozen precisely so that the answer survives the break it
is announcing.

Two consequences follow, and both are normative:

- A host `MUST` write the literal `TBH1` at the head of every response frame at every future version of
  the request grammar. A requester `MUST` compare that literal and `MUST NOT` compare its own magic.
- A requester `MUST` decode a frame by the **direction it arrived from**, never by its opening octets.
  Today a request frame and a response frame open with identical octets, so the tag is not a
  discriminator between them and was never meant to be. A response is only ever read on a stream the
  requester opened and has already written its request on.

A tag mismatch on the response therefore means a foreign or corrupt stream and nothing else. There is no
version to distinguish, because there is only ever one value.

## The body

After an `ok` frame, every remaining octet in the host-to-requester direction is the origin's response
body. It is **raw and unframed**: no length prefix, no chunk header, no delimiter, no trailer. The body
ends when the host closes its write half.

The requester-to-host direction carries nothing after the request frame. This wire has no request body,
because it serves only `GET` and `HEAD`.

A `HEAD` fetch returns the origin's status and headers and an empty body, exactly as HTTP intends. The
`Content-Length` the origin declares is forwarded and describes the body a `GET` would have returned.

**A truncated body is not distinguishable from a complete one on this wire.** A metered host that hits
its byte cap or its deadline mid-body closes the stream, and a clean close is the only end this wire
has. It `MUST NOT` append an error frame in that case: the frame would be read as body octets. A
requester that needs to know `MUST` compare the delivered length against the origin's forwarded
`Content-Length` itself, and `MUST NOT` assume the two agree. This is [known gap 5](#known-gaps).

## The state machine

Both sides are strictly sequential. There is no pipelining and no interleaving.

**Host.**

| state | event | action | next |
| ----- | ----- | ------ | ---- |
| `Admitted` | no complete request frame within the read deadline | close, **no octets** | `Closed` |
| `Admitted` | identity is not `TBH` | close, **no octets** | `Closed` |
| `Admitted` | identity `TBH`, version not served | write error naming both versions, close the write half | `Closed` |
| `Admitted` | frame unreadable (short, invalid UTF-8, header count over cap) | close, **no octets** | `Closed` |
| `Admitted` | frame read | vet the request | `Vetting` |
| `Vetting` | method is not `GET` or `HEAD` | write error | `Closed` |
| `Vetting` | the url does not parse, carries userinfo, or has no host or port | write error | `Closed` |
| `Vetting` | the origin is outside the operator allowlist | write error | `Closed` |
| `Vetting` | the scheme is not `http` or `https` | write error | `Closed` |
| `Vetting` | the host does not resolve, or resolves to nothing | write error | `Closed` |
| `Vetting` | any resolved address is non-public | write error | `Closed` |
| `Vetting` | vetted | perform the origin request, pinned to the vetted address | `Fetching` |
| `Fetching` | the origin fails before a response header | write error | `Closed` |
| `Fetching` | the deadline elapses before a response header | write error naming a timeout | `Closed` |
| `Fetching` | the origin answered | write ok (status and headers) | `Streaming` |
| `Streaming` | the origin body ends | close the write half | `Closed` |
| `Streaming` | the byte cap is reached | close truncated, **no further frame** | `Closed` |
| `Streaming` | the deadline elapses | close truncated, **no further frame** | `Closed` |
| `Streaming` | the origin body fails | close the write half | `Closed` |

Four orderings in that table are normative rather than incidental:

- The **allowlist check runs before the SSRF guard**, and both run before any connection. The allowlist
  is the operator's declared scope and refusing outside it should not first cost a DNS lookup, let alone
  a connection.
- The **SSRF guard runs before the connection**, and the connection is **pinned** to the address it
  vetted. A check that does not pin is a check the resolver can undo.
- The **deadline covers the connect, the response head and the body as one clock.** A separate clock per
  phase lets a slow origin restart the budget at each step.
- Once an `ok` frame is written, **no further frame may be written in that direction, ever.** Every
  failure after that point is a close.

**Requester.**

| state | event | action | next |
| ----- | ----- | ------ | ---- |
| `Opened` | ready | write the request frame | `AwaitingResponse` |
| `AwaitingResponse` | the first four octets are not `TBH1` | fail the stream | `Closed` |
| `AwaitingResponse` | tag `00` | read status and headers, then read the body to EOF | `Reading` |
| `AwaitingResponse` | tag `01` | surface the message; no body follows | `Closed` |
| `AwaitingResponse` | any other tag | fail the stream; `MUST NOT` be read as ok | `Closed` |
| `AwaitingResponse` | short read, invalid UTF-8, or header count over cap | fail the stream | `Closed` |
| `Reading` | EOF | the body is complete as far as this wire can say | `Closed` |

A requester `MUST NOT` write an octet after its request frame. There is no request body and nothing the
host would read it as.

## Unrecognised values

The table a second implementation is judged by. Every value this wire can carry that a receiver might
not know, and what the receiver does with it:

| where | unrecognised value | receiver | required behaviour |
| ----- | ------------------ | -------- | ------------------ |
| request identity | anything but `TBH` | host | close with **no octets written** |
| request version | any served-identity version the host does not speak | host | write an error frame whose message names the peer's version and the host's |
| request method | anything but `GET` or `HEAD` | host | write an error frame naming the method it was given |
| url scheme | anything but `http` or `https` | host | write an error frame |
| url | carries userinfo | host | write an error frame; `MUST NOT` strip it and continue |
| header name or value | any content | host | forward verbatim, unless hop-by-hop, which is dropped silently |
| header count | greater than 128 | either | reject **before** allocating the buffer |
| any text field | not valid UTF-8 | either | fail the frame; never repair lossily |
| any text field | short, or the stream closed mid-field | either | fail the frame; close with no octets |
| response tag (the four octets) | anything but `TBH1` | requester | fail the stream as not a fetch stream |
| response frame tag | anything but `00` or `01` | requester | fail the stream; `MUST NOT` be read as ok, and `MUST NOT` be skipped |
| status | any `u16` | requester | accept; this wire assigns no meaning beyond HTTP's |
| response header value | not representable as a string | host | **dropped** from the forwarded list |
| body octets | any content | requester | never inspected by this wire |

The rule behind the two frame-tag rows is one rule: a value a build cannot name is never quietly
promoted to one it can. An unknown response frame tag is followed by fields of unknown width, so it
cannot be skipped and `MUST` fail the stream rather than be guessed at.

## Refusal semantics

**Every refusal on this wire is the same frame with different prose.** Tag `01`, a length-prefixed UTF-8
message, and nothing else. There is no code, no class, and no machine-readable distinction between a
method this service does not serve, an origin outside the operator's scope, an SSRF refusal, a DNS
failure, an unreachable origin and a timeout.

The consequence is normative and it cuts both ways:

- A requester `MUST NOT` branch on the message text. Matching on prose makes the host's wording part of
  the wire, and it is not.
- A requester therefore `MUST` treat every error frame as one class: **this fetch did not happen, and a
  person should read why.** It cannot retry selectively, because it cannot tell a transient origin
  failure from a permanent policy refusal.

That is a real limitation rather than a simplification, and it is [known gap 4](#known-gaps).

What a host may put in the message is bounded:

- The **version mismatch** message is FIXED text plus the two version tags and nothing else. A host
  `MUST NOT` interpolate any other host state into it. It is the one refusal written before the host has
  read a request it understands, and a message that varied with host state would put a side channel on
  it.
- Every other message `MAY` name the requester's **own input** back to it: the method it sent, the URL
  or origin it asked for. Those are already known to the requester and reveal nothing.
- The SSRF refusal additionally names the **resolved address** that failed the check. That is a
  resolution result the requester could obtain itself, and naming it is what makes the refusal
  actionable rather than mysterious.
- A host `MUST NOT` put its operator allowlist, its configuration, its load, or the contents of any
  other request into a message.

An error frame is **not** an authorization outcome. The gate already ruled, and it ruled admit. A
requester `MUST NOT` record an error here as a ruling about its credentials.

## Caps

Every bound this wire places on a receiver, with its unit. A cap marked `MUST` is part of the wire: an
implementation that exceeds it writes frames a conformant peer rejects, or accepts frames a conformant
peer never writes. A cap marked *host policy* is a resource decision each host makes for itself; the
value shipped here is given so a second implementation has a sane starting point, and the `MUST` is only
that **some** finite bound exists.

| bound | value | unit | strength |
| ----- | ----- | ---- | -------- |
| any length-prefixed text field | 65 535 | octets | `MUST` (the u16 length cannot express more) |
| headers per frame | 128 | headers | `MUST` on the reader, both frames (see gap 2) |
| request frame, total | 16 908 552 | octets | `MUST` (derived: `4 + 2+65535 + 2+65535 + 2 + 128 x 2 x (2+65535)`) |
| ok frame, total | 16 777 481 | octets | `MUST` (derived: `4 + 1 + 2 + 2 + 128 x 2 x (2+65535)`) |
| error frame, total | 65 542 | octets | `MUST` (derived: `4 + 1 + 2 + 65535`) |
| methods served | `GET`, `HEAD` | | `MUST` |
| schemes served | `http`, `https` | | `MUST` |
| request frame read deadline | 10 | seconds | host policy; a finite deadline is a `MUST` |
| response body | 16 777 216 | octets | host policy (metered engines only) |
| whole origin operation | 30 | seconds | host policy (metered engines only) |

The request frame read deadline is the one host-policy bound with a wire consequence. The tunnel
underneath stops its own pre-gate clock at admission, so without this one an admitted peer could open a
stream, write three octets, and park a host task and its buffers indefinitely. A host `MUST` bound the
time from admission to a complete request frame, and `MUST` close the stream with no octets written when
it elapses. A requester `SHOULD` therefore write its request immediately on opening a stream.

The body cap and the total timeout apply to the **scoped** engine, which is the one an operator may open
to strangers, and they are not configurable there: it carries both by construction. The **unscoped**
engine is member-only and applies neither, streaming the origin to its own end. An implementation
`MUST NOT` serve an unbounded fetch on a route open to strangers: an unbounded origin fetch with no
operator scope is an egress relay any stranger could aim at any public origin, which has no legitimate
public use.

Both bounds end the stream by closing it, never by writing a frame. See [The body](#the-body).

## Versioning and the frozen response

There is **no negotiation** on this wire. A requester speaks exactly one version, a host serves exactly
one version, and there is no field in which to offer a list. A requester `MUST NOT` attempt to downgrade
by retrying with another version's magic; the answer to a version mismatch is to run the same release at
both ends.

**The request frame may break with a version bump.** It carries the evolving vocabulary. A host
`MUST NOT` skip a request field it does not understand: the frame has no frame-level length prefix, so a
field it cannot read is a frame whose end it cannot find.

**The response frame is frozen.** A future version of the request grammar:

- `MUST NOT` change the four-octet response tag, which stays the literal `TBH1`,
- `MUST NOT` change the meaning or width of the frame tag byte,
- `MUST NOT` change the shape or meaning of the `ok` or `error` frames,
- `MAY` add a new frame tag.

The freeze is what makes a version mismatch answerable at all: a host that cannot parse a peer's request
must still be able to write something that peer can parse. The response tag is the load-bearing half of
that promise, which is why it has [a section of its own](#the-response-tag-and-why-it-is-a-literal).

**The version answer rides the `error` frame, and an extension `MUST NOT` move it.** A new frame tag is
legible only to a peer that already knows it, because this reader rejects an unknown tag outright rather
than reading it as something it cannot name. The peer a version answer is written for is by definition
on another build, so the answer has to ride the one frame every shipped build already decodes. An added
tag `MAY` therefore serve new conditions and `MUST NOT` carry anything an older peer needs to
understand.

The freeze promises "a break names itself and announces itself", never "no break".

## Known gaps

Stated here rather than discovered later. Each is a real limit of the wire as it stands, with the fix it
is waiting for.

**1. The two frames are told apart only by direction.** A request frame and a response frame open with
the same four octets today, and after a version bump they will open with different ones for a reason
that has nothing to do with telling them apart. Nothing in either frame identifies which it is. That is
sound while one stream carries one fetch in one direction each way, and it forecloses ever multiplexing
two exchanges on one stream without a new framing layer. No fix is planned, because the constraint that
makes it safe is the same constraint that makes this wire simple.

**2. The encoder accepts a header list its own decoder refuses.** The reader rejects a header count
above 128; the writer only fails above 65 535, the most the count field can express. A conformant writer
`MUST` cap at 128, and this one does not, so it can emit a frame it cannot itself read. It is not
reachable through the shipped host, which caps forwarded origin headers before encoding, but it is
reachable by any caller building a request directly. The named fix is one check in the writer, matching
the reader's. It needs no version bump.

**3. An unreadable frame gets silence, where an unreadable version gets an answer.** A head that names
`TBH1` has proved the peer speaks this grammar, so a truncated field, invalid UTF-8 or an over-count
header block could be answered with an error frame the peer would understand. It is not: every failure
past the version check closes the stream with no octets. A requester therefore `MUST` treat a closed
stream with no response as "the host could not read my request", which is the least actionable outcome
on the wire. The fix is to answer an error frame for a malformed body too, which needs no version bump
because it adds no tag.

**4. Refusals carry prose, not a class.** Every refusal is one frame with a human sentence, so a
requester cannot tell a transient failure (origin unreachable, timeout) from a permanent one (method not
served, origin outside the allowlist, SSRF refusal) except by reading text it is forbidden to parse. A
requester cannot implement retry, backoff, or a typed error surface. This is the largest gap on this
wire. The named fix is a refusal **code** byte in front of the message, the shape the tunnel wire under
this one already uses, with the message kept as prose beside it. It needs the `error` frame to grow a
field, so it rides a version bump of the response frame, which the freeze does not currently permit: it
would have to be an added frame tag, legible only to peers that know it, with the old tag kept for the
version answer.

**5. A truncated body reads as a complete one.** The body is unframed and EOF-delimited, so a host
stopped by its 16 MiB cap or its 30 second deadline closes exactly as a host that finished does, while
the forwarded `Content-Length` still describes the whole origin body. A requester that does not compare
the two accepts a partial download as complete. The fix would be a trailer, which this wire cannot have
while the body is unframed, or a status the host substitutes, which it must not do because the status is
the origin's.

**6. The version answer is written, then the write half closes, with no way to say the same thing
later.** The answer is the only frame this wire can write to a peer whose grammar it does not share. If
a future break also changes the response frame's own shape, there is no second channel to announce that
with. This is the cost of having exactly one frozen frame, and it is accepted rather than fixed.

## Test vectors

Every octet below is produced by the codec in `src/http.rs` and checked against this page by
`cargo test`; see [Regenerating the vectors](#regenerating-the-vectors). Three of the vectors are peer
**heads** rather than frames this implementation writes: they are the input, published beside the answer
each provokes, and two of the three provoke none.

The refusal vectors are generated from the host's own refusal **causes**, not from typed-out sentences,
so a reworded cause moves the vector and this page with it.

### A ranged GET

`GET https://example.com/big.iso` with one `Range` header, which is the case this whole service exists
for: the origin answers `206` and a resumable download works.

```text
vector tbh1-request-get
54 42 48 31 00 03 47 45 54 00 1b 68 74 74 70 73
3a 2f 2f 65 78 61 6d 70 6c 65 2e 63 6f 6d 2f 62
69 67 2e 69 73 6f 00 01 00 05 52 61 6e 67 65 00
0c 62 79 74 65 73 3d 30 2d 31 30 32 33
```

| octets | field | value |
| ------ | ----- | ----- |
| `54 42 48` | identity | `TBH` |
| `31` | version | `1` |
| `00 03` `47 45 54` | method | `GET` |
| `00 1b` | url length | 27 |
| `68 74 ... 73 6f` | url | `https://example.com/big.iso` |
| `00 01` | header count | 1 |
| `00 05` `52 61 6e 67 65` | header name | `Range` |
| `00 0c` `62 79 ... 32 33` | header value | `bytes=0-1023` |

### A HEAD with no headers

The minimal request frame: the header count is present and zero, never absent.

```text
vector tbh1-request-head
54 42 48 31 00 04 48 45 41 44 00 1b 68 74 74 70
73 3a 2f 2f 65 78 61 6d 70 6c 65 2e 63 6f 6d 2f
62 69 67 2e 69 73 6f 00 00
```

| octets | field | value |
| ------ | ----- | ----- |
| `54 42 48 31` | magic | `TBH1` |
| `00 04` `48 45 41 44` | method | `HEAD` |
| `00 1b` `68 74 ... 73 6f` | url | `https://example.com/big.iso` |
| `00 00` | header count | 0 |

### The origin answered

A `206` with the two headers a ranged GET comes back with. The body follows the last header immediately
and runs to the end of the stream.

```text
vector tbh1-response-ok
54 42 48 31 00 00 ce 00 02 00 0d 63 6f 6e 74 65
6e 74 2d 72 61 6e 67 65 00 11 62 79 74 65 73 20
30 2d 31 30 32 33 2f 34 30 39 36 00 0d 61 63 63
65 70 74 2d 72 61 6e 67 65 73 00 05 62 79 74 65
73
```

| octets | field | value |
| ------ | ----- | ----- |
| `54 42 48 31` | response tag | the frozen literal |
| `00` | tag | ok |
| `00 ce` | status | 206 |
| `00 02` | header count | 2 |
| `00 0d` `63 6f ... 67 65` | header name | `content-range` |
| `00 11` `62 79 ... 39 36` | header value | `bytes 0-1023/4096` |
| `00 0d` `61 63 ... 65 73` | header name | `accept-ranges` |
| `00 05` `62 79 74 65 73` | header value | `bytes` |

### Refused, the method is not served

`POST` is not `GET` or `HEAD`. The message is host prose and is shown only so the framing is complete.

```text
vector tbh1-response-error-method
54 42 48 31 01 00 30 6d 65 74 68 6f 64 20 50 4f
53 54 20 6e 6f 74 20 61 6c 6c 6f 77 65 64 20 28
66 65 74 63 68 20 69 73 20 47 45 54 2f 48 45 41
44 20 6f 6e 6c 79 29
```

| octets | field | value |
| ------ | ----- | ----- |
| `54 42 48 31` | response tag | the frozen literal |
| `01` | tag | error |
| `00 30` | message length | 48 |
| `6d 65 ... 79 29` | message | `method POST not allowed (fetch is GET/HEAD only)` |

### Refused, the target is not public

The SSRF guard. The host named resolves to the cloud metadata address, so the whole request is refused
before any connection. Every other refusal cause on this wire uses this identical frame shape with
different prose.

```text
vector tbh1-response-error-non-public
54 42 48 31 01 00 59 72 65 66 75 73 69 6e 67 20
74 6f 20 66 65 74 63 68 20 6d 65 74 61 64 61 74
61 2e 65 78 61 6d 70 6c 65 3a 20 69 74 20 72 65
73 6f 6c 76 65 73 20 74 6f 20 74 68 65 20 6e 6f
6e 2d 70 75 62 6c 69 63 20 61 64 64 72 65 73 73
20 31 36 39 2e 32 35 34 2e 31 36 39 2e 32 35 34
```

| octets | field | value |
| ------ | ----- | ----- |
| `01` | tag | error |
| `00 59` | message length | 89 |
| `72 65 ... 35 34` | message | `refusing to fetch metadata.example: it resolves to the non-public address 169.254.169.254` |

### A version mismatch, and the answer to it

The head a peer on `TBH2` writes, carrying the same ranged-GET body:

```text
vector tbh1-head-version-mismatch
54 42 48 32 00 03 47 45 54 00 1b 68 74 74 70 73
3a 2f 2f 65 78 61 6d 70 6c 65 2e 63 6f 6d 2f 62
69 67 2e 69 73 6f 00 01 00 05 52 61 6e 67 65 00
0c 62 79 74 65 73 3d 30 2d 31 30 32 33
```

The answer a `TBH1` host writes to it, and the only answer a version mismatch ever gets. Both versions
are named, because either one alone leaves the reader guessing at the other:

```text
vector tbh1-response-version-mismatch
54 42 48 31 01 00 6a 66 65 74 63 68 20 77 69 72
65 20 76 65 72 73 69 6f 6e 20 6d 69 73 6d 61 74
63 68 3a 20 74 68 65 20 72 65 71 75 65 73 74 20
69 73 20 54 42 48 32 2c 20 74 68 69 73 20 68 6f
73 74 20 73 70 65 61 6b 73 20 54 42 48 31 3b 20
72 75 6e 20 74 68 65 20 73 61 6d 65 20 72 65 6c
65 61 73 65 20 61 74 20 62 6f 74 68 20 65 6e 64
73
```

| octets | field | value |
| ------ | ----- | ----- |
| `54 42 48 31` | response tag | the frozen literal, **not** the peer's `TBH2` and not a future host's own magic |
| `01` | tag | error |
| `00 6a` | message length | 106 |
| `66 65 ... 64 73` | message | `fetch wire version mismatch: the request is TBH2, this host speaks TBH1; run the same release at both ends` |

Read the first four octets of that answer against the first four of the head above it. They differ, and
they are meant to: the head names the peer's request grammar and the answer names the frozen response
tag. An implementation that derived one from the other would write `TBH2` there and the peer would
reject its own answer.

The peer's version octet is arbitrary and need not be printable. A host `MUST` escape it before putting
it in the message, and `MUST` keep the result inside the 65 535 octet field, since escaping expands each
octet.

### A foreign head

A head from something that is not this protocol at all:

```text
vector tbh1-head-foreign
53 53 48 2d 00 03 47 45 54 00 1b 68 74 74 70 73
3a 2f 2f 65 78 61 6d 70 6c 65 2e 63 6f 6d 2f 62
69 67 2e 69 73 6f 00 01 00 05 52 61 6e 67 65 00
0c 62 79 74 65 73 3d 30 2d 31 30 32 33
```

The correct answer is **zero octets**. There is no vector for the reply because there is no reply.

### The neighbouring identity

The head a peer on the tunnel wire this one rides inside writes. Its identity is `TB`, not `TBH`, so it
is foreign here however closely it starts like this one. The correct answer is **zero octets**, exactly
as for the foreign head above, and there is no vector for the reply because there is no reply:

```text
vector tbh1-head-neighbouring-identity
54 42 30 34 00 03 47 45 54 00 1b 68 74 74 70 73
3a 2f 2f 65 78 61 6d 70 6c 65 2e 63 6f 6d 2f 62
69 67 2e 69 73 6f 00 01 00 05 52 61 6e 67 65 00
0c 62 79 74 65 73 3d 30 2d 31 30 32 33
```

The collision runs the other way too, and that direction is the dangerous one: a reader of the `TB`
identity that compared two octets instead of the maximal run would read the `tbh1-request-get` vector
at the top of this page as its own wire on version `H1`, and answer it. That is the concrete reason the
identity is the maximal leading capital run and not a fixed-width prefix.

## Regenerating the vectors

The vectors are generated, never typed. `src/http_tests.rs` builds each frame with this crate's own
writer, proves the reader takes the same octets back to the same value, parses this file, and fails when
the two disagree in either direction: a vector this page publishes that the codec no longer writes is
the same defect as one the codec writes that this page has not caught up with.

Run:

```text
cargo test -p fetch --lib http_tests::vectors -- --nocapture
```

It prints every vector in exactly the block form used above, sixteen octets to a line, so a regenerated
block pastes over the old one unedited. The test reads this file for lines of the form `vector <name>`
and takes the octet lines under each, up to the next blank line or fence, as that vector's frame. Keep
that shape when editing, and keep the field-by-field tables beside the blocks in step by hand: the
octets are checked mechanically, the annotations are not.
