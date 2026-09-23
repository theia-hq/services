# DG02, the probe wire

The frame every measure stream opens with: the client names a method and its bound, the responder
either performs it or refuses it in a typed frame, and the counted payload that some methods carry
flows around those frames as raw bytes this wire gives no meaning to.

This document is the wire, not the implementation of it. A second implementation built from this page
alone must interoperate with the one in `src/protocol.rs`, octet for octet. Where a statement here is
weaker than the code (a `MAY` the code happens to satisfy), the weaker statement is the contract. Where
the code is weaker than a `MUST` here, it is named in [Known gaps](#known-gaps) rather than left for a
reader to discover.

- [Conventions](#conventions)
- [Where this wire sits](#where-this-wire-sits)
- [The magic: identity and version](#the-magic-identity-and-version)
- [The request frame](#the-request-frame)
- [The response frame](#the-response-frame)
- [The payload](#the-payload)
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
the previous one ends. The one text field is UTF-8 and is never NUL-terminated; its length is carried in
front of it.

The **client** is the side that opens a stream and names a method. The **responder** is the side that
accepts the stream and performs or refuses it. A node is usually both, to different peers.

Octets are shown as lowercase hexadecimal pairs. Grouping and line breaks inside a vector block are
presentation only.

## Where this wire sits

DG02 is carried on one bidirectional stream of a session between two node identities (ed25519 public
keys), and that stream has **already been admitted by the host's gate** before the first octet of this
wire moves. It assumes an ordered, reliable, framed-by-nothing byte stream in each direction, and an
attribution of the peer's identity. It specifies no session establishment and adds no authentication of
its own.

**Admitted is not the same as authorized, and a reimplementation must not confuse them.** The gate that
admitted the stream is per service, and an operator `MAY` open either service to callers holding no
credential at all. On such a route the peer is an anonymous stranger whose node identity is attributed
and nothing more, and the responder-side [caps](#caps) are the only thing bounding it. A responder
`MUST NOT` treat arrival on this wire as evidence that the peer was vouched for.

Four rules bind this wire to the layer under it:

1. **The client opens every stream.** A responder `MUST NOT` open a stream toward a client and call it
   measure: there is one door, and a request frame only ever travels client to responder.
2. **The service that admitted the stream does not constrain the method, so this wire does.** `ping` and
   `speed` are two independent services with two independent gates. The gate admits a stream, not a
   method, so a responder `MUST` check that the method the request frame names is the one the admitting
   service serves, and `MUST` refuse it with [wrong method](#refusal-semantics) otherwise. Without that
   check a grant for `ping` opens a `speed` drain, because both speak this same frame.
3. **A ping stream carries a RUN; a speed stream carries one exchange.** A ping stream carries many
   complete request frames, each with its own magic, until the client closes its write half. A speed
   stream carries exactly one request frame, and every octet after the frames belongs to the payload.
4. **This wire defines no trailer, no keepalive and no close frame.** A stream ends when the session's
   stream ends.

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
| `DG02` (`44 47 30 32`) | `DG` | `02` |

The identity is **frozen forever**. A stream that does not open with `DG` is not a measure stream, and
that is the only thing an identity mismatch is ever allowed to mean. The version names the request
grammar, and only the request grammar: see [Versioning](#versioning-and-the-frozen-response).

A receiver `MUST` split the four octets by the rule above and compare the **identity** first, then the
version. Comparing all four at once collapses two different facts into one and makes the answer in the
next section impossible to write. Comparing a fixed-width prefix instead of the maximal run is a
different parse with an observable consequence: a receiver that stops after two octets reads a head
whose identity is `DGX` as this wire on version `X1` and answers it, handing this responder's version to
something that does not speak this wire. A receiver `MUST` therefore read a capital octet immediately
after `DG` as the same run continuing into a longer identity, which is foreign.

**No identity in this family may be a prefix of another.** `DG` is not a prefix of any existing identity
and none is a prefix of it, and a new wire `MUST NOT` be given an identity that opens with `DG`. The
rule exists because a prefix collision makes the two facts above ambiguous for a receiver that does not
apply the maximal-run rule; where a collision already exists, the run-end check is the only thing
holding it safe.

The three conditions a responder can meet on the head of a stream are distinct, and each has exactly
one correct answer:

| condition | what it means | the responder's answer |
| --------- | ------------- | ---------------------- |
| identity is not `DG` | not a measure stream | **no octets at all**; close the stream |
| identity is `DG`, version is not served | a measure peer on another grammar | an `unsupported` frame naming **both** versions |
| the head cannot be read (short, closed, timed out) | nothing was received | no octets; close the stream |

A foreign identity `MUST` receive **zero** octets in reply. Nothing true can be said to a protocol we
cannot name, and a reply would be a guess at what is meaningful to it.

A served identity on an unserved version `MUST` be answered, because the peer has already proved it
speaks measure, and both ends can act on the fact. This is the single reason the response frame is
frozen: a responder that cannot parse a peer's request must still be able to write something that peer
can parse.

## The request frame

Written by the client. One frame opens a speed stream; a ping stream carries one frame per probe, each
complete with its own magic.

| offset | field | width | encoding |
| ------ | ----- | ----- | -------- |
| 0 | identity | 2 | ASCII `DG` (`44 47`) |
| 2 | version | 2 | ASCII `02` (`30 32`) |
| 4 | method tag | 1 | `00` ping, `01` speed sink, `02` speed source, `03` speed bidir |
| 5 | *the tag's own fields* | 8 or 12 | below |

The frame is **fixed width per tag**, with no length prefix anywhere. A ping frame is 17 octets and
every speed frame is 13.

### `00` ping

| offset | field | width | encoding |
| ------ | ----- | ----- | -------- |
| 5 | seq | 4 | u32 big-endian |
| 9 | sent unix nanos | 8 | u64 big-endian |

`seq` is the client's own probe index. `sent unix nanos` is an **opaque nonce**: the responder returns
it untouched and `MUST NOT` read it as a clock, and the client `MUST` time the round trip with its own
monotonic clock. Two machines' wall clocks are not comparable, so a stamp-derived round trip is not a
measurement. Together the two fields are what lets a client reject a reply that belongs to an earlier
probe.

### `01` speed sink (the client uploads)

| offset | field | width | encoding |
| ------ | ----- | ----- | -------- |
| 5 | limit bytes | 8 | u64 big-endian |

How many payload octets the client will send after this frame, for the responder to drain and count.

The value `ffffffffffffffff` is the [unbounded sentinel](#the-unbounded-sentinel) and means **"no exact
count"**: it is a time-bounded client naming its own ceiling, not an ask. A responder `MUST` read it as
such and `MUST NOT` refuse it as an over-cap request, or every time-bounded upload against a metered
responder is refused.

### `02` speed source (the responder downloads to the client)

| offset | field | width | encoding |
| ------ | ----- | ----- | -------- |
| 5 | limit bytes | 8 | u64 big-endian |

How many payload octets the responder should send. `ffffffffffffffff` means **stream until the client
stops reading**, where the client's own deadline is the sole terminator.

### `03` speed bidir (both directions at once)

| offset | field | width | encoding |
| ------ | ----- | ----- | -------- |
| 5 | limit bytes | 8 | u64 big-endian |

How many payload octets to move in **each** direction, with the same sentinel meaning as `02`. Both
halves run on the one stream at once, which is what lets a single-stream transport measure upload and
download over one window.

### The unbounded sentinel

`ffffffffffffffff` (`u64::MAX`) is a sentinel rather than a flag octet, so the field stays a fixed-width
`u64` and the frame stays fixed width. `u64::MAX` octets is unreachable in any real transfer, so it
cannot collide with a genuine count. Its meaning is per tag and is given above: on `02` and `03` it is
"until the peer stops", and on `01` it is "the client named no exact count".

## The response frame

Written by the responder. **This frame is frozen**; see
[Versioning](#versioning-and-the-frozen-response).

It carries **no magic and no version of its own**. A response is only ever read on a stream the client
opened and has already written a request on, so there is nothing for a magic to disambiguate, and
omitting it is what lets a build that predates a future request grammar still read the answer.

| offset | field | width | encoding |
| ------ | ----- | ----- | -------- |
| 0 | tag | 1 | `00` pong, `01` received, `02` sourcing, `03` unsupported |
| 1 | *the tag's own fields* | 0 to 1029 | below |

| tag | name | fields | total width |
| --- | ---- | ------ | ----------- |
| `00` | pong | seq (u32), sent unix nanos (u64), both echoed verbatim | 13 |
| `01` | received | bytes (u64): what the responder drained and counted | 9 |
| `02` | sourcing | none | 1 |
| `03` | unsupported | refusal code (1), detail length (u32), detail (UTF-8) | 6 to 1030 |

The response tag namespace is **independent of the request tag namespace**. Tag `00` is a pong on the
way back and a ping on the way out; a reader `MUST` decode a frame by the direction it arrived from and
never by matching the two tables against each other.

### `00` pong

| offset | field | width | encoding |
| ------ | ----- | ----- | -------- |
| 1 | seq | 4 | u32 big-endian, echoed from the request |
| 5 | sent unix nanos | 8 | u64 big-endian, echoed from the request |

Both fields `MUST` be returned verbatim. A client `MUST` reject a pong whose `seq` or nonce does not
match the probe it is waiting on: a reply that arrives after its own probe was written off would
otherwise be credited to the next probe as an impossibly fast round trip.

### `01` received

| offset | field | width | encoding |
| ------ | ----- | ----- | -------- |
| 1 | bytes | 8 | u64 big-endian |

How many payload octets the responder actually drained before the client's EOF, its own byte cap, or
its own deadline ended the drain. It is what the responder counted, never what the client asked for.

### `02` sourcing

One octet, and the whole frame. It is the **go-ahead** that precedes a download payload, and its only
job is to be a frame where a refusal could also have been: a client reading it knows the next octet is
payload, and a client reading `03` instead knows the run was refused. Without it a wrong-method refusal
would be drained as payload and reported as a measurement of zero.

### `03` unsupported

| offset | field | width | encoding |
| ------ | ----- | ----- | -------- |
| 1 | refusal code | 1 | `00` wrong method, `01` rate limited, `02` busy |
| 2 | detail length | 4 | u32 big-endian, at most 1024 |
| 6 | detail | *detail length* | UTF-8, no terminator |

The detail is human-readable prose from the responder. A client `MAY` show it to a person and `MUST NOT`
parse it, match on it, or branch on its content: the code is the machine-readable part, and the detail's
wording is not part of this wire. A reader `MUST` reject a claimed detail length above 1024 **before**
allocating for it, and `MUST` reject a detail that is not valid UTF-8 rather than repairing it lossily.

The length prefix is a `u32` here. Other wires in this family prefix the same bounded detail with a
`u16`, so an implementation sharing one detail codec across wires `MUST NOT` assume a single prefix
width.

## The payload

Some methods carry a counted byte stream around the frames. The payload is **raw octets with no framing
whatsoever**: no length prefix, no chunk header, no delimiter. Its content is meaningless (throughput is
the only signal), so an implementation `MAY` send any octets it likes and a receiver `MUST NOT` inspect
them.

| method | who writes the payload | what ends it |
| ------ | ---------------------- | ------------ |
| `01` sink | the client, after its request frame | the client closing its write half (EOF), or the responder's cap |
| `02` source | the responder, after its `sourcing` frame | the byte count, the responder's cap, or the client closing its read half |
| `03` bidir | both, at once, after the `sourcing` frame | each direction as above |

**Once payload begins in a direction, no further frame may be written in that direction.** There is no
boundary a reader could find, so a refusal appended after payload would be read as payload. This is the
single most load-bearing consequence of the payload being unframed, and two rules follow from it:

- A responder `MUST` refuse an unserviceable ask **before** the `sourcing` frame (source, bidir) or
  before draining (sink). A run that starts can only end as a truncated close.
- A responder that hits a cap mid-payload `MUST` close the stream rather than explain. The client sees a
  short stream, and the client is the side that must name that as an error rather than a smaller
  throughput.

A sink is the one method whose frame comes **after** its payload: the client sends, closes its write
half, and the responder answers `received`. A client `MUST` keep its read half live while it sends, so a
refusal written before any octet is drained is read rather than deadlocked behind a responder that has
stopped reading.

## The state machine

Both sides are strictly sequential within a method; there is no pipelining. Bidir is the one place two
transfers are in flight at once, and they are two directions of the same single exchange.

**Responder, on a stream admitted for `ping`.**

| state | event | action | next |
| ----- | ----- | ------ | ---- |
| `Admitted` | this caller opened a run too recently | write `unsupported` / rate limited | `Closed` |
| `Admitted` | the run deadline elapses before any frame arrives | close, **no octets** | `Closed` |
| `Admitted` | identity is not `DG` | close, **no octets** | `Closed` |
| `Admitted` | identity `DG`, version not served | write `unsupported` / wrong method naming both versions | `Closed` |
| `Admitted` | the frame is unreadable (unknown tag, short) | close, **no octets** | `Closed` |
| `Admitted` | a speed frame | write `unsupported` / wrong method | `Closed` |
| `Admitted` | a ping frame | charge the frame against the byte ceiling | `Echoing` |
| `Echoing` | the run deadline has passed | write `unsupported` / rate limited, best effort | `Closed` |
| `Echoing` | one more echo would cross the byte ceiling | write `unsupported` / rate limited, best effort | `Closed` |
| `Echoing` | otherwise | write `pong`, then read the next frame | `Echoing` |
| `Echoing` | clean EOF on the read half | close | `Closed` |
| `Echoing` | a speed frame mid-run | close, no octets | `Closed` |

The rate-limit refusal is written **before the request frame is read**, so a client `MUST` be prepared
to read a response frame at any point after opening the stream, including before its own frame has been
consumed.

Both cap endings are **best effort**: the write is itself bounded by the run deadline, because a peer
that has stopped reading has no room for the frame. The cap owns the end of the stream, not the write.

**Responder, on a stream admitted for `speed`.**

| state | event | action | next |
| ----- | ----- | ------ | ---- |
| `Admitted` | no transfer slot free | write `unsupported` / busy | `Closed` |
| `Admitted` | the stream deadline elapses before any frame arrives | close, **no octets** | `Closed` |
| `Admitted` | identity is not `DG` | close, **no octets** | `Closed` |
| `Admitted` | identity `DG`, version not served | write `unsupported` / wrong method naming both versions | `Closed` |
| `Admitted` | the frame is unreadable (unknown tag, short) | close, **no octets** | `Closed` |
| `Admitted` | a ping frame | write `unsupported` / wrong method | `Closed` |
| `Admitted` | sink, an exact count over the byte cap | write `unsupported` / rate limited | `Closed` |
| `Admitted` | sink, otherwise | drain to the clamped count, then write `received` | `Closed` |
| `Admitted` | source or bidir, an exact count over the byte cap | write `unsupported` / rate limited | `Closed` |
| `Admitted` | source, otherwise | write `sourcing`, then send the clamped payload | `Closed` |
| `Admitted` | bidir, otherwise | write `sourcing`, then send and drain at once | `Closed` |

Three orderings in that table are normative rather than incidental:

- The busy refusal `MUST` be written rather than the stream queued. Queueing parks one caller's stream
  behind another's for the whole of a metered transfer, and the client has a bound it would hit first.
- An over-cap **exact** ask `MUST` be refused before any payload. An ask that is the unbounded sentinel
  is not an exact ask and `MUST NOT` be refused for being over a cap; it is clamped instead.
- A responder `MUST` write `sourcing` only after it has decided to serve the run. Announcing the
  go-ahead and then refusing is unsayable on this wire, because the refusal would be payload.

**Client.**

| state | event | action | next |
| ----- | ----- | ------ | ---- |
| `Opened` | ping | write a ping frame | `AwaitingPong` |
| `AwaitingPong` | `pong` whose seq and nonce match | record the round trip | `AwaitingPong` (next probe) |
| `AwaitingPong` | `pong` that does not match | fail the run as mismatched | `Closed` |
| `AwaitingPong` | `unsupported` | surface the typed refusal; **no report** | `Closed` |
| `AwaitingPong` | nothing within the probe bound | count this probe lost, continue the run | `AwaitingPong` (next probe) |
| `Opened` | sink | write the request, then send payload while reading | `Sending` |
| `Sending` | `unsupported` arrives first | surface the typed refusal; **no report** | `Closed` |
| `Sending` | payload done | close the write half, await `received` | `AwaitingCount` |
| `AwaitingCount` | `received` below an exact ask | fail as ended early | `Closed` |
| `AwaitingCount` | `received` otherwise | report | `Closed` |
| `AwaitingCount` | nothing within the stall bound | fail as ended early | `Closed` |
| `Opened` | source or bidir | write the request, await the go-ahead | `AwaitingGoAhead` |
| `AwaitingGoAhead` | `sourcing` | begin the payload | `Transferring` |
| `AwaitingGoAhead` | `unsupported` | surface the typed refusal; **no report** | `Closed` |
| `AwaitingGoAhead` | any other frame | fail as mismatched | `Closed` |
| `Transferring` | an exact ask fell short | fail as ended early | `Closed` |
| `Transferring` | otherwise | report | `Closed` |

Two client rules are normative, and they are the reason this wire has typed refusals at all:

- **A refusal is not a measurement.** A client `MUST NOT` render an `unsupported` frame, a refusal code
  it does not know, or a byte-bounded run that fell short as `0`, `100% loss`, or `0.00 MiB/s`. Each
  `MUST` surface as a distinct error, because a plausible-looking zero is indistinguishable from a real
  measurement of a very bad link, and that is the one failure a diagnostic must never produce.
- **A probe with no reply is loss; a refusal is not.** An unanswered probe `MUST` be counted lost and the
  run `MUST` continue, because the next probe is a fresh question. A refusal `MUST` short-circuit the
  whole run.

## Unrecognised values

The table a second implementation is judged by. Every value this wire can carry that a receiver might
not know, and what the receiver does with it:

| where | unrecognised value | receiver | required behaviour |
| ----- | ------------------ | -------- | ------------------ |
| request identity | anything but `DG` | responder | close with **no octets written** |
| request version | any served-identity version the responder does not speak | responder | write `unsupported` / wrong method, detail naming the peer's version and its own |
| request method tag | anything but `00`, `01`, `02`, `03` | responder | close with no octets; the frame's remaining width is unknowable |
| request frame | short, or the stream closed mid-frame | responder | close with no octets |
| ping seq or nonce | any value | responder | echo verbatim; never interpret |
| limit bytes | `ffffffffffffffff` on `01` | responder | read as "no exact count"; clamp, `MUST NOT` refuse as over cap |
| limit bytes | `ffffffffffffffff` on `02` or `03` | responder | read as "until the peer stops"; clamp to the cap when metered |
| limit bytes | a count above the responder's cap on `01`, `02`, `03` | responder | write `unsupported` / rate limited before any payload |
| request frame | a ping frame arriving mid-ping-run's payload position | responder | see [known gap 3](#known-gaps) |
| response tag | anything but `00`, `01`, `02`, `03` | client | fail the stream; `MUST NOT` be read as a measurement |
| refusal code | anything but `00`, `01`, `02` | client | fail the stream, reporting a refusal class this build cannot name; `MUST NOT` be mapped onto a known class, and `MUST NOT` fold into loss |
| detail length | greater than 1024 | client | reject **before** allocating the buffer |
| detail | not valid UTF-8 | client | reject; never repair lossily |
| pong seq or nonce | not the ones this probe sent | client | fail as mismatched; `MUST NOT` credit the round trip |
| payload octets | any content | either | never inspected |

The rule behind the two refusal rows is one rule: a value a build cannot name is never quietly promoted
to one it can. Reusing *wrong method* for an unknown code tells a client the responder ruled something
it did not; folding it into loss turns a refusal into a measurement, which is the class of error this
whole wire's typed refusals exist to prevent.

## Refusal semantics

A refusal class names the set of **client responses**, not the set of responder causes. There are three
things a client can do about a refusal, so there are three codes.

| code | class | what the client does |
| ---- | ----- | -------------------- |
| `00` | wrong method | the same dial will be refused again; this service does not serve this method. Ask the peer for the other service. No retry. |
| `01` | rate limited | a responder-side bound stopped this run: a per-caller interval, a stream's byte ceiling or lifetime, or an ask larger than the cap allows. Retry later, or ask for less. |
| `02` | busy | a transfer slot is taken by another caller. Retry later; nothing about this caller was ruled on. |

Every one of these is a **Layer 2** refusal: the gate already admitted the stream, and the responder then
declined the method. A refusal the gate itself produced never reaches this wire, because no stream was
admitted to carry it. A client `MUST` keep the two apart when it reports, because they have different
owners: a gate refusal is the operator's policy and a Layer 2 refusal is the responder's own state.

**Wrong method carries the version answer as well as its own.** That is deliberate and it is a real
constraint on anyone extending this wire; see [Versioning](#versioning-and-the-frozen-response).

**Rate limited is deliberately coarse.** A conformant responder writes it for all of:

- the caller opened another run inside the per-caller interval,
- a ping stream reached its byte ceiling,
- a ping stream reached its lifetime cap,
- a speed request named more bytes than the per-direction cap allows.

The detail distinguishes them for a person; the code does not, and a client `MUST NOT` branch on which
one it was. All four mean the same thing to a caller: this run stopped on a responder-side bound, and a
smaller or later run may succeed.

**No refusal on this wire is an authorization outcome.** A client `MUST NOT` record any of the three as
a ruling about its credentials. The gate already ruled, and it ruled admit.

## Caps

Every bound this wire places on a receiver, with its unit. A cap marked `MUST` is part of the wire: an
implementation that exceeds it writes frames a conformant peer rejects, or accepts frames a conformant
peer never writes. A cap marked *responder policy* or *client policy* is a resource decision each side
makes for itself; the value shipped here is given so a second implementation has a sane starting point,
and the `MUST` is only that **some** finite bound exists where the table says so.

| bound | value | unit | strength |
| ----- | ----- | ---- | -------- |
| ping request frame | 17 | octets | `MUST` (fixed: `2 + 2 + 1 + 4 + 8`) |
| speed request frame | 13 | octets | `MUST` (fixed: `2 + 2 + 1 + 8`) |
| pong frame | 13 | octets | `MUST` (fixed: `1 + 4 + 8`) |
| received frame | 9 | octets | `MUST` (fixed: `1 + 8`) |
| sourcing frame | 1 | octet | `MUST` |
| refusal detail | 1024 | octets | `MUST`, enforced by both writer and reader |
| unsupported frame, total | 1030 | octets | `MUST` (derived: `1 + 1 + 4 + 1024`) |
| ping run interval, per caller | 1 | second | responder policy |
| ping stream byte ceiling | 1 073 741 824 | octets | responder policy; the frames, not a payload |
| ping stream lifetime | 60 | seconds | responder policy; a finite bound is a `MUST` |
| rate-limiter caller table | 8192 | callers | responder policy |
| concurrent speed transfers | 1 | transfers | responder policy |
| speed bytes, per direction | 67 108 864 | octets | responder policy |
| speed stream lifetime | 15 | seconds | responder policy; a finite bound is a `MUST` |
| payload chunk | 65 536 | octets | neither; an implementation detail of the shipped sender |
| probe reply bound | 10 | seconds | client policy; a finite bound is a `MUST` |
| payload stall bound | 20 | seconds | client policy; a finite bound is a `MUST` |

The responder-policy values above are the **metered profile**, the one an operator may open to callers
holding no credential. There is a second shipped profile, the **owner profile**, in which every
responder-policy row is absent: no interval, no ceilings, no lifetimes, no slot bound. It exists for
routes behind a gate that admits only the operator's own devices, where the gate is the terminator. An
implementation `MUST NOT` serve the owner profile on a route open to strangers: an unbounded ping stream
and an unbounded speed drain are each a way to hold a node's uplink indefinitely.

Three of these bounds have a wire consequence rather than only a local one:

- **The two stream lifetimes bound the opening read as well as the run.** A peer that opens an admitted
  stream and never writes would otherwise park a responder task and its buffer indefinitely. The clock
  starts when the stream is admitted, not when the first octet arrives.
- **The client's probe bound is not derived from the responder's lifetime.** Nothing on the serving side
  legitimately delays an echo: the responder writes the reply as it reads the request. Waiting a run's
  whole lifetime for a reply due in milliseconds answers a hang with a slower hang.
- **The client's stall bound is derived from the responder's speed lifetime**, and `MUST` exceed it. A
  metered responder that stops at its own cap and a peer that has crashed look identical from the other
  end of the stream, so a client that waited less than the responder's lifetime would report a healthy
  capped run as a dead peer.

## Versioning and the frozen response

There is **no negotiation** on this wire. A client speaks exactly one version, a responder serves
exactly one version, and there is no field in which to offer a list. A client `MUST NOT` attempt to
downgrade by retrying with another version's magic; the answer to a version mismatch is to run the same
release at both ends.

**The request frame may break with a version bump.** It carries the evolving method vocabulary. A
responder `MUST NOT` skip a request field it does not understand: the frame is fixed width per tag with
no length prefix, so a field it cannot read is a frame it cannot find the end of.

**The response frame is frozen.** A future version of the request grammar:

- `MUST NOT` change the meaning or width of any response tag,
- `MUST NOT` change the shape or meaning of an existing refusal code,
- `MUST NOT` give the response frame a magic or a version of its own,
- `MAY` add a new response tag,
- `MAY` add a new refusal code.

The freeze is what makes a version mismatch answerable at all: a responder cannot answer a peer whose
request it cannot parse unless part of the wire is stable across every version.

**The version answer rides refusal code `00`, and an extension `MUST NOT` move it.** This is the part a
reimplementer is most likely to get wrong, because a new "wire version mismatch" code looks obviously
tidier. It would not work. The peer that answer is written for is by definition running another build,
and that build's reader decodes the refusal code **before** it reads the detail: a code it has no tag
for is rejected outright, and the one sentence the frame exists to carry is lost with it. So the answer
has to ride a code that every shipped build already decodes, and `00` is also the honest reading of the
condition, since a method named in a grammar this build does not speak is not a method it serves.

The same reasoning bounds every future addition: **an added tag or code is legible only to a peer that
already knows it.** Additions `MAY` therefore serve new conditions, and `MUST NOT` carry anything an
older peer needs to understand.

The freeze promises "a break names itself and announces itself", never "no break".

## Known gaps

Stated here rather than discovered later. Each is a real limit of the wire as it stands, with the fix it
is waiting for.

**1. An unknown refusal code cannot be read, even though the frame's shape would allow it.** Every
refusal code carries the same length-prefixed detail, so a peer meeting an unknown code could skip the
detail and know exactly where the frame ends. The shipped reader does not: it decodes the code first and
fails the stream, discarding a detail it could have found. That makes "a new code is additive" true only
between ends that both know the code, which is what forces the version answer onto code `00` forever.
The named fix is to read the detail before rejecting the code, so an unknown code surfaces as a class
this build cannot name **with the responder's own sentence attached**. It needs no version bump.

**2. An unreadable frame gets silence, where an unreadable version gets an answer.** A head that names
`DG02` has proved the peer speaks this grammar, so an unknown method tag or a truncated frame could be
answered with a refusal the peer would understand. It is not: any failure past the version check closes
the stream with no octets. The same holds for a speed frame arriving mid-ping-run, which closes the
stream rather than refusing. A client therefore `MUST` treat a closed stream with no response as "the
responder could not read my frame", which is the least actionable outcome on the wire. The fix is to
answer wrong method for these too, which needs no version bump because it adds no code.

**3. A truncated payload is indistinguishable from a complete one.** The payload is unframed and
EOF-delimited, so a responder stopped by its byte cap or its lifetime closes the stream exactly as a
responder that finished does. Only the client's own count of what it asked for tells the two apart,
which is why a byte-bounded run that falls short `MUST` be an error rather than a smaller throughput,
and why a time-bounded run reports whatever arrived. A sink is the exception: its `received` frame
carries the responder's own count, which is why that count `MUST` be reported rather than the client's.
The fix would be a trailer, which this wire cannot have while the payload is unframed.

## Test vectors

Every octet below is produced by the codec in `src/protocol.rs` and checked against this page by
`cargo test`; see [Regenerating the vectors](#regenerating-the-vectors). Three of the vectors are peer
**heads** rather than frames this implementation writes: they are the input, published beside the answer
each provokes.

Every ping-shaped vector carries seq `7` and the nonce `1726000000000000000`. The nonce is opaque to
this wire, so the vector fixes the field's width and position, never the stamp's own meaning.

### A ping request

```text
vector dg02-request-ping
44 47 30 32 00 00 00 00 07 17 f3 fb da f2 53 00
00
```

| octets | field | value |
| ------ | ----- | ----- |
| `44 47` | identity | `DG` |
| `30 32` | version | `02` |
| `00` | method tag | ping |
| `00 00 00 07` | seq | 7 |
| `17 f3 fb da f2 53 00 00` | sent unix nanos | 1726000000000000000 |

### A speed sink request, an exact count

The client will send 8 MiB for the responder to drain and count.

```text
vector dg02-request-speed-sink
44 47 30 32 01 00 00 00 00 00 80 00 00
```

| octets | field | value |
| ------ | ----- | ----- |
| `44 47 30 32` | magic | `DG02` |
| `01` | method tag | speed sink |
| `00 00 00 00 00 80 00 00` | limit bytes | 8388608 |

### A speed sink request naming no exact count

A time-bounded upload. The sentinel is the client's ceiling, not an ask, and a responder `MUST NOT`
refuse it for exceeding a cap.

```text
vector dg02-request-speed-sink-no-exact-count
44 47 30 32 01 ff ff ff ff ff ff ff ff
```

### A speed source request, an exact count

```text
vector dg02-request-speed-source
44 47 30 32 02 00 00 00 00 00 40 00 00
```

| octets | field | value |
| ------ | ----- | ----- |
| `44 47 30 32` | magic | `DG02` |
| `02` | method tag | speed source |
| `00 00 00 00 00 40 00 00` | limit bytes | 4194304 |

### A speed source request, unbounded

The same frame with the sentinel. Here it means "stream until I stop reading".

```text
vector dg02-request-speed-source-unbounded
44 47 30 32 02 ff ff ff ff ff ff ff ff
```

### A speed bidir request

2 MiB in each direction, both at once on the one stream.

```text
vector dg02-request-speed-bidir
44 47 30 32 03 00 00 00 00 00 20 00 00
```

### The pong

The request's two fields, returned verbatim. Note that it carries no magic.

```text
vector dg02-response-pong
00 00 00 00 07 17 f3 fb da f2 53 00 00
```

| octets | field | value |
| ------ | ----- | ----- |
| `00` | tag | pong |
| `00 00 00 07` | seq | 7 |
| `17 f3 fb da f2 53 00 00` | sent unix nanos | 1726000000000000000 |

### The drained count

What the responder actually took, after the client closed its write half.

```text
vector dg02-response-received
01 00 00 00 00 00 80 00 00
```

| octets | field | value |
| ------ | ----- | ----- |
| `01` | tag | received |
| `00 00 00 00 00 80 00 00` | bytes | 8388608 |

### The download go-ahead

One octet. Payload begins immediately after it.

```text
vector dg02-response-sourcing
02
```

### Refused, wrong method

A ping frame arrived on the `speed` service, or a speed frame on `ping`. The detail is responder prose
and is shown only so the framing is complete.

```text
vector dg02-response-unsupported-wrong-method
03 00 00 00 00 20 74 68 69 73 20 6e 6f 64 65 20
73 65 72 76 65 73 20 70 69 6e 67 2c 20 6e 6f 74
20 73 70 65 65 64
```

| octets | field | value |
| ------ | ----- | ----- |
| `03` | tag | unsupported |
| `00` | refusal code | wrong method |
| `00 00 00 20` | detail length | 32 |
| `74 68 ... 65 64` | detail | `this node serves ping, not speed` |

### Refused, rate limited

The ask is larger than the per-direction byte cap, so it is refused before any payload rather than
truncated.

```text
vector dg02-response-unsupported-rate-limited
03 01 00 00 00 37 73 70 65 65 64 20 72 65 71 75
65 73 74 20 69 73 20 6f 76 65 72 20 74 68 65 20
62 79 74 65 20 63 61 70 2c 20 72 65 71 75 65 73
74 20 66 65 77 65 72 20 62 79 74 65 73
```

| octets | field | value |
| ------ | ----- | ----- |
| `03` | tag | unsupported |
| `01` | refusal code | rate limited |
| `00 00 00 37` | detail length | 55 |
| `73 70 ... 65 73` | detail | `speed request is over the byte cap, request fewer bytes` |

### Refused, busy

Another caller holds the transfer slot. Refused, never queued.

```text
vector dg02-response-unsupported-busy
03 02 00 00 00 1d 73 70 65 65 64 20 62 75 73 79
2c 20 74 72 79 20 61 67 61 69 6e 20 73 68 6f 72
74 6c 79
```

| octets | field | value |
| ------ | ----- | ----- |
| `03` | tag | unsupported |
| `02` | refusal code | busy |
| `00 00 00 1d` | detail length | 29 |
| `73 70 ... 6c 79` | detail | `speed busy, try again shortly` |

### A version mismatch, and the answer to it

The head a peer on `DG03` writes, carrying the same ping body:

```text
vector dg02-head-version-mismatch
44 47 30 33 00 00 00 00 07 17 f3 fb da f2 53 00
00
```

The answer a `DG02` responder writes to it, and the only answer a version mismatch ever gets. It rides
refusal code `00` because that is the code every shipped build already decodes, and both versions are
named because either one alone leaves the reader guessing at the other:

```text
vector dg02-response-version-mismatch
03 00 00 00 00 6c 6d 65 61 73 75 72 65 20 77 69
72 65 20 76 65 72 73 69 6f 6e 20 6d 69 73 6d 61
74 63 68 3a 20 74 68 65 20 72 65 71 75 65 73 74
20 69 73 20 44 47 30 33 2c 20 74 68 69 73 20 68
6f 73 74 20 73 70 65 61 6b 73 20 44 47 30 32 3b
20 72 75 6e 20 74 68 65 20 73 61 6d 65 20 72 65
6c 65 61 73 65 20 61 74 20 62 6f 74 68 20 65 6e
64 73
```

| octets | field | value |
| ------ | ----- | ----- |
| `03` | tag | unsupported |
| `00` | refusal code | wrong method |
| `00 00 00 6c` | detail length | 108 |
| `6d 65 ... 64 73` | detail | `measure wire version mismatch: the request is DG03, this host speaks DG02; run the same release at both ends` |

The peer's two version octets are arbitrary and need not be printable. A responder `MUST` escape them
before putting them in the detail, and `MUST` keep the result inside the 1024 octet detail cap, since
escaping expands each octet.

### A foreign head

A head from something that is not this protocol at all:

```text
vector dg02-head-foreign
53 53 48 2d 00 00 00 00 07 17 f3 fb da f2 53 00
00
```

The correct answer is **zero octets**. There is no vector for the reply because there is no reply.

### A longer identity

The head a hypothetical `DGX1` wire would write. Its identity is `DGX`, not `DG`, because the run of
capitals does not stop after two octets, so the correct answer is **zero octets**, exactly as for the
foreign head above.

```text
vector dg02-head-longer-identity
44 47 58 31 00 00 00 00 07 17 f3 fb da f2 53 00
00
```

The codec meets this: `WireVersion::read` refuses a capital where the version begins, so the run is
seen to continue and the head is foreign. Two of this family's identities already share a prefix, so
that one comparison is the only thing standing between a longer neighbour and an answer naming this
host's version. The test that generates these vectors asserts the silence, and reverting the check
turns this page red.

## Regenerating the vectors

The vectors are generated, never typed. `src/protocol_tests.rs` builds each frame with this crate's own
writer, proves the reader takes the same octets back to the same value, parses this file, and fails when
the two disagree in either direction: a vector this page publishes that the codec no longer writes is
the same defect as one the codec writes that this page has not caught up with.

Run:

```text
cargo test -p measure --lib protocol::protocol_tests::vectors -- --nocapture
```

It prints every vector in exactly the block form used above, sixteen octets to a line, so a regenerated
block pastes over the old one unedited. The test reads this file for lines of the form `vector <name>`
and takes the octet lines under each, up to the next blank line or fence, as that vector's frame. Keep
that shape when editing, and keep the field-by-field tables beside the blocks in step by hand: the
octets are checked mechanically, the annotations are not.
