# transfer

Push a file to a keyed node and receive it there, over an admitted stream.

`Recv` reads one blob off an already-admitted stream, checks every byte against the BLAKE3 root the
sender sent (the `wire` module's `Transfer`), and moves it into place under an output directory. The
check catches a file corrupted or changed while it was sent; it does not prove who wrote it, because the
sender names the root. Who sent it is the key the gate admitted, reported with each file. A failed
transfer never lands under the sender's name, and its temp file is removed; only a receiver killed
mid-transfer leaves one behind.

A sender-supplied name is reduced to a safe relative path first: roots, prefixes, and `..` are dropped, so
a peer can never write outside the output directory. An empty or all-stripped name falls back to
`download`.

One stream carries one file. Call it once per stream; this crate has no fan-out of its own.

## The entry points

`Recv::new(out)` is the receiving engine: a `Handler` impl whose ceiling is `Never` (a stranger writing
files into the node's output directory has no public use), bound to the `recv:` route. The protocol body
is crate-private. Each instance owns its output directory and its per-stream temp tag, so two receive
services never share one and concurrent pushes never contend for the same temp path. The caller owns
admission and disk bounds.

The engine prints nothing. To hear about files that land, build it with `Recv::new(out).with_sink(sink)`:
each landed file is handed to `sink` as a `Received` value (its path, its byte count and the sender's
admitted key), once, after it is in place. The path is the sender's name, so escape it before you print
it. A `ReceivedSink` must not block, because it runs before the stream finishes.

To push, hash the source with `wire::Blob::hash`, rewind it, and call
`wire::Transfer::new(writer, reader).send(name, &blob, &mut source)` on a stream the receiver admits. It
returns once the receiver has landed the file; a refusal once the receiver has read the frame's head is
`wire::Error::Rejected`, and no answer within 10 minutes is `wire::Error::AckTimeout`, and the file may have
landed.

## The limits

- **No byte cap.** An admitted sender can fill the output directory. The caller bounds disk.
- **One file per stream.** Dialing, walking a directory and running streams in parallel are the caller's.
- **A failed transfer is not resumed.** A truncated or corrupted transfer is rejected and its temp file is
  removed, so the sender starts that file over.
- **Concurrent calls need distinct tags.** The tag names the temp file (`.transfer-<pid>-<tag>.part`), so
  two streams that share a tag share a temp path.
- **Experimental.** The API changes without notice.

## License

Licensed under either of Apache-2.0 or MIT at your option.
