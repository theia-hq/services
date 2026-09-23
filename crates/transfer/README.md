# transfer

Receive one pushed file at a keyed node, over an admitted stream.

`Recv` reads one blob off an already-admitted stream, verifies every byte against the sender's
BLAKE3 root (`bifrost-wire`'s `Transfer`), and moves it into place under an output directory. On any
failure the temp file is removed, so a rejected or truncated transfer leaves no partial file behind.

A sender-supplied name is reduced to a safe relative path first: roots, prefixes, and `..` are dropped, so
a peer can never write outside the output directory. An empty or all-stripped name falls back to
`download`.

One stream carries one file. Call it once per stream; this crate has no fan-out of its own.

## The entry point

`Recv::new(out)` is the whole engine: a `Handler` impl whose ceiling is `Never` (a stranger writing files
into the node's output directory has no public use), bound to the `recv:` route. The protocol body is
crate-private. Each instance owns its output directory and its per-stream temp tag, so two receive services
never share one and concurrent pushes never contend for the same temp path. The caller owns admission and
disk bounds.

The engine prints nothing. To hear about files that land, build it with `Recv::new(out).with_sink(sink)`:
each landed file is handed to `sink` as a `Received` value (its path and byte count), once, after it is in
place. The path is the sender's name, so escape it before you print it. A `ReceivedSink` must not block,
because it runs before the stream finishes.

## The limits

- **No byte cap.** An admitted sender can fill the output directory. The caller bounds disk.
- **Receive only.** This crate is the receiver's per-stream work. The sender side (dial, directory walk,
  concurrent streams) is not here.
- **A failed transfer is not resumed.** A truncated or tampered transfer is rejected and its temp file is
  removed, so the sender starts that file over.
- **Concurrent calls need distinct tags.** The tag names the temp file (`.transfer-<pid>-<tag>.part`), so
  two streams that share a tag share a temp path.
- **Experimental.** The API changes without notice.

## License

Licensed under either of Apache-2.0 or MIT at your option.
