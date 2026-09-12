# transfer

Receive one pushed file at a keyed node, over an admitted stream.

`receive_file` reads one blob off an already-admitted stream, verifies every byte against the sender's
BLAKE3 root (`bifrost-wire`'s `Transfer`), and moves it into place under an output directory. On any
failure the temp file is removed, so a rejected or truncated transfer leaves no partial file behind.

A sender-supplied name is reduced to a safe relative path first: roots, prefixes, and `..` are dropped, so
a peer can never write outside the output directory. An empty or all-stripped name falls back to
`download`.

One stream carries one file. The caller accepts the sender's per-file streams and calls this once per
stream; a directory's files can arrive in parallel with no fan-out logic in this crate.

## The entry point

`receive_file(writer, reader, out, tag)` returns `Received { path, bytes }`: the safe relative path and
the verified length. The caller owns admission and the output directory.

## Honest limits

- **No byte cap.** An admitted sender can fill the output directory. The caller bounds disk.
- **Receive only.** This crate is the receiver's per-stream work. The sender side (dial, directory walk,
  concurrent streams) is not here.
- **One file per stream.** A directory push is many calls, one per stream.
- **A failed transfer is not resumed.** A truncated or tampered transfer is rejected and its temp file is
  removed, so the sender starts that file over.
- **Concurrent calls need distinct tags.** The tag names the temp file (`.transfer-<pid>-<tag>.part`), so
  two streams that share a tag share a temp path.
- **Experimental.** Version `0.0.0`, `publish = false`, consumed by exact git revs. The API changes
  without notice.

## License

Licensed under either of Apache-2.0 or MIT at your option.
