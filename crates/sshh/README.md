# sshh

A keyless SSH server over an already-authenticated byte stream.

`serve` runs one SSH connection on a stream that was already admitted: it accepts the SSH `none` auth
method and goes straight to a shell, because the stream's admission is the authentication. The host key is
derived from the node's own identity, so a client's `known_hosts` pins the machine across connections.

## Authorize before serve

`serve` consumes a `nauthy::Admitted` witness by value, and `Admitted` has no public constructor, so
"authorize before serve" is a compile-time precondition, not a check the caller remembers to write. The
witness is single-use: one admission authorizes exactly one connection.

## What it refuses

- **Root.** A shell served to an admitted peer runs as this process's user, so a privileged process would
  hand out a root shell. `serve` returns `ServeError::Root` instead.
- **More than 64 live shells per process.** Past the cap, a new shell request is refused, so an admitted
  peer cannot fork-bomb the host.
- **A revoked capability, at connect.** Revocation plus a short capability lifetime is the recall story;
  it does not cut a session already in progress.

## The entry point

`serve(admitted, host_seed, writer, reader)` is the whole engine. The caller owns admission and exposure,
and derives the host-key seed from the node identity with the exported `host_seed(&secret)`.

It is its own crate so the heavy, security-sensitive dependency tree (`russh`, `ssh-key`, `pty-process`)
stays out of programs that do not serve a shell.

## Honest limits

- **The shell is remote code execution by construction.** The guards bound how many shells run and who can
  start one; they do not make a shell safe to expose. Who reaches it, and with what capability, is the
  embedder's policy.
- **The shell runs as this process's user.** There is no per-user mapping. A standard `ssh` client works
  unchanged and `scp -O` (the legacy exec mode) works; the SFTP subsystem is not implemented, so `sftp`
  and the newer SFTP-based `scp` do not.
- **Unix only.** The pty layer is `rustix`'s Unix pty API, and the root check reads the process's Unix user
  ids.
- **Dropping the `serve` future does not abort a live shell.** The SSH session runs on a detached task, so
  the shell runs until the client disconnects or exits. The live-shell cap bounds how many run at once.
- **Experimental.** Version `0.0.0`, `publish = false`, consumed by exact git revs. The API changes
  without notice.

## License

Licensed under either of Apache-2.0 or MIT at your option.
