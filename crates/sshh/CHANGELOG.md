# Changelog: sshh

All notable changes to `sshh`, newest first. It carries its own version and its own cadence, so a
release here moves `sshh` and nothing else in this repository.

## v0.6.0

### Changed
- **The host key is derived under `sshh host key v1`.** Every node's host key changes.
- **Needs Rust 1.91 or newer.** The minimum was 1.85. tightbeam v0.16.0 and bifrost v0.7.0 need
  1.91.
- Builds against tightbeam v0.16.0's handler contract, nauthy v0.9.0 and bifrost v0.7.0. The
  contract itself is unchanged.

## v0.5.0

### Changed
- **A command run without a terminal gets pipes, as with OpenSSH.** When the client asks for no pty,
  sshh runs `sh -c <command>` with plain pipes for stdin, stdout and stderr. Bytes pass through
  unchanged, stderr arrives as stderr, and when the client ends its input the command sees end of
  input. Before, every command ran in a pty: control bytes and line endings were rewritten, input was
  echoed, stderr was mixed into stdout, and a command that read its input to the end never finished. A
  shell, or a command run with `ssh -t`, still gets a pty.
- **A command's session ends when the command exits**, once its output is sent. The client no longer
  has to close its input first.
- **A command runs in a session of its own with no terminal**, so it cannot open `/dev/tty`. When the
  node cuts the session, the command's process group gets SIGHUP.
- **A command killed by a signal reports that signal** (`exit-signal`), with or without a pty. Before,
  it reported exit status 0.
- Builds against tightbeam v0.15.0's handler contract, nauthy v0.7.0 and bifrost v0.6.1. The
  contract itself is unchanged.

## v0.4.0

### Changed
- **A shell ends when its session is cut.** When the node cuts a session (for example, because the
  peer's access was revoked while it was connected), sshh hangs up: the shell's process group gets
  SIGHUP, and anything still running 3 s later is killed. Before, a shell could outlive the connection
  that opened it.
- **Children start with SIGHUP, SIGINT and SIGQUIT at their defaults**, so a shell started under a
  parent that ignored them (for example, one run with `nohup`) still hangs up.

Nothing before v0.3.0 is recorded here. Until then the four engines released together under one
number, and those entries are about the set rather than about any one engine, so they stay in the
[repository changelog](../../CHANGELOG.md). `sshh` starts from the v0.3.0 it holds today, reached
in lockstep rather than earned alone, and that stands.

Read that number carefully: `sshh` has had no source change since its first release, and the ten
versions between 0.1.0 and 0.3.0 record releases of the other engines, not of this one. From here
the number moves only when `sshh` does.
