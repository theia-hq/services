# Changelog: sshh

All notable changes to `sshh`, newest first. It carries its own version and its own cadence, so a
release here moves `sshh` and nothing else in this repository.

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
