use super::*;

#[test]
fn host_seed_derivation_is_byte_frozen() {
    // The host-key seed is a byte-frozen KDF: a client pins the resulting host key in its `known_hosts`, so
    // the derivation MUST reproduce these exact bytes forever. These vectors were captured from the
    // derivation's prior home before it moved into this crate; a mismatch means the host key silently
    // changed under every already-pinned client.
    assert_eq!(
        host_seed(&[0u8; 32]),
        [
            151, 138, 105, 169, 13, 104, 41, 191, 14, 92, 218, 56, 119, 215, 55, 71, 101, 199, 251,
            237, 109, 231, 230, 255, 113, 225, 147, 158, 213, 47, 167, 116
        ],
        "the all-zero secret must derive its frozen host seed"
    );
    let secret: [u8; 32] = core::array::from_fn(|i| i as u8);
    assert_eq!(
        host_seed(&secret),
        [
            122, 160, 229, 137, 134, 125, 49, 136, 50, 153, 71, 185, 13, 248, 208, 94, 143, 160,
            225, 86, 61, 41, 106, 190, 227, 244, 37, 162, 103, 244, 91, 5
        ],
        "a non-trivial secret must derive its frozen host seed"
    );
}

#[test]
fn geometry_maps_cols_rows_and_floors_zero_at_one() {
    // A client (cols, rows) maps to the (rows, cols) order `Size::new` takes; a normal geometry passes
    // through untouched.
    assert_eq!(
        clamp_geometry(120, 40),
        (40, 120),
        "returns (rows, cols) from (cols, rows)"
    );
    // A zero dimension (an unset field defaults to 0) is degenerate for a pty, so each floors at 1: a
    // window-change carrying a 0 must never resize the pty to a 0-row/0-col grid that misrenders.
    assert_eq!(clamp_geometry(0, 40), (40, 1), "0 cols floors to 1");
    assert_eq!(clamp_geometry(120, 0), (1, 120), "0 rows floors to 1");
    assert_eq!(clamp_geometry(0, 0), (1, 1), "both floor to 1");
}

#[test]
fn shell_slots_are_capped_and_released() {
    // A count of its own: the process-wide one is shared with every other test that opens a shell.
    static SLOTS: ShellSlots = ShellSlots(AtomicUsize::new(0));
    // Reserve the whole ceiling; the next reservation is refused (the fork-bomb guard).
    let mut slots: Vec<ShellSlot> = Vec::new();
    for _ in 0..MAX_LIVE_SHELLS {
        match SLOTS.acquire() {
            Some(slot) => slots.push(slot),
            None => panic!("reservations under the cap must succeed"),
        }
    }
    assert!(
        SLOTS.acquire().is_none(),
        "at the cap, a new shell is refused"
    );
    // Releasing one frees exactly one slot, then the cap holds again.
    slots.pop();
    match SLOTS.acquire() {
        Some(freed) => {
            assert!(
                SLOTS.acquire().is_none(),
                "still capped after taking the freed slot"
            );
            drop(freed);
        }
        None => panic!("a released slot must reopen"),
    }
    drop(slots);
    assert_eq!(SLOTS.0.load(Ordering::Acquire), 0, "every slot released");
}

/// A shell in a real pty, running `script`, with its pid, the two splice halves, and the far end of
/// each, which the caller holds open so nothing but the shell itself or a hangup can end the attendance.
struct Fixture {
    pty: pty_process::Pty,
    child: tokio::process::Child,
    pid: u32,
    writer: io::DuplexStream,
    reader: io::DuplexStream,
    _far: (io::DuplexStream, io::DuplexStream),
}

fn shell(script: &str) -> Fixture {
    let (pty, pts) = pty_process::open().expect("open a pty");
    pty.resize(Size::new(24, 80)).expect("size the pty");
    let child = pty_signals(Command::new("/bin/sh").arg("-c").arg(script))
        .spawn(pts)
        .expect("spawn the shell");
    let pid = child.id().expect("a running child has a pid");
    let (writer, client_reads) = io::duplex(1024);
    let (client_writes, reader) = io::duplex(1024);
    Fixture {
        pty,
        child,
        pid,
        writer,
        reader,
        _far: (client_reads, client_writes),
    }
}

/// Whether `pid` still names a live process (a zombie counts as gone: it has exited).
fn alive(pid: u32) -> bool {
    let Ok(pid) = i32::try_from(pid) else {
        return false;
    };
    // SAFETY: `kill` with signal 0 only probes whether the pid exists and may be signalled.
    unsafe { libc::kill(pid, 0) == 0 }
}

#[tokio::test]
async fn a_shell_whose_connection_ends_is_hung_up_and_reaped() {
    // The connection outlives nothing: once its sender drops, the pty closes under the shell and the
    // shell exits, rather than running on with no one attached.
    let Fixture {
        pty,
        child,
        pid,
        writer,
        reader,
        _far,
    } = shell("exec sleep 1000");
    let (connection, hangup) = watch::channel(());
    let (_resize, resize_rx) = watch::channel(Size::new(24, 80));
    let attending = tokio::spawn(attend(pty, child, writer, reader, resize_rx, hangup));
    tokio::time::sleep(core::time::Duration::from_millis(200)).await;
    assert!(alive(pid), "the shell runs while its connection lives");

    drop(connection);
    let attended = tokio::time::timeout(core::time::Duration::from_secs(5), attending)
        .await
        .expect("a hung-up shell must exit, not outlive its connection")
        .expect("the attendance task joins");
    assert_eq!(attended, Attended::HungUp);
    assert!(!alive(pid), "the shell is gone and reaped");
}

#[tokio::test]
async fn a_shell_that_exits_first_reports_its_code() {
    let Fixture {
        pty,
        child,
        writer,
        reader,
        _far: (_client_reads, client_writes),
        ..
    } = shell("exit 3");
    // The client has nothing more to send, so the splice's input side ends and only the shell is left.
    drop(client_writes);
    let (_connection, hangup) = watch::channel(());
    let (_resize, resize_rx) = watch::channel(Size::new(24, 80));
    let attended = tokio::time::timeout(
        core::time::Duration::from_secs(5),
        attend(pty, child, writer, reader, resize_rx, hangup),
    )
    .await
    .expect("a shell that exits ends its attendance");
    assert_eq!(
        attended,
        Attended::Exited(Exit::Code(3)),
        "the client is owed the code"
    );
}

/// Hang up `script`'s shell and return how long it took to be gone, asserting it is gone.
async fn hang_up(script: &str) -> core::time::Duration {
    let Fixture {
        pty,
        child,
        pid,
        writer,
        reader,
        _far,
    } = shell(script);
    let (connection, hangup) = watch::channel(());
    let (_resize, resize_rx) = watch::channel(Size::new(24, 80));
    let attending = tokio::spawn(attend(pty, child, writer, reader, resize_rx, hangup));
    tokio::time::sleep(core::time::Duration::from_millis(200)).await;
    assert!(alive(pid), "the shell runs while its connection lives");
    let started = std::time::Instant::now();
    drop(connection);
    let attended = tokio::time::timeout(HANGUP_GRACE * 3, attending)
        .await
        .expect("a hung-up shell must be gone within the grace, not outlive its connection")
        .expect("the attendance task joins");
    assert_eq!(attended, Attended::HungUp);
    assert!(!alive(pid), "the shell is gone and reaped");
    started.elapsed()
}

#[tokio::test]
async fn a_shell_that_ignores_the_hangup_is_killed_after_the_grace() {
    // The shell ignores SIGHUP and execs into a sleep that inherits it: only the kill ends it.
    let took = hang_up("trap '' HUP; exec sleep 1000").await;
    assert!(
        took >= HANGUP_GRACE,
        "killed after the grace, not before: {took:?}"
    );
}

#[test]
fn a_shell_spawned_by_a_process_ignoring_the_hangup_still_hears_it() {
    // A serve under `nohup` ignores SIGHUP, and an ignored disposition survives `exec`. The shell must
    // start with it at its default, so the hangup ends it at once rather than only the grace's kill.
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("a runtime");
    // SAFETY: sets this test process's own SIGHUP disposition, restored below; nothing here raises it.
    let previous = unsafe { libc::signal(libc::SIGHUP, libc::SIG_IGN) };
    let took = runtime.block_on(hang_up("exec sleep 1000"));
    // SAFETY: restores the disposition saved above.
    unsafe { libc::signal(libc::SIGHUP, previous) };
    assert!(took < HANGUP_GRACE, "the hangup itself ended it: {took:?}");
}

#[test]
fn a_signal_goes_by_its_ssh_name() {
    assert!(matches!(sig(libc::SIGTERM), russh::Sig::TERM));
    assert!(matches!(sig(libc::SIGKILL), russh::Sig::KILL));
    // A signal russh has no variant for goes by its name without `SIG`, never a number a client cannot
    // read.
    assert!(matches!(sig(libc::SIGUSR2), russh::Sig::Custom(name) if name == "USR2"));
}

/// A client that trusts any host key: the session under test is reached over an in-memory stream.
struct Client;

impl russh::client::Handler for Client {
    type Error = russh::Error;

    async fn check_server_key(
        &mut self,
        _key: &russh::keys::PublicKeyOrCertificate,
    ) -> Result<bool, Self::Error> {
        Ok(true)
    }
}

/// A client signed in to a fresh session, and the session's task, which a test aborts to cut it.
async fn connect() -> (
    russh::client::Handle<Client>,
    tokio::task::JoinHandle<Result<(), ServeError>>,
) {
    // Room for a whole test's traffic each way. Both ends here are russh, whose session loop stops
    // reading while a write to the stream is pending, so with both directions busy (a MiB in while a
    // child's output streams out) a small in-memory pipe fills on both sides and wedges both loops. A real
    // `ssh` client reads while it writes and never meets the other in that state.
    let (client_end, server_end) = io::duplex(16 * 1024 * 1024);
    let (reader, writer) = io::split(server_end);
    let server = tokio::spawn(session([7; 32], writer, reader));
    let config = std::sync::Arc::new(russh::client::Config::default());
    let mut client = russh::client::connect_stream(config, client_end, Client)
        .await
        .expect("the handshake completes");
    let auth = client.authenticate_none("anyone").await.expect("auth runs");
    assert!(auth.success(), "the session accepts `none`");
    (client, server)
}

/// Whether the exec asks for a terminal first.
enum Tty {
    Pipes,
    Pty,
}

/// What the client sends the exec.
enum Input {
    /// These bytes, then end of input.
    Closed(Vec<u8>),
    /// Nothing, and never end of input, as a terminal holding `ssh`'s stdin behaves.
    Held,
}

/// What a client saw of one exec.
#[derive(Default)]
struct Seen {
    stdout: Vec<u8>,
    stderr: Vec<u8>,
    status: Option<u32>,
    signal: Option<russh::Sig>,
    /// Whether an output byte arrived after the exit report.
    output_after_report: bool,
}

impl Seen {
    fn reported(&self) -> bool {
        self.status.is_some() || self.signal.is_some()
    }
}

/// How long an exec may take to end on its own before a test calls it hung.
const EXEC_LIMIT: core::time::Duration = core::time::Duration::from_secs(10);

/// Run `command` over a fresh session and collect everything the client sees until the channel closes.
async fn exec(command: &str, tty: Tty, input: Input) -> Seen {
    let (client, _server) = connect().await;
    let channel = client
        .channel_open_session()
        .await
        .expect("a session channel opens");
    if let Tty::Pty = tty {
        channel
            .request_pty(true, "xterm", 80, 24, 0, 0, &[])
            .await
            .expect("the pty request is sent");
    }
    channel.exec(true, command).await.expect("the exec is sent");
    let (mut read, write) = channel.split();
    let feed = async {
        if let Input::Closed(bytes) = input {
            // The exec may end before it has read all of this; a refused write is its business.
            let _ = write.data(&bytes[..]).await;
            let _ = write.eof().await;
        }
    };
    let collect = async {
        let mut seen = Seen::default();
        while let Some(message) = read.wait().await {
            match message {
                russh::ChannelMsg::Data { data } => {
                    seen.output_after_report |= seen.reported();
                    seen.stdout.extend_from_slice(&data);
                }
                russh::ChannelMsg::ExtendedData { data, ext: 1 } => {
                    seen.output_after_report |= seen.reported();
                    seen.stderr.extend_from_slice(&data);
                }
                russh::ChannelMsg::ExitStatus { exit_status } => seen.status = Some(exit_status),
                russh::ChannelMsg::ExitSignal { signal_name, .. } => {
                    seen.signal = Some(signal_name);
                }
                russh::ChannelMsg::Close => break,
                _ => {}
            }
        }
        seen
    };
    let ((), seen) = tokio::time::timeout(EXEC_LIMIT, async { tokio::join!(feed, collect) })
        .await
        .expect("the exec must end on its own");
    // Held until here, so a held input really is still open while the exec runs.
    drop(write);
    seen
}

#[tokio::test]
async fn an_exec_without_a_pty_request_reads_stdin_to_eof() {
    let seen = exec("cat", Tty::Pipes, Input::Closed(b"hello".to_vec())).await;
    assert_eq!(seen.stdout, b"hello", "the input comes back untouched");
    assert_eq!(seen.status, Some(0), "cat saw end of input and exited");
}

#[tokio::test]
async fn an_exec_without_a_pty_passes_every_byte_value() {
    let every: Vec<u8> = (0..=255).collect();
    let seen = exec("cat", Tty::Pipes, Input::Closed(every.clone())).await;
    assert_eq!(
        seen.stdout, every,
        "no byte is rewritten, dropped or echoed"
    );
    assert_eq!(seen.status, Some(0));
}

#[tokio::test]
async fn an_exec_without_a_pty_keeps_stderr_apart() {
    let seen = exec(
        "echo out; echo err >&2",
        Tty::Pipes,
        Input::Closed(Vec::new()),
    )
    .await;
    assert_eq!(seen.stdout, b"out\n", "stdout carries only stdout");
    assert_eq!(seen.stderr, b"err\n", "stderr arrives as extended data");
    assert_eq!(seen.status, Some(0));
}

#[tokio::test]
async fn an_exec_after_a_pty_request_gets_a_terminal() {
    let seen = exec("test -t 0", Tty::Pty, Input::Closed(Vec::new())).await;
    assert_eq!(seen.status, Some(0), "stdin is a terminal");
}

#[tokio::test]
async fn a_pipe_exec_ends_when_the_child_exits_with_stdin_still_open() {
    let seen = exec("echo hi", Tty::Pipes, Input::Held).await;
    assert_eq!(seen.stdout, b"hi\n");
    assert_eq!(
        seen.status,
        Some(0),
        "reported without the client's end of input"
    );
}

#[tokio::test]
async fn a_pipe_exec_sends_all_output_before_its_exit_status() {
    const MIB: usize = 1024 * 1024;
    let seen = exec(&format!("head -c {MIB} /dev/zero"), Tty::Pipes, Input::Held).await;
    assert_eq!(seen.stdout.len(), MIB, "every byte the child wrote arrives");
    assert!(
        !seen.output_after_report,
        "no output follows the exit report"
    );
    assert_eq!(seen.status, Some(0));
}

#[tokio::test]
async fn a_pipe_exec_whose_child_ignores_stdin_survives_epipe() {
    // The child takes one byte, then lets go of its stdin while a MiB is still arriving for it, and only
    // then writes: every write into its stdin from there on fails with `BrokenPipe`.
    let seen = exec(
        "head -c1 >/dev/null; exec </dev/null; seq 100000",
        Tty::Pipes,
        Input::Closed(vec![b'x'; 1024 * 1024]),
    )
    .await;
    let expected: String = (1..=100_000).map(|n| format!("{n}\n")).collect();
    assert!(
        seen.stdout == expected.as_bytes(),
        "the output is whole: {} of {} bytes",
        seen.stdout.len(),
        expected.len()
    );
    assert_eq!(seen.status, Some(0));
}

#[tokio::test]
async fn a_signalled_exec_reports_exit_signal() {
    for (tty, path) in [(Tty::Pipes, "pipes"), (Tty::Pty, "pty")] {
        let seen = exec("kill -TERM $$", tty, Input::Closed(Vec::new())).await;
        assert!(
            matches!(seen.signal, Some(russh::Sig::TERM)),
            "on {path}, the signal is reported: {:?}",
            seen.signal
        );
        assert_eq!(seen.status, None, "on {path}, no exit status goes with it");
    }
}

/// Open a session and exec `command` on pipes, which prints a pid as its first line. Returns the pid, the
/// session's task to abort for the cut, and the client, held so only the abort ends the session.
async fn exec_printing_a_pid(
    command: &str,
) -> (
    u32,
    tokio::task::JoinHandle<Result<(), ServeError>>,
    russh::client::Handle<Client>,
) {
    let (client, server) = connect().await;
    let mut channel = client
        .channel_open_session()
        .await
        .expect("a session channel opens");
    channel.exec(true, command).await.expect("the exec is sent");
    let mut line = Vec::new();
    while !line.ends_with(b"\n") {
        match channel.wait().await {
            Some(russh::ChannelMsg::Data { data }) => line.extend_from_slice(&data),
            Some(_) => {}
            None => panic!("the channel closed before the pid arrived"),
        }
    }
    let pid: u32 = String::from_utf8_lossy(&line)
        .trim()
        .parse()
        .expect("the exec printed a pid");
    (pid, server, client)
}

/// Cut the session and return how long `pid` took to be gone, asserting it went within twice the grace.
async fn cut(
    pid: u32,
    server: tokio::task::JoinHandle<Result<(), ServeError>>,
) -> core::time::Duration {
    assert!(alive(pid), "the process runs while its session lives");
    // Dropping the session is the cut.
    server.abort();
    let started = std::time::Instant::now();
    while alive(pid) && started.elapsed() < HANGUP_GRACE * 2 {
        tokio::time::sleep(core::time::Duration::from_millis(20)).await;
    }
    let took = started.elapsed();
    assert!(!alive(pid), "the process outlived the cut and the grace");
    took
}

#[tokio::test]
async fn a_cut_session_hangs_up_a_pipe_exec_blocked_off_stdin() {
    let (pid, server, _client) = exec_printing_a_pid("echo $$; exec sleep 60").await;
    let took = cut(pid, server).await;
    assert!(
        took < HANGUP_GRACE,
        "the hangup ended it, not the grace's kill: {took:?}"
    );
}

#[tokio::test]
async fn a_cut_session_hangs_up_a_grandchild_holding_stdout() {
    // The shell exits at once; the sleep it left behind holds stdout, which keeps the exec open. The cut
    // must still reach the sleep, though the process that led its group is gone.
    let (pid, server, _client) = exec_printing_a_pid("sleep 60 & echo $!").await;
    tokio::time::sleep(core::time::Duration::from_millis(500)).await;
    let took = cut(pid, server).await;
    assert!(
        took < HANGUP_GRACE,
        "the hangup ended it, not the grace's kill: {took:?}"
    );
}

#[tokio::test]
async fn a_cut_session_kills_a_grandchild_that_ignores_the_hangup() {
    // As above, but the sleep ignores SIGHUP: only the group's kill after the grace ends it, and that
    // kill must come though the shell that led the group exited long before.
    let (pid, server, _client) = exec_printing_a_pid("trap '' HUP; sleep 60 & echo $!").await;
    tokio::time::sleep(core::time::Duration::from_millis(500)).await;
    let took = cut(pid, server).await;
    assert!(
        took >= HANGUP_GRACE,
        "killed after the grace, not before: {took:?}"
    );
}

#[tokio::test]
async fn an_exec_on_a_second_channel_without_a_pty_request_runs_on_pipes() {
    // Two channels on one connection, as ssh multiplexing makes them: the first asks for a pty, the
    // second does not, and must get pipes.
    let (client, _server) = connect().await;
    let mut first = client
        .channel_open_session()
        .await
        .expect("the first channel opens");
    first
        .request_pty(true, "xterm", 80, 24, 0, 0, &[])
        .await
        .expect("the pty request is sent");
    first
        .exec(true, "exit 0")
        .await
        .expect("the first exec is sent");
    first.eof().await.expect("end of input is sent");
    let first_done = tokio::time::timeout(EXEC_LIMIT, async {
        while let Some(message) = first.wait().await {
            if let russh::ChannelMsg::Close = message {
                break;
            }
        }
    });
    first_done.await.expect("the first exec ends");

    let mut second = client
        .channel_open_session()
        .await
        .expect("the second channel opens");
    second
        .exec(true, "if test -t 0; then echo TTY; else echo PIPE; fi")
        .await
        .expect("the second exec is sent");
    second.eof().await.expect("end of input is sent");
    let mut stdout = Vec::new();
    let mut status = None;
    tokio::time::timeout(EXEC_LIMIT, async {
        while let Some(message) = second.wait().await {
            match message {
                russh::ChannelMsg::Data { data } => stdout.extend_from_slice(&data),
                russh::ChannelMsg::ExitStatus { exit_status } => status = Some(exit_status),
                russh::ChannelMsg::Close => break,
                _ => {}
            }
        }
    })
    .await
    .expect("the second exec must end on its own");
    assert_eq!(
        stdout, b"PIPE\n",
        "the pty asked for on another channel is not this one's"
    );
    assert_eq!(status, Some(0));
}

/// Set for the probe below when this test binary runs it under a terminal of its own.
const TTY_PROBE: &str = "SSHH_TTY_PROBE";

#[tokio::test]
async fn a_pipe_exec_has_no_controlling_terminal() {
    // The test process may have no terminal at all, which would prove nothing, so the probe runs in a
    // copy of this binary that leads a session with a pty as its controlling terminal.
    let (pty, pts) = pty_process::open().expect("open a pty");
    pty.resize(Size::new(24, 80)).expect("size the pty");
    let exe = std::env::current_exe().expect("this test binary's path");
    let mut probe = Command::new(exe)
        .args([
            "--exact",
            "lib_tests::a_pipe_exec_probe_under_a_terminal",
            "--ignored",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(TTY_PROBE, "1")
        .spawn(pts)
        .expect("spawn the probe");
    let (mut output, _input) = pty.into_split();
    let mut seen = Vec::new();
    let read = async {
        let mut buf = [0u8; 4096];
        // The pty reports an error once the probe is gone and the terminal has no one left on it.
        while let Ok(n @ 1..) = output.read(&mut buf).await {
            seen.extend_from_slice(&buf[..n]);
        }
    };
    let _ = tokio::time::timeout(EXEC_LIMIT, read).await;
    let status = tokio::time::timeout(EXEC_LIMIT, probe.wait())
        .await
        .expect("the probe finishes")
        .expect("the probe is waited for");
    let seen = String::from_utf8_lossy(&seen);
    assert!(
        status.success() && seen.contains("1 passed"),
        "the probe must run and pass under a terminal:\n{seen}"
    );
}

#[tokio::test]
#[ignore = "run under a terminal by a_pipe_exec_has_no_controlling_terminal"]
async fn a_pipe_exec_probe_under_a_terminal() {
    if std::env::var_os(TTY_PROBE).is_none() {
        return;
    }
    assert!(
        std::fs::File::open("/dev/tty").is_ok(),
        "the probe itself has a controlling terminal, or it proves nothing"
    );
    let seen = exec("exec 3</dev/tty", Tty::Pipes, Input::Closed(Vec::new())).await;
    assert!(
        matches!(seen.status, Some(code) if code != 0),
        "a piped child cannot open a terminal: {:?}",
        seen.status
    );
}
