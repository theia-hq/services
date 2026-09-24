//! `sshh`: a keyless SSH server over an already-authenticated byte stream.
//!
//! theia's equivalent of Tailscale SSH. The caller hands [`serve`] one stream that a capability-gated
//! overlay has ALREADY mutually authenticated (QUIC + raw-public-key TLS, addressed by ed25519 node id)
//! and encrypted; the peer was authorized by a capability. So SSH's own transport job is already done, and
//! this server accepts the SSH `none` auth method (russh's default) and goes straight to a shell: the
//! capability IS the auth, exactly as Tailscale SSH accepts `none` behind WireGuard. A standard `ssh`/`scp`
//! client works unchanged, with no ssh keys to manage.
//!
//! This lives in its own crate, apart from the byte-moving layer, so its heavy, security-sensitive
//! dependency tree (`russh`, `ssh-key`, `pty-process`) stays out of a lean, reach-only client.
//!
//! SAFETY: a shell has no auth of its own, so [`serve`] must only ever receive a stream a real gate already
//! admitted (never a raw socket, never an `open` gate). [`Sshd`] is the entry and narrows the gate proof to
//! a rooted witness first, so an open witness cannot even name the body; the body itself takes the rooted
//! proof by type. As a second line of defence, it refuses
//! to run as root, since a cap-holder would otherwise get a root shell.
//!
//! NOT YET (tracked follow-ups from the Tailscale-parity study): SFTP/scp, and per-user mapping (today
//! the shell runs as this process's uid).

use core::sync::atomic::{AtomicUsize, Ordering};
use core::time::Duration;

use pty_process::{Command, Size};
use russh::server::{Handler, Msg, Session};
use russh::{Channel, ChannelId};
use tokio::io::{self, AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _};
use tokio::sync::watch;

mod handler;
mod pipes;
pub use handler::Sshd;

/// The maximum number of concurrent shells this process serves across ALL connections. A shell has no
/// login of its own, so an admitted peer (or a leaked slip) could otherwise open unbounded channels and
/// connections and fork-bomb the host, running arbitrary code as this uid. Past this cap a new shell
/// request is refused (`channel_failure`); the ceiling is generous for real interactive/exec use and
/// bounded against abuse. Node-wide, not per-connection, because a flood opens many connections.
const MAX_LIVE_SHELLS: usize = 64;

/// How long a hung-up shell has to exit before its process group is killed. A shell exits on the hangup
/// at once; one that ignores it (a `trap '' HUP`, a `nohup`ed job left as the leader) would otherwise hold
/// its live-shell slot, and run as this user, for as long as the process lives.
const HANGUP_GRACE: Duration = Duration::from_secs(3);

/// Live shell count across the whole process.
static LIVE_SHELLS: ShellSlots = ShellSlots(AtomicUsize::new(0));

/// A count of live shells, capped at [`MAX_LIVE_SHELLS`] and reserved one [`ShellSlot`] at a time.
struct ShellSlots(AtomicUsize);

impl ShellSlots {
    /// Reserve a slot, or `None` if the count is already at [`MAX_LIVE_SHELLS`]. The reserve-then-check
    /// (fetch_add, roll back if over) is race-free under concurrent connections.
    fn acquire(&'static self) -> Option<ShellSlot> {
        if self.0.fetch_add(1, Ordering::AcqRel) >= MAX_LIVE_SHELLS {
            self.0.fetch_sub(1, Ordering::AcqRel);
            None
        } else {
            Some(ShellSlot(self))
        }
    }
}

/// An RAII reservation of one concurrent-shell slot. Held for the shell's whole lifetime (moved into the
/// serving task) and released on drop (including every early return before the task is spawned), so the
/// count can never leak a slot and wedge the cap shut.
struct ShellSlot(&'static ShellSlots);

impl Drop for ShellSlot {
    fn drop(&mut self) {
        self.0.0.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Serving one SSH connection failed.
#[derive(Debug, thiserror::Error)]
pub enum ServeError {
    /// Refused to serve a shell as root (a cap-holder would get a root shell).
    #[error("refusing to serve a shell as root; run the ssh server as an unprivileged user")]
    Root,
    /// The SSH handshake over the stream failed.
    #[error("ssh handshake")]
    Handshake(#[source] russh::Error),
    /// The SSH session failed after the handshake.
    #[error("ssh session")]
    Session(#[source] russh::Error),
}

/// Derive a node's stable SSH host-key seed from its raw identity secret.
///
/// A keyless shell still presents a host key so a client's `known_hosts` can pin the node across
/// connections (trust-on-first-use, with a later swap detected). That key must be STABLE across runs yet
/// DISTINCT from the node's identity key (no cross-protocol reuse), so it is a domain-separated derivation
/// (BLAKE3 `derive_key`) of the raw secret. The caller derives the seed once from its persisted identity and
/// hands it to [`serve`]; the raw secret itself never enters this crate.
///
/// The domain-separator string is FROZEN: a client pins the resulting host key, so changing it would break
/// every existing `known_hosts` entry.
pub fn host_seed(secret: &[u8; 32]) -> [u8; 32] {
    blake3::derive_key("theia sshh host key v1", secret)
}

/// Run one SSH connection over a stream whose gate the [`Sshd`] handler already narrowed to a ROOTED
/// admission: accept `none` auth and serve a shell or a command. Returns when the client disconnects or
/// the shell exits.
///
/// CONSUMES a [`RootedAdmitted`](tightbeam_handler::RootedAdmitted) witness: a keyless shell accepting
/// `none` auth is safe ONLY behind a gate, so requiring the gate's un-forgeable proof makes "authorize
/// before serve" a compile-time precondition, not a caller's discipline. The handler is the only entry and
/// narrows the gate proof first, so an open witness cannot even name this body.
///
/// Refuses to run as root by construction: a shell served to a cap-holder runs as this process's user, so
/// running privileged would hand every cap-holder a root shell. Run the server unprivileged.
pub(crate) async fn serve<W, R>(
    _rooted: tightbeam_handler::RootedAdmitted,
    host_seed: [u8; 32],
    writer: W,
    reader: R,
) -> Result<(), ServeError>
where
    W: AsyncWrite + Unpin + Send + 'static,
    R: AsyncRead + Unpin + Send + 'static,
{
    if is_root() {
        return Err(ServeError::Root);
    }
    session(host_seed, writer, reader).await
}

/// The connection itself, once [`serve`] has its witness and has refused root.
async fn session<W, R>(host_seed: [u8; 32], writer: W, reader: R) -> Result<(), ServeError>
where
    W: AsyncWrite + Unpin + Send + 'static,
    R: AsyncRead + Unpin + Send + 'static,
{
    // The host key is derived by the caller from the node identity, so it is STABLE across connections:
    // `known_hosts` pins the node you dial instead of a fresh key each time (which trained users to click
    // through host-key warnings). It is not the auth (the overlay already authenticated) but host
    // self-consistency, so a client trusts-on-first-use and detects a later swap.
    let key = ssh_key::PrivateKey::from(ssh_key::private::Ed25519Keypair::from_seed(&host_seed));
    let config = std::sync::Arc::new(russh::server::Config {
        keys: vec![key],
        // Offer ONLY `none` auth: the overlay already authenticated the peer, so ssh must not demand a
        // second credential. Without this russh advertises publickey/password and the client, having no
        // key, is refused before it ever reaches `none`.
        methods: russh::MethodSet::from(&[russh::MethodKind::None][..]),
        ..Default::default()
    });
    // The connection's lifetime, as every shell it spawns sees it: this sender lives in this frame and
    // nowhere else, so it drops exactly when `serve` ends, whether the session finished or the caller
    // dropped this future to cut it. Each shell's task watches for that drop and hangs up its pty.
    let (hangup_tx, hangup) = watch::channel(());
    // Join the two stream halves into one duplex for russh, then run the SSH session to completion.
    let stream = tokio::io::join(reader, writer);
    let running = russh::server::run_stream(config, stream, Shell::new(hangup))
        .await
        .map_err(ServeError::Handshake)?;
    // russh spawns the SSH session on a DETACHED task the moment `run_stream` returns Ok; `running.await`
    // only OBSERVES its completion, it does not drive it, and the shells run on tasks of their own. So
    // dropping this future stops no task by itself. This guard is what does: dropping `serve` drops it,
    // which hangs up every shell this connection spawned and ends the detached session, so the stream
    // halves it owns are released too.
    let _connection = Connection {
        _hangup: hangup_tx,
        session: running.handle(),
    };
    running.await.map_err(ServeError::Session)?;
    Ok(())
}

/// One connection's lifetime, held by [`serve`]'s frame and nowhere else, so it ends exactly when `serve`
/// does: because the session finished, or because the caller dropped `serve` to cut it.
struct Connection {
    /// Dropping it hangs up every shell the connection spawned (see [`hung_up`]).
    _hangup: watch::Sender<()>,
    /// The detached SSH session. It owns the stream halves, so it is told to disconnect: a stream it held
    /// on to could keep the caller's transport connection open after the caller let go of it.
    session: russh::server::Handle,
}

impl Drop for Connection {
    fn drop(&mut self) {
        // Drop cannot await, and the disconnect is a message to the session task, so hand it to the
        // runtime. A session that already ended simply refuses the message. Outside a runtime there is no
        // session task left to tell.
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let session = self.session.clone();
        runtime.spawn(async move {
            let _ = session
                .disconnect(
                    russh::Disconnect::ByApplication,
                    "session ended".to_owned(),
                    String::new(),
                )
                .await;
        });
    }
}

/// Per-connection handler: hold the opened session channel and the requested pty, if any, then on a
/// shell or exec request spawn the child and splice the channel to it. A shell, or a command after a pty
/// request, runs in a pty; a command without one runs on pipes. Auth is not implemented, so russh's
/// default `auth_none` (accept) stands: the overlay already proved the peer.
struct Shell {
    channel: Option<Channel<Msg>>,
    /// The client's `pty_request`, if it made one, and the channel it came on. A command runs in a terminal
    /// only when its own channel asked for one, as with OpenSSH: a pty's line discipline rewrites and
    /// echoes bytes, caps a line, never passes end of input on, and merges stderr into stdout, so a command
    /// run in one cannot read binary input to its end. A connection carries many channels (ssh
    /// multiplexing), so a pty asked for on one says nothing about the next.
    terminal: Terminal,
    term: String,
    cols: u16,
    rows: u16,
    /// Live resize handle into the running splice task, `Some` only after [`Shell::spawn`]. A
    /// `window_change_request` pushes the new [`Size`] through this so the task (sole owner of the pty)
    /// applies it; the pty is never shared. `None` before the shell spawns, so a window-change that
    /// arrives first is captured into `cols`/`rows` and used as the initial size instead.
    resize: Option<watch::Sender<Size>>,
    /// Resolves, through [`hung_up`], once the connection this handler serves is over. Cloned into every
    /// shell's task, so no shell outlives the connection it was opened on.
    hangup: watch::Receiver<()>,
}

impl Shell {
    /// A handler for one connection, whose shells hang up when `hangup`'s sender drops.
    fn new(hangup: watch::Receiver<()>) -> Self {
        Self {
            channel: None,
            terminal: Terminal::None,
            term: String::new(),
            cols: 0,
            rows: 0,
            resize: None,
            hangup,
        }
    }

    /// Serve a shell or an exec request: in a pty, unless it is a command the client asked no pty for.
    fn start(
        &mut self,
        id: ChannelId,
        command: Option<String>,
        session: &mut Session,
    ) -> Result<(), russh::Error> {
        match (command, &self.terminal) {
            (Some(command), Terminal::Requested(on)) if *on == id => {
                self.spawn(id, Some(command), session)
            }
            (Some(command), _) => self.spawn_piped(id, &command, session),
            (None, _) => self.spawn(id, None, session),
        }
    }

    /// Spawn `sh -c <command>` on pipes and splice the ssh channel to them, on its own task so the handler
    /// stays responsive: stdout as channel data, stderr as extended data 1, and channel input into stdin.
    fn spawn_piped(
        &mut self,
        id: ChannelId,
        command: &str,
        session: &mut Session,
    ) -> Result<(), russh::Error> {
        let Some(mut channel) = self.channel.take() else {
            let _ = session.channel_failure(id);
            return Ok(());
        };
        // The same cap as a pty shell, reserved before anything is spawned.
        let Some(slot) = LIVE_SHELLS.acquire() else {
            let _ = session.channel_failure(id);
            return Ok(());
        };
        let Ok(piped) = pipes::spawn(command) else {
            let _ = session.channel_failure(id);
            return Ok(());
        };
        let handle = session.handle();
        let hangup = self.hangup.clone();
        session.channel_success(id)?;
        tokio::spawn(async move {
            let _slot = slot;
            let stdout = channel.make_writer();
            let stderr = channel.make_writer_ext(Some(EXTENDED_DATA_STDERR));
            let input = channel.make_reader();
            let Attended::Exited(exit) = pipes::attend(piped, stdout, stderr, input, hangup).await
            else {
                return;
            };
            report(&handle, id, exit).await;
        });
        Ok(())
    }

    /// Spawn the shell (a login shell, or `sh -c <command>` for exec) in a pty at the requested size and
    /// splice the ssh channel to it, on its own task so the handler stays responsive.
    fn spawn(
        &mut self,
        id: ChannelId,
        command: Option<String>,
        session: &mut Session,
    ) -> Result<(), russh::Error> {
        let Some(mut channel) = self.channel.take() else {
            let _ = session.channel_failure(id);
            return Ok(());
        };
        // Reserve a concurrent-shell slot BEFORE opening a pty or spawning: at the node's cap, refuse
        // rather than let a flood exhaust the host. The slot releases on any early return below, and is
        // moved into the serving task so it lives exactly as long as the shell.
        let Some(slot) = LIVE_SHELLS.acquire() else {
            let _ = session.channel_failure(id);
            return Ok(());
        };
        let (pty, pts) = match pty_process::open() {
            Ok(pair) => pair,
            Err(_) => {
                let _ = session.channel_failure(id);
                return Ok(());
            }
        };
        // The latest geometry the client asked for, from `pty_request` and any `window_change_request`
        // that landed before the shell spawned. Seed the pty and the resize channel with it.
        let size = win_size(self.cols, self.rows);
        if pty.resize(size).is_err() {
            let _ = session.channel_failure(id);
            return Ok(());
        }
        // The splice task owns the pty; a later `window_change_request` pushes a new size through this
        // sender and the task applies it. Keep the sender on `self` so the `&mut self` handler can send.
        let (resize_tx, resize_rx) = watch::channel(size);
        self.resize = Some(resize_tx);
        let term = if self.term.is_empty() {
            "xterm-256color"
        } else {
            &self.term
        };
        let cmd = pty_signals(
            match &command {
                Some(command) => Command::new("/bin/sh").arg("-c").arg(command),
                None => Command::new(login_shell()),
            }
            .env("TERM", term),
        );
        let child = match cmd.spawn(pts) {
            Ok(child) => child,
            Err(_) => {
                let _ = session.channel_failure(id);
                return Ok(());
            }
        };
        let handle = session.handle();
        let hangup = self.hangup.clone();
        session.channel_success(id)?;
        tokio::spawn(async move {
            // Hold the shell slot for the child's whole lifetime; it releases when this task ends.
            let _slot = slot;
            // Splice the ssh channel to the pty: channel input -> shell, shell output -> channel. Take the
            // `'static` writer before the borrowing reader.
            let writer = channel.make_writer();
            let reader = channel.make_reader();
            // A connection that is gone has no channel to report an exit on.
            let Attended::Exited(exit) =
                attend(pty, child, writer, reader, resize_rx, hangup).await
            else {
                return;
            };
            report(&handle, id, exit).await;
        });
        Ok(())
    }
}

impl Handler for Shell {
    type Error = russh::Error;

    /// Accept `none` auth: the cap-gated overlay already authenticated the peer, so ssh owes no second
    /// credential. russh's default rejects `none`, so this override is what makes sshh keyless.
    async fn auth_none(&mut self, _user: &str) -> Result<russh::server::Auth, Self::Error> {
        Ok(russh::server::Auth::Accept)
    }

    async fn channel_open_session(
        &mut self,
        channel: Channel<Msg>,
        reply: russh::server::ChannelOpenHandle,
        _session: &mut Session,
    ) -> Result<(), Self::Error> {
        self.channel = Some(channel);
        reply.accept().await;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn pty_request(
        &mut self,
        id: ChannelId,
        term: &str,
        col_width: u32,
        row_height: u32,
        _pix_width: u32,
        _pix_height: u32,
        _modes: &[(russh::Pty, u32)],
        session: &mut Session,
    ) -> Result<(), Self::Error> {
        self.terminal = Terminal::Requested(id);
        self.term = term.to_owned();
        self.cols = col_width as u16;
        self.rows = row_height as u16;
        session.channel_success(id)?;
        Ok(())
    }

    /// Propagate a live terminal resize to the running pty, so full-screen apps (vim, htop, less) reflow
    /// instead of rendering at the old geometry. OpenSSH sends this on the client's SIGWINCH.
    ///
    /// Two orderings, both handled: AFTER the shell spawned, push the new size to the splice task (sole
    /// owner of the pty) via the resize channel; a send error means the shell already exited, so drop it.
    /// BEFORE the shell spawned (no task yet), record the geometry so [`Shell::spawn`] opens the pty at
    /// the up-to-date size instead of a stale one.
    #[allow(clippy::too_many_arguments)]
    async fn window_change_request(
        &mut self,
        id: ChannelId,
        col_width: u32,
        row_height: u32,
        _pix_width: u32,
        _pix_height: u32,
        session: &mut Session,
    ) -> Result<(), Self::Error> {
        self.cols = col_width as u16;
        self.rows = row_height as u16;
        if let Some(resize) = &self.resize {
            let _ = resize.send(win_size(self.cols, self.rows));
        }
        session.channel_success(id)?;
        Ok(())
    }

    async fn shell_request(
        &mut self,
        id: ChannelId,
        session: &mut Session,
    ) -> Result<(), Self::Error> {
        self.start(id, None, session)
    }

    async fn exec_request(
        &mut self,
        id: ChannelId,
        data: &[u8],
        session: &mut Session,
    ) -> Result<(), Self::Error> {
        let command = String::from_utf8_lossy(data).into_owned();
        self.start(id, Some(command), session)
    }
}

/// Whether the client asked for a terminal, and on which channel.
enum Terminal {
    /// No `pty_request`: a command runs on pipes.
    None,
    /// A `pty_request` arrived on this channel: a command on it runs in a pty, as a shell always does. A
    /// command on any other channel runs on pipes.
    Requested(ChannelId),
}

/// The SSH extended-data type that carries stderr (RFC 4254 section 5.2).
const EXTENDED_DATA_STDERR: u32 = 1;

/// Tell the client how its child ended, then end the channel: the report, `eof`, then `close`, in that
/// order, and only once every output byte is sent, so nothing the child wrote follows the report.
async fn report(handle: &russh::server::Handle, id: ChannelId, exit: Exit) {
    match exit {
        Exit::Code(code) => {
            let _ = handle.exit_status_request(id, code).await;
        }
        Exit::Signal {
            signal,
            core_dumped,
        } => {
            // No `exit-status` with it: a client told only of a signal exits non-zero, as OpenSSH's does.
            let _ = handle
                .exit_signal_request(id, sig(signal), core_dumped, String::new(), String::new())
                .await;
        }
        Exit::Unknown => {}
    }
    let _ = handle.eof(id).await;
    let _ = handle.close(id).await;
}

/// How a child ended, as the client is owed it.
#[derive(Debug, PartialEq, Eq)]
enum Exit {
    /// It exited with this code.
    Code(u32),
    /// A signal killed it.
    Signal { signal: i32, core_dumped: bool },
    /// Its status could not be read. The client gets no report, and its `ssh` exits non-zero.
    Unknown,
}

impl From<std::process::ExitStatus> for Exit {
    fn from(status: std::process::ExitStatus) -> Self {
        use std::os::unix::process::ExitStatusExt as _;
        if let Some(signal) = status.signal() {
            return Self::Signal {
                signal,
                core_dumped: status.core_dumped(),
            };
        }
        status
            .code()
            .and_then(|code| u32::try_from(code).ok())
            .map_or(Self::Unknown, Self::Code)
    }
}

/// The SSH name for a signal: a named [`russh::Sig`] where there is one, else its name without `SIG`.
fn sig(signal: i32) -> russh::Sig {
    use russh::Sig;
    let custom = |name: &str| Sig::Custom(name.to_owned());
    match signal {
        libc::SIGABRT => Sig::ABRT,
        libc::SIGALRM => Sig::ALRM,
        libc::SIGFPE => Sig::FPE,
        libc::SIGHUP => Sig::HUP,
        libc::SIGILL => Sig::ILL,
        libc::SIGINT => Sig::INT,
        libc::SIGKILL => Sig::KILL,
        libc::SIGPIPE => Sig::PIPE,
        libc::SIGQUIT => Sig::QUIT,
        libc::SIGSEGV => Sig::SEGV,
        libc::SIGTERM => Sig::TERM,
        libc::SIGUSR1 => Sig::USR1,
        libc::SIGBUS => custom("BUS"),
        libc::SIGCHLD => custom("CHLD"),
        libc::SIGCONT => custom("CONT"),
        libc::SIGIO => custom("IO"),
        libc::SIGPROF => custom("PROF"),
        libc::SIGSTOP => custom("STOP"),
        libc::SIGSYS => custom("SYS"),
        libc::SIGTRAP => custom("TRAP"),
        libc::SIGTSTP => custom("TSTP"),
        libc::SIGTTIN => custom("TTIN"),
        libc::SIGTTOU => custom("TTOU"),
        libc::SIGURG => custom("URG"),
        libc::SIGUSR2 => custom("USR2"),
        libc::SIGVTALRM => custom("VTALRM"),
        libc::SIGWINCH => custom("WINCH"),
        libc::SIGXCPU => custom("XCPU"),
        libc::SIGXFSZ => custom("XFSZ"),
        #[cfg(any(target_os = "linux", target_os = "android"))]
        libc::SIGPWR => custom("PWR"),
        #[cfg(any(target_os = "linux", target_os = "android"))]
        libc::SIGSTKFLT => custom("STKFLT"),
        #[cfg(not(any(target_os = "linux", target_os = "android")))]
        libc::SIGEMT => custom("EMT"),
        #[cfg(not(any(target_os = "linux", target_os = "android")))]
        libc::SIGINFO => custom("INFO"),
        // Only a real-time signal is left, which has no name of its own.
        other => Sig::Custom(other.to_string()),
    }
}

/// How a shell's attendance ended.
#[derive(Debug, PartialEq, Eq)]
enum Attended {
    /// The child's side closed and it ended so, which the client is owed.
    Exited(Exit),
    /// The connection ended first, so the pty was closed under the shell, and the shell has since exited.
    HungUp,
}

/// Serve one shell until it exits or its connection ends, whichever comes first.
///
/// When the connection ends first, the splice is dropped mid-flight, and with it the pty master: closing
/// the master is a terminal hangup, which is what ends a shell whose client is gone, as it does when an
/// ssh server closes a session's pty. Nothing else here could end it: the connection's future is not
/// this task, so dropping it would leave the shell running with a pty no one reads, and a live-shell slot
/// held, until the whole process exited.
///
/// Either way the child is waited for before this returns, so the slot the caller holds is released only
/// once the shell is really gone. One that ignores the hangup is killed with its group after
/// [`HANGUP_GRACE`] (see [`reap`]), so a cut frees the slot either way.
async fn attend<W, R>(
    pty: pty_process::Pty,
    child: tokio::process::Child,
    writer: W,
    reader: R,
    resize: watch::Receiver<Size>,
    mut hangup: watch::Receiver<()>,
) -> Attended
where
    W: AsyncWrite + Unpin,
    R: AsyncRead + Unpin,
{
    // The shell leads its own session and group, so its pid is the group's id.
    let group = child.id().and_then(|pid| i32::try_from(pid).ok());
    // Each arm's body runs only once both futures are gone, so on a hangup the pty is already closed by
    // the time the wait starts, and the wait can see the shell it hung up exit.
    tokio::select! {
        _ = splice(pty, writer, reader, resize) => Attended::Exited(wait_exit(child).await),
        () = hung_up(&mut hangup) => {
            reap(child, group).await;
            Attended::HungUp
        }
    }
}

/// Give a hung-up shell's process `group` [`HANGUP_GRACE`] to be gone, then kill what is left of it, and
/// reap the shell. The shell leads its own session and group (the pty spawn and the pipe spawn both make
/// it so), so the group is the shell and everything it started in the foreground; a job it detached into
/// a group of its own is the holder's, and outlives it.
///
/// The group can outlive its leader: a process the shell started may still run, and hold a pipe exec
/// open, after the shell itself has exited. So the kill goes to the group whether or not the shell is
/// gone, and this returns early only once the whole group is.
async fn reap(mut child: tokio::process::Child, group: Option<i32>) {
    let deadline = tokio::time::Instant::now() + HANGUP_GRACE;
    if tokio::time::timeout_at(deadline, child.wait())
        .await
        .is_err()
    {
        if let Some(group) = group {
            // SAFETY: `killpg` only sends a signal; `group` is the pgid of our own child, which leads it
            // and is still unreaped, so its id cannot have been recycled for another process.
            unsafe { libc::killpg(group, libc::SIGKILL) };
        }
        let _ = child.start_kill();
        let _ = child.wait().await;
        return;
    }
    // The shell is gone and reaped. The group's id stays reserved while any member lives, so it is probed
    // until it empties, and killed if it has not by the deadline.
    let Some(group) = group else {
        return;
    };
    while group_lives(group) {
        if tokio::time::Instant::now() >= deadline {
            // SAFETY: `killpg` only sends a signal. The group had a member at the probe just before, and
            // an id is never reissued while a group of that id has a member; only the group emptying
            // between that probe and this call, with its id reissued to a new group in that instant,
            // would let this reach another group.
            unsafe { libc::killpg(group, libc::SIGKILL) };
            return;
        }
        tokio::time::sleep(GROUP_PROBE).await;
    }
}

/// How often [`reap`] checks whether a group whose leader is gone has emptied.
const GROUP_PROBE: Duration = Duration::from_millis(20);

/// Whether process group `group` has a member left.
fn group_lives(group: i32) -> bool {
    // SAFETY: `killpg` with signal 0 sends nothing; it only reports whether the group exists. A group it
    // may not signal still exists.
    let probed = unsafe { libc::killpg(group, 0) };
    probed == 0 || std::io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH)
}

/// A pty command whose child starts with its signals reset (see [`reset_signals`]).
fn pty_signals(command: Command) -> Command {
    // SAFETY: the closure runs in the forked child before `exec`, where only async-signal-safe calls are
    // allowed; `reset_signals` makes only those.
    unsafe {
        command.pre_exec(|| {
            reset_signals();
            Ok(())
        })
    }
}

/// Put the hangup, interrupt and quit signals back at their defaults in a forked child, whatever this
/// process inherited. A serve started under `nohup`, or in the background of a non-interactive shell,
/// ignores them, and an ignored disposition survives `exec`: its children would then shrug off the very
/// hangup that ends them when their connection goes.
///
/// Called between `fork` and `exec`, so it makes only async-signal-safe calls; `signal` is one, and it
/// changes the calling process's dispositions only.
fn reset_signals() {
    for signal in [libc::SIGHUP, libc::SIGINT, libc::SIGQUIT] {
        // SAFETY: `signal` with a constant signal number and `SIG_DFL` is async-signal-safe and cannot fail.
        unsafe { libc::signal(signal, libc::SIG_DFL) };
    }
}

/// Resolve once the connection's sender is dropped. It never sends, so a change is only ever the drop.
async fn hung_up(hangup: &mut watch::Receiver<()>) {
    while hangup.changed().await.is_ok() {}
}

/// Copy bytes both ways between the pty and the ssh channel until both sides close, applying any
/// terminal resize that arrives on `resize` to the pty along the way.
///
/// Owns the pty by value and splits it into the two directions with `into_split`, so the pty stays
/// single-owned by this task: resizes come in over the channel and are applied on the write half's own
/// `resize`, sidestepping any shared `Pty` handle. The write direction is a manual select loop (not a
/// plain `io::copy`) precisely so it can also poll the resize receiver; each arm is a whole await with no
/// half-read held across it, so cancelling one to run the other loses no bytes.
async fn splice<W, R>(
    local: pty_process::Pty,
    mut writer: W,
    mut reader: R,
    mut resize: watch::Receiver<Size>,
) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
    R: AsyncRead + Unpin,
{
    let (mut local_reader, mut local_writer) = local.into_split();
    let upstream = async {
        io::copy(&mut local_reader, &mut writer).await?;
        writer.shutdown().await
    };
    let downstream = async {
        let mut buf = [0u8; 8 * 1024];
        loop {
            tokio::select! {
                // `biased` so a pending resize is applied before more input bytes are pumped, keeping the
                // pty geometry current for the data that follows.
                biased;
                // A new terminal size: apply it to the pty. A resize failing is non-fatal (the pty may be
                // tearing down), so log nothing and keep splicing.
                changed = resize.changed() => match changed {
                    // `borrow_and_update` marks the value seen, so the next `changed()` waits for the next
                    // send rather than re-firing on this one.
                    Ok(()) => {
                        let _ = local_writer.resize(*resize.borrow_and_update());
                    }
                    // The sender dropped (the handler is gone), so no more resizes will ever come. A closed
                    // watch resolves `changed()` immediately, which would busy-spin this arm, so copy the
                    // rest of the input with a plain `io::copy` and finish.
                    Err(_) => {
                        io::copy(&mut reader, &mut local_writer).await?;
                        break;
                    }
                },
                read = reader.read(&mut buf) => match read? {
                    // Channel EOF: the client closed its input, so half-close the pty and finish.
                    0 => break,
                    n => local_writer.write_all(&buf[..n]).await?,
                },
            }
        }
        local_writer.shutdown().await
    };
    tokio::try_join!(upstream, downstream)?;
    Ok(())
}

/// Map an SSH window geometry (columns, rows) to a pty [`Size`], clamped to at least 1x1. A client can
/// send a zero dimension (or none at all, defaulting the fields to 0); a 0-row/0-col pty is degenerate and
/// makes full-screen apps misrender, so floor each at 1, mirroring the initial-size clamp.
fn win_size(cols: u16, rows: u16) -> Size {
    let (rows, cols) = clamp_geometry(cols, rows);
    Size::new(rows, cols)
}

/// Floor a client (cols, rows) geometry at 1x1 and return it as (rows, cols), the order [`Size::new`]
/// takes. Split out from [`win_size`] as the testable seam, since [`Size`] exposes no field accessors.
fn clamp_geometry(cols: u16, rows: u16) -> (u16, u16) {
    (rows.max(1), cols.max(1))
}

/// This user's login shell for a bare `shell` request: `$SHELL` if set, else a sane default.
fn login_shell() -> String {
    std::env::var("SHELL").unwrap_or_else(|_| "/bin/sh".to_owned())
}

/// Wait for the shell to exit and say how it ended.
async fn wait_exit(mut child: tokio::process::Child) -> Exit {
    child.wait().await.map_or(Exit::Unknown, Exit::from)
}

/// Whether this process runs as the superuser. A shell served here runs as this uid, so root is refused.
fn is_root() -> bool {
    #[cfg(unix)]
    {
        // SAFETY: getuid/geteuid always succeed; they read the process's uids and cannot fail. Check both
        // the real and effective uid, so neither a root real-uid nor an euid-0 process serves a shell.
        unsafe { libc::geteuid() == 0 || libc::getuid() == 0 }
    }
    #[cfg(not(unix))]
    {
        false
    }
}

#[cfg(test)]
#[path = "lib_tests.rs"]
mod lib_tests;
