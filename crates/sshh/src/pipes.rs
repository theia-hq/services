//! A command without a terminal: `sh -c <command>` on three pipes, the way OpenSSH runs an exec that
//! asked for no pty.
//!
//! Bytes pass untouched both ways: no line discipline rewrites control bytes or line endings, echoes
//! input, or caps a line, and stderr stays apart from stdout. The client's end of input closes the
//! child's stdin, so a command that reads to end of input finishes.

use core::convert::Infallible;
use std::process::Stdio;

use tokio::io::{self, AsyncRead, AsyncWrite, AsyncWriteExt as _};
use tokio::process::{Child, ChildStderr, ChildStdin, ChildStdout, Command};
use tokio::sync::watch;

use crate::{Attended, Exit, hung_up, reap, reset_signals};

/// Start `sh -c <command>` on three pipes, in a session of its own.
///
/// The child leads its own session and process group and has no controlling terminal, so opening
/// `/dev/tty` fails in it and a prompt that needs a terminal refuses instead of reaching whatever terminal
/// this process was started from. Leading its own group is also what lets a cut hang up everything it
/// started in one `killpg`.
pub(crate) fn spawn(command: &str) -> io::Result<Piped> {
    let mut child = Command::new("/bin/sh");
    child
        .arg("-c")
        .arg(command)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    // SAFETY: the closure runs in the forked child before `exec`, where only async-signal-safe calls are
    // allowed; `signal` and `setsid` are both, and they change the child alone.
    unsafe {
        child.pre_exec(|| {
            reset_signals();
            if libc::setsid() == -1 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let mut child = child.spawn()?;
    let (Some(stdin), Some(stdout), Some(stderr)) =
        (child.stdin.take(), child.stdout.take(), child.stderr.take())
    else {
        // Unreachable while all three are piped above; a child without them has nothing to serve.
        let _ = child.start_kill();
        return Err(io::Error::other("a piped child is missing a pipe"));
    };
    Ok(Piped {
        child,
        stdin,
        stdout,
        stderr,
    })
}

/// A running child and the three pipes it was started with, held apart so each gets its own pump.
pub(crate) struct Piped {
    pub(crate) child: Child,
    stdin: ChildStdin,
    stdout: ChildStdout,
    stderr: ChildStderr,
}

/// Serve one piped child until it is done or its connection ends, whichever comes first.
///
/// Three pumps run: `input` into the child's stdin, its stdout into `stdout`, and its stderr into
/// `stderr`. The exec is done when the child has exited AND both output pipes have reached end of file,
/// as OpenSSH's session close does: never at the client's end of input, which a client that keeps its
/// stdin open (a terminal) never sends. So a child that exits returns here with its input still open,
/// and every byte it wrote is sent before the caller reports its exit. A grandchild that keeps stdout or
/// stderr open keeps the exec open with it.
///
/// On a cut the three pipes are dropped, the child's group gets SIGHUP, and [`reap`] kills what is left
/// after the grace, so the slot the caller holds is released only once the child is gone.
pub(crate) async fn attend<O, E, I>(
    piped: Piped,
    stdout: O,
    stderr: E,
    input: I,
    mut hangup: watch::Receiver<()>,
) -> Attended
where
    O: AsyncWrite + Unpin,
    E: AsyncWrite + Unpin,
    I: AsyncRead + Unpin,
{
    let Piped {
        mut child,
        stdin,
        stdout: out,
        stderr: err,
    } = piped;
    tokio::select! {
        exit = finish(&mut child, feed(input, stdin), drain(out, stdout), drain(err, stderr)) => {
            Attended::Exited(exit)
        }
        () = hung_up(&mut hangup) => {
            // The pumps are dropped by now, so the child's pipes are closed on our side.
            if let Some(group) = child.id().and_then(|pid| i32::try_from(pid).ok()) {
                // SAFETY: `killpg` only sends a signal; `group` is the pgid of our own unreaped child
                // (it leads its own group), so it cannot have been recycled for another process.
                unsafe { libc::killpg(group, libc::SIGHUP) };
            }
            reap(child).await;
            Attended::HungUp
        }
    }
}

/// Wait for the child to exit and both output pumps to drain, while the input pump runs beside them.
///
/// The input pump is dropped, unfinished if need be, once the rest is done: the exec ends with the child,
/// not with the client's input.
async fn finish<F, O, E>(child: &mut Child, input: F, stdout: O, stderr: E) -> Exit
where
    F: Future<Output = ()>,
    O: Future<Output = ()>,
    E: Future<Output = ()>,
{
    let done = async {
        let (status, (), ()) = tokio::join!(child.wait(), stdout, stderr);
        status.map_or(Exit::Unknown, Exit::from)
    };
    let input = async {
        input.await;
        // The child's stdin is closed now; only the child and its output decide when this is over.
        core::future::pending::<Infallible>().await
    };
    tokio::select! {
        exit = done => exit,
        never = input => match never {},
    }
}

/// Copy the client's input into the child's stdin, then close it, so the child sees end of input.
///
/// Ends on the client's end of input. A write that fails ends only the feeding: `BrokenPipe` means the
/// child closed its stdin or exited, and its output is still owed to the client, so nothing here may cut
/// the exec short. The rest of the input is read and discarded, as OpenSSH does once a child's stdin is
/// gone: input left unread on the channel backs up into the connection and stalls the child's output
/// behind it.
async fn feed<I, W>(mut input: I, mut stdin: W)
where
    I: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let fed = io::copy(&mut input, &mut stdin).await;
    // Dropping `stdin` closes the pipe; the shutdown only flushes first, and a child already gone makes
    // it fail harmlessly.
    let _ = stdin.shutdown().await;
    drop(stdin);
    if fed.is_err() {
        let _ = io::copy(&mut input, &mut io::sink()).await;
    }
}

/// Copy one of the child's output pipes to the client. Ends on the pipe's end of file, which comes when
/// every process holding its write end has closed it. It does not send the channel's EOF: that follows
/// the exit report, so nothing the child wrote can arrive after it.
async fn drain<R, W>(mut pipe: R, mut to: W)
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    // A client that stopped reading is a connection going away, which the hangup handles; the rest of
    // the pipe is read and discarded so the child is never blocked on a full pipe.
    if io::copy(&mut pipe, &mut to).await.is_err() {
        let _ = io::copy(&mut pipe, &mut io::sink()).await;
    }
}
