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
    // Reserve the whole ceiling; the next reservation is refused (the fork-bomb guard).
    let mut slots: Vec<ShellSlot> = Vec::new();
    for _ in 0..MAX_LIVE_SHELLS {
        match ShellSlot::acquire() {
            Some(slot) => slots.push(slot),
            None => panic!("reservations under the cap must succeed"),
        }
    }
    assert!(
        ShellSlot::acquire().is_none(),
        "at the cap, a new shell is refused"
    );
    // Releasing one frees exactly one slot, then the cap holds again.
    slots.pop();
    match ShellSlot::acquire() {
        Some(freed) => {
            assert!(
                ShellSlot::acquire().is_none(),
                "still capped after taking the freed slot"
            );
            drop(freed);
        }
        None => panic!("a released slot must reopen"),
    }
    drop(slots);
    assert_eq!(
        LIVE_SHELLS.load(Ordering::Acquire),
        0,
        "every slot released"
    );
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
    let child = default_signals(Command::new("/bin/sh").arg("-c").arg(script))
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
    assert_eq!(attended, Attended::Exited(3), "the client is owed the code");
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
