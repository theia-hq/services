use core::time::Duration;
use std::os::fd::AsFd as _;
use std::path::{Path, PathBuf};

use rustix::io::Errno;
use tokio::io;

use super::{
    MAX_RENDERED_PATH, ReceiveError, Received, TempName, receive_file, render_path,
    reserve_then_rename, safe_relative_path,
};
use crate::wire::{self, Blob, Transfer};

#[test]
fn a_traversal_header_is_reduced_to_a_safe_relative_path() {
    // A sender that names an absolute escape or a `..` climb cannot write outside the output directory:
    // roots, prefixes, and parent components are dropped, leaving only the normal tail.
    assert_eq!(
        safe_relative_path(b"../../etc/authorized_keys"),
        std::path::Path::new("etc/authorized_keys")
    );
    assert_eq!(
        safe_relative_path(b"/etc/passwd"),
        std::path::Path::new("etc/passwd")
    );
    // A plain nested name is kept as-is, so a directory push preserves its structure.
    assert_eq!(
        safe_relative_path(b"photos/2026/trip.jpg"),
        std::path::Path::new("photos/2026/trip.jpg")
    );
}

#[test]
fn an_empty_or_all_stripped_header_falls_back_to_download() {
    // An empty header, or one that is nothing but `..`/roots, still lands somewhere nameable rather
    // than at the output directory itself (which a link could not target).
    assert_eq!(safe_relative_path(b""), std::path::Path::new("download"));
    assert_eq!(
        safe_relative_path(b"../.."),
        std::path::Path::new("download")
    );
}

/// A landing failure reports the peer path SAFELY: the error the engine wraps into `ServeError` (and the
/// tunnel logs at warn) carries the escaped/capped form, never the raw control bytes. The blob verifies
/// first; a directory at the destination then refuses the landing.
#[tokio::test]
async fn a_landing_failure_reports_the_path_escaped() {
    let out = fresh_dir("landing-failure");

    let hostile = "evil\nname\u{1b}[31m";
    // The destination path exists as a directory, so the verified temp file cannot land there.
    std::fs::create_dir_all(out.join(hostile)).expect("the blocking directory is creatable");

    let error = push(&out, hostile.as_bytes(), b"payload")
        .await
        .expect_err("a landing onto a directory fails");

    let message = format!("{error:#}");
    assert!(
        message.contains(r"evil\nname\u{1b}[31m"),
        "the error renders the hostile path escaped: {message}"
    );
    assert!(
        !message.contains('\n') && !message.contains('\u{1b}'),
        "no raw control byte rides the error: {message:?}"
    );

    let _ = std::fs::remove_dir_all(&out);
}

/// A push lands as a new file at the name the sender gave, hidden components and all: a directory push
/// carries dotfiles, so a leading dot is not a refusal. No temp file is left beside it.
#[tokio::test]
async fn a_push_lands_as_a_new_file() {
    let out = fresh_dir("lands");

    let received = push(&out, b".config/app/settings", b"PUSHED")
        .await
        .expect("a new name under the output directory lands");

    assert_eq!(received.path, Path::new(".config/app/settings"));
    assert_eq!(received.bytes, 6);
    assert_eq!(
        std::fs::read(out.join(".config/app/settings")).expect("the landed file reads"),
        b"PUSHED"
    );
    assert_no_temp_left(&out);

    let _ = std::fs::remove_dir_all(&out);
}

/// A push never replaces a file the receiver already holds: the name is refused as existing, the file
/// keeps its bytes, the verified temp is removed, and the sender hears no.
#[tokio::test]
async fn a_push_never_replaces_a_file() {
    let out = fresh_dir("no-replace");
    let held = out.join(".config/app/settings");
    std::fs::create_dir_all(out.join(".config/app")).expect("the held directory is creatable");
    std::fs::write(&held, b"ORIGINAL").expect("the held file is writable");

    let (received, sent) = exchange(&out, b".config/app/settings", b"REPLACED").await;
    let error = received.expect_err("an existing name is refused");

    assert!(
        matches!(error, ReceiveError::Exists { .. }),
        "refused as existing: {error:?}"
    );
    // The body verified before the landing was refused, so only the answer can tell the sender: a yes
    // here would report as delivered a file that another push's bytes now hold the name of.
    assert!(
        matches!(sent, Err(wire::Error::Rejected)),
        "the sender is told no: {sent:?}"
    );
    assert_eq!(
        std::fs::read(&held).expect("the held file reads"),
        b"ORIGINAL",
        "the held file keeps its bytes"
    );
    assert_no_temp_left(&out);

    let _ = std::fs::remove_dir_all(&out);
}

/// A retry of a push that already landed is a yes: the name holds exactly the bytes sent, so the sender
/// whose answer was lost is not told no the way a squatted name tells it. The held file is untouched and
/// no temp is left. `a_push_never_replaces_a_file` holds the other side: same length, other bytes, no.
#[tokio::test]
async fn a_retry_of_a_landed_push_is_answered_yes() {
    let out = fresh_dir("retry");

    push(&out, b"app.tar", b"RELEASE")
        .await
        .expect("the first push lands");
    let (received, sent) = exchange(&out, b"app.tar", b"RELEASE").await;

    let received = received.expect("the retry of the same bytes is accepted");
    assert_eq!(received.path, Path::new("app.tar"));
    assert!(sent.is_ok(), "the retrying sender is told yes: {sent:?}");
    assert_eq!(
        std::fs::read(out.join("app.tar")).expect("the landed file reads"),
        b"RELEASE"
    );
    assert_no_temp_left(&out);

    let _ = std::fs::remove_dir_all(&out);
}

/// A FIFO at the name is not a file holding the bytes: the check refuses it without opening it for a
/// read that would wait for a writer, so the push is refused rather than stalled. The push is empty, so
/// only the file-type check tells the FIFO's empty read from an empty file.
#[cfg(unix)]
#[tokio::test]
async fn a_fifo_at_the_name_is_refused_without_stalling() {
    let out = fresh_dir("fifo");
    let made = std::process::Command::new("mkfifo")
        .arg(out.join("pipe"))
        .status()
        .expect("mkfifo runs");
    assert!(made.success(), "the fifo is creatable");

    let error = tokio::time::timeout(Duration::from_secs(5), push(&out, b"pipe", b""))
        .await
        .expect("the push finishes")
        .expect_err("a fifo at the name is refused");

    assert!(
        matches!(error, ReceiveError::Exists { .. }),
        "refused as existing: {error:?}"
    );
    assert_no_temp_left(&out);

    let _ = std::fs::remove_dir_all(&out);
}

/// A symlinked directory inside the output directory that leads out of it is never written through: the
/// push is refused and nothing appears at the symlink's target.
#[cfg(unix)]
#[tokio::test]
async fn a_push_through_a_symlinked_dir_that_leads_out_is_refused() {
    let root = fresh_dir("symlink-out");
    let out = root.join("out");
    let elsewhere = root.join("elsewhere");
    std::fs::create_dir_all(&out).expect("the output directory is creatable");
    std::fs::create_dir_all(&elsewhere).expect("the outside directory is creatable");
    std::os::unix::fs::symlink(&elsewhere, out.join(".linked")).expect("the symlink is creatable");

    let error = push(&out, b".linked/settings", b"ESCAPED")
        .await
        .expect_err("a directory that leads out is refused");

    assert!(
        matches!(error, ReceiveError::Escapes { .. }),
        "refused as leading out: {error:?}"
    );
    assert!(
        !elsewhere.join("settings").exists(),
        "nothing lands outside the output directory"
    );
    assert_no_temp_left(&out);

    let _ = std::fs::remove_dir_all(&root);
}

/// A symlink that leads out is refused before anything is made under it: a push naming new directories
/// past it leaves the outside untouched, not even an empty directory.
#[cfg(unix)]
#[tokio::test]
async fn a_push_creates_no_directory_outside_the_output_directory() {
    let root = fresh_dir("no-dir-out");
    let out = root.join("out");
    let elsewhere = root.join("elsewhere");
    std::fs::create_dir_all(&out).expect("the output directory is creatable");
    std::fs::create_dir_all(&elsewhere).expect("the outside directory is creatable");
    std::os::unix::fs::symlink(&elsewhere, out.join(".linked")).expect("the symlink is creatable");

    let error = push(&out, b".linked/new/deep/settings", b"ESCAPED")
        .await
        .expect_err("a directory that leads out is refused");

    assert!(
        matches!(error, ReceiveError::Escapes { .. }),
        "refused as leading out: {error:?}"
    );
    assert!(
        !elsewhere.join("new").exists(),
        "no directory is created outside the output directory"
    );

    let _ = std::fs::remove_dir_all(&root);
}

/// A symlinked directory is refused even when it leads back inside the output directory: following one
/// at all is what lets a directory swapped for a symlink mid-push carry the file out.
#[cfg(unix)]
#[tokio::test]
async fn a_push_through_a_symlinked_dir_that_stays_inside_is_refused() {
    let out = fresh_dir("symlink-in");
    std::fs::create_dir_all(out.join("real")).expect("the inside directory is creatable");
    std::os::unix::fs::symlink(out.join("real"), out.join("inlink"))
        .expect("the symlink is creatable");

    let error = push(&out, b"inlink/settings", b"VIA LINK")
        .await
        .expect_err("a symlinked directory is refused");

    assert!(
        matches!(error, ReceiveError::Escapes { .. }),
        "refused as passing through a symlink: {error:?}"
    );
    assert!(
        !out.join("real/settings").exists(),
        "nothing lands through the symlink"
    );
    assert_no_temp_left(&out);

    let _ = std::fs::remove_dir_all(&out);
}

/// A push refused for its name makes no directory for it: the refusal comes before the first one is made,
/// so the new directories it named never appear, even inside the output directory.
#[tokio::test]
async fn a_refused_push_makes_no_directory() {
    let out = fresh_dir("refused-no-dir");

    let error = push(&out, b"new/deep/na\0me", b"NAMED")
        .await
        .expect_err("a NUL in the name is refused");

    assert!(
        matches!(error, ReceiveError::Save { .. }),
        "refused as unsaveable: {error:?}"
    );
    assert!(!out.join("new").exists(), "no directory is made for it");
    assert_no_temp_left(&out);

    let _ = std::fs::remove_dir_all(&out);
}

/// A push dropped mid-stream, as a caller's deadline drops a stalled stream, leaves no temp behind. The
/// sender writes the frame's head and then stalls on a source that never ends, so the temp exists when
/// the receive is dropped.
#[tokio::test]
async fn an_abandoned_push_leaves_no_temp() {
    let out = fresh_dir("abandoned");
    let (sender, receiver) = io::duplex(64 * 1024);
    let (sender_read, sender_write) = io::split(sender);
    let (receiver_read, receiver_write) = io::split(receiver);
    // The far end of the source is held open and never written, so the body never arrives.
    let (held, mut endless) = io::duplex(1);

    let blob = Blob::hash(&mut b"payload".as_slice())
        .await
        .expect("the blob hashes");
    let sending = tokio::spawn(async move {
        Transfer::new(sender_write, sender_read)
            .send(b"stalled", &blob, &mut endless)
            .await
    });
    let receiving = {
        let out = out.clone();
        tokio::spawn(async move { receive_file(receiver_write, receiver_read, &out, 0).await })
    };
    assert!(
        eventually(|| temps_in(&out) == 1).await,
        "the receive makes its temp once the head is read"
    );
    receiving.abort();
    let _ = receiving.await;
    sending.abort();
    drop(held);

    assert!(
        eventually(|| temps_in(&out) == 0).await,
        "the dropped receive removes its temp"
    );

    let _ = std::fs::remove_dir_all(&out);
}

/// A stream that says nothing costs the receiver nothing on disk: the output directory is opened and the
/// temp made only once a frame's head is read, so a silent stream leaves the directory empty.
#[tokio::test]
async fn a_silent_stream_spends_no_disk() {
    let out = fresh_dir("silent");
    let (silent, receiver) = io::duplex(64 * 1024);
    let (receiver_read, receiver_write) = io::split(receiver);

    let receiving = {
        let out = out.clone();
        tokio::spawn(async move { receive_file(receiver_write, receiver_read, &out, 0).await })
    };
    // A temp made before the head appears within milliseconds; this waits far longer than that.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let made = std::fs::read_dir(&out)
        .expect("the output directory lists")
        .count();
    receiving.abort();
    let _ = receiving.await;
    drop(silent);

    assert_eq!(
        made, 0,
        "nothing is made for a stream that has said nothing"
    );

    let _ = std::fs::remove_dir_all(&out);
}

/// A name refused from the frame's head is refused before the output directory is even opened: the
/// refusal holds where that directory does not exist, so it spent no descriptor and no disk.
#[tokio::test]
async fn a_refused_name_is_refused_before_the_output_directory_is_opened() {
    let root = fresh_dir("refused-unopened");
    let out = root.join("absent");

    let error = push(&out, b".transfer-0123456789abcdef.part", b"PLANTED")
        .await
        .expect_err("a temp file name is refused");

    assert!(
        matches!(error, ReceiveError::TempName { .. }),
        "refused for its name, not for the directory: {error:?}"
    );

    let _ = std::fs::remove_dir_all(&root);
}

/// A file that grew between the sender's hash and its send never lands: the sender refuses the longer
/// source before the declared body is complete, so the receiver reads a short body, and both ends fail.
#[tokio::test]
async fn a_source_that_grew_after_hashing_never_lands() {
    let out = fresh_dir("grown");

    let (received, sent) =
        exchange_changed(&out, b"log.txt", b"line one\n", b"line one\nline two\n").await;

    assert!(
        matches!(sent, Err(wire::Error::LengthMismatch)),
        "the sender refuses the longer source: {sent:?}"
    );
    assert!(
        matches!(
            received,
            Err(ReceiveError::Transfer(wire::Error::Truncated))
        ),
        "the receiver never saw a whole body: {received:?}"
    );
    assert!(!out.join("log.txt").exists(), "nothing lands");
    assert_no_temp_left(&out);

    let _ = std::fs::remove_dir_all(&out);
}

/// A frame that runs past its declared body is refused and answered no, though its body verifies:
/// the receiver requires the stream to end with the body before it keeps anything.
#[tokio::test]
async fn a_stream_that_runs_past_the_body_never_lands() {
    let out = fresh_dir("overrun");
    let blob = Blob::hash(&mut b"PUSHED".as_slice())
        .await
        .expect("the blob hashes");
    // The real sender writes the frame; one byte after it is what a sender that streamed on would add.
    let mut frame = Vec::new();
    Transfer::new(&mut frame, [1u8].as_slice())
        .send(b"pushed", &blob, &mut b"PUSHED".as_slice())
        .await
        .expect("the frame is written");
    frame.push(b'!');

    let mut answer = Vec::new();
    let error = receive_file(&mut answer, frame.as_slice(), &out, 0)
        .await
        .expect_err("a byte past the body is refused");

    assert!(
        matches!(error, ReceiveError::Transfer(wire::Error::Overrun)),
        "{error:?}"
    );
    assert_eq!(answer, [0], "the sender is answered no");
    assert!(!out.join("pushed").exists(), "nothing lands");
    assert_no_temp_left(&out);

    let _ = std::fs::remove_dir_all(&out);
}

/// An empty file is pushed and lands whole: the sender proves its source empty before the frame, the
/// receiver reads no body, and both ends agree.
#[tokio::test]
async fn an_empty_file_lands() {
    let out = fresh_dir("empty");

    let received = push(&out, b".gitkeep", b"")
        .await
        .expect("an empty file lands");

    assert_eq!(received.bytes, 0);
    assert_eq!(
        std::fs::read(out.join(".gitkeep")).expect("the landed file reads"),
        b""
    );
    assert_no_temp_left(&out);

    let _ = std::fs::remove_dir_all(&out);
}

/// A body of several chunks and one byte more lands whole: the sender's held-back last chunk crosses a
/// chunk boundary, and the receiver reads and checks it over many passes.
#[tokio::test]
async fn a_body_of_many_chunks_lands_whole() {
    let out = fresh_dir("many-chunks");
    let body: Vec<u8> = (0..3 * 64 * 1024 + 1).map(|at| (at % 251) as u8).collect();

    let received = push(&out, b"big.bin", &body)
        .await
        .expect("a multi-chunk body lands");

    assert_eq!(received.bytes, body.len() as u64);
    assert_eq!(
        std::fs::read(out.join("big.bin")).expect("the landed file reads"),
        body
    );
    assert_no_temp_left(&out);

    let _ = std::fs::remove_dir_all(&out);
}

/// A name refused from the frame's head is answered at once, while the body is still being written, so
/// the sender's write breaks. It still reports the refusal, not the broken pipe: the body is larger than
/// the stream's buffer, so the sender cannot have finished writing before the receiver stopped reading.
#[tokio::test]
async fn a_sender_mid_body_hears_a_refusal_from_the_head() {
    let out = fresh_dir("refused-mid-body");
    let body = vec![b'x'; 1024 * 1024];

    let (received, sent) = exchange(&out, b".transfer-0123456789abcdef.part", &body).await;

    assert!(
        matches!(received, Err(ReceiveError::TempName { .. })),
        "{received:?}"
    );
    assert!(
        matches!(sent, Err(wire::Error::Rejected)),
        "the sender hears the refusal: {sent:?}"
    );

    let _ = std::fs::remove_dir_all(&out);
}

/// The landing for a filesystem with no exclusive rename, driven directly so it runs on every filesystem:
/// it lands a new name and refuses an existing file and a symlink, keeping the temp for its caller.
#[cfg(unix)]
#[test]
fn the_reservation_landing_never_replaces_a_file() {
    let out = fresh_dir("reserve");
    let dir = std::fs::File::open(&out).expect("the output directory opens");
    let dir = dir.as_fd();
    std::fs::write(out.join("temp"), b"PUSHED").expect("the temp is writable");
    std::fs::write(out.join("held"), b"ORIGINAL").expect("the held file is writable");
    std::os::unix::fs::symlink(out.join("held"), out.join("link"))
        .expect("the symlink is creatable");

    for existing in ["held", "link"] {
        assert_eq!(
            reserve_then_rename(dir, "temp", dir, existing.as_ref()),
            Err(Errno::EXIST),
            "{existing} is refused as existing"
        );
    }
    assert_eq!(
        std::fs::read(out.join("held")).expect("the held file reads"),
        b"ORIGINAL"
    );
    assert!(
        out.join("temp").exists(),
        "a refusal leaves the temp to its caller"
    );

    reserve_then_rename(dir, "temp", dir, "new".as_ref()).expect("a new name lands");
    assert_eq!(
        std::fs::read(out.join("new")).expect("the landed file reads"),
        b"PUSHED"
    );
    assert!(
        !out.join("temp").exists(),
        "the temp name is gone once landed"
    );

    let _ = std::fs::remove_dir_all(&out);
}

/// A sender cannot name the receiver's temp file pattern, at the top or nested, so it can never aim a
/// landing at another stream's in-flight temp.
#[tokio::test]
async fn a_push_naming_a_temp_is_refused() {
    let out = fresh_dir("temp-name");

    for header in [
        ".transfer-0123456789abcdef.part",
        "dir/.transfer-x.part",
        ".TRANSFER-0123456789ABCDEF.PART",
    ] {
        let error = push(&out, header.as_bytes(), b"PLANTED")
            .await
            .expect_err("a temp file name is refused");
        assert!(
            matches!(error, ReceiveError::TempName { .. }),
            "{header} refused as a temp name: {error:?}"
        );
        assert!(!out.join(header).exists(), "{header} did not land");
    }
    assert_no_temp_left(&out);

    let _ = std::fs::remove_dir_all(&out);
}

/// Two fresh temp names never repeat, even for one tag, and each has the shape the refusal matches.
#[test]
fn temp_names_are_fresh_and_match_their_pattern() {
    let first = TempName::fresh(7).0;
    let second = TempName::fresh(7).0;
    assert_ne!(first, second, "one tag still yields two names");
    assert!(TempName::matches(first.as_ref()));
    assert!(TempName::matches(first.to_ascii_uppercase().as_ref()));
    assert!(!TempName::matches("settings".as_ref()));
}

/// A fresh, empty directory under the system temp dir, unique to this process and test.
fn fresh_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("transfer-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("the test directory is creatable");
    dir
}

/// Push `payload` under `header` through the real wire sender into the real `receive_file`, the way
/// `Recv::serve` calls it once the gate admits a stream, and hold the two ends to one verdict: a push the
/// receiver saved is a success at the sender, and every push it refused is a failure there. So each
/// refusal test below also proves the sender was told.
async fn push(out: &Path, header: &[u8], payload: &[u8]) -> Result<Received, ReceiveError> {
    let (received, sent) = exchange(out, header, payload).await;
    assert_eq!(
        received.is_ok(),
        sent.is_ok(),
        "the sender's verdict matches the receiver's: received {received:?}, sent {sent:?}"
    );
    received
}

/// One push, with what each end concluded: the receiver's result and the sender's.
async fn exchange(
    out: &Path,
    header: &[u8],
    payload: &[u8],
) -> (Result<Received, ReceiveError>, Result<(), wire::Error>) {
    exchange_changed(out, header, payload, payload).await
}

/// One push whose source held `hashed` when the sender hashed it and `sent` when it streamed it, as a
/// file written to between the two reads does.
async fn exchange_changed(
    out: &Path,
    header: &[u8],
    hashed: &[u8],
    sent: &[u8],
) -> (Result<Received, ReceiveError>, Result<(), wire::Error>) {
    let (sender, receiver) = io::duplex(64 * 1024);
    let (sender_read, sender_write) = io::split(sender);
    let (receiver_read, receiver_write) = io::split(receiver);

    let header = header.to_vec();
    let (hashed, sent) = (hashed.to_vec(), sent.to_vec());
    let sending = tokio::spawn(async move {
        let blob = Blob::hash(&mut hashed.as_slice())
            .await
            .expect("the blob hashes");
        let mut source = sent.as_slice();
        Transfer::new(sender_write, sender_read)
            .send(&header, &blob, &mut source)
            .await
    });

    let received = receive_file(receiver_write, receiver_read, out, 0).await;
    let sent = sending.await.expect("the sender task completes");
    (received, sent)
}

/// No temp file is left under `out` at its top level, where every temp is made.
fn assert_no_temp_left(out: &Path) {
    assert_eq!(temps_in(out), 0, "no temp file is left");
}

/// How many temp files sit at `out`'s top level.
fn temps_in(out: &Path) -> usize {
    std::fs::read_dir(out)
        .expect("the output directory lists")
        .filter_map(Result::ok)
        .filter(|entry| TempName::matches(&entry.file_name()))
        .count()
}

/// Whether `condition` holds within five seconds, checked every 10 ms: the receive's filesystem steps run
/// on the blocking pool, so a test waits for their effect instead of racing it.
async fn eventually(condition: impl Fn() -> bool) -> bool {
    for _ in 0..500 {
        if condition() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    false
}

/// A peer-supplied filename cannot forge a log line or drive a terminal: the newline, escape byte,
/// carriage return, C1 control, bidi override, and zero-width space in these names all render escaped,
/// and the printable remainder survives.
#[test]
fn a_hostile_filename_renders_escaped() {
    let rendered = render_path(Path::new("evil\nname\u{1b}[31m\r.txt"));
    assert_eq!(
        rendered, r"evil\nname\u{1b}[31m\r.txt",
        "a newline, an escape byte, and a carriage return never reach the line raw"
    );
    assert_eq!(
        render_path(Path::new("a\u{85}b\u{202e}c\u{200b}d")),
        r"a\u{85}b\u{202e}c\u{200b}d",
        "a C1 control, a bidi override, and a zero-width space never reach the line raw"
    );
}

/// A peer-supplied name cannot flood the line: the render caps at [`MAX_RENDERED_PATH`] characters and
/// marks the cut.
#[test]
fn a_long_filename_renders_capped() {
    let long = "a".repeat(MAX_RENDERED_PATH * 4);
    let rendered = render_path(Path::new(&long));
    assert_eq!(
        rendered,
        format!("{}...", "a".repeat(MAX_RENDERED_PATH)),
        "the render holds the cap and marks the cut"
    );
}

/// The cap cuts between escapes, never inside one: 255 printable characters fill all but the last slot,
/// the 6-character ESC escape does not fit whole, so the render backs off to the marker instead of
/// emitting a malformed half-escape.
#[test]
fn a_cap_cut_never_splits_an_escape_sequence() {
    let name = format!("{}\u{1b}", "a".repeat(MAX_RENDERED_PATH - 1));
    let rendered = render_path(Path::new(&name));
    assert_eq!(
        rendered,
        format!("{}...", "a".repeat(MAX_RENDERED_PATH - 1)),
        "the cut lands before the incomplete escape"
    );
}

/// What `escape_debug` leaves alone and what it escapes: a precomposed non-ASCII letter is printable and
/// passes raw, while a grapheme-extended mark (a combining accent) is escaped.
#[test]
fn non_ascii_renders_like_escape_debug() {
    assert_eq!(
        render_path(Path::new("caf\u{e9}")),
        "caf\u{e9}",
        "a printable non-ASCII letter passes raw"
    );
    assert_eq!(
        render_path(Path::new("e\u{301}")),
        r"e\u{301}",
        "a combining mark is grapheme-extended and renders escaped"
    );
    assert_eq!(
        render_path(Path::new("a\u{fffd}b")),
        "a\u{fffd}b",
        "a byte `from_utf8_lossy` already replaced passes raw"
    );
}

/// Letters that print as blank space render as escapes, so a path made of them is never invisible in an
/// error's text. `escape_debug` alone passes them raw.
#[test]
fn blank_letters_render_escaped() {
    assert_eq!(
        render_path(Path::new("\u{115f}\u{1160}\u{3164}\u{ffa0}\u{2800}")),
        r"\u{115f}\u{1160}\u{3164}\u{ffa0}\u{2800}",
        "a name of blank letters renders visible"
    );
}
