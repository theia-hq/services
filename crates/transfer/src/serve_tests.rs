use std::path::{Path, PathBuf};

use bifrost::wire::{Blob, Transfer};
use tokio::io;

use super::{
    MAX_RENDERED_PATH, ReceiveError, Received, TempName, receive_file, render_path,
    safe_relative_path,
};

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
/// and acks first; a directory at the destination then refuses the landing.
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
/// keeps its bytes, and the verified temp is removed.
#[tokio::test]
async fn a_push_never_replaces_a_file() {
    let out = fresh_dir("no-replace");
    let held = out.join(".config/app/settings");
    std::fs::create_dir_all(out.join(".config/app")).expect("the held directory is creatable");
    std::fs::write(&held, b"ORIGINAL").expect("the held file is writable");

    let error = push(&out, b".config/app/settings", b"REPLACED")
        .await
        .expect_err("an existing name is refused");

    assert!(
        matches!(error, ReceiveError::Exists { .. }),
        "refused as existing: {error:?}"
    );
    assert_eq!(
        std::fs::read(&held).expect("the held file reads"),
        b"ORIGINAL",
        "the held file keeps its bytes"
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

/// A sender cannot name the receiver's temp file pattern, at the top or nested, so it can never aim a
/// landing at another stream's in-flight temp.
#[tokio::test]
async fn a_push_naming_a_temp_is_refused() {
    let out = fresh_dir("temp-name");

    for header in [".transfer-0123456789abcdef.part", "dir/.transfer-x.part"] {
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
/// `Recv::serve` calls it once the gate admits a stream.
async fn push(out: &Path, header: &[u8], payload: &[u8]) -> Result<Received, ReceiveError> {
    let (sender, receiver) = io::duplex(64 * 1024);
    let (sender_read, sender_write) = io::split(sender);
    let (receiver_read, receiver_write) = io::split(receiver);

    let header = header.to_vec();
    let payload = payload.to_vec();
    let sending = tokio::spawn(async move {
        let mut source = payload.as_slice();
        let blob = Blob::hash(&mut source).await.expect("the blob hashes");
        let mut source = payload.as_slice();
        Transfer::new(sender_write, sender_read)
            .send(&header, &blob, &mut source)
            .await
    });

    let received = receive_file(receiver_write, receiver_read, out, 0).await;
    assert!(
        sending.await.expect("the sender task completes").is_ok(),
        "the blob verifies and acks before the landing is attempted"
    );
    received
}

/// No temp file is left under `out` at its top level, where every temp is made.
fn assert_no_temp_left(out: &Path) {
    let left: Vec<_> = std::fs::read_dir(out)
        .expect("the output directory lists")
        .filter_map(Result::ok)
        .filter(|entry| TempName::matches(&entry.file_name()))
        .collect();
    assert!(left.is_empty(), "no temp file is left: {left:?}");
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
