use std::path::Path;

use bifrost::wire::{Blob, Transfer};
use tokio::io;

use super::{MAX_RENDERED_PATH, receive_file, render_path, safe_relative_path};

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
    // than at the output directory itself (which `rename` could not target).
    assert_eq!(safe_relative_path(b""), std::path::Path::new("download"));
    assert_eq!(
        safe_relative_path(b"../.."),
        std::path::Path::new("download")
    );
}

/// A rename failure reports the peer path SAFELY: the error the engine wraps into `ServeError` (and the
/// tunnel logs at warn) carries the escaped/capped form, never the raw control bytes. The blob verifies
/// and acks first; a directory at the destination then forces the rename to fail.
#[tokio::test]
async fn a_rename_failure_reports_the_path_escaped() {
    let sink = std::env::temp_dir().join(format!("transfer-rename-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&sink);
    std::fs::create_dir_all(&sink).expect("the sink directory is creatable");

    let hostile = "evil\nname\u{1b}[31m";
    // The destination path exists as a directory, so moving the verified temp file onto it fails.
    std::fs::create_dir_all(sink.join(hostile)).expect("the blocking directory is creatable");

    let (sender, receiver) = io::duplex(64 * 1024);
    let (sender_read, sender_write) = io::split(sender);
    let (receiver_read, receiver_write) = io::split(receiver);

    let payload = b"payload".to_vec();
    let header = hostile.as_bytes().to_vec();
    let sending = tokio::spawn(async move {
        let mut source = payload.as_slice();
        let blob = Blob::hash(&mut source).await.expect("the blob hashes");
        let mut source = payload.as_slice();
        Transfer::new(sender_write, sender_read)
            .send(&header, &blob, &mut source)
            .await
    });

    let error = receive_file(receiver_write, receiver_read, &sink, 0)
        .await
        .expect_err("a rename onto a directory fails");
    assert!(
        sending.await.expect("the sender task completes").is_ok(),
        "the blob verifies and acks before the rename is attempted"
    );

    let message = format!("{error:#}");
    assert!(
        message.contains(r"evil\nname\u{1b}[31m"),
        "the error renders the hostile path escaped: {message}"
    );
    assert!(
        !message.contains('\n') && !message.contains('\u{1b}'),
        "no raw control byte rides the error: {message:?}"
    );

    let _ = std::fs::remove_dir_all(&sink);
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
