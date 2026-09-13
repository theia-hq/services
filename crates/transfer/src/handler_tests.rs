use std::path::Path;

use super::{MAX_RENDERED_PATH, render_path};

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
