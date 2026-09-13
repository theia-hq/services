use std::path::Path;

use super::{MAX_RENDERED_PATH, render_path};

/// A peer-supplied filename cannot forge a log line or drive a terminal: the newline, escape byte, and
/// carriage return in this name all render escaped, and the printable remainder survives.
#[test]
fn a_hostile_filename_renders_escaped() {
    let rendered = render_path(Path::new("evil\nname\u{1b}[31m\r.txt"));
    assert_eq!(
        rendered, r"evil\nname\u{1b}[31m\r.txt",
        "a newline, an escape byte, and a carriage return never reach the event raw"
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
