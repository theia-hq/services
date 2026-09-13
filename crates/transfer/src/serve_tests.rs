use bifrost::wire::{Blob, Transfer};
use tokio::io;

use super::{receive_file, safe_relative_path};

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
