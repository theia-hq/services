use crate::http::{FetchRequest, FetchResponse, RequestReadError};

#[tokio::test]
async fn request_roundtrips_with_headers_and_query() {
    let request = FetchRequest {
        method: "GET".to_string(),
        url: "https://example.com/big.iso?token=abc".to_string(),
        headers: vec![
            ("Range".to_string(), "bytes=0-1023".to_string()),
            ("Accept".to_string(), "*/*".to_string()),
        ],
    };
    let mut buf = Vec::new();
    request.write(&mut buf).await.expect("write");
    let mut slice: &[u8] = &buf;
    let read = FetchRequest::read(&mut slice).await.expect("read");
    assert_eq!(read, request);
}

#[tokio::test]
async fn response_ok_roundtrips_and_carries_range_status() {
    let response = FetchResponse::Ok {
        status: 206,
        headers: vec![
            ("Content-Range".to_string(), "bytes 0-1023/4096".to_string()),
            ("Accept-Ranges".to_string(), "bytes".to_string()),
        ],
    };
    let mut buf = Vec::new();
    response.write(&mut buf).await.expect("write");
    let mut slice: &[u8] = &buf;
    let read = FetchResponse::read(&mut slice).await.expect("read");
    assert_eq!(read, response);
}

#[tokio::test]
async fn response_error_roundtrips() {
    let response = FetchResponse::Error("origin unreachable".to_string());
    let mut buf = Vec::new();
    response.write(&mut buf).await.expect("write");
    let mut slice: &[u8] = &buf;
    let read = FetchResponse::read(&mut slice).await.expect("read");
    assert_eq!(read, response);
}

/// One well-formed request frame with the byte at `at` replaced. The two tests below differ only in
/// WHICH half of the magic they corrupt, because that single difference is the whole claim.
async fn frame_with(at: usize, byte: u8) -> Vec<u8> {
    let mut buf = Vec::new();
    FetchRequest {
        method: "GET".to_string(),
        url: "https://example.com/".to_string(),
        headers: Vec::new(),
    }
    .write(&mut buf)
    .await
    .expect("a request frame fits a vec");
    buf[at] = byte;
    buf
}

/// A stream whose IDENTITY is not ours is not a fetch stream, and it is told nothing: we cannot know
/// what would even be meaningful to whatever is on the other end. Make the version answer fire for a
/// foreign identity too and the first assertion goes red.
#[tokio::test]
async fn a_foreign_stream_is_rejected() {
    // `XBH1`: one byte of the identity changed, and nothing else.
    let buf = frame_with(0, b'X').await;

    let error = FetchRequest::read(&mut buf.as_slice())
        .await
        .expect_err("a foreign identity is not a fetch stream");
    assert!(
        error.answer().is_none(),
        "a foreign stream gets no wire answer, only a host log line"
    );
    assert!(matches!(error, RequestReadError::Foreign));
}

/// The version half of the magic is PARSED, so a fetch peer on another build is a distinguishable
/// condition with a wire answer, not a foreign stream. Revert the parse to a four-byte comparison and
/// this goes red at the first assertion.
#[tokio::test]
async fn a_version_mismatch_is_not_a_foreign_stream() {
    // `TBH2`: one byte of the version changed, and nothing else.
    let buf = frame_with(3, b'2').await;

    let error = FetchRequest::read(&mut buf.as_slice())
        .await
        .expect_err("TBH2 is not this build's grammar");
    assert!(
        !matches!(error, RequestReadError::Foreign),
        "a fetch peer on another build is not a foreign protocol"
    );
    let Some(FetchResponse::Error(message)) = error.answer() else {
        panic!("a version mismatch is answerable on the wire: {error}");
    };
    // Both versions, so the requester learns what it speaks AND what the host speaks; one of them
    // alone leaves it guessing at the other.
    assert!(message.contains("TBH2"), "{message}");
    assert!(message.contains("TBH1"), "{message}");
}

/// The response frame's tag is frozen, so it does not move when the request grammar does: a reply
/// written by a host on another request version is still readable, which is what carries the version
/// answer home. Derive the response tag from the request's own magic and this goes red the day the
/// version bumps, which is exactly the day it would matter.
#[tokio::test]
async fn the_response_tag_is_frozen_against_the_request_version() {
    let mut buf = Vec::new();
    FetchResponse::Error("origin unreachable".to_string())
        .write(&mut buf)
        .await
        .expect("a response frame fits a vec");
    assert_eq!(&buf[..4], b"TBH1");
}
