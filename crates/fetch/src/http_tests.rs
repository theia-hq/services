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

/// The encoder accepts a header list its own decoder refuses, which is
/// [known gap 2](../PROTOCOL.md) and is pinned here so closing it turns the document red rather than
/// letting the gap section quietly go stale. A conformant writer caps at `MAX_HEADERS`; this one caps
/// only at the `u16` the count field can express.
#[tokio::test]
async fn the_encoder_writes_a_header_list_its_own_decoder_refuses() {
    let headers = (0..=crate::http::MAX_HEADERS)
        .map(|n| (format!("x-{n}"), "1".to_owned()))
        .collect();
    let request = FetchRequest {
        method: "GET".to_owned(),
        url: "https://example.com/".to_owned(),
        headers,
    };
    let mut buf = Vec::new();
    request
        .write(&mut buf)
        .await
        .expect("the encoder does not enforce the reader's header cap");
    assert!(
        FetchRequest::read(&mut buf.as_slice()).await.is_err(),
        "the header cap has been moved onto the writer; the known gap in PROTOCOL.md must be struck"
    );
}

/// The wire vectors `PROTOCOL.md` publishes.
///
/// Every octet in that document is produced HERE, by this crate's own codec, and the document is read
/// back and compared. A specification whose vectors are typed by hand is a second implementation nobody
/// runs; these cannot drift, because the wire moving turns the drift into a failing test.
///
/// To regenerate after a wire change, run the module and paste the printed blocks over the ones in the
/// document:
///
/// ```text
/// cargo test -p fetch --lib http_tests::vectors -- --nocapture
/// ```
mod vectors {
    use core::net::{IpAddr, Ipv4Addr};
    use std::collections::BTreeMap;

    use crate::http::{FetchRequest, FetchResponse};
    use crate::serve::FetchError;

    /// The document under test, compiled in, so `cargo test` and the published specification cannot be
    /// two different files.
    const PROTOCOL_MD: &str = include_str!("../PROTOCOL.md");

    /// One published vector: the label the document files it under, and the octets that belong under it.
    struct Vector {
        name: &'static str,
        bytes: Vec<u8>,
    }

    /// Encode a request and prove the reader takes those exact octets back to the same value. A vector is
    /// only a vector when both halves of the codec agree on it, so the round trip is part of building one
    /// rather than a separate test that could be forgotten.
    async fn request_vector(name: &'static str, request: FetchRequest) -> Vector {
        let mut bytes = Vec::new();
        request.write(&mut bytes).await.expect("a Vec never fails");
        assert_eq!(
            FetchRequest::read(&mut bytes.as_slice())
                .await
                .expect("read"),
            request,
            "{name} does not read back"
        );
        Vector { name, bytes }
    }

    /// The same for a response frame.
    async fn response_vector(name: &'static str, response: FetchResponse) -> Vector {
        let mut bytes = Vec::new();
        response.write(&mut bytes).await.expect("a Vec never fails");
        assert_eq!(
            FetchResponse::read(&mut bytes.as_slice())
                .await
                .expect("read"),
            response,
            "{name} does not read back"
        );
        Vector { name, bytes }
    }

    /// A refusal vector built from the CAUSE rather than from a typed-out sentence: the responder renders
    /// exactly this text into the error frame, so a reworded cause moves the vector and the document with
    /// it.
    async fn refusal_vector(name: &'static str, cause: FetchError) -> Vector {
        response_vector(name, FetchResponse::Error(cause.to_string())).await
    }

    /// A peer's opening octets, built by writing a well-formed GET request and overwriting the four magic
    /// bytes with `magic`. Built rather than typed so the body after the magic is exactly the body the
    /// request vector carries, leaving the magic as the only difference under test.
    async fn head_vector(name: &'static str, magic: &[u8; 4]) -> Vector {
        let mut bytes = Vec::new();
        get().write(&mut bytes).await.expect("a Vec never fails");
        bytes[..4].copy_from_slice(magic);
        Vector { name, bytes }
    }

    /// The request every head vector carries after its magic, and the ranged-GET vector in its own right.
    fn get() -> FetchRequest {
        FetchRequest {
            method: "GET".to_owned(),
            url: "https://example.com/big.iso".to_owned(),
            headers: vec![("Range".to_owned(), "bytes=0-1023".to_owned())],
        }
    }

    /// The frame a host writes back to `head`, which is an answer exactly when the head named this
    /// protocol's identity and a version the host does not serve, and nothing at all otherwise.
    async fn answer_to(mut head: &[u8]) -> Option<FetchResponse> {
        FetchRequest::read(&mut head)
            .await
            .expect_err("every head here is one this build cannot read")
            .answer()
    }

    /// Every vector the document publishes, in document order.
    async fn published() -> Vec<Vector> {
        let mismatch = head_vector("tbh1-head-version-mismatch", b"TBH2").await;
        let foreign = head_vector("tbh1-head-foreign", b"SSH-").await;
        let neighbour = head_vector("tbh1-head-neighbouring-identity", b"TB04").await;
        assert!(
            answer_to(&foreign.bytes).await.is_none(),
            "a foreign identity gets no octets back"
        );
        assert!(
            answer_to(&neighbour.bytes).await.is_none(),
            "a neighbouring identity is a foreign wire, so it gets no octets back either"
        );
        let mismatch_answer = answer_to(&mismatch.bytes)
            .await
            .expect("a served identity on an unserved version is answered");
        vec![
            request_vector("tbh1-request-get", get()).await,
            request_vector(
                "tbh1-request-head",
                FetchRequest {
                    method: "HEAD".to_owned(),
                    url: "https://example.com/big.iso".to_owned(),
                    headers: Vec::new(),
                },
            )
            .await,
            response_vector(
                "tbh1-response-ok",
                FetchResponse::Ok {
                    status: 206,
                    headers: vec![
                        ("content-range".to_owned(), "bytes 0-1023/4096".to_owned()),
                        ("accept-ranges".to_owned(), "bytes".to_owned()),
                    ],
                },
            )
            .await,
            refusal_vector(
                "tbh1-response-error-method",
                FetchError::Method("POST".to_owned()),
            )
            .await,
            refusal_vector(
                "tbh1-response-error-non-public",
                FetchError::NonPublic {
                    host: "metadata.example".to_owned(),
                    ip: IpAddr::V4(Ipv4Addr::new(169, 254, 169, 254)),
                },
            )
            .await,
            mismatch,
            response_vector("tbh1-response-version-mismatch", mismatch_answer).await,
            foreign,
            neighbour,
        ]
    }

    /// The octets as one unbroken lowercase hex string: the form the document is compared in, so the
    /// grouping and line breaks a reader sees are presentation and nothing more.
    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    /// The octets as the document shows them: lowercase pairs, sixteen to a line, so a regenerated block
    /// pastes in unedited.
    fn octet_lines(bytes: &[u8]) -> String {
        bytes
            .chunks(16)
            .map(|chunk| {
                chunk
                    .iter()
                    .map(|byte| format!("{byte:02x}"))
                    .collect::<Vec<_>>()
                    .join(" ")
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// The vectors the document publishes, parsed out of it: a block opens with a `vector <name>` line,
    /// and the octet lines under it, to the next blank line or fence, are the frame. Reading the document
    /// rather than restating it is what makes a divergence a test failure instead of a discovery.
    fn documented() -> BTreeMap<String, String> {
        let mut found = BTreeMap::new();
        let mut lines = PROTOCOL_MD.lines();
        while let Some(line) = lines.next() {
            let Some(name) = line.trim().strip_prefix("vector ") else {
                continue;
            };
            let mut octets = String::new();
            for line in lines.by_ref() {
                let line = line.trim();
                if line.is_empty() || line.starts_with("```") {
                    break;
                }
                octets.extend(line.chars().filter(|char| !char.is_whitespace()));
            }
            assert!(
                found.insert(name.trim().to_owned(), octets).is_none(),
                "{name} is published twice"
            );
        }
        found
    }

    /// Every octet the document publishes is an octet this codec writes, and every vector it names is one
    /// the codec still produces. Both directions: a stale vector and an orphaned one are the same defect.
    #[tokio::test]
    async fn the_document_publishes_exactly_what_this_codec_writes() {
        let mut documented = documented();
        let mut wrong = Vec::new();
        for Vector { name, bytes } in published().await {
            // Printed unconditionally: this is the regeneration output, and a run with --nocapture is
            // how the document is rewritten after the wire moves.
            println!("vector {name}\n{}\n", octet_lines(&bytes));
            let expected = hex(&bytes);
            match documented.remove(name) {
                Some(published) if published == expected => {}
                Some(published) => {
                    wrong.push(format!("{name}: published {published}, wire {expected}"))
                }
                None => wrong.push(format!("{name}: not published; wire {expected}")),
            }
        }
        for orphan in documented.keys() {
            wrong.push(format!(
                "{orphan}: published, but this codec writes no such frame"
            ));
        }
        assert!(
            wrong.is_empty(),
            "PROTOCOL.md is out of date:\n{}",
            wrong.join("\n")
        );
    }
}
