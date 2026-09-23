use bifrost::{RefusalDetail, RefusalDetailError};

use super::{MethodRefusal, ProtocolError, Request, Response};

#[tokio::test]
async fn request_variants_roundtrip() {
    let requests = [
        Request::Ping {
            seq: 7,
            sent_unix_nanos: 1_234_567_890,
        },
        Request::SpeedSink {
            limit_bytes: 8 * 1024 * 1024,
        },
        Request::SpeedSource {
            limit_bytes: Some(4 * 1024 * 1024),
        },
        // The unbounded (time-bounded download) source, encoded via the sentinel.
        Request::SpeedSource { limit_bytes: None },
        Request::SpeedBidir {
            limit_bytes: Some(2 * 1024 * 1024),
        },
        // The unbounded (time-bounded) bidir, encoded via the same sentinel.
        Request::SpeedBidir { limit_bytes: None },
    ];
    for request in requests {
        let mut buf = Vec::new();
        request.write(&mut buf).await.unwrap();
        let decoded = Request::read(&mut buf.as_slice()).await.unwrap();
        assert_eq!(decoded, request);
    }
}

#[tokio::test]
async fn response_variants_roundtrip() {
    let responses = [
        Response::Pong {
            seq: 3,
            sent_unix_nanos: 42,
        },
        Response::Received { bytes: 1024 },
        // The download go-ahead and the typed refusal frame: both must survive the wire so a client can
        // tell "here comes the payload" and "this method is refused" from each other and from a raw read.
        // Every method-refusal code is exercised, so a new code cannot reuse a tag unnoticed.
        Response::Sourcing,
        Response::Unsupported {
            code: MethodRefusal::WrongMethod,
            detail: RefusalDetail::bounded("this node serves ping, not speed"),
        },
        Response::Unsupported {
            code: MethodRefusal::RateLimited,
            detail: RefusalDetail::bounded("ping rate limited for this caller"),
        },
        Response::Unsupported {
            code: MethodRefusal::Busy,
            detail: RefusalDetail::bounded("a transfer slot is busy"),
        },
    ];
    for response in &responses {
        let mut buf = Vec::new();
        response.write(&mut buf).await.unwrap();
        let decoded = Response::read(&mut buf.as_slice()).await.unwrap();
        assert_eq!(&decoded, response);
    }
}

/// One well-formed ping frame with the byte at `at` replaced. The two tests below differ only in WHICH
/// half of the magic they corrupt, because that single difference is the whole claim.
async fn frame_with(at: usize, byte: u8) -> Vec<u8> {
    let mut buf = Vec::new();
    Request::Ping {
        seq: 1,
        sent_unix_nanos: 2,
    }
    .write(&mut buf)
    .await
    .expect("a request frame fits a vec");
    buf[at] = byte;
    buf
}

/// A stream whose IDENTITY is not ours is not a measure stream, and it is told nothing: we cannot know
/// what would even be meaningful to whatever is on the other end. Make the version answer fire for a
/// foreign identity too and the first assertion goes red.
#[tokio::test]
async fn rejects_foreign_stream() {
    // `XG02`: one byte of the identity changed, and nothing else.
    let buf = frame_with(0, b'X').await;

    let error = Request::read(&mut buf.as_slice())
        .await
        .expect_err("a foreign identity is not a measure stream");
    assert!(
        error.answer().is_none(),
        "a foreign stream gets no wire answer, only a host log line"
    );
    assert!(matches!(error, ProtocolError::Foreign));
}

/// The version half of the magic is PARSED, so a measure peer on another build is a distinguishable
/// condition with a wire answer, not a foreign stream. Revert the parse to a four-byte comparison and
/// this goes red at the first assertion.
#[tokio::test]
async fn a_version_mismatch_is_not_a_foreign_stream() {
    // `DG03`: one byte of the version changed, and nothing else.
    let buf = frame_with(3, b'3').await;

    let error = Request::read(&mut buf.as_slice())
        .await
        .expect_err("DG03 is not this build's grammar");
    assert!(
        !matches!(error, ProtocolError::Foreign),
        "a measure peer on another build is not a foreign protocol"
    );
    let Some(Response::Unsupported { code, detail }) = error.answer() else {
        panic!("a version mismatch is answerable on the wire: {error}");
    };
    // A code every shipped build already decodes, so the peer this answer is for can read it at all.
    assert_eq!(code, MethodRefusal::WrongMethod);
    // Both versions, so the dialer learns what it speaks AND what the host speaks; one of them alone
    // leaves them guessing at the other.
    assert!(detail.as_str().contains("DG03"), "{detail}");
    assert!(detail.as_str().contains("DG02"), "{detail}");
}

#[tokio::test]
async fn rejects_unknown_request_tag() {
    let mut buf = b"DG02\x7f".as_slice();
    assert!(matches!(
        Request::read(&mut buf).await,
        Err(ProtocolError::UnknownRequest(0x7f))
    ));
}

#[tokio::test]
async fn rejects_an_unknown_refusal_code() {
    // An unsupported response whose code byte selects nothing we know: rejected as itself, never
    // misread as a known method.
    let buf = [super::resp_tag::UNSUPPORTED, super::refusal_tag::BUSY + 1];
    assert!(matches!(
        Response::read(&mut buf.as_slice()).await,
        Err(ProtocolError::UnknownRefusalCode(_))
    ));
}

#[tokio::test]
async fn rejects_an_over_cap_detail_claim_before_allocating() {
    // A hand-written frame claiming one byte past the cap: the reader rejects the claim without reading
    // (or allocating) a body at all, so a hostile length can never make the client allocate on demand.
    let mut buf = vec![
        super::resp_tag::UNSUPPORTED,
        super::refusal_tag::WRONG_METHOD,
    ];
    buf.extend_from_slice(&(RefusalDetail::MAX_LEN as u32 + 1).to_be_bytes());
    assert!(matches!(
        Response::read(&mut buf.as_slice()).await,
        Err(ProtocolError::BadDetail(RefusalDetailError::TooLong(_)))
    ));
}

#[tokio::test]
async fn rejects_a_detail_that_is_not_utf8() {
    // The bytes are in-cap but not UTF-8: a corrupt frame is rejected, never repaired with replacement
    // characters the way a lossy decode would.
    let mut buf = vec![
        super::resp_tag::UNSUPPORTED,
        super::refusal_tag::WRONG_METHOD,
    ];
    let invalid = [0xffu8, 0xfe];
    buf.extend_from_slice(&(invalid.len() as u32).to_be_bytes());
    buf.extend_from_slice(&invalid);
    assert!(matches!(
        Response::read(&mut buf.as_slice()).await,
        Err(ProtocolError::BadDetail(RefusalDetailError::NotUtf8))
    ));
}

/// A session failure that is not a refusal becomes the stream variant, and its message must not name an
/// operation it did not perform. A client renders this error chained over its cause, so an outer half
/// reading `read frame` describes a read to someone whose stream never opened.
#[test]
fn a_stream_failure_does_not_claim_a_read() {
    let error = ProtocolError::from(bifrost::Error::Stream("peer went away".into()));
    let rendered = error.to_string();

    assert!(
        !rendered.contains("read"),
        "the stream variant covers open, read, and close alike: {rendered}"
    );
    let mut chain = rendered.clone();
    let mut next = core::error::Error::source(&error);
    while let Some(cause) = next {
        chain.push_str(": ");
        chain.push_str(&cause.to_string());
        next = cause.source();
    }
    assert_eq!(
        chain, "stream: peer went away",
        "a client chaining the causes reads the failure and its detail, and nothing invented"
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
/// cargo test -p measure --lib protocol::protocol_tests::vectors -- --nocapture
/// ```
mod vectors {
    use std::collections::BTreeMap;

    use bifrost::RefusalDetail;

    use crate::protocol::{MethodRefusal, ProtocolError, Request, Response};

    /// The document under test, compiled in, so `cargo test` and the published specification cannot be
    /// two different files.
    const PROTOCOL_MD: &str = include_str!("../PROTOCOL.md");

    /// The client-chosen nonce every ping vector carries. An opaque `u64` the responder echoes
    /// untouched; a plausible unix-nanos stamp rather than a round number, so all eight octets differ
    /// and a reimplementer reading the block cannot mistake a padded field for a short one.
    const NONCE: u64 = 1_726_000_000_000_000_000;

    /// One published vector: the label the document files it under, and the octets that belong under it.
    struct Vector {
        name: &'static str,
        bytes: Vec<u8>,
    }

    /// Encode a request and prove the reader takes those exact octets back to the same value. A vector is
    /// only a vector when both halves of the codec agree on it, so the round trip is part of building one
    /// rather than a separate test that could be forgotten.
    async fn request_vector(name: &'static str, request: Request) -> Vector {
        let mut bytes = Vec::new();
        request.write(&mut bytes).await.expect("a Vec never fails");
        assert_eq!(
            Request::read(&mut bytes.as_slice()).await.unwrap(),
            request,
            "{name} does not read back"
        );
        Vector { name, bytes }
    }

    /// The same for a response frame.
    async fn response_vector(name: &'static str, response: Response) -> Vector {
        let mut bytes = Vec::new();
        response.write(&mut bytes).await.expect("a Vec never fails");
        assert_eq!(
            Response::read(&mut bytes.as_slice()).await.unwrap(),
            response,
            "{name} does not read back"
        );
        Vector { name, bytes }
    }

    /// A peer's opening octets, built by writing a well-formed ping request and overwriting the four
    /// magic bytes with `magic`. Built rather than typed so the body after the magic is exactly the body
    /// the ping vector carries, leaving the magic as the only difference under test.
    async fn head_vector(name: &'static str, magic: &[u8; 4]) -> Vector {
        let mut bytes = Vec::new();
        ping().write(&mut bytes).await.expect("a Vec never fails");
        bytes[..4].copy_from_slice(magic);
        Vector { name, bytes }
    }

    /// The request every head vector carries after its magic, and the ping vector in its own right.
    fn ping() -> Request {
        Request::Ping {
            seq: 7,
            sent_unix_nanos: NONCE,
        }
    }

    /// The frame a responder writes back to `head`, which is an answer exactly when the head named this
    /// protocol's identity and a version the responder does not serve, and nothing at all otherwise.
    async fn answer_to(mut head: &[u8]) -> Option<Response> {
        Request::read(&mut head)
            .await
            .expect_err("every head here is one this build cannot read")
            .answer()
    }

    /// Every vector the document publishes, in document order.
    async fn published() -> Vec<Vector> {
        let mismatch = head_vector("dg02-head-version-mismatch", b"DG03").await;
        let foreign = head_vector("dg02-head-foreign", b"SSH-").await;
        let longer = head_vector("dg02-head-longer-identity", b"DGX1").await;
        assert!(
            answer_to(&foreign.bytes).await.is_none(),
            "a foreign identity gets no octets back"
        );
        // A head naming a LONGER identity that merely opens with `DG` is a different wire, not this
        // one at an unserved version, and it gets the silence any foreign wire gets. The run-end
        // check in `WireVersion::read` is the only thing holding that, since two of the family's
        // identities already share a prefix; reverting it turns this red and the reader starts
        // handing this host's version to protocols it does not speak.
        assert!(
            answer_to(&longer.bytes).await.is_none(),
            "a longer identity that opens with ours is foreign, and gets no octets either"
        );
        let mismatch_answer = answer_to(&mismatch.bytes)
            .await
            .expect("a served identity on an unserved version is answered");
        vec![
            request_vector("dg02-request-ping", ping()).await,
            request_vector(
                "dg02-request-speed-sink",
                Request::SpeedSink {
                    limit_bytes: 8 * 1024 * 1024,
                },
            )
            .await,
            request_vector(
                "dg02-request-speed-sink-no-exact-count",
                Request::SpeedSink {
                    limit_bytes: crate::protocol::UNBOUNDED,
                },
            )
            .await,
            request_vector(
                "dg02-request-speed-source",
                Request::SpeedSource {
                    limit_bytes: Some(4 * 1024 * 1024),
                },
            )
            .await,
            request_vector(
                "dg02-request-speed-source-unbounded",
                Request::SpeedSource { limit_bytes: None },
            )
            .await,
            request_vector(
                "dg02-request-speed-bidir",
                Request::SpeedBidir {
                    limit_bytes: Some(2 * 1024 * 1024),
                },
            )
            .await,
            response_vector(
                "dg02-response-pong",
                Response::Pong {
                    seq: 7,
                    sent_unix_nanos: NONCE,
                },
            )
            .await,
            response_vector(
                "dg02-response-received",
                Response::Received {
                    bytes: 8 * 1024 * 1024,
                },
            )
            .await,
            response_vector("dg02-response-sourcing", Response::Sourcing).await,
            // The three refusal details are the responder's own prose, restated here because each lives
            // as a literal at the site that writes it. The vector fixes the FRAMING; a client never
            // parses the text.
            response_vector(
                "dg02-response-unsupported-wrong-method",
                Response::Unsupported {
                    code: MethodRefusal::WrongMethod,
                    detail: RefusalDetail::bounded("this node serves ping, not speed"),
                },
            )
            .await,
            response_vector(
                "dg02-response-unsupported-rate-limited",
                Response::Unsupported {
                    code: MethodRefusal::RateLimited,
                    detail: RefusalDetail::bounded(
                        "speed request is over the byte cap, request fewer bytes",
                    ),
                },
            )
            .await,
            response_vector(
                "dg02-response-unsupported-busy",
                Response::Unsupported {
                    code: MethodRefusal::Busy,
                    detail: RefusalDetail::bounded("speed busy, try again shortly"),
                },
            )
            .await,
            mismatch,
            response_vector("dg02-response-version-mismatch", mismatch_answer).await,
            foreign,
            longer,
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

    /// `ProtocolError` is only here so the answer helper can name the error it expects; the assertion
    /// that it is the version arm keeps the head vectors honest about which condition they provoke.
    #[tokio::test]
    async fn the_version_head_provokes_the_version_condition() {
        let mut head = Vec::new();
        ping().write(&mut head).await.expect("a Vec never fails");
        head[..4].copy_from_slice(b"DG03");
        assert!(matches!(
            Request::read(&mut head.as_slice()).await,
            Err(ProtocolError::Version { .. })
        ));
    }
}
