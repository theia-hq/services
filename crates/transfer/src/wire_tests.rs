use tokio::io;

use super::{Blob, Error, MAX_HEADER_LEN, Transfer};

/// The four magic bytes a well-formed frame opens with, spelled out rather than imported, so a test
/// cannot agree with the codec by sharing its constant.
const MAGIC: [u8; 4] = *b"BFW1";

/// A frame prefix: the magic, then a header length, and nothing after it. A receiver that sized a
/// buffer to `header_len` before checking it would find no body and answer `Truncated`; one that
/// checks the prefix answers from these nine bytes alone.
fn header_claim(header_len: u32) -> Vec<u8> {
    let mut frame = MAGIC.to_vec();
    frame.extend_from_slice(&header_len.to_be_bytes());
    frame
}

/// THE guard: a hostile `u32` is refused from the length prefix, before any buffer is sized to it.
///
/// Delete the cap in `read_framed` and this goes red: the receiver allocates 4 GiB, finds no body, and
/// answers `Truncated`. The negative assertion is first so the removal is named as the removal rather
/// than as a mismatched variant.
///
/// What this CANNOT observe is the allocation itself. Watching that directly needs a counting
/// `#[global_allocator]`, which needs `unsafe impl GlobalAlloc`, and this workspace denies
/// `unsafe_code` with a two-file allowlist. So the fixture proves the next best thing and proves it
/// exactly: the refusal is derived from the prefix, because there is nothing else on the stream to
/// derive it from.
#[tokio::test]
async fn an_over_cap_header_length_is_refused_from_the_prefix_alone() {
    let mut sink = Vec::new();
    let error = Transfer::new(Vec::new(), header_claim(u32::MAX).as_slice())
        .recv(&mut sink)
        .await
        .expect_err("a 4 GiB header claim is refused");

    assert!(
        !matches!(error, Error::Truncated),
        "the cap is gone: the receiver sized a buffer to the claim and then ran out of stream"
    );
    assert!(
        matches!(error, Error::OversizedHeader { len } if len == u32::MAX),
        "an oversized frame is its own class, not a truncated one: {error}"
    );
}

/// The cap is a ceiling, not a neighbourhood: one byte over is refused.
#[tokio::test]
async fn a_header_one_byte_over_the_cap_is_refused() {
    let mut sink = Vec::new();
    let error = Transfer::new(Vec::new(), header_claim(MAX_HEADER_LEN + 1).as_slice())
        .recv(&mut sink)
        .await
        .expect_err("one byte over the cap is refused");

    assert!(
        matches!(error, Error::OversizedHeader { len } if len == MAX_HEADER_LEN + 1),
        "{error}"
    );
}

/// The other side of the ceiling: a header exactly at the cap is legal and arrives whole, so the
/// bound refuses only what is over it.
#[tokio::test]
async fn a_header_at_the_cap_round_trips() {
    let header = vec![b'h'; MAX_HEADER_LEN as usize];
    let payload = b"payload".to_vec();

    let (sender, receiver) = io::duplex(64 * 1024);
    let (sender_read, sender_write) = io::split(sender);
    let (receiver_read, receiver_write) = io::split(receiver);

    let sent = header.clone();
    let sending = tokio::spawn(async move {
        let mut source = payload.as_slice();
        let blob = Blob::hash(&mut source).await.expect("the blob hashes");
        let mut source = payload.as_slice();
        Transfer::new(sender_write, sender_read)
            .send(&sent, &blob, &mut source)
            .await
    });

    let mut sink = Vec::new();
    let received = Transfer::new(receiver_write, receiver_read)
        .recv(&mut sink)
        .await
        .expect("a header at the cap is accepted");

    sending
        .await
        .expect("the sender task completes")
        .expect("the sender is acked");
    assert_eq!(received.header, header);
    assert_eq!(sink, b"payload");
}

/// Splitting the magic moved no byte: a frame still opens with the same four octets it always did.
/// The split is a change to how the receiver READS the magic, never to what the sender writes, so a
/// shipped peer on either side is unaffected. Write the version before the identity, or widen either
/// half, and this goes red.
#[tokio::test]
async fn a_frame_still_opens_with_the_same_four_octets() {
    let payload = b"payload".to_vec();
    let blob = Blob::hash(&mut payload.as_slice())
        .await
        .expect("the blob hashes");

    let mut written = Vec::new();
    Transfer::new(&mut written, b"".as_slice())
        .send(b"header", &blob, &mut payload.as_slice())
        .await
        .expect_err("there is no peer to ack, and the frame is written before the read");

    assert_eq!(&written[..4], &MAGIC);
}

/// One well-formed frame prefix with the byte at `at` replaced. The two tests below differ only in
/// WHICH half of the magic they corrupt, because that single difference is the whole claim.
fn magic_with(at: usize, byte: u8) -> Vec<u8> {
    let mut frame = header_claim(0);
    frame[at] = byte;
    frame
}

/// A stream whose IDENTITY is not ours is not a stream of this wire, and that is all it is. Make the
/// version arm fire for a foreign identity too and the first assertion goes red.
#[tokio::test]
async fn a_foreign_identity_is_not_a_version_mismatch() {
    let mut sink = Vec::new();
    // `XFW1`: one byte of the identity changed, and nothing else.
    let error = Transfer::new(Vec::new(), magic_with(0, b'X').as_slice())
        .recv(&mut sink)
        .await
        .expect_err("a foreign identity is not a stream of this wire");

    assert!(
        !matches!(error, Error::VersionMismatch { .. }),
        "whatever wrote XFW1 is not a peer of this wire on another build: {error}"
    );
    assert!(matches!(error, Error::Foreign), "{error}");
}

/// The version half of the magic is PARSED, so a peer of this wire on another build is a
/// distinguishable condition rather than a foreign stream. Revert the parse to a four-byte
/// comparison and this goes red at the first assertion.
///
/// The distinction is worth nothing on the wire here (the sender is mid-body and not reading, so
/// there is nobody to tell) and everything in the message, which is why the last two assertions pin
/// both version tags: that string is the whole answer this wire gets to give.
#[tokio::test]
async fn a_version_mismatch_is_not_a_foreign_stream() {
    let mut sink = Vec::new();
    // `BFW2`: one byte of the version changed, and nothing else.
    let error = Transfer::new(Vec::new(), magic_with(3, b'2').as_slice())
        .recv(&mut sink)
        .await
        .expect_err("BFW2 is not this build's grammar");

    assert!(
        !matches!(error, Error::Foreign),
        "a peer of this wire on another build is not a foreign protocol: {error}"
    );
    assert!(matches!(error, Error::VersionMismatch { .. }), "{error}");
    let message = error.to_string();
    assert!(message.contains("BFW2"), "{message}");
    assert!(message.contains("BFW1"), "{message}");
}

/// A sender refuses its own over-cap header, and refuses it before a byte reaches the wire: the two
/// ends hold one bound, so an over-long header is a local error, never a half-written frame the peer
/// has to reject.
#[tokio::test]
async fn a_sender_refuses_its_own_over_cap_header() {
    let header = vec![b'h'; MAX_HEADER_LEN as usize + 1];
    let payload = b"payload".to_vec();
    let mut source = payload.as_slice();
    let blob = Blob::hash(&mut source).await.expect("the blob hashes");

    let mut written = Vec::new();
    let error = Transfer::new(&mut written, b"".as_slice())
        .send(&header, &blob, &mut payload.as_slice())
        .await
        .expect_err("an over-cap header is refused locally");

    assert!(matches!(error, Error::HeaderTooLong), "{error}");
    assert!(written.is_empty(), "nothing reaches the wire");
}

/// The BLAKE3 root of [`GOLDEN_BODY`] (`ec90915f...a18d1d`), spelled out so the golden frame pins the hash function too: a
/// codec that hashed with anything else would write different bytes here.
const GOLDEN_ROOT: [u8; 32] = [
    0xec, 0x90, 0x91, 0x5f, 0xa2, 0x6a, 0xb0, 0x12, 0xa8, 0x9a, 0x88, 0xec, 0xc8, 0xb4, 0x7e, 0x4d,
    0xd7, 0x6c, 0x4a, 0xdf, 0xd6, 0xab, 0xd1, 0xfc, 0x10, 0xe3, 0x21, 0xb0, 0xfc, 0xa1, 0x8d, 0x1d,
];

/// The golden frame's app header and body.
const GOLDEN_HEADER: &[u8] = b"name.txt";
const GOLDEN_BODY: &[u8] = b"payload";

/// Every byte of one whole frame, written out by hand: the magic, the header's `u32` length and bytes,
/// the body's `u64` length, its root, then the body.
fn golden_frame() -> Vec<u8> {
    let mut frame = b"BFW1".to_vec();
    frame.extend_from_slice(&[0, 0, 0, 8]);
    frame.extend_from_slice(b"name.txt");
    frame.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 7]);
    frame.extend_from_slice(&GOLDEN_ROOT);
    frame.extend_from_slice(b"payload");
    frame
}

/// The sender writes the golden frame byte for byte and takes `1` as the receiver's yes. Any change to
/// the frame's layout, field widths, byte order or hash goes red here, so "the same bytes" is a test.
#[tokio::test]
async fn a_sender_writes_the_golden_frame_and_takes_one_as_yes() {
    let blob = Blob::hash(&mut &*GOLDEN_BODY)
        .await
        .expect("the blob hashes");

    let mut written = Vec::new();
    Transfer::new(&mut written, [1u8].as_slice())
        .send(GOLDEN_HEADER, &blob, &mut &*GOLDEN_BODY)
        .await
        .expect("an ack of 1 is a yes");

    assert_eq!(written, golden_frame());
}

/// Any other ack byte is a refusal, and `0` is the one a receiver writes.
#[tokio::test]
async fn a_sender_takes_zero_as_no() {
    let blob = Blob::hash(&mut &*GOLDEN_BODY)
        .await
        .expect("the blob hashes");

    let error = Transfer::new(Vec::new(), [0u8].as_slice())
        .send(GOLDEN_HEADER, &blob, &mut &*GOLDEN_BODY)
        .await
        .expect_err("an ack of 0 is a no");

    assert!(matches!(error, Error::Rejected), "{error}");
}

/// The receiver reads the golden frame, hands its header and body over, and answers `1`.
#[tokio::test]
async fn a_receiver_reads_the_golden_frame_and_answers_one() {
    let mut answer = Vec::new();
    let mut sink = Vec::new();
    let received = Transfer::new(&mut answer, golden_frame().as_slice())
        .recv(&mut sink)
        .await
        .expect("the golden frame verifies");

    assert_eq!(received.header, GOLDEN_HEADER);
    assert_eq!(received.blob.len(), 7);
    assert_eq!(sink, GOLDEN_BODY);
    assert_eq!(answer, [1]);
}

/// A golden frame whose body no longer matches its root is refused, and the receiver answers `0`.
#[tokio::test]
async fn a_receiver_answers_zero_to_a_body_that_does_not_match_its_root() {
    let mut frame = golden_frame();
    let last = frame.len() - 1;
    frame[last] ^= 1;

    let mut answer = Vec::new();
    let error = Transfer::new(&mut answer, frame.as_slice())
        .recv(&mut Vec::new())
        .await
        .expect_err("a changed body is refused");

    assert!(matches!(error, Error::IntegrityFailed), "{error}");
    assert_eq!(answer, [0]);
}
