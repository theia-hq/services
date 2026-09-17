//! The origin-fetch handler: read a [`FetchRequest`] off the stream, perform
//! the HTTP GET/HEAD at the origin with a real HTTPS client (TLS terminates HERE, not at the requester),
//! and stream the response back (status + headers, then the body to stream close). This is the smallest
//! honest instance of "run this at a keyed node": a fetch, not a general proxy.
//!
//! Every origin operation runs under the engine's [`Limits`]: the fetch and the body share one deadline,
//! and the body stops at the byte cap, so a hanging or endless origin cannot park the stream open.

use core::future::Future;
use core::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use core::time::Duration;

use futures::StreamExt as _;
use tokio::io::{self, AsyncWriteExt as _};
use tokio::time;

use crate::http::{FetchRequest, FetchResponse, MAX_HEADERS};
use crate::origin::{OriginAllowlist, OriginError};

/// How long to wait for a connector to send its fetch frame before dropping the stream. The exposer's
/// pre-gate request timeout does not cover this frame (it is read AFTER admission), so an admitted peer
/// could otherwise open a stream and dribble length prefixes forever. Same bound as the pre-gate one.
const FETCH_READ_TIMEOUT: Duration = Duration::from_secs(10);

/// The largest origin body a metered fetch streams back before it stops and closes truncated.
pub(crate) const FETCH_MAX_BYTES: u64 = 16 * 1024 * 1024;

/// The longest a metered origin fetch may run, from the origin request through the body's end. One clock
/// covers a hanging connect and an endless body, so a stranger's fetch cannot park a stream open.
pub(crate) const FETCH_TOTAL_TIMEOUT: Duration = Duration::from_secs(30);

/// The text a metered fetch puts on the wire when the origin misses [`FETCH_TOTAL_TIMEOUT`] before any
/// header: the rendering of [`FetchError::TimedOut`].
pub(crate) const FETCH_TIMEOUT_MESSAGE: &str = "origin fetch timed out";

/// Why one origin fetch was refused, before or at the origin. Every refusal the responder can produce is
/// one arm here, so the engine matches a cause rather than assembling a message at the site; the requester
/// reads the rendering as the [`FetchResponse::Error`] text, since a cause cannot cross the wire as a type.
#[derive(Debug, thiserror::Error)]
pub(crate) enum FetchError {
    /// The request named a method other than GET or HEAD.
    #[error("method {0} not allowed (fetch is GET/HEAD only)")]
    Method(String),
    /// The request URL does not parse.
    #[error("invalid url: {0}")]
    Url(#[from] url::ParseError),
    /// The request URL's origin is outside this service's allowlist.
    #[error("origin {0} not allowed by this fetch service")]
    OriginNotAllowed(String),
    /// The request URL's scheme is not `http` or `https`.
    #[error("scheme {0} not allowed (http/https only)")]
    Scheme(String),
    /// The request URL has no host, or no port and no known default.
    #[error(transparent)]
    Origin(#[from] OriginError),
    /// The host did not resolve.
    #[error("resolve {host}: {source}")]
    Resolve {
        /// The host the request named.
        host: String,
        /// The resolver's failure.
        #[source]
        source: io::Error,
    },
    /// The host resolved to nothing.
    #[error("{0} resolved to no addresses")]
    NoAddresses(String),
    /// The host resolves to at least one non-public address, the SSRF shape this service refuses whole.
    #[error("refusing to fetch {host}: it resolves to the non-public address {ip}")]
    NonPublic {
        /// The host the request named.
        host: String,
        /// The first non-public address it resolved to.
        ip: IpAddr,
    },
    /// The HTTP client could not be built.
    #[error("http client: {0}")]
    Client(#[source] reqwest::Error),
    /// The origin request failed before a response header arrived.
    #[error("origin request failed: {0}")]
    Request(#[source] reqwest::Error),
    /// The origin missed [`FETCH_TOTAL_TIMEOUT`] before any header.
    #[error("{}", FETCH_TIMEOUT_MESSAGE)]
    TimedOut,
}

/// The responder-side bounds one origin fetch enforces: the largest body it streams back and the longest
/// the whole origin operation may run. Crate-private: [`metered`](Self::metered) is the bound the public
/// scoped engine applies unconditionally, and [`unmetered`](Self::unmetered) is the member-only unscoped
/// path with no bounds.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Limits {
    /// The largest body to stream back, or `None` for unbounded.
    pub(crate) max_bytes: Option<u64>,
    /// The longest the origin operation may run, or `None` for unbounded.
    pub(crate) max_duration: Option<Duration>,
}

impl Limits {
    /// The bounds a scoped fetch enforces unconditionally: a 16 MiB body cap and a 30-second total
    /// timeout. A public scoped fetch cannot be constructed without them.
    pub(crate) fn metered() -> Self {
        Self {
            max_bytes: Some(FETCH_MAX_BYTES),
            max_duration: Some(FETCH_TOTAL_TIMEOUT),
        }
    }

    /// No responder-side bounds: stream the origin to its own end, with no deadline. The member-only
    /// unscoped engine's path; the public scoped engine applies [`metered`](Self::metered).
    pub(crate) fn unmetered() -> Self {
        Self {
            max_bytes: None,
            max_duration: None,
        }
    }
}

/// Read one [`FetchRequest`], fetch the origin, write the [`FetchResponse`] + body, then close the write
/// half so the requester sees the body's end.
///
/// `allow` is the operator's origin scope for this service, set at expose time: if it
/// is non-empty and the request's origin is not in it, the fetch is refused with a typed
/// [`FetchResponse::Error`] BEFORE any connection, IN FRONT of the SSRF guard, not instead of it. An empty
/// allowlist is unconstrained (an unscoped service), so today's any-public-origin behavior is unchanged.
///
/// `limits` bounds the origin operation: the fetch and the body share one deadline, the body stops at the
/// byte cap, and an unbounded configuration streams the origin to its own end.
///
/// Crate-private: the entries are [`Fetch`](crate::Fetch) / [`ScopedFetch`](crate::ScopedFetch), the only
/// public doors, and the ceiling each declares is the posture check.
pub(crate) async fn serve_fetch<W, R>(
    writer: &mut W,
    reader: &mut R,
    allow: &OriginAllowlist,
    limits: Limits,
) -> io::Result<()>
where
    W: io::AsyncWrite + Unpin,
    R: io::AsyncRead + Unpin,
{
    // Bound the frame read: an admitted peer must send its request promptly, not hold a stream open by
    // stalling mid-frame. A timeout maps to a clean drop of this one stream.
    let request = match time::timeout(FETCH_READ_TIMEOUT, FetchRequest::read(reader)).await {
        Ok(result) => result?,
        Err(_) => return Err(io::Error::other("fetch request read timed out")),
    };
    let served = fetch_and_stream(writer, &request, allow, limits).await;
    // Always close the write half, even if the body errored mid-stream, so the requester sees a clean
    // EOF and can distinguish a complete response from a truncated one.
    let closed = writer.shutdown().await;
    served.and(closed)
}

/// Perform the origin request and stream its response back, all under `limits`: the fetch and the body
/// share one deadline, and the body stops at the byte cap. An elapsed deadline answers with the typed
/// timeout error, but only BEFORE the response header; after it, the body can only close early, because
/// an error frame following a valid `Ok` frame would be read as payload by the requester.
async fn fetch_and_stream<W: io::AsyncWrite + Unpin>(
    writer: &mut W,
    request: &FetchRequest,
    allow: &OriginAllowlist,
    limits: Limits,
) -> io::Result<()> {
    // One deadline for the whole origin operation: connect, response head, and body share it, so a
    // hanging or endless origin cannot park this stream open.
    let deadline = limits.max_duration.map(|cap| time::Instant::now() + cap);
    let response = match bounded(deadline, fetch_origin(request, allow)).await {
        Ok(response) => response,
        Err(error) => return FetchResponse::Error(error.to_string()).write(writer).await,
    };
    let Some(deadline) = deadline else {
        // Unmetered: no deadline, so the origin's own end is the only terminator.
        return stream_response(writer, response, limits.max_bytes).await;
    };
    // The header is on the wire now: a deadline that fires while streaming closes the body truncated,
    // never appending a second frame the requester would read as body bytes.
    match time::timeout_at(
        deadline,
        stream_response(writer, response, limits.max_bytes),
    )
    .await
    {
        Ok(streamed) => streamed,
        Err(_elapsed) => Ok(()),
    }
}

/// Await `operation` under `deadline`, mapping an elapsed deadline to [`FetchError::TimedOut`]. `None`
/// runs the operation to completion (an unmetered fetch).
pub(crate) async fn bounded<T>(
    deadline: Option<time::Instant>,
    operation: impl Future<Output = Result<T, FetchError>>,
) -> Result<T, FetchError> {
    match deadline {
        Some(deadline) => match time::timeout_at(deadline, operation).await {
            Ok(result) => result,
            Err(_elapsed) => Err(FetchError::TimedOut),
        },
        None => operation.await,
    }
}

/// Perform the origin request. Redirects are forwarded to the requester verbatim (not followed here), so
/// the client decides; TLS terminates at this node.
///
/// The origin is vetted before any connection: only `http`/`https`, and the host must resolve ENTIRELY to
/// public addresses. This stops a slip-holder from turning the node into an SSRF pivot: fetching its
/// loopback, its LAN (RFC1918), or the cloud metadata endpoint (`169.254.169.254`) to steal instance
/// credentials. The vetted address is pinned into the client so a DNS rebind between the check and the
/// connect cannot swap a public answer for a private one.
async fn fetch_origin(
    request: &FetchRequest,
    allow: &OriginAllowlist,
) -> Result<reqwest::Response, FetchError> {
    let method = allowed_method(&request.method)?;
    let url = reqwest::Url::parse(&request.url)?;
    // Enforce the operator's origin scope BEFORE the SSRF guard and any connection: a request outside the
    // declared origin is refused here, reading its origin from the SAME parse the guard vets below, so the
    // allowlist and the connection cannot see different hosts. An empty allowlist admits any public origin.
    if !allow.admits(&url) {
        return Err(FetchError::OriginNotAllowed(
            url.origin().ascii_serialization(),
        ));
    }
    if !matches!(url.scheme(), "http" | "https") {
        return Err(FetchError::Scheme(url.scheme().to_owned()));
    }
    let host = url.host_str().ok_or(OriginError::NoHost)?.to_owned();
    let port = url.port_or_known_default().ok_or(OriginError::NoPort)?;
    let vetted = resolve_public(&host, port).await?;
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        // Pin resolution to the vetted address: reqwest connects here and nowhere else for this host, so a
        // rebind cannot move the target after the check. The pin key is `host_str()`, which for a rooted
        // FQDN keeps its trailing dot (`api.github.com.`); an observed probe (reqwest
        // 0.12.28) confirms reqwest keys its resolve override on that SAME dotted host it connects for, so
        // the pin HOLDS for the trailing-dot form (no desync, no fresh connect-time lookup) and needs no
        // dot-stripping here.
        .resolve(&host, vetted)
        .build()
        .map_err(FetchError::Client)?;
    let mut outgoing = client.request(method, url);
    for (name, value) in forward_headers(&request.headers) {
        outgoing = outgoing.header(name, value);
    }
    outgoing.send().await.map_err(FetchError::Request)
}

/// Write the response frame (origin status + headers verbatim) then stream the body to the writer until
/// the origin body ends or the byte cap is reached. Never buffers the whole body, so a large download
/// does not grow the node's memory. A body the origin keeps extending past the cap is logged and closed
/// truncated, AFTER a valid `Ok` header, so the requester sees a short body, never a second frame.
pub(crate) async fn stream_response<W: io::AsyncWrite + Unpin>(
    writer: &mut W,
    response: reqwest::Response,
    max_bytes: Option<u64>,
) -> io::Result<()> {
    let status = response.status().as_u16();
    // Cap the forwarded headers at the same bound the reader enforces, so a hostile origin cannot return a
    // frame the requester would then reject as over-count (or force an unbounded write).
    let headers = response
        .headers()
        .iter()
        .filter_map(|(name, value)| {
            value
                .to_str()
                .ok()
                .map(|v| (name.to_string(), v.to_string()))
        })
        .take(MAX_HEADERS)
        .collect();
    FetchResponse::Ok { status, headers }.write(writer).await?;
    let mut body = response.bytes_stream();
    let Some(max_bytes) = max_bytes else {
        // Unmetered: the origin's own end is the only terminator.
        while let Some(chunk) = body.next().await {
            writer.write_all(&chunk.map_err(body_error)?).await?;
        }
        return Ok(());
    };
    let mut written: u64 = 0;
    while let Some(chunk) = body.next().await {
        let chunk = chunk.map_err(body_error)?;
        let room = (max_bytes - written) as usize;
        if chunk.len() > room {
            // The cap lands inside this chunk: forward only what fits, then close. A body that reaches
            // the cap exactly stays in the loop for one more poll, so an origin that ends there is not
            // logged as truncated while one that continues is.
            writer.write_all(&chunk[..room]).await?;
            tracing::warn!(
                max_bytes,
                "origin body exceeded the fetch byte cap; closing truncated"
            );
            return Ok(());
        }
        writer.write_all(&chunk).await?;
        written += chunk.len() as u64;
    }
    Ok(())
}

/// Map a body-stream failure into the stream error the requester's close reports.
fn body_error(error: reqwest::Error) -> io::Error {
    io::Error::other(format!("origin body: {error}"))
}

/// The origin method for a request method: GET and HEAD only (a fetch, not a general HTTP proxy).
pub(crate) fn allowed_method(method: &str) -> Result<reqwest::Method, FetchError> {
    match method {
        "GET" => Ok(reqwest::Method::GET),
        "HEAD" => Ok(reqwest::Method::HEAD),
        other => Err(FetchError::Method(other.to_owned())),
    }
}

/// The request headers to forward to the origin: everything except hop-by-hop headers and `Host` (the
/// client derives Host from the URL). `Range` and the rest pass through, so a ranged/resumable GET works.
pub(crate) fn forward_headers(headers: &[(String, String)]) -> Vec<(&str, &str)> {
    const SKIP: [&str; 7] = [
        "host",
        "connection",
        "proxy-connection",
        "proxy-authorization",
        "keep-alive",
        "transfer-encoding",
        "upgrade",
    ];
    headers
        .iter()
        .filter(|(name, _)| {
            let lower = name.to_ascii_lowercase();
            !SKIP.contains(&lower.as_str())
        })
        .map(|(name, value)| (name.as_str(), value.as_str()))
        .collect()
}

/// Resolve `host:port` and require EVERY resolved address to be public, returning the first. A host that
/// resolves to any non-public address is refused wholesale (that mix is the classic SSRF / DNS-rebinding
/// shape), while a legitimately public host resolves only to public IPs. The returned address is what the
/// client is pinned to, so the connection lands on a vetted IP.
async fn resolve_public(host: &str, port: u16) -> Result<SocketAddr, FetchError> {
    let addrs: Vec<SocketAddr> = tokio::net::lookup_host((host, port))
        .await
        .map_err(|source| FetchError::Resolve {
            host: host.to_owned(),
            source,
        })?
        .collect();
    let first = *addrs
        .first()
        .ok_or_else(|| FetchError::NoAddresses(host.to_owned()))?;
    if let Some(bad) = addrs.iter().find(|addr| !is_public(addr.ip())) {
        return Err(FetchError::NonPublic {
            host: host.to_owned(),
            ip: bad.ip(),
        });
    }
    Ok(first)
}

/// Whether an IP is a public (globally routable) unicast address: the only kind this service will reach.
/// Conservative: loopback, private, link-local, shared (CGNAT), unspecified, and multicast are all NOT
/// public. Any IPv6 that EMBEDS an IPv4 address (mapped, NAT64, or the deprecated compatible form) is
/// unwrapped and judged as IPv4, so `::ffff:169.254.169.254` AND the NAT64 `64:ff9b::169.254.169.254`
/// (which a DNS64/NAT64 host routes straight to the cloud metadata IP) cannot slip past.
pub(crate) fn is_public(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => is_public_v4(v4),
        IpAddr::V6(v6) => match embedded_ipv4(v6) {
            Some(v4) => is_public_v4(v4),
            None => {
                let seg = v6.segments();
                let unique_local = seg[0] & 0xfe00 == 0xfc00; // fc00::/7
                let link_local = seg[0] & 0xffc0 == 0xfe80; // fe80::/10
                !(v6.is_loopback()
                    || v6.is_unspecified()
                    || v6.is_multicast()
                    || unique_local
                    || link_local)
            }
        },
    }
}

/// Extract an IPv4 address embedded in an IPv6 one, so an internal target wearing IPv6 clothing is judged
/// by its real IPv4. Covers the three embeddings a translating host will actually route to that v4:
/// IPv4-mapped `::ffff:0:0/96` (via `to_ipv4_mapped`), NAT64 well-known `64:ff9b::/96` (RFC 6052, the
/// standard DNS64 prefix), and deprecated IPv4-compatible `::/96` (`::a.b.c.d`). `::` and `::1` are left
/// for the caller to refuse as unspecified/loopback. Returns `None` for a native IPv6 address.
fn embedded_ipv4(v6: Ipv6Addr) -> Option<Ipv4Addr> {
    if let Some(v4) = v6.to_ipv4_mapped() {
        return Some(v4);
    }
    let seg = v6.segments();
    let low = |a: u16, b: u16| Ipv4Addr::new((a >> 8) as u8, a as u8, (b >> 8) as u8, b as u8);
    // NAT64 well-known prefix 64:ff9b::/96.
    if seg[0] == 0x0064 && seg[1] == 0xff9b && seg[2..6] == [0, 0, 0, 0] {
        return Some(low(seg[6], seg[7]));
    }
    // IPv4-translatable `::ffff:0:0/96` (RFC 6052): the `ffff` sits in seg[4] (seg[5]==0), so
    // `to_ipv4_mapped` (which matches `ffff` in seg[5]) does not catch it.
    if seg[0..4] == [0, 0, 0, 0] && seg[4] == 0xffff && seg[5] == 0 {
        return Some(low(seg[6], seg[7]));
    }
    // Deprecated IPv4-compatible ::/96, excluding :: and ::1 (handled as unspecified/loopback).
    if seg[0..6] == [0, 0, 0, 0, 0, 0] && !(seg[6] == 0 && (seg[7] == 0 || seg[7] == 1)) {
        return Some(low(seg[6], seg[7]));
    }
    None
}

/// The IPv4 half of [`is_public`]. `is_shared` (100.64.0.0/10, CGNAT) is not yet stable in std, so it is
/// checked by hand; the rest use std predicates. `is_link_local` covers `169.254.0.0/16`, which includes
/// the cloud metadata endpoint `169.254.169.254`.
fn is_public_v4(v4: Ipv4Addr) -> bool {
    let octets = v4.octets();
    let shared = octets[0] == 100 && (octets[1] & 0xc0) == 64;
    !(v4.is_private()
        || v4.is_loopback()
        || v4.is_link_local()
        || v4.is_broadcast()
        || v4.is_documentation()
        || v4.is_unspecified()
        || v4.is_multicast()
        || shared)
}
