//! The operator-set origin scope for the origin-fetch service: an allowlist of origins an admitted
//! requester may fetch, and the normalized (scheme, host, port) [`Origin`] the check compares against.
//!
//! The operator bakes this in at expose time (naming the origins the service may reach); the handler checks
//! each request's origin against it BEFORE the fetch (in front of the SSRF guard, not instead of it). An
//! EMPTY allowlist is unconstrained: an unscoped service fetches any public origin, today's behavior. This is
//! an origin allowlist, never a URL-rewriting policy engine: it gates the (scheme, host, port) triple and
//! says nothing about the path or query.

/// The scheme, host, and port the origin-fetch service may reach, normalized so a request origin and an allowed
/// origin compare as the SAME kind of thing regardless of how each was written. Derived from the SAME
/// `reqwest::Url` parse the SSRF guard uses, so the origin the allowlist checks and the origin the
/// connection lands on cannot diverge.
///
/// Normalization, so a legitimate variant is not spuriously refused and an evasion cannot masquerade as the
/// allowed origin:
/// - **host:** lowercased, with a single trailing dot stripped (`Api.GitHub.COM.` == `api.github.com`).
///   Compared as EXACT equality, never a suffix, so `evil-api.github.com` and `api.github.com.evil.example`
///   both fail against `api.github.com`.
/// - **scheme:** exact, so an allow of `https://x` does not admit `http://x`.
/// - **port:** the URL's `port_or_known_default`, so `https://x` and `https://x:443` are one origin.
///
/// A URL with userinfo (`https://api.github.com@evil.example/`) is REJECTED here, never parsed to its host:
/// so neither the userinfo dressed as the allowed host nor the reverse (`https://evil.example@api.github.com/`)
/// can reach the matcher. Userinfo is rejected ENTIRELY rather than stripped, because a stripping step is
/// a second reading of the URL, and any disagreement between it and the matcher is the evasion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Origin {
    scheme: String,
    host: String,
    port: u16,
}

impl Origin {
    /// Parse an origin from a URL string, keeping only the (scheme, host, port) triple. Errors if the URL
    /// does not parse, carries no host, or has no port and no known default for its scheme. Used both to
    /// build the allowlist from the operator's declared origins and to derive the request's origin from the
    /// same `reqwest::Url` the SSRF guard vets, so both sides of the check see one canonical form.
    pub fn parse(url: &str) -> Result<Self, OriginError> {
        let url = reqwest::Url::parse(url)?;
        Self::of(&url)
    }

    /// The origin of an already-parsed URL: the same source the SSRF `resolve_public` reads its host from,
    /// so the allowlist check and the connection cannot see different hosts (no parse-differential).
    pub fn of(url: &reqwest::Url) -> Result<Self, OriginError> {
        // Reject userinfo (`user:pass@host`) outright rather than parsing around it: a fetch origin/request
        // URL must carry none, and refusing here fails CLOSED, so a request URL whose userinfo is dressed to
        // look like an allowed host (`https://allowed@evil/`) AND the reverse (`https://evil@allowed/`, which
        // parsing around would admit) never reach the matcher at all. `username()` is "" and `password()` is
        // None when absent, so this fires only on a real `@`-authority.
        if !url.username().is_empty() || url.password().is_some() {
            return Err(OriginError::Userinfo);
        }
        let host = url.host_str().ok_or(OriginError::NoHost)?;
        // Lowercase, then strip a SINGLE trailing dot so the rooted-FQDN form `host.` equals `host`; do not
        // strip more than one (`host..` is not `host`).
        let host = host.to_ascii_lowercase();
        let host = host.strip_suffix('.').unwrap_or(&host).to_owned();
        let port = url.port_or_known_default().ok_or(OriginError::NoPort)?;
        Ok(Self {
            scheme: url.scheme().to_ascii_lowercase(),
            host,
            port,
        })
    }
}

/// The operator's origin scope for one origin-fetch service: the set of origins an admitted requester may
/// reach. Empty means unconstrained (an unscoped service fetches any public origin, today's behavior); non-empty means
/// a request origin absent from the set is refused BEFORE the fetch.
#[derive(Debug, Clone, Default)]
pub struct OriginAllowlist(Vec<Origin>);

impl OriginAllowlist {
    /// Build an allowlist from the operator's declared origin strings (`https://news.example`), parsing each
    /// to its normalized [`Origin`]. A malformed origin fails HERE, at expose time, not at dial time as an
    /// opaque refusal.
    pub fn parse<I, S>(origins: I) -> Result<Self, OriginError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        origins
            .into_iter()
            .map(|origin| Origin::parse(origin.as_ref()))
            .collect::<Result<Vec<_>, _>>()
            .map(Self)
    }

    /// Whether this allowlist is unconstrained (empty): an unscoped service with no operator-set scope, which
    /// fetches any public origin. Non-empty scopes every request to the listed origins.
    pub fn is_unconstrained(&self) -> bool {
        let Self(origins) = self;
        origins.is_empty()
    }

    /// Whether an admitted requester may reach `url`: true when the allowlist is unconstrained (empty), or
    /// when `url`'s normalized origin EXACTLY matches a listed one. The check is over the (scheme, host,
    /// port) triple only; the path and query are the requester's to choose. Pass the SAME `reqwest::Url` the
    /// SSRF guard vets, so the check and the connection agree on the host.
    pub fn admits(&self, url: &reqwest::Url) -> bool {
        let Self(origins) = self;
        if origins.is_empty() {
            return true;
        }
        match Origin::of(url) {
            Ok(request) => origins.contains(&request),
            // A URL with no host / no port cannot match a listed origin; refuse rather than admit. The SSRF
            // guard rejects it too, but the allowlist must not fall open on a shape it cannot normalize.
            Err(_) => false,
        }
    }
}

/// Compose the request URL to fetch by resolving `target` (an inbound request path and query) against
/// `base` (the origin the caller named), joining them per the URL grammar rather than concatenating strings.
/// A join merges the two paths correctly, so a base with a trailing slash and a target with a leading one
/// (`https://x/` + `/a`) compose to `https://x/a`, never the `https://x//a` a raw concatenation yields, and a
/// malformed base or target is a typed error here rather than a broken URL sent to the origin.
///
/// A pure edge helper that marshals two strings through the `reqwest::Url` parser (the same parser the SSRF
/// guard and the origin allowlist use), so the composed URL is well-formed by the same grammar the fetch
/// then vets. The caller decides how a root request (`/`) is treated; this always joins.
pub fn compose_url(base: &str, target: &str) -> Result<String, ComposeError> {
    let base = reqwest::Url::parse(base).map_err(ComposeError::Base)?;
    let joined = base.join(target).map_err(|source| ComposeError::Target {
        target: target.to_owned(),
        source,
    })?;
    Ok(joined.into())
}

/// Why a URL could not be read as an [`Origin`]. Each arm is one shape the parse refuses, so a caller
/// matches the cause instead of reading a message, and the expose-time error names the exact fault.
#[derive(Debug, thiserror::Error)]
pub enum OriginError {
    /// The text is not a URL.
    #[error("invalid origin url: {0}")]
    Url(#[from] url::ParseError),
    /// The URL carries `user:pass@`. Refused whole, never parsed around, so a host dressed as userinfo (or
    /// the reverse) cannot reach the matcher.
    #[error("url carries userinfo (user:pass@), which a fetch origin must not")]
    Userinfo,
    /// The URL names no host.
    #[error("url has no host")]
    NoHost,
    /// The URL names no port and its scheme has no known default.
    #[error("url has no port")]
    NoPort,
}

/// Why a base and a target could not compose into one request URL.
#[derive(Debug, thiserror::Error)]
pub enum ComposeError {
    /// The base is not a URL.
    #[error("invalid fetch url: {0}")]
    Base(#[source] url::ParseError),
    /// The target does not join onto the base under the URL grammar.
    #[error("invalid request path {target}: {source}")]
    Target {
        /// The request path and query the caller asked to join.
        target: String,
        /// The join failure.
        #[source]
        source: url::ParseError,
    },
}

#[cfg(test)]
#[path = "origin_tests.rs"]
mod origin_tests;
