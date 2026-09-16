//! fetch: an HTTP origin fetch a keyed node performs on an admitted requester's behalf.
//!
//! The node acts as an HTTP client on the requester's behalf: it reads a [`FetchRequest`] off an admitted
//! stream, performs the GET/HEAD at the origin (TLS terminated HERE, not at the requester), vets the target
//! against SSRF, and streams the response back with `Range` intact so a resumable download works. This is
//! the smallest honest instance of "run this at a keyed node": a fetch scoped to one origin, not a general
//! proxy or an open VPN.
//!
//! **Origin allowlist.** An operator scopes the service to a fixed set of origins at expose time, and the
//! scoped handler refuses any request whose origin is not in that [`OriginAllowlist`] before it connects.
//! This is the control that makes an OPEN (unauthenticated) origin-fetch service safe and narrows an admitted
//! delegate's egress. The unscoped handler carries an EMPTY allowlist, which is unconstrained: it fetches any
//! origin that passes the SSRF guard.
//!
//! **Responder-side bounds.** The scoped handler enforces them by construction: one response body is
//! capped at 16 MiB and the whole origin operation at 30 seconds, and no public constructor drops the
//! caps, so a public route cannot bind an unbounded scoped fetch. The guard is a property of the engine
//! type, not an assembly's choice. The unscoped handler is member-only and streams the origin to its own
//! end.
//!
//! It is a service crate: it knows what to DO with an admitted stream, never how the peer was reached or
//! gated. The composing consumer binds [`Fetch`](crate::Fetch) (unconstrained, never public) or
//! [`ScopedFetch`](crate::ScopedFetch) (an operator allowlist, opt-in public) into its route table; the
//! [`http`] framing is public so the same caller's client side speaks the wire.

pub mod http;
mod origin;
mod serve;

mod handler;
pub use handler::{EmptyScope, Fetch, ScopedFetch};

#[cfg(test)]
mod http_tests;
#[cfg(test)]
mod serve_tests;

pub use crate::http::{FetchRequest, FetchResponse};
pub use crate::origin::{ComposeError, Origin, OriginAllowlist, OriginError, compose_url};
