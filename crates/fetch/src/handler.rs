//! The engine's `Handler` impls: `fetch:` as the contract sees it.
//!
//! Two types, two ceilings. [`Fetch`] is unscoped: an empty allowlist fetches any public origin, which is an
//! egress relay with no legitimate public use, so its ceiling is [`Never`]. [`ScopedFetch`] carries a
//! non-empty operator allowlist, which is a deliberate, bounded public use, so its ceiling is [`OptIn`]. The
//! fetch body stays crate-private; these impls are the entries.
//!
//! The public-shape bounds ride [`ScopedFetch`] BY CONSTRUCTION: every scoped fetch enforces the response
//! cap and the total timeout, and the constructor takes no way to drop them, so a public route cannot bind
//! an unbounded scoped fetch. The unscoped [`Fetch`] is member-only (`Never`), so the caps do not apply and
//! it streams unbounded.

use tightbeam_handler::open_policy::{Never, OptIn, PublicUse};
use tightbeam_handler::{BoxRead, BoxWrite, Handler, Metering, ServeError, Served};

use crate::origin::OriginAllowlist;
use crate::serve::Limits;

/// The unscoped `fetch:` engine: any origin that passes the SSRF guard, with no operator scope.
pub struct Fetch;

impl Handler for Fetch {
    /// NEVER: an unscoped fetch is an egress relay any stranger could aim at any public origin, so it must
    /// not face an open gate. An operator that wants a public fetch scopes it with [`ScopedFetch`].
    type Exposure = Never;

    async fn serve(
        &self,
        _served: Served<Self>,
        mut writer: BoxWrite,
        mut reader: BoxRead,
    ) -> Result<(), ServeError> {
        // Member-only (`Never`), so the public-shape bounds do not apply: stream the origin unbounded.
        crate::serve::serve_fetch(
            &mut writer,
            &mut reader,
            &OriginAllowlist::default(),
            Limits::unmetered(),
        )
        .await?;
        Ok(())
    }
}

/// The scoped `fetch:` engine: an operator-bounded origin allowlist, never empty, with the responder-side
/// bounds applied by construction.
pub struct ScopedFetch {
    allow: OriginAllowlist,
}

impl ScopedFetch {
    /// Scope the engine to `allow`. A non-empty allowlist is required: an empty one is unconstrained (the
    /// unscoped [`Fetch`]), which has no legitimate public use. The bounds are not configurable: every
    /// scoped fetch carries the metered response cap and total timeout.
    pub fn new(allow: OriginAllowlist) -> Result<Self, EmptyScope> {
        if allow.is_unconstrained() {
            return Err(EmptyScope);
        }
        Ok(Self { allow })
    }
}

impl Handler for ScopedFetch {
    /// OPT-IN: a non-empty origin allowlist bounds what an anonymous caller can reach, so a public scoped
    /// fetch is a use an operator may deliberately stand behind.
    type Exposure = OptIn;

    /// METERED by construction: the engine always applies the response cap and the total timeout, and no
    /// constructor drops them, so a public route cannot bind it unbounded.
    fn metering(&self) -> Metering {
        Metering::Metered
    }

    async fn serve(
        &self,
        _served: Served<Self>,
        mut writer: BoxWrite,
        mut reader: BoxRead,
    ) -> Result<(), ServeError> {
        crate::serve::serve_fetch(&mut writer, &mut reader, &self.allow, Limits::metered()).await?;
        Ok(())
    }
}

/// A scoped fetch was built over an empty allowlist, which is unconstrained rather than scoped and has no
/// legitimate public form.
#[derive(Debug, thiserror::Error)]
#[error(
    "a scoped fetch needs a non-empty origin allowlist; an unconstrained fetch is never public"
)]
pub struct EmptyScope;

/// The two ceilings, asserted at compile time: flipping either would open a relay an operator cannot bound,
/// or gate a service that is safe to open deliberately.
const _: () = assert!(!<<Fetch as Handler>::Exposure as PublicUse>::OPEN_SAFE);
const _: () = assert!(<<ScopedFetch as Handler>::Exposure as PublicUse>::OPEN_SAFE);

#[cfg(test)]
mod handler_tests {
    use tightbeam_handler::{Handler as _, Metering};

    use super::{Fetch, OriginAllowlist, ScopedFetch};

    /// The scoped constructor refuses the unconstrained allowlist, so the `OptIn` ceiling cannot be claimed
    /// by an engine that could fetch anywhere.
    #[test]
    fn a_scoped_fetch_refuses_an_empty_allowlist() {
        assert!(
            ScopedFetch::new(OriginAllowlist::default()).is_err(),
            "an empty allowlist is unconstrained and must not claim a scoped ceiling"
        );
        assert!(
            ScopedFetch::new(
                OriginAllowlist::parse(["https://news.example"]).expect("the origin parses")
            )
            .is_ok(),
            "a non-empty allowlist scopes the engine"
        );
    }

    /// The scoped engine is metered by construction: every scoped fetch reports `Metered`, because the
    /// constructor takes no way to drop the caps. The unscoped member-only engine reports `Unmetered`.
    #[test]
    fn a_scoped_fetch_is_metered_by_construction() {
        let scoped = ScopedFetch::new(
            OriginAllowlist::parse(["https://news.example"]).expect("the origin parses"),
        )
        .expect("a non-empty scope");
        assert_eq!(scoped.metering(), Metering::Metered);
        assert_eq!(Fetch.metering(), Metering::Unmetered);
    }
}
