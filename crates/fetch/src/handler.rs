//! The engine's `Handler` impls: `fetch:` as the contract sees it.
//!
//! Two types, two ceilings. [`Fetch`] is unscoped: an empty allowlist fetches any public origin, which is an
//! egress relay with no legitimate public use, so its ceiling is [`Never`]. [`ScopedFetch`] carries a
//! non-empty operator allowlist, which is a deliberate, bounded public use, so its ceiling is [`OptIn`]. The
//! fetch body stays crate-private; these impls are the entries.

use tightbeam_handler::open_policy::{Never, OptIn, PublicUse};
use tightbeam_handler::{BoxRead, BoxWrite, Handler, ServeError, Served};

use crate::origin::OriginAllowlist;

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
        crate::serve::serve_fetch(&mut writer, &mut reader, &OriginAllowlist::default()).await?;
        Ok(())
    }
}

/// The scoped `fetch:` engine: an operator-bounded origin allowlist, never empty.
pub struct ScopedFetch {
    allow: OriginAllowlist,
}

impl ScopedFetch {
    /// Scope the engine to `allow`. A non-empty allowlist is required: an empty one is unconstrained (the
    /// unscoped [`Fetch`]), which has no legitimate public use.
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

    async fn serve(
        &self,
        _served: Served<Self>,
        mut writer: BoxWrite,
        mut reader: BoxRead,
    ) -> Result<(), ServeError> {
        crate::serve::serve_fetch(&mut writer, &mut reader, &self.allow).await?;
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
    use super::{OriginAllowlist, ScopedFetch};

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
}
