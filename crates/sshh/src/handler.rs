//! The engine's `Handler` impl: `sshd:` as the contract sees it.
//!
//! The ceiling is [`Never`]: a keyless shell has no legitimate public use, so the gate is the only auth and
//! the serving proof is minted rooted-only. The protocol body stays crate-private; this impl is the entry.

use tightbeam_handler::open_policy::{Never, PublicUse};
use tightbeam_handler::{BoxRead, BoxWrite, Handler, ServeError, Served};

/// The `sshd:` engine: a keyless shell over one admitted stream, holding the SSH host-key seed the caller
/// derived from the node identity with [`crate::host_seed`].
pub struct Sshd {
    host_seed: [u8; 32],
}

impl Sshd {
    /// Serve a shell presenting `host_seed` as the SSH host key.
    pub fn new(host_seed: [u8; 32]) -> Self {
        Self { host_seed }
    }
}

impl Handler for Sshd {
    /// NEVER: a keyless shell is remote code execution with no legitimate public use, so the gate IS its
    /// authentication. An open witness cannot mint this proof, and the body refuses one again at the door.
    type Exposure = Never;

    async fn serve(
        &self,
        served: Served<Self>,
        writer: BoxWrite,
        reader: BoxRead,
    ) -> Result<(), ServeError> {
        // The one narrowing seam: an engine whose safety precondition is a verified peer can never be handed
        // an open witness, so this refuses the open case here, before any success response.
        let rooted = served.into_rooted()?;
        crate::serve(rooted, self.host_seed, writer, reader)
            .await
            .map_err(|error| ServeError::Io(std::io::Error::other(error)))
    }
}

/// The engine's ceiling, asserted at compile time: an `OPEN_SAFE` flip here would be a release of a keyless
/// shell to strangers, so it must fail the build, not a review.
const _: () = assert!(!<<Sshd as Handler>::Exposure as PublicUse>::OPEN_SAFE);
