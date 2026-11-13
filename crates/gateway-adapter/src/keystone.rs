// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! RustFS's side of the gateway's host-verified header token claim for OpenStack Keystone
//! (gateway ADR-0054): the claim, the value a validated token establishes, and the verifier.
//!
//! NOT responsible for: validating tokens (`rustfs-keystone`), wiring any of this into the
//! server, or authorising a Keystone principal.
//! Upstream: `rustfs-keystone`. Downstream: the gateway assembly.

use rustfs_gateway::{HeaderTokenClaim, HeaderTokenRefused, HeaderTokenVerifier, Identity, InvalidHeaderTokenClaim, RequestNow};
use rustfs_keystone::ValidatedKeystoneToken;

/// The prefix of every principal [`KeystoneVerifier`] reports, and of no access key the
/// credential provider resolves.
pub(crate) const KEYSTONE_PRINCIPAL_PREFIX: &str = "keystone:";

/// `x-auth-token`, refusing `x-storage-token`: the two headers `rustfs-keystone` reads a token from.
///
/// # Errors
///
/// [`InvalidHeaderTokenClaim`] if the gateway's grammar refuses a name; both are constants.
pub fn keystone_header_token_claim() -> Result<HeaderTokenClaim, InvalidHeaderTokenClaim> {
    HeaderTokenClaim::new("x-auth-token")?.refusing_aliases(&["x-storage-token"])
}

/// The identity one validated token established, read by the gateway from the request's own
/// extensions. Only [`establish_keystone_identity`] builds one; it holds no token.
///
/// ```compile_fail,E0451
/// fn forge(established: rustfs_gateway_adapter::KeystoneEstablished) -> rustfs_gateway_adapter::KeystoneEstablished {
///     rustfs_gateway_adapter::KeystoneEstablished { expires_at_unix_seconds: i64::MAX, ..established }
/// }
/// ```
#[derive(Clone)]
pub struct KeystoneEstablished {
    identity: Identity,
    expires_at_unix_seconds: i64,
}

/// [`establish_keystone_identity`] inserted nothing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct KeystoneNotEstablished;

/// Records in `extensions` the identity `validated` proves, replacing any earlier one. Takes the
/// proof `rustfs-keystone` returns from a validation, so nothing else can establish an identity:
///
/// ```compile_fail,E0308
/// fn forge(extensions: &mut http::Extensions, credentials: &rustfs_credentials::Credentials) {
///     let _ = rustfs_gateway_adapter::establish_keystone_identity(extensions, credentials);
/// }
/// ```
///
/// NOT responsible for: which extensions these are (the gateway reads only their top level).
///
/// # Errors
///
/// [`KeystoneNotEstablished`], with nothing left inserted, when the user id is empty or
/// `keystone:<user_id>` is not a principal name the gateway accepts.
pub fn establish_keystone_identity(
    extensions: &mut http::Extensions,
    validated: ValidatedKeystoneToken,
) -> Result<(), KeystoneNotEstablished> {
    extensions.remove::<KeystoneEstablished>();
    if validated.user_id().is_empty() {
        return Err(KeystoneNotEstablished);
    }
    let identity =
        Identity::new(&format!("{KEYSTONE_PRINCIPAL_PREFIX}{}", validated.user_id())).map_err(|_| KeystoneNotEstablished)?;
    extensions.insert(KeystoneEstablished {
        identity,
        // Rounded down: a token never lives past its expiry.
        expires_at_unix_seconds: validated.expires_at().unix_timestamp(),
    });
    Ok(())
}

/// The gateway's `HeaderTokenVerifier` for Keystone: answers from its two arguments alone.
///
/// NOT responsible for: revocation after validation, or authorisation.
pub struct KeystoneVerifier;

impl HeaderTokenVerifier for KeystoneVerifier {
    type Established = KeystoneEstablished;

    /// `keystone:<user_id>` while `now` is before the expiry; refused from the expiry's second on.
    fn identity(&self, established: &KeystoneEstablished, now: RequestNow) -> Result<Identity, HeaderTokenRefused> {
        if now.unix_seconds() >= established.expires_at_unix_seconds {
            return Err(HeaderTokenRefused);
        }
        Ok(established.identity.clone())
    }
}

#[cfg(test)]
pub(crate) mod fixture;
#[cfg(test)]
mod tests;
