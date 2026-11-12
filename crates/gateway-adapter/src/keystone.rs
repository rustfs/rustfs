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

//! OpenStack Keystone callers on the gateway stack: RustFS's side of the gateway's host-verified
//! header token claim (gateway ADR-0054).
//!
//! Responsible for: [`keystone_header_token_claim`], the claim an assembly installs when Keystone
//! is enabled; [`establish_keystone_identity`], which records in a request's extensions what the
//! Keystone layer validated; [`KeystoneVerifier`], which names `keystone:<user_id>` from that
//! record and the request's clock reading; and [`KeystoneCaller`], which a handler or an
//! authorization stage reads back from the gateway's verdict.
//!
//! NOT responsible for the following, which the server that wires this in owes:
//! - validating a token, caching or rate limiting that validation, replay and revocation;
//! - installing the claim only when Keystone is enabled, and establishing the identity in the
//!   extensions the gateway itself receives, at their top level. A layer that moves a request's
//!   extensions into a container of its own hides the value, and every token request is refused;
//! - redacting `x-auth-token` and `x-storage-token` in its own logs, traces and recorders;
//! - shared caches. The gateway adds `Vary: x-auth-token` and, unless the handler's
//!   `Cache-Control` already decides sharing, `private`; a CDN that ignores `Vary` is the
//!   deployment's;
//! - the reviewed list of operations that admit the scheme.
//!
//! Upstream: the Keystone layer (`crates/keystone`). Downstream: the gateway, the handlers and the
//! access check.
//!
//! # Admin and STS
//!
//! The gateway refuses the scheme on every privileged operation and on every STS operation before
//! anything here is asked. A Keystone token therefore reaches no admin API and no STS operation
//! through the gateway, whatever its roles: an `admin` token is an owner on the S3 surface only.
//!
//! # The identity travels in the request
//!
//! The gateway reads [`KeystoneEstablished`] by its type from the extensions of the request it is
//! deciding, and refuses a request that holds none without calling the verifier. Nothing here
//! reads a task-local, a thread-local, a global or a cache, so no request is named by what another
//! one earned.
//!
//! # Secrets
//!
//! The Keystone layer's credentials carry the token as their session token. It is not copied, and
//! no type that holds a caller's facts has `Debug`. The names compared here are principal names,
//! which are public; no token material is compared in this crate.

use std::collections::HashMap;

use rustfs_credentials::Credentials;
use rustfs_gateway::{
    AuthSchemeRef, AuthzRequest, HeaderTokenClaim, HeaderTokenRefused, HeaderTokenVerifier, Identity, IdentityOrigin,
    InvalidHeaderTokenClaim, RequestContext, RequestContextView, RequestNow,
};
use serde_json::Value;

use crate::policy::{IamPrincipalLookup, RustfsPrincipal};

/// The prefix of every principal [`KeystoneVerifier`] reports, and of no access key the
/// credential provider resolves.
pub(crate) const KEYSTONE_PRINCIPAL_PREFIX: &str = "keystone:";

/// The header the Keystone layer reads a token from, and every other header it falls back to
/// (`crates/keystone/src/middleware.rs`, `extract_keystone_token`).
const TOKEN_HEADER: &str = "x-auth-token";
const REFUSED_ALIASES: &[&str] = &["x-storage-token"];

/// Every claim the Keystone layer writes (`crates/keystone/src/auth.rs`,
/// `keystone_token_to_credentials`).
const USER_ID_CLAIM: &str = "keystone_user_id";
const KEYSTONE_CLAIMS: [&str; 8] = [
    USER_ID_CLAIM,
    "keystone_username",
    "keystone_project_id",
    "keystone_project_name",
    "keystone_domain_id",
    "keystone_domain_name",
    "keystone_roles",
    "auth_source",
];

/// The roles that make a Keystone caller an owner, compared byte for byte, as the s3s stack
/// compares them (`rustfs/src/auth.rs`, `check_key_valid_with_context`).
const OWNER_ROLES: [&str; 2] = ["admin", "reseller_admin"];

/// The claim RustFS installs on the gateway, with [`KeystoneVerifier`], when and only when
/// Keystone is enabled: `x-auth-token`, refusing `x-storage-token`.
///
/// The gateway then serves a request whose only credential is `x-auth-token` as the principal the
/// verifier names, and answers `403` to one that carries `x-storage-token`, that carries the
/// token beside any AWS credential, or that targets a privileged or an STS operation.
///
/// NOT responsible for: deciding whether Keystone is enabled. With the claim and no Keystone
/// layer every token request is refused; with a Keystone layer and no claim such a request is
/// served as anonymous. Nor for redacting either header in the server's logs.
///
/// # Errors
///
/// [`InvalidHeaderTokenClaim`] if the gateway's claim grammar refuses a name. Both are constants,
/// so this is an assembly error and never depends on a request.
pub fn keystone_header_token_claim() -> Result<HeaderTokenClaim, InvalidHeaderTokenClaim> {
    HeaderTokenClaim::new(TOKEN_HEADER)?.refusing_aliases(REFUSED_ALIASES)
}

/// What the Keystone layer established for one request: the slot the gateway reads the caller
/// from (`HeaderTokenVerifier::Established`).
///
/// A value of this type in a request's extensions is that request's identity, so only
/// [`establish_keystone_identity`] builds one: the fields are private and there is no other
/// constructor. It is neither RustFS `Credentials` nor a gateway `Identity`, which other code
/// builds too. It holds the principal's name, the user name, the roles, the Keystone claims and
/// the expiry, and not the token.
///
/// `Clone` is required by `http::Extensions`. A copy names the same caller until the same expiry;
/// code that copies one into another request establishes that request's identity.
///
/// NOT responsible for: validating anything. It records a validation that already happened.
///
/// ```compile_fail
/// use rustfs_gateway_adapter::KeystoneEstablished;
/// fn forge(established: KeystoneEstablished) -> KeystoneEstablished {
///     KeystoneEstablished { expires_at_unix_seconds: i64::MAX, ..established }
/// }
/// ```
#[derive(Clone)]
pub struct KeystoneEstablished {
    identity: Identity,
    username: String,
    roles: Option<Vec<String>>,
    claims: HashMap<String, Value>,
    expires_at_unix_seconds: i64,
}

/// [`establish_keystone_identity`] inserted nothing. Carries nothing of what it refused.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct KeystoneNotEstablished;

/// Records in `extensions` the identity the Keystone layer established for the request they
/// belong to. That layer calls this once it has validated the request's `x-auth-token`, with the
/// credentials the validation produced; for a request without a token, or whose validation
/// failed, it does not call this and nothing is inserted.
///
/// The roles (the credentials' groups) and the claims are kept as the Keystone layer wrote them.
/// The token, which the credentials carry as their session token, is not copied.
///
/// NOT responsible for: validating the token, rate limiting or caching that validation, or
/// redacting the header in logs. The caller owes two things this function cannot check:
/// `extensions` are those of the request the gateway is handed, at their top level, and the
/// token came from `x-auth-token`, never from `x-storage-token`, which the gateway refuses.
///
/// # Errors
///
/// [`KeystoneNotEstablished`], with nothing inserted and an earlier value removed, unless:
/// - the access key is `keystone:<user_id>`, a legal principal name, and `<user_id>` is the
///   `keystone_user_id` claim, so the name is the user id and never a user or project name;
/// - every claim is one the Keystone layer writes, so nothing else reaches a policy decision;
/// - the credentials carry an expiry.
pub fn establish_keystone_identity(
    extensions: &mut http::Extensions,
    validated: &Credentials,
) -> Result<(), KeystoneNotEstablished> {
    extensions.remove::<KeystoneEstablished>();
    let user_id = validated
        .access_key
        .strip_prefix(KEYSTONE_PRINCIPAL_PREFIX)
        .filter(|user_id| !user_id.is_empty())
        .ok_or(KeystoneNotEstablished)?;
    let claims = validated.claims.as_ref().ok_or(KeystoneNotEstablished)?;
    let named_by_user_id = claims.get(USER_ID_CLAIM).and_then(Value::as_str) == Some(user_id);
    let only_keystone_claims = claims.keys().all(|name| KEYSTONE_CLAIMS.contains(&name.as_str()));
    if !named_by_user_id || !only_keystone_claims {
        return Err(KeystoneNotEstablished);
    }
    let expiration = validated.expiration.ok_or(KeystoneNotEstablished)?;
    // The one place this crate builds a Keystone principal's name: from the Keystone layer's
    // validation, never from a name read back out of a verdict.
    let identity = Identity::new(&validated.access_key).map_err(|_| KeystoneNotEstablished)?;
    extensions.insert(KeystoneEstablished {
        identity,
        username: validated.parent_user.clone(),
        roles: validated.groups.clone(),
        claims: claims.clone(),
        // Whole seconds, rounded down: a token never lives past its expiry.
        expires_at_unix_seconds: expiration.unix_timestamp(),
    });
    Ok(())
}

/// The gateway's `HeaderTokenVerifier` for Keystone.
///
/// A unit type that reads no task-local, thread-local, global or cache: its answer depends on its
/// two arguments alone. The gateway does not call it for a request that holds no
/// [`KeystoneEstablished`]; such a request is refused.
///
/// NOT responsible for: validating the token, revocation between that validation and this call,
/// the `Vary` and `Cache-Control` the gateway adds to the answer, or authorization, which runs
/// afterwards. There is no not-before to compare: the Keystone layer's credentials carry an
/// expiry and no issue time.
pub struct KeystoneVerifier;

impl HeaderTokenVerifier for KeystoneVerifier {
    type Established = KeystoneEstablished;

    /// `keystone:<user_id>` while `now` is before the token's expiry, and a refusal from the
    /// expiry's own second onwards. The gateway marks the name `IdentityOrigin::HostToken`.
    fn identity(&self, established: &KeystoneEstablished, now: RequestNow) -> Result<Identity, HeaderTokenRefused> {
        if now.unix_seconds() >= established.expires_at_unix_seconds {
            return Err(HeaderTokenRefused);
        }
        Ok(established.identity.clone())
    }
}

/// A request cannot be read as its Keystone caller: the scheme, the principal and the request's
/// own extensions disagree. A refusal, never an anonymous caller and never an access key.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct KeystoneCallerRefused;

/// The Keystone caller of one request: the principal the gateway's verdict names, bound to what
/// the Keystone layer established in that same request.
///
/// Both constructors start from the verdict, so a [`KeystoneEstablished`] in a request the
/// gateway did not admit by its token is nobody. [`Self::identity`] is the verdict's own
/// `Identity`, whose origin is `IdentityOrigin::HostToken`: carry that value, and never rebuild an
/// identity from its name, which would spell an access key.
///
/// NOT responsible for: the decision ([`Self::principal`] is its input), or a token that expires
/// while an answer is being served: the expiry was compared once, at the request's clock reading.
pub struct KeystoneCaller<'a> {
    identity: &'a Identity,
    established: &'a KeystoneEstablished,
}

impl<'a> KeystoneCaller<'a> {
    /// The Keystone caller of the request `context` belongs to; `Ok(None)` when the gateway did
    /// not admit the request by its header token, whatever its extensions hold.
    ///
    /// # Errors
    ///
    /// [`KeystoneCallerRefused`] when the request's own extensions do not carry the principal the
    /// verdict names. The handler refuses the request.
    pub fn from_handler(context: &'a RequestContextView) -> Result<Option<Self>, KeystoneCallerRefused> {
        match context.header_token_principal() {
            None => Ok(None),
            Some(identity) => Self::bind(identity, context.transport_extensions().get()).map(Some),
        }
    }

    /// The Keystone caller an authorization stage is asked about; `Ok(None)` for every scheme but
    /// the header token.
    ///
    /// # Errors
    ///
    /// [`KeystoneCallerRefused`] when the scheme, the principal and the request's own extensions
    /// disagree. The stage answers `Deny`.
    pub fn from_authorizer(
        context: &RequestContext<'a>,
        request: &AuthzRequest<'a>,
    ) -> Result<Option<Self>, KeystoneCallerRefused> {
        Self::of_scheme(context.auth_scheme(), request.identity, context.server_extensions().get())
    }

    /// Every scheme is named: one the gateway adds later does not compile here until someone
    /// decides what it is, and none falls through to "a signature".
    fn of_scheme(
        scheme: AuthSchemeRef,
        identity: Option<&'a Identity>,
        established: Option<&'a KeystoneEstablished>,
    ) -> Result<Option<Self>, KeystoneCallerRefused> {
        match scheme {
            AuthSchemeRef::HeaderToken => Self::bind(identity.ok_or(KeystoneCallerRefused)?, established).map(Some),
            AuthSchemeRef::Anonymous
            | AuthSchemeRef::SigV4Header
            | AuthSchemeRef::SigV4Presigned
            | AuthSchemeRef::SigV2Header
            | AuthSchemeRef::SigV2Presigned
            | AuthSchemeRef::PostPolicy
            | AuthSchemeRef::OtherSigned
            | AuthSchemeRef::Bearer => match identity.map(Identity::origin) {
                // A host-reported name under a scheme that is not the token's is no access key.
                Some(IdentityOrigin::HostToken) => Err(KeystoneCallerRefused),
                Some(IdentityOrigin::AccessKey) | None => Ok(None),
            },
        }
    }

    fn bind(identity: &'a Identity, established: Option<&'a KeystoneEstablished>) -> Result<Self, KeystoneCallerRefused> {
        match (identity.origin(), established) {
            (IdentityOrigin::HostToken, Some(established))
                if established.identity.access_key_id() == identity.access_key_id() =>
            {
                Ok(Self { identity, established })
            }
            (IdentityOrigin::HostToken | IdentityOrigin::AccessKey, _) => Err(KeystoneCallerRefused),
        }
    }

    /// The principal as the gateway's verdict carries it: `keystone:<user_id>`, with origin
    /// `IdentityOrigin::HostToken`. Audit sinks and stores keep this value, not its name.
    #[must_use]
    pub fn identity(&self) -> &'a Identity {
        self.identity
    }

    /// The facts a decision about this caller is made with, of kind
    /// [`crate::PrincipalKind::Keystone`], as the s3s stack judges a Keystone request: `account`
    /// is `keystone:<user_id>`; `username` and `parent_user` are the Keystone user name;
    /// `credential_groups` are the token's roles; `group_memberships` are the groups `iam` lists
    /// now for the user name; `claims` are the claims the Keystone layer wrote; and `is_owner`
    /// holds exactly when a role is `admin` or `reseller_admin`.
    ///
    /// NOT responsible for: evaluating a policy, or mapping a role to a policy document.
    #[must_use]
    pub fn principal(&self, iam: &dyn IamPrincipalLookup) -> RustfsPrincipal {
        let established = self.established;
        let is_owner = established
            .roles
            .iter()
            .flatten()
            .any(|role| OWNER_ROLES.contains(&role.as_str()));
        RustfsPrincipal::keystone(
            self.identity.access_key_id(),
            &established.username,
            established.roles.clone(),
            iam.group_memberships(&established.username),
            established.claims.clone(),
            is_owner,
        )
    }
}

#[cfg(test)]
pub(crate) mod fixture;
#[cfg(test)]
mod tests;
