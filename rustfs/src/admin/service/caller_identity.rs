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

//! Resolves the long-term identity behind an authenticated admin request.
//!
//! Self-service endpoints (`/v3/account/*`, `/v3/mfa/*`) act on "whoever is
//! calling" rather than on a target named in the request, so they all need the
//! same answer to two questions: which durable identity owns this credential,
//! and may that credential mutate the identity's own authentication material?
//!
//! Both answers are subtle. The Console operates entirely with STS session
//! credentials, so "the caller" is almost never the access key that signed the
//! request. Service accounts and OIDC sessions also present as derived
//! credentials but must *not* be allowed to rewrite the parent's secret. This
//! module is the single place those distinctions are made.

use crate::admin::auth::authenticate_request;
use crate::admin::runtime_sources::current_action_credentials;
use crate::admin::storage_api::s3::{self, Body, S3ErrorCode, S3Request, S3Result};
use crate::auth::constant_time_eq;
use rustfs_credentials::Credentials;
use rustfs_iam::federation::OIDC_VIRTUAL_PARENT_CLAIM;
use rustfs_iam::sys::is_rustfs_oidc_claims;
use rustfs_madmin::account::{AccountMutability, CredentialsSource, IdentityType};

/// Claim written by the Keystone middleware onto its synthesized credentials.
const KEYSTONE_ROLES_CLAIM: &str = "keystone_roles";

/// The durable identity that owns a session credential.
///
/// Prefers the `parent_user` field and falls back to the JWT `parent` claim:
/// some stores persist the parent only inside the session token, so checking
/// just one of the two silently misidentifies the caller. This mirrors the
/// resolution order used by the user-management handlers.
pub(crate) fn session_parent_identity(credentials: &Credentials) -> Option<&str> {
    if !credentials.parent_user.is_empty() {
        return Some(credentials.parent_user.as_str());
    }
    credentials
        .claims
        .as_ref()
        .and_then(|claims| claims.get("parent"))
        .and_then(|value| value.as_str())
}

/// Why a credential may not change its own authentication material.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CredentialMutationDenial {
    /// Root credentials are pinned by a process-wide `OnceLock` and also feed
    /// the derived internode RPC secret, so they cannot be rotated at runtime.
    RootIsEnvironmentProvisioned,
    /// The identity lives in an external IdP; RustFS holds no secret to change
    /// and no TOTP enrollment of its own would be authoritative.
    FederatedIdentity,
    /// Machine credentials must not be able to take over the human identity
    /// they were minted from.
    ServiceAccount,
    /// A derived credential whose parent could not be determined.
    UnresolvedParent,
}

impl CredentialMutationDenial {
    pub(crate) const fn message(self) -> &'static str {
        match self {
            Self::RootIsEnvironmentProvisioned => {
                "the root identity is provisioned from the server environment and cannot be changed at runtime"
            }
            Self::FederatedIdentity => "federated identities are managed by their identity provider",
            Self::ServiceAccount => "service account credentials cannot change the credentials of their parent identity",
            Self::UnresolvedParent => "the parent identity of this session could not be resolved",
        }
    }
}

/// An authenticated caller, resolved to the identity it acts as.
#[derive(Debug, Clone)]
pub(crate) struct CallerIdentity {
    /// The durable identity. For STS and service-account credentials this is
    /// the parent, not the ephemeral access key that signed the request.
    pub(crate) access_key: String,
    pub(crate) identity_type: IdentityType,
    /// The access key actually presented, when it differs from `access_key`.
    pub(crate) session_access_key: Option<String>,
    pub(crate) credentials_source: CredentialsSource,
    /// The verified credentials of the presented key.
    pub(crate) credentials: Credentials,
    pub(crate) is_owner: bool,
    /// Set when this credential kind may not rotate its own secret.
    pub(crate) mutation_denial: Option<CredentialMutationDenial>,
    /// Set when this credential kind may not manage its own second factor.
    ///
    /// Distinct from [`Self::mutation_denial`], because the two questions have
    /// different answers for the root identity: its secret key is pinned by a
    /// process-wide `OnceLock`, but its *second factor* is an ordinary record
    /// keyed on its access key. Conflating them would leave the default
    /// deployment — a root administrator signing into the console — unable to
    /// protect the one login that matters most.
    pub(crate) mfa_denial: Option<CredentialMutationDenial>,
}

impl CallerIdentity {
    /// Authenticate the request and resolve who it acts as.
    ///
    /// Authentication only: callers that need an authorization decision must
    /// still gate on an admin action. Self-service endpoints deliberately do
    /// not, because every authenticated identity may inspect and manage itself.
    pub(crate) async fn resolve(req: &S3Request<Body>) -> S3Result<Self> {
        let Some(input_cred) = req.credentials.as_ref() else {
            return Err(s3::error(S3ErrorCode::InvalidRequest, "authentication required"));
        };

        let (credentials, is_owner) = authenticate_request(&req.headers, &req.uri, input_cred).await?;
        Ok(Self::from_credentials(credentials, is_owner))
    }

    fn from_credentials(credentials: Credentials, is_owner: bool) -> Self {
        let presented_access_key = credentials.access_key.clone();
        let is_service_account = credentials.is_service_account();
        let is_temp = credentials.is_temp();
        let federated = is_federated_session(&credentials);

        let parent = session_parent_identity(&credentials).map(str::to_owned);

        // A derived credential acts as its parent; a long-term one acts as
        // itself. `is_service_account` is checked first because service-account
        // credentials also carry a session token and would otherwise look
        // temporary.
        let (identity_type, access_key, unresolved_parent) = if is_service_account {
            match parent {
                Some(parent) => (IdentityType::ServiceAccount, parent, false),
                None => (IdentityType::ServiceAccount, presented_access_key.clone(), true),
            }
        } else if is_temp {
            match parent {
                Some(parent) => (IdentityType::Sts, parent, false),
                None => (IdentityType::Sts, presented_access_key.clone(), true),
            }
        } else {
            (IdentityType::Iam, presented_access_key.clone(), false)
        };

        let is_root = current_action_credentials().is_some_and(|root| constant_time_eq(&root.access_key, &access_key));

        let identity_type = if is_root && matches!(identity_type, IdentityType::Iam) {
            IdentityType::Root
        } else {
            identity_type
        };

        let credentials_source = if is_root {
            CredentialsSource::Env
        } else {
            CredentialsSource::Iam
        };

        // Order matters: report the most specific reason a caller will act on.
        // "You are a service account" is more actionable than "your parent is
        // root", and an unresolved parent must never fall through to a
        // permissive answer.
        //
        // These three denials apply to both capabilities: a machine credential
        // must not take over its parent, a federated identity is owned by its
        // IdP, and an unresolvable parent fails closed.
        let shared_denial = if unresolved_parent {
            Some(CredentialMutationDenial::UnresolvedParent)
        } else if is_service_account {
            Some(CredentialMutationDenial::ServiceAccount)
        } else if federated {
            Some(CredentialMutationDenial::FederatedIdentity)
        } else {
            None
        };

        // Root additionally cannot rotate its secret — but it can still enroll a
        // second factor, which is the whole point of the feature for a default
        // deployment.
        let mutation_denial = shared_denial.or(if is_root {
            Some(CredentialMutationDenial::RootIsEnvironmentProvisioned)
        } else {
            None
        });
        let mfa_denial = shared_denial;

        let session_access_key = (presented_access_key != access_key).then_some(presented_access_key);

        Self {
            access_key,
            identity_type,
            session_access_key,
            credentials_source,
            credentials,
            is_owner,
            mutation_denial,
            mfa_denial,
        }
    }

    /// Which self-service mutations the server will accept for this caller.
    ///
    /// Reported to clients so they can disable a control instead of offering a
    /// request that is guaranteed to fail.
    pub(crate) const fn mutability(&self) -> AccountMutability {
        match self.mutation_denial {
            Some(_) => AccountMutability {
                password: false,
                username: false,
            },
            // Renaming an identity is not a supported mutation for anyone yet:
            // the access key is the primary key for policy mappings, group
            // membership, service-account parents and bucket-policy principals,
            // so a rename is a migration rather than an edit.
            None => AccountMutability {
                password: true,
                username: false,
            },
        }
    }

    /// `Ok(())` when this caller may rotate its own secret.
    pub(crate) fn ensure_credential_mutation_allowed(&self) -> S3Result<()> {
        match self.mutation_denial {
            None => Ok(()),
            Some(denial) => Err(s3::error(S3ErrorCode::InvalidRequest, denial.message())),
        }
    }

    /// `Ok(())` when this caller may manage its own second factor.
    ///
    /// Deliberately more permissive than [`Self::ensure_credential_mutation_allowed`]:
    /// a root identity may enroll even though it cannot change its password.
    pub(crate) fn ensure_mfa_management_allowed(&self) -> S3Result<()> {
        match self.mfa_denial {
            None => Ok(()),
            Some(denial) => Err(s3::error(S3ErrorCode::InvalidRequest, denial.message())),
        }
    }
}

/// Whether the session was minted by an external identity provider.
///
/// Such sessions have no RustFS-held long-term secret, so a password change has
/// nothing to change and a TOTP enrollment would not be consulted at login —
/// the IdP owns both.
fn is_federated_session(credentials: &Credentials) -> bool {
    let Some(claims) = credentials.claims.as_ref() else {
        return false;
    };

    is_rustfs_oidc_claims(claims) || claims.contains_key(OIDC_VIRTUAL_PARENT_CLAIM) || claims.contains_key(KEYSTONE_ROLES_CLAIM)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustfs_credentials::IAM_POLICY_CLAIM_NAME_SA;
    use serde_json::Value;
    use std::collections::HashMap;

    fn long_term(access_key: &str) -> Credentials {
        Credentials {
            access_key: access_key.to_string(),
            secret_key: "secret-key-value".to_string(),
            ..Default::default()
        }
    }

    fn sts_session(access_key: &str, parent: &str) -> Credentials {
        Credentials {
            access_key: access_key.to_string(),
            secret_key: "session-secret".to_string(),
            session_token: "token".to_string(),
            parent_user: parent.to_string(),
            ..Default::default()
        }
    }

    fn service_account(access_key: &str, parent: &str) -> Credentials {
        let mut claims = HashMap::new();
        claims.insert(IAM_POLICY_CLAIM_NAME_SA.to_string(), Value::String("inherited".to_string()));
        Credentials {
            access_key: access_key.to_string(),
            secret_key: "svc-secret".to_string(),
            session_token: "token".to_string(),
            parent_user: parent.to_string(),
            claims: Some(claims),
            ..Default::default()
        }
    }

    #[test]
    fn long_term_iam_user_acts_as_itself_and_may_change_its_password() {
        let caller = CallerIdentity::from_credentials(long_term("sinan"), false);

        assert_eq!(caller.access_key, "sinan");
        assert_eq!(caller.identity_type, IdentityType::Iam);
        assert!(caller.session_access_key.is_none());
        assert_eq!(caller.credentials_source, CredentialsSource::Iam);
        assert!(caller.mutation_denial.is_none());
        assert!(caller.mutability().password);
        // Rename stays unsupported even for the cases password change allows.
        assert!(!caller.mutability().username);
        assert!(caller.ensure_credential_mutation_allowed().is_ok());
    }

    #[test]
    fn sts_session_acts_as_its_parent() {
        // The Console only ever holds STS credentials, so this is the path that
        // every real "change my password" request takes.
        let caller = CallerIdentity::from_credentials(sts_session("TEMPKEY", "sinan"), false);

        assert_eq!(caller.access_key, "sinan");
        assert_eq!(caller.identity_type, IdentityType::Sts);
        assert_eq!(caller.session_access_key.as_deref(), Some("TEMPKEY"));
        assert!(caller.mutation_denial.is_none());
        assert!(caller.mutability().password);
    }

    #[test]
    fn sts_session_falls_back_to_the_jwt_parent_claim() {
        let mut credentials = sts_session("TEMPKEY", "");
        let mut claims = HashMap::new();
        claims.insert("parent".to_string(), Value::String("sinan".to_string()));
        credentials.claims = Some(claims);

        let caller = CallerIdentity::from_credentials(credentials, false);

        assert_eq!(caller.access_key, "sinan");
        assert_eq!(caller.identity_type, IdentityType::Sts);
        assert!(caller.mutation_denial.is_none());
    }

    #[test]
    fn a_root_identity_may_enroll_a_second_factor_even_though_its_password_is_fixed() {
        // The case that matters most: the default deployment signs into the
        // console as root, so refusing enrollment here would leave the one login
        // the feature exists to protect unprotected.
        let mut caller = CallerIdentity::from_credentials(long_term("rustfsadmin"), true);
        caller.identity_type = IdentityType::Root;
        caller.credentials_source = CredentialsSource::Env;
        caller.mutation_denial = Some(CredentialMutationDenial::RootIsEnvironmentProvisioned);
        caller.mfa_denial = None;

        assert!(caller.ensure_credential_mutation_allowed().is_err());
        assert!(caller.ensure_mfa_management_allowed().is_ok());
        assert!(!caller.mutability().password);
    }

    #[test]
    fn a_service_account_may_manage_neither() {
        let caller = CallerIdentity::from_credentials(service_account("SVCKEY", "sinan"), false);

        assert!(caller.ensure_credential_mutation_allowed().is_err());
        assert!(caller.ensure_mfa_management_allowed().is_err());
    }

    #[test]
    fn an_ordinary_iam_user_may_manage_both() {
        let caller = CallerIdentity::from_credentials(long_term("sinan"), false);

        assert!(caller.ensure_credential_mutation_allowed().is_ok());
        assert!(caller.ensure_mfa_management_allowed().is_ok());
    }

    #[test]
    fn service_account_may_not_mutate_its_parent() {
        let caller = CallerIdentity::from_credentials(service_account("SVCKEY", "sinan"), false);

        assert_eq!(caller.access_key, "sinan");
        assert_eq!(caller.identity_type, IdentityType::ServiceAccount);
        assert_eq!(caller.mutation_denial, Some(CredentialMutationDenial::ServiceAccount));
        assert!(!caller.mutability().password);
        assert!(caller.ensure_credential_mutation_allowed().is_err());
    }

    #[test]
    fn oidc_session_is_reported_as_federated() {
        let mut credentials = sts_session("TEMPKEY", "oidc-parent");
        let mut claims = HashMap::new();
        claims.insert("iss".to_string(), Value::String("rustfs-oidc".to_string()));
        claims.insert("oidc_provider".to_string(), Value::String("keycloak".to_string()));
        claims.insert("sub".to_string(), Value::String("user-123".to_string()));
        credentials.claims = Some(claims);

        let caller = CallerIdentity::from_credentials(credentials, false);

        assert_eq!(caller.mutation_denial, Some(CredentialMutationDenial::FederatedIdentity));
        assert!(!caller.mutability().password);
    }

    #[test]
    fn keystone_session_is_reported_as_federated() {
        let mut credentials = sts_session("TEMPKEY", "keystone-parent");
        let mut claims = HashMap::new();
        claims.insert(KEYSTONE_ROLES_CLAIM.to_string(), Value::Array(vec![]));
        credentials.claims = Some(claims);

        let caller = CallerIdentity::from_credentials(credentials, false);

        assert_eq!(caller.mutation_denial, Some(CredentialMutationDenial::FederatedIdentity));
    }

    #[test]
    fn derived_credential_without_a_parent_is_denied_rather_than_allowed() {
        // Fail closed: an unresolvable parent must not be treated as a
        // long-term identity acting on itself.
        let caller = CallerIdentity::from_credentials(sts_session("TEMPKEY", ""), false);

        assert_eq!(caller.mutation_denial, Some(CredentialMutationDenial::UnresolvedParent));
        assert!(!caller.mutability().password);
    }

    #[test]
    fn session_parent_identity_prefers_the_parent_user_field() {
        let mut credentials = sts_session("TEMPKEY", "field-parent");
        let mut claims = HashMap::new();
        claims.insert("parent".to_string(), Value::String("claim-parent".to_string()));
        credentials.claims = Some(claims);

        assert_eq!(session_parent_identity(&credentials), Some("field-parent"));
    }
}
