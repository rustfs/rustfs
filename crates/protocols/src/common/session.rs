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

use rustfs_credentials::Credentials;
use rustfs_policy::auth::UserIdentity;
use std::net::IpAddr;
#[cfg(test)]
use std::net::Ipv4Addr;
use std::sync::Arc;

/// Protocol types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Protocol {
    Ftps,
    Swift,
    WebDav,
    Sftp,
    Tftp,
}

/// Protocol principal representing an authenticated user
#[derive(Debug, Clone)]
pub struct ProtocolPrincipal {
    /// User identity from IAM system
    pub user_identity: Arc<UserIdentity>,
}

impl ProtocolPrincipal {
    pub fn new(user_identity: Arc<UserIdentity>) -> Self {
        Self { user_identity }
    }
    pub fn access_key(&self) -> &str {
        &self.user_identity.credentials.access_key
    }
}

/// Returns `true` when `credentials` are short-lived STS/AssumeRole credentials.
///
/// SECURITY: password-based protocol logins (FTPS, SFTP, WebDAV Basic) have nowhere to carry the
/// session token that an STS credential is only valid with, so accepting the access/secret pair
/// alone would authenticate the holder as the parent user with none of the session-policy
/// restrictions the token encodes. Such logins must be rejected.
///
/// Service accounts also hold a signed token, but their policy is resolved from stored IAM state
/// rather than from a token the client must present, so they stay eligible for these protocols.
/// The `is_temp() && !is_service_account()` pairing is the same STS discriminator the IAM manager
/// uses when routing an identity into the STS account cache.
pub fn is_temporary_credential(credentials: &Credentials) -> bool {
    credentials.is_temp() && !credentials.is_service_account()
}

/// Session context for protocol operations
#[derive(Debug, Clone)]
pub struct SessionContext {
    /// The protocol principal (authenticated user)
    pub principal: ProtocolPrincipal,
    /// The protocol type
    pub protocol: Protocol,
    /// The source IP address
    pub source_ip: IpAddr,
}

impl SessionContext {
    /// Create a new session context
    pub fn new(principal: ProtocolPrincipal, protocol: Protocol, source_ip: IpAddr) -> Self {
        Self {
            principal,
            protocol,
            source_ip,
        }
    }

    /// Get the access key for this session
    pub fn access_key(&self) -> &str {
        self.principal.access_key()
    }
}

/// Build a SessionContext suitable for driver-level unit tests. The
/// principal has an empty access key and an empty secret key. Auth
/// decisions in tests come from the gateway test override, not from
/// these credentials. The fields are inspected only when a test
/// specifically asserts on them. Callers pick the Protocol variant
/// that matches the driver under test.
#[cfg(test)]
pub fn test_session(protocol: Protocol) -> SessionContext {
    let principal = ProtocolPrincipal::new(Arc::new(UserIdentity::default()));
    SessionContext::new(principal, protocol, IpAddr::V4(Ipv4Addr::LOCALHOST))
}

#[cfg(test)]
mod regression_prevention {
    use super::*;
    use rustfs_credentials::{IAM_POLICY_CLAIM_NAME_SA, INHERITED_POLICY_TYPE};
    use serde_json::Value;
    use std::collections::HashMap;
    use time::{Duration, OffsetDateTime};

    fn service_account_claims() -> HashMap<String, Value> {
        let mut claims = HashMap::new();
        claims.insert(IAM_POLICY_CLAIM_NAME_SA.to_string(), Value::String(INHERITED_POLICY_TYPE.to_string()));
        claims
    }

    #[test]
    fn sts_credentials_are_temporary() {
        let sts = Credentials {
            access_key: "VV0V3VYJK2PV6EG45X2Y".to_string(),
            secret_key: "CS_TEST_SECRET_DO_NOT_LOG".to_string(),
            session_token: "jwt-session-token".to_string(),
            parent_user: "alice".to_string(),
            ..Default::default()
        };

        assert!(is_temporary_credential(&sts));
    }

    #[test]
    fn service_accounts_are_not_temporary() {
        // Service accounts also carry a signed token; they must keep working over FTPS/SFTP/WebDAV.
        let service_account = Credentials {
            access_key: "39KNO04Z34D6T4AGL6E6".to_string(),
            secret_key: "CS_TEST_SECRET_DO_NOT_LOG".to_string(),
            session_token: "jwt-session-token".to_string(),
            parent_user: "alice".to_string(),
            claims: Some(service_account_claims()),
            ..Default::default()
        };

        assert!(service_account.is_service_account());
        assert!(!is_temporary_credential(&service_account));
    }

    #[test]
    fn long_term_credentials_are_not_temporary() {
        let long_term = Credentials {
            access_key: "alice".to_string(),
            secret_key: "CS_TEST_SECRET_DO_NOT_LOG".to_string(),
            ..Default::default()
        };

        assert!(!is_temporary_credential(&long_term));
    }

    #[test]
    fn expired_sts_credentials_are_rejected_by_the_validity_gate() {
        // is_temp() goes false once the session expires, so the STS guard alone would let an
        // expired session in. The `is_valid` check every password path runs first is what covers
        // this case; assert both halves so neither can be dropped unnoticed.
        let expired = Credentials {
            access_key: "VV0V3VYJK2PV6EG45X2Y".to_string(),
            secret_key: "CS_TEST_SECRET_DO_NOT_LOG".to_string(),
            session_token: "jwt-session-token".to_string(),
            parent_user: "alice".to_string(),
            expiration: Some(OffsetDateTime::now_utc() - Duration::hours(1)),
            ..Default::default()
        };

        assert!(!is_temporary_credential(&expired));
        assert!(!expired.is_valid());
    }

    #[test]
    fn password_auth_paths_reject_temporary_credentials() {
        let guard = concat!("is_temporary_credential(&identity.", "credentials)");
        for (protocol, source) in [
            ("ftps", include_str!("../ftps/server.rs")),
            ("webdav", include_str!("../webdav/server.rs")),
            ("sftp", include_str!("../sftp/server.rs")),
        ] {
            let guard_at = source
                .find(guard)
                .unwrap_or_else(|| panic!("{protocol} password authentication must reject temporary STS credentials"));
            let accept_at = source
                .find(r#"result = "authenticated""#)
                .unwrap_or_else(|| panic!("{protocol} has no authenticated log line to anchor the guard against"));
            assert!(
                guard_at < accept_at,
                "{protocol} must reject temporary STS credentials before authentication succeeds"
            );
        }
    }

    // Compile-time check that every Protocol variant is acknowledged here.
    // This is intentionally an exhaustive match with no wildcard arm: if a
    // variant is added without being named, or if any variant is removed,
    // this test file will fail to compile.
    #[test]
    fn protocol_variants_are_named() {
        fn _check(protocol: Protocol) {
            match protocol {
                Protocol::Ftps => {}
                Protocol::Swift => {}
                Protocol::WebDav => {}
                Protocol::Sftp => {}
                Protocol::Tftp => {}
            }
        }
    }

    #[test]
    fn test_session_carries_supplied_protocol() {
        assert_eq!(test_session(Protocol::Sftp).protocol, Protocol::Sftp);
        assert_eq!(test_session(Protocol::Ftps).protocol, Protocol::Ftps);
    }
}
