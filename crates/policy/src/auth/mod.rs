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

mod credentials;

pub use credentials::*;

use rustfs_credentials::Credentials;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use time::OffsetDateTime;

/// A nonnil stored principal identifier; parsing it does not establish identity authority.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "uuid::Uuid", into = "uuid::Uuid")]
pub struct PrincipalIncarnation(uuid::Uuid);

#[derive(Debug, thiserror::Error)]
#[error("principal incarnation must not be nil")]
pub struct InvalidPrincipalIncarnation;

impl TryFrom<uuid::Uuid> for PrincipalIncarnation {
    type Error = InvalidPrincipalIncarnation;

    fn try_from(value: uuid::Uuid) -> Result<Self, Self::Error> {
        if value.is_nil() {
            Err(InvalidPrincipalIncarnation)
        } else {
            Ok(Self(value))
        }
    }
}

impl From<PrincipalIncarnation> for uuid::Uuid {
    fn from(value: PrincipalIncarnation) -> Self {
        value.0
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct UserIdentity {
    pub version: i64,
    pub credentials: Credentials,
    /// updatedAt (RFC3339), legacy RustFS: update_at. Serialize as updatedAt
    #[serde(rename = "updatedAt", alias = "update_at", default, with = "crate::serde_datetime::option")]
    pub update_at: Option<OffsetDateTime>,
    #[serde(default, rename = "principalIncarnation", skip_serializing_if = "Option::is_none")]
    pub principal_incarnation: Option<PrincipalIncarnation>,
}

impl UserIdentity {
    /// Create a new UserIdentity
    ///
    /// # Arguments
    /// * `credentials` - Credentials object
    ///
    /// # Returns
    /// * UserIdentity
    pub fn new(credentials: Credentials) -> Self {
        UserIdentity {
            version: 1,
            credentials,
            update_at: Some(OffsetDateTime::now_utc()),
            principal_incarnation: None,
        }
    }

    /// Add an SSH public key to user identity for SFTP authentication
    pub fn add_ssh_public_key(&mut self, public_key: &str) {
        self.credentials
            .claims
            .get_or_insert_with(HashMap::new)
            .insert("ssh_public_keys".to_string(), json!([public_key]));
    }

    /// Get all SSH public keys for user identity
    pub fn get_ssh_public_keys(&self) -> Vec<String> {
        self.credentials
            .claims
            .as_ref()
            .and_then(|claims| claims.get("ssh_public_keys"))
            .and_then(|keys| keys.as_array())
            .map(|arr| arr.iter().filter_map(|v| v.as_str()).map(String::from).collect())
            .unwrap_or_default()
    }
}

impl From<Credentials> for UserIdentity {
    fn from(value: Credentials) -> Self {
        UserIdentity {
            version: 1,
            credentials: value,
            update_at: Some(OffsetDateTime::now_utc()),
            principal_incarnation: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{PrincipalIncarnation, UserIdentity};
    use rustfs_credentials::Credentials;
    use serde_json::json;
    use uuid::Uuid;

    const INCARNATION: &str = "8f422eba-aab4-4ec3-a7ec-d1717463b520";

    #[test]
    fn test_user_identity_legacy_fields_absent() {
        let mut legacy = json!({"version": 1, "credentials": {"accessKey": "ak", "secretKey": "sk12345678"}});
        let identity: UserIdentity = serde_json::from_value(legacy.clone()).expect("decode legacy identity");
        assert!(identity.principal_incarnation.is_none());
        legacy["principalIncarnation"] = serde_json::Value::Null;
        let identity: UserIdentity = serde_json::from_value(legacy).expect("decode null incarnation");
        assert!(identity.principal_incarnation.is_none());
    }

    #[test]
    fn test_user_identity_legacy_serialization_shape() {
        let identity = UserIdentity::default();
        let serialized = serde_json::to_value(&identity).expect("serialize legacy identity");
        assert_eq!(serialized, json!({"version": 0, "credentials": identity.credentials, "updatedAt": null}));
        assert!(serialized.get("principalIncarnation").is_none());
    }

    #[test]
    fn test_user_identity_constructors_do_not_mint_incarnation() {
        let default = UserIdentity::default();
        assert!(default.principal_incarnation.is_none());
        assert_eq!(default.version, 0);
        assert!(default.update_at.is_none());
        let credentials = Credentials {
            access_key: "ak".to_string(),
            secret_key: "sk12345678".to_string(),
            ..Default::default()
        };
        let expected_credentials = serde_json::to_value(&credentials).expect("serialize expected credentials");
        for identity in [UserIdentity::new(credentials.clone()), UserIdentity::from(credentials)] {
            assert!(identity.principal_incarnation.is_none());
            assert_eq!(identity.version, 1);
            assert!(identity.update_at.is_some());
            assert_eq!(
                serde_json::to_value(&identity.credentials).expect("serialize constructor credentials"),
                expected_credentials
            );
            assert!(
                serde_json::to_value(identity)
                    .expect("serialize constructor identity")
                    .get("principalIncarnation")
                    .is_none()
            );
        }
    }

    #[test]
    fn test_principal_incarnation_rejects_nil_and_malformed() {
        assert!(PrincipalIncarnation::try_from(Uuid::nil()).is_err());
        for value in ["00000000-0000-0000-0000-000000000000", "not-a-uuid", ""] {
            assert!(serde_json::from_value::<PrincipalIncarnation>(json!(value)).is_err(), "reject {value}");
            let identity = json!({"version": 1, "credentials": {}, "principalIncarnation": value});
            assert!(serde_json::from_value::<UserIdentity>(identity).is_err(), "identity rejects {value}");
        }
    }

    #[test]
    fn test_principal_incarnation_roundtrip_preserves_uuid() {
        let uuid = Uuid::parse_str(INCARNATION).expect("parse fixed incarnation");
        let incarnation = PrincipalIncarnation::try_from(uuid).expect("accept nonnil incarnation");
        assert_eq!(Uuid::from(incarnation).as_bytes(), uuid.as_bytes());
        let serialized = serde_json::to_value(incarnation).expect("serialize incarnation");
        assert_eq!(serialized, json!(INCARNATION));
        assert_eq!(
            serde_json::from_value::<PrincipalIncarnation>(serialized).expect("decode incarnation"),
            incarnation
        );
    }

    #[test]
    fn test_user_identity_live_incarnation_roundtrip() {
        let mut identity = UserIdentity::new(Credentials::default());
        identity.principal_incarnation = Some(
            PrincipalIncarnation::try_from(Uuid::parse_str(INCARNATION).expect("parse fixed incarnation"))
                .expect("accept fixed incarnation"),
        );
        let serialized = serde_json::to_value(&identity).expect("serialize identity with incarnation");
        assert_eq!(serialized["principalIncarnation"], INCARNATION);
        let encoded = serde_json::to_string(&identity).expect("encode identity with incarnation");
        let decoded: UserIdentity = serde_json::from_str(&encoded).expect("decode identity with incarnation");
        assert_eq!(decoded.principal_incarnation, identity.principal_incarnation);
        assert_eq!(serde_json::to_value(decoded).expect("reserialize identity"), serialized);
    }

    #[test]
    fn test_user_identity_timestamp_aliases_unchanged() {
        let mut minio = json!({"version": 1, "credentials": {}, "updatedAt": "2025-03-07T12:00:00Z"});
        let encoded = serde_json::to_string(&minio).expect("encode updatedAt fixture");
        let identity: UserIdentity = serde_json::from_str(&encoded).expect("decode updatedAt timestamp");
        minio["update_at"] = minio
            .as_object_mut()
            .expect("identity object")
            .remove("updatedAt")
            .expect("existing timestamp");
        let encoded = serde_json::to_string(&minio).expect("encode update_at fixture");
        let legacy: UserIdentity = serde_json::from_str(&encoded).expect("decode update_at alias");
        assert_eq!(legacy.update_at, identity.update_at);
        assert_eq!(legacy.version, 1);
        let serialized = serde_json::to_value(legacy).expect("serialize legacy timestamp");
        assert_eq!(serialized["updatedAt"], "2025-03-07T12:00:00Z");
        assert!(serialized.get("update_at").is_none());
        assert!(serialized.get("principalIncarnation").is_none());
    }

    /// Deserialize UserIdentity from MinIO-style JSON (RFC3339 updatedAt).
    #[test]
    fn test_user_identity_deserialize_minio_style_rfc3339() {
        let minio_style =
            r#"{"version":1,"credentials":{"accessKey":"ak","secretKey":"sk12345678"},"updatedAt":"2025-03-07T12:00:00Z"}"#;
        let u: UserIdentity = serde_json::from_str(minio_style).expect("deserialize MinIO-style identity");
        assert_eq!(u.version, 1);
        assert_eq!(u.credentials.access_key, "ak");
        assert!(u.update_at.is_some());
    }
}
