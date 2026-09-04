#![allow(unused_imports)]
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
#![allow(unused_variables)]
#![allow(unused_mut)]
#![allow(unused_assignments)]
#![allow(unused_must_use)]
#![allow(clippy::all)]

use serde::{Deserialize, Deserializer, Serialize, Serializer, de};

#[derive(Serialize, Deserialize, Default, Clone)]
#[serde(default)]
pub struct TierServicePrincipalAuth {
    #[serde(rename = "TenantID", alias = "tenantID", alias = "tenant_id")]
    pub tenant_id: String,
    #[serde(rename = "ClientID", alias = "clientID", alias = "client_id")]
    pub client_id: String,
    #[serde(rename = "ClientSecret", alias = "clientSecret", alias = "client_secret")]
    pub client_secret: String,
}

impl TierServicePrincipalAuth {
    pub(crate) fn is_empty(&self) -> bool {
        self.tenant_id.is_empty() && self.client_id.is_empty() && self.client_secret.is_empty()
    }
}

impl std::fmt::Debug for TierServicePrincipalAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TierServicePrincipalAuth")
            .field("tenant_id", &self.tenant_id)
            .field("client_id", &self.client_id)
            .field("client_secret", &"REDACTED")
            .finish()
    }
}

#[derive(Serialize, Deserialize, Default, Clone)]
#[serde(default)]
pub struct TierCreds {
    #[serde(rename = "access", alias = "accessKey")]
    pub access_key: String,
    #[serde(rename = "secret", alias = "secretKey")]
    pub secret_key: String,

    #[serde(rename = "awsrole", alias = "awsRole")]
    pub aws_role: bool,
    #[serde(rename = "awsroleWebIdentity", alias = "awsRoleWebIdentityTokenFile")]
    pub aws_role_web_identity_token_file: String,
    #[serde(rename = "awsroleARN", alias = "awsRoleArn", alias = "awsRoleARN")]
    pub aws_role_arn: String,

    #[serde(rename = "azSP", alias = "azsp", skip_serializing_if = "TierServicePrincipalAuth::is_empty")]
    pub azure_service_principal: TierServicePrincipalAuth,

    #[serde(
        rename = "creds",
        alias = "credsJson",
        alias = "credsJSON",
        alias = "creds_json",
        default,
        skip_serializing_if = "Vec::is_empty",
        with = "base64_bytes"
    )]
    pub creds_json: Vec<u8>,
}

impl std::fmt::Debug for TierCreds {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TierCreds")
            .field("access_key", &self.access_key)
            .field("secret_key", &"REDACTED")
            .field("aws_role", &self.aws_role)
            .field(
                "aws_role_web_identity_token_file",
                &(!self.aws_role_web_identity_token_file.is_empty()).then_some("REDACTED"),
            )
            .field("aws_role_arn", &self.aws_role_arn)
            .field("azure_service_principal", &self.azure_service_principal)
            .field("creds_json", &(!self.creds_json.is_empty()).then_some("REDACTED"))
            .finish()
    }
}

mod base64_bytes {
    use super::*;

    pub(super) fn serialize<S>(value: &[u8], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&base64_simd::STANDARD.encode_to_string(value))
    }

    pub(super) fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum EncodedBytes {
            Base64(String),
            Legacy(Vec<u8>),
        }

        match EncodedBytes::deserialize(deserializer)? {
            EncodedBytes::Base64(value) => base64_simd::STANDARD
                .decode_to_vec(value.as_bytes())
                .or_else(|_| base64_simd::STANDARD_NO_PAD.decode_to_vec(value.as_bytes()))
                .or_else(|_| base64_simd::URL_SAFE.decode_to_vec(value.as_bytes()))
                .or_else(|_| base64_simd::URL_SAFE_NO_PAD.decode_to_vec(value.as_bytes()))
                .map_err(de::Error::custom),
            EncodedBytes::Legacy(value) => Ok(value),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tier_creds_accepts_madmin_wire_names_and_base64_gcs_json() {
        let service_account = r#"{"type":"service_account","project_id":"tier-🚀x"}"#.as_bytes();
        let encoded = "eyJ0eXBlIjoic2VydmljZV9hY2NvdW50IiwicHJvamVjdF9pZCI6InRpZXIt8J+agHgifQ==";
        let creds: TierCreds = serde_json::from_value(serde_json::json!({
            "access": "access",
            "secret": "secret",
            "awsrole": false,
            "creds": encoded,
        }))
        .expect("madmin tier credentials should decode");

        assert_eq!(creds.access_key, "access");
        assert_eq!(creds.secret_key, "secret");
        assert_eq!(creds.creds_json.as_slice(), &service_account[..]);

        let wire = serde_json::to_value(&creds).expect("madmin tier credentials should encode");
        assert_eq!(wire["access"], "access");
        assert_eq!(wire["secret"], "secret");
        assert_eq!(wire["creds"], encoded);
        assert!(wire.get("accessKey").is_none());
        assert!(wire.get("secretKey").is_none());

        let legacy: TierCreds = serde_json::from_value(serde_json::json!({
            "accessKey": "legacy-access",
            "secretKey": "legacy-secret",
            "credsJson": service_account,
        }))
        .expect("the former RustFS field names and byte-array encoding should remain readable");
        assert_eq!(legacy.access_key, "legacy-access");
        assert_eq!(legacy.secret_key, "legacy-secret");
        assert_eq!(legacy.creds_json.as_slice(), &service_account[..]);
    }

    #[test]
    fn tier_creds_accepts_all_supported_base64_alphabets_and_padding_modes() {
        let service_account = r#"{"type":"service_account","project_id":"tier-🚀"}"#.as_bytes();
        for encoder in [
            base64_simd::STANDARD,
            base64_simd::STANDARD_NO_PAD,
            base64_simd::URL_SAFE,
            base64_simd::URL_SAFE_NO_PAD,
        ] {
            let encoded = encoder.encode_to_string(service_account);
            let creds: TierCreds = serde_json::from_value(serde_json::json!({ "creds": encoded }))
                .expect("all supported madmin base64 forms should decode");
            assert_eq!(creds.creds_json, service_account);
        }
    }

    #[test]
    fn tier_creds_debug_redacts_secret_payloads() {
        let creds = TierCreds {
            access_key: "access".to_string(),
            secret_key: "tier-secret-value".to_string(),
            aws_role_web_identity_token_file: "/var/run/private-token".to_string(),
            creds_json: br#"{"private_key":"gcs-private-key-value"}"#.to_vec(),
            ..Default::default()
        };

        let rendered = format!("{creds:?}");
        assert!(!rendered.contains("tier-secret-value"));
        assert!(!rendered.contains("/var/run/private-token"));
        assert!(!rendered.contains("gcs-private-key-value"));
    }

    #[test]
    fn tier_creds_accepts_canonical_madmin_azure_service_principal_wire_shape() {
        let creds: TierCreds = serde_json::from_value(serde_json::json!({
            "azSP": {
                "TenantID": "tenant",
                "ClientID": "client",
                "ClientSecret": "service-principal-secret"
            }
        }))
        .expect("canonical madmin azure service principal credentials should decode");

        assert_eq!(creds.azure_service_principal.tenant_id, "tenant");
        assert_eq!(creds.azure_service_principal.client_id, "client");
        assert_eq!(creds.azure_service_principal.client_secret, "service-principal-secret");
        let wire = serde_json::to_value(&creds).expect("canonical madmin credentials should encode");
        assert_eq!(wire["azSP"]["TenantID"], "tenant");
        assert_eq!(wire["azSP"]["ClientID"], "client");
        assert_eq!(wire["azSP"]["ClientSecret"], "service-principal-secret");
        assert!(!format!("{creds:?}").contains("service-principal-secret"));
    }
}
