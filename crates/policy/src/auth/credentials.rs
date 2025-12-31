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

use crate::error::{Error, Result};
use crate::policy::{Policy, Validator};
use crate::utils;
use rustfs_credentials::Credentials;
use serde::Serialize;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::convert::TryFrom;
use time::OffsetDateTime;
use tracing::warn;

const ACCESS_KEY_MIN_LEN: usize = 3;
const ACCESS_KEY_MAX_LEN: usize = 20;
const SECRET_KEY_MIN_LEN: usize = 8;
const SECRET_KEY_MAX_LEN: usize = 40;

pub const ACCOUNT_ON: &str = "on";
pub const ACCOUNT_OFF: &str = "off";

const RESERVED_CHARS: &str = "=,";

/// ContainsReservedChars - returns whether the input string contains reserved characters.
///
/// # Arguments
/// * `s` - input string to check.
///
/// # Returns
/// * `bool` - true if contains reserved characters, false otherwise.
///
pub fn contains_reserved_chars(s: &str) -> bool {
    s.contains(RESERVED_CHARS)
}

/// IsAccessKeyValid - validate access key for right length.
///
/// # Arguments
/// * `access_key` - access key to validate.
///
/// # Returns
/// * `bool` - true if valid, false otherwise.
///
pub fn is_access_key_valid(access_key: &str) -> bool {
    access_key.len() >= ACCESS_KEY_MIN_LEN
}

/// IsSecretKeyValid - validate secret key for right length.
///
/// # Arguments
/// * `secret_key` - secret key to validate.
///
/// # Returns
/// * `bool` - true if valid, false otherwise.
///
pub fn is_secret_key_valid(secret_key: &str) -> bool {
    secret_key.len() >= SECRET_KEY_MIN_LEN
}

/// GenerateCredentials - generate a new access key and secret key pair.
///
/// # Returns
/// * `Ok((String, String))` - access key and secret key pair.
/// * `Err(Error)` - if an error occurs during generation.
///
pub fn generate_credentials() -> Result<(String, String)> {
    let ak = rustfs_credentials::gen_access_key(20)?;
    let sk = rustfs_credentials::gen_secret_key(40)?;
    Ok((ak, sk))
}

/// GetNewCredentialsWithMetadata - generate new credentials with metadata claims and token secret.
///
/// # Arguments
/// * `claims` - metadata claims to be included in the token.
/// * `token_secret` - secret used to sign the token.
///
/// # Returns
/// * `Ok(Credentials)` - newly generated credentials.
/// * `Err(Error)` - if an error occurs during generation.
///
pub fn get_new_credentials_with_metadata(claims: &HashMap<String, Value>, token_secret: &str) -> Result<Credentials> {
    let (ak, sk) = generate_credentials()?;

    create_new_credentials_with_metadata(&ak, &sk, claims, token_secret)
}

/// CreateNewCredentialsWithMetadata - create new credentials with provided access key, secret key, metadata claims, and token secret.
///
/// # Arguments
/// * `ak` - access key.
/// * `sk` - secret key.
/// * `claims` - metadata claims to be included in the token.
/// * `token_secret` - secret used to sign the token.
///
/// # Returns
/// * `Ok(Credentials)` - newly created credentials.
/// * `Err(Error)` - if an error occurs during creation.
///
pub fn create_new_credentials_with_metadata(
    ak: &str,
    sk: &str,
    claims: &HashMap<String, Value>,
    token_secret: &str,
) -> Result<Credentials> {
    if ak.len() < ACCESS_KEY_MIN_LEN || ak.len() > ACCESS_KEY_MAX_LEN {
        return Err(Error::InvalidAccessKeyLength);
    }

    if sk.len() < SECRET_KEY_MIN_LEN || sk.len() > SECRET_KEY_MAX_LEN {
        return Err(Error::InvalidAccessKeyLength);
    }

    if token_secret.is_empty() {
        return Ok(Credentials {
            access_key: ak.to_owned(),
            secret_key: sk.to_owned(),
            status: ACCOUNT_OFF.to_owned(),
            ..Default::default()
        });
    }

    let expiration = {
        if let Some(v) = claims.get("exp") {
            if let Some(expiry) = v.as_i64() {
                Some(OffsetDateTime::from_unix_timestamp(expiry)?)
            } else {
                None
            }
        } else {
            None
        }
    };

    warn!("create_new_credentials_with_metadata expiration {expiration:?}, access_key: {ak}");

    let token = utils::generate_jwt(&claims, token_secret)?;

    Ok(Credentials {
        access_key: ak.to_owned(),
        secret_key: sk.to_owned(),
        session_token: token,
        status: ACCOUNT_ON.to_owned(),
        expiration,
        ..Default::default()
    })
}

/// JWTSign - sign the provided claims with the given token secret to generate a JWT token.
///
/// # Arguments
/// * `claims` - claims to be included in the token.
/// * `token_secret` - secret used to sign the token.
///
/// # Returns
/// * `Ok(String)` - generated JWT token.
/// * `Err(Error)` - if an error occurs during signing.
///
pub fn jwt_sign<T: Serialize>(claims: &T, token_secret: &str) -> Result<String> {
    let token = utils::generate_jwt(claims, token_secret)?;
    Ok(token)
}

#[derive(Default)]
pub struct CredentialsBuilder {
    session_policy: Option<Policy>,
    access_key: String,
    secret_key: String,
    name: Option<String>,
    description: Option<String>,
    expiration: Option<OffsetDateTime>,
    allow_site_replicator_account: bool,
    claims: Option<Value>,
    parent_user: String,
    groups: Option<Vec<String>>,
}

impl CredentialsBuilder {
    /// Create a new CredentialsBuilder instance.
    ///
    /// # Returns
    /// * `CredentialsBuilder` - a new instance of CredentialsBuilder.
    ///
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the session policy for the credentials.
    ///
    /// # Arguments
    /// * `policy` - an optional Policy to set as the session policy.
    ///
    /// # Returns
    /// * `Self` - the updated CredentialsBuilder instance.
    ///
    pub fn session_policy(mut self, policy: Option<Policy>) -> Self {
        self.session_policy = policy;
        self
    }

    pub fn access_key(mut self, access_key: String) -> Self {
        self.access_key = access_key;
        self
    }

    pub fn secret_key(mut self, secret_key: String) -> Self {
        self.secret_key = secret_key;
        self
    }

    pub fn name(mut self, name: String) -> Self {
        self.name = Some(name);
        self
    }

    pub fn description(mut self, description: String) -> Self {
        self.description = Some(description);
        self
    }

    pub fn expiration(mut self, expiration: Option<OffsetDateTime>) -> Self {
        self.expiration = expiration;
        self
    }

    pub fn allow_site_replicator_account(mut self, allow_site_replicator_account: bool) -> Self {
        self.allow_site_replicator_account = allow_site_replicator_account;
        self
    }

    pub fn claims(mut self, claims: Value) -> Self {
        self.claims = Some(claims);
        self
    }

    pub fn parent_user(mut self, parent_user: String) -> Self {
        self.parent_user = parent_user;
        self
    }

    pub fn groups(mut self, groups: Vec<String>) -> Self {
        self.groups = Some(groups);
        self
    }

    pub fn try_build(self) -> Result<Credentials> {
        self.try_into()
    }
}

impl TryFrom<CredentialsBuilder> for Credentials {
    type Error = Error;
    fn try_from(mut value: CredentialsBuilder) -> std::result::Result<Self, Self::Error> {
        if value.parent_user.is_empty() {
            return Err(Error::InvalidArgument);
        }

        if (value.access_key.is_empty() && !value.secret_key.is_empty())
            || (!value.access_key.is_empty() && value.secret_key.is_empty())
        {
            return Err(Error::other("Either ak or sk is empty"));
        }

        if value.parent_user == value.access_key.as_str() {
            return Err(Error::InvalidArgument);
        }

        if value.access_key == "site-replicator-0" && !value.allow_site_replicator_account {
            return Err(Error::InvalidArgument);
        }

        let mut claim = json!({
            "parent": value.parent_user
        });

        if let Some(p) = value.session_policy {
            p.is_valid()?;
            let policy_buf = serde_json::to_vec(&p).map_err(|_| Error::InvalidArgument)?;
            if policy_buf.len() > 4096 {
                return Err(Error::other("session policy is too large"));
            }
            claim["sessionPolicy"] = json!(base64_simd::STANDARD.encode_to_string(&policy_buf));
            claim[rustfs_credentials::IAM_POLICY_CLAIM_NAME_SA] = json!(rustfs_credentials::EMBEDDED_POLICY_TYPE);
        } else {
            claim[rustfs_credentials::IAM_POLICY_CLAIM_NAME_SA] = json!(rustfs_credentials::INHERITED_POLICY_TYPE);
        }

        if let Some(Value::Object(obj)) = value.claims {
            for (key, value) in obj {
                if claim.get(&key).is_some() {
                    continue;
                }
                claim[key] = value;
            }
        }

        if value.access_key.is_empty() {
            value.access_key = rustfs_credentials::gen_access_key(20)?;
        }

        if value.secret_key.is_empty() {
            value.secret_key = rustfs_credentials::gen_secret_key(40)?;
        }

        claim["accessKey"] = json!(&value.access_key);

        let mut cred = Credentials {
            status: "on".into(),
            parent_user: value.parent_user,
            groups: value.groups,
            name: value.name,
            description: value.description,
            ..Default::default()
        };

        if !value.secret_key.is_empty() {
            let session_token = rustfs_crypto::jwt_encode(value.access_key.as_bytes(), &claim)
                .map_err(|_| Error::other("session policy is too large"))?;
            cred.session_token = session_token;
            // cred.expiration = Some(
            //     OffsetDateTime::from_unix_timestamp(
            //         claim
            //             .get("exp")
            //             .and_then(|x| x.as_i64())
            //             .ok_or(Error::StringError("invalid exp".into()))?,
            //     )
            //     .map_err(|_| Error::StringError("invalie timestamp".into()))?,
            // );
        } else {
            // cred.expiration =
            // Some(OffsetDateTime::from_unix_timestamp(0).map_err(|_| Error::StringError("invalie timestamp".into()))?);
        }

        cred.expiration = value.expiration;
        cred.access_key = value.access_key;
        cred.secret_key = value.secret_key;

        Ok(cred)
    }
}

// #[cfg(test)]
// #[allow(non_snake_case)]
// mod tests {
//     use test_case::test_case;
//     use time::Date;

//     use super::CredentialHeader;
//     use super::CredentialHeaderScope;
//     use crate::service_type::ServiceType;

//     #[test_case(
//         "Credential=aaaaaaaaaaaaaaaaaaaa/20241127/us-east-1/s3/aws4_request" =>
//         CredentialHeader{
//             access_key: "aaaaaaaaaaaaaaaaaaaa".into(),
//             scop: CredentialHeaderScope {
//                 date: Date::from_calendar_date(2024, time::Month::November, 27).unwrap(),
//                 region: "us-east-1".to_owned(),
//                 service: ServiceType::S3,
//                 request: "aws4_request".into(),
//             }
//         };
//         "1")]
//     #[test_case(
//         "Credential=aaaaaaaaaaa/aaaaaaaaa/20241127/us-east-1/s3/aws4_request" =>
//         CredentialHeader{
//             access_key: "aaaaaaaaaaa/aaaaaaaaa".into(),
//             scop: CredentialHeaderScope {
//                 date: Date::from_calendar_date(2024, time::Month::November, 27).unwrap(),
//                 region: "us-east-1".to_owned(),
//                 service: ServiceType::S3,
//                 request: "aws4_request".into(),
//             }
//         };
//         "2")]
//     #[test_case(
//         "Credential=aaaaaaaaaaa/aaaaaaaaa/20241127/us-east-1/sts/aws4_request" =>
//         CredentialHeader{
//             access_key: "aaaaaaaaaaa/aaaaaaaaa".into(),
//             scop: CredentialHeaderScope {
//                 date: Date::from_calendar_date(2024, time::Month::November, 27).unwrap(),
//                 region: "us-east-1".to_owned(),
//                 service: ServiceType::STS,
//                 request: "aws4_request".into(),
//             }
//         };
//         "3")]
//     fn test_CredentialHeader_from_str_successful(input: &str) -> CredentialHeader {
//         CredentialHeader::try_from(input).unwrap()
//     }

//     #[test_case("Credential")]
//     #[test_case("Cred=")]
//     #[test_case("Credential=abc")]
//     #[test_case("Credential=a/20241127/us-east-1/s3/aws4_request")]
//     #[test_case("Credential=aa/20241127/us-east-1/s3/aws4_request")]
//     #[test_case("Credential=aaaa/20241127/us-east-1/asa/aws4_request")]
//     #[test_case("Credential=aaaa/20241127/us-east-1/sts/aws4a_request")]
//     fn test_credential_header_from_str_failed(input: &str) {
//         if CredentialHeader::try_from(input).is_ok() {
//             unreachable!()
//         }
//     }
// }
