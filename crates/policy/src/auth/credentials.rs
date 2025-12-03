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

use crate::error::Error as IamError;
use crate::error::{Error, Result};
use crate::policy::{INHERITED_POLICY_TYPE, Policy, Validator, iam_policy_claim_name_sa};
use crate::utils;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::HashMap;
use time::OffsetDateTime;
use time::macros::offset;
use tracing::warn;

const ACCESS_KEY_MIN_LEN: usize = 3;
const ACCESS_KEY_MAX_LEN: usize = 20;
const SECRET_KEY_MIN_LEN: usize = 8;
const SECRET_KEY_MAX_LEN: usize = 40;

pub const ACCOUNT_ON: &str = "on";
pub const ACCOUNT_OFF: &str = "off";

const RESERVED_CHARS: &str = "=,";

// ContainsReservedChars - returns whether the input string contains reserved characters.
pub fn contains_reserved_chars(s: &str) -> bool {
    s.contains(RESERVED_CHARS)
}

// IsAccessKeyValid - validate access key for right length.
pub fn is_access_key_valid(access_key: &str) -> bool {
    access_key.len() >= ACCESS_KEY_MIN_LEN
}

// IsSecretKeyValid - validate secret key for right length.
pub fn is_secret_key_valid(secret_key: &str) -> bool {
    secret_key.len() >= SECRET_KEY_MIN_LEN
}

// #[cfg_attr(test, derive(PartialEq, Eq, Debug))]
// struct CredentialHeader {
//     access_key: String,
//     scop: CredentialHeaderScope,
// }

// #[cfg_attr(test, derive(PartialEq, Eq, Debug))]
// struct CredentialHeaderScope {
//     date: Date,
//     region: String,
//     service: ServiceType,
//     request: String,
// }

// impl TryFrom<&str> for CredentialHeader {
//     type Error = Error;
//     fn try_from(value: &str) -> Result<Self, Self::Error> {
//         let mut elem = value.trim().splitn(2, '=');
//         let (Some(h), Some(cred_elems)) = (elem.next(), elem.next()) else {
//             return Err(IamError::ErrCredMalformed));
//         };

//         if h != "Credential" {
//             return Err(IamError::ErrCredMalformed));
//         }

//         let mut cred_elems = cred_elems.trim().rsplitn(5, '/');

//         let Some(request) = cred_elems.next() else {
//             return Err(IamError::ErrCredMalformed));
//         };

//         let Some(service) = cred_elems.next() else {
//             return Err(IamError::ErrCredMalformed));
//         };

//         let Some(region) = cred_elems.next() else {
//             return Err(IamError::ErrCredMalformed));
//         };

//         let Some(date) = cred_elems.next() else {
//             return Err(IamError::ErrCredMalformed));
//         };

//         let Some(ak) = cred_elems.next() else {
//             return Err(IamError::ErrCredMalformed));
//         };

//         if ak.len() < 3 {
//             return Err(IamError::ErrCredMalformed));
//         }

//         if request != "aws4_request" {
//             return Err(IamError::ErrCredMalformed));
//         }

//         Ok(CredentialHeader {
//             access_key: ak.to_owned(),
//             scop: CredentialHeaderScope {
//                 date: {
//                     const FORMATTER: LazyCell<Vec<BorrowedFormatItem<'static>>> =
//                         LazyCell::new(|| time::format_description::parse("[year][month][day]").unwrap());

//                     Date::parse(date, &FORMATTER).map_err(|_| IamError::ErrCredMalformed))?
//                 },
//                 region: region.to_owned(),
//                 service: service.try_into()?,
//                 request: request.to_owned(),
//             },
//         })
//     }
// }

#[derive(Serialize, Deserialize, Clone, Default, Debug)]
pub struct Credentials {
    pub access_key: String,
    pub secret_key: String,
    pub session_token: String,
    pub expiration: Option<OffsetDateTime>,
    pub status: String,
    pub parent_user: String,
    pub groups: Option<Vec<String>>,
    pub claims: Option<HashMap<String, Value>>,
    pub name: Option<String>,
    pub description: Option<String>,
}

impl Credentials {
    // pub fn new(elem: &str) -> Result<Self> {
    //     let header: CredentialHeader = elem.try_into()?;
    //     Self::check_key_value(header)
    // }

    // pub fn check_key_value(_header: CredentialHeader) -> Result<Self> {
    //     todo!()
    // }

    pub fn is_expired(&self) -> bool {
        if self.expiration.is_none() {
            return false;
        }

        self.expiration
            .as_ref()
            .map(|e| time::OffsetDateTime::now_utc() > *e)
            .unwrap_or(false)
    }

    pub fn is_temp(&self) -> bool {
        !self.session_token.is_empty() && !self.is_expired()
    }

    pub fn is_service_account(&self) -> bool {
        const IAM_POLICY_CLAIM_NAME_SA: &str = "sa-policy";
        self.claims
            .as_ref()
            .map(|x| x.get(IAM_POLICY_CLAIM_NAME_SA).is_some_and(|_| !self.parent_user.is_empty()))
            .unwrap_or_default()
    }

    pub fn is_implied_policy(&self) -> bool {
        if self.is_service_account() {
            return self
                .claims
                .as_ref()
                .map(|x| x.get(&iam_policy_claim_name_sa()).is_some_and(|v| v == INHERITED_POLICY_TYPE))
                .unwrap_or_default();
        }

        false
    }

    pub fn is_valid(&self) -> bool {
        if self.status == "off" {
            return false;
        }

        self.access_key.len() >= 3 && self.secret_key.len() >= 8 && !self.is_expired()
    }

    pub fn is_owner(&self) -> bool {
        false
    }
}

pub fn generate_credentials() -> Result<(String, String)> {
    let ak = utils::gen_access_key(20)?;
    let sk = utils::gen_secret_key(40)?;
    Ok((ak, sk))
}

pub fn get_new_credentials_with_metadata(claims: &HashMap<String, Value>, token_secret: &str) -> Result<Credentials> {
    let (ak, sk) = generate_credentials()?;

    create_new_credentials_with_metadata(&ak, &sk, claims, token_secret)
}

pub fn create_new_credentials_with_metadata(
    ak: &str,
    sk: &str,
    claims: &HashMap<String, Value>,
    token_secret: &str,
) -> Result<Credentials> {
    if ak.len() < ACCESS_KEY_MIN_LEN || ak.len() > ACCESS_KEY_MAX_LEN {
        return Err(IamError::InvalidAccessKeyLength);
    }

    if sk.len() < SECRET_KEY_MIN_LEN || sk.len() > SECRET_KEY_MAX_LEN {
        return Err(IamError::InvalidAccessKeyLength);
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
                Some(OffsetDateTime::from_unix_timestamp(expiry)?.to_offset(offset!(+8)))
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
    claims: Option<serde_json::Value>,
    parent_user: String,
    groups: Option<Vec<String>>,
}

impl CredentialsBuilder {
    pub fn new() -> Self {
        Self::default()
    }

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

    pub fn claims(mut self, claims: serde_json::Value) -> Self {
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
            return Err(IamError::InvalidArgument);
        }

        if (value.access_key.is_empty() && !value.secret_key.is_empty())
            || (!value.access_key.is_empty() && value.secret_key.is_empty())
        {
            return Err(Error::other("Either ak or sk is empty"));
        }

        if value.parent_user == value.access_key.as_str() {
            return Err(IamError::InvalidArgument);
        }

        if value.access_key == "site-replicator-0" && !value.allow_site_replicator_account {
            return Err(IamError::InvalidArgument);
        }

        let mut claim = serde_json::json!({
            "parent": value.parent_user
        });

        if let Some(p) = value.session_policy {
            p.is_valid()?;
            let policy_buf = serde_json::to_vec(&p).map_err(|_| IamError::InvalidArgument)?;
            if policy_buf.len() > 4096 {
                return Err(Error::other("session policy is too large"));
            }
            claim["sessionPolicy"] = serde_json::json!(base64_simd::STANDARD.encode_to_string(&policy_buf));
            claim["sa-policy"] = serde_json::json!("embedded-policy");
        } else {
            claim["sa-policy"] = serde_json::json!("inherited-policy");
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
            value.access_key = utils::gen_access_key(20)?;
        }

        if value.secret_key.is_empty() {
            value.access_key = utils::gen_secret_key(40)?;
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
