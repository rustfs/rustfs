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

use crate::policy::Error::{self, InvalidKeyName};
use serde::{Deserialize, Serialize};
use strum::{EnumString, IntoStaticStr};

#[derive(Clone, Eq, PartialEq, Debug, Serialize, Deserialize)]
#[serde(try_from = "&str", untagged)]
pub enum KeyName {
    Aws(AwsKeyName),
    Jwt(JwtKeyName),
    Ldap(LdapKeyName),
    Sts(StsKeyName),
    Svc(SvcKeyName),
    S3(S3KeyName),
}

impl TryFrom<&str> for KeyName {
    type Error = Error;
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Ok(if value.starts_with("s3:") {
            Self::S3(S3KeyName::try_from(value).map_err(|_| InvalidKeyName(value.into()))?)
        } else if value.starts_with("aws:") {
            Self::Aws(AwsKeyName::try_from(value).map_err(|_| InvalidKeyName(value.into()))?)
        } else if value.starts_with("ldap:") {
            Self::Ldap(LdapKeyName::try_from(value).map_err(|_| InvalidKeyName(value.into()))?)
        } else if value.starts_with("sts:") {
            Self::Sts(StsKeyName::try_from(value).map_err(|_| InvalidKeyName(value.into()))?)
        } else if value.starts_with("jwt:") {
            Self::Jwt(JwtKeyName::try_from(value).map_err(|_| InvalidKeyName(value.into()))?)
        } else if value.starts_with("svc:") {
            Self::Svc(SvcKeyName::try_from(value).map_err(|_| InvalidKeyName(value.into()))?)
        } else {
            Err(InvalidKeyName(value.into()))?
        })
    }
}

impl KeyName {
    pub const COMMON_KEYS: &'static [KeyName] = &[
        // s3
        KeyName::S3(S3KeyName::S3SignatureVersion),
        KeyName::S3(S3KeyName::S3AuthType),
        KeyName::S3(S3KeyName::S3SignatureAge),
        KeyName::S3(S3KeyName::S3XAmzContentSha256),
        KeyName::S3(S3KeyName::S3LocationConstraint),
        //aws
        KeyName::Aws(AwsKeyName::AWSReferer),
        KeyName::Aws(AwsKeyName::AWSSourceIP),
        KeyName::Aws(AwsKeyName::AWSUserAgent),
        KeyName::Aws(AwsKeyName::AWSSecureTransport),
        KeyName::Aws(AwsKeyName::AWSCurrentTime),
        KeyName::Aws(AwsKeyName::AWSEpochTime),
        KeyName::Aws(AwsKeyName::AWSPrincipalType),
        KeyName::Aws(AwsKeyName::AWSUserID),
        KeyName::Aws(AwsKeyName::AWSUsername),
        KeyName::Aws(AwsKeyName::AWSGroups),
        // ldap
        KeyName::Ldap(LdapKeyName::User),
        KeyName::Ldap(LdapKeyName::Username),
        KeyName::Ldap(LdapKeyName::Groups),
        // jwt
        KeyName::Jwt(JwtKeyName::JWTSub),
        KeyName::Jwt(JwtKeyName::JWTIss),
        KeyName::Jwt(JwtKeyName::JWTAud),
        KeyName::Jwt(JwtKeyName::JWTJti),
        KeyName::Jwt(JwtKeyName::JWTName),
        KeyName::Jwt(JwtKeyName::JWTUpn),
        KeyName::Jwt(JwtKeyName::JWTGroups),
        KeyName::Jwt(JwtKeyName::JWTGivenName),
        KeyName::Jwt(JwtKeyName::JWTFamilyName),
        KeyName::Jwt(JwtKeyName::JWTMiddleName),
        KeyName::Jwt(JwtKeyName::JWTNickName),
        KeyName::Jwt(JwtKeyName::JWTPrefUsername),
        KeyName::Jwt(JwtKeyName::JWTProfile),
        KeyName::Jwt(JwtKeyName::JWTPicture),
        KeyName::Jwt(JwtKeyName::JWTWebsite),
        KeyName::Jwt(JwtKeyName::JWTEmail),
        KeyName::Jwt(JwtKeyName::JWTGender),
        KeyName::Jwt(JwtKeyName::JWTBirthdate),
        KeyName::Jwt(JwtKeyName::JWTPhoneNumber),
        KeyName::Jwt(JwtKeyName::JWTAddress),
        KeyName::Jwt(JwtKeyName::JWTScope),
        KeyName::Jwt(JwtKeyName::JWTClientID),
    ];

    pub const fn prefix(&self) -> usize {
        match self {
            KeyName::Aws(_) => "aws:".len(),
            KeyName::Jwt(_) => "jwt:".len(),
            KeyName::Ldap(_) => "ldap:".len(),
            KeyName::Sts(_) => "sts:".len(),
            KeyName::Svc(_) => "svc:".len(),
            KeyName::S3(_) => "s3:".len(),
        }
    }

    pub fn name(&self) -> &str {
        &Into::<&str>::into(self)[self.prefix()..]
    }

    pub fn var_name(&self) -> String {
        match self {
            KeyName::Aws(s) => format!("${{aws:{}}}", Into::<&str>::into(s)),
            KeyName::Jwt(s) => format!("${{jwt:{}}}", Into::<&str>::into(s)),
            KeyName::Ldap(s) => format!("${{ldap:{}}}", Into::<&str>::into(s)),
            KeyName::Sts(s) => format!("${{sts:{}}}", Into::<&str>::into(s)),
            KeyName::Svc(s) => format!("${{svc:{}}}", Into::<&str>::into(s)),
            KeyName::S3(s) => format!("${{s3:{}}}", Into::<&str>::into(s)),
        }
    }
}

impl From<&KeyName> for &'static str {
    fn from(k: &KeyName) -> Self {
        match k {
            KeyName::Aws(aws) => aws.into(),
            KeyName::Jwt(jwt) => jwt.into(),
            KeyName::Ldap(ldap) => ldap.into(),
            KeyName::Sts(sts) => sts.into(),
            KeyName::Svc(svc) => svc.into(),
            KeyName::S3(s3) => s3.into(),
        }
    }
}

#[derive(Clone, EnumString, Debug, IntoStaticStr, Eq, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "&str", into = "&str")]
pub enum S3KeyName {
    #[strum(serialize = "s3:x-amz-copy-source")]
    S3XAmzCopySource,

    #[strum(serialize = "s3:x-amz-server-side-encryption")]
    S3XAmzServerSideEncryption,

    #[strum(serialize = "s3:x-amz-server-side-encryption-customer-algorithm")]
    S3XAmzServerSideEncryptionCustomerAlgorithm,

    #[strum(serialize = "s3:signatureversion")]
    S3SignatureVersion,

    #[strum(serialize = "s3:authType")]
    S3AuthType,

    #[strum(serialize = "s3:signatureAge")]
    S3SignatureAge,

    #[strum(serialize = "s3:x-amz-content-sha256")]
    S3XAmzContentSha256,

    #[strum(serialize = "s3:LocationConstraint")]
    S3LocationConstraint,

    #[strum(serialize = "s3:object-lock-retain-until-date")]
    S3ObjectLockRetainUntilDate,

    #[strum(serialize = "s3:max-keys")]
    S3MaxKeys,

    #[strum(serialize = "s3:x-amz-metadata-directive")]
    S3XAmzMetadataDirective,

    #[strum(serialize = "s3:x-amz-storage-class")]
    S3XAmzStorageClass,

    #[strum(serialize = "s3:prefix")]
    S3Prefix,

    #[strum(serialize = "s3:delimiter")]
    S3Delimiter,

    #[strum(serialize = "s3:ExistingObjectTag")]
    S3ExistingObjectTag,
    #[strum(serialize = "s3:RequestObjectTagKeys")]
    S3RequestObjectTagKeys,
    #[strum(serialize = "s3:RequestObjectTag")]
    S3RequestObjectTag,
}

#[derive(Clone, EnumString, Debug, IntoStaticStr, Eq, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "&str", into = "&str")]
pub enum JwtKeyName {
    #[strum(serialize = "jwt:sub")]
    JWTSub,

    #[strum(serialize = "jwt:iss")]
    JWTIss,

    #[strum(serialize = "jwt:aud")]
    JWTAud,

    #[strum(serialize = "jwt:jti")]
    JWTJti,

    #[strum(serialize = "jwt:name")]
    JWTName,

    #[strum(serialize = "jwt:upn")]
    JWTUpn,

    #[strum(serialize = "jwt:groups")]
    JWTGroups,

    #[strum(serialize = "jwt:given_name")]
    JWTGivenName,

    #[strum(serialize = "jwt:family_name")]
    JWTFamilyName,

    #[strum(serialize = "jwt:middle_name")]
    JWTMiddleName,

    #[strum(serialize = "jwt:nickname")]
    JWTNickName,

    #[strum(serialize = "jwt:preferred_username")]
    JWTPrefUsername,

    #[strum(serialize = "jwt:profile")]
    JWTProfile,

    #[strum(serialize = "jwt:picture")]
    JWTPicture,

    #[strum(serialize = "jwt:website")]
    JWTWebsite,

    #[strum(serialize = "jwt:email")]
    JWTEmail,

    #[strum(serialize = "jwt:gender")]
    JWTGender,

    #[strum(serialize = "jwt:birthdate")]
    JWTBirthdate,

    #[strum(serialize = "jwt:phone_number")]
    JWTPhoneNumber,

    #[strum(serialize = "jwt:address")]
    JWTAddress,

    #[strum(serialize = "jwt:scope")]
    JWTScope,

    #[strum(serialize = "jwt:client_id")]
    JWTClientID,
}

#[derive(Clone, EnumString, Debug, IntoStaticStr, Eq, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "&str", into = "&str")]
pub enum SvcKeyName {
    #[strum(serialize = "svc:DurationSeconds")]
    SVCDurationSeconds,
}

#[derive(Clone, EnumString, Debug, IntoStaticStr, Eq, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "&str", into = "&str")]
pub enum LdapKeyName {
    #[strum(serialize = "ldap:user")]
    User,

    #[strum(serialize = "ldap:username")]
    Username,

    #[strum(serialize = "ldap:groups")]
    Groups,
}

#[derive(Clone, EnumString, Debug, IntoStaticStr, Eq, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "&str", into = "&str")]
pub enum StsKeyName {
    #[strum(serialize = "sts:DurationSeconds")]
    STSDurationSeconds,
}

#[derive(Clone, EnumString, Debug, IntoStaticStr, Eq, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "&str", into = "&str")]
pub enum AwsKeyName {
    #[strum(serialize = "aws:Referer")]
    AWSReferer,

    #[strum(serialize = "aws:SourceIp")]
    AWSSourceIP,

    #[strum(serialize = "aws:UserAgent")]
    AWSUserAgent,

    #[strum(serialize = "aws:SecureTransport")]
    AWSSecureTransport,

    #[strum(serialize = "aws:CurrentTime")]
    AWSCurrentTime,

    #[strum(serialize = "aws:EpochTime")]
    AWSEpochTime,

    #[strum(serialize = "aws:principaltype")]
    AWSPrincipalType,

    #[strum(serialize = "aws:userid")]
    AWSUserID,

    #[strum(serialize = "aws:username")]
    AWSUsername,

    #[strum(serialize = "aws:groups")]
    AWSGroups,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;
    use test_case::test_case;

    #[test_case("s3:x-amz-copy-source", KeyName::S3(S3KeyName::S3XAmzCopySource))]
    #[test_case("aws:SecureTransport", KeyName::Aws(AwsKeyName::AWSSecureTransport))]
    #[test_case("jwt:sub", KeyName::Jwt(JwtKeyName::JWTSub))]
    #[test_case("ldap:user", KeyName::Ldap(LdapKeyName::User))]
    #[test_case("sts:DurationSeconds", KeyName::Sts(StsKeyName::STSDurationSeconds))]
    #[test_case("svc:DurationSeconds", KeyName::Svc(SvcKeyName::SVCDurationSeconds))]
    fn key_name_from_str_successful(val: &str, except: KeyName) {
        let key_name = KeyName::try_from(val);
        assert_eq!(key_name, Ok(except));
    }

    #[test_case("S3:x-amz-copy-source")]
    #[test_case("aWs:SecureTransport")]
    #[test_case("jwt:suB")]
    #[test_case("ldap:us")]
    #[test_case("DurationSeconds")]
    fn key_name_from_str_failed(val: &str) {
        assert_eq!(KeyName::try_from(val), Err(InvalidKeyName(val.to_string())));
    }

    #[test_case("s3:x-amz-copy-source", KeyName::S3(S3KeyName::S3XAmzCopySource))]
    #[test_case("aws:SecureTransport", KeyName::Aws(AwsKeyName::AWSSecureTransport))]
    #[test_case("jwt:sub", KeyName::Jwt(JwtKeyName::JWTSub))]
    #[test_case("ldap:user", KeyName::Ldap(LdapKeyName::User))]
    #[test_case("sts:DurationSeconds", KeyName::Sts(StsKeyName::STSDurationSeconds))]
    #[test_case("svc:DurationSeconds", KeyName::Svc(SvcKeyName::SVCDurationSeconds))]
    fn key_name_deserialize(val: &str, except: KeyName) {
        #[derive(Deserialize)]
        struct TestCase {
            data: KeyName,
        }

        let data = format!("{{\"data\":\"{val}\"}}");
        let data: TestCase = serde_json::from_str(data.as_str()).expect("unmarshal failed");
        assert_eq!(data.data, except);
    }

    #[test_case("s3:x-amz-copy-source", KeyName::S3(S3KeyName::S3XAmzCopySource))]
    #[test_case("aws:SecureTransport", KeyName::Aws(AwsKeyName::AWSSecureTransport))]
    #[test_case("jwt:sub", KeyName::Jwt(JwtKeyName::JWTSub))]
    #[test_case("ldap:user", KeyName::Ldap(LdapKeyName::User))]
    #[test_case("sts:DurationSeconds", KeyName::Sts(StsKeyName::STSDurationSeconds))]
    #[test_case("svc:DurationSeconds", KeyName::Svc(SvcKeyName::SVCDurationSeconds))]
    fn key_name_serialize(except: &str, value: KeyName) {
        #[derive(Serialize)]
        struct TestCase {
            data: KeyName,
        }

        let except = format!("{{\"data\":\"{except}\"}}");
        let data = serde_json::to_string(&TestCase { data: value }).expect("marshal failed");
        assert_eq!(data, except);
    }
}
