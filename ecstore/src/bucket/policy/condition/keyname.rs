use core::fmt;
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use std::str::FromStr;

use crate::error::Error;

use super::key::Key;

// 定义KeyName枚举类型
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum KeyName {
    // S3XAmzCopySource - key representing x-amz-copy-source HTTP header applicable to PutObject API only.
    S3XAmzCopySource, // KeyName = "s3:x-amz-copy-source"

    // S3XAmzServerSideEncryption - key representing x-amz-server-side-encryption HTTP header applicable
    // to PutObject API only.
    S3XAmzServerSideEncryption, // KeyName = "s3:x-amz-server-side-encryption"

    // S3XAmzServerSideEncryptionCustomerAlgorithm - key representing
    // x-amz-server-side-encryption-customer-algorithm HTTP header applicable to PutObject API only.
    S3XAmzServerSideEncryptionCustomerAlgorithm, // KeyName = "s3:x-amz-server-side-encryption-customer-algorithm"

    // S3XAmzMetadataDirective - key representing x-amz-metadata-directive HTTP header applicable to
    // PutObject API only.
    S3XAmzMetadataDirective, // KeyName = "s3:x-amz-metadata-directive"

    // S3XAmzContentSha256 - set a static content-sha256 for all calls for a given action.
    S3XAmzContentSha256, // KeyName = "s3:x-amz-content-sha256"

    // S3XAmzStorageClass - key representing x-amz-storage-class HTTP header applicable to PutObject API
    // only.
    S3XAmzStorageClass, // KeyName = "s3:x-amz-storage-class"

    // S3XAmzServerSideEncryptionAwsKmsKeyID - key representing x-amz-server-side-encryption-aws-kms-key-id
    // HTTP header for S3 API calls
    S3XAmzServerSideEncryptionAwsKmsKeyID, // KeyName = "s3:x-amz-server-side-encryption-aws-kms-key-id"

    // S3LocationConstraint - key representing LocationConstraint XML tag of CreateBucket API only.
    S3LocationConstraint, // KeyName = "s3:LocationConstraint"

    // S3Prefix - key representing prefix query parameter of ListBucket API only.
    S3Prefix, // KeyName = "s3:prefix"

    // S3Delimiter - key representing delimiter query parameter of ListBucket API only.
    S3Delimiter, // KeyName = "s3:delimiter"

    // S3VersionID - Enables you to limit the permission for the
    // s3:PutObjectVersionTagging action to a specific object version.
    S3VersionID, // KeyName = "s3:versionid"

    // S3MaxKeys - key representing max-keys query parameter of ListBucket API only.
    S3MaxKeys, // KeyName = "s3:max-keys"

    // S3ObjectLockRemainingRetentionDays - key representing object-lock-remaining-retention-days
    // Enables enforcement of an object relative to the remaining retention days, you can set
    // minimum and maximum allowable retention periods for a bucket using a bucket policy.
    // This key are specific for s3:PutObjectRetention API.
    S3ObjectLockRemainingRetentionDays, // KeyName = "s3:object-lock-remaining-retention-days"

    // S3ObjectLockMode - key representing object-lock-mode
    // Enables enforcement of the specified object retention mode
    S3ObjectLockMode, // KeyName = "s3:object-lock-mode"

    // S3ObjectLockRetainUntilDate - key representing object-lock-retain-util-date
    // Enables enforcement of a specific retain-until-date
    S3ObjectLockRetainUntilDate, // KeyName = "s3:object-lock-retain-until-date"

    // S3ObjectLockLegalHold - key representing object-local-legal-hold
    // Enables enforcement of the specified object legal hold status
    S3ObjectLockLegalHold, // KeyName = "s3:object-lock-legal-hold"

    // AWSReferer - key representing Referer header of any API.
    AWSReferer, // KeyName = "aws:Referer"

    // AWSSourceIP - key representing client's IP address (not intermittent proxies) of any API.
    AWSSourceIP, // KeyName = "aws:SourceIp"

    // AWSUserAgent - key representing UserAgent header for any API.
    AWSUserAgent, // KeyName = "aws:UserAgent"

    // AWSSecureTransport - key representing if the clients request is authenticated or not.
    AWSSecureTransport, // KeyName = "aws:SecureTransport"

    // AWSCurrentTime - key representing the current time.
    AWSCurrentTime, // KeyName = "aws:CurrentTime"

    // AWSEpochTime - key representing the current epoch time.
    AWSEpochTime, // KeyName = "aws:EpochTime"

    // AWSPrincipalType - user principal type currently supported values are "User" and "Anonymous".
    AWSPrincipalType, // KeyName = "aws:principaltype"

    // AWSUserID - user unique ID, in RustFS this value is same as your user Access Key.
    AWSUserID, // KeyName = "aws:userid"

    // AWSUsername - user friendly name, in RustFS this value is same as your user Access Key.
    AWSUsername, // KeyName = "aws:username"

    // AWSGroups - groups for any authenticating Access Key.
    AWSGroups, // KeyName = "ss"

    // S3SignatureVersion - identifies the version of AWS Signature that you want to support for authenticated requests.
    S3SignatureVersion, // KeyName = "s3:signatureversion"

    // S3SignatureAge - identifies the maximum age of presgiend URL allowed
    S3SignatureAge, // KeyName = "s3:signatureAge"

    // S3AuthType - optionally use this condition key to restrict incoming requests to use a specific authentication method.
    S3AuthType, // KeyName = "s3:authType"

    // Refer https://docs.aws.amazon.com/AmazonS3/latest/userguide/tagging-and-policies.html
    ExistingObjectTag,    // KeyName = "s3:ExistingObjectTag"
    RequestObjectTagKeys, // KeyName = "s3:RequestObjectTagKeys"
    RequestObjectTag,     // KeyName = "s3:RequestObjectTag"

    // JWTSub - JWT subject claim substitution.
    JWTSub, //KeyName = "jwt:sub"

    // JWTIss issuer claim substitution.
    JWTIss, //KeyName = "jwt:iss"

    // JWTAud audience claim substitution.
    JWTAud, //KeyName = "jwt:aud"

    // JWTJti JWT unique identifier claim substitution.
    JWTJti, //KeyName = "jwt:jti"

    JWTUpn,          //KeyName = "jwt:upn"
    JWTName,         //KeyName = "jwt:name"
    JWTGroups,       //KeyName = "jwt:groups"
    JWTGivenName,    //KeyName = "jwt:given_name"
    JWTFamilyName,   //KeyName = "jwt:family_name"
    JWTMiddleName,   //KeyName = "jwt:middle_name"
    JWTNickName,     //KeyName = "jwt:nickname"
    JWTPrefUsername, //KeyName = "jwt:preferred_username"
    JWTProfile,      //KeyName = "jwt:profile"
    JWTPicture,      //KeyName = "jwt:picture"
    JWTWebsite,      //KeyName = "jwt:website"
    JWTEmail,        //KeyName = "jwt:email"
    JWTGender,       //KeyName = "jwt:gender"
    JWTBirthdate,    //KeyName = "jwt:birthdate"
    JWTPhoneNumber,  //KeyName = "jwt:phone_number"
    JWTAddress,      //KeyName = "jwt:address"
    JWTScope,        //KeyName = "jwt:scope"
    JWTClientID,     //KeyName = "jwt:client_id"

    // LDAPUser - LDAP username,  this value is equal to your authenticating LDAP user DN.
    LDAPUser, // KeyName = "ldap:user"

    // LDAPUsername - LDAP username,  is the authenticated simple user.
    LDAPUsername, // KeyName = "ldap:username"

    // LDAPGroups - LDAP groups,  this value is equal LDAP Group DNs for the authenticating user.
    LDAPGroups, // KeyName = "ldap:groups"

    // STSDurationSeconds - Duration seconds condition for STS policy
    STSDurationSeconds, // KeyName = "sts:DurationSeconds"
    // SVCDurationSeconds - Duration seconds condition for Admin policy
    SVCDurationSeconds, // KeyName = "svc:DurationSeconds"

    Undefined,
}

lazy_static! {
    pub static ref JWTKEYS: Vec<KeyName> = {
        vec![
            KeyName::JWTSub,
            KeyName::JWTIss,
            KeyName::JWTAud,
            KeyName::JWTJti,
            KeyName::JWTName,
            KeyName::JWTUpn,
            KeyName::JWTGroups,
            KeyName::JWTGivenName,
            KeyName::JWTFamilyName,
            KeyName::JWTMiddleName,
            KeyName::JWTNickName,
            KeyName::JWTPrefUsername,
            KeyName::JWTProfile,
            KeyName::JWTPicture,
            KeyName::JWTWebsite,
            KeyName::JWTEmail,
            KeyName::JWTGender,
            KeyName::JWTBirthdate,
            KeyName::JWTPhoneNumber,
            KeyName::JWTAddress,
            KeyName::JWTScope,
            KeyName::JWTClientID,
        ]
    };
    pub static ref ALL_SUPPORT_KEYS: Vec<KeyName> = {
        vec![
            KeyName::S3SignatureVersion,
            KeyName::S3AuthType,
            KeyName::S3SignatureAge,
            KeyName::S3XAmzCopySource,
            KeyName::S3XAmzServerSideEncryption,
            KeyName::S3XAmzServerSideEncryptionCustomerAlgorithm,
            KeyName::S3XAmzMetadataDirective,
            KeyName::S3XAmzStorageClass,
            KeyName::S3XAmzServerSideEncryptionAwsKmsKeyID,
            KeyName::S3XAmzContentSha256,
            KeyName::S3LocationConstraint,
            KeyName::S3Prefix,
            KeyName::S3Delimiter,
            KeyName::S3MaxKeys,
            KeyName::S3VersionID,
            KeyName::S3ObjectLockRemainingRetentionDays,
            KeyName::S3ObjectLockMode,
            KeyName::S3ObjectLockLegalHold,
            KeyName::S3ObjectLockRetainUntilDate,
            KeyName::AWSReferer,
            KeyName::AWSSourceIP,
            KeyName::AWSUserAgent,
            KeyName::AWSSecureTransport,
            KeyName::AWSCurrentTime,
            KeyName::AWSEpochTime,
            KeyName::AWSPrincipalType,
            KeyName::AWSUserID,
            KeyName::AWSUsername,
            KeyName::AWSGroups,
            KeyName::LDAPUser,
            KeyName::LDAPUsername,
            KeyName::LDAPGroups,
            KeyName::RequestObjectTag,
            KeyName::ExistingObjectTag,
            KeyName::RequestObjectTagKeys,
            KeyName::JWTSub,
            KeyName::JWTIss,
            KeyName::JWTAud,
            KeyName::JWTJti,
            KeyName::JWTName,
            KeyName::JWTUpn,
            KeyName::JWTGroups,
            KeyName::JWTGivenName,
            KeyName::JWTFamilyName,
            KeyName::JWTMiddleName,
            KeyName::JWTNickName,
            KeyName::JWTPrefUsername,
            KeyName::JWTProfile,
            KeyName::JWTPicture,
            KeyName::JWTWebsite,
            KeyName::JWTEmail,
            KeyName::JWTGender,
            KeyName::JWTBirthdate,
            KeyName::JWTPhoneNumber,
            KeyName::JWTAddress,
            KeyName::JWTScope,
            KeyName::JWTClientID,
            KeyName::STSDurationSeconds,
            KeyName::SVCDurationSeconds,
        ]
    };
    pub static ref COMMOM_KEYS: Vec<KeyName> = {
        let mut keys = vec![
            KeyName::S3SignatureVersion,
            KeyName::S3AuthType,
            KeyName::S3SignatureAge,
            KeyName::S3XAmzContentSha256,
            KeyName::S3LocationConstraint,
            KeyName::AWSReferer,
            KeyName::AWSSourceIP,
            KeyName::AWSUserAgent,
            KeyName::AWSSecureTransport,
            KeyName::AWSCurrentTime,
            KeyName::AWSEpochTime,
            KeyName::AWSPrincipalType,
            KeyName::AWSUserID,
            KeyName::AWSUsername,
            KeyName::AWSGroups,
            KeyName::LDAPUser,
            KeyName::LDAPUsername,
            KeyName::LDAPGroups,
        ];

        keys.extend(JWTKEYS.iter().cloned());

        keys
    };
    pub static ref ALL_SUPPORT_ADMIN_KEYS: Vec<KeyName> = {
        let mut keys = vec![
            KeyName::AWSReferer,
            KeyName::AWSSourceIP,
            KeyName::AWSUserAgent,
            KeyName::AWSSecureTransport,
            KeyName::AWSCurrentTime,
            KeyName::AWSEpochTime,
            KeyName::AWSPrincipalType,
            KeyName::AWSUserID,
            KeyName::AWSUsername,
            KeyName::AWSGroups,
            KeyName::LDAPUser,
            KeyName::LDAPUsername,
            KeyName::LDAPGroups,
            KeyName::SVCDurationSeconds,
        ];

        keys.extend(JWTKEYS.iter().cloned());

        keys
    };
    pub static ref ALL_SUPPORT_STS_KEYS: Vec<KeyName> = vec![KeyName::STSDurationSeconds];
}

// 实现KeyName枚举的方法
impl KeyName {
    pub fn name(&self) -> &str {
        let name = self.as_str();
        if name.starts_with("aws:") {
            name.trim_start_matches("aws:")
        } else if name.starts_with("jwt:") {
            name.trim_start_matches("jwt:")
        } else if name.starts_with("ldap:") {
            name.trim_start_matches("ldap:")
        } else if name.starts_with("sts:") {
            name.trim_start_matches("sts:")
        } else if name.starts_with("svc:") {
            name.trim_start_matches("svc:")
        } else {
            name.trim_start_matches("s3:")
        }
    }

    // Name方法，返回键名的名称
    pub fn as_str(&self) -> &str {
        match self {
            KeyName::S3XAmzCopySource => "s3:x-amz-copy-source",
            KeyName::S3XAmzServerSideEncryption => "s3:x-amz-server-side-encryption",
            KeyName::S3XAmzServerSideEncryptionCustomerAlgorithm => "s3:x-amz-server-side-encryption-customer-algorithm",
            KeyName::S3XAmzMetadataDirective => "s3:x-amz-metadata-directive",
            KeyName::S3XAmzContentSha256 => "s3:x-amz-content-sha256",
            KeyName::S3XAmzStorageClass => "s3:x-amz-storage-class",
            KeyName::S3XAmzServerSideEncryptionAwsKmsKeyID => "s3:x-amz-server-side-encryption-aws-kms-key-id",
            KeyName::S3LocationConstraint => "s3:LocationConstraint",
            KeyName::S3Prefix => "s3:prefix",
            KeyName::S3Delimiter => "s3:delimiter",
            KeyName::S3VersionID => "s3:versionid",
            KeyName::S3MaxKeys => "s3:max-keys",
            KeyName::S3ObjectLockRemainingRetentionDays => "s3:object-lock-remaining-retention-days",
            KeyName::S3ObjectLockMode => "s3:object-lock-mode",
            KeyName::S3ObjectLockRetainUntilDate => "s3:object-lock-retain-until-date",
            KeyName::S3ObjectLockLegalHold => "s3:object-lock-legal-hold",
            KeyName::AWSReferer => "aws:Referer",
            KeyName::AWSSourceIP => "aws:SourceIp",
            KeyName::AWSUserAgent => "aws:UserAgent",
            KeyName::AWSSecureTransport => "aws:SecureTransport",
            KeyName::AWSCurrentTime => "aws:CurrentTime",
            KeyName::AWSEpochTime => "aws:EpochTime",
            KeyName::AWSPrincipalType => "aws:principaltype",
            KeyName::AWSUserID => "aws:userid",
            KeyName::AWSUsername => "aws:username",
            KeyName::AWSGroups => "ss",
            KeyName::S3SignatureVersion => "s3:signatureversion",
            KeyName::S3SignatureAge => "s3:signatureAge",
            KeyName::S3AuthType => "s3:authType",
            KeyName::ExistingObjectTag => "s3:ExistingObjectTag",
            KeyName::RequestObjectTagKeys => "s3:RequestObjectTagKeys",
            KeyName::RequestObjectTag => "s3:RequestObjectTag",
            KeyName::JWTSub => "jwt:sub",
            KeyName::JWTIss => "jwt:iss",
            KeyName::JWTAud => "jwt:aud",
            KeyName::JWTJti => "jwt:jti",
            KeyName::JWTUpn => "jwt:upn",
            KeyName::JWTName => "jwt:name",
            KeyName::JWTGroups => "jwt:groups",
            KeyName::JWTGivenName => "jwt:given_name",
            KeyName::JWTFamilyName => "jwt:family_name",
            KeyName::JWTMiddleName => "jwt:middle_name",
            KeyName::JWTNickName => "jwt:nickname",
            KeyName::JWTPrefUsername => "jwt:preferred_username",
            KeyName::JWTProfile => "jwt:profile",
            KeyName::JWTPicture => "jwt:picture",
            KeyName::JWTWebsite => "jwt:website",
            KeyName::JWTEmail => "jwt:email",
            KeyName::JWTGender => "jwt:gender",
            KeyName::JWTBirthdate => "jwt:birthdate",
            KeyName::JWTPhoneNumber => "jwt:phone_number",
            KeyName::JWTAddress => "jwt:address",
            KeyName::JWTScope => "jwt:scope",
            KeyName::JWTClientID => "jwt:client_id",
            KeyName::LDAPUser => "ldap:user",
            KeyName::LDAPUsername => "ldap:username",
            KeyName::LDAPGroups => "ldap:groups",
            KeyName::STSDurationSeconds => "sts:DurationSeconds",
            KeyName::SVCDurationSeconds => "svc:DurationSeconds",
            KeyName::Undefined => "",
        }
    }

    // VarName方法，返回变量键名，例如 "${aws:username}"
    pub fn var_name(&self) -> String {
        format!("${{{}}}", self.name())
    }

    // ToKey方法，从名称创建键
    pub fn to_key(&self) -> Key {
        Key::new(self.clone(), "".to_string())
    }
}

impl fmt::Display for KeyName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl FromStr for KeyName {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "s3:x-amz-copy-source" => Ok(KeyName::S3XAmzCopySource),
            "s3:x-amz-server-side-encryption" => Ok(KeyName::S3XAmzServerSideEncryption),
            "s3:x-amz-server-side-encryption-customer-algorithm" => Ok(KeyName::S3XAmzServerSideEncryptionCustomerAlgorithm),
            "s3:x-amz-metadata-directive" => Ok(KeyName::S3XAmzMetadataDirective),
            "s3:x-amz-content-sha256" => Ok(KeyName::S3XAmzContentSha256),
            "s3:x-amz-storage-class" => Ok(KeyName::S3XAmzStorageClass),
            "s3:x-amz-server-side-encryption-aws-kms-key-id" => Ok(KeyName::S3XAmzServerSideEncryptionAwsKmsKeyID),
            "s3:LocationConstraint" => Ok(KeyName::S3LocationConstraint),
            "s3:prefix" => Ok(KeyName::S3Prefix),
            "s3:delimiter" => Ok(KeyName::S3Delimiter),
            "s3:versionid" => Ok(KeyName::S3VersionID),
            "s3:max-keys" => Ok(KeyName::S3MaxKeys),
            "s3:object-lock-remaining-retention-days" => Ok(KeyName::S3ObjectLockRemainingRetentionDays),
            "s3:object-lock-mode" => Ok(KeyName::S3ObjectLockMode),
            "s3:object-lock-retain-until-date" => Ok(KeyName::S3ObjectLockRetainUntilDate),
            "s3:object-lock-legal-hold" => Ok(KeyName::S3ObjectLockLegalHold),
            "aws:Referer" => Ok(KeyName::AWSReferer),
            "aws:SourceIp" => Ok(KeyName::AWSSourceIP),
            "aws:UserAgent" => Ok(KeyName::AWSUserAgent),
            "aws:SecureTransport" => Ok(KeyName::AWSSecureTransport),
            "aws:CurrentTime" => Ok(KeyName::AWSCurrentTime),
            "aws:EpochTime" => Ok(KeyName::AWSEpochTime),
            "aws:principaltype" => Ok(KeyName::AWSPrincipalType),
            "aws:userid" => Ok(KeyName::AWSUserID),
            "aws:username" => Ok(KeyName::AWSUsername),
            "aws:groups" => Ok(KeyName::AWSGroups),
            "s3:signatureversion" => Ok(KeyName::S3SignatureVersion),
            "s3:signatureAge" => Ok(KeyName::S3SignatureAge),
            "s3:authType" => Ok(KeyName::S3AuthType),
            "s3:ExistingObjectTag" => Ok(KeyName::ExistingObjectTag),
            "s3:RequestObjectTagKeys" => Ok(KeyName::RequestObjectTagKeys),
            "s3:RequestObjectTag" => Ok(KeyName::RequestObjectTag),
            "jwt:sub" => Ok(KeyName::JWTSub),
            "jwt:iss" => Ok(KeyName::JWTIss),
            "jwt:aud" => Ok(KeyName::JWTAud),
            "jwt:jti" => Ok(KeyName::JWTJti),
            "jwt:upn" => Ok(KeyName::JWTUpn),
            "jwt:name" => Ok(KeyName::JWTName),
            "jwt:groups" => Ok(KeyName::JWTGroups),
            "jwt:given_name" => Ok(KeyName::JWTGivenName),
            "jwt:family_name" => Ok(KeyName::JWTFamilyName),
            "jwt:middle_name" => Ok(KeyName::JWTMiddleName),
            "jwt:nickname" => Ok(KeyName::JWTNickName),
            "jwt:preferred_username" => Ok(KeyName::JWTPrefUsername),
            "jwt:profile" => Ok(KeyName::JWTProfile),
            "jwt:picture" => Ok(KeyName::JWTPicture),
            "jwt:website" => Ok(KeyName::JWTWebsite),
            "jwt:email" => Ok(KeyName::JWTEmail),
            "jwt:gender" => Ok(KeyName::JWTGender),
            "jwt:birthdate" => Ok(KeyName::JWTBirthdate),
            "jwt:phone_number" => Ok(KeyName::JWTPhoneNumber),
            "jwt:address" => Ok(KeyName::JWTAddress),
            "jwt:scope" => Ok(KeyName::JWTScope),
            "jwt:client_id" => Ok(KeyName::JWTClientID),
            "ldap:user" => Ok(KeyName::LDAPUser),
            "ldap:username" => Ok(KeyName::LDAPUsername),
            "ldap:groups" => Ok(KeyName::LDAPGroups),
            "sts:DurationSeconds" => Ok(KeyName::STSDurationSeconds),
            "svc:DurationSeconds" => Ok(KeyName::SVCDurationSeconds),
            _ => Err(Error::msg(format!("keyname not found: {}", s))),
        }
    }
}
