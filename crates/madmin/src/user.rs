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

use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as DeError};
use serde_json::Value;
use serde_json::value::RawValue;
use std::collections::HashMap;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use crate::BackendInfo;

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub enum AccountStatus {
    #[serde(rename = "enabled")]
    Enabled,
    #[serde(rename = "disabled")]
    #[default]
    Disabled,
}

impl AsRef<str> for AccountStatus {
    fn as_ref(&self) -> &str {
        match self {
            AccountStatus::Enabled => "enabled",
            AccountStatus::Disabled => "disabled",
        }
    }
}

impl TryFrom<&str> for AccountStatus {
    type Error = String;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        match s {
            "enabled" => Ok(AccountStatus::Enabled),
            "disabled" => Ok(AccountStatus::Disabled),
            _ => Err(format!("invalid account status: {s}")),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum UserAuthType {
    #[serde(rename = "builtin")]
    Builtin,
    #[serde(rename = "ldap")]
    Ldap,
    #[serde(rename = "oidc")]
    Oidc,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UserAuthInfo {
    #[serde(rename = "type")]
    pub auth_type: UserAuthType,

    #[serde(rename = "authServer", skip_serializing_if = "Option::is_none")]
    pub auth_server: Option<String>,

    #[serde(rename = "authServerUserID", skip_serializing_if = "Option::is_none")]
    pub auth_server_user_id: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct UserInfo {
    #[serde(rename = "userAuthInfo", skip_serializing_if = "Option::is_none")]
    pub auth_info: Option<UserAuthInfo>,

    #[serde(rename = "secretKey", skip_serializing_if = "Option::is_none")]
    pub secret_key: Option<String>,

    #[serde(rename = "policyName", skip_serializing_if = "Option::is_none")]
    pub policy_name: Option<String>,

    #[serde(rename = "status")]
    pub status: AccountStatus,

    #[serde(rename = "memberOf", skip_serializing_if = "Option::is_none")]
    pub member_of: Option<Vec<String>>,

    #[serde(rename = "updatedAt", with = "time::serde::rfc3339::option")]
    pub updated_at: Option<OffsetDateTime>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AddOrUpdateUserReq {
    #[serde(rename = "secretKey")]
    pub secret_key: String,

    #[serde(rename = "policy", skip_serializing_if = "Option::is_none")]
    pub policy: Option<String>,

    #[serde(rename = "status")]
    pub status: AccountStatus,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ServiceAccountInfo {
    #[serde(rename = "parentUser")]
    pub parent_user: String,

    #[serde(rename = "accountStatus")]
    pub account_status: String,

    #[serde(rename = "impliedPolicy")]
    pub implied_policy: bool,

    #[serde(rename = "accessKey")]
    pub access_key: String,

    #[serde(rename = "name", skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,

    #[serde(rename = "description", skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    #[serde(rename = "expiration", with = "time::serde::rfc3339::option")]
    pub expiration: Option<OffsetDateTime>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ListServiceAccountsResp {
    #[serde(rename = "accounts")]
    pub accounts: Vec<ServiceAccountInfo>,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct ListAccessKeysResp {
    #[serde(rename = "serviceAccounts", default)]
    pub service_accounts: Vec<ServiceAccountInfo>,
    #[serde(rename = "stsKeys", default)]
    pub sts_keys: Vec<ServiceAccountInfo>,
}

pub const ACCESS_KEY_LIST_USERS_ONLY: &str = "users-only";
pub const ACCESS_KEY_LIST_STS_ONLY: &str = "sts-only";
pub const ACCESS_KEY_LIST_SVCACC_ONLY: &str = "svcacc-only";
pub const ACCESS_KEY_LIST_ALL: &str = "all";

#[derive(Debug, Serialize, Deserialize)]
pub struct AddServiceAccountReq {
    #[serde(
        rename = "policy",
        skip_serializing_if = "Option::is_none",
        default,
        deserialize_with = "deserialize_optional_policy_value"
    )]
    pub policy: Option<Value>,

    #[serde(rename = "targetUser", skip_serializing_if = "Option::is_none")]
    pub target_user: Option<String>,

    #[serde(rename = "accessKey", default)]
    pub access_key: String,

    #[serde(rename = "secretKey", default)]
    pub secret_key: String,

    #[serde(rename = "name", skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,

    #[serde(rename = "description", skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    #[serde(
        rename = "expiration",
        skip_serializing_if = "Option::is_none",
        default,
        with = "time::serde::rfc3339::option"
    )]
    pub expiration: Option<OffsetDateTime>,

    #[serde(rename = "comment", skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
}

impl AddServiceAccountReq {
    pub fn validate(&self) -> Result<(), String> {
        validate_service_account_name(self.name.as_deref())?;
        validate_service_account_description(self.description.as_deref().or(self.comment.as_deref()))?;
        validate_service_account_expiration(self.expiration)
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Credentials<'a> {
    pub access_key: &'a str,
    pub secret_key: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_token: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(with = "time::serde::rfc3339::option")]
    pub expiration: Option<OffsetDateTime>,
}

#[derive(Serialize)]
pub struct AddServiceAccountResp<'a> {
    pub credentials: Credentials<'a>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct InfoServiceAccountResp {
    pub parent_user: String,
    pub account_status: String,
    pub implied_policy: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub policy: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(with = "time::serde::rfc3339::option")]
    pub expiration: Option<OffsetDateTime>,
}

pub type TemporaryAccountInfoResp = InfoServiceAccountResp;

#[derive(Debug, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct LDAPSpecificAccessKeyInfo {
    #[serde(rename = "username", skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
}

impl LDAPSpecificAccessKeyInfo {
    pub fn is_empty(&self) -> bool {
        self.username.is_none()
    }
}

#[derive(Debug, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct OpenIDSpecificAccessKeyInfo {
    #[serde(rename = "configName", skip_serializing_if = "Option::is_none")]
    pub config_name: Option<String>,
    #[serde(rename = "userID", skip_serializing_if = "Option::is_none")]
    pub user_id: Option<String>,
    #[serde(rename = "userIDClaim", skip_serializing_if = "Option::is_none")]
    pub user_id_claim: Option<String>,
    #[serde(rename = "displayName", skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    #[serde(rename = "displayNameClaim", skip_serializing_if = "Option::is_none")]
    pub display_name_claim: Option<String>,
}

impl OpenIDSpecificAccessKeyInfo {
    pub fn is_empty(&self) -> bool {
        self.config_name.is_none()
            && self.user_id.is_none()
            && self.user_id_claim.is_none()
            && self.display_name.is_none()
            && self.display_name_claim.is_none()
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct InfoAccessKeyResp {
    pub access_key: String,
    #[serde(flatten)]
    pub info: InfoServiceAccountResp,
    pub user_type: String,
    pub user_provider: String,
    #[serde(rename = "ldapSpecificInfo", skip_serializing_if = "LDAPSpecificAccessKeyInfo::is_empty")]
    pub ldap_specific_info: LDAPSpecificAccessKeyInfo,
    #[serde(
        rename = "openIDSpecificInfo",
        skip_serializing_if = "OpenIDSpecificAccessKeyInfo::is_empty"
    )]
    pub open_id_specific_info: OpenIDSpecificAccessKeyInfo,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UpdateServiceAccountReq {
    #[serde(
        rename = "newPolicy",
        skip_serializing_if = "Option::is_none",
        default,
        deserialize_with = "deserialize_optional_policy_value"
    )]
    pub new_policy: Option<Value>,

    #[serde(rename = "newSecretKey", skip_serializing_if = "Option::is_none")]
    pub new_secret_key: Option<String>,

    #[serde(rename = "newStatus", skip_serializing_if = "Option::is_none")]
    pub new_status: Option<String>,

    #[serde(rename = "newName", skip_serializing_if = "Option::is_none")]
    pub new_name: Option<String>,

    #[serde(rename = "newDescription", skip_serializing_if = "Option::is_none")]
    pub new_description: Option<String>,

    #[serde(rename = "newExpiration", skip_serializing_if = "Option::is_none", default)]
    #[serde(with = "time::serde::rfc3339::option")]
    pub new_expiration: Option<OffsetDateTime>,
}

impl UpdateServiceAccountReq {
    pub fn validate(&self) -> Result<(), String> {
        validate_service_account_name(self.new_name.as_deref())?;
        validate_service_account_description(self.new_description.as_deref())?;
        validate_service_account_expiration(self.new_expiration)
    }
}

fn deserialize_optional_policy_value<'de, D>(deserializer: D) -> Result<Option<Value>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<Value>::deserialize(deserializer)?;
    Ok(value.map(normalize_policy_value))
}

fn normalize_policy_value(value: Value) -> Value {
    match value {
        Value::String(policy) => serde_json::from_str(&policy).unwrap_or(Value::String(policy)),
        other => other,
    }
}

fn validate_service_account_name(name: Option<&str>) -> Result<(), String> {
    let Some(name) = name else {
        return Ok(());
    };

    if name.is_empty() {
        return Ok(());
    }

    if name.len() > 32 {
        return Err("name must not be longer than 32 characters".to_string());
    }

    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return Ok(());
    };

    if !first.is_ascii_alphabetic() {
        return Err(
            "name must contain only ASCII letters, digits, underscores and hyphens and must start with a letter".to_string(),
        );
    }

    if chars.any(|c| !c.is_ascii_alphanumeric() && c != '_' && c != '-') {
        return Err(
            "name must contain only ASCII letters, digits, underscores and hyphens and must start with a letter".to_string(),
        );
    }

    Ok(())
}

fn validate_service_account_description(description: Option<&str>) -> Result<(), String> {
    let Some(description) = description else {
        return Ok(());
    };

    if description.len() > 256 {
        return Err("description must be at most 256 bytes long".to_string());
    }

    Ok(())
}

fn validate_service_account_expiration(expiration: Option<OffsetDateTime>) -> Result<(), String> {
    let Some(expiration) = expiration else {
        return Ok(());
    };

    if expiration.unix_timestamp() == 0 {
        return Ok(());
    }

    if expiration < OffsetDateTime::now_utc() {
        return Err("the expiration time should be in the future".to_string());
    }

    Ok(())
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct AccountInfo {
    pub account_name: String,
    pub server: BackendInfo,
    pub policy: serde_json::Value, // Use iam/policy::parse to parse the result, to be done by the caller.
    pub buckets: Vec<BucketAccessInfo>,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct BucketAccessInfo {
    pub name: String,
    pub size: u64,
    pub objects: u64,
    pub object_sizes_histogram: HashMap<String, u64>,
    pub object_versions_histogram: HashMap<String, u64>,
    pub details: Option<BucketDetails>,
    pub prefix_usage: HashMap<String, u64>,
    #[serde(rename = "expiration", with = "time::serde::rfc3339::option")]
    pub created: Option<OffsetDateTime>,
    pub access: AccountAccess,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct BucketDetails {
    pub versioning: bool,
    pub versioning_suspended: bool,
    pub locking: bool,
    pub replication: bool,
    // pub tagging: Option<Tagging>,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct AccountAccess {
    pub read: bool,
    pub write: bool,
}

/// SRSessionPolicy - represents a session policy to be replicated.
#[derive(Debug, Clone)]
pub struct SRSessionPolicy(Option<Box<RawValue>>);

impl SRSessionPolicy {
    pub fn new() -> Self {
        SRSessionPolicy(None)
    }

    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        if json == "null" {
            Ok(SRSessionPolicy(None))
        } else {
            let raw_value = serde_json::from_str(json)?;
            Ok(SRSessionPolicy(Some(raw_value)))
        }
    }

    pub fn is_null(&self) -> bool {
        self.0.is_none()
    }

    pub fn as_str(&self) -> Option<&str> {
        self.0.as_ref().map(|v| v.get())
    }
}

impl Default for SRSessionPolicy {
    fn default() -> Self {
        Self::new()
    }
}

impl PartialEq for SRSessionPolicy {
    fn eq(&self, other: &Self) -> bool {
        self.0.as_ref().map(|v| v.get()) == other.0.as_ref().map(|v| v.get())
    }
}

impl Serialize for SRSessionPolicy {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match &self.0 {
            Some(raw_value) => raw_value.serialize(serializer),
            None => serializer.serialize_none(),
        }
    }
}

impl<'de> Deserialize<'de> for SRSessionPolicy {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw_value: Option<Box<RawValue>> = Option::deserialize(deserializer)?;
        Ok(SRSessionPolicy(raw_value))
    }
}

fn deserialize_vec_or_default<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: Deserializer<'de>,
{
    Ok(Option::<Vec<String>>::deserialize(deserializer)?.unwrap_or_default())
}

fn deserialize_optional_service_account_expiration<'de, D>(deserializer: D) -> Result<Option<OffsetDateTime>, D::Error>
where
    D: Deserializer<'de>,
{
    let expiration = Option::<String>::deserialize(deserializer)?;
    let Some(expiration) = expiration else {
        return Ok(None);
    };
    let expiration = expiration.trim();
    if expiration.is_empty() {
        return Ok(None);
    }

    let expiration = OffsetDateTime::parse(expiration, &Rfc3339).map_err(D::Error::custom)?;
    if expiration.unix_timestamp() == 0 {
        return Ok(None);
    }

    Ok(Some(expiration))
}

/// SRSvcAccCreate - create operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SRSvcAccCreate {
    pub parent: String,

    #[serde(rename = "accessKey")]
    pub access_key: String,

    #[serde(rename = "secretKey")]
    pub secret_key: String,

    #[serde(default, deserialize_with = "deserialize_vec_or_default")]
    pub groups: Vec<String>,

    pub claims: HashMap<String, serde_json::Value>,

    #[serde(rename = "sessionPolicy")]
    pub session_policy: SRSessionPolicy,

    pub status: String,

    pub name: String,

    pub description: String,

    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_service_account_expiration"
    )]
    pub expiration: Option<OffsetDateTime>,

    #[serde(rename = "apiVersion", skip_serializing_if = "Option::is_none")]
    pub api_version: Option<String>,
}

/// ImportIAMResult - represents the structure iam import response
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ImportIAMResult {
    /// Skipped entries while import
    /// This could be due to groups, policies etc missing for
    /// imported entries. We dont fail hard in this case and
    pub skipped: IAMEntities,

    /// Removed entries - this mostly happens for policies
    /// where empty might be getting imported and that's invalid
    pub removed: IAMEntities,

    /// Newly added entries
    pub added: IAMEntities,

    /// Failed entries while import. This would have details of
    /// failed entities with respective errors
    pub failed: IAMErrEntities,
}

/// IAMEntities - represents different IAM entities
#[derive(Default, Debug, Clone, Serialize, Deserialize)]
pub struct IAMEntities {
    /// List of policy names
    pub policies: Vec<String>,

    /// List of user names
    pub users: Vec<String>,

    /// List of group names
    pub groups: Vec<String>,

    /// List of Service Account names
    #[serde(rename = "serviceAccounts")]
    pub service_accounts: Vec<String>,

    /// List of user policies, each entry in map represents list of policies
    /// applicable to the user
    #[serde(rename = "userPolicies")]
    pub user_policies: Vec<HashMap<String, Vec<String>>>,

    /// List of group policies, each entry in map represents list of policies
    /// applicable to the group
    #[serde(rename = "groupPolicies")]
    pub group_policies: Vec<HashMap<String, Vec<String>>>,

    /// List of STS policies, each entry in map represents list of policies
    /// applicable to the STS
    #[serde(rename = "stsPolicies")]
    pub sts_policies: Vec<HashMap<String, Vec<String>>>,
}

/// PolicyEntitiesResult - contains response to a policy entities query.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicyEntitiesResult {
    #[serde(rename = "timestamp", with = "time::serde::rfc3339")]
    pub timestamp: time::OffsetDateTime,
    #[serde(rename = "userMappings", skip_serializing_if = "Vec::is_empty")]
    pub user_mappings: Vec<UserPolicyEntities>,
    #[serde(rename = "groupMappings", skip_serializing_if = "Vec::is_empty")]
    pub group_mappings: Vec<GroupPolicyEntities>,
    #[serde(rename = "policyMappings", skip_serializing_if = "Vec::is_empty")]
    pub policy_mappings: Vec<PolicyEntities>,
}

impl Default for PolicyEntitiesResult {
    fn default() -> Self {
        Self {
            timestamp: time::OffsetDateTime::UNIX_EPOCH,
            user_mappings: Vec::new(),
            group_mappings: Vec::new(),
            policy_mappings: Vec::new(),
        }
    }
}

/// UserPolicyEntities - user -> policies mapping
#[derive(Default, Debug, Clone, Serialize, Deserialize)]
pub struct UserPolicyEntities {
    pub user: String,
    pub policies: Vec<String>,
    #[serde(rename = "memberOfMappings", skip_serializing_if = "Vec::is_empty")]
    pub member_of_mappings: Vec<GroupPolicyEntities>,
}

/// GroupPolicyEntities - group -> policies mapping
#[derive(Default, Debug, Clone, Serialize, Deserialize)]
pub struct GroupPolicyEntities {
    pub group: String,
    pub policies: Vec<String>,
}

/// PolicyEntities - policy -> user+group mapping
#[derive(Default, Debug, Clone, Serialize, Deserialize)]
pub struct PolicyEntities {
    pub policy: String,
    pub users: Vec<String>,
    pub groups: Vec<String>,
}

/// IAMErrEntities - represents errored out IAM entries while import with error
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct IAMErrEntities {
    /// List of errored out policies with errors
    pub policies: Vec<IAMErrEntity>,

    /// List of errored out users with errors
    pub users: Vec<IAMErrEntity>,

    /// List of errored out groups with errors
    pub groups: Vec<IAMErrEntity>,

    /// List of errored out service accounts with errors
    #[serde(rename = "serviceAccounts")]
    pub service_accounts: Vec<IAMErrEntity>,

    /// List of errored out user policies with errors
    #[serde(rename = "userPolicies")]
    pub user_policies: Vec<IAMErrPolicyEntity>,

    /// List of errored out group policies with errors
    #[serde(rename = "groupPolicies")]
    pub group_policies: Vec<IAMErrPolicyEntity>,

    /// List of errored out STS policies with errors
    #[serde(rename = "stsPolicies")]
    pub sts_policies: Vec<IAMErrPolicyEntity>,
}

/// IAMErrEntity - represents an errored IAM entity with error details
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IAMErrEntity {
    pub name: String,
    pub error: String,
}

/// IAMErrPolicyEntity - represents an errored policy entity with error details
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IAMErrPolicyEntity {
    pub name: String,
    pub policies: Vec<String>,
    pub error: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json;
    use time::OffsetDateTime;

    #[test]
    fn test_account_status_default() {
        let status = AccountStatus::default();
        assert_eq!(status, AccountStatus::Disabled);
    }

    #[test]
    fn test_account_status_as_ref() {
        assert_eq!(AccountStatus::Enabled.as_ref(), "enabled");
        assert_eq!(AccountStatus::Disabled.as_ref(), "disabled");
    }

    #[test]
    fn test_account_status_try_from_valid() {
        assert_eq!(AccountStatus::try_from("enabled").unwrap(), AccountStatus::Enabled);
        assert_eq!(AccountStatus::try_from("disabled").unwrap(), AccountStatus::Disabled);
    }

    #[test]
    fn test_account_status_try_from_invalid() {
        let result = AccountStatus::try_from("invalid");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("invalid account status"));
    }

    #[test]
    fn test_account_status_serialization() {
        let enabled = AccountStatus::Enabled;
        let disabled = AccountStatus::Disabled;

        let enabled_json = serde_json::to_string(&enabled).unwrap();
        let disabled_json = serde_json::to_string(&disabled).unwrap();

        assert_eq!(enabled_json, "\"enabled\"");
        assert_eq!(disabled_json, "\"disabled\"");
    }

    #[test]
    fn test_account_status_deserialization() {
        let enabled: AccountStatus = serde_json::from_str("\"enabled\"").unwrap();
        let disabled: AccountStatus = serde_json::from_str("\"disabled\"").unwrap();

        assert_eq!(enabled, AccountStatus::Enabled);
        assert_eq!(disabled, AccountStatus::Disabled);
    }

    #[test]
    fn test_user_auth_type_serialization() {
        let builtin = UserAuthType::Builtin;
        let ldap = UserAuthType::Ldap;

        let builtin_json = serde_json::to_string(&builtin).unwrap();
        let ldap_json = serde_json::to_string(&ldap).unwrap();

        assert_eq!(builtin_json, "\"builtin\"");
        assert_eq!(ldap_json, "\"ldap\"");
    }

    #[test]
    fn test_user_auth_info_creation() {
        let auth_info = UserAuthInfo {
            auth_type: UserAuthType::Ldap,
            auth_server: Some("ldap.example.com".to_string()),
            auth_server_user_id: Some("user123".to_string()),
        };

        assert!(matches!(auth_info.auth_type, UserAuthType::Ldap));
        assert_eq!(auth_info.auth_server.unwrap(), "ldap.example.com");
        assert_eq!(auth_info.auth_server_user_id.unwrap(), "user123");
    }

    #[test]
    fn test_user_auth_info_serialization() {
        let auth_info = UserAuthInfo {
            auth_type: UserAuthType::Builtin,
            auth_server: None,
            auth_server_user_id: None,
        };

        let json = serde_json::to_string(&auth_info).unwrap();
        assert!(json.contains("builtin"));
        assert!(!json.contains("authServer"), "None fields should be skipped");
    }

    #[test]
    fn test_user_info_default() {
        let user_info = UserInfo::default();
        assert!(user_info.auth_info.is_none());
        assert!(user_info.secret_key.is_none());
        assert!(user_info.policy_name.is_none());
        assert_eq!(user_info.status, AccountStatus::Disabled);
        assert!(user_info.member_of.is_none());
        assert!(user_info.updated_at.is_none());
    }

    #[test]
    fn test_user_info_with_values() {
        let now = OffsetDateTime::now_utc();
        let user_info = UserInfo {
            auth_info: Some(UserAuthInfo {
                auth_type: UserAuthType::Builtin,
                auth_server: None,
                auth_server_user_id: None,
            }),
            secret_key: Some("secret123".to_string()),
            policy_name: Some("ReadOnlyAccess".to_string()),
            status: AccountStatus::Enabled,
            member_of: Some(vec!["group1".to_string(), "group2".to_string()]),
            updated_at: Some(now),
        };

        assert!(user_info.auth_info.is_some());
        assert_eq!(user_info.secret_key.unwrap(), "secret123");
        assert_eq!(user_info.policy_name.unwrap(), "ReadOnlyAccess");
        assert_eq!(user_info.status, AccountStatus::Enabled);
        assert_eq!(user_info.member_of.unwrap().len(), 2);
        assert!(user_info.updated_at.is_some());
    }

    #[test]
    fn test_add_or_update_user_req_creation() {
        let req = AddOrUpdateUserReq {
            secret_key: "newsecret".to_string(),
            policy: Some("FullAccess".to_string()),
            status: AccountStatus::Enabled,
        };

        assert_eq!(req.secret_key, "newsecret");
        assert_eq!(req.policy.unwrap(), "FullAccess");
        assert_eq!(req.status, AccountStatus::Enabled);
    }

    #[test]
    fn test_service_account_info_creation() {
        let now = OffsetDateTime::now_utc();
        let service_account = ServiceAccountInfo {
            parent_user: "admin".to_string(),
            account_status: "enabled".to_string(),
            implied_policy: true,
            access_key: "AKIAIOSFODNN7EXAMPLE".to_string(),
            name: Some("test-service".to_string()),
            description: Some("Test service account".to_string()),
            expiration: Some(now),
        };

        assert_eq!(service_account.parent_user, "admin");
        assert_eq!(service_account.account_status, "enabled");
        assert!(service_account.implied_policy);
        assert_eq!(service_account.access_key, "AKIAIOSFODNN7EXAMPLE");
        assert_eq!(service_account.name.unwrap(), "test-service");
        assert!(service_account.expiration.is_some());
    }

    #[test]
    fn test_list_service_accounts_resp_creation() {
        let resp = ListServiceAccountsResp {
            accounts: vec![
                ServiceAccountInfo {
                    parent_user: "user1".to_string(),
                    account_status: "enabled".to_string(),
                    implied_policy: false,
                    access_key: "KEY1".to_string(),
                    name: Some("service1".to_string()),
                    description: None,
                    expiration: None,
                },
                ServiceAccountInfo {
                    parent_user: "user2".to_string(),
                    account_status: "disabled".to_string(),
                    implied_policy: true,
                    access_key: "KEY2".to_string(),
                    name: Some("service2".to_string()),
                    description: Some("Second service".to_string()),
                    expiration: None,
                },
            ],
        };

        assert_eq!(resp.accounts.len(), 2);
        assert_eq!(resp.accounts[0].parent_user, "user1");
        assert_eq!(resp.accounts[1].account_status, "disabled");
    }

    #[test]
    fn test_add_service_account_req_validate_success() {
        let req = AddServiceAccountReq {
            policy: Some(serde_json::json!({"Version": "2012-10-17"})),
            target_user: Some("testuser".to_string()),
            access_key: "AKIAIOSFODNN7EXAMPLE".to_string(),
            secret_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string(),
            name: Some("test-service".to_string()),
            description: Some("Test service account".to_string()),
            expiration: None,
            comment: None,
        };

        let result = req.validate();
        assert!(result.is_ok());
    }

    #[test]
    fn test_add_service_account_req_validate_allows_generated_credentials() {
        let req = AddServiceAccountReq {
            policy: None,
            target_user: None,
            access_key: "".to_string(),
            secret_key: "".to_string(),
            name: None,
            description: None,
            expiration: None,
            comment: None,
        };

        assert!(req.validate().is_ok());
    }

    #[test]
    fn test_add_service_account_req_deserializes_stringified_policy_json() {
        let req: AddServiceAccountReq = serde_json::from_str(
            r#"{
                "policy":"{\"Version\":\"2012-10-17\",\"Statement\":[]}",
                "accessKey":"AKIAIOSFODNN7EXAMPLE",
                "secretKey":"secret"
            }"#,
        )
        .unwrap();

        assert_eq!(req.policy, Some(serde_json::json!({"Version":"2012-10-17","Statement":[]})));
    }

    #[test]
    fn test_add_service_account_req_allows_missing_policy_field() {
        let req: AddServiceAccountReq = serde_json::from_str(
            r#"{
                "accessKey":"AKIAIOSFODNN7EXAMPLE",
                "secretKey":"secret"
            }"#,
        )
        .unwrap();

        assert_eq!(req.policy, None);
    }

    #[test]
    fn test_add_service_account_req_validate_invalid_name() {
        let req = AddServiceAccountReq {
            policy: None,
            target_user: None,
            access_key: "AKIAIOSFODNN7EXAMPLE".to_string(),
            secret_key: "secret".to_string(),
            name: Some("1invalid".to_string()),
            description: None,
            expiration: None,
            comment: None,
        };

        let result = req.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("must start with a letter"));
    }

    #[test]
    fn test_add_service_account_req_validate_rejects_long_description() {
        let req = AddServiceAccountReq {
            policy: None,
            target_user: None,
            access_key: "AKIAIOSFODNN7EXAMPLE".to_string(),
            secret_key: "secret".to_string(),
            name: Some("test".to_string()),
            description: Some("a".repeat(257)),
            expiration: None,
            comment: None,
        };

        let result = req.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("at most 256 bytes"));
    }

    #[test]
    fn test_credentials_serialization() {
        let now = OffsetDateTime::now_utc();
        let credentials = Credentials {
            access_key: "AKIAIOSFODNN7EXAMPLE",
            secret_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            session_token: Some("session123"),
            expiration: Some(now),
        };

        let json = serde_json::to_string(&credentials).unwrap();
        assert!(json.contains("AKIAIOSFODNN7EXAMPLE"));
        assert!(json.contains("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"));
        assert!(json.contains("session123"));
    }

    #[test]
    fn test_credentials_without_optional_fields() {
        let credentials = Credentials {
            access_key: "AKIAIOSFODNN7EXAMPLE",
            secret_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            session_token: None,
            expiration: None,
        };

        let json = serde_json::to_string(&credentials).unwrap();
        assert!(json.contains("AKIAIOSFODNN7EXAMPLE"));
        assert!(!json.contains("sessionToken"), "None fields should be skipped");
        assert!(!json.contains("expiration"), "None fields should be skipped");
    }

    #[test]
    fn test_add_service_account_resp_creation() {
        let credentials = Credentials {
            access_key: "AKIAIOSFODNN7EXAMPLE",
            secret_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            session_token: None,
            expiration: None,
        };

        let resp = AddServiceAccountResp { credentials };

        assert_eq!(resp.credentials.access_key, "AKIAIOSFODNN7EXAMPLE");
        assert_eq!(resp.credentials.secret_key, "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
    }

    #[test]
    fn test_info_service_account_resp_creation() {
        let now = OffsetDateTime::now_utc();
        let resp = InfoServiceAccountResp {
            parent_user: "admin".to_string(),
            account_status: "enabled".to_string(),
            implied_policy: true,
            policy: Some("ReadOnlyAccess".to_string()),
            name: Some("test-service".to_string()),
            description: Some("Test service account".to_string()),
            expiration: Some(now),
        };

        assert_eq!(resp.parent_user, "admin");
        assert_eq!(resp.account_status, "enabled");
        assert!(resp.implied_policy);
        assert_eq!(resp.policy.unwrap(), "ReadOnlyAccess");
        assert_eq!(resp.name.unwrap(), "test-service");
        assert!(resp.expiration.is_some());
    }

    #[test]
    fn test_update_service_account_req_validate() {
        let req = UpdateServiceAccountReq {
            new_policy: Some(serde_json::json!({"Version": "2012-10-17"})),
            new_secret_key: Some("newsecret".to_string()),
            new_status: Some("enabled".to_string()),
            new_name: Some("updated-service".to_string()),
            new_description: Some("Updated description".to_string()),
            new_expiration: None,
        };

        let result = req.validate();
        assert!(result.is_ok());
    }

    #[test]
    fn test_update_service_account_req_deserializes_stringified_policy_json() {
        let req: UpdateServiceAccountReq = serde_json::from_str(
            r#"{
                "newPolicy":"{\"Version\":\"2012-10-17\",\"Statement\":[]}"
            }"#,
        )
        .unwrap();

        assert_eq!(req.new_policy, Some(serde_json::json!({"Version":"2012-10-17","Statement":[]})));
    }

    #[test]
    fn test_update_service_account_req_allows_missing_policy_field() {
        let req: UpdateServiceAccountReq = serde_json::from_str(r#"{}"#).unwrap();

        assert_eq!(req.new_policy, None);
    }

    #[test]
    fn test_account_info_creation() {
        use crate::BackendInfo;

        let account_info = AccountInfo {
            account_name: "testuser".to_string(),
            server: BackendInfo::default(),
            policy: serde_json::json!({"Version": "2012-10-17"}),
            buckets: vec![],
        };

        assert_eq!(account_info.account_name, "testuser");
        assert!(account_info.buckets.is_empty());
        assert!(account_info.policy.is_object());
    }

    #[test]
    fn test_bucket_access_info_creation() {
        let now = OffsetDateTime::now_utc();
        let mut sizes_histogram = HashMap::new();
        sizes_histogram.insert("small".to_string(), 100);
        sizes_histogram.insert("large".to_string(), 50);

        let mut versions_histogram = HashMap::new();
        versions_histogram.insert("v1".to_string(), 80);
        versions_histogram.insert("v2".to_string(), 70);

        let mut prefix_usage = HashMap::new();
        prefix_usage.insert("logs/".to_string(), 1000000);
        prefix_usage.insert("data/".to_string(), 5000000);

        let bucket_info = BucketAccessInfo {
            name: "test-bucket".to_string(),
            size: 6000000,
            objects: 150,
            object_sizes_histogram: sizes_histogram,
            object_versions_histogram: versions_histogram,
            details: Some(BucketDetails {
                versioning: true,
                versioning_suspended: false,
                locking: true,
                replication: false,
            }),
            prefix_usage,
            created: Some(now),
            access: AccountAccess {
                read: true,
                write: false,
            },
        };

        assert_eq!(bucket_info.name, "test-bucket");
        assert_eq!(bucket_info.size, 6000000);
        assert_eq!(bucket_info.objects, 150);
        assert_eq!(bucket_info.object_sizes_histogram.len(), 2);
        assert_eq!(bucket_info.object_versions_histogram.len(), 2);
        assert!(bucket_info.details.is_some());
        assert_eq!(bucket_info.prefix_usage.len(), 2);
        assert!(bucket_info.created.is_some());
        assert!(bucket_info.access.read);
        assert!(!bucket_info.access.write);
    }

    #[test]
    fn test_bucket_details_creation() {
        let details = BucketDetails {
            versioning: true,
            versioning_suspended: false,
            locking: true,
            replication: true,
        };

        assert!(details.versioning);
        assert!(!details.versioning_suspended);
        assert!(details.locking);
        assert!(details.replication);
    }

    #[test]
    fn test_account_access_creation() {
        let read_only = AccountAccess {
            read: true,
            write: false,
        };

        let full_access = AccountAccess { read: true, write: true };

        let no_access = AccountAccess {
            read: false,
            write: false,
        };

        assert!(read_only.read && !read_only.write);
        assert!(full_access.read && full_access.write);
        assert!(!no_access.read && !no_access.write);
    }

    #[test]
    fn test_serialization_deserialization_roundtrip() {
        let now = OffsetDateTime::now_utc().replace_nanosecond(0).unwrap();
        let user_info = UserInfo {
            auth_info: Some(UserAuthInfo {
                auth_type: UserAuthType::Ldap,
                auth_server: Some("ldap.example.com".to_string()),
                auth_server_user_id: Some("user123".to_string()),
            }),
            secret_key: Some("secret123".to_string()),
            policy_name: Some("ReadOnlyAccess".to_string()),
            status: AccountStatus::Enabled,
            member_of: Some(vec!["group1".to_string()]),
            updated_at: Some(now),
        };

        let json = serde_json::to_string(&user_info).unwrap();
        let deserialized: UserInfo = serde_json::from_str(&json).unwrap();

        assert!(json.contains("\"updatedAt\":\""));
        assert!(json.contains('T'));
        assert_eq!(deserialized.secret_key.unwrap(), "secret123");
        assert_eq!(deserialized.policy_name.unwrap(), "ReadOnlyAccess");
        assert_eq!(deserialized.status, AccountStatus::Enabled);
        assert_eq!(deserialized.member_of.unwrap().len(), 1);
        assert_eq!(deserialized.updated_at, Some(now));
    }

    #[test]
    fn test_debug_format_all_structures() {
        let account_status = AccountStatus::Enabled;
        let user_auth_type = UserAuthType::Builtin;
        let user_info = UserInfo::default();
        let service_account = ServiceAccountInfo {
            parent_user: "test".to_string(),
            account_status: "enabled".to_string(),
            implied_policy: false,
            access_key: "key".to_string(),
            name: None,
            description: None,
            expiration: None,
        };

        // Test that all structures can be formatted with Debug
        assert!(!format!("{account_status:?}").is_empty());
        assert!(!format!("{user_auth_type:?}").is_empty());
        assert!(!format!("{user_info:?}").is_empty());
        assert!(!format!("{service_account:?}").is_empty());
    }

    #[test]
    fn test_memory_efficiency() {
        // Test that structures don't use excessive memory
        assert!(std::mem::size_of::<AccountStatus>() < 100);
        assert!(std::mem::size_of::<UserAuthType>() < 100);
        assert!(std::mem::size_of::<UserInfo>() < 2000);
        assert!(std::mem::size_of::<ServiceAccountInfo>() < 2000);
        assert!(std::mem::size_of::<AccountAccess>() < 100);
    }

    #[test]
    fn test_edge_cases() {
        // Test empty strings and edge cases
        let req = AddServiceAccountReq {
            policy: Some(serde_json::Value::Null),
            target_user: Some("".to_string()),
            access_key: "valid_key".to_string(),
            secret_key: "valid_secret".to_string(),
            name: Some("valid_name".to_string()),
            description: Some("".to_string()),
            expiration: None,
            comment: None,
        };

        // Should still validate successfully with empty optional strings
        assert!(req.validate().is_ok());

        // Test very long strings
        let long_string = "a".repeat(1000);
        let long_req = AddServiceAccountReq {
            policy: Some(serde_json::json!({"Statement": [long_string.clone()]})),
            target_user: Some(long_string.clone()),
            access_key: long_string.clone(),
            secret_key: long_string.clone(),
            name: Some("valid_name".to_string()),
            description: Some("valid description".to_string()),
            expiration: None,
            comment: None,
        };

        assert!(long_req.validate().is_ok());
    }

    #[test]
    fn test_sr_svc_acc_create_deserialize_empty_expiration_as_none() {
        let payload = r#"{
            "parent": "useralpha",
            "accessKey": "svcalpha",
            "secretKey": "svcAlphaSecret123",
            "groups": [],
            "claims": {},
            "sessionPolicy": null,
            "status": "on",
            "name": "uploaderKey",
            "description": "alpha upload key",
            "expiration": "   "
        }"#;

        let svc: SRSvcAccCreate = serde_json::from_str(payload).unwrap();
        assert!(svc.expiration.is_none());
    }
}
