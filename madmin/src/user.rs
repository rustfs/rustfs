use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

#[derive(Debug, Serialize, Deserialize, Default)]
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
            _ => Err(format!("invalid account status: {}", s)),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum UserAuthType {
    #[serde(rename = "builtin")]
    Builtin,
    #[serde(rename = "ldap")]
    Ldap,
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

    #[serde(rename = "updatedAt")]
    pub updated_at: Option<OffsetDateTime>,
}

#[derive(Debug, Serialize, Deserialize)]
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

    #[serde(rename = "expiration", skip_serializing_if = "Option::is_none")]
    pub expiration: Option<OffsetDateTime>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ListServiceAccountsResp {
    #[serde(rename = "accounts")]
    pub accounts: Vec<ServiceAccountInfo>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AddServiceAccountReq {
    #[serde(rename = "policy", skip_serializing_if = "Option::is_none")]
    pub policy: Option<String>,

    #[serde(rename = "targetUser", skip_serializing_if = "Option::is_none")]
    pub target_user: Option<String>,

    #[serde(rename = "accessKey")]
    pub access_key: String,

    #[serde(rename = "secretKey")]
    pub secret_key: String,

    #[serde(rename = "name")]
    pub name: String,

    #[serde(rename = "description", skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    #[serde(rename = "expiration", skip_serializing_if = "Option::is_none")]
    pub expiration: Option<OffsetDateTime>,
}
