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
