use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use time::OffsetDateTime;

use crate::BackendInfo;

#[derive(Debug, Serialize, Deserialize, Default, PartialEq, Eq)]
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

    #[serde(rename = "expiration", with = "time::serde::rfc3339::option")]
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
    pub name: Option<String>,

    #[serde(rename = "description", skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    #[serde(rename = "expiration", with = "time::serde::rfc3339::option")]
    pub expiration: Option<OffsetDateTime>,
}

impl AddServiceAccountReq {
    pub fn validate(&self) -> Result<(), String> {
        if self.access_key.is_empty() {
            return Err("accessKey is empty".to_string());
        }

        if self.secret_key.is_empty() {
            return Err("secretKey is empty".to_string());
        }

        if self.name.is_none() {
            return Err("name is empty".to_string());
        }

        // TODO: validate

        Ok(())
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

#[derive(Serialize)]
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

#[derive(Debug, Serialize, Deserialize)]
pub struct UpdateServiceAccountReq {
    #[serde(rename = "newPolicy", skip_serializing_if = "Option::is_none")]
    pub new_policy: Option<String>,

    #[serde(rename = "newSecretKey", skip_serializing_if = "Option::is_none")]
    pub new_secret_key: Option<String>,

    #[serde(rename = "newStatus", skip_serializing_if = "Option::is_none")]
    pub new_status: Option<String>,

    #[serde(rename = "newName", skip_serializing_if = "Option::is_none")]
    pub new_name: Option<String>,

    #[serde(rename = "newDescription", skip_serializing_if = "Option::is_none")]
    pub new_description: Option<String>,

    #[serde(rename = "newExpiration", skip_serializing_if = "Option::is_none")]
    #[serde(with = "time::serde::rfc3339::option")]
    pub new_expiration: Option<OffsetDateTime>,
}

impl UpdateServiceAccountReq {
    pub fn validate(&self) -> Result<(), String> {
        // TODO: validate
        Ok(())
    }
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
            policy: Some("ReadOnlyAccess".to_string()),
            target_user: Some("testuser".to_string()),
            access_key: "AKIAIOSFODNN7EXAMPLE".to_string(),
            secret_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string(),
            name: Some("test-service".to_string()),
            description: Some("Test service account".to_string()),
            expiration: None,
        };

        let result = req.validate();
        assert!(result.is_ok());
    }

    #[test]
    fn test_add_service_account_req_validate_empty_access_key() {
        let req = AddServiceAccountReq {
            policy: None,
            target_user: None,
            access_key: "".to_string(),
            secret_key: "secret".to_string(),
            name: Some("test".to_string()),
            description: None,
            expiration: None,
        };

        let result = req.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("accessKey is empty"));
    }

    #[test]
    fn test_add_service_account_req_validate_empty_secret_key() {
        let req = AddServiceAccountReq {
            policy: None,
            target_user: None,
            access_key: "AKIAIOSFODNN7EXAMPLE".to_string(),
            secret_key: "".to_string(),
            name: Some("test".to_string()),
            description: None,
            expiration: None,
        };

        let result = req.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("secretKey is empty"));
    }

    #[test]
    fn test_add_service_account_req_validate_empty_name() {
        let req = AddServiceAccountReq {
            policy: None,
            target_user: None,
            access_key: "AKIAIOSFODNN7EXAMPLE".to_string(),
            secret_key: "secret".to_string(),
            name: None,
            description: None,
            expiration: None,
        };

        let result = req.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("name is empty"));
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
            new_policy: Some("FullAccess".to_string()),
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
            updated_at: None,
        };

        let json = serde_json::to_string(&user_info).unwrap();
        let deserialized: UserInfo = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.secret_key.unwrap(), "secret123");
        assert_eq!(deserialized.policy_name.unwrap(), "ReadOnlyAccess");
        assert_eq!(deserialized.status, AccountStatus::Enabled);
        assert_eq!(deserialized.member_of.unwrap().len(), 1);
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
        assert!(!format!("{:?}", account_status).is_empty());
        assert!(!format!("{:?}", user_auth_type).is_empty());
        assert!(!format!("{:?}", user_info).is_empty());
        assert!(!format!("{:?}", service_account).is_empty());
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
            policy: Some("".to_string()),
            target_user: Some("".to_string()),
            access_key: "valid_key".to_string(),
            secret_key: "valid_secret".to_string(),
            name: Some("valid_name".to_string()),
            description: Some("".to_string()),
            expiration: None,
        };

        // Should still validate successfully with empty optional strings
        assert!(req.validate().is_ok());

        // Test very long strings
        let long_string = "a".repeat(1000);
        let long_req = AddServiceAccountReq {
            policy: Some(long_string.clone()),
            target_user: Some(long_string.clone()),
            access_key: long_string.clone(),
            secret_key: long_string.clone(),
            name: Some(long_string.clone()),
            description: Some(long_string),
            expiration: None,
        };

        assert!(long_req.validate().is_ok());
    }
}
