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

use crate::{GroupAddRemove, GroupDesc, SRSvcAccCreate, UserInfo};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use time::OffsetDateTime;

pub const SITE_REPL_API_VERSION: &str = "1";

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PeerSite {
    #[serde(default)]
    pub name: String,
    #[serde(rename = "endpoints", default)]
    pub endpoint: String,
    #[serde(rename = "accessKey", default)]
    pub access_key: String,
    #[serde(rename = "secretKey", default)]
    pub secret_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ReplicateAddStatus {
    #[serde(default)]
    pub success: bool,
    #[serde(default)]
    pub status: String,
    #[serde(rename = "errorDetail", skip_serializing_if = "String::is_empty", default)]
    pub err_detail: String,
    #[serde(rename = "initialSyncErrorMessage", skip_serializing_if = "String::is_empty", default)]
    pub initial_sync_error_message: String,
    #[serde(rename = "apiVersion", skip_serializing_if = "Option::is_none")]
    pub api_version: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SiteReplicationInfo {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub name: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sites: Vec<PeerInfo>,
    #[serde(rename = "serviceAccountAccessKey", default, skip_serializing_if = "String::is_empty")]
    pub service_account_access_key: String,
    #[serde(rename = "apiVersion", skip_serializing_if = "Option::is_none")]
    pub api_version: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SRPeerJoinReq {
    #[serde(rename = "svcAcctAccessKey", default)]
    pub svc_acct_access_key: String,
    #[serde(rename = "svcAcctSecretKey", default)]
    pub svc_acct_secret_key: String,
    #[serde(rename = "svcAcctParent", default)]
    pub svc_acct_parent: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub peers: BTreeMap<String, PeerInfo>,
    #[serde(
        rename = "updatedAt",
        default,
        with = "time::serde::rfc3339::option",
        skip_serializing_if = "Option::is_none"
    )]
    pub updated_at: Option<OffsetDateTime>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct BucketBandwidth {
    #[serde(rename = "bandwidthLimitPerBucket", default)]
    pub limit: u64,
    #[serde(default)]
    pub set: bool,
    #[serde(
        rename = "updatedAt",
        default,
        with = "time::serde::rfc3339::option",
        skip_serializing_if = "Option::is_none"
    )]
    pub updated_at: Option<OffsetDateTime>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum SyncStatus {
    #[serde(rename = "enable")]
    Enable,
    #[serde(rename = "disable")]
    Disable,
    #[default]
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PeerInfo {
    #[serde(default)]
    pub endpoint: String,
    #[serde(default)]
    pub name: String,
    #[serde(rename = "deploymentID", default)]
    pub deployment_id: String,
    #[serde(rename = "sync", default)]
    pub sync_state: SyncStatus,
    #[serde(rename = "defaultbandwidth", default)]
    pub default_bandwidth: BucketBandwidth,
    #[serde(rename = "replicate-ilm-expiry", default)]
    pub replicate_ilm_expiry: bool,
    #[serde(rename = "objectNamingMode", default, skip_serializing_if = "String::is_empty")]
    pub object_naming_mode: String,
    #[serde(rename = "apiVersion", skip_serializing_if = "Option::is_none")]
    pub api_version: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SRPolicyMapping {
    #[serde(rename = "userOrGroup", default)]
    pub user_or_group: String,
    #[serde(rename = "userType", default)]
    pub user_type: u64,
    #[serde(rename = "isGroup", default)]
    pub is_group: bool,
    #[serde(default)]
    pub policy: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub provider: String,
    #[serde(rename = "configID", default, skip_serializing_if = "String::is_empty")]
    pub config_id: String,
    #[serde(
        rename = "createdAt",
        default,
        with = "time::serde::rfc3339::option",
        skip_serializing_if = "Option::is_none"
    )]
    pub created_at: Option<OffsetDateTime>,
    #[serde(
        rename = "updatedAt",
        default,
        with = "time::serde::rfc3339::option",
        skip_serializing_if = "Option::is_none"
    )]
    pub updated_at: Option<OffsetDateTime>,
    #[serde(rename = "apiVersion", skip_serializing_if = "Option::is_none")]
    pub api_version: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SRSTSCredential {
    #[serde(rename = "accessKey", default)]
    pub access_key: String,
    #[serde(rename = "secretKey", default)]
    pub secret_key: String,
    #[serde(rename = "sessionToken", default)]
    pub session_token: String,
    #[serde(rename = "parentUser", default)]
    pub parent_user: String,
    #[serde(rename = "parentPolicyMapping", default, skip_serializing_if = "String::is_empty")]
    pub parent_policy_mapping: String,
    #[serde(rename = "apiVersion", skip_serializing_if = "Option::is_none")]
    pub api_version: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SRExternalUser {
    #[serde(rename = "apiVersion", skip_serializing_if = "Option::is_none")]
    pub api_version: Option<String>,
    #[serde(default)]
    pub name: String,
    #[serde(rename = "isDeleteReq", default)]
    pub is_delete_req: bool,
    #[serde(rename = "openIDUser", skip_serializing_if = "Option::is_none")]
    pub open_id_user: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SRLDAPUser {
    #[serde(default)]
    pub dn: String,
    #[serde(default)]
    pub username: String,
    #[serde(rename = "validatedDN", default, skip_serializing_if = "String::is_empty")]
    pub validated_dn: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub groups: Vec<String>,
    #[serde(default, with = "time::serde::rfc3339::option", skip_serializing_if = "Option::is_none")]
    pub expiry: Option<OffsetDateTime>,
    #[serde(rename = "isDeleteReq", default)]
    pub is_delete_req: bool,
    #[serde(rename = "configName", default, skip_serializing_if = "String::is_empty")]
    pub config_name: String,
    #[serde(
        rename = "updatedAt",
        default,
        with = "time::serde::rfc3339::option",
        skip_serializing_if = "Option::is_none"
    )]
    pub updated_at: Option<OffsetDateTime>,
    #[serde(rename = "apiVersion", skip_serializing_if = "Option::is_none")]
    pub api_version: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct SRIAMUser {
    #[serde(rename = "accessKey", default)]
    pub access_key: String,
    #[serde(rename = "isDeleteReq", default)]
    pub is_delete_req: bool,
    #[serde(rename = "userReq", skip_serializing_if = "Option::is_none")]
    pub user_req: Option<crate::AddOrUpdateUserReq>,
    #[serde(rename = "apiVersion", skip_serializing_if = "Option::is_none")]
    pub api_version: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct SRGroupInfo {
    #[serde(rename = "updateReq", default)]
    pub update_req: GroupAddRemove,
    #[serde(rename = "apiVersion", skip_serializing_if = "Option::is_none")]
    pub api_version: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SRSvcAccUpdate {
    #[serde(rename = "accessKey", default)]
    pub access_key: String,
    #[serde(rename = "secretKey", default)]
    pub secret_key: String,
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub description: String,
    #[serde(rename = "sessionPolicy", default)]
    pub session_policy: crate::SRSessionPolicy,
    #[serde(
        rename = "expiration",
        default,
        with = "time::serde::rfc3339::option",
        skip_serializing_if = "Option::is_none"
    )]
    pub expiration: Option<OffsetDateTime>,
    #[serde(rename = "apiVersion", skip_serializing_if = "Option::is_none")]
    pub api_version: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SRSvcAccDelete {
    #[serde(rename = "accessKey", default)]
    pub access_key: String,
    #[serde(rename = "apiVersion", skip_serializing_if = "Option::is_none")]
    pub api_version: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SRSvcAccChange {
    #[serde(rename = "crSvcAccCreate", skip_serializing_if = "Option::is_none")]
    pub create: Option<SRSvcAccCreate>,
    #[serde(rename = "crSvcAccUpdate", skip_serializing_if = "Option::is_none")]
    pub update: Option<SRSvcAccUpdate>,
    #[serde(rename = "crSvcAccDelete", skip_serializing_if = "Option::is_none")]
    pub delete: Option<SRSvcAccDelete>,
    #[serde(rename = "apiVersion", skip_serializing_if = "Option::is_none")]
    pub api_version: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SRCredInfo {
    #[serde(rename = "accessKey", default)]
    pub access_key: String,
    #[serde(rename = "iamUserType", default)]
    pub iam_user_type: u64,
    #[serde(rename = "isDeleteReq", default)]
    pub is_delete_req: bool,
    #[serde(rename = "userIdentityJSON", default, skip_serializing_if = "Option::is_none")]
    pub user_identity_json: Option<Value>,
    #[serde(rename = "apiVersion", skip_serializing_if = "Option::is_none")]
    pub api_version: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct SRIAMItem {
    #[serde(default)]
    pub r#type: String,
    #[serde(default)]
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy: Option<Value>,
    #[serde(rename = "policyMapping", skip_serializing_if = "Option::is_none")]
    pub policy_mapping: Option<SRPolicyMapping>,
    #[serde(rename = "groupInfo", skip_serializing_if = "Option::is_none")]
    pub group_info: Option<SRGroupInfo>,
    #[serde(rename = "credentialChange", skip_serializing_if = "Option::is_none")]
    pub credential_info: Option<SRCredInfo>,
    #[serde(rename = "serviceAccountChange", skip_serializing_if = "Option::is_none")]
    pub svc_acc_change: Option<SRSvcAccChange>,
    #[serde(rename = "stsCredential", skip_serializing_if = "Option::is_none")]
    pub sts_credential: Option<SRSTSCredential>,
    #[serde(rename = "iamUser", skip_serializing_if = "Option::is_none")]
    pub iam_user: Option<SRIAMUser>,
    #[serde(rename = "externalUser", skip_serializing_if = "Option::is_none")]
    pub external_user: Option<SRExternalUser>,
    #[serde(rename = "ldapUser", skip_serializing_if = "Option::is_none")]
    pub ldap_user: Option<SRLDAPUser>,
    #[serde(
        rename = "updatedAt",
        default,
        with = "time::serde::rfc3339::option",
        skip_serializing_if = "Option::is_none"
    )]
    pub updated_at: Option<OffsetDateTime>,
    #[serde(rename = "apiVersion", skip_serializing_if = "Option::is_none")]
    pub api_version: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SRBucketMeta {
    #[serde(default)]
    pub r#type: String,
    #[serde(default)]
    pub bucket: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy: Option<Value>,
    #[serde(rename = "versioningConfig", skip_serializing_if = "Option::is_none")]
    pub versioning: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tags: Option<String>,
    #[serde(rename = "objectLockConfig", skip_serializing_if = "Option::is_none")]
    pub object_lock_config: Option<String>,
    #[serde(rename = "sseConfig", skip_serializing_if = "Option::is_none")]
    pub sse_config: Option<String>,
    #[serde(rename = "replicationConfig", skip_serializing_if = "Option::is_none")]
    pub replication_config: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quota: Option<Value>,
    #[serde(rename = "expLCConfig", skip_serializing_if = "Option::is_none")]
    pub expiry_lc_config: Option<String>,
    #[serde(
        rename = "updatedAt",
        default,
        with = "time::serde::rfc3339::option",
        skip_serializing_if = "Option::is_none"
    )]
    pub updated_at: Option<OffsetDateTime>,
    #[serde(
        rename = "expiryUpdatedAt",
        default,
        with = "time::serde::rfc3339::option",
        skip_serializing_if = "Option::is_none"
    )]
    pub expiry_updated_at: Option<OffsetDateTime>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cors: Option<String>,
    #[serde(rename = "apiVersion", skip_serializing_if = "Option::is_none")]
    pub api_version: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SRBucketInfo {
    #[serde(default)]
    pub bucket: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy: Option<Value>,
    #[serde(rename = "versioningConfig", skip_serializing_if = "Option::is_none")]
    pub versioning: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tags: Option<String>,
    #[serde(rename = "objectLockConfig", skip_serializing_if = "Option::is_none")]
    pub object_lock_config: Option<String>,
    #[serde(rename = "sseConfig", skip_serializing_if = "Option::is_none")]
    pub sse_config: Option<String>,
    #[serde(rename = "replicationConfig", skip_serializing_if = "Option::is_none")]
    pub replication_config: Option<String>,
    #[serde(rename = "quotaConfig", skip_serializing_if = "Option::is_none")]
    pub quota_config: Option<String>,
    #[serde(rename = "expLCConfig", skip_serializing_if = "Option::is_none")]
    pub expiry_lc_config: Option<String>,
    #[serde(rename = "corsConfig", skip_serializing_if = "Option::is_none")]
    pub cors_config: Option<String>,
    #[serde(
        rename = "policyTimestamp",
        default,
        with = "time::serde::rfc3339::option",
        skip_serializing_if = "Option::is_none"
    )]
    pub policy_updated_at: Option<OffsetDateTime>,
    #[serde(
        rename = "tagTimestamp",
        default,
        with = "time::serde::rfc3339::option",
        skip_serializing_if = "Option::is_none"
    )]
    pub tag_config_updated_at: Option<OffsetDateTime>,
    #[serde(
        rename = "olockTimestamp",
        default,
        with = "time::serde::rfc3339::option",
        skip_serializing_if = "Option::is_none"
    )]
    pub object_lock_config_updated_at: Option<OffsetDateTime>,
    #[serde(
        rename = "sseTimestamp",
        default,
        with = "time::serde::rfc3339::option",
        skip_serializing_if = "Option::is_none"
    )]
    pub sse_config_updated_at: Option<OffsetDateTime>,
    #[serde(
        rename = "versioningTimestamp",
        default,
        with = "time::serde::rfc3339::option",
        skip_serializing_if = "Option::is_none"
    )]
    pub versioning_config_updated_at: Option<OffsetDateTime>,
    #[serde(
        rename = "replicationConfigTimestamp",
        default,
        with = "time::serde::rfc3339::option",
        skip_serializing_if = "Option::is_none"
    )]
    pub replication_config_updated_at: Option<OffsetDateTime>,
    #[serde(
        rename = "quotaTimestamp",
        default,
        with = "time::serde::rfc3339::option",
        skip_serializing_if = "Option::is_none"
    )]
    pub quota_config_updated_at: Option<OffsetDateTime>,
    #[serde(
        rename = "expLCTimestamp",
        default,
        with = "time::serde::rfc3339::option",
        skip_serializing_if = "Option::is_none"
    )]
    pub expiry_lc_config_updated_at: Option<OffsetDateTime>,
    #[serde(
        rename = "bucketTimestamp",
        default,
        with = "time::serde::rfc3339::option",
        skip_serializing_if = "Option::is_none"
    )]
    pub created_at: Option<OffsetDateTime>,
    #[serde(
        rename = "bucketDeletedTimestamp",
        default,
        with = "time::serde::rfc3339::option",
        skip_serializing_if = "Option::is_none"
    )]
    pub deleted_at: Option<OffsetDateTime>,
    #[serde(
        rename = "corsTimestamp",
        default,
        with = "time::serde::rfc3339::option",
        skip_serializing_if = "Option::is_none"
    )]
    pub cors_config_updated_at: Option<OffsetDateTime>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub location: String,
    #[serde(rename = "apiVersion", skip_serializing_if = "Option::is_none")]
    pub api_version: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct OpenIDProviderSettings {
    #[serde(rename = "ClaimName", default, skip_serializing_if = "String::is_empty")]
    pub claim_name: String,
    #[serde(rename = "ClaimUserinfoEnabled", default)]
    pub claim_userinfo_enabled: bool,
    #[serde(rename = "RolePolicy", default, skip_serializing_if = "String::is_empty")]
    pub role_policy: String,
    #[serde(rename = "ClientID", default, skip_serializing_if = "String::is_empty")]
    pub client_id: String,
    #[serde(rename = "HashedClientSecret", default, skip_serializing_if = "String::is_empty")]
    pub hashed_client_secret: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct OpenIDSettings {
    #[serde(rename = "Enabled", default)]
    pub enabled: bool,
    #[serde(rename = "Region", default, skip_serializing_if = "String::is_empty")]
    pub region: String,
    #[serde(rename = "Roles", default, skip_serializing_if = "BTreeMap::is_empty")]
    pub roles: BTreeMap<String, OpenIDProviderSettings>,
    #[serde(rename = "ClaimProvider", default)]
    pub claim_provider: OpenIDProviderSettings,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct LDAPSettings {
    #[serde(rename = "IsLDAPEnabled", default)]
    pub is_ldap_enabled: bool,
    #[serde(rename = "LDAPUserDNSearchBase", default, skip_serializing_if = "String::is_empty")]
    pub ldap_user_dn_search_base: String,
    #[serde(rename = "LDAPUserDNSearchFilter", default, skip_serializing_if = "String::is_empty")]
    pub ldap_user_dn_search_filter: String,
    #[serde(rename = "LDAPGroupSearchBase", default, skip_serializing_if = "String::is_empty")]
    pub ldap_group_search_base: String,
    #[serde(rename = "LDAPGroupSearchFilter", default, skip_serializing_if = "String::is_empty")]
    pub ldap_group_search_filter: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct LDAPProviderSettings {
    #[serde(rename = "UserDNSearchBase", default, skip_serializing_if = "String::is_empty")]
    pub user_dn_search_base: String,
    #[serde(rename = "UserDNSearchFilter", default, skip_serializing_if = "String::is_empty")]
    pub user_dn_search_filter: String,
    #[serde(rename = "GroupSearchBase", default, skip_serializing_if = "String::is_empty")]
    pub group_search_base: String,
    #[serde(rename = "GroupSearchFilter", default, skip_serializing_if = "String::is_empty")]
    pub group_search_filter: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct LDAPConfigSettings {
    #[serde(rename = "Enabled", default)]
    pub enabled: bool,
    #[serde(rename = "Configs", default, skip_serializing_if = "BTreeMap::is_empty")]
    pub configs: BTreeMap<String, LDAPProviderSettings>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct IDPSettings {
    #[serde(rename = "LDAP", default)]
    pub ldap: LDAPSettings,
    #[serde(rename = "LDAPConfigs", default)]
    pub ldap_configs: LDAPConfigSettings,
    #[serde(rename = "OpenID", default)]
    pub open_id: OpenIDSettings,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SRIAMPolicy {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy: Option<Value>,
    #[serde(
        rename = "updatedAt",
        default,
        with = "time::serde::rfc3339::option",
        skip_serializing_if = "Option::is_none"
    )]
    pub updated_at: Option<OffsetDateTime>,
    #[serde(rename = "apiVersion", skip_serializing_if = "Option::is_none")]
    pub api_version: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ILMExpiryRule {
    #[serde(rename = "ilm-rule", default, skip_serializing_if = "String::is_empty")]
    pub ilm_rule: String,
    #[serde(default)]
    pub bucket: String,
    #[serde(
        rename = "updatedAt",
        default,
        with = "time::serde::rfc3339::option",
        skip_serializing_if = "Option::is_none"
    )]
    pub updated_at: Option<OffsetDateTime>,
    #[serde(rename = "apiVersion", skip_serializing_if = "Option::is_none")]
    pub api_version: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SRStateInfo {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub name: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub peers: BTreeMap<String, PeerInfo>,
    #[serde(
        rename = "updatedAt",
        default,
        with = "time::serde::rfc3339::option",
        skip_serializing_if = "Option::is_none"
    )]
    pub updated_at: Option<OffsetDateTime>,
    #[serde(rename = "apiVersion", skip_serializing_if = "Option::is_none")]
    pub api_version: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct SRInfo {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub name: String,
    #[serde(rename = "deploymentID", default, skip_serializing_if = "String::is_empty")]
    pub deployment_id: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub buckets: BTreeMap<String, SRBucketInfo>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub policies: BTreeMap<String, SRIAMPolicy>,
    #[serde(rename = "userPolicies", default, skip_serializing_if = "BTreeMap::is_empty")]
    pub user_policies: BTreeMap<String, SRPolicyMapping>,
    #[serde(rename = "userInfoMap", default, skip_serializing_if = "BTreeMap::is_empty")]
    pub user_info_map: BTreeMap<String, UserInfo>,
    #[serde(rename = "groupDescMap", default, skip_serializing_if = "BTreeMap::is_empty")]
    pub group_desc_map: BTreeMap<String, GroupDesc>,
    #[serde(rename = "groupPolicies", default, skip_serializing_if = "BTreeMap::is_empty")]
    pub group_policies: BTreeMap<String, SRPolicyMapping>,
    #[serde(rename = "replicationCfg", default, skip_serializing_if = "BTreeMap::is_empty")]
    pub replication_cfg: BTreeMap<String, Value>,
    #[serde(rename = "ilmExpiryRules", default, skip_serializing_if = "BTreeMap::is_empty")]
    pub ilm_expiry_rules: BTreeMap<String, ILMExpiryRule>,
    #[serde(default)]
    pub state: SRStateInfo,
    #[serde(rename = "apiVersion", skip_serializing_if = "Option::is_none")]
    pub api_version: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SRPolicyStatsSummary {
    #[serde(rename = "DeploymentID", default, skip_serializing_if = "String::is_empty")]
    pub deployment_id: String,
    #[serde(rename = "PolicyMismatch", default)]
    pub policy_mismatch: bool,
    #[serde(rename = "HasPolicy", default)]
    pub has_policy: bool,
    #[serde(rename = "apiVersion", skip_serializing_if = "Option::is_none")]
    pub api_version: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SRUserStatsSummary {
    #[serde(rename = "DeploymentID", default, skip_serializing_if = "String::is_empty")]
    pub deployment_id: String,
    #[serde(rename = "PolicyMismatch", default)]
    pub policy_mismatch: bool,
    #[serde(rename = "UserInfoMismatch", default)]
    pub user_info_mismatch: bool,
    #[serde(rename = "HasUser", default)]
    pub has_user: bool,
    #[serde(rename = "HasPolicyMapping", default)]
    pub has_policy_mapping: bool,
    #[serde(rename = "apiVersion", skip_serializing_if = "Option::is_none")]
    pub api_version: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SRGroupStatsSummary {
    #[serde(rename = "DeploymentID", default, skip_serializing_if = "String::is_empty")]
    pub deployment_id: String,
    #[serde(rename = "PolicyMismatch", default)]
    pub policy_mismatch: bool,
    #[serde(rename = "HasGroup", default)]
    pub has_group: bool,
    #[serde(rename = "GroupDescMismatch", default)]
    pub group_desc_mismatch: bool,
    #[serde(rename = "HasPolicyMapping", default)]
    pub has_policy_mapping: bool,
    #[serde(rename = "apiVersion", skip_serializing_if = "Option::is_none")]
    pub api_version: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SRBucketStatsSummary {
    #[serde(rename = "DeploymentID", default, skip_serializing_if = "String::is_empty")]
    pub deployment_id: String,
    #[serde(rename = "HasBucket", default)]
    pub has_bucket: bool,
    #[serde(rename = "BucketMarkedDeleted", default)]
    pub bucket_marked_deleted: bool,
    #[serde(rename = "TagMismatch", default)]
    pub tag_mismatch: bool,
    #[serde(rename = "VersioningConfigMismatch", default)]
    pub versioning_config_mismatch: bool,
    #[serde(rename = "OLockConfigMismatch", default)]
    pub object_lock_config_mismatch: bool,
    #[serde(rename = "PolicyMismatch", default)]
    pub policy_mismatch: bool,
    #[serde(rename = "SSEConfigMismatch", default)]
    pub sse_config_mismatch: bool,
    #[serde(rename = "ReplicationCfgMismatch", default)]
    pub replication_cfg_mismatch: bool,
    #[serde(rename = "QuotaCfgMismatch", default)]
    pub quota_cfg_mismatch: bool,
    #[serde(rename = "CorsCfgMismatch", default)]
    pub cors_cfg_mismatch: bool,
    #[serde(rename = "HasTagsSet", default)]
    pub has_tags_set: bool,
    #[serde(rename = "HasOLockConfigSet", default)]
    pub has_object_lock_config_set: bool,
    #[serde(rename = "HasPolicySet", default)]
    pub has_policy_set: bool,
    #[serde(rename = "HasSSECfgSet", default)]
    pub has_sse_cfg_set: bool,
    #[serde(rename = "HasReplicationCfg", default)]
    pub has_replication_cfg: bool,
    #[serde(rename = "HasQuotaCfgSet", default)]
    pub has_quota_cfg_set: bool,
    #[serde(rename = "HasCorsCfgSet", default)]
    pub has_cors_cfg_set: bool,
    #[serde(rename = "apiVersion", skip_serializing_if = "Option::is_none")]
    pub api_version: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SRILMExpiryStatsSummary {
    #[serde(rename = "DeploymentID", default, skip_serializing_if = "String::is_empty")]
    pub deployment_id: String,
    #[serde(rename = "ILMExpiryRuleMismatch", default)]
    pub ilm_expiry_rule_mismatch: bool,
    #[serde(rename = "HasILMExpiryRules", default)]
    pub has_ilm_expiry_rules: bool,
    #[serde(rename = "apiVersion", skip_serializing_if = "Option::is_none")]
    pub api_version: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SRSiteSummary {
    #[serde(rename = "ReplicatedBuckets", default)]
    pub replicated_buckets: usize,
    #[serde(rename = "ReplicatedTags", default)]
    pub replicated_tags: usize,
    #[serde(rename = "ReplicatedBucketPolicies", default)]
    pub replicated_bucket_policies: usize,
    #[serde(rename = "ReplicatedIAMPolicies", default)]
    pub replicated_iam_policies: usize,
    #[serde(rename = "ReplicatedUsers", default)]
    pub replicated_users: usize,
    #[serde(rename = "ReplicatedGroups", default)]
    pub replicated_groups: usize,
    #[serde(rename = "ReplicatedLockConfig", default)]
    pub replicated_lock_config: usize,
    #[serde(rename = "ReplicatedSSEConfig", default)]
    pub replicated_sse_config: usize,
    #[serde(rename = "ReplicatedVersioningConfig", default)]
    pub replicated_versioning_config: usize,
    #[serde(rename = "ReplicatedQuotaConfig", default)]
    pub replicated_quota_config: usize,
    #[serde(rename = "ReplicatedUserPolicyMappings", default)]
    pub replicated_user_policy_mappings: usize,
    #[serde(rename = "ReplicatedGroupPolicyMappings", default)]
    pub replicated_group_policy_mappings: usize,
    #[serde(rename = "ReplicatedILMExpiryRules", default)]
    pub replicated_ilm_expiry_rules: usize,
    #[serde(rename = "ReplicatedCorsConfig", default)]
    pub replicated_cors_config: usize,
    #[serde(rename = "TotalBucketsCount", default)]
    pub total_buckets_count: usize,
    #[serde(rename = "TotalTagsCount", default)]
    pub total_tags_count: usize,
    #[serde(rename = "TotalBucketPoliciesCount", default)]
    pub total_bucket_policies_count: usize,
    #[serde(rename = "TotalIAMPoliciesCount", default)]
    pub total_iam_policies_count: usize,
    #[serde(rename = "TotalLockConfigCount", default)]
    pub total_lock_config_count: usize,
    #[serde(rename = "TotalSSEConfigCount", default)]
    pub total_sse_config_count: usize,
    #[serde(rename = "TotalVersioningConfigCount", default)]
    pub total_versioning_config_count: usize,
    #[serde(rename = "TotalQuotaConfigCount", default)]
    pub total_quota_config_count: usize,
    #[serde(rename = "TotalUsersCount", default)]
    pub total_users_count: usize,
    #[serde(rename = "TotalGroupsCount", default)]
    pub total_groups_count: usize,
    #[serde(rename = "TotalUserPolicyMappingCount", default)]
    pub total_user_policy_mapping_count: usize,
    #[serde(rename = "TotalGroupPolicyMappingCount", default)]
    pub total_group_policy_mapping_count: usize,
    #[serde(rename = "TotalILMExpiryRulesCount", default)]
    pub total_ilm_expiry_rules_count: usize,
    #[serde(rename = "TotalCorsConfigCount", default)]
    pub total_cors_config_count: usize,
    #[serde(rename = "apiVersion", skip_serializing_if = "Option::is_none")]
    pub api_version: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct WorkerStat {
    #[serde(rename = "curr", default)]
    pub curr: i32,
    #[serde(rename = "avg", default)]
    pub avg: f64,
    #[serde(rename = "max", default)]
    pub max: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct QStat {
    #[serde(default)]
    pub count: f64,
    #[serde(default)]
    pub bytes: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct InQueueMetric {
    #[serde(default)]
    pub curr: QStat,
    #[serde(default)]
    pub avg: QStat,
    #[serde(default)]
    pub max: QStat,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct InProgressMetric {
    #[serde(default)]
    pub curr: QStat,
    #[serde(default)]
    pub avg: QStat,
    #[serde(default)]
    pub max: QStat,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Counter {
    #[serde(rename = "last1hr", default)]
    pub last_1hr: u64,
    #[serde(rename = "last1m", default)]
    pub last_1m: u64,
    #[serde(default)]
    pub total: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ReplicationWindowedStats {
    #[serde(default)]
    pub curr: u64,
    #[serde(rename = "avgRate", default)]
    pub avg_rate: f64,
    #[serde(rename = "peakRate", default)]
    pub peak_rate: f64,
    #[serde(default)]
    pub total: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ReplProxyMetric {
    #[serde(rename = "putTaggingProxyTotal", default)]
    pub put_tag_total: u64,
    #[serde(rename = "getTaggingProxyTotal", default)]
    pub get_tag_total: u64,
    #[serde(rename = "removeTaggingProxyTotal", default)]
    pub remove_tag_total: u64,
    #[serde(rename = "getProxyTotal", default)]
    pub get_total: u64,
    #[serde(rename = "headProxyTotal", default)]
    pub head_total: u64,
    #[serde(rename = "putTaggingProxyFailed", default)]
    pub put_tag_failed_total: u64,
    #[serde(rename = "getTaggingProxyFailed", default)]
    pub get_tag_failed_total: u64,
    #[serde(rename = "removeTaggingProxyFailed", default)]
    pub remove_tag_failed_total: u64,
    #[serde(rename = "getProxyFailed", default)]
    pub get_failed_total: u64,
    #[serde(rename = "headProxyFailed", default)]
    pub head_failed_total: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct LatencyStat {
    #[serde(rename = "curr", default)]
    pub curr_ns: i64,
    #[serde(rename = "avg", default)]
    pub average_ns: i64,
    #[serde(rename = "max", default)]
    pub max_ns: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RStat {
    #[serde(default)]
    pub count: f64,
    #[serde(default)]
    pub bytes: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TimedErrStats {
    #[serde(rename = "lastMinute", default)]
    pub last_minute: RStat,
    #[serde(rename = "lastHour", default)]
    pub last_hour: RStat,
    #[serde(rename = "totals", default)]
    pub totals: RStat,
    #[serde(rename = "errCounts", default, skip_serializing_if = "BTreeMap::is_empty")]
    pub err_counts: BTreeMap<String, i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct StatRecorder {
    #[serde(default)]
    pub total: i64,
    #[serde(default)]
    pub avg: i64,
    #[serde(default)]
    pub max: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DowntimeInfo {
    #[serde(default)]
    pub duration: StatRecorder,
    #[serde(default)]
    pub count: StatRecorder,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SRMetric {
    #[serde(rename = "deploymentID", default, skip_serializing_if = "String::is_empty")]
    pub deployment_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub endpoint: String,
    #[serde(rename = "totalDowntime", default)]
    pub total_downtime_ns: i64,
    #[serde(
        rename = "lastOnline",
        default,
        with = "time::serde::rfc3339::option",
        skip_serializing_if = "Option::is_none"
    )]
    pub last_online: Option<OffsetDateTime>,
    #[serde(rename = "isOnline", default)]
    pub online: bool,
    #[serde(default)]
    pub latency: LatencyStat,
    #[serde(rename = "replicatedSize", default)]
    pub replicated_size: i64,
    #[serde(rename = "replicatedCount", default)]
    pub replicated_count: i64,
    #[serde(default)]
    pub failed: TimedErrStats,
    #[serde(rename = "transferSummary", default, skip_serializing_if = "HashMap::is_empty")]
    pub transfer_summary: HashMap<String, Value>,
    #[serde(rename = "mrfStats", default, skip_serializing_if = "HashMap::is_empty")]
    pub mrf_stats: HashMap<String, Value>,
    #[serde(rename = "downtimeInfo", default)]
    pub downtime_info: DowntimeInfo,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SRMetricsSummary {
    #[serde(rename = "activeWorkers", default)]
    pub active_workers: WorkerStat,
    #[serde(rename = "replicaSize", default)]
    pub replica_size: i64,
    #[serde(rename = "replicaCount", default)]
    pub replica_count: i64,
    #[serde(default)]
    pub queued: InQueueMetric,
    #[serde(rename = "inProgress", default)]
    pub in_progress: InProgressMetric,
    #[serde(default)]
    pub proxied: ReplProxyMetric,
    #[serde(rename = "replMetrics", default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metrics: BTreeMap<String, SRMetric>,
    #[serde(default)]
    pub uptime: i64,
    #[serde(default)]
    pub retries: Counter,
    #[serde(default)]
    pub errors: Counter,
    #[serde(default)]
    pub replicated: ReplicationWindowedStats,
    #[serde(default)]
    pub received: ReplicationWindowedStats,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SRStatusInfo {
    #[serde(default)]
    pub enabled: bool,
    #[serde(rename = "MaxBuckets", default)]
    pub max_buckets: usize,
    #[serde(rename = "MaxUsers", default)]
    pub max_users: usize,
    #[serde(rename = "MaxGroups", default)]
    pub max_groups: usize,
    #[serde(rename = "MaxPolicies", default)]
    pub max_policies: usize,
    #[serde(rename = "MaxILMExpiryRules", default)]
    pub max_ilm_expiry_rules: usize,
    #[serde(rename = "Sites", default, skip_serializing_if = "BTreeMap::is_empty")]
    pub sites: BTreeMap<String, PeerInfo>,
    #[serde(rename = "StatsSummary", default, skip_serializing_if = "BTreeMap::is_empty")]
    pub stats_summary: BTreeMap<String, SRSiteSummary>,
    #[serde(rename = "BucketStats", default, skip_serializing_if = "BTreeMap::is_empty")]
    pub bucket_stats: BTreeMap<String, BTreeMap<String, SRBucketStatsSummary>>,
    #[serde(rename = "PolicyStats", default, skip_serializing_if = "BTreeMap::is_empty")]
    pub policy_stats: BTreeMap<String, BTreeMap<String, SRPolicyStatsSummary>>,
    #[serde(rename = "UserStats", default, skip_serializing_if = "BTreeMap::is_empty")]
    pub user_stats: BTreeMap<String, BTreeMap<String, SRUserStatsSummary>>,
    #[serde(rename = "GroupStats", default, skip_serializing_if = "BTreeMap::is_empty")]
    pub group_stats: BTreeMap<String, BTreeMap<String, SRGroupStatsSummary>>,
    #[serde(rename = "PeerStates", default, skip_serializing_if = "BTreeMap::is_empty")]
    pub peer_states: BTreeMap<String, SRStateInfo>,
    #[serde(rename = "Metrics", default)]
    pub metrics: SRMetricsSummary,
    #[serde(rename = "ILMExpiryStats", default, skip_serializing_if = "BTreeMap::is_empty")]
    pub ilm_expiry_stats: BTreeMap<String, BTreeMap<String, SRILMExpiryStatsSummary>>,
    #[serde(rename = "apiVersion", skip_serializing_if = "Option::is_none")]
    pub api_version: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ReplicateEditStatus {
    #[serde(default)]
    pub success: bool,
    #[serde(default)]
    pub status: String,
    #[serde(rename = "errorDetail", skip_serializing_if = "String::is_empty", default)]
    pub err_detail: String,
    #[serde(rename = "apiVersion", skip_serializing_if = "Option::is_none")]
    pub api_version: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ReplicateRemoveStatus {
    #[serde(default)]
    pub status: String,
    #[serde(rename = "errorDetail", skip_serializing_if = "String::is_empty", default)]
    pub err_detail: String,
    #[serde(rename = "apiVersion", skip_serializing_if = "Option::is_none")]
    pub api_version: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SRRemoveReq {
    #[serde(rename = "requestingDepID", default, skip_serializing_if = "String::is_empty")]
    pub requesting_dep_id: String,
    #[serde(
        rename = "sites",
        default,
        deserialize_with = "deserialize_vec_null_default",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub site_names: Vec<String>,
    #[serde(rename = "all", default)]
    pub remove_all: bool,
}

fn deserialize_vec_null_default<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Ok(Option::<Vec<String>>::deserialize(deserializer)?.unwrap_or_default())
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SRStateEditReq {
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub peers: BTreeMap<String, PeerInfo>,
    #[serde(
        rename = "updatedAt",
        default,
        with = "time::serde::rfc3339::option",
        skip_serializing_if = "Option::is_none"
    )]
    pub updated_at: Option<OffsetDateTime>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ResyncBucketStatus {
    #[serde(default)]
    pub bucket: String,
    #[serde(default)]
    pub status: String,
    #[serde(rename = "errorDetail", skip_serializing_if = "String::is_empty", default)]
    pub err_detail: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SRResyncOpStatus {
    #[serde(rename = "op", default)]
    pub op_type: String,
    #[serde(rename = "id", default)]
    pub resync_id: String,
    #[serde(default)]
    pub status: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub buckets: Vec<ResyncBucketStatus>,
    #[serde(rename = "errorDetail", skip_serializing_if = "String::is_empty", default)]
    pub err_detail: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SiteNetPerfNodeResult {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub endpoint: String,
    #[serde(default)]
    pub tx: u64,
    #[serde(rename = "txTotalDuration", default)]
    pub tx_total_duration_ns: i64,
    #[serde(default)]
    pub rx: u64,
    #[serde(rename = "rxTotalDuration", default)]
    pub rx_total_duration_ns: i64,
    #[serde(rename = "totalConn", default)]
    pub total_conn: u64,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub error: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SiteNetPerfResult {
    #[serde(rename = "nodeResults", default, skip_serializing_if = "Vec::is_empty")]
    pub node_results: Vec<SiteNetPerfNodeResult>,
}
