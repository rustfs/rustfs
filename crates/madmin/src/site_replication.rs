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
use std::fmt;
use time::OffsetDateTime;

pub const SITE_REPL_API_VERSION: &str = "1";

/// `SRIAMItem` type for replicated STS credentials, matching MinIO madmin-go
/// `SRIAMItemSTSAcc`. MinIO peers reject any other value as an invalid request.
pub const SR_IAM_ITEM_STS_ACC: &str = "sts-account";

/// STS item type emitted by RustFS releases prior to the MinIO alignment.
/// Never emitted anymore, but accepted inbound permanently so mixed-version
/// RustFS sites keep replicating STS credentials during rolling upgrades.
pub const SR_IAM_ITEM_STS_ACC_LEGACY: &str = "sts-credential";

#[derive(Clone, Serialize, Deserialize, Default)]
pub struct PeerSite {
    #[serde(default)]
    pub name: String,
    #[serde(rename = "endpoints", default)]
    pub endpoint: String,
    #[serde(rename = "accessKey", default)]
    pub access_key: String,
    #[serde(rename = "secretKey", default)]
    pub secret_key: String,
    #[serde(rename = "skipTlsVerify", default)]
    pub skip_tls_verify: bool,
    #[serde(rename = "caCertPem", default)]
    pub ca_cert_pem: String,
}

impl fmt::Debug for PeerSite {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PeerSite")
            .field("name", &self.name)
            .field("endpoint", &self.endpoint)
            .field("skip_tls_verify", &self.skip_tls_verify)
            .field("has_custom_ca", &!self.ca_cert_pem.is_empty())
            .finish()
    }
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
    /// Outstanding peer deliveries. Absent when the retry queue is empty, so a
    /// healthy site serializes exactly as it did before this field existed.
    /// Present means peer operations are failing even if `enabled` is true.
    #[serde(rename = "retryStats", default, skip_serializing_if = "Option::is_none")]
    pub retry_stats: Option<SRRetryStats>,
    /// A multi-step lifecycle operation this site has not finished — most
    /// importantly a removal that could not reach its peers, which makes the
    /// site reject peer operations while `enabled` may still read true.
    #[serde(rename = "pendingOperation", default, skip_serializing_if = "Option::is_none")]
    pub pending_operation: Option<SRPendingOperation>,
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

#[derive(Clone, Serialize, Deserialize, Default)]
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
    #[serde(rename = "skipTlsVerify", default)]
    pub skip_tls_verify: bool,
    #[serde(rename = "caCertPem", default)]
    pub ca_cert_pem: String,
    #[serde(rename = "apiVersion", skip_serializing_if = "Option::is_none")]
    pub api_version: Option<String>,
}

impl fmt::Debug for PeerInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PeerInfo")
            .field("endpoint", &self.endpoint)
            .field("name", &self.name)
            .field("deployment_id", &self.deployment_id)
            .field("skip_tls_verify", &self.skip_tls_verify)
            .field("has_custom_ca", &!self.ca_cert_pem.is_empty())
            .finish()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SRPolicyMapping {
    #[serde(rename = "userOrGroup", default)]
    pub user_or_group: String,
    /// MinIO IAMUserType wire value (cmd/iam.go): unknown = -1, regUser = 0,
    /// stsUser = 1, svcUser = 2. Signed because MinIO sends -1 for group
    /// mappings. This is NOT the RustFS-internal `UserType` encoding; translate
    /// at the boundary with `rustfs_iam::store::{sr_wire_user_type, user_type_from_sr_wire}`.
    #[serde(rename = "userType", default)]
    pub user_type: i64,
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

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
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

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
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
pub struct SRSvcAccReplicationEnvelope {
    pub version: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SRSvcAccChange {
    #[serde(rename = "crSvcAccCreate", skip_serializing_if = "Option::is_none")]
    pub create: Option<SRSvcAccCreate>,
    #[serde(rename = "crSvcAccUpdate", skip_serializing_if = "Option::is_none")]
    pub update: Option<SRSvcAccUpdate>,
    #[serde(rename = "crSvcAccDelete", skip_serializing_if = "Option::is_none")]
    pub delete: Option<SRSvcAccDelete>,
    #[serde(rename = "oidcServiceAccountEnvelope", skip_serializing_if = "Option::is_none")]
    pub oidc_service_account_envelope: Option<SRSvcAccReplicationEnvelope>,
    #[serde(rename = "apiVersion", skip_serializing_if = "Option::is_none")]
    pub api_version: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SRCredInfo {
    #[serde(rename = "accessKey", default)]
    pub access_key: String,
    /// MinIO IAMUserType wire value (same table as `SRPolicyMapping::user_type`);
    /// signed because MinIO's unknown is -1.
    #[serde(rename = "iamUserType", default)]
    pub iam_user_type: i64,
    #[serde(rename = "isDeleteReq", default)]
    pub is_delete_req: bool,
    #[serde(rename = "userIdentityJSON", default, skip_serializing_if = "Option::is_none")]
    pub user_identity_json: Option<Value>,
    #[serde(rename = "apiVersion", skip_serializing_if = "Option::is_none")]
    pub api_version: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
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
    /// Set by a sender that merges replication configs under the derived
    /// site-rule contract (operator rule priorities verbatim, `site-repl-*`
    /// ids classified by id/ARN). A receiver merges a payload without it the
    /// pre-contract way; a pre-contract receiver ignores the field.
    #[serde(rename = "derivedRuleContract", default, skip_serializing_if = "std::ops::Not::not")]
    pub derived_rule_contract: bool,
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
    /// Whether every `site-repl-*` rule on the reporting site resolves to a usable bucket
    /// target. A rule without one silently drops every object, and the rule set alone cannot
    /// reveal it — the config is well-formed, the endpoint behind it is not reachable.
    ///
    /// `None` means the peer predates this field: treat it as "unknown", never as a fault.
    #[serde(rename = "replicationTargetsOnline", default, skip_serializing_if = "Option::is_none")]
    pub replication_targets_online: Option<bool>,
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
    #[serde(
        default,
        deserialize_with = "deserialize_null_default",
        skip_serializing_if = "BTreeMap::is_empty"
    )]
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

// madmin-go's SRInfo top-level fields carry no json tags (except APIVersion),
// so MinIO emits them in PascalCase; the aliases below accept that on
// deserialization while serialization stays camelCase. Nested structs
// (SRBucketInfo, SRStateInfo, ...) do have lowercase tags in madmin-go —
// do not spread aliases to them.
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct SRInfo {
    #[serde(alias = "Enabled", default)]
    pub enabled: bool,
    #[serde(alias = "Name", default, skip_serializing_if = "String::is_empty")]
    pub name: String,
    #[serde(
        rename = "deploymentID",
        alias = "DeploymentID",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub deployment_id: String,
    #[serde(
        alias = "Buckets",
        default,
        deserialize_with = "deserialize_null_default",
        skip_serializing_if = "BTreeMap::is_empty"
    )]
    pub buckets: BTreeMap<String, SRBucketInfo>,
    #[serde(
        alias = "Policies",
        default,
        deserialize_with = "deserialize_null_default",
        skip_serializing_if = "BTreeMap::is_empty"
    )]
    pub policies: BTreeMap<String, SRIAMPolicy>,
    #[serde(
        rename = "userPolicies",
        alias = "UserPolicies",
        default,
        deserialize_with = "deserialize_null_default",
        skip_serializing_if = "BTreeMap::is_empty"
    )]
    pub user_policies: BTreeMap<String, SRPolicyMapping>,
    #[serde(
        rename = "userInfoMap",
        alias = "UserInfoMap",
        default,
        deserialize_with = "deserialize_null_default",
        skip_serializing_if = "BTreeMap::is_empty"
    )]
    pub user_info_map: BTreeMap<String, UserInfo>,
    #[serde(
        rename = "groupDescMap",
        alias = "GroupDescMap",
        default,
        deserialize_with = "deserialize_null_default",
        skip_serializing_if = "BTreeMap::is_empty"
    )]
    pub group_desc_map: BTreeMap<String, GroupDesc>,
    #[serde(
        rename = "groupPolicies",
        alias = "GroupPolicies",
        default,
        deserialize_with = "deserialize_null_default",
        skip_serializing_if = "BTreeMap::is_empty"
    )]
    pub group_policies: BTreeMap<String, SRPolicyMapping>,
    #[serde(
        rename = "replicationCfg",
        alias = "ReplicationCfg",
        default,
        deserialize_with = "deserialize_null_default",
        skip_serializing_if = "BTreeMap::is_empty"
    )]
    pub replication_cfg: BTreeMap<String, Value>,
    #[serde(
        rename = "ilmExpiryRules",
        alias = "ILMExpiryRules",
        default,
        deserialize_with = "deserialize_null_default",
        skip_serializing_if = "BTreeMap::is_empty"
    )]
    pub ilm_expiry_rules: BTreeMap<String, ILMExpiryRule>,
    #[serde(alias = "State", default, deserialize_with = "deserialize_null_default")]
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
pub struct SRPeerError {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub name: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub endpoint: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub error: String,
    #[serde(rename = "apiVersion", skip_serializing_if = "Option::is_none")]
    pub api_version: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SRPendingOperation {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub operation: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub id: String,
    #[serde(rename = "pendingPeers", default, skip_serializing_if = "Vec::is_empty")]
    pub pending_peers: Vec<String>,
    #[serde(rename = "ackedPeers", default, skip_serializing_if = "Vec::is_empty")]
    pub acked_peers: Vec<String>,
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
pub struct SRRetryStats {
    #[serde(default)]
    pub pending: usize,
    #[serde(default)]
    pub failed: usize,
    #[serde(rename = "lastError", default, skip_serializing_if = "String::is_empty")]
    pub last_error: String,
    #[serde(rename = "apiVersion", skip_serializing_if = "Option::is_none")]
    pub api_version: Option<String>,
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
    #[serde(rename = "PeerErrors", default, skip_serializing_if = "BTreeMap::is_empty")]
    pub peer_errors: BTreeMap<String, SRPeerError>,
    #[serde(rename = "PendingOperation", default, skip_serializing_if = "Option::is_none")]
    pub pending_operation: Option<SRPendingOperation>,
    #[serde(rename = "RetryStats", default, skip_serializing_if = "Option::is_none")]
    pub retry_stats: Option<SRRetryStats>,
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

// Go json.Marshal emits nil maps (and nil struct pointers) as null; treat
// explicit null like a missing field so MinIO SRInfo payloads parse.
fn deserialize_null_default<'de, T, D>(deserializer: D) -> Result<T, D::Error>
where
    T: Deserialize<'de> + Default,
    D: serde::Deserializer<'de>,
{
    Ok(Option::<T>::deserialize(deserializer)?.unwrap_or_default())
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
    #[serde(rename = "targetArn", default, skip_serializing_if = "String::is_empty")]
    pub target_arn: String,
    #[serde(default)]
    pub status: String,
    #[serde(rename = "errorDetail", skip_serializing_if = "String::is_empty", default)]
    pub err_detail: String,
    #[serde(
        rename = "createdAt",
        default,
        with = "time::serde::rfc3339::option",
        skip_serializing_if = "Option::is_none"
    )]
    pub created_at: Option<OffsetDateTime>,
    #[serde(
        rename = "startedAt",
        default,
        with = "time::serde::rfc3339::option",
        skip_serializing_if = "Option::is_none"
    )]
    pub started_at: Option<OffsetDateTime>,
    #[serde(
        rename = "updatedAt",
        default,
        with = "time::serde::rfc3339::option",
        skip_serializing_if = "Option::is_none"
    )]
    pub updated_at: Option<OffsetDateTime>,
    #[serde(
        rename = "completedAt",
        default,
        with = "time::serde::rfc3339::option",
        skip_serializing_if = "Option::is_none"
    )]
    pub completed_at: Option<OffsetDateTime>,
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub generation: u64,
    #[serde(rename = "replicatedObjects", default, skip_serializing_if = "is_zero_u64")]
    pub replicated_objects: u64,
    #[serde(rename = "replicatedBytes", default, skip_serializing_if = "is_zero_u64")]
    pub replicated_bytes: u64,
    #[serde(rename = "failedObjects", default, skip_serializing_if = "is_zero_u64")]
    pub failed_objects: u64,
    #[serde(rename = "failedBytes", default, skip_serializing_if = "is_zero_u64")]
    pub failed_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SRResyncOpStatus {
    #[serde(rename = "op", default)]
    pub op_type: String,
    #[serde(rename = "id", default)]
    pub resync_id: String,
    #[serde(default)]
    pub status: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub state: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub buckets: Vec<ResyncBucketStatus>,
    #[serde(rename = "errorDetail", skip_serializing_if = "String::is_empty", default)]
    pub err_detail: String,
    #[serde(
        rename = "createdAt",
        default,
        with = "time::serde::rfc3339::option",
        skip_serializing_if = "Option::is_none"
    )]
    pub created_at: Option<OffsetDateTime>,
    #[serde(
        rename = "startedAt",
        default,
        with = "time::serde::rfc3339::option",
        skip_serializing_if = "Option::is_none"
    )]
    pub started_at: Option<OffsetDateTime>,
    #[serde(
        rename = "updatedAt",
        default,
        with = "time::serde::rfc3339::option",
        skip_serializing_if = "Option::is_none"
    )]
    pub updated_at: Option<OffsetDateTime>,
    #[serde(
        rename = "completedAt",
        default,
        with = "time::serde::rfc3339::option",
        skip_serializing_if = "Option::is_none"
    )]
    pub completed_at: Option<OffsetDateTime>,
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub generation: u64,
    #[serde(rename = "totalBuckets", default, skip_serializing_if = "is_zero_u64")]
    pub total_buckets: u64,
    #[serde(rename = "pendingBuckets", default, skip_serializing_if = "is_zero_u64")]
    pub pending_buckets: u64,
    #[serde(rename = "runningBuckets", default, skip_serializing_if = "is_zero_u64")]
    pub running_buckets: u64,
    #[serde(rename = "completedBuckets", default, skip_serializing_if = "is_zero_u64")]
    pub completed_buckets: u64,
    #[serde(rename = "failedBuckets", default, skip_serializing_if = "is_zero_u64")]
    pub failed_buckets: u64,
    #[serde(rename = "canceledBuckets", default, skip_serializing_if = "is_zero_u64")]
    pub canceled_buckets: u64,
    #[serde(rename = "replicatedObjects", default, skip_serializing_if = "is_zero_u64")]
    pub replicated_objects: u64,
    #[serde(rename = "replicatedBytes", default, skip_serializing_if = "is_zero_u64")]
    pub replicated_bytes: u64,
    #[serde(rename = "failedObjects", default, skip_serializing_if = "is_zero_u64")]
    pub failed_objects: u64,
    #[serde(rename = "failedBytes", default, skip_serializing_if = "is_zero_u64")]
    pub failed_bytes: u64,
    #[serde(default, skip_serializing_if = "is_false")]
    pub truncated: bool,
    #[serde(rename = "nextContinuationToken", default, skip_serializing_if = "String::is_empty")]
    pub next_continuation_token: String,
}

fn is_zero_u64(value: &u64) -> bool {
    *value == 0
}

fn is_false(value: &bool) -> bool {
    !*value
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

#[cfg(test)]
mod tests {
    use super::{PeerInfo, PeerSite, SRCredInfo, SRInfo, SRPolicyMapping, SRResyncOpStatus};
    use serde_json::{Value, json};

    const TEST_CA_CERT: &str = "-----BEGIN CERTIFICATE-----\ntest-ca\n-----END CERTIFICATE-----";

    #[test]
    fn peer_tls_fields_default_when_missing_from_legacy_json() {
        let site: PeerSite = serde_json::from_value(json!({
            "name": "site-a",
            "endpoints": "https://site-a.example.com"
        }))
        .expect("legacy PeerSite JSON should deserialize");
        let peer: PeerInfo = serde_json::from_value(json!({
            "endpoint": "https://site-a.example.com",
            "name": "site-a",
            "deploymentID": "deployment-a"
        }))
        .expect("legacy PeerInfo JSON should deserialize");

        assert!(!site.skip_tls_verify);
        assert_eq!(site.ca_cert_pem, "");
        assert!(!peer.skip_tls_verify);
        assert_eq!(peer.ca_cert_pem, "");
    }

    #[test]
    fn peer_tls_fields_round_trip_with_exact_json_names() {
        let site_json = json!({
            "name": "site-a",
            "endpoints": "https://site-a.example.com",
            "accessKey": "access-key",
            "secretKey": "secret-key",
            "skipTlsVerify": true,
            "caCertPem": TEST_CA_CERT
        });
        let peer_json = json!({
            "endpoint": "https://site-a.example.com",
            "name": "site-a",
            "deploymentID": "deployment-a",
            "sync": "unknown",
            "defaultbandwidth": {
                "bandwidthLimitPerBucket": 0,
                "set": false
            },
            "replicate-ilm-expiry": false,
            "objectNamingMode": "path",
            "skipTlsVerify": true,
            "caCertPem": TEST_CA_CERT
        });

        let site: PeerSite = serde_json::from_value(site_json.clone()).expect("PeerSite JSON should deserialize");
        let peer: PeerInfo = serde_json::from_value(peer_json.clone()).expect("PeerInfo JSON should deserialize");

        assert_eq!(serde_json::to_value(site).expect("PeerSite should serialize"), site_json);
        assert_eq!(serde_json::to_value(peer).expect("PeerInfo should serialize"), peer_json);
    }

    #[test]
    fn peer_tls_false_and_empty_ca_are_still_serialized() {
        let site = serde_json::to_value(PeerSite::default()).expect("PeerSite should serialize");
        let peer = serde_json::to_value(PeerInfo::default()).expect("PeerInfo should serialize");

        for value in [site, peer] {
            let object = value.as_object().expect("peer JSON should be an object");
            assert_eq!(object.get("skipTlsVerify"), Some(&Value::Bool(false)));
            assert_eq!(object.get("caCertPem"), Some(&Value::String(String::new())));
        }
    }

    #[test]
    fn peer_debug_output_redacts_secrets_and_ca_contents() {
        let site = PeerSite {
            name: "site-a".to_owned(),
            endpoint: "https://site-a.example.com".to_owned(),
            access_key: "sensitive-access-key".to_owned(),
            secret_key: "sensitive-secret-key".to_owned(),
            skip_tls_verify: true,
            ca_cert_pem: TEST_CA_CERT.to_owned(),
        };
        let peer = PeerInfo {
            endpoint: "https://site-a.example.com".to_owned(),
            name: "site-a".to_owned(),
            deployment_id: "deployment-a".to_owned(),
            skip_tls_verify: false,
            ca_cert_pem: TEST_CA_CERT.to_owned(),
            ..PeerInfo::default()
        };

        let site_debug = format!("{site:?}");
        let peer_debug = format!("{peer:?}");

        assert!(!site_debug.contains("BEGIN CERTIFICATE"));
        assert!(!site_debug.contains("sensitive-access-key"));
        assert!(!site_debug.contains("sensitive-secret-key"));
        assert!(site_debug.contains("skip_tls_verify: true"));
        assert!(site_debug.contains("has_custom_ca: true"));
        assert!(!peer_debug.contains("BEGIN CERTIFICATE"));
        assert!(peer_debug.contains("skip_tls_verify: false"));
        assert!(peer_debug.contains("has_custom_ca: true"));
    }

    /// MinIO IAMUserType wire semantics (cmd/iam.go): unknown = -1,
    /// regUser = 0, stsUser = 1, svcUser = 2. MinIO group policy mappings
    /// arrive with `userType: -1`; the wire field must accept negatives.
    #[test]
    fn sr_policy_mapping_accepts_minio_negative_user_type() {
        let mapping: SRPolicyMapping = serde_json::from_value(json!({
            "userOrGroup": "devs",
            "userType": -1,
            "isGroup": true,
            "policy": "readwrite"
        }))
        .expect("MinIO group mapping with userType -1 must deserialize");
        assert_eq!(mapping.user_type, -1);
        assert!(mapping.is_group);
        assert_eq!(mapping.policy, "readwrite");
    }

    /// Same IAMUserType family as SRPolicyMapping: MinIO may send -1 (unknown).
    #[test]
    fn sr_cred_info_accepts_minio_negative_iam_user_type() {
        let cred: SRCredInfo = serde_json::from_value(json!({
            "accessKey": "replicated-user",
            "iamUserType": -1
        }))
        .expect("SRCredInfo with iamUserType -1 must deserialize");
        assert_eq!(cred.iam_user_type, -1);
        assert_eq!(cred.access_key, "replicated-user");
    }

    #[test]
    fn resync_status_legacy_json_defaults_new_lifecycle_fields() {
        let legacy_json = json!({
            "op": "start",
            "id": "resync-1",
            "status": "success",
            "buckets": [{
                "bucket": "photos",
                "status": "success"
            }]
        });

        let status: SRResyncOpStatus =
            serde_json::from_value(legacy_json.clone()).expect("legacy resync status should deserialize");

        assert_eq!(status.generation, 0);
        assert!(status.state.is_empty());
        assert!(status.created_at.is_none());
        assert_eq!(status.total_buckets, 0);
        assert_eq!(status.replicated_objects, 0);
        assert!(!status.truncated);
        assert!(status.next_continuation_token.is_empty());
        assert!(status.buckets[0].created_at.is_none());
        assert!(status.buckets[0].target_arn.is_empty());
        assert_eq!(status.buckets[0].generation, 0);
        assert_eq!(status.buckets[0].replicated_bytes, 0);
        assert_eq!(serde_json::to_value(status).expect("legacy resync status should serialize"), legacy_json);
    }

    #[test]
    fn resync_status_lifecycle_fields_round_trip_with_exact_json_names() {
        let status_json = json!({
            "op": "status",
            "id": "resync-2",
            "status": "success",
            "state": "running",
            "createdAt": "2026-07-22T01:00:00Z",
            "startedAt": "2026-07-22T01:00:01Z",
            "updatedAt": "2026-07-22T01:01:00Z",
            "completedAt": "2026-07-22T01:02:00Z",
            "generation": 7,
            "totalBuckets": 6,
            "pendingBuckets": 1,
            "runningBuckets": 1,
            "completedBuckets": 1,
            "failedBuckets": 1,
            "canceledBuckets": 2,
            "replicatedObjects": 12,
            "replicatedBytes": 4096,
            "failedObjects": 3,
            "failedBytes": 512,
            "truncated": true,
            "nextContinuationToken": "bucket-page-2",
            "buckets": [{
                "bucket": "photos",
                "targetArn": "arn:rustfs:replication::peer-a:photos",
                "status": "failed",
                "errorDetail": "target unavailable",
                "createdAt": "2026-07-22T01:00:00Z",
                "startedAt": "2026-07-22T01:00:01Z",
                "updatedAt": "2026-07-22T01:01:00Z",
                "completedAt": "2026-07-22T01:02:00Z",
                "generation": 7,
                "replicatedObjects": 12,
                "replicatedBytes": 4096,
                "failedObjects": 3,
                "failedBytes": 512
            }]
        });

        let status: SRResyncOpStatus =
            serde_json::from_value(status_json.clone()).expect("expanded resync status should deserialize");

        assert_eq!(status.generation, 7);
        assert_eq!(status.state, "running");
        assert_eq!(status.total_buckets, 6);
        assert_eq!(status.completed_buckets, 1);
        assert_eq!(status.replicated_bytes, 4096);
        assert!(status.truncated);
        assert_eq!(status.next_continuation_token, "bucket-page-2");
        assert_eq!(status.buckets[0].generation, 7);
        assert_eq!(status.buckets[0].target_arn, "arn:rustfs:replication::peer-a:photos");
        assert_eq!(status.buckets[0].failed_objects, 3);
        assert!(status.buckets[0].completed_at.is_some());
        assert_eq!(
            serde_json::to_value(status).expect("expanded resync status should serialize"),
            status_json
        );
    }

    /// Mirrors `json.Marshal(madmin.SRInfo{...})` output from MinIO: the
    /// madmin-go SRInfo top-level fields carry no json tags (except
    /// APIVersion), so Go emits them in PascalCase, while every nested
    /// struct has lowercase tags.
    fn minio_pascal_case_sr_info_json() -> Value {
        json!({
            "Enabled": true,
            "Name": "site-minio",
            "DeploymentID": "minio-deploy-1",
            "Buckets": {
                "photos": {
                    "bucket": "photos",
                    "versioningConfig": "PHZlcnNpb25pbmcvPg=="
                }
            },
            "Policies": {
                "readonly": {
                    "policy": { "Version": "2012-10-17" },
                    "updatedAt": "2026-07-22T01:00:00Z"
                }
            },
            "UserPolicies": {
                "alice": {
                    "userOrGroup": "alice",
                    "userType": 1,
                    "isGroup": false,
                    "policy": "readonly"
                }
            },
            "UserInfoMap": {
                "alice": {
                    "status": "enabled",
                    "updatedAt": "2026-07-22T01:00:00Z"
                }
            },
            "GroupDescMap": {
                "devs": {
                    "name": "devs",
                    "status": "enabled",
                    "members": ["alice"],
                    "policy": "readonly",
                    "updatedAt": "2026-07-22T01:00:00Z"
                }
            },
            "GroupPolicies": {
                "devs": {
                    "userOrGroup": "devs",
                    "userType": 0,
                    "isGroup": true,
                    "policy": "readonly"
                }
            },
            "ReplicationCfg": {
                "photos": { "role": "arn:minio:replication::minio-deploy-1:photos" }
            },
            "ILMExpiryRules": {
                "rule-1": {
                    "ilm-rule": "PFJ1bGUvPg==",
                    "bucket": "photos"
                }
            },
            "State": {
                "name": "site-minio",
                "peers": {
                    "minio-deploy-1": {
                        "endpoint": "https://minio.example.com",
                        "name": "site-minio",
                        "deploymentID": "minio-deploy-1"
                    }
                },
                "updatedAt": "2026-07-22T01:00:00Z"
            },
            "apiVersion": "1"
        })
    }

    #[test]
    fn sr_info_deserializes_minio_pascal_case_top_level_fields() {
        let info: SRInfo =
            serde_json::from_value(minio_pascal_case_sr_info_json()).expect("MinIO PascalCase SRInfo JSON should deserialize");

        assert!(info.enabled, "Enabled must map to enabled");
        assert_eq!(info.name, "site-minio");
        assert_eq!(info.deployment_id, "minio-deploy-1", "DeploymentID must map to deployment_id");
        assert!(info.buckets.contains_key("photos"), "Buckets must map to buckets");
        assert!(info.policies.contains_key("readonly"), "Policies must map to policies");
        assert!(info.user_policies.contains_key("alice"), "UserPolicies must map to user_policies");
        assert!(info.user_info_map.contains_key("alice"), "UserInfoMap must map to user_info_map");
        assert!(info.group_desc_map.contains_key("devs"), "GroupDescMap must map to group_desc_map");
        assert!(info.group_policies.contains_key("devs"), "GroupPolicies must map to group_policies");
        assert!(info.replication_cfg.contains_key("photos"), "ReplicationCfg must map to replication_cfg");
        assert!(
            info.ilm_expiry_rules.contains_key("rule-1"),
            "ILMExpiryRules must map to ilm_expiry_rules"
        );
        assert!(
            info.state.peers.contains_key("minio-deploy-1"),
            "State must map to state with populated peers"
        );
    }

    #[test]
    fn sr_info_deserializes_minio_nil_maps_as_empty() {
        // Go json.Marshal emits nil maps as null; an SR-unconfigured MinIO
        // site reports SRInfo with every map nil.
        let nil_map_json = json!({
            "Enabled": false,
            "Name": "site-minio",
            "DeploymentID": "minio-deploy-1",
            "Buckets": null,
            "Policies": null,
            "UserPolicies": null,
            "UserInfoMap": null,
            "GroupDescMap": null,
            "GroupPolicies": null,
            "ReplicationCfg": null,
            "ILMExpiryRules": null,
            "State": null,
            "apiVersion": "1"
        });

        let info: SRInfo = serde_json::from_value(nil_map_json).expect("MinIO nil-map SRInfo JSON should deserialize");

        assert!(!info.enabled);
        assert_eq!(info.deployment_id, "minio-deploy-1");
        assert!(info.buckets.is_empty());
        assert!(info.policies.is_empty());
        assert!(info.user_policies.is_empty());
        assert!(info.user_info_map.is_empty());
        assert!(info.group_desc_map.is_empty());
        assert!(info.group_policies.is_empty());
        assert!(info.replication_cfg.is_empty());
        assert!(info.ilm_expiry_rules.is_empty());
        assert!(info.state.peers.is_empty());

        // Go's zero-value SRStateInfo serializes as an object whose nil
        // peers map is null, not as a null State.
        let nil_peers_json = json!({
            "Enabled": false,
            "DeploymentID": "minio-deploy-1",
            "State": { "name": "", "peers": null }
        });

        let info: SRInfo = serde_json::from_value(nil_peers_json).expect("MinIO nil-peers SRInfo JSON should deserialize");

        assert_eq!(info.deployment_id, "minio-deploy-1");
        assert!(info.state.peers.is_empty());
    }

    #[test]
    fn sr_info_serialization_stays_camel_case() {
        let info: SRInfo =
            serde_json::from_value(minio_pascal_case_sr_info_json()).expect("MinIO PascalCase SRInfo JSON should deserialize");

        let value = serde_json::to_value(info).expect("SRInfo should serialize");
        let object = value.as_object().expect("SRInfo JSON should be an object");

        for camel_key in [
            "enabled",
            "name",
            "deploymentID",
            "buckets",
            "policies",
            "userPolicies",
            "userInfoMap",
            "groupDescMap",
            "groupPolicies",
            "replicationCfg",
            "ilmExpiryRules",
            "state",
        ] {
            assert!(object.contains_key(camel_key), "serialized SRInfo must keep camelCase key {camel_key}");
        }
        for pascal_key in ["Enabled", "Name", "DeploymentID", "Buckets", "State", "ILMExpiryRules"] {
            assert!(
                !object.contains_key(pascal_key),
                "serialized SRInfo must not emit PascalCase key {pascal_key}"
            );
        }
    }
}
