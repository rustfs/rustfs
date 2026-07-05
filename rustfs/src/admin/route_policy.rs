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

use rustfs_security_governance::{
    AdminActionRef, AdminRouteMatrixError, AdminRouteSpec, HttpMethod, PublicRouteKind, RouteRiskLevel,
    validate_admin_route_specs,
};

const ALL_ADMIN: AdminActionRef = AdminActionRef::new("AllAdminActions");
const ADD_USER_TO_GROUP: AdminActionRef = AdminActionRef::new("AddUserToGroupAdminAction");
const ATTACH_POLICY: AdminActionRef = AdminActionRef::new("AttachPolicyAdminAction");
const CONFIG_UPDATE: AdminActionRef = AdminActionRef::new("ConfigUpdateAdminAction");
const COMMIT_TABLE: AdminActionRef = AdminActionRef::new("CommitTableAction");
const CREATE_POLICY: AdminActionRef = AdminActionRef::new("CreatePolicyAdminAction");
const CREATE_SERVICE_ACCOUNT: AdminActionRef = AdminActionRef::new("CreateServiceAccountAdminAction");
const CREATE_TABLE: AdminActionRef = AdminActionRef::new("CreateTableAction");
const DECOMMISSION: AdminActionRef = AdminActionRef::new("DecommissionAdminAction");
const DELETE_POLICY: AdminActionRef = AdminActionRef::new("DeletePolicyAdminAction");
const DELETE_TABLE: AdminActionRef = AdminActionRef::new("DeleteTableAction");
const DELETE_TABLE_NAMESPACE: AdminActionRef = AdminActionRef::new("DeleteTableNamespaceAction");
const DELETE_USER: AdminActionRef = AdminActionRef::new("DeleteUserAdminAction");
const ENABLE_GROUP: AdminActionRef = AdminActionRef::new("EnableGroupAdminAction");
const ENABLE_USER: AdminActionRef = AdminActionRef::new("EnableUserAdminAction");
const EXPORT_BUCKET_METADATA: AdminActionRef = AdminActionRef::new("ExportBucketMetadataAction");
const EXPORT_IAM: AdminActionRef = AdminActionRef::new("ExportIAMAction");
const GET_BUCKET_TARGET: AdminActionRef = AdminActionRef::new("GetBucketTargetAction");
const GET_GROUP: AdminActionRef = AdminActionRef::new("GetGroupAdminAction");
const GET_METRICS: AdminActionRef = AdminActionRef::new("GetMetricsAction");
const GET_POLICY: AdminActionRef = AdminActionRef::new("GetPolicyAdminAction");
const GET_REPLICATION_METRICS: AdminActionRef = AdminActionRef::new("GetReplicationMetricsAction");
const GET_TABLE: AdminActionRef = AdminActionRef::new("GetTableAction");
const GET_TABLE_BUCKET: AdminActionRef = AdminActionRef::new("GetTableBucketAction");
const GET_TABLE_CATALOG: AdminActionRef = AdminActionRef::new("GetTableCatalogAction");
const GET_TABLE_CREDENTIALS: AdminActionRef = AdminActionRef::new("GetTableCredentialsAction");
const GET_TABLE_LIFECYCLE: AdminActionRef = AdminActionRef::new("GetTableLifecycleAction");
const GET_TABLE_METADATA: AdminActionRef = AdminActionRef::new("GetTableMetadataAction");
const GET_TABLE_METADATA_LOCATION: AdminActionRef = AdminActionRef::new("GetTableMetadataLocationAction");
const GET_TABLE_NAMESPACE: AdminActionRef = AdminActionRef::new("GetTableNamespaceAction");
const HEAL: AdminActionRef = AdminActionRef::new("HealAdminAction");
const IMPORT_BUCKET_METADATA: AdminActionRef = AdminActionRef::new("ImportBucketMetadataAction");
const IMPORT_IAM: AdminActionRef = AdminActionRef::new("ImportIAMAction");
const KMS_CLEAR_CACHE: AdminActionRef = AdminActionRef::new("kms:ClearCache");
const KMS_CONFIGURE: AdminActionRef = AdminActionRef::new("kms:Configure");
const KMS_DELETE_KEY: AdminActionRef = AdminActionRef::new("kms:DeleteKey");
const KMS_DESCRIBE_KEY: AdminActionRef = AdminActionRef::new("kms:DescribeKey");
const KMS_GENERATE_DATA_KEY: AdminActionRef = AdminActionRef::new("kms:GenerateDataKey");
const KMS_LIST_KEYS: AdminActionRef = AdminActionRef::new("kms:ListKeys");
const KMS_SERVICE_CONTROL: AdminActionRef = AdminActionRef::new("kms:ServiceControl");
const LIST_GROUPS: AdminActionRef = AdminActionRef::new("ListGroupsAdminAction");
const LIST_TEMPORARY_ACCOUNTS: AdminActionRef = AdminActionRef::new("ListTemporaryAccountsAdminAction");
const LIST_TIER: AdminActionRef = AdminActionRef::new("ListTierAction");
const LIST_USER_POLICIES: AdminActionRef = AdminActionRef::new("ListUserPoliciesAdminAction");
const LIST_USERS: AdminActionRef = AdminActionRef::new("ListUsersAdminAction");
const PROFILING: AdminActionRef = AdminActionRef::new("ProfilingAdminAction");
const REBALANCE: AdminActionRef = AdminActionRef::new("RebalanceAdminAction");
const REGISTER_TABLE: AdminActionRef = AdminActionRef::new("RegisterTableAction");
const REMOVE_USER_FROM_GROUP: AdminActionRef = AdminActionRef::new("RemoveUserFromGroupAdminAction");
const RUN_TABLE_MAINTENANCE: AdminActionRef = AdminActionRef::new("RunTableMaintenanceAction");
const SERVER_INFO: AdminActionRef = AdminActionRef::new("ServerInfoAdminAction");
const SET_BUCKET_QUOTA: AdminActionRef = AdminActionRef::new("SetBucketQuotaAdminAction");
const SET_BUCKET_TARGET: AdminActionRef = AdminActionRef::new("SetBucketTargetAction");
const SET_TABLE: AdminActionRef = AdminActionRef::new("SetTableAction");
const SET_TABLE_BUCKET: AdminActionRef = AdminActionRef::new("SetTableBucketAction");
const SET_TABLE_LIFECYCLE: AdminActionRef = AdminActionRef::new("SetTableLifecycleAction");
const SET_TABLE_METADATA_LOCATION: AdminActionRef = AdminActionRef::new("SetTableMetadataLocationAction");
const SET_TABLE_NAMESPACE: AdminActionRef = AdminActionRef::new("SetTableNamespaceAction");
const SET_TIER: AdminActionRef = AdminActionRef::new("SetTierAction");
const SITE_REPLICATION_ADD: AdminActionRef = AdminActionRef::new("SiteReplicationAddAction");
const SITE_REPLICATION_INFO: AdminActionRef = AdminActionRef::new("SiteReplicationInfoAction");
const SITE_REPLICATION_OPERATION: AdminActionRef = AdminActionRef::new("SiteReplicationOperationAction");
const SITE_REPLICATION_REMOVE: AdminActionRef = AdminActionRef::new("SiteReplicationRemoveAction");
const SITE_REPLICATION_RESYNC: AdminActionRef = AdminActionRef::new("SiteReplicationResyncAction");
const STORAGE_INFO: AdminActionRef = AdminActionRef::new("StorageInfoAdminAction");

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DeferredRoutePolicyReason {
    ContextualAuthorization,
    CredentialOnly,
    MultipleActions,
    NotImplemented,
    S3Action,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DeferredAdminRoutePolicy {
    method: HttpMethod,
    path: &'static str,
    reason: DeferredRoutePolicyReason,
}

impl DeferredAdminRoutePolicy {
    pub const fn new(method: HttpMethod, path: &'static str, reason: DeferredRoutePolicyReason) -> Self {
        Self { method, path, reason }
    }

    pub const fn method(self) -> HttpMethod {
        self.method
    }

    pub const fn path(self) -> &'static str {
        self.path
    }

    pub const fn reason(self) -> DeferredRoutePolicyReason {
        self.reason
    }
}

pub const ADMIN_ROUTE_POLICY_SPECS: &[AdminRouteSpec] = &[
    public(HttpMethod::Get, "/health", PublicRouteKind::Health, RouteRiskLevel::Normal),
    public(HttpMethod::Head, "/health", PublicRouteKind::Health, RouteRiskLevel::Normal),
    public(HttpMethod::Get, "/health/ready", PublicRouteKind::Health, RouteRiskLevel::Normal),
    public(HttpMethod::Head, "/health/ready", PublicRouteKind::Health, RouteRiskLevel::Normal),
    public(HttpMethod::Post, "/", PublicRouteKind::StsFormPost, RouteRiskLevel::Sensitive),
    admin(HttpMethod::Get, "/profile/cpu", PROFILING, RouteRiskLevel::High),
    admin(HttpMethod::Get, "/profile/memory", PROFILING, RouteRiskLevel::High),
    admin(HttpMethod::Get, "/rustfs/admin/v3/is-admin", ALL_ADMIN, RouteRiskLevel::Sensitive),
    admin(HttpMethod::Get, "/rustfs/admin/v3/list-users", LIST_USERS, RouteRiskLevel::Sensitive),
    admin(HttpMethod::Delete, "/rustfs/admin/v3/remove-user", DELETE_USER, RouteRiskLevel::High),
    admin(HttpMethod::Put, "/rustfs/admin/v3/set-user-status", ENABLE_USER, RouteRiskLevel::High),
    admin(HttpMethod::Get, "/rustfs/admin/v3/groups", LIST_GROUPS, RouteRiskLevel::Sensitive),
    admin(HttpMethod::Get, "/rustfs/admin/v3/group", GET_GROUP, RouteRiskLevel::Sensitive),
    admin(
        HttpMethod::Delete,
        "/rustfs/admin/v3/group/{group}",
        REMOVE_USER_FROM_GROUP,
        RouteRiskLevel::High,
    ),
    admin(HttpMethod::Put, "/rustfs/admin/v3/set-group-status", ENABLE_GROUP, RouteRiskLevel::High),
    admin(
        HttpMethod::Put,
        "/rustfs/admin/v3/update-group-members",
        ADD_USER_TO_GROUP,
        RouteRiskLevel::High,
    ),
    admin(
        HttpMethod::Get,
        "/rustfs/admin/v3/temporary-account-info",
        LIST_TEMPORARY_ACCOUNTS,
        RouteRiskLevel::Sensitive,
    ),
    admin(
        HttpMethod::Put,
        "/rustfs/admin/v3/add-service-accounts",
        CREATE_SERVICE_ACCOUNT,
        RouteRiskLevel::High,
    ),
    admin(
        HttpMethod::Put,
        "/rustfs/admin/v3/add-service-account",
        CREATE_SERVICE_ACCOUNT,
        RouteRiskLevel::High,
    ),
    admin(HttpMethod::Get, "/rustfs/admin/v3/export-iam", EXPORT_IAM, RouteRiskLevel::High),
    admin(HttpMethod::Put, "/rustfs/admin/v3/import-iam", IMPORT_IAM, RouteRiskLevel::High),
    admin(
        HttpMethod::Get,
        "/rustfs/admin/v3/list-canned-policies",
        LIST_USER_POLICIES,
        RouteRiskLevel::Sensitive,
    ),
    admin(
        HttpMethod::Get,
        "/rustfs/admin/v3/info-canned-policy",
        GET_POLICY,
        RouteRiskLevel::Sensitive,
    ),
    admin(HttpMethod::Put, "/rustfs/admin/v3/add-canned-policy", CREATE_POLICY, RouteRiskLevel::High),
    admin(
        HttpMethod::Delete,
        "/rustfs/admin/v3/remove-canned-policy",
        DELETE_POLICY,
        RouteRiskLevel::High,
    ),
    admin(
        HttpMethod::Put,
        "/rustfs/admin/v3/set-user-or-group-policy",
        ATTACH_POLICY,
        RouteRiskLevel::High,
    ),
    admin(HttpMethod::Put, "/rustfs/admin/v3/set-policy", ATTACH_POLICY, RouteRiskLevel::High),
    admin(
        HttpMethod::Post,
        "/rustfs/admin/v3/idp/builtin/policy/attach",
        ATTACH_POLICY,
        RouteRiskLevel::High,
    ),
    admin(
        HttpMethod::Post,
        "/rustfs/admin/v3/idp/builtin/policy/detach",
        ATTACH_POLICY,
        RouteRiskLevel::High,
    ),
    admin(
        HttpMethod::Get,
        "/rustfs/admin/v3/target/list",
        GET_BUCKET_TARGET,
        RouteRiskLevel::Sensitive,
    ),
    admin(
        HttpMethod::Put,
        "/rustfs/admin/v3/target/{target_type}/{target_name}",
        SET_BUCKET_TARGET,
        RouteRiskLevel::High,
    ),
    admin(
        HttpMethod::Delete,
        "/rustfs/admin/v3/target/{target_type}/{target_name}/reset",
        SET_BUCKET_TARGET,
        RouteRiskLevel::High,
    ),
    admin(
        HttpMethod::Get,
        "/rustfs/admin/v3/target/arns",
        GET_BUCKET_TARGET,
        RouteRiskLevel::Sensitive,
    ),
    admin(HttpMethod::Get, "/rustfs/admin/v3/info", SERVER_INFO, RouteRiskLevel::Sensitive),
    admin(HttpMethod::Get, "/rustfs/admin/v3/storageinfo", STORAGE_INFO, RouteRiskLevel::Sensitive),
    admin(HttpMethod::Get, "/rustfs/admin/v3/metrics", GET_METRICS, RouteRiskLevel::Sensitive),
    admin(
        HttpMethod::Post,
        "/rustfs/admin/v3/pools/decommission",
        DECOMMISSION,
        RouteRiskLevel::High,
    ),
    admin(
        HttpMethod::Get,
        "/rustfs/admin/v3/decommission/status",
        DECOMMISSION,
        RouteRiskLevel::Sensitive,
    ),
    admin(HttpMethod::Post, "/rustfs/admin/v3/pools/cancel", DECOMMISSION, RouteRiskLevel::High),
    admin(HttpMethod::Post, "/rustfs/admin/v3/pools/clear", DECOMMISSION, RouteRiskLevel::High),
    admin(HttpMethod::Post, "/rustfs/admin/v3/rebalance/start", REBALANCE, RouteRiskLevel::High),
    admin(HttpMethod::Get, "/rustfs/admin/v3/rebalance/status", REBALANCE, RouteRiskLevel::Sensitive),
    admin(HttpMethod::Post, "/rustfs/admin/v3/rebalance/stop", REBALANCE, RouteRiskLevel::High),
    admin(HttpMethod::Post, "/rustfs/admin/v3/heal/", HEAL, RouteRiskLevel::High),
    admin(HttpMethod::Post, "/rustfs/admin/v3/heal/{bucket}", HEAL, RouteRiskLevel::High),
    admin(HttpMethod::Post, "/rustfs/admin/v3/heal/{bucket}/{prefix}", HEAL, RouteRiskLevel::High),
    admin(HttpMethod::Post, "/rustfs/admin/v3/background-heal/status", HEAL, RouteRiskLevel::High),
    admin(HttpMethod::Get, "/rustfs/admin/v3/tier", LIST_TIER, RouteRiskLevel::Sensitive),
    admin(HttpMethod::Get, "/rustfs/admin/v3/tier-stats", LIST_TIER, RouteRiskLevel::Sensitive),
    admin(HttpMethod::Get, "/rustfs/admin/v3/tier/{tier}", LIST_TIER, RouteRiskLevel::Sensitive),
    admin(HttpMethod::Delete, "/rustfs/admin/v3/tier/{tiername}", SET_TIER, RouteRiskLevel::High),
    admin(HttpMethod::Put, "/rustfs/admin/v3/tier", SET_TIER, RouteRiskLevel::High),
    admin(HttpMethod::Post, "/rustfs/admin/v3/tier/{tiername}", SET_TIER, RouteRiskLevel::High),
    admin(HttpMethod::Post, "/rustfs/admin/v3/tier/clear", SET_TIER, RouteRiskLevel::High),
    admin(
        HttpMethod::Put,
        "/rustfs/admin/v3/set-bucket-quota",
        SET_BUCKET_QUOTA,
        RouteRiskLevel::High,
    ),
    admin(HttpMethod::Put, "/rustfs/admin/v3/quota/{bucket}", SET_BUCKET_QUOTA, RouteRiskLevel::High),
    admin(
        HttpMethod::Delete,
        "/rustfs/admin/v3/quota/{bucket}",
        SET_BUCKET_QUOTA,
        RouteRiskLevel::High,
    ),
    admin(
        HttpMethod::Get,
        "/rustfs/admin/export-bucket-metadata",
        EXPORT_BUCKET_METADATA,
        RouteRiskLevel::High,
    ),
    admin(
        HttpMethod::Get,
        "/rustfs/admin/v3/export-bucket-metadata",
        EXPORT_BUCKET_METADATA,
        RouteRiskLevel::High,
    ),
    admin(
        HttpMethod::Put,
        "/rustfs/admin/import-bucket-metadata",
        IMPORT_BUCKET_METADATA,
        RouteRiskLevel::High,
    ),
    admin(
        HttpMethod::Put,
        "/rustfs/admin/v3/import-bucket-metadata",
        IMPORT_BUCKET_METADATA,
        RouteRiskLevel::High,
    ),
    admin(HttpMethod::Get, "/rustfs/admin/v3/get-config-kv", CONFIG_UPDATE, RouteRiskLevel::High),
    admin(HttpMethod::Put, "/rustfs/admin/v3/set-config-kv", CONFIG_UPDATE, RouteRiskLevel::High),
    admin(HttpMethod::Delete, "/rustfs/admin/v3/del-config-kv", CONFIG_UPDATE, RouteRiskLevel::High),
    admin(HttpMethod::Get, "/rustfs/admin/v3/help-config-kv", CONFIG_UPDATE, RouteRiskLevel::High),
    admin(
        HttpMethod::Get,
        "/rustfs/admin/v3/list-config-history-kv",
        CONFIG_UPDATE,
        RouteRiskLevel::High,
    ),
    admin(
        HttpMethod::Delete,
        "/rustfs/admin/v3/clear-config-history-kv",
        CONFIG_UPDATE,
        RouteRiskLevel::High,
    ),
    admin(
        HttpMethod::Put,
        "/rustfs/admin/v3/restore-config-history-kv",
        CONFIG_UPDATE,
        RouteRiskLevel::High,
    ),
    admin(HttpMethod::Get, "/rustfs/admin/v3/config", CONFIG_UPDATE, RouteRiskLevel::High),
    admin(HttpMethod::Put, "/rustfs/admin/v3/config", CONFIG_UPDATE, RouteRiskLevel::High),
    admin(HttpMethod::Get, "/rustfs/admin/v3/scanner/status", SERVER_INFO, RouteRiskLevel::Sensitive),
    admin(
        HttpMethod::Get,
        "/rustfs/admin/v3/audit/target/list",
        GET_BUCKET_TARGET,
        RouteRiskLevel::Sensitive,
    ),
    admin(
        HttpMethod::Put,
        "/rustfs/admin/v3/audit/target/{target_type}/{target_name}",
        SET_BUCKET_TARGET,
        RouteRiskLevel::High,
    ),
    admin(
        HttpMethod::Delete,
        "/rustfs/admin/v3/audit/target/{target_type}/{target_name}/reset",
        SET_BUCKET_TARGET,
        RouteRiskLevel::High,
    ),
    admin(
        HttpMethod::Get,
        "/rustfs/admin/v3/module-switches",
        SERVER_INFO,
        RouteRiskLevel::Sensitive,
    ),
    admin(HttpMethod::Put, "/rustfs/admin/v3/module-switches", CONFIG_UPDATE, RouteRiskLevel::High),
    admin(
        HttpMethod::Get,
        "/rustfs/admin/v4/cluster/snapshot",
        SERVER_INFO,
        RouteRiskLevel::Sensitive,
    ),
    admin(
        HttpMethod::Get,
        "/rustfs/admin/v4/extensions/catalog",
        SERVER_INFO,
        RouteRiskLevel::Sensitive,
    ),
    admin(
        HttpMethod::Get,
        "/rustfs/admin/v4/extensions/instances",
        GET_BUCKET_TARGET,
        RouteRiskLevel::Sensitive,
    ),
    admin(
        HttpMethod::Get,
        "/rustfs/admin/v4/runtime/capabilities",
        SERVER_INFO,
        RouteRiskLevel::Sensitive,
    ),
    admin(
        HttpMethod::Get,
        "/rustfs/admin/v4/plugins/catalog",
        SERVER_INFO,
        RouteRiskLevel::Sensitive,
    ),
    admin(
        HttpMethod::Get,
        "/rustfs/admin/v4/plugins/instances",
        GET_BUCKET_TARGET,
        RouteRiskLevel::Sensitive,
    ),
    admin(
        HttpMethod::Get,
        "/rustfs/admin/v4/plugins/instances/{id}",
        GET_BUCKET_TARGET,
        RouteRiskLevel::Sensitive,
    ),
    admin(
        HttpMethod::Put,
        "/rustfs/admin/v4/plugins/instances/{id}",
        SET_BUCKET_TARGET,
        RouteRiskLevel::High,
    ),
    admin(
        HttpMethod::Delete,
        "/rustfs/admin/v4/plugins/instances/{id}",
        SET_BUCKET_TARGET,
        RouteRiskLevel::High,
    ),
    admin(
        HttpMethod::Get,
        "/rustfs/admin/v3/replicationmetrics",
        GET_REPLICATION_METRICS,
        RouteRiskLevel::Sensitive,
    ),
    admin(
        HttpMethod::Put,
        "/rustfs/admin/v3/set-remote-target",
        SET_BUCKET_TARGET,
        RouteRiskLevel::High,
    ),
    admin(
        HttpMethod::Delete,
        "/rustfs/admin/v3/remove-remote-target",
        SET_BUCKET_TARGET,
        RouteRiskLevel::High,
    ),
    admin(
        HttpMethod::Put,
        "/rustfs/admin/v3/site-replication/add",
        SITE_REPLICATION_ADD,
        RouteRiskLevel::High,
    ),
    admin(
        HttpMethod::Put,
        "/rustfs/admin/v3/site-replication/remove",
        SITE_REPLICATION_REMOVE,
        RouteRiskLevel::High,
    ),
    admin(
        HttpMethod::Get,
        "/rustfs/admin/v3/site-replication/info",
        SITE_REPLICATION_INFO,
        RouteRiskLevel::Sensitive,
    ),
    admin(
        HttpMethod::Get,
        "/rustfs/admin/v3/site-replication/metainfo",
        SITE_REPLICATION_INFO,
        RouteRiskLevel::Sensitive,
    ),
    admin(
        HttpMethod::Get,
        "/rustfs/admin/v3/site-replication/status",
        SITE_REPLICATION_INFO,
        RouteRiskLevel::Sensitive,
    ),
    admin(
        HttpMethod::Post,
        "/rustfs/admin/v3/site-replication/devnull",
        SITE_REPLICATION_OPERATION,
        RouteRiskLevel::High,
    ),
    admin(
        HttpMethod::Post,
        "/rustfs/admin/v3/site-replication/netperf",
        SITE_REPLICATION_OPERATION,
        RouteRiskLevel::High,
    ),
    admin(
        HttpMethod::Post,
        "/rustfs/admin/v3/site-replication/rotate-svc-acct",
        SITE_REPLICATION_OPERATION,
        RouteRiskLevel::Sensitive,
    ),
    admin(
        HttpMethod::Put,
        "/rustfs/admin/v3/site-replication/join",
        SITE_REPLICATION_ADD,
        RouteRiskLevel::High,
    ),
    admin(
        HttpMethod::Put,
        "/rustfs/admin/v3/site-replication/peer/join",
        SITE_REPLICATION_ADD,
        RouteRiskLevel::High,
    ),
    admin(
        HttpMethod::Put,
        "/rustfs/admin/v3/site-replication/peer/bucket-ops",
        SITE_REPLICATION_OPERATION,
        RouteRiskLevel::High,
    ),
    admin(
        HttpMethod::Put,
        "/rustfs/admin/v3/site-replication/peer/iam-item",
        SITE_REPLICATION_OPERATION,
        RouteRiskLevel::High,
    ),
    admin(
        HttpMethod::Put,
        "/rustfs/admin/v3/site-replication/peer/bucket-meta",
        SITE_REPLICATION_OPERATION,
        RouteRiskLevel::High,
    ),
    admin(
        HttpMethod::Get,
        "/rustfs/admin/v3/site-replication/peer/idp-settings",
        SITE_REPLICATION_ADD,
        RouteRiskLevel::High,
    ),
    admin(
        HttpMethod::Put,
        "/rustfs/admin/v3/site-replication/edit",
        SITE_REPLICATION_ADD,
        RouteRiskLevel::High,
    ),
    admin(
        HttpMethod::Put,
        "/rustfs/admin/v3/site-replication/peer/edit",
        SITE_REPLICATION_ADD,
        RouteRiskLevel::High,
    ),
    admin(
        HttpMethod::Put,
        "/rustfs/admin/v3/site-replication/peer/remove",
        SITE_REPLICATION_REMOVE,
        RouteRiskLevel::High,
    ),
    admin(
        HttpMethod::Put,
        "/rustfs/admin/v3/site-replication/resync/op",
        SITE_REPLICATION_RESYNC,
        RouteRiskLevel::High,
    ),
    admin(
        HttpMethod::Put,
        "/rustfs/admin/v3/site-replication/state/edit",
        SITE_REPLICATION_OPERATION,
        RouteRiskLevel::High,
    ),
    admin(
        HttpMethod::Put,
        "/rustfs/admin/v3/site-replication/repair",
        SITE_REPLICATION_OPERATION,
        RouteRiskLevel::High,
    ),
    admin(HttpMethod::Get, "/rustfs/admin/debug/pprof/profile", PROFILING, RouteRiskLevel::High),
    admin(HttpMethod::Get, "/rustfs/admin/debug/pprof/status", PROFILING, RouteRiskLevel::High),
    admin(HttpMethod::Get, "/rustfs/admin/debug/tls/status", PROFILING, RouteRiskLevel::High),
    admin(HttpMethod::Post, "/rustfs/admin/v3/kms/create-key", KMS_CONFIGURE, RouteRiskLevel::High),
    admin(HttpMethod::Post, "/rustfs/admin/v3/kms/key/create", KMS_CONFIGURE, RouteRiskLevel::High),
    admin(
        HttpMethod::Get,
        "/rustfs/admin/v3/kms/describe-key",
        KMS_DESCRIBE_KEY,
        RouteRiskLevel::Sensitive,
    ),
    admin(
        HttpMethod::Get,
        "/rustfs/admin/v3/kms/key/status",
        KMS_DESCRIBE_KEY,
        RouteRiskLevel::Sensitive,
    ),
    admin(
        HttpMethod::Get,
        "/rustfs/admin/v3/kms/list-keys",
        KMS_LIST_KEYS,
        RouteRiskLevel::Sensitive,
    ),
    admin(
        HttpMethod::Post,
        "/rustfs/admin/v3/kms/generate-data-key",
        KMS_GENERATE_DATA_KEY,
        RouteRiskLevel::High,
    ),
    admin(
        HttpMethod::Get,
        "/rustfs/admin/v3/kms/status",
        KMS_SERVICE_CONTROL,
        RouteRiskLevel::Sensitive,
    ),
    admin(HttpMethod::Post, "/rustfs/admin/v3/kms/status", KMS_SERVICE_CONTROL, RouteRiskLevel::High),
    admin(HttpMethod::Get, "/rustfs/admin/v3/kms/config", KMS_CONFIGURE, RouteRiskLevel::Sensitive),
    admin(
        HttpMethod::Post,
        "/rustfs/admin/v3/kms/clear-cache",
        KMS_CLEAR_CACHE,
        RouteRiskLevel::High,
    ),
    admin(HttpMethod::Post, "/rustfs/admin/v3/kms/configure", KMS_CONFIGURE, RouteRiskLevel::High),
    admin(HttpMethod::Post, "/rustfs/admin/v3/kms/start", KMS_SERVICE_CONTROL, RouteRiskLevel::High),
    admin(HttpMethod::Post, "/rustfs/admin/v3/kms/stop", KMS_SERVICE_CONTROL, RouteRiskLevel::High),
    admin(
        HttpMethod::Get,
        "/rustfs/admin/v3/kms/service-status",
        KMS_SERVICE_CONTROL,
        RouteRiskLevel::Sensitive,
    ),
    admin(HttpMethod::Post, "/rustfs/admin/v3/kms/reconfigure", KMS_CONFIGURE, RouteRiskLevel::High),
    admin(HttpMethod::Post, "/rustfs/admin/v3/kms/keys", KMS_CONFIGURE, RouteRiskLevel::High),
    admin(
        HttpMethod::Delete,
        "/rustfs/admin/v3/kms/keys/delete",
        KMS_DELETE_KEY,
        RouteRiskLevel::High,
    ),
    admin(
        HttpMethod::Post,
        "/rustfs/admin/v3/kms/keys/cancel-deletion",
        KMS_DELETE_KEY,
        RouteRiskLevel::High,
    ),
    admin(HttpMethod::Get, "/rustfs/admin/v3/kms/keys", KMS_LIST_KEYS, RouteRiskLevel::Sensitive),
    admin(
        HttpMethod::Get,
        "/rustfs/admin/v3/kms/keys/{key_id}",
        KMS_DESCRIBE_KEY,
        RouteRiskLevel::Sensitive,
    ),
    public(
        HttpMethod::Get,
        "/rustfs/admin/v3/oidc/providers",
        PublicRouteKind::OidcBootstrap,
        RouteRiskLevel::Sensitive,
    ),
    public(
        HttpMethod::Get,
        "/rustfs/admin/v3/oidc/authorize/{provider_id}",
        PublicRouteKind::OidcBootstrap,
        RouteRiskLevel::Sensitive,
    ),
    public(
        HttpMethod::Get,
        "/rustfs/admin/v3/oidc/callback/{provider_id}",
        PublicRouteKind::OidcBootstrap,
        RouteRiskLevel::Sensitive,
    ),
    public(
        HttpMethod::Get,
        "/rustfs/admin/v3/oidc/logout",
        PublicRouteKind::OidcBootstrap,
        RouteRiskLevel::Sensitive,
    ),
    admin(HttpMethod::Get, "/rustfs/admin/v3/oidc/config", SERVER_INFO, RouteRiskLevel::Sensitive),
    admin(
        HttpMethod::Put,
        "/rustfs/admin/v3/oidc/config/{provider_id}",
        CONFIG_UPDATE,
        RouteRiskLevel::High,
    ),
    admin(
        HttpMethod::Delete,
        "/rustfs/admin/v3/oidc/config/{provider_id}",
        CONFIG_UPDATE,
        RouteRiskLevel::High,
    ),
    admin(HttpMethod::Post, "/rustfs/admin/v3/oidc/validate", SERVER_INFO, RouteRiskLevel::High),
    admin(HttpMethod::Get, "/iceberg/v1/config", GET_TABLE_CATALOG, RouteRiskLevel::Sensitive),
    admin(HttpMethod::Put, "/iceberg/v1/buckets/{warehouse}", SET_TABLE_BUCKET, RouteRiskLevel::High),
    admin(
        HttpMethod::Get,
        "/iceberg/v1/buckets/{warehouse}",
        GET_TABLE_BUCKET,
        RouteRiskLevel::Sensitive,
    ),
    admin(
        HttpMethod::Get,
        "/iceberg/v1/{warehouse}/catalog/migration",
        GET_TABLE_CATALOG,
        RouteRiskLevel::Sensitive,
    ),
    admin(
        HttpMethod::Get,
        "/iceberg/v1/{warehouse}/namespaces",
        GET_TABLE_NAMESPACE,
        RouteRiskLevel::Sensitive,
    ),
    admin(
        HttpMethod::Post,
        "/iceberg/v1/{warehouse}/namespaces",
        SET_TABLE_NAMESPACE,
        RouteRiskLevel::High,
    ),
    admin(
        HttpMethod::Get,
        "/iceberg/v1/{warehouse}/namespaces/{namespace}",
        GET_TABLE_NAMESPACE,
        RouteRiskLevel::Sensitive,
    ),
    admin(
        HttpMethod::Head,
        "/iceberg/v1/{warehouse}/namespaces/{namespace}",
        GET_TABLE_NAMESPACE,
        RouteRiskLevel::Sensitive,
    ),
    admin(
        HttpMethod::Delete,
        "/iceberg/v1/{warehouse}/namespaces/{namespace}",
        DELETE_TABLE_NAMESPACE,
        RouteRiskLevel::High,
    ),
    admin(
        HttpMethod::Get,
        "/iceberg/v1/{warehouse}/namespaces/{namespace}/tables",
        GET_TABLE,
        RouteRiskLevel::Sensitive,
    ),
    admin(
        HttpMethod::Post,
        "/iceberg/v1/{warehouse}/namespaces/{namespace}/tables",
        CREATE_TABLE,
        RouteRiskLevel::High,
    ),
    admin(
        HttpMethod::Post,
        "/iceberg/v1/{warehouse}/namespaces/{namespace}/register",
        REGISTER_TABLE,
        RouteRiskLevel::High,
    ),
    admin(
        HttpMethod::Get,
        "/iceberg/v1/{warehouse}/namespaces/{namespace}/views",
        GET_TABLE_METADATA,
        RouteRiskLevel::Sensitive,
    ),
    admin(
        HttpMethod::Post,
        "/iceberg/v1/{warehouse}/namespaces/{namespace}/views",
        CREATE_TABLE,
        RouteRiskLevel::High,
    ),
    admin(
        HttpMethod::Get,
        "/iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}",
        GET_TABLE_METADATA,
        RouteRiskLevel::Sensitive,
    ),
    admin(
        HttpMethod::Head,
        "/iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}",
        GET_TABLE,
        RouteRiskLevel::Sensitive,
    ),
    admin(
        HttpMethod::Get,
        "/iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}/credentials",
        GET_TABLE_CREDENTIALS,
        RouteRiskLevel::Sensitive,
    ),
    admin(
        HttpMethod::Post,
        "/iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}",
        COMMIT_TABLE,
        RouteRiskLevel::High,
    ),
    admin(
        HttpMethod::Delete,
        "/iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}",
        DELETE_TABLE,
        RouteRiskLevel::High,
    ),
    admin(
        HttpMethod::Get,
        "/iceberg/v1/{warehouse}/namespaces/{namespace}/views/{view}",
        GET_TABLE_METADATA,
        RouteRiskLevel::Sensitive,
    ),
    admin(
        HttpMethod::Head,
        "/iceberg/v1/{warehouse}/namespaces/{namespace}/views/{view}",
        GET_TABLE,
        RouteRiskLevel::Sensitive,
    ),
    admin(
        HttpMethod::Post,
        "/iceberg/v1/{warehouse}/namespaces/{namespace}/views/{view}",
        COMMIT_TABLE,
        RouteRiskLevel::High,
    ),
    admin(
        HttpMethod::Delete,
        "/iceberg/v1/{warehouse}/namespaces/{namespace}/views/{view}",
        DELETE_TABLE,
        RouteRiskLevel::High,
    ),
    admin(
        HttpMethod::Get,
        "/iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}/refs",
        GET_TABLE_METADATA,
        RouteRiskLevel::Sensitive,
    ),
    admin(
        HttpMethod::Put,
        "/iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}/refs/{ref}",
        COMMIT_TABLE,
        RouteRiskLevel::High,
    ),
    admin(
        HttpMethod::Delete,
        "/iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}/refs/{ref}",
        COMMIT_TABLE,
        RouteRiskLevel::High,
    ),
    admin(
        HttpMethod::Get,
        "/iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}/metadata-location",
        GET_TABLE_METADATA_LOCATION,
        RouteRiskLevel::Sensitive,
    ),
    admin(
        HttpMethod::Put,
        "/iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}/metadata-location",
        SET_TABLE_METADATA_LOCATION,
        RouteRiskLevel::High,
    ),
    admin(
        HttpMethod::Post,
        "/iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}/maintenance/metadata",
        RUN_TABLE_MAINTENANCE,
        RouteRiskLevel::High,
    ),
    admin(
        HttpMethod::Get,
        "/iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}/maintenance/config",
        GET_TABLE_LIFECYCLE,
        RouteRiskLevel::Sensitive,
    ),
    admin(
        HttpMethod::Put,
        "/iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}/maintenance/config",
        SET_TABLE_LIFECYCLE,
        RouteRiskLevel::High,
    ),
    admin(
        HttpMethod::Get,
        "/iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}/maintenance/jobs/{job}",
        GET_TABLE_LIFECYCLE,
        RouteRiskLevel::Sensitive,
    ),
    admin(
        HttpMethod::Get,
        "/iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}/maintenance/scheduler",
        GET_TABLE_LIFECYCLE,
        RouteRiskLevel::Sensitive,
    ),
    admin(
        HttpMethod::Post,
        "/iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}/maintenance/scheduler/run",
        RUN_TABLE_MAINTENANCE,
        RouteRiskLevel::High,
    ),
    admin(
        HttpMethod::Post,
        "/iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}/maintenance/worker/run",
        RUN_TABLE_MAINTENANCE,
        RouteRiskLevel::High,
    ),
    admin(
        HttpMethod::Post,
        "/iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}/maintenance/jobs/{job}/heartbeat",
        RUN_TABLE_MAINTENANCE,
        RouteRiskLevel::High,
    ),
    admin(
        HttpMethod::Post,
        "/iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}/maintenance/jobs/{job}/quarantine",
        RUN_TABLE_MAINTENANCE,
        RouteRiskLevel::High,
    ),
    admin(
        HttpMethod::Get,
        "/iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}/catalog/export",
        GET_TABLE_METADATA,
        RouteRiskLevel::Sensitive,
    ),
    admin(
        HttpMethod::Post,
        "/iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}/catalog/import",
        REGISTER_TABLE,
        RouteRiskLevel::High,
    ),
    admin(
        HttpMethod::Get,
        "/iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}/catalog/external",
        GET_TABLE_METADATA,
        RouteRiskLevel::Sensitive,
    ),
    admin(
        HttpMethod::Put,
        "/iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}/catalog/external",
        REGISTER_TABLE,
        RouteRiskLevel::High,
    ),
    admin(
        HttpMethod::Post,
        "/iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}/catalog/external/sync",
        SET_TABLE_METADATA_LOCATION,
        RouteRiskLevel::High,
    ),
    admin(
        HttpMethod::Get,
        "/iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}/catalog/diagnostics",
        GET_TABLE_METADATA,
        RouteRiskLevel::Sensitive,
    ),
    admin(
        HttpMethod::Post,
        "/iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}/catalog/recovery",
        COMMIT_TABLE,
        RouteRiskLevel::High,
    ),
    admin(
        HttpMethod::Post,
        "/iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}/catalog/rollback",
        COMMIT_TABLE,
        RouteRiskLevel::High,
    ),
    admin(HttpMethod::Get, "/_iceberg/v1/config", GET_TABLE_CATALOG, RouteRiskLevel::Sensitive),
    admin(
        HttpMethod::Put,
        "/_iceberg/v1/buckets/{warehouse}",
        SET_TABLE_BUCKET,
        RouteRiskLevel::High,
    ),
    admin(
        HttpMethod::Get,
        "/_iceberg/v1/buckets/{warehouse}",
        GET_TABLE_BUCKET,
        RouteRiskLevel::Sensitive,
    ),
    admin(
        HttpMethod::Get,
        "/_iceberg/v1/{warehouse}/catalog/migration",
        GET_TABLE_CATALOG,
        RouteRiskLevel::Sensitive,
    ),
    admin(
        HttpMethod::Get,
        "/_iceberg/v1/{warehouse}/namespaces",
        GET_TABLE_NAMESPACE,
        RouteRiskLevel::Sensitive,
    ),
    admin(
        HttpMethod::Post,
        "/_iceberg/v1/{warehouse}/namespaces",
        SET_TABLE_NAMESPACE,
        RouteRiskLevel::High,
    ),
    admin(
        HttpMethod::Get,
        "/_iceberg/v1/{warehouse}/namespaces/{namespace}",
        GET_TABLE_NAMESPACE,
        RouteRiskLevel::Sensitive,
    ),
    admin(
        HttpMethod::Head,
        "/_iceberg/v1/{warehouse}/namespaces/{namespace}",
        GET_TABLE_NAMESPACE,
        RouteRiskLevel::Sensitive,
    ),
    admin(
        HttpMethod::Delete,
        "/_iceberg/v1/{warehouse}/namespaces/{namespace}",
        DELETE_TABLE_NAMESPACE,
        RouteRiskLevel::High,
    ),
    admin(
        HttpMethod::Get,
        "/_iceberg/v1/{warehouse}/namespaces/{namespace}/tables",
        GET_TABLE,
        RouteRiskLevel::Sensitive,
    ),
    admin(
        HttpMethod::Post,
        "/_iceberg/v1/{warehouse}/namespaces/{namespace}/tables",
        CREATE_TABLE,
        RouteRiskLevel::High,
    ),
    admin(
        HttpMethod::Post,
        "/_iceberg/v1/{warehouse}/namespaces/{namespace}/register",
        REGISTER_TABLE,
        RouteRiskLevel::High,
    ),
    admin(
        HttpMethod::Get,
        "/_iceberg/v1/{warehouse}/namespaces/{namespace}/views",
        GET_TABLE_METADATA,
        RouteRiskLevel::Sensitive,
    ),
    admin(
        HttpMethod::Post,
        "/_iceberg/v1/{warehouse}/namespaces/{namespace}/views",
        CREATE_TABLE,
        RouteRiskLevel::High,
    ),
    admin(
        HttpMethod::Get,
        "/_iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}",
        GET_TABLE_METADATA,
        RouteRiskLevel::Sensitive,
    ),
    admin(
        HttpMethod::Head,
        "/_iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}",
        GET_TABLE,
        RouteRiskLevel::Sensitive,
    ),
    admin(
        HttpMethod::Get,
        "/_iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}/credentials",
        GET_TABLE_CREDENTIALS,
        RouteRiskLevel::Sensitive,
    ),
    admin(
        HttpMethod::Post,
        "/_iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}",
        COMMIT_TABLE,
        RouteRiskLevel::High,
    ),
    admin(
        HttpMethod::Delete,
        "/_iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}",
        DELETE_TABLE,
        RouteRiskLevel::High,
    ),
    admin(
        HttpMethod::Get,
        "/_iceberg/v1/{warehouse}/namespaces/{namespace}/views/{view}",
        GET_TABLE_METADATA,
        RouteRiskLevel::Sensitive,
    ),
    admin(
        HttpMethod::Head,
        "/_iceberg/v1/{warehouse}/namespaces/{namespace}/views/{view}",
        GET_TABLE,
        RouteRiskLevel::Sensitive,
    ),
    admin(
        HttpMethod::Post,
        "/_iceberg/v1/{warehouse}/namespaces/{namespace}/views/{view}",
        COMMIT_TABLE,
        RouteRiskLevel::High,
    ),
    admin(
        HttpMethod::Delete,
        "/_iceberg/v1/{warehouse}/namespaces/{namespace}/views/{view}",
        DELETE_TABLE,
        RouteRiskLevel::High,
    ),
    admin(
        HttpMethod::Get,
        "/_iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}/refs",
        GET_TABLE_METADATA,
        RouteRiskLevel::Sensitive,
    ),
    admin(
        HttpMethod::Put,
        "/_iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}/refs/{ref}",
        COMMIT_TABLE,
        RouteRiskLevel::High,
    ),
    admin(
        HttpMethod::Delete,
        "/_iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}/refs/{ref}",
        COMMIT_TABLE,
        RouteRiskLevel::High,
    ),
    admin(
        HttpMethod::Get,
        "/_iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}/metadata-location",
        GET_TABLE_METADATA_LOCATION,
        RouteRiskLevel::Sensitive,
    ),
    admin(
        HttpMethod::Put,
        "/_iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}/metadata-location",
        SET_TABLE_METADATA_LOCATION,
        RouteRiskLevel::High,
    ),
    admin(
        HttpMethod::Post,
        "/_iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}/maintenance/metadata",
        RUN_TABLE_MAINTENANCE,
        RouteRiskLevel::High,
    ),
    admin(
        HttpMethod::Get,
        "/_iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}/maintenance/config",
        GET_TABLE_LIFECYCLE,
        RouteRiskLevel::Sensitive,
    ),
    admin(
        HttpMethod::Put,
        "/_iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}/maintenance/config",
        SET_TABLE_LIFECYCLE,
        RouteRiskLevel::High,
    ),
    admin(
        HttpMethod::Get,
        "/_iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}/maintenance/jobs/{job}",
        GET_TABLE_LIFECYCLE,
        RouteRiskLevel::Sensitive,
    ),
    admin(
        HttpMethod::Get,
        "/_iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}/maintenance/scheduler",
        GET_TABLE_LIFECYCLE,
        RouteRiskLevel::Sensitive,
    ),
    admin(
        HttpMethod::Post,
        "/_iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}/maintenance/scheduler/run",
        RUN_TABLE_MAINTENANCE,
        RouteRiskLevel::High,
    ),
    admin(
        HttpMethod::Post,
        "/_iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}/maintenance/worker/run",
        RUN_TABLE_MAINTENANCE,
        RouteRiskLevel::High,
    ),
    admin(
        HttpMethod::Post,
        "/_iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}/maintenance/jobs/{job}/heartbeat",
        RUN_TABLE_MAINTENANCE,
        RouteRiskLevel::High,
    ),
    admin(
        HttpMethod::Post,
        "/_iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}/maintenance/jobs/{job}/quarantine",
        RUN_TABLE_MAINTENANCE,
        RouteRiskLevel::High,
    ),
    admin(
        HttpMethod::Get,
        "/_iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}/catalog/export",
        GET_TABLE_METADATA,
        RouteRiskLevel::Sensitive,
    ),
    admin(
        HttpMethod::Post,
        "/_iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}/catalog/import",
        REGISTER_TABLE,
        RouteRiskLevel::High,
    ),
    admin(
        HttpMethod::Get,
        "/_iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}/catalog/external",
        GET_TABLE_METADATA,
        RouteRiskLevel::Sensitive,
    ),
    admin(
        HttpMethod::Put,
        "/_iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}/catalog/external",
        REGISTER_TABLE,
        RouteRiskLevel::High,
    ),
    admin(
        HttpMethod::Post,
        "/_iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}/catalog/external/sync",
        SET_TABLE_METADATA_LOCATION,
        RouteRiskLevel::High,
    ),
    admin(
        HttpMethod::Get,
        "/_iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}/catalog/diagnostics",
        GET_TABLE_METADATA,
        RouteRiskLevel::Sensitive,
    ),
    admin(
        HttpMethod::Post,
        "/_iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}/catalog/recovery",
        COMMIT_TABLE,
        RouteRiskLevel::High,
    ),
    admin(
        HttpMethod::Post,
        "/_iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}/catalog/rollback",
        COMMIT_TABLE,
        RouteRiskLevel::High,
    ),
];

pub const DEFERRED_ADMIN_ROUTE_POLICIES: &[DeferredAdminRoutePolicy] = &[
    deferred(HttpMethod::Get, "/rustfs/admin/v3/accountinfo", DeferredRoutePolicyReason::S3Action),
    deferred(
        HttpMethod::Get,
        "/rustfs/admin/v3/user-info",
        DeferredRoutePolicyReason::ContextualAuthorization,
    ),
    deferred(
        HttpMethod::Put,
        "/rustfs/admin/v3/add-user",
        DeferredRoutePolicyReason::ContextualAuthorization,
    ),
    deferred(
        HttpMethod::Post,
        "/rustfs/admin/v3/update-service-account",
        DeferredRoutePolicyReason::ContextualAuthorization,
    ),
    deferred(
        HttpMethod::Get,
        "/rustfs/admin/v3/info-service-account",
        DeferredRoutePolicyReason::ContextualAuthorization,
    ),
    deferred(
        HttpMethod::Get,
        "/rustfs/admin/v3/info-access-key",
        DeferredRoutePolicyReason::ContextualAuthorization,
    ),
    deferred(
        HttpMethod::Get,
        "/rustfs/admin/v3/list-service-accounts",
        DeferredRoutePolicyReason::ContextualAuthorization,
    ),
    deferred(
        HttpMethod::Get,
        "/rustfs/admin/v3/list-access-keys-bulk",
        DeferredRoutePolicyReason::MultipleActions,
    ),
    deferred(
        HttpMethod::Delete,
        "/rustfs/admin/v3/delete-service-accounts",
        DeferredRoutePolicyReason::ContextualAuthorization,
    ),
    deferred(
        HttpMethod::Delete,
        "/rustfs/admin/v3/delete-service-account",
        DeferredRoutePolicyReason::ContextualAuthorization,
    ),
    deferred(
        HttpMethod::Get,
        "/rustfs/admin/v3/idp/builtin/policy-entities",
        DeferredRoutePolicyReason::MultipleActions,
    ),
    deferred(HttpMethod::Post, "/rustfs/admin/v3/service", DeferredRoutePolicyReason::NotImplemented),
    deferred(
        HttpMethod::Get,
        "/rustfs/admin/v3/inspect-data",
        DeferredRoutePolicyReason::NotImplemented,
    ),
    deferred(
        HttpMethod::Post,
        "/rustfs/admin/v3/inspect-data",
        DeferredRoutePolicyReason::NotImplemented,
    ),
    deferred(
        HttpMethod::Get,
        "/rustfs/admin/v3/datausageinfo",
        DeferredRoutePolicyReason::MultipleActions,
    ),
    deferred(
        HttpMethod::Post,
        "/rustfs/admin/v3/object-zip-downloads",
        DeferredRoutePolicyReason::S3Action,
    ),
    deferred(
        HttpMethod::Get,
        "/rustfs/admin/v3/object-zip-downloads/{id}.zip",
        DeferredRoutePolicyReason::CredentialOnly,
    ),
    deferred(HttpMethod::Get, "/rustfs/admin/v3/pools/list", DeferredRoutePolicyReason::MultipleActions),
    deferred(
        HttpMethod::Get,
        "/rustfs/admin/v3/pools/status",
        DeferredRoutePolicyReason::MultipleActions,
    ),
    deferred(HttpMethod::Get, "/rustfs/admin/v3/get-bucket-quota", DeferredRoutePolicyReason::S3Action),
    deferred(HttpMethod::Get, "/rustfs/admin/v3/quota/{bucket}", DeferredRoutePolicyReason::S3Action),
    deferred(
        HttpMethod::Get,
        "/rustfs/admin/v3/quota-stats/{bucket}",
        DeferredRoutePolicyReason::S3Action,
    ),
    deferred(
        HttpMethod::Post,
        "/rustfs/admin/v3/quota-check/{bucket}",
        DeferredRoutePolicyReason::S3Action,
    ),
    deferred(
        HttpMethod::Get,
        "/rustfs/admin/v3/list-remote-targets",
        DeferredRoutePolicyReason::CredentialOnly,
    ),
];

pub fn validate_admin_route_policy_specs() -> Result<(), AdminRouteMatrixError> {
    validate_admin_route_specs(ADMIN_ROUTE_POLICY_SPECS)
}

const fn admin(method: HttpMethod, path: &'static str, action: AdminActionRef, risk_level: RouteRiskLevel) -> AdminRouteSpec {
    AdminRouteSpec::admin(method, path, action, risk_level)
}

const fn public(method: HttpMethod, path: &'static str, kind: PublicRouteKind, risk_level: RouteRiskLevel) -> AdminRouteSpec {
    AdminRouteSpec::public(method, path, kind, risk_level)
}

const fn deferred(method: HttpMethod, path: &'static str, reason: DeferredRoutePolicyReason) -> DeferredAdminRoutePolicy {
    DeferredAdminRoutePolicy::new(method, path, reason)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::admin::router::{AdminOperation, S3Router};
    use hyper::Method;
    use serial_test::serial;
    use std::collections::BTreeSet;
    use temp_env::with_var;

    #[test]
    fn admin_route_policy_specs_validate() {
        assert!(validate_admin_route_policy_specs().is_ok());
    }

    #[test]
    fn route_policy_inventory_has_no_internal_overlap() {
        let mut seen = BTreeSet::new();

        for spec in ADMIN_ROUTE_POLICY_SPECS {
            let key = route_key(spec.method(), spec.path());
            assert!(seen.insert(key), "duplicate direct route policy for {}", spec.path());
        }

        for policy in DEFERRED_ADMIN_ROUTE_POLICIES {
            let key = route_key(policy.method(), policy.path());
            assert!(seen.insert(key), "deferred route overlaps direct policy for {}", policy.path());
        }
    }

    #[test]
    #[serial]
    fn route_policy_inventory_covers_registered_routes() {
        let router = with_var(rustfs_config::ENV_HEALTH_ENDPOINT_ENABLE, Some("true"), || {
            let mut router = S3Router::<AdminOperation>::new(false);
            super::super::register_admin_routes(&mut router).expect("register production admin routes");
            router
        });

        let registered = router.registered_routes().iter().cloned().collect::<BTreeSet<_>>();
        let inventory = route_policy_inventory_keys();

        assert_eq!(inventory, registered, "admin route policy inventory must cover every registered route");
    }

    #[test]
    fn route_policy_keeps_public_exceptions_explicit() {
        assert_public(HttpMethod::Get, "/health", PublicRouteKind::Health);
        assert_public(HttpMethod::Head, "/health/ready", PublicRouteKind::Health);
        assert_public(HttpMethod::Post, "/", PublicRouteKind::StsFormPost);
        assert_public(
            HttpMethod::Get,
            "/rustfs/admin/v3/oidc/authorize/{provider_id}",
            PublicRouteKind::OidcBootstrap,
        );
    }

    #[test]
    fn route_policy_keeps_table_catalog_outside_admin_prefix() {
        let table_specs = ADMIN_ROUTE_POLICY_SPECS
            .iter()
            .filter(|spec| spec.path().starts_with("/iceberg/v1") || spec.path().starts_with("/_iceberg/v1"));
        assert_eq!(table_specs.count(), 90);
        assert_action(HttpMethod::Put, "/iceberg/v1/buckets/{warehouse}", SET_TABLE_BUCKET);
        assert_action(HttpMethod::Get, "/_iceberg/v1/buckets/{warehouse}", GET_TABLE_BUCKET);
        assert_action(HttpMethod::Get, "/iceberg/v1/{warehouse}/namespaces", GET_TABLE_NAMESPACE);
        assert_action(HttpMethod::Get, "/_iceberg/v1/{warehouse}/namespaces", GET_TABLE_NAMESPACE);
        assert_action(HttpMethod::Head, "/iceberg/v1/{warehouse}/namespaces/{namespace}", GET_TABLE_NAMESPACE);
        assert_action(HttpMethod::Head, "/_iceberg/v1/{warehouse}/namespaces/{namespace}", GET_TABLE_NAMESPACE);
        assert_action(HttpMethod::Post, "/iceberg/v1/{warehouse}/namespaces/{namespace}/tables", CREATE_TABLE);
        assert_action(HttpMethod::Post, "/_iceberg/v1/{warehouse}/namespaces/{namespace}/tables", CREATE_TABLE);
        assert_action(
            HttpMethod::Get,
            "/iceberg/v1/{warehouse}/namespaces/{namespace}/views",
            GET_TABLE_METADATA,
        );
        assert_action(HttpMethod::Post, "/_iceberg/v1/{warehouse}/namespaces/{namespace}/views", CREATE_TABLE);
        assert_action(
            HttpMethod::Get,
            "/iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}",
            GET_TABLE_METADATA,
        );
        assert_action(
            HttpMethod::Get,
            "/iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}/credentials",
            GET_TABLE_CREDENTIALS,
        );
        assert_action(
            HttpMethod::Get,
            "/iceberg/v1/{warehouse}/namespaces/{namespace}/views/{view}",
            GET_TABLE_METADATA,
        );
        assert_action(
            HttpMethod::Head,
            "/_iceberg/v1/{warehouse}/namespaces/{namespace}/views/{view}",
            GET_TABLE,
        );
        assert_action(
            HttpMethod::Post,
            "/_iceberg/v1/{warehouse}/namespaces/{namespace}/views/{view}",
            COMMIT_TABLE,
        );
        assert_action(
            HttpMethod::Delete,
            "/iceberg/v1/{warehouse}/namespaces/{namespace}/views/{view}",
            DELETE_TABLE,
        );
        assert_action(
            HttpMethod::Get,
            "/iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}/refs",
            GET_TABLE_METADATA,
        );
        assert_action(
            HttpMethod::Put,
            "/iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}/refs/{ref}",
            COMMIT_TABLE,
        );
        assert_action(
            HttpMethod::Delete,
            "/_iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}/refs/{ref}",
            COMMIT_TABLE,
        );
        assert_action(
            HttpMethod::Get,
            "/_iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}",
            GET_TABLE_METADATA,
        );
        assert_action(
            HttpMethod::Head,
            "/iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}",
            GET_TABLE,
        );
        assert_action(
            HttpMethod::Head,
            "/_iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}",
            GET_TABLE,
        );
        assert_action(
            HttpMethod::Get,
            "/_iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}/credentials",
            GET_TABLE_CREDENTIALS,
        );
        assert_action(
            HttpMethod::Get,
            "/iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}/credentials",
            GET_TABLE_CREDENTIALS,
        );
        assert_action(
            HttpMethod::Post,
            "/iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}",
            COMMIT_TABLE,
        );
        assert_action(
            HttpMethod::Post,
            "/_iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}",
            COMMIT_TABLE,
        );
        assert_action(
            HttpMethod::Get,
            "/iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}/metadata-location",
            GET_TABLE_METADATA_LOCATION,
        );
        assert_action(
            HttpMethod::Put,
            "/_iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}/metadata-location",
            SET_TABLE_METADATA_LOCATION,
        );
        assert_action(
            HttpMethod::Post,
            "/iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}/maintenance/metadata",
            RUN_TABLE_MAINTENANCE,
        );
        assert_action(
            HttpMethod::Post,
            "/_iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}/maintenance/metadata",
            RUN_TABLE_MAINTENANCE,
        );
        assert_action(
            HttpMethod::Put,
            "/iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}/maintenance/config",
            SET_TABLE_LIFECYCLE,
        );
        assert_action(
            HttpMethod::Get,
            "/_iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}/maintenance/jobs/{job}",
            GET_TABLE_LIFECYCLE,
        );
        assert_action(
            HttpMethod::Get,
            "/iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}/maintenance/scheduler",
            GET_TABLE_LIFECYCLE,
        );
        assert_action(
            HttpMethod::Get,
            "/_iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}/maintenance/scheduler",
            GET_TABLE_LIFECYCLE,
        );
        assert_action(
            HttpMethod::Post,
            "/iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}/maintenance/scheduler/run",
            RUN_TABLE_MAINTENANCE,
        );
        assert_action(
            HttpMethod::Post,
            "/_iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}/maintenance/scheduler/run",
            RUN_TABLE_MAINTENANCE,
        );
        assert_action(
            HttpMethod::Post,
            "/iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}/maintenance/worker/run",
            RUN_TABLE_MAINTENANCE,
        );
        assert_action(
            HttpMethod::Post,
            "/_iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}/maintenance/jobs/{job}/heartbeat",
            RUN_TABLE_MAINTENANCE,
        );
        assert_action(
            HttpMethod::Post,
            "/iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}/maintenance/jobs/{job}/quarantine",
            RUN_TABLE_MAINTENANCE,
        );
        assert_action(
            HttpMethod::Post,
            "/_iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}/maintenance/jobs/{job}/quarantine",
            RUN_TABLE_MAINTENANCE,
        );
        assert_action(HttpMethod::Get, "/iceberg/v1/{warehouse}/catalog/migration", GET_TABLE_CATALOG);
        assert_action(HttpMethod::Get, "/_iceberg/v1/{warehouse}/catalog/migration", GET_TABLE_CATALOG);
        assert_action(
            HttpMethod::Post,
            "/iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}/catalog/import",
            REGISTER_TABLE,
        );
        assert_action(
            HttpMethod::Get,
            "/_iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}/catalog/external",
            GET_TABLE_METADATA,
        );
        assert_action(
            HttpMethod::Put,
            "/iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}/catalog/external",
            REGISTER_TABLE,
        );
        assert_action(
            HttpMethod::Post,
            "/_iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}/catalog/external/sync",
            SET_TABLE_METADATA_LOCATION,
        );
        assert_action(
            HttpMethod::Post,
            "/iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}/catalog/recovery",
            COMMIT_TABLE,
        );
        assert_action(
            HttpMethod::Post,
            "/_iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}/catalog/recovery",
            COMMIT_TABLE,
        );
        assert_action(
            HttpMethod::Post,
            "/_iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}/catalog/rollback",
            COMMIT_TABLE,
        );
    }

    #[test]
    fn route_policy_records_dedicated_kms_actions() {
        assert_action(HttpMethod::Post, "/rustfs/admin/v3/kms/create-key", KMS_CONFIGURE);
        assert_action(HttpMethod::Get, "/rustfs/admin/v3/kms/describe-key", KMS_DESCRIBE_KEY);
        assert_action(HttpMethod::Get, "/rustfs/admin/v3/kms/list-keys", KMS_LIST_KEYS);
        assert_action(HttpMethod::Post, "/rustfs/admin/v3/kms/generate-data-key", KMS_GENERATE_DATA_KEY);
        assert_action(HttpMethod::Post, "/rustfs/admin/v3/kms/clear-cache", KMS_CLEAR_CACHE);
        assert_action(HttpMethod::Post, "/rustfs/admin/v3/kms/configure", KMS_CONFIGURE);
        assert_action(HttpMethod::Post, "/rustfs/admin/v3/kms/start", KMS_SERVICE_CONTROL);
        assert_action(HttpMethod::Delete, "/rustfs/admin/v3/kms/keys/delete", KMS_DELETE_KEY);
        assert_action(HttpMethod::Post, "/rustfs/admin/v3/kms/keys/cancel-deletion", KMS_DELETE_KEY);
        assert_action(HttpMethod::Get, "/rustfs/admin/v3/kms/keys/{key_id}", KMS_DESCRIBE_KEY);
    }

    #[test]
    fn route_policy_rejects_server_info_for_sensitive_kms_actions() {
        for (method, path) in [
            (HttpMethod::Post, "/rustfs/admin/v3/kms/generate-data-key"),
            (HttpMethod::Post, "/rustfs/admin/v3/kms/clear-cache"),
            (HttpMethod::Post, "/rustfs/admin/v3/kms/configure"),
            (HttpMethod::Delete, "/rustfs/admin/v3/kms/keys/delete"),
            (HttpMethod::Post, "/rustfs/admin/v3/kms/keys/cancel-deletion"),
        ] {
            assert_not_action(method, path, SERVER_INFO);
        }
    }

    #[test]
    fn route_policy_keeps_contextual_auth_deferred() {
        assert_deferred(
            HttpMethod::Get,
            "/rustfs/admin/v3/info-service-account",
            DeferredRoutePolicyReason::ContextualAuthorization,
        );
        assert_deferred(
            HttpMethod::Get,
            "/rustfs/admin/v3/datausageinfo",
            DeferredRoutePolicyReason::MultipleActions,
        );
        assert_deferred(HttpMethod::Get, "/rustfs/admin/v3/accountinfo", DeferredRoutePolicyReason::S3Action);
        assert_deferred(
            HttpMethod::Post,
            "/rustfs/admin/v3/object-zip-downloads",
            DeferredRoutePolicyReason::S3Action,
        );
        assert_deferred(
            HttpMethod::Get,
            "/rustfs/admin/v3/object-zip-downloads/{id}.zip",
            DeferredRoutePolicyReason::CredentialOnly,
        );
    }

    #[test]
    fn route_policy_maps_metrics_to_explicit_admin_action() {
        assert_action(HttpMethod::Get, "/rustfs/admin/v3/metrics", GET_METRICS);
    }

    #[test]
    fn route_policy_requires_operation_for_site_replication_diagnostics() {
        for path in [
            "/rustfs/admin/v3/site-replication/devnull",
            "/rustfs/admin/v3/site-replication/netperf",
        ] {
            assert_action(HttpMethod::Post, path, SITE_REPLICATION_OPERATION);
            assert_not_action(HttpMethod::Post, path, SITE_REPLICATION_INFO);
        }
    }

    #[test]
    fn route_policy_accepts_minio_style_site_replication_join_alias() {
        assert_action(HttpMethod::Put, "/rustfs/admin/v3/site-replication/join", SITE_REPLICATION_ADD);
    }

    #[test]
    fn route_policy_requires_operation_for_site_replication_repair() {
        assert_action(HttpMethod::Put, "/rustfs/admin/v3/site-replication/repair", SITE_REPLICATION_OPERATION);
    }

    fn route_policy_inventory_keys() -> BTreeSet<String> {
        ADMIN_ROUTE_POLICY_SPECS
            .iter()
            .map(|spec| route_key(spec.method(), spec.path()))
            .chain(
                DEFERRED_ADMIN_ROUTE_POLICIES
                    .iter()
                    .map(|policy| route_key(policy.method(), policy.path())),
            )
            .collect()
    }

    fn route_key(method: HttpMethod, path: &str) -> String {
        format!("{}|{}", method.as_str(), path)
    }

    fn assert_action(method: HttpMethod, path: &str, action: AdminActionRef) {
        let spec = ADMIN_ROUTE_POLICY_SPECS
            .iter()
            .find(|spec| spec.method() == method && spec.path() == path)
            .expect("expected direct route policy");
        assert_eq!(spec.access().admin_action(), Some(action));
    }

    fn assert_not_action(method: HttpMethod, path: &str, action: AdminActionRef) {
        let spec = ADMIN_ROUTE_POLICY_SPECS
            .iter()
            .find(|spec| spec.method() == method && spec.path() == path)
            .expect("expected direct route policy");
        assert_ne!(spec.access().admin_action(), Some(action));
    }

    fn assert_public(method: HttpMethod, path: &str, kind: PublicRouteKind) {
        let spec = ADMIN_ROUTE_POLICY_SPECS
            .iter()
            .find(|spec| spec.method() == method && spec.path() == path)
            .expect("expected public route policy");
        assert_eq!(spec.access().public_kind(), Some(kind));
    }

    fn assert_deferred(method: HttpMethod, path: &str, reason: DeferredRoutePolicyReason) {
        let policy = DEFERRED_ADMIN_ROUTE_POLICIES
            .iter()
            .find(|policy| policy.method() == method && policy.path() == path)
            .expect("expected deferred route policy");
        assert_eq!(policy.reason(), reason);
    }

    #[test]
    fn method_mapping_matches_registered_route_format() {
        assert_eq!(route_key(HttpMethod::Get, "/health"), Method::GET.as_str().to_string() + "|/health");
    }
}
