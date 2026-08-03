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

use crate::admin::runtime_sources::default_admin_usecase;
use crate::admin::runtime_sources::{current_object_store_handle, current_token_signing_key};
use crate::admin::storage_api::bucket::{metadata::table_catalog_path_hash, metadata_sys};
use crate::admin::storage_api::runtime::ECStore;
use crate::admin::{
    auth::{AdminResourceScope, validate_admin_request, validate_admin_request_with_bucket_object},
    router::{AdminOperation, Operation, S3Router},
};
use crate::auth::{check_key_valid, get_session_token};
use crate::server::{RemoteAddr, TABLE_CATALOG_COMPAT_PREFIX, TABLE_CATALOG_PREFIX};
use crate::table_catalog::{DEFAULT_WAREHOUSE_ID, TableCatalogStore};
use http::{HeaderMap, HeaderValue, StatusCode};
use hyper::Method;
use matchit::Params;
use metrics::{counter, histogram};
use rustfs_config::MAX_ADMIN_REQUEST_BODY_SIZE;
use rustfs_iam::sys::SESSION_POLICY_NAME;
use rustfs_policy::{
    auth::get_new_credentials_with_metadata,
    policy::{
        Policy,
        action::{Action, AdminAction},
    },
};
use rustfs_utils::crypto::{base64_decode_url_safe_no_pad, base64_encode_url_safe_no_pad, hex_sha256};
use s3s::{Body, S3Error, S3ErrorCode, S3Request, S3Response, S3Result, header::CONTENT_TYPE, s3_error};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::num::NonZeroUsize;
use std::time::{Duration as StdDuration, Instant};
use time::{Duration, OffsetDateTime};
use uuid::Uuid;

const JSON_CONTENT_TYPE: &str = "application/json";
const ENV_TABLE_CATALOG_CREDENTIAL_VENDING: &str = "RUSTFS_TABLE_CATALOG_CREDENTIAL_VENDING";
const ENV_TABLE_CATALOG_CREDENTIAL_TTL_SECONDS: &str = "RUSTFS_TABLE_CATALOG_CREDENTIAL_TTL_SECONDS";
const DEFAULT_TABLE_CATALOG_CREDENTIAL_TTL_SECONDS: i64 = 15 * 60;
const MIN_TABLE_CATALOG_CREDENTIAL_TTL_SECONDS: i64 = 60;
const MAX_TABLE_CATALOG_CREDENTIAL_TTL_SECONDS: i64 = 60 * 60;
const WAREHOUSE_PROPERTY: &str = "warehouse";
const PREFIX_PROPERTY: &str = "prefix";
const ICEBERG_ERROR_ALREADY_EXISTS: &str = "AlreadyExistsException";
const ICEBERG_ERROR_BAD_REQUEST: &str = "BadRequestException";
const ICEBERG_ERROR_COMMIT_FAILED: &str = "CommitFailedException";
const ICEBERG_ERROR_NAMESPACE_NOT_EMPTY: &str = "NamespaceNotEmptyException";
const ICEBERG_ERROR_NO_SUCH_NAMESPACE: &str = "NoSuchNamespaceException";
const ICEBERG_ERROR_NO_SUCH_RESOURCE: &str = "NoSuchResourceException";
const ICEBERG_ERROR_NO_SUCH_TABLE: &str = "NoSuchTableException";
const ICEBERG_ERROR_NO_SUCH_VIEW: &str = "NoSuchViewException";
const ICEBERG_ERROR_REST: &str = "RESTException";
const REST_PAGE_TOKEN_VERSION: u8 = 1;
const REST_PAGE_TOKEN_MAX_LENGTH: usize = 16 * 1024;
const REST_DEFAULT_PAGE_SIZE: usize = 1000;
const REST_MAX_PAGE_SIZE: usize = 1000;
const REST_PAGE_TOKEN_QUERY_PARAMETER: &str = "pageToken";
const REST_PAGE_SIZE_QUERY_PARAMETER: &str = "pageSize";
const CATALOG_ENDPOINT_PREFIX_CONFIG_KEY: &str = "rustfs.catalog-endpoint-prefix";
const CATALOG_COMPAT_ENDPOINT_PREFIX_CONFIG_KEY: &str = "rustfs.catalog-compat-endpoint-prefix";
const CATALOG_BACKING_CONFIG_KEY: &str = "rustfs.catalog-backing";
const CREDENTIAL_VENDING_CONFIG_KEY: &str = "rustfs.credential-vending";
const CREDENTIAL_VENDING_REASON_CONFIG_KEY: &str = "rustfs.credential-vending-reason";
const CREDENTIAL_SCOPE_CONFIG_KEY: &str = "rustfs.credential-scope";
const CREDENTIAL_SCOPE_PREFIX_CONFIG_KEY: &str = "rustfs.credential-scope-prefix";
const CREDENTIAL_MODE_CONFIG_KEY: &str = "rustfs.credential-mode";
const CREDENTIAL_EXPIRATION_CONFIG_KEY: &str = "rustfs.credential-expiration-unix-seconds";
const CREDENTIAL_VENDING_UNSUPPORTED: &str = "unsupported";
const CREDENTIAL_VENDING_SUPPORTED: &str = "supported";
const CREDENTIAL_VENDING_UNSUPPORTED_REASON: &str = "temporary-credentials-not-implemented";
const CREDENTIAL_VENDING_DISABLED_REASON: &str = "credential-vending-disabled";
const CREDENTIAL_SCOPE_WAREHOUSE_PREFIX: &str = "warehouse-prefix";
const CREDENTIAL_SCOPE_TABLE_PREFIX: &str = "table-prefix";
const CREDENTIAL_MODE_CLIENT_PROVIDED: &str = "client-provided-s3-credentials-required";
const CREDENTIAL_MODE_CATALOG_VENDED: &str = "catalog-vended-temporary-credentials";
const S3_ACCESS_KEY_ID_CONFIG_KEY: &str = "s3.access-key-id";
const S3_SECRET_ACCESS_KEY_CONFIG_KEY: &str = "s3.secret-access-key";
const S3_SESSION_TOKEN_CONFIG_KEY: &str = "s3.session-token";
const TABLE_CATALOG_NAMESPACE_RESOURCE_ROOT: &str = "namespaces";
const TABLE_CATALOG_TABLE_RESOURCE_ROOT: &str = "tables";
const TABLE_CATALOG_VIEW_RESOURCE_ROOT: &str = "views";
const TABLE_CATALOG_ADMIN_OPERATION_SLOW_LOG_THRESHOLD: StdDuration = StdDuration::from_secs(2);
const DEFAULT_TABLE_MAINTENANCE_SCHEDULER_ID: &str = "rustfs-maintenance-scheduler";
const DEFAULT_TABLE_MAINTENANCE_WORKER_ID: &str = "rustfs-maintenance-worker";
const EXTERNAL_CATALOG_BRIDGE_STATUS_UNCONFIGURED: &str = "bridge-unconfigured";
const EXTERNAL_CATALOG_BRIDGE_STATUS_CONFIGURED: &str = "bridge-configured";
const EXTERNAL_CATALOG_BRIDGE_SYNC_STATUS: &str = "synced";
const EXTERNAL_CATALOG_BRIDGE_SUPPORTED_STATUS: &str = "operator-sync-supported";
const EXTERNAL_CATALOG_BRIDGE_SUPPORTED_REASON: &str =
    "operator-supplied metadata pointer sync is supported; online vendor SDK synchronization is not claimed";
const EXTERNAL_CATALOG_POLICY_MODE_RUSTFS: &str = "rustfs-authoritative";
const EXTERNAL_CATALOG_CREDENTIAL_MODE_RUSTFS: &str = "rustfs-table-credentials";
const EXTERNAL_CATALOG_SYNC_MODE_MANUAL: &str = "manual";
const EXTERNAL_CATALOG_ROLLBACK_RETAIN_CURRENT: &str = "retain-current-pointer";
const EXTERNAL_CATALOG_SYNC_OPERATION: &str = "external-catalog-sync";
const EXTERNAL_CATALOG_SYNC_WRITER: &str = "rustfs-external-catalog-bridge";
const EXTERNAL_CATALOG_ACTION_REGISTERED: &str = "registered";
const EXTERNAL_CATALOG_ACTION_COMMITTED: &str = "committed";
const EXTERNAL_CATALOG_BRIDGE_CAPABILITIES: &[&str] = &["polaris", "glue", "dlf", "hive-metastore"];
const TABLE_CATALOG_ENDPOINTS: &[&str] = &[
    "GET /v1/{prefix}/namespaces",
    "POST /v1/{prefix}/namespaces",
    "GET /v1/{prefix}/namespaces/{namespace}",
    "HEAD /v1/{prefix}/namespaces/{namespace}",
    "DELETE /v1/{prefix}/namespaces/{namespace}",
    "GET /v1/{prefix}/namespaces/{namespace}/tables",
    "POST /v1/{prefix}/namespaces/{namespace}/tables",
    "POST /v1/{prefix}/namespaces/{namespace}/register",
    "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}",
    "HEAD /v1/{prefix}/namespaces/{namespace}/tables/{table}",
    "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}/credentials",
    "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}",
    "DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table}",
    "PUT /buckets/{warehouse}",
    "GET /buckets/{warehouse}",
    "GET /{warehouse}/catalog/migration",
    "POST /{warehouse}/catalog/migration",
    "DELETE /{warehouse}/catalog/migration",
    "GET /{warehouse}/namespaces",
    "POST /{warehouse}/namespaces",
    "GET /{warehouse}/namespaces/{namespace}",
    "HEAD /{warehouse}/namespaces/{namespace}",
    "DELETE /{warehouse}/namespaces/{namespace}",
    "GET /{warehouse}/namespaces/{namespace}/tables",
    "POST /{warehouse}/namespaces/{namespace}/tables",
    "POST /{warehouse}/namespaces/{namespace}/register",
    "GET /{warehouse}/namespaces/{namespace}/views",
    "POST /{warehouse}/namespaces/{namespace}/views",
    "GET /{warehouse}/namespaces/{namespace}/tables/{table}",
    "HEAD /{warehouse}/namespaces/{namespace}/tables/{table}",
    "GET /{warehouse}/namespaces/{namespace}/tables/{table}/credentials",
    "POST /{warehouse}/namespaces/{namespace}/tables/{table}",
    "DELETE /{warehouse}/namespaces/{namespace}/tables/{table}",
    "GET /{warehouse}/namespaces/{namespace}/views/{view}",
    "HEAD /{warehouse}/namespaces/{namespace}/views/{view}",
    "POST /{warehouse}/namespaces/{namespace}/views/{view}",
    "DELETE /{warehouse}/namespaces/{namespace}/views/{view}",
    "GET /{warehouse}/namespaces/{namespace}/tables/{table}/refs",
    "PUT /{warehouse}/namespaces/{namespace}/tables/{table}/refs/{ref}",
    "DELETE /{warehouse}/namespaces/{namespace}/tables/{table}/refs/{ref}",
    "POST /{warehouse}/namespaces/{namespace}/tables/{table}/maintenance/metadata",
    "GET /{warehouse}/namespaces/{namespace}/tables/{table}/metadata-location",
    "PUT /{warehouse}/namespaces/{namespace}/tables/{table}/metadata-location",
    "GET /{warehouse}/namespaces/{namespace}/tables/{table}/maintenance/config",
    "PUT /{warehouse}/namespaces/{namespace}/tables/{table}/maintenance/config",
    "GET /{warehouse}/namespaces/{namespace}/tables/{table}/maintenance/jobs/{job}",
    "GET /{warehouse}/namespaces/{namespace}/tables/{table}/maintenance/scheduler",
    "POST /{warehouse}/namespaces/{namespace}/tables/{table}/maintenance/scheduler/run",
    "POST /{warehouse}/namespaces/{namespace}/tables/{table}/maintenance/worker/run",
    "POST /{warehouse}/namespaces/{namespace}/tables/{table}/maintenance/jobs/{job}/heartbeat",
    "POST /{warehouse}/namespaces/{namespace}/tables/{table}/maintenance/jobs/{job}/quarantine",
    "GET /{warehouse}/namespaces/{namespace}/tables/{table}/catalog/export",
    "POST /{warehouse}/namespaces/{namespace}/tables/{table}/catalog/import",
    "GET /{warehouse}/namespaces/{namespace}/tables/{table}/catalog/external",
    "PUT /{warehouse}/namespaces/{namespace}/tables/{table}/catalog/external",
    "POST /{warehouse}/namespaces/{namespace}/tables/{table}/catalog/external/sync",
    "GET /{warehouse}/namespaces/{namespace}/tables/{table}/catalog/diagnostics",
    "POST /{warehouse}/namespaces/{namespace}/tables/{table}/catalog/recovery",
    "POST /{warehouse}/namespaces/{namespace}/tables/{table}/catalog/rollback",
];

static GET_CONFIG_HANDLER: GetCatalogConfigHandler = GetCatalogConfigHandler {};
static ENABLE_TABLE_BUCKET_HANDLER: EnableTableBucketHandler = EnableTableBucketHandler {};
static GET_TABLE_BUCKET_HANDLER: GetTableBucketHandler = GetTableBucketHandler {};
static GET_TABLE_CATALOG_MIGRATION_HANDLER: GetTableCatalogMigrationHandler = GetTableCatalogMigrationHandler {};
static MATERIALIZE_TABLE_CATALOG_MIGRATION_HANDLER: MaterializeTableCatalogMigrationHandler =
    MaterializeTableCatalogMigrationHandler {};
static CANCEL_TABLE_CATALOG_MIGRATION_HANDLER: CancelTableCatalogMigrationHandler = CancelTableCatalogMigrationHandler {};
static LIST_NAMESPACES_HANDLER: RestListNamespacesHandler = RestListNamespacesHandler {};
static CREATE_NAMESPACE_HANDLER: RestCreateNamespaceHandler = RestCreateNamespaceHandler {};
static GET_NAMESPACE_HANDLER: RestGetNamespaceHandler = RestGetNamespaceHandler {};
static NAMESPACE_EXISTS_HANDLER: RestNamespaceExistsHandler = RestNamespaceExistsHandler {};
static DROP_NAMESPACE_HANDLER: RestDropNamespaceHandler = RestDropNamespaceHandler {};
static LIST_TABLES_HANDLER: RestListTablesHandler = RestListTablesHandler {};
static CREATE_TABLE_HANDLER: RestCreateTableHandler = RestCreateTableHandler {};
static REGISTER_TABLE_HANDLER: RestRegisterTableHandler = RestRegisterTableHandler {};
static LIST_VIEWS_HANDLER: RestListViewsHandler = RestListViewsHandler {};
static CREATE_VIEW_HANDLER: RestCreateViewHandler = RestCreateViewHandler {};
static LOAD_TABLE_HANDLER: RestLoadTableHandler = RestLoadTableHandler {};
static TABLE_EXISTS_HANDLER: RestTableExistsHandler = RestTableExistsHandler {};
static LOAD_CREDENTIALS_HANDLER: RestLoadCredentialsHandler = RestLoadCredentialsHandler {};
static COMMIT_TABLE_HANDLER: RestCommitTableHandler = RestCommitTableHandler {};
static DROP_TABLE_HANDLER: RestDropTableHandler = RestDropTableHandler {};
static LOAD_VIEW_HANDLER: RestLoadViewHandler = RestLoadViewHandler {};
static VIEW_EXISTS_HANDLER: RestViewExistsHandler = RestViewExistsHandler {};
static REPLACE_VIEW_HANDLER: RestReplaceViewHandler = RestReplaceViewHandler {};
static DROP_VIEW_HANDLER: RestDropViewHandler = RestDropViewHandler {};
static LIST_TABLE_REFS_HANDLER: ListTableRefsHandler = ListTableRefsHandler {};
static PUT_TABLE_REF_HANDLER: PutTableRefHandler = PutTableRefHandler {};
static DELETE_TABLE_REF_HANDLER: DeleteTableRefHandler = DeleteTableRefHandler {};
static GET_TABLE_METADATA_LOCATION_HANDLER: GetTableMetadataLocationHandler = GetTableMetadataLocationHandler {};
static UPDATE_TABLE_METADATA_LOCATION_HANDLER: UpdateTableMetadataLocationHandler = UpdateTableMetadataLocationHandler {};
static TABLE_METADATA_MAINTENANCE_HANDLER: RestTableMetadataMaintenanceHandler = RestTableMetadataMaintenanceHandler {};
static GET_TABLE_MAINTENANCE_CONFIG_HANDLER: GetTableMaintenanceConfigHandler = GetTableMaintenanceConfigHandler {};
static PUT_TABLE_MAINTENANCE_CONFIG_HANDLER: PutTableMaintenanceConfigHandler = PutTableMaintenanceConfigHandler {};
static GET_TABLE_MAINTENANCE_JOB_HANDLER: GetTableMaintenanceJobHandler = GetTableMaintenanceJobHandler {};
static GET_TABLE_MAINTENANCE_SCHEDULER_HANDLER: GetTableMaintenanceSchedulerHandler = GetTableMaintenanceSchedulerHandler {};
static RUN_TABLE_MAINTENANCE_SCHEDULER_HANDLER: RunTableMaintenanceSchedulerHandler = RunTableMaintenanceSchedulerHandler {};
static RUN_TABLE_MAINTENANCE_WORKER_HANDLER: RunTableMaintenanceWorkerHandler = RunTableMaintenanceWorkerHandler {};
static HEARTBEAT_TABLE_MAINTENANCE_JOB_HANDLER: HeartbeatTableMaintenanceJobHandler = HeartbeatTableMaintenanceJobHandler {};
static TABLE_MAINTENANCE_QUARANTINE_HANDLER: TableMaintenanceQuarantineHandler = TableMaintenanceQuarantineHandler {};
static EXPORT_TABLE_CATALOG_HANDLER: ExportTableCatalogHandler = ExportTableCatalogHandler {};
static IMPORT_TABLE_CATALOG_HANDLER: ImportTableCatalogHandler = ImportTableCatalogHandler {};
static EXTERNAL_CATALOG_BRIDGE_HANDLER: ExternalCatalogBridgeHandler = ExternalCatalogBridgeHandler {};
static PUT_EXTERNAL_CATALOG_BRIDGE_HANDLER: PutExternalCatalogBridgeHandler = PutExternalCatalogBridgeHandler {};
static SYNC_EXTERNAL_CATALOG_BRIDGE_HANDLER: SyncExternalCatalogBridgeHandler = SyncExternalCatalogBridgeHandler {};
static GET_TABLE_CATALOG_DIAGNOSTICS_HANDLER: GetTableCatalogDiagnosticsHandler = GetTableCatalogDiagnosticsHandler {};
static RECOVER_TABLE_CATALOG_HANDLER: RecoverTableCatalogHandler = RecoverTableCatalogHandler {};
static ROLLBACK_TABLE_CATALOG_HANDLER: RollbackTableCatalogHandler = RollbackTableCatalogHandler {};

#[derive(Debug, Serialize)]
struct CatalogConfigResponse {
    defaults: BTreeMap<String, String>,
    overrides: BTreeMap<String, String>,
    endpoints: Vec<&'static str>,
    admin_discovery: CatalogAdminDiscovery,
}

#[derive(Debug, Serialize)]
struct CatalogAdminDiscovery {
    #[serde(rename = "runtimeCapabilities")]
    runtime_capabilities: &'static str,
    #[serde(rename = "clusterSnapshot")]
    cluster_snapshot: &'static str,
    #[serde(rename = "extensionsCatalog")]
    extensions_catalog: &'static str,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateNamespaceRequest {
    namespace: Vec<String>,
    #[serde(default)]
    properties: BTreeMap<String, String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RegisterTableRequest {
    name: String,
    #[serde(rename = "metadata-location")]
    metadata_location: String,
    #[serde(default)]
    overwrite: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateTableRequest {
    name: String,
    #[serde(default)]
    location: Option<String>,
    schema: serde_json::Value,
    #[serde(default, rename = "partition-spec")]
    partition_spec: Option<serde_json::Value>,
    #[serde(default, rename = "write-order")]
    write_order: Option<serde_json::Value>,
    #[serde(default, rename = "stage-create")]
    stage_create: bool,
    #[serde(default)]
    properties: BTreeMap<String, String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateViewRequest {
    name: String,
    #[serde(default)]
    location: Option<String>,
    schema: serde_json::Value,
    #[serde(rename = "view-version")]
    view_version: serde_json::Value,
    #[serde(default)]
    properties: BTreeMap<String, String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RestCommitTableRequest {
    #[serde(default, rename = "identifier")]
    _identifier: Option<serde_json::Value>,
    #[serde(default, rename = "commit-id")]
    commit_id: Option<String>,
    #[serde(default, rename = "idempotency-key")]
    idempotency_key: Option<String>,
    #[serde(default)]
    operation: Option<String>,
    #[serde(default, rename = "expected-version-token")]
    expected_version_token: Option<String>,
    #[serde(default, rename = "expected-metadata-location")]
    expected_metadata_location: Option<String>,
    #[serde(default, rename = "new-metadata-location")]
    new_metadata_location: Option<String>,
    #[serde(default)]
    requirements: Vec<serde_json::Value>,
    #[serde(default)]
    updates: Vec<serde_json::Value>,
    #[serde(default)]
    writer: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RestCommitViewRequest {
    #[serde(default, rename = "commit-id")]
    commit_id: Option<String>,
    #[serde(default, rename = "expected-version-token")]
    expected_version_token: Option<String>,
    #[serde(default, rename = "expected-metadata-location")]
    expected_metadata_location: Option<String>,
    #[serde(default, rename = "new-metadata-location")]
    new_metadata_location: Option<String>,
    #[serde(default)]
    requirements: Vec<serde_json::Value>,
    #[serde(default)]
    updates: Vec<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PutTableRefRequest {
    #[serde(rename = "snapshot-id")]
    snapshot_id: i64,
    #[serde(rename = "type")]
    ref_type: String,
    #[serde(default, rename = "expected-snapshot-id")]
    expected_snapshot_id: Option<serde_json::Value>,
    #[serde(default, rename = "min-snapshots-to-keep")]
    min_snapshots_to_keep: Option<i64>,
    #[serde(default, rename = "max-snapshot-age-ms")]
    max_snapshot_age_ms: Option<i64>,
    #[serde(default, rename = "max-ref-age-ms")]
    max_ref_age_ms: Option<i64>,
    #[serde(default, rename = "commit-id")]
    commit_id: Option<String>,
    #[serde(default, rename = "idempotency-key")]
    idempotency_key: Option<String>,
    #[serde(default)]
    writer: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct DeleteTableRefRequest {
    #[serde(default, rename = "expected-snapshot-id")]
    expected_snapshot_id: Option<serde_json::Value>,
    #[serde(default)]
    force: bool,
    #[serde(default, rename = "commit-id")]
    commit_id: Option<String>,
    #[serde(default, rename = "idempotency-key")]
    idempotency_key: Option<String>,
    #[serde(default)]
    writer: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TableMetadataMaintenanceRequest {
    #[serde(default, rename = "retain-recent-metadata-files")]
    retain_recent_metadata_files: usize,
    #[serde(default)]
    delete: bool,
    #[serde(default, rename = "snapshot-expiration")]
    snapshot_expiration: Option<crate::table_catalog::TableSnapshotExpirationConfig>,
    #[serde(default, rename = "commit-snapshot-expiration")]
    commit_snapshot_expiration: bool,
    #[serde(default)]
    compaction: Option<crate::table_catalog::TableCompactionPlanningConfig>,
    #[serde(default, rename = "commit-compaction")]
    commit_compaction: bool,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct TableMaintenanceSchedulerRunRequest {
    #[serde(default, rename = "scheduler-id")]
    scheduler_id: Option<String>,
}

impl TableMaintenanceSchedulerRunRequest {
    fn scheduler_id(&self) -> &str {
        self.scheduler_id.as_deref().unwrap_or(DEFAULT_TABLE_MAINTENANCE_SCHEDULER_ID)
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TableMaintenanceWorkerRunRequest {
    #[serde(default, rename = "worker-id")]
    worker_id: Option<String>,
}

impl TableMaintenanceWorkerRunRequest {
    fn worker_id(&self) -> &str {
        self.worker_id.as_deref().unwrap_or(DEFAULT_TABLE_MAINTENANCE_WORKER_ID)
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TableMaintenanceHeartbeatRequest {
    #[serde(rename = "lease-id")]
    lease_id: String,
    #[serde(rename = "worker-id")]
    worker_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct UpdateTableMetadataLocationRequest {
    #[serde(rename = "metadata-location", alias = "metadataLocation")]
    metadata_location: String,
    #[serde(rename = "version-token", alias = "versionToken")]
    version_token: String,
    #[serde(default, rename = "commit-id", alias = "commitId")]
    commit_id: Option<String>,
    #[serde(default, rename = "idempotency-key", alias = "idempotencyKey")]
    idempotency_key: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CatalogImportRequest {
    #[serde(rename = "metadata-location", alias = "metadataLocation")]
    metadata_location: String,
    #[serde(default)]
    properties: BTreeMap<String, String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExternalCatalogBridgeRequest {
    catalog: String,
    #[serde(default, rename = "external-catalog-id", alias = "externalCatalogId")]
    external_catalog_id: Option<String>,
    #[serde(rename = "external-namespace", alias = "externalNamespace")]
    external_namespace: String,
    #[serde(rename = "external-table", alias = "externalTable")]
    external_table: String,
    #[serde(default, rename = "external-table-uuid", alias = "externalTableUuid")]
    external_table_uuid: Option<String>,
    #[serde(default, rename = "metadata-location", alias = "metadataLocation")]
    metadata_location: Option<String>,
    #[serde(default, rename = "external-version-token", alias = "externalVersionToken")]
    external_version_token: Option<String>,
    #[serde(default, rename = "policy-mode", alias = "policyMode")]
    policy_mode: Option<String>,
    #[serde(default, rename = "credential-mode", alias = "credentialMode")]
    credential_mode: Option<String>,
    #[serde(default, rename = "sync-mode", alias = "syncMode")]
    sync_mode: Option<String>,
    #[serde(default)]
    properties: BTreeMap<String, String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExternalCatalogBridgeSyncRequest {
    catalog: String,
    #[serde(default, rename = "external-catalog-id", alias = "externalCatalogId")]
    external_catalog_id: Option<String>,
    #[serde(rename = "external-namespace", alias = "externalNamespace")]
    external_namespace: String,
    #[serde(rename = "external-table", alias = "externalTable")]
    external_table: String,
    #[serde(default, rename = "external-table-uuid", alias = "externalTableUuid")]
    external_table_uuid: Option<String>,
    #[serde(rename = "metadata-location", alias = "metadataLocation")]
    metadata_location: String,
    #[serde(default, rename = "external-version-token", alias = "externalVersionToken")]
    external_version_token: Option<String>,
    #[serde(default, rename = "expected-version-token", alias = "expectedVersionToken")]
    expected_version_token: Option<String>,
    #[serde(default, rename = "expected-metadata-location", alias = "expectedMetadataLocation")]
    expected_metadata_location: Option<String>,
    #[serde(default, rename = "commit-id", alias = "commitId")]
    commit_id: Option<String>,
    #[serde(default, rename = "idempotency-key", alias = "idempotencyKey")]
    idempotency_key: Option<String>,
    #[serde(default, rename = "policy-mode", alias = "policyMode")]
    policy_mode: Option<String>,
    #[serde(default, rename = "credential-mode", alias = "credentialMode")]
    credential_mode: Option<String>,
    #[serde(default, rename = "rollback-strategy", alias = "rollbackStrategy")]
    rollback_strategy: Option<String>,
    #[serde(default)]
    properties: BTreeMap<String, String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RollbackTableRequest {
    #[serde(rename = "metadata-location", alias = "metadataLocation")]
    metadata_location: String,
    #[serde(rename = "version-token", alias = "versionToken")]
    version_token: String,
    #[serde(default, rename = "commit-id", alias = "commitId")]
    commit_id: Option<String>,
    #[serde(default, rename = "idempotency-key", alias = "idempotencyKey")]
    idempotency_key: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "kebab-case")]
struct TableRefsResponse {
    table_bucket: String,
    namespace: String,
    table: String,
    current_metadata_location: String,
    current_snapshot_id: Option<i64>,
    protected_ref_count: usize,
    user_defined_ref_count: usize,
    refs: BTreeMap<String, serde_json::Value>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "kebab-case")]
struct ExternalCatalogBridgeResponse {
    table_bucket: String,
    namespace: String,
    table: String,
    status: String,
    supported_import: String,
    #[serde(default)]
    capabilities: Vec<ExternalCatalogBridgeCapability>,
    #[serde(default)]
    unsupported_bridges: Vec<ExternalCatalogBridgeCapability>,
    #[serde(skip_serializing_if = "Option::is_none")]
    bridge: Option<ExternalCatalogBridgeStateResponse>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "kebab-case")]
struct ExternalCatalogBridgeCapability {
    catalog: String,
    status: String,
    reason: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "kebab-case")]
struct ExternalCatalogBridgeStateResponse {
    catalog: String,
    external_catalog_id: Option<String>,
    external_namespace: String,
    external_table: String,
    external_table_uuid: Option<String>,
    metadata_location: Option<String>,
    external_version_token: Option<String>,
    policy_mode: String,
    credential_mode: String,
    sync_mode: String,
    rollback_strategy: String,
    last_sync_status: Option<String>,
    last_synced_metadata_location: Option<String>,
    properties: BTreeMap<String, String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "kebab-case")]
struct ExternalCatalogBridgeSyncResponse {
    action: String,
    table: RestLoadTableResponse,
    bridge: ExternalCatalogBridgeResponse,
}

#[derive(Debug, Serialize)]
struct TableBucketResponse {
    #[serde(rename = "table-bucket")]
    table_bucket: String,
    enabled: bool,
    #[serde(rename = "catalog-type")]
    catalog_type: String,
    warehouse: String,
    #[serde(rename = "warehouse-location")]
    warehouse_location: String,
    #[serde(rename = "catalog-uri")]
    catalog_uri: String,
    #[serde(rename = "compat-catalog-uri")]
    compat_catalog_uri: String,
    #[serde(rename = "credential-vending")]
    credential_vending: &'static str,
    #[serde(rename = "credential-scope")]
    credential_scope: &'static str,
    #[serde(rename = "credential-scope-prefix")]
    credential_scope_prefix: String,
    #[serde(rename = "catalog-entry-present")]
    catalog_entry_present: bool,
    properties: BTreeMap<String, String>,
}

#[derive(Debug, Serialize)]
struct RestNamespaceResponse {
    namespace: Vec<String>,
    properties: BTreeMap<String, String>,
}

#[derive(Debug, Serialize)]
struct RestListNamespacesResponse {
    namespaces: Vec<Vec<String>>,
    #[serde(rename = "next-page-token")]
    next_page_token: Option<String>,
}

#[derive(Debug, Serialize)]
struct RestTableIdentifier {
    namespace: Vec<String>,
    name: String,
}

#[derive(Debug, Serialize)]
struct RestListTablesResponse {
    identifiers: Vec<RestTableIdentifier>,
    #[serde(rename = "next-page-token")]
    next_page_token: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RestPageToken {
    version: u8,
    context: String,
    cursor: String,
}

#[derive(Debug)]
enum RestPagination {
    Unpaginated,
    Paginated {
        cursor: Option<String>,
        limit: NonZeroUsize,
        context: String,
    },
}

impl RestPagination {
    fn page_request(&self) -> Option<(Option<&str>, NonZeroUsize)> {
        match self {
            Self::Unpaginated => None,
            Self::Paginated { cursor, limit, .. } => Some((cursor.as_deref(), *limit)),
        }
    }

    fn next_page_token(&self, cursor: Option<String>) -> S3Result<Option<String>> {
        match (self, cursor) {
            (Self::Unpaginated, _) | (_, None) => Ok(None),
            (Self::Paginated { context, .. }, Some(cursor)) => encode_rest_page_token(&cursor, context).map(Some),
        }
    }
}

#[derive(Clone, Copy)]
struct RestPageContext<'a> {
    resource: &'static str,
    warehouse: &'a str,
    namespace: Option<&'a str>,
}

#[derive(Debug, Serialize)]
struct RestListViewsResponse {
    identifiers: Vec<RestTableIdentifier>,
    #[serde(rename = "next-page-token")]
    next_page_token: Option<String>,
}

#[derive(Debug, Serialize)]
struct RestLoadViewResponse {
    #[serde(rename = "metadata-location")]
    metadata_location: String,
    metadata: serde_json::Value,
    config: BTreeMap<String, String>,
}

#[derive(Debug, Serialize)]
struct RestStorageCredential {
    prefix: String,
    config: BTreeMap<String, String>,
}

#[derive(Debug, Clone)]
struct TableCredentialScope {
    scope_prefix: String,
    object_prefix: String,
}

#[derive(Debug, Clone)]
struct TableCredentialIssueRequest<'a> {
    entry: &'a crate::table_catalog::TableEntry,
    principal: Option<&'a rustfs_credentials::Credentials>,
    scope_prefix: String,
    object_prefix: String,
}

#[derive(Debug, Clone)]
struct IssuedTableCredentials {
    access_key_id: String,
    secret_access_key: String,
    session_token: String,
    expiration: OffsetDateTime,
}

#[async_trait::async_trait]
trait TableCredentialIssuer: Sync {
    fn enabled(&self) -> bool {
        true
    }

    async fn issue_table_credentials(&self, request: TableCredentialIssueRequest<'_>)
    -> S3Result<Option<IssuedTableCredentials>>;
}

#[cfg(test)]
struct DisabledTableCredentialIssuer;

#[cfg(test)]
#[async_trait::async_trait]
impl TableCredentialIssuer for DisabledTableCredentialIssuer {
    fn enabled(&self) -> bool {
        false
    }

    async fn issue_table_credentials(
        &self,
        _request: TableCredentialIssueRequest<'_>,
    ) -> S3Result<Option<IssuedTableCredentials>> {
        Ok(None)
    }
}

struct IamTableCredentialIssuer {
    enabled: bool,
    ttl_seconds: i64,
}

impl IamTableCredentialIssuer {
    fn from_env() -> Self {
        Self {
            enabled: table_credential_vending_enabled(),
            ttl_seconds: table_credential_ttl_seconds(),
        }
    }
}

#[async_trait::async_trait]
impl TableCredentialIssuer for IamTableCredentialIssuer {
    fn enabled(&self) -> bool {
        self.enabled
    }

    async fn issue_table_credentials(
        &self,
        request: TableCredentialIssueRequest<'_>,
    ) -> S3Result<Option<IssuedTableCredentials>> {
        if !self.enabled {
            return Ok(None);
        }

        let Some(principal) = request.principal else {
            return Err(s3_error!(InvalidRequest, "authentication required for table credentials"));
        };
        if principal.is_temp() || principal.is_service_account() {
            return Err(s3_error!(
                AccessDenied,
                "table credential vending does not allow chained temporary credentials"
            ));
        }

        let policy = table_credential_session_policy(request.entry, &request.object_prefix)?;
        let policy_buf = serde_json::to_vec(&policy)
            .map_err(|err| s3_error!(InternalError, "failed to serialize table credential session policy: {}", err))?;
        let expiration = OffsetDateTime::now_utc().saturating_add(Duration::seconds(self.ttl_seconds));
        let mut claims: HashMap<String, serde_json::Value> = principal.claims.clone().unwrap_or_default();
        claims.insert(
            "exp".to_string(),
            serde_json::Value::Number(serde_json::Number::from(expiration.unix_timestamp())),
        );
        claims.insert("parent".to_string(), serde_json::Value::String(principal.access_key.clone()));
        claims.insert(
            SESSION_POLICY_NAME.to_string(),
            serde_json::Value::String(base64_simd::URL_SAFE_NO_PAD.encode_to_string(&policy_buf)),
        );
        claims.insert(
            "rustfs:table-bucket".to_string(),
            serde_json::Value::String(request.entry.table_bucket.clone()),
        );
        claims.insert("rustfs:table-id".to_string(), serde_json::Value::String(request.entry.table_id.clone()));
        claims.insert(
            "rustfs:credential-scope-prefix".to_string(),
            serde_json::Value::String(request.scope_prefix.clone()),
        );

        let secret = current_token_signing_key().ok_or_else(|| s3_error!(InternalError, "token signing key not initialized"))?;
        let mut credential = get_new_credentials_with_metadata(&claims, &secret)
            .map_err(|err| s3_error!(InternalError, "failed to generate table credentials: {}", err))?;
        bind_table_credential_parent(&mut credential, principal);

        let iam_store =
            crate::admin::runtime_sources::current_ready_iam_handle().map_err(|_| s3_error!(InternalError, "iam not init"))?;
        iam_store
            .set_temp_user(&credential.access_key, &credential, None)
            .await
            .map_err(|_| s3_error!(InternalError, "failed to store table credentials"))?;

        Ok(Some(IssuedTableCredentials {
            access_key_id: credential.access_key,
            secret_access_key: credential.secret_key,
            session_token: credential.session_token,
            expiration,
        }))
    }
}

fn bind_table_credential_parent(credential: &mut rustfs_credentials::Credentials, principal: &rustfs_credentials::Credentials) {
    credential.parent_user = principal.access_key.clone();
}

#[derive(Debug, Serialize)]
struct RestLoadTableResponse {
    #[serde(rename = "metadata-location")]
    metadata_location: String,
    metadata: serde_json::Value,
    config: BTreeMap<String, String>,
    #[serde(rename = "storage-credentials")]
    storage_credentials: Vec<RestStorageCredential>,
}

#[derive(Debug, Serialize)]
struct RestLoadCredentialsResponse {
    config: BTreeMap<String, String>,
    #[serde(rename = "storage-credentials")]
    storage_credentials: Vec<RestStorageCredential>,
}

#[derive(Debug, Serialize)]
struct RestCommitTableResponse {
    #[serde(rename = "metadata-location")]
    metadata_location: String,
    metadata: serde_json::Value,
    #[serde(rename = "version-token")]
    version_token: String,
    generation: u64,
    #[serde(rename = "commit-id")]
    commit_id: String,
}

#[derive(Debug, Serialize)]
struct TableMetadataLocationResponse {
    #[serde(rename = "metadata-location")]
    metadata_location: String,
    #[serde(rename = "version-token")]
    version_token: String,
    generation: u64,
    #[serde(rename = "warehouse-location")]
    warehouse_location: String,
}

pub fn register_table_catalog_route(r: &mut S3Router<AdminOperation>) -> std::io::Result<()> {
    for prefix in [TABLE_CATALOG_PREFIX, TABLE_CATALOG_COMPAT_PREFIX] {
        register_table_catalog_prefix_routes(r, prefix)?;
    }

    Ok(())
}

fn register_table_catalog_prefix_routes(r: &mut S3Router<AdminOperation>, prefix: &str) -> std::io::Result<()> {
    r.insert(Method::GET, format!("{prefix}/config").as_str(), AdminOperation(&GET_CONFIG_HANDLER))?;
    r.insert(
        Method::PUT,
        format!("{prefix}/buckets/{{warehouse}}").as_str(),
        AdminOperation(&ENABLE_TABLE_BUCKET_HANDLER),
    )?;
    r.insert(
        Method::GET,
        format!("{prefix}/buckets/{{warehouse}}").as_str(),
        AdminOperation(&GET_TABLE_BUCKET_HANDLER),
    )?;
    r.insert(
        Method::GET,
        format!("{prefix}/{{warehouse}}/catalog/migration").as_str(),
        AdminOperation(&GET_TABLE_CATALOG_MIGRATION_HANDLER),
    )?;
    r.insert(
        Method::POST,
        format!("{prefix}/{{warehouse}}/catalog/migration").as_str(),
        AdminOperation(&MATERIALIZE_TABLE_CATALOG_MIGRATION_HANDLER),
    )?;
    r.insert(
        Method::DELETE,
        format!("{prefix}/{{warehouse}}/catalog/migration").as_str(),
        AdminOperation(&CANCEL_TABLE_CATALOG_MIGRATION_HANDLER),
    )?;
    r.insert(
        Method::GET,
        format!("{prefix}/{{warehouse}}/namespaces").as_str(),
        AdminOperation(&LIST_NAMESPACES_HANDLER),
    )?;
    r.insert(
        Method::POST,
        format!("{prefix}/{{warehouse}}/namespaces").as_str(),
        AdminOperation(&CREATE_NAMESPACE_HANDLER),
    )?;
    r.insert(
        Method::GET,
        format!("{prefix}/{{warehouse}}/namespaces/{{namespace}}").as_str(),
        AdminOperation(&GET_NAMESPACE_HANDLER),
    )?;
    r.insert(
        Method::HEAD,
        format!("{prefix}/{{warehouse}}/namespaces/{{namespace}}").as_str(),
        AdminOperation(&NAMESPACE_EXISTS_HANDLER),
    )?;
    r.insert(
        Method::DELETE,
        format!("{prefix}/{{warehouse}}/namespaces/{{namespace}}").as_str(),
        AdminOperation(&DROP_NAMESPACE_HANDLER),
    )?;
    r.insert(
        Method::GET,
        format!("{prefix}/{{warehouse}}/namespaces/{{namespace}}/tables").as_str(),
        AdminOperation(&LIST_TABLES_HANDLER),
    )?;
    r.insert(
        Method::POST,
        format!("{prefix}/{{warehouse}}/namespaces/{{namespace}}/tables").as_str(),
        AdminOperation(&CREATE_TABLE_HANDLER),
    )?;
    r.insert(
        Method::POST,
        format!("{prefix}/{{warehouse}}/namespaces/{{namespace}}/register").as_str(),
        AdminOperation(&REGISTER_TABLE_HANDLER),
    )?;
    r.insert(
        Method::GET,
        format!("{prefix}/{{warehouse}}/namespaces/{{namespace}}/views").as_str(),
        AdminOperation(&LIST_VIEWS_HANDLER),
    )?;
    r.insert(
        Method::POST,
        format!("{prefix}/{{warehouse}}/namespaces/{{namespace}}/views").as_str(),
        AdminOperation(&CREATE_VIEW_HANDLER),
    )?;
    r.insert(
        Method::GET,
        format!("{prefix}/{{warehouse}}/namespaces/{{namespace}}/tables/{{table}}").as_str(),
        AdminOperation(&LOAD_TABLE_HANDLER),
    )?;
    r.insert(
        Method::HEAD,
        format!("{prefix}/{{warehouse}}/namespaces/{{namespace}}/tables/{{table}}").as_str(),
        AdminOperation(&TABLE_EXISTS_HANDLER),
    )?;
    r.insert(
        Method::GET,
        format!("{prefix}/{{warehouse}}/namespaces/{{namespace}}/tables/{{table}}/credentials").as_str(),
        AdminOperation(&LOAD_CREDENTIALS_HANDLER),
    )?;
    r.insert(
        Method::POST,
        format!("{prefix}/{{warehouse}}/namespaces/{{namespace}}/tables/{{table}}").as_str(),
        AdminOperation(&COMMIT_TABLE_HANDLER),
    )?;
    r.insert(
        Method::DELETE,
        format!("{prefix}/{{warehouse}}/namespaces/{{namespace}}/tables/{{table}}").as_str(),
        AdminOperation(&DROP_TABLE_HANDLER),
    )?;
    r.insert(
        Method::GET,
        format!("{prefix}/{{warehouse}}/namespaces/{{namespace}}/views/{{view}}").as_str(),
        AdminOperation(&LOAD_VIEW_HANDLER),
    )?;
    r.insert(
        Method::HEAD,
        format!("{prefix}/{{warehouse}}/namespaces/{{namespace}}/views/{{view}}").as_str(),
        AdminOperation(&VIEW_EXISTS_HANDLER),
    )?;
    r.insert(
        Method::POST,
        format!("{prefix}/{{warehouse}}/namespaces/{{namespace}}/views/{{view}}").as_str(),
        AdminOperation(&REPLACE_VIEW_HANDLER),
    )?;
    r.insert(
        Method::DELETE,
        format!("{prefix}/{{warehouse}}/namespaces/{{namespace}}/views/{{view}}").as_str(),
        AdminOperation(&DROP_VIEW_HANDLER),
    )?;
    r.insert(
        Method::GET,
        format!("{prefix}/{{warehouse}}/namespaces/{{namespace}}/tables/{{table}}/refs").as_str(),
        AdminOperation(&LIST_TABLE_REFS_HANDLER),
    )?;
    r.insert(
        Method::PUT,
        format!("{prefix}/{{warehouse}}/namespaces/{{namespace}}/tables/{{table}}/refs/{{ref}}").as_str(),
        AdminOperation(&PUT_TABLE_REF_HANDLER),
    )?;
    r.insert(
        Method::DELETE,
        format!("{prefix}/{{warehouse}}/namespaces/{{namespace}}/tables/{{table}}/refs/{{ref}}").as_str(),
        AdminOperation(&DELETE_TABLE_REF_HANDLER),
    )?;
    r.insert(
        Method::GET,
        format!("{prefix}/{{warehouse}}/namespaces/{{namespace}}/tables/{{table}}/metadata-location").as_str(),
        AdminOperation(&GET_TABLE_METADATA_LOCATION_HANDLER),
    )?;
    r.insert(
        Method::PUT,
        format!("{prefix}/{{warehouse}}/namespaces/{{namespace}}/tables/{{table}}/metadata-location").as_str(),
        AdminOperation(&UPDATE_TABLE_METADATA_LOCATION_HANDLER),
    )?;
    r.insert(
        Method::POST,
        format!("{prefix}/{{warehouse}}/namespaces/{{namespace}}/tables/{{table}}/maintenance/metadata").as_str(),
        AdminOperation(&TABLE_METADATA_MAINTENANCE_HANDLER),
    )?;
    r.insert(
        Method::GET,
        format!("{prefix}/{{warehouse}}/namespaces/{{namespace}}/tables/{{table}}/maintenance/config").as_str(),
        AdminOperation(&GET_TABLE_MAINTENANCE_CONFIG_HANDLER),
    )?;
    r.insert(
        Method::PUT,
        format!("{prefix}/{{warehouse}}/namespaces/{{namespace}}/tables/{{table}}/maintenance/config").as_str(),
        AdminOperation(&PUT_TABLE_MAINTENANCE_CONFIG_HANDLER),
    )?;
    r.insert(
        Method::GET,
        format!("{prefix}/{{warehouse}}/namespaces/{{namespace}}/tables/{{table}}/maintenance/jobs/{{job}}").as_str(),
        AdminOperation(&GET_TABLE_MAINTENANCE_JOB_HANDLER),
    )?;
    r.insert(
        Method::GET,
        format!("{prefix}/{{warehouse}}/namespaces/{{namespace}}/tables/{{table}}/maintenance/scheduler").as_str(),
        AdminOperation(&GET_TABLE_MAINTENANCE_SCHEDULER_HANDLER),
    )?;
    r.insert(
        Method::POST,
        format!("{prefix}/{{warehouse}}/namespaces/{{namespace}}/tables/{{table}}/maintenance/scheduler/run").as_str(),
        AdminOperation(&RUN_TABLE_MAINTENANCE_SCHEDULER_HANDLER),
    )?;
    r.insert(
        Method::POST,
        format!("{prefix}/{{warehouse}}/namespaces/{{namespace}}/tables/{{table}}/maintenance/worker/run").as_str(),
        AdminOperation(&RUN_TABLE_MAINTENANCE_WORKER_HANDLER),
    )?;
    r.insert(
        Method::POST,
        format!("{prefix}/{{warehouse}}/namespaces/{{namespace}}/tables/{{table}}/maintenance/jobs/{{job}}/heartbeat").as_str(),
        AdminOperation(&HEARTBEAT_TABLE_MAINTENANCE_JOB_HANDLER),
    )?;
    r.insert(
        Method::POST,
        format!("{prefix}/{{warehouse}}/namespaces/{{namespace}}/tables/{{table}}/maintenance/jobs/{{job}}/quarantine").as_str(),
        AdminOperation(&TABLE_MAINTENANCE_QUARANTINE_HANDLER),
    )?;
    r.insert(
        Method::GET,
        format!("{prefix}/{{warehouse}}/namespaces/{{namespace}}/tables/{{table}}/catalog/export").as_str(),
        AdminOperation(&EXPORT_TABLE_CATALOG_HANDLER),
    )?;
    r.insert(
        Method::POST,
        format!("{prefix}/{{warehouse}}/namespaces/{{namespace}}/tables/{{table}}/catalog/import").as_str(),
        AdminOperation(&IMPORT_TABLE_CATALOG_HANDLER),
    )?;
    r.insert(
        Method::GET,
        format!("{prefix}/{{warehouse}}/namespaces/{{namespace}}/tables/{{table}}/catalog/external").as_str(),
        AdminOperation(&EXTERNAL_CATALOG_BRIDGE_HANDLER),
    )?;
    r.insert(
        Method::PUT,
        format!("{prefix}/{{warehouse}}/namespaces/{{namespace}}/tables/{{table}}/catalog/external").as_str(),
        AdminOperation(&PUT_EXTERNAL_CATALOG_BRIDGE_HANDLER),
    )?;
    r.insert(
        Method::POST,
        format!("{prefix}/{{warehouse}}/namespaces/{{namespace}}/tables/{{table}}/catalog/external/sync").as_str(),
        AdminOperation(&SYNC_EXTERNAL_CATALOG_BRIDGE_HANDLER),
    )?;
    r.insert(
        Method::GET,
        format!("{prefix}/{{warehouse}}/namespaces/{{namespace}}/tables/{{table}}/catalog/diagnostics").as_str(),
        AdminOperation(&GET_TABLE_CATALOG_DIAGNOSTICS_HANDLER),
    )?;
    r.insert(
        Method::POST,
        format!("{prefix}/{{warehouse}}/namespaces/{{namespace}}/tables/{{table}}/catalog/recovery").as_str(),
        AdminOperation(&RECOVER_TABLE_CATALOG_HANDLER),
    )?;
    r.insert(
        Method::POST,
        format!("{prefix}/{{warehouse}}/namespaces/{{namespace}}/tables/{{table}}/catalog/rollback").as_str(),
        AdminOperation(&ROLLBACK_TABLE_CATALOG_HANDLER),
    )?;

    Ok(())
}

fn catalog_config_response(warehouse: Option<&str>) -> S3Result<CatalogConfigResponse> {
    let usecase = default_admin_usecase();
    let backing_mode = crate::table_catalog::TableCatalogBackingMode::from_env().map_err(catalog_store_error)?;
    let mut overrides = BTreeMap::new();
    if backing_mode != crate::table_catalog::TableCatalogBackingMode::ObjectBacked {
        overrides.insert(CATALOG_BACKING_CONFIG_KEY.to_string(), backing_mode.as_str().to_string());
    }
    let mut defaults = BTreeMap::from([
        (WAREHOUSE_PROPERTY.to_string(), DEFAULT_WAREHOUSE_ID.to_string()),
        (CATALOG_ENDPOINT_PREFIX_CONFIG_KEY.to_string(), TABLE_CATALOG_PREFIX.to_string()),
        (
            CATALOG_COMPAT_ENDPOINT_PREFIX_CONFIG_KEY.to_string(),
            TABLE_CATALOG_COMPAT_PREFIX.to_string(),
        ),
        (
            CATALOG_BACKING_CONFIG_KEY.to_string(),
            crate::table_catalog::TABLE_CATALOG_BACKING_OBJECT.to_string(),
        ),
    ]);
    if let Some(warehouse) = warehouse {
        defaults.insert(PREFIX_PROPERTY.to_string(), warehouse.to_string());
    }
    Ok(CatalogConfigResponse {
        defaults,
        overrides,
        endpoints: TABLE_CATALOG_ENDPOINTS.to_vec(),
        admin_discovery: CatalogAdminDiscovery {
            runtime_capabilities: usecase.runtime_capabilities_route(),
            cluster_snapshot: usecase.cluster_snapshot_route(),
            extensions_catalog: usecase.extensions_catalog_route(),
        },
    })
}

fn build_json_response<T: Serialize>(status: StatusCode, body: &T) -> S3Result<S3Response<(StatusCode, Body)>> {
    let data = serde_json::to_vec(body).map_err(|e| s3_error!(InternalError, "failed to serialize response: {}", e))?;
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static(JSON_CONTENT_TYPE));
    Ok(S3Response::with_headers((status, Body::from(data)), headers))
}

fn empty_response(status: StatusCode) -> S3Response<(StatusCode, Body)> {
    S3Response::new((status, Body::default()))
}

fn duration_millis_u64(duration: StdDuration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn table_catalog_admin_operation_result_label<T, E>(result: &Result<T, E>) -> &'static str {
    if result.is_ok() { "success" } else { "failure" }
}

fn record_table_catalog_admin_operation_result<T, E>(
    operation: &str,
    warehouse: &str,
    namespace: &str,
    table: &str,
    started: Instant,
    result: &Result<T, E>,
) {
    let elapsed = started.elapsed();
    let result_label = table_catalog_admin_operation_result_label(result);
    counter!(
        "rustfs_table_catalog_admin_operations_total",
        "operation" => operation.to_string(),
        "result" => result_label.to_string()
    )
    .increment(1);
    histogram!(
        "rustfs_table_catalog_admin_operation_duration_seconds",
        "operation" => operation.to_string(),
        "result" => result_label.to_string()
    )
    .record(elapsed.as_secs_f64());

    if result.is_err() {
        tracing::warn!(
            operation,
            warehouse,
            namespace,
            table,
            result = result_label,
            duration_ms = duration_millis_u64(elapsed),
            "table catalog admin operation failed"
        );
    } else if elapsed >= TABLE_CATALOG_ADMIN_OPERATION_SLOW_LOG_THRESHOLD {
        tracing::warn!(
            operation,
            warehouse,
            namespace,
            table,
            duration_ms = duration_millis_u64(elapsed),
            "slow table catalog admin operation"
        );
    }
}

fn exists_status(exists: bool) -> StatusCode {
    if exists {
        StatusCode::NO_CONTENT
    } else {
        StatusCode::NOT_FOUND
    }
}

async fn authorize_table_catalog_request(req: &S3Request<Body>, action: AdminAction) -> S3Result<()> {
    let Some(input_cred) = &req.credentials else {
        return Err(s3_error!(InvalidRequest, "authentication required"));
    };

    let (cred, owner) =
        check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;

    validate_admin_request(
        &req.headers,
        &cred,
        owner,
        false,
        vec![Action::AdminAction(action)],
        req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0)),
    )
    .await
}

#[derive(Debug, Clone)]
struct TableCatalogResource<'a> {
    warehouse: &'a str,
    namespace: Option<String>,
    table: Option<String>,
    view: Option<String>,
}

impl<'a> TableCatalogResource<'a> {
    fn warehouse(warehouse: &'a str) -> Self {
        Self {
            warehouse,
            namespace: None,
            table: None,
            view: None,
        }
    }

    fn namespace(warehouse: &'a str, namespace: &crate::table_catalog::Namespace) -> Self {
        Self {
            warehouse,
            namespace: Some(namespace.storage_id()),
            table: None,
            view: None,
        }
    }

    fn table(warehouse: &'a str, namespace: &crate::table_catalog::Namespace, table: &str) -> Self {
        Self {
            warehouse,
            namespace: Some(namespace.storage_id()),
            table: Some(table.to_string()),
            view: None,
        }
    }

    fn view(warehouse: &'a str, namespace: &crate::table_catalog::Namespace, view: &str) -> Self {
        Self {
            warehouse,
            namespace: Some(namespace.storage_id()),
            table: None,
            view: Some(view.to_string()),
        }
    }

    fn object_path(&self) -> Option<String> {
        match (&self.namespace, &self.table, &self.view) {
            (Some(namespace), Some(table), None) => Some(format!(
                "{TABLE_CATALOG_NAMESPACE_RESOURCE_ROOT}/{namespace}/{TABLE_CATALOG_TABLE_RESOURCE_ROOT}/{table}"
            )),
            (Some(namespace), None, Some(view)) => Some(format!(
                "{TABLE_CATALOG_NAMESPACE_RESOURCE_ROOT}/{namespace}/{TABLE_CATALOG_VIEW_RESOURCE_ROOT}/{view}"
            )),
            (Some(namespace), None, None) => Some(format!("{TABLE_CATALOG_NAMESPACE_RESOURCE_ROOT}/{namespace}")),
            _ => None,
        }
    }
}

async fn authorize_table_catalog_resource_request(
    req: &S3Request<Body>,
    resource: &TableCatalogResource<'_>,
    action: AdminAction,
) -> S3Result<()> {
    let Some(input_cred) = &req.credentials else {
        return Err(s3_error!(InvalidRequest, "authentication required"));
    };

    let (cred, owner) =
        check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;

    let object_path = resource.object_path();
    validate_admin_request_with_bucket_object(
        &req.headers,
        &cred,
        owner,
        false,
        vec![Action::AdminAction(action)],
        req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0)),
        AdminResourceScope::bucket_object(resource.warehouse, object_path.as_deref().unwrap_or("")),
    )
    .await
}

async fn table_catalog_request_principal(req: &S3Request<Body>) -> S3Result<rustfs_credentials::Credentials> {
    let Some(input_cred) = &req.credentials else {
        return Err(s3_error!(InvalidRequest, "authentication required"));
    };
    let (cred, _owner) =
        check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;
    Ok(cred)
}

async fn read_json_body<T: DeserializeOwned>(mut input: Body) -> S3Result<T> {
    let body = input
        .store_all_limited(MAX_ADMIN_REQUEST_BODY_SIZE)
        .await
        .map_err(|err| s3_error!(InvalidRequest, "failed to read request body: {}", err))?;
    if body.is_empty() {
        return Err(s3_error!(InvalidRequest, "request body is required"));
    }
    serde_json::from_slice(&body).map_err(|err| s3_error!(InvalidRequest, "invalid JSON: {}", err))
}

async fn read_json_body_or_default<T>(mut input: Body) -> S3Result<T>
where
    T: Default + DeserializeOwned,
{
    let body = input
        .store_all_limited(MAX_ADMIN_REQUEST_BODY_SIZE)
        .await
        .map_err(|err| s3_error!(InvalidRequest, "failed to read request body: {}", err))?;
    if body.is_empty() {
        return Ok(T::default());
    }
    serde_json::from_slice(&body).map_err(|err| s3_error!(InvalidRequest, "invalid JSON: {}", err))
}

fn warehouse_from_params(params: &Params<'_, '_>) -> S3Result<String> {
    let warehouse = params.get("warehouse").unwrap_or("");
    if warehouse.is_empty() {
        return Err(s3_error!(InvalidRequest, "warehouse is required"));
    }
    Ok(warehouse.to_string())
}

fn warehouse_from_config_query(uri: &http::Uri) -> S3Result<Option<String>> {
    let Some(query) = uri.query() else {
        return Ok(None);
    };
    let mut warehouse = None;
    for (key, value) in url::form_urlencoded::parse(query.as_bytes()) {
        if key != WAREHOUSE_PROPERTY {
            continue;
        }
        if warehouse.is_some() {
            return Err(s3_error!(InvalidRequest, "warehouse query parameter must not be repeated"));
        }
        if value.is_empty() {
            return Err(s3_error!(InvalidRequest, "warehouse query parameter must not be empty"));
        }
        warehouse = Some(value.into_owned());
    }
    Ok(warehouse)
}

fn rest_pagination_from_query(uri: &http::Uri, context: RestPageContext<'_>) -> S3Result<RestPagination> {
    let mut page_token = None;
    let mut page_token_seen = false;
    let mut page_size = None;

    if let Some(query) = uri.query() {
        for (key, value) in url::form_urlencoded::parse(query.as_bytes()) {
            match key.as_ref() {
                REST_PAGE_TOKEN_QUERY_PARAMETER => {
                    if page_token_seen {
                        return Err(iceberg_rest_error(
                            ICEBERG_ERROR_BAD_REQUEST,
                            StatusCode::BAD_REQUEST,
                            "pageToken query parameter must not be repeated",
                        ));
                    }
                    page_token_seen = true;
                    page_token = Some(value.into_owned());
                }
                REST_PAGE_SIZE_QUERY_PARAMETER => {
                    if page_size.is_some() {
                        return Err(iceberg_rest_error(
                            ICEBERG_ERROR_BAD_REQUEST,
                            StatusCode::BAD_REQUEST,
                            "pageSize query parameter must not be repeated",
                        ));
                    }
                    let value = value.parse::<usize>().map_err(|_| {
                        iceberg_rest_error(
                            ICEBERG_ERROR_BAD_REQUEST,
                            StatusCode::BAD_REQUEST,
                            "pageSize query parameter must be a positive integer",
                        )
                    })?;
                    if value == 0 {
                        return Err(iceberg_rest_error(
                            ICEBERG_ERROR_BAD_REQUEST,
                            StatusCode::BAD_REQUEST,
                            "pageSize query parameter must be greater than zero",
                        ));
                    }
                    page_size = Some(value.min(REST_MAX_PAGE_SIZE));
                }
                _ => {}
            }
        }
    }

    if !page_token_seen && page_size.is_none() {
        return Ok(RestPagination::Unpaginated);
    }

    let context = rest_page_context_fingerprint(context);
    let cursor = match page_token.as_deref() {
        None | Some("") => None,
        Some(encoded) => Some(decode_rest_page_token(encoded, &context)?),
    };
    let limit = NonZeroUsize::new(page_size.unwrap_or(REST_DEFAULT_PAGE_SIZE)).ok_or_else(|| {
        iceberg_rest_error(
            ICEBERG_ERROR_REST,
            StatusCode::INTERNAL_SERVER_ERROR,
            "REST page size must be greater than zero",
        )
    })?;
    Ok(RestPagination::Paginated { cursor, limit, context })
}

fn rest_page_context_fingerprint(context: RestPageContext<'_>) -> String {
    let mut data =
        Vec::with_capacity(context.resource.len() + context.warehouse.len() + context.namespace.map_or(0, str::len) + 2);
    data.extend_from_slice(context.resource.as_bytes());
    data.push(0);
    data.extend_from_slice(context.warehouse.as_bytes());
    data.push(0);
    if let Some(namespace) = context.namespace {
        data.extend_from_slice(namespace.as_bytes());
    }
    hex_sha256(&data, str::to_string)
}

fn decode_rest_page_token(encoded: &str, expected_context: &str) -> S3Result<String> {
    if encoded.len() > REST_PAGE_TOKEN_MAX_LENGTH {
        return Err(iceberg_rest_error(
            ICEBERG_ERROR_BAD_REQUEST,
            StatusCode::BAD_REQUEST,
            "pageToken query parameter is too large",
        ));
    }
    let data = base64_decode_url_safe_no_pad(encoded.as_bytes()).map_err(|_| {
        iceberg_rest_error(
            ICEBERG_ERROR_BAD_REQUEST,
            StatusCode::BAD_REQUEST,
            "pageToken query parameter is malformed",
        )
    })?;
    let token = serde_json::from_slice::<RestPageToken>(&data).map_err(|_| {
        iceberg_rest_error(
            ICEBERG_ERROR_BAD_REQUEST,
            StatusCode::BAD_REQUEST,
            "pageToken query parameter is malformed",
        )
    })?;
    if token.version != REST_PAGE_TOKEN_VERSION || token.context != expected_context || token.cursor.is_empty() {
        return Err(iceberg_rest_error(
            ICEBERG_ERROR_BAD_REQUEST,
            StatusCode::BAD_REQUEST,
            "pageToken query parameter does not match this list operation",
        ));
    }
    Ok(token.cursor)
}

fn encode_rest_page_token(cursor: &str, context: &str) -> S3Result<String> {
    let token = RestPageToken {
        version: REST_PAGE_TOKEN_VERSION,
        context: context.to_string(),
        cursor: cursor.to_string(),
    };
    let data = serde_json::to_vec(&token).map_err(|err| {
        iceberg_rest_error(
            ICEBERG_ERROR_REST,
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to serialize REST page token: {err}"),
        )
    })?;
    let encoded = base64_encode_url_safe_no_pad(&data);
    if encoded.len() > REST_PAGE_TOKEN_MAX_LENGTH {
        return Err(iceberg_rest_error(
            ICEBERG_ERROR_REST,
            StatusCode::INTERNAL_SERVER_ERROR,
            "REST page token exceeds the supported size",
        ));
    }
    Ok(encoded)
}

fn namespace_from_params(params: &Params<'_, '_>) -> S3Result<crate::table_catalog::Namespace> {
    let namespace = params.get("namespace").unwrap_or("");
    crate::table_catalog::Namespace::parse(namespace).map_err(|err| s3_error!(InvalidRequest, "invalid namespace: {}", err))
}

fn table_name_from_params(params: &Params<'_, '_>) -> S3Result<String> {
    let table = params.get("table").unwrap_or("");
    crate::table_catalog::IdentifierSegment::parse(table.to_string())
        .map_err(|err| s3_error!(InvalidRequest, "invalid table name: {}", err))?;
    Ok(table.to_string())
}

fn view_name_from_params(params: &Params<'_, '_>) -> S3Result<String> {
    let view = params.get("view").unwrap_or("");
    crate::table_catalog::IdentifierSegment::parse(view.to_string())
        .map_err(|err| s3_error!(InvalidRequest, "invalid view name: {}", err))?;
    Ok(view.to_string())
}

fn ref_name_from_params(params: &Params<'_, '_>) -> S3Result<String> {
    let ref_name = params.get("ref").unwrap_or("");
    crate::table_catalog::IdentifierSegment::parse(ref_name.to_string())
        .map_err(|err| s3_error!(InvalidRequest, "invalid ref name: {}", err))?;
    Ok(ref_name.to_string())
}

fn job_id_from_params(params: &Params<'_, '_>) -> S3Result<String> {
    let job = params.get("job").unwrap_or("");
    if job.is_empty() {
        return Err(s3_error!(InvalidRequest, "maintenance job id is required"));
    }
    Ok(job.to_string())
}

fn table_catalog_backend() -> S3Result<crate::table_catalog::EcStoreTableCatalogObjectBackend<ECStore>> {
    let store = current_object_store_handle().ok_or_else(|| s3_error!(InternalError, "object store not initialized"))?;
    Ok(crate::table_catalog::EcStoreTableCatalogObjectBackend::new(store))
}

type EcStoreObjectTableCatalogStore =
    crate::table_catalog::ObjectTableCatalogStore<crate::table_catalog::EcStoreTableCatalogObjectBackend<ECStore>>;

fn table_catalog_store_from_backend(
    backend: crate::table_catalog::EcStoreTableCatalogObjectBackend<ECStore>,
) -> S3Result<crate::table_catalog::EcStoreTableCatalogStore<ECStore>> {
    crate::table_catalog::ConfiguredTableCatalogStore::from_env(backend).map_err(catalog_store_error)
}

fn table_catalog_store() -> S3Result<crate::table_catalog::EcStoreTableCatalogStore<ECStore>> {
    let backend = table_catalog_backend()?;
    table_catalog_store_from_backend(backend)
}

fn table_catalog_object_store() -> S3Result<EcStoreObjectTableCatalogStore> {
    match crate::table_catalog::TableCatalogBackingMode::from_env().map_err(catalog_store_error)? {
        crate::table_catalog::TableCatalogBackingMode::ObjectBacked => {
            let backend = table_catalog_backend()?;
            Ok(crate::table_catalog::ObjectTableCatalogStore::new(backend))
        }
        crate::table_catalog::TableCatalogBackingMode::DurableStrong => Err(s3_error!(
            InvalidRequest,
            "operation is not supported with {} table catalog backing",
            crate::table_catalog::TABLE_CATALOG_BACKING_DURABLE_STRONG
        )),
    }
}

async fn table_bucket_enabled_from_metadata(bucket: &str) -> S3Result<bool> {
    let metadata = metadata_sys::get(bucket)
        .await
        .map_err(|err| s3_error!(InvalidRequest, "failed to load table bucket metadata for {bucket}: {}", err))?;
    Ok(metadata.table_bucket_enabled())
}

async fn ensure_table_bucket_enabled(bucket: &str) -> S3Result<()> {
    if table_bucket_enabled_from_metadata(bucket).await? {
        return Ok(());
    }
    Err(s3_error!(InvalidRequest, "bucket {bucket} is not table-enabled"))
}

fn table_bucket_entry_from_metadata_marker(bucket: &str) -> crate::table_catalog::TableBucketEntry {
    crate::table_catalog::TableBucketEntry {
        version: crate::table_catalog::TABLE_CATALOG_ENTRY_VERSION,
        table_bucket: bucket.to_string(),
        catalog_type: crate::table_catalog::TABLE_BUCKET_CATALOG_TYPE.to_string(),
        warehouse_root: format!("s3://{bucket}/"),
        state: crate::table_catalog::TableCatalogEntryState::Active,
        properties: BTreeMap::new(),
        created_at: None,
        updated_at: None,
    }
}

async fn enable_table_bucket_marker(bucket: &str) -> S3Result<()> {
    let marker = crate::table_catalog::table_bucket_marker_json()
        .map_err(|err| s3_error!(InternalError, "failed to serialize table bucket marker: {}", err))?;
    metadata_sys::update(bucket, crate::table_catalog::TABLE_BUCKET_MARKER_CONFIG, marker)
        .await
        .map(|_| ())
        .map_err(|err| s3_error!(InvalidRequest, "failed to enable table bucket {bucket}: {}", err))
}

async fn ensure_table_bucket_entry<S>(store: &S, bucket: &str, table_bucket_enabled: bool) -> S3Result<()>
where
    S: crate::table_catalog::TableCatalogStore + ?Sized,
{
    if !table_bucket_enabled {
        return Err(s3_error!(InvalidRequest, "bucket {bucket} is not table-enabled"));
    }
    if store.get_table_bucket(bucket).await.map_err(catalog_store_error)?.is_some() {
        return Ok(());
    }
    store
        .put_table_bucket(table_bucket_entry_from_metadata_marker(bucket))
        .await
        .map_err(catalog_store_error)
}

async fn table_bucket_response<S>(store: &S, bucket: &str, enabled: bool) -> S3Result<TableBucketResponse>
where
    S: crate::table_catalog::TableCatalogStore + ?Sized,
{
    let entry = store.get_table_bucket(bucket).await.map_err(catalog_store_error)?;
    let (catalog_type, warehouse_location, properties, catalog_entry_present) = match entry {
        Some(entry) => (entry.catalog_type, entry.warehouse_root, entry.properties, true),
        None => (
            crate::table_catalog::TABLE_BUCKET_CATALOG_TYPE.to_string(),
            format!("s3://{bucket}/"),
            BTreeMap::new(),
            false,
        ),
    };

    Ok(TableBucketResponse {
        table_bucket: bucket.to_string(),
        enabled,
        catalog_type,
        warehouse: bucket.to_string(),
        warehouse_location: warehouse_location.clone(),
        catalog_uri: format!("{TABLE_CATALOG_PREFIX}/{bucket}"),
        compat_catalog_uri: format!("{TABLE_CATALOG_COMPAT_PREFIX}/{bucket}"),
        credential_vending: CREDENTIAL_VENDING_UNSUPPORTED,
        credential_scope: CREDENTIAL_SCOPE_WAREHOUSE_PREFIX,
        credential_scope_prefix: warehouse_location,
        catalog_entry_present,
        properties,
    })
}

async fn enable_table_bucket_response<S>(store: &S, bucket: &str) -> S3Result<TableBucketResponse>
where
    S: crate::table_catalog::TableCatalogStore + ?Sized,
{
    enable_table_bucket_marker(bucket).await?;
    ensure_table_bucket_entry(store, bucket, true).await?;
    table_bucket_response(store, bucket, true).await
}

fn namespace_segments(namespace: &crate::table_catalog::Namespace) -> Vec<String> {
    namespace
        .segments()
        .iter()
        .map(|segment| segment.as_str().to_string())
        .collect()
}

fn namespace_from_segments(segments: &[String]) -> S3Result<crate::table_catalog::Namespace> {
    if segments.is_empty() {
        return Err(s3_error!(InvalidRequest, "namespace cannot be empty"));
    }

    let namespace = segments.join(".");
    crate::table_catalog::Namespace::parse(&namespace).map_err(|err| s3_error!(InvalidRequest, "invalid namespace: {}", err))
}

fn namespace_response_from_entry(entry: crate::table_catalog::NamespaceEntry) -> S3Result<RestNamespaceResponse> {
    let namespace = crate::table_catalog::Namespace::parse(&entry.namespace)
        .map_err(|err| s3_error!(InternalError, "persisted namespace entry is invalid: {}", err))?;
    Ok(RestNamespaceResponse {
        namespace: namespace_segments(&namespace),
        properties: entry.properties,
    })
}

fn list_namespaces_response_from_entries(
    entries: Vec<crate::table_catalog::NamespaceEntry>,
    next_page_token: Option<String>,
) -> S3Result<RestListNamespacesResponse> {
    let namespaces = entries
        .into_iter()
        .map(|entry| {
            let namespace = crate::table_catalog::Namespace::parse(&entry.namespace)
                .map_err(|err| s3_error!(InternalError, "persisted namespace entry is invalid: {}", err))?;
            Ok(namespace_segments(&namespace))
        })
        .collect::<S3Result<Vec<_>>>()?;
    Ok(RestListNamespacesResponse {
        namespaces,
        next_page_token,
    })
}

fn list_tables_response_from_entries(
    entries: Vec<crate::table_catalog::TableEntry>,
    next_page_token: Option<String>,
) -> S3Result<RestListTablesResponse> {
    let identifiers = entries
        .into_iter()
        .map(|entry| {
            let namespace = crate::table_catalog::Namespace::parse(&entry.namespace)
                .map_err(|err| s3_error!(InternalError, "persisted table entry namespace is invalid: {}", err))?;
            Ok(RestTableIdentifier {
                namespace: namespace_segments(&namespace),
                name: entry.table,
            })
        })
        .collect::<S3Result<Vec<_>>>()?;
    Ok(RestListTablesResponse {
        identifiers,
        next_page_token,
    })
}

fn list_views_response_from_entries(
    entries: Vec<crate::table_catalog::ViewEntry>,
    next_page_token: Option<String>,
) -> S3Result<RestListViewsResponse> {
    let identifiers = entries
        .into_iter()
        .map(|entry| {
            let namespace = crate::table_catalog::Namespace::parse(&entry.namespace)
                .map_err(|err| s3_error!(InternalError, "persisted view entry namespace is invalid: {}", err))?;
            Ok(RestTableIdentifier {
                namespace: namespace_segments(&namespace),
                name: entry.view,
            })
        })
        .collect::<S3Result<Vec<_>>>()?;
    Ok(RestListViewsResponse {
        identifiers,
        next_page_token,
    })
}

fn table_credential_vending_enabled() -> bool {
    std::env::var(ENV_TABLE_CATALOG_CREDENTIAL_VENDING)
        .ok()
        .map(|value| matches!(value.to_ascii_lowercase().as_str(), "1" | "true" | "on" | "enabled"))
        .unwrap_or(false)
}

fn table_credential_ttl_seconds() -> i64 {
    std::env::var(ENV_TABLE_CATALOG_CREDENTIAL_TTL_SECONDS)
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
        .map(|seconds| seconds.clamp(MIN_TABLE_CATALOG_CREDENTIAL_TTL_SECONDS, MAX_TABLE_CATALOG_CREDENTIAL_TTL_SECONDS))
        .unwrap_or(DEFAULT_TABLE_CATALOG_CREDENTIAL_TTL_SECONDS)
}

fn table_credential_scope(entry: &crate::table_catalog::TableEntry) -> S3Result<TableCredentialScope> {
    let location = entry
        .warehouse_location
        .strip_prefix("s3://")
        .ok_or_else(|| s3_error!(InvalidRequest, "table warehouse location must be an s3 URI"))?;
    let (bucket, object_prefix) = location
        .split_once('/')
        .ok_or_else(|| s3_error!(InvalidRequest, "table warehouse location must include an object prefix"))?;
    if bucket != entry.table_bucket {
        return Err(s3_error!(InvalidRequest, "table warehouse location must be inside the table bucket"));
    }
    let object_prefix = normalize_table_credential_object_prefix(object_prefix)?;
    Ok(TableCredentialScope {
        scope_prefix: format!("s3://{bucket}/{object_prefix}"),
        object_prefix,
    })
}

fn normalize_table_credential_object_prefix(object_prefix: &str) -> S3Result<String> {
    let object_prefix = object_prefix.strip_suffix('/').unwrap_or(object_prefix);
    if object_prefix.is_empty() {
        return Err(s3_error!(InvalidRequest, "table credential scope prefix is empty"));
    }
    if object_prefix.contains('\\') {
        return Err(s3_error!(
            InvalidRequest,
            "table credential scope prefix contains an invalid path separator"
        ));
    }
    if object_prefix
        .split('/')
        .any(|segment| segment.is_empty() || segment == "." || segment == "..")
    {
        return Err(s3_error!(
            InvalidRequest,
            "table credential scope prefix contains an invalid path segment"
        ));
    }

    let mut normalized = object_prefix.to_string();
    normalized.push('/');
    Ok(normalized)
}

fn table_credential_catalog_resource(entry: &crate::table_catalog::TableEntry) -> S3Result<String> {
    let namespace = crate::table_catalog::Namespace::parse(&entry.namespace)
        .map_err(|err| s3_error!(InvalidRequest, "invalid table credential namespace: {}", err))?;
    let table = crate::table_catalog::IdentifierSegment::parse(&entry.table)
        .map_err(|err| s3_error!(InvalidRequest, "invalid table credential table name: {}", err))?;
    Ok(format!("namespaces/{}/tables/{}", namespace.storage_id(), table.as_str()))
}

fn table_credential_session_policy(entry: &crate::table_catalog::TableEntry, object_prefix: &str) -> S3Result<Policy> {
    let bucket = &entry.table_bucket;
    let object_prefix = normalize_table_credential_object_prefix(object_prefix)?;
    let catalog_resource = table_credential_catalog_resource(entry)?;
    let policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject",
                    "s3:PutObject",
                    "s3:DeleteObject",
                    "s3:AbortMultipartUpload",
                    "s3:ListMultipartUploadParts"
                ],
                "Resource": [
                    format!("arn:aws:s3:::{bucket}/{object_prefix}*")
                ]
            },
            {
                "Effect": "Allow",
                "Action": [
                    "s3:GetBucketLocation"
                ],
                "Resource": [
                    format!("arn:aws:s3:::{bucket}")
                ]
            },
            {
                "Effect": "Allow",
                "Action": [
                    "admin:GetTableMetadata",
                    "admin:SetTableMetadata"
                ],
                "Resource": [
                    format!("arn:aws:s3:::{bucket}/{catalog_resource}")
                ]
            }
        ]
    });
    let data = serde_json::to_vec(&policy)
        .map_err(|err| s3_error!(InternalError, "failed to serialize table credential policy: {}", err))?;
    Policy::parse_config(&data).map_err(|err| s3_error!(InvalidRequest, "invalid table credential policy: {}", err))
}

fn storage_credential_from_issued(scope: TableCredentialScope, issued: IssuedTableCredentials) -> RestStorageCredential {
    let mut config = BTreeMap::new();
    config.insert(S3_ACCESS_KEY_ID_CONFIG_KEY.to_string(), issued.access_key_id);
    config.insert(S3_SECRET_ACCESS_KEY_CONFIG_KEY.to_string(), issued.secret_access_key);
    config.insert(S3_SESSION_TOKEN_CONFIG_KEY.to_string(), issued.session_token);
    config.insert(CREDENTIAL_VENDING_CONFIG_KEY.to_string(), CREDENTIAL_VENDING_SUPPORTED.to_string());
    config.insert(CREDENTIAL_MODE_CONFIG_KEY.to_string(), CREDENTIAL_MODE_CATALOG_VENDED.to_string());
    config.insert(CREDENTIAL_SCOPE_CONFIG_KEY.to_string(), CREDENTIAL_SCOPE_TABLE_PREFIX.to_string());
    config.insert(CREDENTIAL_SCOPE_PREFIX_CONFIG_KEY.to_string(), scope.scope_prefix.clone());
    config.insert(
        CREDENTIAL_EXPIRATION_CONFIG_KEY.to_string(),
        issued.expiration.unix_timestamp().to_string(),
    );
    RestStorageCredential {
        prefix: scope.scope_prefix,
        config,
    }
}

fn table_metadata_location_for_client(table_bucket: &str, metadata_location: &str) -> String {
    if crate::table_catalog::is_reserved_table_object_key(metadata_location) {
        format!("s3://{table_bucket}/{metadata_location}")
    } else {
        metadata_location.to_string()
    }
}

fn table_metadata_location_for_catalog(table_bucket: &str, metadata_location: &str) -> S3Result<String> {
    crate::table_catalog::table_catalog_object_key_from_location(table_bucket, metadata_location)
        .ok_or_else(|| s3_error!(InvalidRequest, "metadata location must be inside the table bucket"))
}

fn table_metadata_for_client(table_bucket: &str, mut metadata: serde_json::Value) -> serde_json::Value {
    if let Some(metadata_log) = metadata.get_mut("metadata-log").and_then(serde_json::Value::as_array_mut) {
        for entry in metadata_log {
            let Some(metadata_file) = entry.get_mut("metadata-file") else {
                continue;
            };
            let Some(metadata_location) = metadata_file.as_str() else {
                continue;
            };
            if crate::table_catalog::is_reserved_table_object_key(metadata_location) {
                *metadata_file = serde_json::Value::String(table_metadata_location_for_client(table_bucket, metadata_location));
            }
        }
    }

    metadata
}

fn load_table_response_from_entry(entry: crate::table_catalog::TableEntry, metadata: serde_json::Value) -> RestLoadTableResponse {
    let mut config = BTreeMap::new();
    let warehouse_location = entry.warehouse_location.clone();
    let metadata_location = table_metadata_location_for_client(&entry.table_bucket, &entry.metadata_location);
    let metadata = table_metadata_for_client(&entry.table_bucket, metadata);
    config.insert("warehouse-location".to_string(), warehouse_location.clone());
    config.insert(CREDENTIAL_VENDING_CONFIG_KEY.to_string(), CREDENTIAL_VENDING_UNSUPPORTED.to_string());
    config.insert(
        CREDENTIAL_VENDING_REASON_CONFIG_KEY.to_string(),
        CREDENTIAL_VENDING_UNSUPPORTED_REASON.to_string(),
    );
    config.insert(CREDENTIAL_SCOPE_CONFIG_KEY.to_string(), CREDENTIAL_SCOPE_TABLE_PREFIX.to_string());
    config.insert(CREDENTIAL_SCOPE_PREFIX_CONFIG_KEY.to_string(), warehouse_location);
    config.insert(CREDENTIAL_MODE_CONFIG_KEY.to_string(), CREDENTIAL_MODE_CLIENT_PROVIDED.to_string());

    RestLoadTableResponse {
        metadata_location,
        metadata,
        config,
        storage_credentials: Vec::new(),
    }
}

fn load_view_response_from_entry(entry: crate::table_catalog::ViewEntry, metadata: serde_json::Value) -> RestLoadViewResponse {
    let mut config = BTreeMap::new();
    let warehouse_location = entry.warehouse_location.clone();
    config.insert("warehouse-location".to_string(), warehouse_location.clone());
    config.insert(CREDENTIAL_SCOPE_CONFIG_KEY.to_string(), CREDENTIAL_SCOPE_TABLE_PREFIX.to_string());
    config.insert(CREDENTIAL_SCOPE_PREFIX_CONFIG_KEY.to_string(), warehouse_location);
    config.insert(CREDENTIAL_MODE_CONFIG_KEY.to_string(), CREDENTIAL_MODE_CLIENT_PROVIDED.to_string());

    RestLoadViewResponse {
        metadata_location: entry.metadata_location,
        metadata,
        config,
    }
}

fn load_credentials_response_config(vending: &str, mode: &str, reason: Option<&str>) -> BTreeMap<String, String> {
    let mut config = BTreeMap::new();
    config.insert(CREDENTIAL_VENDING_CONFIG_KEY.to_string(), vending.to_string());
    config.insert(CREDENTIAL_MODE_CONFIG_KEY.to_string(), mode.to_string());
    if let Some(reason) = reason {
        config.insert(CREDENTIAL_VENDING_REASON_CONFIG_KEY.to_string(), reason.to_string());
    }
    config
}

fn add_table_credential_scope_config(config: &mut BTreeMap<String, String>, scope_prefix: &str) {
    config.insert(CREDENTIAL_SCOPE_CONFIG_KEY.to_string(), CREDENTIAL_SCOPE_TABLE_PREFIX.to_string());
    config.insert(CREDENTIAL_SCOPE_PREFIX_CONFIG_KEY.to_string(), scope_prefix.to_string());
}

async fn load_credentials_response_from_entry(
    entry: &crate::table_catalog::TableEntry,
    issuer: &dyn TableCredentialIssuer,
    principal: Option<&rustfs_credentials::Credentials>,
) -> S3Result<RestLoadCredentialsResponse> {
    if !issuer.enabled() {
        return Ok(RestLoadCredentialsResponse {
            config: load_credentials_response_config(
                CREDENTIAL_VENDING_UNSUPPORTED,
                CREDENTIAL_MODE_CLIENT_PROVIDED,
                Some(CREDENTIAL_VENDING_DISABLED_REASON),
            ),
            storage_credentials: Vec::new(),
        });
    }
    let scope = table_credential_scope(entry)?;
    let request = TableCredentialIssueRequest {
        entry,
        principal,
        scope_prefix: scope.scope_prefix.clone(),
        object_prefix: scope.object_prefix.clone(),
    };
    let scope_prefix = scope.scope_prefix.clone();
    let storage_credentials = match issuer.issue_table_credentials(request).await? {
        Some(issued) => vec![storage_credential_from_issued(scope, issued)],
        None => {
            let mut config = load_credentials_response_config(
                CREDENTIAL_VENDING_UNSUPPORTED,
                CREDENTIAL_MODE_CLIENT_PROVIDED,
                Some(CREDENTIAL_VENDING_UNSUPPORTED_REASON),
            );
            add_table_credential_scope_config(&mut config, &scope_prefix);
            return Ok(RestLoadCredentialsResponse {
                config,
                storage_credentials: Vec::new(),
            });
        }
    };
    let mut config = load_credentials_response_config(CREDENTIAL_VENDING_SUPPORTED, CREDENTIAL_MODE_CATALOG_VENDED, None);
    add_table_credential_scope_config(&mut config, &scope_prefix);
    Ok(RestLoadCredentialsResponse {
        config,
        storage_credentials,
    })
}

fn commit_table_response_from_result(
    result: crate::table_catalog::TableCommitResult,
    metadata: serde_json::Value,
) -> RestCommitTableResponse {
    let metadata_location = table_metadata_location_for_client(&result.table.table_bucket, &result.table.metadata_location);
    let metadata = table_metadata_for_client(&result.table.table_bucket, metadata);
    RestCommitTableResponse {
        metadata_location,
        metadata,
        version_token: result.table.version_token,
        generation: result.table.generation,
        commit_id: result.commit_log.commit_id,
    }
}

fn table_metadata_location_response_from_entry(entry: crate::table_catalog::TableEntry) -> TableMetadataLocationResponse {
    let metadata_location = table_metadata_location_for_client(&entry.table_bucket, &entry.metadata_location);
    TableMetadataLocationResponse {
        metadata_location,
        version_token: entry.version_token,
        generation: entry.generation,
        warehouse_location: entry.warehouse_location,
    }
}

fn table_commit_request_from_rest_request(
    bucket: &str,
    namespace: &crate::table_catalog::Namespace,
    table: &str,
    request: RestCommitTableRequest,
) -> S3Result<crate::table_catalog::TableCommitRequest> {
    let expected_metadata_location = request
        .expected_metadata_location
        .ok_or_else(|| s3_error!(InvalidRequest, "legacy commit requires expected-metadata-location"))?;
    let new_metadata_location = request
        .new_metadata_location
        .ok_or_else(|| s3_error!(InvalidRequest, "legacy commit requires new-metadata-location"))?;
    Ok(crate::table_catalog::TableCommitRequest {
        table_bucket: bucket.to_string(),
        namespace: namespace.public_name(),
        table: table.to_string(),
        commit_id: request.commit_id.unwrap_or_else(|| Uuid::new_v4().to_string()),
        idempotency_key: request.idempotency_key,
        operation: request.operation.unwrap_or_else(|| "commit".to_string()),
        expected_version_token: request
            .expected_version_token
            .ok_or_else(|| s3_error!(InvalidRequest, "legacy commit requires expected-version-token"))?,
        expected_metadata_location: table_metadata_location_for_catalog(bucket, &expected_metadata_location)?,
        new_metadata_location: table_metadata_location_for_catalog(bucket, &new_metadata_location)?,
        requirements: request.requirements,
        writer: request.writer,
    })
}

fn validate_table_location_in_bucket(bucket: &str, location: &str) -> S3Result<()> {
    crate::table_catalog::validate_table_warehouse_location(bucket, location).map_err(catalog_store_error)
}

fn validate_view_location_in_bucket(bucket: &str, location: &str) -> S3Result<()> {
    crate::table_catalog::validate_view_warehouse_location(bucket, location).map_err(catalog_store_error)
}

fn metadata_table_uuid(metadata: &serde_json::Value) -> S3Result<&str> {
    metadata
        .get("table-uuid")
        .and_then(serde_json::Value::as_str)
        .filter(|uuid| !uuid.is_empty())
        .ok_or_else(|| s3_error!(InvalidRequest, "table metadata is missing table-uuid"))
}

fn metadata_format_version(metadata: &serde_json::Value) -> S3Result<u16> {
    let version = metadata
        .get("format-version")
        .and_then(serde_json::Value::as_u64)
        .filter(|version| *version > 0)
        .ok_or_else(|| s3_error!(InvalidRequest, "table metadata is missing format-version"))?;
    u16::try_from(version).map_err(|_| s3_error!(InvalidRequest, "table metadata format-version is too large"))
}

fn metadata_table_location(metadata: &serde_json::Value) -> S3Result<&str> {
    metadata
        .get("location")
        .and_then(serde_json::Value::as_str)
        .filter(|location| !location.is_empty())
        .ok_or_else(|| s3_error!(InvalidRequest, "table metadata is missing location"))
}

fn validate_metadata_table_location_in_bucket(bucket: &str, metadata: &serde_json::Value) -> S3Result<()> {
    let location = metadata_table_location(metadata)?;
    validate_table_location_in_bucket(bucket, location)
}

fn validate_metadata_view_location_in_bucket(bucket: &str, metadata: &serde_json::Value) -> S3Result<()> {
    let location = metadata_table_location(metadata)?;
    validate_view_location_in_bucket(bucket, location)
}

fn validate_metadata_matches_current_metadata(
    current_metadata: &serde_json::Value,
    target_metadata: &serde_json::Value,
) -> S3Result<()> {
    let expected_table_uuid = metadata_table_uuid(current_metadata)?;
    metadata_format_version(current_metadata)?;
    let target_table_uuid = metadata_table_uuid(target_metadata)?;
    metadata_format_version(target_metadata)?;
    if target_table_uuid != expected_table_uuid {
        return Err(s3_error!(
            InvalidRequest,
            "table metadata table-uuid does not match current table metadata"
        ));
    }
    Ok(())
}

fn metadata_view_uuid(metadata: &serde_json::Value) -> S3Result<&str> {
    metadata
        .get("view-uuid")
        .and_then(serde_json::Value::as_str)
        .filter(|uuid| !uuid.is_empty())
        .ok_or_else(|| s3_error!(InvalidRequest, "view metadata is missing view-uuid"))
}

fn validate_metadata_matches_current_view_metadata(
    current_metadata: &serde_json::Value,
    target_metadata: &serde_json::Value,
) -> S3Result<()> {
    let expected_view_uuid = metadata_view_uuid(current_metadata)?;
    metadata_format_version(current_metadata)?;
    let target_view_uuid = metadata_view_uuid(target_metadata)?;
    metadata_format_version(target_metadata)?;
    if target_view_uuid != expected_view_uuid {
        return Err(s3_error!(InvalidRequest, "view metadata view-uuid does not match current view metadata"));
    }
    Ok(())
}

fn adopt_registered_metadata_identity(
    entry: &mut crate::table_catalog::TableEntry,
    metadata: &serde_json::Value,
) -> S3Result<()> {
    entry.table_uuid = metadata_table_uuid(metadata)?.to_string();
    entry.format_version = metadata_format_version(metadata)?;
    entry.warehouse_location = metadata_table_location(metadata)?.to_string();
    Ok(())
}

fn table_entry_from_register_request(
    bucket: &str,
    namespace: &crate::table_catalog::Namespace,
    request: RegisterTableRequest,
) -> S3Result<crate::table_catalog::TableEntry> {
    if request.overwrite {
        return Err(s3_error!(NotImplemented, "register table overwrite is not supported"));
    }
    let table = crate::table_catalog::IdentifierSegment::parse(request.name)
        .map_err(|err| s3_error!(InvalidRequest, "invalid table name: {}", err))?;
    let metadata_location = table_metadata_location_for_catalog(bucket, &request.metadata_location)?;
    if !crate::table_catalog::is_valid_table_metadata_location(namespace, &table, &metadata_location) {
        return Err(s3_error!(InvalidRequest, "metadata location must be inside the table metadata directory"));
    }

    let table_id = Uuid::new_v4().to_string();
    Ok(crate::table_catalog::TableEntry {
        version: crate::table_catalog::TABLE_CATALOG_ENTRY_VERSION,
        table_bucket: bucket.to_string(),
        namespace: namespace.public_name(),
        table: table.as_str().to_string(),
        table_id: table_id.clone(),
        table_uuid: Uuid::new_v4().to_string(),
        format: "ICEBERG".to_string(),
        format_version: 2,
        warehouse_location: format!("s3://{bucket}/tables/{table_id}"),
        metadata_location,
        version_token: format!("token-{}", Uuid::new_v4()),
        generation: 1,
        state: crate::table_catalog::TableCatalogEntryState::Active,
        properties: BTreeMap::new(),
        created_at: None,
        updated_at: None,
    })
}

fn table_entry_from_import_request(
    bucket: &str,
    namespace: &crate::table_catalog::Namespace,
    table: &str,
    request: CatalogImportRequest,
) -> S3Result<crate::table_catalog::TableEntry> {
    let table = crate::table_catalog::IdentifierSegment::parse(table.to_string())
        .map_err(|err| s3_error!(InvalidRequest, "invalid table name: {}", err))?;
    let metadata_location = table_metadata_location_for_catalog(bucket, &request.metadata_location)?;
    if !crate::table_catalog::is_valid_table_metadata_location(namespace, &table, &metadata_location) {
        return Err(s3_error!(InvalidRequest, "metadata location must be inside the table metadata directory"));
    }

    let table_id = Uuid::new_v4().to_string();
    Ok(crate::table_catalog::TableEntry {
        version: crate::table_catalog::TABLE_CATALOG_ENTRY_VERSION,
        table_bucket: bucket.to_string(),
        namespace: namespace.public_name(),
        table: table.as_str().to_string(),
        table_id: table_id.clone(),
        table_uuid: Uuid::new_v4().to_string(),
        format: "ICEBERG".to_string(),
        format_version: 2,
        warehouse_location: format!("s3://{bucket}/tables/{table_id}"),
        metadata_location,
        version_token: format!("token-{}", Uuid::new_v4()),
        generation: 1,
        state: crate::table_catalog::TableCatalogEntryState::Active,
        properties: request.properties,
        created_at: None,
        updated_at: None,
    })
}

fn table_entry_from_create_table_request(
    bucket: &str,
    namespace: &crate::table_catalog::Namespace,
    request: CreateTableRequest,
) -> S3Result<(crate::table_catalog::TableEntry, serde_json::Value)> {
    if request.stage_create {
        return Err(s3_error!(NotImplemented, "stage-create is not supported"));
    }

    let table = crate::table_catalog::IdentifierSegment::parse(request.name)
        .map_err(|err| s3_error!(InvalidRequest, "invalid table name: {}", err))?;
    let table_id = Uuid::new_v4().to_string();
    let table_uuid = Uuid::new_v4().to_string();
    let warehouse_location = request.location.unwrap_or_else(|| format!("s3://{bucket}/tables/{table_id}"));
    validate_table_location_in_bucket(bucket, &warehouse_location)?;
    let metadata_location =
        crate::table_catalog::default_table_metadata_file_path(namespace, &table, &next_metadata_file_name(1, &table_id));

    let mut entry = crate::table_catalog::TableEntry {
        version: crate::table_catalog::TABLE_CATALOG_ENTRY_VERSION,
        table_bucket: bucket.to_string(),
        namespace: namespace.public_name(),
        table: table.as_str().to_string(),
        table_id,
        table_uuid,
        format: "ICEBERG".to_string(),
        format_version: 2,
        warehouse_location,
        metadata_location,
        version_token: format!("token-{}", Uuid::new_v4()),
        generation: 1,
        state: crate::table_catalog::TableCatalogEntryState::Active,
        properties: request.properties,
        created_at: None,
        updated_at: None,
    };
    let metadata = initial_table_metadata_json(
        &entry,
        request.schema,
        request.partition_spec,
        request.write_order,
        entry.properties.clone(),
    )?;
    entry.format_version = metadata
        .get("format-version")
        .and_then(serde_json::Value::as_u64)
        .and_then(|version| u16::try_from(version).ok())
        .unwrap_or(2);
    Ok((entry, metadata))
}

fn view_entry_from_create_view_request(
    bucket: &str,
    namespace: &crate::table_catalog::Namespace,
    request: CreateViewRequest,
) -> S3Result<(crate::table_catalog::ViewEntry, serde_json::Value)> {
    let view = crate::table_catalog::IdentifierSegment::parse(request.name)
        .map_err(|err| s3_error!(InvalidRequest, "invalid view name: {}", err))?;
    let view_id = Uuid::new_v4().to_string();
    let view_uuid = Uuid::new_v4().to_string();
    let warehouse_location = request.location.unwrap_or_else(|| format!("s3://{bucket}/views/{view_id}"));
    validate_view_location_in_bucket(bucket, &warehouse_location)?;
    let metadata_location =
        crate::table_catalog::default_view_metadata_file_path(namespace, &view, &next_metadata_file_name(1, &view_id));

    let entry = crate::table_catalog::ViewEntry {
        version: crate::table_catalog::TABLE_CATALOG_ENTRY_VERSION,
        table_bucket: bucket.to_string(),
        namespace: namespace.public_name(),
        view: view.as_str().to_string(),
        view_id,
        view_uuid,
        format: "ICEBERG_VIEW".to_string(),
        format_version: 1,
        warehouse_location,
        metadata_location,
        version_token: format!("token-{}", Uuid::new_v4()),
        generation: 1,
        state: crate::table_catalog::TableCatalogEntryState::Active,
        properties: request.properties,
        created_at: None,
        updated_at: None,
    };
    let metadata = initial_view_metadata_json(&entry, request.schema, request.view_version, entry.properties.clone())?;
    Ok((entry, metadata))
}

fn initial_table_metadata_json(
    entry: &crate::table_catalog::TableEntry,
    mut schema: serde_json::Value,
    partition_spec: Option<serde_json::Value>,
    write_order: Option<serde_json::Value>,
    properties: BTreeMap<String, String>,
) -> S3Result<serde_json::Value> {
    let schema_object = schema
        .as_object_mut()
        .ok_or_else(|| s3_error!(InvalidRequest, "schema must be a JSON object"))?;
    schema_object
        .entry("schema-id".to_string())
        .or_insert_with(|| serde_json::Value::from(0));
    let schema_id = schema_object
        .get("schema-id")
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| s3_error!(InvalidRequest, "schema-id must be an integer"))?;
    let last_column_id = max_field_id(&schema);

    let mut spec = partition_spec.unwrap_or_else(|| {
        serde_json::json!({
            "spec-id": 0,
            "fields": []
        })
    });
    let spec_object = spec
        .as_object_mut()
        .ok_or_else(|| s3_error!(InvalidRequest, "partition-spec must be a JSON object"))?;
    spec_object
        .entry("spec-id".to_string())
        .or_insert_with(|| serde_json::Value::from(0));
    spec_object
        .entry("fields".to_string())
        .or_insert_with(|| serde_json::Value::Array(Vec::new()));
    let spec_id = spec_object
        .get("spec-id")
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| s3_error!(InvalidRequest, "partition spec-id must be an integer"))?;
    let last_partition_id = max_partition_field_id(&spec);

    let mut sort_order = write_order.unwrap_or_else(|| {
        serde_json::json!({
            "order-id": 0,
            "fields": []
        })
    });
    let sort_order_object = sort_order
        .as_object_mut()
        .ok_or_else(|| s3_error!(InvalidRequest, "write-order must be a JSON object"))?;
    sort_order_object
        .entry("order-id".to_string())
        .or_insert_with(|| serde_json::Value::from(0));
    sort_order_object
        .entry("fields".to_string())
        .or_insert_with(|| serde_json::Value::Array(Vec::new()));
    let sort_order_id = sort_order_object
        .get("order-id")
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| s3_error!(InvalidRequest, "sort order-id must be an integer"))?;

    Ok(serde_json::json!({
        "format-version": entry.format_version,
        "table-uuid": entry.table_uuid,
        "location": entry.warehouse_location,
        "last-sequence-number": 0,
        "last-updated-ms": current_time_millis(),
        "last-column-id": last_column_id,
        "schemas": [schema],
        "current-schema-id": schema_id,
        "partition-specs": [spec],
        "default-spec-id": spec_id,
        "last-partition-id": last_partition_id,
        "sort-orders": [sort_order],
        "default-sort-order-id": sort_order_id,
        "properties": properties,
        "snapshots": [],
        "snapshot-log": [],
        "metadata-log": [],
        "refs": {}
    }))
}

fn initial_view_metadata_json(
    entry: &crate::table_catalog::ViewEntry,
    mut schema: serde_json::Value,
    mut view_version: serde_json::Value,
    properties: BTreeMap<String, String>,
) -> S3Result<serde_json::Value> {
    let schema_object = schema
        .as_object_mut()
        .ok_or_else(|| s3_error!(InvalidRequest, "schema must be a JSON object"))?;
    schema_object
        .entry("schema-id".to_string())
        .or_insert_with(|| serde_json::Value::from(0));
    let schema_id = schema_object
        .get("schema-id")
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| s3_error!(InvalidRequest, "schema-id must be an integer"))?;

    let view_version_object = view_version
        .as_object_mut()
        .ok_or_else(|| s3_error!(InvalidRequest, "view-version must be a JSON object"))?;
    view_version_object
        .entry("version-id".to_string())
        .or_insert_with(|| serde_json::Value::from(1));
    view_version_object
        .entry("schema-id".to_string())
        .or_insert_with(|| serde_json::Value::from(schema_id));
    view_version_object
        .entry("timestamp-ms".to_string())
        .or_insert_with(|| serde_json::Value::from(current_time_millis()));
    let version_id = view_version_object
        .get("version-id")
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| s3_error!(InvalidRequest, "view-version version-id must be an integer"))?;
    let timestamp_ms = view_version_object
        .get("timestamp-ms")
        .and_then(serde_json::Value::as_i64)
        .unwrap_or_else(current_time_millis);

    Ok(serde_json::json!({
        "format-version": entry.format_version,
        "view-uuid": entry.view_uuid,
        "location": entry.warehouse_location,
        "current-version-id": version_id,
        "schemas": [schema],
        "versions": [view_version],
        "version-log": [{
            "timestamp-ms": timestamp_ms,
            "version-id": version_id
        }],
        "metadata-log": [],
        "properties": properties
    }))
}

fn current_time_millis() -> i64 {
    let now = OffsetDateTime::now_utc();
    now.unix_timestamp()
        .saturating_mul(1000)
        .saturating_add(i64::from(now.millisecond()))
}

fn max_field_id(value: &serde_json::Value) -> i64 {
    let mut max_id = 0;
    collect_max_field_id(value, &mut max_id);
    max_id
}

fn collect_max_field_id(value: &serde_json::Value, max_id: &mut i64) {
    match value {
        serde_json::Value::Object(object) => {
            if let Some(id) = object.get("id").and_then(serde_json::Value::as_i64) {
                *max_id = (*max_id).max(id);
            }
            for child in object.values() {
                collect_max_field_id(child, max_id);
            }
        }
        serde_json::Value::Array(values) => {
            for child in values {
                collect_max_field_id(child, max_id);
            }
        }
        _ => {}
    }
}

fn max_partition_field_id(value: &serde_json::Value) -> i64 {
    let mut max_id = 999;
    let Some(fields) = value.get("fields").and_then(serde_json::Value::as_array) else {
        return max_id;
    };
    for field in fields {
        if let Some(field_id) = field.get("field-id").and_then(serde_json::Value::as_i64) {
            max_id = max_id.max(field_id);
        }
    }
    max_id
}

fn standard_commit_ids(commit_id: Option<String>) -> (String, String) {
    match commit_id {
        Some(commit_id) => match Uuid::parse_str(&commit_id) {
            Ok(uuid) => {
                let commit_id = uuid.to_string();
                (commit_id.clone(), commit_id)
            }
            Err(_) => {
                let metadata_file_token = table_catalog_path_hash(&commit_id);
                (commit_id, metadata_file_token)
            }
        },
        None => {
            let commit_id = Uuid::new_v4().to_string();
            (commit_id.clone(), commit_id)
        }
    }
}

fn next_metadata_file_name(generation: u64, metadata_file_token: &str) -> String {
    format!("{generation:05}-{metadata_file_token}.metadata.json")
}

fn validate_table_commit_requirements(metadata: &serde_json::Value, requirements: &[serde_json::Value]) -> S3Result<()> {
    for requirement in requirements {
        let requirement_type = requirement
            .get("type")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| s3_error!(InvalidRequest, "commit requirement type is required"))?;
        match requirement_type {
            "assert-create" => {
                return Err(s3_error!(PreconditionFailed, "commit requirement failed: table already exists"));
            }
            "assert-table-uuid" => {
                let expected = requirement
                    .get("uuid")
                    .and_then(serde_json::Value::as_str)
                    .ok_or_else(|| s3_error!(InvalidRequest, "assert-table-uuid requires uuid"))?;
                let actual = metadata
                    .get("table-uuid")
                    .and_then(serde_json::Value::as_str)
                    .ok_or_else(|| s3_error!(InvalidRequest, "current table metadata is missing table-uuid"))?;
                if actual != expected {
                    return Err(s3_error!(PreconditionFailed, "commit requirement failed: table uuid changed"));
                }
            }
            "assert-current-schema-id" => {
                validate_i64_requirement(metadata, requirement, "current-schema-id", "current schema id")?;
            }
            "assert-default-spec-id" => {
                validate_i64_requirement(metadata, requirement, "default-spec-id", "default spec id")?;
            }
            "assert-default-sort-order-id" => {
                validate_i64_requirement(metadata, requirement, "default-sort-order-id", "default sort order id")?;
            }
            "assert-last-assigned-field-id" => {
                validate_i64_requirement_with_metadata_key(
                    metadata,
                    requirement,
                    "last-assigned-field-id",
                    "last-column-id",
                    "last assigned field id",
                )?;
            }
            "assert-last-assigned-partition-id" => {
                validate_i64_requirement_with_metadata_key(
                    metadata,
                    requirement,
                    "last-assigned-partition-id",
                    "last-partition-id",
                    "last assigned partition id",
                )?;
            }
            "assert-ref-snapshot-id" => validate_ref_snapshot_requirement(metadata, requirement)?,
            "assert-current-snapshot-id" => validate_current_snapshot_requirement(metadata, requirement)?,
            _ => return Err(s3_error!(NotImplemented, "unsupported commit requirement: {requirement_type}")),
        }
    }
    Ok(())
}

fn validate_i64_requirement(
    metadata: &serde_json::Value,
    requirement: &serde_json::Value,
    key: &str,
    label: &str,
) -> S3Result<()> {
    validate_i64_requirement_with_metadata_key(metadata, requirement, key, key, label)
}

fn validate_i64_requirement_with_metadata_key(
    metadata: &serde_json::Value,
    requirement: &serde_json::Value,
    requirement_key: &str,
    metadata_key: &str,
    label: &str,
) -> S3Result<()> {
    let expected = requirement
        .get(requirement_key)
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| s3_error!(InvalidRequest, "{requirement_key} must be an integer"))?;
    let actual = metadata
        .get(metadata_key)
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| s3_error!(InvalidRequest, "current table metadata is missing {metadata_key}"))?;
    if actual != expected {
        return Err(s3_error!(PreconditionFailed, "commit requirement failed: {label} changed"));
    }
    Ok(())
}

fn validate_ref_snapshot_requirement(metadata: &serde_json::Value, requirement: &serde_json::Value) -> S3Result<()> {
    let ref_name = requirement
        .get("ref")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| s3_error!(InvalidRequest, "assert-ref-snapshot-id requires ref"))?;
    let refs = metadata.get("refs").and_then(serde_json::Value::as_object);
    let actual = refs
        .and_then(|refs| refs.get(ref_name))
        .and_then(|reference| reference.get("snapshot-id"))
        .and_then(serde_json::Value::as_i64);
    if requirement.get("snapshot-id").is_some_and(serde_json::Value::is_null) {
        if actual.is_some() {
            return Err(s3_error!(PreconditionFailed, "commit requirement failed: snapshot ref exists"));
        }
        return Ok(());
    }
    let expected = requirement
        .get("snapshot-id")
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| s3_error!(InvalidRequest, "assert-ref-snapshot-id requires snapshot-id"))?;
    if actual != Some(expected) {
        return Err(s3_error!(PreconditionFailed, "commit requirement failed: snapshot ref changed"));
    }
    Ok(())
}

fn validate_current_snapshot_requirement(metadata: &serde_json::Value, requirement: &serde_json::Value) -> S3Result<()> {
    let actual = metadata.get("current-snapshot-id").and_then(serde_json::Value::as_i64);
    if requirement.get("snapshot-id").is_some_and(serde_json::Value::is_null) {
        if actual.is_some() {
            return Err(s3_error!(PreconditionFailed, "commit requirement failed: current snapshot exists"));
        }
        return Ok(());
    }
    let expected = requirement
        .get("snapshot-id")
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| s3_error!(InvalidRequest, "assert-current-snapshot-id requires snapshot-id"))?;
    if actual != Some(expected) {
        return Err(s3_error!(PreconditionFailed, "commit requirement failed: current snapshot changed"));
    }
    Ok(())
}

fn apply_table_commit_updates(
    mut metadata: serde_json::Value,
    updates: &[serde_json::Value],
    previous_metadata_location: &str,
) -> S3Result<serde_json::Value> {
    if !metadata.is_object() {
        return Err(s3_error!(InvalidRequest, "current table metadata must be a JSON object"));
    }

    for update in updates {
        let action = update
            .get("action")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| s3_error!(InvalidRequest, "table update action is required"))?;
        match action {
            "assign-uuid" => apply_assign_uuid_update(&mut metadata, update)?,
            "upgrade-format-version" => apply_upgrade_format_version_update(&mut metadata, update)?,
            "add-schema" => apply_add_schema_update(&mut metadata, update)?,
            "set-current-schema" => apply_set_current_schema_update(&mut metadata, update)?,
            "add-spec" => apply_add_spec_update(&mut metadata, update)?,
            "set-default-spec" => apply_set_default_spec_update(&mut metadata, update)?,
            "add-sort-order" => apply_add_sort_order_update(&mut metadata, update)?,
            "set-default-sort-order" => apply_set_default_sort_order_update(&mut metadata, update)?,
            "add-snapshot" => apply_add_snapshot_update(&mut metadata, update)?,
            "set-snapshot-ref" => apply_set_snapshot_ref_update(&mut metadata, update)?,
            "remove-snapshots" => apply_remove_snapshots_update(&mut metadata, update)?,
            "remove-snapshot-ref" => apply_remove_snapshot_ref_update(&mut metadata, update)?,
            "set-location" => apply_set_location_update(&mut metadata, update)?,
            "set-properties" => apply_set_properties_update(&mut metadata, update)?,
            "remove-properties" => apply_remove_properties_update(&mut metadata, update)?,
            _ => return Err(s3_error!(NotImplemented, "unsupported table update: {action}")),
        }
    }

    append_previous_metadata_log(&mut metadata, previous_metadata_location)?;
    metadata_object_mut(&mut metadata)?.insert("last-updated-ms".to_string(), serde_json::Value::from(current_time_millis()));
    Ok(metadata)
}

fn validate_view_commit_requirements(metadata: &serde_json::Value, requirements: &[serde_json::Value]) -> S3Result<()> {
    for requirement in requirements {
        let requirement_type = requirement
            .get("type")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| s3_error!(InvalidRequest, "commit requirement type is required"))?;
        match requirement_type {
            "assert-view-uuid" => {
                let expected = requirement
                    .get("uuid")
                    .and_then(serde_json::Value::as_str)
                    .ok_or_else(|| s3_error!(InvalidRequest, "assert-view-uuid requires uuid"))?;
                let actual = metadata
                    .get("view-uuid")
                    .and_then(serde_json::Value::as_str)
                    .ok_or_else(|| s3_error!(InvalidRequest, "current view metadata is missing view-uuid"))?;
                if actual != expected {
                    return Err(s3_error!(PreconditionFailed, "commit requirement failed: view uuid changed"));
                }
            }
            "assert-current-view-version-id" => {
                validate_i64_requirement_with_metadata_key(
                    metadata,
                    requirement,
                    "current-view-version-id",
                    "current-version-id",
                    "current view version id",
                )?;
            }
            _ => return Err(s3_error!(NotImplemented, "unsupported view commit requirement: {requirement_type}")),
        }
    }
    Ok(())
}

fn apply_view_commit_updates(
    mut metadata: serde_json::Value,
    updates: &[serde_json::Value],
    previous_metadata_location: &str,
) -> S3Result<serde_json::Value> {
    if !metadata.is_object() {
        return Err(s3_error!(InvalidRequest, "current view metadata must be a JSON object"));
    }

    for update in updates {
        let action = update
            .get("action")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| s3_error!(InvalidRequest, "view update action is required"))?;
        match action {
            "assign-uuid" => apply_assign_uuid_update(&mut metadata, update)?,
            "add-schema" => apply_add_schema_update(&mut metadata, update)?,
            "set-current-schema" => apply_set_current_schema_update(&mut metadata, update)?,
            "add-view-version" => apply_add_view_version_update(&mut metadata, update)?,
            "set-current-view-version" => apply_set_current_view_version_update(&mut metadata, update)?,
            "set-location" => apply_set_location_update(&mut metadata, update)?,
            "set-properties" => apply_set_properties_update(&mut metadata, update)?,
            "remove-properties" => apply_remove_properties_update(&mut metadata, update)?,
            _ => return Err(s3_error!(NotImplemented, "unsupported view update: {action}")),
        }
    }

    append_previous_metadata_log(&mut metadata, previous_metadata_location)?;
    metadata_object_mut(&mut metadata)?.insert("last-updated-ms".to_string(), serde_json::Value::from(current_time_millis()));
    Ok(metadata)
}

fn apply_assign_uuid_update(metadata: &mut serde_json::Value, update: &serde_json::Value) -> S3Result<()> {
    let uuid = update
        .get("uuid")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| s3_error!(InvalidRequest, "assign-uuid requires uuid"))?;
    let object = metadata_object_mut(metadata)?;
    if let Some(existing) = object.get("table-uuid").and_then(serde_json::Value::as_str)
        && existing != uuid
    {
        return Err(s3_error!(PreconditionFailed, "cannot reassign table uuid"));
    }
    object.insert("table-uuid".to_string(), serde_json::Value::String(uuid.to_string()));
    Ok(())
}

fn apply_add_view_version_update(metadata: &mut serde_json::Value, update: &serde_json::Value) -> S3Result<()> {
    let mut view_version = update
        .get("view-version")
        .cloned()
        .ok_or_else(|| s3_error!(InvalidRequest, "add-view-version requires view-version"))?;
    if !view_version.is_object() {
        return Err(s3_error!(InvalidRequest, "view-version must be a JSON object"));
    }
    if view_version.get("version-id").is_none() {
        let next_id = next_array_object_i64(metadata, "versions", "version-id")?;
        view_version
            .as_object_mut()
            .ok_or_else(|| s3_error!(InvalidRequest, "view-version must be a JSON object"))?
            .insert("version-id".to_string(), serde_json::Value::from(next_id));
    }
    view_version
        .as_object_mut()
        .ok_or_else(|| s3_error!(InvalidRequest, "view-version must be a JSON object"))?
        .entry("timestamp-ms".to_string())
        .or_insert_with(|| serde_json::Value::from(current_time_millis()));
    ensure_array_field(metadata, "versions")?.push(view_version);
    Ok(())
}

fn apply_set_current_view_version_update(metadata: &mut serde_json::Value, update: &serde_json::Value) -> S3Result<()> {
    let requested_id = update
        .get("view-version-id")
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| s3_error!(InvalidRequest, "set-current-view-version requires view-version-id"))?;
    let version_id = if requested_id == -1 {
        last_array_object_i64(metadata, "versions", "version-id")?
    } else {
        requested_id
    };
    metadata_object_mut(metadata)?.insert("current-version-id".to_string(), serde_json::Value::from(version_id));
    ensure_array_field(metadata, "version-log")?.push(serde_json::json!({
        "timestamp-ms": current_time_millis(),
        "version-id": version_id
    }));
    Ok(())
}

fn apply_upgrade_format_version_update(metadata: &mut serde_json::Value, update: &serde_json::Value) -> S3Result<()> {
    let version = update
        .get("format-version")
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| s3_error!(InvalidRequest, "upgrade-format-version requires format-version"))?;
    let current = metadata
        .get("format-version")
        .and_then(serde_json::Value::as_i64)
        .unwrap_or_default();
    if version < current {
        return Err(s3_error!(InvalidRequest, "format-version cannot be downgraded"));
    }
    metadata_object_mut(metadata)?.insert("format-version".to_string(), serde_json::Value::from(version));
    Ok(())
}

fn apply_add_schema_update(metadata: &mut serde_json::Value, update: &serde_json::Value) -> S3Result<()> {
    let mut schema = update
        .get("schema")
        .cloned()
        .ok_or_else(|| s3_error!(InvalidRequest, "add-schema requires schema"))?;
    if !schema.is_object() {
        return Err(s3_error!(InvalidRequest, "add-schema schema must be a JSON object"));
    }
    if schema.get("schema-id").is_none() {
        let next_id = next_array_object_i64(metadata, "schemas", "schema-id")?;
        schema
            .as_object_mut()
            .ok_or_else(|| s3_error!(InvalidRequest, "add-schema schema must be a JSON object"))?
            .insert("schema-id".to_string(), serde_json::Value::from(next_id));
    }
    let last_column_id = max_field_id(&schema);
    ensure_array_field(metadata, "schemas")?.push(schema);
    let object = metadata_object_mut(metadata)?;
    let current_last = object
        .get("last-column-id")
        .and_then(serde_json::Value::as_i64)
        .unwrap_or_default();
    object.insert("last-column-id".to_string(), serde_json::Value::from(current_last.max(last_column_id)));
    Ok(())
}

fn apply_set_current_schema_update(metadata: &mut serde_json::Value, update: &serde_json::Value) -> S3Result<()> {
    let requested_id = update
        .get("schema-id")
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| s3_error!(InvalidRequest, "set-current-schema requires schema-id"))?;
    let schema_id = if requested_id == -1 {
        last_array_object_i64(metadata, "schemas", "schema-id")?
    } else {
        requested_id
    };
    metadata_object_mut(metadata)?.insert("current-schema-id".to_string(), serde_json::Value::from(schema_id));
    Ok(())
}

fn apply_add_spec_update(metadata: &mut serde_json::Value, update: &serde_json::Value) -> S3Result<()> {
    let mut spec = update
        .get("spec")
        .cloned()
        .ok_or_else(|| s3_error!(InvalidRequest, "add-spec requires spec"))?;
    if !spec.is_object() {
        return Err(s3_error!(InvalidRequest, "add-spec spec must be a JSON object"));
    }
    if spec.get("spec-id").is_none() {
        let next_id = next_array_object_i64(metadata, "partition-specs", "spec-id")?;
        spec.as_object_mut()
            .ok_or_else(|| s3_error!(InvalidRequest, "add-spec spec must be a JSON object"))?
            .insert("spec-id".to_string(), serde_json::Value::from(next_id));
    }
    let last_partition_id = max_partition_field_id(&spec);
    ensure_array_field(metadata, "partition-specs")?.push(spec);
    let object = metadata_object_mut(metadata)?;
    let current_last = object
        .get("last-partition-id")
        .and_then(serde_json::Value::as_i64)
        .unwrap_or(999);
    object.insert(
        "last-partition-id".to_string(),
        serde_json::Value::from(current_last.max(last_partition_id)),
    );
    Ok(())
}

fn apply_set_default_spec_update(metadata: &mut serde_json::Value, update: &serde_json::Value) -> S3Result<()> {
    let requested_id = update
        .get("spec-id")
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| s3_error!(InvalidRequest, "set-default-spec requires spec-id"))?;
    let spec_id = if requested_id == -1 {
        last_array_object_i64(metadata, "partition-specs", "spec-id")?
    } else {
        requested_id
    };
    metadata_object_mut(metadata)?.insert("default-spec-id".to_string(), serde_json::Value::from(spec_id));
    Ok(())
}

fn apply_add_sort_order_update(metadata: &mut serde_json::Value, update: &serde_json::Value) -> S3Result<()> {
    let mut sort_order = update
        .get("sort-order")
        .cloned()
        .ok_or_else(|| s3_error!(InvalidRequest, "add-sort-order requires sort-order"))?;
    if !sort_order.is_object() {
        return Err(s3_error!(InvalidRequest, "add-sort-order sort-order must be a JSON object"));
    }
    if sort_order.get("order-id").is_none() {
        let next_id = next_array_object_i64(metadata, "sort-orders", "order-id")?;
        sort_order
            .as_object_mut()
            .ok_or_else(|| s3_error!(InvalidRequest, "add-sort-order sort-order must be a JSON object"))?
            .insert("order-id".to_string(), serde_json::Value::from(next_id));
    }
    ensure_array_field(metadata, "sort-orders")?.push(sort_order);
    Ok(())
}

fn apply_set_default_sort_order_update(metadata: &mut serde_json::Value, update: &serde_json::Value) -> S3Result<()> {
    let requested_id = update
        .get("sort-order-id")
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| s3_error!(InvalidRequest, "set-default-sort-order requires sort-order-id"))?;
    let sort_order_id = if requested_id == -1 {
        last_array_object_i64(metadata, "sort-orders", "order-id")?
    } else {
        requested_id
    };
    metadata_object_mut(metadata)?.insert("default-sort-order-id".to_string(), serde_json::Value::from(sort_order_id));
    Ok(())
}

fn apply_add_snapshot_update(metadata: &mut serde_json::Value, update: &serde_json::Value) -> S3Result<()> {
    let snapshot = update
        .get("snapshot")
        .cloned()
        .ok_or_else(|| s3_error!(InvalidRequest, "add-snapshot requires snapshot"))?;
    let snapshot_id = snapshot
        .get("snapshot-id")
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| s3_error!(InvalidRequest, "snapshot-id must be an integer"))?;
    let sequence_number = snapshot
        .get("sequence-number")
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| s3_error!(InvalidRequest, "snapshot sequence-number must be an integer"))?;
    let timestamp_ms = snapshot
        .get("timestamp-ms")
        .and_then(serde_json::Value::as_i64)
        .unwrap_or_else(current_time_millis);
    validate_added_snapshot(metadata, &snapshot, snapshot_id, sequence_number)?;
    ensure_array_field(metadata, "snapshots")?.push(snapshot);
    let object = metadata_object_mut(metadata)?;
    object.insert("last-sequence-number".to_string(), serde_json::Value::from(sequence_number));
    object.insert("current-snapshot-id".to_string(), serde_json::Value::from(snapshot_id));
    ensure_array_field(metadata, "snapshot-log")?.push(serde_json::json!({
        "timestamp-ms": timestamp_ms,
        "snapshot-id": snapshot_id
    }));
    Ok(())
}

fn validate_added_snapshot(
    metadata: &serde_json::Value,
    snapshot: &serde_json::Value,
    snapshot_id: i64,
    sequence_number: i64,
) -> S3Result<()> {
    if metadata
        .get("snapshots")
        .and_then(serde_json::Value::as_array)
        .is_some_and(|snapshots| {
            snapshots
                .iter()
                .any(|snapshot| snapshot.get("snapshot-id").and_then(serde_json::Value::as_i64) == Some(snapshot_id))
        })
    {
        return Err(s3_error!(PreconditionFailed, "snapshot id already exists"));
    }

    let current_snapshot_id = metadata.get("current-snapshot-id").and_then(serde_json::Value::as_i64);
    if let Some(parent_snapshot_id) = snapshot.get("parent-snapshot-id").and_then(serde_json::Value::as_i64)
        && Some(parent_snapshot_id) != current_snapshot_id
    {
        return Err(s3_error!(PreconditionFailed, "snapshot parent no longer matches current snapshot"));
    }

    let current_sequence_number = metadata
        .get("last-sequence-number")
        .and_then(serde_json::Value::as_i64)
        .unwrap_or_default();
    if sequence_number <= current_sequence_number {
        return Err(s3_error!(PreconditionFailed, "snapshot sequence number must advance"));
    }

    if !snapshot_has_manifest_references(snapshot) {
        return Err(s3_error!(InvalidRequest, "snapshot manifest-list or manifests are required"));
    }

    let operation = snapshot
        .get("summary")
        .and_then(|summary| summary.get("operation"))
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| s3_error!(InvalidRequest, "snapshot summary.operation is required"))?;
    if !matches!(operation, "append" | "overwrite" | "delete" | "replace") {
        return Err(s3_error!(NotImplemented, "unsupported snapshot operation: {operation}"));
    }

    Ok(())
}

fn snapshot_has_manifest_references(snapshot: &serde_json::Value) -> bool {
    if snapshot
        .get("manifest-list")
        .and_then(serde_json::Value::as_str)
        .is_some_and(|manifest_list| !manifest_list.is_empty())
    {
        return true;
    }
    snapshot
        .get("manifests")
        .and_then(serde_json::Value::as_array)
        .is_some_and(|manifests| {
            !manifests.is_empty()
                && manifests
                    .iter()
                    .all(|manifest| manifest.as_str().is_some_and(|manifest| !manifest.is_empty()))
        })
}

#[derive(Default)]
struct SnapshotLiveFiles {
    data_files: BTreeMap<String, SnapshotFileIdentity>,
    delete_files: BTreeMap<String, SnapshotFileIdentity>,
    manifest_files: BTreeMap<String, SnapshotManifestIdentity>,
}

impl SnapshotLiveFiles {
    fn contains(&self, location: &str) -> bool {
        self.data_files.contains_key(location) || self.delete_files.contains_key(location)
    }

    fn identity(
        &self,
        location: &str,
        object_kind: &crate::table_catalog::TableMetadataMaintenanceObjectKind,
    ) -> Option<&SnapshotFileIdentity> {
        match object_kind {
            crate::table_catalog::TableMetadataMaintenanceObjectKind::DataFile => self.data_files.get(location),
            crate::table_catalog::TableMetadataMaintenanceObjectKind::DeleteFile => self.delete_files.get(location),
            _ => None,
        }
    }
}

#[derive(Default)]
struct SnapshotFileChanges {
    added_data_files: BTreeSet<String>,
    added_delete_files: BTreeSet<String>,
    deleted_data_files: BTreeSet<String>,
    deleted_delete_files: BTreeSet<String>,
}

impl SnapshotFileChanges {
    fn has_delete_or_row_level_change(&self) -> bool {
        !self.added_delete_files.is_empty() || !self.deleted_data_files.is_empty() || !self.deleted_delete_files.is_empty()
    }

    fn has_any_change(&self) -> bool {
        !self.added_data_files.is_empty() || !self.added_delete_files.is_empty() || self.has_deleted_files()
    }

    fn has_deleted_files(&self) -> bool {
        !self.deleted_data_files.is_empty() || !self.deleted_delete_files.is_empty()
    }
}

struct SnapshotChangeContext<'a> {
    current_live_files: &'a SnapshotLiveFiles,
    snapshot_id: i64,
    sequence_number: i64,
}

#[derive(PartialEq, Eq)]
struct SnapshotManifestIdentity {
    sequence_number: Option<i64>,
    added_snapshot_id: Option<i64>,
}

#[derive(PartialEq, Eq)]
struct SnapshotFileIdentity {
    sequence_number: Option<i64>,
    file_sequence_number: Option<i64>,
}

async fn validate_table_snapshot_commit_conflicts<B>(
    metadata_backend: &B,
    bucket: &str,
    namespace: &crate::table_catalog::Namespace,
    table: &crate::table_catalog::IdentifierSegment,
    entry: &crate::table_catalog::TableEntry,
    current_metadata: &serde_json::Value,
    updates: &[serde_json::Value],
) -> S3Result<()>
where
    B: crate::table_catalog::TableCatalogObjectBackend,
{
    let Some(snapshot) = added_snapshot_update(updates)? else {
        return Ok(());
    };
    let snapshot_id = snapshot
        .get("snapshot-id")
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| s3_error!(InvalidRequest, "snapshot-id must be an integer"))?;
    let sequence_number = snapshot
        .get("sequence-number")
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| s3_error!(InvalidRequest, "snapshot sequence-number must be an integer"))?;
    let operation = snapshot
        .get("summary")
        .and_then(|summary| summary.get("operation"))
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| s3_error!(InvalidRequest, "snapshot summary.operation is required"))?;

    let current_live_files =
        load_current_snapshot_live_files(metadata_backend, bucket, namespace, table, entry, current_metadata).await?;
    let changes = load_snapshot_file_changes(
        metadata_backend,
        bucket,
        namespace,
        table,
        entry,
        snapshot,
        SnapshotChangeContext {
            current_live_files: &current_live_files,
            snapshot_id,
            sequence_number,
        },
    )
    .await?;

    for location in changes.added_data_files.iter().chain(changes.added_delete_files.iter()) {
        if current_live_files.contains(location) {
            return Err(s3_error!(
                PreconditionFailed,
                "commit requirement failed: added file already exists in current snapshot"
            ));
        }
    }

    match operation {
        "append" => {
            if changes.has_deleted_files() || !changes.added_delete_files.is_empty() {
                return Err(s3_error!(InvalidRequest, "append snapshot cannot delete data files or add delete files"));
            }
        }
        "overwrite" | "delete" | "replace" => {
            if current_metadata
                .get("current-snapshot-id")
                .and_then(serde_json::Value::as_i64)
                .is_none()
            {
                return Err(s3_error!(InvalidRequest, "row-level snapshot operation requires a current snapshot"));
            }
            if operation == "overwrite" {
                if !changes.has_any_change() {
                    return Err(s3_error!(InvalidRequest, "overwrite snapshot operation requires changed files"));
                }
            } else if !changes.has_delete_or_row_level_change() {
                return Err(s3_error!(
                    InvalidRequest,
                    "row-level snapshot operation requires deleted data files or added delete files"
                ));
            }
            for location in changes.deleted_data_files.iter().chain(changes.deleted_delete_files.iter()) {
                if !current_live_files.contains(location) {
                    return Err(s3_error!(PreconditionFailed, "commit requirement failed: deleted file is not current"));
                }
            }
        }
        _ => return Err(s3_error!(NotImplemented, "unsupported snapshot operation: {operation}")),
    }

    Ok(())
}

fn added_snapshot_update(updates: &[serde_json::Value]) -> S3Result<Option<&serde_json::Value>> {
    let mut snapshot = None;
    for update in updates {
        if update.get("action").and_then(serde_json::Value::as_str) != Some("add-snapshot") {
            continue;
        }
        if snapshot.is_some() {
            return Err(s3_error!(InvalidRequest, "standard commit supports one add-snapshot update"));
        }
        snapshot = Some(
            update
                .get("snapshot")
                .ok_or_else(|| s3_error!(InvalidRequest, "add-snapshot requires snapshot"))?,
        );
    }
    Ok(snapshot)
}

async fn load_current_snapshot_live_files<B>(
    metadata_backend: &B,
    bucket: &str,
    namespace: &crate::table_catalog::Namespace,
    table: &crate::table_catalog::IdentifierSegment,
    entry: &crate::table_catalog::TableEntry,
    current_metadata: &serde_json::Value,
) -> S3Result<SnapshotLiveFiles>
where
    B: crate::table_catalog::TableCatalogObjectBackend,
{
    let Some(current_snapshot_id) = current_metadata
        .get("current-snapshot-id")
        .and_then(serde_json::Value::as_i64)
    else {
        return Ok(SnapshotLiveFiles::default());
    };
    let snapshot = current_metadata
        .get("snapshots")
        .and_then(serde_json::Value::as_array)
        .and_then(|snapshots| {
            snapshots
                .iter()
                .find(|snapshot| snapshot.get("snapshot-id").and_then(serde_json::Value::as_i64) == Some(current_snapshot_id))
        })
        .ok_or_else(|| s3_error!(InvalidRequest, "current snapshot metadata is missing"))?;

    let mut live_files = SnapshotLiveFiles::default();
    for manifest in read_snapshot_manifest_references(metadata_backend, bucket, namespace, table, entry, snapshot).await? {
        let SnapshotManifestLocation {
            manifest_path,
            sequence_number,
            added_snapshot_id,
        } = manifest.location;
        live_files.manifest_files.insert(
            manifest_path,
            SnapshotManifestIdentity {
                sequence_number,
                added_snapshot_id,
            },
        );
        for reference in manifest.references {
            let status = reference
                .entry_status
                .ok_or_else(|| s3_error!(InvalidRequest, "manifest entry status is required"))?;
            match status {
                0 | 1 => {
                    let identity = SnapshotFileIdentity {
                        sequence_number: reference.sequence_number,
                        file_sequence_number: reference.file_sequence_number,
                    };
                    match reference.object_kind {
                        crate::table_catalog::TableMetadataMaintenanceObjectKind::DataFile => {
                            live_files.data_files.insert(reference.location, identity);
                        }
                        crate::table_catalog::TableMetadataMaintenanceObjectKind::DeleteFile => {
                            live_files.delete_files.insert(reference.location, identity);
                        }
                        _ => {}
                    }
                }
                2 => {}
                _ => return Err(s3_error!(InvalidRequest, "manifest entry status is unsupported")),
            }
        }
    }
    Ok(live_files)
}

async fn load_snapshot_file_changes<B>(
    metadata_backend: &B,
    bucket: &str,
    namespace: &crate::table_catalog::Namespace,
    table: &crate::table_catalog::IdentifierSegment,
    entry: &crate::table_catalog::TableEntry,
    snapshot: &serde_json::Value,
    context: SnapshotChangeContext<'_>,
) -> S3Result<SnapshotFileChanges>
where
    B: crate::table_catalog::TableCatalogObjectBackend,
{
    let mut changes = SnapshotFileChanges::default();
    for manifest in read_snapshot_manifest_references(metadata_backend, bucket, namespace, table, entry, snapshot).await? {
        let inherited_identity = context
            .current_live_files
            .manifest_files
            .get(&manifest.location.manifest_path);
        if let Some(inherited_identity) = inherited_identity {
            let candidate_identity = SnapshotManifestIdentity {
                sequence_number: manifest.location.sequence_number,
                added_snapshot_id: manifest.location.added_snapshot_id,
            };
            if inherited_identity
                .sequence_number
                .is_some_and(|sequence_number| candidate_identity.sequence_number != Some(sequence_number))
                || inherited_identity
                    .added_snapshot_id
                    .is_some_and(|snapshot_id| candidate_identity.added_snapshot_id != Some(snapshot_id))
            {
                return Err(s3_error!(InvalidRequest, "inherited manifest must preserve its manifest-list identity"));
            }
            continue;
        }
        if manifest
            .location
            .added_snapshot_id
            .is_some_and(|added_snapshot_id| added_snapshot_id != context.snapshot_id)
        {
            return Err(s3_error!(InvalidRequest, "new manifest must belong to the committed snapshot"));
        }
        if manifest
            .location
            .sequence_number
            .is_some_and(|sequence_number| sequence_number != context.sequence_number)
        {
            return Err(s3_error!(InvalidRequest, "new manifest sequence must match the committed snapshot"));
        }

        for reference in manifest.references {
            let status = reference
                .entry_status
                .ok_or_else(|| s3_error!(InvalidRequest, "manifest entry status is required"))?;
            if matches!(status, 1 | 2) && reference.snapshot_id != Some(context.snapshot_id) {
                return Err(s3_error!(
                    InvalidRequest,
                    "manifest changed entries must belong to the committed snapshot"
                ));
            }
            if status == 1
                && manifest.location.sequence_number.is_some_and(|sequence_number| {
                    reference.sequence_number != Some(sequence_number) || reference.file_sequence_number != Some(sequence_number)
                })
            {
                return Err(s3_error!(InvalidRequest, "added manifest entry sequence must match its manifest"));
            }
            if status == 2
                && context
                    .current_live_files
                    .identity(&reference.location, &reference.object_kind)
                    .is_some_and(|current_identity| {
                        current_identity
                            != &SnapshotFileIdentity {
                                sequence_number: reference.sequence_number,
                                file_sequence_number: reference.file_sequence_number,
                            }
                    })
            {
                return Err(s3_error!(
                    InvalidRequest,
                    "deleted manifest entry must preserve the current file sequence"
                ));
            }

            match (status, reference.object_kind) {
                (0, _) => {}
                (1, crate::table_catalog::TableMetadataMaintenanceObjectKind::DataFile) => {
                    changes.added_data_files.insert(reference.location);
                }
                (1, crate::table_catalog::TableMetadataMaintenanceObjectKind::DeleteFile) => {
                    changes.added_delete_files.insert(reference.location);
                }
                (2, crate::table_catalog::TableMetadataMaintenanceObjectKind::DataFile) => {
                    changes.deleted_data_files.insert(reference.location);
                }
                (2, crate::table_catalog::TableMetadataMaintenanceObjectKind::DeleteFile) => {
                    changes.deleted_delete_files.insert(reference.location);
                }
                _ => return Err(s3_error!(InvalidRequest, "manifest entry status is unsupported")),
            }
        }
    }
    Ok(changes)
}

struct SnapshotManifestReferences {
    location: SnapshotManifestLocation,
    references: Vec<crate::table_catalog::ManifestDataFileReference>,
}

async fn read_snapshot_manifest_references<B>(
    metadata_backend: &B,
    bucket: &str,
    namespace: &crate::table_catalog::Namespace,
    table: &crate::table_catalog::IdentifierSegment,
    entry: &crate::table_catalog::TableEntry,
    snapshot: &serde_json::Value,
) -> S3Result<Vec<SnapshotManifestReferences>>
where
    B: crate::table_catalog::TableCatalogObjectBackend,
{
    let manifest_locations = snapshot_manifest_locations(metadata_backend, bucket, namespace, table, entry, snapshot).await?;
    let mut manifests = Vec::new();
    for manifest_location in manifest_locations {
        let manifest_key = table_commit_object_key(
            bucket,
            namespace,
            table,
            entry,
            &manifest_location.manifest_path,
            crate::table_catalog::TableMetadataMaintenanceObjectKind::ManifestFile,
        )?;
        let manifest_object = metadata_backend
            .read_object(bucket, &manifest_key)
            .await
            .map_err(catalog_store_error)?
            .ok_or_else(|| s3_error!(InvalidRequest, "snapshot manifest object is missing"))?;
        let file_references =
            crate::table_catalog::data_file_references_from_manifest_avro(&manifest_object.data).map_err(catalog_store_error)?;
        let mut references = Vec::with_capacity(file_references.len());
        for mut reference in file_references {
            if reference.snapshot_id.is_none() {
                reference.snapshot_id = manifest_location.added_snapshot_id;
            }
            if reference.sequence_number.is_none() {
                reference.sequence_number = manifest_location.sequence_number;
            }
            if reference.file_sequence_number.is_none() {
                reference.file_sequence_number = manifest_location.sequence_number;
            }
            validate_manifest_data_file_reference(metadata_backend, bucket, namespace, table, entry, &reference).await?;
            references.push(reference);
        }
        manifests.push(SnapshotManifestReferences {
            location: manifest_location,
            references,
        });
    }
    Ok(manifests)
}

#[derive(Debug)]
struct SnapshotManifestLocation {
    manifest_path: String,
    sequence_number: Option<i64>,
    added_snapshot_id: Option<i64>,
}

async fn snapshot_manifest_locations<B>(
    metadata_backend: &B,
    bucket: &str,
    namespace: &crate::table_catalog::Namespace,
    table: &crate::table_catalog::IdentifierSegment,
    entry: &crate::table_catalog::TableEntry,
    snapshot: &serde_json::Value,
) -> S3Result<Vec<SnapshotManifestLocation>>
where
    B: crate::table_catalog::TableCatalogObjectBackend,
{
    if let Some(manifest_list_location) = snapshot.get("manifest-list").and_then(serde_json::Value::as_str) {
        let manifest_list_key = table_commit_object_key(
            bucket,
            namespace,
            table,
            entry,
            manifest_list_location,
            crate::table_catalog::TableMetadataMaintenanceObjectKind::ManifestList,
        )?;
        let manifest_list_object = metadata_backend
            .read_object(bucket, &manifest_list_key)
            .await
            .map_err(catalog_store_error)?
            .ok_or_else(|| s3_error!(InvalidRequest, "snapshot manifest-list object is missing"))?;
        let references = crate::table_catalog::manifest_list_references_from_manifest_list_avro(&manifest_list_object.data)
            .map_err(catalog_store_error)?;
        if references.is_empty() {
            return Err(s3_error!(InvalidRequest, "snapshot manifest-list must reference at least one manifest"));
        }
        return Ok(references
            .into_iter()
            .map(|reference| SnapshotManifestLocation {
                manifest_path: reference.manifest_path,
                sequence_number: reference.sequence_number,
                added_snapshot_id: reference.added_snapshot_id,
            })
            .collect());
    }

    let Some(manifests) = snapshot.get("manifests").and_then(serde_json::Value::as_array) else {
        return Err(s3_error!(InvalidRequest, "snapshot manifest-list is required"));
    };
    if manifests.is_empty() {
        return Err(s3_error!(InvalidRequest, "snapshot manifests must reference at least one manifest"));
    }
    manifests
        .iter()
        .map(|manifest| {
            manifest
                .as_str()
                .filter(|manifest| !manifest.is_empty())
                .map(|manifest| SnapshotManifestLocation {
                    manifest_path: manifest.to_string(),
                    sequence_number: None,
                    added_snapshot_id: None,
                })
                .ok_or_else(|| s3_error!(InvalidRequest, "snapshot manifest location must be a string"))
        })
        .collect()
}

async fn validate_manifest_data_file_reference<B>(
    metadata_backend: &B,
    bucket: &str,
    namespace: &crate::table_catalog::Namespace,
    table: &crate::table_catalog::IdentifierSegment,
    entry: &crate::table_catalog::TableEntry,
    reference: &crate::table_catalog::ManifestDataFileReference,
) -> S3Result<()>
where
    B: crate::table_catalog::TableCatalogObjectBackend,
{
    table_commit_object_key(bucket, namespace, table, entry, &reference.location, reference.object_kind.clone())?;
    let object_key = crate::table_catalog::table_catalog_object_key_from_location(bucket, &reference.location)
        .ok_or_else(|| s3_error!(InvalidRequest, "manifest data file location is invalid"))?;
    if !metadata_backend
        .object_exists(bucket, &object_key)
        .await
        .map_err(catalog_store_error)?
    {
        return Err(s3_error!(InvalidRequest, "manifest referenced data file is missing"));
    }
    Ok(())
}

fn table_commit_object_key(
    bucket: &str,
    namespace: &crate::table_catalog::Namespace,
    table: &crate::table_catalog::IdentifierSegment,
    entry: &crate::table_catalog::TableEntry,
    location: &str,
    expected_kind: crate::table_catalog::TableMetadataMaintenanceObjectKind,
) -> S3Result<String> {
    let object_key = crate::table_catalog::table_catalog_object_key_from_location(bucket, location)
        .ok_or_else(|| s3_error!(InvalidRequest, "snapshot object location is invalid"))?;
    let warehouse_object_prefix = crate::table_catalog::table_warehouse_object_prefix(entry).map_err(catalog_store_error)?;
    let object_kind =
        crate::table_catalog::table_maintenance_object_kind(namespace, table, Some(&warehouse_object_prefix), &object_key)
            .ok_or_else(|| s3_error!(InvalidRequest, "snapshot object is outside the table warehouse"))?;
    if object_kind != expected_kind {
        return Err(s3_error!(InvalidRequest, "snapshot object kind does not match manifest metadata"));
    }
    Ok(object_key)
}

fn apply_set_snapshot_ref_update(metadata: &mut serde_json::Value, update: &serde_json::Value) -> S3Result<()> {
    let ref_name = update
        .get("ref-name")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| s3_error!(InvalidRequest, "set-snapshot-ref requires ref-name"))?;
    let snapshot_id = update
        .get("snapshot-id")
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| s3_error!(InvalidRequest, "set-snapshot-ref requires snapshot-id"))?;
    let reference = update
        .as_object()
        .ok_or_else(|| s3_error!(InvalidRequest, "set-snapshot-ref must be a JSON object"))?
        .iter()
        .filter(|(key, _)| key.as_str() != "action" && key.as_str() != "ref-name")
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<serde_json::Map<_, _>>();
    ensure_object_field(metadata, "refs")?.insert(ref_name.to_string(), serde_json::Value::Object(reference));
    if ref_name == "main" {
        metadata_object_mut(metadata)?.insert("current-snapshot-id".to_string(), serde_json::Value::from(snapshot_id));
    }
    Ok(())
}

fn apply_remove_snapshots_update(metadata: &mut serde_json::Value, update: &serde_json::Value) -> S3Result<()> {
    let ids = update
        .get("snapshot-ids")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| s3_error!(InvalidRequest, "remove-snapshots requires snapshot-ids"))?
        .iter()
        .filter_map(serde_json::Value::as_i64)
        .collect::<std::collections::BTreeSet<_>>();
    ensure_array_field(metadata, "snapshots")?.retain(|snapshot| {
        snapshot
            .get("snapshot-id")
            .and_then(serde_json::Value::as_i64)
            .is_none_or(|snapshot_id| !ids.contains(&snapshot_id))
    });
    ensure_array_field(metadata, "snapshot-log")?.retain(|log| {
        log.get("snapshot-id")
            .and_then(serde_json::Value::as_i64)
            .is_none_or(|snapshot_id| !ids.contains(&snapshot_id))
    });
    Ok(())
}

fn apply_remove_snapshot_ref_update(metadata: &mut serde_json::Value, update: &serde_json::Value) -> S3Result<()> {
    let ref_name = update
        .get("ref-name")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| s3_error!(InvalidRequest, "remove-snapshot-ref requires ref-name"))?;
    ensure_object_field(metadata, "refs")?.remove(ref_name);
    Ok(())
}

fn apply_set_location_update(metadata: &mut serde_json::Value, update: &serde_json::Value) -> S3Result<()> {
    let location = update
        .get("location")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| s3_error!(InvalidRequest, "set-location requires location"))?;
    metadata_object_mut(metadata)?.insert("location".to_string(), serde_json::Value::String(location.to_string()));
    Ok(())
}

fn apply_set_properties_update(metadata: &mut serde_json::Value, update: &serde_json::Value) -> S3Result<()> {
    let updates = update
        .get("updates")
        .and_then(serde_json::Value::as_object)
        .ok_or_else(|| s3_error!(InvalidRequest, "set-properties requires updates"))?;
    let properties = ensure_object_field(metadata, "properties")?;
    for (key, value) in updates {
        let value = value
            .as_str()
            .ok_or_else(|| s3_error!(InvalidRequest, "table property values must be strings"))?;
        properties.insert(key.clone(), serde_json::Value::String(value.to_string()));
    }
    Ok(())
}

fn apply_remove_properties_update(metadata: &mut serde_json::Value, update: &serde_json::Value) -> S3Result<()> {
    let removals = update
        .get("removals")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| s3_error!(InvalidRequest, "remove-properties requires removals"))?;
    let properties = ensure_object_field(metadata, "properties")?;
    for removal in removals {
        let key = removal
            .as_str()
            .ok_or_else(|| s3_error!(InvalidRequest, "property removals must be strings"))?;
        properties.remove(key);
    }
    Ok(())
}

fn append_previous_metadata_log(metadata: &mut serde_json::Value, previous_metadata_location: &str) -> S3Result<()> {
    ensure_array_field(metadata, "metadata-log")?.push(serde_json::json!({
        "timestamp-ms": current_time_millis(),
        "metadata-file": previous_metadata_location
    }));
    Ok(())
}

fn metadata_object_mut(metadata: &mut serde_json::Value) -> S3Result<&mut serde_json::Map<String, serde_json::Value>> {
    metadata
        .as_object_mut()
        .ok_or_else(|| s3_error!(InvalidRequest, "table metadata must be a JSON object"))
}

fn ensure_array_field<'a>(metadata: &'a mut serde_json::Value, key: &str) -> S3Result<&'a mut Vec<serde_json::Value>> {
    let object = metadata_object_mut(metadata)?;
    object
        .entry(key.to_string())
        .or_insert_with(|| serde_json::Value::Array(Vec::new()));
    object
        .get_mut(key)
        .and_then(serde_json::Value::as_array_mut)
        .ok_or_else(|| s3_error!(InvalidRequest, "metadata field {key} must be an array"))
}

fn ensure_object_field<'a>(
    metadata: &'a mut serde_json::Value,
    key: &str,
) -> S3Result<&'a mut serde_json::Map<String, serde_json::Value>> {
    let object = metadata_object_mut(metadata)?;
    object
        .entry(key.to_string())
        .or_insert_with(|| serde_json::Value::Object(serde_json::Map::new()));
    object
        .get_mut(key)
        .and_then(serde_json::Value::as_object_mut)
        .ok_or_else(|| s3_error!(InvalidRequest, "metadata field {key} must be an object"))
}

fn next_array_object_i64(metadata: &serde_json::Value, array_key: &str, id_key: &str) -> S3Result<i64> {
    Ok(last_array_object_i64(metadata, array_key, id_key)?.saturating_add(1))
}

fn last_array_object_i64(metadata: &serde_json::Value, array_key: &str, id_key: &str) -> S3Result<i64> {
    let values = metadata
        .get(array_key)
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| s3_error!(InvalidRequest, "metadata field {array_key} must be an array"))?;
    values
        .iter()
        .filter_map(|value| value.get(id_key).and_then(serde_json::Value::as_i64))
        .max()
        .ok_or_else(|| s3_error!(InvalidRequest, "metadata field {array_key} has no {id_key}"))
}

fn table_commit_operation(metadata: &serde_json::Value) -> String {
    metadata
        .get("snapshots")
        .and_then(serde_json::Value::as_array)
        .and_then(|snapshots| snapshots.last())
        .and_then(|snapshot| snapshot.get("summary"))
        .and_then(|summary| summary.get("operation"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or("commit")
        .to_string()
}

fn namespace_entry_from_create_request(
    bucket: &str,
    request: CreateNamespaceRequest,
) -> S3Result<crate::table_catalog::NamespaceEntry> {
    let namespace = namespace_from_segments(&request.namespace)?;
    Ok(crate::table_catalog::NamespaceEntry {
        version: crate::table_catalog::TABLE_CATALOG_ENTRY_VERSION,
        table_bucket: bucket.to_string(),
        namespace: namespace.public_name(),
        namespace_id: namespace.storage_id(),
        state: crate::table_catalog::TableCatalogEntryState::Active,
        properties: request.properties,
        created_at: None,
        updated_at: None,
    })
}

fn iceberg_rest_error(error_type: &str, status: StatusCode, message: impl Into<String>) -> S3Error {
    let mut err = S3Error::with_message(S3ErrorCode::Custom(error_type.into()), message.into());
    err.set_status_code(status);
    err
}

fn catalog_store_error(err: crate::table_catalog::TableCatalogStoreError) -> S3Error {
    match err {
        crate::table_catalog::TableCatalogStoreError::NotFound(message) => {
            iceberg_rest_error(ICEBERG_ERROR_NO_SUCH_RESOURCE, StatusCode::NOT_FOUND, message)
        }
        crate::table_catalog::TableCatalogStoreError::Conflict(message) => {
            iceberg_rest_error(ICEBERG_ERROR_COMMIT_FAILED, StatusCode::CONFLICT, message)
        }
        crate::table_catalog::TableCatalogStoreError::Invalid(message) => {
            iceberg_rest_error(ICEBERG_ERROR_BAD_REQUEST, StatusCode::BAD_REQUEST, message)
        }
        crate::table_catalog::TableCatalogStoreError::Internal(message) => {
            iceberg_rest_error(ICEBERG_ERROR_REST, StatusCode::INTERNAL_SERVER_ERROR, message)
        }
    }
}

fn catalog_store_conflict_error(err: crate::table_catalog::TableCatalogStoreError, conflict_type: &'static str) -> S3Error {
    match err {
        crate::table_catalog::TableCatalogStoreError::Conflict(message) => {
            iceberg_rest_error(conflict_type, StatusCode::CONFLICT, message)
        }
        err => catalog_store_error(err),
    }
}

fn catalog_store_already_exists_error(err: crate::table_catalog::TableCatalogStoreError) -> S3Error {
    catalog_store_conflict_error(err, ICEBERG_ERROR_ALREADY_EXISTS)
}

fn catalog_store_namespace_drop_error(err: crate::table_catalog::TableCatalogStoreError) -> S3Error {
    match err {
        crate::table_catalog::TableCatalogStoreError::NotFound(message) => {
            iceberg_rest_error(ICEBERG_ERROR_NO_SUCH_NAMESPACE, StatusCode::NOT_FOUND, message)
        }
        err => catalog_store_conflict_error(err, ICEBERG_ERROR_NAMESPACE_NOT_EMPTY),
    }
}

async fn create_namespace_response<S>(
    store: &S,
    bucket: &str,
    request: CreateNamespaceRequest,
    table_bucket_enabled: bool,
) -> S3Result<RestNamespaceResponse>
where
    S: crate::table_catalog::TableCatalogStore + ?Sized,
{
    let entry = namespace_entry_from_create_request(bucket, request)?;
    ensure_table_bucket_entry(store, bucket, table_bucket_enabled).await?;
    store
        .create_namespace(entry.clone())
        .await
        .map_err(catalog_store_already_exists_error)?;
    namespace_response_from_entry(entry)
}

async fn list_namespaces_response<S>(store: &S, bucket: &str, uri: &http::Uri) -> S3Result<RestListNamespacesResponse>
where
    S: crate::table_catalog::TableCatalogStore + ?Sized,
{
    let context = RestPageContext {
        resource: TABLE_CATALOG_NAMESPACE_RESOURCE_ROOT,
        warehouse: bucket,
        namespace: None,
    };
    let pagination = rest_pagination_from_query(uri, context)?;
    let page = match pagination.page_request() {
        Some((cursor, limit)) => store
            .list_namespaces_page(bucket, cursor, limit)
            .await
            .map_err(catalog_store_error)?,
        None => crate::table_catalog::TableCatalogListPage {
            entries: store.list_namespaces(bucket).await.map_err(catalog_store_error)?,
            next_cursor: None,
        },
    };
    let next_page_token = pagination.next_page_token(page.next_cursor)?;
    list_namespaces_response_from_entries(page.entries, next_page_token)
}

async fn get_namespace_response<S>(
    store: &S,
    bucket: &str,
    namespace: &crate::table_catalog::Namespace,
) -> S3Result<RestNamespaceResponse>
where
    S: crate::table_catalog::TableCatalogStore + ?Sized,
{
    let Some(entry) = store
        .get_namespace(bucket, &namespace.public_name())
        .await
        .map_err(catalog_store_error)?
    else {
        return Err(iceberg_rest_error(
            ICEBERG_ERROR_NO_SUCH_NAMESPACE,
            StatusCode::NOT_FOUND,
            "namespace not found",
        ));
    };
    namespace_response_from_entry(entry)
}

async fn namespace_exists_status<S>(store: &S, bucket: &str, namespace: &crate::table_catalog::Namespace) -> S3Result<StatusCode>
where
    S: crate::table_catalog::TableCatalogStore + ?Sized,
{
    let exists = store
        .get_namespace(bucket, &namespace.public_name())
        .await
        .map_err(catalog_store_error)?
        .is_some();
    Ok(exists_status(exists))
}

async fn drop_namespace_in_store<S>(store: &S, bucket: &str, namespace: &str) -> S3Result<()>
where
    S: crate::table_catalog::TableCatalogStore + ?Sized,
{
    store
        .drop_namespace(bucket, namespace)
        .await
        .map_err(catalog_store_namespace_drop_error)
}

async fn register_table_response<S>(
    store: &S,
    metadata_backend: &impl crate::table_catalog::TableCatalogObjectBackend,
    bucket: &str,
    namespace: &crate::table_catalog::Namespace,
    request: RegisterTableRequest,
    table_bucket_enabled: bool,
) -> S3Result<RestLoadTableResponse>
where
    S: crate::table_catalog::TableCatalogStore + ?Sized,
{
    let mut entry = table_entry_from_register_request(bucket, namespace, request)?;
    ensure_table_bucket_entry(store, bucket, table_bucket_enabled).await?;
    let metadata = read_table_metadata_json(metadata_backend, bucket, &entry.metadata_location).await?;
    validate_metadata_table_location_in_bucket(bucket, &metadata)?;
    adopt_registered_metadata_identity(&mut entry, &metadata)?;
    store
        .register_table(entry.clone())
        .await
        .map_err(catalog_store_already_exists_error)?;
    Ok(load_table_response_from_entry(entry, metadata))
}

async fn create_table_response<S>(
    store: &S,
    metadata_backend: &impl crate::table_catalog::TableCatalogObjectBackend,
    bucket: &str,
    namespace: &crate::table_catalog::Namespace,
    request: CreateTableRequest,
    table_bucket_enabled: bool,
) -> S3Result<RestLoadTableResponse>
where
    S: crate::table_catalog::TableCatalogStore + ?Sized,
{
    let (entry, metadata) = table_entry_from_create_table_request(bucket, namespace, request)?;
    ensure_table_bucket_entry(store, bucket, table_bucket_enabled).await?;
    let metadata_data = serde_json::to_vec(&metadata)
        .map_err(|err| s3_error!(InternalError, "failed to serialize initial table metadata: {}", err))?;
    metadata_backend
        .put_object(
            bucket,
            &entry.metadata_location,
            metadata_data,
            crate::table_catalog::TableCatalogPutPrecondition::IfAbsent,
        )
        .await
        .map_err(catalog_store_already_exists_error)?;
    store
        .create_table(entry.clone())
        .await
        .map_err(catalog_store_already_exists_error)?;
    Ok(load_table_response_from_entry(entry, metadata))
}

async fn create_view_response<S>(
    store: &S,
    metadata_backend: &impl crate::table_catalog::TableCatalogObjectBackend,
    bucket: &str,
    namespace: &crate::table_catalog::Namespace,
    request: CreateViewRequest,
    table_bucket_enabled: bool,
) -> S3Result<RestLoadViewResponse>
where
    S: crate::table_catalog::TableCatalogStore + ?Sized,
{
    let (entry, metadata) = view_entry_from_create_view_request(bucket, namespace, request)?;
    ensure_table_bucket_entry(store, bucket, table_bucket_enabled).await?;
    let metadata_data = serde_json::to_vec(&metadata)
        .map_err(|err| s3_error!(InternalError, "failed to serialize initial view metadata: {}", err))?;
    metadata_backend
        .put_object(
            bucket,
            &entry.metadata_location,
            metadata_data,
            crate::table_catalog::TableCatalogPutPrecondition::IfAbsent,
        )
        .await
        .map_err(catalog_store_already_exists_error)?;
    store
        .create_view(entry.clone())
        .await
        .map_err(catalog_store_already_exists_error)?;
    Ok(load_view_response_from_entry(entry, metadata))
}

async fn read_table_metadata_json(
    metadata_backend: &impl crate::table_catalog::TableCatalogObjectBackend,
    bucket: &str,
    metadata_location: &str,
) -> S3Result<serde_json::Value> {
    let Some(object) = metadata_backend
        .read_object(bucket, metadata_location)
        .await
        .map_err(catalog_store_error)?
    else {
        return Err(s3_error!(InvalidRequest, "table metadata object not found: {metadata_location}"));
    };
    let metadata = serde_json::from_slice::<serde_json::Value>(&object.data)
        .map_err(|err| s3_error!(InvalidRequest, "failed to parse table metadata JSON: {}", err))?;
    if !metadata.is_object() {
        return Err(s3_error!(InvalidRequest, "table metadata JSON must be an object"));
    }
    Ok(metadata)
}

async fn list_tables_response<S>(
    store: &S,
    bucket: &str,
    namespace: &crate::table_catalog::Namespace,
    uri: &http::Uri,
) -> S3Result<RestListTablesResponse>
where
    S: crate::table_catalog::TableCatalogStore + ?Sized,
{
    let namespace = namespace.public_name();
    let context = RestPageContext {
        resource: TABLE_CATALOG_TABLE_RESOURCE_ROOT,
        warehouse: bucket,
        namespace: Some(&namespace),
    };
    let pagination = rest_pagination_from_query(uri, context)?;
    let page = match pagination.page_request() {
        Some((cursor, limit)) => store
            .list_tables_page(bucket, &namespace, cursor, limit)
            .await
            .map_err(catalog_store_error)?,
        None => crate::table_catalog::TableCatalogListPage {
            entries: store.list_tables(bucket, &namespace).await.map_err(catalog_store_error)?,
            next_cursor: None,
        },
    };
    let next_page_token = pagination.next_page_token(page.next_cursor)?;
    list_tables_response_from_entries(page.entries, next_page_token)
}

async fn load_table_response<S>(
    store: &S,
    metadata_backend: &impl crate::table_catalog::TableCatalogObjectBackend,
    bucket: &str,
    namespace: &crate::table_catalog::Namespace,
    table: &str,
) -> S3Result<RestLoadTableResponse>
where
    S: crate::table_catalog::TableCatalogStore + ?Sized,
{
    let Some(entry) = store
        .load_table(bucket, &namespace.public_name(), table)
        .await
        .map_err(catalog_store_error)?
    else {
        return Err(iceberg_rest_error(ICEBERG_ERROR_NO_SUCH_TABLE, StatusCode::NOT_FOUND, "table not found"));
    };
    let metadata = read_table_metadata_json(metadata_backend, bucket, &entry.metadata_location).await?;
    Ok(load_table_response_from_entry(entry, metadata))
}

async fn list_views_response<S>(
    store: &S,
    bucket: &str,
    namespace: &crate::table_catalog::Namespace,
    uri: &http::Uri,
) -> S3Result<RestListViewsResponse>
where
    S: crate::table_catalog::TableCatalogStore + ?Sized,
{
    let namespace = namespace.public_name();
    let context = RestPageContext {
        resource: TABLE_CATALOG_VIEW_RESOURCE_ROOT,
        warehouse: bucket,
        namespace: Some(&namespace),
    };
    let pagination = rest_pagination_from_query(uri, context)?;
    let page = match pagination.page_request() {
        Some((cursor, limit)) => store
            .list_views_page(bucket, &namespace, cursor, limit)
            .await
            .map_err(catalog_store_error)?,
        None => crate::table_catalog::TableCatalogListPage {
            entries: store.list_views(bucket, &namespace).await.map_err(catalog_store_error)?,
            next_cursor: None,
        },
    };
    let next_page_token = pagination.next_page_token(page.next_cursor)?;
    list_views_response_from_entries(page.entries, next_page_token)
}

async fn load_view_response<S>(
    store: &S,
    metadata_backend: &impl crate::table_catalog::TableCatalogObjectBackend,
    bucket: &str,
    namespace: &crate::table_catalog::Namespace,
    view: &str,
) -> S3Result<RestLoadViewResponse>
where
    S: crate::table_catalog::TableCatalogStore + ?Sized,
{
    let Some(entry) = store
        .load_view(bucket, &namespace.public_name(), view)
        .await
        .map_err(catalog_store_error)?
    else {
        return Err(iceberg_rest_error(ICEBERG_ERROR_NO_SUCH_VIEW, StatusCode::NOT_FOUND, "view not found"));
    };
    let metadata = read_table_metadata_json(metadata_backend, bucket, &entry.metadata_location).await?;
    Ok(load_view_response_from_entry(entry, metadata))
}

async fn view_exists_status<S>(
    store: &S,
    bucket: &str,
    namespace: &crate::table_catalog::Namespace,
    view: &str,
) -> S3Result<StatusCode>
where
    S: crate::table_catalog::TableCatalogStore + ?Sized,
{
    let exists = store
        .load_view(bucket, &namespace.public_name(), view)
        .await
        .map_err(catalog_store_error)?
        .is_some();
    Ok(exists_status(exists))
}

async fn replace_view_response<S>(
    store: &S,
    metadata_backend: &impl crate::table_catalog::TableCatalogObjectBackend,
    bucket: &str,
    namespace: &crate::table_catalog::Namespace,
    view: &str,
    request: RestCommitViewRequest,
) -> S3Result<RestLoadViewResponse>
where
    S: crate::table_catalog::TableCatalogStore + ?Sized,
{
    let Some(current) = store
        .load_view(bucket, &namespace.public_name(), view)
        .await
        .map_err(catalog_store_error)?
    else {
        return Err(iceberg_rest_error(ICEBERG_ERROR_NO_SUCH_VIEW, StatusCode::NOT_FOUND, "view not found"));
    };
    let current_metadata = read_table_metadata_json(metadata_backend, bucket, &current.metadata_location).await?;
    validate_view_commit_requirements(&current_metadata, &request.requirements)?;
    let view_name = crate::table_catalog::IdentifierSegment::parse(view.to_string())
        .map_err(|err| s3_error!(InvalidRequest, "invalid view name: {}", err))?;
    let (next_metadata_location, next_metadata) = if let Some(new_metadata_location) = request.new_metadata_location {
        if !crate::table_catalog::is_valid_view_metadata_location(namespace, &view_name, &new_metadata_location) {
            return Err(s3_error!(InvalidRequest, "metadata location must be inside the view metadata directory"));
        }
        let target_metadata = read_table_metadata_json(metadata_backend, bucket, &new_metadata_location).await?;
        validate_metadata_view_location_in_bucket(bucket, &target_metadata)?;
        validate_metadata_matches_current_view_metadata(&current_metadata, &target_metadata)?;
        (new_metadata_location, target_metadata)
    } else {
        let next_metadata = apply_view_commit_updates(current_metadata.clone(), &request.updates, &current.metadata_location)?;
        validate_metadata_view_location_in_bucket(bucket, &next_metadata)?;
        validate_metadata_matches_current_view_metadata(&current_metadata, &next_metadata)?;
        let (_, metadata_file_token) = standard_commit_ids(request.commit_id);
        let next_generation = current.generation.saturating_add(1);
        let next_metadata_location = crate::table_catalog::default_view_metadata_file_path(
            namespace,
            &view_name,
            &next_metadata_file_name(next_generation, &metadata_file_token),
        );
        let next_metadata_data = serde_json::to_vec(&next_metadata)
            .map_err(|err| s3_error!(InternalError, "failed to serialize view metadata update: {}", err))?;
        metadata_backend
            .put_object(
                bucket,
                &next_metadata_location,
                next_metadata_data,
                crate::table_catalog::TableCatalogPutPrecondition::IfAbsent,
            )
            .await
            .map_err(catalog_store_error)?;
        (next_metadata_location, next_metadata)
    };

    let result = store
        .replace_view(crate::table_catalog::ViewCommitRequest {
            table_bucket: bucket.to_string(),
            namespace: namespace.public_name(),
            view: view.to_string(),
            expected_version_token: request
                .expected_version_token
                .unwrap_or_else(|| current.version_token.clone()),
            expected_metadata_location: request
                .expected_metadata_location
                .unwrap_or_else(|| current.metadata_location.clone()),
            new_metadata_location: next_metadata_location,
        })
        .await
        .map_err(catalog_store_error)?;
    Ok(load_view_response_from_entry(result.view, next_metadata))
}

async fn table_exists_status<S>(
    store: &S,
    bucket: &str,
    namespace: &crate::table_catalog::Namespace,
    table: &str,
) -> S3Result<StatusCode>
where
    S: crate::table_catalog::TableCatalogStore + ?Sized,
{
    let exists = store
        .load_table(bucket, &namespace.public_name(), table)
        .await
        .map_err(catalog_store_error)?
        .is_some();
    Ok(exists_status(exists))
}

async fn load_credentials_response<S>(
    store: &S,
    bucket: &str,
    namespace: &crate::table_catalog::Namespace,
    table: &str,
    issuer: &dyn TableCredentialIssuer,
    principal: Option<&rustfs_credentials::Credentials>,
) -> S3Result<RestLoadCredentialsResponse>
where
    S: crate::table_catalog::TableCatalogStore + ?Sized,
{
    let Some(entry) = store
        .load_table(bucket, &namespace.public_name(), table)
        .await
        .map_err(catalog_store_error)?
    else {
        return Err(iceberg_rest_error(ICEBERG_ERROR_NO_SUCH_TABLE, StatusCode::NOT_FOUND, "table not found"));
    };
    load_credentials_response_from_entry(&entry, issuer, principal).await
}

async fn get_table_metadata_location_response<S>(
    store: &S,
    bucket: &str,
    namespace: &crate::table_catalog::Namespace,
    table: &str,
) -> S3Result<TableMetadataLocationResponse>
where
    S: crate::table_catalog::TableCatalogStore + ?Sized,
{
    let Some(entry) = store
        .load_table(bucket, &namespace.public_name(), table)
        .await
        .map_err(catalog_store_error)?
    else {
        return Err(iceberg_rest_error(ICEBERG_ERROR_NO_SUCH_TABLE, StatusCode::NOT_FOUND, "table not found"));
    };
    Ok(table_metadata_location_response_from_entry(entry))
}

async fn update_table_metadata_location_response<S>(
    store: &S,
    metadata_backend: &impl crate::table_catalog::TableCatalogObjectBackend,
    bucket: &str,
    namespace: &crate::table_catalog::Namespace,
    table: &str,
    request: UpdateTableMetadataLocationRequest,
) -> S3Result<TableMetadataLocationResponse>
where
    S: crate::table_catalog::TableCatalogStore + ?Sized,
{
    let Some(current) = store
        .load_table(bucket, &namespace.public_name(), table)
        .await
        .map_err(catalog_store_error)?
    else {
        return Err(iceberg_rest_error(ICEBERG_ERROR_NO_SUCH_TABLE, StatusCode::NOT_FOUND, "table not found"));
    };
    let table_name = crate::table_catalog::IdentifierSegment::parse(table.to_string())
        .map_err(|err| s3_error!(InvalidRequest, "invalid table name: {}", err))?;
    let metadata_location = table_metadata_location_for_catalog(bucket, &request.metadata_location)?;
    if !crate::table_catalog::is_valid_table_metadata_location(namespace, &table_name, &metadata_location) {
        return Err(s3_error!(InvalidRequest, "metadata location must be inside the table metadata directory"));
    }
    let current_metadata = read_table_metadata_json(metadata_backend, bucket, &current.metadata_location).await?;
    validate_metadata_table_location_in_bucket(bucket, &current_metadata)?;
    let target_metadata = read_table_metadata_json(metadata_backend, bucket, &metadata_location).await?;
    validate_metadata_table_location_in_bucket(bucket, &target_metadata)?;
    validate_metadata_matches_current_metadata(&current_metadata, &target_metadata)?;
    let commit_request = crate::table_catalog::TableCommitRequest {
        table_bucket: bucket.to_string(),
        namespace: namespace.public_name(),
        table: table.to_string(),
        commit_id: request.commit_id.unwrap_or_else(|| Uuid::new_v4().to_string()),
        idempotency_key: request.idempotency_key,
        operation: "update-metadata-location".to_string(),
        expected_version_token: request.version_token,
        expected_metadata_location: current.metadata_location,
        new_metadata_location: metadata_location,
        requirements: Vec::new(),
        writer: Some("rustfs-metadata-location-api".to_string()),
    };
    let result = store.commit_table(commit_request).await.map_err(catalog_store_error)?;
    Ok(table_metadata_location_response_from_entry(result.table))
}

async fn commit_table_response<S>(
    store: &S,
    metadata_backend: &impl crate::table_catalog::TableCatalogObjectBackend,
    bucket: &str,
    namespace: &crate::table_catalog::Namespace,
    table: &str,
    request: RestCommitTableRequest,
) -> S3Result<RestCommitTableResponse>
where
    S: crate::table_catalog::TableCatalogStore + ?Sized,
{
    if request.new_metadata_location.is_none() {
        return standard_commit_table_response(store, metadata_backend, bucket, namespace, table, request).await;
    }

    let request = table_commit_request_from_rest_request(bucket, namespace, table, request)?;
    let Some(current) = store
        .load_table(bucket, &namespace.public_name(), table)
        .await
        .map_err(catalog_store_error)?
    else {
        return Err(iceberg_rest_error(ICEBERG_ERROR_NO_SUCH_TABLE, StatusCode::NOT_FOUND, "table not found"));
    };
    let table_name = crate::table_catalog::IdentifierSegment::parse(table.to_string())
        .map_err(|err| s3_error!(InvalidRequest, "invalid table name: {}", err))?;
    if !crate::table_catalog::is_valid_table_metadata_location(namespace, &table_name, &request.new_metadata_location) {
        return Err(s3_error!(InvalidRequest, "metadata location must be inside the table metadata directory"));
    }
    let current_metadata = read_table_metadata_json(metadata_backend, bucket, &current.metadata_location).await?;
    validate_metadata_table_location_in_bucket(bucket, &current_metadata)?;
    let target_metadata = read_table_metadata_json(metadata_backend, bucket, &request.new_metadata_location).await?;
    validate_metadata_table_location_in_bucket(bucket, &target_metadata)?;
    validate_metadata_matches_current_metadata(&current_metadata, &target_metadata)?;
    let result = store.commit_table(request).await.map_err(catalog_store_error)?;
    Ok(commit_table_response_from_result(result, target_metadata))
}

async fn standard_commit_table_response<S>(
    store: &S,
    metadata_backend: &impl crate::table_catalog::TableCatalogObjectBackend,
    bucket: &str,
    namespace: &crate::table_catalog::Namespace,
    table: &str,
    request: RestCommitTableRequest,
) -> S3Result<RestCommitTableResponse>
where
    S: crate::table_catalog::TableCatalogStore + ?Sized,
{
    let Some(current) = store
        .load_table(bucket, &namespace.public_name(), table)
        .await
        .map_err(catalog_store_error)?
    else {
        return Err(iceberg_rest_error(ICEBERG_ERROR_NO_SUCH_TABLE, StatusCode::NOT_FOUND, "table not found"));
    };
    let table_name = crate::table_catalog::IdentifierSegment::parse(table.to_string())
        .map_err(|err| s3_error!(InvalidRequest, "invalid table name: {}", err))?;
    let current_metadata = read_table_metadata_json(metadata_backend, bucket, &current.metadata_location).await?;
    validate_table_commit_requirements(&current_metadata, &request.requirements)?;
    let expected_metadata = current_metadata.clone();
    let previous_metadata_location = table_metadata_location_for_client(bucket, &current.metadata_location);
    let next_metadata = apply_table_commit_updates(current_metadata, &request.updates, &previous_metadata_location)?;
    validate_metadata_table_location_in_bucket(bucket, &next_metadata)?;
    validate_metadata_matches_current_metadata(&expected_metadata, &next_metadata)?;
    validate_table_snapshot_commit_conflicts(
        metadata_backend,
        bucket,
        namespace,
        &table_name,
        &current,
        &expected_metadata,
        &request.updates,
    )
    .await?;
    let (commit_id, metadata_file_token) = standard_commit_ids(request.commit_id);
    let next_generation = current.generation.saturating_add(1);
    let next_metadata_location = crate::table_catalog::default_table_metadata_file_path(
        namespace,
        &table_name,
        &next_metadata_file_name(next_generation, &metadata_file_token),
    );
    let next_metadata_data = serde_json::to_vec(&next_metadata)
        .map_err(|err| s3_error!(InternalError, "failed to serialize table metadata update: {}", err))?;
    metadata_backend
        .put_object(
            bucket,
            &next_metadata_location,
            next_metadata_data,
            crate::table_catalog::TableCatalogPutPrecondition::IfAbsent,
        )
        .await
        .map_err(catalog_store_error)?;

    let commit_request = crate::table_catalog::TableCommitRequest {
        table_bucket: bucket.to_string(),
        namespace: namespace.public_name(),
        table: table.to_string(),
        commit_id,
        idempotency_key: request.idempotency_key,
        operation: request.operation.unwrap_or_else(|| table_commit_operation(&next_metadata)),
        expected_version_token: current.version_token,
        expected_metadata_location: current.metadata_location,
        new_metadata_location: next_metadata_location,
        requirements: request.requirements,
        writer: request.writer,
    };
    let result = store.commit_table(commit_request).await.map_err(catalog_store_error)?;
    Ok(commit_table_response_from_result(result, next_metadata))
}

async fn drop_table_in_store<S>(store: &S, bucket: &str, namespace: &crate::table_catalog::Namespace, table: &str) -> S3Result<()>
where
    S: crate::table_catalog::TableCatalogStore + ?Sized,
{
    store
        .drop_table(bucket, &namespace.public_name(), table)
        .await
        .map_err(catalog_store_error)
}

async fn drop_view_in_store<S>(store: &S, bucket: &str, namespace: &crate::table_catalog::Namespace, view: &str) -> S3Result<()>
where
    S: crate::table_catalog::TableCatalogStore + ?Sized,
{
    store
        .drop_view(bucket, &namespace.public_name(), view)
        .await
        .map_err(catalog_store_error)
}

async fn table_metadata_maintenance_response<B>(
    store: &crate::table_catalog::ObjectTableCatalogStore<B>,
    metadata_backend: &B,
    bucket: &str,
    namespace: &crate::table_catalog::Namespace,
    table: &str,
    request: TableMetadataMaintenanceRequest,
) -> S3Result<crate::table_catalog::TableMetadataMaintenanceReport>
where
    B: crate::table_catalog::TableCatalogObjectBackend,
{
    if request.delete && request.commit_snapshot_expiration {
        return Err(s3_error!(
            InvalidRequest,
            "snapshot expiration commit cannot be combined with metadata deletion"
        ));
    }
    if request.delete && request.commit_compaction {
        return Err(s3_error!(InvalidRequest, "compaction commit cannot be combined with metadata deletion"));
    }
    if request.commit_snapshot_expiration && request.commit_compaction {
        return Err(s3_error!(
            InvalidRequest,
            "compaction commit cannot be combined with snapshot expiration commit"
        ));
    }
    if request.commit_compaction && request.compaction.is_none() {
        return Err(s3_error!(InvalidRequest, "commit-compaction requires a compaction request"));
    }

    let snapshot_expiration_request = request.snapshot_expiration;
    let commit_snapshot_expiration = request.commit_snapshot_expiration;
    let compaction_request = request.compaction;
    let commit_compaction = request.commit_compaction;
    let compaction = match compaction_request {
        Some(config) if commit_compaction => Some(
            store
                .commit_table_compaction(bucket, &namespace.public_name(), table, config)
                .await
                .map_err(catalog_store_error)?,
        ),
        Some(config) => Some(
            store
                .plan_table_compaction(bucket, &namespace.public_name(), table, config)
                .await
                .map_err(catalog_store_error)?,
        ),
        None => None,
    };
    let snapshot_expiration_plan = match snapshot_expiration_request {
        Some(config) => Some(
            store
                .plan_table_snapshot_expiration(bucket, &namespace.public_name(), table, config)
                .await
                .map_err(catalog_store_error)?,
        ),
        None => None,
    };
    let mut report = store
        .run_table_metadata_maintenance_with_retention(
            bucket,
            &namespace.public_name(),
            table,
            request.delete,
            Some("rustfs-admin".to_string()),
            request.retain_recent_metadata_files,
        )
        .await
        .map_err(catalog_store_error)?;
    let snapshot_expiration = match (snapshot_expiration_plan, commit_snapshot_expiration) {
        (Some(plan), true) => {
            Some(commit_table_snapshot_expiration_response(store, metadata_backend, bucket, namespace, table, plan).await?)
        }
        (Some(plan), false) => Some(plan),
        (None, _) => None,
    };
    report.snapshot_expiration = snapshot_expiration;
    report.compaction = compaction;
    if report.snapshot_expiration.is_some() || report.compaction.is_some() {
        let committed_snapshot_expiration = report
            .snapshot_expiration
            .as_ref()
            .is_some_and(|snapshot_expiration| snapshot_expiration.committed_metadata_location.is_some());
        let committed_compaction = report
            .compaction
            .as_ref()
            .is_some_and(|compaction| compaction.committed_metadata_location.is_some());
        match store.put_table_metadata_maintenance_report(&report).await {
            Ok(()) => {}
            Err(err) if committed_snapshot_expiration || committed_compaction => {
                tracing::warn!(
                    error = %err,
                    warehouse = bucket,
                    namespace = namespace.public_name(),
                    table,
                    "failed to persist table maintenance report after catalog maintenance commit"
                );
            }
            Err(err) => return Err(catalog_store_error(err)),
        }
    }
    Ok(report)
}

async fn commit_table_snapshot_expiration_response<B>(
    store: &crate::table_catalog::ObjectTableCatalogStore<B>,
    metadata_backend: &B,
    bucket: &str,
    namespace: &crate::table_catalog::Namespace,
    table: &str,
    mut report: crate::table_catalog::TableSnapshotExpirationReport,
) -> S3Result<crate::table_catalog::TableSnapshotExpirationReport>
where
    B: crate::table_catalog::TableCatalogObjectBackend,
{
    let Some(current) = store
        .load_table(bucket, &namespace.public_name(), table)
        .await
        .map_err(catalog_store_error)?
    else {
        return Err(iceberg_rest_error(ICEBERG_ERROR_NO_SUCH_TABLE, StatusCode::NOT_FOUND, "table not found"));
    };
    if report.table_id != current.table_id || report.current_metadata_location != current.metadata_location {
        return Err(s3_error!(PreconditionFailed, "snapshot expiration plan is stale"));
    }
    if report.manual_review_count > 0 {
        return Err(s3_error!(InvalidRequest, "snapshot expiration plan requires manual review before commit"));
    }
    let expired_snapshot_ids = report
        .snapshot_reports
        .iter()
        .filter(|snapshot| snapshot.state == crate::table_catalog::TableSnapshotExpirationSnapshotState::ExpirationCandidate)
        .filter_map(|snapshot| snapshot.snapshot_id)
        .collect::<Vec<_>>();
    if expired_snapshot_ids.is_empty() {
        return Ok(report);
    }

    let current_metadata = read_table_metadata_json(metadata_backend, bucket, &current.metadata_location).await?;
    let updates = [serde_json::json!({
        "action": "remove-snapshots",
        "snapshot-ids": expired_snapshot_ids.clone()
    })];
    let previous_metadata_location = table_metadata_location_for_client(bucket, &current.metadata_location);
    let next_metadata = apply_table_commit_updates(current_metadata.clone(), &updates, &previous_metadata_location)?;
    validate_metadata_matches_current_metadata(&current_metadata, &next_metadata)?;
    validate_metadata_table_location_in_bucket(bucket, &next_metadata)?;
    let table_name = crate::table_catalog::IdentifierSegment::parse(table.to_string())
        .map_err(|err| s3_error!(InvalidRequest, "invalid table name: {}", err))?;
    let (commit_id, metadata_file_token) = standard_commit_ids(None);
    let next_generation = current.generation.saturating_add(1);
    let next_metadata_location = crate::table_catalog::default_table_metadata_file_path(
        namespace,
        &table_name,
        &next_metadata_file_name(next_generation, &metadata_file_token),
    );
    let next_metadata_data = serde_json::to_vec(&next_metadata)
        .map_err(|err| s3_error!(InternalError, "failed to serialize snapshot expiration metadata: {}", err))?;
    metadata_backend
        .put_object(
            bucket,
            &next_metadata_location,
            next_metadata_data,
            crate::table_catalog::TableCatalogPutPrecondition::IfAbsent,
        )
        .await
        .map_err(catalog_store_error)?;

    let commit_request = crate::table_catalog::TableCommitRequest {
        table_bucket: bucket.to_string(),
        namespace: namespace.public_name(),
        table: table.to_string(),
        commit_id,
        idempotency_key: None,
        operation: "expire-snapshots".to_string(),
        expected_version_token: current.version_token,
        expected_metadata_location: current.metadata_location,
        new_metadata_location: next_metadata_location,
        requirements: Vec::new(),
        writer: Some("rustfs-maintenance".to_string()),
    };
    let result = store.commit_table(commit_request).await.map_err(catalog_store_error)?;
    report.expired_snapshot_ids = expired_snapshot_ids;
    report.committed_metadata_location = Some(result.table.metadata_location);
    Ok(report)
}

async fn table_refs_response<S>(
    store: &S,
    metadata_backend: &impl crate::table_catalog::TableCatalogObjectBackend,
    bucket: &str,
    namespace: &crate::table_catalog::Namespace,
    table: &str,
) -> S3Result<TableRefsResponse>
where
    S: crate::table_catalog::TableCatalogStore + ?Sized,
{
    let Some(entry) = store
        .load_table(bucket, &namespace.public_name(), table)
        .await
        .map_err(catalog_store_error)?
    else {
        return Err(iceberg_rest_error(ICEBERG_ERROR_NO_SUCH_TABLE, StatusCode::NOT_FOUND, "table not found"));
    };
    let metadata = read_table_metadata_json(metadata_backend, bucket, &entry.metadata_location).await?;
    let current_snapshot_id = metadata.get("current-snapshot-id").and_then(serde_json::Value::as_i64);
    let refs = metadata
        .get("refs")
        .and_then(serde_json::Value::as_object)
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .collect::<BTreeMap<_, _>>();
    let protected_ref_count = refs
        .values()
        .filter(|reference| {
            reference
                .get("snapshot-id")
                .and_then(serde_json::Value::as_i64)
                .is_some_and(|snapshot_id| Some(snapshot_id) != current_snapshot_id)
        })
        .count();
    let user_defined_ref_count = refs.keys().filter(|name| name.as_str() != "main").count();

    Ok(TableRefsResponse {
        table_bucket: bucket.to_string(),
        namespace: namespace.public_name(),
        table: table.to_string(),
        current_metadata_location: entry.metadata_location,
        current_snapshot_id,
        protected_ref_count,
        user_defined_ref_count,
        refs,
    })
}

async fn put_table_ref_response<S>(
    store: &S,
    metadata_backend: &impl crate::table_catalog::TableCatalogObjectBackend,
    bucket: &str,
    namespace: &crate::table_catalog::Namespace,
    table: &str,
    ref_name: &str,
    request: PutTableRefRequest,
) -> S3Result<RestCommitTableResponse>
where
    S: crate::table_catalog::TableCatalogStore + ?Sized,
{
    if !matches!(request.ref_type.as_str(), "branch" | "tag") {
        return Err(s3_error!(InvalidRequest, "snapshot ref type must be branch or tag"));
    }
    let mut update = serde_json::json!({
        "action": "set-snapshot-ref",
        "ref-name": ref_name,
        "type": request.ref_type,
        "snapshot-id": request.snapshot_id
    });
    if let Some(value) = request.min_snapshots_to_keep {
        update["min-snapshots-to-keep"] = serde_json::Value::from(value);
    }
    if let Some(value) = request.max_snapshot_age_ms {
        update["max-snapshot-age-ms"] = serde_json::Value::from(value);
    }
    if let Some(value) = request.max_ref_age_ms {
        update["max-ref-age-ms"] = serde_json::Value::from(value);
    }
    let mut requirements = Vec::new();
    if let Some(expected_snapshot_id) = request.expected_snapshot_id {
        requirements.push(serde_json::json!({
            "type": "assert-ref-snapshot-id",
            "ref": ref_name,
            "snapshot-id": expected_snapshot_id
        }));
    }
    standard_commit_table_response(
        store,
        metadata_backend,
        bucket,
        namespace,
        table,
        RestCommitTableRequest {
            _identifier: None,
            commit_id: request.commit_id,
            idempotency_key: request.idempotency_key,
            operation: Some("set-snapshot-ref".to_string()),
            expected_version_token: None,
            expected_metadata_location: None,
            new_metadata_location: None,
            requirements,
            updates: vec![update],
            writer: request.writer.or_else(|| Some("rustfs-ref-api".to_string())),
        },
    )
    .await
}

async fn delete_table_ref_response<S>(
    store: &S,
    metadata_backend: &impl crate::table_catalog::TableCatalogObjectBackend,
    bucket: &str,
    namespace: &crate::table_catalog::Namespace,
    table: &str,
    ref_name: &str,
    request: DeleteTableRefRequest,
) -> S3Result<RestCommitTableResponse>
where
    S: crate::table_catalog::TableCatalogStore + ?Sized,
{
    if ref_name == "main" {
        return Err(s3_error!(InvalidRequest, "main snapshot ref cannot be deleted"));
    }
    let Some(entry) = store
        .load_table(bucket, &namespace.public_name(), table)
        .await
        .map_err(catalog_store_error)?
    else {
        return Err(iceberg_rest_error(ICEBERG_ERROR_NO_SUCH_TABLE, StatusCode::NOT_FOUND, "table not found"));
    };
    let metadata = read_table_metadata_json(metadata_backend, bucket, &entry.metadata_location).await?;
    let reference = metadata
        .get("refs")
        .and_then(serde_json::Value::as_object)
        .and_then(|refs| refs.get(ref_name));
    if reference.is_some_and(snapshot_ref_has_explicit_retention) && !request.force {
        return Err(s3_error!(InvalidRequest, "snapshot ref has retention policy; force is required"));
    }
    let mut requirements = Vec::new();
    if let Some(expected_snapshot_id) = request.expected_snapshot_id {
        requirements.push(serde_json::json!({
            "type": "assert-ref-snapshot-id",
            "ref": ref_name,
            "snapshot-id": expected_snapshot_id
        }));
    }
    standard_commit_table_response(
        store,
        metadata_backend,
        bucket,
        namespace,
        table,
        RestCommitTableRequest {
            _identifier: None,
            commit_id: request.commit_id,
            idempotency_key: request.idempotency_key,
            operation: Some("remove-snapshot-ref".to_string()),
            expected_version_token: None,
            expected_metadata_location: None,
            new_metadata_location: None,
            requirements,
            updates: vec![serde_json::json!({
                "action": "remove-snapshot-ref",
                "ref-name": ref_name
            })],
            writer: request.writer.or_else(|| Some("rustfs-ref-api".to_string())),
        },
    )
    .await
}

fn snapshot_ref_has_explicit_retention(reference: &serde_json::Value) -> bool {
    reference.get("min-snapshots-to-keep").is_some()
        || reference.get("max-snapshot-age-ms").is_some()
        || reference.get("max-ref-age-ms").is_some()
}

async fn external_catalog_bridge_response<B>(
    store: &crate::table_catalog::ObjectTableCatalogStore<B>,
    bucket: &str,
    namespace: &crate::table_catalog::Namespace,
    table: &str,
) -> S3Result<ExternalCatalogBridgeResponse>
where
    B: crate::table_catalog::TableCatalogObjectBackend,
{
    let bridge = store
        .get_external_catalog_bridge(bucket, &namespace.public_name(), table)
        .await
        .map_err(catalog_store_error)?;
    Ok(external_catalog_bridge_response_from_entry(bucket, namespace, table, bridge))
}

fn external_catalog_bridge_response_from_entry(
    bucket: &str,
    namespace: &crate::table_catalog::Namespace,
    table: &str,
    bridge: Option<crate::table_catalog::ExternalCatalogBridgeEntry>,
) -> ExternalCatalogBridgeResponse {
    let status = if bridge.is_some() {
        EXTERNAL_CATALOG_BRIDGE_STATUS_CONFIGURED
    } else {
        EXTERNAL_CATALOG_BRIDGE_STATUS_UNCONFIGURED
    };
    ExternalCatalogBridgeResponse {
        table_bucket: bucket.to_string(),
        namespace: namespace.public_name(),
        table: table.to_string(),
        status: status.to_string(),
        supported_import:
            "register/import an existing Iceberg metadata location into the RustFS catalog; operator-supplied external metadata sync is supported"
                .to_string(),
        capabilities: external_catalog_bridge_capabilities(),
        unsupported_bridges: Vec::new(),
        bridge: bridge.map(external_catalog_bridge_state_response),
    }
}

fn external_catalog_bridge_capabilities() -> Vec<ExternalCatalogBridgeCapability> {
    EXTERNAL_CATALOG_BRIDGE_CAPABILITIES
        .iter()
        .map(|catalog| ExternalCatalogBridgeCapability {
            catalog: (*catalog).to_string(),
            status: EXTERNAL_CATALOG_BRIDGE_SUPPORTED_STATUS.to_string(),
            reason: EXTERNAL_CATALOG_BRIDGE_SUPPORTED_REASON.to_string(),
        })
        .collect()
}

fn external_catalog_bridge_state_response(
    entry: crate::table_catalog::ExternalCatalogBridgeEntry,
) -> ExternalCatalogBridgeStateResponse {
    ExternalCatalogBridgeStateResponse {
        catalog: entry.catalog,
        external_catalog_id: entry.external_catalog_id,
        external_namespace: entry.external_namespace,
        external_table: entry.external_table,
        external_table_uuid: entry.external_table_uuid,
        metadata_location: entry.metadata_location,
        external_version_token: entry.external_version_token,
        policy_mode: entry.policy_mode,
        credential_mode: entry.credential_mode,
        sync_mode: entry.sync_mode,
        rollback_strategy: entry.rollback_strategy,
        last_sync_status: entry.last_sync_status,
        last_synced_metadata_location: entry.last_synced_metadata_location,
        properties: entry.properties,
    }
}

fn validate_external_catalog_name(catalog: String) -> S3Result<String> {
    let catalog = catalog.trim().to_ascii_lowercase();
    if EXTERNAL_CATALOG_BRIDGE_CAPABILITIES.contains(&catalog.as_str()) {
        return Ok(catalog);
    }
    Err(s3_error!(InvalidRequest, "unsupported external catalog bridge: {catalog}"))
}

fn validate_external_catalog_field(field_name: &str, value: String) -> S3Result<String> {
    let value = value.trim().to_string();
    if value.is_empty() {
        return Err(s3_error!(InvalidRequest, "{} must not be empty", field_name));
    }
    if value.len() > 512 || value.chars().any(|ch| ch.is_control() || ch == '/' || ch == '\\') {
        return Err(s3_error!(InvalidRequest, "{} contains unsupported characters", field_name));
    }
    Ok(value)
}

fn validate_external_catalog_optional_field(field_name: &str, value: Option<String>) -> S3Result<Option<String>> {
    value
        .map(|value| validate_external_catalog_field(field_name, value))
        .transpose()
}

fn validate_external_catalog_mode(field_name: &str, value: Option<String>, default_value: &str) -> S3Result<String> {
    match value {
        Some(value) if value == default_value => Ok(value),
        Some(_) => Err(s3_error!(
            InvalidRequest,
            "{} must be {} until additional external catalog modes are implemented",
            field_name,
            default_value
        )),
        None => Ok(default_value.to_string()),
    }
}

fn validate_external_catalog_metadata_location(
    namespace: &crate::table_catalog::Namespace,
    table: &str,
    metadata_location: &str,
) -> S3Result<()> {
    let table = crate::table_catalog::IdentifierSegment::parse(table.to_string())
        .map_err(|err| s3_error!(InvalidRequest, "invalid table name: {}", err))?;
    if !crate::table_catalog::is_valid_table_metadata_location(namespace, &table, metadata_location) {
        return Err(s3_error!(InvalidRequest, "metadata location must be inside the table metadata directory"));
    }
    Ok(())
}

fn validate_external_catalog_metadata_uuid(
    request_table_uuid: Option<&str>,
    metadata: &serde_json::Value,
) -> S3Result<Option<String>> {
    let metadata_table_uuid = metadata_table_uuid(metadata)?;
    if let Some(request_table_uuid) = request_table_uuid
        && request_table_uuid != metadata_table_uuid
    {
        return Err(s3_error!(InvalidRequest, "external table uuid does not match table metadata table-uuid"));
    }
    Ok(Some(metadata_table_uuid.to_string()))
}

fn external_catalog_bridge_entry_from_request(
    bucket: &str,
    namespace: &crate::table_catalog::Namespace,
    table: &str,
    request: ExternalCatalogBridgeRequest,
) -> S3Result<crate::table_catalog::ExternalCatalogBridgeEntry> {
    let catalog = validate_external_catalog_name(request.catalog)?;
    let external_catalog_id = validate_external_catalog_optional_field("external catalog id", request.external_catalog_id)?;
    let external_namespace = validate_external_catalog_field("external namespace", request.external_namespace)?;
    let external_table = validate_external_catalog_field("external table", request.external_table)?;
    let external_table_uuid = validate_external_catalog_optional_field("external table uuid", request.external_table_uuid)?;
    let metadata_location = request
        .metadata_location
        .map(|metadata_location| table_metadata_location_for_catalog(bucket, &metadata_location))
        .transpose()?;
    if let Some(metadata_location) = metadata_location.as_deref() {
        validate_external_catalog_metadata_location(namespace, table, metadata_location)?;
    }
    Ok(crate::table_catalog::ExternalCatalogBridgeEntry {
        version: crate::table_catalog::TABLE_EXTERNAL_CATALOG_BRIDGE_VERSION,
        table_bucket: bucket.to_string(),
        namespace: namespace.public_name(),
        table: table.to_string(),
        catalog,
        external_catalog_id,
        external_namespace,
        external_table,
        external_table_uuid,
        metadata_location,
        external_version_token: validate_external_catalog_optional_field(
            "external version token",
            request.external_version_token,
        )?,
        policy_mode: validate_external_catalog_mode("policy mode", request.policy_mode, EXTERNAL_CATALOG_POLICY_MODE_RUSTFS)?,
        credential_mode: validate_external_catalog_mode(
            "credential mode",
            request.credential_mode,
            EXTERNAL_CATALOG_CREDENTIAL_MODE_RUSTFS,
        )?,
        sync_mode: validate_external_catalog_mode("sync mode", request.sync_mode, EXTERNAL_CATALOG_SYNC_MODE_MANUAL)?,
        rollback_strategy: EXTERNAL_CATALOG_ROLLBACK_RETAIN_CURRENT.to_string(),
        last_sync_status: None,
        last_synced_metadata_location: None,
        properties: request.properties,
        created_at: None,
        updated_at: None,
    })
}

fn external_catalog_bridge_entry_from_sync_request(
    bucket: &str,
    namespace: &crate::table_catalog::Namespace,
    table: &str,
    request: &ExternalCatalogBridgeSyncRequest,
    external_table_uuid: Option<String>,
) -> S3Result<crate::table_catalog::ExternalCatalogBridgeEntry> {
    let catalog = validate_external_catalog_name(request.catalog.clone())?;
    let external_catalog_id =
        validate_external_catalog_optional_field("external catalog id", request.external_catalog_id.clone())?;
    let external_namespace = validate_external_catalog_field("external namespace", request.external_namespace.clone())?;
    let external_table = validate_external_catalog_field("external table", request.external_table.clone())?;
    let external_version_token =
        validate_external_catalog_optional_field("external version token", request.external_version_token.clone())?;
    let rollback_strategy = validate_external_catalog_mode(
        "rollback strategy",
        request.rollback_strategy.clone(),
        EXTERNAL_CATALOG_ROLLBACK_RETAIN_CURRENT,
    )?;
    Ok(crate::table_catalog::ExternalCatalogBridgeEntry {
        version: crate::table_catalog::TABLE_EXTERNAL_CATALOG_BRIDGE_VERSION,
        table_bucket: bucket.to_string(),
        namespace: namespace.public_name(),
        table: table.to_string(),
        catalog,
        external_catalog_id,
        external_namespace,
        external_table,
        external_table_uuid,
        metadata_location: Some(request.metadata_location.clone()),
        external_version_token,
        policy_mode: validate_external_catalog_mode(
            "policy mode",
            request.policy_mode.clone(),
            EXTERNAL_CATALOG_POLICY_MODE_RUSTFS,
        )?,
        credential_mode: validate_external_catalog_mode(
            "credential mode",
            request.credential_mode.clone(),
            EXTERNAL_CATALOG_CREDENTIAL_MODE_RUSTFS,
        )?,
        sync_mode: EXTERNAL_CATALOG_SYNC_MODE_MANUAL.to_string(),
        rollback_strategy,
        last_sync_status: Some(EXTERNAL_CATALOG_BRIDGE_SYNC_STATUS.to_string()),
        last_synced_metadata_location: Some(request.metadata_location.clone()),
        properties: request.properties.clone(),
        created_at: None,
        updated_at: None,
    })
}

async fn put_external_catalog_bridge_response<B>(
    store: &crate::table_catalog::ObjectTableCatalogStore<B>,
    bucket: &str,
    namespace: &crate::table_catalog::Namespace,
    table: &str,
    request: ExternalCatalogBridgeRequest,
) -> S3Result<ExternalCatalogBridgeResponse>
where
    B: crate::table_catalog::TableCatalogObjectBackend,
{
    let entry = external_catalog_bridge_entry_from_request(bucket, namespace, table, request)?;
    let entry = store.put_external_catalog_bridge(entry).await.map_err(catalog_store_error)?;
    Ok(external_catalog_bridge_response_from_entry(bucket, namespace, table, Some(entry)))
}

async fn sync_external_catalog_bridge_response<B>(
    store: &crate::table_catalog::ObjectTableCatalogStore<B>,
    metadata_backend: &B,
    bucket: &str,
    namespace: &crate::table_catalog::Namespace,
    table: &str,
    request: ExternalCatalogBridgeSyncRequest,
    table_bucket_enabled: bool,
) -> S3Result<ExternalCatalogBridgeSyncResponse>
where
    B: crate::table_catalog::TableCatalogObjectBackend,
{
    let mut request = request;
    request.metadata_location = table_metadata_location_for_catalog(bucket, &request.metadata_location)?;
    request.expected_metadata_location = request
        .expected_metadata_location
        .map(|metadata_location| table_metadata_location_for_catalog(bucket, &metadata_location))
        .transpose()?;
    ensure_table_bucket_entry(store, bucket, table_bucket_enabled).await?;
    validate_external_catalog_metadata_location(namespace, table, &request.metadata_location)?;
    let target_metadata = read_table_metadata_json(metadata_backend, bucket, &request.metadata_location).await?;
    validate_metadata_table_location_in_bucket(bucket, &target_metadata)?;
    let external_table_uuid = validate_external_catalog_metadata_uuid(request.external_table_uuid.as_deref(), &target_metadata)?;

    let (action, table_response) = if let Some(current) = store
        .load_table(bucket, &namespace.public_name(), table)
        .await
        .map_err(catalog_store_error)?
    {
        let expected_version_token = request
            .expected_version_token
            .clone()
            .ok_or_else(|| s3_error!(InvalidRequest, "external catalog sync requires expected-version-token"))?;
        let expected_metadata_location = request
            .expected_metadata_location
            .clone()
            .ok_or_else(|| s3_error!(InvalidRequest, "external catalog sync requires expected-metadata-location"))?;
        let current_metadata = read_table_metadata_json(metadata_backend, bucket, &current.metadata_location).await?;
        validate_metadata_table_location_in_bucket(bucket, &current_metadata)?;
        validate_metadata_matches_current_metadata(&current_metadata, &target_metadata)?;
        let result = store
            .commit_table(crate::table_catalog::TableCommitRequest {
                table_bucket: bucket.to_string(),
                namespace: namespace.public_name(),
                table: table.to_string(),
                commit_id: request.commit_id.clone().unwrap_or_else(|| Uuid::new_v4().to_string()),
                idempotency_key: request.idempotency_key.clone(),
                operation: EXTERNAL_CATALOG_SYNC_OPERATION.to_string(),
                expected_version_token,
                expected_metadata_location,
                new_metadata_location: request.metadata_location.clone(),
                requirements: Vec::new(),
                writer: Some(EXTERNAL_CATALOG_SYNC_WRITER.to_string()),
            })
            .await
            .map_err(catalog_store_error)?;
        (
            EXTERNAL_CATALOG_ACTION_COMMITTED.to_string(),
            load_table_response_from_entry(result.table, target_metadata),
        )
    } else {
        if request.expected_version_token.is_some() || request.expected_metadata_location.is_some() {
            return Err(s3_error!(
                InvalidRequest,
                "external catalog sync cannot use expected table state when registering a missing table"
            ));
        }
        let mut entry = table_entry_from_import_request(
            bucket,
            namespace,
            table,
            CatalogImportRequest {
                metadata_location: request.metadata_location.clone(),
                properties: request.properties.clone(),
            },
        )?;
        adopt_registered_metadata_identity(&mut entry, &target_metadata)?;
        store.register_table(entry.clone()).await.map_err(catalog_store_error)?;
        (
            EXTERNAL_CATALOG_ACTION_REGISTERED.to_string(),
            load_table_response_from_entry(entry, target_metadata),
        )
    };

    let bridge_entry = external_catalog_bridge_entry_from_sync_request(bucket, namespace, table, &request, external_table_uuid)?;
    let bridge_entry = store
        .put_external_catalog_bridge(bridge_entry)
        .await
        .map_err(catalog_store_error)?;
    Ok(ExternalCatalogBridgeSyncResponse {
        action,
        table: table_response,
        bridge: external_catalog_bridge_response_from_entry(bucket, namespace, table, Some(bridge_entry)),
    })
}

async fn catalog_import_response<S, B>(
    store: &S,
    metadata_backend: &B,
    bucket: &str,
    namespace: &crate::table_catalog::Namespace,
    table: &str,
    request: CatalogImportRequest,
    table_bucket_enabled: bool,
) -> S3Result<RestLoadTableResponse>
where
    S: crate::table_catalog::TableCatalogStore + ?Sized,
    B: crate::table_catalog::TableCatalogObjectBackend,
{
    let started = Instant::now();
    let result = async {
        ensure_table_bucket_entry(store, bucket, table_bucket_enabled).await?;
        let mut entry = table_entry_from_import_request(bucket, namespace, table, request)?;
        let metadata = read_table_metadata_json(metadata_backend, bucket, &entry.metadata_location).await?;
        validate_metadata_table_location_in_bucket(bucket, &metadata)?;
        adopt_registered_metadata_identity(&mut entry, &metadata)?;
        if let Some(existing) = store
            .load_table(bucket, &namespace.public_name(), table)
            .await
            .map_err(catalog_store_error)?
        {
            if existing.table_uuid == entry.table_uuid
                && existing.metadata_location == entry.metadata_location
                && existing.warehouse_location == entry.warehouse_location
            {
                return Ok(load_table_response_from_entry(existing, metadata));
            }
            return Err(s3_error!(
                PreconditionFailed,
                "catalog import target already exists with different table identity or metadata pointer"
            ));
        }
        store.register_table(entry.clone()).await.map_err(catalog_store_error)?;
        Ok(load_table_response_from_entry(entry, metadata))
    }
    .await;
    record_table_catalog_admin_operation_result("import", bucket, &namespace.public_name(), table, started, &result);
    result
}

async fn rollback_table_response<S>(
    store: &S,
    metadata_backend: &impl crate::table_catalog::TableCatalogObjectBackend,
    bucket: &str,
    namespace: &crate::table_catalog::Namespace,
    table: &str,
    request: RollbackTableRequest,
) -> S3Result<RestCommitTableResponse>
where
    S: crate::table_catalog::TableCatalogStore + ?Sized,
{
    let started = Instant::now();
    let result = async {
        let Some(current) = store
            .load_table(bucket, &namespace.public_name(), table)
            .await
            .map_err(catalog_store_error)?
        else {
            return Err(iceberg_rest_error(ICEBERG_ERROR_NO_SUCH_TABLE, StatusCode::NOT_FOUND, "table not found"));
        };
        let table_name = crate::table_catalog::IdentifierSegment::parse(table.to_string())
            .map_err(|err| s3_error!(InvalidRequest, "invalid table name: {}", err))?;
        let metadata_location = table_metadata_location_for_catalog(bucket, &request.metadata_location)?;
        if !crate::table_catalog::is_valid_table_metadata_location(namespace, &table_name, &metadata_location) {
            return Err(s3_error!(InvalidRequest, "metadata location must be inside the table metadata directory"));
        }
        let current_metadata = read_table_metadata_json(metadata_backend, bucket, &current.metadata_location).await?;
        validate_metadata_table_location_in_bucket(bucket, &current_metadata)?;
        let target_metadata = read_table_metadata_json(metadata_backend, bucket, &metadata_location).await?;
        validate_metadata_table_location_in_bucket(bucket, &target_metadata)?;
        validate_metadata_matches_current_metadata(&current_metadata, &target_metadata)?;
        let commit_request = crate::table_catalog::TableCommitRequest {
            table_bucket: bucket.to_string(),
            namespace: namespace.public_name(),
            table: table.to_string(),
            commit_id: request.commit_id.unwrap_or_else(|| Uuid::new_v4().to_string()),
            idempotency_key: request.idempotency_key,
            operation: "rollback".to_string(),
            expected_version_token: request.version_token,
            expected_metadata_location: current.metadata_location,
            new_metadata_location: metadata_location,
            requirements: Vec::new(),
            writer: Some("rustfs-catalog-rollback-api".to_string()),
        };
        let result = store.commit_table(commit_request).await.map_err(catalog_store_error)?;
        Ok(commit_table_response_from_result(result, target_metadata))
    }
    .await;
    record_table_catalog_admin_operation_result("rollback", bucket, &namespace.public_name(), table, started, &result);
    result
}

pub struct GetCatalogConfigHandler {}

#[async_trait::async_trait]
impl Operation for GetCatalogConfigHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        authorize_table_catalog_request(&req, AdminAction::GetTableCatalogAction).await?;
        let warehouse = warehouse_from_config_query(&req.uri)?;
        build_json_response(StatusCode::OK, &catalog_config_response(warehouse.as_deref())?)
    }
}

pub struct EnableTableBucketHandler {}

#[async_trait::async_trait]
impl Operation for EnableTableBucketHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let resource = TableCatalogResource::warehouse(&warehouse);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::SetTableBucketAction).await?;
        let store = table_catalog_store()?;
        let response = enable_table_bucket_response(&store, &warehouse).await?;
        build_json_response(StatusCode::OK, &response)
    }
}

pub struct GetTableBucketHandler {}

#[async_trait::async_trait]
impl Operation for GetTableBucketHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let resource = TableCatalogResource::warehouse(&warehouse);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::GetTableBucketAction).await?;
        let store = table_catalog_store()?;
        let enabled = table_bucket_enabled_from_metadata(&warehouse).await?;
        let response = table_bucket_response(&store, &warehouse, enabled).await?;
        build_json_response(StatusCode::OK, &response)
    }
}

pub struct GetTableCatalogMigrationHandler {}

#[async_trait::async_trait]
impl Operation for GetTableCatalogMigrationHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let resource = TableCatalogResource::warehouse(&warehouse);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::GetTableCatalogAction).await?;
        ensure_table_bucket_enabled(&warehouse).await?;
        let store = table_catalog_object_store()?;
        let started = Instant::now();
        let result = store
            .plan_durable_strong_backing_migration(&warehouse)
            .await
            .map_err(catalog_store_error);
        record_table_catalog_admin_operation_result("migration", &warehouse, "", "", started, &result);
        let response = result?;
        build_json_response(StatusCode::OK, &response)
    }
}

pub struct MaterializeTableCatalogMigrationHandler {}

#[async_trait::async_trait]
impl Operation for MaterializeTableCatalogMigrationHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        authorize_table_catalog_request(&req, AdminAction::MigrateTableCatalogAction).await?;
        ensure_table_bucket_enabled(&warehouse).await?;
        let store = table_catalog_object_store()?;
        let started = Instant::now();
        let result = store
            .materialize_durable_strong_backing_migration(&warehouse)
            .await
            .map_err(catalog_store_error);
        record_table_catalog_admin_operation_result("migration-materialize", &warehouse, "", "", started, &result);
        let response = result?;
        build_json_response(StatusCode::OK, &response)
    }
}

pub struct CancelTableCatalogMigrationHandler {}

#[async_trait::async_trait]
impl Operation for CancelTableCatalogMigrationHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        authorize_table_catalog_request(&req, AdminAction::MigrateTableCatalogAction).await?;
        ensure_table_bucket_enabled(&warehouse).await?;
        let store = table_catalog_object_store()?;
        let started = Instant::now();
        let result = store
            .cancel_durable_strong_backing_migration(&warehouse)
            .await
            .map_err(catalog_store_error);
        record_table_catalog_admin_operation_result("migration-cancel", &warehouse, "", "", started, &result);
        let response = result?;
        build_json_response(StatusCode::OK, &response)
    }
}

pub struct RestListNamespacesHandler {}

#[async_trait::async_trait]
impl Operation for RestListNamespacesHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let resource = TableCatalogResource::warehouse(&warehouse);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::GetTableNamespaceAction).await?;
        ensure_table_bucket_enabled(&warehouse).await?;
        let store = table_catalog_store()?;
        let response = list_namespaces_response(&store, &warehouse, &req.uri).await?;
        build_json_response(StatusCode::OK, &response)
    }
}

pub struct RestCreateNamespaceHandler {}

#[async_trait::async_trait]
impl Operation for RestCreateNamespaceHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let resource = TableCatalogResource::warehouse(&warehouse);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::SetTableNamespaceAction).await?;
        let request = read_json_body::<CreateNamespaceRequest>(req.input).await?;
        let store = table_catalog_store()?;
        let table_bucket_enabled = table_bucket_enabled_from_metadata(&warehouse).await?;
        let response = create_namespace_response(&store, &warehouse, request, table_bucket_enabled).await?;
        build_json_response(StatusCode::OK, &response)
    }
}

pub struct RestGetNamespaceHandler {}

#[async_trait::async_trait]
impl Operation for RestGetNamespaceHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let resource = TableCatalogResource::namespace(&warehouse, &namespace);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::GetTableNamespaceAction).await?;
        ensure_table_bucket_enabled(&warehouse).await?;
        let store = table_catalog_store()?;
        let response = get_namespace_response(&store, &warehouse, &namespace).await?;
        build_json_response(StatusCode::OK, &response)
    }
}

pub struct RestDropNamespaceHandler {}

#[async_trait::async_trait]
impl Operation for RestDropNamespaceHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let resource = TableCatalogResource::namespace(&warehouse, &namespace);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::DeleteTableNamespaceAction).await?;
        ensure_table_bucket_enabled(&warehouse).await?;
        let store = table_catalog_store()?;
        drop_namespace_in_store(&store, &warehouse, &namespace.public_name()).await?;
        Ok(empty_response(StatusCode::NO_CONTENT))
    }
}

pub struct RestNamespaceExistsHandler {}

#[async_trait::async_trait]
impl Operation for RestNamespaceExistsHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let resource = TableCatalogResource::namespace(&warehouse, &namespace);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::GetTableNamespaceAction).await?;
        ensure_table_bucket_enabled(&warehouse).await?;
        let store = table_catalog_store()?;
        Ok(empty_response(namespace_exists_status(&store, &warehouse, &namespace).await?))
    }
}

pub struct RestListTablesHandler {}

#[async_trait::async_trait]
impl Operation for RestListTablesHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let resource = TableCatalogResource::namespace(&warehouse, &namespace);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::GetTableAction).await?;
        ensure_table_bucket_enabled(&warehouse).await?;
        let store = table_catalog_store()?;
        let response = list_tables_response(&store, &warehouse, &namespace, &req.uri).await?;
        build_json_response(StatusCode::OK, &response)
    }
}

pub struct RestCreateTableHandler {}

#[async_trait::async_trait]
impl Operation for RestCreateTableHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let resource = TableCatalogResource::namespace(&warehouse, &namespace);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::CreateTableAction).await?;
        let request = read_json_body::<CreateTableRequest>(req.input).await?;
        let metadata_backend = table_catalog_backend()?;
        let store = table_catalog_store_from_backend(metadata_backend.clone())?;
        let table_bucket_enabled = table_bucket_enabled_from_metadata(&warehouse).await?;
        let response =
            create_table_response(&store, &metadata_backend, &warehouse, &namespace, request, table_bucket_enabled).await?;
        build_json_response(StatusCode::OK, &response)
    }
}

pub struct RestRegisterTableHandler {}

#[async_trait::async_trait]
impl Operation for RestRegisterTableHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let resource = TableCatalogResource::namespace(&warehouse, &namespace);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::RegisterTableAction).await?;
        let request = read_json_body::<RegisterTableRequest>(req.input).await?;
        let metadata_backend = table_catalog_backend()?;
        let store = table_catalog_store_from_backend(metadata_backend.clone())?;
        let table_bucket_enabled = table_bucket_enabled_from_metadata(&warehouse).await?;
        let response =
            register_table_response(&store, &metadata_backend, &warehouse, &namespace, request, table_bucket_enabled).await?;
        build_json_response(StatusCode::OK, &response)
    }
}

pub struct RestListViewsHandler {}

#[async_trait::async_trait]
impl Operation for RestListViewsHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let resource = TableCatalogResource::namespace(&warehouse, &namespace);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::GetTableMetadataAction).await?;
        ensure_table_bucket_enabled(&warehouse).await?;
        let store = table_catalog_store()?;
        let response = list_views_response(&store, &warehouse, &namespace, &req.uri).await?;
        build_json_response(StatusCode::OK, &response)
    }
}

pub struct RestCreateViewHandler {}

#[async_trait::async_trait]
impl Operation for RestCreateViewHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let resource = TableCatalogResource::namespace(&warehouse, &namespace);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::CreateTableAction).await?;
        let request = read_json_body::<CreateViewRequest>(req.input).await?;
        let metadata_backend = table_catalog_backend()?;
        let store = table_catalog_store_from_backend(metadata_backend.clone())?;
        let table_bucket_enabled = table_bucket_enabled_from_metadata(&warehouse).await?;
        let response =
            create_view_response(&store, &metadata_backend, &warehouse, &namespace, request, table_bucket_enabled).await?;
        build_json_response(StatusCode::OK, &response)
    }
}

pub struct RestLoadTableHandler {}

#[async_trait::async_trait]
impl Operation for RestLoadTableHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let table = table_name_from_params(&params)?;
        let resource = TableCatalogResource::table(&warehouse, &namespace, &table);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::GetTableMetadataAction).await?;
        ensure_table_bucket_enabled(&warehouse).await?;
        let metadata_backend = table_catalog_backend()?;
        let store = table_catalog_store_from_backend(metadata_backend.clone())?;
        let response = load_table_response(&store, &metadata_backend, &warehouse, &namespace, &table).await?;
        build_json_response(StatusCode::OK, &response)
    }
}

pub struct RestTableExistsHandler {}

#[async_trait::async_trait]
impl Operation for RestTableExistsHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let table = table_name_from_params(&params)?;
        let resource = TableCatalogResource::table(&warehouse, &namespace, &table);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::GetTableAction).await?;
        ensure_table_bucket_enabled(&warehouse).await?;
        let store = table_catalog_store()?;
        Ok(empty_response(table_exists_status(&store, &warehouse, &namespace, &table).await?))
    }
}

pub struct RestLoadCredentialsHandler {}

#[async_trait::async_trait]
impl Operation for RestLoadCredentialsHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let table = table_name_from_params(&params)?;
        let resource = TableCatalogResource::table(&warehouse, &namespace, &table);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::GetTableCredentialsAction).await?;
        ensure_table_bucket_enabled(&warehouse).await?;
        let principal = table_catalog_request_principal(&req).await?;
        let store = table_catalog_store()?;
        let issuer = IamTableCredentialIssuer::from_env();
        let response = load_credentials_response(&store, &warehouse, &namespace, &table, &issuer, Some(&principal)).await?;
        build_json_response(StatusCode::OK, &response)
    }
}

pub struct RestCommitTableHandler {}

#[async_trait::async_trait]
impl Operation for RestCommitTableHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let table = table_name_from_params(&params)?;
        let resource = TableCatalogResource::table(&warehouse, &namespace, &table);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::CommitTableAction).await?;
        ensure_table_bucket_enabled(&warehouse).await?;
        let request = read_json_body::<RestCommitTableRequest>(req.input).await?;
        let metadata_backend = table_catalog_backend()?;
        let store = table_catalog_store_from_backend(metadata_backend.clone())?;
        let response = commit_table_response(&store, &metadata_backend, &warehouse, &namespace, &table, request).await?;
        build_json_response(StatusCode::OK, &response)
    }
}

pub struct RestDropTableHandler {}

#[async_trait::async_trait]
impl Operation for RestDropTableHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let table = table_name_from_params(&params)?;
        let resource = TableCatalogResource::table(&warehouse, &namespace, &table);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::DeleteTableAction).await?;
        ensure_table_bucket_enabled(&warehouse).await?;
        let store = table_catalog_store()?;
        drop_table_in_store(&store, &warehouse, &namespace, &table).await?;
        Ok(empty_response(StatusCode::NO_CONTENT))
    }
}

pub struct RestLoadViewHandler {}

#[async_trait::async_trait]
impl Operation for RestLoadViewHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let view = view_name_from_params(&params)?;
        let resource = TableCatalogResource::view(&warehouse, &namespace, &view);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::GetTableMetadataAction).await?;
        ensure_table_bucket_enabled(&warehouse).await?;
        let metadata_backend = table_catalog_backend()?;
        let store = table_catalog_store_from_backend(metadata_backend.clone())?;
        let response = load_view_response(&store, &metadata_backend, &warehouse, &namespace, &view).await?;
        build_json_response(StatusCode::OK, &response)
    }
}

pub struct RestViewExistsHandler {}

#[async_trait::async_trait]
impl Operation for RestViewExistsHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let view = view_name_from_params(&params)?;
        let resource = TableCatalogResource::view(&warehouse, &namespace, &view);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::GetTableAction).await?;
        ensure_table_bucket_enabled(&warehouse).await?;
        let store = table_catalog_store()?;
        Ok(empty_response(view_exists_status(&store, &warehouse, &namespace, &view).await?))
    }
}

pub struct RestReplaceViewHandler {}

#[async_trait::async_trait]
impl Operation for RestReplaceViewHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let view = view_name_from_params(&params)?;
        let resource = TableCatalogResource::view(&warehouse, &namespace, &view);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::CommitTableAction).await?;
        ensure_table_bucket_enabled(&warehouse).await?;
        let request = read_json_body::<RestCommitViewRequest>(req.input).await?;
        let metadata_backend = table_catalog_backend()?;
        let store = table_catalog_store_from_backend(metadata_backend.clone())?;
        let response = replace_view_response(&store, &metadata_backend, &warehouse, &namespace, &view, request).await?;
        build_json_response(StatusCode::OK, &response)
    }
}

pub struct RestDropViewHandler {}

#[async_trait::async_trait]
impl Operation for RestDropViewHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let view = view_name_from_params(&params)?;
        let resource = TableCatalogResource::view(&warehouse, &namespace, &view);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::DeleteTableAction).await?;
        ensure_table_bucket_enabled(&warehouse).await?;
        let store = table_catalog_store()?;
        drop_view_in_store(&store, &warehouse, &namespace, &view).await?;
        Ok(empty_response(StatusCode::NO_CONTENT))
    }
}

pub struct ListTableRefsHandler {}

#[async_trait::async_trait]
impl Operation for ListTableRefsHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let table = table_name_from_params(&params)?;
        let resource = TableCatalogResource::table(&warehouse, &namespace, &table);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::GetTableMetadataAction).await?;
        ensure_table_bucket_enabled(&warehouse).await?;
        let metadata_backend = table_catalog_backend()?;
        let store = table_catalog_store_from_backend(metadata_backend.clone())?;
        let response = table_refs_response(&store, &metadata_backend, &warehouse, &namespace, &table).await?;
        build_json_response(StatusCode::OK, &response)
    }
}

pub struct PutTableRefHandler {}

#[async_trait::async_trait]
impl Operation for PutTableRefHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let table = table_name_from_params(&params)?;
        let ref_name = ref_name_from_params(&params)?;
        let resource = TableCatalogResource::table(&warehouse, &namespace, &table);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::CommitTableAction).await?;
        ensure_table_bucket_enabled(&warehouse).await?;
        let request = read_json_body::<PutTableRefRequest>(req.input).await?;
        let metadata_backend = table_catalog_backend()?;
        let store = table_catalog_store_from_backend(metadata_backend.clone())?;
        let response =
            put_table_ref_response(&store, &metadata_backend, &warehouse, &namespace, &table, &ref_name, request).await?;
        build_json_response(StatusCode::OK, &response)
    }
}

pub struct DeleteTableRefHandler {}

#[async_trait::async_trait]
impl Operation for DeleteTableRefHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let table = table_name_from_params(&params)?;
        let ref_name = ref_name_from_params(&params)?;
        let resource = TableCatalogResource::table(&warehouse, &namespace, &table);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::CommitTableAction).await?;
        ensure_table_bucket_enabled(&warehouse).await?;
        let request = read_json_body_or_default::<DeleteTableRefRequest>(req.input).await?;
        let metadata_backend = table_catalog_backend()?;
        let store = table_catalog_store_from_backend(metadata_backend.clone())?;
        let response =
            delete_table_ref_response(&store, &metadata_backend, &warehouse, &namespace, &table, &ref_name, request).await?;
        build_json_response(StatusCode::OK, &response)
    }
}

pub struct GetTableMetadataLocationHandler {}

#[async_trait::async_trait]
impl Operation for GetTableMetadataLocationHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let table = table_name_from_params(&params)?;
        let resource = TableCatalogResource::table(&warehouse, &namespace, &table);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::GetTableMetadataLocationAction).await?;
        ensure_table_bucket_enabled(&warehouse).await?;
        let store = table_catalog_store()?;
        let response = get_table_metadata_location_response(&store, &warehouse, &namespace, &table).await?;
        build_json_response(StatusCode::OK, &response)
    }
}

pub struct UpdateTableMetadataLocationHandler {}

#[async_trait::async_trait]
impl Operation for UpdateTableMetadataLocationHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let table = table_name_from_params(&params)?;
        let resource = TableCatalogResource::table(&warehouse, &namespace, &table);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::SetTableMetadataLocationAction).await?;
        ensure_table_bucket_enabled(&warehouse).await?;
        let request = read_json_body::<UpdateTableMetadataLocationRequest>(req.input).await?;
        let metadata_backend = table_catalog_backend()?;
        let store = table_catalog_store_from_backend(metadata_backend.clone())?;
        let response =
            update_table_metadata_location_response(&store, &metadata_backend, &warehouse, &namespace, &table, request).await?;
        build_json_response(StatusCode::OK, &response)
    }
}

pub struct RestTableMetadataMaintenanceHandler {}

#[async_trait::async_trait]
impl Operation for RestTableMetadataMaintenanceHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let table = table_name_from_params(&params)?;
        let resource = TableCatalogResource::table(&warehouse, &namespace, &table);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::RunTableMaintenanceAction).await?;
        ensure_table_bucket_enabled(&warehouse).await?;
        let request = read_json_body::<TableMetadataMaintenanceRequest>(req.input).await?;
        let metadata_backend = table_catalog_backend()?;
        let store = table_catalog_object_store()?;
        let response =
            table_metadata_maintenance_response(&store, &metadata_backend, &warehouse, &namespace, &table, request).await?;
        build_json_response(StatusCode::OK, &response)
    }
}

pub struct GetTableMaintenanceConfigHandler {}

#[async_trait::async_trait]
impl Operation for GetTableMaintenanceConfigHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let table = table_name_from_params(&params)?;
        let resource = TableCatalogResource::table(&warehouse, &namespace, &table);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::GetTableLifecycleAction).await?;
        ensure_table_bucket_enabled(&warehouse).await?;
        let store = table_catalog_store()?;
        let response = store
            .get_table_maintenance_config(&warehouse, &namespace.public_name(), &table)
            .await
            .map_err(catalog_store_error)?;
        build_json_response(StatusCode::OK, &response)
    }
}

pub struct PutTableMaintenanceConfigHandler {}

#[async_trait::async_trait]
impl Operation for PutTableMaintenanceConfigHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let table = table_name_from_params(&params)?;
        let resource = TableCatalogResource::table(&warehouse, &namespace, &table);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::SetTableLifecycleAction).await?;
        ensure_table_bucket_enabled(&warehouse).await?;
        let request = read_json_body::<crate::table_catalog::TableMaintenanceConfig>(req.input).await?;
        let store = table_catalog_store()?;
        let response = store
            .put_table_maintenance_config(&warehouse, &namespace.public_name(), &table, request)
            .await
            .map_err(catalog_store_error)?;
        build_json_response(StatusCode::OK, &response)
    }
}

pub struct GetTableMaintenanceJobHandler {}

#[async_trait::async_trait]
impl Operation for GetTableMaintenanceJobHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let table = table_name_from_params(&params)?;
        let job = job_id_from_params(&params)?;
        let resource = TableCatalogResource::table(&warehouse, &namespace, &table);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::GetTableLifecycleAction).await?;
        ensure_table_bucket_enabled(&warehouse).await?;
        let store = table_catalog_store()?;
        let Some(response) = store
            .get_table_metadata_maintenance_report(&warehouse, &namespace.public_name(), &table, &job)
            .await
            .map_err(catalog_store_error)?
        else {
            return Err(s3_error!(InvalidRequest, "maintenance job not found"));
        };
        build_json_response(StatusCode::OK, &response)
    }
}

pub struct GetTableMaintenanceSchedulerHandler {}

#[async_trait::async_trait]
impl Operation for GetTableMaintenanceSchedulerHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let table = table_name_from_params(&params)?;
        let resource = TableCatalogResource::table(&warehouse, &namespace, &table);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::GetTableLifecycleAction).await?;
        ensure_table_bucket_enabled(&warehouse).await?;
        let store = table_catalog_store()?;
        let response = store
            .get_table_maintenance_scheduler_report(&warehouse, &namespace.public_name(), &table)
            .await
            .map_err(catalog_store_error)?;
        build_json_response(StatusCode::OK, &response)
    }
}

pub struct RunTableMaintenanceWorkerHandler {}

#[async_trait::async_trait]
impl Operation for RunTableMaintenanceSchedulerHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let table = table_name_from_params(&params)?;
        let resource = TableCatalogResource::table(&warehouse, &namespace, &table);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::RunTableMaintenanceAction).await?;
        ensure_table_bucket_enabled(&warehouse).await?;
        let request = read_json_body_or_default::<TableMaintenanceSchedulerRunRequest>(req.input).await?;
        let store = table_catalog_store()?;
        let response = store
            .run_table_maintenance_scheduler_once(
                &warehouse,
                &namespace.public_name(),
                &table,
                request.scheduler_id().to_string(),
            )
            .await
            .map_err(catalog_store_error)?;
        build_json_response(StatusCode::OK, &response)
    }
}

pub struct RunTableMaintenanceSchedulerHandler {}

#[async_trait::async_trait]
impl Operation for RunTableMaintenanceWorkerHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let table = table_name_from_params(&params)?;
        let resource = TableCatalogResource::table(&warehouse, &namespace, &table);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::RunTableMaintenanceAction).await?;
        ensure_table_bucket_enabled(&warehouse).await?;
        let request = read_json_body::<TableMaintenanceWorkerRunRequest>(req.input).await?;
        let store = table_catalog_store()?;
        let response = store
            .run_table_metadata_maintenance_worker_once(
                &warehouse,
                &namespace.public_name(),
                &table,
                request.worker_id().to_string(),
            )
            .await
            .map_err(catalog_store_error)?;
        build_json_response(StatusCode::OK, &response)
    }
}

pub struct HeartbeatTableMaintenanceJobHandler {}

#[async_trait::async_trait]
impl Operation for HeartbeatTableMaintenanceJobHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let table = table_name_from_params(&params)?;
        let job = job_id_from_params(&params)?;
        let resource = TableCatalogResource::table(&warehouse, &namespace, &table);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::RunTableMaintenanceAction).await?;
        ensure_table_bucket_enabled(&warehouse).await?;
        let request = read_json_body::<TableMaintenanceHeartbeatRequest>(req.input).await?;
        let store = table_catalog_store()?;
        let response = store
            .heartbeat_table_metadata_maintenance_job(
                &warehouse,
                &namespace.public_name(),
                &table,
                &job,
                &request.lease_id,
                &request.worker_id,
            )
            .await
            .map_err(catalog_store_error)?;
        build_json_response(StatusCode::OK, &response)
    }
}

pub struct TableMaintenanceQuarantineHandler {}

#[async_trait::async_trait]
impl Operation for TableMaintenanceQuarantineHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let table = table_name_from_params(&params)?;
        let job = job_id_from_params(&params)?;
        let resource = TableCatalogResource::table(&warehouse, &namespace, &table);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::RunTableMaintenanceAction).await?;
        ensure_table_bucket_enabled(&warehouse).await?;
        let request = read_json_body::<crate::table_catalog::TableMaintenanceQuarantineOperationRequest>(req.input).await?;
        let store = table_catalog_store()?;
        let response = store
            .apply_table_maintenance_quarantine_operation(&warehouse, &namespace.public_name(), &table, &job, request)
            .await
            .map_err(catalog_store_error)?;
        build_json_response(StatusCode::OK, &response)
    }
}

pub struct ExportTableCatalogHandler {}

#[async_trait::async_trait]
impl Operation for ExportTableCatalogHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let table = table_name_from_params(&params)?;
        let resource = TableCatalogResource::table(&warehouse, &namespace, &table);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::GetTableMetadataAction).await?;
        ensure_table_bucket_enabled(&warehouse).await?;
        let store = table_catalog_store()?;
        let started = Instant::now();
        let result = store
            .export_table_catalog_entry(&warehouse, &namespace.public_name(), &table)
            .await
            .map_err(catalog_store_error);
        record_table_catalog_admin_operation_result("export", &warehouse, &namespace.public_name(), &table, started, &result);
        let response = result?;
        build_json_response(StatusCode::OK, &response)
    }
}

pub struct ImportTableCatalogHandler {}

#[async_trait::async_trait]
impl Operation for ImportTableCatalogHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let table = table_name_from_params(&params)?;
        let resource = TableCatalogResource::table(&warehouse, &namespace, &table);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::RegisterTableAction).await?;
        let request = read_json_body::<CatalogImportRequest>(req.input).await?;
        let metadata_backend = table_catalog_backend()?;
        let store = table_catalog_store_from_backend(metadata_backend.clone())?;
        let table_bucket_enabled = table_bucket_enabled_from_metadata(&warehouse).await?;
        let response =
            catalog_import_response(&store, &metadata_backend, &warehouse, &namespace, &table, request, table_bucket_enabled)
                .await?;
        build_json_response(StatusCode::OK, &response)
    }
}

pub struct ExternalCatalogBridgeHandler {}

#[async_trait::async_trait]
impl Operation for ExternalCatalogBridgeHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let table = table_name_from_params(&params)?;
        let resource = TableCatalogResource::table(&warehouse, &namespace, &table);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::GetTableMetadataAction).await?;
        ensure_table_bucket_enabled(&warehouse).await?;
        let store = table_catalog_object_store()?;
        let response = external_catalog_bridge_response(&store, &warehouse, &namespace, &table).await?;
        build_json_response(StatusCode::OK, &response)
    }
}

pub struct PutExternalCatalogBridgeHandler {}

#[async_trait::async_trait]
impl Operation for PutExternalCatalogBridgeHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let table = table_name_from_params(&params)?;
        let resource = TableCatalogResource::table(&warehouse, &namespace, &table);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::RegisterTableAction).await?;
        ensure_table_bucket_enabled(&warehouse).await?;
        let request = read_json_body::<ExternalCatalogBridgeRequest>(req.input).await?;
        let store = table_catalog_object_store()?;
        let response = put_external_catalog_bridge_response(&store, &warehouse, &namespace, &table, request).await?;
        build_json_response(StatusCode::OK, &response)
    }
}

pub struct SyncExternalCatalogBridgeHandler {}

#[async_trait::async_trait]
impl Operation for SyncExternalCatalogBridgeHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let table = table_name_from_params(&params)?;
        let resource = TableCatalogResource::table(&warehouse, &namespace, &table);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::SetTableMetadataLocationAction).await?;
        ensure_table_bucket_enabled(&warehouse).await?;
        let metadata_backend = table_catalog_backend()?;
        let store = table_catalog_object_store()?;
        if store
            .load_table(&warehouse, &namespace.public_name(), &table)
            .await
            .map_err(catalog_store_error)?
            .is_none()
        {
            authorize_table_catalog_resource_request(&req, &resource, AdminAction::RegisterTableAction).await?;
        }
        let request = read_json_body::<ExternalCatalogBridgeSyncRequest>(req.input).await?;
        let table_bucket_enabled = table_bucket_enabled_from_metadata(&warehouse).await?;
        let response = sync_external_catalog_bridge_response(
            &store,
            &metadata_backend,
            &warehouse,
            &namespace,
            &table,
            request,
            table_bucket_enabled,
        )
        .await?;
        build_json_response(StatusCode::OK, &response)
    }
}

pub struct GetTableCatalogDiagnosticsHandler {}

#[async_trait::async_trait]
impl Operation for GetTableCatalogDiagnosticsHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let table = table_name_from_params(&params)?;
        let resource = TableCatalogResource::table(&warehouse, &namespace, &table);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::GetTableMetadataAction).await?;
        ensure_table_bucket_enabled(&warehouse).await?;
        let store = table_catalog_store()?;
        let config = store
            .get_table_maintenance_config(&warehouse, &namespace.public_name(), &table)
            .await
            .map_err(catalog_store_error)?;
        let started = Instant::now();
        let result = store
            .diagnose_table_catalog(&warehouse, &namespace.public_name(), &table, config.retain_recent_metadata_files)
            .await
            .map_err(catalog_store_error);
        record_table_catalog_admin_operation_result(
            "diagnostics",
            &warehouse,
            &namespace.public_name(),
            &table,
            started,
            &result,
        );
        let response = result?;
        build_json_response(StatusCode::OK, &response)
    }
}

pub struct RecoverTableCatalogHandler {}

#[async_trait::async_trait]
impl Operation for RecoverTableCatalogHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let table = table_name_from_params(&params)?;
        let resource = TableCatalogResource::table(&warehouse, &namespace, &table);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::CommitTableAction).await?;
        ensure_table_bucket_enabled(&warehouse).await?;
        let store = table_catalog_store()?;
        let started = Instant::now();
        let result = store
            .recover_table_commits(&warehouse, &namespace.public_name(), &table)
            .await
            .map_err(catalog_store_error);
        record_table_catalog_admin_operation_result("recovery", &warehouse, &namespace.public_name(), &table, started, &result);
        let response = result?;
        build_json_response(StatusCode::OK, &response)
    }
}

pub struct RollbackTableCatalogHandler {}

#[async_trait::async_trait]
impl Operation for RollbackTableCatalogHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let table = table_name_from_params(&params)?;
        let resource = TableCatalogResource::table(&warehouse, &namespace, &table);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::CommitTableAction).await?;
        ensure_table_bucket_enabled(&warehouse).await?;
        let request = read_json_body::<RollbackTableRequest>(req.input).await?;
        let metadata_backend = table_catalog_backend()?;
        let store = table_catalog_store_from_backend(metadata_backend.clone())?;
        let response = rollback_table_response(&store, &metadata_backend, &warehouse, &namespace, &table, request).await?;
        build_json_response(StatusCode::OK, &response)
    }
}

#[cfg(test)]
mod tests;
