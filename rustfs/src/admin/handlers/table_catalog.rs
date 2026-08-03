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

use crate::admin::runtime_sources::current_object_store_handle;
use crate::admin::runtime_sources::default_admin_usecase;
use crate::admin::storage_api::access::{ReqInfo, authorize_request};
use crate::admin::storage_api::bucket::{metadata::table_catalog_path_hash, metadata_sys};
use crate::admin::storage_api::runtime::ECStore;
use crate::admin::{
    auth::{AdminResourceScope, validate_admin_request, validate_admin_request_with_bucket_object},
    router::{AdminOperation, Operation, S3Router},
};
use crate::auth::{check_key_valid, get_session_token};
use crate::server::{RemoteAddr, TABLE_CATALOG_COMPAT_PREFIX, TABLE_CATALOG_PREFIX};
use crate::table_catalog::{DEFAULT_WAREHOUSE_ID, TableCatalogStore};
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt, stream};
use http::{HeaderMap, HeaderValue, StatusCode};
use hyper::Method;
use matchit::Params;
use metrics::{counter, histogram};
use percent_encoding::percent_decode_str;
use rustfs_config::MAX_ADMIN_REQUEST_BODY_SIZE;
use rustfs_policy::policy::action::{Action, AdminAction, S3Action};
use rustfs_utils::crypto::{base64_decode_url_safe_no_pad, base64_encode_url_safe_no_pad, hex_sha256};
use s3s::{Body, S3Error, S3ErrorCode, S3Request, S3Response, S3Result, header::CONTENT_TYPE, s3_error};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroUsize;
use std::time::{Duration as StdDuration, Instant};
use time::OffsetDateTime;
use uuid::Uuid;

const JSON_CONTENT_TYPE: &str = "application/json";
const ENV_TABLE_CATALOG_CREDENTIAL_VENDING: &str = "RUSTFS_TABLE_CATALOG_CREDENTIAL_VENDING";
const NAMESPACE_PROPERTIES_BODY_MAX_SIZE: usize = 64 * 1024;
const NAMESPACE_PROPERTIES_BODY_TIMEOUT: StdDuration = StdDuration::from_secs(10);
const RENAME_TABLE_BODY_MAX_SIZE: usize = 16 * 1024;
const RENAME_TABLE_BODY_TIMEOUT: StdDuration = StdDuration::from_secs(10);
const TABLE_CATALOG_REQUEST_BODY_TIMEOUT: StdDuration = StdDuration::from_secs(30);
const WAREHOUSE_PROPERTY: &str = "warehouse";
const PREFIX_PROPERTY: &str = "prefix";
const NAMESPACE_SEPARATOR_PROPERTY: &str = "namespace-separator";
const ICEBERG_ERROR_ALREADY_EXISTS: &str = "AlreadyExistsException";
const ICEBERG_ERROR_BAD_REQUEST: &str = "BadRequestException";
const ICEBERG_ERROR_COMMIT_FAILED: &str = "CommitFailedException";
const ICEBERG_ERROR_NAMESPACE_NOT_EMPTY: &str = "NamespaceNotEmptyException";
const ICEBERG_ERROR_NO_SUCH_NAMESPACE: &str = "NoSuchNamespaceException";
const ICEBERG_ERROR_NO_SUCH_RESOURCE: &str = "NoSuchResourceException";
const ICEBERG_ERROR_NO_SUCH_TABLE: &str = "NoSuchTableException";
const ICEBERG_ERROR_NO_SUCH_VIEW: &str = "NoSuchViewException";
const ICEBERG_ERROR_REST: &str = "RESTException";
const ICEBERG_ERROR_UNPROCESSABLE_ENTITY: &str = "UnprocessableEntityException";
const ICEBERG_ERROR_UNSUPPORTED_OPERATION: &str = "UnsupportedOperationException";
const ICEBERG_VIEW_FORMAT_VERSION: i64 = 1;
const IDEMPOTENCY_KEY_HEADER: &str = "idempotency-key";
const REST_PAGE_TOKEN_VERSION: u8 = 1;
const REST_PAGE_TOKEN_MAX_LENGTH: usize = 16 * 1024;
const REST_DEFAULT_PAGE_SIZE: usize = 1000;
const REST_MAX_PAGE_SIZE: usize = 1000;
const REST_PAGE_TOKEN_QUERY_PARAMETER: &str = "pageToken";
const REST_PAGE_SIZE_QUERY_PARAMETER: &str = "pageSize";
const REST_NAMESPACE_SEPARATOR: char = '\u{1f}';
const REST_NAMESPACE_SEPARATOR_UTF8: &str = "\u{1f}";
const REST_NAMESPACE_SEPARATOR_URL_ENCODED: &str = "%1F";
const TABLE_COMMIT_MAX_MANIFESTS: usize = 10_000;
const TABLE_COMMIT_MAX_AVRO_BYTES: usize = 512 * 1024 * 1024;
const TABLE_COMMIT_MAX_FILE_REFERENCES: usize = 1_000_000;
const TABLE_COMMIT_OBJECT_VALIDATION_CONCURRENCY: usize = 16;
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
    "GET /v1/{prefix}/namespaces/{namespace}/views",
    "POST /v1/{prefix}/namespaces/{namespace}/views",
    "GET /v1/{prefix}/namespaces/{namespace}/views/{view}",
    "HEAD /v1/{prefix}/namespaces/{namespace}/views/{view}",
    "POST /v1/{prefix}/namespaces/{namespace}/views/{view}",
    "DELETE /v1/{prefix}/namespaces/{namespace}/views/{view}",
];
const TABLE_CATALOG_DURABLE_STRONG_ENDPOINTS: &[&str] = &[
    "POST /v1/{prefix}/namespaces/{namespace}/properties",
    "POST /v1/{prefix}/tables/rename",
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
static UPDATE_NAMESPACE_PROPERTIES_HANDLER: RestUpdateNamespacePropertiesHandler = RestUpdateNamespacePropertiesHandler {};
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
static RENAME_TABLE_HANDLER: RestRenameTableHandler = RestRenameTableHandler {};
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
struct UpdateNamespacePropertiesRequest {
    #[serde(default)]
    removals: Vec<String>,
    #[serde(default)]
    updates: BTreeMap<String, String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RenameTableRequest {
    source: RestTableIdentifier,
    destination: RestTableIdentifier,
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
    identifier: Option<RestTableIdentifier>,
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
    #[serde(default, rename = "identifier")]
    identifier: Option<RestTableIdentifier>,
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

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
}

#[derive(Debug, Clone)]
struct TableCredentialIssueRequest<'a> {
    principal: Option<&'a rustfs_credentials::Credentials>,
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
}

impl IamTableCredentialIssuer {
    fn from_env() -> Self {
        Self {
            enabled: table_credential_vending_enabled(),
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

        let Some(_principal) = request.principal else {
            return Err(s3_error!(InvalidRequest, "authentication required for table credentials"));
        };
        Err(iceberg_rest_error(
            ICEBERG_ERROR_UNSUPPORTED_OPERATION,
            StatusCode::NOT_ACCEPTABLE,
            "table credential vending requires a dedicated rotatable IAM token-signing key",
        ))
    }
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
        Method::POST,
        format!("{prefix}/{{warehouse}}/namespaces/{{namespace}}/properties").as_str(),
        AdminOperation(&UPDATE_NAMESPACE_PROPERTIES_HANDLER),
    )?;
    r.insert(
        Method::DELETE,
        format!("{prefix}/{{warehouse}}/namespaces/{{namespace}}").as_str(),
        AdminOperation(&DROP_NAMESPACE_HANDLER),
    )?;
    r.insert(
        Method::POST,
        format!("{prefix}/{{warehouse}}/tables/rename").as_str(),
        AdminOperation(&RENAME_TABLE_HANDLER),
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
    let mut overrides =
        BTreeMap::from([(NAMESPACE_SEPARATOR_PROPERTY.to_string(), REST_NAMESPACE_SEPARATOR_URL_ENCODED.to_string())]);
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
    let mut endpoints = TABLE_CATALOG_ENDPOINTS.to_vec();
    if backing_mode == crate::table_catalog::TableCatalogBackingMode::DurableStrong {
        endpoints.extend_from_slice(TABLE_CATALOG_DURABLE_STRONG_ENDPOINTS);
    }
    Ok(CatalogConfigResponse {
        defaults,
        overrides,
        endpoints,
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

fn build_sensitive_json_response<T: Serialize>(status: StatusCode, body: &T) -> S3Result<S3Response<(StatusCode, Body)>> {
    let data = serde_json::to_vec(body).map_err(|e| s3_error!(InternalError, "failed to serialize response: {}", e))?;
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static(JSON_CONTENT_TYPE));
    headers.insert(http::header::CACHE_CONTROL, HeaderValue::from_static("no-store, private"));
    headers.insert(http::header::PRAGMA, HeaderValue::from_static("no-cache"));
    headers.insert(http::header::EXPIRES, HeaderValue::from_static("0"));
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
) -> S3Result<TableCatalogRequestPrincipal> {
    let principal = table_catalog_request_principal(req).await?;
    authorize_table_catalog_resource_for_principal(req, &principal, resource, action).await?;
    Ok(principal)
}

struct TableCatalogRequestPrincipal {
    credentials: rustfs_credentials::Credentials,
    owner: bool,
}

async fn authorize_table_catalog_resource_for_principal(
    req: &S3Request<Body>,
    principal: &TableCatalogRequestPrincipal,
    resource: &TableCatalogResource<'_>,
    action: AdminAction,
) -> S3Result<()> {
    let object_path = resource.object_path();
    validate_admin_request_with_bucket_object(
        &req.headers,
        &principal.credentials,
        principal.owner,
        false,
        vec![Action::AdminAction(action)],
        req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0)),
        AdminResourceScope::bucket_object(resource.warehouse, object_path.as_deref().unwrap_or("")),
    )
    .await
}

async fn authorize_table_catalog_s3_actions(
    req: &mut S3Request<Body>,
    bucket: &str,
    object: &str,
    actions: &[S3Action],
) -> S3Result<()> {
    if req.extensions.get::<ReqInfo>().is_none() {
        let principal = table_catalog_request_principal(req).await?;
        req.extensions.insert(ReqInfo {
            cred: Some(principal.credentials),
            is_owner: principal.owner,
            ..Default::default()
        });
    }
    let original = {
        let req_info = req
            .extensions
            .get_mut::<ReqInfo>()
            .ok_or_else(|| s3_error!(AccessDenied, "authentication required"))?;
        (
            req_info.bucket.replace(bucket.to_string()),
            req_info.object.replace(object.to_string()),
            req_info.version_id.take(),
        )
    };

    let mut result = Ok(());
    for action in actions {
        if let Err(err) = authorize_request(req, Action::S3Action(*action)).await {
            result = Err(err);
            break;
        }
    }
    if let Some(req_info) = req.extensions.get_mut::<ReqInfo>() {
        (req_info.bucket, req_info.object, req_info.version_id) = original;
    }
    result
}

async fn authorize_optional_table_catalog_object_read(
    req: Option<&mut S3Request<Body>>,
    bucket: &str,
    object: &str,
) -> S3Result<()> {
    match req {
        Some(req) => authorize_table_catalog_s3_actions(req, bucket, object, &[S3Action::GetObjectAction]).await,
        None => Ok(()),
    }
}

async fn authorize_table_warehouse_claim(
    req: &S3Request<Body>,
    principal: &TableCatalogRequestPrincipal,
    bucket: &str,
    warehouse_location: &str,
) -> S3Result<()> {
    validate_table_location_in_bucket(bucket, warehouse_location)?;
    let warehouse_prefix = crate::table_catalog::table_catalog_object_key_from_location(bucket, warehouse_location)
        .ok_or_else(|| s3_error!(InvalidRequest, "table location must be inside the warehouse bucket"))?;
    validate_admin_request_with_bucket_object(
        &req.headers,
        &principal.credentials,
        principal.owner,
        false,
        vec![Action::AdminAction(AdminAction::RegisterTableAction)],
        req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0)),
        AdminResourceScope::bucket_object(bucket, warehouse_prefix.trim_end_matches('/')),
    )
    .await
}

async fn read_authorized_table_metadata_json(
    req: &mut S3Request<Body>,
    metadata_backend: &impl crate::table_catalog::TableCatalogObjectBackend,
    bucket: &str,
    metadata_location: &str,
) -> S3Result<(serde_json::Value, crate::table_catalog::TableCatalogObjectLock)> {
    authorize_table_catalog_s3_actions(req, bucket, metadata_location, &[S3Action::GetObjectAction]).await?;
    let metadata_guard = metadata_backend
        .acquire_read_lock(bucket, metadata_location)
        .await
        .map_err(catalog_store_error)?;
    let Some(object) = metadata_backend
        .read_object_unlocked_limited(bucket, metadata_location, crate::table_catalog::TABLE_METADATA_JSON_MAX_SIZE)
        .await
        .map_err(catalog_store_error)?
    else {
        return Err(s3_error!(InvalidRequest, "table metadata object not found: {metadata_location}"));
    };
    let metadata = parse_table_metadata_json(&object.data)?;
    let principal = table_catalog_request_principal(req).await?;
    authorize_table_warehouse_claim(req, &principal, bucket, metadata_table_location(&metadata)?).await?;
    crate::table_catalog::ensure_table_catalog_lock_held(metadata_guard.as_ref()).map_err(catalog_store_error)?;
    Ok((metadata, metadata_guard))
}

async fn table_catalog_request_principal(req: &S3Request<Body>) -> S3Result<TableCatalogRequestPrincipal> {
    let Some(input_cred) = &req.credentials else {
        return Err(s3_error!(InvalidRequest, "authentication required"));
    };

    let (cred, owner) =
        check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;
    Ok(TableCatalogRequestPrincipal {
        credentials: cred,
        owner,
    })
}

async fn read_limited_body(mut input: Body, max_size: usize, timeout: StdDuration, operation: Option<&str>) -> S3Result<Bytes> {
    tokio::time::timeout(timeout, input.store_all_limited(max_size))
        .await
        .map_err(|_| {
            operation.map_or_else(
                || s3_error!(InvalidRequest, "timed out reading request body"),
                |operation| s3_error!(InvalidRequest, "timed out reading {operation} request body"),
            )
        })?
        .map_err(|err| s3_error!(InvalidRequest, "failed to read request body: {}", err))
}

async fn read_json_body<T: DeserializeOwned>(input: Body) -> S3Result<T> {
    let body = read_limited_body(input, MAX_ADMIN_REQUEST_BODY_SIZE, TABLE_CATALOG_REQUEST_BODY_TIMEOUT, None).await?;
    if body.is_empty() {
        return Err(s3_error!(InvalidRequest, "request body is required"));
    }
    serde_json::from_slice(&body).map_err(|err| s3_error!(InvalidRequest, "invalid JSON: {}", err))
}

async fn read_rest_commit_table_request(headers: &HeaderMap, input: Body) -> S3Result<RestCommitTableRequest> {
    let mut request = read_json_body::<RestCommitTableRequest>(input).await?;
    let mut values = headers.get_all(IDEMPOTENCY_KEY_HEADER).iter();
    let Some(value) = values.next() else {
        return Ok(request);
    };
    if values.next().is_some() {
        return Err(iceberg_rest_error(
            ICEBERG_ERROR_BAD_REQUEST,
            StatusCode::BAD_REQUEST,
            "Idempotency-Key header must not be repeated",
        ));
    }
    let value = value.to_str().map_err(|_| {
        iceberg_rest_error(
            ICEBERG_ERROR_BAD_REQUEST,
            StatusCode::BAD_REQUEST,
            "Idempotency-Key header must be a UUIDv7",
        )
    })?;
    let idempotency_key = Uuid::parse_str(value).map_err(|_| {
        iceberg_rest_error(
            ICEBERG_ERROR_BAD_REQUEST,
            StatusCode::BAD_REQUEST,
            "Idempotency-Key header must be a UUIDv7",
        )
    })?;
    if value.len() != 36 || idempotency_key.get_version_num() != 7 {
        return Err(iceberg_rest_error(
            ICEBERG_ERROR_BAD_REQUEST,
            StatusCode::BAD_REQUEST,
            "Idempotency-Key header must be a UUIDv7",
        ));
    }
    if request.idempotency_key.as_deref().is_some_and(|body_key| body_key != value) {
        return Err(iceberg_rest_error(
            ICEBERG_ERROR_BAD_REQUEST,
            StatusCode::BAD_REQUEST,
            "Idempotency-Key header does not match the request body",
        ));
    }
    request.idempotency_key = Some(value.to_string());
    Ok(request)
}

async fn read_bounded_json_body<T: DeserializeOwned>(
    headers: &HeaderMap,
    input: Body,
    max_size: usize,
    timeout: StdDuration,
    operation: &str,
) -> S3Result<T> {
    if let Some(content_length) = headers.get(http::header::CONTENT_LENGTH) {
        let content_length = content_length
            .to_str()
            .map_err(|_| s3_error!(InvalidRequest, "Content-Length must be valid ASCII"))?
            .parse::<usize>()
            .map_err(|_| s3_error!(InvalidRequest, "Content-Length must be a non-negative integer"))?;
        if content_length > max_size {
            return Err(s3_error!(InvalidRequest, "{operation} request body is too large"));
        }
    }
    let body = read_limited_body(input, max_size, timeout, Some(operation)).await?;
    if body.is_empty() {
        return Err(s3_error!(InvalidRequest, "request body is required"));
    }
    serde_json::from_slice(&body).map_err(|err| s3_error!(InvalidRequest, "invalid JSON: {}", err))
}

async fn read_json_body_or_default<T>(input: Body) -> S3Result<T>
where
    T: Default + DeserializeOwned,
{
    let body = read_limited_body(input, MAX_ADMIN_REQUEST_BODY_SIZE, TABLE_CATALOG_REQUEST_BODY_TIMEOUT, None).await?;
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

fn rest_purge_requested_from_query(uri: &http::Uri) -> S3Result<bool> {
    let mut purge_requested = None;
    if let Some(query) = uri.query() {
        for (key, value) in url::form_urlencoded::parse(query.as_bytes()) {
            if key != "purgeRequested" {
                continue;
            }
            if purge_requested.is_some() {
                return Err(iceberg_rest_error(
                    ICEBERG_ERROR_BAD_REQUEST,
                    StatusCode::BAD_REQUEST,
                    "purgeRequested query parameter must not be repeated",
                ));
            }
            let value = value.parse::<bool>().map_err(|_| {
                iceberg_rest_error(
                    ICEBERG_ERROR_BAD_REQUEST,
                    StatusCode::BAD_REQUEST,
                    "purgeRequested query parameter must be true or false",
                )
            })?;
            purge_requested = Some(value);
        }
    }
    Ok(purge_requested.unwrap_or(false))
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

fn namespace_from_rest_value(
    value: &str,
) -> Result<crate::table_catalog::Namespace, crate::table_catalog::CatalogIdentifierError> {
    let segments = value.split(REST_NAMESPACE_SEPARATOR).map(str::to_string).collect::<Vec<_>>();
    crate::table_catalog::Namespace::from_segments(segments)
}

fn namespace_from_path_value(value: &str) -> S3Result<crate::table_catalog::Namespace> {
    if value.contains('.') && !value.contains(REST_NAMESPACE_SEPARATOR) && !value.contains("%1F") && !value.contains("%1f") {
        let decoded = percent_decode_str(value)
            .decode_utf8()
            .map_err(|_| s3_error!(InvalidRequest, "namespace path must be valid UTF-8"))?;
        return crate::table_catalog::Namespace::parse(decoded.as_ref())
            .map_err(|err| s3_error!(InvalidRequest, "invalid namespace: {}", err));
    }
    let normalized = value
        .replace(REST_NAMESPACE_SEPARATOR_URL_ENCODED, REST_NAMESPACE_SEPARATOR_UTF8)
        .replace("%1f", REST_NAMESPACE_SEPARATOR_UTF8);
    let segments = normalized
        .split(REST_NAMESPACE_SEPARATOR)
        .map(|segment| {
            percent_decode_str(segment)
                .decode_utf8()
                .map(std::borrow::Cow::into_owned)
                .map_err(|_| s3_error!(InvalidRequest, "namespace path must be valid UTF-8"))
        })
        .collect::<S3Result<Vec<_>>>()?;
    crate::table_catalog::Namespace::from_segments(segments)
        .map_err(|err| s3_error!(InvalidRequest, "invalid namespace: {}", err))
}

fn rest_namespace_parent_from_query(uri: &http::Uri) -> S3Result<Option<crate::table_catalog::Namespace>> {
    let mut parent = None;
    let mut parent_seen = false;
    if let Some(query) = uri.query() {
        for (key, value) in url::form_urlencoded::parse(query.as_bytes()) {
            if key != "parent" {
                continue;
            }
            if parent_seen {
                return Err(iceberg_rest_error(
                    ICEBERG_ERROR_BAD_REQUEST,
                    StatusCode::BAD_REQUEST,
                    "parent query parameter must not be repeated",
                ));
            }
            parent_seen = true;
            if !value.is_empty() {
                parent = Some(namespace_from_rest_value(&value).map_err(|err| {
                    iceberg_rest_error(
                        ICEBERG_ERROR_BAD_REQUEST,
                        StatusCode::BAD_REQUEST,
                        format!("invalid parent namespace: {err}"),
                    )
                })?);
            }
        }
    }
    Ok(parent)
}

fn namespace_from_params(params: &Params<'_, '_>) -> S3Result<crate::table_catalog::Namespace> {
    let namespace = params.get("namespace").unwrap_or("");
    namespace_from_path_value(namespace)
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
    if let Some(entry) = store.get_table_bucket(bucket).await.map_err(catalog_store_error)? {
        if entry.state == crate::table_catalog::TableCatalogEntryState::Active {
            return Ok(());
        }
        return Err(s3_error!(InvalidRequest, "table bucket {bucket} catalog entry is not active"));
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

fn namespace_from_segments(segments: Vec<String>) -> S3Result<crate::table_catalog::Namespace> {
    crate::table_catalog::Namespace::from_segments(segments)
        .map_err(|err| s3_error!(InvalidRequest, "invalid namespace: {}", err))
}

fn validate_rest_commit_identifier(
    identifier: Option<&RestTableIdentifier>,
    namespace: &crate::table_catalog::Namespace,
    name: &str,
) -> S3Result<()> {
    if let Some(identifier) = identifier
        && (identifier.namespace != namespace_segments(namespace) || identifier.name != name)
    {
        return Err(s3_error!(InvalidRequest, "request identifier must match the resource URL"));
    }
    Ok(())
}

fn namespace_response_from_entry(entry: crate::table_catalog::NamespaceEntry) -> S3Result<RestNamespaceResponse> {
    let namespace = crate::table_catalog::Namespace::parse(&entry.namespace)
        .map_err(|err| s3_error!(InternalError, "persisted namespace entry is invalid: {}", err))?;
    Ok(RestNamespaceResponse {
        namespace: namespace_segments(&namespace),
        properties: entry.properties,
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

fn table_credential_scope(entry: &crate::table_catalog::TableEntry) -> S3Result<TableCredentialScope> {
    let object_prefix = crate::table_catalog::table_warehouse_object_prefix(entry)
        .map_err(|err| s3_error!(InvalidRequest, "invalid table credential warehouse location: {}", err))?;
    Ok(TableCredentialScope {
        scope_prefix: format!("s3://{}/{object_prefix}", entry.table_bucket),
    })
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
    if metadata_location.starts_with("s3://") {
        metadata_location.to_string()
    } else {
        format!("s3://{table_bucket}/{metadata_location}")
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
    let metadata_location = table_metadata_location_for_client(&entry.table_bucket, &entry.metadata_location);
    let metadata = table_metadata_for_client(&entry.table_bucket, metadata);
    config.insert("warehouse-location".to_string(), warehouse_location.clone());
    config.insert(CREDENTIAL_SCOPE_CONFIG_KEY.to_string(), CREDENTIAL_SCOPE_TABLE_PREFIX.to_string());
    config.insert(CREDENTIAL_SCOPE_PREFIX_CONFIG_KEY.to_string(), warehouse_location);
    config.insert(CREDENTIAL_MODE_CONFIG_KEY.to_string(), CREDENTIAL_MODE_CLIENT_PROVIDED.to_string());

    RestLoadViewResponse {
        metadata_location,
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
    let request = TableCredentialIssueRequest { principal };
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
    let commit_id = request.commit_id.unwrap_or_else(|| {
        request.idempotency_key.as_deref().map_or_else(
            || Uuid::new_v4().to_string(),
            |idempotency_key| format!("idempotency-{}", table_catalog_path_hash(idempotency_key)),
        )
    });
    Ok(crate::table_catalog::TableCommitRequest {
        table_bucket: bucket.to_string(),
        namespace: namespace.public_name(),
        table: table.to_string(),
        commit_id,
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
    let version = u16::try_from(version).map_err(|_| s3_error!(InvalidRequest, "table metadata format-version is too large"))?;
    if !(1..=2).contains(&version) {
        return Err(iceberg_rest_error(
            ICEBERG_ERROR_UNSUPPORTED_OPERATION,
            StatusCode::NOT_ACCEPTABLE,
            format!("unsupported Iceberg table format-version: {version}"),
        ));
    }
    Ok(version)
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
    validate_supported_table_metadata(current_metadata)?;
    let target_table_uuid = metadata_table_uuid(target_metadata)?;
    validate_supported_table_metadata(target_metadata)?;
    if target_table_uuid != expected_table_uuid {
        return Err(s3_error!(
            InvalidRequest,
            "table metadata table-uuid does not match current table metadata"
        ));
    }
    Ok(())
}

fn metadata_sha256(metadata: &serde_json::Value) -> S3Result<String> {
    let canonical = serde_json::to_vec(metadata)
        .map_err(|err| s3_error!(InternalError, "failed to encode metadata digest input: {}", err))?;
    Ok(hex_sha256(&canonical, str::to_string))
}

fn metadata_digest_requirement_with_type(
    metadata: &serde_json::Value,
    requirement_type: &'static str,
) -> S3Result<serde_json::Value> {
    let sha256 = metadata_sha256(metadata)?;
    Ok(serde_json::json!({
        "type": requirement_type,
        "sha256": sha256
    }))
}

fn metadata_digest_requirement(metadata: &serde_json::Value) -> S3Result<serde_json::Value> {
    metadata_digest_requirement_with_type(metadata, crate::table_catalog::TABLE_METADATA_DIGEST_REQUIREMENT_TYPE)
}

fn base_metadata_digest_requirement(metadata: &serde_json::Value) -> S3Result<serde_json::Value> {
    metadata_digest_requirement_with_type(metadata, crate::table_catalog::TABLE_BASE_METADATA_DIGEST_REQUIREMENT_TYPE)
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
    validate_supported_view_metadata(current_metadata)?;
    let target_view_uuid = metadata_view_uuid(target_metadata)?;
    validate_supported_view_metadata(target_metadata)?;
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
        return Err(iceberg_rest_error(
            ICEBERG_ERROR_UNSUPPORTED_OPERATION,
            StatusCode::NOT_ACCEPTABLE,
            "register table overwrite is not supported",
        ));
    }
    let table = crate::table_catalog::IdentifierSegment::parse(request.name)
        .map_err(|err| s3_error!(InvalidRequest, "invalid table name: {}", err))?;
    let metadata_location = table_metadata_location_for_catalog(bucket, &request.metadata_location)?;

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
        return Err(iceberg_rest_error(
            ICEBERG_ERROR_UNSUPPORTED_OPERATION,
            StatusCode::NOT_ACCEPTABLE,
            "stage-create is not supported",
        ));
    }

    let table = crate::table_catalog::IdentifierSegment::parse(request.name)
        .map_err(|err| s3_error!(InvalidRequest, "invalid table name: {}", err))?;
    let table_id = Uuid::new_v4().to_string();
    let table_uuid = Uuid::new_v4().to_string();
    let format_version = match request.properties.get("format-version") {
        Some(version) => version
            .parse::<u16>()
            .map_err(|_| s3_error!(InvalidRequest, "format-version property must be an integer"))?,
        None => 2,
    };
    if !(1..=2).contains(&format_version) {
        return Err(iceberg_rest_error(
            ICEBERG_ERROR_UNSUPPORTED_OPERATION,
            StatusCode::NOT_ACCEPTABLE,
            format!("unsupported Iceberg table format-version: {format_version}"),
        ));
    }
    let warehouse_location = request.location.unwrap_or_else(|| format!("s3://{bucket}/tables/{table_id}"));
    validate_table_location_in_bucket(bucket, &warehouse_location)?;
    let metadata_location =
        crate::table_catalog::default_table_metadata_file_path(namespace, &table, &next_metadata_file_name(1, &table_id));

    let entry = crate::table_catalog::TableEntry {
        version: crate::table_catalog::TABLE_CATALOG_ENTRY_VERSION,
        table_bucket: bucket.to_string(),
        namespace: namespace.public_name(),
        table: table.as_str().to_string(),
        table_id,
        table_uuid,
        format: "ICEBERG".to_string(),
        format_version,
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

    let mut metadata = serde_json::json!({
        "format-version": entry.format_version,
        "table-uuid": entry.table_uuid,
        "location": entry.warehouse_location,
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
    });
    if entry.format_version == 2 {
        metadata_object_mut(&mut metadata)?.insert("last-sequence-number".to_string(), serde_json::Value::from(0));
    }
    synchronize_table_metadata_version_fields(&mut metadata)?;
    validate_supported_table_metadata(&metadata)?;
    Ok(metadata)
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
    if view_version_object.get("schema-id").and_then(serde_json::Value::as_i64) == Some(-1) {
        view_version_object.insert("schema-id".to_string(), serde_json::Value::from(schema_id));
    }
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

    let metadata = serde_json::json!({
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
    });
    validate_supported_view_metadata(&metadata)?;
    Ok(metadata)
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

fn standard_commit_ids(commit_id: Option<String>, idempotency_key: Option<&str>) -> (String, String) {
    let commit_id = commit_id
        .or_else(|| idempotency_key.map(|idempotency_key| format!("idempotency-{}", table_catalog_path_hash(idempotency_key))));
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

struct PublishedApiCommitReplay<'a> {
    route: RestTableRoute<'a>,
    current: &'a crate::table_catalog::TableEntry,
    commit_id: &'a str,
    idempotency_key: Option<&'a str>,
    operation: &'a str,
    expected_version_token: &'a str,
    new_metadata_location: &'a str,
    expected_metadata_location: Option<&'a str>,
}

async fn published_api_commit_replay<S>(
    store: &S,
    replay: PublishedApiCommitReplay<'_>,
) -> S3Result<Option<crate::table_catalog::TableCommitRequest>>
where
    S: crate::table_catalog::TableCatalogStore + ?Sized,
{
    let PublishedApiCommitReplay {
        route,
        current,
        commit_id,
        idempotency_key,
        operation,
        expected_version_token,
        new_metadata_location,
        expected_metadata_location,
    } = replay;
    let mut existing = store
        .get_commit_by_id(&current.table_bucket, &current.table_id, commit_id)
        .await
        .map_err(catalog_store_error)?;
    if existing.is_none()
        && let Some(idempotency_key) = idempotency_key
    {
        existing = store
            .get_commit_by_idempotency_key(&current.table_bucket, &current.table_id, idempotency_key)
            .await
            .map_err(catalog_store_error)?;
    }
    let Some(existing) = existing else {
        return Ok(None);
    };
    if existing.commit_id != commit_id
        || existing.idempotency_key.as_deref() != idempotency_key
        || existing.operation != operation
        || existing.expected_version_token != expected_version_token
        || existing.new_metadata_location != new_metadata_location
        || expected_metadata_location.is_some_and(|location| existing.previous_metadata_location != location)
    {
        return Err(iceberg_rest_error(
            ICEBERG_ERROR_COMMIT_FAILED,
            StatusCode::CONFLICT,
            "commit id or idempotency key is already bound to a different request",
        ));
    }
    if !matches!(existing.status, crate::table_catalog::CommitLogStatus::Committed)
        && (current.metadata_location != existing.new_metadata_location || current.version_token != existing.new_version_token)
    {
        return Ok(None);
    }
    Ok(Some(crate::table_catalog::TableCommitRequest {
        table_bucket: current.table_bucket.clone(),
        namespace: route.namespace.public_name(),
        table: route.table.to_string(),
        commit_id: existing.commit_id,
        idempotency_key: existing.idempotency_key,
        operation: existing.operation,
        expected_version_token: existing.expected_version_token,
        expected_metadata_location: existing.previous_metadata_location,
        new_metadata_location: existing.new_metadata_location,
        requirements: existing.requirements,
        writer: existing.writer,
    }))
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
            _ => return Err(s3_error!(InvalidRequest, "unsupported commit requirement: {requirement_type}")),
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
    metadata: serde_json::Value,
    updates: &[serde_json::Value],
    previous_metadata_location: &str,
) -> S3Result<serde_json::Value> {
    apply_table_commit_updates_at(metadata, updates, previous_metadata_location, current_time_millis())
}

fn apply_table_commit_updates_at(
    mut metadata: serde_json::Value,
    updates: &[serde_json::Value],
    previous_metadata_location: &str,
    commit_timestamp_ms: i64,
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
            "assign-uuid" => apply_assign_uuid_update(&mut metadata, update, "table-uuid", "table")?,
            "upgrade-format-version" => apply_upgrade_format_version_update(&mut metadata, update)?,
            "add-schema" => apply_add_schema_update(&mut metadata, update)?,
            "set-current-schema" => apply_set_current_schema_update(&mut metadata, update)?,
            "add-spec" => apply_add_spec_update(&mut metadata, update)?,
            "set-default-spec" => apply_set_default_spec_update(&mut metadata, update)?,
            "add-sort-order" => apply_add_sort_order_update(&mut metadata, update)?,
            "set-default-sort-order" => apply_set_default_sort_order_update(&mut metadata, update)?,
            "add-snapshot" => apply_add_snapshot_update(&mut metadata, update)?,
            "set-snapshot-ref" => apply_set_snapshot_ref_update(&mut metadata, update, commit_timestamp_ms)?,
            "remove-snapshots" => apply_remove_snapshots_update(&mut metadata, update)?,
            "remove-snapshot-ref" => apply_remove_snapshot_ref_update(&mut metadata, update)?,
            "set-location" => apply_set_location_update(&mut metadata, update)?,
            "set-properties" => apply_set_properties_update(&mut metadata, update)?,
            "remove-properties" => apply_remove_properties_update(&mut metadata, update)?,
            "set-statistics" => apply_set_snapshot_file_update(&mut metadata, update, "statistics", "statistics")?,
            "remove-statistics" => apply_remove_snapshot_file_update(&mut metadata, update, "statistics")?,
            "set-partition-statistics" => {
                apply_set_snapshot_file_update(&mut metadata, update, "partition-statistics", "partition-statistics")?;
            }
            "remove-partition-statistics" => {
                apply_remove_snapshot_file_update(&mut metadata, update, "partition-statistics")?;
            }
            "remove-partition-specs" => {
                apply_remove_metadata_ids_update(&mut metadata, update, "partition-specs", "spec-id", "spec-ids")?;
            }
            "remove-schemas" => {
                apply_remove_metadata_ids_update(&mut metadata, update, "schemas", "schema-id", "schema-ids")?;
            }
            "add-encryption-key" => apply_add_encryption_key_update(&mut metadata, update)?,
            "remove-encryption-key" => apply_remove_encryption_key_update(&mut metadata, update)?,
            _ => return Err(s3_error!(InvalidRequest, "unsupported table update: {action}")),
        }
    }

    if metadata.get("format-version").is_some() {
        synchronize_table_metadata_version_fields(&mut metadata)?;
    }
    validate_table_metadata_references(&metadata)?;
    append_previous_metadata_log(&mut metadata, previous_metadata_location, commit_timestamp_ms)?;
    metadata_object_mut(&mut metadata)?.insert("last-updated-ms".to_string(), serde_json::Value::from(commit_timestamp_ms));
    Ok(metadata)
}

fn synchronize_table_metadata_version_fields(metadata: &mut serde_json::Value) -> S3Result<()> {
    match metadata_format_version(metadata)? {
        1 => {
            if let Some(schemas) = metadata.get("schemas").and_then(serde_json::Value::as_array)
                && !schemas.is_empty()
            {
                let current_schema_id = metadata
                    .get("current-schema-id")
                    .and_then(serde_json::Value::as_i64)
                    .or_else(|| {
                        schemas
                            .last()
                            .and_then(|schema| schema.get("schema-id"))
                            .and_then(serde_json::Value::as_i64)
                    });
                let schema = current_schema_id
                    .and_then(|schema_id| {
                        schemas
                            .iter()
                            .find(|schema| schema.get("schema-id").and_then(serde_json::Value::as_i64) == Some(schema_id))
                    })
                    .cloned()
                    .ok_or_else(|| s3_error!(InvalidRequest, "Iceberg v1 current schema does not exist"))?;
                metadata_object_mut(metadata)?.insert("schema".to_string(), schema);
            }
            if let Some(specs) = metadata.get("partition-specs").and_then(serde_json::Value::as_array)
                && !specs.is_empty()
            {
                let default_spec_id = metadata
                    .get("default-spec-id")
                    .and_then(serde_json::Value::as_i64)
                    .or_else(|| {
                        specs
                            .last()
                            .and_then(|spec| spec.get("spec-id"))
                            .and_then(serde_json::Value::as_i64)
                    });
                let fields = default_spec_id
                    .and_then(|spec_id| {
                        specs
                            .iter()
                            .find(|spec| spec.get("spec-id").and_then(serde_json::Value::as_i64) == Some(spec_id))
                    })
                    .and_then(|spec| spec.get("fields"))
                    .cloned()
                    .ok_or_else(|| s3_error!(InvalidRequest, "Iceberg v1 default partition spec does not exist"))?;
                metadata_object_mut(metadata)?.insert("partition-spec".to_string(), fields);
            }
            let object = metadata_object_mut(metadata)?;
            object.remove("schemas");
            object.remove("current-schema-id");
            object.remove("partition-specs");
            object.remove("default-spec-id");
            object.remove("last-partition-id");
            object.remove("sort-orders");
            object.remove("default-sort-order-id");
            object.remove("last-sequence-number");
        }
        2 => {
            if metadata.get("schemas").is_none() {
                let mut schema = metadata
                    .get("schema")
                    .cloned()
                    .ok_or_else(|| s3_error!(InvalidRequest, "Iceberg v1 table metadata is missing schema"))?;
                schema
                    .as_object_mut()
                    .ok_or_else(|| s3_error!(InvalidRequest, "Iceberg v1 schema must be an object"))?
                    .entry("schema-id".to_string())
                    .or_insert_with(|| serde_json::Value::from(0));
                let schema_id = schema
                    .get("schema-id")
                    .and_then(serde_json::Value::as_i64)
                    .ok_or_else(|| s3_error!(InvalidRequest, "Iceberg v1 schema-id must be an integer"))?;
                metadata_object_mut(metadata)?.insert("schemas".to_string(), serde_json::json!([schema]));
                metadata_object_mut(metadata)?.insert("current-schema-id".to_string(), serde_json::Value::from(schema_id));
            }
            if metadata.get("partition-specs").is_none() {
                let fields = metadata
                    .get("partition-spec")
                    .cloned()
                    .ok_or_else(|| s3_error!(InvalidRequest, "Iceberg v1 table metadata is missing partition-spec"))?;
                metadata_object_mut(metadata)?
                    .insert("partition-specs".to_string(), serde_json::json!([{"spec-id": 0, "fields": fields}]));
                metadata_object_mut(metadata)?.insert("default-spec-id".to_string(), serde_json::Value::from(0));
            }
            if metadata.get("sort-orders").is_none() {
                metadata_object_mut(metadata)?
                    .insert("sort-orders".to_string(), serde_json::json!([{"order-id": 0, "fields": []}]));
                metadata_object_mut(metadata)?.insert("default-sort-order-id".to_string(), serde_json::Value::from(0));
            }
            if metadata.get("last-partition-id").is_none() {
                let last_partition_id = metadata
                    .get("partition-specs")
                    .and_then(serde_json::Value::as_array)
                    .map(|specs| specs.iter().map(max_partition_field_id).max().unwrap_or(999))
                    .unwrap_or(999);
                metadata_object_mut(metadata)?
                    .insert("last-partition-id".to_string(), serde_json::Value::from(last_partition_id));
            }
            if metadata.get("last-sequence-number").is_none() {
                let last_sequence_number = metadata
                    .get("snapshots")
                    .and_then(serde_json::Value::as_array)
                    .and_then(|snapshots| {
                        snapshots
                            .iter()
                            .filter_map(|snapshot| snapshot.get("sequence-number").and_then(serde_json::Value::as_i64))
                            .max()
                    })
                    .unwrap_or(0);
                metadata_object_mut(metadata)?
                    .insert("last-sequence-number".to_string(), serde_json::Value::from(last_sequence_number));
            }
            let object = metadata_object_mut(metadata)?;
            object.remove("schema");
            object.remove("partition-spec");
        }
        _ => {
            return Err(s3_error!(InvalidRequest, "unsupported Iceberg table format-version"));
        }
    }
    Ok(())
}

fn metadata_array_ids(metadata: &serde_json::Value, array_field: &str, id_field: &str, label: &str) -> S3Result<BTreeSet<i64>> {
    let Some(values) = metadata.get(array_field) else {
        return Ok(BTreeSet::new());
    };
    let values = values
        .as_array()
        .ok_or_else(|| s3_error!(InvalidRequest, "{array_field} must be an array"))?;
    let mut ids = BTreeSet::new();
    for value in values {
        let id = value
            .get(id_field)
            .and_then(serde_json::Value::as_i64)
            .ok_or_else(|| s3_error!(InvalidRequest, "{label} is missing {id_field}"))?;
        if !ids.insert(id) {
            return Err(s3_error!(InvalidRequest, "duplicate {label} id {id}"));
        }
    }
    Ok(ids)
}

fn metadata_array_i32_ids(
    metadata: &serde_json::Value,
    array_field: &str,
    id_field: &str,
    label: &str,
) -> S3Result<BTreeSet<i64>> {
    let ids = metadata_array_ids(metadata, array_field, id_field, label)?;
    if let Some(id) = ids.iter().find(|id| i32::try_from(**id).is_err()) {
        return Err(s3_error!(InvalidRequest, "{label} id {id} exceeds the signed 32-bit range"));
    }
    Ok(ids)
}

fn require_metadata_i64(metadata: &serde_json::Value, field: &str) -> S3Result<i64> {
    metadata
        .get(field)
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| s3_error!(InvalidRequest, "table metadata is missing integer field {field}"))
}

fn require_metadata_i32(metadata: &serde_json::Value, field: &str) -> S3Result<i32> {
    let value = require_metadata_i64(metadata, field)?;
    i32::try_from(value).map_err(|_| s3_error!(InvalidRequest, "table metadata field {field} exceeds the signed 32-bit range"))
}

fn require_metadata_array<'a>(metadata: &'a serde_json::Value, field: &str) -> S3Result<&'a Vec<serde_json::Value>> {
    metadata
        .get(field)
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| s3_error!(InvalidRequest, "table metadata is missing array field {field}"))
}

fn validate_supported_table_metadata(metadata: &serde_json::Value) -> S3Result<()> {
    metadata_table_uuid(metadata)?;
    metadata_table_location(metadata)?;
    require_metadata_i64(metadata, "last-updated-ms")?;
    require_metadata_i32(metadata, "last-column-id")?;

    match metadata_format_version(metadata)? {
        1 => {
            if !metadata.get("schema").is_some_and(serde_json::Value::is_object) {
                return Err(s3_error!(InvalidRequest, "Iceberg v1 table metadata is missing schema"));
            }
            require_metadata_array(metadata, "partition-spec")?;
            if let Some(snapshots) = metadata.get("snapshots").and_then(serde_json::Value::as_array) {
                for snapshot in snapshots {
                    if snapshot
                        .get("sequence-number")
                        .is_some_and(|sequence_number| sequence_number.as_i64() != Some(0))
                    {
                        return Err(s3_error!(InvalidRequest, "Iceberg v1 snapshot sequence-number must be zero when present"));
                    }
                }
            }
        }
        2 => {
            let last_sequence_number = require_metadata_i64(metadata, "last-sequence-number")?;
            if last_sequence_number < 0 {
                return Err(s3_error!(InvalidRequest, "last-sequence-number must not be negative"));
            }
            if require_metadata_array(metadata, "schemas")?.is_empty() {
                return Err(s3_error!(InvalidRequest, "table metadata schemas must not be empty"));
            }
            require_metadata_i32(metadata, "current-schema-id")?;
            if require_metadata_array(metadata, "partition-specs")?.is_empty() {
                return Err(s3_error!(InvalidRequest, "table metadata partition-specs must not be empty"));
            }
            require_metadata_i32(metadata, "default-spec-id")?;
            require_metadata_i32(metadata, "last-partition-id")?;
            if require_metadata_array(metadata, "sort-orders")?.is_empty() {
                return Err(s3_error!(InvalidRequest, "table metadata sort-orders must not be empty"));
            }
            require_metadata_i32(metadata, "default-sort-order-id")?;
            if let Some(snapshots) = metadata.get("snapshots").and_then(serde_json::Value::as_array) {
                for snapshot in snapshots {
                    let sequence_number = snapshot
                        .get("sequence-number")
                        .and_then(serde_json::Value::as_i64)
                        .ok_or_else(|| s3_error!(InvalidRequest, "Iceberg v2 snapshot sequence-number must be an integer"))?;
                    if sequence_number < 0 || sequence_number > last_sequence_number {
                        return Err(s3_error!(
                            InvalidRequest,
                            "Iceberg v2 snapshot sequence-number must be between zero and last-sequence-number"
                        ));
                    }
                }
            }
        }
        _ => return Err(s3_error!(InvalidRequest, "unsupported Iceberg table format-version")),
    }
    validate_table_metadata_references(metadata)
}

fn validate_metadata_id_reference(
    metadata: &serde_json::Value,
    reference_field: &str,
    ids: &BTreeSet<i64>,
    label: &str,
) -> S3Result<()> {
    let Some(value) = metadata.get(reference_field) else {
        return Ok(());
    };
    let id = value
        .as_i64()
        .ok_or_else(|| s3_error!(InvalidRequest, "{reference_field} must be an integer"))?;
    if !ids.contains(&id) {
        return Err(s3_error!(InvalidRequest, "{reference_field} targets {label} {id}, which does not exist"));
    }
    Ok(())
}

fn validate_table_metadata_references(metadata: &serde_json::Value) -> S3Result<()> {
    let schema_ids = metadata_array_i32_ids(metadata, "schemas", "schema-id", "schema")?;
    validate_metadata_id_reference(metadata, "current-schema-id", &schema_ids, "schema")?;
    let spec_ids = metadata_array_i32_ids(metadata, "partition-specs", "spec-id", "partition spec")?;
    validate_metadata_id_reference(metadata, "default-spec-id", &spec_ids, "partition spec")?;
    let sort_order_ids = metadata_array_i32_ids(metadata, "sort-orders", "order-id", "sort order")?;
    validate_metadata_id_reference(metadata, "default-sort-order-id", &sort_order_ids, "sort order")?;
    let snapshot_ids = metadata_array_ids(metadata, "snapshots", "snapshot-id", "snapshot")?;

    if let Some(current_snapshot_id) = metadata.get("current-snapshot-id").filter(|value| !value.is_null()) {
        let current_snapshot_id = current_snapshot_id
            .as_i64()
            .ok_or_else(|| s3_error!(InvalidRequest, "current-snapshot-id must be an integer"))?;
        if current_snapshot_id != -1 && !snapshot_ids.contains(&current_snapshot_id) {
            return Err(s3_error!(
                InvalidRequest,
                "current snapshot {current_snapshot_id} does not exist in table metadata"
            ));
        }
    }
    if let Some(refs) = metadata.get("refs").filter(|value| !value.is_null()) {
        let refs = refs
            .as_object()
            .ok_or_else(|| s3_error!(InvalidRequest, "refs must be an object"))?;
        for (name, reference) in refs {
            let snapshot_id = reference
                .get("snapshot-id")
                .and_then(serde_json::Value::as_i64)
                .ok_or_else(|| s3_error!(InvalidRequest, "snapshot ref {name} is missing snapshot-id"))?;
            if !snapshot_ids.contains(&snapshot_id) {
                return Err(s3_error!(
                    InvalidRequest,
                    "snapshot ref {name} targets snapshot {snapshot_id}, which does not exist"
                ));
            }
        }
    }
    if let Some(snapshots) = metadata.get("snapshots").and_then(serde_json::Value::as_array) {
        for snapshot in snapshots {
            if let Some(schema_id) = snapshot.get("schema-id") {
                let schema_id = schema_id
                    .as_i64()
                    .ok_or_else(|| s3_error!(InvalidRequest, "snapshot schema-id must be an integer"))?;
                if !schema_ids.contains(&schema_id) {
                    return Err(s3_error!(
                        InvalidRequest,
                        "snapshot schema-id targets schema {schema_id}, which does not exist"
                    ));
                }
            }
        }
    }
    Ok(())
}

fn table_metadata_partition_spec_ids(metadata: &serde_json::Value) -> S3Result<BTreeSet<i32>> {
    let ids = metadata_array_i32_ids(metadata, "partition-specs", "spec-id", "partition spec")?;
    if !ids.is_empty() {
        return ids
            .into_iter()
            .map(|id| {
                i32::try_from(id).map_err(|_| s3_error!(InvalidRequest, "partition spec id {id} exceeds the signed 32-bit range"))
            })
            .collect();
    }
    if metadata.get("partition-spec").is_some_and(serde_json::Value::is_array) || metadata.get("partition-specs").is_none() {
        return Ok(BTreeSet::from([0]));
    }
    Err(s3_error!(InvalidRequest, "table metadata has no partition specs"))
}

fn validate_view_metadata_references(metadata: &serde_json::Value) -> S3Result<()> {
    let schema_ids = metadata_array_i32_ids(metadata, "schemas", "schema-id", "schema")?;
    validate_metadata_id_reference(metadata, "current-schema-id", &schema_ids, "schema")?;
    let version_ids = metadata_array_i32_ids(metadata, "versions", "version-id", "view version")?;
    validate_metadata_id_reference(metadata, "current-version-id", &version_ids, "view version")?;
    if let Some(versions) = metadata.get("versions").and_then(serde_json::Value::as_array) {
        for version in versions {
            let schema_id = version
                .get("schema-id")
                .and_then(serde_json::Value::as_i64)
                .ok_or_else(|| s3_error!(InvalidRequest, "view version is missing schema-id"))?;
            if !schema_ids.contains(&schema_id) {
                return Err(s3_error!(
                    InvalidRequest,
                    "view version schema-id targets schema {schema_id}, which does not exist"
                ));
            }
        }
    }
    Ok(())
}

fn validate_supported_view_metadata(metadata: &serde_json::Value) -> S3Result<()> {
    let format_version = i64::from(metadata_format_version(metadata)?);
    if format_version != ICEBERG_VIEW_FORMAT_VERSION {
        return Err(iceberg_rest_error(
            ICEBERG_ERROR_UNSUPPORTED_OPERATION,
            StatusCode::NOT_ACCEPTABLE,
            format!("unsupported Iceberg view format-version: {format_version}"),
        ));
    }
    validate_view_metadata_references(metadata)
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
            _ => return Err(s3_error!(InvalidRequest, "unsupported view commit requirement: {requirement_type}")),
        }
    }
    Ok(())
}

fn apply_view_commit_updates_at(
    mut metadata: serde_json::Value,
    updates: &[serde_json::Value],
    previous_metadata_location: &str,
    commit_timestamp_ms: i64,
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
            "assign-uuid" => apply_assign_uuid_update(&mut metadata, update, "view-uuid", "view")?,
            "upgrade-format-version" => apply_upgrade_view_format_version_update(&mut metadata, update)?,
            "add-schema" => apply_add_schema_update(&mut metadata, update)?,
            "set-current-schema" => apply_set_current_schema_update(&mut metadata, update)?,
            "add-view-version" => apply_add_view_version_update(&mut metadata, update, commit_timestamp_ms)?,
            "set-current-view-version" => apply_set_current_view_version_update(&mut metadata, update, commit_timestamp_ms)?,
            "set-location" => apply_set_location_update(&mut metadata, update)?,
            "set-properties" => apply_set_properties_update(&mut metadata, update)?,
            "remove-properties" => apply_remove_properties_update(&mut metadata, update)?,
            _ => return Err(s3_error!(InvalidRequest, "unsupported view update: {action}")),
        }
    }

    validate_supported_view_metadata(&metadata)?;
    append_previous_metadata_log(&mut metadata, previous_metadata_location, commit_timestamp_ms)?;
    metadata_object_mut(&mut metadata)?.insert("last-updated-ms".to_string(), serde_json::Value::from(commit_timestamp_ms));
    Ok(metadata)
}

fn apply_set_snapshot_file_update(
    metadata: &mut serde_json::Value,
    update: &serde_json::Value,
    metadata_field: &str,
    update_field: &str,
) -> S3Result<()> {
    let value = update
        .get(update_field)
        .cloned()
        .ok_or_else(|| s3_error!(InvalidRequest, "{update_field} is required"))?;
    let snapshot_id = value
        .get("snapshot-id")
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| s3_error!(InvalidRequest, "{update_field}.snapshot-id must be an integer"))?;
    if update
        .get("snapshot-id")
        .and_then(serde_json::Value::as_i64)
        .is_some_and(|deprecated_snapshot_id| deprecated_snapshot_id != snapshot_id)
    {
        return Err(s3_error!(InvalidRequest, "{update_field}.snapshot-id does not match snapshot-id"));
    }
    require_snapshot_id(metadata, snapshot_id)?;
    let values = ensure_array_field(metadata, metadata_field)?;
    values.retain(|value| value.get("snapshot-id").and_then(serde_json::Value::as_i64) != Some(snapshot_id));
    values.push(value);
    Ok(())
}

fn apply_remove_snapshot_file_update(
    metadata: &mut serde_json::Value,
    update: &serde_json::Value,
    metadata_field: &str,
) -> S3Result<()> {
    let snapshot_id = update
        .get("snapshot-id")
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| s3_error!(InvalidRequest, "remove update requires snapshot-id"))?;
    if let Some(values) = metadata.get_mut(metadata_field).and_then(serde_json::Value::as_array_mut) {
        values.retain(|value| value.get("snapshot-id").and_then(serde_json::Value::as_i64) != Some(snapshot_id));
    }
    Ok(())
}

fn apply_remove_metadata_ids_update(
    metadata: &mut serde_json::Value,
    update: &serde_json::Value,
    metadata_field: &str,
    id_field: &str,
    update_field: &str,
) -> S3Result<()> {
    let ids = update
        .get(update_field)
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| s3_error!(InvalidRequest, "{update_field} must be an array"))?
        .iter()
        .map(|value| {
            value
                .as_i64()
                .ok_or_else(|| s3_error!(InvalidRequest, "{update_field} must contain integers"))
        })
        .collect::<S3Result<BTreeSet<_>>>()?;
    if let Some(values) = metadata.get_mut(metadata_field).and_then(serde_json::Value::as_array_mut) {
        values.retain(|value| {
            value
                .get(id_field)
                .and_then(serde_json::Value::as_i64)
                .is_none_or(|id| !ids.contains(&id))
        });
    }
    Ok(())
}

fn apply_add_encryption_key_update(metadata: &mut serde_json::Value, update: &serde_json::Value) -> S3Result<()> {
    let encryption_key = update
        .get("encryption-key")
        .cloned()
        .ok_or_else(|| s3_error!(InvalidRequest, "add-encryption-key requires encryption-key"))?;
    let key_id = encryption_key
        .get("key-id")
        .and_then(serde_json::Value::as_str)
        .filter(|key_id| !key_id.is_empty())
        .ok_or_else(|| s3_error!(InvalidRequest, "encryption-key.key-id is required"))?
        .to_string();
    let values = ensure_array_field(metadata, "encryption-keys")?;
    if values
        .iter()
        .any(|value| value.get("key-id").and_then(serde_json::Value::as_str) == Some(key_id.as_str()))
    {
        return Err(s3_error!(PreconditionFailed, "encryption key already exists"));
    }
    values.push(encryption_key);
    Ok(())
}

fn apply_remove_encryption_key_update(metadata: &mut serde_json::Value, update: &serde_json::Value) -> S3Result<()> {
    let key_id = update
        .get("key-id")
        .and_then(serde_json::Value::as_str)
        .filter(|key_id| !key_id.is_empty())
        .ok_or_else(|| s3_error!(InvalidRequest, "remove-encryption-key requires key-id"))?;
    if let Some(values) = metadata.get_mut("encryption-keys").and_then(serde_json::Value::as_array_mut) {
        values.retain(|value| value.get("key-id").and_then(serde_json::Value::as_str) != Some(key_id));
    }
    Ok(())
}

fn require_snapshot_id(metadata: &serde_json::Value, snapshot_id: i64) -> S3Result<()> {
    if metadata
        .get("snapshots")
        .and_then(serde_json::Value::as_array)
        .is_some_and(|snapshots| {
            snapshots
                .iter()
                .any(|snapshot| snapshot.get("snapshot-id").and_then(serde_json::Value::as_i64) == Some(snapshot_id))
        })
    {
        Ok(())
    } else {
        Err(s3_error!(InvalidRequest, "snapshot {snapshot_id} does not exist"))
    }
}

fn apply_assign_uuid_update(
    metadata: &mut serde_json::Value,
    update: &serde_json::Value,
    uuid_field: &str,
    entity: &str,
) -> S3Result<()> {
    let uuid = update
        .get("uuid")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| s3_error!(InvalidRequest, "assign-uuid requires uuid"))?;
    let object = metadata_object_mut(metadata)?;
    if let Some(existing) = object.get(uuid_field).and_then(serde_json::Value::as_str)
        && existing != uuid
    {
        return Err(s3_error!(PreconditionFailed, "cannot reassign {entity} uuid"));
    }
    object.insert(uuid_field.to_string(), serde_json::Value::String(uuid.to_string()));
    Ok(())
}

fn apply_add_view_version_update(
    metadata: &mut serde_json::Value,
    update: &serde_json::Value,
    commit_timestamp_ms: i64,
) -> S3Result<()> {
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
    if view_version.get("schema-id").and_then(serde_json::Value::as_i64) == Some(-1) {
        let schema_id = last_array_object_i64(metadata, "schemas", "schema-id")?;
        view_version
            .as_object_mut()
            .ok_or_else(|| s3_error!(InvalidRequest, "view-version must be a JSON object"))?
            .insert("schema-id".to_string(), serde_json::Value::from(schema_id));
    }
    view_version
        .as_object_mut()
        .ok_or_else(|| s3_error!(InvalidRequest, "view-version must be a JSON object"))?
        .entry("timestamp-ms".to_string())
        .or_insert_with(|| serde_json::Value::from(commit_timestamp_ms));
    ensure_array_field(metadata, "versions")?.push(view_version);
    Ok(())
}

fn apply_set_current_view_version_update(
    metadata: &mut serde_json::Value,
    update: &serde_json::Value,
    commit_timestamp_ms: i64,
) -> S3Result<()> {
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
        "timestamp-ms": commit_timestamp_ms,
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
        .ok_or_else(|| s3_error!(InvalidRequest, "current table metadata is missing format-version"))?;
    if !(1..=2).contains(&version) {
        return Err(iceberg_rest_error(
            ICEBERG_ERROR_UNSUPPORTED_OPERATION,
            StatusCode::NOT_ACCEPTABLE,
            format!("unsupported Iceberg table format-version: {version}"),
        ));
    }
    if version < current {
        return Err(s3_error!(InvalidRequest, "format-version cannot be downgraded"));
    }
    if current == 1
        && version == 2
        && let Some(snapshots) = metadata.get_mut("snapshots").and_then(serde_json::Value::as_array_mut)
    {
        for snapshot in snapshots {
            snapshot
                .as_object_mut()
                .ok_or_else(|| s3_error!(InvalidRequest, "snapshot must be an object"))?
                .entry("sequence-number".to_string())
                .or_insert_with(|| serde_json::Value::from(0));
        }
    }
    metadata_object_mut(metadata)?.insert("format-version".to_string(), serde_json::Value::from(version));
    Ok(())
}

fn apply_upgrade_view_format_version_update(metadata: &mut serde_json::Value, update: &serde_json::Value) -> S3Result<()> {
    let version = update
        .get("format-version")
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| s3_error!(InvalidRequest, "upgrade-format-version requires format-version"))?;
    if version != ICEBERG_VIEW_FORMAT_VERSION {
        return Err(iceberg_rest_error(
            ICEBERG_ERROR_UNSUPPORTED_OPERATION,
            StatusCode::NOT_ACCEPTABLE,
            format!("unsupported Iceberg view format-version: {version}"),
        ));
    }
    apply_upgrade_format_version_update(metadata, update)
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
    let format_version = metadata
        .get("format-version")
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| s3_error!(InvalidRequest, "current table metadata is missing format-version"))?;
    if !matches!(format_version, 1 | 2) {
        return Err(iceberg_rest_error(
            ICEBERG_ERROR_UNSUPPORTED_OPERATION,
            StatusCode::NOT_ACCEPTABLE,
            format!("unsupported Iceberg table format-version: {format_version}"),
        ));
    }
    let sequence_number = if format_version == 1 {
        snapshot
            .get("sequence-number")
            .map(|value| {
                value
                    .as_i64()
                    .filter(|sequence_number| *sequence_number == 0)
                    .ok_or_else(|| s3_error!(InvalidRequest, "v1 snapshot sequence-number must be zero when present"))
            })
            .transpose()?
            .unwrap_or(0)
    } else {
        snapshot
            .get("sequence-number")
            .and_then(serde_json::Value::as_i64)
            .ok_or_else(|| s3_error!(InvalidRequest, "snapshot sequence-number must be an integer"))?
    };
    snapshot
        .get("timestamp-ms")
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| s3_error!(InvalidRequest, "snapshot timestamp-ms must be an integer"))?;
    validate_added_snapshot(metadata, &snapshot, snapshot_id, sequence_number, format_version)?;
    ensure_array_field(metadata, "snapshots")?.push(snapshot);
    let object = metadata_object_mut(metadata)?;
    if format_version >= 2 {
        object.insert("last-sequence-number".to_string(), serde_json::Value::from(sequence_number));
    }
    Ok(())
}

fn validate_added_snapshot(
    metadata: &serde_json::Value,
    snapshot: &serde_json::Value,
    snapshot_id: i64,
    sequence_number: i64,
    format_version: i64,
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

    if let Some(parent_snapshot_id) = snapshot.get("parent-snapshot-id") {
        let parent_snapshot_id = parent_snapshot_id
            .as_i64()
            .ok_or_else(|| s3_error!(InvalidRequest, "snapshot parent-snapshot-id must be an integer"))?;
        if !metadata
            .get("snapshots")
            .and_then(serde_json::Value::as_array)
            .is_some_and(|snapshots| {
                snapshots
                    .iter()
                    .any(|snapshot| snapshot.get("snapshot-id").and_then(serde_json::Value::as_i64) == Some(parent_snapshot_id))
            })
        {
            return Err(s3_error!(PreconditionFailed, "snapshot parent does not exist"));
        }
    }

    if format_version >= 2 {
        let current_sequence_number = metadata
            .get("last-sequence-number")
            .and_then(serde_json::Value::as_i64)
            .ok_or_else(|| s3_error!(InvalidRequest, "current table metadata is missing last-sequence-number"))?;
        if sequence_number <= current_sequence_number {
            return Err(s3_error!(PreconditionFailed, "snapshot sequence number must advance"));
        }
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

#[derive(Default)]
struct SnapshotReadBudget {
    manifest_count: usize,
    avro_bytes: usize,
    file_reference_count: usize,
    manifest_lists: BTreeMap<String, Vec<crate::table_catalog::ManifestListReference>>,
    manifests: BTreeMap<String, Vec<crate::table_catalog::ManifestDataFileReference>>,
}

impl SnapshotReadBudget {
    fn charge_manifests(&mut self, count: usize) -> S3Result<()> {
        self.manifest_count = self
            .manifest_count
            .checked_add(count)
            .ok_or_else(|| s3_error!(InvalidRequest, "snapshot manifest count exceeds the commit limit"))?;
        if self.manifest_count > TABLE_COMMIT_MAX_MANIFESTS {
            return Err(s3_error!(InvalidRequest, "snapshot manifest count exceeds the commit limit"));
        }
        Ok(())
    }

    fn charge_avro_bytes(&mut self, count: usize) -> S3Result<()> {
        self.avro_bytes = self
            .avro_bytes
            .checked_add(count)
            .ok_or_else(|| s3_error!(InvalidRequest, "snapshot Avro bytes exceed the commit limit"))?;
        if self.avro_bytes > TABLE_COMMIT_MAX_AVRO_BYTES {
            return Err(s3_error!(InvalidRequest, "snapshot Avro bytes exceed the commit limit"));
        }
        Ok(())
    }

    fn charge_file_references(&mut self, count: usize) -> S3Result<()> {
        self.file_reference_count = self
            .file_reference_count
            .checked_add(count)
            .ok_or_else(|| s3_error!(InvalidRequest, "snapshot file references exceed the commit limit"))?;
        if self.file_reference_count > TABLE_COMMIT_MAX_FILE_REFERENCES {
            return Err(s3_error!(InvalidRequest, "snapshot file references exceed the commit limit"));
        }
        Ok(())
    }
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
    metadata: &'a serde_json::Value,
    snapshot: &'a serde_json::Value,
    current_live_files: &'a SnapshotLiveFiles,
    snapshot_id: i64,
    sequence_number: i64,
}

struct SnapshotReadContext<'a, B> {
    metadata_backend: &'a B,
    bucket: &'a str,
    namespace: &'a crate::table_catalog::Namespace,
    table: &'a crate::table_catalog::IdentifierSegment,
    entry: &'a crate::table_catalog::TableEntry,
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
    context: &SnapshotReadContext<'_, B>,
    current_metadata: &serde_json::Value,
    next_metadata: &serde_json::Value,
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
    let format_version = current_metadata
        .get("format-version")
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| s3_error!(InvalidRequest, "current table metadata is missing format-version"))?;
    let sequence_number = if format_version == 1 {
        snapshot
            .get("sequence-number")
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(0)
    } else {
        snapshot
            .get("sequence-number")
            .and_then(serde_json::Value::as_i64)
            .ok_or_else(|| s3_error!(InvalidRequest, "snapshot sequence-number must be an integer"))?
    };
    let operation = snapshot
        .get("summary")
        .and_then(|summary| summary.get("operation"))
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| s3_error!(InvalidRequest, "snapshot summary.operation is required"))?;

    let parent_snapshot_id = snapshot
        .get("parent-snapshot-id")
        .map(|snapshot_id| {
            snapshot_id
                .as_i64()
                .ok_or_else(|| s3_error!(InvalidRequest, "snapshot parent-snapshot-id must be an integer"))
        })
        .transpose()?;
    let mut read_budget = SnapshotReadBudget::default();
    let parent_live_files = load_snapshot_live_files(context, current_metadata, parent_snapshot_id, &mut read_budget).await?;
    let changes = load_snapshot_file_changes(
        context,
        SnapshotChangeContext {
            metadata: next_metadata,
            snapshot,
            current_live_files: &parent_live_files,
            snapshot_id,
            sequence_number,
        },
        &mut read_budget,
    )
    .await?;

    for location in changes.added_data_files.iter().chain(changes.added_delete_files.iter()) {
        if parent_live_files.contains(location) {
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
            if parent_snapshot_id.is_none() {
                return Err(s3_error!(InvalidRequest, "row-level snapshot operation requires a parent snapshot"));
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
                if !parent_live_files.contains(location) {
                    return Err(s3_error!(
                        PreconditionFailed,
                        "commit requirement failed: deleted file is not in the parent snapshot"
                    ));
                }
            }
        }
        _ => return Err(s3_error!(NotImplemented, "unsupported snapshot operation: {operation}")),
    }

    Ok(())
}

async fn validate_table_snapshot_graph<B>(context: &SnapshotReadContext<'_, B>, metadata: &serde_json::Value) -> S3Result<()>
where
    B: crate::table_catalog::TableCatalogObjectBackend,
{
    validate_supported_table_metadata(metadata)?;
    let Some(snapshots) = metadata.get("snapshots") else {
        return Ok(());
    };
    let snapshots = snapshots
        .as_array()
        .ok_or_else(|| s3_error!(InvalidRequest, "snapshots must be an array"))?;
    let mut read_budget = SnapshotReadBudget::default();
    for snapshot in snapshots {
        let snapshot_id = snapshot
            .get("snapshot-id")
            .and_then(serde_json::Value::as_i64)
            .ok_or_else(|| s3_error!(InvalidRequest, "snapshot-id must be an integer"))?;
        load_snapshot_live_files(context, metadata, Some(snapshot_id), &mut read_budget).await?;
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

async fn load_snapshot_live_files<B>(
    context: &SnapshotReadContext<'_, B>,
    current_metadata: &serde_json::Value,
    snapshot_id: Option<i64>,
    read_budget: &mut SnapshotReadBudget,
) -> S3Result<SnapshotLiveFiles>
where
    B: crate::table_catalog::TableCatalogObjectBackend,
{
    let Some(snapshot_id) = snapshot_id else {
        return Ok(SnapshotLiveFiles::default());
    };
    let snapshot = current_metadata
        .get("snapshots")
        .and_then(serde_json::Value::as_array)
        .and_then(|snapshots| {
            snapshots
                .iter()
                .find(|snapshot| snapshot.get("snapshot-id").and_then(serde_json::Value::as_i64) == Some(snapshot_id))
        })
        .ok_or_else(|| s3_error!(InvalidRequest, "parent snapshot metadata is missing"))?;

    let mut live_files = SnapshotLiveFiles::default();
    let mut seen_locations = BTreeSet::new();
    for manifest in read_snapshot_manifest_references(context, current_metadata, snapshot, read_budget).await? {
        let SnapshotManifestLocation {
            manifest_path,
            partition_spec_id: _,
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
                    if !seen_locations.insert(reference.location.clone()) {
                        return Err(s3_error!(InvalidRequest, "snapshot contains a duplicate file reference"));
                    }
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
    read_context: &SnapshotReadContext<'_, B>,
    change_context: SnapshotChangeContext<'_>,
    read_budget: &mut SnapshotReadBudget,
) -> S3Result<SnapshotFileChanges>
where
    B: crate::table_catalog::TableCatalogObjectBackend,
{
    let mut changes = SnapshotFileChanges::default();
    let mut seen_locations = BTreeSet::new();
    for manifest in
        read_snapshot_manifest_references(read_context, change_context.metadata, change_context.snapshot, read_budget).await?
    {
        for reference in &manifest.references {
            if !seen_locations.insert(reference.location.clone()) {
                return Err(s3_error!(InvalidRequest, "snapshot contains a duplicate file reference"));
            }
        }
        let inherited_identity = change_context
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
            .is_some_and(|added_snapshot_id| added_snapshot_id != change_context.snapshot_id)
        {
            return Err(s3_error!(InvalidRequest, "new manifest must belong to the committed snapshot"));
        }
        if manifest
            .location
            .sequence_number
            .is_some_and(|sequence_number| sequence_number != change_context.sequence_number)
        {
            return Err(s3_error!(InvalidRequest, "new manifest sequence must match the committed snapshot"));
        }

        for reference in manifest.references {
            let status = reference
                .entry_status
                .ok_or_else(|| s3_error!(InvalidRequest, "manifest entry status is required"))?;
            if matches!(status, 1 | 2) && reference.snapshot_id != Some(change_context.snapshot_id) {
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
                && change_context
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
            if status == 0 {
                let Some(current_identity) = change_context
                    .current_live_files
                    .identity(&reference.location, &reference.object_kind)
                else {
                    return Err(s3_error!(
                        PreconditionFailed,
                        "commit requirement failed: existing file is not in the parent snapshot"
                    ));
                };
                if current_identity
                    != &(SnapshotFileIdentity {
                        sequence_number: reference.sequence_number,
                        file_sequence_number: reference.file_sequence_number,
                    })
                {
                    return Err(s3_error!(
                        InvalidRequest,
                        "existing manifest entry must preserve the parent file sequence"
                    ));
                }
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
    context: &SnapshotReadContext<'_, B>,
    metadata: &serde_json::Value,
    snapshot: &serde_json::Value,
    read_budget: &mut SnapshotReadBudget,
) -> S3Result<Vec<SnapshotManifestReferences>>
where
    B: crate::table_catalog::TableCatalogObjectBackend,
{
    let manifest_locations = snapshot_manifest_locations(context, snapshot, read_budget).await?;
    let partition_spec_ids = table_metadata_partition_spec_ids(metadata)?;
    let format_version = metadata_format_version(metadata)?;
    let mut manifests = Vec::new();
    let mut seen_manifest_paths = BTreeSet::new();
    for manifest_location in manifest_locations {
        match manifest_location.partition_spec_id {
            Some(partition_spec_id) if !partition_spec_ids.contains(&partition_spec_id) => {
                return Err(s3_error!(
                    InvalidRequest,
                    "snapshot manifest references missing partition spec {partition_spec_id}"
                ));
            }
            None if format_version == 2 => {
                return Err(s3_error!(InvalidRequest, "Iceberg v2 manifest-list entry is missing partition_spec_id"));
            }
            _ => {}
        }
        if !seen_manifest_paths.insert(manifest_location.manifest_path.clone()) {
            return Err(s3_error!(InvalidRequest, "snapshot contains a duplicate manifest reference"));
        }
        let manifest_key = table_commit_object_key(
            context.bucket,
            context.namespace,
            context.table,
            context.entry,
            &manifest_location.manifest_path,
            crate::table_catalog::TableMetadataMaintenanceObjectKind::ManifestFile,
        )?;
        let file_references = if let Some(references) = read_budget.manifests.get(&manifest_key).cloned() {
            references
        } else {
            let manifest_object = context
                .metadata_backend
                .read_object_limited(context.bucket, &manifest_key, crate::table_catalog::TABLE_MANIFEST_AVRO_MAX_SIZE)
                .await
                .map_err(catalog_store_error)?
                .ok_or_else(|| s3_error!(InvalidRequest, "snapshot manifest object is missing"))?;
            read_budget.charge_avro_bytes(manifest_object.data.len())?;
            let references = tokio::task::spawn_blocking(move || {
                crate::table_catalog::data_file_references_from_manifest_avro(&manifest_object.data)
            })
            .await
            .map_err(|err| s3_error!(InternalError, "snapshot manifest parser task failed: {err}"))?
            .map_err(catalog_store_error)?;
            read_budget.manifests.insert(manifest_key, references.clone());
            references
        };
        read_budget.charge_file_references(file_references.len())?;
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
            references.push(reference);
        }
        validate_manifest_data_file_references(context, &references).await?;
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
    partition_spec_id: Option<i32>,
    sequence_number: Option<i64>,
    added_snapshot_id: Option<i64>,
}

async fn snapshot_manifest_locations<B>(
    context: &SnapshotReadContext<'_, B>,
    snapshot: &serde_json::Value,
    read_budget: &mut SnapshotReadBudget,
) -> S3Result<Vec<SnapshotManifestLocation>>
where
    B: crate::table_catalog::TableCatalogObjectBackend,
{
    if let Some(manifest_list_location) = snapshot.get("manifest-list").and_then(serde_json::Value::as_str) {
        let manifest_list_key = table_commit_object_key(
            context.bucket,
            context.namespace,
            context.table,
            context.entry,
            manifest_list_location,
            crate::table_catalog::TableMetadataMaintenanceObjectKind::ManifestList,
        )?;
        let references = if let Some(references) = read_budget.manifest_lists.get(&manifest_list_key).cloned() {
            references
        } else {
            let manifest_list_object = context
                .metadata_backend
                .read_object_limited(context.bucket, &manifest_list_key, crate::table_catalog::TABLE_MANIFEST_AVRO_MAX_SIZE)
                .await
                .map_err(catalog_store_error)?
                .ok_or_else(|| s3_error!(InvalidRequest, "snapshot manifest-list object is missing"))?;
            read_budget.charge_avro_bytes(manifest_list_object.data.len())?;
            let references = tokio::task::spawn_blocking(move || {
                crate::table_catalog::manifest_list_references_from_manifest_list_avro(&manifest_list_object.data)
            })
            .await
            .map_err(|err| s3_error!(InternalError, "snapshot manifest-list parser task failed: {err}"))?
            .map_err(catalog_store_error)?;
            read_budget.manifest_lists.insert(manifest_list_key, references.clone());
            references
        };
        if references.is_empty() {
            return Err(s3_error!(InvalidRequest, "snapshot manifest-list must reference at least one manifest"));
        }
        read_budget.charge_manifests(references.len())?;
        return Ok(references
            .into_iter()
            .map(|reference| SnapshotManifestLocation {
                manifest_path: reference.manifest_path,
                partition_spec_id: reference.partition_spec_id,
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
    read_budget.charge_manifests(manifests.len())?;
    manifests
        .iter()
        .map(|manifest| {
            manifest
                .as_str()
                .filter(|manifest| !manifest.is_empty())
                .map(|manifest| SnapshotManifestLocation {
                    manifest_path: manifest.to_string(),
                    partition_spec_id: None,
                    sequence_number: None,
                    added_snapshot_id: None,
                })
                .ok_or_else(|| s3_error!(InvalidRequest, "snapshot manifest location must be a string"))
        })
        .collect()
}

async fn validate_manifest_data_file_references<B>(
    context: &SnapshotReadContext<'_, B>,
    references: &[crate::table_catalog::ManifestDataFileReference],
) -> S3Result<()>
where
    B: crate::table_catalog::TableCatalogObjectBackend,
{
    for references in references.chunks(TABLE_COMMIT_OBJECT_VALIDATION_CONCURRENCY) {
        let mut object_keys = Vec::with_capacity(references.len());
        for reference in references {
            let object_key = table_commit_object_key(
                context.bucket,
                context.namespace,
                context.table,
                context.entry,
                &reference.location,
                reference.object_kind.clone(),
            )?;
            if reference.entry_status != Some(2) {
                object_keys.push(object_key);
            }
        }

        let metadata_backend = context.metadata_backend.clone();
        let bucket = context.bucket.to_string();
        stream::iter(object_keys)
            .map(move |object_key| {
                let metadata_backend = metadata_backend.clone();
                let bucket = bucket.clone();
                async move {
                    if !metadata_backend
                        .object_exists(&bucket, &object_key)
                        .await
                        .map_err(catalog_store_error)?
                    {
                        return Err(s3_error!(InvalidRequest, "manifest referenced data file is missing"));
                    }
                    Ok(())
                }
            })
            .buffered(TABLE_COMMIT_OBJECT_VALIDATION_CONCURRENCY)
            .try_for_each(|()| async { Ok(()) })
            .await?;
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
    if entry.table_bucket != bucket {
        return Err(s3_error!(InvalidRequest, "snapshot object is outside the table bucket"));
    }
    crate::table_catalog::table_reference_object_key(namespace, table, entry, location, expected_kind)
        .ok_or_else(|| s3_error!(InvalidRequest, "snapshot object is outside the table warehouse"))
}

fn apply_set_snapshot_ref_update(
    metadata: &mut serde_json::Value,
    update: &serde_json::Value,
    commit_timestamp_ms: i64,
) -> S3Result<()> {
    let ref_name = update
        .get("ref-name")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| s3_error!(InvalidRequest, "set-snapshot-ref requires ref-name"))?;
    let snapshot_id = update
        .get("snapshot-id")
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| s3_error!(InvalidRequest, "set-snapshot-ref requires snapshot-id"))?;
    let ref_type = update
        .get("type")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| s3_error!(InvalidRequest, "set-snapshot-ref requires type"))?;
    if !matches!(ref_type, "branch" | "tag") {
        return Err(s3_error!(InvalidRequest, "set-snapshot-ref type must be branch or tag"));
    }
    if ref_name == "main" && ref_type != "branch" {
        return Err(s3_error!(InvalidRequest, "main snapshot ref must be a branch"));
    }
    let snapshot_exists = metadata
        .get("snapshots")
        .and_then(serde_json::Value::as_array)
        .is_some_and(|snapshots| {
            snapshots
                .iter()
                .any(|snapshot| snapshot.get("snapshot-id").and_then(serde_json::Value::as_i64) == Some(snapshot_id))
        });
    if !snapshot_exists {
        return Err(s3_error!(
            InvalidRequest,
            "snapshot ref {ref_name} targets snapshot {snapshot_id}, which does not exist"
        ));
    }
    let reference = update
        .as_object()
        .ok_or_else(|| s3_error!(InvalidRequest, "set-snapshot-ref must be a JSON object"))?
        .iter()
        .filter(|(key, _)| key.as_str() != "action" && key.as_str() != "ref-name")
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<serde_json::Map<_, _>>();
    ensure_object_field(metadata, "refs")?.insert(ref_name.to_string(), serde_json::Value::Object(reference));
    if ref_name == "main" && metadata.get("current-snapshot-id").and_then(serde_json::Value::as_i64) != Some(snapshot_id) {
        metadata_object_mut(metadata)?.insert("current-snapshot-id".to_string(), serde_json::Value::from(snapshot_id));
        ensure_array_field(metadata, "snapshot-log")?.push(serde_json::json!({
            "timestamp-ms": commit_timestamp_ms,
            "snapshot-id": snapshot_id
        }));
    }
    Ok(())
}

fn apply_remove_snapshots_update(metadata: &mut serde_json::Value, update: &serde_json::Value) -> S3Result<()> {
    let ids = update
        .get("snapshot-ids")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| s3_error!(InvalidRequest, "remove-snapshots requires snapshot-ids"))?
        .iter()
        .map(|snapshot_id| {
            snapshot_id
                .as_i64()
                .ok_or_else(|| s3_error!(InvalidRequest, "remove-snapshots snapshot-ids must contain integers"))
        })
        .collect::<S3Result<std::collections::BTreeSet<_>>>()?;
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
    if ref_name == "main" {
        return Err(s3_error!(InvalidRequest, "main snapshot ref cannot be deleted"));
    }
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

fn append_previous_metadata_log(
    metadata: &mut serde_json::Value,
    previous_metadata_location: &str,
    commit_timestamp_ms: i64,
) -> S3Result<()> {
    ensure_array_field(metadata, "metadata-log")?.push(serde_json::json!({
        "timestamp-ms": commit_timestamp_ms,
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
    let next = last_array_object_i64(metadata, array_key, id_key)?
        .checked_add(1)
        .ok_or_else(|| s3_error!(InvalidRequest, "metadata field {array_key} has exhausted signed 32-bit {id_key} values"))?;
    if next > i64::from(i32::MAX) {
        return Err(s3_error!(
            InvalidRequest,
            "metadata field {array_key} has exhausted signed 32-bit {id_key} values"
        ));
    }
    Ok(next)
}

fn last_array_object_i64(metadata: &serde_json::Value, array_key: &str, id_key: &str) -> S3Result<i64> {
    let values = metadata
        .get(array_key)
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| s3_error!(InvalidRequest, "metadata field {array_key} must be an array"))?;
    let id = values
        .iter()
        .filter_map(|value| value.get(id_key).and_then(serde_json::Value::as_i64))
        .max()
        .ok_or_else(|| s3_error!(InvalidRequest, "metadata field {array_key} has no {id_key}"))?;
    i32::try_from(id)
        .map(i64::from)
        .map_err(|_| s3_error!(InvalidRequest, "metadata field {array_key} has an out-of-range {id_key}"))
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
    let namespace = namespace_from_segments(request.namespace)?;
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
        crate::table_catalog::TableCatalogStoreError::NamespaceNotFound(message) => {
            iceberg_rest_error(ICEBERG_ERROR_NO_SUCH_NAMESPACE, StatusCode::NOT_FOUND, message)
        }
        crate::table_catalog::TableCatalogStoreError::TableNotFound(message) => {
            iceberg_rest_error(ICEBERG_ERROR_NO_SUCH_TABLE, StatusCode::NOT_FOUND, message)
        }
        crate::table_catalog::TableCatalogStoreError::ViewNotFound(message) => {
            iceberg_rest_error(ICEBERG_ERROR_NO_SUCH_VIEW, StatusCode::NOT_FOUND, message)
        }
        crate::table_catalog::TableCatalogStoreError::AlreadyExists(message) => {
            iceberg_rest_error(ICEBERG_ERROR_ALREADY_EXISTS, StatusCode::CONFLICT, message)
        }
        crate::table_catalog::TableCatalogStoreError::Conflict(message) => {
            iceberg_rest_error(ICEBERG_ERROR_COMMIT_FAILED, StatusCode::CONFLICT, message)
        }
        crate::table_catalog::TableCatalogStoreError::Invalid(message) => {
            iceberg_rest_error(ICEBERG_ERROR_BAD_REQUEST, StatusCode::BAD_REQUEST, message)
        }
        crate::table_catalog::TableCatalogStoreError::Unsupported(message) => {
            iceberg_rest_error(ICEBERG_ERROR_UNSUPPORTED_OPERATION, StatusCode::NOT_ACCEPTABLE, message)
        }
        crate::table_catalog::TableCatalogStoreError::Internal(message) => {
            tracing::error!(error = %message, "table catalog store operation failed");
            iceberg_rest_error(ICEBERG_ERROR_REST, StatusCode::INTERNAL_SERVER_ERROR, "internal table catalog error")
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
        crate::table_catalog::TableCatalogStoreError::NotFound(message)
        | crate::table_catalog::TableCatalogStoreError::NamespaceNotFound(message) => {
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

async fn list_namespaces_response<S>(
    store: &S,
    bucket: &str,
    parent: Option<&crate::table_catalog::Namespace>,
    uri: &http::Uri,
) -> S3Result<RestListNamespacesResponse>
where
    S: crate::table_catalog::TableCatalogStore + ?Sized,
{
    let entries = match parent {
        Some(parent) => store
            .list_namespaces_under(bucket, &parent.public_name())
            .await
            .map_err(catalog_store_error)?,
        None => store.list_namespaces(bucket).await.map_err(catalog_store_error)?,
    };
    let parent_depth = parent.map_or(0, |parent| parent.segments().len());
    let mut parent_exists = parent.is_none();
    let mut direct_children = BTreeMap::new();
    for entry in entries {
        if entry.state != crate::table_catalog::TableCatalogEntryState::Active {
            continue;
        }
        let namespace = crate::table_catalog::Namespace::parse(&entry.namespace).map_err(|err| {
            iceberg_rest_error(
                ICEBERG_ERROR_REST,
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("catalog namespace is invalid: {err}"),
            )
        })?;
        if let Some(parent) = parent {
            if !namespace.segments().starts_with(parent.segments()) {
                continue;
            }
            parent_exists = true;
        }
        if namespace.segments().len() <= parent_depth {
            continue;
        }
        let child_segments = namespace.segments()[..=parent_depth]
            .iter()
            .map(|segment| segment.as_str().to_string())
            .collect::<Vec<_>>();
        direct_children.entry(child_segments.join(".")).or_insert(child_segments);
    }
    if !parent_exists {
        let parent_name = parent.map(crate::table_catalog::Namespace::public_name).unwrap_or_default();
        return Err(iceberg_rest_error(
            ICEBERG_ERROR_NO_SUCH_NAMESPACE,
            StatusCode::NOT_FOUND,
            format!("namespace not found: {bucket}/{parent_name}"),
        ));
    }

    let parent_name = parent.map(crate::table_catalog::Namespace::public_name);
    let context = RestPageContext {
        resource: TABLE_CATALOG_NAMESPACE_RESOURCE_ROOT,
        warehouse: bucket,
        namespace: parent_name.as_deref(),
    };
    let pagination = rest_pagination_from_query(uri, context)?;
    let page = match pagination.page_request() {
        Some((cursor, limit)) => crate::table_catalog::catalog_list_page_from_entries(
            direct_children.into_iter().collect(),
            cursor,
            limit,
            |entry: &(String, Vec<String>)| entry.0.as_str(),
        ),
        None => crate::table_catalog::TableCatalogListPage {
            entries: direct_children.into_iter().collect(),
            next_cursor: None,
        },
    };
    let next_page_token = pagination.next_page_token(page.next_cursor)?;
    Ok(RestListNamespacesResponse {
        namespaces: page.entries.into_iter().map(|(_, segments)| segments).collect(),
        next_page_token,
    })
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
        .filter(|entry| entry.state == crate::table_catalog::TableCatalogEntryState::Active)
    else {
        return Err(iceberg_rest_error(
            ICEBERG_ERROR_NO_SUCH_NAMESPACE,
            StatusCode::NOT_FOUND,
            "namespace not found",
        ));
    };
    namespace_response_from_entry(entry)
}

fn namespace_properties_update_from_request(
    request: UpdateNamespacePropertiesRequest,
) -> S3Result<crate::table_catalog::NamespacePropertiesUpdate> {
    crate::table_catalog::NamespacePropertiesUpdate::try_new(request.removals, request.updates).map_err(|err| match err {
        crate::table_catalog::NamespacePropertiesUpdateError::DuplicateRemoval(key) => iceberg_rest_error(
            ICEBERG_ERROR_BAD_REQUEST,
            StatusCode::BAD_REQUEST,
            format!("namespace property removal is repeated: {key}"),
        ),
        crate::table_catalog::NamespacePropertiesUpdateError::Overlap(key) => iceberg_rest_error(
            ICEBERG_ERROR_UNPROCESSABLE_ENTITY,
            StatusCode::UNPROCESSABLE_ENTITY,
            format!("namespace property cannot be removed and updated in the same request: {key}"),
        ),
    })
}

async fn update_namespace_properties_response<S>(
    store: &S,
    bucket: &str,
    namespace: &crate::table_catalog::Namespace,
    request: UpdateNamespacePropertiesRequest,
) -> S3Result<crate::table_catalog::NamespacePropertiesUpdateResult>
where
    S: crate::table_catalog::TableCatalogStore + ?Sized,
{
    let update = namespace_properties_update_from_request(request)?;
    store
        .update_namespace_properties(bucket, &namespace.public_name(), update)
        .await
        .map_err(catalog_store_error)
}

async fn namespace_exists_status<S>(store: &S, bucket: &str, namespace: &crate::table_catalog::Namespace) -> S3Result<StatusCode>
where
    S: crate::table_catalog::TableCatalogStore + ?Sized,
{
    let exists = store
        .get_namespace(bucket, &namespace.public_name())
        .await
        .map_err(catalog_store_error)?
        .is_some_and(|entry| entry.state == crate::table_catalog::TableCatalogEntryState::Active);
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

fn table_identifier_from_request(
    identifier: RestTableIdentifier,
) -> S3Result<(crate::table_catalog::Namespace, crate::table_catalog::IdentifierSegment)> {
    let namespace = namespace_from_segments(identifier.namespace)?;
    let table = crate::table_catalog::IdentifierSegment::parse(identifier.name)
        .map_err(|err| s3_error!(InvalidRequest, "invalid table name: {}", err))?;
    Ok((namespace, table))
}

async fn register_table_response<S, B>(
    store: &S,
    metadata_backend: &B,
    bucket: &str,
    namespace: &crate::table_catalog::Namespace,
    request: RegisterTableRequest,
    metadata: serde_json::Value,
    table_bucket_enabled: bool,
) -> S3Result<RestLoadTableResponse>
where
    S: crate::table_catalog::TableCatalogStore + ?Sized,
    B: crate::table_catalog::TableCatalogObjectBackend,
{
    let mut entry = table_entry_from_register_request(bucket, namespace, request)?;
    ensure_table_bucket_entry(store, bucket, table_bucket_enabled).await?;
    validate_metadata_table_location_in_bucket(bucket, &metadata)?;
    adopt_registered_metadata_identity(&mut entry, &metadata)?;
    validate_supported_table_metadata(&metadata)?;
    if !crate::table_catalog::is_valid_table_metadata_location_for_entry(&entry, &entry.metadata_location) {
        return Err(s3_error!(InvalidRequest, "metadata location must be inside the table metadata directory"));
    }
    let table = crate::table_catalog::IdentifierSegment::parse(&entry.table)
        .map_err(|err| s3_error!(InvalidRequest, "invalid table name: {}", err))?;
    let snapshot_context = SnapshotReadContext {
        metadata_backend,
        bucket,
        namespace,
        table: &table,
        entry: &entry,
    };
    validate_table_snapshot_graph(&snapshot_context, &metadata).await?;
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
    let metadata_guard = metadata_backend
        .acquire_write_lock(bucket, &entry.metadata_location)
        .await
        .map_err(catalog_store_error)?;
    metadata_backend
        .put_object_unlocked(
            bucket,
            &entry.metadata_location,
            metadata_data,
            crate::table_catalog::TableCatalogPutPrecondition::IfAbsent,
        )
        .await
        .map_err(catalog_store_already_exists_error)?;
    crate::table_catalog::ensure_table_catalog_lock_held(metadata_guard.as_ref()).map_err(catalog_store_error)?;
    if let Err(err) = store
        .create_table(entry.clone())
        .await
        .map_err(catalog_store_already_exists_error)
    {
        rollback_initial_metadata(metadata_backend, metadata_guard.as_ref(), bucket, &entry.metadata_location, "table").await;
        return Err(err);
    }
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
    let metadata_guard = metadata_backend
        .acquire_write_lock(bucket, &entry.metadata_location)
        .await
        .map_err(catalog_store_error)?;
    metadata_backend
        .put_object_unlocked(
            bucket,
            &entry.metadata_location,
            metadata_data,
            crate::table_catalog::TableCatalogPutPrecondition::IfAbsent,
        )
        .await
        .map_err(catalog_store_already_exists_error)?;
    crate::table_catalog::ensure_table_catalog_lock_held(metadata_guard.as_ref()).map_err(catalog_store_error)?;
    if let Err(err) = store
        .create_view(entry.clone())
        .await
        .map_err(catalog_store_already_exists_error)
    {
        rollback_initial_metadata(metadata_backend, metadata_guard.as_ref(), bucket, &entry.metadata_location, "view").await;
        return Err(err);
    }
    Ok(load_view_response_from_entry(entry, metadata))
}

async fn rollback_initial_metadata(
    metadata_backend: &impl crate::table_catalog::TableCatalogObjectBackend,
    metadata_guard: &dyn crate::table_catalog::TableCatalogObjectLockGuard,
    bucket: &str,
    metadata_location: &str,
    object_kind: &'static str,
) {
    if let Err(err) = crate::table_catalog::ensure_table_catalog_lock_held(metadata_guard) {
        tracing::warn!(
            bucket = %bucket,
            metadata_location = %metadata_location,
            object_kind,
            error = %err,
            "retaining initial catalog metadata after publication failure because its lock was lost"
        );
        return;
    }
    if let Err(err) = metadata_backend.delete_object_unlocked(bucket, metadata_location).await {
        tracing::warn!(
            bucket = %bucket,
            metadata_location = %metadata_location,
            object_kind,
            error = %err,
            "failed to roll back initial catalog metadata"
        );
    }
}

async fn read_table_metadata_json(
    metadata_backend: &impl crate::table_catalog::TableCatalogObjectBackend,
    bucket: &str,
    metadata_location: &str,
) -> S3Result<serde_json::Value> {
    let Some(object) = metadata_backend
        .read_object_limited(bucket, metadata_location, crate::table_catalog::TABLE_METADATA_JSON_MAX_SIZE)
        .await
        .map_err(catalog_store_error)?
    else {
        return Err(s3_error!(InvalidRequest, "table metadata object not found: {metadata_location}"));
    };
    parse_table_metadata_json(&object.data)
}

fn parse_table_metadata_json(data: &[u8]) -> S3Result<serde_json::Value> {
    let metadata = serde_json::from_slice::<serde_json::Value>(data)
        .map_err(|err| s3_error!(InvalidRequest, "failed to parse table metadata JSON: {}", err))?;
    if !metadata.is_object() {
        return Err(s3_error!(InvalidRequest, "table metadata JSON must be an object"));
    }
    Ok(metadata)
}

async fn read_existing_table_metadata_target<S>(
    store: &S,
    metadata_backend: &impl crate::table_catalog::TableCatalogObjectBackend,
    bucket: &str,
    namespace: &crate::table_catalog::Namespace,
    table: &str,
    metadata_location: &str,
) -> S3Result<serde_json::Value>
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
    if !crate::table_catalog::is_valid_table_metadata_location_for_entry(&current, metadata_location) {
        return Err(s3_error!(InvalidRequest, "metadata location must be inside the table metadata directory"));
    }
    read_table_metadata_json(metadata_backend, bucket, metadata_location).await
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
    let namespace_is_active = store
        .get_namespace(bucket, &namespace)
        .await
        .map_err(catalog_store_error)?
        .is_some_and(|entry| entry.state == crate::table_catalog::TableCatalogEntryState::Active);
    if !namespace_is_active {
        return Err(iceberg_rest_error(
            ICEBERG_ERROR_NO_SUCH_NAMESPACE,
            StatusCode::NOT_FOUND,
            format!("namespace not found: {bucket}/{namespace}"),
        ));
    }
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
        None => {
            let mut entries = store.list_tables(bucket, &namespace).await.map_err(catalog_store_error)?;
            entries.retain(|entry| entry.state == crate::table_catalog::TableCatalogEntryState::Active);
            crate::table_catalog::TableCatalogListPage {
                entries,
                next_cursor: None,
            }
        }
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
    if !crate::table_catalog::is_valid_table_metadata_location_for_entry(&entry, &entry.metadata_location) {
        return Err(iceberg_rest_error(
            ICEBERG_ERROR_REST,
            StatusCode::INTERNAL_SERVER_ERROR,
            "persisted table metadata location is outside the protected table metadata directory",
        ));
    }
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
    let namespace_is_active = store
        .get_namespace(bucket, &namespace)
        .await
        .map_err(catalog_store_error)?
        .is_some_and(|entry| entry.state == crate::table_catalog::TableCatalogEntryState::Active);
    if !namespace_is_active {
        return Err(iceberg_rest_error(
            ICEBERG_ERROR_NO_SUCH_NAMESPACE,
            StatusCode::NOT_FOUND,
            format!("namespace not found: {bucket}/{namespace}"),
        ));
    }
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
        None => {
            let mut entries = store.list_views(bucket, &namespace).await.map_err(catalog_store_error)?;
            entries.retain(|entry| entry.state == crate::table_catalog::TableCatalogEntryState::Active);
            crate::table_catalog::TableCatalogListPage {
                entries,
                next_cursor: None,
            }
        }
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
    let view = crate::table_catalog::IdentifierSegment::parse(view)
        .map_err(|err| s3_error!(InvalidRequest, "invalid view name: {}", err))?;
    if !crate::table_catalog::is_valid_view_metadata_location(namespace, &view, &entry.metadata_location) {
        return Err(iceberg_rest_error(
            ICEBERG_ERROR_REST,
            StatusCode::INTERNAL_SERVER_ERROR,
            "persisted view metadata location is outside the protected view metadata directory",
        ));
    }
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
    validate_rest_commit_identifier(request.identifier.as_ref(), namespace, view)?;
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
    let requested_new_metadata_location = request
        .new_metadata_location
        .as_deref()
        .map(|location| table_metadata_location_for_catalog(bucket, location))
        .transpose()?;
    let (next_metadata_location, next_metadata) = if let Some(new_metadata_location) = requested_new_metadata_location {
        if !crate::table_catalog::is_valid_view_metadata_location(namespace, &view_name, &new_metadata_location) {
            return Err(s3_error!(InvalidRequest, "metadata location must be inside the view metadata directory"));
        }
        let target_metadata = read_table_metadata_json(metadata_backend, bucket, &new_metadata_location).await?;
        validate_metadata_view_location_in_bucket(bucket, &target_metadata)?;
        validate_metadata_matches_current_view_metadata(&current_metadata, &target_metadata)?;
        (new_metadata_location, target_metadata)
    } else {
        let previous_metadata_location = table_metadata_location_for_client(bucket, &current.metadata_location);
        let commit_timestamp_ms = current_time_millis();
        let mut next_metadata = apply_view_commit_updates_at(
            current_metadata.clone(),
            &request.updates,
            &previous_metadata_location,
            commit_timestamp_ms,
        )?;
        validate_metadata_view_location_in_bucket(bucket, &next_metadata)?;
        validate_metadata_matches_current_view_metadata(&current_metadata, &next_metadata)?;
        let (_, metadata_file_token) = standard_commit_ids(None, None);
        let next_generation =
            crate::table_catalog::next_table_catalog_generation(current.generation).map_err(catalog_store_error)?;
        let next_metadata_location = crate::table_catalog::default_view_metadata_file_path(
            namespace,
            &view_name,
            &next_metadata_file_name(next_generation, &metadata_file_token),
        );
        let next_metadata_data = serde_json::to_vec(&next_metadata)
            .map_err(|err| s3_error!(InternalError, "failed to serialize view metadata update: {}", err))?;
        let put_result = metadata_backend
            .put_object(
                bucket,
                &next_metadata_location,
                next_metadata_data,
                crate::table_catalog::TableCatalogPutPrecondition::IfAbsent,
            )
            .await;
        match put_result {
            Ok(()) => {}
            Err(crate::table_catalog::TableCatalogStoreError::Conflict(_)) => {
                let existing_metadata = read_table_metadata_json(metadata_backend, bucket, &next_metadata_location).await?;
                let persisted_timestamp = existing_metadata
                    .get("last-updated-ms")
                    .and_then(serde_json::Value::as_i64)
                    .ok_or_else(|| s3_error!(InvalidRequest, "existing generated view metadata is missing last-updated-ms"))?;
                let rebuilt_metadata = apply_view_commit_updates_at(
                    current_metadata,
                    &request.updates,
                    &previous_metadata_location,
                    persisted_timestamp,
                )?;
                if existing_metadata != rebuilt_metadata {
                    return Err(iceberg_rest_error(
                        ICEBERG_ERROR_COMMIT_FAILED,
                        StatusCode::CONFLICT,
                        "generated view metadata location already contains a different commit",
                    ));
                }
                next_metadata = existing_metadata;
            }
            Err(err) => return Err(catalog_store_error(err)),
        }
        (next_metadata_location, next_metadata)
    };

    let new_metadata_sha256 = metadata_sha256(&next_metadata)?;
    let expected_metadata_location = request
        .expected_metadata_location
        .as_deref()
        .map(|location| table_metadata_location_for_catalog(bucket, location))
        .transpose()?
        .unwrap_or_else(|| current.metadata_location.clone());
    let result = store
        .replace_view(crate::table_catalog::ViewCommitRequest {
            table_bucket: bucket.to_string(),
            namespace: namespace.public_name(),
            view: view.to_string(),
            expected_version_token: request
                .expected_version_token
                .unwrap_or_else(|| current.version_token.clone()),
            expected_metadata_location,
            new_metadata_location: next_metadata_location,
            new_metadata_sha256: Some(new_metadata_sha256),
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
    target_metadata: serde_json::Value,
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
    let metadata_location = table_metadata_location_for_catalog(bucket, &request.metadata_location)?;
    if !crate::table_catalog::is_valid_table_metadata_location_for_entry(&current, &metadata_location) {
        return Err(s3_error!(InvalidRequest, "metadata location must be inside the table metadata directory"));
    }
    validate_metadata_table_location_in_bucket(bucket, &target_metadata)?;
    if metadata_table_uuid(&target_metadata)? != current.table_uuid {
        return Err(s3_error!(
            InvalidRequest,
            "table metadata table-uuid does not match catalog table identity"
        ));
    }
    crate::table_catalog::validate_table_warehouse_relocation(&current, metadata_table_location(&target_metadata)?)
        .map_err(catalog_store_error)?;
    let table_name = crate::table_catalog::IdentifierSegment::parse(table.to_string())
        .map_err(|err| s3_error!(InvalidRequest, "invalid table name: {}", err))?;
    let snapshot_context = SnapshotReadContext {
        metadata_backend,
        bucket,
        namespace,
        table: &table_name,
        entry: &current,
    };
    validate_table_snapshot_graph(&snapshot_context, &target_metadata).await?;
    let (commit_id, _) = standard_commit_ids(request.commit_id, request.idempotency_key.as_deref());
    if let Some(replay) = published_api_commit_replay(
        store,
        PublishedApiCommitReplay {
            route: RestTableRoute {
                bucket,
                namespace,
                table,
            },
            current: &current,
            commit_id: &commit_id,
            idempotency_key: request.idempotency_key.as_deref(),
            operation: "update-metadata-location",
            expected_version_token: &request.version_token,
            new_metadata_location: &metadata_location,
            expected_metadata_location: None,
        },
    )
    .await?
    {
        let result = store.commit_table(replay).await.map_err(catalog_store_error)?;
        return Ok(table_metadata_location_response_from_entry(result.table));
    }
    let current_metadata = read_table_metadata_json(metadata_backend, bucket, &current.metadata_location).await?;
    validate_metadata_table_location_in_bucket(bucket, &current_metadata)?;
    validate_metadata_matches_current_metadata(&current_metadata, &target_metadata)?;
    let commit_request = crate::table_catalog::TableCommitRequest {
        table_bucket: bucket.to_string(),
        namespace: namespace.public_name(),
        table: table.to_string(),
        commit_id,
        idempotency_key: request.idempotency_key,
        operation: "update-metadata-location".to_string(),
        expected_version_token: request.version_token,
        expected_metadata_location: current.metadata_location,
        new_metadata_location: metadata_location,
        requirements: vec![
            base_metadata_digest_requirement(&current_metadata)?,
            metadata_digest_requirement(&target_metadata)?,
        ],
        writer: Some("rustfs-metadata-location-api".to_string()),
    };
    let result = store.commit_table(commit_request).await.map_err(catalog_store_error)?;
    Ok(table_metadata_location_response_from_entry(result.table))
}

#[derive(Clone, Copy)]
struct RestTableRoute<'a> {
    bucket: &'a str,
    namespace: &'a crate::table_catalog::Namespace,
    table: &'a str,
}

#[cfg(test)]
async fn commit_table_response<S>(
    store: &S,
    metadata_backend: &impl crate::table_catalog::TableCatalogObjectBackend,
    bucket: &str,
    namespace: &crate::table_catalog::Namespace,
    table: &str,
    mut request: RestCommitTableRequest,
) -> S3Result<RestCommitTableResponse>
where
    S: crate::table_catalog::TableCatalogStore + ?Sized,
{
    let target_metadata = if let Some(metadata_location) = request.new_metadata_location.as_deref() {
        let metadata_location = table_metadata_location_for_catalog(bucket, metadata_location)?;
        let metadata =
            read_existing_table_metadata_target(store, metadata_backend, bucket, namespace, table, &metadata_location).await?;
        request.new_metadata_location = Some(metadata_location);
        Some(metadata)
    } else {
        None
    };
    commit_table_response_with_target_metadata(
        None,
        store,
        metadata_backend,
        RestTableRoute {
            bucket,
            namespace,
            table,
        },
        request,
        target_metadata,
    )
    .await
}

async fn table_commit_for_retry<S>(
    store: &S,
    bucket: &str,
    table_id: &str,
    request: &RestCommitTableRequest,
) -> S3Result<Option<crate::table_catalog::CommitLogEntry>>
where
    S: crate::table_catalog::TableCatalogStore + ?Sized,
{
    let derived_commit_id = request.commit_id.clone().or_else(|| {
        request
            .idempotency_key
            .as_deref()
            .map(|idempotency_key| format!("idempotency-{}", table_catalog_path_hash(idempotency_key)))
    });
    let by_commit_id = match derived_commit_id.as_deref() {
        Some(commit_id) => store
            .get_commit_by_id(bucket, table_id, commit_id)
            .await
            .map_err(catalog_store_error)?,
        None => None,
    };
    let by_idempotency_key = match request.idempotency_key.as_deref() {
        Some(idempotency_key) => store
            .get_commit_by_idempotency_key(bucket, table_id, idempotency_key)
            .await
            .map_err(catalog_store_error)?,
        None => None,
    };
    if let (Some(by_commit_id), Some(by_idempotency_key)) = (&by_commit_id, &by_idempotency_key)
        && by_commit_id.commit_id != by_idempotency_key.commit_id
    {
        return Err(iceberg_rest_error(
            ICEBERG_ERROR_COMMIT_FAILED,
            StatusCode::CONFLICT,
            "commit id and idempotency key identify different commits",
        ));
    }
    Ok(by_commit_id.or(by_idempotency_key))
}

async fn table_commit_warehouse_read_location<S>(
    req: Option<&mut S3Request<Body>>,
    store: &S,
    metadata_backend: &impl crate::table_catalog::TableCatalogObjectBackend,
    bucket: &str,
    current: &crate::table_catalog::TableEntry,
    request: &RestCommitTableRequest,
) -> S3Result<String>
where
    S: crate::table_catalog::TableCatalogStore + ?Sized,
{
    let Some(commit) = table_commit_for_retry(store, bucket, &current.table_id, request).await? else {
        return Ok(current.warehouse_location.clone());
    };
    authorize_optional_table_catalog_object_read(req, bucket, &commit.previous_metadata_location).await?;
    let previous_metadata = read_table_metadata_json(metadata_backend, bucket, &commit.previous_metadata_location).await?;
    validate_metadata_table_location_in_bucket(bucket, &previous_metadata)?;
    Ok(metadata_table_location(&previous_metadata)?.to_string())
}

async fn commit_table_replay_response(
    req: Option<&mut S3Request<Body>>,
    metadata_backend: &impl crate::table_catalog::TableCatalogObjectBackend,
    bucket: &str,
    result: crate::table_catalog::TableCommitResult,
    committed_metadata_location: &str,
    committed_metadata: serde_json::Value,
) -> S3Result<RestCommitTableResponse> {
    let metadata = if result.table.metadata_location == committed_metadata_location {
        committed_metadata
    } else if !crate::table_catalog::is_valid_table_metadata_location_for_entry(&result.table, &result.table.metadata_location) {
        return Err(iceberg_rest_error(
            ICEBERG_ERROR_REST,
            StatusCode::INTERNAL_SERVER_ERROR,
            "persisted table metadata location is outside the protected table metadata directory",
        ));
    } else {
        authorize_optional_table_catalog_object_read(req, bucket, &result.table.metadata_location).await?;
        read_table_metadata_json(metadata_backend, bucket, &result.table.metadata_location).await?
    };
    Ok(commit_table_response_from_result(result, metadata))
}

async fn commit_table_response_with_target_metadata<S>(
    mut req: Option<&mut S3Request<Body>>,
    store: &S,
    metadata_backend: &impl crate::table_catalog::TableCatalogObjectBackend,
    route: RestTableRoute<'_>,
    request: RestCommitTableRequest,
    target_metadata: Option<serde_json::Value>,
) -> S3Result<RestCommitTableResponse>
where
    S: crate::table_catalog::TableCatalogStore + ?Sized,
{
    let RestTableRoute {
        bucket,
        namespace,
        table,
    } = route;
    validate_rest_commit_identifier(request.identifier.as_ref(), namespace, table)?;
    if request.new_metadata_location.is_none() {
        return standard_commit_table_response_inner(req, store, metadata_backend, route, request).await;
    }

    let Some(current) = store
        .load_table(bucket, &namespace.public_name(), table)
        .await
        .map_err(catalog_store_error)?
    else {
        return Err(iceberg_rest_error(ICEBERG_ERROR_NO_SUCH_TABLE, StatusCode::NOT_FOUND, "table not found"));
    };
    let mut request = request;
    let existing_commit = table_commit_for_retry(store, bucket, &current.table_id, &request).await?;
    if request.commit_id.is_none()
        && let Some(existing_commit) = existing_commit.as_ref()
    {
        request.commit_id = Some(existing_commit.commit_id.clone());
    }
    let client_requirements = request.requirements.clone();
    let mut request = table_commit_request_from_rest_request(bucket, namespace, table, request)?;
    if !crate::table_catalog::is_valid_table_metadata_location_for_entry(&current, &request.new_metadata_location) {
        return Err(s3_error!(InvalidRequest, "metadata location must be inside the table metadata directory"));
    }
    let target_metadata =
        target_metadata.ok_or_else(|| s3_error!(InternalError, "authorized target metadata snapshot is required"))?;
    validate_metadata_table_location_in_bucket(bucket, &target_metadata)?;
    let table_name = crate::table_catalog::IdentifierSegment::parse(table.to_string())
        .map_err(|err| s3_error!(InvalidRequest, "invalid table name: {}", err))?;
    let snapshot_context = SnapshotReadContext {
        metadata_backend,
        bucket,
        namespace,
        table: &table_name,
        entry: &current,
    };
    validate_table_snapshot_graph(&snapshot_context, &target_metadata).await?;
    request.requirements.push(metadata_digest_requirement(&target_metadata)?);
    if let Some(existing_commit) = existing_commit {
        if !crate::table_catalog::commit_log_matches_request(&existing_commit, &request, &current.table_id) {
            return Err(iceberg_rest_error(
                ICEBERG_ERROR_COMMIT_FAILED,
                StatusCode::CONFLICT,
                "commit retry does not match the original request",
            ));
        }
        authorize_optional_table_catalog_object_read(req.as_deref_mut(), bucket, &existing_commit.previous_metadata_location)
            .await?;
        let previous_metadata =
            read_table_metadata_json(metadata_backend, bucket, &existing_commit.previous_metadata_location).await?;
        validate_metadata_table_location_in_bucket(bucket, &previous_metadata)?;
        validate_table_commit_requirements(&previous_metadata, &client_requirements)?;
        validate_metadata_matches_current_metadata(&previous_metadata, &target_metadata)?;
        request
            .requirements
            .push(base_metadata_digest_requirement(&previous_metadata)?);
        let committed_metadata_location = request.new_metadata_location.clone();
        let result = store.commit_table(request).await.map_err(catalog_store_error)?;
        return commit_table_replay_response(
            req,
            metadata_backend,
            bucket,
            result,
            &committed_metadata_location,
            target_metadata,
        )
        .await;
    }

    authorize_optional_table_catalog_object_read(req, bucket, &current.metadata_location).await?;
    let current_metadata = read_table_metadata_json(metadata_backend, bucket, &current.metadata_location).await?;
    validate_metadata_table_location_in_bucket(bucket, &current_metadata)?;
    validate_table_commit_requirements(&current_metadata, &client_requirements)?;
    validate_metadata_matches_current_metadata(&current_metadata, &target_metadata)?;
    request
        .requirements
        .push(base_metadata_digest_requirement(&current_metadata)?);
    let result = store.commit_table(request).await.map_err(catalog_store_error)?;
    Ok(commit_table_response_from_result(result, target_metadata))
}

#[cfg(test)]
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
    standard_commit_table_response_inner(
        None,
        store,
        metadata_backend,
        RestTableRoute {
            bucket,
            namespace,
            table,
        },
        request,
    )
    .await
}

async fn standard_commit_table_response_inner<S>(
    mut req: Option<&mut S3Request<Body>>,
    store: &S,
    metadata_backend: &impl crate::table_catalog::TableCatalogObjectBackend,
    route: RestTableRoute<'_>,
    request: RestCommitTableRequest,
) -> S3Result<RestCommitTableResponse>
where
    S: crate::table_catalog::TableCatalogStore + ?Sized,
{
    let RestTableRoute {
        bucket,
        namespace,
        table,
    } = route;
    let Some(current) = store
        .load_table(bucket, &namespace.public_name(), table)
        .await
        .map_err(catalog_store_error)?
    else {
        return Err(iceberg_rest_error(ICEBERG_ERROR_NO_SUCH_TABLE, StatusCode::NOT_FOUND, "table not found"));
    };
    if let Some(response) =
        replay_standard_table_commit(req.as_deref_mut(), store, metadata_backend, route, &current, &request).await?
    {
        return Ok(response);
    }
    let table_name = crate::table_catalog::IdentifierSegment::parse(table.to_string())
        .map_err(|err| s3_error!(InvalidRequest, "invalid table name: {}", err))?;
    authorize_optional_table_catalog_object_read(req.as_deref_mut(), bucket, &current.metadata_location).await?;
    let current_metadata = read_table_metadata_json(metadata_backend, bucket, &current.metadata_location).await?;
    validate_table_commit_requirements(&current_metadata, &request.requirements)?;
    let expected_metadata = current_metadata.clone();
    let previous_metadata_location = table_metadata_location_for_client(bucket, &current.metadata_location);
    let commit_timestamp_ms = current_time_millis();
    let mut next_metadata =
        apply_table_commit_updates_at(current_metadata, &request.updates, &previous_metadata_location, commit_timestamp_ms)?;
    validate_metadata_table_location_in_bucket(bucket, &next_metadata)?;
    validate_metadata_matches_current_metadata(&expected_metadata, &next_metadata)?;
    crate::table_catalog::validate_table_warehouse_relocation(&current, metadata_table_location(&next_metadata)?)
        .map_err(catalog_store_error)?;
    let snapshot_context = SnapshotReadContext {
        metadata_backend,
        bucket,
        namespace,
        table: &table_name,
        entry: &current,
    };
    validate_table_snapshot_commit_conflicts(&snapshot_context, &expected_metadata, &next_metadata, &request.updates).await?;
    let (commit_id, metadata_file_token) = standard_commit_ids(request.commit_id, request.idempotency_key.as_deref());
    let next_generation = crate::table_catalog::next_table_catalog_generation(current.generation).map_err(catalog_store_error)?;
    let next_metadata_location = crate::table_catalog::table_metadata_file_path_for_entry(
        &current,
        &next_metadata_file_name(next_generation, &metadata_file_token),
    )
    .map_err(catalog_store_error)?;
    let next_metadata_data = serde_json::to_vec(&next_metadata)
        .map_err(|err| s3_error!(InternalError, "failed to serialize table metadata update: {}", err))?;
    let put_result = metadata_backend
        .put_object(
            bucket,
            &next_metadata_location,
            next_metadata_data,
            crate::table_catalog::TableCatalogPutPrecondition::IfAbsent,
        )
        .await;
    match put_result {
        Ok(()) => {}
        Err(crate::table_catalog::TableCatalogStoreError::Conflict(_)) => {
            authorize_optional_table_catalog_object_read(req, bucket, &next_metadata_location).await?;
            let existing_metadata = read_table_metadata_json(metadata_backend, bucket, &next_metadata_location).await?;
            let persisted_timestamp = existing_metadata
                .get("last-updated-ms")
                .and_then(serde_json::Value::as_i64)
                .ok_or_else(|| s3_error!(InvalidRequest, "existing generated metadata is missing last-updated-ms"))?;
            let rebuilt_metadata = apply_table_commit_updates_at(
                expected_metadata.clone(),
                &request.updates,
                &previous_metadata_location,
                persisted_timestamp,
            )?;
            if existing_metadata != rebuilt_metadata {
                return Err(iceberg_rest_error(
                    ICEBERG_ERROR_COMMIT_FAILED,
                    StatusCode::CONFLICT,
                    "generated metadata location already contains a different commit",
                ));
            }
            next_metadata = existing_metadata;
        }
        Err(err) => return Err(catalog_store_error(err)),
    }

    let mut requirements = request.requirements;
    requirements.push(base_metadata_digest_requirement(&expected_metadata)?);
    requirements.push(metadata_digest_requirement(&next_metadata)?);
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
        requirements,
        writer: request.writer,
    };
    let result = store.commit_table(commit_request).await.map_err(catalog_store_error)?;
    Ok(commit_table_response_from_result(result, next_metadata))
}

async fn replay_standard_table_commit<S>(
    mut req: Option<&mut S3Request<Body>>,
    store: &S,
    metadata_backend: &impl crate::table_catalog::TableCatalogObjectBackend,
    route: RestTableRoute<'_>,
    current: &crate::table_catalog::TableEntry,
    request: &RestCommitTableRequest,
) -> S3Result<Option<RestCommitTableResponse>>
where
    S: crate::table_catalog::TableCatalogStore + ?Sized,
{
    let RestTableRoute {
        bucket,
        namespace,
        table,
    } = route;
    let Some(commit) = table_commit_for_retry(store, bucket, &current.table_id, request).await? else {
        return Ok(None);
    };
    if request
        .commit_id
        .as_deref()
        .is_some_and(|commit_id| commit_id != commit.commit_id)
        || request.idempotency_key != commit.idempotency_key
        || request.writer != commit.writer
    {
        return Err(iceberg_rest_error(
            ICEBERG_ERROR_COMMIT_FAILED,
            StatusCode::CONFLICT,
            "commit retry does not match the original request",
        ));
    }
    authorize_optional_table_catalog_object_read(req.as_deref_mut(), bucket, &commit.previous_metadata_location).await?;
    let previous_metadata = read_table_metadata_json(metadata_backend, bucket, &commit.previous_metadata_location).await?;
    validate_table_commit_requirements(&previous_metadata, &request.requirements)?;
    authorize_optional_table_catalog_object_read(req.as_deref_mut(), bucket, &commit.new_metadata_location).await?;
    let target_metadata = read_table_metadata_json(metadata_backend, bucket, &commit.new_metadata_location).await?;
    let commit_timestamp_ms = target_metadata
        .get("last-updated-ms")
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| s3_error!(InvalidRequest, "committed metadata is missing last-updated-ms"))?;
    let previous_metadata_location = table_metadata_location_for_client(bucket, &commit.previous_metadata_location);
    let rebuilt_metadata = apply_table_commit_updates_at(
        previous_metadata.clone(),
        &request.updates,
        &previous_metadata_location,
        commit_timestamp_ms,
    )?;
    if rebuilt_metadata != target_metadata {
        return Err(iceberg_rest_error(
            ICEBERG_ERROR_COMMIT_FAILED,
            StatusCode::CONFLICT,
            "commit retry updates do not match the original commit",
        ));
    }
    let table_name = crate::table_catalog::IdentifierSegment::parse(table.to_string())
        .map_err(|err| s3_error!(InvalidRequest, "invalid table name: {}", err))?;
    let snapshot_context = SnapshotReadContext {
        metadata_backend,
        bucket,
        namespace,
        table: &table_name,
        entry: current,
    };
    validate_table_snapshot_commit_conflicts(&snapshot_context, &previous_metadata, &target_metadata, &request.updates).await?;
    validate_table_snapshot_graph(&snapshot_context, &target_metadata).await?;
    let operation = request
        .operation
        .clone()
        .unwrap_or_else(|| table_commit_operation(&target_metadata));
    if operation != commit.operation {
        return Err(iceberg_rest_error(
            ICEBERG_ERROR_COMMIT_FAILED,
            StatusCode::CONFLICT,
            "commit retry operation does not match the original commit",
        ));
    }
    let mut requirements = request.requirements.clone();
    requirements.push(base_metadata_digest_requirement(&previous_metadata)?);
    requirements.push(metadata_digest_requirement(&target_metadata)?);
    let committed_metadata_location = commit.new_metadata_location.clone();
    let result = store
        .commit_table(crate::table_catalog::TableCommitRequest {
            table_bucket: bucket.to_string(),
            namespace: namespace.public_name(),
            table: table.to_string(),
            commit_id: commit.commit_id,
            idempotency_key: request.idempotency_key.clone(),
            operation,
            expected_version_token: commit.expected_version_token,
            expected_metadata_location: commit.previous_metadata_location,
            new_metadata_location: commit.new_metadata_location,
            requirements,
            writer: request.writer.clone(),
        })
        .await
        .map_err(catalog_store_error)?;
    Ok(Some(
        commit_table_replay_response(req, metadata_backend, bucket, result, &committed_metadata_location, target_metadata)
            .await?,
    ))
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
    let (commit_id, metadata_file_token) = standard_commit_ids(None, None);
    let next_generation = crate::table_catalog::next_table_catalog_generation(current.generation).map_err(catalog_store_error)?;
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
        requirements: vec![
            base_metadata_digest_requirement(&current_metadata)?,
            metadata_digest_requirement(&next_metadata)?,
        ],
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
    req: Option<&mut S3Request<Body>>,
    store: &S,
    metadata_backend: &impl crate::table_catalog::TableCatalogObjectBackend,
    route: RestTableRoute<'_>,
    ref_name: &str,
    request: PutTableRefRequest,
) -> S3Result<RestCommitTableResponse>
where
    S: crate::table_catalog::TableCatalogStore + ?Sized,
{
    let RestTableRoute {
        bucket,
        namespace,
        table,
    } = route;
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
    standard_commit_table_response_inner(
        req,
        store,
        metadata_backend,
        RestTableRoute {
            bucket,
            namespace,
            table,
        },
        RestCommitTableRequest {
            identifier: None,
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
    mut req: Option<&mut S3Request<Body>>,
    store: &S,
    metadata_backend: &impl crate::table_catalog::TableCatalogObjectBackend,
    route: RestTableRoute<'_>,
    ref_name: &str,
    request: DeleteTableRefRequest,
) -> S3Result<RestCommitTableResponse>
where
    S: crate::table_catalog::TableCatalogStore + ?Sized,
{
    let RestTableRoute {
        bucket,
        namespace,
        table,
    } = route;
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
    authorize_optional_table_catalog_object_read(req.as_deref_mut(), bucket, &entry.metadata_location).await?;
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
    standard_commit_table_response_inner(
        req,
        store,
        metadata_backend,
        RestTableRoute {
            bucket,
            namespace,
            table,
        },
        RestCommitTableRequest {
            identifier: None,
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
        table_id: String::new(),
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
        table_id: String::new(),
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

#[cfg(test)]
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
    let current = store
        .load_table(bucket, &namespace.public_name(), table)
        .await
        .map_err(catalog_store_error)?;
    let target_metadata = read_table_metadata_json(metadata_backend, bucket, &request.metadata_location).await?;
    sync_external_catalog_bridge_response_with_snapshot(
        store,
        metadata_backend,
        bucket,
        namespace,
        table,
        ExternalCatalogBridgeSyncSnapshot {
            request,
            target_metadata,
            current,
        },
    )
    .await
}

struct ExternalCatalogBridgeSyncSnapshot {
    request: ExternalCatalogBridgeSyncRequest,
    target_metadata: serde_json::Value,
    current: Option<crate::table_catalog::TableEntry>,
}

async fn sync_external_catalog_bridge_response_with_snapshot<B>(
    store: &crate::table_catalog::ObjectTableCatalogStore<B>,
    metadata_backend: &B,
    bucket: &str,
    namespace: &crate::table_catalog::Namespace,
    table: &str,
    snapshot: ExternalCatalogBridgeSyncSnapshot,
) -> S3Result<ExternalCatalogBridgeSyncResponse>
where
    B: crate::table_catalog::TableCatalogObjectBackend,
{
    let ExternalCatalogBridgeSyncSnapshot {
        request,
        target_metadata,
        current,
    } = snapshot;
    validate_external_catalog_metadata_location(namespace, table, &request.metadata_location)?;
    validate_metadata_table_location_in_bucket(bucket, &target_metadata)?;
    validate_supported_table_metadata(&target_metadata)?;
    let external_table_uuid = validate_external_catalog_metadata_uuid(request.external_table_uuid.as_deref(), &target_metadata)?;

    let registration_entry = if current.is_none() {
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
        Some(entry)
    } else {
        None
    };
    let graph_entry = current
        .as_ref()
        .or(registration_entry.as_ref())
        .ok_or_else(|| s3_error!(InternalError, "external catalog sync table snapshot is missing"))?;
    let table_name = crate::table_catalog::IdentifierSegment::parse(table.to_string())
        .map_err(|err| s3_error!(InvalidRequest, "invalid table name: {}", err))?;
    let snapshot_context = SnapshotReadContext {
        metadata_backend,
        bucket,
        namespace,
        table: &table_name,
        entry: graph_entry,
    };
    validate_table_snapshot_graph(&snapshot_context, &target_metadata).await?;

    let (action, table_response, table_id, publish_bridge) = if let Some(current) = current {
        if request.expected_version_token.is_none() && request.expected_metadata_location.is_none() {
            if current.metadata_location != request.metadata_location
                || current.table_uuid != metadata_table_uuid(&target_metadata)?
            {
                return Err(iceberg_rest_error(
                    ICEBERG_ERROR_COMMIT_FAILED,
                    StatusCode::CONFLICT,
                    "external catalog sync target already exists with a different table state",
                ));
            }
            let table_id = current.table_id.clone();
            (
                EXTERNAL_CATALOG_ACTION_REGISTERED.to_string(),
                load_table_response_from_entry(current, target_metadata),
                table_id,
                true,
            )
        } else {
            let expected_version_token = request
                .expected_version_token
                .clone()
                .ok_or_else(|| s3_error!(InvalidRequest, "external catalog sync requires expected-version-token"))?;
            let expected_metadata_location = request
                .expected_metadata_location
                .clone()
                .ok_or_else(|| s3_error!(InvalidRequest, "external catalog sync requires expected-metadata-location"))?;
            if current.metadata_location != request.metadata_location
                && request.commit_id.is_none()
                && request.idempotency_key.is_none()
            {
                return Err(s3_error!(
                    InvalidRequest,
                    "external catalog pointer sync requires commit-id or idempotency-key"
                ));
            }
            let (commit_id, _) = standard_commit_ids(request.commit_id.clone(), request.idempotency_key.as_deref());
            let result = if let Some(replay) = published_api_commit_replay(
                store,
                PublishedApiCommitReplay {
                    route: RestTableRoute {
                        bucket,
                        namespace,
                        table,
                    },
                    current: &current,
                    commit_id: &commit_id,
                    idempotency_key: request.idempotency_key.as_deref(),
                    operation: EXTERNAL_CATALOG_SYNC_OPERATION,
                    expected_version_token: &expected_version_token,
                    new_metadata_location: &request.metadata_location,
                    expected_metadata_location: Some(&expected_metadata_location),
                },
            )
            .await?
            {
                store.commit_table(replay).await.map_err(catalog_store_error)?
            } else {
                let current_metadata = read_table_metadata_json(metadata_backend, bucket, &current.metadata_location).await?;
                validate_metadata_table_location_in_bucket(bucket, &current_metadata)?;
                validate_metadata_matches_current_metadata(&current_metadata, &target_metadata)?;
                store
                    .commit_table(crate::table_catalog::TableCommitRequest {
                        table_bucket: bucket.to_string(),
                        namespace: namespace.public_name(),
                        table: table.to_string(),
                        commit_id,
                        idempotency_key: request.idempotency_key.clone(),
                        operation: EXTERNAL_CATALOG_SYNC_OPERATION.to_string(),
                        expected_version_token,
                        expected_metadata_location,
                        new_metadata_location: request.metadata_location.clone(),
                        requirements: vec![
                            base_metadata_digest_requirement(&current_metadata)?,
                            metadata_digest_requirement(&target_metadata)?,
                        ],
                        writer: Some(EXTERNAL_CATALOG_SYNC_WRITER.to_string()),
                    })
                    .await
                    .map_err(catalog_store_error)?
            };
            let publish_bridge = result.table.metadata_location == request.metadata_location;
            let response_metadata = if publish_bridge {
                target_metadata
            } else if !crate::table_catalog::is_valid_table_metadata_location_for_entry(
                &result.table,
                &result.table.metadata_location,
            ) {
                return Err(iceberg_rest_error(
                    ICEBERG_ERROR_REST,
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "persisted table metadata location is outside the protected table metadata directory",
                ));
            } else {
                read_table_metadata_json(metadata_backend, bucket, &result.table.metadata_location).await?
            };
            let table_id = result.table.table_id.clone();
            (
                EXTERNAL_CATALOG_ACTION_COMMITTED.to_string(),
                load_table_response_from_entry(result.table, response_metadata),
                table_id,
                publish_bridge,
            )
        }
    } else {
        if request.expected_version_token.is_some() || request.expected_metadata_location.is_some() {
            return Err(s3_error!(
                InvalidRequest,
                "external catalog sync cannot use expected table state when registering a missing table"
            ));
        }
        let entry = registration_entry
            .ok_or_else(|| s3_error!(InternalError, "external catalog sync registration snapshot is missing"))?;
        store.register_table(entry.clone()).await.map_err(catalog_store_error)?;
        let table_id = entry.table_id.clone();
        (
            EXTERNAL_CATALOG_ACTION_REGISTERED.to_string(),
            load_table_response_from_entry(entry, target_metadata),
            table_id,
            true,
        )
    };

    let bridge_entry = if publish_bridge {
        let mut bridge_entry =
            external_catalog_bridge_entry_from_sync_request(bucket, namespace, table, &request, external_table_uuid)?;
        bridge_entry.table_id = table_id;
        store
            .put_external_catalog_bridge(bridge_entry)
            .await
            .map_err(catalog_store_error)?
    } else {
        store
            .get_external_catalog_bridge(bucket, &namespace.public_name(), table)
            .await
            .map_err(catalog_store_error)?
            .filter(|bridge| bridge.table_id == table_id)
            .ok_or_else(|| {
                iceberg_rest_error(
                    ICEBERG_ERROR_COMMIT_FAILED,
                    StatusCode::CONFLICT,
                    "historical external catalog sync cannot reconstruct the current bridge state",
                )
            })?
    };
    Ok(ExternalCatalogBridgeSyncResponse {
        action,
        table: table_response,
        bridge: external_catalog_bridge_response_from_entry(bucket, namespace, table, Some(bridge_entry)),
    })
}

#[cfg(test)]
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
    let metadata_location = table_metadata_location_for_catalog(bucket, &request.metadata_location)?;
    let metadata = read_table_metadata_json(metadata_backend, bucket, &metadata_location).await?;
    catalog_import_response_with_metadata(
        store,
        metadata_backend,
        RestTableRoute {
            bucket,
            namespace,
            table,
        },
        request,
        metadata,
        table_bucket_enabled,
    )
    .await
}

async fn catalog_import_response_with_metadata<S, B>(
    store: &S,
    metadata_backend: &B,
    route: RestTableRoute<'_>,
    request: CatalogImportRequest,
    metadata: serde_json::Value,
    table_bucket_enabled: bool,
) -> S3Result<RestLoadTableResponse>
where
    S: crate::table_catalog::TableCatalogStore + ?Sized,
    B: crate::table_catalog::TableCatalogObjectBackend,
{
    let RestTableRoute {
        bucket,
        namespace,
        table,
    } = route;
    let started = Instant::now();
    let result = async {
        ensure_table_bucket_entry(store, bucket, table_bucket_enabled).await?;
        let mut entry = table_entry_from_import_request(bucket, namespace, table, request)?;
        validate_metadata_table_location_in_bucket(bucket, &metadata)?;
        adopt_registered_metadata_identity(&mut entry, &metadata)?;
        validate_supported_table_metadata(&metadata)?;
        if !crate::table_catalog::is_valid_table_metadata_location_for_entry(&entry, &entry.metadata_location) {
            return Err(s3_error!(InvalidRequest, "metadata location must be inside the table metadata directory"));
        }
        let table_name = crate::table_catalog::IdentifierSegment::parse(table.to_string())
            .map_err(|err| s3_error!(InvalidRequest, "invalid table name: {}", err))?;
        let snapshot_context = SnapshotReadContext {
            metadata_backend,
            bucket,
            namespace,
            table: &table_name,
            entry: &entry,
        };
        validate_table_snapshot_graph(&snapshot_context, &metadata).await?;
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
    target_metadata: serde_json::Value,
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
        let metadata_location = table_metadata_location_for_catalog(bucket, &request.metadata_location)?;
        let (commit_id, _) = standard_commit_ids(request.commit_id, request.idempotency_key.as_deref());
        if let Some(replay) = published_api_commit_replay(
            store,
            PublishedApiCommitReplay {
                route: RestTableRoute {
                    bucket,
                    namespace,
                    table,
                },
                current: &current,
                commit_id: &commit_id,
                idempotency_key: request.idempotency_key.as_deref(),
                operation: "rollback",
                expected_version_token: &request.version_token,
                new_metadata_location: &metadata_location,
                expected_metadata_location: None,
            },
        )
        .await?
        {
            let result = store.commit_table(replay).await.map_err(catalog_store_error)?;
            return commit_table_replay_response(None, metadata_backend, bucket, result, &metadata_location, target_metadata)
                .await;
        }
        if !crate::table_catalog::is_valid_table_metadata_location_for_entry(&current, &metadata_location) {
            return Err(s3_error!(InvalidRequest, "metadata location must be inside the table metadata directory"));
        }
        let current_metadata = read_table_metadata_json(metadata_backend, bucket, &current.metadata_location).await?;
        validate_metadata_table_location_in_bucket(bucket, &current_metadata)?;
        validate_metadata_table_location_in_bucket(bucket, &target_metadata)?;
        validate_metadata_matches_current_metadata(&current_metadata, &target_metadata)?;
        let table_name = crate::table_catalog::IdentifierSegment::parse(table.to_string())
            .map_err(|err| s3_error!(InvalidRequest, "invalid table name: {}", err))?;
        let snapshot_context = SnapshotReadContext {
            metadata_backend,
            bucket,
            namespace,
            table: &table_name,
            entry: &current,
        };
        validate_table_snapshot_graph(&snapshot_context, &target_metadata).await?;
        let commit_request = crate::table_catalog::TableCommitRequest {
            table_bucket: bucket.to_string(),
            namespace: namespace.public_name(),
            table: table.to_string(),
            commit_id,
            idempotency_key: request.idempotency_key,
            operation: "rollback".to_string(),
            expected_version_token: request.version_token,
            expected_metadata_location: current.metadata_location,
            new_metadata_location: metadata_location,
            requirements: vec![
                base_metadata_digest_requirement(&current_metadata)?,
                metadata_digest_requirement(&target_metadata)?,
            ],
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
        let parent = rest_namespace_parent_from_query(&req.uri)?;
        let resource = match &parent {
            Some(parent) => TableCatalogResource::namespace(&warehouse, parent),
            None => TableCatalogResource::warehouse(&warehouse),
        };
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::GetTableNamespaceAction).await?;
        ensure_table_bucket_enabled(&warehouse).await?;
        let store = table_catalog_store()?;
        let response = list_namespaces_response(&store, &warehouse, parent.as_ref(), &req.uri).await?;
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

pub struct RestUpdateNamespacePropertiesHandler {}

#[async_trait::async_trait]
impl Operation for RestUpdateNamespacePropertiesHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let resource = TableCatalogResource::namespace(&warehouse, &namespace);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::UpdateTableNamespacePropertiesAction).await?;
        ensure_table_bucket_enabled(&warehouse).await?;
        let request = read_bounded_json_body::<UpdateNamespacePropertiesRequest>(
            &req.headers,
            req.input,
            NAMESPACE_PROPERTIES_BODY_MAX_SIZE,
            NAMESPACE_PROPERTIES_BODY_TIMEOUT,
            "namespace properties",
        )
        .await?;
        let store = table_catalog_store()?;
        let response = update_namespace_properties_response(&store, &warehouse, &namespace, request).await?;
        build_json_response(StatusCode::OK, &response)
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

pub struct RestRenameTableHandler {}

#[async_trait::async_trait]
impl Operation for RestRenameTableHandler {
    async fn call(&self, mut req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let principal = table_catalog_request_principal(&req).await?;
        let input = std::mem::replace(&mut req.input, Body::empty());
        let request = read_bounded_json_body::<RenameTableRequest>(
            &req.headers,
            input,
            RENAME_TABLE_BODY_MAX_SIZE,
            RENAME_TABLE_BODY_TIMEOUT,
            "rename table",
        )
        .await?;
        let (source_namespace, source_table) = table_identifier_from_request(request.source)?;
        let (destination_namespace, destination_table) = table_identifier_from_request(request.destination)?;

        let source_resource = TableCatalogResource::table(&warehouse, &source_namespace, source_table.as_str());
        authorize_table_catalog_resource_for_principal(&req, &principal, &source_resource, AdminAction::SetTableAction).await?;
        let destination_resource = TableCatalogResource::table(&warehouse, &destination_namespace, destination_table.as_str());
        authorize_table_catalog_resource_for_principal(&req, &principal, &destination_resource, AdminAction::SetTableAction)
            .await?;
        ensure_table_bucket_enabled(&warehouse).await?;

        let store = table_catalog_store()?;
        store
            .rename_table(
                &warehouse,
                &source_namespace.public_name(),
                source_table.as_str(),
                &destination_namespace.public_name(),
                destination_table.as_str(),
            )
            .await
            .map_err(catalog_store_error)?;
        Ok(empty_response(StatusCode::NO_CONTENT))
    }
}

pub struct RestCreateTableHandler {}

#[async_trait::async_trait]
impl Operation for RestCreateTableHandler {
    async fn call(&self, mut req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let resource = TableCatalogResource::namespace(&warehouse, &namespace);
        let principal = authorize_table_catalog_resource_request(&req, &resource, AdminAction::CreateTableAction).await?;
        let input = std::mem::replace(&mut req.input, Body::empty());
        let request = read_json_body::<CreateTableRequest>(input).await?;
        if let Some(location) = request.location.as_deref() {
            authorize_table_catalog_resource_for_principal(&req, &principal, &resource, AdminAction::RegisterTableAction).await?;
            authorize_table_warehouse_claim(&req, &principal, &warehouse, location).await?;
        }
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
    async fn call(&self, mut req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let resource = TableCatalogResource::namespace(&warehouse, &namespace);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::RegisterTableAction).await?;
        let input = std::mem::replace(&mut req.input, Body::empty());
        let request = read_json_body::<RegisterTableRequest>(input).await?;
        let metadata_backend = table_catalog_backend()?;
        let metadata_location = table_metadata_location_for_catalog(&warehouse, &request.metadata_location)?;
        let (metadata, metadata_guard) =
            read_authorized_table_metadata_json(&mut req, &metadata_backend, &warehouse, &metadata_location).await?;
        let store = table_catalog_store_from_backend(metadata_backend.clone())?;
        let table_bucket_enabled = table_bucket_enabled_from_metadata(&warehouse).await?;
        crate::table_catalog::ensure_table_catalog_lock_held(metadata_guard.as_ref()).map_err(catalog_store_error)?;
        let response =
            register_table_response(&store, &metadata_backend, &warehouse, &namespace, request, metadata, table_bucket_enabled)
                .await?;
        drop(metadata_guard);
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
        let principal = authorize_table_catalog_resource_request(&req, &resource, AdminAction::GetTableCredentialsAction).await?;
        ensure_table_bucket_enabled(&warehouse).await?;
        let store = table_catalog_store()?;
        let issuer = IamTableCredentialIssuer::from_env();
        let response =
            load_credentials_response(&store, &warehouse, &namespace, &table, &issuer, Some(&principal.credentials)).await?;
        build_sensitive_json_response(StatusCode::OK, &response)
    }
}

pub struct RestCommitTableHandler {}

#[async_trait::async_trait]
impl Operation for RestCommitTableHandler {
    async fn call(&self, mut req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let table = table_name_from_params(&params)?;
        let resource = TableCatalogResource::table(&warehouse, &namespace, &table);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::CommitTableAction).await?;
        ensure_table_bucket_enabled(&warehouse).await?;
        let input = std::mem::replace(&mut req.input, Body::empty());
        let mut request = read_rest_commit_table_request(&req.headers, input).await?;
        let metadata_backend = table_catalog_backend()?;
        let store = table_catalog_store_from_backend(metadata_backend.clone())?;
        let current = store
            .load_table(&warehouse, &namespace.public_name(), &table)
            .await
            .map_err(catalog_store_error)?
            .ok_or_else(|| iceberg_rest_error(ICEBERG_ERROR_NO_SUCH_TABLE, StatusCode::NOT_FOUND, "table not found"))?;
        table_commit_warehouse_read_location(Some(&mut req), &store, &metadata_backend, &warehouse, &current, &request).await?;
        let target_metadata = if let Some(metadata_location) = request.new_metadata_location.as_deref() {
            let metadata_location = table_metadata_location_for_catalog(&warehouse, metadata_location)?;
            authorize_table_catalog_s3_actions(&mut req, &warehouse, &metadata_location, &[S3Action::GetObjectAction]).await?;
            let metadata = read_existing_table_metadata_target(
                &store,
                &metadata_backend,
                &warehouse,
                &namespace,
                &table,
                &metadata_location,
            )
            .await?;
            request.new_metadata_location = Some(metadata_location);
            Some(metadata)
        } else {
            None
        };
        let response = commit_table_response_with_target_metadata(
            Some(&mut req),
            &store,
            &metadata_backend,
            RestTableRoute {
                bucket: &warehouse,
                namespace: &namespace,
                table: &table,
            },
            request,
            target_metadata,
        )
        .await?;
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
        if rest_purge_requested_from_query(&req.uri)? {
            return Err(iceberg_rest_error(
                ICEBERG_ERROR_UNSUPPORTED_OPERATION,
                StatusCode::NOT_ACCEPTABLE,
                "purgeRequested=true is not supported",
            ));
        }
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
    async fn call(&self, mut req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let table = table_name_from_params(&params)?;
        let ref_name = ref_name_from_params(&params)?;
        let resource = TableCatalogResource::table(&warehouse, &namespace, &table);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::CommitTableAction).await?;
        ensure_table_bucket_enabled(&warehouse).await?;
        let input = std::mem::replace(&mut req.input, Body::empty());
        let request = read_json_body::<PutTableRefRequest>(input).await?;
        let metadata_backend = table_catalog_backend()?;
        let store = table_catalog_store_from_backend(metadata_backend.clone())?;
        let response = put_table_ref_response(
            Some(&mut req),
            &store,
            &metadata_backend,
            RestTableRoute {
                bucket: &warehouse,
                namespace: &namespace,
                table: &table,
            },
            &ref_name,
            request,
        )
        .await?;
        build_json_response(StatusCode::OK, &response)
    }
}

pub struct DeleteTableRefHandler {}

#[async_trait::async_trait]
impl Operation for DeleteTableRefHandler {
    async fn call(&self, mut req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let table = table_name_from_params(&params)?;
        let ref_name = ref_name_from_params(&params)?;
        let resource = TableCatalogResource::table(&warehouse, &namespace, &table);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::CommitTableAction).await?;
        ensure_table_bucket_enabled(&warehouse).await?;
        let input = std::mem::replace(&mut req.input, Body::empty());
        let request = read_json_body_or_default::<DeleteTableRefRequest>(input).await?;
        let metadata_backend = table_catalog_backend()?;
        let store = table_catalog_store_from_backend(metadata_backend.clone())?;
        let response = delete_table_ref_response(
            Some(&mut req),
            &store,
            &metadata_backend,
            RestTableRoute {
                bucket: &warehouse,
                namespace: &namespace,
                table: &table,
            },
            &ref_name,
            request,
        )
        .await?;
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
    async fn call(&self, mut req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let table = table_name_from_params(&params)?;
        let resource = TableCatalogResource::table(&warehouse, &namespace, &table);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::SetTableMetadataLocationAction).await?;
        ensure_table_bucket_enabled(&warehouse).await?;
        let input = std::mem::replace(&mut req.input, Body::empty());
        let mut request = read_json_body::<UpdateTableMetadataLocationRequest>(input).await?;
        let metadata_backend = table_catalog_backend()?;
        request.metadata_location = table_metadata_location_for_catalog(&warehouse, &request.metadata_location)?;
        let store = table_catalog_store_from_backend(metadata_backend.clone())?;
        authorize_table_catalog_s3_actions(&mut req, &warehouse, &request.metadata_location, &[S3Action::GetObjectAction])
            .await?;
        let target_metadata = read_existing_table_metadata_target(
            &store,
            &metadata_backend,
            &warehouse,
            &namespace,
            &table,
            &request.metadata_location,
        )
        .await?;
        let response = update_table_metadata_location_response(
            &store,
            &metadata_backend,
            &warehouse,
            &namespace,
            &table,
            request,
            target_metadata,
        )
        .await?;
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
    async fn call(&self, mut req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let table = table_name_from_params(&params)?;
        let resource = TableCatalogResource::table(&warehouse, &namespace, &table);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::RegisterTableAction).await?;
        let input = std::mem::replace(&mut req.input, Body::empty());
        let mut request = read_json_body::<CatalogImportRequest>(input).await?;
        let metadata_backend = table_catalog_backend()?;
        request.metadata_location = table_metadata_location_for_catalog(&warehouse, &request.metadata_location)?;
        let (metadata, metadata_guard) =
            read_authorized_table_metadata_json(&mut req, &metadata_backend, &warehouse, &request.metadata_location).await?;
        let store = table_catalog_store_from_backend(metadata_backend.clone())?;
        let table_bucket_enabled = table_bucket_enabled_from_metadata(&warehouse).await?;
        crate::table_catalog::ensure_table_catalog_lock_held(metadata_guard.as_ref()).map_err(catalog_store_error)?;
        let response = catalog_import_response_with_metadata(
            &store,
            &metadata_backend,
            RestTableRoute {
                bucket: &warehouse,
                namespace: &namespace,
                table: &table,
            },
            request,
            metadata,
            table_bucket_enabled,
        )
        .await?;
        drop(metadata_guard);
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
    async fn call(&self, mut req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let table = table_name_from_params(&params)?;
        let resource = TableCatalogResource::table(&warehouse, &namespace, &table);
        let principal =
            authorize_table_catalog_resource_request(&req, &resource, AdminAction::SetTableMetadataLocationAction).await?;
        authorize_table_catalog_resource_for_principal(&req, &principal, &resource, AdminAction::RegisterTableAction).await?;
        ensure_table_bucket_enabled(&warehouse).await?;
        let metadata_backend = table_catalog_backend()?;
        let store = table_catalog_object_store()?;
        let input = std::mem::replace(&mut req.input, Body::empty());
        let mut request = read_json_body::<ExternalCatalogBridgeSyncRequest>(input).await?;
        request.metadata_location = table_metadata_location_for_catalog(&warehouse, &request.metadata_location)?;
        request.expected_metadata_location = request
            .expected_metadata_location
            .map(|metadata_location| table_metadata_location_for_catalog(&warehouse, &metadata_location))
            .transpose()?;
        let table_bucket_enabled = table_bucket_enabled_from_metadata(&warehouse).await?;
        ensure_table_bucket_entry(&store, &warehouse, table_bucket_enabled).await?;
        validate_external_catalog_metadata_location(&namespace, &table, &request.metadata_location)?;
        let current = store
            .load_table(&warehouse, &namespace.public_name(), &table)
            .await
            .map_err(catalog_store_error)?;
        let (target_metadata, metadata_guard) =
            read_authorized_table_metadata_json(&mut req, &metadata_backend, &warehouse, &request.metadata_location).await?;
        crate::table_catalog::ensure_table_catalog_lock_held(metadata_guard.as_ref()).map_err(catalog_store_error)?;
        let response = sync_external_catalog_bridge_response_with_snapshot(
            &store,
            &metadata_backend,
            &warehouse,
            &namespace,
            &table,
            ExternalCatalogBridgeSyncSnapshot {
                request,
                target_metadata,
                current,
            },
        )
        .await?;
        drop(metadata_guard);
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
    async fn call(&self, mut req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let table = table_name_from_params(&params)?;
        let resource = TableCatalogResource::table(&warehouse, &namespace, &table);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::CommitTableAction).await?;
        ensure_table_bucket_enabled(&warehouse).await?;
        let input = std::mem::replace(&mut req.input, Body::empty());
        let mut request = read_json_body::<RollbackTableRequest>(input).await?;
        let metadata_backend = table_catalog_backend()?;
        request.metadata_location = table_metadata_location_for_catalog(&warehouse, &request.metadata_location)?;
        let store = table_catalog_store_from_backend(metadata_backend.clone())?;
        authorize_table_catalog_s3_actions(&mut req, &warehouse, &request.metadata_location, &[S3Action::GetObjectAction])
            .await?;
        let target_metadata = read_existing_table_metadata_target(
            &store,
            &metadata_backend,
            &warehouse,
            &namespace,
            &table,
            &request.metadata_location,
        )
        .await?;
        let response =
            rollback_table_response(&store, &metadata_backend, &warehouse, &namespace, &table, request, target_metadata).await?;
        build_json_response(StatusCode::OK, &response)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table_catalog::{TableCatalogObjectBackend, TableCatalogStore};
    use bytes::Bytes;
    use std::sync::Arc;

    fn test_table_metadata_json(table_uuid: &str, location: &str) -> serde_json::Value {
        serde_json::json!({
            "format-version": 2,
            "table-uuid": table_uuid,
            "location": location,
            "last-sequence-number": 0,
            "last-updated-ms": 0,
            "last-column-id": 0,
            "schemas": [{"schema-id": 0, "type": "struct", "fields": []}],
            "current-schema-id": 0,
            "partition-specs": [{"spec-id": 0, "fields": []}],
            "default-spec-id": 0,
            "last-partition-id": 999,
            "sort-orders": [{"order-id": 0, "fields": []}],
            "default-sort-order-id": 0,
            "properties": {},
            "snapshots": [],
            "snapshot-log": [],
            "metadata-log": [],
            "refs": {}
        })
    }

    #[test]
    fn snapshot_read_budget_accepts_exact_limits_and_rejects_excess() {
        let mut budget = SnapshotReadBudget {
            manifest_count: TABLE_COMMIT_MAX_MANIFESTS - 2,
            avro_bytes: TABLE_COMMIT_MAX_AVRO_BYTES - 3,
            file_reference_count: TABLE_COMMIT_MAX_FILE_REFERENCES - 4,
            ..SnapshotReadBudget::default()
        };

        budget.charge_manifests(2).expect("exact manifest limit should be accepted");
        budget.charge_avro_bytes(3).expect("exact Avro byte limit should be accepted");
        budget
            .charge_file_references(4)
            .expect("exact file reference limit should be accepted");

        let manifest_error = budget
            .charge_manifests(1)
            .expect_err("manifest count above the limit should fail");
        let avro_error = budget
            .charge_avro_bytes(1)
            .expect_err("Avro bytes above the limit should fail");
        let reference_error = budget
            .charge_file_references(1)
            .expect_err("file reference count above the limit should fail");
        for error in [manifest_error, avro_error, reference_error] {
            assert_eq!(error.code(), &S3ErrorCode::InvalidRequest);
        }
    }

    #[tokio::test]
    async fn manifest_object_validation_is_bounded_and_preserves_missing_object_errors() {
        let backend = TestTableCatalogObjectBackend {
            object_exists_delay: Some(StdDuration::from_millis(20)),
            ..Default::default()
        };
        let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
        let table = crate::table_catalog::IdentifierSegment::parse("events").expect("table should parse");
        let entry = crate::table_catalog::TableEntry {
            version: crate::table_catalog::TABLE_CATALOG_ENTRY_VERSION,
            table_bucket: "warehouse".to_string(),
            namespace: namespace.public_name(),
            table: table.as_str().to_string(),
            table_id: "table-id".to_string(),
            table_uuid: "table-uuid".to_string(),
            format: "ICEBERG".to_string(),
            format_version: 2,
            warehouse_location: "s3://warehouse/tables/table-id".to_string(),
            metadata_location: "tables/table-id/metadata/00001.metadata.json".to_string(),
            version_token: "token-v1".to_string(),
            generation: 1,
            state: crate::table_catalog::TableCatalogEntryState::Active,
            properties: BTreeMap::new(),
            created_at: None,
            updated_at: None,
        };
        let mut references = Vec::new();
        for index in 0..(TABLE_COMMIT_OBJECT_VALIDATION_CONCURRENCY * 2) {
            let location = format!("tables/table-id/data/part-{index}.parquet");
            backend.put_bytes("warehouse", &location, b"data".to_vec()).await;
            references.push(crate::table_catalog::ManifestDataFileReference {
                location,
                content: crate::table_catalog::ManifestDataFileContent::Data,
                object_kind: crate::table_catalog::TableMetadataMaintenanceObjectKind::DataFile,
                entry_status: Some(1),
                snapshot_id: Some(1),
                sequence_number: Some(1),
                file_sequence_number: Some(1),
                record_count: Some(1),
                file_size_bytes: Some(4),
                partition: Vec::new(),
                sort_order_id: None,
            });
        }
        let context = SnapshotReadContext {
            metadata_backend: &backend,
            bucket: "warehouse",
            namespace: &namespace,
            table: &table,
            entry: &entry,
        };

        validate_manifest_data_file_references(&context, &references)
            .await
            .expect("existing manifest objects should validate");
        assert!(backend.object_exists_max_in_flight() > 1);
        assert!(backend.object_exists_max_in_flight() <= TABLE_COMMIT_OBJECT_VALIDATION_CONCURRENCY);

        backend
            .delete_object("warehouse", &references[0].location)
            .await
            .expect("test object should be removed");
        let error = validate_manifest_data_file_references(&context, &references)
            .await
            .expect_err("a missing manifest object should still fail validation");
        assert_eq!(error.code(), &S3ErrorCode::InvalidRequest);

        references[0].entry_status = Some(2);
        validate_manifest_data_file_references(&context, &references)
            .await
            .expect("a deleted manifest entry need not retain its physical file");
    }

    #[tokio::test]
    async fn snapshot_graph_rejects_unknown_manifest_partition_spec() {
        let backend = TestTableCatalogObjectBackend::default();
        let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
        let table = crate::table_catalog::IdentifierSegment::parse("events").expect("table should parse");
        let entry = crate::table_catalog::TableEntry {
            version: crate::table_catalog::TABLE_CATALOG_ENTRY_VERSION,
            table_bucket: "warehouse".to_string(),
            namespace: namespace.public_name(),
            table: table.as_str().to_string(),
            table_id: "table-id".to_string(),
            table_uuid: "table-uuid".to_string(),
            format: "ICEBERG".to_string(),
            format_version: 2,
            warehouse_location: "s3://warehouse/tables/table-id".to_string(),
            metadata_location: "tables/table-id/metadata/00001.metadata.json".to_string(),
            version_token: "token-v1".to_string(),
            generation: 1,
            state: crate::table_catalog::TableCatalogEntryState::Active,
            properties: BTreeMap::new(),
            created_at: None,
            updated_at: None,
        };
        let manifest_list = "s3://warehouse/tables/table-id/metadata/snap-1.avro";
        let manifest = "s3://warehouse/tables/table-id/metadata/manifest-1.avro";
        backend
            .put_bytes(
                "warehouse",
                &test_snapshot_object_key("warehouse", manifest_list),
                test_manifest_list_avro_entries_with_spec(&[(manifest, 9, 1, 1)]),
            )
            .await;
        let mut metadata = test_table_metadata_json("table-uuid", "s3://warehouse/tables/table-id");
        metadata["last-sequence-number"] = serde_json::Value::from(1);
        metadata["snapshots"] = serde_json::json!([{
            "snapshot-id": 1,
            "sequence-number": 1,
            "timestamp-ms": 1,
            "manifest-list": manifest_list,
            "summary": {"operation": "append"}
        }]);
        metadata["current-snapshot-id"] = serde_json::Value::from(1);
        metadata["refs"] = serde_json::json!({"main": {"type": "branch", "snapshot-id": 1}});
        let context = SnapshotReadContext {
            metadata_backend: &backend,
            bucket: "warehouse",
            namespace: &namespace,
            table: &table,
            entry: &entry,
        };

        let error = validate_table_snapshot_graph(&context, &metadata)
            .await
            .expect_err("manifest-list partition spec must exist in table metadata");

        assert_eq!(error.code(), &S3ErrorCode::InvalidRequest);
        assert_eq!(error.message(), Some("snapshot manifest references missing partition spec 9"));
    }

    #[tokio::test]
    async fn snapshot_graph_reads_shared_manifest_objects_once() {
        let backend = TestTableCatalogObjectBackend::default();
        let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
        let table = crate::table_catalog::IdentifierSegment::parse("events").expect("table should parse");
        let entry = crate::table_catalog::TableEntry {
            version: crate::table_catalog::TABLE_CATALOG_ENTRY_VERSION,
            table_bucket: "warehouse".to_string(),
            namespace: namespace.public_name(),
            table: table.as_str().to_string(),
            table_id: "table-id".to_string(),
            table_uuid: "table-uuid".to_string(),
            format: "ICEBERG".to_string(),
            format_version: 2,
            warehouse_location: "s3://warehouse/tables/table-id".to_string(),
            metadata_location: "tables/table-id/metadata/00001.metadata.json".to_string(),
            version_token: "token-v1".to_string(),
            generation: 1,
            state: crate::table_catalog::TableCatalogEntryState::Active,
            properties: BTreeMap::new(),
            created_at: None,
            updated_at: None,
        };
        let manifest_list = "s3://warehouse/tables/table-id/metadata/snap-1.avro";
        let data_file = "s3://warehouse/tables/table-id/data/part-1.parquet";
        seed_test_snapshot_manifest(&backend, "warehouse", manifest_list, 1, 1, &[(data_file, 0, 1, 1, 1)]).await;
        let mut metadata = test_table_metadata_json("table-uuid", "s3://warehouse/tables/table-id");
        metadata["last-sequence-number"] = serde_json::Value::from(2);
        metadata["snapshots"] = serde_json::json!([
            {
                "snapshot-id": 1,
                "sequence-number": 1,
                "timestamp-ms": 1,
                "manifest-list": manifest_list,
                "summary": {"operation": "append"}
            },
            {
                "snapshot-id": 2,
                "parent-snapshot-id": 1,
                "sequence-number": 2,
                "timestamp-ms": 2,
                "manifest-list": manifest_list,
                "summary": {"operation": "append"}
            }
        ]);
        metadata["current-snapshot-id"] = serde_json::Value::from(2);
        metadata["refs"] = serde_json::json!({"main": {"type": "branch", "snapshot-id": 2}});
        let context = SnapshotReadContext {
            metadata_backend: &backend,
            bucket: "warehouse",
            namespace: &namespace,
            table: &table,
            entry: &entry,
        };

        validate_table_snapshot_graph(&context, &metadata)
            .await
            .expect("shared snapshot graph should validate");

        assert_eq!(backend.read_object_call_count(), 2);

        let mut budget = SnapshotReadBudget {
            file_reference_count: TABLE_COMMIT_MAX_FILE_REFERENCES - 1,
            ..SnapshotReadBudget::default()
        };
        read_snapshot_manifest_references(&context, &metadata, &metadata["snapshots"][0], &mut budget)
            .await
            .expect("the first manifest traversal should reach the file reference limit");
        let error = read_snapshot_manifest_references(&context, &metadata, &metadata["snapshots"][1], &mut budget)
            .await
            .err()
            .expect("a cached manifest traversal must still consume the file reference budget");
        assert_eq!(error.code(), &S3ErrorCode::InvalidRequest);
    }

    #[test]
    #[serial_test::serial]
    fn catalog_config_response_lists_standard_rest_endpoints() {
        let response =
            temp_env::with_var_unset(crate::table_catalog::ENV_TABLE_CATALOG_BACKING, || catalog_config_response(None))
                .expect("catalog config should build");

        assert_eq!(response.defaults.get(WAREHOUSE_PROPERTY).map(String::as_str), Some(DEFAULT_WAREHOUSE_ID));
        assert_eq!(
            response.defaults.get(CATALOG_ENDPOINT_PREFIX_CONFIG_KEY).map(String::as_str),
            Some(TABLE_CATALOG_PREFIX)
        );
        assert_eq!(
            response
                .defaults
                .get(CATALOG_COMPAT_ENDPOINT_PREFIX_CONFIG_KEY)
                .map(String::as_str),
            Some(TABLE_CATALOG_COMPAT_PREFIX)
        );
        assert_eq!(
            response.defaults.get(CATALOG_BACKING_CONFIG_KEY).map(String::as_str),
            Some(crate::table_catalog::TABLE_CATALOG_BACKING_OBJECT)
        );
        assert!(!response.defaults.contains_key(PREFIX_PROPERTY));
        assert_eq!(
            response.overrides.get(NAMESPACE_SEPARATOR_PROPERTY).map(String::as_str),
            Some(REST_NAMESPACE_SEPARATOR_URL_ENCODED)
        );
        assert_eq!(response.admin_discovery.runtime_capabilities, "/rustfs/admin/v4/runtime/capabilities");
        assert_eq!(response.admin_discovery.cluster_snapshot, "/rustfs/admin/v4/cluster/snapshot");
        assert_eq!(response.admin_discovery.extensions_catalog, "/rustfs/admin/v4/extensions/catalog");
        assert_eq!(response.endpoints.as_slice(), TABLE_CATALOG_ENDPOINTS);
        assert!(response.endpoints.iter().all(|endpoint| endpoint.contains("/v1/{prefix}/")));
    }

    #[test]
    #[serial_test::serial]
    fn catalog_config_response_reports_durable_strong_backing_override() {
        let response = temp_env::with_var(
            crate::table_catalog::ENV_TABLE_CATALOG_BACKING,
            Some(crate::table_catalog::TABLE_CATALOG_BACKING_DURABLE_STRONG),
            || catalog_config_response(None),
        )
        .expect("catalog config should build");

        assert_eq!(
            response.overrides.get(CATALOG_BACKING_CONFIG_KEY).map(String::as_str),
            Some(crate::table_catalog::TABLE_CATALOG_BACKING_DURABLE_STRONG)
        );
        assert!(
            response
                .endpoints
                .contains(&"POST /v1/{prefix}/namespaces/{namespace}/properties")
        );
        assert!(response.endpoints.contains(&"POST /v1/{prefix}/tables/rename"));
        assert!(!response.endpoints.contains(&"POST /{warehouse}/tables/rename"));
    }

    #[test]
    #[serial_test::serial]
    fn catalog_config_response_uses_requested_warehouse_as_standard_prefix() {
        let response = temp_env::with_var_unset(crate::table_catalog::ENV_TABLE_CATALOG_BACKING, || {
            catalog_config_response(Some("analytics"))
        })
        .expect("catalog config should build");

        assert_eq!(response.defaults.get(PREFIX_PROPERTY).map(String::as_str), Some("analytics"));
    }

    #[test]
    fn warehouse_config_query_rejects_empty_and_repeated_values() {
        let uri = "/iceberg/v1/config?warehouse=analytics".parse().expect("URI");
        assert_eq!(warehouse_from_config_query(&uri).expect("warehouse query"), Some("analytics".to_string()));

        let uri = "/iceberg/v1/config?warehouse=".parse().expect("URI");
        assert!(warehouse_from_config_query(&uri).is_err());

        let uri = "/iceberg/v1/config?warehouse=one&warehouse=two".parse().expect("URI");
        assert!(warehouse_from_config_query(&uri).is_err());
    }

    #[test]
    fn drop_table_purge_query_is_explicit_and_strict() {
        for (uri, expected) in [
            ("/iceberg/v1/analytics/namespaces/sales/tables/orders", false),
            ("/iceberg/v1/analytics/namespaces/sales/tables/orders?purgeRequested=false", false),
            ("/iceberg/v1/analytics/namespaces/sales/tables/orders?purgeRequested=true", true),
        ] {
            assert_eq!(
                rest_purge_requested_from_query(&uri.parse().expect("URI")).expect("purge query should parse"),
                expected
            );
        }

        for uri in [
            "/iceberg/v1/analytics/namespaces/sales/tables/orders?purgeRequested=1",
            "/iceberg/v1/analytics/namespaces/sales/tables/orders?purgeRequested=true&purgeRequested=false",
        ] {
            let error = rest_purge_requested_from_query(&uri.parse().expect("URI")).expect_err("invalid purge query should fail");
            assert_eq!(error.code(), &S3ErrorCode::Custom(ICEBERG_ERROR_BAD_REQUEST.into()));
            assert_eq!(error.status_code(), Some(StatusCode::BAD_REQUEST));
        }
    }

    #[test]
    fn rest_pagination_round_trips_opaque_context_bound_tokens() {
        let context = RestPageContext {
            resource: TABLE_CATALOG_NAMESPACE_RESOURCE_ROOT,
            warehouse: "analytics",
            namespace: None,
        };
        let first_uri = "/iceberg/v1/analytics/namespaces?pageToken=&pageSize=2".parse().expect("URI");
        let first_pagination = rest_pagination_from_query(&first_uri, context).expect("first page query should parse");
        let (cursor, limit) = first_pagination.page_request().expect("first page should be paginated");
        let first = crate::table_catalog::catalog_list_page_from_entries(
            vec!["a".to_string(), "b".to_string(), "c".to_string()],
            cursor,
            limit,
            String::as_str,
        );
        assert_eq!(first.entries, vec!["a".to_string(), "b".to_string()]);
        let next_page_token = first_pagination
            .next_page_token(first.next_cursor)
            .expect("first page token should encode")
            .expect("first page should return a token");

        let exact_page_uri = "/iceberg/v1/analytics/namespaces?pageToken=&pageSize=2"
            .parse()
            .expect("exact page URI");
        let exact_page_pagination = rest_pagination_from_query(&exact_page_uri, context).expect("exact page query should parse");
        let (cursor, limit) = exact_page_pagination.page_request().expect("exact page should be paginated");
        let exact_page = crate::table_catalog::catalog_list_page_from_entries(
            vec!["a".to_string(), "b".to_string()],
            cursor,
            limit,
            String::as_str,
        );
        assert_eq!(exact_page.entries, vec!["a".to_string(), "b".to_string()]);
        assert!(
            exact_page_pagination
                .next_page_token(exact_page.next_cursor)
                .expect("terminal token should encode")
                .is_none()
        );

        let query = url::form_urlencoded::Serializer::new(String::new())
            .append_pair("pageToken", &next_page_token)
            .append_pair("pageSize", "2")
            .finish();
        let second_uri = format!("/iceberg/v1/analytics/namespaces?{query}")
            .parse()
            .expect("second page URI");
        let second_pagination = rest_pagination_from_query(&second_uri, context).expect("second page query should parse");
        let (cursor, limit) = second_pagination.page_request().expect("second page should be paginated");
        let second = crate::table_catalog::catalog_list_page_from_entries(
            vec!["aa".to_string(), "c".to_string(), "d".to_string()],
            cursor,
            limit,
            String::as_str,
        );
        assert_eq!(second.entries, vec!["c".to_string(), "d".to_string()]);
        assert!(
            second_pagination
                .next_page_token(second.next_cursor)
                .expect("terminal token should encode")
                .is_none()
        );
    }

    #[test]
    fn rest_pagination_rejects_malformed_repeated_and_cross_context_tokens() {
        let namespace_context = RestPageContext {
            resource: TABLE_CATALOG_NAMESPACE_RESOURCE_ROOT,
            warehouse: "analytics",
            namespace: None,
        };
        for uri in [
            "/iceberg/v1/analytics/namespaces?pageToken=not-base64!",
            "/iceberg/v1/analytics/namespaces?pageToken=one&pageToken=two",
            "/iceberg/v1/analytics/namespaces?pageSize=0",
            "/iceberg/v1/analytics/namespaces?pageSize=one",
            "/iceberg/v1/analytics/namespaces?pageSize=1&pageSize=2",
        ] {
            let error = rest_pagination_from_query(&uri.parse().expect("URI"), namespace_context)
                .expect_err("invalid pagination query should fail");
            assert_eq!(error.code(), &S3ErrorCode::Custom(ICEBERG_ERROR_BAD_REQUEST.into()));
            assert_eq!(error.status_code(), Some(StatusCode::BAD_REQUEST));
        }

        let table_context = RestPageContext {
            resource: TABLE_CATALOG_TABLE_RESOURCE_ROOT,
            warehouse: "analytics",
            namespace: Some("sales"),
        };
        let table_token =
            encode_rest_page_token("orders", &rest_page_context_fingerprint(table_context)).expect("table token should encode");
        let query = url::form_urlencoded::Serializer::new(String::new())
            .append_pair("pageToken", &table_token)
            .finish();
        let uri = format!("/iceberg/v1/analytics/namespaces?{query}")
            .parse()
            .expect("cross-context URI");
        let error = rest_pagination_from_query(&uri, namespace_context).expect_err("cross-context token should fail");
        assert_eq!(error.code(), &S3ErrorCode::Custom(ICEBERG_ERROR_BAD_REQUEST.into()));
        assert_eq!(error.status_code(), Some(StatusCode::BAD_REQUEST));

        let other_warehouse_context = RestPageContext {
            resource: TABLE_CATALOG_NAMESPACE_RESOURCE_ROOT,
            warehouse: "archive",
            namespace: None,
        };
        let warehouse_token = encode_rest_page_token("sales", &rest_page_context_fingerprint(namespace_context))
            .expect("namespace page token should encode");
        assert!(
            decode_rest_page_token(&warehouse_token, &rest_page_context_fingerprint(other_warehouse_context)).is_err(),
            "namespace page tokens must remain warehouse-scoped"
        );

        let namespace_resource_context = RestPageContext {
            resource: TABLE_CATALOG_NAMESPACE_RESOURCE_ROOT,
            warehouse: "analytics",
            namespace: Some("sales"),
        };
        assert!(
            decode_rest_page_token(&table_token, &rest_page_context_fingerprint(namespace_resource_context)).is_err(),
            "page tokens must remain resource-scoped even when warehouse and namespace match"
        );

        let oversized = "a".repeat(REST_PAGE_TOKEN_MAX_LENGTH + 1);
        let namespace_context_fingerprint = rest_page_context_fingerprint(namespace_context);
        assert!(decode_rest_page_token(&oversized, &namespace_context_fingerprint).is_err());

        for token in [
            RestPageToken {
                version: REST_PAGE_TOKEN_VERSION + 1,
                context: namespace_context_fingerprint.clone(),
                cursor: "sales".to_string(),
            },
            RestPageToken {
                version: REST_PAGE_TOKEN_VERSION,
                context: namespace_context_fingerprint.clone(),
                cursor: String::new(),
            },
        ] {
            let encoded =
                base64_encode_url_safe_no_pad(&serde_json::to_vec(&token).expect("invalid test token should serialize"));
            assert!(decode_rest_page_token(&encoded, &namespace_context_fingerprint).is_err());
        }

        let invalid_json = base64_encode_url_safe_no_pad(b"not-json");
        assert!(decode_rest_page_token(&invalid_json, &namespace_context_fingerprint).is_err());

        let unknown_field = base64_encode_url_safe_no_pad(
            &serde_json::to_vec(&serde_json::json!({
                "version": REST_PAGE_TOKEN_VERSION,
                "context": namespace_context_fingerprint,
                "cursor": "sales",
                "unexpected": true
            }))
            .expect("unknown-field token should serialize"),
        );
        assert!(decode_rest_page_token(&unknown_field, &rest_page_context_fingerprint(namespace_context)).is_err());
    }

    #[test]
    fn namespace_parent_query_accepts_standard_and_legacy_path_separators() {
        let path_namespace = namespace_from_path_value("accounting%1Ftax").expect("encoded path namespace should parse");
        assert_eq!(path_namespace.public_name(), "accounting.tax");
        let lowercase_path_namespace =
            namespace_from_path_value("accounting%1ftax").expect("lowercase encoded path namespace should parse");
        assert_eq!(lowercase_path_namespace.public_name(), "accounting.tax");
        let legacy_path_namespace =
            namespace_from_path_value("accounting.tax").expect("legacy dotted path namespace should parse");
        assert_eq!(legacy_path_namespace.public_name(), "accounting.tax");
        assert!(namespace_from_path_value("accounting%2Etax").is_err());
        assert!(namespace_from_path_value("accounting%2Ftax").is_err());
        assert!(namespace_from_path_value("%FF").is_err());

        let uri = "/iceberg/v1/analytics/namespaces?parent=accounting%1Ftax"
            .parse()
            .expect("URI");
        let parent = rest_namespace_parent_from_query(&uri)
            .expect("parent query should parse")
            .expect("parent should be present");
        assert_eq!(parent.public_name(), "accounting.tax");

        let uri = "/iceberg/v1/analytics/namespaces?parent=".parse().expect("URI");
        assert!(
            rest_namespace_parent_from_query(&uri)
                .expect("empty parent should parse")
                .is_none()
        );

        let uri = "/iceberg/v1/analytics/namespaces?parent=one&parent=two".parse().expect("URI");
        let error = rest_namespace_parent_from_query(&uri).expect_err("repeated parent should fail");
        assert_eq!(error.code(), &S3ErrorCode::Custom(ICEBERG_ERROR_BAD_REQUEST.into()));
        assert_eq!(error.status_code(), Some(StatusCode::BAD_REQUEST));
    }

    #[test]
    fn unknown_commit_requirements_and_updates_are_bad_requests() {
        let unknown_requirement = vec![serde_json::json!({"type": "unknown-requirement"})];
        let unknown_update = vec![serde_json::json!({"action": "unknown-update"})];
        let table_requirement_error = validate_table_commit_requirements(&serde_json::json!({}), &unknown_requirement)
            .expect_err("unknown table requirement should fail");
        let table_update_error =
            apply_table_commit_updates(serde_json::json!({}), &unknown_update, "metadata/00001.metadata.json")
                .expect_err("unknown table update should fail");
        let view_requirement_error = validate_view_commit_requirements(&serde_json::json!({}), &unknown_requirement)
            .expect_err("unknown view requirement should fail");
        let view_update_error =
            apply_view_commit_updates_at(serde_json::json!({}), &unknown_update, "metadata/00001.metadata.json", 0)
                .expect_err("unknown view update should fail");

        for error in [
            table_requirement_error,
            table_update_error,
            view_requirement_error,
            view_update_error,
        ] {
            assert_eq!(error.code(), &S3ErrorCode::InvalidRequest);
        }
    }

    #[test]
    fn catalog_conflicts_use_operation_specific_iceberg_errors() {
        let already_exists = catalog_store_already_exists_error(crate::table_catalog::TableCatalogStoreError::Conflict(
            "table already exists".to_string(),
        ));
        assert_eq!(already_exists.code(), &S3ErrorCode::Custom(ICEBERG_ERROR_ALREADY_EXISTS.into()));
        assert_eq!(already_exists.status_code(), Some(StatusCode::CONFLICT));

        let namespace_not_empty = catalog_store_namespace_drop_error(crate::table_catalog::TableCatalogStoreError::Conflict(
            "namespace is not empty".to_string(),
        ));
        assert_eq!(namespace_not_empty.code(), &S3ErrorCode::Custom(ICEBERG_ERROR_NAMESPACE_NOT_EMPTY.into()));
        assert_eq!(namespace_not_empty.status_code(), Some(StatusCode::CONFLICT));

        let namespace_not_found = catalog_store_namespace_drop_error(crate::table_catalog::TableCatalogStoreError::NotFound(
            "namespace not found".to_string(),
        ));
        assert_eq!(namespace_not_found.code(), &S3ErrorCode::Custom(ICEBERG_ERROR_NO_SUCH_NAMESPACE.into()));
        assert_eq!(namespace_not_found.status_code(), Some(StatusCode::NOT_FOUND));

        let destination_conflict = catalog_store_error(crate::table_catalog::TableCatalogStoreError::AlreadyExists(
            "destination exists".to_string(),
        ));
        assert_eq!(destination_conflict.code(), &S3ErrorCode::Custom(ICEBERG_ERROR_ALREADY_EXISTS.into()));
        assert_eq!(destination_conflict.status_code(), Some(StatusCode::CONFLICT));

        let snapshot_conflict = catalog_store_error(crate::table_catalog::TableCatalogStoreError::Conflict(
            "catalog snapshot changed".to_string(),
        ));
        assert_eq!(snapshot_conflict.code(), &S3ErrorCode::Custom(ICEBERG_ERROR_COMMIT_FAILED.into()));
        assert_eq!(snapshot_conflict.status_code(), Some(StatusCode::CONFLICT));

        let unsupported = catalog_store_error(crate::table_catalog::TableCatalogStoreError::Unsupported(
            "rename requires durable backing".to_string(),
        ));
        assert_eq!(unsupported.code(), &S3ErrorCode::Custom(ICEBERG_ERROR_UNSUPPORTED_OPERATION.into()));
        assert_eq!(unsupported.status_code(), Some(StatusCode::NOT_ACCEPTABLE));

        let table_not_found =
            catalog_store_error(crate::table_catalog::TableCatalogStoreError::TableNotFound("table not found".to_string()));
        assert_eq!(table_not_found.code(), &S3ErrorCode::Custom(ICEBERG_ERROR_NO_SUCH_TABLE.into()));
        assert_eq!(table_not_found.status_code(), Some(StatusCode::NOT_FOUND));

        let view_not_found =
            catalog_store_error(crate::table_catalog::TableCatalogStoreError::ViewNotFound("view not found".to_string()));
        assert_eq!(view_not_found.code(), &S3ErrorCode::Custom(ICEBERG_ERROR_NO_SUCH_VIEW.into()));
        assert_eq!(view_not_found.status_code(), Some(StatusCode::NOT_FOUND));

        let namespace_not_found = catalog_store_error(crate::table_catalog::TableCatalogStoreError::NamespaceNotFound(
            "namespace not found".to_string(),
        ));
        assert_eq!(namespace_not_found.code(), &S3ErrorCode::Custom(ICEBERG_ERROR_NO_SUCH_NAMESPACE.into()));
        assert_eq!(namespace_not_found.status_code(), Some(StatusCode::NOT_FOUND));

        let internal = catalog_store_error(crate::table_catalog::TableCatalogStoreError::Internal(
            "sensitive backend detail".to_string(),
        ));
        assert_eq!(internal.code(), &S3ErrorCode::Custom(ICEBERG_ERROR_REST.into()));
        assert_eq!(internal.status_code(), Some(StatusCode::INTERNAL_SERVER_ERROR));
        assert_eq!(internal.message(), Some("internal table catalog error"));
    }

    #[test]
    fn table_catalog_admin_operation_result_labels_are_stable() {
        let success: Result<(), ()> = Ok(());
        let failure: Result<(), ()> = Err(());

        assert_eq!(table_catalog_admin_operation_result_label(&success), "success");
        assert_eq!(table_catalog_admin_operation_result_label(&failure), "failure");
    }

    #[test]
    fn table_catalog_handlers_require_table_admin_actions() {
        let src = include_str!("table_catalog.rs");

        assert!(
            operation_block(src, "GetCatalogConfigHandler")
                .contains("authorize_table_catalog_request(&req, AdminAction::GetTableCatalogAction).await?;")
        );
        assert!(
            src.contains("validate_admin_request_with_bucket_object("),
            "catalog resource auth should pass namespace/table scope into IAM object matching"
        );

        for (handler, action) in [
            ("EnableTableBucketHandler", "AdminAction::SetTableBucketAction"),
            ("GetTableBucketHandler", "AdminAction::GetTableBucketAction"),
            ("GetTableCatalogMigrationHandler", "AdminAction::GetTableCatalogAction"),
            ("RestListNamespacesHandler", "AdminAction::GetTableNamespaceAction"),
            ("RestCreateNamespaceHandler", "AdminAction::SetTableNamespaceAction"),
            ("RestGetNamespaceHandler", "AdminAction::GetTableNamespaceAction"),
            ("RestNamespaceExistsHandler", "AdminAction::GetTableNamespaceAction"),
            (
                "RestUpdateNamespacePropertiesHandler",
                "AdminAction::UpdateTableNamespacePropertiesAction",
            ),
            ("RestDropNamespaceHandler", "AdminAction::DeleteTableNamespaceAction"),
            ("RestListTablesHandler", "AdminAction::GetTableAction"),
            ("RestCreateTableHandler", "AdminAction::CreateTableAction"),
            ("RestRegisterTableHandler", "AdminAction::RegisterTableAction"),
            ("RestListViewsHandler", "AdminAction::GetTableMetadataAction"),
            ("RestCreateViewHandler", "AdminAction::CreateTableAction"),
            ("RestLoadTableHandler", "AdminAction::GetTableMetadataAction"),
            ("RestTableExistsHandler", "AdminAction::GetTableAction"),
            ("RestLoadCredentialsHandler", "AdminAction::GetTableCredentialsAction"),
            ("RestCommitTableHandler", "AdminAction::CommitTableAction"),
            ("RestDropTableHandler", "AdminAction::DeleteTableAction"),
            ("RestLoadViewHandler", "AdminAction::GetTableMetadataAction"),
            ("RestReplaceViewHandler", "AdminAction::CommitTableAction"),
            ("RestDropViewHandler", "AdminAction::DeleteTableAction"),
            ("ListTableRefsHandler", "AdminAction::GetTableMetadataAction"),
            ("GetTableMetadataLocationHandler", "AdminAction::GetTableMetadataLocationAction"),
            ("UpdateTableMetadataLocationHandler", "AdminAction::SetTableMetadataLocationAction"),
            ("RestTableMetadataMaintenanceHandler", "AdminAction::RunTableMaintenanceAction"),
            ("GetTableMaintenanceConfigHandler", "AdminAction::GetTableLifecycleAction"),
            ("PutTableMaintenanceConfigHandler", "AdminAction::SetTableLifecycleAction"),
            ("GetTableMaintenanceJobHandler", "AdminAction::GetTableLifecycleAction"),
            ("GetTableMaintenanceSchedulerHandler", "AdminAction::GetTableLifecycleAction"),
            ("RunTableMaintenanceSchedulerHandler", "AdminAction::RunTableMaintenanceAction"),
            ("TableMaintenanceQuarantineHandler", "AdminAction::RunTableMaintenanceAction"),
            ("ExportTableCatalogHandler", "AdminAction::GetTableMetadataAction"),
            ("ImportTableCatalogHandler", "AdminAction::RegisterTableAction"),
            ("ExternalCatalogBridgeHandler", "AdminAction::GetTableMetadataAction"),
            ("PutExternalCatalogBridgeHandler", "AdminAction::RegisterTableAction"),
            ("SyncExternalCatalogBridgeHandler", "AdminAction::SetTableMetadataLocationAction"),
            ("GetTableCatalogDiagnosticsHandler", "AdminAction::GetTableMetadataAction"),
            ("RecoverTableCatalogHandler", "AdminAction::CommitTableAction"),
            ("RollbackTableCatalogHandler", "AdminAction::CommitTableAction"),
        ] {
            let block = operation_block(src, handler);
            assert!(
                block.contains(&format!("authorize_table_catalog_resource_request(&req, &resource, {action}).await?;")),
                "{handler} should require {action} with catalog resource auth"
            );
            assert!(
                !block.contains("authorize_table_catalog_request(&req,"),
                "{handler} must not use unscoped table catalog authorization"
            );
            assert!(
                !block.contains("authorize_table_catalog_warehouse_request(&req, &warehouse,"),
                "{handler} should not bypass catalog resource auth"
            );
        }

        let create_table_block = operation_block(src, "RestCreateTableHandler");
        assert!(create_table_block.contains("request.location.as_deref()"));
        assert!(create_table_block.contains("AdminAction::RegisterTableAction"));
        assert!(create_table_block.contains("authorize_table_warehouse_claim"));

        let register_table_block = operation_block(src, "RestRegisterTableHandler");
        assert!(register_table_block.contains("read_authorized_table_metadata_json"));
        assert!(register_table_block.contains("request, metadata, table_bucket_enabled"));
        assert!(
            !function_block(src, "async fn register_table_response").contains("read_table_metadata_json"),
            "register should persist the metadata snapshot used for authorization"
        );
        let warehouse_auth_block = function_block(src, "async fn authorize_table_warehouse_claim");
        assert!(warehouse_auth_block.contains("AdminAction::RegisterTableAction"));
        assert!(warehouse_auth_block.contains("AdminResourceScope::bucket_object"));
        let s3_auth_block = function_block(src, "async fn authorize_table_catalog_s3_actions");
        assert!(s3_auth_block.contains("table_catalog_request_principal(req).await?"));
        assert!(s3_auth_block.contains("ReqInfo"));
        let metadata_auth_block = function_block(src, "async fn read_authorized_table_metadata_json");
        assert!(metadata_auth_block.contains("S3Action::GetObjectAction"));
        assert!(metadata_auth_block.contains("authorize_table_warehouse_claim"));
        assert!(metadata_auth_block.contains("acquire_read_lock"));
        assert!(metadata_auth_block.contains("ensure_table_catalog_lock_held"));

        for handler in [
            "RestRegisterTableHandler",
            "ImportTableCatalogHandler",
            "SyncExternalCatalogBridgeHandler",
        ] {
            let block = operation_block(src, handler);
            assert!(block.contains("metadata_guard"), "{handler} should retain the authorized metadata lock");
            assert!(
                block.contains("ensure_table_catalog_lock_held"),
                "{handler} should recheck the metadata lock before publication"
            );
            assert!(
                block.contains("drop(metadata_guard)"),
                "{handler} should hold the metadata lock through catalog publication"
            );
        }

        for handler in ["PutTableRefHandler", "DeleteTableRefHandler"] {
            assert!(
                operation_block(src, handler).contains("Some(&mut req)"),
                "{handler} should preserve request context for object-level authorization"
            );
        }
        for helper in ["async fn put_table_ref_response", "async fn delete_table_ref_response"] {
            assert!(
                function_block(src, helper).contains("standard_commit_table_response_inner"),
                "{helper} should route ref commits through the authorized commit path"
            );
        }

        let sync_bridge_block = operation_block(src, "SyncExternalCatalogBridgeHandler");
        assert!(
            sync_bridge_block.contains("AdminAction::RegisterTableAction"),
            "external catalog sync should require register authorization before creating a missing table"
        );
        assert!(sync_bridge_block.contains("read_authorized_table_metadata_json"));
        assert!(
            sync_bridge_block.contains(".load_table(&warehouse, &namespace.public_name(), &table)"),
            "external catalog sync should load the current table snapshot after authorization"
        );
        let register_auth_position = sync_bridge_block
            .find("AdminAction::RegisterTableAction")
            .expect("external catalog sync should authorize registration");
        let bucket_write_position = sync_bridge_block
            .find("ensure_table_bucket_entry")
            .expect("external catalog sync should materialize the table bucket entry");
        assert!(
            register_auth_position < bucket_write_position,
            "external catalog sync must authorize registration before it can materialize catalog state"
        );
        assert!(
            sync_bridge_block.contains("sync_external_catalog_bridge_response_with_snapshot")
                && sync_bridge_block.contains("target_metadata")
                && sync_bridge_block.contains("current"),
            "external catalog sync should use the state and metadata snapshot authorized by the handler"
        );

        let import_block = operation_block(src, "ImportTableCatalogHandler");
        assert!(import_block.contains("read_authorized_table_metadata_json"));
        assert!(import_block.contains("catalog_import_response_with_metadata"));

        let commit_block = operation_block(src, "RestCommitTableHandler");
        assert!(commit_block.contains("authorize_table_catalog_s3_actions"));
        assert!(commit_block.contains("S3Action::GetObjectAction"));
        for helper in [
            "async fn read_snapshot_manifest_references",
            "async fn snapshot_manifest_locations",
            "async fn validate_manifest_data_file_references",
        ] {
            assert!(
                !function_block(src, helper).contains("authorize_optional_table_catalog_object_read"),
                "{helper} should validate the catalog-owned metadata graph without per-object IAM fan-out"
            );
        }

        let migration_block = operation_block(src, "GetTableCatalogMigrationHandler");
        assert!(
            migration_block.contains("TableCatalogResource::warehouse(&warehouse)"),
            "catalog migration dry-run should authorize against the warehouse resource"
        );
        for handler in [
            "MaterializeTableCatalogMigrationHandler",
            "CancelTableCatalogMigrationHandler",
        ] {
            let block = operation_block(src, handler);
            assert!(
                block.contains("authorize_table_catalog_request(&req, AdminAction::MigrateTableCatalogAction).await?;"),
                "{handler} should require the global catalog migration action"
            );
            assert!(
                !block.contains("authorize_table_catalog_resource_request("),
                "{handler} must not imply that a global backing cutover is warehouse-scoped"
            );
        }

        let rename_block = operation_block(src, "RestRenameTableHandler");
        assert!(rename_block.contains("TableCatalogResource::table(&warehouse, &source_namespace, source_table.as_str())"));
        assert!(
            rename_block.contains("TableCatalogResource::table(&warehouse, &destination_namespace, destination_table.as_str())")
        );
        assert!(rename_block.contains("read_bounded_json_body::<RenameTableRequest>"));
        assert!(rename_block.contains("RENAME_TABLE_BODY_MAX_SIZE"));
        assert!(rename_block.contains("RENAME_TABLE_BODY_TIMEOUT"));
        assert_eq!(
            rename_block.matches("table_catalog_request_principal(&req).await?;").count(),
            1,
            "table rename should authenticate the request once"
        );
        assert_eq!(
            rename_block
                .matches("authorize_table_catalog_resource_for_principal(")
                .count(),
            2,
            "table rename should authorize both the source and destination resources"
        );
        assert_eq!(rename_block.matches("AdminAction::SetTableAction").count(), 2);
        assert!(!rename_block.contains("authorize_table_catalog_request(&req,"));

        let namespace_properties_block = operation_block(src, "RestUpdateNamespacePropertiesHandler");
        assert!(namespace_properties_block.contains("read_bounded_json_body::<UpdateNamespacePropertiesRequest>"));
        assert!(namespace_properties_block.contains("NAMESPACE_PROPERTIES_BODY_MAX_SIZE"));
        assert!(namespace_properties_block.contains("NAMESPACE_PROPERTIES_BODY_TIMEOUT"));

        for (handler, action) in [
            ("RestLoadTableHandler", "AdminAction::GetTableMetadataAction"),
            ("RestTableExistsHandler", "AdminAction::GetTableAction"),
            ("RestLoadCredentialsHandler", "AdminAction::GetTableCredentialsAction"),
            ("RestCommitTableHandler", "AdminAction::CommitTableAction"),
            ("RestDropTableHandler", "AdminAction::DeleteTableAction"),
            ("ListTableRefsHandler", "AdminAction::GetTableMetadataAction"),
            ("GetTableMetadataLocationHandler", "AdminAction::GetTableMetadataLocationAction"),
            ("UpdateTableMetadataLocationHandler", "AdminAction::SetTableMetadataLocationAction"),
            ("RestTableMetadataMaintenanceHandler", "AdminAction::RunTableMaintenanceAction"),
            ("GetTableMaintenanceConfigHandler", "AdminAction::GetTableLifecycleAction"),
            ("PutTableMaintenanceConfigHandler", "AdminAction::SetTableLifecycleAction"),
            ("GetTableMaintenanceJobHandler", "AdminAction::GetTableLifecycleAction"),
            ("GetTableMaintenanceSchedulerHandler", "AdminAction::GetTableLifecycleAction"),
            ("RunTableMaintenanceSchedulerHandler", "AdminAction::RunTableMaintenanceAction"),
            ("TableMaintenanceQuarantineHandler", "AdminAction::RunTableMaintenanceAction"),
            ("ExportTableCatalogHandler", "AdminAction::GetTableMetadataAction"),
            ("ImportTableCatalogHandler", "AdminAction::RegisterTableAction"),
            ("ExternalCatalogBridgeHandler", "AdminAction::GetTableMetadataAction"),
            ("PutExternalCatalogBridgeHandler", "AdminAction::RegisterTableAction"),
            ("SyncExternalCatalogBridgeHandler", "AdminAction::SetTableMetadataLocationAction"),
            ("GetTableCatalogDiagnosticsHandler", "AdminAction::GetTableMetadataAction"),
            ("RecoverTableCatalogHandler", "AdminAction::CommitTableAction"),
            ("RollbackTableCatalogHandler", "AdminAction::CommitTableAction"),
        ] {
            let block = operation_block(src, handler);
            assert!(
                block.contains("TableCatalogResource::table(&warehouse, &namespace, &table)"),
                "{handler} should build a table-aware catalog resource"
            );
            assert!(
                block.contains(&format!("authorize_table_catalog_resource_request(&req, &resource, {action}).await?;")),
                "{handler} should authorize against the table-aware catalog resource"
            );
        }
    }

    #[test]
    fn table_catalog_list_handlers_parse_standard_pagination() {
        let src = include_str!("table_catalog.rs");
        for (handler, helper_call) in [
            (
                "RestListNamespacesHandler",
                "list_namespaces_response(&store, &warehouse, parent.as_ref(), &req.uri).await?",
            ),
            (
                "RestListTablesHandler",
                "list_tables_response(&store, &warehouse, &namespace, &req.uri).await?",
            ),
            (
                "RestListViewsHandler",
                "list_views_response(&store, &warehouse, &namespace, &req.uri).await?",
            ),
        ] {
            let block = operation_block(src, handler);
            assert!(
                block.contains(helper_call),
                "{handler} should pass the request URI to its paginated list helper"
            );
        }
    }

    #[test]
    fn table_catalog_handlers_require_enabled_table_bucket_marker_before_catalog_state() {
        let src = include_str!("table_catalog.rs");

        for handler in [
            "GetTableCatalogMigrationHandler",
            "MaterializeTableCatalogMigrationHandler",
            "CancelTableCatalogMigrationHandler",
            "RestListNamespacesHandler",
            "RestCreateNamespaceHandler",
            "RestGetNamespaceHandler",
            "RestNamespaceExistsHandler",
            "RestUpdateNamespacePropertiesHandler",
            "RestDropNamespaceHandler",
            "RestListTablesHandler",
            "RestRenameTableHandler",
            "RestCreateTableHandler",
            "RestRegisterTableHandler",
            "RestListViewsHandler",
            "RestCreateViewHandler",
            "RestLoadTableHandler",
            "RestTableExistsHandler",
            "RestLoadCredentialsHandler",
            "RestCommitTableHandler",
            "RestDropTableHandler",
            "RestLoadViewHandler",
            "RestViewExistsHandler",
            "RestReplaceViewHandler",
            "RestDropViewHandler",
            "ListTableRefsHandler",
            "PutTableRefHandler",
            "DeleteTableRefHandler",
            "GetTableMetadataLocationHandler",
            "UpdateTableMetadataLocationHandler",
            "RestTableMetadataMaintenanceHandler",
            "GetTableMaintenanceConfigHandler",
            "PutTableMaintenanceConfigHandler",
            "GetTableMaintenanceJobHandler",
            "GetTableMaintenanceSchedulerHandler",
            "RunTableMaintenanceSchedulerHandler",
            "RunTableMaintenanceWorkerHandler",
            "HeartbeatTableMaintenanceJobHandler",
            "TableMaintenanceQuarantineHandler",
            "ExportTableCatalogHandler",
            "ImportTableCatalogHandler",
            "ExternalCatalogBridgeHandler",
            "PutExternalCatalogBridgeHandler",
            "SyncExternalCatalogBridgeHandler",
            "GetTableCatalogDiagnosticsHandler",
            "RecoverTableCatalogHandler",
            "RollbackTableCatalogHandler",
        ] {
            let block = operation_block(src, handler);
            assert!(
                block.contains("ensure_table_bucket_enabled(&warehouse).await?;")
                    || block.contains("table_bucket_enabled_from_metadata(&warehouse).await?;"),
                "{handler} should require the table bucket metadata marker before catalog state access"
            );
        }
    }

    #[test]
    fn enable_table_bucket_response_writes_metadata_marker_before_catalog_entry() {
        let src = include_str!("table_catalog.rs");
        let block = function_block(src, "async fn enable_table_bucket_response");
        let marker_write = block
            .find("enable_table_bucket_marker(bucket).await?;")
            .expect("enable should write the metadata marker");
        let catalog_entry_write = block
            .find("ensure_table_bucket_entry(store, bucket, true).await?;")
            .expect("enable should write the catalog entry");

        assert!(
            marker_write < catalog_entry_write,
            "enable should write the metadata marker before the catalog entry"
        );
    }

    #[test]
    fn table_catalog_resource_builds_policy_object_scope() {
        let namespace = crate::table_catalog::Namespace::parse("analytics.daily_events").expect("namespace should parse");
        let table = crate::table_catalog::IdentifierSegment::parse("events").expect("table should parse");

        assert_eq!(TableCatalogResource::warehouse("warehouse-a").object_path(), None);
        assert_eq!(
            TableCatalogResource::namespace("warehouse-a", &namespace)
                .object_path()
                .as_deref(),
            Some("namespaces/analytics/daily_events")
        );
        assert_eq!(
            TableCatalogResource::table("warehouse-a", &namespace, table.as_str())
                .object_path()
                .as_deref(),
            Some("namespaces/analytics/daily_events/tables/events")
        );
    }

    fn operation_block<'a>(src: &'a str, handler: &str) -> &'a str {
        let marker = format!("impl Operation for {handler}");
        let block = src.split_once(&marker).expect("handler impl should exist").1;
        let end = block
            .find("\npub struct ")
            .or_else(|| block.find("\n#[cfg(test)]"))
            .unwrap_or(block.len());
        &block[..end]
    }

    fn function_block<'a>(src: &'a str, signature: &str) -> &'a str {
        let block = src.split_once(signature).expect("function should exist").1;
        let end = [block.find("\nfn "), block.find("\nasync fn ")]
            .into_iter()
            .flatten()
            .min()
            .unwrap_or(block.len());
        &block[..end]
    }

    #[test]
    fn rest_catalog_mvp_routes_use_implemented_handlers() {
        fn assert_operation<T: Operation>() {}

        let _: &EnableTableBucketHandler = &ENABLE_TABLE_BUCKET_HANDLER;
        let _: &GetTableBucketHandler = &GET_TABLE_BUCKET_HANDLER;
        let _: &GetTableCatalogMigrationHandler = &GET_TABLE_CATALOG_MIGRATION_HANDLER;
        let _: &MaterializeTableCatalogMigrationHandler = &MATERIALIZE_TABLE_CATALOG_MIGRATION_HANDLER;
        let _: &CancelTableCatalogMigrationHandler = &CANCEL_TABLE_CATALOG_MIGRATION_HANDLER;
        let _: &RestListNamespacesHandler = &LIST_NAMESPACES_HANDLER;
        let _: &RestCreateNamespaceHandler = &CREATE_NAMESPACE_HANDLER;
        let _: &RestGetNamespaceHandler = &GET_NAMESPACE_HANDLER;
        let _: &RestNamespaceExistsHandler = &NAMESPACE_EXISTS_HANDLER;
        let _: &RestUpdateNamespacePropertiesHandler = &UPDATE_NAMESPACE_PROPERTIES_HANDLER;
        let _: &RestDropNamespaceHandler = &DROP_NAMESPACE_HANDLER;
        let _: &RestListTablesHandler = &LIST_TABLES_HANDLER;
        let _: &RestRenameTableHandler = &RENAME_TABLE_HANDLER;
        let _: &RestCreateTableHandler = &CREATE_TABLE_HANDLER;
        let _: &RestRegisterTableHandler = &REGISTER_TABLE_HANDLER;
        let _: &RestListViewsHandler = &LIST_VIEWS_HANDLER;
        let _: &RestCreateViewHandler = &CREATE_VIEW_HANDLER;
        let _: &RestLoadTableHandler = &LOAD_TABLE_HANDLER;
        let _: &RestTableExistsHandler = &TABLE_EXISTS_HANDLER;
        let _: &RestLoadCredentialsHandler = &LOAD_CREDENTIALS_HANDLER;
        let _: &RestCommitTableHandler = &COMMIT_TABLE_HANDLER;
        let _: &RestDropTableHandler = &DROP_TABLE_HANDLER;
        let _: &RestLoadViewHandler = &LOAD_VIEW_HANDLER;
        let _: &RestReplaceViewHandler = &REPLACE_VIEW_HANDLER;
        let _: &RestDropViewHandler = &DROP_VIEW_HANDLER;
        let _: &ListTableRefsHandler = &LIST_TABLE_REFS_HANDLER;
        let _: &GetTableMetadataLocationHandler = &GET_TABLE_METADATA_LOCATION_HANDLER;
        let _: &UpdateTableMetadataLocationHandler = &UPDATE_TABLE_METADATA_LOCATION_HANDLER;
        let _: &RestTableMetadataMaintenanceHandler = &TABLE_METADATA_MAINTENANCE_HANDLER;
        let _: &GetTableMaintenanceConfigHandler = &GET_TABLE_MAINTENANCE_CONFIG_HANDLER;
        let _: &PutTableMaintenanceConfigHandler = &PUT_TABLE_MAINTENANCE_CONFIG_HANDLER;
        let _: &GetTableMaintenanceJobHandler = &GET_TABLE_MAINTENANCE_JOB_HANDLER;
        let _: &GetTableMaintenanceSchedulerHandler = &GET_TABLE_MAINTENANCE_SCHEDULER_HANDLER;
        let _: &RunTableMaintenanceSchedulerHandler = &RUN_TABLE_MAINTENANCE_SCHEDULER_HANDLER;
        let _: &TableMaintenanceQuarantineHandler = &TABLE_MAINTENANCE_QUARANTINE_HANDLER;
        let _: &ExportTableCatalogHandler = &EXPORT_TABLE_CATALOG_HANDLER;
        let _: &ImportTableCatalogHandler = &IMPORT_TABLE_CATALOG_HANDLER;
        let _: &ExternalCatalogBridgeHandler = &EXTERNAL_CATALOG_BRIDGE_HANDLER;
        let _: &PutExternalCatalogBridgeHandler = &PUT_EXTERNAL_CATALOG_BRIDGE_HANDLER;
        let _: &SyncExternalCatalogBridgeHandler = &SYNC_EXTERNAL_CATALOG_BRIDGE_HANDLER;
        let _: &GetTableCatalogDiagnosticsHandler = &GET_TABLE_CATALOG_DIAGNOSTICS_HANDLER;
        let _: &RecoverTableCatalogHandler = &RECOVER_TABLE_CATALOG_HANDLER;
        let _: &RollbackTableCatalogHandler = &ROLLBACK_TABLE_CATALOG_HANDLER;

        assert_operation::<EnableTableBucketHandler>();
        assert_operation::<GetTableBucketHandler>();
        assert_operation::<GetTableCatalogMigrationHandler>();
        assert_operation::<MaterializeTableCatalogMigrationHandler>();
        assert_operation::<CancelTableCatalogMigrationHandler>();
        assert_operation::<RestListNamespacesHandler>();
        assert_operation::<RestCreateNamespaceHandler>();
        assert_operation::<RestGetNamespaceHandler>();
        assert_operation::<RestNamespaceExistsHandler>();
        assert_operation::<RestUpdateNamespacePropertiesHandler>();
        assert_operation::<RestDropNamespaceHandler>();
        assert_operation::<RestListTablesHandler>();
        assert_operation::<RestRenameTableHandler>();
        assert_operation::<RestCreateTableHandler>();
        assert_operation::<RestRegisterTableHandler>();
        assert_operation::<RestListViewsHandler>();
        assert_operation::<RestCreateViewHandler>();
        assert_operation::<RestLoadTableHandler>();
        assert_operation::<RestTableExistsHandler>();
        assert_operation::<RestLoadCredentialsHandler>();
        assert_operation::<RestCommitTableHandler>();
        assert_operation::<RestDropTableHandler>();
        assert_operation::<RestLoadViewHandler>();
        assert_operation::<RestReplaceViewHandler>();
        assert_operation::<RestDropViewHandler>();
        assert_operation::<ListTableRefsHandler>();
        assert_operation::<GetTableMetadataLocationHandler>();
        assert_operation::<UpdateTableMetadataLocationHandler>();
        assert_operation::<RestTableMetadataMaintenanceHandler>();
        assert_operation::<GetTableMaintenanceConfigHandler>();
        assert_operation::<PutTableMaintenanceConfigHandler>();
        assert_operation::<GetTableMaintenanceJobHandler>();
        assert_operation::<GetTableMaintenanceSchedulerHandler>();
        assert_operation::<RunTableMaintenanceSchedulerHandler>();
        assert_operation::<RunTableMaintenanceWorkerHandler>();
        assert_operation::<HeartbeatTableMaintenanceJobHandler>();
        assert_operation::<TableMaintenanceQuarantineHandler>();
        assert_operation::<ExportTableCatalogHandler>();
        assert_operation::<ImportTableCatalogHandler>();
        assert_operation::<ExternalCatalogBridgeHandler>();
        assert_operation::<PutExternalCatalogBridgeHandler>();
        assert_operation::<SyncExternalCatalogBridgeHandler>();
        assert_operation::<GetTableCatalogDiagnosticsHandler>();
        assert_operation::<RecoverTableCatalogHandler>();
        assert_operation::<RollbackTableCatalogHandler>();
    }

    #[test]
    fn table_metadata_maintenance_request_uses_conservative_defaults() {
        let request: TableMetadataMaintenanceRequest =
            serde_json::from_value(serde_json::json!({})).expect("default maintenance request should parse");

        assert_eq!(request.retain_recent_metadata_files, 0);
        assert!(!request.delete);
        assert!(request.snapshot_expiration.is_none());
        assert!(!request.commit_snapshot_expiration);
        assert!(request.compaction.is_none());
        assert!(!request.commit_compaction);
    }

    #[test]
    fn table_metadata_maintenance_request_accepts_delete_mode() {
        let request: TableMetadataMaintenanceRequest = serde_json::from_value(serde_json::json!({
            "retain-recent-metadata-files": 2,
            "delete": true
        }))
        .expect("metadata maintenance request should parse");

        assert_eq!(request.retain_recent_metadata_files, 2);
        assert!(request.delete);
        assert!(request.snapshot_expiration.is_none());
        assert!(!request.commit_snapshot_expiration);
        assert!(request.compaction.is_none());
        assert!(!request.commit_compaction);
    }

    #[test]
    fn table_metadata_maintenance_request_accepts_snapshot_and_compaction_plans() {
        let request: TableMetadataMaintenanceRequest = serde_json::from_value(serde_json::json!({
            "commit-snapshot-expiration": true,
            "snapshot-expiration": {
                "min-snapshots-to-keep": 2,
                "max-snapshot-age-ms": 3600000
            },
            "commit-compaction": true,
            "compaction": {
                "target-file-size-bytes": 536870912,
                "small-file-threshold-bytes": 67108864,
                "min-input-files": 5,
                "max-rewrite-bytes-per-job": 10737418240u64
            }
        }))
        .expect("metadata maintenance request should parse maintenance planning config");

        let snapshot_expiration = request
            .snapshot_expiration
            .expect("snapshot expiration config should be present");
        assert_eq!(snapshot_expiration.min_snapshots_to_keep, 2);
        assert_eq!(snapshot_expiration.max_snapshot_age_ms, 3_600_000);
        assert!(request.commit_snapshot_expiration);
        assert!(request.commit_compaction);
        let compaction = request.compaction.expect("compaction config should be present");
        assert_eq!(compaction.target_file_size_bytes, 536_870_912);
        assert_eq!(compaction.small_file_threshold_bytes, 67_108_864);
        assert_eq!(compaction.min_input_files, 5);
        assert_eq!(compaction.max_rewrite_bytes_per_job, 10_737_418_240);
    }

    #[test]
    fn table_maintenance_scheduler_run_request_uses_stable_default_scheduler_id() {
        let request: TableMaintenanceSchedulerRunRequest =
            serde_json::from_value(serde_json::json!({})).expect("scheduler run request should parse");

        assert_eq!(request.scheduler_id(), "rustfs-maintenance-scheduler");
    }

    #[tokio::test]
    async fn table_maintenance_scheduler_run_request_empty_body_uses_default_scheduler_id() {
        let request: TableMaintenanceSchedulerRunRequest = read_json_body_or_default(Body::empty())
            .await
            .expect("bodyless scheduler run should use the default scheduler id");

        assert_eq!(request.scheduler_id(), "rustfs-maintenance-scheduler");
    }

    #[test]
    fn table_maintenance_scheduler_run_request_accepts_scheduler_id() {
        let request: TableMaintenanceSchedulerRunRequest = serde_json::from_value(serde_json::json!({
            "scheduler-id": "scheduler-a"
        }))
        .expect("scheduler run request should parse scheduler id");

        assert_eq!(request.scheduler_id(), "scheduler-a");
    }

    #[test]
    fn table_maintenance_worker_run_request_uses_stable_default_worker_id() {
        let request: TableMaintenanceWorkerRunRequest =
            serde_json::from_value(serde_json::json!({})).expect("worker run request should parse");

        assert_eq!(request.worker_id(), "rustfs-maintenance-worker");
    }

    #[test]
    fn table_maintenance_worker_run_request_accepts_worker_id() {
        let request: TableMaintenanceWorkerRunRequest = serde_json::from_value(serde_json::json!({
            "worker-id": "worker-a"
        }))
        .expect("worker run request should parse worker id");

        assert_eq!(request.worker_id(), "worker-a");
    }

    #[test]
    fn table_maintenance_heartbeat_request_requires_lease_id() {
        let err = serde_json::from_value::<TableMaintenanceHeartbeatRequest>(serde_json::json!({
            "worker-id": "worker-a"
        }))
        .expect_err("heartbeat request should require lease id");

        assert!(err.to_string().contains("lease-id"));
    }

    #[tokio::test]
    async fn table_bucket_response_reports_catalog_discovery_without_credentials() {
        let store = TestTableCatalogStore::default();
        ensure_table_bucket_entry(&store, "warehouse", true)
            .await
            .expect("table bucket entry should be seeded");

        let response = table_bucket_response(&store, "warehouse", true)
            .await
            .expect("bucket response should build");

        assert_eq!(response.table_bucket, "warehouse");
        assert!(response.enabled);
        assert_eq!(response.catalog_type, crate::table_catalog::TABLE_BUCKET_CATALOG_TYPE);
        assert_eq!(response.warehouse_location, "s3://warehouse/");
        assert_eq!(response.catalog_uri, "/iceberg/v1/warehouse");
        assert_eq!(response.compat_catalog_uri, "/_iceberg/v1/warehouse");
        assert_eq!(response.credential_vending, CREDENTIAL_VENDING_UNSUPPORTED);
        assert_eq!(response.credential_scope, "warehouse-prefix");
        assert_eq!(response.credential_scope_prefix, "s3://warehouse/");
        assert!(response.catalog_entry_present);
    }

    #[test]
    fn table_catalog_ingress_requests_reject_unknown_fields() {
        assert_rejects_unknown_field::<CreateNamespaceRequest>(
            "CreateNamespaceRequest",
            serde_json::json!({
                "namespace": ["analytics"],
                "unexpected": true
            }),
        );
        assert_rejects_unknown_field::<RegisterTableRequest>(
            "RegisterTableRequest",
            serde_json::json!({
                "name": "events",
                "metadata-location": ".rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00001.metadata.json",
                "unexpected": true
            }),
        );
        assert_rejects_unknown_field::<CreateTableRequest>(
            "CreateTableRequest",
            serde_json::json!({
                "name": "events",
                "schema": {},
                "unexpected": true
            }),
        );
        assert_rejects_unknown_field::<RestCommitTableRequest>(
            "RestCommitTableRequest",
            serde_json::json!({
                "unexpected": true
            }),
        );
        assert_rejects_unknown_field::<TableMetadataMaintenanceRequest>(
            "TableMetadataMaintenanceRequest",
            serde_json::json!({
                "delete": true,
                "unexpected": true
            }),
        );
        assert_rejects_unknown_field::<UpdateTableMetadataLocationRequest>(
            "UpdateTableMetadataLocationRequest",
            serde_json::json!({
                "metadata-location": ".rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00002.metadata.json",
                "version-token": "token-v1",
                "unexpected": true
            }),
        );
        assert_rejects_unknown_field::<CatalogImportRequest>(
            "CatalogImportRequest",
            serde_json::json!({
                "metadata-location": ".rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00001.metadata.json",
                "unexpected": true
            }),
        );
        assert_rejects_unknown_field::<ExternalCatalogBridgeRequest>(
            "ExternalCatalogBridgeRequest",
            serde_json::json!({
                "catalog": "polaris",
                "external-namespace": "analytics",
                "external-table": "events",
                "unexpected": true
            }),
        );
        assert_rejects_unknown_field::<ExternalCatalogBridgeSyncRequest>(
            "ExternalCatalogBridgeSyncRequest",
            serde_json::json!({
                "catalog": "glue",
                "external-namespace": "analytics",
                "external-table": "events",
                "metadata-location": ".rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00001.metadata.json",
                "unexpected": true
            }),
        );
        assert_rejects_unknown_field::<RollbackTableRequest>(
            "RollbackTableRequest",
            serde_json::json!({
                "metadata-location": ".rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00001.metadata.json",
                "version-token": "token-v2",
                "unexpected": true
            }),
        );
        assert_rejects_unknown_field::<crate::table_catalog::TableMaintenanceConfig>(
            "TableMaintenanceConfig",
            serde_json::json!({
                "version": 1,
                "unexpected": true
            }),
        );
    }

    fn assert_rejects_unknown_field<T>(target: &str, value: serde_json::Value)
    where
        T: serde::de::DeserializeOwned,
    {
        let err = match serde_json::from_value::<T>(value) {
            Ok(_) => panic!("{target} should reject unknown fields"),
            Err(err) => err,
        };
        assert!(
            err.to_string().contains("unknown field"),
            "{target} should reject unknown fields, got: {err}"
        );
    }

    #[test]
    fn create_namespace_request_uses_rest_namespace_segments_and_properties() {
        let request: CreateNamespaceRequest = serde_json::from_value(serde_json::json!({
            "namespace": ["analytics", "daily_events"],
            "properties": {
                "owner": "lakehouse"
            }
        }))
        .expect("request should parse");
        let namespace = namespace_from_segments(request.namespace.clone()).expect("namespace should be valid");
        let response = namespace_response_from_entry(crate::table_catalog::NamespaceEntry {
            version: crate::table_catalog::TABLE_CATALOG_ENTRY_VERSION,
            table_bucket: "warehouse".to_string(),
            namespace: namespace.public_name(),
            namespace_id: namespace.storage_id(),
            state: crate::table_catalog::TableCatalogEntryState::Active,
            properties: request.properties,
            created_at: None,
            updated_at: None,
        })
        .expect("namespace response should build");

        assert_eq!(namespace.public_name(), "analytics.daily_events");
        assert_eq!(response.namespace, vec!["analytics".to_string(), "daily_events".to_string()]);
        assert_eq!(response.properties.get("owner").map(String::as_str), Some("lakehouse"));
        assert!(namespace_from_segments(vec!["analytics.daily_events".to_string()]).is_err());
    }

    #[test]
    fn namespace_property_update_uses_standard_shape_and_rejects_overlap() {
        let request: UpdateNamespacePropertiesRequest = serde_json::from_value(serde_json::json!({
            "removals": ["retention"],
            "updates": {
                "owner": "platform"
            }
        }))
        .expect("namespace property update should parse");
        let update = namespace_properties_update_from_request(request).expect("disjoint property update should validate");
        let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
        let mut entry = crate::table_catalog::NamespaceEntry {
            version: crate::table_catalog::TABLE_CATALOG_ENTRY_VERSION,
            table_bucket: "warehouse".to_string(),
            namespace: namespace.public_name(),
            namespace_id: namespace.storage_id(),
            state: crate::table_catalog::TableCatalogEntryState::Active,
            properties: BTreeMap::from([("retention".to_string(), "30d".to_string())]),
            created_at: None,
            updated_at: None,
        };
        let result = update.apply_to(&mut entry);
        assert_eq!(result.removed, vec!["retention".to_string()]);
        assert_eq!(result.updated, vec!["owner".to_string()]);
        assert_eq!(entry.properties.get("owner").map(String::as_str), Some("platform"));

        let overlap: UpdateNamespacePropertiesRequest = serde_json::from_value(serde_json::json!({
            "removals": ["owner"],
            "updates": {
                "owner": "platform"
            }
        }))
        .expect("overlapping request should parse before semantic validation");
        let error = namespace_properties_update_from_request(overlap).expect_err("overlapping property update should fail");
        assert_eq!(error.code(), &S3ErrorCode::Custom(ICEBERG_ERROR_UNPROCESSABLE_ENTITY.into()));
        assert_eq!(error.status_code(), Some(StatusCode::UNPROCESSABLE_ENTITY));

        assert_rejects_unknown_field::<UpdateNamespacePropertiesRequest>(
            "namespace property update request",
            serde_json::json!({"updates": {}, "unknown": true}),
        );
    }

    #[test]
    fn rename_table_request_uses_standard_identifiers_and_strict_serde() {
        let request: RenameTableRequest = serde_json::from_value(serde_json::json!({
            "source": {
                "namespace": ["sales"],
                "name": "orders"
            },
            "destination": {
                "namespace": ["curated"],
                "name": "orders_v2"
            }
        }))
        .expect("rename request should parse");
        let (source_namespace, source_table) = table_identifier_from_request(request.source).expect("source should validate");
        let (destination_namespace, destination_table) =
            table_identifier_from_request(request.destination).expect("destination should validate");
        assert_eq!(source_namespace.public_name(), "sales");
        assert_eq!(source_table.as_str(), "orders");
        assert_eq!(destination_namespace.public_name(), "curated");
        assert_eq!(destination_table.as_str(), "orders_v2");

        assert_rejects_unknown_field::<RenameTableRequest>(
            "rename table request",
            serde_json::json!({
                "source": {"namespace": ["sales"], "name": "orders"},
                "destination": {"namespace": ["curated"], "name": "orders_v2"},
                "unknown": true
            }),
        );
        assert_rejects_unknown_field::<RenameTableRequest>(
            "rename table source identifier",
            serde_json::json!({
                "source": {"namespace": ["sales"], "name": "orders", "unknown": true},
                "destination": {"namespace": ["curated"], "name": "orders_v2"}
            }),
        );
    }

    #[tokio::test]
    async fn rename_table_body_rejects_declared_and_streamed_oversize_payloads() {
        let error = read_bounded_json_body::<RenameTableRequest>(
            &HeaderMap::new(),
            Body::from(vec![b'x'; RENAME_TABLE_BODY_MAX_SIZE + 1]),
            RENAME_TABLE_BODY_MAX_SIZE,
            RENAME_TABLE_BODY_TIMEOUT,
            "rename table",
        )
        .await
        .expect_err("oversized streamed body should fail");
        assert_eq!(error.code(), &S3ErrorCode::InvalidRequest);

        let body = serde_json::to_vec(&serde_json::json!({
            "source": {
                "namespace": ["sales"],
                "name": "orders"
            },
            "destination": {
                "namespace": ["curated"],
                "name": "orders_v2"
            }
        }))
        .expect("rename request should serialize");
        let request = read_bounded_json_body::<RenameTableRequest>(
            &HeaderMap::new(),
            Body::from(body),
            RENAME_TABLE_BODY_MAX_SIZE,
            RENAME_TABLE_BODY_TIMEOUT,
            "rename table",
        )
        .await
        .expect("bounded rename request should parse");
        assert_eq!(request.source.name, "orders");
        assert_eq!(request.destination.name, "orders_v2");

        let mut exact_limit_body = b"{}".to_vec();
        exact_limit_body.resize(RENAME_TABLE_BODY_MAX_SIZE, b' ');
        let mut headers = HeaderMap::new();
        headers.insert(
            http::header::CONTENT_LENGTH,
            HeaderValue::from_str(&RENAME_TABLE_BODY_MAX_SIZE.to_string()).expect("content length should parse"),
        );
        let value = read_bounded_json_body::<serde_json::Value>(
            &headers,
            Body::from(exact_limit_body),
            RENAME_TABLE_BODY_MAX_SIZE,
            RENAME_TABLE_BODY_TIMEOUT,
            "rename table",
        )
        .await
        .expect("a valid body exactly at the limit should parse");
        assert!(value.is_object());
    }

    #[tokio::test]
    async fn bounded_json_body_times_out_stalled_namespace_property_streams() {
        let stream = futures::stream::pending::<Result<http_body::Frame<Bytes>, std::io::Error>>();
        let error = read_bounded_json_body::<UpdateNamespacePropertiesRequest>(
            &HeaderMap::new(),
            Body::http_body(http_body_util::StreamBody::new(stream)),
            NAMESPACE_PROPERTIES_BODY_MAX_SIZE,
            StdDuration::ZERO,
            "namespace properties",
        )
        .await
        .expect_err("stalled namespace property request should time out");

        assert_eq!(error.code(), &S3ErrorCode::InvalidRequest);
    }

    #[tokio::test(start_paused = true)]
    async fn generic_json_body_readers_time_out_stalled_streams() {
        let required_stream = futures::stream::pending::<Result<http_body::Frame<Bytes>, std::io::Error>>();
        let required = tokio::spawn(read_json_body::<serde_json::Value>(Body::http_body(http_body_util::StreamBody::new(
            required_stream,
        ))));
        let optional_stream = futures::stream::pending::<Result<http_body::Frame<Bytes>, std::io::Error>>();
        let optional = tokio::spawn(read_json_body_or_default::<serde_json::Value>(Body::http_body(
            http_body_util::StreamBody::new(optional_stream),
        )));
        tokio::task::yield_now().await;
        tokio::time::advance(TABLE_CATALOG_REQUEST_BODY_TIMEOUT).await;

        let required_error = required
            .await
            .expect("required body task should complete")
            .expect_err("stalled required body should time out");
        let optional_error = optional
            .await
            .expect("optional body task should complete")
            .expect_err("stalled optional body should time out");

        assert_eq!(required_error.code(), &S3ErrorCode::InvalidRequest);
        assert_eq!(optional_error.code(), &S3ErrorCode::InvalidRequest);
    }

    #[tokio::test]
    async fn bounded_json_body_rejects_oversized_content_length_without_polling_the_body() {
        let mut headers = HeaderMap::new();
        headers.insert(
            http::header::CONTENT_LENGTH,
            HeaderValue::from_str(&(RENAME_TABLE_BODY_MAX_SIZE + 1).to_string()).expect("content length should parse"),
        );
        let stream = futures::stream::poll_fn(|_| -> std::task::Poll<Option<Result<http_body::Frame<Bytes>, std::io::Error>>> {
            panic!("an oversized declared body must not be polled")
        });

        let error = read_bounded_json_body::<RenameTableRequest>(
            &headers,
            Body::http_body(http_body_util::StreamBody::new(stream)),
            RENAME_TABLE_BODY_MAX_SIZE,
            RENAME_TABLE_BODY_TIMEOUT,
            "rename table",
        )
        .await
        .expect_err("an oversized declared body should fail before body polling");

        assert_eq!(error.code(), &S3ErrorCode::InvalidRequest);
    }

    #[test]
    fn list_tables_response_uses_rest_identifier_shape() {
        let namespace = crate::table_catalog::Namespace::parse("analytics.daily_events").expect("namespace should parse");
        let response = list_tables_response_from_entries(
            vec![crate::table_catalog::TableEntry {
                version: crate::table_catalog::TABLE_CATALOG_ENTRY_VERSION,
                table_bucket: "warehouse".to_string(),
                namespace: namespace.public_name(),
                table: "events".to_string(),
                table_id: "table-id".to_string(),
                table_uuid: "table-uuid".to_string(),
                format: "ICEBERG".to_string(),
                format_version: 2,
                warehouse_location: "s3://warehouse/tables/table-id".to_string(),
                metadata_location:
                    ".rustfs-table/warehouses/default/namespaces/analytics/daily_events/tables/events/metadata/00001.metadata.json"
                        .to_string(),
                version_token: "token-v1".to_string(),
                generation: 1,
                state: crate::table_catalog::TableCatalogEntryState::Active,
                properties: BTreeMap::new(),
                created_at: None,
                updated_at: None,
            }],
            None,
        )
        .expect("table list response should build");

        assert_eq!(
            response.identifiers[0].namespace,
            vec!["analytics".to_string(), "daily_events".to_string()]
        );
        assert_eq!(response.identifiers[0].name, "events");
        assert!(response.next_page_token.is_none());
    }

    #[test]
    fn rest_pagination_round_trips_context_bound_tokens() {
        let context = RestPageContext {
            resource: TABLE_CATALOG_TABLE_RESOURCE_ROOT,
            warehouse: "warehouse",
            namespace: Some("analytics"),
        };
        let first_request = "/?pageSize=2".parse::<http::Uri>().expect("first page URI should parse");
        let first_pagination = rest_pagination_from_query(&first_request, context).expect("pageSize should start pagination");
        let (cursor, limit) = first_pagination.page_request().expect("pageSize should enable pagination");
        assert_eq!(cursor, None);
        assert_eq!(limit.get(), 2);
        let next_page_token = first_pagination
            .next_page_token(Some("strong:beta".to_string()))
            .expect("page token should encode")
            .expect("page token should be present");
        let second_request = format!("/?pageSize=2&pageToken={next_page_token}")
            .parse::<http::Uri>()
            .expect("second page URI should parse");
        let second_pagination = rest_pagination_from_query(&second_request, context).expect("continuation token should decode");
        let (cursor, limit) = second_pagination
            .page_request()
            .expect("continuation should remain paginated");
        assert_eq!(cursor, Some("strong:beta"));
        assert_eq!(limit.get(), 2);
        assert!(
            second_pagination
                .next_page_token(None)
                .expect("terminal token should build")
                .is_none()
        );

        let default_size_request = format!("/?pageToken={next_page_token}")
            .parse::<http::Uri>()
            .expect("continuation URI should parse without pageSize");
        let default_size_pagination =
            rest_pagination_from_query(&default_size_request, context).expect("continuation should use the default page size");
        let (cursor, limit) = default_size_pagination
            .page_request()
            .expect("continuation should remain paginated");
        assert_eq!(cursor, Some("strong:beta"));
        assert_eq!(limit.get(), REST_DEFAULT_PAGE_SIZE);

        for other_context in [
            RestPageContext {
                resource: TABLE_CATALOG_VIEW_RESOURCE_ROOT,
                warehouse: "warehouse",
                namespace: Some("analytics"),
            },
            RestPageContext {
                resource: TABLE_CATALOG_TABLE_RESOURCE_ROOT,
                warehouse: "other-warehouse",
                namespace: Some("analytics"),
            },
            RestPageContext {
                resource: TABLE_CATALOG_TABLE_RESOURCE_ROOT,
                warehouse: "warehouse",
                namespace: Some("other-namespace"),
            },
        ] {
            let expected_context = rest_page_context_fingerprint(other_context);
            let error = decode_rest_page_token(&next_page_token, &expected_context).expect_err("cross-context token should fail");
            assert_eq!(error.code(), &S3ErrorCode::Custom(ICEBERG_ERROR_BAD_REQUEST.into()));
            assert_eq!(error.status_code(), Some(StatusCode::BAD_REQUEST));
        }
    }

    #[test]
    fn rest_pagination_rejects_invalid_query_parameters() {
        let context = RestPageContext {
            resource: TABLE_CATALOG_NAMESPACE_RESOURCE_ROOT,
            warehouse: "warehouse",
            namespace: None,
        };
        for uri in [
            "/?pageSize=0",
            "/?pageSize=one",
            "/?pageSize=1&pageSize=2",
            "/?pageToken=first&pageToken=second",
        ] {
            let uri = uri.parse::<http::Uri>().expect("invalid pagination URI should still parse");
            let error = rest_pagination_from_query(&uri, context).expect_err("invalid pagination query should fail");
            assert_eq!(error.code(), &S3ErrorCode::Custom(ICEBERG_ERROR_BAD_REQUEST.into()), "{uri}");
            assert_eq!(error.status_code(), Some(StatusCode::BAD_REQUEST), "{uri}");
        }

        let oversized_token = "a".repeat(REST_PAGE_TOKEN_MAX_LENGTH + 1);
        let oversized_uri = format!("/?pageToken={oversized_token}")
            .parse::<http::Uri>()
            .expect("oversized token URI should parse");
        let error = rest_pagination_from_query(&oversized_uri, context).expect_err("oversized token should fail");
        assert_eq!(error.code(), &S3ErrorCode::Custom(ICEBERG_ERROR_BAD_REQUEST.into()));
        assert_eq!(error.status_code(), Some(StatusCode::BAD_REQUEST));

        let empty_token = "/?pageToken=".parse::<http::Uri>().expect("empty token URI should parse");
        let pagination = rest_pagination_from_query(&empty_token, context).expect("empty token should start the first page");
        let (cursor, limit) = pagination.page_request().expect("empty token should enable pagination");
        assert_eq!(cursor, None);
        assert_eq!(limit.get(), REST_DEFAULT_PAGE_SIZE);

        let capped_size = "/?pageSize=5000"
            .parse::<http::Uri>()
            .expect("large page size URI should parse");
        let pagination = rest_pagination_from_query(&capped_size, context).expect("large page size should be capped");
        let (cursor, limit) = pagination.page_request().expect("pageSize alone should enable pagination");
        assert_eq!(cursor, None);
        assert_eq!(limit.get(), REST_MAX_PAGE_SIZE);

        let unpaginated = rest_pagination_from_query(&"/".parse().expect("URI should parse"), context)
            .expect("request without pagination should parse");
        assert!(unpaginated.page_request().is_none());
    }

    #[test]
    fn rest_pagination_rejects_malformed_token_payloads() {
        let context = RestPageContext {
            resource: TABLE_CATALOG_NAMESPACE_RESOURCE_ROOT,
            warehouse: "warehouse",
            namespace: None,
        };
        let context_fingerprint = rest_page_context_fingerprint(context);
        let encoded_json = |value: serde_json::Value| {
            base64_encode_url_safe_no_pad(&serde_json::to_vec(&value).expect("test token should encode"))
        };
        let malformed_tokens = [
            "*".to_string(),
            base64_encode_url_safe_no_pad(b"not-json"),
            encoded_json(serde_json::json!({
                "version": REST_PAGE_TOKEN_VERSION,
                "context": context_fingerprint,
                "cursor": "strong:alpha",
                "unknown": true
            })),
            encoded_json(serde_json::json!({
                "version": REST_PAGE_TOKEN_VERSION + 1,
                "context": context_fingerprint,
                "cursor": "strong:alpha"
            })),
            encoded_json(serde_json::json!({
                "version": REST_PAGE_TOKEN_VERSION,
                "context": context_fingerprint,
                "cursor": ""
            })),
        ];

        for token in malformed_tokens {
            let uri = format!(
                "/?pageToken={}",
                url::form_urlencoded::byte_serialize(token.as_bytes()).collect::<String>()
            )
            .parse::<http::Uri>()
            .expect("malformed token URI should parse");
            let error = rest_pagination_from_query(&uri, context).expect_err("malformed token should fail");
            assert_eq!(error.code(), &S3ErrorCode::Custom(ICEBERG_ERROR_BAD_REQUEST.into()));
            assert_eq!(error.status_code(), Some(StatusCode::BAD_REQUEST));
        }
    }

    #[tokio::test]
    async fn rest_list_pagination_covers_namespaces_tables_and_views() {
        let store = TestTableCatalogStore::default();
        let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
        for name in ["beta", "alpha"] {
            store.namespaces.lock().await.push(crate::table_catalog::NamespaceEntry {
                version: crate::table_catalog::TABLE_CATALOG_ENTRY_VERSION,
                table_bucket: "warehouse".to_string(),
                namespace: name.to_string(),
                namespace_id: name.to_string(),
                state: crate::table_catalog::TableCatalogEntryState::Active,
                properties: BTreeMap::new(),
                created_at: None,
                updated_at: None,
            });
            store.tables.lock().await.push(crate::table_catalog::TableEntry {
                version: crate::table_catalog::TABLE_CATALOG_ENTRY_VERSION,
                table_bucket: "warehouse".to_string(),
                namespace: namespace.public_name(),
                table: name.to_string(),
                table_id: format!("table-{name}"),
                table_uuid: format!("table-uuid-{name}"),
                format: "ICEBERG".to_string(),
                format_version: 2,
                warehouse_location: format!("s3://warehouse/tables/table-{name}"),
                metadata_location: format!("s3://warehouse/tables/table-{name}/metadata/00001.metadata.json"),
                version_token: "token-v1".to_string(),
                generation: 1,
                state: crate::table_catalog::TableCatalogEntryState::Active,
                properties: BTreeMap::new(),
                created_at: None,
                updated_at: None,
            });
            store.views.lock().await.push(crate::table_catalog::ViewEntry {
                version: crate::table_catalog::TABLE_CATALOG_ENTRY_VERSION,
                table_bucket: "warehouse".to_string(),
                namespace: namespace.public_name(),
                view: name.to_string(),
                view_id: format!("view-{name}"),
                view_uuid: format!("view-uuid-{name}"),
                format: "ICEBERG_VIEW".to_string(),
                format_version: 1,
                warehouse_location: format!("s3://warehouse/views/view-{name}"),
                metadata_location: format!("s3://warehouse/views/view-{name}/metadata/00001.view.json"),
                version_token: "token-v1".to_string(),
                generation: 1,
                state: crate::table_catalog::TableCatalogEntryState::Active,
                properties: BTreeMap::new(),
                created_at: None,
                updated_at: None,
            });
        }

        let first_uri = "/?pageSize=1".parse::<http::Uri>().expect("first page URI should parse");
        let namespaces = list_namespaces_response(&store, "warehouse", None, &first_uri)
            .await
            .expect("namespace first page should load");
        assert_eq!(namespaces.namespaces, vec![vec!["alpha".to_string()]]);
        let namespace_token = namespaces.next_page_token.expect("namespace continuation should exist");
        let namespace_uri = format!("/?pageSize=1&pageToken={namespace_token}")
            .parse::<http::Uri>()
            .expect("namespace continuation URI should parse");
        let namespaces = list_namespaces_response(&store, "warehouse", None, &namespace_uri)
            .await
            .expect("namespace second page should load");
        assert_eq!(namespaces.namespaces, vec![vec!["beta".to_string()]]);
        assert!(namespaces.next_page_token.is_none());

        store.namespaces.lock().await.push(crate::table_catalog::NamespaceEntry {
            version: crate::table_catalog::TABLE_CATALOG_ENTRY_VERSION,
            table_bucket: "warehouse".to_string(),
            namespace: namespace.public_name(),
            namespace_id: "analytics".to_string(),
            state: crate::table_catalog::TableCatalogEntryState::Active,
            properties: BTreeMap::new(),
            created_at: None,
            updated_at: None,
        });

        let tables = list_tables_response(&store, "warehouse", &namespace, &first_uri)
            .await
            .expect("table first page should load");
        assert_eq!(tables.identifiers.len(), 1);
        assert_eq!(tables.identifiers[0].name, "alpha");
        let table_token = tables.next_page_token.expect("table continuation should exist");
        let table_uri = format!("/?pageSize=1&pageToken={table_token}")
            .parse::<http::Uri>()
            .expect("table continuation URI should parse");
        let tables = list_tables_response(&store, "warehouse", &namespace, &table_uri)
            .await
            .expect("table second page should load");
        assert_eq!(tables.identifiers.len(), 1);
        assert_eq!(tables.identifiers[0].name, "beta");
        assert!(tables.next_page_token.is_none());

        let views = list_views_response(&store, "warehouse", &namespace, &first_uri)
            .await
            .expect("view first page should load");
        assert_eq!(views.identifiers.len(), 1);
        assert_eq!(views.identifiers[0].name, "alpha");
        let view_token = views.next_page_token.expect("view continuation should exist");
        let view_uri = format!("/?pageSize=1&pageToken={view_token}")
            .parse::<http::Uri>()
            .expect("view continuation URI should parse");
        let views = list_views_response(&store, "warehouse", &namespace, &view_uri)
            .await
            .expect("view second page should load");
        assert_eq!(views.identifiers.len(), 1);
        assert_eq!(views.identifiers[0].name, "beta");
        assert!(views.next_page_token.is_none());

        for uri in ["/", "/?pageSize=2"] {
            let uri = uri.parse::<http::Uri>().expect("list URI should parse");
            let tables = list_tables_response(&store, "warehouse", &namespace, &uri)
                .await
                .expect("table exact page should load");
            let views = list_views_response(&store, "warehouse", &namespace, &uri)
                .await
                .expect("view exact page should load");
            assert_eq!(tables.identifiers.len(), 2);
            assert!(tables.next_page_token.is_none());
            assert_eq!(views.identifiers.len(), 2);
            assert!(views.next_page_token.is_none());
        }
    }

    #[test]
    fn list_responses_expose_null_next_page_token_at_end() {
        for value in [
            serde_json::to_value(RestListNamespacesResponse {
                namespaces: vec![vec!["analytics".to_string()]],
                next_page_token: None,
            })
            .expect("namespace response should serialize"),
            serde_json::to_value(RestListTablesResponse {
                identifiers: Vec::new(),
                next_page_token: None,
            })
            .expect("table response should serialize"),
            serde_json::to_value(RestListViewsResponse {
                identifiers: Vec::new(),
                next_page_token: None,
            })
            .expect("view response should serialize"),
        ] {
            assert!(value.get("next-page-token").is_some_and(serde_json::Value::is_null));
        }
    }

    #[tokio::test]
    async fn namespace_exists_status_uses_head_rest_semantics() {
        let store = TestTableCatalogStore::default();
        let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");

        assert_eq!(
            namespace_exists_status(&store, "warehouse", &namespace)
                .await
                .expect("missing namespace check should succeed"),
            StatusCode::NOT_FOUND
        );

        create_namespace_response(
            &store,
            "warehouse",
            CreateNamespaceRequest {
                namespace: vec!["analytics".to_string()],
                properties: BTreeMap::new(),
            },
            true,
        )
        .await
        .expect("namespace should be created");

        assert_eq!(
            namespace_exists_status(&store, "warehouse", &namespace)
                .await
                .expect("existing namespace check should succeed"),
            StatusCode::NO_CONTENT
        );
    }

    #[tokio::test]
    async fn table_exists_status_uses_head_rest_semantics() {
        let store = TestTableCatalogStore::default();
        let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");

        create_namespace_response(
            &store,
            "warehouse",
            CreateNamespaceRequest {
                namespace: vec!["analytics".to_string()],
                properties: BTreeMap::new(),
            },
            true,
        )
        .await
        .expect("namespace should be created");

        assert_eq!(
            table_exists_status(&store, "warehouse", &namespace, "events")
                .await
                .expect("missing table check should succeed"),
            StatusCode::NOT_FOUND
        );

        store
            .create_table(crate::table_catalog::TableEntry {
                version: crate::table_catalog::TABLE_CATALOG_ENTRY_VERSION,
                table_bucket: "warehouse".to_string(),
                namespace: namespace.public_name(),
                table: "events".to_string(),
                table_id: "table-id".to_string(),
                table_uuid: "table-uuid".to_string(),
                format: "ICEBERG".to_string(),
                format_version: 2,
                warehouse_location: "s3://warehouse/tables/table-id".to_string(),
                metadata_location:
                    ".rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00001.metadata.json"
                        .to_string(),
                version_token: "token-v1".to_string(),
                generation: 1,
                state: crate::table_catalog::TableCatalogEntryState::Active,
                properties: BTreeMap::new(),
                created_at: None,
                updated_at: None,
            })
            .await
            .expect("table should be created");

        assert_eq!(
            table_exists_status(&store, "warehouse", &namespace, "events")
                .await
                .expect("existing table check should succeed"),
            StatusCode::NO_CONTENT
        );
    }

    #[test]
    fn register_table_request_builds_initial_table_entry() {
        let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
        let request: RegisterTableRequest = serde_json::from_value(serde_json::json!({
            "name": "events",
            "metadata-location": "s3://warehouse/.rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00001.metadata.json",
            "overwrite": false
        }))
        .expect("request should parse");

        let entry = table_entry_from_register_request("warehouse", &namespace, request).expect("table entry should build");

        assert_eq!(entry.table_bucket, "warehouse");
        assert_eq!(entry.namespace, "analytics");
        assert_eq!(entry.table, "events");
        assert_eq!(
            entry.metadata_location,
            ".rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00001.metadata.json"
        );
        assert!(entry.properties.is_empty());
        assert_eq!(entry.generation, 1);
        assert!(!entry.version_token.is_empty());
    }

    #[test]
    fn create_table_request_accepts_standard_iceberg_rest_shape() {
        let request: CreateTableRequest = serde_json::from_value(serde_json::json!({
            "name": "events",
            "schema": {
                "type": "struct",
                "schema-id": 0,
                "fields": [
                    {
                        "id": 1,
                        "name": "id",
                        "required": true,
                        "type": "long"
                    }
                ]
            },
            "partition-spec": {
                "spec-id": 0,
                "fields": []
            },
            "write-order": {
                "order-id": 0,
                "fields": []
            },
            "properties": {
                "write.format.default": "parquet"
            }
        }))
        .expect("standard create table request should parse");

        assert_eq!(request.name, "events");
    }

    #[test]
    fn commit_table_request_accepts_standard_iceberg_rest_shape() {
        let request: RestCommitTableRequest = serde_json::from_value(serde_json::json!({
            "requirements": [
                {
                    "type": "assert-table-uuid",
                    "uuid": "table-uuid"
                }
            ],
            "updates": [
                {
                    "action": "set-properties",
                    "updates": {
                        "owner": "lakehouse"
                    }
                }
            ]
        }))
        .expect("standard commit table request should parse");

        assert_eq!(request.requirements.len(), 1);
    }

    #[tokio::test]
    async fn commit_table_request_honors_standard_idempotency_header() {
        let request_body = |body_key: Option<&str>| {
            Body::from(
                serde_json::to_vec(&serde_json::json!({
                    "idempotency-key": body_key,
                    "requirements": [],
                    "updates": []
                }))
                .expect("commit request should serialize"),
            )
        };
        let header_key = Uuid::now_v7().to_string();
        let mut headers = HeaderMap::new();
        headers.insert(
            IDEMPOTENCY_KEY_HEADER,
            HeaderValue::from_str(&header_key).expect("UUIDv7 header should parse"),
        );

        let request = read_rest_commit_table_request(&headers, request_body(None))
            .await
            .expect("standard Idempotency-Key header should parse");
        assert_eq!(request.idempotency_key.as_deref(), Some(header_key.as_str()));

        let request = read_rest_commit_table_request(&headers, request_body(Some(&header_key)))
            .await
            .expect("matching header and body keys should parse");
        assert_eq!(request.idempotency_key.as_deref(), Some(header_key.as_str()));

        let mismatch = read_rest_commit_table_request(&headers, request_body(Some("different-key")))
            .await
            .expect_err("mismatched header and body keys must fail");
        assert_eq!(mismatch.code(), &S3ErrorCode::Custom(ICEBERG_ERROR_BAD_REQUEST.into()));
        assert_eq!(mismatch.status_code(), Some(StatusCode::BAD_REQUEST));

        let mut invalid_headers = HeaderMap::new();
        invalid_headers.insert(
            IDEMPOTENCY_KEY_HEADER,
            HeaderValue::from_str(&Uuid::new_v4().to_string()).expect("UUIDv4 header should parse"),
        );
        let invalid = read_rest_commit_table_request(&invalid_headers, request_body(None))
            .await
            .expect_err("non-v7 idempotency keys must fail");
        assert_eq!(invalid.code(), &S3ErrorCode::Custom(ICEBERG_ERROR_BAD_REQUEST.into()));
        assert_eq!(invalid.status_code(), Some(StatusCode::BAD_REQUEST));

        let mut repeated_headers = HeaderMap::new();
        repeated_headers.append(
            IDEMPOTENCY_KEY_HEADER,
            HeaderValue::from_str(&header_key).expect("UUIDv7 header should parse"),
        );
        repeated_headers.append(
            IDEMPOTENCY_KEY_HEADER,
            HeaderValue::from_str(&Uuid::now_v7().to_string()).expect("UUIDv7 header should parse"),
        );
        let repeated = read_rest_commit_table_request(&repeated_headers, request_body(None))
            .await
            .expect_err("repeated idempotency headers must fail");
        assert_eq!(repeated.code(), &S3ErrorCode::Custom(ICEBERG_ERROR_BAD_REQUEST.into()));
        assert_eq!(repeated.status_code(), Some(StatusCode::BAD_REQUEST));
    }

    #[test]
    fn commit_requests_accept_legacy_empty_defaults_and_reject_view_commit_ids() {
        let table = serde_json::from_value::<RestCommitTableRequest>(serde_json::json!({}))
            .expect("legacy table commit should default omitted requirements and updates");
        assert!(table.requirements.is_empty());
        assert!(table.updates.is_empty());
        let view = serde_json::from_value::<RestCommitViewRequest>(serde_json::json!({}))
            .expect("legacy view commit should default omitted requirements and updates");
        assert!(view.requirements.is_empty());
        assert!(view.updates.is_empty());
        serde_json::from_value::<RestCommitViewRequest>(serde_json::json!({
            "commit-id": "non-standard-extension",
            "requirements": [],
            "updates": []
        }))
        .expect_err("view commit-id must be rejected until view commits have durable idempotency records");
    }

    #[test]
    fn unsupported_create_and_register_modes_return_iceberg_errors() {
        let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
        let register_error = table_entry_from_register_request(
            "warehouse",
            &namespace,
            RegisterTableRequest {
                name: "events".to_string(),
                metadata_location: "s3://warehouse/metadata/00001.metadata.json".to_string(),
                overwrite: true,
            },
        )
        .expect_err("register overwrite should remain unsupported");
        assert_eq!(register_error.code(), &S3ErrorCode::Custom(ICEBERG_ERROR_UNSUPPORTED_OPERATION.into()));
        assert_eq!(register_error.status_code(), Some(StatusCode::NOT_ACCEPTABLE));

        let create_request: CreateTableRequest = serde_json::from_value(serde_json::json!({
            "name": "events",
            "schema": {
                "type": "struct",
                "schema-id": 0,
                "fields": []
            },
            "stage-create": true
        }))
        .expect("stage-create request should parse");
        let create_error = table_entry_from_create_table_request("warehouse", &namespace, create_request)
            .expect_err("staged create should remain unsupported");
        assert_eq!(create_error.code(), &S3ErrorCode::Custom(ICEBERG_ERROR_UNSUPPORTED_OPERATION.into()));
        assert_eq!(create_error.status_code(), Some(StatusCode::NOT_ACCEPTABLE));
    }

    #[test]
    fn table_format_versions_accept_v1_and_v2_and_reject_v3() {
        let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
        for version in [1, 2] {
            let request = serde_json::from_value(serde_json::json!({
                "name": format!("events_v{version}"),
                "schema": {
                    "type": "struct",
                    "schema-id": 0,
                    "fields": []
                },
                "properties": {
                    "format-version": version.to_string()
                }
            }))
            .expect("create request should parse");
            let (entry, metadata) = table_entry_from_create_table_request("warehouse", &namespace, request)
                .expect("supported format version should create metadata");
            assert_eq!(entry.format_version, version);
            assert_eq!(metadata["format-version"], version);
            if version == 1 {
                assert!(metadata["schema"].is_object());
                assert!(metadata["partition-spec"].is_array());
                for v2_field in [
                    "schemas",
                    "current-schema-id",
                    "partition-specs",
                    "default-spec-id",
                    "last-partition-id",
                    "sort-orders",
                    "default-sort-order-id",
                    "last-sequence-number",
                ] {
                    assert!(metadata.get(v2_field).is_none(), "Iceberg v1 metadata must omit {v2_field}");
                }
            } else {
                assert!(metadata["schemas"].is_array());
                assert!(metadata["partition-specs"].is_array());
                assert!(metadata["sort-orders"].is_array());
                assert!(metadata.get("schema").is_none());
                assert!(metadata.get("partition-spec").is_none());
            }
        }

        let request = serde_json::from_value(serde_json::json!({
            "name": "events_v3",
            "schema": {
                "type": "struct",
                "schema-id": 0,
                "fields": []
            },
            "properties": {
                "format-version": "3"
            }
        }))
        .expect("create request should parse");
        let error = table_entry_from_create_table_request("warehouse", &namespace, request)
            .expect_err("format version 3 is not implemented");
        assert_eq!(error.code(), &S3ErrorCode::Custom(ICEBERG_ERROR_UNSUPPORTED_OPERATION.into()));
        assert_eq!(error.status_code(), Some(StatusCode::NOT_ACCEPTABLE));

        let mut entry = table_entry_from_register_request(
            "warehouse",
            &namespace,
            RegisterTableRequest {
                name: "registered_v3".to_string(),
                metadata_location: "s3://warehouse/tables/registered-v3/metadata/00001.metadata.json".to_string(),
                overwrite: false,
            },
        )
        .expect("register request should build an entry");
        let metadata = serde_json::json!({
            "format-version": 3,
            "table-uuid": "registered-v3-uuid",
            "location": "s3://warehouse/tables/registered-v3"
        });
        let error = adopt_registered_metadata_identity(&mut entry, &metadata)
            .expect_err("registered format version 3 is not implemented");
        assert_eq!(error.code(), &S3ErrorCode::Custom(ICEBERG_ERROR_UNSUPPORTED_OPERATION.into()));
        assert_eq!(error.status_code(), Some(StatusCode::NOT_ACCEPTABLE));
    }

    #[test]
    fn v1_table_metadata_upgrades_to_complete_v2_shape() {
        let metadata = serde_json::json!({
            "format-version": 1,
            "table-uuid": "table-uuid",
            "location": "s3://warehouse/tables/table-id",
            "last-updated-ms": 1,
            "last-column-id": 0,
            "schema": {"type": "struct", "schema-id": 7, "fields": []},
            "partition-spec": [],
            "properties": {},
            "snapshots": [{
                "snapshot-id": 10,
                "timestamp-ms": 1,
                "manifests": ["s3://warehouse/tables/table-id/metadata/manifest.avro"],
                "summary": {"operation": "append"}
            }],
            "current-snapshot-id": 10,
            "snapshot-log": [],
            "metadata-log": []
        });

        let upgraded = apply_table_commit_updates_at(
            metadata,
            &[serde_json::json!({"action": "upgrade-format-version", "format-version": 2})],
            "s3://warehouse/tables/table-id/metadata/v1.metadata.json",
            2,
        )
        .expect("v1 metadata should upgrade to v2");

        assert_eq!(upgraded["format-version"], 2);
        assert_eq!(upgraded["current-schema-id"], 7);
        assert_eq!(upgraded["schemas"][0]["schema-id"], 7);
        assert_eq!(upgraded["default-spec-id"], 0);
        assert_eq!(upgraded["default-sort-order-id"], 0);
        assert_eq!(upgraded["last-sequence-number"], 0);
        assert_eq!(upgraded["snapshots"][0]["sequence-number"], 0);
        assert!(upgraded.get("schema").is_none());
        assert!(upgraded.get("partition-spec").is_none());
        validate_supported_table_metadata(&upgraded).expect("upgraded metadata should satisfy the v2 contract");
    }

    #[test]
    fn standard_commit_ids_use_uuid_for_metadata_file_when_provided() {
        let commit_id = "11111111-1111-4111-8111-111111111111";
        assert_eq!(
            standard_commit_ids(Some(commit_id.to_string()), None),
            (commit_id.to_string(), commit_id.to_string())
        );
    }

    #[test]
    fn standard_commit_ids_generate_metadata_hash_for_non_uuid_client_id() {
        let (commit_id, metadata_file_token) = standard_commit_ids(Some("commit-1".to_string()), None);

        assert_eq!(commit_id, "commit-1");
        assert_ne!(metadata_file_token, commit_id);
        assert_eq!(metadata_file_token, table_catalog_path_hash("commit-1"));
    }

    #[test]
    fn standard_commit_ids_are_stable_for_idempotency_only_retries() {
        let first = standard_commit_ids(None, Some("client-request"));
        let second = standard_commit_ids(None, Some("client-request"));

        assert_eq!(first, second);
        assert!(first.0.starts_with("idempotency-"));
        assert_eq!(first.1, table_catalog_path_hash(&first.0));
    }

    #[tokio::test]
    async fn create_table_response_writes_initial_metadata_for_standard_request() {
        let store = TestTableCatalogStore::default();
        let metadata_backend = TestTableCatalogObjectBackend::default();
        let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
        ensure_table_bucket_entry(&store, "warehouse", true)
            .await
            .expect("table bucket entry should be seeded");
        create_namespace_response(
            &store,
            "warehouse",
            CreateNamespaceRequest {
                namespace: vec!["analytics".to_string()],
                properties: BTreeMap::new(),
            },
            true,
        )
        .await
        .expect("namespace should be created");

        let request: CreateTableRequest = serde_json::from_value(serde_json::json!({
            "name": "events",
            "schema": {
                "type": "struct",
                "schema-id": 0,
                "fields": [
                    {
                        "id": 1,
                        "name": "id",
                        "required": true,
                        "type": "long"
                    }
                ]
            },
            "properties": {
                "write.format.default": "parquet"
            }
        }))
        .expect("standard create table request should parse");

        let response = create_table_response(&store, &metadata_backend, "warehouse", &namespace, request, true)
            .await
            .expect("table should be created");

        assert_eq!(response.metadata["format-version"], 2);
        assert_eq!(response.metadata["current-schema-id"], 0);
        assert_eq!(response.metadata["default-spec-id"], 0);
        assert_eq!(response.metadata["default-sort-order-id"], 0);
        assert_eq!(
            response.metadata["properties"]["write.format.default"],
            serde_json::Value::String("parquet".to_string())
        );
        let entry = store
            .load_table("warehouse", "analytics", "events")
            .await
            .expect("table lookup should succeed")
            .expect("table should exist");
        assert_eq!(
            response.metadata_location,
            format!(
                "s3://warehouse/.rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00001-{}.metadata.json",
                entry.table_id
            )
        );
        assert_eq!(response.metadata["table-uuid"], entry.table_uuid);
        assert!(
            metadata_backend
                .object_exists("warehouse", &entry.metadata_location)
                .await
                .expect("metadata object lookup should succeed")
        );
    }

    #[tokio::test]
    async fn create_responses_remove_initial_metadata_when_catalog_publication_fails() {
        let store = TestTableCatalogStore::default();
        let metadata_backend = TestTableCatalogObjectBackend::default();
        let namespace = crate::table_catalog::Namespace::parse("missing").expect("namespace should parse");

        let table_request = serde_json::from_value(serde_json::json!({
            "name": "events",
            "schema": {
                "type": "struct",
                "schema-id": 0,
                "fields": []
            }
        }))
        .expect("table request should parse");
        create_table_response(&store, &metadata_backend, "warehouse", &namespace, table_request, true)
            .await
            .expect_err("missing namespace should reject table publication");
        assert!(metadata_backend.objects.lock().await.is_empty());

        let view_request = serde_json::from_value(serde_json::json!({
            "name": "recent_events",
            "schema": {
                "type": "struct",
                "schema-id": 0,
                "fields": []
            },
            "view-version": {
                "version-id": 1,
                "schema-id": 0,
                "summary": {},
                "representations": []
            }
        }))
        .expect("view request should parse");
        create_view_response(&store, &metadata_backend, "warehouse", &namespace, view_request, true)
            .await
            .expect_err("missing namespace should reject view publication");
        assert!(metadata_backend.objects.lock().await.is_empty());
    }

    #[tokio::test]
    async fn create_table_response_rejects_lost_initial_metadata_lock_before_catalog_publish() {
        let store = TestTableCatalogStore::default();
        let metadata_backend = TestTableCatalogObjectBackend::default();
        let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
        ensure_table_bucket_entry(&store, "warehouse", true)
            .await
            .expect("table bucket entry should be seeded");
        create_namespace_response(
            &store,
            "warehouse",
            CreateNamespaceRequest {
                namespace: vec!["analytics".to_string()],
                properties: BTreeMap::new(),
            },
            true,
        )
        .await
        .expect("namespace should be created");
        metadata_backend.lose_write_locks();
        let request = serde_json::from_value(serde_json::json!({
            "name": "events",
            "schema": {
                "type": "struct",
                "schema-id": 0,
                "fields": []
            }
        }))
        .expect("create table request should parse");

        create_table_response(&store, &metadata_backend, "warehouse", &namespace, request, true)
            .await
            .expect_err("a lost metadata lock must reject catalog publication");

        assert!(store.tables.lock().await.is_empty());
        assert_eq!(metadata_backend.objects.lock().await.len(), 1);
    }

    #[tokio::test]
    async fn create_table_response_retains_initial_metadata_when_publish_fails_after_lock_loss() {
        let pause = TestCatalogPublishPause::default();
        let store = Arc::new(TestTableCatalogStore {
            create_table_pause: Some(pause.clone()),
            fail_create_table_after_pause: true,
            ..Default::default()
        });
        let metadata_backend = TestTableCatalogObjectBackend::default();
        let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
        ensure_table_bucket_entry(store.as_ref(), "warehouse", true)
            .await
            .expect("table bucket entry should be seeded");
        create_namespace_response(
            store.as_ref(),
            "warehouse",
            CreateNamespaceRequest {
                namespace: vec!["analytics".to_string()],
                properties: BTreeMap::new(),
            },
            true,
        )
        .await
        .expect("namespace should be created");
        let request = serde_json::from_value(serde_json::json!({
            "name": "events",
            "schema": {
                "type": "struct",
                "schema-id": 0,
                "fields": []
            }
        }))
        .expect("create table request should parse");
        let create_store = Arc::clone(&store);
        let create_backend = metadata_backend.clone();
        let create_namespace = namespace.clone();
        let create = tokio::spawn(async move {
            create_table_response(create_store.as_ref(), &create_backend, "warehouse", &create_namespace, request, true).await
        });

        pause.wait_started().await;
        metadata_backend.lose_write_locks();
        pause.release();
        create
            .await
            .expect("create task should join")
            .expect_err("injected catalog publication failure should be returned");

        assert!(store.tables.lock().await.is_empty());
        assert_eq!(metadata_backend.objects.lock().await.len(), 1);
    }

    #[tokio::test]
    async fn create_table_response_holds_initial_metadata_lock_until_catalog_publish() {
        let pause = TestCatalogPublishPause::default();
        let store = Arc::new(TestTableCatalogStore {
            create_table_pause: Some(pause.clone()),
            ..Default::default()
        });
        let metadata_backend = TestTableCatalogObjectBackend::default();
        let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
        ensure_table_bucket_entry(store.as_ref(), "warehouse", true)
            .await
            .expect("table bucket entry should be seeded");
        create_namespace_response(
            store.as_ref(),
            "warehouse",
            CreateNamespaceRequest {
                namespace: vec!["analytics".to_string()],
                properties: BTreeMap::new(),
            },
            true,
        )
        .await
        .expect("namespace should be created");
        let request = serde_json::from_value(serde_json::json!({
            "name": "events",
            "schema": {
                "type": "struct",
                "schema-id": 0,
                "fields": []
            }
        }))
        .expect("create table request should parse");
        let create_store = Arc::clone(&store);
        let create_backend = metadata_backend.clone();
        let create_namespace = namespace.clone();
        let create = tokio::spawn(async move {
            create_table_response(create_store.as_ref(), &create_backend, "warehouse", &create_namespace, request, true).await
        });

        pause.wait_started().await;
        let metadata_location = metadata_backend
            .objects
            .lock()
            .await
            .keys()
            .find(|(bucket, _)| bucket == "warehouse")
            .map(|(_, object)| object.clone())
            .expect("initial metadata object should exist before catalog publish");
        assert!(metadata_backend.write_lock_is_held("warehouse", &metadata_location).await);

        pause.release();
        create
            .await
            .expect("create task should join")
            .expect("table creation should succeed");
        assert!(!metadata_backend.write_lock_is_held("warehouse", &metadata_location).await);
    }

    #[tokio::test]
    async fn create_view_response_holds_initial_metadata_lock_until_catalog_publish() {
        let pause = TestCatalogPublishPause::default();
        let store = Arc::new(TestTableCatalogStore {
            create_view_pause: Some(pause.clone()),
            ..Default::default()
        });
        let metadata_backend = TestTableCatalogObjectBackend::default();
        let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
        ensure_table_bucket_entry(store.as_ref(), "warehouse", true)
            .await
            .expect("table bucket entry should be seeded");
        create_namespace_response(
            store.as_ref(),
            "warehouse",
            CreateNamespaceRequest {
                namespace: vec!["analytics".to_string()],
                properties: BTreeMap::new(),
            },
            true,
        )
        .await
        .expect("namespace should be created");
        let request = serde_json::from_value(serde_json::json!({
            "name": "recent_events",
            "schema": {
                "type": "struct",
                "schema-id": 0,
                "fields": []
            },
            "view-version": {
                "version-id": 1,
                "schema-id": 0,
                "summary": {},
                "representations": []
            }
        }))
        .expect("create view request should parse");
        let create_store = Arc::clone(&store);
        let create_backend = metadata_backend.clone();
        let create_namespace = namespace.clone();
        let create = tokio::spawn(async move {
            create_view_response(create_store.as_ref(), &create_backend, "warehouse", &create_namespace, request, true).await
        });

        pause.wait_started().await;
        let metadata_location = metadata_backend
            .objects
            .lock()
            .await
            .keys()
            .find(|(bucket, _)| bucket == "warehouse")
            .map(|(_, object)| object.clone())
            .expect("initial view metadata object should exist before catalog publish");
        assert!(metadata_backend.write_lock_is_held("warehouse", &metadata_location).await);

        pause.release();
        create
            .await
            .expect("create task should join")
            .expect("view creation should succeed");
        assert!(!metadata_backend.write_lock_is_held("warehouse", &metadata_location).await);
    }

    #[tokio::test]
    async fn create_table_response_recreates_dropped_identifier_without_overwriting_retained_metadata() {
        let store = TestTableCatalogStore::default();
        let metadata_backend = TestTableCatalogObjectBackend::default();
        let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
        create_standard_events_table(&store, &metadata_backend, &namespace).await;
        let first_entry = store
            .load_table("warehouse", "analytics", "events")
            .await
            .expect("first table lookup should succeed")
            .expect("first table should exist");
        let retained_initial = metadata_backend
            .read_object("warehouse", &first_entry.metadata_location)
            .await
            .expect("first metadata lookup should succeed")
            .expect("first metadata should exist");

        let commit_request: RestCommitTableRequest = serde_json::from_value(serde_json::json!({
            "requirements": [],
            "updates": [
                {
                    "action": "set-properties",
                    "updates": {
                        "owner": "first-table"
                    }
                }
            ]
        }))
        .expect("standard commit request should parse");
        standard_commit_table_response(&store, &metadata_backend, "warehouse", &namespace, "events", commit_request)
            .await
            .expect("first table metadata commit should succeed");
        let first_committed_entry = store
            .load_table("warehouse", "analytics", "events")
            .await
            .expect("committed table lookup should succeed")
            .expect("committed table should exist");

        drop_table_in_store(&store, "warehouse", &namespace, "events")
            .await
            .expect("first table should drop");
        drop_namespace_in_store(&store, "warehouse", "analytics")
            .await
            .expect("first namespace should drop");
        create_namespace_response(
            &store,
            "warehouse",
            CreateNamespaceRequest {
                namespace: vec!["analytics".to_string()],
                properties: BTreeMap::new(),
            },
            true,
        )
        .await
        .expect("namespace should be recreated");

        let create_request: CreateTableRequest = serde_json::from_value(serde_json::json!({
            "name": "events",
            "schema": {
                "type": "struct",
                "schema-id": 0,
                "fields": [
                    {
                        "id": 1,
                        "name": "id",
                        "required": true,
                        "type": "long"
                    }
                ]
            }
        }))
        .expect("recreate table request should parse");
        let second = create_table_response(&store, &metadata_backend, "warehouse", &namespace, create_request, true)
            .await
            .expect("dropped table identifier should be reusable");
        let second_entry = store
            .load_table("warehouse", "analytics", "events")
            .await
            .expect("recreated table lookup should succeed")
            .expect("recreated table should exist");

        assert_ne!(first_entry.table_id, second_entry.table_id);
        assert_ne!(first_entry.table_uuid, second_entry.table_uuid);
        assert_ne!(first_entry.metadata_location, second_entry.metadata_location);
        assert_ne!(first_committed_entry.metadata_location, second_entry.metadata_location);
        assert_eq!(second.metadata["table-uuid"], second_entry.table_uuid);
        assert_eq!(second.metadata["metadata-log"], serde_json::json!([]));
        assert_eq!(
            metadata_backend
                .read_object("warehouse", &first_entry.metadata_location)
                .await
                .expect("retained metadata lookup should succeed")
                .expect("retained metadata should still exist")
                .data,
            retained_initial.data
        );
        assert!(
            metadata_backend
                .object_exists("warehouse", &first_committed_entry.metadata_location)
                .await
                .expect("committed metadata lookup should succeed")
        );
        assert!(
            metadata_backend
                .object_exists("warehouse", &second_entry.metadata_location)
                .await
                .expect("recreated metadata lookup should succeed")
        );
    }

    #[tokio::test]
    async fn concurrent_create_table_responses_keep_one_catalog_winner_with_distinct_metadata() {
        let catalog_backend = TestTableCatalogObjectBackend::default();
        let store = crate::table_catalog::ObjectTableCatalogStore::new(catalog_backend);
        let metadata_backend = TestTableCatalogObjectBackend {
            objects: Arc::new(tokio::sync::Mutex::new(BTreeMap::new())),
            put_object_barrier: Some(Arc::new(tokio::sync::Barrier::new(2))),
            ..Default::default()
        };
        let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
        ensure_table_bucket_entry(&store, "warehouse", true)
            .await
            .expect("table bucket entry should be seeded");
        create_namespace_response(
            &store,
            "warehouse",
            CreateNamespaceRequest {
                namespace: vec!["analytics".to_string()],
                properties: BTreeMap::new(),
            },
            true,
        )
        .await
        .expect("namespace should be created");
        let create_request = || {
            serde_json::from_value::<CreateTableRequest>(serde_json::json!({
                "name": "events",
                "schema": {
                    "type": "struct",
                    "schema-id": 0,
                    "fields": [
                        {
                            "id": 1,
                            "name": "id",
                            "required": true,
                            "type": "long"
                        }
                    ]
                }
            }))
            .expect("concurrent create table request should parse")
        };

        let (first, second) = tokio::join!(
            create_table_response(&store, &metadata_backend, "warehouse", &namespace, create_request(), true,),
            create_table_response(&store, &metadata_backend, "warehouse", &namespace, create_request(), true,)
        );
        assert_ne!(first.is_ok(), second.is_ok(), "exactly one concurrent create should succeed");
        let (winner, loser) = match (first, second) {
            (Ok(winner), Err(loser)) | (Err(loser), Ok(winner)) => (winner, loser),
            _ => unreachable!("success count was checked above"),
        };
        assert!(format!("{loser:?}").contains("AlreadyExistsException"));

        let tables = store
            .list_tables("warehouse", "analytics")
            .await
            .expect("table listing should succeed");
        assert_eq!(tables.len(), 1);
        let winner_entry = &tables[0];
        assert_eq!(
            winner.metadata_location,
            table_metadata_location_for_client("warehouse", &winner_entry.metadata_location)
        );
        let metadata_prefix = winner_entry
            .metadata_location
            .rsplit_once('/')
            .map(|(prefix, _)| format!("{prefix}/"))
            .expect("metadata location should contain a file name");
        let metadata_objects = metadata_backend
            .list_objects("warehouse", &metadata_prefix)
            .await
            .expect("metadata listing should succeed");
        assert_eq!(metadata_objects.len(), 2);
        assert_ne!(metadata_objects[0], metadata_objects[1]);

        let mut table_uuids = Vec::with_capacity(metadata_objects.len());
        for metadata_object in metadata_objects {
            let metadata = metadata_backend
                .read_object("warehouse", &metadata_object)
                .await
                .expect("metadata lookup should succeed")
                .expect("metadata object should exist");
            let metadata: serde_json::Value =
                serde_json::from_slice(&metadata.data).expect("metadata object should contain JSON");
            table_uuids.push(
                metadata["table-uuid"]
                    .as_str()
                    .expect("metadata should contain table uuid")
                    .to_string(),
            );
        }
        table_uuids.sort();
        table_uuids.dedup();
        assert_eq!(table_uuids.len(), 2);
        assert!(table_uuids.contains(&winner_entry.table_uuid));
    }

    #[tokio::test]
    async fn standard_commit_applies_updates_and_writes_next_metadata() {
        let store = TestTableCatalogStore::default();
        let metadata_backend = TestTableCatalogObjectBackend::default();
        let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
        let created = create_standard_events_table(&store, &metadata_backend, &namespace).await;
        let table_uuid = created.metadata["table-uuid"]
            .as_str()
            .expect("created metadata should have table uuid")
            .to_string();
        let table_location = created.metadata["location"]
            .as_str()
            .expect("created metadata should have table location");
        let manifest_list = format!("{table_location}/metadata/snap-10.avro");
        let data_file = format!("{table_location}/data/part-10.parquet");
        seed_test_snapshot_manifest(&metadata_backend, "warehouse", &manifest_list, 10, 1, &[(&data_file, 0, 1, 10, 1)]).await;

        let commit_request: RestCommitTableRequest = serde_json::from_value(serde_json::json!({
            "requirements": [
                {
                    "type": "assert-table-uuid",
                    "uuid": table_uuid
                },
                {
                    "type": "assert-current-schema-id",
                    "current-schema-id": 0
                }
            ],
            "updates": [
                {
                    "action": "set-properties",
                    "updates": {
                        "owner": "lakehouse"
                    }
                },
                {
                    "action": "add-snapshot",
                    "snapshot": {
                        "snapshot-id": 10,
                        "sequence-number": 1,
                        "timestamp-ms": 1234,
                        "manifest-list": manifest_list,
                        "summary": {
                            "operation": "append"
                        }
                    }
                },
                {
                    "action": "set-snapshot-ref",
                    "ref-name": "main",
                    "snapshot-id": 10,
                    "type": "branch"
                }
            ]
        }))
        .expect("standard commit table request should parse");

        let commit = commit_table_response(&store, &metadata_backend, "warehouse", &namespace, "events", commit_request)
            .await
            .expect("standard commit should succeed");

        let metadata_file_prefix =
            "s3://warehouse/.rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00002-";
        let metadata_file_suffix = ".metadata.json";
        let generated_commit_id = commit
            .metadata_location
            .strip_prefix(metadata_file_prefix)
            .and_then(|file| file.strip_suffix(metadata_file_suffix))
            .expect("standard commit metadata file should include a UUID suffix");
        Uuid::parse_str(generated_commit_id).expect("metadata file suffix should be a UUID");
        assert_eq!(commit.commit_id, generated_commit_id);
        assert_eq!(commit.metadata["properties"]["owner"], serde_json::Value::String("lakehouse".to_string()));
        assert_eq!(commit.metadata["current-snapshot-id"], 10);
        assert_eq!(commit.metadata["last-sequence-number"], 1);
        assert_eq!(commit.metadata["refs"]["main"]["snapshot-id"], 10);
        assert_eq!(commit.metadata["metadata-log"][0]["metadata-file"], created.metadata_location);
        let committed = store
            .load_table("warehouse", "analytics", "events")
            .await
            .expect("committed table lookup should succeed")
            .expect("committed table should exist");
        assert_eq!(
            table_metadata_location_for_client("warehouse", &committed.metadata_location),
            commit.metadata_location
        );
        assert!(
            metadata_backend
                .object_exists("warehouse", &committed.metadata_location)
                .await
                .expect("committed metadata lookup should succeed")
        );
    }

    #[tokio::test]
    async fn standard_commit_uses_client_uuid_commit_id_in_metadata_file_name() {
        let store = TestTableCatalogStore::default();
        let metadata_backend = TestTableCatalogObjectBackend::default();
        let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
        create_standard_events_table(&store, &metadata_backend, &namespace).await;

        let commit_id = "11111111-1111-4111-8111-111111111111";
        let commit_request: RestCommitTableRequest = serde_json::from_value(serde_json::json!({
            "commit-id": commit_id,
            "requirements": [],
            "updates": [
                {
                    "action": "set-properties",
                    "updates": {
                        "owner": "lakehouse"
                    }
                }
            ]
        }))
        .expect("standard commit table request should parse");
        let commit = commit_table_response(&store, &metadata_backend, "warehouse", &namespace, "events", commit_request)
            .await
            .expect("standard commit should succeed");

        assert_eq!(commit.commit_id, commit_id);
        assert_eq!(
            commit.metadata_location,
            "s3://warehouse/.rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00002-11111111-1111-4111-8111-111111111111.metadata.json"
        );
        let committed = store
            .load_table("warehouse", "analytics", "events")
            .await
            .expect("committed table lookup should succeed")
            .expect("committed table should exist");
        assert!(
            metadata_backend
                .object_exists("warehouse", &committed.metadata_location)
                .await
                .expect("committed metadata lookup should succeed")
        );
    }

    #[tokio::test]
    async fn standard_commit_accepts_non_uuid_client_commit_id_without_using_it_in_metadata_file_name() {
        let store = TestTableCatalogStore::default();
        let metadata_backend = TestTableCatalogObjectBackend::default();
        let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
        create_standard_events_table(&store, &metadata_backend, &namespace).await;

        let commit_request: RestCommitTableRequest = serde_json::from_value(serde_json::json!({
            "commit-id": "commit-1",
            "requirements": [],
            "updates": [
                {
                    "action": "set-properties",
                    "updates": {
                        "owner": "lakehouse"
                    }
                }
            ]
        }))
        .expect("standard commit table request should parse");
        let commit = commit_table_response(&store, &metadata_backend, "warehouse", &namespace, "events", commit_request)
            .await
            .expect("standard commit should succeed");

        let metadata_file_prefix =
            "s3://warehouse/.rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00002-";
        let metadata_file_suffix = ".metadata.json";
        let metadata_file_token = commit
            .metadata_location
            .strip_prefix(metadata_file_prefix)
            .and_then(|file| file.strip_suffix(metadata_file_suffix))
            .expect("standard commit metadata file should include a safe token suffix");
        assert_eq!(commit.commit_id, "commit-1");
        assert_ne!(metadata_file_token, commit.commit_id);
        assert_eq!(metadata_file_token, table_catalog_path_hash("commit-1"));
    }

    #[tokio::test]
    async fn catalog_backings_replay_old_standard_commit_with_current_table_state() {
        for mode in [
            crate::table_catalog::TableCatalogBackingMode::ObjectBacked,
            crate::table_catalog::TableCatalogBackingMode::DurableStrong,
        ] {
            let metadata_backend = TestTableCatalogObjectBackend::default();
            let store = crate::table_catalog::ConfiguredTableCatalogStore::new(metadata_backend.clone(), mode);
            let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
            let created = create_standard_events_table(&store, &metadata_backend, &namespace).await;
            let original_warehouse_location = created.metadata["location"]
                .as_str()
                .expect("created metadata should have a warehouse location")
                .to_string();
            let first_request = serde_json::json!({
                "commit-id": "commit-a",
                "idempotency-key": "request-a",
                "writer": "test-client",
                "requirements": [],
                "updates": [{
                    "action": "set-properties",
                    "updates": {
                        "owner": "first"
                    }
                }]
            });
            standard_commit_table_response(
                &store,
                &metadata_backend,
                "warehouse",
                &namespace,
                "events",
                serde_json::from_value(first_request.clone()).expect("first commit request should parse"),
            )
            .await
            .expect("first commit should advance the table");

            let second = standard_commit_table_response(
                &store,
                &metadata_backend,
                "warehouse",
                &namespace,
                "events",
                serde_json::from_value(serde_json::json!({
                    "commit-id": "commit-b",
                    "idempotency-key": "request-b",
                    "requirements": [],
                    "updates": [{
                        "action": "set-properties",
                        "updates": {
                            "owner": "current"
                        }
                    }]
                }))
                .expect("second commit request should parse"),
            )
            .await
            .expect("second commit should advance the table");
            let current = store
                .load_table("warehouse", "analytics", "events")
                .await
                .expect("current table lookup should succeed")
                .expect("current table should exist");
            let retry: RestCommitTableRequest = serde_json::from_value(first_request).expect("first commit retry should parse");

            let read_location =
                table_commit_warehouse_read_location(None, &store, &metadata_backend, "warehouse", &current, &retry)
                    .await
                    .expect("retry read scope should resolve");
            assert_eq!(read_location, original_warehouse_location, "{mode:?}");

            let replay = standard_commit_table_response(&store, &metadata_backend, "warehouse", &namespace, "events", retry)
                .await
                .expect("an exact old commit retry should succeed");

            assert_eq!(replay.commit_id, "commit-a", "{mode:?}");
            assert_eq!(replay.metadata_location, second.metadata_location, "{mode:?}");
            assert_eq!(replay.version_token, second.version_token, "{mode:?}");
            assert_eq!(replay.generation, second.generation, "{mode:?}");
            assert_eq!(replay.metadata["properties"]["owner"], "current", "{mode:?}");
            let unchanged = store
                .load_table("warehouse", "analytics", "events")
                .await
                .expect("table lookup after replay should succeed")
                .expect("table should remain present");
            assert_eq!(unchanged.metadata_location, current.metadata_location, "{mode:?}");
            assert_eq!(unchanged.version_token, current.version_token, "{mode:?}");
        }
    }

    #[tokio::test]
    async fn catalog_backings_replay_legacy_commit_against_original_requirements() {
        for mode in [
            crate::table_catalog::TableCatalogBackingMode::ObjectBacked,
            crate::table_catalog::TableCatalogBackingMode::DurableStrong,
        ] {
            let metadata_backend = TestTableCatalogObjectBackend::default();
            let store = crate::table_catalog::ConfiguredTableCatalogStore::new(metadata_backend.clone(), mode);
            let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
            let created = create_standard_events_table(&store, &metadata_backend, &namespace).await;
            let current = store
                .load_table("warehouse", "analytics", "events")
                .await
                .expect("current table lookup should succeed")
                .expect("current table should exist");
            let target_location = crate::table_catalog::table_metadata_file_path_for_entry(
                &current,
                "00002-11111111-1111-4111-8111-111111111111.metadata.json",
            )
            .expect("target metadata path should resolve");
            let mut target_metadata = created.metadata.clone();
            target_metadata["schemas"]
                .as_array_mut()
                .expect("schemas should be an array")
                .push(serde_json::json!({
                    "type": "struct",
                    "schema-id": 1,
                    "fields": []
                }));
            target_metadata["current-schema-id"] = serde_json::Value::from(1);
            target_metadata["last-updated-ms"] = serde_json::Value::from(current_time_millis());
            metadata_backend
                .put_json("warehouse", &target_location, target_metadata)
                .await;
            let request = serde_json::json!({
                "idempotency-key": "legacy-request",
                "operation": "schema-update",
                "expected-version-token": current.version_token,
                "expected-metadata-location": current.metadata_location,
                "new-metadata-location": target_location,
                "requirements": [{
                    "type": "assert-current-schema-id",
                    "current-schema-id": 0
                }],
                "updates": [],
                "writer": "test-client"
            });

            let committed = commit_table_response(
                &store,
                &metadata_backend,
                "warehouse",
                &namespace,
                "events",
                serde_json::from_value(request.clone()).expect("legacy commit request should parse"),
            )
            .await
            .expect("legacy commit should succeed");
            assert_eq!(committed.metadata["current-schema-id"], 1, "{mode:?}");

            let replay = commit_table_response(
                &store,
                &metadata_backend,
                "warehouse",
                &namespace,
                "events",
                serde_json::from_value(request).expect("legacy retry should parse"),
            )
            .await
            .expect("legacy retry should validate requirements against the original base");

            assert_eq!(replay.commit_id, committed.commit_id, "{mode:?}");
            assert_eq!(replay.metadata_location, committed.metadata_location, "{mode:?}");
            assert_eq!(replay.version_token, committed.version_token, "{mode:?}");
            assert_eq!(replay.generation, committed.generation, "{mode:?}");
            assert_eq!(replay.metadata["current-schema-id"], 1, "{mode:?}");
        }
    }

    #[tokio::test]
    async fn standard_commit_ignores_generation_only_orphan_metadata_file() {
        let store = TestTableCatalogStore::default();
        let metadata_backend = TestTableCatalogObjectBackend::default();
        let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
        create_standard_events_table(&store, &metadata_backend, &namespace).await;
        metadata_backend
            .put_json(
                "warehouse",
                ".rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00002.metadata.json",
                serde_json::json!({
                    "format-version": 2,
                    "table-uuid": "orphan",
                    "location": "s3://warehouse/tables/table-id"
                }),
            )
            .await;

        let commit_id = "22222222-2222-4222-8222-222222222222";
        let commit_request: RestCommitTableRequest = serde_json::from_value(serde_json::json!({
            "commit-id": commit_id,
            "requirements": [],
            "updates": [
                {
                    "action": "set-properties",
                    "updates": {
                        "owner": "lakehouse"
                    }
                }
            ]
        }))
        .expect("standard commit table request should parse");
        let commit = commit_table_response(&store, &metadata_backend, "warehouse", &namespace, "events", commit_request)
            .await
            .expect("standard commit should not collide with generation-only orphan");

        assert_eq!(
            commit.metadata_location,
            "s3://warehouse/.rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00002-22222222-2222-4222-8222-222222222222.metadata.json"
        );
        assert_eq!(commit.metadata["properties"]["owner"], "lakehouse");
    }

    #[tokio::test]
    async fn concurrent_standard_commits_write_distinct_metadata_files_before_pointer_conflict() {
        let store = TestTableCatalogStore::default();
        let metadata_backend = TestTableCatalogObjectBackend::default();
        let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
        create_standard_events_table(&store, &metadata_backend, &namespace).await;

        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let metadata_backend = TestTableCatalogObjectBackend {
            objects: Arc::clone(&metadata_backend.objects),
            put_object_barrier: Some(barrier),
            ..Default::default()
        };
        let first_commit_id = "33333333-3333-4333-8333-333333333333";
        let second_commit_id = "44444444-4444-4444-8444-444444444444";
        let first_request: RestCommitTableRequest = serde_json::from_value(serde_json::json!({
            "commit-id": first_commit_id,
            "requirements": [],
            "updates": [
                {
                    "action": "set-properties",
                    "updates": {
                        "owner": "first"
                    }
                }
            ]
        }))
        .expect("first standard commit table request should parse");
        let second_request: RestCommitTableRequest = serde_json::from_value(serde_json::json!({
            "commit-id": second_commit_id,
            "requirements": [],
            "updates": [
                {
                    "action": "set-properties",
                    "updates": {
                        "owner": "second"
                    }
                }
            ]
        }))
        .expect("second standard commit table request should parse");

        let (first, second) = tokio::join!(
            commit_table_response(&store, &metadata_backend, "warehouse", &namespace, "events", first_request),
            commit_table_response(&store, &metadata_backend, "warehouse", &namespace, "events", second_request)
        );
        let success_count = [first.is_ok(), second.is_ok()].into_iter().filter(|ok| *ok).count();

        assert_eq!(success_count, 1);
        assert!(
            metadata_backend
                .object_exists(
                    "warehouse",
                    ".rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00002-33333333-3333-4333-8333-333333333333.metadata.json"
                )
                .await
                .expect("first metadata object lookup should succeed")
        );
        assert!(
            metadata_backend
                .object_exists(
                    "warehouse",
                    ".rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00002-44444444-4444-4444-8444-444444444444.metadata.json"
                )
                .await
                .expect("second metadata object lookup should succeed")
        );
    }

    #[tokio::test]
    async fn standard_commit_accepts_legacy_catalog_uuid_when_current_metadata_matches() {
        let store = TestTableCatalogStore::default();
        let metadata_backend = TestTableCatalogObjectBackend::default();
        let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
        ensure_table_bucket_entry(&store, "warehouse", true)
            .await
            .expect("table bucket entry should be seeded");
        create_namespace_response(
            &store,
            "warehouse",
            CreateNamespaceRequest {
                namespace: vec!["analytics".to_string()],
                properties: BTreeMap::new(),
            },
            true,
        )
        .await
        .expect("namespace should be created");
        let current_location = ".rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00001.metadata.json";
        let legacy_entry = table_entry_from_register_request(
            "warehouse",
            &namespace,
            RegisterTableRequest {
                name: "events".to_string(),
                metadata_location: current_location.to_string(),
                overwrite: false,
            },
        )
        .expect("table entry should build");
        assert_ne!(legacy_entry.table_uuid, "metadata-table-uuid");
        store
            .register_table(legacy_entry.clone())
            .await
            .expect("legacy table entry should register");
        metadata_backend
            .put_json(
                "warehouse",
                current_location,
                test_table_metadata_json("metadata-table-uuid", "s3://warehouse/tables/table-id"),
            )
            .await;

        let commit_request: RestCommitTableRequest = serde_json::from_value(serde_json::json!({
            "requirements": [],
            "updates": [
                {
                    "action": "set-properties",
                    "updates": {
                        "owner": "lakehouse"
                    }
                }
            ]
        }))
        .expect("standard commit table request should parse");
        let committed = commit_table_response(&store, &metadata_backend, "warehouse", &namespace, "events", commit_request)
            .await
            .expect("legacy catalog uuid should not block standard commit");

        assert_eq!(committed.metadata["table-uuid"], "metadata-table-uuid");
        assert_eq!(committed.metadata["properties"]["owner"], "lakehouse");
        assert_eq!(committed.generation, legacy_entry.generation + 1);
    }

    #[tokio::test]
    async fn metadata_location_api_accepts_legacy_catalog_uuid_when_target_matches_current_metadata() {
        let store = TestTableCatalogStore::default();
        let metadata_backend = TestTableCatalogObjectBackend::default();
        let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
        ensure_table_bucket_entry(&store, "warehouse", true)
            .await
            .expect("table bucket entry should be seeded");
        create_namespace_response(
            &store,
            "warehouse",
            CreateNamespaceRequest {
                namespace: vec!["analytics".to_string()],
                properties: BTreeMap::new(),
            },
            true,
        )
        .await
        .expect("namespace should be created");
        let current_location = ".rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00001.metadata.json";
        let legacy_entry = table_entry_from_register_request(
            "warehouse",
            &namespace,
            RegisterTableRequest {
                name: "events".to_string(),
                metadata_location: current_location.to_string(),
                overwrite: false,
            },
        )
        .expect("table entry should build");
        assert_ne!(legacy_entry.table_uuid, "metadata-table-uuid");
        store
            .register_table(legacy_entry.clone())
            .await
            .expect("legacy table entry should register");
        metadata_backend
            .put_json(
                "warehouse",
                current_location,
                test_table_metadata_json("metadata-table-uuid", "s3://warehouse/tables/table-id"),
            )
            .await;
        let next_location = ".rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00002.metadata.json";
        metadata_backend
            .put_json(
                "warehouse",
                next_location,
                test_table_metadata_json("metadata-table-uuid", "s3://warehouse/tables/table-id"),
            )
            .await;

        let updated = update_table_metadata_location_response(
            &store,
            &metadata_backend,
            "warehouse",
            &namespace,
            "events",
            UpdateTableMetadataLocationRequest {
                metadata_location: next_location.to_string(),
                version_token: legacy_entry.version_token,
                commit_id: Some("commit-1".to_string()),
                idempotency_key: None,
            },
            read_table_metadata_json(&metadata_backend, "warehouse", next_location)
                .await
                .expect("target metadata should load"),
        )
        .await
        .expect("legacy catalog uuid should not block metadata-location update");

        assert_eq!(updated.metadata_location, table_metadata_location_for_client("warehouse", next_location));
        assert_eq!(updated.generation, legacy_entry.generation + 1);
    }

    #[tokio::test]
    async fn table_metadata_maintenance_helper_runs_dry_run_and_delete() {
        let backend = TestTableCatalogObjectBackend::default();
        let store = crate::table_catalog::ObjectTableCatalogStore::new(backend.clone());
        let bucket = "warehouse";
        let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
        let table = crate::table_catalog::IdentifierSegment::parse("events").expect("table should parse");
        let old = crate::table_catalog::default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
        let current = crate::table_catalog::default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");

        seed_object_table_for_metadata_maintenance(&store, &backend, bucket, &namespace, &table, current.clone()).await;
        backend
            .put_json_with_mod_time(bucket, &old, serde_json::json!({}), Some(OffsetDateTime::UNIX_EPOCH))
            .await;
        backend
            .put_json_with_mod_time(
                bucket,
                &current,
                serde_json::json!({
                    "table-uuid": "table-uuid",
                    "current-snapshot-id": 20,
                    "metadata-log": [],
                    "snapshots": [
                        {
                            "snapshot-id": 10,
                            "timestamp-ms": 1000,
                            "manifest-list": "s3://warehouse/tables/table-id/metadata/snap-10.avro"
                        },
                        {
                            "snapshot-id": 20,
                            "timestamp-ms": 2000,
                            "manifest-list": "s3://warehouse/tables/table-id/metadata/snap-20.avro"
                        }
                    ],
                    "refs": {
                        "main": {
                            "snapshot-id": 20,
                            "type": "branch"
                        }
                    }
                }),
                Some(OffsetDateTime::UNIX_EPOCH),
            )
            .await;

        let default_config = store
            .get_table_maintenance_config(bucket, "analytics", "events")
            .await
            .expect("default maintenance config should load");
        assert_eq!(default_config, crate::table_catalog::TableMaintenanceConfig::default());
        let config = store
            .put_table_maintenance_config(
                bucket,
                "analytics",
                "events",
                crate::table_catalog::TableMaintenanceConfig {
                    version: crate::table_catalog::TABLE_MAINTENANCE_CONFIG_VERSION,
                    retain_recent_metadata_files: 2,
                    delete_enabled: true,
                    background_enabled: false,
                    ..Default::default()
                },
            )
            .await
            .expect("maintenance config should persist");
        assert_eq!(config.retain_recent_metadata_files, 2);
        assert!(config.delete_enabled);
        let background_config = store
            .put_table_maintenance_config(
                bucket,
                "analytics",
                "events",
                crate::table_catalog::TableMaintenanceConfig {
                    version: crate::table_catalog::TABLE_MAINTENANCE_CONFIG_VERSION,
                    retain_recent_metadata_files: 2,
                    delete_enabled: true,
                    background_enabled: true,
                    ..Default::default()
                },
            )
            .await
            .expect("background maintenance config should persist");
        assert!(background_config.background_enabled);

        let dry_run = table_metadata_maintenance_response(
            &store,
            &backend,
            bucket,
            &namespace,
            "events",
            TableMetadataMaintenanceRequest {
                retain_recent_metadata_files: 0,
                delete: false,
                snapshot_expiration: Some(crate::table_catalog::TableSnapshotExpirationConfig {
                    min_snapshots_to_keep: 1,
                    max_snapshot_age_ms: 1,
                }),
                commit_snapshot_expiration: false,
                compaction: Some(crate::table_catalog::TableCompactionPlanningConfig {
                    target_file_size_bytes: 512 * 1024 * 1024,
                    small_file_threshold_bytes: 64 * 1024 * 1024,
                    min_input_files: 2,
                    max_rewrite_bytes_per_job: 1024 * 1024 * 1024,
                }),
                commit_compaction: false,
            },
        )
        .await
        .expect("metadata maintenance dry-run should succeed");
        assert_eq!(dry_run.cleanup_candidate_locations, vec![old.clone()]);
        assert_eq!(dry_run.deletable_metadata_locations, vec![old.clone()]);
        let snapshot_expiration = dry_run
            .snapshot_expiration
            .as_ref()
            .expect("dry-run report should include snapshot expiration planning");
        assert_eq!(snapshot_expiration.expiration_candidate_count, 1);
        assert_eq!(snapshot_expiration.current_snapshot_id, Some(20));
        let compaction = dry_run
            .compaction
            .as_ref()
            .expect("dry-run report should include compaction planning");
        assert_eq!(
            compaction.status,
            crate::table_catalog::TableCompactionPlanningStatus::ManualReviewRequired
        );
        assert_eq!(compaction.manual_review_count, 1);
        let stored_dry_run = store
            .get_table_metadata_maintenance_report(bucket, "analytics", "events", &dry_run.job.job_id)
            .await
            .expect("maintenance job lookup should succeed")
            .expect("maintenance job should be stored");
        assert_eq!(stored_dry_run, dry_run);
        assert!(
            backend
                .object_exists(bucket, &old)
                .await
                .expect("old metadata lookup should succeed")
        );

        let deleted = table_metadata_maintenance_response(
            &store,
            &backend,
            bucket,
            &namespace,
            "events",
            TableMetadataMaintenanceRequest {
                retain_recent_metadata_files: 0,
                delete: true,
                snapshot_expiration: None,
                commit_snapshot_expiration: false,
                compaction: None,
                commit_compaction: false,
            },
        )
        .await
        .expect("metadata maintenance delete should succeed");
        assert_eq!(deleted.cleanup_candidate_locations, vec![old.clone()]);
        assert_eq!(deleted.deletable_metadata_locations, vec![old.clone()]);
        assert!(
            !backend
                .object_exists(bucket, &old)
                .await
                .expect("old metadata lookup should succeed after delete")
        );
    }

    #[tokio::test]
    async fn table_metadata_maintenance_helper_commits_snapshot_expiration() {
        let backend = TestTableCatalogObjectBackend::default();
        let store = crate::table_catalog::ObjectTableCatalogStore::new(backend.clone());
        let bucket = "warehouse";
        let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
        let table = crate::table_catalog::IdentifierSegment::parse("events").expect("table should parse");
        let current = crate::table_catalog::default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");

        seed_object_table_for_metadata_maintenance(&store, &backend, bucket, &namespace, &table, current.clone()).await;
        backend
            .put_json_with_mod_time(
                bucket,
                &current,
                serde_json::json!({
                    "format-version": 2,
                    "table-uuid": "table-uuid",
                    "location": "s3://warehouse/tables/table-id",
                    "last-sequence-number": 2,
                    "last-updated-ms": 2000,
                    "last-column-id": 1,
                    "schemas": [{
                        "type": "struct",
                        "schema-id": 0,
                        "fields": [{
                            "id": 1,
                            "name": "id",
                            "required": true,
                            "type": "long"
                        }]
                    }],
                    "current-schema-id": 0,
                    "partition-specs": [{
                        "spec-id": 0,
                        "fields": []
                    }],
                    "default-spec-id": 0,
                    "sort-orders": [{
                        "order-id": 0,
                        "fields": []
                    }],
                    "default-sort-order-id": 0,
                    "current-snapshot-id": 20,
                    "metadata-log": [],
                    "snapshot-log": [
                        {
                            "timestamp-ms": 1000,
                            "snapshot-id": 10
                        },
                        {
                            "timestamp-ms": 2000,
                            "snapshot-id": 20
                        }
                    ],
                    "snapshots": [
                        {
                            "snapshot-id": 10,
                            "timestamp-ms": 1000,
                            "manifest-list": "s3://warehouse/tables/table-id/metadata/snap-10.avro"
                        },
                        {
                            "snapshot-id": 20,
                            "timestamp-ms": 2000,
                            "manifest-list": "s3://warehouse/tables/table-id/metadata/snap-20.avro"
                        }
                    ],
                    "refs": {
                        "main": {
                            "snapshot-id": 20,
                            "type": "branch"
                        }
                    }
                }),
                Some(OffsetDateTime::UNIX_EPOCH),
            )
            .await;

        let report = table_metadata_maintenance_response(
            &store,
            &backend,
            bucket,
            &namespace,
            "events",
            TableMetadataMaintenanceRequest {
                retain_recent_metadata_files: 0,
                delete: false,
                snapshot_expiration: Some(crate::table_catalog::TableSnapshotExpirationConfig {
                    min_snapshots_to_keep: 1,
                    max_snapshot_age_ms: 1,
                }),
                commit_snapshot_expiration: true,
                compaction: None,
                commit_compaction: false,
            },
        )
        .await
        .expect("snapshot expiration commit should succeed");

        let snapshot_expiration = report
            .snapshot_expiration
            .as_ref()
            .expect("maintenance report should include snapshot expiration");
        assert_eq!(snapshot_expiration.expired_snapshot_ids, vec![10]);
        let committed_location = snapshot_expiration
            .committed_metadata_location
            .as_ref()
            .expect("snapshot expiration commit should report committed metadata")
            .clone();
        assert_ne!(committed_location, current);

        let entry = store
            .load_table(bucket, "analytics", "events")
            .await
            .expect("table lookup should succeed")
            .expect("table should exist");
        assert_eq!(entry.metadata_location, committed_location);
        assert_eq!(entry.generation, 2);

        let committed_object = backend
            .read_object(bucket, &entry.metadata_location)
            .await
            .expect("committed metadata lookup should succeed")
            .expect("committed metadata object should exist");
        let committed_metadata =
            serde_json::from_slice::<serde_json::Value>(&committed_object.data).expect("committed metadata should be valid JSON");
        let snapshots = committed_metadata
            .get("snapshots")
            .and_then(serde_json::Value::as_array)
            .expect("committed metadata should contain snapshots");
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].get("snapshot-id").and_then(serde_json::Value::as_i64), Some(20));
        let snapshot_log = committed_metadata
            .get("snapshot-log")
            .and_then(serde_json::Value::as_array)
            .expect("committed metadata should contain snapshot-log");
        assert_eq!(snapshot_log.len(), 1);
        assert_eq!(
            committed_metadata["metadata-log"][0]["metadata-file"],
            serde_json::Value::String(table_metadata_location_for_client(bucket, &current))
        );
        assert!(
            backend
                .object_exists(bucket, &current)
                .await
                .expect("previous metadata lookup should succeed")
        );
    }

    #[tokio::test]
    async fn table_metadata_maintenance_helper_rejects_snapshot_expiration_manual_review_commit() {
        let backend = TestTableCatalogObjectBackend::default();
        let store = crate::table_catalog::ObjectTableCatalogStore::new(backend.clone());
        let bucket = "warehouse";
        let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
        let table = crate::table_catalog::IdentifierSegment::parse("events").expect("table should parse");
        let current = crate::table_catalog::default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");

        seed_object_table_for_metadata_maintenance(&store, &backend, bucket, &namespace, &table, current.clone()).await;
        backend
            .put_json_with_mod_time(
                bucket,
                &current,
                serde_json::json!({
                    "format-version": 2,
                    "table-uuid": "table-uuid",
                    "location": "s3://warehouse/tables/table-id",
                    "current-snapshot-id": 20,
                    "metadata-log": [],
                    "snapshot-log": [],
                    "snapshots": [
                        {
                            "snapshot-id": 10,
                            "timestamp-ms": 1000,
                            "manifest-list": "s3://warehouse/tables/table-id/metadata/snap-10.avro"
                        },
                        {
                            "snapshot-id": 20,
                            "timestamp-ms": 2000,
                            "manifest-list": "s3://warehouse/tables/table-id/metadata/snap-20.avro"
                        }
                    ],
                    "refs": {
                        "main": {
                            "snapshot-id": 20,
                            "type": "branch"
                        },
                        "audit": {
                            "snapshot-id": 10,
                            "type": "tag"
                        }
                    }
                }),
                Some(OffsetDateTime::UNIX_EPOCH),
            )
            .await;

        let result = table_metadata_maintenance_response(
            &store,
            &backend,
            bucket,
            &namespace,
            "events",
            TableMetadataMaintenanceRequest {
                retain_recent_metadata_files: 0,
                delete: false,
                snapshot_expiration: Some(crate::table_catalog::TableSnapshotExpirationConfig {
                    min_snapshots_to_keep: 1,
                    max_snapshot_age_ms: 1,
                }),
                commit_snapshot_expiration: true,
                compaction: None,
                commit_compaction: false,
            },
        )
        .await;

        assert!(result.is_err());
        let entry = store
            .load_table(bucket, "analytics", "events")
            .await
            .expect("table lookup should succeed")
            .expect("table should exist");
        assert_eq!(entry.metadata_location, current);
        assert_eq!(entry.generation, 1);
    }

    #[tokio::test]
    async fn table_metadata_maintenance_helper_rejects_stale_snapshot_expiration_plan() {
        let backend = TestTableCatalogObjectBackend::default();
        let store = crate::table_catalog::ObjectTableCatalogStore::new(backend.clone());
        let bucket = "warehouse";
        let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
        let table = crate::table_catalog::IdentifierSegment::parse("events").expect("table should parse");
        let current = crate::table_catalog::default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
        let next = crate::table_catalog::default_table_metadata_file_path(&namespace, &table, "00003.metadata.json");

        let metadata = serde_json::json!({
            "format-version": 2,
            "table-uuid": "table-uuid",
            "location": "s3://warehouse/tables/table-id",
            "current-snapshot-id": 20,
            "metadata-log": [],
            "snapshot-log": [],
            "snapshots": [
                {
                    "snapshot-id": 10,
                    "timestamp-ms": 1000,
                    "manifest-list": "s3://warehouse/tables/table-id/metadata/snap-10.avro"
                },
                {
                    "snapshot-id": 20,
                    "timestamp-ms": 2000,
                    "manifest-list": "s3://warehouse/tables/table-id/metadata/snap-20.avro"
                }
            ],
            "refs": {
                "main": {
                    "snapshot-id": 20,
                    "type": "branch"
                }
            }
        });
        seed_object_table_for_metadata_maintenance(&store, &backend, bucket, &namespace, &table, current.clone()).await;
        backend
            .put_json_with_mod_time(bucket, &current, metadata.clone(), Some(OffsetDateTime::UNIX_EPOCH))
            .await;
        backend
            .put_json_with_mod_time(bucket, &next, metadata, Some(OffsetDateTime::UNIX_EPOCH))
            .await;

        let stale_plan = store
            .plan_table_snapshot_expiration(
                bucket,
                "analytics",
                "events",
                crate::table_catalog::TableSnapshotExpirationConfig {
                    min_snapshots_to_keep: 1,
                    max_snapshot_age_ms: 1,
                },
            )
            .await
            .expect("snapshot expiration plan should build");
        store
            .commit_table(crate::table_catalog::TableCommitRequest {
                table_bucket: bucket.to_string(),
                namespace: namespace.public_name(),
                table: table.as_str().to_string(),
                commit_id: "advance-pointer".to_string(),
                idempotency_key: None,
                operation: "append".to_string(),
                expected_version_token: "token-v1".to_string(),
                expected_metadata_location: current,
                new_metadata_location: next,
                requirements: Vec::new(),
                writer: Some("test".to_string()),
            })
            .await
            .expect("pointer advance should succeed");

        let result = commit_table_snapshot_expiration_response(&store, &backend, bucket, &namespace, "events", stale_plan).await;

        assert!(result.is_err());
        let entry = store
            .load_table(bucket, "analytics", "events")
            .await
            .expect("table lookup should succeed")
            .expect("table should exist");
        assert_eq!(entry.generation, 2);
    }

    #[tokio::test]
    async fn table_metadata_maintenance_helper_rejects_delete_with_snapshot_expiration_commit() {
        let backend = TestTableCatalogObjectBackend::default();
        let store = crate::table_catalog::ObjectTableCatalogStore::new(backend.clone());
        let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");

        let result = table_metadata_maintenance_response(
            &store,
            &backend,
            "warehouse",
            &namespace,
            "events",
            TableMetadataMaintenanceRequest {
                retain_recent_metadata_files: 0,
                delete: true,
                snapshot_expiration: Some(crate::table_catalog::TableSnapshotExpirationConfig {
                    min_snapshots_to_keep: 1,
                    max_snapshot_age_ms: 1,
                }),
                commit_snapshot_expiration: true,
                compaction: None,
                commit_compaction: false,
            },
        )
        .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn table_refs_response_reports_current_and_user_defined_refs() {
        let backend = TestTableCatalogObjectBackend::default();
        let store = crate::table_catalog::ObjectTableCatalogStore::new(backend.clone());
        let bucket = "warehouse";
        let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
        let table = crate::table_catalog::IdentifierSegment::parse("events").expect("table should parse");
        let current = crate::table_catalog::default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
        seed_object_table_for_metadata_maintenance(&store, &backend, bucket, &namespace, &table, current.clone()).await;
        backend
            .put_json_with_mod_time(
                bucket,
                &current,
                serde_json::json!({
                    "current-snapshot-id": 10,
                    "refs": {
                        "main": {
                            "snapshot-id": 10,
                            "type": "branch"
                        },
                        "audit": {
                            "snapshot-id": 9,
                            "type": "tag"
                        }
                    }
                }),
                Some(OffsetDateTime::UNIX_EPOCH),
            )
            .await;

        let response = table_refs_response(&store, &backend, bucket, &namespace, "events")
            .await
            .expect("refs response should load");

        assert_eq!(response.current_snapshot_id, Some(10));
        assert_eq!(response.protected_ref_count, 1);
        assert_eq!(response.user_defined_ref_count, 1);
        assert!(response.refs.contains_key("main"));
        assert!(response.refs.contains_key("audit"));
    }

    #[tokio::test]
    async fn external_catalog_bridge_response_lists_supported_operator_bridges() {
        let backend = TestTableCatalogObjectBackend::default();
        let store = crate::table_catalog::ObjectTableCatalogStore::new(backend);
        let bucket = "warehouse";
        let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
        ensure_table_bucket_entry(&store, bucket, true)
            .await
            .expect("table bucket entry should be seeded");
        create_namespace_response(
            &store,
            bucket,
            CreateNamespaceRequest {
                namespace: vec!["analytics".to_string()],
                properties: BTreeMap::new(),
            },
            true,
        )
        .await
        .expect("namespace should be created");
        let response = external_catalog_bridge_response(&store, bucket, &namespace, "events")
            .await
            .expect("bridge response should load");

        assert_eq!(response.status, "bridge-unconfigured");
        assert!(response.unsupported_bridges.is_empty());
        assert_eq!(response.capabilities.len(), 4);
        assert!(
            response
                .capabilities
                .iter()
                .any(|bridge| bridge.catalog == "polaris" && bridge.status == "operator-sync-supported")
        );
    }

    #[tokio::test]
    async fn external_catalog_bridge_persists_identity_and_boundary() {
        let backend = TestTableCatalogObjectBackend::default();
        let store = crate::table_catalog::ObjectTableCatalogStore::new(backend);
        let bucket = "warehouse";
        let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
        ensure_table_bucket_entry(&store, bucket, true)
            .await
            .expect("table bucket entry should be seeded");
        create_namespace_response(
            &store,
            bucket,
            CreateNamespaceRequest {
                namespace: vec!["analytics".to_string()],
                properties: BTreeMap::new(),
            },
            true,
        )
        .await
        .expect("namespace should be created");

        let configured = put_external_catalog_bridge_response(
            &store,
            bucket,
            &namespace,
            "events",
            ExternalCatalogBridgeRequest {
                catalog: "polaris".to_string(),
                external_catalog_id: Some("prod-catalog".to_string()),
                external_namespace: "sales.analytics".to_string(),
                external_table: "orders".to_string(),
                external_table_uuid: Some("table-uuid".to_string()),
                metadata_location: None,
                external_version_token: Some("polaris-v7".to_string()),
                policy_mode: Some("rustfs-authoritative".to_string()),
                credential_mode: Some("rustfs-table-credentials".to_string()),
                sync_mode: Some("manual".to_string()),
                properties: BTreeMap::from([("owner".to_string(), "lakehouse".to_string())]),
            },
        )
        .await
        .expect("bridge should configure");

        assert_eq!(configured.status, "bridge-configured");
        let bridge = configured.bridge.as_ref().expect("bridge state should be returned");
        assert_eq!(bridge.catalog, "polaris");
        assert_eq!(bridge.external_catalog_id.as_deref(), Some("prod-catalog"));
        assert_eq!(bridge.external_namespace, "sales.analytics");
        assert_eq!(bridge.external_table, "orders");
        assert_eq!(bridge.external_version_token.as_deref(), Some("polaris-v7"));
        assert_eq!(bridge.policy_mode, "rustfs-authoritative");
        assert_eq!(bridge.credential_mode, "rustfs-table-credentials");
        assert_eq!(bridge.sync_mode, "manual");

        let loaded = external_catalog_bridge_response(&store, bucket, &namespace, "events")
            .await
            .expect("bridge response should load");
        assert_eq!(loaded.status, "bridge-configured");
        assert!(
            loaded
                .capabilities
                .iter()
                .any(|capability| capability.catalog == "polaris" && capability.status == "operator-sync-supported")
        );
    }

    #[tokio::test]
    async fn external_catalog_bridge_sync_registers_missing_table_from_snapshot() {
        let backend = TestTableCatalogObjectBackend::default();
        let store = crate::table_catalog::ObjectTableCatalogStore::new(backend.clone());
        let bucket = "warehouse";
        let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
        let table = crate::table_catalog::IdentifierSegment::parse("events").expect("table should parse");
        ensure_table_bucket_entry(&store, bucket, true)
            .await
            .expect("table bucket entry should be seeded");
        create_namespace_response(
            &store,
            bucket,
            CreateNamespaceRequest {
                namespace: vec!["analytics".to_string()],
                properties: BTreeMap::new(),
            },
            true,
        )
        .await
        .expect("namespace should be created");
        let metadata_location = crate::table_catalog::default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
        backend
            .put_json(
                bucket,
                &metadata_location,
                test_table_metadata_json("table-uuid", "s3://warehouse/tables/table-id"),
            )
            .await;

        let synced = sync_external_catalog_bridge_response(
            &store,
            &backend,
            bucket,
            &namespace,
            "events",
            ExternalCatalogBridgeSyncRequest {
                catalog: "glue".to_string(),
                external_catalog_id: Some("aws-glue-prod".to_string()),
                external_namespace: "sales".to_string(),
                external_table: "orders".to_string(),
                external_table_uuid: Some("table-uuid".to_string()),
                metadata_location: metadata_location.clone(),
                external_version_token: Some("glue-version-1".to_string()),
                expected_version_token: None,
                expected_metadata_location: None,
                commit_id: Some("external-sync-1".to_string()),
                idempotency_key: Some("external-sync-idempotency-1".to_string()),
                policy_mode: Some("rustfs-authoritative".to_string()),
                credential_mode: Some("rustfs-table-credentials".to_string()),
                rollback_strategy: Some("retain-current-pointer".to_string()),
                properties: BTreeMap::new(),
            },
            true,
        )
        .await
        .expect("external sync should register missing table");

        assert_eq!(synced.action, "registered");
        assert_eq!(
            synced.table.metadata_location,
            table_metadata_location_for_client(bucket, &metadata_location)
        );
        let bridge = synced.bridge.bridge.as_ref().expect("bridge state should be returned");
        assert_eq!(bridge.catalog, "glue");
        assert_eq!(bridge.last_sync_status.as_deref(), Some("synced"));
        assert_eq!(bridge.last_synced_metadata_location.as_deref(), Some(metadata_location.as_str()));
        assert_eq!(bridge.rollback_strategy, "retain-current-pointer");
        let current = store
            .load_table(bucket, "analytics", "events")
            .await
            .expect("table lookup should succeed")
            .expect("table should be registered");
        assert_eq!(current.metadata_location, metadata_location);
        assert_eq!(current.table_uuid, "table-uuid");
    }

    #[tokio::test]
    async fn external_catalog_bridge_sync_recovers_after_registration_bridge_write_failure() {
        let backend = TestTableCatalogObjectBackend::default();
        let store = crate::table_catalog::ObjectTableCatalogStore::new(backend.clone());
        let bucket = "warehouse";
        let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
        let table = crate::table_catalog::IdentifierSegment::parse("events").expect("table should parse");
        ensure_table_bucket_entry(&store, bucket, true)
            .await
            .expect("table bucket entry should be seeded");
        create_namespace_response(
            &store,
            bucket,
            CreateNamespaceRequest {
                namespace: vec!["analytics".to_string()],
                properties: BTreeMap::new(),
            },
            true,
        )
        .await
        .expect("namespace should be created");
        let metadata_location = crate::table_catalog::default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
        backend
            .put_json(
                bucket,
                &metadata_location,
                test_table_metadata_json("table-uuid", "s3://warehouse/tables/table-id"),
            )
            .await;
        let bridge_path =
            crate::table_catalog::TableCatalogObjectPaths::default().external_catalog_bridge_path(bucket, &namespace, &table);
        backend
            .fail_next_put(crate::storage_api::table::RUSTFS_META_BUCKET, &bridge_path)
            .await;
        let sync_request = || ExternalCatalogBridgeSyncRequest {
            catalog: "glue".to_string(),
            external_catalog_id: Some("aws-glue-prod".to_string()),
            external_namespace: "sales".to_string(),
            external_table: "orders".to_string(),
            external_table_uuid: Some("table-uuid".to_string()),
            metadata_location: metadata_location.clone(),
            external_version_token: Some("glue-version-1".to_string()),
            expected_version_token: None,
            expected_metadata_location: None,
            commit_id: None,
            idempotency_key: Some("external-register-retry".to_string()),
            policy_mode: Some("rustfs-authoritative".to_string()),
            credential_mode: Some("rustfs-table-credentials".to_string()),
            rollback_strategy: Some("retain-current-pointer".to_string()),
            properties: BTreeMap::new(),
        };

        sync_external_catalog_bridge_response(&store, &backend, bucket, &namespace, "events", sync_request(), true)
            .await
            .expect_err("bridge write failure should surface after table registration");
        let registered = store
            .load_table(bucket, "analytics", "events")
            .await
            .expect("table lookup should succeed")
            .expect("table registration should remain durable");

        let recovered =
            sync_external_catalog_bridge_response(&store, &backend, bucket, &namespace, "events", sync_request(), true)
                .await
                .expect("exact retry should finish the bridge write");

        assert_eq!(recovered.action, EXTERNAL_CATALOG_ACTION_REGISTERED);
        let bridge = store
            .get_external_catalog_bridge(bucket, "analytics", "events")
            .await
            .expect("bridge lookup should succeed")
            .expect("bridge should be stored");
        assert_eq!(bridge.table_id, registered.table_id);
        assert_eq!(bridge.last_synced_metadata_location.as_deref(), Some(metadata_location.as_str()));
    }

    #[tokio::test]
    async fn external_catalog_bridge_sync_commits_existing_table_pointer() {
        let backend = TestTableCatalogObjectBackend::default();
        let store = crate::table_catalog::ObjectTableCatalogStore::new(backend.clone());
        let bucket = "warehouse";
        let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
        let table = crate::table_catalog::IdentifierSegment::parse("events").expect("table should parse");
        let current_location = crate::table_catalog::default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
        let next_location = crate::table_catalog::default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
        let later_location = crate::table_catalog::default_table_metadata_file_path(&namespace, &table, "00003.metadata.json");
        seed_object_table_for_metadata_maintenance(&store, &backend, bucket, &namespace, &table, current_location.clone()).await;
        let mut current_metadata = test_table_metadata_json("table-uuid", "s3://warehouse/tables/table-id");
        current_metadata["last-sequence-number"] = serde_json::json!(1);
        let mut next_metadata = test_table_metadata_json("table-uuid", "s3://warehouse/tables/table-id");
        next_metadata["last-sequence-number"] = serde_json::json!(2);
        let mut later_metadata = test_table_metadata_json("table-uuid", "s3://warehouse/tables/table-id");
        later_metadata["last-sequence-number"] = serde_json::json!(3);
        backend.put_json(bucket, &current_location, current_metadata).await;
        backend.put_json(bucket, &next_location, next_metadata).await;
        backend.put_json(bucket, &later_location, later_metadata).await;

        let sync_request = || ExternalCatalogBridgeSyncRequest {
            catalog: "hive-metastore".to_string(),
            external_catalog_id: Some("hms-prod".to_string()),
            external_namespace: "sales".to_string(),
            external_table: "orders".to_string(),
            external_table_uuid: Some("table-uuid".to_string()),
            metadata_location: table_metadata_location_for_client(bucket, &next_location),
            external_version_token: Some("hms-version-2".to_string()),
            expected_version_token: Some("token-v1".to_string()),
            expected_metadata_location: Some(table_metadata_location_for_client(bucket, &current_location)),
            commit_id: None,
            idempotency_key: Some("external-sync-idempotency-2".to_string()),
            policy_mode: Some("rustfs-authoritative".to_string()),
            credential_mode: Some("rustfs-table-credentials".to_string()),
            rollback_strategy: Some("retain-current-pointer".to_string()),
            properties: BTreeMap::new(),
        };
        let mut non_idempotent_request = sync_request();
        non_idempotent_request.idempotency_key = None;
        let error =
            sync_external_catalog_bridge_response(&store, &backend, bucket, &namespace, "events", non_idempotent_request, true)
                .await
                .expect_err("a pointer-changing external sync must carry a stable request identity");
        assert_eq!(error.code(), &S3ErrorCode::InvalidRequest);

        let synced = sync_external_catalog_bridge_response(&store, &backend, bucket, &namespace, "events", sync_request(), true)
            .await
            .expect("external sync should commit existing table");

        assert_eq!(synced.action, "committed");
        assert_eq!(synced.table.metadata_location, table_metadata_location_for_client(bucket, &next_location));
        let current = store
            .load_table(bucket, "analytics", "events")
            .await
            .expect("table lookup should succeed")
            .expect("table should exist");
        assert_eq!(current.metadata_location, next_location);
        assert_eq!(current.generation, 2);

        let replayed =
            sync_external_catalog_bridge_response(&store, &backend, bucket, &namespace, "events", sync_request(), true)
                .await
                .expect("idempotency-only external sync retry should replay the published result");

        assert_eq!(replayed.action, EXTERNAL_CATALOG_ACTION_COMMITTED);
        assert_eq!(replayed.table.metadata_location, synced.table.metadata_location);
        assert_eq!(replayed.table.config, synced.table.config);
        let replayed_current = store
            .load_table(bucket, "analytics", "events")
            .await
            .expect("table lookup should succeed")
            .expect("table should exist");
        assert_eq!(replayed_current.version_token, current.version_token);
        assert_eq!(replayed_current.generation, current.generation);

        let later = sync_external_catalog_bridge_response(
            &store,
            &backend,
            bucket,
            &namespace,
            "events",
            ExternalCatalogBridgeSyncRequest {
                catalog: "hive-metastore".to_string(),
                external_catalog_id: Some("hms-prod".to_string()),
                external_namespace: "sales".to_string(),
                external_table: "orders".to_string(),
                external_table_uuid: Some("table-uuid".to_string()),
                metadata_location: table_metadata_location_for_client(bucket, &later_location),
                external_version_token: Some("hms-version-3".to_string()),
                expected_version_token: Some(current.version_token.clone()),
                expected_metadata_location: Some(table_metadata_location_for_client(bucket, &next_location)),
                commit_id: None,
                idempotency_key: Some("external-sync-idempotency-3".to_string()),
                policy_mode: Some("rustfs-authoritative".to_string()),
                credential_mode: Some("rustfs-table-credentials".to_string()),
                rollback_strategy: Some("retain-current-pointer".to_string()),
                properties: BTreeMap::new(),
            },
            true,
        )
        .await
        .expect("a later external sync should commit");
        assert_eq!(later.table.metadata_location, table_metadata_location_for_client(bucket, &later_location));

        let historical =
            sync_external_catalog_bridge_response(&store, &backend, bucket, &namespace, "events", sync_request(), true)
                .await
                .expect("a historical retry should return the latest published state");
        assert_eq!(historical.table.metadata_location, later.table.metadata_location);
        assert_eq!(historical.table.config, later.table.config);
        let bridge = historical
            .bridge
            .bridge
            .expect("historical retry should preserve the current bridge");
        assert_eq!(bridge.last_synced_metadata_location.as_deref(), Some(later_location.as_str()));
        assert_eq!(bridge.external_version_token.as_deref(), Some("hms-version-3"));
    }

    #[tokio::test]
    async fn external_catalog_bridge_sync_does_not_register_after_captured_table_is_dropped() {
        let backend = TestTableCatalogObjectBackend::default();
        let store = crate::table_catalog::ObjectTableCatalogStore::new(backend.clone());
        let bucket = "warehouse";
        let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
        let table = crate::table_catalog::IdentifierSegment::parse("events").expect("table should parse");
        let current_location = crate::table_catalog::default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
        let next_location = crate::table_catalog::default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
        seed_object_table_for_metadata_maintenance(&store, &backend, bucket, &namespace, &table, current_location.clone()).await;
        let target_metadata = test_table_metadata_json("table-uuid", "s3://warehouse/tables/table-id");
        backend.put_json(bucket, &next_location, target_metadata.clone()).await;
        let captured_current = store
            .load_table(bucket, "analytics", "events")
            .await
            .expect("table lookup should succeed")
            .expect("table should exist before the race");
        store
            .drop_table(bucket, "analytics", "events")
            .await
            .expect("concurrent table drop should succeed");

        let result = sync_external_catalog_bridge_response_with_snapshot(
            &store,
            &backend,
            bucket,
            &namespace,
            "events",
            ExternalCatalogBridgeSyncSnapshot {
                request: ExternalCatalogBridgeSyncRequest {
                    catalog: "glue".to_string(),
                    external_catalog_id: Some("aws-glue-prod".to_string()),
                    external_namespace: "sales".to_string(),
                    external_table: "orders".to_string(),
                    external_table_uuid: Some("table-uuid".to_string()),
                    metadata_location: next_location,
                    external_version_token: Some("glue-version-2".to_string()),
                    expected_version_token: Some(captured_current.version_token.clone()),
                    expected_metadata_location: Some(captured_current.metadata_location.clone()),
                    commit_id: Some("external-sync-race".to_string()),
                    idempotency_key: Some("external-sync-race-request".to_string()),
                    policy_mode: Some("rustfs-authoritative".to_string()),
                    credential_mode: Some("rustfs-table-credentials".to_string()),
                    rollback_strategy: Some("retain-current-pointer".to_string()),
                    properties: BTreeMap::new(),
                },
                target_metadata,
                current: Some(captured_current),
            },
        )
        .await;

        assert!(result.is_err());
        assert!(
            store
                .load_table(bucket, "analytics", "events")
                .await
                .expect("table lookup should succeed")
                .is_none(),
            "sync must not fall back to registration after the authorized table state changes"
        );
    }

    #[tokio::test]
    async fn external_catalog_bridge_sync_conflicts_leave_pointer_unchanged() {
        let backend = TestTableCatalogObjectBackend::default();
        let store = crate::table_catalog::ObjectTableCatalogStore::new(backend.clone());
        let bucket = "warehouse";
        let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
        let table = crate::table_catalog::IdentifierSegment::parse("events").expect("table should parse");
        let current_location = crate::table_catalog::default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
        let next_location = crate::table_catalog::default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
        seed_object_table_for_metadata_maintenance(&store, &backend, bucket, &namespace, &table, current_location.clone()).await;
        backend
            .put_json(
                bucket,
                &current_location,
                test_table_metadata_json("table-uuid", "s3://warehouse/tables/table-id"),
            )
            .await;
        backend
            .put_json(
                bucket,
                &next_location,
                test_table_metadata_json("different-table-uuid", "s3://warehouse/tables/table-id"),
            )
            .await;

        let result = sync_external_catalog_bridge_response(
            &store,
            &backend,
            bucket,
            &namespace,
            "events",
            ExternalCatalogBridgeSyncRequest {
                catalog: "dlf".to_string(),
                external_catalog_id: Some("dlf-prod".to_string()),
                external_namespace: "sales".to_string(),
                external_table: "orders".to_string(),
                external_table_uuid: Some("different-table-uuid".to_string()),
                metadata_location: next_location.clone(),
                external_version_token: Some("dlf-version-2".to_string()),
                expected_version_token: Some("token-v1".to_string()),
                expected_metadata_location: Some(current_location.clone()),
                commit_id: Some("external-sync-3".to_string()),
                idempotency_key: Some("external-sync-idempotency-3".to_string()),
                policy_mode: Some("rustfs-authoritative".to_string()),
                credential_mode: Some("rustfs-table-credentials".to_string()),
                rollback_strategy: Some("retain-current-pointer".to_string()),
                properties: BTreeMap::new(),
            },
            true,
        )
        .await;

        assert!(result.is_err());
        let current = store
            .load_table(bucket, "analytics", "events")
            .await
            .expect("table lookup should succeed")
            .expect("table should exist");
        assert_eq!(current.metadata_location, current_location);
        assert_eq!(current.version_token, "token-v1");
    }

    #[test]
    fn commit_requirements_reject_mismatched_table_uuid() {
        let metadata = serde_json::json!({
            "table-uuid": "actual-table-uuid"
        });
        let requirements = vec![serde_json::json!({
            "type": "assert-table-uuid",
            "uuid": "stale-table-uuid"
        })];

        assert!(validate_table_commit_requirements(&metadata, &requirements).is_err());
    }

    #[test]
    fn snapshot_conflict_requirements_validate_current_snapshot_id() {
        let metadata = serde_json::json!({
            "current-snapshot-id": 10
        });

        let matching = vec![serde_json::json!({
            "type": "assert-current-snapshot-id",
            "snapshot-id": 10
        })];
        validate_table_commit_requirements(&metadata, &matching).expect("matching current snapshot should pass");

        let stale = vec![serde_json::json!({
            "type": "assert-current-snapshot-id",
            "snapshot-id": 9
        })];
        assert!(validate_table_commit_requirements(&metadata, &stale).is_err());

        let no_snapshot_metadata = serde_json::json!({});
        let create_like = vec![serde_json::json!({
            "type": "assert-current-snapshot-id",
            "snapshot-id": null
        })];
        validate_table_commit_requirements(&no_snapshot_metadata, &create_like)
            .expect("null current snapshot requirement should pass when no current snapshot exists");
    }

    #[test]
    fn table_commit_rejects_refs_to_missing_snapshots() {
        let metadata = serde_json::json!({
            "current-snapshot-id": 10,
            "snapshots": [
                {
                    "snapshot-id": 10,
                    "sequence-number": 1,
                    "timestamp-ms": 1234,
                    "manifest-list": "s3://warehouse/tables/table-id/metadata/snap-10.avro",
                    "summary": {
                        "operation": "append"
                    }
                }
            ],
            "refs": {
                "main": {
                    "snapshot-id": 10,
                    "type": "branch"
                }
            },
            "metadata-log": []
        });
        let updates = vec![serde_json::json!({
            "action": "set-snapshot-ref",
            "ref-name": "main",
            "snapshot-id": 999,
            "type": "branch"
        })];

        let error = apply_table_commit_updates(metadata, &updates, "s3://warehouse/tables/table-id/metadata/v1.metadata.json")
            .expect_err("a snapshot ref must not target a missing snapshot");

        assert_eq!(error.code(), &S3ErrorCode::InvalidRequest);
    }

    #[test]
    fn table_commit_rejects_dangling_schema_spec_and_sort_order_ids() {
        let metadata = serde_json::json!({
            "schemas": [{"schema-id": 0, "type": "struct", "fields": []}],
            "current-schema-id": 0,
            "partition-specs": [{"spec-id": 0, "fields": []}],
            "default-spec-id": 0,
            "sort-orders": [{"order-id": 0, "fields": []}],
            "default-sort-order-id": 0,
            "metadata-log": []
        });
        let updates = [
            serde_json::json!({"action": "set-current-schema", "schema-id": 999}),
            serde_json::json!({"action": "set-default-spec", "spec-id": 999}),
            serde_json::json!({"action": "set-default-sort-order", "sort-order-id": 999}),
        ];

        for update in updates {
            let error = apply_table_commit_updates(
                metadata.clone(),
                &[update],
                "s3://warehouse/tables/table-id/metadata/v1.metadata.json",
            )
            .expect_err("a current or default id must reference existing metadata");

            assert_eq!(error.code(), &S3ErrorCode::InvalidRequest);
        }
    }

    #[test]
    fn table_metadata_reference_validation_rejects_malformed_typed_fields() {
        let metadata = serde_json::json!({
            "schemas": [{"schema-id": 0, "type": "struct", "fields": []}],
            "current-schema-id": 0,
            "snapshots": [{"snapshot-id": 10, "schema-id": 0}],
            "current-snapshot-id": 10,
            "refs": {
                "main": {
                    "snapshot-id": 10,
                    "type": "branch"
                }
            }
        });
        let cases = [
            ("current-snapshot-id", serde_json::json!("10"), "current-snapshot-id must be an integer"),
            ("refs", serde_json::json!([]), "refs must be an object"),
        ];

        for (field, value, expected_message) in cases {
            let mut malformed = metadata.clone();
            malformed[field] = value;
            let error =
                validate_table_metadata_references(&malformed).expect_err("malformed table metadata field must be rejected");
            assert_eq!(error.message(), Some(expected_message));
        }

        let mut malformed_snapshot_schema = metadata;
        malformed_snapshot_schema["snapshots"][0]["schema-id"] = serde_json::json!("0");
        let error = validate_table_metadata_references(&malformed_snapshot_schema)
            .expect_err("a malformed snapshot schema-id must be rejected");
        assert_eq!(error.message(), Some("snapshot schema-id must be an integer"));

        let metadata_without_current_snapshot = serde_json::json!({
            "schemas": [{"schema-id": 0, "type": "struct", "fields": []}],
            "current-schema-id": 0,
            "snapshots": [],
            "current-snapshot-id": null,
            "refs": null
        });
        validate_table_metadata_references(&metadata_without_current_snapshot)
            .expect("Iceberg null snapshot fields should represent a table without a current snapshot");
    }

    #[test]
    fn table_metadata_validation_requires_complete_v2_core_fields() {
        let metadata = test_table_metadata_json("table-uuid", "s3://warehouse/tables/table-id");
        for required_field in [
            "last-updated-ms",
            "last-column-id",
            "last-sequence-number",
            "schemas",
            "current-schema-id",
            "partition-specs",
            "default-spec-id",
            "last-partition-id",
            "sort-orders",
            "default-sort-order-id",
        ] {
            let mut incomplete = metadata.clone();
            incomplete
                .as_object_mut()
                .expect("metadata should be an object")
                .remove(required_field);
            validate_supported_table_metadata(&incomplete)
                .expect_err("missing required Iceberg v2 metadata field must be rejected");
        }
    }

    #[test]
    fn table_metadata_validation_bounds_snapshot_sequence_numbers() {
        let mut metadata = test_table_metadata_json("table-uuid", "s3://warehouse/tables/table-id");
        metadata["last-sequence-number"] = serde_json::Value::from(1);
        metadata["snapshots"] = serde_json::json!([{
            "snapshot-id": 10,
            "sequence-number": 2,
            "timestamp-ms": 1,
            "manifest-list": "s3://warehouse/tables/table-id/metadata/snap-10.avro",
            "summary": {"operation": "append"}
        }]);

        let error = validate_supported_table_metadata(&metadata)
            .expect_err("snapshot sequence-number must not exceed last-sequence-number");

        assert_eq!(error.code(), &S3ErrorCode::InvalidRequest);
    }

    #[test]
    fn view_commit_rejects_a_dangling_current_version_id() {
        let metadata = serde_json::json!({
            "schemas": [{"schema-id": 0, "type": "struct", "fields": []}],
            "current-schema-id": 0,
            "versions": [{"version-id": 1, "schema-id": 0, "representations": []}],
            "current-version-id": 1,
            "version-log": [],
            "metadata-log": []
        });

        let error = apply_view_commit_updates_at(
            metadata,
            &[serde_json::json!({
                "action": "set-current-view-version",
                "view-version-id": 999
            })],
            "s3://warehouse/views/analytics/recent_events/metadata/v1.metadata.json",
            0,
        )
        .expect_err("the current view version must exist");

        assert_eq!(error.code(), &S3ErrorCode::InvalidRequest);
    }

    #[test]
    fn view_commit_rejects_unsupported_format_version_upgrade() {
        let metadata = serde_json::json!({
            "format-version": 1,
            "schemas": [{"schema-id": 0, "type": "struct", "fields": []}],
            "current-schema-id": 0,
            "versions": [{"version-id": 1, "schema-id": 0, "representations": []}],
            "current-version-id": 1,
            "version-log": [],
            "metadata-log": []
        });

        let error = apply_view_commit_updates_at(
            metadata,
            &[serde_json::json!({
                "action": "upgrade-format-version",
                "format-version": 2
            })],
            "s3://warehouse/views/analytics/recent_events/metadata/v1.metadata.json",
            0,
        )
        .expect_err("Iceberg view format-version 2 is unsupported");

        assert_eq!(error.code(), &S3ErrorCode::Custom(ICEBERG_ERROR_UNSUPPORTED_OPERATION.into()));
        assert_eq!(error.status_code(), Some(StatusCode::NOT_ACCEPTABLE));
    }

    #[test]
    fn commit_identifier_must_match_the_resource_url() {
        let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
        let matching = RestTableIdentifier {
            namespace: vec!["analytics".to_string()],
            name: "events".to_string(),
        };
        validate_rest_commit_identifier(Some(&matching), &namespace, "events").expect("matching identifier should be accepted");

        let wrong_namespace = RestTableIdentifier {
            namespace: vec!["staging".to_string()],
            name: "events".to_string(),
        };
        let wrong_name = RestTableIdentifier {
            namespace: vec!["analytics".to_string()],
            name: "other".to_string(),
        };
        assert_eq!(
            validate_rest_commit_identifier(Some(&wrong_namespace), &namespace, "events")
                .expect_err("namespace mismatch should fail")
                .code(),
            &S3ErrorCode::InvalidRequest
        );
        assert_eq!(
            validate_rest_commit_identifier(Some(&wrong_name), &namespace, "events")
                .expect_err("name mismatch should fail")
                .code(),
            &S3ErrorCode::InvalidRequest
        );
    }

    #[test]
    fn snapshot_conflict_rejects_stale_parent_or_sequence_number() {
        let metadata = serde_json::json!({
            "current-snapshot-id": 10,
            "last-sequence-number": 4,
            "snapshots": [
                {
                    "snapshot-id": 10,
                    "sequence-number": 4,
                    "timestamp-ms": 1234,
                    "manifest-list": "s3://warehouse/tables/table-id/metadata/snap-10.avro",
                    "summary": {
                        "operation": "append"
                    }
                }
            ],
            "snapshot-log": [],
            "metadata-log": []
        });

        let stale_parent = vec![serde_json::json!({
            "action": "add-snapshot",
            "snapshot": {
                "snapshot-id": 11,
                "parent-snapshot-id": 9,
                "sequence-number": 5,
                "timestamp-ms": 2234,
                "manifest-list": "s3://warehouse/tables/table-id/metadata/snap-11.avro",
                "summary": {
                    "operation": "append"
                }
            }
        })];
        assert!(apply_table_commit_updates(metadata.clone(), &stale_parent, "metadata/00001.metadata.json").is_err());

        let stale_sequence = vec![serde_json::json!({
            "action": "add-snapshot",
            "snapshot": {
                "snapshot-id": 11,
                "parent-snapshot-id": 10,
                "sequence-number": 4,
                "timestamp-ms": 2234,
                "manifest-list": "s3://warehouse/tables/table-id/metadata/snap-11.avro",
                "summary": {
                    "operation": "append"
                }
            }
        })];
        assert!(apply_table_commit_updates(metadata, &stale_sequence, "metadata/00001.metadata.json").is_err());
    }

    #[test]
    fn snapshot_updates_apply_v1_sequence_rules_and_reject_unsupported_versions() {
        let add_snapshot = serde_json::json!({
            "action": "add-snapshot",
            "snapshot": {
                "snapshot-id": 10,
                "timestamp-ms": 1234,
                "manifest-list": "s3://warehouse/tables/table-id/metadata/snap-10.avro",
                "summary": {"operation": "append"}
            }
        });
        let set_main = serde_json::json!({
            "action": "set-snapshot-ref",
            "ref-name": "main",
            "snapshot-id": 10,
            "type": "branch"
        });
        let base_metadata = |format_version| {
            serde_json::json!({
                "format-version": format_version,
                "last-sequence-number": 0,
                "snapshots": [],
                "refs": {},
                "snapshot-log": [],
                "metadata-log": []
            })
        };

        let v1 = apply_table_commit_updates(
            base_metadata(1),
            &[add_snapshot.clone(), set_main.clone()],
            "metadata/00001.metadata.json",
        )
        .expect("v1 snapshot may omit sequence-number");
        assert_eq!(v1["current-snapshot-id"], 10);
        assert!(v1["snapshots"][0].get("sequence-number").is_none());

        let v2_error =
            apply_table_commit_updates(base_metadata(2), &[add_snapshot.clone(), set_main], "metadata/00001.metadata.json")
                .expect_err("v2 snapshot must include sequence-number");
        assert_eq!(v2_error.code(), &S3ErrorCode::InvalidRequest);

        let unsupported = apply_table_commit_updates(base_metadata(3), &[add_snapshot], "metadata/00001.metadata.json")
            .expect_err("format version 3 is not implemented");
        assert_eq!(unsupported.code(), &S3ErrorCode::Custom(ICEBERG_ERROR_UNSUPPORTED_OPERATION.into()));

        let remove_main = apply_table_commit_updates(
            base_metadata(2),
            &[serde_json::json!({
                "action": "remove-snapshot-ref",
                "ref-name": "main"
            })],
            "metadata/00001.metadata.json",
        )
        .expect_err("generic commits must not remove main");
        assert_eq!(remove_main.code(), &S3ErrorCode::InvalidRequest);
    }

    #[test]
    fn snapshot_updates_advance_only_the_main_branch_and_record_main_history() {
        let metadata = serde_json::json!({
            "current-snapshot-id": 10,
            "last-sequence-number": 4,
            "snapshots": [
                {
                    "snapshot-id": 5,
                    "sequence-number": 3,
                    "timestamp-ms": 1000,
                    "manifest-list": "s3://warehouse/tables/table-id/metadata/snap-5.avro",
                    "summary": {"operation": "append"}
                },
                {
                    "snapshot-id": 10,
                    "sequence-number": 4,
                    "timestamp-ms": 1234,
                    "manifest-list": "s3://warehouse/tables/table-id/metadata/snap-10.avro",
                    "summary": {"operation": "append"}
                }
            ],
            "refs": {
                "main": {"snapshot-id": 10, "type": "branch"}
            },
            "snapshot-log": [
                {"timestamp-ms": 1234, "snapshot-id": 10}
            ],
            "metadata-log": []
        });
        let add_snapshot = serde_json::json!({
            "action": "add-snapshot",
            "snapshot": {
                "snapshot-id": 11,
                "parent-snapshot-id": 5,
                "sequence-number": 5,
                "timestamp-ms": 2234,
                "manifest-list": "s3://warehouse/tables/table-id/metadata/snap-11.avro",
                "summary": {"operation": "append"}
            }
        });
        let branch = apply_table_commit_updates_at(
            metadata.clone(),
            &[
                add_snapshot.clone(),
                serde_json::json!({
                    "action": "set-snapshot-ref",
                    "ref-name": "audit",
                    "snapshot-id": 11,
                    "type": "branch"
                }),
            ],
            "metadata/00001.metadata.json",
            3000,
        )
        .expect("a branch snapshot may descend from an existing non-current snapshot");
        assert_eq!(branch["current-snapshot-id"], 10);
        assert_eq!(branch["refs"]["main"]["snapshot-id"], 10);
        assert_eq!(branch["refs"]["audit"]["snapshot-id"], 11);
        assert_eq!(
            branch["snapshot-log"]
                .as_array()
                .expect("snapshot log should be an array")
                .len(),
            1
        );

        let main = apply_table_commit_updates_at(
            metadata,
            &[
                add_snapshot,
                serde_json::json!({
                    "action": "set-snapshot-ref",
                    "ref-name": "main",
                    "snapshot-id": 11,
                    "type": "branch"
                }),
            ],
            "metadata/00001.metadata.json",
            3000,
        )
        .expect("advancing main should succeed");
        assert_eq!(main["current-snapshot-id"], 11);
        assert_eq!(main["snapshot-log"][1]["snapshot-id"], 11);
        assert_eq!(main["snapshot-log"][1]["timestamp-ms"], 3000);
    }

    #[test]
    fn snapshot_updates_reject_missing_timestamps_malformed_removals_and_main_tags() {
        let metadata = serde_json::json!({
            "current-snapshot-id": 10,
            "last-sequence-number": 1,
            "snapshots": [{
                "snapshot-id": 10,
                "sequence-number": 1,
                "timestamp-ms": 1234,
                "manifest-list": "s3://warehouse/tables/table-id/metadata/snap-10.avro",
                "summary": {"operation": "append"}
            }],
            "refs": {"main": {"snapshot-id": 10, "type": "branch"}},
            "snapshot-log": [],
            "metadata-log": []
        });
        let missing_timestamp = serde_json::json!({
            "action": "add-snapshot",
            "snapshot": {
                "snapshot-id": 11,
                "parent-snapshot-id": 10,
                "sequence-number": 2,
                "manifest-list": "s3://warehouse/tables/table-id/metadata/snap-11.avro",
                "summary": {"operation": "append"}
            }
        });
        let malformed_removal = serde_json::json!({
            "action": "remove-snapshots",
            "snapshot-ids": ["10"]
        });
        let malformed_parent = serde_json::json!({
            "action": "add-snapshot",
            "snapshot": {
                "snapshot-id": 11,
                "parent-snapshot-id": "10",
                "sequence-number": 2,
                "timestamp-ms": 2234,
                "manifest-list": "s3://warehouse/tables/table-id/metadata/snap-11.avro",
                "summary": {"operation": "append"}
            }
        });
        let main_tag = serde_json::json!({
            "action": "set-snapshot-ref",
            "ref-name": "main",
            "snapshot-id": 10,
            "type": "tag"
        });

        for (update, expected) in [
            (missing_timestamp, "snapshot timestamp-ms must be an integer"),
            (malformed_parent, "snapshot parent-snapshot-id must be an integer"),
            (malformed_removal, "remove-snapshots snapshot-ids must contain integers"),
            (main_tag, "main snapshot ref must be a branch"),
        ] {
            let error = apply_table_commit_updates(metadata.clone(), &[update], "metadata/00001.metadata.json")
                .expect_err("malformed snapshot update should fail");
            assert_eq!(error.message(), Some(expected));
        }
    }

    #[test]
    fn snapshot_conflict_rejects_unknown_snapshot_operations() {
        let metadata = serde_json::json!({
            "current-snapshot-id": 10,
            "last-sequence-number": 4,
            "snapshots": [
                {
                    "snapshot-id": 10,
                    "sequence-number": 4,
                    "timestamp-ms": 1234,
                    "manifest-list": "s3://warehouse/tables/table-id/metadata/snap-10.avro",
                    "summary": {
                        "operation": "append"
                    }
                }
            ],
            "snapshot-log": [],
            "metadata-log": []
        });

        let updates = vec![serde_json::json!({
            "action": "add-snapshot",
            "snapshot": {
                "snapshot-id": 11,
                "parent-snapshot-id": 10,
                "sequence-number": 5,
                "timestamp-ms": 2234,
                "manifest-list": "s3://warehouse/tables/table-id/metadata/snap-11.avro",
                "summary": {
                    "operation": "unknown"
                }
            }
        })];
        assert!(apply_table_commit_updates(metadata, &updates, "metadata/00001.metadata.json").is_err());
    }

    #[tokio::test]
    async fn row_level_conflict_uses_the_declared_branch_parent_snapshot() {
        let store = TestTableCatalogStore::default();
        let metadata_backend = TestTableCatalogObjectBackend::default();
        let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
        let created = create_standard_events_table(&store, &metadata_backend, &namespace).await;
        let table_location = created.metadata["location"]
            .as_str()
            .expect("created metadata should have table location");
        let branch_manifest_list = format!("{table_location}/metadata/snap-10.avro");
        let main_manifest_list = format!("{table_location}/metadata/snap-20.avro");
        let overwrite_manifest_list = format!("{table_location}/metadata/snap-30.avro");
        let branch_data_file = format!("{table_location}/data/branch.parquet");
        let main_data_file = format!("{table_location}/data/main.parquet");
        seed_test_snapshot_manifest(
            &metadata_backend,
            "warehouse",
            &branch_manifest_list,
            10,
            1,
            &[(&branch_data_file, 0, 1, 10, 1)],
        )
        .await;
        seed_test_snapshot_manifest(
            &metadata_backend,
            "warehouse",
            &main_manifest_list,
            20,
            2,
            &[(&main_data_file, 0, 1, 20, 2)],
        )
        .await;
        seed_test_snapshot_manifest(
            &metadata_backend,
            "warehouse",
            &overwrite_manifest_list,
            30,
            3,
            &[(&branch_data_file, 0, 2, 30, 1)],
        )
        .await;
        let current_metadata = serde_json::json!({
            "current-snapshot-id": 20,
            "snapshots": [
                {
                    "snapshot-id": 10,
                    "sequence-number": 1,
                    "manifest-list": branch_manifest_list
                },
                {
                    "snapshot-id": 20,
                    "sequence-number": 2,
                    "manifest-list": main_manifest_list
                }
            ]
        });
        let entry = store
            .load_table("warehouse", "analytics", "events")
            .await
            .expect("table lookup should succeed")
            .expect("table should exist");
        let table = crate::table_catalog::IdentifierSegment::parse("events").expect("table should parse");
        let context = SnapshotReadContext {
            metadata_backend: &metadata_backend,
            bucket: "warehouse",
            namespace: &namespace,
            table: &table,
            entry: &entry,
        };
        let updates = vec![serde_json::json!({
            "action": "add-snapshot",
            "snapshot": {
                "snapshot-id": 30,
                "parent-snapshot-id": 10,
                "sequence-number": 3,
                "timestamp-ms": 3000,
                "manifest-list": overwrite_manifest_list,
                "summary": {
                    "operation": "overwrite"
                }
            }
        })];

        validate_table_snapshot_commit_conflicts(&context, &current_metadata, &current_metadata, &updates)
            .await
            .expect("a branch commit should validate deletes against its declared parent snapshot");
    }

    #[tokio::test]
    async fn row_level_conflict_allows_overwrite_when_deleted_file_is_current() {
        let store = TestTableCatalogStore::default();
        let metadata_backend = TestTableCatalogObjectBackend::default();
        let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
        let created = create_standard_events_table(&store, &metadata_backend, &namespace).await;
        let table_location = created.metadata["location"]
            .as_str()
            .expect("created metadata should have table location");
        let current_manifest_list = format!("{table_location}/metadata/snap-10.avro");
        let old_data_file = format!("{table_location}/data/part-10.parquet");
        seed_test_snapshot_manifest(
            &metadata_backend,
            "warehouse",
            &current_manifest_list,
            10,
            1,
            &[(&old_data_file, 0, 1, 10, 1)],
        )
        .await;
        let append_request: RestCommitTableRequest = serde_json::from_value(serde_json::json!({
            "requirements": [],
            "updates": [
                {
                    "action": "add-snapshot",
                    "snapshot": {
                        "snapshot-id": 10,
                        "sequence-number": 1,
                        "timestamp-ms": 1234,
                        "manifest-list": current_manifest_list,
                        "summary": {
                            "operation": "append"
                        }
                    }
                },
                {
                    "action": "set-snapshot-ref",
                    "ref-name": "main",
                    "snapshot-id": 10,
                    "type": "branch"
                }
            ]
        }))
        .expect("append request should parse");
        commit_table_response(&store, &metadata_backend, "warehouse", &namespace, "events", append_request)
            .await
            .expect("append commit should succeed");

        let overwrite_manifest_list = format!("{table_location}/metadata/snap-11.avro");
        let replacement_data_file = format!("{table_location}/data/part-11.parquet");
        seed_test_snapshot_manifest(
            &metadata_backend,
            "warehouse",
            &overwrite_manifest_list,
            11,
            2,
            &[(&old_data_file, 0, 2, 11, 2), (&replacement_data_file, 0, 1, 11, 2)],
        )
        .await;
        let overwrite_request_json = serde_json::json!({
            "requirements": [
                {
                    "type": "assert-current-snapshot-id",
                    "snapshot-id": 10
                }
            ],
            "updates": [
                {
                    "action": "add-snapshot",
                    "snapshot": {
                        "snapshot-id": 11,
                        "parent-snapshot-id": 10,
                        "sequence-number": 2,
                        "timestamp-ms": 2234,
                        "manifest-list": overwrite_manifest_list,
                        "summary": {
                            "operation": "overwrite"
                        }
                    }
                },
                {
                    "action": "set-snapshot-ref",
                    "ref-name": "main",
                    "snapshot-id": 11,
                    "type": "branch"
                }
            ]
        });
        let stale_overwrite_request: RestCommitTableRequest =
            serde_json::from_value(overwrite_request_json.clone()).expect("stale overwrite request should parse");

        let error = commit_table_response(&store, &metadata_backend, "warehouse", &namespace, "events", stale_overwrite_request)
            .await
            .expect_err("deleted file sequence must not change");
        assert_eq!(error.code(), &s3s::S3ErrorCode::InvalidRequest);

        seed_test_snapshot_manifest(
            &metadata_backend,
            "warehouse",
            &overwrite_manifest_list,
            11,
            2,
            &[(&old_data_file, 0, 2, 11, 1), (&replacement_data_file, 0, 1, 11, 2)],
        )
        .await;
        let overwrite_request: RestCommitTableRequest =
            serde_json::from_value(overwrite_request_json).expect("overwrite request should parse");

        let commit = commit_table_response(&store, &metadata_backend, "warehouse", &namespace, "events", overwrite_request)
            .await
            .expect("overwrite commit should pass manifest conflict validation");

        assert_eq!(commit.metadata["current-snapshot-id"], 11);
        assert_eq!(commit.metadata["last-sequence-number"], 2);
    }

    #[tokio::test]
    async fn row_level_conflict_rejects_duplicate_manifest_and_file_references() {
        let store = TestTableCatalogStore::default();
        let metadata_backend = TestTableCatalogObjectBackend::default();
        let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
        let created = create_standard_events_table(&store, &metadata_backend, &namespace).await;
        let table_location = created.metadata["location"]
            .as_str()
            .expect("created metadata should have table location");
        let manifest_list = format!("{table_location}/metadata/snap-10.avro");
        let manifest = format!("{table_location}/metadata/manifest-10.avro");
        let data_file = format!("{table_location}/data/part-10.parquet");
        let manifest_list_key = test_snapshot_object_key("warehouse", &manifest_list);
        metadata_backend
            .put_bytes(
                "warehouse",
                &manifest_list_key,
                test_manifest_list_avro_entries(&[(&manifest, 1, 10), (&manifest, 1, 10)]),
            )
            .await;
        seed_test_manifest(&metadata_backend, "warehouse", &manifest, &[(&data_file, 0, 1, 10, 1)]).await;
        let request = serde_json::from_value(serde_json::json!({
            "requirements": [],
            "updates": [{
                "action": "add-snapshot",
                "snapshot": {
                    "snapshot-id": 10,
                    "sequence-number": 1,
                    "timestamp-ms": 1234,
                    "manifest-list": manifest_list,
                    "summary": {"operation": "append"}
                }
            }]
        }))
        .expect("duplicate manifest request should parse");
        let error = commit_table_response(&store, &metadata_backend, "warehouse", &namespace, "events", request)
            .await
            .expect_err("duplicate manifest references must fail");
        assert_eq!(error.code(), &S3ErrorCode::InvalidRequest);

        let duplicate_file_manifest = format!("{table_location}/metadata/manifest-duplicate-files.avro");
        seed_test_manifest(
            &metadata_backend,
            "warehouse",
            &duplicate_file_manifest,
            &[(&data_file, 0, 1, 11, 1), (&data_file, 0, 1, 11, 1)],
        )
        .await;
        let request = serde_json::from_value(serde_json::json!({
            "requirements": [],
            "updates": [{
                "action": "add-snapshot",
                "snapshot": {
                    "snapshot-id": 11,
                    "sequence-number": 1,
                    "timestamp-ms": 2234,
                    "manifests": [duplicate_file_manifest],
                    "summary": {"operation": "append"}
                }
            }]
        }))
        .expect("duplicate file request should parse");
        let error = commit_table_response(&store, &metadata_backend, "warehouse", &namespace, "events", request)
            .await
            .expect_err("duplicate file references must fail");
        assert_eq!(error.code(), &S3ErrorCode::InvalidRequest);
    }

    #[tokio::test]
    async fn row_level_conflict_allows_v1_manifest_snapshot() {
        let store = TestTableCatalogStore::default();
        let metadata_backend = TestTableCatalogObjectBackend::default();
        let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
        let created = create_standard_events_table(&store, &metadata_backend, &namespace).await;
        let table_location = created.metadata["location"]
            .as_str()
            .expect("created metadata should have table location");
        let manifest = format!("{table_location}/metadata/manifest-10.avro");
        let data_file = format!("{table_location}/data/part-10.parquet");
        seed_test_manifest(&metadata_backend, "warehouse", &manifest, &[(&data_file, 0, 1, 10, 1)]).await;
        let append_request: RestCommitTableRequest = serde_json::from_value(serde_json::json!({
            "requirements": [],
            "updates": [
                {
                    "action": "add-snapshot",
                    "snapshot": {
                        "snapshot-id": 10,
                        "sequence-number": 1,
                        "timestamp-ms": 1234,
                        "manifests": [
                            manifest.clone()
                        ],
                        "summary": {
                            "operation": "append"
                        }
                    }
                },
                {
                    "action": "set-snapshot-ref",
                    "ref-name": "main",
                    "snapshot-id": 10,
                    "type": "branch"
                }
            ]
        }))
        .expect("append request should parse");

        let commit = commit_table_response(&store, &metadata_backend, "warehouse", &namespace, "events", append_request)
            .await
            .expect("v1 manifests snapshot should commit");

        assert_eq!(commit.metadata["current-snapshot-id"], 10);
        assert_eq!(commit.metadata["last-sequence-number"], 1);

        let second_manifest_list = format!("{table_location}/metadata/snap-11.avro");
        let second_manifest = format!("{table_location}/metadata/manifest-11.avro");
        let second_data_file = format!("{table_location}/data/part-11.parquet");
        let second_manifest_list_key = test_snapshot_object_key("warehouse", &second_manifest_list);
        metadata_backend
            .put_bytes(
                "warehouse",
                &second_manifest_list_key,
                test_manifest_list_avro_entries(&[(&manifest, 1, 10), (&second_manifest, 2, 11)]),
            )
            .await;
        seed_test_manifest(&metadata_backend, "warehouse", &second_manifest, &[(&second_data_file, 0, 1, 11, 2)]).await;
        let second_append: RestCommitTableRequest = serde_json::from_value(serde_json::json!({
            "requirements": [
                {
                    "type": "assert-current-snapshot-id",
                    "snapshot-id": 10
                }
            ],
            "updates": [
                {
                    "action": "add-snapshot",
                    "snapshot": {
                        "snapshot-id": 11,
                        "parent-snapshot-id": 10,
                        "sequence-number": 2,
                        "timestamp-ms": 2234,
                        "manifest-list": second_manifest_list,
                        "summary": {
                            "operation": "append"
                        }
                    }
                },
                {
                    "action": "set-snapshot-ref",
                    "ref-name": "main",
                    "snapshot-id": 11,
                    "type": "branch"
                }
            ]
        }))
        .expect("second append request should parse");

        let upgraded = commit_table_response(&store, &metadata_backend, "warehouse", &namespace, "events", second_append)
            .await
            .expect("manifest-list snapshot should inherit a legacy manifest with unknown provenance");

        assert_eq!(upgraded.metadata["current-snapshot-id"], 11);
        assert_eq!(upgraded.metadata["last-sequence-number"], 2);
    }

    #[tokio::test]
    async fn row_level_conflict_inherits_manifest_list_sequence_numbers() {
        let store = TestTableCatalogStore::default();
        let metadata_backend = TestTableCatalogObjectBackend::default();
        let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
        let created = create_standard_events_table(&store, &metadata_backend, &namespace).await;
        let table_location = created.metadata["location"]
            .as_str()
            .expect("created metadata should have table location");
        let manifest_list = format!("{table_location}/metadata/snap-10.avro");
        let manifest = format!("{table_location}/metadata/manifest-10.avro");
        let data_file = format!("{table_location}/data/part-10.parquet");
        seed_test_manifest_list(&metadata_backend, "warehouse", &manifest_list, &[&manifest], 1, 10).await;
        seed_test_manifest_with_nullable_sequences(&metadata_backend, "warehouse", &manifest, &[(&data_file, 0, 1, 10, None)])
            .await;
        let append_request: RestCommitTableRequest = serde_json::from_value(serde_json::json!({
            "requirements": [],
            "updates": [
                {
                    "action": "add-snapshot",
                    "snapshot": {
                        "snapshot-id": 10,
                        "sequence-number": 1,
                        "timestamp-ms": 1234,
                        "manifest-list": manifest_list,
                        "summary": {
                            "operation": "append"
                        }
                    }
                },
                {
                    "action": "set-snapshot-ref",
                    "ref-name": "main",
                    "snapshot-id": 10,
                    "type": "branch"
                }
            ]
        }))
        .expect("append request should parse");

        let commit = commit_table_response(&store, &metadata_backend, "warehouse", &namespace, "events", append_request)
            .await
            .expect("manifest entry should inherit sequence numbers from manifest list");

        assert_eq!(commit.metadata["current-snapshot-id"], 10);
        assert_eq!(commit.metadata["last-sequence-number"], 1);
    }

    #[tokio::test]
    async fn row_level_conflict_allows_inherited_manifests_on_append() {
        let store = TestTableCatalogStore::default();
        let metadata_backend = TestTableCatalogObjectBackend::default();
        let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
        let created = create_standard_events_table(&store, &metadata_backend, &namespace).await;
        let table_location = created.metadata["location"]
            .as_str()
            .expect("created metadata should have table location");
        let first_manifest_list = format!("{table_location}/metadata/snap-10.avro");
        let first_manifest = format!("{table_location}/metadata/manifest-10.avro");
        let first_data_file = format!("{table_location}/data/part-10.parquet");
        seed_test_manifest_list(&metadata_backend, "warehouse", &first_manifest_list, &[&first_manifest], 1, 10).await;
        seed_test_manifest(&metadata_backend, "warehouse", &first_manifest, &[(&first_data_file, 0, 1, 10, 1)]).await;
        let first_append: RestCommitTableRequest = serde_json::from_value(serde_json::json!({
            "requirements": [],
            "updates": [
                {
                    "action": "add-snapshot",
                    "snapshot": {
                        "snapshot-id": 10,
                        "sequence-number": 1,
                        "timestamp-ms": 1234,
                        "manifest-list": first_manifest_list,
                        "summary": {
                            "operation": "append"
                        }
                    }
                },
                {
                    "action": "set-snapshot-ref",
                    "ref-name": "main",
                    "snapshot-id": 10,
                    "type": "branch"
                }
            ]
        }))
        .expect("first append request should parse");
        commit_table_response(&store, &metadata_backend, "warehouse", &namespace, "events", first_append)
            .await
            .expect("first append should commit");

        let second_manifest_list = format!("{table_location}/metadata/snap-11.avro");
        let second_manifest = format!("{table_location}/metadata/manifest-11.avro");
        let second_data_file = format!("{table_location}/data/part-11.parquet");
        let second_manifest_list_key = test_snapshot_object_key("warehouse", &second_manifest_list);
        metadata_backend
            .put_bytes(
                "warehouse",
                &second_manifest_list_key,
                test_manifest_list_avro_entries(&[(&first_manifest, 1, 10), (&second_manifest, 2, 11)]),
            )
            .await;
        seed_test_manifest(&metadata_backend, "warehouse", &second_manifest, &[(&second_data_file, 0, 1, 11, 2)]).await;
        let second_append: RestCommitTableRequest = serde_json::from_value(serde_json::json!({
            "requirements": [
                {
                    "type": "assert-current-snapshot-id",
                    "snapshot-id": 10
                }
            ],
            "updates": [
                {
                    "action": "add-snapshot",
                    "snapshot": {
                        "snapshot-id": 11,
                        "parent-snapshot-id": 10,
                        "sequence-number": 2,
                        "timestamp-ms": 2234,
                        "manifest-list": second_manifest_list,
                        "summary": {
                            "operation": "append"
                        }
                    }
                },
                {
                    "action": "set-snapshot-ref",
                    "ref-name": "main",
                    "snapshot-id": 11,
                    "type": "branch"
                }
            ]
        }))
        .expect("second append request should parse");

        let commit = commit_table_response(&store, &metadata_backend, "warehouse", &namespace, "events", second_append)
            .await
            .expect("append should preserve inherited manifests");

        assert_eq!(commit.metadata["current-snapshot-id"], 11);
        assert_eq!(commit.metadata["last-sequence-number"], 2);
    }

    #[tokio::test]
    async fn row_level_conflict_rejects_changed_inherited_manifest_identity() {
        let store = TestTableCatalogStore::default();
        let metadata_backend = TestTableCatalogObjectBackend::default();
        let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
        let created = create_standard_events_table(&store, &metadata_backend, &namespace).await;
        let table_location = created.metadata["location"]
            .as_str()
            .expect("created metadata should have table location");
        let first_manifest_list = format!("{table_location}/metadata/snap-10.avro");
        let first_manifest = format!("{table_location}/metadata/manifest-10.avro");
        let first_data_file = format!("{table_location}/data/part-10.parquet");
        seed_test_manifest_list(&metadata_backend, "warehouse", &first_manifest_list, &[&first_manifest], 1, 10).await;
        seed_test_manifest(&metadata_backend, "warehouse", &first_manifest, &[(&first_data_file, 0, 1, 10, 1)]).await;
        let first_append: RestCommitTableRequest = serde_json::from_value(serde_json::json!({
            "requirements": [],
            "updates": [
                {
                    "action": "add-snapshot",
                    "snapshot": {
                        "snapshot-id": 10,
                        "sequence-number": 1,
                        "timestamp-ms": 1234,
                        "manifest-list": first_manifest_list,
                        "summary": {
                            "operation": "append"
                        }
                    }
                },
                {
                    "action": "set-snapshot-ref",
                    "ref-name": "main",
                    "snapshot-id": 10,
                    "type": "branch"
                }
            ]
        }))
        .expect("first append request should parse");
        commit_table_response(&store, &metadata_backend, "warehouse", &namespace, "events", first_append)
            .await
            .expect("first append should commit");
        let current = store
            .load_table("warehouse", "analytics", "events")
            .await
            .expect("table lookup should succeed")
            .expect("table should exist");

        let second_manifest_list = format!("{table_location}/metadata/snap-11.avro");
        seed_test_manifest_list(&metadata_backend, "warehouse", &second_manifest_list, &[&first_manifest], 2, 10).await;
        let second_append: RestCommitTableRequest = serde_json::from_value(serde_json::json!({
            "requirements": [
                {
                    "type": "assert-current-snapshot-id",
                    "snapshot-id": 10
                }
            ],
            "updates": [
                {
                    "action": "add-snapshot",
                    "snapshot": {
                        "snapshot-id": 11,
                        "parent-snapshot-id": 10,
                        "sequence-number": 2,
                        "timestamp-ms": 2234,
                        "manifest-list": second_manifest_list,
                        "summary": {
                            "operation": "append"
                        }
                    }
                }
            ]
        }))
        .expect("second append request should parse");

        let error = commit_table_response(&store, &metadata_backend, "warehouse", &namespace, "events", second_append)
            .await
            .expect_err("inherited manifest identity must not change");

        assert_eq!(error.code(), &s3s::S3ErrorCode::InvalidRequest);
        let unchanged = store
            .load_table("warehouse", "analytics", "events")
            .await
            .expect("table lookup should succeed")
            .expect("table should still exist");
        assert_eq!(unchanged.metadata_location, current.metadata_location);
        assert_eq!(unchanged.version_token, current.version_token);
        assert_eq!(unchanged.generation, current.generation);
    }

    #[tokio::test]
    async fn row_level_conflict_rejects_stale_new_manifest_sequence() {
        let store = TestTableCatalogStore::default();
        let metadata_backend = TestTableCatalogObjectBackend::default();
        let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
        let created = create_standard_events_table(&store, &metadata_backend, &namespace).await;
        let table_location = created.metadata["location"]
            .as_str()
            .expect("created metadata should have table location");
        let current = store
            .load_table("warehouse", "analytics", "events")
            .await
            .expect("table lookup should succeed")
            .expect("table should exist");
        let manifest_list = format!("{table_location}/metadata/snap-11.avro");
        let manifest = format!("{table_location}/metadata/manifest-11.avro");
        let data_file = format!("{table_location}/data/part-11.parquet");
        seed_test_manifest_list(&metadata_backend, "warehouse", &manifest_list, &[&manifest], 1, 11).await;
        seed_test_manifest(&metadata_backend, "warehouse", &manifest, &[(&data_file, 0, 1, 11, 1)]).await;
        let append_request: RestCommitTableRequest = serde_json::from_value(serde_json::json!({
            "requirements": [],
            "updates": [
                {
                    "action": "add-snapshot",
                    "snapshot": {
                        "snapshot-id": 11,
                        "sequence-number": 2,
                        "timestamp-ms": 2234,
                        "manifest-list": manifest_list,
                        "summary": {
                            "operation": "append"
                        }
                    }
                }
            ]
        }))
        .expect("append request should parse");

        let error = commit_table_response(&store, &metadata_backend, "warehouse", &namespace, "events", append_request)
            .await
            .expect_err("new manifest sequence must match the committed snapshot");

        assert_eq!(error.code(), &s3s::S3ErrorCode::InvalidRequest);
        let unchanged = store
            .load_table("warehouse", "analytics", "events")
            .await
            .expect("table lookup should succeed")
            .expect("table should still exist");
        assert_eq!(unchanged.metadata_location, current.metadata_location);
        assert_eq!(unchanged.version_token, current.version_token);
        assert_eq!(unchanged.generation, current.generation);
    }

    #[tokio::test]
    async fn row_level_conflict_rejects_stale_added_entry_sequence() {
        let store = TestTableCatalogStore::default();
        let metadata_backend = TestTableCatalogObjectBackend::default();
        let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
        let created = create_standard_events_table(&store, &metadata_backend, &namespace).await;
        let table_location = created.metadata["location"]
            .as_str()
            .expect("created metadata should have table location");
        let current = store
            .load_table("warehouse", "analytics", "events")
            .await
            .expect("table lookup should succeed")
            .expect("table should exist");
        let manifest_list = format!("{table_location}/metadata/snap-11.avro");
        let manifest = format!("{table_location}/metadata/manifest-11.avro");
        let data_file = format!("{table_location}/data/part-11.parquet");
        seed_test_manifest_list(&metadata_backend, "warehouse", &manifest_list, &[&manifest], 2, 11).await;
        seed_test_manifest(&metadata_backend, "warehouse", &manifest, &[(&data_file, 0, 1, 11, 1)]).await;
        let append_request: RestCommitTableRequest = serde_json::from_value(serde_json::json!({
            "requirements": [],
            "updates": [
                {
                    "action": "add-snapshot",
                    "snapshot": {
                        "snapshot-id": 11,
                        "sequence-number": 2,
                        "timestamp-ms": 2234,
                        "manifest-list": manifest_list,
                        "summary": {
                            "operation": "append"
                        }
                    }
                }
            ]
        }))
        .expect("append request should parse");

        let error = commit_table_response(&store, &metadata_backend, "warehouse", &namespace, "events", append_request)
            .await
            .expect_err("added file sequence must match the new manifest");

        assert_eq!(error.code(), &s3s::S3ErrorCode::InvalidRequest);
        let unchanged = store
            .load_table("warehouse", "analytics", "events")
            .await
            .expect("table lookup should succeed")
            .expect("table should still exist");
        assert_eq!(unchanged.metadata_location, current.metadata_location);
        assert_eq!(unchanged.version_token, current.version_token);
        assert_eq!(unchanged.generation, current.generation);
    }

    #[tokio::test]
    async fn row_level_conflict_rejects_historical_change_in_new_manifest() {
        let store = TestTableCatalogStore::default();
        let metadata_backend = TestTableCatalogObjectBackend::default();
        let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
        let created = create_standard_events_table(&store, &metadata_backend, &namespace).await;
        let table_location = created.metadata["location"]
            .as_str()
            .expect("created metadata should have table location");
        let current = store
            .load_table("warehouse", "analytics", "events")
            .await
            .expect("table lookup should succeed")
            .expect("table should exist");
        let manifest_list = format!("{table_location}/metadata/snap-11.avro");
        let manifest = format!("{table_location}/metadata/manifest-11.avro");
        let data_file = format!("{table_location}/data/part-10.parquet");
        seed_test_manifest_list(&metadata_backend, "warehouse", &manifest_list, &[&manifest], 2, 11).await;
        seed_test_manifest(&metadata_backend, "warehouse", &manifest, &[(&data_file, 0, 1, 10, 1)]).await;
        let append_request: RestCommitTableRequest = serde_json::from_value(serde_json::json!({
            "requirements": [],
            "updates": [
                {
                    "action": "add-snapshot",
                    "snapshot": {
                        "snapshot-id": 11,
                        "sequence-number": 2,
                        "timestamp-ms": 2234,
                        "manifest-list": manifest_list,
                        "summary": {
                            "operation": "append"
                        }
                    }
                }
            ]
        }))
        .expect("append request should parse");

        let error = commit_table_response(&store, &metadata_backend, "warehouse", &namespace, "events", append_request)
            .await
            .expect_err("new manifest must not claim a historical changed entry");

        assert_eq!(error.code(), &s3s::S3ErrorCode::InvalidRequest);
        let unchanged = store
            .load_table("warehouse", "analytics", "events")
            .await
            .expect("table lookup should succeed")
            .expect("table should still exist");
        assert_eq!(unchanged.metadata_location, current.metadata_location);
        assert_eq!(unchanged.version_token, current.version_token);
        assert_eq!(unchanged.generation, current.generation);
    }

    #[tokio::test]
    async fn row_level_conflict_rejects_existing_file_absent_from_parent_snapshot() {
        let store = TestTableCatalogStore::default();
        let metadata_backend = TestTableCatalogObjectBackend::default();
        let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
        let created = create_standard_events_table(&store, &metadata_backend, &namespace).await;
        let table_location = created.metadata["location"]
            .as_str()
            .expect("created metadata should have table location");
        let parent_manifest_list = format!("{table_location}/metadata/snap-10.avro");
        let parent_data_file = format!("{table_location}/data/part-10.parquet");
        seed_test_snapshot_manifest(
            &metadata_backend,
            "warehouse",
            &parent_manifest_list,
            10,
            1,
            &[(&parent_data_file, 0, 1, 10, 1)],
        )
        .await;
        let append_request: RestCommitTableRequest = serde_json::from_value(serde_json::json!({
            "requirements": [],
            "updates": [
                {
                    "action": "add-snapshot",
                    "snapshot": {
                        "snapshot-id": 10,
                        "sequence-number": 1,
                        "timestamp-ms": 1234,
                        "manifest-list": parent_manifest_list,
                        "summary": {"operation": "append"}
                    }
                },
                {
                    "action": "set-snapshot-ref",
                    "ref-name": "main",
                    "snapshot-id": 10,
                    "type": "branch"
                }
            ]
        }))
        .expect("append request should parse");
        commit_table_response(&store, &metadata_backend, "warehouse", &namespace, "events", append_request)
            .await
            .expect("parent snapshot should commit");
        let committed = store
            .load_table("warehouse", "analytics", "events")
            .await
            .expect("table lookup should succeed")
            .expect("table should exist");

        let next_manifest_list = format!("{table_location}/metadata/snap-11.avro");
        let absent_parent_file = format!("{table_location}/data/not-in-parent.parquet");
        seed_test_snapshot_manifest(
            &metadata_backend,
            "warehouse",
            &next_manifest_list,
            11,
            2,
            &[(&absent_parent_file, 0, 0, 10, 1)],
        )
        .await;
        let overwrite_request: RestCommitTableRequest = serde_json::from_value(serde_json::json!({
            "requirements": [{"type": "assert-current-snapshot-id", "snapshot-id": 10}],
            "updates": [
                {
                    "action": "add-snapshot",
                    "snapshot": {
                        "snapshot-id": 11,
                        "parent-snapshot-id": 10,
                        "sequence-number": 2,
                        "timestamp-ms": 2234,
                        "manifest-list": next_manifest_list,
                        "summary": {"operation": "overwrite"}
                    }
                },
                {
                    "action": "set-snapshot-ref",
                    "ref-name": "main",
                    "snapshot-id": 11,
                    "type": "branch"
                }
            ]
        }))
        .expect("overwrite request should parse");

        let error = commit_table_response(&store, &metadata_backend, "warehouse", &namespace, "events", overwrite_request)
            .await
            .expect_err("an existing entry absent from the parent snapshot must conflict");

        assert_eq!(error.code(), &S3ErrorCode::PreconditionFailed);
        let unchanged = store
            .load_table("warehouse", "analytics", "events")
            .await
            .expect("table lookup should succeed")
            .expect("table should still exist");
        assert_eq!(unchanged.metadata_location, committed.metadata_location);
        assert_eq!(unchanged.version_token, committed.version_token);
        assert_eq!(unchanged.generation, committed.generation);
    }

    #[tokio::test]
    async fn row_level_conflict_allows_add_only_overwrite_snapshot() {
        let store = TestTableCatalogStore::default();
        let metadata_backend = TestTableCatalogObjectBackend::default();
        let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
        let created = create_standard_events_table(&store, &metadata_backend, &namespace).await;
        let table_location = created.metadata["location"]
            .as_str()
            .expect("created metadata should have table location");
        let current_manifest_list = format!("{table_location}/metadata/snap-10.avro");
        let current_data_file = format!("{table_location}/data/part-10.parquet");
        seed_test_snapshot_manifest(
            &metadata_backend,
            "warehouse",
            &current_manifest_list,
            10,
            1,
            &[(&current_data_file, 0, 1, 10, 1)],
        )
        .await;
        let append_request: RestCommitTableRequest = serde_json::from_value(serde_json::json!({
            "requirements": [],
            "updates": [
                {
                    "action": "add-snapshot",
                    "snapshot": {
                        "snapshot-id": 10,
                        "sequence-number": 1,
                        "timestamp-ms": 1234,
                        "manifest-list": current_manifest_list,
                        "summary": {
                            "operation": "append"
                        }
                    }
                },
                {
                    "action": "set-snapshot-ref",
                    "ref-name": "main",
                    "snapshot-id": 10,
                    "type": "branch"
                }
            ]
        }))
        .expect("append request should parse");
        commit_table_response(&store, &metadata_backend, "warehouse", &namespace, "events", append_request)
            .await
            .expect("append commit should succeed");

        let overwrite_manifest_list = format!("{table_location}/metadata/snap-11.avro");
        let added_data_file = format!("{table_location}/data/part-11.parquet");
        seed_test_snapshot_manifest(
            &metadata_backend,
            "warehouse",
            &overwrite_manifest_list,
            11,
            2,
            &[(&added_data_file, 0, 1, 11, 2)],
        )
        .await;
        let overwrite_request: RestCommitTableRequest = serde_json::from_value(serde_json::json!({
            "requirements": [
                {
                    "type": "assert-current-snapshot-id",
                    "snapshot-id": 10
                }
            ],
            "updates": [
                {
                    "action": "add-snapshot",
                    "snapshot": {
                        "snapshot-id": 11,
                        "parent-snapshot-id": 10,
                        "sequence-number": 2,
                        "timestamp-ms": 2234,
                        "manifest-list": overwrite_manifest_list,
                        "summary": {
                            "operation": "overwrite"
                        }
                    }
                },
                {
                    "action": "set-snapshot-ref",
                    "ref-name": "main",
                    "snapshot-id": 11,
                    "type": "branch"
                }
            ]
        }))
        .expect("overwrite request should parse");

        let commit = commit_table_response(&store, &metadata_backend, "warehouse", &namespace, "events", overwrite_request)
            .await
            .expect("add-only overwrite should pass conflict validation");

        assert_eq!(commit.metadata["current-snapshot-id"], 11);
        assert_eq!(commit.metadata["last-sequence-number"], 2);
    }

    #[tokio::test]
    async fn row_level_conflict_rejects_delete_of_non_current_file() {
        let store = TestTableCatalogStore::default();
        let metadata_backend = TestTableCatalogObjectBackend::default();
        let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
        let created = create_standard_events_table(&store, &metadata_backend, &namespace).await;
        let table_location = created.metadata["location"]
            .as_str()
            .expect("created metadata should have table location");
        let current_manifest_list = format!("{table_location}/metadata/snap-10.avro");
        let current_data_file = format!("{table_location}/data/part-10.parquet");
        seed_test_snapshot_manifest(
            &metadata_backend,
            "warehouse",
            &current_manifest_list,
            10,
            1,
            &[(&current_data_file, 0, 1, 10, 1)],
        )
        .await;
        let append_request: RestCommitTableRequest = serde_json::from_value(serde_json::json!({
            "requirements": [],
            "updates": [
                {
                    "action": "add-snapshot",
                    "snapshot": {
                        "snapshot-id": 10,
                        "sequence-number": 1,
                        "timestamp-ms": 1234,
                        "manifest-list": current_manifest_list,
                        "summary": {
                            "operation": "append"
                        }
                    }
                },
                {
                    "action": "set-snapshot-ref",
                    "ref-name": "main",
                    "snapshot-id": 10,
                    "type": "branch"
                }
            ]
        }))
        .expect("append request should parse");
        commit_table_response(&store, &metadata_backend, "warehouse", &namespace, "events", append_request)
            .await
            .expect("append commit should succeed");
        let committed = store
            .load_table("warehouse", "analytics", "events")
            .await
            .expect("table lookup should succeed")
            .expect("table should exist");

        let stale_data_file = format!("{table_location}/data/stale.parquet");
        let stale_key = test_snapshot_object_key("warehouse", &stale_data_file);
        metadata_backend.put_bytes("warehouse", &stale_key, b"stale".to_vec()).await;
        let overwrite_manifest_list = format!("{table_location}/metadata/snap-11.avro");
        seed_test_snapshot_manifest(
            &metadata_backend,
            "warehouse",
            &overwrite_manifest_list,
            11,
            2,
            &[(&stale_data_file, 0, 2, 11, 2)],
        )
        .await;
        let overwrite_request: RestCommitTableRequest = serde_json::from_value(serde_json::json!({
            "requirements": [
                {
                    "type": "assert-current-snapshot-id",
                    "snapshot-id": 10
                }
            ],
            "updates": [
                {
                    "action": "add-snapshot",
                    "snapshot": {
                        "snapshot-id": 11,
                        "parent-snapshot-id": 10,
                        "sequence-number": 2,
                        "timestamp-ms": 2234,
                        "manifest-list": overwrite_manifest_list,
                        "summary": {
                            "operation": "overwrite"
                        }
                    }
                }
            ]
        }))
        .expect("overwrite request should parse");

        let error = commit_table_response(&store, &metadata_backend, "warehouse", &namespace, "events", overwrite_request)
            .await
            .expect_err("stale row-level delete should conflict");

        assert_eq!(error.code(), &s3s::S3ErrorCode::PreconditionFailed);
        let unchanged = store
            .load_table("warehouse", "analytics", "events")
            .await
            .expect("table lookup should succeed")
            .expect("table should still exist");
        assert_eq!(unchanged.metadata_location, committed.metadata_location);
        assert_eq!(unchanged.version_token, committed.version_token);
        assert_eq!(unchanged.generation, committed.generation);
    }

    #[tokio::test]
    async fn row_level_conflict_rejects_append_with_delete_files() {
        let store = TestTableCatalogStore::default();
        let metadata_backend = TestTableCatalogObjectBackend::default();
        let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
        let created = create_standard_events_table(&store, &metadata_backend, &namespace).await;
        let table_location = created.metadata["location"]
            .as_str()
            .expect("created metadata should have table location");
        let current = store
            .load_table("warehouse", "analytics", "events")
            .await
            .expect("table lookup should succeed")
            .expect("table should exist");
        let manifest_list = format!("{table_location}/metadata/snap-10.avro");
        let delete_file = format!("{table_location}/delete/delete-10.parquet");
        seed_test_snapshot_manifest(&metadata_backend, "warehouse", &manifest_list, 10, 1, &[(&delete_file, 1, 1, 10, 1)]).await;
        let append_request: RestCommitTableRequest = serde_json::from_value(serde_json::json!({
            "requirements": [],
            "updates": [
                {
                    "action": "add-snapshot",
                    "snapshot": {
                        "snapshot-id": 10,
                        "sequence-number": 1,
                        "timestamp-ms": 1234,
                        "manifest-list": manifest_list,
                        "summary": {
                            "operation": "append"
                        }
                    }
                }
            ]
        }))
        .expect("append request should parse");

        let error = commit_table_response(&store, &metadata_backend, "warehouse", &namespace, "events", append_request)
            .await
            .expect_err("append must not add delete files");

        assert_eq!(error.code(), &s3s::S3ErrorCode::InvalidRequest);
        let unchanged = store
            .load_table("warehouse", "analytics", "events")
            .await
            .expect("table lookup should succeed")
            .expect("table should still exist");
        assert_eq!(unchanged.metadata_location, current.metadata_location);
        assert_eq!(unchanged.version_token, current.version_token);
        assert_eq!(unchanged.generation, current.generation);
    }

    #[tokio::test]
    async fn row_level_conflict_rejects_missing_manifest_before_pointer_update() {
        let store = TestTableCatalogStore::default();
        let metadata_backend = TestTableCatalogObjectBackend::default();
        let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
        let created = create_standard_events_table(&store, &metadata_backend, &namespace).await;
        let table_location = created.metadata["location"]
            .as_str()
            .expect("created metadata should have table location");
        let current_manifest_list = format!("{table_location}/metadata/snap-10.avro");
        let current_data_file = format!("{table_location}/data/part-10.parquet");
        seed_test_snapshot_manifest(
            &metadata_backend,
            "warehouse",
            &current_manifest_list,
            10,
            1,
            &[(&current_data_file, 0, 1, 10, 1)],
        )
        .await;
        let append_request: RestCommitTableRequest = serde_json::from_value(serde_json::json!({
            "requirements": [],
            "updates": [
                {
                    "action": "add-snapshot",
                    "snapshot": {
                        "snapshot-id": 10,
                        "sequence-number": 1,
                        "timestamp-ms": 1234,
                        "manifest-list": current_manifest_list,
                        "summary": {
                            "operation": "append"
                        }
                    }
                },
                {
                    "action": "set-snapshot-ref",
                    "ref-name": "main",
                    "snapshot-id": 10,
                    "type": "branch"
                }
            ]
        }))
        .expect("append request should parse");
        commit_table_response(&store, &metadata_backend, "warehouse", &namespace, "events", append_request)
            .await
            .expect("append commit should succeed");
        let committed = store
            .load_table("warehouse", "analytics", "events")
            .await
            .expect("table lookup should succeed")
            .expect("table should exist");
        let missing_manifest_list = format!("{table_location}/metadata/missing-snap-11.avro");
        let overwrite_request: RestCommitTableRequest = serde_json::from_value(serde_json::json!({
            "requirements": [
                {
                    "type": "assert-current-snapshot-id",
                    "snapshot-id": 10
                }
            ],
            "updates": [
                {
                    "action": "add-snapshot",
                    "snapshot": {
                        "snapshot-id": 11,
                        "parent-snapshot-id": 10,
                        "sequence-number": 2,
                        "timestamp-ms": 2234,
                        "manifest-list": missing_manifest_list,
                        "summary": {
                            "operation": "overwrite"
                        }
                    }
                }
            ]
        }))
        .expect("overwrite request should parse");

        let error = commit_table_response(&store, &metadata_backend, "warehouse", &namespace, "events", overwrite_request)
            .await
            .expect_err("missing manifest-list should fail before pointer update");

        assert_eq!(error.code(), &s3s::S3ErrorCode::InvalidRequest);
        let unchanged = store
            .load_table("warehouse", "analytics", "events")
            .await
            .expect("table lookup should succeed")
            .expect("table should still exist");
        assert_eq!(unchanged.metadata_location, committed.metadata_location);
        assert_eq!(unchanged.version_token, committed.version_token);
        assert_eq!(unchanged.generation, committed.generation);
    }

    #[tokio::test]
    async fn row_level_conflict_rejects_manifest_outside_table_warehouse() {
        let store = TestTableCatalogStore::default();
        let metadata_backend = TestTableCatalogObjectBackend::default();
        let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
        let created = create_standard_events_table(&store, &metadata_backend, &namespace).await;
        let table_location = created.metadata["location"]
            .as_str()
            .expect("created metadata should have table location");
        let current_manifest_list = format!("{table_location}/metadata/snap-10.avro");
        let current_data_file = format!("{table_location}/data/part-10.parquet");
        seed_test_snapshot_manifest(
            &metadata_backend,
            "warehouse",
            &current_manifest_list,
            10,
            1,
            &[(&current_data_file, 0, 1, 10, 1)],
        )
        .await;
        let append_request: RestCommitTableRequest = serde_json::from_value(serde_json::json!({
            "requirements": [],
            "updates": [
                {
                    "action": "add-snapshot",
                    "snapshot": {
                        "snapshot-id": 10,
                        "sequence-number": 1,
                        "timestamp-ms": 1234,
                        "manifest-list": current_manifest_list,
                        "summary": {
                            "operation": "append"
                        }
                    }
                },
                {
                    "action": "set-snapshot-ref",
                    "ref-name": "main",
                    "snapshot-id": 10,
                    "type": "branch"
                }
            ]
        }))
        .expect("append request should parse");
        commit_table_response(&store, &metadata_backend, "warehouse", &namespace, "events", append_request)
            .await
            .expect("append commit should succeed");
        let committed = store
            .load_table("warehouse", "analytics", "events")
            .await
            .expect("table lookup should succeed")
            .expect("table should exist");
        let outside_manifest_list = "s3://warehouse/tables/other-table/metadata/snap-11.avro";
        let overwrite_request: RestCommitTableRequest = serde_json::from_value(serde_json::json!({
            "requirements": [
                {
                    "type": "assert-current-snapshot-id",
                    "snapshot-id": 10
                }
            ],
            "updates": [
                {
                    "action": "add-snapshot",
                    "snapshot": {
                        "snapshot-id": 11,
                        "parent-snapshot-id": 10,
                        "sequence-number": 2,
                        "timestamp-ms": 2234,
                        "manifest-list": outside_manifest_list,
                        "summary": {
                            "operation": "overwrite"
                        }
                    }
                }
            ]
        }))
        .expect("overwrite request should parse");

        let error = commit_table_response(&store, &metadata_backend, "warehouse", &namespace, "events", overwrite_request)
            .await
            .expect_err("outside manifest-list should fail before pointer update");

        assert_eq!(error.code(), &s3s::S3ErrorCode::InvalidRequest);
        let unchanged = store
            .load_table("warehouse", "analytics", "events")
            .await
            .expect("table lookup should succeed")
            .expect("table should still exist");
        assert_eq!(unchanged.metadata_location, committed.metadata_location);
        assert_eq!(unchanged.version_token, committed.version_token);
        assert_eq!(unchanged.generation, committed.generation);
    }

    #[tokio::test]
    async fn bodyless_ref_delete_uses_default_request_options() {
        let request: DeleteTableRefRequest = read_json_body_or_default(Body::empty())
            .await
            .expect("bodyless ref delete should use default request options");

        assert!(request.expected_snapshot_id.is_none());
        assert!(!request.force);
        assert!(request.commit_id.is_none());
        assert!(request.idempotency_key.is_none());
        assert!(request.writer.is_none());
    }

    #[test]
    fn table_updates_reject_unknown_actions() {
        let metadata = serde_json::json!({
            "metadata-log": []
        });
        let updates = vec![serde_json::json!({
            "action": "rewrite-everything"
        })];

        assert!(apply_table_commit_updates(metadata, &updates, "metadata/00001.metadata.json").is_err());
    }

    #[test]
    fn metadata_id_assignment_rejects_exhausted_integer_space() {
        let metadata = serde_json::json!({
            "schemas": [{"schema-id": i32::MAX}]
        });

        let error = next_array_object_i64(&metadata, "schemas", "schema-id")
            .expect_err("the next schema id must not reuse the maximum integer");

        assert_eq!(error.code(), &s3s::S3ErrorCode::InvalidRequest);

        let metadata = serde_json::json!({
            "schemas": [{"schema-id": i32::MAX - 1}]
        });
        assert_eq!(
            next_array_object_i64(&metadata, "schemas", "schema-id").expect("the final signed 32-bit id should be assignable"),
            i64::from(i32::MAX)
        );
    }

    #[test]
    fn table_updates_apply_standard_statistics_cleanup_and_encryption_actions() {
        let metadata = serde_json::json!({
            "schemas": [
                {"type": "struct", "schema-id": 0, "fields": []},
                {"type": "struct", "schema-id": 1, "fields": []}
            ],
            "current-schema-id": 0,
            "partition-specs": [
                {"spec-id": 0, "fields": []},
                {"spec-id": 1, "fields": []}
            ],
            "default-spec-id": 0,
            "sort-orders": [{"order-id": 0, "fields": []}],
            "default-sort-order-id": 0,
            "snapshots": [{"snapshot-id": 10, "schema-id": 0}],
            "current-snapshot-id": 10,
            "refs": {"main": {"type": "branch", "snapshot-id": 10}},
            "metadata-log": []
        });
        let updates = vec![
            serde_json::json!({
                "action": "set-statistics",
                "statistics": {
                    "snapshot-id": 10,
                    "statistics-path": "s3://warehouse/tables/table-id/metadata/stats.puffin"
                }
            }),
            serde_json::json!({
                "action": "set-partition-statistics",
                "partition-statistics": {
                    "snapshot-id": 10,
                    "statistics-path": "s3://warehouse/tables/table-id/metadata/partition-stats.parquet"
                }
            }),
            serde_json::json!({"action": "remove-partition-specs", "spec-ids": [1]}),
            serde_json::json!({"action": "remove-schemas", "schema-ids": [1]}),
            serde_json::json!({
                "action": "add-encryption-key",
                "encryption-key": {"key-id": "key-1", "encrypted-key-metadata": "AQID"}
            }),
        ];

        let updated = apply_table_commit_updates_at(metadata, &updates, "metadata/00001.metadata.json", 100)
            .expect("standard table updates should apply");
        assert_eq!(updated["statistics"][0]["snapshot-id"], 10);
        assert_eq!(updated["partition-statistics"][0]["snapshot-id"], 10);
        assert_eq!(updated["partition-specs"].as_array().map(Vec::len), Some(1));
        assert_eq!(updated["schemas"].as_array().map(Vec::len), Some(1));
        assert_eq!(updated["encryption-keys"][0]["key-id"], "key-1");

        let removed = apply_table_commit_updates_at(
            updated,
            &[
                serde_json::json!({"action": "remove-statistics", "snapshot-id": 10}),
                serde_json::json!({"action": "remove-partition-statistics", "snapshot-id": 10}),
                serde_json::json!({"action": "remove-encryption-key", "key-id": "key-1"}),
            ],
            "metadata/00002.metadata.json",
            101,
        )
        .expect("standard table removals should apply");
        assert!(removed["statistics"].as_array().is_some_and(Vec::is_empty));
        assert!(removed["partition-statistics"].as_array().is_some_and(Vec::is_empty));
        assert!(removed["encryption-keys"].as_array().is_some_and(Vec::is_empty));
    }

    #[test]
    fn table_location_updates_must_stay_inside_bucket() {
        let metadata = serde_json::json!({
            "location": "s3://warehouse/tables/table-id",
            "metadata-log": []
        });
        let updates = vec![serde_json::json!({
            "action": "set-location",
            "location": "s3://other-warehouse/tables/table-id"
        })];

        let updated = apply_table_commit_updates(metadata, &updates, "metadata/00001.metadata.json")
            .expect("set-location should update metadata before boundary validation");

        assert!(validate_metadata_table_location_in_bucket("warehouse", &updated).is_err());
    }

    #[test]
    fn create_view_request_accepts_standard_iceberg_rest_shape() {
        let request: CreateViewRequest = serde_json::from_value(serde_json::json!({
            "name": "recent_events",
            "schema": {
                "type": "struct",
                "schema-id": 0,
                "fields": [
                    {
                        "id": 1,
                        "name": "id",
                        "required": true,
                        "type": "long"
                    }
                ]
            },
            "view-version": {
                "version-id": 1,
                "schema-id": 0,
                "summary": {
                    "engine-name": "spark",
                    "engine-version": "3.5.0"
                },
                "default-catalog": "warehouse",
                "default-namespace": ["analytics"],
                "representations": [
                    {
                        "type": "sql",
                        "sql": "SELECT id FROM analytics.events WHERE ts >= current_date()",
                        "dialect": "spark"
                    }
                ]
            },
            "properties": {
                "comment": "recent event ids"
            }
        }))
        .expect("standard create view request should parse");

        assert_eq!(request.name, "recent_events");
        assert_eq!(request.properties.get("comment").map(String::as_str), Some("recent event ids"));
    }

    #[test]
    fn view_versions_resolve_minus_one_to_the_current_schema() {
        let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
        let request: CreateViewRequest = serde_json::from_value(serde_json::json!({
            "name": "recent_events",
            "schema": {"type": "struct", "schema-id": 3, "fields": []},
            "view-version": {
                "version-id": 1,
                "schema-id": -1,
                "summary": {"engine-name": "spark"},
                "default-catalog": "warehouse",
                "default-namespace": ["analytics"],
                "representations": [{"type": "sql", "sql": "SELECT 1", "dialect": "spark"}]
            }
        }))
        .expect("view request should parse");
        let (_, metadata) = view_entry_from_create_view_request("warehouse", &namespace, request)
            .expect("create should resolve the current schema placeholder");
        assert_eq!(metadata["versions"][0]["schema-id"], 3);

        let updated = apply_view_commit_updates_at(
            metadata,
            &[serde_json::json!({
                "action": "add-view-version",
                "view-version": {
                    "version-id": 2,
                    "schema-id": -1,
                    "summary": {"engine-name": "spark"},
                    "default-catalog": "warehouse",
                    "default-namespace": ["analytics"],
                    "representations": [{"type": "sql", "sql": "SELECT 2", "dialect": "spark"}]
                }
            })],
            "s3://warehouse/views/view-id/metadata/v1.metadata.json",
            2,
        )
        .expect("view commit should resolve the current schema placeholder");
        assert_eq!(updated["versions"][1]["schema-id"], 3);
    }

    #[test]
    fn create_view_request_accepts_deep_warehouse_location() {
        let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
        let deep_prefix = (0..80).map(|index| format!("level-{index}")).collect::<Vec<_>>().join("/");
        let location = format!("s3://warehouse/{deep_prefix}");
        assert!(crate::table_catalog::validate_table_warehouse_location("warehouse", &location).is_err());
        let request: CreateViewRequest = serde_json::from_value(serde_json::json!({
            "name": "recent_events",
            "location": location,
            "schema": {
                "type": "struct",
                "schema-id": 0,
                "fields": []
            },
            "view-version": {
                "version-id": 1,
                "schema-id": 0,
                "summary": {
                    "engine-name": "spark"
                },
                "default-catalog": "warehouse",
                "default-namespace": ["analytics"],
                "representations": [
                    {
                        "type": "sql",
                        "sql": "SELECT 1",
                        "dialect": "spark"
                    }
                ]
            }
        }))
        .expect("deep create view request should parse");

        let (entry, metadata) = view_entry_from_create_view_request("warehouse", &namespace, request)
            .expect("view location should not inherit table warehouse index depth limits");

        assert_eq!(entry.warehouse_location, format!("s3://warehouse/{deep_prefix}"));
        validate_metadata_view_location_in_bucket("warehouse", &metadata)
            .expect("view metadata location should not inherit table warehouse index depth limits");
    }

    #[test]
    fn create_view_rejects_versions_with_unknown_schemas() {
        let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
        let request: CreateViewRequest = serde_json::from_value(serde_json::json!({
            "name": "recent_events",
            "schema": {
                "type": "struct",
                "schema-id": 0,
                "fields": []
            },
            "view-version": {
                "version-id": 1,
                "schema-id": 99,
                "summary": {
                    "engine-name": "spark"
                },
                "default-catalog": "warehouse",
                "default-namespace": ["analytics"],
                "representations": [{
                    "type": "sql",
                    "sql": "SELECT 1",
                    "dialect": "spark"
                }]
            }
        }))
        .expect("view request should parse");

        let error = view_entry_from_create_view_request("warehouse", &namespace, request)
            .expect_err("view versions must reference a declared schema");
        assert_eq!(error.code(), &S3ErrorCode::InvalidRequest);
        assert_eq!(error.status_code(), Some(StatusCode::BAD_REQUEST));
    }

    #[tokio::test]
    async fn view_catalog_responses_persist_replace_and_drop_view_metadata() {
        let store = TestTableCatalogStore::default();
        let metadata_backend = TestTableCatalogObjectBackend::default();
        let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
        ensure_table_bucket_entry(&store, "warehouse", true)
            .await
            .expect("table bucket entry should be seeded");
        create_namespace_response(
            &store,
            "warehouse",
            CreateNamespaceRequest {
                namespace: vec!["analytics".to_string()],
                properties: BTreeMap::new(),
            },
            true,
        )
        .await
        .expect("namespace should be created");

        let create_request: CreateViewRequest = serde_json::from_value(serde_json::json!({
            "name": "recent_events",
            "schema": {
                "type": "struct",
                "schema-id": 0,
                "fields": [
                    {
                        "id": 1,
                        "name": "id",
                        "required": true,
                        "type": "long"
                    }
                ]
            },
            "view-version": {
                "version-id": 1,
                "schema-id": 0,
                "summary": {
                    "engine-name": "spark"
                },
                "default-catalog": "warehouse",
                "default-namespace": ["analytics"],
                "representations": [
                    {
                        "type": "sql",
                        "sql": "SELECT id FROM analytics.events",
                        "dialect": "spark"
                    }
                ]
            }
        }))
        .expect("standard create view request should parse");

        let created = create_view_response(&store, &metadata_backend, "warehouse", &namespace, create_request, true)
            .await
            .expect("view should be created");
        assert_eq!(created.metadata["format-version"], 1);
        assert_eq!(created.metadata["current-version-id"], 1);
        assert_eq!(created.metadata["versions"][0]["representations"][0]["dialect"], "spark");
        let created_metadata_key = table_metadata_location_for_catalog("warehouse", &created.metadata_location)
            .expect("client metadata location should map to the catalog object key");
        assert!(
            metadata_backend
                .object_exists("warehouse", &created_metadata_key)
                .await
                .expect("view metadata object lookup should succeed")
        );

        let unpaginated_uri = "/".parse::<http::Uri>().expect("list URI should parse");
        let listed = list_views_response(&store, "warehouse", &namespace, &unpaginated_uri)
            .await
            .expect("views should list");
        assert_eq!(listed.identifiers.len(), 1);
        assert_eq!(listed.identifiers[0].name, "recent_events");

        let loaded = load_view_response(&store, &metadata_backend, "warehouse", &namespace, "recent_events")
            .await
            .expect("view should load");
        assert_eq!(loaded.metadata_location, created.metadata_location);
        let view_uuid = created.metadata["view-uuid"].clone();
        let replace_request: RestCommitViewRequest = serde_json::from_value(serde_json::json!({
            "identifier": {
                "namespace": ["analytics"],
                "name": "recent_events"
            },
            "expected-metadata-location": created.metadata_location,
            "updates": [
                {
                    "action": "assign-uuid",
                    "uuid": view_uuid
                },
                {
                    "action": "add-view-version",
                    "view-version": {
                        "version-id": 2,
                        "schema-id": 0,
                        "summary": {
                            "engine-name": "spark"
                        },
                        "default-catalog": "warehouse",
                        "default-namespace": ["analytics"],
                        "representations": [
                            {
                                "type": "sql",
                                "sql": "SELECT id FROM analytics.events WHERE id > 10",
                                "dialect": "spark"
                            }
                        ]
                    }
                },
                {
                    "action": "set-current-view-version",
                    "view-version-id": 2
                }
            ]
        }))
        .expect("replace view request should parse");
        let replaced =
            replace_view_response(&store, &metadata_backend, "warehouse", &namespace, "recent_events", replace_request)
                .await
                .expect("view should replace");
        assert_ne!(replaced.metadata_location, created.metadata_location);
        assert_eq!(replaced.metadata["current-version-id"], 2);
        assert_eq!(replaced.metadata["view-uuid"], created.metadata["view-uuid"]);
        assert!(replaced.metadata.get("table-uuid").is_none());
        assert_eq!(replaced.metadata["metadata-log"][0]["metadata-file"], created.metadata_location);
        assert_eq!(
            replaced.metadata["version-log"]
                .as_array()
                .expect("version log should be an array")
                .len(),
            2
        );

        let current = store
            .load_view("warehouse", "analytics", "recent_events")
            .await
            .expect("view lookup should succeed")
            .expect("view should exist");
        let explicit_metadata_location =
            ".rustfs-table/warehouses/default/namespaces/analytics/views/recent_events/metadata/00003.metadata.json";
        let mut unsupported_metadata = replaced.metadata.clone();
        unsupported_metadata["format-version"] = serde_json::Value::from(2);
        metadata_backend
            .put_json("warehouse", explicit_metadata_location, unsupported_metadata)
            .await;
        let unsupported_replace_request: RestCommitViewRequest = serde_json::from_value(serde_json::json!({
            "expected-version-token": current.version_token,
            "expected-metadata-location": replaced.metadata_location,
            "new-metadata-location": table_metadata_location_for_client("warehouse", explicit_metadata_location),
            "updates": []
        }))
        .expect("unsupported metadata replace request should parse");
        let error = replace_view_response(
            &store,
            &metadata_backend,
            "warehouse",
            &namespace,
            "recent_events",
            unsupported_replace_request,
        )
        .await
        .expect_err("external view metadata must use format version 1");
        assert_eq!(error.code(), &S3ErrorCode::Custom(ICEBERG_ERROR_UNSUPPORTED_OPERATION.into()));
        assert_eq!(error.status_code(), Some(StatusCode::NOT_ACCEPTABLE));
        let unchanged = store
            .load_view("warehouse", "analytics", "recent_events")
            .await
            .expect("view lookup should succeed")
            .expect("view should remain present");
        assert_eq!(unchanged.version_token, current.version_token);
        assert_eq!(unchanged.metadata_location, current.metadata_location);

        metadata_backend
            .put_json("warehouse", explicit_metadata_location, replaced.metadata.clone())
            .await;
        let explicit_replace_request: RestCommitViewRequest = serde_json::from_value(serde_json::json!({
            "expected-version-token": current.version_token,
            "expected-metadata-location": replaced.metadata_location,
            "new-metadata-location": table_metadata_location_for_client("warehouse", explicit_metadata_location),
            "updates": []
        }))
        .expect("explicit metadata replace request should parse");
        let explicitly_replaced = replace_view_response(
            &store,
            &metadata_backend,
            "warehouse",
            &namespace,
            "recent_events",
            explicit_replace_request,
        )
        .await
        .expect("client metadata locations should map to catalog object keys");
        assert_eq!(
            explicitly_replaced.metadata_location,
            table_metadata_location_for_client("warehouse", explicit_metadata_location)
        );

        drop_view_in_store(&store, "warehouse", &namespace, "recent_events")
            .await
            .expect("view should drop");
        let listed = list_views_response(&store, "warehouse", &namespace, &unpaginated_uri)
            .await
            .expect("views should list after drop");
        assert!(listed.identifiers.is_empty());

        let recreate_request: CreateViewRequest = serde_json::from_value(serde_json::json!({
            "name": "recent_events",
            "schema": {
                "type": "struct",
                "schema-id": 0,
                "fields": [
                    {
                        "id": 1,
                        "name": "id",
                        "required": true,
                        "type": "long"
                    }
                ]
            },
            "view-version": {
                "version-id": 1,
                "schema-id": 0,
                "summary": {
                    "engine-name": "spark"
                },
                "default-catalog": "warehouse",
                "default-namespace": ["analytics"],
                "representations": [
                    {
                        "type": "sql",
                        "sql": "SELECT id FROM analytics.events WHERE id > 100",
                        "dialect": "spark"
                    }
                ]
            }
        }))
        .expect("standard recreate view request should parse");
        let recreated = create_view_response(&store, &metadata_backend, "warehouse", &namespace, recreate_request, true)
            .await
            .expect("dropped view name should be reusable");
        assert_ne!(recreated.metadata_location, created.metadata_location);
    }

    #[tokio::test]
    async fn load_responses_reject_persisted_metadata_pointers_outside_protected_roots() {
        let store = TestTableCatalogStore::default();
        let metadata_backend = TestTableCatalogObjectBackend::default();
        let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
        create_standard_events_table(&store, &metadata_backend, &namespace).await;
        store.tables.lock().await[0].metadata_location = "outside/table.metadata.json".to_string();
        metadata_backend
            .put_json("warehouse", "outside/table.metadata.json", serde_json::json!({}))
            .await;

        let table_error = load_table_response(&store, &metadata_backend, "warehouse", &namespace, "events")
            .await
            .expect_err("table metadata outside the protected root should fail closed");
        assert_eq!(table_error.code(), &S3ErrorCode::Custom(ICEBERG_ERROR_REST.into()));
        assert_eq!(table_error.status_code(), Some(StatusCode::INTERNAL_SERVER_ERROR));

        let view = crate::table_catalog::IdentifierSegment::parse("recent_events").expect("view should parse");
        store.views.lock().await.push(crate::table_catalog::ViewEntry {
            version: crate::table_catalog::TABLE_CATALOG_ENTRY_VERSION,
            table_bucket: "warehouse".to_string(),
            namespace: namespace.public_name(),
            view: view.as_str().to_string(),
            view_id: "view-id".to_string(),
            view_uuid: "view-uuid".to_string(),
            format: "ICEBERG_VIEW".to_string(),
            format_version: 1,
            warehouse_location: "s3://warehouse/views/view-id".to_string(),
            metadata_location: "outside/view.metadata.json".to_string(),
            version_token: "token-v1".to_string(),
            generation: 1,
            state: crate::table_catalog::TableCatalogEntryState::Active,
            properties: BTreeMap::new(),
            created_at: None,
            updated_at: None,
        });
        metadata_backend
            .put_json("warehouse", "outside/view.metadata.json", serde_json::json!({}))
            .await;

        let view_error = load_view_response(&store, &metadata_backend, "warehouse", &namespace, view.as_str())
            .await
            .expect_err("view metadata outside the protected root should fail closed");
        assert_eq!(view_error.code(), &S3ErrorCode::Custom(ICEBERG_ERROR_REST.into()));
        assert_eq!(view_error.status_code(), Some(StatusCode::INTERNAL_SERVER_ERROR));
    }

    #[tokio::test]
    async fn table_ref_write_responses_commit_retention_refs_and_protect_deletes() {
        let store = TestTableCatalogStore::default();
        let metadata_backend = TestTableCatalogObjectBackend::default();
        let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
        let route = RestTableRoute {
            bucket: "warehouse",
            namespace: &namespace,
            table: "events",
        };
        let created = create_standard_events_table(&store, &metadata_backend, &namespace).await;
        let table_location = created.metadata["location"]
            .as_str()
            .expect("created metadata should have table location");
        let manifest_list = format!("{table_location}/metadata/snap-10.avro");
        let data_file = format!("{table_location}/data/part-10.parquet");
        seed_test_snapshot_manifest(&metadata_backend, "warehouse", &manifest_list, 10, 1, &[(&data_file, 0, 1, 10, 1)]).await;

        let append_request: RestCommitTableRequest = serde_json::from_value(serde_json::json!({
            "requirements": [],
            "updates": [
                {
                    "action": "add-snapshot",
                    "snapshot": {
                        "snapshot-id": 10,
                        "sequence-number": 1,
                        "timestamp-ms": 1234,
                        "manifest-list": manifest_list,
                        "summary": {
                            "operation": "append"
                        }
                    }
                },
                {
                    "action": "set-snapshot-ref",
                    "ref-name": "main",
                    "snapshot-id": 10,
                    "type": "branch"
                }
            ]
        }))
        .expect("append request should parse");
        commit_table_response(&store, &metadata_backend, "warehouse", &namespace, "events", append_request)
            .await
            .expect("append should commit");

        let ref_request: PutTableRefRequest = serde_json::from_value(serde_json::json!({
            "snapshot-id": 10,
            "type": "tag",
            "max-ref-age-ms": 86400000,
            "expected-snapshot-id": null
        }))
        .expect("ref put request should parse");
        put_table_ref_response(None, &store, &metadata_backend, route, "audit", ref_request)
            .await
            .expect("ref put should commit");

        let refs = table_refs_response(&store, &metadata_backend, "warehouse", &namespace, "events")
            .await
            .expect("refs should load");
        assert_eq!(refs.refs["audit"]["type"], "tag");
        assert_eq!(refs.refs["audit"]["max-ref-age-ms"], 86400000);

        let delete_without_force: DeleteTableRefRequest =
            serde_json::from_value(serde_json::json!({})).expect("ref delete request should parse");
        let error = delete_table_ref_response(None, &store, &metadata_backend, route, "audit", delete_without_force)
            .await
            .expect_err("retention refs should require force delete");
        assert_eq!(error.code(), &s3s::S3ErrorCode::InvalidRequest);

        let force_delete: DeleteTableRefRequest =
            serde_json::from_value(serde_json::json!({ "force": true })).expect("ref force delete should parse");
        delete_table_ref_response(None, &store, &metadata_backend, route, "audit", force_delete)
            .await
            .expect("force delete should commit");
        let refs = table_refs_response(&store, &metadata_backend, "warehouse", &namespace, "events")
            .await
            .expect("refs should load after delete");
        assert!(!refs.refs.contains_key("audit"));

        let main_delete: DeleteTableRefRequest =
            serde_json::from_value(serde_json::json!({ "force": true })).expect("main delete request should parse");
        let error = delete_table_ref_response(None, &store, &metadata_backend, route, "main", main_delete)
            .await
            .expect_err("main ref should remain protected");
        assert_eq!(error.code(), &s3s::S3ErrorCode::InvalidRequest);
    }

    #[tokio::test]
    async fn put_table_ref_rejects_a_missing_snapshot_without_advancing_the_table() {
        let store = TestTableCatalogStore::default();
        let metadata_backend = TestTableCatalogObjectBackend::default();
        let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
        let route = RestTableRoute {
            bucket: "warehouse",
            namespace: &namespace,
            table: "events",
        };
        create_standard_events_table(&store, &metadata_backend, &namespace).await;
        let before = store
            .load_table("warehouse", &namespace.public_name(), "events")
            .await
            .expect("table lookup should succeed")
            .expect("table should exist");
        let object_count_before = metadata_backend.objects.lock().await.len();
        let request: PutTableRefRequest = serde_json::from_value(serde_json::json!({
            "snapshot-id": 999,
            "type": "tag"
        }))
        .expect("ref put request should parse");

        let error = put_table_ref_response(None, &store, &metadata_backend, route, "missing", request)
            .await
            .expect_err("a ref must not target a missing snapshot");

        assert_eq!(error.code(), &s3s::S3ErrorCode::InvalidRequest);
        assert_eq!(error.message(), Some("snapshot ref missing targets snapshot 999, which does not exist"));
        let after = store
            .load_table("warehouse", &namespace.public_name(), "events")
            .await
            .expect("table lookup should succeed")
            .expect("table should remain");
        assert_eq!(after.metadata_location, before.metadata_location);
        assert_eq!(after.version_token, before.version_token);
        assert_eq!(after.generation, before.generation);
        assert_eq!(metadata_backend.objects.lock().await.len(), object_count_before);
    }

    #[test]
    fn load_table_response_includes_rest_metadata_payload() {
        let metadata = serde_json::json!({
            "format-version": 2,
            "table-uuid": "table-uuid",
            "location": "s3://warehouse/tables/table-id",
            "metadata-log": [
                {
                    "timestamp-ms": 1,
                    "metadata-file": ".rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00000.metadata.json"
                },
                {
                    "timestamp-ms": 2,
                    "metadata-file": "s3://warehouse/external/metadata/00000.metadata.json"
                }
            ]
        });
        let response = load_table_response_from_entry(table_entry_for_credentials(), metadata);

        assert_eq!(
            response.metadata_location,
            "s3://warehouse/.rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00001.metadata.json"
        );
        assert_eq!(
            response.metadata["metadata-log"][0]["metadata-file"],
            "s3://warehouse/.rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00000.metadata.json"
        );
        assert_eq!(
            response.metadata["metadata-log"][1]["metadata-file"],
            "s3://warehouse/external/metadata/00000.metadata.json"
        );
        assert!(response.storage_credentials.is_empty());
        assert_eq!(response.config.get("rustfs.credential-vending"), Some(&"unsupported".to_string()));
        assert_eq!(
            response.config.get("rustfs.credential-vending-reason"),
            Some(&"temporary-credentials-not-implemented".to_string())
        );
        assert_eq!(response.config.get("rustfs.credential-scope"), Some(&"table-prefix".to_string()));
        assert_eq!(
            response.config.get("rustfs.credential-scope-prefix"),
            Some(&"s3://warehouse/tables/table-id".to_string())
        );
        assert_eq!(
            response.config.get("rustfs.credential-mode"),
            Some(&"client-provided-s3-credentials-required".to_string())
        );
        assert!(!response.config.contains_key("s3.access-key-id"));
        assert!(!response.config.contains_key("s3.secret-access-key"));
        assert!(!response.config.contains_key("s3.session-token"));
    }

    #[test]
    fn load_table_response_preserves_format_v4_relative_metadata_log() {
        let metadata = serde_json::json!({
            "format-version": 4,
            "table-uuid": "table-uuid",
            "metadata-log": [
                {
                    "timestamp-ms": 1,
                    "metadata-file": "metadata/00000.metadata.json"
                },
                {
                    "timestamp-ms": 2,
                    "metadata-file": ".rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00000.metadata.json"
                }
            ]
        });

        let response = load_table_response_from_entry(table_entry_for_credentials(), metadata);

        assert_eq!(response.metadata["metadata-log"][0]["metadata-file"], "metadata/00000.metadata.json");
        assert_eq!(
            response.metadata["metadata-log"][1]["metadata-file"],
            "s3://warehouse/.rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00000.metadata.json"
        );
    }

    #[test]
    fn table_metadata_location_for_catalog_accepts_only_the_table_bucket() {
        let object_key = ".rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00001.metadata.json";

        assert_eq!(
            table_metadata_location_for_catalog("warehouse", &format!("s3://warehouse/{object_key}"))
                .expect("same-bucket metadata location should normalize"),
            object_key
        );
        assert!(table_metadata_location_for_catalog("warehouse", &format!("s3://other/{object_key}")).is_err());
    }

    fn table_entry_for_credentials() -> crate::table_catalog::TableEntry {
        crate::table_catalog::TableEntry {
            version: crate::table_catalog::TABLE_CATALOG_ENTRY_VERSION,
            table_bucket: "warehouse".to_string(),
            namespace: "analytics".to_string(),
            table: "events".to_string(),
            table_id: "table-id".to_string(),
            table_uuid: "table-uuid".to_string(),
            format: "ICEBERG".to_string(),
            format_version: 2,
            warehouse_location: "s3://warehouse/tables/table-id".to_string(),
            metadata_location: ".rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00001.metadata.json"
                .to_string(),
            version_token: "token-v1".to_string(),
            generation: 1,
            state: crate::table_catalog::TableCatalogEntryState::Active,
            properties: BTreeMap::new(),
            created_at: None,
            updated_at: None,
        }
    }

    #[tokio::test]
    async fn disabled_table_credential_issuer_keeps_credentials_empty() {
        let issuer = DisabledTableCredentialIssuer;
        let response = load_credentials_response_from_entry(&table_entry_for_credentials(), &issuer, None)
            .await
            .expect("disabled issuer should build an empty response");

        assert!(response.storage_credentials.is_empty());
        assert_eq!(
            response.config.get(CREDENTIAL_VENDING_CONFIG_KEY),
            Some(&CREDENTIAL_VENDING_UNSUPPORTED.to_string())
        );
        assert_eq!(
            response.config.get(CREDENTIAL_MODE_CONFIG_KEY),
            Some(&CREDENTIAL_MODE_CLIENT_PROVIDED.to_string())
        );
        assert_eq!(
            response.config.get(CREDENTIAL_VENDING_REASON_CONFIG_KEY),
            Some(&"credential-vending-disabled".to_string())
        );
    }

    #[tokio::test]
    async fn disabled_table_credential_issuer_skips_scope_validation() {
        let issuer = DisabledTableCredentialIssuer;
        let mut entry = table_entry_for_credentials();
        entry.warehouse_location = "s3://warehouse/".to_string();

        let response = load_credentials_response_from_entry(&entry, &issuer, None)
            .await
            .expect("disabled issuer should not validate credential scopes");

        assert!(response.storage_credentials.is_empty());
        assert_eq!(
            response.config.get(CREDENTIAL_VENDING_CONFIG_KEY),
            Some(&CREDENTIAL_VENDING_UNSUPPORTED.to_string())
        );
        assert!(!response.config.contains_key(CREDENTIAL_SCOPE_PREFIX_CONFIG_KEY));
    }

    struct UnavailableTableCredentialIssuer;

    #[async_trait::async_trait]
    impl TableCredentialIssuer for UnavailableTableCredentialIssuer {
        async fn issue_table_credentials(
            &self,
            _request: TableCredentialIssueRequest<'_>,
        ) -> S3Result<Option<IssuedTableCredentials>> {
            Ok(None)
        }
    }

    #[tokio::test]
    async fn unavailable_table_credential_issuer_reports_fallback_scope() {
        let issuer = UnavailableTableCredentialIssuer;
        let response = load_credentials_response_from_entry(&table_entry_for_credentials(), &issuer, None)
            .await
            .expect("unavailable issuer should build a fallback response");

        assert!(response.storage_credentials.is_empty());
        assert_eq!(
            response.config.get(CREDENTIAL_VENDING_CONFIG_KEY),
            Some(&CREDENTIAL_VENDING_UNSUPPORTED.to_string())
        );
        assert_eq!(
            response.config.get(CREDENTIAL_MODE_CONFIG_KEY),
            Some(&CREDENTIAL_MODE_CLIENT_PROVIDED.to_string())
        );
        assert_eq!(
            response.config.get(CREDENTIAL_VENDING_REASON_CONFIG_KEY),
            Some(&CREDENTIAL_VENDING_UNSUPPORTED_REASON.to_string())
        );
        assert_eq!(
            response.config.get(CREDENTIAL_SCOPE_CONFIG_KEY),
            Some(&CREDENTIAL_SCOPE_TABLE_PREFIX.to_string())
        );
        assert_eq!(
            response.config.get(CREDENTIAL_SCOPE_PREFIX_CONFIG_KEY),
            Some(&"s3://warehouse/tables/table-id/".to_string())
        );
    }

    struct TestTableCredentialIssuer;

    #[async_trait::async_trait]
    impl TableCredentialIssuer for TestTableCredentialIssuer {
        async fn issue_table_credentials(
            &self,
            _request: TableCredentialIssueRequest<'_>,
        ) -> S3Result<Option<IssuedTableCredentials>> {
            Ok(Some(IssuedTableCredentials {
                access_key_id: "temporary-access-key".to_string(),
                secret_access_key: "temporary-secret-key".to_string(),
                session_token: "temporary-session-token".to_string(),
                expiration: OffsetDateTime::from_unix_timestamp(1_800_000_000).expect("test timestamp should be valid"),
            }))
        }
    }

    #[tokio::test]
    async fn credential_issuer_returns_temporary_scoped_storage_credentials() {
        let issuer = TestTableCredentialIssuer;
        let principal = rustfs_credentials::Credentials {
            access_key: "parent-access-key".to_string(),
            secret_key: "parent-secret-key".to_string(),
            ..Default::default()
        };

        let response = load_credentials_response_from_entry(&table_entry_for_credentials(), &issuer, Some(&principal))
            .await
            .expect("issuer should build a scoped credential response");

        assert_eq!(
            response.config.get(CREDENTIAL_VENDING_CONFIG_KEY),
            Some(&CREDENTIAL_VENDING_SUPPORTED.to_string())
        );
        assert_eq!(
            response.config.get(CREDENTIAL_MODE_CONFIG_KEY),
            Some(&CREDENTIAL_MODE_CATALOG_VENDED.to_string())
        );
        assert!(!response.config.contains_key(S3_ACCESS_KEY_ID_CONFIG_KEY));
        assert!(!response.config.contains_key(S3_SECRET_ACCESS_KEY_CONFIG_KEY));
        assert!(!response.config.contains_key(S3_SESSION_TOKEN_CONFIG_KEY));
        assert_eq!(response.storage_credentials.len(), 1);
        let credential = &response.storage_credentials[0];
        assert_eq!(credential.prefix, "s3://warehouse/tables/table-id/");
        assert_eq!(credential.config.get("s3.access-key-id"), Some(&"temporary-access-key".to_string()));
        assert_eq!(credential.config.get("s3.secret-access-key"), Some(&"temporary-secret-key".to_string()));
        assert_eq!(credential.config.get("s3.session-token"), Some(&"temporary-session-token".to_string()));
        assert_eq!(
            credential.config.get("rustfs.credential-mode"),
            Some(&"catalog-vended-temporary-credentials".to_string())
        );
        assert_eq!(
            credential.config.get("rustfs.credential-scope-prefix"),
            Some(&"s3://warehouse/tables/table-id/".to_string())
        );
        assert_eq!(
            credential.config.get("rustfs.credential-expiration-unix-seconds"),
            Some(&"1800000000".to_string())
        );
        assert!(!credential.config.contains_key("rustfs.credential-vending-reason"));
    }

    #[tokio::test]
    async fn credential_response_serializes_sensitive_config_only_inside_storage_credentials() {
        let issuer = TestTableCredentialIssuer;
        let response = load_credentials_response_from_entry(&table_entry_for_credentials(), &issuer, None)
            .await
            .expect("issuer should build a scoped credential response");

        let value = serde_json::to_value(&response).expect("credential response should serialize");

        assert_eq!(
            value["config"][CREDENTIAL_VENDING_CONFIG_KEY],
            serde_json::Value::String(CREDENTIAL_VENDING_SUPPORTED.to_string())
        );
        assert!(value["config"].get(S3_ACCESS_KEY_ID_CONFIG_KEY).is_none());
        assert!(value["config"].get(S3_SECRET_ACCESS_KEY_CONFIG_KEY).is_none());
        assert!(value["config"].get(S3_SESSION_TOKEN_CONFIG_KEY).is_none());
        assert_eq!(
            value["storage-credentials"][0]["config"][S3_ACCESS_KEY_ID_CONFIG_KEY],
            serde_json::Value::String("temporary-access-key".to_string())
        );
    }

    #[test]
    fn credential_http_response_disables_caching() {
        let response = build_sensitive_json_response(StatusCode::OK, &serde_json::json!({"storage-credentials": []}))
            .expect("sensitive response should build");

        assert_eq!(
            response.headers.get(http::header::CACHE_CONTROL),
            Some(&HeaderValue::from_static("no-store, private"))
        );
        assert_eq!(response.headers.get(http::header::PRAGMA), Some(&HeaderValue::from_static("no-cache")));
        assert_eq!(response.headers.get(http::header::EXPIRES), Some(&HeaderValue::from_static("0")));
    }

    #[tokio::test]
    async fn iam_table_credentials_require_a_dedicated_signing_key() {
        let principal = rustfs_credentials::Credentials {
            access_key: "parent-access-key".to_string(),
            ..Default::default()
        };
        let issuer = IamTableCredentialIssuer { enabled: true };
        let error = issuer
            .issue_table_credentials(TableCredentialIssueRequest {
                principal: Some(&principal),
            })
            .await
            .expect_err("credential vending must not reuse the root S3 secret as a signing key");

        assert_eq!(error.code(), &S3ErrorCode::Custom(ICEBERG_ERROR_UNSUPPORTED_OPERATION.into()));
        assert_eq!(error.status_code(), Some(StatusCode::NOT_ACCEPTABLE));
    }

    #[test]
    fn table_credential_scope_rejects_cross_bucket_or_unsafe_prefix() {
        let mut entry = table_entry_for_credentials();
        entry.warehouse_location = "s3://other-warehouse/tables/table-id".to_string();
        assert!(table_credential_scope(&entry).is_err());

        let mut entry = table_entry_for_credentials();
        entry.warehouse_location = "s3://warehouse/tables/../table-id".to_string();
        assert!(table_credential_scope(&entry).is_err());

        for prefix in ["tables/*/table-id", "tables/?/table-id"] {
            let mut entry = table_entry_for_credentials();
            entry.warehouse_location = format!("s3://warehouse/{prefix}");
            assert!(table_credential_scope(&entry).is_err());
        }

        let mut entry = table_entry_for_credentials();
        entry.warehouse_location = "s3://warehouse/.rustfs-table/private".to_string();
        assert!(table_credential_scope(&entry).is_err());
    }

    #[test]
    fn commit_table_request_uses_rest_commit_fields() {
        let request: RestCommitTableRequest = serde_json::from_value(serde_json::json!({
            "commit-id": "commit-1",
            "idempotency-key": "retry-1",
            "operation": "append",
            "expected-version-token": "token-v1",
            "expected-metadata-location": ".rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00001.metadata.json",
            "new-metadata-location": ".rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00002.metadata.json",
            "requirements": [
                {
                    "type": "assert-current-snapshot-id",
                    "snapshot-id": 10
                }
            ],
            "updates": [],
            "writer": "pyiceberg"
        }))
        .expect("commit request should parse");

        assert_eq!(request.commit_id.as_deref(), Some("commit-1"));
        assert_eq!(request.idempotency_key.as_deref(), Some("retry-1"));
        assert_eq!(request.operation.as_deref(), Some("append"));
        assert_eq!(request.expected_version_token.as_deref(), Some("token-v1"));
        assert_eq!(
            request.new_metadata_location.as_deref(),
            Some(".rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00002.metadata.json")
        );
        assert_eq!(request.requirements.len(), 1);
        assert_eq!(request.writer.as_deref(), Some("pyiceberg"));
    }

    #[derive(Default)]
    struct TestTableCatalogStore {
        table_buckets: tokio::sync::Mutex<Vec<crate::table_catalog::TableBucketEntry>>,
        namespaces: tokio::sync::Mutex<Vec<crate::table_catalog::NamespaceEntry>>,
        tables: tokio::sync::Mutex<Vec<crate::table_catalog::TableEntry>>,
        views: tokio::sync::Mutex<Vec<crate::table_catalog::ViewEntry>>,
        commits: tokio::sync::Mutex<Vec<crate::table_catalog::CommitLogEntry>>,
        fail_put_table_bucket: tokio::sync::Mutex<bool>,
        create_table_pause: Option<TestCatalogPublishPause>,
        create_view_pause: Option<TestCatalogPublishPause>,
        fail_create_table_after_pause: bool,
    }

    #[derive(Clone, Default)]
    struct TestCatalogPublishPause {
        started: Arc<tokio::sync::Notify>,
        release: Arc<tokio::sync::Notify>,
    }

    impl TestCatalogPublishPause {
        async fn wait_started(&self) {
            self.started.notified().await;
        }

        fn release(&self) {
            self.release.notify_one();
        }
    }

    type TestTableCatalogObjectLocks = Arc<tokio::sync::Mutex<BTreeMap<(String, String), Arc<tokio::sync::Mutex<()>>>>>;

    #[derive(Clone, Default)]
    struct TestTableCatalogObjectBackend {
        objects: Arc<tokio::sync::Mutex<BTreeMap<(String, String), crate::table_catalog::TableCatalogObject>>>,
        fail_next_puts: Arc<tokio::sync::Mutex<BTreeSet<(String, String)>>>,
        put_object_barrier: Option<Arc<tokio::sync::Barrier>>,
        read_object_calls: Arc<std::sync::atomic::AtomicUsize>,
        object_exists_delay: Option<StdDuration>,
        object_exists_in_flight: Arc<std::sync::atomic::AtomicUsize>,
        object_exists_max_in_flight: Arc<std::sync::atomic::AtomicUsize>,
        locks: TestTableCatalogObjectLocks,
        write_locks_lost: Arc<std::sync::atomic::AtomicBool>,
    }

    struct TestTableCatalogObjectLockGuard {
        _guard: tokio::sync::OwnedMutexGuard<()>,
        lost: Arc<std::sync::atomic::AtomicBool>,
    }

    impl crate::table_catalog::TableCatalogObjectLockGuard for TestTableCatalogObjectLockGuard {
        fn is_lock_lost(&self) -> bool {
            self.lost.load(std::sync::atomic::Ordering::Relaxed)
        }
    }

    impl TestTableCatalogObjectBackend {
        fn read_object_call_count(&self) -> usize {
            self.read_object_calls.load(std::sync::atomic::Ordering::Relaxed)
        }

        fn object_exists_max_in_flight(&self) -> usize {
            self.object_exists_max_in_flight.load(std::sync::atomic::Ordering::Relaxed)
        }

        async fn write_lock_is_held(&self, bucket: &str, object: &str) -> bool {
            let lock = self
                .locks
                .lock()
                .await
                .get(&(bucket.to_string(), object.to_string()))
                .cloned();
            lock.is_some_and(|lock| lock.try_lock_owned().is_err())
        }

        fn lose_write_locks(&self) {
            self.write_locks_lost.store(true, std::sync::atomic::Ordering::Relaxed);
        }

        async fn put_bytes(&self, bucket: &str, object: &str, data: Vec<u8>) {
            self.objects.lock().await.insert(
                (bucket.to_string(), object.to_string()),
                crate::table_catalog::TableCatalogObject {
                    data,
                    etag: Some("etag".to_string()),
                    mod_time: None,
                },
            );
        }

        async fn fail_next_put(&self, bucket: &str, object: &str) {
            self.fail_next_puts
                .lock()
                .await
                .insert((bucket.to_string(), object.to_string()));
        }

        async fn put_json(&self, bucket: &str, object: &str, value: serde_json::Value) {
            self.put_json_with_mod_time(bucket, object, value, None).await;
        }

        async fn put_json_with_mod_time(
            &self,
            bucket: &str,
            object: &str,
            value: serde_json::Value,
            mod_time: Option<OffsetDateTime>,
        ) {
            let data = serde_json::to_vec(&value).expect("metadata JSON should serialize");
            self.objects.lock().await.insert(
                (bucket.to_string(), object.to_string()),
                crate::table_catalog::TableCatalogObject {
                    data,
                    etag: Some("etag".to_string()),
                    mod_time,
                },
            );
        }
    }

    fn test_snapshot_object_key(bucket: &str, location: &str) -> String {
        crate::table_catalog::table_catalog_object_key_from_location(bucket, location)
            .expect("test snapshot object location should be valid")
    }

    fn test_manifest_list_avro_bytes(manifest_paths: &[&str], sequence_number: i64, snapshot_id: i64) -> Vec<u8> {
        let manifests = manifest_paths
            .iter()
            .map(|manifest_path| (*manifest_path, 0, sequence_number, snapshot_id))
            .collect::<Vec<_>>();
        test_manifest_list_avro_entries_with_spec(&manifests)
    }

    fn test_manifest_list_avro_entries(manifests: &[(&str, i64, i64)]) -> Vec<u8> {
        let manifests = manifests
            .iter()
            .map(|(manifest_path, sequence_number, snapshot_id)| (*manifest_path, 0, *sequence_number, *snapshot_id))
            .collect::<Vec<_>>();
        test_manifest_list_avro_entries_with_spec(&manifests)
    }

    fn test_manifest_list_avro_entries_with_spec(manifests: &[(&str, i32, i64, i64)]) -> Vec<u8> {
        let schema = apache_avro::Schema::parse_str(
            r#"
            {
              "type": "record",
              "name": "manifest_file",
              "fields": [
                {"name": "manifest_path", "type": "string"},
                {"name": "partition_spec_id", "type": "int"},
                {"name": "sequence_number", "type": "long"},
                {"name": "added_snapshot_id", "type": "long"}
              ]
            }
            "#,
        )
        .expect("manifest list avro schema should parse");
        let mut writer = apache_avro::Writer::new(&schema, Vec::new());
        for (manifest_path, partition_spec_id, sequence_number, snapshot_id) in manifests {
            writer
                .append(apache_avro::types::Value::Record(vec![
                    (
                        "manifest_path".to_string(),
                        apache_avro::types::Value::String((*manifest_path).to_string()),
                    ),
                    ("partition_spec_id".to_string(), apache_avro::types::Value::Int(*partition_spec_id)),
                    ("sequence_number".to_string(), apache_avro::types::Value::Long(*sequence_number)),
                    ("added_snapshot_id".to_string(), apache_avro::types::Value::Long(*snapshot_id)),
                ]))
                .expect("manifest list record should append");
        }
        writer.into_inner().expect("manifest list avro bytes should flush")
    }

    fn test_manifest_avro_bytes(files: &[(&str, i32, i32, i64, i64)]) -> Vec<u8> {
        let schema = apache_avro::Schema::parse_str(
            r#"
            {
              "type": "record",
              "name": "manifest_entry",
              "fields": [
                {"name": "status", "type": "int"},
                {"name": "snapshot_id", "type": "long"},
                {"name": "sequence_number", "type": "long"},
                {"name": "file_sequence_number", "type": "long"},
                {
                  "name": "data_file",
                  "type": {
                    "type": "record",
                    "name": "data_file",
                    "fields": [
                      {"name": "content", "type": "int"},
                      {"name": "file_path", "type": "string"},
                      {"name": "record_count", "type": "long"},
                      {"name": "file_size_in_bytes", "type": "long"}
                    ]
                  }
                }
              ]
            }
            "#,
        )
        .expect("manifest avro schema should parse");
        let mut writer = apache_avro::Writer::new(&schema, Vec::new());
        for (file_path, content, status, snapshot_id, sequence_number) in files {
            writer
                .append(apache_avro::types::Value::Record(vec![
                    ("status".to_string(), apache_avro::types::Value::Int(*status)),
                    ("snapshot_id".to_string(), apache_avro::types::Value::Long(*snapshot_id)),
                    ("sequence_number".to_string(), apache_avro::types::Value::Long(*sequence_number)),
                    ("file_sequence_number".to_string(), apache_avro::types::Value::Long(*sequence_number)),
                    (
                        "data_file".to_string(),
                        apache_avro::types::Value::Record(vec![
                            ("content".to_string(), apache_avro::types::Value::Int(*content)),
                            ("file_path".to_string(), apache_avro::types::Value::String((*file_path).to_string())),
                            ("record_count".to_string(), apache_avro::types::Value::Long(1)),
                            ("file_size_in_bytes".to_string(), apache_avro::types::Value::Long(1)),
                        ]),
                    ),
                ]))
                .expect("manifest record should append");
        }
        writer.into_inner().expect("manifest avro bytes should flush")
    }

    fn test_nullable_long(value: Option<i64>) -> apache_avro::types::Value {
        match value {
            Some(value) => apache_avro::types::Value::Union(1, Box::new(apache_avro::types::Value::Long(value))),
            None => apache_avro::types::Value::Union(0, Box::new(apache_avro::types::Value::Null)),
        }
    }

    fn test_manifest_avro_bytes_with_nullable_sequences(files: &[(&str, i32, i32, i64, Option<i64>)]) -> Vec<u8> {
        let schema = apache_avro::Schema::parse_str(
            r#"
            {
              "type": "record",
              "name": "manifest_entry",
              "fields": [
                {"name": "status", "type": "int"},
                {"name": "snapshot_id", "type": "long"},
                {"name": "sequence_number", "type": ["null", "long"], "default": null},
                {"name": "file_sequence_number", "type": ["null", "long"], "default": null},
                {
                  "name": "data_file",
                  "type": {
                    "type": "record",
                    "name": "data_file",
                    "fields": [
                      {"name": "content", "type": "int"},
                      {"name": "file_path", "type": "string"},
                      {"name": "record_count", "type": "long"},
                      {"name": "file_size_in_bytes", "type": "long"}
                    ]
                  }
                }
              ]
            }
            "#,
        )
        .expect("manifest avro schema should parse");
        let mut writer = apache_avro::Writer::new(&schema, Vec::new());
        for (file_path, content, status, snapshot_id, sequence_number) in files {
            writer
                .append(apache_avro::types::Value::Record(vec![
                    ("status".to_string(), apache_avro::types::Value::Int(*status)),
                    ("snapshot_id".to_string(), apache_avro::types::Value::Long(*snapshot_id)),
                    ("sequence_number".to_string(), test_nullable_long(*sequence_number)),
                    ("file_sequence_number".to_string(), test_nullable_long(*sequence_number)),
                    (
                        "data_file".to_string(),
                        apache_avro::types::Value::Record(vec![
                            ("content".to_string(), apache_avro::types::Value::Int(*content)),
                            ("file_path".to_string(), apache_avro::types::Value::String((*file_path).to_string())),
                            ("record_count".to_string(), apache_avro::types::Value::Long(1)),
                            ("file_size_in_bytes".to_string(), apache_avro::types::Value::Long(1)),
                        ]),
                    ),
                ]))
                .expect("manifest record should append");
        }
        writer.into_inner().expect("manifest avro bytes should flush")
    }

    async fn seed_test_manifest_list(
        backend: &TestTableCatalogObjectBackend,
        bucket: &str,
        manifest_list_location: &str,
        manifest_locations: &[&str],
        sequence_number: i64,
        snapshot_id: i64,
    ) {
        let manifest_list_key = test_snapshot_object_key(bucket, manifest_list_location);
        backend
            .put_bytes(
                bucket,
                &manifest_list_key,
                test_manifest_list_avro_bytes(manifest_locations, sequence_number, snapshot_id),
            )
            .await;
    }

    async fn seed_test_snapshot_manifest(
        backend: &TestTableCatalogObjectBackend,
        bucket: &str,
        manifest_list_location: &str,
        snapshot_id: i64,
        sequence_number: i64,
        files: &[(&str, i32, i32, i64, i64)],
    ) {
        let manifest_location = manifest_list_location
            .rsplit_once('/')
            .map(|(prefix, name)| format!("{prefix}/manifest-{name}"))
            .expect("manifest list location should include a file name");
        let manifest_key = test_snapshot_object_key(bucket, &manifest_location);
        let manifest_list_key = test_snapshot_object_key(bucket, manifest_list_location);
        backend
            .put_bytes(
                bucket,
                &manifest_list_key,
                test_manifest_list_avro_bytes(&[&manifest_location], sequence_number, snapshot_id),
            )
            .await;
        backend
            .put_bytes(bucket, &manifest_key, test_manifest_avro_bytes(files))
            .await;
        seed_test_manifest_data_files(backend, bucket, files).await;
    }

    async fn seed_test_manifest_with_nullable_sequences(
        backend: &TestTableCatalogObjectBackend,
        bucket: &str,
        manifest_location: &str,
        files: &[(&str, i32, i32, i64, Option<i64>)],
    ) {
        let manifest_key = test_snapshot_object_key(bucket, manifest_location);
        backend
            .put_bytes(bucket, &manifest_key, test_manifest_avro_bytes_with_nullable_sequences(files))
            .await;
        let data_files = files
            .iter()
            .map(|(file_path, content, status, snapshot_id, sequence_number)| {
                (*file_path, *content, *status, *snapshot_id, sequence_number.unwrap_or_default())
            })
            .collect::<Vec<_>>();
        seed_test_manifest_data_files(backend, bucket, &data_files).await;
    }

    async fn seed_test_manifest(
        backend: &TestTableCatalogObjectBackend,
        bucket: &str,
        manifest_location: &str,
        files: &[(&str, i32, i32, i64, i64)],
    ) {
        let manifest_key = test_snapshot_object_key(bucket, manifest_location);
        backend
            .put_bytes(bucket, &manifest_key, test_manifest_avro_bytes(files))
            .await;
        seed_test_manifest_data_files(backend, bucket, files).await;
    }

    async fn seed_test_manifest_data_files(
        backend: &TestTableCatalogObjectBackend,
        bucket: &str,
        files: &[(&str, i32, i32, i64, i64)],
    ) {
        for (file_path, _, status, _, _) in files {
            if *status != 2 {
                let object_key = test_snapshot_object_key(bucket, file_path);
                backend.put_bytes(bucket, &object_key, b"data".to_vec()).await;
            }
        }
    }

    async fn create_standard_events_table<S>(
        store: &S,
        metadata_backend: &TestTableCatalogObjectBackend,
        namespace: &crate::table_catalog::Namespace,
    ) -> RestLoadTableResponse
    where
        S: crate::table_catalog::TableCatalogStore + ?Sized,
    {
        ensure_table_bucket_entry(store, "warehouse", true)
            .await
            .expect("table bucket entry should be seeded");
        create_namespace_response(
            store,
            "warehouse",
            CreateNamespaceRequest {
                namespace: namespace.public_name().split('.').map(str::to_string).collect(),
                properties: BTreeMap::new(),
            },
            true,
        )
        .await
        .expect("namespace should be created");
        let create_request: CreateTableRequest = serde_json::from_value(serde_json::json!({
            "name": "events",
            "schema": {
                "type": "struct",
                "schema-id": 0,
                "fields": [
                    {
                        "id": 1,
                        "name": "id",
                        "required": true,
                        "type": "long"
                    }
                ]
            }
        }))
        .expect("standard create table request should parse");
        create_table_response(store, metadata_backend, "warehouse", namespace, create_request, true)
            .await
            .expect("table should be created")
    }

    async fn seed_object_table_for_metadata_maintenance(
        store: &crate::table_catalog::ObjectTableCatalogStore<TestTableCatalogObjectBackend>,
        backend: &TestTableCatalogObjectBackend,
        bucket: &str,
        namespace: &crate::table_catalog::Namespace,
        table: &crate::table_catalog::IdentifierSegment,
        current_metadata_location: String,
    ) {
        store
            .put_table_bucket(crate::table_catalog::TableBucketEntry {
                version: crate::table_catalog::TABLE_CATALOG_ENTRY_VERSION,
                table_bucket: bucket.to_string(),
                catalog_type: crate::table_catalog::TABLE_BUCKET_CATALOG_TYPE.to_string(),
                warehouse_root: format!("s3://{bucket}/"),
                state: crate::table_catalog::TableCatalogEntryState::Active,
                properties: BTreeMap::new(),
                created_at: None,
                updated_at: None,
            })
            .await
            .expect("table bucket entry should seed");
        store
            .create_namespace(crate::table_catalog::NamespaceEntry {
                version: crate::table_catalog::TABLE_CATALOG_ENTRY_VERSION,
                table_bucket: bucket.to_string(),
                namespace: namespace.public_name(),
                namespace_id: namespace.storage_id(),
                state: crate::table_catalog::TableCatalogEntryState::Active,
                properties: BTreeMap::new(),
                created_at: None,
                updated_at: None,
            })
            .await
            .expect("namespace entry should seed");
        store
            .create_table(crate::table_catalog::TableEntry {
                version: crate::table_catalog::TABLE_CATALOG_ENTRY_VERSION,
                table_bucket: bucket.to_string(),
                namespace: namespace.public_name(),
                table: table.as_str().to_string(),
                table_id: "table-id".to_string(),
                table_uuid: "table-uuid".to_string(),
                format: "ICEBERG".to_string(),
                format_version: 2,
                warehouse_location: format!("s3://{bucket}/tables/table-id"),
                metadata_location: current_metadata_location,
                version_token: "token-v1".to_string(),
                generation: 1,
                state: crate::table_catalog::TableCatalogEntryState::Active,
                properties: BTreeMap::new(),
                created_at: None,
                updated_at: None,
            })
            .await
            .expect("table entry should seed");
        backend
            .put_json(bucket, "unrelated/ignored.json", serde_json::json!({}))
            .await;
    }

    #[async_trait::async_trait]
    impl crate::table_catalog::TableCatalogObjectBackend for TestTableCatalogObjectBackend {
        async fn read_object(
            &self,
            bucket: &str,
            object: &str,
        ) -> crate::table_catalog::TableCatalogStoreResult<Option<crate::table_catalog::TableCatalogObject>> {
            self.read_object_calls.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Ok(self
                .objects
                .lock()
                .await
                .get(&(bucket.to_string(), object.to_string()))
                .cloned())
        }

        async fn object_exists(&self, bucket: &str, object: &str) -> crate::table_catalog::TableCatalogStoreResult<bool> {
            let in_flight = self
                .object_exists_in_flight
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                + 1;
            self.object_exists_max_in_flight
                .fetch_max(in_flight, std::sync::atomic::Ordering::Relaxed);
            if let Some(delay) = self.object_exists_delay {
                tokio::time::sleep(delay).await;
            }
            let exists = self
                .objects
                .lock()
                .await
                .contains_key(&(bucket.to_string(), object.to_string()));
            self.object_exists_in_flight
                .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
            Ok(exists)
        }

        async fn put_object(
            &self,
            bucket: &str,
            object: &str,
            data: Vec<u8>,
            precondition: crate::table_catalog::TableCatalogPutPrecondition,
        ) -> crate::table_catalog::TableCatalogStoreResult<()> {
            let key = (bucket.to_string(), object.to_string());
            if self.fail_next_puts.lock().await.remove(&key) {
                return Err(crate::table_catalog::TableCatalogStoreError::Internal(format!(
                    "injected object write failure for {object}"
                )));
            }
            let mut objects = self.objects.lock().await;
            let result = if matches!(precondition, crate::table_catalog::TableCatalogPutPrecondition::IfAbsent)
                && objects.contains_key(&key)
            {
                Err(crate::table_catalog::TableCatalogStoreError::Conflict(format!(
                    "object already exists: {object}"
                )))
            } else {
                objects.insert(
                    key,
                    crate::table_catalog::TableCatalogObject {
                        data,
                        etag: Some("etag".to_string()),
                        mod_time: None,
                    },
                );
                Ok(())
            };
            drop(objects);
            if let Some(barrier) = &self.put_object_barrier {
                barrier.wait().await;
            }
            result
        }

        async fn delete_object(&self, bucket: &str, object: &str) -> crate::table_catalog::TableCatalogStoreResult<()> {
            self.objects.lock().await.remove(&(bucket.to_string(), object.to_string()));
            Ok(())
        }

        async fn list_objects(&self, bucket: &str, prefix: &str) -> crate::table_catalog::TableCatalogStoreResult<Vec<String>> {
            Ok(self
                .objects
                .lock()
                .await
                .keys()
                .filter(|(object_bucket, object)| object_bucket == bucket && object.starts_with(prefix))
                .map(|(_, object)| object.clone())
                .collect())
        }

        async fn acquire_write_lock(
            &self,
            bucket: &str,
            object: &str,
        ) -> crate::table_catalog::TableCatalogStoreResult<crate::table_catalog::TableCatalogObjectLock> {
            let lock = {
                let mut locks = self.locks.lock().await;
                locks
                    .entry((bucket.to_string(), object.to_string()))
                    .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
                    .clone()
            };
            Ok(Box::new(TestTableCatalogObjectLockGuard {
                _guard: lock.lock_owned().await,
                lost: Arc::clone(&self.write_locks_lost),
            }))
        }
    }

    #[async_trait::async_trait]
    impl crate::table_catalog::TableCatalogStore for TestTableCatalogStore {
        async fn get_table_bucket(
            &self,
            table_bucket: &str,
        ) -> crate::table_catalog::TableCatalogStoreResult<Option<crate::table_catalog::TableBucketEntry>> {
            Ok(self
                .table_buckets
                .lock()
                .await
                .iter()
                .find(|entry| entry.table_bucket == table_bucket)
                .cloned())
        }

        async fn put_table_bucket(
            &self,
            entry: crate::table_catalog::TableBucketEntry,
        ) -> crate::table_catalog::TableCatalogStoreResult<()> {
            let mut fail_put_table_bucket = self.fail_put_table_bucket.lock().await;
            if *fail_put_table_bucket {
                *fail_put_table_bucket = false;
                return Err(crate::table_catalog::TableCatalogStoreError::Internal(
                    "injected table bucket write failure".to_string(),
                ));
            }
            drop(fail_put_table_bucket);

            let mut table_buckets = self.table_buckets.lock().await;
            table_buckets.retain(|existing| existing.table_bucket != entry.table_bucket);
            table_buckets.push(entry);
            Ok(())
        }

        async fn create_namespace(
            &self,
            entry: crate::table_catalog::NamespaceEntry,
        ) -> crate::table_catalog::TableCatalogStoreResult<()> {
            if self.get_table_bucket(&entry.table_bucket).await?.is_none() {
                return Err(crate::table_catalog::TableCatalogStoreError::NotFound(format!(
                    "table bucket {}",
                    entry.table_bucket
                )));
            }
            self.namespaces.lock().await.push(entry);
            Ok(())
        }

        async fn list_namespaces(
            &self,
            table_bucket: &str,
        ) -> crate::table_catalog::TableCatalogStoreResult<Vec<crate::table_catalog::NamespaceEntry>> {
            Ok(self
                .namespaces
                .lock()
                .await
                .iter()
                .filter(|entry| entry.table_bucket == table_bucket)
                .cloned()
                .collect())
        }

        async fn get_namespace(
            &self,
            table_bucket: &str,
            namespace: &str,
        ) -> crate::table_catalog::TableCatalogStoreResult<Option<crate::table_catalog::NamespaceEntry>> {
            Ok(self
                .namespaces
                .lock()
                .await
                .iter()
                .find(|entry| entry.table_bucket == table_bucket && entry.namespace == namespace)
                .cloned())
        }

        async fn update_namespace_properties(
            &self,
            table_bucket: &str,
            namespace: &str,
            update: crate::table_catalog::NamespacePropertiesUpdate,
        ) -> crate::table_catalog::TableCatalogStoreResult<crate::table_catalog::NamespacePropertiesUpdateResult> {
            let mut namespaces = self.namespaces.lock().await;
            let Some(entry) = namespaces
                .iter_mut()
                .find(|entry| entry.table_bucket == table_bucket && entry.namespace == namespace)
            else {
                return Err(crate::table_catalog::TableCatalogStoreError::NamespaceNotFound(format!(
                    "{table_bucket}/{namespace}"
                )));
            };
            Ok(update.apply_to(entry))
        }

        async fn drop_namespace(&self, table_bucket: &str, namespace: &str) -> crate::table_catalog::TableCatalogStoreResult<()> {
            self.namespaces
                .lock()
                .await
                .retain(|entry| !(entry.table_bucket == table_bucket && entry.namespace == namespace));
            Ok(())
        }

        async fn create_table(
            &self,
            entry: crate::table_catalog::TableEntry,
        ) -> crate::table_catalog::TableCatalogStoreResult<()> {
            if self.get_table_bucket(&entry.table_bucket).await?.is_none() {
                return Err(crate::table_catalog::TableCatalogStoreError::NotFound(format!(
                    "table bucket {}",
                    entry.table_bucket
                )));
            }
            if self.get_namespace(&entry.table_bucket, &entry.namespace).await?.is_none() {
                return Err(crate::table_catalog::TableCatalogStoreError::NotFound(format!(
                    "namespace {}/{}",
                    entry.table_bucket, entry.namespace
                )));
            }
            if let Some(pause) = &self.create_table_pause {
                pause.started.notify_one();
                pause.release.notified().await;
            }
            if self.fail_create_table_after_pause {
                return Err(crate::table_catalog::TableCatalogStoreError::Internal(
                    "injected table publication failure".to_string(),
                ));
            }
            self.tables.lock().await.push(entry);
            Ok(())
        }

        async fn register_table(
            &self,
            entry: crate::table_catalog::TableEntry,
        ) -> crate::table_catalog::TableCatalogStoreResult<()> {
            if self.get_table_bucket(&entry.table_bucket).await?.is_none() {
                return Err(crate::table_catalog::TableCatalogStoreError::NotFound(format!(
                    "table bucket {}",
                    entry.table_bucket
                )));
            }
            if self.get_namespace(&entry.table_bucket, &entry.namespace).await?.is_none() {
                return Err(crate::table_catalog::TableCatalogStoreError::NotFound(format!(
                    "namespace {}/{}",
                    entry.table_bucket, entry.namespace
                )));
            }
            self.tables.lock().await.push(entry);
            Ok(())
        }

        async fn list_tables(
            &self,
            table_bucket: &str,
            namespace: &str,
        ) -> crate::table_catalog::TableCatalogStoreResult<Vec<crate::table_catalog::TableEntry>> {
            Ok(self
                .tables
                .lock()
                .await
                .iter()
                .filter(|entry| entry.table_bucket == table_bucket && entry.namespace == namespace)
                .cloned()
                .collect())
        }

        async fn load_table(
            &self,
            table_bucket: &str,
            namespace: &str,
            table: &str,
        ) -> crate::table_catalog::TableCatalogStoreResult<Option<crate::table_catalog::TableEntry>> {
            Ok(self
                .tables
                .lock()
                .await
                .iter()
                .find(|entry| entry.table_bucket == table_bucket && entry.namespace == namespace && entry.table == table)
                .cloned())
        }

        async fn commit_table(
            &self,
            request: crate::table_catalog::TableCommitRequest,
        ) -> crate::table_catalog::TableCatalogStoreResult<crate::table_catalog::TableCommitResult> {
            let mut tables = self.tables.lock().await;
            let Some(index) = tables.iter().position(|entry| {
                entry.table_bucket == request.table_bucket && entry.namespace == request.namespace && entry.table == request.table
            }) else {
                return Err(crate::table_catalog::TableCatalogStoreError::NotFound(format!(
                    "table {}/{}/{}",
                    request.table_bucket, request.namespace, request.table
                )));
            };

            let current = tables[index].clone();
            if current.version_token != request.expected_version_token {
                return Err(crate::table_catalog::TableCatalogStoreError::Conflict(
                    "current table version token does not match expected token".to_string(),
                ));
            }
            if current.metadata_location != request.expected_metadata_location {
                return Err(crate::table_catalog::TableCatalogStoreError::Conflict(
                    "current table metadata location does not match expected location".to_string(),
                ));
            }

            let mut next = current.clone();
            next.metadata_location = request.new_metadata_location.clone();
            next.version_token = "token-committed".to_string();
            next.generation = crate::table_catalog::next_table_catalog_generation(next.generation)?;
            tables[index] = next.clone();
            drop(tables);

            let commit_log = crate::table_catalog::CommitLogEntry {
                version: crate::table_catalog::TABLE_CATALOG_ENTRY_VERSION,
                commit_id: request.commit_id,
                idempotency_key: request.idempotency_key,
                table_id: current.table_id,
                operation: request.operation,
                expected_version_token: request.expected_version_token,
                new_version_token: next.version_token.clone(),
                previous_metadata_location: request.expected_metadata_location,
                new_metadata_location: request.new_metadata_location,
                requirements: request.requirements,
                status: crate::table_catalog::CommitLogStatus::Committed,
                writer: request.writer,
                created_at: None,
                updated_at: None,
            };
            self.commits.lock().await.push(commit_log.clone());

            Ok(crate::table_catalog::TableCommitResult { table: next, commit_log })
        }

        async fn drop_table(
            &self,
            table_bucket: &str,
            namespace: &str,
            table: &str,
        ) -> crate::table_catalog::TableCatalogStoreResult<()> {
            self.tables
                .lock()
                .await
                .retain(|entry| !(entry.table_bucket == table_bucket && entry.namespace == namespace && entry.table == table));
            Ok(())
        }

        async fn create_view(&self, entry: crate::table_catalog::ViewEntry) -> crate::table_catalog::TableCatalogStoreResult<()> {
            if self.get_table_bucket(&entry.table_bucket).await?.is_none() {
                return Err(crate::table_catalog::TableCatalogStoreError::NotFound(format!(
                    "table bucket {}",
                    entry.table_bucket
                )));
            }
            if self.get_namespace(&entry.table_bucket, &entry.namespace).await?.is_none() {
                return Err(crate::table_catalog::TableCatalogStoreError::NotFound(format!(
                    "namespace {}/{}",
                    entry.table_bucket, entry.namespace
                )));
            }
            if let Some(pause) = &self.create_view_pause {
                pause.started.notify_one();
                pause.release.notified().await;
            }
            self.views.lock().await.push(entry);
            Ok(())
        }

        async fn list_views(
            &self,
            table_bucket: &str,
            namespace: &str,
        ) -> crate::table_catalog::TableCatalogStoreResult<Vec<crate::table_catalog::ViewEntry>> {
            Ok(self
                .views
                .lock()
                .await
                .iter()
                .filter(|entry| entry.table_bucket == table_bucket && entry.namespace == namespace)
                .cloned()
                .collect())
        }

        async fn load_view(
            &self,
            table_bucket: &str,
            namespace: &str,
            view: &str,
        ) -> crate::table_catalog::TableCatalogStoreResult<Option<crate::table_catalog::ViewEntry>> {
            Ok(self
                .views
                .lock()
                .await
                .iter()
                .find(|entry| entry.table_bucket == table_bucket && entry.namespace == namespace && entry.view == view)
                .cloned())
        }

        async fn replace_view(
            &self,
            request: crate::table_catalog::ViewCommitRequest,
        ) -> crate::table_catalog::TableCatalogStoreResult<crate::table_catalog::ViewCommitResult> {
            let mut views = self.views.lock().await;
            let Some(index) = views.iter().position(|entry| {
                entry.table_bucket == request.table_bucket && entry.namespace == request.namespace && entry.view == request.view
            }) else {
                return Err(crate::table_catalog::TableCatalogStoreError::NotFound(format!(
                    "view {}/{}/{}",
                    request.table_bucket, request.namespace, request.view
                )));
            };
            let current = views[index].clone();
            if current.version_token != request.expected_version_token {
                return Err(crate::table_catalog::TableCatalogStoreError::Conflict(
                    "current view version token does not match expected token".to_string(),
                ));
            }
            if current.metadata_location != request.expected_metadata_location {
                return Err(crate::table_catalog::TableCatalogStoreError::Conflict(
                    "current view metadata location does not match expected location".to_string(),
                ));
            }
            let mut next = current;
            next.metadata_location = request.new_metadata_location;
            next.version_token = "token-view-committed".to_string();
            next.generation = crate::table_catalog::next_table_catalog_generation(next.generation)?;
            views[index] = next.clone();
            Ok(crate::table_catalog::ViewCommitResult { view: next })
        }

        async fn drop_view(
            &self,
            table_bucket: &str,
            namespace: &str,
            view: &str,
        ) -> crate::table_catalog::TableCatalogStoreResult<()> {
            self.views
                .lock()
                .await
                .retain(|entry| !(entry.table_bucket == table_bucket && entry.namespace == namespace && entry.view == view));
            Ok(())
        }

        async fn get_commit_by_id(
            &self,
            _table_bucket: &str,
            _table_id: &str,
            _commit_id: &str,
        ) -> crate::table_catalog::TableCatalogStoreResult<Option<crate::table_catalog::CommitLogEntry>> {
            Ok(None)
        }

        async fn get_commit_by_idempotency_key(
            &self,
            _table_bucket: &str,
            _table_id: &str,
            _idempotency_key: &str,
        ) -> crate::table_catalog::TableCatalogStoreResult<Option<crate::table_catalog::CommitLogEntry>> {
            Ok(None)
        }
    }

    #[tokio::test]
    async fn ensure_table_bucket_entry_seeds_enabled_bucket_before_namespace_create() {
        let store = TestTableCatalogStore::default();

        ensure_table_bucket_entry(&store, "warehouse", true)
            .await
            .expect("table bucket entry should be seeded");
        let table_bucket = store
            .get_table_bucket("warehouse")
            .await
            .expect("table bucket lookup should succeed")
            .expect("table bucket entry should exist");

        assert_eq!(table_bucket.table_bucket, "warehouse");
        assert_eq!(table_bucket.catalog_type, crate::table_catalog::TABLE_BUCKET_CATALOG_TYPE);
        assert_eq!(table_bucket.warehouse_root, "s3://warehouse/");
    }

    #[tokio::test]
    async fn ensure_table_bucket_entry_rejects_bucket_without_table_marker() {
        let store = TestTableCatalogStore::default();

        assert!(ensure_table_bucket_entry(&store, "warehouse", false).await.is_err());
        assert!(
            store
                .get_table_bucket("warehouse")
                .await
                .expect("table bucket lookup should succeed")
                .is_none()
        );
    }

    #[tokio::test]
    async fn ensure_table_bucket_entry_propagates_catalog_entry_failure() {
        let store = TestTableCatalogStore::default();
        *store.fail_put_table_bucket.lock().await = true;

        assert!(ensure_table_bucket_entry(&store, "warehouse", true).await.is_err());
        assert!(
            store
                .get_table_bucket("warehouse")
                .await
                .expect("table bucket lookup should succeed")
                .is_none()
        );
        assert!(!*store.fail_put_table_bucket.lock().await);
    }

    #[tokio::test]
    async fn namespace_helpers_call_catalog_store() {
        let store = TestTableCatalogStore::default();
        ensure_table_bucket_entry(&store, "warehouse", true)
            .await
            .expect("table bucket entry should be seeded");
        let create = create_namespace_response(
            &store,
            "warehouse",
            CreateNamespaceRequest {
                namespace: vec!["analytics".to_string()],
                properties: BTreeMap::from([("owner".to_string(), "lakehouse".to_string())]),
            },
            true,
        )
        .await
        .expect("namespace should be created");

        assert_eq!(create.namespace, vec!["analytics".to_string()]);
        assert_eq!(create.properties.get("owner").map(String::as_str), Some("lakehouse"));

        let unpaginated_uri = "/".parse::<http::Uri>().expect("list URI should parse");
        let list = list_namespaces_response(&store, "warehouse", None, &unpaginated_uri)
            .await
            .expect("namespace list should load");
        assert_eq!(list.namespaces, vec![vec!["analytics".to_string()]]);

        let update = update_namespace_properties_response(
            &store,
            "warehouse",
            &crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse"),
            UpdateNamespacePropertiesRequest {
                removals: vec!["missing".to_string()],
                updates: BTreeMap::from([("owner".to_string(), "platform".to_string())]),
            },
        )
        .await
        .expect("namespace properties should update");
        assert_eq!(update.updated, vec!["owner".to_string()]);
        assert!(update.removed.is_empty());
        assert_eq!(update.missing, vec!["missing".to_string()]);
        let namespace = store
            .get_namespace("warehouse", "analytics")
            .await
            .expect("namespace lookup should succeed")
            .expect("namespace should remain");
        assert_eq!(namespace.properties.get("owner").map(String::as_str), Some("platform"));

        drop_namespace_in_store(&store, "warehouse", "analytics")
            .await
            .expect("namespace should drop");
        let list = list_namespaces_response(&store, "warehouse", None, &unpaginated_uri)
            .await
            .expect("namespace list should load after drop");
        assert!(list.namespaces.is_empty());
    }

    #[tokio::test]
    async fn namespace_list_returns_only_direct_children_and_binds_parent_pagination() {
        let store = TestTableCatalogStore::default();
        ensure_table_bucket_entry(&store, "warehouse", true)
            .await
            .expect("table bucket entry should be seeded");
        for namespace in [
            vec!["analytics"],
            vec!["analytics", "curated"],
            vec!["analytics", "raw"],
            vec!["analytics", "raw", "daily"],
            vec!["sales"],
        ] {
            create_namespace_response(
                &store,
                "warehouse",
                CreateNamespaceRequest {
                    namespace: namespace.into_iter().map(str::to_string).collect(),
                    properties: BTreeMap::new(),
                },
                true,
            )
            .await
            .expect("namespace should be created");
        }

        let unpaginated_uri = "/".parse::<http::Uri>().expect("list URI should parse");
        let top_level = list_namespaces_response(&store, "warehouse", None, &unpaginated_uri)
            .await
            .expect("top-level namespaces should list");
        assert_eq!(top_level.namespaces, vec![vec!["analytics".to_string()], vec!["sales".to_string()]]);

        let analytics = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
        let first_page_uri = "/?pageSize=1".parse::<http::Uri>().expect("first page URI should parse");
        let children = list_namespaces_response(&store, "warehouse", Some(&analytics), &first_page_uri)
            .await
            .expect("direct children should list");
        assert_eq!(children.namespaces, vec![vec!["analytics".to_string(), "curated".to_string()]]);
        let next_page_token = children.next_page_token.expect("first child page should return a token");
        let second_page_uri = format!("/?pageSize=1&pageToken={next_page_token}")
            .parse::<http::Uri>()
            .expect("second page URI should parse");
        let second_page = list_namespaces_response(&store, "warehouse", Some(&analytics), &second_page_uri)
            .await
            .expect("second child page should list");
        assert_eq!(second_page.namespaces, vec![vec!["analytics".to_string(), "raw".to_string()]]);
        assert!(second_page.next_page_token.is_none());

        let missing = crate::table_catalog::Namespace::parse("missing").expect("namespace should parse");
        let error = list_namespaces_response(&store, "warehouse", Some(&missing), &unpaginated_uri)
            .await
            .expect_err("missing parent should fail");
        assert_eq!(error.code(), &S3ErrorCode::Custom(ICEBERG_ERROR_NO_SUCH_NAMESPACE.into()));
        assert_eq!(error.status_code(), Some(StatusCode::NOT_FOUND));

        let parent_context = RestPageContext {
            resource: TABLE_CATALOG_NAMESPACE_RESOURCE_ROOT,
            warehouse: "warehouse",
            namespace: Some("analytics"),
        };
        let token = encode_rest_page_token("analytics.raw", &rest_page_context_fingerprint(parent_context))
            .expect("parent token should encode");
        let wrong_parent_context = RestPageContext {
            resource: TABLE_CATALOG_NAMESPACE_RESOURCE_ROOT,
            warehouse: "warehouse",
            namespace: Some("sales"),
        };
        let error = decode_rest_page_token(&token, &rest_page_context_fingerprint(wrong_parent_context))
            .expect_err("token must stay parent-scoped");
        assert_eq!(error.code(), &S3ErrorCode::Custom(ICEBERG_ERROR_BAD_REQUEST.into()));
    }

    #[tokio::test]
    async fn namespace_list_synthesizes_ancestors_and_hides_inactive_entries() {
        let store = TestTableCatalogStore::default();
        ensure_table_bucket_entry(&store, "warehouse", true)
            .await
            .expect("table bucket entry should be seeded");
        for namespace in [vec!["analytics", "raw", "daily"], vec!["archived"]] {
            create_namespace_response(
                &store,
                "warehouse",
                CreateNamespaceRequest {
                    namespace: namespace.into_iter().map(str::to_string).collect(),
                    properties: BTreeMap::new(),
                },
                true,
            )
            .await
            .expect("namespace should be created");
        }
        store
            .namespaces
            .lock()
            .await
            .iter_mut()
            .find(|entry| entry.namespace == "archived")
            .expect("archived namespace should exist")
            .state = crate::table_catalog::TableCatalogEntryState::Deleted;

        let unpaginated_uri = "/".parse::<http::Uri>().expect("list URI should parse");
        let top_level = list_namespaces_response(&store, "warehouse", None, &unpaginated_uri)
            .await
            .expect("top-level namespaces should list");
        assert_eq!(top_level.namespaces, vec![vec!["analytics".to_string()]]);

        let analytics = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
        let children = list_namespaces_response(&store, "warehouse", Some(&analytics), &unpaginated_uri)
            .await
            .expect("synthesized namespace children should list");
        assert_eq!(children.namespaces, vec![vec!["analytics".to_string(), "raw".to_string()]]);

        let archived = crate::table_catalog::Namespace::parse("archived").expect("namespace should parse");
        let error = get_namespace_response(&store, "warehouse", &archived)
            .await
            .expect_err("inactive namespace should not load");
        assert_eq!(error.code(), &S3ErrorCode::Custom(ICEBERG_ERROR_NO_SUCH_NAMESPACE.into()));
        assert_eq!(
            namespace_exists_status(&store, "warehouse", &archived)
                .await
                .expect("namespace status should resolve"),
            StatusCode::NOT_FOUND
        );
    }

    #[tokio::test]
    async fn table_list_reports_missing_namespace() {
        let store = TestTableCatalogStore::default();
        let namespace = crate::table_catalog::Namespace::parse("missing").expect("namespace should parse");
        let uri = "/".parse::<http::Uri>().expect("list URI should parse");
        let error = list_tables_response(&store, "warehouse", &namespace, &uri)
            .await
            .expect_err("missing namespace should fail");
        assert_eq!(error.code(), &S3ErrorCode::Custom(ICEBERG_ERROR_NO_SUCH_NAMESPACE.into()));
        assert_eq!(error.status_code(), Some(StatusCode::NOT_FOUND));
    }

    #[tokio::test]
    async fn view_list_reports_missing_namespace_and_paginates_active_entries() {
        let store = TestTableCatalogStore::default();
        let missing = crate::table_catalog::Namespace::parse("missing").expect("namespace should parse");
        let unpaginated_uri = "/".parse::<http::Uri>().expect("list URI should parse");
        let error = list_views_response(&store, "warehouse", &missing, &unpaginated_uri)
            .await
            .expect_err("missing namespace should fail");
        assert_eq!(error.code(), &S3ErrorCode::Custom(ICEBERG_ERROR_NO_SUCH_NAMESPACE.into()));
        assert_eq!(error.status_code(), Some(StatusCode::NOT_FOUND));

        let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
        ensure_table_bucket_entry(&store, "warehouse", true)
            .await
            .expect("table bucket entry should be seeded");
        create_namespace_response(
            &store,
            "warehouse",
            CreateNamespaceRequest {
                namespace: vec!["analytics".to_string()],
                properties: BTreeMap::new(),
            },
            true,
        )
        .await
        .expect("namespace should be created");
        for (name, state) in [
            ("alpha", crate::table_catalog::TableCatalogEntryState::Active),
            ("beta", crate::table_catalog::TableCatalogEntryState::Active),
            ("deleted", crate::table_catalog::TableCatalogEntryState::Deleted),
            ("gamma", crate::table_catalog::TableCatalogEntryState::Active),
        ] {
            store.views.lock().await.push(crate::table_catalog::ViewEntry {
                version: crate::table_catalog::TABLE_CATALOG_ENTRY_VERSION,
                table_bucket: "warehouse".to_string(),
                namespace: namespace.public_name(),
                view: name.to_string(),
                view_id: format!("{name}-id"),
                view_uuid: format!("{name}-uuid"),
                format: "ICEBERG_VIEW".to_string(),
                format_version: 1,
                warehouse_location: format!("s3://warehouse/views/{name}-id"),
                metadata_location: crate::table_catalog::default_view_metadata_file_path(
                    &namespace,
                    &crate::table_catalog::IdentifierSegment::parse(name).expect("view should parse"),
                    "00001.metadata.json",
                ),
                version_token: "token-v1".to_string(),
                generation: 1,
                state,
                properties: BTreeMap::new(),
                created_at: None,
                updated_at: None,
            });
        }

        let first_page_uri = "/?pageSize=2".parse::<http::Uri>().expect("first page URI should parse");
        let first = list_views_response(&store, "warehouse", &namespace, &first_page_uri)
            .await
            .expect("first view page should load");
        assert_eq!(
            first
                .identifiers
                .iter()
                .map(|identifier| identifier.name.as_str())
                .collect::<Vec<_>>(),
            vec!["alpha", "beta"]
        );
        let token = first.next_page_token.expect("first view page should return a token");
        let second_page_uri = format!("/?pageSize=2&pageToken={token}")
            .parse::<http::Uri>()
            .expect("second page URI should parse");
        let second = list_views_response(&store, "warehouse", &namespace, &second_page_uri)
            .await
            .expect("second view page should load");
        assert_eq!(
            second
                .identifiers
                .iter()
                .map(|identifier| identifier.name.as_str())
                .collect::<Vec<_>>(),
            vec!["gamma"]
        );
        assert!(second.next_page_token.is_none());
    }

    #[tokio::test]
    async fn table_list_hides_inactive_entries() {
        let store = TestTableCatalogStore::default();
        let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
        ensure_table_bucket_entry(&store, "warehouse", true)
            .await
            .expect("table bucket entry should be seeded");
        create_namespace_response(
            &store,
            "warehouse",
            CreateNamespaceRequest {
                namespace: vec!["analytics".to_string()],
                properties: BTreeMap::new(),
            },
            true,
        )
        .await
        .expect("namespace should be created");
        for (name, state) in [
            ("active", crate::table_catalog::TableCatalogEntryState::Active),
            ("deleted", crate::table_catalog::TableCatalogEntryState::Deleted),
        ] {
            store.tables.lock().await.push(crate::table_catalog::TableEntry {
                version: crate::table_catalog::TABLE_CATALOG_ENTRY_VERSION,
                table_bucket: "warehouse".to_string(),
                namespace: "analytics".to_string(),
                table: name.to_string(),
                table_id: format!("{name}-id"),
                table_uuid: format!("{name}-uuid"),
                format: "ICEBERG".to_string(),
                format_version: 2,
                warehouse_location: format!("s3://warehouse/tables/{name}-id"),
                metadata_location: format!("metadata/{name}.metadata.json"),
                version_token: "token-v1".to_string(),
                generation: 1,
                state,
                properties: BTreeMap::new(),
                created_at: None,
                updated_at: None,
            });
        }

        let uri = "/".parse::<http::Uri>().expect("list URI should parse");
        let response = list_tables_response(&store, "warehouse", &namespace, &uri)
            .await
            .expect("table list should load");
        assert_eq!(
            response.identifiers,
            vec![RestTableIdentifier {
                namespace: vec!["analytics".to_string()],
                name: "active".to_string(),
            }]
        );
    }

    #[tokio::test]
    async fn table_helpers_call_catalog_store() {
        let store = TestTableCatalogStore::default();
        let metadata_backend = TestTableCatalogObjectBackend::default();
        let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
        ensure_table_bucket_entry(&store, "warehouse", true)
            .await
            .expect("table bucket entry should be seeded");
        create_namespace_response(
            &store,
            "warehouse",
            CreateNamespaceRequest {
                namespace: vec!["analytics".to_string()],
                properties: BTreeMap::new(),
            },
            true,
        )
        .await
        .expect("namespace should be created");

        let metadata_location =
            ".rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00001.metadata.json";
        let metadata = test_table_metadata_json("table-uuid", "s3://warehouse/tables/table-id");
        metadata_backend
            .put_json("warehouse", metadata_location, metadata.clone())
            .await;
        let register = register_table_response(
            &store,
            &metadata_backend,
            "warehouse",
            &namespace,
            RegisterTableRequest {
                name: "events".to_string(),
                metadata_location: metadata_location.to_string(),
                overwrite: false,
            },
            metadata,
            true,
        )
        .await
        .expect("table should register");

        let client_metadata_location = table_metadata_location_for_client("warehouse", metadata_location);
        assert_eq!(register.metadata_location, client_metadata_location);
        assert_eq!(register.metadata["format-version"], 2);

        let unpaginated_uri = "/".parse::<http::Uri>().expect("list URI should parse");
        let list = list_tables_response(&store, "warehouse", &namespace, &unpaginated_uri)
            .await
            .expect("table list should load");
        assert_eq!(list.identifiers[0].name, "events");

        let load = load_table_response(&store, &metadata_backend, "warehouse", &namespace, "events")
            .await
            .expect("table should load");
        assert_eq!(load.metadata_location, client_metadata_location);
        assert_eq!(load.metadata["table-uuid"], "table-uuid");

        let current = store
            .load_table("warehouse", "analytics", "events")
            .await
            .expect("table lookup should succeed")
            .expect("table should exist");
        let next_metadata_location =
            ".rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00002.metadata.json";
        let mut next_metadata = test_table_metadata_json("table-uuid", "s3://warehouse/tables/table-id");
        next_metadata["last-sequence-number"] = serde_json::json!(2);
        metadata_backend
            .put_json("warehouse", next_metadata_location, next_metadata)
            .await;
        let commit = commit_table_response(
            &store,
            &metadata_backend,
            "warehouse",
            &namespace,
            "events",
            RestCommitTableRequest {
                commit_id: Some("commit-1".to_string()),
                idempotency_key: Some("retry-1".to_string()),
                operation: Some("append".to_string()),
                expected_version_token: Some(current.version_token.clone()),
                expected_metadata_location: Some(table_metadata_location_for_client("warehouse", &current.metadata_location)),
                new_metadata_location: Some(table_metadata_location_for_client("warehouse", next_metadata_location)),
                requirements: Vec::new(),
                updates: Vec::new(),
                identifier: None,
                writer: Some("pyiceberg".to_string()),
            },
        )
        .await
        .expect("table commit should succeed");
        assert_eq!(
            commit.metadata_location,
            table_metadata_location_for_client("warehouse", next_metadata_location)
        );
        assert_eq!(commit.version_token, "token-committed");
        assert_eq!(commit.generation, current.generation + 1);
        assert_eq!(commit.commit_id, "commit-1");

        let committed = store
            .load_table("warehouse", "analytics", "events")
            .await
            .expect("committed table lookup should succeed")
            .expect("committed table should exist");
        assert_eq!(committed.metadata_location, next_metadata_location);

        drop_table_in_store(&store, "warehouse", &namespace, "events")
            .await
            .expect("table should drop");
        assert!(
            load_table_response(&store, &metadata_backend, "warehouse", &namespace, "events")
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn register_table_response_adopts_metadata_table_uuid() {
        let store = TestTableCatalogStore::default();
        let metadata_backend = TestTableCatalogObjectBackend::default();
        let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
        ensure_table_bucket_entry(&store, "warehouse", true)
            .await
            .expect("table bucket entry should be seeded");
        create_namespace_response(
            &store,
            "warehouse",
            CreateNamespaceRequest {
                namespace: vec!["analytics".to_string()],
                properties: BTreeMap::new(),
            },
            true,
        )
        .await
        .expect("namespace should be created");
        let metadata_location =
            ".rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00001.metadata.json";
        let metadata = test_table_metadata_json("metadata-table-uuid", "s3://warehouse/tables/table-id");
        metadata_backend
            .put_json("warehouse", metadata_location, metadata.clone())
            .await;

        register_table_response(
            &store,
            &metadata_backend,
            "warehouse",
            &namespace,
            RegisterTableRequest {
                name: "events".to_string(),
                metadata_location: metadata_location.to_string(),
                overwrite: false,
            },
            metadata,
            true,
        )
        .await
        .expect("table should register");

        let entry = store
            .load_table("warehouse", "analytics", "events")
            .await
            .expect("table lookup should succeed")
            .expect("table should exist");
        assert_eq!(entry.table_uuid, "metadata-table-uuid");
    }

    #[tokio::test]
    async fn register_table_response_accepts_metadata_in_existing_warehouse() {
        let metadata_backend = TestTableCatalogObjectBackend::default();
        let store = crate::table_catalog::ObjectTableCatalogStore::new(metadata_backend.clone());
        let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
        ensure_table_bucket_entry(&store, "warehouse", true)
            .await
            .expect("table bucket entry should be seeded");
        create_namespace_response(
            &store,
            "warehouse",
            CreateNamespaceRequest {
                namespace: vec!["analytics".to_string()],
                properties: BTreeMap::new(),
            },
            true,
        )
        .await
        .expect("namespace should be created");
        let metadata_location = "catalog-meta/events/00001.metadata.json";
        let metadata = test_table_metadata_json("existing-table-uuid", "s3://warehouse/tables/existing-table");
        metadata_backend
            .put_json("warehouse", metadata_location, metadata.clone())
            .await;

        let response = register_table_response(
            &store,
            &metadata_backend,
            "warehouse",
            &namespace,
            RegisterTableRequest {
                name: "events".to_string(),
                metadata_location: format!("s3://warehouse/{metadata_location}"),
                overwrite: false,
            },
            metadata,
            true,
        )
        .await
        .expect("existing table metadata should register");

        assert_eq!(response.metadata_location, format!("s3://warehouse/{metadata_location}"));
        let entry = store
            .load_table("warehouse", "analytics", "events")
            .await
            .expect("table lookup should succeed")
            .expect("table should exist");
        assert_eq!(entry.metadata_location, metadata_location);
        assert_eq!(entry.warehouse_location, "s3://warehouse/tables/existing-table");
    }

    #[tokio::test]
    async fn standard_commit_rejects_warehouse_relocation() {
        let metadata_backend = TestTableCatalogObjectBackend::default();
        let store = crate::table_catalog::ObjectTableCatalogStore::new(metadata_backend.clone());
        let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
        ensure_table_bucket_entry(&store, "warehouse", true)
            .await
            .expect("table bucket entry should be seeded");
        create_namespace_response(
            &store,
            "warehouse",
            CreateNamespaceRequest {
                namespace: vec!["analytics".to_string()],
                properties: BTreeMap::new(),
            },
            true,
        )
        .await
        .expect("namespace should be created");
        let metadata_location = "catalog-meta/events/00001.metadata.json";
        let metadata = test_table_metadata_json("existing-table-uuid", "s3://warehouse/tables/existing-table");
        metadata_backend
            .put_json("warehouse", metadata_location, metadata.clone())
            .await;
        register_table_response(
            &store,
            &metadata_backend,
            "warehouse",
            &namespace,
            RegisterTableRequest {
                name: "events".to_string(),
                metadata_location: metadata_location.to_string(),
                overwrite: false,
            },
            metadata,
            true,
        )
        .await
        .expect("existing table metadata should register");
        let object_count = metadata_backend.objects.lock().await.len();
        let request = serde_json::from_value(serde_json::json!({
            "requirements": [],
            "updates": [{
                "action": "set-location",
                "location": "s3://warehouse/tables/relocated-table"
            }]
        }))
        .expect("set-location request should parse");

        let error = standard_commit_table_response(&store, &metadata_backend, "warehouse", &namespace, "events", request)
            .await
            .expect_err("warehouse relocation must be rejected");
        assert_eq!(error.code(), &S3ErrorCode::Custom(ICEBERG_ERROR_UNSUPPORTED_OPERATION.into()));
        assert_eq!(error.status_code(), Some(StatusCode::NOT_ACCEPTABLE));
        assert_eq!(metadata_backend.objects.lock().await.len(), object_count);
        let unchanged = store
            .load_table("warehouse", "analytics", "events")
            .await
            .expect("table lookup should succeed")
            .expect("table should exist");
        assert_eq!(unchanged.warehouse_location, "s3://warehouse/tables/existing-table");
    }

    #[tokio::test]
    async fn register_table_response_rejects_metadata_without_format_version() {
        let store = TestTableCatalogStore::default();
        let metadata_backend = TestTableCatalogObjectBackend::default();
        let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
        ensure_table_bucket_entry(&store, "warehouse", true)
            .await
            .expect("table bucket entry should be seeded");
        create_namespace_response(
            &store,
            "warehouse",
            CreateNamespaceRequest {
                namespace: vec!["analytics".to_string()],
                properties: BTreeMap::new(),
            },
            true,
        )
        .await
        .expect("namespace should be created");
        let metadata_location =
            ".rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00001.metadata.json";
        let metadata = serde_json::json!({
            "table-uuid": "metadata-table-uuid",
            "location": "s3://warehouse/tables/table-id"
        });
        metadata_backend
            .put_json("warehouse", metadata_location, metadata.clone())
            .await;

        assert!(
            register_table_response(
                &store,
                &metadata_backend,
                "warehouse",
                &namespace,
                RegisterTableRequest {
                    name: "events".to_string(),
                    metadata_location: metadata_location.to_string(),
                    overwrite: false,
                },
                metadata,
                true,
            )
            .await
            .is_err()
        );
        assert!(
            store
                .load_table("warehouse", "analytics", "events")
                .await
                .expect("table lookup should succeed")
                .is_none()
        );
    }

    #[tokio::test]
    async fn register_table_response_rejects_dangling_metadata_references() {
        let store = TestTableCatalogStore::default();
        let metadata_backend = TestTableCatalogObjectBackend::default();
        let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
        ensure_table_bucket_entry(&store, "warehouse", true)
            .await
            .expect("table bucket entry should be seeded");
        create_namespace_response(
            &store,
            "warehouse",
            CreateNamespaceRequest {
                namespace: vec!["analytics".to_string()],
                properties: BTreeMap::new(),
            },
            true,
        )
        .await
        .expect("namespace should be created");
        let metadata_location =
            ".rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00001.metadata.json";
        let mut metadata = test_table_metadata_json("metadata-table-uuid", "s3://warehouse/tables/table-id");
        metadata["current-schema-id"] = serde_json::json!(99);
        metadata_backend
            .put_json("warehouse", metadata_location, metadata.clone())
            .await;

        let error = register_table_response(
            &store,
            &metadata_backend,
            "warehouse",
            &namespace,
            RegisterTableRequest {
                name: "events".to_string(),
                metadata_location: metadata_location.to_string(),
                overwrite: false,
            },
            metadata,
            true,
        )
        .await
        .expect_err("registration must reject dangling metadata references");

        assert_eq!(error.code(), &S3ErrorCode::InvalidRequest);
        assert!(
            store
                .load_table("warehouse", "analytics", "events")
                .await
                .expect("table lookup should succeed")
                .is_none()
        );
    }

    #[tokio::test]
    async fn metadata_location_api_loads_and_updates_current_pointer() {
        let store = TestTableCatalogStore::default();
        let metadata_backend = TestTableCatalogObjectBackend::default();
        let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
        ensure_table_bucket_entry(&store, "warehouse", true)
            .await
            .expect("table bucket entry should be seeded");
        create_namespace_response(
            &store,
            "warehouse",
            CreateNamespaceRequest {
                namespace: vec!["analytics".to_string()],
                properties: BTreeMap::new(),
            },
            true,
        )
        .await
        .expect("namespace should be created");
        let current_location = ".rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00001.metadata.json";
        let entry = table_entry_from_register_request(
            "warehouse",
            &namespace,
            RegisterTableRequest {
                name: "events".to_string(),
                metadata_location: current_location.to_string(),
                overwrite: false,
            },
        )
        .expect("table entry should build");
        let table_uuid = entry.table_uuid.clone();
        store.register_table(entry).await.expect("table should register");
        metadata_backend
            .put_json(
                "warehouse",
                current_location,
                test_table_metadata_json(&table_uuid, "s3://warehouse/tables/table-id"),
            )
            .await;
        let current = get_table_metadata_location_response(&store, "warehouse", &namespace, "events")
            .await
            .expect("metadata location should load");
        assert_eq!(
            current.metadata_location,
            table_metadata_location_for_client("warehouse", current_location)
        );
        let next_location = ".rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00002.metadata.json";
        metadata_backend
            .put_json(
                "warehouse",
                next_location,
                test_table_metadata_json(&table_uuid, "s3://warehouse/tables/table-id"),
            )
            .await;

        let updated = update_table_metadata_location_response(
            &store,
            &metadata_backend,
            "warehouse",
            &namespace,
            "events",
            UpdateTableMetadataLocationRequest {
                metadata_location: table_metadata_location_for_client("warehouse", next_location),
                version_token: current.version_token.clone(),
                commit_id: None,
                idempotency_key: Some("retry-1".to_string()),
            },
            read_table_metadata_json(&metadata_backend, "warehouse", next_location)
                .await
                .expect("target metadata should load"),
        )
        .await
        .expect("metadata location should update");

        assert_eq!(updated.metadata_location, table_metadata_location_for_client("warehouse", next_location));
        assert_eq!(updated.generation, current.generation + 1);
        assert_ne!(updated.version_token, current.version_token);

        let replayed = update_table_metadata_location_response(
            &store,
            &metadata_backend,
            "warehouse",
            &namespace,
            "events",
            UpdateTableMetadataLocationRequest {
                metadata_location: table_metadata_location_for_client("warehouse", next_location),
                version_token: current.version_token,
                commit_id: None,
                idempotency_key: Some("retry-1".to_string()),
            },
            read_table_metadata_json(&metadata_backend, "warehouse", next_location)
                .await
                .expect("target metadata should load"),
        )
        .await
        .expect("idempotency-only retry should replay the published result");

        assert_eq!(replayed.metadata_location, updated.metadata_location);
        assert_eq!(replayed.version_token, updated.version_token);
        assert_eq!(replayed.generation, updated.generation);
    }

    #[tokio::test]
    async fn metadata_location_api_revalidates_snapshot_objects_before_commit_and_replay() {
        let store = TestTableCatalogStore::default();
        let metadata_backend = TestTableCatalogObjectBackend::default();
        let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
        let created = create_standard_events_table(&store, &metadata_backend, &namespace).await;
        let current = store
            .load_table("warehouse", "analytics", "events")
            .await
            .expect("table lookup should succeed")
            .expect("table should exist");
        let target_location = crate::table_catalog::table_metadata_file_path_for_entry(&current, "00002.metadata.json")
            .expect("target metadata path should build");
        let manifest_list = format!("{}/metadata/snap-10.avro", current.warehouse_location);
        let data_file = format!("{}/data/part-10.parquet", current.warehouse_location);
        let mut target_metadata = created.metadata;
        target_metadata["last-sequence-number"] = serde_json::Value::from(1);
        target_metadata["snapshots"] = serde_json::json!([{
            "snapshot-id": 10,
            "sequence-number": 1,
            "timestamp-ms": 10,
            "manifest-list": manifest_list,
            "summary": {"operation": "append"}
        }]);
        target_metadata["current-snapshot-id"] = serde_json::Value::from(10);
        target_metadata["refs"] = serde_json::json!({"main": {"type": "branch", "snapshot-id": 10}});
        metadata_backend
            .put_json("warehouse", &target_location, target_metadata.clone())
            .await;
        let request = || UpdateTableMetadataLocationRequest {
            metadata_location: table_metadata_location_for_client("warehouse", &target_location),
            version_token: current.version_token.clone(),
            commit_id: None,
            idempotency_key: Some("metadata-location-retry".to_string()),
        };

        let error = update_table_metadata_location_response(
            &store,
            &metadata_backend,
            "warehouse",
            &namespace,
            "events",
            request(),
            target_metadata.clone(),
        )
        .await
        .expect_err("missing manifest-list must fail before pointer publication");
        assert_eq!(error.code(), &S3ErrorCode::InvalidRequest);
        let unchanged = store
            .load_table("warehouse", "analytics", "events")
            .await
            .expect("table lookup should succeed")
            .expect("table should remain present");
        assert_eq!(unchanged.metadata_location, current.metadata_location);

        seed_test_snapshot_manifest(
            &metadata_backend,
            "warehouse",
            target_metadata["snapshots"][0]["manifest-list"]
                .as_str()
                .expect("manifest-list should be a string"),
            10,
            1,
            &[(&data_file, 0, 1, 10, 1)],
        )
        .await;
        update_table_metadata_location_response(
            &store,
            &metadata_backend,
            "warehouse",
            &namespace,
            "events",
            request(),
            target_metadata.clone(),
        )
        .await
        .expect("complete snapshot graph should commit");

        metadata_backend
            .delete_object("warehouse", &test_snapshot_object_key("warehouse", &data_file))
            .await
            .expect("referenced data file should be removed");
        let error = update_table_metadata_location_response(
            &store,
            &metadata_backend,
            "warehouse",
            &namespace,
            "events",
            request(),
            target_metadata,
        )
        .await
        .expect_err("idempotent replay must revalidate referenced objects");
        assert_eq!(error.code(), &S3ErrorCode::InvalidRequest);
        assert_eq!(error.message(), Some("manifest referenced data file is missing"));
    }

    #[tokio::test]
    async fn metadata_pointer_mutations_reject_outside_target_before_reading_it() {
        let store = TestTableCatalogStore::default();
        let metadata_backend = TestTableCatalogObjectBackend::default();
        let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
        create_standard_events_table(&store, &metadata_backend, &namespace).await;
        let outside_location = "unrelated/metadata/00002.metadata.json";
        let read_object_calls_before = metadata_backend.read_object_call_count();

        let error =
            read_existing_table_metadata_target(&store, &metadata_backend, "warehouse", &namespace, "events", outside_location)
                .await
                .expect_err("an outside metadata target should be rejected before object lookup");

        assert_eq!(error.code(), &S3ErrorCode::InvalidRequest);
        assert_eq!(error.message(), Some("metadata location must be inside the table metadata directory"));
        assert_eq!(metadata_backend.read_object_call_count(), read_object_calls_before);
    }

    #[tokio::test]
    async fn metadata_location_api_rejects_invalid_target_metadata_before_commit() {
        let store = TestTableCatalogStore::default();
        let metadata_backend = TestTableCatalogObjectBackend::default();
        let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
        ensure_table_bucket_entry(&store, "warehouse", true)
            .await
            .expect("table bucket entry should be seeded");
        create_namespace_response(
            &store,
            "warehouse",
            CreateNamespaceRequest {
                namespace: vec!["analytics".to_string()],
                properties: BTreeMap::new(),
            },
            true,
        )
        .await
        .expect("namespace should be created");
        let current_location = ".rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00001.metadata.json";
        store
            .register_table(
                table_entry_from_register_request(
                    "warehouse",
                    &namespace,
                    RegisterTableRequest {
                        name: "events".to_string(),
                        metadata_location: current_location.to_string(),
                        overwrite: false,
                    },
                )
                .expect("table entry should build"),
            )
            .await
            .expect("table should register");
        let current = get_table_metadata_location_response(&store, "warehouse", &namespace, "events")
            .await
            .expect("metadata location should load");
        let invalid_location = ".rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00002.metadata.json";
        metadata_backend
            .put_json(
                "warehouse",
                invalid_location,
                test_table_metadata_json("table-uuid", "s3://other-warehouse/tables/table-id"),
            )
            .await;

        assert!(
            update_table_metadata_location_response(
                &store,
                &metadata_backend,
                "warehouse",
                &namespace,
                "events",
                UpdateTableMetadataLocationRequest {
                    metadata_location: invalid_location.to_string(),
                    version_token: current.version_token,
                    commit_id: Some("commit-1".to_string()),
                    idempotency_key: None,
                },
                read_table_metadata_json(&metadata_backend, "warehouse", invalid_location)
                    .await
                    .expect("target metadata should load"),
            )
            .await
            .is_err()
        );
        let unchanged = get_table_metadata_location_response(&store, "warehouse", &namespace, "events")
            .await
            .expect("metadata location should still load");
        assert_eq!(
            unchanged.metadata_location,
            table_metadata_location_for_client("warehouse", current_location)
        );
        assert_eq!(unchanged.generation, current.generation);
    }

    #[tokio::test]
    async fn metadata_location_api_rejects_mismatched_table_uuid_before_commit() {
        let store = TestTableCatalogStore::default();
        let metadata_backend = TestTableCatalogObjectBackend::default();
        let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
        ensure_table_bucket_entry(&store, "warehouse", true)
            .await
            .expect("table bucket entry should be seeded");
        create_namespace_response(
            &store,
            "warehouse",
            CreateNamespaceRequest {
                namespace: vec!["analytics".to_string()],
                properties: BTreeMap::new(),
            },
            true,
        )
        .await
        .expect("namespace should be created");
        let current_location = ".rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00001.metadata.json";
        let current_metadata = test_table_metadata_json("table-uuid", "s3://warehouse/tables/table-id");
        metadata_backend
            .put_json("warehouse", current_location, current_metadata.clone())
            .await;
        register_table_response(
            &store,
            &metadata_backend,
            "warehouse",
            &namespace,
            RegisterTableRequest {
                name: "events".to_string(),
                metadata_location: current_location.to_string(),
                overwrite: false,
            },
            current_metadata,
            true,
        )
        .await
        .expect("table should register");
        let current = get_table_metadata_location_response(&store, "warehouse", &namespace, "events")
            .await
            .expect("metadata location should load");
        let mismatched_location =
            ".rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00002.metadata.json";
        metadata_backend
            .put_json(
                "warehouse",
                mismatched_location,
                test_table_metadata_json("other-table-uuid", "s3://warehouse/tables/table-id"),
            )
            .await;

        assert!(
            update_table_metadata_location_response(
                &store,
                &metadata_backend,
                "warehouse",
                &namespace,
                "events",
                UpdateTableMetadataLocationRequest {
                    metadata_location: mismatched_location.to_string(),
                    version_token: current.version_token,
                    commit_id: Some("commit-1".to_string()),
                    idempotency_key: None,
                },
                read_table_metadata_json(&metadata_backend, "warehouse", mismatched_location)
                    .await
                    .expect("target metadata should load"),
            )
            .await
            .is_err()
        );
        let unchanged = get_table_metadata_location_response(&store, "warehouse", &namespace, "events")
            .await
            .expect("metadata location should still load");
        assert_eq!(
            unchanged.metadata_location,
            table_metadata_location_for_client("warehouse", current_location)
        );
        assert_eq!(unchanged.generation, current.generation);
    }

    #[tokio::test]
    async fn catalog_import_and_rollback_use_register_and_commit_paths() {
        let backend = TestTableCatalogObjectBackend::default();
        let store = crate::table_catalog::ObjectTableCatalogStore::new(backend.clone());
        let bucket = "warehouse";
        let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
        let table = crate::table_catalog::IdentifierSegment::parse("events").expect("table should parse");
        ensure_table_bucket_entry(&store, bucket, true)
            .await
            .expect("table bucket entry should be seeded");
        create_namespace_response(
            &store,
            bucket,
            CreateNamespaceRequest {
                namespace: vec!["analytics".to_string()],
                properties: BTreeMap::new(),
            },
            true,
        )
        .await
        .expect("namespace should be created");
        let imported_location = crate::table_catalog::default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
        backend
            .put_json(
                bucket,
                &imported_location,
                test_table_metadata_json("table-uuid", "s3://warehouse/tables/table-id"),
            )
            .await;

        let imported = catalog_import_response(
            &store,
            &backend,
            bucket,
            &namespace,
            "events",
            CatalogImportRequest {
                metadata_location: table_metadata_location_for_client(bucket, &imported_location),
                properties: BTreeMap::from([("owner".to_string(), "lakehouse".to_string())]),
            },
            true,
        )
        .await
        .expect("catalog import should register table");
        assert_eq!(imported.metadata_location, table_metadata_location_for_client(bucket, &imported_location));
        let current = store
            .load_table(bucket, "analytics", "events")
            .await
            .expect("table lookup should succeed")
            .expect("table should exist");
        assert_eq!(current.properties.get("owner").map(String::as_str), Some("lakehouse"));

        let imported_again = catalog_import_response(
            &store,
            &backend,
            bucket,
            &namespace,
            "events",
            CatalogImportRequest {
                metadata_location: imported_location.clone(),
                properties: BTreeMap::from([("owner".to_string(), "lakehouse".to_string())]),
            },
            true,
        )
        .await
        .expect("repeated catalog import should be idempotent");
        assert_eq!(
            imported_again.metadata_location,
            table_metadata_location_for_client(bucket, &imported_location)
        );

        let rollback_location = crate::table_catalog::default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
        let mut rollback_metadata = test_table_metadata_json("table-uuid", "s3://warehouse/tables/table-id");
        rollback_metadata["last-sequence-number"] = serde_json::Value::from(2);
        backend.put_json(bucket, &rollback_location, rollback_metadata).await;
        let rollback = rollback_table_response(
            &store,
            &backend,
            bucket,
            &namespace,
            "events",
            RollbackTableRequest {
                metadata_location: table_metadata_location_for_client(bucket, &rollback_location),
                version_token: current.version_token.clone(),
                commit_id: None,
                idempotency_key: Some("rollback-request-1".to_string()),
            },
            read_table_metadata_json(&backend, bucket, &rollback_location)
                .await
                .expect("rollback metadata should load"),
        )
        .await
        .expect("rollback should commit selected metadata");

        assert_eq!(rollback.metadata_location, table_metadata_location_for_client(bucket, &rollback_location));
        assert!(rollback.commit_id.starts_with("idempotency-"));

        let replayed = rollback_table_response(
            &store,
            &backend,
            bucket,
            &namespace,
            "events",
            RollbackTableRequest {
                metadata_location: table_metadata_location_for_client(bucket, &rollback_location),
                version_token: current.version_token.clone(),
                commit_id: None,
                idempotency_key: Some("rollback-request-1".to_string()),
            },
            read_table_metadata_json(&backend, bucket, &rollback_location)
                .await
                .expect("rollback metadata should load"),
        )
        .await
        .expect("idempotency-only rollback retry should replay the published result");

        assert_eq!(replayed.commit_id, rollback.commit_id);
        assert_eq!(replayed.version_token, rollback.version_token);
        assert_eq!(replayed.generation, rollback.generation);

        let later_location = crate::table_catalog::default_table_metadata_file_path(&namespace, &table, "00003.metadata.json");
        let mut later_metadata = test_table_metadata_json("table-uuid", "s3://warehouse/tables/table-id");
        later_metadata["last-sequence-number"] = serde_json::Value::from(3);
        backend.put_json(bucket, &later_location, later_metadata).await;
        let later = rollback_table_response(
            &store,
            &backend,
            bucket,
            &namespace,
            "events",
            RollbackTableRequest {
                metadata_location: table_metadata_location_for_client(bucket, &later_location),
                version_token: rollback.version_token,
                commit_id: None,
                idempotency_key: Some("rollback-request-2".to_string()),
            },
            read_table_metadata_json(&backend, bucket, &later_location)
                .await
                .expect("later rollback metadata should load"),
        )
        .await
        .expect("a later rollback should commit");

        let historical = rollback_table_response(
            &store,
            &backend,
            bucket,
            &namespace,
            "events",
            RollbackTableRequest {
                metadata_location: table_metadata_location_for_client(bucket, &rollback_location),
                version_token: current.version_token,
                commit_id: None,
                idempotency_key: Some("rollback-request-1".to_string()),
            },
            read_table_metadata_json(&backend, bucket, &rollback_location)
                .await
                .expect("rollback metadata should load"),
        )
        .await
        .expect("a historical rollback retry should return the current table state");

        assert_eq!(historical.metadata_location, later.metadata_location);
        assert_eq!(historical.version_token, later.version_token);
        assert_eq!(historical.generation, later.generation);
    }

    #[tokio::test]
    async fn rollback_rejects_invalid_target_metadata_before_commit() {
        let backend = TestTableCatalogObjectBackend::default();
        let store = crate::table_catalog::ObjectTableCatalogStore::new(backend.clone());
        let bucket = "warehouse";
        let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
        let table = crate::table_catalog::IdentifierSegment::parse("events").expect("table should parse");
        ensure_table_bucket_entry(&store, bucket, true)
            .await
            .expect("table bucket entry should be seeded");
        create_namespace_response(
            &store,
            bucket,
            CreateNamespaceRequest {
                namespace: vec!["analytics".to_string()],
                properties: BTreeMap::new(),
            },
            true,
        )
        .await
        .expect("namespace should be created");
        let current_location = crate::table_catalog::default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
        backend
            .put_json(
                bucket,
                &current_location,
                test_table_metadata_json("table-uuid", "s3://warehouse/tables/table-id"),
            )
            .await;
        catalog_import_response(
            &store,
            &backend,
            bucket,
            &namespace,
            "events",
            CatalogImportRequest {
                metadata_location: current_location.clone(),
                properties: BTreeMap::new(),
            },
            true,
        )
        .await
        .expect("catalog import should register table");
        let current = store
            .load_table(bucket, "analytics", "events")
            .await
            .expect("table lookup should succeed")
            .expect("table should exist");

        let invalid_location = crate::table_catalog::default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
        backend
            .put_json(
                bucket,
                &invalid_location,
                test_table_metadata_json("table-uuid", "s3://other-warehouse/tables/table-id"),
            )
            .await;

        assert!(
            rollback_table_response(
                &store,
                &backend,
                bucket,
                &namespace,
                "events",
                RollbackTableRequest {
                    metadata_location: invalid_location.clone(),
                    version_token: current.version_token,
                    commit_id: Some("rollback-1".to_string()),
                    idempotency_key: None,
                },
                read_table_metadata_json(&backend, bucket, &invalid_location)
                    .await
                    .expect("rollback metadata should load"),
            )
            .await
            .is_err()
        );
        let unchanged = store
            .load_table(bucket, "analytics", "events")
            .await
            .expect("table lookup should succeed")
            .expect("table should still exist");

        assert_eq!(unchanged.metadata_location, current_location);
        assert_eq!(unchanged.generation, current.generation);
    }

    #[tokio::test]
    async fn rollback_rejects_mismatched_table_uuid_before_commit() {
        let backend = TestTableCatalogObjectBackend::default();
        let store = crate::table_catalog::ObjectTableCatalogStore::new(backend.clone());
        let bucket = "warehouse";
        let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
        let table = crate::table_catalog::IdentifierSegment::parse("events").expect("table should parse");
        ensure_table_bucket_entry(&store, bucket, true)
            .await
            .expect("table bucket entry should be seeded");
        create_namespace_response(
            &store,
            bucket,
            CreateNamespaceRequest {
                namespace: vec!["analytics".to_string()],
                properties: BTreeMap::new(),
            },
            true,
        )
        .await
        .expect("namespace should be created");
        let current_location = crate::table_catalog::default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
        backend
            .put_json(
                bucket,
                &current_location,
                test_table_metadata_json("table-uuid", "s3://warehouse/tables/table-id"),
            )
            .await;
        catalog_import_response(
            &store,
            &backend,
            bucket,
            &namespace,
            "events",
            CatalogImportRequest {
                metadata_location: current_location.clone(),
                properties: BTreeMap::new(),
            },
            true,
        )
        .await
        .expect("catalog import should register table");
        let current = store
            .load_table(bucket, "analytics", "events")
            .await
            .expect("table lookup should succeed")
            .expect("table should exist");

        let mismatched_location =
            crate::table_catalog::default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
        backend
            .put_json(
                bucket,
                &mismatched_location,
                test_table_metadata_json("other-table-uuid", "s3://warehouse/tables/table-id"),
            )
            .await;

        assert!(
            rollback_table_response(
                &store,
                &backend,
                bucket,
                &namespace,
                "events",
                RollbackTableRequest {
                    metadata_location: mismatched_location.clone(),
                    version_token: current.version_token,
                    commit_id: Some("rollback-1".to_string()),
                    idempotency_key: None,
                },
                read_table_metadata_json(&backend, bucket, &mismatched_location)
                    .await
                    .expect("rollback metadata should load"),
            )
            .await
            .is_err()
        );
        let unchanged = store
            .load_table(bucket, "analytics", "events")
            .await
            .expect("table lookup should succeed")
            .expect("table should still exist");
        assert_eq!(unchanged.metadata_location, current_location);
        assert_eq!(unchanged.generation, current.generation);
    }

    #[tokio::test]
    async fn legacy_commit_rejects_mismatched_table_uuid_before_commit() {
        let store = TestTableCatalogStore::default();
        let metadata_backend = TestTableCatalogObjectBackend::default();
        let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
        ensure_table_bucket_entry(&store, "warehouse", true)
            .await
            .expect("table bucket entry should be seeded");
        create_namespace_response(
            &store,
            "warehouse",
            CreateNamespaceRequest {
                namespace: vec!["analytics".to_string()],
                properties: BTreeMap::new(),
            },
            true,
        )
        .await
        .expect("namespace should be created");
        let current_location = ".rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00001.metadata.json";
        let current_metadata = test_table_metadata_json("table-uuid", "s3://warehouse/tables/table-id");
        metadata_backend
            .put_json("warehouse", current_location, current_metadata.clone())
            .await;
        register_table_response(
            &store,
            &metadata_backend,
            "warehouse",
            &namespace,
            RegisterTableRequest {
                name: "events".to_string(),
                metadata_location: current_location.to_string(),
                overwrite: false,
            },
            current_metadata,
            true,
        )
        .await
        .expect("table should register");
        let current = store
            .load_table("warehouse", "analytics", "events")
            .await
            .expect("table lookup should succeed")
            .expect("table should exist");
        let mismatched_location =
            ".rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00002.metadata.json";
        metadata_backend
            .put_json(
                "warehouse",
                mismatched_location,
                test_table_metadata_json("other-table-uuid", "s3://warehouse/tables/table-id"),
            )
            .await;

        assert!(
            commit_table_response(
                &store,
                &metadata_backend,
                "warehouse",
                &namespace,
                "events",
                RestCommitTableRequest {
                    commit_id: Some("commit-1".to_string()),
                    idempotency_key: None,
                    operation: Some("append".to_string()),
                    expected_version_token: Some(current.version_token.clone()),
                    expected_metadata_location: Some(current.metadata_location.clone()),
                    new_metadata_location: Some(mismatched_location.to_string()),
                    requirements: Vec::new(),
                    updates: Vec::new(),
                    identifier: None,
                    writer: Some("pyiceberg".to_string()),
                },
            )
            .await
            .is_err()
        );
        let unchanged = store
            .load_table("warehouse", "analytics", "events")
            .await
            .expect("table lookup should succeed")
            .expect("table should still exist");
        assert_eq!(unchanged.metadata_location, current_location);
        assert_eq!(unchanged.generation, current.generation);
    }
}
