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

use crate::admin::runtime_sources;
use crate::admin::runtime_sources::default_admin_usecase;
use crate::admin::storage_api::access::{ReqInfo, authorize_internal_object_request};
use crate::admin::storage_api::bucket::metadata::table_catalog_path_hash;
use crate::admin::storage_api::runtime::ECStore;
use crate::admin::{
    auth::{AdminResourceScope, validate_admin_action_with_bucket_object_for_iam},
    router::{AdminOperation, Operation, S3Router},
};
use crate::auth::{check_key_valid_with_context, get_session_token};
use crate::error::ApiError;
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
use rustfs_iam::sys::SESSION_POLICY_NAME;
use rustfs_policy::{
    auth::get_new_credentials_with_metadata,
    policy::{
        Policy,
        action::{Action, AdminAction, S3Action},
    },
};
use rustfs_utils::crypto::{base64_decode_url_safe_no_pad, base64_encode_url_safe_no_pad, hex_sha256};
use s3s::{Body, S3Error, S3ErrorCode, S3Request, S3Response, S3Result, header::CONTENT_TYPE, s3_error};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration as StdDuration, Instant};
use time::{Duration, OffsetDateTime};
use uuid::Uuid;

mod config;
mod credentials;
mod maintenance;
mod namespace;
mod refs;
mod routes;
mod table;
mod view;

pub use config::*;
pub use credentials::*;
pub use maintenance::*;
pub use namespace::*;
pub use refs::*;
pub use routes::register_table_catalog_route;
pub use table::*;
pub use view::*;

const JSON_CONTENT_TYPE: &str = "application/json";
const ENV_TABLE_CATALOG_CREDENTIAL_VENDING: &str = "RUSTFS_TABLE_CATALOG_CREDENTIAL_VENDING";
const ENV_TABLE_CATALOG_CREDENTIAL_TTL_SECONDS: &str = "RUSTFS_TABLE_CATALOG_CREDENTIAL_TTL_SECONDS";
const DEFAULT_TABLE_CATALOG_CREDENTIAL_TTL_SECONDS: i64 = 15 * 60;
const MIN_TABLE_CATALOG_CREDENTIAL_TTL_SECONDS: i64 = 60;
const MAX_TABLE_CATALOG_CREDENTIAL_TTL_SECONDS: i64 = 60 * 60;
const TABLE_CATALOG_REQUEST_BODY_TIMEOUT: StdDuration = StdDuration::from_secs(30);
const TABLE_CATALOG_COMMIT_REQUIREMENT_MAX_COUNT: usize = 1_024;
const TABLE_CATALOG_COMMIT_UPDATE_MAX_COUNT: usize = 1_024;
const NAMESPACE_REQUEST_BODY_MAX_SIZE: usize = MAX_ADMIN_REQUEST_BODY_SIZE;
const NAMESPACE_REQUEST_BODY_TIMEOUT: StdDuration = StdDuration::from_secs(10);
const RENAME_TABLE_BODY_MAX_SIZE: usize = 16 * 1024;
const RENAME_TABLE_BODY_TIMEOUT: StdDuration = StdDuration::from_secs(10);
const WAREHOUSE_PROPERTY: &str = "warehouse";
const PREFIX_PROPERTY: &str = "prefix";

fn table_catalog_internal_error(message: &'static str) -> S3Error {
    S3Error::with_message(S3ErrorCode::InternalError, message)
}
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
const REST_PAGE_TOKEN_VERSION: u8 = 1;
const REST_PAGE_TOKEN_MAX_LENGTH: usize = 16 * 1024;
const REST_DEFAULT_PAGE_SIZE: usize = 1000;
const REST_MAX_PAGE_SIZE: usize = 1000;
const REST_PAGE_TOKEN_QUERY_PARAMETER: &str = "pageToken";
const REST_PAGE_SIZE_QUERY_PARAMETER: &str = "pageSize";
const REST_NAMESPACE_SEPARATOR: char = '\u{1f}';
const REST_NAMESPACE_SEPARATOR_URL_ENCODED: &str = "%1F";
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
    #[serde(default, rename = "commit-id")]
    _commit_id: Option<String>,
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

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RenameTableRequest {
    source: RestTableIdentifier,
    destination: RestTableIdentifier,
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RestTableSnapshotSelection {
    All,
    Refs,
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
    iam_store: Arc<rustfs_iam::sys::IamSys<rustfs_iam::store::object::ObjectStore>>,
    token_signing_key: Option<String>,
}

impl IamTableCredentialIssuer {
    fn from_request(req: &S3Request<Body>) -> S3Result<Self> {
        let context = runtime_sources::app_context_from_req(req)
            .ok_or_else(|| table_catalog_internal_error("request application context is not initialized"))?;
        let iam = context.iam();
        if !iam.is_ready() {
            return Err(table_catalog_internal_error("iam not init"));
        }
        let token_signing_key = context.action_credentials().get().map(|credentials| credentials.secret_key);
        Ok(Self {
            enabled: table_credential_vending_enabled(),
            ttl_seconds: table_credential_ttl_seconds(),
            iam_store: iam.handle(),
            token_signing_key,
        })
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

        let secret = self
            .token_signing_key
            .as_deref()
            .ok_or_else(|| table_catalog_internal_error("token signing key not initialized"))?;
        let mut credential = get_new_credentials_with_metadata(&claims, secret)
            .map_err(|err| s3_error!(InternalError, "failed to generate table credentials: {}", err))?;
        bind_table_credential_parent(&mut credential, principal);

        self.iam_store
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
    let mut response = build_json_response(status, body)?;
    response
        .headers
        .insert(http::header::CACHE_CONTROL, HeaderValue::from_static("no-store, private"));
    response
        .headers
        .insert(http::header::PRAGMA, HeaderValue::from_static("no-cache"));
    response.headers.insert(http::header::EXPIRES, HeaderValue::from_static("0"));
    Ok(response)
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
    let principal = table_catalog_request_principal(req).await?;
    validate_admin_action_with_bucket_object_for_iam(
        principal.iam_store,
        &req.headers,
        &principal.credentials,
        principal.owner,
        Action::AdminAction(action),
        req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0)),
        AdminResourceScope::bucket(""),
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

async fn authorize_table_catalog_resource_for_principal(
    req: &S3Request<Body>,
    principal: &TableCatalogRequestPrincipal,
    resource: &TableCatalogResource<'_>,
    action: AdminAction,
) -> S3Result<()> {
    let object_path = resource.object_path();
    validate_admin_action_with_bucket_object_for_iam(
        principal.iam_store.clone(),
        &req.headers,
        &principal.credentials,
        principal.owner,
        Action::AdminAction(action),
        req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0)),
        AdminResourceScope::bucket_object(resource.warehouse, object_path.as_deref().unwrap_or("")),
    )
    .await
}

struct TableCatalogRequestPrincipal {
    credentials: rustfs_credentials::Credentials,
    owner: bool,
    iam_store: Arc<rustfs_iam::sys::IamSys<rustfs_iam::store::object::ObjectStore>>,
}

fn install_table_catalog_s3_request_info(req: &mut S3Request<Body>, principal: &TableCatalogRequestPrincipal) -> S3Result<()> {
    if req.extensions.get::<ReqInfo>().is_none() {
        req.extensions.insert(ReqInfo {
            region: req.region.clone(),
            ..Default::default()
        });
    }
    let req_info = req
        .extensions
        .get_mut::<ReqInfo>()
        .ok_or_else(|| s3_error!(InternalError, "failed to install table catalog authorization context"))?;
    req_info.cred = Some(principal.credentials.clone());
    req_info.is_owner = principal.owner;
    Ok(())
}

async fn authorize_table_catalog_s3_actions(
    req: &mut S3Request<Body>,
    bucket: &str,
    object: &str,
    actions: &[S3Action],
) -> S3Result<()> {
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
        if let Err(err) = authorize_internal_object_request(req, Action::S3Action(*action)).await {
            result = Err(err);
            break;
        }
    }
    if let Some(req_info) = req.extensions.get_mut::<ReqInfo>() {
        (req_info.bucket, req_info.object, req_info.version_id) = original;
    }
    result
}

async fn table_catalog_request_principal(req: &S3Request<Body>) -> S3Result<TableCatalogRequestPrincipal> {
    let Some(input_cred) = &req.credentials else {
        return Err(s3_error!(InvalidRequest, "authentication required"));
    };
    let context = runtime_sources::app_context_from_req(req)
        .ok_or_else(|| table_catalog_internal_error("request application context is not initialized"))?;
    let (credentials, owner) = check_key_valid_with_context(
        get_session_token(&req.uri, &req.headers).unwrap_or_default(),
        &input_cred.access_key,
        Some(context.as_ref()),
    )
    .await?;
    let iam = context.iam();
    if !iam.is_ready() {
        return Err(table_catalog_internal_error("iam not init"));
    }
    Ok(TableCatalogRequestPrincipal {
        credentials,
        owner,
        iam_store: iam.handle(),
    })
}

#[derive(Clone)]
enum TableCommitObjectAuthorization {
    Request(Arc<tokio::sync::Mutex<S3Request<Body>>>),
    Preauthorized,
    #[cfg(test)]
    Test {
        authorized_objects: Arc<tokio::sync::Mutex<Vec<(String, S3Action)>>>,
        denied_object: Option<String>,
    },
}

#[derive(Default, PartialEq, Eq)]
enum TableCommitPublicationPhase {
    #[default]
    Discovering,
    Preparing,
    Prepared,
    Complete,
}

#[derive(Clone, PartialEq, Eq)]
enum TableCommitObjectIdentity {
    Missing,
    Metadata(crate::table_catalog::TableCatalogObjectMetadata),
    ContentSha256(String),
}

#[derive(Clone, PartialEq, Eq)]
struct TableCommitObservedObject {
    identity: TableCommitObjectIdentity,
    max_size: Option<usize>,
}

#[derive(Default)]
struct TableCommitPublicationState {
    phase: TableCommitPublicationPhase,
    bucket_fence: Option<String>,
    table_fence: Option<(String, String, String)>,
    observed_objects: BTreeMap<(String, String), TableCommitObservedObject>,
    guards: Vec<crate::table_catalog::TableCatalogLockGuard>,
}

#[derive(Clone)]
struct TableCommitObjectBackend<B> {
    backend: B,
    authorization: TableCommitObjectAuthorization,
    authorization_error: Arc<tokio::sync::Mutex<Option<S3Error>>>,
    publication: Arc<parking_lot::Mutex<TableCommitPublicationState>>,
    publication_fence_fleet_confirmed: bool,
}

impl<B> TableCommitObjectBackend<B>
where
    B: crate::table_catalog::TableCatalogObjectBackend,
{
    fn new(backend: B, authorization: TableCommitObjectAuthorization, publication_fence_fleet_confirmed: bool) -> Self {
        Self {
            backend,
            authorization,
            authorization_error: Arc::new(tokio::sync::Mutex::new(None)),
            publication: Arc::new(parking_lot::Mutex::new(TableCommitPublicationState::default())),
            publication_fence_fleet_confirmed,
        }
    }

    fn for_request(backend: B, req: S3Request<Body>) -> Self {
        Self::new(
            backend,
            TableCommitObjectAuthorization::Request(Arc::new(tokio::sync::Mutex::new(req))),
            rustfs_utils::get_env_bool(crate::table_catalog::ENV_TABLE_CATALOG_PUBLICATION_FENCE_FLEET_CONFIRMED, false),
        )
    }

    fn preauthorized(backend: B) -> Self {
        Self::new(
            backend,
            TableCommitObjectAuthorization::Preauthorized,
            rustfs_utils::get_env_bool(crate::table_catalog::ENV_TABLE_CATALOG_PUBLICATION_FENCE_FLEET_CONFIRMED, false),
        )
    }

    #[cfg(test)]
    fn trusted(backend: B) -> Self {
        Self::new(backend, TableCommitObjectAuthorization::Preauthorized, true)
    }

    #[cfg(test)]
    fn rolling_upgrade(backend: B) -> Self {
        Self::new(backend, TableCommitObjectAuthorization::Preauthorized, false)
    }

    #[cfg(test)]
    fn test(
        backend: B,
        authorized_objects: Arc<tokio::sync::Mutex<Vec<(String, S3Action)>>>,
        denied_object: Option<String>,
    ) -> Self {
        Self::new(
            backend,
            TableCommitObjectAuthorization::Test {
                authorized_objects,
                denied_object,
            },
            true,
        )
    }

    async fn authorize(&self, bucket: &str, object: &str, action: S3Action) -> crate::table_catalog::TableCatalogStoreResult<()> {
        let result = match &self.authorization {
            TableCommitObjectAuthorization::Request(req) => {
                let mut req = req.lock().await;
                authorize_table_catalog_s3_actions(&mut req, bucket, object, &[action]).await
            }
            TableCommitObjectAuthorization::Preauthorized => Ok(()),
            #[cfg(test)]
            TableCommitObjectAuthorization::Test {
                authorized_objects,
                denied_object,
            } => {
                authorized_objects.lock().await.push((object.to_string(), action));
                if denied_object
                    .as_deref()
                    .is_some_and(|denied| denied == "*" || denied == object)
                {
                    Err(s3_error!(AccessDenied, "test object authorization denied"))
                } else {
                    Ok(())
                }
            }
        };
        if let Err(err) = result {
            let mut authorization_error = self.authorization_error.lock().await;
            if authorization_error.is_none() {
                *authorization_error = Some(err);
            }
            return Err(crate::table_catalog::TableCatalogStoreError::Internal(
                "table commit object authorization failed".to_string(),
            ));
        }
        Ok(())
    }

    fn ensure_observation_allowed(&self, bucket: &str, object: &str) -> crate::table_catalog::TableCatalogStoreResult<()> {
        let publication = self.publication.lock();
        match publication.phase {
            TableCommitPublicationPhase::Discovering | TableCommitPublicationPhase::Complete => Ok(()),
            TableCommitPublicationPhase::Preparing => Err(crate::table_catalog::TableCatalogStoreError::Internal(
                "table commit publication preparation is already in progress".to_string(),
            )),
            TableCommitPublicationPhase::Prepared => Err(crate::table_catalog::TableCatalogStoreError::Internal(format!(
                "table commit accessed an object after publication preparation: {bucket}/{object}"
            ))),
        }
    }

    fn record_observation(
        &self,
        bucket: &str,
        object: &str,
        observation: TableCommitObservedObject,
    ) -> crate::table_catalog::TableCatalogStoreResult<()> {
        let key = (bucket.to_string(), object.to_string());
        let mut publication = self.publication.lock();
        if publication.phase == TableCommitPublicationPhase::Complete {
            return Ok(());
        }
        let Some(existing) = publication.observed_objects.get_mut(&key) else {
            if publication.phase != TableCommitPublicationPhase::Discovering {
                return Err(crate::table_catalog::TableCatalogStoreError::Internal(format!(
                    "table commit accessed an unvalidated object after publication preparation: {bucket}/{object}"
                )));
            }
            publication.observed_objects.insert(key, observation);
            return Ok(());
        };
        if existing.identity != observation.identity {
            return Err(crate::table_catalog::TableCatalogStoreError::Conflict(format!(
                "table commit object changed during validation: {bucket}/{object}"
            )));
        }
        existing.max_size = match (existing.max_size, observation.max_size) {
            (Some(expected), Some(actual)) => Some(expected.min(actual)),
            (Some(expected), None) => Some(expected),
            (None, actual) => actual,
        };
        Ok(())
    }

    fn observed_content(
        object: Option<&crate::table_catalog::TableCatalogObject>,
        max_size: Option<usize>,
    ) -> TableCommitObservedObject {
        let identity = match object {
            None => TableCommitObjectIdentity::Missing,
            Some(object) => TableCommitObjectIdentity::ContentSha256(hex_sha256(&object.data, str::to_string)),
        };
        TableCommitObservedObject { identity, max_size }
    }

    fn observed_metadata(
        bucket: &str,
        object: &str,
        metadata: Option<crate::table_catalog::TableCatalogObjectMetadata>,
    ) -> crate::table_catalog::TableCatalogStoreResult<TableCommitObservedObject> {
        let identity = match metadata {
            None => TableCommitObjectIdentity::Missing,
            Some(metadata) if metadata.etag.is_some() || metadata.mod_time.is_some() => {
                TableCommitObjectIdentity::Metadata(metadata)
            }
            Some(_) => {
                return Err(crate::table_catalog::TableCatalogStoreError::Internal(format!(
                    "catalog object {bucket}/{object} does not expose a stable fingerprint"
                )));
            }
        };
        Ok(TableCommitObservedObject {
            identity,
            max_size: None,
        })
    }

    async fn begin_bucket_publication(&self, table_bucket: &str) -> crate::table_catalog::TableCatalogStoreResult<()> {
        {
            let mut publication = self.publication.lock();
            if publication.phase != TableCommitPublicationPhase::Discovering {
                return Err(crate::table_catalog::TableCatalogStoreError::Internal(
                    "table-bucket commit publication must begin before publication preparation".to_string(),
                ));
            }
            if publication.bucket_fence.as_deref() == Some(table_bucket) {
                return Ok(());
            }
            if publication.bucket_fence.is_some() {
                return Err(crate::table_catalog::TableCatalogStoreError::Internal(
                    "table commit publication cannot span table buckets".to_string(),
                ));
            }
            publication.bucket_fence = Some(table_bucket.to_string());
        }
        let publication_lock = crate::table_catalog::default_table_bucket_publication_lock_path();
        let guard = match self.backend.acquire_write_lock(table_bucket, &publication_lock).await {
            Ok(guard) => guard,
            Err(err) => {
                self.publication.lock().bucket_fence = None;
                return Err(err);
            }
        };
        self.publication.lock().guards.push(guard);
        Ok(())
    }

    async fn prepare_publication(
        &self,
        table_bucket: &str,
        namespace: &str,
        table: &str,
    ) -> crate::table_catalog::TableCatalogStoreResult<()> {
        let namespace = crate::table_catalog::Namespace::parse(namespace)
            .map_err(|err| crate::table_catalog::TableCatalogStoreError::Invalid(format!("invalid namespace: {err}")))?;
        let table = crate::table_catalog::IdentifierSegment::parse(table.to_string())
            .map_err(|err| crate::table_catalog::TableCatalogStoreError::Invalid(format!("invalid table: {err}")))?;
        let table_fence = (table_bucket.to_string(), namespace.public_name(), table.as_str().to_string());
        let publication_lock = crate::table_catalog::default_table_publication_lock_path(&namespace, &table);
        let expected = {
            let mut publication = self.publication.lock();
            match publication.phase {
                TableCommitPublicationPhase::Prepared if publication.table_fence.as_ref() == Some(&table_fence) => {
                    return Ok(());
                }
                TableCommitPublicationPhase::Prepared => {
                    return Err(crate::table_catalog::TableCatalogStoreError::Internal(
                        "table commit publication cannot span tables".to_string(),
                    ));
                }
                TableCommitPublicationPhase::Discovering => {
                    publication.phase = TableCommitPublicationPhase::Preparing;
                    std::mem::take(&mut publication.observed_objects)
                }
                TableCommitPublicationPhase::Preparing => {
                    return Err(crate::table_catalog::TableCatalogStoreError::Internal(
                        "table commit publication preparation is already in progress".to_string(),
                    ));
                }
                TableCommitPublicationPhase::Complete => {
                    return Err(crate::table_catalog::TableCatalogStoreError::Internal(
                        "table commit publication has already completed".to_string(),
                    ));
                }
            }
        };

        let prepared = async {
            // RUSTFS_COMPAT_TODO(table-publication-fence-v1): Retain exact live-file guards for old nodes. Remove after the minimum supported release uses table and bucket publication fences.
            let retain_legacy_object_guards = !self.publication_fence_fleet_confirmed;
            let guarded_object_count = if retain_legacy_object_guards {
                expected.len()
            } else {
                expected
                    .values()
                    .filter(|object| matches!(&object.identity, TableCommitObjectIdentity::ContentSha256(_)))
                    .count()
            };
            let mut guards = Vec::with_capacity(guarded_object_count.saturating_add(1));
            guards.push(self.backend.acquire_write_lock(table_bucket, &publication_lock).await?);
            for ((bucket, object), expected_object) in &expected {
                if retain_legacy_object_guards || matches!(&expected_object.identity, TableCommitObjectIdentity::ContentSha256(_))
                {
                    guards.push(self.backend.acquire_read_lock(bucket, object).await?);
                }
            }
            for ((bucket, object), expected_object) in &expected {
                if !matches!(&expected_object.identity, TableCommitObjectIdentity::ContentSha256(_)) {
                    continue;
                }
                let actual_object = match expected_object.max_size {
                    Some(max_size) => self.backend.read_object_unlocked_limited(bucket, object, max_size).await?,
                    None => self.backend.read_object_unlocked(bucket, object).await?,
                };
                let actual = Self::observed_content(actual_object.as_ref(), expected_object.max_size);
                if actual.identity != expected_object.identity {
                    return Err(crate::table_catalog::TableCatalogStoreError::Conflict(format!(
                        "table commit object changed before catalog publication: {bucket}/{object}"
                    )));
                }
            }
            stream::iter(
                expected
                    .into_iter()
                    .filter_map(|((bucket, object), expected_object)| match expected_object.identity {
                        expected_identity @ (TableCommitObjectIdentity::Metadata(_) | TableCommitObjectIdentity::Missing) => {
                            Some((bucket, object, expected_identity))
                        }
                        TableCommitObjectIdentity::ContentSha256(_) => None,
                    }),
            )
            .map(|(bucket, object, expected_identity)| async move {
                let metadata = if retain_legacy_object_guards {
                    self.backend.object_metadata_unlocked(&bucket, &object).await?
                } else {
                    self.backend.object_metadata(&bucket, &object).await?
                };
                let actual = Self::observed_metadata(&bucket, &object, metadata)?;
                if actual.identity != expected_identity {
                    return Err(crate::table_catalog::TableCatalogStoreError::Conflict(format!(
                        "table commit object changed before catalog publication: {bucket}/{object}"
                    )));
                }
                Ok(())
            })
            .buffer_unordered(crate::table_catalog::TABLE_COMMIT_OBJECT_VALIDATION_CONCURRENCY)
            .try_for_each(|()| async { Ok::<(), crate::table_catalog::TableCatalogStoreError>(()) })
            .await?;
            Ok(guards)
        }
        .await;

        let mut publication = self.publication.lock();
        match prepared {
            Ok(guards) if publication.phase == TableCommitPublicationPhase::Preparing => {
                publication.guards.extend(guards);
                publication.table_fence = Some(table_fence);
                publication.phase = TableCommitPublicationPhase::Prepared;
                Ok(())
            }
            Ok(_) => {
                publication.phase = TableCommitPublicationPhase::Discovering;
                Err(crate::table_catalog::TableCatalogStoreError::Internal(
                    "table commit publication state changed during preparation".to_string(),
                ))
            }
            Err(err) => {
                publication.phase = TableCommitPublicationPhase::Discovering;
                Err(err)
            }
        }
    }

    fn complete_publication(&self) {
        let mut publication = self.publication.lock();
        publication.guards.clear();
        publication.observed_objects.clear();
        publication.bucket_fence = None;
        publication.table_fence = None;
        publication.phase = TableCommitPublicationPhase::Complete;
    }

    async fn finish<T>(&self, result: S3Result<T>) -> S3Result<T> {
        match self.authorization_error.lock().await.take() {
            Some(err) => Err(err),
            None => result,
        }
    }
}

#[async_trait::async_trait]
impl<B> crate::table_catalog::TableCatalogObjectBackend for TableCommitObjectBackend<B>
where
    B: crate::table_catalog::TableCatalogObjectBackend,
{
    async fn read_object(
        &self,
        bucket: &str,
        object: &str,
    ) -> crate::table_catalog::TableCatalogStoreResult<Option<crate::table_catalog::TableCatalogObject>> {
        self.authorize(bucket, object, S3Action::GetObjectAction).await?;
        self.ensure_observation_allowed(bucket, object)?;
        let result = self.backend.read_object(bucket, object).await?;
        self.record_observation(bucket, object, Self::observed_content(result.as_ref(), None))?;
        Ok(result)
    }

    async fn read_object_limited(
        &self,
        bucket: &str,
        object: &str,
        max_size: usize,
    ) -> crate::table_catalog::TableCatalogStoreResult<Option<crate::table_catalog::TableCatalogObject>> {
        self.authorize(bucket, object, S3Action::GetObjectAction).await?;
        self.ensure_observation_allowed(bucket, object)?;
        let result = self.backend.read_object_limited(bucket, object, max_size).await?;
        self.record_observation(bucket, object, Self::observed_content(result.as_ref(), Some(max_size)))?;
        Ok(result)
    }

    async fn read_object_unlocked(
        &self,
        bucket: &str,
        object: &str,
    ) -> crate::table_catalog::TableCatalogStoreResult<Option<crate::table_catalog::TableCatalogObject>> {
        self.read_object(bucket, object).await
    }

    async fn read_object_unlocked_limited(
        &self,
        bucket: &str,
        object: &str,
        max_size: usize,
    ) -> crate::table_catalog::TableCatalogStoreResult<Option<crate::table_catalog::TableCatalogObject>> {
        self.read_object_limited(bucket, object, max_size).await
    }

    async fn object_exists(&self, bucket: &str, object: &str) -> crate::table_catalog::TableCatalogStoreResult<bool> {
        self.authorize(bucket, object, S3Action::GetObjectAction).await?;
        self.ensure_observation_allowed(bucket, object)?;
        let metadata = self.backend.object_metadata(bucket, object).await?;
        let observation = Self::observed_metadata(bucket, object, metadata)?;
        let exists = observation.identity != TableCommitObjectIdentity::Missing;
        self.record_observation(bucket, object, observation)?;
        Ok(exists)
    }

    async fn object_exists_unlocked(&self, bucket: &str, object: &str) -> crate::table_catalog::TableCatalogStoreResult<bool> {
        self.object_exists(bucket, object).await
    }

    async fn put_object(
        &self,
        bucket: &str,
        object: &str,
        data: Vec<u8>,
        precondition: crate::table_catalog::TableCatalogPutPrecondition,
    ) -> crate::table_catalog::TableCatalogStoreResult<()> {
        self.authorize(bucket, object, S3Action::PutObjectAction).await?;
        self.ensure_observation_allowed(bucket, object)?;
        let observation = TableCommitObservedObject {
            identity: TableCommitObjectIdentity::ContentSha256(hex_sha256(&data, str::to_string)),
            max_size: None,
        };
        self.backend.put_object(bucket, object, data, precondition).await?;
        self.record_observation(bucket, object, observation)
    }

    async fn put_object_unlocked(
        &self,
        bucket: &str,
        object: &str,
        data: Vec<u8>,
        precondition: crate::table_catalog::TableCatalogPutPrecondition,
    ) -> crate::table_catalog::TableCatalogStoreResult<()> {
        self.put_object(bucket, object, data, precondition).await
    }

    async fn delete_object(&self, bucket: &str, object: &str) -> crate::table_catalog::TableCatalogStoreResult<()> {
        self.authorize(bucket, object, S3Action::DeleteObjectAction).await?;
        self.ensure_observation_allowed(bucket, object)?;
        self.backend.delete_object(bucket, object).await
    }

    async fn delete_object_unlocked(&self, bucket: &str, object: &str) -> crate::table_catalog::TableCatalogStoreResult<()> {
        self.delete_object(bucket, object).await
    }

    async fn list_objects(&self, _bucket: &str, _prefix: &str) -> crate::table_catalog::TableCatalogStoreResult<Vec<String>> {
        Err(crate::table_catalog::TableCatalogStoreError::Unsupported(
            "table commit validation does not list object prefixes".to_string(),
        ))
    }

    async fn acquire_read_lock(
        &self,
        bucket: &str,
        object: &str,
    ) -> crate::table_catalog::TableCatalogStoreResult<crate::table_catalog::TableCatalogLockGuard> {
        self.backend.acquire_read_lock(bucket, object).await
    }

    async fn acquire_write_lock(
        &self,
        bucket: &str,
        object: &str,
    ) -> crate::table_catalog::TableCatalogStoreResult<crate::table_catalog::TableCatalogLockGuard> {
        self.backend.acquire_write_lock(bucket, object).await
    }

    async fn begin_table_bucket_commit_publication(
        &self,
        table_bucket: &str,
    ) -> crate::table_catalog::TableCatalogStoreResult<()> {
        self.begin_bucket_publication(table_bucket).await
    }

    fn table_bucket_commit_publication_is_held(&self, table_bucket: &str) -> bool {
        let publication = self.publication.lock();
        publication.bucket_fence.as_deref() == Some(table_bucket) && publication.guards.iter().all(|guard| !guard.is_lock_lost())
    }

    async fn prepare_table_commit_publication(
        &self,
        table_bucket: &str,
        namespace: &str,
        table: &str,
    ) -> crate::table_catalog::TableCatalogStoreResult<()> {
        self.prepare_publication(table_bucket, namespace, table).await
    }

    fn table_commit_publication_is_held(&self, table_bucket: &str, namespace: &str, table: &str) -> bool {
        let publication = self.publication.lock();
        publication
            .table_fence
            .as_ref()
            .is_some_and(|held| held.0 == table_bucket && held.1 == namespace && held.2 == table)
            && publication.guards.iter().all(|guard| !guard.is_lock_lost())
    }

    fn complete_table_commit_publication(&self) {
        self.complete_publication();
    }
}

async fn read_limited_body(mut input: Body, max_size: usize, timeout: StdDuration, operation: Option<&str>) -> S3Result<Bytes> {
    tokio::time::timeout(timeout, input.store_all_limited(max_size))
        .await
        .map_err(|_| {
            operation.map_or_else(
                || S3Error::from(ApiError::invalid_request("timed out reading request body")),
                |operation| S3Error::from(ApiError::invalid_request(format!("timed out reading {operation} request body"))),
            )
        })?
        .map_err(|err| S3Error::from(ApiError::invalid_request(format!("failed to read request body: {err}"))))
}

async fn read_json_body<T: DeserializeOwned>(input: Body) -> S3Result<T> {
    let body = read_limited_body(input, MAX_ADMIN_REQUEST_BODY_SIZE, TABLE_CATALOG_REQUEST_BODY_TIMEOUT, None).await?;
    if body.is_empty() {
        return Err(S3Error::from(ApiError::invalid_request("request body is required")));
    }
    serde_json::from_slice(&body).map_err(|err| S3Error::from(ApiError::invalid_request(format!("invalid JSON: {err}"))))
}

fn validate_rest_commit_request_shape(
    value: &serde_json::Value,
    require_requirements: bool,
    require_updates: bool,
) -> S3Result<()> {
    let object = value
        .as_object()
        .ok_or_else(|| S3Error::from(ApiError::invalid_request("commit request must be a JSON object")))?;
    if object.get("new-metadata-location").is_some_and(serde_json::Value::is_string) {
        for field in ["requirements", "updates"] {
            if object
                .get(field)
                .and_then(serde_json::Value::as_array)
                .is_some_and(|values| !values.is_empty())
            {
                return Err(S3Error::from(ApiError::invalid_request(format!(
                    "legacy metadata pointer commit must not include standard {field}"
                ))));
            }
        }
        return Ok(());
    }
    if require_requirements && !object.contains_key("requirements") {
        return Err(S3Error::from(ApiError::invalid_request("commit request requires requirements")));
    }
    if require_updates && !object.contains_key("updates") {
        return Err(S3Error::from(ApiError::invalid_request("commit request requires updates")));
    }
    Ok(())
}

async fn read_rest_commit_table_request(input: Body) -> S3Result<RestCommitTableRequest> {
    let value = read_json_body::<serde_json::Value>(input).await?;
    validate_rest_commit_request_shape(&value, true, true)?;
    serde_json::from_value(value).map_err(|err| S3Error::from(ApiError::invalid_request(format!("invalid JSON: {err}"))))
}

async fn read_rest_commit_view_request(input: Body) -> S3Result<RestCommitViewRequest> {
    let value = read_json_body::<serde_json::Value>(input).await?;
    validate_rest_commit_request_shape(&value, false, true)?;
    serde_json::from_value(value).map_err(|err| S3Error::from(ApiError::invalid_request(format!("invalid JSON: {err}"))))
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
            .map_err(|_| S3Error::from(ApiError::invalid_request("Content-Length must be valid ASCII")))?
            .parse::<usize>()
            .map_err(|_| S3Error::from(ApiError::invalid_request("Content-Length must be a non-negative integer")))?;
        if content_length > max_size {
            return Err(S3Error::from(ApiError::invalid_request(format!("{operation} request body is too large"))));
        }
    }
    let body = read_limited_body(input, max_size, timeout, Some(operation)).await?;
    if body.is_empty() {
        return Err(S3Error::from(ApiError::invalid_request("request body is required")));
    }
    serde_json::from_slice(&body).map_err(|err| S3Error::from(ApiError::invalid_request(format!("invalid JSON: {err}"))))
}

async fn read_json_body_or_default<T>(input: Body) -> S3Result<T>
where
    T: Default + DeserializeOwned,
{
    let body = read_limited_body(input, MAX_ADMIN_REQUEST_BODY_SIZE, TABLE_CATALOG_REQUEST_BODY_TIMEOUT, None).await?;
    if body.is_empty() {
        return Ok(T::default());
    }
    serde_json::from_slice(&body).map_err(|err| S3Error::from(ApiError::invalid_request(format!("invalid JSON: {err}"))))
}

fn warehouse_from_params(params: &Params<'_, '_>) -> S3Result<String> {
    let warehouse = params.get("warehouse").unwrap_or("");
    if warehouse.is_empty() {
        return Err(S3Error::from(ApiError::invalid_request("warehouse is required")));
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
            let value = if value.eq_ignore_ascii_case("true") {
                true
            } else if value.eq_ignore_ascii_case("false") {
                false
            } else {
                return Err(iceberg_rest_error(
                    ICEBERG_ERROR_BAD_REQUEST,
                    StatusCode::BAD_REQUEST,
                    "purgeRequested query parameter must be true or false",
                ));
            };
            purge_requested = Some(value);
        }
    }
    Ok(purge_requested.unwrap_or(false))
}

fn rest_table_snapshot_selection_from_query(uri: &http::Uri) -> S3Result<RestTableSnapshotSelection> {
    let mut selection = None;
    if let Some(query) = uri.query() {
        for (key, value) in url::form_urlencoded::parse(query.as_bytes()) {
            if key != "snapshots" {
                continue;
            }
            if selection.is_some() {
                return Err(iceberg_rest_error(
                    ICEBERG_ERROR_BAD_REQUEST,
                    StatusCode::BAD_REQUEST,
                    "snapshots query parameter must not be repeated",
                ));
            }
            selection = Some(match value.as_ref() {
                "all" => RestTableSnapshotSelection::All,
                "refs" => RestTableSnapshotSelection::Refs,
                _ => {
                    return Err(iceberg_rest_error(
                        ICEBERG_ERROR_BAD_REQUEST,
                        StatusCode::BAD_REQUEST,
                        "snapshots query parameter must be all or refs",
                    ));
                }
            });
        }
    }
    Ok(selection.unwrap_or(RestTableSnapshotSelection::All))
}

fn apply_rest_table_snapshot_selection(metadata: &mut serde_json::Value, selection: RestTableSnapshotSelection) {
    if selection == RestTableSnapshotSelection::All {
        return;
    }
    let mut referenced_snapshot_ids = metadata
        .get("refs")
        .and_then(serde_json::Value::as_object)
        .into_iter()
        .flat_map(|refs| refs.values())
        .filter_map(|reference| reference.get("snapshot-id").and_then(serde_json::Value::as_i64))
        .collect::<BTreeSet<_>>();
    if let Some(current_snapshot_id) = metadata
        .get("current-snapshot-id")
        .and_then(serde_json::Value::as_i64)
        .filter(|snapshot_id| *snapshot_id != -1)
    {
        referenced_snapshot_ids.insert(current_snapshot_id);
    }
    if let Some(snapshots) = metadata.get_mut("snapshots").and_then(serde_json::Value::as_array_mut) {
        snapshots.retain(|snapshot| {
            snapshot
                .get("snapshot-id")
                .and_then(serde_json::Value::as_i64)
                .is_some_and(|snapshot_id| referenced_snapshot_ids.contains(&snapshot_id))
        });
    }
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
    namespace_from_path_value(namespace)
}

fn namespace_from_path_value(value: &str) -> S3Result<crate::table_catalog::Namespace> {
    let legacy_dotted = value.contains('.')
        && !value.contains(REST_NAMESPACE_SEPARATOR)
        && !value.contains(REST_NAMESPACE_SEPARATOR_URL_ENCODED)
        && !value.contains("%1f");
    // RUSTFS_COMPAT_TODO(table-catalog-dotted-namespace): Remove after the minimum supported release
    // advertises %1F; until then, keep dotted paths for clients using the legacy namespace contract.
    let segments = if legacy_dotted {
        value
            .split('.')
            .map(|segment| {
                percent_decode_str(segment)
                    .decode_utf8()
                    .map(|decoded| decoded.into_owned())
                    .map_err(|_| s3_error!(InvalidRequest, "namespace path must be valid UTF-8"))
            })
            .collect::<S3Result<Vec<_>>>()?
    } else {
        let decoded = percent_decode_str(value)
            .decode_utf8()
            .map_err(|_| s3_error!(InvalidRequest, "namespace path must be valid UTF-8"))?;
        decoded.split(REST_NAMESPACE_SEPARATOR).map(str::to_string).collect()
    };
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
                parent = Some(
                    crate::table_catalog::Namespace::from_segments(
                        value.split(REST_NAMESPACE_SEPARATOR).map(str::to_string).collect(),
                    )
                    .map_err(|err| {
                        iceberg_rest_error(
                            ICEBERG_ERROR_BAD_REQUEST,
                            StatusCode::BAD_REQUEST,
                            format!("invalid parent namespace: {err}"),
                        )
                    })?,
                );
            }
        }
    }
    Ok(parent)
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

fn table_catalog_backend_from_extensions(
    extensions: &http::Extensions,
) -> S3Result<crate::table_catalog::EcStoreTableCatalogObjectBackend<ECStore>> {
    let context = runtime_sources::app_context_from_extensions(extensions)
        .ok_or_else(|| table_catalog_internal_error("request application context is not initialized"))?;
    Ok(crate::table_catalog::EcStoreTableCatalogObjectBackend::new_with_strong_runtime(
        context.object_store(),
        context.table_catalog_strong_runtime(),
    ))
}

type EcStoreObjectTableCatalogStore =
    crate::table_catalog::ObjectTableCatalogStore<crate::table_catalog::EcStoreTableCatalogObjectBackend<ECStore>>;

fn table_catalog_store_from_backend(
    backend: crate::table_catalog::EcStoreTableCatalogObjectBackend<ECStore>,
) -> S3Result<crate::table_catalog::EcStoreTableCatalogStore<ECStore>> {
    crate::table_catalog::ConfiguredTableCatalogStore::from_env(backend).map_err(catalog_store_error)
}

fn table_catalog_store_from_extensions(
    extensions: &http::Extensions,
) -> S3Result<crate::table_catalog::EcStoreTableCatalogStore<ECStore>> {
    table_catalog_store_from_backend(table_catalog_backend_from_extensions(extensions)?)
}

fn table_catalog_object_store_from_extensions(extensions: &http::Extensions) -> S3Result<EcStoreObjectTableCatalogStore> {
    match crate::table_catalog::TableCatalogBackingMode::from_env().map_err(catalog_store_error)? {
        crate::table_catalog::TableCatalogBackingMode::ObjectBacked => Ok(crate::table_catalog::ObjectTableCatalogStore::new(
            table_catalog_backend_from_extensions(extensions)?,
        )),
        crate::table_catalog::TableCatalogBackingMode::DurableStrong => Err(s3_error!(
            InvalidRequest,
            "operation is not supported with {} table catalog backing",
            crate::table_catalog::TABLE_CATALOG_BACKING_DURABLE_STRONG
        )),
    }
}

async fn table_bucket_enabled_from_extensions(extensions: &http::Extensions, bucket: &str) -> S3Result<bool> {
    let store = runtime_sources::object_store_from_extensions(extensions)
        .ok_or_else(|| table_catalog_internal_error("request object store is not initialized"))?;
    let metadata = store
        .get_bucket_metadata(bucket)
        .await
        .map_err(|err| s3_error!(InvalidRequest, "failed to load table bucket metadata for {bucket}: {}", err))?;
    Ok(metadata.table_bucket_enabled())
}

async fn ensure_table_bucket_enabled_from_extensions(extensions: &http::Extensions, bucket: &str) -> S3Result<()> {
    if table_bucket_enabled_from_extensions(extensions, bucket).await? {
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

async fn enable_table_bucket_marker(store: &ECStore, bucket: &str) -> S3Result<()> {
    let marker = crate::table_catalog::table_bucket_marker_json()
        .map_err(|err| s3_error!(InternalError, "failed to serialize table bucket marker: {}", err))?;
    store
        .update_bucket_metadata_config(bucket, crate::table_catalog::TABLE_BUCKET_MARKER_CONFIG, marker)
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

async fn enable_table_bucket_response<S>(
    store: &S,
    publication: &impl crate::table_catalog::TableCatalogObjectBackend,
    object_store: &ECStore,
    bucket: &str,
) -> S3Result<TableBucketResponse>
where
    S: crate::table_catalog::TableCatalogStore + ?Sized,
{
    crate::table_catalog::TableCommitPublication::begin_table_bucket(publication, bucket)
        .await
        .map_err(catalog_store_error)?;
    if !crate::table_catalog::TableCommitPublication::holds_table_bucket(publication, bucket) {
        return Err(s3_error!(InternalError, "table bucket enablement requires a publication fence"));
    }
    let _publication_completion = crate::table_catalog::TableCommitPublicationCompletion::new(publication);
    enable_table_bucket_marker(object_store, bucket).await?;
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

fn validate_rest_commit_identifier(
    identifier: Option<&RestTableIdentifier>,
    namespace: &crate::table_catalog::Namespace,
    name: &str,
) -> S3Result<()> {
    if let Some(identifier) = identifier
        && (identifier.namespace != namespace_segments(namespace) || identifier.name != name)
    {
        return Err(iceberg_rest_error(
            ICEBERG_ERROR_BAD_REQUEST,
            StatusCode::BAD_REQUEST,
            "request identifier must match the resource URL",
        ));
    }
    Ok(())
}

fn namespace_from_segments(segments: &[String]) -> S3Result<crate::table_catalog::Namespace> {
    crate::table_catalog::Namespace::from_segments(segments.to_vec())
        .map_err(|err| s3_error!(InvalidRequest, "invalid namespace: {}", err))
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
        commit_id: request
            .commit_id
            .or_else(|| request.idempotency_key.clone())
            .unwrap_or_else(|| Uuid::new_v4().to_string()),
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
    crate::table_catalog::table_metadata_uuid(metadata).map_err(catalog_store_error)
}

fn metadata_format_version(metadata: &serde_json::Value) -> S3Result<u16> {
    crate::table_catalog::table_metadata_format_version(metadata).map_err(catalog_store_error)
}

fn metadata_table_location(metadata: &serde_json::Value) -> S3Result<&str> {
    crate::table_catalog::table_metadata_location(metadata).map_err(catalog_store_error)
}

fn validate_metadata_table_location_in_bucket(bucket: &str, metadata: &serde_json::Value) -> S3Result<()> {
    let location = metadata_table_location(metadata)?;
    validate_table_location_in_bucket(bucket, location)
}

fn table_warehouse_location_changes(
    current: &crate::table_catalog::TableEntry,
    target_metadata: &serde_json::Value,
) -> S3Result<bool> {
    Ok(current.warehouse_location != metadata_table_location(target_metadata)?)
}

fn metadata_digest_requirement(metadata: &serde_json::Value) -> S3Result<serde_json::Value> {
    let sha256 = crate::table_catalog::canonical_json_sha256(metadata).map_err(catalog_store_error)?;
    Ok(serde_json::json!({
        "type": crate::table_catalog::TABLE_METADATA_DIGEST_REQUIREMENT_TYPE,
        "sha256": sha256
    }))
}

fn validate_metadata_view_location_in_bucket(bucket: &str, metadata: &serde_json::Value) -> S3Result<()> {
    let location = metadata_table_location(metadata)?;
    validate_view_location_in_bucket(bucket, location)
}

fn validate_persisted_table_metadata(
    entry: &crate::table_catalog::TableEntry,
    metadata: &serde_json::Value,
    require_current_warehouse: bool,
) -> S3Result<()> {
    crate::table_catalog::validate_supported_table_metadata(metadata).map_err(|_| persisted_metadata_error("table"))?;
    validate_metadata_table_location_in_bucket(&entry.table_bucket, metadata).map_err(|_| persisted_metadata_error("table"))?;
    let metadata_uuid = metadata_table_uuid(metadata).map_err(|_| persisted_metadata_error("table"))?;
    let metadata_location = metadata_table_location(metadata).map_err(|_| persisted_metadata_error("table"))?;
    let format_version = metadata_format_version(metadata).map_err(|_| persisted_metadata_error("table"))?;
    if metadata_uuid != entry.table_uuid
        || (require_current_warehouse && format_version < entry.format_version)
        || (require_current_warehouse && metadata_location != entry.warehouse_location)
    {
        return Err(persisted_metadata_error("table"));
    }
    Ok(())
}

fn validate_persisted_table_metadata_location(entry: &crate::table_catalog::TableEntry, metadata_location: &str) -> S3Result<()> {
    if !crate::table_catalog::is_valid_table_metadata_location_for_entry(entry, metadata_location) {
        return Err(persisted_metadata_error("table"));
    }
    Ok(())
}

fn validate_persisted_view_metadata(entry: &crate::table_catalog::ViewEntry, metadata: &serde_json::Value) -> S3Result<()> {
    validate_persisted_view_metadata_identity(entry, metadata)?;
    crate::table_catalog::validate_supported_view_metadata(metadata).map_err(|_| persisted_metadata_error("view"))
}

fn validate_persisted_view_metadata_identity(
    entry: &crate::table_catalog::ViewEntry,
    metadata: &serde_json::Value,
) -> S3Result<()> {
    validate_metadata_view_location_in_bucket(&entry.table_bucket, metadata).map_err(|_| persisted_metadata_error("view"))?;
    let metadata_uuid = metadata_view_uuid(metadata).map_err(|_| persisted_metadata_error("view"))?;
    let metadata_location = metadata_table_location(metadata).map_err(|_| persisted_metadata_error("view"))?;
    let format_version = metadata_format_version(metadata).map_err(|_| persisted_metadata_error("view"))?;
    if metadata_uuid != entry.view_uuid || metadata_location != entry.warehouse_location || format_version != entry.format_version
    {
        return Err(persisted_metadata_error("view"));
    }
    Ok(())
}

fn validate_metadata_matches_current_metadata(
    current_metadata: &serde_json::Value,
    target_metadata: &serde_json::Value,
) -> S3Result<()> {
    crate::table_catalog::validate_supported_table_metadata(target_metadata).map_err(catalog_store_error)?;
    validate_metadata_identity_matches_current_metadata(current_metadata, target_metadata)?;
    crate::table_catalog::validate_table_metadata_transition(current_metadata, target_metadata).map_err(catalog_store_error)
}

fn validate_metadata_identity_matches_current_metadata(
    current_metadata: &serde_json::Value,
    target_metadata: &serde_json::Value,
) -> S3Result<()> {
    let expected_table_uuid = metadata_table_uuid(current_metadata)?;
    let expected_format_version = metadata_format_version(current_metadata)?;
    let target_table_uuid = metadata_table_uuid(target_metadata)?;
    let target_format_version = metadata_format_version(target_metadata)?;
    if target_table_uuid != expected_table_uuid {
        return Err(s3_error!(
            InvalidRequest,
            "table metadata table-uuid does not match current table metadata"
        ));
    }
    if target_format_version < expected_format_version {
        return Err(S3Error::from(ApiError::invalid_request(
            "table metadata format-version cannot be downgraded",
        )));
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
    crate::table_catalog::validate_supported_table_metadata(metadata).map_err(catalog_store_error)?;
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
    let CreateTableRequest {
        name,
        location,
        mut schema,
        mut partition_spec,
        mut write_order,
        stage_create,
        mut properties,
    } = request;
    if stage_create {
        return Err(iceberg_rest_error(
            ICEBERG_ERROR_UNSUPPORTED_OPERATION,
            StatusCode::NOT_ACCEPTABLE,
            "stage-create is not supported",
        ));
    }

    let table = crate::table_catalog::IdentifierSegment::parse(name)
        .map_err(|err| s3_error!(InvalidRequest, "invalid table name: {}", err))?;
    let table_id = Uuid::new_v4().to_string();
    let table_uuid = Uuid::new_v4().to_string();
    let format_version = match properties.remove("format-version") {
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
    let warehouse_location = location.unwrap_or_else(|| format!("s3://{bucket}/tables/{table_id}"));
    validate_table_location_in_bucket(bucket, &warehouse_location)?;
    let metadata_location =
        crate::table_catalog::default_table_metadata_file_path(namespace, &table, &next_metadata_file_name(1, &table_id));

    crate::table_catalog::assign_fresh_create_schema_ids(&mut schema, partition_spec.as_mut(), write_order.as_mut())
        .map_err(catalog_store_error)?;

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
        properties,
        created_at: None,
        updated_at: None,
    };
    let metadata = initial_table_metadata_json(&entry, schema, partition_spec, write_order, entry.properties.clone())?;
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
    schema_object.insert("schema-id".to_string(), serde_json::Value::from(0));
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
    spec_object.insert("spec-id".to_string(), serde_json::Value::from(0));
    spec_object
        .entry("fields".to_string())
        .or_insert_with(|| serde_json::Value::Array(Vec::new()));
    let last_partition_id = assign_partition_field_ids(&mut spec, 999, &BTreeMap::new())?;

    let mut sort_order = write_order.unwrap_or_else(|| {
        serde_json::json!({
            "order-id": 0,
            "fields": []
        })
    });
    let sort_order_object = sort_order
        .as_object_mut()
        .ok_or_else(|| s3_error!(InvalidRequest, "write-order must be a JSON object"))?;
    let sort_order_fields = sort_order_object
        .entry("fields".to_string())
        .or_insert_with(|| serde_json::Value::Array(Vec::new()));
    let sort_order_id = if sort_order_fields
        .as_array()
        .ok_or_else(|| S3Error::from(ApiError::invalid_request("write-order fields must be an array")))?
        .is_empty()
    {
        0
    } else {
        1
    };
    sort_order_object.insert("order-id".to_string(), serde_json::Value::from(sort_order_id));
    let mut metadata = serde_json::json!({
        "format-version": entry.format_version,
        "table-uuid": entry.table_uuid,
        "location": entry.warehouse_location,
        "last-updated-ms": current_time_millis(),
        "last-column-id": last_column_id,
        "schemas": [schema],
        "current-schema-id": 0,
        "partition-specs": [spec],
        "default-spec-id": 0,
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
    crate::table_catalog::synchronize_table_metadata_version_fields(&mut metadata).map_err(catalog_store_error)?;
    crate::table_catalog::validate_supported_table_metadata(&metadata).map_err(catalog_store_error)?;
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
    schema_object.insert("schema-id".to_string(), serde_json::Value::from(0));

    let view_version_object = view_version
        .as_object_mut()
        .ok_or_else(|| s3_error!(InvalidRequest, "view-version must be a JSON object"))?;
    view_version_object.insert("schema-id".to_string(), serde_json::Value::from(0));
    let version_id = view_version_object
        .get("version-id")
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| s3_error!(InvalidRequest, "view-version version-id must be an integer"))?;
    let timestamp_ms = view_version_object
        .get("timestamp-ms")
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| S3Error::from(ApiError::invalid_request("view-version timestamp-ms must be an integer")))?;

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
            for field in ["id", "element-id", "key-id", "value-id"] {
                if let Some(id) = object.get(field).and_then(serde_json::Value::as_i64) {
                    *max_id = (*max_id).max(id);
                }
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

fn table_scoped_metadata_file_name(generation: u64, table_id: &str, metadata_file_token: &str) -> String {
    let scoped_token = table_catalog_path_hash(&format!("table-metadata:{}:{table_id}{metadata_file_token}", table_id.len()));
    format!("{generation:05}-table-{scoped_token}.metadata.json")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GeneratedMetadataComparison {
    MatchingCommit,
    DifferentTable,
}

fn generated_metadata_error(err: crate::table_catalog::TableCatalogStoreError) -> S3Error {
    match err {
        err @ crate::table_catalog::TableCatalogStoreError::Conflict(_) => catalog_store_error(err),
        _ => iceberg_rest_error(
            ICEBERG_ERROR_REST,
            StatusCode::INTERNAL_SERVER_ERROR,
            "existing generated metadata is invalid",
        ),
    }
}

fn compare_generated_metadata_with_standard_commit(
    metadata: &serde_json::Value,
    expected_metadata: &serde_json::Value,
    updates: &[serde_json::Value],
    previous_metadata_location: &str,
) -> S3Result<GeneratedMetadataComparison> {
    let table_uuid = crate::table_catalog::table_metadata_uuid(metadata).map_err(generated_metadata_error)?;
    if metadata_table_uuid(expected_metadata)? != table_uuid {
        crate::table_catalog::validate_supported_table_metadata(metadata).map_err(generated_metadata_error)?;
        return Ok(GeneratedMetadataComparison::DifferentTable);
    }
    let timestamp_ms = metadata
        .get("last-updated-ms")
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| {
            iceberg_rest_error(
                ICEBERG_ERROR_REST,
                StatusCode::INTERNAL_SERVER_ERROR,
                "existing generated metadata is invalid",
            )
        })?;
    let rebuilt_metadata =
        apply_table_commit_updates_at(expected_metadata.clone(), updates, previous_metadata_location, timestamp_ms)?;
    if &rebuilt_metadata == metadata {
        return Ok(GeneratedMetadataComparison::MatchingCommit);
    }
    crate::table_catalog::validate_supported_table_metadata(metadata).map_err(generated_metadata_error)?;
    Err(iceberg_rest_error(
        ICEBERG_ERROR_COMMIT_FAILED,
        StatusCode::CONFLICT,
        "generated metadata location already contains a different commit",
    ))
}

fn validate_table_commit_requirements(metadata: &serde_json::Value, requirements: &[serde_json::Value]) -> S3Result<()> {
    for requirement in requirements {
        let requirement_type = requirement
            .get("type")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| s3_error!(InvalidRequest, "commit requirement type is required"))?;
        match requirement_type {
            "assert-create" => {
                return Err(commit_requirement_failed("commit requirement failed: table already exists"));
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
                    return Err(commit_requirement_failed("commit requirement failed: table uuid changed"));
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
            _ => {
                return Err(S3Error::from(ApiError::invalid_request(format!(
                    "unsupported commit requirement: {requirement_type}"
                ))));
            }
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
        return Err(commit_requirement_failed(format!("commit requirement failed: {label} changed")));
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
            return Err(commit_requirement_failed("commit requirement failed: snapshot ref exists"));
        }
        return Ok(());
    }
    let expected = requirement
        .get("snapshot-id")
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| s3_error!(InvalidRequest, "assert-ref-snapshot-id requires snapshot-id"))?;
    if actual != Some(expected) {
        return Err(commit_requirement_failed("commit requirement failed: snapshot ref changed"));
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
    if metadata.get("format-version").is_some() {
        crate::table_catalog::synchronize_table_metadata_version_fields(&mut metadata).map_err(catalog_store_error)?;
    }
    let mut next_schema_id = next_catalog_id_for_updates(&metadata, updates, "add-schema", "schemas", "schema-id")?;
    let mut next_spec_id = next_catalog_id_for_updates(&metadata, updates, "add-spec", "partition-specs", "spec-id")?;
    let mut next_sort_order_id = next_catalog_id_for_updates(&metadata, updates, "add-sort-order", "sort-orders", "order-id")?;
    let mut last_added_schema_id = None;
    let mut last_added_spec_id = None;
    let mut last_added_sort_order_id = None;
    let mut added_snapshot_ids = BTreeSet::new();

    for update in updates {
        let action = update
            .get("action")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| s3_error!(InvalidRequest, "table update action is required"))?;
        match action {
            "assign-uuid" => apply_assign_uuid_update(&mut metadata, update, "table-uuid", "table")?,
            "upgrade-format-version" => apply_upgrade_format_version_update(&mut metadata, update)?,
            "add-schema" => {
                let schema_id = take_catalog_assigned_id(&mut next_schema_id, "schema-id")?;
                apply_add_table_schema_update(&mut metadata, update, schema_id)?;
                last_added_schema_id = Some(schema_id);
            }
            "set-current-schema" => {
                apply_set_current_schema_update(&mut metadata, update, last_added_schema_id)?;
            }
            "add-spec" => {
                let spec_id = take_catalog_assigned_id(&mut next_spec_id, "spec-id")?;
                apply_add_spec_update(&mut metadata, update, spec_id)?;
                last_added_spec_id = Some(spec_id);
            }
            "set-default-spec" => {
                apply_set_default_spec_update(&mut metadata, update, last_added_spec_id)?;
            }
            "add-sort-order" => {
                let sort_order_id = take_catalog_assigned_id(&mut next_sort_order_id, "sort order-id")?;
                last_added_sort_order_id = Some(apply_add_sort_order_update(&mut metadata, update, sort_order_id)?);
            }
            "set-default-sort-order" => {
                apply_set_default_sort_order_update(&mut metadata, update, last_added_sort_order_id)?;
            }
            "add-snapshot" => {
                added_snapshot_ids.insert(apply_add_snapshot_update(&mut metadata, update)?);
            }
            "set-snapshot-ref" => {
                apply_set_snapshot_ref_update(&mut metadata, update, &added_snapshot_ids, commit_timestamp_ms)?;
            }
            "remove-snapshots" => apply_remove_snapshots_update(&mut metadata, update)?,
            "remove-snapshot-ref" => apply_remove_snapshot_ref_update(&mut metadata, update)?,
            "set-location" => apply_set_location_update(&mut metadata, update)?,
            "set-properties" => apply_set_properties_update(&mut metadata, update)?,
            "remove-properties" => apply_remove_properties_update(&mut metadata, update)?,
            "set-statistics" => apply_set_snapshot_file_update(
                &mut metadata,
                update,
                "statistics",
                "statistics",
                crate::table_catalog::IcebergStatisticsFileKind::Table,
            )?,
            "remove-statistics" => apply_remove_snapshot_file_update(&mut metadata, update, "statistics")?,
            "set-partition-statistics" => {
                apply_set_snapshot_file_update(
                    &mut metadata,
                    update,
                    "partition-statistics",
                    "partition-statistics",
                    crate::table_catalog::IcebergStatisticsFileKind::Partition,
                )?;
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
            "add-encryption-key" | "remove-encryption-key" => {
                return Err(iceberg_rest_error(
                    ICEBERG_ERROR_UNSUPPORTED_OPERATION,
                    StatusCode::NOT_ACCEPTABLE,
                    "table encryption keys require Iceberg format-version 3",
                ));
            }
            _ => return Err(S3Error::from(ApiError::invalid_request(format!("unsupported table update: {action}")))),
        }
    }

    prune_intermediate_snapshot_log_entries(&mut metadata, &added_snapshot_ids)?;

    if metadata.get("format-version").is_some() {
        crate::table_catalog::synchronize_table_metadata_version_fields(&mut metadata).map_err(catalog_store_error)?;
    }
    append_previous_metadata_log(&mut metadata, previous_metadata_location)?;
    metadata_object_mut(&mut metadata)?.insert("last-updated-ms".to_string(), serde_json::Value::from(commit_timestamp_ms));
    Ok(metadata)
}

fn prune_intermediate_snapshot_log_entries(metadata: &mut serde_json::Value, added_snapshot_ids: &BTreeSet<i64>) -> S3Result<()> {
    if added_snapshot_ids.is_empty() {
        return Ok(());
    }
    let current_snapshot_id = metadata.get("current-snapshot-id").and_then(serde_json::Value::as_i64);
    let snapshot_log = ensure_array_field(metadata, "snapshot-log")?;
    for entry in snapshot_log.iter() {
        entry
            .get("snapshot-id")
            .and_then(serde_json::Value::as_i64)
            .ok_or_else(|| S3Error::from(ApiError::invalid_request("snapshot-log snapshot-id must be an integer")))?;
    }
    snapshot_log.retain(|entry| {
        entry
            .get("snapshot-id")
            .and_then(serde_json::Value::as_i64)
            .is_none_or(|snapshot_id| !added_snapshot_ids.contains(&snapshot_id) || Some(snapshot_id) == current_snapshot_id)
    });
    Ok(())
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
                    return Err(commit_requirement_failed("commit requirement failed: view uuid changed"));
                }
            }
            _ => {
                return Err(S3Error::from(ApiError::invalid_request(format!(
                    "unsupported view commit requirement: {requirement_type}"
                ))));
            }
        }
    }
    Ok(())
}

fn validate_supported_view_metadata(metadata: &serde_json::Value) -> S3Result<()> {
    crate::table_catalog::validate_supported_view_metadata(metadata).map_err(catalog_store_error)
}

fn apply_view_commit_updates_at(
    mut metadata: serde_json::Value,
    updates: &[serde_json::Value],
    commit_timestamp_ms: i64,
) -> S3Result<serde_json::Value> {
    if !metadata.is_object() {
        return Err(s3_error!(InvalidRequest, "current view metadata must be a JSON object"));
    }
    let mut next_schema_id = next_catalog_id_for_updates(&metadata, updates, "add-schema", "schemas", "schema-id")?;
    let mut last_added_schema_id = None;
    let mut last_added_view_version_id = None;
    let mut added_view_version_timestamps = BTreeMap::new();

    for update in updates {
        let action = update
            .get("action")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| s3_error!(InvalidRequest, "view update action is required"))?;
        match action {
            "assign-uuid" => apply_assign_uuid_update(&mut metadata, update, "view-uuid", "view")?,
            "upgrade-format-version" => apply_upgrade_view_format_version_update(update)?,
            "add-schema" => {
                let schema_id = take_catalog_assigned_id(&mut next_schema_id, "schema-id")?;
                apply_add_view_schema_update(&mut metadata, update, schema_id)?;
                last_added_schema_id = Some(schema_id);
            }
            "add-view-version" => {
                let (version_id, timestamp_ms) = apply_add_view_version_update(&mut metadata, update, last_added_schema_id)?;
                last_added_view_version_id = Some(version_id);
                added_view_version_timestamps.insert(version_id, timestamp_ms);
            }
            "set-current-view-version" => {
                apply_set_current_view_version_update(
                    &mut metadata,
                    update,
                    last_added_view_version_id,
                    &added_view_version_timestamps,
                    commit_timestamp_ms,
                )?;
            }
            "set-location" => apply_set_location_update(&mut metadata, update)?,
            "set-properties" => apply_set_properties_update(&mut metadata, update)?,
            "remove-properties" => apply_remove_properties_update(&mut metadata, update)?,
            _ => return Err(S3Error::from(ApiError::invalid_request(format!("unsupported view update: {action}")))),
        }
    }

    validate_supported_view_metadata(&metadata)?;
    Ok(metadata)
}

fn apply_set_snapshot_file_update(
    metadata: &mut serde_json::Value,
    update: &serde_json::Value,
    metadata_field: &str,
    update_field: &str,
    kind: crate::table_catalog::IcebergStatisticsFileKind,
) -> S3Result<()> {
    let value = update
        .get(update_field)
        .cloned()
        .ok_or_else(|| S3Error::from(ApiError::invalid_request(format!("{update_field} is required"))))?;
    let snapshot_id =
        crate::table_catalog::validate_iceberg_statistics_file(&value, update_field, kind).map_err(catalog_store_error)?;
    if let Some(deprecated_snapshot_id) = update.get("snapshot-id") {
        let deprecated_snapshot_id = deprecated_snapshot_id.as_i64().ok_or_else(|| {
            iceberg_rest_error(ICEBERG_ERROR_BAD_REQUEST, StatusCode::BAD_REQUEST, "snapshot-id must be an integer")
        })?;
        if deprecated_snapshot_id != snapshot_id {
            return Err(iceberg_rest_error(
                ICEBERG_ERROR_BAD_REQUEST,
                StatusCode::BAD_REQUEST,
                format!("{update_field}.snapshot-id does not match snapshot-id"),
            ));
        }
    }
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
        .ok_or_else(|| S3Error::from(ApiError::invalid_request("remove update requires snapshot-id")))?;
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
        .ok_or_else(|| S3Error::from(ApiError::invalid_request(format!("{update_field} must be an array"))))?
        .iter()
        .map(|value| {
            value
                .as_i64()
                .ok_or_else(|| S3Error::from(ApiError::invalid_request(format!("{update_field} must contain integers"))))
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
        return Err(commit_requirement_failed(format!("cannot reassign {entity} uuid")));
    }
    object.insert(uuid_field.to_string(), serde_json::Value::String(uuid.to_string()));
    Ok(())
}

fn apply_add_view_version_update(
    metadata: &mut serde_json::Value,
    update: &serde_json::Value,
    last_added_schema_id: Option<i64>,
) -> S3Result<(i64, i64)> {
    let mut view_version = update
        .get("view-version")
        .cloned()
        .ok_or_else(|| s3_error!(InvalidRequest, "add-view-version requires view-version"))?;
    if !view_version.is_object() {
        return Err(s3_error!(InvalidRequest, "view-version must be a JSON object"));
    }
    let version_id = view_version
        .get("version-id")
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| s3_error!(InvalidRequest, "view-version version-id must be an integer"))?;
    if view_version.get("schema-id").and_then(serde_json::Value::as_i64) == Some(-1) {
        let schema_id = resolve_last_added_update_id(-1, last_added_schema_id, "add-view-version", "add-schema")?;
        view_version
            .as_object_mut()
            .ok_or_else(|| s3_error!(InvalidRequest, "view-version must be a JSON object"))?
            .insert("schema-id".to_string(), serde_json::Value::from(schema_id));
    }
    let timestamp_ms = view_version
        .get("timestamp-ms")
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| s3_error!(InvalidRequest, "view-version timestamp-ms must be an integer"))?;
    ensure_array_field(metadata, "versions")?.push(view_version);
    Ok((version_id, timestamp_ms))
}

fn apply_set_current_view_version_update(
    metadata: &mut serde_json::Value,
    update: &serde_json::Value,
    last_added_view_version_id: Option<i64>,
    added_view_version_timestamps: &BTreeMap<i64, i64>,
    commit_timestamp_ms: i64,
) -> S3Result<()> {
    let requested_id = update
        .get("view-version-id")
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| s3_error!(InvalidRequest, "set-current-view-version requires view-version-id"))?;
    let version_id =
        resolve_last_added_update_id(requested_id, last_added_view_version_id, "set-current-view-version", "add-view-version")?;
    if metadata.get("current-version-id").and_then(serde_json::Value::as_i64) == Some(version_id) {
        return Ok(());
    }
    let history_timestamp_ms = added_view_version_timestamps
        .get(&version_id)
        .copied()
        .unwrap_or(commit_timestamp_ms);
    metadata_object_mut(metadata)?.insert("current-version-id".to_string(), serde_json::Value::from(version_id));
    ensure_array_field(metadata, "version-log")?.push(serde_json::json!({
        "timestamp-ms": history_timestamp_ms,
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

fn apply_upgrade_view_format_version_update(update: &serde_json::Value) -> S3Result<()> {
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
    Ok(())
}

fn catalog_assigned_schema(update: &serde_json::Value, schema_id: i64) -> S3Result<serde_json::Value> {
    let mut schema = update
        .get("schema")
        .cloned()
        .ok_or_else(|| s3_error!(InvalidRequest, "add-schema requires schema"))?;
    let schema_object = schema
        .as_object_mut()
        .ok_or_else(|| s3_error!(InvalidRequest, "add-schema schema must be a JSON object"))?;
    schema_object.insert("schema-id".to_string(), serde_json::Value::from(schema_id));
    Ok(schema)
}

fn apply_add_table_schema_update(metadata: &mut serde_json::Value, update: &serde_json::Value, schema_id: i64) -> S3Result<()> {
    let schema = catalog_assigned_schema(update, schema_id)?;
    let last_column_id = max_field_id(&schema);
    ensure_array_field(metadata, "schemas")?.push(schema);
    let object = metadata_object_mut(metadata)?;
    let current_last = object
        .get("last-column-id")
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| s3_error!(InvalidRequest, "current table metadata is missing last-column-id"))?;
    object.insert("last-column-id".to_string(), serde_json::Value::from(current_last.max(last_column_id)));
    Ok(())
}

fn apply_add_view_schema_update(metadata: &mut serde_json::Value, update: &serde_json::Value, schema_id: i64) -> S3Result<()> {
    let schema = catalog_assigned_schema(update, schema_id)?;
    ensure_array_field(metadata, "schemas")?.push(schema);
    Ok(())
}

fn apply_set_current_schema_update(
    metadata: &mut serde_json::Value,
    update: &serde_json::Value,
    last_added_schema_id: Option<i64>,
) -> S3Result<()> {
    let requested_id = update
        .get("schema-id")
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| s3_error!(InvalidRequest, "set-current-schema requires schema-id"))?;
    let schema_id = resolve_last_added_update_id(requested_id, last_added_schema_id, "set-current-schema", "add-schema")?;
    metadata_object_mut(metadata)?.insert("current-schema-id".to_string(), serde_json::Value::from(schema_id));
    Ok(())
}

fn apply_add_spec_update(metadata: &mut serde_json::Value, update: &serde_json::Value, spec_id: i64) -> S3Result<()> {
    let mut spec = update
        .get("spec")
        .cloned()
        .ok_or_else(|| s3_error!(InvalidRequest, "add-spec requires spec"))?;
    let spec_object = spec
        .as_object_mut()
        .ok_or_else(|| s3_error!(InvalidRequest, "add-spec spec must be a JSON object"))?;
    spec_object.insert("spec-id".to_string(), serde_json::Value::from(spec_id));
    let current_last = metadata
        .get("last-partition-id")
        .and_then(serde_json::Value::as_i64)
        .unwrap_or(999);
    let existing_fields = existing_partition_field_ids(metadata)?;
    let last_partition_id = assign_partition_field_ids(&mut spec, current_last, &existing_fields)?;
    crate::table_catalog::validate_partition_spec_sources_against_current_schema(metadata, &spec).map_err(catalog_store_error)?;
    ensure_array_field(metadata, "partition-specs")?.push(spec);
    let object = metadata_object_mut(metadata)?;
    object.insert("last-partition-id".to_string(), serde_json::Value::from(last_partition_id));
    Ok(())
}

fn existing_partition_field_ids(metadata: &serde_json::Value) -> S3Result<BTreeMap<(i64, String), i64>> {
    let mut existing = BTreeMap::new();
    for spec in metadata
        .get("partition-specs")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
    {
        for field in spec.get("fields").and_then(serde_json::Value::as_array).into_iter().flatten() {
            let source_id = field
                .get("source-id")
                .and_then(serde_json::Value::as_i64)
                .ok_or_else(|| s3_error!(InvalidRequest, "partition source-id must be an integer"))?;
            let transform = field
                .get("transform")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| s3_error!(InvalidRequest, "partition transform must be a string"))?;
            let field_id = field
                .get("field-id")
                .and_then(serde_json::Value::as_i64)
                .ok_or_else(|| s3_error!(InvalidRequest, "partition field-id must be an integer"))?;
            match existing.insert((source_id, transform.to_string()), field_id) {
                Some(previous) if previous != field_id => {
                    return Err(s3_error!(InvalidRequest, "equivalent partition fields must reuse the same field-id"));
                }
                _ => {}
            }
        }
    }
    Ok(existing)
}

fn assign_partition_field_ids(
    spec: &mut serde_json::Value,
    current_last: i64,
    existing_fields: &BTreeMap<(i64, String), i64>,
) -> S3Result<i64> {
    let fields = spec
        .get_mut("fields")
        .and_then(serde_json::Value::as_array_mut)
        .ok_or_else(|| s3_error!(InvalidRequest, "partition spec fields must be an array"))?;
    let mut assigned_ids = BTreeSet::new();
    let mut last_partition_id = current_last;
    for field in fields.iter() {
        let field = field
            .as_object()
            .ok_or_else(|| s3_error!(InvalidRequest, "partition spec fields must be JSON objects"))?;
        let Some(field_id) = field.get("field-id") else {
            continue;
        };
        let field_id = field_id
            .as_i64()
            .ok_or_else(|| s3_error!(InvalidRequest, "partition field-id must be an integer"))?;
        if i32::try_from(field_id).is_err() || !assigned_ids.insert(field_id) {
            return Err(s3_error!(InvalidRequest, "partition field-id must be a unique signed 32-bit integer"));
        }
        let source_id = field
            .get("source-id")
            .and_then(serde_json::Value::as_i64)
            .ok_or_else(|| s3_error!(InvalidRequest, "partition source-id must be an integer"))?;
        let transform = field
            .get("transform")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| s3_error!(InvalidRequest, "partition transform must be a string"))?;
        if existing_fields
            .get(&(source_id, transform.to_string()))
            .is_some_and(|existing_id| *existing_id != field_id)
        {
            return Err(s3_error!(InvalidRequest, "equivalent partition fields must reuse the same field-id"));
        }
        last_partition_id = last_partition_id.max(field_id);
    }
    for field in fields.iter_mut().filter(|field| field.get("field-id").is_none()) {
        let source_id = field
            .get("source-id")
            .and_then(serde_json::Value::as_i64)
            .ok_or_else(|| s3_error!(InvalidRequest, "partition source-id must be an integer"))?;
        let transform = field
            .get("transform")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| s3_error!(InvalidRequest, "partition transform must be a string"))?;
        let field_id = match existing_fields.get(&(source_id, transform.to_string())) {
            Some(field_id) => *field_id,
            None => {
                last_partition_id = last_partition_id
                    .checked_add(1)
                    .filter(|field_id| i32::try_from(*field_id).is_ok())
                    .ok_or_else(|| s3_error!(InvalidRequest, "partition field-id exceeds the signed 32-bit range"))?;
                last_partition_id
            }
        };
        if !assigned_ids.insert(field_id) {
            return Err(s3_error!(InvalidRequest, "partition field-id must be unique within a partition spec"));
        }
        field
            .as_object_mut()
            .ok_or_else(|| s3_error!(InvalidRequest, "partition spec fields must be JSON objects"))?
            .insert("field-id".to_string(), serde_json::Value::from(field_id));
    }
    Ok(last_partition_id)
}

fn apply_set_default_spec_update(
    metadata: &mut serde_json::Value,
    update: &serde_json::Value,
    last_added_spec_id: Option<i64>,
) -> S3Result<()> {
    let requested_id = update
        .get("spec-id")
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| s3_error!(InvalidRequest, "set-default-spec requires spec-id"))?;
    let spec_id = resolve_last_added_update_id(requested_id, last_added_spec_id, "set-default-spec", "add-spec")?;
    metadata_object_mut(metadata)?.insert("default-spec-id".to_string(), serde_json::Value::from(spec_id));
    Ok(())
}

fn apply_add_sort_order_update(
    metadata: &mut serde_json::Value,
    update: &serde_json::Value,
    sort_order_id: i64,
) -> S3Result<i64> {
    let mut sort_order = update
        .get("sort-order")
        .cloned()
        .ok_or_else(|| s3_error!(InvalidRequest, "add-sort-order requires sort-order"))?;
    let sort_order_object = sort_order
        .as_object_mut()
        .ok_or_else(|| s3_error!(InvalidRequest, "add-sort-order sort-order must be a JSON object"))?;
    let fields_are_empty = sort_order_object
        .get("fields")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| s3_error!(InvalidRequest, "sort-order fields must be an array"))?
        .is_empty();
    let assigned_id = if fields_are_empty { 0 } else { sort_order_id };
    sort_order_object.insert("order-id".to_string(), serde_json::Value::from(assigned_id));
    crate::table_catalog::validate_sort_order_sources_against_current_schema(metadata, &sort_order)
        .map_err(catalog_store_error)?;
    let sort_orders = ensure_array_field(metadata, "sort-orders")?;
    if assigned_id == 0 {
        sort_orders.retain(|order| order.get("order-id").and_then(serde_json::Value::as_i64) != Some(0));
    }
    sort_orders.push(sort_order);
    Ok(assigned_id)
}

fn apply_set_default_sort_order_update(
    metadata: &mut serde_json::Value,
    update: &serde_json::Value,
    last_added_sort_order_id: Option<i64>,
) -> S3Result<()> {
    let requested_id = update
        .get("sort-order-id")
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| s3_error!(InvalidRequest, "set-default-sort-order requires sort-order-id"))?;
    let sort_order_id =
        resolve_last_added_update_id(requested_id, last_added_sort_order_id, "set-default-sort-order", "add-sort-order")?;
    metadata_object_mut(metadata)?.insert("default-sort-order-id".to_string(), serde_json::Value::from(sort_order_id));
    Ok(())
}

fn apply_add_snapshot_update(metadata: &mut serde_json::Value, update: &serde_json::Value) -> S3Result<i64> {
    let snapshot = update
        .get("snapshot")
        .cloned()
        .ok_or_else(|| s3_error!(InvalidRequest, "add-snapshot requires snapshot"))?;
    let format_version = metadata
        .get("format-version")
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| s3_error!(InvalidRequest, "current table metadata is missing format-version"))?;
    if format_version == 2 && snapshot.get("manifests").is_some() {
        return Err(s3_error!(InvalidRequest, "Iceberg v2 snapshots require manifest-list"));
    }
    let snapshot_id = snapshot
        .get("snapshot-id")
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| s3_error!(InvalidRequest, "snapshot-id must be an integer"))?;
    let sequence_number = snapshot_sequence_number(&snapshot, format_version)?;
    snapshot
        .get("timestamp-ms")
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| s3_error!(InvalidRequest, "snapshot timestamp-ms must be an integer"))?;
    validate_added_snapshot(metadata, &snapshot, snapshot_id, sequence_number, format_version)?;
    ensure_array_field(metadata, "snapshots")?.push(snapshot);
    if format_version > 1 {
        metadata_object_mut(metadata)?.insert("last-sequence-number".to_string(), serde_json::Value::from(sequence_number));
    }
    Ok(snapshot_id)
}

fn snapshot_sequence_number(snapshot: &serde_json::Value, format_version: i64) -> S3Result<i64> {
    let sequence_number = match snapshot.get("sequence-number") {
        Some(sequence_number) => sequence_number
            .as_i64()
            .ok_or_else(|| s3_error!(InvalidRequest, "snapshot sequence-number must be an integer")),
        None if format_version == 1 => Ok(0),
        None => Err(s3_error!(InvalidRequest, "Iceberg v2 snapshot sequence-number is required")),
    }?;
    if format_version == 1 && sequence_number != 0 {
        return Err(s3_error!(InvalidRequest, "Iceberg v1 snapshot sequence-number must be zero"));
    }
    Ok(sequence_number)
}

fn snapshot_parent_id(snapshot: &serde_json::Value) -> S3Result<Option<i64>> {
    snapshot
        .get("parent-snapshot-id")
        .map(|parent_snapshot_id| {
            parent_snapshot_id
                .as_i64()
                .ok_or_else(|| s3_error!(InvalidRequest, "snapshot parent-snapshot-id must be an integer"))
        })
        .transpose()
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
        return Err(commit_requirement_failed("snapshot id already exists"));
    }

    let parent_snapshot_id = snapshot_parent_id(snapshot)?;
    if let Some(parent_snapshot_id) = parent_snapshot_id
        && !metadata
            .get("snapshots")
            .and_then(serde_json::Value::as_array)
            .is_some_and(|snapshots| {
                snapshots
                    .iter()
                    .any(|snapshot| snapshot.get("snapshot-id").and_then(serde_json::Value::as_i64) == Some(parent_snapshot_id))
            })
    {
        return Err(commit_requirement_failed("snapshot parent does not exist"));
    }

    if format_version > 1 {
        let current_sequence_number = metadata
            .get("last-sequence-number")
            .and_then(serde_json::Value::as_i64)
            .ok_or_else(|| s3_error!(InvalidRequest, "current Iceberg v2 metadata is missing last-sequence-number"))?;
        if sequence_number <= current_sequence_number {
            return Err(commit_requirement_failed("snapshot sequence number must advance"));
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
        return Err(s3_error!(InvalidRequest, "unsupported snapshot operation: {operation}"));
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
    entry: &crate::table_catalog::TableEntry,
    current_metadata: &serde_json::Value,
    updates: &[serde_json::Value],
) -> S3Result<()>
where
    B: crate::table_catalog::TableCatalogObjectBackend,
{
    let mut snapshot_state = current_metadata.clone();
    for update in updates {
        match update.get("action").and_then(serde_json::Value::as_str) {
            Some("add-snapshot") => {
                let snapshot = update
                    .get("snapshot")
                    .ok_or_else(|| s3_error!(InvalidRequest, "add-snapshot requires snapshot"))?;
                validate_snapshot_file_conflicts(metadata_backend, bucket, entry, &snapshot_state, snapshot).await?;
                apply_add_snapshot_update(&mut snapshot_state, update)?;
            }
            Some("remove-snapshots") => apply_remove_snapshots_update(&mut snapshot_state, update)?,
            _ => {}
        }
    }
    Ok(())
}

async fn validate_snapshot_file_conflicts<B>(
    metadata_backend: &B,
    bucket: &str,
    entry: &crate::table_catalog::TableEntry,
    snapshot_state: &serde_json::Value,
    snapshot: &serde_json::Value,
) -> S3Result<()>
where
    B: crate::table_catalog::TableCatalogObjectBackend,
{
    let snapshot_id = snapshot
        .get("snapshot-id")
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| s3_error!(InvalidRequest, "snapshot-id must be an integer"))?;
    let format_version = snapshot_state
        .get("format-version")
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| s3_error!(InvalidRequest, "current table metadata is missing format-version"))?;
    let sequence_number = snapshot_sequence_number(snapshot, format_version)?;
    let operation = snapshot
        .get("summary")
        .and_then(|summary| summary.get("operation"))
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| s3_error!(InvalidRequest, "snapshot summary.operation is required"))?;
    let parent_snapshot_id = snapshot_parent_id(snapshot)?;
    let parent_live_files = load_snapshot_live_files(metadata_backend, bucket, entry, snapshot_state, parent_snapshot_id).await?;
    let changes = load_snapshot_file_changes(
        metadata_backend,
        bucket,
        entry,
        snapshot,
        SnapshotChangeContext {
            current_live_files: &parent_live_files,
            snapshot_id,
            sequence_number,
        },
    )
    .await?;

    for location in changes.added_data_files.iter().chain(changes.added_delete_files.iter()) {
        if parent_live_files.contains(location) {
            return Err(commit_requirement_failed(
                "commit requirement failed: added file already exists in parent snapshot",
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
                    return Err(commit_requirement_failed(
                        "commit requirement failed: deleted file is not in the parent snapshot",
                    ));
                }
            }
        }
        _ => return Err(s3_error!(InvalidRequest, "unsupported snapshot operation: {operation}")),
    }

    Ok(())
}

async fn load_snapshot_live_files<B>(
    metadata_backend: &B,
    bucket: &str,
    entry: &crate::table_catalog::TableEntry,
    current_metadata: &serde_json::Value,
    snapshot_id: Option<i64>,
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
        .ok_or_else(|| commit_requirement_failed("commit requirement failed: parent snapshot no longer exists"))?;

    let mut live_files = SnapshotLiveFiles::default();
    for manifest in read_snapshot_manifest_references(metadata_backend, bucket, entry, snapshot).await? {
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
    entry: &crate::table_catalog::TableEntry,
    snapshot: &serde_json::Value,
    context: SnapshotChangeContext<'_>,
) -> S3Result<SnapshotFileChanges>
where
    B: crate::table_catalog::TableCatalogObjectBackend,
{
    let mut changes = SnapshotFileChanges::default();
    for manifest in read_snapshot_manifest_references(metadata_backend, bucket, entry, snapshot).await? {
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
    entry: &crate::table_catalog::TableEntry,
    snapshot: &serde_json::Value,
) -> S3Result<Vec<SnapshotManifestReferences>>
where
    B: crate::table_catalog::TableCatalogObjectBackend,
{
    let manifest_locations = snapshot_manifest_locations(metadata_backend, bucket, entry, snapshot).await?;
    let mut manifests = Vec::new();
    for manifest_location in manifest_locations {
        let manifest_key = table_commit_object_key(
            bucket,
            entry,
            &manifest_location.manifest_path,
            crate::table_catalog::TableMetadataMaintenanceObjectKind::ManifestFile,
        )?;
        let manifest_object = metadata_backend
            .read_object_limited(bucket, &manifest_key, crate::table_catalog::TABLE_MANIFEST_AVRO_MAX_SIZE)
            .await
            .map_err(catalog_store_error)?
            .ok_or_else(|| s3_error!(InvalidRequest, "snapshot manifest object is missing"))?;
        let file_references = crate::table_catalog::decode_manifest_avro_async(manifest_object.data)
            .await
            .map_err(catalog_store_error)?
            .references;
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
            validate_manifest_data_file_reference(metadata_backend, bucket, entry, &reference).await?;
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
    entry: &crate::table_catalog::TableEntry,
    snapshot: &serde_json::Value,
) -> S3Result<Vec<SnapshotManifestLocation>>
where
    B: crate::table_catalog::TableCatalogObjectBackend,
{
    if let Some(manifest_list_location) = snapshot.get("manifest-list").and_then(serde_json::Value::as_str) {
        let manifest_list_key = table_commit_object_key(
            bucket,
            entry,
            manifest_list_location,
            crate::table_catalog::TableMetadataMaintenanceObjectKind::ManifestList,
        )?;
        let manifest_list_object = metadata_backend
            .read_object_limited(bucket, &manifest_list_key, crate::table_catalog::TABLE_MANIFEST_AVRO_MAX_SIZE)
            .await
            .map_err(catalog_store_error)?
            .ok_or_else(|| s3_error!(InvalidRequest, "snapshot manifest-list object is missing"))?;
        let references = crate::table_catalog::decode_manifest_list_avro_async(manifest_list_object.data)
            .await
            .map_err(catalog_store_error)?
            .references;
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
    entry: &crate::table_catalog::TableEntry,
    reference: &crate::table_catalog::ManifestDataFileReference,
) -> S3Result<()>
where
    B: crate::table_catalog::TableCatalogObjectBackend,
{
    table_commit_object_key(bucket, entry, &reference.location, reference.object_kind.clone())?;
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
    entry: &crate::table_catalog::TableEntry,
    location: &str,
    expected_kind: crate::table_catalog::TableMetadataMaintenanceObjectKind,
) -> S3Result<String> {
    let object_key = crate::table_catalog::table_catalog_object_key_from_location(bucket, location)
        .ok_or_else(|| s3_error!(InvalidRequest, "snapshot object location is invalid"))?;
    let warehouse_object_prefix = crate::table_catalog::table_warehouse_object_prefix(entry).map_err(catalog_store_error)?;
    let object_kind =
        crate::table_catalog::table_maintenance_object_kind_for_entry(entry, Some(&warehouse_object_prefix), &object_key)
            .ok_or_else(|| s3_error!(InvalidRequest, "snapshot object is outside the table warehouse"))?;
    if !crate::table_catalog::table_maintenance_object_kind_matches_reference(&object_kind, &expected_kind) {
        return Err(s3_error!(InvalidRequest, "snapshot object kind does not match manifest metadata"));
    }
    Ok(object_key)
}

fn apply_set_snapshot_ref_update(
    metadata: &mut serde_json::Value,
    update: &serde_json::Value,
    added_snapshot_ids: &BTreeSet<i64>,
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
    let reference = update
        .as_object()
        .ok_or_else(|| s3_error!(InvalidRequest, "set-snapshot-ref must be a JSON object"))?
        .iter()
        .filter(|(key, _)| key.as_str() != "action" && key.as_str() != "ref-name")
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<serde_json::Map<_, _>>();
    if !metadata
        .get("snapshots")
        .and_then(serde_json::Value::as_array)
        .is_some_and(|snapshots| {
            snapshots
                .iter()
                .any(|snapshot| snapshot.get("snapshot-id").and_then(serde_json::Value::as_i64) == Some(snapshot_id))
        })
    {
        return Err(s3_error!(InvalidRequest, "set-snapshot-ref targets an unknown snapshot"));
    }
    let next_reference = serde_json::Value::Object(reference);
    let unchanged = metadata
        .get("refs")
        .and_then(serde_json::Value::as_object)
        .and_then(|refs| refs.get(ref_name))
        == Some(&next_reference);
    ensure_object_field(metadata, "refs")?.insert(ref_name.to_string(), next_reference);
    if ref_name == "main" {
        metadata_object_mut(metadata)?.insert("current-snapshot-id".to_string(), serde_json::Value::from(snapshot_id));
        if !unchanged {
            let timestamp_ms = if added_snapshot_ids.contains(&snapshot_id) {
                metadata
                    .get("snapshots")
                    .and_then(serde_json::Value::as_array)
                    .and_then(|snapshots| {
                        snapshots
                            .iter()
                            .find(|snapshot| snapshot.get("snapshot-id").and_then(serde_json::Value::as_i64) == Some(snapshot_id))
                    })
                    .and_then(|snapshot| snapshot.get("timestamp-ms"))
                    .and_then(serde_json::Value::as_i64)
                    .ok_or_else(|| s3_error!(InvalidRequest, "snapshot timestamp-ms must be an integer"))?
            } else {
                commit_timestamp_ms
            };
            ensure_array_field(metadata, "snapshot-log")?.push(serde_json::json!({
                "timestamp-ms": timestamp_ms,
                "snapshot-id": snapshot_id
            }));
        }
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
                .ok_or_else(|| s3_error!(InvalidRequest, "snapshot-ids must contain integers"))
        })
        .collect::<S3Result<BTreeSet<_>>>()?;
    let snapshots = ensure_array_field(metadata, "snapshots")?;
    let snapshot_count = snapshots.len();
    snapshots.retain(|snapshot| {
        snapshot
            .get("snapshot-id")
            .and_then(serde_json::Value::as_i64)
            .is_none_or(|snapshot_id| !ids.contains(&snapshot_id))
    });
    let removed_snapshot = snapshots.len() != snapshot_count;
    if removed_snapshot {
        let remaining_snapshot_ids = snapshots
            .iter()
            .filter_map(|snapshot| snapshot.get("snapshot-id").and_then(serde_json::Value::as_i64))
            .collect::<BTreeSet<_>>();
        let snapshot_log = ensure_array_field(metadata, "snapshot-log")?;
        let previous_log = std::mem::take(snapshot_log);
        for log in previous_log {
            let snapshot_id = log
                .get("snapshot-id")
                .and_then(serde_json::Value::as_i64)
                .ok_or_else(|| s3_error!(InvalidRequest, "snapshot-log snapshot-id must be an integer"))?;
            if remaining_snapshot_ids.contains(&snapshot_id) {
                snapshot_log.push(log);
            } else {
                snapshot_log.clear();
            }
        }
    }
    let dangling_refs = metadata
        .get("refs")
        .and_then(serde_json::Value::as_object)
        .into_iter()
        .flat_map(|refs| refs.iter())
        .filter_map(|(name, reference)| {
            reference
                .get("snapshot-id")
                .and_then(serde_json::Value::as_i64)
                .filter(|snapshot_id| ids.contains(snapshot_id))
                .map(|_| name.clone())
        })
        .collect::<Vec<_>>();
    let removed_main = dangling_refs.iter().any(|name| name == "main")
        || metadata
            .get("current-snapshot-id")
            .and_then(serde_json::Value::as_i64)
            .is_some_and(|snapshot_id| ids.contains(&snapshot_id));
    let refs = ensure_object_field(metadata, "refs")?;
    for name in dangling_refs {
        refs.remove(&name);
    }
    if removed_main {
        metadata_object_mut(metadata)?.insert("current-snapshot-id".to_string(), serde_json::Value::from(-1));
    }
    for field in ["statistics", "partition-statistics"] {
        if let Some(values) = metadata.get_mut(field).and_then(serde_json::Value::as_array_mut) {
            values.retain(|value| {
                value
                    .get("snapshot-id")
                    .and_then(serde_json::Value::as_i64)
                    .is_none_or(|snapshot_id| !ids.contains(&snapshot_id))
            });
        }
    }
    Ok(())
}

fn apply_remove_snapshot_ref_update(metadata: &mut serde_json::Value, update: &serde_json::Value) -> S3Result<()> {
    let ref_name = update
        .get("ref-name")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| s3_error!(InvalidRequest, "remove-snapshot-ref requires ref-name"))?;
    let removed = ensure_object_field(metadata, "refs")?.remove(ref_name).is_some();
    if removed && ref_name == "main" {
        metadata_object_mut(metadata)?.insert("current-snapshot-id".to_string(), serde_json::Value::from(-1));
    }
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
    let previous_metadata_timestamp = metadata
        .get("last-updated-ms")
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| s3_error!(InvalidRequest, "current table metadata is missing last-updated-ms"))?;
    ensure_array_field(metadata, "metadata-log")?.push(serde_json::json!({
        "timestamp-ms": previous_metadata_timestamp,
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

fn validate_commit_item_count(label: &str, count: usize, max_count: usize) -> S3Result<()> {
    if count > max_count {
        return Err(s3_error!(InvalidRequest, "{label} exceeds the maximum count of {max_count}"));
    }
    Ok(())
}

fn validate_rest_commit_item_counts(requirements: &[serde_json::Value], updates: &[serde_json::Value]) -> S3Result<()> {
    validate_commit_item_count("commit requirements", requirements.len(), TABLE_CATALOG_COMMIT_REQUIREMENT_MAX_COUNT)?;
    validate_commit_item_count("commit updates", updates.len(), TABLE_CATALOG_COMMIT_UPDATE_MAX_COUNT)
}

fn next_catalog_id_for_updates(
    metadata: &serde_json::Value,
    updates: &[serde_json::Value],
    action: &str,
    array_key: &str,
    id_key: &str,
) -> S3Result<Option<i64>> {
    updates
        .iter()
        .any(|update| update.get("action").and_then(serde_json::Value::as_str) == Some(action))
        .then(|| next_array_object_i64(metadata, array_key, id_key))
        .transpose()
}

fn take_catalog_assigned_id(next_id: &mut Option<i64>, label: &str) -> S3Result<i64> {
    let next_id = next_id
        .as_mut()
        .ok_or_else(|| s3_error!(InternalError, "catalog-assigned {label} state is missing"))?;
    let assigned_id = *next_id;
    *next_id = next_id
        .checked_add(1)
        .ok_or_else(|| s3_error!(InvalidRequest, "catalog-assigned {label} exceeds the signed 64-bit range"))?;
    Ok(assigned_id)
}

fn next_array_object_i64(metadata: &serde_json::Value, array_key: &str, id_key: &str) -> S3Result<i64> {
    last_array_object_i64(metadata, array_key, id_key)?
        .checked_add(1)
        .ok_or_else(|| s3_error!(InvalidRequest, "metadata field {array_key} {id_key} exceeds the signed 64-bit range"))
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

fn resolve_last_added_update_id(
    requested_id: i64,
    last_added_id: Option<i64>,
    update_action: &str,
    required_action: &str,
) -> S3Result<i64> {
    if requested_id != -1 {
        return Ok(requested_id);
    }
    last_added_id.ok_or_else(|| s3_error!(InvalidRequest, "{update_action} id -1 requires a preceding {required_action} update"))
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
    crate::table_catalog::validate_namespace_properties(&request.properties).map_err(catalog_store_error)?;
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

fn commit_requirement_failed(message: impl Into<String>) -> S3Error {
    iceberg_rest_error(ICEBERG_ERROR_COMMIT_FAILED, StatusCode::CONFLICT, message)
}

fn persisted_metadata_error(entity: &str) -> S3Error {
    iceberg_rest_error(
        ICEBERG_ERROR_REST,
        StatusCode::INTERNAL_SERVER_ERROR,
        format!("persisted {entity} metadata is invalid"),
    )
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
            iceberg_rest_error(ICEBERG_ERROR_REST, StatusCode::INTERNAL_SERVER_ERROR, message)
        }
    }
}

fn table_identifier_from_request(
    identifier: RestTableIdentifier,
) -> S3Result<(crate::table_catalog::Namespace, crate::table_catalog::IdentifierSegment)> {
    let namespace = namespace_from_segments(&identifier.namespace)?;
    let table = crate::table_catalog::IdentifierSegment::parse(identifier.name)
        .map_err(|err| s3_error!(InvalidRequest, "invalid table name: {}", err))?;
    Ok((namespace, table))
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

async fn list_namespaces_response<S>(
    store: &S,
    bucket: &str,
    parent: Option<&crate::table_catalog::Namespace>,
    uri: &http::Uri,
) -> S3Result<RestListNamespacesResponse>
where
    S: crate::table_catalog::TableCatalogStore + ?Sized,
{
    let parent_name = parent.map(crate::table_catalog::Namespace::public_name);
    let context = RestPageContext {
        resource: TABLE_CATALOG_NAMESPACE_RESOURCE_ROOT,
        warehouse: bucket,
        namespace: parent_name.as_deref(),
    };
    let pagination = rest_pagination_from_query(uri, context)?;
    let map_list_error = |err| match err {
        crate::table_catalog::TableCatalogStoreError::NotFound(message) => {
            iceberg_rest_error(ICEBERG_ERROR_NO_SUCH_NAMESPACE, StatusCode::NOT_FOUND, message)
        }
        err => catalog_store_error(err),
    };
    let page = match pagination.page_request() {
        Some((cursor, limit)) => store
            .list_namespace_children_page(bucket, parent_name.as_deref(), cursor, limit)
            .await
            .map_err(map_list_error)?,
        None => crate::table_catalog::TableCatalogListPage {
            entries: store
                .list_namespace_children(bucket, parent_name.as_deref())
                .await
                .map_err(map_list_error)?,
            next_cursor: None,
        },
    };
    let next_page_token = pagination.next_page_token(page.next_cursor)?;
    let namespaces = page
        .entries
        .into_iter()
        .map(|entry| {
            crate::table_catalog::Namespace::parse(&entry.namespace)
                .map(|namespace| namespace_segments(&namespace))
                .map_err(|err| {
                    iceberg_rest_error(
                        ICEBERG_ERROR_REST,
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("catalog namespace is invalid: {err}"),
                    )
                })
        })
        .collect::<S3Result<Vec<_>>>()?;
    Ok(RestListNamespacesResponse {
        namespaces,
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
        .map_err(|err| match err {
            crate::table_catalog::TableCatalogStoreError::NotFound(message) => {
                iceberg_rest_error(ICEBERG_ERROR_NO_SUCH_NAMESPACE, StatusCode::NOT_FOUND, message)
            }
            err => catalog_store_error(err),
        })
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
    validate_table_metadata_snapshot_graph(metadata_backend, bucket, &entry, None, &metadata).await?;
    store
        .register_table_with_publication(entry.clone(), metadata_backend)
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
    crate::table_catalog::TableCommitPublication::begin_table_bucket(metadata_backend, bucket)
        .await
        .map_err(catalog_store_error)?;
    if !crate::table_catalog::TableCommitPublication::holds_table_bucket(metadata_backend, bucket) {
        return Err(catalog_store_error(crate::table_catalog::TableCatalogStoreError::Internal(
            "table creation requires a table-bucket publication fence".to_string(),
        )));
    }
    let _publication_completion = crate::table_catalog::TableCommitPublicationCompletion::new(metadata_backend);
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
        .register_table_with_publication(entry.clone(), metadata_backend)
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
    crate::table_catalog::TableCommitPublication::begin_table_bucket(metadata_backend, bucket)
        .await
        .map_err(catalog_store_error)?;
    if !crate::table_catalog::TableCommitPublication::holds_table_bucket(metadata_backend, bucket) {
        return Err(catalog_store_error(crate::table_catalog::TableCatalogStoreError::Internal(
            "view creation requires a table-bucket publication fence".to_string(),
        )));
    }
    let _publication_completion = crate::table_catalog::TableCommitPublicationCompletion::new(metadata_backend);
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
        .create_view_with_publication(entry.clone(), metadata_backend)
        .await
        .map_err(catalog_store_already_exists_error)?;
    Ok(load_view_response_from_entry(entry, metadata))
}

async fn read_table_metadata_json(
    metadata_backend: &impl crate::table_catalog::TableCatalogObjectBackend,
    bucket: &str,
    metadata_location: &str,
) -> S3Result<serde_json::Value> {
    let Some(metadata) = crate::table_catalog::read_table_metadata_value(metadata_backend, bucket, metadata_location)
        .await
        .map_err(catalog_store_error)?
    else {
        return Err(s3_error!(InvalidRequest, "table metadata object not found: {metadata_location}"));
    };
    Ok(metadata)
}

async fn read_persisted_metadata_json(
    metadata_backend: &impl crate::table_catalog::TableCatalogObjectBackend,
    bucket: &str,
    metadata_location: &str,
    entity: &str,
) -> S3Result<serde_json::Value> {
    read_table_metadata_json(metadata_backend, bucket, metadata_location)
        .await
        .map_err(|_| persisted_metadata_error(entity))
}

async fn read_generated_table_metadata_json(
    metadata_backend: &impl crate::table_catalog::TableCatalogObjectBackend,
    bucket: &str,
    metadata_location: &str,
) -> S3Result<serde_json::Value> {
    let Some(metadata) = crate::table_catalog::read_table_metadata_value(metadata_backend, bucket, metadata_location)
        .await
        .map_err(generated_metadata_error)?
    else {
        return Err(iceberg_rest_error(
            ICEBERG_ERROR_REST,
            StatusCode::INTERNAL_SERVER_ERROR,
            "generated metadata object is missing",
        ));
    };
    Ok(metadata)
}

async fn validate_table_metadata_snapshot_graph<B>(
    metadata_backend: &B,
    bucket: &str,
    entry: &crate::table_catalog::TableEntry,
    current_metadata: Option<&serde_json::Value>,
    metadata: &serde_json::Value,
) -> S3Result<()>
where
    B: crate::table_catalog::TableCatalogObjectBackend,
{
    validate_table_metadata_snapshot_graph_result(metadata_backend, bucket, entry, current_metadata, metadata)
        .await
        .map_err(catalog_store_error)
}

async fn validate_table_metadata_snapshot_graph_result<B>(
    metadata_backend: &B,
    bucket: &str,
    entry: &crate::table_catalog::TableEntry,
    current_metadata: Option<&serde_json::Value>,
    metadata: &serde_json::Value,
) -> crate::table_catalog::TableCatalogStoreResult<()>
where
    B: crate::table_catalog::TableCatalogObjectBackend,
{
    let mut target_entry = entry.clone();
    target_entry.warehouse_location = crate::table_catalog::table_metadata_location(metadata)?.to_string();
    let context = crate::table_catalog::TableSnapshotGraphValidationContext::new(metadata_backend, bucket, &target_entry);
    crate::table_catalog::validate_table_snapshot_changes(&context, current_metadata, metadata).await
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
    let metadata = read_persisted_table_metadata_for_entry(metadata_backend, &entry, &entry.metadata_location, true).await?;
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
    let view_name =
        crate::table_catalog::IdentifierSegment::parse(view.to_string()).map_err(|_| persisted_metadata_error("view"))?;
    if !crate::table_catalog::is_valid_view_metadata_location(namespace, &view_name, &entry.metadata_location) {
        return Err(persisted_metadata_error("view"));
    }
    let metadata = read_persisted_metadata_json(metadata_backend, bucket, &entry.metadata_location, "view").await?;
    validate_persisted_view_metadata(&entry, &metadata)?;
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
    validate_rest_commit_item_counts(&request.requirements, &request.updates)?;
    validate_rest_commit_identifier(request.identifier.as_ref(), namespace, view)?;
    let Some(current) = store
        .load_view(bucket, &namespace.public_name(), view)
        .await
        .map_err(catalog_store_error)?
    else {
        return Err(iceberg_rest_error(ICEBERG_ERROR_NO_SUCH_VIEW, StatusCode::NOT_FOUND, "view not found"));
    };
    let view_name = crate::table_catalog::IdentifierSegment::parse(view.to_string())
        .map_err(|err| s3_error!(InvalidRequest, "invalid view name: {}", err))?;
    if !crate::table_catalog::is_valid_view_metadata_location(namespace, &view_name, &current.metadata_location) {
        return Err(persisted_metadata_error("view"));
    }
    let current_metadata = read_persisted_metadata_json(metadata_backend, bucket, &current.metadata_location, "view").await?;
    if request.new_metadata_location.is_some() {
        validate_persisted_view_metadata_identity(&current, &current_metadata)?;
    } else {
        validate_persisted_view_metadata(&current, &current_metadata)?;
    }
    validate_view_commit_requirements(&current_metadata, &request.requirements)?;
    let (next_metadata_location, next_metadata) = if let Some(new_metadata_location) = request.new_metadata_location {
        let new_metadata_location = table_metadata_location_for_catalog(bucket, &new_metadata_location)?;
        if !crate::table_catalog::is_valid_view_metadata_location(namespace, &view_name, &new_metadata_location) {
            return Err(s3_error!(InvalidRequest, "metadata location must be inside the view metadata directory"));
        }
        let target_metadata = read_table_metadata_json(metadata_backend, bucket, &new_metadata_location).await?;
        validate_supported_view_metadata(&target_metadata)?;
        validate_metadata_view_location_in_bucket(bucket, &target_metadata)?;
        validate_metadata_matches_current_view_metadata(&current_metadata, &target_metadata)?;
        (new_metadata_location, target_metadata)
    } else {
        let next_metadata = apply_view_commit_updates_at(current_metadata.clone(), &request.updates, current_time_millis())?;
        validate_metadata_view_location_in_bucket(bucket, &next_metadata)?;
        validate_metadata_matches_current_view_metadata(&current_metadata, &next_metadata)?;
        let (_, metadata_file_token) = standard_commit_ids(None);
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

    let expected_metadata_location = request
        .expected_metadata_location
        .as_deref()
        .map(|location| table_metadata_location_for_catalog(bucket, location))
        .transpose()?
        .unwrap_or_else(|| current.metadata_location.clone());
    let table_bucket_fence_required = metadata_table_location(&next_metadata)? != current.warehouse_location;

    let result = store
        .replace_view_with_publication(
            crate::table_catalog::ViewCommitRequest {
                table_bucket: bucket.to_string(),
                namespace: namespace.public_name(),
                view: view.to_string(),
                expected_version_token: request
                    .expected_version_token
                    .unwrap_or_else(|| current.version_token.clone()),
                expected_metadata_location,
                new_metadata_location: next_metadata_location,
            },
            table_bucket_fence_required,
            metadata_backend,
        )
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

async fn publish_table_commit<S>(
    store: &S,
    metadata_backend: &impl crate::table_catalog::TableCatalogObjectBackend,
    table_bucket_fence_required: bool,
    request: crate::table_catalog::TableCommitRequest,
) -> S3Result<crate::table_catalog::TableCommitResult>
where
    S: crate::table_catalog::TableCatalogStore + ?Sized,
{
    let _bucket_publication_completion = if table_bucket_fence_required {
        metadata_backend
            .begin_table_bucket_commit_publication(&request.table_bucket)
            .await
            .map_err(catalog_store_error)?;
        Some(crate::table_catalog::TableCommitPublicationCompletion::new(metadata_backend))
    } else {
        None
    };
    store
        .commit_table_with_publication(request, metadata_backend)
        .await
        .map_err(catalog_store_error)
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
    let metadata_location = table_metadata_location_for_catalog(bucket, &request.metadata_location)?;
    if !crate::table_catalog::is_valid_table_metadata_location_for_entry(&current, &metadata_location) {
        return Err(s3_error!(InvalidRequest, "metadata location must be inside the table metadata directory"));
    }
    let existing_commit = table_commit_for_retry_ids(
        store,
        bucket,
        &current.table_id,
        request.commit_id.as_deref(),
        request.idempotency_key.as_deref(),
    )
    .await?;
    let previous_metadata_location = existing_commit
        .as_ref()
        .map_or_else(|| current.metadata_location.clone(), |commit| commit.previous_metadata_location.clone());
    let require_current_warehouse = existing_commit.is_none();
    let previous_metadata = read_persisted_table_metadata_for_entry(
        metadata_backend,
        &current,
        &previous_metadata_location,
        require_current_warehouse,
    )
    .await?;
    let target_metadata = read_table_metadata_json(metadata_backend, bucket, &metadata_location).await?;
    validate_metadata_table_location_in_bucket(bucket, &target_metadata)?;
    let table_bucket_fence_required = table_warehouse_location_changes(&current, &target_metadata)?;
    validate_metadata_matches_current_metadata(&previous_metadata, &target_metadata)?;
    validate_table_metadata_snapshot_graph(metadata_backend, bucket, &current, Some(&previous_metadata), &target_metadata)
        .await?;
    let requirements = match existing_commit.as_ref() {
        Some(existing_commit) => replay_commit_requirements(existing_commit, &[], &target_metadata)?,
        None => Vec::new(),
    };
    let commit_id = existing_commit
        .as_ref()
        .map(|commit| commit.commit_id.clone())
        .or(request.commit_id)
        .or_else(|| request.idempotency_key.clone())
        .unwrap_or_else(|| Uuid::new_v4().to_string());
    let commit_request = crate::table_catalog::TableCommitRequest {
        table_bucket: bucket.to_string(),
        namespace: namespace.public_name(),
        table: table.to_string(),
        commit_id,
        idempotency_key: request.idempotency_key,
        operation: "update-metadata-location".to_string(),
        expected_version_token: request.version_token,
        expected_metadata_location: previous_metadata_location,
        new_metadata_location: metadata_location,
        requirements,
        writer: Some("rustfs-metadata-location-api".to_string()),
    };
    if let Some(existing_commit) = existing_commit.as_ref()
        && !crate::table_catalog::commit_log_matches_request(existing_commit, &commit_request, &current.table_id)
    {
        return Err(iceberg_rest_error(
            ICEBERG_ERROR_COMMIT_FAILED,
            StatusCode::CONFLICT,
            "commit retry does not match the original request",
        ));
    }
    let result = publish_table_commit(store, metadata_backend, table_bucket_fence_required, commit_request).await?;
    Ok(table_metadata_location_response_from_entry(result.table))
}

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
    validate_rest_commit_item_counts(&request.requirements, &request.updates)?;
    validate_rest_commit_identifier(request.identifier.as_ref(), namespace, table)?;
    if request.new_metadata_location.is_none() {
        return standard_commit_table_response(store, metadata_backend, bucket, namespace, table, request).await;
    }

    let Some(current) = store
        .load_table(bucket, &namespace.public_name(), table)
        .await
        .map_err(catalog_store_error)?
    else {
        return Err(iceberg_rest_error(ICEBERG_ERROR_NO_SUCH_TABLE, StatusCode::NOT_FOUND, "table not found"));
    };
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
    let current_metadata =
        read_persisted_table_metadata_for_entry(metadata_backend, &current, &current.metadata_location, true).await?;
    let target_metadata = read_table_metadata_json(metadata_backend, bucket, &request.new_metadata_location).await?;
    validate_metadata_table_location_in_bucket(bucket, &target_metadata)?;
    let table_bucket_fence_required = table_warehouse_location_changes(&current, &target_metadata)?;
    if let Some(existing_commit) = existing_commit {
        request.requirements = replay_commit_requirements(&existing_commit, &client_requirements, &target_metadata)?;
        if !crate::table_catalog::commit_log_matches_request(&existing_commit, &request, &current.table_id) {
            return Err(iceberg_rest_error(
                ICEBERG_ERROR_COMMIT_FAILED,
                StatusCode::CONFLICT,
                "commit retry does not match the original request",
            ));
        }
        let previous_metadata = read_persisted_table_metadata_for_entry(
            metadata_backend,
            &current,
            &existing_commit.previous_metadata_location,
            false,
        )
        .await?;
        validate_table_commit_requirements(&previous_metadata, &client_requirements)?;
        validate_metadata_matches_current_metadata(&previous_metadata, &target_metadata)?;
        validate_table_metadata_snapshot_graph(metadata_backend, bucket, &current, Some(&previous_metadata), &target_metadata)
            .await?;
        let committed_metadata_location = request.new_metadata_location.clone();
        let result = publish_table_commit(store, metadata_backend, table_bucket_fence_required, request).await?;
        return commit_table_replay_response(metadata_backend, bucket, result, &committed_metadata_location, target_metadata)
            .await;
    }
    validate_metadata_matches_current_metadata(&current_metadata, &target_metadata)?;
    validate_table_commit_requirements(&current_metadata, &client_requirements)?;
    validate_table_metadata_snapshot_graph(metadata_backend, bucket, &current, Some(&current_metadata), &target_metadata).await?;
    let result = publish_table_commit(store, metadata_backend, table_bucket_fence_required, request).await?;
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
    if let Some(response) =
        replay_standard_table_commit(store, metadata_backend, bucket, namespace, table, &current, &request).await?
    {
        return Ok(response);
    }
    let current_metadata =
        read_persisted_table_metadata_for_entry(metadata_backend, &current, &current.metadata_location, true).await?;
    validate_table_commit_requirements(&current_metadata, &request.requirements)?;
    let expected_metadata = current_metadata.clone();
    let previous_metadata_location = table_metadata_location_for_client(bucket, &current.metadata_location);
    let commit_timestamp_ms = current_time_millis();
    let mut next_metadata =
        apply_table_commit_updates_at(current_metadata, &request.updates, &previous_metadata_location, commit_timestamp_ms)?;
    validate_metadata_table_location_in_bucket(bucket, &next_metadata)?;
    validate_metadata_identity_matches_current_metadata(&expected_metadata, &next_metadata)?;
    validate_table_metadata_snapshot_graph_result(metadata_backend, bucket, &current, Some(&expected_metadata), &next_metadata)
        .await
        .map_err(|err| match err {
            crate::table_catalog::TableCatalogStoreError::Invalid(message) => s3_error!(InvalidRequest, "{}", message),
            err => catalog_store_error(err),
        })?;
    validate_table_snapshot_commit_conflicts(metadata_backend, bucket, &current, &expected_metadata, &request.updates).await?;
    validate_metadata_matches_current_metadata(&expected_metadata, &next_metadata)?;
    let (commit_id, metadata_file_token) = standard_commit_ids(request.commit_id.or_else(|| request.idempotency_key.clone()));
    let next_generation = current.generation.saturating_add(1);
    let mut next_metadata_location = crate::table_catalog::table_metadata_file_path_for_entry(
        &current,
        &next_metadata_file_name(next_generation, &metadata_file_token),
    )
    .map_err(catalog_store_error)?;
    let mut using_table_scoped_location = false;
    loop {
        let metadata_data = serde_json::to_vec(&next_metadata)
            .map_err(|err| s3_error!(InternalError, "failed to serialize table metadata update: {}", err))?;
        match metadata_backend
            .put_object(
                bucket,
                &next_metadata_location,
                metadata_data,
                crate::table_catalog::TableCatalogPutPrecondition::IfAbsent,
            )
            .await
        {
            Ok(()) => {
                let persisted_metadata =
                    read_generated_table_metadata_json(metadata_backend, bucket, &next_metadata_location).await?;
                if persisted_metadata != next_metadata {
                    return Err(iceberg_rest_error(
                        ICEBERG_ERROR_COMMIT_FAILED,
                        StatusCode::CONFLICT,
                        "generated metadata changed before catalog publication",
                    ));
                }
                break;
            }
            Err(crate::table_catalog::TableCatalogStoreError::Conflict(_)) => {
                let persisted_metadata =
                    read_generated_table_metadata_json(metadata_backend, bucket, &next_metadata_location).await?;
                match compare_generated_metadata_with_standard_commit(
                    &persisted_metadata,
                    &expected_metadata,
                    &request.updates,
                    &previous_metadata_location,
                )? {
                    GeneratedMetadataComparison::MatchingCommit => {
                        next_metadata = persisted_metadata;
                        break;
                    }
                    GeneratedMetadataComparison::DifferentTable if using_table_scoped_location => {
                        return Err(iceberg_rest_error(
                            ICEBERG_ERROR_REST,
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "table-scoped metadata location contains another table",
                        ));
                    }
                    GeneratedMetadataComparison::DifferentTable => {
                        next_metadata_location = crate::table_catalog::table_metadata_file_path_for_entry(
                            &current,
                            &table_scoped_metadata_file_name(next_generation, &current.table_id, &metadata_file_token),
                        )
                        .map_err(catalog_store_error)?;
                        using_table_scoped_location = true;
                    }
                }
            }
            Err(err) => return Err(catalog_store_error(err)),
        }
    }
    let table_bucket_fence_required = table_warehouse_location_changes(&current, &next_metadata)?;

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
    let result = publish_table_commit(store, metadata_backend, table_bucket_fence_required, commit_request).await?;
    Ok(commit_table_response_from_result(result, next_metadata))
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
    table_commit_for_retry_ids(store, bucket, table_id, request.commit_id.as_deref(), request.idempotency_key.as_deref()).await
}

async fn table_commit_for_retry_ids<S>(
    store: &S,
    bucket: &str,
    table_id: &str,
    commit_id: Option<&str>,
    idempotency_key: Option<&str>,
) -> S3Result<Option<crate::table_catalog::CommitLogEntry>>
where
    S: crate::table_catalog::TableCatalogStore + ?Sized,
{
    let by_commit_id = match commit_id {
        Some(commit_id) => store
            .get_commit_by_id(bucket, table_id, commit_id)
            .await
            .map_err(catalog_store_error)?,
        None => None,
    };
    let by_idempotency_key = match idempotency_key {
        Some(idempotency_key) => store
            .get_commit_by_idempotency_key(bucket, table_id, idempotency_key)
            .await
            .map_err(catalog_store_error)?,
        None => None,
    };
    if let (Some(by_commit_id), Some(by_idempotency_key)) = (&by_commit_id, &by_idempotency_key)
        && !crate::table_catalog::commit_logs_share_recovery_payload(by_commit_id, by_idempotency_key)
    {
        return Err(iceberg_rest_error(
            ICEBERG_ERROR_COMMIT_FAILED,
            StatusCode::CONFLICT,
            "commit id and idempotency key identify different commit payloads",
        ));
    }
    Ok(by_commit_id.or(by_idempotency_key))
}

fn replay_commit_requirements(
    commit: &crate::table_catalog::CommitLogEntry,
    client_requirements: &[serde_json::Value],
    target_metadata: &serde_json::Value,
) -> S3Result<Vec<serde_json::Value>> {
    if commit.requirements == client_requirements {
        return Ok(client_requirements.to_vec());
    }
    let mut requirements = client_requirements.to_vec();
    requirements.push(metadata_digest_requirement(target_metadata)?);
    if commit.requirements != requirements {
        return Err(iceberg_rest_error(
            ICEBERG_ERROR_COMMIT_FAILED,
            StatusCode::CONFLICT,
            "commit retry requirements do not match the original commit",
        ));
    }
    Ok(requirements)
}

async fn commit_table_replay_response(
    metadata_backend: &impl crate::table_catalog::TableCatalogObjectBackend,
    bucket: &str,
    result: crate::table_catalog::TableCommitResult,
    committed_metadata_location: &str,
    committed_metadata: serde_json::Value,
) -> S3Result<RestCommitTableResponse> {
    let metadata = if result.table.metadata_location == committed_metadata_location {
        committed_metadata
    } else {
        if !crate::table_catalog::is_valid_table_metadata_location_for_entry(&result.table, &result.table.metadata_location) {
            return Err(iceberg_rest_error(
                ICEBERG_ERROR_REST,
                StatusCode::INTERNAL_SERVER_ERROR,
                "persisted table metadata location is outside the protected table metadata directory",
            ));
        }
        read_table_metadata_json(metadata_backend, bucket, &result.table.metadata_location).await?
    };
    validate_persisted_table_metadata(&result.table, &metadata, true)?;
    Ok(commit_table_response_from_result(result, metadata))
}

async fn replay_standard_table_commit<S>(
    store: &S,
    metadata_backend: &impl crate::table_catalog::TableCatalogObjectBackend,
    bucket: &str,
    namespace: &crate::table_catalog::Namespace,
    table: &str,
    current: &crate::table_catalog::TableEntry,
    request: &RestCommitTableRequest,
) -> S3Result<Option<RestCommitTableResponse>>
where
    S: crate::table_catalog::TableCatalogStore + ?Sized,
{
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

    let previous_metadata =
        read_persisted_table_metadata_for_entry(metadata_backend, current, &commit.previous_metadata_location, false).await?;
    validate_table_commit_requirements(&previous_metadata, &request.requirements)?;
    let target_metadata = read_table_metadata_json(metadata_backend, bucket, &commit.new_metadata_location).await?;
    validate_metadata_table_location_in_bucket(bucket, &target_metadata)?;
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
    if crate::table_catalog::table_matches_staged_base(current, &commit) {
        validate_table_metadata_snapshot_graph_result(
            metadata_backend,
            bucket,
            current,
            Some(&previous_metadata),
            &target_metadata,
        )
        .await
        .map_err(|err| match err {
            crate::table_catalog::TableCatalogStoreError::Invalid(message) => s3_error!(InvalidRequest, "{}", message),
            err => catalog_store_error(err),
        })?;
        validate_table_snapshot_commit_conflicts(metadata_backend, bucket, current, &previous_metadata, &request.updates).await?;
    }
    let requirements = replay_commit_requirements(&commit, &request.requirements, &target_metadata)?;
    let committed_metadata_location = commit.new_metadata_location.clone();
    let table_bucket_fence_required = table_warehouse_location_changes(current, &target_metadata)?;
    let result = publish_table_commit(
        store,
        metadata_backend,
        table_bucket_fence_required,
        crate::table_catalog::TableCommitRequest {
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
        },
    )
    .await?;
    Ok(Some(
        commit_table_replay_response(metadata_backend, bucket, result, &committed_metadata_location, target_metadata).await?,
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
    B: crate::table_catalog::TableCatalogObjectBackend + Clone,
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
        Some(config) if commit_compaction => {
            let publication_backend = TableCommitObjectBackend::preauthorized(metadata_backend.clone());
            Some(
                store
                    .commit_table_compaction_with_publication(
                        &publication_backend,
                        &publication_backend,
                        bucket,
                        &namespace.public_name(),
                        table,
                        config,
                    )
                    .await
                    .map_err(catalog_store_error)?,
            )
        }
        Some(config) => Some(
            store
                .plan_table_compaction(bucket, &namespace.public_name(), table, config)
                .await
                .map_err(catalog_store_error)?,
        ),
        None => None,
    };
    let (snapshot_expiration_plan, snapshot_publication_backend) = match snapshot_expiration_request {
        Some(config) if commit_snapshot_expiration => {
            let publication_backend = TableCommitObjectBackend::preauthorized(metadata_backend.clone());
            let plan = store
                .plan_table_snapshot_expiration_with_backend(
                    &publication_backend,
                    bucket,
                    &namespace.public_name(),
                    table,
                    config,
                )
                .await
                .map_err(catalog_store_error)?;
            (Some(plan), Some(publication_backend))
        }
        Some(config) => (
            Some(
                store
                    .plan_table_snapshot_expiration(bucket, &namespace.public_name(), table, config)
                    .await
                    .map_err(catalog_store_error)?,
            ),
            None,
        ),
        None => (None, None),
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
    let snapshot_expiration = match (snapshot_expiration_plan, snapshot_publication_backend) {
        (Some(plan), Some(publication_backend)) => {
            Some(commit_table_snapshot_expiration_response(store, &publication_backend, bucket, namespace, table, plan).await?)
        }
        (Some(plan), None) => Some(plan),
        (None, None) => None,
        (None, Some(_)) => {
            return Err(s3_error!(InternalError, "snapshot expiration publication state is missing its plan"));
        }
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

async fn commit_table_snapshot_expiration_response<B, M>(
    store: &crate::table_catalog::ObjectTableCatalogStore<B>,
    metadata_backend: &M,
    bucket: &str,
    namespace: &crate::table_catalog::Namespace,
    table: &str,
    mut report: crate::table_catalog::TableSnapshotExpirationReport,
) -> S3Result<crate::table_catalog::TableSnapshotExpirationReport>
where
    B: crate::table_catalog::TableCatalogObjectBackend,
    M: crate::table_catalog::TableCatalogObjectBackend,
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

    let current_metadata =
        read_persisted_table_metadata_for_entry(metadata_backend, &current, &current.metadata_location, true).await?;
    let updates = [serde_json::json!({
        "action": "remove-snapshots",
        "snapshot-ids": expired_snapshot_ids.clone()
    })];
    let previous_metadata_location = table_metadata_location_for_client(bucket, &current.metadata_location);
    let next_metadata = apply_table_commit_updates(current_metadata.clone(), &updates, &previous_metadata_location)?;
    validate_metadata_matches_current_metadata(&current_metadata, &next_metadata)?;
    validate_metadata_table_location_in_bucket(bucket, &next_metadata)?;
    validate_table_metadata_snapshot_graph(metadata_backend, bucket, &current, Some(&current_metadata), &next_metadata).await?;
    let (commit_id, metadata_file_token) = standard_commit_ids(None);
    let next_generation = current.generation.saturating_add(1);
    let next_metadata_location = crate::table_catalog::table_metadata_file_path_for_entry(
        &current,
        &next_metadata_file_name(next_generation, &metadata_file_token),
    )
    .map_err(catalog_store_error)?;
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
    let table_bucket_fence_required = table_warehouse_location_changes(&current, &next_metadata)?;

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
    let result = publish_table_commit(store, metadata_backend, table_bucket_fence_required, commit_request).await?;
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
    let metadata = read_persisted_table_metadata_for_entry(metadata_backend, &entry, &entry.metadata_location, true).await?;
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
    let metadata = read_persisted_table_metadata_for_entry(metadata_backend, &entry, &entry.metadata_location, true).await?;
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

async fn read_persisted_table_metadata_for_entry(
    metadata_backend: &impl crate::table_catalog::TableCatalogObjectBackend,
    entry: &crate::table_catalog::TableEntry,
    metadata_location: &str,
    require_current_warehouse: bool,
) -> S3Result<serde_json::Value> {
    validate_persisted_table_metadata_location(entry, metadata_location)?;
    let metadata = read_persisted_metadata_json(metadata_backend, &entry.table_bucket, metadata_location, "table").await?;
    validate_persisted_table_metadata(entry, &metadata, require_current_warehouse)?;
    Ok(metadata)
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

async fn sync_external_catalog_bridge_response<B, M>(
    store: &crate::table_catalog::ObjectTableCatalogStore<B>,
    metadata_backend: &M,
    bucket: &str,
    namespace: &crate::table_catalog::Namespace,
    table: &str,
    request: ExternalCatalogBridgeSyncRequest,
    table_bucket_enabled: bool,
) -> S3Result<ExternalCatalogBridgeSyncResponse>
where
    B: crate::table_catalog::TableCatalogObjectBackend,
    M: crate::table_catalog::TableCatalogObjectBackend,
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
        let current_metadata =
            read_persisted_table_metadata_for_entry(metadata_backend, &current, &current.metadata_location, true).await?;
        validate_metadata_matches_current_metadata(&current_metadata, &target_metadata)?;
        validate_table_metadata_snapshot_graph(metadata_backend, bucket, &current, Some(&current_metadata), &target_metadata)
            .await?;
        let table_bucket_fence_required = table_warehouse_location_changes(&current, &target_metadata)?;
        let result = publish_table_commit(
            store,
            metadata_backend,
            table_bucket_fence_required,
            crate::table_catalog::TableCommitRequest {
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
            },
        )
        .await?;
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
        validate_table_metadata_snapshot_graph(metadata_backend, bucket, &entry, None, &target_metadata).await?;
        store
            .register_table_with_publication(entry.clone(), metadata_backend)
            .await
            .map_err(catalog_store_error)?;
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
        validate_table_metadata_snapshot_graph(metadata_backend, bucket, &entry, None, &metadata).await?;
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
        store
            .register_table_with_publication(entry.clone(), metadata_backend)
            .await
            .map_err(catalog_store_error)?;
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
        let metadata_location = table_metadata_location_for_catalog(bucket, &request.metadata_location)?;
        if !crate::table_catalog::is_valid_table_metadata_location_for_entry(&current, &metadata_location) {
            return Err(s3_error!(InvalidRequest, "metadata location must be inside the table metadata directory"));
        }
        let current_metadata =
            read_persisted_table_metadata_for_entry(metadata_backend, &current, &current.metadata_location, true).await?;
        let target_metadata = read_table_metadata_json(metadata_backend, bucket, &metadata_location).await?;
        validate_metadata_table_location_in_bucket(bucket, &target_metadata)?;
        validate_metadata_matches_current_metadata(&current_metadata, &target_metadata)?;
        validate_table_metadata_snapshot_graph(metadata_backend, bucket, &current, None, &target_metadata).await?;
        let table_bucket_fence_required = table_warehouse_location_changes(&current, &target_metadata)?;
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
        let result = publish_table_commit(store, metadata_backend, table_bucket_fence_required, commit_request).await?;
        Ok(commit_table_response_from_result(result, target_metadata))
    }
    .await;
    record_table_catalog_admin_operation_result("rollback", bucket, &namespace.public_name(), table, started, &result);
    result
}

#[cfg(test)]
mod tests;
