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

//! Internal table catalog primitives for the Iceberg REST Catalog framework.
//!
//! This module intentionally does not expose HTTP handlers or mutate existing
//! S3 object behavior. It defines the stable internal boundary that later
//! catalog routes and object guards can share.

#![allow(dead_code)]

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    sync::Arc,
    time::{Duration as StdDuration, Instant},
};

use http::HeaderMap;
use metrics::{counter, histogram};
use rustfs_ecstore::bucket::{
    metadata::{
        BUCKET_TABLE_CATALOG_META_PREFIX, BUCKET_TABLE_CATALOG_TABLE_BUCKETS_PREFIX, BUCKET_TABLE_CONFIG,
        BUCKET_TABLE_RESERVED_PREFIX, table_catalog_path_hash,
    },
    metadata_sys,
};
use rustfs_ecstore::disk::RUSTFS_META_BUCKET;
use rustfs_ecstore::error::StorageError;
use rustfs_ecstore::{
    set_disk::get_lock_acquire_timeout,
    store_api::{HTTPPreconditions, NamespaceLocking, ObjectOptions, PutObjReader, StorageAPI},
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use time::{Duration, OffsetDateTime};
use tokio::io::AsyncReadExt;
use uuid::Uuid;

pub(crate) const TABLE_BUCKET_MARKER_CONFIG: &str = BUCKET_TABLE_CONFIG;
pub(crate) const RESERVED_CATALOG_OBJECT_MESSAGE: &str = "Object key is reserved for the table catalog";
pub(crate) const TABLE_BUCKET_CATALOG_TYPE: &str = "iceberg-rest";
pub(crate) const TABLE_BUCKET_CONFIG_VERSION: u16 = 1;
pub(crate) const DEFAULT_WAREHOUSE_ID: &str = "default";
pub(crate) const TABLE_NAMESPACE_MARKER_VERSION: u16 = 1;
pub(crate) const TABLE_RESOURCE_MARKER_VERSION: u16 = 1;
pub(crate) const TABLE_METADATA_POINTER_VERSION: u16 = 1;
pub(crate) const TABLE_CATALOG_ENTRY_VERSION: u16 = 1;
pub(crate) const TABLE_MAINTENANCE_CONFIG_VERSION: u16 = 1;
pub(crate) const TABLE_METADATA_FILE_NAME_MAX_LEN: usize = 128;
pub const TABLE_RESERVED_PREFIX: &str = BUCKET_TABLE_RESERVED_PREFIX;
const WAREHOUSE_ROOT: &str = "warehouses";
const NAMESPACE_ROOT: &str = "namespaces";
const TABLE_ROOT: &str = "tables";
const NAMESPACE_MARKER_FILE: &str = "namespace.json";
const TABLE_MARKER_FILE: &str = "table.json";
const CURRENT_POINTER_FILE: &str = "current.json";
const LIFECYCLE_FILE: &str = "lifecycle.json";
const METADATA_DIR: &str = "metadata";
const TABLE_BUCKET_ENTRY_FILE: &str = "table-bucket.json";
const NAMESPACE_ENTRY_FILE: &str = "namespace-entry.json";
const TABLE_ENTRY_FILE: &str = "table-entry.json";
const INTERNAL_CATALOG_ROOT: &str = BUCKET_TABLE_CATALOG_META_PREFIX;
const TABLE_BUCKET_ROOT: &str = BUCKET_TABLE_CATALOG_TABLE_BUCKETS_PREFIX;
const COMMIT_LOG_ROOT: &str = "commits";
const COMMIT_IDEMPOTENCY_ROOT: &str = "commit-idempotency";
const MAINTENANCE_ROOT: &str = "maintenance";
const MAINTENANCE_CONFIG_FILE: &str = "config.json";
const MAINTENANCE_JOB_ROOT: &str = "jobs";
const MAINTENANCE_LATEST_JOB_FILE: &str = "latest.json";
const MAINTENANCE_CURRENT_JOB_FILE: &str = "current.json";
const MAINTENANCE_JOB_ALIAS_LATEST: &str = "latest";
const MAINTENANCE_JOB_ALIAS_CURRENT: &str = "current";
const TABLE_CATALOG_LIST_MAX_KEYS: i32 = 1000;
const TABLE_METADATA_CLEANUP_SAFETY_WINDOW_SECONDS: i64 = 15 * 60;
const TABLE_MAINTENANCE_RETRY_BACKOFF_MAX_SECONDS: u64 = 24 * 60 * 60;
const TABLE_MAINTENANCE_WORKER_LEASE_TIMEOUT_DEFAULT_SECONDS: u64 = 15 * 60;
const TABLE_MAINTENANCE_WORKER_LEASE_TIMEOUT_MAX_SECONDS: u64 = 24 * 60 * 60;
const TABLE_COMMIT_SLOW_LOG_THRESHOLD: StdDuration = StdDuration::from_secs(2);
const ICEBERG_MAIN_REF: &str = "main";
const ICEBERG_MIN_SNAPSHOTS_TO_KEEP_PROPERTY: &str = "history.expire.min-snapshots-to-keep";
const ICEBERG_MAX_SNAPSHOT_AGE_MS_PROPERTY: &str = "history.expire.max-snapshot-age-ms";
const ICEBERG_MAX_REF_AGE_MS_PROPERTY: &str = "history.expire.max-ref-age-ms";
const ICEBERG_REF_MIN_SNAPSHOTS_TO_KEEP_FIELD: &str = "min-snapshots-to-keep";
const ICEBERG_REF_MAX_SNAPSHOT_AGE_MS_FIELD: &str = "max-snapshot-age-ms";
const ICEBERG_REF_MAX_REF_AGE_MS_FIELD: &str = "max-ref-age-ms";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CatalogIdentifierError {
    Empty,
    TooLong { max: usize },
    InvalidCharacter,
    InvalidBoundary,
    Ambiguous,
}

impl fmt::Display for CatalogIdentifierError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => f.write_str("catalog identifier segment is empty"),
            Self::TooLong { max } => write!(f, "catalog identifier segment exceeds {max} characters"),
            Self::InvalidCharacter => f.write_str("catalog identifier segment contains invalid characters"),
            Self::InvalidBoundary => {
                f.write_str("catalog identifier segment must start and end with a lowercase letter or digit")
            }
            Self::Ambiguous => f.write_str("catalog identifier segment is ambiguous"),
        }
    }
}

impl std::error::Error for CatalogIdentifierError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TableObjectMutationError {
    ReservedCatalogObject,
}

impl fmt::Display for TableObjectMutationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ReservedCatalogObject => f.write_str("object key is reserved for the table catalog"),
        }
    }
}

impl std::error::Error for TableObjectMutationError {}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct TableBucketMarker {
    pub version: u16,
    pub catalog_type: &'static str,
    pub reserved_prefix: &'static str,
}

impl Default for TableBucketMarker {
    fn default() -> Self {
        Self {
            version: TABLE_BUCKET_CONFIG_VERSION,
            catalog_type: TABLE_BUCKET_CATALOG_TYPE,
            reserved_prefix: TABLE_RESERVED_PREFIX,
        }
    }
}

pub(crate) fn table_bucket_marker_json() -> Result<Vec<u8>, serde_json::Error> {
    serde_json::to_vec(&TableBucketMarker::default())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum TableCatalogEntryState {
    Active,
    Deleting,
    Deleted,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TableBucketEntry {
    pub version: u16,
    pub table_bucket: String,
    pub catalog_type: String,
    pub warehouse_root: String,
    pub state: TableCatalogEntryState,
    #[serde(default)]
    pub properties: BTreeMap<String, String>,
    pub created_at: Option<String>,
    pub updated_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NamespaceEntry {
    pub version: u16,
    pub table_bucket: String,
    pub namespace: String,
    pub namespace_id: String,
    pub state: TableCatalogEntryState,
    #[serde(default)]
    pub properties: BTreeMap<String, String>,
    pub created_at: Option<String>,
    pub updated_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TableEntry {
    pub version: u16,
    pub table_bucket: String,
    pub namespace: String,
    pub table: String,
    pub table_id: String,
    pub table_uuid: String,
    pub format: String,
    pub format_version: u16,
    pub warehouse_location: String,
    pub metadata_location: String,
    pub version_token: String,
    pub generation: u64,
    pub state: TableCatalogEntryState,
    #[serde(default)]
    pub properties: BTreeMap<String, String>,
    pub created_at: Option<String>,
    pub updated_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum CommitLogStatus {
    Staged,
    Committed,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CommitLogEntry {
    pub version: u16,
    pub commit_id: String,
    pub idempotency_key: Option<String>,
    pub table_id: String,
    pub operation: String,
    pub expected_version_token: String,
    pub new_version_token: String,
    pub previous_metadata_location: String,
    pub new_metadata_location: String,
    #[serde(default)]
    pub requirements: Vec<serde_json::Value>,
    pub status: CommitLogStatus,
    pub writer: Option<String>,
    pub created_at: Option<String>,
    pub updated_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TableCommitRequest {
    pub table_bucket: String,
    pub namespace: String,
    pub table: String,
    pub commit_id: String,
    pub idempotency_key: Option<String>,
    pub operation: String,
    pub expected_version_token: String,
    pub expected_metadata_location: String,
    pub new_metadata_location: String,
    #[serde(default)]
    pub requirements: Vec<serde_json::Value>,
    pub writer: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TableCommitResult {
    pub table: TableEntry,
    pub commit_log: CommitLogEntry,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TableMaintenanceConfig {
    pub version: u16,
    #[serde(rename = "retain-recent-metadata-files")]
    pub retain_recent_metadata_files: usize,
    #[serde(rename = "delete-enabled")]
    pub delete_enabled: bool,
    #[serde(rename = "background-enabled")]
    pub background_enabled: bool,
    #[serde(default, rename = "worker-paused")]
    pub worker_paused: bool,
    #[serde(
        default = "default_table_maintenance_worker_lease_timeout_seconds",
        rename = "worker-lease-timeout-seconds"
    )]
    pub worker_lease_timeout_seconds: u64,
    #[serde(default, rename = "max-retry-attempts")]
    pub max_retry_attempts: u16,
    #[serde(default, rename = "retry-initial-backoff-seconds")]
    pub retry_initial_backoff_seconds: u64,
    #[serde(default, rename = "retry-max-backoff-seconds")]
    pub retry_max_backoff_seconds: u64,
    #[serde(default, rename = "quarantine-enabled")]
    pub quarantine_enabled: bool,
    #[serde(default, rename = "quarantine-retention-seconds")]
    pub quarantine_retention_seconds: u64,
}

impl Default for TableMaintenanceConfig {
    fn default() -> Self {
        Self {
            version: TABLE_MAINTENANCE_CONFIG_VERSION,
            retain_recent_metadata_files: 0,
            delete_enabled: false,
            background_enabled: false,
            worker_paused: false,
            worker_lease_timeout_seconds: TABLE_MAINTENANCE_WORKER_LEASE_TIMEOUT_DEFAULT_SECONDS,
            max_retry_attempts: 0,
            retry_initial_backoff_seconds: 5,
            retry_max_backoff_seconds: 300,
            quarantine_enabled: false,
            quarantine_retention_seconds: 0,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum TableMaintenanceConfigSource {
    #[default]
    Default,
    TableBucketDefault,
    TableOverride,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TableMaintenanceEffectiveConfig {
    pub config: TableMaintenanceConfig,
    pub source: TableMaintenanceConfigSource,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TableMetadataMaintenanceJob {
    pub job_id: String,
    pub table_bucket: String,
    pub namespace: String,
    pub table: String,
    pub table_id: String,
    #[serde(default)]
    pub operation: TableMetadataMaintenanceOperation,
    #[serde(default)]
    pub status: TableMetadataMaintenanceJobStatus,
    #[serde(default)]
    pub failure_reason: Option<String>,
    #[serde(default)]
    pub config_source: TableMaintenanceConfigSource,
    #[serde(default)]
    pub worker_id: Option<String>,
    #[serde(default)]
    pub lease_id: String,
    #[serde(default)]
    pub attempt: u16,
    #[serde(default, rename = "max-retry-attempts")]
    pub max_retry_attempts: u16,
    #[serde(default, rename = "next-retry-after")]
    pub next_retry_after: Option<String>,
    #[serde(default, rename = "quarantine-enabled")]
    pub quarantine_enabled: bool,
    #[serde(default, rename = "quarantine-retention-seconds")]
    pub quarantine_retention_seconds: u64,
    #[serde(default)]
    pub heartbeat_at: Option<String>,
    #[serde(default)]
    pub started_at: Option<String>,
    #[serde(default)]
    pub finished_at: Option<String>,
    pub current_metadata_location: String,
    pub current_generation: u64,
    pub retain_recent_metadata_files: usize,
    pub safety_window_seconds: i64,
    pub cleanup_watermark_unix_seconds: i64,
    #[serde(default)]
    pub planned_metadata_file_count: usize,
    #[serde(default)]
    pub retained_metadata_file_count: usize,
    #[serde(default)]
    pub cleanup_candidate_count: usize,
    #[serde(default)]
    pub deletable_metadata_file_count: usize,
    #[serde(default)]
    pub deleted_metadata_file_count: usize,
    #[serde(default)]
    pub quarantined_object_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TableMetadataMaintenanceReport {
    pub job: TableMetadataMaintenanceJob,
    pub current_metadata_location: String,
    pub retained_metadata_locations: Vec<String>,
    pub cleanup_candidate_locations: Vec<String>,
    pub deletable_metadata_locations: Vec<String>,
    #[serde(default)]
    pub object_reports: Vec<TableMetadataMaintenanceObjectReport>,
    #[serde(default)]
    pub referenced_object_reports: Vec<TableMetadataMaintenanceReferencedObjectReport>,
    #[serde(default, rename = "reachability-graph")]
    pub reachability_graph: TableMaintenanceReachabilityGraphReport,
    #[serde(default, rename = "snapshot-expiration")]
    pub snapshot_expiration: Option<TableSnapshotExpirationReport>,
    #[serde(default)]
    pub compaction: Option<TableCompactionPlanningReport>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TableMaintenanceReachabilityGraphReport {
    pub status: TableMaintenanceReachabilityGraphStatus,
    pub metadata_file_count: usize,
    pub manifest_list_count: usize,
    pub manifest_file_count: usize,
    pub data_file_count: usize,
    pub delete_file_count: usize,
    pub manual_review_count: usize,
    pub reasons: Vec<TableMaintenanceReachabilityGraphReason>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum TableMaintenanceReachabilityGraphStatus {
    Complete,
    #[default]
    ManualReviewRequired,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum TableMaintenanceReachabilityGraphReason {
    MetadataJsonParsed,
    ManifestListAvroReferenced,
    ManifestAvroReaderUnavailable,
    DataFileCleanupDeferred,
    DeleteFileCleanupDeferred,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TableSnapshotExpirationConfig {
    #[serde(rename = "min-snapshots-to-keep")]
    pub min_snapshots_to_keep: usize,
    #[serde(rename = "max-snapshot-age-ms")]
    pub max_snapshot_age_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TableSnapshotExpirationReport {
    pub table_bucket: String,
    pub namespace: String,
    pub table: String,
    pub table_id: String,
    pub current_metadata_location: String,
    pub current_snapshot_id: Option<i64>,
    pub config: TableSnapshotExpirationConfig,
    pub expiration_watermark_ms: i64,
    pub retained_snapshot_count: usize,
    pub expiration_candidate_count: usize,
    pub manual_review_count: usize,
    #[serde(default)]
    pub expired_snapshot_ids: Vec<i64>,
    #[serde(default)]
    pub committed_metadata_location: Option<String>,
    pub snapshot_reports: Vec<TableSnapshotExpirationSnapshotReport>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TableSnapshotExpirationSnapshotReport {
    pub snapshot_id: Option<i64>,
    pub sequence_number: Option<i64>,
    pub timestamp_ms: Option<i64>,
    pub manifest_list: Option<String>,
    pub state: TableSnapshotExpirationSnapshotState,
    pub reasons: Vec<TableSnapshotExpirationReason>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum TableSnapshotExpirationSnapshotState {
    Retained,
    ExpirationCandidate,
    ManualReviewRequired,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum TableSnapshotExpirationReason {
    CurrentSnapshot,
    MinSnapshotsToKeep,
    ProtectedSnapshotRef,
    UserDefinedSnapshotRef,
    SnapshotRefRetentionConflict,
    TableRetentionPropertyConflict,
    MissingSnapshotId,
    MissingSnapshotTimestamp,
    SnapshotAgeWithinRetention,
    SnapshotAgeExpired,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TableCompactionPlanningConfig {
    #[serde(rename = "target-file-size-bytes")]
    pub target_file_size_bytes: u64,
    #[serde(rename = "small-file-threshold-bytes")]
    pub small_file_threshold_bytes: u64,
    #[serde(rename = "min-input-files")]
    pub min_input_files: usize,
    #[serde(rename = "max-rewrite-bytes-per-job")]
    pub max_rewrite_bytes_per_job: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TableCompactionPlanningReport {
    pub table_bucket: String,
    pub namespace: String,
    pub table: String,
    pub table_id: String,
    pub current_metadata_location: String,
    pub current_snapshot_id: Option<i64>,
    pub config: TableCompactionPlanningConfig,
    pub status: TableCompactionPlanningStatus,
    pub candidate_file_count: usize,
    pub rewrite_group_count: usize,
    pub manual_review_count: usize,
    pub snapshot_reports: Vec<TableCompactionSnapshotReport>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TableCompactionSnapshotReport {
    pub snapshot_id: Option<i64>,
    pub manifest_list: Option<String>,
    pub status: TableCompactionPlanningStatus,
    pub reasons: Vec<TableCompactionPlanningReason>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum TableCompactionPlanningStatus {
    NoCandidates,
    ManualReviewRequired,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum TableCompactionPlanningReason {
    ManifestList,
    ManifestAvroReaderUnavailable,
    MissingCurrentSnapshot,
    MissingManifestList,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum TableMetadataMaintenanceOperation {
    #[default]
    DryRun,
    Delete,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum TableMetadataMaintenanceJobStatus {
    NotYetRun,
    Running,
    #[default]
    Successful,
    Failed,
    Disabled,
    Paused,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum TableMetadataMaintenanceObjectState {
    Retained,
    PendingSafetyWindow,
    Deletable,
    Deleted,
    ManualReviewRequired,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum TableMetadataMaintenanceReason {
    CurrentMetadata,
    MetadataLog,
    ProtectedSnapshotRef,
    RecentMetadata,
    NoCurrentReachability,
    SafetyWindowPending,
    SafetyWindowSatisfied,
    DeletedByMaintenance,
    ManifestList,
    UnsupportedManifestAvro,
    QuarantineEnabled,
    RetryScheduled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum TableMetadataMaintenanceObjectKind {
    MetadataFile,
    ManifestList,
    ManifestFile,
    DataFile,
    DeleteFile,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TableMetadataMaintenanceObjectReport {
    pub metadata_location: String,
    pub state: TableMetadataMaintenanceObjectState,
    pub reasons: Vec<TableMetadataMaintenanceReason>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TableMetadataMaintenanceReferencedObjectReport {
    pub object_location: String,
    pub object_kind: TableMetadataMaintenanceObjectKind,
    pub state: TableMetadataMaintenanceObjectState,
    pub reasons: Vec<TableMetadataMaintenanceReason>,
}

struct TableMaintenanceHeartbeatRef<'a> {
    table_bucket: &'a str,
    namespace: &'a str,
    table: &'a str,
    job_id: &'a str,
    lease_id: &'a str,
    worker_id: &'a str,
}

struct TableMaintenanceWorkerControlReport<'a> {
    table_bucket: &'a str,
    namespace: &'a str,
    table: &'a str,
    worker_id: String,
    effective: &'a TableMaintenanceEffectiveConfig,
    status: TableMetadataMaintenanceJobStatus,
    reason: &'a str,
    now: OffsetDateTime,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct TableCatalogExport {
    pub table_bucket: TableBucketEntry,
    pub namespace: NamespaceEntry,
    pub table: TableEntry,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TableDataPlaneResource {
    pub table_bucket: String,
    pub namespace: String,
    pub table: String,
    pub table_id: String,
    pub warehouse_object_prefix: String,
}

impl TableDataPlaneResource {
    pub(crate) fn catalog_resource_object(&self) -> String {
        let namespace = Namespace::parse(&self.namespace)
            .map(|namespace| namespace.storage_id())
            .unwrap_or_else(|_| self.namespace.clone());
        format!("{NAMESPACE_ROOT}/{namespace}/{TABLE_ROOT}/{}", self.table)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum TableMetadataPointerStatus {
    Valid,
    MissingObject,
    InvalidLocation,
    InvalidJson,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum TableCatalogRecoveryStatus {
    Healthy,
    Recoverable,
    ManualReviewRequired,
    ReadOnlyRecommended,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum TableCatalogRecoveryAction {
    RunCommitRecovery,
    RetryCommit,
    RestoreCurrentMetadataObject,
    FixCurrentMetadataJson,
    MoveCurrentMetadataInsideTable,
    ReviewCommitLog,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct TableCatalogDiagnosticsReport {
    pub catalog: TableCatalogExport,
    pub current_metadata_status: TableMetadataPointerStatus,
    pub recovery_status: TableCatalogRecoveryStatus,
    pub recommended_actions: Vec<TableCatalogRecoveryAction>,
    pub commit_recovery: TableCommitRecoveryReport,
    pub orphan_metadata_candidate_locations: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum TableCommitRecoveryState {
    Committed,
    StagedBeforeTableUpdate,
    FinalizationRequired,
    IdempotencyIndexRepairRequired,
    ManualReview,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum TableCommitIdempotencyIndexStatus {
    NotRequired,
    Missing,
    Matches,
    Stale,
    Conflicting,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct TableCommitRecoveryEntry {
    pub commit_id: String,
    pub idempotency_key: Option<String>,
    pub operation: String,
    pub status: CommitLogStatus,
    pub recovery_state: TableCommitRecoveryState,
    pub previous_metadata_location: String,
    pub new_metadata_location: String,
    pub expected_version_token: String,
    pub new_version_token: String,
    pub idempotency_index_present: bool,
    pub idempotency_index_status: TableCommitIdempotencyIndexStatus,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct TableCommitRecoveryReport {
    pub table_bucket: String,
    pub namespace: String,
    pub table: String,
    pub table_id: String,
    pub current_metadata_location: String,
    pub current_version_token: String,
    pub current_generation: u64,
    pub commits: Vec<TableCommitRecoveryEntry>,
    pub staged_before_table_update_count: usize,
    pub finalization_required_count: usize,
    pub idempotency_repair_required_count: usize,
    pub manual_review_count: usize,
    pub finalized_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TableCatalogStoreError {
    NotFound(String),
    Conflict(String),
    Invalid(String),
    Internal(String),
}

impl fmt::Display for TableCatalogStoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotFound(message) => write!(f, "table catalog entry not found: {message}"),
            Self::Conflict(message) => write!(f, "table catalog conflict: {message}"),
            Self::Invalid(message) => write!(f, "invalid table catalog entry: {message}"),
            Self::Internal(message) => write!(f, "table catalog store error: {message}"),
        }
    }
}

impl std::error::Error for TableCatalogStoreError {}

pub(crate) type TableCatalogStoreResult<T> = Result<T, TableCatalogStoreError>;

fn normalize_warehouse_object_prefix(object_prefix: &str) -> TableCatalogStoreResult<String> {
    let object_prefix = object_prefix.strip_suffix('/').unwrap_or(object_prefix);
    if object_prefix.is_empty() {
        return Err(TableCatalogStoreError::Invalid(
            "table warehouse location must include an object prefix".to_string(),
        ));
    }
    if object_prefix.contains('\\') {
        return Err(TableCatalogStoreError::Invalid(
            "table warehouse location contains an invalid path separator".to_string(),
        ));
    }
    if object_prefix
        .split('/')
        .any(|segment| segment.is_empty() || segment == "." || segment == "..")
    {
        return Err(TableCatalogStoreError::Invalid(
            "table warehouse location contains an invalid path segment".to_string(),
        ));
    }

    let mut normalized = object_prefix.to_string();
    normalized.push('/');
    Ok(normalized)
}

fn table_warehouse_object_prefix_from_location(table_bucket: &str, warehouse_location: &str) -> TableCatalogStoreResult<String> {
    let location = warehouse_location
        .strip_prefix("s3://")
        .ok_or_else(|| TableCatalogStoreError::Invalid("table warehouse location must be an s3 URI".to_string()))?;
    let (bucket, object_prefix) = location
        .split_once('/')
        .ok_or_else(|| TableCatalogStoreError::Invalid("table warehouse location must include an object prefix".to_string()))?;
    if bucket != table_bucket {
        return Err(TableCatalogStoreError::Invalid(
            "table warehouse location must be inside the table bucket".to_string(),
        ));
    }
    normalize_warehouse_object_prefix(object_prefix)
}

fn table_warehouse_object_prefix(entry: &TableEntry) -> TableCatalogStoreResult<String> {
    table_warehouse_object_prefix_from_location(&entry.table_bucket, &entry.warehouse_location)
}

fn metadata_warehouse_location(
    table_bucket: &str,
    metadata_location: &str,
    metadata_object: &TableCatalogObject,
) -> TableCatalogStoreResult<Option<String>> {
    let metadata: serde_json::Value = serde_json::from_slice(&metadata_object.data)
        .map_err(|err| TableCatalogStoreError::Invalid(format!("failed to parse new metadata {metadata_location}: {err}")))?;
    let Some(location) = metadata.get("location").and_then(serde_json::Value::as_str) else {
        return Ok(None);
    };
    table_warehouse_object_prefix_from_location(table_bucket, location)?;
    Ok(Some(location.to_string()))
}

pub(crate) async fn table_data_plane_resource_for_object<S>(
    store: &S,
    bucket: &str,
    object: &str,
) -> TableCatalogStoreResult<Option<TableDataPlaneResource>>
where
    S: TableCatalogStore + ?Sized,
{
    if bucket.is_empty() || object.is_empty() {
        return Ok(None);
    }

    let Some(table_bucket) = store.get_table_bucket(bucket).await? else {
        return Ok(None);
    };
    if table_bucket.state != TableCatalogEntryState::Active {
        return Ok(None);
    }

    let mut matched: Option<TableDataPlaneResource> = None;
    for namespace in store.list_namespaces(bucket).await? {
        if namespace.state != TableCatalogEntryState::Active {
            continue;
        }
        for table in store.list_tables(bucket, &namespace.namespace).await? {
            if table.state != TableCatalogEntryState::Active {
                continue;
            }
            let Ok(warehouse_object_prefix) = table_warehouse_object_prefix(&table) else {
                continue;
            };
            if !object.starts_with(&warehouse_object_prefix) {
                continue;
            }
            if matched
                .as_ref()
                .is_some_and(|current| current.warehouse_object_prefix.len() >= warehouse_object_prefix.len())
            {
                continue;
            }
            matched = Some(TableDataPlaneResource {
                table_bucket: table.table_bucket,
                namespace: table.namespace,
                table: table.table,
                table_id: table.table_id,
                warehouse_object_prefix,
            });
        }
    }

    Ok(matched)
}

#[async_trait::async_trait]
pub(crate) trait TableCatalogStore: Send + Sync {
    async fn get_table_bucket(&self, table_bucket: &str) -> TableCatalogStoreResult<Option<TableBucketEntry>>;

    async fn put_table_bucket(&self, entry: TableBucketEntry) -> TableCatalogStoreResult<()>;

    async fn create_namespace(&self, entry: NamespaceEntry) -> TableCatalogStoreResult<()>;

    async fn list_namespaces(&self, table_bucket: &str) -> TableCatalogStoreResult<Vec<NamespaceEntry>>;

    async fn get_namespace(&self, table_bucket: &str, namespace: &str) -> TableCatalogStoreResult<Option<NamespaceEntry>>;

    async fn drop_namespace(&self, table_bucket: &str, namespace: &str) -> TableCatalogStoreResult<()>;

    async fn create_table(&self, entry: TableEntry) -> TableCatalogStoreResult<()>;

    async fn register_table(&self, entry: TableEntry) -> TableCatalogStoreResult<()>;

    async fn list_tables(&self, table_bucket: &str, namespace: &str) -> TableCatalogStoreResult<Vec<TableEntry>>;

    async fn load_table(&self, table_bucket: &str, namespace: &str, table: &str) -> TableCatalogStoreResult<Option<TableEntry>>;

    async fn commit_table(&self, request: TableCommitRequest) -> TableCatalogStoreResult<TableCommitResult>;

    async fn drop_table(&self, table_bucket: &str, namespace: &str, table: &str) -> TableCatalogStoreResult<()>;

    async fn get_commit_by_id(
        &self,
        table_bucket: &str,
        table_id: &str,
        commit_id: &str,
    ) -> TableCatalogStoreResult<Option<CommitLogEntry>>;

    async fn get_commit_by_idempotency_key(
        &self,
        table_bucket: &str,
        table_id: &str,
        idempotency_key: &str,
    ) -> TableCatalogStoreResult<Option<CommitLogEntry>>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TableCatalogObject {
    pub data: Vec<u8>,
    pub etag: Option<String>,
    pub mod_time: Option<OffsetDateTime>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TableCatalogPutPrecondition {
    Any,
    IfAbsent,
    IfMatch(String),
}

#[async_trait::async_trait]
pub(crate) trait TableCatalogObjectBackend: Clone + Send + Sync + 'static {
    async fn read_object(&self, bucket: &str, object: &str) -> TableCatalogStoreResult<Option<TableCatalogObject>>;

    async fn read_object_unlocked(&self, bucket: &str, object: &str) -> TableCatalogStoreResult<Option<TableCatalogObject>> {
        self.read_object(bucket, object).await
    }

    async fn object_exists(&self, bucket: &str, object: &str) -> TableCatalogStoreResult<bool>;

    async fn put_object(
        &self,
        bucket: &str,
        object: &str,
        data: Vec<u8>,
        precondition: TableCatalogPutPrecondition,
    ) -> TableCatalogStoreResult<()>;

    async fn put_object_unlocked(
        &self,
        bucket: &str,
        object: &str,
        data: Vec<u8>,
        precondition: TableCatalogPutPrecondition,
    ) -> TableCatalogStoreResult<()> {
        self.put_object(bucket, object, data, precondition).await
    }

    async fn delete_object(&self, bucket: &str, object: &str) -> TableCatalogStoreResult<()>;

    async fn list_objects(&self, bucket: &str, prefix: &str) -> TableCatalogStoreResult<Vec<String>>;

    async fn acquire_write_lock(&self, bucket: &str, object: &str) -> TableCatalogStoreResult<Box<dyn Send>>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TableCatalogObjectPaths {
    catalog_root: &'static str,
}

impl Default for TableCatalogObjectPaths {
    fn default() -> Self {
        Self {
            catalog_root: INTERNAL_CATALOG_ROOT,
        }
    }
}

impl TableCatalogObjectPaths {
    pub fn table_bucket_entry_path(&self, table_bucket: &str) -> String {
        format!("{}{}", self.table_bucket_root_prefix(table_bucket), TABLE_BUCKET_ENTRY_FILE)
    }

    pub fn table_bucket_maintenance_config_path(&self, table_bucket: &str) -> String {
        format!(
            "{}{MAINTENANCE_ROOT}/{MAINTENANCE_CONFIG_FILE}",
            self.table_bucket_root_prefix(table_bucket)
        )
    }

    pub fn namespace_entries_prefix(&self, table_bucket: &str) -> String {
        format!("{}{}/", self.table_bucket_root_prefix(table_bucket), NAMESPACE_ROOT)
    }

    pub fn namespace_entry_path(&self, table_bucket: &str, namespace: &Namespace) -> String {
        format!(
            "{}{}/{}",
            self.namespace_entries_prefix(table_bucket),
            namespace.storage_id(),
            NAMESPACE_ENTRY_FILE
        )
    }

    pub fn table_entries_prefix(&self, table_bucket: &str, namespace: &Namespace) -> String {
        format!(
            "{}{}/{}/",
            self.namespace_entries_prefix(table_bucket),
            namespace.storage_id(),
            TABLE_ROOT
        )
    }

    pub fn table_entry_path(&self, table_bucket: &str, namespace: &Namespace, table: &IdentifierSegment) -> String {
        format!(
            "{}{}/{}",
            self.table_entries_prefix(table_bucket, namespace),
            table.as_str(),
            TABLE_ENTRY_FILE
        )
    }

    pub fn table_maintenance_config_path(
        &self,
        table_bucket: &str,
        namespace: &Namespace,
        table: &IdentifierSegment,
        table_id: &str,
    ) -> String {
        format!(
            "{}{}/{MAINTENANCE_ROOT}/{}/{MAINTENANCE_CONFIG_FILE}",
            self.table_entries_prefix(table_bucket, namespace),
            table.as_str(),
            table_catalog_path_hash(table_id)
        )
    }

    pub fn table_maintenance_job_path(
        &self,
        table_bucket: &str,
        namespace: &Namespace,
        table: &IdentifierSegment,
        table_id: &str,
        job_id: &str,
    ) -> String {
        format!(
            "{}{}/{MAINTENANCE_ROOT}/{}/{MAINTENANCE_JOB_ROOT}/{}.json",
            self.table_entries_prefix(table_bucket, namespace),
            table.as_str(),
            table_catalog_path_hash(table_id),
            table_catalog_path_hash(job_id)
        )
    }

    pub fn table_maintenance_latest_job_path(
        &self,
        table_bucket: &str,
        namespace: &Namespace,
        table: &IdentifierSegment,
        table_id: &str,
    ) -> String {
        format!(
            "{}{}/{MAINTENANCE_ROOT}/{}/{MAINTENANCE_LATEST_JOB_FILE}",
            self.table_entries_prefix(table_bucket, namespace),
            table.as_str(),
            table_catalog_path_hash(table_id)
        )
    }

    pub fn table_maintenance_current_job_path(
        &self,
        table_bucket: &str,
        namespace: &Namespace,
        table: &IdentifierSegment,
        table_id: &str,
    ) -> String {
        format!(
            "{}{}/{MAINTENANCE_ROOT}/{}/{MAINTENANCE_CURRENT_JOB_FILE}",
            self.table_entries_prefix(table_bucket, namespace),
            table.as_str(),
            table_catalog_path_hash(table_id)
        )
    }

    pub fn commit_log_entry_path(&self, table_bucket: &str, table_id: &str, commit_id: &str) -> String {
        format!(
            "{}{}/{}/{}.json",
            self.table_bucket_root_prefix(table_bucket),
            COMMIT_LOG_ROOT,
            table_catalog_path_hash(table_id),
            table_catalog_path_hash(commit_id)
        )
    }

    pub fn commit_log_entries_prefix(&self, table_bucket: &str, table_id: &str) -> String {
        format!(
            "{}{}/{}/",
            self.table_bucket_root_prefix(table_bucket),
            COMMIT_LOG_ROOT,
            table_catalog_path_hash(table_id)
        )
    }

    pub fn commit_idempotency_entry_path(&self, table_bucket: &str, table_id: &str, idempotency_key: &str) -> String {
        format!(
            "{}{}/{}/{}.json",
            self.table_bucket_root_prefix(table_bucket),
            COMMIT_IDEMPOTENCY_ROOT,
            table_catalog_path_hash(table_id),
            table_catalog_path_hash(idempotency_key)
        )
    }

    fn table_bucket_root_prefix(&self, table_bucket: &str) -> String {
        format!("{}/{}/{}/", self.catalog_root, TABLE_BUCKET_ROOT, table_catalog_path_hash(table_bucket))
    }
}

#[derive(Clone)]
pub(crate) struct ObjectTableCatalogStore<B> {
    backend: B,
    paths: TableCatalogObjectPaths,
}

impl<B> ObjectTableCatalogStore<B>
where
    B: TableCatalogObjectBackend,
{
    pub fn new(backend: B) -> Self {
        Self {
            backend,
            paths: TableCatalogObjectPaths::default(),
        }
    }

    fn catalog_bucket(&self) -> &'static str {
        RUSTFS_META_BUCKET
    }

    async fn read_entry<T>(&self, bucket: &str, object: &str) -> TableCatalogStoreResult<Option<(T, Option<String>)>>
    where
        T: DeserializeOwned,
    {
        self.read_entry_with(bucket, object, |backend, bucket, object| {
            Box::pin(async move { backend.read_object(bucket, object).await })
        })
        .await
    }

    async fn read_entry_unlocked<T>(&self, bucket: &str, object: &str) -> TableCatalogStoreResult<Option<(T, Option<String>)>>
    where
        T: DeserializeOwned,
    {
        self.read_entry_with(bucket, object, |backend, bucket, object| {
            Box::pin(async move { backend.read_object_unlocked(bucket, object).await })
        })
        .await
    }

    async fn read_entry_with<'a, T, F>(
        &'a self,
        bucket: &'a str,
        object: &'a str,
        read_object: F,
    ) -> TableCatalogStoreResult<Option<(T, Option<String>)>>
    where
        T: DeserializeOwned,
        F: FnOnce(
            &'a B,
            &'a str,
            &'a str,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = TableCatalogStoreResult<Option<TableCatalogObject>>> + Send + 'a>,
        >,
    {
        let Some(object_data) = read_object(&self.backend, bucket, object).await? else {
            return Ok(None);
        };

        let entry = serde_json::from_slice(&object_data.data)
            .map_err(|err| TableCatalogStoreError::Invalid(format!("failed to parse catalog entry {object}: {err}")))?;
        Ok(Some((entry, object_data.etag)))
    }

    async fn write_entry<T>(
        &self,
        bucket: &str,
        object: &str,
        entry: &T,
        precondition: TableCatalogPutPrecondition,
    ) -> TableCatalogStoreResult<()>
    where
        T: Serialize,
    {
        let data = serde_json::to_vec(entry)
            .map_err(|err| TableCatalogStoreError::Internal(format!("failed to serialize catalog entry {object}: {err}")))?;
        self.backend.put_object(bucket, object, data, precondition).await
    }

    async fn write_entry_unlocked<T>(
        &self,
        bucket: &str,
        object: &str,
        entry: &T,
        precondition: TableCatalogPutPrecondition,
    ) -> TableCatalogStoreResult<()>
    where
        T: Serialize,
    {
        let data = serde_json::to_vec(entry)
            .map_err(|err| TableCatalogStoreError::Internal(format!("failed to serialize catalog entry {object}: {err}")))?;
        self.backend.put_object_unlocked(bucket, object, data, precondition).await
    }

    async fn require_table_bucket(&self, table_bucket: &str) -> TableCatalogStoreResult<()> {
        if self.get_table_bucket(table_bucket).await?.is_none() {
            return Err(TableCatalogStoreError::NotFound(format!("table bucket {table_bucket}")));
        }
        Ok(())
    }

    async fn read_table_with_etag(
        &self,
        table_bucket: &str,
        namespace: &Namespace,
        table: &IdentifierSegment,
    ) -> TableCatalogStoreResult<Option<(TableEntry, String)>> {
        let table_path = self.paths.table_entry_path(table_bucket, namespace, table);
        let Some((entry, etag)) = self.read_entry::<TableEntry>(self.catalog_bucket(), &table_path).await? else {
            return Ok(None);
        };
        let Some(etag) = etag else {
            return Err(TableCatalogStoreError::Internal(format!("catalog table entry has no etag: {table_path}")));
        };
        Ok(Some((entry, etag)))
    }

    async fn read_table_with_etag_unlocked(
        &self,
        table_bucket: &str,
        namespace: &Namespace,
        table: &IdentifierSegment,
    ) -> TableCatalogStoreResult<Option<(TableEntry, String)>> {
        let table_path = self.paths.table_entry_path(table_bucket, namespace, table);
        let Some((entry, etag)) = self
            .read_entry_unlocked::<TableEntry>(self.catalog_bucket(), &table_path)
            .await?
        else {
            return Ok(None);
        };
        let Some(etag) = etag else {
            return Err(TableCatalogStoreError::Internal(format!("catalog table entry has no etag: {table_path}")));
        };
        Ok(Some((entry, etag)))
    }

    async fn write_table_entry(
        &self,
        entry: TableEntry,
        precondition: TableCatalogPutPrecondition,
    ) -> TableCatalogStoreResult<()> {
        validate_catalog_entry_version("table", entry.version)?;
        self.require_table_bucket(&entry.table_bucket).await?;
        let namespace = parse_namespace_for_store(&entry.namespace)?;
        let table = parse_table_for_store(&entry.table)?;
        if self.get_namespace(&entry.table_bucket, &entry.namespace).await?.is_none() {
            return Err(TableCatalogStoreError::NotFound(format!(
                "namespace {}/{}",
                entry.table_bucket, entry.namespace
            )));
        }
        let table_path = self.paths.table_entry_path(&entry.table_bucket, &namespace, &table);
        self.write_entry(self.catalog_bucket(), &table_path, &entry, precondition)
            .await
    }

    async fn read_commit_by_path(&self, object: &str) -> TableCatalogStoreResult<Option<CommitLogEntry>> {
        self.read_entry::<CommitLogEntry>(self.catalog_bucket(), object)
            .await
            .map(|entry| entry.map(|(commit, _)| commit))
    }

    async fn finalize_commit_log(
        &self,
        commit_path: &str,
        idempotency_path: Option<&str>,
        commit_log: &CommitLogEntry,
    ) -> TableCatalogStoreResult<()> {
        self.write_entry(self.catalog_bucket(), commit_path, commit_log, TableCatalogPutPrecondition::Any)
            .await?;
        if let Some(idempotency_path) = idempotency_path {
            self.write_entry(self.catalog_bucket(), idempotency_path, commit_log, TableCatalogPutPrecondition::Any)
                .await?;
        }
        Ok(())
    }

    async fn table_commit_recovery_report_for_entry(
        &self,
        entry: &TableEntry,
        finalized_count: usize,
    ) -> TableCatalogStoreResult<TableCommitRecoveryReport> {
        let commit_prefix = self.paths.commit_log_entries_prefix(&entry.table_bucket, &entry.table_id);
        let mut commits = Vec::new();
        for object in self.backend.list_objects(self.catalog_bucket(), &commit_prefix).await? {
            if !object.ends_with(".json") {
                continue;
            }
            let Some(commit_log) = self.read_commit_by_path(&object).await? else {
                continue;
            };
            let idempotency_commit = match commit_log.idempotency_key.as_deref() {
                Some(idempotency_key) => {
                    let idempotency_path =
                        self.paths
                            .commit_idempotency_entry_path(&entry.table_bucket, &entry.table_id, idempotency_key);
                    self.read_commit_by_path(&idempotency_path).await?
                }
                None => None,
            };
            commits.push(table_commit_recovery_entry(entry, &commit_log, idempotency_commit.as_ref()));
        }
        commits.sort_by(|left, right| left.commit_id.cmp(&right.commit_id));

        let finalization_required_count = commits
            .iter()
            .filter(|commit| matches!(commit.recovery_state, TableCommitRecoveryState::FinalizationRequired))
            .count();
        let idempotency_repair_required_count = commits
            .iter()
            .filter(|commit| matches!(commit.recovery_state, TableCommitRecoveryState::IdempotencyIndexRepairRequired))
            .count();
        let staged_before_table_update_count = commits
            .iter()
            .filter(|commit| matches!(commit.recovery_state, TableCommitRecoveryState::StagedBeforeTableUpdate))
            .count();
        let manual_review_count = commits
            .iter()
            .filter(|commit| matches!(commit.recovery_state, TableCommitRecoveryState::ManualReview))
            .count();

        Ok(TableCommitRecoveryReport {
            table_bucket: entry.table_bucket.clone(),
            namespace: entry.namespace.clone(),
            table: entry.table.clone(),
            table_id: entry.table_id.clone(),
            current_metadata_location: entry.metadata_location.clone(),
            current_version_token: entry.version_token.clone(),
            current_generation: entry.generation,
            commits,
            staged_before_table_update_count,
            finalization_required_count,
            idempotency_repair_required_count,
            manual_review_count,
            finalized_count,
        })
    }

    pub(crate) async fn plan_table_commit_recovery(
        &self,
        table_bucket: &str,
        namespace: &str,
        table: &str,
    ) -> TableCatalogStoreResult<TableCommitRecoveryReport> {
        let namespace = parse_namespace_for_store(namespace)?;
        let table = parse_table_for_store(table)?;
        let table_path = self.paths.table_entry_path(table_bucket, &namespace, &table);
        let Some((entry, _)) = self.read_entry::<TableEntry>(self.catalog_bucket(), &table_path).await? else {
            return Err(TableCatalogStoreError::NotFound(format!(
                "table {}/{}/{}",
                table_bucket,
                namespace.public_name(),
                table.as_str()
            )));
        };
        self.table_commit_recovery_report_for_entry(&entry, 0).await
    }

    pub(crate) async fn recover_table_commits(
        &self,
        table_bucket: &str,
        namespace: &str,
        table: &str,
    ) -> TableCatalogStoreResult<TableCommitRecoveryReport> {
        let namespace = parse_namespace_for_store(namespace)?;
        let table = parse_table_for_store(table)?;
        let table_path = self.paths.table_entry_path(table_bucket, &namespace, &table);
        let _guard = self.backend.acquire_write_lock(self.catalog_bucket(), &table_path).await?;
        let Some((entry, _)) = self.read_table_with_etag_unlocked(table_bucket, &namespace, &table).await? else {
            return Err(TableCatalogStoreError::NotFound(format!(
                "table {}/{}/{}",
                table_bucket,
                namespace.public_name(),
                table.as_str()
            )));
        };

        let commit_prefix = self.paths.commit_log_entries_prefix(table_bucket, &entry.table_id);
        let mut finalized_count = 0;
        for commit_path in self.backend.list_objects(self.catalog_bucket(), &commit_prefix).await? {
            if !commit_path.ends_with(".json") {
                continue;
            }
            let Some(commit_log) = self.read_commit_by_path(&commit_path).await? else {
                continue;
            };
            let idempotency_path = commit_log.idempotency_key.as_deref().map(|idempotency_key| {
                self.paths
                    .commit_idempotency_entry_path(table_bucket, &entry.table_id, idempotency_key)
            });
            let idempotency_commit = match idempotency_path.as_deref() {
                Some(idempotency_path) => self.read_commit_by_path(idempotency_path).await?,
                None => None,
            };
            let recovery_entry = table_commit_recovery_entry(&entry, &commit_log, idempotency_commit.as_ref());
            if matches!(
                recovery_entry.recovery_state,
                TableCommitRecoveryState::FinalizationRequired | TableCommitRecoveryState::IdempotencyIndexRepairRequired
            ) {
                let mut committed = commit_log;
                committed.status = CommitLogStatus::Committed;
                self.finalize_commit_log(&commit_path, idempotency_path.as_deref(), &committed)
                    .await?;
                finalized_count += 1;
            }
        }

        self.table_commit_recovery_report_for_entry(&entry, finalized_count).await
    }

    pub(crate) async fn get_table_maintenance_config(
        &self,
        table_bucket: &str,
        namespace: &str,
        table: &str,
    ) -> TableCatalogStoreResult<TableMaintenanceConfig> {
        let namespace = parse_namespace_for_store(namespace)?;
        let table = parse_table_for_store(table)?;
        let table_path = self.paths.table_entry_path(table_bucket, &namespace, &table);
        let Some((entry, _)) = self.read_entry::<TableEntry>(self.catalog_bucket(), &table_path).await? else {
            return Err(TableCatalogStoreError::NotFound(format!(
                "table {}/{}/{}",
                table_bucket,
                namespace.public_name(),
                table.as_str()
            )));
        };

        let config_path = self
            .paths
            .table_maintenance_config_path(table_bucket, &namespace, &table, &entry.table_id);
        let config = self
            .read_entry::<TableMaintenanceConfig>(self.catalog_bucket(), &config_path)
            .await?
            .map(|(config, _)| config)
            .unwrap_or_default();
        validate_table_maintenance_config(&config)?;
        Ok(config)
    }

    pub(crate) async fn put_table_bucket_maintenance_config(
        &self,
        table_bucket: &str,
        config: TableMaintenanceConfig,
    ) -> TableCatalogStoreResult<TableMaintenanceConfig> {
        validate_table_maintenance_config(&config)?;
        self.require_table_bucket(table_bucket).await?;
        let config_path = self.paths.table_bucket_maintenance_config_path(table_bucket);
        self.write_entry(self.catalog_bucket(), &config_path, &config, TableCatalogPutPrecondition::Any)
            .await?;
        Ok(config)
    }

    pub(crate) async fn get_effective_table_maintenance_config(
        &self,
        table_bucket: &str,
        namespace: &str,
        table: &str,
    ) -> TableCatalogStoreResult<TableMaintenanceEffectiveConfig> {
        let namespace = parse_namespace_for_store(namespace)?;
        let table = parse_table_for_store(table)?;
        let table_path = self.paths.table_entry_path(table_bucket, &namespace, &table);
        let Some((entry, _)) = self.read_entry::<TableEntry>(self.catalog_bucket(), &table_path).await? else {
            return Err(TableCatalogStoreError::NotFound(format!(
                "table {}/{}/{}",
                table_bucket,
                namespace.public_name(),
                table.as_str()
            )));
        };

        let table_config_path = self
            .paths
            .table_maintenance_config_path(table_bucket, &namespace, &table, &entry.table_id);
        if let Some((config, _)) = self
            .read_entry::<TableMaintenanceConfig>(self.catalog_bucket(), &table_config_path)
            .await?
        {
            validate_table_maintenance_config(&config)?;
            return Ok(TableMaintenanceEffectiveConfig {
                config,
                source: TableMaintenanceConfigSource::TableOverride,
            });
        }

        let bucket_config_path = self.paths.table_bucket_maintenance_config_path(table_bucket);
        if let Some((config, _)) = self
            .read_entry::<TableMaintenanceConfig>(self.catalog_bucket(), &bucket_config_path)
            .await?
        {
            validate_table_maintenance_config(&config)?;
            return Ok(TableMaintenanceEffectiveConfig {
                config,
                source: TableMaintenanceConfigSource::TableBucketDefault,
            });
        }

        Ok(TableMaintenanceEffectiveConfig {
            config: TableMaintenanceConfig::default(),
            source: TableMaintenanceConfigSource::Default,
        })
    }

    pub(crate) async fn put_table_maintenance_config(
        &self,
        table_bucket: &str,
        namespace: &str,
        table: &str,
        config: TableMaintenanceConfig,
    ) -> TableCatalogStoreResult<TableMaintenanceConfig> {
        validate_table_maintenance_config(&config)?;
        let namespace = parse_namespace_for_store(namespace)?;
        let table = parse_table_for_store(table)?;
        let table_path = self.paths.table_entry_path(table_bucket, &namespace, &table);
        let Some((entry, _)) = self.read_entry::<TableEntry>(self.catalog_bucket(), &table_path).await? else {
            return Err(TableCatalogStoreError::NotFound(format!(
                "table {}/{}/{}",
                table_bucket,
                namespace.public_name(),
                table.as_str()
            )));
        };

        let config_path = self
            .paths
            .table_maintenance_config_path(table_bucket, &namespace, &table, &entry.table_id);
        self.write_entry(self.catalog_bucket(), &config_path, &config, TableCatalogPutPrecondition::Any)
            .await?;
        Ok(config)
    }

    pub(crate) async fn put_table_metadata_maintenance_report(
        &self,
        report: &TableMetadataMaintenanceReport,
    ) -> TableCatalogStoreResult<()> {
        let namespace = parse_namespace_for_store(&report.job.namespace)?;
        let table = parse_table_for_store(&report.job.table)?;
        let job_path = self.paths.table_maintenance_job_path(
            &report.job.table_bucket,
            &namespace,
            &table,
            &report.job.table_id,
            &report.job.job_id,
        );
        let latest_job_path =
            self.paths
                .table_maintenance_latest_job_path(&report.job.table_bucket, &namespace, &table, &report.job.table_id);
        let current_job_path =
            self.paths
                .table_maintenance_current_job_path(&report.job.table_bucket, &namespace, &table, &report.job.table_id);
        self.write_entry(self.catalog_bucket(), &job_path, report, TableCatalogPutPrecondition::Any)
            .await?;
        self.write_entry(self.catalog_bucket(), &latest_job_path, report, TableCatalogPutPrecondition::Any)
            .await?;
        self.write_entry(self.catalog_bucket(), &current_job_path, report, TableCatalogPutPrecondition::Any)
            .await
    }

    pub(crate) async fn get_table_metadata_maintenance_report(
        &self,
        table_bucket: &str,
        namespace: &str,
        table: &str,
        job_id: &str,
    ) -> TableCatalogStoreResult<Option<TableMetadataMaintenanceReport>> {
        let namespace = parse_namespace_for_store(namespace)?;
        let table = parse_table_for_store(table)?;
        let table_path = self.paths.table_entry_path(table_bucket, &namespace, &table);
        let Some((entry, _)) = self.read_entry::<TableEntry>(self.catalog_bucket(), &table_path).await? else {
            return Err(TableCatalogStoreError::NotFound(format!(
                "table {}/{}/{}",
                table_bucket,
                namespace.public_name(),
                table.as_str()
            )));
        };
        let job_path = match job_id {
            MAINTENANCE_JOB_ALIAS_LATEST => {
                self.paths
                    .table_maintenance_latest_job_path(table_bucket, &namespace, &table, &entry.table_id)
            }
            MAINTENANCE_JOB_ALIAS_CURRENT => {
                self.paths
                    .table_maintenance_current_job_path(table_bucket, &namespace, &table, &entry.table_id)
            }
            _ => self
                .paths
                .table_maintenance_job_path(table_bucket, &namespace, &table, &entry.table_id, job_id),
        };
        self.read_entry::<TableMetadataMaintenanceReport>(self.catalog_bucket(), &job_path)
            .await
            .map(|entry| entry.map(|(report, _)| report))
    }

    pub(crate) async fn run_table_metadata_maintenance_worker_once(
        &self,
        table_bucket: &str,
        namespace: &str,
        table: &str,
        worker_id: String,
    ) -> TableCatalogStoreResult<TableMetadataMaintenanceReport> {
        self.run_table_metadata_maintenance_worker_once_at(table_bucket, namespace, table, worker_id, OffsetDateTime::now_utc())
            .await
    }

    async fn run_table_metadata_maintenance_worker_once_at(
        &self,
        table_bucket: &str,
        namespace: &str,
        table: &str,
        worker_id: String,
        now: OffsetDateTime,
    ) -> TableCatalogStoreResult<TableMetadataMaintenanceReport> {
        let effective = self
            .get_effective_table_maintenance_config(table_bucket, namespace, table)
            .await?;
        if !effective.config.background_enabled {
            return self
                .put_table_metadata_maintenance_worker_control_report(TableMaintenanceWorkerControlReport {
                    table_bucket,
                    namespace,
                    table,
                    worker_id,
                    effective: &effective,
                    status: TableMetadataMaintenanceJobStatus::Disabled,
                    reason: "background maintenance is disabled",
                    now,
                })
                .await;
        }
        if effective.config.worker_paused {
            return self
                .put_table_metadata_maintenance_worker_control_report(TableMaintenanceWorkerControlReport {
                    table_bucket,
                    namespace,
                    table,
                    worker_id,
                    effective: &effective,
                    status: TableMetadataMaintenanceJobStatus::Paused,
                    reason: "background maintenance worker is paused",
                    now,
                })
                .await;
        }

        if let Some(current) = self
            .get_table_metadata_maintenance_report(table_bucket, namespace, table, MAINTENANCE_JOB_ALIAS_CURRENT)
            .await?
        {
            if matches!(current.job.status, TableMetadataMaintenanceJobStatus::Running) {
                if table_maintenance_job_lease_is_active(&current.job, effective.config.worker_lease_timeout_seconds, now) {
                    return Ok(current);
                }
                let mut expired = current;
                expired.job.status = TableMetadataMaintenanceJobStatus::Failed;
                expired.job.failure_reason = Some("maintenance worker lease expired".to_string());
                expired.job.finished_at = Some(maintenance_timestamp(now));
                self.put_table_metadata_maintenance_report(&expired).await?;
            } else if table_maintenance_job_retry_is_pending(&current.job, now) {
                return Ok(current);
            }
        }

        self.run_table_metadata_maintenance_with_config(
            table_bucket,
            namespace,
            table,
            effective.config.delete_enabled,
            Some(worker_id),
            effective,
        )
        .await
    }

    pub(crate) async fn heartbeat_table_metadata_maintenance_job(
        &self,
        table_bucket: &str,
        namespace: &str,
        table: &str,
        job_id: &str,
        lease_id: &str,
        worker_id: &str,
    ) -> TableCatalogStoreResult<TableMetadataMaintenanceReport> {
        self.heartbeat_table_metadata_maintenance_job_at(
            TableMaintenanceHeartbeatRef {
                table_bucket,
                namespace,
                table,
                job_id,
                lease_id,
                worker_id,
            },
            OffsetDateTime::now_utc(),
        )
        .await
    }

    async fn heartbeat_table_metadata_maintenance_job_at(
        &self,
        heartbeat: TableMaintenanceHeartbeatRef<'_>,
        now: OffsetDateTime,
    ) -> TableCatalogStoreResult<TableMetadataMaintenanceReport> {
        let namespace = parse_namespace_for_store(heartbeat.namespace)?;
        let table = parse_table_for_store(heartbeat.table)?;
        let table_path = self.paths.table_entry_path(heartbeat.table_bucket, &namespace, &table);
        let _guard = self.backend.acquire_write_lock(self.catalog_bucket(), &table_path).await?;
        let Some(mut report) = self
            .get_table_metadata_maintenance_report(
                heartbeat.table_bucket,
                &namespace.public_name(),
                table.as_str(),
                MAINTENANCE_JOB_ALIAS_CURRENT,
            )
            .await?
        else {
            return Err(TableCatalogStoreError::NotFound(format!(
                "maintenance job {}/{}/{}/{}",
                heartbeat.table_bucket,
                namespace.public_name(),
                table.as_str(),
                heartbeat.job_id
            )));
        };
        if report.job.job_id != heartbeat.job_id {
            return Err(TableCatalogStoreError::Conflict("maintenance job is not current".to_string()));
        }
        if !matches!(report.job.status, TableMetadataMaintenanceJobStatus::Running) {
            return Err(TableCatalogStoreError::Conflict("maintenance job is not running".to_string()));
        }
        if report.job.lease_id != heartbeat.lease_id {
            return Err(TableCatalogStoreError::Conflict("maintenance lease does not match".to_string()));
        }
        if report.job.worker_id.as_deref() != Some(heartbeat.worker_id) {
            return Err(TableCatalogStoreError::Conflict("maintenance worker does not match".to_string()));
        }

        report.job.heartbeat_at = Some(maintenance_timestamp(now));
        self.put_table_metadata_maintenance_report(&report).await?;
        Ok(report)
    }

    async fn put_table_metadata_maintenance_worker_control_report(
        &self,
        control: TableMaintenanceWorkerControlReport<'_>,
    ) -> TableCatalogStoreResult<TableMetadataMaintenanceReport> {
        let namespace = parse_namespace_for_store(control.namespace)?;
        let table = parse_table_for_store(control.table)?;
        let table_path = self.paths.table_entry_path(control.table_bucket, &namespace, &table);
        let Some((entry, _)) = self.read_entry::<TableEntry>(self.catalog_bucket(), &table_path).await? else {
            return Err(TableCatalogStoreError::NotFound(format!(
                "table {}/{}/{}",
                control.table_bucket,
                namespace.public_name(),
                table.as_str()
            )));
        };
        let timestamp = maintenance_timestamp(control.now);
        let cleanup_watermark_unix_seconds =
            (control.now - Duration::seconds(TABLE_METADATA_CLEANUP_SAFETY_WINDOW_SECONDS)).unix_timestamp();
        let current_metadata_location = entry.metadata_location.clone();
        let report = TableMetadataMaintenanceReport {
            job: TableMetadataMaintenanceJob {
                job_id: Uuid::new_v4().to_string(),
                table_bucket: control.table_bucket.to_string(),
                namespace: namespace.public_name(),
                table: table.as_str().to_string(),
                table_id: entry.table_id,
                operation: TableMetadataMaintenanceOperation::DryRun,
                status: control.status,
                failure_reason: Some(control.reason.to_string()),
                config_source: control.effective.source,
                worker_id: Some(control.worker_id),
                lease_id: String::new(),
                attempt: 0,
                max_retry_attempts: control.effective.config.max_retry_attempts,
                next_retry_after: None,
                quarantine_enabled: control.effective.config.quarantine_enabled,
                quarantine_retention_seconds: control.effective.config.quarantine_retention_seconds,
                heartbeat_at: None,
                started_at: Some(timestamp.clone()),
                finished_at: Some(timestamp),
                current_metadata_location: current_metadata_location.clone(),
                current_generation: entry.generation,
                retain_recent_metadata_files: control.effective.config.retain_recent_metadata_files,
                safety_window_seconds: TABLE_METADATA_CLEANUP_SAFETY_WINDOW_SECONDS,
                cleanup_watermark_unix_seconds,
                planned_metadata_file_count: 0,
                retained_metadata_file_count: 0,
                cleanup_candidate_count: 0,
                deletable_metadata_file_count: 0,
                deleted_metadata_file_count: 0,
                quarantined_object_count: 0,
            },
            current_metadata_location,
            retained_metadata_locations: Vec::new(),
            cleanup_candidate_locations: Vec::new(),
            deletable_metadata_locations: Vec::new(),
            object_reports: Vec::new(),
            referenced_object_reports: Vec::new(),
            reachability_graph: TableMaintenanceReachabilityGraphReport::default(),
            snapshot_expiration: None,
            compaction: None,
        };
        self.put_table_metadata_maintenance_report(&report).await?;
        Ok(report)
    }

    pub(crate) async fn plan_table_snapshot_expiration(
        &self,
        table_bucket: &str,
        namespace: &str,
        table: &str,
        config: TableSnapshotExpirationConfig,
    ) -> TableCatalogStoreResult<TableSnapshotExpirationReport> {
        validate_table_snapshot_expiration_config(&config)?;
        let namespace = parse_namespace_for_store(namespace)?;
        let table = parse_table_for_store(table)?;
        let table_path = self.paths.table_entry_path(table_bucket, &namespace, &table);
        let Some((entry, _)) = self.read_entry::<TableEntry>(self.catalog_bucket(), &table_path).await? else {
            return Err(TableCatalogStoreError::NotFound(format!(
                "table {}/{}/{}",
                table_bucket,
                namespace.public_name(),
                table.as_str()
            )));
        };
        if !is_valid_table_metadata_location(&namespace, &table, &entry.metadata_location) {
            return Err(TableCatalogStoreError::Invalid(
                "current metadata location must be inside the table metadata directory".to_string(),
            ));
        }

        let Some(current_metadata_object) = self.backend.read_object(table_bucket, &entry.metadata_location).await? else {
            return Err(TableCatalogStoreError::NotFound(format!(
                "current metadata object {}",
                entry.metadata_location
            )));
        };
        let current_metadata = serde_json::from_slice::<serde_json::Value>(&current_metadata_object.data).map_err(|err| {
            TableCatalogStoreError::Invalid(format!("failed to parse current metadata {}: {err}", entry.metadata_location))
        })?;
        if !current_metadata.is_object() {
            return Err(TableCatalogStoreError::Invalid(format!(
                "current metadata {} must be a JSON object",
                entry.metadata_location
            )));
        }

        Ok(table_snapshot_expiration_report(
            table_bucket,
            &namespace,
            &table,
            &entry,
            &current_metadata,
            config,
            OffsetDateTime::now_utc(),
        ))
    }

    pub(crate) async fn plan_table_compaction(
        &self,
        table_bucket: &str,
        namespace: &str,
        table: &str,
        config: TableCompactionPlanningConfig,
    ) -> TableCatalogStoreResult<TableCompactionPlanningReport> {
        validate_table_compaction_planning_config(&config)?;
        let namespace = parse_namespace_for_store(namespace)?;
        let table = parse_table_for_store(table)?;
        let table_path = self.paths.table_entry_path(table_bucket, &namespace, &table);
        let Some((entry, _)) = self.read_entry::<TableEntry>(self.catalog_bucket(), &table_path).await? else {
            return Err(TableCatalogStoreError::NotFound(format!(
                "table {}/{}/{}",
                table_bucket,
                namespace.public_name(),
                table.as_str()
            )));
        };
        if !is_valid_table_metadata_location(&namespace, &table, &entry.metadata_location) {
            return Err(TableCatalogStoreError::Invalid(
                "current metadata location must be inside the table metadata directory".to_string(),
            ));
        }

        let Some(current_metadata_object) = self.backend.read_object(table_bucket, &entry.metadata_location).await? else {
            return Err(TableCatalogStoreError::NotFound(format!(
                "current metadata object {}",
                entry.metadata_location
            )));
        };
        let current_metadata = serde_json::from_slice::<serde_json::Value>(&current_metadata_object.data).map_err(|err| {
            TableCatalogStoreError::Invalid(format!("failed to parse current metadata {}: {err}", entry.metadata_location))
        })?;
        if !current_metadata.is_object() {
            return Err(TableCatalogStoreError::Invalid(format!(
                "current metadata {} must be a JSON object",
                entry.metadata_location
            )));
        }

        Ok(table_compaction_planning_report(
            table_bucket,
            &namespace,
            &table,
            &entry,
            &current_metadata,
            config,
        ))
    }

    pub(crate) async fn export_table_catalog_entry(
        &self,
        table_bucket: &str,
        namespace: &str,
        table: &str,
    ) -> TableCatalogStoreResult<TableCatalogExport> {
        let namespace = parse_namespace_for_store(namespace)?;
        let table = parse_table_for_store(table)?;

        let Some((table_bucket_entry, _)) = self
            .read_entry::<TableBucketEntry>(self.catalog_bucket(), &self.paths.table_bucket_entry_path(table_bucket))
            .await?
        else {
            return Err(TableCatalogStoreError::NotFound(format!("table bucket {table_bucket}")));
        };
        let Some((namespace_entry, _)) = self
            .read_entry::<NamespaceEntry>(self.catalog_bucket(), &self.paths.namespace_entry_path(table_bucket, &namespace))
            .await?
        else {
            return Err(TableCatalogStoreError::NotFound(format!(
                "namespace {}/{}",
                table_bucket,
                namespace.public_name()
            )));
        };
        let Some((table_entry, _)) = self
            .read_entry::<TableEntry>(self.catalog_bucket(), &self.paths.table_entry_path(table_bucket, &namespace, &table))
            .await?
        else {
            return Err(TableCatalogStoreError::NotFound(format!(
                "table {}/{}/{}",
                table_bucket,
                namespace.public_name(),
                table.as_str()
            )));
        };

        Ok(TableCatalogExport {
            table_bucket: table_bucket_entry,
            namespace: namespace_entry,
            table: table_entry,
        })
    }

    pub(crate) async fn diagnose_table_catalog(
        &self,
        table_bucket: &str,
        namespace: &str,
        table: &str,
        retain_recent_metadata_files: usize,
    ) -> TableCatalogStoreResult<TableCatalogDiagnosticsReport> {
        let parsed_namespace = parse_namespace_for_store(namespace)?;
        let parsed_table = parse_table_for_store(table)?;
        let catalog = self.export_table_catalog_entry(table_bucket, namespace, table).await?;
        let current_metadata_location = catalog.table.metadata_location.clone();

        let mut retained = BTreeSet::new();
        let mut current_metadata_for_refs = None;
        let current_metadata_status =
            if is_valid_table_metadata_location(&parsed_namespace, &parsed_table, &current_metadata_location) {
                retained.insert(current_metadata_location.clone());
                match self.backend.read_object(table_bucket, &current_metadata_location).await? {
                    Some(current_metadata_object) => {
                        match serde_json::from_slice::<serde_json::Value>(&current_metadata_object.data) {
                            Ok(current_metadata) if current_metadata.is_object() => {
                                retained.extend(metadata_log_locations(&current_metadata, &parsed_namespace, &parsed_table));
                                current_metadata_for_refs = Some(current_metadata);
                                TableMetadataPointerStatus::Valid
                            }
                            Ok(_) | Err(_) => TableMetadataPointerStatus::InvalidJson,
                        }
                    }
                    None => TableMetadataPointerStatus::MissingObject,
                }
            } else {
                TableMetadataPointerStatus::InvalidLocation
            };

        let mut metadata_locations = Vec::new();
        let metadata_prefix = format!("{}/", default_table_metadata_dir_path(&parsed_namespace, &parsed_table));
        for object in self.backend.list_objects(table_bucket, &metadata_prefix).await? {
            if let Some(metadata_location) = metadata_location_from_metadata_file_path(&parsed_namespace, &parsed_table, &object)
            {
                metadata_locations.push(metadata_location);
            }
        }
        metadata_locations.sort();
        metadata_locations.dedup();

        for metadata_location in metadata_locations.iter().rev().take(retain_recent_metadata_files) {
            retained.insert(metadata_location.clone());
        }
        if let Some(current_metadata) = current_metadata_for_refs.as_ref() {
            retained.extend(
                metadata_locations_for_protected_snapshot_refs(
                    &self.backend,
                    table_bucket,
                    &parsed_namespace,
                    &parsed_table,
                    current_metadata,
                    &metadata_locations,
                )
                .await?,
            );
        }

        let orphan_metadata_candidate_locations = metadata_locations
            .into_iter()
            .filter(|metadata_location| !retained.contains(metadata_location))
            .collect();

        let commit_recovery = self.plan_table_commit_recovery(table_bucket, namespace, table).await?;
        let (recovery_status, recommended_actions) = table_catalog_recovery_summary(&current_metadata_status, &commit_recovery);

        Ok(TableCatalogDiagnosticsReport {
            catalog,
            current_metadata_status,
            recovery_status,
            recommended_actions,
            commit_recovery,
            orphan_metadata_candidate_locations,
        })
    }

    pub(crate) async fn plan_table_metadata_maintenance(
        &self,
        table_bucket: &str,
        namespace: &str,
        table: &str,
        retain_recent_metadata_files: usize,
    ) -> TableCatalogStoreResult<TableMetadataMaintenanceReport> {
        let namespace = parse_namespace_for_store(namespace)?;
        let table = parse_table_for_store(table)?;
        let table_path = self.paths.table_entry_path(table_bucket, &namespace, &table);
        let Some((entry, _)) = self.read_entry::<TableEntry>(self.catalog_bucket(), &table_path).await? else {
            return Err(TableCatalogStoreError::NotFound(format!(
                "table {}/{}/{}",
                table_bucket,
                namespace.public_name(),
                table.as_str()
            )));
        };
        if !is_valid_table_metadata_location(&namespace, &table, &entry.metadata_location) {
            return Err(TableCatalogStoreError::Invalid(
                "current metadata location must be inside the table metadata directory".to_string(),
            ));
        }

        let Some(current_metadata_object) = self.backend.read_object(table_bucket, &entry.metadata_location).await? else {
            return Err(TableCatalogStoreError::NotFound(format!(
                "current metadata object {}",
                entry.metadata_location
            )));
        };
        let current_metadata = serde_json::from_slice::<serde_json::Value>(&current_metadata_object.data).map_err(|err| {
            TableCatalogStoreError::Invalid(format!("failed to parse current metadata {}: {err}", entry.metadata_location))
        })?;
        if !current_metadata.is_object() {
            return Err(TableCatalogStoreError::Invalid(format!(
                "current metadata {} must be a JSON object",
                entry.metadata_location
            )));
        }

        let mut retained = BTreeSet::new();
        let mut maintenance_reasons = BTreeMap::<String, BTreeSet<TableMetadataMaintenanceReason>>::new();
        for metadata_location in metadata_log_locations(&current_metadata, &namespace, &table) {
            retained.insert(metadata_location.clone());
            insert_metadata_maintenance_reason(
                &mut maintenance_reasons,
                metadata_location,
                TableMetadataMaintenanceReason::MetadataLog,
            );
        }
        retained.insert(entry.metadata_location.clone());
        insert_metadata_maintenance_reason(
            &mut maintenance_reasons,
            entry.metadata_location.clone(),
            TableMetadataMaintenanceReason::CurrentMetadata,
        );

        let mut metadata_locations = Vec::new();
        let metadata_prefix = format!("{}/", default_table_metadata_dir_path(&namespace, &table));
        for object in self.backend.list_objects(table_bucket, &metadata_prefix).await? {
            if let Some(metadata_location) = metadata_location_from_metadata_file_path(&namespace, &table, &object) {
                metadata_locations.push(metadata_location);
            }
        }
        metadata_locations.sort();
        metadata_locations.dedup();
        let planned_metadata_file_count = metadata_locations.len();

        for metadata_location in metadata_locations.iter().rev().take(retain_recent_metadata_files) {
            retained.insert(metadata_location.clone());
            if metadata_location != &entry.metadata_location {
                insert_metadata_maintenance_reason(
                    &mut maintenance_reasons,
                    metadata_location.clone(),
                    TableMetadataMaintenanceReason::RecentMetadata,
                );
            }
        }
        for metadata_location in metadata_locations_for_protected_snapshot_refs(
            &self.backend,
            table_bucket,
            &namespace,
            &table,
            &current_metadata,
            &metadata_locations,
        )
        .await?
        {
            retained.insert(metadata_location.clone());
            insert_metadata_maintenance_reason(
                &mut maintenance_reasons,
                metadata_location,
                TableMetadataMaintenanceReason::ProtectedSnapshotRef,
            );
        }

        let cleanup_candidate_locations = metadata_locations
            .iter()
            .filter(|metadata_location| !retained.contains(metadata_location.as_str()))
            .cloned()
            .collect::<Vec<_>>();

        let now = OffsetDateTime::now_utc();
        let mut deletable_metadata_locations = Vec::new();
        for metadata_location in &cleanup_candidate_locations {
            insert_metadata_maintenance_reason(
                &mut maintenance_reasons,
                metadata_location.clone(),
                TableMetadataMaintenanceReason::NoCurrentReachability,
            );
            let Some(candidate_object) = self.backend.read_object(table_bucket, metadata_location).await? else {
                insert_metadata_maintenance_reason(
                    &mut maintenance_reasons,
                    metadata_location.clone(),
                    TableMetadataMaintenanceReason::SafetyWindowPending,
                );
                continue;
            };
            if metadata_candidate_is_past_safety_window(candidate_object.mod_time, now) {
                deletable_metadata_locations.push(metadata_location.clone());
                insert_metadata_maintenance_reason(
                    &mut maintenance_reasons,
                    metadata_location.clone(),
                    TableMetadataMaintenanceReason::SafetyWindowSatisfied,
                );
            } else {
                insert_metadata_maintenance_reason(
                    &mut maintenance_reasons,
                    metadata_location.clone(),
                    TableMetadataMaintenanceReason::SafetyWindowPending,
                );
            }
        }
        let current_metadata_location = entry.metadata_location;
        let retained_metadata_locations = retained.into_iter().collect::<Vec<_>>();
        let object_reports = metadata_maintenance_object_reports(maintenance_reasons);
        let referenced_object_reports = metadata_maintenance_referenced_object_reports(&current_metadata);
        let reachability_graph =
            metadata_maintenance_reachability_graph_report(planned_metadata_file_count, &referenced_object_reports);

        Ok(TableMetadataMaintenanceReport {
            job: TableMetadataMaintenanceJob {
                job_id: Uuid::new_v4().to_string(),
                table_bucket: table_bucket.to_string(),
                namespace: namespace.public_name(),
                table: table.as_str().to_string(),
                table_id: entry.table_id,
                operation: TableMetadataMaintenanceOperation::DryRun,
                status: TableMetadataMaintenanceJobStatus::Successful,
                failure_reason: None,
                config_source: TableMaintenanceConfigSource::Default,
                worker_id: None,
                lease_id: String::new(),
                attempt: 0,
                max_retry_attempts: 0,
                next_retry_after: None,
                quarantine_enabled: false,
                quarantine_retention_seconds: 0,
                heartbeat_at: None,
                started_at: None,
                finished_at: None,
                current_metadata_location: current_metadata_location.clone(),
                current_generation: entry.generation,
                retain_recent_metadata_files,
                safety_window_seconds: TABLE_METADATA_CLEANUP_SAFETY_WINDOW_SECONDS,
                cleanup_watermark_unix_seconds: (now - Duration::seconds(TABLE_METADATA_CLEANUP_SAFETY_WINDOW_SECONDS))
                    .unix_timestamp(),
                planned_metadata_file_count,
                retained_metadata_file_count: retained_metadata_locations.len(),
                cleanup_candidate_count: cleanup_candidate_locations.len(),
                deletable_metadata_file_count: deletable_metadata_locations.len(),
                deleted_metadata_file_count: 0,
                quarantined_object_count: 0,
            },
            current_metadata_location,
            retained_metadata_locations,
            cleanup_candidate_locations,
            deletable_metadata_locations,
            object_reports,
            referenced_object_reports,
            reachability_graph,
            snapshot_expiration: None,
            compaction: None,
        })
    }

    pub(crate) async fn delete_table_metadata_maintenance_candidates(
        &self,
        table_bucket: &str,
        namespace: &str,
        table: &str,
        retain_recent_metadata_files: usize,
    ) -> TableCatalogStoreResult<TableMetadataMaintenanceReport> {
        let report = self
            .plan_table_metadata_maintenance(table_bucket, namespace, table, retain_recent_metadata_files)
            .await?;
        self.delete_table_metadata_maintenance_report(table_bucket, namespace, table, report)
            .await
    }

    pub(crate) async fn run_table_metadata_maintenance(
        &self,
        table_bucket: &str,
        namespace: &str,
        table: &str,
        delete: bool,
        worker_id: Option<String>,
    ) -> TableCatalogStoreResult<TableMetadataMaintenanceReport> {
        let effective = self
            .get_effective_table_maintenance_config(table_bucket, namespace, table)
            .await?;
        self.run_table_metadata_maintenance_with_config(table_bucket, namespace, table, delete, worker_id, effective)
            .await
    }

    pub(crate) async fn run_table_metadata_maintenance_with_retention(
        &self,
        table_bucket: &str,
        namespace: &str,
        table: &str,
        delete: bool,
        worker_id: Option<String>,
        retain_recent_metadata_files: usize,
    ) -> TableCatalogStoreResult<TableMetadataMaintenanceReport> {
        let mut effective = self
            .get_effective_table_maintenance_config(table_bucket, namespace, table)
            .await?;
        effective.config.retain_recent_metadata_files = retain_recent_metadata_files;
        self.run_table_metadata_maintenance_with_config(table_bucket, namespace, table, delete, worker_id, effective)
            .await
    }

    async fn run_table_metadata_maintenance_with_config(
        &self,
        table_bucket: &str,
        namespace: &str,
        table: &str,
        delete: bool,
        worker_id: Option<String>,
        effective: TableMaintenanceEffectiveConfig,
    ) -> TableCatalogStoreResult<TableMetadataMaintenanceReport> {
        let mut report = self
            .plan_table_metadata_maintenance(table_bucket, namespace, table, effective.config.retain_recent_metadata_files)
            .await?;

        let started_at = maintenance_timestamp(OffsetDateTime::now_utc());
        report.job.operation = if delete {
            TableMetadataMaintenanceOperation::Delete
        } else {
            TableMetadataMaintenanceOperation::DryRun
        };
        report.job.status = TableMetadataMaintenanceJobStatus::Running;
        report.job.failure_reason = None;
        report.job.config_source = effective.source;
        report.job.worker_id = worker_id;
        report.job.lease_id = Uuid::new_v4().to_string();
        report.job.attempt = 1;
        report.job.max_retry_attempts = effective.config.max_retry_attempts;
        report.job.next_retry_after = None;
        report.job.quarantine_enabled = effective.config.quarantine_enabled;
        report.job.quarantine_retention_seconds = effective.config.quarantine_retention_seconds;
        report.job.heartbeat_at = Some(started_at.clone());
        report.job.started_at = Some(started_at);
        report.job.finished_at = None;
        self.put_table_metadata_maintenance_report(&report).await?;

        if delete && !effective.config.delete_enabled {
            let mut failed = report;
            failed.job.status = TableMetadataMaintenanceJobStatus::Failed;
            failed.job.failure_reason = Some("metadata delete is disabled by maintenance config".to_string());
            apply_maintenance_retry_after(&mut failed.job, &effective.config, OffsetDateTime::now_utc());
            failed.job.finished_at = Some(maintenance_timestamp(OffsetDateTime::now_utc()));
            self.put_table_metadata_maintenance_report(&failed).await?;
            return Ok(failed);
        }

        if delete {
            let running_report = report.clone();
            let mut deleted = match self
                .delete_table_metadata_maintenance_report(table_bucket, namespace, table, report)
                .await
            {
                Ok(report) => report,
                Err(err) => {
                    let mut failed = running_report;
                    failed.job.status = TableMetadataMaintenanceJobStatus::Failed;
                    failed.job.failure_reason = Some(err.to_string());
                    apply_maintenance_retry_after(&mut failed.job, &effective.config, OffsetDateTime::now_utc());
                    failed.job.finished_at = Some(maintenance_timestamp(OffsetDateTime::now_utc()));
                    self.put_table_metadata_maintenance_report(&failed).await?;
                    return Err(err);
                }
            };
            deleted.job.finished_at = Some(maintenance_timestamp(OffsetDateTime::now_utc()));
            self.put_table_metadata_maintenance_report(&deleted).await?;
            return Ok(deleted);
        }

        report.job.status = TableMetadataMaintenanceJobStatus::Successful;
        report.job.finished_at = Some(maintenance_timestamp(OffsetDateTime::now_utc()));
        self.put_table_metadata_maintenance_report(&report).await?;
        Ok(report)
    }

    async fn delete_table_metadata_maintenance_report(
        &self,
        table_bucket: &str,
        namespace: &str,
        table: &str,
        report: TableMetadataMaintenanceReport,
    ) -> TableCatalogStoreResult<TableMetadataMaintenanceReport> {
        let namespace = parse_namespace_for_store(namespace)?;
        let table = parse_table_for_store(table)?;
        if !is_valid_table_metadata_location(&namespace, &table, &report.current_metadata_location) {
            return Err(TableCatalogStoreError::Invalid(
                "maintenance report current metadata location must be inside the table metadata directory".to_string(),
            ));
        }

        let table_path = self.paths.table_entry_path(table_bucket, &namespace, &table);
        let _guard = self.backend.acquire_write_lock(self.catalog_bucket(), &table_path).await?;
        let Some((entry, _)) = self.read_table_with_etag_unlocked(table_bucket, &namespace, &table).await? else {
            return Err(TableCatalogStoreError::NotFound(format!(
                "table {}/{}/{}",
                table_bucket,
                namespace.public_name(),
                table.as_str()
            )));
        };
        if entry.metadata_location != report.current_metadata_location {
            return Err(TableCatalogStoreError::Conflict(
                "current metadata location changed before maintenance delete".to_string(),
            ));
        }

        let Some(current_metadata_object) = self.backend.read_object(table_bucket, &entry.metadata_location).await? else {
            return Err(TableCatalogStoreError::NotFound(format!(
                "current metadata object {}",
                entry.metadata_location
            )));
        };
        let current_metadata = serde_json::from_slice::<serde_json::Value>(&current_metadata_object.data).map_err(|err| {
            TableCatalogStoreError::Invalid(format!("failed to parse current metadata {}: {err}", entry.metadata_location))
        })?;
        if !current_metadata.is_object() {
            return Err(TableCatalogStoreError::Invalid(format!(
                "current metadata {} must be a JSON object",
                entry.metadata_location
            )));
        }

        let mut protected = metadata_log_locations(&current_metadata, &namespace, &table);
        protected.insert(entry.metadata_location.clone());
        protected.extend(report.retained_metadata_locations.iter().cloned());
        protected.extend(
            metadata_locations_for_protected_snapshot_refs(
                &self.backend,
                table_bucket,
                &namespace,
                &table,
                &current_metadata,
                &report.cleanup_candidate_locations,
            )
            .await?,
        );

        let cleanup_candidate_count = report.cleanup_candidate_locations.len();
        let planned_deletable_locations = report.deletable_metadata_locations.iter().cloned().collect::<BTreeSet<_>>();
        let mut cleanup_candidate_locations = BTreeSet::new();
        let now = OffsetDateTime::now_utc();
        for metadata_location in &report.cleanup_candidate_locations {
            if !is_valid_table_metadata_location(&namespace, &table, metadata_location) {
                return Err(TableCatalogStoreError::Invalid(format!(
                    "cleanup candidate {metadata_location} must be inside the table metadata directory"
                )));
            }
            if protected.contains(metadata_location.as_str()) {
                return Err(TableCatalogStoreError::Conflict(format!(
                    "cleanup candidate {metadata_location} is retained by current metadata"
                )));
            }
            let Some(candidate_object) = self.backend.read_object(table_bucket, metadata_location).await? else {
                continue;
            };
            if !planned_deletable_locations.contains(metadata_location.as_str()) {
                continue;
            }
            if !metadata_candidate_is_past_safety_window(candidate_object.mod_time, now) {
                continue;
            }
            cleanup_candidate_locations.insert(metadata_location.clone());
        }

        let cleanup_candidate_locations = cleanup_candidate_locations.into_iter().collect::<Vec<_>>();
        let deleted_locations = cleanup_candidate_locations.iter().cloned().collect::<BTreeSet<_>>();
        for metadata_location in &cleanup_candidate_locations {
            self.backend.delete_object(table_bucket, metadata_location).await?;
        }
        let retained_metadata_locations = protected.into_iter().collect::<Vec<_>>();
        let mut job = report.job;
        job.operation = TableMetadataMaintenanceOperation::Delete;
        job.status = TableMetadataMaintenanceJobStatus::Successful;
        job.failure_reason = None;
        job.retained_metadata_file_count = retained_metadata_locations.len();
        job.cleanup_candidate_count = cleanup_candidate_count;
        job.deletable_metadata_file_count = planned_deletable_locations.len();
        job.deleted_metadata_file_count = cleanup_candidate_locations.len();
        let mut object_reports = report.object_reports;
        mark_deleted_metadata_object_reports(&mut object_reports, &deleted_locations);
        let referenced_object_reports = report.referenced_object_reports;

        Ok(TableMetadataMaintenanceReport {
            job,
            current_metadata_location: entry.metadata_location,
            retained_metadata_locations,
            cleanup_candidate_locations: cleanup_candidate_locations.clone(),
            deletable_metadata_locations: cleanup_candidate_locations,
            object_reports,
            referenced_object_reports,
            reachability_graph: report.reachability_graph,
            snapshot_expiration: report.snapshot_expiration,
            compaction: report.compaction,
        })
    }
}

#[async_trait::async_trait]
impl<B> TableCatalogStore for ObjectTableCatalogStore<B>
where
    B: TableCatalogObjectBackend,
{
    async fn get_table_bucket(&self, table_bucket: &str) -> TableCatalogStoreResult<Option<TableBucketEntry>> {
        self.read_entry::<TableBucketEntry>(self.catalog_bucket(), &self.paths.table_bucket_entry_path(table_bucket))
            .await
            .map(|entry| entry.map(|(bucket, _)| bucket))
    }

    async fn put_table_bucket(&self, entry: TableBucketEntry) -> TableCatalogStoreResult<()> {
        validate_catalog_entry_version("table bucket", entry.version)?;
        if entry.table_bucket.is_empty() {
            return Err(TableCatalogStoreError::Invalid("table bucket name cannot be empty".to_string()));
        }
        if entry.catalog_type != TABLE_BUCKET_CATALOG_TYPE {
            return Err(TableCatalogStoreError::Invalid("unsupported table bucket catalog type".to_string()));
        }

        self.write_entry(
            self.catalog_bucket(),
            &self.paths.table_bucket_entry_path(&entry.table_bucket),
            &entry,
            TableCatalogPutPrecondition::Any,
        )
        .await
    }

    async fn create_namespace(&self, entry: NamespaceEntry) -> TableCatalogStoreResult<()> {
        validate_catalog_entry_version("namespace", entry.version)?;
        self.require_table_bucket(&entry.table_bucket).await?;
        let namespace = parse_namespace_for_store(&entry.namespace)?;
        let object = self.paths.namespace_entry_path(&entry.table_bucket, &namespace);
        self.write_entry(self.catalog_bucket(), &object, &entry, TableCatalogPutPrecondition::IfAbsent)
            .await
    }

    async fn list_namespaces(&self, table_bucket: &str) -> TableCatalogStoreResult<Vec<NamespaceEntry>> {
        let mut entries = Vec::new();
        for object in self
            .backend
            .list_objects(self.catalog_bucket(), &self.paths.namespace_entries_prefix(table_bucket))
            .await?
        {
            if !object.ends_with(NAMESPACE_ENTRY_FILE) {
                continue;
            }
            if let Some((entry, _)) = self.read_entry::<NamespaceEntry>(self.catalog_bucket(), &object).await? {
                entries.push(entry);
            }
        }
        entries.sort_by(|left, right| left.namespace.cmp(&right.namespace));
        Ok(entries)
    }

    async fn get_namespace(&self, table_bucket: &str, namespace: &str) -> TableCatalogStoreResult<Option<NamespaceEntry>> {
        let namespace = parse_namespace_for_store(namespace)?;
        self.read_entry::<NamespaceEntry>(self.catalog_bucket(), &self.paths.namespace_entry_path(table_bucket, &namespace))
            .await
            .map(|entry| entry.map(|(namespace, _)| namespace))
    }

    async fn drop_namespace(&self, table_bucket: &str, namespace: &str) -> TableCatalogStoreResult<()> {
        let namespace = parse_namespace_for_store(namespace)?;
        if self.get_namespace(table_bucket, &namespace.public_name()).await?.is_none() {
            return Err(TableCatalogStoreError::NotFound(format!(
                "namespace {}/{}",
                table_bucket,
                namespace.public_name()
            )));
        }
        if !self.list_tables(table_bucket, &namespace.public_name()).await?.is_empty() {
            return Err(TableCatalogStoreError::Conflict(format!(
                "namespace {}/{} is not empty",
                table_bucket,
                namespace.public_name()
            )));
        }
        self.backend
            .delete_object(self.catalog_bucket(), &self.paths.namespace_entry_path(table_bucket, &namespace))
            .await
    }

    async fn create_table(&self, entry: TableEntry) -> TableCatalogStoreResult<()> {
        self.write_table_entry(entry, TableCatalogPutPrecondition::IfAbsent).await
    }

    async fn register_table(&self, entry: TableEntry) -> TableCatalogStoreResult<()> {
        self.write_table_entry(entry, TableCatalogPutPrecondition::IfAbsent).await
    }

    async fn list_tables(&self, table_bucket: &str, namespace: &str) -> TableCatalogStoreResult<Vec<TableEntry>> {
        let namespace = parse_namespace_for_store(namespace)?;
        let mut entries = Vec::new();
        for object in self
            .backend
            .list_objects(self.catalog_bucket(), &self.paths.table_entries_prefix(table_bucket, &namespace))
            .await?
        {
            if !object.ends_with(TABLE_ENTRY_FILE) {
                continue;
            }
            if let Some((entry, _)) = self.read_entry::<TableEntry>(self.catalog_bucket(), &object).await? {
                entries.push(entry);
            }
        }
        entries.sort_by(|left, right| left.table.cmp(&right.table));
        Ok(entries)
    }

    async fn load_table(&self, table_bucket: &str, namespace: &str, table: &str) -> TableCatalogStoreResult<Option<TableEntry>> {
        let namespace = parse_namespace_for_store(namespace)?;
        let table = parse_table_for_store(table)?;
        self.read_entry::<TableEntry>(self.catalog_bucket(), &self.paths.table_entry_path(table_bucket, &namespace, &table))
            .await
            .map(|entry| entry.map(|(table, _)| table))
    }

    async fn commit_table(&self, request: TableCommitRequest) -> TableCatalogStoreResult<TableCommitResult> {
        let commit_started = Instant::now();
        record_table_commit_attempt(&request.operation);
        let namespace = parse_namespace_for_store(&request.namespace)?;
        let table = parse_table_for_store(&request.table)?;
        let table_path = self.paths.table_entry_path(&request.table_bucket, &namespace, &table);
        let _guard = self.backend.acquire_write_lock(self.catalog_bucket(), &table_path).await?;

        let Some((current, current_etag)) = self
            .read_table_with_etag_unlocked(&request.table_bucket, &namespace, &table)
            .await?
        else {
            return table_commit_result(
                &request.table_bucket,
                &request.namespace,
                &request.table,
                &request.commit_id,
                &request.operation,
                commit_started,
                Err(TableCatalogStoreError::NotFound(format!(
                    "table {}/{}/{}",
                    request.table_bucket, request.namespace, request.table
                ))),
            );
        };

        let commit_path = self
            .paths
            .commit_log_entry_path(&request.table_bucket, &current.table_id, &request.commit_id);
        let existing_commit = self.read_commit_by_path(&commit_path).await?;
        let idempotency_path = request.idempotency_key.as_deref().map(|idempotency_key| {
            self.paths
                .commit_idempotency_entry_path(&request.table_bucket, &current.table_id, idempotency_key)
        });
        let existing_idempotency_commit = match idempotency_path.as_deref() {
            Some(idempotency_path) => self.read_commit_by_path(idempotency_path).await?,
            None => None,
        };

        if let Some(existing) = existing_commit.as_ref() {
            if !commit_log_matches_request(existing, &request, &current.table_id) {
                return table_commit_result(
                    &request.table_bucket,
                    &request.namespace,
                    &request.table,
                    &request.commit_id,
                    &request.operation,
                    commit_started,
                    Err(TableCatalogStoreError::Conflict(format!(
                        "commit id already exists: {}",
                        request.commit_id
                    ))),
                );
            }
            if matches!(existing.status, CommitLogStatus::Committed) || table_matches_committed_log(&current, existing) {
                let mut committed = existing.clone();
                committed.status = CommitLogStatus::Committed;
                let _ = self
                    .finalize_commit_log(&commit_path, idempotency_path.as_deref(), &committed)
                    .await;
                return table_commit_result(
                    &request.table_bucket,
                    &request.namespace,
                    &request.table,
                    &request.commit_id,
                    &request.operation,
                    commit_started,
                    Ok(TableCommitResult {
                        table: current,
                        commit_log: committed,
                    }),
                );
            }
            if !matches!(existing.status, CommitLogStatus::Staged) || !table_matches_staged_base(&current, existing) {
                return table_commit_result(
                    &request.table_bucket,
                    &request.namespace,
                    &request.table,
                    &request.commit_id,
                    &request.operation,
                    commit_started,
                    Err(TableCatalogStoreError::Conflict(
                        "existing commit record does not match current table state".to_string(),
                    )),
                );
            }
        }
        if let Some(existing) = existing_idempotency_commit.as_ref()
            && !commit_log_matches_request(existing, &request, &current.table_id)
        {
            return table_commit_result(
                &request.table_bucket,
                &request.namespace,
                &request.table,
                &request.commit_id,
                &request.operation,
                commit_started,
                Err(TableCatalogStoreError::Conflict("idempotency key already exists".to_string())),
            );
        }
        if existing_commit.is_none() && existing_idempotency_commit.is_some() {
            return table_commit_result(
                &request.table_bucket,
                &request.namespace,
                &request.table,
                &request.commit_id,
                &request.operation,
                commit_started,
                Err(TableCatalogStoreError::Conflict(
                    "idempotency key exists without a recoverable commit record".to_string(),
                )),
            );
        }

        if current.version_token != request.expected_version_token {
            return table_commit_result(
                &request.table_bucket,
                &request.namespace,
                &request.table,
                &request.commit_id,
                &request.operation,
                commit_started,
                Err(TableCatalogStoreError::Conflict(
                    "current table version token does not match expected token".to_string(),
                )),
            );
        }
        if current.metadata_location != request.expected_metadata_location {
            return table_commit_result(
                &request.table_bucket,
                &request.namespace,
                &request.table,
                &request.commit_id,
                &request.operation,
                commit_started,
                Err(TableCatalogStoreError::Conflict(
                    "current table metadata location does not match expected location".to_string(),
                )),
            );
        }
        if !is_valid_table_metadata_location(&namespace, &table, &request.new_metadata_location) {
            return table_commit_result(
                &request.table_bucket,
                &request.namespace,
                &request.table,
                &request.commit_id,
                &request.operation,
                commit_started,
                Err(TableCatalogStoreError::Invalid(
                    "new metadata location must be inside the table metadata directory".to_string(),
                )),
            );
        }
        let Some(new_metadata_object) = self
            .backend
            .read_object(&request.table_bucket, &request.new_metadata_location)
            .await?
        else {
            return table_commit_result(
                &request.table_bucket,
                &request.namespace,
                &request.table,
                &request.commit_id,
                &request.operation,
                commit_started,
                Err(TableCatalogStoreError::NotFound(format!(
                    "new metadata object {}",
                    request.new_metadata_location
                ))),
            );
        };
        let next_warehouse_location =
            metadata_warehouse_location(&request.table_bucket, &request.new_metadata_location, &new_metadata_object)?;

        let has_existing_commit = existing_commit.is_some();
        let mut staged_commit_log = existing_commit.unwrap_or_else(|| CommitLogEntry {
            version: TABLE_CATALOG_ENTRY_VERSION,
            commit_id: request.commit_id.clone(),
            idempotency_key: request.idempotency_key.clone(),
            table_id: current.table_id.clone(),
            operation: request.operation.clone(),
            expected_version_token: request.expected_version_token.clone(),
            new_version_token: format!("token-{}", Uuid::new_v4()),
            previous_metadata_location: current.metadata_location.clone(),
            new_metadata_location: request.new_metadata_location.clone(),
            requirements: request.requirements.clone(),
            status: CommitLogStatus::Staged,
            writer: request.writer.clone(),
            created_at: None,
            updated_at: None,
        });
        staged_commit_log.status = CommitLogStatus::Staged;

        let mut next = current.clone();
        next.metadata_location = staged_commit_log.new_metadata_location.clone();
        if let Some(warehouse_location) = next_warehouse_location {
            next.warehouse_location = warehouse_location;
        }
        next.version_token = staged_commit_log.new_version_token.clone();
        next.generation = current.generation.saturating_add(1);

        if !has_existing_commit {
            self.write_entry(
                self.catalog_bucket(),
                &commit_path,
                &staged_commit_log,
                TableCatalogPutPrecondition::IfAbsent,
            )
            .await?;
        }
        if let Some(idempotency_path) = idempotency_path.as_deref()
            && existing_idempotency_commit.is_none()
        {
            self.write_entry(
                self.catalog_bucket(),
                idempotency_path,
                &staged_commit_log,
                TableCatalogPutPrecondition::IfAbsent,
            )
            .await?;
        }

        let cas_started = Instant::now();
        let cas_result = self
            .write_entry_unlocked(
                self.catalog_bucket(),
                &table_path,
                &next,
                TableCatalogPutPrecondition::IfMatch(current_etag),
            )
            .await;
        record_table_commit_cas_result(&request.operation, cas_started, &cas_result);
        if let Err(err) = cas_result {
            return table_commit_result(
                &request.table_bucket,
                &request.namespace,
                &request.table,
                &request.commit_id,
                &request.operation,
                commit_started,
                Err(err),
            );
        }

        let mut commit_log = staged_commit_log;
        commit_log.status = CommitLogStatus::Committed;
        // After the table CAS succeeds, the staged record is the durable recovery source.
        // A finalization failure must not turn an externally committed pointer into a failed commit response.
        let _ = self
            .finalize_commit_log(&commit_path, idempotency_path.as_deref(), &commit_log)
            .await;

        table_commit_result(
            &request.table_bucket,
            &request.namespace,
            &request.table,
            &request.commit_id,
            &request.operation,
            commit_started,
            Ok(TableCommitResult { table: next, commit_log }),
        )
    }

    async fn drop_table(&self, table_bucket: &str, namespace: &str, table: &str) -> TableCatalogStoreResult<()> {
        let namespace = parse_namespace_for_store(namespace)?;
        let table = parse_table_for_store(table)?;
        let object = self.paths.table_entry_path(table_bucket, &namespace, &table);
        if self
            .load_table(table_bucket, &namespace.public_name(), table.as_str())
            .await?
            .is_none()
        {
            return Err(TableCatalogStoreError::NotFound(format!(
                "table {}/{}/{}",
                table_bucket,
                namespace.public_name(),
                table.as_str()
            )));
        }
        self.backend.delete_object(self.catalog_bucket(), &object).await
    }

    async fn get_commit_by_id(
        &self,
        table_bucket: &str,
        table_id: &str,
        commit_id: &str,
    ) -> TableCatalogStoreResult<Option<CommitLogEntry>> {
        let object = self.paths.commit_log_entry_path(table_bucket, table_id, commit_id);
        self.read_commit_by_path(&object).await
    }

    async fn get_commit_by_idempotency_key(
        &self,
        table_bucket: &str,
        table_id: &str,
        idempotency_key: &str,
    ) -> TableCatalogStoreResult<Option<CommitLogEntry>> {
        let object = self
            .paths
            .commit_idempotency_entry_path(table_bucket, table_id, idempotency_key);
        self.read_commit_by_path(&object).await
    }
}

pub(crate) struct EcStoreTableCatalogObjectBackend<S> {
    store: Arc<S>,
}

impl<S> Clone for EcStoreTableCatalogObjectBackend<S> {
    fn clone(&self) -> Self {
        Self {
            store: self.store.clone(),
        }
    }
}

impl<S> EcStoreTableCatalogObjectBackend<S>
where
    S: StorageAPI + NamespaceLocking,
{
    pub fn new(store: Arc<S>) -> Self {
        Self { store }
    }
}

pub(crate) type EcStoreTableCatalogStore<S> = ObjectTableCatalogStore<EcStoreTableCatalogObjectBackend<S>>;

#[async_trait::async_trait]
impl<S> TableCatalogObjectBackend for EcStoreTableCatalogObjectBackend<S>
where
    S: StorageAPI + NamespaceLocking,
{
    async fn read_object(&self, bucket: &str, object: &str) -> TableCatalogStoreResult<Option<TableCatalogObject>> {
        self.read_object_with_options(bucket, object, ObjectOptions::default()).await
    }

    async fn read_object_unlocked(&self, bucket: &str, object: &str) -> TableCatalogStoreResult<Option<TableCatalogObject>> {
        self.read_object_with_options(
            bucket,
            object,
            ObjectOptions {
                no_lock: true,
                ..Default::default()
            },
        )
        .await
    }

    async fn object_exists(&self, bucket: &str, object: &str) -> TableCatalogStoreResult<bool> {
        match self.store.get_object_info(bucket, object, &ObjectOptions::default()).await {
            Ok(_) => Ok(true),
            Err(err) if is_missing_storage_error(&err) => Ok(false),
            Err(err) => Err(storage_error_to_catalog("check catalog object", err)),
        }
    }

    async fn put_object(
        &self,
        bucket: &str,
        object: &str,
        data: Vec<u8>,
        precondition: TableCatalogPutPrecondition,
    ) -> TableCatalogStoreResult<()> {
        self.put_object_with_options(bucket, object, data, precondition, false).await
    }

    async fn put_object_unlocked(
        &self,
        bucket: &str,
        object: &str,
        data: Vec<u8>,
        precondition: TableCatalogPutPrecondition,
    ) -> TableCatalogStoreResult<()> {
        self.put_object_with_options(bucket, object, data, precondition, true).await
    }

    async fn delete_object(&self, bucket: &str, object: &str) -> TableCatalogStoreResult<()> {
        match self.store.delete_object(bucket, object, ObjectOptions::default()).await {
            Ok(_) => Ok(()),
            Err(err) if is_missing_storage_error(&err) => Ok(()),
            Err(err) => Err(storage_error_to_catalog("delete catalog object", err)),
        }
    }

    async fn list_objects(&self, bucket: &str, prefix: &str) -> TableCatalogStoreResult<Vec<String>> {
        let mut continuation = None;
        let mut objects = BTreeSet::new();

        loop {
            let result = self
                .store
                .clone()
                .list_objects_v2(bucket, prefix, continuation, None, TABLE_CATALOG_LIST_MAX_KEYS, false, None, false)
                .await
                .map_err(|err| storage_error_to_catalog("list catalog objects", err))?;

            for object in result.objects {
                objects.insert(object.name);
            }

            if !result.is_truncated {
                break;
            }

            let Some(next) = result.next_continuation_token else {
                break;
            };
            continuation = Some(next);
        }

        Ok(objects.into_iter().collect())
    }

    async fn acquire_write_lock(&self, bucket: &str, object: &str) -> TableCatalogStoreResult<Box<dyn Send>> {
        let lock = self
            .store
            .new_ns_lock(bucket, object)
            .await
            .map_err(|err| storage_error_to_catalog("create catalog table lock", err))?;
        let guard = lock
            .get_write_lock(get_lock_acquire_timeout())
            .await
            .map_err(|err| TableCatalogStoreError::Internal(format!("failed to acquire catalog table lock: {err}")))?;
        Ok(Box::new(guard))
    }
}

impl<S> EcStoreTableCatalogObjectBackend<S>
where
    S: StorageAPI + NamespaceLocking,
{
    async fn read_object_with_options(
        &self,
        bucket: &str,
        object: &str,
        opts: ObjectOptions,
    ) -> TableCatalogStoreResult<Option<TableCatalogObject>> {
        let info = match self.store.get_object_info(bucket, object, &opts).await {
            Ok(info) => info,
            Err(err) if is_missing_storage_error(&err) => return Ok(None),
            Err(err) => return Err(storage_error_to_catalog("read catalog object info", err)),
        };
        let mut reader = match self
            .store
            .get_object_reader(bucket, object, None, HeaderMap::new(), &opts)
            .await
        {
            Ok(reader) => reader,
            Err(err) if is_missing_storage_error(&err) => return Ok(None),
            Err(err) => return Err(storage_error_to_catalog("read catalog object", err)),
        };
        let mut data = Vec::new();
        reader
            .stream
            .read_to_end(&mut data)
            .await
            .map_err(|err| TableCatalogStoreError::Internal(format!("failed to read catalog object {bucket}/{object}: {err}")))?;
        Ok(Some(TableCatalogObject {
            data,
            etag: info.etag,
            mod_time: info.mod_time,
        }))
    }

    async fn put_object_with_options(
        &self,
        bucket: &str,
        object: &str,
        data: Vec<u8>,
        precondition: TableCatalogPutPrecondition,
        no_lock: bool,
    ) -> TableCatalogStoreResult<()> {
        let mut reader = PutObjReader::from_vec(data);
        let opts = ObjectOptions {
            http_preconditions: http_preconditions_for_catalog_put(precondition),
            no_lock,
            ..Default::default()
        };
        self.store
            .put_object(bucket, object, &mut reader, &opts)
            .await
            .map(|_| ())
            .map_err(|err| storage_error_to_catalog("write catalog object", err))
    }
}

fn parse_namespace_for_store(namespace: &str) -> TableCatalogStoreResult<Namespace> {
    Namespace::parse(namespace).map_err(|err| TableCatalogStoreError::Invalid(format!("invalid namespace: {err}")))
}

fn parse_table_for_store(table: &str) -> TableCatalogStoreResult<IdentifierSegment> {
    IdentifierSegment::parse(table).map_err(|err| TableCatalogStoreError::Invalid(format!("invalid table name: {err}")))
}

fn insert_metadata_maintenance_reason(
    reasons_by_location: &mut BTreeMap<String, BTreeSet<TableMetadataMaintenanceReason>>,
    metadata_location: String,
    reason: TableMetadataMaintenanceReason,
) {
    reasons_by_location.entry(metadata_location).or_default().insert(reason);
}

fn metadata_maintenance_object_reports(
    reasons_by_location: BTreeMap<String, BTreeSet<TableMetadataMaintenanceReason>>,
) -> Vec<TableMetadataMaintenanceObjectReport> {
    reasons_by_location
        .into_iter()
        .map(|(metadata_location, reasons)| {
            let reasons = reasons.into_iter().collect::<Vec<_>>();
            let state = if reasons.contains(&TableMetadataMaintenanceReason::SafetyWindowSatisfied) {
                TableMetadataMaintenanceObjectState::Deletable
            } else if reasons.contains(&TableMetadataMaintenanceReason::SafetyWindowPending) {
                TableMetadataMaintenanceObjectState::PendingSafetyWindow
            } else {
                TableMetadataMaintenanceObjectState::Retained
            };
            TableMetadataMaintenanceObjectReport {
                metadata_location,
                state,
                reasons,
            }
        })
        .collect()
}

fn metadata_maintenance_referenced_object_reports(
    current_metadata: &serde_json::Value,
) -> Vec<TableMetadataMaintenanceReferencedObjectReport> {
    let mut reasons_by_location = BTreeMap::<String, BTreeSet<TableMetadataMaintenanceReason>>::new();
    if let Some(snapshots) = current_metadata.get("snapshots").and_then(serde_json::Value::as_array) {
        for snapshot in snapshots {
            let Some(manifest_list) = snapshot.get("manifest-list").and_then(serde_json::Value::as_str) else {
                continue;
            };
            reasons_by_location.entry(manifest_list.to_string()).or_default().extend([
                TableMetadataMaintenanceReason::ManifestList,
                TableMetadataMaintenanceReason::UnsupportedManifestAvro,
            ]);
        }
    }

    reasons_by_location
        .into_iter()
        .map(|(object_location, reasons)| TableMetadataMaintenanceReferencedObjectReport {
            object_location,
            object_kind: TableMetadataMaintenanceObjectKind::ManifestList,
            state: TableMetadataMaintenanceObjectState::ManualReviewRequired,
            reasons: reasons.into_iter().collect(),
        })
        .collect()
}

fn metadata_maintenance_reachability_graph_report(
    metadata_file_count: usize,
    referenced_object_reports: &[TableMetadataMaintenanceReferencedObjectReport],
) -> TableMaintenanceReachabilityGraphReport {
    let manifest_list_count = referenced_object_reports
        .iter()
        .filter(|report| report.object_kind == TableMetadataMaintenanceObjectKind::ManifestList)
        .count();
    let manifest_file_count = referenced_object_reports
        .iter()
        .filter(|report| report.object_kind == TableMetadataMaintenanceObjectKind::ManifestFile)
        .count();
    let data_file_count = referenced_object_reports
        .iter()
        .filter(|report| report.object_kind == TableMetadataMaintenanceObjectKind::DataFile)
        .count();
    let delete_file_count = referenced_object_reports
        .iter()
        .filter(|report| report.object_kind == TableMetadataMaintenanceObjectKind::DeleteFile)
        .count();
    let manual_review_count = referenced_object_reports
        .iter()
        .filter(|report| report.state == TableMetadataMaintenanceObjectState::ManualReviewRequired)
        .count();
    let mut reasons = BTreeSet::from([TableMaintenanceReachabilityGraphReason::MetadataJsonParsed]);
    if manifest_list_count > 0 {
        reasons.insert(TableMaintenanceReachabilityGraphReason::ManifestListAvroReferenced);
        reasons.insert(TableMaintenanceReachabilityGraphReason::ManifestAvroReaderUnavailable);
    }
    reasons.insert(TableMaintenanceReachabilityGraphReason::DataFileCleanupDeferred);
    reasons.insert(TableMaintenanceReachabilityGraphReason::DeleteFileCleanupDeferred);

    TableMaintenanceReachabilityGraphReport {
        status: if manual_review_count == 0 {
            TableMaintenanceReachabilityGraphStatus::Complete
        } else {
            TableMaintenanceReachabilityGraphStatus::ManualReviewRequired
        },
        metadata_file_count,
        manifest_list_count,
        manifest_file_count,
        data_file_count,
        delete_file_count,
        manual_review_count,
        reasons: reasons.into_iter().collect(),
    }
}

fn mark_deleted_metadata_object_reports(
    object_reports: &mut [TableMetadataMaintenanceObjectReport],
    deleted_locations: &BTreeSet<String>,
) {
    for object_report in object_reports {
        if !deleted_locations.contains(&object_report.metadata_location) {
            continue;
        }
        object_report.state = TableMetadataMaintenanceObjectState::Deleted;
        if !object_report
            .reasons
            .contains(&TableMetadataMaintenanceReason::DeletedByMaintenance)
        {
            object_report
                .reasons
                .push(TableMetadataMaintenanceReason::DeletedByMaintenance);
        }
    }
}

fn metadata_log_locations(
    current_metadata: &serde_json::Value,
    namespace: &Namespace,
    table: &IdentifierSegment,
) -> BTreeSet<String> {
    let mut locations = BTreeSet::new();
    let Some(metadata_log) = current_metadata.get("metadata-log").and_then(serde_json::Value::as_array) else {
        return locations;
    };

    for entry in metadata_log {
        let Some(metadata_location) = entry.get("metadata-file").and_then(serde_json::Value::as_str) else {
            continue;
        };
        if is_valid_table_metadata_location(namespace, table, metadata_location) {
            locations.insert(metadata_location.to_string());
        }
    }

    locations
}

async fn metadata_locations_for_protected_snapshot_refs<B>(
    backend: &B,
    table_bucket: &str,
    namespace: &Namespace,
    table: &IdentifierSegment,
    current_metadata: &serde_json::Value,
    metadata_locations: &[String],
) -> TableCatalogStoreResult<BTreeSet<String>>
where
    B: TableCatalogObjectBackend,
{
    let protected_snapshot_ids = protected_ref_snapshot_ids(current_metadata);
    if protected_snapshot_ids.is_empty() {
        return Ok(BTreeSet::new());
    }

    let mut retained = BTreeSet::new();
    for metadata_location in metadata_locations {
        if !is_valid_table_metadata_location(namespace, table, metadata_location) {
            continue;
        }
        let Some(metadata_object) = backend.read_object(table_bucket, metadata_location).await? else {
            continue;
        };
        let Ok(metadata) = serde_json::from_slice::<serde_json::Value>(&metadata_object.data) else {
            continue;
        };
        if metadata_contains_protected_snapshot_ref(&metadata, &protected_snapshot_ids) {
            retained.insert(metadata_location.clone());
        }
    }
    Ok(retained)
}

fn protected_ref_snapshot_ids(current_metadata: &serde_json::Value) -> BTreeSet<i64> {
    let mut snapshot_ids = BTreeSet::new();
    let current_snapshot_id = current_metadata
        .get("current-snapshot-id")
        .and_then(serde_json::Value::as_i64);
    let Some(refs) = current_metadata.get("refs").and_then(serde_json::Value::as_object) else {
        return snapshot_ids;
    };

    for reference in refs.values() {
        if let Some(snapshot_id) = reference.get("snapshot-id").and_then(serde_json::Value::as_i64)
            && Some(snapshot_id) != current_snapshot_id
        {
            snapshot_ids.insert(snapshot_id);
        }
    }
    snapshot_ids
}

fn metadata_contains_protected_snapshot_ref(metadata: &serde_json::Value, protected_snapshot_ids: &BTreeSet<i64>) -> bool {
    let current_snapshot_matches = metadata
        .get("current-snapshot-id")
        .and_then(serde_json::Value::as_i64)
        .is_some_and(|snapshot_id| protected_snapshot_ids.contains(&snapshot_id));
    if current_snapshot_matches {
        return true;
    }

    let Some(refs) = metadata.get("refs").and_then(serde_json::Value::as_object) else {
        return false;
    };
    refs.values().any(|reference| {
        reference
            .get("snapshot-id")
            .and_then(serde_json::Value::as_i64)
            .is_some_and(|snapshot_id| protected_snapshot_ids.contains(&snapshot_id))
    })
}

fn metadata_candidate_is_past_safety_window(mod_time: Option<OffsetDateTime>, now: OffsetDateTime) -> bool {
    let Some(mod_time) = mod_time else {
        return false;
    };
    mod_time <= now - Duration::seconds(TABLE_METADATA_CLEANUP_SAFETY_WINDOW_SECONDS)
}

#[derive(Debug)]
struct TableSnapshotExpirationDraft {
    snapshot_id: Option<i64>,
    sequence_number: Option<i64>,
    timestamp_ms: Option<i64>,
    manifest_list: Option<String>,
    reasons: BTreeSet<TableSnapshotExpirationReason>,
}

fn table_snapshot_expiration_report(
    table_bucket: &str,
    namespace: &Namespace,
    table: &IdentifierSegment,
    entry: &TableEntry,
    current_metadata: &serde_json::Value,
    config: TableSnapshotExpirationConfig,
    now: OffsetDateTime,
) -> TableSnapshotExpirationReport {
    let current_snapshot_id = current_metadata
        .get("current-snapshot-id")
        .and_then(serde_json::Value::as_i64);
    let expiration_watermark_ms = unix_timestamp_millis(now).saturating_sub(config.max_snapshot_age_ms);
    let (protected_ref_snapshot_ids, user_defined_ref_snapshot_ids, ref_retention_conflict_snapshot_ids) =
        snapshot_expiration_ref_state(current_metadata, current_snapshot_id);
    let table_retention_property_conflict = snapshot_expiration_table_property_conflicts(current_metadata, &config);

    let mut drafts = snapshot_expiration_drafts(current_metadata, current_snapshot_id);
    mark_recent_snapshots_to_keep(&mut drafts, config.min_snapshots_to_keep);

    let mut snapshot_reports = Vec::with_capacity(drafts.len());
    for mut draft in drafts {
        if let Some(snapshot_id) = draft.snapshot_id {
            if protected_ref_snapshot_ids.contains(&snapshot_id) {
                draft.reasons.insert(TableSnapshotExpirationReason::ProtectedSnapshotRef);
            }
            if user_defined_ref_snapshot_ids.contains(&snapshot_id) {
                draft.reasons.insert(TableSnapshotExpirationReason::UserDefinedSnapshotRef);
            }
            if ref_retention_conflict_snapshot_ids.contains(&snapshot_id) {
                draft
                    .reasons
                    .insert(TableSnapshotExpirationReason::SnapshotRefRetentionConflict);
            }
        }
        if table_retention_property_conflict {
            draft
                .reasons
                .insert(TableSnapshotExpirationReason::TableRetentionPropertyConflict);
        }

        let state = if snapshot_expiration_requires_manual_review(&draft.reasons) {
            TableSnapshotExpirationSnapshotState::ManualReviewRequired
        } else if snapshot_expiration_is_retained(&draft.reasons) {
            TableSnapshotExpirationSnapshotState::Retained
        } else if let Some(timestamp_ms) = draft.timestamp_ms {
            if timestamp_ms <= expiration_watermark_ms {
                draft.reasons.insert(TableSnapshotExpirationReason::SnapshotAgeExpired);
                TableSnapshotExpirationSnapshotState::ExpirationCandidate
            } else {
                draft
                    .reasons
                    .insert(TableSnapshotExpirationReason::SnapshotAgeWithinRetention);
                TableSnapshotExpirationSnapshotState::Retained
            }
        } else {
            draft.reasons.insert(TableSnapshotExpirationReason::MissingSnapshotTimestamp);
            TableSnapshotExpirationSnapshotState::ManualReviewRequired
        };

        snapshot_reports.push(TableSnapshotExpirationSnapshotReport {
            snapshot_id: draft.snapshot_id,
            sequence_number: draft.sequence_number,
            timestamp_ms: draft.timestamp_ms,
            manifest_list: draft.manifest_list,
            state,
            reasons: draft.reasons.into_iter().collect(),
        });
    }

    let retained_snapshot_count = snapshot_reports
        .iter()
        .filter(|snapshot| snapshot.state == TableSnapshotExpirationSnapshotState::Retained)
        .count();
    let expiration_candidate_count = snapshot_reports
        .iter()
        .filter(|snapshot| snapshot.state == TableSnapshotExpirationSnapshotState::ExpirationCandidate)
        .count();
    let manual_review_count = snapshot_reports
        .iter()
        .filter(|snapshot| snapshot.state == TableSnapshotExpirationSnapshotState::ManualReviewRequired)
        .count();

    TableSnapshotExpirationReport {
        table_bucket: table_bucket.to_string(),
        namespace: namespace.public_name(),
        table: table.as_str().to_string(),
        table_id: entry.table_id.clone(),
        current_metadata_location: entry.metadata_location.clone(),
        current_snapshot_id,
        config,
        expiration_watermark_ms,
        retained_snapshot_count,
        expiration_candidate_count,
        manual_review_count,
        expired_snapshot_ids: Vec::new(),
        committed_metadata_location: None,
        snapshot_reports,
    }
}

fn table_compaction_planning_report(
    table_bucket: &str,
    namespace: &Namespace,
    table: &IdentifierSegment,
    entry: &TableEntry,
    current_metadata: &serde_json::Value,
    config: TableCompactionPlanningConfig,
) -> TableCompactionPlanningReport {
    let current_snapshot_id = current_metadata
        .get("current-snapshot-id")
        .and_then(serde_json::Value::as_i64);
    let snapshot_reports = match current_snapshot_id {
        Some(current_snapshot_id) => {
            let current_snapshot = current_metadata
                .get("snapshots")
                .and_then(serde_json::Value::as_array)
                .into_iter()
                .flatten()
                .find(|snapshot| {
                    snapshot
                        .get("snapshot-id")
                        .and_then(serde_json::Value::as_i64)
                        .is_some_and(|snapshot_id| snapshot_id == current_snapshot_id)
                });
            match current_snapshot {
                Some(snapshot) => {
                    let manifest_list = snapshot
                        .get("manifest-list")
                        .and_then(serde_json::Value::as_str)
                        .map(ToString::to_string);
                    let mut reasons = BTreeSet::new();
                    if manifest_list.is_some() {
                        reasons.insert(TableCompactionPlanningReason::ManifestList);
                        reasons.insert(TableCompactionPlanningReason::ManifestAvroReaderUnavailable);
                    } else {
                        reasons.insert(TableCompactionPlanningReason::MissingManifestList);
                    }

                    vec![TableCompactionSnapshotReport {
                        snapshot_id: Some(current_snapshot_id),
                        manifest_list,
                        status: TableCompactionPlanningStatus::ManualReviewRequired,
                        reasons: reasons.into_iter().collect(),
                    }]
                }
                None => vec![TableCompactionSnapshotReport {
                    snapshot_id: Some(current_snapshot_id),
                    manifest_list: None,
                    status: TableCompactionPlanningStatus::ManualReviewRequired,
                    reasons: vec![TableCompactionPlanningReason::MissingCurrentSnapshot],
                }],
            }
        }
        None => Vec::new(),
    };

    let (status, manual_review_count) = if snapshot_reports.is_empty() {
        (TableCompactionPlanningStatus::NoCandidates, 0)
    } else {
        (
            TableCompactionPlanningStatus::ManualReviewRequired,
            snapshot_reports
                .iter()
                .filter(|snapshot| snapshot.status == TableCompactionPlanningStatus::ManualReviewRequired)
                .count(),
        )
    };

    TableCompactionPlanningReport {
        table_bucket: table_bucket.to_string(),
        namespace: namespace.public_name(),
        table: table.as_str().to_string(),
        table_id: entry.table_id.clone(),
        current_metadata_location: entry.metadata_location.clone(),
        current_snapshot_id,
        config,
        status,
        candidate_file_count: 0,
        rewrite_group_count: 0,
        manual_review_count,
        snapshot_reports,
    }
}

fn snapshot_expiration_drafts(
    current_metadata: &serde_json::Value,
    current_snapshot_id: Option<i64>,
) -> Vec<TableSnapshotExpirationDraft> {
    let Some(snapshots) = current_metadata.get("snapshots").and_then(serde_json::Value::as_array) else {
        return Vec::new();
    };

    snapshots
        .iter()
        .map(|snapshot| {
            let snapshot_id = snapshot.get("snapshot-id").and_then(serde_json::Value::as_i64);
            let timestamp_ms = snapshot.get("timestamp-ms").and_then(serde_json::Value::as_i64);
            let mut reasons = BTreeSet::new();
            if snapshot_id.is_none() {
                reasons.insert(TableSnapshotExpirationReason::MissingSnapshotId);
            }
            if timestamp_ms.is_none() {
                reasons.insert(TableSnapshotExpirationReason::MissingSnapshotTimestamp);
            }
            if snapshot_id.is_some() && snapshot_id == current_snapshot_id {
                reasons.insert(TableSnapshotExpirationReason::CurrentSnapshot);
            }

            TableSnapshotExpirationDraft {
                snapshot_id,
                sequence_number: snapshot.get("sequence-number").and_then(serde_json::Value::as_i64),
                timestamp_ms,
                manifest_list: snapshot
                    .get("manifest-list")
                    .and_then(serde_json::Value::as_str)
                    .map(ToString::to_string),
                reasons,
            }
        })
        .collect()
}

fn mark_recent_snapshots_to_keep(drafts: &mut [TableSnapshotExpirationDraft], min_snapshots_to_keep: usize) {
    let mut snapshots_by_time = drafts
        .iter()
        .enumerate()
        .filter_map(|(index, draft)| Some((draft.timestamp_ms?, index)))
        .collect::<Vec<_>>();
    snapshots_by_time.sort_by(|(left_timestamp, left_index), (right_timestamp, right_index)| {
        right_timestamp.cmp(left_timestamp).then_with(|| left_index.cmp(right_index))
    });

    for (_, index) in snapshots_by_time.into_iter().take(min_snapshots_to_keep) {
        drafts[index]
            .reasons
            .insert(TableSnapshotExpirationReason::MinSnapshotsToKeep);
    }
}

fn snapshot_expiration_ref_state(
    current_metadata: &serde_json::Value,
    current_snapshot_id: Option<i64>,
) -> (BTreeSet<i64>, BTreeSet<i64>, BTreeSet<i64>) {
    let mut protected_ref_snapshot_ids = BTreeSet::new();
    let mut user_defined_ref_snapshot_ids = BTreeSet::new();
    let mut ref_retention_conflict_snapshot_ids = BTreeSet::new();
    let Some(refs) = current_metadata.get("refs").and_then(serde_json::Value::as_object) else {
        return (
            protected_ref_snapshot_ids,
            user_defined_ref_snapshot_ids,
            ref_retention_conflict_snapshot_ids,
        );
    };

    for (name, reference) in refs {
        let Some(snapshot_id) = reference.get("snapshot-id").and_then(serde_json::Value::as_i64) else {
            continue;
        };
        if name != ICEBERG_MAIN_REF || Some(snapshot_id) != current_snapshot_id {
            protected_ref_snapshot_ids.insert(snapshot_id);
        }
        if name != ICEBERG_MAIN_REF {
            user_defined_ref_snapshot_ids.insert(snapshot_id);
        }
        if snapshot_ref_has_retention_policy(reference) {
            ref_retention_conflict_snapshot_ids.insert(snapshot_id);
        }
    }

    (
        protected_ref_snapshot_ids,
        user_defined_ref_snapshot_ids,
        ref_retention_conflict_snapshot_ids,
    )
}

fn snapshot_ref_has_retention_policy(reference: &serde_json::Value) -> bool {
    reference.get(ICEBERG_REF_MIN_SNAPSHOTS_TO_KEEP_FIELD).is_some()
        || reference.get(ICEBERG_REF_MAX_SNAPSHOT_AGE_MS_FIELD).is_some()
        || reference.get(ICEBERG_REF_MAX_REF_AGE_MS_FIELD).is_some()
}

fn snapshot_expiration_table_property_conflicts(
    current_metadata: &serde_json::Value,
    config: &TableSnapshotExpirationConfig,
) -> bool {
    let Some(properties) = current_metadata.get("properties").and_then(serde_json::Value::as_object) else {
        return false;
    };

    if properties.contains_key(ICEBERG_MAX_REF_AGE_MS_PROPERTY) {
        return true;
    }
    if retention_property_conflicts_usize(properties, ICEBERG_MIN_SNAPSHOTS_TO_KEEP_PROPERTY, config.min_snapshots_to_keep) {
        return true;
    }
    retention_property_conflicts_i64(properties, ICEBERG_MAX_SNAPSHOT_AGE_MS_PROPERTY, config.max_snapshot_age_ms)
}

fn retention_property_conflicts_usize(
    properties: &serde_json::Map<String, serde_json::Value>,
    key: &str,
    expected: usize,
) -> bool {
    let Some(value) = properties.get(key) else {
        return false;
    };
    serde_json_i64(value).and_then(|value| usize::try_from(value).ok()) != Some(expected)
}

fn retention_property_conflicts_i64(properties: &serde_json::Map<String, serde_json::Value>, key: &str, expected: i64) -> bool {
    let Some(value) = properties.get(key) else {
        return false;
    };
    serde_json_i64(value) != Some(expected)
}

fn serde_json_i64(value: &serde_json::Value) -> Option<i64> {
    value.as_i64().or_else(|| value.as_str()?.parse::<i64>().ok())
}

fn snapshot_expiration_requires_manual_review(reasons: &BTreeSet<TableSnapshotExpirationReason>) -> bool {
    reasons.contains(&TableSnapshotExpirationReason::MissingSnapshotId)
        || reasons.contains(&TableSnapshotExpirationReason::MissingSnapshotTimestamp)
        || reasons.contains(&TableSnapshotExpirationReason::UserDefinedSnapshotRef)
        || reasons.contains(&TableSnapshotExpirationReason::SnapshotRefRetentionConflict)
        || reasons.contains(&TableSnapshotExpirationReason::TableRetentionPropertyConflict)
}

fn snapshot_expiration_is_retained(reasons: &BTreeSet<TableSnapshotExpirationReason>) -> bool {
    reasons.contains(&TableSnapshotExpirationReason::CurrentSnapshot)
        || reasons.contains(&TableSnapshotExpirationReason::MinSnapshotsToKeep)
        || reasons.contains(&TableSnapshotExpirationReason::ProtectedSnapshotRef)
}

fn unix_timestamp_millis(now: OffsetDateTime) -> i64 {
    now.unix_timestamp()
        .saturating_mul(1000)
        .saturating_add(i64::from(now.millisecond()))
}

fn maintenance_timestamp(now: OffsetDateTime) -> String {
    now.format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_else(|_| now.unix_timestamp().to_string())
}

fn default_table_maintenance_worker_lease_timeout_seconds() -> u64 {
    TABLE_MAINTENANCE_WORKER_LEASE_TIMEOUT_DEFAULT_SECONDS
}

fn parse_maintenance_timestamp(timestamp: &str) -> Option<OffsetDateTime> {
    OffsetDateTime::parse(timestamp, &time::format_description::well_known::Rfc3339).ok()
}

fn table_maintenance_job_lease_is_active(
    job: &TableMetadataMaintenanceJob,
    worker_lease_timeout_seconds: u64,
    now: OffsetDateTime,
) -> bool {
    let Some(heartbeat_at) = job.heartbeat_at.as_deref().and_then(parse_maintenance_timestamp) else {
        return false;
    };
    let timeout_seconds = i64::try_from(worker_lease_timeout_seconds).unwrap_or(i64::MAX);
    heartbeat_at.saturating_add(Duration::seconds(timeout_seconds)) > now
}

fn table_maintenance_job_retry_is_pending(job: &TableMetadataMaintenanceJob, now: OffsetDateTime) -> bool {
    if !matches!(job.status, TableMetadataMaintenanceJobStatus::Failed) {
        return false;
    }
    let Some(next_retry_after) = job.next_retry_after.as_deref().and_then(parse_maintenance_timestamp) else {
        return false;
    };
    next_retry_after > now
}

fn validate_catalog_entry_version(kind: &str, version: u16) -> TableCatalogStoreResult<()> {
    if version != TABLE_CATALOG_ENTRY_VERSION {
        return Err(TableCatalogStoreError::Invalid(format!("unsupported {kind} entry version")));
    }
    Ok(())
}

fn validate_table_maintenance_config_version(version: u16) -> TableCatalogStoreResult<()> {
    if version != TABLE_MAINTENANCE_CONFIG_VERSION {
        return Err(TableCatalogStoreError::Invalid(
            "unsupported table maintenance config entry version".to_string(),
        ));
    }
    Ok(())
}

fn validate_table_maintenance_config(config: &TableMaintenanceConfig) -> TableCatalogStoreResult<()> {
    validate_table_maintenance_config_version(config.version)?;
    if config.worker_lease_timeout_seconds == 0 {
        return Err(TableCatalogStoreError::Invalid(
            "worker-lease-timeout-seconds must be greater than zero".to_string(),
        ));
    }
    if config.worker_lease_timeout_seconds > TABLE_MAINTENANCE_WORKER_LEASE_TIMEOUT_MAX_SECONDS {
        return Err(TableCatalogStoreError::Invalid(format!(
            "worker-lease-timeout-seconds cannot exceed {TABLE_MAINTENANCE_WORKER_LEASE_TIMEOUT_MAX_SECONDS}"
        )));
    }
    if config.max_retry_attempts > 10 {
        return Err(TableCatalogStoreError::Invalid("max-retry-attempts cannot exceed 10".to_string()));
    }
    if config.max_retry_attempts > 0 && config.retry_initial_backoff_seconds == 0 {
        return Err(TableCatalogStoreError::Invalid(
            "retry-initial-backoff-seconds must be greater than zero when retry is enabled".to_string(),
        ));
    }
    if config.max_retry_attempts > 0 && config.retry_initial_backoff_seconds > TABLE_MAINTENANCE_RETRY_BACKOFF_MAX_SECONDS {
        return Err(TableCatalogStoreError::Invalid(format!(
            "retry-initial-backoff-seconds cannot exceed {TABLE_MAINTENANCE_RETRY_BACKOFF_MAX_SECONDS}"
        )));
    }
    if config.max_retry_attempts > 0 && config.retry_max_backoff_seconds > TABLE_MAINTENANCE_RETRY_BACKOFF_MAX_SECONDS {
        return Err(TableCatalogStoreError::Invalid(format!(
            "retry-max-backoff-seconds cannot exceed {TABLE_MAINTENANCE_RETRY_BACKOFF_MAX_SECONDS}"
        )));
    }
    if config.max_retry_attempts > 0 && config.retry_max_backoff_seconds < config.retry_initial_backoff_seconds {
        return Err(TableCatalogStoreError::Invalid(
            "retry-max-backoff-seconds must be greater than or equal to retry-initial-backoff-seconds".to_string(),
        ));
    }
    if config.quarantine_enabled && config.quarantine_retention_seconds == 0 {
        return Err(TableCatalogStoreError::Invalid(
            "quarantine-retention-seconds must be greater than zero when quarantine is enabled".to_string(),
        ));
    }
    Ok(())
}

fn apply_maintenance_retry_after(job: &mut TableMetadataMaintenanceJob, config: &TableMaintenanceConfig, now: OffsetDateTime) {
    if config.max_retry_attempts == 0 || job.attempt >= config.max_retry_attempts {
        job.next_retry_after = None;
        return;
    }
    let attempt_index = u32::from(job.attempt.saturating_sub(1));
    let multiplier = 1_u64.checked_shl(attempt_index).unwrap_or(u64::MAX);
    let delay_seconds = config
        .retry_initial_backoff_seconds
        .saturating_mul(multiplier)
        .min(config.retry_max_backoff_seconds);
    let delay_seconds = i64::try_from(delay_seconds).unwrap_or(i64::MAX);
    job.next_retry_after = Some(maintenance_timestamp(now.saturating_add(Duration::seconds(delay_seconds))));
}

fn validate_table_snapshot_expiration_config(config: &TableSnapshotExpirationConfig) -> TableCatalogStoreResult<()> {
    if config.min_snapshots_to_keep == 0 {
        return Err(TableCatalogStoreError::Invalid(
            "min-snapshots-to-keep must be greater than zero".to_string(),
        ));
    }
    if config.max_snapshot_age_ms < 0 {
        return Err(TableCatalogStoreError::Invalid("max-snapshot-age-ms cannot be negative".to_string()));
    }
    Ok(())
}

fn validate_table_compaction_planning_config(config: &TableCompactionPlanningConfig) -> TableCatalogStoreResult<()> {
    if config.target_file_size_bytes == 0 {
        return Err(TableCatalogStoreError::Invalid(
            "target-file-size-bytes must be greater than zero".to_string(),
        ));
    }
    if config.small_file_threshold_bytes == 0 {
        return Err(TableCatalogStoreError::Invalid(
            "small-file-threshold-bytes must be greater than zero".to_string(),
        ));
    }
    if config.small_file_threshold_bytes > config.target_file_size_bytes {
        return Err(TableCatalogStoreError::Invalid(
            "small-file-threshold-bytes cannot exceed target-file-size-bytes".to_string(),
        ));
    }
    if config.min_input_files < 2 {
        return Err(TableCatalogStoreError::Invalid("min-input-files must be at least two".to_string()));
    }
    if config.max_rewrite_bytes_per_job < config.target_file_size_bytes {
        return Err(TableCatalogStoreError::Invalid(
            "max-rewrite-bytes-per-job must be at least target-file-size-bytes".to_string(),
        ));
    }
    Ok(())
}

fn commit_log_matches_request(commit_log: &CommitLogEntry, request: &TableCommitRequest, table_id: &str) -> bool {
    commit_log.version == TABLE_CATALOG_ENTRY_VERSION
        && commit_log.commit_id == request.commit_id
        && commit_log.idempotency_key == request.idempotency_key
        && commit_log.table_id == table_id
        && commit_log.operation == request.operation
        && commit_log.expected_version_token == request.expected_version_token
        && commit_log.previous_metadata_location == request.expected_metadata_location
        && commit_log.new_metadata_location == request.new_metadata_location
        && commit_log.requirements == request.requirements
        && commit_log.writer == request.writer
}

fn table_matches_committed_log(table: &TableEntry, commit_log: &CommitLogEntry) -> bool {
    table.table_id == commit_log.table_id
        && table.metadata_location == commit_log.new_metadata_location
        && table.version_token == commit_log.new_version_token
}

fn table_matches_staged_base(table: &TableEntry, commit_log: &CommitLogEntry) -> bool {
    table.table_id == commit_log.table_id
        && table.metadata_location == commit_log.previous_metadata_location
        && table.version_token == commit_log.expected_version_token
}

fn table_catalog_recovery_summary(
    metadata_status: &TableMetadataPointerStatus,
    commit_recovery: &TableCommitRecoveryReport,
) -> (TableCatalogRecoveryStatus, Vec<TableCatalogRecoveryAction>) {
    let mut actions = Vec::new();
    let metadata_status = match metadata_status {
        TableMetadataPointerStatus::Valid => None,
        TableMetadataPointerStatus::MissingObject => {
            actions.push(TableCatalogRecoveryAction::RestoreCurrentMetadataObject);
            Some(TableCatalogRecoveryStatus::ReadOnlyRecommended)
        }
        TableMetadataPointerStatus::InvalidJson => {
            actions.push(TableCatalogRecoveryAction::FixCurrentMetadataJson);
            Some(TableCatalogRecoveryStatus::ReadOnlyRecommended)
        }
        TableMetadataPointerStatus::InvalidLocation => {
            actions.push(TableCatalogRecoveryAction::MoveCurrentMetadataInsideTable);
            Some(TableCatalogRecoveryStatus::ReadOnlyRecommended)
        }
    };

    if commit_recovery.manual_review_count > 0 {
        actions.push(TableCatalogRecoveryAction::ReviewCommitLog);
        return (metadata_status.unwrap_or(TableCatalogRecoveryStatus::ManualReviewRequired), actions);
    }
    if commit_recovery.finalization_required_count > 0 || commit_recovery.idempotency_repair_required_count > 0 {
        actions.push(TableCatalogRecoveryAction::RunCommitRecovery);
        return (metadata_status.unwrap_or(TableCatalogRecoveryStatus::Recoverable), actions);
    }
    if commit_recovery.staged_before_table_update_count > 0 {
        actions.push(TableCatalogRecoveryAction::RetryCommit);
        return (metadata_status.unwrap_or(TableCatalogRecoveryStatus::Recoverable), actions);
    }

    (metadata_status.unwrap_or(TableCatalogRecoveryStatus::Healthy), actions)
}

fn commit_logs_share_recovery_payload(left: &CommitLogEntry, right: &CommitLogEntry) -> bool {
    left.version == right.version
        && left.commit_id == right.commit_id
        && left.idempotency_key == right.idempotency_key
        && left.table_id == right.table_id
        && left.operation == right.operation
        && left.expected_version_token == right.expected_version_token
        && left.new_version_token == right.new_version_token
        && left.previous_metadata_location == right.previous_metadata_location
        && left.new_metadata_location == right.new_metadata_location
        && left.requirements == right.requirements
        && left.writer == right.writer
}

fn commit_idempotency_index_status(
    commit_log: &CommitLogEntry,
    idempotency_commit: Option<&CommitLogEntry>,
) -> TableCommitIdempotencyIndexStatus {
    match (commit_log.idempotency_key.as_ref(), idempotency_commit) {
        (None, _) => TableCommitIdempotencyIndexStatus::NotRequired,
        (Some(_), None) => TableCommitIdempotencyIndexStatus::Missing,
        (Some(_), Some(indexed)) if indexed == commit_log => TableCommitIdempotencyIndexStatus::Matches,
        (Some(_), Some(indexed)) if commit_logs_share_recovery_payload(indexed, commit_log) => {
            TableCommitIdempotencyIndexStatus::Stale
        }
        (Some(_), Some(_)) => TableCommitIdempotencyIndexStatus::Conflicting,
    }
}

fn table_commit_recovery_entry(
    table: &TableEntry,
    commit_log: &CommitLogEntry,
    idempotency_commit: Option<&CommitLogEntry>,
) -> TableCommitRecoveryEntry {
    let idempotency_index_status = commit_idempotency_index_status(commit_log, idempotency_commit);
    let idempotency_index_present = matches!(
        idempotency_index_status,
        TableCommitIdempotencyIndexStatus::Matches
            | TableCommitIdempotencyIndexStatus::Stale
            | TableCommitIdempotencyIndexStatus::Conflicting
    );
    let idempotency_index_repair_required = matches!(
        idempotency_index_status,
        TableCommitIdempotencyIndexStatus::Missing | TableCommitIdempotencyIndexStatus::Stale
    );

    let (recovery_state, reason) = if matches!(idempotency_index_status, TableCommitIdempotencyIndexStatus::Conflicting) {
        (
            TableCommitRecoveryState::ManualReview,
            "idempotency index points at a different commit payload".to_string(),
        )
    } else if table_matches_committed_log(table, commit_log) {
        if matches!(commit_log.status, CommitLogStatus::Committed) {
            if idempotency_index_repair_required {
                (
                    TableCommitRecoveryState::IdempotencyIndexRepairRequired,
                    "committed table pointer is durable but idempotency index needs repair".to_string(),
                )
            } else {
                (
                    TableCommitRecoveryState::Committed,
                    "commit log and current table pointer agree".to_string(),
                )
            }
        } else {
            (
                TableCommitRecoveryState::FinalizationRequired,
                "current table pointer already advanced but commit log is not finalized".to_string(),
            )
        }
    } else if matches!(commit_log.status, CommitLogStatus::Committed) {
        if idempotency_index_repair_required {
            (
                TableCommitRecoveryState::IdempotencyIndexRepairRequired,
                "historical committed log needs idempotency index repair".to_string(),
            )
        } else {
            (
                TableCommitRecoveryState::Committed,
                "commit is finalized and may be older than the current table pointer".to_string(),
            )
        }
    } else if table_matches_staged_base(table, commit_log) {
        (
            TableCommitRecoveryState::StagedBeforeTableUpdate,
            "staged commit exists but table pointer has not advanced".to_string(),
        )
    } else {
        (
            TableCommitRecoveryState::ManualReview,
            "staged commit no longer matches the current table pointer or its expected base".to_string(),
        )
    };

    TableCommitRecoveryEntry {
        commit_id: commit_log.commit_id.clone(),
        idempotency_key: commit_log.idempotency_key.clone(),
        operation: commit_log.operation.clone(),
        status: commit_log.status.clone(),
        recovery_state,
        previous_metadata_location: commit_log.previous_metadata_location.clone(),
        new_metadata_location: commit_log.new_metadata_location.clone(),
        expected_version_token: commit_log.expected_version_token.clone(),
        new_version_token: commit_log.new_version_token.clone(),
        idempotency_index_present,
        idempotency_index_status,
        reason,
    }
}

fn record_table_commit_attempt(operation: &str) {
    counter!("rustfs_table_catalog_commit_attempts_total", "operation" => operation.to_string()).increment(1);
}

fn table_catalog_store_result_label<T>(result: &TableCatalogStoreResult<T>) -> &'static str {
    match result {
        Ok(_) => "success",
        Err(TableCatalogStoreError::Conflict(_)) => "conflict",
        Err(TableCatalogStoreError::Invalid(_)) => "invalid",
        Err(TableCatalogStoreError::NotFound(_)) => "not_found",
        Err(TableCatalogStoreError::Internal(_)) => "failure",
    }
}

fn duration_millis_u64(duration: StdDuration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn record_table_commit_cas_result(operation: &str, started: Instant, result: &TableCatalogStoreResult<()>) {
    let elapsed = started.elapsed();
    let result_label = table_catalog_store_result_label(result);
    counter!(
        "rustfs_table_catalog_commit_cas_results_total",
        "operation" => operation.to_string(),
        "result" => result_label.to_string()
    )
    .increment(1);
    histogram!(
        "rustfs_table_catalog_commit_cas_duration_seconds",
        "operation" => operation.to_string(),
        "result" => result_label.to_string()
    )
    .record(elapsed.as_secs_f64());
}

fn record_table_commit_result(
    table_bucket: &str,
    namespace: &str,
    table: &str,
    commit_id: &str,
    operation: &str,
    started: Instant,
    result: &TableCatalogStoreResult<TableCommitResult>,
) {
    let elapsed = started.elapsed();
    let result_label = table_catalog_store_result_label(result);
    counter!(
        "rustfs_table_catalog_commit_results_total",
        "operation" => operation.to_string(),
        "result" => result_label.to_string()
    )
    .increment(1);
    if matches!(result, Err(TableCatalogStoreError::Conflict(_))) {
        counter!("rustfs_table_catalog_commit_conflicts_total", "operation" => operation.to_string()).increment(1);
    }
    histogram!(
        "rustfs_table_catalog_commit_duration_seconds",
        "operation" => operation.to_string(),
        "result" => result_label.to_string()
    )
    .record(elapsed.as_secs_f64());

    match result {
        Ok(commit) if elapsed >= TABLE_COMMIT_SLOW_LOG_THRESHOLD => {
            tracing::warn!(
                table_bucket,
                namespace,
                table,
                commit_id,
                operation,
                generation = commit.table.generation,
                duration_ms = duration_millis_u64(elapsed),
                "slow table catalog commit"
            );
        }
        Ok(commit) => {
            tracing::debug!(
                table_bucket,
                namespace,
                table,
                commit_id,
                operation,
                generation = commit.table.generation,
                duration_ms = duration_millis_u64(elapsed),
                "table catalog commit completed"
            );
        }
        Err(error) => {
            tracing::warn!(
                table_bucket,
                namespace,
                table,
                commit_id,
                operation,
                result = result_label,
                duration_ms = duration_millis_u64(elapsed),
                error = %error,
                "table catalog commit did not complete"
            );
        }
    }
}

fn table_commit_result(
    table_bucket: &str,
    namespace: &str,
    table: &str,
    commit_id: &str,
    operation: &str,
    started: Instant,
    result: TableCatalogStoreResult<TableCommitResult>,
) -> TableCatalogStoreResult<TableCommitResult> {
    record_table_commit_result(table_bucket, namespace, table, commit_id, operation, started, &result);
    result
}

fn http_preconditions_for_catalog_put(precondition: TableCatalogPutPrecondition) -> Option<HTTPPreconditions> {
    match precondition {
        TableCatalogPutPrecondition::Any => None,
        TableCatalogPutPrecondition::IfAbsent => Some(HTTPPreconditions {
            if_none_match: Some("*".to_string()),
            ..Default::default()
        }),
        TableCatalogPutPrecondition::IfMatch(etag) => Some(HTTPPreconditions {
            if_match: Some(etag),
            ..Default::default()
        }),
    }
}

fn is_missing_storage_error(err: &StorageError) -> bool {
    matches!(
        err,
        StorageError::ObjectNotFound(_, _) | StorageError::FileNotFound | StorageError::ConfigNotFound
    )
}

fn storage_error_to_catalog(action: &str, err: StorageError) -> TableCatalogStoreError {
    match err {
        StorageError::ObjectNotFound(bucket, object) => TableCatalogStoreError::NotFound(format!("{action}: {bucket}/{object}")),
        StorageError::BucketNotFound(bucket) => TableCatalogStoreError::NotFound(format!("{action}: bucket {bucket}")),
        StorageError::PreconditionFailed => TableCatalogStoreError::Conflict(format!("{action}: precondition failed")),
        other => TableCatalogStoreError::Internal(format!("{action}: {other}")),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct NamespaceMarker {
    pub version: u16,
    pub namespace: String,
}

impl NamespaceMarker {
    pub fn new(namespace: &Namespace) -> Self {
        Self {
            version: TABLE_NAMESPACE_MARKER_VERSION,
            namespace: namespace.public_name(),
        }
    }
}

pub(crate) fn namespace_marker_json(namespace: &Namespace) -> Result<Vec<u8>, serde_json::Error> {
    serde_json::to_vec(&NamespaceMarker::new(namespace))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct TableMarker {
    pub version: u16,
    pub namespace: String,
    pub name: String,
    pub metadata_location: Option<String>,
}

impl TableMarker {
    pub fn new(namespace: &Namespace, table: &IdentifierSegment) -> Self {
        Self {
            version: TABLE_RESOURCE_MARKER_VERSION,
            namespace: namespace.public_name(),
            name: table.as_str().to_string(),
            metadata_location: None,
        }
    }
}

pub(crate) fn table_marker_json(namespace: &Namespace, table: &IdentifierSegment) -> Result<Vec<u8>, serde_json::Error> {
    serde_json::to_vec(&TableMarker::new(namespace, table))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TableMetadataPointer {
    pub version: u16,
    pub metadata_location: String,
}

impl TableMetadataPointer {
    pub fn new(metadata_location: String) -> Self {
        Self {
            version: TABLE_METADATA_POINTER_VERSION,
            metadata_location,
        }
    }
}

pub(crate) fn table_metadata_pointer_json(metadata_location: String) -> Result<Vec<u8>, serde_json::Error> {
    serde_json::to_vec(&TableMetadataPointer::new(metadata_location))
}

pub(crate) fn parse_table_metadata_pointer(data: &[u8]) -> Result<TableMetadataPointer, serde_json::Error> {
    serde_json::from_slice(data)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdentifierSegment(String);

impl IdentifierSegment {
    pub const MAX_LEN: usize = 64;

    pub fn parse(value: impl Into<String>) -> Result<Self, CatalogIdentifierError> {
        let value = value.into();
        validate_identifier_segment(&value)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Namespace {
    segments: Vec<IdentifierSegment>,
}

impl Namespace {
    pub fn parse(value: &str) -> Result<Self, CatalogIdentifierError> {
        if value.is_empty() {
            return Err(CatalogIdentifierError::Empty);
        }

        let mut segments = Vec::new();
        for segment in value.split('.') {
            segments.push(IdentifierSegment::parse(segment.to_string())?);
        }

        Ok(Self { segments })
    }

    pub fn segments(&self) -> &[IdentifierSegment] {
        &self.segments
    }

    pub fn storage_id(&self) -> String {
        self.segments
            .iter()
            .map(IdentifierSegment::as_str)
            .collect::<Vec<_>>()
            .join("/")
    }

    pub fn public_name(&self) -> String {
        self.segments
            .iter()
            .map(IdentifierSegment::as_str)
            .collect::<Vec<_>>()
            .join(".")
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableIdentifier {
    warehouse: IdentifierSegment,
    namespace: Namespace,
    name: IdentifierSegment,
}

impl TableIdentifier {
    pub fn new(warehouse: IdentifierSegment, namespace: Namespace, name: IdentifierSegment) -> Self {
        Self {
            warehouse,
            namespace,
            name,
        }
    }

    pub fn warehouse(&self) -> &IdentifierSegment {
        &self.warehouse
    }

    pub fn namespace(&self) -> &Namespace {
        &self.namespace
    }

    pub fn name(&self) -> &IdentifierSegment {
        &self.name
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TablePathResolver {
    reserved_prefix: &'static str,
}

impl Default for TablePathResolver {
    fn default() -> Self {
        Self {
            reserved_prefix: TABLE_RESERVED_PREFIX,
        }
    }
}

impl TablePathResolver {
    pub fn current_pointer_path(&self, table: &TableIdentifier) -> String {
        format!("{}/{}", self.table_root(table), CURRENT_POINTER_FILE)
    }

    pub fn metadata_dir_path(&self, table: &TableIdentifier) -> String {
        format!("{}/{}", self.table_root(table), METADATA_DIR)
    }

    pub fn metadata_file_path(&self, table: &TableIdentifier, metadata_file_name: &str) -> String {
        format!("{}/{}", self.metadata_dir_path(table), metadata_file_name)
    }

    fn table_root(&self, table: &TableIdentifier) -> String {
        format!(
            "{}/{}/{}/{}/{}/{}/{}",
            self.reserved_prefix,
            WAREHOUSE_ROOT,
            table.warehouse().as_str(),
            NAMESPACE_ROOT,
            table.namespace().storage_id(),
            TABLE_ROOT,
            table.name().as_str()
        )
    }
}

pub fn is_reserved_table_object_key(object_key: &str) -> bool {
    object_key == TABLE_RESERVED_PREFIX
        || object_key
            .strip_prefix(TABLE_RESERVED_PREFIX)
            .is_some_and(|rest| rest.starts_with('/'))
}

pub(crate) fn default_namespace_root_prefix() -> String {
    format!(
        "{}/{}/{}/{}/",
        TABLE_RESERVED_PREFIX, WAREHOUSE_ROOT, DEFAULT_WAREHOUSE_ID, NAMESPACE_ROOT
    )
}

pub(crate) fn default_namespace_marker_path(namespace: &Namespace) -> String {
    format!("{}{}/{}", default_namespace_root_prefix(), namespace.storage_id(), NAMESPACE_MARKER_FILE)
}

pub(crate) fn default_table_root_prefix(namespace: &Namespace) -> String {
    format!("{}{}/{}/", default_namespace_root_prefix(), namespace.storage_id(), TABLE_ROOT)
}

pub(crate) fn default_table_marker_path(namespace: &Namespace, table: &IdentifierSegment) -> String {
    format!("{}{}/{}", default_table_root_prefix(namespace), table.as_str(), TABLE_MARKER_FILE)
}

pub(crate) fn default_table_metadata_dir_path(namespace: &Namespace, table: &IdentifierSegment) -> String {
    format!("{}{}/{}", default_table_root_prefix(namespace), table.as_str(), METADATA_DIR)
}

pub(crate) fn default_table_metadata_file_path(
    namespace: &Namespace,
    table: &IdentifierSegment,
    metadata_file_name: &str,
) -> String {
    format!("{}/{}", default_table_metadata_dir_path(namespace, table), metadata_file_name)
}

pub(crate) fn default_table_current_pointer_path(namespace: &Namespace, table: &IdentifierSegment) -> String {
    format!("{}{}/{}", default_table_root_prefix(namespace), table.as_str(), CURRENT_POINTER_FILE)
}

pub(crate) fn default_table_lifecycle_path(namespace: &Namespace, table: &IdentifierSegment) -> String {
    format!("{}{}/{}", default_table_root_prefix(namespace), table.as_str(), LIFECYCLE_FILE)
}

pub(crate) fn namespace_name_from_marker_path(object_key: &str) -> Option<String> {
    let prefix = default_namespace_root_prefix();
    let suffix = format!("/{NAMESPACE_MARKER_FILE}");

    object_key
        .strip_prefix(prefix.as_str())
        .and_then(|value| value.strip_suffix(suffix.as_str()))
        .filter(|value| !value.is_empty())
        .map(|value| value.replace('/', "."))
}

pub(crate) fn table_name_from_marker_path(namespace: &Namespace, object_key: &str) -> Option<String> {
    let prefix = default_table_root_prefix(namespace);
    let suffix = format!("/{TABLE_MARKER_FILE}");

    object_key
        .strip_prefix(prefix.as_str())
        .and_then(|value| value.strip_suffix(suffix.as_str()))
        .filter(|value| !value.is_empty() && !value.contains('/'))
        .map(ToString::to_string)
}

pub(crate) fn metadata_location_from_metadata_file_path(
    namespace: &Namespace,
    table: &IdentifierSegment,
    object_key: &str,
) -> Option<String> {
    let prefix = format!("{}/", default_table_metadata_dir_path(namespace, table));

    object_key
        .strip_prefix(prefix.as_str())
        .filter(|value| is_valid_table_metadata_file_name(value))
        .map(|_| object_key.to_string())
}

pub(crate) fn is_valid_table_metadata_location(
    namespace: &Namespace,
    table: &IdentifierSegment,
    metadata_location: &str,
) -> bool {
    if metadata_location.is_empty() {
        return false;
    }

    let metadata_prefix = format!("{}/", default_table_metadata_dir_path(namespace, table));
    metadata_location
        .strip_prefix(&metadata_prefix)
        .is_some_and(is_valid_table_metadata_file_name)
}

pub(crate) fn is_valid_table_metadata_file_name(metadata_file_name: &str) -> bool {
    if metadata_file_name.is_empty()
        || metadata_file_name.len() > TABLE_METADATA_FILE_NAME_MAX_LEN
        || !metadata_file_name.ends_with(".json")
        || metadata_file_name.contains("..")
        || metadata_file_name.contains('%')
        || metadata_file_name.contains('/')
        || metadata_file_name.contains('\\')
        || metadata_file_name.bytes().any(|byte| byte.is_ascii_control())
    {
        return false;
    }

    let bytes = metadata_file_name.as_bytes();
    if !is_lower_ascii_alnum(bytes[0]) || !is_lower_ascii_alnum(bytes[bytes.len() - 1]) {
        return false;
    }

    bytes
        .iter()
        .all(|byte| is_lower_ascii_alnum(*byte) || matches!(*byte, b'.' | b'_' | b'-'))
}

pub fn validate_object_mutation(table_bucket_enabled: bool, object_key: &str) -> Result<(), TableObjectMutationError> {
    if table_bucket_enabled && is_reserved_table_object_key(object_key) {
        return Err(TableObjectMutationError::ReservedCatalogObject);
    }

    Ok(())
}

pub(crate) async fn validate_bucket_object_mutation(bucket: &str, object_key: &str) -> Result<(), TableObjectMutationError> {
    let table_bucket_enabled = metadata_sys::get(bucket)
        .await
        .is_ok_and(|metadata| metadata.table_bucket_enabled());

    validate_object_mutation(table_bucket_enabled, object_key)
}

fn validate_identifier_segment(value: &str) -> Result<(), CatalogIdentifierError> {
    if value.is_empty() {
        return Err(CatalogIdentifierError::Empty);
    }

    if value.len() > IdentifierSegment::MAX_LEN {
        return Err(CatalogIdentifierError::TooLong {
            max: IdentifierSegment::MAX_LEN,
        });
    }

    if matches!(value, "." | "..") || value.contains('%') || value.contains('/') || value.contains('\\') {
        return Err(CatalogIdentifierError::Ambiguous);
    }

    let bytes = value.as_bytes();
    if !is_lower_ascii_alnum(bytes[0]) || !is_lower_ascii_alnum(bytes[bytes.len() - 1]) {
        return Err(CatalogIdentifierError::InvalidBoundary);
    }

    if bytes
        .iter()
        .any(|byte| !is_lower_ascii_alnum(*byte) && !matches!(*byte, b'_' | b'-'))
    {
        return Err(CatalogIdentifierError::InvalidCharacter);
    }

    Ok(())
}

fn is_lower_ascii_alnum(value: u8) -> bool {
    value.is_ascii_lowercase() || value.is_ascii_digit()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::assert_matches;

    #[test]
    fn reserved_table_object_key_matches_exact_prefix_and_children_only() {
        assert!(is_reserved_table_object_key(".rustfs-table"));
        assert!(is_reserved_table_object_key(".rustfs-table/"));
        assert!(is_reserved_table_object_key(".rustfs-table/metadata/current.json"));

        assert!(!is_reserved_table_object_key(""));
        assert!(!is_reserved_table_object_key(".rustfs-table-other"));
        assert!(!is_reserved_table_object_key("prefix/.rustfs-table/object"));
        assert!(!is_reserved_table_object_key("user/.rustfs-table"));
    }

    #[test]
    fn object_mutation_guard_only_blocks_reserved_prefix_for_table_buckets() {
        assert!(validate_object_mutation(false, ".rustfs-table/current.json").is_ok());
        assert_eq!(
            validate_object_mutation(true, ".rustfs-table/current.json").unwrap_err(),
            TableObjectMutationError::ReservedCatalogObject
        );
        assert!(validate_object_mutation(true, ".rustfs-table-other/current.json").is_ok());
    }

    #[tokio::test]
    async fn bucket_object_mutation_guard_allows_when_bucket_metadata_is_unavailable() {
        assert!(
            validate_bucket_object_mutation("missing-bucket", ".rustfs-table/current.json")
                .await
                .is_ok()
        );
    }

    #[test]
    fn table_bucket_marker_json_uses_stable_catalog_defaults() {
        let marker = serde_json::to_value(TableBucketMarker::default()).unwrap();

        assert_eq!(marker["version"], TABLE_BUCKET_CONFIG_VERSION);
        assert_eq!(marker["catalog_type"], TABLE_BUCKET_CATALOG_TYPE);
        assert_eq!(marker["reserved_prefix"], TABLE_RESERVED_PREFIX);
        assert!(!table_bucket_marker_json().unwrap().is_empty());
    }

    #[test]
    fn catalog_entry_structures_serialize_stable_fields() {
        use std::collections::BTreeMap;

        let bucket = TableBucketEntry {
            version: TABLE_CATALOG_ENTRY_VERSION,
            table_bucket: "analytics".to_string(),
            catalog_type: TABLE_BUCKET_CATALOG_TYPE.to_string(),
            warehouse_root: "s3://analytics/".to_string(),
            state: TableCatalogEntryState::Active,
            properties: BTreeMap::from([("owner".to_string(), "platform".to_string())]),
            created_at: Some("2026-05-23T00:00:00Z".to_string()),
            updated_at: Some("2026-05-23T00:00:00Z".to_string()),
        };
        let namespace = NamespaceEntry {
            version: TABLE_CATALOG_ENTRY_VERSION,
            table_bucket: "analytics".to_string(),
            namespace: "sales".to_string(),
            namespace_id: "sales".to_string(),
            state: TableCatalogEntryState::Active,
            properties: BTreeMap::from([("purpose".to_string(), "orders".to_string())]),
            created_at: Some("2026-05-23T00:00:00Z".to_string()),
            updated_at: Some("2026-05-23T00:00:00Z".to_string()),
        };
        let table = TableEntry {
            version: TABLE_CATALOG_ENTRY_VERSION,
            table_bucket: "analytics".to_string(),
            namespace: "sales".to_string(),
            table: "orders".to_string(),
            table_id: "table-id".to_string(),
            table_uuid: "table-uuid".to_string(),
            format: "ICEBERG".to_string(),
            format_version: 2,
            warehouse_location: "s3://analytics/tables/table-id".to_string(),
            metadata_location: "s3://analytics/tables/table-id/metadata/v1.metadata.json".to_string(),
            version_token: "token-v1".to_string(),
            generation: 1,
            state: TableCatalogEntryState::Active,
            properties: BTreeMap::from([("write.format.default".to_string(), "parquet".to_string())]),
            created_at: Some("2026-05-23T00:00:00Z".to_string()),
            updated_at: Some("2026-05-23T00:00:00Z".to_string()),
        };
        let commit = CommitLogEntry {
            version: TABLE_CATALOG_ENTRY_VERSION,
            commit_id: "commit-id".to_string(),
            idempotency_key: Some("client-request-id".to_string()),
            table_id: "table-id".to_string(),
            operation: "append".to_string(),
            expected_version_token: "token-v1".to_string(),
            new_version_token: "token-v2".to_string(),
            previous_metadata_location: "s3://analytics/tables/table-id/metadata/v1.metadata.json".to_string(),
            new_metadata_location: "s3://analytics/tables/table-id/metadata/v2.metadata.json".to_string(),
            requirements: vec![serde_json::json!({"type": "assert-table-uuid", "uuid": "table-uuid"})],
            status: CommitLogStatus::Committed,
            writer: Some("pyiceberg/test".to_string()),
            created_at: Some("2026-05-23T00:01:00Z".to_string()),
            updated_at: Some("2026-05-23T00:01:00Z".to_string()),
        };

        let bucket_json = serde_json::to_value(&bucket).unwrap();
        let namespace_json = serde_json::to_value(&namespace).unwrap();
        let table_json = serde_json::to_value(&table).unwrap();
        let commit_json = serde_json::to_value(&commit).unwrap();

        assert_eq!(bucket_json["state"], "ACTIVE");
        assert_eq!(bucket_json["properties"]["owner"], "platform");
        assert_eq!(namespace_json["namespace_id"], "sales");
        assert_eq!(table_json["version_token"], "token-v1");
        assert_eq!(table_json["generation"], 1);
        assert_eq!(table_json["state"], "ACTIVE");
        assert_eq!(commit_json["status"], "COMMITTED");
        assert_eq!(commit_json["requirements"][0]["type"], "assert-table-uuid");
    }

    #[test]
    fn catalog_entry_deserialization_rejects_unknown_fields() {
        use std::collections::BTreeMap;

        let table = TableEntry {
            version: TABLE_CATALOG_ENTRY_VERSION,
            table_bucket: "analytics".to_string(),
            namespace: "sales".to_string(),
            table: "orders".to_string(),
            table_id: "table-id".to_string(),
            table_uuid: "table-uuid".to_string(),
            format: "ICEBERG".to_string(),
            format_version: 2,
            warehouse_location: "s3://analytics/tables/table-id".to_string(),
            metadata_location: "s3://analytics/tables/table-id/metadata/v1.metadata.json".to_string(),
            version_token: "token-v1".to_string(),
            generation: 1,
            state: TableCatalogEntryState::Active,
            properties: BTreeMap::new(),
            created_at: None,
            updated_at: None,
        };
        let mut value = serde_json::to_value(table).unwrap();
        value
            .as_object_mut()
            .unwrap()
            .insert("unexpected".to_string(), serde_json::json!(true));

        assert!(serde_json::from_value::<TableEntry>(value).is_err());
    }

    struct NoopTableCatalogStore;

    #[async_trait::async_trait]
    impl TableCatalogStore for NoopTableCatalogStore {
        async fn get_table_bucket(&self, _table_bucket: &str) -> TableCatalogStoreResult<Option<TableBucketEntry>> {
            Ok(None)
        }

        async fn put_table_bucket(&self, _entry: TableBucketEntry) -> TableCatalogStoreResult<()> {
            Ok(())
        }

        async fn create_namespace(&self, _entry: NamespaceEntry) -> TableCatalogStoreResult<()> {
            Ok(())
        }

        async fn list_namespaces(&self, _table_bucket: &str) -> TableCatalogStoreResult<Vec<NamespaceEntry>> {
            Ok(Vec::new())
        }

        async fn get_namespace(&self, _table_bucket: &str, _namespace: &str) -> TableCatalogStoreResult<Option<NamespaceEntry>> {
            Ok(None)
        }

        async fn drop_namespace(&self, _table_bucket: &str, _namespace: &str) -> TableCatalogStoreResult<()> {
            Ok(())
        }

        async fn create_table(&self, _entry: TableEntry) -> TableCatalogStoreResult<()> {
            Ok(())
        }

        async fn register_table(&self, _entry: TableEntry) -> TableCatalogStoreResult<()> {
            Ok(())
        }

        async fn list_tables(&self, _table_bucket: &str, _namespace: &str) -> TableCatalogStoreResult<Vec<TableEntry>> {
            Ok(Vec::new())
        }

        async fn load_table(
            &self,
            _table_bucket: &str,
            _namespace: &str,
            _table: &str,
        ) -> TableCatalogStoreResult<Option<TableEntry>> {
            Ok(None)
        }

        async fn commit_table(&self, request: TableCommitRequest) -> TableCatalogStoreResult<TableCommitResult> {
            let table = TableEntry {
                version: TABLE_CATALOG_ENTRY_VERSION,
                table_bucket: request.table_bucket,
                namespace: request.namespace,
                table: request.table,
                table_id: "table-id".to_string(),
                table_uuid: "table-uuid".to_string(),
                format: "ICEBERG".to_string(),
                format_version: 2,
                warehouse_location: "s3://analytics/tables/table-id".to_string(),
                metadata_location: request.new_metadata_location.clone(),
                version_token: "token-v2".to_string(),
                generation: 2,
                state: TableCatalogEntryState::Active,
                properties: BTreeMap::new(),
                created_at: None,
                updated_at: None,
            };
            let commit_log = CommitLogEntry {
                version: TABLE_CATALOG_ENTRY_VERSION,
                commit_id: request.commit_id,
                idempotency_key: request.idempotency_key,
                table_id: table.table_id.clone(),
                operation: request.operation,
                expected_version_token: request.expected_version_token,
                new_version_token: table.version_token.clone(),
                previous_metadata_location: request.expected_metadata_location,
                new_metadata_location: table.metadata_location.clone(),
                requirements: request.requirements,
                status: CommitLogStatus::Committed,
                writer: request.writer,
                created_at: None,
                updated_at: None,
            };

            Ok(TableCommitResult { table, commit_log })
        }

        async fn drop_table(&self, _table_bucket: &str, _namespace: &str, _table: &str) -> TableCatalogStoreResult<()> {
            Ok(())
        }

        async fn get_commit_by_id(
            &self,
            _table_bucket: &str,
            _table_id: &str,
            _commit_id: &str,
        ) -> TableCatalogStoreResult<Option<CommitLogEntry>> {
            Ok(None)
        }

        async fn get_commit_by_idempotency_key(
            &self,
            _table_bucket: &str,
            _table_id: &str,
            _idempotency_key: &str,
        ) -> TableCatalogStoreResult<Option<CommitLogEntry>> {
            Ok(None)
        }
    }

    #[tokio::test]
    async fn table_catalog_store_trait_covers_entry_read_write_shapes() {
        let store: &dyn TableCatalogStore = &NoopTableCatalogStore;

        assert!(store.get_table_bucket("analytics").await.unwrap().is_none());
        assert!(store.list_namespaces("analytics").await.unwrap().is_empty());
        assert!(
            store
                .get_commit_by_id("analytics", "table-id", "commit-id")
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .get_commit_by_idempotency_key("analytics", "table-id", "client-request-id")
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn table_catalog_store_trait_has_atomic_commit_shape() {
        let store: &dyn TableCatalogStore = &NoopTableCatalogStore;
        let request = TableCommitRequest {
            table_bucket: "analytics".to_string(),
            namespace: "sales".to_string(),
            table: "orders".to_string(),
            commit_id: "commit-id".to_string(),
            idempotency_key: Some("client-request-id".to_string()),
            operation: "append".to_string(),
            expected_version_token: "token-v1".to_string(),
            expected_metadata_location: "s3://analytics/tables/table-id/metadata/v1.metadata.json".to_string(),
            new_metadata_location: "s3://analytics/tables/table-id/metadata/v2.metadata.json".to_string(),
            requirements: vec![serde_json::json!({"type": "assert-table-uuid", "uuid": "table-uuid"})],
            writer: Some("pyiceberg/test".to_string()),
        };

        let result = store.commit_table(request).await.unwrap();

        assert_eq!(result.table.version_token, "token-v2");
        assert_eq!(result.table.generation, 2);
        assert_eq!(result.commit_log.status, CommitLogStatus::Committed);
    }

    #[test]
    fn catalog_object_entry_paths_use_internal_root_and_hashed_untrusted_ids() {
        let paths = TableCatalogObjectPaths::default();
        let bucket = "analytics";
        let namespace = Namespace::parse("analytics.daily_events").unwrap();
        let table = IdentifierSegment::parse("events").unwrap();
        let bucket_root = format!("s3tables/catalog/table-buckets/{}/", table_catalog_path_hash(bucket));

        assert_eq!(paths.table_bucket_entry_path(bucket), format!("{bucket_root}table-bucket.json"));
        assert_eq!(
            paths.namespace_entry_path(bucket, &namespace),
            format!("{bucket_root}namespaces/analytics/daily_events/namespace-entry.json")
        );
        assert_eq!(
            paths.table_entry_path(bucket, &namespace, &table),
            format!("{bucket_root}namespaces/analytics/daily_events/tables/events/table-entry.json")
        );

        let commit_path = paths.commit_log_entry_path("table/../bucket", "table/../id", "commit/%2f\nid");
        let idempotency_path = paths.commit_idempotency_entry_path("table/../bucket", "table/../id", "client/%2f\nrequest");
        let maintenance_config_path = paths.table_maintenance_config_path("table/../bucket", &namespace, &table, "table/../id");
        let maintenance_job_path =
            paths.table_maintenance_job_path("table/../bucket", &namespace, &table, "table/../id", "job/%2f\nid");

        for path in [commit_path, idempotency_path, maintenance_config_path, maintenance_job_path] {
            assert!(path.starts_with("s3tables/catalog/table-buckets/"));
            assert!(path.ends_with(".json"));
            assert!(!path.contains(".."));
            assert!(!path.contains('%'));
            assert!(!path.contains('\n'));
            assert!(!path.contains("table/../bucket"));
            assert!(!path.contains("table/../id"));
            assert!(!path.contains("client/%2f"));
        }
    }

    #[derive(Clone, Default)]
    struct TestCatalogObjectBackend {
        state: std::sync::Arc<tokio::sync::Mutex<TestCatalogObjectState>>,
        write_lock: std::sync::Arc<tokio::sync::Mutex<()>>,
    }

    #[derive(Default)]
    struct TestCatalogObjectState {
        objects: BTreeMap<(String, String), TestCatalogObjectRecord>,
        fail_put_attempts: BTreeMap<(String, String), BTreeSet<usize>>,
        put_attempts: BTreeMap<(String, String), usize>,
        next_etag: u64,
    }

    #[derive(Clone)]
    struct TestCatalogObjectRecord {
        data: Vec<u8>,
        etag: String,
        mod_time: Option<OffsetDateTime>,
    }

    impl TestCatalogObjectBackend {
        async fn seed_object(&self, bucket: &str, object: &str, data: Vec<u8>) {
            self.seed_object_with_mod_time(bucket, object, data, Some(OffsetDateTime::UNIX_EPOCH))
                .await;
        }

        async fn seed_object_with_mod_time(&self, bucket: &str, object: &str, data: Vec<u8>, mod_time: Option<OffsetDateTime>) {
            let mut state = self.state.lock().await;
            let etag = state.next_etag();
            state
                .objects
                .insert((bucket.to_string(), object.to_string()), TestCatalogObjectRecord { data, etag, mod_time });
        }

        async fn fail_put_attempt(&self, bucket: &str, object: &str, attempt: usize) {
            let mut state = self.state.lock().await;
            state
                .fail_put_attempts
                .entry((bucket.to_string(), object.to_string()))
                .or_default()
                .insert(attempt);
        }
    }

    impl TestCatalogObjectState {
        fn next_etag(&mut self) -> String {
            self.next_etag += 1;
            format!("etag-{}", self.next_etag)
        }
    }

    fn maintenance_object_report<'a>(
        report: &'a TableMetadataMaintenanceReport,
        metadata_location: &str,
    ) -> &'a TableMetadataMaintenanceObjectReport {
        report
            .object_reports
            .iter()
            .find(|object| object.metadata_location == metadata_location)
            .expect("metadata maintenance object report should exist")
    }

    fn snapshot_expiration_report(
        report: &TableSnapshotExpirationReport,
        snapshot_id: i64,
    ) -> &TableSnapshotExpirationSnapshotReport {
        report
            .snapshot_reports
            .iter()
            .find(|snapshot| snapshot.snapshot_id == Some(snapshot_id))
            .expect("snapshot expiration report should include the snapshot")
    }

    fn compaction_snapshot_report(report: &TableCompactionPlanningReport, snapshot_id: i64) -> &TableCompactionSnapshotReport {
        report
            .snapshot_reports
            .iter()
            .find(|snapshot| snapshot.snapshot_id == Some(snapshot_id))
            .expect("compaction planning report should include the snapshot")
    }

    #[async_trait::async_trait]
    impl TableCatalogObjectBackend for TestCatalogObjectBackend {
        async fn read_object(&self, bucket: &str, object: &str) -> TableCatalogStoreResult<Option<TableCatalogObject>> {
            let state = self.state.lock().await;
            Ok(state
                .objects
                .get(&(bucket.to_string(), object.to_string()))
                .map(|record| TableCatalogObject {
                    data: record.data.clone(),
                    etag: Some(record.etag.clone()),
                    mod_time: record.mod_time,
                }))
        }

        async fn object_exists(&self, bucket: &str, object: &str) -> TableCatalogStoreResult<bool> {
            let state = self.state.lock().await;
            Ok(state.objects.contains_key(&(bucket.to_string(), object.to_string())))
        }

        async fn put_object(
            &self,
            bucket: &str,
            object: &str,
            data: Vec<u8>,
            precondition: TableCatalogPutPrecondition,
        ) -> TableCatalogStoreResult<()> {
            let mut state = self.state.lock().await;
            let key = (bucket.to_string(), object.to_string());
            let attempt = {
                let attempts = state.put_attempts.entry(key.clone()).or_default();
                *attempts += 1;
                *attempts
            };
            if state
                .fail_put_attempts
                .get(&key)
                .is_some_and(|attempts| attempts.contains(&attempt))
            {
                return Err(TableCatalogStoreError::Internal(format!(
                    "injected put failure for {object} attempt {attempt}"
                )));
            }
            match precondition {
                TableCatalogPutPrecondition::IfAbsent if state.objects.contains_key(&key) => {
                    return Err(TableCatalogStoreError::Conflict(format!("object already exists: {object}")));
                }
                TableCatalogPutPrecondition::IfMatch(expected) => {
                    let Some(current) = state.objects.get(&key) else {
                        return Err(TableCatalogStoreError::Conflict(format!("object is missing: {object}")));
                    };
                    if current.etag != expected {
                        return Err(TableCatalogStoreError::Conflict(format!("object changed: {object}")));
                    }
                }
                _ => {}
            }

            let etag = state.next_etag();
            state.objects.insert(
                key,
                TestCatalogObjectRecord {
                    data,
                    etag,
                    mod_time: Some(OffsetDateTime::now_utc()),
                },
            );
            Ok(())
        }

        async fn delete_object(&self, bucket: &str, object: &str) -> TableCatalogStoreResult<()> {
            let mut state = self.state.lock().await;
            state.objects.remove(&(bucket.to_string(), object.to_string()));
            Ok(())
        }

        async fn list_objects(&self, bucket: &str, prefix: &str) -> TableCatalogStoreResult<Vec<String>> {
            let state = self.state.lock().await;
            Ok(state
                .objects
                .keys()
                .filter(|(entry_bucket, object)| entry_bucket == bucket && object.starts_with(prefix))
                .map(|(_, object)| object.clone())
                .collect())
        }

        async fn acquire_write_lock(&self, _bucket: &str, _object: &str) -> TableCatalogStoreResult<Box<dyn Send>> {
            Ok(Box::new(self.write_lock.clone().lock_owned().await))
        }
    }

    fn test_bucket_entry(bucket: &str) -> TableBucketEntry {
        TableBucketEntry {
            version: TABLE_CATALOG_ENTRY_VERSION,
            table_bucket: bucket.to_string(),
            catalog_type: TABLE_BUCKET_CATALOG_TYPE.to_string(),
            warehouse_root: format!("s3://{bucket}/"),
            state: TableCatalogEntryState::Active,
            properties: BTreeMap::new(),
            created_at: None,
            updated_at: None,
        }
    }

    fn test_namespace_entry(bucket: &str, namespace: &Namespace) -> NamespaceEntry {
        NamespaceEntry {
            version: TABLE_CATALOG_ENTRY_VERSION,
            table_bucket: bucket.to_string(),
            namespace: namespace.public_name(),
            namespace_id: namespace.storage_id(),
            state: TableCatalogEntryState::Active,
            properties: BTreeMap::new(),
            created_at: None,
            updated_at: None,
        }
    }

    fn test_table_entry(bucket: &str, namespace: &Namespace, table: &IdentifierSegment, metadata_location: String) -> TableEntry {
        TableEntry {
            version: TABLE_CATALOG_ENTRY_VERSION,
            table_bucket: bucket.to_string(),
            namespace: namespace.public_name(),
            table: table.as_str().to_string(),
            table_id: "table-id".to_string(),
            table_uuid: "table-uuid".to_string(),
            format: "ICEBERG".to_string(),
            format_version: 2,
            warehouse_location: format!("s3://{bucket}/tables/table-id"),
            metadata_location,
            version_token: "token-v1".to_string(),
            generation: 1,
            state: TableCatalogEntryState::Active,
            properties: BTreeMap::new(),
            created_at: None,
            updated_at: None,
        }
    }

    async fn seed_table_for_metadata_maintenance(
        store: &ObjectTableCatalogStore<TestCatalogObjectBackend>,
        bucket: &str,
        namespace: &Namespace,
        table: &IdentifierSegment,
        current_metadata: String,
    ) {
        store.put_table_bucket(test_bucket_entry(bucket)).await.unwrap();
        store.create_namespace(test_namespace_entry(bucket, namespace)).await.unwrap();
        store
            .create_table(test_table_entry(bucket, namespace, table, current_metadata))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn object_table_catalog_store_writes_catalog_entries_to_internal_meta_bucket() {
        let backend = TestCatalogObjectBackend::default();
        let store = ObjectTableCatalogStore::new(backend.clone());
        let bucket = "analytics";

        store.put_table_bucket(test_bucket_entry(bucket)).await.unwrap();

        let state = backend.state.lock().await;
        let object_buckets = state
            .objects
            .keys()
            .map(|(bucket, _)| bucket.as_str())
            .collect::<BTreeSet<_>>();

        assert_eq!(object_buckets, BTreeSet::from([rustfs_ecstore::disk::RUSTFS_META_BUCKET]));
    }

    #[tokio::test]
    async fn maintenance_dry_run_keeps_current_metadata() {
        let backend = TestCatalogObjectBackend::default();
        let store = ObjectTableCatalogStore::new(backend.clone());
        let bucket = "analytics";
        let namespace = Namespace::parse("sales").unwrap();
        let table = IdentifierSegment::parse("orders").unwrap();
        let v1 = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
        let v2 = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
        let current = default_table_metadata_file_path(&namespace, &table, "00003.metadata.json");

        seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current.clone()).await;
        backend.seed_object(bucket, &v1, b"{}".to_vec()).await;
        backend.seed_object(bucket, &v2, b"{}".to_vec()).await;
        backend
            .seed_object(bucket, &current, br#"{"metadata-log":[]}"#.to_vec())
            .await;

        let report = store
            .plan_table_metadata_maintenance(bucket, "sales", "orders", 0)
            .await
            .unwrap();

        assert_eq!(report.current_metadata_location, current);
        assert!(report.retained_metadata_locations.contains(&report.current_metadata_location));
        assert!(!report.cleanup_candidate_locations.contains(&report.current_metadata_location));
        assert_eq!(report.cleanup_candidate_locations, vec![v1, v2]);
    }

    #[tokio::test]
    async fn table_data_plane_resource_resolves_registered_warehouse_prefix() {
        let backend = TestCatalogObjectBackend::default();
        let store = ObjectTableCatalogStore::new(backend);
        let bucket = "analytics";
        let namespace = Namespace::parse("sales").unwrap();
        let table = IdentifierSegment::parse("orders").unwrap();
        let current = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");

        seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current).await;

        let resource = table_data_plane_resource_for_object(&store, bucket, "tables/table-id/data/part-00001.parquet")
            .await
            .expect("data-plane resource lookup should succeed")
            .expect("object should resolve to the registered table");

        assert_eq!(resource.table_bucket, bucket);
        assert_eq!(resource.namespace, "sales");
        assert_eq!(resource.table, "orders");
        assert_eq!(resource.table_id, "table-id");
        assert_eq!(resource.warehouse_object_prefix, "tables/table-id/");
        assert_eq!(resource.catalog_resource_object(), "namespaces/sales/tables/orders");
    }

    #[tokio::test]
    async fn table_data_plane_resource_does_not_match_sibling_prefix() {
        let backend = TestCatalogObjectBackend::default();
        let store = ObjectTableCatalogStore::new(backend);
        let bucket = "analytics";
        let namespace = Namespace::parse("sales").unwrap();
        let table = IdentifierSegment::parse("orders").unwrap();
        let current = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");

        seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current).await;

        let resource = table_data_plane_resource_for_object(&store, bucket, "tables/table-id-other/data/part-00001.parquet")
            .await
            .expect("data-plane resource lookup should succeed");

        assert!(resource.is_none());
    }

    #[tokio::test]
    async fn table_data_plane_resource_prefers_longest_registered_warehouse_prefix() {
        let backend = TestCatalogObjectBackend::default();
        let store = ObjectTableCatalogStore::new(backend);
        let bucket = "analytics";
        let namespace = Namespace::parse("sales").unwrap();
        let parent_table = IdentifierSegment::parse("orders").unwrap();
        let child_table = IdentifierSegment::parse("orders_child").unwrap();
        let current = default_table_metadata_file_path(&namespace, &parent_table, "00001.metadata.json");

        seed_table_for_metadata_maintenance(&store, bucket, &namespace, &parent_table, current.clone()).await;
        let mut child_entry = test_table_entry(bucket, &namespace, &child_table, current);
        child_entry.table_id = "table-id-child".to_string();
        child_entry.warehouse_location = format!("s3://{bucket}/tables/table-id/child");
        store.create_table(child_entry).await.unwrap();

        let resource = table_data_plane_resource_for_object(&store, bucket, "tables/table-id/child/data/part-00001.parquet")
            .await
            .expect("data-plane resource lookup should succeed")
            .expect("object should resolve to the child table");

        assert_eq!(resource.table, "orders_child");
        assert_eq!(resource.table_id, "table-id-child");
        assert_eq!(resource.warehouse_object_prefix, "tables/table-id/child/");
        assert_eq!(resource.catalog_resource_object(), "namespaces/sales/tables/orders_child");
    }

    #[tokio::test]
    async fn table_data_plane_resource_skips_invalid_warehouse_locations() {
        let backend = TestCatalogObjectBackend::default();
        let store = ObjectTableCatalogStore::new(backend);
        let bucket = "analytics";
        let namespace = Namespace::parse("sales").unwrap();
        let invalid_table = IdentifierSegment::parse("bad_orders").unwrap();
        let valid_table = IdentifierSegment::parse("orders").unwrap();
        let current = default_table_metadata_file_path(&namespace, &valid_table, "00001.metadata.json");

        store.put_table_bucket(test_bucket_entry(bucket)).await.unwrap();
        store
            .create_namespace(test_namespace_entry(bucket, &namespace))
            .await
            .unwrap();

        let mut invalid_entry = test_table_entry(bucket, &namespace, &invalid_table, current.clone());
        invalid_entry.table_id = "bad-table-id".to_string();
        invalid_entry.warehouse_location = format!("s3://{bucket}/");
        store.create_table(invalid_entry).await.unwrap();
        store
            .create_table(test_table_entry(bucket, &namespace, &valid_table, current))
            .await
            .unwrap();

        let unrelated = table_data_plane_resource_for_object(&store, bucket, "ordinary/object.parquet")
            .await
            .expect("invalid table warehouse location should not deny unrelated object lookup");
        assert!(unrelated.is_none());

        let resource = table_data_plane_resource_for_object(&store, bucket, "tables/table-id/data/part-00001.parquet")
            .await
            .expect("invalid table warehouse location should not block a later valid match")
            .expect("valid table warehouse object should resolve to the table");
        assert_eq!(resource.table, "orders");
        assert_eq!(resource.table_id, "table-id");
        assert_eq!(resource.warehouse_object_prefix, "tables/table-id/");
    }

    #[tokio::test]
    async fn maintenance_dry_run_reports_job_context_and_deletable_candidates() {
        let backend = TestCatalogObjectBackend::default();
        let store = ObjectTableCatalogStore::new(backend.clone());
        let bucket = "analytics";
        let namespace = Namespace::parse("sales").unwrap();
        let table = IdentifierSegment::parse("orders").unwrap();
        let old = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
        let current = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
        let fresh = default_table_metadata_file_path(&namespace, &table, "00003.metadata.json");

        seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current.clone()).await;
        backend.seed_object(bucket, &old, b"{}".to_vec()).await;
        backend
            .seed_object(bucket, &current, br#"{"metadata-log":[]}"#.to_vec())
            .await;
        backend
            .seed_object_with_mod_time(bucket, &fresh, b"{}".to_vec(), Some(OffsetDateTime::now_utc()))
            .await;

        let report = store
            .plan_table_metadata_maintenance(bucket, "sales", "orders", 0)
            .await
            .unwrap();

        assert_eq!(report.job.table_bucket, bucket);
        assert_eq!(report.job.namespace, "sales");
        assert_eq!(report.job.table, "orders");
        assert_eq!(report.job.table_id, "table-id");
        assert_eq!(report.job.operation, TableMetadataMaintenanceOperation::DryRun);
        assert_eq!(report.job.status, TableMetadataMaintenanceJobStatus::Successful);
        assert_eq!(report.job.deleted_metadata_file_count, 0);
        assert_eq!(report.job.current_generation, 1);
        assert_eq!(report.job.safety_window_seconds, TABLE_METADATA_CLEANUP_SAFETY_WINDOW_SECONDS);
        assert!(!report.job.job_id.is_empty());
        assert!(report.job.cleanup_watermark_unix_seconds <= OffsetDateTime::now_utc().unix_timestamp());
        assert_eq!(report.cleanup_candidate_locations, vec![old.clone(), fresh]);
        assert_eq!(report.deletable_metadata_locations, vec![old]);
    }

    #[tokio::test]
    async fn maintenance_dry_run_explains_metadata_reachability() {
        let backend = TestCatalogObjectBackend::default();
        let store = ObjectTableCatalogStore::new(backend.clone());
        let bucket = "analytics";
        let namespace = Namespace::parse("sales").unwrap();
        let table = IdentifierSegment::parse("orders").unwrap();
        let logged = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
        let fresh = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
        let old = default_table_metadata_file_path(&namespace, &table, "00003.metadata.json");
        let recent = default_table_metadata_file_path(&namespace, &table, "00004.metadata.json");
        let current = default_table_metadata_file_path(&namespace, &table, "00005.metadata.json");

        seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current.clone()).await;
        backend.seed_object(bucket, &logged, b"{}".to_vec()).await;
        backend.seed_object(bucket, &recent, b"{}".to_vec()).await;
        backend.seed_object(bucket, &old, b"{}".to_vec()).await;
        backend
            .seed_object_with_mod_time(bucket, &fresh, b"{}".to_vec(), Some(OffsetDateTime::now_utc()))
            .await;
        backend
            .seed_object(
                bucket,
                &current,
                serde_json::to_vec(&serde_json::json!({
                    "metadata-log": [
                        {
                            "timestamp-ms": 1,
                            "metadata-file": logged
                        }
                    ]
                }))
                .unwrap(),
            )
            .await;

        let report = store
            .plan_table_metadata_maintenance(bucket, "sales", "orders", 2)
            .await
            .unwrap();

        assert_eq!(report.job.planned_metadata_file_count, 5);
        assert_eq!(report.job.retained_metadata_file_count, 3);
        assert_eq!(report.job.cleanup_candidate_count, 2);
        assert_eq!(report.job.deletable_metadata_file_count, 1);

        let current_report = maintenance_object_report(&report, &current);
        assert_eq!(current_report.state, TableMetadataMaintenanceObjectState::Retained);
        assert_eq!(current_report.reasons, vec![TableMetadataMaintenanceReason::CurrentMetadata]);

        let logged_report = maintenance_object_report(&report, &logged);
        assert_eq!(logged_report.state, TableMetadataMaintenanceObjectState::Retained);
        assert_eq!(logged_report.reasons, vec![TableMetadataMaintenanceReason::MetadataLog]);

        let recent_report = maintenance_object_report(&report, &recent);
        assert_eq!(recent_report.state, TableMetadataMaintenanceObjectState::Retained);
        assert_eq!(recent_report.reasons, vec![TableMetadataMaintenanceReason::RecentMetadata]);

        let old_report = maintenance_object_report(&report, &old);
        assert_eq!(old_report.state, TableMetadataMaintenanceObjectState::Deletable);
        assert_eq!(
            old_report.reasons,
            vec![
                TableMetadataMaintenanceReason::NoCurrentReachability,
                TableMetadataMaintenanceReason::SafetyWindowSatisfied,
            ]
        );

        let fresh_report = maintenance_object_report(&report, &fresh);
        assert_eq!(fresh_report.state, TableMetadataMaintenanceObjectState::PendingSafetyWindow);
        assert_eq!(
            fresh_report.reasons,
            vec![
                TableMetadataMaintenanceReason::NoCurrentReachability,
                TableMetadataMaintenanceReason::SafetyWindowPending,
            ]
        );
    }

    #[tokio::test]
    async fn maintenance_state_is_scoped_to_current_table_identity() {
        let backend = TestCatalogObjectBackend::default();
        let store = ObjectTableCatalogStore::new(backend.clone());
        let bucket = "analytics";
        let namespace = Namespace::parse("sales").unwrap();
        let table = IdentifierSegment::parse("orders").unwrap();
        let current = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");

        store.put_table_bucket(test_bucket_entry(bucket)).await.unwrap();
        store
            .create_namespace(test_namespace_entry(bucket, &namespace))
            .await
            .unwrap();

        let mut first_table = test_table_entry(bucket, &namespace, &table, current.clone());
        first_table.table_id = "table-id-1".to_string();
        store.create_table(first_table).await.unwrap();
        store
            .put_table_maintenance_config(
                bucket,
                "sales",
                "orders",
                TableMaintenanceConfig {
                    version: TABLE_MAINTENANCE_CONFIG_VERSION,
                    retain_recent_metadata_files: 7,
                    delete_enabled: true,
                    background_enabled: false,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        backend
            .seed_object(bucket, &current, br#"{"metadata-log":[]}"#.to_vec())
            .await;
        let report = store
            .plan_table_metadata_maintenance(bucket, "sales", "orders", 0)
            .await
            .unwrap();
        store.put_table_metadata_maintenance_report(&report).await.unwrap();
        assert!(
            store
                .get_table_metadata_maintenance_report(bucket, "sales", "orders", &report.job.job_id)
                .await
                .unwrap()
                .is_some()
        );

        store.drop_table(bucket, "sales", "orders").await.unwrap();

        let mut second_table = test_table_entry(bucket, &namespace, &table, current);
        second_table.table_id = "table-id-2".to_string();
        store.create_table(second_table).await.unwrap();

        assert_eq!(
            store.get_table_maintenance_config(bucket, "sales", "orders").await.unwrap(),
            TableMaintenanceConfig::default()
        );
        assert!(
            store
                .get_table_metadata_maintenance_report(bucket, "sales", "orders", &report.job.job_id)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn maintenance_config_rejects_unsupported_config_version() {
        let backend = TestCatalogObjectBackend::default();
        let store = ObjectTableCatalogStore::new(backend);
        let bucket = "analytics";
        let namespace = Namespace::parse("sales").unwrap();
        let table = IdentifierSegment::parse("orders").unwrap();
        let current = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");

        seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current).await;

        let err = store
            .put_table_maintenance_config(
                bucket,
                "sales",
                "orders",
                TableMaintenanceConfig {
                    version: TABLE_MAINTENANCE_CONFIG_VERSION.saturating_add(1),
                    retain_recent_metadata_files: 1,
                    delete_enabled: false,
                    background_enabled: false,
                    ..Default::default()
                },
            )
            .await
            .unwrap_err();

        assert_matches!(err, TableCatalogStoreError::Invalid(_));
    }

    #[tokio::test]
    async fn maintenance_config_inherits_bucket_default_and_tracks_override_source() {
        let backend = TestCatalogObjectBackend::default();
        let store = ObjectTableCatalogStore::new(backend);
        let bucket = "analytics";
        let namespace = Namespace::parse("sales").unwrap();
        let table = IdentifierSegment::parse("orders").unwrap();
        let current = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");

        seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current).await;
        store
            .put_table_bucket_maintenance_config(
                bucket,
                TableMaintenanceConfig {
                    version: TABLE_MAINTENANCE_CONFIG_VERSION,
                    retain_recent_metadata_files: 3,
                    delete_enabled: true,
                    background_enabled: false,
                    ..Default::default()
                },
            )
            .await
            .expect("bucket default maintenance config should persist");

        let inherited = store
            .get_effective_table_maintenance_config(bucket, "sales", "orders")
            .await
            .expect("effective maintenance config should load");

        assert_eq!(inherited.source, TableMaintenanceConfigSource::TableBucketDefault);
        assert_eq!(inherited.config.retain_recent_metadata_files, 3);
        assert!(inherited.config.delete_enabled);
        assert!(!inherited.config.background_enabled);

        store
            .put_table_maintenance_config(
                bucket,
                "sales",
                "orders",
                TableMaintenanceConfig {
                    version: TABLE_MAINTENANCE_CONFIG_VERSION,
                    retain_recent_metadata_files: 1,
                    delete_enabled: false,
                    background_enabled: false,
                    ..Default::default()
                },
            )
            .await
            .expect("table maintenance override should persist");

        let overridden = store
            .get_effective_table_maintenance_config(bucket, "sales", "orders")
            .await
            .expect("effective maintenance override should load");

        assert_eq!(overridden.source, TableMaintenanceConfigSource::TableOverride);
        assert_eq!(overridden.config.retain_recent_metadata_files, 1);
        assert!(!overridden.config.delete_enabled);
        assert!(!overridden.config.background_enabled);
    }

    #[tokio::test]
    async fn maintenance_config_accepts_background_enabled_worker_runtime_controls() {
        let backend = TestCatalogObjectBackend::default();
        let store = ObjectTableCatalogStore::new(backend);
        let bucket = "analytics";
        let namespace = Namespace::parse("sales").unwrap();
        let table = IdentifierSegment::parse("orders").unwrap();
        let current = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");

        seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current).await;

        let bucket_config = store
            .put_table_bucket_maintenance_config(
                bucket,
                TableMaintenanceConfig {
                    version: TABLE_MAINTENANCE_CONFIG_VERSION,
                    retain_recent_metadata_files: 1,
                    delete_enabled: false,
                    background_enabled: true,
                    worker_paused: true,
                    worker_lease_timeout_seconds: 60,
                    ..Default::default()
                },
            )
            .await
            .expect("background maintenance bucket config should persist");
        assert!(bucket_config.background_enabled);
        assert!(bucket_config.worker_paused);
        assert_eq!(bucket_config.worker_lease_timeout_seconds, 60);

        let table_config = store
            .put_table_maintenance_config(
                bucket,
                "sales",
                "orders",
                TableMaintenanceConfig {
                    version: TABLE_MAINTENANCE_CONFIG_VERSION,
                    retain_recent_metadata_files: 1,
                    delete_enabled: false,
                    background_enabled: true,
                    worker_paused: false,
                    worker_lease_timeout_seconds: 120,
                    ..Default::default()
                },
            )
            .await
            .expect("background maintenance table config should persist");
        assert!(table_config.background_enabled);
        assert!(!table_config.worker_paused);
        assert_eq!(table_config.worker_lease_timeout_seconds, 120);
    }

    #[tokio::test]
    async fn maintenance_config_accepts_retry_and_quarantine_policy() {
        let backend = TestCatalogObjectBackend::default();
        let store = ObjectTableCatalogStore::new(backend);
        let bucket = "analytics";
        let namespace = Namespace::parse("sales").unwrap();
        let table = IdentifierSegment::parse("orders").unwrap();
        let current = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");

        seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current).await;

        let config = store
            .put_table_maintenance_config(
                bucket,
                "sales",
                "orders",
                TableMaintenanceConfig {
                    version: TABLE_MAINTENANCE_CONFIG_VERSION,
                    retain_recent_metadata_files: 2,
                    delete_enabled: false,
                    background_enabled: false,
                    max_retry_attempts: 3,
                    retry_initial_backoff_seconds: 10,
                    retry_max_backoff_seconds: 60,
                    quarantine_enabled: true,
                    quarantine_retention_seconds: 86_400,
                    ..Default::default()
                },
            )
            .await
            .expect("retry and quarantine maintenance config should persist");

        assert_eq!(config.max_retry_attempts, 3);
        assert_eq!(config.retry_initial_backoff_seconds, 10);
        assert_eq!(config.retry_max_backoff_seconds, 60);
        assert!(config.quarantine_enabled);
        assert_eq!(config.quarantine_retention_seconds, 86_400);
    }

    #[tokio::test]
    async fn maintenance_config_rejects_retry_backoff_above_limit() {
        let backend = TestCatalogObjectBackend::default();
        let store = ObjectTableCatalogStore::new(backend);
        let bucket = "analytics";
        let namespace = Namespace::parse("sales").unwrap();
        let table = IdentifierSegment::parse("orders").unwrap();
        let current = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");

        seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current).await;

        let initial_err = store
            .put_table_maintenance_config(
                bucket,
                "sales",
                "orders",
                TableMaintenanceConfig {
                    version: TABLE_MAINTENANCE_CONFIG_VERSION,
                    retain_recent_metadata_files: 1,
                    delete_enabled: false,
                    background_enabled: false,
                    max_retry_attempts: 1,
                    retry_initial_backoff_seconds: TABLE_MAINTENANCE_RETRY_BACKOFF_MAX_SECONDS.saturating_add(1),
                    retry_max_backoff_seconds: TABLE_MAINTENANCE_RETRY_BACKOFF_MAX_SECONDS.saturating_add(1),
                    ..Default::default()
                },
            )
            .await
            .unwrap_err();
        assert_matches!(initial_err, TableCatalogStoreError::Invalid(_));

        let max_err = store
            .put_table_maintenance_config(
                bucket,
                "sales",
                "orders",
                TableMaintenanceConfig {
                    version: TABLE_MAINTENANCE_CONFIG_VERSION,
                    retain_recent_metadata_files: 1,
                    delete_enabled: false,
                    background_enabled: false,
                    max_retry_attempts: 1,
                    retry_initial_backoff_seconds: TABLE_MAINTENANCE_RETRY_BACKOFF_MAX_SECONDS,
                    retry_max_backoff_seconds: TABLE_MAINTENANCE_RETRY_BACKOFF_MAX_SECONDS.saturating_add(1),
                    ..Default::default()
                },
            )
            .await
            .unwrap_err();
        assert_matches!(max_err, TableCatalogStoreError::Invalid(_));
    }

    #[tokio::test]
    async fn maintenance_run_rejects_existing_invalid_retry_config_before_scheduling() {
        let backend = TestCatalogObjectBackend::default();
        let store = ObjectTableCatalogStore::new(backend);
        let bucket = "analytics";
        let namespace = Namespace::parse("sales").unwrap();
        let table = IdentifierSegment::parse("orders").unwrap();
        let current = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");

        seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current).await;
        let config_path = store
            .paths
            .table_maintenance_config_path(bucket, &namespace, &table, "table-id");
        store
            .write_entry(
                store.catalog_bucket(),
                &config_path,
                &TableMaintenanceConfig {
                    version: TABLE_MAINTENANCE_CONFIG_VERSION,
                    retain_recent_metadata_files: 1,
                    delete_enabled: false,
                    background_enabled: false,
                    max_retry_attempts: 1,
                    retry_initial_backoff_seconds: u64::MAX,
                    retry_max_backoff_seconds: u64::MAX,
                    ..Default::default()
                },
                TableCatalogPutPrecondition::Any,
            )
            .await
            .expect("invalid legacy maintenance config should be seeded");

        let err = store
            .run_table_metadata_maintenance(bucket, "sales", "orders", true, Some("worker-a".to_string()))
            .await
            .unwrap_err();

        assert_matches!(err, TableCatalogStoreError::Invalid(_));
    }

    #[tokio::test]
    async fn maintenance_run_persists_latest_job_alias_with_worker_and_lease_context() {
        let backend = TestCatalogObjectBackend::default();
        let store = ObjectTableCatalogStore::new(backend.clone());
        let bucket = "analytics";
        let namespace = Namespace::parse("sales").unwrap();
        let table = IdentifierSegment::parse("orders").unwrap();
        let old = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
        let current = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");

        seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current.clone()).await;
        store
            .put_table_bucket_maintenance_config(
                bucket,
                TableMaintenanceConfig {
                    version: TABLE_MAINTENANCE_CONFIG_VERSION,
                    retain_recent_metadata_files: 0,
                    delete_enabled: false,
                    background_enabled: false,
                    ..Default::default()
                },
            )
            .await
            .expect("bucket default maintenance config should persist");
        backend.seed_object(bucket, &old, b"{}".to_vec()).await;
        backend
            .seed_object(bucket, &current, br#"{"metadata-log":[]}"#.to_vec())
            .await;

        let report = store
            .run_table_metadata_maintenance(bucket, "sales", "orders", false, Some("worker-a".to_string()))
            .await
            .expect("metadata maintenance run should succeed");

        assert_eq!(report.job.status, TableMetadataMaintenanceJobStatus::Successful);
        assert_eq!(report.job.config_source, TableMaintenanceConfigSource::TableBucketDefault);
        assert_eq!(report.job.worker_id.as_deref(), Some("worker-a"));
        assert!(!report.job.lease_id.is_empty());
        assert_eq!(report.job.attempt, 1);
        assert_eq!(report.job.max_retry_attempts, 0);
        assert!(report.job.next_retry_after.is_none());
        assert!(report.job.heartbeat_at.is_some());
        assert!(report.job.started_at.is_some());
        assert!(report.job.finished_at.is_some());

        let latest = store
            .get_table_metadata_maintenance_report(bucket, "sales", "orders", "latest")
            .await
            .expect("latest maintenance lookup should succeed")
            .expect("latest maintenance job should be stored");
        let current_alias = store
            .get_table_metadata_maintenance_report(bucket, "sales", "orders", "current")
            .await
            .expect("current maintenance lookup should succeed")
            .expect("current maintenance job should be stored");

        assert_eq!(latest.job.job_id, report.job.job_id);
        assert_eq!(current_alias.job.job_id, report.job.job_id);
    }

    #[tokio::test]
    async fn maintenance_delete_request_records_failed_job_when_delete_is_disabled() {
        let backend = TestCatalogObjectBackend::default();
        let store = ObjectTableCatalogStore::new(backend.clone());
        let bucket = "analytics";
        let namespace = Namespace::parse("sales").unwrap();
        let table = IdentifierSegment::parse("orders").unwrap();
        let old = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
        let current = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");

        seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current.clone()).await;
        store
            .put_table_maintenance_config(
                bucket,
                "sales",
                "orders",
                TableMaintenanceConfig {
                    version: TABLE_MAINTENANCE_CONFIG_VERSION,
                    retain_recent_metadata_files: 0,
                    delete_enabled: false,
                    background_enabled: false,
                    max_retry_attempts: 2,
                    retry_initial_backoff_seconds: 10,
                    retry_max_backoff_seconds: 30,
                    quarantine_enabled: true,
                    quarantine_retention_seconds: 86_400,
                    ..Default::default()
                },
            )
            .await
            .expect("table maintenance override should persist");
        backend.seed_object(bucket, &old, b"{}".to_vec()).await;
        backend
            .seed_object(bucket, &current, br#"{"metadata-log":[]}"#.to_vec())
            .await;

        let report = store
            .run_table_metadata_maintenance(bucket, "sales", "orders", true, Some("worker-a".to_string()))
            .await
            .expect("disabled delete request should still persist a failed maintenance job");

        assert_eq!(report.job.operation, TableMetadataMaintenanceOperation::Delete);
        assert_eq!(report.job.status, TableMetadataMaintenanceJobStatus::Failed);
        assert_eq!(report.job.config_source, TableMaintenanceConfigSource::TableOverride);
        assert_eq!(report.job.max_retry_attempts, 2);
        assert!(report.job.next_retry_after.is_some());
        assert!(report.job.quarantine_enabled);
        assert_eq!(report.job.quarantine_retention_seconds, 86_400);
        assert!(
            report
                .job
                .failure_reason
                .as_deref()
                .is_some_and(|reason| reason.contains("disabled"))
        );
        assert!(backend.object_exists(bucket, &old).await.unwrap());

        let latest = store
            .get_table_metadata_maintenance_report(bucket, "sales", "orders", "latest")
            .await
            .expect("latest maintenance lookup should succeed")
            .expect("failed maintenance job should be stored");
        assert_eq!(latest.job.job_id, report.job.job_id);
        assert_eq!(latest.job.status, TableMetadataMaintenanceJobStatus::Failed);
    }

    #[tokio::test]
    async fn maintenance_worker_run_skips_when_background_is_disabled() {
        let backend = TestCatalogObjectBackend::default();
        let store = ObjectTableCatalogStore::new(backend.clone());
        let bucket = "analytics";
        let namespace = Namespace::parse("sales").unwrap();
        let table = IdentifierSegment::parse("orders").unwrap();
        let old = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
        let current = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");

        seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current.clone()).await;
        backend.seed_object(bucket, &old, b"{}".to_vec()).await;
        backend
            .seed_object(bucket, &current, br#"{"metadata-log":[]}"#.to_vec())
            .await;

        let report = store
            .run_table_metadata_maintenance_worker_once(bucket, "sales", "orders", "worker-a".to_string())
            .await
            .expect("disabled background worker tick should report a safe no-op");

        assert_eq!(report.job.status, TableMetadataMaintenanceJobStatus::Disabled);
        assert_eq!(report.job.worker_id.as_deref(), Some("worker-a"));
        assert_eq!(report.job.deleted_metadata_file_count, 0);
        assert!(backend.object_exists(bucket, &old).await.unwrap());
    }

    #[tokio::test]
    async fn maintenance_worker_run_honors_paused_config() {
        let backend = TestCatalogObjectBackend::default();
        let store = ObjectTableCatalogStore::new(backend.clone());
        let bucket = "analytics";
        let namespace = Namespace::parse("sales").unwrap();
        let table = IdentifierSegment::parse("orders").unwrap();
        let old = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
        let current = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");

        seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current.clone()).await;
        backend.seed_object(bucket, &old, b"{}".to_vec()).await;
        backend
            .seed_object(bucket, &current, br#"{"metadata-log":[]}"#.to_vec())
            .await;
        store
            .put_table_maintenance_config(
                bucket,
                "sales",
                "orders",
                TableMaintenanceConfig {
                    version: TABLE_MAINTENANCE_CONFIG_VERSION,
                    retain_recent_metadata_files: 0,
                    delete_enabled: true,
                    background_enabled: true,
                    worker_paused: true,
                    ..Default::default()
                },
            )
            .await
            .expect("paused background maintenance config should persist");

        let report = store
            .run_table_metadata_maintenance_worker_once(bucket, "sales", "orders", "worker-a".to_string())
            .await
            .expect("paused worker tick should report a safe no-op");

        assert_eq!(report.job.status, TableMetadataMaintenanceJobStatus::Paused);
        assert_eq!(report.job.operation, TableMetadataMaintenanceOperation::DryRun);
        assert_eq!(report.job.deleted_metadata_file_count, 0);
        assert!(backend.object_exists(bucket, &old).await.unwrap());
    }

    #[tokio::test]
    async fn maintenance_worker_run_defers_until_retry_after() {
        let backend = TestCatalogObjectBackend::default();
        let store = ObjectTableCatalogStore::new(backend.clone());
        let bucket = "analytics";
        let namespace = Namespace::parse("sales").unwrap();
        let table = IdentifierSegment::parse("orders").unwrap();
        let old = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
        let current = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
        let now = OffsetDateTime::UNIX_EPOCH + Duration::seconds(100);

        seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current.clone()).await;
        backend.seed_object(bucket, &old, b"{}".to_vec()).await;
        backend
            .seed_object(bucket, &current, br#"{"metadata-log":[]}"#.to_vec())
            .await;
        store
            .put_table_maintenance_config(
                bucket,
                "sales",
                "orders",
                TableMaintenanceConfig {
                    version: TABLE_MAINTENANCE_CONFIG_VERSION,
                    retain_recent_metadata_files: 0,
                    delete_enabled: false,
                    background_enabled: true,
                    max_retry_attempts: 2,
                    retry_initial_backoff_seconds: 60,
                    retry_max_backoff_seconds: 60,
                    ..Default::default()
                },
            )
            .await
            .expect("retry-enabled maintenance config should persist");
        let mut failed = store
            .run_table_metadata_maintenance(bucket, "sales", "orders", true, Some("worker-a".to_string()))
            .await
            .expect("delete failure should be recorded when delete is disabled");
        failed.job.next_retry_after = Some(maintenance_timestamp(now + Duration::seconds(30)));
        store
            .put_table_metadata_maintenance_report(&failed)
            .await
            .expect("failed retry report should be seeded");

        let deferred = store
            .run_table_metadata_maintenance_worker_once_at(bucket, "sales", "orders", "worker-b".to_string(), now)
            .await
            .expect("worker tick should defer while retry backoff is active");

        assert_eq!(deferred.job.job_id, failed.job.job_id);
        assert_eq!(deferred.job.status, TableMetadataMaintenanceJobStatus::Failed);
        assert_eq!(deferred.job.worker_id.as_deref(), Some("worker-a"));
        assert!(backend.object_exists(bucket, &old).await.unwrap());
    }

    #[tokio::test]
    async fn maintenance_worker_run_backpressures_active_running_job() {
        let backend = TestCatalogObjectBackend::default();
        let store = ObjectTableCatalogStore::new(backend.clone());
        let bucket = "analytics";
        let namespace = Namespace::parse("sales").unwrap();
        let table = IdentifierSegment::parse("orders").unwrap();
        let current = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
        let now = OffsetDateTime::UNIX_EPOCH + Duration::seconds(100);

        seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current.clone()).await;
        backend
            .seed_object(bucket, &current, br#"{"metadata-log":[]}"#.to_vec())
            .await;
        store
            .put_table_maintenance_config(
                bucket,
                "sales",
                "orders",
                TableMaintenanceConfig {
                    version: TABLE_MAINTENANCE_CONFIG_VERSION,
                    retain_recent_metadata_files: 0,
                    delete_enabled: false,
                    background_enabled: true,
                    worker_lease_timeout_seconds: 300,
                    ..Default::default()
                },
            )
            .await
            .expect("background maintenance config should persist");
        let mut running = store
            .plan_table_metadata_maintenance(bucket, "sales", "orders", 0)
            .await
            .expect("maintenance report should be planned");
        running.job.status = TableMetadataMaintenanceJobStatus::Running;
        running.job.worker_id = Some("worker-a".to_string());
        running.job.lease_id = "lease-a".to_string();
        running.job.heartbeat_at = Some(maintenance_timestamp(now - Duration::seconds(10)));
        store
            .put_table_metadata_maintenance_report(&running)
            .await
            .expect("running maintenance report should be seeded");

        let report = store
            .run_table_metadata_maintenance_worker_once_at(bucket, "sales", "orders", "worker-b".to_string(), now)
            .await
            .expect("worker tick should return the active running job");

        assert_eq!(report.job.job_id, running.job.job_id);
        assert_eq!(report.job.status, TableMetadataMaintenanceJobStatus::Running);
        assert_eq!(report.job.worker_id.as_deref(), Some("worker-a"));
    }

    #[tokio::test]
    async fn maintenance_worker_run_recovers_expired_running_job() {
        let backend = TestCatalogObjectBackend::default();
        let store = ObjectTableCatalogStore::new(backend.clone());
        let bucket = "analytics";
        let namespace = Namespace::parse("sales").unwrap();
        let table = IdentifierSegment::parse("orders").unwrap();
        let old = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
        let current = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
        let now = OffsetDateTime::UNIX_EPOCH + Duration::seconds(1000);

        seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current.clone()).await;
        backend.seed_object(bucket, &old, b"{}".to_vec()).await;
        backend
            .seed_object(bucket, &current, br#"{"metadata-log":[]}"#.to_vec())
            .await;
        store
            .put_table_maintenance_config(
                bucket,
                "sales",
                "orders",
                TableMaintenanceConfig {
                    version: TABLE_MAINTENANCE_CONFIG_VERSION,
                    retain_recent_metadata_files: 0,
                    delete_enabled: false,
                    background_enabled: true,
                    worker_lease_timeout_seconds: 60,
                    ..Default::default()
                },
            )
            .await
            .expect("background maintenance config should persist");
        let mut running = store
            .plan_table_metadata_maintenance(bucket, "sales", "orders", 0)
            .await
            .expect("maintenance report should be planned");
        let expired_job_id = running.job.job_id.clone();
        running.job.status = TableMetadataMaintenanceJobStatus::Running;
        running.job.worker_id = Some("worker-a".to_string());
        running.job.lease_id = "lease-a".to_string();
        running.job.heartbeat_at = Some(maintenance_timestamp(now - Duration::seconds(120)));
        store
            .put_table_metadata_maintenance_report(&running)
            .await
            .expect("expired running maintenance report should be seeded");

        let report = store
            .run_table_metadata_maintenance_worker_once_at(bucket, "sales", "orders", "worker-b".to_string(), now)
            .await
            .expect("worker tick should recover expired running job and run again");

        assert_ne!(report.job.job_id, expired_job_id);
        assert_eq!(report.job.status, TableMetadataMaintenanceJobStatus::Successful);
        assert_eq!(report.job.worker_id.as_deref(), Some("worker-b"));

        let expired = store
            .get_table_metadata_maintenance_report(bucket, "sales", "orders", &expired_job_id)
            .await
            .expect("expired job lookup should succeed")
            .expect("expired job should remain addressable");
        assert_eq!(expired.job.status, TableMetadataMaintenanceJobStatus::Failed);
        assert!(
            expired
                .job
                .failure_reason
                .as_deref()
                .is_some_and(|reason| reason.contains("lease expired"))
        );
    }

    #[tokio::test]
    async fn maintenance_worker_heartbeat_updates_current_running_job() {
        let backend = TestCatalogObjectBackend::default();
        let store = ObjectTableCatalogStore::new(backend.clone());
        let bucket = "analytics";
        let namespace = Namespace::parse("sales").unwrap();
        let table = IdentifierSegment::parse("orders").unwrap();
        let current = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
        let first = OffsetDateTime::UNIX_EPOCH + Duration::seconds(100);
        let second = OffsetDateTime::UNIX_EPOCH + Duration::seconds(130);

        seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current).await;
        backend
            .seed_object(
                bucket,
                &default_table_metadata_file_path(&namespace, &table, "00002.metadata.json"),
                br#"{"metadata-log":[]}"#.to_vec(),
            )
            .await;
        let mut running = store
            .plan_table_metadata_maintenance(bucket, "sales", "orders", 0)
            .await
            .expect("maintenance report should be planned");
        running.job.status = TableMetadataMaintenanceJobStatus::Running;
        running.job.worker_id = Some("worker-a".to_string());
        running.job.lease_id = "lease-a".to_string();
        running.job.heartbeat_at = Some(maintenance_timestamp(first));
        let job_id = running.job.job_id.clone();
        store
            .put_table_metadata_maintenance_report(&running)
            .await
            .expect("running maintenance report should be seeded");

        let heartbeat = store
            .heartbeat_table_metadata_maintenance_job_at(
                TableMaintenanceHeartbeatRef {
                    table_bucket: bucket,
                    namespace: "sales",
                    table: "orders",
                    job_id: &job_id,
                    lease_id: "lease-a",
                    worker_id: "worker-a",
                },
                second,
            )
            .await
            .expect("heartbeat should update the current running job");

        assert_eq!(heartbeat.job.job_id, job_id);
        assert_eq!(heartbeat.job.status, TableMetadataMaintenanceJobStatus::Running);
        assert_eq!(heartbeat.job.heartbeat_at.as_deref(), Some(maintenance_timestamp(second).as_str()));
    }

    #[tokio::test]
    async fn maintenance_reachability_reports_manifest_lists_as_manual_review() {
        let backend = TestCatalogObjectBackend::default();
        let store = ObjectTableCatalogStore::new(backend.clone());
        let bucket = "analytics";
        let namespace = Namespace::parse("sales").unwrap();
        let table = IdentifierSegment::parse("orders").unwrap();
        let old = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
        let current = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
        let manifest_list = format!("{}/snap-10.avro", default_table_metadata_dir_path(&namespace, &table));

        seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current.clone()).await;
        backend.seed_object(bucket, &old, b"{}".to_vec()).await;
        backend.seed_object(bucket, &manifest_list, b"avro".to_vec()).await;
        backend
            .seed_object(
                bucket,
                &current,
                serde_json::to_vec(&serde_json::json!({
                    "metadata-log": [],
                    "schemas": [],
                    "partition-specs": [],
                    "sort-orders": [],
                    "snapshots": [
                        {
                            "snapshot-id": 10,
                            "manifest-list": manifest_list
                        }
                    ],
                    "snapshot-log": [
                        {
                            "timestamp-ms": 1,
                            "snapshot-id": 10
                        }
                    ],
                    "refs": {
                        "main": {
                            "snapshot-id": 10,
                            "type": "branch"
                        }
                    }
                }))
                .unwrap(),
            )
            .await;

        let report = store
            .plan_table_metadata_maintenance(bucket, "sales", "orders", 0)
            .await
            .expect("metadata maintenance dry-run should succeed");

        assert_eq!(report.cleanup_candidate_locations, vec![old]);
        let manifest_report = report
            .referenced_object_reports
            .iter()
            .find(|object| object.object_location == manifest_list)
            .expect("manifest list should be reported as a referenced object");
        assert_eq!(manifest_report.object_kind, TableMetadataMaintenanceObjectKind::ManifestList);
        assert_eq!(manifest_report.state, TableMetadataMaintenanceObjectState::ManualReviewRequired);
        assert_eq!(
            manifest_report.reasons,
            vec![
                TableMetadataMaintenanceReason::ManifestList,
                TableMetadataMaintenanceReason::UnsupportedManifestAvro,
            ]
        );
        assert_eq!(
            report.reachability_graph.status,
            TableMaintenanceReachabilityGraphStatus::ManualReviewRequired
        );
        assert_eq!(report.reachability_graph.metadata_file_count, 2);
        assert_eq!(report.reachability_graph.manifest_list_count, 1);
        assert_eq!(report.reachability_graph.manual_review_count, 1);
        assert!(
            report
                .reachability_graph
                .reasons
                .contains(&TableMaintenanceReachabilityGraphReason::ManifestAvroReaderUnavailable)
        );
    }

    #[tokio::test]
    async fn maintenance_dry_run_keeps_metadata_log_references() {
        let backend = TestCatalogObjectBackend::default();
        let store = ObjectTableCatalogStore::new(backend.clone());
        let bucket = "analytics";
        let namespace = Namespace::parse("sales").unwrap();
        let table = IdentifierSegment::parse("orders").unwrap();
        let v1 = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
        let logged = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
        let v3 = default_table_metadata_file_path(&namespace, &table, "00003.metadata.json");
        let current = default_table_metadata_file_path(&namespace, &table, "00004.metadata.json");

        seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current.clone()).await;
        for metadata in [&v1, &logged, &v3] {
            backend.seed_object(bucket, metadata, b"{}".to_vec()).await;
        }
        backend
            .seed_object(
                bucket,
                &current,
                serde_json::to_vec(&serde_json::json!({
                    "metadata-log": [
                        {
                            "timestamp-ms": 1,
                            "metadata-file": logged
                        }
                    ]
                }))
                .unwrap(),
            )
            .await;

        let report = store
            .plan_table_metadata_maintenance(bucket, "sales", "orders", 0)
            .await
            .unwrap();

        assert!(report.retained_metadata_locations.contains(&current));
        assert!(report.retained_metadata_locations.contains(&logged));
        assert_eq!(report.cleanup_candidate_locations, vec![v1, v3]);
    }

    #[tokio::test]
    async fn maintenance_dry_run_keeps_metadata_for_protected_snapshot_refs() {
        let backend = TestCatalogObjectBackend::default();
        let store = ObjectTableCatalogStore::new(backend.clone());
        let bucket = "analytics";
        let namespace = Namespace::parse("sales").unwrap();
        let table = IdentifierSegment::parse("orders").unwrap();
        let orphan = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
        let tagged = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
        let unreferenced = default_table_metadata_file_path(&namespace, &table, "00003.metadata.json");
        let current = default_table_metadata_file_path(&namespace, &table, "00004.metadata.json");

        seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current.clone()).await;
        backend.seed_object(bucket, &orphan, b"{}".to_vec()).await;
        backend
            .seed_object(
                bucket,
                &tagged,
                serde_json::to_vec(&serde_json::json!({
                    "current-snapshot-id": 10
                }))
                .unwrap(),
            )
            .await;
        backend
            .seed_object(
                bucket,
                &unreferenced,
                serde_json::to_vec(&serde_json::json!({
                    "current-snapshot-id": 20
                }))
                .unwrap(),
            )
            .await;
        backend
            .seed_object(
                bucket,
                &current,
                serde_json::to_vec(&serde_json::json!({
                    "current-snapshot-id": 30,
                    "metadata-log": [],
                    "refs": {
                        "main": {
                            "snapshot-id": 30,
                            "type": "branch"
                        },
                        "audit": {
                            "snapshot-id": 10,
                            "type": "tag"
                        }
                    }
                }))
                .unwrap(),
            )
            .await;

        let report = store
            .plan_table_metadata_maintenance(bucket, "sales", "orders", 0)
            .await
            .unwrap();

        assert!(report.retained_metadata_locations.contains(&tagged));
        assert_eq!(report.cleanup_candidate_locations, vec![orphan, unreferenced]);
    }

    #[tokio::test]
    async fn snapshot_expiration_plan_retains_current_recent_and_protected_refs() {
        let backend = TestCatalogObjectBackend::default();
        let store = ObjectTableCatalogStore::new(backend.clone());
        let bucket = "analytics";
        let namespace = Namespace::parse("sales").unwrap();
        let table = IdentifierSegment::parse("orders").unwrap();
        let current = default_table_metadata_file_path(&namespace, &table, "00004.metadata.json");

        seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current.clone()).await;
        backend
            .seed_object(
                bucket,
                &current,
                serde_json::to_vec(&serde_json::json!({
                    "current-snapshot-id": 30,
                    "metadata-log": [],
                    "snapshots": [
                        {
                            "snapshot-id": 10,
                            "timestamp-ms": 1000,
                            "manifest-list": "s3://analytics/tables/table-id/metadata/snap-10.avro"
                        },
                        {
                            "snapshot-id": 20,
                            "timestamp-ms": 2000,
                            "manifest-list": "s3://analytics/tables/table-id/metadata/snap-20.avro"
                        },
                        {
                            "snapshot-id": 30,
                            "timestamp-ms": 3000,
                            "manifest-list": "s3://analytics/tables/table-id/metadata/snap-30.avro"
                        }
                    ],
                    "refs": {
                        "main": {
                            "snapshot-id": 30,
                            "type": "branch"
                        },
                        "audit": {
                            "snapshot-id": 10,
                            "type": "tag"
                        }
                    }
                }))
                .unwrap(),
            )
            .await;

        let report = store
            .plan_table_snapshot_expiration(
                bucket,
                "sales",
                "orders",
                TableSnapshotExpirationConfig {
                    min_snapshots_to_keep: 1,
                    max_snapshot_age_ms: 1,
                },
            )
            .await
            .expect("snapshot expiration planning should succeed");

        let current_snapshot = snapshot_expiration_report(&report, 30);
        assert_eq!(current_snapshot.state, TableSnapshotExpirationSnapshotState::Retained);
        assert!(
            current_snapshot
                .reasons
                .contains(&TableSnapshotExpirationReason::CurrentSnapshot)
        );

        let protected_snapshot = snapshot_expiration_report(&report, 10);
        assert_eq!(protected_snapshot.state, TableSnapshotExpirationSnapshotState::ManualReviewRequired);
        assert!(
            protected_snapshot
                .reasons
                .contains(&TableSnapshotExpirationReason::ProtectedSnapshotRef)
        );
        assert!(
            protected_snapshot
                .reasons
                .contains(&TableSnapshotExpirationReason::UserDefinedSnapshotRef)
        );

        let expired_snapshot = snapshot_expiration_report(&report, 20);
        assert_eq!(expired_snapshot.state, TableSnapshotExpirationSnapshotState::ExpirationCandidate);
        assert!(
            expired_snapshot
                .reasons
                .contains(&TableSnapshotExpirationReason::SnapshotAgeExpired)
        );
        assert_eq!(report.expiration_candidate_count, 1);
        assert_eq!(report.manual_review_count, 1);
    }

    #[tokio::test]
    async fn snapshot_expiration_plan_fails_closed_for_table_retention_property_conflicts() {
        let backend = TestCatalogObjectBackend::default();
        let store = ObjectTableCatalogStore::new(backend.clone());
        let bucket = "analytics";
        let namespace = Namespace::parse("sales").unwrap();
        let table = IdentifierSegment::parse("orders").unwrap();
        let current = default_table_metadata_file_path(&namespace, &table, "00004.metadata.json");

        seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current.clone()).await;
        backend
            .seed_object(
                bucket,
                &current,
                serde_json::to_vec(&serde_json::json!({
                    "current-snapshot-id": 20,
                    "metadata-log": [],
                    "properties": {
                        "history.expire.min-snapshots-to-keep": "5"
                    },
                    "snapshots": [
                        {
                            "snapshot-id": 10,
                            "timestamp-ms": 1000,
                            "manifest-list": "s3://analytics/tables/table-id/metadata/snap-10.avro"
                        },
                        {
                            "snapshot-id": 20,
                            "timestamp-ms": 2000,
                            "manifest-list": "s3://analytics/tables/table-id/metadata/snap-20.avro"
                        }
                    ],
                    "refs": {
                        "main": {
                            "snapshot-id": 20,
                            "type": "branch"
                        }
                    }
                }))
                .unwrap(),
            )
            .await;

        let report = store
            .plan_table_snapshot_expiration(
                bucket,
                "sales",
                "orders",
                TableSnapshotExpirationConfig {
                    min_snapshots_to_keep: 1,
                    max_snapshot_age_ms: 1,
                },
            )
            .await
            .expect("snapshot expiration planning should succeed");

        assert_eq!(report.expiration_candidate_count, 0);
        assert_eq!(report.manual_review_count, 2);
        for snapshot in &report.snapshot_reports {
            assert_eq!(snapshot.state, TableSnapshotExpirationSnapshotState::ManualReviewRequired);
            assert!(
                snapshot
                    .reasons
                    .contains(&TableSnapshotExpirationReason::TableRetentionPropertyConflict)
            );
        }
    }

    #[tokio::test]
    async fn snapshot_expiration_plan_requires_snapshot_timestamps() {
        let backend = TestCatalogObjectBackend::default();
        let store = ObjectTableCatalogStore::new(backend.clone());
        let bucket = "analytics";
        let namespace = Namespace::parse("sales").unwrap();
        let table = IdentifierSegment::parse("orders").unwrap();
        let current = default_table_metadata_file_path(&namespace, &table, "00004.metadata.json");

        seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current.clone()).await;
        backend
            .seed_object(
                bucket,
                &current,
                serde_json::to_vec(&serde_json::json!({
                    "current-snapshot-id": 20,
                    "metadata-log": [],
                    "snapshots": [
                        {
                            "snapshot-id": 10,
                            "manifest-list": "s3://analytics/tables/table-id/metadata/snap-10.avro"
                        },
                        {
                            "snapshot-id": 20,
                            "timestamp-ms": 2000,
                            "manifest-list": "s3://analytics/tables/table-id/metadata/snap-20.avro"
                        }
                    ],
                    "refs": {
                        "main": {
                            "snapshot-id": 20,
                            "type": "branch"
                        }
                    }
                }))
                .unwrap(),
            )
            .await;

        let report = store
            .plan_table_snapshot_expiration(
                bucket,
                "sales",
                "orders",
                TableSnapshotExpirationConfig {
                    min_snapshots_to_keep: 1,
                    max_snapshot_age_ms: 1,
                },
            )
            .await
            .expect("snapshot expiration planning should succeed");

        let missing_timestamp = snapshot_expiration_report(&report, 10);
        assert_eq!(missing_timestamp.state, TableSnapshotExpirationSnapshotState::ManualReviewRequired);
        assert!(
            missing_timestamp
                .reasons
                .contains(&TableSnapshotExpirationReason::MissingSnapshotTimestamp)
        );
        assert_eq!(report.expiration_candidate_count, 0);
        assert_eq!(report.manual_review_count, 1);
    }

    #[tokio::test]
    async fn compaction_plan_reports_manifest_reader_gap_without_rewrite_candidates() {
        let backend = TestCatalogObjectBackend::default();
        let store = ObjectTableCatalogStore::new(backend.clone());
        let bucket = "analytics";
        let namespace = Namespace::parse("sales").unwrap();
        let table = IdentifierSegment::parse("orders").unwrap();
        let current = default_table_metadata_file_path(&namespace, &table, "00004.metadata.json");

        seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current.clone()).await;
        backend
            .seed_object(
                bucket,
                &current,
                serde_json::to_vec(&serde_json::json!({
                    "current-snapshot-id": 20,
                    "metadata-log": [],
                    "snapshots": [
                        {
                            "snapshot-id": 20,
                            "timestamp-ms": 2000,
                            "manifest-list": "s3://analytics/tables/table-id/metadata/snap-20.avro"
                        }
                    ],
                    "refs": {
                        "main": {
                            "snapshot-id": 20,
                            "type": "branch"
                        }
                    }
                }))
                .unwrap(),
            )
            .await;

        let report = store
            .plan_table_compaction(
                bucket,
                "sales",
                "orders",
                TableCompactionPlanningConfig {
                    target_file_size_bytes: 512 * 1024 * 1024,
                    small_file_threshold_bytes: 64 * 1024 * 1024,
                    min_input_files: 2,
                    max_rewrite_bytes_per_job: 1024 * 1024 * 1024,
                },
            )
            .await
            .expect("compaction planning should succeed");

        assert_eq!(report.status, TableCompactionPlanningStatus::ManualReviewRequired);
        assert_eq!(report.candidate_file_count, 0);
        assert_eq!(report.rewrite_group_count, 0);
        assert_eq!(report.manual_review_count, 1);
        let snapshot = compaction_snapshot_report(&report, 20);
        assert_eq!(snapshot.status, TableCompactionPlanningStatus::ManualReviewRequired);
        assert!(snapshot.reasons.contains(&TableCompactionPlanningReason::ManifestList));
        assert!(
            snapshot
                .reasons
                .contains(&TableCompactionPlanningReason::ManifestAvroReaderUnavailable)
        );
    }

    #[tokio::test]
    async fn compaction_plan_requires_current_snapshot_metadata() {
        let backend = TestCatalogObjectBackend::default();
        let store = ObjectTableCatalogStore::new(backend.clone());
        let bucket = "analytics";
        let namespace = Namespace::parse("sales").unwrap();
        let table = IdentifierSegment::parse("orders").unwrap();
        let current = default_table_metadata_file_path(&namespace, &table, "00004.metadata.json");

        seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current.clone()).await;
        backend
            .seed_object(
                bucket,
                &current,
                serde_json::to_vec(&serde_json::json!({
                    "current-snapshot-id": 30,
                    "metadata-log": [],
                    "snapshots": [
                        {
                            "snapshot-id": 20,
                            "timestamp-ms": 2000,
                            "manifest-list": "s3://analytics/tables/table-id/metadata/snap-20.avro"
                        }
                    ]
                }))
                .unwrap(),
            )
            .await;

        let report = store
            .plan_table_compaction(
                bucket,
                "sales",
                "orders",
                TableCompactionPlanningConfig {
                    target_file_size_bytes: 512 * 1024 * 1024,
                    small_file_threshold_bytes: 64 * 1024 * 1024,
                    min_input_files: 2,
                    max_rewrite_bytes_per_job: 1024 * 1024 * 1024,
                },
            )
            .await
            .expect("compaction planning should succeed");

        assert_eq!(report.status, TableCompactionPlanningStatus::ManualReviewRequired);
        assert_eq!(report.manual_review_count, 1);
        let snapshot = compaction_snapshot_report(&report, 30);
        assert!(
            snapshot
                .reasons
                .contains(&TableCompactionPlanningReason::MissingCurrentSnapshot)
        );
    }

    #[tokio::test]
    async fn maintenance_dry_run_keeps_recent_metadata_files_and_ignores_non_metadata_objects() {
        let backend = TestCatalogObjectBackend::default();
        let store = ObjectTableCatalogStore::new(backend.clone());
        let bucket = "analytics";
        let namespace = Namespace::parse("sales").unwrap();
        let table = IdentifierSegment::parse("orders").unwrap();
        let v1 = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
        let v2 = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
        let recent = default_table_metadata_file_path(&namespace, &table, "00003.metadata.json");
        let current = default_table_metadata_file_path(&namespace, &table, "00004.metadata.json");
        let manifest = format!("{}/snap-1.avro", default_table_metadata_dir_path(&namespace, &table));

        seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current.clone()).await;
        for metadata in [&v1, &v2, &recent] {
            backend.seed_object(bucket, metadata, b"{}".to_vec()).await;
        }
        backend
            .seed_object(bucket, &current, br#"{"metadata-log":[]}"#.to_vec())
            .await;
        backend.seed_object(bucket, &manifest, b"manifest".to_vec()).await;

        let report = store
            .plan_table_metadata_maintenance(bucket, "sales", "orders", 2)
            .await
            .unwrap();

        assert!(report.retained_metadata_locations.contains(&recent));
        assert!(report.retained_metadata_locations.contains(&current));
        assert_eq!(report.cleanup_candidate_locations, vec![v1, v2]);
        assert!(!report.cleanup_candidate_locations.contains(&manifest));
    }

    #[tokio::test]
    async fn maintenance_delete_removes_only_dry_run_metadata_candidates() {
        let backend = TestCatalogObjectBackend::default();
        let store = ObjectTableCatalogStore::new(backend.clone());
        let bucket = "analytics";
        let namespace = Namespace::parse("sales").unwrap();
        let table = IdentifierSegment::parse("orders").unwrap();
        let old = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
        let retained = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
        let current = default_table_metadata_file_path(&namespace, &table, "00003.metadata.json");
        let manifest = format!("{}/snap-1.avro", default_table_metadata_dir_path(&namespace, &table));

        seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current.clone()).await;
        backend.seed_object(bucket, &old, b"{}".to_vec()).await;
        backend.seed_object(bucket, &retained, b"{}".to_vec()).await;
        backend
            .seed_object(
                bucket,
                &current,
                serde_json::to_vec(&serde_json::json!({
                    "metadata-log": [
                        {
                            "timestamp-ms": 1,
                            "metadata-file": retained
                        }
                    ]
                }))
                .unwrap(),
            )
            .await;
        backend.seed_object(bucket, &manifest, b"manifest".to_vec()).await;

        let report = store
            .delete_table_metadata_maintenance_candidates(bucket, "sales", "orders", 0)
            .await
            .unwrap();

        assert_eq!(report.cleanup_candidate_locations, vec![old.clone()]);
        assert!(!backend.object_exists(bucket, &old).await.unwrap());
        assert!(backend.object_exists(bucket, &retained).await.unwrap());
        assert!(backend.object_exists(bucket, &current).await.unwrap());
        assert!(backend.object_exists(bucket, &manifest).await.unwrap());
    }

    #[tokio::test]
    async fn maintenance_delete_skips_recent_uncommitted_metadata_candidates() {
        let backend = TestCatalogObjectBackend::default();
        let store = ObjectTableCatalogStore::new(backend.clone());
        let bucket = "analytics";
        let namespace = Namespace::parse("sales").unwrap();
        let table = IdentifierSegment::parse("orders").unwrap();
        let old = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
        let current = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
        let fresh = default_table_metadata_file_path(&namespace, &table, "00003.metadata.json");

        seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current.clone()).await;
        backend.seed_object(bucket, &old, b"{}".to_vec()).await;
        backend
            .seed_object(bucket, &current, br#"{"metadata-log":[]}"#.to_vec())
            .await;
        backend
            .seed_object_with_mod_time(bucket, &fresh, br#"{"metadata-log":[]}"#.to_vec(), Some(OffsetDateTime::now_utc()))
            .await;

        let report = store
            .delete_table_metadata_maintenance_candidates(bucket, "sales", "orders", 0)
            .await
            .unwrap();

        assert_eq!(report.cleanup_candidate_locations, vec![old.clone()]);
        assert!(!backend.object_exists(bucket, &old).await.unwrap());
        assert!(backend.object_exists(bucket, &fresh).await.unwrap());
    }

    #[tokio::test]
    async fn maintenance_delete_does_not_expand_beyond_planned_deletable_candidates() {
        let backend = TestCatalogObjectBackend::default();
        let store = ObjectTableCatalogStore::new(backend.clone());
        let bucket = "analytics";
        let namespace = Namespace::parse("sales").unwrap();
        let table = IdentifierSegment::parse("orders").unwrap();
        let old = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
        let current = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
        let fresh = default_table_metadata_file_path(&namespace, &table, "00003.metadata.json");

        seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current.clone()).await;
        backend.seed_object(bucket, &old, b"{}".to_vec()).await;
        backend
            .seed_object(bucket, &current, br#"{"metadata-log":[]}"#.to_vec())
            .await;
        backend
            .seed_object_with_mod_time(bucket, &fresh, b"{}".to_vec(), Some(OffsetDateTime::now_utc()))
            .await;

        let report = store
            .plan_table_metadata_maintenance(bucket, "sales", "orders", 0)
            .await
            .unwrap();
        assert_eq!(report.cleanup_candidate_locations, vec![old.clone(), fresh.clone()]);
        assert_eq!(report.deletable_metadata_locations, vec![old.clone()]);

        backend.seed_object(bucket, &fresh, b"{}".to_vec()).await;
        let deleted = store
            .delete_table_metadata_maintenance_report(bucket, "sales", "orders", report)
            .await
            .unwrap();

        assert_eq!(deleted.job.operation, TableMetadataMaintenanceOperation::Delete);
        assert_eq!(deleted.job.status, TableMetadataMaintenanceJobStatus::Successful);
        assert_eq!(deleted.job.deleted_metadata_file_count, 1);
        assert_eq!(deleted.cleanup_candidate_locations, vec![old.clone()]);
        assert_eq!(deleted.deletable_metadata_locations, vec![old.clone()]);
        assert_eq!(
            maintenance_object_report(&deleted, &old).state,
            TableMetadataMaintenanceObjectState::Deleted
        );
        assert_eq!(
            maintenance_object_report(&deleted, &fresh).state,
            TableMetadataMaintenanceObjectState::PendingSafetyWindow
        );
        assert!(!backend.object_exists(bucket, &old).await.unwrap());
        assert!(backend.object_exists(bucket, &fresh).await.unwrap());
    }

    #[tokio::test]
    async fn maintenance_delete_conflicts_when_current_pointer_changes_before_delete() {
        let backend = TestCatalogObjectBackend::default();
        let store = ObjectTableCatalogStore::new(backend.clone());
        let bucket = "analytics";
        let namespace = Namespace::parse("sales").unwrap();
        let table = IdentifierSegment::parse("orders").unwrap();
        let old = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
        let current = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
        let next = default_table_metadata_file_path(&namespace, &table, "00003.metadata.json");

        seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current.clone()).await;
        backend.seed_object(bucket, &old, b"{}".to_vec()).await;
        backend
            .seed_object(bucket, &current, br#"{"metadata-log":[]}"#.to_vec())
            .await;

        let report = store
            .plan_table_metadata_maintenance(bucket, "sales", "orders", 0)
            .await
            .unwrap();
        assert_eq!(report.cleanup_candidate_locations, vec![old.clone()]);

        backend.seed_object(bucket, &next, br#"{"metadata-log":[]}"#.to_vec()).await;
        store
            .commit_table(TableCommitRequest {
                table_bucket: bucket.to_string(),
                namespace: namespace.public_name(),
                table: table.as_str().to_string(),
                commit_id: "commit-id".to_string(),
                idempotency_key: None,
                operation: "append".to_string(),
                expected_version_token: "token-v1".to_string(),
                expected_metadata_location: current,
                new_metadata_location: next,
                requirements: Vec::new(),
                writer: Some("test".to_string()),
            })
            .await
            .unwrap();

        let err = store
            .delete_table_metadata_maintenance_report(bucket, "sales", "orders", report)
            .await
            .unwrap_err();

        assert_matches!(err, TableCatalogStoreError::Conflict(_));
        assert!(backend.object_exists(bucket, &old).await.unwrap());
    }

    #[tokio::test]
    async fn export_catalog_entry_contains_table_identity_and_pointer() {
        let backend = TestCatalogObjectBackend::default();
        let store = ObjectTableCatalogStore::new(backend);
        let bucket = "analytics";
        let namespace = Namespace::parse("sales").unwrap();
        let table = IdentifierSegment::parse("orders").unwrap();
        let current = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");

        seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current.clone()).await;

        let export = store.export_table_catalog_entry(bucket, "sales", "orders").await.unwrap();

        assert_eq!(export.table_bucket.table_bucket, bucket);
        assert_eq!(export.namespace.namespace, "sales");
        assert_eq!(export.table.namespace, "sales");
        assert_eq!(export.table.table, "orders");
        assert_eq!(export.table.table_id, "table-id");
        assert_eq!(export.table.table_uuid, "table-uuid");
        assert_eq!(export.table.metadata_location, current);
        assert_eq!(export.table.version_token, "token-v1");
        assert_eq!(export.table.generation, 1);
    }

    #[tokio::test]
    async fn consistency_check_reports_missing_metadata_object() {
        let backend = TestCatalogObjectBackend::default();
        let store = ObjectTableCatalogStore::new(backend);
        let bucket = "analytics";
        let namespace = Namespace::parse("sales").unwrap();
        let table = IdentifierSegment::parse("orders").unwrap();
        let current = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");

        seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current.clone()).await;

        let report = store.diagnose_table_catalog(bucket, "sales", "orders", 0).await.unwrap();

        assert_eq!(report.catalog.table.metadata_location, current.clone());
        assert_eq!(report.current_metadata_status, TableMetadataPointerStatus::MissingObject);
        assert_eq!(report.recovery_status, TableCatalogRecoveryStatus::ReadOnlyRecommended);
        assert_eq!(report.recommended_actions, vec![TableCatalogRecoveryAction::RestoreCurrentMetadataObject]);
        assert!(report.orphan_metadata_candidate_locations.is_empty());
    }

    #[tokio::test]
    async fn consistency_check_reports_invalid_metadata_location() {
        let backend = TestCatalogObjectBackend::default();
        let store = ObjectTableCatalogStore::new(backend);
        let bucket = "analytics";
        let namespace = Namespace::parse("sales").unwrap();
        let table = IdentifierSegment::parse("orders").unwrap();
        let invalid_metadata = ".rustfs-table/warehouses/default/namespaces/sales/tables/other/metadata/00001.metadata.json";

        store.put_table_bucket(test_bucket_entry(bucket)).await.unwrap();
        store
            .create_namespace(test_namespace_entry(bucket, &namespace))
            .await
            .unwrap();
        store
            .create_table(test_table_entry(bucket, &namespace, &table, invalid_metadata.to_string()))
            .await
            .unwrap();

        let report = store.diagnose_table_catalog(bucket, "sales", "orders", 0).await.unwrap();

        assert_eq!(report.catalog.table.metadata_location, invalid_metadata);
        assert_eq!(report.current_metadata_status, TableMetadataPointerStatus::InvalidLocation);
        assert!(report.orphan_metadata_candidate_locations.is_empty());
    }

    #[tokio::test]
    async fn orphan_metadata_scan_does_not_treat_largest_version_as_committed() {
        let backend = TestCatalogObjectBackend::default();
        let store = ObjectTableCatalogStore::new(backend.clone());
        let bucket = "analytics";
        let namespace = Namespace::parse("sales").unwrap();
        let table = IdentifierSegment::parse("orders").unwrap();
        let current = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
        let uncommitted = default_table_metadata_file_path(&namespace, &table, "00003.metadata.json");

        seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current.clone()).await;
        backend
            .seed_object(bucket, &current, br#"{"metadata-log":[]}"#.to_vec())
            .await;
        backend
            .seed_object(bucket, &uncommitted, br#"{"metadata-log":[]}"#.to_vec())
            .await;

        let report = store.diagnose_table_catalog(bucket, "sales", "orders", 0).await.unwrap();

        assert_eq!(report.current_metadata_status, TableMetadataPointerStatus::Valid);
        assert_eq!(report.catalog.table.metadata_location, current);
        assert_eq!(report.orphan_metadata_candidate_locations, vec![uncommitted]);
    }

    #[tokio::test]
    async fn orphan_metadata_scan_keeps_metadata_for_protected_snapshot_refs() {
        let backend = TestCatalogObjectBackend::default();
        let store = ObjectTableCatalogStore::new(backend.clone());
        let bucket = "analytics";
        let namespace = Namespace::parse("sales").unwrap();
        let table = IdentifierSegment::parse("orders").unwrap();
        let orphan = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
        let tagged = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
        let current = default_table_metadata_file_path(&namespace, &table, "00003.metadata.json");

        seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current.clone()).await;
        backend.seed_object(bucket, &orphan, b"{}".to_vec()).await;
        backend
            .seed_object(
                bucket,
                &tagged,
                serde_json::to_vec(&serde_json::json!({
                    "current-snapshot-id": 10
                }))
                .unwrap(),
            )
            .await;
        backend
            .seed_object(
                bucket,
                &current,
                serde_json::to_vec(&serde_json::json!({
                    "current-snapshot-id": 20,
                    "metadata-log": [],
                    "refs": {
                        "audit": {
                            "snapshot-id": 10,
                            "type": "tag"
                        }
                    }
                }))
                .unwrap(),
            )
            .await;

        let report = store.diagnose_table_catalog(bucket, "sales", "orders", 0).await.unwrap();

        assert_eq!(report.current_metadata_status, TableMetadataPointerStatus::Valid);
        assert_eq!(report.orphan_metadata_candidate_locations, vec![orphan]);
    }

    #[tokio::test]
    async fn object_table_catalog_store_commits_with_token_match_and_writes_log() {
        let backend = TestCatalogObjectBackend::default();
        let store = ObjectTableCatalogStore::new(backend.clone());
        let bucket = "analytics";
        let namespace = Namespace::parse("sales").unwrap();
        let table = IdentifierSegment::parse("orders").unwrap();
        let current_metadata = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
        let new_metadata = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");

        store.put_table_bucket(test_bucket_entry(bucket)).await.unwrap();
        store
            .create_namespace(test_namespace_entry(bucket, &namespace))
            .await
            .unwrap();
        store
            .create_table(test_table_entry(bucket, &namespace, &table, current_metadata.clone()))
            .await
            .unwrap();
        backend.seed_object(bucket, &new_metadata, b"{}".to_vec()).await;

        let result = store
            .commit_table(TableCommitRequest {
                table_bucket: bucket.to_string(),
                namespace: namespace.public_name(),
                table: table.as_str().to_string(),
                commit_id: "commit-1".to_string(),
                idempotency_key: Some("client/%2f\nrequest".to_string()),
                operation: "append".to_string(),
                expected_version_token: "token-v1".to_string(),
                expected_metadata_location: current_metadata,
                new_metadata_location: new_metadata.clone(),
                requirements: vec![serde_json::json!({"type": "assert-table-uuid", "uuid": "table-uuid"})],
                writer: Some("pyiceberg/test".to_string()),
            })
            .await
            .unwrap();

        assert_eq!(result.table.metadata_location, new_metadata);
        assert_ne!(result.table.version_token, "token-v1");
        assert_eq!(result.table.generation, 2);
        assert_eq!(result.commit_log.status, CommitLogStatus::Committed);

        let loaded = store.load_table(bucket, "sales", "orders").await.unwrap().unwrap();
        assert_eq!(loaded.metadata_location, result.table.metadata_location);
        assert_eq!(loaded.version_token, result.table.version_token);
        assert!(
            store
                .get_commit_by_id(bucket, "table-id", "commit-1")
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            store
                .get_commit_by_idempotency_key(bucket, "table-id", "client/%2f\nrequest")
                .await
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn object_table_catalog_store_syncs_warehouse_location_from_committed_metadata() {
        let backend = TestCatalogObjectBackend::default();
        let store = ObjectTableCatalogStore::new(backend.clone());
        let bucket = "analytics";
        let namespace = Namespace::parse("sales").unwrap();
        let table = IdentifierSegment::parse("orders").unwrap();
        let current_metadata = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
        let new_metadata = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");

        store.put_table_bucket(test_bucket_entry(bucket)).await.unwrap();
        store
            .create_namespace(test_namespace_entry(bucket, &namespace))
            .await
            .unwrap();
        store
            .create_table(test_table_entry(bucket, &namespace, &table, current_metadata.clone()))
            .await
            .unwrap();
        backend
            .seed_object(
                bucket,
                &new_metadata,
                serde_json::to_vec(&serde_json::json!({
                    "location": "s3://analytics/tables/relocated-table-id",
                    "table-uuid": "table-uuid"
                }))
                .unwrap(),
            )
            .await;

        let result = store
            .commit_table(TableCommitRequest {
                table_bucket: bucket.to_string(),
                namespace: namespace.public_name(),
                table: table.as_str().to_string(),
                commit_id: "commit-1".to_string(),
                idempotency_key: None,
                operation: "set-location".to_string(),
                expected_version_token: "token-v1".to_string(),
                expected_metadata_location: current_metadata,
                new_metadata_location: new_metadata,
                requirements: vec![serde_json::json!({"type": "assert-table-uuid", "uuid": "table-uuid"})],
                writer: Some("pyiceberg/test".to_string()),
            })
            .await
            .unwrap();

        assert_eq!(result.table.warehouse_location, "s3://analytics/tables/relocated-table-id");
        let resource = table_data_plane_resource_for_object(&store, bucket, "tables/relocated-table-id/data/part-00001.parquet")
            .await
            .expect("data-plane resource lookup should succeed")
            .expect("relocated table warehouse object should resolve to the table");
        assert_eq!(resource.table, "orders");
        assert_eq!(resource.warehouse_object_prefix, "tables/relocated-table-id/");
    }

    #[tokio::test]
    async fn object_table_catalog_store_does_not_advance_table_when_idempotency_staging_fails() {
        let backend = TestCatalogObjectBackend::default();
        let store = ObjectTableCatalogStore::new(backend.clone());
        let bucket = "analytics";
        let namespace = Namespace::parse("sales").unwrap();
        let table = IdentifierSegment::parse("orders").unwrap();
        let current_metadata = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
        let new_metadata = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
        let idempotency_key = "client-request";
        let idempotency_path =
            TableCatalogObjectPaths::default().commit_idempotency_entry_path(bucket, "table-id", idempotency_key);

        store.put_table_bucket(test_bucket_entry(bucket)).await.unwrap();
        store
            .create_namespace(test_namespace_entry(bucket, &namespace))
            .await
            .unwrap();
        store
            .create_table(test_table_entry(bucket, &namespace, &table, current_metadata.clone()))
            .await
            .unwrap();
        backend.seed_object(bucket, &new_metadata, b"{}".to_vec()).await;
        backend
            .fail_put_attempt(rustfs_ecstore::disk::RUSTFS_META_BUCKET, &idempotency_path, 1)
            .await;

        let err = store
            .commit_table(TableCommitRequest {
                table_bucket: bucket.to_string(),
                namespace: namespace.public_name(),
                table: table.as_str().to_string(),
                commit_id: "commit-1".to_string(),
                idempotency_key: Some(idempotency_key.to_string()),
                operation: "append".to_string(),
                expected_version_token: "token-v1".to_string(),
                expected_metadata_location: current_metadata.clone(),
                new_metadata_location: new_metadata,
                requirements: Vec::new(),
                writer: None,
            })
            .await
            .unwrap_err();

        assert_matches!(err, TableCatalogStoreError::Internal(_));
        let loaded = store.load_table(bucket, "sales", "orders").await.unwrap().unwrap();
        assert_eq!(loaded.metadata_location, current_metadata);
        assert_eq!(loaded.version_token, "token-v1");
        let staged = store.get_commit_by_id(bucket, "table-id", "commit-1").await.unwrap().unwrap();
        assert_eq!(staged.status, CommitLogStatus::Staged);
    }

    #[tokio::test]
    async fn object_table_catalog_store_recovers_staged_commit_after_post_cas_finalization_failure() {
        let backend = TestCatalogObjectBackend::default();
        let store = ObjectTableCatalogStore::new(backend.clone());
        let bucket = "analytics";
        let namespace = Namespace::parse("sales").unwrap();
        let table = IdentifierSegment::parse("orders").unwrap();
        let current_metadata = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
        let new_metadata = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
        let commit_path = TableCatalogObjectPaths::default().commit_log_entry_path(bucket, "table-id", "commit-1");

        store.put_table_bucket(test_bucket_entry(bucket)).await.unwrap();
        store
            .create_namespace(test_namespace_entry(bucket, &namespace))
            .await
            .unwrap();
        store
            .create_table(test_table_entry(bucket, &namespace, &table, current_metadata.clone()))
            .await
            .unwrap();
        backend.seed_object(bucket, &new_metadata, b"{}".to_vec()).await;
        backend
            .fail_put_attempt(rustfs_ecstore::disk::RUSTFS_META_BUCKET, &commit_path, 2)
            .await;

        let request = TableCommitRequest {
            table_bucket: bucket.to_string(),
            namespace: namespace.public_name(),
            table: table.as_str().to_string(),
            commit_id: "commit-1".to_string(),
            idempotency_key: None,
            operation: "append".to_string(),
            expected_version_token: "token-v1".to_string(),
            expected_metadata_location: current_metadata,
            new_metadata_location: new_metadata.clone(),
            requirements: Vec::new(),
            writer: None,
        };

        let result = store.commit_table(request.clone()).await.unwrap();

        assert_eq!(result.table.metadata_location, new_metadata);
        assert_eq!(result.commit_log.status, CommitLogStatus::Committed);
        let loaded = store.load_table(bucket, "sales", "orders").await.unwrap().unwrap();
        assert_eq!(loaded.version_token, result.table.version_token);
        let staged = store.get_commit_by_id(bucket, "table-id", "commit-1").await.unwrap().unwrap();
        assert_eq!(staged.status, CommitLogStatus::Staged);

        let retry = store.commit_table(request).await.unwrap();
        assert_eq!(retry.table.version_token, result.table.version_token);
        assert_eq!(retry.commit_log.status, CommitLogStatus::Committed);
        let committed = store.get_commit_by_id(bucket, "table-id", "commit-1").await.unwrap().unwrap();
        assert_eq!(committed.status, CommitLogStatus::Committed);
    }

    #[tokio::test]
    async fn table_commit_recovery_reports_post_cas_staged_commit() {
        let backend = TestCatalogObjectBackend::default();
        let store = ObjectTableCatalogStore::new(backend.clone());
        let bucket = "analytics";
        let namespace = Namespace::parse("sales").unwrap();
        let table = IdentifierSegment::parse("orders").unwrap();
        let current_metadata = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
        let new_metadata = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
        let commit_path = TableCatalogObjectPaths::default().commit_log_entry_path(bucket, "table-id", "commit-1");

        store.put_table_bucket(test_bucket_entry(bucket)).await.unwrap();
        store
            .create_namespace(test_namespace_entry(bucket, &namespace))
            .await
            .unwrap();
        store
            .create_table(test_table_entry(bucket, &namespace, &table, current_metadata.clone()))
            .await
            .unwrap();
        backend.seed_object(bucket, &new_metadata, b"{}".to_vec()).await;
        backend
            .fail_put_attempt(rustfs_ecstore::disk::RUSTFS_META_BUCKET, &commit_path, 2)
            .await;

        let result = store
            .commit_table(TableCommitRequest {
                table_bucket: bucket.to_string(),
                namespace: namespace.public_name(),
                table: table.as_str().to_string(),
                commit_id: "commit-1".to_string(),
                idempotency_key: None,
                operation: "append".to_string(),
                expected_version_token: "token-v1".to_string(),
                expected_metadata_location: current_metadata,
                new_metadata_location: new_metadata.clone(),
                requirements: Vec::new(),
                writer: None,
            })
            .await
            .unwrap();
        assert_eq!(result.commit_log.status, CommitLogStatus::Committed);

        let report = store.plan_table_commit_recovery(bucket, "sales", "orders").await.unwrap();

        assert_eq!(report.current_metadata_location, new_metadata);
        assert_eq!(report.finalization_required_count, 1);
        assert_eq!(report.manual_review_count, 0);
        assert_eq!(report.commits.len(), 1);
        assert_eq!(report.commits[0].commit_id, "commit-1");
        assert_eq!(report.commits[0].status, CommitLogStatus::Staged);
        assert_eq!(report.commits[0].recovery_state, TableCommitRecoveryState::FinalizationRequired);
    }

    #[tokio::test]
    async fn diagnostics_report_includes_table_commit_recovery_state() {
        let backend = TestCatalogObjectBackend::default();
        let store = ObjectTableCatalogStore::new(backend.clone());
        let bucket = "analytics";
        let namespace = Namespace::parse("sales").unwrap();
        let table = IdentifierSegment::parse("orders").unwrap();
        let current_metadata = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
        let new_metadata = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
        let commit_path = TableCatalogObjectPaths::default().commit_log_entry_path(bucket, "table-id", "commit-1");

        store.put_table_bucket(test_bucket_entry(bucket)).await.unwrap();
        store
            .create_namespace(test_namespace_entry(bucket, &namespace))
            .await
            .unwrap();
        store
            .create_table(test_table_entry(bucket, &namespace, &table, current_metadata.clone()))
            .await
            .unwrap();
        backend.seed_object(bucket, &new_metadata, b"{}".to_vec()).await;
        backend
            .fail_put_attempt(rustfs_ecstore::disk::RUSTFS_META_BUCKET, &commit_path, 2)
            .await;

        store
            .commit_table(TableCommitRequest {
                table_bucket: bucket.to_string(),
                namespace: namespace.public_name(),
                table: table.as_str().to_string(),
                commit_id: "commit-1".to_string(),
                idempotency_key: None,
                operation: "append".to_string(),
                expected_version_token: "token-v1".to_string(),
                expected_metadata_location: current_metadata,
                new_metadata_location: new_metadata,
                requirements: Vec::new(),
                writer: None,
            })
            .await
            .unwrap();

        let report = store.diagnose_table_catalog(bucket, "sales", "orders", 0).await.unwrap();

        assert_eq!(report.recovery_status, TableCatalogRecoveryStatus::Recoverable);
        assert_eq!(report.recommended_actions, vec![TableCatalogRecoveryAction::RunCommitRecovery]);
        assert_eq!(report.commit_recovery.finalization_required_count, 1);
        assert_eq!(report.commit_recovery.commits.len(), 1);
        assert_eq!(
            report.commit_recovery.commits[0].recovery_state,
            TableCommitRecoveryState::FinalizationRequired
        );
    }

    #[tokio::test]
    async fn table_commit_recovery_finalizes_post_cas_staged_commit() {
        let backend = TestCatalogObjectBackend::default();
        let store = ObjectTableCatalogStore::new(backend.clone());
        let bucket = "analytics";
        let namespace = Namespace::parse("sales").unwrap();
        let table = IdentifierSegment::parse("orders").unwrap();
        let current_metadata = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
        let new_metadata = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
        let commit_path = TableCatalogObjectPaths::default().commit_log_entry_path(bucket, "table-id", "commit-1");

        store.put_table_bucket(test_bucket_entry(bucket)).await.unwrap();
        store
            .create_namespace(test_namespace_entry(bucket, &namespace))
            .await
            .unwrap();
        store
            .create_table(test_table_entry(bucket, &namespace, &table, current_metadata.clone()))
            .await
            .unwrap();
        backend.seed_object(bucket, &new_metadata, b"{}".to_vec()).await;
        backend
            .fail_put_attempt(rustfs_ecstore::disk::RUSTFS_META_BUCKET, &commit_path, 2)
            .await;

        store
            .commit_table(TableCommitRequest {
                table_bucket: bucket.to_string(),
                namespace: namespace.public_name(),
                table: table.as_str().to_string(),
                commit_id: "commit-1".to_string(),
                idempotency_key: None,
                operation: "append".to_string(),
                expected_version_token: "token-v1".to_string(),
                expected_metadata_location: current_metadata,
                new_metadata_location: new_metadata,
                requirements: Vec::new(),
                writer: None,
            })
            .await
            .unwrap();

        let report = store.recover_table_commits(bucket, "sales", "orders").await.unwrap();

        assert_eq!(report.finalized_count, 1);
        assert_eq!(report.finalization_required_count, 0);
        let committed = store.get_commit_by_id(bucket, "table-id", "commit-1").await.unwrap().unwrap();
        assert_eq!(committed.status, CommitLogStatus::Committed);
    }

    #[tokio::test]
    async fn table_commit_recovery_reports_staged_commit_after_table_cas_failure() {
        let backend = TestCatalogObjectBackend::default();
        let store = ObjectTableCatalogStore::new(backend.clone());
        let bucket = "analytics";
        let namespace = Namespace::parse("sales").unwrap();
        let table = IdentifierSegment::parse("orders").unwrap();
        let current_metadata = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
        let new_metadata = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
        let table_path = TableCatalogObjectPaths::default().table_entry_path(bucket, &namespace, &table);

        store.put_table_bucket(test_bucket_entry(bucket)).await.unwrap();
        store
            .create_namespace(test_namespace_entry(bucket, &namespace))
            .await
            .unwrap();
        store
            .create_table(test_table_entry(bucket, &namespace, &table, current_metadata.clone()))
            .await
            .unwrap();
        backend.seed_object(bucket, &current_metadata, b"{}".to_vec()).await;
        backend.seed_object(bucket, &new_metadata, b"{}".to_vec()).await;
        backend
            .fail_put_attempt(rustfs_ecstore::disk::RUSTFS_META_BUCKET, &table_path, 2)
            .await;

        let err = store
            .commit_table(TableCommitRequest {
                table_bucket: bucket.to_string(),
                namespace: namespace.public_name(),
                table: table.as_str().to_string(),
                commit_id: "commit-1".to_string(),
                idempotency_key: None,
                operation: "append".to_string(),
                expected_version_token: "token-v1".to_string(),
                expected_metadata_location: current_metadata.clone(),
                new_metadata_location: new_metadata,
                requirements: Vec::new(),
                writer: None,
            })
            .await
            .unwrap_err();
        assert_matches!(err, TableCatalogStoreError::Internal(_));
        let loaded = store.load_table(bucket, "sales", "orders").await.unwrap().unwrap();
        assert_eq!(loaded.metadata_location, current_metadata);

        let report = store.plan_table_commit_recovery(bucket, "sales", "orders").await.unwrap();
        assert_eq!(report.staged_before_table_update_count, 1);
        assert_eq!(report.finalization_required_count, 0);
        assert_eq!(report.commits[0].recovery_state, TableCommitRecoveryState::StagedBeforeTableUpdate);

        let diagnostics = store.diagnose_table_catalog(bucket, "sales", "orders", 0).await.unwrap();
        assert_eq!(diagnostics.current_metadata_status, TableMetadataPointerStatus::Valid);
        assert_eq!(diagnostics.recovery_status, TableCatalogRecoveryStatus::Recoverable);
        assert_eq!(diagnostics.recommended_actions, vec![TableCatalogRecoveryAction::RetryCommit]);
    }

    #[tokio::test]
    async fn table_commit_recovery_repairs_stale_idempotency_index_after_partial_finalization() {
        let backend = TestCatalogObjectBackend::default();
        let store = ObjectTableCatalogStore::new(backend.clone());
        let bucket = "analytics";
        let namespace = Namespace::parse("sales").unwrap();
        let table = IdentifierSegment::parse("orders").unwrap();
        let current_metadata = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
        let new_metadata = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
        let idempotency_path =
            TableCatalogObjectPaths::default().commit_idempotency_entry_path(bucket, "table-id", "client-request-1");

        store.put_table_bucket(test_bucket_entry(bucket)).await.unwrap();
        store
            .create_namespace(test_namespace_entry(bucket, &namespace))
            .await
            .unwrap();
        store
            .create_table(test_table_entry(bucket, &namespace, &table, current_metadata.clone()))
            .await
            .unwrap();
        backend.seed_object(bucket, &new_metadata, b"{}".to_vec()).await;
        backend
            .fail_put_attempt(rustfs_ecstore::disk::RUSTFS_META_BUCKET, &idempotency_path, 2)
            .await;

        let result = store
            .commit_table(TableCommitRequest {
                table_bucket: bucket.to_string(),
                namespace: namespace.public_name(),
                table: table.as_str().to_string(),
                commit_id: "commit-1".to_string(),
                idempotency_key: Some("client-request-1".to_string()),
                operation: "append".to_string(),
                expected_version_token: "token-v1".to_string(),
                expected_metadata_location: current_metadata,
                new_metadata_location: new_metadata,
                requirements: Vec::new(),
                writer: None,
            })
            .await
            .unwrap();
        assert_eq!(result.commit_log.status, CommitLogStatus::Committed);
        let stale_index = store
            .get_commit_by_idempotency_key(bucket, "table-id", "client-request-1")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stale_index.status, CommitLogStatus::Staged);

        let report = store.plan_table_commit_recovery(bucket, "sales", "orders").await.unwrap();
        assert_eq!(report.idempotency_repair_required_count, 1);
        assert_eq!(report.manual_review_count, 0);
        assert_eq!(report.commits[0].recovery_state, TableCommitRecoveryState::IdempotencyIndexRepairRequired);
        assert_eq!(report.commits[0].idempotency_index_status, TableCommitIdempotencyIndexStatus::Stale);

        let repaired = store.recover_table_commits(bucket, "sales", "orders").await.unwrap();

        assert_eq!(repaired.finalized_count, 1);
        assert_eq!(repaired.idempotency_repair_required_count, 0);
        let repaired_index = store
            .get_commit_by_idempotency_key(bucket, "table-id", "client-request-1")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(repaired_index.status, CommitLogStatus::Committed);
    }

    #[tokio::test]
    async fn object_table_catalog_store_rejects_stale_commit_token() {
        let backend = TestCatalogObjectBackend::default();
        let store = ObjectTableCatalogStore::new(backend.clone());
        let bucket = "analytics";
        let namespace = Namespace::parse("sales").unwrap();
        let table = IdentifierSegment::parse("orders").unwrap();
        let current_metadata = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
        let new_metadata = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");

        store.put_table_bucket(test_bucket_entry(bucket)).await.unwrap();
        store
            .create_namespace(test_namespace_entry(bucket, &namespace))
            .await
            .unwrap();
        store
            .create_table(test_table_entry(bucket, &namespace, &table, current_metadata.clone()))
            .await
            .unwrap();
        backend.seed_object(bucket, &new_metadata, b"{}".to_vec()).await;

        let err = store
            .commit_table(TableCommitRequest {
                table_bucket: bucket.to_string(),
                namespace: namespace.public_name(),
                table: table.as_str().to_string(),
                commit_id: "commit-1".to_string(),
                idempotency_key: None,
                operation: "append".to_string(),
                expected_version_token: "stale-token".to_string(),
                expected_metadata_location: current_metadata.clone(),
                new_metadata_location: new_metadata,
                requirements: Vec::new(),
                writer: None,
            })
            .await
            .unwrap_err();

        assert_matches!(err, TableCatalogStoreError::Conflict(_));
        let loaded = store.load_table(bucket, "sales", "orders").await.unwrap().unwrap();
        assert_eq!(loaded.metadata_location, current_metadata);
        assert_eq!(loaded.version_token, "token-v1");
        assert!(
            store
                .get_commit_by_id(bucket, "table-id", "commit-1")
                .await
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn namespace_marker_path_stays_under_default_reserved_boundary() {
        let namespace = Namespace::parse("analytics.daily_events").unwrap();

        assert_eq!(
            default_namespace_marker_path(&namespace),
            ".rustfs-table/warehouses/default/namespaces/analytics/daily_events/namespace.json"
        );
        assert_eq!(namespace.public_name(), "analytics.daily_events");
    }

    #[test]
    fn namespace_marker_path_extracts_public_name() {
        assert_eq!(
            namespace_name_from_marker_path(".rustfs-table/warehouses/default/namespaces/analytics/daily_events/namespace.json"),
            Some("analytics.daily_events".to_string())
        );
        assert_eq!(
            namespace_name_from_marker_path(
                ".rustfs-table/warehouses/default/namespaces/analytics/daily_events/tables/events/current.json"
            ),
            None
        );
        assert_eq!(
            namespace_name_from_marker_path(".rustfs-table/warehouses/other/namespaces/analytics/daily_events/namespace.json"),
            None
        );
    }

    #[test]
    fn namespace_marker_json_uses_stable_catalog_defaults() {
        let namespace = Namespace::parse("analytics.daily_events").unwrap();
        let marker = serde_json::to_value(NamespaceMarker::new(&namespace)).unwrap();

        assert_eq!(marker["version"], TABLE_NAMESPACE_MARKER_VERSION);
        assert_eq!(marker["namespace"], "analytics.daily_events");
        assert!(!namespace_marker_json(&namespace).unwrap().is_empty());
    }

    #[test]
    fn table_marker_path_stays_under_namespace_reserved_boundary() {
        let namespace = Namespace::parse("analytics.daily_events").unwrap();
        let table = IdentifierSegment::parse("events").unwrap();

        assert_eq!(
            default_table_root_prefix(&namespace),
            ".rustfs-table/warehouses/default/namespaces/analytics/daily_events/tables/"
        );
        assert_eq!(
            default_table_marker_path(&namespace, &table),
            ".rustfs-table/warehouses/default/namespaces/analytics/daily_events/tables/events/table.json"
        );
    }

    #[test]
    fn table_marker_path_extracts_table_name() {
        let namespace = Namespace::parse("analytics.daily_events").unwrap();

        assert_eq!(
            table_name_from_marker_path(
                &namespace,
                ".rustfs-table/warehouses/default/namespaces/analytics/daily_events/tables/events/table.json"
            ),
            Some("events".to_string())
        );
        assert_eq!(
            table_name_from_marker_path(
                &namespace,
                ".rustfs-table/warehouses/default/namespaces/analytics/daily_events/tables/events/metadata/current.json"
            ),
            None
        );
        assert_eq!(
            table_name_from_marker_path(
                &namespace,
                ".rustfs-table/warehouses/default/namespaces/analytics/other/tables/events/table.json"
            ),
            None
        );
    }

    #[test]
    fn table_marker_json_uses_stable_catalog_defaults() {
        let namespace = Namespace::parse("analytics.daily_events").unwrap();
        let table = IdentifierSegment::parse("events").unwrap();
        let marker = serde_json::to_value(TableMarker::new(&namespace, &table)).unwrap();

        assert_eq!(marker["version"], TABLE_RESOURCE_MARKER_VERSION);
        assert_eq!(marker["namespace"], "analytics.daily_events");
        assert_eq!(marker["name"], "events");
        assert!(marker["metadata_location"].is_null());
        assert!(!table_marker_json(&namespace, &table).unwrap().is_empty());
    }

    #[test]
    fn table_current_pointer_path_stays_under_table_boundary() {
        let namespace = Namespace::parse("analytics.daily_events").unwrap();
        let table = IdentifierSegment::parse("events").unwrap();

        assert_eq!(
            default_table_metadata_dir_path(&namespace, &table),
            ".rustfs-table/warehouses/default/namespaces/analytics/daily_events/tables/events/metadata"
        );
        assert_eq!(
            default_table_current_pointer_path(&namespace, &table),
            ".rustfs-table/warehouses/default/namespaces/analytics/daily_events/tables/events/current.json"
        );
        assert_eq!(
            default_table_lifecycle_path(&namespace, &table),
            ".rustfs-table/warehouses/default/namespaces/analytics/daily_events/tables/events/lifecycle.json"
        );
    }

    #[test]
    fn table_metadata_file_path_stays_under_metadata_boundary() {
        let namespace = Namespace::parse("analytics.daily_events").unwrap();
        let table = IdentifierSegment::parse("events").unwrap();
        let table_identifier =
            TableIdentifier::new(IdentifierSegment::parse(DEFAULT_WAREHOUSE_ID).unwrap(), namespace.clone(), table.clone());

        assert_eq!(
            default_table_metadata_file_path(&namespace, &table, "00001.metadata.json"),
            ".rustfs-table/warehouses/default/namespaces/analytics/daily_events/tables/events/metadata/00001.metadata.json"
        );
        assert_eq!(
            TablePathResolver::default().metadata_file_path(&table_identifier, "00001.metadata.json"),
            ".rustfs-table/warehouses/default/namespaces/analytics/daily_events/tables/events/metadata/00001.metadata.json"
        );
    }

    #[test]
    fn table_metadata_file_name_validation_rejects_unsafe_names() {
        assert!(is_valid_table_metadata_file_name("00001.metadata.json"));
        assert!(is_valid_table_metadata_file_name("v1-4f2c_metadata.json"));

        assert!(!is_valid_table_metadata_file_name(""));
        assert!(!is_valid_table_metadata_file_name(".metadata.json"));
        assert!(!is_valid_table_metadata_file_name("00001.metadata"));
        assert!(!is_valid_table_metadata_file_name("00001.JSON"));
        assert!(!is_valid_table_metadata_file_name("../current.json"));
        assert!(!is_valid_table_metadata_file_name("nested/00001.json"));
        assert!(!is_valid_table_metadata_file_name("nested%2f00001.json"));
        assert!(!is_valid_table_metadata_file_name("00001\\metadata.json"));
        assert!(!is_valid_table_metadata_file_name("00001\nmetadata.json"));
    }

    #[test]
    fn table_metadata_location_validation_stays_inside_metadata_dir() {
        let namespace = Namespace::parse("analytics.daily_events").unwrap();
        let table = IdentifierSegment::parse("events").unwrap();

        assert!(is_valid_table_metadata_location(
            &namespace,
            &table,
            ".rustfs-table/warehouses/default/namespaces/analytics/daily_events/tables/events/metadata/00001.metadata.json"
        ));
        assert!(!is_valid_table_metadata_location(&namespace, &table, ""));
        assert!(!is_valid_table_metadata_location(
            &namespace,
            &table,
            ".rustfs-table/warehouses/default/namespaces/analytics/daily_events/tables/events/current.json"
        ));
        assert!(!is_valid_table_metadata_location(
            &namespace,
            &table,
            ".rustfs-table/warehouses/default/namespaces/analytics/daily_events/tables/other/metadata/00001.json"
        ));
        assert!(!is_valid_table_metadata_location(
            &namespace,
            &table,
            ".rustfs-table/warehouses/default/namespaces/analytics/daily_events/tables/events/metadata/../current.json"
        ));
        assert!(!is_valid_table_metadata_location(
            &namespace,
            &table,
            ".rustfs-table/warehouses/default/namespaces/analytics/daily_events/tables/events/metadata/nested/00001.json"
        ));
    }

    #[test]
    fn metadata_location_from_metadata_file_path_extracts_table_metadata_only() {
        let namespace = Namespace::parse("analytics.daily_events").unwrap();
        let table = IdentifierSegment::parse("events").unwrap();

        assert_eq!(
            metadata_location_from_metadata_file_path(
                &namespace,
                &table,
                ".rustfs-table/warehouses/default/namespaces/analytics/daily_events/tables/events/metadata/00001.metadata.json"
            ),
            Some(
                ".rustfs-table/warehouses/default/namespaces/analytics/daily_events/tables/events/metadata/00001.metadata.json"
                    .to_string()
            )
        );
        assert_eq!(
            metadata_location_from_metadata_file_path(
                &namespace,
                &table,
                ".rustfs-table/warehouses/default/namespaces/analytics/daily_events/tables/events/current.json"
            ),
            None
        );
        assert_eq!(
            metadata_location_from_metadata_file_path(
                &namespace,
                &table,
                ".rustfs-table/warehouses/default/namespaces/analytics/daily_events/tables/events/metadata/nested/00001.metadata.json"
            ),
            None
        );
        assert_eq!(
            metadata_location_from_metadata_file_path(
                &namespace,
                &table,
                ".rustfs-table/warehouses/default/namespaces/analytics/daily_events/tables/other/metadata/00001.metadata.json"
            ),
            None
        );
    }

    #[test]
    fn table_metadata_pointer_json_round_trips() {
        let location =
            ".rustfs-table/warehouses/default/namespaces/analytics/daily_events/tables/events/metadata/00001.json".to_string();
        let data = table_metadata_pointer_json(location.clone()).unwrap();
        let pointer = parse_table_metadata_pointer(&data).unwrap();

        assert_eq!(pointer.version, TABLE_METADATA_POINTER_VERSION);
        assert_eq!(pointer.metadata_location, location);
    }

    #[test]
    fn object_mutation_entrypoints_call_reserved_prefix_guard() {
        let source = include_str!("app/object_usecase.rs");
        for expected in [
            "validate_object_key(&key, request_method_name)?;\n        validate_table_catalog_object_mutation(&bucket, &key).await?;",
            "validate_object_key(&key, \"COPY (dest)\")?;\n        validate_table_catalog_object_mutation(&bucket, &key).await?;",
            "if let Err(err) = validate_table_catalog_object_mutation(&bucket, &obj_id.key).await",
            "validate_object_key(&key, \"DELETE\")?;\n        validate_table_catalog_object_mutation(&bucket, &key).await?;",
            "validate_table_catalog_object_mutation(&bucket, &object).await?;",
            "validate_object_key(&key, \"PUT\")?;\n        validate_table_catalog_object_mutation(&bucket, &key).await?;",
            "validate_table_catalog_object_mutation(&bucket, &fpath).await?;",
        ] {
            assert!(source.contains(expected), "missing object mutation guard: {expected}");
        }
    }

    #[test]
    fn multipart_mutation_entrypoints_call_reserved_prefix_guard() {
        let source = include_str!("app/multipart_usecase.rs");

        assert_eq!(
            source
                .matches("validate_table_catalog_object_mutation(&bucket, &key).await?;")
                .count(),
            4
        );
    }

    #[test]
    fn object_metadata_mutation_entrypoints_call_reserved_prefix_guard() {
        let source = include_str!("storage/ecfs.rs");

        assert_eq!(
            source
                .matches("validate_table_catalog_object_mutation(&bucket, &object).await?;")
                .count(),
            2
        );
        assert_eq!(
            source
                .matches("validate_table_catalog_object_mutation(&bucket, &key).await?;")
                .count(),
            2
        );
    }

    #[test]
    fn identifier_segment_accepts_conservative_catalog_names() {
        for value in [
            "a",
            "a1",
            "a-b",
            "a_b",
            "abc123",
            "a23456789012345678901234567890123456789012345678901234567890123",
        ] {
            assert_eq!(IdentifierSegment::parse(value).unwrap().as_str(), value);
        }
    }

    #[test]
    fn identifier_segment_rejects_ambiguous_or_unsafe_names() {
        for value in [
            "",
            ".",
            "..",
            "Upper",
            "has.dot",
            "has/slash",
            "has\\slash",
            "has%2fslash",
            "-leading",
            "trailing-",
            "_leading",
            "trailing_",
            "has space",
            "name\nbreak",
        ] {
            assert!(IdentifierSegment::parse(value).is_err(), "value should be rejected: {value:?}");
        }

        let too_long = "a".repeat(IdentifierSegment::MAX_LEN + 1);
        assert!(IdentifierSegment::parse(too_long).is_err());
    }

    #[test]
    fn namespace_uses_dot_syntax_for_public_identity_and_slash_for_storage() {
        let namespace = Namespace::parse("analytics.daily_events").unwrap();

        assert_eq!(namespace.segments().len(), 2);
        assert_eq!(namespace.storage_id(), "analytics/daily_events");
    }

    #[test]
    fn resolver_builds_paths_under_reserved_table_boundary() {
        let table = TableIdentifier::new(
            IdentifierSegment::parse("warehouse1").unwrap(),
            Namespace::parse("analytics.daily").unwrap(),
            IdentifierSegment::parse("events").unwrap(),
        );
        let resolver = TablePathResolver::default();

        assert_eq!(
            resolver.current_pointer_path(&table),
            ".rustfs-table/warehouses/warehouse1/namespaces/analytics/daily/tables/events/current.json"
        );
        assert_eq!(
            resolver.metadata_dir_path(&table),
            ".rustfs-table/warehouses/warehouse1/namespaces/analytics/daily/tables/events/metadata"
        );
    }
}
