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

use super::*;
use crate::storage_api::table::managed::{
    AccountNamespaceId, BucketHandle, BucketIncarnation, BucketKind, BucketPublicName, ManagedTableId, StorageBucketId,
};

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
    /// Persisted only behind a rename intent so older readers reject the unknown state and fail closed.
    Renaming,
    Deleting,
    Deleted,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TableBucketEntry {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vector_bucket_binding: Option<VectorBucketBindingV1>,
    pub version: u16,
    pub table_bucket: String,
    pub catalog_type: String,
    pub warehouse_root: String,
    pub state: TableCatalogEntryState,
    #[serde(default)]
    pub properties: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_rename_id: Option<String>,
    pub created_at: Option<String>,
    pub updated_at: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum TableRenameIntentState {
    Prepared,
    /// The maintenance plan is durable before any destination sidecar is written.
    MaintenancePrepared,
    SourceFenced,
    DestinationWritten,
    SourceTombstoned,
    IndexPublished,
    DestinationPublished,
    Completed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TableRenameIntent {
    pub version: u16,
    pub rename_id: String,
    pub table_bucket: String,
    pub source: TableEntry,
    pub destination: TableEntry,
    pub source_etag: String,
    pub destination_etag: Option<String>,
    pub warehouse_index_etag: String,
    pub state: TableRenameIntentState,
    pub created_at: String,
    pub updated_at: String,
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

pub(crate) const NAMESPACE_PROPERTIES_MAX_ENTRIES: usize = 256;
pub(crate) const NAMESPACE_PROPERTY_KEY_MAX_LEN: usize = 256;
pub(crate) const NAMESPACE_PROPERTY_VALUE_MAX_LEN: usize = 4096;
pub(crate) const NAMESPACE_PROPERTIES_MAX_TOTAL_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NamespacePropertiesUpdate {
    removals: Vec<String>,
    updates: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum NamespacePropertiesUpdateError {
    DuplicateRemoval(String),
    Overlap(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct NamespacePropertiesUpdateResult {
    pub updated: Vec<String>,
    pub removed: Vec<String>,
    pub missing: Vec<String>,
}

impl NamespacePropertiesUpdate {
    pub(crate) fn try_new(
        removals: Vec<String>,
        updates: BTreeMap<String, String>,
    ) -> Result<Self, NamespacePropertiesUpdateError> {
        let mut removal_keys = BTreeSet::new();
        for key in &removals {
            if !removal_keys.insert(key.as_str()) {
                return Err(NamespacePropertiesUpdateError::DuplicateRemoval(key.clone()));
            }
            if updates.contains_key(key) {
                return Err(NamespacePropertiesUpdateError::Overlap(key.clone()));
            }
        }
        Ok(Self { removals, updates })
    }

    pub(crate) fn apply_to(self, entry: &mut NamespaceEntry) -> NamespacePropertiesUpdateResult {
        let updated = self.updates.keys().cloned().collect::<Vec<_>>();
        for (key, value) in self.updates {
            entry.properties.insert(key, value);
        }

        let mut removed = Vec::new();
        let mut missing = Vec::new();
        for key in self.removals {
            if entry.properties.remove(&key).is_some() {
                removed.push(key);
            } else {
                missing.push(key);
            }
        }

        NamespacePropertiesUpdateResult {
            updated,
            removed,
            missing,
        }
    }
}

pub(crate) fn validate_namespace_properties(properties: &BTreeMap<String, String>) -> TableCatalogStoreResult<()> {
    if properties.len() > NAMESPACE_PROPERTIES_MAX_ENTRIES {
        return Err(TableCatalogStoreError::Invalid(format!(
            "namespace properties exceed the maximum of {NAMESPACE_PROPERTIES_MAX_ENTRIES} entries"
        )));
    }

    let mut total_bytes = 0usize;
    for (key, value) in properties {
        if key.is_empty() || key.len() > NAMESPACE_PROPERTY_KEY_MAX_LEN {
            return Err(TableCatalogStoreError::Invalid(format!(
                "namespace property key length must be between 1 and {NAMESPACE_PROPERTY_KEY_MAX_LEN} bytes"
            )));
        }
        if value.len() > NAMESPACE_PROPERTY_VALUE_MAX_LEN {
            return Err(TableCatalogStoreError::Invalid(format!(
                "namespace property value exceeds {NAMESPACE_PROPERTY_VALUE_MAX_LEN} bytes"
            )));
        }
        total_bytes += key.len() + value.len();
        if total_bytes > NAMESPACE_PROPERTIES_MAX_TOTAL_BYTES {
            return Err(TableCatalogStoreError::Invalid(format!(
                "namespace properties exceed {NAMESPACE_PROPERTIES_MAX_TOTAL_BYTES} bytes"
            )));
        }
    }
    Ok(())
}

pub(crate) fn namespace_is_descendant(candidate: &str, parent: &str) -> bool {
    candidate.strip_prefix(parent).is_some_and(|suffix| suffix.starts_with('.'))
}

pub(crate) fn synthetic_namespace_entry(table_bucket: &str, namespace: &Namespace) -> NamespaceEntry {
    let namespace_id = namespace.storage_id();
    let namespace = namespace.public_name();
    NamespaceEntry {
        version: TABLE_CATALOG_ENTRY_VERSION,
        table_bucket: table_bucket.to_string(),
        namespace,
        namespace_id,
        state: TableCatalogEntryState::Active,
        properties: BTreeMap::new(),
        created_at: None,
        updated_at: None,
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TableEntry {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vector_head_binding: Option<VectorHeadBindingV1>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vector_index_definition: Option<VectorIndexDefinitionV1>,
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
#[serde(deny_unknown_fields)]
pub(crate) struct TableWarehouseIndexEntry {
    pub(super) version: u16,
    pub(super) table_bucket: String,
    pub(super) namespace: String,
    pub(super) table: String,
    pub(super) table_id: String,
    pub(super) warehouse_object_prefix: String,
    pub(super) state: TableCatalogEntryState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TableWarehouseIndexStateEntry {
    pub(super) version: u16,
    pub(super) table_bucket: String,
    pub(super) state: TableCatalogEntryState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum WarehouseIndexReservation {
    Created,
    AlreadyReserved,
    Replaced(TableWarehouseIndexEntry),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ViewEntry {
    pub version: u16,
    pub table_bucket: String,
    pub namespace: String,
    pub view: String,
    pub view_id: String,
    pub view_uuid: String,
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

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CommitLogEntry {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vector_commit_binding: Option<VectorCommitBindingV1>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vector_resource_mutation: Option<VectorResourceMutationBindingV1>,
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
pub(crate) struct TableCommitResult {
    pub table: TableEntry,
    pub commit_log: CommitLogEntry,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ExternalCatalogBridgeEntry {
    pub version: u16,
    pub table_bucket: String,
    pub namespace: String,
    pub table: String,
    pub catalog: String,
    pub external_catalog_id: Option<String>,
    pub external_namespace: String,
    pub external_table: String,
    pub external_table_uuid: Option<String>,
    pub metadata_location: Option<String>,
    pub external_version_token: Option<String>,
    pub policy_mode: String,
    pub credential_mode: String,
    pub sync_mode: String,
    pub rollback_strategy: String,
    pub last_sync_status: Option<String>,
    pub last_synced_metadata_location: Option<String>,
    #[serde(default)]
    pub properties: BTreeMap<String, String>,
    pub created_at: Option<String>,
    pub updated_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ViewCommitRequest {
    pub table_bucket: String,
    pub namespace: String,
    pub view: String,
    pub expected_version_token: String,
    pub expected_metadata_location: String,
    pub new_metadata_location: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ViewCommitResult {
    pub view: ViewEntry,
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
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum TableMaintenanceSchedulerStatus {
    Ready,
    Queued,
    Disabled,
    Paused,
    Backpressured,
    RetryDeferred,
    Quarantined,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TableMaintenanceSchedulerReport {
    pub table_bucket: String,
    pub namespace: String,
    pub table: String,
    pub table_id: String,
    pub status: TableMaintenanceSchedulerStatus,
    pub config_source: TableMaintenanceConfigSource,
    pub background_enabled: bool,
    pub worker_paused: bool,
    pub delete_enabled: bool,
    pub worker_lease_timeout_seconds: u64,
    pub max_retry_attempts: u16,
    pub retry_initial_backoff_seconds: u64,
    pub retry_max_backoff_seconds: u64,
    pub recommended_actions: Vec<TableMaintenanceRecommendedAction>,
    pub current_job: Option<TableMaintenanceSchedulerJobSummary>,
    pub quarantine: TableMaintenanceSchedulerQuarantineBoundary,
    pub audit_timeline: Vec<TableMaintenanceSchedulerJobSummary>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TableMaintenanceSchedulerRunResult {
    pub report: TableMetadataMaintenanceReport,
    pub scheduler: TableMaintenanceSchedulerReport,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TableMaintenanceSchedulerQuarantineBoundary {
    pub enabled: bool,
    pub active: bool,
    pub retention_seconds: u64,
    pub quarantined_object_count: usize,
    pub source_job_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum TableMaintenanceAuditActor {
    Scheduler,
    Worker,
    Operator,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum TableMaintenanceAuditAction {
    Planned,
    WorkerControl,
    SchedulerControl,
    SchedulerQueued,
    SchedulerLeaseExpired,
    WorkerStarted,
    WorkerHeartbeat,
    WorkerLeaseExpired,
    WorkerSucceeded,
    WorkerFailed,
    QuarantineRelease,
    QuarantineRetry,
    QuarantineAbandon,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TableMaintenanceAuditEvent {
    pub timestamp: String,
    pub actor: TableMaintenanceAuditActor,
    pub action: TableMaintenanceAuditAction,
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default, rename = "before-status")]
    pub before_status: Option<TableMetadataMaintenanceJobStatus>,
    #[serde(default, rename = "after-status")]
    pub after_status: Option<TableMetadataMaintenanceJobStatus>,
    #[serde(default, rename = "before-quarantined-object-count")]
    pub before_quarantined_object_count: Option<usize>,
    #[serde(default, rename = "after-quarantined-object-count")]
    pub after_quarantined_object_count: Option<usize>,
    #[serde(default, rename = "recommended-actions")]
    pub recommended_actions: Vec<TableMaintenanceRecommendedAction>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum TableMaintenanceQuarantineAction {
    Inspect,
    Release,
    Retry,
    Abandon,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TableMaintenanceQuarantineOperationRequest {
    pub action: TableMaintenanceQuarantineAction,
    #[serde(default)]
    pub reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TableMaintenanceQuarantineOperationResult {
    pub action: TableMaintenanceQuarantineAction,
    pub report: TableMetadataMaintenanceReport,
    pub scheduler: TableMaintenanceSchedulerReport,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TableMaintenanceSchedulerJobSummary {
    pub job_id: String,
    pub operation: TableMetadataMaintenanceOperation,
    pub status: TableMetadataMaintenanceJobStatus,
    #[serde(default, rename = "scheduler-id")]
    pub scheduler_id: Option<String>,
    #[serde(default, rename = "scheduled-at")]
    pub scheduled_at: Option<String>,
    pub worker_id: Option<String>,
    pub attempt: u16,
    pub started_at: Option<String>,
    pub finished_at: Option<String>,
    pub heartbeat_at: Option<String>,
    pub next_retry_after: Option<String>,
    pub recommended_actions: Vec<TableMaintenanceRecommendedAction>,
    #[serde(default, rename = "audit-events")]
    pub audit_events: Vec<TableMaintenanceAuditEvent>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TableMaintenanceWorkerCondition {
    pub expected_job_id: String,
    pub expected_attempt: u16,
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
    #[serde(default, rename = "recommended-actions")]
    pub recommended_actions: Vec<TableMaintenanceRecommendedAction>,
    #[serde(default)]
    pub config_source: TableMaintenanceConfigSource,
    #[serde(default, rename = "scheduler-id")]
    pub scheduler_id: Option<String>,
    #[serde(default, rename = "scheduler-lease-id")]
    pub scheduler_lease_id: String,
    #[serde(default, rename = "scheduled-at")]
    pub scheduled_at: Option<String>,
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
    pub planned_object_file_count: usize,
    #[serde(default)]
    pub cleanup_candidate_object_count: usize,
    #[serde(default)]
    pub deletable_object_count: usize,
    #[serde(default)]
    pub deleted_object_count: usize,
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
    #[serde(default, rename = "cleanup-object-candidate-locations")]
    pub cleanup_object_candidate_locations: Vec<String>,
    #[serde(default, rename = "deletable-object-locations")]
    pub deletable_object_locations: Vec<String>,
    #[serde(default)]
    pub object_reports: Vec<TableMetadataMaintenanceObjectReport>,
    #[serde(default, rename = "object-cleanup-reports")]
    pub object_cleanup_reports: Vec<TableMetadataMaintenanceObjectCleanupReport>,
    #[serde(default)]
    pub referenced_object_reports: Vec<TableMetadataMaintenanceReferencedObjectReport>,
    #[serde(default, rename = "reachability-graph")]
    pub reachability_graph: TableMaintenanceReachabilityGraphReport,
    #[serde(default, rename = "snapshot-expiration")]
    pub snapshot_expiration: Option<TableSnapshotExpirationReport>,
    #[serde(default)]
    pub compaction: Option<TableCompactionPlanningReport>,
    #[serde(default, rename = "audit-events")]
    pub audit_events: Vec<TableMaintenanceAuditEvent>,
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
    #[serde(default, rename = "committed-metadata-location")]
    pub committed_metadata_location: Option<String>,
    #[serde(default, rename = "row-level-planning")]
    pub row_level_planning: TableRowLevelMaintenancePlanningReport,
    #[serde(default, rename = "rewrite-groups")]
    pub rewrite_groups: Vec<TableCompactionRewriteGroup>,
    pub snapshot_reports: Vec<TableCompactionSnapshotReport>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TableRowLevelMaintenancePlanningReport {
    pub status: TableRowLevelMaintenancePlanningStatus,
    #[serde(rename = "delete-file-count")]
    pub delete_file_count: usize,
    #[serde(rename = "position-delete-file-count")]
    pub position_delete_file_count: usize,
    #[serde(rename = "equality-delete-file-count")]
    pub equality_delete_file_count: usize,
    #[serde(rename = "manual-review-count")]
    pub manual_review_count: usize,
    pub reasons: Vec<TableRowLevelMaintenancePlanningReason>,
    #[serde(rename = "delete-files")]
    pub delete_files: Vec<TableRowLevelDeleteFilePlanningReport>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum TableRowLevelMaintenancePlanningStatus {
    #[default]
    NoDeleteFiles,
    ManualReviewRequired,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum TableRowLevelMaintenancePlanningReason {
    PositionDeleteFile,
    EqualityDeleteFile,
    DeleteFileRewriteUnsupported,
    MissingDeleteFile,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TableRowLevelDeleteFilePlanningReport {
    #[serde(rename = "file-location")]
    pub file_location: String,
    pub content: TableRowLevelDeleteFileContent,
    #[serde(rename = "object-exists")]
    pub object_exists: bool,
    #[serde(default, rename = "record-count", skip_serializing_if = "Option::is_none")]
    pub record_count: Option<u64>,
    #[serde(default, rename = "file-size-bytes", skip_serializing_if = "Option::is_none")]
    pub file_size_bytes: Option<u64>,
    #[serde(default, rename = "sequence-number", skip_serializing_if = "Option::is_none")]
    pub sequence_number: Option<i64>,
    #[serde(default, rename = "file-sequence-number", skip_serializing_if = "Option::is_none")]
    pub file_sequence_number: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum TableRowLevelDeleteFileContent {
    PositionDelete,
    EqualityDelete,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TableCompactionRewriteGroup {
    pub group_id: String,
    #[serde(default, rename = "sort-order-id", skip_serializing_if = "Option::is_none")]
    pub sort_order_id: Option<i32>,
    #[serde(rename = "input-file-locations")]
    pub input_file_locations: Vec<String>,
    #[serde(rename = "input-file-count")]
    pub input_file_count: usize,
    #[serde(rename = "input-bytes")]
    pub input_bytes: u64,
    #[serde(default, rename = "output-file-location")]
    pub output_file_location: Option<String>,
    #[serde(default, rename = "output-bytes")]
    pub output_bytes: Option<u64>,
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
    RewriteCandidates,
    Committed,
    ManualReviewRequired,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum TableCompactionPlanningReason {
    ManifestList,
    ManifestFile,
    SmallDataFile,
    RewriteGroup,
    CompactionCommitted,
    ManifestAvroReaderUnavailable,
    MissingCurrentSnapshot,
    MissingManifestList,
    MissingDataFile,
    DeleteFile,
    PositionDeleteFile,
    EqualityDeleteFile,
    RowLevelRewriteUnsupported,
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
    Queued,
    Running,
    #[default]
    Successful,
    Failed,
    Disabled,
    Paused,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum TableMaintenanceRecommendedAction {
    NoActionRequired,
    RunMaintenanceWorker,
    ReviewAndRunDelete,
    ReviewQuarantine,
    EnableDelete,
    EnableBackgroundMaintenance,
    ResumeMaintenanceWorker,
    WaitForRetryBackoff,
    WaitForActiveWorker,
    InvestigateFailure,
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
    ManifestFile,
    DataFile,
    DeleteFile,
    UnsupportedManifestAvro,
    UnreadableMetadata,
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
pub(crate) struct TableMetadataMaintenanceObjectCleanupReport {
    pub object_location: String,
    pub object_kind: TableMetadataMaintenanceObjectKind,
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

pub(crate) struct TableMaintenanceHeartbeatRef<'a> {
    pub(super) table_bucket: &'a str,
    pub(super) namespace: &'a str,
    pub(super) table: &'a str,
    pub(super) job_id: &'a str,
    pub(super) lease_id: &'a str,
    pub(super) worker_id: &'a str,
}

pub(crate) struct TableMaintenancePreflightContext<'a> {
    pub(super) table_bucket: &'a str,
    pub(super) namespace: &'a Namespace,
    pub(super) table: &'a IdentifierSegment,
    pub(super) entry: &'a TableEntry,
}

pub(crate) struct TableMaintenanceWorkerControlReport<'a> {
    pub(super) table_bucket: &'a str,
    pub(super) namespace: &'a Namespace,
    pub(super) table: &'a IdentifierSegment,
    pub(super) entry: &'a TableEntry,
    pub(super) worker_id: String,
    pub(super) effective: &'a TableMaintenanceEffectiveConfig,
    pub(super) status: TableMetadataMaintenanceJobStatus,
    pub(super) reason: &'a str,
    pub(super) now: OffsetDateTime,
}

pub(crate) struct TableMaintenanceSchedulerControlReport<'a> {
    pub(super) table_bucket: &'a str,
    pub(super) namespace: &'a Namespace,
    pub(super) table: &'a IdentifierSegment,
    pub(super) entry: &'a TableEntry,
    pub(super) scheduler_id: String,
    pub(super) effective: &'a TableMaintenanceEffectiveConfig,
    pub(super) status: TableMetadataMaintenanceJobStatus,
    pub(super) reason: &'a str,
    pub(super) now: OffsetDateTime,
}

pub(crate) enum TableMaintenanceWorkerPreflight {
    Ready {
        effective: TableMaintenanceEffectiveConfig,
        queued: Option<Box<TableMetadataMaintenanceReport>>,
    },
    Complete(Box<TableMetadataMaintenanceReport>),
}

pub(crate) enum TableMaintenanceSchedulerPreflight {
    Ready(TableMaintenanceEffectiveConfig),
    Complete(Box<TableMetadataMaintenanceReport>),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct TableCatalogExport {
    pub table_bucket: TableBucketEntry,
    pub namespace: NamespaceEntry,
    pub table: TableEntry,
    pub backing_manifest: TableCatalogBackingManifest,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct TableCatalogBackingManifest {
    pub version: u16,
    pub current: TableCatalogBackingProfile,
    pub ha: TableCatalogHaPolicy,
    pub scale_validation: TableCatalogScaleValidation,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct TableCatalogBackingProfile {
    pub kind: TableCatalogBackingKind,
    pub authority: TableCatalogAuthority,
    pub consistency: TableCatalogConsistencyMode,
    pub durability: TableCatalogDurabilityMode,
    pub current_pointer_path: String,
    pub wal: TableCatalogWalState,
    pub snapshot: TableCatalogSnapshotState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum TableCatalogBackingKind {
    ObjectBacked,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum TableCatalogAuthority {
    RustfsSysObject,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum TableCatalogConsistencyMode {
    ConditionalObjectCas,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum TableCatalogDurabilityMode {
    StagedCommitLogBeforePointerUpdate,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct TableCatalogWalState {
    pub status: TableCatalogWalStatus,
    pub commit_log_prefix: String,
    pub idempotency_index_prefix: String,
    pub committed_generation: u64,
    pub staged_before_table_update_count: usize,
    pub finalization_required_count: usize,
    pub idempotency_repair_required_count: usize,
    pub manual_review_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum TableCatalogWalStatus {
    Recoverable,
    RecoveryRequired,
    ManualReviewRequired,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct TableCatalogSnapshotState {
    pub export_api: String,
    pub includes_table_bucket: bool,
    pub includes_namespace: bool,
    pub includes_table_pointer: bool,
    pub includes_backing_manifest: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot_etag: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot_version: Option<u16>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct TableCatalogHaPolicy {
    pub writer_region_model: TableCatalogHaWriterModel,
    pub read_replica_strategy: TableCatalogReadReplicaStrategy,
    pub commit_read_requirement: TableCatalogCommitReadRequirement,
    pub active_active_supported: bool,
    pub failover_requires_operator_promotion: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum TableCatalogHaWriterModel {
    SingleActiveWriterRegion,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum TableCatalogReadReplicaStrategy {
    ReadOnlyReplicasForListAndLoad,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum TableCatalogCommitReadRequirement {
    LinearizableLeaderRead,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct TableCatalogScaleValidation {
    pub status: TableCatalogScaleValidationStatus,
    pub benchmark_required: bool,
    pub required_scenarios: Vec<TableCatalogScaleValidationScenario>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum TableCatalogScaleValidationStatus {
    MatrixPublished,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum TableCatalogScaleValidationScenario {
    ConcurrentCommitCas,
    CommitLogRecoveryReplay,
    ReadReplicaStaleReadGuard,
    ClientConformanceMatrix,
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
    pub backing_manifest: TableCatalogBackingManifest,
    pub orphan_metadata_candidate_locations: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum TableCommitRecoveryState {
    Committed,
    Superseded,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[allow(
    dead_code,
    reason = "exercised by table_catalog/tests.rs; the lib target cannot see test-only consumers (backlog#1823)"
)]
pub(crate) struct NamespaceMarker {
    pub version: u16,
    pub namespace: String,
}

impl NamespaceMarker {
    #[allow(
        dead_code,
        reason = "exercised by table_catalog/tests.rs; the lib target cannot see test-only consumers (backlog#1823)"
    )]
    pub fn new(namespace: &Namespace) -> Self {
        Self {
            version: TABLE_NAMESPACE_MARKER_VERSION,
            namespace: namespace.public_name(),
        }
    }
}

#[allow(
    dead_code,
    reason = "exercised by table_catalog/tests.rs; the lib target cannot see test-only consumers (backlog#1823)"
)]
pub(crate) fn namespace_marker_json(namespace: &Namespace) -> Result<Vec<u8>, serde_json::Error> {
    serde_json::to_vec(&NamespaceMarker::new(namespace))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[allow(
    dead_code,
    reason = "exercised by table_catalog/tests.rs; the lib target cannot see test-only consumers (backlog#1823)"
)]
pub(crate) struct TableMarker {
    pub version: u16,
    pub namespace: String,
    pub name: String,
    pub metadata_location: Option<String>,
}

impl TableMarker {
    #[allow(
        dead_code,
        reason = "exercised by table_catalog/tests.rs; the lib target cannot see test-only consumers (backlog#1823)"
    )]
    pub fn new(namespace: &Namespace, table: &IdentifierSegment) -> Self {
        Self {
            version: TABLE_RESOURCE_MARKER_VERSION,
            namespace: namespace.public_name(),
            name: table.as_str().to_string(),
            metadata_location: None,
        }
    }
}

#[allow(
    dead_code,
    reason = "exercised by table_catalog/tests.rs; the lib target cannot see test-only consumers (backlog#1823)"
)]
pub(crate) fn table_marker_json(namespace: &Namespace, table: &IdentifierSegment) -> Result<Vec<u8>, serde_json::Error> {
    serde_json::to_vec(&TableMarker::new(namespace, table))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[allow(
    dead_code,
    reason = "exercised by table_catalog/tests.rs; the lib target cannot see test-only consumers (backlog#1823)"
)]
pub(crate) struct TableMetadataPointer {
    pub version: u16,
    pub metadata_location: String,
}

impl TableMetadataPointer {
    #[allow(
        dead_code,
        reason = "exercised by table_catalog/tests.rs; the lib target cannot see test-only consumers (backlog#1823)"
    )]
    pub fn new(metadata_location: String) -> Self {
        Self {
            version: TABLE_METADATA_POINTER_VERSION,
            metadata_location,
        }
    }
}

#[allow(
    dead_code,
    reason = "exercised by table_catalog/tests.rs; the lib target cannot see test-only consumers (backlog#1823)"
)]
pub(crate) fn table_metadata_pointer_json(metadata_location: String) -> Result<Vec<u8>, serde_json::Error> {
    serde_json::to_vec(&TableMetadataPointer::new(metadata_location))
}

#[allow(
    dead_code,
    reason = "exercised by table_catalog/tests.rs; the lib target cannot see test-only consumers (backlog#1823)"
)]
pub(crate) fn parse_table_metadata_pointer(data: &[u8]) -> Result<TableMetadataPointer, serde_json::Error> {
    serde_json::from_slice(data)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CatalogFormat {
    Iceberg,
    Lance,
}

impl CatalogFormat {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Iceberg => "iceberg",
            Self::Lance => "lance",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CatalogGeneration(u64);

impl CatalogGeneration {
    pub(crate) fn get(self) -> u64 {
        self.0
    }
    pub(crate) fn checked_next(self) -> TableCatalogStoreResult<Self> {
        self.0
            .checked_add(1)
            .map(Self)
            .ok_or_else(|| TableCatalogStoreError::Conflict("catalog generation overflow".to_string()))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CatalogVersionToken(String);
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CatalogCommitId(String);
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CatalogObjectKey(String);
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CatalogContentSha256([u8; 32]);
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CatalogBodyRevision(Uuid);
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CatalogRequestDigest([u8; 32]);

impl CatalogVersionToken {
    pub(crate) fn try_new(value: String) -> TableCatalogStoreResult<Self> {
        validate_catalog_identity_text(&value, "version token")?;
        Ok(Self(value))
    }
    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}
impl CatalogCommitId {
    pub(crate) fn try_new(value: String) -> TableCatalogStoreResult<Self> {
        validate_catalog_identity_text(&value, "commit id")?;
        Ok(Self(value))
    }
    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}
impl CatalogObjectKey {
    pub(crate) fn try_new(value: String) -> TableCatalogStoreResult<Self> {
        if value.is_empty()
            || value.len() > 1024
            || value.starts_with('/')
            || value.chars().any(|ch| ch.is_control() || matches!(ch, '\\' | '%'))
            || value.split('/').any(|part| matches!(part, "" | "." | ".."))
        {
            return Err(TableCatalogStoreError::Invalid("invalid catalog object key".to_string()));
        }
        Ok(Self(value))
    }
    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}
impl CatalogContentSha256 {
    pub(crate) fn digest(bytes: &[u8]) -> Self {
        Self(Sha256::digest(bytes).into())
    }
    pub(crate) fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
    pub(crate) fn to_hex(self) -> String {
        hex_simd::encode_to_string(self.0, hex_simd::AsciiCase::Lower)
    }
}
impl CatalogRequestDigest {
    pub(in crate::table_catalog) fn from_bytes(value: [u8; 32]) -> Self {
        Self(value)
    }
    pub(crate) fn to_hex(self) -> String {
        hex_simd::encode_to_string(self.0, hex_simd::AsciiCase::Lower)
    }
}
impl CatalogBodyRevision {
    pub(crate) fn try_new(value: Uuid) -> TableCatalogStoreResult<Self> {
        if value.is_nil() {
            return Err(TableCatalogStoreError::Invalid("catalog body revision must be non-nil".to_string()));
        }
        Ok(Self(value))
    }
    pub(crate) fn as_uuid(self) -> Uuid {
        self.0
    }
}
fn validate_catalog_identity_text(value: &str, label: &str) -> TableCatalogStoreResult<()> {
    if value.is_empty() || value.len() > 4096 || value.chars().any(char::is_control) {
        return Err(TableCatalogStoreError::Invalid(format!("invalid catalog {label}")));
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum CatalogBucketIdentity {
    Table {
        storage_bucket_id: String,
        incarnation: BucketIncarnation,
    },
    Vector(BucketHandle),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CatalogResourceIdentity {
    bucket: CatalogBucketIdentity,
    table_id: ManagedTableId,
    format: CatalogFormat,
}
impl CatalogResourceIdentity {
    pub(crate) fn try_new(
        bucket: BucketHandle,
        table_id: ManagedTableId,
        format: CatalogFormat,
    ) -> TableCatalogStoreResult<Self> {
        if bucket.kind() != BucketKind::Vector || format != CatalogFormat::Lance {
            return Err(TableCatalogStoreError::Invalid(
                "catalog vector format does not match bucket kind".to_string(),
            ));
        }
        if Uuid::parse_str(table_id.as_str()).ok().filter(|id| !id.is_nil()).is_none() {
            return Err(TableCatalogStoreError::Invalid("vector table id must be a non-nil UUID".to_string()));
        }
        Ok(Self {
            bucket: CatalogBucketIdentity::Vector(bucket),
            table_id,
            format,
        })
    }
    pub(crate) fn try_table(
        storage_bucket_id: String,
        incarnation: Uuid,
        table_id: ManagedTableId,
    ) -> TableCatalogStoreResult<Self> {
        validate_catalog_identity_text(&storage_bucket_id, "storage bucket")?;
        let incarnation =
            BucketIncarnation::try_new(incarnation).map_err(|err| TableCatalogStoreError::Invalid(err.to_string()))?;
        Ok(Self {
            bucket: CatalogBucketIdentity::Table {
                storage_bucket_id,
                incarnation,
            },
            table_id,
            format: CatalogFormat::Iceberg,
        })
    }
    pub(crate) fn vector_bucket(&self) -> Option<&BucketHandle> {
        match &self.bucket {
            CatalogBucketIdentity::Vector(bucket) => Some(bucket),
            CatalogBucketIdentity::Table { .. } => None,
        }
    }
    pub(crate) fn storage_bucket_id(&self) -> &str {
        match &self.bucket {
            CatalogBucketIdentity::Vector(bucket) => bucket.storage_bucket_id().as_str(),
            CatalogBucketIdentity::Table { storage_bucket_id, .. } => storage_bucket_id,
        }
    }
    pub(crate) fn incarnation(&self) -> Uuid {
        match &self.bucket {
            CatalogBucketIdentity::Vector(bucket) => bucket.incarnation().as_uuid(),
            CatalogBucketIdentity::Table { incarnation, .. } => incarnation.as_uuid(),
        }
    }
    pub(crate) fn table_id(&self) -> &ManagedTableId {
        &self.table_id
    }
    pub(crate) fn format(&self) -> CatalogFormat {
        self.format
    }
    pub(crate) fn dataset_prefix(&self) -> String {
        format!(".rustfs-table/{}/dataset/", self.table_id.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CatalogMetadataRef {
    object_key: CatalogObjectKey,
    content_length: u64,
    content_sha256: CatalogContentSha256,
    body_revision: Option<CatalogBodyRevision>,
    etag: Option<String>,
}
impl CatalogMetadataRef {
    pub(crate) fn from_read(object_key: CatalogObjectKey, object: &TableCatalogObject) -> TableCatalogStoreResult<Self> {
        Ok(Self {
            object_key,
            content_length: u64::try_from(object.data.len())
                .map_err(|_| TableCatalogStoreError::Invalid("catalog metadata length overflow".to_string()))?,
            content_sha256: CatalogContentSha256(Sha256::digest(&object.data).into()),
            body_revision: object.body_revision.map(CatalogBodyRevision::try_new).transpose()?,
            etag: object.etag.clone(),
        })
    }
    pub(crate) fn object_key(&self) -> &CatalogObjectKey {
        &self.object_key
    }
    pub(crate) fn content_length(&self) -> u64 {
        self.content_length
    }
    pub(crate) fn content_sha256(&self) -> CatalogContentSha256 {
        self.content_sha256
    }
    pub(crate) fn body_revision(&self) -> Option<CatalogBodyRevision> {
        self.body_revision
    }
    pub(crate) fn etag(&self) -> Option<&str> {
        self.etag.as_deref()
    }
    pub(in crate::table_catalog) fn verify_read(&self, object: &TableCatalogObject) -> TableCatalogStoreResult<()> {
        let read = Self::from_read(self.object_key.clone(), object)?;
        if self.content_length != read.content_length
            || self.content_sha256 != read.content_sha256
            || (self.body_revision.is_some() && self.body_revision != read.body_revision)
            || (self.etag.is_some() && self.etag != read.etag)
        {
            return Err(TableCatalogStoreError::Conflict(
                "catalog candidate metadata changed after preparation".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CatalogFormatDescriptor {
    encoding_version: u16,
    format: CatalogFormat,
    format_version: u16,
    metadata: CatalogMetadataRef,
    payload: Vec<u8>,
}
impl CatalogFormatDescriptor {
    pub(crate) fn try_new(
        encoding_version: u16,
        format: CatalogFormat,
        format_version: u16,
        metadata: CatalogMetadataRef,
        payload: Vec<u8>,
    ) -> TableCatalogStoreResult<Self> {
        if encoding_version != 1
            || !matches!((format, format_version), (CatalogFormat::Lance, 1) | (CatalogFormat::Iceberg, 1 | 2))
        {
            return Err(TableCatalogStoreError::Unsupported(
                "unsupported catalog format descriptor version".to_string(),
            ));
        }
        if payload.is_empty() || payload.len() > 1024 * 1024 || metadata.content_length == 0 {
            return Err(TableCatalogStoreError::Invalid(
                "catalog format descriptor requires bounded payload and nonempty metadata".to_string(),
            ));
        }
        Ok(Self {
            encoding_version,
            format,
            format_version,
            metadata,
            payload,
        })
    }
    pub(crate) fn encoding_version(&self) -> u16 {
        self.encoding_version
    }
    pub(crate) fn format(&self) -> CatalogFormat {
        self.format
    }
    pub(crate) fn format_version(&self) -> u16 {
        self.format_version
    }
    pub(crate) fn metadata(&self) -> &CatalogMetadataRef {
        &self.metadata
    }
    pub(crate) fn payload(&self) -> &[u8] {
        &self.payload
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CatalogExpectedHead {
    version_token: CatalogVersionToken,
    generation: CatalogGeneration,
    metadata: Option<CatalogMetadataRef>,
    format_descriptor: Option<CatalogFormatDescriptor>,
}
impl CatalogExpectedHead {
    pub(crate) fn from_entry(
        entry: &TableEntry,
        metadata: Option<CatalogMetadataRef>,
        format_descriptor: Option<CatalogFormatDescriptor>,
    ) -> TableCatalogStoreResult<Self> {
        match (&metadata, &format_descriptor) {
            (None, None) if entry.metadata_location.is_empty() => {}
            (Some(metadata), Some(descriptor))
                if descriptor.metadata() == metadata && metadata.object_key.as_str() == entry.metadata_location => {}
            _ => {
                return Err(TableCatalogStoreError::Invalid(
                    "catalog expected metadata and descriptor do not match entry".to_string(),
                ));
            }
        }
        Ok(Self {
            version_token: CatalogVersionToken::try_new(entry.version_token.clone())?,
            generation: CatalogGeneration(entry.generation),
            metadata,
            format_descriptor,
        })
    }
    pub(crate) fn version_token(&self) -> &CatalogVersionToken {
        &self.version_token
    }
    pub(crate) fn generation(&self) -> CatalogGeneration {
        self.generation
    }
    pub(crate) fn metadata(&self) -> Option<&CatalogMetadataRef> {
        self.metadata.as_ref()
    }
    pub(crate) fn format_descriptor(&self) -> Option<&CatalogFormatDescriptor> {
        self.format_descriptor.as_ref()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CatalogEntryUpdates {
    warehouse_location: Option<String>,
    format_version: Option<u16>,
}
impl CatalogEntryUpdates {
    pub(crate) fn try_new(warehouse_location: Option<String>, format_version: Option<u16>) -> TableCatalogStoreResult<Self> {
        if warehouse_location
            .as_ref()
            .is_some_and(|value| value.is_empty() || value.chars().any(char::is_control))
            || format_version == Some(0)
        {
            return Err(TableCatalogStoreError::Invalid("invalid prepared catalog entry updates".to_string()));
        }
        Ok(Self {
            warehouse_location,
            format_version,
        })
    }
    pub(crate) fn warehouse_location(&self) -> Option<&str> {
        self.warehouse_location.as_deref()
    }
    pub(crate) fn format_version(&self) -> Option<u16> {
        self.format_version
    }
}

pub(crate) struct CatalogCommitParts {
    pub resource_identity: CatalogResourceIdentity,
    pub namespace: Namespace,
    pub name: CatalogName,
    pub expected_head: CatalogExpectedHead,
    pub commit_id: CatalogCommitId,
    pub operation: String,
    pub idempotency_key: Option<String>,
    pub writer: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PreparedCatalogCommit {
    resource_identity: CatalogResourceIdentity,
    namespace: Namespace,
    name: CatalogName,
    expected_head: CatalogExpectedHead,
    candidate_metadata: CatalogMetadataRef,
    format_descriptor: CatalogFormatDescriptor,
    commit_id: CatalogCommitId,
    request_digest: CatalogRequestDigest,
    operation: String,
    idempotency_key: Option<String>,
    writer: Option<String>,
    entry_updates: CatalogEntryUpdates,
    legacy_requirements: Vec<u8>,
    vector_resource_mutation: Option<VectorResourceMutationBindingV1>,
}
impl PreparedCatalogCommit {
    pub(crate) fn try_from_validated_parts(
        parts: CatalogCommitParts,
        candidate_metadata: CatalogMetadataRef,
        format_descriptor: CatalogFormatDescriptor,
        entry_updates: CatalogEntryUpdates,
        legacy_requirements: Vec<u8>,
    ) -> TableCatalogStoreResult<Self> {
        validate_catalog_identity_text(&parts.operation, "operation")?;
        if format_descriptor.metadata() != &candidate_metadata
            || format_descriptor.format() != parts.resource_identity.format()
            || entry_updates
                .format_version
                .is_some_and(|version| version != format_descriptor.format_version())
        {
            return Err(TableCatalogStoreError::Invalid(
                "prepared catalog descriptor does not match candidate".to_string(),
            ));
        }
        if parts.resource_identity.format() == CatalogFormat::Lance {
            let prefix = parts.resource_identity.dataset_prefix();
            if parts.namespace.public_name() != "indexes"
                || !legacy_requirements.is_empty()
                || !candidate_metadata.object_key.as_str().starts_with(&prefix)
                || entry_updates.warehouse_location.as_deref()
                    != Some(
                        format!("s3://{}/{}", parts.resource_identity.storage_bucket_id(), prefix.trim_end_matches('/')).as_str(),
                    )
            {
                return Err(TableCatalogStoreError::Invalid(
                    "prepared vector metadata is outside its dataset scope".to_string(),
                ));
            }
        }
        if parts.resource_identity.format() == CatalogFormat::Iceberg {
            CatalogJsonArray::from_canonical_bytes(&legacy_requirements)?;
        }
        let mut prepared = Self {
            resource_identity: parts.resource_identity,
            namespace: parts.namespace,
            name: parts.name,
            expected_head: parts.expected_head,
            candidate_metadata,
            format_descriptor,
            commit_id: parts.commit_id,
            request_digest: CatalogRequestDigest([0; 32]),
            operation: parts.operation,
            idempotency_key: parts.idempotency_key,
            writer: parts.writer,
            entry_updates,
            legacy_requirements,
            vector_resource_mutation: None,
        };
        prepared.request_digest = catalog_prepared_request_digest(&prepared)?;
        Ok(prepared)
    }
    pub(crate) fn resource_identity(&self) -> &CatalogResourceIdentity {
        &self.resource_identity
    }
    pub(crate) fn namespace(&self) -> &Namespace {
        &self.namespace
    }
    pub(crate) fn name(&self) -> &CatalogName {
        &self.name
    }
    pub(crate) fn expected_head(&self) -> &CatalogExpectedHead {
        &self.expected_head
    }
    pub(crate) fn candidate_metadata(&self) -> &CatalogMetadataRef {
        &self.candidate_metadata
    }
    pub(crate) fn format_descriptor(&self) -> &CatalogFormatDescriptor {
        &self.format_descriptor
    }
    pub(crate) fn commit_id(&self) -> &CatalogCommitId {
        &self.commit_id
    }
    pub(crate) fn request_digest(&self) -> CatalogRequestDigest {
        self.request_digest
    }
    pub(crate) fn operation(&self) -> &str {
        &self.operation
    }
    pub(crate) fn idempotency_key(&self) -> Option<&str> {
        self.idempotency_key.as_deref()
    }
    pub(crate) fn writer(&self) -> Option<&str> {
        self.writer.as_deref()
    }
    pub(crate) fn entry_updates(&self) -> &CatalogEntryUpdates {
        &self.entry_updates
    }
    pub(crate) fn legacy_requirements(&self) -> &[u8] {
        &self.legacy_requirements
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CatalogMetadataRefRecordV1 {
    pub object_key: String,
    pub content_length: u64,
    pub content_sha256: String,
    pub body_revision: Option<Uuid>,
    pub etag: Option<String>,
}
impl From<&CatalogMetadataRef> for CatalogMetadataRefRecordV1 {
    fn from(value: &CatalogMetadataRef) -> Self {
        Self {
            object_key: value.object_key.as_str().to_string(),
            content_length: value.content_length,
            content_sha256: value.content_sha256.to_hex(),
            body_revision: value.body_revision.map(CatalogBodyRevision::as_uuid),
            etag: value.etag.clone(),
        }
    }
}
impl TryFrom<&CatalogMetadataRefRecordV1> for CatalogMetadataRef {
    type Error = TableCatalogStoreError;
    fn try_from(value: &CatalogMetadataRefRecordV1) -> Result<Self, Self::Error> {
        Ok(Self {
            object_key: CatalogObjectKey::try_new(value.object_key.clone())?,
            content_length: value.content_length,
            content_sha256: CatalogContentSha256(decode_catalog_digest(&value.content_sha256)?),
            body_revision: value.body_revision.map(CatalogBodyRevision::try_new).transpose()?,
            etag: value.etag.clone(),
        })
    }
}
fn decode_catalog_digest(value: &str) -> TableCatalogStoreResult<[u8; 32]> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(TableCatalogStoreError::Invalid(
            "catalog digest must be lowercase SHA-256 hex".to_string(),
        ));
    }
    let bytes = hex_simd::decode_to_vec(value.as_bytes())
        .map_err(|_| TableCatalogStoreError::Invalid("invalid catalog digest".to_string()))?;
    bytes
        .try_into()
        .map_err(|_| TableCatalogStoreError::Invalid("invalid catalog digest length".to_string()))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CatalogFormatDescriptorRecordV1 {
    pub encoding_version: u16,
    pub format: String,
    pub format_version: u16,
    pub metadata: CatalogMetadataRefRecordV1,
    pub payload_base64: String,
}
impl From<&CatalogFormatDescriptor> for CatalogFormatDescriptorRecordV1 {
    fn from(value: &CatalogFormatDescriptor) -> Self {
        Self {
            encoding_version: value.encoding_version,
            format: value.format.as_str().to_string(),
            format_version: value.format_version,
            metadata: (&value.metadata).into(),
            payload_base64: base64_simd::STANDARD.encode_to_string(&value.payload),
        }
    }
}
impl TryFrom<&CatalogFormatDescriptorRecordV1> for CatalogFormatDescriptor {
    type Error = TableCatalogStoreError;
    fn try_from(value: &CatalogFormatDescriptorRecordV1) -> Result<Self, Self::Error> {
        let format = match value.format.as_str() {
            "iceberg" => CatalogFormat::Iceberg,
            "lance" => CatalogFormat::Lance,
            _ => return Err(TableCatalogStoreError::Unsupported("unknown catalog format".to_string())),
        };
        let payload = base64_simd::STANDARD
            .decode_to_vec(value.payload_base64.as_bytes())
            .map_err(|_| TableCatalogStoreError::Invalid("invalid catalog descriptor base64".to_string()))?;
        if base64_simd::STANDARD.encode_to_string(&payload) != value.payload_base64 {
            return Err(TableCatalogStoreError::Invalid(
                "catalog descriptor base64 must use canonical padding".to_string(),
            ));
        }
        Self::try_new(
            value.encoding_version,
            format,
            value.format_version,
            (&value.metadata).try_into()?,
            payload,
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct VectorBucketBindingV1 {
    pub version: u16,
    pub storage_bucket_id: String,
    pub bucket_incarnation: Uuid,
    pub account_namespace_id: String,
    pub region: String,
    pub public_name: String,
}
impl VectorBucketBindingV1 {
    pub(crate) fn from_handle(bucket: &BucketHandle) -> TableCatalogStoreResult<Self> {
        if bucket.kind() != BucketKind::Vector {
            return Err(TableCatalogStoreError::Invalid("vector binding requires a vector bucket".to_string()));
        }
        Ok(Self {
            version: 1,
            storage_bucket_id: bucket.storage_bucket_id().as_str().to_string(),
            bucket_incarnation: bucket.incarnation().as_uuid(),
            account_namespace_id: bucket.account_namespace_id().as_str().to_string(),
            region: bucket.region().to_string(),
            public_name: bucket.public_name().as_str().to_string(),
        })
    }
    pub(crate) fn to_handle(&self) -> TableCatalogStoreResult<BucketHandle> {
        if self.version != 1 {
            return Err(TableCatalogStoreError::Unsupported(
                "unsupported vector bucket binding version".to_string(),
            ));
        }
        let invalid = |err| TableCatalogStoreError::Invalid(format!("invalid vector bucket binding: {err}"));
        BucketHandle::try_from_parts(
            BucketKind::Vector,
            AccountNamespaceId::try_new(self.account_namespace_id.clone()).map_err(invalid)?,
            self.region.clone(),
            BucketPublicName::try_new(self.public_name.clone()).map_err(invalid)?,
            StorageBucketId::try_new(self.storage_bucket_id.clone()).map_err(invalid)?,
            BucketIncarnation::try_new(self.bucket_incarnation).map_err(invalid)?,
        )
        .map_err(invalid)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct VectorHeadBindingV1 {
    pub version: u16,
    pub bucket: VectorBucketBindingV1,
    pub table_id: String,
    pub metadata: CatalogMetadataRefRecordV1,
    pub descriptor: CatalogFormatDescriptorRecordV1,
}
impl VectorHeadBindingV1 {
    pub(crate) fn try_new(
        resource: &CatalogResourceIdentity,
        metadata: &CatalogMetadataRef,
        descriptor: &CatalogFormatDescriptor,
    ) -> TableCatalogStoreResult<Self> {
        if resource.format() != CatalogFormat::Lance
            || descriptor.format() != CatalogFormat::Lance
            || descriptor.metadata() != metadata
            || !metadata.object_key.as_str().starts_with(&resource.dataset_prefix())
        {
            return Err(TableCatalogStoreError::Invalid("invalid vector head content binding".to_string()));
        }
        Ok(Self {
            version: 1,
            bucket: VectorBucketBindingV1::from_handle(
                resource
                    .vector_bucket()
                    .ok_or_else(|| TableCatalogStoreError::Invalid("vector binding requires vector identity".to_string()))?,
            )?,
            table_id: resource.table_id.as_str().to_string(),
            metadata: metadata.into(),
            descriptor: descriptor.into(),
        })
    }
    pub(in crate::table_catalog) fn validate(
        &self,
    ) -> TableCatalogStoreResult<(CatalogResourceIdentity, CatalogMetadataRef, CatalogFormatDescriptor)> {
        if self.version != 1 {
            return Err(TableCatalogStoreError::Unsupported("unsupported vector head binding version".to_string()));
        }
        let resource = CatalogResourceIdentity::try_new(
            self.bucket.to_handle()?,
            ManagedTableId::try_new(self.table_id.clone()).map_err(|err| TableCatalogStoreError::Invalid(err.to_string()))?,
            CatalogFormat::Lance,
        )?;
        let metadata = CatalogMetadataRef::try_from(&self.metadata)?;
        let descriptor = CatalogFormatDescriptor::try_from(&self.descriptor)?;
        Self::try_new(&resource, &metadata, &descriptor)?;
        Ok((resource, metadata, descriptor))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct VectorCommitBindingV1 {
    pub version: u16,
    pub bucket: VectorBucketBindingV1,
    pub table_id: String,
    pub expected_generation: u64,
    pub expected_metadata: Option<CatalogMetadataRefRecordV1>,
    pub expected_descriptor: Option<CatalogFormatDescriptorRecordV1>,
    pub candidate_metadata: CatalogMetadataRefRecordV1,
    pub descriptor: CatalogFormatDescriptorRecordV1,
    pub request_digest_sha256: String,
}
impl VectorCommitBindingV1 {
    pub(in crate::table_catalog) fn from_prepared(prepared: &PreparedCatalogCommit) -> TableCatalogStoreResult<Self> {
        Ok(Self {
            version: 1,
            bucket: VectorBucketBindingV1::from_handle(
                prepared
                    .resource_identity
                    .vector_bucket()
                    .ok_or_else(|| TableCatalogStoreError::Invalid("vector commit requires vector identity".to_string()))?,
            )?,
            table_id: prepared.resource_identity.table_id.as_str().to_string(),
            expected_generation: prepared.expected_head.generation.get(),
            expected_metadata: prepared.expected_head.metadata.as_ref().map(Into::into),
            expected_descriptor: prepared.expected_head.format_descriptor.as_ref().map(Into::into),
            candidate_metadata: (&prepared.candidate_metadata).into(),
            descriptor: (&prepared.format_descriptor).into(),
            request_digest_sha256: prepared.request_digest().to_hex(),
        })
    }
}

#[derive(Debug, Clone)]
pub(crate) struct CatalogHead {
    resource_identity: CatalogResourceIdentity,
    namespace: Namespace,
    name: CatalogName,
    version_token: CatalogVersionToken,
    generation: CatalogGeneration,
    metadata: CatalogMetadataRef,
    format_descriptor: CatalogFormatDescriptor,
}
impl CatalogHead {
    pub(in crate::table_catalog) fn from_vector_entry(entry: &TableEntry) -> TableCatalogStoreResult<Self> {
        match (entry.version, entry.vector_index_definition.as_ref()) {
            (2, None) => {}
            (3, Some(definition)) => definition.validate()?,
            (2 | 3, _) => {
                return Err(TableCatalogStoreError::Invalid(
                    "Vector entry definition does not match its version".into(),
                ));
            }
            _ => return Err(TableCatalogStoreError::Unsupported("unsupported Vector entry version".into())),
        }
        let binding = entry
            .vector_head_binding
            .as_ref()
            .ok_or_else(|| TableCatalogStoreError::Unsupported("catalog entry has no vector head binding".to_string()))?;
        let (resource_identity, metadata, format_descriptor) = binding.validate()?;
        if entry.format != "lance"
            || entry.format_version != 1
            || entry.table_id != binding.table_id
            || entry.table_uuid != binding.table_id
            || entry.table_bucket != binding.bucket.storage_bucket_id
            || entry.metadata_location != metadata.object_key.as_str()
            || entry.namespace != "indexes"
            || entry.warehouse_location
                != format!("s3://{}/{}", entry.table_bucket, resource_identity.dataset_prefix().trim_end_matches('/'))
        {
            return Err(TableCatalogStoreError::Invalid(
                "vector catalog entry contradicts head binding".to_string(),
            ));
        }
        let _namespace = Namespace::parse(&entry.namespace).map_err(|err| TableCatalogStoreError::Invalid(err.to_string()))?;
        let _name = CatalogName::parse(&entry.table).map_err(|err| TableCatalogStoreError::Invalid(err.to_string()))?;
        Ok(Self {
            resource_identity,
            namespace: _namespace,
            name: _name,
            version_token: CatalogVersionToken::try_new(entry.version_token.clone())?,
            generation: CatalogGeneration(entry.generation),
            metadata,
            format_descriptor,
        })
    }
    pub(crate) fn resource_identity(&self) -> &CatalogResourceIdentity {
        &self.resource_identity
    }
    pub(crate) fn namespace(&self) -> &Namespace {
        &self.namespace
    }
    pub(crate) fn name(&self) -> &CatalogName {
        &self.name
    }
    pub(crate) fn version_token(&self) -> &CatalogVersionToken {
        &self.version_token
    }
    pub(crate) fn generation(&self) -> CatalogGeneration {
        self.generation
    }
    pub(crate) fn metadata(&self) -> &CatalogMetadataRef {
        &self.metadata
    }
    pub(crate) fn format_descriptor(&self) -> &CatalogFormatDescriptor {
        &self.format_descriptor
    }
    pub(crate) fn expected_head(&self) -> CatalogExpectedHead {
        CatalogExpectedHead {
            version_token: self.version_token.clone(),
            generation: self.generation,
            metadata: Some(self.metadata.clone()),
            format_descriptor: Some(self.format_descriptor.clone()),
        }
    }
}
#[derive(Debug, Clone)]
pub(crate) struct CatalogCommittedVersion {
    metadata: CatalogMetadataRef,
    format_descriptor: CatalogFormatDescriptor,
    commit_id: Option<CatalogCommitId>,
}
impl CatalogCommittedVersion {
    pub(in crate::table_catalog) fn new(
        metadata: CatalogMetadataRef,
        format_descriptor: CatalogFormatDescriptor,
        commit_id: Option<CatalogCommitId>,
    ) -> TableCatalogStoreResult<Self> {
        if format_descriptor.metadata() != &metadata {
            return Err(TableCatalogStoreError::Invalid("catalog history descriptor mismatch".to_string()));
        }
        Ok(Self {
            metadata,
            format_descriptor,
            commit_id,
        })
    }
    pub(crate) fn metadata(&self) -> &CatalogMetadataRef {
        &self.metadata
    }
    pub(crate) fn format_descriptor(&self) -> &CatalogFormatDescriptor {
        &self.format_descriptor
    }
    pub(crate) fn commit_id(&self) -> Option<&CatalogCommitId> {
        self.commit_id.as_ref()
    }
}
#[derive(Debug, Clone)]
pub(crate) struct CatalogCommittedHistory {
    versions: Vec<CatalogCommittedVersion>,
}
impl CatalogCommittedHistory {
    pub(in crate::table_catalog) fn new(versions: Vec<CatalogCommittedVersion>) -> Self {
        Self { versions }
    }
    pub(crate) fn versions(&self) -> &[CatalogCommittedVersion] {
        &self.versions
    }
}

impl VectorCommitBindingV1 {
    pub(in crate::table_catalog) fn validate_log(
        &self,
        entry: &CommitLogEntry,
        bucket: &str,
        table_id: &str,
    ) -> TableCatalogStoreResult<()> {
        if self.version != 1 || !matches!(entry.version, 2 | 3) {
            return Err(TableCatalogStoreError::Unsupported("unsupported Vector commit version".to_string()));
        }
        let identity = CatalogResourceIdentity::try_new(
            self.bucket.to_handle()?,
            ManagedTableId::try_new(self.table_id.clone()).map_err(|err| TableCatalogStoreError::Invalid(err.to_string()))?,
            CatalogFormat::Lance,
        )?;
        let candidate = CatalogMetadataRef::try_from(&self.candidate_metadata)?;
        let descriptor = CatalogFormatDescriptor::try_from(&self.descriptor)?;
        VectorHeadBindingV1::try_new(&identity, &candidate, &descriptor)?;
        decode_catalog_digest(&self.request_digest_sha256)?;
        match (entry.version, entry.vector_resource_mutation.as_ref()) {
            (2, None) => {}
            (3, Some(mutation)) => mutation.validate()?,
            _ => {
                return Err(TableCatalogStoreError::Invalid(
                    "Vector commit mutation does not match its version".into(),
                ));
            }
        }
        if self.table_id != table_id
            || entry.table_id != table_id
            || identity.storage_bucket_id() != bucket
            || entry.new_metadata_location != candidate.object_key().as_str()
            || !entry.requirements.is_empty()
        {
            return Err(TableCatalogStoreError::Invalid(
                "Vector commit record contradicts its binding".to_string(),
            ));
        }
        match (&self.expected_metadata, &self.expected_descriptor) {
            (Some(metadata), Some(descriptor)) => {
                let metadata = CatalogMetadataRef::try_from(metadata)?;
                let descriptor = CatalogFormatDescriptor::try_from(descriptor)?;
                VectorHeadBindingV1::try_new(&identity, &metadata, &descriptor)?;
                if let Some(mutation) = entry.vector_resource_mutation.as_ref() {
                    mutation.validate_metadata(
                        &metadata,
                        &descriptor,
                        &candidate,
                        &CatalogFormatDescriptor::try_from(&self.descriptor)?,
                    )?;
                }
                if entry.previous_metadata_location != metadata.object_key().as_str() {
                    return Err(TableCatalogStoreError::Invalid("Vector commit previous metadata mismatch".to_string()));
                }
            }
            (None, None) if entry.version == 2 && entry.previous_metadata_location.is_empty() => {}
            _ => return Err(TableCatalogStoreError::Invalid("Vector commit expected descriptor mismatch".to_string())),
        }
        Ok(())
    }
}

/// An opaque JSON array used only to preserve an existing record's JSON field shape.
#[derive(Debug, Clone, PartialEq)]
pub(in crate::table_catalog) struct CatalogJsonArray(Vec<serde_json::Value>);
impl CatalogJsonArray {
    pub(in crate::table_catalog) fn from_canonical_bytes(bytes: &[u8]) -> TableCatalogStoreResult<Self> {
        if bytes.len() > 1024 * 1024 {
            return Err(TableCatalogStoreError::Invalid(
                "catalog compatibility JSON exceeds its size limit".to_string(),
            ));
        }
        let values: Vec<serde_json::Value> = serde_json::from_slice(bytes)
            .map_err(|err| TableCatalogStoreError::Invalid(format!("invalid catalog compatibility JSON array: {err}")))?;
        let encoded = serde_json::to_vec(&values).map_err(|err| {
            TableCatalogStoreError::Invalid(format!("failed to encode catalog compatibility JSON array: {err}"))
        })?;
        if encoded != bytes {
            return Err(TableCatalogStoreError::Invalid("catalog compatibility JSON is not canonical".to_string()));
        }
        Ok(Self(values))
    }
    pub(in crate::table_catalog) fn into_values(self) -> Vec<serde_json::Value> {
        self.0
    }
    pub(in crate::table_catalog) fn matches_values(&self, values: &[serde_json::Value]) -> bool {
        self.0 == values
    }
}
impl PreparedCatalogCommit {
    pub(in crate::table_catalog) fn compatibility_json_array(&self) -> TableCatalogStoreResult<CatalogJsonArray> {
        if self.resource_identity().format() == CatalogFormat::Lance && self.legacy_requirements.is_empty() {
            return Ok(CatalogJsonArray(Vec::new()));
        }
        CatalogJsonArray::from_canonical_bytes(&self.legacy_requirements)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
pub(crate) enum VectorEffectiveEncryptionV1 {
    Unencrypted,
    Aes256,
    AwsKms { key_id: String, kms_key_arn: String },
}

impl VectorEffectiveEncryptionV1 {
    pub(crate) fn validate(&self) -> TableCatalogStoreResult<()> {
        if let Self::AwsKms { key_id, kms_key_arn } = self {
            validate_catalog_identity_text(key_id, "Vector KMS key ID")?;
            validate_catalog_identity_text(kms_key_arn, "Vector KMS key ARN")?;
        }
        Ok(())
    }

    pub(crate) fn canonical_bytes(&self) -> TableCatalogStoreResult<Vec<u8>> {
        self.validate()?;
        match self {
            Self::Unencrypted => Ok(b"null".to_vec()),
            Self::Aes256 => Ok(br#"{"sseType":"AES256"}"#.to_vec()),
            Self::AwsKms { kms_key_arn, .. } => {
                #[derive(Serialize)]
                #[serde(rename_all = "camelCase")]
                struct CanonicalKms<'a> {
                    sse_type: &'static str,
                    kms_key_arn: &'a str,
                }
                serde_json::to_vec(&CanonicalKms {
                    sse_type: "aws:kms",
                    kms_key_arn,
                })
                .map_err(|error| TableCatalogStoreError::Invalid(format!("encode Vector encryption descriptor: {error}")))
            }
        }
    }

    pub(crate) fn descriptor_sha256(&self) -> TableCatalogStoreResult<CatalogContentSha256> {
        Ok(CatalogContentSha256::digest(&self.canonical_bytes()?))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct VectorIndexDefinitionV1 {
    pub version: u16,
    pub effective_encryption: VectorEffectiveEncryptionV1,
    #[serde(deserialize_with = "deserialize_vector_tags")]
    pub tags: BTreeMap<String, String>,
}

impl VectorIndexDefinitionV1 {
    pub(crate) fn validate(&self) -> TableCatalogStoreResult<()> {
        if self.version != 1 {
            return Err(TableCatalogStoreError::Unsupported("unsupported Vector Index definition version".into()));
        }
        self.effective_encryption.validate()?;
        validate_vector_tags(&self.tags)
    }
}

fn deserialize_vector_tags<'de, D>(deserializer: D) -> Result<BTreeMap<String, String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct TagsVisitor;
    impl<'de> serde::de::Visitor<'de> for TagsVisitor {
        type Value = BTreeMap<String, String>;
        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("a tag map with unique keys")
        }
        fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
        where
            A: serde::de::MapAccess<'de>,
        {
            let mut tags = BTreeMap::new();
            while let Some((key, value)) = map.next_entry::<String, String>()? {
                if tags.insert(key, value).is_some() {
                    return Err(serde::de::Error::custom("duplicate Vector tag key"));
                }
                if tags.len() > 50 {
                    return Err(serde::de::Error::custom("Vector tags exceed the maximum of 50 entries"));
                }
            }
            validate_vector_tags(&tags).map_err(serde::de::Error::custom)?;
            Ok(tags)
        }
    }
    deserializer.deserialize_map(TagsVisitor)
}

pub(crate) fn validate_vector_tags(tags: &BTreeMap<String, String>) -> TableCatalogStoreResult<()> {
    static CHARACTERS: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
        // This is the fixed tag character grammar shared by Vector resource records.
        regex::Regex::new(r"\A[\p{L}\p{Z}\p{N}_.:/=+\-@]*\z").expect("fixed Vector tag character grammar")
    });
    if tags.len() > 50
        || tags.iter().any(|(key, value)| {
            !(1..=128).contains(&key.chars().count())
                || value.chars().count() > 256
                || !CHARACTERS.is_match(key)
                || !CHARACTERS.is_match(value)
        })
    {
        return Err(TableCatalogStoreError::Invalid("invalid Vector Index tags".into()));
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum VectorResourceMutationKind {
    Dataset,
    Tags,
    Delete,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct VectorResourceMutationBindingV1 {
    pub version: u16,
    pub kind: VectorResourceMutationKind,
    pub expected_definition: VectorIndexDefinitionV1,
    pub next_definition: VectorIndexDefinitionV1,
    pub expected_state: TableCatalogEntryState,
    pub next_state: TableCatalogEntryState,
}

impl VectorResourceMutationBindingV1 {
    pub(crate) fn validate(&self) -> TableCatalogStoreResult<()> {
        if self.version != 1 {
            return Err(TableCatalogStoreError::Unsupported("unsupported Vector resource mutation version".into()));
        }
        self.expected_definition.validate()?;
        self.next_definition.validate()?;
        let valid = self.expected_state == TableCatalogEntryState::Active
            && match self.kind {
                VectorResourceMutationKind::Dataset => {
                    self.next_state == TableCatalogEntryState::Active && self.expected_definition == self.next_definition
                }
                VectorResourceMutationKind::Tags => {
                    self.next_state == TableCatalogEntryState::Active
                        && self.expected_definition.effective_encryption == self.next_definition.effective_encryption
                }
                VectorResourceMutationKind::Delete => {
                    self.next_state == TableCatalogEntryState::Deleted && self.expected_definition == self.next_definition
                }
            };
        if !valid {
            return Err(TableCatalogStoreError::Invalid("invalid Vector resource mutation transition".into()));
        }
        Ok(())
    }

    pub(in crate::table_catalog) fn validate_metadata(
        &self,
        expected: &CatalogMetadataRef,
        expected_descriptor: &CatalogFormatDescriptor,
        candidate: &CatalogMetadataRef,
        candidate_descriptor: &CatalogFormatDescriptor,
    ) -> TableCatalogStoreResult<()> {
        self.validate()?;
        if self.kind != VectorResourceMutationKind::Dataset
            && (expected != candidate || expected_descriptor != candidate_descriptor)
        {
            return Err(TableCatalogStoreError::Invalid(
                "Vector resource mutation changed Dataset metadata".into(),
            ));
        }
        Ok(())
    }

    pub(in crate::table_catalog) fn request_digest(
        &self,
        prepared: &PreparedCatalogCommit,
    ) -> TableCatalogStoreResult<CatalogRequestDigest> {
        self.validate()?;
        #[derive(Serialize)]
        struct CanonicalMutation<'a> {
            domain: &'static str,
            prepared_sha256: String,
            resource_mutation: &'a VectorResourceMutationBindingV1,
        }
        let bytes = serde_json::to_vec(&CanonicalMutation {
            domain: "rustfs.vector.catalog-mutation.v1",
            prepared_sha256: prepared.request_digest().to_hex(),
            resource_mutation: self,
        })
        .map_err(|error| TableCatalogStoreError::Invalid(format!("encode Vector resource mutation: {error}")))?;
        Ok(CatalogRequestDigest::from_bytes(Sha256::digest(bytes).into()))
    }
}

fn deserialize_vector_record<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    struct ObjectVisitor<T>(std::marker::PhantomData<fn() -> T>);
    impl<'de, T: Deserialize<'de>> serde::de::Visitor<'de> for ObjectVisitor<T> {
        type Value = T;
        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("a Vector record object")
        }
        fn visit_map<A: serde::de::MapAccess<'de>>(self, map: A) -> Result<T, A::Error> {
            T::deserialize(serde::de::value::MapAccessDeserializer::new(map))
        }
    }
    deserializer.deserialize_map(ObjectVisitor(std::marker::PhantomData))
}

impl<'de> Deserialize<'de> for VectorEffectiveEncryptionV1 {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct EncryptionVisitor;
        impl<'de> serde::de::Visitor<'de> for EncryptionVisitor {
            type Value = VectorEffectiveEncryptionV1;
            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a Vector encryption configuration object")
            }
            fn visit_map<A: serde::de::MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
                let mut kind = None;
                let mut key_id = None;
                let mut kms_key_arn = None;
                while let Some(field) = map.next_key::<String>()? {
                    let slot = match field.as_str() {
                        "kind" => &mut kind,
                        "key_id" => &mut key_id,
                        "kms_key_arn" => &mut kms_key_arn,
                        _ => return Err(serde::de::Error::unknown_field(&field, &["kind", "key_id", "kms_key_arn"])),
                    };
                    if slot.is_some() {
                        return Err(serde::de::Error::custom(format!("duplicate Vector encryption field {field}")));
                    }
                    *slot = Some(map.next_value::<String>()?);
                }
                match kind
                    .as_deref()
                    .ok_or_else(|| <A::Error as serde::de::Error>::missing_field("kind"))?
                {
                    "unencrypted" | "aes256" if key_id.is_some() || kms_key_arn.is_some() => {
                        Err(serde::de::Error::custom("KMS fields require aws-kms encryption"))
                    }
                    "unencrypted" => Ok(VectorEffectiveEncryptionV1::Unencrypted),
                    "aes256" => Ok(VectorEffectiveEncryptionV1::Aes256),
                    "aws-kms" => Ok(VectorEffectiveEncryptionV1::AwsKms {
                        key_id: key_id.ok_or_else(|| <A::Error as serde::de::Error>::missing_field("key_id"))?,
                        kms_key_arn: kms_key_arn.ok_or_else(|| <A::Error as serde::de::Error>::missing_field("kms_key_arn"))?,
                    }),
                    _ => Err(serde::de::Error::custom("unsupported Vector encryption kind")),
                }
            }
        }
        deserializer.deserialize_map(EncryptionVisitor)
    }
}

impl<'de> Deserialize<'de> for VectorIndexDefinitionV1 {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Fields {
            version: u16,
            effective_encryption: VectorEffectiveEncryptionV1,
            #[serde(deserialize_with = "deserialize_vector_tags")]
            tags: BTreeMap<String, String>,
        }
        let fields: Fields = deserialize_vector_record(deserializer)?;
        Ok(Self {
            version: fields.version,
            effective_encryption: fields.effective_encryption,
            tags: fields.tags,
        })
    }
}

impl<'de> Deserialize<'de> for VectorResourceMutationKind {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        match String::deserialize(deserializer)?.as_str() {
            "DATASET" => Ok(Self::Dataset),
            "TAGS" => Ok(Self::Tags),
            "DELETE" => Ok(Self::Delete),
            _ => Err(serde::de::Error::custom("unsupported Vector resource mutation kind")),
        }
    }
}

fn deserialize_vector_entry_state<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<TableCatalogEntryState, D::Error> {
    match String::deserialize(deserializer)?.as_str() {
        "ACTIVE" => Ok(TableCatalogEntryState::Active),
        "RENAMING" => Ok(TableCatalogEntryState::Renaming),
        "DELETING" => Ok(TableCatalogEntryState::Deleting),
        "DELETED" => Ok(TableCatalogEntryState::Deleted),
        _ => Err(serde::de::Error::custom("unsupported Vector resource state")),
    }
}

impl<'de> Deserialize<'de> for VectorResourceMutationBindingV1 {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Fields {
            version: u16,
            kind: VectorResourceMutationKind,
            expected_definition: VectorIndexDefinitionV1,
            next_definition: VectorIndexDefinitionV1,
            #[serde(deserialize_with = "deserialize_vector_entry_state")]
            expected_state: TableCatalogEntryState,
            #[serde(deserialize_with = "deserialize_vector_entry_state")]
            next_state: TableCatalogEntryState,
        }
        let fields: Fields = deserialize_vector_record(deserializer)?;
        Ok(Self {
            version: fields.version,
            kind: fields.kind,
            expected_definition: fields.expected_definition,
            next_definition: fields.next_definition,
            expected_state: fields.expected_state,
            next_state: fields.next_state,
        })
    }
}

#[derive(Deserialize)]
#[serde(untagged)]
enum CatalogEntryStateRepresentation {
    String(String),
    Legacy(TableCatalogEntryState),
}

impl CatalogEntryStateRepresentation {
    fn is_string(&self) -> bool {
        matches!(self, Self::String(_))
    }
    fn into_state<E: serde::de::Error>(self) -> Result<TableCatalogEntryState, E> {
        match self {
            Self::String(value) => TableCatalogEntryState::deserialize(serde::de::value::StringDeserializer::new(value)),
            Self::Legacy(value) => Ok(value),
        }
    }
}

impl<'de> Deserialize<'de> for TableEntry {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Fields {
            #[serde(default)]
            vector_head_binding: Option<CatalogRecordRepresentation<VectorHeadBindingFields>>,
            version: u16,
            table_bucket: String,
            namespace: String,
            table: String,
            table_id: String,
            table_uuid: String,
            format: String,
            format_version: u16,
            warehouse_location: String,
            metadata_location: String,
            version_token: String,
            generation: u64,
            state: CatalogEntryStateRepresentation,
            #[serde(default)]
            properties: BTreeMap<String, String>,
            created_at: Option<String>,
            updated_at: Option<String>,
            #[serde(default)]
            vector_index_definition: Option<VectorIndexDefinitionV1>,
        }
        impl Fields {
            fn finish<E: serde::de::Error>(self, object: bool) -> Result<TableEntry, E> {
                if self.version == 3
                    && (!object
                        || !self.state.is_string()
                        || self
                            .vector_head_binding
                            .as_ref()
                            .is_some_and(|binding| !binding.is_object_tree()))
                {
                    return Err(E::custom("Vector v3 entry requires object records and a string state"));
                }
                Ok(TableEntry {
                    vector_head_binding: self.vector_head_binding.map(|binding| binding.into_binding()),
                    vector_index_definition: self.vector_index_definition,
                    version: self.version,
                    table_bucket: self.table_bucket,
                    namespace: self.namespace,
                    table: self.table,
                    table_id: self.table_id,
                    table_uuid: self.table_uuid,
                    format: self.format,
                    format_version: self.format_version,
                    warehouse_location: self.warehouse_location,
                    metadata_location: self.metadata_location,
                    version_token: self.version_token,
                    generation: self.generation,
                    state: self.state.into_state()?,
                    properties: self.properties,
                    created_at: self.created_at,
                    updated_at: self.updated_at,
                })
            }
        }
        struct EntryVisitor;
        impl<'de> serde::de::Visitor<'de> for EntryVisitor {
            type Value = TableEntry;
            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a Catalog entry in its supported version representation")
            }
            fn visit_map<A: serde::de::MapAccess<'de>>(self, map: A) -> Result<Self::Value, A::Error> {
                Fields::deserialize(serde::de::value::MapAccessDeserializer::new(map))?.finish(true)
            }
            fn visit_seq<A: serde::de::SeqAccess<'de>>(self, sequence: A) -> Result<Self::Value, A::Error> {
                Fields::deserialize(serde::de::value::SeqAccessDeserializer::new(sequence))?.finish(false)
            }
        }
        deserializer.deserialize_any(EntryVisitor)
    }
}

#[derive(Deserialize)]
#[serde(untagged)]
enum CatalogCommitStatusRepresentation {
    String(String),
    Legacy(CommitLogStatus),
}

impl CatalogCommitStatusRepresentation {
    fn is_string(&self) -> bool {
        matches!(self, Self::String(_))
    }
    fn into_status<E: serde::de::Error>(self) -> Result<CommitLogStatus, E> {
        match self {
            Self::String(value) => CommitLogStatus::deserialize(serde::de::value::StringDeserializer::new(value)),
            Self::Legacy(value) => Ok(value),
        }
    }
}

impl<'de> Deserialize<'de> for CommitLogEntry {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Fields {
            #[serde(default)]
            vector_commit_binding: Option<CatalogRecordRepresentation<VectorCommitBindingFields>>,
            version: u16,
            commit_id: String,
            idempotency_key: Option<String>,
            table_id: String,
            operation: String,
            expected_version_token: String,
            new_version_token: String,
            previous_metadata_location: String,
            new_metadata_location: String,
            #[serde(default)]
            requirements: Vec<serde_json::Value>,
            status: CatalogCommitStatusRepresentation,
            writer: Option<String>,
            created_at: Option<String>,
            updated_at: Option<String>,
            #[serde(default)]
            vector_resource_mutation: Option<VectorResourceMutationBindingV1>,
        }
        impl Fields {
            fn finish<E: serde::de::Error>(self, object: bool) -> Result<CommitLogEntry, E> {
                if self.version == 3
                    && (!object
                        || !self.status.is_string()
                        || self
                            .vector_commit_binding
                            .as_ref()
                            .is_some_and(|binding| !binding.is_object_tree()))
                {
                    return Err(E::custom("Vector v3 commit requires object records and a string status"));
                }
                Ok(CommitLogEntry {
                    vector_commit_binding: self.vector_commit_binding.map(|binding| binding.into_binding()),
                    vector_resource_mutation: self.vector_resource_mutation,
                    version: self.version,
                    commit_id: self.commit_id,
                    idempotency_key: self.idempotency_key,
                    table_id: self.table_id,
                    operation: self.operation,
                    expected_version_token: self.expected_version_token,
                    new_version_token: self.new_version_token,
                    previous_metadata_location: self.previous_metadata_location,
                    new_metadata_location: self.new_metadata_location,
                    requirements: self.requirements,
                    status: self.status.into_status()?,
                    writer: self.writer,
                    created_at: self.created_at,
                    updated_at: self.updated_at,
                })
            }
        }
        struct CommitVisitor;
        impl<'de> serde::de::Visitor<'de> for CommitVisitor {
            type Value = CommitLogEntry;
            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a Catalog commit in its supported version representation")
            }
            fn visit_map<A: serde::de::MapAccess<'de>>(self, map: A) -> Result<Self::Value, A::Error> {
                Fields::deserialize(serde::de::value::MapAccessDeserializer::new(map))?.finish(true)
            }
            fn visit_seq<A: serde::de::SeqAccess<'de>>(self, sequence: A) -> Result<Self::Value, A::Error> {
                Fields::deserialize(serde::de::value::SeqAccessDeserializer::new(sequence))?.finish(false)
            }
        }
        deserializer.deserialize_any(CommitVisitor)
    }
}

struct CatalogRecordRepresentation<T> {
    value: T,
    object: bool,
}

impl<'de, T: Deserialize<'de>> Deserialize<'de> for CatalogRecordRepresentation<T> {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct RecordVisitor<T>(std::marker::PhantomData<fn() -> T>);
        impl<'de, T: Deserialize<'de>> serde::de::Visitor<'de> for RecordVisitor<T> {
            type Value = CatalogRecordRepresentation<T>;
            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a Catalog record in its supported representation")
            }
            fn visit_map<A: serde::de::MapAccess<'de>>(self, map: A) -> Result<Self::Value, A::Error> {
                Ok(CatalogRecordRepresentation {
                    value: T::deserialize(serde::de::value::MapAccessDeserializer::new(map))?,
                    object: true,
                })
            }
            fn visit_seq<A: serde::de::SeqAccess<'de>>(self, sequence: A) -> Result<Self::Value, A::Error> {
                Ok(CatalogRecordRepresentation {
                    value: T::deserialize(serde::de::value::SeqAccessDeserializer::new(sequence))?,
                    object: false,
                })
            }
        }
        deserializer.deserialize_any(RecordVisitor(std::marker::PhantomData))
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CatalogDescriptorFields {
    encoding_version: u16,
    format: String,
    format_version: u16,
    metadata: CatalogRecordRepresentation<CatalogMetadataRefRecordV1>,
    payload_base64: String,
}

impl CatalogDescriptorFields {
    fn into_record(self) -> CatalogFormatDescriptorRecordV1 {
        CatalogFormatDescriptorRecordV1 {
            encoding_version: self.encoding_version,
            format: self.format,
            format_version: self.format_version,
            metadata: self.metadata.value,
            payload_base64: self.payload_base64,
        }
    }
}

impl CatalogRecordRepresentation<CatalogDescriptorFields> {
    fn is_object_tree(&self) -> bool {
        self.object && self.value.metadata.object
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct VectorHeadBindingFields {
    version: u16,
    bucket: CatalogRecordRepresentation<VectorBucketBindingV1>,
    table_id: String,
    metadata: CatalogRecordRepresentation<CatalogMetadataRefRecordV1>,
    descriptor: CatalogRecordRepresentation<CatalogDescriptorFields>,
}

impl CatalogRecordRepresentation<VectorHeadBindingFields> {
    fn is_object_tree(&self) -> bool {
        self.object && self.value.bucket.object && self.value.metadata.object && self.value.descriptor.is_object_tree()
    }

    fn into_binding(self) -> VectorHeadBindingV1 {
        VectorHeadBindingV1 {
            version: self.value.version,
            bucket: self.value.bucket.value,
            table_id: self.value.table_id,
            metadata: self.value.metadata.value,
            descriptor: self.value.descriptor.value.into_record(),
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct VectorCommitBindingFields {
    version: u16,
    bucket: CatalogRecordRepresentation<VectorBucketBindingV1>,
    table_id: String,
    expected_generation: u64,
    expected_metadata: Option<CatalogRecordRepresentation<CatalogMetadataRefRecordV1>>,
    expected_descriptor: Option<CatalogRecordRepresentation<CatalogDescriptorFields>>,
    candidate_metadata: CatalogRecordRepresentation<CatalogMetadataRefRecordV1>,
    descriptor: CatalogRecordRepresentation<CatalogDescriptorFields>,
    request_digest_sha256: String,
}

impl CatalogRecordRepresentation<VectorCommitBindingFields> {
    fn is_object_tree(&self) -> bool {
        self.object
            && self.value.bucket.object
            && self.value.expected_metadata.as_ref().is_none_or(|metadata| metadata.object)
            && self
                .value
                .expected_descriptor
                .as_ref()
                .is_none_or(|descriptor| descriptor.is_object_tree())
            && self.value.candidate_metadata.object
            && self.value.descriptor.is_object_tree()
    }

    fn into_binding(self) -> VectorCommitBindingV1 {
        VectorCommitBindingV1 {
            version: self.value.version,
            bucket: self.value.bucket.value,
            table_id: self.value.table_id,
            expected_generation: self.value.expected_generation,
            expected_metadata: self.value.expected_metadata.map(|metadata| metadata.value),
            expected_descriptor: self
                .value
                .expected_descriptor
                .map(|descriptor| descriptor.value.into_record()),
            candidate_metadata: self.value.candidate_metadata.value,
            descriptor: self.value.descriptor.value.into_record(),
            request_digest_sha256: self.value.request_digest_sha256,
        }
    }
}

impl PreparedCatalogCommit {
    pub(in crate::table_catalog) fn with_vector_resource_mutation(
        mut self,
        mutation: VectorResourceMutationBindingV1,
    ) -> TableCatalogStoreResult<Self> {
        if self.resource_identity.format() != CatalogFormat::Lance || self.vector_resource_mutation.is_some() {
            return Err(TableCatalogStoreError::Invalid("invalid prepared Vector resource mutation".into()));
        }
        let expected_metadata = self.expected_head.metadata().ok_or_else(|| {
            TableCatalogStoreError::Invalid("Vector resource mutation requires an existing exact metadata reference".into())
        })?;
        let expected_descriptor = self.expected_head.format_descriptor().ok_or_else(|| {
            TableCatalogStoreError::Invalid("Vector resource mutation requires an existing exact descriptor".into())
        })?;
        mutation.validate_metadata(expected_metadata, expected_descriptor, &self.candidate_metadata, &self.format_descriptor)?;
        self.request_digest = mutation.request_digest(&self)?;
        self.vector_resource_mutation = Some(mutation);
        Ok(self)
    }

    pub(in crate::table_catalog) fn vector_resource_mutation(&self) -> Option<&VectorResourceMutationBindingV1> {
        self.vector_resource_mutation.as_ref()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum VectorIndexCreatePhase {
    Reserved,
    CandidateReady,
    Published,
    Aborted,
    ManualReview,
}
impl<'de> Deserialize<'de> for VectorIndexCreatePhase {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        match String::deserialize(deserializer)?.as_str() {
            "RESERVED" => Ok(Self::Reserved),
            "CANDIDATE_READY" => Ok(Self::CandidateReady),
            "PUBLISHED" => Ok(Self::Published),
            "ABORTED" => Ok(Self::Aborted),
            "MANUAL_REVIEW" => Ok(Self::ManualReview),
            _ => Err(serde::de::Error::custom("unsupported Vector lifecycle phase")),
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum VectorIndexDeletePhase {
    Prepared,
    LogicallyDeleted,
    RetirementPending,
    Complete,
    ManualReview,
}
impl<'de> Deserialize<'de> for VectorIndexDeletePhase {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        match String::deserialize(deserializer)?.as_str() {
            "PREPARED" => Ok(Self::Prepared),
            "LOGICALLY_DELETED" => Ok(Self::LogicallyDeleted),
            "RETIREMENT_PENDING" => Ok(Self::RetirementPending),
            "COMPLETE" => Ok(Self::Complete),
            "MANUAL_REVIEW" => Ok(Self::ManualReview),
            _ => Err(serde::de::Error::custom("unsupported Vector lifecycle phase")),
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct VectorIndexCreateParentV1 {
    pub bucket_entry_etag: String,
    pub bucket: VectorBucketBindingV1,
    pub state: TableCatalogEntryState,
}
impl<'de> Deserialize<'de> for VectorIndexCreateParentV1 {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Fields {
            bucket_entry_etag: String,
            #[serde(deserialize_with = "deserialize_vector_bucket_record")]
            bucket: VectorBucketBindingV1,
            #[serde(deserialize_with = "deserialize_vector_entry_state")]
            state: TableCatalogEntryState,
        }
        let fields: Fields = deserialize_vector_record(deserializer)?;
        Ok(Self {
            bucket_entry_etag: fields.bucket_entry_etag,
            bucket: fields.bucket,
            state: fields.state,
        })
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct VectorIndexCreateNameV1 {
    pub table_id: String,
    pub version_token: String,
    pub generation: u64,
    pub state: TableCatalogEntryState,
    pub entry_etag: String,
}
impl<'de> Deserialize<'de> for VectorIndexCreateNameV1 {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Fields {
            table_id: String,
            version_token: String,
            generation: u64,
            #[serde(deserialize_with = "deserialize_vector_entry_state")]
            state: TableCatalogEntryState,
            entry_etag: String,
        }
        let fields: Fields = deserialize_vector_record(deserializer)?;
        Ok(Self {
            table_id: fields.table_id,
            version_token: fields.version_token,
            generation: fields.generation,
            state: fields.state,
            entry_etag: fields.entry_etag,
        })
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct VectorIndexCreateCandidateV1 {
    pub metadata: CatalogMetadataRefRecordV1,
    pub descriptor: CatalogFormatDescriptorRecordV1,
}
impl<'de> Deserialize<'de> for VectorIndexCreateCandidateV1 {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Fields {
            #[serde(deserialize_with = "deserialize_vector_metadata_record")]
            metadata: CatalogMetadataRefRecordV1,
            #[serde(deserialize_with = "deserialize_vector_descriptor_record")]
            descriptor: CatalogFormatDescriptorRecordV1,
        }
        let fields: Fields = deserialize_vector_record(deserializer)?;
        Ok(Self {
            metadata: fields.metadata,
            descriptor: fields.descriptor,
        })
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct VectorIndexCreateIntentV1 {
    pub version: u16,
    pub bucket: VectorBucketBindingV1,
    pub namespace: String,
    pub name: String,
    pub table_id: String,
    pub create_id: String,
    pub request_id: String,
    pub request_sha256: String,
    pub expected_parent: VectorIndexCreateParentV1,
    pub expected_name: Option<VectorIndexCreateNameV1>,
    pub definition: VectorIndexDefinitionV1,
    pub initialization_payload_base64: String,
    pub planned_manifest: CatalogMetadataRefRecordV1,
    pub candidate: Option<VectorIndexCreateCandidateV1>,
    pub phase: VectorIndexCreatePhase,
    pub published_head: Option<TableEntry>,
    pub created_at: String,
    pub updated_at: String,
}
impl<'de> Deserialize<'de> for VectorIndexCreateIntentV1 {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Fields {
            version: u16,
            #[serde(deserialize_with = "deserialize_vector_bucket_record")]
            bucket: VectorBucketBindingV1,
            namespace: String,
            name: String,
            table_id: String,
            create_id: String,
            request_id: String,
            request_sha256: String,
            expected_parent: VectorIndexCreateParentV1,
            #[serde(deserialize_with = "deserialize_vector_required_option")]
            expected_name: Option<VectorIndexCreateNameV1>,
            definition: VectorIndexDefinitionV1,
            initialization_payload_base64: String,
            #[serde(deserialize_with = "deserialize_vector_metadata_record")]
            planned_manifest: CatalogMetadataRefRecordV1,
            #[serde(deserialize_with = "deserialize_vector_required_option")]
            candidate: Option<VectorIndexCreateCandidateV1>,
            phase: VectorIndexCreatePhase,
            #[serde(deserialize_with = "deserialize_vector_required_option")]
            published_head: Option<TableEntry>,
            created_at: String,
            updated_at: String,
        }
        let fields: Fields = deserialize_vector_record(deserializer)?;
        Ok(Self {
            version: fields.version,
            bucket: fields.bucket,
            namespace: fields.namespace,
            name: fields.name,
            table_id: fields.table_id,
            create_id: fields.create_id,
            request_id: fields.request_id,
            request_sha256: fields.request_sha256,
            expected_parent: fields.expected_parent,
            expected_name: fields.expected_name,
            definition: fields.definition,
            initialization_payload_base64: fields.initialization_payload_base64,
            planned_manifest: fields.planned_manifest,
            candidate: fields.candidate,
            phase: fields.phase,
            published_head: fields.published_head,
            created_at: fields.created_at,
            updated_at: fields.updated_at,
        })
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct VectorIndexDeleteIntentV1 {
    pub version: u16,
    pub bucket: VectorBucketBindingV1,
    pub namespace: String,
    pub name: String,
    pub table_id: String,
    pub delete_id: String,
    pub phase: VectorIndexDeletePhase,
    pub expected_entry: TableEntry,
    pub deleted_entry: TableEntry,
    pub request_sha256: String,
    pub created_at: String,
    pub updated_at: String,
}
impl<'de> Deserialize<'de> for VectorIndexDeleteIntentV1 {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Fields {
            version: u16,
            #[serde(deserialize_with = "deserialize_vector_bucket_record")]
            bucket: VectorBucketBindingV1,
            namespace: String,
            name: String,
            table_id: String,
            delete_id: String,
            phase: VectorIndexDeletePhase,
            expected_entry: TableEntry,
            deleted_entry: TableEntry,
            request_sha256: String,
            created_at: String,
            updated_at: String,
        }
        let fields: Fields = deserialize_vector_record(deserializer)?;
        Ok(Self {
            version: fields.version,
            bucket: fields.bucket,
            namespace: fields.namespace,
            name: fields.name,
            table_id: fields.table_id,
            delete_id: fields.delete_id,
            phase: fields.phase,
            expected_entry: fields.expected_entry,
            deleted_entry: fields.deleted_entry,
            request_sha256: fields.request_sha256,
            created_at: fields.created_at,
            updated_at: fields.updated_at,
        })
    }
}
fn deserialize_vector_required_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::deserialize(deserializer)
}

fn deserialize_vector_bucket_record<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<VectorBucketBindingV1, D::Error> {
    deserialize_vector_record(deserializer)
}

fn deserialize_vector_metadata_record<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<CatalogMetadataRefRecordV1, D::Error> {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Fields {
        object_key: String,
        content_length: u64,
        content_sha256: String,
        #[serde(deserialize_with = "deserialize_vector_required_option")]
        body_revision: Option<Uuid>,
        #[serde(deserialize_with = "deserialize_vector_required_option")]
        etag: Option<String>,
    }
    let fields: Fields = deserialize_vector_record(deserializer)?;
    Ok(CatalogMetadataRefRecordV1 {
        object_key: fields.object_key,
        content_length: fields.content_length,
        content_sha256: fields.content_sha256,
        body_revision: fields.body_revision,
        etag: fields.etag,
    })
}

fn deserialize_vector_descriptor_record<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<CatalogFormatDescriptorRecordV1, D::Error> {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Fields {
        encoding_version: u16,
        format: String,
        format_version: u16,
        #[serde(deserialize_with = "deserialize_vector_metadata_record")]
        metadata: CatalogMetadataRefRecordV1,
        payload_base64: String,
    }
    let fields: Fields = deserialize_vector_record(deserializer)?;
    Ok(CatalogFormatDescriptorRecordV1 {
        encoding_version: fields.encoding_version,
        format: fields.format,
        format_version: fields.format_version,
        metadata: fields.metadata,
        payload_base64: fields.payload_base64,
    })
}

fn validate_vector_intent_uuid(value: &str) -> TableCatalogStoreResult<()> {
    let id = Uuid::parse_str(value).map_err(|_| TableCatalogStoreError::Invalid("invalid Vector lifecycle UUID".into()))?;
    if id.is_nil() || id.to_string() != value {
        return Err(TableCatalogStoreError::Invalid(
            "Vector lifecycle UUID must be canonical and non-nil".into(),
        ));
    }
    Ok(())
}

fn validate_vector_intent_timestamps(created_at: &str, updated_at: &str) -> TableCatalogStoreResult<()> {
    let parse = |value| {
        OffsetDateTime::parse(value, &time::format_description::well_known::Rfc3339)
            .map_err(|_| TableCatalogStoreError::Invalid("invalid Vector lifecycle UTC timestamp".into()))
    };
    let created = parse(created_at)?;
    let updated = parse(updated_at)?;
    if created.offset() != time::UtcOffset::UTC || updated.offset() != time::UtcOffset::UTC || updated < created {
        return Err(TableCatalogStoreError::Invalid(
            "invalid Vector lifecycle timestamp order or offset".into(),
        ));
    }
    Ok(())
}

impl VectorIndexCreateIntentV1 {
    pub(in crate::table_catalog) fn resource_identity(&self) -> TableCatalogStoreResult<CatalogResourceIdentity> {
        validate_vector_intent_uuid(&self.table_id)?;
        CatalogResourceIdentity::try_new(
            self.bucket.to_handle()?,
            ManagedTableId::try_new(self.table_id.clone()).map_err(|error| TableCatalogStoreError::Invalid(error.to_string()))?,
            CatalogFormat::Lance,
        )
    }

    pub(in crate::table_catalog) fn canonical_request_digest(&self) -> TableCatalogStoreResult<CatalogRequestDigest> {
        #[derive(Serialize)]
        struct Description<'a> {
            bucket: &'a VectorBucketBindingV1,
            namespace: &'a str,
            name: &'a str,
            table_id: &'a str,
            create_id: &'a str,
            definition: &'a VectorIndexDefinitionV1,
            initialization_payload_base64: &'a str,
            planned_manifest: &'a CatalogMetadataRefRecordV1,
        }
        let bytes = serde_json::to_vec(&Description {
            bucket: &self.bucket,
            namespace: &self.namespace,
            name: &self.name,
            table_id: &self.table_id,
            create_id: &self.create_id,
            definition: &self.definition,
            initialization_payload_base64: &self.initialization_payload_base64,
            planned_manifest: &self.planned_manifest,
        })
        .map_err(|error| TableCatalogStoreError::Invalid(format!("encode Vector creation digest: {error}")))?;
        let mut digest = Sha256::new();
        digest.update(b"rustfs.vector.create.v1");
        digest.update(bytes);
        Ok(CatalogRequestDigest::from_bytes(digest.finalize().into()))
    }

    pub(in crate::table_catalog) fn validate(&self) -> TableCatalogStoreResult<()> {
        if self.version != 1 {
            return Err(TableCatalogStoreError::Unsupported("unsupported Vector create intent version".into()));
        }
        let resource = self.resource_identity()?;
        if self.namespace != "indexes" {
            return Err(TableCatalogStoreError::Invalid("Vector create intent has another namespace".into()));
        }
        CatalogName::parse(&self.name).map_err(|error| TableCatalogStoreError::Invalid(error.to_string()))?;
        validate_vector_intent_uuid(&self.create_id)?;
        validate_vector_intent_uuid(&self.request_id)?;
        self.definition.validate()?;
        validate_vector_intent_timestamps(&self.created_at, &self.updated_at)?;
        validate_catalog_identity_text(&self.expected_parent.bucket_entry_etag, "Vector parent ETag")?;
        if self.expected_parent.bucket != self.bucket || self.expected_parent.state != TableCatalogEntryState::Active {
            return Err(TableCatalogStoreError::Invalid("Vector create parent differs from its resource".into()));
        }
        if let Some(previous) = &self.expected_name {
            validate_vector_intent_uuid(&previous.table_id)?;
            validate_catalog_identity_text(&previous.version_token, "Vector replaced token")?;
            validate_catalog_identity_text(&previous.entry_etag, "Vector replaced entry ETag")?;
            if previous.table_id == self.table_id || previous.generation == 0 || previous.state != TableCatalogEntryState::Deleted
            {
                return Err(TableCatalogStoreError::Invalid(
                    "Vector name replacement requires another deleted resource".into(),
                ));
            }
        }
        let initialization = base64_simd::STANDARD
            .decode_to_vec(self.initialization_payload_base64.as_bytes())
            .map_err(|_| TableCatalogStoreError::Invalid("invalid Vector initialization payload base64".into()))?;
        if initialization.is_empty()
            || base64_simd::STANDARD.encode_to_string(&initialization) != self.initialization_payload_base64
        {
            return Err(TableCatalogStoreError::Invalid(
                "Vector initialization payload requires canonical nonempty base64".into(),
            ));
        }
        let planned = CatalogMetadataRef::try_from(&self.planned_manifest)?;
        if !planned.object_key().as_str().starts_with(&resource.dataset_prefix())
            || planned.body_revision().is_some()
            || planned.etag().is_some()
        {
            return Err(TableCatalogStoreError::Invalid(
                "Vector creation plan must bind immutable bytes before upload".into(),
            ));
        }
        if let Some(candidate) = &self.candidate {
            let metadata = CatalogMetadataRef::try_from(&candidate.metadata)?;
            let descriptor = CatalogFormatDescriptor::try_from(&candidate.descriptor)?;
            VectorHeadBindingV1::try_new(&resource, &metadata, &descriptor)?;
            if metadata.object_key() != planned.object_key()
                || metadata.content_length() != planned.content_length()
                || metadata.content_sha256() != planned.content_sha256()
                || metadata.body_revision().is_none()
            {
                return Err(TableCatalogStoreError::Invalid(
                    "Vector creation candidate differs from reserved content".into(),
                ));
            }
        }
        if let Some(published) = &self.published_head {
            let head = CatalogHead::from_vector_entry(published)?;
            let candidate = self
                .candidate
                .as_ref()
                .ok_or_else(|| TableCatalogStoreError::Invalid("published Vector create has no candidate".into()))?;
            if published.version != 3
                || published.namespace != self.namespace
                || published.table != self.name
                || published.state != TableCatalogEntryState::Active
                || published.generation != 1
                || published.vector_index_definition.as_ref() != Some(&self.definition)
                || head.resource_identity() != &resource
                || CatalogMetadataRefRecordV1::from(head.metadata()) != candidate.metadata
                || CatalogFormatDescriptorRecordV1::from(head.format_descriptor()) != candidate.descriptor
            {
                return Err(TableCatalogStoreError::Invalid(
                    "published Vector create differs from its reservation".into(),
                ));
            }
        }
        let consistent_phase = match self.phase {
            VectorIndexCreatePhase::Reserved => self.candidate.is_none() && self.published_head.is_none(),
            VectorIndexCreatePhase::CandidateReady => self.candidate.is_some() && self.published_head.is_none(),
            VectorIndexCreatePhase::Published => self.candidate.is_some() && self.published_head.is_some(),
            VectorIndexCreatePhase::Aborted => self.published_head.is_none(),
            VectorIndexCreatePhase::ManualReview => true,
        };
        if !consistent_phase || decode_catalog_digest(&self.request_sha256)? != self.canonical_request_digest()?.0 {
            return Err(TableCatalogStoreError::Invalid(
                "Vector creation phase or request digest is inconsistent".into(),
            ));
        }
        Ok(())
    }
}

impl VectorIndexDeleteIntentV1 {
    pub(in crate::table_catalog) fn canonical_request_digest(&self) -> TableCatalogStoreResult<CatalogRequestDigest> {
        let head = CatalogHead::from_vector_entry(&self.expected_entry)?;
        let definition = self
            .expected_entry
            .vector_index_definition
            .clone()
            .ok_or_else(|| TableCatalogStoreError::Invalid("Vector deletion requires an exact Index definition".into()))?;
        let prepared = PreparedCatalogCommit::try_from_validated_parts(
            CatalogCommitParts {
                resource_identity: head.resource_identity().clone(),
                namespace: Namespace::parse(&self.namespace)
                    .map_err(|error| TableCatalogStoreError::Invalid(error.to_string()))?,
                name: CatalogName::parse(&self.name).map_err(|error| TableCatalogStoreError::Invalid(error.to_string()))?,
                expected_head: head.expected_head(),
                commit_id: CatalogCommitId::try_new(self.delete_id.clone())?,
                operation: "vector-delete".into(),
                idempotency_key: None,
                writer: None,
            },
            head.metadata().clone(),
            head.format_descriptor().clone(),
            CatalogEntryUpdates::try_new(
                Some(self.expected_entry.warehouse_location.clone()),
                Some(self.expected_entry.format_version),
            )?,
            Vec::new(),
        )?;
        VectorResourceMutationBindingV1 {
            version: 1,
            kind: VectorResourceMutationKind::Delete,
            expected_definition: definition.clone(),
            next_definition: definition,
            expected_state: TableCatalogEntryState::Active,
            next_state: TableCatalogEntryState::Deleted,
        }
        .request_digest(&prepared)
    }

    pub(in crate::table_catalog) fn validate(&self) -> TableCatalogStoreResult<()> {
        if self.version != 1 {
            return Err(TableCatalogStoreError::Unsupported("unsupported Vector delete intent version".into()));
        }
        validate_vector_intent_uuid(&self.table_id)?;
        validate_vector_intent_uuid(&self.delete_id)?;
        validate_vector_intent_timestamps(&self.created_at, &self.updated_at)?;
        let expected = CatalogHead::from_vector_entry(&self.expected_entry)?;
        let deleted = CatalogHead::from_vector_entry(&self.deleted_entry)?;
        let next_generation = self
            .expected_entry
            .generation
            .checked_add(1)
            .ok_or_else(|| TableCatalogStoreError::Invalid("Vector deletion generation overflow".into()))?;
        let mut intended = self.expected_entry.clone();
        intended.state = TableCatalogEntryState::Deleted;
        intended.version_token.clone_from(&self.deleted_entry.version_token);
        intended.generation = next_generation;
        intended.updated_at.clone_from(&self.deleted_entry.updated_at);
        if self.expected_entry.version != 3
            || self.deleted_entry.version != 3
            || self.namespace != "indexes"
            || self.expected_entry.namespace != self.namespace
            || self.expected_entry.table != self.name
            || self.expected_entry.table_id != self.table_id
            || self.expected_entry.state != TableCatalogEntryState::Active
            || expected.resource_identity().vector_bucket() != Some(&self.bucket.to_handle()?)
            || expected.resource_identity() != deleted.resource_identity()
            || self.expected_entry.version_token == self.deleted_entry.version_token
            || intended != self.deleted_entry
            || decode_catalog_digest(&self.request_sha256)? != self.canonical_request_digest()?.0
        {
            return Err(TableCatalogStoreError::Invalid(
                "Vector delete intent does not bind the exact resource transition".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub(crate) struct VectorIndexCreateRequest {
    pub resource_identity: CatalogResourceIdentity,
    pub namespace: Namespace,
    pub name: CatalogName,
    pub create_id: CatalogCommitId,
    pub request_id: Uuid,
    pub definition: VectorIndexDefinitionV1,
    pub initialization_payload: Vec<u8>,
    pub planned_manifest: CatalogMetadataRef,
}

impl CatalogExpectedHead {
    pub(in crate::table_catalog) fn for_vector_creation(create_id: &str) -> TableCatalogStoreResult<Self> {
        validate_vector_intent_uuid(create_id)?;
        Ok(Self {
            version_token: CatalogVersionToken::try_new(create_id.to_string())?,
            generation: CatalogGeneration(0),
            metadata: None,
            format_descriptor: None,
        })
    }
}
