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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WarehouseIndexReservation {
    Created,
    AlreadyReserved,
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
    pub migration: TableCatalogBackingMigrationPlan,
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
    // RUSTFS_COMPAT_TODO(table-catalog-backing-manifest-v1-wire-labels): Keep the version 1 wire label for existing clients. Remove after a versioned manifest with an explicit client migration contract replaces it.
    #[serde(rename = "STRONG_KV_WAL")]
    DurableStrongSnapshot,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum TableCatalogAuthority {
    RustfsSysObject,
    #[allow(
        dead_code,
        reason = "exercised by table_catalog/tests.rs; the lib target cannot see test-only consumers (backlog#1823)"
    )]
    LinearizableMetadataKv,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum TableCatalogConsistencyMode {
    ConditionalObjectCas,
    #[allow(
        dead_code,
        reason = "exercised by table_catalog/tests.rs; the lib target cannot see test-only consumers (backlog#1823)"
    )]
    LinearizableCas,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum TableCatalogDurabilityMode {
    StagedCommitLogBeforePointerUpdate,
    #[allow(
        dead_code,
        reason = "exercised by table_catalog/tests.rs; the lib target cannot see test-only consumers (backlog#1823)"
    )]
    WalBeforeStateMachineApply,
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
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct TableCatalogBackingMigrationPlan {
    pub source_kind: TableCatalogBackingKind,
    pub target_kind: TableCatalogBackingKind,
    pub status: TableCatalogBackingMigrationStatus,
    pub required_steps: Vec<TableCatalogBackingMigrationStep>,
    pub blockers: Vec<TableCatalogBackingMigrationBlocker>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct TableCatalogBackingMigrationDryRunReport {
    pub table_bucket: String,
    pub source_kind: TableCatalogBackingKind,
    pub target_kind: TableCatalogBackingKind,
    pub status: TableCatalogBackingMigrationStatus,
    pub namespace_count: usize,
    pub table_count: usize,
    pub view_count: usize,
    pub commit_log_count: usize,
    pub idempotency_index_count: usize,
    pub warehouse_prefix_count: usize,
    pub warehouse_index_ready: bool,
    pub object_backed_writes_fenced: bool,
    pub ready_to_enable_durable_strong: bool,
    pub blockers: Vec<TableCatalogBackingMigrationBlocker>,
    pub recommended_actions: Vec<TableCatalogBackingMigrationAction>,
    pub rollback: TableCatalogBackingRollbackPlan,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct TableCatalogBackingMigrationExecutionReport {
    pub table_bucket: String,
    pub source_kind: TableCatalogBackingKind,
    pub target_kind: TableCatalogBackingKind,
    pub status: TableCatalogBackingMigrationExecutionStatus,
    pub namespace_count: usize,
    pub table_count: usize,
    pub view_count: usize,
    pub commit_log_count: usize,
    pub idempotency_index_count: usize,
    pub source_fingerprint: String,
    pub target_snapshot_etag: String,
    pub object_backed_writes_fenced: bool,
    pub ready_to_enable_durable_strong: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum TableCatalogBackingMigrationExecutionStatus {
    SnapshotMaterialized,
    SnapshotAlreadyMaterialized,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct TableCatalogBackingMigrationCancelReport {
    pub table_bucket: String,
    pub status: TableCatalogBackingMigrationCancelStatus,
    pub object_backed_writes_fenced: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum TableCatalogBackingMigrationCancelStatus {
    FenceReleased,
    NoMigrationFence,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum TableCatalogBackingMigrationStatus {
    ReadyToSnapshot,
    SnapshotMaterialized,
    RecoveryRequired,
    ManualReviewRequired,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum TableCatalogBackingMigrationStep {
    SnapshotCatalogExport,
    ReplayCommitLog,
    VerifyCurrentPointer,
    EnableSingleWriterFencing,
    // RUSTFS_COMPAT_TODO(table-catalog-backing-manifest-v1-wire-labels): Keep the version 1 wire label for existing clients. Remove after a versioned manifest with an explicit client migration contract replaces it.
    #[serde(rename = "CUT_OVER_LINEARIZABLE_READS")]
    CutOverDurableSnapshotReads,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum TableCatalogBackingMigrationBlocker {
    CommitRecoveryRequired,
    CommitManualReviewRequired,
    WarehouseIndexBackfillRequired,
    DuplicateWarehousePrefix,
    DuplicateTableIdentity,
    TableViewIdentifierCollision,
    DurableStrongSnapshotChanged,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum TableCatalogBackingMigrationAction {
    RunCatalogRecovery,
    BackfillWarehouseIndex,
    ReviewDuplicateWarehousePrefixes,
    ReviewDuplicateTableIdentities,
    ReviewTableViewIdentifierCollisions,
    SnapshotObjectBackedCatalog,
    EnableDurableStrongBacking,
    VerifyDurableStrongSnapshot,
    SnapshotRemainingTableBuckets,
    ReviewDurableStrongSnapshot,
    KeepObjectBackedRollbackConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct TableCatalogBackingRollbackPlan {
    pub backing_config_key: &'static str,
    pub current_backing_value: &'static str,
    pub rollback_backing_value: &'static str,
    pub preserves_object_backed_catalog: bool,
    pub requires_operator_restart: bool,
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
    MigrationSnapshotReplay,
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
