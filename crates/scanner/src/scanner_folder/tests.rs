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

use crate::SCANNER_SLEEPER;

use super::*;
use crate::storage_api::VersionPurgeStatusType;
use crate::{DiskOption, Endpoint, STORAGE_FORMAT_FILE, TierStats, new_disk, storageclass};
use rustfs_filemeta::{FileInfo, FileMeta, MetadataResolutionParams};
use std::io::Write;
#[cfg(unix)]
use std::os::unix::fs::{PermissionsExt, symlink};
use std::sync::Mutex;

/// Reset the process-global alert cooldown map; test-only.
fn reset_alert_cooldowns() {
    *SCANNER_ALERT_EMISSION_COOLDOWN
        .lock()
        .unwrap_or_else(|poison| poison.into_inner()) = Some(ScannerAlertCooldownMap::new());
}

/// The emitted event-name strings must be exactly what `EventName`
/// serializes, or a bucket notification subscribed to the documented name
/// would silently never match (rustfs/backlog#1868).
#[test]
fn scanner_alert_wire_names_match_canonical_event_names() {
    use rustfs_s3_types::EventName;
    assert_eq!(EVENT_SCANNER_MANY_VERSIONS, EventName::ScannerManyVersions.to_string());
    assert_eq!(EVENT_SCANNER_LARGE_VERSIONS, EventName::ScannerLargeVersions.to_string());
    assert_eq!(EVENT_SCANNER_BIG_PREFIX, EventName::ScannerBigPrefix.to_string());
}

/// Single-flight decision for the corrupt-metadata branch (backlog#1894
/// axis A): an accepted MRF intent must drop the immediate heal request
/// (the consumer files one; the manager would double-book) while a
/// rejected one must keep it — in both cases a ledger entry remains, so
/// the backstop survives regardless of delivery.
#[test]
fn corrupt_metadata_recording_maps_delivery_to_backstop() {
    assert_eq!(
        corrupt_metadata_recording(rustfs_common::mrf_channel::MrfIngressResult::Enqueued),
        CorruptMetadataRecording::LedgerOnly
    );
    assert_eq!(
        corrupt_metadata_recording(rustfs_common::mrf_channel::MrfIngressResult::Coalesced),
        CorruptMetadataRecording::LedgerOnly
    );
    assert_eq!(
        corrupt_metadata_recording(rustfs_common::mrf_channel::MrfIngressResult::Dropped(
            rustfs_common::mrf_channel::MrfDropReason::Full
        )),
        CorruptMetadataRecording::ImmediateAndLedger
    );
}

fn cooldown_map_len() -> usize {
    SCANNER_ALERT_EMISSION_COOLDOWN
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .as_ref()
        .map(|map| map.len())
        .unwrap_or(0)
}

/// Backdate every recorded cooldown so the next check fires again.
fn expire_all_alert_cooldowns(cooldown: Duration) {
    let now = Instant::now();
    let mut guard = SCANNER_ALERT_EMISSION_COOLDOWN
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
    if let Some(map) = guard.as_mut() {
        for fired_at in map.values_mut() {
            if let Some(expired) = now.checked_sub(cooldown + Duration::from_secs(1)) {
                *fired_at = expired;
            }
        }
    }
}

/// The emission gate is the only thing standing between an over-threshold
/// object and one S3 event per scan cycle, so its edge semantics get
/// pinned directly. All scenarios share one #[test] because the cooldown
/// map is process-global and parallel tests would read each other's
/// firings.
#[test]
fn scanner_alert_emission_is_edge_held_per_key_and_bounded() {
    reset_alert_cooldowns();
    let cooldown = Duration::from_secs(3600);

    // First firing allows, an immediate re-check is held.
    assert!(scanner_alert_emission_allows(ScannerAlertKind::ManyVersions, "bkt", "obj", cooldown));
    assert!(!scanner_alert_emission_allows(ScannerAlertKind::ManyVersions, "bkt", "obj", cooldown));

    // Different kind, object, and bucket are independent keys.
    assert!(scanner_alert_emission_allows(ScannerAlertKind::LargeVersions, "bkt", "obj", cooldown));
    assert!(scanner_alert_emission_allows(ScannerAlertKind::ManyVersions, "bkt", "other", cooldown));
    assert!(scanner_alert_emission_allows(ScannerAlertKind::ManyVersions, "other", "obj", cooldown));
    assert_eq!(cooldown_map_len(), 4);

    // After the cooldown elapses the same key fires again.
    expire_all_alert_cooldowns(cooldown);
    assert!(scanner_alert_emission_allows(ScannerAlertKind::ManyVersions, "bkt", "obj", cooldown));

    // A zero cooldown degenerates to always-emit (operators may want that).
    assert!(scanner_alert_emission_allows(ScannerAlertKind::BigPrefix, "bkt", "dir", Duration::ZERO));
    assert!(scanner_alert_emission_allows(ScannerAlertKind::BigPrefix, "bkt", "dir", Duration::ZERO));

    // Hard bound: overflow the cap with zero-cooldown keys and confirm the
    // map clears rather than growing past it.
    reset_alert_cooldowns();
    for index in 0..=(MAX_SCANNER_ALERT_COOLDOWN_KEYS + 8) {
        let _ = scanner_alert_emission_allows(ScannerAlertKind::BigPrefix, "bkt", &format!("dir-{index}"), Duration::ZERO);
    }
    assert!(
        cooldown_map_len() <= MAX_SCANNER_ALERT_COOLDOWN_KEYS,
        "cooldown map must stay bounded, got {}",
        cooldown_map_len()
    );
}

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use temp_env::{with_var, with_var_unset};
use tracing_subscriber::fmt::MakeWriter;
use uuid::Uuid;

#[derive(Clone, Default)]
struct CapturedLogs {
    buffer: Arc<Mutex<Vec<u8>>>,
}

struct CapturedLogWriter {
    buffer: Arc<Mutex<Vec<u8>>>,
}

impl CapturedLogs {
    fn contents(&self) -> String {
        let buffer = self
            .buffer
            .lock()
            .expect("captured logs mutex should not be poisoned")
            .clone();
        String::from_utf8(buffer).expect("captured logs should be valid UTF-8")
    }
}

impl Write for CapturedLogWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.buffer
            .lock()
            .expect("captured logs mutex should not be poisoned")
            .extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for CapturedLogs {
    type Writer = CapturedLogWriter;

    fn make_writer(&'a self) -> Self::Writer {
        CapturedLogWriter {
            buffer: Arc::clone(&self.buffer),
        }
    }
}

#[test]
fn scanner_size_summary_application_saturates_usage_counters() {
    let target = "arn:minio:replication::target".to_string();
    let mut entry = DataUsageEntry {
        size: usize::MAX,
        versions: usize::MAX,
        delete_markers: usize::MAX,
        replication_stats: Some(Default::default()),
        ..Default::default()
    };
    let replication_stats = entry
        .replication_stats
        .as_mut()
        .expect("replication statistics should be initialized");
    replication_stats.replica_size = u64::MAX;
    replication_stats.replica_count = u64::MAX;
    let target_stats = replication_stats.targets.entry(target.clone()).or_default();
    target_stats.pending_size = u64::MAX;
    target_stats.failed_size = u64::MAX;
    target_stats.replicated_size = u64::MAX;
    target_stats.replicated_count = u64::MAX;
    target_stats.failed_count = u64::MAX;
    target_stats.pending_count = u64::MAX;

    let mut summary = SizeSummary {
        total_size: 1,
        versions: 1,
        delete_markers: 1,
        replica_size: 1,
        replica_count: 1,
        ..Default::default()
    };
    summary.repl_target_stats.insert(
        target.clone(),
        ReplTargetSizeSummary {
            replicated_size: 1,
            replicated_count: 1,
            pending_size: 1,
            failed_size: 1,
            pending_count: 1,
            failed_count: 1,
        },
    );

    apply_scanner_size_summary(&mut entry, &summary);

    assert_eq!(entry.size, usize::MAX);
    assert_eq!(entry.versions, usize::MAX);
    assert_eq!(entry.delete_markers, usize::MAX);
    assert_eq!(entry.obj_sizes.to_map()["LESS_THAN_1024_B"], 1);
    assert_eq!(entry.obj_versions.to_map()["SINGLE_VERSION"], 1);

    let replication_stats = entry
        .replication_stats
        .as_ref()
        .expect("replication statistics should remain present");
    assert_eq!(replication_stats.replica_size, u64::MAX);
    assert_eq!(replication_stats.replica_count, u64::MAX);
    let target_stats = replication_stats
        .targets
        .get(&target)
        .expect("replication target statistics should remain present");
    assert_eq!(target_stats.pending_size, u64::MAX);
    assert_eq!(target_stats.failed_size, u64::MAX);
    assert_eq!(target_stats.replicated_size, u64::MAX);
    assert_eq!(target_stats.replicated_count, u64::MAX);
    assert_eq!(target_stats.failed_count, u64::MAX);
    assert_eq!(target_stats.pending_count, u64::MAX);
}

#[test]
fn scanner_size_summary_application_accumulates_tier_stats() {
    let mut entry = DataUsageEntry::default();
    let mut summary = SizeSummary::default();
    summary.tier_stats.insert(
        "WARM".to_string(),
        TierStats {
            total_size: 100,
            num_versions: 2,
            num_objects: 1,
        },
    );
    // Scanners seed a zeroed entry for every configured tier; those must not
    // reach the cache as empty keys.
    summary
        .tier_stats
        .insert(storageclass::STANDARD.to_string(), TierStats::default());

    apply_scanner_size_summary(&mut entry, &summary);
    apply_scanner_size_summary(&mut entry, &summary);

    let tiers = entry.all_tier_stats.as_ref().expect("tier stats should be recorded");
    assert_eq!(
        tiers.tiers.get("WARM"),
        Some(&TierStats {
            total_size: 200,
            num_versions: 4,
            num_objects: 2,
        })
    );
    assert!(!tiers.tiers.contains_key(storageclass::STANDARD));
}

#[test]
fn scanner_size_summary_application_skips_untiered_summaries() {
    let mut entry = DataUsageEntry::default();
    let mut summary = SizeSummary {
        total_size: 10,
        ..Default::default()
    };
    summary
        .tier_stats
        .insert(storageclass::STANDARD.to_string(), TierStats::default());
    summary.tier_stats.insert(storageclass::RRS.to_string(), TierStats::default());

    apply_scanner_size_summary(&mut entry, &summary);

    assert!(entry.all_tier_stats.is_none(), "zero-only tier maps must not allocate cache state");
}

async fn build_test_scanner() -> (FolderScanner, std::path::PathBuf) {
    let temp_dir = std::env::temp_dir().join(format!("rustfs-scanner-test-{}", Uuid::new_v4()));
    tokio::fs::create_dir_all(&temp_dir)
        .await
        .expect("failed to create test directory");

    let endpoint = Endpoint::try_from(temp_dir.to_string_lossy().as_ref()).expect("failed to create endpoint");
    let disk = new_disk(
        &endpoint,
        &DiskOption {
            cleanup: false,
            health_check: false,
        },
    )
    .await
    .expect("failed to create disk");

    let update_current_path: UpdateCurrentPathFn = Arc::new(|_: &str| Box::pin(async {}));

    let scanner = FolderScanner {
        root: temp_dir.to_string_lossy().to_string(),
        old_cache: DataUsageCache::default(),
        new_cache: DataUsageCache::default(),
        update_cache: DataUsageCache::default(),
        data_usage_scanner_debug: false,
        heal_object_select: 0,
        scan_mode: HealScanMode::Normal,
        is_erasure_mode: false,
        failed_object_ttl_secs: u64::MAX,
        failed_objects_max: usize::MAX,
        sleeper: SCANNER_SLEEPER.clone(),
        disks: Vec::new(),
        disks_quorum: 0,
        updates: None,
        last_update: SystemTime::UNIX_EPOCH,
        update_current_path,
        budget: ScannerCycleBudget::new(&CancellationToken::new(), Default::default()),
        skip_heal: Arc::new(AtomicBool::new(false)),
        local_disk: disk,
        pending_heals_changed: false,
        pending_size_reconciliation_keys: HashSet::new(),
        pending_size_reconciliation_scopes: HashSet::new(),
        pending_size_reconciliation_truncated: false,
        list_path_raw_options_observer: None,
    };

    (scanner, temp_dir)
}

struct TestGuard {
    temp_dir: Option<std::path::PathBuf>,
}

impl TestGuard {
    fn new(ttl: u64, max: usize, scanner: &mut FolderScanner, temp_dir: std::path::PathBuf) -> Self {
        scanner.failed_object_ttl_secs = ttl;
        scanner.failed_objects_max = max;
        Self {
            temp_dir: Some(temp_dir),
        }
    }
}

impl Drop for TestGuard {
    fn drop(&mut self) {
        if let Some(temp_dir) = self.temp_dir.take() {
            let _ = std::fs::remove_dir_all(&temp_dir);
        }
    }
}

#[tokio::test]
async fn test_should_skip_failed_respects_ttl() {
    let (mut scanner, temp_dir) = build_test_scanner().await;
    let _guard = TestGuard::new(60, 100, &mut scanner, temp_dir);
    let now = FolderScanner::now_secs();

    scanner
        .new_cache
        .info
        .failed_objects
        .insert("recent".to_string(), now.saturating_sub(10));
    scanner
        .new_cache
        .info
        .failed_objects
        .insert("expired".to_string(), now.saturating_sub(120));

    assert!(scanner.should_skip_failed("recent"));
    assert!(!scanner.should_skip_failed("expired"));
}

#[tokio::test]
async fn test_record_failed_ttl_zero_noop() {
    let (mut scanner, temp_dir) = build_test_scanner().await;
    let _guard = TestGuard::new(0, 100, &mut scanner, temp_dir);

    scanner.record_failed("path1");
    assert!(scanner.new_cache.info.failed_objects.is_empty());

    let now = FolderScanner::now_secs();
    scanner.new_cache.info.failed_objects.insert("path2".to_string(), now);
    assert!(!scanner.should_skip_failed("path2"));
}

#[tokio::test]
async fn malformed_size_reconciliation_replays_after_restart() {
    let (mut scanner, temp_dir) = build_test_scanner().await;
    let _guard = TestGuard::new(60, 100, &mut scanner, temp_dir);

    let entry = SizeReconciliationEntry {
        key: "1:b|6:object|0:|0:".to_string(),
        bucket: "b".to_string(),
        object: "object".to_string(),
        reason: "invalid_declared_size".to_string(),
        physical_size: Some(12),
        ..Default::default()
    };
    let mut summary = SizeSummary::default();
    summary.record_size_reconciliation(entry.clone());
    summary.record_reconciliation_scope("b", "object");
    scanner.apply_size_reconciliation(&summary);
    scanner.apply_size_reconciliation(&summary);

    assert_eq!(scanner.new_cache.info.size_reconciliation.len(), 1);
    assert_eq!(scanner.update_cache.info.size_reconciliation.len(), 1);
    assert_eq!(scanner.new_cache.info.size_reconciliation[&entry.key].attempts, 2);

    let encoded = rmp_serde::to_vec_named(&scanner.new_cache.info).expect("size ledger should encode");
    let decoded: crate::data_usage_define::DataUsageCacheInfo =
        rmp_serde::from_slice(&encoded).expect("size ledger should decode");
    assert_eq!(decoded.size_reconciliation.len(), 1);
    assert_eq!(decoded.size_reconciliation[&entry.key].reason, "invalid_declared_size");

    let mut resolved = SizeSummary::default();
    resolved.record_reconciliation_scope("b", "object");
    scanner.apply_size_reconciliation(&resolved);
    assert!(scanner.new_cache.info.size_reconciliation.is_empty());
    assert!(scanner.update_cache.info.size_reconciliation.is_empty());
}

#[tokio::test]
async fn malformed_size_reconciliation_clears_bounded_long_object_scope() {
    let (mut scanner, temp_dir) = build_test_scanner().await;
    let _guard = TestGuard::new(60, 100, &mut scanner, temp_dir);
    let long_object = "o".repeat(600);
    let bounded_object = item_actions::bounded_reconciliation_field(&long_object);
    let entry = SizeReconciliationEntry {
        key: "long-object-key".to_string(),
        bucket: "b".to_string(),
        object: bounded_object,
        reason: "invalid_declared_size".to_string(),
        ..Default::default()
    };
    let mut summary = SizeSummary::default();
    summary.record_size_reconciliation(entry);
    scanner.apply_size_reconciliation(&summary);
    assert_eq!(scanner.new_cache.info.size_reconciliation.len(), 1);

    let mut resolved = SizeSummary::default();
    resolved.record_reconciliation_scope("b", &long_object);
    scanner.apply_size_reconciliation(&resolved);
    assert!(scanner.new_cache.info.size_reconciliation.is_empty());
}

#[test]
fn test_classify_get_size_failure_marks_metadata_heal_object_path() {
    let temp_dir = std::env::temp_dir();
    let file_type = std::fs::metadata(&temp_dir)
        .expect("temp dir metadata should be readable")
        .file_type();
    let item = ScannerItem {
        path: temp_dir.join("bucket/dir/object/xl.meta").to_string_lossy().to_string(),
        bucket: "bucket".to_string(),
        prefix: "dir/object".to_string(),
        object_name: "xl.meta".to_string(),
        file_type,
        lifecycle: None,
        object_lock: None,
        replication: None,
        heal_enabled: false,
        heal_bitrot: false,
        debug: false,
    };
    let err = StorageError::other(format!("{}: corrupt metadata", crate::scanner_io::SCANNER_METADATA_CORRUPT_ERROR));

    let action = classify_get_size_failure(&item, &err);

    assert_eq!(
        action,
        GetSizeFailureAction::HealMetadata {
            object: "dir/object".to_string()
        }
    );
}

#[test]
fn test_classify_get_size_failure_records_transient_metadata_error_without_heal() {
    let temp_dir = std::env::temp_dir();
    let file_type = std::fs::metadata(&temp_dir)
        .expect("temp dir metadata should be readable")
        .file_type();
    let item = ScannerItem {
        path: temp_dir.join("bucket/dir/object/xl.meta").to_string_lossy().to_string(),
        bucket: "bucket".to_string(),
        prefix: "dir/object".to_string(),
        object_name: "xl.meta".to_string(),
        file_type,
        lifecycle: None,
        object_lock: None,
        replication: None,
        heal_enabled: false,
        heal_bitrot: false,
        debug: false,
    };
    let err = StorageError::other(format!("{}: temporary read failure", crate::scanner_io::SCANNER_METADATA_TRANSIENT_ERROR));

    let action = classify_get_size_failure(&item, &err);

    assert_eq!(action, GetSizeFailureAction::RecordFailed);
}

#[test]
fn test_should_account_replication_stats_only_for_live_object_versions() {
    let live = ObjectInfo::default();
    assert!(ScannerItem::should_account_replication_stats(&live));

    let delete_marker = ObjectInfo {
        delete_marker: true,
        ..Default::default()
    };
    assert!(!ScannerItem::should_account_replication_stats(&delete_marker));

    let purge_version = ObjectInfo {
        version_purge_status: VersionPurgeStatusType::Pending,
        ..Default::default()
    };
    assert!(!ScannerItem::should_account_replication_stats(&purge_version));
}

#[tokio::test]
async fn test_heal_replication_only_queues_pending_null_deletes() {
    async fn replication_skipped_count() -> u64 {
        global_metrics()
            .report()
            .await
            .source_work
            .iter()
            .find(|work| work.source == ScannerWorkSource::BucketReplication.as_str())
            .map(|work| work.skipped)
            .unwrap_or_default()
    }

    let temp_dir = std::env::temp_dir();
    let file_type = std::fs::metadata(&temp_dir)
        .expect("temp dir metadata should be readable")
        .file_type();
    let mut item = ScannerItem {
        path: temp_dir.join("object").to_string_lossy().to_string(),
        bucket: "bucket".to_string(),
        prefix: String::new(),
        object_name: "object".to_string(),
        file_type,
        lifecycle: None,
        object_lock: None,
        replication: Some(Arc::new(ReplicationConfig::new(None, None))),
        heal_enabled: false,
        heal_bitrot: false,
        debug: false,
    };
    let null_object = ObjectInfo {
        bucket: "bucket".to_string(),
        name: "object".to_string(),
        version_id: Some(Uuid::nil()),
        mod_time: Some(OffsetDateTime::now_utc()),
        ..Default::default()
    };
    let mut size_summary = SizeSummary::default();
    let before = replication_skipped_count().await;

    item.heal_replication(&null_object, &mut size_summary).await;
    assert_eq!(replication_skipped_count().await, before);

    item.heal_replication(
        &ObjectInfo {
            version_purge_status: VersionPurgeStatusType::Pending,
            ..null_object
        },
        &mut size_summary,
    )
    .await;
    assert_eq!(replication_skipped_count().await, before + 1);
}

#[tokio::test]
async fn test_scanner_ilm_action_accounting_requires_enqueue_success() {
    let metrics = Metrics::new();
    let _start = metrics.start_scan_cycle_work();

    record_scanner_ilm_action_if_queued(&metrics, IlmAction::DeleteAction, 2, false);
    let report = metrics.report().await;
    let lifecycle = report
        .source_work
        .iter()
        .find(|work| work.source == ScannerWorkSource::Lifecycle.as_str())
        .expect("lifecycle source work should be visible");
    assert_eq!(lifecycle.executed, 0);
    assert_eq!(report.current_cycle_lifecycle_expiry_actions, 0);

    record_scanner_ilm_action_if_queued(&metrics, IlmAction::DeleteAction, 3, true);
    let report = metrics.report().await;
    let lifecycle = report
        .source_work
        .iter()
        .find(|work| work.source == ScannerWorkSource::Lifecycle.as_str())
        .expect("lifecycle source work should be visible");
    assert_eq!(lifecycle.executed, 3);
    assert_eq!(report.current_cycle_lifecycle_expiry_actions, 3);
    assert_eq!(report.current_cycle_lifecycle_transition_actions, 0);

    record_scanner_ilm_action_if_queued(&metrics, IlmAction::TransitionAction, 4, true);
    let report = metrics.report().await;
    let lifecycle = report
        .source_work
        .iter()
        .find(|work| work.source == ScannerWorkSource::Lifecycle.as_str())
        .expect("lifecycle source work should be visible");
    assert_eq!(lifecycle.executed, 7);
    assert_eq!(report.current_cycle_lifecycle_expiry_actions, 3);
    assert_eq!(report.current_cycle_lifecycle_transition_actions, 4);
}

#[test]
fn test_pending_scanner_accounting_requires_enqueue_success() {
    let object = ObjectInfo {
        size: 10,
        version_id: Some(uuid::Uuid::new_v4()),
        ..Default::default()
    };
    let pending = PendingScannerAccounting {
        object: &object,
        retained_size: 10,
        expired_size: 0,
    };

    let mut failed_summary = SizeSummary::default();
    let mut failed_cumulative_size = 0;
    pending.apply(&mut failed_summary, &mut failed_cumulative_size, false);
    assert_eq!(failed_summary.versions, 1);
    assert_eq!(failed_summary.total_size, 10);
    assert_eq!(failed_cumulative_size, 10);

    let mut queued_summary = SizeSummary::default();
    let mut queued_cumulative_size = 0;
    pending.apply(&mut queued_summary, &mut queued_cumulative_size, true);
    assert_eq!(queued_summary.versions, 0);
    assert_eq!(queued_summary.total_size, 0);
    assert_eq!(queued_cumulative_size, 0);
}

#[tokio::test]
async fn test_scanner_replication_admission_accounting_maps_source_work() {
    let metrics = Metrics::new();
    let object = ReplicationHealObject::new("bucket-a", "object-a");

    record_scanner_replication_admission(&metrics, &object, ReplicationQueueAdmission::Skipped);
    record_scanner_replication_admission(&metrics, &object, ReplicationQueueAdmission::Queued);
    record_scanner_replication_admission(&metrics, &object, ReplicationQueueAdmission::Missed);

    let report = metrics.report().await;
    let replication = report
        .source_work
        .iter()
        .find(|work| work.source == ScannerWorkSource::BucketReplication.as_str())
        .expect("bucket replication source work should be visible");

    assert_eq!(replication.skipped, 1);
    assert_eq!(replication.queued, 1);
    assert_eq!(replication.missed, 1);

    let object_repair = report
        .replication_repair
        .iter()
        .find(|repair| repair.source == ScannerWorkSource::BucketReplication.as_str() && repair.kind == "object")
        .expect("bucket object repair work should be visible");
    assert_eq!(object_repair.scanner_role, "repair_admission");
    assert_eq!(object_repair.execution_owner, "bucket_replication_queue");
    assert_eq!(object_repair.skipped, 1);
    assert_eq!(object_repair.queued, 1);
    assert_eq!(object_repair.missed, 1);
}

#[test]
fn test_scanner_replication_repair_kind_maps_bucket_variants() {
    let object = ReplicationHealObject::new("bucket-a", "object-a");
    assert_eq!(scanner_replication_repair_kind(&object), Some(ScannerReplicationRepairKind::BucketObject));

    let delete_marker = ReplicationHealObject::new("bucket-a", "delete-marker-a").with_delete_marker();
    assert_eq!(
        scanner_replication_repair_kind(&delete_marker),
        Some(ScannerReplicationRepairKind::BucketDeleteMarker)
    );

    let version_purge = ReplicationHealObject::new("bucket-a", "version-purge-a").with_pending_version_purge();
    assert_eq!(
        scanner_replication_repair_kind(&version_purge),
        Some(ScannerReplicationRepairKind::BucketVersionPurge)
    );

    let existing_object = ReplicationHealObject::new("bucket-a", "existing-object-a").with_existing_object();
    assert_eq!(
        scanner_replication_repair_kind(&existing_object),
        Some(ScannerReplicationRepairKind::BucketExistingObject)
    );

    let existing_delete_marker = ReplicationHealObject::new("bucket-a", "existing-delete-marker-a")
        .with_delete_marker()
        .with_existing_object_resync();
    assert_eq!(
        scanner_replication_repair_kind(&existing_delete_marker),
        Some(ScannerReplicationRepairKind::BucketExistingObject)
    );

    assert_eq!(scanner_replication_repair_kind(&ReplicationHealObject::default()), None);
}

#[tokio::test]
async fn test_scanner_heal_admission_accounting_maps_normal_scan_to_heal() {
    let metrics = Metrics::new();

    record_scanner_heal_admission(&metrics, HealScanMode::Normal, Ok(HealAdmissionResult::Accepted));
    record_scanner_heal_admission(&metrics, HealScanMode::Normal, Ok(HealAdmissionResult::Merged));
    record_scanner_heal_admission(&metrics, HealScanMode::Normal, Ok(HealAdmissionResult::Full));
    record_scanner_heal_admission(
        &metrics,
        HealScanMode::Normal,
        Ok(HealAdmissionResult::Dropped(
            rustfs_common::heal_channel::HealAdmissionDropReason::QueueFull,
        )),
    );
    record_scanner_heal_admission(&metrics, HealScanMode::Normal, Err(()));

    let report = metrics.report().await;
    let heal = report
        .source_work
        .iter()
        .find(|work| work.source == ScannerWorkSource::Heal.as_str())
        .expect("heal source work should be visible");
    let bitrot = report
        .source_work
        .iter()
        .find(|work| work.source == ScannerWorkSource::Bitrot.as_str())
        .expect("bitrot source work should be visible");

    assert_eq!(heal.queued, 1);
    assert_eq!(heal.skipped, 1);
    assert_eq!(heal.missed, 3);
    assert_eq!(heal.executed, 0);
    assert_eq!(bitrot.queued + bitrot.skipped + bitrot.missed + bitrot.executed, 0);
}

#[tokio::test]
async fn test_scanner_heal_admission_accounting_maps_deep_scan_to_bitrot() {
    let metrics = Metrics::new();

    record_scanner_heal_admission(&metrics, HealScanMode::Deep, Ok(HealAdmissionResult::Accepted));
    record_scanner_heal_admission(&metrics, HealScanMode::Deep, Ok(HealAdmissionResult::Merged));
    record_scanner_heal_admission(&metrics, HealScanMode::Deep, Ok(HealAdmissionResult::Full));

    let report = metrics.report().await;
    let heal = report
        .source_work
        .iter()
        .find(|work| work.source == ScannerWorkSource::Heal.as_str())
        .expect("heal source work should be visible");
    let bitrot = report
        .source_work
        .iter()
        .find(|work| work.source == ScannerWorkSource::Bitrot.as_str())
        .expect("bitrot source work should be visible");

    assert_eq!(bitrot.queued, 1);
    assert_eq!(bitrot.skipped, 1);
    assert_eq!(bitrot.missed, 1);
    assert_eq!(bitrot.executed, 0);
    assert_eq!(heal.queued + heal.skipped + heal.missed + heal.executed, 0);
}

#[test]
fn test_excessive_version_alert_thresholds_use_env() {
    with_var(rustfs_config::ENV_SCANNER_ALERT_EXCESS_VERSIONS, Some("3"), || {
        with_var(rustfs_config::ENV_SCANNER_ALERT_EXCESS_VERSION_SIZE, Some("100"), || {
            crate::runtime_config::refresh_scanner_runtime_config_for_tests();
            assert_eq!(should_alert_excessive_versions(2, 99), (false, false));
            assert_eq!(should_alert_excessive_versions(3, 99), (true, false));
            assert_eq!(should_alert_excessive_versions(2, 100), (false, true));
            assert_eq!(should_alert_excessive_versions(3, 100), (true, true));
        });
    });
    crate::runtime_config::refresh_scanner_runtime_config_for_tests();
}

#[test]
fn test_excessive_folders_threshold_uses_env() {
    with_var(rustfs_config::ENV_SCANNER_ALERT_EXCESS_FOLDERS, Some("3"), || {
        crate::runtime_config::refresh_scanner_runtime_config_for_tests();
        assert_eq!(scanner_excess_folders_threshold(), 3);
    });
    crate::runtime_config::refresh_scanner_runtime_config_for_tests();
}

#[test]
fn test_excessive_folders_threshold_default_supports_pbs_layout() {
    with_var_unset(rustfs_config::ENV_SCANNER_ALERT_EXCESS_FOLDERS, || {
        crate::runtime_config::refresh_scanner_runtime_config_for_tests();
        assert_eq!(scanner_excess_folders_threshold(), 65_538);
    });
    crate::runtime_config::refresh_scanner_runtime_config_for_tests();
}

#[test]
fn test_scanner_yield_every_n_objects_uses_env() {
    with_var(rustfs_config::ENV_SCANNER_YIELD_EVERY_N_OBJECTS, Some("32"), || {
        crate::runtime_config::refresh_scanner_runtime_config_for_tests();
        assert_eq!(scanner_yield_every_n_objects(), 32);
    });
    crate::runtime_config::refresh_scanner_runtime_config_for_tests();
}

#[test]
fn test_scanner_yield_every_n_objects_uses_default() {
    with_var_unset(rustfs_config::ENV_SCANNER_YIELD_EVERY_N_OBJECTS, || {
        crate::runtime_config::refresh_scanner_runtime_config_for_tests();
        assert_eq!(scanner_yield_every_n_objects(), rustfs_config::DEFAULT_SCANNER_YIELD_EVERY_N_OBJECTS);
    });
    crate::runtime_config::refresh_scanner_runtime_config_for_tests();
}

#[test]
fn test_should_yield_after_object_respects_interval_and_disable() {
    assert!(!should_yield_after_object(127, 128));
    assert!(should_yield_after_object(128, 128));
    assert!(!should_yield_after_object(128, 0));
}

#[test]
fn test_checkpoint_reason_from_budget_maps_all_budget_reasons() {
    assert_eq!(
        checkpoint_reason_from_budget(Some(crate::scanner_budget::ScannerCycleBudgetReason::Runtime)),
        crate::data_usage_define::DataUsageScanCheckpointReason::Runtime
    );
    assert_eq!(
        checkpoint_reason_from_budget(Some(crate::scanner_budget::ScannerCycleBudgetReason::Objects)),
        crate::data_usage_define::DataUsageScanCheckpointReason::Objects
    );
    assert_eq!(
        checkpoint_reason_from_budget(Some(crate::scanner_budget::ScannerCycleBudgetReason::Directories)),
        crate::data_usage_define::DataUsageScanCheckpointReason::Directories
    );
    assert_eq!(
        checkpoint_reason_from_budget(None),
        crate::data_usage_define::DataUsageScanCheckpointReason::Unknown
    );
}

#[test]
fn test_order_folders_for_resume_rotates_after_exact_resume_hint() {
    let mut folders = vec![
        CachedFolder {
            name: "bucket/child-c".to_string(),
            parent: None,
            object_heal_prob_div: 1,
        },
        CachedFolder {
            name: "bucket/child-a".to_string(),
            parent: None,
            object_heal_prob_div: 1,
        },
        CachedFolder {
            name: "bucket/child-b".to_string(),
            parent: None,
            object_heal_prob_div: 1,
        },
    ];

    let outcome = order_folders_for_resume(&mut folders, Some("bucket/child-b"));

    let names = folders.into_iter().map(|folder| folder.name).collect::<Vec<_>>();
    assert_eq!(outcome, FolderResumeOrder::Used);
    assert_eq!(
        names,
        vec![
            "bucket/child-c".to_string(),
            "bucket/child-a".to_string(),
            "bucket/child-b".to_string()
        ]
    );
}

#[test]
fn test_order_folders_for_resume_prioritizes_descendant_resume_hint() {
    let mut folders = vec![
        CachedFolder {
            name: "bucket/child-c".to_string(),
            parent: None,
            object_heal_prob_div: 1,
        },
        CachedFolder {
            name: "bucket/child-a".to_string(),
            parent: None,
            object_heal_prob_div: 1,
        },
        CachedFolder {
            name: "bucket/child-b".to_string(),
            parent: None,
            object_heal_prob_div: 1,
        },
    ];

    let outcome = order_folders_for_resume(&mut folders, Some("bucket/child-b/grandchild"));

    let names = folders.into_iter().map(|folder| folder.name).collect::<Vec<_>>();
    assert_eq!(outcome, FolderResumeOrder::Used);
    assert_eq!(
        names,
        vec![
            "bucket/child-b".to_string(),
            "bucket/child-c".to_string(),
            "bucket/child-a".to_string()
        ]
    );
}

#[test]
fn test_order_folders_for_resume_reports_stale_hint() {
    let mut folders = vec![
        CachedFolder {
            name: "bucket/child-c".to_string(),
            parent: None,
            object_heal_prob_div: 1,
        },
        CachedFolder {
            name: "bucket/child-a".to_string(),
            parent: None,
            object_heal_prob_div: 1,
        },
    ];

    let outcome = order_folders_for_resume(&mut folders, Some("bucket/child-b"));

    let names = folders.into_iter().map(|folder| folder.name).collect::<Vec<_>>();
    assert_eq!(outcome, FolderResumeOrder::Stale);
    assert_eq!(names, vec!["bucket/child-a".to_string(), "bucket/child-c".to_string()]);
}

#[tokio::test]
async fn test_record_failed_prunes_to_max_entries() {
    let (mut scanner, temp_dir) = build_test_scanner().await;
    let _guard = TestGuard::new(1000, 2, &mut scanner, temp_dir);
    let now = FolderScanner::now_secs();

    scanner
        .new_cache
        .info
        .failed_objects
        .insert("old1".to_string(), now.saturating_sub(50));
    scanner
        .new_cache
        .info
        .failed_objects
        .insert("old2".to_string(), now.saturating_sub(40));
    scanner
        .new_cache
        .info
        .failed_objects
        .insert("old3".to_string(), now.saturating_sub(30));

    scanner.record_failed("new");

    assert_eq!(scanner.new_cache.info.failed_objects.len(), 2);
    assert!(scanner.new_cache.info.failed_objects.contains_key("new"));
    assert!(scanner.new_cache.info.failed_objects.contains_key("old3"));
    assert!(!scanner.new_cache.info.failed_objects.contains_key("old1"));
    assert!(!scanner.new_cache.info.failed_objects.contains_key("old2"));
}

#[tokio::test]
async fn test_prune_failed_objects_cache_drops_expired() {
    let (mut scanner, temp_dir) = build_test_scanner().await;
    let _guard = TestGuard::new(5, 10, &mut scanner, temp_dir);
    let now = FolderScanner::now_secs();

    scanner
        .new_cache
        .info
        .failed_objects
        .insert("expired".to_string(), now.saturating_sub(10));
    scanner
        .new_cache
        .info
        .failed_objects
        .insert("fresh".to_string(), now.saturating_sub(2));

    scanner.prune_failed_objects_cache();

    assert_eq!(scanner.new_cache.info.failed_objects.len(), 1);
    assert!(scanner.new_cache.info.failed_objects.contains_key("fresh"));
}

#[tokio::test]
async fn test_prune_failed_objects_max_zero_keeps_fresh() {
    let (mut scanner, temp_dir) = build_test_scanner().await;
    let _guard = TestGuard::new(60, 0, &mut scanner, temp_dir);
    let now = FolderScanner::now_secs();

    scanner
        .new_cache
        .info
        .failed_objects
        .insert("fresh1".to_string(), now.saturating_sub(5));
    scanner
        .new_cache
        .info
        .failed_objects
        .insert("fresh2".to_string(), now.saturating_sub(10));
    scanner
        .new_cache
        .info
        .failed_objects
        .insert("expired".to_string(), now.saturating_sub(120));

    scanner.prune_failed_objects_cache();

    assert_eq!(scanner.new_cache.info.failed_objects.len(), 2);
    assert!(scanner.new_cache.info.failed_objects.contains_key("fresh1"));
    assert!(scanner.new_cache.info.failed_objects.contains_key("fresh2"));
    assert!(!scanner.new_cache.info.failed_objects.contains_key("expired"));
}

#[test]
fn test_build_object_heal_request_omits_nil_version_id() {
    let request = build_object_heal_request(
        "bucket".to_string(),
        "path/to/object".to_string(),
        None,
        HealScanMode::Deep,
        HealChannelPriority::Low,
    );

    assert_eq!(request.bucket, "bucket");
    assert_eq!(request.object_prefix.as_deref(), Some("path/to/object"));
    assert!(request.object_version_id.is_none());
    assert_eq!(request.scan_mode, Some(HealScanMode::Deep));
    assert_eq!(request.priority, HealChannelPriority::Low);
    assert_eq!(request.source, HealRequestSource::Scanner);
    assert_eq!(request.remove_corrupted, Some(HEAL_DELETE_DANGLING));
    assert_eq!(request.recreate_missing, Some(false));
}

#[test]
fn test_build_non_destructive_object_heal_request_disables_removal() {
    let request = build_non_destructive_object_heal_request(
        "bucket".to_string(),
        "path/to/object".to_string(),
        HealScanMode::Deep,
        HealChannelPriority::High,
    );

    assert_eq!(request.object_version_id, None);
    assert_eq!(request.remove_corrupted, Some(false));
    assert_eq!(request.recreate_missing, Some(false));
    assert_eq!(request.source, HealRequestSource::Scanner);
}

#[test]
fn test_build_bucket_heal_request_disables_recreate_for_scanner() {
    let request = build_bucket_heal_request("bucket".to_string(), HealChannelPriority::Low);

    assert_eq!(request.bucket, "bucket");
    assert_eq!(request.priority, HealChannelPriority::Low);
    assert_eq!(request.source, HealRequestSource::Scanner);
    assert_eq!(request.recreate_missing, Some(false));
}

fn pending_heal(
    kind: PendingScannerHealKind,
    bucket: &str,
    object: Option<&str>,
    version_id: Option<&str>,
    last_attempt: u64,
    attempts: u32,
) -> PendingScannerHeal {
    PendingScannerHeal {
        kind,
        bucket: bucket.to_string(),
        object: object.map(ToOwned::to_owned),
        version_id: version_id.map(ToOwned::to_owned),
        scan_mode: HealScanMode::Deep,
        first_seen: FolderScanner::now_secs(),
        last_attempt,
        attempts,
        last_admission_result: "full".to_string(),
        last_admission_reason: "none".to_string(),
    }
}

/// The nil-UUID branch of the defensive-UUID invariant: a nil version in
/// a repaired notice means "no value" and must match unversioned ledger
/// entries only.
#[test]
fn test_mrf_repaired_version_id_maps_nil_to_none() {
    assert_eq!(mrf_repaired_version_id(None), None);
    assert_eq!(mrf_repaired_version_id(Some([0u8; 16])), None);
    let uuid = Uuid::new_v4();
    assert_eq!(mrf_repaired_version_id(Some(*uuid.as_bytes())), Some(uuid.to_string()));
}

/// Full wiring of backlog#1894 axis B: notes taken for the scanned bucket
/// clear exactly the matching Object ledger entries — bucket-level
/// entries, other buckets' entries, and version-mismatched entries
/// survive; a real (non-nil) version matches only the same version.
#[tokio::test]
async fn test_mrf_repaired_notices_clear_matching_ledger_entries() {
    use rustfs_common::mrf_channel::note_mrf_repaired;

    let (mut scanner, temp_dir) = build_test_scanner().await;
    let _guard = TestGuard::new(u64::MAX, usize::MAX, &mut scanner, temp_dir);
    scanner.new_cache.info.name = "bucket".to_string();
    scanner.update_cache.info.name = "bucket".to_string();
    scanner.heal_object_select = 1;

    let version = Uuid::new_v4().to_string();
    scanner.new_cache.info.pending_heals = vec![
        pending_heal(PendingScannerHealKind::Object, "bucket", Some("object-a"), None, 1, 1),
        pending_heal(PendingScannerHealKind::Object, "bucket", Some("object-b"), Some(&version), 1, 1),
        pending_heal(PendingScannerHealKind::Object, "bucket", Some("object-c"), None, 1, 1),
        pending_heal(
            PendingScannerHealKind::Object,
            "bucket",
            Some("object-c"),
            Some("00000000-0000-0000-0000-000000000001"),
            1,
            1,
        ),
        pending_heal(PendingScannerHealKind::Bucket, "bucket", None, None, 1, 1),
        pending_heal(PendingScannerHealKind::Object, "other-bucket", Some("object-a"), None, 1, 1),
    ];

    note_mrf_repaired("bucket", "object-a", None);
    note_mrf_repaired("bucket", "object-b", Some(*Uuid::parse_str(&version).unwrap().as_bytes()));
    // A nil-UUID notice for object-c means "no value": it clears the
    // unversioned entry but must not touch the versioned one.
    note_mrf_repaired("bucket", "object-c", Some([0u8; 16]));
    // A notice for a target the ledger does not track must be a no-op.
    note_mrf_repaired("bucket", "object-untracked", None);

    scanner
        .retry_pending_scanner_heals()
        .await
        .expect("retry pass should succeed");

    let survivors: Vec<(PendingScannerHealKind, &str, Option<&str>, Option<&str>)> = scanner
        .new_cache
        .info
        .pending_heals
        .iter()
        .map(|entry| (entry.kind, entry.bucket.as_str(), entry.object.as_deref(), entry.version_id.as_deref()))
        .collect();
    // Cleared: object-a (no version), object-b (exact version match), and
    // object-c's unversioned entry (the nil branch matched no-version
    // only — the versioned object-c entry survives).
    assert_eq!(
        survivors,
        vec![
            (
                PendingScannerHealKind::Object,
                "bucket",
                Some("object-c"),
                Some("00000000-0000-0000-0000-000000000001")
            ),
            (PendingScannerHealKind::Bucket, "bucket", None, None),
            (PendingScannerHealKind::Object, "other-bucket", Some("object-a"), None),
        ]
    );
}

#[test]
fn test_pending_heal_reconstructs_bucket_request() {
    let pending = pending_heal(PendingScannerHealKind::Bucket, "bucket", None, None, 1, 1);

    let request = build_pending_scanner_heal_request(&pending).expect("bucket request should rebuild");

    assert_eq!(request.bucket, "bucket");
    assert_eq!(request.priority, HealChannelPriority::High);
    assert_eq!(request.source, HealRequestSource::Scanner);
    assert_eq!(request.recreate_missing, Some(false));
    assert!(request.object_prefix.is_none());
}

#[test]
fn test_pending_heal_reconstructs_object_request_with_version() {
    let pending = pending_heal(PendingScannerHealKind::Object, "bucket", Some("path/to/object"), Some("version-a"), 1, 1);

    let request = build_pending_scanner_heal_request(&pending).expect("object request should rebuild");

    assert_eq!(request.bucket, "bucket");
    assert_eq!(request.object_prefix.as_deref(), Some("path/to/object"));
    assert_eq!(request.object_version_id.as_deref(), Some("version-a"));
    assert_eq!(request.scan_mode, Some(HealScanMode::Deep));
    assert_eq!(request.priority, HealChannelPriority::High);
    assert_eq!(request.source, HealRequestSource::Scanner);
}

#[test]
fn test_pending_heal_reconstructs_unversioned_request_without_removal() {
    let pending = pending_heal(PendingScannerHealKind::Object, "bucket", Some("object"), None, 1, 1);

    let request = build_pending_scanner_heal_request(&pending).expect("unversioned object request should rebuild");

    assert!(request.object_version_id.is_none());
    assert_eq!(request.remove_corrupted, Some(false));
    assert_eq!(request.recreate_missing, Some(false));
}

#[tokio::test]
async fn test_pending_heal_reason_preserves_sub_quorum_discovery() {
    let (mut scanner, temp_dir) = build_test_scanner().await;
    let _guard = TestGuard::new(u64::MAX, usize::MAX, &mut scanner, temp_dir);

    scanner.update_pending_scanner_heal_after_admission(
        PendingScannerHealKind::Object,
        "bucket",
        Some("object"),
        Some("version-a"),
        HealScanMode::Deep,
        HealAdmissionResult::Full,
    );
    scanner.mark_pending_scanner_heal_reason(
        PendingScannerHealKind::Object,
        "bucket",
        Some("object"),
        Some("version-a"),
        "sub_quorum_metadata",
    );

    assert_eq!(scanner.new_cache.info.pending_heals.len(), 1);
    assert_eq!(scanner.new_cache.info.pending_heals[0].last_admission_reason, "sub_quorum_metadata");
}

#[test]
fn test_pending_heal_retry_candidates_respect_cap_and_order() {
    let pending: Vec<PendingScannerHeal> = (0..(MAX_PENDING_SCANNER_HEAL_RETRIES_PER_BUCKET + 2))
        .map(|idx| {
            pending_heal(
                PendingScannerHealKind::Object,
                "bucket",
                Some(&format!("object-{idx:03}")),
                None,
                idx as u64,
                1,
            )
        })
        .collect();

    let candidates = pending_scanner_heal_retry_candidates(&pending, "bucket");

    assert_eq!(candidates.len(), MAX_PENDING_SCANNER_HEAL_RETRIES_PER_BUCKET);
    assert_eq!(candidates.first().and_then(|entry| entry.object.as_deref()), Some("object-000"));
    assert_eq!(candidates.last().and_then(|entry| entry.object.as_deref()), Some("object-127"));
}

#[tokio::test]
async fn test_pending_heal_prune_expires_stale_entries() {
    let (mut scanner, temp_dir) = build_test_scanner().await;
    let _guard = TestGuard::new(u64::MAX, usize::MAX, &mut scanner, temp_dir);
    scanner.new_cache.info.name = "bucket".to_string();
    scanner.update_cache.info.name = "bucket".to_string();

    let mut stale = pending_heal(PendingScannerHealKind::Object, "bucket", Some("stale"), None, 1, 1);
    stale.first_seen = FolderScanner::now_secs().saturating_sub(MAX_PENDING_SCANNER_HEAL_AGE_SECS + 1);
    let fresh = pending_heal(PendingScannerHealKind::Object, "bucket", Some("fresh"), None, 1, 1);
    scanner.new_cache.info.pending_heals = vec![stale, fresh];

    scanner.prune_pending_scanner_heals();

    assert_eq!(scanner.new_cache.info.pending_heals.len(), 1);
    assert_eq!(scanner.new_cache.info.pending_heals[0].object.as_deref(), Some("fresh"));
    assert_eq!(scanner.update_cache.info.pending_heals, scanner.new_cache.info.pending_heals);
    assert!(scanner.pending_heals_changed);
}

#[tokio::test]
async fn test_pending_heal_update_keeps_stale_entry_until_retry_prune() {
    let (mut scanner, temp_dir) = build_test_scanner().await;
    let _guard = TestGuard::new(u64::MAX, usize::MAX, &mut scanner, temp_dir);
    scanner.new_cache.info.name = "bucket".to_string();
    scanner.update_cache.info.name = "bucket".to_string();

    let mut stale = pending_heal(PendingScannerHealKind::Object, "bucket", Some("object"), None, 1, 1);
    stale.first_seen = FolderScanner::now_secs().saturating_sub(MAX_PENDING_SCANNER_HEAL_AGE_SECS + 1);
    scanner.new_cache.info.pending_heals = vec![stale];

    scanner.update_pending_scanner_heal_after_admission(
        PendingScannerHealKind::Object,
        "bucket",
        Some("object"),
        None,
        HealScanMode::Deep,
        HealAdmissionResult::Dropped(HealAdmissionDropReason::QueueFull),
    );

    assert_eq!(scanner.new_cache.info.pending_heals.len(), 1);
    assert_eq!(scanner.new_cache.info.pending_heals[0].attempts, 2);
    assert_eq!(scanner.new_cache.info.pending_heals[0].object.as_deref(), Some("object"));
    assert_eq!(scanner.update_cache.info.pending_heals, scanner.new_cache.info.pending_heals);
}

#[tokio::test]
async fn test_pending_heal_queue_full_deduplicates_object_entry() {
    let (mut scanner, temp_dir) = build_test_scanner().await;
    let _guard = TestGuard::new(u64::MAX, usize::MAX, &mut scanner, temp_dir);
    scanner.new_cache.info.name = "bucket".to_string();
    scanner.update_cache.info.name = "bucket".to_string();

    scanner.update_pending_scanner_heal_after_admission(
        PendingScannerHealKind::Object,
        "bucket",
        Some("object"),
        Some("version-a"),
        HealScanMode::Deep,
        HealAdmissionResult::Full,
    );
    scanner.update_pending_scanner_heal_after_admission(
        PendingScannerHealKind::Object,
        "bucket",
        Some("object"),
        Some("version-a"),
        HealScanMode::Deep,
        HealAdmissionResult::Dropped(HealAdmissionDropReason::QueueFull),
    );

    assert_eq!(scanner.new_cache.info.pending_heals.len(), 1);
    let pending = &scanner.new_cache.info.pending_heals[0];
    assert_eq!(pending.object.as_deref(), Some("object"));
    assert_eq!(pending.version_id.as_deref(), Some("version-a"));
    assert_eq!(pending.attempts, 2);
    assert_eq!(pending.last_admission_result, "dropped");
    assert_eq!(pending.last_admission_reason, "queue_full");
    assert_eq!(scanner.update_cache.info.pending_heals, scanner.new_cache.info.pending_heals);
    assert!(scanner.pending_heals_changed);
}

#[tokio::test]
async fn test_pending_heal_admitted_results_clear_matching_entry() {
    let (mut scanner, temp_dir) = build_test_scanner().await;
    let _guard = TestGuard::new(u64::MAX, usize::MAX, &mut scanner, temp_dir);

    scanner.update_pending_scanner_heal_after_admission(
        PendingScannerHealKind::Object,
        "bucket",
        Some("object"),
        None,
        HealScanMode::Normal,
        HealAdmissionResult::Full,
    );
    scanner.update_pending_scanner_heal_after_admission(
        PendingScannerHealKind::Object,
        "bucket",
        Some("object"),
        None,
        HealScanMode::Normal,
        HealAdmissionResult::Accepted,
    );

    assert!(scanner.new_cache.info.pending_heals.is_empty());

    scanner.update_pending_scanner_heal_after_admission(
        PendingScannerHealKind::Bucket,
        "bucket",
        None,
        None,
        HealScanMode::Normal,
        HealAdmissionResult::Full,
    );
    scanner.update_pending_scanner_heal_after_admission(
        PendingScannerHealKind::Bucket,
        "bucket",
        None,
        None,
        HealScanMode::Normal,
        HealAdmissionResult::Merged,
    );

    assert!(scanner.new_cache.info.pending_heals.is_empty());
}

#[tokio::test]
async fn test_pending_heal_policy_dropped_clears_without_creating_entry() {
    let (mut scanner, temp_dir) = build_test_scanner().await;
    let _guard = TestGuard::new(u64::MAX, usize::MAX, &mut scanner, temp_dir);

    scanner.update_pending_scanner_heal_after_admission(
        PendingScannerHealKind::Object,
        "bucket",
        Some("object"),
        None,
        HealScanMode::Normal,
        HealAdmissionResult::Dropped(HealAdmissionDropReason::PolicyDropped),
    );
    assert!(scanner.new_cache.info.pending_heals.is_empty());

    scanner.update_pending_scanner_heal_after_admission(
        PendingScannerHealKind::Object,
        "bucket",
        Some("object"),
        None,
        HealScanMode::Normal,
        HealAdmissionResult::Full,
    );
    scanner.update_pending_scanner_heal_after_admission(
        PendingScannerHealKind::Object,
        "bucket",
        Some("object"),
        None,
        HealScanMode::Normal,
        HealAdmissionResult::Dropped(HealAdmissionDropReason::PolicyDropped),
    );

    assert!(scanner.new_cache.info.pending_heals.is_empty());
}

#[test]
fn test_partial_cache_is_useful_when_pending_heals_changed() {
    let root = DataUsageEntry::default();

    assert!(!partial_cache_is_useful(&root, false));
    assert!(partial_cache_is_useful(&root, true));
}

fn metadata_for_object(bucket: &str, object: &str) -> Vec<u8> {
    let mut meta = FileMeta::new();
    meta.add_version(FileInfo {
        volume: bucket.to_string(),
        name: object.to_string(),
        mod_time: Some(OffsetDateTime::now_utc()),
        ..Default::default()
    })
    .expect("test metadata version should be accepted");
    meta.marshal_msg().expect("test metadata should marshal")
}

fn metadata_for_object_version(bucket: &str, object: &str, version_id: Option<Uuid>) -> Vec<u8> {
    let mut file_info = FileInfo::new(object, 4, 2);
    file_info.volume = bucket.to_string();
    file_info.name = object.to_string();
    file_info.version_id = version_id;
    file_info.versioned = version_id.is_some();
    file_info.mod_time = Some(OffsetDateTime::now_utc());
    file_info.size = 1;

    let mut meta = FileMeta::new();
    meta.add_version(file_info).expect("test metadata version should be accepted");
    meta.marshal_msg().expect("test metadata should marshal")
}

async fn write_test_object_metadata(root: &std::path::Path, bucket: &str, object: &str) {
    write_test_object_metadata_bytes(root, bucket, object, &metadata_for_object(bucket, object)).await;
}

async fn write_test_object_metadata_bytes(root: &std::path::Path, bucket: &str, object: &str, metadata: &[u8]) {
    let object_dir = root.join(bucket).join(object);
    tokio::fs::create_dir_all(&object_dir)
        .await
        .expect("failed to create test object directory");
    tokio::fs::write(object_dir.join("xl.meta"), metadata)
        .await
        .expect("failed to write test object metadata");
}

fn test_metadata_resolver(bucket: &str) -> MetadataResolutionParams {
    MetadataResolutionParams {
        bucket: bucket.to_string(),
        dir_quorum: 2,
        obj_quorum: 2,
        ..Default::default()
    }
}

#[test]
fn test_resolve_object_heal_entry_allows_plain_unresolved_fallback() {
    let entries = MetaCacheEntries(vec![Some(MetaCacheEntry {
        name: "object".to_string(),
        metadata: vec![1, 2, 3],
        ..Default::default()
    })]);

    let entry = resolve_object_heal_entry(&entries, test_metadata_resolver("bucket"))
        .expect("plain object fallback should be eligible for heal");

    assert_eq!(entry.name, "object");
}

#[test]
fn test_resolve_object_heal_entry_skips_unresolved_trailing_slash_fallback() {
    let entries = MetaCacheEntries(vec![Some(MetaCacheEntry {
        name: "object/".to_string(),
        metadata: vec![1, 2, 3],
        ..Default::default()
    })]);

    assert!(
        resolve_object_heal_entry(&entries, test_metadata_resolver("bucket")).is_none(),
        "unresolved trailing-slash fallback must not be submitted as an object heal"
    );
}

#[test]
fn test_resolve_object_heal_entry_skips_resolved_empty_directory_candidate() {
    let entries = MetaCacheEntries(vec![
        Some(MetaCacheEntry {
            name: "object/".to_string(),
            metadata: Vec::new(),
            ..Default::default()
        }),
        Some(MetaCacheEntry {
            name: "object/".to_string(),
            metadata: Vec::new(),
            ..Default::default()
        }),
    ]);

    assert!(
        resolve_object_heal_entry(&entries, test_metadata_resolver("bucket")).is_none(),
        "resolved empty directory candidates must not be submitted as object heals"
    );
}

#[test]
fn test_resolve_object_heal_entry_skips_only_empty_directory_fallback_candidates() {
    let entries = MetaCacheEntries(vec![
        Some(MetaCacheEntry {
            name: "object/".to_string(),
            metadata: Vec::new(),
            ..Default::default()
        }),
        Some(MetaCacheEntry {
            name: "prefix/".to_string(),
            metadata: Vec::new(),
            ..Default::default()
        }),
    ]);

    assert!(
        resolve_object_heal_entry(&entries, test_metadata_resolver("bucket")).is_none(),
        "unresolved fallback must ignore empty directory candidates"
    );
}

#[test]
fn test_resolve_object_heal_entry_uses_plain_fallback_after_trailing_slash() {
    let entries = MetaCacheEntries(vec![
        Some(MetaCacheEntry {
            name: "object/".to_string(),
            metadata: vec![1, 2, 3],
            ..Default::default()
        }),
        Some(MetaCacheEntry {
            name: "object".to_string(),
            metadata: vec![1, 2, 3],
            ..Default::default()
        }),
    ]);

    let entry = resolve_object_heal_entry(&entries, test_metadata_resolver("bucket"))
        .expect("plain object fallback should remain eligible after a trailing-slash candidate");

    assert_eq!(entry.name, "object");
}

#[test]
fn test_resolve_object_heal_entry_uses_plain_fallback_after_empty_directory_candidate() {
    let entries = MetaCacheEntries(vec![
        Some(MetaCacheEntry {
            name: "object/".to_string(),
            metadata: Vec::new(),
            ..Default::default()
        }),
        Some(MetaCacheEntry {
            name: "object".to_string(),
            metadata: vec![1, 2, 3],
            ..Default::default()
        }),
    ]);

    let entry = resolve_object_heal_entry(&entries, test_metadata_resolver("bucket"))
        .expect("plain object fallback should remain eligible after an empty directory candidate");

    assert_eq!(entry.name, "object");
}

#[test]
fn test_resolve_object_heal_entry_preserves_resolved_trailing_slash_object() {
    let metadata = metadata_for_object("bucket", "object/");
    let entries = MetaCacheEntries(vec![
        Some(MetaCacheEntry {
            name: "object/".to_string(),
            metadata: metadata.clone(),
            ..Default::default()
        }),
        Some(MetaCacheEntry {
            name: "object/".to_string(),
            metadata,
            ..Default::default()
        }),
    ]);

    let entry = resolve_object_heal_entry(&entries, test_metadata_resolver("bucket"))
        .expect("resolved trailing-slash object should remain eligible");

    assert_eq!(entry.name, "object/");
    assert!(entry.is_object_dir());
}

#[test]
fn test_disk_errors_are_only_missing_paths_accepts_missing_mix() {
    let errs = vec![
        Some(DiskError::FileNotFound),
        None,
        Some(DiskError::FileVersionNotFound),
        Some(DiskError::VolumeNotFound),
    ];

    assert!(disk_errors_are_only_missing_paths(&errs));
}

#[test]
fn test_disk_errors_are_only_missing_paths_rejects_empty_or_actionable_errors() {
    assert!(!disk_errors_are_only_missing_paths(&[None, None]));
    assert!(!disk_errors_are_only_missing_paths(&[
        Some(DiskError::FileNotFound),
        Some(DiskError::Timeout),
    ]));
}

#[test]
fn test_effective_object_heal_scan_mode_keeps_normal_when_bitrot_disabled() {
    let now = OffsetDateTime::now_utc();
    assert_eq!(effective_object_heal_scan_mode(false, Some(now), now), HealScanMode::Normal);
}

#[test]
fn test_effective_object_heal_scan_mode_downgrades_recent_object_to_normal() {
    let now = OffsetDateTime::now_utc();
    let recent = now - time::Duration::seconds(5);
    assert_eq!(effective_object_heal_scan_mode(true, Some(recent), now), HealScanMode::Normal);
}

#[test]
fn test_effective_object_heal_scan_mode_keeps_old_object_deep() {
    let now = OffsetDateTime::now_utc();
    let old = now - time::Duration::seconds((DEFAULT_SCANNER_DEEP_VERIFY_COOLDOWN_SECS as i64) + 5);
    assert_eq!(effective_object_heal_scan_mode(true, Some(old), now), HealScanMode::Deep);
}

#[test]
fn test_heal_priority_label_matches_priority_names() {
    assert_eq!(heal_priority_label(HealChannelPriority::Low), "low");
    assert_eq!(heal_priority_label(HealChannelPriority::Normal), "normal");
    assert_eq!(heal_priority_label(HealChannelPriority::High), "high");
    assert_eq!(heal_priority_label(HealChannelPriority::Critical), "critical");
}

#[test]
fn test_describe_heal_admission_formats_unadmitted_results() {
    assert_eq!(describe_heal_admission(HealAdmissionResult::Accepted), "accepted");
    assert_eq!(describe_heal_admission(HealAdmissionResult::Merged), "merged");
    assert_eq!(describe_heal_admission(HealAdmissionResult::Full), "queue_full");
    assert_eq!(
        describe_heal_admission(HealAdmissionResult::Dropped(
            rustfs_common::heal_channel::HealAdmissionDropReason::QueueFull
        )),
        "dropped:queue_full"
    );
}

#[tokio::test]
async fn scanner_trace_helpers_emit_expected_events() {
    let mut trace = rustfs_common::trace_bus::subscribe_trace_events();

    emit_scanner_folder_trace(
        "/tmp/rustfs-scanner-trace",
        "/tmp/rustfs-scanner-trace/bucket-a/folder-a",
        7,
        Some(Instant::now()),
        "completed",
    );
    let folder = recv_scanner_trace_event(
        &mut trace,
        TraceFunc::ScannerFolder,
        Some("bucket-a"),
        Some("folder-a"),
        Some("completed"),
    )
    .await;
    assert_eq!(trace_attr_string(&folder, "objects").as_deref(), Some("7"));

    emit_scanner_ilm_action_trace("bucket-a", "object-a", IlmAction::DeleteAction, 2, true, Some(Instant::now()));
    let ilm = recv_scanner_trace_event(
        &mut trace,
        TraceFunc::ScannerIlmAction,
        Some("bucket-a"),
        Some("object-a"),
        Some("queued"),
    )
    .await;
    assert_eq!(trace_attr_string(&ilm, "action").as_deref(), Some("delete"));
    assert_eq!(trace_attr_string(&ilm, "count").as_deref(), Some("2"));
    assert_eq!(trace_attr_string(&ilm, "queued").as_deref(), Some("true"));

    emit_scanner_heal_candidate_trace(ScannerHealCandidateTrace {
        candidate_type: "object",
        bucket: "bucket-a",
        object: Some("object-a"),
        version_id: Some("version-a"),
        priority: HealChannelPriority::High,
        scan_mode: Some(HealScanMode::Deep),
        result: Ok(HealAdmissionResult::Merged),
        started_at: Instant::now(),
    });
    let heal_candidate = recv_scanner_trace_event(
        &mut trace,
        TraceFunc::ScannerHealCandidate,
        Some("bucket-a"),
        Some("object-a"),
        Some("admitted"),
    )
    .await;
    assert_eq!(trace_attr_string(&heal_candidate, "candidate_type").as_deref(), Some("object"));
    assert_eq!(trace_attr_string(&heal_candidate, "priority").as_deref(), Some("high"));
    assert_eq!(trace_attr_string(&heal_candidate, "scan_mode").as_deref(), Some("deep"));
    assert_eq!(trace_attr_string(&heal_candidate, "version_id").as_deref(), Some("version-a"));
    assert_eq!(trace_attr_string(&heal_candidate, "admission").as_deref(), Some("merged"));
}

async fn recv_scanner_trace_event(
    trace: &mut rustfs_common::trace_bus::TraceSubscription,
    func: TraceFunc,
    bucket: Option<&str>,
    object: Option<&str>,
    state: Option<&str>,
) -> TraceEvent {
    for _ in 0..32 {
        let event = tokio::time::timeout(Duration::from_secs(1), trace.recv())
            .await
            .expect("scanner trace event should arrive")
            .expect("trace bus should stay open");
        if event.kind == TraceKind::Scanner
            && event.func == func
            && event.bucket.as_deref() == bucket
            && event.object.as_deref() == object
            && state.is_none_or(|state| trace_attr_string(&event, "state").as_deref() == Some(state))
        {
            return (*event).clone();
        }
    }

    panic!("expected scanner trace event {func:?} for bucket {bucket:?} object {object:?}");
}

fn trace_attr_string(event: &TraceEvent, key: &str) -> Option<String> {
    event.attrs.iter().find_map(|attr| {
        if attr.key != key {
            return None;
        }
        Some(match &attr.value {
            rustfs_common::trace_bus::TraceVal::Bool(value) => value.to_string(),
            rustfs_common::trace_bus::TraceVal::U64(value) => value.to_string(),
            rustfs_common::trace_bus::TraceVal::I64(value) => value.to_string(),
            rustfs_common::trace_bus::TraceVal::Str(value) => value.to_string(),
        })
    })
}

#[test]
fn test_build_high_priority_heal_admission_error_contains_context() {
    let err = build_high_priority_heal_admission_error(
        "object",
        "bucket-a",
        Some("path/to/object"),
        HealChannelPriority::High,
        HealAdmissionResult::Full,
    );

    let err_text = err.to_string();
    assert!(err_text.contains("type=object"));
    assert!(err_text.contains("bucket='bucket-a'"));
    assert!(err_text.contains("object='path/to/object'"));
    assert!(err_text.contains("priority=high"));
    assert!(err_text.contains("admission=queue_full"));
}

#[tokio::test]
async fn test_heal_actions_returns_actual_size_without_inline_heal() {
    let temp_dir = std::env::temp_dir();
    let file_type = std::fs::metadata(&temp_dir).unwrap().file_type();

    let mut item = ScannerItem {
        path: temp_dir.join("object").to_string_lossy().to_string(),
        bucket: "bucket".to_string(),
        prefix: "".to_string(),
        object_name: "object".to_string(),
        file_type,
        lifecycle: None,
        object_lock: None,
        replication: None,
        heal_enabled: true,
        heal_bitrot: true,
        debug: false,
    };
    let object_info = ObjectInfo {
        bucket: "bucket".to_string(),
        name: "object".to_string(),
        ..Default::default()
    };
    let mut size_summary = SizeSummary::default();

    let size = item.heal_actions(&object_info, 123, &mut size_summary).await;
    assert_eq!(size, 123);
}

#[tokio::test]
#[cfg(unix)]
async fn test_scan_folder_skips_unreadable_child_directory() {
    let (mut scanner, temp_dir) = build_test_scanner().await;
    let _guard = TestGuard::new(60, 0, &mut scanner, temp_dir.clone());

    let bucket_dir = temp_dir.join("bucket");
    let good_dir = bucket_dir.join("good");
    let bad_dir = bucket_dir.join("bad");

    std::fs::create_dir_all(&good_dir).expect("failed to create good dir");
    std::fs::create_dir_all(&bad_dir).expect("failed to create bad dir");
    std::fs::set_permissions(&bad_dir, std::fs::Permissions::from_mode(0o000)).expect("failed to remove bad dir permissions");

    scanner.old_cache.info.name = "bucket".to_string();
    scanner.new_cache.info.name = "bucket".to_string();
    scanner.update_cache.info.name = "bucket".to_string();

    let folder = CachedFolder {
        name: "bucket".to_string(),
        parent: None,
        object_heal_prob_div: 1,
    };

    let mut into = DataUsageEntry::default();
    let result = scanner.scan_folder(CancellationToken::new(), folder, &mut into).await;

    std::fs::set_permissions(&bad_dir, std::fs::Permissions::from_mode(0o755)).expect("failed to restore bad dir permissions");

    assert!(result.is_ok(), "expected unreadable child directory to be skipped");
}

#[tokio::test]
async fn test_scan_folder_exits_when_abandoned_child_listing_finishes() {
    let (mut scanner, temp_dir) = build_test_scanner().await;
    let _guard = TestGuard::new(60, 100, &mut scanner, temp_dir.clone());
    let heal_starts = Arc::new(AtomicUsize::new(0));
    let heal_starts_clone = heal_starts.clone();
    let healed_versions = Arc::new(Mutex::new(Vec::<Option<String>>::new()));
    let healed_versions_clone = healed_versions.clone();
    let mut heal_rx =
        rustfs_common::heal_channel::init_heal_channel().expect("heal channel should initialize once for scanner tests");
    let _heal_responder = tokio::spawn(async move {
        while let Some(command) = heal_rx.recv().await {
            if let rustfs_common::heal_channel::HealChannelCommand::Start {
                request, response_tx, ..
            } = command
            {
                heal_starts_clone.fetch_add(1, Ordering::Relaxed);
                healed_versions_clone
                    .lock()
                    .expect("heal version capture lock should not be poisoned")
                    .push(request.object_version_id);
                let _ = response_tx.send(Ok(HealAdmissionResult::Accepted));
            }
        }
    });

    let bucket = "src-archive";
    let object = "snapshots/37b3f20d941e2f5e6d99114d9bb2f3e67a8a2e5c9c4c5a1b0d6e7f8091a2b3c4";
    let orphan_version = Uuid::from_u128(0x1934);
    let shared_version = Uuid::from_u128(0x1935);
    let orphan_metadata = metadata_for_object_version(bucket, object, Some(orphan_version));
    let shared_metadata = metadata_for_object_version(bucket, object, Some(shared_version));
    write_test_object_metadata_bytes(&temp_dir, bucket, object, &orphan_metadata).await;
    let mut expected_metadata = vec![(temp_dir.join(bucket).join(object).join("xl.meta"), orphan_metadata.clone())];

    let mut disks = vec![scanner.local_disk.clone()];
    for disk_name in ["disk2", "disk3", "disk4"] {
        let disk_root = temp_dir.join(disk_name);
        write_test_object_metadata_bytes(&disk_root, bucket, object, &shared_metadata).await;
        expected_metadata.push((disk_root.join(bucket).join(object).join("xl.meta"), shared_metadata.clone()));
        let endpoint = Endpoint::try_from(disk_root.to_string_lossy().as_ref()).expect("failed to create extra disk endpoint");
        let disk = new_disk(
            &endpoint,
            &DiskOption {
                cleanup: false,
                health_check: false,
            },
        )
        .await
        .expect("failed to create extra disk");
        disks.push(disk);
    }

    scanner.heal_object_select = 1;
    scanner.disks = disks;
    scanner.disks_quorum = 2;
    let (options_tx, mut options_rx) = mpsc::unbounded_channel();
    scanner.list_path_raw_options_observer = Some(options_tx);
    scanner.old_cache.replace(
        &format!("{bucket}/{object}"),
        bucket,
        DataUsageEntry {
            objects: 1,
            ..Default::default()
        },
    );

    let mut into = DataUsageEntry::default();
    let folder = CachedFolder {
        name: bucket.to_string(),
        parent: None,
        object_heal_prob_div: 1,
    };

    tokio::time::timeout(Duration::from_secs(2), scanner.scan_folder(CancellationToken::new(), folder, &mut into))
        .await
        .expect("scan_folder should not hang after list_path_raw finishes")
        .expect("scan_folder should finish successfully");

    let observed_options = tokio::time::timeout(Duration::from_secs(1), options_rx.recv())
        .await
        .expect("abandoned-child listing options should be observed promptly")
        .expect("abandoned-child listing options channel should remain open");
    assert_eq!(observed_options, (true, None, Some(SCANNER_LIST_PATH_RAW_STALL_TIMEOUT)));
    let root = scanner
        .new_cache
        .checked_flatten(bucket)
        .expect("healed cache must contain canonical child links");
    // The fixture intentionally exposes two divergent version histories, so
    // the scanner keeps both logical versions visible while discovering heals.
    assert_eq!(root.objects, 2);
    assert!(heal_starts.load(Ordering::Relaxed) > 0, "test must execute the heal child-link path");
    let orphan_version_text = orphan_version.to_string();
    assert!(
        healed_versions
            .lock()
            .expect("heal version capture lock should not be poisoned")
            .iter()
            .any(|version| version.as_deref() == Some(orphan_version_text.as_str())),
        "sub-quorum orphan version must be submitted as an exact heal candidate"
    );
    for (path, expected) in expected_metadata {
        assert_eq!(
            tokio::fs::read(&path)
                .await
                .expect("scanner discovery must not delete metadata"),
            expected,
            "scanner discovery must not modify candidate metadata: {}",
            path.display()
        );
    }
}

#[tokio::test]
async fn test_scan_folder_xl_meta_named_directory_uses_namespace_descent() {
    let (mut scanner, temp_dir) = build_test_scanner().await;
    let _guard = TestGuard::new(60, 100, &mut scanner, temp_dir.clone());

    let bucket_dir = temp_dir.join("bucket");
    tokio::fs::create_dir_all(bucket_dir.join(STORAGE_FORMAT_FILE))
        .await
        .expect("failed to create xl.meta namespace directory");

    scanner.old_cache.info.name = "bucket".to_string();
    scanner.new_cache.info.name = "bucket".to_string();
    scanner.update_cache.info.name = "bucket".to_string();
    scanner.is_erasure_mode = true;

    let parent = CancellationToken::new();
    let budget = ScannerCycleBudget::new_with_progress_tracking(
        &parent,
        crate::scanner_budget::ScannerCycleBudgetConfig {
            max_directories: Some(1),
            ..Default::default()
        },
    );
    let ctx = budget.token();
    scanner.budget = budget.clone();

    let folder = CachedFolder {
        name: "bucket".to_string(),
        parent: None,
        object_heal_prob_div: 1,
    };

    let mut into = DataUsageEntry::default();
    let result = scanner.scan_folder(ctx, folder, &mut into).await;

    assert!(
        result.is_err(),
        "an xl.meta namespace directory must be traversed instead of treated as object metadata"
    );
    assert!(budget.budget_elapsed());
    assert_eq!(budget.reason(), Some(crate::scanner_budget::ScannerCycleBudgetReason::Directories));
    assert!(budget.token().is_cancelled());
    assert!(budget.entries_visited() >= 1);
}

#[tokio::test(flavor = "current_thread")]
async fn test_scan_folder_corrupt_xl_meta_stops_erasure_data_dir_descent() {
    let logs = CapturedLogs::default();
    let subscriber = tracing_subscriber::fmt()
        .json()
        .with_max_level(tracing::Level::ERROR)
        .with_writer(logs.clone())
        .with_ansi(false)
        .without_time()
        .finish();
    let _subscriber_guard = tracing::subscriber::set_default(subscriber);

    let (mut scanner, temp_dir) = build_test_scanner().await;
    // Canonicalize for the "drive" field comparison (scanner resolves symlinks).
    let canonical_temp_dir = std::fs::canonicalize(&temp_dir).unwrap_or_else(|_| temp_dir.clone());
    let _guard = TestGuard::new(60, 100, &mut scanner, temp_dir.clone());

    let object_dir = temp_dir.join("bucket").join("object");
    let data_dir = object_dir.join(Uuid::new_v4().to_string());
    tokio::fs::create_dir_all(&data_dir)
        .await
        .expect("failed to create erasure data directory");
    let metadata_path = object_dir.join(STORAGE_FORMAT_FILE);
    tokio::fs::write(&metadata_path, b"")
        .await
        .expect("failed to create corrupt object metadata");

    scanner.old_cache.info.name = "bucket".to_string();
    scanner.new_cache.info.name = "bucket".to_string();
    scanner.update_cache.info.name = "bucket".to_string();
    scanner.is_erasure_mode = true;

    let parent = CancellationToken::new();
    let budget = ScannerCycleBudget::new_with_progress_tracking(
        &parent,
        crate::scanner_budget::ScannerCycleBudgetConfig {
            max_directories: Some(1),
            ..Default::default()
        },
    );
    scanner.budget = budget.clone();

    let folder = CachedFolder {
        name: "bucket/object".to_string(),
        parent: None,
        object_heal_prob_div: 1,
    };
    let mut into = DataUsageEntry::default();

    scanner
        .scan_folder(budget.token(), folder, &mut into)
        .await
        .expect("failed metadata must not make scanner descend into erasure data directories");

    assert_eq!(into.failed_objects, 1);
    assert!(
        scanner
            .new_cache
            .info
            .failed_objects
            .contains_key(metadata_path.to_string_lossy().as_ref())
    );
    assert!(!budget.budget_elapsed());
    assert_eq!(budget.reason(), None);

    let captured = logs.contents();
    assert!(
        !captured.contains("failed to check XL2 v1 format"),
        "the context-free filemeta parser error must not be emitted"
    );
    let events = captured
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).expect("captured scanner log should be valid JSON"))
        .filter(|line| line["fields"]["event"] == EVENT_SCANNER_METADATA_CORRUPT)
        .collect::<Vec<_>>();
    assert_eq!(
        events.len(),
        1,
        "one corrupt metadata observation must emit one scanner-owned diagnostic event"
    );
    let fields = &events[0]["fields"];
    assert_eq!(fields["component"], LOG_COMPONENT_SCANNER);
    assert_eq!(fields["subsystem"], LOG_SUBSYSTEM_FOLDER);
    assert_eq!(fields["drive"], canonical_temp_dir.to_string_lossy().as_ref());
    assert_eq!(fields["bucket"], "bucket");
    assert_eq!(fields["object"], "object");
    assert_eq!(fields["metadata_path"], metadata_path.to_string_lossy().as_ref());
    assert_eq!(fields["state"], "metadata_corrupt");

    let retry_budget = ScannerCycleBudget::new_with_progress_tracking(
        &parent,
        crate::scanner_budget::ScannerCycleBudgetConfig {
            max_directories: Some(1),
            ..Default::default()
        },
    );
    scanner.budget = retry_budget.clone();
    let retry_folder = CachedFolder {
        name: "bucket/object".to_string(),
        parent: None,
        object_heal_prob_div: 1,
    };
    let mut retry_into = DataUsageEntry::default();

    scanner
        .scan_folder(retry_budget.token(), retry_folder, &mut retry_into)
        .await
        .expect("cached metadata failure must still stop erasure data directory descent");

    assert_eq!(retry_into.failed_objects, 0, "cached failure should not be counted twice");
    assert!(!retry_budget.budget_elapsed());
    assert_eq!(retry_budget.reason(), None);

    #[cfg(unix)]
    {
        tokio::fs::remove_file(&metadata_path)
            .await
            .expect("failed to remove corrupt object metadata");
        tokio::fs::write(data_dir.join("part.1"), b"shard")
            .await
            .expect("failed to create erasure shard");
        scanner.new_cache.info.failed_objects.clear();
        let metadata_target = temp_dir.join("metadata-target");
        tokio::fs::create_dir(&metadata_target)
            .await
            .expect("failed to create metadata symlink target");
        std::os::unix::fs::symlink(&metadata_target, &metadata_path).expect("failed to create metadata directory symlink");

        let symlink_budget = ScannerCycleBudget::new_with_progress_tracking(
            &parent,
            crate::scanner_budget::ScannerCycleBudgetConfig {
                max_directories: Some(1),
                ..Default::default()
            },
        );
        scanner.budget = symlink_budget.clone();
        let symlink_folder = CachedFolder {
            name: "bucket/object".to_string(),
            parent: None,
            object_heal_prob_div: 1,
        };
        let mut symlink_into = DataUsageEntry::default();

        scanner
            .scan_folder(symlink_budget.token(), symlink_folder, &mut symlink_into)
            .await
            .expect("metadata symlink must still stop erasure data directory descent");

        assert_eq!(symlink_into.failed_objects, 1);
        assert!(
            scanner
                .new_cache
                .info
                .failed_objects
                .contains_key(metadata_path.to_string_lossy().as_ref())
        );
        assert!(!symlink_budget.budget_elapsed());
        assert_eq!(symlink_budget.reason(), None);
    }
}

#[tokio::test]
async fn test_scan_folder_missing_xl_meta_stops_erasure_data_dir_descent() {
    let (mut scanner, temp_dir) = build_test_scanner().await;
    let _guard = TestGuard::new(60, 100, &mut scanner, temp_dir.clone());

    let object_dir = temp_dir.join("bucket").join("object");
    let data_dir = object_dir.join(Uuid::new_v4().to_string());
    tokio::fs::create_dir_all(&data_dir)
        .await
        .expect("failed to create erasure data directory");
    tokio::fs::write(data_dir.join("part.1"), b"shard")
        .await
        .expect("failed to create erasure shard");
    let metadata_path = object_dir.join(STORAGE_FORMAT_FILE);

    scanner.old_cache.info.name = "bucket".to_string();
    scanner.new_cache.info.name = "bucket".to_string();
    scanner.update_cache.info.name = "bucket".to_string();
    scanner.is_erasure_mode = true;

    let parent = CancellationToken::new();
    let budget = ScannerCycleBudget::new_with_progress_tracking(
        &parent,
        crate::scanner_budget::ScannerCycleBudgetConfig {
            max_directories: Some(1),
            ..Default::default()
        },
    );
    scanner.budget = budget.clone();
    let folder = CachedFolder {
        name: "bucket/object".to_string(),
        parent: None,
        object_heal_prob_div: 1,
    };
    let mut into = DataUsageEntry::default();

    scanner
        .scan_folder(budget.token(), folder, &mut into)
        .await
        .expect("missing metadata must not make scanner descend into erasure data directories");

    assert_eq!(into.failed_objects, 1);
    assert!(
        scanner
            .new_cache
            .info
            .failed_objects
            .contains_key(metadata_path.to_string_lossy().as_ref())
    );
    assert!(!budget.budget_elapsed());
    assert_eq!(budget.reason(), None);

    let retry_budget = ScannerCycleBudget::new_with_progress_tracking(
        &parent,
        crate::scanner_budget::ScannerCycleBudgetConfig {
            max_directories: Some(1),
            ..Default::default()
        },
    );
    scanner.budget = retry_budget.clone();
    let retry_folder = CachedFolder {
        name: "bucket/object".to_string(),
        parent: None,
        object_heal_prob_div: 1,
    };
    let mut retry_into = DataUsageEntry::default();

    scanner
        .scan_folder(retry_budget.token(), retry_folder, &mut retry_into)
        .await
        .expect("cached missing metadata must still stop erasure data directory descent");

    assert_eq!(retry_into.failed_objects, 0, "cached failure should not be counted twice");
    assert!(!retry_budget.budget_elapsed());
    assert_eq!(retry_budget.reason(), None);
}

#[tokio::test]
async fn test_scan_folder_uuid_namespace_part_name_directory_is_not_data_dir() {
    let (mut scanner, temp_dir) = build_test_scanner().await;
    let _guard = TestGuard::new(60, 100, &mut scanner, temp_dir.clone());

    let namespace_name = Uuid::new_v4().to_string();
    let namespace = temp_dir.join("bucket").join(&namespace_name);
    tokio::fs::create_dir_all(namespace.join("part.1"))
        .await
        .expect("failed to create UUID namespace with part-like child directory");
    let nil_uuid_namespace = temp_dir.join("bucket").join(Uuid::nil().to_string());
    tokio::fs::create_dir_all(&nil_uuid_namespace)
        .await
        .expect("failed to create nil UUID namespace");
    tokio::fs::write(nil_uuid_namespace.join("part.1"), b"namespace object")
        .await
        .expect("failed to create object in nil UUID namespace");

    scanner.old_cache.info.name = "bucket".to_string();
    scanner.new_cache.info.name = "bucket".to_string();
    scanner.update_cache.info.name = "bucket".to_string();
    scanner.is_erasure_mode = true;
    let namespace_hash = hash_path(&format!("bucket/{namespace_name}"));
    scanner.old_cache.replace_hashed(
        &namespace_hash,
        &Some(hash_path("bucket")),
        &DataUsageEntry {
            objects: 1,
            size: 16,
            ..Default::default()
        },
    );

    let parent = CancellationToken::new();
    let budget = ScannerCycleBudget::new_with_progress_tracking(
        &parent,
        crate::scanner_budget::ScannerCycleBudgetConfig {
            max_directories: Some(1),
            ..Default::default()
        },
    );
    scanner.budget = budget.clone();
    let folder = CachedFolder {
        name: "bucket".to_string(),
        parent: None,
        object_heal_prob_div: 1,
    };
    let mut into = DataUsageEntry::default();

    let result = scanner.scan_folder(budget.token(), folder, &mut into).await;

    assert!(result.is_err(), "part.N directories and nil UUID namespaces must remain traversable");
    assert!(budget.budget_elapsed());
    assert_eq!(budget.reason(), Some(crate::scanner_budget::ScannerCycleBudgetReason::Directories));
    assert!(scanner.new_cache.info.failed_objects.is_empty());
    assert!(
        scanner.update_cache.find(&namespace_hash.key()).is_some(),
        "an existing UUID namespace must retain its cached subtree"
    );
}

#[tokio::test]
async fn test_scan_folder_non_erasure_metadata_keeps_namespace_descent() {
    let (mut scanner, temp_dir) = build_test_scanner().await;
    let _guard = TestGuard::new(60, 100, &mut scanner, temp_dir.clone());

    let folder_path = temp_dir.join("bucket").join("object");
    tokio::fs::create_dir_all(folder_path.join("child"))
        .await
        .expect("failed to create child namespace");
    tokio::fs::write(folder_path.join(STORAGE_FORMAT_FILE), b"")
        .await
        .expect("failed to create metadata-shaped file");

    scanner.old_cache.info.name = "bucket".to_string();
    scanner.new_cache.info.name = "bucket".to_string();
    scanner.update_cache.info.name = "bucket".to_string();
    scanner.is_erasure_mode = false;

    let parent = CancellationToken::new();
    let budget = ScannerCycleBudget::new_with_progress_tracking(
        &parent,
        crate::scanner_budget::ScannerCycleBudgetConfig {
            max_directories: Some(1),
            ..Default::default()
        },
    );
    scanner.budget = budget.clone();
    let folder = CachedFolder {
        name: "bucket/object".to_string(),
        parent: None,
        object_heal_prob_div: 1,
    };
    let mut into = DataUsageEntry::default();

    let result = scanner.scan_folder(budget.token(), folder, &mut into).await;

    assert!(result.is_err(), "non-erasure scans must not stop at metadata-shaped files");
    assert!(budget.budget_elapsed());
    assert_eq!(budget.reason(), Some(crate::scanner_budget::ScannerCycleBudgetReason::Directories));
}

#[tokio::test]
async fn test_scan_folder_compacted_parent_sends_partial_update() {
    let (mut scanner, temp_dir) = build_test_scanner().await;
    let _guard = TestGuard::new(60, 100, &mut scanner, temp_dir.clone());

    let bucket_dir = temp_dir.join("bucket");
    tokio::fs::create_dir_all(bucket_dir.join("child"))
        .await
        .expect("failed to create child directory");

    scanner.old_cache.info.name = "bucket".to_string();
    scanner.new_cache.info.name = "bucket".to_string();
    scanner.update_cache.info.name = "bucket".to_string();
    scanner.last_update = SystemTime::UNIX_EPOCH;

    let (tx, mut rx) = mpsc::channel(1);
    scanner.updates = Some(tx);

    let folder = CachedFolder {
        name: "bucket".to_string(),
        parent: None,
        object_heal_prob_div: 1,
    };
    let mut into = DataUsageEntry {
        compacted: true,
        ..Default::default()
    };

    scanner
        .scan_folder(CancellationToken::new(), folder, &mut into)
        .await
        .expect("compacted scan should finish successfully");

    let update = tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("compacted scan should send a partial update")
        .expect("partial update channel should remain open");

    assert!(update.compacted, "partial update should preserve compacted state");
}

#[tokio::test]
async fn test_scan_data_folder_cancelled_before_scan_clears_current_path() {
    let (scanner, temp_dir) = build_test_scanner().await;
    let _guard = TestGuard {
        temp_dir: Some(temp_dir),
    };
    let parent = CancellationToken::new();
    parent.cancel();
    let budget = ScannerCycleBudget::new(&parent, Default::default());
    let cache = DataUsageCache {
        info: crate::data_usage_define::DataUsageCacheInfo {
            name: "bucket".to_string(),
            ..Default::default()
        },
        ..Default::default()
    };
    let disk_path = scanner.local_disk.path().to_string_lossy().to_string();

    let result = scan_data_folder(
        budget.token(),
        budget,
        vec![scanner.local_disk.clone()],
        scanner.local_disk,
        cache,
        None,
        HealScanMode::Normal,
        SCANNER_SLEEPER.clone(),
    )
    .await;

    assert!(matches!(result, Err(ScannerError::Other(message)) if message == "Operation cancelled"));
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let report = global_metrics().report().await;
            if report.active_paths.iter().all(|path| !path.starts_with(&disk_path)) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("cancelled scan should deregister its active path");
}

#[tokio::test]
async fn test_scan_data_folder_returns_partial_cache_on_budget_cancel() {
    let (mut scanner, temp_dir) = build_test_scanner().await;
    let _guard = TestGuard::new(60, 100, &mut scanner, temp_dir.clone());

    let bucket_dir = temp_dir.join("bucket");
    tokio::fs::create_dir_all(bucket_dir.join("child-a"))
        .await
        .expect("failed to create first child directory");
    tokio::fs::create_dir_all(bucket_dir.join("child-b"))
        .await
        .expect("failed to create second child directory");

    let parent = CancellationToken::new();
    let budget = ScannerCycleBudget::new(
        &parent,
        crate::scanner_budget::ScannerCycleBudgetConfig {
            max_directories: Some(2),
            ..Default::default()
        },
    );
    let cache = DataUsageCache {
        info: crate::data_usage_define::DataUsageCacheInfo {
            name: "bucket".to_string(),
            next_cycle: 7,
            ..Default::default()
        },
        ..Default::default()
    };

    let result = scan_data_folder(
        budget.token(),
        budget.clone(),
        vec![scanner.local_disk.clone()],
        scanner.local_disk.clone(),
        cache,
        None,
        HealScanMode::Normal,
        SCANNER_SLEEPER.clone(),
    )
    .await;

    let partial_cache = match result {
        Err(ScannerError::PartialCache(partial_cache)) => partial_cache,
        other => panic!("expected partial cache after directory budget cancellation, got {other:?}"),
    };

    assert!(partial_cache.info.last_update.is_some());
    assert_eq!(partial_cache.info.next_cycle, 7);
    assert!(!partial_cache.info.snapshot_complete);
    assert!(partial_cache.root().is_some(), "partial cache should keep completed scan progress");
    assert!(budget.budget_elapsed());
    assert_eq!(budget.reason(), Some(crate::scanner_budget::ScannerCycleBudgetReason::Directories));
}

#[tokio::test]
async fn test_scan_data_folder_reports_invalid_checkpoint_ignored_once() {
    let (scanner, temp_dir) = build_test_scanner().await;
    let _guard = TestGuard {
        temp_dir: Some(temp_dir.clone()),
    };

    tokio::fs::create_dir_all(temp_dir.join("bucket").join("child-a").join("grandchild"))
        .await
        .expect("failed to create nested child directory");

    let before = global_metrics().report().await.scan_checkpoint_ignored;
    let parent = CancellationToken::new();
    let budget = ScannerCycleBudget::new(&parent, Default::default());
    let cache = DataUsageCache {
        info: crate::data_usage_define::DataUsageCacheInfo {
            name: "bucket".to_string(),
            scan_checkpoint: Some(crate::data_usage_define::DataUsageScanCheckpoint {
                version: crate::data_usage_define::DATA_USAGE_SCAN_CHECKPOINT_VERSION + 1,
                resume_after: "bucket/child-a".to_string(),
                reason: crate::data_usage_define::DataUsageScanCheckpointReason::Unknown,
            }),
            ..Default::default()
        },
        ..Default::default()
    };

    let result = scan_data_folder(
        budget.token(),
        budget.clone(),
        vec![scanner.local_disk.clone()],
        scanner.local_disk.clone(),
        cache,
        None,
        HealScanMode::Normal,
        SCANNER_SLEEPER.clone(),
    )
    .await;

    assert!(result.is_ok(), "scan should complete with an ignored checkpoint");
    let after = global_metrics().report().await.scan_checkpoint_ignored;
    assert_eq!(after.saturating_sub(before), 1);
}

#[tokio::test]
async fn test_scan_data_folder_resume_hint_prioritizes_next_existing_folder() {
    let (scanner, temp_dir) = build_test_scanner().await;
    let _guard = TestGuard {
        temp_dir: Some(temp_dir.clone()),
    };

    let bucket_dir = temp_dir.join("bucket");
    for child in ["child-a", "child-b", "child-c"] {
        tokio::fs::create_dir_all(bucket_dir.join(child))
            .await
            .expect("failed to create child directory");
    }

    let root_hash = hash_path("bucket");
    let mut cache = DataUsageCache {
        info: crate::data_usage_define::DataUsageCacheInfo {
            name: "bucket".to_string(),
            next_cycle: 9,
            scan_resume_after: Some("bucket/child-a".to_string()),
            ..Default::default()
        },
        ..Default::default()
    };
    cache.replace_hashed(&root_hash, &None, &DataUsageEntry::default());
    for child in ["child-a", "child-b", "child-c"] {
        cache.replace_hashed(
            &hash_path(&format!("bucket/{child}")),
            &Some(root_hash.clone()),
            &DataUsageEntry::default(),
        );
    }

    let parent = CancellationToken::new();
    let budget = ScannerCycleBudget::new(
        &parent,
        crate::scanner_budget::ScannerCycleBudgetConfig {
            max_directories: Some(2),
            ..Default::default()
        },
    );

    let result = scan_data_folder(
        budget.token(),
        budget.clone(),
        vec![scanner.local_disk.clone()],
        scanner.local_disk.clone(),
        cache,
        None,
        HealScanMode::Normal,
        SCANNER_SLEEPER.clone(),
    )
    .await;

    let partial_cache = match result {
        Err(ScannerError::PartialCache(partial_cache)) => partial_cache,
        other => panic!("expected partial cache after directory budget cancellation, got {other:?}"),
    };

    assert_eq!(partial_cache.info.scan_resume_after.as_deref(), Some("bucket/child-b"));
    let checkpoint = partial_cache.info.scan_checkpoint.as_ref().expect("partial scan checkpoint");
    assert_eq!(checkpoint.version, crate::data_usage_define::DATA_USAGE_SCAN_CHECKPOINT_VERSION);
    assert_eq!(checkpoint.resume_after, "bucket/child-b");
    assert_eq!(checkpoint.reason, crate::data_usage_define::DataUsageScanCheckpointReason::Directories);
    assert!(
        partial_cache
            .root()
            .is_some_and(|root| root.children.contains(&hash_path("bucket/child-b").key()))
    );
    assert_eq!(partial_cache.info.next_cycle, 9);
    assert_eq!(budget.reason(), Some(crate::scanner_budget::ScannerCycleBudgetReason::Directories));
}

#[tokio::test]
async fn scan_data_folder_missing_bucket_returns_partial() {
    let (scanner, temp_dir) = build_test_scanner().await;
    let _guard = TestGuard {
        temp_dir: Some(temp_dir),
    };
    let parent = CancellationToken::new();
    let budget = ScannerCycleBudget::new(&parent, Default::default());
    let mut cache = DataUsageCache {
        info: crate::data_usage_define::DataUsageCacheInfo {
            name: "missing-bucket".to_string(),
            next_cycle: 9,
            ..Default::default()
        },
        ..Default::default()
    };
    cache.replace(
        "missing-bucket",
        crate::data_usage_define::DATA_USAGE_ROOT,
        DataUsageEntry {
            objects: 7,
            size: 70,
            ..Default::default()
        },
    );

    let result = scan_data_folder(
        budget.token(),
        budget,
        vec![scanner.local_disk.clone()],
        scanner.local_disk,
        cache,
        None,
        HealScanMode::Normal,
        SCANNER_SLEEPER.clone(),
    )
    .await;

    let partial = match result {
        Err(ScannerError::NamespaceNotFoundCache(partial)) => partial,
        other => panic!("missing bucket should keep the scan incomplete, got {other:?}"),
    };
    assert!(!partial.info.snapshot_complete);
    assert_eq!(partial.info.next_cycle, 9);
    let root = partial
        .checked_flatten("missing-bucket")
        .expect("missing bucket partial must retain the last durable usage");
    assert_eq!(root.objects, 7);
    assert_eq!(root.size, 70);
}

#[tokio::test]
async fn scan_data_folder_missing_scan_root_returns_partial() {
    let (scanner, temp_dir) = build_test_scanner().await;
    tokio::fs::remove_dir_all(&temp_dir)
        .await
        .expect("failed to remove scanner root");
    let _guard = TestGuard {
        temp_dir: Some(temp_dir),
    };
    let parent = CancellationToken::new();
    let budget = ScannerCycleBudget::new(&parent, Default::default());
    let cache = DataUsageCache {
        info: crate::data_usage_define::DataUsageCacheInfo {
            name: "missing-bucket".to_string(),
            next_cycle: 9,
            ..Default::default()
        },
        ..Default::default()
    };

    let result = scan_data_folder(
        budget.token(),
        budget,
        vec![scanner.local_disk.clone()],
        scanner.local_disk,
        cache,
        None,
        HealScanMode::Normal,
        SCANNER_SLEEPER.clone(),
    )
    .await;

    let partial = match result {
        Err(ScannerError::NamespaceNotFoundCache(partial)) => partial,
        other => panic!("missing scan root should keep the scan incomplete, got {other:?}"),
    };
    assert!(!partial.info.snapshot_complete);
    assert_eq!(partial.info.next_cycle, 9);
    let root = partial
        .checked_flatten("missing-bucket")
        .expect("missing scan root partial should retain a non-publishable root");
    assert_eq!(root.objects, 0);
    assert_eq!(root.size, 0);
}

#[tokio::test]
async fn test_scan_data_folder_resume_hint_orders_across_new_and_existing_folders() {
    let (scanner, temp_dir) = build_test_scanner().await;
    let _guard = TestGuard {
        temp_dir: Some(temp_dir.clone()),
    };

    let bucket_dir = temp_dir.join("bucket");
    for child in ["child-a", "child-b", "child-c", "child-d"] {
        tokio::fs::create_dir_all(bucket_dir.join(child))
            .await
            .expect("failed to create child directory");
    }

    let root_hash = hash_path("bucket");
    let mut cache = DataUsageCache {
        info: crate::data_usage_define::DataUsageCacheInfo {
            name: "bucket".to_string(),
            next_cycle: 9,
            scan_resume_after: Some("bucket/child-b".to_string()),
            ..Default::default()
        },
        ..Default::default()
    };
    cache.replace_hashed(&root_hash, &None, &DataUsageEntry::default());
    for child in ["child-b", "child-c"] {
        cache.replace_hashed(
            &hash_path(&format!("bucket/{child}")),
            &Some(root_hash.clone()),
            &DataUsageEntry::default(),
        );
    }

    let parent = CancellationToken::new();
    let budget = ScannerCycleBudget::new(
        &parent,
        crate::scanner_budget::ScannerCycleBudgetConfig {
            max_directories: Some(2),
            ..Default::default()
        },
    );

    let result = scan_data_folder(
        budget.token(),
        budget.clone(),
        vec![scanner.local_disk.clone()],
        scanner.local_disk.clone(),
        cache,
        None,
        HealScanMode::Normal,
        SCANNER_SLEEPER.clone(),
    )
    .await;

    let partial_cache = match result {
        Err(ScannerError::PartialCache(partial_cache)) => partial_cache,
        other => panic!("expected partial cache after directory budget cancellation, got {other:?}"),
    };

    assert_eq!(partial_cache.info.scan_resume_after.as_deref(), Some("bucket/child-c"));
    assert!(
        partial_cache
            .root()
            .is_some_and(|root| root.children.contains(&hash_path("bucket/child-c").key()))
    );
    assert_eq!(budget.reason(), Some(crate::scanner_budget::ScannerCycleBudgetReason::Directories));
}

#[tokio::test]
async fn test_scan_data_folder_partial_object_budget_accumulates_progress() {
    let (scanner, temp_dir) = build_test_scanner().await;
    let _guard = TestGuard {
        temp_dir: Some(temp_dir.clone()),
    };

    for index in 0..5 {
        write_test_object_metadata(&temp_dir, "bucket", &format!("obj/{index:04}")).await;
    }

    let mut cache = DataUsageCache {
        info: crate::data_usage_define::DataUsageCacheInfo {
            name: "bucket".to_string(),
            next_cycle: 9,
            ..Default::default()
        },
        ..Default::default()
    };

    for expected_min_objects in [2, 4] {
        let parent = CancellationToken::new();
        let budget = ScannerCycleBudget::new(
            &parent,
            crate::scanner_budget::ScannerCycleBudgetConfig {
                max_objects: Some(2),
                ..Default::default()
            },
        );

        let result = scan_data_folder(
            budget.token(),
            budget.clone(),
            vec![scanner.local_disk.clone()],
            scanner.local_disk.clone(),
            cache,
            None,
            HealScanMode::Normal,
            SCANNER_SLEEPER.clone(),
        )
        .await;

        cache = match result {
            Err(ScannerError::PartialCache(partial_cache)) => *partial_cache,
            other => panic!("expected partial cache after object budget cancellation, got {other:?}"),
        };

        let root = cache
            .size_recursive("bucket")
            .expect("partial cache should retain bucket progress");
        assert!(
            root.objects >= expected_min_objects,
            "partial scan progress should accumulate across cycles; expected at least {expected_min_objects}, got {}",
            root.objects
        );
        assert_eq!(budget.reason(), Some(crate::scanner_budget::ScannerCycleBudgetReason::Objects));
    }

    let parent = CancellationToken::new();
    let budget = ScannerCycleBudget::new(&parent, Default::default());
    let result = scan_data_folder(
        budget.token(),
        budget,
        vec![scanner.local_disk.clone()],
        scanner.local_disk.clone(),
        cache,
        None,
        HealScanMode::Normal,
        SCANNER_SLEEPER.clone(),
    )
    .await
    .expect("unbounded scan should finish after partial progress");

    let root = result
        .checked_flatten("bucket")
        .expect("completed cache should retain bucket usage");
    assert_eq!(root.objects, 5);
    assert!(result.info.snapshot_complete);
    assert!(result.info.scan_resume_after.is_none());
    assert!(result.info.scan_checkpoint.is_none());
}

#[tokio::test]
async fn test_partial_compacted_entry_does_not_carry_children() {
    let (mut scanner, temp_dir) = build_test_scanner().await;
    let _guard = TestGuard {
        temp_dir: Some(temp_dir),
    };

    let child_hash = hash_path("bucket/child");
    let old_leaf_hash = hash_path("bucket/child/old");
    let mut old_child = DataUsageEntry::default();
    old_child.add_child(&old_leaf_hash);

    scanner
        .old_cache
        .replace_hashed(&child_hash, &Some(hash_path("bucket")), &old_child);
    scanner.old_cache.replace_hashed(
        &old_leaf_hash,
        &Some(child_hash.clone()),
        &DataUsageEntry {
            objects: 7,
            size: 7,
            ..Default::default()
        },
    );

    let mut compacted_partial = DataUsageEntry {
        objects: 2,
        size: 2,
        compacted: true,
        ..Default::default()
    };

    scanner.carry_forward_old_children(&child_hash, &mut compacted_partial);

    assert!(
        compacted_partial.children.is_empty(),
        "compacted entries already contain flattened totals and must not retain child entries"
    );
    assert_eq!(compacted_partial.objects, 2);
    assert!(
        scanner.new_cache.find(&old_leaf_hash.key()).is_none(),
        "carried children would be flattened again by size_recursive"
    );
}

#[tokio::test]
async fn test_partial_entry_does_not_carry_missing_old_child() {
    let (mut scanner, temp_dir) = build_test_scanner().await;
    let _guard = TestGuard {
        temp_dir: Some(temp_dir),
    };
    let root_hash = hash_path("bucket");
    scanner.old_cache.cache.insert(
        root_hash.key(),
        DataUsageEntry {
            children: HashSet::from([hash_path("bucket/missing").key()]),
            ..Default::default()
        },
    );

    let mut partial = DataUsageEntry {
        objects: 2,
        size: 2,
        ..Default::default()
    };
    scanner.carry_forward_old_children(&root_hash, &mut partial);
    scanner.new_cache.replace_hashed(&root_hash, &None, &partial);

    assert!(partial.children.is_empty());
    let flattened = scanner
        .new_cache
        .checked_flatten("bucket")
        .expect("a partial cache must not retain dangling child links");
    assert_eq!(flattened.objects, 2);
    assert_eq!(flattened.size, 2);
}

#[tokio::test]
async fn test_legacy_windows_cache_rebuilds_and_round_trips_portable_keys() {
    let (scanner, temp_dir) = build_test_scanner().await;
    let _guard = TestGuard {
        temp_dir: Some(temp_dir.clone()),
    };
    write_test_object_metadata(&temp_dir, "bucket", "prefix/object").await;

    let source = crate::data_usage_define::DataUsageCacheSource::new(0, 0);
    let scan_plan_digest = crate::data_usage_define::DataUsageScanPlanDigest([9; 32]);
    let mut legacy = DataUsageCache {
        info: crate::data_usage_define::DataUsageCacheInfo {
            name: "bucket".to_string(),
            next_cycle: 7,
            source: Some(source),
            scan_plan_digest: Some(scan_plan_digest),
            ..Default::default()
        },
        ..Default::default()
    };
    legacy.cache.insert(
        "bucket".to_string(),
        DataUsageEntry {
            children: HashSet::from(["bucket\\prefix".to_string()]),
            ..Default::default()
        },
    );
    legacy.cache.insert(
        "bucket\\prefix".to_string(),
        DataUsageEntry {
            objects: 1,
            ..Default::default()
        },
    );
    let encoded = legacy.marshal_msg().expect("legacy cache should serialize");
    let mut migrated = DataUsageCache::unmarshal(&encoded).expect("legacy cache should deserialize");
    assert_eq!(
        migrated.prepare_for_scan("bucket", 8, 0, source, scan_plan_digest, true),
        crate::data_usage_define::DataUsageCachePrepareOutcome::Reset
    );

    let parent = CancellationToken::new();
    let budget = ScannerCycleBudget::new(&parent, Default::default());
    let rebuilt = scan_data_folder(
        budget.token(),
        budget,
        vec![scanner.local_disk.clone()],
        scanner.local_disk,
        migrated,
        None,
        HealScanMode::Normal,
        SCANNER_SLEEPER.clone(),
    )
    .await
    .expect("portable cache rebuild should complete");
    let persisted = rebuilt.marshal_msg().expect("rebuilt cache should serialize");
    let decoded = DataUsageCache::unmarshal(&persisted).expect("rebuilt cache should deserialize");

    assert_eq!(decoded.info.cache_key_format, crate::data_usage_define::DATA_USAGE_CACHE_KEY_FORMAT);
    assert!(decoded.cache.keys().all(|key| !key.contains('\\')));
    let root = decoded
        .checked_flatten("bucket")
        .expect("rebuilt persisted cache should have a complete root");
    assert_eq!(root.objects, 1);
}

#[tokio::test]
async fn test_scan_data_folder_success_clears_resume_hint() {
    let (scanner, temp_dir) = build_test_scanner().await;
    let _guard = TestGuard {
        temp_dir: Some(temp_dir.clone()),
    };

    tokio::fs::create_dir_all(temp_dir.join("bucket").join("child-a"))
        .await
        .expect("failed to create child directory");

    let cache = DataUsageCache {
        info: crate::data_usage_define::DataUsageCacheInfo {
            name: "bucket".to_string(),
            next_cycle: 11,
            scan_resume_after: Some("bucket/child-a".to_string()),
            ..Default::default()
        },
        ..Default::default()
    };

    let parent = CancellationToken::new();
    let budget = ScannerCycleBudget::new(&parent, Default::default());

    let result = scan_data_folder(
        budget.token(),
        budget,
        vec![scanner.local_disk.clone()],
        scanner.local_disk.clone(),
        cache,
        None,
        HealScanMode::Normal,
        SCANNER_SLEEPER.clone(),
    )
    .await
    .expect("scan should complete successfully");

    assert!(result.info.scan_resume_after.is_none());
    assert!(result.info.scan_checkpoint.is_none());
    assert_eq!(result.info.next_cycle, 11);
}

#[tokio::test]
async fn test_scan_data_folder_keeps_unresolved_objects_partial() {
    let (scanner, temp_dir) = build_test_scanner().await;
    let _guard = TestGuard {
        temp_dir: Some(temp_dir.clone()),
    };
    write_test_object_metadata(&temp_dir, "bucket", "object").await;

    let failed_path = temp_dir
        .join("bucket")
        .join("object")
        .join(STORAGE_FORMAT_FILE)
        .to_string_lossy()
        .into_owned();
    let mut cache = DataUsageCache {
        info: crate::data_usage_define::DataUsageCacheInfo {
            name: "bucket".to_string(),
            next_cycle: 12,
            ..Default::default()
        },
        ..Default::default()
    };
    cache.info.failed_objects.insert(failed_path, FolderScanner::now_secs());

    let parent = CancellationToken::new();
    let budget = ScannerCycleBudget::new(&parent, Default::default());
    let result = scan_data_folder(
        budget.token(),
        budget,
        vec![scanner.local_disk.clone()],
        scanner.local_disk.clone(),
        cache,
        None,
        HealScanMode::Normal,
        SCANNER_SLEEPER.clone(),
    )
    .await;

    let partial = match result {
        Err(ScannerError::PartialCache(partial)) => partial,
        other => panic!("expected unresolved object to keep the cache partial, got {other:?}"),
    };
    assert!(!partial.info.snapshot_complete);
    assert!(!partial.info.failed_objects.is_empty());
}

#[tokio::test]
#[cfg(unix)]
async fn test_scan_folder_ignores_symlinked_child_directory() {
    let (mut scanner, temp_dir) = build_test_scanner().await;
    let _guard = TestGuard::new(60, 0, &mut scanner, temp_dir.clone());

    let bucket_dir = temp_dir.join("bucket");
    let target_dir = bucket_dir.join("target");
    let link_dir = bucket_dir.join("link");

    std::fs::create_dir_all(&target_dir).expect("failed to create target dir");
    symlink(&target_dir, &link_dir).expect("failed to create symlinked dir");

    scanner.old_cache.info.name = "bucket".to_string();
    scanner.new_cache.info.name = "bucket".to_string();
    scanner.update_cache.info.name = "bucket".to_string();

    let folder = CachedFolder {
        name: "bucket".to_string(),
        parent: None,
        object_heal_prob_div: 1,
    };

    let mut into = DataUsageEntry::default();
    let result = scanner.scan_folder(CancellationToken::new(), folder, &mut into).await;

    assert!(result.is_ok(), "expected symlinked child directory to be ignored");
    assert_eq!(into.failed_objects, 0, "expected ignored symlink not to count as a failed object");
}

#[test]
fn test_should_log_failed_object_samples_after_initial_limit() {
    assert!(should_log_failed_object(1));
    assert!(should_log_failed_object(SCANNER_FAILED_OBJECT_LOG_INITIAL_LIMIT));
    assert!(!should_log_failed_object(SCANNER_FAILED_OBJECT_LOG_INITIAL_LIMIT + 1));
    assert!(should_log_failed_object(SCANNER_FAILED_OBJECT_LOG_EVERY));
    assert!(!should_log_failed_object(SCANNER_FAILED_OBJECT_LOG_EVERY + 1));
    assert!(should_log_failed_object(SCANNER_FAILED_OBJECT_LOG_EVERY * 2));
}
