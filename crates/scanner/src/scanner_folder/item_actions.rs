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
/// Per-object scan actions: ScannerItem, the get-size failure policy, and the heal/ILM admission helpers.
use super::*;
#[cfg(test)]
use rustfs_filemeta::MetadataResolutionParams;
use sha2::{Digest as _, Sha256};

/// Cached folder information for scanning
#[derive(Clone, Debug)]
pub struct CachedFolder {
    pub name: String,
    pub parent: Option<DataUsageHash>,
    pub object_heal_prob_div: u32,
}

/// Type alias for get size function
pub type GetSizeFn = Box<dyn Fn(ScannerItem) -> Result<SizeSummary, StorageError> + Send + Sync>;

#[derive(Debug, PartialEq, Eq)]
pub(super) enum GetSizeFailureAction {
    Skip,
    RecordFailed,
    HealMetadata { object: String },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum SizeResolutionReason {
    CompressedSizeUnknown,
    InvalidPhysicalSize,
    UnsupportedCompression,
    InvalidObjectSize,
    InvalidPartSize,
    InvalidDeclaredSize,
    SizeOverflowOrMismatch,
}

impl SizeResolutionReason {
    fn as_str(self) -> &'static str {
        match self {
            Self::CompressedSizeUnknown => "compressed_size_unknown",
            Self::InvalidPhysicalSize => "invalid_physical_size",
            Self::UnsupportedCompression => "unsupported_compression",
            Self::InvalidObjectSize => "invalid_object_size",
            Self::InvalidPartSize => "invalid_part_size",
            Self::InvalidDeclaredSize => "invalid_declared_size",
            Self::SizeOverflowOrMismatch => "size_overflow_or_mismatch",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum SizeResolution {
    Known { logical: i64, physical: i64 },
    Unknown { physical: i64, reason: SizeResolutionReason },
    Corrupt { physical: i64, reason: SizeResolutionReason },
}

impl SizeResolution {
    fn known_size(&self) -> Option<i64> {
        match self {
            Self::Known { logical, .. } => Some(*logical),
            Self::Unknown { .. } | Self::Corrupt { .. } => None,
        }
    }
}

fn size_reconciliation_key(oi: &ObjectInfo, reason: SizeResolutionReason) -> String {
    let version = oi
        .version_id
        .filter(|version| !version.is_nil())
        .map(|version| version.to_string())
        .unwrap_or_default();
    let generation = oi
        .data_dir
        .filter(|generation| !generation.is_nil())
        .map(|generation| generation.to_string())
        .unwrap_or_default();
    // Length-prefix each component so an object key containing the separator
    // cannot alias another identity. S3 keys are bounded in normal operation;
    // oversized persisted values use a digest so a corrupt metadata record
    // cannot grow the ledger without bound.
    fn component(value: &str) -> String {
        const MAX_COMPONENT_LEN: usize = 512;
        if value.len() <= MAX_COMPONENT_LEN {
            return format!("{}:{}", value.len(), value);
        }
        let digest = Sha256::digest(value.as_bytes());
        let digest = hex_simd::encode_to_string(digest, hex_simd::AsciiCase::Lower);
        format!("hash:{}:{}", value.len(), digest)
    }
    format!(
        "{}|{}|{}|{}|{}",
        component(&oi.bucket),
        component(&oi.name),
        component(&version),
        component(&generation),
        component(reason.as_str())
    )
}

pub(super) fn bounded_reconciliation_field(value: &str) -> String {
    const MAX_FIELD_LEN: usize = 512;
    if value.len() <= MAX_FIELD_LEN {
        return value.to_string();
    }
    let digest = hex_simd::encode_to_string(Sha256::digest(value.as_bytes()), hex_simd::AsciiCase::Lower);
    let prefix_len = MAX_FIELD_LEN - 65;
    let prefix = value
        .char_indices()
        .take_while(|(offset, ch)| offset.saturating_add(ch.len_utf8()) <= prefix_len)
        .map(|(_, ch)| ch)
        .collect::<String>();
    format!("{}~{}", prefix, digest)
}

fn record_size_resolution(summary: &mut SizeSummary, oi: &ObjectInfo, resolution: &SizeResolution) {
    match resolution {
        SizeResolution::Known { .. } => {}
        SizeResolution::Unknown { physical, reason } | SizeResolution::Corrupt { physical, reason } => {
            summary.record_size_reconciliation(SizeReconciliationEntry {
                key: size_reconciliation_key(oi, *reason),
                bucket: bounded_reconciliation_field(&oi.bucket),
                object: bounded_reconciliation_field(&oi.name),
                version_id: oi
                    .version_id
                    .filter(|version| !version.is_nil())
                    .map(|version| version.to_string()),
                generation: oi
                    .data_dir
                    .filter(|generation| !generation.is_nil())
                    .map(|generation| generation.to_string()),
                reason: reason.as_str().to_string(),
                physical_size: u64::try_from(*physical).ok(),
                first_seen: 0,
                attempts: 0,
            });
        }
    }
}

/// Resolve the size metadata once at the scanner trust boundary. A compressed
/// -1 sentinel is valid legacy metadata, but it cannot participate in normal
/// logical-size accounting or size-filtered lifecycle rules.
pub(super) fn resolve_size(oi: &ObjectInfo) -> SizeResolution {
    let physical = oi.size;
    if physical < 0 {
        return SizeResolution::Corrupt {
            physical,
            reason: SizeResolutionReason::InvalidPhysicalSize,
        };
    }

    let compressed = match oi.compression_read_plan() {
        Ok((_, _, compressed)) => compressed,
        Err(_) => {
            return SizeResolution::Corrupt {
                physical,
                reason: SizeResolutionReason::UnsupportedCompression,
            };
        }
    };

    if oi.actual_size < -1 || (oi.actual_size == -1 && !compressed) {
        return SizeResolution::Corrupt {
            physical,
            reason: SizeResolutionReason::InvalidObjectSize,
        };
    }

    // Match ObjectInfo::get_actual_size: a positive in-memory value is the
    // authoritative decoded size. Stale declared/part metadata must not turn
    // an otherwise valid object into a false corruption report.
    if oi.actual_size > 0 {
        return SizeResolution::Known {
            logical: oi.actual_size,
            physical,
        };
    }

    if oi
        .parts
        .iter()
        .any(|part| part.actual_size < -1 || (part.actual_size < 0 && !compressed))
    {
        return SizeResolution::Corrupt {
            physical,
            reason: SizeResolutionReason::InvalidPartSize,
        };
    }

    let declared = rustfs_utils::http::get_str(&oi.user_defined, rustfs_utils::http::SUFFIX_ACTUAL_SIZE);
    let declared = match declared {
        Some(value) if value.is_empty() => {
            return SizeResolution::Corrupt {
                physical,
                reason: SizeResolutionReason::InvalidDeclaredSize,
            };
        }
        Some(value) => match value.parse::<i64>() {
            Ok(value) if value >= 0 => Some(value),
            _ => {
                return SizeResolution::Corrupt {
                    physical,
                    reason: SizeResolutionReason::InvalidDeclaredSize,
                };
            }
        },
        None => None,
    };

    let logical = match oi.get_actual_size() {
        Ok(size) if size == -1 && compressed && declared.is_none() => {
            return SizeResolution::Unknown {
                physical,
                reason: SizeResolutionReason::CompressedSizeUnknown,
            };
        }
        Ok(size) if size >= 0 => size,
        Ok(_) | Err(_) => {
            return SizeResolution::Corrupt {
                physical,
                reason: SizeResolutionReason::SizeOverflowOrMismatch,
            };
        }
    };

    if compressed && logical == 0 && physical != 0 && oi.parts.is_empty() && declared.is_none() {
        return SizeResolution::Corrupt {
            physical,
            reason: SizeResolutionReason::SizeOverflowOrMismatch,
        };
    }

    SizeResolution::Known { logical, physical }
}

fn resolve_sizes(object_infos: &[ObjectInfo]) -> Vec<SizeResolution> {
    object_infos.iter().map(resolve_size).collect()
}

fn lifecycle_rule_has_size_filter(lifecycle: &BucketLifecycleConfiguration, rule_id: &str) -> bool {
    let filter_has_size = |filter: &s3s::dto::LifecycleRuleFilter| {
        filter.object_size_greater_than.is_some()
            || filter.object_size_less_than.is_some()
            || filter
                .and
                .as_ref()
                .is_some_and(|and| and.object_size_greater_than.is_some() || and.object_size_less_than.is_some())
    };
    lifecycle
        .rules
        .iter()
        .find(|rule| {
            if rule_id.is_empty() {
                rule.id.as_deref().is_none_or(str::is_empty)
            } else {
                rule.id.as_deref() == Some(rule_id)
            }
        })
        .and_then(|rule| rule.filter.as_ref())
        .is_some_and(filter_has_size)
}

fn lifecycle_event_allowed(resolution: &SizeResolution, event: &Event, lifecycle: &BucketLifecycleConfiguration) -> bool {
    match resolution {
        // Missing or invalid logical size only defers actions whose selected
        // rule actually depends on that size. Time/version-only actions retain
        // their existing semantics, including intrinsic events without a rule ID.
        SizeResolution::Unknown { .. } | SizeResolution::Corrupt { .. } => {
            !lifecycle_rule_has_size_filter(lifecycle, &event.rule_id)
        }
        SizeResolution::Known { .. } => true,
    }
}

/// A successful newer-noncurrent batch consumes both known and unresolved
/// versions from the retained-version alert count. The two accounting paths
/// are separate because only known sizes can contribute byte totals.
fn remaining_versions_after_queued_noncurrent(remaining_versions: usize, known_count: usize, unknown_count: usize) -> usize {
    remaining_versions.saturating_sub(known_count.saturating_add(unknown_count))
}

/// How the corrupt-metadata branch records the repair after attempting an
/// MRF intent (backlog#1894 axis A).
#[derive(Debug, PartialEq, Eq)]
pub(super) enum CorruptMetadataRecording {
    /// Intent accepted: the MRF consumer owns the repair (High Metadata
    /// heal, durable after the journal's group-commit flush), so the
    /// immediate heal request is skipped — the manager would otherwise book
    /// two tasks for one target. A pending-ledger entry stays behind as the
    /// backstop for what the journal cannot cover on its own (a crash inside
    /// the flush window, or the consumer exhausting its admission attempts);
    /// the repaired-notice fanout (axis B) drops the entry once the repair
    /// lands.
    LedgerOnly,
    /// Intent rejected (feature disabled, channel uninitialized, or full):
    /// the historical immediate heal request plus the ledger entry.
    ImmediateAndLedger,
}

pub(super) fn corrupt_metadata_recording(result: rustfs_common::mrf_channel::MrfIngressResult) -> CorruptMetadataRecording {
    match result {
        rustfs_common::mrf_channel::MrfIngressResult::Enqueued | rustfs_common::mrf_channel::MrfIngressResult::Coalesced => {
            CorruptMetadataRecording::LedgerOnly
        }
        rustfs_common::mrf_channel::MrfIngressResult::Dropped(_) => CorruptMetadataRecording::ImmediateAndLedger,
    }
}

pub(super) fn build_bucket_heal_request(bucket: String, priority: HealChannelPriority) -> HealChannelRequest {
    HealChannelRequest {
        bucket,
        priority,
        recreate_missing: Some(false),
        source: HealRequestSource::Scanner,
        ..Default::default()
    }
}

pub(super) fn build_object_heal_request(
    bucket: String,
    object: String,
    version_id: Option<String>,
    scan_mode: HealScanMode,
    priority: HealChannelPriority,
) -> HealChannelRequest {
    HealChannelRequest {
        bucket,
        object_prefix: Some(object),
        object_version_id: version_id,
        priority,
        scan_mode: Some(scan_mode),
        remove_corrupted: Some(HEAL_DELETE_DANGLING),
        recreate_missing: Some(false),
        source: HealRequestSource::Scanner,
        ..Default::default()
    }
}

/// Build the versionless inspection request used when discovery cannot prove
/// a destructive version identity (for example an unversioned object or a
/// bounded candidate overflow). The explicit flag is the fail-closed safety
/// boundary; callers must not reconstruct it with the destructive default.
pub(super) fn build_non_destructive_object_heal_request(
    bucket: String,
    object: String,
    scan_mode: HealScanMode,
    priority: HealChannelPriority,
) -> HealChannelRequest {
    let mut request = build_object_heal_request(bucket, object, None, scan_mode, priority);
    request.remove_corrupted = Some(false);
    request
}

#[cfg(test)]
pub(super) fn resolve_object_heal_entry(
    entries: &MetaCacheEntries,
    resolver: MetadataResolutionParams,
) -> Option<MetaCacheEntry> {
    if let Some(entry) = entries.resolve(resolver) {
        return entry.is_object().then_some(entry);
    }

    entries
        .as_ref()
        .iter()
        .flatten()
        .find(|entry| entry.is_object() && !entry.name.ends_with(SLASH_SEPARATOR))
        .cloned()
}

pub(super) fn is_missing_path_disk_error(err: &DiskError) -> bool {
    matches!(err, DiskError::FileNotFound | DiskError::FileVersionNotFound | DiskError::VolumeNotFound)
}

pub(super) fn disk_errors_are_only_missing_paths(errs: &[Option<DiskError>]) -> bool {
    let mut saw_missing_path = false;
    for err in errs.iter().flatten() {
        if !is_missing_path_disk_error(err) {
            return false;
        }
        saw_missing_path = true;
    }
    saw_missing_path
}

pub(super) fn heal_priority_label(priority: HealChannelPriority) -> &'static str {
    match priority {
        HealChannelPriority::Low => "low",
        HealChannelPriority::Normal => "normal",
        HealChannelPriority::High => "high",
        HealChannelPriority::Critical => "critical",
    }
}

pub(super) fn describe_heal_admission(result: HealAdmissionResult) -> String {
    match result {
        HealAdmissionResult::Accepted | HealAdmissionResult::Merged => result.result_label().to_string(),
        HealAdmissionResult::Full => "queue_full".to_string(),
        HealAdmissionResult::Dropped(reason) => format!("dropped:{}", reason.as_str()),
    }
}

pub(super) fn record_high_priority_heal_escalation(
    candidate_type: &'static str,
    priority: HealChannelPriority,
    result: HealAdmissionResult,
) {
    counter!(
        "rustfs_heal_candidate_priority_reject_total",
        "type" => candidate_type.to_string(),
        "priority" => heal_priority_label(priority).to_string(),
        "result" => result.result_label().to_string(),
        "reason" => result.reason_label().to_string()
    )
    .increment(1);
}

pub(super) fn build_high_priority_heal_admission_error(
    candidate_type: &'static str,
    bucket: &str,
    object: Option<&str>,
    priority: HealChannelPriority,
    result: HealAdmissionResult,
) -> ScannerError {
    let object_text = object.map(|object| format!(", object='{object}'")).unwrap_or_default();
    ScannerError::Other(format!(
        "high-priority heal request was not admitted: type={candidate_type}, bucket='{bucket}'{object_text}, priority={}, admission={}",
        heal_priority_label(priority),
        describe_heal_admission(result)
    ))
}

pub(super) fn record_heal_candidate_admission(
    candidate_type: &'static str,
    priority: HealChannelPriority,
    result: HealAdmissionResult,
) {
    counter!(
        "rustfs_heal_candidate_enqueue_total",
        "type" => candidate_type.to_string(),
        "priority" => heal_priority_label(priority).to_string(),
        "result" => result.result_label().to_string()
    )
    .increment(1);

    if matches!(result, HealAdmissionResult::Merged) {
        counter!(
            "rustfs_heal_candidate_merge_total",
            "type" => candidate_type.to_string()
        )
        .increment(1);
    }

    if let HealAdmissionResult::Dropped(reason) = result {
        counter!(
            "rustfs_heal_candidate_drop_total",
            "type" => candidate_type.to_string(),
            "reason" => reason.as_str().to_string()
        )
        .increment(1);
    }
}

pub(super) async fn send_scanner_heal_request(
    candidate_type: &'static str,
    request: HealChannelRequest,
) -> Result<HealAdmissionResult, ScannerError> {
    let priority = request.priority;
    let trace_context = scanner_heal_candidate_trace_context(&request);
    match send_heal_request_with_admission(request).await {
        Ok(result) => {
            record_heal_candidate_admission(candidate_type, priority, result);
            if let Some(trace_context) = trace_context.as_ref() {
                emit_scanner_heal_candidate_trace(ScannerHealCandidateTrace {
                    candidate_type,
                    bucket: &trace_context.bucket,
                    object: trace_context.object.as_deref(),
                    version_id: trace_context.version_id.as_deref(),
                    priority,
                    scan_mode: trace_context.scan_mode,
                    result: Ok(result),
                    started_at: trace_context.started_at,
                });
            }
            Ok(result)
        }
        Err(err) => {
            counter!(
                "rustfs_heal_candidate_enqueue_total",
                "type" => candidate_type.to_string(),
                "priority" => heal_priority_label(priority).to_string(),
                "result" => "channel_error".to_string()
            )
            .increment(1);
            if let Some(trace_context) = trace_context.as_ref() {
                emit_scanner_heal_candidate_trace(ScannerHealCandidateTrace {
                    candidate_type,
                    bucket: &trace_context.bucket,
                    object: trace_context.object.as_deref(),
                    version_id: trace_context.version_id.as_deref(),
                    priority,
                    scan_mode: trace_context.scan_mode,
                    result: Err(err.as_str()),
                    started_at: trace_context.started_at,
                });
            }
            Err(ScannerError::Other(err))
        }
    }
}

/// Scanner item representing a file during scanning
#[derive(Clone, Debug)]
pub struct ScannerItem {
    pub path: String,
    pub bucket: String,
    pub prefix: String,
    pub object_name: String,
    pub file_type: FileType,
    pub lifecycle: Option<Arc<BucketLifecycleConfiguration>>,
    pub object_lock: Option<Arc<ObjectLockConfiguration>>,
    pub replication: Option<Arc<ReplicationConfig>>,
    pub heal_enabled: bool,
    pub heal_bitrot: bool,
    pub debug: bool,
}

impl ScannerItem {
    /// Get the object path (prefix + object_name)
    pub fn object_path(&self) -> String {
        if self.prefix.is_empty() {
            self.object_name.clone()
        } else {
            path_join_buf(&[&self.prefix, &self.object_name])
        }
    }

    /// Transform meta directory by splitting prefix and extracting object name
    /// This converts a directory path like "bucket/dir1/dir2/file" to prefix="bucket/dir1/dir2" and object_name="file"
    pub fn transform_meta_dir(&mut self) {
        let prefix = std::mem::take(&mut self.prefix);
        if let Some((parent, object_name)) = prefix.rsplit_once(SLASH_SEPARATOR) {
            self.prefix = path_join_buf(&[parent]);
            self.object_name = object_name.to_string();
        } else {
            self.object_name = prefix;
        }
    }

    pub(super) fn metadata_object_path(&self) -> String {
        let mut item = self.clone();
        item.transform_meta_dir();
        item.object_path()
    }

    pub async fn apply_actions(
        &mut self,
        object_infos: Vec<ObjectInfo>,
        lock_retention: Option<Arc<ObjectLockConfiguration>>,
        versioning_config: VersioningConfiguration,
        size_summary: &mut SizeSummary,
    ) {
        let object_path = self.object_path();
        if object_infos.is_empty() {
            debug!(
                target: "rustfs::scanner::folder",
                event = EVENT_SCANNER_LIFECYCLE_ACTION,
                component = LOG_COMPONENT_SCANNER,
                subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                object_path = %object_path,
                state = "no_object_versions",
                "Scanner lifecycle action skipped"
            );
            return;
        }
        debug!(
            target: "rustfs::scanner::folder",
            event = EVENT_SCANNER_LIFECYCLE_ACTION,
            component = LOG_COMPONENT_SCANNER,
            subsystem = LOG_SUBSYSTEM_LIFECYCLE,
            object_path = %object_path,
            state = "started",
            "Scanner lifecycle evaluation started"
        );

        let resolved_sizes = resolve_sizes(&object_infos);
        if let Some(first) = object_infos.first() {
            size_summary.record_reconciliation_scope(
                &bounded_reconciliation_field(&first.bucket),
                &bounded_reconciliation_field(&first.name),
            );
        }
        for (oi, resolution) in object_infos.iter().zip(resolved_sizes.iter()) {
            record_size_resolution(size_summary, oi, resolution);
        }
        let has_corrupt_size = resolved_sizes
            .iter()
            .any(|resolution| matches!(resolution, SizeResolution::Corrupt { .. }));

        // `versioning_config` is resolved once per object by the caller
        // (`get_size`) and handed in; only `prefix_enabled` is consulted here.

        let Some(lifecycle) = self.lifecycle.clone() else {
            let mut cumulative_size: i64 = 0;
            for (oi, resolved_size) in object_infos.iter().zip(resolved_sizes.iter()) {
                let accounting_size = match resolved_size {
                    SizeResolution::Known { logical, .. } => *logical,
                    // A valid compressed legacy sentinel has no logical size,
                    // but heal and replication still need to run. The
                    // physical size is only an input to those operations; it
                    // is not folded into the logical total below.
                    SizeResolution::Unknown { physical, .. } => {
                        self.heal_actions(oi, *physical, size_summary).await;
                        size_summary.actions_accounting_unknown(oi);
                        continue;
                    }
                    SizeResolution::Corrupt { .. } => {
                        size_summary.actions_accounting_unknown(oi);
                        continue;
                    }
                };

                let size = self.heal_actions(oi, accounting_size, size_summary).await;

                size_summary.actions_accounting(oi, size, accounting_size);

                cumulative_size = cumulative_size.saturating_add(size);
            }

            self.alert_excessive_versions(object_infos.len(), cumulative_size);

            debug!(
                target: "rustfs::scanner::folder",
                event = EVENT_SCANNER_LIFECYCLE_ACTION,
                component = LOG_COMPONENT_SCANNER,
                subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                object_path = %object_path,
                state = "no_lifecycle_config",
                "Scanner lifecycle action finished without lifecycle rules"
            );
            return;
        };

        let object_opts = object_infos
            .iter()
            .map(crate::ecstore_object_opts_from_object_info)
            .collect::<Vec<ObjectOpts>>();

        let events = match Evaluator::new(lifecycle.clone())
            .with_lock_retention(lock_retention)
            .with_replication_config(scanner_replication_config_for_lifecycle_eval(self.replication.clone()))
            .eval(&object_opts)
            .await
        {
            Ok(events) => events,
            Err(e) => {
                warn!(
                    target: "rustfs::scanner::folder",
                    event = EVENT_SCANNER_LIFECYCLE_ACTION,
                    component = LOG_COMPONENT_SCANNER,
                    subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                    object_path = %object_path,
                    state = "evaluate_failed",
                    error = %e,
                    "Scanner lifecycle action evaluation failed"
                );
                return;
            }
        };

        // Every version handed to the evaluator counts as an ILM-checked
        // version, whether or not a rule matched (NoneAction included). This is
        // reached only for buckets with lifecycle rules; it feeds
        // rustfs_ilm_versions_scanned_total via the Lifecycle source's checked
        // counter.
        global_metrics().record_scanner_source_checked(ScannerWorkSource::Lifecycle, object_opts.len() as u64);

        let mut to_delete_objs: Vec<ObjectToDelete> = Vec::new();
        let mut noncurrent_events: Vec<Event> = Vec::new();
        let mut noncurrent_accounting: Vec<PendingScannerAccounting<'_>> = Vec::new();
        let mut noncurrent_unknown: Vec<&ObjectInfo> = Vec::new();
        let mut cumulative_size = 0;
        let mut remaining_versions = object_infos.len();
        'eventLoop: {
            for (i, event) in events.iter().enumerate() {
                let oi = &object_infos[i];
                let known_size = resolved_sizes[i].known_size();
                if has_corrupt_size
                    && matches!(
                        event.action,
                        IlmAction::DeleteAllVersionsAction | IlmAction::DelMarkerDeleteAllVersionsAction
                    )
                {
                    // An all-version delete would also remove a corrupt
                    // sibling that could not be reconciled safely.
                    continue;
                }
                if !lifecycle_event_allowed(&resolved_sizes[i], event, &lifecycle) {
                    // An unknown logical size must not make an otherwise
                    // non-destructive scan disappear from heal/physical-tier
                    // accounting. Size-filtered or deferred events remain
                    // pending, so retain the version-only physical counters.
                    if let SizeResolution::Unknown { physical, .. } = &resolved_sizes[i] {
                        self.heal_actions(oi, *physical, size_summary).await;
                        size_summary.actions_accounting_unknown(oi);
                    }
                    continue;
                }
                let actual_size = match known_size {
                    Some(size) => size,
                    None => {
                        match event.action {
                            IlmAction::DeleteAction
                            | IlmAction::DeleteRestoredAction
                            | IlmAction::DeleteRestoredVersionAction
                            | IlmAction::DeleteAllVersionsAction
                            | IlmAction::DelMarkerDeleteAllVersionsAction => {
                                let done_ilm = Metrics::time_ilm(event.action);
                                let trace_started_at = trace_start_instant();
                                let queued = apply_expiry_rule(event, &LcEventSrc::Scanner, oi).await;
                                emit_scanner_ilm_action_trace(&self.bucket, &oi.name, event.action, 1, queued, trace_started_at);
                                if record_scanner_ilm_action_if_queued(global_metrics(), event.action, 1, queued) {
                                    done_ilm(1)();
                                    if event.action == IlmAction::DeleteAllVersionsAction
                                        || event.action == IlmAction::DelMarkerDeleteAllVersionsAction
                                    {
                                        remaining_versions = 0;
                                    }
                                } else if matches!(
                                    event.action,
                                    IlmAction::DeleteAction
                                        | IlmAction::DeleteRestoredAction
                                        | IlmAction::DeleteRestoredVersionAction
                                ) {
                                    size_summary.actions_accounting_unknown(oi);
                                } else {
                                    size_summary.actions_accounting_unknown(oi);
                                    for (j, retained) in object_infos.iter().enumerate().skip(i + 1) {
                                        match &resolved_sizes[j] {
                                            SizeResolution::Known { logical, .. } => PendingScannerAccounting {
                                                object: retained,
                                                retained_size: *logical,
                                                expired_size: 0,
                                            }
                                            .apply(size_summary, &mut cumulative_size, false),
                                            SizeResolution::Unknown { .. } => {
                                                size_summary.actions_accounting_unknown(retained);
                                            }
                                            SizeResolution::Corrupt { .. } => {}
                                        }
                                    }
                                }
                            }
                            IlmAction::DeleteVersionAction => {
                                if let Some(opt) = object_opts.get(i) {
                                    to_delete_objs.push(ObjectToDelete {
                                        object_name: opt.name.clone(),
                                        version_id: opt.version_id,
                                        ..Default::default()
                                    });
                                    noncurrent_events.push(event.clone());
                                    noncurrent_unknown.push(oi);
                                }
                            }
                            IlmAction::TransitionAction | IlmAction::TransitionVersionAction => {
                                let trace_started_at = trace_start_instant();
                                let queued = apply_transition_rule(event, &LcEventSrc::Scanner, oi).await;
                                emit_scanner_ilm_action_trace(&self.bucket, &oi.name, event.action, 1, queued, trace_started_at);
                                if record_scanner_ilm_action_if_queued(global_metrics(), event.action, 1, queued) {
                                    let done_ilm = Metrics::time_ilm(event.action);
                                    done_ilm(1)();
                                }
                                size_summary.actions_accounting_unknown(oi);
                            }
                            IlmAction::NoneAction | IlmAction::ActionCount => {
                                if let SizeResolution::Unknown { physical, .. } = &resolved_sizes[i] {
                                    self.heal_actions(oi, *physical, size_summary).await;
                                }
                                size_summary.actions_accounting_unknown(oi);
                            }
                        }
                        continue;
                    }
                };

                let mut size = actual_size;
                let mut account_now = true;

                match event.action {
                    IlmAction::DeleteAllVersionsAction | IlmAction::DelMarkerDeleteAllVersionsAction => {
                        debug!(
                            target: "rustfs::scanner::folder",
                            event = EVENT_SCANNER_LIFECYCLE_ACTION,
                            component = LOG_COMPONENT_SCANNER,
                            subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                            bucket = %self.bucket,
                            object = %oi.name,
                            action = %event.action,
                            state = "apply_expiry_rule",
                            "Scanner lifecycle action dispatched"
                        );
                        let done_ilm = Metrics::time_ilm(event.action);
                        let trace_started_at = trace_start_instant();
                        let queued = apply_expiry_rule(event, &LcEventSrc::Scanner, oi).await;
                        emit_scanner_ilm_action_trace(&self.bucket, &oi.name, event.action, 1, queued, trace_started_at);
                        if record_scanner_ilm_action_if_queued(global_metrics(), event.action, 1, queued) {
                            done_ilm(1)();
                            remaining_versions = 0;
                        } else {
                            if let Some(actual_size) = known_size {
                                PendingScannerAccounting {
                                    object: oi,
                                    retained_size: actual_size,
                                    expired_size: 0,
                                }
                                .apply(size_summary, &mut cumulative_size, false);
                            }
                            for (j, retained) in object_infos.iter().enumerate().skip(i + 1) {
                                if let Some(retained_size) = resolved_sizes[j].known_size() {
                                    PendingScannerAccounting {
                                        object: retained,
                                        retained_size,
                                        expired_size: 0,
                                    }
                                    .apply(size_summary, &mut cumulative_size, false);
                                }
                            }
                        }
                        break 'eventLoop;
                    }

                    IlmAction::DeleteAction | IlmAction::DeleteRestoredAction | IlmAction::DeleteRestoredVersionAction => {
                        debug!(
                            target: "rustfs::scanner::folder",
                            event = EVENT_SCANNER_LIFECYCLE_ACTION,
                            component = LOG_COMPONENT_SCANNER,
                            subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                            bucket = %self.bucket,
                            object = %oi.name,
                            action = %event.action,
                            state = "apply_expiry_rule",
                            "Scanner lifecycle action dispatched"
                        );
                        let done_ilm = Metrics::time_ilm(event.action);
                        let trace_started_at = trace_start_instant();
                        let queued = apply_expiry_rule(event, &LcEventSrc::Scanner, oi).await;
                        emit_scanner_ilm_action_trace(&self.bucket, &oi.name, event.action, 1, queued, trace_started_at);
                        if record_scanner_ilm_action_if_queued(global_metrics(), event.action, 1, queued) {
                            done_ilm(1)();
                            if !versioning_config.prefix_enabled(&object_path) && event.action == IlmAction::DeleteAction {
                                remaining_versions -= 1;
                                size = 0;
                            }
                        }
                    }
                    IlmAction::DeleteVersionAction => {
                        if let Some(opt) = object_opts.get(i) {
                            to_delete_objs.push(ObjectToDelete {
                                object_name: opt.name.clone(),
                                version_id: opt.version_id,
                                ..Default::default()
                            });
                            if let Some(actual_size) = known_size {
                                noncurrent_accounting.push(PendingScannerAccounting {
                                    object: oi,
                                    retained_size: actual_size,
                                    expired_size: 0,
                                });
                            }
                            account_now = false;
                        }
                        noncurrent_events.push(event.clone());
                    }
                    IlmAction::TransitionAction | IlmAction::TransitionVersionAction => {
                        debug!(
                            target: "rustfs::scanner::folder",
                            event = EVENT_SCANNER_LIFECYCLE_ACTION,
                            component = LOG_COMPONENT_SCANNER,
                            subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                            bucket = %self.bucket,
                            object = %oi.name,
                            action = %event.action,
                            state = "apply_transition_rule",
                            "Scanner lifecycle action dispatched"
                        );
                        let done_ilm = Metrics::time_ilm(event.action);
                        let trace_started_at = trace_start_instant();
                        let queued = apply_transition_rule(event, &LcEventSrc::Scanner, oi).await;
                        emit_scanner_ilm_action_trace(&self.bucket, &oi.name, event.action, 1, queued, trace_started_at);
                        if record_scanner_ilm_action_if_queued(global_metrics(), event.action, 1, queued) {
                            done_ilm(1)();
                        }
                    }

                    IlmAction::NoneAction | IlmAction::ActionCount => {
                        size = self.heal_actions(oi, actual_size, size_summary).await;
                    }
                }

                if account_now {
                    size_summary.actions_accounting(oi, size, actual_size);
                    cumulative_size = cumulative_size.saturating_add(size);
                }
            }
        }

        if !to_delete_objs.is_empty()
            && let Some(event) = noncurrent_events.first().cloned()
        {
            let action = event.action;
            let count = u64::try_from(to_delete_objs.len()).unwrap_or(u64::MAX);
            let done_ilm = Metrics::time_ilm(action);
            let trace_started_at = trace_start_instant();
            let queued = enqueue_runtime_newer_noncurrent(&self.bucket, to_delete_objs, event, &LcEventSrc::Scanner).await;
            if let Some(trace_started_at) = trace_started_at {
                let state = if queued { "queued" } else { "not_queued" };
                trace_emit(|| {
                    TraceEvent::new(TraceKind::Scanner, TraceFunc::ScannerIlmAction)
                        .with_bucket(self.bucket.as_str())
                        .with_object(object_path.as_str())
                        .with_duration(trace_started_at.elapsed())
                        .with_attr("state", state)
                        .with_attr("action", action.as_str())
                        .with_attr("count", count)
                        .with_attr("queued", queued)
                });
            }
            if record_scanner_ilm_action_if_queued(global_metrics(), action, count, queued) {
                done_ilm(count)();
                remaining_versions = remaining_versions_after_queued_noncurrent(
                    remaining_versions,
                    noncurrent_accounting.len(),
                    noncurrent_unknown.len(),
                );
            }
            for pending in noncurrent_accounting {
                pending.apply(size_summary, &mut cumulative_size, queued);
            }
            if !queued {
                for object in noncurrent_unknown {
                    size_summary.actions_accounting_unknown(object);
                }
            }
        }
        self.alert_excessive_versions(remaining_versions, cumulative_size);
    }

    pub(super) async fn heal_actions(&mut self, oi: &ObjectInfo, actual_size: i64, size_summary: &mut SizeSummary) -> i64 {
        if self.heal_enabled {
            self.enqueue_heal(oi).await;
        }

        self.heal_replication(oi, size_summary).await;

        actual_size
    }

    pub(super) async fn heal_replication(&mut self, oi: &ObjectInfo, size_summary: &mut SizeSummary) {
        if oi.version_id.is_none_or(|version| version.is_nil()) && !oi.delete_marker && oi.version_purge_status.is_empty() {
            return;
        }

        let Some(replication) = self.replication.clone() else {
            return;
        };

        let done_replication = Metrics::time(Metric::CheckReplication);
        let replication_result = queue_replication_heal(&oi.bucket, oi.clone(), (*replication).clone(), 0).await;
        done_replication();
        let roi = replication_result.object_info;
        record_scanner_replication_admission(global_metrics(), &roi, replication_result.admission);
        if !Self::should_account_replication_stats(oi) {
            return;
        }

        for (arn, target_status) in roi.target_statuses.iter() {
            if !size_summary.repl_target_stats.contains_key(arn.as_str()) {
                size_summary
                    .repl_target_stats
                    .insert(arn.clone(), ReplTargetSizeSummary::default());
            }

            if let Some(repl_target_size_summary) = size_summary.repl_target_stats.get_mut(arn.as_str()) {
                match target_status {
                    ReplicationStatusType::Pending => {
                        repl_target_size_summary.pending_size = repl_target_size_summary.pending_size.saturating_add(roi.size);
                        repl_target_size_summary.pending_count = repl_target_size_summary.pending_count.saturating_add(1);
                        size_summary.pending_size = size_summary.pending_size.saturating_add(roi.size);
                        size_summary.pending_count = size_summary.pending_count.saturating_add(1);
                    }
                    ReplicationStatusType::Failed => {
                        repl_target_size_summary.failed_size = repl_target_size_summary.failed_size.saturating_add(roi.size);
                        repl_target_size_summary.failed_count = repl_target_size_summary.failed_count.saturating_add(1);
                        size_summary.failed_size = size_summary.failed_size.saturating_add(roi.size);
                        size_summary.failed_count = size_summary.failed_count.saturating_add(1);
                    }
                    ReplicationStatusType::Completed | ReplicationStatusType::CompletedLegacy => {
                        repl_target_size_summary.replicated_size =
                            repl_target_size_summary.replicated_size.saturating_add(roi.size);
                        repl_target_size_summary.replicated_count = repl_target_size_summary.replicated_count.saturating_add(1);
                        size_summary.replicated_size = size_summary.replicated_size.saturating_add(roi.size);
                        size_summary.replicated_count = size_summary.replicated_count.saturating_add(1);
                    }
                    _ => {}
                }
            }
        }

        if oi.replication_status == ReplicationStatusType::Replica {
            size_summary.replica_size = size_summary.replica_size.saturating_add(roi.size);
            size_summary.replica_count = size_summary.replica_count.saturating_add(1);
        }
    }

    pub(super) fn should_account_replication_stats(oi: &ObjectInfo) -> bool {
        !oi.delete_marker && oi.version_purge_status.is_empty()
    }

    pub(super) async fn enqueue_heal(&mut self, oi: &ObjectInfo) {
        let done_heal = Metrics::time(Metric::HealAbandonedObject);
        let object = if oi.name.is_empty() {
            self.object_path()
        } else {
            oi.name.clone()
        };
        debug!(
            target: "rustfs::scanner::folder",
            event = EVENT_SCANNER_HEAL_ADMISSION,
            component = LOG_COMPONENT_SCANNER,
            subsystem = LOG_SUBSYSTEM_HEAL,
            bucket = %self.bucket,
            object = %object,
            version_id = %oi.version_id.unwrap_or_default(),
            state = "request_started",
            "Scanner heal admission started"
        );

        let now = OffsetDateTime::now_utc();
        let scan_mode = effective_object_heal_scan_mode(self.heal_bitrot, oi.mod_time, now);
        if self.heal_bitrot && scan_mode != HealScanMode::Deep {
            let cooldown = deep_verify_cooldown();
            let age_secs = oi.mod_time.map(|mod_time| {
                let age = now - mod_time;
                age.whole_seconds().max(0)
            });
            debug!(
                target: "rustfs::scanner::folder",
                event = EVENT_SCANNER_HEAL_ADMISSION,
                component = LOG_COMPONENT_SCANNER,
                subsystem = LOG_SUBSYSTEM_HEAL,
                bucket = %self.bucket,
                object = %object,
                version_id = %oi.version_id.unwrap_or_default(),
                object_age_secs = age_secs.unwrap_or_default(),
                cooldown_secs = cooldown.as_secs(),
                original_scan_mode = %HealScanMode::Deep.as_str(),
                effective_scan_mode = %scan_mode.as_str(),
                state = "downgraded_to_normal",
                "Scanner heal deep scan downgraded"
            );
        }

        let result = send_scanner_heal_request(
            "object",
            build_object_heal_request(
                self.bucket.clone(),
                object.clone(),
                oi.version_id
                    .and_then(|v| if v.is_nil() { None } else { Some(v.to_string()) }),
                scan_mode,
                HealChannelPriority::Low,
            ),
        )
        .await;

        let admission = result.as_ref().copied().map_err(|_| ());
        let admitted = record_scanner_heal_admission(global_metrics(), scan_mode, admission);
        match result {
            Ok(HealAdmissionResult::Accepted | HealAdmissionResult::Merged) => {}
            Ok(result @ (HealAdmissionResult::Full | HealAdmissionResult::Dropped(_))) => {
                warn!(
                    target: "rustfs::scanner::folder",
                    event = EVENT_SCANNER_HEAL_ADMISSION,
                    component = LOG_COMPONENT_SCANNER,
                    subsystem = LOG_SUBSYSTEM_HEAL,
                    bucket = %self.bucket,
                    object = %object,
                    admission = %describe_heal_admission(result),
                    state = "not_admitted",
                    "Scanner heal admission rejected low-priority request"
                );
            }
            Err(e) => warn!(
                target: "rustfs::scanner::folder",
                event = EVENT_SCANNER_HEAL_ADMISSION,
                component = LOG_COMPONENT_SCANNER,
                subsystem = LOG_SUBSYSTEM_HEAL,
                bucket = %self.bucket,
                object = %object,
                state = "submit_failed",
                error = %e,
                "Scanner heal admission submission failed"
            ),
        }
        if admitted {
            done_heal();
        }
    }

    pub(super) fn alert_excessive_versions(&self, remaining_versions: usize, cumulative_size: i64) {
        ensure_scanner_alert_metrics_registered();
        let (too_many_versions, too_large_versions) = should_alert_excessive_versions(remaining_versions, cumulative_size);
        // Threshold check first so healthy objects never pay for the
        // object-path allocation below.
        if !too_many_versions && !too_large_versions {
            return;
        }
        let object_path = self.object_path();
        if too_many_versions {
            global_metrics().record_scanner_source_executed(ScannerWorkSource::Alerts, 1);
            counter!(
                METRIC_SCANNER_EXCESS_OBJECT_VERSIONS_TOTAL,
                "bucket" => self.bucket.clone()
            )
            .increment(1);
            if scanner_alert_emission_allows(ScannerAlertKind::ManyVersions, &self.bucket, &object_path, scanner_alert_cooldown())
            {
                emit_scanner_alert_event(
                    EVENT_SCANNER_MANY_VERSIONS,
                    &self.bucket,
                    &object_path,
                    cumulative_size,
                    &[
                        ("versions", remaining_versions.to_string()),
                        ("threshold", scanner_excess_versions_threshold().to_string()),
                    ],
                );
            }
            warn!(
                target: "rustfs::scanner::folder",
                event = EVENT_SCANNER_ALERT_STATE,
                component = LOG_COMPONENT_SCANNER,
                subsystem = LOG_SUBSYSTEM_FOLDER,
                bucket = %self.bucket,
                object = %object_path,
                versions = remaining_versions,
                threshold = scanner_excess_versions_threshold(),
                state = "excess_versions",
                "Scanner alert recorded excessive retained versions"
            );
        }
        if too_large_versions {
            global_metrics().record_scanner_source_executed(ScannerWorkSource::Alerts, 1);
            counter!(
                METRIC_SCANNER_EXCESS_OBJECT_VERSION_SIZE_TOTAL,
                "bucket" => self.bucket.clone()
            )
            .increment(1);
            if scanner_alert_emission_allows(
                ScannerAlertKind::LargeVersions,
                &self.bucket,
                &object_path,
                scanner_alert_cooldown(),
            ) {
                emit_scanner_alert_event(
                    EVENT_SCANNER_LARGE_VERSIONS,
                    &self.bucket,
                    &object_path,
                    cumulative_size,
                    &[
                        ("versions", remaining_versions.to_string()),
                        ("cumulativeSize", cumulative_size.to_string()),
                        ("threshold", scanner_excess_version_size_threshold().to_string()),
                    ],
                );
            }
            warn!(
                target: "rustfs::scanner::folder",
                event = EVENT_SCANNER_ALERT_STATE,
                component = LOG_COMPONENT_SCANNER,
                subsystem = LOG_SUBSYSTEM_FOLDER,
                bucket = %self.bucket,
                object = %object_path,
                versions = remaining_versions,
                cumulative_size,
                threshold = scanner_excess_version_size_threshold(),
                state = "excess_version_size",
                "Scanner alert recorded excessive retained version size"
            );
        }
    }
}

pub(super) fn classify_get_size_failure(item: &ScannerItem, err: &StorageError) -> GetSizeFailureAction {
    if matches!(err, StorageError::Io(io) if io.to_string() == SCANNER_SKIP_FILE_ERROR) {
        return GetSizeFailureAction::Skip;
    }

    if is_scanner_metadata_corrupt_error(err) {
        return GetSizeFailureAction::HealMetadata {
            object: item.metadata_object_path(),
        };
    }

    if is_scanner_metadata_transient_error(err) {
        return GetSizeFailureAction::RecordFailed;
    }

    GetSizeFailureAction::RecordFailed
}

pub(super) async fn contains_erasure_part_file(path: &str) -> Result<bool, ScannerError> {
    let mut entries = match tokio::fs::read_dir(path).await {
        Ok(entries) => entries,
        Err(err) if matches!(err.kind(), ErrorKind::NotFound | ErrorKind::NotADirectory) => return Ok(false),
        Err(err) => return Err(ScannerError::Io(err)),
    };

    for _ in 0..ERASURE_DATA_DIR_PROBE_ENTRY_LIMIT {
        let entry = match entries.next_entry().await {
            Ok(Some(entry)) => entry,
            Ok(None) => return Ok(false),
            Err(err) if matches!(err.kind(), ErrorKind::NotFound | ErrorKind::NotADirectory) => return Ok(false),
            Err(err) => return Err(ScannerError::Io(err)),
        };
        let file_name = entry.file_name();
        let Some(part_number) = file_name
            .to_str()
            .and_then(|name| name.strip_prefix("part."))
            .and_then(|number| number.parse::<u32>().ok())
        else {
            continue;
        };
        if part_number == 0 {
            continue;
        }

        match entry.file_type().await {
            Ok(file_type) if file_type.is_file() => return Ok(true),
            Ok(_) => {}
            Err(err) if matches!(err.kind(), ErrorKind::NotFound | ErrorKind::TooManyLinks) => {}
            Err(err) => return Err(ScannerError::Io(err)),
        }
    }

    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scanner_item_with_prefix(prefix: &str) -> ScannerItem {
        ScannerItem {
            path: String::new(),
            bucket: "bucket".to_string(),
            prefix: prefix.to_string(),
            object_name: String::new(),
            file_type: std::fs::metadata(std::env::temp_dir())
                .expect("temp dir metadata should be readable")
                .file_type(),
            lifecycle: None,
            object_lock: None,
            replication: None,
            heal_enabled: false,
            heal_bitrot: false,
            debug: false,
        }
    }

    #[test]
    fn transform_meta_dir_splits_parent_and_object_without_extra_components() {
        let mut item = scanner_item_with_prefix("bucket/prefix/object");

        item.transform_meta_dir();

        assert_eq!(item.prefix, "bucket/prefix");
        assert_eq!(item.object_name, "object");
        assert_eq!(item.object_path(), "bucket/prefix/object");
    }

    #[test]
    fn transform_meta_dir_moves_single_component_into_object_name() {
        let mut item = scanner_item_with_prefix("object");

        item.transform_meta_dir();

        assert_eq!(item.prefix, "");
        assert_eq!(item.object_name, "object");
        assert_eq!(item.object_path(), "object");
    }

    #[test]
    fn size_resolution_rejects_negative_overflow_and_unknown_compression() {
        let compressed = |actual_size: i64, declared: Option<&str>| {
            let mut user_defined = HashMap::new();
            rustfs_utils::http::insert_str(&mut user_defined, rustfs_utils::http::SUFFIX_COMPRESSION, "zstd".to_string());
            if let Some(declared) = declared {
                rustfs_utils::http::insert_str(&mut user_defined, rustfs_utils::http::SUFFIX_ACTUAL_SIZE, declared.to_string());
            }
            ObjectInfo {
                size: 12,
                actual_size,
                user_defined: Arc::new(user_defined),
                ..Default::default()
            }
        };

        let normal = ObjectInfo {
            size: 12,
            actual_size: 10,
            ..Default::default()
        };
        assert_eq!(
            resolve_size(&normal),
            SizeResolution::Known {
                logical: 10,
                physical: 12
            }
        );

        let stale_declared_metadata = ObjectInfo {
            size: 12,
            actual_size: 10,
            user_defined: Arc::new(HashMap::from([("x-rustfs-internal-actual-size".to_string(), "not-a-size".to_string())])),
            parts: Arc::new(vec![rustfs_filemeta::ObjectPartInfo {
                actual_size: -2,
                ..Default::default()
            }]),
            ..Default::default()
        };
        assert_eq!(
            resolve_size(&stale_declared_metadata),
            SizeResolution::Known {
                logical: 10,
                physical: 12
            }
        );

        assert_eq!(
            resolve_size(&compressed(0, Some("9"))),
            SizeResolution::Known {
                logical: 9,
                physical: 12
            }
        );
        assert_eq!(
            resolve_size(&compressed(-1, None)),
            SizeResolution::Unknown {
                physical: 12,
                reason: SizeResolutionReason::CompressedSizeUnknown,
            }
        );
        assert!(matches!(
            resolve_size(&compressed(0, Some("not-a-size"))),
            SizeResolution::Corrupt {
                reason: SizeResolutionReason::InvalidDeclaredSize,
                ..
            }
        ));
        assert!(matches!(
            resolve_size(&ObjectInfo {
                size: 12,
                actual_size: -2,
                ..Default::default()
            }),
            SizeResolution::Corrupt { .. }
        ));
        assert!(matches!(resolve_size(&compressed(0, Some("-1"))), SizeResolution::Corrupt { .. }));
        assert!(matches!(resolve_size(&compressed(0, Some(""))), SizeResolution::Corrupt { .. }));

        let unsupported = {
            let mut object = compressed(0, None);
            let mut metadata = (*object.user_defined).clone();
            rustfs_utils::http::insert_str(&mut metadata, rustfs_utils::http::SUFFIX_COMPRESSION, "unsupported".to_string());
            object.user_defined = Arc::new(metadata);
            object
        };
        assert!(matches!(resolve_size(&unsupported), SizeResolution::Corrupt { .. }));

        let invalid_part = {
            let mut object = compressed(0, None);
            object.parts = Arc::new(vec![rustfs_filemeta::ObjectPartInfo {
                size: 12,
                actual_size: -2,
                ..Default::default()
            }]);
            object
        };
        assert!(matches!(resolve_size(&invalid_part), SizeResolution::Corrupt { .. }));

        let overflow = {
            let mut object = compressed(0, None);
            object.parts = Arc::new(vec![
                rustfs_filemeta::ObjectPartInfo {
                    size: 1,
                    actual_size: i64::MAX,
                    ..Default::default()
                },
                rustfs_filemeta::ObjectPartInfo {
                    size: 1,
                    actual_size: 1,
                    ..Default::default()
                },
            ]);
            object
        };
        assert!(matches!(resolve_size(&overflow), SizeResolution::Corrupt { .. }));

        let mismatch = compressed(0, None);
        assert!(matches!(resolve_size(&mismatch), SizeResolution::Corrupt { .. }));
        assert_eq!(
            resolve_size(&ObjectInfo {
                size: 0,
                actual_size: 0,
                ..Default::default()
            }),
            SizeResolution::Known { logical: 0, physical: 0 }
        );
    }

    #[test]
    fn size_resolution_records_and_replays_one_identity() {
        let version_id = uuid::Uuid::new_v4();
        let generation = uuid::Uuid::new_v4();
        let mut metadata = HashMap::new();
        rustfs_utils::http::insert_str(&mut metadata, rustfs_utils::http::SUFFIX_COMPRESSION, "zstd".to_string());
        rustfs_utils::http::insert_str(&mut metadata, rustfs_utils::http::SUFFIX_ACTUAL_SIZE, "not-a-number".to_string());
        let corrupt = ObjectInfo {
            bucket: "bucket".to_string(),
            name: "object".to_string(),
            size: 12,
            version_id: Some(version_id),
            data_dir: Some(generation),
            user_defined: Arc::new(metadata),
            ..Default::default()
        };

        let mut summary = SizeSummary::default();
        let resolution = resolve_size(&corrupt);
        record_size_resolution(&mut summary, &corrupt, &resolution);
        record_size_resolution(&mut summary, &corrupt, &resolution);
        assert_eq!(summary.size_reconciliation.len(), 1);
        assert_eq!(summary.size_reconciliation[0].reason, "invalid_declared_size");
        assert_eq!(summary.size_reconciliation[0].physical_size, Some(12));

        let known = ObjectInfo {
            actual_size: 12,
            user_defined: Arc::new(HashMap::new()),
            ..corrupt.clone()
        };
        record_size_resolution(&mut summary, &known, &resolve_size(&known));
        summary.record_reconciliation_scope(&known.bucket, &known.name);
        assert_eq!(summary.reconciliation_scopes.len(), 1);
        assert_eq!(summary.reconciliation_scopes[0].bucket, "bucket");
    }

    #[test]
    fn malformed_size_has_same_ilm_accounting() {
        let mut metadata = HashMap::new();
        rustfs_utils::http::insert_str(&mut metadata, rustfs_utils::http::SUFFIX_COMPRESSION, "zstd".to_string());
        rustfs_utils::http::insert_str(&mut metadata, rustfs_utils::http::SUFFIX_ACTUAL_SIZE, "invalid".to_string());
        let object = ObjectInfo {
            bucket: "bucket".to_string(),
            name: "object".to_string(),
            size: 12,
            user_defined: Arc::new(metadata),
            ..Default::default()
        };
        let resolution = resolve_size(&object);
        let mut without_ilm = SizeSummary::default();
        let mut with_ilm = SizeSummary::default();
        record_size_resolution(&mut without_ilm, &object, &resolution);
        record_size_resolution(&mut with_ilm, &object, &resolution);
        assert_eq!(without_ilm.size_reconciliation, with_ilm.size_reconciliation);
        assert_eq!(without_ilm.total_size, 0);
        assert_eq!(with_ilm.total_size, 0);
        assert!(without_ilm.tier_stats.is_empty());
        assert!(with_ilm.tier_stats.is_empty());
    }

    #[test]
    fn size_resolution_parses_once_per_version() {
        let objects = vec![
            ObjectInfo {
                bucket: "bucket".to_string(),
                name: "one".to_string(),
                size: 1,
                actual_size: 1,
                ..Default::default()
            },
            ObjectInfo {
                bucket: "bucket".to_string(),
                name: "two".to_string(),
                size: 2,
                actual_size: -2,
                ..Default::default()
            },
        ];
        let resolutions = resolve_sizes(&objects);
        assert_eq!(resolutions.len(), objects.len());
        assert!(matches!(resolutions[0], SizeResolution::Known { logical: 1, .. }));
        assert!(matches!(resolutions[1], SizeResolution::Corrupt { .. }));
    }

    #[test]
    fn queued_unknown_noncurrent_versions_are_removed_from_alert_count() {
        assert_eq!(remaining_versions_after_queued_noncurrent(3, 1, 2), 0);
        assert_eq!(remaining_versions_after_queued_noncurrent(7, 2, 1), 4);
        assert_eq!(remaining_versions_after_queued_noncurrent(usize::MAX, usize::MAX, usize::MAX), 0);
    }

    #[test]
    fn malformed_size_blocks_size_dependent_transition_but_allows_time_only_expiry() {
        let size_filtered = BucketLifecycleConfiguration {
            rules: vec![s3s::dto::LifecycleRule {
                status: s3s::dto::ExpirationStatus::from_static(s3s::dto::ExpirationStatus::ENABLED),
                expiration: None,
                abort_incomplete_multipart_upload: None,
                del_marker_expiration: None,
                id: Some("size".to_string()),
                filter: Some(s3s::dto::LifecycleRuleFilter {
                    object_size_greater_than: Some(1),
                    ..Default::default()
                }),
                noncurrent_version_expiration: None,
                noncurrent_version_transitions: None,
                prefix: None,
                transitions: None,
            }],
            ..Default::default()
        };
        let unknown = SizeResolution::Unknown {
            physical: 12,
            reason: SizeResolutionReason::CompressedSizeUnknown,
        };
        let size_event = Event {
            action: IlmAction::DeleteAction,
            rule_id: "size".to_string(),
            ..Default::default()
        };
        assert!(!lifecycle_event_allowed(&unknown, &size_event, &size_filtered));
        assert!(!lifecycle_event_allowed(
            &unknown,
            &Event {
                action: IlmAction::TransitionAction,
                rule_id: "size".to_string(),
                ..Default::default()
            },
            &size_filtered
        ));
        let mixed_filters = BucketLifecycleConfiguration {
            rules: vec![
                size_filtered.rules[0].clone(),
                s3s::dto::LifecycleRule {
                    status: s3s::dto::ExpirationStatus::from_static(s3s::dto::ExpirationStatus::ENABLED),
                    expiration: None,
                    abort_incomplete_multipart_upload: None,
                    del_marker_expiration: None,
                    id: Some("time".to_string()),
                    filter: None,
                    noncurrent_version_expiration: None,
                    noncurrent_version_transitions: None,
                    prefix: None,
                    transitions: None,
                },
            ],
            ..Default::default()
        };
        assert!(lifecycle_event_allowed(
            &unknown,
            &Event {
                action: IlmAction::DeleteAction,
                rule_id: "time".to_string(),
                ..Default::default()
            },
            &mixed_filters
        ));
        assert!(lifecycle_event_allowed(
            &unknown,
            &Event {
                action: IlmAction::TransitionAction,
                ..Default::default()
            },
            &BucketLifecycleConfiguration::default()
        ));
        assert!(lifecycle_event_allowed(
            &SizeResolution::Corrupt {
                physical: 12,
                reason: SizeResolutionReason::InvalidDeclaredSize,
            },
            &Event {
                action: IlmAction::DeleteAction,
                ..Default::default()
            },
            &BucketLifecycleConfiguration::default()
        ));
        assert!(!lifecycle_event_allowed(
            &SizeResolution::Corrupt {
                physical: 12,
                reason: SizeResolutionReason::InvalidDeclaredSize,
            },
            &Event {
                action: IlmAction::DeleteAction,
                rule_id: "size".to_string(),
                ..Default::default()
            },
            &size_filtered
        ));
        assert!(lifecycle_rule_has_size_filter(
            &BucketLifecycleConfiguration {
                rules: vec![s3s::dto::LifecycleRule {
                    status: s3s::dto::ExpirationStatus::from_static(s3s::dto::ExpirationStatus::ENABLED),
                    expiration: None,
                    abort_incomplete_multipart_upload: None,
                    del_marker_expiration: None,
                    id: None,
                    filter: Some(s3s::dto::LifecycleRuleFilter {
                        object_size_greater_than: Some(1),
                        ..Default::default()
                    }),
                    noncurrent_version_expiration: None,
                    noncurrent_version_transitions: None,
                    prefix: None,
                    transitions: None,
                }],
                ..Default::default()
            },
            ""
        ));
        assert!(lifecycle_event_allowed(
            &SizeResolution::Known {
                logical: 10,
                physical: 12,
            },
            &Event {
                action: IlmAction::DeleteAllVersionsAction,
                ..Default::default()
            },
            &BucketLifecycleConfiguration::default()
        ));
        assert!(lifecycle_event_allowed(
            &unknown,
            &Event {
                action: IlmAction::DeleteAction,
                rule_id: "time-only".to_string(),
                ..Default::default()
            },
            &BucketLifecycleConfiguration::default()
        ));
    }

    #[tokio::test]
    async fn long_object_size_reconciliation_scope_uses_bounded_identity() {
        let object_name = "o".repeat(600);
        let mut item = scanner_item_with_prefix("");
        item.object_name = object_name.clone();

        let mut metadata = HashMap::new();
        rustfs_utils::http::insert_str(&mut metadata, rustfs_utils::http::SUFFIX_COMPRESSION, "zstd".to_string());
        let object = ObjectInfo {
            bucket: item.bucket.clone(),
            name: object_name.clone(),
            size: 12,
            actual_size: -1,
            version_id: Some(uuid::Uuid::new_v4()),
            user_defined: Arc::new(metadata),
            ..Default::default()
        };
        let mut summary = SizeSummary::default();
        item.apply_actions(vec![object], None, VersioningConfiguration::default(), &mut summary)
            .await;

        let bounded_bucket = bounded_reconciliation_field(&item.bucket);
        let bounded_object = bounded_reconciliation_field(&object_name);
        assert_eq!(summary.reconciliation_scopes[0].bucket, bounded_bucket);
        assert_eq!(summary.reconciliation_scopes[0].object, bounded_object);
        assert_eq!(summary.size_reconciliation[0].object, bounded_object);
        assert_eq!(summary.versions, 1);
        assert_eq!(summary.total_size, 0);
    }
}
