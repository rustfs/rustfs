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

    fn effective_tier(oi: &ObjectInfo) -> &str {
        if oi.transitioned_object.status == crate::TRANSITION_COMPLETE {
            oi.transitioned_object.tier.as_str()
        } else {
            oi.storage_class.as_deref().unwrap_or(crate::storageclass::STANDARD)
        }
    }

    fn tier_name_is_known(tier: &str, tier_names: &[String]) -> bool {
        !tier.is_empty()
            && tier != crate::data_usage_define::UNKNOWN_TIER
            && (tier == crate::storageclass::STANDARD
                || tier == crate::storageclass::RRS
                || tier_names.iter().any(|name| name == tier))
    }

    pub(crate) fn tier_is_known(oi: &ObjectInfo, tier_names: &[String]) -> bool {
        Self::tier_name_is_known(Self::effective_tier(oi), tier_names)
    }

    fn action_requires_known_tier(action: IlmAction) -> bool {
        matches!(
            action,
            IlmAction::TransitionAction
                | IlmAction::TransitionVersionAction
                | IlmAction::DeleteAction
                | IlmAction::DeleteVersionAction
                | IlmAction::DeleteRestoredAction
                | IlmAction::DeleteRestoredVersionAction
                | IlmAction::DeleteAllVersionsAction
                | IlmAction::DelMarkerDeleteAllVersionsAction
        )
    }

    fn action_blocked_by_unknown_tier(
        action: IlmAction,
        oi: &ObjectInfo,
        all_versions_known: bool,
        tier_names: &[String],
        target: &str,
    ) -> bool {
        if !Self::action_requires_known_tier(action) {
            return false;
        }
        !Self::tier_is_known(oi, tier_names)
            || (action.delete_all() && !all_versions_known)
            || (matches!(action, IlmAction::TransitionAction | IlmAction::TransitionVersionAction)
                && !Self::tier_name_is_known(target, tier_names))
    }

    pub async fn apply_actions(
        &mut self,
        object_infos: Vec<ObjectInfo>,
        lock_retention: Option<Arc<ObjectLockConfiguration>>,
        versioning_config: VersioningConfiguration,
        tier_names: &[String],
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

        // `versioning_config` is resolved once per object by the caller
        // (`get_size`) and handed in; only `prefix_enabled` is consulted here.

        let Some(lifecycle) = self.lifecycle.as_ref() else {
            let mut cumulative_size = 0;
            for oi in object_infos.iter() {
                let actual_size = match oi.get_actual_size() {
                    Ok(size) => size,
                    Err(_) => {
                        warn!(
                            target: "rustfs::scanner::folder",
                            event = EVENT_SCANNER_LIFECYCLE_ACTION,
                            component = LOG_COMPONENT_SCANNER,
                            subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                            bucket = %self.bucket,
                            object = %oi.name,
                            state = "size_lookup_failed",
                            "Scanner lifecycle action used fallback size"
                        );
                        continue;
                    }
                };

                let size = self.heal_actions(oi, actual_size, size_summary).await;

                size_summary.actions_accounting(oi, size, actual_size);

                cumulative_size += size;
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
        let mut cumulative_size = 0;
        let mut remaining_versions = object_infos.len();
        let all_versions_known = object_infos
            .iter()
            .all(|candidate| Self::tier_is_known(candidate, tier_names));
        'eventLoop: {
            for (i, event) in events.iter().enumerate() {
                let oi = &object_infos[i];
                let actual_size = match oi.get_actual_size() {
                    Ok(size) => size,
                    Err(_) => {
                        warn!(
                            target: "rustfs::scanner::folder",
                            event = EVENT_SCANNER_LIFECYCLE_ACTION,
                            component = LOG_COMPONENT_SCANNER,
                            subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                            bucket = %self.bucket,
                            object = %oi.name,
                            state = "size_lookup_failed",
                            "Scanner lifecycle action used fallback size"
                        );
                        0
                    }
                };

                let mut size = actual_size;
                let mut account_now = true;

                // A retired/unknown source tier may point at a remote object
                // that cannot be safely deleted or transitioned. Lifecycle
                // evaluation is still useful for accounting, but all
                // side-effecting tier actions fail closed until the registry
                // recognizes the source again.
                if Self::action_blocked_by_unknown_tier(event.action, oi, all_versions_known, tier_names, &event.storage_class) {
                    size = self.heal_actions(oi, actual_size, size_summary).await;
                    size_summary.actions_accounting(oi, size, actual_size);
                    cumulative_size += size;
                    continue;
                }

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
                            PendingScannerAccounting {
                                object: oi,
                                retained_size: actual_size,
                                expired_size: 0,
                            }
                            .apply(size_summary, &mut cumulative_size, false);
                            for retained in object_infos.iter().skip(i + 1) {
                                let retained_size = match retained.get_actual_size() {
                                    Ok(size) => size,
                                    Err(_) => {
                                        warn!(
                                            target: "rustfs::scanner::folder",
                                            event = EVENT_SCANNER_LIFECYCLE_ACTION,
                                            component = LOG_COMPONENT_SCANNER,
                                            subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                                            bucket = %self.bucket,
                                            object = %retained.name,
                                            state = "size_lookup_failed",
                                            "Scanner lifecycle action used fallback size"
                                        );
                                        0
                                    }
                                };
                                PendingScannerAccounting {
                                    object: retained,
                                    retained_size,
                                    expired_size: 0,
                                }
                                .apply(size_summary, &mut cumulative_size, false);
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
                            noncurrent_accounting.push(PendingScannerAccounting {
                                object: oi,
                                retained_size: actual_size,
                                expired_size: 0,
                            });
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
                    cumulative_size += size;
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
                remaining_versions = remaining_versions.saturating_sub(noncurrent_accounting.len());
            }
            for pending in noncurrent_accounting {
                pending.apply(size_summary, &mut cumulative_size, queued);
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
    fn unknown_tier_never_triggers_transition() {
        let object = ObjectInfo {
            storage_class: Some("retired-tier".to_string()),
            ..Default::default()
        };
        let tier_names = ["WARM".to_string()];
        assert!(!ScannerItem::tier_is_known(&object, &tier_names));
        assert!(ScannerItem::action_requires_known_tier(IlmAction::TransitionAction));
        assert!(ScannerItem::action_requires_known_tier(IlmAction::DeleteVersionAction));
        assert!(ScannerItem::action_blocked_by_unknown_tier(
            IlmAction::TransitionAction,
            &object,
            false,
            &tier_names,
            "WARM"
        ));
        assert!(!ScannerItem::action_blocked_by_unknown_tier(
            IlmAction::NoneAction,
            &object,
            false,
            &tier_names,
            "WARM"
        ));

        let known = ObjectInfo {
            storage_class: Some(crate::storageclass::STANDARD.to_string()),
            ..Default::default()
        };
        assert!(ScannerItem::action_blocked_by_unknown_tier(
            IlmAction::DeleteAllVersionsAction,
            &known,
            false,
            &tier_names,
            "WARM"
        ));
        assert!(!ScannerItem::action_blocked_by_unknown_tier(
            IlmAction::TransitionAction,
            &known,
            true,
            &tier_names,
            crate::storageclass::STANDARD
        ));
    }
}
