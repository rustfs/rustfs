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

use crate::heal::{
    progress::{HealProgress, add_bytes, increment_counter},
    resume::{
        CheckpointManager, CheckpointObjectOutcome, CheckpointObjectOutcomeRecord, ReplacementTargetIdentity, ResumeManager,
        ResumeUtils, compose_key, replacement_target_identities_match,
    },
    storage::{HealStorageAPI, next_heal_listing_token},
    task::{demote_to_debug_when, is_missing_object_dir_heal_result, take_failure_log_sample},
};
use crate::{Error, Result};
use futures::{StreamExt, stream::FuturesUnordered};
use metrics::{counter, gauge};
use rustfs_common::heal_channel::{HealOpts, HealRequestSource, HealScanMode};
use rustfs_madmin::heal_commands::HealResultItem;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::{Duration, UNIX_EPOCH};
use tokio::sync::{RwLock, Semaphore};
use tracing::{debug, error, warn};

use super::{DiskStore, EcstoreError};

/// Outcome of classifying an error returned by [`HealStorageAPI::heal_object`].
enum HealObjectOutcome {
    /// The object/version is genuinely absent — nothing left to heal.
    Absent,
    /// A transient infrastructure condition (offline disk, unmet quorum,
    /// contended lock). The object is skipped and retried on a later pass
    /// instead of being recorded as processed.
    Transient,
    /// A real heal failure that should be recorded as failed.
    Failed,
}

fn result_object_size_u64(result: &HealResultItem) -> u64 {
    u64::try_from(result.object_size).unwrap_or(u64::MAX)
}

const NEW_VERSION_SKIP_GRACE_SECS: u64 = 60;
const NANOS_PER_SECOND: i128 = 1_000_000_000;

fn should_skip_new_version(mod_time_unix_nanos: Option<i128>, started_at_secs: u64) -> bool {
    let Some(mod_time_unix_nanos) = mod_time_unix_nanos else {
        return false;
    };
    let cutoff_secs = started_at_secs.saturating_add(NEW_VERSION_SKIP_GRACE_SECS);
    mod_time_unix_nanos > i128::from(cutoff_secs).saturating_mul(NANOS_PER_SECOND)
}

struct PageConcurrencyGuard {
    in_flight: Arc<AtomicUsize>,
    set_label: String,
}

impl PageConcurrencyGuard {
    fn new(in_flight: Arc<AtomicUsize>, set_label: String) -> Self {
        let current = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        gauge!(
            "rustfs_heal_page_concurrency_current",
            "set" => set_label.clone()
        )
        .set(current as f64);
        Self { in_flight, set_label }
    }
}

impl Drop for PageConcurrencyGuard {
    fn drop(&mut self) {
        let current = self.in_flight.fetch_sub(1, Ordering::SeqCst) - 1;
        gauge!(
            "rustfs_heal_page_concurrency_current",
            "set" => self.set_label.clone()
        )
        .set(current as f64);
    }
}

const LOG_COMPONENT_HEAL: &str = "heal";
const LOG_SUBSYSTEM_ERASURE_HEALER: &str = "erasure_healer";
const EVENT_HEAL_ERASURE_RESUME_STATE: &str = "heal_erasure_resume_state";
const EVENT_HEAL_ERASURE_BUCKET_STATE: &str = "heal_erasure_bucket_state";
const EVENT_HEAL_ERASURE_OBJECT_STATE: &str = "heal_erasure_object_state";

/// Erasure Set Healer
pub struct ErasureSetHealer {
    storage: Arc<dyn HealStorageAPI>,
    progress: Arc<RwLock<HealProgress>>,
    cancel_token: tokio_util::sync::CancellationToken,
    disk: DiskStore,
    heal_opts: HealOpts,
    source: HealRequestSource,
    target_endpoints: Arc<[String]>,
    replacement_task_id: Option<String>,
    replacement_target_identities: Option<Arc<[ReplacementTargetIdentity]>>,
}

pub(crate) fn target_outcomes_complete(result: &HealResultItem, target_endpoints: &[String]) -> bool {
    target_endpoints.iter().all(|endpoint| {
        let mut drives = result.after.drives.iter().filter(|drive| drive.endpoint == *endpoint);
        matches!(drives.next(), Some(drive) if drive.state == "ok") && drives.next().is_none()
    })
}

impl ErasureSetHealer {
    fn page_parallel_enabled() -> bool {
        rustfs_utils::get_env_bool(
            rustfs_config::ENV_HEAL_PAGE_PARALLEL_ENABLE,
            rustfs_config::DEFAULT_HEAL_PAGE_PARALLEL_ENABLE,
        )
    }

    fn heal_page_object_concurrency() -> usize {
        rustfs_utils::get_env_usize(
            rustfs_config::ENV_HEAL_PAGE_OBJECT_CONCURRENCY,
            rustfs_config::DEFAULT_HEAL_PAGE_OBJECT_CONCURRENCY,
        )
        .max(1)
    }

    fn effective_heal_page_object_concurrency() -> usize {
        if Self::page_parallel_enabled() {
            Self::heal_page_object_concurrency()
        } else {
            1
        }
    }

    fn effective_heal_page_object_concurrency_for_scan_mode(scan_mode: HealScanMode) -> usize {
        if matches!(scan_mode, HealScanMode::Deep) {
            1
        } else {
            Self::effective_heal_page_object_concurrency()
        }
    }

    fn effective_heal_page_object_concurrency_for_source(source: HealRequestSource, scan_mode: HealScanMode) -> usize {
        if matches!(source, HealRequestSource::AutoHeal) {
            1
        } else {
            Self::effective_heal_page_object_concurrency_for_scan_mode(scan_mode)
        }
    }

    /// Classify an error returned by [`HealStorageAPI::heal_object`].
    ///
    /// Both the inner `Ok((_, Some(err)))` and the outer `Err(err)` produced by
    /// `heal_object` wrap `Error::Storage(StorageError)`, so match on that.
    fn classify_heal_object_error(err: &Error) -> HealObjectOutcome {
        let Error::Storage(se) = err else {
            return HealObjectOutcome::Failed;
        };

        // Genuine object/version absence: nothing left to heal, treat as handled.
        if matches!(
            se,
            EcstoreError::FileNotFound
                | EcstoreError::FileVersionNotFound
                | EcstoreError::ObjectNotFound(_, _)
                | EcstoreError::VersionNotFound(_, _, _)
        ) {
            return HealObjectOutcome::Absent;
        }

        // Transient infrastructure conditions — skip and retry on a later pass.
        // NOTE: do NOT use `StorageError::is_not_found()` here: it lumps
        // `DiskNotFound`/`VolumeNotFound` together with object absence, which is
        // exactly the conflation that previously let an offline drive be recorded
        // as "healed/absent" and permanently skipped (backlog#856 / #799 B7).
        if se.is_quorum_error()
            || matches!(
                se,
                EcstoreError::DiskNotFound
                    | EcstoreError::VolumeNotFound
                    | EcstoreError::SlowDown
                    | EcstoreError::OperationCanceled
            )
        {
            return HealObjectOutcome::Transient;
        }

        HealObjectOutcome::Failed
    }

    pub fn new(
        storage: Arc<dyn HealStorageAPI>,
        progress: Arc<RwLock<HealProgress>>,
        cancel_token: tokio_util::sync::CancellationToken,
        disk: DiskStore,
        heal_opts: HealOpts,
        source: HealRequestSource,
    ) -> Self {
        Self {
            storage,
            progress,
            cancel_token,
            disk,
            heal_opts,
            source,
            target_endpoints: Vec::new().into(),
            replacement_task_id: None,
            replacement_target_identities: None,
        }
    }

    pub(crate) fn with_replacement_targets(
        mut self,
        mut target_endpoints: Vec<String>,
        replacement_task_id: Option<String>,
    ) -> Self {
        target_endpoints.sort_unstable();
        target_endpoints.dedup();
        self.target_endpoints = target_endpoints.into();
        self.replacement_task_id = replacement_task_id;
        self
    }

    pub(crate) fn with_replacement_identity_fence(
        mut self,
        replacement_target_identities: Option<Vec<ReplacementTargetIdentity>>,
    ) -> Self {
        self.replacement_target_identities = replacement_target_identities.map(Into::into);
        self
    }

    async fn verify_replacement_identity_fence(&self, stage: &str) -> Result<()> {
        let Some(expected_identities) = self.replacement_target_identities.as_ref() else {
            return Ok(());
        };
        let actual_identities = self.storage.replacement_target_identities(&self.target_endpoints).await?;
        if replacement_target_identities_match(expected_identities, &actual_identities) {
            return Ok(());
        }

        Err(Error::TaskExecutionFailed {
            message: format!("Replacement target changed during {stage}"),
        })
    }

    /// execute erasure set heal with resume
    #[tracing::instrument(skip(self, buckets), fields(set_disk_id = %set_disk_id, bucket_count = buckets.len()))]
    #[hotpath::measure]
    pub async fn heal_erasure_set(&self, buckets: &[String], set_disk_id: &str) -> Result<()> {
        debug!(
            target: "rustfs::heal::erasure_healer",
            event = EVENT_HEAL_ERASURE_RESUME_STATE,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_ERASURE_HEALER,
            set_disk_id,
            bucket_count = buckets.len(),
            state = "started",
            "Erasure set heal started"
        );

        // 1. generate or get task id
        let task_id = self.get_or_create_task_id(set_disk_id).await?;

        // 2. initialize or resume resume state
        let (resume_manager, checkpoint_manager) = self.initialize_resume_state(&task_id, set_disk_id, buckets).await?;

        // 3. execute heal with resume
        let result = self
            .execute_heal_with_resume(buckets, set_disk_id, &resume_manager, &checkpoint_manager)
            .await;

        result?;
        self.verify_replacement_identity_fence("completion").await?;

        if self.replacement_task_id.is_some() {
            // A replacement marker must outlive the successful data scan. The
            // task clears that owner marker before deleting these artifacts.
            resume_manager.mark_replacement_completed_and_verified().await?;
            return Ok(());
        }

        checkpoint_manager.cleanup().await?;
        resume_manager.cleanup().await?;
        Ok(())
    }

    /// get or create task id
    async fn get_or_create_task_id(&self, set_disk_id: &str) -> Result<String> {
        if let Some(task_id) = &self.replacement_task_id {
            let manager = ResumeManager::load_replacement_intent(self.disk.clone(), task_id).await?;
            let state = manager.get_state().await;
            if !state.completed
                && state.set_disk_id == set_disk_id
                && state.replacement_targets.as_slice() == self.target_endpoints.as_ref()
                && state.replacement_generation.as_deref() == Some(task_id.as_str())
            {
                return Ok(task_id.clone());
            }
            return Err(Error::TaskExecutionFailed {
                message: format!("Replacement resume intent does not match task {task_id}"),
            });
        }

        // check if there are resumable tasks
        let resumable_tasks = ResumeUtils::get_resumable_tasks(&self.disk).await?;

        for task_id in resumable_tasks {
            match ResumeManager::load_from_disk(self.disk.clone(), &task_id).await {
                Ok(manager) => {
                    let state = manager.get_state().await;
                    if !state.completed
                        && state.set_disk_id == set_disk_id
                        && state.replacement_targets.as_slice() == self.target_endpoints.as_ref()
                        && ResumeUtils::can_resume_task(&self.disk, &task_id).await
                    {
                        debug!(
                            target: "rustfs::heal::erasure_healer",
                            event = EVENT_HEAL_ERASURE_RESUME_STATE,
                            component = LOG_COMPONENT_HEAL,
                            subsystem = LOG_SUBSYSTEM_ERASURE_HEALER,
                            task_id,
                            set_disk_id,
                            state = "resume_found",
                            "Erasure set resume selected"
                        );
                        return Ok(task_id);
                    }
                }
                Err(e) => {
                    warn!(
                        target: "rustfs::heal::erasure_healer",
                        event = EVENT_HEAL_ERASURE_RESUME_STATE,
                        component = LOG_COMPONENT_HEAL,
                        subsystem = LOG_SUBSYSTEM_ERASURE_HEALER,
                        task_id,
                        set_disk_id,
                        state = "resume_load_failed",
                        error = %e,
                        "Erasure set resume state load failed"
                    );
                }
            }
        }

        // create new task id
        let task_id = ResumeUtils::generate_task_id();
        debug!(
            target: "rustfs::heal::erasure_healer",
            event = EVENT_HEAL_ERASURE_RESUME_STATE,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_ERASURE_HEALER,
            task_id,
            set_disk_id,
            state = "resume_created",
            "Erasure set resume state created"
        );
        Ok(task_id)
    }

    /// initialize or resume resume state
    async fn initialize_resume_state(
        &self,
        task_id: &str,
        set_disk_id: &str,
        buckets: &[String],
    ) -> Result<(ResumeManager, CheckpointManager)> {
        if self.replacement_task_id.is_none() && CheckpointManager::is_blocked(&self.disk, task_id).await {
            return Err(Error::TaskExecutionFailed {
                message: format!("Resume task {task_id} has a blocked checkpoint"),
            });
        }
        // check if resume state exists
        let has_resume_state = if self.replacement_task_id.is_some() {
            ResumeManager::has_replacement_intent(&self.disk, task_id).await
        } else {
            ResumeManager::has_resume_state(&self.disk, task_id).await
        };
        if has_resume_state {
            debug!(
                target: "rustfs::heal::erasure_healer",
                event = EVENT_HEAL_ERASURE_RESUME_STATE,
                component = LOG_COMPONENT_HEAL,
                subsystem = LOG_SUBSYSTEM_ERASURE_HEALER,
                task_id,
                set_disk_id,
                state = "loading_existing",
                "Erasure set resume state loading"
            );

            let resume_manager = if self.replacement_task_id.is_some() {
                ResumeManager::load_replacement_intent(self.disk.clone(), task_id).await?
            } else {
                ResumeManager::load_from_disk(self.disk.clone(), task_id).await?
            };
            let checkpoint_manager = if CheckpointManager::has_checkpoint(&self.disk, task_id).await {
                CheckpointManager::load_from_disk(self.disk.clone(), task_id).await?
            } else {
                CheckpointManager::new(self.disk.clone(), task_id.to_string()).await?
            };

            let state = resume_manager.get_state().await;
            if state.retry_count > 0
                && state.completed_buckets.is_empty()
                && state.resume_cursor.is_none()
                && state.processed_objects == 0
                && state.successful_objects == 0
                && state.failed_objects == 0
                && state.skipped_objects == 0
                && state.skipped_new_versions == 0
                && state.skipped_ilm_expired == 0
                && state.processed_bytes == 0
            {
                // schedule_retry persists the authoritative resume reset before
                // resetting the checkpoint. Reapply the checkpoint reset after
                // a crash in that window so stale positions cannot skip work.
                checkpoint_manager.reset_for_retry().await?;
            }

            Ok((resume_manager, checkpoint_manager))
        } else {
            debug!(
                target: "rustfs::heal::erasure_healer",
                event = EVENT_HEAL_ERASURE_RESUME_STATE,
                component = LOG_COMPONENT_HEAL,
                subsystem = LOG_SUBSYSTEM_ERASURE_HEALER,
                task_id,
                set_disk_id,
                state = "creating_new",
                "Erasure set resume created"
            );

            let resume_manager = ResumeManager::new(
                self.disk.clone(),
                task_id.to_string(),
                "erasure_set".to_string(),
                set_disk_id.to_string(),
                buckets.to_vec(),
            )
            .await?;
            resume_manager
                .set_replacement_targets(self.target_endpoints.as_ref().to_vec())
                .await?;

            let checkpoint_manager = CheckpointManager::new(self.disk.clone(), task_id.to_string()).await?;

            Ok((resume_manager, checkpoint_manager))
        }
    }

    /// execute heal with resume
    async fn execute_heal_with_resume(
        &self,
        buckets: &[String],
        set_disk_id: &str,
        resume_manager: &ResumeManager,
        checkpoint_manager: &CheckpointManager,
    ) -> Result<()> {
        // 1. get current state
        let state = resume_manager.get_state().await;
        let checkpoint = checkpoint_manager.get_checkpoint().await;

        debug!(
            target: "rustfs::heal::erasure_healer",
            event = EVENT_HEAL_ERASURE_RESUME_STATE,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_ERASURE_HEALER,
            set_disk_id,
            current_bucket_index = checkpoint.current_bucket_index,
            current_object_index = checkpoint.current_object_index,
            state = "resuming",
            "Erasure set resumed"
        );

        // 2. initialize progress
        self.initialize_progress(buckets, &state).await;
        let (baseline_known, baseline_count, baseline_size, baseline_generation) = {
            let baseline = self.progress.read().await;
            (
                baseline.baseline_known,
                baseline.objects_total_count,
                baseline.objects_total_size,
                baseline.baseline_generation,
            )
        };
        if baseline_known {
            resume_manager
                .set_progress_baseline(baseline_count, baseline_size, baseline_generation)
                .await?;
            checkpoint_manager
                .set_progress_baseline(baseline_count, baseline_size, baseline_generation)
                .await?;
        }

        // 3. continue from checkpoint
        let current_bucket_index = checkpoint.current_bucket_index;
        let mut current_object_index = checkpoint.current_object_index;

        let mut processed_objects = state.processed_objects;
        let mut successful_objects = state.successful_objects;
        let mut failed_objects = state.failed_objects;
        let mut skipped_objects = state.skipped_objects;
        let checkpoint_has_progress = checkpoint.baseline_known
            || checkpoint.successful_objects > 0
            || checkpoint.failed_object_count > 0
            || checkpoint.skipped_object_count > 0
            || checkpoint.skipped_new_versions > 0
            || checkpoint.skipped_ilm_expired > 0
            || checkpoint.processed_bytes > 0
            || checkpoint.total_objects > 0
            || checkpoint.total_bytes > 0
            || checkpoint.baseline_generation.is_some()
            || checkpoint.counter_unknown;
        let checkpoint_generation_mismatch = checkpoint.baseline_known && checkpoint.baseline_generation != baseline_generation;
        let mut restored_counter_unknown = state.counter_unknown || checkpoint.counter_unknown;
        if checkpoint_has_progress {
            successful_objects = checkpoint.successful_objects;
            failed_objects = checkpoint.failed_object_count;
            skipped_objects = checkpoint.skipped_object_count;
            let restored_processed_objects = successful_objects
                .checked_add(failed_objects)
                .and_then(|value| value.checked_add(skipped_objects))
                .and_then(|value| value.checked_add(checkpoint.skipped_new_versions))
                .and_then(|value| value.checked_add(checkpoint.skipped_ilm_expired));
            let checkpoint_counter_overflow = restored_processed_objects.is_none();
            restored_counter_unknown |= checkpoint_counter_overflow;
            processed_objects = restored_processed_objects.unwrap_or(u64::MAX);
            let mut progress = self.progress.write().await;
            progress.objects_scanned = processed_objects;
            progress.objects_healed = successful_objects;
            progress.objects_failed = failed_objects;
            progress.skipped_objects = skipped_objects;
            progress.skipped_new_versions = checkpoint.skipped_new_versions;
            progress.skipped_ilm_expired = checkpoint.skipped_ilm_expired;
            if checkpoint.baseline_known && !checkpoint_generation_mismatch {
                progress.objects_total_count = checkpoint.total_objects;
                progress.objects_total_size = checkpoint.total_bytes;
                progress.baseline_generation = checkpoint.baseline_generation;
                progress.baseline_known = true;
            }
            progress.bytes_processed = checkpoint.processed_bytes;
            progress.counter_unknown = state.counter_unknown || checkpoint.counter_unknown;
            progress.refresh_progress_percentage();
            if checkpoint_generation_mismatch || checkpoint_counter_overflow || progress.counter_unknown {
                progress.mark_unknown();
            }
        }
        if checkpoint_generation_mismatch {
            restored_counter_unknown = true;
        }
        if restored_counter_unknown {
            checkpoint_manager.mark_counter_unknown().await?;
            resume_manager.mark_counter_unknown().await?;
        }
        let mut failed_buckets = 0u64;

        // 4. process remaining buckets
        for (bucket_idx, bucket) in buckets.iter().enumerate().skip(current_bucket_index) {
            // check if completed
            if state.completed_buckets.contains(bucket) {
                checkpoint_manager.complete_bucket(bucket_idx.saturating_add(1)).await?;
                current_object_index = 0;
                continue;
            }

            // update current bucket
            resume_manager.set_current_item(Some(bucket.clone()), None).await?;

            // process objects in bucket
            let bucket_result = self
                .heal_bucket_with_resume(
                    bucket,
                    set_disk_id,
                    bucket_idx,
                    &mut current_object_index,
                    &mut processed_objects,
                    &mut successful_objects,
                    &mut failed_objects,
                    &mut skipped_objects,
                    resume_manager,
                    checkpoint_manager,
                    state.start_time,
                )
                .await;

            if matches!(bucket_result, Err(Error::TaskCancelled | Error::TaskTimeout)) {
                return bucket_result;
            }

            // update progress
            let progress_snapshot = self.progress.read().await;
            let bytes_processed = progress_snapshot.bytes_processed;
            let skipped_new_versions = progress_snapshot.skipped_new_versions;
            let skipped_ilm_expired = progress_snapshot.skipped_ilm_expired;
            let counter_unknown = progress_snapshot.counter_unknown;
            drop(progress_snapshot);
            // The checkpoint is the recovery authority for object progress.
            // Publish its counters and fence before the resume summary so a
            // crash between the two stores cannot make recovery select newer
            // summary bytes with an older checkpoint ledger.
            if counter_unknown {
                checkpoint_manager.mark_counter_unknown().await?;
            }
            checkpoint_manager
                .update_progress(successful_objects, failed_objects, skipped_objects, bytes_processed)
                .await?;
            checkpoint_manager
                .set_skipped_version_counts(skipped_new_versions, skipped_ilm_expired)
                .await?;
            checkpoint_manager.update_position(bucket_idx, current_object_index).await?;
            resume_manager
                .update_progress_with_bytes(
                    processed_objects,
                    successful_objects,
                    failed_objects,
                    skipped_objects,
                    bytes_processed,
                )
                .await?;
            resume_manager
                .set_skipped_version_counts(skipped_new_versions, skipped_ilm_expired)
                .await?;
            if counter_unknown {
                resume_manager.mark_counter_unknown().await?;
            }

            // check cancel status
            if self.cancel_token.is_cancelled() {
                warn!(
                    target: "rustfs::heal::erasure_healer",
                    event = EVENT_HEAL_ERASURE_RESUME_STATE,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_ERASURE_HEALER,
                    set_disk_id,
                    state = "cancelled",
                    "Erasure set heal cancelled"
                );
                return Err(Error::TaskCancelled);
            }

            // process bucket result
            match bucket_result {
                Ok(_) => {
                    resume_manager.complete_bucket(bucket).await?;
                    checkpoint_manager.complete_bucket(bucket_idx.saturating_add(1)).await?;
                    debug!(
                        target: "rustfs::heal::erasure_healer",
                        event = EVENT_HEAL_ERASURE_BUCKET_STATE,
                        component = LOG_COMPONENT_HEAL,
                        subsystem = LOG_SUBSYSTEM_ERASURE_HEALER,
                        set_disk_id,
                        bucket,
                        state = "completed",
                        "Erasure set bucket completed"
                    );
                }
                Err(err @ Error::TaskCancelled) | Err(err @ Error::TaskTimeout) => return Err(err),
                Err(e) => {
                    failed_buckets = failed_buckets.saturating_add(1);
                    error!(
                        target: "rustfs::heal::erasure_healer",
                        event = EVENT_HEAL_ERASURE_BUCKET_STATE,
                        component = LOG_COMPONENT_HEAL,
                        subsystem = LOG_SUBSYSTEM_ERASURE_HEALER,
                        set_disk_id,
                        bucket,
                        state = "failed",
                        error = %e,
                        "Erasure set bucket heal failed"
                    );
                    // A single durable cursor and ledger cannot safely preserve
                    // this bucket while processing a later one.
                    break;
                }
            }

            // reset object index
            current_object_index = 0;
        }

        // 5. finalize. Only declare the set healed when nothing failed AND
        // nothing was transiently skipped — otherwise the resume/checkpoint
        // state must survive so the failed/skipped versions are retried instead
        // of being silently marked "completed" and discarded
        // (backlog#855 / #799 B6 / #1033). Transient skips (unmet quorum,
        // DiskNotFound, SlowDown, cancellation) are recorded in the checkpoint's
        // skipped set, which suppresses them on resume; treating a skip pass as
        // complete would discard that set and never re-heal the versions. The
        // skip may be because the disk is still down, so these are deferred to a
        // later heal cycle via the same bounded-retry mechanism as failures —
        // never hot-retried in place here.
        if failed_objects > 0 || skipped_objects > 0 || failed_buckets > 0 {
            if self.replacement_task_id.is_some() && resume_manager.schedule_retry().await? {
                checkpoint_manager.reset_for_retry().await?;
                return Err(Error::transient_skip(format!(
                    "Replacement erasure set heal incomplete: {failed_buckets} bucket(s) failed, {failed_objects} object(s) failed, {skipped_objects} object(s) skipped; retry scheduled"
                )));
            }
            if resume_manager.schedule_retry().await? {
                // Both persistence layers must be reset together: schedule_retry
                // rewinds the resume state (cursor + counters), and the
                // checkpoint's per-version dedup + skipped sets and position must
                // be cleared in lockstep, or the retry would skip the very
                // versions it is meant to re-heal.
                checkpoint_manager.reset_for_retry().await?;
                // Retry budget remains: state has been reset for a full re-scan.
                // Return Err so `heal_erasure_set` preserves (does not clean up)
                // the resume/checkpoint state and the caller keeps the healing
                // markers for the next heal run.
                warn!(
                    target: "rustfs::heal::erasure_healer",
                    event = EVENT_HEAL_ERASURE_RESUME_STATE,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_ERASURE_HEALER,
                    set_disk_id,
                    failed_buckets,
                    failed_objects,
                    skipped_objects,
                    state = "retry_scheduled",
                    "Erasure set heal pass finished with unhealed versions; scheduled full re-heal retry"
                );
                return Err(Error::transient_skip(format!(
                    "Erasure set heal incomplete: {failed_buckets} bucket(s) failed, {failed_objects} object(s) failed, {skipped_objects} object(s) skipped; retry scheduled"
                )));
            }

            // Retry budget exhausted: keep the resume/checkpoint state while
            // the replacement marker remains. A later repair must retain the
            // durable evidence of the incomplete generation instead of
            // starting from an indistinguishable blank state.
            error!(
                target: "rustfs::heal::erasure_healer",
                event = EVENT_HEAL_ERASURE_RESUME_STATE,
                component = LOG_COMPONENT_HEAL,
                subsystem = LOG_SUBSYSTEM_ERASURE_HEALER,
                set_disk_id,
                failed_buckets,
                failed_objects,
                skipped_objects,
                state = "failed_after_retries",
                "Erasure set heal exhausted retries with unrecovered versions"
            );
            return Err(Error::other(format!(
                "Erasure set heal exhausted retries with {failed_buckets} bucket(s) failed, {failed_objects} object(s) failed, {skipped_objects} object(s) skipped"
            )));
        }

        // No failures — ordinary heals are complete now. Replacement heals
        // atomically transition to Verified after the terminal identity fence.
        if self.replacement_task_id.is_none() {
            resume_manager.mark_completed().await?;
        }

        debug!(
            target: "rustfs::heal::erasure_healer",
            event = EVENT_HEAL_ERASURE_RESUME_STATE,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_ERASURE_HEALER,
            set_disk_id,
            state = "completed",
            "Erasure set completed"
        );
        Ok(())
    }

    /// heal single bucket with resume
    #[allow(clippy::too_many_arguments)]
    #[tracing::instrument(skip(self, current_object_index, processed_objects, successful_objects, failed_objects, skipped_objects, resume_manager, checkpoint_manager), fields(bucket = %bucket, bucket_index = bucket_index))]
    async fn heal_bucket_with_resume(
        &self,
        bucket: &str,
        set_disk_id: &str,
        bucket_index: usize,
        current_object_index: &mut usize,
        processed_objects: &mut u64,
        successful_objects: &mut u64,
        failed_objects: &mut u64,
        skipped_objects: &mut u64,
        resume_manager: &ResumeManager,
        checkpoint_manager: &CheckpointManager,
        started_at_secs: u64,
    ) -> Result<()> {
        debug!(
            target: "rustfs::heal::erasure_healer",
            event = EVENT_HEAL_ERASURE_BUCKET_STATE,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_ERASURE_HEALER,
            set_disk_id,
            bucket,
            bucket_index,
            current_object_index = *current_object_index,
            state = "started",
            "Erasure set bucket started"
        );

        // 1. get bucket info
        let _bucket_info = match self.storage.get_bucket_info(bucket).await? {
            Some(info) => info,
            None => {
                warn!(
                    target: "rustfs::heal::erasure_healer",
                    event = EVENT_HEAL_ERASURE_BUCKET_STATE,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_ERASURE_HEALER,
                    set_disk_id,
                    bucket,
                    state = "missing",
                    "Erasure set bucket heal skipped because bucket is missing"
                );
                return Ok(());
            }
        };

        // 2. process object VERSIONS with pagination to avoid loading everything into memory.
        //    The continuation token is the authoritative opaque (marker, version_marker)
        //    cursor seeded from the resume state, so a resume continues exactly where the
        //    previous pass stopped — including mid-object across version pages.
        let mut continuation_token: Option<String> = resume_manager.resume_cursor().await;
        let mut global_obj_idx = 0usize;
        // Anti-loop guard: the last (name, version_id) actually observed on the
        // previous page. Comparing decoded item identities (not raw tokens)
        // detects a non-advancing cursor even though next_marker embeds a
        // transient cache id that changes between identical pages.
        let mut previous_page_last: Option<(String, Option<String>)> = None;
        let page_concurrency_limit =
            Self::effective_heal_page_object_concurrency_for_source(self.source, self.heal_opts.scan_mode);
        let in_flight = Arc::new(AtomicUsize::new(0));
        // Per-bucket sample caps for per-object warn! lines: a flapping rebuild
        // disk can fail/skip hundreds of thousands of versions in one sweep, so
        // only the first few occurrences warn and the rest demote to debug!.
        // The end-of-pass summary reports the full failed/skipped counts.
        let mut transient_skip_samples_logged = 0_u64;
        let mut failure_samples_logged = 0_u64;
        let mut bytes_processed = self.progress.read().await.bytes_processed;

        // backlog#920: select the per-erasure-set DISK-WALK union enumerator when
        // the scan is Deep OR the request came from AutoHeal — these are the paths
        // that must repair sub-quorum-but-reconstructable versions. Every other
        // (Normal, non-AutoHeal) request keeps the unchanged B5 read-quorum path,
        // which stays the default.
        let use_disk_walk =
            matches!(self.heal_opts.scan_mode, HealScanMode::Deep) || matches!(self.source, HealRequestSource::AutoHeal);
        let lifecycle_expiry_context = self.storage.load_heal_lifecycle_expiry_context(bucket).await?;
        let include_lifecycle_object_info = lifecycle_expiry_context.is_some();

        loop {
            self.verify_replacement_identity_fence("page scan").await?;
            // Get one page of object versions
            let (objects, next_token, is_truncated) = if use_disk_walk {
                self.storage
                    .list_versions_for_heal_page_disk_walk(
                        set_disk_id,
                        bucket,
                        "",
                        continuation_token.as_deref(),
                        include_lifecycle_object_info,
                    )
                    .await?
            } else {
                self.storage
                    .list_objects_for_heal_page(bucket, "", continuation_token.as_deref(), include_lifecycle_object_info)
                    .await?
            };
            let page_is_empty = objects.is_empty();
            let checkpoint = checkpoint_manager.get_checkpoint().await;
            let page_resume_index = *current_object_index;
            let semaphore = Arc::new(Semaphore::new(page_concurrency_limit));
            let mut page_tasks = FuturesUnordered::new();
            let mut completed_in_page = 0usize;

            // Capture the last version identity of this page for the anti-loop guard.
            let page_last = objects.last().map(|item| (item.name.clone(), item.version_id.clone()));

            for item in objects {
                // current_object_index is now only a progress metric; the cursor
                // drives resume, so we no longer skip by position.
                global_obj_idx += 1;

                // Per-version dedup identity — the single canonical key.
                let key = compose_key(&item.name, item.version_id.as_deref());
                if checkpoint.processed_objects.contains(&key)
                    || checkpoint.failed_objects.contains(&key)
                    || checkpoint.skipped_objects.contains(&key)
                {
                    continue;
                }

                if should_skip_new_version(item.mod_time_unix_nanos, started_at_secs) {
                    let counter_ok = increment_counter(processed_objects);
                    completed_in_page = completed_in_page.saturating_add(1);
                    counter!("rustfs_heal_skipped_new_versions_total").increment(1);
                    let (outcome_record, counter_unknown) = {
                        let mut progress = self.progress.write().await;
                        progress.record_skipped_new_version();
                        progress.set_current_object(Some(format!("skipped_new: {bucket}/{}", item.name)));
                        progress.update_object_progress(
                            *processed_objects,
                            *successful_objects,
                            *failed_objects,
                            *skipped_objects,
                            bytes_processed,
                        );
                        if !counter_ok {
                            progress.mark_unknown();
                        }
                        (
                            CheckpointObjectOutcomeRecord {
                                object: key,
                                outcome: CheckpointObjectOutcome::Processed,
                                successful: progress.objects_healed,
                                failed: progress.objects_failed,
                                skipped: progress.skipped_objects,
                                bytes: progress.bytes_processed,
                                skipped_new_versions: progress.skipped_new_versions,
                                skipped_ilm_expired: progress.skipped_ilm_expired,
                                counter_unknown: progress.counter_unknown,
                            },
                            progress.counter_unknown,
                        )
                    };
                    checkpoint_manager.record_object_outcome(outcome_record).await?;
                    if counter_unknown {
                        resume_manager.mark_counter_unknown().await?;
                    }
                    debug!(
                        target: "rustfs::heal::erasure_healer",
                        event = EVENT_HEAL_ERASURE_OBJECT_STATE,
                        component = LOG_COMPONENT_HEAL,
                        subsystem = LOG_SUBSYSTEM_ERASURE_HEALER,
                        set_disk_id,
                        bucket,
                        object = %item.name,
                        version_id = ?item.version_id,
                        state = "skipped_new_version",
                        "Erasure set object version skipped because it was written after heal started"
                    );
                    if completed_in_page.is_multiple_of(100) {
                        checkpoint_manager.update_position(bucket_index, page_resume_index).await?;
                    }
                    continue;
                }

                if let Some(context) = lifecycle_expiry_context.as_ref()
                    && self
                        .storage
                        .enqueue_heal_lifecycle_expiry(
                            context,
                            bucket,
                            &item.name,
                            item.version_id.as_deref(),
                            item.lifecycle_object_info.as_ref(),
                        )
                        .await?
                {
                    let counter_ok = increment_counter(processed_objects);
                    completed_in_page = completed_in_page.saturating_add(1);
                    counter!("rustfs_heal_skipped_ilm_expired_total").increment(1);
                    let (outcome_record, counter_unknown) = {
                        let mut progress = self.progress.write().await;
                        progress.record_skipped_ilm_expired();
                        progress.set_current_object(Some(format!("skipped_ilm: {bucket}/{}", item.name)));
                        progress.update_object_progress(
                            *processed_objects,
                            *successful_objects,
                            *failed_objects,
                            *skipped_objects,
                            bytes_processed,
                        );
                        if !counter_ok {
                            progress.mark_unknown();
                        }
                        (
                            CheckpointObjectOutcomeRecord {
                                object: key,
                                outcome: CheckpointObjectOutcome::Processed,
                                successful: progress.objects_healed,
                                failed: progress.objects_failed,
                                skipped: progress.skipped_objects,
                                bytes: progress.bytes_processed,
                                skipped_new_versions: progress.skipped_new_versions,
                                skipped_ilm_expired: progress.skipped_ilm_expired,
                                counter_unknown: progress.counter_unknown,
                            },
                            progress.counter_unknown,
                        )
                    };
                    checkpoint_manager.record_object_outcome(outcome_record).await?;
                    if counter_unknown {
                        resume_manager.mark_counter_unknown().await?;
                    }
                    debug!(
                        target: "rustfs::heal::erasure_healer",
                        event = EVENT_HEAL_ERASURE_OBJECT_STATE,
                        component = LOG_COMPONENT_HEAL,
                        subsystem = LOG_SUBSYSTEM_ERASURE_HEALER,
                        set_disk_id,
                        bucket,
                        object = %item.name,
                        version_id = ?item.version_id,
                        state = "skipped_ilm_expired",
                        "Erasure set object version skipped because lifecycle expiry was queued"
                    );
                    if completed_in_page.is_multiple_of(100) {
                        checkpoint_manager.update_position(bucket_index, page_resume_index).await?;
                    }
                    continue;
                }

                resume_manager
                    .set_current_item(Some(bucket.to_string()), Some(item.name.clone()))
                    .await?;

                let storage = self.storage.clone();
                let bucket_name = bucket.to_string();
                let object_name = item.name.clone();
                let version_id = item.version_id.clone();
                let dedup_key = key;
                let cancel_token = self.cancel_token.clone();
                let in_flight = in_flight.clone();
                let set_label = set_disk_id.to_string();
                let heal_opts = self.heal_opts;
                let semaphore = semaphore.clone();
                let target_endpoints = self.target_endpoints.clone();
                let replacement_commit_evidence_required = self.replacement_task_id.is_some();

                page_tasks.push(async move {
                    let permit = semaphore
                        .clone()
                        .acquire_owned()
                        .await
                        .map_err(|e| Error::other(format!("Failed to acquire page concurrency permit: {e}")));

                    let _permit = match permit {
                        Ok(permit) => permit,
                        Err(err) => return (dedup_key, object_name, version_id, (0, Err(err))),
                    };

                    let _in_flight_guard = PageConcurrencyGuard::new(in_flight, set_label);

                    // Always go through heal_object. Genuine absence flows through
                    // heal_object -> FileVersionNotFound/FileNotFound ->
                    // classify_heal_object_error -> Absent, so gone versions are
                    // recorded as skipped-ok rather than failed. The delete-marker
                    // vs data path is chosen internally in ops/heal.rs.
                    let result = if cancel_token.is_cancelled() {
                        (0, Err(Error::TaskCancelled))
                    } else {
                        match storage
                            .heal_object(&bucket_name, &object_name, version_id.as_deref(), &heal_opts)
                            .await
                        {
                            Ok((result, None))
                                if target_outcomes_complete(&result, &target_endpoints) =>
                            {
                                let object_size = result_object_size_u64(&result);
                                if !replacement_commit_evidence_required {
                                    (object_size, Ok(true))
                                } else {
                                    match storage
                                        .replacement_targets_have_version(
                                            &bucket_name,
                                            &object_name,
                                            version_id.as_deref(),
                                            &heal_opts,
                                            &target_endpoints,
                                        )
                                        .await
                                    {
                                        Ok(true) => (object_size, Ok(true)),
                                        Ok(false) => (object_size, Err(Error::transient_skip(format!(
                                            "Skipped heal for {bucket_name}/{object_name} because replacement target readback did not confirm the committed version"
                                        )))),
                                        Err(err) => (object_size, Err(Error::transient_skip(format!(
                                            "Skipped heal for {bucket_name}/{object_name} because replacement target readback failed: {err}"
                                        )))),
                                    }
                                }
                            },
                            Ok((result, None)) if !target_endpoints.is_empty() => (
                                result_object_size_u64(&result),
                                Err(Error::transient_skip(format!(
                                    "Skipped heal for {bucket_name}/{object_name} because a replacement target was not committed"
                                ))),
                            ),
                            Ok((result, None)) => (result_object_size_u64(&result), Ok(true)),
                            Ok((result, Some(err))) if is_missing_object_dir_heal_result(&object_name, &err) => {
                                (result_object_size_u64(&result), Ok(false))
                            }
                            Ok((result, Some(err))) => {
                                let object_size = result_object_size_u64(&result);
                                match Self::classify_heal_object_error(&err) {
                                    HealObjectOutcome::Absent => (object_size, Ok(false)),
                                    HealObjectOutcome::Transient => (object_size, Err(Error::transient_skip(format!(
                                        "Skipped heal for {bucket_name}/{object_name} due to transient error: {err}"
                                    )))),
                                    HealObjectOutcome::Failed => (object_size, Err(err)),
                                }
                            }
                            Err(err) => match Self::classify_heal_object_error(&err) {
                                HealObjectOutcome::Absent => (0, Ok(false)),
                                HealObjectOutcome::Transient => (0, Err(Error::transient_skip(format!(
                                    "Skipped heal for {bucket_name}/{object_name} due to transient error: {err}"
                                )))),
                                HealObjectOutcome::Failed => (0, Err(err)),
                            },
                        }
                    };

                    (dedup_key, object_name, version_id, result)
                });
            }

            while let Some((key, object, version_id, result)) = page_tasks.next().await {
                let (object_size, result) = result;
                let mut telemetry_unknown = false;
                let checkpoint_outcome = match result {
                    Ok(true) => {
                        telemetry_unknown |= !increment_counter(successful_objects);
                        telemetry_unknown |= !add_bytes(&mut bytes_processed, object_size);
                        debug!(
                            target: "rustfs::heal::erasure_healer",
                            event = EVENT_HEAL_ERASURE_OBJECT_STATE,
                            component = LOG_COMPONENT_HEAL,
                            subsystem = LOG_SUBSYSTEM_ERASURE_HEALER,
                            set_disk_id,
                            bucket,
                            object = %object,
                            version_id = ?version_id,
                            state = "healed",
                            "Erasure set object healed"
                        );
                        CheckpointObjectOutcome::Processed
                    }
                    Ok(false) => {
                        telemetry_unknown |= !increment_counter(successful_objects);
                        telemetry_unknown |= !add_bytes(&mut bytes_processed, object_size);
                        debug!(
                            target: "rustfs::heal::erasure_healer",
                            event = EVENT_HEAL_ERASURE_OBJECT_STATE,
                            component = LOG_COMPONENT_HEAL,
                            subsystem = LOG_SUBSYSTEM_ERASURE_HEALER,
                            set_disk_id,
                            bucket,
                            object = %object,
                            version_id = ?version_id,
                            state = "missing_treated_as_ok",
                            "Erasure set missing object treated as ok"
                        );
                        CheckpointObjectOutcome::Processed
                    }
                    Err(err @ Error::TaskCancelled) | Err(err @ Error::TaskTimeout) => return Err(err),
                    Err(Error::TransientSkip { message }) => {
                        telemetry_unknown |= !increment_counter(skipped_objects);
                        telemetry_unknown |= !add_bytes(&mut bytes_processed, object_size);
                        demote_to_debug_when!(!take_failure_log_sample(&mut transient_skip_samples_logged), warn, target: "rustfs::heal::erasure_healer", {
                            event = EVENT_HEAL_ERASURE_OBJECT_STATE,
                            component = LOG_COMPONENT_HEAL,
                            subsystem = LOG_SUBSYSTEM_ERASURE_HEALER,
                            set_disk_id,
                            bucket,
                            object = %object,
                            version_id = ?version_id,
                            state = "transient_skip",
                            error = %message,
                            "Erasure set object heal skipped due to transient error"
                        });
                        CheckpointObjectOutcome::Skipped
                    }
                    Err(err) => {
                        telemetry_unknown |= !increment_counter(failed_objects);
                        telemetry_unknown |= !add_bytes(&mut bytes_processed, object_size);
                        demote_to_debug_when!(!take_failure_log_sample(&mut failure_samples_logged), warn, target: "rustfs::heal::erasure_healer", {
                            event = EVENT_HEAL_ERASURE_OBJECT_STATE,
                            component = LOG_COMPONENT_HEAL,
                            subsystem = LOG_SUBSYSTEM_ERASURE_HEALER,
                            set_disk_id,
                            bucket,
                            object = %object,
                            version_id = ?version_id,
                            state = "failed",
                            error = %err,
                            "Erasure set object heal failed"
                        });
                        CheckpointObjectOutcome::Failed
                    }
                };

                telemetry_unknown |= !increment_counter(processed_objects);
                completed_in_page += 1;
                let (outcome_record, counter_unknown) = {
                    let mut progress = self.progress.write().await;
                    progress.set_current_object(Some(format!("{bucket}/{object}")));
                    progress.update_object_progress(
                        *processed_objects,
                        *successful_objects,
                        *failed_objects,
                        *skipped_objects,
                        bytes_processed,
                    );
                    if telemetry_unknown {
                        progress.mark_unknown();
                    }
                    (
                        CheckpointObjectOutcomeRecord {
                            object: key,
                            outcome: checkpoint_outcome,
                            successful: progress.objects_healed,
                            failed: progress.objects_failed,
                            skipped: progress.skipped_objects,
                            bytes: progress.bytes_processed,
                            skipped_new_versions: progress.skipped_new_versions,
                            skipped_ilm_expired: progress.skipped_ilm_expired,
                            counter_unknown: progress.counter_unknown,
                        },
                        progress.counter_unknown,
                    )
                };
                checkpoint_manager.record_object_outcome(outcome_record).await?;
                if counter_unknown {
                    resume_manager.mark_counter_unknown().await?;
                }

                if completed_in_page.is_multiple_of(100) {
                    checkpoint_manager.update_position(bucket_index, page_resume_index).await?;
                }
            }

            *current_object_index = global_obj_idx;

            // Persist the checkpoint ledger and page position before exposing
            // the next resume cursor. A crash before cursor publication keeps
            // the page identities available for exact-once replay.
            checkpoint_manager.advance_page(bucket_index, *current_object_index).await?;
            // Check if there are more pages
            if !is_truncated {
                break;
            }
            continuation_token = next_heal_listing_token(bucket, "", next_token, is_truncated)?;
            if continuation_token.is_none() {
                // A truncated page without a continuation token is terminal.
                // Retain its ledger until bucket completion is durable.
                break;
            }
            resume_manager.set_resume_cursor(continuation_token.clone()).await?;
            checkpoint_manager.prune_completed_page().await?;

            // Anti-loop guard: an empty page reported as truncated cannot advance
            // the cursor (there is no last identity to move past), so treat it as a
            // non-advancing listing and abort rather than spin forever.
            if page_is_empty {
                return Err(Error::other(format!(
                    "Erasure set heal listing for bucket {bucket} returned an empty page marked truncated; aborting to avoid an infinite loop"
                )));
            }

            // Anti-loop guard: if the backend keeps reporting truncation but the
            // last version identity did not advance, we would spin forever.
            if page_last.is_some() && page_last == previous_page_last {
                return Err(Error::other(format!(
                    "Erasure set heal listing for bucket {bucket} is not advancing (repeated last version {page_last:?}); aborting to avoid an infinite loop"
                )));
            }
            previous_page_last = page_last;
        }

        Ok(())
    }

    /// initialize progress tracking
    async fn initialize_progress(&self, _buckets: &[String], state: &crate::heal::resume::ResumeState) {
        let mut progress = self.progress.write().await;
        let existing_baseline = (
            progress.objects_total_count,
            progress.objects_total_size,
            progress.baseline_generation,
            progress.progress_state,
            progress.baseline_known,
        );
        let baseline_generation_mismatch =
            state.baseline_known && existing_baseline.4 && state.baseline_generation != existing_baseline.2;
        let use_persisted_baseline = state.baseline_known && !baseline_generation_mismatch;
        progress.objects_scanned = state.processed_objects;
        progress.objects_healed = state.successful_objects;
        progress.objects_failed = state.failed_objects;
        progress.skipped_objects = state.skipped_objects;
        progress.skipped_new_versions = state.skipped_new_versions;
        progress.skipped_ilm_expired = state.skipped_ilm_expired;
        progress.bytes_processed = state.processed_bytes;
        progress.counter_unknown = state.counter_unknown;
        if use_persisted_baseline
            || existing_baseline.0 > 0
            || existing_baseline.1 > 0
            || existing_baseline.2.is_some()
            || existing_baseline.4
        {
            progress.objects_total_count = if use_persisted_baseline {
                state.total_objects
            } else {
                existing_baseline.0
            };
            progress.objects_total_size = if use_persisted_baseline {
                state.total_bytes
            } else {
                existing_baseline.1
            };
            progress.baseline_generation = if use_persisted_baseline {
                state.baseline_generation
            } else {
                existing_baseline.2
            };
            progress.baseline_known = use_persisted_baseline
                || existing_baseline.0 > 0
                || existing_baseline.1 > 0
                || existing_baseline.2.is_some()
                || existing_baseline.4;
        }
        progress.progress_state = if use_persisted_baseline
            || existing_baseline.0 > 0
            || existing_baseline.1 > 0
            || existing_baseline.2.is_some()
            || existing_baseline.4
        {
            crate::heal::progress::HealProgressState::Running
        } else {
            crate::heal::progress::HealProgressState::Indeterminate
        };
        if baseline_generation_mismatch || state.counter_unknown {
            progress.mark_unknown();
        }
        progress.ledger_complete = false;
        progress.refresh_progress_percentage();
        progress.start_time = UNIX_EPOCH.checked_add(Duration::from_secs(state.start_time));
        progress.last_update_time = UNIX_EPOCH.checked_add(Duration::from_secs(state.last_update));
        progress.set_current_object(state.current_object.clone());
    }
}

#[cfg(test)]
mod tests {
    use super::{ErasureSetHealer, PageConcurrencyGuard};
    use rustfs_common::heal_channel::{HealRequestSource, HealScanMode};
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    #[tokio::test]
    async fn dropping_pending_page_heal_releases_concurrency_slot() {
        let in_flight = Arc::new(AtomicUsize::new(0));
        let mut pending_heal = Box::pin({
            let in_flight = in_flight.clone();
            async move {
                let _guard = PageConcurrencyGuard::new(in_flight, "pool_0_set_0".to_string());
                std::future::pending::<()>().await;
            }
        });

        assert!(futures::poll!(pending_heal.as_mut()).is_pending());
        assert_eq!(in_flight.load(Ordering::SeqCst), 1);

        drop(pending_heal);
        assert_eq!(in_flight.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn heal_page_object_concurrency_uses_default_when_env_is_unset() {
        temp_env::with_var_unset(rustfs_config::ENV_HEAL_PAGE_OBJECT_CONCURRENCY, || {
            assert_eq!(
                ErasureSetHealer::heal_page_object_concurrency(),
                rustfs_config::DEFAULT_HEAL_PAGE_OBJECT_CONCURRENCY
            );
        });
    }

    #[test]
    fn heal_page_object_concurrency_respects_env_override() {
        temp_env::with_var(rustfs_config::ENV_HEAL_PAGE_OBJECT_CONCURRENCY, Some("11"), || {
            assert_eq!(ErasureSetHealer::heal_page_object_concurrency(), 11);
        });
    }

    #[test]
    fn effective_heal_page_object_concurrency_disables_parallelism_when_flag_is_off() {
        temp_env::with_var(rustfs_config::ENV_HEAL_PAGE_PARALLEL_ENABLE, Some("false"), || {
            temp_env::with_var(rustfs_config::ENV_HEAL_PAGE_OBJECT_CONCURRENCY, Some("11"), || {
                assert_eq!(ErasureSetHealer::effective_heal_page_object_concurrency(), 1);
            });
        });
    }

    #[test]
    fn deep_scan_heal_page_object_concurrency_is_serial() {
        temp_env::with_var(rustfs_config::ENV_HEAL_PAGE_OBJECT_CONCURRENCY, Some("11"), || {
            assert_eq!(
                ErasureSetHealer::effective_heal_page_object_concurrency_for_scan_mode(HealScanMode::Deep),
                1
            );
        });
    }

    #[test]
    fn normal_scan_heal_page_object_concurrency_uses_effective_limit() {
        temp_env::with_var(rustfs_config::ENV_HEAL_PAGE_OBJECT_CONCURRENCY, Some("11"), || {
            assert_eq!(
                ErasureSetHealer::effective_heal_page_object_concurrency_for_scan_mode(HealScanMode::Normal),
                11
            );
        });
    }

    #[test]
    fn auto_heal_page_object_concurrency_is_serial() {
        temp_env::with_vars(
            [
                (rustfs_config::ENV_HEAL_PAGE_PARALLEL_ENABLE, Some("true")),
                (rustfs_config::ENV_HEAL_PAGE_OBJECT_CONCURRENCY, Some("11")),
            ],
            || {
                assert_eq!(
                    ErasureSetHealer::effective_heal_page_object_concurrency_for_source(
                        HealRequestSource::AutoHeal,
                        HealScanMode::Normal,
                    ),
                    1
                );
            },
        );
    }

    #[test]
    fn non_auto_normal_scan_page_object_concurrency_uses_effective_limit() {
        temp_env::with_vars(
            [
                (rustfs_config::ENV_HEAL_PAGE_PARALLEL_ENABLE, Some("true")),
                (rustfs_config::ENV_HEAL_PAGE_OBJECT_CONCURRENCY, Some("11")),
            ],
            || {
                assert_eq!(
                    ErasureSetHealer::effective_heal_page_object_concurrency_for_source(
                        HealRequestSource::Admin,
                        HealScanMode::Normal,
                    ),
                    11
                );
            },
        );
    }

    // Regression guards for backlog#856 / #799 B7: heal-object error
    // classification must not conflate an offline drive (or unmet quorum) with
    // genuine object absence, or transient failures get recorded as "healed" and
    // permanently skipped.
    use super::{EcstoreError, Error, HealObjectOutcome};

    fn classify(err: EcstoreError) -> HealObjectOutcome {
        ErasureSetHealer::classify_heal_object_error(&Error::Storage(err))
    }

    #[test]
    fn disk_not_found_is_transient_not_absent() {
        assert!(matches!(classify(EcstoreError::DiskNotFound), HealObjectOutcome::Transient));
        assert!(matches!(classify(EcstoreError::VolumeNotFound), HealObjectOutcome::Transient));
    }

    #[test]
    fn quorum_errors_are_transient() {
        assert!(matches!(classify(EcstoreError::ErasureReadQuorum), HealObjectOutcome::Transient));
        assert!(matches!(
            classify(EcstoreError::InsufficientReadQuorum(String::new(), String::new())),
            HealObjectOutcome::Transient
        ));
    }

    #[test]
    fn genuine_object_absence_is_absent() {
        assert!(matches!(classify(EcstoreError::FileNotFound), HealObjectOutcome::Absent));
        assert!(matches!(classify(EcstoreError::FileVersionNotFound), HealObjectOutcome::Absent));
        assert!(matches!(
            classify(EcstoreError::ObjectNotFound("bucket".into(), "object".into())),
            HealObjectOutcome::Absent
        ));
        assert!(matches!(
            classify(EcstoreError::VersionNotFound("bucket".into(), "object".into(), "vid".into())),
            HealObjectOutcome::Absent
        ));
    }

    #[test]
    fn other_errors_are_failures() {
        assert!(matches!(
            ErasureSetHealer::classify_heal_object_error(&Error::Other("boom".into())),
            HealObjectOutcome::Failed
        ));
    }
}

#[cfg(test)]
mod resume_loop_tests {
    //! Loop-level tests driving the private concurrent resume loop
    //! (`heal_bucket_with_resume`) against a controllable fake `HealStorageAPI`
    //! that emits programmable multi-version pages. These exercise the real loop
    //! logic (cursor seeding, per-version dedup, anti-loop guard, absence
    //! handling) — not merely a mock's own output.
    use super::{
        ErasureSetHealer, NANOS_PER_SECOND, NEW_VERSION_SKIP_GRACE_SECS, should_skip_new_version, target_outcomes_complete,
    };
    use crate::heal::progress::HealProgress;
    use crate::heal::resume::{
        CheckpointManager, CheckpointObjectOutcome, CheckpointObjectOutcomeRecord, RESUME_CHECKPOINT_FILE,
        ReplacementTargetIdentity, ResumeDeleteFailure, ResumeManager, ResumeUtils, compose_key,
    };
    use crate::heal::storage::{HealLifecycleExpiryContext, HealListItem, HealObjectInfo, HealStorageAPI};
    use crate::heal::storage_api::status::BucketInfo;
    use crate::heal::{
        BUCKET_META_PREFIX, DiskOption, DiskStore, EcstoreError, Endpoint, HealDiskExt as _, RUSTFS_META_BUCKET, new_disk,
    };
    use crate::{Error, Result};
    use rustfs_common::heal_channel::{HealOpts, HealRequestSource};
    use rustfs_madmin::heal_commands::{HealDriveInfo, HealResultItem, Infos};
    use std::collections::{HashMap, HashSet, VecDeque};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};
    use tempfile::TempDir;
    use tokio::sync::RwLock;
    use tokio_util::sync::CancellationToken;

    fn item(name: &str, version: Option<&str>, delete_marker: bool) -> HealListItem {
        HealListItem {
            name: name.to_string(),
            version_id: version.map(str::to_string),
            mod_time_unix_nanos: None,
            lifecycle_object_info: None,
            is_delete_marker: delete_marker,
        }
    }

    fn item_with_mod_time(name: &str, version: Option<&str>, mod_time_secs: u64) -> HealListItem {
        HealListItem {
            name: name.to_string(),
            version_id: version.map(str::to_string),
            mod_time_unix_nanos: Some(i128::from(mod_time_secs).saturating_mul(NANOS_PER_SECOND)),
            lifecycle_object_info: None,
            is_delete_marker: false,
        }
    }

    #[test]
    fn new_version_filter_respects_grace_boundary() {
        let started_at = 1_700_000_000;

        assert!(!should_skip_new_version(None, started_at));
        assert!(!should_skip_new_version(
            Some(i128::from(started_at + NEW_VERSION_SKIP_GRACE_SECS).saturating_mul(NANOS_PER_SECOND)),
            started_at,
        ));
        assert!(should_skip_new_version(
            Some(i128::from(started_at + NEW_VERSION_SKIP_GRACE_SECS + 1).saturating_mul(NANOS_PER_SECOND)),
            started_at,
        ));
    }

    #[test]
    fn target_outcomes_require_each_requested_endpoint_once_and_ok() {
        let result = HealResultItem {
            after: Infos {
                drives: vec![
                    HealDriveInfo {
                        endpoint: "replacement-a".to_string(),
                        state: "ok".to_string(),
                        ..Default::default()
                    },
                    HealDriveInfo {
                        endpoint: "replacement-b".to_string(),
                        state: "missing".to_string(),
                        ..Default::default()
                    },
                ],
            },
            ..Default::default()
        };

        assert!(target_outcomes_complete(&result, &["replacement-a".to_string()]));
        assert!(!target_outcomes_complete(
            &result,
            &["replacement-a".to_string(), "replacement-b".to_string()]
        ));
        assert!(!target_outcomes_complete(&result, &["replacement-c".to_string()]));

        let duplicate = HealResultItem {
            after: Infos {
                drives: vec![
                    HealDriveInfo {
                        endpoint: "replacement-a".to_string(),
                        state: "ok".to_string(),
                        ..Default::default()
                    },
                    HealDriveInfo {
                        endpoint: "replacement-a".to_string(),
                        state: "missing".to_string(),
                        ..Default::default()
                    },
                ],
            },
            ..Default::default()
        };
        assert!(!target_outcomes_complete(&duplicate, &["replacement-a".to_string()]));
    }

    #[derive(Clone)]
    struct Page {
        items: Vec<HealListItem>,
        next: Option<String>,
        truncated: bool,
    }

    #[derive(Clone)]
    enum HealOutcome {
        Ok,
        /// The version vanished before heal ran (deleted mid-heal).
        VersionNotFound,
        /// A transient infrastructure condition (offline disk / unmet quorum):
        /// the version must be recorded as skipped and retried on a later pass.
        Transient,
        Timeout,
    }

    #[derive(Clone)]
    enum ReplacementCommitEvidence {
        Confirmed(bool),
        Error(String),
    }

    #[derive(Default)]
    struct FakeStorage {
        /// page keyed by the *incoming* continuation token
        pages: Mutex<HashMap<Option<String>, Page>>,
        /// per-`compose_key` heal outcome; default is `Ok`
        outcomes: Mutex<HashMap<String, HealOutcome>>,
        /// successful low-level result per `compose_key`; default has no drive outcomes.
        results: Mutex<HashMap<String, HealResultItem>>,
        /// Target-specific physical readback evidence per `compose_key`; the
        /// fake models a healthy backend unless a test explicitly revokes it.
        replacement_commit_evidence: Mutex<HashMap<String, ReplacementCommitEvidence>>,
        lifecycle_expired: Mutex<HashSet<String>>,
        /// every heal_object call recorded as (name, version_id)
        heal_calls: Mutex<Vec<(String, Option<String>)>>,
        list_include_lifecycle_object_info: Mutex<Vec<bool>>,
        replacement_target_identity_sequences: Mutex<VecDeque<Vec<ReplacementTargetIdentity>>>,
        fail_listing: AtomicBool,
        fail_listing_buckets: Mutex<HashSet<String>>,
    }

    impl FakeStorage {
        fn set_page(&self, token: Option<&str>, page: Page) {
            self.pages.lock().unwrap().insert(token.map(str::to_string), page);
        }
        fn set_outcome(&self, name: &str, version: Option<&str>, outcome: HealOutcome) {
            self.outcomes.lock().unwrap().insert(compose_key(name, version), outcome);
        }
        fn set_result(&self, name: &str, version: Option<&str>, result: HealResultItem) {
            self.results.lock().unwrap().insert(compose_key(name, version), result);
        }
        fn set_replacement_commit_evidence(&self, name: &str, version: Option<&str>, committed: bool) {
            self.replacement_commit_evidence
                .lock()
                .unwrap()
                .insert(compose_key(name, version), ReplacementCommitEvidence::Confirmed(committed));
        }
        fn set_replacement_commit_evidence_error(&self, name: &str, version: Option<&str>, message: &str) {
            self.replacement_commit_evidence
                .lock()
                .unwrap()
                .insert(compose_key(name, version), ReplacementCommitEvidence::Error(message.to_string()));
        }
        fn set_lifecycle_expired(&self, name: &str, version: Option<&str>) {
            self.lifecycle_expired.lock().unwrap().insert(compose_key(name, version));
        }
        fn calls(&self) -> Vec<(String, Option<String>)> {
            self.heal_calls.lock().unwrap().clone()
        }
        fn list_include_lifecycle_object_info_calls(&self) -> Vec<bool> {
            self.list_include_lifecycle_object_info.lock().unwrap().clone()
        }
        fn fail_listing(&self) {
            self.fail_listing.store(true, Ordering::SeqCst);
        }
        fn fail_bucket_listing(&self, bucket: &str) {
            self.fail_listing_buckets.lock().unwrap().insert(bucket.to_string());
        }
    }

    #[async_trait::async_trait]
    impl HealStorageAPI for FakeStorage {
        async fn get_object_meta(&self, _b: &str, _o: &str) -> Result<Option<HealObjectInfo>> {
            Ok(None)
        }
        async fn ec_decode_rebuild(&self, _b: &str, _o: &str) -> Result<Vec<u8>> {
            Ok(Vec::new())
        }
        async fn get_bucket_info(&self, bucket: &str) -> Result<Option<BucketInfo>> {
            Ok(Some(BucketInfo {
                name: bucket.to_string(),
                ..Default::default()
            }))
        }
        async fn list_buckets(&self) -> Result<Vec<BucketInfo>> {
            Ok(Vec::new())
        }
        async fn object_exists(&self, _b: &str, _o: &str) -> Result<bool> {
            // Must never be consulted: the resume loop always goes through heal_object.
            panic!("object_exists must not be called by the resume heal loop");
        }
        async fn load_heal_lifecycle_expiry_context(&self, _bucket: &str) -> Result<Option<HealLifecycleExpiryContext>> {
            Ok((!self.lifecycle_expired.lock().unwrap().is_empty()).then(HealLifecycleExpiryContext::test))
        }
        async fn enqueue_heal_lifecycle_expiry(
            &self,
            _context: &HealLifecycleExpiryContext,
            _bucket: &str,
            object: &str,
            version_id: Option<&str>,
            _object_info: Option<&HealObjectInfo>,
        ) -> Result<bool> {
            Ok(self
                .lifecycle_expired
                .lock()
                .unwrap()
                .contains(&compose_key(object, version_id)))
        }
        async fn heal_object(
            &self,
            _bucket: &str,
            object: &str,
            version_id: Option<&str>,
            _opts: &HealOpts,
        ) -> Result<(HealResultItem, Option<Error>)> {
            self.heal_calls
                .lock()
                .unwrap()
                .push((object.to_string(), version_id.map(str::to_string)));
            let key = compose_key(object, version_id);
            let outcome = self.outcomes.lock().unwrap().get(&key).cloned().unwrap_or(HealOutcome::Ok);
            match outcome {
                HealOutcome::Ok => Ok((self.results.lock().unwrap().get(&key).cloned().unwrap_or_default(), None)),
                HealOutcome::VersionNotFound => {
                    Ok((HealResultItem::default(), Some(Error::Storage(EcstoreError::FileVersionNotFound))))
                }
                HealOutcome::Transient => Ok((HealResultItem::default(), Some(Error::Storage(EcstoreError::DiskNotFound)))),
                HealOutcome::Timeout => Err(Error::TaskTimeout),
            }
        }
        async fn heal_bucket(&self, _b: &str, _o: &HealOpts) -> Result<HealResultItem> {
            Ok(HealResultItem::default())
        }
        async fn heal_format(&self, _dry: bool) -> Result<(HealResultItem, Option<Error>)> {
            Ok((HealResultItem::default(), None))
        }
        async fn replacement_targets_have_version(
            &self,
            _bucket: &str,
            object: &str,
            version_id: Option<&str>,
            _opts: &HealOpts,
            _targets: &[String],
        ) -> Result<bool> {
            match self
                .replacement_commit_evidence
                .lock()
                .unwrap()
                .get(&compose_key(object, version_id))
                .cloned()
                .unwrap_or(ReplacementCommitEvidence::Confirmed(true))
            {
                ReplacementCommitEvidence::Confirmed(committed) => Ok(committed),
                ReplacementCommitEvidence::Error(message) => Err(Error::other(message)),
            }
        }
        async fn list_objects_for_heal_page(
            &self,
            bucket: &str,
            _prefix: &str,
            continuation_token: Option<&str>,
            include_lifecycle_object_info: bool,
        ) -> Result<(Vec<HealListItem>, Option<String>, bool)> {
            self.list_include_lifecycle_object_info
                .lock()
                .unwrap()
                .push(include_lifecycle_object_info);
            if self.fail_listing.load(Ordering::SeqCst) || self.fail_listing_buckets.lock().unwrap().contains(bucket) {
                return Err(Error::other("injected listing failure"));
            }
            let key = continuation_token.map(str::to_string);
            let page = self.pages.lock().unwrap().get(&key).cloned();
            match page {
                Some(p) => Ok((p.items, p.next, p.truncated)),
                None => Ok((Vec::new(), None, false)),
            }
        }
        async fn get_disk_for_resume(&self, _id: &str) -> Result<DiskStore> {
            Err(Error::other("not implemented in tests"))
        }
        async fn replacement_target_identities(&self, _targets: &[String]) -> Result<Vec<ReplacementTargetIdentity>> {
            self.replacement_target_identity_sequences
                .lock()
                .unwrap()
                .pop_front()
                .ok_or_else(|| Error::other("replacement identity sequence exhausted"))
        }
    }

    async fn make_disk(temp: &TempDir) -> DiskStore {
        let disk_path = temp.path().join("test_disk");
        std::fs::create_dir_all(&disk_path).unwrap();
        let endpoint = Endpoint::try_from(disk_path.to_string_lossy().as_ref()).unwrap();
        let disk = new_disk(
            &endpoint,
            &DiskOption {
                cleanup: false,
                health_check: false,
            },
        )
        .await
        .unwrap();
        let _ = disk.make_volume(RUSTFS_META_BUCKET).await;
        let _ = disk.make_volume(&format!("{RUSTFS_META_BUCKET}/{BUCKET_META_PREFIX}")).await;
        disk
    }

    struct Env {
        healer: ErasureSetHealer,
        storage: Arc<FakeStorage>,
        resume: ResumeManager,
        checkpoint: CheckpointManager,
        task_id: String,
        _temp: TempDir,
    }

    async fn make_env() -> Env {
        make_env_with_targets(Vec::new()).await
    }

    async fn make_env_with_targets(target_endpoints: Vec<String>) -> Env {
        let temp = TempDir::new().unwrap();
        let disk = make_disk(&temp).await;
        let storage = Arc::new(FakeStorage::default());
        let task_id = ResumeUtils::generate_task_id();
        let healer = ErasureSetHealer::new(
            storage.clone(),
            Arc::new(RwLock::new(HealProgress::new())),
            CancellationToken::new(),
            disk.clone(),
            HealOpts::default(),
            HealRequestSource::Internal,
        )
        .with_replacement_targets(target_endpoints, None);
        let resume = ResumeManager::new(
            disk.clone(),
            task_id.clone(),
            "erasure_set".to_string(),
            "pool_0_set_0".to_string(),
            vec!["b".to_string()],
        )
        .await
        .unwrap();
        let checkpoint = CheckpointManager::new(disk, task_id.clone()).await.unwrap();
        Env {
            healer,
            storage,
            resume,
            checkpoint,
            task_id,
            _temp: temp,
        }
    }

    /// Drive one bucket heal pass; returns (processed, successful, failed, skipped, result).
    async fn run(env: &Env) -> (u64, u64, u64, u64, Result<()>) {
        let state = env.resume.get_state().await;
        let mut current_object_index = 0usize;
        let mut processed = 0u64;
        let mut successful = 0u64;
        let mut failed = 0u64;
        let mut skipped = 0u64;
        let result = env
            .healer
            .heal_bucket_with_resume(
                "b",
                "pool_0_set_0",
                0,
                &mut current_object_index,
                &mut processed,
                &mut successful,
                &mut failed,
                &mut skipped,
                &env.resume,
                &env.checkpoint,
                state.start_time,
            )
            .await;
        (processed, successful, failed, skipped, result)
    }

    #[tokio::test]
    async fn test_empty_bucket_no_panic() {
        let env = make_env().await;
        // no pages configured => empty, non-truncated page
        let (processed, successful, failed, skipped, result) = run(&env).await;
        result.expect("empty bucket must succeed");
        assert_eq!(processed, 0);
        assert_eq!(successful, 0);
        assert_eq!(failed, 0);
        assert_eq!(skipped, 0);
        assert!(env.storage.calls().is_empty());
        assert_eq!(env.resume.resume_cursor().await, None);
    }

    #[tokio::test]
    async fn replacement_targets_use_a_canonical_order() {
        let env = make_env_with_targets(vec![
            "replacement-b".to_string(),
            "replacement-a".to_string(),
            "replacement-b".to_string(),
        ])
        .await;

        assert_eq!(env.healer.target_endpoints.as_ref(), ["replacement-a", "replacement-b"]);
    }

    #[tokio::test]
    async fn replacement_identity_fence_rejects_a_remount_before_page_scan() {
        let env = make_env_with_targets(vec!["replacement-a".to_string()]).await;
        let expected_identity = ReplacementTargetIdentity {
            endpoint: "replacement-a".to_string(),
            canonical_path: "/mnt/replacement-a".to_string(),
            physical_device_ids: vec!["device-a".to_string()],
            filesystem_identity: "filesystem-a".to_string(),
        };
        let remounted_identity = ReplacementTargetIdentity {
            physical_device_ids: vec!["device-b".to_string()],
            filesystem_identity: "filesystem-b".to_string(),
            ..expected_identity.clone()
        };
        env.storage
            .replacement_target_identity_sequences
            .lock()
            .unwrap()
            .push_back(vec![remounted_identity]);
        let healer = ErasureSetHealer::new(
            env.storage.clone(),
            Arc::new(RwLock::new(HealProgress::new())),
            CancellationToken::new(),
            env.healer.disk.clone(),
            HealOpts::default(),
            HealRequestSource::AutoHeal,
        )
        .with_replacement_targets(vec!["replacement-a".to_string()], Some("generation-a".to_string()))
        .with_replacement_identity_fence(Some(vec![expected_identity]));
        let mut current_object_index = 0;
        let mut processed = 0;
        let mut successful = 0;
        let mut failed = 0;
        let mut skipped = 0;
        let started_at = env.resume.get_state().await.start_time;

        let error = healer
            .heal_bucket_with_resume(
                "b",
                "pool_0_set_0",
                0,
                &mut current_object_index,
                &mut processed,
                &mut successful,
                &mut failed,
                &mut skipped,
                &env.resume,
                &env.checkpoint,
                started_at,
            )
            .await
            .expect_err("a remounted target must not begin a new page scan");

        assert!(error.to_string().contains("page scan"));
        assert!(env.storage.calls().is_empty());
    }

    #[tokio::test]
    async fn replacement_generation_never_reuses_another_disk_cursor() {
        let env = make_env_with_targets(vec!["replacement-a".to_string()]).await;
        ResumeManager::new_replacement_intent(
            env.healer.disk.clone(),
            ResumeUtils::generate_task_id(),
            "pool_0_set_0".to_string(),
            vec!["b".to_string()],
            vec!["replacement-a".to_string()],
            vec![crate::heal::resume::ReplacementTargetIdentity {
                endpoint: "replacement-a".to_string(),
                canonical_path: "/mnt/replacement-a".to_string(),
                physical_device_ids: vec!["device-a".to_string()],
                filesystem_identity: "1:2:3".to_string(),
            }],
        )
        .await
        .expect("first replacement intent should persist");

        let healer = ErasureSetHealer::new(
            env.storage.clone(),
            Arc::new(RwLock::new(HealProgress::new())),
            CancellationToken::new(),
            env.healer.disk.clone(),
            HealOpts::default(),
            HealRequestSource::AutoHeal,
        )
        .with_replacement_targets(vec!["replacement-a".to_string()], Some(ResumeUtils::generate_task_id()));

        let error = healer
            .get_or_create_task_id("pool_0_set_0")
            .await
            .expect_err("a second replacement must not reuse the first replacement cursor");
        assert!(
            !error.to_string().contains("generation-a"),
            "the previous replacement generation must not be selected"
        );
    }

    #[tokio::test]
    async fn object_timeout_aborts_the_bucket_page_immediately() {
        let env = make_env().await;
        env.storage.set_page(
            None,
            Page {
                items: vec![item("timed-out", None, false)],
                next: None,
                truncated: false,
            },
        );
        env.storage.set_outcome("timed-out", None, HealOutcome::Timeout);

        let (processed, successful, failed, skipped, result) = run(&env).await;

        assert!(matches!(result, Err(Error::TaskTimeout)));
        assert_eq!(processed, 0);
        assert_eq!(successful, 0);
        assert_eq!(failed, 0);
        assert_eq!(skipped, 0);
    }

    #[tokio::test]
    async fn erasure_set_progress_accumulates_healed_object_bytes() {
        let env = make_env().await;
        env.storage.set_page(
            None,
            Page {
                items: vec![item("first", Some("v1"), false), item("second", Some("v2"), false)],
                next: None,
                truncated: false,
            },
        );
        env.storage.set_result(
            "first",
            Some("v1"),
            HealResultItem {
                object_size: 1024,
                ..Default::default()
            },
        );
        env.storage.set_result(
            "second",
            Some("v2"),
            HealResultItem {
                object_size: 2048,
                ..Default::default()
            },
        );

        let (processed, successful, failed, skipped, result) = run(&env).await;

        result.expect("page heal should succeed");
        assert_eq!(processed, 2);
        assert_eq!(successful, 2);
        assert_eq!(failed, 0);
        assert_eq!(skipped, 0);
        let progress = env.healer.progress.read().await;
        assert_eq!(progress.objects_scanned, 2);
        assert_eq!(progress.objects_healed, 2);
        assert_eq!(progress.objects_failed, 0);
        assert_eq!(progress.bytes_processed, 3072);
        assert!(matches!(progress.current_object.as_deref(), Some("b/first" | "b/second")));
    }

    #[tokio::test]
    async fn erasure_set_skips_versions_written_after_heal_started() {
        let env = make_env().await;
        let started_at = env.resume.get_state().await.start_time;
        env.storage.set_page(
            None,
            Page {
                items: vec![
                    item_with_mod_time("old", Some("v1"), started_at + NEW_VERSION_SKIP_GRACE_SECS),
                    item_with_mod_time("new", Some("v2"), started_at + NEW_VERSION_SKIP_GRACE_SECS + 1),
                ],
                next: None,
                truncated: false,
            },
        );

        let (processed, successful, failed, skipped, result) = run(&env).await;

        result.expect("page heal should succeed");
        assert_eq!(processed, 2);
        assert_eq!(successful, 1);
        assert_eq!(failed, 0);
        assert_eq!(skipped, 0);
        assert_eq!(env.storage.calls(), vec![("old".to_string(), Some("v1".to_string()))]);
        let progress = env.healer.progress.read().await;
        assert_eq!(progress.skipped_new_versions, 1);
        assert_eq!(progress.objects_scanned, 2);
        assert_eq!(progress.objects_healed, 1);
        assert_eq!(progress.objects_failed, 0);
    }

    #[tokio::test]
    async fn erasure_set_skips_versions_queued_for_lifecycle_expiry() {
        let env = make_env().await;
        env.storage.set_page(
            None,
            Page {
                items: vec![item("expired", Some("v1"), false), item("kept", Some("v2"), false)],
                next: None,
                truncated: false,
            },
        );
        env.storage.set_lifecycle_expired("expired", Some("v1"));

        let (processed, successful, failed, skipped, result) = run(&env).await;

        result.expect("page heal should succeed");
        assert_eq!(processed, 2);
        assert_eq!(successful, 1);
        assert_eq!(failed, 0);
        assert_eq!(skipped, 0);
        assert_eq!(env.storage.calls(), vec![("kept".to_string(), Some("v2".to_string()))]);
        assert_eq!(env.storage.list_include_lifecycle_object_info_calls(), vec![true]);
        let progress = env.healer.progress.read().await;
        assert_eq!(progress.skipped_ilm_expired, 1);
        assert_eq!(progress.objects_scanned, 2);
        assert_eq!(progress.objects_healed, 1);
        assert_eq!(progress.objects_failed, 0);
    }

    #[tokio::test]
    async fn bucket_listing_failure_does_not_mark_set_completed() {
        let env = make_env().await;
        env.storage.fail_listing();

        let result = env
            .healer
            .execute_heal_with_resume(&["b".to_string()], "pool_0_set_0", &env.resume, &env.checkpoint)
            .await;

        assert!(result.is_err(), "a bucket listing failure must fail the set heal pass");
        let state = env.resume.get_state().await;
        assert!(!state.completed, "a failed bucket must not mark the set completed");
        assert_eq!(state.retry_count, 1, "the failed bucket must schedule a bounded retry");
        assert!(state.completed_buckets.is_empty(), "the failed bucket must remain resumable");
    }

    #[tokio::test]
    async fn bucket_failure_stops_before_a_later_bucket_checkpoint() {
        let env = make_env().await;
        let task_id = ResumeUtils::generate_task_id();
        let buckets = vec!["a".to_string(), "b".to_string()];
        let resume = ResumeManager::new(
            env.healer.disk.clone(),
            task_id.clone(),
            "erasure_set".to_string(),
            "pool_0_set_0".to_string(),
            buckets.clone(),
        )
        .await
        .unwrap();
        let checkpoint = CheckpointManager::new(env.healer.disk.clone(), task_id.clone())
            .await
            .unwrap();
        env.storage.fail_bucket_listing("a");
        for _ in 0..3 {
            assert!(resume.schedule_retry().await.unwrap());
        }

        env.healer
            .execute_heal_with_resume(&buckets, "pool_0_set_0", &resume, &checkpoint)
            .await
            .expect_err("the first bucket failure must keep the pass incomplete");
        let persisted = checkpoint.get_checkpoint().await;
        assert_eq!(persisted.current_bucket_index, 0);
        assert!(resume.get_state().await.completed_buckets.is_empty());

        let resumed = ResumeManager::load_from_disk(env.healer.disk.clone(), &task_id)
            .await
            .unwrap();
        let checkpoint = CheckpointManager::load_from_disk(env.healer.disk.clone(), &task_id)
            .await
            .unwrap();
        env.healer
            .execute_heal_with_resume(&buckets, "pool_0_set_0", &resumed, &checkpoint)
            .await
            .expect_err("recovery must retry the earlier failed bucket");
        assert!(!resumed.get_state().await.completed);
    }

    #[tokio::test]
    async fn completed_resume_state_is_not_selected_for_a_new_heal() {
        let env = make_env().await;
        env.resume
            .mark_completed()
            .await
            .expect("completed resume state should persist");

        let task_id = env
            .healer
            .get_or_create_task_id("pool_0_set_0")
            .await
            .expect("new heal should allocate a task id");

        assert_ne!(task_id, env.task_id, "a completed resume state must not suppress a new heal");
        assert!(uuid::Uuid::parse_str(&task_id).is_ok(), "new resume task ids must be UUIDs");
    }

    #[tokio::test]
    async fn cleanup_failure_keeps_erasure_set_heal_incomplete() {
        let env = make_env().await;
        let checkpoint_path = format!("{BUCKET_META_PREFIX}/{}_{RESUME_CHECKPOINT_FILE}", env.task_id);
        let _failure = ResumeDeleteFailure::install(checkpoint_path, crate::heal::DiskError::DiskAccessDenied);

        let error = env
            .healer
            .heal_erasure_set(&["b".to_string()], "pool_0_set_0")
            .await
            .expect_err("checkpoint cleanup failure must fail the erasure-set heal");

        assert!(matches!(error, Error::Disk(crate::heal::DiskError::DiskAccessDenied)));
        let state = ResumeManager::load_from_disk(env.healer.disk.clone(), &env.task_id)
            .await
            .expect("completed state must remain discoverable after cleanup failure")
            .get_state()
            .await;
        assert!(state.completed, "successful data heal must be persisted before cleanup is attempted");
    }

    #[tokio::test]
    async fn replacement_completion_keeps_resume_artifacts_until_marker_cleanup() {
        let env = make_env_with_targets(vec!["replacement-a".to_string()]).await;
        let replacement_task_id = ResumeUtils::generate_task_id();
        ResumeManager::new_replacement_intent(
            env.healer.disk.clone(),
            replacement_task_id.clone(),
            "pool_0_set_0".to_string(),
            vec!["b".to_string()],
            vec!["replacement-a".to_string()],
            vec![crate::heal::resume::ReplacementTargetIdentity {
                endpoint: "replacement-a".to_string(),
                canonical_path: "/mnt/replacement-a".to_string(),
                physical_device_ids: vec!["device-a".to_string()],
                filesystem_identity: "1:2:3".to_string(),
            }],
        )
        .await
        .expect("replacement intent should persist");
        let checkpoint = CheckpointManager::new(env.healer.disk.clone(), replacement_task_id.clone())
            .await
            .expect("replacement checkpoint should persist");
        let healer = ErasureSetHealer::new(
            env.storage.clone(),
            Arc::new(RwLock::new(HealProgress::new())),
            CancellationToken::new(),
            env.healer.disk.clone(),
            HealOpts::default(),
            HealRequestSource::AutoHeal,
        )
        .with_replacement_targets(vec!["replacement-a".to_string()], Some(replacement_task_id.clone()));

        healer
            .heal_erasure_set(&["b".to_string()], "pool_0_set_0")
            .await
            .expect("replacement data scan should complete");

        let state = ResumeManager::load_replacement_intent(env.healer.disk.clone(), &replacement_task_id)
            .await
            .expect("verified replacement state must remain after data scan")
            .get_state()
            .await;
        assert!(state.completed, "the verified state must record a completed data scan");
        assert_eq!(state.replacement_phase, crate::heal::resume::ReplacementPhase::Verified);
        assert!(
            CheckpointManager::has_checkpoint(&env.healer.disk, &replacement_task_id).await,
            "the checkpoint must survive until the caller clears the healing marker"
        );
        drop(checkpoint);
    }

    #[tokio::test]
    async fn retry_exhaustion_keeps_resume_artifacts_for_recovery() {
        let env = make_env().await;
        for _ in 0..3 {
            assert!(env.resume.schedule_retry().await.expect("retry state should persist"));
            env.checkpoint
                .reset_for_retry()
                .await
                .expect("checkpoint reset should persist");
        }
        env.storage.fail_listing();

        env.healer
            .execute_heal_with_resume(&["b".to_string()], "pool_0_set_0", &env.resume, &env.checkpoint)
            .await
            .expect_err("exhausted retry state must report the incomplete heal");

        assert!(
            ResumeManager::has_resume_state(&env.healer.disk, &env.task_id).await,
            "retry exhaustion must not delete the resumable state while a marker may remain"
        );
        assert!(
            CheckpointManager::has_checkpoint(&env.healer.disk, &env.task_id).await,
            "retry exhaustion must retain the checkpoint with the resumable state"
        );
    }

    #[tokio::test]
    async fn retry_resume_repairs_checkpoint_after_crash_between_resets() {
        let env = make_env().await;
        env.resume
            .update_progress(3, 1, 1, 1)
            .await
            .expect("dirty resume progress should persist");
        env.resume
            .complete_bucket("b")
            .await
            .expect("dirty completed bucket should persist");
        env.resume
            .set_resume_cursor(Some("stale-cursor".to_string()))
            .await
            .expect("dirty resume cursor should persist");
        env.checkpoint
            .add_skipped_object(compose_key("stale-object", None))
            .await
            .expect("dirty checkpoint object should be recorded");
        env.checkpoint
            .update_position(4, 9)
            .await
            .expect("dirty checkpoint position should persist");

        assert!(
            env.resume.schedule_retry().await.expect("resume retry reset should persist"),
            "retry budget should remain"
        );

        let (_, checkpoint) = env
            .healer
            .initialize_resume_state(&env.task_id, "pool_0_set_0", &["b".to_string()])
            .await
            .expect("resume initialization should repair a stale checkpoint");
        let checkpoint = checkpoint.get_checkpoint().await;

        assert_eq!(checkpoint.current_bucket_index, 0);
        assert_eq!(checkpoint.current_object_index, 0);
        assert!(checkpoint.processed_objects.is_empty());
        assert!(checkpoint.failed_objects.is_empty());
        assert!(checkpoint.skipped_objects.is_empty());
    }

    #[tokio::test]
    async fn test_resume_across_page_boundary_no_drop_no_double() {
        let env = make_env().await;
        env.storage.set_page(
            None,
            Page {
                items: vec![item("a", None, false), item("b", None, false)],
                next: Some("t1".to_string()),
                truncated: true,
            },
        );
        env.storage.set_page(
            Some("t1"),
            Page {
                items: vec![item("c", None, false), item("d", None, false)],
                next: None,
                truncated: false,
            },
        );

        let (processed, successful, failed, _skipped, result) = run(&env).await;
        result.expect("two-page heal must succeed");
        assert_eq!(processed, 4);
        assert_eq!(successful, 4);
        assert_eq!(failed, 0);

        let mut names: Vec<String> = env.storage.calls().into_iter().map(|(n, _)| n).collect();
        names.sort();
        assert_eq!(names, vec!["a", "b", "c", "d"], "every object exactly once, none dropped/doubled");
        // Keep the final page cursor until the outer loop durably completes the
        // bucket, so a crash can replay only this page against its identities.
        assert_eq!(env.resume.resume_cursor().await, Some("t1".to_string()));
    }

    #[tokio::test]
    async fn persisted_failure_waits_for_the_bounded_retry_after_page_replay() {
        let env = make_env().await;
        env.storage.set_page(
            None,
            Page {
                items: vec![item("object", Some("v1"), false)],
                next: None,
                truncated: false,
            },
        );
        env.checkpoint
            .record_object_outcome(CheckpointObjectOutcomeRecord {
                object: compose_key("object", Some("v1")),
                outcome: CheckpointObjectOutcome::Failed,
                successful: 0,
                failed: 1,
                skipped: 0,
                bytes: 0,
                skipped_new_versions: 0,
                skipped_ilm_expired: 0,
                counter_unknown: false,
            })
            .await
            .unwrap();
        env.checkpoint.advance_page(0, 1).await.unwrap();

        let resumed = ResumeManager::load_from_disk(env.healer.disk.clone(), &env.task_id)
            .await
            .unwrap();
        let checkpoint = CheckpointManager::load_from_disk(env.healer.disk.clone(), &env.task_id)
            .await
            .unwrap();

        env.healer
            .execute_heal_with_resume(&["b".to_string()], "pool_0_set_0", &resumed, &checkpoint)
            .await
            .expect_err("the persisted failure must schedule a bounded retry");
        assert!(
            env.storage.calls().is_empty(),
            "the failed identity must not be repeated in the same pass"
        );

        env.healer
            .execute_heal_with_resume(&["b".to_string()], "pool_0_set_0", &resumed, &checkpoint)
            .await
            .expect("the bounded retry must heal the object");
        assert_eq!(env.storage.calls(), vec![("object".to_string(), Some("v1".to_string()))]);
        let state = resumed.get_state().await;
        assert_eq!(state.successful_objects, 1);
        assert_eq!(state.failed_objects, 0);
    }

    #[tokio::test]
    async fn final_page_crash_replays_only_the_retained_page_identities() {
        let env = make_env().await;
        env.storage.set_page(
            None,
            Page {
                items: vec![item("first", Some("v1"), false)],
                next: Some("final-page".to_string()),
                truncated: true,
            },
        );
        env.storage.set_page(
            Some("final-page"),
            Page {
                items: vec![item("last", Some("v1"), false)],
                next: None,
                truncated: false,
            },
        );

        let (processed, successful, failed, skipped, result) = run(&env).await;
        result.expect("the bucket pass must finish before the simulated crash");
        assert_eq!((processed, successful, failed, skipped), (2, 2, 0, 0));

        let resumed = ResumeManager::load_from_disk(env.healer.disk.clone(), &env.task_id)
            .await
            .unwrap();
        let checkpoint = CheckpointManager::load_from_disk(env.healer.disk.clone(), &env.task_id)
            .await
            .unwrap();
        env.healer
            .execute_heal_with_resume(&["b".to_string()], "pool_0_set_0", &resumed, &checkpoint)
            .await
            .expect("the retained final-page ledger must make recovery exact");

        assert_eq!(
            env.storage.calls(),
            vec![
                ("first".to_string(), Some("v1".to_string())),
                ("last".to_string(), Some("v1".to_string()))
            ]
        );
        let state = resumed.get_state().await;
        assert_eq!(state.successful_objects, 2);
        assert_eq!(state.processed_objects, 2);
    }

    #[tokio::test]
    async fn truncated_page_without_token_retains_its_replay_ledger() {
        let env = make_env().await;
        env.storage.set_page(
            None,
            Page {
                items: vec![item("object", Some("v1"), false)],
                next: None,
                truncated: true,
            },
        );

        let (processed, successful, failed, skipped, result) = run(&env).await;
        result.expect("the tokenless truncated page is a terminal page");
        assert_eq!((processed, successful, failed, skipped), (1, 1, 0, 0));

        let resumed = ResumeManager::load_from_disk(env.healer.disk.clone(), &env.task_id)
            .await
            .unwrap();
        let checkpoint = CheckpointManager::load_from_disk(env.healer.disk.clone(), &env.task_id)
            .await
            .unwrap();
        env.healer
            .execute_heal_with_resume(&["b".to_string()], "pool_0_set_0", &resumed, &checkpoint)
            .await
            .expect("terminal-page recovery must not replay a durable identity");

        assert_eq!(env.storage.calls(), vec![("object".to_string(), Some("v1".to_string()))]);
        let state = resumed.get_state().await;
        assert_eq!(state.successful_objects, 1);
        assert_eq!(state.processed_objects, 1);
    }

    #[tokio::test]
    async fn completed_bucket_reconciles_its_final_page_checkpoint_after_crash() {
        let env = make_env().await;
        env.storage.set_page(
            None,
            Page {
                items: vec![item("object", Some("v1"), false)],
                next: None,
                truncated: false,
            },
        );

        let (_, _, _, _, result) = run(&env).await;
        result.expect("the bucket pass must finish before the simulated crash");
        env.resume.complete_bucket("b").await.unwrap();

        let resumed = ResumeManager::load_from_disk(env.healer.disk.clone(), &env.task_id)
            .await
            .unwrap();
        let checkpoint = CheckpointManager::load_from_disk(env.healer.disk.clone(), &env.task_id)
            .await
            .unwrap();
        env.healer
            .execute_heal_with_resume(&["b".to_string()], "pool_0_set_0", &resumed, &checkpoint)
            .await
            .expect("recovery must finish the checkpoint transition without replaying the bucket");

        assert_eq!(env.storage.calls(), vec![("object".to_string(), Some("v1".to_string()))]);
        let checkpoint = checkpoint.get_checkpoint().await;
        assert_eq!(checkpoint.current_bucket_index, 1);
        assert!(checkpoint.processed_objects.is_empty());
    }

    #[tokio::test]
    async fn test_object_with_versions_spanning_pages_advances() {
        let env = make_env().await;
        env.storage.set_page(
            None,
            Page {
                items: vec![item("obj", Some("v1"), false)],
                next: Some("t1".to_string()),
                truncated: true,
            },
        );
        env.storage.set_page(
            Some("t1"),
            Page {
                items: vec![item("obj", Some("v2"), false)],
                next: None,
                truncated: false,
            },
        );

        let (processed, _s, failed, _sk, result) = run(&env).await;
        result.expect("object whose versions span pages must heal fully");
        assert_eq!(processed, 2);
        assert_eq!(failed, 0);
        let calls = env.storage.calls();
        assert!(calls.contains(&("obj".to_string(), Some("v1".to_string()))));
        assert!(calls.contains(&("obj".to_string(), Some("v2".to_string()))));
    }

    #[tokio::test]
    async fn test_non_advancing_cursor_aborts() {
        let env = make_env().await;
        // Both pages end on the same (name, version) even though the raw token
        // advances (t1 -> t2): identity comparison must detect the stall.
        env.storage.set_page(
            None,
            Page {
                items: vec![item("a", None, false)],
                next: Some("t1".to_string()),
                truncated: true,
            },
        );
        env.storage.set_page(
            Some("t1"),
            Page {
                items: vec![item("a", None, false)],
                next: Some("t2".to_string()),
                truncated: true,
            },
        );

        let (_p, _s, _f, _sk, result) = run(&env).await;
        let err = result.expect_err("a non-advancing cursor must abort the loop");
        assert!(err.to_string().contains("not advancing"), "unexpected error: {err}");
    }

    #[tokio::test]
    async fn test_concurrent_page_dedup_exact_once() {
        let env = make_env().await;
        let items: Vec<HealListItem> = (0..50).map(|i| item(&format!("obj-{i}"), Some("v"), false)).collect();
        env.storage.set_page(
            None,
            Page {
                items,
                next: None,
                truncated: false,
            },
        );

        let (processed, successful, failed, _sk, result) = run(&env).await;
        result.expect("concurrent page must succeed");
        assert_eq!(processed, 50);
        assert_eq!(successful, 50);
        assert_eq!(failed, 0);
        let calls = env.storage.calls();
        assert_eq!(calls.len(), 50, "each version healed exactly once under concurrency");
        let unique: std::collections::HashSet<_> = calls.into_iter().collect();
        assert_eq!(unique.len(), 50, "no version healed twice");
    }

    #[tokio::test]
    async fn test_resume_after_version_deleted_midheal_no_skip() {
        let env = make_env().await;
        // Simulate a resumed pass: (a,v1) was already processed last time, and
        // the cursor points at the in-flight page.
        env.checkpoint
            .add_processed_object(compose_key("a", Some("v1")))
            .await
            .unwrap();
        env.resume.set_resume_cursor(Some("t0".to_string())).await.unwrap();

        env.storage.set_page(
            Some("t0"),
            Page {
                items: vec![
                    item("a", Some("v1"), false), // already done -> deduped
                    item("a", Some("v2"), false), // deleted mid-heal
                    item("c", None, true),        // delete-marker latest, still healed
                ],
                next: None,
                truncated: false,
            },
        );
        // v2 vanished before heal ran.
        env.storage.set_outcome("a", Some("v2"), HealOutcome::VersionNotFound);

        let (_p, _s, failed, skipped, result) = run(&env).await;
        result.expect("resume after mid-heal deletion must succeed");
        // Genuine absence is handled (Ok), never counted as a failure.
        assert_eq!(failed, 0, "a deleted version must not be a failure");
        assert_eq!(skipped, 0, "absence is treated as healed-ok, not skipped");

        let calls = env.storage.calls();
        assert!(
            !calls.contains(&("a".to_string(), Some("v1".to_string()))),
            "already-processed version must be deduped, not re-healed"
        );
        assert!(
            calls.contains(&("a".to_string(), Some("v2".to_string()))),
            "the surviving-but-now-gone version must still be attempted, not skipped"
        );
        assert!(calls.contains(&("c".to_string(), None)), "delete-marker latest must be healed");
    }

    #[tokio::test]
    async fn test_schedule_retry_resets_both_managers_and_reheals() {
        let env = make_env().await;
        // Seed some progress that a retry must discard.
        env.checkpoint
            .add_processed_object(compose_key("a", Some("v1")))
            .await
            .unwrap();
        env.checkpoint.update_position(1, 42).await.unwrap();
        env.resume.set_resume_cursor(Some("t9".to_string())).await.unwrap();
        assert_eq!(env.resume.resume_cursor().await, Some("t9".to_string()));

        // The retry branch calls BOTH together.
        assert!(env.resume.schedule_retry().await.unwrap(), "retry budget should be available");
        env.checkpoint.reset_for_retry().await.unwrap();

        // Resume state: cursor cleared, retry counted, progress zeroed.
        let state = env.resume.get_state().await;
        assert_eq!(env.resume.resume_cursor().await, None, "cursor must be cleared for a full re-scan");
        assert_eq!(state.retry_count, 1);
        // Checkpoint: dedup sets and position cleared so the retry re-heals everything.
        let checkpoint = env.checkpoint.get_checkpoint().await;
        assert!(checkpoint.processed_objects.is_empty());
        assert_eq!(checkpoint.current_object_index, 0);
    }

    #[tokio::test]
    async fn test_transient_error_recorded_as_skipped_not_failed() {
        // A version whose heal hits a transient infrastructure error (offline
        // disk) must be counted as skipped, never failed, and never recorded as a
        // success. Regression guard for backlog#1033. (The per-page checkpoint
        // skipped set is pruned by complete_page once the page advances; the
        // durable skip signal is the counter, which the finalize block gates on.)
        let env = make_env().await;
        env.storage.set_page(
            None,
            Page {
                items: vec![item("ok", None, false), item("down", Some("v1"), false)],
                next: None,
                truncated: false,
            },
        );
        env.storage.set_outcome("down", Some("v1"), HealOutcome::Transient);

        let (_processed, successful, failed, skipped, result) = run(&env).await;
        result.expect("a transient skip must not fail the bucket pass");
        assert_eq!(successful, 1, "the healthy version still heals");
        assert_eq!(failed, 0, "a transient error is not a failure");
        assert_eq!(skipped, 1, "the transient version is skipped");

        // The transient version must actually have been attempted, not silently
        // dropped, so the skip reflects a real unmet heal.
        assert!(
            env.storage.calls().contains(&("down".to_string(), Some("v1".to_string()))),
            "the transient version must be attempted before being skipped"
        );
    }

    #[tokio::test]
    async fn test_transient_skip_pass_not_marked_completed_state_survives() {
        // The finalize block must NOT declare a clean completion when a pass had
        // transient skips (even with zero hard failures): it must not
        // mark_completed and must preserve the resume/checkpoint state (via the
        // bounded-retry mechanism) so a later heal cycle re-heals the skipped
        // versions. Regression guard for backlog#1033 (Transient invariant).
        let env = make_env().await;
        env.storage.set_page(
            None,
            Page {
                items: vec![item("down", Some("v1"), false)],
                next: None,
                truncated: false,
            },
        );
        env.storage.set_outcome("down", Some("v1"), HealOutcome::Transient);

        let result = env
            .healer
            .execute_heal_with_resume(&["b".to_string()], "pool_0_set_0", &env.resume, &env.checkpoint)
            .await;

        result.expect_err("a pass with transient skips must not report clean completion");

        // State must survive for a later pass: not marked completed, and the
        // bounded retry was armed rather than the state being cleaned up.
        let state = env.resume.get_state().await;
        assert!(!state.completed, "a transient-skip pass must not be marked completed");
        assert_eq!(
            state.retry_count, 1,
            "the bounded retry must be armed, preserving state for the next cycle"
        );
        // reset_for_retry clears the skipped set so the next pass re-heals the
        // version instead of suppressing it as already-skipped.
        let checkpoint = env.checkpoint.get_checkpoint().await;
        assert!(
            checkpoint.skipped_objects.is_empty(),
            "the skipped set must be cleared so the retry re-heals the version"
        );
    }

    #[tokio::test]
    async fn replacement_target_missing_from_success_result_retries_the_full_pass() {
        let env = make_env_with_targets(vec!["replacement-a".to_string()]).await;
        env.storage.set_page(
            None,
            Page {
                items: vec![item("object", Some("v1"), false)],
                next: None,
                truncated: false,
            },
        );
        env.storage.set_result(
            "object",
            Some("v1"),
            HealResultItem {
                after: Infos {
                    drives: vec![HealDriveInfo {
                        endpoint: "replacement-a".to_string(),
                        state: "missing".to_string(),
                        ..Default::default()
                    }],
                },
                ..Default::default()
            },
        );

        let result = env
            .healer
            .execute_heal_with_resume(&["b".to_string()], "pool_0_set_0", &env.resume, &env.checkpoint)
            .await;

        result.expect_err("a missing replacement target must not report completion");
        let state = env.resume.get_state().await;
        assert!(!state.completed);
        assert_eq!(state.retry_count, 1);
        assert_eq!(env.storage.calls(), vec![("object".to_string(), Some("v1".to_string()))]);
        assert!(env.checkpoint.get_checkpoint().await.processed_objects.is_empty());
    }

    #[tokio::test]
    async fn replacement_target_readback_evidence_must_confirm_the_healed_version() {
        let env = make_env_with_targets(vec!["replacement-a".to_string()]).await;
        let healer = ErasureSetHealer::new(
            env.storage.clone(),
            Arc::new(RwLock::new(HealProgress::new())),
            CancellationToken::new(),
            env.healer.disk.clone(),
            HealOpts::default(),
            HealRequestSource::AutoHeal,
        )
        .with_replacement_targets(vec!["replacement-a".to_string()], Some("generation-a".to_string()));
        env.storage.set_page(
            None,
            Page {
                items: vec![item("object", Some("v1"), false)],
                next: None,
                truncated: false,
            },
        );
        env.storage.set_result(
            "object",
            Some("v1"),
            HealResultItem {
                after: Infos {
                    drives: vec![HealDriveInfo {
                        endpoint: "replacement-a".to_string(),
                        state: "ok".to_string(),
                        ..Default::default()
                    }],
                },
                ..Default::default()
            },
        );
        env.storage.set_replacement_commit_evidence("object", Some("v1"), false);

        let result = healer
            .execute_heal_with_resume(&["b".to_string()], "pool_0_set_0", &env.resume, &env.checkpoint)
            .await;

        result.expect_err("a success result without target readback evidence must retry");
        assert_eq!(env.resume.get_state().await.retry_count, 1);
        assert!(env.checkpoint.get_checkpoint().await.processed_objects.is_empty());
    }

    #[tokio::test]
    async fn replacement_delete_marker_readback_error_keeps_auto_heal_resumable() {
        let env = make_env_with_targets(vec!["replacement-a".to_string()]).await;
        let healer = ErasureSetHealer::new(
            env.storage.clone(),
            Arc::new(RwLock::new(HealProgress::new())),
            CancellationToken::new(),
            env.healer.disk.clone(),
            HealOpts::default(),
            HealRequestSource::AutoHeal,
        )
        .with_replacement_targets(vec!["replacement-a".to_string()], Some("generation-a".to_string()));
        env.storage.set_page(
            None,
            Page {
                items: vec![item("object", Some("dm-v1"), true)],
                next: None,
                truncated: false,
            },
        );
        env.storage.set_result(
            "object",
            Some("dm-v1"),
            HealResultItem {
                after: Infos {
                    drives: vec![HealDriveInfo {
                        endpoint: "replacement-a".to_string(),
                        state: "ok".to_string(),
                        ..Default::default()
                    }],
                },
                ..Default::default()
            },
        );
        env.storage
            .set_replacement_commit_evidence_error("object", Some("dm-v1"), "injected target readback failure");

        let result = healer
            .execute_heal_with_resume(&["b".to_string()], "pool_0_set_0", &env.resume, &env.checkpoint)
            .await;

        let error = result.expect_err("a target readback error must not complete automatic replacement");
        let error = error.to_string();
        assert!(error.contains("Transient heal skip"), "unexpected error: {error}");
        assert!(error.contains("retry scheduled"), "unexpected error: {error}");
        let state = env.resume.get_state().await;
        assert!(!state.completed, "readback errors must leave the replacement task incomplete");
        assert_eq!(state.retry_count, 1, "readback errors must arm the bounded retry path");
        assert!(env.checkpoint.get_checkpoint().await.processed_objects.is_empty());
        assert_eq!(env.storage.calls(), vec![("object".to_string(), Some("dm-v1".to_string()))]);
    }

    #[tokio::test]
    async fn manual_targeted_heal_keeps_existing_best_effort_result_semantics() {
        let env = make_env_with_targets(vec!["replacement-a".to_string()]).await;
        let healer = ErasureSetHealer::new(
            env.storage.clone(),
            Arc::new(RwLock::new(HealProgress::new())),
            CancellationToken::new(),
            env.healer.disk.clone(),
            HealOpts::default(),
            HealRequestSource::Admin,
        )
        .with_replacement_targets(vec!["replacement-a".to_string()], None);
        env.storage.set_page(
            None,
            Page {
                items: vec![item("object", Some("v1"), false)],
                next: None,
                truncated: false,
            },
        );
        env.storage.set_result(
            "object",
            Some("v1"),
            HealResultItem {
                after: Infos {
                    drives: vec![HealDriveInfo {
                        endpoint: "replacement-a".to_string(),
                        state: "ok".to_string(),
                        ..Default::default()
                    }],
                },
                ..Default::default()
            },
        );
        env.storage.set_replacement_commit_evidence("object", Some("v1"), false);

        healer
            .execute_heal_with_resume(&["b".to_string()], "pool_0_set_0", &env.resume, &env.checkpoint)
            .await
            .expect("manual targeted healing must retain its existing success semantics");

        assert_eq!(env.resume.get_state().await.retry_count, 0);
    }
}
