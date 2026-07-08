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
    progress::HealProgress,
    resume::{CheckpointManager, ResumeManager, ResumeUtils, compose_key},
    storage::{HealStorageAPI, next_heal_listing_token},
    task::is_missing_object_dir_heal_result,
};
use crate::{Error, Result};
use futures::{StreamExt, stream::FuturesUnordered};
use metrics::gauge;
use rustfs_common::heal_channel::{HealOpts, HealRequestSource, HealScanMode};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
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
        }
    }

    /// execute erasure set heal with resume
    #[tracing::instrument(skip(self, buckets), fields(set_disk_id = %set_disk_id, bucket_count = buckets.len()))]
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

        // 4. cleanup resume state
        if result.is_ok() {
            if let Err(e) = resume_manager.cleanup().await {
                warn!(
                    target: "rustfs::heal::erasure_healer",
                    event = EVENT_HEAL_ERASURE_RESUME_STATE,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_ERASURE_HEALER,
                    set_disk_id,
                    state = "resume_cleanup_failed",
                    error = %e,
                    "Erasure set resume cleanup failed"
                );
            }
            if let Err(e) = checkpoint_manager.cleanup().await {
                warn!(
                    target: "rustfs::heal::erasure_healer",
                    event = EVENT_HEAL_ERASURE_RESUME_STATE,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_ERASURE_HEALER,
                    set_disk_id,
                    state = "checkpoint_cleanup_failed",
                    error = %e,
                    "Erasure set checkpoint cleanup failed"
                );
            }
        }

        result
    }

    /// get or create task id
    async fn get_or_create_task_id(&self, set_disk_id: &str) -> Result<String> {
        // check if there are resumable tasks
        let resumable_tasks = ResumeUtils::get_resumable_tasks(&self.disk).await?;

        for task_id in resumable_tasks {
            match ResumeManager::load_from_disk(self.disk.clone(), &task_id).await {
                Ok(manager) => {
                    let state = manager.get_state().await;
                    if state.set_disk_id == set_disk_id && ResumeUtils::can_resume_task(&self.disk, &task_id).await {
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
        let task_id = format!("{}_{}", set_disk_id, ResumeUtils::generate_task_id());
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
        // check if resume state exists
        if ResumeManager::has_resume_state(&self.disk, task_id).await {
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

            let resume_manager = ResumeManager::load_from_disk(self.disk.clone(), task_id).await?;
            let checkpoint_manager = if CheckpointManager::has_checkpoint(&self.disk, task_id).await {
                CheckpointManager::load_from_disk(self.disk.clone(), task_id).await?
            } else {
                CheckpointManager::new(self.disk.clone(), task_id.to_string()).await?
            };

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

        // 3. continue from checkpoint
        let current_bucket_index = checkpoint.current_bucket_index;
        let mut current_object_index = checkpoint.current_object_index;

        let mut processed_objects = state.processed_objects;
        let mut successful_objects = state.successful_objects;
        let mut failed_objects = state.failed_objects;
        let mut skipped_objects = state.skipped_objects;

        // 4. process remaining buckets
        for (bucket_idx, bucket) in buckets.iter().enumerate().skip(current_bucket_index) {
            // check if completed
            if state.completed_buckets.contains(bucket) {
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
                )
                .await;

            // update checkpoint position
            checkpoint_manager.update_position(bucket_idx, current_object_index).await?;

            // update progress
            resume_manager
                .update_progress(processed_objects, successful_objects, failed_objects, skipped_objects)
                .await?;

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
                Err(e) => {
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
                    // continue to next bucket, do not interrupt the whole process
                }
            }

            // reset object index
            current_object_index = 0;
        }

        // 5. finalize. Only declare the set healed when nothing failed —
        // otherwise the resume/checkpoint state must survive so the failures are
        // retried instead of being silently marked "completed" and discarded
        // (backlog#855 / #799 B6).
        if failed_objects > 0 {
            if resume_manager.schedule_retry().await? {
                // Both persistence layers must be reset together: schedule_retry
                // rewinds the resume state (cursor + counters), and the
                // checkpoint's per-version dedup sets + position must be cleared
                // in lockstep, or the retry would skip the very versions it is
                // meant to re-heal.
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
                    failed_objects,
                    state = "retry_scheduled",
                    "Erasure set heal pass finished with failures; scheduled full re-heal retry"
                );
                return Err(Error::other(format!(
                    "Erasure set heal incomplete: {failed_objects} object(s) failed; retry scheduled"
                )));
            }

            // Retry budget exhausted: drop the resume/checkpoint state so this
            // task does not loop, but keep the healing markers (return Err) so a
            // later heal cycle / the background scanner starts a fresh attempt.
            // Never silently claim a clean completion while objects are unhealed.
            error!(
                target: "rustfs::heal::erasure_healer",
                event = EVENT_HEAL_ERASURE_RESUME_STATE,
                component = LOG_COMPONENT_HEAL,
                subsystem = LOG_SUBSYSTEM_ERASURE_HEALER,
                set_disk_id,
                failed_objects,
                state = "failed_after_retries",
                "Erasure set heal exhausted retries with unrecovered failures"
            );
            let _ = resume_manager.cleanup().await;
            let _ = checkpoint_manager.cleanup().await;
            return Err(Error::other(format!(
                "Erasure set heal exhausted retries with {failed_objects} unrecovered object(s)"
            )));
        }

        // no failures — mark task completed
        resume_manager.mark_completed().await?;

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

        loop {
            // Get one page of object versions
            let (objects, next_token, is_truncated) = self
                .storage
                .list_objects_for_heal_page(bucket, "", continuation_token.as_deref())
                .await?;
            let checkpoint = checkpoint_manager.get_checkpoint().await;
            let page_resume_index = *current_object_index;
            let semaphore = Arc::new(Semaphore::new(page_concurrency_limit));
            let mut page_tasks = FuturesUnordered::new();

            // Capture the last version identity of this page for the anti-loop guard.
            let page_last = objects.last().map(|item| (item.name.clone(), item.version_id.clone()));

            for item in objects {
                // current_object_index is now only a progress metric; the cursor
                // drives resume, so we no longer skip by position.
                global_obj_idx += 1;

                // Per-version dedup identity — the single canonical key.
                let key = compose_key(&item.name, item.version_id.as_deref());
                if checkpoint.processed_objects.contains(&key) || checkpoint.skipped_objects.contains(&key) {
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

                page_tasks.push(async move {
                    let permit = semaphore
                        .clone()
                        .acquire_owned()
                        .await
                        .map_err(|e| Error::other(format!("Failed to acquire page concurrency permit: {e}")));

                    let _permit = match permit {
                        Ok(permit) => permit,
                        Err(err) => return (dedup_key, object_name, version_id, Err(err)),
                    };

                    let current_in_flight = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                    gauge!(
                        "rustfs_heal_page_concurrency_current",
                        "set" => set_label.clone()
                    )
                    .set(current_in_flight as f64);

                    // Always go through heal_object. Genuine absence flows through
                    // heal_object -> FileVersionNotFound/FileNotFound ->
                    // classify_heal_object_error -> Absent, so gone versions are
                    // recorded as skipped-ok rather than failed. The delete-marker
                    // vs data path is chosen internally in ops/heal.rs.
                    let result = if cancel_token.is_cancelled() {
                        Err(Error::TaskCancelled)
                    } else {
                        match storage
                            .heal_object(&bucket_name, &object_name, version_id.as_deref(), &heal_opts)
                            .await
                        {
                            Ok((_result, None)) => Ok(true),
                            Ok((_, Some(err))) if is_missing_object_dir_heal_result(&object_name, &err) => Ok(false),
                            Ok((_, Some(err))) | Err(err) => match Self::classify_heal_object_error(&err) {
                                HealObjectOutcome::Absent => Ok(false),
                                HealObjectOutcome::Transient => Err(Error::transient_skip(format!(
                                    "Skipped heal for {bucket_name}/{object_name} due to transient error: {err}"
                                ))),
                                HealObjectOutcome::Failed => Err(err),
                            },
                        }
                    };

                    let current = in_flight.fetch_sub(1, Ordering::SeqCst) - 1;
                    gauge!(
                        "rustfs_heal_page_concurrency_current",
                        "set" => set_label.clone()
                    )
                    .set(current as f64);

                    (dedup_key, object_name, version_id, result)
                });
            }

            let mut completed_in_page = 0usize;
            while let Some((key, object, version_id, result)) = page_tasks.next().await {
                match result {
                    Ok(true) => {
                        *successful_objects += 1;
                        checkpoint_manager.add_processed_object(key).await?;
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
                    }
                    Ok(false) => {
                        checkpoint_manager.add_processed_object(key).await?;
                        *successful_objects += 1;
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
                    }
                    Err(Error::TaskCancelled) => {
                        gauge!(
                            "rustfs_heal_page_concurrency_current",
                            "set" => set_disk_id.to_string()
                        )
                        .set(0.0);
                        return Err(Error::TaskCancelled);
                    }
                    Err(Error::TransientSkip { message }) => {
                        *skipped_objects += 1;
                        checkpoint_manager.add_skipped_object(key).await?;
                        warn!(
                            target: "rustfs::heal::erasure_healer",
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
                        );
                    }
                    Err(err) => {
                        *failed_objects += 1;
                        checkpoint_manager.add_failed_object(key).await?;
                        warn!(
                            target: "rustfs::heal::erasure_healer",
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
                        );
                    }
                }

                *processed_objects += 1;
                completed_in_page += 1;

                if completed_in_page.is_multiple_of(100) {
                    checkpoint_manager.update_position(bucket_index, page_resume_index).await?;
                }
            }

            *current_object_index = global_obj_idx;

            // Persist the authoritative cursor FIRST (points at the next page
            // boundary), then prune the per-version dedup sets. Both are
            // idempotent under crash: heal_object re-heals safely.
            let next_cursor = if is_truncated { next_token.clone() } else { None };
            resume_manager.set_resume_cursor(next_cursor.clone()).await?;
            checkpoint_manager.complete_page(bucket_index, *current_object_index).await?;
            gauge!(
                "rustfs_heal_page_concurrency_current",
                "set" => set_disk_id.to_string()
            )
            .set(0.0);

            // Check if there are more pages
            if !is_truncated {
                break;
            }

            // Anti-loop guard: if the backend keeps reporting truncation but the
            // last version identity did not advance, we would spin forever.
            if page_last.is_some() && page_last == previous_page_last {
                return Err(Error::other(format!(
                    "Erasure set heal listing for bucket {bucket} is not advancing (repeated last version {page_last:?}); aborting to avoid an infinite loop"
                )));
            }
            previous_page_last = page_last;

            continuation_token = next_heal_listing_token(bucket, "", next_token, is_truncated)?;
            if continuation_token.is_none() {
                // Truncated but no continuation token: treat as end of listing.
                break;
            }
        }

        Ok(())
    }

    /// initialize progress tracking
    async fn initialize_progress(&self, _buckets: &[String], state: &crate::heal::resume::ResumeState) {
        let mut progress = self.progress.write().await;
        progress.objects_scanned = state.total_objects;
        progress.objects_healed = state.successful_objects;
        progress.objects_failed = state.failed_objects;
        progress.bytes_processed = 0; // set to 0 for now, can be extended later
        progress.set_current_object(state.current_object.clone());
    }
}

#[cfg(test)]
mod tests {
    use super::ErasureSetHealer;
    use rustfs_common::heal_channel::{HealRequestSource, HealScanMode};

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
    use super::ErasureSetHealer;
    use crate::heal::progress::HealProgress;
    use crate::heal::resume::{CheckpointManager, ResumeManager, compose_key};
    use crate::heal::storage::{DiskStatus, HealListItem, HealObjectInfo, HealStorageAPI};
    use crate::heal::storage_api::status::BucketInfo;
    use crate::heal::{
        BUCKET_META_PREFIX, DiskOption, DiskStore, EcstoreError, Endpoint, HealDiskExt as _, RUSTFS_META_BUCKET, new_disk,
    };
    use crate::{Error, Result};
    use rustfs_common::heal_channel::{HealOpts, HealRequestSource};
    use rustfs_madmin::heal_commands::HealResultItem;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};
    use tempfile::TempDir;
    use tokio::sync::RwLock;
    use tokio_util::sync::CancellationToken;

    fn item(name: &str, version: Option<&str>, delete_marker: bool) -> HealListItem {
        HealListItem {
            name: name.to_string(),
            version_id: version.map(str::to_string),
            is_delete_marker: delete_marker,
        }
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
    }

    #[derive(Default)]
    struct FakeStorage {
        /// page keyed by the *incoming* continuation token
        pages: Mutex<HashMap<Option<String>, Page>>,
        /// per-`compose_key` heal outcome; default is `Ok`
        outcomes: Mutex<HashMap<String, HealOutcome>>,
        /// every heal_object call recorded as (name, version_id)
        heal_calls: Mutex<Vec<(String, Option<String>)>>,
    }

    impl FakeStorage {
        fn set_page(&self, token: Option<&str>, page: Page) {
            self.pages.lock().unwrap().insert(token.map(str::to_string), page);
        }
        fn set_outcome(&self, name: &str, version: Option<&str>, outcome: HealOutcome) {
            self.outcomes.lock().unwrap().insert(compose_key(name, version), outcome);
        }
        fn calls(&self) -> Vec<(String, Option<String>)> {
            self.heal_calls.lock().unwrap().clone()
        }
    }

    #[async_trait::async_trait]
    impl HealStorageAPI for FakeStorage {
        async fn get_object_meta(&self, _b: &str, _o: &str) -> Result<Option<HealObjectInfo>> {
            Ok(None)
        }
        async fn get_object_data(&self, _b: &str, _o: &str) -> Result<Option<Vec<u8>>> {
            Ok(None)
        }
        async fn put_object_data(&self, _b: &str, _o: &str, _d: &[u8]) -> Result<()> {
            Ok(())
        }
        async fn delete_object(&self, _b: &str, _o: &str) -> Result<()> {
            Ok(())
        }
        async fn verify_object_integrity(&self, _b: &str, _o: &str) -> Result<bool> {
            Ok(true)
        }
        async fn ec_decode_rebuild(&self, _b: &str, _o: &str) -> Result<Vec<u8>> {
            Ok(Vec::new())
        }
        async fn get_disk_status(&self, _e: &Endpoint) -> Result<DiskStatus> {
            Ok(DiskStatus::Ok)
        }
        async fn format_disk(&self, _e: &Endpoint) -> Result<()> {
            Ok(())
        }
        async fn get_bucket_info(&self, bucket: &str) -> Result<Option<BucketInfo>> {
            Ok(Some(BucketInfo {
                name: bucket.to_string(),
                ..Default::default()
            }))
        }
        async fn heal_bucket_metadata(&self, _b: &str) -> Result<()> {
            Ok(())
        }
        async fn list_buckets(&self) -> Result<Vec<BucketInfo>> {
            Ok(Vec::new())
        }
        async fn object_exists(&self, _b: &str, _o: &str) -> Result<bool> {
            // Must never be consulted: the resume loop always goes through heal_object.
            panic!("object_exists must not be called by the resume heal loop");
        }
        async fn get_object_size(&self, _b: &str, _o: &str) -> Result<Option<u64>> {
            Ok(None)
        }
        async fn get_object_checksum(&self, _b: &str, _o: &str) -> Result<Option<String>> {
            Ok(None)
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
                HealOutcome::Ok => Ok((HealResultItem::default(), None)),
                HealOutcome::VersionNotFound => {
                    Ok((HealResultItem::default(), Some(Error::Storage(EcstoreError::FileVersionNotFound))))
                }
            }
        }
        async fn heal_bucket(&self, _b: &str, _o: &HealOpts) -> Result<HealResultItem> {
            Ok(HealResultItem::default())
        }
        async fn heal_format(&self, _dry: bool) -> Result<(HealResultItem, Option<Error>)> {
            Ok((HealResultItem::default(), None))
        }
        async fn list_objects_for_heal(&self, _b: &str, _p: &str) -> Result<Vec<HealListItem>> {
            Ok(Vec::new())
        }
        async fn list_objects_for_heal_page(
            &self,
            _bucket: &str,
            _prefix: &str,
            continuation_token: Option<&str>,
        ) -> Result<(Vec<HealListItem>, Option<String>, bool)> {
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
        _temp: TempDir,
    }

    async fn make_env() -> Env {
        let temp = TempDir::new().unwrap();
        let disk = make_disk(&temp).await;
        let storage = Arc::new(FakeStorage::default());
        let healer = ErasureSetHealer::new(
            storage.clone(),
            Arc::new(RwLock::new(HealProgress::new())),
            CancellationToken::new(),
            disk.clone(),
            HealOpts::default(),
            HealRequestSource::Internal,
        );
        let resume = ResumeManager::new(
            disk.clone(),
            "task".to_string(),
            "erasure_set".to_string(),
            "pool_0_set_0".to_string(),
            vec!["b".to_string()],
        )
        .await
        .unwrap();
        let checkpoint = CheckpointManager::new(disk, "task".to_string()).await.unwrap();
        Env {
            healer,
            storage,
            resume,
            checkpoint,
            _temp: temp,
        }
    }

    /// Drive one bucket heal pass; returns (processed, successful, failed, skipped, result).
    async fn run(env: &Env) -> (u64, u64, u64, u64, Result<()>) {
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
        // Final page not truncated => cursor cleared.
        assert_eq!(env.resume.resume_cursor().await, None);
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
}
