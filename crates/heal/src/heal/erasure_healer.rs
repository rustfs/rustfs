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
    resume::{CheckpointManager, ResumeManager, ResumeUtils},
    storage::HealStorageAPI,
};
use crate::{Error, Result};
use futures::{StreamExt, future::join_all, stream::FuturesUnordered};
use metrics::gauge;
use rustfs_common::heal_channel::{HealOpts, HealScanMode};
use rustfs_ecstore::disk::DiskStore;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tokio::sync::{RwLock, Semaphore};
use tracing::{error, info, warn};

/// Erasure Set Healer
pub struct ErasureSetHealer {
    storage: Arc<dyn HealStorageAPI>,
    progress: Arc<RwLock<HealProgress>>,
    cancel_token: tokio_util::sync::CancellationToken,
    disk: DiskStore,
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

    pub fn new(
        storage: Arc<dyn HealStorageAPI>,
        progress: Arc<RwLock<HealProgress>>,
        cancel_token: tokio_util::sync::CancellationToken,
        disk: DiskStore,
    ) -> Self {
        Self {
            storage,
            progress,
            cancel_token,
            disk,
        }
    }

    /// execute erasure set heal with resume
    #[tracing::instrument(skip(self, buckets), fields(set_disk_id = %set_disk_id, bucket_count = buckets.len()))]
    pub async fn heal_erasure_set(&self, buckets: &[String], set_disk_id: &str) -> Result<()> {
        info!("Starting erasure set heal");

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
                warn!("Failed to cleanup resume state: {}", e);
            }
            if let Err(e) = checkpoint_manager.cleanup().await {
                warn!("Failed to cleanup checkpoint: {}", e);
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
                        info!("Found resumable task: {} for set {}", task_id, set_disk_id);
                        return Ok(task_id);
                    }
                }
                Err(e) => {
                    warn!("Failed to load resume state for task {}: {}", task_id, e);
                }
            }
        }

        // create new task id
        let task_id = format!("{}_{}", set_disk_id, ResumeUtils::generate_task_id());
        info!("Created new heal task: {}", task_id);
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
            info!("Loading existing resume state for task: {}", task_id);

            let resume_manager = ResumeManager::load_from_disk(self.disk.clone(), task_id).await?;
            let checkpoint_manager = if CheckpointManager::has_checkpoint(&self.disk, task_id).await {
                CheckpointManager::load_from_disk(self.disk.clone(), task_id).await?
            } else {
                CheckpointManager::new(self.disk.clone(), task_id.to_string()).await?
            };

            Ok((resume_manager, checkpoint_manager))
        } else {
            info!("Creating new resume state for task: {}", task_id);

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

        info!(
            "Resuming from bucket {} object {}",
            checkpoint.current_bucket_index, checkpoint.current_object_index
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
                warn!("Heal task cancelled");
                return Err(Error::TaskCancelled);
            }

            // process bucket result
            match bucket_result {
                Ok(_) => {
                    resume_manager.complete_bucket(bucket).await?;
                    info!("Completed heal for bucket: {}", bucket);
                }
                Err(e) => {
                    error!("Failed to heal bucket {}: {}", bucket, e);
                    // continue to next bucket, do not interrupt the whole process
                }
            }

            // reset object index
            current_object_index = 0;
        }

        // 5. mark task completed
        resume_manager.mark_completed().await?;

        info!("Erasure set heal completed successfully");
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
        info!(target: "rustfs:heal:heal_bucket_with_resume" ,"Starting heal for bucket from object index {}", current_object_index);

        // 1. get bucket info
        let _bucket_info = match self.storage.get_bucket_info(bucket).await? {
            Some(info) => info,
            None => {
                warn!("Bucket {} not found, skipping", bucket);
                return Ok(());
            }
        };

        // 2. process objects with pagination to avoid loading all objects into memory
        let mut continuation_token: Option<String> = None;
        let mut global_obj_idx = 0usize;
        let page_concurrency_limit = Self::effective_heal_page_object_concurrency();
        let in_flight = Arc::new(AtomicUsize::new(0));

        loop {
            // Get one page of objects
            let (objects, next_token, is_truncated) = self
                .storage
                .list_objects_for_heal_page(bucket, "", continuation_token.as_deref())
                .await?;
            let checkpoint = checkpoint_manager.get_checkpoint().await;
            let page_resume_index = *current_object_index;
            let semaphore = Arc::new(Semaphore::new(page_concurrency_limit));
            let mut page_tasks = FuturesUnordered::new();

            for object in objects {
                let object_idx = global_obj_idx;
                global_obj_idx += 1;

                if object_idx < *current_object_index {
                    continue;
                }

                if checkpoint.processed_objects.contains(&object) || checkpoint.skipped_objects.contains(&object) {
                    continue;
                }

                resume_manager
                    .set_current_item(Some(bucket.to_string()), Some(object.clone()))
                    .await?;

                let storage = self.storage.clone();
                let bucket_name = bucket.to_string();
                let object_name = object.clone();
                let cancel_token = self.cancel_token.clone();
                let in_flight = in_flight.clone();
                let set_label = set_disk_id.to_string();
                let permit = semaphore
                    .clone()
                    .acquire_owned()
                    .await
                    .map_err(|e| Error::other(format!("Failed to acquire page concurrency permit: {e}")))?;

                let current_in_flight = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                gauge!(
                    "rustfs_heal_page_concurrency_current",
                    "set" => set_label.clone()
                )
                .set(current_in_flight as f64);

                page_tasks.push(async move {
                    let _permit = permit;
                    let result = if cancel_token.is_cancelled() {
                        Err(Error::TaskCancelled)
                    } else {
                        let object_exists = match storage.object_exists(&bucket_name, &object_name).await {
                            Ok(exists) => exists,
                            Err(err @ Error::TransientSkip { .. }) => {
                                let current = in_flight.fetch_sub(1, Ordering::SeqCst) - 1;
                                gauge!(
                                    "rustfs_heal_page_concurrency_current",
                                    "set" => set_label.clone()
                                )
                                .set(current as f64);
                                return (object_name, Err(err));
                            }
                            Err(err) => {
                                let object_name_for_error = object_name.clone();
                                let current = in_flight.fetch_sub(1, Ordering::SeqCst) - 1;
                                gauge!(
                                    "rustfs_heal_page_concurrency_current",
                                    "set" => set_label.clone()
                                )
                                .set(current as f64);
                                return (
                                    object_name,
                                    Err(Error::other(format!(
                                        "Failed to check existence of {}/{}: {}",
                                        bucket_name, object_name_for_error, err
                                    ))),
                                );
                            }
                        };

                        if !object_exists {
                            Ok(false)
                        } else {
                            let heal_opts = HealOpts {
                                scan_mode: HealScanMode::Normal,
                                remove: true,
                                recreate: true,
                                ..Default::default()
                            };
                            match storage.heal_object(&bucket_name, &object_name, None, &heal_opts).await {
                                Ok((_result, None)) => Ok(true),
                                Ok((_, Some(err))) => Err(Error::other(err)),
                                Err(err) => Err(err),
                            }
                        }
                    };

                    let current = in_flight.fetch_sub(1, Ordering::SeqCst) - 1;
                    gauge!(
                        "rustfs_heal_page_concurrency_current",
                        "set" => set_label.clone()
                    )
                    .set(current as f64);

                    (object_name, result)
                });
            }

            let mut completed_in_page = 0usize;
            while let Some((object, result)) = page_tasks.next().await {
                match result {
                    Ok(true) => {
                        *successful_objects += 1;
                        checkpoint_manager.add_processed_object(object.clone()).await?;
                        info!("Successfully healed object {}/{}", bucket, object);
                    }
                    Ok(false) => {
                        checkpoint_manager.add_processed_object(object.clone()).await?;
                        *successful_objects += 1;
                        info!(
                            target: "rustfs:heal:heal_bucket_with_resume" ,"Object {}/{} no longer exists, skipping heal (likely deleted intentionally)",
                            bucket, object
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
                        checkpoint_manager.add_skipped_object(object.clone()).await?;
                        warn!(
                            "Skipping heal for object {}/{} due to transient existence check error: {}",
                            bucket, object, message
                        );
                    }
                    Err(err) => {
                        *failed_objects += 1;
                        checkpoint_manager.add_failed_object(object.clone()).await?;
                        warn!("Error healing object {}/{}: {}", bucket, object, err);
                    }
                }

                *processed_objects += 1;
                completed_in_page += 1;

                if completed_in_page.is_multiple_of(100) {
                    checkpoint_manager.update_position(bucket_index, page_resume_index).await?;
                }
            }

            *current_object_index = global_obj_idx;
            checkpoint_manager
                .update_position(bucket_index, *current_object_index)
                .await?;
            gauge!(
                "rustfs_heal_page_concurrency_current",
                "set" => set_disk_id.to_string()
            )
            .set(0.0);

            // Check if there are more pages
            if !is_truncated {
                break;
            }

            continuation_token = next_token;
            if continuation_token.is_none() {
                warn!("List is truncated but no continuation token provided for {}", bucket);
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

    /// heal all buckets concurrently
    #[allow(dead_code)]
    async fn heal_buckets_concurrently(&self, buckets: &[String]) -> Vec<Result<()>> {
        // use semaphore to control concurrency, avoid too many concurrent healings
        let semaphore = Arc::new(tokio::sync::Semaphore::new(4)); // max 4 concurrent healings

        let heal_futures = buckets.iter().map(|bucket| {
            let bucket = bucket.clone();
            let storage = self.storage.clone();
            let progress = self.progress.clone();
            let semaphore = semaphore.clone();
            let cancel_token = self.cancel_token.clone();

            async move {
                let _permit = semaphore
                    .acquire()
                    .await
                    .map_err(|e| Error::other(format!("Failed to acquire semaphore for bucket heal: {e}")))?;

                if cancel_token.is_cancelled() {
                    return Err(Error::TaskCancelled);
                }

                Self::heal_single_bucket(&storage, &bucket, &progress).await
            }
        });

        // use join_all to process concurrently
        join_all(heal_futures).await
    }

    /// heal single bucket
    #[allow(dead_code)]
    async fn heal_single_bucket(
        storage: &Arc<dyn HealStorageAPI>,
        bucket: &str,
        progress: &Arc<RwLock<HealProgress>>,
    ) -> Result<()> {
        info!("Starting heal for bucket: {}", bucket);

        // 1. get bucket info
        let _bucket_info = match storage.get_bucket_info(bucket).await? {
            Some(info) => info,
            None => {
                warn!("Bucket {} not found, skipping", bucket);
                return Ok(());
            }
        };

        // 2. process objects with pagination to avoid loading all objects into memory
        let mut continuation_token: Option<String> = None;
        let mut total_scanned = 0u64;
        let mut total_success = 0u64;
        let mut total_failed = 0u64;

        let heal_opts = HealOpts {
            scan_mode: HealScanMode::Normal,
            remove: true,   // remove corrupted data
            recreate: true, // recreate missing data
            ..Default::default()
        };

        loop {
            // Get one page of objects
            let (objects, next_token, is_truncated) = storage
                .list_objects_for_heal_page(bucket, "", continuation_token.as_deref())
                .await?;

            let page_count = objects.len() as u64;
            total_scanned += page_count;

            // 3. update progress
            {
                let mut p = progress.write().await;
                p.objects_scanned = total_scanned;
            }

            // 4. heal objects concurrently for this page
            let object_results = Self::heal_objects_concurrently(storage, bucket, &objects, &heal_opts, progress).await;

            // 5. count results for this page
            let (success_count, failure_count) =
                object_results
                    .into_iter()
                    .fold((0, 0), |(success, failure), result| match result {
                        Ok(_) => (success + 1, failure),
                        Err(_) => (success, failure + 1),
                    });

            total_success += success_count;
            total_failed += failure_count;

            // 6. update progress
            {
                let mut p = progress.write().await;
                p.objects_healed = total_success;
                p.objects_failed = total_failed;
                p.set_current_object(Some(format!("processing bucket: {bucket} (page)")));
            }

            // Check if there are more pages
            if !is_truncated {
                break;
            }

            continuation_token = next_token;
            if continuation_token.is_none() {
                warn!("List is truncated but no continuation token provided for {}", bucket);
                break;
            }
        }

        // 7. final progress update
        {
            let mut p = progress.write().await;
            p.set_current_object(Some(format!("completed bucket: {bucket}")));
        }

        info!(
            "Completed heal for bucket {}: {} success, {} failures (total scanned: {})",
            bucket, total_success, total_failed, total_scanned
        );

        Ok(())
    }

    /// heal objects concurrently
    #[allow(dead_code)]
    async fn heal_objects_concurrently(
        storage: &Arc<dyn HealStorageAPI>,
        bucket: &str,
        objects: &[String],
        heal_opts: &HealOpts,
        _progress: &Arc<RwLock<HealProgress>>,
    ) -> Vec<Result<()>> {
        // use semaphore to control object healing concurrency
        let semaphore = Arc::new(tokio::sync::Semaphore::new(8)); // max 8 concurrent object healings

        let heal_futures = objects.iter().map(|object| {
            let object = object.clone();
            let bucket = bucket.to_string();
            let storage = storage.clone();
            let heal_opts = *heal_opts;
            let semaphore = semaphore.clone();

            async move {
                let _permit = semaphore
                    .acquire()
                    .await
                    .map_err(|e| Error::other(format!("Failed to acquire semaphore for object heal: {e}")))?;

                match storage.heal_object(&bucket, &object, None, &heal_opts).await {
                    Ok((_result, None)) => {
                        info!("Successfully healed object {}/{}", bucket, object);
                        Ok(())
                    }
                    Ok((_, Some(err))) => {
                        warn!("Failed to heal object {}/{}: {}", bucket, object, err);
                        Err(Error::other(err))
                    }
                    Err(err) => {
                        warn!("Error healing object {}/{}: {}", bucket, object, err);
                        Err(err)
                    }
                }
            }
        });

        join_all(heal_futures).await
    }

    /// process results
    #[allow(dead_code)]
    async fn process_results(&self, results: Vec<Result<()>>) -> Result<()> {
        let (success_count, failure_count): (usize, usize) =
            results.into_iter().fold((0, 0), |(success, failure), result| match result {
                Ok(_) => (success + 1, failure),
                Err(_) => (success, failure + 1),
            });

        let total = success_count + failure_count;

        info!("Erasure set heal completed: {}/{} buckets successful", success_count, total);

        if failure_count > 0 {
            warn!("{} buckets failed to heal", failure_count);
            return Err(Error::other(format!("{failure_count} buckets failed to heal")));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::ErasureSetHealer;

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
}
