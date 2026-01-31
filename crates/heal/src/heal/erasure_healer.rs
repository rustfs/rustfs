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
use futures::future::join_all;
use rustfs_common::heal_channel::{HealOpts, HealScanMode};
use rustfs_ecstore::disk::DiskStore;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{error, info, warn};

/// Erasure Set Healer
pub struct ErasureSetHealer {
    storage: Arc<dyn HealStorageAPI>,
    progress: Arc<RwLock<HealProgress>>,
    cancel_token: tokio_util::sync::CancellationToken,
    disk: DiskStore,
}

impl ErasureSetHealer {
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
            .execute_heal_with_resume(buckets, &resume_manager, &checkpoint_manager)
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
    #[tracing::instrument(skip(self, current_object_index, processed_objects, successful_objects, failed_objects, _skipped_objects, resume_manager, checkpoint_manager), fields(bucket = %bucket, bucket_index = bucket_index))]
    async fn heal_bucket_with_resume(
        &self,
        bucket: &str,
        bucket_index: usize,
        current_object_index: &mut usize,
        processed_objects: &mut u64,
        successful_objects: &mut u64,
        failed_objects: &mut u64,
        _skipped_objects: &mut u64,
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

        loop {
            // Get one page of objects
            let (objects, next_token, is_truncated) = self
                .storage
                .list_objects_for_heal_page(bucket, "", continuation_token.as_deref())
                .await?;

            // Process objects in this page
            for object in objects {
                // Skip objects before the checkpoint
                if global_obj_idx < *current_object_index {
                    global_obj_idx += 1;
                    continue;
                }

                // check if already processed
                if checkpoint_manager.get_checkpoint().await.processed_objects.contains(&object) {
                    global_obj_idx += 1;
                    continue;
                }

                // update current object
                resume_manager
                    .set_current_item(Some(bucket.to_string()), Some(object.clone()))
                    .await?;

                // Check if object still exists before attempting heal
                let object_exists = match self.storage.object_exists(bucket, &object).await {
                    Ok(exists) => exists,
                    Err(e) => {
                        warn!("Failed to check existence of {}/{}: {}, marking as failed", bucket, object, e);
                        *failed_objects += 1;
                        checkpoint_manager.add_failed_object(object.clone()).await?;
                        global_obj_idx += 1;
                        *current_object_index = global_obj_idx;
                        continue;
                    }
                };

                if !object_exists {
                    info!(
                        target: "rustfs:heal:heal_bucket_with_resume" ,"Object {}/{} no longer exists, skipping heal (likely deleted intentionally)",
                        bucket, object
                    );
                    checkpoint_manager.add_processed_object(object.clone()).await?;
                    *successful_objects += 1; // Treat as successful - object is gone as intended
                    global_obj_idx += 1;
                    *current_object_index = global_obj_idx;
                    continue;
                }

                // heal object
                let heal_opts = HealOpts {
                    scan_mode: HealScanMode::Normal,
                    remove: true,
                    recreate: true, // Keep recreate enabled for legitimate heal scenarios
                    ..Default::default()
                };

                match self.storage.heal_object(bucket, &object, None, &heal_opts).await {
                    Ok((_result, None)) => {
                        *successful_objects += 1;
                        checkpoint_manager.add_processed_object(object.clone()).await?;
                        info!("Successfully healed object {}/{}", bucket, object);
                    }
                    Ok((_, Some(err))) => {
                        *failed_objects += 1;
                        checkpoint_manager.add_failed_object(object.clone()).await?;
                        warn!("Failed to heal object {}/{}: {}", bucket, object, err);
                    }
                    Err(err) => {
                        *failed_objects += 1;
                        checkpoint_manager.add_failed_object(object.clone()).await?;
                        warn!("Error healing object {}/{}: {}", bucket, object, err);
                    }
                }

                *processed_objects += 1;
                global_obj_idx += 1;
                *current_object_index = global_obj_idx;

                // check cancel status
                if self.cancel_token.is_cancelled() {
                    info!("Heal task cancelled during object processing");
                    return Err(Error::TaskCancelled);
                }

                // save checkpoint periodically
                if global_obj_idx.is_multiple_of(100) {
                    checkpoint_manager
                        .update_position(bucket_index, *current_object_index)
                        .await?;
                }
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
