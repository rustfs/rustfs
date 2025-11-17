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

use crate::heal::{ErasureSetHealer, progress::HealProgress, storage::HealStorageAPI};
use crate::{Error, Result};
use rustfs_common::heal_channel::{HealOpts, HealScanMode};
use serde::{Deserialize, Serialize};
use std::{
    future::Future,
    sync::Arc,
    time::{Duration, Instant, SystemTime},
};
use tokio::sync::RwLock;
use tracing::{error, info, warn};
use uuid::Uuid;

/// Heal type
#[derive(Debug, Clone)]
pub enum HealType {
    /// Object heal
    Object {
        bucket: String,
        object: String,
        version_id: Option<String>,
    },
    /// Bucket heal
    Bucket { bucket: String },
    /// Erasure Set heal (includes disk format repair)
    ErasureSet { buckets: Vec<String>, set_disk_id: String },
    /// Metadata heal
    Metadata { bucket: String, object: String },
    /// MRF heal
    MRF { meta_path: String },
    /// EC decode heal
    ECDecode {
        bucket: String,
        object: String,
        version_id: Option<String>,
    },
}

/// Heal priority
#[derive(Debug, Default, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum HealPriority {
    /// Low priority
    Low = 0,
    /// Normal priority
    #[default]
    Normal = 1,
    /// High priority
    High = 2,
    /// Urgent priority
    Urgent = 3,
}

/// Heal options
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealOptions {
    /// Scan mode
    pub scan_mode: HealScanMode,
    /// Whether to remove corrupted data
    pub remove_corrupted: bool,
    /// Whether to recreate
    pub recreate_missing: bool,
    /// Whether to update parity
    pub update_parity: bool,
    /// Whether to recursively process
    pub recursive: bool,
    /// Whether to dry run
    pub dry_run: bool,
    /// Timeout
    pub timeout: Option<Duration>,
    /// pool index
    pub pool_index: Option<usize>,
    /// set index
    pub set_index: Option<usize>,
}

impl Default for HealOptions {
    fn default() -> Self {
        Self {
            scan_mode: HealScanMode::Normal,
            remove_corrupted: false,
            recreate_missing: true,
            update_parity: true,
            recursive: false,
            dry_run: false,
            timeout: Some(Duration::from_secs(300)), // 5 minutes default timeout
            pool_index: None,
            set_index: None,
        }
    }
}

/// Heal task status
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum HealTaskStatus {
    /// Pending
    Pending,
    /// Running
    Running,
    /// Completed
    Completed,
    /// Failed
    Failed { error: String },
    /// Cancelled
    Cancelled,
    /// Timeout
    Timeout,
}

/// Heal request
#[derive(Debug, Clone)]
pub struct HealRequest {
    /// Request ID
    pub id: String,
    /// Heal type
    pub heal_type: HealType,
    /// Heal options
    pub options: HealOptions,
    /// Priority
    pub priority: HealPriority,
    /// Created time
    pub created_at: SystemTime,
}

impl HealRequest {
    pub fn new(heal_type: HealType, options: HealOptions, priority: HealPriority) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            heal_type,
            options,
            priority,
            created_at: SystemTime::now(),
        }
    }

    pub fn object(bucket: String, object: String, version_id: Option<String>) -> Self {
        Self::new(
            HealType::Object {
                bucket,
                object,
                version_id,
            },
            HealOptions::default(),
            HealPriority::Normal,
        )
    }

    pub fn bucket(bucket: String) -> Self {
        Self::new(HealType::Bucket { bucket }, HealOptions::default(), HealPriority::Normal)
    }

    pub fn metadata(bucket: String, object: String) -> Self {
        Self::new(HealType::Metadata { bucket, object }, HealOptions::default(), HealPriority::High)
    }

    pub fn ec_decode(bucket: String, object: String, version_id: Option<String>) -> Self {
        Self::new(
            HealType::ECDecode {
                bucket,
                object,
                version_id,
            },
            HealOptions::default(),
            HealPriority::Urgent,
        )
    }
}

/// Heal task
pub struct HealTask {
    /// Task ID
    pub id: String,
    /// Heal type
    pub heal_type: HealType,
    /// Heal options
    pub options: HealOptions,
    /// Task status
    pub status: Arc<RwLock<HealTaskStatus>>,
    /// Progress tracking
    pub progress: Arc<RwLock<HealProgress>>,
    /// Created time
    pub created_at: SystemTime,
    /// Started time
    pub started_at: Arc<RwLock<Option<SystemTime>>>,
    /// Completed time
    pub completed_at: Arc<RwLock<Option<SystemTime>>>,
    /// Task start instant for timeout calculation (monotonic)
    task_start_instant: Arc<RwLock<Option<Instant>>>,
    /// Cancel token
    pub cancel_token: tokio_util::sync::CancellationToken,
    /// Storage layer interface
    pub storage: Arc<dyn HealStorageAPI>,
}

impl HealTask {
    pub fn from_request(request: HealRequest, storage: Arc<dyn HealStorageAPI>) -> Self {
        Self {
            id: request.id,
            heal_type: request.heal_type,
            options: request.options,
            status: Arc::new(RwLock::new(HealTaskStatus::Pending)),
            progress: Arc::new(RwLock::new(HealProgress::new())),
            created_at: request.created_at,
            started_at: Arc::new(RwLock::new(None)),
            completed_at: Arc::new(RwLock::new(None)),
            task_start_instant: Arc::new(RwLock::new(None)),
            cancel_token: tokio_util::sync::CancellationToken::new(),
            storage,
        }
    }

    async fn remaining_timeout(&self) -> Result<Option<Duration>> {
        if let Some(total) = self.options.timeout {
            let start_instant = { *self.task_start_instant.read().await };
            if let Some(started_at) = start_instant {
                let elapsed = started_at.elapsed();
                if elapsed >= total {
                    return Err(Error::TaskTimeout);
                }
                return Ok(Some(total - elapsed));
            }
            Ok(Some(total))
        } else {
            Ok(None)
        }
    }

    async fn check_control_flags(&self) -> Result<()> {
        if self.cancel_token.is_cancelled() {
            return Err(Error::TaskCancelled);
        }
        // Only interested in propagating an error if the timeout has expired;
        // the actual Duration value is not needed here
        let _ = self.remaining_timeout().await?;
        Ok(())
    }

    async fn await_with_control<F, T>(&self, fut: F) -> Result<T>
    where
        F: Future<Output = Result<T>> + Send,
        T: Send,
    {
        let cancel_token = self.cancel_token.clone();
        if let Some(remaining) = self.remaining_timeout().await? {
            if remaining.is_zero() {
                return Err(Error::TaskTimeout);
            }
            let mut fut = Box::pin(fut);
            tokio::select! {
                _ = cancel_token.cancelled() => Err(Error::TaskCancelled),
                _ = tokio::time::sleep(remaining) => Err(Error::TaskTimeout),
                result = &mut fut => result,
            }
        } else {
            tokio::select! {
                _ = cancel_token.cancelled() => Err(Error::TaskCancelled),
                result = fut => result,
            }
        }
    }

    pub async fn execute(&self) -> Result<()> {
        // update status and timestamps atomically to avoid race conditions
        let now = SystemTime::now();
        let start_instant = Instant::now();
        {
            let mut status = self.status.write().await;
            let mut started_at = self.started_at.write().await;
            let mut task_start_instant = self.task_start_instant.write().await;
            *status = HealTaskStatus::Running;
            *started_at = Some(now);
            *task_start_instant = Some(start_instant);
        }

        info!("Starting heal task: {} with type: {:?}", self.id, self.heal_type);

        let result = match &self.heal_type {
            HealType::Object {
                bucket,
                object,
                version_id,
            } => self.heal_object(bucket, object, version_id.as_deref()).await,
            HealType::Bucket { bucket } => self.heal_bucket(bucket).await,

            HealType::Metadata { bucket, object } => self.heal_metadata(bucket, object).await,
            HealType::MRF { meta_path } => self.heal_mrf(meta_path).await,
            HealType::ECDecode {
                bucket,
                object,
                version_id,
            } => self.heal_ec_decode(bucket, object, version_id.as_deref()).await,
            HealType::ErasureSet { buckets, set_disk_id } => self.heal_erasure_set(buckets.clone(), set_disk_id.clone()).await,
        };

        // update completed time and status
        {
            let mut completed_at = self.completed_at.write().await;
            *completed_at = Some(SystemTime::now());
        }

        match &result {
            Ok(_) => {
                let mut status = self.status.write().await;
                *status = HealTaskStatus::Completed;
                info!("Heal task completed successfully: {}", self.id);
            }
            Err(Error::TaskCancelled) => {
                let mut status = self.status.write().await;
                *status = HealTaskStatus::Cancelled;
                info!("Heal task was cancelled: {}", self.id);
            }
            Err(Error::TaskTimeout) => {
                let mut status = self.status.write().await;
                *status = HealTaskStatus::Timeout;
                warn!("Heal task timed out: {}", self.id);
            }
            Err(e) => {
                let mut status = self.status.write().await;
                *status = HealTaskStatus::Failed { error: e.to_string() };
                error!("Heal task failed: {} with error: {}", self.id, e);
            }
        }

        result
    }

    pub async fn cancel(&self) -> Result<()> {
        self.cancel_token.cancel();
        let mut status = self.status.write().await;
        *status = HealTaskStatus::Cancelled;
        info!("Heal task cancelled: {}", self.id);
        Ok(())
    }

    pub async fn get_status(&self) -> HealTaskStatus {
        self.status.read().await.clone()
    }

    pub async fn get_progress(&self) -> HealProgress {
        self.progress.read().await.clone()
    }

    // specific heal implementation method
    async fn heal_object(&self, bucket: &str, object: &str, version_id: Option<&str>) -> Result<()> {
        info!("Healing object: {}/{}", bucket, object);

        // update progress
        {
            let mut progress = self.progress.write().await;
            progress.set_current_object(Some(format!("{bucket}/{object}")));
            progress.update_progress(0, 4, 0, 0);
        }

        // Step 1: Check if object exists and get metadata
        info!("Step 1: Checking object existence and metadata");
        self.check_control_flags().await?;
        let object_exists = self.await_with_control(self.storage.object_exists(bucket, object)).await?;
        if !object_exists {
            warn!("Object does not exist: {}/{}", bucket, object);
            if self.options.recreate_missing {
                info!("Attempting to recreate missing object: {}/{}", bucket, object);
                return self.recreate_missing_object(bucket, object, version_id).await;
            } else {
                return Err(Error::TaskExecutionFailed {
                    message: format!("Object not found: {bucket}/{object}"),
                });
            }
        }

        {
            let mut progress = self.progress.write().await;
            progress.update_progress(1, 3, 0, 0);
        }

        // Step 2: directly call ecstore to perform heal
        info!("Step 2: Performing heal using ecstore");
        let heal_opts = HealOpts {
            recursive: self.options.recursive,
            dry_run: self.options.dry_run,
            remove: self.options.remove_corrupted,
            recreate: self.options.recreate_missing,
            scan_mode: self.options.scan_mode,
            update_parity: self.options.update_parity,
            no_lock: false,
            pool: self.options.pool_index,
            set: self.options.set_index,
        };

        let heal_result = self
            .await_with_control(self.storage.heal_object(bucket, object, version_id, &heal_opts))
            .await;

        match heal_result {
            Ok((result, error)) => {
                if let Some(e) = error {
                    // Check if this is a "File not found" error during delete operations
                    let error_msg = format!("{e}");
                    if error_msg.contains("File not found") || error_msg.contains("not found") {
                        info!(
                            "Object {}/{} not found during heal - likely deleted intentionally, treating as successful",
                            bucket, object
                        );
                        {
                            let mut progress = self.progress.write().await;
                            progress.update_progress(3, 3, 0, 0);
                        }
                        return Ok(());
                    }

                    error!("Heal operation failed: {}/{} - {}", bucket, object, e);

                    // If heal failed and remove_corrupted is enabled, delete the corrupted object
                    if self.options.remove_corrupted {
                        warn!("Removing corrupted object: {}/{}", bucket, object);
                        if !self.options.dry_run {
                            self.await_with_control(self.storage.delete_object(bucket, object)).await?;
                            info!("Successfully deleted corrupted object: {}/{}", bucket, object);
                        } else {
                            info!("Dry run mode - would delete corrupted object: {}/{}", bucket, object);
                        }
                    }

                    {
                        let mut progress = self.progress.write().await;
                        progress.update_progress(3, 3, 0, 0);
                    }

                    return Err(Error::TaskExecutionFailed {
                        message: format!("Failed to heal object {bucket}/{object}: {e}"),
                    });
                }

                // Step 3: Verify heal result
                info!("Step 3: Verifying heal result");
                let object_size = result.object_size as u64;
                info!(
                    "Heal completed successfully: {}/{} ({} bytes, {} drives healed)",
                    bucket,
                    object,
                    object_size,
                    result.after.drives.len()
                );

                {
                    let mut progress = self.progress.write().await;
                    progress.update_progress(3, 3, object_size, object_size);
                }
                Ok(())
            }
            Err(Error::TaskCancelled) => Err(Error::TaskCancelled),
            Err(Error::TaskTimeout) => Err(Error::TaskTimeout),
            Err(e) => {
                // Check if this is a "File not found" error during delete operations
                let error_msg = format!("{e}");
                if error_msg.contains("File not found") || error_msg.contains("not found") {
                    info!(
                        "Object {}/{} not found during heal - likely deleted intentionally, treating as successful",
                        bucket, object
                    );
                    {
                        let mut progress = self.progress.write().await;
                        progress.update_progress(3, 3, 0, 0);
                    }
                    return Ok(());
                }

                error!("Heal operation failed: {}/{} - {}", bucket, object, e);

                // If heal failed and remove_corrupted is enabled, delete the corrupted object
                if self.options.remove_corrupted {
                    warn!("Removing corrupted object: {}/{}", bucket, object);
                    if !self.options.dry_run {
                        self.await_with_control(self.storage.delete_object(bucket, object)).await?;
                        info!("Successfully deleted corrupted object: {}/{}", bucket, object);
                    } else {
                        info!("Dry run mode - would delete corrupted object: {}/{}", bucket, object);
                    }
                }

                {
                    let mut progress = self.progress.write().await;
                    progress.update_progress(3, 3, 0, 0);
                }

                Err(Error::TaskExecutionFailed {
                    message: format!("Failed to heal object {bucket}/{object}: {e}"),
                })
            }
        }
    }

    /// Recreate missing object (for EC decode scenarios)
    async fn recreate_missing_object(&self, bucket: &str, object: &str, version_id: Option<&str>) -> Result<()> {
        info!("Attempting to recreate missing object: {}/{}", bucket, object);

        // Use ecstore's heal_object with recreate option
        let heal_opts = HealOpts {
            recursive: false,
            dry_run: self.options.dry_run,
            remove: false,
            recreate: true,
            scan_mode: HealScanMode::Deep,
            update_parity: true,
            no_lock: false,
            pool: None,
            set: None,
        };

        match self
            .await_with_control(self.storage.heal_object(bucket, object, version_id, &heal_opts))
            .await
        {
            Ok((result, error)) => {
                if let Some(e) = error {
                    error!("Failed to recreate missing object: {}/{} - {}", bucket, object, e);
                    return Err(Error::TaskExecutionFailed {
                        message: format!("Failed to recreate missing object {bucket}/{object}: {e}"),
                    });
                }

                let object_size = result.object_size as u64;
                info!("Successfully recreated missing object: {}/{} ({} bytes)", bucket, object, object_size);

                {
                    let mut progress = self.progress.write().await;
                    progress.update_progress(4, 4, object_size, object_size);
                }
                Ok(())
            }
            Err(Error::TaskCancelled) => Err(Error::TaskCancelled),
            Err(Error::TaskTimeout) => Err(Error::TaskTimeout),
            Err(e) => {
                error!("Failed to recreate missing object: {}/{} - {}", bucket, object, e);
                Err(Error::TaskExecutionFailed {
                    message: format!("Failed to recreate missing object {bucket}/{object}: {e}"),
                })
            }
        }
    }

    async fn heal_bucket(&self, bucket: &str) -> Result<()> {
        info!("Healing bucket: {}", bucket);

        // update progress
        {
            let mut progress = self.progress.write().await;
            progress.set_current_object(Some(format!("bucket: {bucket}")));
            progress.update_progress(0, 3, 0, 0);
        }

        // Step 1: Check if bucket exists
        info!("Step 1: Checking bucket existence");
        self.check_control_flags().await?;
        let bucket_exists = self.await_with_control(self.storage.get_bucket_info(bucket)).await?.is_some();
        if !bucket_exists {
            warn!("Bucket does not exist: {}", bucket);
            return Err(Error::TaskExecutionFailed {
                message: format!("Bucket not found: {bucket}"),
            });
        }

        {
            let mut progress = self.progress.write().await;
            progress.update_progress(1, 3, 0, 0);
        }

        // Step 2: Perform bucket heal using ecstore
        info!("Step 2: Performing bucket heal using ecstore");
        let heal_opts = HealOpts {
            recursive: self.options.recursive,
            dry_run: self.options.dry_run,
            remove: self.options.remove_corrupted,
            recreate: self.options.recreate_missing,
            scan_mode: self.options.scan_mode,
            update_parity: self.options.update_parity,
            no_lock: false,
            pool: self.options.pool_index,
            set: self.options.set_index,
        };

        let heal_result = self.await_with_control(self.storage.heal_bucket(bucket, &heal_opts)).await;

        match heal_result {
            Ok(result) => {
                info!("Bucket heal completed successfully: {} ({} drives)", bucket, result.after.drives.len());

                {
                    let mut progress = self.progress.write().await;
                    progress.update_progress(3, 3, 0, 0);
                }
                Ok(())
            }
            Err(Error::TaskCancelled) => Err(Error::TaskCancelled),
            Err(Error::TaskTimeout) => Err(Error::TaskTimeout),
            Err(e) => {
                error!("Bucket heal failed: {} - {}", bucket, e);
                {
                    let mut progress = self.progress.write().await;
                    progress.update_progress(3, 3, 0, 0);
                }
                Err(Error::TaskExecutionFailed {
                    message: format!("Failed to heal bucket {bucket}: {e}"),
                })
            }
        }
    }

    async fn heal_metadata(&self, bucket: &str, object: &str) -> Result<()> {
        info!("Healing metadata: {}/{}", bucket, object);

        // update progress
        {
            let mut progress = self.progress.write().await;
            progress.set_current_object(Some(format!("metadata: {bucket}/{object}")));
            progress.update_progress(0, 3, 0, 0);
        }

        // Step 1: Check if object exists
        info!("Step 1: Checking object existence");
        self.check_control_flags().await?;
        let object_exists = self.await_with_control(self.storage.object_exists(bucket, object)).await?;
        if !object_exists {
            warn!("Object does not exist: {}/{}", bucket, object);
            return Err(Error::TaskExecutionFailed {
                message: format!("Object not found: {bucket}/{object}"),
            });
        }

        {
            let mut progress = self.progress.write().await;
            progress.update_progress(1, 3, 0, 0);
        }

        // Step 2: Perform metadata heal using ecstore
        info!("Step 2: Performing metadata heal using ecstore");
        let heal_opts = HealOpts {
            recursive: false,
            dry_run: self.options.dry_run,
            remove: false,
            recreate: false,
            scan_mode: HealScanMode::Deep,
            update_parity: false,
            no_lock: false,
            pool: self.options.pool_index,
            set: self.options.set_index,
        };

        let heal_result = self
            .await_with_control(self.storage.heal_object(bucket, object, None, &heal_opts))
            .await;

        match heal_result {
            Ok((result, error)) => {
                if let Some(e) = error {
                    error!("Metadata heal failed: {}/{} - {}", bucket, object, e);
                    {
                        let mut progress = self.progress.write().await;
                        progress.update_progress(3, 3, 0, 0);
                    }
                    return Err(Error::TaskExecutionFailed {
                        message: format!("Failed to heal metadata {bucket}/{object}: {e}"),
                    });
                }

                info!(
                    "Metadata heal completed successfully: {}/{} ({} drives)",
                    bucket,
                    object,
                    result.after.drives.len()
                );

                {
                    let mut progress = self.progress.write().await;
                    progress.update_progress(3, 3, 0, 0);
                }
                Ok(())
            }
            Err(Error::TaskCancelled) => Err(Error::TaskCancelled),
            Err(Error::TaskTimeout) => Err(Error::TaskTimeout),
            Err(e) => {
                error!("Metadata heal failed: {}/{} - {}", bucket, object, e);
                {
                    let mut progress = self.progress.write().await;
                    progress.update_progress(3, 3, 0, 0);
                }
                Err(Error::TaskExecutionFailed {
                    message: format!("Failed to heal metadata {bucket}/{object}: {e}"),
                })
            }
        }
    }

    async fn heal_mrf(&self, meta_path: &str) -> Result<()> {
        info!("Healing MRF: {}", meta_path);

        // update progress
        {
            let mut progress = self.progress.write().await;
            progress.set_current_object(Some(format!("mrf: {meta_path}")));
            progress.update_progress(0, 2, 0, 0);
        }

        // Parse meta_path to extract bucket and object
        let parts: Vec<&str> = meta_path.split('/').collect();
        if parts.len() < 2 {
            return Err(Error::TaskExecutionFailed {
                message: format!("Invalid meta path format: {meta_path}"),
            });
        }

        let bucket = parts[0];
        let object = parts[1..].join("/");

        // Step 1: Perform MRF heal using ecstore
        info!("Step 1: Performing MRF heal using ecstore");
        let heal_opts = HealOpts {
            recursive: true,
            dry_run: self.options.dry_run,
            remove: self.options.remove_corrupted,
            recreate: self.options.recreate_missing,
            scan_mode: HealScanMode::Deep,
            update_parity: true,
            no_lock: false,
            pool: None,
            set: None,
        };

        let heal_result = self
            .await_with_control(self.storage.heal_object(bucket, &object, None, &heal_opts))
            .await;

        match heal_result {
            Ok((result, error)) => {
                if let Some(e) = error {
                    error!("MRF heal failed: {} - {}", meta_path, e);
                    {
                        let mut progress = self.progress.write().await;
                        progress.update_progress(2, 2, 0, 0);
                    }
                    return Err(Error::TaskExecutionFailed {
                        message: format!("Failed to heal MRF {meta_path}: {e}"),
                    });
                }

                info!("MRF heal completed successfully: {} ({} drives)", meta_path, result.after.drives.len());

                {
                    let mut progress = self.progress.write().await;
                    progress.update_progress(2, 2, 0, 0);
                }
                Ok(())
            }
            Err(Error::TaskCancelled) => Err(Error::TaskCancelled),
            Err(Error::TaskTimeout) => Err(Error::TaskTimeout),
            Err(e) => {
                error!("MRF heal failed: {} - {}", meta_path, e);
                {
                    let mut progress = self.progress.write().await;
                    progress.update_progress(2, 2, 0, 0);
                }
                Err(Error::TaskExecutionFailed {
                    message: format!("Failed to heal MRF {meta_path}: {e}"),
                })
            }
        }
    }

    async fn heal_ec_decode(&self, bucket: &str, object: &str, version_id: Option<&str>) -> Result<()> {
        info!("Healing EC decode: {}/{}", bucket, object);

        // update progress
        {
            let mut progress = self.progress.write().await;
            progress.set_current_object(Some(format!("ec_decode: {bucket}/{object}")));
            progress.update_progress(0, 3, 0, 0);
        }

        // Step 1: Check if object exists
        info!("Step 1: Checking object existence");
        self.check_control_flags().await?;
        let object_exists = self.await_with_control(self.storage.object_exists(bucket, object)).await?;
        if !object_exists {
            warn!("Object does not exist: {}/{}", bucket, object);
            return Err(Error::TaskExecutionFailed {
                message: format!("Object not found: {bucket}/{object}"),
            });
        }

        {
            let mut progress = self.progress.write().await;
            progress.update_progress(1, 3, 0, 0);
        }

        // Step 2: Perform EC decode heal using ecstore
        info!("Step 2: Performing EC decode heal using ecstore");
        let heal_opts = HealOpts {
            recursive: false,
            dry_run: self.options.dry_run,
            remove: false,
            recreate: true,
            scan_mode: HealScanMode::Deep,
            update_parity: true,
            no_lock: false,
            pool: None,
            set: None,
        };

        let heal_result = self
            .await_with_control(self.storage.heal_object(bucket, object, version_id, &heal_opts))
            .await;

        match heal_result {
            Ok((result, error)) => {
                if let Some(e) = error {
                    error!("EC decode heal failed: {}/{} - {}", bucket, object, e);
                    {
                        let mut progress = self.progress.write().await;
                        progress.update_progress(3, 3, 0, 0);
                    }
                    return Err(Error::TaskExecutionFailed {
                        message: format!("Failed to heal EC decode {bucket}/{object}: {e}"),
                    });
                }

                let object_size = result.object_size as u64;
                info!(
                    "EC decode heal completed successfully: {}/{} ({} bytes, {} drives)",
                    bucket,
                    object,
                    object_size,
                    result.after.drives.len()
                );

                {
                    let mut progress = self.progress.write().await;
                    progress.update_progress(3, 3, object_size, object_size);
                }
                Ok(())
            }
            Err(Error::TaskCancelled) => Err(Error::TaskCancelled),
            Err(Error::TaskTimeout) => Err(Error::TaskTimeout),
            Err(e) => {
                error!("EC decode heal failed: {}/{} - {}", bucket, object, e);
                {
                    let mut progress = self.progress.write().await;
                    progress.update_progress(3, 3, 0, 0);
                }
                Err(Error::TaskExecutionFailed {
                    message: format!("Failed to heal EC decode {bucket}/{object}: {e}"),
                })
            }
        }
    }

    async fn heal_erasure_set(&self, buckets: Vec<String>, set_disk_id: String) -> Result<()> {
        info!("Healing Erasure Set: {} ({} buckets)", set_disk_id, buckets.len());

        // update progress
        {
            let mut progress = self.progress.write().await;
            progress.set_current_object(Some(format!("erasure_set: {} ({} buckets)", set_disk_id, buckets.len())));
            progress.update_progress(0, 4, 0, 0);
        }

        let buckets = if buckets.is_empty() {
            info!("No buckets specified, listing all buckets");
            let bucket_infos = self.await_with_control(self.storage.list_buckets()).await?;
            bucket_infos.into_iter().map(|info| info.name).collect()
        } else {
            buckets
        };

        // Step 1: Perform disk format heal using ecstore
        info!("Step 1: Performing disk format heal using ecstore");
        let format_result = self.await_with_control(self.storage.heal_format(self.options.dry_run)).await;

        match format_result {
            Ok((result, error)) => {
                if let Some(e) = error {
                    error!("Disk format heal failed: {} - {}", set_disk_id, e);
                    {
                        let mut progress = self.progress.write().await;
                        progress.update_progress(4, 4, 0, 0);
                    }
                    return Err(Error::TaskExecutionFailed {
                        message: format!("Failed to heal disk format for {set_disk_id}: {e}"),
                    });
                }

                info!(
                    "Disk format heal completed successfully: {} ({} drives)",
                    set_disk_id,
                    result.after.drives.len()
                );
            }
            Err(Error::TaskCancelled) => return Err(Error::TaskCancelled),
            Err(Error::TaskTimeout) => return Err(Error::TaskTimeout),
            Err(e) => {
                error!("Disk format heal failed: {} - {}", set_disk_id, e);
                {
                    let mut progress = self.progress.write().await;
                    progress.update_progress(4, 4, 0, 0);
                }
                return Err(Error::TaskExecutionFailed {
                    message: format!("Failed to heal disk format for {set_disk_id}: {e}"),
                });
            }
        }

        {
            let mut progress = self.progress.write().await;
            progress.update_progress(1, 4, 0, 0);
        }

        // Step 2: Get disk for resume functionality
        info!("Step 2: Getting disk for resume functionality");
        let disk = self
            .await_with_control(self.storage.get_disk_for_resume(&set_disk_id))
            .await?;

        {
            let mut progress = self.progress.write().await;
            progress.update_progress(2, 4, 0, 0);
        }

        // Step 3: Heal bucket structure
        // Check control flags before each iteration to ensure timely cancellation.
        // Each heal_bucket call may handle timeout/cancellation internally, see its implementation for details.
        for bucket in buckets.iter() {
            // Check control flags before starting each bucket heal
            self.check_control_flags().await?;
            // heal_bucket internally uses await_with_control for timeout/cancellation handling
            if let Err(err) = self.heal_bucket(bucket).await {
                // Check if error is due to cancellation or timeout
                if matches!(err, Error::TaskCancelled | Error::TaskTimeout) {
                    return Err(err);
                }
                info!("Bucket heal failed: {}", err.to_string());
            }
        }

        // Step 3: Create erasure set healer with resume support
        info!("Step 3: Creating erasure set healer with resume support");
        let erasure_healer = ErasureSetHealer::new(self.storage.clone(), self.progress.clone(), self.cancel_token.clone(), disk);

        {
            let mut progress = self.progress.write().await;
            progress.update_progress(3, 4, 0, 0);
        }

        // Step 4: Execute erasure set heal with resume
        info!("Step 4: Executing erasure set heal with resume");
        let result = erasure_healer.heal_erasure_set(&buckets, &set_disk_id).await;

        {
            let mut progress = self.progress.write().await;
            progress.update_progress(4, 4, 0, 0);
        }

        match result {
            Ok(_) => {
                info!("Erasure set heal completed successfully: {} ({} buckets)", set_disk_id, buckets.len());
                Ok(())
            }
            Err(Error::TaskCancelled) => Err(Error::TaskCancelled),
            Err(Error::TaskTimeout) => Err(Error::TaskTimeout),
            Err(e) => {
                error!("Erasure set heal failed: {} - {}", set_disk_id, e);
                Err(Error::TaskExecutionFailed {
                    message: format!("Failed to heal erasure set {set_disk_id}: {e}"),
                })
            }
        }
    }
}

impl std::fmt::Debug for HealTask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HealTask")
            .field("id", &self.id)
            .field("heal_type", &self.heal_type)
            .field("options", &self.options)
            .field("created_at", &self.created_at)
            .finish()
    }
}
