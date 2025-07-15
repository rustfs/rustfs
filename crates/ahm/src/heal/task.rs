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

use crate::error::{Error, Result};
use crate::heal::{progress::HealProgress, storage::HealStorageAPI};
use rustfs_ecstore::config::RUSTFS_CONFIG_PREFIX;
use rustfs_ecstore::disk::endpoint::Endpoint;
use rustfs_ecstore::disk::error::DiskError;
use rustfs_ecstore::disk::{DiskAPI, DiskInfoOptions, BUCKET_META_PREFIX, RUSTFS_META_BUCKET};
use rustfs_ecstore::heal::heal_commands::HEAL_NORMAL_SCAN;
use rustfs_ecstore::heal::heal_commands::{init_healing_tracker, load_healing_tracker, HealScanMode};
use rustfs_ecstore::new_object_layer_fn;
use rustfs_ecstore::store::get_disk_via_endpoint;
use rustfs_ecstore::store_api::BucketInfo;
use rustfs_utils::path::path_join;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
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
    /// Disk heal
    Disk { endpoint: Endpoint },
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
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum HealPriority {
    /// Low priority
    Low = 0,
    /// Normal priority
    Normal = 1,
    /// High priority
    High = 2,
    /// Urgent priority
    Urgent = 3,
}

impl Default for HealPriority {
    fn default() -> Self {
        Self::Normal
    }
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
            scan_mode: HEAL_NORMAL_SCAN,
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

    pub fn disk(endpoint: Endpoint) -> Self {
        Self::new(HealType::Disk { endpoint }, HealOptions::default(), HealPriority::High)
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
            cancel_token: tokio_util::sync::CancellationToken::new(),
            storage,
        }
    }

    pub async fn execute(&self) -> Result<()> {
        // update status to running
        {
            let mut status = self.status.write().await;
            *status = HealTaskStatus::Running;
        }
        {
            let mut started_at = self.started_at.write().await;
            *started_at = Some(SystemTime::now());
        }

        info!("Starting heal task: {} with type: {:?}", self.id, self.heal_type);

        let result = match &self.heal_type {
            HealType::Object {
                bucket,
                object,
                version_id,
            } => self.heal_object(bucket, object, version_id.as_deref()).await,
            HealType::Bucket { bucket } => self.heal_bucket(bucket).await,
            HealType::Disk { endpoint } => self.heal_disk(endpoint).await,
            HealType::Metadata { bucket, object } => self.heal_metadata(bucket, object).await,
            HealType::MRF { meta_path } => self.heal_mrf(meta_path).await,
            HealType::ECDecode {
                bucket,
                object,
                version_id,
            } => self.heal_ec_decode(bucket, object, version_id.as_deref()).await,
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
            progress.update_progress(0, 4, 0, 0); // 开始heal，总共4个步骤
        }

        // Step 1: Check if object exists and get metadata
        info!("Step 1: Checking object existence and metadata");
        let object_exists = self.storage.object_exists(bucket, object).await?;
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
        let heal_opts = rustfs_ecstore::heal::heal_commands::HealOpts {
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

        match self.storage.heal_object(bucket, object, version_id, &heal_opts).await {
            Ok((result, error)) => {
                if let Some(e) = error {
                    error!("Heal operation failed: {}/{} - {}", bucket, object, e);

                    // If heal failed and remove_corrupted is enabled, delete the corrupted object
                    if self.options.remove_corrupted {
                        warn!("Removing corrupted object: {}/{}", bucket, object);
                        if !self.options.dry_run {
                            self.storage.delete_object(bucket, object).await?;
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
            Err(e) => {
                error!("Heal operation failed: {}/{} - {}", bucket, object, e);

                // If heal failed and remove_corrupted is enabled, delete the corrupted object
                if self.options.remove_corrupted {
                    warn!("Removing corrupted object: {}/{}", bucket, object);
                    if !self.options.dry_run {
                        self.storage.delete_object(bucket, object).await?;
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
        let heal_opts = rustfs_ecstore::heal::heal_commands::HealOpts {
            recursive: false,
            dry_run: self.options.dry_run,
            remove: false,
            recreate: true,
            scan_mode: rustfs_ecstore::heal::heal_commands::HEAL_DEEP_SCAN,
            update_parity: true,
            no_lock: false,
            pool: None,
            set: None,
        };

        match self.storage.heal_object(bucket, object, version_id, &heal_opts).await {
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
        let bucket_exists = self.storage.get_bucket_info(bucket).await?.is_some();
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
        let heal_opts = rustfs_ecstore::heal::heal_commands::HealOpts {
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

        match self.storage.heal_bucket(bucket, &heal_opts).await {
            Ok(result) => {
                info!("Bucket heal completed successfully: {} ({} drives)", bucket, result.after.drives.len());

                {
                    let mut progress = self.progress.write().await;
                    progress.update_progress(3, 3, 0, 0);
                }
                Ok(())
            }
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

    async fn heal_disk(&self, endpoint: &Endpoint) -> Result<()> {
        info!("Healing disk: {:?}", endpoint);

        // update progress
        {
            let mut progress = self.progress.write().await;
            progress.set_current_object(Some(format!("disk: {endpoint:?}")));
            progress.update_progress(0, 3, 0, 0);
        }

        // Step 1: Perform disk format heal using ecstore
        info!("Step 1: Performing disk format heal using ecstore");

        match self.storage.heal_format(self.options.dry_run).await {
            Ok((result, error)) => {
                if let Some(e) = error {
                    error!("Disk heal failed: {:?} - {}", endpoint, e);
                    {
                        let mut progress = self.progress.write().await;
                        progress.update_progress(3, 3, 0, 0);
                    }
                    return Err(Error::TaskExecutionFailed {
                        message: format!("Failed to heal disk {endpoint:?}: {e}"),
                    });
                }

                info!("Disk heal completed successfully: {:?} ({} drives)", endpoint, result.after.drives.len());

                {
                    let mut progress = self.progress.write().await;
                    progress.update_progress(2, 3, 0, 0);
                }

                // Step 2: Synchronize data/buckets on the fresh disk
                info!("Step 2: Healing buckets on fresh disk");
                self.heal_fresh_disk(endpoint).await?;

                {
                    let mut progress = self.progress.write().await;
                    progress.update_progress(3, 3, 0, 0);
                }
                Ok(())
            }
            Err(e) => {
                error!("Disk heal failed: {:?} - {}", endpoint, e);
                {
                    let mut progress = self.progress.write().await;
                    progress.update_progress(3, 3, 0, 0);
                }
                Err(Error::TaskExecutionFailed {
                    message: format!("Failed to heal disk {endpoint:?}: {e}"),
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
        let object_exists = self.storage.object_exists(bucket, object).await?;
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
        let heal_opts = rustfs_ecstore::heal::heal_commands::HealOpts {
            recursive: false,
            dry_run: self.options.dry_run,
            remove: false,
            recreate: false,
            scan_mode: rustfs_ecstore::heal::heal_commands::HEAL_DEEP_SCAN,
            update_parity: false,
            no_lock: false,
            pool: self.options.pool_index,
            set: self.options.set_index,
        };

        match self.storage.heal_object(bucket, object, None, &heal_opts).await {
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
        let heal_opts = rustfs_ecstore::heal::heal_commands::HealOpts {
            recursive: true,
            dry_run: self.options.dry_run,
            remove: self.options.remove_corrupted,
            recreate: self.options.recreate_missing,
            scan_mode: rustfs_ecstore::heal::heal_commands::HEAL_DEEP_SCAN,
            update_parity: true,
            no_lock: false,
            pool: None,
            set: None,
        };

        match self.storage.heal_object(bucket, &object, None, &heal_opts).await {
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
        let object_exists = self.storage.object_exists(bucket, object).await?;
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
        let heal_opts = rustfs_ecstore::heal::heal_commands::HealOpts {
            recursive: false,
            dry_run: self.options.dry_run,
            remove: false,
            recreate: true,
            scan_mode: rustfs_ecstore::heal::heal_commands::HEAL_DEEP_SCAN,
            update_parity: true,
            no_lock: false,
            pool: None,
            set: None,
        };

        match self.storage.heal_object(bucket, object, version_id, &heal_opts).await {
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

    async fn heal_fresh_disk(&self, endpoint: &Endpoint) -> Result<()> {
        // Locate disk via endpoint
        let disk = get_disk_via_endpoint(endpoint)
            .await
            .ok_or_else(|| Error::other(format!("Disk not found for endpoint: {endpoint}")))?;

        // Skip if drive is root or other fatal errors
        if let Err(e) = disk.disk_info(&DiskInfoOptions::default()).await {
            match e {
                DiskError::DriveIsRoot => return Ok(()),
                DiskError::UnformattedDisk => { /* continue healing */ }
                _ => return Err(Error::other(e)),
            }
        }

        // Load or init HealingTracker
        let mut tracker = match load_healing_tracker(&Some(disk.clone())).await {
            Ok(t) => t,
            Err(err) => match err {
                DiskError::FileNotFound => init_healing_tracker(disk.clone(), &Uuid::new_v4().to_string())
                    .await
                    .map_err(Error::other)?,
                _ => return Err(Error::other(err)),
            },
        };

        // Build bucket list
        let mut buckets = self.storage.list_buckets().await.map_err(Error::other)?;
        buckets.push(BucketInfo {
            name: path_join(&[PathBuf::from(RUSTFS_META_BUCKET), PathBuf::from(RUSTFS_CONFIG_PREFIX)])
                .to_string_lossy()
                .to_string(),
            ..Default::default()
        });
        buckets.push(BucketInfo {
            name: path_join(&[PathBuf::from(RUSTFS_META_BUCKET), PathBuf::from(BUCKET_META_PREFIX)])
                .to_string_lossy()
                .to_string(),
            ..Default::default()
        });

        // Sort: system buckets first, others by creation time desc
        buckets.sort_by(|a, b| {
            let a_sys = a.name.starts_with(RUSTFS_META_BUCKET);
            let b_sys = b.name.starts_with(RUSTFS_META_BUCKET);
            match (a_sys, b_sys) {
                (true, false) => Ordering::Less,
                (false, true) => Ordering::Greater,
                _ => b.created.cmp(&a.created),
            }
        });

        // Update tracker queue and persist
        tracker.set_queue_buckets(&buckets).await;
        tracker.save().await.map_err(Error::other)?;

        // Prepare bucket names list
        let bucket_names: Vec<String> = buckets.iter().map(|b| b.name.clone()).collect();

        // Run heal_erasure_set using underlying SetDisk
        let (pool_idx, set_idx) = (endpoint.pool_idx as usize, endpoint.set_idx as usize);
        let Some(store) = new_object_layer_fn() else {
            return Err(Error::other("errServerNotInitialized"));
        };
        let set_disk = store.pools[pool_idx].disk_set[set_idx].clone();

        let tracker_arc = Arc::new(RwLock::new(tracker));
        set_disk
            .heal_erasure_set(&bucket_names, tracker_arc)
            .await
            .map_err(Error::other)?;

        Ok(())
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
