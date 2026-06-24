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
use metrics::{counter, histogram};
use rustfs_common::heal_channel::{HealOpts, HealRequestSource, HealScanMode};
use rustfs_madmin::heal_commands::HealResultItem;
use serde::{Deserialize, Serialize};
use std::{
    future::Future,
    sync::Arc,
    time::{Duration, Instant, SystemTime},
};
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use super::{BUCKET_META_PREFIX, DATA_USAGE_CACHE_NAME, RUSTFS_META_BUCKET};

const LOG_COMPONENT_HEAL: &str = "heal";
const LOG_SUBSYSTEM_TASK: &str = "task";
const LOG_SUBSYSTEM_OBJECT: &str = "object";
const EVENT_HEAL_TASK_STATE: &str = "heal_task_state";
const EVENT_HEAL_OBJECT_STAGE: &str = "heal_object_stage";
const EVENT_HEAL_OBJECT_MISSING: &str = "heal_object_missing";
const EVENT_HEAL_OBJECT_RESULT: &str = "heal_object_result";
const EVENT_HEAL_OBJECT_CLEANUP: &str = "heal_object_cleanup";
const EVENT_HEAL_BUCKET_STAGE: &str = "heal_bucket_stage";
const EVENT_HEAL_BUCKET_RESULT: &str = "heal_bucket_result";
const EVENT_HEAL_METADATA_STAGE: &str = "heal_metadata_stage";
const EVENT_HEAL_METADATA_RESULT: &str = "heal_metadata_result";
const EVENT_HEAL_MRF_STAGE: &str = "heal_mrf_stage";
const EVENT_HEAL_MRF_RESULT: &str = "heal_mrf_result";
const EVENT_HEAL_EC_DECODE_STAGE: &str = "heal_ec_decode_stage";
const EVENT_HEAL_EC_DECODE_RESULT: &str = "heal_ec_decode_result";
const EVENT_HEAL_ERASURE_SET_STAGE: &str = "heal_erasure_set_stage";
const EVENT_HEAL_ERASURE_SET_RESULT: &str = "heal_erasure_set_result";

/// Heal type
#[derive(Debug, Clone)]
pub enum HealType {
    /// Cluster heal
    Cluster,
    /// Object heal
    Object {
        bucket: String,
        object: String,
        version_id: Option<String>,
    },
    /// Bucket heal
    Bucket { bucket: String },
    /// Prefix heal
    Prefix { bucket: String, prefix: String },
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

impl HealType {
    fn log_kind(&self) -> &'static str {
        match self {
            Self::Cluster => "cluster",
            Self::Object { .. } => "object",
            Self::Bucket { .. } => "bucket",
            Self::Prefix { .. } => "prefix",
            Self::ErasureSet { .. } => "erasure_set",
            Self::Metadata { .. } => "metadata",
            Self::MRF { .. } => "mrf",
            Self::ECDecode { .. } => "ec_decode",
        }
    }
}

/// Heal priority
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
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
    /// Retrying after a recoverable failure
    Retrying { error: String, retry_attempt: u32 },
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
    /// Origin of the request for operational status.
    pub source: HealRequestSource,
    /// Whether this request should bypass queue admission dedup/full policies.
    pub force_start: bool,
    /// Number of recoverable retry attempts already scheduled for this request.
    pub retry_attempts: u32,
    /// Created time
    pub created_at: SystemTime,
    /// Queue admission time used for scheduler delay metrics
    pub enqueued_at: SystemTime,
}

impl HealRequest {
    pub fn new(heal_type: HealType, options: HealOptions, priority: HealPriority) -> Self {
        let now = SystemTime::now();
        Self {
            id: Uuid::new_v4().to_string(),
            heal_type,
            options,
            priority,
            source: HealRequestSource::Internal,
            force_start: false,
            retry_attempts: 0,
            created_at: now,
            enqueued_at: now,
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
    /// Priority inherited from the request
    pub priority: HealPriority,
    /// Origin inherited from the request
    pub source: HealRequestSource,
    /// Number of recoverable retry attempts already scheduled for this task.
    pub retry_attempts: u32,
    /// Task status
    pub status: Arc<RwLock<HealTaskStatus>>,
    /// Progress tracking
    pub progress: Arc<RwLock<HealProgress>>,
    /// Result items collected from storage heal calls.
    pub result_items: Arc<RwLock<Vec<HealResultItem>>>,
    /// Created time
    pub created_at: SystemTime,
    /// Queue admission time
    pub enqueued_at: SystemTime,
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
            priority: request.priority,
            source: request.source,
            retry_attempts: request.retry_attempts,
            status: Arc::new(RwLock::new(HealTaskStatus::Pending)),
            progress: Arc::new(RwLock::new(HealProgress::new())),
            result_items: Arc::new(RwLock::new(Vec::new())),
            created_at: request.created_at,
            enqueued_at: request.enqueued_at,
            started_at: Arc::new(RwLock::new(None)),
            completed_at: Arc::new(RwLock::new(None)),
            task_start_instant: Arc::new(RwLock::new(None)),
            cancel_token: tokio_util::sync::CancellationToken::new(),
            storage,
        }
    }

    pub fn retry_request(&self) -> HealRequest {
        HealRequest {
            id: self.id.clone(),
            heal_type: self.heal_type.clone(),
            options: self.options.clone(),
            priority: self.priority,
            source: self.source,
            force_start: false,
            retry_attempts: self.retry_attempts.saturating_add(1),
            created_at: self.created_at,
            enqueued_at: SystemTime::now(),
        }
    }

    pub fn metric_type_label(&self) -> &'static str {
        match &self.heal_type {
            HealType::Cluster => "cluster",
            HealType::Object { .. } => "object",
            HealType::Bucket { .. } => "bucket",
            HealType::Prefix { .. } => "prefix",
            HealType::ErasureSet { .. } => "erasure_set",
            HealType::Metadata { .. } => "metadata",
            HealType::MRF { .. } => "mrf",
            HealType::ECDecode { .. } => "ec_decode",
        }
    }

    pub fn metric_set_label(&self) -> String {
        match &self.heal_type {
            HealType::ErasureSet { set_disk_id, .. } => set_disk_id.clone(),
            _ => match (self.options.pool_index, self.options.set_index) {
                (Some(pool), Some(set)) => format!("pool_{pool}_set_{set}"),
                _ => "global".to_string(),
            },
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

    async fn skip_due_to_transient_object_exists(&self, bucket: &str, object: &str, err: &Error) -> Result<()> {
        warn!(
            target: "rustfs::heal::task",
            event = EVENT_HEAL_OBJECT_RESULT,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_OBJECT,
            task_id = %self.id,
            bucket,
            object,
            result = "transient_skip",
            error = %err,
            "Heal object skipped due to transient existence check error"
        );

        let mut progress = self.progress.write().await;
        progress.set_current_object(Some(format!("skipped: {bucket}/{object}")));
        progress.update_progress(0, 1, 0, 0);
        Ok(())
    }

    fn is_data_usage_cache_object(bucket: &str, object: &str) -> bool {
        bucket == RUSTFS_META_BUCKET
            && object
                .strip_prefix(BUCKET_META_PREFIX)
                .and_then(|suffix| suffix.strip_prefix('/'))
                .is_some_and(|name| name.contains(DATA_USAGE_CACHE_NAME))
    }

    fn is_transient_lock_or_timeout_error(err: &Error) -> bool {
        let message = err.to_string().to_ascii_lowercase();
        message.contains("lock acquisition timeout")
            || message.contains("lock acquisition failed")
            || message.contains("timed out")
            || message.contains("deadline has elapsed")
    }

    fn should_skip_data_usage_cache_heal_error(bucket: &str, object: &str, err: &Error) -> bool {
        Self::is_data_usage_cache_object(bucket, object) && Self::is_transient_lock_or_timeout_error(err)
    }

    fn is_no_heal_required_error(err: &Error) -> bool {
        match err {
            Error::Other(message) => matches!(message.as_str(), "No heal required" | "No healing is required"),
            _ => matches!(err.to_string().as_str(), "No heal required" | "No healing is required"),
        }
    }

    async fn skip_data_usage_cache_heal_error(&self, bucket: &str, object: &str, err: &Error) -> bool {
        if !Self::should_skip_data_usage_cache_heal_error(bucket, object, err) {
            return false;
        }

        warn!(
            target: "rustfs::heal::task",
            event = EVENT_HEAL_OBJECT_RESULT,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_OBJECT,
            task_id = %self.id,
            bucket,
            object,
            result = "data_usage_cache_transient_skip",
            error = %err,
            "Heal object skipped for data usage cache after transient error"
        );
        let mut progress = self.progress.write().await;
        progress.update_progress(3, 3, 0, 0);
        true
    }

    #[tracing::instrument(skip(self), fields(task_id = %self.id, heal_type = ?self.heal_type))]
    pub async fn execute(&self) -> Result<()> {
        // update status and timestamps atomically to avoid race conditions
        let now = SystemTime::now();
        let start_instant = Instant::now();
        let queue_delay = now.duration_since(self.enqueued_at).unwrap_or_default();
        let type_label = self.metric_type_label().to_string();
        let set_label = self.metric_set_label();
        {
            let mut status = self.status.write().await;
            let mut started_at = self.started_at.write().await;
            let mut task_start_instant = self.task_start_instant.write().await;
            *status = HealTaskStatus::Running;
            *started_at = Some(now);
            *task_start_instant = Some(start_instant);
        }

        histogram!(
            "rustfs_heal_queue_delay_seconds",
            "type" => type_label.clone(),
            "set" => set_label.clone()
        )
        .record(queue_delay.as_secs_f64());
        counter!(
            "rustfs_heal_task_start_total",
            "type" => type_label,
            "set" => set_label
        )
        .increment(1);

        info!(
            target: "rustfs::heal::task",
            event = EVENT_HEAL_TASK_STATE,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_TASK,
            task_id = %self.id,
            heal_type = self.heal_type.log_kind(),
            state = "started",
            queue_delay = ?queue_delay,
            "Heal task started"
        );

        let result = match &self.heal_type {
            HealType::Cluster => self.heal_cluster().await,
            HealType::Object {
                bucket,
                object,
                version_id,
            } => self.heal_object(bucket, object, version_id.as_deref()).await,
            HealType::Bucket { bucket } => self.heal_bucket(bucket).await,
            HealType::Prefix { bucket, prefix } => self.heal_prefix(bucket, prefix).await,

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
                info!(
                    target: "rustfs::heal::task",
                    event = EVENT_HEAL_TASK_STATE,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_TASK,
                    task_id = %self.id,
                    heal_type = self.heal_type.log_kind(),
                    state = "completed",
                    "Heal task completed"
                );
            }
            Err(Error::TaskCancelled) => {
                let mut status = self.status.write().await;
                *status = HealTaskStatus::Cancelled;
                info!(
                    target: "rustfs::heal::task",
                    event = EVENT_HEAL_TASK_STATE,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_TASK,
                    task_id = %self.id,
                    heal_type = self.heal_type.log_kind(),
                    state = "cancelled",
                    "Heal task cancelled"
                );
            }
            Err(Error::TaskTimeout) => {
                let mut status = self.status.write().await;
                *status = HealTaskStatus::Timeout;
                warn!(
                    target: "rustfs::heal::task",
                    event = EVENT_HEAL_TASK_STATE,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_TASK,
                    task_id = %self.id,
                    heal_type = self.heal_type.log_kind(),
                    state = "timed_out",
                    "Heal task timed out"
                );
            }
            Err(e) => {
                let mut status = self.status.write().await;
                *status = HealTaskStatus::Failed { error: e.to_string() };
                error!(
                    target: "rustfs::heal::task",
                    event = EVENT_HEAL_TASK_STATE,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_TASK,
                    task_id = %self.id,
                    heal_type = self.heal_type.log_kind(),
                    state = "failed",
                    error = %e,
                    "Heal task failed"
                );
            }
        }

        result
    }

    pub async fn cancel(&self) -> Result<()> {
        self.cancel_token.cancel();
        let mut status = self.status.write().await;
        *status = HealTaskStatus::Cancelled;
        debug!(
            target: "rustfs::heal::task",
            event = EVENT_HEAL_TASK_STATE,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_TASK,
            task_id = %self.id,
            heal_type = self.heal_type.log_kind(),
            state = "cancelled",
            source = "manual",
            "Heal task cancellation requested"
        );
        Ok(())
    }

    pub async fn get_status(&self) -> HealTaskStatus {
        self.status.read().await.clone()
    }

    pub async fn get_progress(&self) -> HealProgress {
        self.progress.read().await.clone()
    }

    pub async fn get_result_items(&self) -> Vec<HealResultItem> {
        self.result_items.read().await.clone()
    }

    async fn record_result_item(&self, result: HealResultItem) {
        self.result_items.write().await.push(result);
    }

    // specific heal implementation method
    #[tracing::instrument(skip(self), fields(bucket = %bucket, object = %object, version_id = ?version_id))]
    async fn heal_object(&self, bucket: &str, object: &str, version_id: Option<&str>) -> Result<()> {
        debug!(
            target: "rustfs::heal::task",
            event = EVENT_HEAL_OBJECT_STAGE,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_OBJECT,
            task_id = %self.id,
            bucket,
            object,
            version_id = ?version_id,
            stage = "start",
            "Heal object started"
        );

        // update progress
        {
            let mut progress = self.progress.write().await;
            progress.set_current_object(Some(format!("{bucket}/{object}")));
            progress.update_progress(0, 4, 0, 0);
        }

        // Step 1: Check if object exists and get metadata
        debug!(
            target: "rustfs::heal::task",
            event = EVENT_HEAL_OBJECT_STAGE,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_OBJECT,
            task_id = %self.id,
            bucket,
            object,
            stage = "check_existence",
            "Heal object stage entered"
        );
        self.check_control_flags().await?;
        let object_exists = match self.await_with_control(self.storage.object_exists(bucket, object)).await {
            Ok(exists) => exists,
            Err(err @ Error::TransientSkip { .. }) => {
                return self.skip_due_to_transient_object_exists(bucket, object, &err).await;
            }
            Err(err) => return Err(err),
        };
        if !object_exists {
            warn!(
                target: "rustfs::heal::task",
                event = EVENT_HEAL_OBJECT_MISSING,
                component = LOG_COMPONENT_HEAL,
                subsystem = LOG_SUBSYSTEM_OBJECT,
                task_id = %self.id,
                bucket,
                object,
                recreate_missing = self.options.recreate_missing,
                "Heal target object is missing"
            );
            if self.options.recreate_missing {
                debug!(
                    target: "rustfs::heal::task",
                    event = EVENT_HEAL_OBJECT_STAGE,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_OBJECT,
                    task_id = %self.id,
                    bucket,
                    object,
                    stage = "recreate_missing",
                    "Heal object recreate requested"
                );
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
        debug!(
            target: "rustfs::heal::task",
            event = EVENT_HEAL_OBJECT_STAGE,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_OBJECT,
            task_id = %self.id,
            bucket,
            object,
            stage = "heal_with_ecstore",
            dry_run = self.options.dry_run,
            remove_corrupted = self.options.remove_corrupted,
            update_parity = self.options.update_parity,
            "Heal object stage entered"
        );
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
                    if self.skip_data_usage_cache_heal_error(bucket, object, &e).await {
                        return Ok(());
                    }

                    // Check if this is a "File not found" error during delete operations
                    let error_msg = format!("{e}");
                    if error_msg.contains("File not found") || error_msg.contains("not found") {
                        debug!(
                            target: "rustfs::heal::task",
                            event = EVENT_HEAL_OBJECT_RESULT,
                            component = LOG_COMPONENT_HEAL,
                            subsystem = LOG_SUBSYSTEM_OBJECT,
                            task_id = %self.id,
                            bucket,
                            object,
                            result = "treated_as_deleted",
                            "Heal missing object treated as deleted"
                        );
                        {
                            let mut progress = self.progress.write().await;
                            progress.update_progress(3, 3, 0, 0);
                        }
                        return Ok(());
                    }

                    error!(
                        target: "rustfs::heal::task",
                        event = EVENT_HEAL_OBJECT_RESULT,
                        component = LOG_COMPONENT_HEAL,
                        subsystem = LOG_SUBSYSTEM_OBJECT,
                        task_id = %self.id,
                        bucket,
                        object,
                        result = "failed",
                        error = %e,
                        "Heal object operation failed"
                    );

                    // If heal failed and remove_corrupted is enabled, delete the corrupted object
                    if self.options.remove_corrupted {
                        debug!(
                            target: "rustfs::heal::task",
                            event = EVENT_HEAL_OBJECT_CLEANUP,
                            component = LOG_COMPONENT_HEAL,
                            subsystem = LOG_SUBSYSTEM_OBJECT,
                            task_id = %self.id,
                            bucket,
                            object,
                            action = "delete_corrupted_object",
                            dry_run = self.options.dry_run,
                            "Heal object cleanup requested"
                        );
                        if !self.options.dry_run {
                            self.await_with_control(self.storage.delete_object(bucket, object)).await?;
                            debug!(
                                target: "rustfs::heal::task",
                                event = EVENT_HEAL_OBJECT_CLEANUP,
                                component = LOG_COMPONENT_HEAL,
                                subsystem = LOG_SUBSYSTEM_OBJECT,
                                task_id = %self.id,
                                bucket,
                                object,
                                action = "delete_corrupted_object",
                                result = "deleted",
                                "Heal corrupted object deleted"
                            );
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
                debug!(
                    target: "rustfs::heal::task",
                    event = EVENT_HEAL_OBJECT_STAGE,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_OBJECT,
                    task_id = %self.id,
                    bucket,
                    object,
                    stage = "verify_result",
                    "Heal object stage entered"
                );
                let object_size = result.object_size as u64;
                debug!(
                    target: "rustfs::heal::task",
                    event = EVENT_HEAL_OBJECT_RESULT,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_OBJECT,
                    task_id = %self.id,
                    bucket,
                    object,
                    object_size = object_size,
                    drives_healed = result.after.drives.len(),
                    result = "ok",
                    "Heal object repaired"
                );

                {
                    let mut progress = self.progress.write().await;
                    progress.update_progress(3, 3, object_size, object_size);
                }
                self.record_result_item(result).await;
                Ok(())
            }
            Err(Error::TaskCancelled) => Err(Error::TaskCancelled),
            Err(Error::TaskTimeout) => Err(Error::TaskTimeout),
            Err(e) => {
                if self.skip_data_usage_cache_heal_error(bucket, object, &e).await {
                    return Ok(());
                }

                // Check if this is a "File not found" error during delete operations
                let error_msg = format!("{e}");
                if error_msg.contains("File not found") || error_msg.contains("not found") {
                    debug!(
                        target: "rustfs::heal::task",
                        event = EVENT_HEAL_OBJECT_RESULT,
                        component = LOG_COMPONENT_HEAL,
                        subsystem = LOG_SUBSYSTEM_OBJECT,
                        task_id = %self.id,
                        bucket,
                        object,
                        result = "treated_as_deleted",
                        "Heal missing object treated as deleted"
                    );
                    {
                        let mut progress = self.progress.write().await;
                        progress.update_progress(3, 3, 0, 0);
                    }
                    return Ok(());
                }

                error!(
                    target: "rustfs::heal::task",
                    event = EVENT_HEAL_OBJECT_RESULT,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_OBJECT,
                    task_id = %self.id,
                    bucket,
                    object,
                    result = "failed",
                    error = %e,
                    "Heal object operation failed"
                );

                // If heal failed and remove_corrupted is enabled, delete the corrupted object
                if self.options.remove_corrupted {
                    debug!(
                        target: "rustfs::heal::task",
                        event = EVENT_HEAL_OBJECT_CLEANUP,
                        component = LOG_COMPONENT_HEAL,
                        subsystem = LOG_SUBSYSTEM_OBJECT,
                        task_id = %self.id,
                        bucket,
                        object,
                        action = "delete_corrupted_object",
                        dry_run = self.options.dry_run,
                        "Heal object cleanup requested"
                    );
                    if !self.options.dry_run {
                        self.await_with_control(self.storage.delete_object(bucket, object)).await?;
                        debug!(
                            target: "rustfs::heal::task",
                            event = EVENT_HEAL_OBJECT_CLEANUP,
                            component = LOG_COMPONENT_HEAL,
                            subsystem = LOG_SUBSYSTEM_OBJECT,
                            task_id = %self.id,
                            bucket,
                            object,
                            action = "delete_corrupted_object",
                            result = "deleted",
                            "Heal corrupted object deleted"
                        );
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
        debug!(
            target: "rustfs::heal::task",
            event = EVENT_HEAL_OBJECT_STAGE,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_OBJECT,
            task_id = %self.id,
            bucket,
            object,
            version_id = ?version_id,
            stage = "recreate_missing",
            "Heal object recreate started"
        );

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
                    error!(
                        target: "rustfs::heal::task",
                        event = EVENT_HEAL_OBJECT_RESULT,
                        component = LOG_COMPONENT_HEAL,
                        subsystem = LOG_SUBSYSTEM_OBJECT,
                        task_id = %self.id,
                        bucket,
                        object,
                        result = "recreate_failed",
                        error = %e,
                        "Heal object recovery failed"
                    );
                    return Err(Error::TaskExecutionFailed {
                        message: format!("Failed to recreate missing object {bucket}/{object}: {e}"),
                    });
                }

                let object_size = result.object_size as u64;
                debug!(
                    target: "rustfs::heal::task",
                    event = EVENT_HEAL_OBJECT_RESULT,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_OBJECT,
                    task_id = %self.id,
                    bucket,
                    object,
                    object_size,
                    result = "recreated",
                    "Heal object recreated"
                );

                {
                    let mut progress = self.progress.write().await;
                    progress.update_progress(4, 4, object_size, object_size);
                }
                self.record_result_item(result).await;
                Ok(())
            }
            Err(Error::TaskCancelled) => Err(Error::TaskCancelled),
            Err(Error::TaskTimeout) => Err(Error::TaskTimeout),
            Err(e) => {
                error!(
                    target: "rustfs::heal::task",
                    event = EVENT_HEAL_OBJECT_RESULT,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_OBJECT,
                    task_id = %self.id,
                    bucket,
                    object,
                    result = "recreate_failed",
                    error = %e,
                    "Heal object recovery failed"
                );
                Err(Error::TaskExecutionFailed {
                    message: format!("Failed to recreate missing object {bucket}/{object}: {e}"),
                })
            }
        }
    }

    async fn heal_bucket(&self, bucket: &str) -> Result<()> {
        debug!(
            target: "rustfs::heal::task",
            event = EVENT_HEAL_BUCKET_STAGE,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_TASK,
            task_id = %self.id,
            bucket,
            stage = "start",
            recursive = self.options.recursive,
            "Heal bucket started"
        );

        // update progress
        {
            let mut progress = self.progress.write().await;
            progress.set_current_object(Some(format!("bucket: {bucket}")));
            progress.update_progress(0, 3, 0, 0);
        }

        // Step 1: Check if bucket exists
        debug!(
            target: "rustfs::heal::task",
            event = EVENT_HEAL_BUCKET_STAGE,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_TASK,
            task_id = %self.id,
            bucket,
            stage = "check_existence",
            "Heal bucket stage entered"
        );
        self.check_control_flags().await?;
        let bucket_exists = self.await_with_control(self.storage.get_bucket_info(bucket)).await?.is_some();
        if !bucket_exists {
            warn!(
                target: "rustfs::heal::task",
                event = EVENT_HEAL_BUCKET_RESULT,
                component = LOG_COMPONENT_HEAL,
                subsystem = LOG_SUBSYSTEM_TASK,
                task_id = %self.id,
                bucket,
                result = "missing",
                "Heal bucket failed because the bucket does not exist"
            );
            return Err(Error::TaskExecutionFailed {
                message: format!("Bucket not found: {bucket}"),
            });
        }

        {
            let mut progress = self.progress.write().await;
            progress.update_progress(1, 3, 0, 0);
        }

        // Step 2: Perform bucket heal using ecstore
        debug!(
            target: "rustfs::heal::task",
            event = EVENT_HEAL_BUCKET_STAGE,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_TASK,
            task_id = %self.id,
            bucket,
            stage = "heal_with_ecstore",
            dry_run = self.options.dry_run,
            "Heal bucket stage entered"
        );
        let heal_opts = HealOpts {
            recursive: self.options.recursive,
            dry_run: self.options.dry_run,
            remove: if self.options.recursive {
                false
            } else {
                self.options.remove_corrupted
            },
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
                debug!(
                    target: "rustfs::heal::task",
                    event = EVENT_HEAL_BUCKET_RESULT,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_TASK,
                    task_id = %self.id,
                    bucket,
                    drives_healed = result.after.drives.len(),
                    recursive = self.options.recursive,
                    result = "ok",
                    "Heal bucket completed"
                );
                self.record_result_item(result).await;

                if self.options.recursive {
                    self.heal_bucket_objects(bucket, "").await?;
                }

                if !self.options.recursive {
                    let mut progress = self.progress.write().await;
                    progress.update_progress(3, 3, 0, 0);
                }
                Ok(())
            }
            Err(Error::TaskCancelled) => Err(Error::TaskCancelled),
            Err(Error::TaskTimeout) => Err(Error::TaskTimeout),
            Err(e) => {
                error!(
                    target: "rustfs::heal::task",
                    event = EVENT_HEAL_BUCKET_RESULT,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_TASK,
                    task_id = %self.id,
                    bucket,
                    result = "failed",
                    error = %e,
                    "Heal bucket failed"
                );
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

    async fn heal_cluster(&self) -> Result<()> {
        debug!(
            target: "rustfs::heal::task",
            event = EVENT_HEAL_BUCKET_STAGE,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_TASK,
            task_id = %self.id,
            stage = "cluster_recursive",
            "Heal cluster started"
        );

        let bucket_infos = self.await_with_control(self.storage.list_buckets()).await?;
        for bucket_info in bucket_infos {
            self.check_control_flags().await?;
            self.heal_bucket(&bucket_info.name).await?;
        }

        Ok(())
    }

    async fn heal_prefix(&self, bucket: &str, prefix: &str) -> Result<()> {
        debug!(
            target: "rustfs::heal::task",
            event = EVENT_HEAL_BUCKET_STAGE,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_TASK,
            task_id = %self.id,
            bucket,
            prefix,
            stage = "prefix_recursive",
            "Heal prefix started"
        );

        self.heal_bucket_objects(bucket, prefix).await
    }

    async fn heal_bucket_objects(&self, bucket: &str, prefix: &str) -> Result<()> {
        let mut continuation_token: Option<String> = None;
        let mut scanned = 0u64;
        let mut healed = 0u64;
        let mut failed = 0u64;
        let mut bytes = 0u64;

        let heal_opts = HealOpts {
            recursive: false,
            dry_run: self.options.dry_run,
            remove: self.options.remove_corrupted,
            recreate: self.options.recreate_missing,
            scan_mode: self.options.scan_mode,
            update_parity: self.options.update_parity,
            no_lock: false,
            pool: self.options.pool_index,
            set: self.options.set_index,
        };

        loop {
            self.check_control_flags().await?;
            let (objects, next_token, is_truncated) = self
                .await_with_control(
                    self.storage
                        .list_objects_for_heal_page(bucket, prefix, continuation_token.as_deref()),
                )
                .await?;

            for object in objects {
                self.check_control_flags().await?;
                scanned += 1;
                {
                    let mut progress = self.progress.write().await;
                    progress.set_current_object(Some(format!("{bucket}/{object}")));
                    progress.update_progress(scanned, healed, failed, bytes);
                }

                match self.await_with_control(self.storage.object_exists(bucket, &object)).await {
                    Ok(false) => {
                        healed += 1;
                    }
                    Ok(true) => match self
                        .await_with_control(self.storage.heal_object(bucket, &object, None, &heal_opts))
                        .await
                    {
                        Ok((result, None)) => {
                            healed += 1;
                            bytes = bytes.saturating_add(result.object_size as u64);
                            self.record_result_item(result).await;
                        }
                        Ok((_, Some(err))) => {
                            if Self::should_skip_data_usage_cache_heal_error(bucket, &object, &err) {
                                warn!(
                                    target: "rustfs::heal::task",
                                    event = EVENT_HEAL_BUCKET_RESULT,
                                    component = LOG_COMPONENT_HEAL,
                                    subsystem = LOG_SUBSYSTEM_TASK,
                                    task_id = %self.id,
                                    bucket,
                                    object = %object,
                                    result = "transient_skip",
                                    error = %err,
                                    "Heal bucket object repair skipped due to transient error"
                                );
                            } else {
                                failed += 1;
                                warn!(
                                    target: "rustfs::heal::task",
                                    event = EVENT_HEAL_BUCKET_RESULT,
                                    component = LOG_COMPONENT_HEAL,
                                    subsystem = LOG_SUBSYSTEM_TASK,
                                    task_id = %self.id,
                                    bucket,
                                    object = %object,
                                    result = "object_failed",
                                    error = %err,
                                    "Heal bucket object repair failed"
                                );
                            }
                        }
                        Err(err) => {
                            if Self::should_skip_data_usage_cache_heal_error(bucket, &object, &err) {
                                warn!(
                                    target: "rustfs::heal::task",
                                    event = EVENT_HEAL_BUCKET_RESULT,
                                    component = LOG_COMPONENT_HEAL,
                                    subsystem = LOG_SUBSYSTEM_TASK,
                                    task_id = %self.id,
                                    bucket,
                                    object = %object,
                                    result = "transient_skip",
                                    error = %err,
                                    "Heal bucket object repair skipped due to transient error"
                                );
                            } else {
                                failed += 1;
                                warn!(
                                    target: "rustfs::heal::task",
                                    event = EVENT_HEAL_BUCKET_RESULT,
                                    component = LOG_COMPONENT_HEAL,
                                    subsystem = LOG_SUBSYSTEM_TASK,
                                    task_id = %self.id,
                                    bucket,
                                    object = %object,
                                    result = "object_failed",
                                    error = %err,
                                    "Heal bucket object repair failed"
                                );
                            }
                        }
                    },
                    Err(err) => {
                        failed += 1;
                        warn!(
                            target: "rustfs::heal::task",
                            event = EVENT_HEAL_BUCKET_RESULT,
                            component = LOG_COMPONENT_HEAL,
                            subsystem = LOG_SUBSYSTEM_TASK,
                            task_id = %self.id,
                            bucket,
                            object = %object,
                            result = "precheck_failed",
                            error = %err,
                            "Heal bucket object precheck failed"
                        );
                    }
                }

                {
                    let mut progress = self.progress.write().await;
                    progress.update_progress(scanned, healed, failed, bytes);
                }
            }

            if !is_truncated {
                break;
            }

            continuation_token = next_token;
            if continuation_token.is_none() {
                warn!(
                    target: "rustfs::heal::task",
                    event = EVENT_HEAL_BUCKET_RESULT,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_TASK,
                    task_id = %self.id,
                    bucket,
                    result = "missing_continuation_token",
                    "Heal bucket listing truncated without continuation token"
                );
                break;
            }
        }

        if failed > 0 {
            return Err(Error::TaskExecutionFailed {
                message: format!("Failed to heal {failed} object(s) in bucket {bucket}"),
            });
        }

        debug!(
            target: "rustfs::heal::task",
            event = EVENT_HEAL_BUCKET_RESULT,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_TASK,
            task_id = %self.id,
            bucket,
            prefix,
            scanned,
            healed,
            failed,
            bytes_processed = bytes,
            result = "recursive_ok",
            "Heal bucket recursive pass completed"
        );
        Ok(())
    }

    async fn heal_metadata(&self, bucket: &str, object: &str) -> Result<()> {
        debug!(
            target: "rustfs::heal::task",
            event = EVENT_HEAL_METADATA_STAGE,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_TASK,
            task_id = %self.id,
            bucket,
            object,
            stage = "start",
            "Heal metadata started"
        );

        // update progress
        {
            let mut progress = self.progress.write().await;
            progress.set_current_object(Some(format!("metadata: {bucket}/{object}")));
            progress.update_progress(0, 3, 0, 0);
        }

        // Step 1: Check if object exists
        debug!(
            target: "rustfs::heal::task",
            event = EVENT_HEAL_METADATA_STAGE,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_TASK,
            task_id = %self.id,
            bucket,
            object,
            stage = "check_existence",
            "Heal metadata stage entered"
        );
        self.check_control_flags().await?;
        let object_exists = match self.await_with_control(self.storage.object_exists(bucket, object)).await {
            Ok(exists) => exists,
            Err(err @ Error::TransientSkip { .. }) => {
                return self.skip_due_to_transient_object_exists(bucket, object, &err).await;
            }
            Err(err) => return Err(err),
        };
        if !object_exists {
            warn!(
                target: "rustfs::heal::task",
                event = EVENT_HEAL_METADATA_RESULT,
                component = LOG_COMPONENT_HEAL,
                subsystem = LOG_SUBSYSTEM_TASK,
                task_id = %self.id,
                bucket,
                object,
                result = "missing",
                "Heal metadata failed because object is missing"
            );
            return Err(Error::TaskExecutionFailed {
                message: format!("Object not found: {bucket}/{object}"),
            });
        }

        {
            let mut progress = self.progress.write().await;
            progress.update_progress(1, 3, 0, 0);
        }

        // Step 2: Perform metadata heal using ecstore
        debug!(
            target: "rustfs::heal::task",
            event = EVENT_HEAL_METADATA_STAGE,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_TASK,
            task_id = %self.id,
            bucket,
            object,
            stage = "heal_with_ecstore",
            "Heal metadata stage entered"
        );
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
                    error!(
                        target: "rustfs::heal::task",
                        event = EVENT_HEAL_METADATA_RESULT,
                        component = LOG_COMPONENT_HEAL,
                        subsystem = LOG_SUBSYSTEM_TASK,
                        task_id = %self.id,
                        bucket,
                        object,
                        result = "failed",
                        error = %e,
                        "Heal metadata failed"
                    );
                    {
                        let mut progress = self.progress.write().await;
                        progress.update_progress(3, 3, 0, 0);
                    }
                    return Err(Error::TaskExecutionFailed {
                        message: format!("Failed to heal metadata {bucket}/{object}: {e}"),
                    });
                }

                debug!(
                    target: "rustfs::heal::task",
                    event = EVENT_HEAL_METADATA_RESULT,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_TASK,
                    task_id = %self.id,
                    bucket,
                    object,
                    drives_healed = result.after.drives.len(),
                    result = "ok",
                    "Heal metadata repaired"
                );

                {
                    let mut progress = self.progress.write().await;
                    progress.update_progress(3, 3, 0, 0);
                }
                self.record_result_item(result).await;
                Ok(())
            }
            Err(Error::TaskCancelled) => Err(Error::TaskCancelled),
            Err(Error::TaskTimeout) => Err(Error::TaskTimeout),
            Err(e) => {
                error!(
                    target: "rustfs::heal::task",
                    event = EVENT_HEAL_METADATA_RESULT,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_TASK,
                    task_id = %self.id,
                    bucket,
                    object,
                    result = "failed",
                    error = %e,
                    "Heal metadata failed"
                );
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
        debug!(
            target: "rustfs::heal::task",
            event = EVENT_HEAL_MRF_STAGE,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_TASK,
            task_id = %self.id,
            meta_path,
            stage = "start",
            "Heal MRF started"
        );

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
        debug!(
            target: "rustfs::heal::task",
            event = EVENT_HEAL_MRF_STAGE,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_TASK,
            task_id = %self.id,
            meta_path,
            bucket,
            object = %object,
            stage = "heal_with_ecstore",
            "Heal MRF stage entered"
        );
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
                    error!(
                        target: "rustfs::heal::task",
                        event = EVENT_HEAL_MRF_RESULT,
                        component = LOG_COMPONENT_HEAL,
                        subsystem = LOG_SUBSYSTEM_TASK,
                        task_id = %self.id,
                        meta_path,
                        bucket,
                        object = %object,
                        result = "failed",
                        error = %e,
                        "Heal MRF failed"
                    );
                    {
                        let mut progress = self.progress.write().await;
                        progress.update_progress(2, 2, 0, 0);
                    }
                    return Err(Error::TaskExecutionFailed {
                        message: format!("Failed to heal MRF {meta_path}: {e}"),
                    });
                }

                debug!(
                    target: "rustfs::heal::task",
                    event = EVENT_HEAL_MRF_RESULT,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_TASK,
                    task_id = %self.id,
                    meta_path,
                    bucket,
                    object = %object,
                    drives_healed = result.after.drives.len(),
                    result = "ok",
                    "Heal MRF repaired"
                );

                {
                    let mut progress = self.progress.write().await;
                    progress.update_progress(2, 2, 0, 0);
                }
                self.record_result_item(result).await;
                Ok(())
            }
            Err(Error::TaskCancelled) => Err(Error::TaskCancelled),
            Err(Error::TaskTimeout) => Err(Error::TaskTimeout),
            Err(e) => {
                error!(
                    target: "rustfs::heal::task",
                    event = EVENT_HEAL_MRF_RESULT,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_TASK,
                    task_id = %self.id,
                    meta_path,
                    bucket,
                    object = %object,
                    result = "failed",
                    error = %e,
                    "Heal MRF failed"
                );
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
        debug!(
            target: "rustfs::heal::task",
            event = EVENT_HEAL_EC_DECODE_STAGE,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_TASK,
            task_id = %self.id,
            bucket,
            object,
            version_id = ?version_id,
            stage = "start",
            "Heal EC decode started"
        );

        // update progress
        {
            let mut progress = self.progress.write().await;
            progress.set_current_object(Some(format!("ec_decode: {bucket}/{object}")));
            progress.update_progress(0, 3, 0, 0);
        }

        // Step 1: Check if object exists
        debug!(
            target: "rustfs::heal::task",
            event = EVENT_HEAL_EC_DECODE_STAGE,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_TASK,
            task_id = %self.id,
            bucket,
            object,
            stage = "check_existence",
            "Heal EC decode stage entered"
        );
        self.check_control_flags().await?;
        let object_exists = match self.await_with_control(self.storage.object_exists(bucket, object)).await {
            Ok(exists) => exists,
            Err(err @ Error::TransientSkip { .. }) => {
                return self.skip_due_to_transient_object_exists(bucket, object, &err).await;
            }
            Err(err) => return Err(err),
        };
        if !object_exists {
            warn!(
                target: "rustfs::heal::task",
                event = EVENT_HEAL_EC_DECODE_RESULT,
                component = LOG_COMPONENT_HEAL,
                subsystem = LOG_SUBSYSTEM_TASK,
                task_id = %self.id,
                bucket,
                object,
                result = "missing",
                "Heal EC decode failed because object is missing"
            );
            return Err(Error::TaskExecutionFailed {
                message: format!("Object not found: {bucket}/{object}"),
            });
        }

        {
            let mut progress = self.progress.write().await;
            progress.update_progress(1, 3, 0, 0);
        }

        // Step 2: Perform EC decode heal using ecstore
        debug!(
            target: "rustfs::heal::task",
            event = EVENT_HEAL_EC_DECODE_STAGE,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_TASK,
            task_id = %self.id,
            bucket,
            object,
            stage = "heal_with_ecstore",
            "Heal EC decode stage entered"
        );
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
                    error!(
                        target: "rustfs::heal::task",
                        event = EVENT_HEAL_EC_DECODE_RESULT,
                        component = LOG_COMPONENT_HEAL,
                        subsystem = LOG_SUBSYSTEM_TASK,
                        task_id = %self.id,
                        bucket,
                        object,
                        result = "failed",
                        error = %e,
                        "Heal EC decode failed"
                    );
                    {
                        let mut progress = self.progress.write().await;
                        progress.update_progress(3, 3, 0, 0);
                    }
                    return Err(Error::TaskExecutionFailed {
                        message: format!("Failed to heal EC decode {bucket}/{object}: {e}"),
                    });
                }

                let object_size = result.object_size as u64;
                debug!(
                    target: "rustfs::heal::task",
                    event = EVENT_HEAL_EC_DECODE_RESULT,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_TASK,
                    task_id = %self.id,
                    bucket,
                    object,
                    object_size,
                    drives_healed = result.after.drives.len(),
                    result = "ok",
                    "Heal EC decode repaired"
                );

                {
                    let mut progress = self.progress.write().await;
                    progress.update_progress(3, 3, object_size, object_size);
                }
                self.record_result_item(result).await;
                Ok(())
            }
            Err(Error::TaskCancelled) => Err(Error::TaskCancelled),
            Err(Error::TaskTimeout) => Err(Error::TaskTimeout),
            Err(e) => {
                error!(
                    target: "rustfs::heal::task",
                    event = EVENT_HEAL_EC_DECODE_RESULT,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_TASK,
                    task_id = %self.id,
                    bucket,
                    object,
                    result = "failed",
                    error = %e,
                    "Heal EC decode failed"
                );
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
        debug!(
            target: "rustfs::heal::task",
            event = EVENT_HEAL_ERASURE_SET_STAGE,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_TASK,
            task_id = %self.id,
            set_disk_id,
            bucket_count = buckets.len(),
            stage = "start",
            "Heal erasure set started"
        );

        // update progress
        {
            let mut progress = self.progress.write().await;
            progress.set_current_object(Some(format!("erasure_set: {} ({} buckets)", set_disk_id, buckets.len())));
            progress.update_progress(0, 4, 0, 0);
        }

        let buckets = if buckets.is_empty() {
            debug!(
                target: "rustfs::heal::task",
                event = EVENT_HEAL_ERASURE_SET_STAGE,
                component = LOG_COMPONENT_HEAL,
                subsystem = LOG_SUBSYSTEM_TASK,
                task_id = %self.id,
                set_disk_id,
                stage = "list_buckets",
                "Heal erasure set bucket list resolved"
            );
            let bucket_infos = self.await_with_control(self.storage.list_buckets()).await?;
            bucket_infos.into_iter().map(|info| info.name).collect()
        } else {
            buckets
        };

        // Step 1: Perform disk format heal using ecstore
        debug!(
            target: "rustfs::heal::task",
            event = EVENT_HEAL_ERASURE_SET_STAGE,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_TASK,
            task_id = %self.id,
            set_disk_id,
            stage = "heal_format",
            "Heal erasure set stage entered"
        );
        let format_result = self.await_with_control(self.storage.heal_format(self.options.dry_run)).await;

        match format_result {
            Ok((result, error)) => {
                if let Some(e) = error {
                    if Self::is_no_heal_required_error(&e) {
                        debug!(
                            target: "rustfs::heal::task",
                            event = EVENT_HEAL_ERASURE_SET_RESULT,
                            component = LOG_COMPONENT_HEAL,
                            subsystem = LOG_SUBSYSTEM_TASK,
                            task_id = %self.id,
                            set_disk_id,
                            result = "format_noop",
                            "Heal erasure set format repair skipped because no format heal was required"
                        );
                    } else {
                        error!(
                            target: "rustfs::heal::task",
                            event = EVENT_HEAL_ERASURE_SET_RESULT,
                            component = LOG_COMPONENT_HEAL,
                            subsystem = LOG_SUBSYSTEM_TASK,
                            task_id = %self.id,
                            set_disk_id,
                            result = "format_failed",
                            error = %e,
                            "Heal erasure set failed"
                        );
                        {
                            let mut progress = self.progress.write().await;
                            progress.update_progress(4, 4, 0, 0);
                        }
                        return Err(Error::TaskExecutionFailed {
                            message: format!("Failed to heal disk format for {set_disk_id}: {e}"),
                        });
                    }
                } else {
                    debug!(
                        target: "rustfs::heal::task",
                        event = EVENT_HEAL_ERASURE_SET_RESULT,
                        component = LOG_COMPONENT_HEAL,
                        subsystem = LOG_SUBSYSTEM_TASK,
                        task_id = %self.id,
                        set_disk_id,
                        drives_healed = result.after.drives.len(),
                        result = "format_ok",
                        "Heal erasure set format repaired"
                    );
                }
            }
            Err(Error::TaskCancelled) => return Err(Error::TaskCancelled),
            Err(Error::TaskTimeout) => return Err(Error::TaskTimeout),
            Err(e) => {
                error!(
                    target: "rustfs::heal::task",
                    event = EVENT_HEAL_ERASURE_SET_RESULT,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_TASK,
                    task_id = %self.id,
                    set_disk_id,
                    result = "format_failed",
                    error = %e,
                    "Heal erasure set failed"
                );
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
        debug!(
            target: "rustfs::heal::task",
            event = EVENT_HEAL_ERASURE_SET_STAGE,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_TASK,
            task_id = %self.id,
            set_disk_id,
            stage = "resolve_resume_disk",
            "Heal erasure set stage entered"
        );
        let disk = self
            .await_with_control(self.storage.get_disk_for_resume(&set_disk_id))
            .await?;

        {
            let mut progress = self.progress.write().await;
            progress.update_progress(2, 4, 0, 0);
        }

        // Step 3: Heal bucket structure
        // Check control flags before each iteration to ensure timely cancellation.
        let bucket_heal_opts = HealOpts {
            recursive: false,
            dry_run: self.options.dry_run,
            remove: false,
            recreate: self.options.recreate_missing,
            scan_mode: self.options.scan_mode,
            update_parity: self.options.update_parity,
            no_lock: false,
            pool: self.options.pool_index,
            set: self.options.set_index,
        };

        for bucket in buckets.iter() {
            // Check control flags before starting each bucket heal
            self.check_control_flags().await?;
            let heal_result = self
                .await_with_control(self.storage.heal_bucket(bucket, &bucket_heal_opts))
                .await;
            match heal_result {
                Ok(result) => {
                    self.record_result_item(result).await;
                }
                Err(err) => {
                    // Check if error is due to cancellation or timeout
                    if matches!(err, Error::TaskCancelled | Error::TaskTimeout) {
                        return Err(err);
                    }
                    warn!(
                        target: "rustfs::heal::task",
                        event = EVENT_HEAL_ERASURE_SET_RESULT,
                        component = LOG_COMPONENT_HEAL,
                        subsystem = LOG_SUBSYSTEM_TASK,
                        task_id = %self.id,
                        set_disk_id,
                        bucket,
                        result = "bucket_failed",
                        error = %err,
                        "Heal erasure set bucket prepass failed"
                    );
                }
            }
        }

        // Create erasure set healer with resume support
        debug!(
            target: "rustfs::heal::task",
            event = EVENT_HEAL_ERASURE_SET_STAGE,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_TASK,
            task_id = %self.id,
            set_disk_id,
            stage = "build_resumable_healer",
            "Heal erasure set stage entered"
        );
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
        let erasure_healer =
            ErasureSetHealer::new(self.storage.clone(), self.progress.clone(), self.cancel_token.clone(), disk, heal_opts);

        {
            let mut progress = self.progress.write().await;
            progress.update_progress(3, 4, 0, 0);
        }

        // Step 4: Execute erasure set heal with resume
        debug!(
            target: "rustfs::heal::task",
            event = EVENT_HEAL_ERASURE_SET_STAGE,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_TASK,
            task_id = %self.id,
            set_disk_id,
            stage = "execute_resumable_heal",
            "Heal erasure set stage entered"
        );
        let result = erasure_healer.heal_erasure_set(&buckets, &set_disk_id).await;

        {
            let mut progress = self.progress.write().await;
            progress.update_progress(4, 4, 0, 0);
        }

        match result {
            Ok(_) => {
                debug!(
                    target: "rustfs::heal::task",
                    event = EVENT_HEAL_ERASURE_SET_RESULT,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_TASK,
                    task_id = %self.id,
                    set_disk_id,
                    bucket_count = buckets.len(),
                    result = "ok",
                    "Heal erasure set repaired"
                );
                Ok(())
            }
            Err(Error::TaskCancelled) => Err(Error::TaskCancelled),
            Err(Error::TaskTimeout) => Err(Error::TaskTimeout),
            Err(e) => {
                error!(
                    target: "rustfs::heal::task",
                    event = EVENT_HEAL_ERASURE_SET_RESULT,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_TASK,
                    task_id = %self.id,
                    set_disk_id,
                    result = "failed",
                    error = %e,
                    "Heal erasure set failed"
                );
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

#[cfg(test)]
mod tests {
    use super::super::{DiskStore, Endpoint};
    use super::*;
    use crate::heal::storage::{DiskStatus, HealObjectInfo};
    use rustfs_madmin::heal_commands::HealResultItem;
    use rustfs_storage_api::BucketInfo;
    use std::sync::Mutex;

    #[derive(Default)]
    struct MockStorage {
        listed: Mutex<bool>,
        healed_objects: Mutex<Vec<String>>,
        bucket_heal_opts: Mutex<Vec<HealOpts>>,
        object_heal_opts: Mutex<Vec<HealOpts>>,
        format_no_heal_required: Mutex<bool>,
        listed_prefixes: Mutex<Vec<String>>,
    }

    #[async_trait::async_trait]
    impl HealStorageAPI for MockStorage {
        async fn get_object_meta(&self, _bucket: &str, _object: &str) -> Result<Option<HealObjectInfo>> {
            Ok(None)
        }

        async fn get_object_data(&self, _bucket: &str, _object: &str) -> Result<Option<Vec<u8>>> {
            Ok(None)
        }

        async fn put_object_data(&self, _bucket: &str, _object: &str, _data: &[u8]) -> Result<()> {
            Ok(())
        }

        async fn delete_object(&self, _bucket: &str, _object: &str) -> Result<()> {
            Ok(())
        }

        async fn verify_object_integrity(&self, _bucket: &str, _object: &str) -> Result<bool> {
            Ok(true)
        }

        async fn ec_decode_rebuild(&self, _bucket: &str, _object: &str) -> Result<Vec<u8>> {
            Ok(Vec::new())
        }

        async fn get_disk_status(&self, _endpoint: &Endpoint) -> Result<DiskStatus> {
            Ok(DiskStatus::Ok)
        }

        async fn format_disk(&self, _endpoint: &Endpoint) -> Result<()> {
            Ok(())
        }

        async fn get_bucket_info(&self, bucket: &str) -> Result<Option<BucketInfo>> {
            Ok(Some(BucketInfo {
                name: bucket.to_string(),
                ..Default::default()
            }))
        }

        async fn heal_bucket_metadata(&self, _bucket: &str) -> Result<()> {
            Ok(())
        }

        async fn list_buckets(&self) -> Result<Vec<BucketInfo>> {
            Ok(vec![BucketInfo {
                name: "bucket-a".to_string(),
                ..Default::default()
            }])
        }

        async fn object_exists(&self, _bucket: &str, _object: &str) -> Result<bool> {
            Ok(true)
        }

        async fn get_object_size(&self, _bucket: &str, _object: &str) -> Result<Option<u64>> {
            Ok(None)
        }

        async fn get_object_checksum(&self, _bucket: &str, _object: &str) -> Result<Option<String>> {
            Ok(None)
        }

        async fn heal_object(
            &self,
            bucket: &str,
            object: &str,
            _version_id: Option<&str>,
            opts: &HealOpts,
        ) -> Result<(HealResultItem, Option<Error>)> {
            self.healed_objects.lock().unwrap().push(object.to_string());
            self.object_heal_opts.lock().unwrap().push(*opts);
            if bucket == RUSTFS_META_BUCKET && object == format!("{BUCKET_META_PREFIX}/{DATA_USAGE_CACHE_NAME}") {
                return Ok((
                    HealResultItem::default(),
                    Some(Error::other(
                        "Lock error: Lock acquisition timeout for resource '.rustfs.sys/buckets/.usage-cache.bin@latest' after 5s",
                    )),
                ));
            }
            Ok((
                HealResultItem {
                    object_size: 1,
                    ..Default::default()
                },
                None,
            ))
        }

        async fn heal_bucket(&self, _bucket: &str, opts: &HealOpts) -> Result<HealResultItem> {
            self.bucket_heal_opts.lock().unwrap().push(*opts);
            Ok(HealResultItem::default())
        }

        async fn heal_format(&self, _dry_run: bool) -> Result<(HealResultItem, Option<Error>)> {
            let no_heal_required = *self.format_no_heal_required.lock().unwrap();
            if no_heal_required {
                Ok((HealResultItem::default(), Some(Error::other("No heal required"))))
            } else {
                Ok((HealResultItem::default(), None))
            }
        }

        async fn list_objects_for_heal(&self, _bucket: &str, _prefix: &str) -> Result<Vec<String>> {
            Ok(vec!["object-a".to_string(), "object-b".to_string()])
        }

        async fn list_objects_for_heal_page(
            &self,
            bucket: &str,
            prefix: &str,
            continuation_token: Option<&str>,
        ) -> Result<(Vec<String>, Option<String>, bool)> {
            self.listed_prefixes.lock().unwrap().push(prefix.to_string());
            let mut listed = self.listed.lock().unwrap();
            if continuation_token.is_none() && !*listed {
                *listed = true;
                let objects = if bucket == RUSTFS_META_BUCKET {
                    vec![
                        format!("{BUCKET_META_PREFIX}/{DATA_USAGE_CACHE_NAME}"),
                        format!("{BUCKET_META_PREFIX}/bucket-metadata.bin"),
                    ]
                } else if prefix == "logs/" {
                    vec!["logs/object-a".to_string(), "logs/object-b".to_string()]
                } else {
                    vec!["object-a".to_string(), "object-b".to_string()]
                };
                Ok((objects, None, false))
            } else {
                Ok((Vec::new(), None, false))
            }
        }

        async fn get_disk_for_resume(&self, _set_disk_id: &str) -> Result<DiskStore> {
            Err(Error::other("not implemented in tests"))
        }
    }

    #[tokio::test]
    async fn test_recursive_bucket_heal_visits_objects() {
        let storage = Arc::new(MockStorage::default());
        let request = HealRequest::new(
            HealType::Bucket {
                bucket: "bucket-a".to_string(),
            },
            HealOptions {
                recursive: true,
                timeout: None,
                ..Default::default()
            },
            HealPriority::Normal,
        );
        let task = HealTask::from_request(request, storage.clone());

        task.heal_bucket("bucket-a")
            .await
            .expect("recursive bucket heal should succeed");

        assert_eq!(
            storage.healed_objects.lock().unwrap().as_slice(),
            ["object-a".to_string(), "object-b".to_string()]
        );
        let progress = task.get_progress().await;
        assert_eq!(progress.objects_scanned, 2);
        assert_eq!(progress.objects_healed, 2);
        let result_items = task.get_result_items().await;
        assert_eq!(result_items.len(), 3);
        assert_eq!(result_items.iter().filter(|item| item.object_size == 1).count(), 2);
    }

    #[tokio::test]
    async fn test_cluster_heal_visits_bucket_objects() {
        let storage = Arc::new(MockStorage::default());
        let request = HealRequest::new(
            HealType::Cluster,
            HealOptions {
                recursive: true,
                timeout: None,
                ..Default::default()
            },
            HealPriority::Normal,
        );
        let task = HealTask::from_request(request, storage.clone());

        task.execute().await.expect("cluster heal should visit bucket objects");

        assert_eq!(
            storage.healed_objects.lock().unwrap().as_slice(),
            ["object-a".to_string(), "object-b".to_string()]
        );
        assert!(matches!(task.get_status().await, HealTaskStatus::Completed));
    }

    #[tokio::test]
    async fn test_recursive_bucket_heal_does_not_remove_bucket_metadata() {
        let storage = Arc::new(MockStorage::default());
        let request = HealRequest::new(
            HealType::Bucket {
                bucket: "bucket-a".to_string(),
            },
            HealOptions {
                recursive: true,
                remove_corrupted: true,
                recreate_missing: true,
                scan_mode: HealScanMode::Deep,
                timeout: None,
                ..Default::default()
            },
            HealPriority::Normal,
        );
        let task = HealTask::from_request(request, storage.clone());

        task.heal_bucket("bucket-a")
            .await
            .expect("recursive bucket heal should succeed");

        let bucket_opts = storage.bucket_heal_opts.lock().unwrap();
        assert_eq!(bucket_opts.len(), 1);
        assert!(!bucket_opts[0].remove);
        assert!(bucket_opts[0].recreate);
        assert_eq!(bucket_opts[0].scan_mode, HealScanMode::Deep);

        let object_opts = storage.object_heal_opts.lock().unwrap();
        assert_eq!(object_opts.len(), 2);
        assert!(object_opts.iter().all(|opts| opts.remove));
        assert!(object_opts.iter().all(|opts| opts.recreate));
        assert!(object_opts.iter().all(|opts| opts.scan_mode == HealScanMode::Deep));
    }

    #[tokio::test]
    async fn test_prefix_heal_lists_and_repairs_objects_under_prefix() {
        let storage = Arc::new(MockStorage::default());
        let request = HealRequest::new(
            HealType::Prefix {
                bucket: "bucket-a".to_string(),
                prefix: "logs/".to_string(),
            },
            HealOptions {
                recursive: true,
                timeout: None,
                ..Default::default()
            },
            HealPriority::Normal,
        );
        let task = HealTask::from_request(request, storage.clone());

        task.execute()
            .await
            .expect("prefix heal should scan and repair objects under the prefix");

        assert_eq!(storage.listed_prefixes.lock().unwrap().as_slice(), ["logs/".to_string()]);
        assert_eq!(
            storage.healed_objects.lock().unwrap().as_slice(),
            ["logs/object-a".to_string(), "logs/object-b".to_string()]
        );
    }

    #[tokio::test]
    async fn test_data_usage_cache_lock_timeout_does_not_fail_object_heal() {
        let storage = Arc::new(MockStorage::default());
        let request = HealRequest::new(
            HealType::Object {
                bucket: RUSTFS_META_BUCKET.to_string(),
                object: format!("{BUCKET_META_PREFIX}/{DATA_USAGE_CACHE_NAME}"),
                version_id: None,
            },
            HealOptions {
                recreate_missing: true,
                timeout: None,
                ..Default::default()
            },
            HealPriority::Normal,
        );
        let task = HealTask::from_request(request, storage);

        task.execute()
            .await
            .expect("data usage cache lock timeout should be skipped during heal");

        assert!(matches!(task.get_status().await, HealTaskStatus::Completed));
    }

    #[tokio::test]
    async fn test_data_usage_cache_lock_timeout_does_not_fail_recursive_bucket_heal() {
        let storage = Arc::new(MockStorage::default());
        let request = HealRequest::new(
            HealType::Bucket {
                bucket: RUSTFS_META_BUCKET.to_string(),
            },
            HealOptions {
                recursive: true,
                timeout: None,
                ..Default::default()
            },
            HealPriority::Normal,
        );
        let task = HealTask::from_request(request, storage);

        task.execute()
            .await
            .expect("recursive bucket heal should skip transient data usage cache lock timeouts");

        assert!(matches!(task.get_status().await, HealTaskStatus::Completed));
        let progress = task.get_progress().await;
        assert_eq!(progress.objects_scanned, 2);
        assert_eq!(progress.objects_failed, 0);
    }

    #[tokio::test]
    async fn test_erasure_set_heal_continues_after_format_no_heal_required() {
        let storage = Arc::new(MockStorage::default());
        *storage.format_no_heal_required.lock().unwrap() = true;
        let request = HealRequest::new(
            HealType::ErasureSet {
                buckets: Vec::new(),
                set_disk_id: "pool_0_set_0".to_string(),
            },
            HealOptions {
                timeout: None,
                ..Default::default()
            },
            HealPriority::Normal,
        );
        let task = HealTask::from_request(request, storage);

        let err = task
            .heal_erasure_set(Vec::new(), "pool_0_set_0".to_string())
            .await
            .expect_err("test mock should fail after format when resolving resume disk");

        assert!(
            err.to_string().contains("not implemented in tests"),
            "erasure-set heal should continue past NoHealRequired format result, got: {err}"
        );
    }
}
