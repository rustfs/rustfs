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

use crate::disk::{
    CheckPartsResp, DeleteOptions, DiskAPI, DiskError, DiskInfo, DiskInfoOptions, DiskLocation, Endpoint, Error,
    FileInfoVersions, ReadMultipleReq, ReadMultipleResp, ReadOptions, RenameDataResp, Result, UpdateMetadataOpts, VolumeInfo,
    WalkDirOptions,
    health_state::{
        RuntimeDriveHealthState, classify_drive_recovery, get_drive_returning_probe_interval,
        get_drive_returning_success_threshold, get_drive_suspect_failure_threshold, record_drive_offline_duration,
        record_drive_recovery_class, record_drive_runtime_state, record_drive_state_transition,
    },
    local::{LocalDisk, ScanGuard},
};
use crate::global::GLOBAL_LOCAL_DISK_ID_MAP;
use bytes::Bytes;
use metrics::counter;
use rustfs_filemeta::{FileInfo, ObjectPartInfo, RawFileInfo};
use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicI64, AtomicU32, Ordering},
    },
    time::Duration,
};
use tokio::{sync::RwLock, time};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};
use uuid::Uuid;

/// Disk health status constants
const DISK_HEALTH_OK: u32 = 0;
const DISK_HEALTH_FAULTY: u32 = 1;

pub const ENV_RUSTFS_DRIVE_ACTIVE_MONITORING: &str = "RUSTFS_DRIVE_ACTIVE_MONITORING";
pub const DEFAULT_RUSTFS_DRIVE_ACTIVE_MONITORING: bool = true;
pub const SKIP_IF_SUCCESS_BEFORE: Duration = Duration::from_secs(5);

lazy_static::lazy_static! {
    static ref TEST_DATA: Bytes = Bytes::from(vec![42u8; 2048]);
    static ref TEST_BUCKET: String = ".rustfs.sys/tmp".to_string();
}

pub fn get_max_timeout_duration() -> Duration {
    Duration::from_secs(rustfs_utils::get_env_u64(
        rustfs_config::ENV_DRIVE_MAX_TIMEOUT_DURATION,
        rustfs_config::DEFAULT_DRIVE_MAX_TIMEOUT_DURATION_SECS,
    ))
}

fn get_drive_timeout_duration(env_key: &str, default_secs: u64) -> Duration {
    Duration::from_secs(rustfs_utils::get_env_u64(env_key, default_secs))
}

pub fn get_drive_metadata_timeout() -> Duration {
    get_drive_timeout_duration(
        rustfs_config::ENV_DRIVE_METADATA_TIMEOUT_SECS,
        rustfs_config::DEFAULT_DRIVE_METADATA_TIMEOUT_SECS,
    )
}

pub fn get_drive_disk_info_timeout() -> Duration {
    get_drive_timeout_duration(
        rustfs_config::ENV_DRIVE_DISK_INFO_TIMEOUT_SECS,
        rustfs_config::DEFAULT_DRIVE_DISK_INFO_TIMEOUT_SECS,
    )
}

pub fn get_drive_list_dir_timeout() -> Duration {
    get_drive_timeout_duration(
        rustfs_config::ENV_DRIVE_LIST_DIR_TIMEOUT_SECS,
        rustfs_config::DEFAULT_DRIVE_LIST_DIR_TIMEOUT_SECS,
    )
}

pub fn get_drive_walkdir_timeout() -> Duration {
    get_drive_timeout_duration(
        rustfs_config::ENV_DRIVE_WALKDIR_TIMEOUT_SECS,
        rustfs_config::DEFAULT_DRIVE_WALKDIR_TIMEOUT_SECS,
    )
}

pub fn get_drive_walkdir_stall_timeout() -> Duration {
    get_drive_timeout_duration(
        rustfs_config::ENV_DRIVE_WALKDIR_STALL_TIMEOUT_SECS,
        rustfs_config::DEFAULT_DRIVE_WALKDIR_STALL_TIMEOUT_SECS,
    )
}

pub fn get_drive_active_check_interval() -> Duration {
    Duration::from_secs(rustfs_utils::get_env_u64(
        rustfs_config::ENV_DRIVE_ACTIVE_CHECK_INTERVAL_SECS,
        rustfs_config::DEFAULT_DRIVE_ACTIVE_CHECK_INTERVAL_SECS,
    ))
}

pub fn get_drive_active_check_timeout() -> Duration {
    Duration::from_secs(rustfs_utils::get_env_u64(
        rustfs_config::ENV_DRIVE_ACTIVE_CHECK_TIMEOUT_SECS,
        rustfs_config::DEFAULT_DRIVE_ACTIVE_CHECK_TIMEOUT_SECS,
    ))
}

/// DiskHealthTracker tracks the health status of a disk.
/// Similar to Go's diskHealthTracker.
#[derive(Debug)]
pub struct DiskHealthTracker {
    /// Atomic timestamp of last successful operation
    pub last_success: AtomicI64,
    /// Atomic timestamp of last operation start
    pub last_started: AtomicI64,
    /// Atomic disk status (OK or Faulty)
    pub status: AtomicU32,
    /// Atomic number of waiting operations
    pub waiting: AtomicU32,
    /// Runtime drive health state
    pub runtime_state: AtomicU32,
    /// Consecutive failures while transitioning away from online
    pub consecutive_failures: AtomicU32,
    /// Consecutive successes while returning online
    pub consecutive_successes: AtomicU32,
    /// When the drive first left the online state
    pub offline_since_unix_secs: AtomicI64,
    /// Last runtime state transition timestamp
    pub last_transition_unix_secs: AtomicI64,
}

impl DiskHealthTracker {
    /// Create a new disk health tracker
    pub fn new() -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as i64;

        Self {
            last_success: AtomicI64::new(now),
            last_started: AtomicI64::new(now),
            status: AtomicU32::new(DISK_HEALTH_OK),
            waiting: AtomicU32::new(0),
            runtime_state: AtomicU32::new(RuntimeDriveHealthState::Online as u32),
            consecutive_failures: AtomicU32::new(0),
            consecutive_successes: AtomicU32::new(0),
            offline_since_unix_secs: AtomicI64::new(0),
            last_transition_unix_secs: AtomicI64::new(now / 1_000_000_000),
        }
    }

    /// Log a successful operation
    pub fn log_success(&self) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as i64;
        self.last_success.store(now, Ordering::Relaxed);
    }

    /// Check if disk is faulty
    pub fn is_faulty(&self) -> bool {
        self.status.load(Ordering::Acquire) == DISK_HEALTH_FAULTY
    }

    /// Set disk as faulty
    pub fn set_faulty(&self) {
        self.status.store(DISK_HEALTH_FAULTY, Ordering::Release);
    }

    /// Set disk as OK
    pub fn set_ok(&self) {
        self.status.store(DISK_HEALTH_OK, Ordering::Release);
    }

    #[cfg(test)]
    pub fn force_runtime_state_for_test(&self, state: RuntimeDriveHealthState) {
        self.runtime_state.store(state as u32, Ordering::Release);
        match state {
            RuntimeDriveHealthState::Offline => self.set_faulty(),
            RuntimeDriveHealthState::Online | RuntimeDriveHealthState::Suspect | RuntimeDriveHealthState::Returning => {
                self.set_ok();
            }
        }
    }

    pub fn swap_ok_to_faulty(&self) -> bool {
        self.status
            .compare_exchange(DISK_HEALTH_OK, DISK_HEALTH_FAULTY, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
    }

    pub fn runtime_state(&self) -> RuntimeDriveHealthState {
        RuntimeDriveHealthState::from_u32(self.runtime_state.load(Ordering::Acquire))
    }

    pub fn offline_duration(&self) -> Option<Duration> {
        let offline_since = self.offline_since_unix_secs.load(Ordering::Acquire);
        if offline_since <= 0 {
            return None;
        }
        let now = current_unix_secs();
        Some(Duration::from_secs(now.saturating_sub(offline_since as u64)))
    }

    pub fn mark_failure(&self, endpoint: &Endpoint, reason: &'static str) -> bool {
        let current = self.runtime_state();
        let now = current_unix_secs();
        let next = match current {
            RuntimeDriveHealthState::Online => {
                self.consecutive_failures.store(1, Ordering::Release);
                self.consecutive_successes.store(0, Ordering::Release);
                self.offline_since_unix_secs
                    .compare_exchange(0, now as i64, Ordering::AcqRel, Ordering::Relaxed)
                    .ok();
                RuntimeDriveHealthState::Suspect
            }
            RuntimeDriveHealthState::Suspect => {
                let failures = self.consecutive_failures.fetch_add(1, Ordering::AcqRel) + 1;
                if failures >= get_drive_suspect_failure_threshold() {
                    RuntimeDriveHealthState::Offline
                } else {
                    RuntimeDriveHealthState::Suspect
                }
            }
            RuntimeDriveHealthState::Returning => {
                self.consecutive_failures.store(0, Ordering::Release);
                self.consecutive_successes.store(0, Ordering::Release);
                RuntimeDriveHealthState::Offline
            }
            RuntimeDriveHealthState::Offline => RuntimeDriveHealthState::Offline,
        };

        self.status.store(DISK_HEALTH_FAULTY, Ordering::Release);
        self.transition_state(endpoint, current, next, reason);
        current == RuntimeDriveHealthState::Online
    }

    pub fn mark_recovery_success(&self, endpoint: &Endpoint, reason: &'static str) -> bool {
        let current = self.runtime_state();
        let next = match current {
            RuntimeDriveHealthState::Online => RuntimeDriveHealthState::Online,
            RuntimeDriveHealthState::Suspect => RuntimeDriveHealthState::Online,
            RuntimeDriveHealthState::Offline => {
                self.consecutive_successes.store(1, Ordering::Release);
                RuntimeDriveHealthState::Returning
            }
            RuntimeDriveHealthState::Returning => {
                let successes = self.consecutive_successes.fetch_add(1, Ordering::AcqRel) + 1;
                if successes >= get_drive_returning_success_threshold() {
                    RuntimeDriveHealthState::Online
                } else {
                    RuntimeDriveHealthState::Returning
                }
            }
        };

        let became_online = next == RuntimeDriveHealthState::Online;
        if became_online {
            self.status.store(DISK_HEALTH_OK, Ordering::Release);
            self.consecutive_failures.store(0, Ordering::Release);
            self.consecutive_successes.store(0, Ordering::Release);
        }
        self.transition_state(endpoint, current, next, reason);
        if became_online {
            self.log_success();
        }
        became_online
    }

    fn transition_state(
        &self,
        endpoint: &Endpoint,
        current: RuntimeDriveHealthState,
        next: RuntimeDriveHealthState,
        reason: &'static str,
    ) {
        if current == next {
            return;
        }

        self.runtime_state.store(next as u32, Ordering::Release);
        self.last_transition_unix_secs
            .store(current_unix_secs() as i64, Ordering::Release);

        if matches!(
            next,
            RuntimeDriveHealthState::Suspect | RuntimeDriveHealthState::Offline | RuntimeDriveHealthState::Returning
        ) && self.offline_since_unix_secs.load(Ordering::Acquire) == 0
        {
            self.offline_since_unix_secs
                .store(current_unix_secs() as i64, Ordering::Release);
        }

        if next == RuntimeDriveHealthState::Online {
            if let Some(duration) = self.offline_duration() {
                record_drive_offline_duration(endpoint, duration);
                record_drive_recovery_class(classify_drive_recovery(duration));
            }
            self.offline_since_unix_secs.store(0, Ordering::Release);
        } else if let Some(duration) = self.offline_duration() {
            record_drive_offline_duration(endpoint, duration);
        }

        record_drive_state_transition(endpoint, current, next, reason);
        record_drive_runtime_state(endpoint, next);
    }

    /// Increment waiting operations counter
    pub fn increment_waiting(&self) {
        self.waiting.fetch_add(1, Ordering::Relaxed);
    }

    /// Decrement waiting operations counter
    pub fn decrement_waiting(&self) {
        self.waiting.fetch_sub(1, Ordering::Relaxed);
    }

    /// Get waiting operations count
    pub fn waiting_count(&self) -> u32 {
        self.waiting.load(Ordering::Relaxed)
    }

    /// Get last success timestamp
    pub fn last_success(&self) -> i64 {
        self.last_success.load(Ordering::Acquire)
    }
}

fn current_unix_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

impl Default for DiskHealthTracker {
    fn default() -> Self {
        Self::new()
    }
}

/// Health check context key for tracking disk operations
#[derive(Debug, Clone)]
struct HealthDiskCtxKey;

#[derive(Debug)]
struct HealthDiskCtxValue {
    last_success: Arc<AtomicI64>,
}

impl HealthDiskCtxValue {
    fn log_success(&self) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as i64;
        self.last_success.store(now, Ordering::Relaxed);
    }
}

/// LocalDiskWrapper wraps a DiskStore with health tracking capabilities.
/// This is similar to Go's xlStorageDiskIDCheck.
#[derive(Debug, Clone)]
pub struct LocalDiskWrapper {
    /// The underlying disk store
    disk: Arc<LocalDisk>,
    /// Health tracker
    health: Arc<DiskHealthTracker>,
    /// Whether health checking is enabled
    health_check: bool,
    /// Cancellation token for monitoring tasks
    cancel_token: CancellationToken,
    /// Disk ID for stale checking
    disk_id: Arc<RwLock<Option<Uuid>>>,
}

impl LocalDiskWrapper {
    /// Create a new LocalDiskWrapper
    pub fn new(disk: Arc<LocalDisk>, health_check: bool) -> Self {
        // Check environment variable for health check override.
        // Only enable if both param and env are true.
        let env_health_check =
            rustfs_utils::get_env_bool(ENV_RUSTFS_DRIVE_ACTIVE_MONITORING, DEFAULT_RUSTFS_DRIVE_ACTIVE_MONITORING);

        let wrapper = Self {
            disk,
            health: Arc::new(DiskHealthTracker::new()),
            health_check: health_check && env_health_check,
            cancel_token: CancellationToken::new(),
            disk_id: Arc::new(RwLock::new(None)),
        };
        record_drive_runtime_state(&wrapper.disk.endpoint(), RuntimeDriveHealthState::Online);
        wrapper
    }

    pub fn get_disk(&self) -> Arc<LocalDisk> {
        self.disk.clone()
    }

    pub fn runtime_state(&self) -> RuntimeDriveHealthState {
        self.health.runtime_state()
    }

    pub fn offline_duration_secs(&self) -> Option<u64> {
        self.health.offline_duration().map(|duration| duration.as_secs())
    }

    #[cfg(test)]
    pub fn force_runtime_state_for_test(&self, state: RuntimeDriveHealthState) {
        self.health.force_runtime_state_for_test(state);
    }

    /// Enable health monitoring after disk creation.
    /// Used to defer health checks until after startup format loading completes.
    pub fn enable_health_check(&self) {
        if !self.health_check {
            return;
        }
        let health = Arc::clone(&self.health);
        let cancel_token = self.cancel_token.clone();
        let disk = Arc::clone(&self.disk);

        tokio::spawn(async move {
            Self::monitor_disk_writable(disk, health, cancel_token).await;
        });
    }

    /// Stop the disk monitoring
    pub async fn stop_monitoring(&self) {
        self.cancel_token.cancel();
    }

    fn spawn_recovery_monitor_if_needed(&self) {
        if !self.health_check {
            return;
        }

        self.health.increment_waiting();
        let health = Arc::clone(&self.health);
        let disk = Arc::clone(&self.disk);
        let cancel_token = self.cancel_token.clone();
        tokio::spawn(async move {
            Self::monitor_disk_status(disk, health, cancel_token).await;
        });
    }

    /// Monitor disk writability periodically
    async fn monitor_disk_writable(disk: Arc<LocalDisk>, health: Arc<DiskHealthTracker>, cancel_token: CancellationToken) {
        // TODO: config interval

        let mut interval = time::interval(get_drive_active_check_interval());

        loop {
            tokio::select! {
                _ = cancel_token.cancelled() => {
                    return;
                }
                _ = interval.tick() => {
                    if cancel_token.is_cancelled() {
                        return;
                    }

                    if health.status.load(Ordering::Relaxed) != DISK_HEALTH_OK {
                        continue;
                    }

                    let last_success_nanos = health.last_success.load(Ordering::Relaxed);
                    let elapsed = Duration::from_nanos(
                        (std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .as_nanos() as i64 - last_success_nanos) as u64
                    );

                    if elapsed < SKIP_IF_SUCCESS_BEFORE {
                        continue;
                    }

                    tokio::time::sleep(Duration::from_secs(1)).await;



                    let test_obj = format!("health-check-{}", Uuid::new_v4());
                    if Self::perform_health_check(
                        disk.clone(),
                        &TEST_BUCKET,
                        &test_obj,
                        &TEST_DATA,
                        true,
                        get_drive_active_check_timeout(),
                    )
                    .await
                    .is_err()
                        && health.mark_failure(&disk.endpoint(), "active_health_check_failed")
                    {
                        // Health check failed, disk is considered faulty
                        warn!("health check: failed, disk is considered faulty");

                        health.increment_waiting(); // Balance the increment from failed operation

                        let health_clone = Arc::clone(&health);
                        let disk_clone = disk.clone();
                        let cancel_clone = cancel_token.clone();

                        tokio::spawn(async move {
                            Self::monitor_disk_status(disk_clone, health_clone, cancel_clone).await;
                        });
                    }
                }
            }
        }
    }

    /// Perform a health check by writing and reading a test file
    async fn perform_health_check(
        disk: Arc<LocalDisk>,
        test_bucket: &str,
        test_filename: &str,
        test_data: &Bytes,
        check_faulty_only: bool,
        timeout_duration: Duration,
    ) -> Result<()> {
        // Perform health check with timeout
        let health_check_result = tokio::time::timeout(timeout_duration, async {
            // Try to write test data
            disk.write_all(test_bucket, test_filename, test_data.clone()).await?;

            // Try to read back the data
            let read_data = disk.read_all(test_bucket, test_filename).await?;

            // Verify data integrity
            if read_data.len() != test_data.len() {
                warn!(
                    "health check: test file data length mismatch: expected {} bytes, got {}",
                    test_data.len(),
                    read_data.len()
                );
                if check_faulty_only {
                    return Ok(());
                }
                return Err(DiskError::FaultyDisk);
            }

            // Clean up
            disk.delete(
                test_bucket,
                test_filename,
                DeleteOptions {
                    recursive: false,
                    immediate: false,
                    undo_write: false,
                    old_data_dir: None,
                },
            )
            .await?;

            Ok(())
        })
        .await;

        match health_check_result {
            Ok(result) => match result {
                Ok(()) => Ok(()),
                Err(e) => {
                    warn!("health check: failed: {:?}", e);

                    if e == DiskError::FaultyDisk {
                        return Err(e);
                    }

                    if check_faulty_only { Ok(()) } else { Err(e) }
                }
            },
            Err(_) => {
                // Timeout occurred
                warn!("health check: timeout after {:?}", timeout_duration);
                Err(DiskError::FaultyDisk)
            }
        }
    }

    /// Monitor disk status and try to bring it back online
    async fn monitor_disk_status(disk: Arc<LocalDisk>, health: Arc<DiskHealthTracker>, cancel_token: CancellationToken) {
        let check_every = get_drive_returning_probe_interval();

        let mut interval = time::interval(check_every);

        loop {
            tokio::select! {
                _ = cancel_token.cancelled() => {
                    return;
                }
                _ = interval.tick() => {
                    if cancel_token.is_cancelled() {
                        return;
                    }

                    let test_obj = format!("health-check-{}", Uuid::new_v4());
                    match Self::perform_health_check(
                        disk.clone(),
                        &TEST_BUCKET,
                        &test_obj,
                        &TEST_DATA,
                        false,
                        get_drive_active_check_timeout(),
                    )
                    .await
                    {
                        Ok(_) => {
                            let state_before = health.runtime_state();
                            let is_online = health.mark_recovery_success(&disk.endpoint(), "recovery_probe_success");
                            info!("Disk {} recovery probe succeeded; state={:?}", disk.to_string(), state_before);
                            if !is_online {
                                continue;
                            }
                            info!("Disk {} is back online", disk.to_string());
                            health.decrement_waiting();
                            return;
                        }
                        Err(e) => {
                            health.mark_failure(&disk.endpoint(), "recovery_probe_failed");
                            warn!("Disk {} still faulty: {:?}", disk.to_string(), e);
                        }
                    }
                }
            }
        }
    }

    async fn check_id(&self, want_id: Option<Uuid>) -> Result<()> {
        if want_id.is_none() {
            return Ok(());
        }

        let stored_disk_id = self.disk.get_disk_id().await?;

        if stored_disk_id != want_id {
            return Err(Error::other(format!("Disk ID mismatch wanted {want_id:?}, got {stored_disk_id:?}")));
        }

        Ok(())
    }

    /// Check if disk ID is stale
    async fn check_disk_stale(&self) -> Result<()> {
        let Some(current_disk_id) = *self.disk_id.read().await else {
            return Ok(());
        };

        let stored_disk_id = match self.disk.get_disk_id().await? {
            Some(id) => id,
            None => return Ok(()), // Empty disk ID is allowed during initialization
        };

        if current_disk_id != stored_disk_id {
            return Err(DiskError::DiskNotFound);
        }

        Ok(())
    }

    /// Set the disk ID
    pub async fn set_disk_id_internal(&self, id: Option<Uuid>) -> Result<()> {
        let mut disk_id = self.disk_id.write().await;
        let previous = *disk_id;
        *disk_id = id;
        drop(disk_id);

        if self.disk.is_local() {
            let mut disk_id_map = GLOBAL_LOCAL_DISK_ID_MAP.write().await;
            if let Some(previous_id) = previous {
                disk_id_map.remove(&previous_id);
            }
            if let Some(current_id) = id {
                disk_id_map.insert(current_id, self.disk.endpoint().to_string());
            }
        }
        Ok(())
    }

    /// Get the current disk ID
    pub async fn get_current_disk_id(&self) -> Option<Uuid> {
        *self.disk_id.read().await
    }

    /// Track disk health for an operation.
    /// This method should wrap disk operations to ensure health checking.
    pub async fn track_disk_health<T, F, Fut>(&self, operation: F, timeout_duration: Duration) -> Result<T>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        self.track_disk_health_with_op("unknown", operation, timeout_duration).await
    }

    pub async fn track_disk_health_with_op<T, F, Fut>(
        &self,
        op: &'static str,
        operation: F,
        timeout_duration: Duration,
    ) -> Result<T>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        // Check if disk is faulty
        if self.health.is_faulty() {
            warn!("local disk {} health is faulty, returning error", self.to_string());
            return Err(DiskError::FaultyDisk);
        }

        // Check if disk is stale
        self.check_disk_stale().await?;

        // Record operation start
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as i64;
        self.health.last_started.store(now, Ordering::Relaxed);
        self.health.increment_waiting();

        if timeout_duration == Duration::ZERO {
            let result = operation().await;
            self.health.decrement_waiting();
            if result.is_ok() {
                self.health.log_success();
            }
            return result;
        }
        // Execute the operation with timeout
        let result = tokio::time::timeout(timeout_duration, operation()).await;

        match result {
            Ok(operation_result) => {
                // Log success and decrement waiting counter
                if operation_result.is_ok() {
                    self.health.log_success();
                }
                self.health.decrement_waiting();
                operation_result
            }
            Err(_) => {
                // Timeout occurred, mark disk as potentially faulty and decrement waiting counter
                self.health.decrement_waiting();
                if self.health.mark_failure(&self.endpoint(), "operation_timeout") {
                    self.spawn_recovery_monitor_if_needed();
                }
                counter!(
                    "rustfs_drive_op_timeout_total",
                    "endpoint" => self.endpoint().to_string(),
                    "op" => op.to_string()
                )
                .increment(1);
                warn!(
                    endpoint = %self.endpoint(),
                    op,
                    timeout_ms = timeout_duration.as_millis(),
                    "Local disk operation timed out"
                );
                Err(DiskError::other(format!("disk operation timeout after {timeout_duration:?}")))
            }
        }
    }
}

#[async_trait::async_trait]
impl DiskAPI for LocalDiskWrapper {
    async fn read_metadata(&self, volume: &str, path: &str) -> Result<Bytes> {
        self.track_disk_health_with_op(
            "read_metadata",
            || async { self.disk.read_metadata(volume, path).await },
            get_drive_metadata_timeout(),
        )
        .await
    }

    fn start_scan(&self) -> ScanGuard {
        self.disk.start_scan()
    }

    fn to_string(&self) -> String {
        self.disk.to_string()
    }

    async fn is_online(&self) -> bool {
        let Ok(Some(disk_id)) = self.disk.get_disk_id().await else {
            return false;
        };

        // if disk_id is not set use the current disk_id
        if let Some(current_disk_id) = *self.disk_id.read().await {
            return current_disk_id == disk_id;
        } else {
            // if disk_id is not set, update the disk_id
            let _ = self.set_disk_id_internal(Some(disk_id)).await;
        }

        return true;
    }

    fn is_local(&self) -> bool {
        self.disk.is_local()
    }

    fn host_name(&self) -> String {
        self.disk.host_name()
    }

    fn endpoint(&self) -> Endpoint {
        self.disk.endpoint()
    }

    async fn close(&self) -> Result<()> {
        self.stop_monitoring().await;
        self.disk.close().await
    }

    async fn get_disk_id(&self) -> Result<Option<Uuid>> {
        self.disk.get_disk_id().await
    }

    async fn set_disk_id(&self, id: Option<Uuid>) -> Result<()> {
        self.set_disk_id_internal(id).await
    }

    fn path(&self) -> PathBuf {
        self.disk.path()
    }

    fn get_disk_location(&self) -> DiskLocation {
        self.disk.get_disk_location()
    }

    async fn disk_info(&self, opts: &DiskInfoOptions) -> Result<DiskInfo> {
        if opts.noop && opts.metrics {
            let mut info = DiskInfo::default();
            // Add health metrics
            info.metrics.total_waiting = self.health.waiting_count();
            if self.health.is_faulty() {
                return Err(DiskError::FaultyDisk);
            }
            return Ok(info);
        }

        if self.health.is_faulty() {
            return Err(DiskError::FaultyDisk);
        }

        self.track_disk_health_with_op(
            "disk_info",
            || async {
                let result = self.disk.disk_info(opts).await?;

                if let Some(current_disk_id) = *self.disk_id.read().await
                    && Some(current_disk_id) != result.id
                {
                    return Err(DiskError::DiskNotFound);
                };

                Ok(result)
            },
            get_drive_disk_info_timeout(),
        )
        .await
    }

    async fn make_volume(&self, volume: &str) -> Result<()> {
        self.track_disk_health(|| async { self.disk.make_volume(volume).await }, get_max_timeout_duration())
            .await
    }

    async fn make_volumes(&self, volumes: Vec<&str>) -> Result<()> {
        self.track_disk_health(|| async { self.disk.make_volumes(volumes).await }, get_max_timeout_duration())
            .await
    }

    async fn list_volumes(&self) -> Result<Vec<VolumeInfo>> {
        self.track_disk_health_with_op("list_volumes", || async { self.disk.list_volumes().await }, Duration::ZERO)
            .await
    }

    async fn stat_volume(&self, volume: &str) -> Result<VolumeInfo> {
        self.track_disk_health(|| async { self.disk.stat_volume(volume).await }, get_max_timeout_duration())
            .await
    }

    async fn delete_volume(&self, volume: &str) -> Result<()> {
        self.track_disk_health(|| async { self.disk.delete_volume(volume).await }, Duration::ZERO)
            .await
    }

    async fn walk_dir<W: tokio::io::AsyncWrite + Unpin + Send>(&self, opts: WalkDirOptions, wr: &mut W) -> Result<()> {
        self.track_disk_health_with_op("walk_dir", || async { self.disk.walk_dir(opts, wr).await }, get_drive_walkdir_timeout())
            .await
    }

    async fn delete_version(
        &self,
        volume: &str,
        path: &str,
        fi: FileInfo,
        force_del_marker: bool,
        opts: DeleteOptions,
    ) -> Result<()> {
        self.track_disk_health(
            || async { self.disk.delete_version(volume, path, fi, force_del_marker, opts).await },
            get_max_timeout_duration(),
        )
        .await
    }

    async fn delete_versions(&self, volume: &str, versions: Vec<FileInfoVersions>, opts: DeleteOptions) -> Vec<Option<Error>> {
        // Check if disk is faulty before proceeding
        if self.health.is_faulty() {
            return vec![Some(DiskError::FaultyDisk); versions.len()];
        }

        // Check if disk is stale
        if let Err(e) = self.check_disk_stale().await {
            return vec![Some(e); versions.len()];
        }

        // Record operation start
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as i64;
        self.health.last_started.store(now, Ordering::Relaxed);
        self.health.increment_waiting();

        // Execute the operation
        let result = self.disk.delete_versions(volume, versions, opts).await;

        self.health.decrement_waiting();
        let has_err = result.iter().any(|e| e.is_some());
        if !has_err {
            // Log success and decrement waiting counter
            self.health.log_success();
        }

        result
    }

    async fn delete_paths(&self, volume: &str, paths: &[String]) -> Result<()> {
        self.track_disk_health(|| async { self.disk.delete_paths(volume, paths).await }, get_max_timeout_duration())
            .await
    }

    async fn write_metadata(&self, org_volume: &str, volume: &str, path: &str, fi: FileInfo) -> Result<()> {
        self.track_disk_health(
            || async { self.disk.write_metadata(org_volume, volume, path, fi).await },
            get_max_timeout_duration(),
        )
        .await
    }

    async fn update_metadata(&self, volume: &str, path: &str, fi: FileInfo, opts: &UpdateMetadataOpts) -> Result<()> {
        self.track_disk_health(
            || async { self.disk.update_metadata(volume, path, fi, opts).await },
            get_max_timeout_duration(),
        )
        .await
    }

    async fn read_version(
        &self,
        org_volume: &str,
        volume: &str,
        path: &str,
        version_id: &str,
        opts: &ReadOptions,
    ) -> Result<FileInfo> {
        self.track_disk_health(
            || async { self.disk.read_version(org_volume, volume, path, version_id, opts).await },
            get_max_timeout_duration(),
        )
        .await
    }

    async fn read_xl(&self, volume: &str, path: &str, read_data: bool) -> Result<RawFileInfo> {
        self.track_disk_health(|| async { self.disk.read_xl(volume, path, read_data).await }, get_max_timeout_duration())
            .await
    }

    async fn rename_data(
        &self,
        src_volume: &str,
        src_path: &str,
        fi: FileInfo,
        dst_volume: &str,
        dst_path: &str,
    ) -> Result<RenameDataResp> {
        self.track_disk_health(
            || async { self.disk.rename_data(src_volume, src_path, fi, dst_volume, dst_path).await },
            get_max_timeout_duration(),
        )
        .await
    }

    async fn list_dir(&self, origvolume: &str, volume: &str, dir_path: &str, count: i32) -> Result<Vec<String>> {
        self.track_disk_health_with_op(
            "list_dir",
            || async { self.disk.list_dir(origvolume, volume, dir_path, count).await },
            get_drive_list_dir_timeout(),
        )
        .await
    }

    async fn read_file(&self, volume: &str, path: &str) -> Result<crate::disk::FileReader> {
        self.track_disk_health(|| async { self.disk.read_file(volume, path).await }, get_max_timeout_duration())
            .await
    }

    async fn read_file_stream(&self, volume: &str, path: &str, offset: usize, length: usize) -> Result<crate::disk::FileReader> {
        self.track_disk_health(
            || async { self.disk.read_file_stream(volume, path, offset, length).await },
            get_max_timeout_duration(),
        )
        .await
    }

    async fn read_file_zero_copy(&self, volume: &str, path: &str, offset: usize, length: usize) -> Result<bytes::Bytes> {
        self.track_disk_health(
            || async { self.disk.read_file_zero_copy(volume, path, offset, length).await },
            get_max_timeout_duration(),
        )
        .await
    }

    async fn append_file(&self, volume: &str, path: &str) -> Result<crate::disk::FileWriter> {
        self.track_disk_health(|| async { self.disk.append_file(volume, path).await }, Duration::ZERO)
            .await
    }

    async fn create_file(&self, origvolume: &str, volume: &str, path: &str, file_size: i64) -> Result<crate::disk::FileWriter> {
        self.track_disk_health(
            || async { self.disk.create_file(origvolume, volume, path, file_size).await },
            Duration::ZERO,
        )
        .await
    }

    async fn rename_file(&self, src_volume: &str, src_path: &str, dst_volume: &str, dst_path: &str) -> Result<()> {
        self.track_disk_health(
            || async { self.disk.rename_file(src_volume, src_path, dst_volume, dst_path).await },
            get_max_timeout_duration(),
        )
        .await
    }

    async fn rename_part(&self, src_volume: &str, src_path: &str, dst_volume: &str, dst_path: &str, meta: Bytes) -> Result<()> {
        self.track_disk_health(
            || async { self.disk.rename_part(src_volume, src_path, dst_volume, dst_path, meta).await },
            get_max_timeout_duration(),
        )
        .await
    }

    async fn delete(&self, volume: &str, path: &str, opt: DeleteOptions) -> Result<()> {
        self.track_disk_health(|| async { self.disk.delete(volume, path, opt).await }, get_max_timeout_duration())
            .await
    }

    async fn verify_file(&self, volume: &str, path: &str, fi: &FileInfo) -> Result<CheckPartsResp> {
        self.track_disk_health(|| async { self.disk.verify_file(volume, path, fi).await }, Duration::ZERO)
            .await
    }

    async fn check_parts(&self, volume: &str, path: &str, fi: &FileInfo) -> Result<CheckPartsResp> {
        self.track_disk_health(|| async { self.disk.check_parts(volume, path, fi).await }, Duration::ZERO)
            .await
    }

    async fn read_parts(&self, bucket: &str, paths: &[String]) -> Result<Vec<ObjectPartInfo>> {
        self.track_disk_health(|| async { self.disk.read_parts(bucket, paths).await }, Duration::ZERO)
            .await
    }

    async fn read_multiple(&self, req: ReadMultipleReq) -> Result<Vec<ReadMultipleResp>> {
        self.track_disk_health(|| async { self.disk.read_multiple(req).await }, Duration::ZERO)
            .await
    }

    async fn write_all(&self, volume: &str, path: &str, data: Bytes) -> Result<()> {
        self.track_disk_health(|| async { self.disk.write_all(volume, path, data).await }, get_max_timeout_duration())
            .await
    }

    async fn read_all(&self, volume: &str, path: &str) -> Result<Bytes> {
        self.track_disk_health(|| async { self.disk.read_all(volume, path).await }, get_max_timeout_duration())
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::disk::endpoint::Endpoint;
    use crate::disk::health_state::RuntimeDriveHealthState;

    #[test]
    fn drive_metadata_timeout_uses_default_when_unset() {
        temp_env::with_var_unset(rustfs_config::ENV_DRIVE_METADATA_TIMEOUT_SECS, || {
            temp_env::with_var_unset(rustfs_config::ENV_DRIVE_MAX_TIMEOUT_DURATION, || {
                assert_eq!(
                    get_drive_metadata_timeout(),
                    Duration::from_secs(rustfs_config::DEFAULT_DRIVE_METADATA_TIMEOUT_SECS)
                );
            });
        });
    }

    #[test]
    fn drive_metadata_timeout_ignores_legacy_fallback_when_canonical_unset() {
        temp_env::with_var_unset(rustfs_config::ENV_DRIVE_METADATA_TIMEOUT_SECS, || {
            temp_env::with_var(rustfs_config::ENV_DRIVE_MAX_TIMEOUT_DURATION, Some("17"), || {
                assert_eq!(
                    get_drive_metadata_timeout(),
                    Duration::from_secs(rustfs_config::DEFAULT_DRIVE_METADATA_TIMEOUT_SECS)
                );
            });
        });
    }

    #[test]
    fn drive_metadata_timeout_prefers_canonical_over_legacy() {
        temp_env::with_var(rustfs_config::ENV_DRIVE_METADATA_TIMEOUT_SECS, Some("7"), || {
            temp_env::with_var(rustfs_config::ENV_DRIVE_MAX_TIMEOUT_DURATION, Some("17"), || {
                assert_eq!(get_drive_metadata_timeout(), Duration::from_secs(7));
            });
        });
    }

    #[test]
    fn drive_active_check_interval_uses_default_when_unset() {
        temp_env::with_var_unset(rustfs_config::ENV_DRIVE_ACTIVE_CHECK_INTERVAL_SECS, || {
            assert_eq!(
                get_drive_active_check_interval(),
                Duration::from_secs(rustfs_config::DEFAULT_DRIVE_ACTIVE_CHECK_INTERVAL_SECS)
            );
        });
    }

    #[test]
    fn drive_active_check_interval_reads_env_override() {
        temp_env::with_var(rustfs_config::ENV_DRIVE_ACTIVE_CHECK_INTERVAL_SECS, Some("3"), || {
            assert_eq!(get_drive_active_check_interval(), Duration::from_secs(3));
        });
    }

    #[test]
    fn drive_active_check_timeout_uses_default_when_unset() {
        temp_env::with_var_unset(rustfs_config::ENV_DRIVE_ACTIVE_CHECK_TIMEOUT_SECS, || {
            assert_eq!(
                get_drive_active_check_timeout(),
                Duration::from_secs(rustfs_config::DEFAULT_DRIVE_ACTIVE_CHECK_TIMEOUT_SECS)
            );
        });
    }

    #[test]
    fn drive_active_check_timeout_reads_env_override() {
        temp_env::with_var(rustfs_config::ENV_DRIVE_ACTIVE_CHECK_TIMEOUT_SECS, Some("1"), || {
            assert_eq!(get_drive_active_check_timeout(), Duration::from_secs(1));
        });
    }

    #[test]
    fn runtime_state_transitions_from_online_to_suspect_then_offline() {
        let endpoint = Endpoint::try_from("/tmp/runtime-state-disk").expect("endpoint should parse");
        let health = DiskHealthTracker::new();

        assert_eq!(health.runtime_state(), RuntimeDriveHealthState::Online);
        assert!(health.mark_failure(&endpoint, "timeout"));
        assert_eq!(health.runtime_state(), RuntimeDriveHealthState::Suspect);

        assert!(!health.mark_failure(&endpoint, "timeout"));
        assert_eq!(health.runtime_state(), RuntimeDriveHealthState::Offline);
        assert!(health.offline_duration().is_some());
    }

    #[test]
    fn runtime_state_transitions_back_online_after_recovery_threshold() {
        let endpoint = Endpoint::try_from("/tmp/runtime-state-recovery").expect("endpoint should parse");
        let health = DiskHealthTracker::new();

        health.mark_failure(&endpoint, "timeout");
        health.mark_failure(&endpoint, "timeout");
        assert_eq!(health.runtime_state(), RuntimeDriveHealthState::Offline);

        assert!(!health.mark_recovery_success(&endpoint, "probe"));
        assert_eq!(health.runtime_state(), RuntimeDriveHealthState::Returning);

        assert!(!health.mark_recovery_success(&endpoint, "probe"));
        assert_eq!(health.runtime_state(), RuntimeDriveHealthState::Returning);

        assert!(health.mark_recovery_success(&endpoint, "probe"));
        assert_eq!(health.runtime_state(), RuntimeDriveHealthState::Online);
        assert!(health.offline_duration().is_none());
    }
}
