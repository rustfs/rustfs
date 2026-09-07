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

//! Concurrency manager for coordinating concurrent GetObject and PutObject requests.

use super::io_schedule::{
    IoLoadLevel, IoLoadMetrics, IoPriority, IoPriorityQueue, IoPriorityQueueConfig, IoQueueStatus, IoSchedulerConfig, IoStrategy,
    get_advanced_buffer_size,
};
use super::request_guard::{GetObjectGuard, PutObjectGuard};
use crate::storage::storage_api::runtime_sources_consumer::runtime_sources;
use rustfs_concurrency::{
    AdmissionState, GetObjectQueueSnapshot, WorkloadAdmissionRegistrySnapshot, WorkloadAdmissionSnapshot,
    WorkloadAdmissionSnapshotProvider, WorkloadClass,
};
use rustfs_config::{KI_B, MI_B};
use rustfs_io_core::BytesPool;
use rustfs_io_core::io_profile::{AccessPattern, IoPatternDetector, StorageMedia, detect_storage_media};
use rustfs_io_metrics::bandwidth::{BandwidthMonitor, BandwidthSnapshot};
use rustfs_io_metrics::{MetricsCollector, PerformanceMetrics};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Duration;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tracing::debug;

const DERIVED_LARGE_PUT_ADMISSION_LIMIT_MAX: usize = 32;
// A queued multipart part holds a connection but no body, so the queue can be
// several times deeper than the permit pool. Sixteen uploads sending sixteen
// parts each through one node fits inside the derived depth of 32 * 16.
const DERIVED_MULTIPART_ADMISSION_MAX_PENDING_FACTOR: usize = 16;
// Framed S2 alone can retain one encoded and one decoded block of roughly
// 4 MiB each, while other codecs have their own larger windows. Four keeps
// useful request parallelism without scaling codec memory and CPU with clients.
const SNOWBALL_ARCHIVE_DECODER_LIMIT: usize = 4;
pub(crate) const SNOWBALL_MEMBER_COMMIT_LIMIT: usize = 32;
pub(crate) const SNOWBALL_STAGING_BYTES_LIMIT: usize = 4 * MI_B;

/// Global concurrency manager instance
pub(crate) static CONCURRENCY_MANAGER: LazyLock<ConcurrencyManager> = LazyLock::new(ConcurrencyManager::new);

#[derive(Clone)]
pub struct ConcurrencyManager {
    /// Semaphore to limit concurrent disk reads
    disk_read_semaphore: Arc<Semaphore>,
    /// Bounded overflow lane for GETs that time out waiting on the primary
    /// disk-read permit pool. Admitting from this lane instead of reading
    /// without any permit gives a hard upper bound on concurrent disk-active
    /// reads (`primary cap + degraded cap`); when it is also full a GET is
    /// rejected with `SlowDown` rather than proceeding unbounded.
    degraded_read_semaphore: Arc<Semaphore>,
    /// I/O load metrics for adaptive strategy calculation
    io_metrics: Arc<Mutex<IoLoadMetrics>>,
    /// I/O priority queue for request scheduling
    #[allow(dead_code, reason = "written but never read back (backlog#1823)")]
    priority_queue: Arc<IoPriorityQueue<()>>,
    /// Bytes pool for buffer allocation and reuse
    bytes_pool: Arc<BytesPool>,
    // Enhanced scheduler state
    /// I/O scheduler configuration (cached at initialization)
    scheduler_config: IoSchedulerConfig,
    /// Detected storage media type
    storage_media: StorageMedia,
    /// I/O pattern detector for sequential/random access tracking
    pattern_detector: Arc<Mutex<IoPatternDetector>>,
    /// Bandwidth monitor for adaptive I/O sizing
    bandwidth_monitor: Arc<Mutex<BandwidthMonitor>>,
    /// Metrics collector for I/O latency tracking (P50, P95, P99)
    metrics_collector: Arc<MetricsCollector>,
    /// Foreground write admission policy, resolved once at startup.
    foreground_write_admission_policy: ForegroundWriteAdmissionPolicy,
    /// Bounds active Snowball archive inspection and decoding across requests.
    snowball_archive_decoder_semaphore: Arc<Semaphore>,
    /// Snowball members are internal PUTs, so they use a separate global gate
    /// from preparation through the independently owned post-commit tail.
    snowball_member_commit_semaphore: Arc<Semaphore>,
    /// Bounds the owned member bodies and metadata retained between TAR parsing
    /// and storage commit across all extract requests.
    snowball_staging_bytes_semaphore: Arc<Semaphore>,
}

impl std::fmt::Debug for ConcurrencyManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use std::sync::atomic::Ordering;
        let io_metrics_info = if let Ok(metrics) = self.io_metrics.lock() {
            format!("avg_wait={:?}, observations={}", metrics.average_wait(), metrics.observation_count())
        } else {
            "locked".to_string()
        };
        let bandwidth_info = if let Ok(monitor) = self.bandwidth_monitor.lock() {
            format!("{:?}", monitor.snapshot())
        } else {
            "locked".to_string()
        };
        f.debug_struct("ConcurrencyManager")
            .field(
                "active_requests",
                &crate::storage::concurrency::io_schedule::ACTIVE_GET_REQUESTS.load(Ordering::Relaxed),
            )
            .field("disk_read_permits", &self.disk_read_semaphore.available_permits())
            .field("io_metrics", &io_metrics_info)
            .field("storage_media", &self.storage_media)
            .field("bandwidth", &bandwidth_info)
            .field("bytes_pool", &self.bytes_pool)
            .finish()
    }
}
/// Outcome of [`ConcurrencyManager::admit_disk_read`].
///
/// `Primary`/`Degraded` both carry an owned permit that must be held for the
/// full body transfer; `Rejected` means the hard concurrency cap was reached and
/// the caller must fail the GET with `SlowDown`/503 instead of reading without a
/// permit.
#[derive(Debug)]
pub enum DiskReadAdmission {
    /// Admitted from the primary disk-read permit pool.
    Primary(tokio::sync::OwnedSemaphorePermit),
    /// Admitted from the bounded degraded overflow lane after the primary pool
    /// stayed saturated past the configured wait.
    Degraded(tokio::sync::OwnedSemaphorePermit),
    /// Disk-read throttling is disabled (primary cap configured to `0`): the GET
    /// proceeds without an admission token. This is the only permit-less path
    /// and is an explicit operator opt-out, not a saturation bypass.
    Unbounded,
    /// Hard concurrency cap reached; the caller must reject with `SlowDown`.
    Rejected,
}

/// Outcome of foreground write request admission.
#[derive(Debug)]
pub enum ForegroundWriteAdmission {
    /// Foreground write admission is disabled; proceed on the legacy path.
    Disabled,
    /// Request is admitted and must hold the permit until the store write
    /// returns or the request fails before mutation.
    Admitted(tokio::sync::OwnedSemaphorePermit),
    /// The selected foreground write admission gate stayed full until the configured wait timeout.
    Rejected,
}

#[derive(Clone)]
struct ForegroundWriteAdmissionGate {
    semaphore: Arc<Semaphore>,
    limit: usize,
    wait_timeout: Duration,
    /// Requests currently waiting in the bounded multipart queue.
    pending: Arc<AtomicUsize>,
}

/// Reservation of one slot in the bounded multipart wait queue; released on
/// drop so a cancelled or timed-out waiter never leaks queue depth.
struct PendingSlot(Arc<AtomicUsize>);

impl PendingSlot {
    fn reserve(pending: &Arc<AtomicUsize>, max_pending: usize) -> Option<Self> {
        if pending.fetch_add(1, Ordering::AcqRel) >= max_pending {
            pending.fetch_sub(1, Ordering::AcqRel);
            return None;
        }
        Some(Self(pending.clone()))
    }
}

impl Drop for PendingSlot {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

impl ForegroundWriteAdmissionGate {
    fn new(limit: usize, wait_timeout: Duration) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(limit)),
            limit,
            wait_timeout,
            pending: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn active(&self) -> usize {
        self.limit.saturating_sub(self.semaphore.available_permits())
    }

    fn pending(&self) -> usize {
        self.pending.load(Ordering::Acquire)
    }

    /// Admit through the same permit pool as [`Self::admit`], but let the
    /// request wait in a bounded queue for `wait_timeout` instead of failing
    /// on the gate's own short wait. A full queue rejects immediately.
    async fn admit_queued(
        &self,
        wait_timeout: Duration,
        max_pending: usize,
    ) -> Result<ForegroundWriteAdmission, tokio::sync::AcquireError> {
        match self.semaphore.clone().try_acquire_owned() {
            Ok(permit) => return Ok(ForegroundWriteAdmission::Admitted(permit)),
            Err(tokio::sync::TryAcquireError::Closed) => return Ok(ForegroundWriteAdmission::Rejected),
            Err(tokio::sync::TryAcquireError::NoPermits) => {}
        }
        if wait_timeout.is_zero() {
            return Ok(ForegroundWriteAdmission::Rejected);
        }
        let Some(_slot) = PendingSlot::reserve(&self.pending, max_pending) else {
            return Ok(ForegroundWriteAdmission::Rejected);
        };
        match tokio::time::timeout(wait_timeout, self.semaphore.clone().acquire_owned()).await {
            Ok(permit) => Ok(ForegroundWriteAdmission::Admitted(permit?)),
            Err(_) => Ok(ForegroundWriteAdmission::Rejected),
        }
    }

    async fn admit(&self) -> Result<ForegroundWriteAdmission, tokio::sync::AcquireError> {
        if self.wait_timeout.is_zero() {
            return Ok(match self.semaphore.clone().try_acquire_owned() {
                Ok(permit) => ForegroundWriteAdmission::Admitted(permit),
                Err(tokio::sync::TryAcquireError::NoPermits) => ForegroundWriteAdmission::Rejected,
                Err(tokio::sync::TryAcquireError::Closed) => ForegroundWriteAdmission::Rejected,
            });
        }

        match tokio::time::timeout(self.wait_timeout, self.semaphore.clone().acquire_owned()).await {
            Ok(permit) => Ok(ForegroundWriteAdmission::Admitted(permit?)),
            Err(_) => Ok(ForegroundWriteAdmission::Rejected),
        }
    }
}

#[derive(Clone)]
enum ForegroundWriteAdmissionPolicy {
    /// Strict admission was explicitly enabled with limit `0`.
    Disabled,
    /// No hard PUT gate is configured; foreground write snapshots use the
    /// existing active request counter as a soft pressure signal.
    LegacyCounterOnly,
    /// Explicit all foreground write admission gate.
    Strict(ForegroundWriteAdmissionGate),
    /// Default foreground write admission gate for pressure-heavy writes.
    Large {
        gate: ForegroundWriteAdmissionGate,
        put_object_min_size_bytes: usize,
        multipart_part_min_size_bytes: usize,
        multipart_wait_timeout: Duration,
        multipart_max_pending: usize,
    },
}

#[derive(Clone, Copy)]
enum ForegroundWriteAdmissionKind {
    PutObject,
    MultipartPart,
}

impl ForegroundWriteAdmissionPolicy {
    fn from_env(max_disk_reads: usize) -> Self {
        let strict_enabled = rustfs_utils::get_env_bool(
            rustfs_config::ENV_PUT_FOREGROUND_ADMISSION_ENABLE,
            rustfs_config::DEFAULT_PUT_FOREGROUND_ADMISSION_ENABLE,
        );
        if strict_enabled {
            let strict_limit = rustfs_utils::get_env_usize(
                rustfs_config::ENV_PUT_FOREGROUND_ADMISSION_LIMIT,
                rustfs_config::DEFAULT_PUT_FOREGROUND_ADMISSION_LIMIT,
            );
            let strict_wait_timeout = Duration::from_millis(rustfs_utils::get_env_u64(
                rustfs_config::ENV_PUT_FOREGROUND_ADMISSION_WAIT_TIMEOUT_MS,
                rustfs_config::DEFAULT_PUT_FOREGROUND_ADMISSION_WAIT_TIMEOUT_MS,
            ));
            return if strict_limit == 0 {
                Self::Disabled
            } else {
                Self::Strict(ForegroundWriteAdmissionGate::new(strict_limit, strict_wait_timeout))
            };
        }

        let large_enabled = rustfs_utils::get_env_bool(
            rustfs_config::ENV_PUT_LARGE_FOREGROUND_ADMISSION_ENABLE,
            rustfs_config::DEFAULT_PUT_LARGE_FOREGROUND_ADMISSION_ENABLE,
        );
        if !large_enabled {
            return Self::LegacyCounterOnly;
        }

        let large_limit = derive_large_put_admission_limit(
            rustfs_utils::get_env_usize(
                rustfs_config::ENV_PUT_LARGE_FOREGROUND_ADMISSION_LIMIT,
                rustfs_config::DEFAULT_PUT_LARGE_FOREGROUND_ADMISSION_LIMIT,
            ),
            max_disk_reads,
        );
        let put_object_min_size_bytes = rustfs_utils::get_env_usize(
            rustfs_config::ENV_PUT_LARGE_FOREGROUND_ADMISSION_MIN_SIZE_BYTES,
            rustfs_config::DEFAULT_PUT_LARGE_FOREGROUND_ADMISSION_MIN_SIZE_BYTES,
        );
        let multipart_part_min_size_bytes = rustfs_utils::get_env_usize(
            rustfs_config::ENV_PUT_MULTIPART_FOREGROUND_ADMISSION_MIN_SIZE_BYTES,
            rustfs_config::DEFAULT_PUT_MULTIPART_FOREGROUND_ADMISSION_MIN_SIZE_BYTES,
        );
        let wait_timeout = Duration::from_millis(rustfs_utils::get_env_u64(
            rustfs_config::ENV_PUT_LARGE_FOREGROUND_ADMISSION_WAIT_TIMEOUT_MS,
            rustfs_config::DEFAULT_PUT_LARGE_FOREGROUND_ADMISSION_WAIT_TIMEOUT_MS,
        ));
        let multipart_wait_timeout = Duration::from_millis(rustfs_utils::get_env_u64(
            rustfs_config::ENV_PUT_MULTIPART_FOREGROUND_ADMISSION_WAIT_TIMEOUT_MS,
            rustfs_config::DEFAULT_PUT_MULTIPART_FOREGROUND_ADMISSION_WAIT_TIMEOUT_MS,
        ));
        let multipart_max_pending = derive_multipart_admission_max_pending(
            rustfs_utils::get_env_usize(
                rustfs_config::ENV_PUT_MULTIPART_FOREGROUND_ADMISSION_MAX_PENDING,
                rustfs_config::DEFAULT_PUT_MULTIPART_FOREGROUND_ADMISSION_MAX_PENDING,
            ),
            large_limit,
        );

        Self::Large {
            gate: ForegroundWriteAdmissionGate::new(large_limit, wait_timeout),
            put_object_min_size_bytes,
            multipart_part_min_size_bytes,
            multipart_wait_timeout,
            multipart_max_pending,
        }
    }

    #[cfg(test)]
    fn strict_for_test(enabled: bool, limit: usize, wait_timeout: Duration) -> Self {
        if enabled {
            if limit == 0 {
                Self::Disabled
            } else {
                Self::Strict(ForegroundWriteAdmissionGate::new(limit, wait_timeout))
            }
        } else {
            Self::LegacyCounterOnly
        }
    }

    #[cfg(test)]
    fn large_for_test(enabled: bool, limit: usize, min_size_bytes: usize, wait_timeout: Duration) -> Self {
        Self::large_with_multipart_queue_for_test(enabled, limit, min_size_bytes, wait_timeout, wait_timeout, 0)
    }

    #[cfg(test)]
    fn large_with_multipart_queue_for_test(
        enabled: bool,
        limit: usize,
        min_size_bytes: usize,
        wait_timeout: Duration,
        multipart_wait_timeout: Duration,
        multipart_max_pending: usize,
    ) -> Self {
        if enabled && limit > 0 {
            Self::Large {
                gate: ForegroundWriteAdmissionGate::new(limit, wait_timeout),
                put_object_min_size_bytes: min_size_bytes,
                multipart_part_min_size_bytes: 0,
                multipart_wait_timeout,
                multipart_max_pending: derive_multipart_admission_max_pending(multipart_max_pending, limit),
            }
        } else {
            Self::LegacyCounterOnly
        }
    }

    async fn admit(
        &self,
        kind: ForegroundWriteAdmissionKind,
        size: i64,
    ) -> Result<ForegroundWriteAdmission, tokio::sync::AcquireError> {
        match self {
            Self::Disabled | Self::LegacyCounterOnly => Ok(ForegroundWriteAdmission::Disabled),
            Self::Strict(gate) => gate.admit().await,
            Self::Large {
                gate,
                put_object_min_size_bytes,
                multipart_part_min_size_bytes,
                multipart_wait_timeout,
                multipart_max_pending,
            } => match kind {
                ForegroundWriteAdmissionKind::PutObject if should_gate_foreground_write(size, *put_object_min_size_bytes) => {
                    gate.admit().await
                }
                ForegroundWriteAdmissionKind::MultipartPart
                    if should_gate_foreground_write(size, *multipart_part_min_size_bytes) =>
                {
                    gate.admit_queued(*multipart_wait_timeout, *multipart_max_pending).await
                }
                _ => Ok(ForegroundWriteAdmission::Disabled),
            },
        }
    }

    fn snapshot(&self, legacy_limit: usize) -> WorkloadAdmissionSnapshot {
        match self {
            Self::Disabled => put_admission_snapshot(0, None, 0, None),
            Self::LegacyCounterOnly => put_admission_snapshot(PutObjectGuard::concurrent_count(), None, legacy_limit, None),
            Self::Strict(gate) => {
                put_admission_snapshot(gate.active(), None, gate.limit, Some("foreground write admission permits exhausted"))
            }
            Self::Large { gate, .. } => put_admission_snapshot(
                gate.active(),
                Some(gate.pending()),
                gate.limit,
                Some("large foreground write admission permits exhausted"),
            ),
        }
    }
}

fn put_admission_snapshot(
    active: usize,
    queued: Option<usize>,
    limit: usize,
    hard_gate_reason: Option<&'static str>,
) -> WorkloadAdmissionSnapshot {
    let state = if limit == 0 {
        AdmissionState::Disabled
    } else if active >= limit {
        AdmissionState::Saturated
    } else {
        AdmissionState::Open
    };

    let admission =
        WorkloadAdmissionSnapshot::new(WorkloadClass::ForegroundWrite, state).with_counts(Some(active), queued, Some(limit));

    match state {
        AdmissionState::Disabled => admission.with_reason("foreground write admission disabled"),
        AdmissionState::Saturated => {
            admission.with_reason(hard_gate_reason.unwrap_or("foreground write concurrency reached local pressure limit"))
        }
        _ => admission,
    }
}

fn derive_multipart_admission_max_pending(configured_max_pending: usize, limit: usize) -> usize {
    if configured_max_pending > 0 {
        return configured_max_pending;
    }
    limit.saturating_mul(DERIVED_MULTIPART_ADMISSION_MAX_PENDING_FACTOR)
}

fn derive_large_put_admission_limit(configured_limit: usize, max_disk_reads: usize) -> usize {
    if configured_limit > 0 {
        return configured_limit;
    }

    let scheduler_base = if max_disk_reads == 0 {
        rustfs_config::DEFAULT_OBJECT_MAX_CONCURRENT_DISK_READS
    } else {
        max_disk_reads
    };
    scheduler_base.div_ceil(2).clamp(1, DERIVED_LARGE_PUT_ADMISSION_LIMIT_MAX)
}

fn should_gate_foreground_write(size: i64, min_size_bytes: usize) -> bool {
    if min_size_bytes == 0 || size < 0 {
        return true;
    }

    usize::try_from(size).is_ok_and(|size| size >= min_size_bytes)
}

impl ConcurrencyManager {
    /// Create a new concurrency manager with default settings
    ///
    /// Reads configuration from environment variables:
    /// - `RUSTFS_OBJECT_MAX_CONCURRENT_DISK_READS`: Maximum concurrent disk reads (default: 64)
    pub fn new() -> Self {
        // Load scheduler configuration once at initialization
        let scheduler_config = IoSchedulerConfig::from_env();

        let max_disk_reads = scheduler_config.max_concurrent_reads;

        // Bounded degraded admission lane. A configured `0` mirrors the primary
        // cap, so the absolute hard cap on concurrent disk-active reads defaults
        // to twice the primary disk-read concurrency.
        let degraded_read_cap = {
            let configured = rustfs_utils::get_env_usize(
                rustfs_config::ENV_OBJECT_DISK_DEGRADED_READ_CAP,
                rustfs_config::DEFAULT_OBJECT_DISK_DEGRADED_READ_CAP,
            );
            if configured == 0 { max_disk_reads } else { configured }
        };

        // Detect storage media
        let storage_media =
            detect_storage_media(scheduler_config.storage_detection_enabled, &scheduler_config.storage_media_override);

        // Initialize I/O pattern detector
        let pattern_detector = Arc::new(Mutex::new(IoPatternDetector::new(
            scheduler_config.pattern_history_size,
            scheduler_config.sequential_step_tolerance_bytes,
        )));

        // Initialize bandwidth monitor
        let bandwidth_monitor = Arc::new(Mutex::new(BandwidthMonitor::new(
            scheduler_config.bandwidth_ema_beta,
            scheduler_config.bandwidth_low_threshold_bps,
            scheduler_config.bandwidth_high_threshold_bps,
        )));

        // Use global performance metrics instance for consistent metrics tracking
        // This allows AutoTuner and other components to access the same metrics data
        let performance_metrics = runtime_sources::current_performance_metrics();

        // Initialize metrics collector for I/O latency tracking
        // Keep 1000 samples for P95/P99 calculation
        let metrics_collector = Arc::new(MetricsCollector::new(performance_metrics, 1000));
        let foreground_write_admission_policy = ForegroundWriteAdmissionPolicy::from_env(max_disk_reads);

        // Build queue config directly from scheduler config.
        let queue_config = IoPriorityQueueConfig::from_scheduler_config(&scheduler_config);

        Self {
            disk_read_semaphore: Arc::new(Semaphore::new(max_disk_reads)),
            degraded_read_semaphore: Arc::new(Semaphore::new(degraded_read_cap)),
            io_metrics: Arc::new(Mutex::new(IoLoadMetrics::new(scheduler_config.load_sample_window))),
            priority_queue: Arc::new(IoPriorityQueue::new(queue_config)),
            bytes_pool: Arc::new(BytesPool::new_tiered()),
            scheduler_config,
            storage_media,
            pattern_detector,
            bandwidth_monitor,
            metrics_collector,
            foreground_write_admission_policy,
            snowball_archive_decoder_semaphore: Arc::new(Semaphore::new(SNOWBALL_ARCHIVE_DECODER_LIMIT)),
            snowball_member_commit_semaphore: Arc::new(Semaphore::new(SNOWBALL_MEMBER_COMMIT_LIMIT)),
            snowball_staging_bytes_semaphore: Arc::new(Semaphore::new(SNOWBALL_STAGING_BYTES_LIMIT)),
        }
    }

    /// Build a manager with explicit disk-read admission caps for tests.
    ///
    /// Overrides only the primary/degraded semaphores and the snapshot cap so
    /// admission behavior can be exercised deterministically under a paused
    /// virtual clock, without depending on process-wide environment variables.
    #[cfg(test)]
    pub(crate) fn with_disk_read_caps_for_test(primary: usize, degraded: usize) -> Self {
        let mut manager = Self::new();
        manager.disk_read_semaphore = Arc::new(Semaphore::new(primary));
        manager.degraded_read_semaphore = Arc::new(Semaphore::new(degraded));
        manager.scheduler_config.max_concurrent_reads = primary;
        manager
    }

    #[cfg(test)]
    pub(crate) fn close_disk_read_admission_for_test(&self) {
        self.disk_read_semaphore.close();
        self.degraded_read_semaphore.close();
    }

    #[cfg(test)]
    pub(crate) fn with_put_admission_for_test(enabled: bool, limit: usize, wait_timeout: Duration) -> Self {
        let mut manager = Self::new();
        manager.foreground_write_admission_policy = ForegroundWriteAdmissionPolicy::strict_for_test(enabled, limit, wait_timeout);
        manager
    }

    #[cfg(test)]
    pub(crate) fn with_multipart_admission_queue_for_test(
        limit: usize,
        multipart_wait_timeout: Duration,
        multipart_max_pending: usize,
    ) -> Self {
        let mut manager = Self::new();
        manager.foreground_write_admission_policy = ForegroundWriteAdmissionPolicy::large_with_multipart_queue_for_test(
            true,
            limit,
            rustfs_config::DEFAULT_PUT_LARGE_FOREGROUND_ADMISSION_MIN_SIZE_BYTES,
            Duration::ZERO,
            multipart_wait_timeout,
            multipart_max_pending,
        );
        manager
    }

    #[cfg(test)]
    pub(crate) fn with_large_put_admission_for_test(
        enabled: bool,
        limit: usize,
        min_size_bytes: usize,
        wait_timeout: Duration,
    ) -> Self {
        let mut manager = Self::new();
        manager.foreground_write_admission_policy =
            ForegroundWriteAdmissionPolicy::large_for_test(enabled, limit, min_size_bytes, wait_timeout);
        manager
    }

    /// Track a GetObject request
    pub fn track_request() -> GetObjectGuard {
        GetObjectGuard::new()
    }

    pub fn track_put_request() -> PutObjectGuard {
        PutObjectGuard::new()
    }

    /// Get the bytes pool for buffer allocation
    ///
    /// Returns a reference to the BytesPool which can be used to acquire
    /// reusable buffers for I/O operations, reducing allocation overhead.
    ///
    /// # Returns
    ///
    /// Arc-wrapped BytesPool instance
    pub fn bytes_pool(&self) -> Arc<BytesPool> {
        self.bytes_pool.clone()
    }

    /// Acquire a permit to perform a disk read operation
    ///
    /// This ensures we don't overwhelm the disk subsystem with too many
    /// concurrent reads, which can cause performance degradation.
    pub async fn acquire_disk_read_permit(&self) -> Result<tokio::sync::SemaphorePermit<'_>, tokio::sync::AcquireError> {
        self.disk_read_semaphore.acquire().await
    }

    /// Acquire an owned permit to perform a disk read operation.
    ///
    /// Use this when the permit must outlive the borrow of the manager, such as
    /// response body streams that continue after the S3 handler returns.
    pub async fn acquire_owned_disk_read_permit(&self) -> Result<tokio::sync::OwnedSemaphorePermit, tokio::sync::AcquireError> {
        self.disk_read_semaphore.clone().acquire_owned().await
    }

    /// Admit a GET disk read under a hard concurrency cap.
    ///
    /// The permit is held for the whole response body transfer, so this is the
    /// single admission boundary that bounds concurrent disk-active reads:
    ///
    /// - `primary_wait == 0` waits on the primary permit pool indefinitely and
    ///   always yields a [`DiskReadAdmission::Primary`] permit (no degrade, no
    ///   reject); this preserves the "wait forever" opt-out.
    /// - Otherwise it waits up to `primary_wait` for a primary permit. On
    ///   timeout it takes one permit from the bounded degraded overflow lane
    ///   without blocking. If that lane is also full the request is
    ///   [`DiskReadAdmission::Rejected`] — it must surface as `SlowDown`/503
    ///   rather than reading without any admission token.
    ///
    /// Total GETs holding a permit therefore never exceed
    /// `primary_cap + degraded_cap`.
    pub async fn admit_disk_read(&self, primary_wait: Duration) -> Result<DiskReadAdmission, tokio::sync::AcquireError> {
        // Primary cap of 0 means disk-read throttling is disabled (matches the
        // `AdmissionState::Disabled` snapshot semantics). Serve without an
        // admission token rather than rejecting every GET; this preserves the
        // pre-existing "permits disabled" behavior and is the only permit-less
        // path.
        if self.scheduler_config.max_concurrent_reads == 0 {
            return Ok(DiskReadAdmission::Unbounded);
        }

        if primary_wait.is_zero() {
            let permit = self.disk_read_semaphore.clone().acquire_owned().await?;
            return Ok(DiskReadAdmission::Primary(permit));
        }

        match tokio::time::timeout(primary_wait, self.disk_read_semaphore.clone().acquire_owned()).await {
            Ok(permit) => Ok(DiskReadAdmission::Primary(permit?)),
            Err(_) => {
                // Primary pool saturated within the wait window. Do not read
                // without a permit; fall through to the bounded degraded lane,
                // and reject once the hard cap is reached.
                match self.degraded_read_semaphore.clone().try_acquire_owned() {
                    Ok(permit) => Ok(DiskReadAdmission::Degraded(permit)),
                    // NoPermits => hard cap reached; Closed never happens for a
                    // semaphore we never close. Either way, reject rather than
                    // bypass admission.
                    Err(_) => Ok(DiskReadAdmission::Rejected),
                }
            }
        }
    }

    /// Admit a foreground PutObject request under the configured write gate.
    ///
    /// The strict experimental gate applies to every PUT only when explicitly
    /// enabled. Otherwise the default-on large-object gate protects sustained
    /// erasure/RPC pressure while keeping small PUTs on the legacy path.
    pub async fn admit_put_object(&self, size: i64) -> Result<ForegroundWriteAdmission, tokio::sync::AcquireError> {
        self.foreground_write_admission_policy
            .admit(ForegroundWriteAdmissionKind::PutObject, size)
            .await
    }

    /// Admit a Snowball member through the foreground PUT policy using the
    /// member's logical size. The outer archive has a separate preflight, while
    /// every member shares the ordinary PUT gate and its wait/rejection policy.
    pub(crate) async fn admit_snowball_foreground_write(
        &self,
        member_size: i64,
    ) -> Result<ForegroundWriteAdmission, tokio::sync::AcquireError> {
        self.foreground_write_admission_policy
            .admit(ForegroundWriteAdmissionKind::PutObject, member_size)
            .await
    }

    /// Try to acquire one global Snowball archive decoder slot.
    pub(crate) fn try_acquire_snowball_archive_decoder(&self) -> Option<OwnedSemaphorePermit> {
        self.snowball_archive_decoder_semaphore.clone().try_acquire_owned().ok()
    }

    /// Acquire one global Snowball member lifecycle slot.
    pub(crate) async fn acquire_snowball_member_commit(&self) -> Result<OwnedSemaphorePermit, tokio::sync::AcquireError> {
        self.snowball_member_commit_semaphore.clone().acquire_owned().await
    }

    /// Try to acquire one global Snowball member lifecycle slot.
    pub(crate) fn try_acquire_snowball_member_commit(&self) -> Option<OwnedSemaphorePermit> {
        self.snowball_member_commit_semaphore.clone().try_acquire_owned().ok()
    }

    /// Try to reserve prepared-member bytes without waiting.
    ///
    /// A producer holding a non-empty micro-batch must use this method and
    /// flush before waiting, otherwise several archives can each retain part of
    /// the global budget while waiting forever for the remainder.
    pub(crate) fn try_acquire_snowball_staging_bytes(&self, bytes: u32) -> Option<OwnedSemaphorePermit> {
        self.snowball_staging_bytes_semaphore
            .clone()
            .try_acquire_many_owned(bytes)
            .ok()
    }

    /// Admit a multipart UploadPart request under the configured write gate.
    ///
    /// Multipart workloads can saturate memory and internode write streams with
    /// many moderate-sized parts, so they use an operation-specific threshold
    /// while sharing the same foreground write permit pool.
    pub async fn admit_multipart_part(&self, size: i64) -> Result<ForegroundWriteAdmission, tokio::sync::AcquireError> {
        self.foreground_write_admission_policy
            .admit(ForegroundWriteAdmissionKind::MultipartPart, size)
            .await
    }

    // ============================================
    // Adaptive I/O Strategy Methods
    // ============================================

    /// Record a disk permit wait observation for load tracking.
    ///
    /// This method updates the rolling metrics used to calculate adaptive I/O
    /// strategies. Should be called after each disk permit acquisition.
    ///
    /// # Arguments
    ///
    /// * `wait_duration` - Time spent waiting for the disk read permit
    pub fn record_permit_wait(&self, wait_duration: Duration) {
        if let Ok(mut metrics) = self.io_metrics.lock() {
            metrics.record(wait_duration);
        }
    }

    // ============================================
    // Metrics Collection Methods
    // ============================================

    /// Record a disk I/O operation for latency tracking.
    ///
    /// This method delegates to MetricsCollector which:
    /// 1. Updates atomic counters in PerformanceMetrics
    /// 2. Records latency for P95/P99 calculation
    /// 3. Reports to metrics crate (which exports to OTEL)
    ///
    /// # Arguments
    ///
    /// * `bytes` - Number of bytes transferred
    /// * `duration` - Duration of the I/O operation
    /// * `is_read` - true for read operations, false for writes
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let manager = get_concurrency_manager();
    /// let start = Instant::now();
    /// // ... perform disk I/O ...
    /// let duration = start.elapsed();
    /// manager.record_disk_operation(1024 * 1024, duration, true).await;
    /// ```
    pub async fn record_disk_operation(&self, bytes: u64, duration: Duration, is_read: bool) {
        self.metrics_collector.record_io_operation(bytes, duration, is_read).await;
    }

    /// Get a reference to the metrics collector for external use.
    ///
    /// # Returns
    ///
    /// Arc-wrapped MetricsCollector instance
    pub fn metrics_collector(&self) -> &Arc<MetricsCollector> {
        &self.metrics_collector
    }

    /// Get the global performance metrics instance.
    ///
    /// This provides access to the shared PerformanceMetrics that is used
    /// across all components, including AutoTuner.
    ///
    /// # Returns
    ///
    /// Arc-wrapped PerformanceMetrics instance
    pub fn performance_metrics(&self) -> Arc<PerformanceMetrics> {
        runtime_sources::current_performance_metrics()
    }

    /// Calculate an adaptive I/O strategy based on disk permit wait time.
    ///
    /// This method analyzes the permit wait duration to determine the current
    /// I/O load level and returns optimized parameters for the read operation.
    ///
    /// # Arguments
    ///
    /// * `permit_wait_duration` - Time spent waiting for disk read permit
    /// * `base_buffer_size` - Base buffer size from workload configuration
    ///
    /// # Returns
    ///
    /// An `IoStrategy` containing optimized I/O parameters.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let permit_wait_start = Instant::now();
    /// let _permit = manager.acquire_disk_read_permit().await;
    /// let permit_wait_duration = permit_wait_start.elapsed();
    ///
    /// let strategy = manager.calculate_io_strategy(permit_wait_duration, 256 * 1024);
    /// let optimal_buffer = strategy.buffer_size;
    /// ```
    pub fn calculate_io_strategy(&self, permit_wait_duration: Duration, base_buffer_size: usize) -> IoStrategy {
        // Record the observation for future smoothing
        self.record_permit_wait(permit_wait_duration);

        // Calculate strategy from the current wait duration
        IoStrategy::from_wait_duration(permit_wait_duration, base_buffer_size)
    }

    /// Calculate I/O strategy with enhanced multi-factor context.
    ///
    /// This method integrates storage media, access patterns, bandwidth observations,
    /// and concurrent request count to provide a more sophisticated I/O strategy.
    ///
    /// # Arguments
    ///
    /// * `file_size` - Size of the file/object being read (-1 if unknown)
    /// * `base_buffer_size` - Base buffer size from workload configuration
    /// * `permit_wait_duration` - Time spent waiting for disk read permit
    /// * `is_sequential_hint` - Whether the access pattern is known to be sequential
    ///
    /// # Returns
    ///
    /// An `IoStrategy` with optimized parameters based on all available factors.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let strategy = manager.calculate_io_strategy_with_context(
    ///     file_size,
    ///     256 * 1024,
    ///     permit_wait_duration,
    ///     false,
    /// );
    /// let optimal_buffer = strategy.buffer_size;
    /// let enable_readahead = strategy.enable_readahead;
    /// ```
    pub fn calculate_io_strategy_with_context(
        &self,
        file_size: i64,
        base_buffer_size: usize,
        permit_wait_duration: Duration,
        is_sequential_hint: bool,
    ) -> IoStrategy {
        use crate::storage::concurrency::io_schedule::IoSchedulingContext;

        // Record the observation for future smoothing
        self.record_permit_wait(permit_wait_duration);

        // Get current access pattern
        let access_pattern = if let Ok(detector) = self.pattern_detector.lock() {
            detector.current_pattern()
        } else {
            AccessPattern::Unknown
        };

        // Get current bandwidth snapshot
        let observed_bandwidth_bps = if let Ok(monitor) = self.bandwidth_monitor.lock() {
            let snapshot = monitor.snapshot();
            if snapshot.tier == rustfs_io_metrics::bandwidth::BandwidthTier::Unknown {
                None
            } else {
                Some(snapshot.bytes_per_second)
            }
        } else {
            None
        };

        // Get concurrent request count
        let concurrent_requests =
            crate::storage::concurrency::io_schedule::ACTIVE_GET_REQUESTS.load(std::sync::atomic::Ordering::Relaxed);

        // Build scheduling context
        let context = IoSchedulingContext {
            file_size,
            base_buffer_size,
            permit_wait_duration,
            is_sequential_hint,
            access_pattern,
            storage_media: self.storage_media,
            observed_bandwidth_bps,
            concurrent_requests,
        };

        // Calculate strategy using multi-factor approach
        IoStrategy::from_context_with_config(&context, &self.scheduler_config)
    }

    /// Get the smoothed I/O load level based on recent observations.
    ///
    /// This uses the rolling window of permit wait times to provide a more
    /// stable estimate of the current load level, reducing oscillation from
    /// transient spikes.
    ///
    /// # Returns
    ///
    /// The smoothed `IoLoadLevel` based on average recent wait times.
    pub fn smoothed_load_level(&self) -> IoLoadLevel {
        if let Ok(metrics) = self.io_metrics.lock() {
            metrics.smoothed_load_level()
        } else {
            IoLoadLevel::Medium // Default to medium if lock fails
        }
    }

    /// Get I/O load statistics for monitoring.
    ///
    /// Returns statistics about recent disk permit wait times for
    /// monitoring dashboards and capacity planning.
    ///
    /// # Returns
    ///
    /// A tuple of (average_wait, p95_wait, max_wait, observation_count)
    pub fn io_load_stats(&self) -> (Duration, Duration, Duration, u64) {
        if let Ok(metrics) = self.io_metrics.lock() {
            (
                metrics.average_wait(),
                metrics.p95_wait(),
                metrics.max_wait(),
                metrics.observation_count(),
            )
        } else {
            (Duration::ZERO, Duration::ZERO, Duration::ZERO, 0)
        }
    }

    /// Get the recommended buffer size based on current I/O load.
    ///
    /// This is a convenience method that combines load level detection with
    /// buffer size calculation. Uses the smoothed load level for stability.
    ///
    /// # Arguments
    ///
    /// * `base_buffer_size` - Base buffer size from workload configuration
    ///
    /// # Returns
    ///
    /// Recommended buffer size in bytes.
    pub fn adaptive_buffer_size(&self, base_buffer_size: usize) -> usize {
        let load_level = self.smoothed_load_level();
        let multiplier = match load_level {
            IoLoadLevel::Low => 1.0,
            IoLoadLevel::Medium => 0.75,
            IoLoadLevel::High => 0.5,
            IoLoadLevel::Critical => 0.4,
        };

        let buffer_size = ((base_buffer_size as f64) * multiplier) as usize;
        buffer_size.clamp(32 * KI_B, MI_B)
    }

    // ============================================
    // Enhanced I/O Scheduling Methods
    // ============================================

    /// Record an I/O access for pattern detection.
    ///
    /// This updates the pattern detector with the offset and size of an access,
    /// allowing it to distinguish between sequential and random access patterns.
    ///
    /// # Arguments
    ///
    /// * `offset` - File offset being accessed
    /// * `len` - Length of the access
    pub fn record_access(&self, offset: u64, len: u64) {
        if let Ok(mut detector) = self.pattern_detector.lock() {
            detector.record(offset, len);
        }
    }

    /// Get the current access pattern.
    ///
    /// Returns the detected access pattern (Sequential, Random, Mixed, or Unknown).
    pub fn current_access_pattern(&self) -> AccessPattern {
        if let Ok(detector) = self.pattern_detector.lock() {
            detector.current_pattern()
        } else {
            AccessPattern::Unknown
        }
    }

    /// Record a data transfer for bandwidth monitoring.
    ///
    /// This updates the bandwidth monitor with the bytes transferred and duration,
    /// allowing it to maintain an EMA (Exponential Moving Average) of the observed bandwidth.
    ///
    /// # Arguments
    ///
    /// * `bytes` - Number of bytes transferred
    /// * `duration` - Duration of the transfer
    pub fn record_transfer(&self, bytes: u64, duration: Duration) {
        if let Ok(mut monitor) = self.bandwidth_monitor.lock() {
            monitor.record_transfer(bytes, duration);
        }
    }

    /// Get the current bandwidth snapshot.
    ///
    /// Returns a snapshot of the current bandwidth including bytes per second and tier.
    pub fn current_bandwidth_snapshot(&self) -> BandwidthSnapshot {
        if let Ok(monitor) = self.bandwidth_monitor.lock() {
            monitor.snapshot()
        } else {
            BandwidthSnapshot {
                bytes_per_second: 0,
                tier: rustfs_io_metrics::bandwidth::BandwidthTier::Unknown,
            }
        }
    }

    /// Get the detected storage media type.
    pub fn storage_media(&self) -> StorageMedia {
        self.storage_media
    }

    /// Get the scheduler configuration.
    pub fn scheduler_config(&self) -> &IoSchedulerConfig {
        &self.scheduler_config
    }

    /// Get optimized buffer size for a request
    ///
    /// This wraps the advanced buffer sizing logic and makes it accessible
    /// through the concurrency manager interface.
    pub fn buffer_size(&self, file_size: i64, base: usize, sequential: bool) -> usize {
        get_advanced_buffer_size(file_size, base, sequential)
    }

    // ============================================
    // Priority-Based I/O Scheduling Methods
    // ============================================

    /// Get I/O priority for a request based on its size.
    ///
    /// This enables priority-based scheduling where small requests
    /// are processed before large requests to prevent starvation.
    ///
    /// # Arguments
    ///
    /// * `request_size` - Size of the request in bytes (-1 if unknown)
    ///
    /// # Returns
    ///
    /// Priority level (High, Normal, or Low)
    pub fn get_io_priority(&self, request_size: i64) -> IoPriority {
        if request_size < 0 {
            // Unknown size, use normal priority
            IoPriority::Normal
        } else {
            // Use cached scheduler config thresholds
            IoPriority::from_size_with_thresholds(
                request_size,
                self.scheduler_config.high_priority_size_threshold,
                self.scheduler_config.low_priority_size_threshold,
            )
        }
    }

    /// Check if priority scheduling is enabled.
    pub fn is_priority_scheduling_enabled(&self) -> bool {
        self.scheduler_config.enable_priority
    }

    /// Get current I/O queue status for monitoring.
    ///
    /// Returns information about permit usage and waiting requests.
    pub fn io_queue_status(&self) -> IoQueueStatus {
        let snapshot = self.get_object_queue_snapshot();

        IoQueueStatus {
            total_permits: snapshot.total_permits,
            permits_in_use: snapshot.permits_in_use,
            high_priority_waiting: 0, // Would need additional tracking
            normal_priority_waiting: 0,
            low_priority_waiting: 0,
            high_priority_processed: 0,
            normal_priority_processed: 0,
            low_priority_processed: 0,
            starvation_events: 0,
        }
    }

    /// Get a read-only snapshot of local GetObject disk-read admission.
    pub fn get_object_queue_snapshot(&self) -> GetObjectQueueSnapshot {
        GetObjectQueueSnapshot::from_available_permits(
            self.scheduler_config.max_concurrent_reads,
            self.disk_read_semaphore.available_permits(),
        )
    }

    /// Get a read-only workload admission snapshot for foreground reads.
    pub fn get_object_admission_snapshot(&self) -> WorkloadAdmissionSnapshot {
        let snapshot = self.get_object_queue_snapshot();
        let state = if snapshot.total_permits == 0 {
            AdmissionState::Disabled
        } else if snapshot.permits_available() == 0 {
            AdmissionState::Saturated
        } else {
            AdmissionState::Open
        };

        let admission = WorkloadAdmissionSnapshot::new(WorkloadClass::ForegroundRead, state).with_counts(
            Some(snapshot.permits_in_use),
            None,
            Some(snapshot.total_permits),
        );

        match state {
            AdmissionState::Disabled => admission.with_reason("disk read permits disabled"),
            AdmissionState::Saturated => admission.with_reason("all disk read permits are in use"),
            _ => admission,
        }
    }

    /// Get a read-only workload admission snapshot for foreground writes.
    pub fn put_object_admission_snapshot(&self) -> WorkloadAdmissionSnapshot {
        self.foreground_write_admission_policy
            .snapshot(self.scheduler_config.max_concurrent_reads)
    }

    /// Get a read-only workload admission registry snapshot for local storage concurrency.
    pub fn workload_admission_registry_snapshot(&self) -> WorkloadAdmissionRegistrySnapshot {
        let entries = WorkloadClass::REQUIRED
            .iter()
            .copied()
            .map(|class| match class {
                WorkloadClass::ForegroundRead => self.get_object_admission_snapshot(),
                WorkloadClass::ForegroundWrite => self.put_object_admission_snapshot(),
                class => WorkloadAdmissionSnapshot::new(class, AdmissionState::Unknown)
                    .with_reason("not exposed by storage concurrency manager"),
            })
            .collect();

        WorkloadAdmissionRegistrySnapshot::new(entries)
    }

    /// Acquire a disk read permit with priority awareness.
    ///
    /// When priority scheduling is enabled, this method logs the priority
    /// for observability. The actual acquisition uses the same semaphore
    /// but priority information is used for monitoring.
    ///
    /// # Arguments
    ///
    /// * `priority` - Priority level for this request
    ///
    /// # Returns
    ///
    /// Semaphore permit on success, error on failure
    pub async fn acquire_priority_permit(
        &self,
        priority: IoPriority,
    ) -> Result<tokio::sync::SemaphorePermit<'_>, tokio::sync::AcquireError> {
        rustfs_io_metrics::record_io_priority_assignment(priority.as_str());

        debug!(
            priority = %priority,
            available_permits = self.disk_read_semaphore.available_permits(),
            "Acquiring disk read permit"
        );

        self.disk_read_semaphore.acquire().await
    }

    /// Get the global concurrency manager instance.
    pub fn global() -> &'static Self {
        &CONCURRENCY_MANAGER
    }
}

impl WorkloadAdmissionSnapshotProvider for ConcurrencyManager {
    fn workload_admission_snapshot(&self) -> WorkloadAdmissionRegistrySnapshot {
        self.workload_admission_registry_snapshot()
    }
}

impl Default for ConcurrencyManager {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================
// Integration Tests for ConcurrencyManager
// ============================================

#[cfg(test)]
#[allow(unused_imports)]
mod integration_tests {
    use super::super::io_schedule::{IoLoadLevel, IoPriority};
    use super::super::request_guard::GetObjectGuard;
    use super::{
        ConcurrencyManager, ForegroundWriteAdmission, SNOWBALL_ARCHIVE_DECODER_LIMIT, SNOWBALL_MEMBER_COMMIT_LIMIT,
        SNOWBALL_STAGING_BYTES_LIMIT, derive_large_put_admission_limit, derive_multipart_admission_max_pending,
    };
    use crate::storage::storage_api::concurrency_consumer::PutObjectGuard;
    use rustfs_concurrency::{AdmissionState, WorkloadAdmissionSnapshotProvider, WorkloadClass};
    use rustfs_io_core::io_profile::{AccessPattern, StorageMedia};
    use serial_test::serial;
    use std::time::Duration;

    #[test]
    fn test_snowball_gates_are_global_bounded_and_reusable() {
        let manager = ConcurrencyManager::new();
        let clone = manager.clone();

        let decoder_permits = manager
            .snowball_archive_decoder_semaphore
            .clone()
            .try_acquire_many_owned(
                u32::try_from(SNOWBALL_ARCHIVE_DECODER_LIMIT).expect("Snowball decoder limit must fit into u32"),
            )
            .expect("the exact Snowball decoder limit must be available");
        assert!(
            clone.try_acquire_snowball_archive_decoder().is_none(),
            "a cloned manager must share the global decoder gate"
        );
        drop(decoder_permits);
        assert!(clone.try_acquire_snowball_archive_decoder().is_some());

        let commit_permits = manager
            .snowball_member_commit_semaphore
            .clone()
            .try_acquire_many_owned(u32::try_from(SNOWBALL_MEMBER_COMMIT_LIMIT).expect("Snowball commit limit must fit into u32"))
            .expect("the exact Snowball commit limit must be available");
        assert!(
            clone.snowball_member_commit_semaphore.clone().try_acquire_owned().is_err(),
            "a cloned manager must share the global commit gate"
        );
        drop(commit_permits);
        assert!(clone.snowball_member_commit_semaphore.clone().try_acquire_owned().is_ok());

        let staging_bytes = u32::try_from(SNOWBALL_STAGING_BYTES_LIMIT).expect("Snowball staging limit must fit into u32");
        let staging_permit = manager
            .try_acquire_snowball_staging_bytes(staging_bytes)
            .expect("the exact Snowball staging budget must be available");
        assert!(
            clone.try_acquire_snowball_staging_bytes(1).is_none(),
            "a cloned manager must share the global staging budget"
        );
        drop(staging_permit);
        assert!(clone.try_acquire_snowball_staging_bytes(1).is_some());
    }

    #[tokio::test]
    async fn test_snowball_members_share_the_strict_foreground_put_gate() {
        let manager = ConcurrencyManager::with_put_admission_for_test(true, 2, Duration::ZERO);
        let outer = match manager
            .admit_put_object(1)
            .await
            .expect("strict outer admission must remain open")
        {
            ForegroundWriteAdmission::Admitted(permit) => permit,
            outcome => panic!("strict outer admission must return a permit: {outcome:?}"),
        };
        let member = match manager
            .admit_snowball_foreground_write(1)
            .await
            .expect("strict member admission must remain open")
        {
            ForegroundWriteAdmission::Admitted(permit) => permit,
            outcome => panic!("strict member admission must return a permit: {outcome:?}"),
        };
        assert!(
            matches!(
                manager
                    .admit_snowball_foreground_write(1)
                    .await
                    .expect("strict member admission must remain open"),
                ForegroundWriteAdmission::Rejected
            ),
            "a saturated Snowball member admission must preserve the zero-wait rejection policy"
        );
        assert!(
            matches!(
                manager.admit_put_object(1).await.expect("strict gate must remain usable"),
                ForegroundWriteAdmission::Rejected
            ),
            "outer PUTs and Snowball members must exhaust the same strict gate"
        );

        drop(outer);
        let replacement = match manager
            .admit_snowball_foreground_write(1)
            .await
            .expect("released strict capacity must be reusable")
        {
            ForegroundWriteAdmission::Admitted(permit) => permit,
            outcome => panic!("strict replacement admission must return a permit: {outcome:?}"),
        };
        drop((member, replacement));
    }

    #[tokio::test]
    async fn test_snowball_members_use_their_size_for_the_large_foreground_put_gate() {
        let min_size = 16 * 1024 * 1024;
        let manager = ConcurrencyManager::with_large_put_admission_for_test(true, 1, min_size, Duration::ZERO);

        assert!(matches!(
            manager
                .admit_put_object((min_size - 1) as i64)
                .await
                .expect("small outer archive admission must remain open"),
            ForegroundWriteAdmission::Disabled
        ));
        let large_member = match manager
            .admit_snowball_foreground_write(min_size as i64)
            .await
            .expect("large Snowball member admission must remain open")
        {
            ForegroundWriteAdmission::Admitted(permit) => permit,
            outcome => panic!("large Snowball member must consume the large PUT gate: {outcome:?}"),
        };
        assert!(matches!(
            manager
                .admit_snowball_foreground_write(min_size as i64)
                .await
                .expect("saturated Snowball member admission must remain open"),
            ForegroundWriteAdmission::Rejected
        ));
        assert!(matches!(
            manager
                .admit_put_object(min_size as i64)
                .await
                .expect("ordinary large PUT admission must remain open"),
            ForegroundWriteAdmission::Rejected
        ));
        assert!(matches!(
            manager
                .admit_snowball_foreground_write((min_size - 1) as i64)
                .await
                .expect("small Snowball member admission must remain open"),
            ForegroundWriteAdmission::Disabled
        ));
        drop(large_member);
    }

    #[tokio::test]
    #[serial]
    async fn test_concurrency_manager_priority_queue_integration() {
        let manager = ConcurrencyManager::new();

        // Test priority determination
        let small_size = 100 * 1024; // 100KB
        let large_size = 200 * 1024 * 1024; // 200MB

        let small_priority = manager.get_io_priority(small_size as i64);
        let large_priority = manager.get_io_priority(large_size as i64);

        assert_eq!(small_priority, IoPriority::High);
        assert_eq!(large_priority, IoPriority::Low);
    }

    #[tokio::test]
    #[serial]
    async fn test_concurrency_manager_io_queue_status() {
        let manager = ConcurrencyManager::new();

        let status = manager.io_queue_status();

        // Initial state should have no waiting requests
        assert_eq!(status.high_priority_waiting, 0);
        assert_eq!(status.normal_priority_waiting, 0);
        assert_eq!(status.low_priority_waiting, 0);
    }

    #[tokio::test]
    #[serial]
    async fn test_concurrency_manager_workload_admission_snapshot_tracks_disk_read_permits() {
        let manager = ConcurrencyManager::new();
        let initial = manager.get_object_admission_snapshot();

        assert_eq!(initial.class, WorkloadClass::ForegroundRead);
        assert_eq!(initial.state, AdmissionState::Open);
        assert_eq!(initial.active, Some(0));
        assert_eq!(initial.queued, None);
        assert_eq!(initial.limit, Some(manager.scheduler_config().max_concurrent_reads));

        let permit = manager.acquire_disk_read_permit().await;
        assert!(permit.is_ok());
        let _permit = permit.ok();
        let snapshot = manager.get_object_admission_snapshot();

        assert_eq!(snapshot.active, Some(1));
        assert_eq!(snapshot.limit, Some(manager.scheduler_config().max_concurrent_reads));
    }

    #[tokio::test]
    #[serial]
    async fn test_concurrency_manager_owned_disk_read_permit_tracks_queue_snapshot() {
        let manager = ConcurrencyManager::new();

        let permit = manager
            .acquire_owned_disk_read_permit()
            .await
            .expect("owned disk read permit should be acquired");
        let snapshot = manager.get_object_admission_snapshot();

        assert_eq!(snapshot.active, Some(1));
        assert_eq!(snapshot.limit, Some(manager.scheduler_config().max_concurrent_reads));

        drop(permit);
        assert_eq!(manager.get_object_admission_snapshot().active, Some(0));
    }

    #[tokio::test]
    #[serial]
    async fn test_concurrency_manager_workload_admission_snapshot_tracks_put_requests() {
        crate::storage::concurrency::reset_active_put_requests();
        let manager = ConcurrencyManager::with_put_admission_for_test(false, 0, Duration::ZERO);
        let initial = manager.put_object_admission_snapshot();

        assert_eq!(initial.class, WorkloadClass::ForegroundWrite);
        assert_eq!(initial.state, AdmissionState::Open);
        assert_eq!(initial.active, Some(0));
        assert_eq!(initial.queued, None);
        assert_eq!(initial.limit, Some(manager.scheduler_config().max_concurrent_reads));

        let guard = PutObjectGuard::new();
        let snapshot = manager.put_object_admission_snapshot();

        assert_eq!(snapshot.active, Some(1));
        assert_eq!(snapshot.limit, Some(manager.scheduler_config().max_concurrent_reads));
        drop(guard);
        crate::storage::concurrency::reset_active_put_requests();
    }

    #[tokio::test]
    #[serial]
    async fn test_concurrency_manager_put_admission_disabled_does_not_touch_gate() {
        let manager = ConcurrencyManager::with_put_admission_for_test(false, 1, Duration::ZERO);

        let admission = manager
            .admit_put_object(1024)
            .await
            .expect("disabled put admission must not close");

        assert!(matches!(admission, ForegroundWriteAdmission::Disabled));
        assert_eq!(manager.put_object_admission_snapshot().state, AdmissionState::Open);
    }

    #[tokio::test]
    #[serial]
    async fn test_concurrency_manager_strict_put_admission_zero_limit_disables_large_gate() {
        let manager = ConcurrencyManager::with_put_admission_for_test(true, 0, Duration::ZERO);

        let admission = manager
            .admit_put_object(32 * 1024 * 1024)
            .await
            .expect("strict zero-limit put admission must not close");

        assert!(matches!(admission, ForegroundWriteAdmission::Disabled));
        assert_eq!(manager.put_object_admission_snapshot().state, AdmissionState::Disabled);
    }

    #[tokio::test]
    #[serial]
    async fn test_concurrency_manager_put_admission_rejects_when_limit_full() {
        let manager = ConcurrencyManager::with_put_admission_for_test(true, 1, Duration::ZERO);

        let first = manager
            .admit_put_object(1024)
            .await
            .expect("first put admission should acquire");
        assert!(matches!(first, ForegroundWriteAdmission::Admitted(_)));
        assert_eq!(manager.put_object_admission_snapshot().state, AdmissionState::Saturated);

        let second = manager
            .admit_put_object(1024)
            .await
            .expect("full put admission gate should reject, not close");
        assert!(matches!(second, ForegroundWriteAdmission::Rejected));
    }

    #[tokio::test]
    #[serial]
    async fn test_concurrency_manager_put_admission_reuses_released_permit() {
        let manager = ConcurrencyManager::with_put_admission_for_test(true, 1, Duration::ZERO);

        let first = manager
            .admit_put_object(1024)
            .await
            .expect("first put admission should acquire");
        drop(first);

        let second = manager
            .admit_put_object(1024)
            .await
            .expect("released put admission permit should be reusable");
        assert!(matches!(second, ForegroundWriteAdmission::Admitted(_)));
    }

    #[tokio::test(start_paused = true)]
    #[serial]
    async fn test_concurrency_manager_put_admission_wait_timeout_rejects() {
        let manager = ConcurrencyManager::with_put_admission_for_test(true, 1, Duration::from_secs(5));
        let held = manager
            .admit_put_object(1024)
            .await
            .expect("first put admission should acquire");
        let waiter_manager = manager.clone();

        let waiter = tokio::spawn(async move { waiter_manager.admit_put_object(1024).await });
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(5)).await;

        let admission = waiter
            .await
            .expect("put admission waiter task must not panic")
            .expect("put admission gate must stay open");
        assert!(matches!(admission, ForegroundWriteAdmission::Rejected));
        drop(held);
    }

    #[tokio::test]
    #[serial]
    async fn test_concurrency_manager_large_put_admission_bypasses_small_puts() {
        let min_size = rustfs_config::DEFAULT_PUT_LARGE_FOREGROUND_ADMISSION_MIN_SIZE_BYTES;
        let manager = ConcurrencyManager::with_large_put_admission_for_test(true, 1, min_size, Duration::ZERO);

        let held = manager
            .admit_put_object(min_size as i64)
            .await
            .expect("large put admission should acquire");
        assert!(matches!(held, ForegroundWriteAdmission::Admitted(_)));

        let small = manager
            .admit_put_object((min_size - 1) as i64)
            .await
            .expect("small put should bypass large admission");
        assert!(matches!(small, ForegroundWriteAdmission::Disabled));

        let large = manager
            .admit_put_object(min_size as i64)
            .await
            .expect("second large put should reject when the gate is full");
        assert!(matches!(large, ForegroundWriteAdmission::Rejected));
    }

    #[tokio::test]
    #[serial]
    async fn test_concurrency_manager_large_put_admission_gates_unknown_size() {
        let manager = ConcurrencyManager::with_large_put_admission_for_test(true, 1, 32 * 1024 * 1024, Duration::ZERO);

        let held = manager
            .admit_put_object(-1)
            .await
            .expect("unknown-size put admission should acquire");
        assert!(matches!(held, ForegroundWriteAdmission::Admitted(_)));

        let second = manager
            .admit_put_object(-1)
            .await
            .expect("unknown-size put admission should reject when the gate is full");
        assert!(matches!(second, ForegroundWriteAdmission::Rejected));
    }

    #[tokio::test]
    #[serial]
    async fn test_concurrency_manager_large_put_admission_gates_multipart_parts_by_default() {
        let min_size = rustfs_config::DEFAULT_PUT_LARGE_FOREGROUND_ADMISSION_MIN_SIZE_BYTES;
        let manager = ConcurrencyManager::with_large_put_admission_for_test(true, 1, min_size, Duration::ZERO);

        let held = manager
            .admit_multipart_part(1024)
            .await
            .expect("first multipart part admission should acquire");
        assert!(matches!(held, ForegroundWriteAdmission::Admitted(_)));

        let direct_small_put = manager
            .admit_put_object((min_size - 1) as i64)
            .await
            .expect("small direct put should bypass the large put threshold");
        assert!(matches!(direct_small_put, ForegroundWriteAdmission::Disabled));

        let second_part = manager
            .admit_multipart_part(1024)
            .await
            .expect("full multipart admission gate should reject, not close");
        assert!(matches!(second_part, ForegroundWriteAdmission::Rejected));
    }

    #[tokio::test]
    #[serial]
    async fn test_concurrency_manager_large_put_snapshot_tracks_gate() {
        let manager = ConcurrencyManager::with_large_put_admission_for_test(true, 2, 32 * 1024 * 1024, Duration::ZERO);
        let first = manager
            .admit_put_object(32 * 1024 * 1024)
            .await
            .expect("first large put admission should acquire");
        let initial = manager.put_object_admission_snapshot();

        assert_eq!(initial.class, WorkloadClass::ForegroundWrite);
        assert_eq!(initial.state, AdmissionState::Open);
        assert_eq!(initial.active, Some(1));
        assert_eq!(initial.limit, Some(2));

        let second = manager
            .admit_put_object(32 * 1024 * 1024)
            .await
            .expect("second large put admission should acquire");
        let saturated = manager.put_object_admission_snapshot();

        assert_eq!(saturated.state, AdmissionState::Saturated);
        assert_eq!(saturated.active, Some(2));
        assert_eq!(saturated.limit, Some(2));
        drop((first, second));
    }

    #[tokio::test(start_paused = true)]
    #[serial]
    async fn test_concurrency_manager_multipart_part_waits_for_released_permit() {
        let manager = ConcurrencyManager::with_multipart_admission_queue_for_test(1, Duration::from_secs(30), 0);
        let held = manager
            .admit_multipart_part(8 * 1024 * 1024)
            .await
            .expect("first multipart part admission should acquire");
        assert!(matches!(held, ForegroundWriteAdmission::Admitted(_)));

        let waiter_manager = manager.clone();
        let waiter = tokio::spawn(async move { waiter_manager.admit_multipart_part(8 * 1024 * 1024).await });
        tokio::task::yield_now().await;
        assert_eq!(manager.put_object_admission_snapshot().queued, Some(1));

        tokio::time::advance(Duration::from_secs(5)).await;
        drop(held);

        let admission = waiter
            .await
            .expect("multipart admission waiter task must not panic")
            .expect("multipart admission gate must stay open");
        assert!(matches!(admission, ForegroundWriteAdmission::Admitted(_)));
        assert_eq!(manager.put_object_admission_snapshot().queued, Some(0));
    }

    #[tokio::test(start_paused = true)]
    #[serial]
    async fn test_concurrency_manager_multipart_part_rejects_after_queue_wait_timeout() {
        let manager = ConcurrencyManager::with_multipart_admission_queue_for_test(1, Duration::from_secs(30), 0);
        let held = manager
            .admit_multipart_part(1024)
            .await
            .expect("first multipart part admission should acquire");

        let waiter_manager = manager.clone();
        let waiter = tokio::spawn(async move { waiter_manager.admit_multipart_part(1024).await });
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(30)).await;

        let admission = waiter
            .await
            .expect("multipart admission waiter task must not panic")
            .expect("multipart admission gate must stay open");
        assert!(matches!(admission, ForegroundWriteAdmission::Rejected));
        assert_eq!(manager.put_object_admission_snapshot().queued, Some(0));
        drop(held);
    }

    #[tokio::test(start_paused = true)]
    #[serial]
    async fn test_concurrency_manager_multipart_part_rejects_immediately_when_queue_is_full() {
        let manager = ConcurrencyManager::with_multipart_admission_queue_for_test(1, Duration::from_secs(30), 1);
        let held = manager
            .admit_multipart_part(1024)
            .await
            .expect("first multipart part admission should acquire");

        let waiter_manager = manager.clone();
        let queued = tokio::spawn(async move { waiter_manager.admit_multipart_part(1024).await });
        tokio::task::yield_now().await;
        assert_eq!(manager.put_object_admission_snapshot().queued, Some(1));

        let overflow = manager
            .admit_multipart_part(1024)
            .await
            .expect("full multipart queue should reject, not close");
        assert!(matches!(overflow, ForegroundWriteAdmission::Rejected));

        drop(held);
        let admission = queued
            .await
            .expect("queued multipart part task must not panic")
            .expect("multipart admission gate must stay open");
        assert!(matches!(admission, ForegroundWriteAdmission::Admitted(_)));
    }

    #[tokio::test(start_paused = true)]
    #[serial]
    async fn test_concurrency_manager_cancelled_multipart_waiter_releases_queue_slot() {
        let manager = ConcurrencyManager::with_multipart_admission_queue_for_test(1, Duration::from_secs(30), 1);
        let held = manager
            .admit_multipart_part(1024)
            .await
            .expect("first multipart part admission should acquire");

        let waiter_manager = manager.clone();
        let queued = tokio::spawn(async move { waiter_manager.admit_multipart_part(1024).await });
        tokio::task::yield_now().await;
        assert_eq!(manager.put_object_admission_snapshot().queued, Some(1));
        queued.abort();
        let _ = queued.await;

        assert_eq!(manager.put_object_admission_snapshot().queued, Some(0));
        drop(held);
    }

    #[test]
    fn test_concurrency_manager_derives_multipart_admission_max_pending_from_limit() {
        assert_eq!(derive_multipart_admission_max_pending(7, 32), 7);
        assert_eq!(derive_multipart_admission_max_pending(0, 32), 512);
        assert_eq!(derive_multipart_admission_max_pending(0, 1), 16);
    }

    #[test]
    fn test_concurrency_manager_derives_large_put_admission_limit_from_scheduler_cap() {
        assert_eq!(derive_large_put_admission_limit(7, 64), 7);
        assert_eq!(derive_large_put_admission_limit(0, 64), 32);
        assert_eq!(derive_large_put_admission_limit(0, 8), 4);
        assert_eq!(derive_large_put_admission_limit(0, 1), 1);
        assert_eq!(derive_large_put_admission_limit(0, 0), 32);
    }

    #[tokio::test]
    #[serial]
    async fn test_concurrency_manager_workload_admission_registry_covers_required_classes() {
        let manager = ConcurrencyManager::new();
        let registry = manager.workload_admission_registry_snapshot();
        let provider: &dyn WorkloadAdmissionSnapshotProvider = &manager;
        let trait_registry = provider.workload_admission_snapshot();

        assert_eq!(registry.entries(), trait_registry.entries());
        assert_eq!(registry.entries().len(), WorkloadClass::REQUIRED.len());
        for class in WorkloadClass::REQUIRED {
            assert!(registry.get(class).is_some(), "missing workload class {class}");
        }

        assert_eq!(
            registry.get(WorkloadClass::ForegroundRead).map(|snapshot| snapshot.state),
            Some(AdmissionState::Open)
        );
        assert_eq!(
            registry.get(WorkloadClass::ForegroundWrite).map(|snapshot| snapshot.state),
            Some(AdmissionState::Open)
        );
        assert_eq!(
            registry.get(WorkloadClass::Scanner).map(|snapshot| snapshot.state),
            Some(AdmissionState::Unknown)
        );
    }

    #[tokio::test]
    #[serial]
    async fn test_concurrency_manager_disk_read_permit() {
        let manager = ConcurrencyManager::new();

        // Acquire permit
        let permit = manager.acquire_disk_read_permit().await;
        assert!(permit.is_ok());

        // Permit should be released when dropped
        drop(permit);

        // Should be able to acquire again
        let permit2 = manager.acquire_disk_read_permit().await;
        assert!(permit2.is_ok());
    }

    #[tokio::test]
    #[serial]
    async fn test_concurrency_manager_priority_scheduling() {
        let manager = ConcurrencyManager::new();

        // Test if priority scheduling is enabled
        let enabled = manager.is_priority_scheduling_enabled();
        assert!(enabled); // Should be enabled by default

        // Test priority determination for different sizes
        assert_eq!(manager.get_io_priority(500 * 1024), IoPriority::High); // 500KB
        assert_eq!(manager.get_io_priority(5 * 1024 * 1024), IoPriority::Normal); // 5MB
        assert_eq!(manager.get_io_priority(50 * 1024 * 1024), IoPriority::Low); // 50MB
    }

    #[tokio::test]
    #[serial]
    async fn test_concurrency_manager_io_strategy() {
        let manager = ConcurrencyManager::new();

        // Test I/O strategy calculation
        let low_wait = Duration::from_millis(5);
        let high_wait = Duration::from_millis(100);

        let strategy_low = manager.calculate_io_strategy(low_wait, 128 * 1024);
        let strategy_high = manager.calculate_io_strategy(high_wait, 128 * 1024);

        // Under low load, should use larger buffers
        assert!(strategy_low.buffer_size >= strategy_high.buffer_size);
    }

    #[tokio::test]
    #[serial]
    async fn test_concurrency_manager_adaptive_buffer_size() {
        let manager = ConcurrencyManager::new();

        let base_size = 128 * 1024; // 128KB

        // Test adaptive buffer sizing
        let size1 = manager.adaptive_buffer_size(base_size);

        // Should return a reasonable buffer size
        assert!(size1 > 0);
        assert!(size1 <= 2 * 1024 * 1024); // Not more than 2MB
    }

    // ============================================
    // Multi-Factor Strategy Integration Tests
    // ============================================

    #[tokio::test]
    #[serial]
    async fn test_concurrency_manager_multi_factor_strategy_nvme_optimal() {
        let manager = ConcurrencyManager::new();

        // Simulate optimal conditions: Unknown/SSD + Sequential + Low load
        let file_size = 100 * 1024 * 1024; // 100MB
        let base_buffer = 256 * 1024;
        let permit_wait = Duration::from_millis(5); // Low load
        let is_sequential = true;

        let strategy = manager.calculate_io_strategy_with_context(file_size, base_buffer, permit_wait, is_sequential);
        let media = manager.storage_media();

        // Verify basic optimizations work
        assert_eq!(strategy.storage_media, media);
        assert!(strategy.buffer_size >= base_buffer * 8 / 10, "Sequential should maintain or boost buffer");
        let expected_readahead = !matches!(media, StorageMedia::Hdd);
        assert_eq!(
            strategy.enable_readahead, expected_readahead,
            "Readahead should follow storage profile preference under low load"
        );
        assert_eq!(strategy.load_level, IoLoadLevel::Low);
    }

    #[tokio::test]
    #[serial]
    async fn test_concurrency_manager_multi_factor_strategy_access_pattern_tracking() {
        let manager = ConcurrencyManager::new();

        // Record sequential accesses
        for offset in [0, 1024, 2048, 3072, 4096] {
            manager.record_access(offset, 1024);
        }

        // Check pattern detection
        let pattern = manager.current_access_pattern();
        assert_eq!(pattern, AccessPattern::Sequential);

        // Record random accesses
        for offset in [0, 10 * 1024, 100 * 1024, 5 * 1024 * 1024] {
            manager.record_access(offset, 1024);
        }

        // Pattern should change to mixed or random
        let pattern_after = manager.current_access_pattern();
        assert!(!matches!(pattern_after, AccessPattern::Sequential));
    }

    #[tokio::test]
    #[serial]
    async fn test_concurrency_manager_multi_factor_strategy_bandwidth_recording() {
        let manager = ConcurrencyManager::new();

        // Simulate transfer
        let bytes = 10 * 1024 * 1024; // 10MB
        let duration = Duration::from_millis(100); // 100ms = 100MB/s

        manager.record_transfer(bytes, duration);

        // Check bandwidth snapshot (returns BandwidthSnapshot directly)
        let snapshot = manager.current_bandwidth_snapshot();
        assert!(snapshot.bytes_per_second > 0, "Should have bandwidth data after recording");
    }

    #[tokio::test]
    #[serial]
    async fn test_concurrency_manager_multi_factor_strategy_compatibility() {
        let manager = ConcurrencyManager::new();

        // Test that old API still works
        let old_strategy = manager.calculate_io_strategy(Duration::from_millis(50), 256 * 1024);

        assert!(old_strategy.buffer_size > 0);

        // New API with context should also work
        let new_strategy =
            manager.calculate_io_strategy_with_context(50 * 1024 * 1024, 256 * 1024, Duration::from_millis(50), false);

        assert!(new_strategy.buffer_size > 0);
    }

    #[tokio::test]
    #[serial]
    async fn test_concurrency_manager_multi_factor_strategy_high_concurrency() {
        let manager = ConcurrencyManager::new();

        // Simulate high concurrent requests by keeping guards alive
        let _guards: Vec<_> = (0..20).map(|_| GetObjectGuard::new()).collect();

        let strategy = manager.calculate_io_strategy_with_context(100 * 1024 * 1024, 512 * 1024, Duration::from_millis(10), true);

        // High concurrency should reduce buffer
        assert!(strategy.concurrent_requests >= manager.scheduler_config().high_concurrency_threshold);
        assert!(strategy.buffer_size < 512 * 1024, "High concurrency should reduce buffer");
    }

    #[tokio::test]
    #[serial]
    async fn test_concurrency_manager_multi_factor_strategy_buffer_clamp() {
        let manager = ConcurrencyManager::new();
        let media = manager.storage_media();
        let config = manager.scheduler_config();

        // Request very large base buffer
        let large_base = 16 * 1024 * 1024; // 16MB

        let strategy = manager.calculate_io_strategy_with_context(
            1024 * 1024, // 1GB file
            large_base,
            Duration::from_millis(1),
            true,
        );

        let media_cap = match media {
            StorageMedia::Nvme => config.nvme_buffer_cap,
            StorageMedia::Ssd => config.ssd_buffer_cap,
            StorageMedia::Hdd => config.hdd_buffer_cap,
            StorageMedia::Unknown => config.ssd_buffer_cap,
        };

        // Large base buffer should be constrained by the storage media cap.
        // The final safety clamp is [32KiB, media_cap.max(MI_B)], so it never
        // lowers the result below the media cap (e.g. NVMe's 2MiB cap stays
        // effective even though the global floor clamp is 1MiB).
        assert_eq!(strategy.buffer_size, media_cap, "Buffer should be capped by the storage media profile");
    }

    #[tokio::test]
    #[serial]
    async fn test_concurrency_manager_multi_factor_strategy_storage_media_detection() {
        let manager = ConcurrencyManager::new();

        // Check storage media was detected at initialization
        let media = manager.storage_media();

        // Should be one of the known types (not Unknown unless detection failed)
        // We accept Unknown if detection wasn't configured
        assert!(matches!(
            media,
            StorageMedia::Nvme | StorageMedia::Ssd | StorageMedia::Hdd | StorageMedia::Unknown
        ));
    }

    #[tokio::test]
    #[serial]
    async fn test_concurrency_manager_multi_factor_strategy_priority_with_context() {
        let manager = ConcurrencyManager::new();

        // Test priority is correctly calculated in multi-factor strategy
        let small_file_strategy = manager.calculate_io_strategy_with_context(
            500 * 1024, // 500KB
            256 * 1024,
            Duration::from_millis(10),
            false,
        );

        let large_file_strategy = manager.calculate_io_strategy_with_context(
            50 * 1024 * 1024, // 50MB
            256 * 1024,
            Duration::from_millis(10),
            false,
        );

        assert_eq!(small_file_strategy.priority, IoPriority::High);
        assert_eq!(large_file_strategy.priority, IoPriority::Low);
    }

    // ============================================
    // Bounded disk-read admission (backlog#1317)
    // ============================================

    use super::DiskReadAdmission;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

    /// Primary pool saturation degrades into the bounded lane, and the hard cap
    /// rejects with `Rejected` instead of admitting a permit-less read. Dropping
    /// a permit frees the token so the next admission succeeds again.
    #[tokio::test(start_paused = true)]
    async fn test_admit_disk_read_degrades_then_hard_rejects() {
        let manager = ConcurrencyManager::with_disk_read_caps_for_test(1, 1);

        // Primary permit granted immediately.
        let primary = match manager.admit_disk_read(Duration::from_millis(100)).await.unwrap() {
            DiskReadAdmission::Primary(permit) => permit,
            other => panic!("expected primary admission, got {other:?}"),
        };

        // Primary pool saturated: next admission waits out the primary timeout
        // and falls through to the bounded degraded lane.
        let degraded = match manager.admit_disk_read(Duration::from_millis(100)).await.unwrap() {
            DiskReadAdmission::Degraded(permit) => permit,
            other => panic!("expected degraded admission, got {other:?}"),
        };

        // Both lanes full: hard cap reached, must reject rather than bypass.
        match manager.admit_disk_read(Duration::from_millis(100)).await.unwrap() {
            DiskReadAdmission::Rejected => {}
            other => panic!("expected hard rejection, got {other:?}"),
        }

        // Releasing the degraded permit re-opens exactly one degraded slot.
        drop(degraded);
        match manager.admit_disk_read(Duration::from_millis(100)).await.unwrap() {
            DiskReadAdmission::Degraded(_permit) => {}
            other => panic!("expected degraded admission after release, got {other:?}"),
        }

        // Releasing the primary permit re-opens the primary lane immediately
        // (no timeout wait), proving EOF/drop returns the token.
        drop(primary);
        match manager.admit_disk_read(Duration::from_millis(100)).await.unwrap() {
            DiskReadAdmission::Primary(_permit) => {}
            other => panic!("expected primary admission after release, got {other:?}"),
        }
    }

    /// With max primary = 1 and degraded = 1, 100 concurrent GETs never hold
    /// more than `1 + 1` admission tokens simultaneously. Reverting the hard cap
    /// (unbounded bypass) would let this exceed 2 and fail the test.
    #[tokio::test(start_paused = true)]
    async fn test_admit_disk_read_hard_caps_concurrent_admissions() {
        let manager = std::sync::Arc::new(ConcurrencyManager::with_disk_read_caps_for_test(1, 1));
        let in_flight = std::sync::Arc::new(AtomicUsize::new(0));
        let max_in_flight = std::sync::Arc::new(AtomicUsize::new(0));
        let admitted = std::sync::Arc::new(AtomicUsize::new(0));
        let rejected = std::sync::Arc::new(AtomicUsize::new(0));

        let mut handles = Vec::with_capacity(100);
        for _ in 0..100 {
            let manager = manager.clone();
            let in_flight = in_flight.clone();
            let max_in_flight = max_in_flight.clone();
            let admitted = admitted.clone();
            let rejected = rejected.clone();
            handles.push(tokio::spawn(async move {
                match manager.admit_disk_read(Duration::from_millis(50)).await.unwrap() {
                    DiskReadAdmission::Primary(permit) | DiskReadAdmission::Degraded(permit) => {
                        let current = in_flight.fetch_add(1, AtomicOrdering::SeqCst) + 1;
                        max_in_flight.fetch_max(current, AtomicOrdering::SeqCst);
                        admitted.fetch_add(1, AtomicOrdering::SeqCst);
                        // Hold the admission token across an await point, as the
                        // real body transfer does, then release it.
                        tokio::time::sleep(Duration::from_millis(10)).await;
                        in_flight.fetch_sub(1, AtomicOrdering::SeqCst);
                        drop(permit);
                    }
                    DiskReadAdmission::Rejected => {
                        rejected.fetch_add(1, AtomicOrdering::SeqCst);
                    }
                    DiskReadAdmission::Unbounded => {
                        unreachable!("throttling is enabled (primary cap = 1) in this test");
                    }
                }
            }));
        }

        for handle in handles {
            handle.await.unwrap();
        }

        // The invariant the hard cap guarantees: never more than
        // primary + degraded = 2 GETs admitted at once.
        assert!(
            max_in_flight.load(AtomicOrdering::SeqCst) <= 2,
            "max concurrent admissions {} exceeded the hard cap of 2",
            max_in_flight.load(AtomicOrdering::SeqCst)
        );
        // Every request is accounted for: admitted or explicitly rejected, never
        // a permit-less bypass.
        assert_eq!(admitted.load(AtomicOrdering::SeqCst) + rejected.load(AtomicOrdering::SeqCst), 100);
        assert!(
            rejected.load(AtomicOrdering::SeqCst) > 0,
            "expected some hard rejections under saturation"
        );
    }

    /// A primary cap of 0 (throttling disabled) serves every GET without an
    /// admission token instead of rejecting them, preserving the pre-existing
    /// "disk read permits disabled" behavior.
    #[tokio::test(start_paused = true)]
    async fn test_admit_disk_read_disabled_cap_is_unbounded() {
        let manager = ConcurrencyManager::with_disk_read_caps_for_test(0, 0);
        for _ in 0..10 {
            match manager.admit_disk_read(Duration::from_millis(50)).await.unwrap() {
                DiskReadAdmission::Unbounded => {}
                other => panic!("expected unbounded admission when throttling disabled, got {other:?}"),
            }
        }
    }

    /// `primary_wait == 0` preserves the wait-forever opt-out: it always yields
    /// a primary permit and never degrades or rejects.
    #[tokio::test]
    async fn test_admit_disk_read_zero_wait_waits_on_primary() {
        let manager = ConcurrencyManager::with_disk_read_caps_for_test(1, 1);
        let held = match manager.admit_disk_read(Duration::ZERO).await.unwrap() {
            DiskReadAdmission::Primary(permit) => permit,
            other => panic!("expected primary admission, got {other:?}"),
        };

        // A second zero-wait admission blocks on the primary pool rather than
        // degrading; it completes only once the first permit is released.
        let manager2 = manager.clone();
        let waiter = tokio::spawn(async move { manager2.admit_disk_read(Duration::ZERO).await.unwrap() });
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished(), "zero-wait admission must block on the primary lane");

        drop(held);
        match waiter.await.unwrap() {
            DiskReadAdmission::Primary(_permit) => {}
            other => panic!("expected primary admission after release, got {other:?}"),
        }
    }
}
