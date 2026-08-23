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

//! Low-level read/erasure IO primitives shared across the `SetDisks`
//! operation families (P5 of the God-Object split, tracking backlog#815,
//! issue backlog#820).
//!
//! These are the metadata-fanout quorum accumulator, bitrot reader
//! scheduling/creation, shard-cost classification, and read-repair heal
//! dedup helpers relocated verbatim from `set_disk/read.rs`. Bodies are
//! byte-identical to the pre-move sources; only the module header
//! (`use super::*;` -> `use super::super::*;`) and item visibility change.

use super::super::*;
use crate::diagnostics::get::{
    GET_DIRECT_MEMORY_SUBPATH_DISK_DATA_BLOCKS, GET_DIRECT_MEMORY_SUBPATH_INLINE_BUFFERED, GET_METADATA_CACHE_DECISION_HIT,
    GET_METADATA_CACHE_DECISION_MISS, GET_METADATA_CACHE_DECISION_REJECT, GET_METADATA_CACHE_DECISION_SKIP,
    GET_METADATA_CACHE_REASON_DATA_MOVEMENT, GET_METADATA_CACHE_REASON_DELETE_MARKER, GET_METADATA_CACHE_REASON_DIST_ERASURE,
    GET_METADATA_CACHE_REASON_INCL_FREE_VERSIONS, GET_METADATA_CACHE_REASON_INSUFFICIENT_CACHED_QUORUM,
    GET_METADATA_CACHE_REASON_META_BUCKET, GET_METADATA_CACHE_REASON_NO_LOCK, GET_METADATA_CACHE_REASON_NOT_FOUND_OR_EXPIRED,
    GET_METADATA_CACHE_REASON_NOT_READ_DATA, GET_METADATA_CACHE_REASON_PART_NUMBER,
    GET_METADATA_CACHE_REASON_RAW_DATA_MOVEMENT_READ, GET_METADATA_CACHE_REASON_USABLE, GET_METADATA_CACHE_REASON_VERSION_ID,
    GET_METADATA_CACHE_REASON_VERSION_SUSPENDED, GET_METADATA_CACHE_REASON_VERSIONED,
    GET_METADATA_EARLY_STOP_REASON_CONFLICTING_METADATA, GET_METADATA_EARLY_STOP_REASON_DATA_READ_INLINE_BODY_VERIFY,
    GET_METADATA_EARLY_STOP_REASON_DATA_READ_INLINE_DELETED, GET_METADATA_EARLY_STOP_REASON_DATA_READ_INLINE_GEOMETRY,
    GET_METADATA_EARLY_STOP_REASON_DATA_READ_INLINE_IDENTITY_MISMATCH,
    GET_METADATA_EARLY_STOP_REASON_DATA_READ_INLINE_MISSING_PAYLOAD,
    GET_METADATA_EARLY_STOP_REASON_DATA_READ_INLINE_MISSING_SHARD, GET_METADATA_EARLY_STOP_REASON_DATA_READ_INLINE_NOT_INLINE,
    GET_METADATA_EARLY_STOP_REASON_DATA_READ_INLINE_PART_SHAPE, GET_METADATA_EARLY_STOP_REASON_DATA_READ_INLINE_REMOTE,
    GET_METADATA_EARLY_STOP_REASON_DATA_READ_INLINE_SIZE, GET_METADATA_EARLY_STOP_REASON_DATA_READ_INLINE_TRANSFORMED,
    GET_METADATA_EARLY_STOP_REASON_DELETE_MARKER, GET_METADATA_EARLY_STOP_REASON_ERROR,
    GET_METADATA_EARLY_STOP_REASON_INSUFFICIENT_QUORUM, GET_METADATA_EARLY_STOP_REASON_NOT_FOUND,
    GET_METADATA_EARLY_STOP_REASON_UNSAFE_REQUEST, GET_METADATA_EARLY_STOP_REASON_VALID_QUORUM,
    GET_METADATA_EARLY_STOP_REASON_VERSION_MATCH_QUORUM, GET_METADATA_EARLY_STOP_REASON_VERSION_NOT_FOUND,
    GET_METADATA_RESPONSE_CORRUPT, GET_METADATA_RESPONSE_DISK_NOT_FOUND, GET_METADATA_RESPONSE_ERROR,
    GET_METADATA_RESPONSE_IGNORED, GET_METADATA_RESPONSE_NOT_FOUND, GET_METADATA_RESPONSE_TIMEOUT, GET_METADATA_RESPONSE_VALID,
    GET_METADATA_RESPONSE_VERSION_NOT_FOUND, GET_OBJECT_PATH_CODEC_STREAMING, GET_OBJECT_PATH_DIRECT_MEMORY,
    GET_OBJECT_PATH_INTERNAL_META, GET_OBJECT_PATH_LEGACY_DUPLEX, GET_OBJECT_PATH_SET_DISK, GET_STAGE_DECODE,
    GET_STAGE_METADATA_CACHE_LOOKUP, GET_STAGE_METADATA_RESOLVE, GET_STAGE_RANGE, GET_STAGE_READER_SETUP,
    GET_STAGE_READER_SETUP_DROP_PENDING, GET_STAGE_READER_SETUP_SCHEDULE, GET_STAGE_READER_SETUP_WAIT_QUORUM,
    GET_STAGE_READER_TASK_BITROT_READER_INIT, GET_STAGE_READER_TASK_FILE_OPEN, GET_STAGE_READER_TASK_READER_CONSTRUCTION,
    GetObjectFailureReason, classify_disk_error, get_stage_timer_if_enabled, record_get_object_pipeline_failure,
    record_get_object_pipeline_failure_for_path, record_get_stage_duration_if_enabled,
};
use crate::disk::disk_store::{DiskStoreRenameDataExt, get_drive_metadata_timeout};
use crate::disk::local::DELETE_DATA_DIR_MARKER_PREFIX;
use crate::disk::{
    BATCH_READ_VERSION_MAX_ITEMS, BatchReadVersionItem, BatchReadVersionReq, BatchReadVersionResp, DataDirDeleteStatus, Disk,
    OldCurrentSize, PART_TRANSACTION_NEW_META, PART_TRANSACTION_OLD_META, PART_TRANSACTION_ROLLBACK, PartTransactionAction,
    STORAGE_FORMAT_FILE_BACKUP, part_transaction_path,
};
use crate::erasure::coding::BitrotReader;
use crate::io_support::bitrot::ShardReader;
use crate::io_support::bitrot::{
    BitrotReaderStageMetrics, DeferredReaderStripeHandle, adjust_shard_read_params,
    create_bitrot_reader_from_bytes_with_stage_metrics, create_deferred_bitrot_reader_with_stripe_handle,
    object_mmap_read_enabled, object_mmap_read_max_length,
};
use crate::set_disk::shard_source::ShardReadCost;
use futures::FutureExt as _;
use futures::stream::{FuturesUnordered, StreamExt};
use metrics::counter;
use std::{
    collections::{HashMap, HashSet, VecDeque},
    future::Future,
    pin::Pin,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::{Duration, Instant},
};

fn metadata_metrics_path(bucket: &str) -> &'static str {
    if crate::bucket::utils::is_meta_bucketname(bucket) {
        GET_OBJECT_PATH_INTERNAL_META
    } else {
        GET_OBJECT_PATH_LEGACY_DUPLEX
    }
}

fn metadata_distribution_key(bucket: &str, object: &str) -> String {
    [bucket, object].join("/")
}

fn read_version_coalescing_enabled() -> bool {
    let enabled = || {
        rustfs_utils::get_env_opt_str(ENV_RUSTFS_GET_METADATA_READ_VERSION_COALESCE)
            .is_some_and(|value| value.eq_ignore_ascii_case("auto") || value.eq_ignore_ascii_case("on"))
    };

    #[cfg(test)]
    {
        enabled()
    }

    #[cfg(not(test))]
    {
        static ENABLED: OnceLock<bool> = OnceLock::new();
        *ENABLED.get_or_init(enabled)
    }
}

fn read_version_coalescing_delay() -> Duration {
    #[cfg(test)]
    {
        let micros = rustfs_utils::get_env_u64(
            ENV_RUSTFS_GET_METADATA_READ_VERSION_COALESCE_DELAY_MICROS,
            DEFAULT_GET_METADATA_READ_VERSION_COALESCE_DELAY_MICROS,
        );
        Duration::from_micros(micros)
    }

    #[cfg(not(test))]
    {
        static DELAY: OnceLock<Duration> = OnceLock::new();
        *DELAY.get_or_init(|| {
            Duration::from_micros(rustfs_utils::get_env_u64(
                ENV_RUSTFS_GET_METADATA_READ_VERSION_COALESCE_DELAY_MICROS,
                DEFAULT_GET_METADATA_READ_VERSION_COALESCE_DELAY_MICROS,
            ))
        })
    }
}

struct CoalescedReadVersionRequest {
    item: BatchReadVersionItem,
    tx: oneshot::Sender<disk::error::Result<FileInfo>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ExpectedBatchReadVersionItem {
    path: String,
    version_id: String,
}

impl From<&BatchReadVersionItem> for ExpectedBatchReadVersionItem {
    fn from(item: &BatchReadVersionItem) -> Self {
        Self {
            path: item.path.clone(),
            version_id: item.version_id.clone(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct ReadVersionCoalescerKey {
    disk: usize,
    incl_free_versions: bool,
    read_data: bool,
    healing: bool,
}

impl ReadVersionCoalescerKey {
    fn new(disk: &DiskStore, opts: &ReadOptions) -> Self {
        Self {
            disk: Arc::as_ptr(disk) as usize,
            incl_free_versions: opts.incl_free_versions,
            read_data: opts.read_data,
            healing: opts.healing,
        }
    }
}

#[derive(Default)]
struct ReadVersionCoalescer {
    lanes: HashMap<ReadVersionCoalescerKey, Vec<CoalescedReadVersionRequest>>,
}

fn read_version_coalescer() -> &'static Mutex<ReadVersionCoalescer> {
    static COALESCER: OnceLock<Mutex<ReadVersionCoalescer>> = OnceLock::new();
    COALESCER.get_or_init(|| Mutex::new(ReadVersionCoalescer::default()))
}

fn record_read_version_coalescer_event(event: &'static str, item_count: usize) {
    counter!(
        METRIC_GET_METADATA_READ_VERSION_COALESCER_TOTAL,
        "event" => event,
        "item_count" => item_count.to_string()
    )
    .increment(1);
}

async fn read_version_via_coalescer(
    disk: DiskStore,
    org_bucket: &str,
    bucket: &str,
    object: &str,
    version_id: &str,
    opts: &ReadOptions,
    allow_coalescing: bool,
) -> disk::error::Result<FileInfo> {
    if !allow_coalescing || !read_version_coalescing_enabled() {
        return disk.read_version(org_bucket, bucket, object, version_id, opts).await;
    }
    if !matches!(disk.as_ref(), Disk::Remote(_)) {
        record_read_version_coalescer_event("bypass_non_remote", 1);
        return disk.read_version(org_bucket, bucket, object, version_id, opts).await;
    }

    let (tx, rx) = oneshot::channel();
    let item = BatchReadVersionItem {
        org_volume: org_bucket.to_string(),
        volume: bucket.to_string(),
        path: object.to_string(),
        version_id: version_id.to_string(),
    };
    let lane_key = ReadVersionCoalescerKey::new(&disk, opts);
    let pending = {
        let mut coalescer = read_version_coalescer().lock().await;
        let lane = coalescer.lanes.entry(lane_key).or_default();
        let schedule_delayed_flush = lane.is_empty();
        lane.push(CoalescedReadVersionRequest { item, tx });
        if lane.len() >= BATCH_READ_VERSION_MAX_ITEMS {
            coalescer.lanes.remove(&lane_key)
        } else if schedule_delayed_flush {
            let disk = disk.clone();
            let task_opts = *opts;
            tokio::spawn(async move {
                tokio::time::sleep(read_version_coalescing_delay()).await;
                flush_read_version_coalescer_lane(lane_key, disk, task_opts).await;
            });
            None
        } else {
            None
        }
    };

    if let Some(pending) = pending {
        flush_read_version_coalescer_pending(lane_key, disk, *opts, pending).await;
    }

    rx.await
        .unwrap_or_else(|_| Err(DiskError::other("coalesced read_version response channel closed")))
}

async fn flush_read_version_coalescer_lane(lane_key: ReadVersionCoalescerKey, disk: DiskStore, opts: ReadOptions) {
    let pending = {
        let mut coalescer = read_version_coalescer().lock().await;
        coalescer.lanes.remove(&lane_key).unwrap_or_default()
    };
    flush_read_version_coalescer_pending(lane_key, disk, opts, pending).await;
}

async fn flush_read_version_coalescer_pending(
    lane_key: ReadVersionCoalescerKey,
    disk: DiskStore,
    opts: ReadOptions,
    pending: Vec<CoalescedReadVersionRequest>,
) {
    if pending.is_empty() {
        return;
    }

    #[cfg(test)]
    {
        let mut observed_paths = HashSet::new();
        for request in &pending {
            if observed_paths.insert(request.item.path.as_str()) {
                disk_call_counters::record(&request.item.path, disk_call_counters::KIND_BATCH_READ_VERSION, lane_key.disk);
            }
        }
    }

    let mut senders = Vec::with_capacity(pending.len());
    let mut items = Vec::with_capacity(pending.len());
    for request in pending {
        senders.push(request.tx);
        items.push(request.item);
    }

    let expected_items = items.iter().map(ExpectedBatchReadVersionItem::from).collect::<Vec<_>>();
    record_read_version_coalescer_event("attempted_batch", items.len());
    let result =
        match tokio::time::timeout(get_drive_metadata_timeout(), disk.batch_read_version(BatchReadVersionReq { items, opts }))
            .await
        {
            Ok(result) => result,
            Err(_) => Err(DiskError::Timeout),
        };
    match result {
        Ok(responses) => {
            let results = map_batch_read_version_responses(&expected_items, responses);
            for (tx, result) in senders.into_iter().zip(results) {
                let _ = tx.send(result);
            }
        }
        Err(err) => {
            let message = err.to_string();
            for tx in senders {
                let _ = tx.send(Err(DiskError::other(message.clone())));
            }
        }
    }
}

fn map_batch_read_version_responses(
    expected_items: &[ExpectedBatchReadVersionItem],
    responses: Vec<BatchReadVersionResp>,
) -> Vec<crate::disk::error::Result<FileInfo>> {
    let mut results = (0..expected_items.len())
        .map(|_| Err(DiskError::other("coalesced read_version response missing")))
        .collect::<Vec<_>>();
    let mut seen = vec![false; expected_items.len()];
    for response in responses {
        let Some(expected) = expected_items.get(response.index) else {
            continue;
        };
        let Some(slot) = results.get_mut(response.index) else {
            continue;
        };
        if seen[response.index] {
            *slot = Err(DiskError::other("coalesced read_version response duplicate index"));
            continue;
        }
        seen[response.index] = true;
        if response.path != expected.path || response.version_id != expected.version_id {
            *slot = Err(DiskError::other("coalesced read_version response identity mismatch"));
        } else {
            *slot = if response.success {
                Ok(response.file_info)
            } else {
                Err(batch_read_version_response_error(response.error_code, response.error))
            };
        }
    }
    results
}

fn batch_read_version_response_error(error_code: u32, error: String) -> DiskError {
    match DiskError::from_u32(error_code) {
        Some(DiskError::Io(_)) | None => DiskError::other(error),
        Some(error) => error,
    }
}

pub(in crate::set_disk) fn bounded_metadata_fanout_order(
    bucket: &str,
    object: &str,
    total_disks: usize,
    default_parity_count: usize,
) -> Vec<usize> {
    let fallback_order = || (0..total_disks).collect::<Vec<_>>();
    if default_parity_count == 0 || default_parity_count >= total_disks {
        return fallback_order();
    }

    let data_blocks = total_disks - default_parity_count;
    let distribution_key = metadata_distribution_key(bucket, object);
    let distribution = FileInfo::new(&distribution_key, data_blocks, default_parity_count)
        .erasure
        .distribution;
    if distribution.len() != total_disks {
        return fallback_order();
    }

    let mut order = Vec::with_capacity(total_disks);
    for block_index in 1..=data_blocks {
        let Some(disk_index) = distribution
            .iter()
            .position(|distributed_block| *distributed_block == block_index)
        else {
            return fallback_order();
        };
        order.push(disk_index);
    }
    order.extend(
        distribution
            .iter()
            .enumerate()
            .filter_map(|(disk_index, block_index)| (*block_index > data_blocks).then_some(disk_index)),
    );
    order
}
use tokio::io::{AsyncRead, ReadBuf};
use tokio::sync::{Mutex, RwLock, oneshot};
use tokio::task::JoinSet;

pub(in crate::set_disk) const EVENT_SET_DISK_READ: &str = "set_disk_read";
pub(in crate::set_disk) const ENV_RUSTFS_GET_DATA_BLOCKS_FIRST_READER_SETUP: &str = "RUSTFS_GET_DATA_BLOCKS_FIRST_READER_SETUP";
const ENV_RUSTFS_GET_METADATA_READ_VERSION_COALESCE: &str = "RUSTFS_GET_METADATA_READ_VERSION_COALESCE";
const ENV_RUSTFS_GET_METADATA_READ_VERSION_COALESCE_DELAY_MICROS: &str = "RUSTFS_GET_METADATA_READ_VERSION_COALESCE_DELAY_MICROS";
const DEFAULT_GET_METADATA_READ_VERSION_COALESCE_DELAY_MICROS: u64 = 200;
const METRIC_GET_METADATA_READ_VERSION_COALESCER_TOTAL: &str = "rustfs_get_metadata_read_version_coalescer_total";
pub(in crate::set_disk) const ENV_RUSTFS_PUT_RENAME_EARLY_ACK_ENABLE: &str = "RUSTFS_PUT_RENAME_EARLY_ACK_ENABLE";
/// Default reader-setup strategy for the GET read path (rustfs/backlog#1215,
/// #1159, #923).
///
/// `true` means "data-blocks-first" is the default for every full-object GET:
/// the bitrot reader setup schedules only the `data_shards` data blocks up
/// front and *defers* the parity shards (see [`BitrotReaderSetupStrategy`] and
/// `DeferredReaderStripeHandle`). Parity reads are engaged lazily, only when a
/// data shard turns out to be missing/corrupt and reconstruction needs them.
///
/// Why this is the default (do NOT flip it back to `false` here):
/// - It is the deliberate, already-rolled-out direction from backlog#1159/#923.
///   deferred-parity is now the full-object GET default path, not an
///   experiment; changing this constant to `false` would silently revert that
///   rollout for every deployment that has not set the env var.
///
/// Known trade-off (the reason this constant carries a hazard note at all):
/// - Deferring parity means the parity disks are engaged *late*. A data disk
///   that is slow-but-not-dead (high tail latency, not an outright failure)
///   therefore holds up the read longer than the all-shards strategy would,
///   because the faster parity shards are not raced against it until a data
///   shard is declared missing. This can raise GET p99 on clusters with a
///   chronically slow data drive. `MetadataFanoutObservation` / the deferred
///   stripe handles are where a "slow data disk engaged deferred parity" signal
///   would be recorded.
///
/// Rollback switch: set `RUSTFS_GET_DATA_BLOCKS_FIRST_READER_SETUP=false` (see
/// [`ENV_RUSTFS_GET_DATA_BLOCKS_FIRST_READER_SETUP`]) to restore the
/// all-shards-up-front behaviour for a deployment without changing this
/// default. This is an operational escape hatch for the tail-latency case
/// above; it is intentionally an env override, not a code change.
const DEFAULT_RUSTFS_GET_DATA_BLOCKS_FIRST_READER_SETUP: bool = true;
pub(in crate::set_disk) const ENV_RUSTFS_GET_CODEC_STREAMING_DATA_BLOCKS_FIRST_READER_SETUP: &str =
    "RUSTFS_GET_CODEC_STREAMING_DATA_BLOCKS_FIRST_READER_SETUP";
pub(in crate::set_disk) const SLOW_OBJECT_READ_LOG_THRESHOLD: Duration = Duration::from_secs(5);
pub(in crate::set_disk) const READ_REPAIR_HEAL_DEDUP_TTL: Duration = Duration::from_secs(60);
pub(in crate::set_disk) const READ_REPAIR_HEAL_DEDUP_MAX_ENTRIES: usize = 4096;

pub(in crate::set_disk) static READ_REPAIR_HEAL_CACHE: OnceLock<RwLock<HashMap<ReadRepairHealCacheKey, Instant>>> =
    OnceLock::new();

pub(in crate::set_disk) enum GetCodecStreamingReaderBuildOutcome {
    Reader(Box<dyn AsyncRead + Unpin + Send + Sync>),
    Fallback(GetCodecStreamingFallbackReason),
}

#[allow(dead_code, reason = "asserted by this file's tests (backlog#1823)")]
pub(in crate::set_disk) struct MultipartCodecStreamingReader {
    pub(in crate::set_disk) readers: VecDeque<Box<dyn AsyncRead + Unpin + Send + Sync>>,
}

impl MultipartCodecStreamingReader {
    #[allow(dead_code, reason = "asserted by this file's tests (backlog#1823)")]
    pub(in crate::set_disk) fn new(readers: Vec<Box<dyn AsyncRead + Unpin + Send + Sync>>) -> Self {
        Self {
            readers: VecDeque::from(readers),
        }
    }
}

impl AsyncRead for MultipartCodecStreamingReader {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }

        loop {
            let Some(reader) = self.readers.front_mut() else {
                return Poll::Ready(Ok(()));
            };
            let filled_before = buf.filled().len();
            match Pin::new(reader).poll_read(cx, buf) {
                Poll::Ready(Ok(())) if buf.filled().len() == filled_before => {
                    self.readers.pop_front();
                }
                result => return result,
            }
        }
    }
}

pub(in crate::set_disk) fn codec_streaming_reader_setup_fallback_reason(
    missing_shards: usize,
) -> Option<GetCodecStreamingFallbackReason> {
    (missing_shards > 0).then_some(GetCodecStreamingFallbackReason::ReadQuorumNotSafe)
}

#[derive(Clone, Copy, Debug)]
pub(in crate::set_disk) struct MetadataFanoutObservation {
    pub(in crate::set_disk) outcome: &'static str,
    pub(in crate::set_disk) elapsed: Duration,
    pub(in crate::set_disk) valid: bool,
    pub(in crate::set_disk) ignored: bool,
}

impl MetadataFanoutObservation {
    pub(in crate::set_disk) fn from_file_info(file_info: &FileInfo, elapsed: Duration) -> Self {
        if file_info_is_valid_for_metadata(file_info) {
            Self {
                outcome: GET_METADATA_RESPONSE_VALID,
                elapsed,
                valid: true,
                ignored: false,
            }
        } else {
            Self {
                outcome: GET_METADATA_RESPONSE_ERROR,
                elapsed,
                valid: false,
                ignored: false,
            }
        }
    }

    pub(in crate::set_disk) fn from_error(err: &DiskError, elapsed: Duration) -> Self {
        Self {
            outcome: classify_metadata_response_error(err),
            elapsed,
            valid: false,
            ignored: is_metadata_fanout_ignored_error(err),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub(in crate::set_disk) struct MetadataFanoutDiagnostics {
    pub(in crate::set_disk) fanout_duration: Duration,
    pub(in crate::set_disk) observations: Vec<MetadataFanoutObservation>,
}

impl MetadataFanoutDiagnostics {
    pub(in crate::set_disk) fn new(fanout_duration: Duration, observations: Vec<MetadataFanoutObservation>) -> Self {
        Self {
            fanout_duration,
            observations,
        }
    }

    pub(in crate::set_disk) fn total_responses(&self) -> usize {
        self.observations.len()
    }

    pub(in crate::set_disk) fn valid_responses(&self) -> usize {
        self.observations.iter().filter(|observation| observation.valid).count()
    }

    pub(in crate::set_disk) fn ignored_responses(&self) -> usize {
        self.observations.iter().filter(|observation| observation.ignored).count()
    }

    pub(in crate::set_disk) fn non_valid_responses(&self) -> usize {
        self.total_responses().saturating_sub(self.valid_responses())
    }

    pub(in crate::set_disk) fn first_response_latency(&self) -> Option<Duration> {
        self.observations.iter().map(|observation| observation.elapsed).min()
    }

    pub(in crate::set_disk) fn first_valid_response_latency(&self) -> Option<Duration> {
        self.observations
            .iter()
            .filter(|observation| observation.valid)
            .map(|observation| observation.elapsed)
            .min()
    }

    pub(in crate::set_disk) fn slowest_response_latency(&self) -> Option<Duration> {
        self.observations.iter().map(|observation| observation.elapsed).max()
    }

    pub(in crate::set_disk) fn quorum_candidate_latency(&self, read_quorum: usize) -> Option<Duration> {
        if read_quorum == 0 {
            return Some(Duration::ZERO);
        }

        let mut valid_latencies = self
            .observations
            .iter()
            .filter(|observation| observation.valid)
            .map(|observation| observation.elapsed)
            .collect::<Vec<_>>();
        valid_latencies.sort_unstable();
        valid_latencies.get(read_quorum.saturating_sub(1)).copied()
    }

    pub(in crate::set_disk) fn record(&self, path: &'static str) {
        rustfs_io_metrics::record_get_object_metadata_fanout_duration(path, self.fanout_duration.as_secs_f64());
        if let Some(latency) = self.first_response_latency() {
            rustfs_io_metrics::record_get_object_first_metadata_response_latency(path, latency.as_secs_f64());
        }
        if let Some(latency) = self.first_valid_response_latency() {
            rustfs_io_metrics::record_get_object_first_valid_metadata_response_latency(path, latency.as_secs_f64());
        }
        if let Some(latency) = self.slowest_response_latency() {
            rustfs_io_metrics::record_get_object_slowest_metadata_response_latency(path, latency.as_secs_f64());
        }
        rustfs_io_metrics::record_get_object_metadata_fanout_shape(
            path,
            self.total_responses(),
            self.valid_responses(),
            self.ignored_responses(),
            self.non_valid_responses(),
        );
        for observation in &self.observations {
            rustfs_io_metrics::record_get_object_metadata_response(path, observation.outcome);
        }
    }

    pub(in crate::set_disk) fn record_quorum_candidate_latency(&self, path: &'static str, read_quorum: usize) {
        if let Some(latency) = self.quorum_candidate_latency(read_quorum) {
            rustfs_io_metrics::record_get_object_quorum_reached_latency(path, latency.as_secs_f64());
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::set_disk) struct MetadataEarlyStopDecision {
    pub(in crate::set_disk) reason: &'static str,
}

#[derive(Clone, Debug)]
pub(in crate::set_disk) struct MetadataQuorumAccumulator {
    pub(in crate::set_disk) total_disks: usize,
    pub(in crate::set_disk) default_parity_count: usize,
    pub(in crate::set_disk) allow_early_stop: bool,
    pub(in crate::set_disk) valid_responses: usize,
    pub(in crate::set_disk) not_found_responses: usize,
    pub(in crate::set_disk) version_not_found_responses: usize,
    pub(in crate::set_disk) ignored_errors: usize,
    pub(in crate::set_disk) hard_errors: usize,
    pub(in crate::set_disk) candidate: Option<FileInfo>,
    pub(in crate::set_disk) candidate_votes: usize,
    pub(in crate::set_disk) conflicting_metadata: bool,
    pub(in crate::set_disk) delete_marker_seen: bool,
    pub(in crate::set_disk) delete_marker_candidates: Vec<(FileInfo, usize)>,
    pub(in crate::set_disk) delete_marker_votes: usize,
    pub(in crate::set_disk) requested_version_id: String,
    pub(in crate::set_disk) matching_version_votes: usize,
}

impl MetadataQuorumAccumulator {
    pub(in crate::set_disk) fn new(total_disks: usize, default_parity_count: usize, allow_early_stop: bool) -> Self {
        Self {
            total_disks,
            default_parity_count,
            allow_early_stop,
            valid_responses: 0,
            not_found_responses: 0,
            version_not_found_responses: 0,
            ignored_errors: 0,
            hard_errors: 0,
            candidate: None,
            candidate_votes: 0,
            conflicting_metadata: false,
            delete_marker_seen: false,
            delete_marker_candidates: Vec::new(),
            delete_marker_votes: 0,
            requested_version_id: String::new(),
            matching_version_votes: 0,
        }
    }

    pub(in crate::set_disk) fn with_requested_version_id(mut self, version_id: &str) -> Self {
        self.requested_version_id = version_id.to_string();
        self
    }

    pub(in crate::set_disk) fn observe_file_info(&mut self, file_info: &FileInfo) {
        if !file_info_is_valid_for_metadata(file_info) {
            self.hard_errors = self.hard_errors.saturating_add(1);
            return;
        }

        self.valid_responses = self.valid_responses.saturating_add(1);

        // Track version match for versioned requests
        if !self.requested_version_id.is_empty()
            && let Some(ref vid) = file_info.version_id
            && vid.to_string() == self.requested_version_id
        {
            self.matching_version_votes = self.matching_version_votes.saturating_add(1);
        }

        if file_info.is_canonical_delete_marker() {
            self.delete_marker_seen = true;
            if let Some((_, votes)) = self
                .delete_marker_candidates
                .iter_mut()
                .find(|(candidate, _)| metadata_early_stop_candidate_matches(candidate, file_info))
            {
                *votes = votes.saturating_add(1);
            } else {
                self.delete_marker_candidates.push((file_info.clone(), 1));
            }
            self.delete_marker_votes = self
                .delete_marker_candidates
                .iter()
                .map(|(_, votes)| *votes)
                .max()
                .unwrap_or_default();
            self.conflicting_metadata |= self.delete_marker_candidates.len() > 1;
            return;
        }

        match &self.candidate {
            Some(candidate) if metadata_early_stop_candidate_matches(candidate, file_info) => {
                self.candidate_votes = self.candidate_votes.saturating_add(1);
            }
            Some(_) => {
                self.conflicting_metadata = true;
            }
            None => {
                self.candidate = Some(file_info.clone());
                self.candidate_votes = 1;
            }
        }
    }

    pub(in crate::set_disk) fn observe_error(&mut self, err: &DiskError) {
        match err {
            DiskError::FileNotFound | DiskError::VolumeNotFound => {
                self.not_found_responses = self.not_found_responses.saturating_add(1);
            }
            DiskError::FileVersionNotFound => {
                self.version_not_found_responses = self.version_not_found_responses.saturating_add(1);
            }
            _ if is_metadata_fanout_ignored_error(err) => {
                self.ignored_errors = self.ignored_errors.saturating_add(1);
            }
            _ => {
                self.hard_errors = self.hard_errors.saturating_add(1);
            }
        }
    }

    pub(in crate::set_disk) fn early_stop_decision(&self) -> Option<MetadataEarlyStopDecision> {
        if !self.allow_early_stop {
            return None;
        }
        if self.delete_marker_votes >= self.default_write_quorum() {
            return Some(MetadataEarlyStopDecision {
                reason: GET_METADATA_EARLY_STOP_REASON_DELETE_MARKER,
            });
        }
        if self.conflicting_metadata
            || self.delete_marker_seen
            || self.not_found_responses > 0
            || self.version_not_found_responses > 0
            || self.hard_errors > 0
        {
            return None;
        }
        if self
            .candidate
            .as_ref()
            .and_then(|candidate| self.candidate_latest_quorum(candidate))
            .is_some_and(|latest_quorum| self.candidate_votes >= latest_quorum)
        {
            return Some(MetadataEarlyStopDecision {
                reason: GET_METADATA_EARLY_STOP_REASON_VALID_QUORUM,
            });
        }
        None
    }

    /// Check if a versioned request can early-stop because the requested
    /// version_id has reached quorum across disks.
    pub(in crate::set_disk) fn version_early_stop_decision(&self) -> Option<MetadataEarlyStopDecision> {
        if !self.allow_early_stop {
            return None;
        }
        if self.requested_version_id.is_empty() {
            return None;
        }
        if self.conflicting_metadata
            || self.delete_marker_seen
            || self.not_found_responses > 0
            || self.version_not_found_responses > 0
            || self.hard_errors > 0
        {
            return None;
        }
        if self.matching_version_votes >= self.read_quorum_for_version() {
            return Some(MetadataEarlyStopDecision {
                reason: GET_METADATA_EARLY_STOP_REASON_VERSION_MATCH_QUORUM,
            });
        }
        None
    }

    pub(in crate::set_disk) fn can_still_reach_early_stop_with_pending(&self, pending: usize) -> bool {
        if !self.allow_early_stop {
            return false;
        }
        if self.delete_marker_votes.saturating_add(pending) >= self.default_write_quorum() {
            return true;
        }
        if self.conflicting_metadata
            || self.delete_marker_seen
            || self.not_found_responses > 0
            || self.version_not_found_responses > 0
            || self.hard_errors > 0
        {
            return false;
        }
        if !self.requested_version_id.is_empty()
            && self.matching_version_votes.saturating_add(pending) >= self.read_quorum_for_version()
        {
            return true;
        }
        match &self.candidate {
            Some(candidate) => self
                .candidate_latest_quorum(candidate)
                .is_some_and(|latest_quorum| self.candidate_votes.saturating_add(pending) >= latest_quorum),
            None => pending >= self.default_write_quorum(),
        }
    }

    /// Compute the read quorum threshold for version-aware early-stop.
    /// Uses `total_disks / 2` (like `missing_response_quorum`) when
    /// `default_parity_count` is set, otherwise requires all disks.
    pub(in crate::set_disk) fn read_quorum_for_version(&self) -> usize {
        self.missing_response_quorum()
    }

    pub(in crate::set_disk) fn final_miss_reason(&self) -> &'static str {
        if !self.allow_early_stop {
            return GET_METADATA_EARLY_STOP_REASON_UNSAFE_REQUEST;
        }
        if self.conflicting_metadata {
            return GET_METADATA_EARLY_STOP_REASON_CONFLICTING_METADATA;
        }
        if self.delete_marker_seen {
            return GET_METADATA_EARLY_STOP_REASON_DELETE_MARKER;
        }
        let missing_response_quorum = self.missing_response_quorum();
        if self.version_not_found_responses >= missing_response_quorum {
            return GET_METADATA_EARLY_STOP_REASON_VERSION_NOT_FOUND;
        }
        if self.not_found_responses >= missing_response_quorum {
            return GET_METADATA_EARLY_STOP_REASON_NOT_FOUND;
        }
        if self.hard_errors > 0 {
            return GET_METADATA_EARLY_STOP_REASON_ERROR;
        }
        if self.ignored_errors > 0 {
            return GET_METADATA_EARLY_STOP_REASON_INSUFFICIENT_QUORUM;
        }
        GET_METADATA_EARLY_STOP_REASON_INSUFFICIENT_QUORUM
    }

    pub(in crate::set_disk) fn candidate_latest_quorum(&self, candidate: &FileInfo) -> Option<usize> {
        if self.default_parity_count == 0 {
            return Some(self.total_disks);
        }
        if candidate.is_canonical_delete_marker() || candidate.size == 0 || candidate.erasure.parity_blocks >= self.total_disks {
            return None;
        }
        let data_blocks = candidate.erasure.data_blocks;
        Some(if data_blocks == candidate.erasure.parity_blocks {
            data_blocks.saturating_add(1)
        } else {
            data_blocks
        })
    }

    pub(crate) fn default_write_quorum(&self) -> usize {
        if self.default_parity_count == 0 || self.default_parity_count >= self.total_disks {
            return self.total_disks;
        }
        let data_blocks = self.total_disks.saturating_sub(self.default_parity_count);
        if data_blocks == self.default_parity_count {
            data_blocks.saturating_add(1)
        } else {
            data_blocks
        }
    }

    pub(in crate::set_disk) fn missing_response_quorum(&self) -> usize {
        if self.default_parity_count == 0 || self.default_parity_count >= self.total_disks {
            self.total_disks
        } else {
            self.total_disks / 2
        }
    }
}

#[derive(Clone, Debug)]
pub(in crate::set_disk) enum MetadataCacheLookup {
    Hit(Arc<GetObjectMetadataCacheEntry>),
    Miss,
    RejectedInsufficientQuorum,
}

pub(in crate::set_disk) fn metadata_early_stop_candidate_matches(left: &FileInfo, right: &FileInfo) -> bool {
    left.volume == right.volume
        && left.name == right.name
        && left.version_id == right.version_id
        && left.is_latest == right.is_latest
        && left.deleted == right.deleted
        && left.mark_deleted == right.mark_deleted
        && left.transition_status == right.transition_status
        && left.transitioned_objname == right.transitioned_objname
        && left.transition_tier == right.transition_tier
        && left.transition_version_id == right.transition_version_id
        && left.transition_version == right.transition_version
        && left.transition_version_state == right.transition_version_state
        && left.expire_restored == right.expire_restored
        && left.size == right.size
        && left.mod_time == right.mod_time
        && left.mode == right.mode
        && left.written_by_version == right.written_by_version
        && left.metadata == right.metadata
        && left.replication_state_internal == right.replication_state_internal
        && left.parts == right.parts
        && left.checksum == right.checksum
        && left.versioned == right.versioned
        && left.num_versions == right.num_versions
        && left.successor_mod_time == right.successor_mod_time
        && left.data_dir == right.data_dir
        && left.erasure.algorithm == right.erasure.algorithm
        && left.erasure.data_blocks == right.erasure.data_blocks
        && left.erasure.parity_blocks == right.erasure.parity_blocks
        && left.erasure.block_size == right.erasure.block_size
        && left.erasure.distribution == right.erasure.distribution
}

pub(in crate::set_disk) async fn data_read_early_stop_inline_body_miss_reason(
    bucket: &str,
    object: &str,
    candidate: &FileInfo,
    parts_metadata: &[FileInfo],
    disks: &[Option<DiskStore>],
) -> Option<&'static str> {
    if let Some(reason) = data_read_early_stop_inline_candidate_miss_reason(candidate) {
        return Some(reason);
    }

    let Ok(erasure) = coding::Erasure::try_new_with_options(
        candidate.erasure.data_blocks,
        candidate.erasure.parity_blocks,
        candidate.erasure.block_size,
        candidate.uses_legacy_checksum,
    ) else {
        return Some(GET_METADATA_EARLY_STOP_REASON_DATA_READ_INLINE_GEOMETRY);
    };
    let data_files =
        match collect_inline_data_shard_fileinfos_by_index_or_reason(parts_metadata, candidate, erasure.data_shards, |index| {
            disks.get(index).is_some_and(Option::is_some)
        }) {
            Ok(data_files) => data_files,
            Err(reason) => return Some(reason),
        };

    let Some(part) = candidate.parts.first() else {
        return Some(GET_METADATA_EARLY_STOP_REASON_DATA_READ_INLINE_PART_SHAPE);
    };
    let Ok(object_size) = usize::try_from(candidate.size) else {
        return Some(GET_METADATA_EARLY_STOP_REASON_DATA_READ_INLINE_SIZE);
    };
    let checksum_info = candidate.erasure.get_checksum_info(part.number);
    let checksum_algo = if candidate.uses_legacy_checksum && checksum_info.algorithm == HashAlgorithm::HighwayHash256S {
        HashAlgorithm::HighwayHash256SLegacy
    } else {
        checksum_info.algorithm
    };
    let read_length = inline_erasure_shard_file_offset(
        0,
        object_size,
        object_size,
        candidate.erasure.block_size,
        erasure.data_shards,
        candidate.uses_legacy_checksum,
    );
    let shard_size = inline_erasure_shard_size(candidate.erasure.block_size, erasure.data_shards, candidate.uses_legacy_checksum);
    let Ok(mut readers) =
        build_inline_bitrot_readers_from_refs(&data_files, bucket, object, read_length, shard_size, &checksum_algo, false).await
    else {
        return Some(GET_METADATA_EARLY_STOP_REASON_DATA_READ_INLINE_BODY_VERIFY);
    };

    match try_read_inline_data_shards_direct(&mut readers, erasure.data_shards, read_length, object_size).await {
        Some(body) if body.len() == object_size => None,
        _ => Some(GET_METADATA_EARLY_STOP_REASON_DATA_READ_INLINE_BODY_VERIFY),
    }
}

fn data_read_early_stop_inline_candidate_miss_reason(candidate: &FileInfo) -> Option<&'static str> {
    // `inline_data` excludes remote objects; this diagnostic reports them separately.
    if !rustfs_utils::http::contains_key_str(&candidate.metadata, rustfs_utils::http::SUFFIX_INLINE_DATA) {
        return Some(GET_METADATA_EARLY_STOP_REASON_DATA_READ_INLINE_NOT_INLINE);
    }
    if candidate.is_compressed()
        || candidate
            .metadata
            .keys()
            .any(|key| rustfs_utils::http::is_object_encryption_marker(key))
    {
        return Some(GET_METADATA_EARLY_STOP_REASON_DATA_READ_INLINE_TRANSFORMED);
    }
    if candidate.is_remote() {
        return Some(GET_METADATA_EARLY_STOP_REASON_DATA_READ_INLINE_REMOTE);
    }
    if candidate.deleted {
        return Some(GET_METADATA_EARLY_STOP_REASON_DATA_READ_INLINE_DELETED);
    }
    if candidate.size <= 0 {
        return Some(GET_METADATA_EARLY_STOP_REASON_DATA_READ_INLINE_SIZE);
    }
    if candidate.parts.len() != 1 {
        return Some(GET_METADATA_EARLY_STOP_REASON_DATA_READ_INLINE_PART_SHAPE);
    }
    if !candidate.has_valid_erasure_geometry() {
        return Some(GET_METADATA_EARLY_STOP_REASON_DATA_READ_INLINE_GEOMETRY);
    }

    let Ok(object_size) = usize::try_from(candidate.size) else {
        return Some(GET_METADATA_EARLY_STOP_REASON_DATA_READ_INLINE_SIZE);
    };
    if candidate.parts.first().is_none_or(|part| part.size != object_size) {
        return Some(GET_METADATA_EARLY_STOP_REASON_DATA_READ_INLINE_PART_SHAPE);
    }
    if !can_try_inline_data_shards_direct(object_size, candidate.erasure.block_size) {
        return Some(GET_METADATA_EARLY_STOP_REASON_DATA_READ_INLINE_SIZE);
    }
    None
}

fn data_read_inline_missing_shards_are_pending(
    candidate: &FileInfo,
    parts_metadata: &[FileInfo],
    errors: &[Option<DiskError>],
    disks: &[Option<DiskStore>],
    fanout_order: &[usize],
    scheduled_fanout_len: usize,
) -> bool {
    let Ok(erasure) = coding::Erasure::try_new_with_options(
        candidate.erasure.data_blocks,
        candidate.erasure.parity_blocks,
        candidate.erasure.block_size,
        candidate.uses_legacy_checksum,
    ) else {
        return false;
    };
    let distribution = &candidate.erasure.distribution;
    let mut data_shards_seen_or_pending = vec![false; erasure.data_shards];
    let mut missing_pending_data_shards = 0usize;

    for (disk_index, file_info) in parts_metadata.iter().enumerate() {
        let Some(&block_index) = distribution.get(disk_index) else {
            return false;
        };
        if block_index == 0 || block_index > erasure.data_shards {
            continue;
        }
        if !disks.get(disk_index).is_some_and(Option::is_some) {
            return false;
        }

        let data_slot = block_index - 1;
        if file_info.name.is_empty() {
            let scheduled_and_not_failed = fanout_order
                .get(..scheduled_fanout_len)
                .is_some_and(|scheduled_disks| scheduled_disks.contains(&disk_index))
                && errors.get(disk_index).is_some_and(Option::is_none);
            if scheduled_and_not_failed {
                data_shards_seen_or_pending[data_slot] = true;
                missing_pending_data_shards = missing_pending_data_shards.saturating_add(1);
                continue;
            }
            return false;
        }
        if file_info.erasure.index != block_index
            || !file_info.has_valid_erasure_geometry()
            || !metadata_early_stop_candidate_matches(file_info, candidate)
            || file_info.data.as_ref().is_none_or(|data| data.is_empty())
        {
            return false;
        }
        data_shards_seen_or_pending[data_slot] = true;
    }

    missing_pending_data_shards > 0 && data_shards_seen_or_pending.into_iter().all(|seen_or_pending| seen_or_pending)
}

pub(in crate::set_disk) fn classify_metadata_response_error(err: &DiskError) -> &'static str {
    match err {
        DiskError::FileNotFound | DiskError::VolumeNotFound => GET_METADATA_RESPONSE_NOT_FOUND,
        DiskError::FileVersionNotFound => GET_METADATA_RESPONSE_VERSION_NOT_FOUND,
        DiskError::DiskNotFound => GET_METADATA_RESPONSE_DISK_NOT_FOUND,
        DiskError::FileCorrupt | DiskError::CorruptedFormat | DiskError::CorruptedBackend | DiskError::OutdatedXLMeta => {
            GET_METADATA_RESPONSE_CORRUPT
        }
        DiskError::Timeout => GET_METADATA_RESPONSE_TIMEOUT,
        DiskError::FaultyDisk | DiskError::FaultyRemoteDisk => GET_METADATA_RESPONSE_IGNORED,
        _ => GET_METADATA_RESPONSE_ERROR,
    }
}

pub(in crate::set_disk) fn is_metadata_fanout_ignored_error(err: &DiskError) -> bool {
    OBJECT_OP_IGNORED_ERRS.iter().any(|ignored| ignored == err)
}

pub(in crate::set_disk) fn is_confirmed_missing_part_error(err: Option<&str>) -> bool {
    let Some(err) = err else {
        return false;
    };

    err.contains("file not found")
        || err.contains("No such file or directory")
        || err.contains("Specified part could not be found")
        || (err.starts_with("part.") && err.ends_with(" not found"))
}

pub(in crate::set_disk) fn resolve_read_part_from_responses(
    bucket: &str,
    part_meta_path: &str,
    part_number: usize,
    part_idx: usize,
    expected_part_count: usize,
    responses: &[Option<Vec<ObjectPartInfo>>],
    read_quorum: usize,
) -> disk::error::Result<ObjectPartInfo> {
    let mut part_quorum: HashMap<(&str, usize, usize, i64), (usize, &ObjectPartInfo)> = HashMap::new();
    let mut present_count = 0usize;
    let mut missing_count = 0usize;
    let mut transient_error_count = 0usize;
    let mut mismatched_response_count = 0usize;
    for response in responses.iter() {
        let Some(parts) = response else {
            transient_error_count += 1;
            continue;
        };

        if parts.len() != expected_part_count {
            mismatched_response_count += 1;
            continue;
        }

        if !parts[part_idx].etag.is_empty() {
            present_count += 1;
            let part = &parts[part_idx];
            let key = (part.etag.as_str(), part.number, part.size, part.actual_size);
            let (count, _) = part_quorum.entry(key).or_insert((0, part));
            *count += 1;
            continue;
        }

        if is_confirmed_missing_part_error(parts[part_idx].error.as_deref()) {
            missing_count += 1;
        } else {
            transient_error_count += 1;
        }
    }

    let max_part = part_quorum.values().max_by_key(|(count, _)| count);
    let max_quorum = max_part.map_or(0, |(count, _)| *count).max(missing_count);
    if let Some((count, part)) = max_part
        && *count >= read_quorum
    {
        return Ok((*part).clone());
    }

    if missing_count >= read_quorum {
        return Ok(ObjectPartInfo {
            number: part_number,
            error: Some(format!("part.{part_number} not found")),
            ..Default::default()
        });
    }

    if issue3031_diag_enabled() {
        warn!(
            target: "rustfs_ecstore::set_disk",
            bucket = %bucket,
            part_meta_path = %part_meta_path,
            part_id = part_number,
            read_quorum = read_quorum,
            max_quorum = max_quorum,
            disk_response_count = responses.len(),
            present_count = present_count,
            missing_count = missing_count,
            transient_error_count = transient_error_count,
            mismatched_response_count = mismatched_response_count,
            "issue3031_read_parts_part_quorum"
        );
    }

    Err(DiskError::ErasureReadQuorum)
}

pub(in crate::set_disk) fn shard_read_costs_for_disks(disks: &[Option<DiskStore>]) -> Vec<ShardReadCost> {
    if disks.is_empty() {
        return Vec::new();
    }

    let local_endpoint_hosts = local_endpoint_hosts_for_shard_costs();
    disks
        .iter()
        .map(|disk| shard_read_cost_for_disk(disk.as_ref(), local_endpoint_hosts))
        .collect()
}

pub(in crate::set_disk) fn shard_read_cost_for_disk(disk: Option<&DiskStore>, local_endpoint_hosts: &[String]) -> ShardReadCost {
    match disk {
        Some(disk) if disk.is_local() => ShardReadCost::Local,
        Some(disk) => shard_read_cost_for_endpoint(false, &disk.host_name(), local_endpoint_hosts),
        None => ShardReadCost::Unknown,
    }
}

pub(in crate::set_disk) fn shard_read_cost_for_endpoint(
    is_local: bool,
    host_name: &str,
    local_endpoint_hosts: &[String],
) -> ShardReadCost {
    if is_local {
        return ShardReadCost::Local;
    }

    if !host_name.is_empty() && local_endpoint_hosts.iter().any(|host| host == host_name) {
        return ShardReadCost::SameNode;
    }

    ShardReadCost::Remote
}

pub(in crate::set_disk) fn local_endpoint_hosts_for_shard_costs() -> &'static [String] {
    // Endpoint pools are immutable after startup, so build the host list once
    // instead of walking every pool on each read. Do not cache the empty
    // pre-startup answer: only memoize once the pools are published.
    static LOCAL_ENDPOINT_HOSTS: std::sync::OnceLock<Vec<String>> = std::sync::OnceLock::new();

    if let Some(hosts) = LOCAL_ENDPOINT_HOSTS.get() {
        return hosts;
    }

    let Some(endpoint_pools) = runtime_sources::endpoint_pools() else {
        return &[];
    };

    let mut hosts = Vec::new();
    for pool in endpoint_pools.as_ref() {
        for endpoint in pool.endpoints.as_ref() {
            if !endpoint.is_local {
                continue;
            }

            let host = endpoint.host_port();
            if !host.is_empty() && !hosts.contains(&host) {
                hosts.push(host);
            }
        }
    }
    LOCAL_ENDPOINT_HOSTS.get_or_init(|| hosts)
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(in crate::set_disk) struct ReadRepairHealCacheKey {
    pub(in crate::set_disk) bucket: String,
    pub(in crate::set_disk) object: String,
    pub(in crate::set_disk) version_id: Option<String>,
    pub(in crate::set_disk) pool_index: usize,
    pub(in crate::set_disk) set_index: usize,
}

impl ReadRepairHealCacheKey {
    pub(in crate::set_disk) fn new(
        bucket: &str,
        object: &str,
        version_id: Option<&str>,
        pool_index: usize,
        set_index: usize,
    ) -> Self {
        Self {
            bucket: bucket.to_string(),
            object: object.to_string(),
            version_id: version_id.filter(|value| !value.is_empty()).map(str::to_string),
            pool_index,
            set_index,
        }
    }
}

pub(in crate::set_disk) fn resolved_read_repair_version_id(fi: &FileInfo, requested_version_id: Option<&str>) -> Option<String> {
    fi.version_id
        .as_ref()
        .map(ToString::to_string)
        .or_else(|| requested_version_id.filter(|value| !value.is_empty()).map(str::to_string))
}

pub(in crate::set_disk) async fn reserve_read_repair_heal(
    bucket: &str,
    object: &str,
    version_id: Option<&str>,
    pool_index: usize,
    set_index: usize,
) -> Option<ReadRepairHealCacheKey> {
    let key = ReadRepairHealCacheKey::new(bucket, object, version_id, pool_index, set_index);
    let now = Instant::now();
    let cache = READ_REPAIR_HEAL_CACHE.get_or_init(|| RwLock::new(HashMap::new()));

    {
        let cache = cache.read().await;
        if cache
            .get(&key)
            .is_some_and(|seen_at| now.saturating_duration_since(*seen_at) <= READ_REPAIR_HEAL_DEDUP_TTL)
        {
            return None;
        }
    }

    let mut cache = cache.write().await;
    if cache
        .get(&key)
        .is_some_and(|seen_at| now.saturating_duration_since(*seen_at) <= READ_REPAIR_HEAL_DEDUP_TTL)
    {
        return None;
    }

    if cache.len() >= READ_REPAIR_HEAL_DEDUP_MAX_ENTRIES {
        cache.retain(|_, seen_at| now.saturating_duration_since(*seen_at) <= READ_REPAIR_HEAL_DEDUP_TTL);
    }
    if cache.len() >= READ_REPAIR_HEAL_DEDUP_MAX_ENTRIES
        && let Some(oldest_key) = cache.iter().min_by_key(|(_, seen_at)| **seen_at).map(|(key, _)| key.clone())
    {
        cache.remove(&oldest_key);
    }
    cache.insert(key.clone(), now);
    Some(key)
}

pub(in crate::set_disk) async fn release_read_repair_heal_reservation(key: &ReadRepairHealCacheKey) {
    if let Some(cache) = READ_REPAIR_HEAL_CACHE.get() {
        cache.write().await.remove(key);
    }
}

pub(in crate::set_disk) fn record_read_repair_dedup(reason: &'static str) {
    // `reason` is already `&'static str`; the macro takes it directly, so no
    // per-call `String` allocation.
    counter!("rustfs_heal_read_repair_dedup_total", "reason" => reason).increment(1);
}

pub(in crate::set_disk) enum ReadRepairAdmissionOutcome {
    Response(HealAdmissionResult),
    Failed(String),
}

pub(in crate::set_disk) type ReadRepairAdmissionFuture = Pin<Box<dyn Future<Output = ReadRepairAdmissionOutcome> + Send>>;
pub(in crate::set_disk) type ReadRepairAdmissionSubmitter =
    fn(rustfs_common::heal_channel::HealChannelRequest) -> ReadRepairAdmissionFuture;

pub(in crate::set_disk) struct ReadRepairHealSubmission<'a> {
    pub(in crate::set_disk) bucket: &'a str,
    pub(in crate::set_disk) object: &'a str,
    pub(in crate::set_disk) version_id: Option<&'a str>,
    pub(in crate::set_disk) pool_index: usize,
    pub(in crate::set_disk) set_index: usize,
    pub(in crate::set_disk) part_number: Option<usize>,
    pub(in crate::set_disk) reason: &'static str,
    /// Durable MRF journal intent to file alongside the read-repair request
    /// (backlog#1894 axis A): the intent kind plus its native `Uuid`
    /// version id (the submission's string form stays display-only). Bound
    /// to the reservation — the intent is only delivered when this sighting
    /// wins the dedup TTL, so a burst of reads failing on the same object
    /// books exactly one journal record instead of one per retry. `None`
    /// keeps the historical no-intent behavior.
    pub(in crate::set_disk) mrf_intent: Option<(rustfs_common::mrf_channel::MrfKind, Option<uuid::Uuid>)>,
}

pub(in crate::set_disk) fn send_read_repair_heal_request(
    request: rustfs_common::heal_channel::HealChannelRequest,
) -> ReadRepairAdmissionFuture {
    Box::pin(async {
        match send_heal_request_with_admission(request).await {
            Ok(result) => ReadRepairAdmissionOutcome::Response(result),
            Err(err) => ReadRepairAdmissionOutcome::Failed(err),
        }
    })
}

pub(in crate::set_disk) async fn submit_read_repair_heal(
    bucket: &str,
    object: &str,
    version_id: Option<&str>,
    pool_index: usize,
    set_index: usize,
    part_number: Option<usize>,
    reason: &'static str,
) {
    submit_read_repair_heal_with_submitter(
        ReadRepairHealSubmission {
            bucket,
            object,
            version_id,
            pool_index,
            set_index,
            part_number,
            reason,
            mrf_intent: None,
        },
        send_read_repair_heal_request,
    )
    .await;
}

pub(in crate::set_disk) async fn submit_read_repair_heal_with_submitter(
    submission: ReadRepairHealSubmission<'_>,
    submitter: ReadRepairAdmissionSubmitter,
) {
    let ReadRepairHealSubmission {
        bucket,
        object,
        version_id,
        pool_index,
        set_index,
        part_number,
        reason,
        mrf_intent,
    } = submission;

    let Some(dedup_key) = reserve_read_repair_heal(bucket, object, version_id, pool_index, set_index).await else {
        record_read_repair_dedup("duplicate");
        debug!(
            bucket,
            object, part_number, pool_index, set_index, reason, "Skipped duplicate read-repair heal request"
        );
        return;
    };

    // Reservation won: this sighting owns the repair records for the object,
    // including the durable journal intent when the caller asked for one.
    if let Some((kind, version_uuid)) = mrf_intent
        && let (Ok(pool_index), Ok(set_index)) = (u32::try_from(pool_index), u32::try_from(set_index))
    {
        let scope = rustfs_common::mrf_channel::MrfScope { pool_index, set_index };
        let _ = rustfs_common::mrf_channel::try_send_mrf_intent_typed(kind, bucket, object, version_uuid, Some(scope));
    }

    let mut request = rustfs_common::heal_channel::create_heal_request_with_options(
        bucket.to_string(),
        Some(object.to_string()),
        false,
        Some(HealChannelPriority::Low),
        Some(pool_index),
        Some(set_index),
    );
    request.source = HealRequestSource::ReadRepair;
    request.object_version_id = version_id.filter(|value| !value.is_empty()).map(str::to_string);
    request.recreate_missing = Some(true);

    let request_id = request.id.clone();
    let bucket = bucket.to_string();
    let object = object.to_string();
    tokio::spawn(async move {
        match submitter(request).await {
            ReadRepairAdmissionOutcome::Response(result) if result.is_admitted() => {
                debug!(
                    bucket,
                    object,
                    part_number,
                    pool_index,
                    set_index,
                    request_id,
                    reason,
                    admission = result.result_label(),
                    "Read-repair heal request admitted"
                );
            }
            ReadRepairAdmissionOutcome::Response(result) => {
                release_read_repair_heal_reservation(&dedup_key).await;
                debug!(
                    bucket,
                    object,
                    part_number,
                    pool_index,
                    set_index,
                    request_id,
                    reason,
                    admission = result.result_label(),
                    drop_reason = result.reason_label(),
                    "Read-repair heal request not admitted"
                );
            }
            ReadRepairAdmissionOutcome::Failed(err) => {
                release_read_repair_heal_reservation(&dedup_key).await;
                debug!(
                    bucket,
                    object,
                    part_number,
                    pool_index,
                    set_index,
                    request_id,
                    reason,
                    error = %err,
                    "Read-repair heal request could not be submitted"
                );
            }
        }
    });
}

pub(in crate::set_disk) type ObjectBitrotReader = BitrotReader<ShardReader>;
pub(in crate::set_disk) type BitrotReaderTask<'a> =
    Pin<Box<dyn Future<Output = (usize, std::result::Result<Option<ObjectBitrotReader>, DiskError>)> + Send + 'a>>;

pub(in crate::set_disk) const DIRECT_MEMORY_BITROT_READER_STAGE_METRICS: BitrotReaderStageMetrics = BitrotReaderStageMetrics {
    path: GET_OBJECT_PATH_DIRECT_MEMORY,
    reader_construction_stage: GET_STAGE_READER_TASK_READER_CONSTRUCTION,
    file_open_stage: GET_STAGE_READER_TASK_FILE_OPEN,
    bitrot_reader_init_stage: GET_STAGE_READER_TASK_BITROT_READER_INIT,
};

pub(in crate::set_disk) struct BitrotReaderSetup {
    pub(in crate::set_disk) readers: Vec<Option<ObjectBitrotReader>>,
    /// Per-slot stripe handles for readers that are still unopened deferred
    /// readers. The lockstep GET decode uses them to open a parity shard
    /// aligned to the stripe where a data shard failed (backlog#923).
    pub(in crate::set_disk) deferred_stripe_handles: Vec<Option<DeferredReaderStripeHandle>>,
    pub(in crate::set_disk) errors: Vec<Option<DiskError>>,
    pub(in crate::set_disk) scheduled: Vec<bool>,
    pub(in crate::set_disk) attempted: Vec<bool>,
    pub(in crate::set_disk) ready: Vec<bool>,
    pub(in crate::set_disk) scheduled_count: usize,
    pub(in crate::set_disk) attempted_count: usize,
    pub(in crate::set_disk) ready_count: usize,
    pub(in crate::set_disk) failed_count: usize,
    pub(in crate::set_disk) deferred_count: usize,
}

#[derive(Clone, Copy)]
pub(in crate::set_disk) struct BitrotReaderSetupAttribution {
    pub(in crate::set_disk) path: &'static str,
    pub(in crate::set_disk) object_class: &'static str,
    pub(in crate::set_disk) size_bucket: &'static str,
}

#[derive(Clone, Copy)]
pub(in crate::set_disk) enum BitrotReaderSetupMode {
    ReadQuorum,
    VerifyReconstruction,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::set_disk) enum BitrotReaderSetupStrategy {
    AllShards,
    DataBlocksFirst,
    DataBlocksOnly,
}

impl BitrotReaderSetupMode {
    pub(in crate::set_disk) fn as_str(self) -> &'static str {
        match self {
            BitrotReaderSetupMode::ReadQuorum => "read_quorum",
            BitrotReaderSetupMode::VerifyReconstruction => "verify_reconstruction",
        }
    }
}

impl BitrotReaderSetupStrategy {
    pub(in crate::set_disk) fn as_str(self) -> &'static str {
        match self {
            BitrotReaderSetupStrategy::AllShards => "all_shards",
            BitrotReaderSetupStrategy::DataBlocksFirst => "data_blocks_first",
            BitrotReaderSetupStrategy::DataBlocksOnly => "data_blocks_only",
        }
    }
}

/// Resolve which bitrot reader-setup strategy a read uses.
///
/// For `ReadQuorum` (the full-object GET path) the default is data-blocks-first
/// / deferred-parity (see [`DEFAULT_RUSTFS_GET_DATA_BLOCKS_FIRST_READER_SETUP`]
/// for the rollout rationale and the tail-latency trade-off). Operators can
/// force the older all-shards-up-front behaviour per deployment by setting
/// `RUSTFS_GET_DATA_BLOCKS_FIRST_READER_SETUP=false`; the constant default must
/// stay `true` (rustfs/backlog#1215/#1159/#923).
pub(in crate::set_disk) fn get_bitrot_reader_setup_strategy(
    mode: BitrotReaderSetupMode,
    prefer_data_blocks_first: bool,
) -> BitrotReaderSetupStrategy {
    match mode {
        BitrotReaderSetupMode::ReadQuorum
            if prefer_data_blocks_first
                || rustfs_utils::get_env_bool(
                    ENV_RUSTFS_GET_DATA_BLOCKS_FIRST_READER_SETUP,
                    DEFAULT_RUSTFS_GET_DATA_BLOCKS_FIRST_READER_SETUP,
                ) =>
        {
            BitrotReaderSetupStrategy::DataBlocksFirst
        }
        BitrotReaderSetupMode::VerifyReconstruction
            if prefer_data_blocks_first
                || rustfs_utils::get_env_bool(ENV_RUSTFS_GET_CODEC_STREAMING_DATA_BLOCKS_FIRST_READER_SETUP, false) =>
        {
            BitrotReaderSetupStrategy::DataBlocksFirst
        }
        _ => BitrotReaderSetupStrategy::AllShards,
    }
}

impl BitrotReaderSetup {
    pub(in crate::set_disk) fn new(shards: usize) -> Self {
        Self {
            readers: (0..shards).map(|_| None).collect(),
            deferred_stripe_handles: (0..shards).map(|_| None).collect(),
            errors: vec![Some(DiskError::DiskNotFound); shards],
            scheduled: vec![false; shards],
            attempted: vec![false; shards],
            ready: vec![false; shards],
            scheduled_count: 0,
            attempted_count: 0,
            ready_count: 0,
            failed_count: 0,
            deferred_count: 0,
        }
    }

    pub(in crate::set_disk) fn scheduled_shards(&self) -> usize {
        self.scheduled_count
    }

    pub(in crate::set_disk) fn attempted_shards(&self) -> usize {
        self.attempted_count
    }

    pub(in crate::set_disk) fn pending_scheduled_shards(&self) -> usize {
        self.scheduled_count.saturating_sub(self.attempted_count)
    }

    pub(in crate::set_disk) fn available_shards(&self) -> usize {
        self.ready_count
    }

    pub(in crate::set_disk) fn available_data_shards(&self, data_shards: usize) -> usize {
        self.ready.iter().take(data_shards).filter(|ready| **ready).count()
    }

    pub(in crate::set_disk) fn completed_failed_shards(&self) -> usize {
        self.failed_count
    }

    pub(in crate::set_disk) fn data_shards_attempted(&self, data_shards: usize) -> bool {
        self.attempted.iter().take(data_shards).all(|attempted| *attempted)
    }

    pub(in crate::set_disk) fn reconstruction_verification_target(&self, data_shards: usize, parity_shards: usize) -> usize {
        let missing_data_sources = data_shards.saturating_sub(self.available_data_shards(data_shards));
        if missing_data_sources > 0 && missing_data_sources < parity_shards {
            data_shards.saturating_add(1).min(data_shards.saturating_add(parity_shards))
        } else {
            data_shards
        }
    }

    pub(in crate::set_disk) fn has_setup_quorum(
        &self,
        data_shards: usize,
        parity_shards: usize,
        mode: BitrotReaderSetupMode,
    ) -> bool {
        self.available_shards() >= self.setup_target(data_shards, parity_shards, mode)
    }

    pub(in crate::set_disk) fn setup_target(
        &self,
        data_shards: usize,
        parity_shards: usize,
        mode: BitrotReaderSetupMode,
    ) -> usize {
        match mode {
            BitrotReaderSetupMode::ReadQuorum => data_shards,
            BitrotReaderSetupMode::VerifyReconstruction => self.reconstruction_verification_target(data_shards, parity_shards),
        }
    }

    pub(in crate::set_disk) fn scheduling_target(
        &self,
        data_shards: usize,
        parity_shards: usize,
        mode: BitrotReaderSetupMode,
    ) -> usize {
        match mode {
            BitrotReaderSetupMode::ReadQuorum => data_shards,
            BitrotReaderSetupMode::VerifyReconstruction if !self.data_shards_attempted(data_shards) => data_shards,
            BitrotReaderSetupMode::VerifyReconstruction => self.reconstruction_verification_target(data_shards, parity_shards),
        }
    }

    pub(in crate::set_disk) fn deferred_shards(&self) -> usize {
        self.deferred_count
    }

    pub(in crate::set_disk) fn mark_scheduled(&mut self, idx: usize) -> bool {
        if self.scheduled[idx] {
            return false;
        }
        self.scheduled[idx] = true;
        self.scheduled_count = self.scheduled_count.saturating_add(1);
        true
    }

    pub(in crate::set_disk) fn apply_reader_result(
        &mut self,
        idx: usize,
        result: std::result::Result<Option<ObjectBitrotReader>, DiskError>,
    ) {
        self.attempted[idx] = true;
        self.attempted_count = self.attempted_count.saturating_add(1);
        match result {
            Ok(Some(reader)) => {
                self.readers[idx] = Some(reader);
                self.errors[idx] = None;
                self.ready[idx] = true;
                self.ready_count = self.ready_count.saturating_add(1);
            }
            Ok(None) => {
                self.readers[idx] = None;
                self.errors[idx] = Some(DiskError::DiskNotFound);
                self.ready[idx] = false;
                self.failed_count = self.failed_count.saturating_add(1);
            }
            Err(e) => {
                self.readers[idx] = None;
                self.errors[idx] = Some(e);
                self.ready[idx] = false;
                self.failed_count = self.failed_count.saturating_add(1);
            }
        }
    }

    pub(in crate::set_disk) fn retain_deferred_reader(
        &mut self,
        idx: usize,
        reader: ObjectBitrotReader,
        stripe_handle: DeferredReaderStripeHandle,
    ) {
        self.readers[idx] = Some(reader);
        self.deferred_stripe_handles[idx] = Some(stripe_handle);
        self.errors[idx] = None;
        self.deferred_count = self.deferred_count.saturating_add(1);
    }
}

#[allow(clippy::too_many_arguments)]
pub(in crate::set_disk) fn schedule_bitrot_reader_task<'a>(
    reader_tasks: &mut FuturesUnordered<BitrotReaderTask<'a>>,
    setup: &mut BitrotReaderSetup,
    idx: usize,
    files: &'a [FileInfo],
    disks: &'a [Option<DiskStore>],
    bucket: &'a str,
    object: &'a str,
    part_number: usize,
    read_offset: usize,
    read_length: usize,
    shard_size: usize,
    checksum_algo: HashAlgorithm,
    skip_verify_bitrot: bool,
    use_mmap_read: bool,
    stage_metrics: Option<BitrotReaderStageMetrics>,
) {
    if idx >= disks.len() || !setup.mark_scheduled(idx) {
        return;
    }

    let inline_data = files[idx].data.clone();
    let data_dir = files[idx].data_dir.unwrap_or_default();
    let disk = disks[idx].as_ref();
    let path = format!("{object}/{data_dir}/part.{part_number}");

    reader_tasks.push(Box::pin(async move {
        let result = create_bitrot_reader_from_bytes_with_stage_metrics(
            inline_data,
            disk,
            bucket,
            &path,
            read_offset,
            read_length,
            shard_size,
            checksum_algo,
            skip_verify_bitrot,
            use_mmap_read,
            stage_metrics,
        )
        .await;
        (idx, result)
    }));
}

pub(in crate::set_disk) fn next_unscheduled_reader_index(
    setup: &BitrotReaderSetup,
    total_shards: usize,
    data_shards: usize,
) -> Option<usize> {
    (data_shards..total_shards)
        .chain(0..data_shards.min(total_shards))
        .find(|idx| !setup.scheduled[*idx])
}

#[allow(clippy::too_many_arguments)]
pub(in crate::set_disk) fn fill_deferred_bitrot_readers(
    setup: &mut BitrotReaderSetup,
    files: &[FileInfo],
    disks: &[Option<DiskStore>],
    bucket: &str,
    object: &str,
    part_number: usize,
    read_offset: usize,
    read_length: usize,
    shard_size: usize,
    checksum_algo: HashAlgorithm,
    skip_verify_bitrot: bool,
    use_mmap_read: bool,
    data_shards: usize,
    parity_shards: usize,
    mode: BitrotReaderSetupMode,
) {
    if !setup.has_setup_quorum(data_shards, parity_shards, mode) {
        return;
    }

    for idx in 0..disks.len() {
        if setup.attempted[idx] {
            continue;
        }

        if files[idx].data.is_none() && disks[idx].is_none() {
            continue;
        }

        let inline_data = files[idx].data.clone();
        let disk = disks[idx].clone();
        let data_dir = files[idx].data_dir.unwrap_or_default();
        let path = format!("{object}/{data_dir}/part.{part_number}");
        let (reader, stripe_handle) = create_deferred_bitrot_reader_with_stripe_handle(
            inline_data,
            disk,
            bucket,
            &path,
            read_offset,
            read_length,
            shard_size,
            checksum_algo.clone(),
            skip_verify_bitrot,
            use_mmap_read,
        );
        setup.retain_deferred_reader(idx, reader, stripe_handle);
    }

    // With the data-shards-only lockstep gate on (backlog#923), the GET decode
    // reads only the data shards while the object is healthy; a parity reader
    // is engaged on demand and must therefore stay unopened so its start
    // offset can be advanced to the failing stripe. A parity reader that was
    // opened eagerly during setup is pinned at the stripe-0 stream position
    // and could never be engaged mid-object, so swap it for an unopened
    // deferred reader carrying a stripe handle. The eager open already proved
    // the shard is reachable; no shard bytes were read from it, and the
    // ready/error bookkeeping that quorum decisions rely on is left untouched.
    // Gate off (default): keep the eagerly opened parity readers exactly as
    // before — the lockstep path reads them on every stripe.
    if !crate::erasure::coding::decode::get_lockstep_data_shards_only_enabled() {
        return;
    }
    for idx in data_shards..disks.len() {
        if setup.readers[idx].is_none() || setup.deferred_stripe_handles[idx].is_some() {
            continue;
        }

        let inline_data = files[idx].data.clone();
        let disk = disks[idx].clone();
        let data_dir = files[idx].data_dir.unwrap_or_default();
        let path = format!("{object}/{data_dir}/part.{part_number}");
        let (reader, stripe_handle) = create_deferred_bitrot_reader_with_stripe_handle(
            inline_data,
            disk,
            bucket,
            &path,
            read_offset,
            read_length,
            shard_size,
            checksum_algo.clone(),
            skip_verify_bitrot,
            use_mmap_read,
        );
        setup.readers[idx] = Some(reader);
        setup.deferred_stripe_handles[idx] = Some(stripe_handle);
    }
}

pub(in crate::set_disk) fn record_bitrot_reader_setup_fanout(
    strategy: BitrotReaderSetupStrategy,
    mode: BitrotReaderSetupMode,
    setup: &BitrotReaderSetup,
    attribution: Option<BitrotReaderSetupAttribution>,
) {
    let strategy = strategy.as_str();
    let mode = mode.as_str();
    let scheduled = setup.scheduled_shards();
    let attempted = setup.attempted_shards();
    let ready = setup.available_shards();
    let failed = setup.completed_failed_shards();
    let deferred = setup.deferred_shards();
    rustfs_io_metrics::record_get_object_reader_setup_fanout(strategy, mode, scheduled, attempted, ready, failed, deferred);
    if let Some(attribution) = attribution {
        rustfs_io_metrics::record_get_object_reader_setup_fanout_by_size(
            attribution.path,
            strategy,
            mode,
            attribution.object_class,
            attribution.size_bucket,
            scheduled,
            attempted,
            ready,
            failed,
            deferred,
        );
    }
}

pub(in crate::set_disk) fn record_bitrot_reader_setup_strategy(
    strategy: BitrotReaderSetupStrategy,
    mode: BitrotReaderSetupMode,
    attribution: Option<BitrotReaderSetupAttribution>,
) {
    let strategy = strategy.as_str();
    let mode = mode.as_str();
    rustfs_io_metrics::record_get_object_reader_setup_strategy(strategy, mode);
    if let Some(attribution) = attribution {
        rustfs_io_metrics::record_get_object_reader_setup_strategy_by_size(
            attribution.path,
            strategy,
            mode,
            attribution.object_class,
            attribution.size_bucket,
        );
    }
}

/// When all online shards are local and mmap-read is enabled, materialize
/// shard bytes with one `batch_shard_pread` instead of per-shard
/// `spawn_blocking` via `open_disk_reader`.
#[cfg(unix)]
#[allow(clippy::too_many_arguments)]
async fn try_create_bitrot_readers_via_batch_pread(
    files: &[FileInfo],
    disks: &[Option<DiskStore>],
    bucket: &str,
    object: &str,
    part_number: usize,
    read_offset: usize,
    read_length: usize,
    shard_size: usize,
    checksum_algo: HashAlgorithm,
    skip_verify_bitrot: bool,
) -> Option<BitrotReaderSetup> {
    use crate::disk::local::batch_shard_pread;
    use std::io::Cursor;

    let (adj_off, adj_len) = adjust_shard_read_params(read_offset, read_length, shard_size, &checksum_algo);
    if adj_len > object_mmap_read_max_length() {
        return None;
    }

    let mut batch_items: Vec<(usize, std::path::PathBuf, usize, usize)> = Vec::new();
    for (idx, disk_op) in disks.iter().enumerate() {
        if files.get(idx).is_some_and(|fi| fi.data.is_some()) {
            return None;
        }
        if let Some(disk) = disk_op.as_ref() {
            let data_dir = files[idx].data_dir.unwrap_or_default();
            let path_str = format!("{object}/{data_dir}/part.{part_number}");
            match disk.get_object_path_for_io_if_local(bucket, &path_str) {
                Some(Ok(p)) => batch_items.push((idx, p, adj_off, adj_len)),
                _ => return None,
            }
        }
    }

    if batch_items.is_empty() {
        return None;
    }

    let requests: Vec<_> = batch_items.iter().map(|(_, p, off, len)| (p.clone(), *off, *len)).collect();
    let batch_results = batch_shard_pread(requests).await;

    let mut setup = BitrotReaderSetup::new(disks.len());
    for (i, (idx, _, _, _)) in batch_items.iter().enumerate() {
        setup.mark_scheduled(*idx);
        match &batch_results[i] {
            Ok(bytes) => {
                let reader = BitrotReader::new(
                    ShardReader::InMemory(Cursor::new(bytes.clone())),
                    shard_size,
                    checksum_algo.clone(),
                    skip_verify_bitrot,
                );
                setup.apply_reader_result(*idx, Ok(Some(reader)));
            }
            Err(e) => {
                setup.apply_reader_result(*idx, Err(e.clone()));
            }
        }
    }

    for (idx, disk_op) in disks.iter().enumerate() {
        if setup.scheduled[idx] {
            continue;
        }
        setup.mark_scheduled(idx);
        if disk_op.is_none() {
            setup.apply_reader_result(idx, Ok(None));
        }
    }

    Some(setup)
}

#[cfg(not(unix))]
#[allow(clippy::too_many_arguments)]
async fn try_create_bitrot_readers_via_batch_pread(
    _files: &[FileInfo],
    _disks: &[Option<DiskStore>],
    _bucket: &str,
    _object: &str,
    _part_number: usize,
    _read_offset: usize,
    _read_length: usize,
    _shard_size: usize,
    _checksum_algo: HashAlgorithm,
    _skip_verify_bitrot: bool,
) -> Option<BitrotReaderSetup> {
    None
}

#[allow(clippy::too_many_arguments)]
pub(in crate::set_disk) async fn create_bitrot_readers_until_quorum_all_shards(
    files: &[FileInfo],
    disks: &[Option<DiskStore>],
    bucket: &str,
    object: &str,
    part_number: usize,
    read_offset: usize,
    read_length: usize,
    shard_size: usize,
    checksum_algo: HashAlgorithm,
    skip_verify_bitrot: bool,
    use_mmap_read: bool,
    data_shards: usize,
    parity_shards: usize,
    mode: BitrotReaderSetupMode,
    stage_metrics: Option<BitrotReaderStageMetrics>,
    attribution: Option<BitrotReaderSetupAttribution>,
) -> BitrotReaderSetup {
    let strategy = BitrotReaderSetupStrategy::AllShards;
    let mut setup = BitrotReaderSetup::new(disks.len());
    let mut reader_tasks = FuturesUnordered::new();
    let stage_metrics = stage_metrics.filter(|_| rustfs_io_metrics::get_stage_metrics_enabled());

    record_bitrot_reader_setup_strategy(strategy, mode, attribution);

    let schedule_stage_start = stage_metrics.map(|_| Instant::now());
    for (idx, disk_op) in disks.iter().enumerate() {
        setup.mark_scheduled(idx);
        let inline_data = files[idx].data.clone();
        let data_dir = files[idx].data_dir.unwrap_or_default();
        let disk = disk_op.as_ref();
        let path = format!("{object}/{data_dir}/part.{part_number}");
        let checksum_algo = checksum_algo.clone();

        reader_tasks.push(async move {
            let result = create_bitrot_reader_from_bytes_with_stage_metrics(
                inline_data,
                disk,
                bucket,
                &path,
                read_offset,
                read_length,
                shard_size,
                checksum_algo,
                skip_verify_bitrot,
                use_mmap_read,
                stage_metrics,
            )
            .await;
            (idx, result)
        });
    }
    if let Some(stage_metrics) = stage_metrics {
        record_get_stage_duration_if_enabled(stage_metrics.path, GET_STAGE_READER_SETUP_SCHEDULE, schedule_stage_start);
    }

    let wait_quorum_stage_start = stage_metrics.map(|_| Instant::now());
    while let Some((idx, result)) = reader_tasks.next().await {
        setup.apply_reader_result(idx, result);

        if setup.has_setup_quorum(data_shards, parity_shards, mode) {
            break;
        }
    }
    if let Some(stage_metrics) = stage_metrics {
        record_get_stage_duration_if_enabled(stage_metrics.path, GET_STAGE_READER_SETUP_WAIT_QUORUM, wait_quorum_stage_start);
    }

    fill_deferred_bitrot_readers(
        &mut setup,
        files,
        disks,
        bucket,
        object,
        part_number,
        read_offset,
        read_length,
        shard_size,
        checksum_algo,
        skip_verify_bitrot,
        use_mmap_read,
        data_shards,
        parity_shards,
        mode,
    );
    let drop_pending_stage_start = stage_metrics.map(|_| Instant::now());
    drop(reader_tasks);
    if let Some(stage_metrics) = stage_metrics {
        record_get_stage_duration_if_enabled(stage_metrics.path, GET_STAGE_READER_SETUP_DROP_PENDING, drop_pending_stage_start);
    }
    record_bitrot_reader_setup_fanout(strategy, mode, &setup, attribution);

    setup
}

#[allow(clippy::too_many_arguments)]
#[allow(dead_code, reason = "asserted by this file's tests (backlog#1823)")]
pub(in crate::set_disk) async fn create_bitrot_readers_until_quorum(
    files: &[FileInfo],
    disks: &[Option<DiskStore>],
    bucket: &str,
    object: &str,
    part_number: usize,
    read_offset: usize,
    read_length: usize,
    shard_size: usize,
    checksum_algo: HashAlgorithm,
    skip_verify_bitrot: bool,
    use_mmap_read: bool,
    data_shards: usize,
    parity_shards: usize,
    mode: BitrotReaderSetupMode,
    stage_metrics: Option<BitrotReaderStageMetrics>,
    attribution: Option<BitrotReaderSetupAttribution>,
) -> BitrotReaderSetup {
    create_bitrot_readers_until_quorum_with_preference(
        files,
        disks,
        bucket,
        object,
        part_number,
        read_offset,
        read_length,
        shard_size,
        checksum_algo,
        skip_verify_bitrot,
        use_mmap_read,
        data_shards,
        parity_shards,
        mode,
        false,
        stage_metrics,
        attribution,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(in crate::set_disk) async fn create_bitrot_readers_until_quorum_with_preference(
    files: &[FileInfo],
    disks: &[Option<DiskStore>],
    bucket: &str,
    object: &str,
    part_number: usize,
    read_offset: usize,
    read_length: usize,
    shard_size: usize,
    checksum_algo: HashAlgorithm,
    skip_verify_bitrot: bool,
    use_mmap_read: bool,
    data_shards: usize,
    parity_shards: usize,
    mode: BitrotReaderSetupMode,
    prefer_data_blocks_first: bool,
    stage_metrics: Option<BitrotReaderStageMetrics>,
    attribution: Option<BitrotReaderSetupAttribution>,
) -> BitrotReaderSetup {
    let strategy = get_bitrot_reader_setup_strategy(mode, prefer_data_blocks_first);

    if use_mmap_read
        && let Some(mut setup) = try_create_bitrot_readers_via_batch_pread(
            files,
            disks,
            bucket,
            object,
            part_number,
            read_offset,
            read_length,
            shard_size,
            checksum_algo.clone(),
            skip_verify_bitrot,
        )
        .await
    {
        record_bitrot_reader_setup_strategy(strategy, mode, attribution);
        fill_deferred_bitrot_readers(
            &mut setup,
            files,
            disks,
            bucket,
            object,
            part_number,
            read_offset,
            read_length,
            shard_size,
            checksum_algo,
            skip_verify_bitrot,
            use_mmap_read,
            data_shards,
            parity_shards,
            mode,
        );
        record_bitrot_reader_setup_fanout(strategy, mode, &setup, attribution);
        return setup;
    }

    if strategy == BitrotReaderSetupStrategy::AllShards {
        return create_bitrot_readers_until_quorum_all_shards(
            files,
            disks,
            bucket,
            object,
            part_number,
            read_offset,
            read_length,
            shard_size,
            checksum_algo,
            skip_verify_bitrot,
            use_mmap_read,
            data_shards,
            parity_shards,
            mode,
            stage_metrics,
            attribution,
        )
        .await;
    }

    let mut setup = BitrotReaderSetup::new(disks.len());
    let mut reader_tasks: FuturesUnordered<BitrotReaderTask<'_>> = FuturesUnordered::new();
    let total_shards = disks.len();
    let stage_metrics = stage_metrics.filter(|_| rustfs_io_metrics::get_stage_metrics_enabled());

    record_bitrot_reader_setup_strategy(strategy, mode, attribution);

    let schedule_stage_start = stage_metrics.map(|_| Instant::now());
    let initial_target = setup.setup_target(data_shards, parity_shards, mode);
    for idx in 0..initial_target.min(data_shards).min(total_shards) {
        schedule_bitrot_reader_task(
            &mut reader_tasks,
            &mut setup,
            idx,
            files,
            disks,
            bucket,
            object,
            part_number,
            read_offset,
            read_length,
            shard_size,
            checksum_algo.clone(),
            skip_verify_bitrot,
            use_mmap_read,
            stage_metrics,
        );
    }
    if let Some(stage_metrics) = stage_metrics {
        record_get_stage_duration_if_enabled(stage_metrics.path, GET_STAGE_READER_SETUP_SCHEDULE, schedule_stage_start);
    }

    let wait_quorum_stage_start = stage_metrics.map(|_| Instant::now());
    while let Some((idx, result)) = reader_tasks.next().await {
        setup.apply_reader_result(idx, result);

        if setup.has_setup_quorum(data_shards, parity_shards, mode) {
            break;
        }

        let target = setup.scheduling_target(data_shards, parity_shards, mode);
        while setup.available_shards().saturating_add(setup.pending_scheduled_shards()) < target {
            let Some(next_idx) = next_unscheduled_reader_index(&setup, total_shards, data_shards) else {
                break;
            };
            schedule_bitrot_reader_task(
                &mut reader_tasks,
                &mut setup,
                next_idx,
                files,
                disks,
                bucket,
                object,
                part_number,
                read_offset,
                read_length,
                shard_size,
                checksum_algo.clone(),
                skip_verify_bitrot,
                use_mmap_read,
                stage_metrics,
            );
        }
    }
    if let Some(stage_metrics) = stage_metrics {
        record_get_stage_duration_if_enabled(stage_metrics.path, GET_STAGE_READER_SETUP_WAIT_QUORUM, wait_quorum_stage_start);
    }

    fill_deferred_bitrot_readers(
        &mut setup,
        files,
        disks,
        bucket,
        object,
        part_number,
        read_offset,
        read_length,
        shard_size,
        checksum_algo,
        skip_verify_bitrot,
        use_mmap_read,
        data_shards,
        parity_shards,
        mode,
    );
    let drop_pending_stage_start = stage_metrics.map(|_| Instant::now());
    drop(reader_tasks);
    if let Some(stage_metrics) = stage_metrics {
        record_get_stage_duration_if_enabled(stage_metrics.path, GET_STAGE_READER_SETUP_DROP_PENDING, drop_pending_stage_start);
    }
    record_bitrot_reader_setup_fanout(strategy, mode, &setup, attribution);

    setup
}

#[allow(clippy::too_many_arguments)]
pub(in crate::set_disk) async fn create_data_block_bitrot_readers(
    files: &[FileInfo],
    disks: &[Option<DiskStore>],
    bucket: &str,
    object: &str,
    part_number: usize,
    read_offset: usize,
    read_length: usize,
    shard_size: usize,
    checksum_algo: HashAlgorithm,
    skip_verify_bitrot: bool,
    use_mmap_read: bool,
    data_shards: usize,
) -> BitrotReaderSetup {
    let strategy = BitrotReaderSetupStrategy::DataBlocksOnly;
    let total_shards = disks.len().min(files.len());
    let mut setup = BitrotReaderSetup::new(total_shards);
    let mut reader_tasks: FuturesUnordered<BitrotReaderTask<'_>> = FuturesUnordered::new();
    let stage_metrics_enabled = rustfs_io_metrics::get_stage_metrics_enabled();
    let reader_stage_metrics = stage_metrics_enabled.then_some(DIRECT_MEMORY_BITROT_READER_STAGE_METRICS);

    rustfs_io_metrics::record_get_object_reader_setup_strategy(strategy.as_str(), BitrotReaderSetupMode::ReadQuorum.as_str());

    let schedule_stage_start = get_stage_timer_if_enabled(stage_metrics_enabled);
    for idx in 0..data_shards.min(total_shards) {
        schedule_bitrot_reader_task(
            &mut reader_tasks,
            &mut setup,
            idx,
            files,
            disks,
            bucket,
            object,
            part_number,
            read_offset,
            read_length,
            shard_size,
            checksum_algo.clone(),
            skip_verify_bitrot,
            use_mmap_read,
            reader_stage_metrics,
        );
    }
    record_get_stage_duration_if_enabled(GET_OBJECT_PATH_DIRECT_MEMORY, GET_STAGE_READER_SETUP_SCHEDULE, schedule_stage_start);

    let wait_quorum_stage_start = get_stage_timer_if_enabled(stage_metrics_enabled);
    while let Some((idx, result)) = reader_tasks.next().await {
        setup.apply_reader_result(idx, result);
        if setup.available_data_shards(data_shards) >= data_shards {
            break;
        }
    }
    record_get_stage_duration_if_enabled(
        GET_OBJECT_PATH_DIRECT_MEMORY,
        GET_STAGE_READER_SETUP_WAIT_QUORUM,
        wait_quorum_stage_start,
    );

    let drop_pending_stage_start = get_stage_timer_if_enabled(stage_metrics_enabled);
    drop(reader_tasks);
    record_get_stage_duration_if_enabled(
        GET_OBJECT_PATH_DIRECT_MEMORY,
        GET_STAGE_READER_SETUP_DROP_PENDING,
        drop_pending_stage_start,
    );

    // The direct-memory path only consumes the data shard readers. If one of
    // them is missing, the caller falls back to the regular GET path.
    record_bitrot_reader_setup_fanout(strategy, BitrotReaderSetupMode::ReadQuorum, &setup, None);

    setup
}

#[allow(dead_code, reason = "asserted by this file's tests (backlog#1823)")]
pub(in crate::set_disk) async fn collect_read_multiple_results<F>(
    tasks: Vec<F>,
    read_quorum: usize,
) -> std::result::Result<(Vec<Option<Vec<ReadMultipleResp>>>, Vec<Option<DiskError>>), ()>
where
    F: Future<Output = disk::error::Result<Vec<ReadMultipleResp>>> + Send + 'static,
{
    let mut responses = vec![None; tasks.len()];
    let mut errors = vec![Some(DiskError::DiskNotFound); tasks.len()];
    let mut successful_responses = 0usize;
    let mut pending = tasks.len();
    let mut join_set = JoinSet::new();

    for (index, task) in tasks.into_iter().enumerate() {
        join_set.spawn(async move { (index, task.await) });
    }

    while let Some(join_result) = join_set.join_next().await {
        pending = pending.saturating_sub(1);

        match join_result {
            Ok((index, Ok(resp))) => {
                responses[index] = Some(resp);
                errors[index] = None;
                successful_responses += 1;
            }
            Ok((index, Err(err))) => {
                errors[index] = Some(err);
            }
            Err(_) => {}
        }

        if successful_responses + pending < read_quorum {
            return Err(());
        }
    }

    Ok((responses, errors))
}

pub(in crate::set_disk) async fn collect_read_parts_results<F>(
    tasks: Vec<F>,
    read_quorum: usize,
) -> std::result::Result<(Vec<Option<Vec<ObjectPartInfo>>>, Vec<Option<DiskError>>), ()>
where
    F: Future<Output = disk::error::Result<Vec<ObjectPartInfo>>> + Send + 'static,
{
    let mut responses = vec![None; tasks.len()];
    let mut errors = vec![Some(DiskError::DiskNotFound); tasks.len()];
    let mut successful_responses = 0usize;
    let mut pending = tasks.len();
    let mut join_set = JoinSet::new();

    for (index, task) in tasks.into_iter().enumerate() {
        join_set.spawn(async move { (index, task.await) });
    }

    while let Some(join_result) = join_set.join_next().await {
        pending = pending.saturating_sub(1);

        match join_result {
            Ok((index, Ok(resp))) => {
                responses[index] = Some(resp);
                errors[index] = None;
                successful_responses += 1;
            }
            Ok((index, Err(err))) => {
                errors[index] = Some(err);
            }
            Err(_) => {}
        }

        if successful_responses + pending < read_quorum {
            return Err(());
        }
    }

    Ok((responses, errors))
}

// ===========================================================================
// Shared metadata/erasure read primitives (relocated verbatim from
// set_disk/read.rs, P5 step 3, tracking backlog#815, issue backlog#820).
// The object-read operation itself (get_object_*, read_version_optimized, the
// metadata cache) stays in read.rs and reaches these through the SetDisks core.
// ===========================================================================

pub(in crate::set_disk) fn should_allow_metadata_early_stop(
    read_data: bool,
    version_id: &str,
    healing: bool,
    incl_free_versions: bool,
) -> bool {
    if read_data && !is_get_metadata_data_read_early_stop_enabled() {
        return false;
    }

    (is_get_metadata_early_stop_enabled() && version_id.is_empty() && !healing && !incl_free_versions)
        || (is_version_early_stop_enabled() && !version_id.is_empty() && !healing && !incl_free_versions)
}

/// Final gate for the metadata early-stop fast path.
///
/// `caller_allows_early_stop=false` unconditionally forces the full quorum
/// fanout so read-before-write callers (object tagging) get the complete
/// online-disk set as their write target; the early-stop subset would only
/// carry read quorum and fail write quorum (backlog#872 regression).
pub(in crate::set_disk) fn metadata_early_stop_permitted(
    caller_allows_early_stop: bool,
    observe: bool,
    read_data: bool,
    version_id: &str,
    healing: bool,
    incl_free_versions: bool,
) -> bool {
    caller_allows_early_stop && observe && should_allow_metadata_early_stop(read_data, version_id, healing, incl_free_versions)
}

impl SetDisks {
    pub(in crate::set_disk) async fn read_parts(
        disks: &[Option<DiskStore>],
        bucket: &str,
        part_meta_paths: &[String],
        part_numbers: &[usize],
        read_quorum: usize,
    ) -> disk::error::Result<Vec<ObjectPartInfo>> {
        let bucket = bucket.to_string();
        let part_meta_paths = part_meta_paths.to_vec();

        let tasks: Vec<_> = disks
            .iter()
            .map(|disk| {
                let disk = disk.clone();
                let bucket = bucket.clone();
                let part_meta_paths = part_meta_paths.clone();

                async move {
                    if let Some(disk) = disk {
                        disk.read_parts(&bucket, &part_meta_paths).await
                    } else {
                        Err(DiskError::DiskNotFound)
                    }
                }
            })
            .collect();

        let (responses, collected_errors) = match collect_read_parts_results(tasks, read_quorum).await {
            Ok(collected) => collected,
            Err(()) => return Err(DiskError::ErasureReadQuorum),
        };

        if let Some(err) = reduce_read_quorum_errs(&collected_errors, OBJECT_OP_IGNORED_ERRS, read_quorum) {
            return Err(err);
        }

        let mut ret = vec![ObjectPartInfo::default(); part_meta_paths.len()];

        for (part_idx, part_info) in part_meta_paths.iter().enumerate() {
            ret[part_idx] = resolve_read_part_from_responses(
                &bucket,
                part_info,
                part_numbers[part_idx],
                part_idx,
                part_meta_paths.len(),
                &responses,
                read_quorum,
            )?;
        }

        Ok(ret)
    }

    #[allow(clippy::too_many_arguments)]
    #[tracing::instrument(level = "debug", skip(disks))]
    pub(in crate::set_disk) async fn read_all_fileinfo(
        disks: &[Option<DiskStore>],
        org_bucket: &str,
        bucket: &str,
        object: &str,
        version_id: &str,
        read_data: bool,
        healing: bool,
        incl_free_versions: bool,
    ) -> disk::error::Result<(Vec<FileInfo>, Vec<Option<DiskError>>)> {
        let (ress, errors, _) = Self::read_all_fileinfo_inner(
            disks,
            org_bucket,
            bucket,
            object,
            version_id,
            read_data,
            healing,
            incl_free_versions,
            false,
            true,
            0,
            false,
        )
        .await?;
        Ok((ress, errors))
    }

    #[allow(clippy::too_many_arguments)]
    pub(in crate::set_disk) async fn read_all_fileinfo_observed(
        disks: &[Option<DiskStore>],
        org_bucket: &str,
        bucket: &str,
        object: &str,
        version_id: &str,
        read_data: bool,
        healing: bool,
        incl_free_versions: bool,
        caller_allows_early_stop: bool,
        default_parity_count: usize,
    ) -> disk::error::Result<(Vec<FileInfo>, Vec<Option<DiskError>>, MetadataFanoutDiagnostics)> {
        Self::read_all_fileinfo_inner(
            disks,
            org_bucket,
            bucket,
            object,
            version_id,
            read_data,
            healing,
            incl_free_versions,
            true,
            caller_allows_early_stop,
            default_parity_count,
            false,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(in crate::set_disk) async fn read_all_fileinfo_observed_for_get_object(
        disks: &[Option<DiskStore>],
        org_bucket: &str,
        bucket: &str,
        object: &str,
        version_id: &str,
        read_data: bool,
        incl_free_versions: bool,
        caller_allows_early_stop: bool,
        default_parity_count: usize,
    ) -> disk::error::Result<(Vec<FileInfo>, Vec<Option<DiskError>>, MetadataFanoutDiagnostics)> {
        Self::read_all_fileinfo_inner(
            disks,
            org_bucket,
            bucket,
            object,
            version_id,
            read_data,
            false,
            incl_free_versions,
            true,
            caller_allows_early_stop,
            default_parity_count,
            true,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn read_all_fileinfo_inner(
        disks: &[Option<DiskStore>],
        org_bucket: &str,
        bucket: &str,
        object: &str,
        version_id: &str,
        read_data: bool,
        healing: bool,
        incl_free_versions: bool,
        observe: bool,
        // When false, the caller opts out of the early-stop fast path even for
        // otherwise-eligible reads. Read-before-write callers (e.g. object
        // tagging) must set this so the returned online-disk set reflects the
        // full quorum fanout rather than the early-stop subset — writing to the
        // subset would fail write quorum (backlog#872 regression).
        caller_allows_early_stop: bool,
        default_parity_count: usize,
        allow_coalescing: bool,
    ) -> disk::error::Result<(Vec<FileInfo>, Vec<Option<DiskError>>, MetadataFanoutDiagnostics)> {
        let early_stop_enabled =
            caller_allows_early_stop && observe && (is_get_metadata_early_stop_enabled() || is_version_early_stop_enabled());
        let allow_early_stop =
            metadata_early_stop_permitted(caller_allows_early_stop, observe, read_data, version_id, healing, incl_free_versions);
        if allow_early_stop {
            return Self::read_all_fileinfo_early_stop(
                disks,
                org_bucket,
                bucket,
                object,
                version_id,
                read_data,
                healing,
                incl_free_versions,
                default_parity_count,
                allow_coalescing,
            )
            .await;
        }
        if early_stop_enabled {
            let metrics_path = metadata_metrics_path(bucket);
            rustfs_io_metrics::record_get_object_metadata_early_stop_miss(
                metrics_path,
                GET_METADATA_EARLY_STOP_REASON_UNSAFE_REQUEST,
            );
            rustfs_io_metrics::record_get_object_metadata_early_stop_saved_responses(metrics_path, 0);
        }

        Self::read_all_fileinfo_full_wait(
            disks,
            org_bucket,
            bucket,
            object,
            version_id,
            read_data,
            healing,
            incl_free_versions,
            observe,
            allow_coalescing,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn read_all_fileinfo_full_wait(
        disks: &[Option<DiskStore>],
        org_bucket: &str,
        bucket: &str,
        object: &str,
        version_id: &str,
        read_data: bool,
        healing: bool,
        incl_free_versions: bool,
        observe: bool,
        allow_coalescing: bool,
    ) -> disk::error::Result<(Vec<FileInfo>, Vec<Option<DiskError>>, MetadataFanoutDiagnostics)> {
        let fanout_start = observe.then(Instant::now);
        let mut ress = Vec::with_capacity(disks.len());
        let mut errors = Vec::with_capacity(disks.len());
        let mut observations = observe.then(|| Vec::with_capacity(disks.len()));
        let scheduled_count = disks.len();
        let opts = ReadOptions {
            incl_free_versions,
            read_data,
            healing,
        };
        let org_bucket: Arc<str> = Arc::from(org_bucket);
        let bucket: Arc<str> = Arc::from(bucket);
        let object: Arc<str> = Arc::from(object);
        let version_id: Arc<str> = Arc::from(version_id);
        let slowtail_fault = get_metadata_slowtail_fault_request(bucket.as_ref(), object.as_ref(), read_data);
        let futures = disks.iter().enumerate().map(|(disk_index, disk)| {
            let disk = disk.clone();
            let task_opts = opts;
            let org_bucket = org_bucket.clone();
            let bucket = bucket.clone();
            let object = object.clone();
            let version_id = version_id.clone();
            let slowtail_fault = slowtail_fault.clone();
            tokio::spawn(async move {
                let response_start = observe.then(Instant::now);
                let result = if let Some(disk) = disk {
                    Self::record_read_version_call(&object, disk_index);
                    if let Some(delay) = slowtail_fault.as_ref().and_then(|fault| fault.delay_for_disk(disk_index)) {
                        tokio::time::sleep(delay).await;
                    }
                    read_version_via_coalescer(disk, &org_bucket, &bucket, &object, &version_id, &task_opts, allow_coalescing)
                        .await
                } else {
                    Err(DiskError::DiskNotFound)
                };
                let elapsed = response_start.map(|start| start.elapsed());
                (result, elapsed)
            })
        });

        // Wait for all futures to complete
        let results = join_all(futures).await;

        for join_result in results {
            match join_result {
                Ok((res, elapsed)) => match res {
                    Ok(file_info) => {
                        if let (Some(observations), Some(elapsed)) = (&mut observations, elapsed) {
                            observations.push(MetadataFanoutObservation::from_file_info(&file_info, elapsed));
                        }
                        ress.push(file_info);
                        errors.push(None);
                    }
                    Err(e) => {
                        if let (Some(observations), Some(elapsed)) = (&mut observations, elapsed) {
                            observations.push(MetadataFanoutObservation::from_error(&e, elapsed));
                        }
                        ress.push(FileInfo::default());
                        errors.push(Some(e));
                    }
                },
                Err(_join_err) => {
                    // A spawned task panicked — treat as unexpected disk error
                    if let Some(observations) = &mut observations {
                        observations.push(MetadataFanoutObservation::from_error(&DiskError::Unexpected, Duration::ZERO));
                    }
                    ress.push(FileInfo::default());
                    errors.push(Some(DiskError::Unexpected));
                }
            }
        }
        let diagnostics = match (fanout_start, observations) {
            (Some(fanout_start), Some(observations)) => MetadataFanoutDiagnostics::new(fanout_start.elapsed(), observations),
            _ => MetadataFanoutDiagnostics::default(),
        };
        if observe {
            rustfs_io_metrics::record_get_object_metadata_fanout_lifecycle(
                metadata_metrics_path(bucket.as_ref()),
                scheduled_count,
                scheduled_count,
                0,
            );
        }
        Ok((ress, errors, diagnostics))
    }

    #[allow(clippy::too_many_arguments)]
    async fn read_all_fileinfo_early_stop(
        disks: &[Option<DiskStore>],
        org_bucket: &str,
        bucket: &str,
        object: &str,
        version_id: &str,
        read_data: bool,
        healing: bool,
        incl_free_versions: bool,
        default_parity_count: usize,
        allow_coalescing: bool,
    ) -> disk::error::Result<(Vec<FileInfo>, Vec<Option<DiskError>>, MetadataFanoutDiagnostics)> {
        let fanout_start = Instant::now();
        let mut ress = vec![FileInfo::default(); disks.len()];
        let mut errors = vec![None; disks.len()];
        let mut observations = Vec::with_capacity(disks.len());
        let mut accumulator =
            MetadataQuorumAccumulator::new(disks.len(), default_parity_count, true).with_requested_version_id(version_id);
        let opts = ReadOptions {
            incl_free_versions,
            read_data,
            healing,
        };
        let org_bucket: Arc<str> = Arc::from(org_bucket);
        let bucket: Arc<str> = Arc::from(bucket);
        let object: Arc<str> = Arc::from(object);
        let version_id: Arc<str> = Arc::from(version_id);
        let metrics_path = metadata_metrics_path(bucket.as_ref());
        let mut join_set = JoinSet::new();
        let bounded_fanout = is_get_metadata_early_stop_bounded_fanout_enabled();
        let fanout_order = if bounded_fanout {
            bounded_metadata_fanout_order(bucket.as_ref(), object.as_ref(), disks.len(), default_parity_count)
        } else {
            Vec::new()
        };
        let mut next_fanout_index = 0usize;
        let mut scheduled_count = 0usize;
        let mut force_full_wait = false;
        let mut final_miss_reason_override = None;
        let slowtail_fault = get_metadata_slowtail_fault_request(bucket.as_ref(), object.as_ref(), read_data);
        let spawn_read_version =
            |join_set: &mut JoinSet<(usize, disk::error::Result<FileInfo>, Duration)>, index: usize, disk: Option<DiskStore>| {
                let task_opts = opts;
                let org_bucket = org_bucket.clone();
                let bucket = bucket.clone();
                let object = object.clone();
                let version_id = version_id.clone();
                let slowtail_fault = slowtail_fault.clone();
                join_set.spawn(async move {
                    let response_start = Instant::now();
                    let result = if let Some(disk) = disk {
                        #[allow(clippy::let_unit_value)]
                        let _fanout_task_guard = Self::rename_fanout_task_guard(&object);
                        Self::record_read_version_call(&object, index);
                        #[cfg(test)]
                        Self::read_version_fanout_barrier(&object, index).await;
                        if let Some(delay) = slowtail_fault.as_ref().and_then(|fault| fault.delay_for_disk(index)) {
                            tokio::time::sleep(delay).await;
                        }
                        read_version_via_coalescer(disk, &org_bucket, &bucket, &object, &version_id, &task_opts, allow_coalescing)
                            .await
                    } else {
                        Err(DiskError::DiskNotFound)
                    };
                    (index, result, response_start.elapsed())
                });
            };

        if bounded_fanout {
            let initial_target = accumulator.default_write_quorum().min(disks.len());
            while next_fanout_index < initial_target {
                let disk_index = fanout_order[next_fanout_index];
                if let Some(disk) = disks.get(disk_index).cloned() {
                    spawn_read_version(&mut join_set, disk_index, disk);
                    scheduled_count = scheduled_count.saturating_add(1);
                }
                next_fanout_index = next_fanout_index.saturating_add(1);
            }
        } else {
            for (index, disk) in disks.iter().cloned().enumerate() {
                spawn_read_version(&mut join_set, index, disk);
                scheduled_count = scheduled_count.saturating_add(1);
            }
        }

        while let Some(result) = join_set.join_next().await {
            let mut defer_pending_inline_data_shard = false;
            match result {
                Ok((index, res, elapsed)) => match res {
                    Ok(file_info) => {
                        observations.push(MetadataFanoutObservation::from_file_info(&file_info, elapsed));
                        accumulator.observe_file_info(&file_info);
                        if bounded_fanout
                            && read_data
                            && !force_full_wait
                            && let Some(reason) = data_read_early_stop_inline_candidate_miss_reason(&file_info)
                        {
                            force_full_wait = true;
                            final_miss_reason_override.get_or_insert(reason);
                        }
                        if let Some(slot) = ress.get_mut(index) {
                            *slot = file_info;
                        }
                    }
                    Err(err) => {
                        observations.push(MetadataFanoutObservation::from_error(&err, elapsed));
                        accumulator.observe_error(&err);
                        if let Some(slot) = errors.get_mut(index) {
                            *slot = Some(err);
                        }
                    }
                },
                Err(_) => {
                    let err = DiskError::Unexpected;
                    observations.push(MetadataFanoutObservation::from_error(&err, fanout_start.elapsed()));
                    accumulator.observe_error(&err);
                }
            }

            if !force_full_wait
                && let Some(decision) = accumulator
                    .early_stop_decision()
                    .or_else(|| accumulator.version_early_stop_decision())
            {
                let should_return_early = if read_data {
                    match accumulator.candidate.as_ref() {
                        Some(candidate) => match data_read_early_stop_inline_body_miss_reason(
                            bucket.as_ref(),
                            object.as_ref(),
                            candidate,
                            &ress,
                            disks,
                        )
                        .await
                        {
                            None => true,
                            Some(reason) => {
                                final_miss_reason_override = Some(reason);
                                if bounded_fanout
                                    && reason == GET_METADATA_EARLY_STOP_REASON_DATA_READ_INLINE_MISSING_SHARD
                                    && data_read_inline_missing_shards_are_pending(
                                        candidate,
                                        &ress,
                                        &errors,
                                        disks,
                                        &fanout_order,
                                        next_fanout_index,
                                    )
                                {
                                    defer_pending_inline_data_shard = true;
                                } else {
                                    force_full_wait = true;
                                }
                                false
                            }
                        },
                        None => {
                            force_full_wait = true;
                            final_miss_reason_override = Some(GET_METADATA_EARLY_STOP_REASON_INSUFFICIENT_QUORUM);
                            false
                        }
                    }
                } else {
                    true
                };

                if should_return_early {
                    let saved_responses = if bounded_fanout {
                        disks.len().saturating_sub(observations.len())
                    } else {
                        join_set.len()
                    };
                    join_set.abort_all();
                    rustfs_io_metrics::record_get_object_metadata_early_stop_hit(metrics_path, decision.reason);
                    rustfs_io_metrics::record_get_object_metadata_early_stop_saved_responses(metrics_path, saved_responses);
                    let mut cancelled_count = 0usize;
                    while let Some(join_result) = join_set.join_next().await {
                        match join_result {
                            Err(join_error) if join_error.is_cancelled() => {
                                cancelled_count = cancelled_count.saturating_add(1);
                            }
                            _ => {}
                        }
                    }
                    rustfs_io_metrics::record_get_object_metadata_fanout_lifecycle(
                        metrics_path,
                        scheduled_count,
                        scheduled_count.saturating_sub(cancelled_count),
                        cancelled_count,
                    );
                    let diagnostics = MetadataFanoutDiagnostics::new(fanout_start.elapsed(), observations);
                    return Ok((ress, errors, diagnostics));
                }
            }

            let pending_responses = join_set.len();
            let should_hedge_single_pending_data_read = read_data
                && !force_full_wait
                && !defer_pending_inline_data_shard
                && pending_responses == 1
                && accumulator.can_still_reach_early_stop_with_pending(pending_responses);
            if bounded_fanout && force_full_wait {
                while next_fanout_index < disks.len() {
                    let disk_index = fanout_order[next_fanout_index];
                    if let Some(disk) = disks.get(disk_index).cloned() {
                        spawn_read_version(&mut join_set, disk_index, disk);
                        scheduled_count = scheduled_count.saturating_add(1);
                    }
                    next_fanout_index = next_fanout_index.saturating_add(1);
                }
            } else if bounded_fanout
                && !defer_pending_inline_data_shard
                && next_fanout_index < disks.len()
                && (!accumulator.can_still_reach_early_stop_with_pending(pending_responses)
                    || should_hedge_single_pending_data_read)
            {
                let disk_index = fanout_order[next_fanout_index];
                if let Some(disk) = disks.get(disk_index).cloned() {
                    spawn_read_version(&mut join_set, disk_index, disk);
                    scheduled_count = scheduled_count.saturating_add(1);
                }
                next_fanout_index = next_fanout_index.saturating_add(1);
            }
        }

        let accumulator_miss_reason = accumulator.final_miss_reason();
        let final_miss_reason = match (final_miss_reason_override, accumulator_miss_reason) {
            (Some(reason), GET_METADATA_EARLY_STOP_REASON_INSUFFICIENT_QUORUM) => reason,
            _ => accumulator_miss_reason,
        };
        rustfs_io_metrics::record_get_object_metadata_early_stop_miss(metrics_path, final_miss_reason);
        rustfs_io_metrics::record_get_object_metadata_early_stop_saved_responses(metrics_path, 0);
        rustfs_io_metrics::record_get_object_metadata_fanout_lifecycle(metrics_path, scheduled_count, scheduled_count, 0);
        let diagnostics = MetadataFanoutDiagnostics::new(fanout_start.elapsed(), observations);
        Ok((ress, errors, diagnostics))
    }

    pub(in crate::set_disk) async fn read_all_xl(
        disks: &[Option<DiskStore>],
        bucket: &str,
        object: &str,
        read_data: bool,
        incl_free_vers: bool,
    ) -> (Vec<FileInfo>, Vec<Option<DiskError>>) {
        let (fileinfos, errs) = Self::read_all_raw_file_info(disks, bucket, object, read_data).await;

        Self::pick_latest_quorum_files_info(fileinfos, errs, bucket, object, read_data, incl_free_vers).await
    }

    pub(crate) async fn load_file_info_versions_exact(
        &self,
        bucket: &str,
        object: &str,
    ) -> Result<Option<rustfs_filemeta::FileInfoVersions>> {
        let disk_object = rustfs_utils::path::encode_dir_object(object);
        let disks = self.get_disks_internal().await;
        if disks.is_empty() {
            return Err(to_object_err(StorageError::ErasureReadQuorum, vec![bucket, object]));
        }

        let read_quorum = disks.len().div_ceil(2).max(1);
        let (raw_fileinfos, errs) = Self::read_all_raw_file_info(&disks, bucket, disk_object.as_str(), false).await;

        if let Some(err) = reduce_read_quorum_errs(&errs, OBJECT_OP_IGNORED_ERRS, read_quorum) {
            let object_err = to_object_err(err.into(), vec![bucket, object]);
            if is_err_object_not_found(&object_err) || is_err_version_not_found(&object_err) {
                return Ok(None);
            }
            return Err(object_err);
        }

        let mut shallow_versions = Vec::with_capacity(raw_fileinfos.len());
        for raw_fileinfo in raw_fileinfos.into_iter().flatten() {
            let meta = FileMeta::load(&raw_fileinfo.buf)
                .map_err(|err| Error::other(format!("exact object metadata decode failed for {bucket}/{object}: {err}")))?;
            shallow_versions.push(meta.versions);
        }

        if shallow_versions.len() < read_quorum {
            return Err(to_object_err(StorageError::ErasureReadQuorum, vec![bucket, object]));
        }

        let versions = merge_file_meta_versions(read_quorum, true, 0, &shallow_versions);
        if versions.is_empty() {
            return Err(Error::other(format!(
                "exact object metadata read returned no quorum versions for {bucket}/{object}"
            )));
        }

        let file_info_versions = FileMeta {
            versions,
            ..Default::default()
        }
        .get_all_file_info_versions(bucket, object, true)
        .map_err(|err| Error::other(format!("exact object versions decode failed for {bucket}/{object}: {err}")))?;

        for file_info in file_info_versions
            .versions
            .iter()
            .chain(file_info_versions.free_versions.iter())
        {
            file_info
                .validate_for_metadata_read()
                .map_err(|err| Error::other(format!("exact object versions validation failed for {bucket}/{object}: {err}")))?;
        }

        Ok(Some(file_info_versions))
    }

    pub(in crate::set_disk) async fn read_all_raw_file_info(
        disks: &[Option<DiskStore>],
        bucket: &str,
        object: &str,
        read_data: bool,
    ) -> (Vec<Option<RawFileInfo>>, Vec<Option<DiskError>>) {
        let mut ress = Vec::with_capacity(disks.len());
        let mut errors = Vec::with_capacity(disks.len());

        let mut futures = Vec::with_capacity(disks.len());

        for disk in disks.iter() {
            futures.push(async move {
                if let Some(disk) = disk {
                    disk.read_xl(bucket, object, read_data).await
                } else {
                    Err(DiskError::DiskNotFound)
                }
            });
        }

        let results = join_all(futures).await;
        for result in results {
            match result {
                Ok(res) => {
                    ress.push(Some(res));
                    errors.push(None);
                }
                Err(e) => {
                    ress.push(None);
                    errors.push(Some(e));
                }
            }
        }

        (ress, errors)
    }

    pub(in crate::set_disk) async fn pick_latest_quorum_files_info(
        fileinfos: Vec<Option<RawFileInfo>>,
        errs: Vec<Option<DiskError>>,
        bucket: &str,
        object: &str,
        read_data: bool,
        incl_free_vers: bool,
    ) -> (Vec<FileInfo>, Vec<Option<DiskError>>) {
        let mut metadata_array = vec![None; fileinfos.len()];
        let mut meta_file_infos = vec![FileInfo::default(); fileinfos.len()];
        let mut metadata_shallow_versions = vec![None; fileinfos.len()];

        let mut v2_bufs = {
            if !read_data {
                vec![Vec::new(); fileinfos.len()]
            } else {
                Vec::new()
            }
        };

        let mut errs = errs;

        for (idx, info_op) in fileinfos.iter().enumerate() {
            if let Some(info) = info_op {
                if !read_data {
                    v2_bufs[idx] = info.buf.clone();
                }

                let xlmeta = match FileMeta::load(&info.buf) {
                    Ok(res) => res,
                    Err(err) => {
                        errs[idx] = Some(err.into());
                        continue;
                    }
                };

                metadata_array[idx] = Some(xlmeta);
                meta_file_infos[idx] = FileInfo::default();
            }
        }

        for (idx, info_op) in metadata_array.iter().enumerate() {
            if let Some(info) = info_op {
                metadata_shallow_versions[idx] = Some(info.versions.clone());
            }
        }

        let shallow_versions: Vec<Vec<FileMetaShallowVersion>> = metadata_shallow_versions.iter().flatten().cloned().collect();

        let read_quorum = fileinfos.len().div_ceil(2);
        let versions = merge_file_meta_versions(read_quorum, false, 1, &shallow_versions);
        let meta = FileMeta {
            versions,
            ..Default::default()
        };

        // Determine the winning version id. When the merged representative decodes to
        // a valid FileInfo, use its version id. When it is undecodable (Err from
        // corrupt part arrays) OR decodes but is not valid (e.g. a shallow merged
        // representative missing erasure detail), do NOT poison every disk: derive the
        // winning vid from the intact version header and fall into the per-disk loop
        // below, so healthy disks still populate `meta_file_infos` to satisfy
        // read_quorum while corrupt disks fail `into_fileinfo` and are flagged
        // `FileCorrupt` for heal. If every disk is corrupt, they all fail in the loop,
        // leaving no valid FileInfo so the caller's read_quorum fails cleanly instead
        // of panicking or returning half-corrupt data. Only when there is no non-free
        // version header at all is there genuinely nothing to read.
        //
        // `into_fileinfo` with an empty version_id selects the first non-free version
        // (see FileMeta::into_fileinfo); replicate that selection from the header here.
        let vid = match meta.into_fileinfo_without_part_checksums(bucket, object, "", true, incl_free_vers) {
            Ok(finfo) if file_info_is_valid_for_metadata(&finfo) => finfo.version_id.unwrap_or(Uuid::nil()),
            _ => match meta
                .versions
                .iter()
                .find(|v| !v.header.free_version())
                .and_then(|v| v.header.version_id)
            {
                Some(id) => id,
                None => {
                    for item in errs.iter_mut() {
                        if item.is_none() {
                            *item = Some(DiskError::FileCorrupt);
                        }
                    }

                    return (meta_file_infos, errs);
                }
            },
        };

        for (idx, meta_op) in metadata_array.iter().enumerate() {
            if let Some(meta) = meta_op {
                match meta.into_fileinfo_without_part_checksums(
                    bucket,
                    object,
                    vid.to_string().as_str(),
                    read_data,
                    incl_free_vers,
                ) {
                    Ok(res) => match res.validate_for_metadata_read() {
                        Ok(_) => meta_file_infos[idx] = res,
                        Err(err) => errs[idx] = Some(err.into()),
                    },
                    Err(err) => errs[idx] = Some(err.into()),
                }
            }
        }

        (meta_file_infos, errs)
    }

    #[allow(dead_code, reason = "asserted by this file's tests (backlog#1823)")]
    pub(in crate::set_disk) async fn read_multiple_files(
        disks: &[Option<DiskStore>],
        req: ReadMultipleReq,
        read_quorum: usize,
    ) -> Vec<ReadMultipleResp> {
        let mut futures = Vec::with_capacity(disks.len());
        let empty_quorum_result = || {
            req.files
                .iter()
                .map(|want| ReadMultipleResp {
                    bucket: req.bucket.clone(),
                    prefix: req.prefix.clone(),
                    file: want.clone(),
                    exists: false,
                    error: Error::ErasureReadQuorum.to_string(),
                    data: Vec::new(),
                    mod_time: None,
                })
                .collect::<Vec<_>>()
        };

        for disk in disks.iter() {
            let disk = disk.clone();
            let req = req.clone();
            futures.push(async move {
                if let Some(disk) = disk {
                    disk.read_multiple(req).await
                } else {
                    Err(DiskError::DiskNotFound)
                }
            });
        }

        let (ress, _errors) = match collect_read_multiple_results(futures, read_quorum).await {
            Ok(collected) => collected,
            Err(()) => return empty_quorum_result(),
        };

        let mut ret = Vec::with_capacity(req.files.len());

        for want in req.files.iter() {
            let mut quorum = 0;

            let mut get_res = ReadMultipleResp::default();

            for res in ress.iter() {
                if res.is_none() {
                    continue;
                }

                let disk_res = res.as_ref().unwrap();

                for resp in disk_res.iter() {
                    if !resp.error.is_empty() || !resp.exists {
                        continue;
                    }

                    if &resp.file != want || resp.bucket != req.bucket || resp.prefix != req.prefix {
                        continue;
                    }
                    quorum += 1;

                    if get_res.mod_time > resp.mod_time || get_res.data.len() > resp.data.len() {
                        continue;
                    }

                    get_res = resp.clone();
                }
            }

            if quorum < read_quorum {
                // debug!("quorum < read_quorum: {} < {}", quorum, read_quorum);
                get_res.exists = false;
                get_res.error = Error::ErasureReadQuorum.to_string();
                get_res.data = Vec::new();
            }

            ret.push(get_res);
        }

        // log err

        ret
    }
}

// ===========================================================================
// Write / rename / delete primitives (relocated verbatim from set_disk/write.rs,
// P5 step 2, tracking backlog#815, issue backlog#820).
// ===========================================================================

/// Grace window during which a recently modified object is never deleted as
/// dangling. 0 disables the grace window.
const ENV_HEAL_DANGLING_DELETE_GRACE_SECS: &str = "RUSTFS_HEAL_DANGLING_DELETE_GRACE_SECS";
const DEFAULT_HEAL_DANGLING_DELETE_GRACE_SECS: u64 = 3600;

fn dangling_delete_grace() -> time::Duration {
    let secs = rustfs_utils::get_env_u64(ENV_HEAL_DANGLING_DELETE_GRACE_SECS, DEFAULT_HEAL_DANGLING_DELETE_GRACE_SECS);
    time::Duration::seconds(i64::try_from(secs).unwrap_or(i64::MAX))
}

/// Result of scanning one disk's copy of a directory prefix while deciding
/// whether an orphan (metadata-less) directory tree can be safely purged.
enum OrphanDirScan {
    /// The subtree holds at least one regular file (object metadata or data), so
    /// it is a real object and must not be purged.
    HasData,
    /// The prefix exists on this disk and contains only nested empty directories.
    /// Carries every directory path in pre-order (parents before children).
    Empty(Vec<String>),
    /// The prefix does not exist on this disk.
    Missing,
}

/// Outcome of a *post-quorum* `rename_data` commit, classifying whether the
/// committed replicas converged so the caller can decide heal admission
/// WITHOUT conflating "a version signature exists" with "this write needs
/// heal" (rustfs/backlog#1321).
///
/// `rename_data` reaches this classification only once write quorum is met — a
/// sub-quorum commit returns `Err` and never produces a convergence — so every
/// variant describes an already-durable, already-ACKable write.
///
/// # Extension point for #1312 (commit fencing)
///
/// #1312 layers a per-object fencing epoch onto the SAME `rename_data` path.
/// An epoch rejection is a *commit* failure surfaced through the existing
/// `Result::Err` channel (the write never lands), which is orthogonal to this
/// post-commit convergence signal (the write landed; do the replicas need
/// heal). The two therefore compose: fencing gates whether we reach a
/// convergence at all, and this enum classifies the convergence once reached.
/// A future fence-aware variant, if ever needed, is an additive change here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::set_disk) enum RenameConvergence {
    /// Every attempted disk committed and reported an identical, known version
    /// signature. The committed replicas are already converged; no heal needed.
    AllSuccessIdentical,
    /// Write quorum was met but at least one attempted disk failed to commit
    /// (error or offline). The committed set may be short a replica; the
    /// laggard must be converged by heal.
    PartialCommit,
    /// Every attempted disk committed but their version signatures diverge (or
    /// mix signed <=10-version disks with unsigned >10-version disks, itself a
    /// version-count divergence). Heal must reconcile the committed replicas.
    SignatureDivergent,
    /// Every attempted disk committed but none produced a version signature
    /// (all observed >10 versions, where the signature is deliberately
    /// omitted). Divergence can neither be proven nor ruled out here, so any
    /// latent divergence is left to the scanner backstop rather than enqueued
    /// for heal.
    Unknown,
}

impl RenameConvergence {
    /// Whether this commit outcome must enqueue an object heal to converge the
    /// committed replicas.
    ///
    /// `Unknown` and `AllSuccessIdentical` return `false`: the former is
    /// scanner-backstopped (see the variant docs), the latter is already
    /// converged. This is the single decision point that replaced the old
    /// `Option<Vec<u8>>::is_some()` heuristic, under which a healthy MPU
    /// (identical signatures on every disk) always looked like it needed heal.
    pub(in crate::set_disk) fn needs_heal(&self) -> bool {
        matches!(self, Self::PartialCommit | Self::SignatureDivergent)
    }
}

pub(in crate::set_disk) struct RenameDataCommit {
    pub(in crate::set_disk) online_disks: Vec<Option<DiskStore>>,
    pub(in crate::set_disk) convergence: RenameConvergence,
    pub(in crate::set_disk) data_dir: Option<Uuid>,
    pub(in crate::set_disk) cleanup_disks: Vec<Option<DiskStore>>,
    pub(in crate::set_disk) old_current_size: Option<OldCurrentSize>,
    pub(in crate::set_disk) committed_file_info: FileInfo,
    pub(in crate::set_disk) tail_drain: Option<tokio::task::JoinHandle<()>>,
}

#[allow(dead_code, reason = "asserted by this file's tests (backlog#1823)")]
type RenameDataLegacyTuple = (
    Vec<Option<DiskStore>>,
    RenameConvergence,
    Option<Uuid>,
    Vec<Option<DiskStore>>,
    Option<OldCurrentSize>,
);

fn put_rename_early_ack_enabled() -> bool {
    rustfs_utils::get_env_bool(ENV_RUSTFS_PUT_RENAME_EARLY_ACK_ENABLE, true)
}

impl RenameDataCommit {
    #[allow(dead_code, reason = "asserted by this file's tests (backlog#1823)")]
    fn into_legacy_tuple(self) -> RenameDataLegacyTuple {
        (
            self.online_disks,
            self.convergence,
            self.data_dir,
            self.cleanup_disks,
            self.old_current_size,
        )
    }
}

impl SetDisks {
    pub(in crate::set_disk) fn default_read_quorum(&self) -> usize {
        self.set_drive_count - self.default_parity_count
    }

    pub(crate) fn default_write_quorum(&self) -> usize {
        let mut data_count = self.set_drive_count - self.default_parity_count;
        if data_count == self.default_parity_count {
            data_count += 1
        }

        data_count
    }

    pub(in crate::set_disk) async fn prepare_quota_mutation_fences(
        disks: &[Option<DiskStore>],
        bucket: &str,
        object: &str,
        write_quorum: usize,
    ) -> crate::error::Result<(Vec<Option<DiskStore>>, Vec<Option<SnapshotLeaseToken>>)> {
        let fence_path = crate::disk::quota_mutation_fence_path(bucket, object);
        let results = join_all(disks.iter().map(|disk| {
            let disk = disk.clone();
            let fence_path = fence_path.clone();
            async move {
                let disk = disk?;
                match disk.acquire_snapshot_lease(RUSTFS_META_BUCKET, &fence_path).await {
                    Ok(token) => Some((disk, token)),
                    Err(_) => None,
                }
            }
        }))
        .await;
        if results.iter().flatten().count() < write_quorum {
            for (disk, token) in results.iter().flatten() {
                let _ = disk.release_snapshot_lease(RUSTFS_META_BUCKET, &fence_path, *token).await;
            }
            return Err(StorageError::ErasureWriteQuorum);
        }
        let mut fenced_disks = Vec::with_capacity(results.len());
        let mut tokens = Vec::with_capacity(results.len());
        for result in results {
            match result {
                Some((disk, token)) => {
                    fenced_disks.push(Some(disk));
                    tokens.push(Some(token));
                }
                None => {
                    fenced_disks.push(None);
                    tokens.push(None);
                }
            }
        }
        Ok((fenced_disks, tokens))
    }

    pub(in crate::set_disk) async fn release_quota_mutation_fences(
        disks: &[Option<DiskStore>],
        tokens: &[Option<SnapshotLeaseToken>],
        bucket: &str,
        object: &str,
        write_quorum: usize,
    ) -> crate::error::Result<()> {
        let fence_path = crate::disk::quota_mutation_fence_path(bucket, object);
        let results = join_all(disks.iter().zip(tokens).filter_map(|(disk, token)| {
            let disk = disk.as_ref()?.clone();
            let token = (*token)?;
            let fence_path = fence_path.clone();
            Some(async move { disk.release_snapshot_lease(RUSTFS_META_BUCKET, &fence_path, token).await })
        }))
        .await;
        if results.iter().filter(|result| result.is_ok()).count() < write_quorum {
            return Err(StorageError::ErasureWriteQuorum);
        }
        Ok(())
    }

    pub(in crate::set_disk) fn assign_rename_data_indexes(file_infos: &mut [FileInfo]) {
        for (index, file_info) in file_infos.iter_mut().enumerate() {
            if file_info.erasure.index == 0 {
                file_info.erasure.index = index + 1;
            }
        }
    }

    fn rename_data_commit_from_observations(
        disks: &[Option<DiskStore>],
        file_infos: &[FileInfo],
        disk_versions: &[Option<Vec<u8>>],
        errs: &[Option<DiskError>],
        cleanup_data_dirs: &[Option<Uuid>],
        old_current_sizes: &[Option<OldCurrentSize>],
        write_quorum: usize,
    ) -> disk::error::Result<RenameDataCommit> {
        let data_dir = Self::reduce_common_data_dir(cleanup_data_dirs, write_quorum);
        let convergence = Self::classify_rename_convergence(disk_versions, errs);
        let old_current_size = Self::reduce_common_old_current_size(old_current_sizes, write_quorum);
        let online_disks = Self::eval_disks(disks, errs);
        let committed_slot = online_disks.iter().position(Option::is_some).ok_or(DiskError::Unexpected)?;
        let committed_file_info = file_infos.get(committed_slot).cloned().ok_or(DiskError::Unexpected)?;
        let cleanup_disks = if let Some(data_dir) = data_dir {
            disks
                .iter()
                .zip(errs.iter())
                .zip(cleanup_data_dirs.iter())
                .map(|((disk, err), old_data_dir)| {
                    if err.is_none() && *old_data_dir == Some(data_dir) {
                        disk.clone()
                    } else {
                        None
                    }
                })
                .collect()
        } else {
            vec![None; disks.len()]
        };

        Ok(RenameDataCommit {
            online_disks,
            convergence,
            data_dir,
            cleanup_disks,
            old_current_size,
            committed_file_info,
            tail_drain: None,
        })
    }

    pub(in crate::set_disk) async fn abort_quota_reservation_after_fence(
        reservation: crate::bucket::quota::reservation::QuotaReservation,
        disks: &[Option<DiskStore>],
        tokens: &[Option<SnapshotLeaseToken>],
        bucket: &str,
        object: &str,
        write_quorum: usize,
        fenced: bool,
    ) {
        let safe_to_abort = !fenced
            || Self::release_quota_mutation_fences(disks, tokens, bucket, object, write_quorum)
                .await
                .is_ok();
        if safe_to_abort {
            reservation.abort().await;
        } else {
            reservation.defer_after_fence();
        }
    }

    #[tracing::instrument(level = "debug", skip(disks, file_infos))]
    #[allow(clippy::type_complexity)]
    #[allow(dead_code, reason = "asserted by this file's tests (backlog#1823)")]
    pub(in crate::set_disk) async fn rename_data(
        disks: &[Option<DiskStore>],
        src_bucket: &str,
        src_object: &str,
        file_infos: &[FileInfo],
        dst_bucket: &str,
        dst_object: &str,
        write_quorum: usize,
    ) -> disk::error::Result<RenameDataLegacyTuple> {
        Self::rename_data_owned(disks, src_bucket, src_object, file_infos.to_vec(), dst_bucket, dst_object, write_quorum)
            .await
            .map(RenameDataCommit::into_legacy_tuple)
    }

    #[tracing::instrument(level = "debug", skip(disks, file_infos))]
    async fn rename_data_owned_early_ack(
        disks: &[Option<DiskStore>],
        src_bucket: &str,
        src_object: &str,
        file_infos: Vec<FileInfo>,
        dst_bucket: &str,
        dst_object: &str,
        write_quorum: usize,
    ) -> disk::error::Result<RenameDataCommit> {
        if let Some(file_info) = disks
            .iter()
            .zip(file_infos.iter())
            .find_map(|(disk, file_info)| disk.as_ref().map(|_| file_info))
        {
            if file_info.is_canonical_delete_marker() {
                file_info.validate_for_metadata_read()?;
            } else {
                file_info.validate_for_erasure_write()?;
            }
        }

        let disk_count = disks.len();
        let fanout_disks = disks.to_vec();
        let coordinator_disks = fanout_disks.clone();
        let src_bucket = Arc::new(src_bucket.to_string());
        let src_object = Arc::new(src_object.to_string());
        let dst_bucket = Arc::new(dst_bucket.to_string());
        let dst_object = Arc::new(dst_object.to_string());
        let (commit_tx, commit_rx) = tokio::sync::oneshot::channel();

        let tail_drain = tokio::spawn({
            let fanout_src_bucket = src_bucket.clone();
            let fanout_src_object = src_object.clone();
            let fanout_dst_bucket = dst_bucket.clone();
            let fanout_dst_object = dst_object.clone();
            async move {
                let successful_rename_completion_rank =
                    rustfs_io_metrics::put_stage_metrics_enabled().then(|| Arc::new(AtomicUsize::new(0)));
                let mut tasks = JoinSet::new();
                for (i, (disk, file_info)) in fanout_disks.into_iter().zip(file_infos.iter()).enumerate() {
                    let src_bucket = fanout_src_bucket.clone();
                    let src_object = fanout_src_object.clone();
                    let dst_bucket = fanout_dst_bucket.clone();
                    let dst_object = fanout_dst_object.clone();
                    let file_info = file_info.clone();
                    let successful_rename_completion_rank = successful_rename_completion_rank.clone();
                    tasks.spawn(async move {
                        let result = std::panic::AssertUnwindSafe(async move {
                            #[allow(clippy::let_unit_value)]
                            let _fanout_task_guard = Self::rename_fanout_task_guard(&dst_object);

                            let Some(disk) = disk else {
                                return Err(DiskError::DiskNotFound);
                            };

                            let is_delete_marker = file_info.is_canonical_delete_marker();
                            let mut local_file_info;
                            let file_info = if file_info.erasure.index == 0 {
                                local_file_info = file_info.clone();
                                local_file_info.erasure.index = i + 1;
                                &local_file_info
                            } else {
                                &file_info
                            };
                            if file_info.erasure.index == 0 || (!is_delete_marker && !file_info.has_valid_erasure_geometry()) {
                                return Err(DiskError::FileCorrupt);
                            }

                            Self::rename_fanout_barrier(&dst_object, i, rename_fanout_barrier_phase::RENAME).await;

                            if let Some(err) = Self::rename_injected_error(&dst_object, i) {
                                return Err(err);
                            }

                            let disk_wait_started = rustfs_io_metrics::put_stage_timer();
                            let result = disk
                                .rename_data_borrowed(&src_bucket, &src_object, file_info, &dst_bucket, &dst_object)
                                .await;
                            if let Some(disk_wait_started) = disk_wait_started {
                                let duration_ms = disk_wait_started.elapsed().as_secs_f64() * 1000.0;
                                rustfs_io_metrics::record_put_object_stage_duration(
                                    rustfs_io_metrics::PUT_STAGE_SET_DISK_RENAME_DISK_WAIT,
                                    duration_ms,
                                );
                                let position = if result.is_ok() {
                                    let rank = successful_rename_completion_rank
                                        .as_ref()
                                        .map(|rank| rank.fetch_add(1, Ordering::Relaxed) + 1)
                                        .unwrap_or(1);
                                    if rank <= write_quorum {
                                        rustfs_io_metrics::PUT_RENAME_DISK_WAIT_COMPLETION_POSITION_QUORUM_FIRST
                                    } else {
                                        rustfs_io_metrics::PUT_RENAME_DISK_WAIT_COMPLETION_POSITION_QUORUM_TAIL
                                    }
                                } else {
                                    rustfs_io_metrics::PUT_RENAME_DISK_WAIT_COMPLETION_POSITION_ERROR
                                };
                                rustfs_io_metrics::record_put_rename_disk_wait_completion(position, duration_ms);
                            }
                            result
                        })
                        .catch_unwind()
                        .await;
                        (i, result)
                    });
                }

                let mut commit_tx = Some(commit_tx);
                let mut sent_commit = false;
                let mut success_count = 0usize;
                let mut fanout_panic = 0usize;
                let mut results_seen = 0usize;
                let mut errs = vec![Some(DiskError::DiskNotFound); disk_count];
                let mut disk_versions = vec![None; disk_count];
                let mut data_dirs = vec![None; disk_count];
                let mut cleanup_data_dirs = vec![None; disk_count];
                let mut old_current_sizes = vec![None; disk_count];

                while let Some(joined) = tasks.join_next().await {
                    results_seen += 1;
                    match joined {
                        Ok((idx, Ok(Ok(res)))) => {
                            data_dirs[idx] = res.rollback_data_dir.or(res.old_data_dir);
                            cleanup_data_dirs[idx] = res.cleanup_data_dir;
                            disk_versions[idx] = res.sign;
                            old_current_sizes[idx] = res.old_current_size;
                            errs[idx] = None;
                            success_count += 1;
                        }
                        Ok((idx, Ok(Err(err)))) => {
                            errs[idx] = Some(err);
                        }
                        Ok((idx, Err(_))) => {
                            errs[idx] = Some(DiskError::Unexpected);
                            fanout_panic += 1;
                        }
                        Err(_) => {
                            fanout_panic += 1;
                        }
                    }

                    if !sent_commit && success_count >= write_quorum {
                        let snapshot_commit = Self::rename_data_commit_from_observations(
                            &coordinator_disks,
                            &file_infos,
                            &disk_versions,
                            &errs,
                            &cleanup_data_dirs,
                            &old_current_sizes,
                            write_quorum,
                        );
                        if let Some(commit_tx) = commit_tx.take() {
                            let _ = commit_tx.send(snapshot_commit);
                        }
                        sent_commit = true;
                    }
                }

                if rustfs_io_metrics::put_stage_metrics_enabled() {
                    let fanout_success = errs.iter().filter(|err| err.is_none()).count();
                    let fanout_error = errs.len().saturating_sub(fanout_success + fanout_panic);
                    rustfs_io_metrics::record_put_rename_quorum_wait_fanout(
                        results_seen,
                        write_quorum,
                        fanout_success,
                        fanout_error,
                        fanout_panic,
                    );
                }

                if !sent_commit {
                    let ret_err =
                        reduce_write_quorum_errs(&errs, OBJECT_OP_IGNORED_ERRS, write_quorum).unwrap_or(DiskError::Unexpected);
                    let mut rollbacks = Vec::new();
                    let mut rollback_file_infos = file_infos;
                    for (i, err) in errs.iter().enumerate() {
                        if err.is_some() {
                            continue;
                        }

                        if let Some(disk) = coordinator_disks[i].as_ref() {
                            let fi = std::mem::take(&mut rollback_file_infos[i]);
                            let old_data_dir = data_dirs[i];
                            let disk = disk.clone();
                            let dst_bucket = fanout_dst_bucket.clone();
                            let dst_object = fanout_dst_object.clone();
                            rollbacks.push(tokio::spawn(async move {
                                disk.delete_version(
                                    &dst_bucket,
                                    &dst_object,
                                    fi,
                                    false,
                                    DeleteOptions {
                                        undo_write: true,
                                        old_data_dir,
                                        ..Default::default()
                                    },
                                )
                                .await
                            }));
                        }
                    }
                    let _ = join_all(rollbacks).await;
                    if let Some(commit_tx) = commit_tx.take() {
                        let _ = commit_tx.send(Err(ret_err));
                    }
                    return;
                }

                let mut backup_reclaims = Vec::new();
                for (idx, disk) in coordinator_disks.iter().enumerate() {
                    if errs[idx].is_some() {
                        continue;
                    }
                    let Some(rollback_dir) = data_dirs[idx] else {
                        continue;
                    };
                    if cleanup_data_dirs[idx] == Some(rollback_dir) {
                        continue;
                    }
                    let Some(disk) = disk.clone() else {
                        continue;
                    };
                    let dst_bucket = fanout_dst_bucket.clone();
                    let dst_object = fanout_dst_object.clone();
                    backup_reclaims.push(tokio::spawn(async move {
                        let backup_path = format!("{dst_object}/{rollback_dir}/{STORAGE_FORMAT_FILE_BACKUP}");
                        disk.delete(&dst_bucket, &backup_path, DeleteOptions::default()).await
                    }));
                }
                for result in join_all(backup_reclaims).await {
                    match result {
                        Ok(Ok(())) => {}
                        Ok(Err(DiskError::FileNotFound | DiskError::VolumeNotFound)) => {}
                        Ok(Err(err)) => {
                            warn!(
                                dst_bucket = %fanout_dst_bucket,
                                dst_object = %fanout_dst_object,
                                error = %err,
                                "rollback backup reclamation failed after committed rename"
                            );
                        }
                        Err(join_err) => {
                            warn!(
                                dst_bucket = %fanout_dst_bucket,
                                dst_object = %fanout_dst_object,
                                error = %join_err,
                                "rollback backup reclamation task failed after committed rename"
                            );
                        }
                    }
                }
            }
        });

        let quorum_wait_started = rustfs_io_metrics::put_stage_timer();
        let commit = commit_rx.await.map_err(|_| DiskError::Unexpected)?;
        rustfs_io_metrics::record_put_object_stage_duration_from(
            rustfs_io_metrics::PUT_STAGE_SET_DISK_RENAME_QUORUM_WAIT,
            quorum_wait_started,
        );
        commit.map(|mut commit| {
            commit.tail_drain = Some(tail_drain);
            commit
        })
    }

    #[tracing::instrument(level = "debug", skip(disks, file_infos))]
    pub(in crate::set_disk) async fn rename_data_owned(
        disks: &[Option<DiskStore>],
        src_bucket: &str,
        src_object: &str,
        file_infos: Vec<FileInfo>,
        dst_bucket: &str,
        dst_object: &str,
        write_quorum: usize,
    ) -> disk::error::Result<RenameDataCommit> {
        if put_rename_early_ack_enabled() {
            return Self::rename_data_owned_early_ack(
                disks,
                src_bucket,
                src_object,
                file_infos,
                dst_bucket,
                dst_object,
                write_quorum,
            )
            .await;
        }
        if let Some(file_info) = disks
            .iter()
            .zip(file_infos.iter())
            .find_map(|(disk, file_info)| disk.as_ref().map(|_| file_info))
        {
            // Newly encoded metadata does not acquire its per-disk shard index
            // until the fanout below. Validate the shared metadata shape once,
            // using an online slot because shuffled offline slots contain the
            // default placeholder, then validate each assigned geometry in its task.
            if file_info.is_canonical_delete_marker() {
                file_info.validate_for_metadata_read()?;
            } else {
                file_info.validate_for_erasure_write()?;
            }
        }
        let mut errs = Vec::with_capacity(disks.len());

        let src_bucket = Arc::new(src_bucket.to_string());
        let src_object = Arc::new(src_object.to_string());
        let dst_bucket = Arc::new(dst_bucket.to_string());
        let dst_object = Arc::new(dst_object.to_string());

        let disk_count = disks.len();
        let fanout_disks = disks.to_vec();
        let fanout_file_infos = file_infos;
        let fanout_src_bucket = src_bucket.clone();
        let fanout_src_object = src_object.clone();
        let fanout_dst_bucket = dst_bucket.clone();
        let fanout_dst_object = dst_object.clone();
        // Keep one coordinator task so a cancelled caller cannot drop partially
        // completed disk mutations. Per-disk futures stay ordered in `join_all`,
        // preserving slot-indexed quorum and convergence accounting without a
        // scheduler task for every disk.
        let fanout = tokio::spawn(async move {
            let successful_rename_completion_rank =
                rustfs_io_metrics::put_stage_metrics_enabled().then(|| Arc::new(AtomicUsize::new(0)));
            let futures = fanout_disks
                .into_iter()
                .zip(fanout_file_infos.iter())
                .enumerate()
                .map(|(i, (disk, file_info))| {
                    let src_bucket = fanout_src_bucket.clone();
                    let src_object = fanout_src_object.clone();
                    let dst_object = fanout_dst_object.clone();
                    let dst_bucket = fanout_dst_bucket.clone();
                    let successful_rename_completion_rank = successful_rename_completion_rank.clone();

                    std::panic::AssertUnwindSafe(async move {
                        // Test-only introspection guard: counts this operation as
                        // in-flight for the whole body. Compiles to `()` in production.
                        #[allow(clippy::let_unit_value)]
                        let _fanout_task_guard = Self::rename_fanout_task_guard(&dst_object);

                        let Some(disk) = disk else {
                            return Err(DiskError::DiskNotFound);
                        };

                        let is_delete_marker = file_info.is_canonical_delete_marker();
                        let mut local_file_info;
                        let file_info = if file_info.erasure.index == 0 {
                            local_file_info = file_info.clone();
                            local_file_info.erasure.index = i + 1;
                            &local_file_info
                        } else {
                            file_info
                        };
                        if file_info.erasure.index == 0 || (!is_delete_marker && !file_info.has_valid_erasure_geometry()) {
                            return Err(DiskError::FileCorrupt);
                        }

                        // Test-only awaitable pause point right before the disk rename.
                        // A no-op immediately-ready future in production.
                        Self::rename_fanout_barrier(&dst_object, i, rename_fanout_barrier_phase::RENAME).await;

                        if let Some(err) = Self::rename_injected_error(&dst_object, i) {
                            return Err(err);
                        }

                        let disk_wait_started = rustfs_io_metrics::put_stage_timer();
                        let result = disk
                            .rename_data_borrowed(&src_bucket, &src_object, file_info, &dst_bucket, &dst_object)
                            .await;
                        if let Some(disk_wait_started) = disk_wait_started {
                            let duration_ms = disk_wait_started.elapsed().as_secs_f64() * 1000.0;
                            rustfs_io_metrics::record_put_object_stage_duration(
                                rustfs_io_metrics::PUT_STAGE_SET_DISK_RENAME_DISK_WAIT,
                                duration_ms,
                            );
                            let position = if result.is_ok() {
                                let rank = successful_rename_completion_rank
                                    .as_ref()
                                    .map(|rank| rank.fetch_add(1, Ordering::Relaxed) + 1)
                                    .unwrap_or(1);
                                if rank <= write_quorum {
                                    rustfs_io_metrics::PUT_RENAME_DISK_WAIT_COMPLETION_POSITION_QUORUM_FIRST
                                } else {
                                    rustfs_io_metrics::PUT_RENAME_DISK_WAIT_COMPLETION_POSITION_QUORUM_TAIL
                                }
                            } else {
                                rustfs_io_metrics::PUT_RENAME_DISK_WAIT_COMPLETION_POSITION_ERROR
                            };
                            rustfs_io_metrics::record_put_rename_disk_wait_completion(position, duration_ms);
                        }
                        result
                    })
                    .catch_unwind()
                });
            let results = join_all(futures).await;
            (results, fanout_file_infos)
        });

        let mut disk_versions = vec![None; disk_count];
        let mut data_dirs = vec![None; disk_count];
        let mut cleanup_data_dirs = vec![None; disk_count];
        let mut old_current_sizes = vec![None; disk_count];

        let quorum_wait_started = rustfs_io_metrics::put_stage_timer();
        let fanout_result = fanout.await;
        rustfs_io_metrics::record_put_object_stage_duration_from(
            rustfs_io_metrics::PUT_STAGE_SET_DISK_RENAME_QUORUM_WAIT,
            quorum_wait_started,
        );
        let (results, mut file_infos) = fanout_result.map_err(|_| DiskError::Unexpected)?;
        if rustfs_io_metrics::put_stage_metrics_enabled() {
            let mut fanout_success = 0;
            let mut fanout_error = 0;
            let mut fanout_panic = 0;
            for result in &results {
                match result {
                    Ok(Ok(_)) => fanout_success += 1,
                    Ok(Err(_)) => fanout_error += 1,
                    Err(_) => fanout_panic += 1,
                }
            }
            rustfs_io_metrics::record_put_rename_quorum_wait_fanout(
                results.len(),
                write_quorum,
                fanout_success,
                fanout_error,
                fanout_panic,
            );
        }

        for (idx, result) in results.iter().enumerate() {
            match result {
                Ok(Ok(res)) => {
                    data_dirs[idx] = res.rollback_data_dir.or(res.old_data_dir);
                    cleanup_data_dirs[idx] = res.cleanup_data_dir;
                    disk_versions[idx].clone_from(&res.sign);
                    old_current_sizes[idx] = res.old_current_size;
                    errs.push(None);
                }
                Ok(Err(e)) => {
                    errs.push(Some(e.clone()));
                }
                Err(_) => {
                    errs.push(Some(DiskError::Unexpected));
                }
            }
        }

        if issue3031_diag_enabled() {
            let success_count = errs.iter().filter(|err| err.is_none()).count();
            let failure_count = errs.len().saturating_sub(success_count);
            let ignored_failure_count = errs
                .iter()
                .filter(|err| err.as_ref().is_some_and(|err| OBJECT_OP_IGNORED_ERRS.contains(err)))
                .count();
            let data_dir_vote_count = data_dirs.iter().filter(|data_dir| data_dir.is_some()).count();
            let reduced_data_dir = Self::reduce_common_data_dir(&data_dirs, write_quorum);
            warn!(
                target: "rustfs_ecstore::set_disk",
                src_bucket = %src_bucket,
                src_object = %src_object,
                dst_bucket = %dst_bucket,
                dst_object = %dst_object,
                write_quorum,
                disk_count = errs.len(),
                success_count,
                failure_count,
                ignored_failure_count,
                data_dir_vote_count,
                reduced_data_dir = ?reduced_data_dir,
                errs = ?errs,
                data_dirs = ?data_dirs,
                "issue3031_rename_data_quorum_context"
            );
        }

        let mut futures = Vec::with_capacity(disks.len());
        if let Some(ret_err) = reduce_write_quorum_errs(&errs, OBJECT_OP_IGNORED_ERRS, write_quorum) {
            for (i, err) in errs.iter().enumerate() {
                if err.is_some() {
                    continue;
                }

                if let Some(disk) = disks[i].as_ref() {
                    let fi = std::mem::take(&mut file_infos[i]);
                    let old_data_dir = data_dirs[i];
                    let disk = disk.clone();
                    let dst_bucket = dst_bucket.clone();
                    let dst_object = dst_object.clone();
                    futures.push(tokio::spawn(async move {
                        disk.delete_version(
                            &dst_bucket,
                            &dst_object,
                            fi,
                            false,
                            DeleteOptions {
                                undo_write: true,
                                old_data_dir,
                                ..Default::default()
                            },
                        )
                        .await
                    }));
                }
            }

            if issue3031_diag_enabled() {
                warn!(
                    target: "rustfs_ecstore::set_disk",
                    src_bucket = %src_bucket,
                    src_object = %src_object,
                    dst_bucket = %dst_bucket,
                    dst_object = %dst_object,
                    write_quorum,
                    ret_err = %ret_err,
                    errs = ?errs,
                    data_dirs = ?data_dirs,
                    "issue3031_rename_data_quorum_failed"
                );
            }

            let undo_results = join_all(futures).await;
            let undo_error_count = undo_results
                .iter()
                .filter(|result| match result {
                    Err(_) | Ok(Err(_)) => true,
                    Ok(Ok(_)) => false,
                })
                .count();
            if undo_error_count > 0 {
                warn!(
                    target: "rustfs_ecstore::set_disk",
                    dst_bucket = %dst_bucket,
                    dst_object = %dst_object,
                    undo_error_count,
                    "rename_data quorum rollback reported errors"
                );
            }
            return Err(ret_err);
        }

        // The write is authoritatively committed, so the per-disk rollback
        // backup (`object/<rollback_dir>/xl.meta.bkp`) is dead weight now.
        // When the rollback dir doubles as the real dereferenced data dir it
        // is reclaimed wholesale by `commit_rename_data_dir`; a rollback dir
        // reported separately (an overwrite of an inline version, whose dir is
        // synthetic) is excluded from that recursive reclamation for safety
        // (#5703) and must be reclaimed here instead — otherwise every inline
        // overwrite strands a backup file that keeps the object dir non-empty
        // and makes a later DeleteBucket fail with BucketNotEmpty forever.
        // Delete exactly the backup file, never the directory tree: the
        // synthetic UUID is a fixed, publicly-known constant for unversioned
        // objects, so `object/<rollback_dir>` can simultaneously be a
        // legitimate child key's directory — recursively deleting it would
        // reopen the authorization bypass #5703 closed. The non-recursive
        // delete removes the directory only when the backup was its sole
        // content. Best-effort space reclamation — like
        // `commit_rename_data_dir`, this must never negate the already-durable
        // ACK.
        let mut backup_reclaims = Vec::new();
        for (idx, disk) in disks.iter().enumerate() {
            if errs[idx].is_some() {
                continue;
            }
            let Some(rollback_dir) = data_dirs[idx] else {
                continue;
            };
            if cleanup_data_dirs[idx] == Some(rollback_dir) {
                continue;
            }
            let Some(disk) = disk.clone() else {
                continue;
            };
            let dst_bucket = dst_bucket.clone();
            let dst_object = dst_object.clone();
            backup_reclaims.push(tokio::spawn(async move {
                let backup_path = format!("{dst_object}/{rollback_dir}/{STORAGE_FORMAT_FILE_BACKUP}");
                disk.delete(&dst_bucket, &backup_path, DeleteOptions::default()).await
            }));
        }
        for result in join_all(backup_reclaims).await {
            match result {
                Ok(Ok(())) => {}
                Ok(Err(DiskError::FileNotFound | DiskError::VolumeNotFound)) => {}
                Ok(Err(err)) => {
                    warn!(
                        dst_bucket = %dst_bucket,
                        dst_object = %dst_object,
                        error = %err,
                        "rollback backup reclamation failed after committed rename"
                    );
                }
                Err(join_err) => {
                    warn!(
                        dst_bucket = %dst_bucket,
                        dst_object = %dst_object,
                        error = %join_err,
                        "rollback backup reclamation task failed after committed rename"
                    );
                }
            }
        }

        let data_dir = Self::reduce_common_data_dir(&cleanup_data_dirs, write_quorum);
        let convergence = Self::classify_rename_convergence(&disk_versions, &errs);
        let old_current_size = Self::reduce_common_old_current_size(&old_current_sizes, write_quorum);
        let online_disks = Self::eval_disks(disks, &errs);
        let committed_slot = online_disks.iter().position(Option::is_some).ok_or(DiskError::Unexpected)?;
        let committed_file_info = std::mem::take(&mut file_infos[committed_slot]);
        let cleanup_disks = if let Some(data_dir) = data_dir {
            disks
                .iter()
                .zip(errs.iter())
                .zip(cleanup_data_dirs.iter())
                .map(|((disk, err), old_data_dir)| {
                    if err.is_none() && *old_data_dir == Some(data_dir) {
                        disk.clone()
                    } else {
                        None
                    }
                })
                .collect()
        } else {
            vec![None; disks.len()]
        };

        Ok(RenameDataCommit {
            online_disks,
            convergence,
            data_dir,
            cleanup_disks,
            old_current_size,
            committed_file_info,
            tail_drain: None,
        })
    }

    /// rustfs/backlog#1009: reduce the per-disk observations of the
    /// destination's previous current version to one set-level value, mirroring
    /// `reduce_common_data_dir`: the observation reported by at least
    /// `write_quorum` disks wins; anything short of that (disk errors, unknown
    /// votes from pre-#1009 peers or unparsable metadata, genuine divergence)
    /// yields `None` (unknown). Unknown per-disk entries never vote.
    pub(in crate::set_disk) fn reduce_common_old_current_size(
        old_current_sizes: &[Option<OldCurrentSize>],
        write_quorum: usize,
    ) -> Option<OldCurrentSize> {
        let mut counts: HashMap<OldCurrentSize, usize> = HashMap::new();

        for observation in old_current_sizes.iter().flatten().copied() {
            *counts.entry(observation).or_insert(0) += 1;
        }

        let mut max = 0;
        let mut old_current_size = None;
        for (observation, count) in counts {
            if count > max {
                max = count;
                old_current_size = Some(observation);
            }
        }

        if max >= write_quorum { old_current_size } else { None }
    }

    /// Classify a *post-quorum* `rename_data` commit into an explicit
    /// convergence outcome (rustfs/backlog#1321). This runs only after the
    /// write-quorum gate has already passed, so every returned variant
    /// describes an already-durable, already-ACKable write; the decision here
    /// is purely "do the committed replicas need heal to converge".
    ///
    /// The per-disk version signature (`disk_versions[i]`) is used only as
    /// comparison material — it is deliberately NOT overloaded to also mean
    /// "needs heal". A `Some(bytes)` signature carries the concatenated
    /// version-id bytes of a disk that observed <=10 versions; a `None`
    /// signature is a disk that committed but deliberately produced no
    /// signature (>10 versions). A `None` entry for a *failed* disk never
    /// reaches this point as a signature because failures are handled first.
    pub(in crate::set_disk) fn classify_rename_convergence(
        disk_versions: &[Option<Vec<u8>>],
        errs: &[Option<DiskError>],
    ) -> RenameConvergence {
        // Any failed / offline disk that got past the write-quorum gate means a
        // committed replica is missing or stale: converge it via heal,
        // regardless of what the surviving disks' signatures say.
        if errs.iter().any(|err| err.is_some()) {
            return RenameConvergence::PartialCommit;
        }

        // Every disk committed. Compare their reported version signatures.
        let mut seen: Option<&Vec<u8>> = None;
        let mut signed_count = 0usize;
        let mut divergent = false;
        for signature in disk_versions.iter() {
            let Some(sig) = signature.as_ref() else {
                continue;
            };
            signed_count += 1;
            match seen {
                None => seen = Some(sig),
                Some(prev) if prev != sig => divergent = true,
                Some(_) => {}
            }
        }

        if divergent {
            return RenameConvergence::SignatureDivergent;
        }
        if signed_count == 0 {
            // No disk produced a signature (all observed >10 versions): a
            // latent divergence cannot be proven or ruled out here, so it is
            // left to the scanner backstop rather than enqueued for heal.
            return RenameConvergence::Unknown;
        }
        if signed_count < disk_versions.len() {
            // A mix of signed (<=10 versions) and unsigned (>10 versions) disks
            // is itself a version-count divergence between committed replicas:
            // reconcile it via heal.
            return RenameConvergence::SignatureDivergent;
        }
        RenameConvergence::AllSuccessIdentical
    }

    /// Reclaim the old (now dereferenced) `object/<old_data_dir>` on the disks
    /// that just committed the new version.
    ///
    /// # Deliberate divergence from MinIO (backlog#898)
    ///
    /// This runs *after* the write is authoritatively committed (`rename_data`
    /// returned `Ok`, i.e. the new version is durable on >= write_quorum disks
    /// and immediately readable). The target `object/<old_data_dir>` has already
    /// been dereferenced from every committed replica's `xl.meta`, so removing
    /// it is pure space reclamation. MinIO couples a below-quorum failure here
    /// back into the client response (`erasure-object.go:1577`); RustFS
    /// historically mirrored that (`reduce_write_quorum_errs` -> `Err` ->
    /// 503/SlowDown), producing a **false-negative ACK** for an already-durable
    /// write. We deliberately break that coupling: this function **never returns
    /// `Err`**. It returns a structured [`OldDataDirCleanup`] receipt whose
    /// fields are signals only — none of them can negate an already-ACKed write.
    ///
    /// The disk-health signal that MinIO raises via 503 is not dropped; the
    /// caller re-surfaces it by enqueuing an object heal on residue (see
    /// `report_old_data_dir_cleanup`), and the leaked residue is made observable
    /// via `rustfs_old_data_dir_leaked_total`.
    #[tracing::instrument(level = "debug", skip(self, disks))]
    pub(in crate::set_disk) async fn commit_rename_data_dir(
        &self,
        disks: &[Option<DiskStore>],
        bucket: &str,
        object: &str,
        old_data_dir: &str,
        committed_data_dir: &str,
        write_quorum: usize,
    ) -> OldDataDirCleanup {
        crate::hp_guard!("SetDisks::commit_rename_data_dir");
        // Anti-misdelete guard (parity retained): MinIO sets `res.OldDataDir=""`
        // when old == new (`xl-storage.go:2796`) and `commitRenameDataDir` skips
        // the empty value (`:1837`). Rather miss a reclaim than delete the data
        // dir we just committed. `#864` isolation: never touch the commit point.
        if old_data_dir.is_empty() || old_data_dir == committed_data_dir {
            return OldDataDirCleanup::default();
        }

        let file_path = Arc::new(format!("{object}/{old_data_dir}"));
        let bucket = Arc::new(bucket.to_string());
        // A disk slot was actually targeted for deletion iff it is `Some`; the
        // `None` slots are ignored placeholders and must be excluded from the
        // attempted/reclaimed/residue accounting (they leak nothing).
        let attempted: Vec<bool> = disks.iter().map(|d| d.is_some()).collect();
        // Test-only object filter for the delete fault-injection seam. In
        // non-test builds `cleanup_injected_error` is a `None`-returning no-op,
        // so this clone and the per-disk check compile away to nothing.
        let object_for_fault = Arc::new(object.to_string());

        let futures = disks.iter().enumerate().map(|(idx, disk)| {
            let file_path = file_path.clone();
            let bucket = bucket.clone();
            let disk = disk.clone();
            let object_for_fault = object_for_fault.clone();
            tokio::spawn(async move {
                // Test-only introspection guard + awaitable pause point for the
                // old-data-dir cleanup fan-out. Both compile away in production.
                #[allow(clippy::let_unit_value)]
                let _fanout_task_guard = Self::rename_fanout_task_guard(&object_for_fault);
                Self::rename_fanout_barrier(&object_for_fault, idx, rename_fanout_barrier_phase::CLEANUP).await;

                if let Some(err) = Self::cleanup_injected_error(&object_for_fault, idx) {
                    return (false, Some(err));
                }
                if let Some(disk) = disk {
                    match disk
                        .delete_data_dir(
                            &bucket,
                            &file_path,
                            DeleteOptions {
                                recursive: true,
                                ..Default::default()
                            },
                        )
                        .await
                    {
                        Ok(DataDirDeleteStatus::Deleted) => (false, None),
                        Ok(DataDirDeleteStatus::Deferred) => (true, None),
                        Err(err) => (false, Some(err)),
                    }
                } else {
                    // `None` slot: ignored placeholder. It is not `attempted`, so
                    // classification excludes it from residue regardless.
                    (false, Some(DiskError::DiskNotFound))
                }
            })
        });
        let mut deferred = 0usize;
        let errs: Vec<Option<DiskError>> = join_all(futures)
            .await
            .into_iter()
            .map(|result| match result {
                Ok((was_deferred, err)) => {
                    deferred += usize::from(was_deferred);
                    err
                }
                Err(join_err) => Some(DiskError::other(format!("old data dir cleanup task failed: {join_err}"))),
            })
            .collect();

        let mut cleanup = classify_old_data_dir_cleanup(&errs, &attempted, write_quorum);
        cleanup.deferred = deferred;
        cleanup.reclaimed = cleanup.reclaimed.saturating_sub(deferred);
        cleanup
    }

    /// Test-only fault-injection seam for the old-data-dir cleanup path
    /// (backlog#898 §5). In production this is inlined to `None` and adds no
    /// behavior; only the `#[cfg(test)]` variant consults the fault registry.
    #[cfg(test)]
    fn cleanup_injected_error(object: &str, disk_index: usize) -> Option<DiskError> {
        cleanup_fault_injection::injected_error(object, disk_index)
    }

    #[cfg(not(test))]
    #[inline(always)]
    fn cleanup_injected_error(_object: &str, _disk_index: usize) -> Option<DiskError> {
        None
    }

    /// Test-only fault-injection seam for the rename-data fan-out. In production
    /// this is inlined to `None`; only the `#[cfg(test)]` variant consults the
    /// fault registry after the per-disk barrier and before mutating the disk.
    #[cfg(test)]
    fn rename_injected_error(object: &str, disk_index: usize) -> Option<DiskError> {
        rename_fault_injection::injected_error(object, disk_index)
    }

    #[cfg(not(test))]
    #[inline(always)]
    fn rename_injected_error(_object: &str, _disk_index: usize) -> Option<DiskError> {
        None
    }

    /// Test-only seam that records one per-disk `read_version` metadata RPC for
    /// the call-counter registry (backlog#1325). In production this is inlined to
    /// nothing and adds no behavior; only the `#[cfg(test)]` variant touches the
    /// registry. Placed inside the fan-out spawn tasks so counts are observed
    /// even though the increments run on arbitrary runtime worker threads.
    #[cfg(test)]
    #[inline]
    fn record_read_version_call(object: &str, disk_index: usize) {
        disk_call_counters::record(object, disk_call_counters::KIND_READ_VERSION, disk_index);
    }

    #[cfg(not(test))]
    #[inline(always)]
    fn record_read_version_call(_object: &str, _disk_index: usize) {}

    #[cfg(test)]
    #[inline]
    async fn read_version_fanout_barrier(object: &str, disk_index: usize) {
        rename_fanout_barrier::checkpoint(object, disk_index, rename_fanout_barrier::PHASE_READ_VERSION).await;
    }

    /// Test-only awaitable pause point for the rename/commit fan-out (backlog#1325,
    /// serving the barrier-style acceptances of #1312 / #1319 / #1313). `phase` is
    /// [`rename_fanout_barrier::PHASE_RENAME`] or `PHASE_CLEANUP`. When a test has
    /// armed a barrier for `object` at this `(disk_index, phase)`, the spawned
    /// fan-out task blocks here until the test releases it, so the test can await
    /// the pause point and then introspect in-flight background disk work. In
    /// production this awaits an immediately-ready no-op future — no yield, no
    /// registry access, no behavior change.
    #[cfg(test)]
    #[inline]
    async fn rename_fanout_barrier(object: &str, disk_index: usize, phase: &'static str) {
        rename_fanout_barrier::checkpoint(object, disk_index, phase).await;
    }

    #[cfg(not(test))]
    #[inline(always)]
    #[allow(clippy::unused_async)]
    async fn rename_fanout_barrier(_object: &str, _disk_index: usize, _phase: &'static str) {}

    /// Test-only RAII counter for one in-flight rename/commit fan-out task
    /// (backlog#1325). Held for the whole spawned task body so a test observing
    /// `object` can assert "background disk writes are still running" while the
    /// fan-out is paused and "no background disk writes remain" once it drains —
    /// the exact signal #1312 needs after a lock release. In production this
    /// returns `()` and touches no registry.
    #[cfg(test)]
    #[inline]
    fn rename_fanout_task_guard(object: &str) -> rename_fanout_barrier::TaskGuard {
        rename_fanout_barrier::task_guard(object)
    }

    #[cfg(not(test))]
    #[inline(always)]
    fn rename_fanout_task_guard(_object: &str) {}

    /// Report a post-commit old-data-dir cleanup receipt: emit metrics, warn on
    /// residue/below-quorum, and — on residue — enqueue an object heal.
    ///
    /// Open-question defaults adopted from backlog#898 §7 (flagged for
    /// maintainer confirmation):
    /// - the heal is enqueued **inline** (awaited), matching the existing
    ///   `multipart.rs` fire-and-forget precedent; it runs only on the rare
    ///   residue path, after the object write lock is released, and is
    ///   deduplicated/back-pressured by heal admission — so the ACK tail impact
    ///   is negligible. (Alternative: `tokio::spawn`.)
    /// - the heal is enqueued strictly on `has_residue()`, not on
    ///   `below_quorum` alone (a pure parity lens that can flag offline/`None`
    ///   slots which leak nothing).
    pub(in crate::set_disk) async fn report_old_data_dir_cleanup(
        &self,
        bucket: &str,
        object: &str,
        old_dir: &str,
        c: &OldDataDirCleanup,
    ) {
        let actions = old_data_dir_cleanup_actions(c);

        rustfs_io_metrics::record_old_data_dir_cleanup(c.attempted, c.reclaimed, c.unreclaimed_disks.len(), c.below_quorum);

        if c.deferred > 0 {
            debug!(
                event = EVENT_SET_DISK_WRITE,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_SET_DISK,
                bucket = %bucket,
                object = %object,
                old_data_dir = %old_dir,
                deferred = c.deferred,
                state = "old_data_cleanup_deferred",
                "Old data directory cleanup deferred for active snapshot leases"
            );
        }

        if actions.warn {
            warn!(
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_SET_DISK,
                bucket = %bucket,
                object = %object,
                old_dir = %old_dir,
                attempted = c.attempted,
                reclaimed = c.reclaimed,
                unreclaimed = ?c.unreclaimed_disks,
                below_quorum = c.below_quorum,
                "old data dir cleanup left residue after committed write"
            );
        }

        if actions.enqueue_heal {
            // Disk-health signal (replacing MinIO's 503) + convergence hook: the
            // leaked local data dir is physically reclaimed by heal_object ->
            // reclaim_orphan_data_dirs. Reuses the existing heal channel, which
            // deduplicates and back-pressures via admission; failures only drop
            // the return value (same shape as multipart's existing heal enqueue).
            let _ =
                rustfs_common::heal_channel::send_heal_request(rustfs_common::heal_channel::create_heal_request_with_options(
                    bucket.to_string(),
                    Some(object.to_string()),
                    false,
                    Some(rustfs_common::heal_channel::HealChannelPriority::Normal),
                    Some(self.pool_index),
                    Some(self.set_index),
                ))
                .await;
        }
    }

    #[tracing::instrument(skip(self))]
    pub(in crate::set_disk) async fn cleanup_multipart_path(&self, paths: &[String]) {
        if paths.is_empty() {
            return;
        }
        let disks = self.get_disks_internal().await;

        let mut errs = Vec::with_capacity(disks.len());

        // Use improved simple batch processor instead of join_all for better performance
        let processor = runtime_sources::batch_processors().write_processor();

        let tasks: Vec<_> = disks
            .iter()
            .map(|disk| {
                let disk = disk.clone();
                let paths = paths.to_vec();

                async move {
                    if let Some(disk) = disk {
                        disk.delete_paths(RUSTFS_META_MULTIPART_BUCKET, &paths).await
                    } else {
                        Err(DiskError::DiskNotFound)
                    }
                }
            })
            .collect();

        let results = processor.execute_batch(tasks).await;
        for result in results {
            match result {
                Ok(_) => {
                    errs.push(None);
                }
                Err(e) => {
                    errs.push(Some(e));
                }
            }
        }

        if errs.iter().any(|e| e.is_some()) {
            warn!("cleanup_multipart_path errs {:?}", &errs);
        }
    }

    async fn recover_part_transaction(&self, dst_object: &str, write_quorum: usize) -> disk::error::Result<bool> {
        struct PartTransactionObservation {
            transaction_meta: Option<Bytes>,
            current_meta: Option<Bytes>,
            rollback: bool,
            err: Option<DiskError>,
        }

        enum PartTransactionOutcome {
            Commit,
            Rollback,
        }

        let disks = self.get_disks_internal().await;
        let transaction_path = part_transaction_path(dst_object);
        let transaction_meta_path = format!("{transaction_path}/{PART_TRANSACTION_NEW_META}");
        let rollback_path = format!("{transaction_path}/{PART_TRANSACTION_ROLLBACK}");
        let current_meta_path = format!("{dst_object}.meta");

        let reads = disks.iter().map(|disk| {
            let disk = disk.clone();
            let transaction_meta_path = transaction_meta_path.clone();
            let rollback_path = rollback_path.clone();
            let current_meta_path = current_meta_path.clone();
            async move {
                let Some(disk) = disk else {
                    return PartTransactionObservation {
                        transaction_meta: None,
                        current_meta: None,
                        rollback: false,
                        err: Some(DiskError::DiskNotFound),
                    };
                };
                let transaction_meta = match disk.read_all(RUSTFS_META_MULTIPART_BUCKET, &transaction_meta_path).await {
                    Ok(meta) => Some(meta),
                    Err(DiskError::FileNotFound) => None,
                    Err(err) => {
                        return PartTransactionObservation {
                            transaction_meta: None,
                            current_meta: None,
                            rollback: false,
                            err: Some(err),
                        };
                    }
                };
                let rollback = match disk.read_all(RUSTFS_META_MULTIPART_BUCKET, &rollback_path).await {
                    Ok(_) => true,
                    Err(DiskError::FileNotFound) => false,
                    Err(err) => {
                        return PartTransactionObservation {
                            transaction_meta,
                            current_meta: None,
                            rollback: false,
                            err: Some(err),
                        };
                    }
                };
                let current_meta = match disk.read_all(RUSTFS_META_MULTIPART_BUCKET, &current_meta_path).await {
                    Ok(meta) => Some(meta),
                    Err(DiskError::FileNotFound | DiskError::DiskNotFound) => None,
                    Err(_) => None,
                };
                PartTransactionObservation {
                    transaction_meta,
                    current_meta,
                    rollback,
                    err: None,
                }
            }
        });
        let observations = join_all(reads).await;
        let read_errs = observations
            .iter()
            .map(|observation| observation.err.clone())
            .collect::<Vec<_>>();
        if let Some(err) = reduce_write_quorum_errs(&read_errs, OBJECT_OP_IGNORED_ERRS, write_quorum) {
            return Err(err);
        }

        if observations
            .iter()
            .filter(|observation| observation.err.is_none())
            .all(|observation| observation.transaction_meta.is_none())
        {
            return Ok(false);
        }

        let mut current_counts: HashMap<Bytes, usize> = HashMap::new();
        let mut transaction_meta_values = HashSet::new();
        for observation in observations.iter().filter(|observation| observation.err.is_none()) {
            if let Some(current) = &observation.current_meta {
                *current_counts.entry(current.clone()).or_default() += 1;
            }
            if let Some(transaction_meta) = &observation.transaction_meta {
                transaction_meta_values.insert(transaction_meta.clone());
            }
        }
        let current_quorum = current_counts
            .into_iter()
            .find_map(|(meta, count)| (count >= write_quorum).then_some(meta));

        let old_meta_path = format!("{transaction_path}/{PART_TRANSACTION_OLD_META}");
        let old_meta_absent_path = format!("{transaction_path}/old.meta.absent");
        let mut outcomes = Vec::with_capacity(observations.len());
        for observation in &observations {
            let outcome = if observation.err.is_none() && observation.transaction_meta.is_none() {
                match &observation.current_meta {
                    Some(current_meta) if transaction_meta_values.contains(current_meta) => Some(PartTransactionOutcome::Commit),
                    _ => Some(PartTransactionOutcome::Rollback),
                }
            } else {
                None
            };
            outcomes.push(outcome);
        }
        let decisions = observations
            .iter()
            .enumerate()
            .filter_map(|(index, observation)| {
                if observation.err.is_some() {
                    return None;
                }
                observation
                    .transaction_meta
                    .as_ref()
                    .map(|meta| (index, meta.clone(), observation.rollback))
            })
            .map(|(index, transaction_meta, rollback)| {
                let disk = disks[index].clone();
                let old_meta_path = old_meta_path.clone();
                let old_meta_absent_path = old_meta_absent_path.clone();
                let current_quorum = current_quorum.clone();
                async move {
                    let Some(disk) = disk else {
                        return (index, Err(DiskError::DiskNotFound));
                    };
                    let result = async {
                        let action = if rollback {
                            PartTransactionAction::Rollback
                        } else if current_quorum.as_ref() == Some(&transaction_meta) {
                            PartTransactionAction::Commit
                        } else if let Some(current_quorum) = current_quorum {
                            match disk.read_all(RUSTFS_META_MULTIPART_BUCKET, &old_meta_path).await {
                                Ok(old_meta) if old_meta == current_quorum => PartTransactionAction::Rollback,
                                Ok(_) => PartTransactionAction::Commit,
                                Err(DiskError::FileNotFound) => {
                                    match disk.read_all(RUSTFS_META_MULTIPART_BUCKET, &old_meta_absent_path).await {
                                        Ok(_) => PartTransactionAction::Commit,
                                        Err(_) => return Err(DiskError::FileCorrupt),
                                    }
                                }
                                Err(err) => return Err(err),
                            }
                        } else {
                            PartTransactionAction::Rollback
                        };
                        disk.settle_part_transaction(RUSTFS_META_MULTIPART_BUCKET, dst_object, action)
                            .await?;
                        let outcome = match action {
                            PartTransactionAction::Commit => PartTransactionOutcome::Commit,
                            PartTransactionAction::Rollback => PartTransactionOutcome::Rollback,
                        };
                        Ok(outcome)
                    };
                    (index, result.await)
                }
            });

        let results = join_all(decisions).await;
        let mut settle_errs = read_errs;
        for result in results {
            match result {
                (index, Ok(outcome)) => outcomes[index] = Some(outcome),
                (index, Err(err)) => settle_errs[index] = Some(err),
            }
        }
        if let Some(err) = reduce_write_quorum_errs(&settle_errs, OBJECT_OP_IGNORED_ERRS, write_quorum) {
            return Err(err);
        }

        let commit_count = outcomes
            .iter()
            .filter(|outcome| matches!(outcome, Some(PartTransactionOutcome::Commit)))
            .count();
        if commit_count >= write_quorum {
            return Ok(true);
        }
        let rollback_count = outcomes
            .iter()
            .filter(|outcome| matches!(outcome, Some(PartTransactionOutcome::Rollback)))
            .count();
        if rollback_count >= write_quorum {
            return Ok(false);
        }

        Err(DiskError::ErasureWriteQuorum)
    }

    pub(in crate::set_disk) async fn recover_part_transactions(
        &self,
        part_path: &str,
        read_quorum: usize,
        write_quorum: usize,
    ) -> disk::error::Result<()> {
        let disks = self.get_disks_internal().await;
        let listings = join_all(disks.iter().map(|disk| {
            let disk = disk.clone();
            async move {
                let Some(disk) = disk else {
                    return Err(DiskError::DiskNotFound);
                };
                disk.list_dir(RUSTFS_META_MULTIPART_BUCKET, RUSTFS_META_MULTIPART_BUCKET, part_path, -1)
                    .await
            }
        }))
        .await;

        let mut transaction_parts = HashSet::new();
        let mut errs = Vec::with_capacity(listings.len());
        for listing in listings {
            match listing {
                Ok(entries) => {
                    errs.push(None);
                    for entry in entries {
                        let name = entry.trim_end_matches('/');
                        let Some(part_number) = name
                            .strip_prefix(".part.")
                            .and_then(|name| name.strip_suffix(".rustfs-txn"))
                            .and_then(|number| number.parse::<usize>().ok())
                        else {
                            continue;
                        };
                        transaction_parts.insert(part_number);
                    }
                }
                Err(err) => errs.push(Some(err)),
            }
        }
        if let Some(err) = reduce_read_quorum_errs(&errs, OBJECT_OP_IGNORED_ERRS, read_quorum) {
            return Err(err);
        }

        for part_number in transaction_parts {
            self.recover_part_transaction(&format!("{part_path}part.{part_number}"), write_quorum)
                .await?;
        }
        Ok(())
    }

    #[tracing::instrument(skip(disks, meta))]
    #[allow(clippy::too_many_arguments)]
    pub(in crate::set_disk) async fn rename_part(
        &self,
        disks: &[Option<DiskStore>],
        src_bucket: &str,
        src_object: &str,
        dst_bucket: &str,
        dst_object: &str,
        meta: Bytes,
        write_quorum: usize,
        quorum_context: Option<MultipartWriteQuorumContext<'_>>,
    ) -> disk::error::Result<Vec<Option<DiskStore>>> {
        self.recover_part_transaction(dst_object, write_quorum).await?;

        let src_bucket = Arc::new(src_bucket.to_string());
        let src_object = Arc::new(src_object.to_string());
        let dst_bucket = Arc::new(dst_bucket.to_string());
        let dst_object = Arc::new(dst_object.to_string());

        let prepare_tasks = disks.iter().map(|disk| {
            let disk = disk.clone();
            let src_bucket = src_bucket.clone();
            let src_object = src_object.clone();
            let dst_bucket = dst_bucket.clone();
            let dst_object = dst_object.clone();
            let meta = meta.clone();
            async move {
                let disk = disk?;
                Some(
                    disk.prepare_part_transaction(&src_bucket, &src_object, &dst_bucket, &dst_object, meta)
                        .await,
                )
            }
        });
        let prepare_results = join_all(prepare_tasks).await;
        let prepare_errs = prepare_results
            .into_iter()
            .map(|result| match result {
                Some(Ok(())) => None,
                Some(Err(err)) => Some(err),
                None => Some(DiskError::DiskNotFound),
            })
            .collect::<Vec<_>>();
        let prepared_disks = Self::eval_disks(disks, &prepare_errs);
        if reduce_write_quorum_errs(&prepare_errs, OBJECT_OP_IGNORED_ERRS, write_quorum).is_some() {
            self.recover_part_transaction(&dst_object, write_quorum).await?;
            return Err(DiskError::ErasureWriteQuorum);
        }

        let mut errs = Vec::with_capacity(disks.len());

        let futures = prepared_disks.iter().map(|disk| {
            let disk = disk.clone();
            let meta = meta.clone();
            let src_bucket = src_bucket.clone();
            let src_object = src_object.clone();
            let dst_bucket = dst_bucket.clone();
            let dst_object = dst_object.clone();
            tokio::spawn(async move {
                if let Some(disk) = disk {
                    disk.rename_part(&src_bucket, &src_object, &dst_bucket, &dst_object, meta)
                        .await
                } else {
                    Err(DiskError::DiskNotFound)
                }
            })
        });

        let results = join_all(futures).await;
        for result in results {
            match result? {
                Ok(_) => {
                    errs.push(None);
                }
                Err(e) => {
                    errs.push(Some(e));
                }
            }
        }

        if issue3031_diag_enabled() {
            let success_count = errs.iter().filter(|err| err.is_none()).count();
            let error_count = errs.len().saturating_sub(success_count);
            let disk_not_found_count = errs.iter().filter(|err| matches!(err, Some(DiskError::DiskNotFound))).count();
            let file_not_found_count = errs.iter().filter(|err| matches!(err, Some(DiskError::FileNotFound))).count();
            warn!(
                target: "rustfs_ecstore::set_disk",
                src_bucket = %src_bucket,
                src_object = %src_object,
                dst_bucket = %dst_bucket,
                dst_object = %dst_object,
                write_quorum = write_quorum,
                disk_count = errs.len(),
                success_count = success_count,
                error_count = error_count,
                disk_not_found_count = disk_not_found_count,
                file_not_found_count = file_not_found_count,
                "issue3031_rename_part_context"
            );
        }

        let reduced_err = reduce_write_quorum_errs(&errs, OBJECT_OP_IGNORED_ERRS, write_quorum);
        if let Some(err) = reduced_err {
            let rollbacks = prepared_disks.iter().filter_map(|disk| {
                disk.clone().map(|disk| {
                    let dst_object = dst_object.clone();
                    async move {
                        disk.settle_part_transaction(RUSTFS_META_MULTIPART_BUCKET, &dst_object, PartTransactionAction::Rollback)
                            .await
                    }
                })
            });
            let rollback_results = join_all(rollbacks).await;
            self.recover_part_transaction(&dst_object, write_quorum).await?;
            if let Some(rollback_err) = rollback_results.iter().find_map(|result| result.as_ref().err()) {
                warn!(error = %rollback_err, "rename_part rollback did not settle on every prepared disk");
            }
            if let Some(context) = quorum_context {
                log_multipart_write_quorum_failure(context, &errs, write_quorum, &err);
            } else {
                warn!("rename_part errs {:?}", &errs);
            }
            return Err(err);
        }

        let committed = self.recover_part_transaction(&dst_object, write_quorum).await?;
        if !committed {
            let err = DiskError::ErasureWriteQuorum;
            if let Some(context) = quorum_context {
                log_multipart_write_quorum_failure(context, &errs, write_quorum, &err);
            } else {
                warn!("rename_part errs {:?}", &errs);
            }
            return Err(err);
        }

        Ok(Self::eval_disks(&prepared_disks, &errs))
    }

    pub(in crate::set_disk) fn eval_disks(disks: &[Option<DiskStore>], errs: &[Option<DiskError>]) -> Vec<Option<DiskStore>> {
        if disks.len() != errs.len() {
            return Vec::new();
        }

        let mut online_disks = vec![None; disks.len()];

        for (i, err_op) in errs.iter().enumerate() {
            if err_op.is_none() {
                online_disks[i].clone_from(&disks[i]);
            }
        }

        online_disks
    }

    #[tracing::instrument(skip(disks, files))]
    pub(in crate::set_disk) async fn write_unique_file_info(
        disks: &[Option<DiskStore>],
        org_bucket: &str,
        bucket: &str,
        prefix: &str,
        files: &[FileInfo],
        write_quorum: usize,
    ) -> disk::error::Result<()> {
        let mut futures = Vec::with_capacity(disks.len());
        let mut errs = Vec::with_capacity(disks.len());

        for (i, disk) in disks.iter().enumerate() {
            let mut file_info = files[i].clone();
            file_info.erasure.index = i + 1;
            futures.push(async move {
                if let Some(disk) = disk {
                    disk.write_metadata(org_bucket, bucket, prefix, file_info).await
                } else {
                    Err(DiskError::DiskNotFound)
                }
            });
        }

        let results = join_all(futures).await;
        for result in results {
            match result {
                Ok(_) => {
                    errs.push(None);
                }
                Err(e) => {
                    errs.push(Some(e));
                }
            }
        }

        if let Some(err) = reduce_write_quorum_errs(&errs, OBJECT_OP_IGNORED_ERRS, write_quorum) {
            let mut revert_futures = Vec::with_capacity(disks.len());
            for (i, err) in errs.iter().enumerate() {
                if err.is_some() {
                    continue;
                }

                if let Some(disk) = disks[i].as_ref() {
                    let path = path_join_buf(&[prefix, STORAGE_FORMAT_FILE]);
                    revert_futures.push(async move {
                        if let Err(err) = disk
                            .delete(
                                bucket,
                                &path,
                                DeleteOptions {
                                    recursive: true,
                                    ..Default::default()
                                },
                            )
                            .await
                        {
                            warn!("write meta revert err {:?}", err);
                        }
                    });
                }
            }

            join_all(revert_futures).await;
            return Err(err);
        }
        Ok(())
    }

    pub(in crate::set_disk) async fn update_object_meta(
        &self,
        bucket: &str,
        object: &str,
        fi: FileInfo,
        disks: &[Option<DiskStore>],
    ) -> disk::error::Result<()> {
        self.update_object_meta_with_opts(bucket, object, fi, disks, &UpdateMetadataOpts::default())
            .await
    }

    pub(in crate::set_disk) async fn update_object_meta_with_opts(
        &self,
        bucket: &str,
        object: &str,
        fi: FileInfo,
        disks: &[Option<DiskStore>],
        opts: &UpdateMetadataOpts,
    ) -> disk::error::Result<()> {
        if fi.metadata.is_empty() && !opts.replace_user_metadata {
            return Ok(());
        }

        self.invalidate_get_object_metadata_cache(bucket, object).await;

        let mut futures = Vec::with_capacity(disks.len());

        let mut errs = Vec::with_capacity(disks.len());

        for disk in disks.iter() {
            let fi = fi.clone();
            futures.push(async move {
                if let Some(disk) = disk {
                    disk.update_metadata(bucket, object, fi, opts).await
                } else {
                    Err(DiskError::DiskNotFound)
                }
            })
        }

        let results = join_all(futures).await;
        for result in results {
            match result {
                Ok(_) => {
                    errs.push(None);
                }
                Err(e) => {
                    errs.push(Some(e));
                }
            }
        }

        if let Some(err) = reduce_write_quorum_errs(&errs, OBJECT_OP_IGNORED_ERRS, fi.write_quorum(self.default_write_quorum())) {
            return Err(err);
        }

        self.invalidate_get_object_metadata_cache(bucket, object).await;

        Ok(())
    }

    pub(in crate::set_disk) async fn delete_if_dangling(
        &self,
        bucket: &str,
        object: &str,
        meta_arr: &[FileInfo],
        errs: &[Option<DiskError>],
        data_errs_by_part: &HashMap<usize, Vec<usize>>,
        opts: ObjectOptions,
    ) -> disk::error::Result<FileInfo> {
        let (m, can_heal) = is_object_dangling(meta_arr, errs, data_errs_by_part);

        if !can_heal {
            return Err(DiskError::ErasureReadQuorum);
        }

        // Recently written objects get a grace window before dangling cleanup: after
        // an unclean shutdown some disks may still be catching up (or carry writes
        // that were never made durable), and deleting the surviving shards right away
        // turns a partial loss into a total one. Skip deletion and leave the object
        // for a later heal/scanner pass to re-evaluate.
        if file_info_is_valid_for_metadata(&m)
            && let Some(mod_time) = m.mod_time
        {
            let grace = dangling_delete_grace();
            if !grace.is_zero() && OffsetDateTime::now_utc() - mod_time < grace {
                info!(
                    bucket = bucket,
                    object = object,
                    mod_time = %mod_time,
                    grace_secs = grace.whole_seconds(),
                    "skipping dangling-object deletion within grace window"
                );
                return Err(DiskError::ErasureReadQuorum);
            }
        }

        let mut tags: HashMap<String, String> = HashMap::new();
        tags.insert("set".to_string(), self.set_index.to_string());
        tags.insert("pool".to_string(), self.pool_index.to_string());
        tags.insert("merrs".to_string(), join_errs(errs));
        tags.insert("derrs".to_string(), format!("{data_errs_by_part:?}"));
        if file_info_is_valid_for_metadata(&m) {
            tags.insert("sz".to_string(), m.size.to_string());
            tags.insert(
                "mt".to_string(),
                m.mod_time
                    .as_ref()
                    .map_or(String::new(), |mod_time| mod_time.unix_timestamp().to_string()),
            );
            tags.insert("d:p".to_string(), format!("{}:{}", m.erasure.data_blocks, m.erasure.parity_blocks));
        } else {
            tags.insert("invalid".to_string(), "1".to_string());
            tags.insert(
                "d:p".to_string(),
                format!("{}:{}", self.set_drive_count - self.default_parity_count, self.default_parity_count),
            );
        }
        let mut offline = 0;
        for (i, err) in errs.iter().enumerate() {
            let mut found = false;
            if let Some(err) = err
                && err == &DiskError::DiskNotFound
            {
                found = true;
            }
            for p in data_errs_by_part {
                if let Some(v) = p.1.get(i)
                    && *v == CHECK_PART_DISK_NOT_FOUND
                {
                    found = true;
                    break;
                }
            }

            if found {
                offline += 1;
            }
        }

        if offline > 0 {
            tags.insert("offline".to_string(), offline.to_string());
        }

        let mut fi = FileInfo::default();
        if let Some(ref version_id) = opts.version_id {
            fi.version_id = Uuid::parse_str(version_id).ok();
        }

        fi.set_tier_free_version_id(&Uuid::new_v4().to_string());

        let disks = self.get_disks_internal().await;

        let mut futures = Vec::with_capacity(disks.len());
        for disk_op in disks.iter() {
            let bucket = bucket.to_string();
            let object = object.to_string();
            let fi = fi.clone();
            futures.push(async move {
                if let Some(disk) = disk_op {
                    disk.delete_version(&bucket, &object, fi, false, DeleteOptions::default())
                        .await
                } else {
                    Err(DiskError::DiskNotFound)
                }
            });
        }

        let results = join_all(futures).await;
        let mut delete_errs = Vec::with_capacity(results.len());
        for (index, result) in results.into_iter().enumerate() {
            let key = format!("ddisk-{index}");
            let already_absent = matches!(
                errs.get(index).and_then(Option::as_ref),
                Some(DiskError::FileNotFound | DiskError::FileVersionNotFound)
            );
            match result {
                Ok(_) => {
                    tags.insert(key, "<nil>".to_string());
                    delete_errs.push(None);
                }
                Err(e) => {
                    tags.insert(key, e.to_string());
                    if already_absent || matches!(&e, DiskError::FileNotFound | DiskError::FileVersionNotFound) {
                        delete_errs.push(None);
                    } else {
                        delete_errs.push(Some(e));
                    }
                }
            }
        }

        let write_quorum = if file_info_is_valid_for_metadata(&m) {
            m.write_quorum(self.default_write_quorum())
        } else {
            self.default_write_quorum()
        };
        if let Some(err) = reduce_write_quorum_errs(&delete_errs, OBJECT_OP_IGNORED_ERRS, write_quorum) {
            return Err(err);
        }

        Ok(m)
    }

    fn reduce_delete_prefix_results(results: Vec<disk::error::Result<()>>, write_quorum: usize) -> disk::error::Result<()> {
        let has_existing_volume = results
            .iter()
            .any(|result| matches!(result, Ok(()) | Err(DiskError::FileNotFound)));
        let volume_not_found_count = results
            .iter()
            .filter(|result| matches!(result, Err(DiskError::VolumeNotFound)))
            .count();
        let errs = results
            .into_iter()
            .map(|result| result.err().filter(|err| !DiskError::is_err_object_not_found(err)))
            .collect::<Vec<_>>();

        if let Some(err) = reduce_write_quorum_errs(&errs, OBJECT_OP_IGNORED_ERRS, write_quorum) {
            return Err(err);
        }
        if !has_existing_volume && volume_not_found_count >= write_quorum {
            return Err(DiskError::VolumeNotFound);
        }

        Ok(())
    }

    pub(in crate::set_disk) async fn delete_prefix(&self, bucket: &str, prefix: &str) -> disk::error::Result<()> {
        let disks = self.get_disks_internal().await;
        let write_quorum = disks.len() / 2 + 1;

        let mut futures = Vec::with_capacity(disks.len());

        for disk_op in disks.iter() {
            let bucket = bucket.to_string();
            let prefix = prefix.to_string();
            futures.push(async move {
                if let Some(disk) = disk_op {
                    disk.delete(
                        &bucket,
                        &prefix,
                        DeleteOptions {
                            recursive: true,
                            immediate: true,
                            ..Default::default()
                        },
                    )
                    .await
                } else {
                    Err(DiskError::DiskNotFound)
                }
            });
        }

        Self::reduce_delete_prefix_results(join_all(futures).await, write_quorum)
    }

    /// Scan a single disk's copy of `prefix` and decide whether it is an orphan
    /// (metadata-less) directory subtree. Walks the tree iteratively and returns
    /// [`OrphanDirScan::HasData`] as soon as any regular file is found.
    async fn scan_orphan_dir(disk: &DiskStore, bucket: &str, prefix: &str) -> OrphanDirScan {
        let root = prefix.trim_end_matches(SLASH_SEPARATOR).to_string();
        let mut stack = vec![root.clone()];
        // Pre-order list of directories (a parent always precedes its descendants),
        // so reversing it yields a safe children-first removal order.
        let mut dirs: Vec<String> = Vec::new();
        let mut existed = false;

        while let Some(dir) = stack.pop() {
            let entries = match disk.list_dir("", bucket, &dir, 0).await {
                Ok(entries) => entries,
                Err(_) => {
                    // The root missing (or never existing) means there is nothing to
                    // purge on this disk. A nested directory vanishing mid-scan is a
                    // benign race, so skip it and keep walking.
                    if dir == root {
                        return OrphanDirScan::Missing;
                    }
                    continue;
                }
            };

            existed = true;
            dirs.push(dir.clone());

            for entry in entries {
                match entry.strip_suffix(SLASH_SEPARATOR) {
                    // `read_dir` marks directories with a trailing slash; anything else
                    // is a regular file, which means real object data lives here.
                    Some(child) => stack.push(format!("{dir}{SLASH_SEPARATOR}{child}")),
                    None => return OrphanDirScan::HasData,
                }
            }
        }

        if existed {
            OrphanDirScan::Empty(dirs)
        } else {
            OrphanDirScan::Missing
        }
    }

    /// Purge an orphan directory prefix — a trailing-slash key that exists on disk
    /// as an empty directory tree with no object metadata on any disk of this set.
    /// Such prefixes are listable (see `scan_dir`) yet are not real objects, so the
    /// normal delete path returns NotFound and leaves them stranded (issue #4189).
    ///
    /// Callers pass the *decoded* directory name (`prefix/`), not the `__XLDIR__`
    /// encoded object key — the orphan tree on disk uses the plain path.
    ///
    /// Returns `Ok(true)` when the prefix was an orphan tree on this set and was
    /// removed, `Ok(false)` when it holds real data or does not exist on any disk
    /// of this set (the caller should surface the original NotFound), and `Err` on
    /// a hard disk failure.
    pub(crate) async fn purge_orphan_dir_object(&self, bucket: &str, object: &str) -> disk::error::Result<bool> {
        let disks = self.get_disks_internal().await;

        // Phase 1: classify every online disk. Refuse to purge if ANY disk holds
        // object data under the prefix, so a degraded/healable object is never
        // destroyed.
        let mut per_disk_dirs: Vec<(usize, Vec<String>)> = Vec::new();
        let mut existed = false;
        for (i, disk) in disks.iter().enumerate() {
            let Some(disk) = disk else { continue };
            match Self::scan_orphan_dir(disk, bucket, object).await {
                OrphanDirScan::HasData => return Ok(false),
                OrphanDirScan::Empty(dirs) => {
                    existed = true;
                    per_disk_dirs.push((i, dirs));
                }
                OrphanDirScan::Missing => {}
            }
        }

        if !existed {
            return Ok(false);
        }

        // Phase 2: remove the empty directories children-first on each disk. A
        // non-recursive delete performs an empty-only `rmdir`, so a directory that
        // concurrently gained an object fails with DirectoryNotEmpty and is skipped —
        // a racing PutObject is never clobbered.
        for (i, mut dirs) in per_disk_dirs {
            let Some(disk) = disks[i].as_ref() else { continue };
            dirs.reverse();
            for dir in dirs {
                if let Err(err) = disk
                    .delete(
                        bucket,
                        &dir,
                        DeleteOptions {
                            recursive: false,
                            immediate: true,
                            ..Default::default()
                        },
                    )
                    .await
                {
                    // Best effort: a sibling removal may have already cleared a shared
                    // parent, or a concurrent writer repopulated the directory. Neither
                    // is fatal to purging the orphan tree.
                    debug!(bucket, object, dir, error = ?err, "purge_orphan_dir_object: skipped non-empty/absent directory");
                }
            }
        }

        Ok(true)
    }

    /// Reclaim orphaned physical data directories under `bucket/object` that no
    /// live version in the object's `xl.meta` references any longer.
    ///
    /// Before #3510, an unversioned overwrite leaked one UUID-named data dir per
    /// PUT. The write path now cleans up going forward, but pre-existing strays
    /// stay on disk forever: `heal`'s dangling logic only removes *whole* objects
    /// whose data is missing, never surplus data dirs of an otherwise-healthy
    /// object. This closes that gap so the scanner/heal sweep can recover the
    /// leaked space automatically (issues #3231, #3191).
    ///
    /// Safety — fail closed:
    /// * The set of referenced data dirs is the UNION of `get_data_dirs()` across
    ///   every online disk's `xl.meta`, so a dir named by *any* replica is kept.
    /// * If a disk holds the object directory but its `xl.meta` is missing or
    ///   unparsable, the object is treated as degraded and unmarked data dirs are
    ///   never removed. A data dir carrying a committed delete-transaction marker
    ///   remains reclaimable after a downgrade/re-upgrade cleanup interruption.
    /// * Only subdirectories whose names parse as a UUID are ever considered;
    ///   removal is non-recursive-safe via a recursive delete of the full stray
    ///   data-dir path only.
    ///
    /// Returns the number of stray data directories removed across the set. This
    /// is best-effort maintenance: individual delete failures are logged and
    /// skipped rather than propagated.
    pub(crate) async fn reclaim_orphan_data_dirs(&self, bucket: &str, object: &str) -> disk::error::Result<usize> {
        self.reclaim_orphan_data_dirs_inner(bucket, object, false).await
    }

    pub(crate) async fn dry_run_reclaim_orphan_data_dirs(&self, bucket: &str, object: &str) -> disk::error::Result<usize> {
        self.reclaim_orphan_data_dirs_inner(bucket, object, true).await
    }

    async fn reclaim_orphan_data_dirs_inner(&self, bucket: &str, object: &str, dry_run: bool) -> disk::error::Result<usize> {
        let disks = self.get_disks_internal().await;

        // Phase 1 (read-only): build the referenced-data-dir union and record the
        // physical UUID subdirectories present on each disk. Abort on any degraded
        // copy so a healable object is never stripped of a referenced data dir.
        let mut referenced: HashSet<Uuid> = HashSet::new();
        let mut per_disk_dirs: Vec<(usize, Vec<(Uuid, bool)>)> = Vec::new();
        let mut healthy_metas = 0usize;

        for (i, disk) in disks.iter().enumerate() {
            let Some(disk) = disk else { continue };

            let entries = match disk.list_dir("", bucket, object, 0).await {
                Ok(entries) => entries,
                // No object directory on this disk: nothing to reclaim here.
                Err(DiskError::FileNotFound | DiskError::VolumeNotFound) => continue,
                Err(err) => return Err(err),
            };

            // Collect the UUID-named subdirectories: these are the physical data
            // dirs. Non-directory entries (xl.meta) and non-UUID names are ignored.
            let mut physical = Vec::new();
            for entry in &entries {
                let Some(name) = entry.strip_suffix(SLASH_SEPARATOR) else { continue };
                if let Ok(uuid) = Uuid::parse_str(name)
                    && !uuid.is_nil()
                {
                    physical.push(uuid);
                }
            }

            // Read and parse this replica's metadata. A directory that carries data
            // dirs but no readable xl.meta is degraded — fail closed.
            let meta_path = path_join_buf(&[object, STORAGE_FORMAT_FILE]);
            let buf = match disk.read_metadata(bucket, &meta_path).await {
                Ok(buf) => buf,
                Err(DiskError::FileNotFound | DiskError::FileVersionNotFound) => {
                    if physical.is_empty() {
                        // Bare directory with no data dirs and no metadata: leave it
                        // to the orphan-dir / dangling-object heal paths.
                        continue;
                    }
                    let mut committed = Vec::with_capacity(physical.len());
                    for dir in physical {
                        let data_dir = format!("{object}/{dir}");
                        let committed_delete = disk.list_dir("", bucket, &data_dir, 0).await.is_ok_and(|entries| {
                            entries.iter().any(|entry| {
                                entry
                                    .strip_prefix(DELETE_DATA_DIR_MARKER_PREFIX)
                                    .is_some_and(|transaction| Uuid::parse_str(transaction).is_ok())
                            })
                        });
                        committed.push((dir, committed_delete));
                    }
                    if committed.iter().all(|(_, committed_delete)| *committed_delete) {
                        per_disk_dirs.push((i, committed));
                        continue;
                    }
                    warn!(
                        target: "rustfs_ecstore::set_disk",
                        bucket, object,
                        "reclaim_orphan_data_dirs: aborting, data dirs present without xl.meta on a disk"
                    );
                    return Ok(0);
                }
                Err(err) => return Err(err),
            };

            let meta = match FileMeta::load(&buf) {
                Ok(meta) => meta,
                Err(err) => {
                    warn!(
                        target: "rustfs_ecstore::set_disk",
                        bucket, object, error = %err,
                        "reclaim_orphan_data_dirs: aborting, unparsable xl.meta on a disk"
                    );
                    return Ok(0);
                }
            };

            match meta.get_data_dirs() {
                Ok(dirs) => referenced.extend(dirs.into_iter().flatten().filter(|d| !d.is_nil())),
                Err(err) => {
                    warn!(
                        target: "rustfs_ecstore::set_disk",
                        bucket, object, error = %err,
                        "reclaim_orphan_data_dirs: aborting, could not decode data dirs from xl.meta"
                    );
                    return Ok(0);
                }
            }

            healthy_metas += 1;
            if !physical.is_empty() {
                per_disk_dirs.push((i, physical.into_iter().map(|dir| (dir, false)).collect()));
            }
        }

        // Phase 2: delete every physical data dir not referenced by the union.
        let mut removed = 0usize;
        for (i, physical) in per_disk_dirs {
            let Some(disk) = disks[i].as_ref() else { continue };
            for (dir, committed_delete) in physical {
                if referenced.contains(&dir) || (healthy_metas == 0 && !committed_delete) {
                    continue;
                }
                let stray = format!("{object}/{dir}");
                if dry_run {
                    removed += 1;
                    debug!(
                        target: "rustfs_ecstore::set_disk",
                        event = "heal_abandoned_parts",
                        component = "ecstore",
                        subsystem = "heal",
                        state = "dry_run_matched",
                        result = "matched",
                        bucket, object, data_dir = %dir,
                        "Heal abandoned parts dry-run matched orphaned data directory"
                    );
                    continue;
                }
                match disk
                    .delete(
                        bucket,
                        &stray,
                        DeleteOptions {
                            recursive: true,
                            immediate: true,
                            ..Default::default()
                        },
                    )
                    .await
                {
                    Ok(()) => {
                        removed += 1;
                        info!(
                            target: "rustfs_ecstore::set_disk",
                            bucket, object, data_dir = %dir,
                            "reclaim_orphan_data_dirs: removed orphaned data directory"
                        );
                    }
                    Err(DiskError::FileNotFound | DiskError::VolumeNotFound) => {}
                    Err(err) => {
                        warn!(
                            target: "rustfs_ecstore::set_disk",
                            bucket, object, data_dir = %dir, error = %err,
                            "reclaim_orphan_data_dirs: failed to remove orphaned data directory"
                        );
                    }
                }
            }
        }

        Ok(removed)
    }

    fn write_precondition_lookup_error(
        error: StorageError,
        http_preconditions: &HTTPPreconditions,
        bucket: &str,
        object: &str,
    ) -> Option<StorageError> {
        match error {
            StorageError::VersionNotFound(_, _, _) | StorageError::ObjectNotFound(_, _) => {
                if http_preconditions.if_match_value().is_some() {
                    Some(StorageError::ObjectNotFound(bucket.to_string(), object.to_string()))
                } else {
                    None
                }
            }
            error => Some(error),
        }
    }

    pub(in crate::set_disk) async fn check_write_precondition(
        &self,
        bucket: &str,
        object: &str,
        opts: &ObjectOptions,
    ) -> Option<StorageError> {
        let mut lookup_opts = opts.clone();

        let http_preconditions = lookup_opts.http_preconditions?;
        lookup_opts.http_preconditions = None;

        // Never claim a lock here, to avoid deadlock
        // - If no_lock is false, we must have obtained the lock out side of this function
        // - If no_lock is true, we should not obtain locks
        lookup_opts.no_lock = true;
        let oi = self.get_object_info(bucket, object, &lookup_opts).await;

        match oi {
            Ok(oi) => {
                // Ordinary writes may proceed past a top-level delete marker;
                // data movement must not replace an acknowledged deletion.
                if oi.delete_marker {
                    return opts.data_movement.then_some(StorageError::PreconditionFailed);
                }
                let if_none_match = http_preconditions.if_none_match_value().map(str::to_owned);
                let if_match = http_preconditions.if_match_value().map(str::to_owned);
                if should_prevent_write(&oi, if_none_match, if_match)
                    && !crate::data_movement::can_replace_stale_data_movement_target(&oi, opts)
                {
                    return Some(StorageError::PreconditionFailed);
                }
            }

            Err(error) => {
                return Self::write_precondition_lookup_error(error, &http_preconditions, bucket, object);
            }
        }

        None
    }
}

/// Structured receipt for post-commit old-data-dir reclamation (backlog#898).
///
/// Every field is a *signal*; none of them can negate an already-ACKed write.
/// `commit_rename_data_dir` returns this by value (never a `Result`), so the two
/// call sites cannot `?`-propagate a 503 out of a successful commit.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(in crate::set_disk) struct OldDataDirCleanup {
    /// Number of disks a delete was actually issued to (`disks[i].is_some()`).
    pub attempted: usize,
    /// Number of attempted disks that returned `Ok` or a not-found variant
    /// (a missing dir == already reclaimed).
    pub reclaimed: usize,
    /// Number of attempted disks that retained the directory for an active
    /// snapshot lease and registered it for deletion after the final release.
    pub deferred: usize,
    /// Indices of attempted disks that failed with a non-ignored, non-not-found
    /// error (including task panic/cancel). This is the residue that actually
    /// leaks and drives the leak metric + heal enqueue.
    pub unreclaimed_disks: Vec<usize>,
    /// Parity-lens signal only: `reduce_write_quorum_errs` over the raw per-disk
    /// errors. It can be `true` while `unreclaimed_disks` is empty (e.g. many
    /// offline/`None` slots, which are ignored). Used for logging/metrics only —
    /// it never participates in any return decision.
    pub below_quorum: bool,
}

impl OldDataDirCleanup {
    /// Whether any attempted disk left a real, leaked residue.
    pub fn has_residue(&self) -> bool {
        !self.unreclaimed_disks.is_empty()
    }
}

/// not-found normalized to success (parity with MinIO `commitRenameDataDir`).
fn is_cleanup_not_found(e: &DiskError) -> bool {
    matches!(e, DiskError::FileNotFound | DiskError::VolumeNotFound | DiskError::PathNotFound)
}

/// Map a tokio join result into a reducible per-disk error.
///
/// A task panic/cancel is mapped to a **non-ignored** `DiskError::other`, never
/// normalized to `DiskNotFound`: a panic is not a "disk absent" condition and
/// must not be silently swallowed as an ignorable error (fixes the historical
/// `Unexpected`/`DiskNotFound` misclassification).
#[allow(dead_code, reason = "asserted by this file's tests (backlog#1823)")]
fn map_cleanup_join_result(joined: std::result::Result<Option<DiskError>, tokio::task::JoinError>) -> Option<DiskError> {
    match joined {
        Ok(res) => res,
        Err(join_err) => Some(DiskError::other(format!("old data dir cleanup task failed: {join_err}"))),
    }
}

/// Pure classification: reduce per-disk cleanup results into a receipt.
///
/// `errs[i] == None` means success; `attempted[i]` marks that `disks[i]` was a
/// real target (`Some`). Untargeted `None` slots still contribute to the
/// `below_quorum` parity lens (via their ignored placeholder error) but are
/// excluded from attempted/reclaimed/residue.
fn classify_old_data_dir_cleanup(errs: &[Option<DiskError>], attempted: &[bool], write_quorum: usize) -> OldDataDirCleanup {
    debug_assert_eq!(errs.len(), attempted.len());
    let below_quorum = reduce_write_quorum_errs(errs, OBJECT_OP_IGNORED_ERRS, write_quorum).is_some();
    let mut reclaimed = 0usize;
    let mut attempted_count = 0usize;
    let mut unreclaimed_disks = Vec::new();
    for (i, err) in errs.iter().enumerate() {
        if !attempted.get(i).copied().unwrap_or(false) {
            continue;
        }
        attempted_count += 1;
        match err {
            None => reclaimed += 1,
            Some(e) if is_cleanup_not_found(e) => reclaimed += 1,
            Some(_) => unreclaimed_disks.push(i),
        }
    }
    OldDataDirCleanup {
        attempted: attempted_count,
        reclaimed,
        deferred: 0,
        unreclaimed_disks,
        below_quorum,
    }
}

/// Decision derived purely from a cleanup receipt, kept separate from `&self`
/// so the policy is unit-testable (backlog#898 §6).
#[derive(Debug, Default, PartialEq, Eq)]
pub(in crate::set_disk) struct CleanupActions {
    pub warn: bool,
    pub emit_leak_metric: bool,
    pub enqueue_heal: bool,
}

/// Decide what to do about a cleanup receipt. Residue is the only trigger for
/// the leak metric and the heal enqueue; `below_quorum` only widens the warn.
fn old_data_dir_cleanup_actions(c: &OldDataDirCleanup) -> CleanupActions {
    CleanupActions {
        warn: c.has_residue() || c.below_quorum,
        emit_leak_metric: c.has_residue(),
        enqueue_heal: c.has_residue(),
    }
}

/// Test-only delete fault-injection seam for the old-data-dir cleanup path
/// (backlog#898 §5).
///
/// This entire module is `#[cfg(test)]`, so it never compiles into production.
/// Because the cleanup deletes run in spawned tasks (potentially on different
/// worker threads), the registry is a process-global keyed by object name; a
/// test scopes its injection to a unique object name via [`fail_cleanup_on`],
/// and the returned guard clears the registry on drop.
#[cfg(test)]
pub(in crate::set_disk) mod cleanup_fault_injection {
    use super::DiskError;
    use std::sync::{Mutex, OnceLock};

    #[derive(Default)]
    struct FaultState {
        object: Option<String>,
        fail_indices: Vec<usize>,
    }

    fn state() -> &'static Mutex<FaultState> {
        static STATE: OnceLock<Mutex<FaultState>> = OnceLock::new();
        STATE.get_or_init(|| Mutex::new(FaultState::default()))
    }

    /// Guard that clears the fault registry on drop.
    pub struct FaultGuard;

    impl Drop for FaultGuard {
        fn drop(&mut self) {
            if let Ok(mut s) = state().lock() {
                *s = FaultState::default();
            }
        }
    }

    /// Force old-data-dir cleanup deletes for `object` on the given disk indices
    /// to fail with a transient error, until the returned guard is dropped.
    #[must_use]
    pub fn fail_cleanup_on(object: &str, indices: &[usize]) -> FaultGuard {
        let mut s = state().lock().expect("cleanup fault registry poisoned");
        s.object = Some(object.to_string());
        s.fail_indices = indices.to_vec();
        FaultGuard
    }

    pub(super) fn injected_error(object: &str, disk_index: usize) -> Option<DiskError> {
        let s = state().lock().expect("cleanup fault registry poisoned");
        match &s.object {
            Some(target) if target == object && s.fail_indices.contains(&disk_index) => {
                Some(DiskError::other("injected old-data-dir cleanup failure (test-only)"))
            }
            _ => None,
        }
    }
}

/// Test-only rename fault-injection seam for the set-disk rename fan-out.
///
/// This entire module is `#[cfg(test)]`, so it never compiles into production.
/// The registry is keyed by object name because the disk mutations happen in
/// spawned fan-out tasks; each test uses a unique object and clears the
/// registry through the returned guard.
#[cfg(test)]
pub(in crate::set_disk) mod rename_fault_injection {
    use super::DiskError;
    use std::sync::{Mutex, OnceLock};

    #[derive(Default)]
    struct FaultState {
        object: Option<String>,
        fail_indices: Vec<usize>,
    }

    fn state() -> &'static Mutex<FaultState> {
        static STATE: OnceLock<Mutex<FaultState>> = OnceLock::new();
        STATE.get_or_init(|| Mutex::new(FaultState::default()))
    }

    /// Guard that clears the fault registry on drop.
    pub struct FaultGuard;

    impl Drop for FaultGuard {
        fn drop(&mut self) {
            if let Ok(mut s) = state().lock() {
                *s = FaultState::default();
            }
        }
    }

    /// Force rename-data mutations for `object` on the given disk indices to
    /// fail with a transient error, until the returned guard is dropped.
    #[must_use]
    pub fn fail_rename_on(object: &str, indices: &[usize]) -> FaultGuard {
        let mut s = state().lock().expect("rename fault registry poisoned");
        s.object = Some(object.to_string());
        s.fail_indices = indices.to_vec();
        FaultGuard
    }

    pub(super) fn injected_error(object: &str, disk_index: usize) -> Option<DiskError> {
        let s = state().lock().expect("rename fault registry poisoned");
        match &s.object {
            Some(target) if target == object && s.fail_indices.contains(&disk_index) => {
                Some(DiskError::other("injected rename failure (test-only)"))
            }
            _ => None,
        }
    }
}

/// Test-only per-disk call counters for the metadata fan-out (backlog#1325,
/// serving the RPC-count assertions of #1309 / #1314 / #1315).
///
/// The metadata fan-out issues each per-disk `read_version` inside a
/// `tokio::spawn` task, so the increments happen on arbitrary runtime worker
/// threads. A thread-local `metrics::Recorder` (see `test_metrics.rs`) cannot
/// observe those spawned increments; this registry is a process-global keyed by
/// object name, so counts recorded inside spawned tasks are visible from the
/// test thread that installed the observing scope.
///
/// The whole module is `#[cfg(test)]`, so it never compiles into production.
/// Parallel tests stay isolated by observing distinct object names — an
/// unobserved object records nothing, keeping the registry bounded, and each
/// [`CallCounterScope`] clears only its own object's counts on drop.
#[cfg(test)]
pub(crate) mod disk_call_counters {
    use std::collections::{HashMap, HashSet};
    use std::sync::{Mutex, OnceLock};

    /// Kind label for the per-disk `read_version` metadata RPC.
    pub const KIND_READ_VERSION: &str = "read_version";
    pub const KIND_BATCH_READ_VERSION: &str = "batch_read_version";

    /// Registry key: (object, kind, disk_index).
    type CountKey = (String, String, usize);

    #[derive(Default)]
    struct Registry {
        /// Object names with an active observing scope. Only these accumulate,
        /// so unrelated concurrent tests never inflate one another's counts.
        observed: HashSet<String>,
        /// (object, kind, disk_index) -> call count.
        counts: HashMap<CountKey, u64>,
    }

    fn registry() -> &'static Mutex<Registry> {
        static REG: OnceLock<Mutex<Registry>> = OnceLock::new();
        REG.get_or_init(|| Mutex::new(Registry::default()))
    }

    /// Record one call of `kind` against `object`'s `disk_index`. A no-op unless
    /// an observing scope for `object` is currently installed.
    pub(super) fn record(object: &str, kind: &str, disk_index: usize) {
        let mut reg = registry().lock().expect("disk call-counter registry poisoned");
        if !reg.observed.contains(object) {
            return;
        }
        *reg.counts
            .entry((object.to_string(), kind.to_string(), disk_index))
            .or_insert(0) += 1;
    }

    /// RAII scope that observes call counts for a single `object`. Counts recorded
    /// while the scope is alive — including those recorded inside spawned tasks —
    /// are queryable; the scope clears its own counts on drop.
    #[must_use]
    pub struct CallCounterScope {
        object: String,
    }

    /// Begin observing per-disk call counts for `object`.
    pub fn observe(object: &str) -> CallCounterScope {
        let mut reg = registry().lock().expect("disk call-counter registry poisoned");
        reg.observed.insert(object.to_string());
        CallCounterScope {
            object: object.to_string(),
        }
    }

    impl CallCounterScope {
        /// Total number of `kind` calls across all disks for the observed object.
        pub fn total(&self, kind: &str) -> u64 {
            let reg = registry().lock().expect("disk call-counter registry poisoned");
            reg.counts
                .iter()
                .filter(|((obj, k, _), _)| obj == &self.object && k == kind)
                .map(|(_, count)| *count)
                .sum()
        }

        /// Number of `kind` calls recorded against a specific `disk_index`.
        pub fn for_disk(&self, kind: &str, disk_index: usize) -> u64 {
            let reg = registry().lock().expect("disk call-counter registry poisoned");
            reg.counts
                .get(&(self.object.clone(), kind.to_string(), disk_index))
                .copied()
                .unwrap_or(0)
        }
    }

    impl Drop for CallCounterScope {
        fn drop(&mut self) {
            let mut reg = registry().lock().expect("disk call-counter registry poisoned");
            reg.observed.remove(&self.object);
            reg.counts.retain(|(obj, _, _), _| obj != &self.object);
        }
    }
}

/// Fan-out phase labels for the rename/commit barrier seam (backlog#1325).
///
/// These are referenced from the production fan-out call sites (as inert string
/// literals passed to a no-op seam), so — unlike the `#[cfg(test)]` barrier
/// registry itself — they are defined unconditionally. In production builds the
/// seam ignores them entirely.
pub(in crate::set_disk) mod rename_fanout_barrier_phase {
    /// The per-disk `rename_data` phase of the write-commit fan-out.
    pub const RENAME: &str = "rename";
    /// The per-disk old-data-dir cleanup phase of the commit fan-out.
    pub const CLEANUP: &str = "cleanup";
    /// The per-disk `read_version` phase of metadata read fan-out.
    #[allow(dead_code, reason = "asserted by this file's tests (backlog#1823)")]
    pub const READ_VERSION: &str = "read_version";
}

/// Test-only awaitable pause barrier + background-task introspection for the
/// rename/commit fan-out (backlog#1325, the second facility block after the
/// per-disk call counters). Serves the barrier-style white-box acceptances of
/// #1312 (commit fencing: "abort at the first-disk rename barrier, assert the
/// coordinator still holds the lock, assert no background disk write remains
/// after release"), #1319, and #1313.
///
/// Two independent, object-keyed mechanisms share one process-global registry:
///
/// 1. **Awaitable pause barrier.** A test [`arm`]s a barrier for `(object,
///    disk_index, phase)`. The matching spawned fan-out task blocks at its
///    [`checkpoint`] until the test releases it. The test awaits the pause via
///    [`BarrierHandle::wait_until_paused`] (a deterministic `Notify` handshake —
///    no sleeps) and resumes it via [`BarrierHandle::release`]. At most one
///    barrier is armed per object at a time, matching the single-scope style of
///    `disk_call_counters`.
///
/// 2. **Background-task introspection.** A test [`observe_tasks`] for `object`;
///    each instrumented fan-out task then holds a [`TaskGuard`] for its whole
///    body, so [`TaskTrackerScope::running`] reports how many rename/cleanup
///    background disk tasks are still in flight. This is the concrete "is there
///    still a background disk write?" signal #1312 asserts after a lock release.
///
/// Both are keyed by object and only accumulate for armed/observed objects, so
/// concurrent tests using distinct object names stay fully isolated. The whole
/// module is `#[cfg(test)]` and never compiles into production; the fan-out call
/// sites reach it only through the `#[cfg(not(test))]` no-op seams on `SetDisks`.
///
/// Scope of this block (white-box, in-process only): coordinator lock-holding is
/// asserted by the test at the store/coordinator layer via the guard it already
/// holds — `io_primitives` has no handle to that namespace lock, and fabricating
/// a lock-state registry here would be a look-alike rather than the real lock.
/// Cross-process/black-box fault injection (toxiproxy, blackhole peers, 2-pool)
/// is a later cluster-harness block, not this one.
#[cfg(test)]
pub(in crate::set_disk) mod rename_fanout_barrier {
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex, OnceLock};
    use tokio::sync::Notify;

    pub use super::rename_fanout_barrier_phase::{
        CLEANUP as PHASE_CLEANUP, READ_VERSION as PHASE_READ_VERSION, RENAME as PHASE_RENAME,
    };

    /// One armed barrier: the fan-out task matching `(disk_index, phase)` pauses.
    struct Armed {
        disk_index: usize,
        phase: &'static str,
        /// Signalled (task -> test) when the target task reaches the checkpoint.
        arrived: Arc<Notify>,
        /// Signalled (test -> task) to release the paused task.
        release: Arc<Notify>,
        /// Set once the target task is parked at the checkpoint.
        paused: Arc<AtomicBool>,
    }

    #[derive(Default)]
    struct Registry {
        /// object -> armed barrier (at most one per object).
        armed: HashMap<String, Armed>,
        /// object -> live in-flight fan-out task count, only for observed objects.
        observed: HashMap<String, Arc<AtomicUsize>>,
    }

    fn registry() -> &'static Mutex<Registry> {
        static REG: OnceLock<Mutex<Registry>> = OnceLock::new();
        REG.get_or_init(|| Mutex::new(Registry::default()))
    }

    fn lock() -> std::sync::MutexGuard<'static, Registry> {
        registry().lock().expect("rename fan-out barrier registry poisoned")
    }

    /// RAII handle for one armed barrier. Dropping it disarms the barrier and
    /// releases any still-parked task, so a panicking or forgetful test can never
    /// leave a spawned fan-out task wedged.
    #[must_use]
    pub struct BarrierHandle {
        object: String,
        arrived: Arc<Notify>,
        release: Arc<Notify>,
        paused: Arc<AtomicBool>,
    }

    /// Arm a barrier: the fan-out task for `object` at `(disk_index, phase)` will
    /// pause at its checkpoint until the returned handle is released or dropped.
    pub fn arm(object: &str, disk_index: usize, phase: &'static str) -> BarrierHandle {
        let arrived = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let paused = Arc::new(AtomicBool::new(false));
        lock().armed.insert(
            object.to_string(),
            Armed {
                disk_index,
                phase,
                arrived: arrived.clone(),
                release: release.clone(),
                paused: paused.clone(),
            },
        );
        BarrierHandle {
            object: object.to_string(),
            arrived,
            release,
            paused,
        }
    }

    impl BarrierHandle {
        /// Await until the armed fan-out task has parked at the checkpoint. Uses a
        /// stored-permit `Notify`, so this is race-free regardless of whether the
        /// task reaches the checkpoint before or after this call — no sleeps.
        pub async fn wait_until_paused(&self) {
            if self.paused.load(Ordering::SeqCst) {
                return;
            }
            self.arrived.notified().await;
        }

        /// Whether the target task is currently parked at the checkpoint.
        pub fn is_paused(&self) -> bool {
            self.paused.load(Ordering::SeqCst)
        }

        /// Release the parked task so the fan-out can proceed.
        pub fn release(&self) {
            self.release.notify_one();
        }
    }

    impl Drop for BarrierHandle {
        fn drop(&mut self) {
            // Unblock any task still parked at the checkpoint before disarming, so
            // a dropped handle can never wedge a spawned fan-out task.
            self.release.notify_one();
            lock().armed.remove(&self.object);
        }
    }

    /// Seam entry point invoked from inside each spawned fan-out task. A no-op
    /// unless a barrier is armed for exactly this `(object, disk_index, phase)`.
    /// The registry mutex is released before awaiting, so it is never held across
    /// the pause.
    pub(in crate::set_disk) async fn checkpoint(object: &str, disk_index: usize, phase: &'static str) {
        let hooks = {
            let reg = lock();
            match reg.armed.get(object) {
                Some(a) if a.disk_index == disk_index && a.phase == phase => {
                    Some((a.arrived.clone(), a.release.clone(), a.paused.clone()))
                }
                _ => None,
            }
        };
        if let Some((arrived, release, paused)) = hooks {
            paused.store(true, Ordering::SeqCst);
            arrived.notify_one();
            release.notified().await;
        }
    }

    /// RAII scope that observes in-flight fan-out task counts for a single
    /// `object`. Counts accrue only while the scope is alive; it clears its own
    /// entry on drop.
    #[must_use]
    pub struct TaskTrackerScope {
        object: String,
    }

    /// Begin observing in-flight rename/cleanup fan-out task counts for `object`.
    pub fn observe_tasks(object: &str) -> TaskTrackerScope {
        lock()
            .observed
            .entry(object.to_string())
            .or_insert_with(|| Arc::new(AtomicUsize::new(0)));
        TaskTrackerScope {
            object: object.to_string(),
        }
    }

    impl TaskTrackerScope {
        /// Number of instrumented fan-out tasks for the observed object that are
        /// currently in flight (task body entered, guard not yet dropped).
        pub fn running(&self) -> usize {
            lock()
                .observed
                .get(&self.object)
                .map(|c| c.load(Ordering::SeqCst))
                .unwrap_or(0)
        }
    }

    impl Drop for TaskTrackerScope {
        fn drop(&mut self) {
            lock().observed.remove(&self.object);
        }
    }

    /// RAII guard held for the lifetime of one spawned fan-out task. Increments
    /// the observed counter on creation (if the object is observed) and decrements
    /// it on drop. Holding the `Arc` keeps the decrement sound even if the scope
    /// is dropped while a task is still draining.
    #[must_use]
    pub struct TaskGuard {
        counter: Option<Arc<AtomicUsize>>,
    }

    /// Create a task guard for `object`. A no-op guard unless `object` is observed.
    pub(in crate::set_disk) fn task_guard(object: &str) -> TaskGuard {
        let counter = lock().observed.get(object).cloned();
        if let Some(c) = &counter {
            c.fetch_add(1, Ordering::SeqCst);
        }
        TaskGuard { counter }
    }

    impl Drop for TaskGuard {
        fn drop(&mut self) {
            if let Some(c) = &self.counter {
                c.fetch_sub(1, Ordering::SeqCst);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::disk::local::{DurabilityMode, durability_mode_override};

    use super::*;
    use std::io::Cursor;
    use tempfile::TempDir;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[test]
    fn write_precondition_lookup_errors_fail_closed_unless_absence_is_known() {
        let create_only = HTTPPreconditions {
            if_none_match: Some("*".to_string()),
            ..Default::default()
        };
        let replace_only = HTTPPreconditions {
            if_match: Some("etag".to_string()),
            ..Default::default()
        };

        assert!(matches!(
            SetDisks::write_precondition_lookup_error(StorageError::ErasureReadQuorum, &create_only, "bucket", "object",),
            Some(StorageError::ErasureReadQuorum)
        ));
        assert!(
            SetDisks::write_precondition_lookup_error(
                StorageError::ObjectNotFound("bucket".to_string(), "object".to_string()),
                &create_only,
                "bucket",
                "object",
            )
            .is_none()
        );
        assert!(matches!(
            SetDisks::write_precondition_lookup_error(
                StorageError::ObjectNotFound("bucket".to_string(), "object".to_string()),
                &replace_only,
                "bucket",
                "object",
            ),
            Some(StorageError::ObjectNotFound(_, _))
        ));
    }

    fn metadata_test_fileinfo(object: &str) -> FileInfo {
        let mut fi = FileInfo::new(object, 2, 2);
        fi.volume = "bucket".to_string();
        fi.name = object.to_string();
        fi.size = 1;
        fi.erasure.index = 1;
        fi.metadata.insert("etag".to_string(), "etag-1".to_string());
        fi.add_object_part(1, "part-etag-1".to_string(), 1, None, 1, None, None);
        fi
    }

    fn metadata_test_delete_marker(object: &str, version_id: Uuid, mod_time: OffsetDateTime) -> FileInfo {
        FileInfo {
            volume: "bucket".to_string(),
            name: object.to_string(),
            version_id: Some(version_id),
            deleted: true,
            mod_time: Some(mod_time),
            ..Default::default()
        }
    }

    fn read_part_test_part(number: usize, etag: &str) -> ObjectPartInfo {
        ObjectPartInfo {
            number,
            etag: etag.to_string(),
            ..Default::default()
        }
    }

    fn read_part_test_error(number: usize, error: &str) -> ObjectPartInfo {
        ObjectPartInfo {
            number,
            error: Some(error.to_string()),
            ..Default::default()
        }
    }

    async fn read_multiple_test_disk(bucket: &str, objects: &[(&str, &[u8])]) -> (TempDir, DiskStore) {
        let dir = tempfile::tempdir().expect("tempdir should be created");
        let endpoint =
            Endpoint::try_from(dir.path().to_str().expect("tempdir path should be utf8")).expect("endpoint should parse");
        let disk = new_disk(
            &endpoint,
            &DiskOption {
                cleanup: false,
                health_check: false,
            },
        )
        .await
        .expect("disk should be created");

        match disk.make_volume(bucket).await {
            Ok(()) | Err(DiskError::VolumeExists) => {}
            Err(err) => panic!("bucket should be available: {err:?}"),
        }
        for (object, body) in objects {
            disk.write_all(bucket, object, Bytes::copy_from_slice(body))
                .await
                .expect("object should be written");
        }

        (dir, disk)
    }

    async fn io_primitives_test_set(disks: Vec<Option<DiskStore>>, default_parity_count: usize) -> Arc<SetDisks> {
        let set_drive_count = disks.len();
        SetDisks::new(
            "io-primitives-test".to_string(),
            Arc::new(RwLock::new(disks)),
            set_drive_count,
            default_parity_count,
            0,
            0,
            Vec::new(),
            FormatV3::new(1, 1),
            Vec::new(),
        )
        .await
    }

    async fn write_raw_file_meta_unchecked(disk: &DiskStore, bucket: &str, object: &str, metadata: FileMeta) {
        let encoded = metadata.marshal_msg().expect("raw regression metadata should serialize");
        disk.write_all(bucket, &format!("{object}/{STORAGE_FORMAT_FILE}"), Bytes::from(encoded))
            .await
            .expect("raw regression metadata should be installed");
    }

    async fn write_raw_file_info_unchecked(disk: &DiskStore, bucket: &str, object: &str, file_info: FileInfo) {
        let mut metadata = FileMeta::new();
        metadata
            .add_version(file_info)
            .expect("raw regression metadata should encode");
        write_raw_file_meta_unchecked(disk, bucket, object, metadata).await;
    }

    fn failed_read_repair_submitter(_request: rustfs_common::heal_channel::HealChannelRequest) -> ReadRepairAdmissionFuture {
        Box::pin(async { ReadRepairAdmissionOutcome::Failed("injected submit failure".to_string()) })
    }

    fn accepted_read_repair_submitter(_request: rustfs_common::heal_channel::HealChannelRequest) -> ReadRepairAdmissionFuture {
        Box::pin(async { ReadRepairAdmissionOutcome::Response(HealAdmissionResult::Accepted) })
    }

    fn dropped_read_repair_submitter(_request: rustfs_common::heal_channel::HealChannelRequest) -> ReadRepairAdmissionFuture {
        Box::pin(async {
            ReadRepairAdmissionOutcome::Response(HealAdmissionResult::Dropped(
                rustfs_common::heal_channel::HealAdmissionDropReason::PolicyDropped,
            ))
        })
    }

    fn test_object_bitrot_reader() -> ObjectBitrotReader {
        BitrotReader::new(
            ShardReader::Stream(Box::new(Cursor::new(vec![1u8, 2, 3, 4]))),
            4,
            HashAlgorithm::None,
            false,
        )
    }

    fn test_deferred_object_bitrot_reader() -> (ObjectBitrotReader, DeferredReaderStripeHandle) {
        create_deferred_bitrot_reader_with_stripe_handle(
            Some(Bytes::from_static(&[1, 2, 3, 4])),
            None,
            "bucket",
            "object/part.1",
            0,
            4,
            4,
            HashAlgorithm::None,
            false,
            false,
        )
    }

    #[test]
    fn dangling_delete_grace_defaults_to_one_hour() {
        temp_env::with_var(ENV_HEAL_DANGLING_DELETE_GRACE_SECS, None::<&str>, || {
            assert_eq!(dangling_delete_grace(), time::Duration::seconds(3600));
        });
    }

    #[test]
    fn dangling_delete_grace_env_override_and_disable() {
        temp_env::with_var(ENV_HEAL_DANGLING_DELETE_GRACE_SECS, Some("120"), || {
            assert_eq!(dangling_delete_grace(), time::Duration::seconds(120));
        });
        temp_env::with_var(ENV_HEAL_DANGLING_DELETE_GRACE_SECS, Some("0"), || {
            assert!(dangling_delete_grace().is_zero());
        });
    }

    /// Builds `count` empty local disks (each in its own tempdir) for the given
    /// bucket. Returns the tempdir guards (which must outlive the disks) and the
    /// disk slot vector expected by the metadata fan-out.
    async fn call_counter_local_disks(bucket: &str, count: usize) -> (Vec<TempDir>, Vec<Option<DiskStore>>) {
        let mut dirs = Vec::with_capacity(count);
        let mut disks = Vec::with_capacity(count);
        for _ in 0..count {
            let (dir, disk) = read_multiple_test_disk(bucket, &[]).await;
            dirs.push(dir);
            disks.push(Some(disk));
        }
        (dirs, disks)
    }

    async fn reopen_local_disk(dir: &TempDir) -> DiskStore {
        let endpoint =
            Endpoint::try_from(dir.path().to_str().expect("tempdir path should be utf8")).expect("endpoint should parse");
        new_disk(
            &endpoint,
            &DiskOption {
                cleanup: false,
                health_check: false,
            },
        )
        .await
        .expect("disk should reopen from the same tempdir")
    }

    async fn prepare_rename_source_dirs(dirs: &[TempDir], disks: &[Option<DiskStore>], source: &str) {
        for (dir, disk) in dirs.iter().zip(disks.iter()) {
            let Some(disk) = disk else {
                continue;
            };
            match disk.make_volume(RUSTFS_META_TMP_BUCKET).await {
                Ok(()) | Err(DiskError::VolumeExists) => {}
                Err(err) => panic!("temporary metadata volume should be available: {err:?}"),
            }
            std::fs::create_dir_all(dir.path().join(RUSTFS_META_TMP_BUCKET).join(source))
                .expect("rename staging source dir should be created");
        }
    }

    #[test]
    fn metadata_slowtail_fault_delay_parses_and_filters_request() {
        temp_env::with_vars(
            [
                (ENV_RUSTFS_GET_METADATA_SLOWTAIL_FAULT_DELAY_MS, Some("25")),
                (ENV_RUSTFS_GET_METADATA_SLOWTAIL_FAULT_DISKS, Some("1,3")),
                (ENV_RUSTFS_GET_METADATA_SLOWTAIL_FAULT_BUCKET, Some("bench-bucket")),
                (ENV_RUSTFS_GET_METADATA_SLOWTAIL_FAULT_OBJECT_PREFIX, Some("objects/")),
            ],
            || {
                assert_eq!(
                    get_metadata_slowtail_fault_delay("bench-bucket", "objects/000001", 3, true),
                    Some(Duration::from_millis(25))
                );
                assert!(get_metadata_slowtail_fault_delay("bench-bucket", "objects/000001", 2, true).is_none());
                assert!(get_metadata_slowtail_fault_delay("other-bucket", "objects/000001", 3, true).is_none());
                assert!(get_metadata_slowtail_fault_delay("bench-bucket", "other/000001", 3, true).is_none());
                assert!(get_metadata_slowtail_fault_delay("bench-bucket", "objects/000001", 3, false).is_none());
            },
        );
    }

    #[test]
    fn metadata_slowtail_fault_delay_disables_invalid_disk_list() {
        temp_env::with_vars(
            [
                (ENV_RUSTFS_GET_METADATA_SLOWTAIL_FAULT_DELAY_MS, Some("25")),
                (ENV_RUSTFS_GET_METADATA_SLOWTAIL_FAULT_DISKS, Some("1,nope")),
            ],
            || {
                assert!(get_metadata_slowtail_fault_delay("bucket", "object", 1, true).is_none());
            },
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn metadata_slowtail_fault_delays_only_data_read_metadata_task() {
        const DISKS: usize = 4;
        let bucket = "metadata-slowtail-fault-bucket";
        let object = "objects/metadata-slowtail-fault-object";
        let (dirs, disks) = call_counter_local_disks(bucket, DISKS).await;
        install_metadata_fanout_fileinfo(&disks, bucket, object, None).await;

        temp_env::async_with_vars(
            [
                (ENV_RUSTFS_GET_METADATA_EARLY_STOP_ENABLE, Some("false")),
                (ENV_RUSTFS_GET_METADATA_SLOWTAIL_FAULT_DELAY_MS, Some("150")),
                (ENV_RUSTFS_GET_METADATA_SLOWTAIL_FAULT_DISKS, Some("3")),
                (ENV_RUSTFS_GET_METADATA_SLOWTAIL_FAULT_BUCKET, Some(bucket)),
                (ENV_RUSTFS_GET_METADATA_SLOWTAIL_FAULT_OBJECT_PREFIX, Some("objects/")),
            ],
            async {
                let read_without_data =
                    SetDisks::read_all_fileinfo_observed(&disks, bucket, bucket, object, "", false, false, false, true, 2);
                tokio::time::timeout(Duration::from_millis(100), read_without_data)
                    .await
                    .expect("non-data metadata fanout must not be delayed by the data-read slowtail hook")
                    .expect("metadata fanout without read_data should resolve");

                let mut read_with_data = Box::pin(SetDisks::read_all_fileinfo_observed(
                    &disks, bucket, bucket, object, "", true, false, false, true, 2,
                ));
                assert!(
                    tokio::time::timeout(Duration::from_millis(40), &mut read_with_data)
                        .await
                        .is_err(),
                    "data-read metadata fanout must wait for the injected slow read_version response"
                );
                let (parts_metadata, errs, diagnostics) = tokio::time::timeout(Duration::from_secs(2), read_with_data)
                    .await
                    .expect("injected slowtail should eventually complete")
                    .expect("data-read metadata fanout should resolve");
                assert_eq!(parts_metadata.iter().filter(|fi| fi.name == object).count(), DISKS);
                assert!(errs.iter().all(Option::is_none));
                assert_eq!(diagnostics.total_responses(), DISKS);
            },
        )
        .await;

        drop(dirs);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn metadata_slowtail_fault_delays_early_stop_metadata_task() {
        const DISKS: usize = 4;
        let bucket = "metadata-slowtail-early-stop-bucket";
        let object = "objects/metadata-slowtail-early-stop-object";
        let (dirs, disks) = call_counter_local_disks(bucket, DISKS).await;
        install_metadata_fanout_fileinfo(&disks, bucket, object, None).await;

        temp_env::async_with_vars(
            [
                (ENV_RUSTFS_GET_METADATA_EARLY_STOP_ENABLE, Some("true")),
                (ENV_RUSTFS_GET_METADATA_DATA_READ_EARLY_STOP_ENABLE, Some("true")),
                (ENV_RUSTFS_GET_METADATA_EARLY_STOP_BOUNDED_FANOUT, Some("false")),
                (ENV_RUSTFS_GET_METADATA_SLOWTAIL_FAULT_DELAY_MS, Some("150")),
                (ENV_RUSTFS_GET_METADATA_SLOWTAIL_FAULT_DISKS, Some("3")),
                (ENV_RUSTFS_GET_METADATA_SLOWTAIL_FAULT_BUCKET, Some(bucket)),
                (ENV_RUSTFS_GET_METADATA_SLOWTAIL_FAULT_OBJECT_PREFIX, Some("objects/")),
            ],
            async {
                let mut read_with_data = Box::pin(SetDisks::read_all_fileinfo_observed(
                    &disks, bucket, bucket, object, "", true, false, false, true, 2,
                ));
                assert!(
                    tokio::time::timeout(Duration::from_millis(40), &mut read_with_data)
                        .await
                        .is_err(),
                    "early-stop metadata fanout must still wait for the injected slow response after fallback to full wait"
                );
                let (parts_metadata, errs, diagnostics) = tokio::time::timeout(Duration::from_secs(2), read_with_data)
                    .await
                    .expect("injected early-stop slowtail should eventually complete")
                    .expect("early-stop metadata fanout should resolve");
                assert_eq!(parts_metadata.iter().filter(|fi| fi.name == object).count(), DISKS);
                assert!(errs.iter().all(Option::is_none));
                assert_eq!(diagnostics.total_responses(), DISKS);
            },
        )
        .await;

        drop(dirs);
    }

    /// Demo / regression guard for the backlog#1325 per-disk call counters.
    ///
    /// The metadata fan-out issues each `read_version` inside its own
    /// `tokio::spawn` task; on a multi-thread runtime those tasks land on
    /// arbitrary worker threads. This asserts the process-global registry still
    /// observes every per-disk increment — the exact gap that makes the
    /// thread-local `CapturingRecorder` unusable for #1309 / #1314 counting.
    ///
    /// Reverting `record_read_version_call` to a no-op drops both the total and
    /// the per-disk counts to zero and fails this test.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn read_version_call_counter_observes_spawned_fanout() {
        const DISKS: usize = 4;
        let bucket = "counter-bucket";
        let (dirs, disks) = call_counter_local_disks(bucket, DISKS).await;

        let object = "counter-object";
        let scope = disk_call_counters::observe(object);

        // read_data=false, observe=false -> deterministic full-wait fan-out.
        let _ = SetDisks::read_all_fileinfo(&disks, bucket, bucket, object, "", false, false, false).await;

        assert_eq!(
            scope.total(disk_call_counters::KIND_READ_VERSION),
            DISKS as u64,
            "every online disk must record exactly one read_version, even from a spawned task"
        );
        for idx in 0..DISKS {
            assert_eq!(
                scope.for_disk(disk_call_counters::KIND_READ_VERSION, idx),
                1,
                "disk {idx} should record exactly one read_version"
            );
        }

        // A second observed fan-out accumulates onto the same scope.
        let _ = SetDisks::read_all_fileinfo(&disks, bucket, bucket, object, "", false, false, false).await;
        assert_eq!(scope.total(disk_call_counters::KIND_READ_VERSION), (DISKS * 2) as u64);

        drop(dirs);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn metadata_read_version_coalescer_bypasses_local_disks() {
        const DISKS: usize = 4;
        let bucket = "coalesced-read-version-local-bypass-bucket";
        let object_a = "coalesced-local-object-a";
        let object_b = "coalesced-local-object-b";
        let (dirs, disks) = call_counter_local_disks(bucket, DISKS).await;
        install_metadata_fanout_fileinfo(&disks, bucket, object_a, None).await;
        install_metadata_fanout_fileinfo(&disks, bucket, object_b, None).await;

        temp_env::async_with_vars(
            [
                (ENV_RUSTFS_GET_METADATA_READ_VERSION_COALESCE, Some("auto")),
                (ENV_RUSTFS_GET_METADATA_READ_VERSION_COALESCE_DELAY_MICROS, Some("5000")),
            ],
            async {
                let calls = disk_call_counters::observe(object_a);
                let disks_a = disks.clone();
                let disks_b = disks.clone();
                let read_a = tokio::spawn(async move {
                    SetDisks::read_all_fileinfo_observed_for_get_object(
                        &disks_a, "", bucket, object_a, "", false, false, false, 2,
                    )
                    .await
                    .map(|(file_infos, errors, _)| (file_infos, errors))
                });
                tokio::task::yield_now().await;
                let read_b = tokio::spawn(async move {
                    SetDisks::read_all_fileinfo_observed_for_get_object(
                        &disks_b, "", bucket, object_b, "", false, false, false, 2,
                    )
                    .await
                    .map(|(file_infos, errors, _)| (file_infos, errors))
                });

                let (metadata_a, errs_a) = read_a
                    .await
                    .expect("first read task should not panic")
                    .expect("first coalesced read should resolve");
                let (metadata_b, errs_b) = read_b
                    .await
                    .expect("second read task should not panic")
                    .expect("second coalesced read should resolve");

                assert_eq!(metadata_a.iter().filter(|fi| fi.name == object_a).count(), DISKS);
                assert_eq!(metadata_b.iter().filter(|fi| fi.name == object_b).count(), DISKS);
                assert!(errs_a.iter().all(Option::is_none));
                assert!(errs_b.iter().all(Option::is_none));
                assert_eq!(
                    calls.total(disk_call_counters::KIND_READ_VERSION),
                    DISKS as u64,
                    "local disks still execute the ordinary per-disk read_version path"
                );
                assert_eq!(
                    calls.total(disk_call_counters::KIND_BATCH_READ_VERSION),
                    0,
                    "GET coalescing targets internode RPC count only and must not batch local disk reads"
                );
            },
        )
        .await;

        drop(dirs);
    }

    #[tokio::test]
    async fn metadata_read_version_coalescer_requires_get_object_intent() {
        const DISKS: usize = 4;
        let bucket = "coalesced-read-version-default-bypass-bucket";
        let object = "default-bypass-object";
        let (dirs, disks) = call_counter_local_disks(bucket, DISKS).await;
        install_metadata_fanout_fileinfo(&disks, bucket, object, None).await;

        temp_env::async_with_vars([(ENV_RUSTFS_GET_METADATA_READ_VERSION_COALESCE, Some("auto"))], async {
            let calls = disk_call_counters::observe(object);
            let (metadata, errs) = SetDisks::read_all_fileinfo(&disks, "", bucket, object, "", false, false, false)
                .await
                .expect("default metadata read should resolve");

            assert_eq!(metadata.iter().filter(|fi| fi.name == object).count(), DISKS);
            assert!(errs.iter().all(Option::is_none));
            assert_eq!(calls.total(disk_call_counters::KIND_READ_VERSION), DISKS as u64);
            assert_eq!(
                calls.total(disk_call_counters::KIND_BATCH_READ_VERSION),
                0,
                "non-GET metadata paths must bypass coalescer even when the env gate is enabled"
            );
        })
        .await;

        drop(dirs);
    }

    #[test]
    fn batch_read_version_response_mapping_preserves_index_and_errors() {
        let expected_items = vec![
            BatchReadVersionItem {
                org_volume: String::new(),
                volume: "bucket".to_string(),
                path: "object-a".to_string(),
                version_id: "v-a".to_string(),
            },
            BatchReadVersionItem {
                org_volume: String::new(),
                volume: "bucket".to_string(),
                path: "object-b".to_string(),
                version_id: "v-b".to_string(),
            },
            BatchReadVersionItem {
                org_volume: String::new(),
                volume: "bucket".to_string(),
                path: "object-c".to_string(),
                version_id: "v-c".to_string(),
            },
        ];
        let ok_file_info = FileInfo {
            name: "object-a".to_string(),
            ..Default::default()
        };
        let responses = vec![
            BatchReadVersionResp {
                index: 2,
                path: "object-c".to_string(),
                version_id: "v-c".to_string(),
                success: false,
                file_info: FileInfo::default(),
                error: "disk read failed".to_string(),
                error_code: 0,
            },
            BatchReadVersionResp {
                index: 0,
                path: "object-a".to_string(),
                version_id: "v-a".to_string(),
                success: true,
                file_info: ok_file_info,
                error: String::new(),
                error_code: 0,
            },
        ];

        let expected_items = expected_batch_read_version_items(&expected_items);
        let mut results = map_batch_read_version_responses(&expected_items, responses).into_iter();
        let first = results
            .next()
            .expect("slot 0 should exist")
            .expect("slot 0 should map the success response by index");
        assert_eq!(first.name, "object-a");

        let missing = results
            .next()
            .expect("slot 1 should exist")
            .expect_err("slot 1 should stay missing");
        assert!(
            missing.to_string().contains("response missing"),
            "unexpected missing response error: {missing}"
        );

        let failed = results
            .next()
            .expect("slot 2 should exist")
            .expect_err("slot 2 should map the response error");
        assert!(failed.to_string().contains("disk read failed"), "unexpected per-item error: {failed}");
        assert!(results.next().is_none());
    }

    #[test]
    fn batch_read_version_response_mapping_preserves_typed_not_found_errors() {
        let expected_items = vec![
            BatchReadVersionItem {
                org_volume: String::new(),
                volume: "bucket".to_string(),
                path: "object-a".to_string(),
                version_id: "v-a".to_string(),
            },
            BatchReadVersionItem {
                org_volume: String::new(),
                volume: "bucket".to_string(),
                path: "object-b".to_string(),
                version_id: "v-b".to_string(),
            },
        ];
        let expected_items = expected_batch_read_version_items(&expected_items);
        let results = map_batch_read_version_responses(
            &expected_items,
            vec![
                BatchReadVersionResp {
                    index: 0,
                    path: "object-a".to_string(),
                    version_id: "v-a".to_string(),
                    success: false,
                    file_info: FileInfo::default(),
                    error: DiskError::FileNotFound.to_string(),
                    error_code: DiskError::FileNotFound.to_u32(),
                },
                BatchReadVersionResp {
                    index: 1,
                    path: "object-b".to_string(),
                    version_id: "v-b".to_string(),
                    success: false,
                    file_info: FileInfo::default(),
                    error: DiskError::FileVersionNotFound.to_string(),
                    error_code: DiskError::FileVersionNotFound.to_u32(),
                },
            ],
        );

        assert!(matches!(results.first().expect("slot 0 should exist"), Err(DiskError::FileNotFound)));
        assert!(matches!(
            results.get(1).expect("slot 1 should exist"),
            Err(DiskError::FileVersionNotFound)
        ));
    }

    #[test]
    fn batch_read_version_response_mapping_rejects_identity_mismatch_and_duplicate_index() {
        let expected_items = vec![BatchReadVersionItem {
            org_volume: String::new(),
            volume: "bucket".to_string(),
            path: "object-a".to_string(),
            version_id: "v-a".to_string(),
        }];
        let expected_items = expected_batch_read_version_items(&expected_items);
        let mismatched = map_batch_read_version_responses(
            &expected_items,
            vec![BatchReadVersionResp {
                index: 0,
                path: "object-b".to_string(),
                version_id: "v-a".to_string(),
                success: true,
                file_info: FileInfo {
                    name: "object-b".to_string(),
                    ..Default::default()
                },
                error: String::new(),
                error_code: 0,
            }],
        )
        .pop()
        .expect("slot 0 should exist")
        .expect_err("identity mismatch should fail closed");
        assert!(
            mismatched.to_string().contains("identity mismatch"),
            "unexpected mismatch error: {mismatched}"
        );

        let duplicate = map_batch_read_version_responses(
            &expected_items,
            vec![
                BatchReadVersionResp {
                    index: 0,
                    path: "object-a".to_string(),
                    version_id: "v-a".to_string(),
                    success: true,
                    file_info: FileInfo {
                        name: "object-a".to_string(),
                        ..Default::default()
                    },
                    error: String::new(),
                    error_code: 0,
                },
                BatchReadVersionResp {
                    index: 0,
                    path: "object-a".to_string(),
                    version_id: "v-a".to_string(),
                    success: true,
                    file_info: FileInfo {
                        name: "object-a".to_string(),
                        ..Default::default()
                    },
                    error: String::new(),
                    error_code: 0,
                },
            ],
        )
        .pop()
        .expect("slot 0 should exist")
        .expect_err("duplicate response index should fail closed");
        assert!(
            duplicate.to_string().contains("duplicate index"),
            "unexpected duplicate error: {duplicate}"
        );
    }

    fn expected_batch_read_version_items(items: &[BatchReadVersionItem]) -> Vec<ExpectedBatchReadVersionItem> {
        items.iter().map(ExpectedBatchReadVersionItem::from).collect()
    }

    /// Isolation guard: unobserved objects record nothing (so parallel tests do
    /// not inflate one another), and a scope clears its own counts on drop.
    #[tokio::test]
    async fn call_counter_isolates_unobserved_objects_and_clears_on_drop() {
        let bucket = "counter-bucket-iso";
        let (dirs, disks) = call_counter_local_disks(bucket, 1).await;
        let object = "iso-object";

        // No active scope: the fan-out records nothing.
        let _ = SetDisks::read_all_fileinfo(&disks, bucket, bucket, object, "", false, false, false).await;

        {
            let scope = disk_call_counters::observe(object);
            assert_eq!(
                scope.total(disk_call_counters::KIND_READ_VERSION),
                0,
                "reads before the scope existed must not be counted"
            );
            let _ = SetDisks::read_all_fileinfo(&disks, bucket, bucket, object, "", false, false, false).await;
            assert_eq!(scope.total(disk_call_counters::KIND_READ_VERSION), 1);
        }

        // Previous scope dropped -> counts cleared; a fresh scope starts at zero.
        let scope = disk_call_counters::observe(object);
        assert_eq!(
            scope.total(disk_call_counters::KIND_READ_VERSION),
            0,
            "dropping a scope must clear its counts"
        );

        drop(dirs);
    }

    fn valid_metadata_fanout_fileinfo(
        bucket: &str,
        object: &str,
        version_id: Uuid,
        data_dir: Uuid,
        mod_time: OffsetDateTime,
    ) -> FileInfo {
        let mut fi = FileInfo::new(object, 2, 2);
        fi.volume = bucket.to_string();
        fi.name = object.to_string();
        fi.size = 1;
        fi.erasure.index = 1;
        fi.version_id = Some(version_id);
        fi.is_latest = true;
        fi.data_dir = Some(data_dir);
        fi.mod_time = Some(mod_time);
        fi.metadata.insert("etag".to_string(), "etag-1".to_string());
        fi.add_object_part(1, "part-etag".to_string(), 1, fi.mod_time, 1, None, None);
        fi
    }

    async fn install_metadata_fanout_fileinfo(
        disks: &[Option<DiskStore>],
        bucket: &str,
        object: &str,
        missing_part_disk: Option<usize>,
    ) {
        let version_id = Uuid::new_v4();
        let data_dir = Uuid::new_v4();
        let mod_time = OffsetDateTime::now_utc();
        for (index, disk) in disks
            .iter()
            .enumerate()
            .filter_map(|(index, disk)| disk.as_ref().map(|disk| (index, disk)))
        {
            if missing_part_disk != Some(index) {
                disk.write_all(bucket, &format!("{object}/{data_dir}/part.1"), Bytes::from_static(b"x"))
                    .await
                    .expect("part data should be installed on every disk");
            }
            disk.write_metadata(
                bucket,
                bucket,
                object,
                valid_metadata_fanout_fileinfo(bucket, object, version_id, data_dir, mod_time),
            )
            .await
            .expect("metadata should be installed on every disk");
        }
    }

    async fn inline_metadata_fanout_fileinfos_with_mode(
        bucket: &str,
        object: &str,
        payload: &[u8],
        uses_legacy_checksum: bool,
    ) -> Vec<FileInfo> {
        inline_metadata_fanout_fileinfos_with_geometry(bucket, object, payload, uses_legacy_checksum, 2, 2).await
    }

    async fn inline_metadata_fanout_fileinfos_with_geometry(
        bucket: &str,
        object: &str,
        payload: &[u8],
        uses_legacy_checksum: bool,
        data_shards: usize,
        parity_shards: usize,
    ) -> Vec<FileInfo> {
        let distribution_key = metadata_distribution_key(bucket, object);
        let mut base = FileInfo::new(&distribution_key, data_shards, parity_shards);
        base.volume = bucket.to_string();
        base.name = object.to_string();
        base.size = i64::try_from(payload.len()).expect("test payload should fit i64");
        base.is_latest = true;
        base.version_id = Some(Uuid::new_v4());
        base.data_dir = Some(Uuid::new_v4());
        base.mod_time = Some(OffsetDateTime::now_utc());
        base.metadata.insert("etag".to_string(), "etag-inline".to_string());
        base.add_object_part(1, "part-etag-inline".to_string(), payload.len(), base.mod_time, base.size, None, None);
        base.set_inline_data();
        base.uses_legacy_checksum = uses_legacy_checksum;

        let erasure = coding::Erasure::new_with_options(
            base.erasure.data_blocks,
            base.erasure.parity_blocks,
            base.erasure.block_size,
            base.uses_legacy_checksum,
        );
        let shards = erasure.encode_data(payload).expect("inline payload should encode");
        let checksum_algo = if base.uses_legacy_checksum {
            HashAlgorithm::HighwayHash256SLegacy
        } else {
            HashAlgorithm::HighwayHash256S
        };

        let mut files = Vec::with_capacity(shards.len());
        for (index, shard) in shards.into_iter().enumerate() {
            let mut writer = coding::BitrotWriterWrapper::new(
                coding::CustomWriter::new_inline_buffer(),
                erasure.shard_size(),
                checksum_algo.clone(),
            );
            writer.write(&shard).await.expect("inline shard should write");
            writer.shutdown().await.expect("inline writer should shutdown");
            let mut fi = base.clone();
            fi.erasure.index = index + 1;
            fi.data = Some(Bytes::from(
                writer
                    .into_inline_data()
                    .expect("inline bitrot writer should retain encoded data"),
            ));
            files.push(fi);
        }
        files
    }

    async fn inline_metadata_fanout_fileinfos(bucket: &str, object: &str, payload: &[u8]) -> Vec<FileInfo> {
        inline_metadata_fanout_fileinfos_with_mode(bucket, object, payload, false).await
    }

    async fn install_inline_metadata_fanout_fileinfo(
        disks: &[Option<DiskStore>],
        bucket: &str,
        object: &str,
        payload: &[u8],
        mutate: impl FnOnce(&mut [FileInfo]),
    ) {
        let mut files = inline_metadata_fanout_fileinfos(bucket, object, payload).await;
        mutate(&mut files);
        install_inline_metadata_fanout_files(disks, bucket, object, files).await;
    }

    async fn install_inline_metadata_fanout_fileinfo_with_geometry(
        disks: &[Option<DiskStore>],
        bucket: &str,
        object: &str,
        payload: &[u8],
        data_shards: usize,
        parity_shards: usize,
        mutate: impl FnOnce(&mut [FileInfo]),
    ) {
        let mut files =
            inline_metadata_fanout_fileinfos_with_geometry(bucket, object, payload, false, data_shards, parity_shards).await;
        mutate(&mut files);
        install_inline_metadata_fanout_files(disks, bucket, object, files).await;
    }

    async fn install_inline_metadata_fanout_files(disks: &[Option<DiskStore>], bucket: &str, object: &str, files: Vec<FileInfo>) {
        let distribution = files
            .first()
            .map(|file| file.erasure.distribution.clone())
            .expect("inline metadata fixture should include shards");
        for (disk_index, disk) in disks
            .iter()
            .enumerate()
            .filter_map(|(disk_index, disk)| disk.as_ref().map(|disk| (disk_index, disk)))
        {
            let block_index = distribution
                .get(disk_index)
                .copied()
                .expect("inline metadata fixture should cover every disk");
            let file_info = files
                .get(block_index.checked_sub(1).expect("erasure block indexes are one-based"))
                .expect("inline metadata fixture should include every distributed shard")
                .clone();
            disk.write_metadata(bucket, bucket, object, file_info)
                .await
                .expect("inline metadata should be installed on every disk");
        }
    }

    fn object_with_initial_data_shards(bucket: &str, prefix: &str, data_shards: usize, initial_fanout: usize) -> String {
        (0..1000)
            .map(|index| format!("{prefix}-{index}"))
            .find(|name| {
                let order = bounded_metadata_fanout_order(bucket, name, data_shards + 2, 2);
                let distribution_key = metadata_distribution_key(bucket, name);
                let distribution = FileInfo::new(&distribution_key, data_shards, 2).erasure.distribution;
                order
                    .iter()
                    .take(initial_fanout)
                    .filter_map(|disk_index| distribution.get(*disk_index).copied())
                    .filter(|block_index| (1..=data_shards).contains(block_index))
                    .collect::<HashSet<_>>()
                    .len()
                    == data_shards
            })
            .expect("test should find an object whose initial fanout covers every data shard")
    }

    fn initial_data_shard_indexes(bucket: &str, object: &str, data_shards: usize, initial_fanout: usize) -> Vec<usize> {
        let order = bounded_metadata_fanout_order(bucket, object, data_shards + 2, 2);
        let distribution_key = metadata_distribution_key(bucket, object);
        let distribution = FileInfo::new(&distribution_key, data_shards, 2).erasure.distribution;
        order
            .iter()
            .take(initial_fanout)
            .filter_map(|disk_index| distribution.get(*disk_index).copied())
            .filter(|block_index| (1..=data_shards).contains(block_index))
            .collect()
    }

    fn bounded_spare_disk_index(bucket: &str, object: &str, data_shards: usize, parity_shards: usize) -> usize {
        let total_disks = data_shards + parity_shards;
        let initial_fanout = if data_shards == parity_shards {
            data_shards + 1
        } else {
            data_shards
        };
        *bounded_metadata_fanout_order(bucket, object, total_disks, parity_shards)
            .get(initial_fanout)
            .expect("test geometry should leave one bounded spare disk")
    }

    #[test]
    fn bounded_metadata_fanout_order_prioritizes_default_data_shards() {
        let bucket = "bounded-order-bucket";
        let object = "bounded-order-data-shards-first";
        let order = bounded_metadata_fanout_order(bucket, object, 16, 4);
        let distribution_key = [bucket, object].join("/");
        let distribution = FileInfo::new(&distribution_key, 12, 4).erasure.distribution;
        let initial_blocks: HashSet<_> = order
            .iter()
            .take(12)
            .filter_map(|disk_index| distribution.get(*disk_index).copied())
            .collect();

        assert_eq!(order.len(), 16);
        assert_eq!(order.iter().copied().collect::<HashSet<_>>().len(), 16);
        assert_eq!(initial_blocks, (1..=12).collect());
    }

    #[test]
    fn bounded_metadata_fanout_order_uses_written_bucket_object_key() {
        let bucket = "bounded-order-rotation-bucket";
        let (object, bare_distribution, stored_distribution) = (0..1000)
            .map(|index| format!("bounded-order-rotation-object-{index}"))
            .find_map(|object| {
                let bare_distribution = FileInfo::new(&object, 12, 4).erasure.distribution;
                let stored_key = [bucket, object.as_str()].join("/");
                let stored_distribution = FileInfo::new(&stored_key, 12, 4).erasure.distribution;
                (bare_distribution != stored_distribution).then_some((object, bare_distribution, stored_distribution))
            })
            .expect("test should find a bucket/object pair with a different distribution rotation");

        let order = bounded_metadata_fanout_order(bucket, &object, 16, 4);
        let initial_stored_blocks: HashSet<_> = order
            .iter()
            .take(12)
            .filter_map(|disk_index| stored_distribution.get(*disk_index).copied())
            .collect();
        let initial_bare_blocks: HashSet<_> = order
            .iter()
            .take(12)
            .filter_map(|disk_index| bare_distribution.get(*disk_index).copied())
            .collect();

        assert_eq!(initial_stored_blocks, (1..=12).collect());
        assert_ne!(
            initial_bare_blocks,
            (1..=12).collect(),
            "test fixture must prove the bare object distribution would pick the wrong initial data-shard set"
        );
    }

    #[tokio::test]
    async fn bounded_metadata_early_stop_non_inline_fallback_schedules_beyond_initial_quorum() {
        const DISKS: usize = 6;
        let bucket = "bounded-data-get-six-disk-fanout-bucket";
        let object = "bounded-data-get-six-disk-object";
        let (dirs, disks) = call_counter_local_disks(bucket, DISKS).await;
        install_metadata_fanout_fileinfo(&disks, bucket, object, None).await;

        temp_env::async_with_vars(
            [
                ("RUSTFS_GET_METADATA_EARLY_STOP_ENABLE", Some("true")),
                ("RUSTFS_GET_METADATA_DATA_READ_EARLY_STOP_ENABLE", Some("true")),
                ("RUSTFS_GET_METADATA_EARLY_STOP_BOUNDED_FANOUT", Some("true")),
            ],
            async {
                let calls = disk_call_counters::observe(object);
                let (parts_metadata, errs, diagnostics) =
                    SetDisks::read_all_fileinfo_observed(&disks, bucket, bucket, object, "", true, false, false, true, 3)
                        .await
                        .expect("non-inline metadata should resolve after force-full-wait fallback");

                assert_eq!(
                    calls.total(disk_call_counters::KIND_READ_VERSION),
                    DISKS as u64,
                    "force-full-wait fallback must schedule disks beyond the initial write quorum"
                );
                assert_eq!(diagnostics.total_responses(), DISKS);
                assert_eq!(parts_metadata.iter().filter(|fi| fi.name == object).count(), DISKS);
                assert!(errs.iter().all(Option::is_none));
            },
        )
        .await;

        drop(dirs);
    }

    #[tokio::test]
    async fn bounded_metadata_early_stop_allows_verified_inline_data_get_quorum() {
        const DISKS: usize = 4;
        let bucket = "bounded-inline-data-get-fanout-bucket";
        let object = object_with_initial_data_shards(bucket, "bounded-inline-data-get-object", 2, 3);
        let (dirs, disks) = call_counter_local_disks(bucket, DISKS).await;
        install_inline_metadata_fanout_fileinfo(&disks, bucket, &object, b"verified inline payload", |_| {}).await;

        temp_env::async_with_vars(
            [
                ("RUSTFS_GET_METADATA_EARLY_STOP_ENABLE", Some("true")),
                ("RUSTFS_GET_METADATA_DATA_READ_EARLY_STOP_ENABLE", Some("true")),
                ("RUSTFS_GET_METADATA_EARLY_STOP_BOUNDED_FANOUT", Some("true")),
            ],
            async {
                let spare_disk = bounded_spare_disk_index(bucket, &object, 2, 2);
                let barrier = rename_fanout_barrier::arm(&object, spare_disk, rename_fanout_barrier::PHASE_READ_VERSION);
                let tracker = rename_fanout_barrier::observe_tasks(&object);
                let calls = disk_call_counters::observe(&object);
                let disks_for_read = disks.clone();
                let object_for_read = object.clone();
                let mut read = tokio::spawn(async move {
                    SetDisks::read_all_fileinfo_observed(
                        &disks_for_read,
                        bucket,
                        bucket,
                        &object_for_read,
                        "",
                        true,
                        false,
                        false,
                        true,
                        2,
                    )
                    .await
                });

                tokio::time::timeout(BARRIER_PAUSE_GUARD, barrier.wait_until_paused())
                    .await
                    .expect("bounded fanout should hedge and pause the spare disk");
                let (parts_metadata, errs, diagnostics) = tokio::time::timeout(BARRIER_PAUSE_GUARD, &mut read)
                    .await
                    .expect("verified inline quorum should return without the paused spare")
                    .expect("read task should join")
                    .expect("verified inline metadata should reach early-stop quorum");

                assert_eq!(
                    calls.total(disk_call_counters::KIND_READ_VERSION),
                    DISKS as u64,
                    "bounded fanout may schedule one spare before the verified inline quorum returns"
                );
                assert_eq!(
                    tracker.running(),
                    0,
                    "early-stop should drain spawned read_version tasks before returning"
                );
                assert_eq!(diagnostics.total_responses(), 3);
                assert_eq!(parts_metadata.iter().filter(|fi| fi.name == object).count(), 3);
                assert!(errs.iter().all(Option::is_none));
            },
        )
        .await;

        drop(dirs);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn bounded_metadata_early_stop_waits_for_pending_inline_data_shard() {
        const DISKS: usize = 6;
        const DATA_SHARDS: usize = 4;
        const PARITY_SHARDS: usize = 2;
        let bucket = "bounded-inline-data-get-pending-shard-bucket";
        let object =
            object_with_initial_data_shards(bucket, "bounded-inline-data-get-pending-shard-object", DATA_SHARDS, DATA_SHARDS);
        let (dirs, disks) = call_counter_local_disks(bucket, DISKS).await;
        install_inline_metadata_fanout_fileinfo_with_geometry(
            &disks,
            bucket,
            &object,
            b"verified inline payload",
            DATA_SHARDS,
            PARITY_SHARDS,
            |_| {},
        )
        .await;

        temp_env::async_with_vars(
            [
                ("RUSTFS_GET_METADATA_EARLY_STOP_ENABLE", Some("true")),
                ("RUSTFS_GET_METADATA_DATA_READ_EARLY_STOP_ENABLE", Some("true")),
                ("RUSTFS_GET_METADATA_EARLY_STOP_BOUNDED_FANOUT", Some("true")),
            ],
            async {
                let fanout_order = bounded_metadata_fanout_order(bucket, &object, DISKS, PARITY_SHARDS);
                let distribution_key = metadata_distribution_key(bucket, &object);
                let distribution = FileInfo::new(&distribution_key, DATA_SHARDS, PARITY_SHARDS)
                    .erasure
                    .distribution;
                let paused_data_disk = *fanout_order
                    .iter()
                    .take(DATA_SHARDS)
                    .find(|disk_index| {
                        distribution
                            .get(**disk_index)
                            .is_some_and(|block_index| (1..=DATA_SHARDS).contains(block_index))
                    })
                    .expect("initial fanout should include a data shard to pause");
                let hedged_parity_disk = fanout_order[DATA_SHARDS];
                let unscheduled_parity_disk = fanout_order[DATA_SHARDS + 1];

                let barrier = rename_fanout_barrier::arm(&object, paused_data_disk, rename_fanout_barrier::PHASE_READ_VERSION);
                let tracker = rename_fanout_barrier::observe_tasks(&object);
                let calls = disk_call_counters::observe(&object);
                let disks_for_read = disks.clone();
                let object_for_read = object.clone();
                let mut read = tokio::spawn(async move {
                    SetDisks::read_all_fileinfo_observed(
                        &disks_for_read,
                        bucket,
                        bucket,
                        &object_for_read,
                        "",
                        true,
                        false,
                        false,
                        true,
                        PARITY_SHARDS,
                    )
                    .await
                });

                tokio::time::timeout(BARRIER_PAUSE_GUARD, barrier.wait_until_paused())
                    .await
                    .expect("initial data shard should pause before returning");
                tokio::time::timeout(BARRIER_PAUSE_GUARD, async {
                    while calls.for_disk(disk_call_counters::KIND_READ_VERSION, hedged_parity_disk) == 0 {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .expect("bounded fanout should hedge one parity disk while the data shard is pending");

                assert!(
                    tokio::time::timeout(BARRIER_PAUSE_GUARD, &mut read).await.is_err(),
                    "inline data-read early-stop must wait for a scheduled missing data shard instead of forcing full wait"
                );

                barrier.release();
                let (parts_metadata, errs, diagnostics) = read
                    .await
                    .expect("metadata read task should not panic")
                    .expect("pending data shard should let the inline verifier finish");

                assert_eq!(
                    calls.total(disk_call_counters::KIND_READ_VERSION),
                    5,
                    "pending data-shard defer should not schedule the final parity disk"
                );
                assert_eq!(
                    calls.for_disk(disk_call_counters::KIND_READ_VERSION, unscheduled_parity_disk),
                    0,
                    "the remaining parity disk must stay unissued when pending data verification succeeds"
                );
                assert_eq!(
                    tracker.running(),
                    0,
                    "early-stop should drain spawned read_version tasks before returning"
                );
                assert_eq!(diagnostics.total_responses(), 5);
                assert_eq!(parts_metadata.iter().filter(|fi| fi.name == object).count(), 5);
                assert!(errs.iter().all(Option::is_none));
            },
        )
        .await;

        drop(dirs);
    }

    #[tokio::test]
    async fn data_read_early_stop_verifies_legacy_inline_checksum_payload() {
        let bucket = "legacy-inline-data-get-fanout-bucket";
        let object = "legacy-inline-data-get-object";
        let payload = b"legacy inline payload whose size is not divisible by the data shard count";
        let (_dirs, disks) = call_counter_local_disks(bucket, 4).await;
        let files = inline_metadata_fanout_fileinfos_with_mode(bucket, object, payload, true).await;
        let distribution = files
            .first()
            .map(|file| file.erasure.distribution.clone())
            .expect("legacy fixture should include metadata");
        let order = bounded_metadata_fanout_order(bucket, object, 4, 2);
        let mut parts_metadata = vec![FileInfo::default(); 4];
        for disk_index in order.into_iter().take(3) {
            let block_index = distribution
                .get(disk_index)
                .copied()
                .expect("legacy fixture distribution should cover every disk");
            parts_metadata[disk_index] = files
                .get(block_index.checked_sub(1).expect("erasure block indexes are one-based"))
                .expect("legacy fixture should include every distributed shard")
                .clone();
        }
        let candidate = parts_metadata
            .iter()
            .find(|file| file.name == object)
            .expect("legacy fixture should include observed metadata")
            .clone();

        assert!(
            data_read_early_stop_inline_body_miss_reason(bucket, object, &candidate, &parts_metadata, &disks)
                .await
                .is_none(),
            "legacy inline metadata must use the legacy bitrot shard sizing and checksum algorithm"
        );
    }

    #[tokio::test]
    async fn data_read_early_stop_reports_inline_miss_reasons() {
        let bucket = "inline-data-get-miss-reason-bucket";
        let object = "inline-data-get-miss-reason-object";
        let payload = b"verified inline payload";
        let (_dirs, disks) = call_counter_local_disks(bucket, 4).await;
        let files = inline_metadata_fanout_fileinfos_with_mode(bucket, object, payload, false).await;
        let distribution = files
            .first()
            .map(|file| file.erasure.distribution.clone())
            .expect("fixture should include metadata");
        let order = bounded_metadata_fanout_order(bucket, object, 4, 2);
        let mut parts_metadata = vec![FileInfo::default(); 4];
        for disk_index in order.into_iter().take(3) {
            let block_index = distribution
                .get(disk_index)
                .copied()
                .expect("fixture distribution should cover every disk");
            parts_metadata[disk_index] = files
                .get(block_index.checked_sub(1).expect("erasure block indexes are one-based"))
                .expect("fixture should include every distributed shard")
                .clone();
        }
        let candidate = parts_metadata
            .iter()
            .find(|file| file.name == object)
            .expect("fixture should include observed metadata")
            .clone();
        let data_disk = distribution
            .iter()
            .position(|block_index| *block_index == 1)
            .expect("fixture distribution should include first data shard");

        assert_eq!(
            data_read_early_stop_inline_body_miss_reason(bucket, object, &candidate, &parts_metadata, &disks).await,
            None
        );

        let mut not_inline = candidate.clone();
        rustfs_utils::http::remove_str(&mut not_inline.metadata, rustfs_utils::http::SUFFIX_INLINE_DATA);
        assert_eq!(
            data_read_early_stop_inline_body_miss_reason(bucket, object, &not_inline, &parts_metadata, &disks).await,
            Some(GET_METADATA_EARLY_STOP_REASON_DATA_READ_INLINE_NOT_INLINE)
        );

        let mut remote = candidate.clone();
        remote.transition_status = TRANSITION_COMPLETE.to_string();
        assert_eq!(
            data_read_early_stop_inline_body_miss_reason(bucket, object, &remote, &parts_metadata, &disks).await,
            Some(GET_METADATA_EARLY_STOP_REASON_DATA_READ_INLINE_REMOTE)
        );

        let mut transformed = candidate.clone();
        rustfs_utils::http::insert_str(&mut transformed.metadata, rustfs_utils::http::SUFFIX_COMPRESSION, "zstd".to_string());
        assert_eq!(
            data_read_early_stop_inline_body_miss_reason(bucket, object, &transformed, &parts_metadata, &disks).await,
            Some(GET_METADATA_EARLY_STOP_REASON_DATA_READ_INLINE_TRANSFORMED)
        );

        let mut deleted = candidate.clone();
        deleted.deleted = true;
        assert_eq!(
            data_read_early_stop_inline_body_miss_reason(bucket, object, &deleted, &parts_metadata, &disks).await,
            Some(GET_METADATA_EARLY_STOP_REASON_DATA_READ_INLINE_DELETED)
        );

        let mut zero_size = candidate.clone();
        zero_size.size = 0;
        assert_eq!(
            data_read_early_stop_inline_body_miss_reason(bucket, object, &zero_size, &parts_metadata, &disks).await,
            Some(GET_METADATA_EARLY_STOP_REASON_DATA_READ_INLINE_SIZE)
        );

        let mut multipart = candidate.clone();
        multipart.parts.push(multipart.parts[0].clone());
        assert_eq!(
            data_read_early_stop_inline_body_miss_reason(bucket, object, &multipart, &parts_metadata, &disks).await,
            Some(GET_METADATA_EARLY_STOP_REASON_DATA_READ_INLINE_PART_SHAPE)
        );

        let mut invalid_geometry = candidate.clone();
        invalid_geometry.erasure.data_blocks = 0;
        assert_eq!(
            data_read_early_stop_inline_body_miss_reason(bucket, object, &invalid_geometry, &parts_metadata, &disks).await,
            Some(GET_METADATA_EARLY_STOP_REASON_DATA_READ_INLINE_GEOMETRY)
        );

        let mut missing_shard = parts_metadata.clone();
        missing_shard[data_disk] = FileInfo::default();
        assert_eq!(
            data_read_early_stop_inline_body_miss_reason(bucket, object, &candidate, &missing_shard, &disks).await,
            Some(GET_METADATA_EARLY_STOP_REASON_DATA_READ_INLINE_MISSING_SHARD)
        );

        let mut missing_payload = parts_metadata.clone();
        missing_payload[data_disk].data = None;
        assert_eq!(
            data_read_early_stop_inline_body_miss_reason(bucket, object, &candidate, &missing_payload, &disks).await,
            Some(GET_METADATA_EARLY_STOP_REASON_DATA_READ_INLINE_MISSING_PAYLOAD)
        );

        let mut identity_mismatch = parts_metadata.clone();
        identity_mismatch[data_disk].version_id = Some(Uuid::new_v4());
        assert_eq!(
            data_read_early_stop_inline_body_miss_reason(bucket, object, &candidate, &identity_mismatch, &disks).await,
            Some(GET_METADATA_EARLY_STOP_REASON_DATA_READ_INLINE_IDENTITY_MISMATCH)
        );

        let mut corrupt = parts_metadata.clone();
        if let Some(data) = corrupt[data_disk].data.as_mut() {
            let mut corrupt_data = data.to_vec();
            corrupt_data[0] ^= 0x01;
            *data = Bytes::from(corrupt_data);
        }
        assert_eq!(
            data_read_early_stop_inline_body_miss_reason(bucket, object, &candidate, &corrupt, &disks).await,
            Some(GET_METADATA_EARLY_STOP_REASON_DATA_READ_INLINE_BODY_VERIFY)
        );
    }

    #[test]
    #[serial_test::serial]
    fn metadata_fanout_lifecycle_records_real_early_stop_abort() {
        assert_metadata_fanout_lifecycle_records_real_early_stop_abort(
            "lifecycle-inline-data-get-fanout-bucket",
            "lifecycle-inline-data-get-object",
            GET_OBJECT_PATH_LEGACY_DUPLEX,
            None,
        );
    }

    #[test]
    #[serial_test::serial]
    fn metadata_fanout_lifecycle_records_bounded_early_stop_abort() {
        assert_metadata_fanout_lifecycle_records_real_early_stop_abort(
            "lifecycle-bounded-inline-data-get-fanout-bucket",
            "lifecycle-bounded-inline-data-get-object",
            GET_OBJECT_PATH_LEGACY_DUPLEX,
            Some("true"),
        );
    }

    #[test]
    #[serial_test::serial]
    fn metadata_fanout_lifecycle_records_internal_meta_early_stop_abort_path() {
        assert_metadata_fanout_lifecycle_records_real_early_stop_abort(
            RUSTFS_META_BUCKET,
            "buckets/.usage-cache/lifecycle-inline-data-get-object",
            GET_OBJECT_PATH_INTERNAL_META,
            None,
        );
    }

    #[test]
    #[serial_test::serial]
    fn metadata_fanout_records_internal_meta_final_miss_path() {
        const DISKS: usize = 4;
        let bucket = RUSTFS_META_BUCKET;
        let object = object_with_initial_data_shards(bucket, "buckets/.usage-cache/final-miss-inline-data-get-object", 2, 3);
        let corrupt_shard = initial_data_shard_indexes(bucket, &object, 2, 3)[0];
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime should build");
        let recorder = crate::test_metrics::CapturingRecorder::default();
        let previous_gate = rustfs_io_metrics::get_stage_metrics_enabled();
        rustfs_io_metrics::set_get_stage_metrics_enabled(true);

        metrics::with_local_recorder(&recorder, || {
            runtime.block_on(async {
                let (dirs, disks) = call_counter_local_disks(bucket, DISKS).await;
                install_inline_metadata_fanout_fileinfo(&disks, bucket, &object, b"verified inline payload", |files| {
                    if let Some(data) = files.get_mut(corrupt_shard - 1).and_then(|file| file.data.as_mut()) {
                        let mut corrupt = data.to_vec();
                        corrupt[0] ^= 0x01;
                        *data = Bytes::from(corrupt);
                    }
                })
                .await;
                temp_env::async_with_vars(
                    [
                        ("RUSTFS_GET_METADATA_EARLY_STOP_ENABLE", Some("true")),
                        ("RUSTFS_GET_METADATA_DATA_READ_EARLY_STOP_ENABLE", Some("true")),
                        ("RUSTFS_GET_METADATA_EARLY_STOP_BOUNDED_FANOUT", Some("true")),
                    ],
                    async {
                        let (parts_metadata, errs, diagnostics) = SetDisks::read_all_fileinfo_observed(
                            &disks, bucket, bucket, &object, "", true, false, false, true, 2,
                        )
                        .await
                        .expect("corrupt internal inline metadata should fall back to full fanout");

                        assert_eq!(diagnostics.total_responses(), DISKS);
                        assert_eq!(parts_metadata.iter().filter(|fi| fi.name == object).count(), DISKS);
                        assert!(errs.iter().all(Option::is_none));
                    },
                )
                .await;
                drop(dirs);
            });
        });
        rustfs_io_metrics::set_get_stage_metrics_enabled(previous_gate);

        assert_eq!(
            recorder.counter_value(
                "rustfs_io_get_object_metadata_early_stop_total",
                &[
                    ("path", GET_OBJECT_PATH_INTERNAL_META),
                    ("decision", "miss"),
                    ("reason", GET_METADATA_EARLY_STOP_REASON_DATA_READ_INLINE_BODY_VERIFY),
                ],
            ),
            1,
            "internal metadata final early-stop miss must retain its path label"
        );
        assert_eq!(
            recorder.counter_value(
                "rustfs_io_get_object_metadata_early_stop_total",
                &[
                    ("path", GET_OBJECT_PATH_LEGACY_DUPLEX),
                    ("decision", "miss"),
                    ("reason", GET_METADATA_EARLY_STOP_REASON_DATA_READ_INLINE_BODY_VERIFY),
                ],
            ),
            0,
            "internal metadata final early-stop miss must not leak into legacy_duplex"
        );
        assert_eq!(
            recorder.histogram_values(
                "rustfs_io_get_object_metadata_early_stop_saved_responses",
                &[("path", GET_OBJECT_PATH_INTERNAL_META)]
            ),
            vec![0.0],
            "internal metadata final miss must record zero saved responses on internal_meta"
        );
        assert!(
            recorder
                .histogram_values(
                    "rustfs_io_get_object_metadata_early_stop_saved_responses",
                    &[("path", GET_OBJECT_PATH_LEGACY_DUPLEX)]
                )
                .is_empty(),
            "internal metadata final miss saved responses must not leak into legacy_duplex"
        );
    }

    fn assert_metadata_fanout_lifecycle_records_real_early_stop_abort(
        bucket: &'static str,
        object_prefix: &str,
        expected_path: &'static str,
        bounded_fanout_env: Option<&'static str>,
    ) {
        const DISKS: usize = 4;
        let object = object_with_initial_data_shards(bucket, object_prefix, 2, 3);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime should build");
        let recorder = crate::test_metrics::CapturingRecorder::default();
        let previous_gate = rustfs_io_metrics::get_stage_metrics_enabled();
        rustfs_io_metrics::set_get_stage_metrics_enabled(true);

        metrics::with_local_recorder(&recorder, || {
            runtime.block_on(async {
                let (dirs, disks) = call_counter_local_disks(bucket, DISKS).await;
                install_inline_metadata_fanout_fileinfo(&disks, bucket, &object, b"verified inline payload", |_| {}).await;
                temp_env::async_with_vars(
                    [
                        ("RUSTFS_GET_METADATA_EARLY_STOP_ENABLE", Some("true")),
                        ("RUSTFS_GET_METADATA_DATA_READ_EARLY_STOP_ENABLE", Some("true")),
                        ("RUSTFS_GET_METADATA_EARLY_STOP_BOUNDED_FANOUT", bounded_fanout_env),
                    ],
                    async {
                        let barrier_disk = if bounded_fanout_env.is_some() {
                            bounded_spare_disk_index(bucket, &object, 2, 2)
                        } else {
                            3
                        };
                        let barrier =
                            rename_fanout_barrier::arm(&object, barrier_disk, rename_fanout_barrier::PHASE_READ_VERSION);
                        let disks_for_read = disks.clone();
                        let object_for_read = object.clone();
                        let mut read = tokio::spawn(async move {
                            SetDisks::read_all_fileinfo_observed(
                                &disks_for_read,
                                bucket,
                                bucket,
                                &object_for_read,
                                "",
                                true,
                                false,
                                false,
                                true,
                                2,
                            )
                            .await
                        });

                        tokio::time::timeout(BARRIER_PAUSE_GUARD, barrier.wait_until_paused())
                            .await
                            .expect("metadata fanout should pause the spare metadata task");
                        let (parts_metadata, errs, diagnostics) = tokio::time::timeout(BARRIER_PAUSE_GUARD, &mut read)
                            .await
                            .expect("verified inline quorum should abort the paused eager fanout")
                            .expect("read task should join")
                            .expect("verified inline metadata should reach early-stop quorum");

                        assert_eq!(diagnostics.total_responses(), 3);
                        assert_eq!(parts_metadata.iter().filter(|fi| fi.name == object).count(), 3);
                        assert!(errs.iter().all(Option::is_none));
                    },
                )
                .await;
                drop(dirs);
            });
        });
        rustfs_io_metrics::set_get_stage_metrics_enabled(previous_gate);

        assert_eq!(
            recorder.histogram_values("rustfs_io_get_object_metadata_fanout_scheduled", &[("path", expected_path)]),
            vec![4.0]
        );
        assert_eq!(
            recorder.histogram_values("rustfs_io_get_object_metadata_fanout_completed", &[("path", expected_path)]),
            vec![3.0]
        );
        assert_eq!(
            recorder.histogram_values("rustfs_io_get_object_metadata_fanout_cancelled", &[("path", expected_path)]),
            vec![1.0]
        );
        assert_eq!(
            recorder.counter_value(
                "rustfs_io_get_object_metadata_early_stop_total",
                &[
                    ("path", expected_path),
                    ("decision", "hit"),
                    ("reason", GET_METADATA_EARLY_STOP_REASON_VALID_QUORUM),
                ],
            ),
            1,
            "early-stop hit must retain its path label"
        );
        let unexpected_path = if expected_path == GET_OBJECT_PATH_INTERNAL_META {
            GET_OBJECT_PATH_LEGACY_DUPLEX
        } else {
            GET_OBJECT_PATH_INTERNAL_META
        };
        assert_eq!(
            recorder.counter_value(
                "rustfs_io_get_object_metadata_early_stop_total",
                &[
                    ("path", unexpected_path),
                    ("decision", "hit"),
                    ("reason", GET_METADATA_EARLY_STOP_REASON_VALID_QUORUM),
                ],
            ),
            0,
            "early-stop hit must not leak into the other metadata path"
        );
        assert_eq!(
            recorder.histogram_values("rustfs_io_get_object_metadata_early_stop_saved_responses", &[("path", expected_path)]),
            vec![1.0]
        );
        assert!(
            recorder
                .histogram_values("rustfs_io_get_object_metadata_early_stop_saved_responses", &[("path", unexpected_path)])
                .is_empty(),
            "early-stop saved responses must not leak into the other metadata path"
        );
    }

    #[tokio::test]
    async fn bounded_metadata_early_stop_full_waits_when_inline_data_is_corrupt() {
        const DISKS: usize = 4;
        let bucket = "bounded-inline-data-get-corrupt-bucket";
        let object = object_with_initial_data_shards(bucket, "bounded-inline-data-get-corrupt-object", 2, 3);
        let corrupt_shard = initial_data_shard_indexes(bucket, &object, 2, 3)[0];
        let (dirs, disks) = call_counter_local_disks(bucket, DISKS).await;
        install_inline_metadata_fanout_fileinfo(&disks, bucket, &object, b"verified inline payload", |files| {
            if let Some(data) = files.get_mut(corrupt_shard - 1).and_then(|file| file.data.as_mut()) {
                let mut corrupt = data.to_vec();
                corrupt[0] ^= 0x01;
                *data = Bytes::from(corrupt);
            }
        })
        .await;

        temp_env::async_with_vars(
            [
                ("RUSTFS_GET_METADATA_EARLY_STOP_ENABLE", Some("true")),
                ("RUSTFS_GET_METADATA_DATA_READ_EARLY_STOP_ENABLE", Some("true")),
                ("RUSTFS_GET_METADATA_EARLY_STOP_BOUNDED_FANOUT", Some("true")),
            ],
            async {
                let calls = disk_call_counters::observe(&object);
                let (parts_metadata, errs, diagnostics) =
                    SetDisks::read_all_fileinfo_observed(&disks, bucket, bucket, &object, "", true, false, false, true, 2)
                        .await
                        .expect("corrupt inline metadata should fall back to full fanout");

                assert_eq!(
                    calls.total(disk_call_counters::KIND_READ_VERSION),
                    DISKS as u64,
                    "inline bitrot failure must keep metadata fanout open instead of aborting spares"
                );
                assert_eq!(diagnostics.total_responses(), DISKS);
                assert_eq!(parts_metadata.iter().filter(|fi| fi.name == object).count(), DISKS);
                assert!(errs.iter().all(Option::is_none));
            },
        )
        .await;

        drop(dirs);
    }

    #[tokio::test]
    async fn bounded_metadata_early_stop_full_waits_when_inline_generation_differs() {
        const DISKS: usize = 4;
        let bucket = "bounded-inline-data-get-generation-bucket";
        let object = object_with_initial_data_shards(bucket, "bounded-inline-data-get-generation-object", 2, 3);
        let stale_shard = initial_data_shard_indexes(bucket, &object, 2, 3)[0];
        let (dirs, disks) = call_counter_local_disks(bucket, DISKS).await;
        install_inline_metadata_fanout_fileinfo(&disks, bucket, &object, b"verified inline payload", |files| {
            if let Some(file) = files.get_mut(stale_shard - 1) {
                file.version_id = Some(Uuid::new_v4());
            }
        })
        .await;

        temp_env::async_with_vars(
            [
                ("RUSTFS_GET_METADATA_EARLY_STOP_ENABLE", Some("true")),
                ("RUSTFS_GET_METADATA_DATA_READ_EARLY_STOP_ENABLE", Some("true")),
                ("RUSTFS_GET_METADATA_EARLY_STOP_BOUNDED_FANOUT", Some("true")),
            ],
            async {
                let calls = disk_call_counters::observe(&object);
                let (parts_metadata, errs, diagnostics) =
                    SetDisks::read_all_fileinfo_observed(&disks, bucket, bucket, &object, "", true, false, false, true, 2)
                        .await
                        .expect("mixed-generation inline metadata should fall back to full fanout");

                assert_eq!(
                    calls.total(disk_call_counters::KIND_READ_VERSION),
                    DISKS as u64,
                    "inline data from a different metadata generation must not satisfy the data-read gate"
                );
                assert_eq!(diagnostics.total_responses(), DISKS);
                assert_eq!(parts_metadata.iter().filter(|fi| fi.name == object).count(), DISKS);
                assert!(errs.iter().all(Option::is_none));
            },
        )
        .await;

        drop(dirs);
    }

    #[tokio::test]
    async fn bounded_metadata_early_stop_full_waits_when_inline_shard_identity_is_copied() {
        const DISKS: usize = 4;
        let bucket = "bounded-inline-data-get-copied-shard-bucket";
        let object = object_with_initial_data_shards(bucket, "bounded-inline-data-get-copied-shard-object", 2, 3);
        let data_shards = initial_data_shard_indexes(bucket, &object, 2, 3);
        let source_shard = data_shards[0];
        let target_shard = data_shards[1];
        let (dirs, disks) = call_counter_local_disks(bucket, DISKS).await;
        install_inline_metadata_fanout_fileinfo(&disks, bucket, &object, b"verified inline payload", |files| {
            let copied = files[source_shard - 1].clone();
            files[target_shard - 1].erasure.index = copied.erasure.index;
            files[target_shard - 1].data = copied.data;
        })
        .await;

        temp_env::async_with_vars(
            [
                ("RUSTFS_GET_METADATA_EARLY_STOP_ENABLE", Some("true")),
                ("RUSTFS_GET_METADATA_DATA_READ_EARLY_STOP_ENABLE", Some("true")),
                ("RUSTFS_GET_METADATA_EARLY_STOP_BOUNDED_FANOUT", Some("true")),
            ],
            async {
                let spare_disk = bounded_spare_disk_index(bucket, &object, 2, 2);
                let barrier = rename_fanout_barrier::arm(&object, spare_disk, rename_fanout_barrier::PHASE_READ_VERSION);
                let calls = disk_call_counters::observe(&object);
                let disks_for_read = disks.clone();
                let object_for_read = object.clone();
                let mut read = tokio::spawn(async move {
                    SetDisks::read_all_fileinfo_observed(
                        &disks_for_read,
                        bucket,
                        bucket,
                        &object_for_read,
                        "",
                        true,
                        false,
                        false,
                        true,
                        2,
                    )
                    .await
                });

                tokio::time::timeout(BARRIER_PAUSE_GUARD, barrier.wait_until_paused())
                    .await
                    .expect("full-wait fallback should schedule the spare disk");
                assert!(
                    tokio::time::timeout(BARRIER_PAUSE_GUARD, &mut read).await.is_err(),
                    "copied inline shard identity must not satisfy the early-stop data gate"
                );
                barrier.release();

                let (parts_metadata, errs, diagnostics) = read
                    .await
                    .expect("metadata read task should not panic")
                    .expect("copied inline shard identity should fall back to full fanout");

                assert_eq!(
                    calls.total(disk_call_counters::KIND_READ_VERSION),
                    DISKS as u64,
                    "copied shard identity must keep metadata fanout open instead of aborting spares"
                );
                assert_eq!(diagnostics.total_responses(), DISKS);
                assert_eq!(parts_metadata.iter().filter(|fi| fi.name == object).count(), DISKS);
                assert!(errs.iter().all(Option::is_none));
            },
        )
        .await;

        drop(dirs);
    }

    #[tokio::test]
    async fn bounded_metadata_early_stop_full_waits_for_transformed_inline_metadata() {
        const DISKS: usize = 4;
        let bucket = "bounded-inline-data-get-transform-bucket";
        let (dirs, disks) = call_counter_local_disks(bucket, DISKS).await;

        async fn assert_full_wait(disks: &[Option<DiskStore>], bucket: &str, object: &str, mutate: impl FnOnce(&mut [FileInfo])) {
            const DISKS: usize = 4;
            install_inline_metadata_fanout_fileinfo(disks, bucket, object, b"verified inline payload", mutate).await;

            temp_env::async_with_vars(
                [
                    ("RUSTFS_GET_METADATA_EARLY_STOP_ENABLE", Some("true")),
                    ("RUSTFS_GET_METADATA_DATA_READ_EARLY_STOP_ENABLE", Some("true")),
                    ("RUSTFS_GET_METADATA_EARLY_STOP_BOUNDED_FANOUT", Some("true")),
                ],
                async {
                    let calls = disk_call_counters::observe(object);
                    let (parts_metadata, errs, diagnostics) =
                        SetDisks::read_all_fileinfo_observed(disks, bucket, bucket, object, "", true, false, false, true, 2)
                            .await
                            .expect("transformed inline metadata should fall back to full fanout");

                    assert_eq!(
                        calls.total(disk_call_counters::KIND_READ_VERSION),
                        DISKS as u64,
                        "transformed inline metadata must not pass the plaintext data-read gate"
                    );
                    assert_eq!(diagnostics.total_responses(), DISKS);
                    assert_eq!(parts_metadata.iter().filter(|fi| fi.name == object).count(), DISKS);
                    assert!(errs.iter().all(Option::is_none));
                },
            )
            .await;
        }

        assert_full_wait(&disks, bucket, "bounded-inline-data-get-compressed-object", |files| {
            for file in files {
                rustfs_utils::http::insert_str(&mut file.metadata, rustfs_utils::http::SUFFIX_COMPRESSION, "zstd".to_string());
            }
        })
        .await;

        assert_full_wait(&disks, bucket, "bounded-inline-data-get-encrypted-object", |files| {
            for file in files {
                file.metadata
                    .insert(rustfs_utils::http::headers::SSEC_ALGORITHM_HEADER.to_string(), "AES256".to_string());
            }
        })
        .await;

        drop(dirs);
    }

    #[tokio::test]
    async fn bounded_metadata_early_stop_full_waits_for_unsafe_inline_metadata_shapes() {
        const DISKS: usize = 4;
        let bucket = "bounded-inline-data-get-unsafe-shape-bucket";
        let (dirs, disks) = call_counter_local_disks(bucket, DISKS).await;

        async fn assert_full_wait(disks: &[Option<DiskStore>], bucket: &str, object: &str, mutate: impl FnOnce(&mut [FileInfo])) {
            const DISKS: usize = 4;
            install_inline_metadata_fanout_fileinfo(disks, bucket, object, b"verified inline payload", mutate).await;

            temp_env::async_with_vars(
                [
                    ("RUSTFS_GET_METADATA_EARLY_STOP_ENABLE", Some("true")),
                    ("RUSTFS_GET_METADATA_DATA_READ_EARLY_STOP_ENABLE", Some("true")),
                    ("RUSTFS_GET_METADATA_EARLY_STOP_BOUNDED_FANOUT", Some("true")),
                ],
                async {
                    let calls = disk_call_counters::observe(object);
                    let (parts_metadata, errs, diagnostics) =
                        SetDisks::read_all_fileinfo_observed(disks, bucket, bucket, object, "", true, false, false, true, 2)
                            .await
                            .expect("unsafe inline metadata shape should fall back to full fanout");

                    assert_eq!(
                        calls.total(disk_call_counters::KIND_READ_VERSION),
                        DISKS as u64,
                        "unsafe inline metadata shape {object} must not abort remaining metadata responses"
                    );
                    assert_eq!(diagnostics.total_responses(), DISKS);
                    assert_eq!(parts_metadata.iter().filter(|fi| fi.name == object).count(), DISKS);
                    assert!(errs.iter().all(Option::is_none));
                },
            )
            .await;
        }

        assert_full_wait(&disks, bucket, "bounded-inline-data-get-remote-object", |files| {
            for file in files {
                file.transition_status = TRANSITION_COMPLETE.to_string();
            }
        })
        .await;

        assert_full_wait(&disks, bucket, "bounded-inline-data-get-zero-size-object", |files| {
            for file in files {
                file.size = 0;
                if let Some(part) = file.parts.first_mut() {
                    part.size = 0;
                }
            }
        })
        .await;

        assert_full_wait(&disks, bucket, "bounded-inline-data-get-multipart-object", |files| {
            for file in files {
                let mut second_part = file.parts[0].clone();
                second_part.number = 2;
                file.parts.push(second_part);
            }
        })
        .await;

        assert_full_wait(&disks, bucket, "bounded-inline-data-get-part-size-mismatch-object", |files| {
            for file in files {
                if let Some(part) = file.parts.first_mut() {
                    part.size = part.size.saturating_add(1);
                }
            }
        })
        .await;

        assert_full_wait(&disks, bucket, "bounded-inline-data-get-oversize-object", |files| {
            for file in files {
                let oversize = file.erasure.block_size.saturating_add(1);
                file.size = i64::try_from(oversize).expect("test block size should fit i64");
                if let Some(part) = file.parts.first_mut() {
                    part.size = oversize;
                }
            }
        })
        .await;

        drop(dirs);
    }

    #[tokio::test]
    async fn bounded_metadata_early_stop_full_waits_for_purge_pending_inline_payload() {
        const DISKS: usize = 4;
        let bucket = "bounded-inline-data-get-purge-pending-bucket";
        let object = object_with_initial_data_shards(bucket, "bounded-inline-data-get-purge-pending-object", 2, 3);
        let (dirs, disks) = call_counter_local_disks(bucket, DISKS).await;
        install_inline_metadata_fanout_fileinfo(&disks, bucket, &object, b"verified inline payload", |files| {
            for file in files {
                rustfs_utils::http::insert_str(
                    &mut file.metadata,
                    rustfs_utils::http::SUFFIX_PURGESTATUS,
                    "target=PENDING;".to_string(),
                );
                let replication_state = crate::bucket::replication::ReplicationState {
                    version_purge_status_internal: Some("target=PENDING;".to_string()),
                    purge_targets: crate::bucket::replication::version_purge_statuses_map("target=PENDING;"),
                    ..Default::default()
                };
                file.replication_state_internal =
                    Some(crate::bucket::replication::replication_state_to_filemeta(&replication_state));
                file.deleted = true;
                assert!(
                    !file.is_canonical_delete_marker(),
                    "test fixture must remain an erasure-backed purge-pending payload"
                );
            }
        })
        .await;

        temp_env::async_with_vars(
            [
                ("RUSTFS_GET_METADATA_EARLY_STOP_ENABLE", Some("true")),
                ("RUSTFS_GET_METADATA_DATA_READ_EARLY_STOP_ENABLE", Some("true")),
                ("RUSTFS_GET_METADATA_EARLY_STOP_BOUNDED_FANOUT", Some("true")),
            ],
            async {
                let spare_disk = bounded_spare_disk_index(bucket, &object, 2, 2);
                let barrier = rename_fanout_barrier::arm(&object, spare_disk, rename_fanout_barrier::PHASE_READ_VERSION);
                let calls = disk_call_counters::observe(&object);
                let disks_for_read = disks.clone();
                let object_for_read = object.clone();
                let mut read = tokio::spawn(async move {
                    SetDisks::read_all_fileinfo_observed(
                        &disks_for_read,
                        bucket,
                        bucket,
                        &object_for_read,
                        "",
                        true,
                        false,
                        false,
                        true,
                        2,
                    )
                    .await
                });

                tokio::time::timeout(BARRIER_PAUSE_GUARD, barrier.wait_until_paused())
                    .await
                    .expect("purge-pending fallback should schedule the spare disk");
                assert!(
                    tokio::time::timeout(BARRIER_PAUSE_GUARD, &mut read).await.is_err(),
                    "purge-pending payload metadata must not satisfy the inline data-read gate"
                );
                barrier.release();

                let (parts_metadata, errs, diagnostics) = read
                    .await
                    .expect("metadata read task should not panic")
                    .expect("purge-pending inline payload should fall back to full fanout");

                assert_eq!(
                    calls.total(disk_call_counters::KIND_READ_VERSION),
                    DISKS as u64,
                    "purge-pending payload must keep metadata fanout open instead of aborting spares"
                );
                assert_eq!(diagnostics.total_responses(), DISKS);
                assert_eq!(parts_metadata.iter().filter(|fi| fi.name == object).count(), DISKS);
                assert!(errs.iter().all(Option::is_none));
            },
        )
        .await;

        drop(dirs);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn bounded_non_inline_data_get_immediately_forces_full_fanout() {
        const DISKS: usize = 4;
        let bucket = "bounded-data-get-hedge-bucket";
        let object = "bounded-data-get-hedge-object";
        let (dirs, disks) = call_counter_local_disks(bucket, DISKS).await;
        install_metadata_fanout_fileinfo(&disks, bucket, object, None).await;

        temp_env::async_with_vars(
            [
                ("RUSTFS_GET_METADATA_EARLY_STOP_ENABLE", Some("true")),
                ("RUSTFS_GET_METADATA_DATA_READ_EARLY_STOP_ENABLE", Some("true")),
                ("RUSTFS_GET_METADATA_EARLY_STOP_BOUNDED_FANOUT", Some("true")),
            ],
            async {
                let barrier = rename_fanout_barrier::arm(object, 2, rename_fanout_barrier::PHASE_READ_VERSION);
                let calls = disk_call_counters::observe(object);
                let disks_for_read = disks.clone();
                let mut read = tokio::spawn(async move {
                    SetDisks::read_all_fileinfo_observed(&disks_for_read, bucket, bucket, object, "", true, false, false, true, 2)
                        .await
                });

                tokio::time::timeout(BARRIER_PAUSE_GUARD, barrier.wait_until_paused())
                    .await
                    .expect("third scheduled read_version should pause at the deterministic barrier");
                tokio::time::timeout(BARRIER_PAUSE_GUARD, async {
                    while calls.for_disk(disk_call_counters::KIND_READ_VERSION, 3) == 0 {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .expect("bounded non-inline data-read fanout should immediately schedule the spare disk");

                let pending = tokio::time::timeout(BARRIER_PAUSE_GUARD, &mut read).await;
                assert!(
                    pending.is_err(),
                    "non-inline data reads must not return before the paused metadata response"
                );
                barrier.release();
                let (parts_metadata, errs, diagnostics) = read
                    .await
                    .expect("metadata read task should not panic")
                    .expect("healthy spare metadata should resolve");

                assert_eq!(
                    calls.total(disk_call_counters::KIND_READ_VERSION),
                    DISKS as u64,
                    "bounded non-inline data-read fanout should issue the paused disk plus the remaining spare"
                );
                assert_eq!(diagnostics.total_responses(), DISKS);
                assert_eq!(parts_metadata.iter().filter(|fi| fi.name == object).count(), DISKS);
                assert!(errs.iter().all(Option::is_none));
            },
        )
        .await;

        drop(dirs);
    }

    #[tokio::test]
    async fn bounded_metadata_early_stop_defaults_keep_non_inline_data_get_full_fanout() {
        const DISKS: usize = 4;
        let bucket = "bounded-data-get-default-bucket";
        let object = "bounded-data-get-default-object";
        let (dirs, disks) = call_counter_local_disks(bucket, DISKS).await;
        install_metadata_fanout_fileinfo(&disks, bucket, object, None).await;

        temp_env::async_with_vars(
            [
                ("RUSTFS_GET_METADATA_EARLY_STOP_ENABLE", None::<&str>),
                ("RUSTFS_GET_METADATA_DATA_READ_EARLY_STOP_ENABLE", None::<&str>),
                ("RUSTFS_GET_METADATA_EARLY_STOP_BOUNDED_FANOUT", None::<&str>),
            ],
            async {
                let barrier = rename_fanout_barrier::arm(object, 2, rename_fanout_barrier::PHASE_READ_VERSION);
                let calls = disk_call_counters::observe(object);
                let disks_for_read = disks.clone();
                let mut read = tokio::spawn(async move {
                    SetDisks::read_all_fileinfo_observed(&disks_for_read, bucket, bucket, object, "", true, false, false, true, 2)
                        .await
                });

                tokio::time::timeout(BARRIER_PAUSE_GUARD, barrier.wait_until_paused())
                    .await
                    .expect("default bounded non-inline read should schedule the paused metadata task");
                tokio::time::timeout(BARRIER_PAUSE_GUARD, async {
                    while calls.for_disk(disk_call_counters::KIND_READ_VERSION, 3) == 0 {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .expect(
                    "default bounded non-inline read should immediately force full fanout after the first non-inline response",
                );

                let pending = tokio::time::timeout(BARRIER_PAUSE_GUARD, &mut read).await;
                assert!(
                    pending.is_err(),
                    "default non-inline data reads must not return before the paused metadata response"
                );
                barrier.release();
                let (parts_metadata, errs, diagnostics) = read
                    .await
                    .expect("metadata read task should not panic")
                    .expect("default data-read metadata should resolve");

                assert_eq!(
                    calls.total(disk_call_counters::KIND_READ_VERSION),
                    DISKS as u64,
                    "default non-inline GET data-read metadata must keep full fanout without waiting for a quorum miss first"
                );
                assert_eq!(diagnostics.total_responses(), DISKS);
                assert_eq!(parts_metadata.iter().filter(|fi| fi.name == object).count(), DISKS);
                assert!(errs.iter().all(Option::is_none));
            },
        )
        .await;

        drop(dirs);
    }

    #[tokio::test]
    async fn bounded_metadata_early_stop_falls_back_to_full_fanout_on_data_read_error() {
        const DISKS: usize = 4;
        let bucket = "bounded-data-get-error-bucket";
        let object = "bounded-data-get-error-object";
        let (dirs, disks) = call_counter_local_disks(bucket, DISKS).await;
        install_metadata_fanout_fileinfo(&disks, bucket, object, Some(0)).await;

        temp_env::async_with_vars(
            [
                ("RUSTFS_GET_METADATA_EARLY_STOP_ENABLE", Some("true")),
                ("RUSTFS_GET_METADATA_DATA_READ_EARLY_STOP_ENABLE", Some("true")),
                ("RUSTFS_GET_METADATA_EARLY_STOP_BOUNDED_FANOUT", Some("true")),
            ],
            async {
                let calls = disk_call_counters::observe(object);
                let (_, errs, diagnostics) =
                    SetDisks::read_all_fileinfo_observed(&disks, bucket, bucket, object, "", true, false, false, true, 2)
                        .await
                        .expect("metadata fanout should complete after falling back to all disks");

                assert_eq!(
                    calls.total(disk_call_counters::KIND_READ_VERSION),
                    DISKS as u64,
                    "a data-read error must force bounded fanout to schedule every disk before returning"
                );
                assert_eq!(diagnostics.total_responses(), DISKS);
                assert!(
                    errs.iter()
                        .any(|err| err.as_ref().is_some_and(|err| matches!(err, DiskError::FileNotFound)))
                );
            },
        )
        .await;

        drop(dirs);
    }

    /// Bound for the pause handshake. This is a hang-guard, not a timing
    /// dependency: under a working barrier `wait_until_paused` returns via the
    /// `Notify` handshake far below this bound regardless of IO pressure, so the
    /// assertions never depend on the value. It only turns a neutralized-barrier
    /// hang into a deterministic failure instead of an infinite wait.
    const BARRIER_PAUSE_GUARD: std::time::Duration = std::time::Duration::from_secs(10);

    fn rename_barrier_fileinfos(object: &str, count: usize) -> Vec<FileInfo> {
        (0..count).map(|_| metadata_test_fileinfo(object)).collect()
    }

    fn rename_commit_fileinfos(object: &str, count: usize, etag: &str) -> Vec<FileInfo> {
        (0..count)
            .map(|idx| {
                let mut file_info = metadata_test_fileinfo(object);
                file_info.mod_time = Some(OffsetDateTime::now_utc());
                file_info.erasure.index = idx + 1;
                file_info.data = Some(Bytes::from_static(b"inline-body"));
                file_info.metadata.insert("etag".to_string(), etag.to_string());
                file_info
            })
            .collect()
    }

    #[tokio::test]
    async fn rename_data_skips_offline_placeholder_when_validating_new_metadata() {
        let (_dirs, mut online_disks) = call_counter_local_disks("rename-validation-bucket", 1).await;
        let online_disk = online_disks.pop().expect("one test disk should be present");
        let mut file_info = metadata_test_fileinfo("rename-unassigned-index");
        file_info.erasure.index = 0;

        let err = SetDisks::rename_data(
            &[None, online_disk],
            RUSTFS_META_TMP_BUCKET,
            "source",
            &[FileInfo::default(), file_info],
            "bucket",
            "object",
            1,
        )
        .await
        .expect_err("the missing staged source must fail after metadata validation");

        assert_ne!(err, DiskError::FileCorrupt);
    }

    #[tokio::test]
    async fn rename_data_accepts_canonical_delete_marker() {
        let bucket = "rename-delete-marker-bucket";
        let object = "object";
        let (_dirs, mut online_disks) = call_counter_local_disks(bucket, 1).await;
        let online_disk = online_disks.pop().expect("one test disk slot should be present");
        let disk = online_disk.as_ref().expect("test disk should be online");
        match disk.make_volume(RUSTFS_META_TMP_BUCKET).await {
            Ok(()) | Err(DiskError::VolumeExists) => {}
            Err(err) => panic!("temporary metadata volume should be available: {err:?}"),
        }
        let version_id = Uuid::new_v4();
        let mut marker = metadata_test_delete_marker(object, version_id, OffsetDateTime::now_utc());
        marker
            .metadata
            .insert("x-rustfs-internal-purgestatus".to_string(), "pending".to_string());

        SetDisks::rename_data(
            &[None, online_disk.clone()],
            RUSTFS_META_TMP_BUCKET,
            "source",
            &[FileInfo::default(), marker],
            bucket,
            object,
            1,
        )
        .await
        .expect("canonical delete marker should commit without erasure payload");

        let stored = disk
            .read_version("", bucket, object, &version_id.to_string(), &ReadOptions::default())
            .await
            .expect("committed delete marker should be readable");

        assert!(stored.deleted);
        assert_eq!(stored.version_id, Some(version_id));
        assert_eq!(stored.erasure.index, 0);
        assert_eq!(stored.metadata.get("x-rustfs-internal-purgestatus").map(String::as_str), Some("pending"));
    }

    #[tokio::test]
    async fn rename_data_preserves_null_delete_marker_type() {
        let bucket = "rename-null-marker-bucket";
        let object = "object";
        let (_dirs, mut online_disks) = call_counter_local_disks(bucket, 1).await;
        let online_disk = online_disks.pop().expect("one test disk slot should be present");
        let disk = online_disk.as_ref().expect("test disk should be online");
        match disk.make_volume(RUSTFS_META_TMP_BUCKET).await {
            Ok(()) | Err(DiskError::VolumeExists) => {}
            Err(err) => panic!("temporary metadata volume should be available: {err:?}"),
        }
        let marker = metadata_test_delete_marker(object, Uuid::new_v4(), OffsetDateTime::now_utc());
        let marker = FileInfo {
            version_id: None,
            ..marker
        };

        SetDisks::rename_data(
            std::slice::from_ref(&online_disk),
            RUSTFS_META_TMP_BUCKET,
            "source",
            &[marker],
            bucket,
            object,
            1,
        )
        .await
        .expect("null delete marker should commit");

        let stored = disk
            .read_version("", bucket, object, "", &ReadOptions::default())
            .await
            .expect("null delete marker should remain readable");
        assert!(stored.deleted);
        assert_eq!(stored.version_id, None);
        assert!(stored.is_canonical_delete_marker());
    }

    /// Overwriting an inline version with a non-inline one stages the old
    /// xl.meta as `<object>/<synthetic-rollback-dir>/xl.meta.bkp` for the
    /// quorum-failure undo. After a successful quorum commit that dir must be
    /// reclaimed — leftover residue keeps DeleteBucket failing with
    /// BucketNotEmpty long after the object itself is deleted.
    #[tokio::test]
    async fn rename_data_reclaims_synthetic_inline_rollback_dir_after_commit() {
        let bucket = "rename-inline-rollback-bucket";
        let object = "object";
        let (dirs, mut online_disks) = call_counter_local_disks(bucket, 1).await;
        let online_disk = online_disks.pop().expect("one test disk slot should be present");
        let disk = online_disk.as_ref().expect("test disk should be online");
        match disk.make_volume(RUSTFS_META_TMP_BUCKET).await {
            Ok(()) | Err(DiskError::VolumeExists) => {}
            Err(err) => panic!("temporary metadata volume should be available: {err:?}"),
        }

        let disk_root = dirs[0].path();

        // Commit an inline version (data carried in xl.meta, no data dir).
        let mut inline_fi = metadata_test_fileinfo(object);
        inline_fi.data = Some(Bytes::from_static(b"inline-body"));
        inline_fi.mod_time = Some(OffsetDateTime::now_utc());
        std::fs::create_dir_all(disk_root.join(RUSTFS_META_TMP_BUCKET).join("tmp-inline"))
            .expect("inline staging dir should be created");
        // Use rename_data_owned so we can await the tail_drain for cleanup.
        let commit = SetDisks::rename_data_owned(
            std::slice::from_ref(&online_disk),
            RUSTFS_META_TMP_BUCKET,
            "tmp-inline",
            vec![inline_fi],
            bucket,
            object,
            1,
        )
        .await
        .expect("inline version should commit");
        if let Some(td) = commit.tail_drain {
            td.await.expect("inline commit tail drain must succeed");
        }

        // Overwrite the same (nil) version with a non-inline one.
        let new_data_dir = Uuid::new_v4();
        let mut streaming_fi = metadata_test_fileinfo(object);
        streaming_fi.data_dir = Some(new_data_dir);
        streaming_fi.mod_time = Some(OffsetDateTime::now_utc());
        let staged_data_dir = disk_root
            .join(RUSTFS_META_TMP_BUCKET)
            .join("tmp-streaming")
            .join(new_data_dir.to_string());
        std::fs::create_dir_all(&staged_data_dir).expect("streaming staging dir should be created");
        std::fs::write(staged_data_dir.join("part.1"), b"streamed-body").expect("staged part should be written");
        let commit = SetDisks::rename_data_owned(
            std::slice::from_ref(&online_disk),
            RUSTFS_META_TMP_BUCKET,
            "tmp-streaming",
            vec![streaming_fi],
            bucket,
            object,
            1,
        )
        .await
        .expect("non-inline overwrite should commit");
        if let Some(td) = commit.tail_drain {
            td.await.expect("non-inline overwrite tail drain must succeed");
        }

        let mut leftovers: Vec<String> = std::fs::read_dir(disk_root.join(bucket).join(object))
            .expect("committed object dir should be readable")
            .map(|entry| {
                entry
                    .expect("object dir entry should be readable")
                    .file_name()
                    .to_string_lossy()
                    .into_owned()
            })
            .collect();
        leftovers.sort();
        assert_eq!(
            leftovers,
            vec![new_data_dir.to_string(), STORAGE_FORMAT_FILE.to_string()],
            "only the committed data dir and xl.meta may remain — synthetic rollback residue breaks DeleteBucket"
        );
    }

    #[tokio::test]
    async fn rename_delete_marker_quorum_failure_restores_existing_metadata() {
        let bucket = "rename-marker-quorum-bucket";
        let object = "object";
        let (_dirs, mut online_disks) = call_counter_local_disks(bucket, 1).await;
        let online_disk = online_disks.pop().expect("one test disk slot should be present");
        let disk = online_disk.as_ref().expect("test disk should be online");
        match disk.make_volume(RUSTFS_META_TMP_BUCKET).await {
            Ok(()) | Err(DiskError::VolumeExists) => {}
            Err(err) => panic!("temporary metadata volume should be available: {err:?}"),
        }
        let old_version_id = Uuid::new_v4();
        let mut old = metadata_test_fileinfo(object);
        old.version_id = Some(old_version_id);
        old.mod_time = Some(OffsetDateTime::now_utc());
        disk.write_metadata(bucket, bucket, object, old)
            .await
            .expect("old metadata should be written");
        let marker_version_id = Uuid::new_v4();
        let marker = metadata_test_delete_marker(object, marker_version_id, OffsetDateTime::now_utc());

        let err = SetDisks::rename_data(
            &[online_disk.clone(), None],
            RUSTFS_META_TMP_BUCKET,
            "source",
            &[marker, FileInfo::default()],
            bucket,
            object,
            2,
        )
        .await
        .expect_err("quorum-minus-one marker commit should fail");

        assert_eq!(err, DiskError::ErasureWriteQuorum);
        let restored = disk
            .read_version("", bucket, object, &old_version_id.to_string(), &ReadOptions::default())
            .await
            .expect("old metadata should remain after rollback");
        assert!(!restored.deleted);
        assert_eq!(restored.version_id, Some(old_version_id));
        assert!(matches!(
            disk.read_version("", bucket, object, &marker_version_id.to_string(), &ReadOptions::default())
                .await,
            Err(DiskError::FileVersionNotFound)
        ));
    }

    /// Demo / regression guard for the backlog#1325 rename fan-out pause barrier
    /// and background-task introspection. Serves the barrier-style acceptance of
    /// #1312 ("assert no background disk write remains after release").
    ///
    /// It drives the real `SetDisks::rename_data` fan-out: a barrier is armed at
    /// the first disk's `rename` phase, and the test awaits that pause point,
    /// asserts a background disk task is still in flight, releases it, and then
    /// asserts the in-flight count drains to zero once the fan-out completes.
    ///
    /// Neutralizing the barrier seam (`rename_fanout_barrier` -> immediate no-op)
    /// makes `wait_until_paused` never wake, so the guarded await elapses and the
    /// test fails. Neutralizing the task guard (`rename_fanout_task_guard` ->
    /// `()`) pins `running()` at zero, so the "still in flight" assertion fails.
    #[tokio::test]
    async fn rename_fanout_barrier_pauses_and_reports_running_background_tasks() {
        const DISKS: usize = 4;
        let bucket = "rename-barrier-bucket";
        let object = "rename-barrier-object";
        let (dirs, disks) = call_counter_local_disks(bucket, DISKS).await;
        let file_infos = rename_barrier_fileinfos(object, DISKS);

        let tracker = rename_fanout_barrier::observe_tasks(object);
        assert_eq!(tracker.running(), 0, "no fan-out tasks before rename starts");

        let barrier = rename_fanout_barrier::arm(object, 0, rename_fanout_barrier::PHASE_RENAME);

        // Run the real rename fan-out concurrently with the control flow so we can
        // introspect while it is parked at the first disk's rename checkpoint.
        let rename_fut = SetDisks::rename_data(&disks, bucket, object, &file_infos, bucket, object, DISKS - 1);
        let control_fut = async {
            tokio::time::timeout(BARRIER_PAUSE_GUARD, barrier.wait_until_paused())
                .await
                .expect("fan-out must reach the armed rename barrier");
            assert!(barrier.is_paused(), "target task must be parked at the checkpoint");
            assert!(tracker.running() >= 1, "a background rename task must still be in flight while paused");
            barrier.release();
        };
        let (_rename_res, ()) = tokio::join!(rename_fut, control_fut);

        // The fan-out has fully joined -> every task guard has dropped.
        assert_eq!(tracker.running(), 0, "no background rename task may remain once the fan-out has drained");

        drop(dirs);
    }

    #[tokio::test]
    #[serial_test::serial(rename_quorum_ack)]
    async fn rename_fanout_drains_after_caller_cancellation() {
        const DISKS: usize = 4;
        let bucket = "rename-cancel-bucket";
        let object = "rename-cancel-object";
        let (dirs, disks) = call_counter_local_disks(bucket, DISKS).await;
        let marker = metadata_test_delete_marker(object, Uuid::new_v4(), OffsetDateTime::now_utc());
        let file_infos = vec![marker; DISKS];
        let tracker = rename_fanout_barrier::observe_tasks(object);
        let barrier = rename_fanout_barrier::arm(object, 0, rename_fanout_barrier::PHASE_RENAME);

        let rename =
            tokio::spawn(
                async move { SetDisks::rename_data(&disks, bucket, object, &file_infos, bucket, object, DISKS - 1).await },
            );
        tokio::time::timeout(BARRIER_PAUSE_GUARD, barrier.wait_until_paused())
            .await
            .expect("rename fan-out must reach the armed barrier");
        rename.abort();
        assert!(
            rename
                .await
                .expect_err("aborted caller should report cancellation")
                .is_cancelled(),
            "caller task should be cancelled, not panic"
        );
        assert!(tracker.running() >= 1, "the coordinator must retain in-flight disk mutations");

        barrier.release();
        tokio::time::timeout(BARRIER_PAUSE_GUARD, async {
            while tracker.running() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("cancelled caller's disk mutations must drain");

        for (idx, dir) in dirs.iter().enumerate() {
            assert!(
                dir.path().join(bucket).join(object).join(STORAGE_FORMAT_FILE).exists(),
                "disk {idx} must finish the rename after caller cancellation"
            );
        }
    }

    #[tokio::test]
    #[serial_test::serial(rename_quorum_ack)]
    async fn rename_data_waits_for_tail_disk_after_write_quorum() {
        // Explicitly test the serial (join_all) path: early ack is now the
        // default, so disable it to verify the legacy behaviour still works.
        temp_env::async_with_vars([(ENV_RUSTFS_PUT_RENAME_EARLY_ACK_ENABLE, Some("false"))], async {
            const DISKS: usize = 4;
            let bucket = "rename-tail-success-bucket";
            let object = "rename-tail-success-object";
            let (dirs, disks) = call_counter_local_disks(bucket, DISKS).await;
            prepare_rename_source_dirs(&dirs, &disks, "source").await;
            let file_infos = rename_commit_fileinfos(object, DISKS, "tail-success-etag");
            let barrier = rename_fanout_barrier::arm(object, 0, rename_fanout_barrier::PHASE_RENAME);

            let rename = SetDisks::rename_data(&disks, RUSTFS_META_TMP_BUCKET, "source", &file_infos, bucket, object, 3);
            tokio::pin!(rename);
            tokio::time::timeout(BARRIER_PAUSE_GUARD, async {
                tokio::select! {
                    () = barrier.wait_until_paused() => {}
                    result = &mut rename => panic!("rename_data returned before the armed fan-out barrier: {result:?}"),
                }
            })
            .await
            .expect("paused disk must reach the armed rename barrier");

            assert!(
                tokio::time::timeout(Duration::from_millis(50), &mut rename).await.is_err(),
                "serial rename_data waits for the paused fan-out disk even after write quorum"
            );

            barrier.release();
            rename
                .await
                .expect("tail success must complete the rename after the barrier is released");

            for (idx, dir) in dirs.iter().enumerate() {
                let reopened = reopen_local_disk(dir).await;
                let stored = reopened
                    .read_version("", bucket, object, "", &ReadOptions::default())
                    .await
                    .unwrap_or_else(|err| panic!("disk {idx} must contain the tail-success commit after reopen: {err:?}"));
                assert_eq!(
                    stored.metadata.get("etag").map(String::as_str),
                    Some("tail-success-etag"),
                    "disk {idx} must expose the same committed metadata after tail success and reopen"
                );
            }

            drop(dirs);
        })
        .await;
    }

    #[tokio::test]
    #[serial_test::serial(rename_quorum_ack)]
    async fn rename_data_early_ack_returns_after_write_quorum_and_drains_tail_success() {
        temp_env::async_with_vars([(ENV_RUSTFS_PUT_RENAME_EARLY_ACK_ENABLE, Some("true"))], async {
            const DISKS: usize = 4;
            let bucket = "rename-early-ack-tail-success-bucket";
            let object = "rename-early-ack-tail-success-object";
            let (dirs, disks) = call_counter_local_disks(bucket, DISKS).await;
            prepare_rename_source_dirs(&dirs, &disks, "source").await;
            let file_infos = rename_commit_fileinfos(object, DISKS, "early-tail-success-etag");
            let tracker = rename_fanout_barrier::observe_tasks(object);
            let barrier = rename_fanout_barrier::arm(object, 0, rename_fanout_barrier::PHASE_RENAME);

            let mut rename = Box::pin(SetDisks::rename_data(
                &disks,
                RUSTFS_META_TMP_BUCKET,
                "source",
                &file_infos,
                bucket,
                object,
                3,
            ));
            tokio::time::timeout(BARRIER_PAUSE_GUARD, async {
                tokio::select! {
                    () = barrier.wait_until_paused() => {}
                    result = rename.as_mut() => panic!("rename_data returned before the armed fan-out barrier: {result:?}"),
                }
            })
            .await
            .expect("paused disk must reach the armed rename barrier");

            rename
                .await
                .expect("early ACK must return once the other three disks satisfy write quorum");
            assert!(
                tracker.running() >= 1,
                "the paused tail disk must continue in the background after early ACK"
            );

            barrier.release();
            tokio::time::timeout(BARRIER_PAUSE_GUARD, async {
                while tracker.running() != 0 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("background tail disk must drain after release");

            for (idx, dir) in dirs.iter().enumerate() {
                let reopened = reopen_local_disk(dir).await;
                let stored = reopened
                    .read_version("", bucket, object, "", &ReadOptions::default())
                    .await
                    .unwrap_or_else(|err| panic!("disk {idx} must contain the early-ACK commit after reopen: {err:?}"));
                assert_eq!(
                    stored.metadata.get("etag").map(String::as_str),
                    Some("early-tail-success-etag"),
                    "disk {idx} must expose the same committed metadata after background tail success and reopen"
                );
            }
        })
        .await;
    }

    #[tokio::test]
    #[serial_test::serial(rename_quorum_ack)]
    async fn rename_data_early_ack_tail_failure_does_not_expose_partial_fresh_after_reopen() {
        temp_env::async_with_vars([(ENV_RUSTFS_PUT_RENAME_EARLY_ACK_ENABLE, Some("true"))], async {
            const DISKS: usize = 4;
            let bucket = "rename-early-ack-tail-failure-bucket";
            let object = "rename-early-ack-tail-failure-object";
            let (dirs, disks) = call_counter_local_disks(bucket, DISKS).await;
            prepare_rename_source_dirs(&dirs, &disks, "source").await;
            let file_infos = rename_commit_fileinfos(object, DISKS, "early-tail-failure-etag");
            let tracker = rename_fanout_barrier::observe_tasks(object);
            let barrier = rename_fanout_barrier::arm(object, 0, rename_fanout_barrier::PHASE_RENAME);
            let _fault = rename_fault_injection::fail_rename_on(object, &[0]);

            let mut rename = Box::pin(SetDisks::rename_data(
                &disks,
                RUSTFS_META_TMP_BUCKET,
                "source",
                &file_infos,
                bucket,
                object,
                3,
            ));
            tokio::time::timeout(BARRIER_PAUSE_GUARD, async {
                tokio::select! {
                    () = barrier.wait_until_paused() => {}
                    result = rename.as_mut() => panic!("rename_data returned before the armed fan-out barrier: {result:?}"),
                }
            })
            .await
            .expect("paused disk must reach the armed rename barrier");

            rename
                .await
                .expect("early ACK must return after write quorum before the tail failure");
            barrier.release();
            tokio::time::timeout(BARRIER_PAUSE_GUARD, async {
                while tracker.running() != 0 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("background tail failure must drain after release");

            for (idx, dir) in dirs.iter().enumerate() {
                let reopened = reopen_local_disk(dir).await;
                let read = reopened.read_version("", bucket, object, "", &ReadOptions::default()).await;
                if idx == 0 {
                    assert!(
                        matches!(read, Err(DiskError::FileNotFound | DiskError::FileVersionNotFound)),
                        "failed background tail disk must not expose a partial fresh commit after reopen: {read:?}"
                    );
                } else {
                    let stored = read.unwrap_or_else(|err| {
                        panic!("quorum disk {idx} must contain the early-ACK commit after reopen: {err:?}")
                    });
                    assert_eq!(
                        stored.metadata.get("etag").map(String::as_str),
                        Some("early-tail-failure-etag"),
                        "quorum disk {idx} must expose the committed metadata after reopen"
                    );
                }
            }
        })
        .await;
    }

    #[tokio::test]
    #[serial_test::serial(rename_quorum_ack)]
    async fn rename_data_early_ack_tail_failure_preserves_overwrite_after_reopen() {
        temp_env::async_with_vars([(ENV_RUSTFS_PUT_RENAME_EARLY_ACK_ENABLE, Some("true"))], async {
            const DISKS: usize = 4;
            let bucket = "rename-early-ack-overwrite-tail-failure-bucket";
            let object = "rename-early-ack-overwrite-tail-failure-object";
            let (dirs, disks) = call_counter_local_disks(bucket, DISKS).await;
            prepare_rename_source_dirs(&dirs, &disks, "source").await;
            let mut old = metadata_test_fileinfo(object);
            old.mod_time = Some(OffsetDateTime::now_utc());
            old.data = Some(Bytes::from_static(b"old-inline-body"));
            old.metadata.insert("etag".to_string(), "early-old-etag".to_string());
            for disk in disks.iter().flatten() {
                disk.write_metadata(bucket, bucket, object, old.clone())
                    .await
                    .expect("old metadata should be written before early overwrite");
            }

            let file_infos = rename_commit_fileinfos(object, DISKS, "early-new-etag");
            let tracker = rename_fanout_barrier::observe_tasks(object);
            let barrier = rename_fanout_barrier::arm(object, 0, rename_fanout_barrier::PHASE_RENAME);
            let _fault = rename_fault_injection::fail_rename_on(object, &[0]);

            let mut rename = Box::pin(SetDisks::rename_data(
                &disks,
                RUSTFS_META_TMP_BUCKET,
                "source",
                &file_infos,
                bucket,
                object,
                3,
            ));
            tokio::time::timeout(BARRIER_PAUSE_GUARD, async {
                tokio::select! {
                    () = barrier.wait_until_paused() => {}
                    result = rename.as_mut() => panic!("rename_data returned before the armed fan-out barrier: {result:?}"),
                }
            })
            .await
            .expect("paused disk must reach the armed rename barrier");

            rename
                .await
                .expect("early ACK must return after write quorum before overwrite tail failure");
            barrier.release();
            tokio::time::timeout(BARRIER_PAUSE_GUARD, async {
                while tracker.running() != 0 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("background overwrite tail failure must drain after release");

            for (idx, dir) in dirs.iter().enumerate() {
                let reopened = reopen_local_disk(dir).await;
                let stored = reopened
                    .read_version("", bucket, object, "", &ReadOptions::default())
                    .await
                    .unwrap_or_else(|err| panic!("disk {idx} must have a readable version after early overwrite: {err:?}"));
                let expected_etag = if idx == 0 { "early-old-etag" } else { "early-new-etag" };
                assert_eq!(
                    stored.metadata.get("etag").map(String::as_str),
                    Some(expected_etag),
                    "disk {idx} must keep the correct early-ACK overwrite visibility after reopen"
                );
            }
        })
        .await;
    }

    #[tokio::test]
    #[serial_test::serial(rename_quorum_ack)]
    async fn rename_data_early_ack_background_drains_after_caller_cancellation() {
        temp_env::async_with_vars([(ENV_RUSTFS_PUT_RENAME_EARLY_ACK_ENABLE, Some("true"))], async {
            const DISKS: usize = 4;
            let bucket = "rename-early-ack-cancel-bucket";
            let object = "rename-early-ack-cancel-object";
            let (dirs, disks) = call_counter_local_disks(bucket, DISKS).await;
            prepare_rename_source_dirs(&dirs, &disks, "source").await;
            let file_infos = rename_commit_fileinfos(object, DISKS, "early-cancel-etag");
            let tracker = rename_fanout_barrier::observe_tasks(object);
            let barrier = rename_fanout_barrier::arm(object, 0, rename_fanout_barrier::PHASE_RENAME);

            let mut rename = Box::pin(SetDisks::rename_data(
                &disks,
                RUSTFS_META_TMP_BUCKET,
                "source",
                &file_infos,
                bucket,
                object,
                3,
            ));
            tokio::time::timeout(BARRIER_PAUSE_GUARD, async {
                tokio::select! {
                    () = barrier.wait_until_paused() => {}
                    result = rename.as_mut() => panic!("rename_data returned before the armed fan-out barrier: {result:?}"),
                }
            })
            .await
            .expect("paused disk must reach the armed rename barrier");

            drop(rename);
            barrier.release();
            tokio::time::timeout(BARRIER_PAUSE_GUARD, async {
                while tracker.running() != 0 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("background rename fan-out must drain after caller cancellation");

            for (idx, dir) in dirs.iter().enumerate() {
                let reopened = reopen_local_disk(dir).await;
                let stored = reopened
                    .read_version("", bucket, object, "", &ReadOptions::default())
                    .await
                    .unwrap_or_else(|err| panic!("disk {idx} must contain the cancelled early-ACK commit after drain: {err:?}"));
                assert_eq!(
                    stored.metadata.get("etag").map(String::as_str),
                    Some("early-cancel-etag"),
                    "disk {idx} must expose the background-drained commit after caller cancellation"
                );
            }
        })
        .await;
    }

    #[tokio::test]
    #[serial_test::serial(rename_quorum_ack)]
    async fn rename_data_early_ack_strict_quorum_failure_rolls_back_fresh_after_reopen() {
        temp_env::async_with_vars([(ENV_RUSTFS_PUT_RENAME_EARLY_ACK_ENABLE, Some("true"))], async {
            const DISKS: usize = 4;
            let bucket = "rename-early-ack-strict-rollback-bucket";
            let object = "rename-early-ack-strict-rollback-object";
            let (dirs, disks) = call_counter_local_disks(bucket, DISKS).await;
            prepare_rename_source_dirs(&dirs, &disks, "source").await;
            let file_infos = rename_commit_fileinfos(object, DISKS, "early-strict-rollback-etag");
            let _fault = rename_fault_injection::fail_rename_on(object, &[0]);

            SetDisks::rename_data(&disks, RUSTFS_META_TMP_BUCKET, "source", &file_infos, bucket, object, 4)
                .await
                .expect_err("three successful disks must fail an early-ACK strict write quorum of four");

            for (idx, dir) in dirs.iter().enumerate() {
                let reopened = reopen_local_disk(dir).await;
                let read = reopened.read_version("", bucket, object, "", &ReadOptions::default()).await;
                assert!(
                    matches!(read, Err(DiskError::FileNotFound | DiskError::FileVersionNotFound)),
                    "disk {idx} must not expose a fresh object after early-ACK strict rollback and reopen: {read:?}"
                );
            }
        })
        .await;
    }

    #[tokio::test]
    #[serial_test::serial(rename_quorum_ack)]
    async fn rename_data_commits_fresh_object_when_tail_disk_fails_after_write_quorum() {
        const DISKS: usize = 4;
        let bucket = "rename-tail-failure-fresh-bucket";
        let object = "rename-tail-failure-fresh-object";
        let (dirs, disks) = call_counter_local_disks(bucket, DISKS).await;
        prepare_rename_source_dirs(&dirs, &disks, "source").await;
        let mut file_infos = rename_commit_fileinfos(object, DISKS, "fresh-etag");
        file_infos[3] = FileInfo::default();

        SetDisks::rename_data(&disks, RUSTFS_META_TMP_BUCKET, "source", &file_infos, bucket, object, 3)
            .await
            .expect("three successful disks must satisfy write quorum despite one tail failure");

        for (idx, dir) in dirs.iter().enumerate() {
            let reopened = reopen_local_disk(dir).await;
            let read = reopened.read_version("", bucket, object, "", &ReadOptions::default()).await;
            if idx < 3 {
                read.unwrap_or_else(|err| panic!("quorum disk {idx} must contain the fresh commit after reopen: {err:?}"));
            } else {
                assert!(
                    matches!(read, Err(DiskError::FileNotFound | DiskError::FileVersionNotFound)),
                    "failed tail disk must not expose a partial fresh commit after reopen: {read:?}"
                );
            }
        }
    }

    #[tokio::test]
    #[serial_test::serial(rename_quorum_ack)]
    async fn rename_data_overwrite_tail_failure_preserves_old_tail_version_after_reopen() {
        const DISKS: usize = 4;
        let bucket = "rename-tail-failure-overwrite-bucket";
        let object = "rename-tail-failure-overwrite-object";
        let (dirs, disks) = call_counter_local_disks(bucket, DISKS).await;
        prepare_rename_source_dirs(&dirs, &disks, "source").await;
        let mut old = metadata_test_fileinfo(object);
        old.mod_time = Some(OffsetDateTime::now_utc());
        old.data = Some(Bytes::from_static(b"old-inline-body"));
        old.metadata.insert("etag".to_string(), "old-etag".to_string());
        for disk in disks.iter().flatten() {
            disk.write_metadata(bucket, bucket, object, old.clone())
                .await
                .expect("old metadata should be written before overwrite");
        }

        let mut file_infos = rename_commit_fileinfos(object, DISKS, "new-etag");
        file_infos[3] = FileInfo::default();

        SetDisks::rename_data(&disks, RUSTFS_META_TMP_BUCKET, "source", &file_infos, bucket, object, 3)
            .await
            .expect("three successful disks must satisfy overwrite quorum despite one tail failure");

        for (idx, dir) in dirs.iter().enumerate() {
            let reopened = reopen_local_disk(dir).await;
            let stored = reopened
                .read_version("", bucket, object, "", &ReadOptions::default())
                .await
                .unwrap_or_else(|err| panic!("disk {idx} must have a readable version after reopen: {err:?}"));
            let expected_etag = if idx < 3 { "new-etag" } else { "old-etag" };
            assert_eq!(
                stored.metadata.get("etag").map(String::as_str),
                Some(expected_etag),
                "disk {idx} must keep the correct overwrite visibility after reopen"
            );
        }
    }

    #[tokio::test]
    #[serial_test::serial(rename_quorum_ack)]
    async fn rename_data_strict_quorum_failure_rolls_back_fresh_object_after_reopen() {
        let _mode = durability_mode_override::set(DurabilityMode::Strict);
        const DISKS: usize = 4;
        let bucket = "rename-strict-rollback-fresh-bucket";
        let object = "rename-strict-rollback-fresh-object";
        let (dirs, disks) = call_counter_local_disks(bucket, DISKS).await;
        prepare_rename_source_dirs(&dirs, &disks, "source").await;
        let mut file_infos = rename_commit_fileinfos(object, DISKS, "fresh-rollback-etag");
        file_infos[3] = FileInfo::default();

        SetDisks::rename_data(&disks, RUSTFS_META_TMP_BUCKET, "source", &file_infos, bucket, object, 4)
            .await
            .expect_err("three successful disks must fail a strict write quorum of four");

        for (idx, dir) in dirs.iter().enumerate() {
            let reopened = reopen_local_disk(dir).await;
            let read = reopened.read_version("", bucket, object, "", &ReadOptions::default()).await;
            assert!(
                matches!(read, Err(DiskError::FileNotFound | DiskError::FileVersionNotFound)),
                "disk {idx} must not expose a fresh object after strict quorum rollback and reopen: {read:?}"
            );
        }
    }

    #[tokio::test]
    #[serial_test::serial(rename_quorum_ack)]
    async fn rename_data_strict_quorum_failure_restores_overwrite_after_reopen() {
        let _mode = durability_mode_override::set(DurabilityMode::Strict);
        const DISKS: usize = 4;
        let bucket = "rename-strict-rollback-overwrite-bucket";
        let object = "rename-strict-rollback-overwrite-object";
        let (dirs, disks) = call_counter_local_disks(bucket, DISKS).await;
        prepare_rename_source_dirs(&dirs, &disks, "source").await;
        let mut old = metadata_test_fileinfo(object);
        old.mod_time = Some(OffsetDateTime::now_utc());
        old.data = Some(Bytes::from_static(b"old-inline-body"));
        old.metadata.insert("etag".to_string(), "old-rollback-etag".to_string());
        for disk in disks.iter().flatten() {
            disk.write_metadata(bucket, bucket, object, old.clone())
                .await
                .expect("old metadata should be written before overwrite rollback test");
        }

        let mut file_infos = rename_commit_fileinfos(object, DISKS, "new-rollback-etag");
        file_infos[3] = FileInfo::default();

        SetDisks::rename_data(&disks, RUSTFS_META_TMP_BUCKET, "source", &file_infos, bucket, object, 4)
            .await
            .expect_err("three successful disks must fail a strict overwrite quorum of four");

        for (idx, dir) in dirs.iter().enumerate() {
            let reopened = reopen_local_disk(dir).await;
            let stored = reopened
                .read_version("", bucket, object, "", &ReadOptions::default())
                .await
                .unwrap_or_else(|err| panic!("disk {idx} must keep old metadata after strict rollback: {err:?}"));
            assert_eq!(
                stored.metadata.get("etag").map(String::as_str),
                Some("old-rollback-etag"),
                "disk {idx} must restore the old overwrite target after strict rollback and reopen"
            );
        }
    }

    /// Demo / regression guard for the barrier on the commit (old-data-dir)
    /// cleanup fan-out. Serves the same #1312/#1319 "no background disk write
    /// after release" shape, on the reclamation path that runs *after* a write is
    /// ACKed — the classic detached background delete #1312 fences against.
    ///
    /// It stages a real `object/<old_data_dir>` on two disks and drives the real
    /// `commit_rename_data_dir` fan-out, pausing the first disk's cleanup delete.
    #[tokio::test]
    async fn commit_cleanup_fanout_barrier_pauses_background_deletes() {
        let bucket = "cleanup-barrier-bucket";
        let object = "cleanup-barrier-object";
        let old_data_dir = "11111111-1111-1111-1111-111111111111";
        let committed_data_dir = "22222222-2222-2222-2222-222222222222";
        let path = format!("{object}/{old_data_dir}/part.1");
        let (_dir1, disk1) = read_multiple_test_disk(bucket, &[(&path, b"one".as_slice())]).await;
        let (_dir2, disk2) = read_multiple_test_disk(bucket, &[(&path, b"two".as_slice())]).await;
        let set = io_primitives_test_set(vec![Some(disk1.clone()), Some(disk2.clone())], 1).await;
        let disks = [Some(disk1.clone()), Some(disk2.clone())];

        let tracker = rename_fanout_barrier::observe_tasks(object);
        let barrier = rename_fanout_barrier::arm(object, 0, rename_fanout_barrier::PHASE_CLEANUP);

        let cleanup_fut = set.commit_rename_data_dir(&disks, bucket, object, old_data_dir, committed_data_dir, 2);
        let control_fut = async {
            tokio::time::timeout(BARRIER_PAUSE_GUARD, barrier.wait_until_paused())
                .await
                .expect("cleanup fan-out must reach the armed cleanup barrier");
            assert!(tracker.running() >= 1, "a background cleanup delete must still be in flight while paused");
            barrier.release();
        };
        let (cleanup, ()) = tokio::join!(cleanup_fut, control_fut);

        assert_eq!(tracker.running(), 0, "no background cleanup task may remain after drain");
        assert_eq!(cleanup.attempted, 2);
        assert_eq!(cleanup.reclaimed, 2, "the real old-data-dir must still be reclaimed after release");

        drop((disk1, disk2));
    }

    #[tokio::test]
    async fn commit_cleanup_reports_and_releases_deferred_snapshot_data_dirs() {
        let bucket = "cleanup-lease-bucket";
        let object = "cleanup-lease-object";
        let old_data_dir = "11111111-1111-1111-1111-111111111111";
        let committed_data_dir = "22222222-2222-2222-2222-222222222222";
        let data_dir_path = format!("{object}/{old_data_dir}");
        let shard_path = format!("{data_dir_path}/part.1");
        let (_dir1, disk1) = read_multiple_test_disk(bucket, &[(&shard_path, b"one".as_slice())]).await;
        let set = io_primitives_test_set(vec![Some(disk1.clone())], 0).await;
        let lease = disk1
            .acquire_snapshot_lease(bucket, &data_dir_path)
            .await
            .expect("snapshot lease should be acquired before cleanup");

        let cleanup = set
            .commit_rename_data_dir(&[Some(disk1.clone())], bucket, object, old_data_dir, committed_data_dir, 1)
            .await;
        assert_eq!(cleanup.attempted, 1);
        assert_eq!(cleanup.reclaimed, 0);
        assert_eq!(cleanup.deferred, 1);
        assert!(cleanup.unreclaimed_disks.is_empty());
        disk1
            .read_all(bucket, &shard_path)
            .await
            .expect("deferred cleanup must leave later shard opens available");

        disk1
            .release_snapshot_lease(bucket, &data_dir_path, lease)
            .await
            .expect("final lease release should reclaim the old data directory");
        assert!(matches!(disk1.read_all(bucket, &shard_path).await, Err(DiskError::FileNotFound)));
    }

    /// Isolation guard: an armed barrier / observed object only affects its own
    /// object. A fan-out for a different (unobserved, unarmed) object must not be
    /// paused and must not accrue any tracked task count — so concurrent tests
    /// using distinct object names never interfere.
    #[tokio::test]
    async fn barrier_and_task_tracker_isolate_by_object() {
        const DISKS: usize = 3;
        let bucket = "barrier-iso-bucket";
        let observed_object = "iso-observed-object";
        let other_object = "iso-other-object";
        let (dirs, disks) = call_counter_local_disks(bucket, DISKS).await;

        // Observe + arm one object, then run the fan-out for a *different* object.
        let tracker = rename_fanout_barrier::observe_tasks(observed_object);
        let _barrier = rename_fanout_barrier::arm(observed_object, 0, rename_fanout_barrier::PHASE_RENAME);

        let file_infos = rename_barrier_fileinfos(other_object, DISKS);
        // This must run to completion without ever pausing (no barrier for it) and
        // must not touch the observed object's counter. A hang here (e.g. if the
        // barrier ignored the object key) would surface as a timeout.
        let _ = tokio::time::timeout(
            BARRIER_PAUSE_GUARD,
            SetDisks::rename_data(&disks, bucket, other_object, &file_infos, bucket, other_object, DISKS - 1),
        )
        .await
        .expect("fan-out for an unarmed object must not pause");

        assert_eq!(
            tracker.running(),
            0,
            "another object's fan-out must not accrue tracked tasks for the observed object"
        );

        drop(dirs);
    }

    #[tokio::test]
    async fn multipart_codec_streaming_reader_zero_buffer_is_noop() {
        let reader = tokio::io::BufReader::new(Cursor::new(b"payload".to_vec()));
        let mut reader = MultipartCodecStreamingReader::new(vec![Box::new(reader)]);
        let mut out = [];

        let read = reader
            .read(&mut out)
            .await
            .expect("zero-length read should not poll inner readers");

        assert_eq!(read, 0);
        assert_eq!(reader.readers.len(), 1);
    }

    #[test]
    fn metadata_fanout_observation_classifies_invalid_and_ignored_results() {
        let invalid = MetadataFanoutObservation::from_file_info(&FileInfo::default(), Duration::from_millis(7));
        assert_eq!(invalid.outcome, GET_METADATA_RESPONSE_ERROR);
        assert!(!invalid.valid);
        assert!(!invalid.ignored);

        let ignored = MetadataFanoutObservation::from_error(&DiskError::DiskNotFound, Duration::from_millis(9));
        assert_eq!(ignored.outcome, GET_METADATA_RESPONSE_DISK_NOT_FOUND);
        assert!(!ignored.valid);
        assert!(ignored.ignored);

        let corrupt = MetadataFanoutObservation::from_error(&DiskError::FileCorrupt, Duration::from_millis(11));
        assert_eq!(corrupt.outcome, GET_METADATA_RESPONSE_CORRUPT);
        assert!(!corrupt.ignored);
    }

    #[test]
    fn metadata_fanout_diagnostics_reports_counts_and_latency_edges() {
        let diagnostics = MetadataFanoutDiagnostics::new(
            Duration::from_millis(40),
            vec![
                MetadataFanoutObservation {
                    outcome: GET_METADATA_RESPONSE_VALID,
                    elapsed: Duration::from_millis(30),
                    valid: true,
                    ignored: false,
                },
                MetadataFanoutObservation {
                    outcome: GET_METADATA_RESPONSE_IGNORED,
                    elapsed: Duration::from_millis(10),
                    valid: false,
                    ignored: true,
                },
                MetadataFanoutObservation {
                    outcome: GET_METADATA_RESPONSE_ERROR,
                    elapsed: Duration::from_millis(20),
                    valid: false,
                    ignored: false,
                },
            ],
        );

        assert_eq!(diagnostics.total_responses(), 3);
        assert_eq!(diagnostics.valid_responses(), 1);
        assert_eq!(diagnostics.ignored_responses(), 1);
        assert_eq!(diagnostics.non_valid_responses(), 2);
        assert_eq!(diagnostics.first_response_latency(), Some(Duration::from_millis(10)));
        assert_eq!(diagnostics.first_valid_response_latency(), Some(Duration::from_millis(30)));
        assert_eq!(diagnostics.slowest_response_latency(), Some(Duration::from_millis(30)));
        assert_eq!(diagnostics.quorum_candidate_latency(0), Some(Duration::ZERO));
        assert_eq!(diagnostics.quorum_candidate_latency(1), Some(Duration::from_millis(30)));
        assert_eq!(diagnostics.quorum_candidate_latency(2), None);
    }

    #[test]
    fn metadata_quorum_accumulator_counts_invalid_metadata_and_ignored_errors() {
        let mut accumulator = MetadataQuorumAccumulator::new(4, 2, true);

        accumulator.observe_file_info(&FileInfo::default());
        accumulator.observe_error(&DiskError::DiskNotFound);

        assert_eq!(accumulator.hard_errors, 1);
        assert_eq!(accumulator.ignored_errors, 1);
        assert_eq!(accumulator.final_miss_reason(), GET_METADATA_EARLY_STOP_REASON_ERROR);
    }

    #[test]
    fn metadata_quorum_accumulator_early_stops_on_one_delete_marker_majority() {
        let marker = metadata_test_delete_marker("object", Uuid::new_v4(), OffsetDateTime::now_utc());
        let mut accumulator = MetadataQuorumAccumulator::new(6, 3, true);

        for _ in 0..4 {
            accumulator.observe_file_info(&marker);
        }

        assert_eq!(accumulator.default_write_quorum(), 4);
        assert_eq!(accumulator.delete_marker_votes, 4);
        assert_eq!(
            accumulator.early_stop_decision(),
            Some(MetadataEarlyStopDecision {
                reason: GET_METADATA_EARLY_STOP_REASON_DELETE_MARKER,
            })
        );
    }

    #[test]
    fn metadata_quorum_accumulator_does_not_combine_distinct_delete_markers() {
        let now = OffsetDateTime::now_utc();
        let first = metadata_test_delete_marker("object", Uuid::new_v4(), now);
        let second = metadata_test_delete_marker("object", Uuid::new_v4(), now + time::Duration::seconds(1));
        let third = metadata_test_delete_marker("object", Uuid::new_v4(), now + time::Duration::seconds(2));
        let mut accumulator = MetadataQuorumAccumulator::new(4, 2, true);

        accumulator.observe_file_info(&first);
        accumulator.observe_file_info(&second);
        accumulator.observe_file_info(&third);

        assert_eq!(accumulator.delete_marker_votes, 1);
        assert_eq!(accumulator.delete_marker_candidates.len(), 3);
        assert_eq!(accumulator.early_stop_decision(), None);

        accumulator.observe_file_info(&second);
        accumulator.observe_file_info(&second);
        assert_eq!(accumulator.delete_marker_votes, 3);
        assert!(accumulator.early_stop_decision().is_some());
    }

    #[test]
    fn metadata_quorum_accumulator_candidate_latest_quorum_handles_zero_parity_and_invalid_candidates() {
        let accumulator = MetadataQuorumAccumulator::new(4, 0, true);
        let candidate = metadata_test_fileinfo("object");
        assert_eq!(accumulator.candidate_latest_quorum(&candidate), Some(4));
        assert_eq!(accumulator.missing_response_quorum(), 4);

        let accumulator = MetadataQuorumAccumulator::new(4, 2, true);
        let mut deleted = candidate.clone();
        deleted.deleted = true;
        assert_eq!(accumulator.candidate_latest_quorum(&deleted), Some(3));

        let marker = metadata_test_delete_marker("object", Uuid::new_v4(), OffsetDateTime::now_utc());
        assert_eq!(accumulator.candidate_latest_quorum(&marker), None);

        let mut empty = candidate.clone();
        empty.size = 0;
        assert_eq!(accumulator.candidate_latest_quorum(&empty), None);

        let mut impossible_parity = candidate;
        impossible_parity.erasure.parity_blocks = 4;
        assert_eq!(accumulator.candidate_latest_quorum(&impossible_parity), None);
    }

    #[test]
    fn metadata_quorum_accumulator_treats_invalid_default_parity_as_full_fanout() {
        let accumulator = MetadataQuorumAccumulator::new(2, 2, true);

        assert_eq!(accumulator.default_write_quorum(), 2);
        assert_eq!(accumulator.missing_response_quorum(), 2);
        assert!(accumulator.can_still_reach_early_stop_with_pending(2));
        assert!(!accumulator.can_still_reach_early_stop_with_pending(1));
    }

    #[test]
    fn confirmed_missing_part_error_recognizes_legacy_and_s3_markers() {
        assert!(!is_confirmed_missing_part_error(None));
        assert!(is_confirmed_missing_part_error(Some("file not found")));
        assert!(is_confirmed_missing_part_error(Some("No such file or directory")));
        assert!(is_confirmed_missing_part_error(Some("Specified part could not be found")));
        assert!(is_confirmed_missing_part_error(Some("part.7 not found")));
        assert!(!is_confirmed_missing_part_error(Some("part.7 missing")));
        assert!(!is_confirmed_missing_part_error(Some("permission denied")));
    }

    #[test]
    fn resolve_read_part_handles_mismatched_and_transient_responses_without_false_missing() {
        let responses = vec![
            Some(Vec::new()),
            Some(vec![read_part_test_error(1, "permission denied")]),
            None,
        ];

        let err = resolve_read_part_from_responses("bucket", "upload/part.1.meta", 1, 0, 1, &responses, 2)
            .expect_err("mismatched and transient responses must not be treated as confirmed missing");

        assert_eq!(err, DiskError::ErasureReadQuorum);
    }

    #[test]
    fn resolve_read_part_accepts_alternate_missing_error_markers() {
        let responses = vec![
            Some(vec![read_part_test_error(1, "Specified part could not be found")]),
            Some(vec![read_part_test_error(1, "part.1 not found")]),
            Some(vec![read_part_test_part(1, "stale-etag")]),
        ];

        let part = resolve_read_part_from_responses("bucket", "upload/part.1.meta", 1, 0, 1, &responses, 2)
            .expect("alternate missing markers should satisfy missing quorum");

        assert_eq!(part.number, 1);
        assert_eq!(part.error.as_deref(), Some("part.1 not found"));
        assert!(part.etag.is_empty());
    }

    #[test]
    fn resolve_read_part_requires_layout_fields_to_reach_quorum() {
        let mut valid = read_part_test_part(1, "winner");
        valid.size = 100;
        valid.actual_size = 90;
        let mut wrong_etag = valid.clone();
        wrong_etag.etag = "loser".to_string();
        let mut wrong_number = valid.clone();
        wrong_number.number = 2;
        let mut wrong_size = valid.clone();
        wrong_size.size = 50;
        let mut wrong_actual_size = valid.clone();
        wrong_actual_size.actual_size = 40;

        for (field, corrupted) in [
            ("etag", wrong_etag),
            ("number", wrong_number),
            ("size", wrong_size),
            ("actual_size", wrong_actual_size),
        ] {
            let responses = vec![Some(vec![corrupted]), Some(vec![valid.clone()]), Some(vec![valid.clone()])];
            let part = resolve_read_part_from_responses("bucket", "upload/part.1.meta", 1, 0, 1, &responses, 2)
                .unwrap_or_else(|err| panic!("{field} mismatch must not defeat layout quorum: {err}"));

            assert_eq!(part.etag, "winner", "{field}");
            assert_eq!(part.number, 1, "{field}");
            assert_eq!(part.size, 100, "{field}");
            assert_eq!(part.actual_size, 90, "{field}");
        }
    }

    #[test]
    fn resolve_read_part_diagnostic_branch_keeps_read_quorum_error() {
        temp_env::with_var("RUSTFS_ISSUE3031_DIAG_ENABLE", Some("true"), || {
            let responses = vec![
                Some(Vec::new()),
                None,
                Some(vec![read_part_test_error(1, "permission denied")]),
            ];

            let err = resolve_read_part_from_responses("bucket", "upload/part.1.meta", 1, 0, 1, &responses, 2)
                .expect_err("diagnostic logging must not change the read-quorum result");

            assert_eq!(err, DiskError::ErasureReadQuorum);
        });
    }

    // Runs under a Tokio runtime like the sibling reservation tests:
    // shard_read_costs_for_disks consults process-global topology state
    // (local_endpoint_hosts_for_shard_costs), whose fast-lock manager lazily
    // spawns a background cleanup task on first access. As a plain sync #[test]
    // this panicked with a TryCurrentError whenever it was the first test in a
    // process to touch that global (e.g. under nextest's per-test isolation),
    // making it order-dependent flaky in CI.
    #[tokio::test]
    async fn shard_read_costs_for_empty_disk_set_are_empty() {
        assert!(shard_read_costs_for_disks(&[]).is_empty());
    }

    #[test]
    fn shard_read_cost_for_endpoint_and_missing_disk_cover_all_cost_classes() {
        let same_node_hosts = vec!["node-a:9000".to_string()];

        assert_eq!(shard_read_cost_for_disk(None, &same_node_hosts), ShardReadCost::Unknown);
        assert_eq!(shard_read_cost_for_endpoint(true, "", &same_node_hosts), ShardReadCost::Local);
        assert_eq!(
            shard_read_cost_for_endpoint(false, "node-a:9000", &same_node_hosts),
            ShardReadCost::SameNode
        );
        assert_eq!(
            shard_read_cost_for_endpoint(false, "node-b:9000", &same_node_hosts),
            ShardReadCost::Remote
        );
        assert!(local_endpoint_hosts_for_shard_costs().is_empty());
    }

    #[test]
    #[serial_test::serial]
    fn bitrot_reader_setup_tracks_strategy_counters_and_deferred_readers() {
        temp_env::with_var(ENV_RUSTFS_GET_DATA_BLOCKS_FIRST_READER_SETUP, None::<&str>, || {
            assert!(matches!(
                get_bitrot_reader_setup_strategy(BitrotReaderSetupMode::ReadQuorum, false),
                BitrotReaderSetupStrategy::DataBlocksFirst
            ));
        });
        temp_env::with_var(ENV_RUSTFS_GET_DATA_BLOCKS_FIRST_READER_SETUP, Some("false"), || {
            assert!(matches!(
                get_bitrot_reader_setup_strategy(BitrotReaderSetupMode::ReadQuorum, false),
                BitrotReaderSetupStrategy::AllShards
            ));
        });
        temp_env::with_var(ENV_RUSTFS_GET_DATA_BLOCKS_FIRST_READER_SETUP, Some("true"), || {
            assert!(matches!(
                get_bitrot_reader_setup_strategy(BitrotReaderSetupMode::ReadQuorum, false),
                BitrotReaderSetupStrategy::DataBlocksFirst
            ));
        });
        temp_env::with_var(ENV_RUSTFS_GET_CODEC_STREAMING_DATA_BLOCKS_FIRST_READER_SETUP, Some("true"), || {
            assert!(matches!(
                get_bitrot_reader_setup_strategy(BitrotReaderSetupMode::VerifyReconstruction, false),
                BitrotReaderSetupStrategy::DataBlocksFirst
            ));
        });
        assert_eq!(BitrotReaderSetupMode::ReadQuorum.as_str(), "read_quorum");
        assert_eq!(BitrotReaderSetupMode::VerifyReconstruction.as_str(), "verify_reconstruction");
        assert_eq!(BitrotReaderSetupStrategy::AllShards.as_str(), "all_shards");
        assert_eq!(BitrotReaderSetupStrategy::DataBlocksFirst.as_str(), "data_blocks_first");
        assert_eq!(BitrotReaderSetupStrategy::DataBlocksOnly.as_str(), "data_blocks_only");

        let mut setup = BitrotReaderSetup::new(4);
        assert_eq!(setup.scheduled_shards(), 0);
        assert!(setup.mark_scheduled(0));
        assert!(!setup.mark_scheduled(0));
        assert_eq!(setup.scheduled_shards(), 1);
        assert_eq!(setup.pending_scheduled_shards(), 1);
        assert_eq!(setup.available_shards(), 0);
        assert_eq!(setup.completed_failed_shards(), 0);
        assert_eq!(setup.reconstruction_verification_target(3, 2), 3);
        assert!(!setup.has_setup_quorum(3, 2, BitrotReaderSetupMode::ReadQuorum));
        assert!(!setup.data_shards_attempted(3));
        assert_eq!(setup.scheduling_target(3, 2, BitrotReaderSetupMode::VerifyReconstruction), 3);

        setup.apply_reader_result(0, Ok(Some(test_object_bitrot_reader())));
        setup.apply_reader_result(1, Ok(None));
        setup.apply_reader_result(2, Err(DiskError::FileCorrupt));

        assert_eq!(setup.attempted_shards(), 3);
        assert_eq!(setup.pending_scheduled_shards(), 0);
        assert_eq!(setup.available_shards(), 1);
        assert_eq!(setup.available_data_shards(3), 1);
        assert_eq!(setup.completed_failed_shards(), 2);
        assert!(setup.data_shards_attempted(3));
        assert_eq!(setup.reconstruction_verification_target(3, 2), 3);
        assert_eq!(setup.setup_target(3, 2, BitrotReaderSetupMode::VerifyReconstruction), 3);
        assert_eq!(setup.scheduling_target(3, 2, BitrotReaderSetupMode::VerifyReconstruction), 3);

        let mut verification_setup = BitrotReaderSetup::new(4);
        verification_setup.apply_reader_result(0, Ok(Some(test_object_bitrot_reader())));
        verification_setup.apply_reader_result(1, Ok(Some(test_object_bitrot_reader())));
        verification_setup.apply_reader_result(2, Err(DiskError::FileCorrupt));
        verification_setup.apply_reader_result(3, Ok(Some(test_object_bitrot_reader())));
        assert_eq!(verification_setup.reconstruction_verification_target(3, 2), 4);
        assert_eq!(verification_setup.setup_target(3, 2, BitrotReaderSetupMode::VerifyReconstruction), 4);
        assert!(verification_setup.has_setup_quorum(3, 2, BitrotReaderSetupMode::ReadQuorum));

        let (deferred_reader, stripe_handle) = test_deferred_object_bitrot_reader();
        setup.retain_deferred_reader(3, deferred_reader, stripe_handle);
        assert_eq!(setup.deferred_shards(), 1);
        assert!(setup.readers[3].is_some());
        assert!(setup.errors[3].is_none());
    }

    #[tokio::test]
    async fn write_unique_file_info_reverts_metadata_when_write_quorum_fails() {
        let bucket = "write-unique-bucket";
        let object = "object";
        let (_dir, disk) = read_multiple_test_disk(bucket, &[]).await;
        let files = vec![metadata_test_fileinfo(object), metadata_test_fileinfo(object)];

        let result = SetDisks::write_unique_file_info(&[Some(disk.clone()), None], bucket, bucket, object, &files, 2).await;

        assert!(result.is_err(), "missing disk must prevent the requested write quorum");
        assert!(
            matches!(
                disk.read_all(bucket, &path_join_buf(&[object, STORAGE_FORMAT_FILE])).await,
                Err(DiskError::FileNotFound)
            ),
            "successful metadata write must be reverted when quorum is not reached"
        );
    }

    #[tokio::test]
    async fn update_object_meta_handles_empty_metadata_and_missing_quorum() {
        let set = io_primitives_test_set(vec![None, None], 1).await;
        let mut empty = metadata_test_fileinfo("object");
        empty.metadata.clear();

        set.update_object_meta("bucket", "object", empty, &[None, None])
            .await
            .expect("empty metadata update without replacement should be a no-op");

        let mut with_metadata = metadata_test_fileinfo("object");
        with_metadata
            .metadata
            .insert("x-amz-meta-test".to_string(), "value".to_string());
        let result = set.update_object_meta("bucket", "object", with_metadata, &[None, None]).await;

        assert!(result.is_err(), "missing disks must prevent metadata write quorum");
    }

    #[tokio::test]
    async fn load_file_info_versions_exact_returns_versions_from_read_quorum() {
        let bucket = "exact-versions-bucket";
        let object = "exact-object";
        let (_dir, disk) = read_multiple_test_disk(bucket, &[]).await;
        let mut fi = metadata_test_fileinfo(object);
        fi.version_id = Some(Uuid::new_v4());
        fi.mod_time = Some(OffsetDateTime::now_utc());
        disk.write_metadata(bucket, bucket, object, fi.clone())
            .await
            .expect("metadata should be written");
        let set = io_primitives_test_set(vec![Some(disk)], 0).await;

        let versions = set
            .load_file_info_versions_exact(bucket, object)
            .await
            .expect("exact version load should succeed")
            .expect("exact version load should find metadata");

        assert_eq!(versions.versions.len(), 1);
        assert_eq!(versions.versions[0].version_id, fi.version_id);
        assert_eq!(versions.versions[0].name, object);
    }

    #[tokio::test]
    async fn load_file_info_versions_exact_encodes_directory_key_but_returns_logical_name() {
        let bucket = "exact-directory-versions-bucket";
        let object = "prefix/directory/";
        let disk_object = rustfs_utils::path::encode_dir_object(object);
        let (_dir, disk) = read_multiple_test_disk(bucket, &[]).await;
        let mut fi = metadata_test_fileinfo(object);
        fi.version_id = Some(Uuid::new_v4());
        fi.mod_time = Some(OffsetDateTime::now_utc());
        disk.write_metadata(bucket, bucket, disk_object.as_str(), fi.clone())
            .await
            .expect("directory metadata should be written under the encoded key");
        let set = io_primitives_test_set(vec![Some(disk)], 0).await;

        let versions = set
            .load_file_info_versions_exact(bucket, object)
            .await
            .expect("exact directory version load should succeed")
            .expect("exact directory version load should find metadata");

        assert_eq!(versions.name, object);
        assert_eq!(versions.versions.len(), 1);
        assert_eq!(versions.versions[0].name, object);
        assert_eq!(versions.versions[0].version_id, fi.version_id);
    }

    #[tokio::test]
    async fn load_file_info_versions_exact_rejects_transitioned_duplicate_parts() {
        let bucket = "exact-versions-bucket";
        let object = "poisoned-transitioned-object";
        let (_dir, disk) = read_multiple_test_disk(bucket, &[]).await;
        let mut file_info = metadata_test_fileinfo(object);
        file_info.version_id = Some(Uuid::new_v4());
        file_info.mod_time = Some(OffsetDateTime::now_utc());
        file_info.transition_status = TRANSITION_COMPLETE.to_string();
        file_info.transitioned_objname = "remote/object".to_string();
        file_info.transition_tier = "WARM".to_string();
        file_info.parts.push(file_info.parts[0].clone());
        write_raw_file_info_unchecked(&disk, bucket, object, file_info).await;
        let set = io_primitives_test_set(vec![Some(disk)], 0).await;

        let err = set
            .load_file_info_versions_exact(bucket, object)
            .await
            .expect_err("exact loader must reject metadata that would poison decommission");

        assert!(err.to_string().contains("validation failed"), "unexpected error: {err}");
    }

    #[tokio::test]
    async fn load_file_info_versions_exact_rejects_default_like_delete_marker() {
        let bucket = "exact-versions-bucket";
        let object = "forged-delete-marker";
        let (_dir, disk) = read_multiple_test_disk(bucket, &[]).await;
        let forged_version = rustfs_filemeta::FileMetaVersion {
            version_type: rustfs_filemeta::VersionType::Delete,
            delete_marker: Some(rustfs_filemeta::MetaDeleteMarker {
                version_id: Some(Uuid::new_v4()),
                mod_time: None,
                ..Default::default()
            }),
            write_version: 1,
            ..Default::default()
        };
        let mut forged_meta = FileMeta::new();
        forged_meta
            .versions
            .push(FileMetaShallowVersion::try_from(forged_version).expect("forged marker body should encode"));
        write_raw_file_meta_unchecked(&disk, bucket, object, forged_meta).await;
        let set = io_primitives_test_set(vec![Some(disk)], 0).await;

        let err = set
            .load_file_info_versions_exact(bucket, object)
            .await
            .expect_err("default-like delete marker must be rejected at the exact loader boundary");

        assert!(err.to_string().contains("exact object versions decode failed"), "unexpected error: {err}");
    }

    #[tokio::test]
    async fn commit_rename_data_dir_reclaims_old_data_dir_and_reports_receipt() {
        let bucket = "commit-rename-bucket";
        let object = "object";
        let old_data_dir = "11111111-1111-1111-1111-111111111111";
        let committed_data_dir = "22222222-2222-2222-2222-222222222222";
        let path = format!("{object}/{old_data_dir}/part.1");
        let (_dir1, disk1) = read_multiple_test_disk(bucket, &[(&path, b"one".as_slice())]).await;
        let (_dir2, disk2) = read_multiple_test_disk(bucket, &[(&path, b"two".as_slice())]).await;
        let set = io_primitives_test_set(vec![Some(disk1.clone()), Some(disk2.clone())], 1).await;

        let cleanup = set
            .commit_rename_data_dir(
                &[Some(disk1.clone()), Some(disk2.clone())],
                bucket,
                object,
                old_data_dir,
                committed_data_dir,
                2,
            )
            .await;
        assert_eq!(cleanup.attempted, 2);
        assert_eq!(cleanup.reclaimed, 2);
        assert!(!cleanup.has_residue());

        assert!(matches!(disk1.read_all(bucket, &path).await, Err(DiskError::FileNotFound)));
        assert!(matches!(disk2.read_all(bucket, &path).await, Err(DiskError::FileNotFound)));

        let missing = set
            .commit_rename_data_dir(&[None, None], bucket, object, old_data_dir, committed_data_dir, 1)
            .await;
        assert_eq!(missing.attempted, 0);
        assert_eq!(missing.reclaimed, 0);
        assert!(!missing.has_residue(), "missing disk slots must not be counted as cleanup residue");
        assert!(missing.below_quorum, "missing disk slots should remain visible in the quorum lens");
    }

    #[tokio::test]
    async fn delete_prefix_succeeds_when_present_disks_reach_quorum() {
        let bucket = "delete-prefix-bucket";
        let (_dir1, disk1) = read_multiple_test_disk(bucket, &[("prefix/object.txt", b"one".as_slice())]).await;
        let (_dir2, disk2) = read_multiple_test_disk(bucket, &[("prefix/object.txt", b"two".as_slice())]).await;
        let (_dir3, disk3) = read_multiple_test_disk(bucket, &[("prefix/object.txt", b"three".as_slice())]).await;
        let set = io_primitives_test_set(vec![Some(disk1.clone()), Some(disk2.clone()), Some(disk3.clone()), None], 2).await;

        set.delete_prefix(bucket, "prefix")
            .await
            .expect("three successful disks should meet a four-disk write quorum");

        for disk in [disk1, disk2, disk3] {
            assert!(matches!(disk.read_all(bucket, "prefix/object.txt").await, Err(DiskError::FileNotFound)));
        }
    }

    #[tokio::test]
    async fn delete_prefix_counts_confirmed_absence_toward_quorum() {
        let bucket = "delete-prefix-confirmed-absence";
        let (_dir1, disk1) = read_multiple_test_disk(bucket, &[("prefix/object.txt", b"one".as_slice())]).await;
        let (_dir2, disk2) = read_multiple_test_disk(bucket, &[("prefix/object.txt", b"two".as_slice())]).await;
        let (_dir3, disk3) = read_multiple_test_disk(bucket, &[]).await;
        let (_dir4, disk4) = read_multiple_test_disk(bucket, &[]).await;
        disk3
            .delete_volume(bucket, true)
            .await
            .expect("third disk bucket should be absent");
        disk4
            .delete_volume(bucket, true)
            .await
            .expect("fourth disk bucket should be absent");
        let set = io_primitives_test_set(vec![Some(disk1.clone()), Some(disk2.clone()), Some(disk3), Some(disk4)], 2).await;

        set.delete_prefix(bucket, "prefix")
            .await
            .expect("successful deletes and confirmed absence should jointly meet quorum");

        for disk in [disk1, disk2] {
            assert!(matches!(disk.read_all(bucket, "prefix/object.txt").await, Err(DiskError::FileNotFound)));
        }
    }

    #[test]
    fn delete_prefix_result_reduction_preserves_existing_volume_evidence() {
        assert_eq!(
            SetDisks::reduce_delete_prefix_results(
                vec![
                    Err(DiskError::FileNotFound),
                    Err(DiskError::FileNotFound),
                    Err(DiskError::FileNotFound),
                    Err(DiskError::DiskNotFound),
                ],
                3,
            ),
            Ok(())
        );
        assert_eq!(
            SetDisks::reduce_delete_prefix_results(
                vec![
                    Err(DiskError::FileNotFound),
                    Err(DiskError::VolumeNotFound),
                    Err(DiskError::VolumeNotFound),
                    Err(DiskError::VolumeNotFound),
                ],
                3,
            ),
            Ok(())
        );
        assert_eq!(
            SetDisks::reduce_delete_prefix_results(
                vec![
                    Ok(()),
                    Err(DiskError::VolumeNotFound),
                    Err(DiskError::VolumeNotFound),
                    Err(DiskError::VolumeNotFound),
                ],
                3,
            ),
            Ok(())
        );
        assert_eq!(
            SetDisks::reduce_delete_prefix_results(
                vec![
                    Err(DiskError::VolumeNotFound),
                    Err(DiskError::VolumeNotFound),
                    Err(DiskError::VolumeNotFound),
                    Err(DiskError::VolumeNotFound),
                ],
                3,
            ),
            Err(DiskError::VolumeNotFound)
        );
        assert_eq!(
            SetDisks::reduce_delete_prefix_results(
                vec![Ok(()), Ok(()), Err(DiskError::DiskNotFound), Err(DiskError::DiskNotFound)],
                3,
            ),
            Err(DiskError::ErasureWriteQuorum)
        );
        assert_eq!(
            SetDisks::reduce_delete_prefix_results(
                vec![
                    Ok(()),
                    Err(DiskError::FileAccessDenied),
                    Err(DiskError::FileAccessDenied),
                    Err(DiskError::FileAccessDenied),
                ],
                3,
            ),
            Err(DiskError::FileAccessDenied)
        );
    }

    #[tokio::test]
    async fn delete_prefix_fails_at_quorum_minus_one() {
        let bucket = "delete-prefix-quorum-minus-one";
        let (_dir1, disk1) = read_multiple_test_disk(bucket, &[("prefix/object.txt", b"one".as_slice())]).await;
        let (_dir2, disk2) = read_multiple_test_disk(bucket, &[("prefix/object.txt", b"two".as_slice())]).await;
        let set = io_primitives_test_set(vec![Some(disk1.clone()), Some(disk2.clone()), None, None], 2).await;

        let err = set
            .delete_prefix(bucket, "prefix")
            .await
            .expect_err("two successful disks must not meet a four-disk write quorum");

        assert_eq!(err, DiskError::ErasureWriteQuorum);
        for disk in [disk1, disk2] {
            assert!(matches!(disk.read_all(bucket, "prefix/object.txt").await, Err(DiskError::FileNotFound)));
        }
    }

    #[tokio::test]
    async fn delete_prefix_fails_when_all_disk_slots_are_missing() {
        let set = io_primitives_test_set(vec![None, None, None, None], 2).await;

        let err = set
            .delete_prefix("delete-prefix-offline", "prefix")
            .await
            .expect_err("an entirely offline set must not report a successful deletion");

        assert_eq!(err, DiskError::ErasureWriteQuorum);
    }

    #[tokio::test]
    async fn delete_if_dangling_respects_recent_write_grace_without_deleting_metadata() {
        let bucket = "dangling-grace-bucket";
        let object = "object";
        let (_dir, disk) = read_multiple_test_disk(bucket, &[]).await;
        let set = io_primitives_test_set(vec![Some(disk.clone()), None, None], 1).await;
        let mut fi = metadata_test_fileinfo(object);
        fi.mod_time = Some(OffsetDateTime::now_utc());
        disk.write_metadata(bucket, bucket, object, fi.clone())
            .await
            .expect("metadata should be written before dangling check");

        let err = set
            .delete_if_dangling(
                bucket,
                object,
                &[fi, FileInfo::default(), FileInfo::default()],
                &[None, Some(DiskError::FileNotFound), Some(DiskError::FileNotFound)],
                &HashMap::new(),
                ObjectOptions::default(),
            )
            .await
            .expect_err("recent dangling metadata must stay protected by grace");

        assert_eq!(err, DiskError::ErasureReadQuorum);
        disk.read_all(bucket, &path_join_buf(&[object, STORAGE_FORMAT_FILE]))
            .await
            .expect("metadata should remain during dangling grace");
    }

    #[tokio::test]
    async fn delete_if_dangling_returns_stale_metadata_when_all_slots_are_already_absent() {
        let bucket = "dangling-delete-bucket";
        let object = "object";
        let set = io_primitives_test_set(vec![None, None, None], 1).await;
        let mut fi = metadata_test_fileinfo(object);
        fi.mod_time = Some(OffsetDateTime::now_utc() - time::Duration::hours(2));

        let deleted = set
            .delete_if_dangling(
                bucket,
                object,
                &[fi.clone(), FileInfo::default(), FileInfo::default()],
                &[
                    Some(DiskError::FileNotFound),
                    Some(DiskError::FileNotFound),
                    Some(DiskError::FileNotFound),
                ],
                &HashMap::new(),
                ObjectOptions::default(),
            )
            .await
            .expect("stale dangling metadata should pass when every delete target was already absent");

        assert_eq!(deleted.name, object);
        assert_eq!(deleted.mod_time, fi.mod_time);
    }

    #[tokio::test]
    async fn delete_if_dangling_cleans_when_only_part_results_prove_invalid_metadata_is_dangling() {
        let bucket = "dangling-invalid-meta-bucket";
        let object = "object";
        let set = io_primitives_test_set(vec![None, None, None, None], 1).await;
        let mut data_errs_by_part = HashMap::new();
        data_errs_by_part.insert(
            1,
            vec![
                CHECK_PART_FILE_NOT_FOUND,
                CHECK_PART_FILE_NOT_FOUND,
                CHECK_PART_FILE_NOT_FOUND,
                CHECK_PART_DISK_NOT_FOUND,
            ],
        );

        let deleted = set
            .delete_if_dangling(
                bucket,
                object,
                &[
                    FileInfo::default(),
                    FileInfo::default(),
                    FileInfo::default(),
                    FileInfo::default(),
                ],
                &[
                    Some(DiskError::FileNotFound),
                    Some(DiskError::FileNotFound),
                    Some(DiskError::FileNotFound),
                    Some(DiskError::DiskNotFound),
                ],
                &data_errs_by_part,
                ObjectOptions::default(),
            )
            .await
            .expect("invalid metadata should be cleanable when part results prove the object is dangling");

        assert!(!deleted.is_valid());
    }

    #[tokio::test]
    async fn read_multiple_files_returns_quorum_data_and_fails_closed_for_partial_file() {
        let bucket = "read-multiple-bucket";
        let prefix = "prefix";
        let (_dir1, disk1) = read_multiple_test_disk(
            bucket,
            &[
                ("prefix/shared.txt", b"longer shared payload".as_slice()),
                ("prefix/partial.txt", b"only one disk".as_slice()),
            ],
        )
        .await;
        let (_dir2, disk2) = read_multiple_test_disk(bucket, &[("prefix/shared.txt", b"short".as_slice())]).await;
        let req = ReadMultipleReq {
            bucket: bucket.to_string(),
            prefix: prefix.to_string(),
            files: vec!["shared.txt".to_string(), "partial.txt".to_string()],
            max_size: 0,
            metadata_only: false,
            abort404: false,
            max_results: 0,
        };

        let responses = SetDisks::read_multiple_files(&[Some(disk1), Some(disk2)], req, 2).await;

        assert_eq!(responses.len(), 2);
        assert!(responses[0].exists);
        assert_eq!(responses[0].data, b"longer shared payload");
        assert!(!responses[1].exists);
        assert_eq!(responses[1].error, Error::ErasureReadQuorum.to_string());
    }

    #[tokio::test]
    async fn read_multiple_files_returns_read_quorum_error_when_no_disk_can_answer() {
        let req = ReadMultipleReq {
            bucket: "bucket".to_string(),
            prefix: "prefix".to_string(),
            files: vec!["missing.txt".to_string()],
            max_size: 0,
            metadata_only: false,
            abort404: false,
            max_results: 0,
        };

        let responses = SetDisks::read_multiple_files(&[None, None], req, 1).await;

        assert_eq!(responses.len(), 1);
        assert!(!responses[0].exists);
        assert_eq!(responses[0].error, Error::ErasureReadQuorum.to_string());
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn mrf_intent_is_filed_once_per_read_repair_reservation() {
        // Serial: owns the process-global MRF channel for this test binary
        // (same key as the other channel-owning tests above).
        let bucket = format!("mrf-intent-bucket-{}", Uuid::new_v4());
        let object = format!("object-{}", Uuid::new_v4());
        let mut receiver = rustfs_common::mrf_channel::init_mrf_channel().expect("first channel init in this binary");
        rustfs_common::mrf_channel::set_mrf_delivery_enabled(true);

        fn intent_submission<'a>(bucket: &'a str, object: &'a str) -> ReadRepairHealSubmission<'a> {
            ReadRepairHealSubmission {
                bucket,
                object,
                version_id: None,
                pool_index: 9,
                set_index: 9,
                part_number: Some(1),
                reason: "decode_error",
                mrf_intent: Some((rustfs_common::mrf_channel::MrfKind::DecodeFailure, None)),
            }
        }

        // First sighting wins the reservation: the journal intent is filed
        // synchronously before the admission task is spawned.
        submit_read_repair_heal_with_submitter(intent_submission(&bucket, &object), accepted_read_repair_submitter).await;
        let first = receiver.try_recv().expect("first sighting must file exactly one MRF intent");
        assert_eq!(*first.bucket, bucket);
        assert_eq!(*first.object, object);

        // Second sighting within the dedup TTL is a duplicate: no request, no
        // second journal record.
        submit_read_repair_heal_with_submitter(intent_submission(&bucket, &object), accepted_read_repair_submitter).await;
        assert!(receiver.try_recv().is_err(), "duplicate sighting must not file another MRF intent");
    }

    #[tokio::test]
    async fn reserve_read_repair_heal_dedupes_by_object_version_and_set() {
        let object = format!("object-{}", Uuid::new_v4());
        let key = reserve_read_repair_heal("bucket", &object, Some("version-1"), 1, 2)
            .await
            .expect("first read-repair reservation should be accepted");

        assert_eq!(key.version_id.as_deref(), Some("version-1"));
        assert!(
            reserve_read_repair_heal("bucket", &object, Some("version-1"), 1, 2)
                .await
                .is_none()
        );

        release_read_repair_heal_reservation(&key).await;
        let retry_key = reserve_read_repair_heal("bucket", &object, Some("version-1"), 1, 2)
            .await
            .expect("released read-repair reservation should allow retry");
        release_read_repair_heal_reservation(&retry_key).await;
    }

    #[test]
    fn record_read_repair_dedup_counts_each_reason_separately() {
        let recorder = crate::test_metrics::CapturingRecorder::default();
        metrics::with_local_recorder(&recorder, || {
            record_read_repair_dedup("duplicate");
            record_read_repair_dedup("duplicate");
            record_read_repair_dedup("policy_drop");
        });

        assert_eq!(
            recorder.counter_value("rustfs_heal_read_repair_dedup_total", &[("reason", "duplicate")]),
            2
        );
        assert_eq!(
            recorder.counter_value("rustfs_heal_read_repair_dedup_total", &[("reason", "policy_drop")]),
            1
        );
    }

    #[tokio::test]
    async fn reserve_read_repair_heal_prunes_oldest_entry_at_capacity() {
        let bucket = format!("bucket-{}", Uuid::new_v4());
        let first_object = format!("object-{}", Uuid::new_v4());
        let first_key = reserve_read_repair_heal(&bucket, &first_object, None, 1, 1)
            .await
            .expect("first reservation should be accepted");

        let mut keys = vec![first_key.clone()];
        for index in 0..(READ_REPAIR_HEAL_DEDUP_MAX_ENTRIES + 8) {
            let object = format!("object-{index}-{}", Uuid::new_v4());
            if let Some(key) = reserve_read_repair_heal(&bucket, &object, None, 1, 1).await {
                keys.push(key);
            }
        }

        let replaced_first = reserve_read_repair_heal(&bucket, &first_object, None, 1, 1).await;
        for key in keys {
            release_read_repair_heal_reservation(&key).await;
        }
        if let Some(key) = replaced_first.as_ref() {
            release_read_repair_heal_reservation(key).await;
        }

        assert!(
            replaced_first.is_some(),
            "capacity pruning should evict the oldest read-repair reservation"
        );
    }

    #[tokio::test]
    // Serialized so this never overlaps the blackbox corrupt-shard test, which
    // owns the global heal channel receiver: without the serial key this test
    // could submit into a live-but-not-yet-drained channel and time out below.
    #[serial_test::serial]
    async fn submit_read_repair_heal_wrapper_releases_reservation_when_channel_unavailable() {
        // Serialized against the blackbox heal-channel owner, the channel is in
        // one of two deterministic states here: never initialized (the default
        // submitter fails with "Heal channel not initialized") or initialized
        // with its receiver already dropped (the send fails immediately). Either
        // way the wrapper must release the dedup reservation it recorded before
        // spawning — the fail-closed release path.
        let object = format!("object-{}", Uuid::new_v4());
        submit_read_repair_heal("bucket", &object, None, 4, 5, Some(7), "test").await;

        for _ in 0..20 {
            if let Some(key) = reserve_read_repair_heal("bucket", &object, None, 4, 5).await {
                release_read_repair_heal_reservation(&key).await;
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        panic!("unserviced heal channel must release the wrapper's read-repair dedup reservation");
    }

    #[tokio::test]
    async fn submit_read_repair_heal_releases_reservation_after_submitter_failure() {
        let object = format!("object-{}", Uuid::new_v4());
        submit_read_repair_heal_with_submitter(
            ReadRepairHealSubmission {
                bucket: "bucket",
                object: &object,
                version_id: None,
                pool_index: 1,
                set_index: 2,
                part_number: Some(1),
                reason: "test",
                mrf_intent: None,
            },
            failed_read_repair_submitter,
        )
        .await;

        for _ in 0..20 {
            if let Some(key) = reserve_read_repair_heal("bucket", &object, None, 1, 2).await {
                release_read_repair_heal_reservation(&key).await;
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        panic!("failed read-repair submission should release its dedup reservation");
    }

    #[tokio::test]
    async fn submit_read_repair_heal_releases_reservation_after_not_admitted_response() {
        let object = format!("object-{}", Uuid::new_v4());
        submit_read_repair_heal_with_submitter(
            ReadRepairHealSubmission {
                bucket: "bucket",
                object: &object,
                version_id: Some("version-1"),
                pool_index: 2,
                set_index: 3,
                part_number: Some(2),
                reason: "test",
                mrf_intent: None,
            },
            dropped_read_repair_submitter,
        )
        .await;

        for _ in 0..20 {
            if let Some(key) = reserve_read_repair_heal("bucket", &object, Some("version-1"), 2, 3).await {
                release_read_repair_heal_reservation(&key).await;
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        panic!("not-admitted read-repair submission should release its dedup reservation");
    }

    #[tokio::test]
    async fn submit_read_repair_heal_keeps_admitted_reservation_deduped() {
        let object = format!("object-{}", Uuid::new_v4());
        submit_read_repair_heal_with_submitter(
            ReadRepairHealSubmission {
                bucket: "bucket",
                object: &object,
                version_id: None,
                pool_index: 3,
                set_index: 4,
                part_number: None,
                reason: "test",
                mrf_intent: None,
            },
            accepted_read_repair_submitter,
        )
        .await;

        assert!(reserve_read_repair_heal("bucket", &object, None, 3, 4).await.is_none());
        let key = ReadRepairHealCacheKey::new("bucket", &object, None, 3, 4);
        release_read_repair_heal_reservation(&key).await;
    }

    // ========================================================================
    // backlog#898 — groups A/C: pure old-data-dir cleanup classification.
    // ========================================================================
    use crate::disk::error_reduce::is_ignored_err;

    // A1: all deletes succeed => full reclaim, no residue, not below quorum.
    #[test]
    fn cleanup_receipt_all_deletes_succeed_marks_full_reclaim() {
        let errs = vec![None, None, None];
        let attempted = vec![true, true, true];
        let r = classify_old_data_dir_cleanup(&errs, &attempted, 2);
        assert_eq!(r.attempted, 3);
        assert_eq!(r.reclaimed, 3);
        assert!(r.unreclaimed_disks.is_empty());
        assert!(!r.below_quorum);
        assert!(!r.has_residue());
    }

    // rustfs/backlog#1009: quorum reduction of the per-disk old-current-size
    // observations returned by rename_data.
    mod reduce_common_old_current_size {
        use super::*;

        #[test]
        fn agreement_at_quorum_wins() {
            let observations = vec![
                Some(OldCurrentSize::Present(5)),
                Some(OldCurrentSize::Present(5)),
                Some(OldCurrentSize::Present(5)),
                Some(OldCurrentSize::Absent),
            ];
            assert_eq!(
                SetDisks::reduce_common_old_current_size(&observations, 3),
                Some(OldCurrentSize::Present(5))
            );
        }

        #[test]
        fn absent_is_a_definite_vote() {
            let observations = vec![
                Some(OldCurrentSize::Absent),
                Some(OldCurrentSize::Absent),
                Some(OldCurrentSize::Absent),
                None,
            ];
            assert_eq!(SetDisks::reduce_common_old_current_size(&observations, 3), Some(OldCurrentSize::Absent));
        }

        #[test]
        fn divergence_below_quorum_is_unknown() {
            let observations = vec![
                Some(OldCurrentSize::Present(5)),
                Some(OldCurrentSize::Present(7)),
                Some(OldCurrentSize::Absent),
                Some(OldCurrentSize::Absent),
            ];
            assert_eq!(SetDisks::reduce_common_old_current_size(&observations, 3), None);
        }

        #[test]
        fn unknown_disks_do_not_vote() {
            // Two agreeing disks plus two unknowns must not fabricate quorum.
            let observations = vec![Some(OldCurrentSize::Present(5)), Some(OldCurrentSize::Present(5)), None, None];
            assert_eq!(SetDisks::reduce_common_old_current_size(&observations, 3), None);
        }

        /// Kills the "unknown counts as an Absent vote" mutant: a full set of
        /// unknowns (rolling upgrade, every peer pre-#1009) must stay unknown —
        /// fabricating `Absent` would record "new object" on every overwrite
        /// and inflate objects_count.
        #[test]
        fn all_unknown_is_unknown() {
            let observations: Vec<Option<OldCurrentSize>> = vec![None; 4];
            assert_eq!(SetDisks::reduce_common_old_current_size(&observations, 3), None);
        }

        #[test]
        fn unknown_majority_does_not_become_absent_quorum() {
            let observations = vec![Some(OldCurrentSize::Present(5)), None, None, None];
            assert_eq!(SetDisks::reduce_common_old_current_size(&observations, 3), None);
        }

        #[test]
        fn empty_observations_are_unknown() {
            assert_eq!(SetDisks::reduce_common_old_current_size(&[], 2), None);
        }
    }

    // A2: not-found normalized to success (parity with MinIO commitRenameDataDir).
    #[test]
    fn cleanup_receipt_not_found_counts_as_reclaimed() {
        let errs = vec![None, Some(DiskError::FileNotFound), Some(DiskError::VolumeNotFound)];
        let attempted = vec![true, true, true];
        let r = classify_old_data_dir_cleanup(&errs, &attempted, 1);
        assert_eq!(r.reclaimed, 3, "absent dir must count as already reclaimed");
        assert!(r.unreclaimed_disks.is_empty());
    }

    // A3: non-ignored transient failures below quorum => a receipt (not an Err),
    // below_quorum true, residue set listing exactly the failed disks.
    #[test]
    fn cleanup_receipt_transient_failures_are_residue_not_error() {
        let errs = vec![
            None,
            None,
            Some(DiskError::other("connection reset")),
            Some(DiskError::other("io timeout")),
        ];
        let attempted = vec![true, true, true, true];
        let r = classify_old_data_dir_cleanup(&errs, &attempted, 3);
        assert_eq!(r.attempted, 4);
        assert_eq!(r.reclaimed, 2);
        assert_eq!(r.unreclaimed_disks, vec![2, 3], "residue set must be exact");
        assert!(r.below_quorum, "2 achieved < wq 3");
        assert!(r.has_residue());
    }

    // A4: a join error (panic/cancel) must not be masked as DiskNotFound; it must
    // land in the residue and never be treated as an ignored error. Directly
    // regresses the old `Unexpected`/`DiskNotFound` misclassification.
    #[tokio::test]
    async fn cleanup_join_error_maps_to_non_ignored_error_not_disk_not_found() {
        let handle: tokio::task::JoinHandle<Option<DiskError>> = tokio::spawn(async { panic!("boom") });
        let joined = handle.await;
        assert!(joined.is_err(), "task must have panicked");
        let mapped = map_cleanup_join_result(joined).expect("panic must surface as an error");
        assert_ne!(
            mapped,
            DiskError::DiskNotFound,
            "a task panic must never be masked as an ignorable DiskNotFound"
        );
        assert!(
            !is_ignored_err(OBJECT_OP_IGNORED_ERRS, &mapped),
            "a task panic must not be treated as an ignored error"
        );
        let r = classify_old_data_dir_cleanup(&[Some(mapped)], &[true], 1);
        assert_eq!(r.unreclaimed_disks, vec![0]);
        assert!(r.has_residue());
    }

    // A6: all-None targeted slots => no attempt, no residue; below_quorum may be
    // true (parity lens) but nothing actually leaks. Pins the two as independent.
    #[test]
    fn cleanup_receipt_ignores_untargeted_none_slots() {
        let errs = vec![Some(DiskError::DiskNotFound), Some(DiskError::DiskNotFound)];
        let attempted = vec![false, false];
        let r = classify_old_data_dir_cleanup(&errs, &attempted, 1);
        assert_eq!(r.attempted, 0);
        assert_eq!(r.reclaimed, 0);
        assert!(r.unreclaimed_disks.is_empty());
        assert!(!r.has_residue(), "None-only slots never leak anything");
        assert!(r.below_quorum, "parity lens still flags it, but nothing is actually leaked");
    }

    // divergence: residue and below_quorum are independent signals — quorum met
    // can still leak.
    #[test]
    fn cleanup_receipt_residue_and_below_quorum_are_independent_signals() {
        let errs = vec![None, None, None, Some(DiskError::other("io"))];
        let attempted = vec![true, true, true, true];
        let r = classify_old_data_dir_cleanup(&errs, &attempted, 3);
        assert!(!r.below_quorum, "3 achieved >= wq 3");
        assert_eq!(r.unreclaimed_disks, vec![3]);
        assert!(r.has_residue(), "leak metric must fire even when write quorum was met");
    }

    // C: persistent failure => leak metric + heal enqueue decided, no Err/panic.
    #[test]
    fn persistent_cleanup_failure_triggers_leak_metric_and_heal_not_error() {
        let errs = vec![None, Some(DiskError::other("disk down")), Some(DiskError::other("disk down"))];
        let attempted = vec![true, true, true];
        let receipt = classify_old_data_dir_cleanup(&errs, &attempted, 2);
        assert!(receipt.has_residue());
        let actions = old_data_dir_cleanup_actions(&receipt);
        assert!(actions.emit_leak_metric, "persistent residue must be recorded by an observable metric");
        assert!(actions.enqueue_heal, "persistent residue must enqueue a heal (disk-health signal)");
        assert!(actions.warn);
    }

    // A clean receipt takes no action.
    #[test]
    fn clean_cleanup_receipt_triggers_no_actions() {
        let receipt = classify_old_data_dir_cleanup(&[None, None], &[true, true], 1);
        assert_eq!(old_data_dir_cleanup_actions(&receipt), CleanupActions::default());
    }

    // ------------------------------------------------------------------
    // backlog#900: pick_latest_quorum_files_info must survive a single
    // corrupt-part disk (even in the merged representative slot) by deriving
    // the vid from the header and falling into the per-disk loop, flagging the
    // corrupt disk for heal instead of poisoning the whole read.
    // ------------------------------------------------------------------

    use rustfs_filemeta::{ChecksumAlgo, ErasureAlgo, FileMeta, FileMetaVersion, MetaObject, RawFileInfo, VersionType};
    use time::OffsetDateTime;
    use uuid::Uuid;

    fn raw_object_version(vid: Uuid, part_sizes: Vec<usize>) -> RawFileInfo {
        let mut fm = FileMeta::new();
        fm.add_version_filemata(FileMetaVersion {
            version_type: VersionType::Object,
            object: Some(MetaObject {
                version_id: Some(vid),
                erasure_algorithm: ErasureAlgo::ReedSolomon,
                erasure_m: 2,
                erasure_n: 1,
                erasure_index: 1,
                erasure_dist: vec![1, 2, 3],
                erasure_block_size: 1 << 20,
                bitrot_checksum_algo: ChecksumAlgo::HighwayHash,
                part_numbers: vec![1, 2],
                part_sizes,
                part_actual_sizes: vec![10, 20],
                mod_time: Some(OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap()),
                ..Default::default()
            }),
            ..Default::default()
        })
        .unwrap();
        RawFileInfo {
            buf: fm.marshal_msg().unwrap(),
        }
    }

    #[tokio::test]
    async fn pick_latest_quorum_masks_single_corrupt_disk_in_representative_slot() {
        let vid = Uuid::new_v4();
        // Deterministic: the corrupt disk is fixed at index 0 (representative slot).
        let fileinfos = vec![
            Some(raw_object_version(vid, vec![10])),
            Some(raw_object_version(vid, vec![10, 20])),
            Some(raw_object_version(vid, vec![10, 20])),
        ];
        let errs = vec![None, None, None];

        let (infos, out_errs) = SetDisks::pick_latest_quorum_files_info(fileinfos, errs, "bucket", "obj", false, false).await;

        // The corrupt representative disk is flagged for heal.
        assert_eq!(out_errs[0], Some(DiskError::FileCorrupt), "corrupt representative disk must be flagged");
        // Good disks still produce valid FileInfo, satisfying read_quorum (3.div_ceil(2)=2).
        let good = infos.iter().filter(|fi| fi.is_valid()).count();
        assert!(good >= 2, "quorum of good disks must survive corrupt representative, got {good}");
    }

    #[tokio::test]
    async fn pick_latest_quorum_all_corrupt_fails_clean_without_panic() {
        let vid = Uuid::new_v4();
        let fileinfos = vec![
            Some(raw_object_version(vid, vec![10])),
            Some(raw_object_version(vid, vec![10])),
            Some(raw_object_version(vid, vec![10])),
        ];
        let errs = vec![None, None, None];

        let (infos, out_errs) = SetDisks::pick_latest_quorum_files_info(fileinfos, errs, "bucket", "obj", false, false).await;

        assert!(
            out_errs.iter().all(|e| e == &Some(DiskError::FileCorrupt)),
            "all disks must be flagged corrupt"
        );
        assert!(infos.iter().all(|fi| !fi.is_valid()), "no half-corrupt FileInfo may be returned");
    }
}
