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

//! Metadata read scheduling, cancellation and coalescing owned by `SetDisks`.
//! Quorum decisions consume slot observations in `metadata_quorum`.

#[cfg(test)]
use super::io_primitives::{disk_call_counters, rename_fanout_barrier};
use super::metadata_quorum::{
    MetadataDiskObservation, MetadataQuorumAccumulator, is_metadata_fanout_ignored_error, metadata_early_stop_candidate_matches,
};
use crate::diagnostics::get::{
    GET_METADATA_EARLY_STOP_REASON_DATA_READ_INLINE_BODY_VERIFY, GET_METADATA_EARLY_STOP_REASON_DATA_READ_INLINE_DELETED,
    GET_METADATA_EARLY_STOP_REASON_DATA_READ_INLINE_GEOMETRY, GET_METADATA_EARLY_STOP_REASON_DATA_READ_INLINE_MISSING_SHARD,
    GET_METADATA_EARLY_STOP_REASON_DATA_READ_INLINE_NOT_INLINE, GET_METADATA_EARLY_STOP_REASON_DATA_READ_INLINE_PART_SHAPE,
    GET_METADATA_EARLY_STOP_REASON_DATA_READ_INLINE_REMOTE, GET_METADATA_EARLY_STOP_REASON_DATA_READ_INLINE_SIZE,
    GET_METADATA_EARLY_STOP_REASON_DATA_READ_INLINE_TRANSFORMED, GET_METADATA_EARLY_STOP_REASON_INSUFFICIENT_QUORUM,
    GET_METADATA_EARLY_STOP_REASON_UNSAFE_REQUEST, GET_METADATA_RESPONSE_CORRUPT, GET_METADATA_RESPONSE_DISK_NOT_FOUND,
    GET_METADATA_RESPONSE_ERROR, GET_METADATA_RESPONSE_IGNORED, GET_METADATA_RESPONSE_NOT_FOUND, GET_METADATA_RESPONSE_TIMEOUT,
    GET_METADATA_RESPONSE_VALID, GET_METADATA_RESPONSE_VERSION_NOT_FOUND, GET_OBJECT_PATH_INTERNAL_META,
    GET_OBJECT_PATH_LEGACY_DUPLEX,
};
use crate::disk::disk_store::get_drive_metadata_timeout;
use crate::disk::{
    self, BATCH_READ_VERSION_MAX_ITEMS, BatchReadVersionItem, BatchReadVersionReq, BatchReadVersionResp, Disk, DiskAPI,
};
use crate::set_disk::{
    DiskError, DiskStore, FileInfo, HashAlgorithm, ReadOptions, SetDisks, build_inline_bitrot_readers_from_refs,
    can_try_inline_data_shards_direct, codec_streaming_rollout_applies, coding,
    collect_inline_data_shard_fileinfos_from_observations, file_info_is_valid_for_metadata, get_metadata_slowtail_fault_request,
    inline_erasure_shard_file_offset, inline_erasure_shard_size, is_get_metadata_data_read_early_stop_enabled,
    is_get_metadata_early_stop_bounded_fanout_enabled, is_get_metadata_early_stop_enabled,
    is_get_metadata_non_inline_data_read_early_stop_enabled, is_version_early_stop_enabled, object_fits_single_block,
    try_read_inline_data_shards_direct,
};
use futures::future::join_all;
use metrics::counter;
use rustfs_io_metrics::internode_metrics::{
    INTERNODE_STAGE_BATCH_READ_VERSION_COALESCER_WAIT, INTERNODE_STAGE_BATCH_READ_VERSION_RESPONSE_MAP,
};
#[cfg(test)]
use std::collections::HashSet;
use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::{Arc, OnceLock},
    task::{Context, Poll},
    time::{Duration, Instant},
};
use tokio::sync::{Mutex, oneshot};
use tokio::task::JoinSet;

#[derive(Debug)]
pub(in crate::set_disk) struct MetadataReadResult {
    pub(in crate::set_disk) slots: Vec<MetadataDiskObservation>,
    pub(in crate::set_disk) diagnostics: MetadataFanoutDiagnostics,
    pub(in crate::set_disk) condition_stopped: bool,
}

impl MetadataReadResult {
    fn pending(total_disks: usize) -> Vec<MetadataDiskObservation> {
        (0..total_disks)
            .map(|disk_index| MetadataDiskObservation {
                disk_index,
                result: None,
            })
            .collect()
    }

    pub(in crate::set_disk) fn is_complete(&self) -> bool {
        self.slots.iter().all(|slot| slot.result.is_some())
    }

    /// Legacy quorum/layout helpers still require aligned slices. Only this
    /// boundary produces their empty placeholders; the scheduler and reducer
    /// never infer a response from a default `FileInfo` or a missing error.
    // RUSTFS_COMPAT_TODO(backlog-2253-metadata-observation-adapter): Existing layout, shard and exact-version writer/delete callers still consume aligned slices. Remove after those consumers use typed observations in a separate cleanup.
    pub(in crate::set_disk) fn into_legacy(self) -> (Vec<FileInfo>, Vec<Option<DiskError>>, MetadataFanoutDiagnostics) {
        let mut metadata = Vec::with_capacity(self.slots.len());
        let mut errors = Vec::with_capacity(self.slots.len());
        for slot in self.slots {
            match slot.result {
                None => {
                    metadata.push(FileInfo::default());
                    errors.push(None);
                }
                Some(Ok(file_info)) => {
                    metadata.push(file_info);
                    errors.push(None);
                }
                Some(Err(error)) => {
                    metadata.push(FileInfo::default());
                    errors.push(Some(error));
                }
            }
        }
        (metadata, errors, self.diagnostics)
    }
}

pub(super) const ENV_RUSTFS_GET_METADATA_READ_VERSION_COALESCE: &str = "RUSTFS_GET_METADATA_READ_VERSION_COALESCE";
pub(super) const ENV_RUSTFS_GET_METADATA_READ_VERSION_COALESCE_DELAY_MICROS: &str =
    "RUSTFS_GET_METADATA_READ_VERSION_COALESCE_DELAY_MICROS";
const DEFAULT_GET_METADATA_READ_VERSION_COALESCE_DELAY_MICROS: u64 = 200;
const METRIC_GET_METADATA_READ_VERSION_COALESCER_TOTAL: &str = "rustfs_get_metadata_read_version_coalescer_total";

fn metadata_metrics_path(bucket: &str) -> &'static str {
    if crate::bucket::utils::is_meta_bucketname(bucket) {
        GET_OBJECT_PATH_INTERNAL_META
    } else {
        GET_OBJECT_PATH_LEGACY_DUPLEX
    }
}

pub(super) fn metadata_distribution_key(bucket: &str, object: &str) -> String {
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
pub(super) struct ExpectedBatchReadVersionItem {
    pub(super) path: String,
    pub(super) version_id: String,
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

fn batch_read_version_stage_timer() -> Option<Instant> {
    rustfs_io_metrics::get_stage_metrics_enabled().then(Instant::now)
}

fn record_batch_read_version_stage(stage: &'static str, started_at: Option<Instant>) {
    if let Some(started_at) = started_at {
        crate::cluster::rpc::runtime_sources::record_remote_disk_grpc_batch_read_version_stage(stage, started_at.elapsed());
    }
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

    let wait_started = batch_read_version_stage_timer();
    let response = rx
        .await
        .unwrap_or_else(|_| Err(DiskError::other("coalesced read_version response channel closed")));
    record_batch_read_version_stage(INTERNODE_STAGE_BATCH_READ_VERSION_COALESCER_WAIT, wait_started);
    response
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

    // Only the #[cfg(test)] counter-recording block below reads this.
    #[cfg(not(test))]
    let _ = lane_key;
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
            let map_started = batch_read_version_stage_timer();
            let results = map_batch_read_version_responses(&expected_items, responses);
            record_batch_read_version_stage(INTERNODE_STAGE_BATCH_READ_VERSION_RESPONSE_MAP, map_started);
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

pub(super) fn map_batch_read_version_responses(
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

struct AbortOnDropJoinHandle<T>(tokio::task::JoinHandle<T>);

impl<T> Future for AbortOnDropJoinHandle<T> {
    type Output = std::result::Result<T, tokio::task::JoinError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.0).poll(cx)
    }
}

impl<T> Drop for AbortOnDropJoinHandle<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
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

#[cfg(test)]
pub(in crate::set_disk) async fn data_read_early_stop_inline_body_miss_reason(
    bucket: &str,
    object: &str,
    candidate: &FileInfo,
    parts_metadata: &[FileInfo],
    disks: &[Option<DiskStore>],
) -> Option<&'static str> {
    inline_body_miss_reason_from_observations(
        bucket,
        object,
        candidate,
        parts_metadata
            .iter()
            .enumerate()
            .map(|(index, metadata)| (index, Some(metadata))),
        disks,
    )
    .await
}

async fn inline_body_miss_reason_from_observations<'a>(
    bucket: &str,
    object: &str,
    candidate: &FileInfo,
    observations: impl IntoIterator<Item = (usize, Option<&'a FileInfo>)>,
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
        match collect_inline_data_shard_fileinfos_from_observations(observations, candidate, erasure.data_shards, |index| {
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

pub(in crate::set_disk) fn non_inline_data_read_candidate_is_safe(candidate: &FileInfo) -> bool {
    if candidate.inline_data()
        || candidate.is_compressed()
        || candidate.is_remote()
        || candidate
            .metadata
            .keys()
            .any(|key| rustfs_utils::http::is_object_encryption_marker(key))
        || candidate.parts.len() != 1
    {
        return false;
    }
    candidate.has_valid_erasure_geometry()
}

pub(in crate::set_disk) fn late_materialization_candidate_is_safe(candidate: &FileInfo) -> bool {
    non_inline_data_read_candidate_is_safe(candidate)
        && candidate.size > 512 * 1024
        && object_fits_single_block(candidate.size, candidate.erasure.block_size)
}

pub(in crate::set_disk) fn non_inline_data_read_early_stop_allowed(read_data: bool, bucket: &str, object: &str) -> bool {
    read_data && is_get_metadata_non_inline_data_read_early_stop_enabled() && !codec_streaming_rollout_applies(bucket, object)
}

const NON_INLINE_SINGLE_PENDING_HEDGE_DELAY: Duration = Duration::from_millis(100);

fn data_read_inline_missing_shards_are_pending(
    candidate: &FileInfo,
    slots: &[MetadataDiskObservation],
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

    for slot in slots {
        let disk_index = slot.disk_index;
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
        if slot.file_info().is_none_or(|metadata| metadata.name.is_empty()) {
            let scheduled_and_not_failed = fanout_order
                .get(..scheduled_fanout_len)
                .is_some_and(|scheduled_disks| scheduled_disks.contains(&disk_index))
                && slot.result.is_none();
            if scheduled_and_not_failed {
                data_shards_seen_or_pending[data_slot] = true;
                missing_pending_data_shards = missing_pending_data_shards.saturating_add(1);
                continue;
            }
            return false;
        }
        let Some(file_info) = slot.file_info() else { return false };
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
    #[allow(clippy::too_many_arguments)]
    #[tracing::instrument(level = "debug", skip(disks))]
    pub(in crate::set_disk) async fn read_metadata(
        disks: &[Option<DiskStore>],
        org_bucket: &str,
        bucket: &str,
        object: &str,
        version_id: &str,
        read_data: bool,
        healing: bool,
        incl_free_versions: bool,
    ) -> disk::error::Result<MetadataReadResult> {
        Self::read_metadata_inner(
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
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(in crate::set_disk) async fn read_metadata_observed(
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
    ) -> disk::error::Result<MetadataReadResult> {
        Self::read_metadata_inner(
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
    pub(in crate::set_disk) async fn read_metadata_for_get_object(
        disks: &[Option<DiskStore>],
        org_bucket: &str,
        bucket: &str,
        object: &str,
        version_id: &str,
        read_data: bool,
        incl_free_versions: bool,
        caller_allows_early_stop: bool,
        default_parity_count: usize,
    ) -> disk::error::Result<MetadataReadResult> {
        Self::read_metadata_inner(
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
    pub(in crate::set_disk) async fn read_metadata_inner(
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
    ) -> disk::error::Result<MetadataReadResult> {
        let early_stop_enabled =
            caller_allows_early_stop && observe && (is_get_metadata_early_stop_enabled() || is_version_early_stop_enabled());
        let allow_early_stop =
            metadata_early_stop_permitted(caller_allows_early_stop, observe, read_data, version_id, healing, incl_free_versions);
        if allow_early_stop {
            return Self::read_metadata_early_stop(
                disks,
                org_bucket,
                bucket,
                object,
                version_id,
                read_data,
                healing,
                incl_free_versions,
                non_inline_data_read_early_stop_allowed(read_data, bucket, object),
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

        Self::read_metadata_full_wait(
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
    async fn read_metadata_full_wait(
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
    ) -> disk::error::Result<MetadataReadResult> {
        let fanout_start = observe.then(Instant::now);
        let mut slots = MetadataReadResult::pending(disks.len());
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
            AbortOnDropJoinHandle(tokio::spawn(async move {
                let response_start = observe.then(Instant::now);
                let result = if let Some(disk) = disk {
                    Self::record_read_version_call(&object, disk_index);
                    if let Some(delay) = slowtail_fault.as_ref().and_then(|fault| fault.delay_for_disk(disk_index)) {
                        Self::record_metadata_slowtail_fault(&object, disk_index);
                        tokio::time::sleep(delay).await;
                    }
                    read_version_via_coalescer(disk, &org_bucket, &bucket, &object, &version_id, &task_opts, allow_coalescing)
                        .await
                } else {
                    Err(DiskError::DiskNotFound)
                };
                let elapsed = response_start.map(|start| start.elapsed());
                (result, elapsed)
            }))
        });

        // Wait for all futures to complete
        let results = join_all(futures).await;

        for (index, join_result) in results.into_iter().enumerate() {
            match join_result {
                Ok((res, elapsed)) => match res {
                    Ok(file_info) => {
                        if let (Some(observations), Some(elapsed)) = (&mut observations, elapsed) {
                            observations.push(MetadataFanoutObservation::from_file_info(&file_info, elapsed));
                        }
                        slots[index].result = Some(Ok(file_info));
                    }
                    Err(e) => {
                        if let (Some(observations), Some(elapsed)) = (&mut observations, elapsed) {
                            observations.push(MetadataFanoutObservation::from_error(&e, elapsed));
                        }
                        slots[index].result = Some(Err(e));
                    }
                },
                Err(_join_err) => {
                    // A spawned task panicked — treat as unexpected disk error
                    if let Some(observations) = &mut observations {
                        observations.push(MetadataFanoutObservation::from_error(&DiskError::Unexpected, Duration::ZERO));
                    }
                    slots[index].result = Some(Err(DiskError::Unexpected));
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
        Ok(MetadataReadResult {
            slots,
            diagnostics,
            condition_stopped: false,
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn read_metadata_early_stop(
        disks: &[Option<DiskStore>],
        org_bucket: &str,
        bucket: &str,
        object: &str,
        version_id: &str,
        read_data: bool,
        healing: bool,
        incl_free_versions: bool,
        allow_non_inline_data_read_early_stop: bool,
        default_parity_count: usize,
        allow_coalescing: bool,
    ) -> disk::error::Result<MetadataReadResult> {
        let fanout_start = Instant::now();
        let mut slots = MetadataReadResult::pending(disks.len());
        let mut task_ids = vec![None; disks.len()];
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
        let read_condition_active = read_data && crate::object_api::get_object_read_condition_is_active();
        let mut final_miss_reason_override = None;
        let mut non_inline_candidate_eligible = None;
        let mut single_pending_hedge_deadline = None;
        let slowtail_fault = get_metadata_slowtail_fault_request(bucket.as_ref(), object.as_ref(), read_data);
        let spawn_read_version = |join_set: &mut JoinSet<(usize, disk::error::Result<FileInfo>, Duration)>,
                                  task_ids: &mut [Option<tokio::task::Id>],
                                  index: usize,
                                  disk: Option<DiskStore>| {
            let task_opts = opts;
            let org_bucket = org_bucket.clone();
            let bucket = bucket.clone();
            let object = object.clone();
            let version_id = version_id.clone();
            let slowtail_fault = slowtail_fault.clone();
            let task = join_set.spawn(async move {
                let response_start = Instant::now();
                let result = if let Some(disk) = disk {
                    #[cfg(test)]
                    let _fanout_task_guard = rename_fanout_barrier::task_guard(&object);
                    Self::record_read_version_call(&object, index);
                    #[cfg(test)]
                    Self::read_version_fanout_barrier(&object, index).await;
                    if let Some(delay) = slowtail_fault.as_ref().and_then(|fault| fault.delay_for_disk(index)) {
                        Self::record_metadata_slowtail_fault(&object, index);
                        #[cfg(test)]
                        rename_fanout_barrier::checkpoint(&object, index, rename_fanout_barrier::PHASE_METADATA_SLOWTAIL_FAULT)
                            .await;
                        tokio::time::sleep(delay).await;
                    }
                    read_version_via_coalescer(disk, &org_bucket, &bucket, &object, &version_id, &task_opts, allow_coalescing)
                        .await
                } else {
                    Err(DiskError::DiskNotFound)
                };
                (index, result, response_start.elapsed())
            });
            task_ids[index] = Some(task.id());
        };

        if bounded_fanout {
            let initial_target = accumulator.default_write_quorum().min(disks.len());
            while next_fanout_index < initial_target {
                let disk_index = fanout_order[next_fanout_index];
                if let Some(disk) = disks.get(disk_index).cloned() {
                    spawn_read_version(&mut join_set, &mut task_ids, disk_index, disk);
                    scheduled_count = scheduled_count.saturating_add(1);
                }
                next_fanout_index = next_fanout_index.saturating_add(1);
            }
        } else {
            for (index, disk) in disks.iter().cloned().enumerate() {
                spawn_read_version(&mut join_set, &mut task_ids, index, disk);
                scheduled_count = scheduled_count.saturating_add(1);
            }
        }

        loop {
            let mut defer_pending_inline_data_shard = false;
            let result = if let Some(deadline) = single_pending_hedge_deadline.take() {
                tokio::select! {
                    result = join_set.join_next_with_id() => result,
                    _ = async {
                        tokio::time::sleep_until(deadline).await;
                        #[cfg(test)]
                        rename_fanout_barrier::checkpoint(
                            object.as_ref(),
                            0,
                            rename_fanout_barrier::PHASE_NON_INLINE_HEDGE_TIMER,
                        )
                        .await;
                    } => {
                        if bounded_fanout
                            && !force_full_wait
                            && join_set.len() == 1
                            && non_inline_candidate_eligible == Some(true)
                            && !accumulator.candidate_has_read_reserve()
                            && next_fanout_index < disks.len()
                        {
                            while next_fanout_index < disks.len() {
                                let disk_index = fanout_order[next_fanout_index];
                                next_fanout_index = next_fanout_index.saturating_add(1);
                                if let Some(disk) = disks.get(disk_index).cloned() {
                                    spawn_read_version(&mut join_set, &mut task_ids, disk_index, disk);
                                    scheduled_count = scheduled_count.saturating_add(1);
                                    break;
                                }
                            }
                        }
                        continue;
                    }
                }
            } else {
                join_set.join_next_with_id().await
            };
            let Some(result) = result else { break };
            match result {
                Ok((_task_id, (index, res, elapsed))) => match res {
                    Ok(file_info) => {
                        observations.push(MetadataFanoutObservation::from_file_info(&file_info, elapsed));
                        let inline_candidate_miss_reason = if bounded_fanout && read_data && !force_full_wait {
                            data_read_early_stop_inline_candidate_miss_reason(&file_info)
                        } else {
                            None
                        };
                        slots[index].result = Some(Ok(file_info));
                        accumulator.observe(&slots[index]);
                        if allow_non_inline_data_read_early_stop && non_inline_candidate_eligible.is_none() {
                            non_inline_candidate_eligible =
                                accumulator.candidate.as_ref().map(non_inline_data_read_candidate_is_safe);
                        }
                        if bounded_fanout
                            && read_data
                            && !force_full_wait
                            && let Some(reason) = inline_candidate_miss_reason
                            && !(non_inline_candidate_eligible == Some(true)
                                && reason == GET_METADATA_EARLY_STOP_REASON_DATA_READ_INLINE_NOT_INLINE)
                        {
                            force_full_wait = true;
                            final_miss_reason_override.get_or_insert(reason);
                        }
                    }
                    Err(err) => {
                        observations.push(MetadataFanoutObservation::from_error(&err, elapsed));
                        slots[index].result = Some(Err(err));
                        accumulator.observe(&slots[index]);
                    }
                },
                Err(join_error) => {
                    let err = DiskError::Unexpected;
                    observations.push(MetadataFanoutObservation::from_error(&err, fanout_start.elapsed()));
                    if let Some(index) = task_ids.iter().position(|id| *id == Some(join_error.id())) {
                        slots[index].result = Some(Err(err));
                        accumulator.observe(&slots[index]);
                    }
                }
            }

            if (!force_full_wait || read_condition_active)
                && let Some(decision) = accumulator
                    .early_stop_decision()
                    .or_else(|| accumulator.version_early_stop_decision())
            {
                let condition_stopped = read_condition_active
                    && decision.reason == crate::diagnostics::get::GET_METADATA_EARLY_STOP_REASON_VALID_QUORUM
                    && accumulator.candidate.as_ref().is_some_and(|candidate| {
                        !candidate.deleted
                            && crate::object_api::get_object_read_condition_is_terminal(
                                crate::object_api::GetObjectReadMetadata::from_file_info(
                                    bucket.as_ref(),
                                    object.as_ref(),
                                    candidate,
                                ),
                            )
                    });
                let should_return_early = if condition_stopped {
                    true
                } else if force_full_wait {
                    false
                } else if read_data {
                    match accumulator.candidate.as_ref() {
                        Some(_candidate) if non_inline_candidate_eligible == Some(true) => {
                            accumulator.candidate_has_read_reserve()
                        }
                        Some(candidate) => match inline_body_miss_reason_from_observations(
                            bucket.as_ref(),
                            object.as_ref(),
                            candidate,
                            slots.iter().map(|slot| (slot.disk_index, slot.file_info())),
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
                                        &slots,
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
                    while let Some(join_result) = join_set.join_next_with_id().await {
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
                    return Ok(MetadataReadResult {
                        slots,
                        diagnostics,
                        condition_stopped,
                    });
                }
            }

            let pending_responses = join_set.len();
            // Inline verification can still depend on a missing data shard;
            // issue one immediate spare when only that shard remains. The
            // non-inline path keeps its delayed hedge below to avoid healthy
            // reads paying speculative I/O before the candidate is classified.
            let should_hedge_single_pending_inline_read = read_data
                && !force_full_wait
                && !defer_pending_inline_data_shard
                && pending_responses == 1
                && non_inline_candidate_eligible != Some(true)
                && accumulator.can_still_reach_early_stop_with_pending(pending_responses);
            // A non-inline plan must retain one extra matching shard as a
            // reconstruction reserve. Schedule that reserve only after the
            // candidate is known to be eligible, so inline GETs do not pay an
            // extra fanout and the healthy path remains allocation-free.
            let needs_non_inline_read_reserve = non_inline_candidate_eligible == Some(true)
                && !accumulator.candidate_has_read_reserve()
                && accumulator
                    .candidate_read_reserve_target()
                    .is_some_and(|reserve_target| scheduled_count < reserve_target || pending_responses == 0);
            if bounded_fanout
                && !force_full_wait
                && (needs_non_inline_read_reserve || should_hedge_single_pending_inline_read)
                && next_fanout_index < disks.len()
            {
                let disk_index = fanout_order[next_fanout_index];
                if let Some(disk) = disks.get(disk_index).cloned() {
                    spawn_read_version(&mut join_set, &mut task_ids, disk_index, disk);
                    scheduled_count = scheduled_count.saturating_add(1);
                }
                next_fanout_index = next_fanout_index.saturating_add(1);
            } else if bounded_fanout && force_full_wait {
                while next_fanout_index < disks.len() {
                    let disk_index = fanout_order[next_fanout_index];
                    if let Some(disk) = disks.get(disk_index).cloned() {
                        spawn_read_version(&mut join_set, &mut task_ids, disk_index, disk);
                        scheduled_count = scheduled_count.saturating_add(1);
                    }
                    next_fanout_index = next_fanout_index.saturating_add(1);
                }
            } else if bounded_fanout
                && !defer_pending_inline_data_shard
                && next_fanout_index < disks.len()
                && !accumulator.can_still_reach_early_stop_with_pending(pending_responses)
            {
                let disk_index = fanout_order[next_fanout_index];
                if let Some(disk) = disks.get(disk_index).cloned() {
                    spawn_read_version(&mut join_set, &mut task_ids, disk_index, disk);
                    scheduled_count = scheduled_count.saturating_add(1);
                }
                next_fanout_index = next_fanout_index.saturating_add(1);
            }
            if bounded_fanout
                && !force_full_wait
                && !defer_pending_inline_data_shard
                && join_set.len() == 1
                && non_inline_candidate_eligible == Some(true)
                && !accumulator.candidate_has_read_reserve()
                && accumulator.can_still_reach_early_stop_with_pending(join_set.len())
                && next_fanout_index < disks.len()
            {
                single_pending_hedge_deadline = Some(tokio::time::Instant::now() + NON_INLINE_SINGLE_PENDING_HEDGE_DELAY);
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
        Ok(MetadataReadResult {
            slots,
            diagnostics,
            condition_stopped: false,
        })
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
    fn record_metadata_slowtail_fault(object: &str, disk_index: usize) {
        disk_call_counters::record(object, disk_call_counters::KIND_METADATA_SLOWTAIL_FAULT, disk_index);
    }

    #[cfg(not(test))]
    #[inline(always)]
    fn record_metadata_slowtail_fault(_object: &str, _disk_index: usize) {}

    #[cfg(test)]
    #[inline]
    async fn read_version_fanout_barrier(object: &str, disk_index: usize) {
        rename_fanout_barrier::checkpoint(object, disk_index, rename_fanout_barrier::PHASE_READ_VERSION).await;
    }
}
