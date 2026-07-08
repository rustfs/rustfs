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
    GET_METADATA_EARLY_STOP_REASON_CONFLICTING_METADATA, GET_METADATA_EARLY_STOP_REASON_DELETE_MARKER,
    GET_METADATA_EARLY_STOP_REASON_ERROR, GET_METADATA_EARLY_STOP_REASON_INSUFFICIENT_QUORUM,
    GET_METADATA_EARLY_STOP_REASON_NOT_FOUND, GET_METADATA_EARLY_STOP_REASON_UNSAFE_REQUEST,
    GET_METADATA_EARLY_STOP_REASON_VALID_QUORUM, GET_METADATA_EARLY_STOP_REASON_VERSION_MATCH_QUORUM,
    GET_METADATA_EARLY_STOP_REASON_VERSION_NOT_FOUND, GET_METADATA_RESPONSE_CORRUPT, GET_METADATA_RESPONSE_DISK_NOT_FOUND,
    GET_METADATA_RESPONSE_ERROR, GET_METADATA_RESPONSE_IGNORED, GET_METADATA_RESPONSE_NOT_FOUND, GET_METADATA_RESPONSE_TIMEOUT,
    GET_METADATA_RESPONSE_VALID, GET_METADATA_RESPONSE_VERSION_NOT_FOUND, GET_OBJECT_PATH_CODEC_STREAMING,
    GET_OBJECT_PATH_DIRECT_MEMORY, GET_OBJECT_PATH_LEGACY_DUPLEX, GET_OBJECT_PATH_SET_DISK, GET_STAGE_DECODE,
    GET_STAGE_METADATA_CACHE_LOOKUP, GET_STAGE_METADATA_RESOLVE, GET_STAGE_RANGE, GET_STAGE_READER_SETUP,
    GET_STAGE_READER_SETUP_DROP_PENDING, GET_STAGE_READER_SETUP_SCHEDULE, GET_STAGE_READER_SETUP_WAIT_QUORUM,
    GET_STAGE_READER_TASK_BITROT_READER_INIT, GET_STAGE_READER_TASK_FILE_OPEN, GET_STAGE_READER_TASK_READER_CONSTRUCTION,
    GetObjectFailureReason, classify_disk_error, get_stage_timer_if_enabled, record_get_object_pipeline_failure,
    record_get_object_pipeline_failure_for_path, record_get_stage_duration_if_enabled,
};
use crate::erasure::coding::BitrotReader;
use crate::io_support::bitrot::{
    BitrotReaderStageMetrics, DeferredReaderStripeHandle, create_bitrot_reader_with_stage_metrics,
    create_deferred_bitrot_reader_with_stripe_handle, object_mmap_read_enabled,
};
use crate::set_disk::shard_source::ShardReadCost;
use futures::stream::{FuturesUnordered, StreamExt};
use metrics::counter;
use std::{
    collections::{HashMap, HashSet, VecDeque},
    future::Future,
    pin::Pin,
    sync::OnceLock,
    task::{Context, Poll},
    time::{Duration, Instant},
};
use tokio::io::{AsyncRead, ReadBuf};
use tokio::sync::RwLock;
use tokio::task::JoinSet;

pub(in crate::set_disk) const EVENT_SET_DISK_READ: &str = "set_disk_read";
pub(in crate::set_disk) const ENV_RUSTFS_GET_DATA_BLOCKS_FIRST_READER_SETUP: &str = "RUSTFS_GET_DATA_BLOCKS_FIRST_READER_SETUP";
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

pub(in crate::set_disk) struct MultipartCodecStreamingReader {
    pub(in crate::set_disk) readers: VecDeque<Box<dyn AsyncRead + Unpin + Send + Sync>>,
}

impl MultipartCodecStreamingReader {
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
        if file_info.is_valid() {
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

    pub(in crate::set_disk) fn error_responses(&self) -> usize {
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
            self.error_responses(),
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
        if !file_info.is_valid() {
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

        if file_info.deleted {
            self.delete_marker_votes = self.delete_marker_votes.saturating_add(1);
            self.delete_marker_seen = true;
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
        if self.delete_marker_votes >= self.missing_response_quorum() {
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
            .and_then(|candidate| self.candidate_read_quorum(candidate))
            .is_some_and(|read_quorum| self.candidate_votes >= read_quorum)
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

    pub(in crate::set_disk) fn candidate_read_quorum(&self, candidate: &FileInfo) -> Option<usize> {
        if self.default_parity_count == 0 {
            return Some(self.total_disks);
        }
        if candidate.deleted || candidate.size == 0 || candidate.erasure.parity_blocks >= self.total_disks {
            return None;
        }
        Some(self.total_disks.saturating_sub(candidate.erasure.parity_blocks))
    }

    pub(in crate::set_disk) fn missing_response_quorum(&self) -> usize {
        if self.default_parity_count == 0 {
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
        && left.size == right.size
        && left.mod_time == right.mod_time
        && left.mode == right.mode
        && left.metadata == right.metadata
        && left.parts == right.parts
        && left.checksum == right.checksum
        && left.versioned == right.versioned
        && left.data_dir == right.data_dir
        && left.erasure.algorithm == right.erasure.algorithm
        && left.erasure.data_blocks == right.erasure.data_blocks
        && left.erasure.parity_blocks == right.erasure.parity_blocks
        && left.erasure.block_size == right.erasure.block_size
        && left.erasure.distribution == right.erasure.distribution
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
    let mut etag_quorum = HashMap::new();
    let mut part_infos = Vec::new();
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
            *etag_quorum.entry(parts[part_idx].etag.clone()).or_insert(0) += 1;
            part_infos.push(parts[part_idx].clone());
            continue;
        }

        if is_confirmed_missing_part_error(parts[part_idx].error.as_deref()) {
            missing_count += 1;
        } else {
            transient_error_count += 1;
        }
    }

    let mut max_etag_quorum = 0;
    let mut max_etag = None;
    for (etag, quorum) in etag_quorum.iter() {
        if quorum > &max_etag_quorum {
            max_etag_quorum = *quorum;
            max_etag = Some(etag);
        }
    }
    let max_quorum = max_etag_quorum.max(missing_count);

    let mut found = None;
    for info in part_infos.iter() {
        if let Some(etag) = max_etag
            && info.etag == *etag
        {
            found = Some(info.clone());
            break;
        }
    }

    if let (Some(found), Some(max_etag)) = (found, max_etag)
        && !found.etag.is_empty()
        && etag_quorum.get(max_etag).unwrap_or(&0) >= &read_quorum
    {
        return Ok(found);
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
    counter!("rustfs_heal_read_repair_dedup_total", "reason" => reason.to_string()).increment(1);
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
    } = submission;

    let Some(dedup_key) = reserve_read_repair_heal(bucket, object, version_id, pool_index, set_index).await else {
        record_read_repair_dedup("duplicate");
        debug!(
            bucket,
            object, part_number, pool_index, set_index, reason, "Skipped duplicate read-repair heal request"
        );
        return;
    };

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

pub(in crate::set_disk) type ObjectBitrotReader = BitrotReader<Box<dyn AsyncRead + Send + Sync + Unpin>>;
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

pub(in crate::set_disk) fn get_bitrot_reader_setup_strategy(
    mode: BitrotReaderSetupMode,
    prefer_data_blocks_first: bool,
) -> BitrotReaderSetupStrategy {
    match mode {
        BitrotReaderSetupMode::ReadQuorum
            if prefer_data_blocks_first || rustfs_utils::get_env_bool(ENV_RUSTFS_GET_DATA_BLOCKS_FIRST_READER_SETUP, false) =>
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

    let inline_data = files[idx].data.as_deref();
    let data_dir = files[idx].data_dir.unwrap_or_default();
    let disk = disks[idx].as_ref();
    let path = format!("{object}/{data_dir}/part.{part_number}");

    reader_tasks.push(Box::pin(async move {
        let result = create_bitrot_reader_with_stage_metrics(
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
        let inline_data = files[idx].data.as_deref();
        let data_dir = files[idx].data_dir.unwrap_or_default();
        let disk = disk_op.as_ref();
        let path = format!("{object}/{data_dir}/part.{part_number}");
        let checksum_algo = checksum_algo.clone();

        reader_tasks.push(async move {
            let result = create_bitrot_reader_with_stage_metrics(
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
    if read_data {
        return false;
    }

    (is_get_metadata_early_stop_enabled() && version_id.is_empty() && !healing && !incl_free_versions)
        || (is_version_early_stop_enabled() && !version_id.is_empty() && !healing)
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
        allow_early_stop: bool,
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
            allow_early_stop,
            default_parity_count,
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
            )
            .await;
        }
        if early_stop_enabled {
            rustfs_io_metrics::record_get_object_metadata_early_stop_miss(
                GET_OBJECT_PATH_LEGACY_DUPLEX,
                GET_METADATA_EARLY_STOP_REASON_UNSAFE_REQUEST,
            );
            rustfs_io_metrics::record_get_object_metadata_early_stop_saved_responses(GET_OBJECT_PATH_LEGACY_DUPLEX, 0);
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
    ) -> disk::error::Result<(Vec<FileInfo>, Vec<Option<DiskError>>, MetadataFanoutDiagnostics)> {
        let fanout_start = observe.then(Instant::now);
        let mut ress = Vec::with_capacity(disks.len());
        let mut errors = Vec::with_capacity(disks.len());
        let mut observations = observe.then(|| Vec::with_capacity(disks.len()));
        let opts = Arc::new(ReadOptions {
            incl_free_versions,
            read_data,
            healing,
        });
        let org_bucket = Arc::new(org_bucket.to_string());
        let bucket = Arc::new(bucket.to_string());
        let object = Arc::new(object.to_string());
        let version_id = Arc::new(version_id.to_string());
        let futures = disks.iter().map(|disk| {
            let disk = disk.clone();
            let opts = opts.clone();
            let org_bucket = org_bucket.clone();
            let bucket = bucket.clone();
            let object = object.clone();
            let version_id = version_id.clone();
            tokio::spawn(async move {
                let response_start = observe.then(Instant::now);
                let result = if let Some(disk) = disk {
                    disk.read_version(&org_bucket, &bucket, &object, &version_id, &opts).await
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
    ) -> disk::error::Result<(Vec<FileInfo>, Vec<Option<DiskError>>, MetadataFanoutDiagnostics)> {
        let fanout_start = Instant::now();
        let mut ress = vec![FileInfo::default(); disks.len()];
        let mut errors = vec![None; disks.len()];
        let mut observations = Vec::with_capacity(disks.len());
        let mut accumulator =
            MetadataQuorumAccumulator::new(disks.len(), default_parity_count, true).with_requested_version_id(version_id);
        let opts = Arc::new(ReadOptions {
            incl_free_versions,
            read_data,
            healing,
        });
        let org_bucket = Arc::new(org_bucket.to_string());
        let bucket = Arc::new(bucket.to_string());
        let object = Arc::new(object.to_string());
        let version_id = Arc::new(version_id.to_string());
        let mut join_set = JoinSet::new();

        for (index, disk) in disks.iter().cloned().enumerate() {
            let opts = opts.clone();
            let org_bucket = org_bucket.clone();
            let bucket = bucket.clone();
            let object = object.clone();
            let version_id = version_id.clone();
            join_set.spawn(async move {
                let response_start = Instant::now();
                let result = if let Some(disk) = disk {
                    disk.read_version(&org_bucket, &bucket, &object, &version_id, &opts).await
                } else {
                    Err(DiskError::DiskNotFound)
                };
                (index, result, response_start.elapsed())
            });
        }

        while let Some(result) = join_set.join_next().await {
            match result {
                Ok((index, res, elapsed)) => match res {
                    Ok(file_info) => {
                        observations.push(MetadataFanoutObservation::from_file_info(&file_info, elapsed));
                        accumulator.observe_file_info(&file_info);
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

            if let Some(decision) = accumulator
                .early_stop_decision()
                .or_else(|| accumulator.version_early_stop_decision())
            {
                let saved_responses = join_set.len();
                join_set.abort_all();
                rustfs_io_metrics::record_get_object_metadata_early_stop_hit(GET_OBJECT_PATH_LEGACY_DUPLEX, decision.reason);
                rustfs_io_metrics::record_get_object_metadata_early_stop_saved_responses(
                    GET_OBJECT_PATH_LEGACY_DUPLEX,
                    saved_responses,
                );
                while join_set.join_next().await.is_some() {}
                let diagnostics = MetadataFanoutDiagnostics::new(fanout_start.elapsed(), observations);
                return Ok((ress, errors, diagnostics));
            }
        }

        rustfs_io_metrics::record_get_object_metadata_early_stop_miss(
            GET_OBJECT_PATH_LEGACY_DUPLEX,
            accumulator.final_miss_reason(),
        );
        rustfs_io_metrics::record_get_object_metadata_early_stop_saved_responses(GET_OBJECT_PATH_LEGACY_DUPLEX, 0);
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
        let disks = self.get_disks_internal().await;
        if disks.is_empty() {
            return Err(to_object_err(StorageError::ErasureReadQuorum, vec![bucket, object]));
        }

        let read_quorum = disks.len().div_ceil(2).max(1);
        let (raw_fileinfos, errs) = Self::read_all_raw_file_info(&disks, bucket, object, false).await;

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

        FileMeta {
            versions,
            ..Default::default()
        }
        .get_all_file_info_versions(bucket, object, true)
        .map(Some)
        .map_err(|err| Error::other(format!("exact object versions decode failed for {bucket}/{object}: {err}")))
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
        let vid = match meta.into_fileinfo(bucket, object, "", true, incl_free_vers, true) {
            Ok(finfo) if finfo.is_valid() => finfo.version_id.unwrap_or(Uuid::nil()),
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
                match meta.into_fileinfo(bucket, object, vid.to_string().as_str(), read_data, incl_free_vers, true) {
                    Ok(res) => meta_file_infos[idx] = res,
                    Err(err) => errs[idx] = Some(err.into()),
                }
            }
        }

        (meta_file_infos, errs)
    }

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

        let (ress, errors) = match collect_read_multiple_results(futures, read_quorum).await {
            Ok(collected) => collected,
            Err(()) => return empty_quorum_result(),
        };

        // debug!("ReadMultipleResp ress {:?}", ress);
        // debug!("ReadMultipleResp errors {:?}", errors);

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

fn rename_data_versions_key(versions: &[u8]) -> Option<[u8; 8]> {
    let prefix = versions.get(..8)?;
    let mut key = [0; 8];
    key.copy_from_slice(prefix);
    Some(key)
}

impl SetDisks {
    pub(in crate::set_disk) fn default_read_quorum(&self) -> usize {
        self.set_drive_count - self.default_parity_count
    }

    pub(in crate::set_disk) fn default_write_quorum(&self) -> usize {
        let mut data_count = self.set_drive_count - self.default_parity_count;
        if data_count == self.default_parity_count {
            data_count += 1
        }

        data_count
    }

    #[tracing::instrument(level = "debug", skip(disks, file_infos))]
    #[allow(clippy::type_complexity)]
    pub(in crate::set_disk) async fn rename_data(
        disks: &[Option<DiskStore>],
        src_bucket: &str,
        src_object: &str,
        file_infos: &[FileInfo],
        dst_bucket: &str,
        dst_object: &str,
        write_quorum: usize,
    ) -> disk::error::Result<(Vec<Option<DiskStore>>, Option<Vec<u8>>, Option<Uuid>, Vec<Option<DiskStore>>)> {
        let mut futures = Vec::with_capacity(disks.len());

        let mut errs = Vec::with_capacity(disks.len());

        let src_bucket = Arc::new(src_bucket.to_string());
        let src_object = Arc::new(src_object.to_string());
        let dst_bucket = Arc::new(dst_bucket.to_string());
        let dst_object = Arc::new(dst_object.to_string());

        for (i, (disk, file_info)) in disks.iter().zip(file_infos.iter()).enumerate() {
            let mut file_info = file_info.clone();
            let disk = disk.clone();
            let src_bucket = src_bucket.clone();
            let src_object = src_object.clone();
            let dst_object = dst_object.clone();
            let dst_bucket = dst_bucket.clone();

            futures.push(tokio::spawn(async move {
                if file_info.erasure.index == 0 {
                    file_info.erasure.index = i + 1;
                }

                if !file_info.is_valid() {
                    return Err(DiskError::FileCorrupt);
                }

                if let Some(disk) = disk {
                    disk.rename_data(&src_bucket, &src_object, file_info, &dst_bucket, &dst_object)
                        .await
                } else {
                    Err(DiskError::DiskNotFound)
                }
            }));
        }

        let mut disk_versions = vec![None; disks.len()];
        let mut data_dirs = vec![None; disks.len()];

        let results = join_all(futures).await;

        for (idx, result) in results.iter().enumerate() {
            match result.as_ref().map_err(|_| DiskError::Unexpected)? {
                Ok(res) => {
                    data_dirs[idx] = res.old_data_dir;
                    disk_versions[idx].clone_from(&res.sign);
                    errs.push(None);
                }
                Err(e) => {
                    errs.push(Some(e.clone()));
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
                    let fi = file_infos[i].clone();
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

        let data_dir = Self::reduce_common_data_dir(&data_dirs, write_quorum);
        let versions = Self::select_rename_data_versions(&disk_versions, &errs, write_quorum);
        let online_disks = Self::eval_disks(disks, &errs);
        let cleanup_disks = if let Some(data_dir) = data_dir {
            disks
                .iter()
                .zip(errs.iter())
                .zip(data_dirs.iter())
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

        Ok((online_disks, versions, data_dir, cleanup_disks))
    }

    pub(in crate::set_disk) fn reduce_common_versions(disk_versions: &[Option<Vec<u8>>], write_quorum: usize) -> Option<Vec<u8>> {
        let mut versions_count = HashMap::new();

        for versions in disk_versions.iter().flatten() {
            if let Some(key) = rename_data_versions_key(versions) {
                *versions_count.entry(key).or_insert(0usize) += 1;
            }
        }

        let (common_versions, max_count) = versions_count
            .into_iter()
            .max_by_key(|(_, count)| *count)
            .unwrap_or(([0; 8], 0));

        if max_count < write_quorum {
            return None;
        }

        disk_versions
            .iter()
            .flatten()
            .find(|versions| rename_data_versions_key(versions).is_some_and(|key| key == common_versions))
            .cloned()
    }

    pub(in crate::set_disk) fn select_rename_data_versions(
        disk_versions: &[Option<Vec<u8>>],
        errs: &[Option<DiskError>],
        write_quorum: usize,
    ) -> Option<Vec<u8>> {
        let mut versions = Self::reduce_common_versions(disk_versions, write_quorum);
        for (dversions, err) in disk_versions.iter().zip(errs.iter()) {
            if err.is_some() {
                continue;
            }
            let Some(dversions) = dversions.as_ref().filter(|versions| !versions.is_empty()) else {
                continue;
            };

            match versions.as_ref() {
                Some(current_versions) if dversions != current_versions => {
                    if dversions.len() > current_versions.len() {
                        versions = Some(dversions.clone());
                    }
                    break;
                }
                Some(_) => {}
                None => {
                    versions = Some(dversions.clone());
                    break;
                }
            }
        }

        versions
    }

    #[allow(dead_code)]
    #[tracing::instrument(level = "debug", skip(self, disks))]
    pub(in crate::set_disk) async fn commit_rename_data_dir(
        &self,
        disks: &[Option<DiskStore>],
        bucket: &str,
        object: &str,
        data_dir: &str,
        write_quorum: usize,
    ) -> disk::error::Result<()> {
        let file_path = Arc::new(format!("{object}/{data_dir}"));
        let bucket = Arc::new(bucket.to_string());
        let futures = disks.iter().map(|disk| {
            let file_path = file_path.clone();
            let bucket = bucket.clone();
            let disk = disk.clone();
            tokio::spawn(async move {
                if let Some(disk) = disk {
                    (disk
                        .delete(
                            &bucket,
                            &file_path,
                            DeleteOptions {
                                recursive: true,
                                ..Default::default()
                            },
                        )
                        .await)
                        .err()
                } else {
                    Some(DiskError::DiskNotFound)
                }
            })
        });
        let errs: Vec<Option<DiskError>> = join_all(futures)
            .await
            .into_iter()
            .map(|e| e.unwrap_or(Some(DiskError::Unexpected)))
            .collect();

        if let Some(err) = reduce_write_quorum_errs(&errs, OBJECT_OP_IGNORED_ERRS, write_quorum) {
            return Err(err);
        }

        Ok(())
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
        let src_bucket = Arc::new(src_bucket.to_string());
        let src_object = Arc::new(src_object.to_string());
        let dst_bucket = Arc::new(dst_bucket.to_string());
        let dst_object = Arc::new(dst_object.to_string());

        // Do NOT pre-delete the destination part before renaming: the per-disk
        // `rename_part` replaces `part.N` atomically (std::fs::rename) and rewrites
        // `part.N.meta`, so the pre-delete is redundant — and destructive. It
        // opened a window where an already-committed (ACKed) part was removed on
        // every disk before the new rename landed, so a re-upload that then failed
        // quorum destroyed the committed part outright (backlog#853 / #799 B4).
        // The atomic rename overwrites in place; on quorum failure below we roll
        // the destination back.

        let mut errs = Vec::with_capacity(disks.len());

        let futures = disks.iter().map(|disk| {
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

        if let Some(err) = reduce_write_quorum_errs(&errs, OBJECT_OP_IGNORED_ERRS, write_quorum) {
            if let Some(context) = quorum_context {
                log_multipart_write_quorum_failure(context, &errs, write_quorum, &err);
            } else {
                warn!("rename_part errs {:?}", &errs);
            }
            self.cleanup_multipart_path(&[dst_object.to_string(), format!("{dst_object}.meta")])
                .await;
            return Err(err);
        }

        let disks = Self::eval_disks(disks, &errs);
        Ok(disks)
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
                    let disk = disk.clone();
                    let bucket = bucket.to_string();
                    let path = path_join_buf(&[prefix, STORAGE_FORMAT_FILE]);
                    revert_futures.push(async move {
                        if let Err(err) = disk
                            .delete(
                                &bucket,
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
        if m.is_valid()
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
        if m.is_valid() {
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

        let write_quorum = if m.is_valid() {
            m.write_quorum(self.default_write_quorum())
        } else {
            self.default_write_quorum()
        };
        if let Some(err) = reduce_write_quorum_errs(&delete_errs, OBJECT_OP_IGNORED_ERRS, write_quorum) {
            return Err(err);
        }

        Ok(m)
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
                    Ok(())
                }
            });
        }

        let errs = join_all(futures).await.into_iter().map(|v| v.err()).collect::<Vec<_>>();

        if let Some(err) = reduce_write_quorum_errs(&errs, OBJECT_OP_IGNORED_ERRS, write_quorum) {
            return Err(err);
        }

        Ok(())
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
    ///   unparsable, the object is treated as degraded and NOTHING is removed —
    ///   the unreadable copy could be the only one naming a live data dir, and a
    ///   heal must run first.
    /// * Only subdirectories whose names parse as a UUID are ever considered;
    ///   removal is non-recursive-safe via a recursive delete of the full stray
    ///   data-dir path only.
    ///
    /// Returns the number of stray data directories removed across the set. This
    /// is best-effort maintenance: individual delete failures are logged and
    /// skipped rather than propagated.
    pub(crate) async fn reclaim_orphan_data_dirs(&self, bucket: &str, object: &str) -> disk::error::Result<usize> {
        let disks = self.get_disks_internal().await;

        // Phase 1 (read-only): build the referenced-data-dir union and record the
        // physical UUID subdirectories present on each disk. Abort on any degraded
        // copy so a healable object is never stripped of a referenced data dir.
        let mut referenced: HashSet<Uuid> = HashSet::new();
        let mut per_disk_dirs: Vec<(usize, Vec<Uuid>)> = Vec::new();
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
                per_disk_dirs.push((i, physical));
            }
        }

        // No healthy metadata anywhere: this is not a live object, so surplus dirs
        // (if any) belong to the dangling-object heal path, not here.
        if healthy_metas == 0 {
            return Ok(0);
        }

        // Phase 2: delete every physical data dir not referenced by the union.
        let mut removed = 0usize;
        for (i, physical) in per_disk_dirs {
            let Some(disk) = disks[i].as_ref() else { continue };
            for dir in physical {
                if referenced.contains(&dir) {
                    continue;
                }
                let stray = format!("{object}/{dir}");
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

    pub(in crate::set_disk) async fn check_write_precondition(
        &self,
        bucket: &str,
        object: &str,
        opts: &ObjectOptions,
    ) -> Option<StorageError> {
        let mut opts = opts.clone();

        let http_preconditions = opts.http_preconditions?;
        opts.http_preconditions = None;

        // Never claim a lock here, to avoid deadlock
        // - If no_lock is false, we must have obtained the lock out side of this function
        // - If no_lock is true, we should not obtain locks
        opts.no_lock = true;
        let oi = self.get_object_info(bucket, object, &opts).await;

        match oi {
            Ok(oi) => {
                // If top level is a delete marker proceed to upload.
                if oi.delete_marker {
                    return None;
                }
                let if_none_match = http_preconditions.if_none_match_value().map(str::to_owned);
                let if_match = http_preconditions.if_match_value().map(str::to_owned);
                if should_prevent_write(&oi, if_none_match, if_match) {
                    return Some(StorageError::PreconditionFailed);
                }
            }

            Err(StorageError::VersionNotFound(_, _, _))
            | Err(StorageError::ObjectNotFound(_, _))
            | Err(StorageError::ErasureReadQuorum) => {
                // When the object is not found,
                // - if If-Match is set, we should return 404 NotFound
                // - if If-None-Match is set, we should be able to proceed with the request
                if http_preconditions.if_match_value().is_some() {
                    return Some(StorageError::ObjectNotFound(bucket.to_string(), object.to_string()));
                }
            }

            Err(e) => {
                return Some(e);
            }
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use tokio::io::AsyncReadExt;

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

    fn failed_read_repair_submitter(_request: rustfs_common::heal_channel::HealChannelRequest) -> ReadRepairAdmissionFuture {
        Box::pin(async { ReadRepairAdmissionOutcome::Failed("injected submit failure".to_string()) })
    }

    fn accepted_read_repair_submitter(_request: rustfs_common::heal_channel::HealChannelRequest) -> ReadRepairAdmissionFuture {
        Box::pin(async { ReadRepairAdmissionOutcome::Response(HealAdmissionResult::Accepted) })
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
        assert_eq!(diagnostics.error_responses(), 2);
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
    fn metadata_quorum_accumulator_candidate_quorum_handles_zero_parity_and_invalid_candidates() {
        let accumulator = MetadataQuorumAccumulator::new(4, 0, true);
        let candidate = metadata_test_fileinfo("object");
        assert_eq!(accumulator.candidate_read_quorum(&candidate), Some(4));
        assert_eq!(accumulator.missing_response_quorum(), 4);

        let accumulator = MetadataQuorumAccumulator::new(4, 2, true);
        let mut deleted = candidate.clone();
        deleted.deleted = true;
        assert_eq!(accumulator.candidate_read_quorum(&deleted), None);

        let mut empty = candidate.clone();
        empty.size = 0;
        assert_eq!(accumulator.candidate_read_quorum(&empty), None);

        let mut impossible_parity = candidate;
        impossible_parity.erasure.parity_blocks = 4;
        assert_eq!(accumulator.candidate_read_quorum(&impossible_parity), None);
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
    fn shard_read_costs_for_empty_disk_set_are_empty() {
        assert!(shard_read_costs_for_disks(&[]).is_empty());
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
            },
            accepted_read_repair_submitter,
        )
        .await;

        assert!(reserve_read_repair_heal("bucket", &object, None, 3, 4).await.is_none());
        let key = ReadRepairHealCacheKey::new("bucket", &object, None, 3, 4);
        release_read_repair_heal_reservation(&key).await;
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
