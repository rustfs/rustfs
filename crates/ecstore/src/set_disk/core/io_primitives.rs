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
    BitrotReaderStageMetrics, create_bitrot_reader_with_stage_metrics, create_deferred_bitrot_reader, object_mmap_read_enabled,
};
use crate::set_disk::shard_source::ShardReadCost;
use futures::stream::{FuturesUnordered, StreamExt};
use metrics::counter;
use std::{
    collections::{HashMap, VecDeque},
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

    pub(in crate::set_disk) fn retain_deferred_reader(&mut self, idx: usize, reader: ObjectBitrotReader) {
        self.readers[idx] = Some(reader);
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
        setup.retain_deferred_reader(
            idx,
            create_deferred_bitrot_reader(
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
            ),
        );
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
