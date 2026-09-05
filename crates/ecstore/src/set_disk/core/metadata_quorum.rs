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

//! Pure metadata quorum and early-stop decisions for `SetDisks` reads.
//!
//! Disk scheduling, coalescing, cancellation, and late shard materialization
//! remain with their existing owners; this module only classifies observations.

use crate::diagnostics::get::{
    GET_METADATA_EARLY_STOP_REASON_CONFLICTING_METADATA, GET_METADATA_EARLY_STOP_REASON_DELETE_MARKER,
    GET_METADATA_EARLY_STOP_REASON_ERROR, GET_METADATA_EARLY_STOP_REASON_INSUFFICIENT_QUORUM,
    GET_METADATA_EARLY_STOP_REASON_NOT_FOUND, GET_METADATA_EARLY_STOP_REASON_UNSAFE_REQUEST,
    GET_METADATA_EARLY_STOP_REASON_VALID_QUORUM, GET_METADATA_EARLY_STOP_REASON_VERSION_MATCH_QUORUM,
    GET_METADATA_EARLY_STOP_REASON_VERSION_NOT_FOUND,
};
use crate::disk::error::DiskError;
use crate::disk::error_reduce::OBJECT_OP_IGNORED_ERRS;
use crate::set_disk::file_info_is_valid_for_metadata;
use rustfs_filemeta::FileInfo;

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
    // Bitset of shard indexes whose metadata matches the candidate. Erasure
    // layouts are capped at 16 shards, so this stays allocation-free on the
    // GET metadata hot path.
    candidate_shard_mask: u16,
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
            candidate_shard_mask: 0,
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
        self.observe_file_info_with_index(None, file_info);
    }

    pub(in crate::set_disk) fn observe_file_info_at(&mut self, disk_index: usize, file_info: &FileInfo) {
        self.observe_file_info_with_index(Some(disk_index), file_info);
    }

    fn observe_file_info_with_index(&mut self, disk_index: Option<usize>, file_info: &FileInfo) {
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
                if let Some(disk_index) = disk_index
                    && let Some(bit) = Self::candidate_shard_bit(candidate, file_info, disk_index)
                {
                    self.candidate_shard_mask |= bit;
                }
            }
            Some(_) => {
                self.conflicting_metadata = true;
            }
            None => {
                self.candidate = Some(file_info.clone());
                self.candidate_votes = 1;
                if let Some(disk_index) = disk_index
                    && let Some(bit) = Self::candidate_shard_bit(file_info, file_info, disk_index)
                {
                    self.candidate_shard_mask |= bit;
                }
            }
        }
    }

    fn candidate_shard_bit(candidate: &FileInfo, file_info: &FileInfo, disk_index: usize) -> Option<u16> {
        let &erasure_index = candidate.erasure.distribution.get(disk_index)?;
        if erasure_index == 0 || erasure_index > u16::BITS as usize || file_info.erasure.index != erasure_index {
            return None;
        }
        Some(1u16 << (erasure_index - 1))
    }

    pub(in crate::set_disk) fn candidate_has_read_reserve(&self) -> bool {
        self.candidate_read_reserve_target()
            .is_some_and(|required| self.candidate_shard_mask.count_ones() as usize >= required)
    }

    pub(in crate::set_disk) fn candidate_read_reserve_target(&self) -> Option<usize> {
        let candidate = self.candidate.as_ref()?;
        Some(
            candidate
                .erasure
                .data_blocks
                .saturating_add(usize::from(candidate.erasure.parity_blocks > 0)),
        )
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

pub(in crate::set_disk) fn is_metadata_fanout_ignored_error(err: &DiskError) -> bool {
    OBJECT_OP_IGNORED_ERRS.iter().any(|ignored| ignored == err)
}
