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

/// One disk's observation. Pending is neither a successful vote nor an offline
/// disk: the scheduler may not have issued this slot or may still be waiting.
// Option<Result<...>> keeps FileInfo inline without a per-response Box.
// None is pending; Some(Ok(_)) is success; Some(Err(_)) retains a disk error.
pub(in crate::set_disk) type MetadataDiskResult = Option<crate::disk::error::Result<FileInfo>>;

#[derive(Debug)]
pub(in crate::set_disk) struct MetadataDiskObservation {
    pub(in crate::set_disk) disk_index: usize,
    pub(in crate::set_disk) result: MetadataDiskResult,
}

impl MetadataDiskObservation {
    pub(in crate::set_disk) fn file_info(&self) -> Option<&FileInfo> {
        match &self.result {
            Some(Ok(metadata)) => Some(metadata),
            None | Some(Err(_)) => None,
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
    pub(in crate::set_disk) fn observe(&mut self, observation: &MetadataDiskObservation) {
        match &observation.result {
            None => {}
            Some(Ok(metadata)) => self.observe_file_info_at(observation.disk_index, metadata),
            Some(Err(error)) => self.observe_error(error),
        }
    }

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

    #[cfg(test)]
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::set_disk::SetDisks;
    use time::OffsetDateTime;
    use uuid::Uuid;

    fn payload(disk_index: usize) -> FileInfo {
        let mut metadata = FileInfo::new("object", 2, 2);
        metadata.volume = "bucket".to_string();
        metadata.name = "object".to_string();
        metadata.size = 1;
        metadata.mod_time = Some(OffsetDateTime::UNIX_EPOCH);
        metadata.data_dir = Some(Uuid::from_u128(1));
        metadata.erasure.index = metadata.erasure.distribution[disk_index];
        metadata.metadata.insert("etag".to_string(), "etag".to_string());
        metadata.add_object_part(1, "etag".to_string(), 1, None, 1, None, None);
        assert!(file_info_is_valid_for_metadata(&metadata), "the observation corpus needs a valid payload");
        metadata
    }

    fn observation(disk_index: usize, state: usize) -> MetadataDiskObservation {
        MetadataDiskObservation {
            disk_index,
            result: match state {
                0 => Some(Ok(payload(disk_index))),
                1 => Some(Err(DiskError::FileNotFound)),
                2 => Some(Err(DiskError::FileCorrupt)),
                3 => Some(Err(DiskError::DiskNotFound)),
                4 => None,
                _ => panic!("unexpected test state"),
            },
        }
    }

    fn assert_same_reduction(typed: &MetadataQuorumAccumulator, legacy: &MetadataQuorumAccumulator) {
        assert_eq!(typed.early_stop_decision(), legacy.early_stop_decision());
        assert_eq!(typed.version_early_stop_decision(), legacy.version_early_stop_decision());
        assert_eq!(typed.final_miss_reason(), legacy.final_miss_reason());
        assert_eq!(typed.valid_responses, legacy.valid_responses);
        assert_eq!(typed.not_found_responses, legacy.not_found_responses);
        assert_eq!(typed.version_not_found_responses, legacy.version_not_found_responses);
        assert_eq!(typed.ignored_errors, legacy.ignored_errors);
        assert_eq!(typed.hard_errors, legacy.hard_errors);
        assert_eq!(typed.candidate_votes, legacy.candidate_votes);
        assert_eq!(typed.candidate_shard_mask, legacy.candidate_shard_mask);
        assert_eq!(typed.matching_version_votes, legacy.matching_version_votes);
        assert_eq!(typed.delete_marker_votes, legacy.delete_marker_votes);
        assert_eq!(typed.conflicting_metadata, legacy.conflicting_metadata);
        assert_eq!(
            typed.candidate.as_ref().map(SetDisks::file_info_quorum_hash),
            legacy.candidate.as_ref().map(SetDisks::file_info_quorum_hash)
        );
    }

    fn observe_legacy(accumulator: &mut MetadataQuorumAccumulator, observation: &MetadataDiskObservation) {
        match &observation.result {
            None => {}
            Some(Ok(metadata)) => accumulator.observe_file_info_at(observation.disk_index, metadata),
            Some(Err(error)) => accumulator.observe_error(error),
        }
    }

    #[test]
    fn metadata_observation_all_four_slot_states_and_arrival_orders_preserve_reduction() {
        let permutations = (0..4)
            .flat_map(|a| (0..4).filter(move |b| *b != a).map(move |b| (a, b)))
            .flat_map(|(a, b)| (0..4).filter(move |c| *c != a && *c != b).map(move |c| (a, b, c)))
            .map(|(a, b, c)| [a, b, c, 6 - a - b - c])
            .collect::<Vec<_>>();
        assert_eq!(permutations.len(), 24);
        // 5^4 states and all 4! arrivals, both with early-stop enabled and disabled.
        for encoded in 0..625usize {
            let states = [encoded % 5, encoded / 5 % 5, encoded / 25 % 5, encoded / 125 % 5];
            let inputs = std::array::from_fn::<_, 4, _>(|index| observation(index, states[index]));
            for enabled in [false, true] {
                for order in &permutations {
                    let mut typed = MetadataQuorumAccumulator::new(4, 2, enabled);
                    let mut legacy = MetadataQuorumAccumulator::new(4, 2, enabled);
                    for &index in order {
                        typed.observe(&inputs[index]);
                        observe_legacy(&mut legacy, &inputs[index]);
                        assert_same_reduction(&typed, &legacy);
                    }
                    assert_eq!(typed.valid_responses, states.iter().filter(|&&state| state == 0).count());
                    assert_eq!(typed.not_found_responses, states.iter().filter(|&&state| state == 1).count());
                    assert_eq!(typed.hard_errors, states.iter().filter(|&&state| state == 2).count());
                    assert_eq!(typed.ignored_errors, states.iter().filter(|&&state| state == 3).count());
                    let expected_early_stop =
                        enabled && typed.valid_responses >= 3 && !states.iter().any(|&state| state == 1 || state == 2);
                    assert_eq!(
                        typed.early_stop_decision().is_some(),
                        expected_early_stop,
                        "states={states:?}, order={order:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn metadata_observation_mixed_current_identity_arrivals_preserve_selection() {
        let permutations = (0..4)
            .flat_map(|a| (0..4).filter(move |b| *b != a).map(move |b| (a, b)))
            .flat_map(|(a, b)| (0..4).filter(move |c| *c != a && *c != b).map(move |c| (a, b, c)))
            .map(|(a, b, c)| [a, b, c, 6 - a - b - c])
            .collect::<Vec<_>>();
        assert_eq!(permutations.len(), 24);

        for (case, newer_version) in [("newer-current-version", true), ("same-time-different-directory", false)] {
            let metadata = std::array::from_fn::<_, 4, _>(|index| {
                let mut file_info = payload(index);
                file_info.version_id = Some(Uuid::from_u128(if index > 0 && newer_version { 11 } else { 10 }));
                file_info.is_latest = true;
                if index > 0 {
                    file_info.data_dir = Some(Uuid::from_u128(2));
                    if newer_version {
                        file_info.mod_time = Some(OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(1));
                    }
                }
                assert!(file_info_is_valid_for_metadata(&file_info), "case={case}, disk={index}");
                file_info
            });
            let expected_current = &metadata[1];
            let expected_hash = SetDisks::file_info_quorum_hash(expected_current);
            assert_ne!(SetDisks::file_info_quorum_hash(&metadata[0]), expected_hash);

            for enabled in [false, true] {
                for order in &permutations {
                    let mut slots = std::array::from_fn::<_, 4, _>(|index| observation(index, 4));
                    let mut typed = MetadataQuorumAccumulator::new(4, 2, enabled);
                    let mut legacy = MetadataQuorumAccumulator::new(4, 2, enabled);
                    for slot in &slots {
                        typed.observe(slot);
                        observe_legacy(&mut legacy, slot);
                    }
                    assert_same_reduction(&typed, &legacy);
                    assert_eq!(typed.valid_responses, 0, "pending slots cannot supply votes");

                    let mut old_seen = false;
                    let mut current_votes = 0;
                    for (prefix, &index) in order.iter().enumerate() {
                        slots[index].result = Some(Ok(metadata[index].clone()));
                        typed.observe(&slots[index]);
                        observe_legacy(&mut legacy, &slots[index]);
                        assert_same_reduction(&typed, &legacy);
                        if index == 0 {
                            old_seen = true;
                        } else {
                            current_votes += 1;
                        }

                        // A+B+B must wait for pending B; three B responses may
                        // finish early only before the conflicting A arrives.
                        let expected_decision = if enabled && !old_seen && current_votes == 3 {
                            Some(MetadataEarlyStopDecision {
                                reason: GET_METADATA_EARLY_STOP_REASON_VALID_QUORUM,
                            })
                        } else {
                            None
                        };
                        assert_eq!(
                            typed.early_stop_decision(),
                            expected_decision,
                            "case={case}, enabled={enabled}, order={order:?}, prefix={prefix}"
                        );
                        assert_eq!(typed.valid_responses, prefix + 1);
                        assert_eq!(typed.conflicting_metadata, old_seen && current_votes > 0);
                        assert_eq!(typed.version_early_stop_decision(), None, "current reads do not request a version ID");
                        if expected_decision.is_some() {
                            assert_eq!(
                                SetDisks::file_info_quorum_hash(
                                    typed.candidate.as_ref().expect("three B votes need a candidate")
                                ),
                                expected_hash
                            );
                        }
                    }

                    let completed_metadata = slots
                        .iter()
                        .map(|slot| slot.file_info().expect("every disk must have arrived").clone())
                        .collect::<Vec<_>>();
                    let (_, selected, selection_quorum) =
                        SetDisks::select_valid_fileinfo(&vec![None; 4], &completed_metadata, &vec![None; 4], "", 2, 3)
                            .expect("three matching B disks must select the current identity");
                    assert_eq!(selection_quorum, 3);
                    assert_eq!(selected.version_id, expected_current.version_id);
                    assert_eq!(selected.mod_time, expected_current.mod_time);
                    assert_eq!(selected.data_dir, expected_current.data_dir);
                    assert_eq!(SetDisks::file_info_quorum_hash(&selected), expected_hash);
                }
            }
        }
    }

    #[test]
    fn metadata_observation_pending_newer_version_and_same_time_different_directory_force_full_wait() {
        let mut accumulator = MetadataQuorumAccumulator::new(4, 2, true);
        for index in 0..2 {
            accumulator.observe(&observation(index, 0));
        }
        accumulator.observe(&observation(2, 4));
        assert_eq!(accumulator.candidate_votes, 2, "an outstanding response cannot supply the deciding vote");
        assert_eq!(accumulator.early_stop_decision(), None);

        let mut newer = payload(2);
        newer.mod_time = Some(OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(1));
        accumulator.observe(&MetadataDiskObservation {
            disk_index: 2,
            result: Some(Ok(newer)),
        });
        assert!(accumulator.conflicting_metadata);
        assert_eq!(accumulator.early_stop_decision(), None);

        let mut same_time_different_directory = payload(3);
        same_time_different_directory.data_dir = Some(Uuid::from_u128(2));
        accumulator.observe(&MetadataDiskObservation {
            disk_index: 3,
            result: Some(Ok(same_time_different_directory)),
        });
        assert_eq!(accumulator.early_stop_decision(), None);
    }

    #[test]
    fn metadata_observation_duplicate_shard_cannot_supply_a_read_reserve() {
        let mut accumulator = MetadataQuorumAccumulator::new(4, 2, true);
        let first = payload(0);
        for disk_index in 0..3 {
            accumulator.observe(&MetadataDiskObservation {
                disk_index,
                result: Some(Ok(first.clone())),
            });
        }
        assert_eq!(accumulator.candidate_votes, 3);
        assert!(
            !accumulator.candidate_has_read_reserve(),
            "copied erasure indexes cannot supply independent data shards"
        );
    }

    #[test]
    fn metadata_observation_null_versions_markers_and_invalid_success_keep_their_meaning() {
        let mut null_version = MetadataQuorumAccumulator::new(4, 2, true);
        for index in 0..3 {
            null_version.observe(&observation(index, 0));
        }
        assert!(null_version.early_stop_decision().is_some());
        assert_eq!(null_version.matching_version_votes, 0);

        let mut marker = MetadataQuorumAccumulator::new(4, 2, true);
        for disk_index in 0..3 {
            let metadata = FileInfo {
                volume: "bucket".to_string(),
                name: "object".to_string(),
                deleted: true,
                mod_time: Some(OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(1)),
                ..Default::default()
            };
            assert!(metadata.is_canonical_delete_marker(), "the null marker fixture must reach marker voting");
            marker.observe(&MetadataDiskObservation {
                disk_index,
                result: Some(Ok(metadata)),
            });
        }
        assert_eq!(marker.delete_marker_votes, 3);
        assert!(marker.early_stop_decision().is_some());

        let mut invalid = MetadataQuorumAccumulator::new(4, 2, true);
        invalid.observe(&MetadataDiskObservation {
            disk_index: 0,
            result: Some(Ok(FileInfo::default())),
        });
        assert_eq!(invalid.valid_responses, 0);
        assert_eq!(invalid.hard_errors, 1);
        assert_eq!(invalid.early_stop_decision(), None);
    }
}
