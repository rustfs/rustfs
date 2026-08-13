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

use crate::diagnostics::get::{
    GET_SHARD_READ_COST_LOCAL, GET_SHARD_READ_COST_REMOTE, GET_SHARD_READ_COST_SAME_NODE, GET_SHARD_READ_COST_UNKNOWN,
};
use crate::disk::error::Error;
use crate::layout::disks_layout::MAX_ERASURE_SET_DRIVE_COUNT;
use smallvec::SmallVec;

/// Generic codec callers may exceed the production set limit; `SmallVec` then
/// spills without changing slot semantics.
pub(crate) const INLINE_SHARD_SLOTS: usize = MAX_ERASURE_SET_DRIVE_COUNT;
pub(crate) type ShardBuffers = SmallVec<[Option<Vec<u8>>; INLINE_SHARD_SLOTS]>;
pub(crate) type ShardErrors = SmallVec<[Option<Error>; INLINE_SHARD_SLOTS]>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ShardReadCost {
    Local,
    SameNode,
    Remote,
    Unknown,
}

impl ShardReadCost {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Local => GET_SHARD_READ_COST_LOCAL,
            Self::SameNode => GET_SHARD_READ_COST_SAME_NODE,
            Self::Remote => GET_SHARD_READ_COST_REMOTE,
            Self::Unknown => GET_SHARD_READ_COST_UNKNOWN,
        }
    }

    pub(crate) const fn is_low_cost(self) -> bool {
        matches!(self, Self::Local | Self::SameNode)
    }
    pub(crate) const fn is_remote(self) -> bool {
        matches!(self, Self::Remote)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StripeReadState {
    shards: ShardBuffers,
    errors: ShardErrors,
    read_quorum: usize,
}

impl StripeReadState {
    #[cfg(test)]
    pub(crate) fn from_parts(shards: Vec<Option<Vec<u8>>>, errors: Vec<Option<Error>>, read_quorum: usize) -> Self {
        let mut shards = SmallVec::from_vec(shards);
        let mut errors = SmallVec::from_vec(errors);
        let slot_count = shards.len().max(errors.len());
        shards.resize_with(slot_count, || None);
        errors.resize_with(slot_count, || None);
        Self {
            shards,
            errors,
            read_quorum,
        }
    }

    pub(crate) fn with_slot_count(slot_count: usize, read_quorum: usize) -> Self {
        let mut state = Self {
            shards: SmallVec::new(),
            errors: SmallVec::new(),
            read_quorum,
        };
        state.reset(slot_count, read_quorum);
        state
    }

    pub(crate) fn reset(&mut self, slot_count: usize, read_quorum: usize) {
        self.shards.clear();
        self.shards.resize_with(slot_count, || None);
        self.errors.clear();
        self.errors.resize_with(slot_count, || None);
        self.read_quorum = read_quorum;
    }

    pub(crate) fn available_shards(&self) -> usize {
        self.shards.iter().filter(|shard| shard.is_some()).count()
    }

    pub(crate) fn can_decode(&self) -> bool {
        self.available_shards() >= self.read_quorum
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.shards.is_empty()
    }

    pub(crate) fn data_bytes(&self, index: usize) -> Option<&[u8]> {
        self.shards.get(index).and_then(Option::as_deref)
    }

    #[cfg(test)]
    pub(crate) fn error(&self, index: usize) -> Option<&Error> {
        self.errors.get(index).and_then(Option::as_ref)
    }

    pub(crate) fn data_shards_complete(&self, data_shards: usize) -> bool {
        self.shards.len() >= data_shards && self.shards.iter().take(data_shards).all(Option::is_some)
    }

    pub(crate) fn parts_mut(&mut self) -> (&mut ShardBuffers, &mut ShardErrors) {
        (&mut self.shards, &mut self.errors)
    }

    pub(crate) fn shards_mut(&mut self) -> &mut ShardBuffers {
        &mut self.shards
    }

    pub(crate) fn into_parts(self) -> (ShardBuffers, ShardErrors) {
        (self.shards, self.errors)
    }

    #[cfg(test)]
    pub(crate) fn scratch_storage(&self) -> (*const Option<Vec<u8>>, *const Option<Error>, bool, bool) {
        (self.shards.as_ptr(), self.errors.as_ptr(), self.shards.spilled(), self.errors.spilled())
    }

    #[cfg(test)]
    pub(crate) fn shard_allocation(&self, index: usize) -> Option<(*const u8, usize)> {
        self.shards
            .get(index)
            .and_then(|shard| shard.as_ref().map(|shard| (shard.as_ptr(), shard.capacity())))
    }
}

#[async_trait::async_trait]
pub(crate) trait ShardStripeSource: Send {
    async fn read_next_stripe(&mut self) -> Box<StripeReadState>;

    fn recycle_stripe(&mut self, _state: Box<StripeReadState>) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::mem::size_of;

    #[test]
    fn stripe_scratch_capacity_matches_the_production_set_limit() {
        type OversizedShardBuffers = SmallVec<[Option<Vec<u8>>; 32]>;
        type OversizedShardErrors = SmallVec<[Option<Error>; 32]>;

        assert_eq!(INLINE_SHARD_SLOTS, MAX_ERASURE_SET_DRIVE_COUNT);
        assert!(size_of::<ShardBuffers>() < size_of::<OversizedShardBuffers>());
        assert!(size_of::<ShardErrors>() < size_of::<OversizedShardErrors>());
    }

    #[test]
    fn stripe_read_state_tracks_decode_quorum_and_slot_access() {
        let state =
            StripeReadState::from_parts(vec![Some(vec![1]), None, Some(vec![2])], vec![None, Some(Error::FileNotFound), None], 2);

        assert_eq!(state.available_shards(), 2);
        assert!(state.can_decode());
        assert_eq!(state.data_bytes(0), Some(&[1][..]));
        assert_eq!(state.error(1), Some(&Error::FileNotFound));
    }

    #[test]
    fn stripe_read_state_preserves_shards_and_errors() {
        let state = StripeReadState::from_parts(vec![Some(vec![1, 2, 3]), None], vec![None, Some(Error::FileCorrupt)], 2);

        assert!(!state.can_decode());
        let (shards, errors) = state.into_parts();
        assert_eq!(shards.as_slice(), &[Some(vec![1, 2, 3]), None]);
        assert_eq!(errors.as_slice(), &[None, Some(Error::FileCorrupt)]);
    }

    #[test]
    fn shard_read_cost_reports_all_labels_and_remote_classification() {
        assert_eq!(ShardReadCost::Local.as_str(), GET_SHARD_READ_COST_LOCAL);
        assert_eq!(ShardReadCost::SameNode.as_str(), GET_SHARD_READ_COST_SAME_NODE);
        assert_eq!(ShardReadCost::Remote.as_str(), GET_SHARD_READ_COST_REMOTE);
        assert_eq!(ShardReadCost::Unknown.as_str(), GET_SHARD_READ_COST_UNKNOWN);

        assert!(ShardReadCost::Remote.is_remote());
        assert!(!ShardReadCost::Local.is_remote());
        assert!(!ShardReadCost::Unknown.is_low_cost());
    }

    #[test]
    fn stripe_read_state_reports_complete_data_shards_without_parity() {
        let state = StripeReadState::from_parts(vec![Some(vec![1]), Some(vec![2]), None], Vec::new(), 2);

        assert!(state.data_shards_complete(2));
        assert_eq!(state.data_bytes(0), Some(&[1][..]));
        assert_eq!(state.data_bytes(1), Some(&[2][..]));
    }

    #[test]
    fn stripe_read_state_rejects_missing_data_shard_for_complete_fast_path() {
        let state = StripeReadState::from_parts(vec![Some(vec![1]), None, Some(vec![3])], Vec::new(), 2);

        assert!(!state.data_shards_complete(2));
    }
}
