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

use crate::disk::error::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ShardSlot {
    index: usize,
    data: Option<Vec<u8>>,
    error: Option<Error>,
}

impl ShardSlot {
    pub(crate) fn new(index: usize, data: Option<Vec<u8>>, error: Option<Error>) -> Self {
        Self { index, data, error }
    }

    pub(crate) fn data(index: usize, data: Vec<u8>) -> Self {
        Self::new(index, Some(data), None)
    }

    pub(crate) fn missing(index: usize, error: Error) -> Self {
        Self::new(index, None, Some(error))
    }

    pub(crate) fn index(&self) -> usize {
        self.index
    }

    pub(crate) fn has_data(&self) -> bool {
        self.data.is_some()
    }

    pub(crate) fn data_bytes(&self) -> Option<&[u8]> {
        self.data.as_deref()
    }

    pub(crate) fn error(&self) -> Option<&Error> {
        self.error.as_ref()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StripeReadState {
    slots: Vec<ShardSlot>,
    read_quorum: usize,
}

impl StripeReadState {
    pub(crate) fn new(slots: Vec<ShardSlot>, read_quorum: usize) -> Self {
        Self { slots, read_quorum }
    }

    pub(crate) fn from_parts(shards: Vec<Option<Vec<u8>>>, errors: Vec<Option<Error>>, read_quorum: usize) -> Self {
        let slot_count = shards.len().max(errors.len());
        let mut slots = Vec::with_capacity(slot_count);
        let mut shards = shards.into_iter();
        let mut errors = errors.into_iter();
        for index in 0..slot_count {
            slots.push(ShardSlot::new(index, shards.next().flatten(), errors.next().flatten()));
        }
        Self::new(slots, read_quorum)
    }

    pub(crate) fn available_shards(&self) -> usize {
        self.slots.iter().filter(|slot| slot.has_data()).count()
    }

    pub(crate) fn can_decode(&self) -> bool {
        self.available_shards() >= self.read_quorum
    }

    pub(crate) fn slots(&self) -> &[ShardSlot] {
        &self.slots
    }

    pub(crate) fn slot_by_index(&self, index: usize) -> Option<&ShardSlot> {
        if let Some(slot) = self.slots.get(index)
            && slot.index == index
        {
            return Some(slot);
        }
        self.slots.iter().find(|slot| slot.index == index)
    }

    pub(crate) fn data_shards_complete(&self, data_shards: usize) -> bool {
        (0..data_shards).all(|index| self.slot_by_index(index).is_some_and(ShardSlot::has_data))
    }

    pub(crate) fn into_parts(self) -> (Vec<Option<Vec<u8>>>, Vec<Option<Error>>) {
        let part_count = self.slots.iter().map(|slot| slot.index).max().map_or(0, |index| index + 1);
        let mut shards = Vec::with_capacity(part_count);
        shards.resize_with(part_count, || None);
        let mut errors = Vec::with_capacity(part_count);
        errors.resize_with(part_count, || None);
        for slot in self.slots {
            shards[slot.index] = slot.data;
            errors[slot.index] = slot.error;
        }
        (shards, errors)
    }
}

#[async_trait::async_trait]
pub(crate) trait ShardStripeSource: Send {
    async fn read_next_stripe(&mut self) -> StripeReadState;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stripe_read_state_tracks_decode_quorum() {
        let state = StripeReadState::new(
            vec![
                ShardSlot::data(0, vec![1]),
                ShardSlot::missing(1, Error::FileNotFound),
                ShardSlot::data(2, vec![2]),
            ],
            2,
        );

        assert_eq!(state.available_shards(), 2);
        assert!(state.can_decode());
        assert_eq!(state.slots()[1].index(), 1);
    }

    #[test]
    fn stripe_read_state_preserves_shards_and_errors() {
        let state = StripeReadState::new(vec![ShardSlot::missing(1, Error::FileCorrupt), ShardSlot::data(0, vec![1, 2, 3])], 2);

        assert!(!state.can_decode());
        let (shards, errors) = state.into_parts();
        assert_eq!(shards, vec![Some(vec![1, 2, 3]), None]);
        assert_eq!(errors, vec![None, Some(Error::FileCorrupt)]);
    }

    #[test]
    fn stripe_read_state_builds_slots_from_parallel_reader_parts() {
        let state =
            StripeReadState::from_parts(vec![Some(vec![1]), None, Some(vec![3])], vec![None, Some(Error::FileNotFound)], 2);

        assert!(state.can_decode());
        assert_eq!(state.slots()[1].index(), 1);
        assert_eq!(state.slots()[1].error(), Some(&Error::FileNotFound));
    }

    #[test]
    fn stripe_read_state_reports_complete_data_shards_without_parity() {
        let state = StripeReadState::from_parts(vec![Some(vec![1]), Some(vec![2]), None], Vec::new(), 2);

        assert!(state.data_shards_complete(2));
        assert_eq!(state.slots()[0].data_bytes(), Some(&[1][..]));
        assert_eq!(state.slot_by_index(1).and_then(ShardSlot::data_bytes), Some(&[2][..]));
    }

    #[test]
    fn stripe_read_state_rejects_missing_data_shard_for_complete_fast_path() {
        let state = StripeReadState::from_parts(vec![Some(vec![1]), None, Some(vec![3])], Vec::new(), 2);

        assert!(!state.data_shards_complete(2));
    }

    #[test]
    fn stripe_read_state_finds_out_of_order_slots_by_index() {
        let state = StripeReadState::new(vec![ShardSlot::data(2, vec![3]), ShardSlot::data(0, vec![1])], 2);

        assert_eq!(state.slot_by_index(0).and_then(ShardSlot::data_bytes), Some(&[1][..]));
        assert!(state.slot_by_index(1).is_none());
    }
}
