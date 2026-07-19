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

use crate::key::ObjectDataCacheKey;
use crate::starshard_index::StarshardIdentityIndex;

/// Legacy public identity-index token type.
///
/// The backend uses an internal monotonic `u64` generation instead; keeping this
/// alias preserves the existing public `StarshardIdentityIndex` method shapes.
pub(crate) type ObjectDataCacheKeyToken = usize;
pub(crate) type ObjectDataCacheGeneration = u64;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ObjectDataCacheEvictedGeneration {
    pub(crate) key: ObjectDataCacheKey,
    pub(crate) generation: ObjectDataCacheGeneration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ObjectDataCacheGenerationalInsertResult {
    Inserted { evicted: Vec<ObjectDataCacheEvictedGeneration> },
    Duplicate,
}

/// Result of inserting a cache key into the identity index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObjectDataCacheIndexInsertResult {
    /// The key was inserted into the identity set.
    Inserted {
        /// Oldest keys evicted to keep the identity within its key budget.
        ///
        /// Empty unless the identity was already at `identity_keys_max`; the
        /// caller must drop these keys from the cache.
        evicted_keys: Vec<ObjectDataCacheKey>,
    },
    /// The key was already tracked for the identity; its generation token was refreshed.
    Duplicate,
}

#[derive(Debug, Clone)]
struct TrackedKey {
    key: ObjectDataCacheKey,
    generation: ObjectDataCacheGeneration,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct ObjectDataCacheKeySet {
    keys: Vec<TrackedKey>,
}

impl ObjectDataCacheKeySet {
    pub(crate) fn insert_generation(
        &mut self,
        key: ObjectDataCacheKey,
        generation: ObjectDataCacheGeneration,
        max_keys: usize,
    ) -> ObjectDataCacheGenerationalInsertResult {
        if let Some(existing) = self.keys.iter_mut().find(|existing| existing.key == key) {
            // Refresh the generation so a later eviction of the superseded
            // body cannot remove the key registered for this new body.
            existing.generation = generation;
            return ObjectDataCacheGenerationalInsertResult::Duplicate;
        }

        // Bounded eviction: the Vec preserves insertion order, so evicting from
        // the front drops the oldest keys and keeps hot (recently filled) ones,
        // rather than clearing the whole identity and rejecting the new key.
        let mut evicted = Vec::new();
        while self.keys.len() >= max_keys {
            let tracked = self.keys.remove(0);
            evicted.push(ObjectDataCacheEvictedGeneration {
                key: tracked.key,
                generation: tracked.generation,
            });
        }

        self.keys.push(TrackedKey { key, generation });
        ObjectDataCacheGenerationalInsertResult::Inserted { evicted }
    }

    /// Removes the key only when its tracked generation token matches, so an
    /// eviction notification for an old generation leaves a refreshed key intact.
    pub(crate) fn remove_generation(&mut self, key: &ObjectDataCacheKey, generation: ObjectDataCacheGeneration) -> bool {
        let original_len = self.keys.len();
        self.keys
            .retain(|existing| !(existing.key == *key && existing.generation == generation));
        original_len != self.keys.len()
    }

    pub(crate) fn retain<F>(&mut self, mut keep: F)
    where
        F: FnMut(&ObjectDataCacheKey) -> bool,
    {
        self.keys.retain(|tracked| keep(&tracked.key));
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    pub(crate) fn contains(&self, key: &ObjectDataCacheKey) -> bool {
        self.keys.iter().any(|existing| &existing.key == key)
    }

    pub(crate) fn contains_generation(&self, key: &ObjectDataCacheKey, generation: ObjectDataCacheGeneration) -> bool {
        self.keys
            .iter()
            .any(|existing| &existing.key == key && existing.generation == generation)
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.keys.len()
    }

    pub(crate) fn cloned(&self) -> Vec<ObjectDataCacheKey> {
        self.keys.iter().map(|tracked| tracked.key.clone()).collect()
    }
}

/// Public identity-index façade used by the cache backend.
pub type ObjectDataCacheIdentityIndex = StarshardIdentityIndex;

#[cfg(test)]
mod tests {
    use super::{ObjectDataCacheGenerationalInsertResult, ObjectDataCacheKeySet};
    use crate::key::{ObjectDataCacheBodyVariant, ObjectDataCacheKey};

    fn make_key(id: &str) -> ObjectDataCacheKey {
        ObjectDataCacheKey::new("bucket", "object", Some(id), "etag", 1, ObjectDataCacheBodyVariant::FullObjectPlainV1)
    }

    #[test]
    fn key_set_deduplicates_existing_key() {
        let mut set = ObjectDataCacheKeySet::default();
        let key = make_key("v1");

        let first = set.insert_generation(key.clone(), 1, 4);
        let second = set.insert_generation(key, 2, 4);

        assert_eq!(first, ObjectDataCacheGenerationalInsertResult::Inserted { evicted: Vec::new() });
        assert_eq!(second, ObjectDataCacheGenerationalInsertResult::Duplicate);
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn key_set_bounded_eviction_evicts_oldest_and_inserts_new() {
        let mut set = ObjectDataCacheKeySet::default();
        let key_a = make_key("v1");
        let key_b = make_key("v2");

        let _ = set.insert_generation(key_a.clone(), 1, 1);
        let result = set.insert_generation(key_b.clone(), 2, 1);

        assert!(matches!(
            result,
            ObjectDataCacheGenerationalInsertResult::Inserted { evicted }
                if evicted.len() == 1 && evicted[0].key == key_a && evicted[0].generation == 1
        ));
        // The new key replaces the evicted one instead of the identity being cleared.
        assert!(set.contains(&key_b));
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn key_set_removes_only_matching_generation_token() {
        let mut set = ObjectDataCacheKeySet::default();
        let key = make_key("v1");

        let _ = set.insert_generation(key.clone(), 1, 4);

        // A stale eviction notification carrying the old token must not remove the key.
        assert!(!set.remove_generation(&key, 2));
        assert!(set.contains(&key));

        // The matching token removes the key.
        assert!(set.remove_generation(&key, 1));
        assert!(!set.contains(&key));
    }

    #[test]
    fn key_set_duplicate_refreshes_generation_token() {
        let mut set = ObjectDataCacheKeySet::default();
        let key = make_key("v1");

        let _ = set.insert_generation(key.clone(), 1, 4);
        let _ = set.insert_generation(key.clone(), 2, 4);

        // After the refresh the old token no longer matches.
        assert!(!set.remove_generation(&key, 1));
        assert!(set.remove_generation(&key, 2));
    }
}
