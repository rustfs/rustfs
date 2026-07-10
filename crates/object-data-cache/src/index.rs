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

/// Per-entry generation token used to tell a freshly refilled body apart from a
/// superseded one under the same key. The token is the cache entry's `Arc`
/// pointer captured at fill time: the eviction listener receives the evicted
/// value and can compare its pointer against the token currently tracked, so a
/// deferred or inline eviction of an old generation cannot remove the index key
/// registered for the current generation.
pub(crate) type ObjectDataCacheKeyToken = usize;

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
    token: ObjectDataCacheKeyToken,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct ObjectDataCacheKeySet {
    keys: Vec<TrackedKey>,
}

impl ObjectDataCacheKeySet {
    pub(crate) fn insert(
        &mut self,
        key: ObjectDataCacheKey,
        token: ObjectDataCacheKeyToken,
        max_keys: usize,
    ) -> ObjectDataCacheIndexInsertResult {
        if let Some(existing) = self.keys.iter_mut().find(|existing| existing.key == key) {
            // Refresh the generation token so a later eviction of the superseded
            // body cannot remove the key registered for this new body.
            existing.token = token;
            return ObjectDataCacheIndexInsertResult::Duplicate;
        }

        // Bounded eviction: the Vec preserves insertion order, so evicting from
        // the front drops the oldest keys and keeps hot (recently filled) ones,
        // rather than clearing the whole identity and rejecting the new key.
        let mut evicted_keys = Vec::new();
        while self.keys.len() >= max_keys {
            evicted_keys.push(self.keys.remove(0).key);
        }

        self.keys.push(TrackedKey { key, token });
        ObjectDataCacheIndexInsertResult::Inserted { evicted_keys }
    }

    /// Removes the key only when its tracked generation token matches, so an
    /// eviction notification for an old generation leaves a refreshed key intact.
    pub(crate) fn remove_evicted_key(&mut self, key: &ObjectDataCacheKey, token: ObjectDataCacheKeyToken) -> bool {
        let original_len = self.keys.len();
        self.keys
            .retain(|existing| !(existing.key == *key && existing.token == token));
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
    use super::{ObjectDataCacheIndexInsertResult, ObjectDataCacheKeySet};
    use crate::key::{ObjectDataCacheBodyVariant, ObjectDataCacheKey};

    fn make_key(id: &str) -> ObjectDataCacheKey {
        ObjectDataCacheKey::new("bucket", "object", Some(id), "etag", 1, ObjectDataCacheBodyVariant::FullObjectPlainV1)
    }

    #[test]
    fn key_set_deduplicates_existing_key() {
        let mut set = ObjectDataCacheKeySet::default();
        let key = make_key("v1");

        let first = set.insert(key.clone(), 1, 4);
        let second = set.insert(key, 2, 4);

        assert_eq!(
            first,
            ObjectDataCacheIndexInsertResult::Inserted {
                evicted_keys: Vec::new()
            }
        );
        assert_eq!(second, ObjectDataCacheIndexInsertResult::Duplicate);
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn key_set_bounded_eviction_evicts_oldest_and_inserts_new() {
        let mut set = ObjectDataCacheKeySet::default();
        let key_a = make_key("v1");
        let key_b = make_key("v2");

        let _ = set.insert(key_a.clone(), 1, 1);
        let result = set.insert(key_b.clone(), 2, 1);

        assert!(matches!(
            result,
            ObjectDataCacheIndexInsertResult::Inserted { evicted_keys } if evicted_keys == vec![key_a]
        ));
        // The new key replaces the evicted one instead of the identity being cleared.
        assert!(set.contains(&key_b));
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn key_set_removes_only_matching_generation_token() {
        let mut set = ObjectDataCacheKeySet::default();
        let key = make_key("v1");

        let _ = set.insert(key.clone(), 1, 4);

        // A stale eviction notification carrying the old token must not remove the key.
        assert!(!set.remove_evicted_key(&key, 2));
        assert!(set.contains(&key));

        // The matching token removes the key.
        assert!(set.remove_evicted_key(&key, 1));
        assert!(!set.contains(&key));
    }

    #[test]
    fn key_set_duplicate_refreshes_generation_token() {
        let mut set = ObjectDataCacheKeySet::default();
        let key = make_key("v1");

        let _ = set.insert(key.clone(), 1, 4);
        let _ = set.insert(key.clone(), 2, 4);

        // After the refresh the old token no longer matches.
        assert!(!set.remove_evicted_key(&key, 1));
        assert!(set.remove_evicted_key(&key, 2));
    }
}
