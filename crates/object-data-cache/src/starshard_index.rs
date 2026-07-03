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

use crate::index::{ObjectDataCacheIndexInsertResult, ObjectDataCacheKeySet};
use crate::key::{ObjectDataCacheIdentity, ObjectDataCacheKey};
use starshard::{AsyncShardedHashMap, DEFAULT_SHARDS, SnapshotMode};
use std::collections::hash_map::RandomState;
use std::sync::Arc;

/// Async starshard-backed identity -> keys index.
#[derive(Clone)]
pub struct StarshardIdentityIndex {
    by_object: Arc<AsyncShardedHashMap<ObjectDataCacheIdentity, ObjectDataCacheKeySet, RandomState>>,
    max_keys_per_identity: usize,
}

impl std::fmt::Debug for StarshardIdentityIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StarshardIdentityIndex")
            .field("max_keys_per_identity", &self.max_keys_per_identity)
            .finish()
    }
}

impl StarshardIdentityIndex {
    /// Creates a new identity index.
    pub fn new(max_keys_per_identity: usize) -> Self {
        Self {
            by_object: Arc::new(AsyncShardedHashMap::with_shards_and_hasher_and_snapshot_mode(
                DEFAULT_SHARDS,
                RandomState::new(),
                SnapshotMode::Cached,
            )),
            max_keys_per_identity,
        }
    }

    /// Inserts a key into the identity index.
    ///
    /// Runs the read-modify-write under the shard write lock so concurrent
    /// fills for the same identity cannot drop each other's keys.
    pub async fn insert(&self, identity: ObjectDataCacheIdentity, key: ObjectDataCacheKey) -> ObjectDataCacheIndexInsertResult {
        let max_keys = self.max_keys_per_identity;
        loop {
            let mut outcome = None;
            {
                let outcome = &mut outcome;
                let key = key.clone();
                let _ = self
                    .by_object
                    .compute_if_present(&identity, move |mut key_set| {
                        let result = key_set.insert(key, max_keys);
                        let keep = !matches!(result, ObjectDataCacheIndexInsertResult::Overflow { .. }) && !key_set.is_empty();
                        *outcome = Some(result);
                        keep.then_some(key_set)
                    })
                    .await;
            }
            if let Some(result) = outcome {
                return result;
            }

            // Identity not tracked yet: publish a fresh single-key set.
            let mut fresh = ObjectDataCacheKeySet::default();
            let result = fresh.insert(key.clone(), max_keys);
            if !matches!(result, ObjectDataCacheIndexInsertResult::Inserted) {
                return result;
            }
            let final_set = self.by_object.compute_if_absent(identity.clone(), move || fresh).await;
            if final_set.contains(&key) {
                return ObjectDataCacheIndexInsertResult::Inserted;
            }
            // Lost the race to a concurrent insert; retry against the now
            // present entry.
        }
    }

    /// Removes all keys tracked for an identity.
    pub async fn remove_identity(&self, identity: &ObjectDataCacheIdentity) -> Vec<ObjectDataCacheKey> {
        self.by_object
            .remove(identity)
            .await
            .map_or_else(Vec::new, |set| set.cloned())
    }

    /// Removes a single key tracked under an identity.
    pub async fn remove_key(&self, identity: &ObjectDataCacheIdentity, key: &ObjectDataCacheKey) -> bool {
        let mut removed = false;
        {
            let removed = &mut removed;
            let _ = self
                .by_object
                .compute_if_present(identity, move |mut key_set| {
                    *removed = key_set.remove_key(key);
                    (!key_set.is_empty()).then_some(key_set)
                })
                .await;
        }
        removed
    }

    /// Returns whether the identity currently tracks the supplied key.
    pub async fn contains_key(&self, identity: &ObjectDataCacheIdentity, key: &ObjectDataCacheKey) -> bool {
        self.by_object
            .get(identity)
            .await
            .is_some_and(|key_set| key_set.contains(key))
    }

    /// Removes index keys that no longer exist in the cache.
    pub async fn prune_missing<F>(&self, identity: &ObjectDataCacheIdentity, mut key_exists: F)
    where
        F: FnMut(&ObjectDataCacheKey) -> bool,
    {
        let _ = self
            .by_object
            .compute_if_present(identity, move |mut key_set| {
                key_set.retain(|key| key_exists(key));
                (!key_set.is_empty()).then_some(key_set)
            })
            .await;
    }
}

#[cfg(test)]
mod tests {
    use super::StarshardIdentityIndex;
    use crate::index::ObjectDataCacheIndexInsertResult;
    use crate::key::{ObjectDataCacheBodyVariant, ObjectDataCacheIdentity, ObjectDataCacheKey};

    fn identity() -> ObjectDataCacheIdentity {
        ObjectDataCacheIdentity::new("bucket", "object")
    }

    fn key(id: &str) -> ObjectDataCacheKey {
        ObjectDataCacheKey::new("bucket", "object", Some(id), "etag", 1, ObjectDataCacheBodyVariant::FullObjectPlainV1)
    }

    #[tokio::test]
    async fn identity_index_removes_all_keys_for_identity() {
        let index = StarshardIdentityIndex::new(4);
        let identity = identity();
        let key_a = key("v1");
        let key_b = key("v2");

        let _ = index.insert(identity.clone(), key_a.clone()).await;
        let _ = index.insert(identity.clone(), key_b.clone()).await;
        let removed = index.remove_identity(&identity).await;

        assert_eq!(removed, vec![key_a, key_b]);
    }

    #[tokio::test]
    async fn identity_index_prunes_stale_keys() {
        let index = StarshardIdentityIndex::new(4);
        let identity = identity();
        let key_a = key("v1");
        let key_b = key("v2");

        let _ = index.insert(identity.clone(), key_a.clone()).await;
        let _ = index.insert(identity.clone(), key_b.clone()).await;
        index.prune_missing(&identity, |candidate| candidate == &key_b).await;
        let removed = index.remove_identity(&identity).await;

        assert_eq!(removed, vec![key_b]);
    }

    #[tokio::test]
    async fn identity_index_concurrent_inserts_keep_all_keys() {
        let index = StarshardIdentityIndex::new(64);
        let identity = identity();

        let mut handles = Vec::new();
        for i in 0..32 {
            let index = index.clone();
            let identity = identity.clone();
            handles.push(tokio::spawn(async move { index.insert(identity, key(&format!("v{i}"))).await }));
        }
        for handle in handles {
            handle.await.expect("insert task should complete");
        }

        let removed = index.remove_identity(&identity).await;
        assert_eq!(removed.len(), 32, "no concurrent insert may drop another fill's key");
    }

    #[tokio::test]
    async fn identity_index_overflow_clears_identity() {
        let index = StarshardIdentityIndex::new(1);
        let identity = identity();
        let key_a = key("v1");
        let key_b = key("v2");

        let _ = index.insert(identity.clone(), key_a.clone()).await;
        let result = index.insert(identity.clone(), key_b).await;
        let removed = index.remove_identity(&identity).await;

        assert!(matches!(
            result,
            ObjectDataCacheIndexInsertResult::Overflow { cleared_keys } if cleared_keys == vec![key_a]
        ));
        assert!(removed.is_empty());
    }
}
