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
    pub async fn insert(&self, identity: ObjectDataCacheIdentity, key: ObjectDataCacheKey) -> ObjectDataCacheIndexInsertResult {
        let mut key_set = self.by_object.get(&identity).await.unwrap_or_default();
        let result = key_set.insert(key, self.max_keys_per_identity);

        match &result {
            ObjectDataCacheIndexInsertResult::Overflow { .. } => {
                let _ = self.by_object.remove(&identity).await;
            }
            _ if key_set.is_empty() => {
                let _ = self.by_object.remove(&identity).await;
            }
            _ => {
                let _ = self.by_object.insert(identity, key_set).await;
            }
        }

        result
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
        let Some(mut key_set) = self.by_object.get(identity).await else {
            return false;
        };

        let removed = key_set.remove_key(key);
        if !removed {
            return false;
        }

        if key_set.is_empty() {
            let _ = self.by_object.remove(identity).await;
        } else {
            let _ = self.by_object.insert(identity.clone(), key_set).await;
        }

        true
    }

    /// Removes index keys that no longer exist in the cache.
    pub async fn prune_missing<F>(&self, identity: &ObjectDataCacheIdentity, mut key_exists: F)
    where
        F: FnMut(&ObjectDataCacheKey) -> bool,
    {
        let Some(mut key_set) = self.by_object.get(identity).await else {
            return;
        };

        key_set.retain(|key| key_exists(key));

        if key_set.is_empty() {
            let _ = self.by_object.remove(identity).await;
        } else {
            let _ = self.by_object.insert(identity.clone(), key_set).await;
        }
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
