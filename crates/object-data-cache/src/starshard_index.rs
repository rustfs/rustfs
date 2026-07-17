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

use crate::index::{
    ObjectDataCacheGeneration, ObjectDataCacheGenerationalInsertResult, ObjectDataCacheIndexInsertResult, ObjectDataCacheKeySet,
    ObjectDataCacheKeyToken,
};
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
    pub async fn insert(
        &self,
        identity: ObjectDataCacheIdentity,
        key: ObjectDataCacheKey,
        token: ObjectDataCacheKeyToken,
    ) -> ObjectDataCacheIndexInsertResult {
        let generation = u64::try_from(token).unwrap_or(u64::MAX);
        match self.insert_generation(identity, key, generation).await {
            ObjectDataCacheGenerationalInsertResult::Inserted { evicted } => ObjectDataCacheIndexInsertResult::Inserted {
                evicted_keys: evicted.into_iter().map(|entry| entry.key).collect(),
            },
            ObjectDataCacheGenerationalInsertResult::Duplicate => ObjectDataCacheIndexInsertResult::Duplicate,
        }
    }

    pub(crate) async fn insert_generation(
        &self,
        identity: ObjectDataCacheIdentity,
        key: ObjectDataCacheKey,
        generation: ObjectDataCacheGeneration,
    ) -> ObjectDataCacheGenerationalInsertResult {
        let max_keys = self.max_keys_per_identity;
        loop {
            let mut outcome = None;
            {
                let outcome = &mut outcome;
                let key = key.clone();
                let _ = self
                    .by_object
                    .compute_if_present(&identity, move |mut key_set| {
                        let result = key_set.insert_generation(key, generation, max_keys);
                        // Insert never empties the set (it either dedups or
                        // bounded-evicts and adds the new key), but keep the
                        // guard so an already-empty entry is not republished.
                        *outcome = Some(result);
                        (!key_set.is_empty()).then_some(key_set)
                    })
                    .await;
            }
            if let Some(result) = outcome {
                return result;
            }

            // Identity not tracked yet: publish a fresh single-key set.
            let mut fresh = ObjectDataCacheKeySet::default();
            let result = fresh.insert_generation(key.clone(), generation, max_keys);
            let final_set = self.by_object.compute_if_absent(identity.clone(), move || fresh).await;
            if final_set.contains(&key) {
                return result;
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

    /// Removes every tracked identity whose key matches `predicate`, returning
    /// all keys that were dropped so the caller can evict them from the cache.
    ///
    /// This performs a **full shard scan** (`keys()` snapshots every identity,
    /// then each match is removed under its shard write lock). It is therefore
    /// restricted to the rare prefix-delete, bucket-delete, and admin
    /// `clear()`/flush paths — it must never run on the GET or fill hot path.
    /// A `true`-returning predicate clears the whole index (used by `clear()`).
    ///
    /// The snapshot/remove split leaves a small window: an identity inserted
    /// after the snapshot but before removal is not visited, and a key added to
    /// a matched identity between snapshot and its `remove` is still dropped
    /// from the index (via `remove`) but returned for cache eviction, so no
    /// tracked body is stranded. This is acceptable hygiene slack on these rare
    /// paths (backlog#1132/#1133/#1143).
    pub async fn remove_matching<F>(&self, predicate: F) -> Vec<ObjectDataCacheKey>
    where
        F: Fn(&ObjectDataCacheIdentity) -> bool,
    {
        let identities: Vec<ObjectDataCacheIdentity> = self
            .by_object
            .keys()
            .await
            .into_iter()
            .filter(|identity| predicate(identity))
            .collect();

        let mut removed = Vec::new();
        for identity in identities {
            if let Some(key_set) = self.by_object.remove(&identity).await {
                removed.extend(key_set.cloned());
            }
        }
        removed
    }

    /// Removes a single evicted key tracked under an identity, but only when the
    /// evicted entry's generation token still matches the tracked one.
    ///
    /// This lets the cache eviction listener prune keys of genuinely evicted
    /// entries while leaving a key that was refilled under a new generation
    /// intact, so a stale (inline `Expired` upsert or deferred `Size`) eviction
    /// notification cannot silently break the invalidation contract.
    pub async fn remove_evicted_key(
        &self,
        identity: &ObjectDataCacheIdentity,
        key: &ObjectDataCacheKey,
        token: ObjectDataCacheKeyToken,
    ) -> bool {
        self.remove_generation(identity, key, u64::try_from(token).unwrap_or(u64::MAX))
            .await
    }

    pub(crate) async fn remove_generation(
        &self,
        identity: &ObjectDataCacheIdentity,
        key: &ObjectDataCacheKey,
        generation: ObjectDataCacheGeneration,
    ) -> bool {
        let mut removed = false;
        {
            let removed = &mut removed;
            let _ = self
                .by_object
                .compute_if_present(identity, move |mut key_set| {
                    *removed = key_set.remove_generation(key, generation);
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

    #[cfg(test)]
    pub(crate) async fn contains_generation(
        &self,
        identity: &ObjectDataCacheIdentity,
        key: &ObjectDataCacheKey,
        generation: ObjectDataCacheGeneration,
    ) -> bool {
        self.by_object
            .get(identity)
            .await
            .is_some_and(|key_set| key_set.contains_generation(key, generation))
    }

    /// Returns the number of tracked identities.
    #[cfg(test)]
    pub(crate) async fn identity_count(&self) -> usize {
        self.by_object.len().await
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

        let _ = index.insert(identity.clone(), key_a.clone(), 1).await;
        let _ = index.insert(identity.clone(), key_b.clone(), 2).await;
        let removed = index.remove_identity(&identity).await;

        assert_eq!(removed, vec![key_a, key_b]);
    }

    #[tokio::test]
    async fn identity_index_prunes_stale_keys() {
        let index = StarshardIdentityIndex::new(4);
        let identity = identity();
        let key_a = key("v1");
        let key_b = key("v2");

        let _ = index.insert(identity.clone(), key_a.clone(), 1).await;
        let _ = index.insert(identity.clone(), key_b.clone(), 2).await;
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
            handles.push(tokio::spawn(
                async move { index.insert(identity, key(&format!("v{i}")), i as usize).await },
            ));
        }
        for handle in handles {
            handle.await.expect("insert task should complete");
        }

        let removed = index.remove_identity(&identity).await;
        assert_eq!(removed.len(), 32, "no concurrent insert may drop another fill's key");
    }

    #[tokio::test]
    async fn identity_index_bounded_eviction_evicts_oldest() {
        let index = StarshardIdentityIndex::new(1);
        let identity = identity();
        let key_a = key("v1");
        let key_b = key("v2");

        let _ = index.insert(identity.clone(), key_a.clone(), 1).await;
        let result = index.insert(identity.clone(), key_b.clone(), 2).await;
        let removed = index.remove_identity(&identity).await;

        assert!(matches!(
            result,
            ObjectDataCacheIndexInsertResult::Inserted { evicted_keys } if evicted_keys == vec![key_a]
        ));
        // The newest key is retained rather than the identity being cleared.
        assert_eq!(removed, vec![key_b]);
    }

    #[tokio::test]
    async fn identity_index_stale_eviction_preserves_refreshed_key() {
        // Manifestation of ODC-12(2): a deferred eviction notification for an old
        // generation must not break a later invalidate_object for the same key.
        let index = StarshardIdentityIndex::new(4);
        let identity = identity();
        let key_a = key("v1");

        // First fill registers token 1; a refill (same key, new body) refreshes
        // the token to 2.
        let _ = index.insert(identity.clone(), key_a.clone(), 1).await;
        let _ = index.insert(identity.clone(), key_a.clone(), 2).await;

        // A stale eviction notification for the old generation (token 1) arrives.
        let removed = index.remove_evicted_key(&identity, &key_a, 1).await;
        assert!(!removed, "stale-generation eviction must not remove the refreshed key");

        // A later invalidate_object still finds and removes the key.
        let invalidated = index.remove_identity(&identity).await;
        assert_eq!(invalidated, vec![key_a]);
    }

    fn bucketed_key(bucket: &str, object: &str) -> ObjectDataCacheKey {
        ObjectDataCacheKey::new(bucket, object, Some("v1"), "etag", 1, ObjectDataCacheBodyVariant::FullObjectPlainV1)
    }

    #[tokio::test]
    async fn remove_matching_returns_only_predicate_matches() {
        let index = StarshardIdentityIndex::new(4);
        let id_photos = ObjectDataCacheIdentity::new("bucket", "photos/a.jpg");
        let id_photos2 = ObjectDataCacheIdentity::new("bucket", "photos/b.jpg");
        let id_videos = ObjectDataCacheIdentity::new("bucket", "videos/c.mp4");

        let _ = index
            .insert(id_photos.clone(), bucketed_key("bucket", "photos/a.jpg"), 1)
            .await;
        let _ = index
            .insert(id_photos2.clone(), bucketed_key("bucket", "photos/b.jpg"), 2)
            .await;
        let _ = index
            .insert(id_videos.clone(), bucketed_key("bucket", "videos/c.mp4"), 3)
            .await;

        let removed = index
            .remove_matching(|identity| identity.bucket.as_ref() == "bucket" && identity.object.starts_with("photos/"))
            .await;

        assert_eq!(removed.len(), 2, "only the two photos/ identities match the prefix");
        // The unmatched identity is still tracked; the matched ones are gone.
        assert!(index.remove_identity(&id_videos).await.len() == 1);
        assert!(index.remove_identity(&id_photos).await.is_empty());
    }

    #[tokio::test]
    async fn remove_matching_true_predicate_clears_index() {
        let index = StarshardIdentityIndex::new(4);
        let _ = index
            .insert(ObjectDataCacheIdentity::new("b1", "o1"), bucketed_key("b1", "o1"), 1)
            .await;
        let _ = index
            .insert(ObjectDataCacheIdentity::new("b2", "o2"), bucketed_key("b2", "o2"), 2)
            .await;

        let removed = index.remove_matching(|_| true).await;

        assert_eq!(removed.len(), 2);
        assert_eq!(index.identity_count().await, 0, "a true predicate clears every identity");
    }

    #[tokio::test]
    async fn identity_index_matching_eviction_removes_key() {
        let index = StarshardIdentityIndex::new(4);
        let identity = identity();
        let key_a = key("v1");

        let _ = index.insert(identity.clone(), key_a.clone(), 7).await;
        let removed = index.remove_evicted_key(&identity, &key_a, 7).await;

        assert!(removed, "an eviction carrying the tracked token must remove the key");
        assert!(index.remove_identity(&identity).await.is_empty());
    }
}
