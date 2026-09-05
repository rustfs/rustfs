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

//! Per-bucket cache of keys the source answered 404 for
//! (rustfs/backlog#2152). A hit short-circuits the source lookup for
//! `policy.negative_cache_ttl_secs`; a TTL of zero disables the cache.
//!
//! Entries are never invalidated on a local PUT: once the object exists
//! locally the handler never consults ODM for it, so a stale negative entry
//! is harmless.

use std::time::Duration;

/// Upper bound on remembered keys per bucket; LRU eviction beyond it.
pub const NEGATIVE_CACHE_MAX_ENTRIES: u64 = 100_000;

#[derive(Debug)]
pub struct NegativeCache {
    cache: Option<moka::sync::Cache<String, ()>>,
    ttl: Duration,
}

impl NegativeCache {
    /// `ttl == 0` builds a disabled cache that never records anything.
    pub fn new(ttl: Duration) -> Self {
        Self::with_capacity(ttl, NEGATIVE_CACHE_MAX_ENTRIES)
    }

    pub fn with_capacity(ttl: Duration, max_entries: u64) -> Self {
        let cache = (!ttl.is_zero()).then(|| {
            moka::sync::Cache::builder()
                .max_capacity(max_entries)
                .time_to_live(ttl)
                .build()
        });
        Self { cache, ttl }
    }

    pub fn is_enabled(&self) -> bool {
        self.cache.is_some()
    }

    pub fn ttl(&self) -> Duration {
        self.ttl
    }

    /// Whether `key` is currently remembered as absent on the source.
    pub fn contains(&self, key: &str) -> bool {
        self.cache.as_ref().is_some_and(|cache| cache.get(key).is_some())
    }

    /// Remembers `key` as absent; no-op when disabled.
    pub fn insert(&self, key: &str) {
        if let Some(cache) = &self.cache {
            cache.insert(key.to_string(), ());
        }
    }

    /// Forgets `key` (e.g. after an admin-triggered backfill found it).
    pub fn remove(&self, key: &str) {
        if let Some(cache) = &self.cache {
            cache.invalidate(key);
        }
    }

    /// Approximate live entry count, for status snapshots only.
    pub fn len(&self) -> u64 {
        self.cache.as_ref().map_or(0, |cache| {
            cache.run_pending_tasks();
            cache.entry_count()
        })
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entry_expires_after_ttl() {
        let cache = NegativeCache::new(Duration::from_millis(80));
        assert!(cache.is_enabled());
        cache.insert("a/x");
        assert!(cache.contains("a/x"));
        assert!(!cache.contains("a/y"));
        std::thread::sleep(Duration::from_millis(160));
        assert!(!cache.contains("a/x"), "entry must expire after the TTL");
    }

    #[test]
    fn zero_ttl_disables_the_cache() {
        let cache = NegativeCache::new(Duration::ZERO);
        assert!(!cache.is_enabled());
        cache.insert("a/x");
        assert!(!cache.contains("a/x"));
        assert!(cache.is_empty());
    }

    #[test]
    fn remove_forgets_a_key() {
        let cache = NegativeCache::new(Duration::from_secs(30));
        cache.insert("a/x");
        cache.remove("a/x");
        assert!(!cache.contains("a/x"));
    }

    #[test]
    fn capacity_bounds_entries() {
        let cache = NegativeCache::with_capacity(Duration::from_secs(30), 4);
        for i in 0..64 {
            cache.insert(&format!("k{i}"));
        }
        assert!(cache.len() <= 4, "len {} exceeds capacity", cache.len());
    }
}
