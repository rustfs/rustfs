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

//! Prefix-level bucket usage for admin/console consumers (rustfs/backlog#1872,
//! MinIO `loadPrefixUsageFromBackend` parity).
//!
//! The per-bucket, per-set `.usage-cache.bin` objects already hold a
//! path-keyed prefix tree; this module reads every set's copy through that
//! set's own object layer (the hash-routed store path would always land on
//! one set), aggregates the overlapping trees, and serves the result from a
//! bounded 30-second cache. Bucket writes poke the cache through the
//! dirty-usage hook so a fresh scan is visible immediately.

use crate::data_usage_define::{DATA_USAGE_CACHE_NAME, DataUsageCache};
use crate::error::ScannerError;
use crate::storage_api::owner::{
    EcstoreSetDisks, EcstoreStore, ecstore_is_reserved_or_invalid_bucket, ecstore_resolve_object_store_handle,
};
use futures::future::join_all;
use rustfs_data_usage::{PrefixUsageEntry, PrefixUsageSummary};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};
use tracing::{debug, warn};

const LOG_COMPONENT_SCANNER: &str = "scanner";
const LOG_SUBSYSTEM_PREFIX_USAGE: &str = "prefix_usage";
const EVENT_PREFIX_USAGE_CACHE_STATE: &str = "prefix_usage_cache_state";

/// How long a computed breakdown stays fresh. MinIO uses the same 30s for
/// its prefix-usage cache; bucket writes additionally invalidate on the spot.
const CACHE_TTL: Duration = Duration::from_secs(30);
/// Hard entry cap for the result cache; exceeded, expired entries go first
/// and the map clears rather than growing past the bound.
const CACHE_MAX_ENTRIES: usize = 128;
/// Per-set cache read budget. The underlying loader retries for up to a
/// minute per attempt on backend errors — far too long for an admin GET, so
/// a slow set degrades to "not reporting" instead of stalling the caller.
const PER_SET_LOAD_TIMEOUT: Duration = Duration::from_secs(5);

/// Aggregated prefix-usage answer across every erasure set.
#[derive(Clone, Debug, PartialEq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BucketPrefixUsageResponse {
    pub bucket: String,
    pub prefix: String,
    pub usage: PrefixUsageSummary,
    /// Every reporting set's prefix entry was compacted: the aggregate is
    /// valid, the sub-prefix breakdown is empty on disk.
    pub compacted: bool,
    /// The sub-prefix breakdown is incomplete: at least one reporting set
    /// had the prefix compacted (or absent while others found it), so its
    /// objects cannot be attributed to a sub-prefix.
    pub sub_prefixes_partial: bool,
    /// The breakdown exceeded the caller's entry limit; largest remain.
    pub truncated: bool,
    pub sub_prefixes: Vec<PrefixUsageEntry>,
    /// Sets whose cache held this bucket and prefix.
    pub sets_reporting: usize,
    pub sets_total: usize,
    /// Newest `last_update` across reporting sets, unix seconds.
    pub last_update_unix_secs: Option<u64>,
}

#[derive(Clone)]
struct CachedResponse {
    computed_at: std::time::Instant,
    response: Arc<BucketPrefixUsageResponse>,
}

/// Cache key: (lowercased bucket, normalized prefix, max entries).
type PrefixUsageCacheKey = (String, String, usize);
type PrefixUsageCacheMap = Option<HashMap<PrefixUsageCacheKey, CachedResponse>>;

static PREFIX_USAGE_CACHE: Mutex<PrefixUsageCacheMap> = Mutex::new(None);

/// Drop cached results for `bucket` (empty string clears everything). Wired
/// into the dirty-usage recording path so a write makes the next prefix
/// query recompute instead of serving up to `CACHE_TTL` seconds of stale
/// numbers.
pub fn invalidate_prefix_usage_cache(bucket: &str) {
    let mut guard = PREFIX_USAGE_CACHE.lock().unwrap_or_else(|poison| poison.into_inner());
    let Some(map) = guard.as_mut() else {
        return;
    };
    if bucket.is_empty() {
        map.clear();
        return;
    }
    map.retain(|(cached_bucket, ..), _| !cached_bucket.eq_ignore_ascii_case(bucket));
}

/// Query prefix usage for `bucket` (arbitrary `prefix`, empty = whole
/// bucket), merging every erasure set's own cache copy. `max_entries` bounds
/// the sub-prefix rows (largest first).
pub async fn bucket_prefix_usage(
    bucket: &str,
    prefix: &str,
    max_entries: usize,
) -> Result<BucketPrefixUsageResponse, ScannerError> {
    if ecstore_is_reserved_or_invalid_bucket(bucket, true) {
        return Err(ScannerError::Other(format!("invalid bucket name: {bucket}")));
    }
    let normalized_prefix = prefix.trim_matches('/').to_string();
    let cache_key = (bucket.to_ascii_lowercase(), normalized_prefix.clone(), max_entries);
    if let Some(response) = lookup_cached(&cache_key) {
        return Ok((*response).clone());
    }

    let store = ecstore_resolve_object_store_handle()
        .ok_or_else(|| ScannerError::Other("object store is not initialized".to_string()))?;
    let response = Arc::new(compute_prefix_usage(store, bucket, &normalized_prefix, max_entries).await);
    store_cached(cache_key, response.clone());
    Ok((*response).clone())
}

async fn compute_prefix_usage(
    store: Arc<EcstoreStore>,
    bucket: &str,
    prefix: &str,
    max_entries: usize,
) -> BucketPrefixUsageResponse {
    let sets: Vec<Arc<EcstoreSetDisks>> = store.all_set_disks();
    let sets_total = sets.len();
    let cache_name = format!("{bucket}/{DATA_USAGE_CACHE_NAME}");

    let per_set = join_all(sets.into_iter().map(|set| {
        let cache_name = cache_name.clone();
        async move {
            let mut cache = DataUsageCache::default();
            // A set that has never scanned this bucket (or cannot be read
            // within the budget) reports nothing — the remaining sets still
            // produce a usable, flagged answer.
            let loaded = match tokio::time::timeout(PER_SET_LOAD_TIMEOUT, cache.load(set, &cache_name)).await {
                Ok(Ok(())) => cache,
                Ok(Err(err)) => {
                    debug!(
                        target: "rustfs::scanner::prefix_usage",
                        event = EVENT_PREFIX_USAGE_CACHE_STATE,
                        component = LOG_COMPONENT_SCANNER,
                        subsystem = LOG_SUBSYSTEM_PREFIX_USAGE,
                        bucket = %bucket,
                        state = "set_load_failed",
                        error = %err,
                        "Prefix usage set cache load failed"
                    );
                    return None;
                }
                Err(_) => {
                    warn!(
                        target: "rustfs::scanner::prefix_usage",
                        event = EVENT_PREFIX_USAGE_CACHE_STATE,
                        component = LOG_COMPONENT_SCANNER,
                        subsystem = LOG_SUBSYSTEM_PREFIX_USAGE,
                        bucket = %bucket,
                        state = "set_load_timeout",
                        "Prefix usage set cache load timed out"
                    );
                    return None;
                }
            };
            if loaded.info.name != bucket {
                // Empty or stale-scoped cache: this set has no data for the bucket.
                return None;
            }
            let last_update = loaded.info.last_update;
            let query = loaded.prefix_usage(bucket, prefix, max_entries);
            Some((query, last_update))
        }
    }))
    .await;

    let mut usage = PrefixUsageSummary::default();
    let mut sub_prefix_map: HashMap<String, PrefixUsageSummary> = HashMap::new();
    let mut sets_reporting = 0usize;
    let mut reporting_but_absent = 0usize;
    let mut any_compacted = false;
    let mut all_compacted = true;
    let mut truncated = false;
    let mut last_update: Option<SystemTime> = None;

    for (query, set_last_update) in per_set.into_iter().flatten() {
        // last_update counts every set that has scanned the bucket, even
        // when the prefix itself is absent on that set.
        if let Some(set_last_update) = set_last_update
            && last_update.map(|current| set_last_update > current).unwrap_or(true)
        {
            last_update = Some(set_last_update);
        }
        let Some(query) = query else {
            // The set knows the bucket but not this prefix: legitimate when
            // the prefix's objects all hash to other sets, but it means the
            // breakdown below cannot attribute that set's (zero) objects.
            reporting_but_absent += 1;
            continue;
        };
        sets_reporting += 1;
        usage.merge(&query.usage);
        if query.compacted {
            any_compacted = true;
        } else {
            all_compacted = false;
        }
        truncated |= query.truncated;
        for entry in query.sub_prefixes {
            sub_prefix_map.entry(entry.prefix).or_default().merge(&entry.usage);
        }
    }

    let mut sub_prefixes: Vec<PrefixUsageEntry> = sub_prefix_map
        .into_iter()
        .map(|(prefix, usage)| PrefixUsageEntry { prefix, usage })
        .collect();
    sub_prefixes.sort_by(|left, right| {
        right
            .usage
            .size
            .cmp(&left.usage.size)
            .then_with(|| left.prefix.cmp(&right.prefix))
    });
    // Merged rows can exceed max_entries only when per-set truncation
    // already flagged; enforce the caller bound on the merged view too.
    if sub_prefixes.len() > max_entries {
        truncated = true;
        sub_prefixes.truncate(max_entries);
    }

    let found = sets_reporting > 0;
    BucketPrefixUsageResponse {
        bucket: bucket.to_string(),
        prefix: prefix.to_string(),
        usage,
        compacted: found && all_compacted,
        sub_prefixes_partial: any_compacted || reporting_but_absent > 0,
        truncated,
        sub_prefixes,
        sets_reporting,
        sets_total,
        last_update_unix_secs: last_update
            .and_then(|time| time.duration_since(SystemTime::UNIX_EPOCH).ok())
            .map(|dur| dur.as_secs()),
    }
}

fn lookup_cached(key: &(String, String, usize)) -> Option<Arc<BucketPrefixUsageResponse>> {
    let mut guard = PREFIX_USAGE_CACHE.lock().unwrap_or_else(|poison| poison.into_inner());
    let map = guard.as_mut()?;
    let cached = map.get(key)?;
    if cached.computed_at.elapsed() > CACHE_TTL {
        map.remove(key);
        return None;
    }
    Some(cached.response.clone())
}

fn store_cached(key: (String, String, usize), response: Arc<BucketPrefixUsageResponse>) {
    let mut guard = PREFIX_USAGE_CACHE.lock().unwrap_or_else(|poison| poison.into_inner());
    let map = guard.get_or_insert_with(HashMap::new);
    // Bound the cache: drop expired entries first, and if the cap is still
    // exceeded clear wholesale — the next queries recompute in milliseconds.
    if map.len() >= CACHE_MAX_ENTRIES {
        map.retain(|_, cached| cached.computed_at.elapsed() <= CACHE_TTL);
        if map.len() >= CACHE_MAX_ENTRIES {
            map.clear();
        }
    }
    map.insert(
        key,
        CachedResponse {
            computed_at: std::time::Instant::now(),
            response,
        },
    );
}

#[cfg(test)]
mod tests {
    use super::{CACHE_MAX_ENTRIES, PREFIX_USAGE_CACHE, invalidate_prefix_usage_cache, store_cached};
    use rustfs_data_usage::PrefixUsageSummary;

    fn response(bucket: &str) -> super::BucketPrefixUsageResponse {
        super::BucketPrefixUsageResponse {
            bucket: bucket.to_string(),
            prefix: String::new(),
            usage: PrefixUsageSummary::default(),
            compacted: false,
            sub_prefixes_partial: false,
            truncated: false,
            sub_prefixes: Vec::new(),
            sets_reporting: 1,
            sets_total: 1,
            last_update_unix_secs: None,
        }
    }

    fn seed(bucket: &str, prefix: &str) {
        store_cached(
            (bucket.to_ascii_lowercase(), prefix.to_string(), 10),
            std::sync::Arc::new(response(bucket)),
        );
    }

    fn contains(bucket: &str, prefix: &str) -> bool {
        PREFIX_USAGE_CACHE
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .as_ref()
            .is_some_and(|map| map.contains_key(&(bucket.to_ascii_lowercase(), prefix.to_string(), 10)))
    }

    /// All cache tests run inside one test to keep the process-global map
    /// free of cross-test ordering (the flake class this module avoids).
    #[test]
    fn invalidation_scopes_to_bucket_and_cache_stays_bounded() {
        invalidate_prefix_usage_cache("");
        seed("alpha", "x");
        seed("beta", "y");

        // Case-insensitive bucket scoping.
        invalidate_prefix_usage_cache("ALPHA");
        assert!(!contains("alpha", "x"));
        assert!(contains("beta", "y"));

        // Wholesale clear.
        invalidate_prefix_usage_cache("");
        assert!(!contains("beta", "y"));

        // Hard cap: overflow clears rather than grows.
        for index in 0..=(CACHE_MAX_ENTRIES / 2) {
            let bucket = format!("cap-bucket-{index}");
            seed(&bucket, "a");
            seed(&bucket, "b");
        }
        let guard = PREFIX_USAGE_CACHE.lock().unwrap_or_else(|poison| poison.into_inner());
        let map = guard.as_ref().expect("seeded");
        assert!(map.len() <= CACHE_MAX_ENTRIES, "cache must stay bounded, got {}", map.len());
    }
}
