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

//! Lockless cache buckets.
//!
//! A bucket named in `RUSTFS_LOCKLESS_CACHE_BUCKETS` holds a content-addressed
//! cache (an LLM KV cache): a key always names the same bytes, and a lost or
//! unreadable object only costs a recompute. PutObject, GetObject and HeadObject
//! on such a bucket take no distributed locks and read no bucket metadata from
//! disk, so concurrent writers into one bucket no longer meet on bucket-wide
//! lock keys.
//!
//! What a listed bucket gives up, by design:
//! - Writes of one key are not serialized. Racing commits of the same bytes can
//!   leave the object without read quorum until it is rewritten or healed,
//!   which a cache treats as a miss.
//! - Reads are not fenced against a concurrent overwrite or delete.
//! - Writes are not fenced against bucket deletion, Object Lock, quota,
//!   versioning, replication or table-catalog changes made while they run. A
//!   PUT falls back to the locked path whenever the cached bucket state shows
//!   any of those features, but listed buckets must not enable them.
//!
//! The list is read once per process; RustFS system buckets are never lockless.

use super::utils::is_meta_bucketname;
use std::collections::HashSet;
use std::sync::OnceLock;

/// Buckets named in a comma-separated list, without blanks or system buckets.
fn parse_lockless_cache_buckets(value: &str) -> HashSet<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|bucket| !bucket.is_empty() && !is_meta_bucketname(bucket))
        .map(str::to_owned)
        .collect()
}

/// Whether `bucket` is configured as a lockless cache bucket.
pub fn is_lockless_cache_bucket(bucket: &str) -> bool {
    static BUCKETS: OnceLock<HashSet<String>> = OnceLock::new();
    BUCKETS
        .get_or_init(|| {
            rustfs_utils::get_env_opt_str(rustfs_config::ENV_LOCKLESS_CACHE_BUCKETS)
                .map(|value| parse_lockless_cache_buckets(&value))
                .unwrap_or_default()
        })
        .contains(bucket)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_keeps_named_user_buckets_only() {
        let buckets = parse_lockless_cache_buckets(" kvcache-a,,kvcache-b , .rustfs.sys ,");
        assert_eq!(
            buckets,
            HashSet::from(["kvcache-a".to_owned(), "kvcache-b".to_owned()]),
            "blanks and system buckets must be dropped"
        );
        assert!(parse_lockless_cache_buckets("").is_empty());
    }
}
