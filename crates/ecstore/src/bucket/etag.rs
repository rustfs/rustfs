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

//! Per-bucket ETag algorithm.
//!
//! A bucket can derive the ETags of objects written without Content-MD5 from
//! XXH3-128 instead of MD5, for clients that never treat the ETag as an MD5
//! (an LLM KV cache). The choice is stored as a RustFS extension entry
//! (`etag.json`) in the bucket metadata file and published to an in-memory
//! registry whenever the metadata is installed, so the write path looks it up
//! without I/O. Buckets without the entry keep MD5 ETags.

use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{OnceLock, PoisonError, RwLock};

pub const BUCKET_ETAG_ALGORITHM_MD5: &str = "md5";
pub const BUCKET_ETAG_ALGORITHM_XXH3: &str = "xxh3";

/// JSON payload stored under the `etag.json` bucket metadata entry. An absent
/// entry, an empty payload or an invalid algorithm all mean MD5 ETags.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketEtagConfig {
    /// `md5` | `xxh3` (case-insensitive, whitespace-tolerant).
    #[serde(default)]
    pub algorithm: String,
}

impl BucketEtagConfig {
    pub fn new(algorithm: &str) -> Self {
        Self {
            algorithm: algorithm.to_string(),
        }
    }

    /// Whether `algorithm` names a supported ETag algorithm.
    pub fn is_valid_algorithm(algorithm: &str) -> bool {
        matches!(
            algorithm.trim().to_ascii_lowercase().as_str(),
            BUCKET_ETAG_ALGORITHM_MD5 | BUCKET_ETAG_ALGORITHM_XXH3
        )
    }

    /// The canonical (trimmed, lowercase) algorithm name, or `None` when the
    /// config does not carry a valid one.
    pub fn normalized_algorithm(&self) -> Option<String> {
        let algorithm = self.algorithm.trim().to_ascii_lowercase();
        if Self::is_valid_algorithm(&algorithm) {
            Some(algorithm)
        } else {
            None
        }
    }

    /// Whether this config selects XXH3 ETags.
    pub fn uses_xxh3(&self) -> bool {
        self.normalized_algorithm().as_deref() == Some(BUCKET_ETAG_ALGORITHM_XXH3)
    }
}

/// Buckets whose ETags use XXH3, kept in step with the installed bucket
/// metadata. `ANY` lets the write path skip the lock while no bucket opts in.
static XXH3_BUCKETS: OnceLock<RwLock<HashSet<String>>> = OnceLock::new();
static ANY: AtomicBool = AtomicBool::new(false);

fn xxh3_buckets() -> &'static RwLock<HashSet<String>> {
    XXH3_BUCKETS.get_or_init(|| RwLock::new(HashSet::new()))
}

/// Records whether `bucket` uses XXH3 ETags.
pub fn set_bucket_xxh3_etag(bucket: &str, xxh3: bool) {
    if bucket.is_empty() {
        return;
    }
    let mut buckets = xxh3_buckets().write().unwrap_or_else(PoisonError::into_inner);
    if xxh3 {
        buckets.insert(bucket.to_string());
    } else {
        buckets.remove(bucket);
    }
    ANY.store(!buckets.is_empty(), Ordering::Release);
}

/// Whether objects written to `bucket` without Content-MD5 get XXH3 ETags.
pub fn bucket_uses_xxh3_etag(bucket: &str) -> bool {
    if !ANY.load(Ordering::Acquire) {
        return false;
    }
    xxh3_buckets().read().unwrap_or_else(PoisonError::into_inner).contains(bucket)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_algorithms_are_recognized() {
        assert!(BucketEtagConfig::is_valid_algorithm("md5"));
        assert!(BucketEtagConfig::is_valid_algorithm(" XXH3 "));
        assert!(!BucketEtagConfig::is_valid_algorithm(""));
        assert!(!BucketEtagConfig::is_valid_algorithm("sha256"));
    }

    #[test]
    fn normalized_algorithm_canonicalizes_or_rejects() {
        assert_eq!(BucketEtagConfig::new(" Xxh3 ").normalized_algorithm().as_deref(), Some("xxh3"));
        assert_eq!(BucketEtagConfig::new("MD5").normalized_algorithm().as_deref(), Some("md5"));
        assert_eq!(BucketEtagConfig::new("crc32").normalized_algorithm(), None);
        assert!(BucketEtagConfig::new("xxh3").uses_xxh3());
        assert!(!BucketEtagConfig::new("md5").uses_xxh3());
        assert!(!BucketEtagConfig::default().uses_xxh3());
    }

    #[test]
    fn json_round_trip() {
        let config = BucketEtagConfig::new("xxh3");
        let json = serde_json::to_vec(&config).expect("serialize");
        assert_eq!(json, br#"{"algorithm":"xxh3"}"#);
        assert_eq!(serde_json::from_slice::<BucketEtagConfig>(&json).expect("parse"), config);
        assert_eq!(
            serde_json::from_slice::<BucketEtagConfig>(b"{}").expect("parse"),
            BucketEtagConfig::default()
        );
    }

    #[test]
    fn registry_tracks_xxh3_buckets() {
        let bucket = "etag-registry-test-bucket";
        assert!(!bucket_uses_xxh3_etag(bucket));
        set_bucket_xxh3_etag(bucket, true);
        assert!(bucket_uses_xxh3_etag(bucket));
        assert!(!bucket_uses_xxh3_etag("etag-registry-test-other"));
        set_bucket_xxh3_etag(bucket, false);
        assert!(!bucket_uses_xxh3_etag(bucket));
        set_bucket_xxh3_etag("", true);
        assert!(!bucket_uses_xxh3_etag(""));
    }
}
