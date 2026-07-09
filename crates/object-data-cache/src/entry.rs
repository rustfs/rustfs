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

use bytes::Bytes;
use std::cmp;
use std::sync::Arc;
use std::time::Instant;

use crate::key::ObjectDataCacheKey;

const ENTRY_OVERHEAD_BYTES: usize = 64;

/// Cached object body entry.
#[derive(Debug, Clone)]
pub struct ObjectDataCacheEntry {
    bytes: Bytes,
    content_length: u64,
    etag: Arc<str>,
    inserted_at: Instant,
}

impl ObjectDataCacheEntry {
    /// Creates a new cached entry.
    pub fn new(bytes: Bytes, content_length: u64, etag: Arc<str>) -> Self {
        Self {
            bytes,
            content_length,
            etag,
            inserted_at: Instant::now(),
        }
    }

    /// Returns a clone of the cached body bytes.
    pub fn bytes(&self) -> Bytes {
        self.bytes.clone()
    }

    /// Returns the recorded content length.
    pub const fn content_length(&self) -> u64 {
        self.content_length
    }

    /// Returns the cached etag reference.
    pub fn etag(&self) -> &Arc<str> {
        &self.etag
    }

    /// Returns the insertion timestamp.
    pub const fn inserted_at(&self) -> Instant {
        self.inserted_at
    }

    /// Returns the estimated weighted size for capacity accounting.
    pub fn estimated_weight(&self, key: &ObjectDataCacheKey) -> u32 {
        let key_bytes = key.bucket.len() + key.object.len() + key.version_id.len() + key.etag.len();
        let body_bytes = self.bytes.len();
        let total = key_bytes.saturating_add(body_bytes).saturating_add(ENTRY_OVERHEAD_BYTES);
        let clamped = cmp::min(total, u32::MAX as usize);

        u32::try_from(clamped).unwrap_or(u32::MAX)
    }
}

#[cfg(test)]
mod tests {
    use super::ObjectDataCacheEntry;
    use crate::key::{ObjectDataCacheBodyVariant, ObjectDataCacheKey};
    use bytes::Bytes;
    use std::sync::Arc;

    #[test]
    fn estimated_weight_includes_body_and_key_bytes() {
        let key =
            ObjectDataCacheKey::new("bucket", "object", Some("vid"), "etag", 5, ObjectDataCacheBodyVariant::FullObjectPlainV1);
        let entry = ObjectDataCacheEntry::new(Bytes::from_static(b"hello"), 5, Arc::<str>::from("etag"));

        let weight = entry.estimated_weight(&key);

        assert!(weight >= 5);
    }

    #[test]
    fn estimated_weight_clamps_to_u32_max() {
        let huge_bucket = "b".repeat(1024);
        let huge_object = "o".repeat(1024);
        let huge_etag = "e".repeat(1024);
        let key = ObjectDataCacheKey::new(
            huge_bucket.as_str(),
            huge_object.as_str(),
            Some("version"),
            huge_etag.as_str(),
            1,
            ObjectDataCacheBodyVariant::FullObjectPlainV1,
        );
        let huge = vec![0u8; (u32::MAX as usize).saturating_add(1024)];
        let entry = ObjectDataCacheEntry::new(Bytes::from(huge), 1, Arc::<str>::from("etag"));

        let weight = entry.estimated_weight(&key);

        assert_eq!(weight, u32::MAX);
    }
}
