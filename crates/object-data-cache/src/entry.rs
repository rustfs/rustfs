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

use crate::index::ObjectDataCacheGeneration;
use crate::key::ObjectDataCacheKey;

const ENTRY_OVERHEAD_BYTES: u64 = 64;

/// Estimates the weighted capacity charged for a key and planned body.
pub(crate) fn projected_weight(key: &ObjectDataCacheKey, body_bytes: u64) -> u64 {
    let key_bytes = key
        .bucket
        .len()
        .saturating_add(key.object.len())
        .saturating_add(key.version_id.len())
        .saturating_add(key.etag.len());

    u64::try_from(key_bytes)
        .unwrap_or(u64::MAX)
        .saturating_add(body_bytes)
        .saturating_add(ENTRY_OVERHEAD_BYTES)
}

/// Cached object body entry.
///
/// The content length and etag are already part of the moka key's `Hash`/`Eq`
/// identity (see [`ObjectDataCacheKey`]), so a cached hit is only returned for a
/// key that already matched them. Storing per-entry copies (`content_length`,
/// `etag`, `inserted_at`) added memory not captured by `ENTRY_OVERHEAD_BYTES`
/// with no reader, so the entry is now a thin `Bytes` wrapper (backlog#1141).
#[derive(Debug, Clone)]
pub struct ObjectDataCacheEntry {
    bytes: Bytes,
    generation: ObjectDataCacheGeneration,
}

impl ObjectDataCacheEntry {
    /// Creates a new cached entry.
    pub fn new(bytes: Bytes) -> Self {
        Self { bytes, generation: 0 }
    }

    pub(crate) fn with_generation(bytes: Bytes, generation: ObjectDataCacheGeneration) -> Self {
        Self { bytes, generation }
    }

    /// Returns a clone of the cached body bytes.
    pub fn bytes(&self) -> Bytes {
        self.bytes.clone()
    }

    pub(crate) const fn generation(&self) -> ObjectDataCacheGeneration {
        self.generation
    }

    /// Returns the estimated weighted size for capacity accounting.
    pub fn estimated_weight(&self, key: &ObjectDataCacheKey) -> u32 {
        let body_bytes = u64::try_from(self.bytes.len()).unwrap_or(u64::MAX);
        u32::try_from(projected_weight(key, body_bytes)).unwrap_or(u32::MAX)
    }
}

#[cfg(test)]
mod tests {
    use super::ObjectDataCacheEntry;
    use crate::key::{ObjectDataCacheBodyVariant, ObjectDataCacheKey};
    use bytes::Bytes;

    #[test]
    fn estimated_weight_includes_body_and_key_bytes() {
        let key =
            ObjectDataCacheKey::new("bucket", "object", Some("vid"), "etag", 5, ObjectDataCacheBodyVariant::FullObjectPlainV1);
        let entry = ObjectDataCacheEntry::new(Bytes::from_static(b"hello"));

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
        let entry = ObjectDataCacheEntry::new(Bytes::from(huge));

        let weight = entry.estimated_weight(&key);

        assert_eq!(weight, u32::MAX);
    }
}
