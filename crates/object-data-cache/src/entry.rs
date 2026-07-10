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

use crate::key::ObjectDataCacheKey;

const ENTRY_OVERHEAD_BYTES: usize = 64;

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
}

impl ObjectDataCacheEntry {
    /// Creates a new cached entry.
    ///
    /// `content_length` and `etag` are accepted but intentionally ignored: they
    /// are redundant with the moka key identity. The arity is retained so the
    /// only external constructor caller (`MokaBackend::fill_body`, owned by a
    /// concurrent branch) keeps compiling; the follow-up dropping the two unused
    /// arguments belongs with that file.
    pub fn new(bytes: Bytes, content_length: u64, etag: Arc<str>) -> Self {
        let _ = content_length;
        let _ = etag;
        Self { bytes }
    }

    /// Returns a clone of the cached body bytes.
    pub fn bytes(&self) -> Bytes {
        self.bytes.clone()
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
