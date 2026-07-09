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

use std::sync::Arc;

/// Canonical synthetic version id for unversioned or latest-only object bodies.
pub const NULL_VERSION_ID: &str = "null";

/// Response body variant supported by the cache.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum ObjectDataCacheBodyVariant {
    /// Full plain object body.
    #[default]
    FullObjectPlainV1,
}

/// Stable cache key for a reusable object body.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ObjectDataCacheKey {
    /// Bucket name.
    pub bucket: Arc<str>,
    /// Object key.
    pub object: Arc<str>,
    /// Canonical version id, using `"null"` for unversioned bodies.
    pub version_id: Arc<str>,
    /// Object ETag.
    pub etag: Arc<str>,
    /// Object size in bytes.
    pub size: u64,
    /// Cached body semantics.
    pub body_variant: ObjectDataCacheBodyVariant,
}

/// Identity used for conservative invalidation.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ObjectDataCacheIdentity {
    /// Bucket name.
    pub bucket: Arc<str>,
    /// Object key.
    pub object: Arc<str>,
}

impl ObjectDataCacheKey {
    /// Creates a new stable object data cache key.
    pub fn new(
        bucket: impl Into<Arc<str>>,
        object: impl Into<Arc<str>>,
        version_id: Option<&str>,
        etag: impl Into<Arc<str>>,
        size: u64,
        body_variant: ObjectDataCacheBodyVariant,
    ) -> Self {
        Self {
            bucket: bucket.into(),
            object: object.into(),
            version_id: version_id.map_or_else(|| Arc::<str>::from(NULL_VERSION_ID), Arc::<str>::from),
            etag: etag.into(),
            size,
            body_variant,
        }
    }

    /// Returns true when this key targets the canonical unversioned body variant.
    pub fn is_null_version(&self) -> bool {
        self.version_id.as_ref() == NULL_VERSION_ID
    }
}

impl ObjectDataCacheIdentity {
    /// Creates a conservative invalidation identity.
    pub fn new(bucket: impl Into<Arc<str>>, object: impl Into<Arc<str>>) -> Self {
        Self {
            bucket: bucket.into(),
            object: object.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{NULL_VERSION_ID, ObjectDataCacheBodyVariant, ObjectDataCacheIdentity, ObjectDataCacheKey};

    #[test]
    fn key_uses_canonical_null_version_for_missing_version_id() {
        let key = ObjectDataCacheKey::new("bucket", "object", None, "etag", 42, ObjectDataCacheBodyVariant::FullObjectPlainV1);

        assert_eq!(key.version_id.as_ref(), NULL_VERSION_ID);
        assert!(key.is_null_version());
    }

    #[test]
    fn key_distinguishes_explicit_version_ids() {
        let latest = ObjectDataCacheKey::new("bucket", "object", None, "etag", 42, ObjectDataCacheBodyVariant::FullObjectPlainV1);
        let versioned =
            ObjectDataCacheKey::new("bucket", "object", Some("3d2"), "etag", 42, ObjectDataCacheBodyVariant::FullObjectPlainV1);

        assert_ne!(latest, versioned);
    }

    #[test]
    fn identity_new_preserves_bucket_and_object() {
        let identity = ObjectDataCacheIdentity::new("bucket", "object");

        assert_eq!(identity.bucket.as_ref(), "bucket");
        assert_eq!(identity.object.as_ref(), "object");
    }
}
