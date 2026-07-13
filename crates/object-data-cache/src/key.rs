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
///
/// The key is *write-unique*, not merely *content-unique* (backlog#1111 /
/// ODC-06). `etag + size` alone identify content: for an unversioned overwrite
/// two same-length payloads that collide on MD5 would derive the identical key,
/// so a GET on a node that never observed the overwrite could serve the old
/// bytes for up to the TTL, and the same collision turns the
/// fill-after-invalidation race (backlog#1118) into a serving bug.
///
/// The write-unique anchor is `data_dir` — the xl.meta directory UUID that
/// ecstore regenerates on *every* body write. Two distinct writes of the same
/// object therefore never share a `data_dir`, so the key changes on overwrite
/// even when `etag + size` collide (an MD5 collision) *and* `mod_time` is equal
/// (clock skew or same-tick writes). `mod_time_unix_nanos` remains as a second
/// anchor for reads where `data_dir` is unavailable, and `etag + size` stay as
/// belt-and-braces.
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
    /// Resolved version's `data_dir` UUID as a `u128` (`Uuid::as_u128`), or
    /// `None` when the read did not resolve one. This is the primary
    /// write-unique component: ecstore regenerates `data_dir` on every body
    /// write, so it is distinct across overwrites regardless of content or
    /// timestamp. Held as `u128` to keep this crate free of a `uuid` dependency.
    pub data_dir_u128: Option<u128>,
    /// Resolved version's modification time as Unix nanoseconds
    /// (`OffsetDateTime::unix_timestamp_nanos`), or `0` when absent. Second
    /// write-unique anchor, used when `data_dir` is unavailable.
    pub mod_time_unix_nanos: i128,
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
    /// Creates a stable object data cache key with explicit write anchors
    /// (`data_dir` and `mod_time`). This is the full constructor; the production
    /// GET planner uses it so the key is write-unique (backlog#1111 / ODC-06).
    #[allow(clippy::too_many_arguments)]
    pub fn with_write_anchors(
        bucket: impl Into<Arc<str>>,
        object: impl Into<Arc<str>>,
        version_id: Option<&str>,
        etag: impl Into<Arc<str>>,
        size: u64,
        data_dir_u128: Option<u128>,
        mod_time_unix_nanos: i128,
        body_variant: ObjectDataCacheBodyVariant,
    ) -> Self {
        Self {
            bucket: bucket.into(),
            object: object.into(),
            version_id: version_id.map_or_else(|| Arc::<str>::from(NULL_VERSION_ID), Arc::<str>::from),
            etag: etag.into(),
            size,
            data_dir_u128,
            mod_time_unix_nanos,
            body_variant,
        }
    }

    /// Creates a stable object data cache key with no write anchors (`data_dir`
    /// absent, `mod_time_unix_nanos == 0`). Retained for callers that key purely
    /// by content identity (index/backend tests).
    pub fn new(
        bucket: impl Into<Arc<str>>,
        object: impl Into<Arc<str>>,
        version_id: Option<&str>,
        etag: impl Into<Arc<str>>,
        size: u64,
        body_variant: ObjectDataCacheBodyVariant,
    ) -> Self {
        Self::with_write_anchors(bucket, object, version_id, etag, size, None, 0, body_variant)
    }

    /// Returns true when this key targets the canonical unversioned body variant.
    ///
    /// Only exercised by tests; gated so it is not compiled into the shipping
    /// binary as dead code (backlog#1141).
    #[cfg(test)]
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
    fn key_distinguishes_writes_by_mod_time() {
        // ODC-06 (backlog#1111): two writes with identical etag + size (an MD5
        // collision on an unversioned overwrite) must derive different keys once
        // the modification time differs, so a stale node cannot serve old bytes.
        let old = ObjectDataCacheKey::with_write_anchors(
            "bucket",
            "object",
            None,
            "etag",
            42,
            None,
            1_000,
            ObjectDataCacheBodyVariant::FullObjectPlainV1,
        );
        let new = ObjectDataCacheKey::with_write_anchors(
            "bucket",
            "object",
            None,
            "etag",
            42,
            None,
            2_000,
            ObjectDataCacheBodyVariant::FullObjectPlainV1,
        );

        assert_ne!(old, new, "keys differing only by mod_time must not collide");
        // Same mod_time still collapses to one key (a true re-read of one write).
        let new_again = ObjectDataCacheKey::with_write_anchors(
            "bucket",
            "object",
            None,
            "etag",
            42,
            None,
            2_000,
            ObjectDataCacheBodyVariant::FullObjectPlainV1,
        );
        assert_eq!(new, new_again);
    }

    #[test]
    fn key_distinguishes_writes_by_data_dir() {
        // ODC-06 (backlog#1111): `data_dir` is regenerated on every body write,
        // so two writes distinct only by `data_dir` — identical etag + size (MD5
        // collision) AND identical mod_time (clock skew or same-tick writes) —
        // must still derive different keys. This is the case mod_time alone
        // cannot cover.
        let first = ObjectDataCacheKey::with_write_anchors(
            "bucket",
            "object",
            None,
            "etag",
            42,
            Some(1),
            1_000,
            ObjectDataCacheBodyVariant::FullObjectPlainV1,
        );
        let second = ObjectDataCacheKey::with_write_anchors(
            "bucket",
            "object",
            None,
            "etag",
            42,
            Some(2),
            1_000,
            ObjectDataCacheBodyVariant::FullObjectPlainV1,
        );
        assert_ne!(first, second, "keys differing only by data_dir must not collide");

        // Absent data_dir falls back to the remaining anchors (no regression).
        let no_data_dir = ObjectDataCacheKey::with_write_anchors(
            "bucket",
            "object",
            None,
            "etag",
            42,
            None,
            1_000,
            ObjectDataCacheBodyVariant::FullObjectPlainV1,
        );
        assert_ne!(first, no_data_dir);
    }

    #[test]
    fn key_new_defaults_write_anchors() {
        let key = ObjectDataCacheKey::new("bucket", "object", None, "etag", 42, ObjectDataCacheBodyVariant::FullObjectPlainV1);

        assert_eq!(key.mod_time_unix_nanos, 0);
        assert_eq!(key.data_dir_u128, None);
    }

    #[test]
    fn identity_new_preserves_bucket_and_object() {
        let identity = ObjectDataCacheIdentity::new("bucket", "object");

        assert_eq!(identity.bucket.as_ref(), "bucket");
        assert_eq!(identity.object.as_ref(), "object");
    }
}
