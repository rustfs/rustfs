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

//! Invalidation glue for the object data cache adapter.

use crate::app::object_data_cache::ObjectDataCacheAdapter;
use rustfs_object_data_cache::{ObjectDataCacheIdentity, ObjectDataCacheInvalidationReason, ObjectDataCacheInvalidationResult};

/// Invalidates a single object identity through the app-layer cache adapter.
pub(crate) async fn invalidate_object_data_cache_object(
    adapter: &ObjectDataCacheAdapter,
    bucket: &str,
    key: &str,
    reason: ObjectDataCacheInvalidationReason,
) -> ObjectDataCacheInvalidationResult {
    adapter
        .invalidate_object(ObjectDataCacheIdentity::new(bucket, key), reason)
        .await
}

/// Invalidates many object identities through the app-layer cache adapter.
pub(crate) async fn invalidate_object_data_cache_objects<'a, I>(
    adapter: &ObjectDataCacheAdapter,
    bucket: &str,
    keys: I,
    reason: ObjectDataCacheInvalidationReason,
) where
    I: IntoIterator<Item = &'a String>,
{
    for key in keys {
        let _ = invalidate_object_data_cache_object(adapter, bucket, key.as_str(), reason).await;
    }
}

#[cfg(test)]
mod tests {
    use super::{invalidate_object_data_cache_object, invalidate_object_data_cache_objects};
    use crate::app::object_data_cache::ObjectDataCacheAdapter;
    use bytes::Bytes;
    use rustfs_object_data_cache::{
        ObjectDataCacheBodyVariant, ObjectDataCacheConfig, ObjectDataCacheFillResult, ObjectDataCacheInvalidationReason,
        ObjectDataCacheInvalidationResult, ObjectDataCacheLookup, ObjectDataCacheMode,
    };

    fn enabled_adapter() -> ObjectDataCacheAdapter {
        let config = ObjectDataCacheConfig {
            mode: ObjectDataCacheMode::FillMaterializeEnabled,
            max_bytes: 8_388_608,
            ..ObjectDataCacheConfig::default()
        };
        ObjectDataCacheAdapter::new(config).expect("enabled config should build adapter")
    }

    #[tokio::test]
    async fn single_object_invalidation_removes_cached_body() {
        let adapter = enabled_adapter();
        let plan = adapter.plan_get(rustfs_object_data_cache::ObjectDataCacheGetRequest {
            bucket: "bucket",
            object: "object",
            version_id: None,
            etag: "etag",
            size: 5,
            body_variant: ObjectDataCacheBodyVariant::FullObjectPlainV1,
        });
        let fill = adapter.fill_body(&plan, Bytes::from_static(b"hello")).await;
        let invalidation = invalidate_object_data_cache_object(
            &adapter,
            "bucket",
            "object",
            ObjectDataCacheInvalidationReason::AfterDeleteSuccess,
        )
        .await;
        let lookup = adapter.lookup_body(&plan).await;

        assert_eq!(fill, ObjectDataCacheFillResult::Inserted);
        assert_eq!(invalidation, ObjectDataCacheInvalidationResult::Success);
        assert!(matches!(lookup, ObjectDataCacheLookup::Miss));
    }

    #[tokio::test]
    async fn batch_invalidation_removes_each_requested_identity() {
        let adapter = enabled_adapter();
        let plan_a = adapter.plan_get(rustfs_object_data_cache::ObjectDataCacheGetRequest {
            bucket: "bucket",
            object: "a",
            version_id: None,
            etag: "etag-a",
            size: 5,
            body_variant: ObjectDataCacheBodyVariant::FullObjectPlainV1,
        });
        let plan_b = adapter.plan_get(rustfs_object_data_cache::ObjectDataCacheGetRequest {
            bucket: "bucket",
            object: "b",
            version_id: None,
            etag: "etag-b",
            size: 5,
            body_variant: ObjectDataCacheBodyVariant::FullObjectPlainV1,
        });
        let _ = adapter.fill_body(&plan_a, Bytes::from_static(b"aaaaa")).await;
        let _ = adapter.fill_body(&plan_b, Bytes::from_static(b"bbbbb")).await;

        let keys = vec!["a".to_string(), "b".to_string()];
        invalidate_object_data_cache_objects(&adapter, "bucket", keys.iter(), ObjectDataCacheInvalidationReason::BeforeMutation)
            .await;

        assert!(matches!(adapter.lookup_body(&plan_a).await, ObjectDataCacheLookup::Miss));
        assert!(matches!(adapter.lookup_body(&plan_b).await, ObjectDataCacheLookup::Miss));
    }
}
