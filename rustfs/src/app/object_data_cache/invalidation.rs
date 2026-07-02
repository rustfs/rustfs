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

/// Invalidates a single object before a mutating operation begins.
pub(crate) async fn invalidate_object_data_cache_before_mutation(
    adapter: &ObjectDataCacheAdapter,
    bucket: &str,
    key: &str,
) -> ObjectDataCacheInvalidationResult {
    invalidate_object_data_cache_object(adapter, bucket, key, ObjectDataCacheInvalidationReason::BeforeMutation).await
}

/// Invalidates a single object after a successful `PutObject`.
pub(crate) async fn invalidate_object_data_cache_after_put_success(
    adapter: &ObjectDataCacheAdapter,
    bucket: &str,
    key: &str,
) -> ObjectDataCacheInvalidationResult {
    invalidate_object_data_cache_object(adapter, bucket, key, ObjectDataCacheInvalidationReason::AfterPutSuccess).await
}

/// Invalidates a single object after a successful delete.
pub(crate) async fn invalidate_object_data_cache_after_delete_success(
    adapter: &ObjectDataCacheAdapter,
    bucket: &str,
    key: &str,
) -> ObjectDataCacheInvalidationResult {
    invalidate_object_data_cache_object(adapter, bucket, key, ObjectDataCacheInvalidationReason::AfterDeleteSuccess).await
}

/// Invalidates a single object after a successful copy destination write.
pub(crate) async fn invalidate_object_data_cache_after_copy_success(
    adapter: &ObjectDataCacheAdapter,
    bucket: &str,
    key: &str,
) -> ObjectDataCacheInvalidationResult {
    invalidate_object_data_cache_object(adapter, bucket, key, ObjectDataCacheInvalidationReason::AfterCopySuccess).await
}

/// Invalidates a single object after a successful multipart completion.
pub(crate) async fn invalidate_object_data_cache_after_complete_multipart_success(
    adapter: &ObjectDataCacheAdapter,
    bucket: &str,
    key: &str,
) -> ObjectDataCacheInvalidationResult {
    invalidate_object_data_cache_object(adapter, bucket, key, ObjectDataCacheInvalidationReason::AfterCompleteMultipartSuccess)
        .await
}

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

/// Invalidates many object identities before a mutating operation begins.
pub(crate) async fn invalidate_object_data_cache_objects_before_mutation<'a, I>(
    adapter: &ObjectDataCacheAdapter,
    bucket: &str,
    keys: I,
) where
    I: IntoIterator<Item = &'a String>,
{
    invalidate_object_data_cache_objects(adapter, bucket, keys, ObjectDataCacheInvalidationReason::BeforeMutation).await;
}

/// Invalidates many object identities after successful deletes.
pub(crate) async fn invalidate_object_data_cache_objects_after_delete_success<'a, I>(
    adapter: &ObjectDataCacheAdapter,
    bucket: &str,
    keys: I,
) where
    I: IntoIterator<Item = &'a String>,
{
    invalidate_object_data_cache_objects(adapter, bucket, keys, ObjectDataCacheInvalidationReason::AfterDeleteSuccess).await;
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
    use super::{
        invalidate_object_data_cache_after_delete_success, invalidate_object_data_cache_before_mutation,
        invalidate_object_data_cache_objects_before_mutation,
    };
    use crate::app::object_data_cache::ObjectDataCacheAdapter;
    use bytes::Bytes;
    use rustfs_object_data_cache::{
        ObjectDataCacheBodyVariant, ObjectDataCacheConfig, ObjectDataCacheFillResult, ObjectDataCacheInvalidationResult,
        ObjectDataCacheLookup, ObjectDataCacheMode,
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
        let _ = invalidate_object_data_cache_before_mutation(&adapter, "bucket", "object").await;
        let invalidation = invalidate_object_data_cache_after_delete_success(&adapter, "bucket", "object").await;
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
        invalidate_object_data_cache_objects_before_mutation(&adapter, "bucket", keys.iter()).await;

        assert!(matches!(adapter.lookup_body(&plan_a).await, ObjectDataCacheLookup::Miss));
        assert!(matches!(adapter.lookup_body(&plan_b).await, ObjectDataCacheLookup::Miss));
    }
}
