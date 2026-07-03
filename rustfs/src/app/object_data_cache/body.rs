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

//! Body handoff glue for the object data cache adapter.

use crate::app::object_data_cache::ObjectDataCacheAdapter;
use crate::app::object_data_cache::planner::GetObjectBodyCachePlan;
use bytes::Bytes;
use rustfs_object_data_cache::{ObjectDataCacheFillResult, ObjectDataCacheLookup};

/// Result of an app-layer GET body cache lookup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum GetObjectBodyCacheLookup {
    Disabled,
    Skip,
    Miss,
    Hit(Bytes),
}

/// Attempts a conservative cache lookup for the GET response body.
pub(crate) async fn lookup_get_object_body_cache_hit(
    adapter: &ObjectDataCacheAdapter,
    plan: &GetObjectBodyCachePlan,
) -> GetObjectBodyCacheLookup {
    match plan {
        GetObjectBodyCachePlan::Disabled => GetObjectBodyCacheLookup::Disabled,
        GetObjectBodyCachePlan::Skip => GetObjectBodyCacheLookup::Skip,
        GetObjectBodyCachePlan::Cacheable(plan) => match adapter.lookup_body(plan).await {
            ObjectDataCacheLookup::Hit(bytes) => GetObjectBodyCacheLookup::Hit(bytes),
            ObjectDataCacheLookup::Miss => GetObjectBodyCacheLookup::Miss,
            ObjectDataCacheLookup::SkipDisabled => GetObjectBodyCacheLookup::Disabled,
            ObjectDataCacheLookup::SkipNotCacheable => GetObjectBodyCacheLookup::Skip,
        },
    }
}

/// Attempts a conservative cache fill from an already buffered full object body.
pub(crate) async fn fill_get_object_body_cache_from_buffered_body(
    adapter: &ObjectDataCacheAdapter,
    plan: &GetObjectBodyCachePlan,
    buffered_body: &Bytes,
) -> ObjectDataCacheFillResult {
    fill_get_object_body_cache_from_bytes(adapter, plan, buffered_body).await
}

/// Attempts a conservative cache fill from an already materialized full object body.
pub(crate) async fn fill_get_object_body_cache_from_materialized_body(
    adapter: &ObjectDataCacheAdapter,
    plan: &GetObjectBodyCachePlan,
    materialized_body: &Bytes,
) -> ObjectDataCacheFillResult {
    fill_get_object_body_cache_from_bytes(adapter, plan, materialized_body).await
}

async fn fill_get_object_body_cache_from_bytes(
    adapter: &ObjectDataCacheAdapter,
    plan: &GetObjectBodyCachePlan,
    body: &Bytes,
) -> ObjectDataCacheFillResult {
    match plan {
        GetObjectBodyCachePlan::Disabled => ObjectDataCacheFillResult::SkippedDisabled,
        GetObjectBodyCachePlan::Skip => ObjectDataCacheFillResult::SkippedNotCacheable,
        GetObjectBodyCachePlan::Cacheable(plan) => adapter.fill_body(plan, body.clone()).await,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        GetObjectBodyCacheLookup, fill_get_object_body_cache_from_buffered_body,
        fill_get_object_body_cache_from_materialized_body, lookup_get_object_body_cache_hit,
    };
    use crate::app::object_data_cache::ObjectDataCacheAdapter;
    use crate::app::object_data_cache::planner::{GetObjectBodyCacheRequest, build_get_object_body_cache_plan};
    use bytes::Bytes;
    use rustfs_object_data_cache::{
        ObjectDataCacheBodyVariant, ObjectDataCacheConfig, ObjectDataCacheFillResult, ObjectDataCacheMode,
    };

    fn enabled_fill_adapter() -> ObjectDataCacheAdapter {
        let config = ObjectDataCacheConfig {
            mode: ObjectDataCacheMode::FillBufferedOnly,
            max_bytes: 8_388_608,
            ..ObjectDataCacheConfig::default()
        };
        ObjectDataCacheAdapter::new(config).expect("fill-enabled config should build adapter")
    }

    #[tokio::test]
    async fn body_lookup_returns_hit_when_cache_contains_matching_body() {
        let adapter = enabled_fill_adapter();
        let info = crate::storage::storage_api::StorageObjectInfo {
            etag: Some("etag".to_string()),
            size: 5,
            ..Default::default()
        };
        let request = GetObjectBodyCacheRequest {
            bucket: "bucket",
            key: "object",
            info: &info,
            response_content_length: 5,
            has_range: false,
            part_number: None,
            encryption_applied: false,
        };
        let cache_plan = build_get_object_body_cache_plan(&adapter, request);

        let plan = adapter.plan_get(rustfs_object_data_cache::ObjectDataCacheGetRequest {
            bucket: "bucket",
            object: "object",
            version_id: None,
            etag: "etag",
            size: 5,
            body_variant: ObjectDataCacheBodyVariant::FullObjectPlainV1,
        });
        let fill = adapter.cache().fill_body(&plan, Bytes::from_static(b"hello")).await;
        let lookup = lookup_get_object_body_cache_hit(&adapter, &cache_plan).await;

        assert_eq!(fill, ObjectDataCacheFillResult::Inserted);
        assert!(matches!(lookup, GetObjectBodyCacheLookup::Hit(ref bytes) if bytes.as_ref() == b"hello"));
    }

    #[tokio::test]
    async fn body_lookup_returns_skip_for_part_requests() {
        let adapter = enabled_fill_adapter();
        let info = crate::storage::storage_api::StorageObjectInfo {
            etag: Some("etag".to_string()),
            size: 5,
            ..Default::default()
        };

        let request = GetObjectBodyCacheRequest {
            bucket: "bucket",
            key: "object",
            info: &info,
            response_content_length: 5,
            has_range: false,
            part_number: Some(1),
            encryption_applied: false,
        };
        let cache_plan = build_get_object_body_cache_plan(&adapter, request);
        let lookup = lookup_get_object_body_cache_hit(&adapter, &cache_plan).await;

        assert!(matches!(lookup, GetObjectBodyCacheLookup::Skip));
    }

    #[tokio::test]
    async fn buffered_body_fill_populates_cache_for_later_hit() {
        let adapter = enabled_fill_adapter();
        let info = crate::storage::storage_api::StorageObjectInfo {
            etag: Some("etag".to_string()),
            size: 5,
            ..Default::default()
        };
        let request = GetObjectBodyCacheRequest {
            bucket: "bucket",
            key: "object",
            info: &info,
            response_content_length: 5,
            has_range: false,
            part_number: None,
            encryption_applied: false,
        };
        let cache_plan = build_get_object_body_cache_plan(&adapter, request);

        let fill = fill_get_object_body_cache_from_buffered_body(&adapter, &cache_plan, &Bytes::from_static(b"hello")).await;
        let lookup = lookup_get_object_body_cache_hit(&adapter, &cache_plan).await;

        assert_eq!(fill, ObjectDataCacheFillResult::Inserted);
        assert!(matches!(lookup, GetObjectBodyCacheLookup::Hit(ref bytes) if bytes.as_ref() == b"hello"));
    }

    #[tokio::test]
    async fn buffered_body_fill_rejects_size_mismatch() {
        let adapter = enabled_fill_adapter();
        let info = crate::storage::storage_api::StorageObjectInfo {
            etag: Some("etag".to_string()),
            size: 5,
            ..Default::default()
        };
        let request = GetObjectBodyCacheRequest {
            bucket: "bucket",
            key: "object",
            info: &info,
            response_content_length: 5,
            has_range: false,
            part_number: None,
            encryption_applied: false,
        };
        let cache_plan = build_get_object_body_cache_plan(&adapter, request);

        let fill = fill_get_object_body_cache_from_buffered_body(&adapter, &cache_plan, &Bytes::from_static(b"oops")).await;
        let lookup = lookup_get_object_body_cache_hit(&adapter, &cache_plan).await;

        assert_eq!(fill, ObjectDataCacheFillResult::SkippedSizeMismatch);
        assert!(matches!(lookup, GetObjectBodyCacheLookup::Miss));
    }

    #[tokio::test]
    async fn materialized_body_fill_populates_cache_for_later_hit() {
        let adapter = enabled_fill_adapter();
        let info = crate::storage::storage_api::StorageObjectInfo {
            etag: Some("etag".to_string()),
            size: 5,
            ..Default::default()
        };
        let request = GetObjectBodyCacheRequest {
            bucket: "bucket",
            key: "object",
            info: &info,
            response_content_length: 5,
            has_range: false,
            part_number: None,
            encryption_applied: false,
        };
        let cache_plan = build_get_object_body_cache_plan(&adapter, request);

        let fill = fill_get_object_body_cache_from_materialized_body(&adapter, &cache_plan, &Bytes::from_static(b"hello")).await;
        let lookup = lookup_get_object_body_cache_hit(&adapter, &cache_plan).await;

        assert_eq!(fill, ObjectDataCacheFillResult::Inserted);
        assert!(matches!(lookup, GetObjectBodyCacheLookup::Hit(ref bytes) if bytes.as_ref() == b"hello"));
    }
}
