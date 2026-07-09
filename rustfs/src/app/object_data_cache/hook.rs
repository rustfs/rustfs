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

//! ecstore-facing GET body cache hook.
//!
//! Registered into ecstore so the cache probe runs inside `get_object_reader`
//! right after metadata resolution: probing earlier needs a second metadata
//! fan-out, probing later (after the reader is built) means a hit no longer
//! saves the erasure read/decode.

use crate::app::object_data_cache::{
    GetObjectBodyCacheLookup, GetObjectBodyCacheRequest, ObjectDataCacheAdapter, build_get_object_body_cache_plan,
    lookup_get_object_body_cache_hit,
};
use crate::app::storage_api::object_usecase::StorageObjectInfo;
use crate::storage::sse::contains_managed_encryption_metadata;
use crate::storage::storage_api::ecstore_bucket::lifecycle::bucket_lifecycle_ops::LifecycleOps as _;
use crate::storage::storage_api::ecstore_object::{GetObjectBodyCacheHook, register_get_object_body_cache_hook};
use bytes::Bytes;
use rustfs_utils::http::headers::SSEC_ALGORITHM_HEADER;
use std::sync::Arc;

/// Adapter-backed implementation of ecstore's GET body cache hook.
pub(crate) struct ObjectDataCacheBodyHook {
    adapter: Arc<ObjectDataCacheAdapter>,
}

/// Registers the body-cache hook into ecstore. No-op for a disabled cache so
/// the hot path keeps a single `None` branch when the feature is off.
pub(crate) fn register_object_data_cache_body_hook(adapter: Arc<ObjectDataCacheAdapter>) {
    if adapter.is_disabled() {
        return;
    }
    register_get_object_body_cache_hook(Arc::new(ObjectDataCacheBodyHook { adapter }));
}

fn object_metadata_indicates_encryption(metadata: &std::collections::HashMap<String, String>) -> bool {
    // SSE-C or managed SSE bodies are decrypted on the normal read path, so
    // the cached plaintext identity used by the planner would not match what
    // ecstore returns here (pre-decryption). Skip them entirely.
    metadata.contains_key(SSEC_ALGORITHM_HEADER) || contains_managed_encryption_metadata(metadata)
}

#[async_trait::async_trait]
impl GetObjectBodyCacheHook for ObjectDataCacheBodyHook {
    async fn lookup(&self, bucket: &str, object: &str, info: &StorageObjectInfo) -> Option<Bytes> {
        if info.is_remote() || object_metadata_indicates_encryption(&info.user_defined) {
            return None;
        }
        let response_content_length = info.get_actual_size().ok()?;
        let request = GetObjectBodyCacheRequest {
            bucket,
            key: object,
            info,
            response_content_length,
            has_range: false,
            part_number: None,
            encryption_applied: false,
        };
        let plan = build_get_object_body_cache_plan(&self.adapter, request);
        match lookup_get_object_body_cache_hit(&self.adapter, &plan).await {
            GetObjectBodyCacheLookup::Hit(bytes) => Some(bytes),
            GetObjectBodyCacheLookup::Disabled | GetObjectBodyCacheLookup::Skip | GetObjectBodyCacheLookup::Miss => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustfs_object_data_cache::{ObjectDataCacheConfig, ObjectDataCacheMode};

    fn hit_only_adapter() -> Arc<ObjectDataCacheAdapter> {
        Arc::new(
            ObjectDataCacheAdapter::new(ObjectDataCacheConfig {
                mode: ObjectDataCacheMode::FillBufferedOnly,
                max_bytes: 4 * 1024 * 1024,
                ..ObjectDataCacheConfig::default()
            })
            .expect("adapter"),
        )
    }

    fn plain_info(size: i64) -> StorageObjectInfo {
        StorageObjectInfo {
            bucket: "b".to_string(),
            name: "k".to_string(),
            etag: Some("etag-1".to_string()),
            size,
            actual_size: size,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn hook_lookup_returns_cached_body_after_fill() {
        let adapter = hit_only_adapter();
        let info = plain_info(5);
        let request = GetObjectBodyCacheRequest {
            bucket: "b",
            key: "k",
            info: &info,
            response_content_length: 5,
            has_range: false,
            part_number: None,
            encryption_applied: false,
        };
        let plan = build_get_object_body_cache_plan(&adapter, request);
        let body = Bytes::from_static(b"hello");
        let _ = crate::app::object_data_cache::fill_get_object_body_cache_from_buffered_body(&adapter, &plan, &body).await;

        let hook = ObjectDataCacheBodyHook {
            adapter: Arc::clone(&adapter),
        };
        let hit = hook.lookup("b", "k", &info).await.expect("cache hit");
        assert_eq!(hit, body);
    }

    #[tokio::test]
    async fn hook_lookup_skips_encrypted_objects() {
        let adapter = hit_only_adapter();
        let mut info = plain_info(5);
        info.user_defined = std::sync::Arc::new(
            [(SSEC_ALGORITHM_HEADER.to_string(), "AES256".to_string())]
                .into_iter()
                .collect(),
        );

        let hook = ObjectDataCacheBodyHook { adapter };
        assert!(hook.lookup("b", "k", &info).await.is_none());
    }
}
