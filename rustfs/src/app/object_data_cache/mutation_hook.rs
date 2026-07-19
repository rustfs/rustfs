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

//! ecstore-facing write-side cache hook.
//!
//! Registered into ecstore so its internal delete paths (lifecycle/scanner
//! expiry, noncurrent-version cleanup, restored-copy expiry) can drop the
//! object's cached body the moment the object is removed, instead of leaving
//! dead bytes resident until TTL (ODC-26, backlog#1131).

use crate::app::object_data_cache::ObjectDataCacheAdapter;
use crate::storage::storage_api::ecstore_object::{
    ObjectMutationHook, register_object_mutation_hook, unregister_object_mutation_hook,
};
use rustfs_object_data_cache::{ObjectDataCacheIdentity, ObjectDataCacheInvalidationReason};
use std::sync::Arc;

/// Adapter-backed implementation of ecstore's object mutation hook.
pub(crate) struct ObjectDataCacheMutationHook {
    adapter: Arc<ObjectDataCacheAdapter>,
}

/// Registers the mutation hook into ecstore, or removes the previous hook when
/// a config reload disables caching.
pub(crate) fn register_object_data_cache_mutation_hook(adapter: Arc<ObjectDataCacheAdapter>) {
    if adapter.is_disabled() {
        unregister_object_mutation_hook();
        return;
    }
    register_object_mutation_hook(Arc::new(ObjectDataCacheMutationHook { adapter }));
}

#[async_trait::async_trait]
impl ObjectMutationHook for ObjectDataCacheMutationHook {
    async fn after_object_mutation(&self, bucket: &str, object: &str) {
        let _ = self
            .adapter
            .invalidate_object(
                ObjectDataCacheIdentity::new(bucket, object),
                ObjectDataCacheInvalidationReason::AfterLifecycleExpiry,
            )
            .await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use rustfs_object_data_cache::{
        ObjectDataCacheConfig, ObjectDataCacheFillResult, ObjectDataCacheGetRequest, ObjectDataCacheLookup, ObjectDataCacheMode,
    };

    fn fill_enabled_adapter() -> Arc<ObjectDataCacheAdapter> {
        Arc::new(
            ObjectDataCacheAdapter::new(ObjectDataCacheConfig {
                mode: ObjectDataCacheMode::FillMaterializeEnabled,
                max_bytes: 8_388_608,
                // Fill must not depend on the live memory reading (host vs container).
                min_free_memory_percent: 0,
                ..ObjectDataCacheConfig::default()
            })
            .expect("adapter"),
        )
    }

    fn request<'a>(bucket: &'a str, object: &'a str) -> ObjectDataCacheGetRequest<'a> {
        ObjectDataCacheGetRequest {
            bucket,
            object,
            etag: "etag",
            size: 5,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn mutation_hook_invalidates_cached_body() {
        let adapter = fill_enabled_adapter();
        let plan = adapter.plan_get(request("b", "k"));
        assert_eq!(
            adapter.fill_body(&plan, Bytes::from_static(b"hello")).await,
            ObjectDataCacheFillResult::Inserted
        );

        let hook = ObjectDataCacheMutationHook {
            adapter: Arc::clone(&adapter),
        };
        hook.after_object_mutation("b", "k").await;

        assert!(matches!(adapter.lookup_body(&plan).await, ObjectDataCacheLookup::Miss));
    }

    #[test]
    #[serial_test::serial(object_mutation_hook)]
    fn disabled_reload_unregisters_and_releases_previous_adapter() {
        let adapter = fill_enabled_adapter();
        let weak = Arc::downgrade(&adapter);
        register_object_data_cache_mutation_hook(adapter);

        let disabled = Arc::new(ObjectDataCacheAdapter::disabled());
        let disabled_weak = Arc::downgrade(&disabled);
        register_object_data_cache_mutation_hook(disabled);

        assert!(weak.upgrade().is_none());
        assert!(
            disabled_weak.upgrade().is_none(),
            "disabled reload must clear the mutation-hook slot instead of registering the disabled adapter"
        );
    }
}
