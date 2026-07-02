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
use rustfs_object_data_cache::{
    ObjectDataCache, ObjectDataCacheConfig, ObjectDataCacheConfigError, ObjectDataCacheFillResult, ObjectDataCacheGetPlan,
    ObjectDataCacheGetRequest, ObjectDataCacheIdentity, ObjectDataCacheInvalidationReason, ObjectDataCacheInvalidationResult,
    ObjectDataCacheLookup,
};
use std::sync::Arc;

/// App-layer wrapper around the engine-only object data cache.
#[derive(Debug, Clone)]
pub(crate) struct ObjectDataCacheAdapter {
    cache: Arc<ObjectDataCache>,
}

impl ObjectDataCacheAdapter {
    /// Creates an adapter from validated cache configuration.
    pub(crate) fn new(config: ObjectDataCacheConfig) -> Result<Self, ObjectDataCacheConfigError> {
        Ok(Self {
            cache: Arc::new(ObjectDataCache::new(config)?),
        })
    }

    /// Creates a disabled no-op adapter.
    pub(crate) fn disabled() -> Self {
        Self {
            cache: Arc::new(ObjectDataCache::disabled()),
        }
    }

    /// Creates a shared disabled no-op adapter.
    pub(crate) fn disabled_arc() -> Arc<Self> {
        Arc::new(Self::disabled())
    }

    /// Returns the underlying shared cache handle.
    #[cfg(test)]
    pub(crate) fn cache(&self) -> Arc<ObjectDataCache> {
        Arc::clone(&self.cache)
    }

    /// Returns true when the adapter is wired to a disabled cache.
    pub(crate) fn is_disabled(&self) -> bool {
        self.cache.is_disabled()
    }

    /// Returns true when the adapter allows materialize fill.
    pub(crate) fn materialize_fill_enabled(&self) -> bool {
        self.cache.materialize_fill_enabled()
    }

    /// Builds an engine-level GET plan.
    pub(crate) fn plan_get(&self, request: ObjectDataCacheGetRequest<'_>) -> ObjectDataCacheGetPlan {
        self.cache.plan_get(request)
    }

    /// Executes an engine-level cache lookup.
    pub(crate) async fn lookup_body(&self, plan: &ObjectDataCacheGetPlan) -> ObjectDataCacheLookup {
        self.cache.lookup_body(plan).await
    }

    /// Executes an engine-level cache fill.
    pub(crate) async fn fill_body(&self, plan: &ObjectDataCacheGetPlan, bytes: Bytes) -> ObjectDataCacheFillResult {
        self.cache.fill_body(plan, bytes).await
    }

    /// Executes an engine-level object invalidation.
    pub(crate) async fn invalidate_object(
        &self,
        identity: ObjectDataCacheIdentity,
        reason: ObjectDataCacheInvalidationReason,
    ) -> ObjectDataCacheInvalidationResult {
        self.cache.invalidate_object(identity, reason).await
    }
}

impl Default for ObjectDataCacheAdapter {
    fn default() -> Self {
        Self::disabled()
    }
}

#[cfg(test)]
mod tests {
    use super::ObjectDataCacheAdapter;
    use rustfs_object_data_cache::ObjectDataCacheConfig;

    #[test]
    fn disabled_adapter_exposes_disabled_engine() {
        let adapter = ObjectDataCacheAdapter::disabled();

        assert!(adapter.is_disabled());
        assert!(adapter.cache().is_disabled());
    }

    #[test]
    fn adapter_new_accepts_default_disabled_config() {
        let adapter = ObjectDataCacheAdapter::new(ObjectDataCacheConfig::default()).expect("disabled config should initialize");

        assert!(adapter.is_disabled());
    }
}
