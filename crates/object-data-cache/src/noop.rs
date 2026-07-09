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

use crate::cache::{ObjectDataCacheFillResult, ObjectDataCacheGetPlan, ObjectDataCacheInvalidationResult, ObjectDataCacheLookup};

/// Lightweight no-op backend for the initial skeleton.
#[derive(Debug, Default)]
pub struct NoopBackend;

impl NoopBackend {
    /// Returns a disabled lookup result.
    pub async fn lookup_body(&self, _plan: &ObjectDataCacheGetPlan) -> ObjectDataCacheLookup {
        ObjectDataCacheLookup::SkipDisabled
    }

    /// Returns a skipped fill result.
    pub async fn fill_body(&self, _plan: &ObjectDataCacheGetPlan) -> ObjectDataCacheFillResult {
        ObjectDataCacheFillResult::SkippedDisabled
    }

    /// Returns a successful no-op invalidation result.
    pub async fn invalidate_object(&self) -> ObjectDataCacheInvalidationResult {
        ObjectDataCacheInvalidationResult::Success
    }
}

#[cfg(test)]
mod tests {
    use super::NoopBackend;
    use crate::cache::{
        ObjectDataCacheFillResult, ObjectDataCacheGetPlan, ObjectDataCacheInvalidationResult, ObjectDataCacheLookup,
    };

    #[tokio::test]
    async fn noop_backend_returns_disabled_results() {
        let backend = NoopBackend;
        let plan = ObjectDataCacheGetPlan::Disabled;

        let lookup = backend.lookup_body(&plan).await;
        let fill = backend.fill_body(&plan).await;
        let invalidation = backend.invalidate_object().await;

        assert!(matches!(lookup, ObjectDataCacheLookup::SkipDisabled));
        assert!(matches!(fill, ObjectDataCacheFillResult::SkippedDisabled));
        assert!(matches!(invalidation, ObjectDataCacheInvalidationResult::Success));
    }
}
