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

use crate::backend::ObjectDataCacheBackendKind;
use crate::config::ObjectDataCacheConfig;
use crate::error::ObjectDataCacheConfigError;
use crate::key::{ObjectDataCacheBodyVariant, ObjectDataCacheIdentity, ObjectDataCacheKey};
use crate::metrics::{
    describe_metrics_once, publish_cache_state, record_fill_result, record_hit_bytes, record_invalidation,
    record_request_decision,
};
use crate::moka_backend::MokaBackend;
use crate::noop::NoopBackend;
use crate::stats::{ObjectDataCacheStats, ObjectDataCacheStatsSnapshot};
use bytes::Bytes;
use std::sync::Arc;
use std::time::Instant;

/// Protocol-neutral cache facade for object body reuse.
#[derive(Debug)]
pub struct ObjectDataCache {
    backend: ObjectDataCacheBackendKind,
    config: Arc<ObjectDataCacheConfig>,
    stats: Arc<ObjectDataCacheStats>,
}

impl ObjectDataCache {
    /// Creates a disabled cache facade without requiring configuration parsing.
    pub fn disabled() -> Self {
        let config = Arc::new(ObjectDataCacheConfig::default());
        let stats = Arc::new(ObjectDataCacheStats::default());

        Self {
            backend: ObjectDataCacheBackendKind::Noop(NoopBackend),
            config,
            stats,
        }
    }

    /// Creates a new cache facade.
    pub fn new(config: ObjectDataCacheConfig) -> Result<Self, ObjectDataCacheConfigError> {
        describe_metrics_once();
        config.validate()?;
        let stats = Arc::new(ObjectDataCacheStats::default());
        let backend = if config.is_disabled() {
            ObjectDataCacheBackendKind::Noop(NoopBackend)
        } else {
            ObjectDataCacheBackendKind::Moka(MokaBackend::new(&config, Arc::clone(&stats))?)
        };

        Ok(Self {
            backend,
            config: Arc::new(config),
            stats,
        })
    }

    /// Produces a lightweight GET plan from request metadata.
    pub fn plan_get(&self, request: ObjectDataCacheGetRequest<'_>) -> ObjectDataCacheGetPlan {
        if self.config.is_disabled() {
            record_request_decision(
                self.backend.as_metric_label(),
                self.config.mode,
                "disabled",
                "mode_disabled",
                request.size,
            );
            return ObjectDataCacheGetPlan::Disabled;
        }

        if request.size > self.config.max_entry_bytes {
            record_request_decision(self.backend.as_metric_label(), self.config.mode, "skip", "too_large", request.size);
            return ObjectDataCacheGetPlan::SkipTooLarge;
        }

        record_request_decision(self.backend.as_metric_label(), self.config.mode, "cacheable", "eligible", request.size);

        ObjectDataCacheGetPlan::Cacheable {
            key: ObjectDataCacheKey::new(
                request.bucket,
                request.object,
                request.version_id.as_deref(),
                request.etag,
                request.size,
                request.body_variant,
            ),
        }
    }

    /// Looks up an object body from the configured backend.
    pub async fn lookup_body(&self, plan: &ObjectDataCacheGetPlan) -> ObjectDataCacheLookup {
        let lookup = match &self.backend {
            ObjectDataCacheBackendKind::Noop(backend) => backend.lookup_body(plan).await,
            ObjectDataCacheBackendKind::Moka(backend) => backend.lookup_body(plan).await,
        };

        self.stats.record_lookup(matches!(lookup, ObjectDataCacheLookup::Hit(_)));
        self.refresh_entry_count();
        match &lookup {
            ObjectDataCacheLookup::Hit(bytes) => {
                let size_bytes = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
                record_request_decision(self.backend.as_metric_label(), self.config.mode, "hit", "cache_hit", size_bytes);
                record_hit_bytes(self.backend.as_metric_label(), self.config.mode, size_bytes);
            }
            ObjectDataCacheLookup::Miss => {
                let size_bytes = match plan {
                    ObjectDataCacheGetPlan::Cacheable { key } => key.size,
                    _ => 0,
                };
                record_request_decision(self.backend.as_metric_label(), self.config.mode, "miss", "cache_miss", size_bytes);
            }
            ObjectDataCacheLookup::SkipDisabled => {
                record_request_decision(self.backend.as_metric_label(), self.config.mode, "skip", "lookup_disabled", 0);
            }
            ObjectDataCacheLookup::SkipNotCacheable => {
                record_request_decision(self.backend.as_metric_label(), self.config.mode, "skip", "lookup_not_cacheable", 0);
            }
        }

        lookup
    }

    /// Attempts to fill the cache body for the current plan.
    pub async fn fill_body(&self, plan: &ObjectDataCacheGetPlan, bytes: Bytes) -> ObjectDataCacheFillResult {
        let fill_bytes = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        if !self.config.fill_enabled() {
            record_fill_result(self.backend.as_metric_label(), self.config.mode, "skipped_by_mode", fill_bytes, 0.0);
            return ObjectDataCacheFillResult::SkippedByMode;
        }

        let fill_start = Instant::now();
        let result = match &self.backend {
            ObjectDataCacheBackendKind::Noop(backend) => backend.fill_body(plan).await,
            ObjectDataCacheBackendKind::Moka(backend) => backend.fill_body(plan, bytes).await,
        };

        if matches!(result, ObjectDataCacheFillResult::Inserted) {
            self.stats.record_fill();
        }
        self.refresh_entry_count();
        record_fill_result(
            self.backend.as_metric_label(),
            self.config.mode,
            result.as_metric_label(),
            fill_bytes,
            fill_start.elapsed().as_secs_f64(),
        );

        result
    }

    /// Invalidates all cache entries associated with the object identity.
    pub async fn invalidate_object(
        &self,
        _identity: ObjectDataCacheIdentity,
        _reason: ObjectDataCacheInvalidationReason,
    ) -> ObjectDataCacheInvalidationResult {
        let result = match &self.backend {
            ObjectDataCacheBackendKind::Noop(backend) => backend.invalidate_object().await,
            ObjectDataCacheBackendKind::Moka(backend) => backend.invalidate_object(&_identity).await,
        };

        self.stats.record_invalidation();
        self.refresh_entry_count();
        record_invalidation(self.backend.as_metric_label(), _reason.as_metric_label());

        result
    }

    /// Returns the current stats snapshot.
    pub fn stats(&self) -> ObjectDataCacheStatsSnapshot {
        self.stats.snapshot()
    }

    /// Returns true when the cache facade is fully disabled.
    pub fn is_disabled(&self) -> bool {
        self.config.is_disabled()
    }

    /// Returns true when the cache mode allows materialize fill.
    pub fn materialize_fill_enabled(&self) -> bool {
        matches!(self.config.mode, crate::config::ObjectDataCacheMode::FillMaterializeEnabled)
    }

    fn refresh_entry_count(&self) {
        let (entries, weighted_bytes) = match &self.backend {
            ObjectDataCacheBackendKind::Noop(_) => (0, 0),
            ObjectDataCacheBackendKind::Moka(backend) => (backend.entry_count(), backend.weighted_size()),
        };
        self.stats.set_entries(entries);
        publish_cache_state(self.backend.as_metric_label(), entries, weighted_bytes);
    }
}

/// Protocol-neutral GET request metadata for cache planning.
#[derive(Debug, Clone)]
pub struct ObjectDataCacheGetRequest<'a> {
    /// Bucket name.
    pub bucket: &'a str,
    /// Object key.
    pub object: &'a str,
    /// Optional version id.
    pub version_id: Option<String>,
    /// Object ETag.
    pub etag: &'a str,
    /// Object size in bytes.
    pub size: u64,
    /// Supported response body variant.
    pub body_variant: ObjectDataCacheBodyVariant,
}

/// Planning result for a cache-aware GET.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObjectDataCacheGetPlan {
    /// Cache is globally disabled.
    Disabled,
    /// Object body exceeds the configured cacheable entry size.
    SkipTooLarge,
    /// Request is eligible for cache lookup and fill.
    Cacheable {
        /// Stable cache key for the request.
        key: ObjectDataCacheKey,
    },
}

/// Result of a cache lookup attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObjectDataCacheLookup {
    /// Cache is disabled.
    SkipDisabled,
    /// Request was not cacheable under the current plan.
    SkipNotCacheable,
    /// Cache did not contain a matching object body.
    Miss,
    /// Cache returned a reusable object body.
    Hit(Bytes),
}

/// Result of a cache fill attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObjectDataCacheFillResult {
    /// Cache is disabled.
    SkippedDisabled,
    /// Cache mode does not currently allow fill.
    SkippedByMode,
    /// Request was not cacheable under the current plan.
    SkippedNotCacheable,
    /// Fill was skipped because the local memory gate rejected it.
    SkippedMemoryPressure,
    /// Fill was skipped because the per-identity key budget overflowed and was conservatively cleared.
    SkippedIdentityOverflow,
    /// Fill waiters were released without a published leader result.
    SkippedSingleflightClosed,
    /// The cache entry was inserted successfully.
    Inserted,
}

impl ObjectDataCacheFillResult {
    pub(crate) const fn as_metric_label(&self) -> &'static str {
        match self {
            Self::SkippedDisabled => "skipped_disabled",
            Self::SkippedByMode => "skipped_by_mode",
            Self::SkippedNotCacheable => "skipped_not_cacheable",
            Self::SkippedMemoryPressure => "skipped_memory_pressure",
            Self::SkippedIdentityOverflow => "skipped_identity_overflow",
            Self::SkippedSingleflightClosed => "skipped_singleflight_closed",
            Self::Inserted => "inserted",
        }
    }
}

/// Invalidation reason placeholder for the skeleton.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectDataCacheInvalidationReason {
    /// Conservative invalidation before a mutating write or delete begins.
    BeforeMutation,
    /// Invalidation after a successful PutObject write.
    AfterPutSuccess,
    /// Invalidation after a successful delete.
    AfterDeleteSuccess,
    /// Invalidation after a successful copy destination write.
    AfterCopySuccess,
    /// Invalidation after a successful complete multipart upload.
    AfterCompleteMultipartSuccess,
    /// Manual invalidation requested by the caller.
    Manual,
}

impl ObjectDataCacheInvalidationReason {
    pub(crate) const fn as_metric_label(self) -> &'static str {
        match self {
            Self::BeforeMutation => "before_mutation",
            Self::AfterPutSuccess => "after_put_success",
            Self::AfterDeleteSuccess => "after_delete_success",
            Self::AfterCopySuccess => "after_copy_success",
            Self::AfterCompleteMultipartSuccess => "after_complete_multipart_success",
            Self::Manual => "manual",
        }
    }
}

/// Result of an invalidation request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectDataCacheInvalidationResult {
    /// Invalidation completed successfully.
    Success,
}
