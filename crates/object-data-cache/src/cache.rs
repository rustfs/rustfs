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
use crate::entry::projected_weight;
use crate::error::ObjectDataCacheConfigError;
use crate::key::{ObjectDataCacheBodyVariant, ObjectDataCacheIdentity, ObjectDataCacheKey};
use crate::memory::ObjectDataCacheMemoryReservation;
use crate::metrics::{
    describe_metrics_once, publish_cache_state, record_fill_result, record_hit_bytes, record_invalidation, record_lookup_result,
    record_plan_decision,
};
use crate::moka_backend::MokaBackend;
use crate::noop::NoopBackend;
use crate::stats::{ObjectDataCacheStats, ObjectDataCacheStatsSnapshot};
use bytes::Bytes;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use tokio::sync::OwnedSemaphorePermit;

/// Admission token for one body allocation performed before a cold cache fill.
#[derive(Debug)]
pub struct ObjectDataCacheBodyReservation {
    pub(crate) memory: ObjectDataCacheMemoryReservation,
    pub(crate) permit: OwnedSemaphorePermit,
    pub(crate) fill_generation: crate::moka_backend::FillGenerationGuard,
    pub(crate) key: ObjectDataCacheKey,
    pub(crate) expected_size: u64,
}

/// A materialized body whose allocation owns its memory claim until the last
/// `Bytes` clone is dropped.
#[derive(Debug)]
pub struct ObjectDataCacheReservedBody {
    pub(crate) bytes: Bytes,
    pub(crate) permit: OwnedSemaphorePermit,
    pub(crate) fill_generation: crate::moka_backend::FillGenerationGuard,
    pub(crate) key: ObjectDataCacheKey,
    pub(crate) expected_size: u64,
}

impl ObjectDataCacheBodyReservation {
    /// Attaches this reservation to a newly materialized body.
    pub fn wrap_bytes(self, bytes: Bytes) -> ObjectDataCacheReservedBody {
        ObjectDataCacheReservedBody {
            bytes: self.memory.wrap_bytes(bytes),
            permit: self.permit,
            fill_generation: self.fill_generation,
            key: self.key,
            expected_size: self.expected_size,
        }
    }
}

impl ObjectDataCacheReservedBody {
    /// Returns a clone that shares the reservation-owning allocation.
    pub fn bytes(&self) -> Bytes {
        self.bytes.clone()
    }

    pub(crate) fn into_parts(self) -> (Bytes, OwnedSemaphorePermit, crate::moka_backend::FillGenerationGuard) {
        (self.bytes, self.permit, self.fill_generation)
    }
}

/// Minimum spacing between cache-state gauge publishes. Moka's `entry_count`
/// and `weighted_size` are cross-segment approximations that only settle after
/// pending tasks run, so republishing on every fill/invalidate lands the same
/// stale value thousands of times between scrapes (backlog#1134).
const ENTRY_COUNT_PUBLISH_DEBOUNCE_MS: u64 = 1000;

/// Invalidation outcome label: keys were dropped from the cache.
const INVALIDATION_OUTCOME_REMOVED: &str = "removed";
/// Invalidation outcome label: the identity was not cached, nothing removed.
const INVALIDATION_OUTCOME_NOOP: &str = "noop";

/// Protocol-neutral cache facade for object body reuse.
#[derive(Debug)]
pub struct ObjectDataCache {
    backend: ObjectDataCacheBackendKind,
    config: Arc<ObjectDataCacheConfig>,
    stats: Arc<ObjectDataCacheStats>,
    /// Resolved Moka weighted capacity used by the planner's exact key-aware
    /// admission check. Zero for the disabled backend.
    max_capacity: u64,
    /// Effective fill ceiling in bytes: a body larger than this is planned
    /// `SkipTooLarge` even when it fits `max_entry_bytes`. The app layer sets it
    /// to `min(max_entry_bytes, seek-support threshold, 64 MiB buffer cap)` so a
    /// `max_entry_bytes` above the in-memory GET fill limits is not reported as
    /// eligible while fill could never materialize it (backlog#1129 / ODC-24).
    /// `0` means "no additional clamp" — eligibility rests on `max_entry_bytes`
    /// alone (the default, used by engine-level tests). The concurrency-driven
    /// shrink of the fill threshold is dynamic and cannot be captured at plan
    /// time; this static ceiling is the conservative floor.
    fill_ceiling_bytes: u64,
    /// Monotonic origin for the cache-state publish debounce.
    created_at: Instant,
    /// Millis since `created_at` of the last cache-state gauge publish, or `0`
    /// when it has never published.
    last_entry_publish_ms: AtomicU64,
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
            max_capacity: 0,
            fill_ceiling_bytes: 0,
            created_at: Instant::now(),
            last_entry_publish_ms: AtomicU64::new(0),
        }
    }

    /// Creates a new cache facade.
    pub fn new(config: ObjectDataCacheConfig) -> Result<Self, ObjectDataCacheConfigError> {
        describe_metrics_once();
        config.validate()?;
        let stats = Arc::new(ObjectDataCacheStats::default());
        let (backend, max_capacity) = if config.is_disabled() {
            (ObjectDataCacheBackendKind::Noop(NoopBackend), 0)
        } else {
            let backend = MokaBackend::new(&config, Arc::clone(&stats))?;
            let max_capacity = backend.max_capacity();
            (ObjectDataCacheBackendKind::Moka(Box::new(backend)), max_capacity)
        };

        Ok(Self {
            backend,
            config: Arc::new(config),
            stats,
            max_capacity,
            fill_ceiling_bytes: 0,
            created_at: Instant::now(),
            last_entry_publish_ms: AtomicU64::new(0),
        })
    }

    /// Sets the effective fill ceiling (backlog#1129 / ODC-24). The app layer
    /// applies this at startup so a body above the in-memory GET fill limits is
    /// planned `SkipTooLarge` instead of being reported eligible. `0` disables
    /// the extra clamp.
    #[must_use]
    pub fn with_fill_ceiling_bytes(mut self, fill_ceiling_bytes: u64) -> Self {
        self.fill_ceiling_bytes = fill_ceiling_bytes;
        self
    }

    /// Effective size above which a body is planned `SkipTooLarge`:
    /// `max_entry_bytes` clamped by the fill ceiling when one is set.
    fn effective_size_ceiling(&self) -> u64 {
        if self.fill_ceiling_bytes == 0 {
            self.config.max_entry_bytes
        } else {
            self.config.max_entry_bytes.min(self.fill_ceiling_bytes)
        }
    }

    /// Produces a lightweight GET plan from request metadata.
    pub fn plan_get(&self, request: ObjectDataCacheGetRequest<'_>) -> ObjectDataCacheGetPlan {
        self.plan_get_inner(request, true)
    }

    /// Rebuilds a plan for identity revalidation without counting another GET.
    #[doc(hidden)]
    pub fn plan_get_untracked(&self, request: ObjectDataCacheGetRequest<'_>) -> ObjectDataCacheGetPlan {
        self.plan_get_inner(request, false)
    }

    fn plan_get_inner(&self, request: ObjectDataCacheGetRequest<'_>, record_metric: bool) -> ObjectDataCacheGetPlan {
        if self.config.is_disabled() {
            if record_metric {
                record_plan_decision(
                    self.backend.as_metric_label(),
                    self.config.mode,
                    "disabled",
                    "mode_disabled",
                    request.size,
                );
            }
            return ObjectDataCacheGetPlan::Disabled;
        }

        // ODC-24: clamp eligibility to the effective fill ceiling, not just
        // `max_entry_bytes`. A body in the gap between `max_entry_bytes` and the
        // in-memory GET fill limits could never fill, so admitting it here would
        // report it "eligible" while it kept a permanent 0% hit rate.
        if request.size > self.effective_size_ceiling() {
            if record_metric {
                record_plan_decision(self.backend.as_metric_label(), self.config.mode, "skip", "too_large", request.size);
            }
            return ObjectDataCacheGetPlan::SkipTooLarge;
        }

        let key = ObjectDataCacheKey::with_write_anchors(
            request.bucket,
            request.object,
            request.version_id.as_deref(),
            request.etag,
            request.size,
            request.data_dir_u128,
            request.mod_time_unix_nanos,
            request.body_variant,
        );
        if projected_weight(&key, request.size) > self.max_capacity {
            if record_metric {
                record_plan_decision(self.backend.as_metric_label(), self.config.mode, "skip", "too_large", request.size);
            }
            return ObjectDataCacheGetPlan::SkipTooLarge;
        }

        if record_metric {
            record_plan_decision(self.backend.as_metric_label(), self.config.mode, "cacheable", "eligible", request.size);
        }

        ObjectDataCacheGetPlan::Cacheable { key }
    }

    /// Looks up an object body from the configured backend.
    pub async fn lookup_body(&self, plan: &ObjectDataCacheGetPlan) -> ObjectDataCacheLookup {
        let lookup = match &self.backend {
            ObjectDataCacheBackendKind::Noop(backend) => backend.lookup_body(plan).await,
            ObjectDataCacheBackendKind::Moka(backend) => backend.lookup_body(plan).await,
        };

        self.stats.record_lookup(matches!(lookup, ObjectDataCacheLookup::Hit(_)));
        // Do not refresh the cache-state gauge on the lookup hot path: moka's
        // approximations do not change on a read, so it would only republish the
        // same stale value on every GET (backlog#1134).
        match &lookup {
            ObjectDataCacheLookup::Hit(bytes) => {
                let size_bytes = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
                record_lookup_result(self.backend.as_metric_label(), self.config.mode, "hit", size_bytes);
                record_hit_bytes(self.backend.as_metric_label(), self.config.mode, size_bytes);
            }
            ObjectDataCacheLookup::Miss => {
                let size_bytes = match plan {
                    ObjectDataCacheGetPlan::Cacheable { key } => key.size,
                    _ => 0,
                };
                record_lookup_result(self.backend.as_metric_label(), self.config.mode, "miss", size_bytes);
            }
            ObjectDataCacheLookup::SkipDisabled => {
                record_lookup_result(self.backend.as_metric_label(), self.config.mode, "skip_disabled", 0);
            }
            ObjectDataCacheLookup::SkipNotCacheable => {
                record_lookup_result(self.backend.as_metric_label(), self.config.mode, "skip_not_cacheable", 0);
            }
        }

        lookup
    }

    /// Performs an internal second-chance lookup without recording another
    /// request lookup. Callers must have already performed the authoritative
    /// lookup for the current GET.
    #[doc(hidden)]
    pub async fn peek_body_untracked(&self, plan: &ObjectDataCacheGetPlan) -> ObjectDataCacheLookup {
        match &self.backend {
            ObjectDataCacheBackendKind::Noop(backend) => backend.lookup_body(plan).await,
            ObjectDataCacheBackendKind::Moka(backend) => backend.lookup_body(plan).await,
        }
    }

    /// Reserves memory and a fill slot before allocating a cold-fill body.
    pub fn reserve_body(&self, plan: &ObjectDataCacheGetPlan) -> Option<ObjectDataCacheBodyReservation> {
        if !self.config.fill_enabled() {
            return None;
        }
        match &self.backend {
            ObjectDataCacheBackendKind::Noop(_) => None,
            ObjectDataCacheBackendKind::Moka(backend) => backend.reserve_body(plan),
        }
    }

    /// Fills from a body admitted before allocation.
    pub async fn fill_reserved_body(
        &self,
        plan: &ObjectDataCacheGetPlan,
        body: ObjectDataCacheReservedBody,
    ) -> ObjectDataCacheFillResult {
        let fill_bytes = u64::try_from(body.bytes.len()).unwrap_or(u64::MAX);
        let fill_start = Instant::now();
        let result = match &self.backend {
            ObjectDataCacheBackendKind::Noop(_) => ObjectDataCacheFillResult::SkippedDisabled,
            ObjectDataCacheBackendKind::Moka(backend) => backend.fill_reserved_body(plan, body).await,
        };
        let (recorded_bytes, duration) = match result {
            ObjectDataCacheFillResult::Inserted => {
                self.stats.record_fill();
                self.refresh_entry_count();
                (fill_bytes, Some(fill_start.elapsed().as_secs_f64()))
            }
            ObjectDataCacheFillResult::SkippedInvalidationRace | ObjectDataCacheFillResult::SkippedIdentityOverflow => {
                (fill_bytes, Some(fill_start.elapsed().as_secs_f64()))
            }
            _ => (0, None),
        };
        record_fill_result(
            self.backend.as_metric_label(),
            self.config.mode,
            result.as_metric_label(),
            recorded_bytes,
            duration,
        );
        result
    }

    /// Attempts to fill the cache body for the current plan.
    pub async fn fill_body(&self, plan: &ObjectDataCacheGetPlan, bytes: Bytes) -> ObjectDataCacheFillResult {
        let fill_bytes = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        if !self.config.fill_enabled() {
            // Never reached the backend: count the outcome, but record no bytes
            // (nothing was submitted) and no duration (there was no fill work).
            let result = ObjectDataCacheFillResult::SkippedByMode;
            record_fill_result(self.backend.as_metric_label(), self.config.mode, result.as_metric_label(), 0, None);
            return result;
        }

        if let ObjectDataCacheGetPlan::Cacheable { key } = plan
            && fill_bytes != key.size
        {
            // Never reached the backend either: count only.
            let result = ObjectDataCacheFillResult::SkippedSizeMismatch;
            record_fill_result(self.backend.as_metric_label(), self.config.mode, result.as_metric_label(), 0, None);
            return result;
        }

        let fill_start = Instant::now();
        let result = match &self.backend {
            ObjectDataCacheBackendKind::Noop(backend) => backend.fill_body(plan).await,
            ObjectDataCacheBackendKind::Moka(backend) => backend.fill_body(plan, bytes).await,
        };

        // Fill bytes and duration describe work the backend actually performed,
        // so each outcome is listed explicitly rather than caught by a wildcard:
        // a rejected fill wrote nothing and must not inflate fill_bytes_total.
        // See backlog#1123.
        let (recorded_bytes, duration) = match &result {
            // Inserted the body: the only outcome that moves the entry count.
            ObjectDataCacheFillResult::Inserted => {
                self.stats.record_fill();
                self.refresh_entry_count();
                (fill_bytes, Some(fill_start.elapsed().as_secs_f64()))
            }
            // Reached the backend and wrote the body, then undid it. The bytes
            // were written, so both are real.
            ObjectDataCacheFillResult::SkippedInvalidationRace | ObjectDataCacheFillResult::SkippedIdentityOverflow => {
                (fill_bytes, Some(fill_start.elapsed().as_secs_f64()))
            }
            // Rejected before writing anything: count the outcome, nothing else.
            // A `JoinedInflightFill` is already counted by singleflight_joins,
            // and its elapsed time would be wait time, not fill work.
            ObjectDataCacheFillResult::JoinedInflightFill
            | ObjectDataCacheFillResult::SkippedFillConcurrency
            | ObjectDataCacheFillResult::SkippedMemoryPressure
            | ObjectDataCacheFillResult::SkippedDisabled
            | ObjectDataCacheFillResult::SkippedByMode
            | ObjectDataCacheFillResult::SkippedNotCacheable
            | ObjectDataCacheFillResult::SkippedSizeMismatch => (0, None),
        };
        record_fill_result(
            self.backend.as_metric_label(),
            self.config.mode,
            result.as_metric_label(),
            recorded_bytes,
            duration,
        );

        result
    }

    /// Invalidates all cache entries associated with the object identity.
    pub async fn invalidate_object(
        &self,
        identity: ObjectDataCacheIdentity,
        reason: ObjectDataCacheInvalidationReason,
    ) -> ObjectDataCacheInvalidationResult {
        let result = match &self.backend {
            ObjectDataCacheBackendKind::Noop(backend) => backend.invalidate_object().await,
            ObjectDataCacheBackendKind::Moka(backend) => backend.invalidate_object(&identity).await,
        };
        self.finish_invalidation(result, reason)
    }

    /// Invalidates every cached body under `bucket`/`prefix`.
    ///
    /// Drops the cached bodies of every identity in `bucket` whose object key
    /// starts with `prefix`. Backed by a full identity-index scan, so it must
    /// stay on the rare force-delete and admin paths and never touch the GET or
    /// fill hot path (ODC-27, backlog#1132).
    pub async fn invalidate_prefix(
        &self,
        bucket: &str,
        prefix: &str,
        reason: ObjectDataCacheInvalidationReason,
    ) -> ObjectDataCacheInvalidationResult {
        let result = match &self.backend {
            ObjectDataCacheBackendKind::Noop(backend) => backend.invalidate_prefix().await,
            ObjectDataCacheBackendKind::Moka(backend) => backend.invalidate_prefix(bucket, prefix).await,
        };
        self.finish_invalidation(result, reason)
    }

    /// Invalidates every cached body in `bucket`.
    ///
    /// Backed by a full identity-index scan; keep it on the rare bucket-delete
    /// and admin paths only (ODC-28, backlog#1133).
    pub async fn invalidate_bucket(
        &self,
        bucket: &str,
        reason: ObjectDataCacheInvalidationReason,
    ) -> ObjectDataCacheInvalidationResult {
        let result = match &self.backend {
            ObjectDataCacheBackendKind::Noop(backend) => backend.invalidate_bucket().await,
            ObjectDataCacheBackendKind::Moka(backend) => backend.invalidate_bucket(bucket).await,
        };
        self.finish_invalidation(result, reason)
    }

    /// Drops every cached body and resets the identity index.
    ///
    /// The only production remediation for a poisoned or stale entry short of a
    /// node restart (ODC-C2, backlog#1143). Rare admin path only.
    pub async fn clear(&self, reason: ObjectDataCacheInvalidationReason) -> ObjectDataCacheInvalidationResult {
        let result = match &self.backend {
            ObjectDataCacheBackendKind::Noop(backend) => backend.clear().await,
            ObjectDataCacheBackendKind::Moka(backend) => backend.clear().await,
        };
        self.finish_invalidation(result, reason)
    }

    /// Shared post-processing for every invalidation primitive: bump the
    /// invalidation counter, refresh the cache-state gauge only when something
    /// was removed, and emit the outcome-labelled metric. A mutating op
    /// invalidates twice by design (before + after) and the vast majority of
    /// those touch identities that were never cached, so the no-op path skips
    /// the gauge refresh (backlog#1141).
    fn finish_invalidation(
        &self,
        result: ObjectDataCacheInvalidationResult,
        reason: ObjectDataCacheInvalidationReason,
    ) -> ObjectDataCacheInvalidationResult {
        self.stats.record_invalidation();
        let outcome = invalidation_outcome(&result);
        if outcome != INVALIDATION_OUTCOME_NOOP {
            self.refresh_entry_count();
        }
        record_invalidation(self.backend.as_metric_label(), reason.as_metric_label(), outcome);
        result
    }

    /// Returns the current stats snapshot.
    pub fn stats(&self) -> ObjectDataCacheStatsSnapshot {
        self.stats.snapshot()
    }

    /// Returns the configured runtime mode, for admin status reporting.
    pub fn mode(&self) -> crate::config::ObjectDataCacheMode {
        self.config.mode
    }

    /// Returns true when the cache facade is fully disabled.
    pub fn is_disabled(&self) -> bool {
        self.config.is_disabled()
    }

    /// Returns true when the cache mode allows materialize fill.
    pub fn materialize_fill_enabled(&self) -> bool {
        matches!(self.config.mode, crate::config::ObjectDataCacheMode::FillMaterializeEnabled)
    }

    /// Publishes the cache-state gauge (and mirrors it into stats), debounced so
    /// it fires at most once per [`ENTRY_COUNT_PUBLISH_DEBOUNCE_MS`]. Moka's
    /// `entry_count`/`weighted_size` are approximations that only settle after
    /// pending tasks run, so publishing on every fill/invalidate would restore
    /// the same stale value thousands of times between scrapes (backlog#1134).
    fn refresh_entry_count(&self) {
        let now_ms = u64::try_from(self.created_at.elapsed().as_millis())
            .unwrap_or(u64::MAX)
            // Reserve 0 as the "never published" sentinel.
            .max(1);
        let last = self.last_entry_publish_ms.load(Ordering::Relaxed);
        if last != 0 && now_ms.saturating_sub(last) < ENTRY_COUNT_PUBLISH_DEBOUNCE_MS {
            return;
        }
        self.last_entry_publish_ms.store(now_ms, Ordering::Relaxed);

        let (entries, weighted_bytes) = match &self.backend {
            ObjectDataCacheBackendKind::Noop(_) => (0, 0),
            ObjectDataCacheBackendKind::Moka(backend) => (backend.entry_count(), backend.weighted_size()),
        };
        self.stats.set_entries(entries);
        publish_cache_state(self.backend.as_metric_label(), entries, weighted_bytes);
    }
}

/// Maps an invalidation result to its metric outcome label.
const fn invalidation_outcome(result: &ObjectDataCacheInvalidationResult) -> &'static str {
    match result {
        ObjectDataCacheInvalidationResult::Removed { keys } if *keys > 0 => INVALIDATION_OUTCOME_REMOVED,
        ObjectDataCacheInvalidationResult::Removed { .. } | ObjectDataCacheInvalidationResult::NoOp => INVALIDATION_OUTCOME_NOOP,
        // Transitional: a backend that has not yet been widened to report the
        // removal count (`MokaBackend`) still returns `Success`. Treat it as a
        // removal so the gauge keeps refreshing until the backend reports
        // `Removed`/`NoOp` (see report / backlog#1141).
    }
}

/// Protocol-neutral GET request metadata for cache planning.
///
/// Derives `Default` so tests construct it with `..Default::default()`; a new
/// field then does not have to be added to every literal (which is how the
/// `mod_time` seam bug arose during backlog#1111).
#[derive(Debug, Clone, Default)]
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
    /// Resolved version's `data_dir` UUID as a `u128`, or `None`. Primary
    /// write-unique key component (backlog#1111 / ODC-06).
    pub data_dir_u128: Option<u128>,
    /// Resolved version's modification time as Unix nanoseconds, or `0` when
    /// absent. Second write-unique anchor.
    pub mod_time_unix_nanos: i128,
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

impl ObjectDataCacheGetPlan {
    /// Returns the stable cache key for a cacheable plan.
    pub fn key(&self) -> Option<&ObjectDataCacheKey> {
        match self {
            Self::Cacheable { key } => Some(key),
            Self::Disabled | Self::SkipTooLarge => None,
        }
    }
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
    /// Fill was skipped because the provided body length did not match the cache key identity.
    SkippedSizeMismatch,
    /// Fill was undone because an invalidation raced with the insert.
    SkippedInvalidationRace,
    /// The caller joined an in-flight leader fill instead of performing one, so
    /// it did no fill work of its own (the join is counted by
    /// `singleflight_joins`). Distinguishes waiters from true inserts so N
    /// concurrent GETs of one cold key do not record N inserts (backlog#1123).
    JoinedInflightFill,
    /// The cache entry was inserted successfully.
    Inserted,
    /// Fill was skipped because the fill-concurrency limiter was saturated.
    SkippedFillConcurrency,
}

impl ObjectDataCacheFillResult {
    pub(crate) const fn as_metric_label(&self) -> &'static str {
        match self {
            Self::SkippedDisabled => "skipped_disabled",
            Self::SkippedByMode => "skipped_by_mode",
            Self::SkippedNotCacheable => "skipped_not_cacheable",
            Self::SkippedMemoryPressure => "skipped_memory_pressure",
            Self::SkippedIdentityOverflow => "skipped_identity_overflow",
            Self::SkippedSizeMismatch => "skipped_size_mismatch",
            Self::SkippedInvalidationRace => "skipped_invalidation_race",
            Self::JoinedInflightFill => "joined_inflight",
            Self::Inserted => "inserted",
            Self::SkippedFillConcurrency => "skipped_fill_concurrency",
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
    /// Invalidation after an ecstore-internal lifecycle/scanner expiry deleted
    /// the object body (ODC-26).
    AfterLifecycleExpiry,
    /// Invalidation after a forced prefix delete removed every object under a
    /// prefix (ODC-27).
    AfterPrefixDelete,
    /// Invalidation after a bucket delete removed every object in the bucket
    /// (ODC-28).
    AfterBucketDelete,
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
            Self::AfterLifecycleExpiry => "after_lifecycle_expiry",
            Self::AfterPrefixDelete => "after_prefix_delete",
            Self::AfterBucketDelete => "after_bucket_delete",
            Self::Manual => "manual",
        }
    }
}

/// Result of an invalidation request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectDataCacheInvalidationResult {
    /// Cache keys were removed for the identity.
    Removed {
        /// Number of cache keys dropped.
        keys: usize,
    },
    /// The identity was not cached, so nothing was removed.
    NoOp,
}

#[cfg(test)]
mod tests {
    use super::{
        ObjectDataCache, ObjectDataCacheFillResult, ObjectDataCacheGetPlan, ObjectDataCacheGetRequest,
        ObjectDataCacheInvalidationReason, ObjectDataCacheInvalidationResult, ObjectDataCacheLookup,
    };
    use crate::config::{ObjectDataCacheConfig, ObjectDataCacheMode};
    use crate::key::ObjectDataCacheIdentity;
    use bytes::Bytes;
    use metrics_util::MetricKind;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};

    fn fill_enabled_cache() -> ObjectDataCache {
        let config = ObjectDataCacheConfig {
            mode: ObjectDataCacheMode::FillBufferedOnly,
            max_bytes: 8_388_608,
            // Fill must not depend on the live memory reading, which differs
            // between a developer host and a CI container.
            min_free_memory_percent: 0,
            ..ObjectDataCacheConfig::default()
        };
        ObjectDataCache::new(config).expect("fill-enabled cache config should initialize")
    }

    fn hit_only_cache() -> ObjectDataCache {
        let config = ObjectDataCacheConfig {
            mode: ObjectDataCacheMode::HitOnly,
            max_bytes: 8_388_608,
            ..ObjectDataCacheConfig::default()
        };
        ObjectDataCache::new(config).expect("hit-only cache config should initialize")
    }

    fn plain_request<'a>(bucket: &'a str, object: &'a str, etag: &'a str, size: u64) -> ObjectDataCacheGetRequest<'a> {
        ObjectDataCacheGetRequest {
            bucket,
            object,
            etag,
            size,
            ..Default::default()
        }
    }

    struct CapturedMetric {
        kind: MetricKind,
        name: String,
        labels: Vec<(String, String)>,
        value: DebugValue,
    }

    /// Runs `f` under a thread-local debugging recorder and a current-thread
    /// runtime, so every metric the async body emits is captured without
    /// touching the process-global registry. A current-thread runtime keeps the
    /// spawned fill tasks on the recorder's thread.
    fn capture_metrics<F, Fut>(f: F) -> Vec<CapturedMetric>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = ()>,
    {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("current-thread runtime should build");
        metrics::with_local_recorder(&recorder, || runtime.block_on(f()));
        snapshotter
            .snapshot()
            .into_vec()
            .into_iter()
            .map(|(composite, _unit, _desc, value)| {
                let labels = composite
                    .key()
                    .labels()
                    .map(|label| (label.key().to_string(), label.value().to_string()))
                    .collect();
                CapturedMetric {
                    kind: composite.kind(),
                    name: composite.key().name().to_string(),
                    labels,
                    value,
                }
            })
            .collect()
    }

    fn counter_total(metrics: &[CapturedMetric], name: &str) -> Option<u64> {
        let mut found = false;
        let mut sum = 0u64;
        for metric in metrics {
            if metric.kind == MetricKind::Counter && metric.name == name {
                found = true;
                if let DebugValue::Counter(v) = metric.value {
                    sum += v;
                }
            }
        }
        found.then_some(sum)
    }

    fn has_gauge(metrics: &[CapturedMetric], name: &str) -> bool {
        metrics.iter().any(|m| m.kind == MetricKind::Gauge && m.name == name)
    }

    fn has_counter_with_label(metrics: &[CapturedMetric], name: &str, label: (&str, &str)) -> bool {
        metrics.iter().any(|m| {
            m.kind == MetricKind::Counter && m.name == name && m.labels.iter().any(|(k, v)| k == label.0 && v == label.1)
        })
    }

    #[test]
    fn single_get_records_one_plan_and_one_lookup_increment() {
        // ODC-17: one GET must produce exactly one plan increment and one lookup
        // increment, no longer conflated on a single requests_total counter.
        let cache = hit_only_cache();
        let metrics = capture_metrics(|| async {
            let plan = cache.plan_get(plain_request("bucket", "object", "etag", 1024));
            let _ = cache.lookup_body(&plan).await;
        });

        assert_eq!(
            counter_total(&metrics, "rustfs_object_data_cache_plan_total"),
            Some(1),
            "exactly one plan decision per GET"
        );
        assert_eq!(
            counter_total(&metrics, "rustfs_object_data_cache_lookup_total"),
            Some(1),
            "exactly one lookup outcome per GET"
        );
    }

    #[test]
    fn lookup_does_not_publish_entry_count_gauge() {
        // ODC-29: the lookup hot path must not republish the cache-state gauge.
        let cache = hit_only_cache();
        let metrics = capture_metrics(|| async {
            let plan = cache.plan_get(plain_request("bucket", "object", "etag", 1024));
            let _ = cache.lookup_body(&plan).await;
        });

        assert!(
            !has_gauge(&metrics, "rustfs_object_data_cache_entries"),
            "lookup must not publish the entries gauge"
        );
    }

    #[test]
    fn joined_inflight_result_maps_to_metric_label() {
        // ODC-18: the waiter outcome has its own, distinct metric label.
        assert_eq!(ObjectDataCacheFillResult::JoinedInflightFill.as_metric_label(), "joined_inflight");
        assert_ne!(
            ObjectDataCacheFillResult::JoinedInflightFill.as_metric_label(),
            ObjectDataCacheFillResult::Inserted.as_metric_label()
        );
    }

    #[test]
    fn disabled_cache_invalidation_labels_noop_and_skips_gauge() {
        // ODC-36: invalidating an identity that was never cached is a no-op; it
        // is labeled outcome=noop and must not refresh the cache-state gauge.
        let cache = ObjectDataCache::disabled();
        let metrics = capture_metrics(|| async {
            let _ = cache
                .invalidate_object(
                    ObjectDataCacheIdentity::new("bucket", "object"),
                    ObjectDataCacheInvalidationReason::BeforeMutation,
                )
                .await;
        });

        assert!(
            has_counter_with_label(&metrics, "rustfs_object_data_cache_invalidations_total", ("outcome", "noop")),
            "a no-op invalidation must be labeled outcome=noop"
        );
        assert!(
            !has_gauge(&metrics, "rustfs_object_data_cache_entries"),
            "a no-op invalidation must not refresh the entries gauge"
        );
    }

    #[test]
    fn invalidation_of_cached_identity_labels_removed() {
        // ODC-36: invalidating an identity that held cached keys is labeled
        // outcome=removed.
        let cache = fill_enabled_cache();
        let metrics = capture_metrics(|| async {
            let plan = cache.plan_get(plain_request("bucket", "object", "etag", 5));
            let ObjectDataCacheGetPlan::Cacheable { .. } = &plan else {
                panic!("plan should be cacheable");
            };
            assert_eq!(
                cache.fill_body(&plan, Bytes::from_static(b"hello")).await,
                ObjectDataCacheFillResult::Inserted
            );
            let _ = cache
                .invalidate_object(
                    ObjectDataCacheIdentity::new("bucket", "object"),
                    ObjectDataCacheInvalidationReason::AfterDeleteSuccess,
                )
                .await;
        });

        assert!(
            has_counter_with_label(&metrics, "rustfs_object_data_cache_invalidations_total", ("outcome", "removed")),
            "invalidating a cached identity must be labeled outcome=removed"
        );
    }

    #[test]
    fn new_invalidation_reasons_map_to_distinct_labels() {
        assert_eq!(
            ObjectDataCacheInvalidationReason::AfterLifecycleExpiry.as_metric_label(),
            "after_lifecycle_expiry"
        );
        assert_eq!(
            ObjectDataCacheInvalidationReason::AfterPrefixDelete.as_metric_label(),
            "after_prefix_delete"
        );
        assert_eq!(
            ObjectDataCacheInvalidationReason::AfterBucketDelete.as_metric_label(),
            "after_bucket_delete"
        );
    }

    #[test]
    fn prefix_invalidation_of_cached_identity_labels_reason_and_removed() {
        // ODC-27: a prefix flush that drops a cached body is labelled with its
        // own reason and outcome=removed so dashboards attribute the churn.
        let cache = fill_enabled_cache();
        let metrics = capture_metrics(|| async {
            let plan = cache.plan_get(plain_request("bucket", "photos/a", "etag", 5));
            assert_eq!(
                cache.fill_body(&plan, Bytes::from_static(b"hello")).await,
                ObjectDataCacheFillResult::Inserted
            );
            let result = cache
                .invalidate_prefix("bucket", "photos/", ObjectDataCacheInvalidationReason::AfterPrefixDelete)
                .await;
            assert_eq!(result, ObjectDataCacheInvalidationResult::Removed { keys: 1 });
        });

        assert!(has_counter_with_label(
            &metrics,
            "rustfs_object_data_cache_invalidations_total",
            ("reason", "after_prefix_delete")
        ));
        assert!(has_counter_with_label(
            &metrics,
            "rustfs_object_data_cache_invalidations_total",
            ("outcome", "removed")
        ));
    }

    #[test]
    fn bucket_invalidation_of_uncached_bucket_labels_noop() {
        // ODC-28: a bucket flush that matched nothing is a no-op and must not
        // refresh the entries gauge.
        let cache = fill_enabled_cache();
        let metrics = capture_metrics(|| async {
            let result = cache
                .invalidate_bucket("empty-bucket", ObjectDataCacheInvalidationReason::AfterBucketDelete)
                .await;
            assert_eq!(result, ObjectDataCacheInvalidationResult::NoOp);
        });

        assert!(has_counter_with_label(
            &metrics,
            "rustfs_object_data_cache_invalidations_total",
            ("outcome", "noop")
        ));
        assert!(
            !has_gauge(&metrics, "rustfs_object_data_cache_entries"),
            "a no-op bucket flush must not refresh the entries gauge"
        );
    }

    #[test]
    fn clear_of_cached_cache_labels_manual_and_removed() {
        // ODC-C2: an admin clear reuses the Manual reason and reports removed.
        let cache = fill_enabled_cache();
        let metrics = capture_metrics(|| async {
            let plan = cache.plan_get(plain_request("bucket", "object", "etag", 5));
            assert_eq!(
                cache.fill_body(&plan, Bytes::from_static(b"hello")).await,
                ObjectDataCacheFillResult::Inserted
            );
            let result = cache.clear(ObjectDataCacheInvalidationReason::Manual).await;
            assert_eq!(result, ObjectDataCacheInvalidationResult::Removed { keys: 1 });
            assert!(matches!(cache.lookup_body(&plan).await, ObjectDataCacheLookup::Miss));
        });

        assert!(has_counter_with_label(
            &metrics,
            "rustfs_object_data_cache_invalidations_total",
            ("reason", "manual")
        ));
    }

    #[tokio::test]
    async fn fill_body_rejects_size_mismatch() {
        let cache = fill_enabled_cache();
        let plan = cache.plan_get(ObjectDataCacheGetRequest {
            bucket: "bucket",
            object: "object",
            etag: "etag",
            size: 5,
            ..Default::default()
        });

        let fill = cache.fill_body(&plan, Bytes::from_static(b"oops")).await;
        let lookup = cache.lookup_body(&plan).await;

        assert_eq!(fill, ObjectDataCacheFillResult::SkippedSizeMismatch);
        assert!(matches!(lookup, ObjectDataCacheLookup::Miss));
    }

    #[tokio::test]
    async fn reserved_body_is_bound_to_origin_cache_and_plan() {
        let cache_a = fill_enabled_cache();
        let cache_b = fill_enabled_cache();
        let plan = cache_a.plan_get(plain_request("bucket", "object", "etag", 5));
        let wrong_plan = cache_a.plan_get(plain_request("bucket", "other", "etag", 5));

        let wrong_plan_body = cache_a
            .reserve_body(&plan)
            .expect("the original plan should be admitted")
            .wrap_bytes(Bytes::from_static(b"hello"));
        assert_eq!(
            cache_a.fill_reserved_body(&wrong_plan, wrong_plan_body).await,
            ObjectDataCacheFillResult::SkippedNotCacheable
        );

        let cross_cache_body = cache_a
            .reserve_body(&plan)
            .expect("the original cache should admit another reservation")
            .wrap_bytes(Bytes::from_static(b"hello"));
        assert_eq!(
            cache_b.fill_reserved_body(&plan, cross_cache_body).await,
            ObjectDataCacheFillResult::SkippedNotCacheable
        );

        let wrong_size_body = cache_a
            .reserve_body(&plan)
            .expect("the original plan should still be admitted")
            .wrap_bytes(Bytes::from_static(b"oops"));
        assert_eq!(
            cache_a.fill_reserved_body(&plan, wrong_size_body).await,
            ObjectDataCacheFillResult::SkippedSizeMismatch
        );
        assert_eq!(cache_a.stats().fills, 0, "rejected reservations must not count as successful fills");
        assert!(matches!(cache_a.lookup_body(&plan).await, ObjectDataCacheLookup::Miss));
        assert!(matches!(cache_a.lookup_body(&wrong_plan).await, ObjectDataCacheLookup::Miss));
        assert!(matches!(cache_b.lookup_body(&plan).await, ObjectDataCacheLookup::Miss));

        let body = cache_a
            .reserve_body(&plan)
            .expect("the matching reservation should be admitted")
            .wrap_bytes(Bytes::from_static(b"hello"));
        assert_eq!(cache_a.fill_reserved_body(&plan, body).await, ObjectDataCacheFillResult::Inserted);
        assert_eq!(cache_a.stats().fills, 1);
        assert_eq!(cache_a.lookup_body(&plan).await, ObjectDataCacheLookup::Hit(Bytes::from_static(b"hello")));
    }

    #[test]
    fn reserved_size_mismatch_records_fill_outcome_without_bytes() {
        let cache = fill_enabled_cache();
        let metrics = capture_metrics(|| async {
            let plan = cache.plan_get(plain_request("bucket", "object", "etag", 5));
            let body = cache
                .reserve_body(&plan)
                .expect("the plan should be admitted before materialization")
                .wrap_bytes(Bytes::from_static(b"oops"));
            assert_eq!(
                cache.fill_reserved_body(&plan, body).await,
                ObjectDataCacheFillResult::SkippedSizeMismatch
            );
        });

        assert!(has_counter_with_label(
            &metrics,
            "rustfs_object_data_cache_fill_total",
            ("result", "skipped_size_mismatch")
        ));
        assert_eq!(
            counter_total(&metrics, "rustfs_object_data_cache_fill_bytes_total"),
            None,
            "a rejected reserved body must not record filled bytes"
        );
    }

    #[test]
    fn plan_clamps_size_eligibility_to_fill_ceiling() {
        // ODC-24 (backlog#1129): a body in the gap between `max_entry_bytes` and
        // the effective fill ceiling must plan `SkipTooLarge`, not `Cacheable`,
        // so it stops being reported as eligible while it can never fill.
        let config = ObjectDataCacheConfig {
            mode: ObjectDataCacheMode::HitOnly,
            max_bytes: 32 * 1024 * 1024,
            max_memory_percent: 0,
            max_entry_bytes: 16 * 1024 * 1024,
            ..ObjectDataCacheConfig::default()
        };
        let cache = ObjectDataCache::new(config)
            .expect("clamped cache config should initialize")
            .with_fill_ceiling_bytes(4 * 1024 * 1024);

        // Within the ceiling: eligible.
        assert!(matches!(
            cache.plan_get(plain_request("bucket", "object", "etag", 4 * 1024 * 1024)),
            ObjectDataCacheGetPlan::Cacheable { .. }
        ));
        // In the gap (ceiling < size <= max_entry_bytes): skipped as too large.
        assert_eq!(
            cache.plan_get(plain_request("bucket", "object", "etag", 4 * 1024 * 1024 + 1)),
            ObjectDataCacheGetPlan::SkipTooLarge
        );
        assert_eq!(
            cache.plan_get(plain_request("bucket", "object", "etag", 16 * 1024 * 1024)),
            ObjectDataCacheGetPlan::SkipTooLarge
        );
    }

    #[test]
    fn planner_rejects_key_whose_projected_weight_exceeds_capacity() {
        let config = ObjectDataCacheConfig {
            mode: ObjectDataCacheMode::HitOnly,
            max_bytes: 8 * 1024,
            max_memory_percent: 0,
            max_entry_bytes: 4 * 1024,
            ..ObjectDataCacheConfig::default()
        };
        let cache = ObjectDataCache::new(config).expect("capacity test config should initialize");
        // Body (4096) + fixed fields (bucket=1, version=null=4, etag=1) +
        // object (4026) + entry overhead (64) = 8192, exactly capacity.
        let boundary_object = "o".repeat(4026);
        assert!(matches!(
            cache.plan_get(plain_request("b", &boundary_object, "e", 4 * 1024)),
            ObjectDataCacheGetPlan::Cacheable { .. }
        ));

        // Body (4096) + fixed fields (bucket=1, version=null=4, etag=1) +
        // object (4027) + entry overhead (64) = 8193, one byte over capacity.
        let overweight_object = "o".repeat(4027);

        assert_eq!(
            cache.plan_get(plain_request("b", &overweight_object, "e", 4 * 1024)),
            ObjectDataCacheGetPlan::SkipTooLarge
        );
    }

    #[test]
    fn plan_carries_mod_time_into_key() {
        // ODC-06: the resolved modification time must reach the key so two
        // writes with identical etag + size derive different keys.
        let cache = hit_only_cache();
        let request = ObjectDataCacheGetRequest {
            bucket: "bucket",
            object: "object",
            etag: "etag",
            size: 5,
            mod_time_unix_nanos: 42,
            ..Default::default()
        };
        let ObjectDataCacheGetPlan::Cacheable { key } = cache.plan_get(request) else {
            panic!("plan should be cacheable");
        };
        assert_eq!(key.mod_time_unix_nanos, 42);
    }
}
