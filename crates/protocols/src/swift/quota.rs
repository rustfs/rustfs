// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Container Quota Support for Swift API
//!
//! This module implements container quotas that limit the size and/or number
//! of objects that can be stored in a container. Quotas are enforced during
//! PUT operations and reject uploads that would exceed configured limits.
//!
//! # Configuration
//!
//! Quotas are configured via container metadata:
//!
//! - `X-Container-Meta-Quota-Bytes`: Maximum total bytes allowed in container
//! - `X-Container-Meta-Quota-Count`: Maximum number of objects allowed in container
//!
//! # Usage
//!
//! ```bash
//! # Set byte quota (10 GB)
//! swift post my-container -H "X-Container-Meta-Quota-Bytes: 10737418240"
//!
//! # Set object count quota (1000 objects)
//! swift post my-container -H "X-Container-Meta-Quota-Count: 1000"
//!
//! # Set both quotas
//! swift post my-container \
//!   -H "X-Container-Meta-Quota-Bytes: 10737418240" \
//!   -H "X-Container-Meta-Quota-Count: 1000"
//!
//! # Remove quotas
//! swift post my-container \
//!   -H "X-Remove-Container-Meta-Quota-Bytes:" \
//!   -H "X-Remove-Container-Meta-Quota-Count:"
//! ```
//!
//! # Enforcement
//!
//! When a PUT request would cause the container to exceed its quota:
//! - Request is rejected with 413 Payload Too Large
//! - Response includes quota headers showing current usage
//! - Object is not uploaded
//!
//! # Example
//!
//! ```bash
//! # Container has quota of 1GB
//! swift post my-container -H "X-Container-Meta-Quota-Bytes: 1073741824"
//!
//! # Current usage: 900MB, uploading 200MB file
//! swift upload my-container large-file.bin
//!
//! # Response: 413 Payload Too Large
//! # X-Container-Meta-Quota-Bytes: 1073741824
//! # X-Container-Bytes-Used: 943718400
//! ```

use super::account::validate_account_access;
use super::container::ContainerMapper;
use super::{SwiftError, SwiftResult, container};
use rustfs_credentials::Credentials;
use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex};
use tracing::debug;

const LOG_COMPONENT_PROTOCOLS: &str = "protocols";
const LOG_SUBSYSTEM_SWIFT_QUOTA: &str = "swift_quota";
const EVENT_SWIFT_QUOTA_STATE: &str = "swift_quota_state";

#[derive(Debug, Default, Clone, Copy)]
struct InFlightUsage {
    bytes: u64,
    objects: u64,
}

struct BucketQuotaSlot {
    /// Serializes quota snapshot read + admission + reservation increment per bucket.
    /// Never held across the object payload transfer itself.
    admission_lock: tokio::sync::Mutex<()>,
    in_flight: Mutex<InFlightUsage>,
}

impl BucketQuotaSlot {
    fn new() -> Self {
        Self {
            admission_lock: tokio::sync::Mutex::new(()),
            in_flight: Mutex::new(InFlightUsage::default()),
        }
    }

    fn current_in_flight(&self) -> InFlightUsage {
        match self.in_flight.lock() {
            Ok(guard) => *guard,
            Err(poisoned) => *poisoned.into_inner(),
        }
    }

    fn add_in_flight(&self, bytes: u64, objects: u64) {
        let mut guard = match self.in_flight.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.bytes = guard.bytes.saturating_add(bytes);
        guard.objects = guard.objects.saturating_add(objects);
    }

    fn release_in_flight(&self, bytes: u64, objects: u64) {
        let mut guard = match self.in_flight.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.bytes = guard.bytes.saturating_sub(bytes);
        guard.objects = guard.objects.saturating_sub(objects);
    }
}

static BUCKET_QUOTA_SLOTS: LazyLock<Mutex<HashMap<String, Arc<BucketQuotaSlot>>>> = LazyLock::new(|| Mutex::new(HashMap::new()));

fn bucket_quota_slot(bucket: &str) -> Arc<BucketQuotaSlot> {
    let mut slots = match BUCKET_QUOTA_SLOTS.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    Arc::clone(
        slots
            .entry(bucket.to_string())
            .or_insert_with(|| Arc::new(BucketQuotaSlot::new())),
    )
}

struct ActiveQuotaReservation {
    slot: Arc<BucketQuotaSlot>,
    bytes: u64,
    objects: u64,
}

/// RAII reservation guard holding in-flight Swift container quota usage until
/// the write finishes updating the usage overlay or aborts/cancels.
#[must_use = "QuotaReservationGuard must stay alive across the object write"]
pub struct QuotaReservationGuard {
    active: Option<ActiveQuotaReservation>,
}

impl QuotaReservationGuard {
    /// No-op guard used when a container has no quotas configured.
    pub(crate) fn noop() -> Self {
        Self { active: None }
    }

    /// Returns `true` when this guard tracks an active in-flight reservation.
    pub fn is_active(&self) -> bool {
        self.active.is_some()
    }

    /// Commit the in-memory usage overlay update and release the in-flight
    /// reservation atomically under the bucket's admission lock.
    ///
    /// Keeps `self` intact across both `.await` points so cancellation or panic
    /// at either point still runs [`Drop`] and releases the in-flight reservation.
    pub(crate) async fn finish_with_overlay<F, Fut>(self, update_overlay: F)
    where
        F: FnOnce() -> Fut + Send,
        Fut: std::future::Future<Output = ()> + Send,
    {
        if let Some(slot) = self.active.as_ref().map(|r| Arc::clone(&r.slot)) {
            let _admission = slot.admission_lock.lock().await;
            update_overlay().await;
            drop(self);
        } else {
            update_overlay().await;
        }
    }
}

impl Drop for QuotaReservationGuard {
    fn drop(&mut self) {
        if let Some(reservation) = self.active.take() {
            reservation.slot.release_in_flight(reservation.bytes, reservation.objects);
        }
    }
}

/// Quota configuration for a container
#[derive(Debug, Clone, Default)]
pub struct QuotaConfig {
    /// Maximum total bytes allowed in container
    pub quota_bytes: Option<u64>,

    /// Maximum number of objects allowed in container
    pub quota_count: Option<u64>,
}

impl QuotaConfig {
    /// Parse quota configuration from Swift container custom metadata.
    ///
    /// Supports both the canonical stripped metadata keys produced by
    /// `MetadataUpdate::from_container_headers` (`quota-bytes`, `quota-count`)
    /// and the full header-prefixed keys (`x-container-meta-quota-*`).
    pub fn from_custom_metadata(custom_metadata: &HashMap<String, String>) -> Self {
        let mut config = QuotaConfig::default();

        if let Some(quota_bytes_str) = custom_metadata
            .get("quota-bytes")
            .or_else(|| custom_metadata.get("x-container-meta-quota-bytes"))
        {
            config.quota_bytes = quota_bytes_str.trim().parse().ok();
        }

        if let Some(quota_count_str) = custom_metadata
            .get("quota-count")
            .or_else(|| custom_metadata.get("x-container-meta-quota-count"))
        {
            config.quota_count = quota_count_str.trim().parse().ok();
        }

        config
    }

    /// Load quota configuration from container metadata
    pub async fn load(account: &str, container_name: &str, credentials: &Credentials) -> SwiftResult<Self> {
        let custom_metadata = container::get_container_custom_metadata(account, container_name, credentials).await?;
        Ok(Self::from_custom_metadata(&custom_metadata))
    }

    /// Check if any quotas are configured
    pub fn is_enabled(&self) -> bool {
        self.quota_bytes.is_some() || self.quota_count.is_some()
    }

    /// Check if adding one object would exceed quotas
    ///
    /// Returns Ok(()) if within quota, Err with 413 if exceeded
    pub fn check_quota(&self, current_bytes: u64, current_count: u64, additional_bytes: Option<u64>) -> SwiftResult<()> {
        self.check_quota_for_objects(current_bytes, current_count, additional_bytes, 1)
    }

    /// Check if adding `additional_objects` objects totalling `additional_bytes` would exceed quotas.
    pub fn check_quota_for_objects(
        &self,
        current_bytes: u64,
        current_count: u64,
        additional_bytes: Option<u64>,
        additional_objects: u64,
    ) -> SwiftResult<()> {
        // Check byte quota
        if let Some(max_bytes) = self.quota_bytes {
            let additional_bytes = additional_bytes.ok_or_else(|| {
                SwiftError::LengthRequired("Content-Length is required when a byte quota is configured".to_string())
            })?;
            let new_bytes = current_bytes.saturating_add(additional_bytes);
            if new_bytes > max_bytes {
                return Err(SwiftError::RequestEntityTooLarge(format!(
                    "Upload would exceed quota-bytes limit: {} + {} > {}",
                    current_bytes, additional_bytes, max_bytes
                )));
            }
        }

        // Check count quota
        if let Some(max_count) = self.quota_count {
            let new_count = current_count.saturating_add(additional_objects);
            if new_count > max_count {
                return Err(SwiftError::RequestEntityTooLarge(format!(
                    "Upload would exceed quota-count limit: {} + {} > {}",
                    current_count, additional_objects, max_count
                )));
            }
        }

        Ok(())
    }
}

async fn reserve_with_slot<F, Fut>(
    slot: Arc<BucketQuotaSlot>,
    quota: &QuotaConfig,
    object_size: Option<u64>,
    object_count: u64,
    load_usage: F,
) -> SwiftResult<QuotaReservationGuard>
where
    F: FnOnce() -> Fut + Send,
    Fut: std::future::Future<Output = SwiftResult<(u64, u64)>> + Send,
{
    if !quota.is_enabled() {
        return Ok(QuotaReservationGuard::noop());
    }

    let _admission = slot.admission_lock.lock().await;
    let (used_bytes, used_objects) = load_usage().await?;
    let in_flight = slot.current_in_flight();

    let effective_bytes = used_bytes.saturating_add(in_flight.bytes);
    let effective_objects = used_objects.saturating_add(in_flight.objects);

    quota.check_quota_for_objects(effective_bytes, effective_objects, object_size, object_count)?;

    let reserved_bytes = object_size.unwrap_or(0);
    slot.add_in_flight(reserved_bytes, object_count);
    drop(_admission);

    debug!(
        event = EVENT_SWIFT_QUOTA_STATE,
        component = LOG_COMPONENT_PROTOCOLS,
        subsystem = LOG_SUBSYSTEM_SWIFT_QUOTA,
        state = "reserved",
        current_bytes = used_bytes,
        in_flight_bytes = in_flight.bytes,
        quota_bytes = ?quota.quota_bytes,
        current_count = used_objects,
        in_flight_objects = in_flight.objects,
        quota_count = ?quota.quota_count,
        object_size = ?object_size,
        object_count,
        "swift quota state changed"
    );

    Ok(QuotaReservationGuard {
        active: Some(ActiveQuotaReservation {
            slot,
            bytes: reserved_bytes,
            objects: object_count,
        }),
    })
}

/// Atomically check and reserve container quota for an upcoming Swift upload.
///
/// The returned [`QuotaReservationGuard`] accounts for the upload's bytes and
/// object count while the write is in flight so concurrent uploads cannot race
/// past the configured quota. Hold the guard until the object commit and
/// usage-overlay update complete.
pub async fn reserve_upload_quota(
    account: &str,
    container_name: &str,
    object_size: Option<u64>,
    object_count: u64,
    credentials: &Credentials,
) -> SwiftResult<QuotaReservationGuard> {
    let project_id = validate_account_access(account, credentials)?;
    let bucket_name = ContainerMapper::default().swift_to_s3_bucket(container_name, &project_id);

    let quota = QuotaConfig::load(account, container_name, credentials).await?;
    if !quota.is_enabled() {
        return Ok(QuotaReservationGuard::noop());
    }

    let slot = bucket_quota_slot(&bucket_name);
    reserve_with_slot(slot, &quota, object_size, object_count, || async {
        let metadata = container::get_container_metadata(account, container_name, credentials).await?;
        Ok((metadata.bytes_used, metadata.object_count))
    })
    .await
}

/// Check if upload would exceed container quotas (including in-flight reservations).
///
/// Returns Ok(()) if quota not exceeded or not configured
/// Returns Err(SwiftError::RequestEntityTooLarge) if quota would be exceeded
pub async fn check_upload_quota(
    account: &str,
    container_name: &str,
    object_size: Option<u64>,
    credentials: &Credentials,
) -> SwiftResult<()> {
    let project_id = validate_account_access(account, credentials)?;
    let bucket_name = ContainerMapper::default().swift_to_s3_bucket(container_name, &project_id);

    let quota = QuotaConfig::load(account, container_name, credentials).await?;
    if !quota.is_enabled() {
        return Ok(());
    }

    let slot = bucket_quota_slot(&bucket_name);
    let _admission = slot.admission_lock.lock().await;
    let metadata = container::get_container_metadata(account, container_name, credentials).await?;
    let in_flight = slot.current_in_flight();

    let effective_bytes = metadata.bytes_used.saturating_add(in_flight.bytes);
    let effective_count = metadata.object_count.saturating_add(in_flight.objects);
    quota.check_quota(effective_bytes, effective_count, object_size)?;

    debug!(
        event = EVENT_SWIFT_QUOTA_STATE,
        component = LOG_COMPONENT_PROTOCOLS,
        subsystem = LOG_SUBSYSTEM_SWIFT_QUOTA,
        state = "check_passed",
        current_bytes = metadata.bytes_used,
        in_flight_bytes = in_flight.bytes,
        quota_bytes = ?quota.quota_bytes,
        current_count = metadata.object_count,
        in_flight_objects = in_flight.objects,
        quota_count = ?quota.quota_count,
        object_size = ?object_size,
        "swift quota state changed"
    );

    Ok(())
}

/// Check if quotas are enabled for a container
pub async fn is_enabled(account: &str, container_name: &str, credentials: &Credentials) -> SwiftResult<bool> {
    match QuotaConfig::load(account, container_name, credentials).await {
        Ok(config) => Ok(config.is_enabled()),
        Err(_) => Ok(false), // Container doesn't exist or no quotas configured
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_quota_config_default() {
        let config = QuotaConfig::default();
        assert!(!config.is_enabled());
        assert!(config.quota_bytes.is_none());
        assert!(config.quota_count.is_none());
    }

    #[test]
    fn test_quota_config_enabled_bytes() {
        let config = QuotaConfig {
            quota_bytes: Some(1000),
            quota_count: None,
        };
        assert!(config.is_enabled());
    }

    #[test]
    fn test_quota_config_enabled_count() {
        let config = QuotaConfig {
            quota_bytes: None,
            quota_count: Some(100),
        };
        assert!(config.is_enabled());
    }

    #[test]
    fn test_quota_config_enabled_both() {
        let config = QuotaConfig {
            quota_bytes: Some(1000),
            quota_count: Some(100),
        };
        assert!(config.is_enabled());
    }

    #[test]
    fn test_check_quota_within_bytes_limit() {
        let config = QuotaConfig {
            quota_bytes: Some(1000),
            quota_count: None,
        };

        // Current: 500 bytes, adding 400 bytes = 900 total (within 1000 limit)
        let result = config.check_quota(500, 0, Some(400));
        assert!(result.is_ok());
    }

    #[test]
    fn test_check_quota_exceeds_bytes_limit() {
        let config = QuotaConfig {
            quota_bytes: Some(1000),
            quota_count: None,
        };

        // Current: 500 bytes, adding 600 bytes = 1100 total (exceeds 1000 limit)
        let result = config.check_quota(500, 0, Some(600));
        assert!(result.is_err());
        match result {
            Err(SwiftError::RequestEntityTooLarge(msg)) => {
                assert!(msg.contains("quota-bytes"));
            }
            _ => panic!("Expected RequestEntityTooLarge error"),
        }
    }

    #[test]
    fn test_check_quota_exact_bytes_limit() {
        let config = QuotaConfig {
            quota_bytes: Some(1000),
            quota_count: None,
        };

        // Current: 500 bytes, adding 500 bytes = 1000 total (exactly at limit)
        let result = config.check_quota(500, 0, Some(500));
        assert!(result.is_ok());
    }

    #[test]
    fn test_check_quota_within_count_limit() {
        let config = QuotaConfig {
            quota_bytes: None,
            quota_count: Some(10),
        };

        // Current: 5 objects, adding 1 = 6 total (within 10 limit)
        let result = config.check_quota(0, 5, Some(100));
        assert!(result.is_ok());
    }

    #[test]
    fn test_check_quota_exceeds_count_limit() {
        let config = QuotaConfig {
            quota_bytes: None,
            quota_count: Some(10),
        };

        // Current: 10 objects, adding 1 = 11 total (exceeds 10 limit)
        let result = config.check_quota(0, 10, Some(100));
        assert!(result.is_err());
        match result {
            Err(SwiftError::RequestEntityTooLarge(msg)) => {
                assert!(msg.contains("quota-count"));
            }
            _ => panic!("Expected RequestEntityTooLarge error"),
        }
    }

    #[test]
    fn test_check_quota_exact_count_limit() {
        let config = QuotaConfig {
            quota_bytes: None,
            quota_count: Some(10),
        };

        // Current: 9 objects, adding 1 = 10 total (exactly at limit)
        let result = config.check_quota(0, 9, Some(100));
        assert!(result.is_ok());
    }

    #[test]
    fn test_check_quota_both_limits_within() {
        let config = QuotaConfig {
            quota_bytes: Some(1000),
            quota_count: Some(10),
        };

        // Both within limits
        let result = config.check_quota(500, 5, Some(400));
        assert!(result.is_ok());
    }

    #[test]
    fn test_check_quota_bytes_exceeded_count_within() {
        let config = QuotaConfig {
            quota_bytes: Some(1000),
            quota_count: Some(10),
        };

        // Bytes exceeded, count within
        let result = config.check_quota(500, 5, Some(600));
        assert!(result.is_err());
    }

    #[test]
    fn test_check_quota_count_exceeded_bytes_within() {
        let config = QuotaConfig {
            quota_bytes: Some(1000),
            quota_count: Some(10),
        };

        // Count exceeded, bytes within
        let result = config.check_quota(500, 10, Some(100));
        assert!(result.is_err());
    }

    #[test]
    fn test_check_quota_no_limits() {
        let config = QuotaConfig {
            quota_bytes: None,
            quota_count: None,
        };

        // No limits, should always pass
        let result = config.check_quota(999999, 999999, Some(999999));
        assert!(result.is_ok());
    }

    #[test]
    fn test_check_quota_zero_bytes_limit() {
        let config = QuotaConfig {
            quota_bytes: Some(0),
            quota_count: None,
        };

        // Zero limit means no uploads allowed
        let result = config.check_quota(0, 0, Some(1));
        assert!(result.is_err());
    }

    #[test]
    fn test_check_quota_zero_count_limit() {
        let config = QuotaConfig {
            quota_bytes: None,
            quota_count: Some(0),
        };

        // Zero limit means no objects allowed
        let result = config.check_quota(0, 0, Some(100));
        assert!(result.is_err());
    }

    #[test]
    fn test_quota_overflow_protection() {
        let config = QuotaConfig {
            quota_bytes: Some(u64::MAX),
            quota_count: Some(u64::MAX),
        };

        // Test saturating_add protection
        let result = config.check_quota(u64::MAX - 100, 0, Some(200));
        // Should saturate to u64::MAX and compare against quota
        assert!(result.is_ok());
    }

    #[test]
    fn byte_quota_rejects_upload_without_content_length() {
        let config = QuotaConfig {
            quota_bytes: Some(1000),
            quota_count: None,
        };

        assert!(matches!(config.check_quota(500, 0, None), Err(SwiftError::LengthRequired(_))));
    }

    #[test]
    fn count_only_quota_checks_upload_without_content_length() {
        let config = QuotaConfig {
            quota_bytes: None,
            quota_count: Some(10),
        };

        assert!(matches!(config.check_quota(0, 10, None), Err(SwiftError::RequestEntityTooLarge(_))));
    }

    #[test]
    fn quota_config_from_custom_metadata_parses_canonical_and_legacy_keys() {
        let mut canonical = std::collections::HashMap::new();
        canonical.insert("quota-bytes".to_string(), "4096".to_string());
        canonical.insert("quota-count".to_string(), "7".to_string());
        let parsed = QuotaConfig::from_custom_metadata(&canonical);
        assert_eq!(parsed.quota_bytes, Some(4096));
        assert_eq!(parsed.quota_count, Some(7));

        let mut legacy = std::collections::HashMap::new();
        legacy.insert("x-container-meta-quota-bytes".to_string(), "8192".to_string());
        legacy.insert("x-container-meta-quota-count".to_string(), "12".to_string());
        let parsed_legacy = QuotaConfig::from_custom_metadata(&legacy);
        assert_eq!(parsed_legacy.quota_bytes, Some(8192));
        assert_eq!(parsed_legacy.quota_count, Some(12));
    }

    #[test]
    fn check_quota_for_objects_enforces_multi_object_writes() {
        let config = QuotaConfig {
            quota_bytes: Some(10_000),
            quota_count: Some(2),
        };
        // 1 existing object + 2 new objects (SLO manifest + marker) = 3 > 2 -> reject.
        assert!(matches!(
            config.check_quota_for_objects(0, 1, Some(100), 2),
            Err(SwiftError::RequestEntityTooLarge(_))
        ));
        // 0 existing + 2 new = 2 <= 2 -> allow.
        assert!(config.check_quota_for_objects(0, 0, Some(100), 2).is_ok());
    }

    #[tokio::test]
    async fn concurrent_byte_quota_reservations_prevent_race_and_release_on_drop() {
        let slot = Arc::new(BucketQuotaSlot::new());
        let quota = QuotaConfig {
            quota_bytes: Some(1000),
            quota_count: None,
        };
        let barrier = Arc::new(tokio::sync::Barrier::new(8));

        let mut handles = Vec::new();
        for _ in 0..8 {
            let slot = Arc::clone(&slot);
            let quota = quota.clone();
            let barrier = Arc::clone(&barrier);
            handles.push(tokio::spawn(async move {
                barrier.wait().await;
                reserve_with_slot(slot, &quota, Some(600), 1, || async { Ok((0, 0)) }).await
            }));
        }

        let mut granted = Vec::new();
        let mut rejected = 0usize;
        for handle in handles {
            match handle.await.expect("task should not panic") {
                Ok(guard) => granted.push(guard),
                Err(SwiftError::RequestEntityTooLarge(_)) => rejected += 1,
                Err(other) => panic!("unexpected error: {other:?}"),
            }
        }

        assert_eq!(granted.len(), 1, "at most one 600-byte upload fits in 1000-byte quota");
        assert_eq!(rejected, 7);
        assert_eq!(slot.current_in_flight().bytes, 600);

        // Dropping the held guard immediately releases the in-flight reservation.
        drop(granted);
        assert_eq!(slot.current_in_flight().bytes, 0);
        let next = reserve_with_slot(Arc::clone(&slot), &quota, Some(600), 1, || async { Ok((0, 0)) })
            .await
            .expect("reservation must succeed once prior guard is dropped");
        drop(next);
        assert_eq!(slot.current_in_flight().bytes, 0);
    }

    #[tokio::test]
    async fn concurrent_count_quota_reservations_prevent_race() {
        let slot = Arc::new(BucketQuotaSlot::new());
        let quota = QuotaConfig {
            quota_bytes: None,
            quota_count: Some(2),
        };
        let barrier = Arc::new(tokio::sync::Barrier::new(6));

        let mut handles = Vec::new();
        for _ in 0..6 {
            let slot = Arc::clone(&slot);
            let quota = quota.clone();
            let barrier = Arc::clone(&barrier);
            handles.push(tokio::spawn(async move {
                barrier.wait().await;
                reserve_with_slot(slot, &quota, Some(10), 1, || async { Ok((0, 0)) }).await
            }));
        }

        let mut granted = Vec::new();
        let mut rejected = 0usize;
        for handle in handles {
            match handle.await.expect("task should not panic") {
                Ok(guard) => granted.push(guard),
                Err(SwiftError::RequestEntityTooLarge(_)) => rejected += 1,
                Err(other) => panic!("unexpected error: {other:?}"),
            }
        }

        assert_eq!(granted.len(), 2, "exactly two uploads fit in count=2 quota");
        assert_eq!(rejected, 4);
        assert_eq!(slot.current_in_flight().objects, 2);

        drop(granted.pop());
        assert_eq!(slot.current_in_flight().objects, 1);
        let replacement = reserve_with_slot(Arc::clone(&slot), &quota, Some(10), 1, || async { Ok((0, 0)) })
            .await
            .expect("releasing one guard must free one object slot");
        drop(replacement);
        drop(granted);
        assert_eq!(slot.current_in_flight().objects, 0);
    }

    #[tokio::test]
    async fn finish_with_overlay_releases_in_flight_on_cancellation() {
        use std::future::Future;
        use std::pin::pin;
        use std::task::{Context, Poll};

        let slot = Arc::new(BucketQuotaSlot::new());
        let quota = QuotaConfig {
            quota_bytes: Some(1000),
            quota_count: Some(5),
        };

        // 1. Cancel while suspended waiting on `admission_lock`.
        let guard = reserve_with_slot(Arc::clone(&slot), &quota, Some(400), 2, || async { Ok((0, 0)) })
            .await
            .expect("reservation should succeed");
        assert_eq!(slot.current_in_flight().bytes, 400);
        assert_eq!(slot.current_in_flight().objects, 2);

        let held_lock = slot.admission_lock.lock().await;
        {
            let mut fut = pin!(guard.finish_with_overlay(|| async {}));
            let waker = futures::task::noop_waker();
            let mut cx = Context::from_waker(&waker);
            assert!(
                matches!(fut.as_mut().poll(&mut cx), Poll::Pending),
                "finish_with_overlay must suspend while admission_lock is held"
            );
            // Dropping `fut` here simulates task cancellation mid-await.
        }
        drop(held_lock);

        assert_eq!(slot.current_in_flight().bytes, 0, "cancelled finish_with_overlay must not leak bytes");
        assert_eq!(
            slot.current_in_flight().objects,
            0,
            "cancelled finish_with_overlay must not leak object count"
        );

        // 2. Cancel while suspended inside `update_overlay().await`.
        let guard2 = reserve_with_slot(Arc::clone(&slot), &quota, Some(300), 1, || async { Ok((0, 0)) })
            .await
            .expect("second reservation should succeed");
        {
            let mut fut = pin!(guard2.finish_with_overlay(std::future::pending::<()>));
            let waker = futures::task::noop_waker();
            let mut cx = Context::from_waker(&waker);
            assert!(matches!(fut.as_mut().poll(&mut cx), Poll::Pending));
        }
        assert_eq!(slot.current_in_flight().bytes, 0);
        assert_eq!(slot.current_in_flight().objects, 0);
    }
}
