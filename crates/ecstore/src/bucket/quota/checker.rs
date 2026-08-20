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

use super::{BucketQuota, QuotaCheckResult, QuotaError, QuotaOperation};
use crate::bucket::metadata_sys::{BucketMetadataSys, update, update_if_incarnation};
use crate::data_usage::get_bucket_usage_memory;
use rustfs_common::metrics::Metric;
use rustfs_config::QUOTA_CONFIG_FILE;
use std::sync::Arc;
use std::time::Instant;
use time::OffsetDateTime;
use tokio::sync::RwLock;
use tracing::{debug, warn};

pub struct QuotaChecker {
    metadata_sys: Arc<RwLock<BucketMetadataSys>>,
}

impl QuotaChecker {
    pub fn new(metadata_sys: Arc<RwLock<BucketMetadataSys>>) -> Self {
        Self { metadata_sys }
    }

    pub async fn check_quota(
        &self,
        bucket: &str,
        operation: QuotaOperation,
        operation_size: u64,
    ) -> Result<QuotaCheckResult, QuotaError> {
        self.check_quota_with_usage_reporting(bucket, operation, operation_size, false)
            .await
    }

    /// Check quota with option to force usage calculation even when no quota is configured
    pub async fn check_quota_with_usage_reporting(
        &self,
        bucket: &str,
        operation: QuotaOperation,
        operation_size: u64,
        force_usage_calculation: bool,
    ) -> Result<QuotaCheckResult, QuotaError> {
        let start_time = Instant::now();
        let quota_config = self.get_quota_config(bucket).await?;
        let uses_durable_reservations = quota_config.uses_durable_reservations();

        // If no quota limit is set, allow operation
        let quota_limit = match quota_config.quota {
            None => {
                let current_usage = if force_usage_calculation {
                    Some(self.get_real_time_usage(bucket).await?)
                } else {
                    None // Skip expensive usage calculation when no quota and not forced for performance
                };
                return Ok(QuotaCheckResult {
                    allowed: true,
                    current_usage,
                    quota_limit: None,
                    operation_size,
                    remaining: None,
                    uses_durable_reservations,
                });
            }
            Some(q) => q,
        };

        let current_usage = self.get_real_time_usage(bucket).await?;

        // The reporting path projects this operation; storage mutations reserve it at commit.
        let admission_size = if uses_durable_reservations && !force_usage_calculation {
            0
        } else {
            operation_size
        };
        let expected_usage = match operation {
            QuotaOperation::PutObject | QuotaOperation::PostObject | QuotaOperation::CopyObject => {
                current_usage.saturating_add(admission_size)
            }
            QuotaOperation::DeleteObject => current_usage.saturating_sub(operation_size),
        };

        let allowed = match operation {
            QuotaOperation::PutObject | QuotaOperation::PostObject | QuotaOperation::CopyObject => {
                quota_config.check_operation_allowed(current_usage, admission_size)
            }
            QuotaOperation::DeleteObject => true,
        };

        let remaining = if quota_limit >= expected_usage {
            Some(quota_limit - expected_usage)
        } else {
            Some(0)
        };

        if !allowed {
            warn!(
                "Quota exceeded for bucket: {}, current: {}, limit: {}, attempted: {}",
                bucket, current_usage, quota_limit, operation_size
            );
        }

        let result = QuotaCheckResult {
            allowed,
            current_usage: Some(current_usage),
            quota_limit: Some(quota_limit),
            operation_size,
            remaining,
            uses_durable_reservations,
        };

        let duration = start_time.elapsed();
        // inc_time is now a plain fn (not async) — no .await needed.
        rustfs_common::metrics::Metrics::inc_time(Metric::QuotaCheck, duration);
        if !allowed {
            rustfs_common::metrics::Metrics::inc_time(Metric::QuotaViolation, duration);
        }

        Ok(result)
    }

    pub async fn get_quota_config(&self, bucket: &str) -> Result<BucketQuota, QuotaError> {
        // `get_config`, not the map-only `get()`: a bucket with no persisted
        // metadata must resolve to the fabricated default (no quota
        // configured) so the admission check passes and the request reaches
        // the NoSuchBucket answer — a map-only miss would fail every such
        // PUT closed with 503 before the 404 could be produced. Real read
        // faults still surface as errors and keep the fail-closed behavior.
        let (meta, _) = self
            .metadata_sys
            .read()
            .await
            .get_config(bucket)
            .await
            .map_err(QuotaError::StorageError)?;

        if meta.quota_config_json.is_empty() {
            debug!("No quota config found for bucket: {}, using default", bucket);
            return Ok(BucketQuota::new(None));
        }

        let quota: BucketQuota = serde_json::from_slice(&meta.quota_config_json).map_err(|e| QuotaError::InvalidConfig {
            reason: format!("Failed to parse quota config: {}", e),
        })?;

        Ok(quota)
    }

    pub async fn set_quota_config(&mut self, bucket: &str, quota: BucketQuota) -> Result<OffsetDateTime, QuotaError> {
        self.set_quota_config_for_incarnation(bucket, quota, None).await
    }

    pub async fn set_quota_config_if_incarnation(
        &mut self,
        bucket: &str,
        quota: BucketQuota,
        expected_incarnation_id: uuid::Uuid,
    ) -> Result<OffsetDateTime, QuotaError> {
        self.set_quota_config_for_incarnation(bucket, quota, Some(expected_incarnation_id))
            .await
    }

    pub async fn set_durable_quota_config_if_incarnation(
        &mut self,
        bucket: &str,
        quota: BucketQuota,
        expected_incarnation_id: uuid::Uuid,
        proof: &crate::services::notification_sys::CrossPoolFenceFleetProofToken,
    ) -> Result<OffsetDateTime, QuotaError> {
        let json_data = serde_json::to_vec(&quota).map_err(|e| QuotaError::InvalidConfig {
            reason: format!("Failed to serialize quota config: {}", e),
        })?;
        let start_time = Instant::now();
        let updated_at =
            crate::bucket::metadata_sys::update_quota_if_incarnation(bucket, json_data, expected_incarnation_id, proof)
                .await
                .map_err(QuotaError::StorageError)?;

        rustfs_common::metrics::Metrics::inc_time(Metric::QuotaSync, start_time.elapsed());
        Ok(updated_at)
    }

    async fn set_quota_config_for_incarnation(
        &mut self,
        bucket: &str,
        quota: BucketQuota,
        expected_incarnation_id: Option<uuid::Uuid>,
    ) -> Result<OffsetDateTime, QuotaError> {
        let json_data = serde_json::to_vec(&quota).map_err(|e| QuotaError::InvalidConfig {
            reason: format!("Failed to serialize quota config: {}", e),
        })?;
        let start_time = Instant::now();

        let updated_at = match expected_incarnation_id {
            Some(incarnation_id) => update_if_incarnation(bucket, QUOTA_CONFIG_FILE, json_data, incarnation_id).await,
            None => update(bucket, QUOTA_CONFIG_FILE, json_data).await,
        }
        .map_err(QuotaError::StorageError)?;

        rustfs_common::metrics::Metrics::inc_time(Metric::QuotaSync, start_time.elapsed());
        Ok(updated_at)
    }

    pub async fn get_quota_stats(&self, bucket: &str) -> Result<(BucketQuota, Option<u64>), QuotaError> {
        // If bucket doesn't exist, return ConfigNotFound error
        if !self.bucket_exists(bucket).await {
            return Err(QuotaError::ConfigNotFound {
                bucket: bucket.to_string(),
            });
        }

        let quota = self.get_quota_config(bucket).await?;
        let current_usage = self.get_real_time_usage(bucket).await?;

        Ok((quota, Some(current_usage)))
    }

    pub async fn bucket_exists(&self, bucket: &str) -> bool {
        self.metadata_sys.read().await.get(bucket).await.is_ok()
    }

    pub async fn get_real_time_usage(&self, bucket: &str) -> Result<u64, QuotaError> {
        if let Some(usage) = get_bucket_usage_memory(bucket).await {
            return Ok(usage);
        }

        // Degraded window (issue #5716): with no authoritative usage — most
        // prominently after upgrading from a pre-v2 release, whose legacy
        // `.usage.json` is demoted to non-authoritative until the scanner's
        // first complete cycle persists `.usage.v2.json` — failing closed
        // turned every write to a quota-enabled bucket into a retryable 503
        // for the whole window. Quota admission instead degrades to the last
        // persisted per-bucket size. That baseline is static between snapshot
        // loads (live writes do not advance it), so hard-quota enforcement is
        // advisory for the duration of the window: the overrun is bounded by
        // the writes issued before the next complete scanner cycle. Buckets
        // with no persisted baseline anywhere keep failing closed.
        let store = self.metadata_sys.read().await.object_store();
        // Box the fallback: it embeds the whole snapshot-load future, and every
        // object write nests a quota check several futures deep, so keeping it
        // inline would grow each write's state machine by the loader's full
        // size — the debug-build 2MiB worker-stack overflow class fixed for
        // bucket-config writes in #5648. The allocation only happens on the
        // degraded path; the authoritative fast path returns above.
        if let Some(baseline) = Box::pin(crate::data_usage::lookup_degraded_bucket_usage_baseline(store, bucket)).await {
            debug!(bucket, baseline, "Bucket quota admission using degraded persisted usage baseline");
            return Ok(baseline);
        }

        Err(QuotaError::UsageUnavailable {
            bucket: bucket.to_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bucket::metadata_sys::test_support::isolated_store_over_temp_disks;
    use serial_test::serial;
    use uuid::Uuid;

    /// Regression (PR #5307 / s3-tests `test_100_continue_error_retry`): a
    /// bucket with no persisted metadata has no quota, so the admission check
    /// must pass and let the request reach its NoSuchBucket answer. With the
    /// map-only `get()` this failed closed as a retryable 503 on every PUT to
    /// a nonexistent bucket.
    #[tokio::test]
    async fn quota_check_allows_bucket_without_persisted_metadata() {
        let (_dirs, ecstore) = isolated_store_over_temp_disks().await;
        let sys = Arc::new(RwLock::new(BucketMetadataSys::new(ecstore)));
        let checker = QuotaChecker::new(sys);

        let result = checker
            .check_quota("no-such-bucket", QuotaOperation::PutObject, 1024)
            .await
            .expect("a bucket with no persisted metadata has no quota and must not fail the check");
        assert!(result.allowed);
        assert_eq!(result.quota_limit, None);
    }

    /// Regression (issue #5716): an upgrade from a pre-v2 release leaves only
    /// the legacy `.usage.json` snapshot, which has no completeness marker and
    /// is demoted to non-authoritative, and the scanner's first complete cycle
    /// can be a long way off. Quota admission must degrade to that persisted
    /// baseline instead of failing every write to a quota-enabled bucket with
    /// a retryable 503 for the whole window.
    #[tokio::test]
    #[serial]
    async fn quota_admission_falls_back_to_legacy_snapshot_baseline() {
        let (_dirs, ecstore) = isolated_store_over_temp_disks().await;
        let sys = Arc::new(RwLock::new(BucketMetadataSys::new(ecstore.clone())));
        let checker = QuotaChecker::new(sys);
        let bucket = format!("quota-legacy-{}", Uuid::new_v4().simple());

        let mut legacy = rustfs_data_usage::DataUsageInfo {
            last_update: Some(std::time::SystemTime::now()),
            buckets_count: 1,
            ..Default::default()
        };
        legacy.buckets_usage.insert(
            bucket.clone(),
            rustfs_data_usage::BucketUsageInfo {
                size: 1_234,
                ..Default::default()
            },
        );
        legacy.bucket_sizes.insert(bucket.clone(), 1_234);
        // usage_snapshot_complete stays false: pre-v2 snapshots do not carry
        // the field at all, so they always deserialize as incomplete.
        let legacy_path = format!("{}/{}", crate::disk::BUCKET_META_PREFIX, rustfs_data_usage::LEGACY_DATA_USAGE_OBJECT_NAME);
        crate::config::com::save_config(
            ecstore.clone(),
            &legacy_path,
            serde_json::to_vec(&legacy).expect("legacy snapshot should encode"),
        )
        .await
        .expect("legacy snapshot fixture should be stored");
        crate::data_usage::invalidate_data_usage_snapshot_cache().await;

        let usage = checker
            .get_real_time_usage(&bucket)
            .await
            .expect("quota admission must degrade to the persisted legacy baseline");
        assert_eq!(usage, 1_234);

        // A bucket absent from every persisted snapshot still has no grounded
        // baseline and must keep failing closed.
        let unknown = format!("quota-unknown-{}", Uuid::new_v4().simple());
        assert!(matches!(
            checker.get_real_time_usage(&unknown).await,
            Err(QuotaError::UsageUnavailable { .. })
        ));

        // Deleting the bucket's usage from the backend must purge the
        // baseline: a recreated bucket may not inherit the dead incarnation's
        // size, so with no persisted trace left it fails closed again.
        crate::data_usage::remove_bucket_usage_from_backend(ecstore.clone(), &bucket)
            .await
            .expect("bucket usage removal should succeed");
        assert!(matches!(
            checker.get_real_time_usage(&bucket).await,
            Err(QuotaError::UsageUnavailable { .. })
        ));

        crate::data_usage::prepare_bucket_usage_for_namespace_change(&bucket, None)
            .await
            .expect("test usage cache cleanup should succeed");
        crate::data_usage::invalidate_data_usage_snapshot_cache().await;
    }

    #[tokio::test]
    #[serial]
    async fn quota_usage_rejects_an_unknown_mutation_baseline() {
        let (_dirs, ecstore) = isolated_store_over_temp_disks().await;
        let sys = Arc::new(RwLock::new(BucketMetadataSys::new(ecstore)));
        let checker = QuotaChecker::new(sys);
        let bucket = format!("quota-unknown-{}", Uuid::new_v4().simple());

        crate::data_usage::record_bucket_object_write_memory(&bucket, None, 42).await;
        let result = checker.get_real_time_usage(&bucket).await;
        crate::data_usage::prepare_bucket_usage_for_namespace_change(&bucket, None)
            .await
            .expect("test usage cache cleanup should succeed");

        assert!(
            matches!(result, Err(QuotaError::UsageUnavailable { bucket: failed_bucket }) if failed_bucket == bucket),
            "quota decisions must fail closed without an authoritative usage baseline"
        );
    }

    #[tokio::test]
    async fn test_quota_check_no_limit() {
        let result = QuotaCheckResult {
            allowed: true,
            current_usage: None,
            quota_limit: None,
            operation_size: 1024,
            remaining: None,
            uses_durable_reservations: false,
        };

        assert!(result.allowed);
        assert_eq!(result.quota_limit, None);
    }

    #[tokio::test]
    async fn test_quota_check_within_limit() {
        let quota = BucketQuota::new(Some(2048)); // 2KB

        // Current usage 512, trying to add 1024
        let allowed = quota.check_operation_allowed(512, 1024);
        assert!(allowed);
    }

    #[tokio::test]
    async fn test_quota_check_exceeds_limit() {
        let quota = BucketQuota::new(Some(1024)); // 1KB

        // Current usage 512, trying to add 1024
        let allowed = quota.check_operation_allowed(512, 1024);
        assert!(!allowed);
    }

    #[test]
    fn legacy_quota_rejects_full_operation_while_v1_defers_net_growth() {
        let legacy: BucketQuota = serde_json::from_str(r#"{"quota":5}"#).expect("legacy quota should parse");
        let durable = BucketQuota::new(Some(5));

        assert!(!legacy.check_operation_allowed(4, 2));
        assert!(durable.uses_durable_reservations());
    }
}
