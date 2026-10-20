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

use super::{
    BucketQuota, QUOTA_RESERVATION_PROTOCOL_V2, QuotaCheckResult, QuotaError, QuotaMigrationSkipReason, QuotaOperation,
    QuotaProtocolMigration,
};
use crate::bucket::metadata_sys::{
    BucketMetadataSys, update, update_if_incarnation, update_quota_with_sys_if_incarnation_and_revision,
};
use crate::data_usage::get_bucket_usage_memory;
use crate::error::StorageError;
use rustfs_config::QUOTA_CONFIG_FILE;
use rustfs_scanner_metrics::metrics::Metric;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::{Duration, Instant};
use time::OffsetDateTime;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

const LOG_COMPONENT_ECSTORE: &str = "ecstore";
const LOG_SUBSYSTEM_QUOTA: &str = "quota";
const EVENT_QUOTA_USAGE_REPORT: &str = "quota_usage_report";
const EVENT_QUOTA_PROTOCOL_MIGRATION: &str = "quota_reservation_protocol_migration";
const METRIC_QUOTA_USAGE_REPORT_UNAVAILABLE_TOTAL: &str = "rustfs_quota_usage_report_unavailable_total";
const METRIC_QUOTA_PROTOCOL_MIGRATIONS_TOTAL: &str = "rustfs_quota_reservation_protocol_migrations_total";
/// A lazy migration attempt that did not settle the bucket is retried no
/// sooner than this; the admin command is not rate limited.
const LAZY_MIGRATION_RETRY_INTERVAL: Duration = Duration::from_secs(300);

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

        // A durable quota is admitted by the reservation ledger at commit, so
        // scanner usage is reporting input here: an unavailable lookup is
        // reported as absent instead of failing the write (rustfs/backlog#2904).
        // The snapshot protocol still admits from this value and fails closed.
        let current_usage = if uses_durable_reservations {
            self.report_only_usage(bucket).await
        } else {
            Some(self.get_real_time_usage(bucket).await?)
        };

        // The reporting path projects this operation; storage mutations reserve it at commit.
        let admission_size = if uses_durable_reservations && !force_usage_calculation {
            0
        } else {
            operation_size
        };
        let expected_usage = current_usage.map(|current_usage| match operation {
            QuotaOperation::PutObject | QuotaOperation::PostObject | QuotaOperation::CopyObject => {
                current_usage.saturating_add(admission_size)
            }
            QuotaOperation::DeleteObject => current_usage.saturating_sub(operation_size),
        });

        let allowed = match operation {
            QuotaOperation::PutObject | QuotaOperation::PostObject | QuotaOperation::CopyObject => {
                current_usage.is_none_or(|current_usage| quota_config.check_operation_allowed(current_usage, admission_size))
            }
            QuotaOperation::DeleteObject => true,
        };

        let remaining = expected_usage.map(|expected_usage| quota_limit.saturating_sub(expected_usage));

        if let (false, Some(current_usage)) = (allowed, current_usage) {
            warn!(
                "Quota exceeded for bucket: {}, current: {}, limit: {}, attempted: {}",
                bucket, current_usage, quota_limit, operation_size
            );
        }

        let result = QuotaCheckResult {
            allowed,
            current_usage,
            quota_limit: Some(quota_limit),
            operation_size,
            remaining,
            uses_durable_reservations,
        };

        let duration = start_time.elapsed();
        // inc_time is now a plain fn (not async) — no .await needed.
        rustfs_scanner_metrics::metrics::Metrics::inc_time(Metric::QuotaCheck, duration);
        if !allowed {
            rustfs_scanner_metrics::metrics::Metrics::inc_time(Metric::QuotaViolation, duration);
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

        if quota.needs_reservation_protocol_migration() {
            self.schedule_reservation_protocol_migration(bucket).await;
        }

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

        rustfs_scanner_metrics::metrics::Metrics::inc_time(Metric::QuotaSync, start_time.elapsed());
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

        rustfs_scanner_metrics::metrics::Metrics::inc_time(Metric::QuotaSync, start_time.elapsed());
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
        let current_usage = if quota.uses_durable_reservations() {
            self.report_only_usage(bucket).await
        } else {
            Some(self.get_real_time_usage(bucket).await?)
        };

        Ok((quota, current_usage))
    }

    /// Scanner usage for a bucket the reservation ledger admits is reporting
    /// input only, so a failed lookup degrades to "unknown" and is counted
    /// instead of becoming an error the write path would fail on.
    async fn report_only_usage(&self, bucket: &str) -> Option<u64> {
        match self.get_real_time_usage(bucket).await {
            Ok(usage) => Some(usage),
            Err(err) => {
                metrics::counter!(METRIC_QUOTA_USAGE_REPORT_UNAVAILABLE_TOTAL).increment(1);
                debug!(
                    event = EVENT_QUOTA_USAGE_REPORT,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_QUOTA,
                    state = "usage_unknown",
                    bucket = %bucket,
                    error = %err,
                    "durable quota usage report degraded"
                );
                None
            }
        }
    }

    /// Re-save `bucket`'s quota under the sharded durable reservation
    /// protocol (rustfs/backlog#2904). The limit and type are unchanged. The
    /// write takes the incarnation- and revision-checked quota save path with
    /// the fleet capability proof, so a mixed-version fleet fails the attempt
    /// and a concurrent quota edit or bucket replacement leaves the newer
    /// configuration untouched.
    pub async fn migrate_reservation_protocol(&self, bucket: &str) -> Result<QuotaProtocolMigration, QuotaError> {
        let (meta, _) = self
            .metadata_sys
            .read()
            .await
            .get_config(bucket)
            .await
            .map_err(QuotaError::StorageError)?;
        if meta.quota_config_json.is_empty() {
            return Ok(QuotaProtocolMigration::NoQuota);
        }
        let quota: BucketQuota = serde_json::from_slice(&meta.quota_config_json).map_err(|e| QuotaError::InvalidConfig {
            reason: format!("Failed to parse quota config: {}", e),
        })?;
        if let Some(reservation_protocol) = quota.reservation_protocol {
            return Ok(if quota.uses_durable_reservations() {
                QuotaProtocolMigration::AlreadyDurable { reservation_protocol }
            } else {
                QuotaProtocolMigration::Skipped {
                    reason: QuotaMigrationSkipReason::UnsupportedReservationProtocol,
                }
            });
        }
        let Some(migrated) = quota.migrated_to_sharded_reservations() else {
            return Ok(QuotaProtocolMigration::NoQuota);
        };
        if !meta.bucket_incarnation_sidecar || meta.bucket_incarnation_id.is_nil() {
            return Err(QuotaError::StorageError(StorageError::other(
                "bucket incarnation metadata is not authoritative",
            )));
        }
        // Same gate as the admin save: a durable protocol must not reach a
        // fleet that still has nodes that would fail closed on it.
        let proof = crate::services::notification_sys::acquire_cross_pool_fence_fleet_proof().ok_or_else(|| {
            QuotaError::StorageError(StorageError::NamespaceLockQuorumUnavailable {
                mode: "quota_capability",
                bucket: bucket.to_string(),
                object: QUOTA_CONFIG_FILE.to_string(),
                required: 1,
                achieved: 0,
            })
        })?;
        let json_data = serde_json::to_vec(&migrated).map_err(|e| QuotaError::InvalidConfig {
            reason: format!("Failed to serialize quota config: {}", e),
        })?;
        let start_time = Instant::now();
        let saved = update_quota_with_sys_if_incarnation_and_revision(
            Arc::clone(&self.metadata_sys),
            bucket,
            json_data,
            meta.bucket_incarnation_id,
            meta.quota_config_updated_at,
            &proof,
        )
        .await;
        match saved {
            Ok(updated_at) => {
                rustfs_scanner_metrics::metrics::Metrics::inc_time(Metric::QuotaSync, start_time.elapsed());
                Ok(QuotaProtocolMigration::Migrated {
                    reservation_protocol: QUOTA_RESERVATION_PROTOCOL_V2,
                    updated_at,
                })
            }
            Err(StorageError::PreconditionFailed) => Ok(QuotaProtocolMigration::Skipped {
                reason: QuotaMigrationSkipReason::QuotaRevisionChanged,
            }),
            Err(StorageError::BucketNotFound(_)) => Ok(QuotaProtocolMigration::Skipped {
                reason: QuotaMigrationSkipReason::BucketIncarnationChanged,
            }),
            Err(err) => Err(QuotaError::StorageError(err)),
        }
    }

    /// Lazy half of the migration: the first quota read of a bucket still on
    /// the snapshot protocol re-saves it under the sharded durable protocol
    /// in the background; the read itself is neither blocked nor failed by
    /// it. Only the metadata system registered on the store's instance
    /// context migrates, so the rewrite publishes into the cache every
    /// reader of this store consults; a checker over an ad-hoc system
    /// (tooling, tests) only reads. An attempt that does not settle the
    /// bucket is retried no sooner than `LAZY_MIGRATION_RETRY_INTERVAL`.
    async fn schedule_reservation_protocol_migration(&self, bucket: &str) {
        let registered = self.metadata_sys.read().await.object_store().ctx.bucket_metadata_sys();
        if !registered.is_some_and(|registered| Arc::ptr_eq(&registered, &self.metadata_sys)) {
            return;
        }
        if !lazy_migration_attempt_due(bucket) {
            return;
        }
        let checker = Self::new(Arc::clone(&self.metadata_sys));
        let bucket = bucket.to_string();
        tokio::spawn(async move {
            let outcome = checker.migrate_reservation_protocol(&bucket).await;
            record_reservation_protocol_migration(&bucket, "lazy", &outcome);
            if matches!(
                outcome,
                Ok(QuotaProtocolMigration::Migrated { .. }
                    | QuotaProtocolMigration::AlreadyDurable { .. }
                    | QuotaProtocolMigration::NoQuota)
            ) {
                lazy_migration_attempt_settled(&bucket);
            }
        });
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

/// One entry per bucket whose lazy migration is in flight or did not settle;
/// the entry is dropped once the bucket settles, so the map is bounded by the
/// number of buckets still on the snapshot protocol.
fn lazy_migration_attempts() -> &'static Mutex<HashMap<String, Instant>> {
    static ATTEMPTS: OnceLock<Mutex<HashMap<String, Instant>>> = OnceLock::new();
    ATTEMPTS.get_or_init(Mutex::default)
}

fn lazy_migration_attempt_due(bucket: &str) -> bool {
    let mut attempts = lazy_migration_attempts().lock().unwrap_or_else(PoisonError::into_inner);
    let now = Instant::now();
    if attempts
        .get(bucket)
        .is_some_and(|last| now.duration_since(*last) < LAZY_MIGRATION_RETRY_INTERVAL)
    {
        return false;
    }
    attempts.insert(bucket.to_string(), now);
    true
}

fn lazy_migration_attempt_settled(bucket: &str) {
    lazy_migration_attempts()
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .remove(bucket);
}

#[cfg(test)]
fn lazy_migration_attempt_pending(bucket: &str) -> bool {
    lazy_migration_attempts()
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .contains_key(bucket)
}

/// One log line and one counter per migration attempt; `trigger` names the
/// caller (`lazy` from the quota read path, `admin` from the admin command).
pub fn record_reservation_protocol_migration(
    bucket: &str,
    trigger: &'static str,
    outcome: &Result<QuotaProtocolMigration, QuotaError>,
) {
    let result = match outcome {
        Ok(QuotaProtocolMigration::Migrated { .. }) => "migrated",
        Ok(QuotaProtocolMigration::AlreadyDurable { .. }) | Ok(QuotaProtocolMigration::NoQuota) => "noop",
        Ok(QuotaProtocolMigration::Skipped { .. }) => "skipped",
        Err(_) => "failed",
    };
    metrics::counter!(METRIC_QUOTA_PROTOCOL_MIGRATIONS_TOTAL, "trigger" => trigger, "result" => result).increment(1);
    match outcome {
        Ok(QuotaProtocolMigration::Migrated {
            reservation_protocol, ..
        }) => info!(
            event = EVENT_QUOTA_PROTOCOL_MIGRATION,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_QUOTA,
            result,
            trigger,
            bucket = %bucket,
            reservation_protocol,
            "quota reservation protocol migrated"
        ),
        Ok(QuotaProtocolMigration::Skipped { reason }) => debug!(
            event = EVENT_QUOTA_PROTOCOL_MIGRATION,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_QUOTA,
            result,
            trigger,
            bucket = %bucket,
            reason = reason.as_str(),
            "quota reservation protocol migration skipped"
        ),
        Ok(_) => debug!(
            event = EVENT_QUOTA_PROTOCOL_MIGRATION,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_QUOTA,
            result,
            trigger,
            bucket = %bucket,
            "quota reservation protocol migration not needed"
        ),
        Err(err) => warn!(
            event = EVENT_QUOTA_PROTOCOL_MIGRATION,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_QUOTA,
            result,
            trigger,
            bucket = %bucket,
            error = %err,
            "quota reservation protocol migration failed"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bucket::metadata_sys::test_support::isolated_store_over_temp_disks;
    use crate::bucket::quota::QuotaType;
    use crate::error::StorageError;
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

    async fn isolated_durable_quota_store() -> (Vec<tempfile::TempDir>, Arc<crate::store::ECStore>, Arc<RwLock<BucketMetadataSys>>)
    {
        let (dirs, store) = isolated_store_over_temp_disks().await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(Arc::clone(&store), Vec::new()).await;
        let sys = crate::bucket::metadata_sys::bucket_metadata_sys_of(&store.ctx).expect("scoped metadata system");
        (dirs, store, sys)
    }

    async fn create_bucket_with_quota_json(store: &Arc<crate::store::ECStore>, bucket: &str, quota_json: Vec<u8>) {
        use crate::storage_api_contracts::bucket::{BucketOperations as _, MakeBucketOptions};
        store
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("create quota test bucket");
        store
            .update_bucket_metadata_config(bucket, rustfs_config::QUOTA_CONFIG_FILE, quota_json)
            .await
            .expect("persist quota config");
    }

    /// Regression (rustfs/backlog#2904, decision D3 in rustfs/backlog#2872): a
    /// bucket whose quota is admitted by the durable reservation ledger must
    /// not fail its write because scanner usage is unavailable. The checker
    /// reports `current_usage = None` instead of `UsageUnavailable`, while the
    /// ledger still rejects an over-limit write and a bucket without a
    /// reservation protocol keeps failing closed.
    #[tokio::test]
    #[serial]
    async fn durable_quota_check_reports_unknown_usage_without_failing_the_write() {
        use crate::object_api::{ObjectOptions, PutObjReader};
        use crate::storage_api_contracts::object::ObjectIO as _;

        let (_dirs, store, sys) = isolated_durable_quota_store().await;
        let checker = QuotaChecker::new(Arc::clone(&sys));
        let bucket = format!("quota-durable-{}", Uuid::new_v4().simple());
        create_bucket_with_quota_json(
            &store,
            &bucket,
            serde_json::to_vec(&BucketQuota::new_sharded(4096)).expect("sharded quota should encode"),
        )
        .await;
        crate::data_usage::invalidate_data_usage_snapshot_cache().await;

        let result = checker
            .check_quota(&bucket, QuotaOperation::PutObject, 512)
            .await
            .expect("a ledger-admitted bucket must not fail the check without scanner usage");
        assert!(result.allowed);
        assert!(result.uses_durable_reservations);
        assert_eq!(result.quota_limit, Some(4096));
        assert_eq!(result.current_usage, None, "unknown usage is reported as absent, never as zero");
        assert_eq!(result.remaining, None);

        let (_, usage) = checker
            .get_quota_stats(&bucket)
            .await
            .expect("stats for a ledger-admitted bucket degrade to an absent usage");
        assert_eq!(usage, None);

        // Negative control: the ledger, not the scanner, still refuses an
        // over-limit write with the same error.
        let mut too_large = PutObjReader::from_vec(vec![0x5a; 8192]);
        let err = store
            .put_object(&bucket, "too-large", &mut too_large, &ObjectOptions::default())
            .await
            .expect_err("the ledger must reject a write above the quota limit");
        assert!(matches!(err, StorageError::QuotaExceeded { .. }), "{err:?}");

        let mut fits = PutObjReader::from_vec(vec![0x5a; 512]);
        store
            .put_object(&bucket, "fits", &mut fits, &ObjectOptions::default())
            .await
            .expect("a write within the quota limit commits through the ledger");

        // A bucket without a reservation protocol keeps the fail-closed path.
        let legacy = format!("quota-legacy-{}", Uuid::new_v4().simple());
        create_bucket_with_quota_json(&store, &legacy, br#"{"quota":4096,"quota_type":"Hard"}"#.to_vec()).await;
        assert!(matches!(
            checker.check_quota(&legacy, QuotaOperation::PutObject, 512).await,
            Err(QuotaError::UsageUnavailable { .. })
        ));
        assert!(matches!(checker.get_quota_stats(&legacy).await, Err(QuotaError::UsageUnavailable { .. })));

        crate::data_usage::prepare_bucket_usage_for_namespace_change(&bucket, None)
            .await
            .expect("test usage cache cleanup should succeed");
        crate::data_usage::invalidate_data_usage_snapshot_cache().await;
    }

    fn legacy_quota_json(limit: u64) -> Vec<u8> {
        format!(r#"{{"quota":{limit},"quota_type":"Hard","created_at":"2024-01-01T00:00:00Z"}}"#).into_bytes()
    }

    async fn persisted_quota(store: &Arc<crate::store::ECStore>, bucket: &str) -> BucketQuota {
        let (quota, _, _) = crate::bucket::metadata_sys::get_quota_config_and_incarnation_from_disk_in(&store.ctx, bucket)
            .await
            .expect("quota metadata should load from disk");
        quota.expect("a quota should be persisted")
    }

    /// rustfs/backlog#2904: a quota saved before the reservation protocols is
    /// re-saved under the sharded durable protocol with the same limit, type
    /// and audit timestamp; durable and unknown protocols are left alone.
    #[tokio::test]
    #[serial]
    async fn legacy_quota_migrates_to_sharded_reservation_protocol_once() {
        let (_dirs, store, sys) = isolated_durable_quota_store().await;
        crate::services::notification_sys::install_cross_pool_fence_fleet_proof_for_test();
        let checker = QuotaChecker::new(Arc::clone(&sys));
        let bucket = format!("quota-migrate-{}", Uuid::new_v4().simple());
        create_bucket_with_quota_json(&store, &bucket, legacy_quota_json(4096)).await;

        let outcome = checker
            .migrate_reservation_protocol(&bucket)
            .await
            .expect("migration should succeed");
        assert!(
            matches!(
                outcome,
                QuotaProtocolMigration::Migrated {
                    reservation_protocol: QUOTA_RESERVATION_PROTOCOL_V2,
                    ..
                }
            ),
            "{outcome:?}"
        );
        let migrated = persisted_quota(&store, &bucket).await;
        assert!(migrated.uses_sharded_reservations());
        assert_eq!(migrated.quota, Some(4096));
        assert_eq!(migrated.quota_type, QuotaType::Hard);
        assert_eq!(migrated.created_at.map(|at| at.unix_timestamp()), Some(1_704_067_200));
        assert_eq!(
            checker.get_quota_config(&bucket).await.expect("cached quota should load"),
            migrated,
            "the rewrite publishes into the registered cache"
        );

        assert_eq!(
            checker
                .migrate_reservation_protocol(&bucket)
                .await
                .expect("second attempt should succeed"),
            QuotaProtocolMigration::AlreadyDurable {
                reservation_protocol: QUOTA_RESERVATION_PROTOCOL_V2
            }
        );

        let no_quota = format!("quota-none-{}", Uuid::new_v4().simple());
        create_bucket_with_quota_json(
            &store,
            &no_quota,
            serde_json::to_vec(&BucketQuota::new(None)).expect("empty quota should encode"),
        )
        .await;
        assert_eq!(
            checker
                .migrate_reservation_protocol(&no_quota)
                .await
                .expect("no quota is a no-op"),
            QuotaProtocolMigration::NoQuota
        );

        let unsupported = format!("quota-unsupported-{}", Uuid::new_v4().simple());
        let unsupported_json = br#"{"quota":0,"quota_type":"Hard","reservation_protocol":3,"reservation_quota":2048}"#.to_vec();
        create_bucket_with_quota_json(&store, &unsupported, unsupported_json.clone()).await;
        assert_eq!(
            checker
                .migrate_reservation_protocol(&unsupported)
                .await
                .expect("an unknown protocol is skipped, never rewritten"),
            QuotaProtocolMigration::Skipped {
                reason: QuotaMigrationSkipReason::UnsupportedReservationProtocol
            }
        );
        let (raw, _) = sys.read().await.get_config(&unsupported).await.expect("cached metadata");
        assert_eq!(raw.quota_config_json, unsupported_json);
    }

    /// The guarded save refuses a replaced bucket and a quota edited since the
    /// cached read; the migration reports the latter as a skip and leaves the
    /// newer configuration for the next attempt.
    #[tokio::test]
    #[serial]
    async fn quota_protocol_migration_requires_matching_incarnation_and_revision() {
        let (_dirs, store, sys) = isolated_durable_quota_store().await;
        crate::services::notification_sys::install_cross_pool_fence_fleet_proof_for_test();
        let checker = QuotaChecker::new(Arc::clone(&sys));
        let bucket = format!("quota-cas-{}", Uuid::new_v4().simple());
        create_bucket_with_quota_json(&store, &bucket, legacy_quota_json(4096)).await;
        let (meta, _) = sys.read().await.get_config(&bucket).await.expect("cached metadata");
        let proof = crate::services::notification_sys::acquire_cross_pool_fence_fleet_proof().expect("fleet proof is installed");
        let data = serde_json::to_vec(&BucketQuota::new_sharded(4096)).expect("sharded quota should encode");

        let err = update_quota_with_sys_if_incarnation_and_revision(
            Arc::clone(&sys),
            &bucket,
            data.clone(),
            Uuid::new_v4(),
            meta.quota_config_updated_at,
            &proof,
        )
        .await
        .expect_err("a different incarnation must not be rewritten");
        assert!(matches!(err, StorageError::BucketNotFound(_)), "{err:?}");

        let err = update_quota_with_sys_if_incarnation_and_revision(
            Arc::clone(&sys),
            &bucket,
            data,
            meta.bucket_incarnation_id,
            meta.quota_config_updated_at + time::Duration::seconds(1),
            &proof,
        )
        .await
        .expect_err("a different quota revision must not be rewritten");
        assert!(matches!(err, StorageError::PreconditionFailed), "{err:?}");
        assert_eq!(persisted_quota(&store, &bucket).await.quota, Some(4096));
        assert!(persisted_quota(&store, &bucket).await.needs_reservation_protocol_migration());

        // Model a peer whose cache missed a newer save: the migration reads
        // the stale entry, the guarded write sees the newer revision, and the
        // newer configuration stays in place.
        let (stale, _) = sys.read().await.get_config(&bucket).await.expect("cached metadata");
        store
            .update_bucket_metadata_config(&bucket, QUOTA_CONFIG_FILE, legacy_quota_json(8192))
            .await
            .expect("newer quota save");
        sys.read().await.set(bucket.clone(), stale).await;
        assert_eq!(
            checker
                .migrate_reservation_protocol(&bucket)
                .await
                .expect("a stale read is a skip, not a failure"),
            QuotaProtocolMigration::Skipped {
                reason: QuotaMigrationSkipReason::QuotaRevisionChanged
            }
        );
        let on_disk = persisted_quota(&store, &bucket).await;
        assert_eq!(on_disk.quota, Some(8192));
        assert!(
            on_disk.needs_reservation_protocol_migration(),
            "the newer save is left for the next attempt"
        );
    }

    /// The first quota read through the instance's registered metadata
    /// system migrates the bucket in the background and still answers with
    /// the configuration it found; a checker over an ad-hoc system only reads.
    #[tokio::test]
    #[serial]
    async fn lazy_quota_protocol_migration_runs_from_the_quota_read_path() {
        let (_dirs, store, sys) = isolated_durable_quota_store().await;
        crate::services::notification_sys::install_cross_pool_fence_fleet_proof_for_test();
        let checker = QuotaChecker::new(Arc::clone(&sys));
        let bucket = format!("quota-lazy-{}", Uuid::new_v4().simple());
        create_bucket_with_quota_json(&store, &bucket, legacy_quota_json(4096)).await;

        let first = checker.get_quota_config(&bucket).await.expect("legacy quota should load");
        assert!(
            first.needs_reservation_protocol_migration(),
            "the triggering read answers with what it found"
        );
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let on_disk = persisted_quota(&store, &bucket).await;
            if on_disk.uses_sharded_reservations() {
                assert_eq!(on_disk.quota, Some(4096));
                break;
            }
            assert!(Instant::now() < deadline, "lazy migration did not land");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let settled = Instant::now() + Duration::from_secs(10);
        while lazy_migration_attempt_pending(&bucket) {
            assert!(Instant::now() < settled, "a migrated bucket must leave the retry map");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        let ad_hoc = QuotaChecker::new(Arc::new(RwLock::new(BucketMetadataSys::new(Arc::clone(&store)))));
        let other = format!("quota-adhoc-{}", Uuid::new_v4().simple());
        create_bucket_with_quota_json(&store, &other, legacy_quota_json(4096)).await;
        assert!(
            ad_hoc
                .get_quota_config(&other)
                .await
                .expect("legacy quota should load")
                .needs_reservation_protocol_migration()
        );
        assert!(
            !lazy_migration_attempt_pending(&other),
            "an ad-hoc metadata system never schedules a migration"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(persisted_quota(&store, &other).await.needs_reservation_protocol_migration());
    }

    /// Migration while writes are in flight: objects committed under the
    /// snapshot protocol before and around the rewrite are accounted exactly
    /// once by the sharded ledger's exact bootstrap, so the limit holds to
    /// the byte afterwards. (Under `cfg(test)` a handler-supplied snapshot
    /// admission short-circuits `begin`, so every concurrent write here takes
    /// the snapshot path; the bootstrap accounting is what is verified.)
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    async fn concurrent_puts_across_protocol_migration_are_accounted_exactly_once() {
        use crate::object_api::{ObjectOptions, PutObjReader};
        use crate::storage_api_contracts::object::ObjectIO as _;

        const OBJECTS: u64 = 8;
        const SIZE: u64 = 256;
        const SLACK: u64 = 64;
        const LIMIT: u64 = OBJECTS * SIZE + SLACK;

        let (_dirs, store, sys) = isolated_durable_quota_store().await;
        crate::services::notification_sys::install_cross_pool_fence_fleet_proof_for_test();
        let bucket = format!("quota-race-{}", Uuid::new_v4().simple());
        create_bucket_with_quota_json(&store, &bucket, legacy_quota_json(LIMIT)).await;

        let mut writers = Vec::with_capacity(OBJECTS as usize);
        for index in 0..OBJECTS {
            let store = Arc::clone(&store);
            let bucket = bucket.clone();
            writers.push(tokio::spawn(async move {
                // The admission a request handler supplies on the snapshot
                // protocol; ignored once the ledger owns admission.
                let mut opts = ObjectOptions::default();
                assert!(opts.set_quota_admission(0, LIMIT));
                let mut body = PutObjReader::from_vec(vec![0x5a; SIZE as usize]);
                store.put_object(&bucket, &format!("object-{index}"), &mut body, &opts).await
            }));
        }
        let migration = {
            let checker = QuotaChecker::new(Arc::clone(&sys));
            let bucket = bucket.clone();
            tokio::spawn(async move { checker.migrate_reservation_protocol(&bucket).await })
        };
        for writer in writers {
            writer
                .await
                .expect("writer task should not panic")
                .expect("every write fits the limit");
        }
        let outcome = migration
            .await
            .expect("migration task should not panic")
            .expect("migration should succeed");
        assert!(matches!(outcome, QuotaProtocolMigration::Migrated { .. }), "{outcome:?}");

        let mut over = PutObjReader::from_vec(vec![0x5a; (SLACK + 1) as usize]);
        let err = store
            .put_object(&bucket, "over", &mut over, &ObjectOptions::default())
            .await
            .expect_err("one byte above the remaining quota must be rejected");
        assert!(matches!(err, StorageError::QuotaExceeded { .. }), "{err:?}");
        let mut exact = PutObjReader::from_vec(vec![0x5a; SLACK as usize]);
        store
            .put_object(&bucket, "exact", &mut exact, &ObjectOptions::default())
            .await
            .expect("exactly the remaining bytes fit");
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
