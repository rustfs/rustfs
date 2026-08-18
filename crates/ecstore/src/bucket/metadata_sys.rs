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

use super::metadata::{
    BUCKET_TARGETS_FILE, BucketMetadata, load_bucket_incarnation, load_bucket_metadata, save_bucket_incarnation,
};
use super::quota::BucketQuota;
use super::target::BucketTargets;
use crate::bucket::bucket_target_sys::BucketTargetSys;
use crate::bucket::metadata::{load_bucket_metadata_parse, load_bucket_metadata_parse_with_presence};
use crate::bucket::utils::is_meta_bucketname;
use crate::disk::RUSTFS_META_BUCKET;
use crate::error::{Error, Result, is_err_bucket_not_found};
use crate::runtime::sources as runtime_sources;
use crate::storage_api_contracts::heal::HealOperations as _;
use crate::storage_api_contracts::namespace::NamespaceLocking as _;
use crate::store::{ECStore, await_bucket_namespace_operation};
use futures::future::join_all;
use rustfs_common::heal_channel::HealOpts;
use rustfs_policy::policy::BucketPolicy;
use s3s::dto::ReplicationConfiguration;
use s3s::dto::{
    AccelerateConfiguration, BucketLifecycleConfiguration, BucketLoggingStatus, CORSConfiguration, NotificationConfiguration,
    ObjectLockConfiguration, ObjectLockEnabled, ObjectLockRetentionMode, PublicAccessBlockConfiguration,
    RequestPaymentConfiguration, ServerSideEncryptionConfiguration, Tagging, VersioningConfiguration, WebsiteConfiguration,
};
use std::collections::HashSet;
use std::time::Duration;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex as StdMutex, Weak},
};
use time::OffsetDateTime;
use tokio::sync::{Mutex, RwLock};
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{error, warn};
use uuid::Uuid;

const BUCKET_METADATA_REFRESH_INTERVAL: Duration = Duration::from_secs(15 * 60);

#[cfg(any(test, feature = "test-util"))]
struct ConfigWriteLockProbeState {
    bucket: String,
    arrived: tokio::sync::Notify,
}

#[cfg(any(test, feature = "test-util"))]
static CONFIG_WRITE_LOCK_PROBES: std::sync::OnceLock<StdMutex<Vec<Arc<ConfigWriteLockProbeState>>>> = std::sync::OnceLock::new();

#[cfg(any(test, feature = "test-util"))]
#[allow(dead_code, reason = "installed by tests behind `--features test-util` (backlog#1823)")]
pub struct ConfigWriteLockProbe {
    state: Arc<ConfigWriteLockProbeState>,
}

#[cfg(any(test, feature = "test-util"))]
impl ConfigWriteLockProbe {
    #[allow(dead_code, reason = "installed by tests behind `--features test-util` (backlog#1823)")]
    pub fn install(bucket: &str) -> Self {
        let state = Arc::new(ConfigWriteLockProbeState {
            bucket: bucket.to_string(),
            arrived: tokio::sync::Notify::new(),
        });
        let mut probes = CONFIG_WRITE_LOCK_PROBES
            .get_or_init(|| StdMutex::new(Vec::new()))
            .lock()
            .expect("config write lock probe mutex should not poison");
        assert!(
            !probes.iter().any(|current| current.bucket == state.bucket),
            "config write lock probe must be unique for a bucket"
        );
        probes.push(Arc::clone(&state));
        drop(probes);
        Self { state }
    }

    #[allow(dead_code, reason = "installed by tests behind `--features test-util` (backlog#1823)")]
    pub async fn wait_until_attempted(&self) {
        tokio::time::timeout(Duration::from_secs(30), self.state.arrived.notified())
            .await
            .expect("bucket config update should attempt the transaction lock");
    }
}

#[cfg(any(test, feature = "test-util"))]
impl Drop for ConfigWriteLockProbe {
    fn drop(&mut self) {
        let mut probes = CONFIG_WRITE_LOCK_PROBES
            .get_or_init(|| StdMutex::new(Vec::new()))
            .lock()
            .expect("config write lock probe mutex should not poison");
        probes.retain(|state| !Arc::ptr_eq(state, &self.state));
    }
}

#[cfg(any(test, feature = "test-util"))]
fn notify_config_write_lock_attempt(bucket: &str) {
    let probe = CONFIG_WRITE_LOCK_PROBES
        .get_or_init(|| StdMutex::new(Vec::new()))
        .lock()
        .expect("config write lock probe mutex should not poison")
        .iter()
        .find(|probe| probe.bucket == bucket)
        .cloned();
    if let Some(probe) = probe {
        probe.arrived.notify_one();
    }
}

#[derive(Clone, Copy)]
enum MetadataLoadMode {
    Initial,
    Refresh,
}

#[derive(Debug, Clone)]
pub enum ObjectLockConfigState {
    Configured {
        config: ObjectLockConfiguration,
        updated_at: OffsetDateTime,
    },
    ConfirmedAbsent,
    Fabricated,
}

enum BucketMetadataAuthority {
    Authoritative(Arc<BucketMetadata>),
    Fabricated,
    MissingBucket,
}

pub(crate) fn object_lock_config_state_from_authoritative_metadata(bm: &BucketMetadata) -> Result<ObjectLockConfigState> {
    if bm.object_lock_config.is_none() && !bm.object_lock_config_xml.is_empty() {
        return Err(Error::other("persisted bucket Object Lock configuration is invalid"));
    }

    if let Some(config) = bm.object_lock_config.clone() {
        validate_authoritative_object_lock_config(&config)?;
        return Ok(ObjectLockConfigState::Configured {
            config,
            updated_at: bm.object_lock_config_updated_at,
        });
    }

    if bm.lock_enabled {
        return Ok(ObjectLockConfigState::Configured {
            config: ObjectLockConfiguration {
                object_lock_enabled: Some(ObjectLockEnabled::from_static(ObjectLockEnabled::ENABLED)),
                rule: None,
            },
            updated_at: bm.object_lock_config_updated_at,
        });
    }

    Ok(ObjectLockConfigState::ConfirmedAbsent)
}

fn validate_authoritative_object_lock_config(config: &ObjectLockConfiguration) -> Result<()> {
    if config.object_lock_enabled.as_ref().map(ObjectLockEnabled::as_str) != Some(ObjectLockEnabled::ENABLED) {
        return Err(Error::other("persisted bucket Object Lock enabled state is invalid"));
    }

    let Some(rule) = config.rule.as_ref() else {
        return Ok(());
    };
    let Some(retention) = rule.default_retention.as_ref() else {
        return Err(Error::other("persisted bucket Object Lock rule has no default retention"));
    };
    if !retention
        .mode
        .as_ref()
        .is_some_and(|mode| matches!(mode.as_str(), ObjectLockRetentionMode::COMPLIANCE | ObjectLockRetentionMode::GOVERNANCE))
    {
        return Err(Error::other("persisted bucket Object Lock retention mode is invalid"));
    }
    match (retention.days, retention.years) {
        (Some(days), None) if (1..=36_500).contains(&days) => Ok(()),
        (None, Some(years)) if (1..=100).contains(&years) => Ok(()),
        _ => Err(Error::other("persisted bucket Object Lock retention period is invalid")),
    }
}

pub async fn init_bucket_metadata_sys(api: Arc<ECStore>, buckets: Vec<String>) {
    // The metadata system is inherently per-store (it holds the store handle
    // and that store's bucket cache), so it lives on the store's own instance
    // context (backlog#1052 S3) — a second instance initializes its own cell
    // instead of panicking on the process-global one.
    let instance_ctx = api.ctx.clone();
    let is_dist_erasure = instance_ctx.is_dist_erasure().await;

    let mut sys = BucketMetadataSys::new(api);
    sys.init(buckets).await;

    let sys = Arc::new(RwLock::new(sys));

    instance_ctx.init_bucket_metadata_sys(sys.clone());

    if is_dist_erasure {
        start_refresh_buckets_metadata_loop(sys);
    }
}

/// The current instance's bucket metadata system (legacy free-function
/// facade: resolves the published store's context, or the bootstrap one).
pub fn get_global_bucket_metadata_sys() -> Option<Arc<RwLock<BucketMetadataSys>>> {
    crate::runtime::global::current_ctx().bucket_metadata_sys()
}

pub(super) fn get_bucket_metadata_sys() -> Result<Arc<RwLock<BucketMetadataSys>>> {
    if let Some(sys) = get_global_bucket_metadata_sys() {
        Ok(sys)
    } else {
        Err(Error::other("bucket metadata sys not initialized for this instance"))
    }
}

pub async fn set_bucket_metadata(bucket: String, bm: BucketMetadata) -> Result<()> {
    let sys = get_bucket_metadata_sys()?;
    let lock = sys.write().await;
    lock.set(bucket, Arc::new(bm)).await;
    Ok(())
}

pub async fn reload_bucket_metadata(api: Arc<ECStore>, bucket: &str) -> Result<()> {
    if is_meta_bucketname(bucket) {
        return Err(Error::other("errInvalidArgument"));
    }
    let namespace_lock = api.new_ns_lock(bucket, bucket).await?;
    let namespace_guard = namespace_lock
        .get_read_lock(crate::set_disk::get_lock_acquire_timeout())
        .await?;
    let sys = bucket_metadata_sys_of(&api.ctx)?;
    let sys = sys.read().await.clone();
    sys.reload_from_store_under_namespace(bucket, &namespace_guard).await
}

/// Drop a bucket's cached metadata from the in-memory map.
///
/// This is the counterpart to [`set_bucket_metadata`] and is invoked when a
/// bucket is deleted so peers stop serving stale cached configuration for it.
/// Returns `true` if an entry was present.
pub async fn remove_bucket_metadata(bucket: &str) -> Result<bool> {
    let sys = get_bucket_metadata_sys()?;
    let lock = sys.read().await;
    Ok(lock.remove(bucket).await)
}

fn start_refresh_buckets_metadata_loop(sys: Arc<RwLock<BucketMetadataSys>>) {
    let Some(cancel_token) = runtime_sources::background_services_cancel_token() else {
        warn!("bucket metadata refresh loop skipped because background cancellation token is not initialized");
        return;
    };

    tokio::spawn(async move {
        refresh_buckets_metadata_loop(sys, cancel_token).await;
    });
}

async fn refresh_buckets_metadata_loop(sys: Arc<RwLock<BucketMetadataSys>>, cancel_token: CancellationToken) {
    loop {
        if !wait_refresh_interval_or_cancel(&cancel_token, BUCKET_METADATA_REFRESH_INTERVAL).await {
            break;
        }
        refresh_buckets_metadata_once(sys.clone()).await;
    }
}

async fn wait_refresh_interval_or_cancel(cancel_token: &CancellationToken, interval: Duration) -> bool {
    tokio::select! {
        _ = cancel_token.cancelled() => false,
        _ = sleep(interval) => true,
    }
}

async fn refresh_buckets_metadata_once(sys: Arc<RwLock<BucketMetadataSys>>) {
    let buckets = {
        let sys = sys.read().await;
        sys.bucket_names().await
    };
    if buckets.is_empty() {
        return;
    }

    let count = runtime_sources::endpoint_erasure_set_count()
        .map(|count| count * 10)
        .unwrap_or(10)
        .max(1);
    let mut failed_buckets = HashSet::new();

    for chunk in buckets.chunks(count) {
        BucketMetadataSys::concurrent_refresh_load(Arc::clone(&sys), chunk, &mut failed_buckets).await;
    }

    if !failed_buckets.is_empty() {
        warn!(
            failed_bucket_count = failed_buckets.len(),
            "bucket metadata refresh loop left buckets queued for retry"
        );
    }
}

async fn sync_bucket_target_sys(bucket: &str, bm: &BucketMetadata) {
    BucketTargetSys::get()
        .update_all_targets(bucket, bm.bucket_target_config.as_ref())
        .await;
}

/// Publish the bucket's durability override (or its absence) to the disk
/// layer registry consulted by `effective_durability`.
///
/// Called from every path that installs a bucket's metadata into the cache
/// (initial load, config update, peer reload notification, refresh loop,
/// lazy load), so the override propagates with exactly the bucket-metadata
/// cache invalidation semantics and never through a channel of its own.
fn sync_bucket_durability(bucket: &str, bm: &BucketMetadata) {
    let mode = bm
        .durability_config()
        .and_then(|cfg| cfg.normalized_mode())
        .and_then(|mode| crate::disk::local::DurabilityMode::parse(&mode));
    crate::disk::local::bucket_durability::set(bucket, mode);
}

/// Drop a bucket's durability override when its metadata leaves the cache.
fn clear_bucket_durability(bucket: &str) {
    crate::disk::local::bucket_durability::set(bucket, None);
}

pub async fn get(bucket: &str) -> Result<Arc<BucketMetadata>> {
    let sys = get_bucket_metadata_sys()?;
    let lock = sys.read().await;
    lock.get(bucket).await
}

// ---- Instance-scoped variants (backlog#1052 S7) ----
//
// A store's own bucket operations resolve the metadata system of *their*
// instance context so two servers in one process stay isolated; when the
// instance cell is not initialized yet (early startup) they fall back to the
// ambient default — the single-instance legacy behavior.

pub(crate) fn bucket_metadata_sys_of(ctx: &crate::runtime::instance::InstanceContext) -> Result<Arc<RwLock<BucketMetadataSys>>> {
    if let Some(sys) = ctx.bucket_metadata_sys() {
        return Ok(sys);
    }
    get_bucket_metadata_sys()
}

pub(crate) fn require_bucket_metadata_sys_in(
    ctx: &crate::runtime::instance::InstanceContext,
) -> Result<Arc<RwLock<BucketMetadataSys>>> {
    ctx.bucket_metadata_sys()
        .ok_or_else(|| Error::other("bucket metadata sys not initialized for this instance"))
}

pub(crate) async fn object_store_in(ctx: &crate::runtime::instance::InstanceContext) -> Result<Arc<ECStore>> {
    let sys = bucket_metadata_sys_of(ctx)?;
    Ok(sys.read().await.api.clone())
}

pub(crate) async fn get_in(ctx: &crate::runtime::instance::InstanceContext, bucket: &str) -> Result<Arc<BucketMetadata>> {
    let sys = bucket_metadata_sys_of(ctx)?;
    let lock = sys.read().await;
    lock.get(bucket).await
}

pub(crate) async fn created_at_in(ctx: &crate::runtime::instance::InstanceContext, bucket: &str) -> Result<OffsetDateTime> {
    let sys = bucket_metadata_sys_of(ctx)?;
    let lock = sys.read().await;
    lock.created_at(bucket).await
}

pub(crate) async fn get_config_from_disk_with_presence_in(
    ctx: &crate::runtime::instance::InstanceContext,
    bucket: &str,
) -> Result<(BucketMetadata, bool)> {
    let sys = bucket_metadata_sys_of(ctx)?;
    let api = sys.read().await.api.clone();
    load_bucket_metadata_parse_with_presence(api, bucket, true).await
}

pub(crate) async fn get_bucket_incarnation_id_in(ctx: &crate::runtime::instance::InstanceContext, bucket: &str) -> Result<Uuid> {
    let sys = bucket_metadata_sys_of(ctx)?;
    let sys = sys.read().await.clone();
    sys.get_bucket_incarnation_id_from_disk(bucket).await
}

pub(crate) async fn get_cached_bucket_incarnation_id_in(
    ctx: &crate::runtime::instance::InstanceContext,
    bucket: &str,
) -> Result<Uuid> {
    let sys = bucket_metadata_sys_of(ctx)?;
    let sys = sys.read().await.clone();
    sys.get_bucket_incarnation_id(bucket).await
}

pub(crate) async fn set_bucket_metadata_in(ctx: &crate::runtime::instance::InstanceContext, bm: BucketMetadata) -> Result<()> {
    let sys = bucket_metadata_sys_of(ctx)?;
    let lock = sys.read().await;
    lock.persist_and_set(bm).await
}

pub(crate) async fn set_new_bucket_metadata_in(
    ctx: &crate::runtime::instance::InstanceContext,
    bm: BucketMetadata,
) -> Result<()> {
    let sys = bucket_metadata_sys_of(ctx)?;
    let lock = sys.read().await;
    lock.persist_new_and_set(bm).await
}

pub(crate) async fn cache_bucket_metadata_in(ctx: &crate::runtime::instance::InstanceContext, bm: BucketMetadata) -> Result<()> {
    let sys = bucket_metadata_sys_of(ctx)?;
    let lock = sys.read().await;
    lock.set(bm.name.clone(), Arc::new(bm)).await;
    Ok(())
}

pub(crate) async fn remove_bucket_metadata_in(ctx: &crate::runtime::instance::InstanceContext, bucket: &str) -> Result<bool> {
    let sys = bucket_metadata_sys_of(ctx)?;
    let lock = sys.read().await;
    Ok(lock.remove(bucket).await)
}

#[cfg(test)]
pub(crate) async fn inject_object_lock_disk_read_error_in(
    ctx: &crate::runtime::instance::InstanceContext,
    bucket: &str,
) -> Result<()> {
    let sys = bucket_metadata_sys_of(ctx)?;
    let sys = sys.read().await.clone();
    sys.object_lock_disk_read_errors.write().await.insert(bucket.to_string());
    Ok(())
}

/// Rewrite one config file of a bucket's metadata, serialized cluster-wide.
///
/// See [`acquire_bucket_metadata_transaction_lock`] for why every config
/// write — not just the replication-targets one — has to hold that lock.
pub async fn update(bucket: &str, config_file: &str, data: Vec<u8>) -> Result<OffsetDateTime> {
    Box::pin(update_with_sys(get_bucket_metadata_sys()?, bucket, config_file, data)).await
}

pub(crate) async fn update_in(
    ctx: &crate::runtime::instance::InstanceContext,
    bucket: &str,
    config_file: &str,
    data: Vec<u8>,
) -> Result<OffsetDateTime> {
    Box::pin(update_with_sys(require_bucket_metadata_sys_in(ctx)?, bucket, config_file, data)).await
}

pub async fn delete(bucket: &str, config_file: &str) -> Result<OffsetDateTime> {
    delete_with_sys(get_bucket_metadata_sys()?, bucket, config_file).await
}

pub async fn update_if_incarnation(
    bucket: &str,
    config_file: &str,
    data: Vec<u8>,
    expected_incarnation_id: Uuid,
) -> Result<OffsetDateTime> {
    // Boxed for the same reason as [`update`]: this is the path an authorized
    // bucket-config mutation actually takes once it carries an incarnation, so
    // leaving it inlined puts the whole resolve/load/save chain back on the
    // caller's stack (rustfs#5648).
    Box::pin(update_with_sys_expected(
        get_bucket_metadata_sys()?,
        bucket,
        config_file,
        data,
        Some(expected_incarnation_id),
    ))
    .await
}

pub async fn delete_if_incarnation(bucket: &str, config_file: &str, expected_incarnation_id: Uuid) -> Result<OffsetDateTime> {
    Box::pin(delete_with_sys_expected(
        get_bucket_metadata_sys()?,
        bucket,
        config_file,
        Some(expected_incarnation_id),
    ))
    .await
}

pub async fn capture_bucket_metadata_incarnation(bucket: &str) -> Result<Uuid> {
    let guard = acquire_config_write_guard(get_bucket_metadata_sys()?, bucket).await?;
    Ok(guard.incarnation_id)
}

/// [`update`] against an explicitly supplied metadata system.
///
/// The free functions resolve the instance's own system; this variant takes
/// it as an argument so a test can drive two independent systems over one
/// backing store — the in-process stand-in for two nodes, which is the only
/// configuration where the transaction lock is what does the serializing.
async fn update_with_sys(
    sys: Arc<RwLock<BucketMetadataSys>>,
    bucket: &str,
    config_file: &str,
    data: Vec<u8>,
) -> Result<OffsetDateTime> {
    update_with_sys_expected(sys, bucket, config_file, data, None).await
}

async fn update_with_sys_expected(
    sys: Arc<RwLock<BucketMetadataSys>>,
    bucket: &str,
    config_file: &str,
    data: Vec<u8>,
    expected_incarnation_id: Option<Uuid>,
) -> Result<OffsetDateTime> {
    let guard = acquire_config_write_guard_for_incarnation(sys.clone(), bucket, expected_incarnation_id).await?;
    update_under_config_write_guard(sys, &guard, config_file, data).await
}

/// [`delete`] against an explicitly supplied metadata system. See
/// [`update_with_sys`].
async fn delete_with_sys(sys: Arc<RwLock<BucketMetadataSys>>, bucket: &str, config_file: &str) -> Result<OffsetDateTime> {
    delete_with_sys_expected(sys, bucket, config_file, None).await
}

async fn delete_with_sys_expected(
    sys: Arc<RwLock<BucketMetadataSys>>,
    bucket: &str,
    config_file: &str,
    expected_incarnation_id: Option<Uuid>,
) -> Result<OffsetDateTime> {
    let guard = acquire_config_write_guard_for_incarnation(sys.clone(), bucket, expected_incarnation_id).await?;
    delete_under_config_write_guard(sys, &guard, config_file).await
}

/// Owns the complete bucket-config mutation fence.
///
/// Lock order: bucket lifecycle sentinel (read), then metadata transaction
/// (write). The immutable incarnation is checked before any persisted
/// metadata can be rewritten.
pub struct BucketMetadataMutationGuard {
    bucket: String,
    incarnation_id: Uuid,
    lifecycle_guard: rustfs_lock::NamespaceLockGuard,
    transaction_guard: rustfs_lock::NamespaceLockGuard,
}

impl BucketMetadataMutationGuard {
    fn ensure_valid(&self, bucket: &str) -> Result<()> {
        if self.bucket != bucket {
            return Err(Error::other("bucket metadata mutation guard does not match bucket"));
        }
        if self.lifecycle_guard.is_lock_lost() || self.transaction_guard.is_lock_lost() {
            return Err(Error::other(format!("bucket metadata mutation lock was lost: {bucket}")));
        }
        Ok(())
    }
}

async fn acquire_config_write_guard(sys: Arc<RwLock<BucketMetadataSys>>, bucket: &str) -> Result<BucketMetadataMutationGuard> {
    acquire_config_write_guard_for_incarnation(sys, bucket, None).await
}

async fn acquire_config_write_guard_for_incarnation(
    sys: Arc<RwLock<BucketMetadataSys>>,
    bucket: &str,
    expected_incarnation_id: Option<Uuid>,
) -> Result<BucketMetadataMutationGuard> {
    let metadata_sys = sys.read().await.clone();
    let lifecycle_guard = metadata_sys.api.acquire_bucket_lifecycle_read_lock(bucket).await?;

    // Legacy buckets are migrated while the lifecycle fence prevents a
    // same-name replacement. The second read under the write transaction is
    // the CAS source of truth for the actual rewrite.
    await_bucket_namespace_operation(
        Some(&lifecycle_guard),
        bucket,
        "bucket config incarnation migration",
        metadata_sys.get_bucket_incarnation_id(bucket),
    )
    .await?;
    let transaction_guard = await_bucket_namespace_operation(
        Some(&lifecycle_guard),
        bucket,
        "bucket config transaction lock acquisition",
        acquire_transaction_lock_with_sys(&sys, bucket),
    )
    .await?;
    await_bucket_namespace_operation(
        Some(&lifecycle_guard),
        bucket,
        "bucket config existence validation",
        await_bucket_namespace_operation(
            Some(&transaction_guard),
            bucket,
            "bucket config existence transaction validation",
            async {
                match metadata_sys
                    .api
                    .peer_sys
                    .get_bucket_info(bucket, &crate::storage_api_contracts::bucket::BucketOptions::default())
                    .await
                {
                    Ok(_) => Ok(()),
                    Err(crate::disk::error::Error::VolumeNotFound) => Err(Error::BucketNotFound(bucket.to_string())),
                    Err(err) => Err(err.into()),
                }
            },
        ),
    )
    .await?;
    let current_incarnation_id = await_bucket_namespace_operation(
        Some(&lifecycle_guard),
        bucket,
        "bucket config incarnation validation",
        await_bucket_namespace_operation(
            Some(&transaction_guard),
            bucket,
            "bucket config incarnation transaction validation",
            load_bucket_incarnation(metadata_sys.api.clone(), bucket),
        ),
    )
    .await?
    .filter(|incarnation_id| !incarnation_id.is_nil())
    .ok_or_else(|| Error::other(format!("bucket incarnation metadata is not authoritative: {bucket}")))?;
    if expected_incarnation_id.is_some_and(|expected| expected != current_incarnation_id) {
        return Err(Error::BucketNotFound(bucket.to_string()));
    }

    Ok(BucketMetadataMutationGuard {
        bucket: bucket.to_string(),
        incarnation_id: current_incarnation_id,
        lifecycle_guard,
        transaction_guard,
    })
}

/// Rewrite one config file while the caller already holds this bucket's
/// transaction lock.
///
/// [`update`] would deadlock here: the lock is not reentrant, so a holder
/// that called it would block until its own guard timed out.
pub async fn update_under_transaction_lock(
    guard: &BucketMetadataMutationGuard,
    bucket: &str,
    config_file: &str,
    data: Vec<u8>,
) -> Result<OffsetDateTime> {
    guard.ensure_valid(bucket)?;
    update_under_config_write_guard(get_bucket_metadata_sys()?, guard, config_file, data).await
}

/// Clear one config file while the caller holds this bucket's transaction lock.
pub async fn delete_under_transaction_lock(
    guard: &BucketMetadataMutationGuard,
    bucket: &str,
    config_file: &str,
) -> Result<OffsetDateTime> {
    guard.ensure_valid(bucket)?;
    delete_under_config_write_guard(get_bucket_metadata_sys()?, guard, config_file).await
}

pub async fn update_quota_if_incarnation(
    bucket: &str,
    data: Vec<u8>,
    expected_incarnation_id: Uuid,
    proof: &crate::services::notification_sys::CrossPoolFenceFleetProofToken,
) -> Result<OffsetDateTime> {
    let sys = get_bucket_metadata_sys()?;
    let guard = Box::pin(acquire_config_write_guard_for_incarnation(
        sys.clone(),
        bucket,
        Some(expected_incarnation_id),
    ))
    .await?;
    if !crate::services::notification_sys::cross_pool_fence_fleet_proof_matches(proof) {
        return Err(Error::NamespaceLockQuorumUnavailable {
            mode: "quota_capability",
            bucket: bucket.to_string(),
            object: rustfs_config::QUOTA_CONFIG_FILE.to_string(),
            required: 1,
            achieved: 0,
        });
    }
    update_under_config_write_guard(sys, &guard, rustfs_config::QUOTA_CONFIG_FILE, data).await
}

pub async fn update_bucket_targets_under_transaction_lock(
    guard: &BucketMetadataMutationGuard,
    bucket: &str,
    data: Vec<u8>,
) -> Result<OffsetDateTime> {
    update_under_transaction_lock(guard, bucket, BUCKET_TARGETS_FILE, data).await
}

async fn update_under_config_write_guard(
    sys: Arc<RwLock<BucketMetadataSys>>,
    guard: &BucketMetadataMutationGuard,
    config_file: &str,
    data: Vec<u8>,
) -> Result<OffsetDateTime> {
    guard.ensure_valid(&guard.bucket)?;
    let metadata_sys = sys.read().await.clone();
    let updated = await_bucket_namespace_operation(
        Some(&guard.lifecycle_guard),
        &guard.bucket,
        "bucket config mutation",
        await_bucket_namespace_operation(
            Some(&guard.transaction_guard),
            &guard.bucket,
            "bucket config transaction",
            metadata_sys.update_checked(&guard.bucket, config_file, data, true, guard.incarnation_id),
        ),
    )
    .await?;
    guard.ensure_valid(&guard.bucket)?;
    Ok(updated)
}

async fn delete_under_config_write_guard(
    sys: Arc<RwLock<BucketMetadataSys>>,
    guard: &BucketMetadataMutationGuard,
    config_file: &str,
) -> Result<OffsetDateTime> {
    guard.ensure_valid(&guard.bucket)?;
    let metadata_sys = sys.read().await.clone();
    let updated = await_bucket_namespace_operation(
        Some(&guard.lifecycle_guard),
        &guard.bucket,
        "bucket config deletion",
        await_bucket_namespace_operation(
            Some(&guard.transaction_guard),
            &guard.bucket,
            "bucket config deletion transaction",
            metadata_sys.update_checked(&guard.bucket, config_file, Vec::new(), false, guard.incarnation_id),
        ),
    )
    .await?;
    guard.ensure_valid(&guard.bucket)?;
    Ok(updated)
}

/// Read-modify-write one bucket config file under both guards a config
/// write takes.
///
/// `mutate` sees the freshly loaded on-disk metadata and returns the
/// replacement payload for `config_file` (empty clears it, like
/// [`delete`]). Both the read and the persisted write happen inside the
/// same guards [`update`] takes, so the rewrite can neither clobber a
/// concurrent update to another config file nor lose a concurrent write to
/// the same one — unlike caching a mutated clone of previously read
/// metadata.
///
/// That exclusion is cluster-wide, not merely process-local: the transaction
/// lock is now taken for every config file rather than only the replication
/// targets one, so a writer on another node cannot land a whole-file save in
/// the middle of this read-modify-write.
pub async fn update_config_with<F>(bucket: &str, config_file: &str, mutate: F) -> Result<OffsetDateTime>
where
    F: FnOnce(&BucketMetadata) -> Result<Vec<u8>> + Send,
{
    let sys = get_bucket_metadata_sys()?;
    let guard = Box::pin(acquire_config_write_guard(sys.clone(), bucket)).await?;
    guard.ensure_valid(bucket)?;
    let metadata_sys = sys.read().await.clone();
    let updated = await_bucket_namespace_operation(
        Some(&guard.lifecycle_guard),
        bucket,
        "bucket config read-modify-write",
        await_bucket_namespace_operation(
            Some(&guard.transaction_guard),
            bucket,
            "bucket config read-modify-write transaction",
            Box::pin(metadata_sys.update_config_with_checked(bucket, config_file, mutate, guard.incarnation_id)),
        ),
    )
    .await?;
    guard.ensure_valid(bucket)?;
    Ok(updated)
}

/// Acquire a bucket's metadata transaction lock, held across a whole
/// read-modify-write of its metadata file.
///
/// Every config write loads the entire [`BucketMetadata`] blob, replaces one
/// field, and saves the whole thing back. The namespace locks inside
/// `read_config`/`save_config` are taken and released separately, so they do
/// not span that cycle: two nodes updating *different* config files of one
/// bucket both load the same blob, each set their own field, and the later
/// save drops the other's — with both clients already told 2xx. This is not
/// last-writer-wins on one document; an orthogonal config silently vanishes.
///
/// So the lock is per bucket, not per config file: a per-file key would let
/// exactly that pair run concurrently.
///
/// Callers that hold this guard must use [`update_under_transaction_lock`]
/// rather than [`update`] — see that function.
pub async fn acquire_bucket_metadata_transaction_lock(bucket: &str) -> Result<BucketMetadataMutationGuard> {
    acquire_config_write_guard(get_bucket_metadata_sys()?, bucket).await
}

/// Acquire the bucket transaction lock only if its incarnation still matches.
pub async fn acquire_bucket_metadata_transaction_lock_for_incarnation(
    bucket: &str,
    expected_incarnation_id: Uuid,
) -> Result<BucketMetadataMutationGuard> {
    acquire_config_write_guard_for_incarnation(get_bucket_metadata_sys()?, bucket, Some(expected_incarnation_id)).await
}

pub(crate) async fn acquire_bucket_metadata_transaction_lock_in(
    ctx: &crate::runtime::instance::InstanceContext,
    bucket: &str,
) -> Result<rustfs_lock::NamespaceLockGuard> {
    acquire_transaction_lock_with_sys(&bucket_metadata_sys_of(ctx)?, bucket).await
}

pub(crate) async fn acquire_bucket_metadata_transaction_read_lock_in(
    ctx: &crate::runtime::instance::InstanceContext,
    bucket: &str,
) -> Result<rustfs_lock::NamespaceLockGuard> {
    let sys = bucket_metadata_sys_of(ctx)?;
    let api = sys.read().await.object_store();
    let lock = api
        .new_ns_lock(RUSTFS_META_BUCKET, &bucket_metadata_transaction_lock_key(bucket))
        .await?;
    Ok(lock.get_read_lock(crate::set_disk::get_lock_acquire_timeout()).await?)
}

async fn acquire_transaction_lock_with_sys(
    sys: &Arc<RwLock<BucketMetadataSys>>,
    bucket: &str,
) -> Result<rustfs_lock::NamespaceLockGuard> {
    // Resolve the store under a short-lived read guard: this runs before the
    // write guard in `acquire_config_write_guards`, and must not still hold a
    // read guard when the namespace lock is awaited.
    let api = sys.read().await.object_store();
    let lock = api
        .new_ns_lock(RUSTFS_META_BUCKET, &bucket_metadata_transaction_lock_key(bucket))
        .await?;
    let acquire = lock.get_write_lock(crate::set_disk::get_lock_acquire_timeout());
    #[cfg(any(test, feature = "test-util"))]
    {
        tokio::pin!(acquire);
        let mut notified = false;
        let guard = futures::future::poll_fn(|cx| match std::future::Future::poll(acquire.as_mut(), cx) {
            std::task::Poll::Pending => {
                if !notified {
                    notify_config_write_lock_attempt(bucket);
                    notified = true;
                }
                std::task::Poll::Pending
            }
            std::task::Poll::Ready(result) => std::task::Poll::Ready(result),
        })
        .await?;
        Ok(guard)
    }
    #[cfg(not(any(test, feature = "test-util")))]
    Ok(acquire.await?)
}

/// The lock resource name is deliberately still the `bucket-targets` one it
/// had when only replication-target writes took it. The key is what nodes
/// agree on, so renaming it would leave a mixed-version cluster with two
/// disjoint keys — and old and new nodes would stop excluding each other on
/// the very writes that are serialized today.
fn bucket_metadata_transaction_lock_key(bucket: &str) -> String {
    format!("bucket-targets/{bucket}/transaction.lock")
}

pub async fn get_bucket_policy(bucket: &str) -> Result<(BucketPolicy, OffsetDateTime)> {
    let bucket_meta_sys_lock = get_bucket_metadata_sys()?;
    let bucket_meta_sys = bucket_meta_sys_lock.read().await.clone();

    bucket_meta_sys.get_bucket_policy(bucket).await
}

/// Returns the raw JSON string of the bucket policy as originally stored.
/// This preserves the exact format of the policy document as it was PUT.
pub async fn get_bucket_policy_raw(bucket: &str) -> Result<(String, OffsetDateTime)> {
    let bucket_meta_sys_lock = get_bucket_metadata_sys()?;
    let bucket_meta_sys = bucket_meta_sys_lock.read().await.clone();

    bucket_meta_sys.get_bucket_policy_raw(bucket).await
}

#[allow(
    dead_code,
    reason = "free-function facade over the live BucketMetadataSys::get_bucket_acl_config; no caller in this port (backlog#1823)"
)]
pub async fn get_bucket_acl_config(bucket: &str) -> Result<(String, OffsetDateTime)> {
    let bucket_meta_sys_lock = get_bucket_metadata_sys()?;
    let bucket_meta_sys = bucket_meta_sys_lock.read().await;

    bucket_meta_sys.get_bucket_acl_config(bucket).await
}

/// The bucket's durability override config (if any) with its update time.
///
/// `Ok((None, ..))` means the bucket has no override and follows the global
/// durability mode.
pub async fn get_durability_config(
    bucket: &str,
) -> Result<(Option<crate::bucket::durability::BucketDurabilityConfig>, OffsetDateTime)> {
    let bucket_meta_sys_lock = get_bucket_metadata_sys()?;
    let bucket_meta_sys = bucket_meta_sys_lock.read().await;

    let (bm, _) = bucket_meta_sys.get_config(bucket).await?;
    Ok((bm.durability_config(), bm.durability_config_updated_at))
}

pub async fn get_quota_config(bucket: &str) -> Result<(BucketQuota, OffsetDateTime)> {
    let bucket_meta_sys_lock = get_bucket_metadata_sys()?;
    let bucket_meta_sys = bucket_meta_sys_lock.read().await;

    bucket_meta_sys.get_quota_config(bucket).await
}

pub async fn get_bucket_targets_config(bucket: &str) -> Result<BucketTargets> {
    let bucket_meta_sys_lock = get_bucket_metadata_sys()?;
    let bucket_meta_sys = bucket_meta_sys_lock.read().await;

    bucket_meta_sys.get_bucket_targets_config(bucket).await
}

pub async fn get_cors_config(bucket: &str) -> Result<(CORSConfiguration, OffsetDateTime)> {
    let bucket_meta_sys_lock = get_bucket_metadata_sys()?;
    let bucket_meta_sys = bucket_meta_sys_lock.read().await;

    bucket_meta_sys.get_cors_config(bucket).await
}

pub async fn get_tagging_config(bucket: &str) -> Result<(Tagging, OffsetDateTime)> {
    let bucket_meta_sys_lock = get_bucket_metadata_sys()?;
    let bucket_meta_sys = bucket_meta_sys_lock.read().await;

    bucket_meta_sys.get_tagging_config(bucket).await
}

pub async fn get_public_access_block_config(bucket: &str) -> Result<(PublicAccessBlockConfiguration, OffsetDateTime)> {
    let bucket_meta_sys_lock = get_bucket_metadata_sys()?;
    let bucket_meta_sys = bucket_meta_sys_lock.read().await;

    bucket_meta_sys.get_public_access_block_config(bucket).await
}

pub async fn get_lifecycle_config(bucket: &str) -> Result<(BucketLifecycleConfiguration, OffsetDateTime)> {
    let bucket_meta_sys_lock = get_bucket_metadata_sys()?;
    let bucket_meta_sys = bucket_meta_sys_lock.read().await;

    bucket_meta_sys.get_lifecycle_config(bucket).await
}

pub async fn get_sse_config(bucket: &str) -> Result<(ServerSideEncryptionConfiguration, OffsetDateTime)> {
    let bucket_meta_sys_lock = get_bucket_metadata_sys()?;
    let bucket_meta_sys = bucket_meta_sys_lock.read().await;

    bucket_meta_sys.get_sse_config(bucket).await
}

pub async fn get_object_lock_config(bucket: &str) -> Result<(ObjectLockConfiguration, OffsetDateTime)> {
    let bucket_meta_sys_lock = get_bucket_metadata_sys()?;
    let bucket_meta_sys = bucket_meta_sys_lock.read().await.clone();

    bucket_meta_sys.get_object_lock_config(bucket).await
}

pub async fn get_object_lock_config_state(bucket: &str) -> Result<ObjectLockConfigState> {
    let bucket_meta_sys_lock = get_bucket_metadata_sys()?;
    let bucket_meta_sys = bucket_meta_sys_lock.read().await.clone();

    bucket_meta_sys.get_object_lock_config_state(bucket).await
}

pub(crate) async fn get_object_lock_config_state_in(
    ctx: &crate::runtime::instance::InstanceContext,
    bucket: &str,
) -> Result<ObjectLockConfigState> {
    let bucket_meta_sys_lock = bucket_metadata_sys_of(ctx)?;
    let bucket_meta_sys = bucket_meta_sys_lock.read().await.clone();

    bucket_meta_sys.get_object_lock_config_state(bucket).await
}

/// Re-read the Object Lock state and bucket incarnation from the same
/// authoritative metadata blob while the caller holds the transaction lock.
pub(crate) async fn get_object_lock_config_and_incarnation_from_disk_in(
    ctx: &crate::runtime::instance::InstanceContext,
    bucket: &str,
) -> Result<(ObjectLockConfigState, Uuid, OffsetDateTime)> {
    let bucket_meta_sys_lock = bucket_metadata_sys_of(ctx)?;
    let bucket_meta_sys = bucket_meta_sys_lock.read().await.clone();

    match bucket_meta_sys
        .read_authoritative_metadata_from_disk_under_transaction_lock(bucket)
        .await?
    {
        BucketMetadataAuthority::Authoritative(metadata)
            if metadata.bucket_incarnation_sidecar && !metadata.bucket_incarnation_id.is_nil() =>
        {
            let object_lock_config_updated_at = metadata.object_lock_config_updated_at;
            Ok((
                object_lock_config_state_from_authoritative_metadata(&metadata)?,
                metadata.bucket_incarnation_id,
                object_lock_config_updated_at,
            ))
        }
        BucketMetadataAuthority::Authoritative(_) => {
            Err(Error::other(format!("bucket incarnation metadata is not authoritative: {bucket}")))
        }
        BucketMetadataAuthority::MissingBucket => Err(Error::BucketNotFound(bucket.to_string())),
        BucketMetadataAuthority::Fabricated => {
            Err(Error::other(format!("bucket Object Lock metadata is not authoritative: {bucket}")))
        }
    }
}

/// Re-read the quota configuration and bucket incarnation from the same
/// authoritative metadata blob while the caller holds the bucket metadata
/// transaction read lock.
pub(crate) async fn get_quota_config_and_incarnation_from_disk_in(
    ctx: &crate::runtime::instance::InstanceContext,
    bucket: &str,
) -> Result<(Option<BucketQuota>, Uuid, OffsetDateTime)> {
    let bucket_meta_sys_lock = bucket_metadata_sys_of(ctx)?;
    let bucket_meta_sys = bucket_meta_sys_lock.read().await.clone();

    match bucket_meta_sys
        .read_authoritative_metadata_from_disk_under_transaction_lock(bucket)
        .await?
    {
        BucketMetadataAuthority::Authoritative(metadata)
            if metadata.bucket_incarnation_sidecar && !metadata.bucket_incarnation_id.is_nil() =>
        {
            Ok((
                metadata.quota_config.clone(),
                metadata.bucket_incarnation_id,
                metadata.quota_config_updated_at,
            ))
        }
        BucketMetadataAuthority::Authoritative(_) => {
            Err(Error::other(format!("bucket incarnation metadata is not authoritative: {bucket}")))
        }
        BucketMetadataAuthority::MissingBucket => Err(Error::BucketNotFound(bucket.to_string())),
        BucketMetadataAuthority::Fabricated => Err(Error::other(format!("bucket quota metadata is not authoritative: {bucket}"))),
    }
}

pub async fn get_replication_config(bucket: &str) -> Result<(ReplicationConfiguration, OffsetDateTime)> {
    let bucket_meta_sys_lock = get_bucket_metadata_sys()?;
    let bucket_meta_sys = bucket_meta_sys_lock.read().await;

    bucket_meta_sys.get_replication_config(bucket).await
}

pub async fn get_notification_config(bucket: &str) -> Result<Option<NotificationConfiguration>> {
    let bucket_meta_sys_lock = get_bucket_metadata_sys()?;
    let bucket_meta_sys = bucket_meta_sys_lock.read().await;

    bucket_meta_sys.get_notification_config(bucket).await
}

pub async fn get_versioning_config(bucket: &str) -> Result<(VersioningConfiguration, OffsetDateTime)> {
    let bucket_meta_sys_lock = get_bucket_metadata_sys()?;
    let bucket_meta_sys = bucket_meta_sys_lock.read().await;

    bucket_meta_sys.get_versioning_config(bucket).await
}

pub async fn get_website_config(bucket: &str) -> Result<(WebsiteConfiguration, OffsetDateTime)> {
    let bucket_meta_sys_lock = get_bucket_metadata_sys()?;
    let bucket_meta_sys = bucket_meta_sys_lock.read().await;

    bucket_meta_sys.get_website_config(bucket).await
}

pub async fn get_logging_config(bucket: &str) -> Result<(BucketLoggingStatus, OffsetDateTime)> {
    let bucket_meta_sys_lock = get_bucket_metadata_sys()?;
    let bucket_meta_sys = bucket_meta_sys_lock.read().await;

    bucket_meta_sys.get_logging_config(bucket).await
}

pub async fn get_accelerate_config(bucket: &str) -> Result<(AccelerateConfiguration, OffsetDateTime)> {
    let bucket_meta_sys_lock = get_bucket_metadata_sys()?;
    let bucket_meta_sys = bucket_meta_sys_lock.read().await;

    bucket_meta_sys.get_accelerate_config(bucket).await
}

pub async fn get_request_payment_config(bucket: &str) -> Result<(RequestPaymentConfiguration, OffsetDateTime)> {
    let bucket_meta_sys_lock = get_bucket_metadata_sys()?;
    let bucket_meta_sys = bucket_meta_sys_lock.read().await;

    bucket_meta_sys.get_request_payment_config(bucket).await
}

pub async fn get_config_from_disk(bucket: &str) -> Result<BucketMetadata> {
    let bucket_meta_sys_lock = get_bucket_metadata_sys()?;
    let bucket_meta_sys = bucket_meta_sys_lock.read().await;

    bucket_meta_sys.get_config_from_disk(bucket).await
}

#[allow(
    dead_code,
    reason = "ambient-facade variant of the live created_at_in; no caller in this port (backlog#1823)"
)]
pub async fn created_at(bucket: &str) -> Result<OffsetDateTime> {
    let bucket_meta_sys_lock = get_bucket_metadata_sys()?;
    let bucket_meta_sys = bucket_meta_sys_lock.read().await;

    bucket_meta_sys.created_at(bucket).await
}

pub async fn list_bucket_targets(bucket: &str) -> Result<BucketTargets> {
    let bucket_meta_sys_lock = get_bucket_metadata_sys()?;
    let bucket_meta_sys = bucket_meta_sys_lock.read().await;

    bucket_meta_sys.get_bucket_targets_config(bucket).await
}

/// Bound and lifetime of the negative cache for physically missing buckets.
/// Existing legacy buckets without metadata use a separate lifetime marker so
/// auth checks do not repeat erasure reads every TTL.
const MISSING_BUCKET_TTL: Duration = Duration::from_secs(30);
const MISSING_BUCKET_MAX_ENTRIES: u64 = 10_000;
const PEER_METADATA_NOT_PERSISTED: &str = "no persisted bucket metadata readable; peer cache left unchanged";
#[derive(Debug)]
struct MetadataPublishLockRegistry {
    locks: StdMutex<HashMap<String, Weak<Mutex<MetadataPublishLockState>>>>,
}

#[derive(Debug)]
struct MetadataPublishLockState {
    bucket: String,
    registry: Weak<MetadataPublishLockRegistry>,
    lock: Weak<Mutex<MetadataPublishLockState>>,
}

impl Drop for MetadataPublishLockState {
    fn drop(&mut self) {
        let Some(registry) = self.registry.upgrade() else {
            return;
        };
        let mut locks = registry.locks.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        if locks.get(&self.bucket).is_some_and(|current| current.ptr_eq(&self.lock)) {
            locks.remove(&self.bucket);
        }
    }
}

#[derive(Debug)]
struct MetadataPublishGuard {
    _guard: tokio::sync::OwnedMutexGuard<MetadataPublishLockState>,
}

#[derive(Debug, Clone)]
pub struct BucketMetadataSys {
    metadata_map: Arc<RwLock<HashMap<String, Arc<BucketMetadata>>>>,
    /// Serializes metadata-map commits and their derived cache updates for one
    /// bucket. Namespace locks, when present, are acquired before this lock.
    metadata_publish_locks: Arc<MetadataPublishLockRegistry>,
    /// Deduplicates concurrent lazy loads of one bucket's metadata, so N
    /// simultaneous cache misses issue a single disk read instead of N.
    ///
    /// This is the `singleflight` that upstream applies to its own lazy
    /// `GetConfig`. Without it the namespace *read* lock the load holds is no
    /// help: read locks are shared, so it excludes concurrent config writers
    /// but not concurrent readers, and every caller still pays a full
    /// erasure-set metadata fanout. A separate registry from
    /// `metadata_publish_locks`, reusing the same per-bucket lock machinery.
    ///
    /// Lock order: this lock, then the namespace lock, then the publish lock,
    /// then the metadata map. It is only ever taken as the first of those, so
    /// it cannot invert against a path that already holds one of the others.
    lazy_load_locks: Arc<MetadataPublishLockRegistry>,
    #[cfg(test)]
    lazy_load_lock_probe: Arc<std::sync::atomic::AtomicBool>,
    /// Counts disk loads taken by the lazy `get_config` path, so a test can
    /// prove concurrent misses collapse into one.
    #[cfg(test)]
    lazy_disk_loads: Arc<std::sync::atomic::AtomicUsize>,
    #[cfg(test)]
    legacy_migration_write_error: Arc<std::sync::atomic::AtomicBool>,
    #[cfg(test)]
    legacy_migration_lock_probe: Arc<std::sync::atomic::AtomicBool>,
    #[cfg(test)]
    object_lock_disk_read_errors: Arc<RwLock<HashSet<String>>>,
    /// Existing buckets confirmed to have no persisted metadata. These entries
    /// are non-authoritative and never enter `metadata_map`; config publish,
    /// peer reload, refresh, and bucket deletion fence their invalidation.
    fabricated_metadata: Arc<RwLock<HashSet<String>>>,
    /// Physically missing names are TTL-bounded to limit memory under bogus
    /// name floods while avoiding repeated namespace and erasure reads.
    missing_buckets: moka::future::Cache<String, ()>,
    api: Arc<ECStore>,
    initialized: Arc<RwLock<bool>>,
}

impl BucketMetadataSys {
    pub fn new(api: Arc<ECStore>) -> Self {
        Self {
            metadata_map: Arc::new(RwLock::new(HashMap::new())),
            metadata_publish_locks: Arc::new(MetadataPublishLockRegistry {
                locks: StdMutex::new(HashMap::new()),
            }),
            lazy_load_locks: Arc::new(MetadataPublishLockRegistry {
                locks: StdMutex::new(HashMap::new()),
            }),
            #[cfg(test)]
            lazy_load_lock_probe: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            #[cfg(test)]
            lazy_disk_loads: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            #[cfg(test)]
            legacy_migration_write_error: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            #[cfg(test)]
            legacy_migration_lock_probe: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            #[cfg(test)]
            object_lock_disk_read_errors: Arc::new(RwLock::new(HashSet::new())),
            fabricated_metadata: Arc::new(RwLock::new(HashSet::new())),
            missing_buckets: moka::future::Cache::builder()
                .max_capacity(MISSING_BUCKET_MAX_ENTRIES)
                .time_to_live(MISSING_BUCKET_TTL)
                .build(),
            api,
            initialized: Arc::new(RwLock::new(false)),
        }
    }

    pub(crate) fn object_store(&self) -> Arc<ECStore> {
        self.api.clone()
    }

    fn metadata_publish_lock(&self, bucket: &str) -> Arc<Mutex<MetadataPublishLockState>> {
        Self::bucket_lock_in(&self.metadata_publish_locks, bucket)
    }

    /// Per-bucket gate for the lazy `get_config` disk load. See
    /// [`Self::lazy_load_locks`].
    fn lazy_load_lock(&self, bucket: &str) -> Arc<Mutex<MetadataPublishLockState>> {
        Self::bucket_lock_in(&self.lazy_load_locks, bucket)
    }

    fn bucket_lock_in(registry: &Arc<MetadataPublishLockRegistry>, bucket: &str) -> Arc<Mutex<MetadataPublishLockState>> {
        let mut locks = registry.locks.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        locks.get(bucket).and_then(Weak::upgrade).unwrap_or_else(|| {
            let lock = Arc::new_cyclic(|lock| {
                Mutex::new(MetadataPublishLockState {
                    bucket: bucket.to_string(),
                    registry: Arc::downgrade(registry),
                    lock: lock.clone(),
                })
            });
            locks.insert(bucket.to_string(), Arc::downgrade(&lock));
            lock
        })
    }

    async fn lock_metadata_publish(
        &self,
        bucket: &str,
        namespace_guard: &rustfs_lock::NamespaceLockGuard,
        operation: &'static str,
    ) -> Result<MetadataPublishGuard> {
        let lock = self.metadata_publish_lock(bucket);
        let guard = await_bucket_namespace_operation(Some(namespace_guard), bucket, operation, async {
            Ok(MetadataPublishGuard {
                _guard: lock.lock_owned().await,
            })
        })
        .await?;
        if namespace_guard.is_lock_lost() {
            return Err(Error::other(format!("bucket namespace lock was lost before {operation}: {bucket}")));
        }
        Ok(guard)
    }

    async fn bucket_exists(
        &self,
        bucket: &str,
        namespace_guard: &rustfs_lock::NamespaceLockGuard,
        operation: &'static str,
    ) -> Result<bool> {
        await_bucket_namespace_operation(Some(namespace_guard), bucket, operation, async {
            match self
                .api
                .peer_sys
                .get_bucket_info(bucket, &crate::storage_api_contracts::bucket::BucketOptions::default())
                .await
            {
                Ok(_) => Ok(true),
                Err(crate::disk::error::Error::VolumeNotFound) => Ok(false),
                Err(err) => Err(err.into()),
            }
        })
        .await
    }

    pub async fn init(&mut self, buckets: Vec<String>) {
        let _ = self.init_internal(buckets).await;
    }
    async fn init_internal(&self, buckets: Vec<String>) -> Result<()> {
        let count = runtime_sources::endpoint_erasure_set_count()
            .map(|count| count * 10)
            .ok_or_else(|| Error::other("endpoint pools not initialized"))?;

        let mut failed_buckets: HashSet<String> = HashSet::new();
        let mut buckets = buckets.as_slice();

        loop {
            if buckets.len() < count {
                self.concurrent_load(buckets, &mut failed_buckets, MetadataLoadMode::Initial)
                    .await;
                break;
            }

            self.concurrent_load(&buckets[..count], &mut failed_buckets, MetadataLoadMode::Initial)
                .await;

            buckets = &buckets[count..]
        }

        let mut initialized = self.initialized.write().await;
        *initialized = true;

        Ok(())
    }

    async fn concurrent_load(&self, buckets: &[String], failed_buckets: &mut HashSet<String>, mode: MetadataLoadMode) {
        let mut futures = Vec::new();

        for bucket in buckets.iter() {
            let api = self.api.clone();
            let bucket = bucket.clone();
            futures.push(async move {
                sleep(Duration::from_millis(30)).await;
                let expected = match mode {
                    MetadataLoadMode::Initial => None,
                    MetadataLoadMode::Refresh => self.metadata_map.read().await.get(&bucket).cloned(),
                };
                let namespace_lock = api.new_ns_lock(&bucket, &bucket).await?;
                let namespace_guard = namespace_lock
                    .get_read_lock(crate::set_disk::get_lock_acquire_timeout())
                    .await?;
                self.load_bucket_under_namespace(&bucket, mode, expected.as_ref(), &namespace_guard)
                    .await
            });
        }

        let results = join_all(futures).await;

        for (idx, res) in results.into_iter().enumerate() {
            match res {
                Ok(()) => {}
                Err(e) => {
                    error!("Unable to load bucket metadata, will be retried: {:?}", e);
                    if let Some(bucket) = buckets.get(idx) {
                        failed_buckets.insert(bucket.clone());
                    }
                }
            }
        }
    }

    async fn concurrent_refresh_load(sys: Arc<RwLock<Self>>, buckets: &[String], failed_buckets: &mut HashSet<String>) {
        let mut futures = Vec::with_capacity(buckets.len());
        for bucket in buckets {
            let sys = Arc::clone(&sys);
            let bucket = bucket.clone();
            futures.push(async move {
                sleep(Duration::from_millis(30)).await;
                let api = sys.read().await.api.clone();
                let namespace_lock = api.new_ns_lock(&bucket, &bucket).await?;
                let namespace_guard = namespace_lock
                    .get_read_lock(crate::set_disk::get_lock_acquire_timeout())
                    .await?;
                let metadata_sys = sys.read().await;
                let expected = metadata_sys.metadata_map.read().await.get(&bucket).cloned();
                metadata_sys
                    .load_bucket_under_namespace(&bucket, MetadataLoadMode::Refresh, expected.as_ref(), &namespace_guard)
                    .await
            });
        }
        let results = join_all(futures).await;
        for (idx, result) in results.into_iter().enumerate() {
            if let Err(err) = result {
                error!("Unable to load bucket metadata, will be retried: {:?}", err);
                if let Some(bucket) = buckets.get(idx) {
                    failed_buckets.insert(bucket.clone());
                }
            }
        }
    }

    async fn load_bucket_under_namespace(
        &self,
        bucket: &str,
        mode: MetadataLoadMode,
        expected: Option<&Arc<BucketMetadata>>,
        namespace_guard: &rustfs_lock::NamespaceLockGuard,
    ) -> Result<()> {
        await_bucket_namespace_operation(
            Some(namespace_guard),
            bucket,
            "bucket metadata heal",
            self.api.heal_bucket(bucket, &HealOpts::default()),
        )
        .await?;

        if !self
            .bucket_exists(bucket, namespace_guard, "bucket metadata existence check")
            .await?
        {
            if matches!(mode, MetadataLoadMode::Refresh) {
                let _publish_guard = self
                    .lock_metadata_publish(bucket, namespace_guard, "stale bucket metadata removal")
                    .await?;
                let removed = self.metadata_map.write().await.remove(bucket).is_some();
                self.fabricated_metadata.write().await.remove(bucket);
                self.missing_buckets.insert(bucket.to_string(), ()).await;
                if removed {
                    BucketTargetSys::get().delete(bucket).await;
                    clear_bucket_durability(bucket);
                }
            }
            return Ok(());
        }

        let (bm, persisted) = await_bucket_namespace_operation(
            Some(namespace_guard),
            bucket,
            "bucket metadata load",
            load_bucket_metadata_parse_with_presence(self.api.clone(), bucket, true),
        )
        .await?;
        match mode {
            MetadataLoadMode::Initial if persisted => {
                let bm = Arc::new(bm);
                let _publish_guard = self
                    .lock_metadata_publish(bucket, namespace_guard, "initial bucket metadata publish")
                    .await?;
                self.metadata_map.write().await.insert(bucket.to_string(), Arc::clone(&bm));
                self.fabricated_metadata.write().await.remove(bucket);
                self.missing_buckets.invalidate(bucket).await;
                sync_bucket_target_sys(bucket, &bm).await;
                sync_bucket_durability(bucket, &bm);
            }
            MetadataLoadMode::Initial => {
                let _publish_guard = self
                    .lock_metadata_publish(bucket, namespace_guard, "initial bucket metadata absence publish")
                    .await?;
                if !self.metadata_map.read().await.contains_key(bucket) {
                    self.fabricated_metadata.write().await.insert(bucket.to_string());
                    self.missing_buckets.invalidate(bucket).await;
                }
            }
            MetadataLoadMode::Refresh => {
                self.publish_if_unchanged(bucket, expected, bm, persisted, namespace_guard)
                    .await?;
            }
        }
        Ok(())
    }

    async fn publish_if_unchanged(
        &self,
        bucket: &str,
        expected: Option<&Arc<BucketMetadata>>,
        metadata: BucketMetadata,
        persisted: bool,
        namespace_guard: &rustfs_lock::NamespaceLockGuard,
    ) -> Result<()> {
        if !persisted {
            let _publish_guard = self
                .lock_metadata_publish(bucket, namespace_guard, "refreshed bucket metadata absence publish")
                .await?;
            let mut map = self.metadata_map.write().await;
            let unchanged = match (expected, map.get(bucket)) {
                (None, None) => true,
                (Some(expected), Some(current)) => Arc::ptr_eq(expected, current),
                _ => false,
            };
            if !unchanged {
                return Ok(());
            }
            let removed = map.remove(bucket).is_some();
            drop(map);
            self.fabricated_metadata.write().await.insert(bucket.to_string());
            self.missing_buckets.invalidate(bucket).await;
            if removed {
                BucketTargetSys::get().delete(bucket).await;
                clear_bucket_durability(bucket);
            }
            return Ok(());
        }
        let _publish_guard = self
            .lock_metadata_publish(bucket, namespace_guard, "refreshed bucket metadata publish")
            .await?;
        let metadata = Arc::new(metadata);
        let mut map = self.metadata_map.write().await;
        let unchanged = match (expected, map.get(bucket)) {
            (None, None) => true,
            (Some(expected), Some(current)) => Arc::ptr_eq(expected, current),
            _ => false,
        };
        if !unchanged {
            return Ok(());
        }
        map.insert(bucket.to_string(), Arc::clone(&metadata));
        drop(map);
        self.fabricated_metadata.write().await.remove(bucket);
        self.missing_buckets.invalidate(bucket).await;
        sync_bucket_target_sys(bucket, &metadata).await;
        sync_bucket_durability(bucket, &metadata);
        Ok(())
    }

    pub async fn get(&self, bucket: &str) -> Result<Arc<BucketMetadata>> {
        if is_meta_bucketname(bucket) {
            return Err(Error::ConfigNotFound);
        }

        let map = self.metadata_map.read().await;
        if let Some(bm) = map.get(bucket) {
            Ok(bm.clone())
        } else {
            Err(Error::ConfigNotFound)
        }
    }

    pub async fn set(&self, bucket: String, bm: Arc<BucketMetadata>) {
        if !is_meta_bucketname(&bucket) {
            let publish_lock = self.metadata_publish_lock(&bucket);
            let _publish_guard = publish_lock.lock().await;
            let mut map = self.metadata_map.write().await;
            map.insert(bucket.clone(), bm.clone());
            drop(map);
            self.fabricated_metadata.write().await.remove(&bucket);
            self.missing_buckets.invalidate(&bucket).await;
            sync_bucket_target_sys(&bucket, &bm).await;
            sync_bucket_durability(&bucket, &bm);
        }
    }

    /// Remove a bucket's cached metadata from the in-memory map.
    ///
    /// Returns `true` if an entry was present. Reserved meta buckets are ignored.
    pub async fn remove(&self, bucket: &str) -> bool {
        if is_meta_bucketname(bucket) {
            return false;
        }
        let publish_lock = self.metadata_publish_lock(bucket);
        let _publish_guard = publish_lock.lock().await;
        let mut map = self.metadata_map.write().await;
        let removed = map.remove(bucket).is_some();
        drop(map);
        let removed_fabricated = self.fabricated_metadata.write().await.remove(bucket);
        self.missing_buckets.insert(bucket.to_string(), ()).await;
        if removed {
            BucketTargetSys::get().delete(bucket).await;
            clear_bucket_durability(bucket);
        }
        removed || removed_fabricated
    }

    async fn _reset(&mut self) {
        let mut map = self.metadata_map.write().await;
        map.clear();
        drop(map);
        self.fabricated_metadata.write().await.clear();
        self.missing_buckets.invalidate_all();
    }

    /// The `Box::pin`s here and in [`Self::update_checked`] are load-bearing, not
    /// style. A bucket-config write nests incarnation resolution (which can drive
    /// legacy migration and a peer fan-out), a full metadata load, and `save`,
    /// which is itself an object PUT that pulls in the whole erasure write path —
    /// and every request that mutates bucket config is already several futures
    /// deep. Inlining all of that into one state machine overflows the worker
    /// stack in debug builds (rustfs#5648: `SIGABRT`, ~780KiB consumed between
    /// `update` and the config read alone). Keep these boxed.
    pub async fn update(&self, bucket: &str, config_file: &str, data: Vec<u8>) -> Result<OffsetDateTime> {
        let incarnation_id = Box::pin(self.get_bucket_incarnation_id(bucket)).await?;
        Box::pin(self.update_checked(bucket, config_file, data, true, incarnation_id)).await
    }

    pub async fn delete(&self, bucket: &str, config_file: &str) -> Result<OffsetDateTime> {
        let incarnation_id = self.get_bucket_incarnation_id(bucket).await?;
        self.update_checked(bucket, config_file, Vec::new(), false, incarnation_id)
            .await
    }

    async fn update_checked(
        &self,
        bucket: &str,
        config_file: &str,
        data: Vec<u8>,
        parse: bool,
        expected_incarnation_id: Uuid,
    ) -> Result<OffsetDateTime> {
        // Load through this system's own store, the one `save` persists to
        // (backlog#1052 S7). Reading from the ambient handle instead made the
        // read and the write of a single read-modify-write able to target
        // different instances.
        let mut bm = Box::pin(Self::load_bucket_metadata_for_update(self.api.clone(), bucket, parse)).await?;
        if !bm.bucket_incarnation_sidecar || bm.bucket_incarnation_id != expected_incarnation_id {
            return Err(Error::BucketNotFound(bucket.to_string()));
        }

        let updated = bm.update_config(config_file, data)?;

        Box::pin(self.save(bm)).await?;

        Ok(updated)
    }

    /// See the free [`update_config_with`]: same load-mutate-persist cycle as
    /// [`Self::update`], with the payload computed from the loaded metadata
    /// instead of supplied up front. Loads through this system's own store so
    /// the read and the persisted write target the same instance.
    #[allow(dead_code, reason = "asserted by this file's tests (backlog#1823)")]
    async fn update_config_with<F>(&self, bucket: &str, config_file: &str, mutate: F) -> Result<OffsetDateTime>
    where
        F: FnOnce(&BucketMetadata) -> Result<Vec<u8>> + Send,
    {
        let incarnation_id = self.get_bucket_incarnation_id(bucket).await?;
        self.update_config_with_checked(bucket, config_file, mutate, incarnation_id)
            .await
    }

    async fn update_config_with_checked<F>(
        &self,
        bucket: &str,
        config_file: &str,
        mutate: F,
        expected_incarnation_id: Uuid,
    ) -> Result<OffsetDateTime>
    where
        F: FnOnce(&BucketMetadata) -> Result<Vec<u8>> + Send,
    {
        let mut bm = Box::pin(Self::load_bucket_metadata_for_update(self.api.clone(), bucket, true)).await?;
        if !bm.bucket_incarnation_sidecar || bm.bucket_incarnation_id != expected_incarnation_id {
            return Err(Error::BucketNotFound(bucket.to_string()));
        }

        let data = mutate(&bm)?;
        let updated = bm.update_config(config_file, data)?;

        Box::pin(self.save(bm)).await?;

        Ok(updated)
    }

    /// Load a bucket's on-disk metadata as the base of a config rewrite.
    /// Outside erasure setups a missing metadata file degrades to a fresh
    /// default (legacy buckets without one); erasure setups fail instead of
    /// fabricating state that a quorum may still hold.
    async fn load_bucket_metadata_for_update(store: Arc<ECStore>, bucket: &str, parse: bool) -> Result<BucketMetadata> {
        if is_meta_bucketname(bucket) {
            return Err(Error::other("errInvalidArgument"));
        }

        match load_bucket_metadata_parse(store, bucket, parse).await {
            Ok(res) => Ok(res),
            Err(err) => {
                if !runtime_sources::setup_is_erasure().await
                    && !runtime_sources::setup_is_dist_erasure().await
                    && is_err_bucket_not_found(&err)
                {
                    Ok(BucketMetadata::new(bucket))
                } else {
                    error!("load bucket metadata failed: {}", err);
                    Err(err)
                }
            }
        }
    }

    async fn save(&self, bm: BucketMetadata) -> Result<()> {
        if is_meta_bucketname(&bm.name) {
            return Err(Error::other("errInvalidArgument"));
        }

        self.persist_and_set(bm).await
    }

    /// Persist metadata through this system's own store and cache it here
    /// (backlog#1052 S7). The store-scoped bucket path uses this so a second
    /// server's metadata never leaks into the ambient (first) instance.
    pub(crate) async fn persist_and_set(&self, bm: BucketMetadata) -> Result<()> {
        let mut bm = bm;
        bm.save_with_store(self.api.clone()).await?;

        self.set(bm.name.clone(), Arc::new(bm)).await;

        Ok(())
    }

    async fn persist_new_and_set(&self, mut bm: BucketMetadata) -> Result<()> {
        bm.save_with_store(self.api.clone()).await?;
        save_bucket_incarnation(self.api.clone(), &bm.name, bm.bucket_incarnation_id).await?;
        bm.bucket_incarnation_sidecar = true;
        self.set(bm.name.clone(), Arc::new(bm)).await;
        Ok(())
    }

    async fn bucket_names(&self) -> Vec<String> {
        let mut names = self.metadata_map.read().await.keys().cloned().collect::<HashSet<_>>();
        names.extend(self.fabricated_metadata.read().await.iter().cloned());
        names.into_iter().collect()
    }

    pub async fn get_config_from_disk(&self, bucket: &str) -> Result<BucketMetadata> {
        if is_meta_bucketname(bucket) {
            return Err(Error::other("errInvalidArgument"));
        }

        load_bucket_metadata(self.api.clone(), bucket).await
    }

    /// Reload persisted metadata under the bucket namespace generation fence.
    ///
    /// A miss is never published as an authoritative default, and a snapshot
    /// read before delete plus same-name recreation cannot replace the new
    /// generation.
    #[allow(dead_code, reason = "asserted by this file's tests (backlog#1823)")]
    pub(crate) async fn reload_from_store(&self, bucket: &str) -> Result<()> {
        if is_meta_bucketname(bucket) {
            return Err(Error::other("errInvalidArgument"));
        }

        let namespace_lock = self.api.new_ns_lock(bucket, bucket).await?;
        let namespace_guard = namespace_lock
            .get_read_lock(crate::set_disk::get_lock_acquire_timeout())
            .await?;
        self.reload_from_store_under_namespace(bucket, &namespace_guard).await
    }

    async fn reload_from_store_under_namespace(
        &self,
        bucket: &str,
        namespace_guard: &rustfs_lock::NamespaceLockGuard,
    ) -> Result<()> {
        let expected = self.metadata_map.read().await.get(bucket).cloned();
        if !self
            .bucket_exists(bucket, namespace_guard, "peer bucket metadata existence check")
            .await?
        {
            return Err(Error::other(PEER_METADATA_NOT_PERSISTED));
        }
        let (metadata, persisted) = await_bucket_namespace_operation(
            Some(namespace_guard),
            bucket,
            "peer bucket metadata load",
            load_bucket_metadata_parse_with_presence(self.api.clone(), bucket, true),
        )
        .await?;
        if !persisted {
            return Err(Error::other(PEER_METADATA_NOT_PERSISTED));
        }
        self.publish_if_unchanged(bucket, expected.as_ref(), metadata, true, namespace_guard)
            .await
    }

    pub async fn get_config(&self, bucket: &str) -> Result<(Arc<BucketMetadata>, bool)> {
        let has_bm = {
            let map = self.metadata_map.read().await;
            map.get(bucket).cloned()
        };

        if let Some(bm) = has_bm {
            Ok((bm, false))
        } else {
            if self.fabricated_metadata.read().await.contains(bucket) || self.missing_buckets.get(bucket).await.is_some() {
                let mut bm = BucketMetadata::new(bucket);
                bm.default_timestamps();
                return Ok((Arc::new(bm), true));
            }

            // Collapse concurrent misses for this bucket into one disk load.
            // Taken before the namespace lock — see `lazy_load_locks` for the
            // ordering rule.
            let load_lock = self.lazy_load_lock(bucket);
            let _load_guard = load_lock.lock_owned().await;

            // Re-check every state: whoever held the gate before us may have
            // already answered this exact question.
            if let Some(bm) = self.metadata_map.read().await.get(bucket).cloned() {
                return Ok((bm, true));
            }
            if self.fabricated_metadata.read().await.contains(bucket) || self.missing_buckets.get(bucket).await.is_some() {
                let mut bm = BucketMetadata::new(bucket);
                bm.default_timestamps();
                return Ok((Arc::new(bm), true));
            }

            #[cfg(test)]
            self.lazy_disk_loads.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

            let lock = self.api.new_ns_lock(bucket, bucket).await?;
            let guard = lock.get_read_lock(crate::set_disk::get_lock_acquire_timeout()).await?;
            #[cfg(test)]
            if self.lazy_load_lock_probe.load(std::sync::atomic::Ordering::Relaxed) {
                let competing = self.api.new_ns_lock(bucket, bucket).await?;
                assert!(
                    competing.get_write_lock(Duration::from_millis(20)).await.is_err(),
                    "lazy metadata IO must start while the bucket namespace read lock is held"
                );
            }
            let (bm, persisted) = match await_bucket_namespace_operation(
                Some(&guard),
                bucket,
                "lazy bucket metadata load",
                Box::pin(load_bucket_metadata_parse_with_presence(self.api.clone(), bucket, true)),
            )
            .await
            {
                Ok(res) => res,
                Err(err) => {
                    return if *self.initialized.read().await {
                        Err(Error::other("errBucketMetadataNotInitialized"))
                    } else {
                        Err(err)
                    };
                }
            };

            let bm = Arc::new(bm);

            if persisted {
                await_bucket_namespace_operation(
                    Some(&guard),
                    bucket,
                    "lazy bucket metadata existence check",
                    Box::pin(async {
                        self.api
                            .peer_sys
                            .get_bucket_info(bucket, &crate::storage_api_contracts::bucket::BucketOptions::default())
                            .await
                            .map(|_| ())
                            .map_err(Into::into)
                    }),
                )
                .await?;
                if guard.is_lock_lost() {
                    return Err(Error::other(format!(
                        "bucket namespace lock was lost before lazy bucket metadata publish: {bucket}"
                    )));
                }
                let _publish_guard = self
                    .lock_metadata_publish(bucket, &guard, "lazy bucket metadata publish")
                    .await?;
                let mut map = self.metadata_map.write().await;
                if let Some(current) = map.get(bucket) {
                    return Ok((Arc::clone(current), true));
                }
                map.insert(bucket.to_string(), bm.clone());
                drop(map);
                self.fabricated_metadata.write().await.remove(bucket);
                self.missing_buckets.invalidate(bucket).await;
                sync_bucket_target_sys(bucket, &bm).await;
                sync_bucket_durability(bucket, &bm);
            } else {
                let exists = self
                    .bucket_exists(bucket, &guard, "lazy bucket metadata existence check")
                    .await?;
                let _publish_guard = self
                    .lock_metadata_publish(bucket, &guard, "lazy bucket metadata absence publish")
                    .await?;
                if let Some(current) = self.metadata_map.read().await.get(bucket).cloned() {
                    return Ok((current, true));
                }
                if exists {
                    self.fabricated_metadata.write().await.insert(bucket.to_string());
                    self.missing_buckets.invalidate(bucket).await;
                } else {
                    self.fabricated_metadata.write().await.remove(bucket);
                    self.missing_buckets.insert(bucket.to_string(), ()).await;
                }
            }

            Ok((bm, true))
        }
    }

    pub async fn get_versioning_config(&self, bucket: &str) -> Result<(VersioningConfiguration, OffsetDateTime)> {
        let bm = match self.get_config(bucket).await {
            Ok((res, _)) => res,
            Err(err) => {
                return if err == Error::ConfigNotFound {
                    Ok((VersioningConfiguration::default(), OffsetDateTime::UNIX_EPOCH))
                } else {
                    Err(err)
                };
            }
        };

        if !bm.versioning_config_xml.is_empty() && bm.versioning_config.is_none() {
            Err(Error::other("persisted bucket versioning configuration is invalid"))
        } else if let Some(config) = &bm.versioning_config {
            Ok((config.clone(), bm.versioning_config_updated_at))
        } else {
            Ok((VersioningConfiguration::default(), bm.versioning_config_updated_at))
        }
    }

    pub async fn get_bucket_policy(&self, bucket: &str) -> Result<(BucketPolicy, OffsetDateTime)> {
        let bm = match self.get_metadata_authority(bucket).await? {
            BucketMetadataAuthority::Authoritative(bm) => bm,
            BucketMetadataAuthority::Fabricated => match self.migrate_legacy_metadata(bucket).await? {
                BucketMetadataAuthority::Authoritative(bm) => bm,
                BucketMetadataAuthority::Fabricated => {
                    return Err(Error::other(format!("bucket policy metadata is not authoritative: {bucket}")));
                }
                BucketMetadataAuthority::MissingBucket => return Err(Error::ConfigNotFound),
            },
            BucketMetadataAuthority::MissingBucket => return Err(Error::ConfigNotFound),
        };

        if let Some(config) = &bm.policy_config {
            Ok((config.clone(), bm.policy_config_updated_at))
        } else if !bm.policy_config_json.is_empty() {
            Ok((serde_json::from_slice(&bm.policy_config_json)?, bm.policy_config_updated_at))
        } else {
            Err(Error::ConfigNotFound)
        }
    }

    /// Returns the raw JSON string of the bucket policy as originally stored.
    /// This preserves the exact format of the policy document as it was PUT.
    pub async fn get_bucket_policy_raw(&self, bucket: &str) -> Result<(String, OffsetDateTime)> {
        let bm = match self.get_metadata_authority(bucket).await? {
            BucketMetadataAuthority::Authoritative(bm) => bm,
            BucketMetadataAuthority::Fabricated => {
                return Err(Error::other(format!("bucket policy metadata is not authoritative: {bucket}")));
            }
            BucketMetadataAuthority::MissingBucket => return Err(Error::ConfigNotFound),
        };

        if bm.policy_config_json.is_empty() {
            Err(Error::ConfigNotFound)
        } else {
            let policy_str = String::from_utf8(bm.policy_config_json.clone())
                .map_err(|e| Error::other(format!("invalid UTF-8 in policy JSON: {}", e)))?;
            Ok((policy_str, bm.policy_config_updated_at))
        }
    }

    pub async fn get_bucket_acl_config(&self, bucket: &str) -> Result<(String, OffsetDateTime)> {
        let (bm, _) = self.get_config(bucket).await?;

        if let Some(config) = &bm.bucket_acl_config {
            Ok((config.clone(), bm.bucket_acl_config_updated_at))
        } else {
            Err(Error::ConfigNotFound)
        }
    }

    pub async fn get_tagging_config(&self, bucket: &str) -> Result<(Tagging, OffsetDateTime)> {
        let (bm, _) = self.get_config(bucket).await?;

        if let Some(config) = &bm.tagging_config {
            Ok((config.clone(), bm.tagging_config_updated_at))
        } else {
            Err(Error::ConfigNotFound)
        }
    }

    pub async fn get_public_access_block_config(&self, bucket: &str) -> Result<(PublicAccessBlockConfiguration, OffsetDateTime)> {
        let (bm, _) = self.get_config(bucket).await?;

        if let Some(config) = &bm.public_access_block_config {
            Ok((config.clone(), bm.public_access_block_config_updated_at))
        } else {
            Err(Error::ConfigNotFound)
        }
    }

    pub async fn get_object_lock_config(&self, bucket: &str) -> Result<(ObjectLockConfiguration, OffsetDateTime)> {
        match self.get_object_lock_config_state(bucket).await? {
            ObjectLockConfigState::Configured { config, updated_at } => Ok((config, updated_at)),
            ObjectLockConfigState::ConfirmedAbsent => Err(Error::ConfigNotFound),
            ObjectLockConfigState::Fabricated => {
                Err(Error::other(format!("bucket Object Lock metadata is not authoritative: {bucket}")))
            }
        }
    }

    pub async fn get_object_lock_config_state(&self, bucket: &str) -> Result<ObjectLockConfigState> {
        match self.get_metadata_authority(bucket).await? {
            BucketMetadataAuthority::Authoritative(bm) => object_lock_config_state_from_authoritative_metadata(&bm),
            BucketMetadataAuthority::Fabricated => match self.migrate_legacy_metadata(bucket).await? {
                BucketMetadataAuthority::Authoritative(bm) => object_lock_config_state_from_authoritative_metadata(&bm),
                BucketMetadataAuthority::MissingBucket => Err(Error::ConfigNotFound),
                BucketMetadataAuthority::Fabricated => Ok(ObjectLockConfigState::Fabricated),
            },
            BucketMetadataAuthority::MissingBucket => Err(Error::ConfigNotFound),
        }
    }

    async fn get_bucket_incarnation_id(&self, bucket: &str) -> Result<Uuid> {
        let authority = self.get_metadata_authority(bucket).await?;
        if let BucketMetadataAuthority::Authoritative(metadata) = &authority
            && metadata.bucket_incarnation_sidecar
            && !metadata.bucket_incarnation_id.is_nil()
        {
            return Ok(metadata.bucket_incarnation_id);
        }

        match authority {
            BucketMetadataAuthority::Authoritative(_) | BucketMetadataAuthority::Fabricated => {
                match self.migrate_legacy_metadata(bucket).await? {
                    BucketMetadataAuthority::Authoritative(metadata)
                        if metadata.bucket_incarnation_sidecar && !metadata.bucket_incarnation_id.is_nil() =>
                    {
                        Ok(metadata.bucket_incarnation_id)
                    }
                    BucketMetadataAuthority::Authoritative(_) | BucketMetadataAuthority::Fabricated => {
                        Err(Error::other(format!("bucket incarnation metadata is not authoritative: {bucket}")))
                    }
                    BucketMetadataAuthority::MissingBucket => Err(Error::BucketNotFound(bucket.to_string())),
                }
            }
            BucketMetadataAuthority::MissingBucket => Err(Error::BucketNotFound(bucket.to_string())),
        }
    }

    async fn get_bucket_incarnation_id_from_disk(&self, bucket: &str) -> Result<Uuid> {
        let transaction_lock = self
            .api
            .new_ns_lock(RUSTFS_META_BUCKET, &bucket_metadata_transaction_lock_key(bucket))
            .await?;
        let _transaction_guard = transaction_lock
            .get_read_lock(crate::set_disk::get_lock_acquire_timeout())
            .await?;
        let incarnation_id = load_bucket_incarnation(self.api.clone(), bucket).await?;
        if _transaction_guard.is_lock_lost() {
            return Err(Error::other(format!("bucket incarnation metadata transaction lock was lost: {bucket}")));
        }
        match incarnation_id {
            Some(incarnation_id) if !incarnation_id.is_nil() => Ok(incarnation_id),
            _ => Err(Error::other(format!("bucket incarnation metadata is not authoritative: {bucket}"))),
        }
    }

    async fn get_metadata_authority(&self, bucket: &str) -> Result<BucketMetadataAuthority> {
        if let Some(bm) = self.metadata_map.read().await.get(bucket).cloned() {
            return Ok(BucketMetadataAuthority::Authoritative(bm));
        }
        if self.fabricated_metadata.read().await.contains(bucket) {
            return Ok(BucketMetadataAuthority::Fabricated);
        }
        if self.missing_buckets.get(bucket).await.is_some() {
            return Ok(BucketMetadataAuthority::MissingBucket);
        }

        self.get_config(bucket).await?;

        if let Some(bm) = self.metadata_map.read().await.get(bucket).cloned() {
            Ok(BucketMetadataAuthority::Authoritative(bm))
        } else if self.fabricated_metadata.read().await.contains(bucket) {
            Ok(BucketMetadataAuthority::Fabricated)
        } else if self.missing_buckets.get(bucket).await.is_some() {
            Ok(BucketMetadataAuthority::MissingBucket)
        } else {
            Err(Error::other(format!("bucket metadata authority was not classified: {bucket}")))
        }
    }

    async fn migrate_legacy_metadata(&self, bucket: &str) -> Result<BucketMetadataAuthority> {
        let transaction_lock = self
            .api
            .new_ns_lock(RUSTFS_META_BUCKET, &bucket_metadata_transaction_lock_key(bucket))
            .await?;
        let _transaction_guard = transaction_lock
            .get_write_lock(crate::set_disk::get_lock_acquire_timeout())
            .await?;

        let authority = self
            .load_authoritative_metadata_from_disk_under_transaction_lock(bucket)
            .await?;
        if _transaction_guard.is_lock_lost() {
            return Err(Error::other(format!("legacy bucket metadata transaction lock was lost: {bucket}")));
        }
        Ok(authority)
    }

    async fn load_authoritative_metadata_from_disk_under_transaction_lock(
        &self,
        bucket: &str,
    ) -> Result<BucketMetadataAuthority> {
        #[cfg(test)]
        if self.object_lock_disk_read_errors.write().await.remove(bucket) {
            return Err(Error::other(format!("injected Object Lock metadata disk read failure: {bucket}")));
        }

        let namespace_lock = self.api.new_ns_lock(bucket, bucket).await?;
        let namespace_guard = namespace_lock
            .get_read_lock(crate::set_disk::get_lock_acquire_timeout())
            .await?;

        let bucket_info = match await_bucket_namespace_operation(
            Some(&namespace_guard),
            bucket,
            "legacy bucket metadata existence check",
            async {
                self.api
                    .peer_sys
                    .get_bucket_info(bucket, &crate::storage_api_contracts::bucket::BucketOptions::default())
                    .await
                    .map_err(crate::error::StorageError::from)
            },
        )
        .await
        {
            Ok(info) => info,
            Err(Error::VolumeNotFound) => {
                let _publish_guard = self
                    .lock_metadata_publish(bucket, &namespace_guard, "missing legacy bucket metadata publish")
                    .await?;
                self.metadata_map.write().await.remove(bucket);
                self.fabricated_metadata.write().await.remove(bucket);
                self.missing_buckets.insert(bucket.to_string(), ()).await;
                return Ok(BucketMetadataAuthority::MissingBucket);
            }
            Err(err) => return Err(err),
        };

        let (mut metadata, persisted) = await_bucket_namespace_operation(
            Some(&namespace_guard),
            bucket,
            "legacy bucket metadata confirmation",
            load_bucket_metadata_parse_with_presence(self.api.clone(), bucket, true),
        )
        .await?;
        if persisted && !metadata.bucket_incarnation_sidecar && !metadata.bucket_incarnation_id.is_nil() {
            return Err(Error::other(format!(
                "bucket incarnation sidecar is missing for new-format metadata: {bucket}"
            )));
        }
        let needs_migration = !persisted || !metadata.bucket_incarnation_sidecar;
        if !persisted {
            metadata = BucketMetadata::new(bucket);
            metadata.created = bucket_info.created.unwrap_or(OffsetDateTime::UNIX_EPOCH);
        } else if metadata.bucket_incarnation_id.is_nil() {
            metadata.bucket_incarnation_id = Uuid::new_v4();
        }
        if needs_migration {
            #[cfg(test)]
            if self.legacy_migration_write_error.load(std::sync::atomic::Ordering::Relaxed) {
                return Err(Error::other("injected legacy metadata migration write failure"));
            }
            #[cfg(test)]
            if self.legacy_migration_lock_probe.load(std::sync::atomic::Ordering::Relaxed) {
                let competing = self.api.new_ns_lock(bucket, bucket).await?;
                assert!(
                    competing.get_write_lock(Duration::from_millis(20)).await.is_err(),
                    "bucket delete/recreate must not cross the legacy metadata migration fence"
                );
            }
            save_bucket_incarnation(self.api.clone(), bucket, metadata.bucket_incarnation_id).await?;
            metadata.bucket_incarnation_sidecar = true;
            if !persisted {
                await_bucket_namespace_operation(
                    Some(&namespace_guard),
                    bucket,
                    "legacy bucket metadata migration",
                    metadata.save_with_store(self.api.clone()),
                )
                .await?;
            }
        }

        if namespace_guard.is_lock_lost() {
            return Err(Error::other(format!(
                "bucket namespace lock was lost before legacy metadata publish: {bucket}"
            )));
        }
        let metadata = Arc::new(metadata);
        let _publish_guard = self
            .lock_metadata_publish(bucket, &namespace_guard, "legacy bucket metadata publish")
            .await?;
        self.metadata_map
            .write()
            .await
            .insert(bucket.to_string(), Arc::clone(&metadata));
        self.fabricated_metadata.write().await.remove(bucket);
        self.missing_buckets.invalidate(bucket).await;
        sync_bucket_target_sys(bucket, &metadata).await;
        sync_bucket_durability(bucket, &metadata);
        Ok(BucketMetadataAuthority::Authoritative(metadata))
    }

    async fn read_authoritative_metadata_from_disk_under_transaction_lock(
        &self,
        bucket: &str,
    ) -> Result<BucketMetadataAuthority> {
        #[cfg(test)]
        if self.object_lock_disk_read_errors.write().await.remove(bucket) {
            return Err(Error::other(format!("injected Object Lock metadata disk read failure: {bucket}")));
        }

        let namespace_lock = self.api.new_ns_lock(bucket, bucket).await?;
        let namespace_guard = namespace_lock
            .get_read_lock(crate::set_disk::get_lock_acquire_timeout())
            .await?;
        match await_bucket_namespace_operation(
            Some(&namespace_guard),
            bucket,
            "bucket metadata snapshot existence check",
            async {
                self.api
                    .peer_sys
                    .get_bucket_info(bucket, &crate::storage_api_contracts::bucket::BucketOptions::default())
                    .await
                    .map_err(crate::error::StorageError::from)
            },
        )
        .await
        {
            Ok(_) => {}
            Err(Error::VolumeNotFound) => return Ok(BucketMetadataAuthority::MissingBucket),
            Err(err) => return Err(err),
        }

        let (metadata, persisted) = await_bucket_namespace_operation(
            Some(&namespace_guard),
            bucket,
            "bucket metadata authoritative snapshot",
            load_bucket_metadata_parse_with_presence(self.api.clone(), bucket, true),
        )
        .await?;
        if persisted {
            Ok(BucketMetadataAuthority::Authoritative(Arc::new(metadata)))
        } else {
            Ok(BucketMetadataAuthority::Fabricated)
        }
    }

    pub(crate) async fn get_authoritative_metadata(&self, bucket: &str) -> Result<Arc<BucketMetadata>> {
        match self.get_metadata_authority(bucket).await? {
            BucketMetadataAuthority::Authoritative(bm) => Ok(bm),
            BucketMetadataAuthority::Fabricated => match self.migrate_legacy_metadata(bucket).await? {
                BucketMetadataAuthority::Authoritative(bm) => Ok(bm),
                BucketMetadataAuthority::Fabricated => {
                    Err(Error::other(format!("bucket metadata is not authoritative: {bucket}")))
                }
                BucketMetadataAuthority::MissingBucket => Err(Error::ConfigNotFound),
            },
            BucketMetadataAuthority::MissingBucket => Err(Error::ConfigNotFound),
        }
    }

    pub async fn get_lifecycle_config(&self, bucket: &str) -> Result<(BucketLifecycleConfiguration, OffsetDateTime)> {
        let (bm, _) = self.get_config(bucket).await?;

        if let Some(config) = &bm.lifecycle_config {
            if config.rules.is_empty() {
                Err(Error::ConfigNotFound)
            } else {
                Ok((config.clone(), bm.lifecycle_config_updated_at))
            }
        } else {
            Err(Error::ConfigNotFound)
        }
    }

    pub async fn get_notification_config(&self, bucket: &str) -> Result<Option<NotificationConfiguration>> {
        let bm = match self.get_config(bucket).await {
            Ok((bm, _)) => bm.notification_config.clone(),
            Err(err) => {
                if err == Error::ConfigNotFound {
                    None
                } else {
                    return Err(err);
                }
            }
        };

        Ok(bm)
    }

    pub async fn get_sse_config(&self, bucket: &str) -> Result<(ServerSideEncryptionConfiguration, OffsetDateTime)> {
        let (bm, _) = self.get_config(bucket).await?;

        if let Some(config) = &bm.sse_config {
            Ok((config.clone(), bm.encryption_config_updated_at))
        } else {
            Err(Error::ConfigNotFound)
        }
    }

    pub async fn get_cors_config(&self, bucket: &str) -> Result<(CORSConfiguration, OffsetDateTime)> {
        let (bm, _) = self.get_config(bucket).await?;

        if let Some(config) = &bm.cors_config {
            Ok((config.clone(), bm.cors_config_updated_at))
        } else {
            Err(Error::ConfigNotFound)
        }
    }

    pub async fn get_website_config(&self, bucket: &str) -> Result<(WebsiteConfiguration, OffsetDateTime)> {
        let (bm, _) = self.get_config(bucket).await?;

        if let Some(config) = &bm.website_config {
            Ok((config.clone(), bm.website_config_updated_at))
        } else {
            Err(Error::ConfigNotFound)
        }
    }

    pub async fn get_logging_config(&self, bucket: &str) -> Result<(BucketLoggingStatus, OffsetDateTime)> {
        let (bm, _) = self.get_config(bucket).await?;

        if let Some(config) = &bm.logging_config {
            Ok((config.clone(), bm.logging_config_updated_at))
        } else {
            Err(Error::ConfigNotFound)
        }
    }

    pub async fn get_accelerate_config(&self, bucket: &str) -> Result<(AccelerateConfiguration, OffsetDateTime)> {
        let (bm, _) = self.get_config(bucket).await?;

        if let Some(config) = &bm.accelerate_config {
            Ok((config.clone(), bm.accelerate_config_updated_at))
        } else {
            Err(Error::ConfigNotFound)
        }
    }

    pub async fn get_request_payment_config(&self, bucket: &str) -> Result<(RequestPaymentConfiguration, OffsetDateTime)> {
        let (bm, _) = self.get_config(bucket).await?;

        if let Some(config) = &bm.request_payment_config {
            Ok((config.clone(), bm.request_payment_config_updated_at))
        } else {
            Err(Error::ConfigNotFound)
        }
    }

    pub async fn created_at(&self, bucket: &str) -> Result<OffsetDateTime> {
        let bm = match self.get_config(bucket).await {
            Ok((bm, _)) => bm.created,
            Err(err) => {
                return Err(err);
            }
        };

        Ok(bm)
    }

    pub async fn get_quota_config(&self, bucket: &str) -> Result<(BucketQuota, OffsetDateTime)> {
        let (bm, _) = self.get_config(bucket).await?;

        if let Some(config) = &bm.quota_config {
            Ok((config.clone(), bm.quota_config_updated_at))
        } else {
            Err(Error::ConfigNotFound)
        }
    }

    pub async fn get_replication_config(&self, bucket: &str) -> Result<(ReplicationConfiguration, OffsetDateTime)> {
        let (bm, _) = self.get_config(bucket).await?;

        if !bm.replication_config_xml.is_empty() && bm.replication_config.is_none() {
            Err(Error::other("persisted bucket replication configuration is invalid"))
        } else if let Some(config) = &bm.replication_config {
            Ok((config.clone(), bm.replication_config_updated_at))
        } else {
            Err(Error::ConfigNotFound)
        }
    }

    pub async fn get_bucket_targets_config(&self, bucket: &str) -> Result<BucketTargets> {
        let (bm, _) = self.get_config(bucket).await?;

        if let Some(config) = &bm.bucket_target_config {
            Ok(config.clone())
        } else {
            Err(Error::ConfigNotFound)
        }
    }
}

/// Test-only fixture shared with sibling modules (e.g. the quota checker
/// tests): a 4-disk `ECStore` on an isolated instance context, so tests
/// exercising the metadata system never touch ambient process state.
#[cfg(test)]
pub(crate) mod test_support {
    use super::*;
    use crate::disk::endpoint::Endpoint;
    use crate::layout::endpoints::{EndpointServerPools, Endpoints, PoolEndpoints};
    use crate::runtime::instance::InstanceContext;
    use crate::store::init_local_disks_with_instance_ctx;

    pub(crate) async fn isolated_store_over_temp_disks() -> (Vec<tempfile::TempDir>, Arc<ECStore>) {
        let mut dirs = Vec::with_capacity(4);
        let mut endpoints = Vec::with_capacity(4);
        for disk_idx in 0..4 {
            let dir = tempfile::tempdir().expect("tempdir should be created");
            let mut endpoint =
                Endpoint::try_from(dir.path().to_str().expect("tempdir path should be utf8")).expect("endpoint should parse");
            endpoint.set_pool_index(0);
            endpoint.set_set_index(0);
            endpoint.set_disk_index(disk_idx);
            dirs.push(dir);
            endpoints.push(endpoint);
        }
        let endpoint_pools = EndpointServerPools(vec![PoolEndpoints {
            legacy: false,
            set_count: 1,
            drives_per_set: 4,
            endpoints: Endpoints::from(endpoints),
            cmd_line: "metadata-sys-cache-test".to_string(),
            platform: "test".to_string(),
        }]);
        let instance_ctx = Arc::new(InstanceContext::new());
        init_local_disks_with_instance_ctx(&instance_ctx, endpoint_pools.clone())
            .await
            .expect("local disks should initialize");
        let ecstore = ECStore::new_with_instance_ctx(
            "127.0.0.1:0".parse().expect("test address"),
            endpoint_pools,
            CancellationToken::new(),
            instance_ctx,
        )
        .await
        .expect("ECStore should initialize");
        (dirs, ecstore)
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::isolated_store_over_temp_disks;
    use super::*;
    use crate::bucket::target::{BucketTarget, BucketTargetType, Credentials};
    use crate::storage_api_contracts::bucket::{BucketOperations as _, DeleteBucketOptions, MakeBucketOptions};
    use serial_test::serial;
    use tokio::time::timeout;

    #[tokio::test]
    async fn malformed_delete_configs_are_not_treated_as_absent() {
        let (_dirs, ecstore) = isolated_store_over_temp_disks().await;
        let sys = BucketMetadataSys::new(ecstore);
        let bucket = "malformed-delete-config";
        let mut metadata = BucketMetadata::new(bucket);
        metadata.versioning_config_xml = b"<VersioningConfiguration>".to_vec();
        metadata.versioning_config = None;
        metadata.replication_config_xml = b"<ReplicationConfiguration>".to_vec();
        metadata.replication_config = None;
        metadata.object_lock_config_xml = b"<ObjectLockConfiguration>".to_vec();
        metadata.object_lock_config = None;
        sys.set(bucket.to_string(), Arc::new(metadata)).await;

        assert!(
            sys.get_versioning_config(bucket).await.is_err(),
            "malformed versioning metadata must block destructive requests"
        );
        assert!(
            sys.get_replication_config(bucket).await.is_err(),
            "malformed replication metadata must not be reported as ConfigNotFound"
        );
        assert!(
            sys.get_object_lock_config_state(bucket).await.is_err(),
            "malformed Object Lock metadata must not be reported as absent"
        );
    }

    #[tokio::test]
    async fn config_states_distinguish_authoritative_absence_from_fabricated_metadata() {
        use std::sync::atomic::Ordering;

        let (dirs, ecstore) = isolated_store_over_temp_disks().await;
        let sys = BucketMetadataSys::new(ecstore);

        sys.set("authoritative-empty".to_string(), Arc::new(BucketMetadata::new("authoritative-empty")))
            .await;
        assert!(matches!(
            sys.get_object_lock_config_state("authoritative-empty").await.unwrap(),
            ObjectLockConfigState::ConfirmedAbsent
        ));
        assert!(matches!(sys.get_bucket_policy("authoritative-empty").await, Err(Error::ConfigNotFound)));

        for dir in &dirs {
            std::fs::create_dir_all(dir.path().join("policy-only-legacy")).unwrap();
        }
        assert!(matches!(sys.get_bucket_policy("policy-only-legacy").await, Err(Error::ConfigNotFound)));
        assert!(matches!(
            sys.get_bucket_policy_raw("policy-only-legacy").await,
            Err(Error::ConfigNotFound)
        ));

        for dir in &dirs {
            std::fs::create_dir_all(dir.path().join("raw-policy-only-legacy")).unwrap();
        }
        assert!(!matches!(
            sys.get_bucket_policy_raw("raw-policy-only-legacy").await,
            Err(Error::ConfigNotFound)
        ));

        for dir in &dirs {
            std::fs::create_dir_all(dir.path().join("fabricated")).unwrap();
        }
        let loads_before = sys.lazy_disk_loads.load(Ordering::Relaxed);
        sys.legacy_migration_lock_probe.store(true, Ordering::Relaxed);
        assert!(matches!(
            sys.get_object_lock_config_state("fabricated").await.unwrap(),
            ObjectLockConfigState::ConfirmedAbsent
        ));
        sys.legacy_migration_lock_probe.store(false, Ordering::Relaxed);
        assert!(matches!(sys.get_bucket_policy("fabricated").await, Err(Error::ConfigNotFound)));
        assert!(matches!(sys.get_bucket_policy_raw("fabricated").await, Err(Error::ConfigNotFound)));
        assert!(matches!(
            sys.get_object_lock_config_state("fabricated").await.unwrap(),
            ObjectLockConfigState::ConfirmedAbsent
        ));
        assert_eq!(
            sys.lazy_disk_loads.load(Ordering::Relaxed),
            loads_before + 1,
            "legacy metadata migration must not repeat the initial erasure read"
        );
        assert!(
            !sys.get("fabricated").await.unwrap().bucket_incarnation_id.is_nil(),
            "legacy metadata absence migration must persist a bucket incarnation"
        );

        for dir in &dirs {
            std::fs::create_dir_all(dir.path().join("migration-write-failure")).unwrap();
        }
        sys.legacy_migration_write_error.store(true, Ordering::Relaxed);
        assert!(!matches!(
            sys.get_bucket_policy_raw("migration-write-failure").await,
            Err(Error::ConfigNotFound)
        ));
        assert!(!matches!(
            sys.get_bucket_policy("migration-write-failure").await,
            Err(Error::ConfigNotFound)
        ));
        assert!(sys.get_object_lock_config_state("migration-write-failure").await.is_err());
        assert!(sys.get("migration-write-failure").await.is_err());
        sys.legacy_migration_write_error.store(false, Ordering::Relaxed);

        assert!(matches!(
            sys.get_object_lock_config_state("missing-bucket").await,
            Err(Error::ConfigNotFound)
        ));
        assert!(matches!(sys.get_bucket_policy("missing-bucket").await, Err(Error::ConfigNotFound)));
        assert!(matches!(sys.get_bucket_policy_raw("missing-bucket").await, Err(Error::ConfigNotFound)));

        let policy_json = br#"{
            "Version":"2012-10-17",
            "Statement":[{
                "Effect":"Allow",
                "Principal":{"AWS":"*"},
                "Action":["s3:GetObject"],
                "Resource":["arn:aws:s3:::configured-policy/*"]
            }]
        }"#;
        let mut configured_policy = BucketMetadata::new("configured-policy");
        configured_policy.policy_config_json = policy_json.to_vec();
        sys.set("configured-policy".to_string(), Arc::new(configured_policy)).await;
        sys.get_bucket_policy("configured-policy")
            .await
            .expect("configured bucket policy JSON should parse");

        let mut legacy = BucketMetadata::new("legacy-lock-enabled");
        legacy.lock_enabled = true;
        sys.set("legacy-lock-enabled".to_string(), Arc::new(legacy)).await;
        let ObjectLockConfigState::Configured { config, .. } =
            sys.get_object_lock_config_state("legacy-lock-enabled").await.unwrap()
        else {
            panic!("legacy lock-enabled metadata must remain configured");
        };
        assert_eq!(
            config.object_lock_enabled.as_ref().map(|value| value.as_str()),
            Some(ObjectLockEnabled::ENABLED)
        );
        assert!(config.rule.is_none());
    }

    #[test]
    fn authoritative_object_lock_state_rejects_invalid_retention_periods() {
        use s3s::dto::{DefaultRetention, ObjectLockRule};

        for (days, years) in [(Some(0), None), (Some(-1), None), (Some(1), Some(1)), (None, None)] {
            let mut metadata = BucketMetadata::new("invalid-object-lock");
            metadata.object_lock_config = Some(ObjectLockConfiguration {
                object_lock_enabled: Some(ObjectLockEnabled::from_static(ObjectLockEnabled::ENABLED)),
                rule: Some(ObjectLockRule {
                    default_retention: Some(DefaultRetention {
                        days,
                        mode: Some(ObjectLockRetentionMode::from_static(ObjectLockRetentionMode::COMPLIANCE)),
                        years,
                    }),
                }),
            });

            assert!(
                object_lock_config_state_from_authoritative_metadata(&metadata).is_err(),
                "invalid retention days={days:?} years={years:?} must fail closed"
            );
        }
    }

    #[test]
    fn authoritative_object_lock_state_rejects_invalid_enabled_mode_and_rule() {
        use s3s::dto::{DefaultRetention, ObjectLockRule};

        for enabled in [None, Some(ObjectLockEnabled::from("Disabled".to_string()))] {
            let mut metadata = BucketMetadata::new("invalid-object-lock-enabled");
            metadata.lock_enabled = true;
            metadata.object_lock_config = Some(ObjectLockConfiguration {
                object_lock_enabled: enabled,
                rule: None,
            });
            assert!(object_lock_config_state_from_authoritative_metadata(&metadata).is_err());
        }

        let mut invalid_mode = BucketMetadata::new("invalid-object-lock-mode");
        invalid_mode.object_lock_config = Some(ObjectLockConfiguration {
            object_lock_enabled: Some(ObjectLockEnabled::from_static(ObjectLockEnabled::ENABLED)),
            rule: Some(ObjectLockRule {
                default_retention: Some(DefaultRetention {
                    days: Some(1),
                    mode: Some(ObjectLockRetentionMode::from("invalid".to_string())),
                    years: None,
                }),
            }),
        });
        assert!(object_lock_config_state_from_authoritative_metadata(&invalid_mode).is_err());

        let mut missing_retention = BucketMetadata::new("missing-object-lock-retention");
        missing_retention.object_lock_config = Some(ObjectLockConfiguration {
            object_lock_enabled: Some(ObjectLockEnabled::from_static(ObjectLockEnabled::ENABLED)),
            rule: Some(ObjectLockRule { default_retention: None }),
        });
        assert!(object_lock_config_state_from_authoritative_metadata(&missing_retention).is_err());
    }

    #[test]
    fn object_lock_ever_enabled_marker_survives_config_removal() {
        let mut metadata = BucketMetadata::new("lock-config-removed");
        let config = ObjectLockConfiguration {
            object_lock_enabled: Some(ObjectLockEnabled::from_static(ObjectLockEnabled::ENABLED)),
            rule: None,
        };
        metadata
            .update_config(
                crate::bucket::metadata::OBJECT_LOCK_CONFIG,
                crate::bucket::utils::serialize(&config).unwrap(),
            )
            .unwrap();
        metadata
            .update_config(crate::bucket::metadata::OBJECT_LOCK_CONFIG, Vec::new())
            .unwrap();

        let ObjectLockConfigState::Configured { config, .. } =
            object_lock_config_state_from_authoritative_metadata(&metadata).unwrap()
        else {
            panic!("an ever-enabled bucket must remain Object Lock configured");
        };
        assert_eq!(
            config.object_lock_enabled.as_ref().map(ObjectLockEnabled::as_str),
            Some(ObjectLockEnabled::ENABLED)
        );
    }

    #[tokio::test]
    async fn site_replication_empty_object_lock_update_preserves_ever_enabled_state_after_reload() {
        use crate::bucket::metadata::OBJECT_LOCK_CONFIG;
        use crate::bucket::object_lock::objectlock_sys::ensure_recursive_force_delete_allowed_for_state;

        let (dirs, ecstore) = isolated_store_over_temp_disks().await;
        let bucket = "site-repl-lock-removal";
        for dir in &dirs {
            std::fs::create_dir_all(dir.path().join(bucket)).unwrap();
        }

        let node = Arc::new(RwLock::new(BucketMetadataSys::new(ecstore.clone())));
        let config = ObjectLockConfiguration {
            object_lock_enabled: Some(ObjectLockEnabled::from_static(ObjectLockEnabled::ENABLED)),
            rule: None,
        };
        update_with_sys(
            Arc::clone(&node),
            bucket,
            OBJECT_LOCK_CONFIG,
            crate::bucket::utils::serialize(&config).unwrap(),
        )
        .await
        .expect("initial Object Lock config should persist");

        // Site replication represents an incoming `object_lock_config: None`
        // as deletion of the config payload. That must not clear the durable
        // ever-enabled marker.
        delete_with_sys(Arc::clone(&node), bucket, OBJECT_LOCK_CONFIG)
            .await
            .expect("empty site-replication update should persist");

        let restarted = BucketMetadataSys::new(ecstore);
        restarted
            .reload_from_store(bucket)
            .await
            .expect("persisted metadata should reload after restart");
        let state = restarted
            .get_object_lock_config_state(bucket)
            .await
            .expect("ever-enabled state should remain authoritative");
        assert!(matches!(state, ObjectLockConfigState::Configured { .. }));
        assert!(
            ensure_recursive_force_delete_allowed_for_state(bucket, &state).is_err(),
            "force delete must remain forbidden after config removal and reload"
        );
    }

    #[tokio::test]
    async fn legacy_nil_bucket_incarnation_is_persisted_once_and_fail_closed_on_write_error() {
        use std::sync::atomic::Ordering;

        let (dirs, ecstore) = isolated_store_over_temp_disks().await;
        let sys = BucketMetadataSys::new(ecstore);

        for bucket in ["legacy-nil-incarnation", "legacy-nil-incarnation-write-failure"] {
            for dir in &dirs {
                std::fs::create_dir_all(dir.path().join(bucket)).unwrap();
            }
            let mut metadata = BucketMetadata::new(bucket);
            metadata.bucket_incarnation_id = Uuid::nil();
            sys.persist_and_set(metadata).await.unwrap();
        }

        sys.legacy_migration_lock_probe.store(true, Ordering::Relaxed);
        let migrated = sys
            .get_bucket_incarnation_id("legacy-nil-incarnation")
            .await
            .expect("legacy nil incarnation should migrate under the namespace fence");
        sys.legacy_migration_lock_probe.store(false, Ordering::Relaxed);
        assert!(!migrated.is_nil());
        let persisted = sys.get_config_from_disk("legacy-nil-incarnation").await.unwrap();
        assert_eq!(persisted.bucket_incarnation_id, migrated);
        assert!(persisted.bucket_incarnation_sidecar);
        assert_eq!(
            sys.get_bucket_incarnation_id("legacy-nil-incarnation").await.unwrap(),
            migrated,
            "the durable incarnation must remain stable after migration"
        );

        sys.legacy_migration_write_error.store(true, Ordering::Relaxed);
        assert!(
            sys.get_bucket_incarnation_id("legacy-nil-incarnation-write-failure")
                .await
                .is_err(),
            "a failed incarnation migration must fail closed"
        );
        sys.legacy_migration_write_error.store(false, Ordering::Relaxed);
        assert!(
            sys.get_config_from_disk("legacy-nil-incarnation-write-failure")
                .await
                .unwrap()
                .bucket_incarnation_id
                .is_nil(),
            "a failed migration must not fabricate authority in memory or on disk"
        );
    }

    #[tokio::test]
    async fn old_node_metadata_rewrite_cannot_replace_bucket_incarnation_sidecar() {
        let (dirs, ecstore) = isolated_store_over_temp_disks().await;
        let sys = BucketMetadataSys::new(ecstore.clone());
        let bucket = "old-node-incarnation-rewrite";
        for dir in &dirs {
            std::fs::create_dir_all(dir.path().join(bucket)).unwrap();
        }

        let metadata = BucketMetadata::new(bucket);
        let incarnation = metadata.bucket_incarnation_id;
        sys.persist_new_and_set(metadata).await.unwrap();

        let mut old_node_rewrite = sys.get_config_from_disk(bucket).await.unwrap();
        old_node_rewrite.bucket_incarnation_id = Uuid::nil();
        old_node_rewrite.save_with_store(ecstore.clone()).await.unwrap();
        sys.metadata_map.write().await.clear();

        assert_eq!(
            sys.get_bucket_incarnation_id_from_disk(bucket).await.unwrap(),
            incarnation,
            "rewriting only .metadata.bin must not replace sidecar authority"
        );

        let mut mismatched_rewrite = sys.get_config_from_disk(bucket).await.unwrap();
        mismatched_rewrite.bucket_incarnation_id = Uuid::new_v4();
        mismatched_rewrite.save_with_store(ecstore).await.unwrap();
        let err = sys
            .get_config_from_disk(bucket)
            .await
            .expect_err("a valid but mismatched embedded incarnation must fail closed");
        assert!(err.to_string().contains("does not match"));
    }

    #[tokio::test]
    async fn corrupt_bucket_incarnation_sidecar_fails_closed() {
        let (dirs, ecstore) = isolated_store_over_temp_disks().await;
        let sys = BucketMetadataSys::new(ecstore.clone());
        let bucket = "corrupt-incarnation-sidecar";
        for dir in &dirs {
            std::fs::create_dir_all(dir.path().join(bucket)).unwrap();
        }

        let metadata = BucketMetadata::new(bucket);
        sys.persist_new_and_set(metadata).await.unwrap();
        crate::config::com::save_config(
            ecstore.clone(),
            &format!(
                "{}/{bucket}/{}",
                crate::disk::BUCKET_META_PREFIX,
                crate::bucket::metadata::BUCKET_INCARNATION_FILE
            ),
            vec![0_u8; 15],
        )
        .await
        .unwrap();
        sys.metadata_map.write().await.clear();

        let err = sys
            .get_bucket_incarnation_id_from_disk(bucket)
            .await
            .expect_err("corrupt sidecar must never fall back to the embedded msgpack UUID");
        assert!(err.to_string().contains("persisted bucket incarnation is invalid"));

        crate::config::com::save_config(
            ecstore,
            &format!(
                "{}/{bucket}/{}",
                crate::disk::BUCKET_META_PREFIX,
                crate::bucket::metadata::BUCKET_INCARNATION_FILE
            ),
            Uuid::nil().as_bytes().to_vec(),
        )
        .await
        .unwrap();
        let err = sys
            .get_bucket_incarnation_id_from_disk(bucket)
            .await
            .expect_err("a nil sidecar must fail closed");
        assert!(err.to_string().contains("persisted bucket incarnation is nil"));
    }

    #[tokio::test]
    async fn missing_bucket_incarnation_sidecar_for_new_metadata_fails_closed() {
        let (dirs, ecstore) = isolated_store_over_temp_disks().await;
        let sys = BucketMetadataSys::new(ecstore.clone());
        let bucket = "missing-incarnation-sidecar";
        for dir in &dirs {
            std::fs::create_dir_all(dir.path().join(bucket)).unwrap();
        }

        let metadata = BucketMetadata::new(bucket);
        sys.persist_new_and_set(metadata).await.unwrap();
        crate::config::com::delete_config(
            ecstore,
            &format!(
                "{}/{bucket}/{}",
                crate::disk::BUCKET_META_PREFIX,
                crate::bucket::metadata::BUCKET_INCARNATION_FILE
            ),
        )
        .await
        .unwrap();
        sys.metadata_map.write().await.clear();

        let err = sys
            .get_object_lock_config_state(bucket)
            .await
            .expect_err("new-format metadata without its sidecar must fail closed");
        assert!(err.to_string().contains("sidecar is missing"));
    }

    /// Concurrent cache misses for one bucket must collapse into a single disk
    /// load.
    ///
    /// The namespace read lock the lazy path already holds does not provide
    /// this: read locks are shared, so it excludes concurrent config writers
    /// but not concurrent readers. Without the dedup gate every caller pays its
    /// own namespace-lock acquisition plus a full erasure-set metadata fanout —
    /// and the paths that reach `get_config` are per-request, so the multiplier
    /// is request concurrency.
    #[tokio::test]
    async fn concurrent_lazy_loads_of_one_bucket_issue_a_single_disk_read() {
        use std::sync::atomic::Ordering;

        let (_dirs, ecstore) = isolated_store_over_temp_disks().await;
        let sys = Arc::new(BucketMetadataSys::new(ecstore));

        // A name with no persisted metadata: every caller misses the map, and
        // the absent-cache entry does not exist until the first load records it.
        let bucket = "singleflight-bucket";
        let waiters = 8;

        let results = futures::future::join_all((0..waiters).map(|_| {
            let sys = Arc::clone(&sys);
            async move { sys.get_config(bucket).await.map(|(bm, _)| bm.name.clone()) }
        }))
        .await;

        for result in results {
            assert_eq!(result.expect("every caller must get an answer"), bucket);
        }
        assert_eq!(
            sys.lazy_disk_loads.load(Ordering::Relaxed),
            1,
            "concurrent misses for one bucket must share a single disk load"
        );
    }

    /// Pins the fail-closed caching contract of the lazy `get_config` path
    /// and refresh authority invalidation: fabricated defaults are never
    /// served by map-only `get()`, persisted metadata supersedes absence, and
    /// refresh never heals a deleted bucket or preserves stale authority.
    #[tokio::test]
    async fn get_config_never_caches_fabricated_defaults_as_authoritative() {
        let (dirs, ecstore) = isolated_store_over_temp_disks().await;
        let sys = Arc::new(BucketMetadataSys::new(ecstore));

        // (a) Miss: the fabricated default is returned but not cached.
        let (bm, _) = sys
            .get_config("absent-bucket")
            .await
            .expect("fabricated default should be returned");
        assert!(bm.object_lock_config_xml.is_empty());
        assert!(
            sys.get("absent-bucket").await.is_err(),
            "a fabricated default must never be served by the map-only get()"
        );

        // The repeat lookup is served from the negative cache, same answer.
        let (bm, _) = sys
            .get_config("absent-bucket")
            .await
            .expect("negative-cached default should be returned");
        assert!(bm.object_lock_config_xml.is_empty());
        assert!(sys.get("absent-bucket").await.is_err());

        // (b) Persisting real metadata supersedes the recorded absence, and a
        // lazy reload after a map wipe re-caches it.
        for dir in &dirs {
            std::fs::create_dir_all(dir.path().join("absent-bucket")).expect("persisted bucket directory should be created");
        }
        let mut persisted = BucketMetadata::new("absent-bucket");
        persisted.policy_config_json = b"persisted-marker".to_vec();
        sys.persist_new_and_set(persisted).await.expect("metadata should persist");
        sys.metadata_map.write().await.clear();
        let _ = sys
            .get_config("absent-bucket")
            .await
            .expect("persisted metadata should lazily reload");
        let cached = sys
            .get("absent-bucket")
            .await
            .expect("lazily loaded persisted metadata must be cached");
        assert_eq!(cached.policy_config_json, b"persisted-marker".to_vec());
        sys.metadata_map.write().await.clear();
        sys.reload_from_store("absent-bucket")
            .await
            .expect("peer reload should publish persisted metadata into a cold cache");
        assert_eq!(sys.get("absent-bucket").await.unwrap().policy_config_json, b"persisted-marker".to_vec());

        // (c) Persisted metadata left behind after physical deletion must not
        // be lazily republished as a live bucket generation.
        let mut deleted_lazy = BucketMetadata::new("deleted-lazy-bucket");
        deleted_lazy.policy_config_json = b"stale-generation".to_vec();
        sys.persist_and_set(deleted_lazy)
            .await
            .expect("stale metadata should persist");
        sys.metadata_map.write().await.remove("deleted-lazy-bucket");
        assert!(
            sys.get_config("deleted-lazy-bucket").await.is_err(),
            "lazy load must fail when the physical bucket no longer exists"
        );
        assert!(sys.get("deleted-lazy-bucket").await.is_err());

        // (d) The namespace generation fence must be acquired before lazy
        // metadata IO, so a writer can replace the generation atomically.
        let fenced_bucket = "fenced-lazy-bucket";
        for dir in &dirs {
            std::fs::create_dir_all(dir.path().join(fenced_bucket)).unwrap();
        }
        let mut old_fenced = BucketMetadata::new(fenced_bucket);
        old_fenced.policy_config_json = b"old-fenced-generation".to_vec();
        sys.persist_new_and_set(old_fenced).await.unwrap();
        sys.metadata_map.write().await.remove(fenced_bucket);
        sys.lazy_load_lock_probe.store(true, std::sync::atomic::Ordering::Relaxed);
        let (loaded, _) = sys.get_config(fenced_bucket).await.unwrap();
        sys.lazy_load_lock_probe.store(false, std::sync::atomic::Ordering::Relaxed);
        assert_eq!(loaded.policy_config_json, b"old-fenced-generation".to_vec());

        // (e) A refresh-load miss invalidates stale authority without caching
        // a fabricated default in the authoritative map. A later real publish
        // restores authority.
        let mut kept = BucketMetadata::new("kept-bucket");
        kept.policy_config_json = b"kept-marker".to_vec();
        sys.set("kept-bucket".to_string(), Arc::new(kept)).await;
        for dir in &dirs {
            std::fs::create_dir_all(dir.path().join("kept-bucket")).expect("kept bucket directory should be created");
        }
        let mut failed = HashSet::new();
        let refresh_targets = vec!["kept-bucket".to_string()];
        sys.concurrent_load(&refresh_targets, &mut failed, MetadataLoadMode::Refresh)
            .await;
        assert!(sys.get("kept-bucket").await.is_err());
        assert!(matches!(
            sys.get_object_lock_config_state("kept-bucket").await.unwrap(),
            ObjectLockConfigState::ConfirmedAbsent
        ));
        let restored = BucketMetadata::new("kept-bucket");
        sys.set("kept-bucket".to_string(), Arc::new(restored)).await;
        assert!(matches!(
            sys.get_object_lock_config_state("kept-bucket").await.unwrap(),
            ObjectLockConfigState::ConfirmedAbsent
        ));

        // (f) A stale cache entry for a physically deleted bucket must be
        // removed without recreating the bucket during periodic refresh.
        sys.set("deleted-bucket".to_string(), Arc::new(BucketMetadata::new("deleted-bucket")))
            .await;
        let deleted_targets = vec!["deleted-bucket".to_string()];
        sys.concurrent_load(&deleted_targets, &mut failed, MetadataLoadMode::Refresh)
            .await;
        assert!(
            dirs.iter().all(|dir| !dir.path().join("deleted-bucket").exists()),
            "periodic refresh must not recreate a bucket from stale cached metadata"
        );
        assert!(
            sys.get("deleted-bucket").await.is_err(),
            "periodic refresh must remove stale cached metadata"
        );

        // (g) Persisted metadata left behind after physical deletion must not
        // keep the deleted generation authoritative during refresh.
        let mut deleted_persisted = BucketMetadata::new("deleted-persisted-bucket");
        deleted_persisted.policy_config_json = b"stale-persisted-generation".to_vec();
        sys.persist_and_set(deleted_persisted)
            .await
            .expect("stale metadata should persist");
        sys.concurrent_load(&["deleted-persisted-bucket".to_string()], &mut failed, MetadataLoadMode::Refresh)
            .await;
        assert!(
            sys.get("deleted-persisted-bucket").await.is_err(),
            "refresh must remove persisted metadata for a physically absent bucket"
        );
        assert!(
            sys.reload_from_store("deleted-persisted-bucket").await.is_err(),
            "peer reload must not publish stale metadata for an absent bucket"
        );

        // (h) Metadata loaded for an old bucket generation must not replace
        // metadata published by delete plus same-name recreation.
        let old = Arc::new(BucketMetadata::new("recreated-bucket"));
        sys.set("recreated-bucket".to_string(), Arc::clone(&old)).await;
        let mut recreated = BucketMetadata::new("recreated-bucket");
        recreated.policy_config_json = b"new-generation".to_vec();
        sys.set("recreated-bucket".to_string(), Arc::new(recreated)).await;
        let mut stale = BucketMetadata::new("recreated-bucket");
        stale.policy_config_json = b"old-generation".to_vec();
        let namespace_lock = sys
            .api
            .new_ns_lock("recreated-bucket", "recreated-bucket")
            .await
            .expect("namespace lock should be created");
        let namespace_guard = namespace_lock
            .get_read_lock(crate::set_disk::get_lock_acquire_timeout())
            .await
            .expect("namespace read lock should be acquired");
        sys.publish_if_unchanged("recreated-bucket", Some(&old), stale, true, &namespace_guard)
            .await
            .expect("stale refresh publish should be fenced");
        assert_eq!(
            sys.get("recreated-bucket")
                .await
                .expect("recreated bucket metadata should remain cached")
                .policy_config_json,
            b"new-generation".to_vec()
        );

        // (i) Refresh retains periodic healing for a partially missing bucket.
        sys.set("partial-bucket".to_string(), Arc::new(BucketMetadata::new("partial-bucket")))
            .await;
        for dir in dirs.iter().take(3) {
            std::fs::create_dir_all(dir.path().join("partial-bucket")).unwrap();
        }
        sys.concurrent_load(&["partial-bucket".to_string()], &mut failed, MetadataLoadMode::Refresh)
            .await;
        assert!(dirs.iter().all(|dir| dir.path().join("partial-bucket").is_dir()));

        // (j) A stale initial snapshot must not recreate a bucket that has
        // disappeared from every disk.
        let stale_initial_targets = vec!["deleted-initial-bucket".to_string()];
        sys.concurrent_load(&stale_initial_targets, &mut failed, MetadataLoadMode::Initial)
            .await;
        assert!(
            dirs.iter().all(|dir| !dir.path().join("deleted-initial-bucket").exists()),
            "initial load must not recreate a bucket absent from every disk"
        );

        // (k) Initial discovery still heals a bucket present on part of the
        // storage topology.
        for dir in dirs.iter().take(3) {
            std::fs::create_dir_all(dir.path().join("initial-bucket"))
                .expect("partial initial bucket directory should be created");
        }
        let initial_targets = vec!["initial-bucket".to_string()];
        sys.concurrent_load(&initial_targets, &mut failed, MetadataLoadMode::Initial)
            .await;
        assert!(
            dirs.iter().all(|dir| dir.path().join("initial-bucket").is_dir()),
            "initial load must heal buckets discovered from storage"
        );
        assert!(
            sys.get("initial-bucket").await.is_err(),
            "initial discovery must not cache fabricated metadata as authoritative"
        );
        assert!(matches!(
            sys.get_object_lock_config_state("initial-bucket").await.unwrap(),
            ObjectLockConfigState::ConfirmedAbsent
        ));
        assert!(sys.get("initial-bucket").await.is_ok());
    }

    #[tokio::test]
    async fn metadata_publish_locks_are_isolated_per_bucket() {
        let (_dirs, ecstore) = isolated_store_over_temp_disks().await;
        let sys = Arc::new(BucketMetadataSys::new(ecstore));
        let first_lock = sys.metadata_publish_lock("blocked-bucket");
        let same_lock = sys.metadata_publish_lock("blocked-bucket");
        assert!(Arc::ptr_eq(&first_lock, &same_lock));
        let first_guard = first_lock.lock().await;
        let cancelled_waiter_lock = sys.metadata_publish_lock("blocked-bucket");
        let cancelled_waiter = tokio::spawn(async move {
            let _guard = cancelled_waiter_lock.lock_owned().await;
        });
        tokio::task::yield_now().await;
        cancelled_waiter.abort();
        assert!(cancelled_waiter.await.unwrap_err().is_cancelled());
        let other_bucket = "other-bucket".to_string();
        let other_lock = sys.metadata_publish_lock(&other_bucket);
        assert!(!Arc::ptr_eq(&first_lock, &other_lock));

        timeout(
            Duration::from_secs(1),
            sys.set(other_bucket.clone(), Arc::new(BucketMetadata::new(&other_bucket))),
        )
        .await
        .expect("one bucket publish lock must not block another bucket");
        assert!(sys.get(&other_bucket).await.is_ok());

        drop(first_guard);
        drop(first_lock);
        drop(same_lock);
        drop(other_lock);
        assert!(
            sys.metadata_publish_locks
                .locks
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .is_empty()
        );
    }

    #[tokio::test]
    async fn get_bucket_policy_rejects_malformed_cached_policy() {
        let (_dirs, ecstore) = isolated_store_over_temp_disks().await;
        let sys = BucketMetadataSys::new(ecstore);
        let mut metadata = BucketMetadata::new("malformed-policy");
        metadata.policy_config_json = b"{".to_vec();
        sys.set("malformed-policy".to_string(), Arc::new(metadata)).await;

        let err = sys
            .get_bucket_policy("malformed-policy")
            .await
            .expect_err("malformed persisted policy must not be treated as missing");

        assert!(matches!(err, Error::Io(_)), "malformed persisted policy must surface its parse failure");
    }
    /// A tagging rewrite through `update_config_with` (the Swift metadata
    /// POST path) is persisted: it survives a metadata reload from disk, and
    /// an emptied rewrite clears the config in the cached copy too instead of
    /// leaving stale parsed tags behind.
    #[tokio::test]
    async fn update_config_with_persists_tagging_rewrite_across_disk_reload() {
        use crate::bucket::metadata::BUCKET_TAGGING_CONFIG;
        use s3s::dto::Tag;

        let (dirs, ecstore) = isolated_store_over_temp_disks().await;

        let bucket = "swift-tagging-bucket";
        for dir in &dirs {
            std::fs::create_dir_all(dir.path().join(bucket)).expect("bucket volume should be created");
        }
        let sys = BucketMetadataSys::new(ecstore);
        sys.persist_new_and_set(BucketMetadata::new(bucket))
            .await
            .expect("initial metadata should persist");

        let tagging = Tagging {
            tag_set: vec![Tag {
                key: Some("swift-meta-color".to_string()),
                value: Some("blue".to_string()),
            }],
        };
        let xml = crate::bucket::utils::serialize::<Tagging>(&tagging).expect("tagging should serialize");
        sys.update_config_with(bucket, BUCKET_TAGGING_CONFIG, move |bm| {
            assert!(bm.tagging_config.is_none(), "rewrite must see the on-disk state");
            Ok(xml)
        })
        .await
        .expect("tagging rewrite should persist");

        // Simulate the disk-truth reload that used to lose Swift writes: drop
        // the cached entry and lazily re-load from the metadata file.
        sys.metadata_map.write().await.clear();
        let (tags, _) = sys
            .get_tagging_config(bucket)
            .await
            .expect("tagging must survive a reload from disk");
        assert_eq!(tags.tag_set.len(), 1);
        assert_eq!(tags.tag_set[0].key.as_deref(), Some("swift-meta-color"));
        assert_eq!(tags.tag_set[0].value.as_deref(), Some("blue"));

        // An emptied rewrite clears the config everywhere.
        sys.update_config_with(bucket, BUCKET_TAGGING_CONFIG, |bm| {
            assert!(bm.tagging_config.is_some(), "rewrite must see the persisted tags");
            Ok(Vec::new())
        })
        .await
        .expect("clearing rewrite should persist");
        assert_eq!(
            sys.get_tagging_config(bucket).await.unwrap_err(),
            Error::ConfigNotFound,
            "cleared tagging must not be served from the cache"
        );
        sys.metadata_map.write().await.clear();
        assert_eq!(
            sys.get_tagging_config(bucket).await.unwrap_err(),
            Error::ConfigNotFound,
            "cleared tagging must not reappear after a reload from disk"
        );
    }

    /// The load and the persisted write share one write guard, so concurrent
    /// rewrites of the same config compose instead of clobbering each other.
    /// Moving the load outside that guard loses all but the last tag.
    #[tokio::test]
    async fn concurrent_update_config_with_calls_do_not_lose_writes() {
        use crate::bucket::metadata::BUCKET_TAGGING_CONFIG;
        use s3s::dto::Tag;

        let (dirs, ecstore) = isolated_store_over_temp_disks().await;
        let sys = Arc::new(RwLock::new(BucketMetadataSys::new(ecstore)));

        let bucket = "swift-tagging-concurrent";
        for dir in &dirs {
            std::fs::create_dir_all(dir.path().join(bucket)).expect("bucket volume should be created");
        }
        sys.read()
            .await
            .persist_new_and_set(BucketMetadata::new(bucket))
            .await
            .expect("initial metadata should persist");

        const WRITERS: usize = 8;
        let mut handles = Vec::with_capacity(WRITERS);
        for idx in 0..WRITERS {
            let sys = sys.clone();
            handles.push(tokio::spawn(async move {
                sys.write()
                    .await
                    .update_config_with(bucket, BUCKET_TAGGING_CONFIG, move |bm| {
                        // Each writer merges its own tag onto whatever is
                        // currently persisted — the Swift rewrite shape.
                        let mut tagging = bm.tagging_config.clone().unwrap_or_else(|| Tagging { tag_set: vec![] });
                        tagging.tag_set.push(Tag {
                            key: Some(format!("swift-meta-key{idx}")),
                            value: Some(idx.to_string()),
                        });
                        crate::bucket::utils::serialize::<Tagging>(&tagging).map_err(|e| Error::other(e.to_string()))
                    })
                    .await
            }));
        }

        for handle in handles {
            handle
                .await
                .expect("writer task should join")
                .expect("rewrite should persist");
        }

        let (tags, _) = sys
            .read()
            .await
            .get_tagging_config(bucket)
            .await
            .expect("tagging should be readable");
        assert_eq!(tags.tag_set.len(), WRITERS, "every concurrent rewrite must survive: {tags:?}");
    }

    /// Pins the peer reload-notification contract (`reload_from_store`, the
    /// LoadBucketMetadata RPC path): only metadata actually read from
    /// persisted storage enters the cache. A load miss errors out and leaves
    /// the cache untouched — it must neither install a fabricated default
    /// for an unknown bucket nor replace an existing entry, since a
    /// transient ConfigNotFound during the notification would otherwise
    /// downgrade a lock-enabled bucket to an authoritative "no Object Lock"
    /// default and disable the batch-delete retention gate on this peer.
    #[tokio::test]
    async fn peer_reload_never_caches_fabricated_defaults_as_authoritative() {
        let (dirs, ecstore) = isolated_store_over_temp_disks().await;
        let sys = BucketMetadataSys::new(ecstore.clone());

        // (a) Miss with no cached entry: the reload fails and installs nothing.
        let err = sys
            .reload_from_store("reload-bucket")
            .await
            .expect_err("a reload miss must be reported to the notifying peer");
        assert!(
            err.to_string().contains("no persisted bucket metadata readable"),
            "the miss must surface through the dedicated non-persisted branch, got: {err}"
        );
        assert!(
            sys.get("reload-bucket").await.is_err(),
            "a reload miss must not install a fabricated default"
        );

        // (b) Miss with an existing entry: the reload fails and the entry
        // (standing in for a lock-enabled bucket's metadata) survives intact.
        let mut kept = BucketMetadata::new("reload-bucket");
        kept.object_lock_config_xml = b"<ObjectLockConfiguration/>".to_vec();
        sys.set("reload-bucket".to_string(), Arc::new(kept)).await;
        assert!(sys.reload_from_store("reload-bucket").await.is_err());
        let cached = sys
            .get("reload-bucket")
            .await
            .expect("existing entry must survive a reload miss");
        assert_eq!(
            cached.object_lock_config_xml,
            b"<ObjectLockConfiguration/>".to_vec(),
            "a reload miss must not replace the cached entry with a fabricated default"
        );

        // (c) Persisted metadata reloads over a stale cached entry: the
        // reload converges the cache to disk truth.
        let mut persisted = BucketMetadata::new("reload-bucket");
        persisted.policy_config_json = b"persisted-marker".to_vec();
        let lock_config = ObjectLockConfiguration {
            object_lock_enabled: Some(ObjectLockEnabled::from_static(ObjectLockEnabled::ENABLED)),
            rule: None,
        };
        persisted
            .update_config(
                crate::bucket::metadata::OBJECT_LOCK_CONFIG,
                crate::bucket::utils::serialize(&lock_config).unwrap(),
            )
            .unwrap();
        for dir in &dirs {
            std::fs::create_dir_all(dir.path().join("reload-bucket")).expect("physical bucket should exist before reload");
        }
        let persisted_incarnation = persisted.bucket_incarnation_id;
        sys.persist_new_and_set(persisted).await.expect("metadata should persist");
        let mut stale = BucketMetadata::new("reload-bucket");
        stale.policy_config_json = b"stale-cache-marker".to_vec();
        sys.set("reload-bucket".to_string(), Arc::new(stale)).await;
        sys.reload_from_store("reload-bucket")
            .await
            .expect("persisted metadata should reload");
        let cached = sys
            .get("reload-bucket")
            .await
            .expect("reloaded persisted metadata must be cached");
        assert_eq!(
            cached.policy_config_json,
            b"persisted-marker".to_vec(),
            "a reload must converge the cache to the persisted disk state"
        );
        assert_eq!(
            cached.bucket_incarnation_id, persisted_incarnation,
            "peer reload must publish the persisted bucket incarnation"
        );
        assert!(matches!(
            sys.get_object_lock_config_state("reload-bucket").await.unwrap(),
            ObjectLockConfigState::Configured { .. }
        ));
    }

    /// Two metadata systems over one backing store: the in-process stand-in
    /// for two nodes. They share no `RwLock`, so nothing but the transaction
    /// lock can serialize them — exactly the cross-node case.
    async fn two_nodes_over_one_store() -> (Vec<tempfile::TempDir>, Arc<RwLock<BucketMetadataSys>>, Arc<RwLock<BucketMetadataSys>>)
    {
        let (dirs, ecstore) = isolated_store_over_temp_disks().await;
        let node_a = Arc::new(RwLock::new(BucketMetadataSys::new(ecstore.clone())));
        let node_b = Arc::new(RwLock::new(BucketMetadataSys::new(ecstore)));
        (dirs, node_a, node_b)
    }

    #[tokio::test]
    #[serial]
    async fn disk_incarnation_read_detects_stale_cache_until_peer_reload() {
        let (dirs, node_a, node_b) = two_nodes_over_one_store().await;
        let bucket = "cross-node-incarnation-refresh";
        for dir in &dirs {
            std::fs::create_dir_all(dir.path().join(bucket)).unwrap();
        }

        let old = BucketMetadata::new(bucket);
        let old_incarnation = old.bucket_incarnation_id;
        node_a.read().await.persist_new_and_set(old).await.unwrap();

        let replacement = BucketMetadata::new(bucket);
        let replacement_incarnation = replacement.bucket_incarnation_id;
        assert_ne!(old_incarnation, replacement_incarnation);
        node_b.read().await.persist_new_and_set(replacement).await.unwrap();

        let node_a_sys = node_a.read().await.clone();

        assert_eq!(
            node_a_sys.get(bucket).await.unwrap().bucket_incarnation_id,
            old_incarnation,
            "node A should start with the predecessor incarnation cached"
        );
        assert_eq!(
            node_a_sys.get_bucket_incarnation_id_from_disk(bucket).await.unwrap(),
            replacement_incarnation,
            "commit-time incarnation validation must use persisted authority"
        );
        assert_eq!(
            node_a_sys.get(bucket).await.unwrap().bucket_incarnation_id,
            old_incarnation,
            "an incarnation-only read must not publish a partial metadata snapshot"
        );
        node_a_sys.reload_from_store(bucket).await.unwrap();
        assert_eq!(
            node_a_sys.get(bucket).await.unwrap().bucket_incarnation_id,
            replacement_incarnation,
            "peer reload must converge the complete cached metadata snapshot"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    async fn stale_config_request_cannot_mutate_a_recreated_bucket() {
        use crate::bucket::metadata::BUCKET_TAGGING_CONFIG;

        let (_dirs, store) = isolated_store_over_temp_disks().await;
        init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        let sys = bucket_metadata_sys_of(&store.ctx).unwrap();
        let bucket = "stale-config-request";

        store.make_bucket(bucket, &MakeBucketOptions::default()).await.unwrap();
        let old_incarnation = store.bucket_incarnation_id_from_disk(bucket).await.unwrap();
        store.delete_bucket(bucket, &DeleteBucketOptions::default()).await.unwrap();
        store.make_bucket(bucket, &MakeBucketOptions::default()).await.unwrap();
        let new_incarnation = store.bucket_incarnation_id_from_disk(bucket).await.unwrap();
        assert_ne!(old_incarnation, new_incarnation);

        let err =
            update_with_sys_expected(sys.clone(), bucket, BUCKET_TAGGING_CONFIG, b"<Tagging/>".to_vec(), Some(old_incarnation))
                .await
                .expect_err("a request authorized for the deleted incarnation must fail closed");
        assert!(matches!(err, Error::BucketNotFound(name) if name == bucket));

        let persisted = sys.read().await.get_config_from_disk(bucket).await.unwrap();
        assert_eq!(persisted.bucket_incarnation_id, new_incarnation);
        assert!(persisted.tagging_config_xml.is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    async fn bucket_delete_waits_for_config_mutation_fence() {
        use crate::bucket::metadata::BUCKET_TAGGING_CONFIG;
        use s3s::dto::Tag;

        let (_dirs, store) = isolated_store_over_temp_disks().await;
        init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        let sys = bucket_metadata_sys_of(&store.ctx).unwrap();
        let bucket = "delete-vs-config-mutation";
        store.make_bucket(bucket, &MakeBucketOptions::default()).await.unwrap();

        let guard = acquire_config_write_guard(sys.clone(), bucket).await.unwrap();
        let delete = tokio::spawn({
            let store = store.clone();
            async move { store.delete_bucket(bucket, &DeleteBucketOptions::default()).await }
        });
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(!delete.is_finished(), "DeleteBucket must wait for the config mutation lifecycle fence");

        let tagging = crate::bucket::utils::serialize(&Tagging {
            tag_set: vec![Tag {
                key: Some("generation".to_string()),
                value: Some("old".to_string()),
            }],
        })
        .unwrap();
        update_under_config_write_guard(sys, &guard, BUCKET_TAGGING_CONFIG, tagging)
            .await
            .unwrap();
        assert!(!delete.is_finished());
        drop(guard);

        timeout(Duration::from_secs(10), delete)
            .await
            .expect("DeleteBucket should resume after the config fence is released")
            .expect("DeleteBucket task should join")
            .expect("DeleteBucket should succeed");
    }

    /// Writers on different nodes updating *different* config files of one
    /// bucket must both survive. Each rewrites the whole metadata blob, so
    /// without a lock spanning the read-modify-write the later save carries
    /// the earlier writer's field back to its pre-update value — losing an
    /// orthogonal config while both clients were told the write succeeded.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    async fn concurrent_config_writes_from_separate_nodes_do_not_lose_writes() {
        use crate::bucket::metadata::{BUCKET_POLICY_CONFIG, BUCKET_TAGGING_CONFIG};

        let (dirs, node_a, node_b) = two_nodes_over_one_store().await;
        let bucket = "cross-node-config-writes";
        for dir in &dirs {
            std::fs::create_dir_all(dir.path().join(bucket)).expect("bucket volume should be created");
        }
        node_a
            .read()
            .await
            .persist_new_and_set(BucketMetadata::new(bucket))
            .await
            .expect("initial metadata should persist");

        // Several rounds: a single pass can serialize by luck, but a lost
        // update only needs one interleaving to show up.
        const ROUNDS: usize = 8;
        for round in 0..ROUNDS {
            let tagging = format!("<Tagging><Round>{round}</Round></Tagging>").into_bytes();
            let policy = format!(r#"{{"Version":"2012-10-17","Round":{round}}}"#).into_bytes();

            let start = Arc::new(tokio::sync::Barrier::new(2));
            let tagging_writer = {
                let (node, start, tagging) = (node_a.clone(), start.clone(), tagging.clone());
                tokio::spawn(async move {
                    start.wait().await;
                    update_with_sys(node, bucket, BUCKET_TAGGING_CONFIG, tagging).await
                })
            };
            let policy_writer = {
                let (node, start, policy) = (node_b.clone(), start.clone(), policy.clone());
                tokio::spawn(async move {
                    start.wait().await;
                    update_with_sys(node, bucket, BUCKET_POLICY_CONFIG, policy).await
                })
            };

            tagging_writer
                .await
                .expect("tagging writer should join")
                .expect("tagging update should succeed");
            policy_writer
                .await
                .expect("policy writer should join")
                .expect("policy update should succeed");

            // Disk truth, not either node's cache: the losing write is the one
            // that never reached the metadata file.
            let persisted = node_a
                .read()
                .await
                .get_config_from_disk(bucket)
                .await
                .expect("metadata should load from disk");
            assert_eq!(
                persisted.tagging_config_xml, tagging,
                "round {round}: the policy write clobbered the concurrent tagging write"
            );
            assert_eq!(
                persisted.policy_config_json, policy,
                "round {round}: the tagging write clobbered the concurrent policy write"
            );
        }
    }

    /// The guard has to cover the load as well as the save. If it were taken
    /// only around the save, a second node could load between the two and
    /// still overwrite with pre-update state.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    async fn bucket_metadata_transaction_lock_blocks_a_concurrent_config_write() {
        use crate::bucket::metadata::BUCKET_TAGGING_CONFIG;

        let (dirs, node_a, node_b) = two_nodes_over_one_store().await;
        let bucket = "cross-node-transaction-lock";
        for dir in &dirs {
            std::fs::create_dir_all(dir.path().join(bucket)).expect("bucket volume should be created");
        }
        node_a
            .read()
            .await
            .persist_new_and_set(BucketMetadata::new(bucket))
            .await
            .expect("initial metadata should persist");

        let held = acquire_transaction_lock_with_sys(&node_a, bucket)
            .await
            .expect("transaction lock should be acquirable");

        let blocked = tokio::spawn({
            let node_b = node_b.clone();
            async move { update_with_sys(node_b, bucket, BUCKET_TAGGING_CONFIG, b"<Tagging/>".to_vec()).await }
        });

        // Long enough for the write to have finished had it not waited: the
        // whole read-modify-write against temp disks is far quicker than this.
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert!(
            !blocked.is_finished(),
            "a config write must not proceed while another node holds the bucket's transaction lock"
        );

        drop(held);

        let updated = timeout(Duration::from_secs(10), blocked)
            .await
            .expect("the blocked write should proceed once the lock is released")
            .expect("blocked writer should join");
        updated.expect("the write should succeed after acquiring the lock");
    }

    /// A holder of the transaction lock must not call the locking entry
    /// point: the namespace lock is not reentrant, so it would block on
    /// itself until the acquire timeout.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    async fn transaction_lock_is_not_reentrant() {
        let (_dirs, node_a, node_b) = two_nodes_over_one_store().await;
        let bucket = "transaction-lock-reentrancy";

        let held = acquire_transaction_lock_with_sys(&node_a, bucket)
            .await
            .expect("first acquisition should succeed");

        // Same store, hence the same locker owner: exclusion must not depend
        // on the two acquisitions coming from different owners. Either
        // outcome is acceptable — still waiting, or refused — as long as no
        // second guard is handed out.
        let reacquired = timeout(Duration::from_millis(500), acquire_transaction_lock_with_sys(&node_b, bucket)).await;
        assert!(
            !matches!(reacquired, Ok(Ok(_))),
            "the transaction lock must exclude a second holder even under the same owner"
        );

        drop(held);
        timeout(Duration::from_secs(10), acquire_transaction_lock_with_sys(&node_b, bucket))
            .await
            .expect("re-acquisition should not time out once released")
            .expect("the lock should be acquirable after release");
    }

    fn target(bucket: &str, id: &str) -> BucketTarget {
        BucketTarget {
            source_bucket: bucket.to_string(),
            endpoint: format!("{id}.example.com:9000"),
            credentials: Some(Credentials {
                access_key: "access".to_string(),
                secret_key: "secret".to_string(),
                ..Default::default()
            }),
            target_bucket: format!("{bucket}-{id}"),
            arn: format!("arn:rustfs:replication:us-east-1:{bucket}:{id}"),
            target_type: BucketTargetType::ReplicationService,
            region: "us-east-1".to_string(),
            ..Default::default()
        }
    }

    #[tokio::test]
    #[serial]
    async fn metadata_reload_syncs_bucket_target_sys() {
        let bucket = "metadata-reload-targets";
        let target_sys = BucketTargetSys::get();
        target_sys.delete(bucket).await;

        let mut bm = BucketMetadata::new(bucket);
        bm.bucket_target_config = Some(BucketTargets {
            targets: vec![target(bucket, "fresh")],
        });

        sync_bucket_target_sys(bucket, &bm).await;

        let targets = target_sys
            .list_bucket_targets(bucket)
            .await
            .expect("target sync should publish bucket targets");
        assert_eq!(targets.targets.len(), 1);
        assert_eq!(targets.targets[0].arn, format!("arn:rustfs:replication:us-east-1:{bucket}:fresh"));

        target_sys.delete(bucket).await;
    }

    #[tokio::test]
    #[serial]
    async fn metadata_reload_clears_stale_bucket_targets_when_config_is_removed() {
        let bucket = "metadata-clear-targets";
        let target_sys = BucketTargetSys::get();
        target_sys.delete(bucket).await;
        target_sys
            .targets_map
            .write()
            .await
            .insert(bucket.to_string(), vec![target(bucket, "stale")]);

        let bm = BucketMetadata::new(bucket);
        sync_bucket_target_sys(bucket, &bm).await;

        assert!(target_sys.list_bucket_targets(bucket).await.is_err());
        target_sys.delete(bucket).await;
    }

    /// HP-5b (rustfs/backlog#938): installing bucket metadata publishes the
    /// durability override to the disk-layer registry, and clearing the
    /// config (or an invalid payload) withdraws it.
    #[test]
    fn metadata_sync_publishes_and_clears_durability_override() {
        use crate::disk::local::{DurabilityMode, bucket_durability};

        let bucket = "metadata-sync-durability";

        let mut bm = BucketMetadata::new(bucket);
        bm.durability_config_json = br#"{"mode":"relaxed"}"#.to_vec();
        sync_bucket_durability(bucket, &bm);
        assert_eq!(bucket_durability::lookup(bucket), Some(DurabilityMode::Relaxed));

        // Metadata without the config entry clears the override.
        let bm = BucketMetadata::new(bucket);
        sync_bucket_durability(bucket, &bm);
        assert_eq!(bucket_durability::lookup(bucket), None);

        // Invalid payloads degrade to "no override", never to a tier.
        let mut bm = BucketMetadata::new(bucket);
        bm.durability_config_json = br#"{"mode":"bogus"}"#.to_vec();
        sync_bucket_durability(bucket, &bm);
        assert_eq!(bucket_durability::lookup(bucket), None);

        // Cache removal clears the override too.
        let mut bm = BucketMetadata::new(bucket);
        bm.durability_config_json = br#"{"mode":"none"}"#.to_vec();
        sync_bucket_durability(bucket, &bm);
        assert_eq!(bucket_durability::lookup(bucket), Some(DurabilityMode::None));
        clear_bucket_durability(bucket);
        assert_eq!(bucket_durability::lookup(bucket), None);
    }

    #[tokio::test]
    async fn refresh_wait_exits_when_cancelled() {
        let cancel_token = CancellationToken::new();
        cancel_token.cancel();

        let should_refresh = timeout(
            Duration::from_millis(100),
            wait_refresh_interval_or_cancel(&cancel_token, Duration::from_secs(60)),
        )
        .await
        .expect("cancelled refresh wait should not sleep until the interval");

        assert!(!should_refresh);
    }
}
