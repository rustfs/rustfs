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

use crate::bucket::replication::replication_state_from_filemeta;
#[cfg(all(test, feature = "test-util"))]
use crate::bucket::utils::is_meta_bucketname;
use crate::bucket::versioning_sys::BucketVersioningSys;
use crate::bucket::{
    lifecycle::{
        DurableIlmRecordCheckpoint, ILM_META_PREFIX, LifecycleExpiryConfigs, ValidatedDurableIlmRecord,
        bucket_lifecycle_audit::LcEventSrc,
        bucket_lifecycle_ops::{
            LifecycleOps, apply_expiry_on_transitioned_object, apply_expiry_rule_for_data_movement, apply_expiry_rule_in,
            eval_action_from_lifecycle, lifecycle_delete_all_versions_blocked_by_replication,
        },
        classify_durable_ilm_record, get_expiry_configs,
        lifecycle::IlmAction,
        validate_durable_ilm_record,
    },
    metadata_sys,
};
use crate::cache_value::metacache_set::{ListPathRawOptions, list_path_raw};
use crate::config::com::{
    CONFIG_PREFIX, delete_config, read_config_limited_preserve_empty, read_config_limited_preserve_empty_with_metadata,
    read_config_no_lock_preserve_empty_with_metadata, read_config_preserve_empty, save_config_with_opts,
    save_config_with_opts_and_metadata,
};
use crate::data_movement;
use crate::data_movement::backpressure::{self, DataMovementOperation};
use crate::data_usage::DATA_USAGE_CACHE_NAME;
use crate::disk::error::DiskError;
use crate::disk::{BUCKET_META_PREFIX, DiskAPI, RUSTFS_META_BUCKET};
use crate::error::{Error, Result};
use crate::error::{
    StorageError, is_err_bucket_exists, is_err_bucket_not_found, is_err_object_not_found, is_err_operation_canceled,
    is_err_version_not_found,
};
use crate::layout::endpoints::EndpointServerPools;
use crate::object_api::{GetObjectReader, ObjectOptions};
use crate::runtime::sources as runtime_sources;
use crate::services::rebalance::{REBAL_META_NAME, RebalanceMeta, is_rebalance_conflicting_with_decommission};
use crate::set_disk::{SetDisks, get_lock_acquire_timeout};
use crate::storage_api_contracts::{
    admin::StorageAdminApi,
    bucket::{BucketOperations, BucketOptions, MakeBucketOptions},
    heal::HealOperations as _,
    list::ListOperations as _,
    namespace::NamespaceLocking as _,
    object::{EcstoreObjectIO, HTTPPreconditions, ObjectIO as _, ObjectOperations as _},
};
use crate::{core::sets::Sets, store::ECStore};
use byteorder::{ByteOrder, LittleEndian, WriteBytesExt};
use futures::{
    StreamExt,
    future::{BoxFuture, join_all},
    stream::FuturesUnordered,
};
use http::HeaderMap;
#[cfg(test)]
use rmp_serde::Deserializer;
use rmp_serde::Serializer;
use rustfs_filemeta::{FileInfoVersions, MetaCacheEntries, MetaCacheEntry, MetadataResolutionParams};
use rustfs_heal_contracts::heal_channel::HealOpts;
use rustfs_utils::crypto::{hex_sha256, is_sha256_checksum};
use rustfs_utils::path::{encode_dir_object, path_join, path_to_bucket_object, path_to_bucket_object_with_base_path};
use s3s::dto::{BucketLifecycleConfiguration, ObjectLockConfiguration, ReplicationConfiguration};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fmt::Display;
#[cfg(test)]
use std::io::Cursor;
use std::io::Write;
use std::path::PathBuf;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use time::{Duration, OffsetDateTime};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

const LOG_COMPONENT_ECSTORE: &str = "ecstore";
const LOG_SUBSYSTEM_POOLS: &str = "pools";
const EVENT_DECOMMISSION_STATE: &str = "decommission_state";
const EVENT_DECOMMISSION_BUCKET: &str = "decommission_bucket";
const EVENT_DECOMMISSION_ENTRY: &str = "decommission_entry";
const POOL_ACTIVATION_FLEET_PROOF_REQUIRED: &str = "pool activation requires a live fleet capability proof";
const POOL_ACTIVATION_FLEET_PROOF_EXPIRED: &str = "pool activation fleet capability proof expired before commit";
const DECOMMISSION_STAGE_MIGRATE_OBJECT: &str = "migrate_object";
const DECOMMISSION_STAGE_CLEANUP_PREFLIGHT: &str = "cleanup_preflight";
const DECOMMISSION_STAGE_SOURCE_CLEANUP: &str = "source_cleanup";
const DECOMMISSION_STAGE_ENTRY_FINISHED: &str = "entry_finished";
const DECOMMISSION_PROGRESS_SAVE_INTERVAL: Duration = Duration::seconds(30);
const DECOMMISSION_PROGRESS_SAVE_ITEM_THRESHOLD: usize = 1000;
const DECOMMISSION_PROGRESS_SAVE_RETRY_BACKOFF: Duration = Duration::seconds(1);
const DECOMMISSION_BUCKET_CONCURRENCY_ENV: &str = "RUSTFS_DECOMMISSION_BUCKET_CONCURRENCY";
const DECOMMISSION_BUCKET_CONCURRENCY_DEFAULT_CAP: usize = 4;
const DECOMMISSION_ENTRY_CONCURRENCY_ENV: &str = "RUSTFS_DECOMMISSION_ENTRY_CONCURRENCY";
const DECOMMISSION_ENTRY_CONCURRENCY_DEFAULT_CAP: usize = 8;
const DECOMMISSION_ENTRY_CONCURRENCY_HARD_CAP: usize = 64;
const DECOMMISSION_ENTRY_WORKERS_PER_SET: usize = 2;
const DECOMMISSION_META_PREFIXES: [&str; 3] = [CONFIG_PREFIX, BUCKET_META_PREFIX, ILM_META_PREFIX];
const DECOMMISSION_TARGET_CAPACITY_OVERHEAD_PERCENT: usize = 30;
const DECOMMISSION_LISTING_MAX_ATTEMPTS: usize = 3;
const DECOMMISSION_LISTING_RETRY_DELAY: std::time::Duration = std::time::Duration::from_secs(5);
pub(crate) const DECOMMISSION_ENTRY_MAX_ATTEMPTS: usize = 3;
const DECOMMISSION_SOURCE_CLEANUP_RETRY_DELAY: std::time::Duration = std::time::Duration::from_millis(100);
pub(crate) const DECOMMISSION_VERSION_COPY_ATTEMPTS: usize = 3;
const DECOMMISSION_COPY_RETRY_DELAY: std::time::Duration = std::time::Duration::from_millis(50);
const DECOMMISSION_SOURCE_CHANGED_EXHAUSTION_LIMIT: usize = 100;
const DECOMMISSION_TERMINAL_RETRY_DELAY: std::time::Duration = std::time::Duration::from_secs(1);
const DECOMMISSION_DURABLE_ILM_RECEIPT_ROOT: &str = "decommission/ilm-receipts";
const DECOMMISSION_DURABLE_ILM_MANIFEST_ROOT: &str = "decommission/ilm-manifests";
const DECOMMISSION_DURABLE_ILM_RECEIPT_SCHEMA: &str = "v2";
const DECOMMISSION_DURABLE_ILM_MANIFEST_SCHEMA: &str = "v1";
const DECOMMISSION_DURABLE_ILM_RECEIPT_MAX_SIZE: usize = 16 * 1024;
const DECOMMISSION_DURABLE_ILM_MANIFEST_MAX_SIZE: usize = 4 * 1024;
const DECOMMISSION_DURABLE_ILM_RECEIPT_CAS_ATTEMPTS: usize = 3;
/// Background decommission walks must tolerate slow object migrations; the
/// stall timeout is the drive-health bound, not the total listing duration.
const DECOMMISSION_BACKGROUND_WALKDIR_STALL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

pub const POOL_META_NAME: &str = "pool.bin";
pub(crate) const POOL_META_IDENTITY_NAME: &str = "pool.bin.identity";
pub const POOL_META_FORMAT: u16 = 1;
const POOL_META_V1_VERSION: u16 = 1;
pub const POOL_META_VERSION: u16 = 2;
const POOL_META_GENERATION_VERSION: u16 = 3;
const POOL_META_IDENTITY_FORMAT: u16 = 1;
const POOL_META_IDENTITY_VERSION: u16 = 1;
const POOL_META_INITIAL_EPOCH: u64 = 1;
const POOL_META_CAS_MAX_ATTEMPTS: usize = 3;
const METRIC_POOL_META_STALE_WRITE_REJECTIONS_TOTAL: &str = "rustfs_pool_meta_stale_write_rejections_total";

fn record_pool_meta_stale_write_rejection(reason: &'static str) {
    metrics::counter!(METRIC_POOL_META_STALE_WRITE_REJECTIONS_TOTAL, "reason" => reason).increment(1);
}

fn pool_meta_v2_writer_enabled_for(requested: bool, fleet_confirmed: bool) -> bool {
    requested && fleet_confirmed
}

fn pool_meta_v2_writer_enabled() -> bool {
    pool_meta_v2_writer_enabled_for(
        rustfs_utils::get_env_bool(rustfs_config::ENV_POOL_META_V2_WRITE, rustfs_config::DEFAULT_POOL_META_V2_WRITE),
        rustfs_utils::get_env_bool(
            rustfs_config::ENV_POOL_META_V2_FLEET_CONFIRMED,
            rustfs_config::DEFAULT_POOL_META_V2_FLEET_CONFIRMED,
        ),
    )
}

fn pool_meta_v3_writer_enabled_for(requested: bool, fleet_confirmed: bool) -> bool {
    requested && fleet_confirmed
}

fn pool_meta_v3_writer_enabled() -> bool {
    pool_meta_v3_writer_enabled_for(
        rustfs_utils::get_env_bool(rustfs_config::ENV_POOL_META_V3_WRITE, rustfs_config::DEFAULT_POOL_META_V3_WRITE),
        rustfs_utils::get_env_bool(
            rustfs_config::ENV_POOL_META_V3_FLEET_CONFIRMED,
            rustfs_config::DEFAULT_POOL_META_V3_FLEET_CONFIRMED,
        ),
    )
}

#[derive(Clone, Debug)]
pub struct DecommissionCanceler {
    operation: Arc<DecommissionOperation>,
}

#[derive(Debug)]
struct DecommissionOperation {
    token: CancellationToken,
    active: AtomicBool,
}

impl DecommissionCanceler {
    pub(crate) fn new(token: CancellationToken) -> Self {
        Self {
            operation: Arc::new(DecommissionOperation {
                token,
                active: AtomicBool::new(true),
            }),
        }
    }

    #[cfg(test)]
    pub(crate) fn new_for_test(token: CancellationToken) -> Self {
        Self::new(token)
    }

    fn token(&self) -> &CancellationToken {
        &self.operation.token
    }

    pub(crate) fn is_active(&self) -> bool {
        self.operation.active.load(Ordering::Acquire)
    }

    #[cfg(test)]
    fn is_cancelled(&self) -> bool {
        self.token().is_cancelled()
    }

    fn cancel(&self) {
        self.token().cancel();
    }

    fn release(&self) {
        self.cancel();
        self.operation.active.store(false, Ordering::Release);
    }

    fn owns_same_operation(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.operation, &other.operation)
    }
}

struct DecommissionCancelerGuard {
    canceler: DecommissionCanceler,
}

impl DecommissionCancelerGuard {
    fn new(canceler: DecommissionCanceler) -> Self {
        Self { canceler }
    }

    fn canceler(&self) -> &DecommissionCanceler {
        &self.canceler
    }
}

impl Drop for DecommissionCancelerGuard {
    fn drop(&mut self) {
        self.canceler.release();
    }
}

fn dedup_indices(indices: &[usize]) -> Vec<usize> {
    let mut seen = HashSet::with_capacity(indices.len());
    let mut output = Vec::with_capacity(indices.len());
    for idx in indices {
        if seen.insert(*idx) {
            output.push(*idx);
        }
    }

    output
}

fn bind_decommission_cancelers(
    indices: &[usize],
    parent: &CancellationToken,
    cancelers: &mut [Option<DecommissionCanceler>],
) -> Vec<(usize, DecommissionCanceler)> {
    let mut bound = Vec::with_capacity(indices.len());

    for idx in indices {
        if let Some(slot) = cancelers.get_mut(*idx) {
            if let Some(existing) = slot.take() {
                existing.release();
            }
            let canceler = DecommissionCanceler::new(parent.child_token());
            *slot = Some(canceler.clone());
            bound.push((*idx, canceler));
        }
    }

    bound
}

fn bind_missing_decommission_cancelers(
    indices: &[usize],
    parent: &CancellationToken,
    cancelers: &mut [Option<DecommissionCanceler>],
) -> Vec<(usize, DecommissionCanceler)> {
    let mut bound = Vec::with_capacity(indices.len());

    for idx in indices {
        let Some(slot) = cancelers.get_mut(*idx) else {
            continue;
        };
        if slot.as_ref().is_some_and(DecommissionCanceler::is_active) {
            break;
        }
        if let Some(stale) = slot.take() {
            stale.release();
        }
        let canceler = DecommissionCanceler::new(parent.child_token());
        *slot = Some(canceler.clone());
        bound.push((*idx, canceler));
    }

    bound
}

fn take_decommission_canceler(cancelers: &mut [Option<DecommissionCanceler>], idx: usize) -> Option<DecommissionCanceler> {
    cancelers.get_mut(idx).and_then(Option::take)
}

fn take_decommission_canceler_for_operation(
    cancelers: &mut [Option<DecommissionCanceler>],
    idx: usize,
    owner: &DecommissionCanceler,
) -> Option<DecommissionCanceler> {
    let slot = cancelers.get_mut(idx)?;
    if slot.as_ref().is_some_and(|canceler| canceler.owns_same_operation(owner)) {
        slot.take()
    } else {
        None
    }
}

fn decommission_canceler_is_owned_by(
    cancelers: &[Option<DecommissionCanceler>],
    idx: usize,
    owner: &DecommissionCanceler,
) -> bool {
    cancelers
        .get(idx)
        .and_then(Option::as_ref)
        .is_some_and(|canceler| canceler.owns_same_operation(owner))
}

fn update_decommission_for_operation<T>(
    cancelers: &[Option<DecommissionCanceler>],
    pool_meta: &mut PoolMeta,
    idx: usize,
    owner: Option<&DecommissionCanceler>,
    update: impl FnOnce(&mut PoolMeta) -> T,
) -> Option<T> {
    if let Some(owner) = owner
        && !decommission_canceler_is_owned_by(cancelers, idx, owner)
    {
        owner.release();
        return None;
    }

    Some(update(pool_meta))
}

fn has_active_decommission_canceler(cancelers: &[Option<DecommissionCanceler>]) -> bool {
    cancelers.iter().flatten().any(DecommissionCanceler::is_active)
}

fn cancel_decommission_canceler(canceler: Option<DecommissionCanceler>) -> bool {
    if let Some(canceler) = canceler {
        canceler.release();
        true
    } else {
        false
    }
}

fn take_and_cancel_decommission_canceler(cancelers: &mut [Option<DecommissionCanceler>], idx: usize) -> bool {
    let canceler = take_decommission_canceler(cancelers, idx);
    cancel_decommission_canceler(canceler)
}

fn take_and_cancel_decommission_canceler_for_operation(
    cancelers: &mut [Option<DecommissionCanceler>],
    idx: usize,
    owner: &DecommissionCanceler,
) -> bool {
    let canceler = take_decommission_canceler_for_operation(cancelers, idx, owner);
    if canceler.is_none() {
        owner.release();
        return false;
    }
    cancel_decommission_canceler(canceler)
}

fn ensure_decommission_routines_scheduled(bound_count: usize, expected_count: usize) -> Result<()> {
    if bound_count == 0 || bound_count != expected_count {
        return Err(Error::other(format!(
            "failed to start decommission routines: scheduled {bound_count} of {expected_count} expected workers"
        )));
    }

    Ok(())
}

fn guard_decommission_cancelers(index_cancelers: Vec<(usize, DecommissionCanceler)>) -> Vec<(usize, DecommissionCancelerGuard)> {
    index_cancelers
        .into_iter()
        .map(|(idx, canceler)| (idx, DecommissionCancelerGuard::new(canceler)))
        .collect()
}

async fn await_decommission_worker(idx: usize, worker: tokio::task::JoinHandle<Result<()>>) -> Result<()> {
    worker
        .await
        .map_err(|err| Error::other(format!("decommission worker {idx} task join error: {err}")))?
}

fn reserve_decommission_start_cancelers(
    pool_meta: &PoolMeta,
    indices: &[usize],
    local_indices: &[usize],
    parent: &CancellationToken,
    cancelers: &mut [Option<DecommissionCanceler>],
) -> Result<Vec<(usize, DecommissionCancelerGuard)>> {
    ensure_decommission_start_pool_states(pool_meta, indices)?;
    if local_indices.is_empty() {
        return Ok(Vec::new());
    }
    let bound = bind_decommission_cancelers(local_indices, parent, cancelers);
    let guards = guard_decommission_cancelers(bound);
    ensure_decommission_routines_scheduled(guards.len(), local_indices.len())?;
    Ok(guards)
}

fn default_decommission_bucket_concurrency(cpu_count: usize) -> usize {
    cpu_count.clamp(1, DECOMMISSION_BUCKET_CONCURRENCY_DEFAULT_CAP)
}

fn decommission_bucket_concurrency_limit() -> usize {
    let default_limit = default_decommission_bucket_concurrency(num_cpus::get());
    rustfs_utils::get_env_usize(DECOMMISSION_BUCKET_CONCURRENCY_ENV, default_limit).max(1)
}

fn default_decommission_entry_concurrency(cpu_count: usize) -> usize {
    cpu_count.clamp(1, DECOMMISSION_ENTRY_CONCURRENCY_DEFAULT_CAP)
}

fn clamp_decommission_entry_concurrency(limit: usize) -> usize {
    limit.clamp(1, DECOMMISSION_ENTRY_CONCURRENCY_HARD_CAP)
}

fn decommission_entry_concurrency_limit() -> usize {
    let default_limit = default_decommission_entry_concurrency(num_cpus::get());
    clamp_decommission_entry_concurrency(rustfs_utils::get_env_usize(DECOMMISSION_ENTRY_CONCURRENCY_ENV, default_limit))
}

fn is_decommission_meta_bucket(bucket: &DecomBucketInfo) -> bool {
    bucket.name == RUSTFS_META_BUCKET
}

fn decommission_meta_buckets() -> [DecomBucketInfo; DECOMMISSION_META_PREFIXES.len()] {
    DECOMMISSION_META_PREFIXES.map(|prefix| DecomBucketInfo {
        name: RUSTFS_META_BUCKET.to_owned(),
        prefix: prefix.to_owned(),
    })
}

fn reconcile_decommission_meta_buckets(meta: &mut PoolMeta, idx: usize) -> bool {
    let before = meta.pending_buckets(idx).len();
    meta.queue_buckets(idx, decommission_meta_buckets().into());
    meta.pending_buckets(idx).len() != before
}

fn split_decommission_buckets(buckets: Vec<DecomBucketInfo>) -> (Vec<DecomBucketInfo>, Vec<DecomBucketInfo>) {
    let mut regular = Vec::with_capacity(buckets.len());
    let mut meta = Vec::new();

    for bucket in buckets {
        if is_decommission_meta_bucket(&bucket) {
            meta.push(bucket);
        } else {
            regular.push(bucket);
        }
    }

    regular.shrink_to_fit();
    (regular, meta)
}

fn ensure_decommission_not_rebalancing(rebalance_running: bool) -> Result<()> {
    if rebalance_running {
        return Err(Error::RebalanceAlreadyRunning);
    }

    Ok(())
}

fn ensure_decommission_start_rebalance_meta_allowed(meta: Option<&RebalanceMeta>) -> Result<()> {
    ensure_decommission_not_rebalancing(meta.is_some_and(is_rebalance_conflicting_with_decommission))
}

#[allow(dead_code, reason = "leader precondition asserted by this file's tests (backlog#1823)")]
fn ensure_local_decommission_pool_leaders(endpoints: &EndpointServerPools, indices: &[usize]) -> Result<()> {
    for idx in indices {
        ensure_local_decommission_pool_leader(endpoints, *idx)?;
    }

    Ok(())
}

fn ensure_local_decommission_pool_leader(endpoints: &EndpointServerPools, idx: usize) -> Result<()> {
    let pool = endpoints
        .as_ref()
        .get(idx)
        .ok_or_else(|| invalid_decommission_pool_index_error(endpoints.as_ref().len(), idx))?;
    let endpoint = pool
        .endpoints
        .as_ref()
        .first()
        .ok_or_else(|| Error::other(format!("decommission pool {idx} has no configured endpoints")))?;

    if !endpoint.is_local {
        return Err(Error::other(format!(
            "decommission for pool {idx} must run on the pool first endpoint {endpoint}"
        )));
    }

    Ok(())
}

fn decommission_pool_first_endpoint_is_local(endpoints: &EndpointServerPools, idx: usize) -> Result<bool> {
    let pool = endpoints
        .as_ref()
        .get(idx)
        .ok_or_else(|| invalid_decommission_pool_index_error(endpoints.as_ref().len(), idx))?;
    let endpoint = pool
        .endpoints
        .as_ref()
        .first()
        .ok_or_else(|| Error::other(format!("decommission pool {idx} has no configured endpoints")))?;

    Ok(endpoint.is_local)
}

pub(crate) fn local_decommission_queue_prefix(endpoints: &EndpointServerPools, indices: &[usize]) -> Result<Vec<usize>> {
    let mut local = Vec::with_capacity(indices.len());

    for idx in indices {
        if decommission_pool_first_endpoint_is_local(endpoints, *idx)? {
            local.push(*idx);
        } else {
            break;
        }
    }

    Ok(local)
}

fn resumable_decommission_queue_indices(meta: &PoolMeta) -> Vec<usize> {
    let mut indices = Vec::new();
    for (idx, pool) in meta.pools.iter().enumerate() {
        if let Some(decommission) = &pool.decommission {
            if !decommission.has_decommission_state() {
                continue;
            }
            if decommission.complete || decommission.failed || decommission.canceled {
                continue;
            }
            indices.push(idx);
        }
    }

    indices
}

fn missing_decommission_worker_prefix(indices: &[usize], cancelers: &[Option<DecommissionCanceler>]) -> Vec<usize> {
    let mut missing = Vec::with_capacity(indices.len());

    for idx in indices {
        if cancelers
            .get(*idx)
            .and_then(Option::as_ref)
            .is_some_and(DecommissionCanceler::is_active)
        {
            break;
        }
        missing.push(*idx);
    }

    missing
}

fn ensure_decommission_start_local_leader(endpoints: &EndpointServerPools, indices: &[usize]) -> Result<()> {
    if let Some(first) = indices.first() {
        ensure_local_decommission_pool_leader(endpoints, *first)?;
    }

    Ok(())
}

fn build_decommission_start_state(
    pi: PoolSpaceInfo,
    queued: bool,
    now: OffsetDateTime,
    previous: Option<&PoolDecommissionInfo>,
) -> PoolDecommissionInfo {
    let mut info = PoolDecommissionInfo {
        start_time: if queued { None } else { Some(now) },
        start_size: pi.free,
        total_size: pi.total,
        current_size: pi.free,
        queued,
        ..Default::default()
    };

    if let Some(previous) = previous
        && (previous.failed || previous.canceled)
    {
        info.decommissioned_buckets = previous.decommissioned_buckets.clone();
        info.items_decommissioned = previous.items_decommissioned;
        info.bytes_done = previous.bytes_done;
        info.unresolved_entries = previous.unresolved_entries.clone();
        if let Some(generation) = info.start_time {
            for entry in &mut info.unresolved_entries {
                entry.source_generation = generation;
            }
        }
        info.mark_progress_saved();
    }

    info
}

fn spawn_decommission_index_cancelers(
    store: Arc<ECStore>,
    rx: CancellationToken,
    index_cancelers: Vec<(usize, DecommissionCancelerGuard)>,
    entry_budget: Arc<Semaphore>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut stop_queue = false;

        for (idx, canceler_guard) in index_cancelers {
            let canceler = canceler_guard.canceler().clone();
            if stop_queue || rx.is_cancelled() {
                canceler.cancel();
                store.retry_decommission_cancel_for_operation(idx, &canceler).await;
                continue;
            }

            let worker = tokio::spawn({
                let store = store.clone();
                let canceler = canceler.clone();
                let entry_budget = entry_budget.clone();
                async move { store.do_decommission_in_routine(canceler, idx, entry_budget).await }
            });
            if let Err(err) = await_decommission_worker(idx, worker).await {
                if let Err(blocked) = store
                    .ensure_pool_meta_side_effects_safe("decommission paused because pool metadata requires recovery")
                    .await
                {
                    warn!(
                        event = EVENT_DECOMMISSION_STATE,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_POOLS,
                        pool_index = idx,
                        state = "routine_blocked",
                        error = %blocked,
                        "Decommission routine paused without changing terminal state"
                    );
                    store.release_decommission_canceler_slot(idx, &canceler).await;
                    stop_queue = true;
                    continue;
                }
                error!(
                    event = EVENT_DECOMMISSION_STATE,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_POOLS,
                    pool_index = idx,
                    state = "routine_failed",
                    error = %err,
                    "Decommission routine failed"
                );
                store.quiesce_decommission_worker_after_join_error(&canceler).await;
                store.retry_decommission_failed_for_operation(idx, &canceler).await;
                stop_queue = true;
                continue;
            }

            stop_queue = {
                let pool_meta = store.pool_meta.read().await;
                !should_continue_decommission_queue(&pool_meta, idx)
            };
        }
    })
}

fn decommission_meta_bucket_options() -> MakeBucketOptions {
    MakeBucketOptions {
        force_create: true,
        ..Default::default()
    }
}

fn is_decommission_active(complete: bool, failed: bool, canceled: bool) -> bool {
    !complete && !failed && !canceled
}

pub(crate) fn pool_meta_has_active_decommission(meta: &PoolMeta) -> bool {
    meta.pools.iter().any(|pool| {
        pool.decommission.as_ref().is_some_and(|info| {
            info.has_decommission_state() && is_decommission_active(info.complete, info.failed, info.canceled)
        })
    })
}

fn is_decommission_suspended(info: &PoolDecommissionInfo) -> bool {
    info.has_decommission_state() && (!info.queued || !info.unresolved_entries.is_empty())
}

fn validate_decommission_terminal_state(complete: bool, failed: bool, canceled: bool) -> Result<()> {
    let terminal_count = [complete, failed, canceled].into_iter().filter(|terminal| *terminal).count();
    if terminal_count > 1 {
        return Err(Error::other(format!(
            "pool metadata load failed: invalid decommission terminal state complete={complete} failed={failed} canceled={canceled}"
        )));
    }
    Ok(())
}

fn invalid_decommission_pool_index_error(pool_count: usize, idx: usize) -> Error {
    Error::other(format!("invalid decommission pool index {idx} for {pool_count} pools"))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DecommissionStartPoolState {
    Missing,
    Active,
    Retryable,
    Decommissioning,
    Decommissioned,
    Blocked,
}

fn decommission_start_pool_state(pool: Option<&PoolStatus>) -> DecommissionStartPoolState {
    let Some(pool) = pool else {
        return DecommissionStartPoolState::Missing;
    };
    let Some(info) = pool.decommission.as_ref() else {
        return DecommissionStartPoolState::Active;
    };
    if !info.has_decommission_state() {
        return DecommissionStartPoolState::Active;
    }

    if info.complete {
        DecommissionStartPoolState::Decommissioned
    } else if info.failed || info.canceled {
        if info.unresolved_entries.is_empty() {
            DecommissionStartPoolState::Blocked
        } else {
            DecommissionStartPoolState::Retryable
        }
    } else {
        DecommissionStartPoolState::Decommissioning
    }
}

fn is_decommission_start_active_pool(pool: &PoolStatus) -> bool {
    decommission_start_pool_state(Some(pool)) == DecommissionStartPoolState::Active
}

fn ensure_decommission_start_allowed(state: DecommissionStartPoolState) -> Result<()> {
    match state {
        DecommissionStartPoolState::Missing => Err(Error::other("failed to start decommission: target pool was not found")),
        DecommissionStartPoolState::Active | DecommissionStartPoolState::Retryable => Ok(()),
        DecommissionStartPoolState::Decommissioning => Err(StorageError::DecommissionAlreadyRunning),
        DecommissionStartPoolState::Decommissioned => {
            Err(Error::other("failed to start decommission: target pool is already decommissioned"))
        }
        DecommissionStartPoolState::Blocked => Err(Error::other(
            "failed to start decommission: target pool decommission is blocked; clear failed or canceled metadata before starting again",
        )),
    }
}

fn ensure_decommission_start_keeps_active_pool(meta: &PoolMeta, indices: &[usize]) -> Result<()> {
    let active_count = meta
        .pools
        .iter()
        .filter(|pool| is_decommission_start_active_pool(pool))
        .count();
    let active_target_count = indices
        .iter()
        .filter(|idx| meta.pools.get(**idx).is_some_and(is_decommission_start_active_pool))
        .count();
    if active_count.saturating_sub(active_target_count) == 0 {
        return Err(Error::other(
            "failed to start decommission: at least one active pool must remain after decommission start",
        ));
    }

    Ok(())
}

fn ensure_decommission_start_pool_states(meta: &PoolMeta, indices: &[usize]) -> Result<()> {
    for idx in indices.iter().copied() {
        ensure_decommission_start_allowed(decommission_start_pool_state(meta.pools.get(idx)))?;
    }
    ensure_decommission_start_keeps_active_pool(meta, indices)
}

fn decommission_target_capacity_required(source_used: usize) -> usize {
    source_used
        .saturating_mul(100 + DECOMMISSION_TARGET_CAPACITY_OVERHEAD_PERCENT)
        .div_ceil(100)
}

fn ensure_decommission_start_target_capacity(
    meta: &PoolMeta,
    indices: &[usize],
    space_infos: &[(usize, PoolSpaceInfo)],
) -> Result<()> {
    let mut source_used = 0usize;
    let mut target_free = 0usize;

    for (idx, info) in space_infos {
        if indices.contains(idx) {
            source_used = source_used.saturating_add(info.used);
        } else if meta.pools.get(*idx).is_some_and(is_decommission_start_active_pool) {
            target_free = target_free.saturating_add(info.free);
        }
    }

    let required = decommission_target_capacity_required(source_used);
    if target_free < required {
        return Err(Error::other(format!(
            "failed to start decommission: insufficient target pool capacity: required {required} bytes available {target_free} bytes for {source_used} bytes used in decommission pools with {DECOMMISSION_TARGET_CAPACITY_OVERHEAD_PERCENT}% overhead"
        )));
    }

    Ok(())
}

fn ensure_valid_decommission_pool_index(pool_count: usize, idx: usize) -> Result<()> {
    if idx >= pool_count {
        return Err(invalid_decommission_pool_index_error(pool_count, idx));
    }

    Ok(())
}

fn get_by_index<'a, T>(items: &'a [T], idx: usize, operation: &'static str) -> Result<&'a T> {
    items.get(idx).ok_or_else(|| {
        Error::other(format!(
            "failed to {operation}: invalid decommission pool index {idx} for {pool_count} pools",
            pool_count = items.len()
        ))
    })
}

fn decommission_metadata_not_initialized_error(operation: &str) -> Error {
    Error::other(format!("failed to {operation}: decommission metadata not initialized"))
}

fn resolve_decommission_bucket_state(meta: &PoolMeta, idx: usize, bucket: &DecomBucketInfo) -> Result<bool> {
    let pool_count = meta.pools.len();
    ensure_valid_decommission_pool_index(pool_count, idx)?;

    let Some(pool) = meta.pools.get(idx) else {
        return Err(invalid_decommission_pool_index_error(pool_count, idx));
    };
    let Some(info) = pool.decommission.as_ref() else {
        return Err(decommission_metadata_not_initialized_error("resolve decommission bucket state"));
    };

    Ok(info.is_bucket_decommissioned(&bucket.to_string()))
}

fn mark_decommission_bucket_done(meta: &mut PoolMeta, idx: usize, bucket: &DecomBucketInfo) -> Result<bool> {
    let pool_count = meta.pools.len();
    ensure_valid_decommission_pool_index(pool_count, idx)?;

    let Some(pool) = meta.pools.get_mut(idx) else {
        return Err(invalid_decommission_pool_index_error(pool_count, idx));
    };
    let Some(info) = pool.decommission.as_mut() else {
        return Err(decommission_metadata_not_initialized_error("mark decommission bucket done"));
    };

    Ok(info.bucket_pop(&bucket.to_string()))
}

fn count_decommission_item(meta: &mut PoolMeta, idx: usize, size: usize, failed: bool) -> Result<()> {
    let pool_count = meta.pools.len();
    ensure_valid_decommission_pool_index(pool_count, idx)?;

    let Some(pool) = meta.pools.get_mut(idx) else {
        return Err(invalid_decommission_pool_index_error(pool_count, idx));
    };
    let Some(info) = pool.decommission.as_mut() else {
        return Err(decommission_metadata_not_initialized_error("count decommission item"));
    };

    if failed {
        info.items_decommission_failed += 1;
        info.bytes_failed += size;
    } else {
        info.items_decommissioned += 1;
        info.bytes_done += size;
    }

    Ok(())
}

fn ensure_decommission_generation(meta: &PoolMeta, idx: usize, generation: OffsetDateTime) -> Result<()> {
    let Some(pool) = meta.pools.get(idx) else {
        return Err(invalid_decommission_pool_index_error(meta.pools.len(), idx));
    };
    let Some(info) = pool.decommission.as_ref() else {
        return Err(decommission_metadata_not_initialized_error("check decommission generation"));
    };

    if info.start_time == Some(generation) && !info.queued && is_decommission_active(info.complete, info.failed, info.canceled) {
        Ok(())
    } else {
        Err(Error::OperationCanceled)
    }
}

fn record_decommission_unresolved_entry(
    meta: &mut PoolMeta,
    idx: usize,
    generation: OffsetDateTime,
    entry: DecommissionUnresolvedEntry,
) -> Result<bool> {
    ensure_decommission_generation(meta, idx, generation)?;
    if entry.pool_index != idx || entry.source_generation != generation {
        return Err(Error::other("decommission unresolved entry does not match the active pool generation"));
    }

    let pool_count = meta.pools.len();
    let Some(pool) = meta.pools.get_mut(idx) else {
        return Err(invalid_decommission_pool_index_error(pool_count, idx));
    };
    let Some(info) = pool.decommission.as_mut() else {
        return Err(decommission_metadata_not_initialized_error("record decommission unresolved entry"));
    };
    let existing = info.unresolved_entries.iter_mut().find(|existing| {
        existing.bucket == entry.bucket
            && existing.object == entry.object
            && existing.pool_index == entry.pool_index
            && existing.set_index == entry.set_index
            && existing.source_generation == entry.source_generation
    });
    let changed = match existing {
        Some(existing) if existing == &entry => false,
        Some(existing) => {
            *existing = entry;
            true
        }
        None => {
            info.unresolved_entries.push(entry);
            true
        }
    };
    if changed {
        pool.last_update = OffsetDateTime::now_utc();
    }
    Ok(changed)
}

type DecommissionUnresolvedEntryIdentity = (usize, String, String);

fn decommission_unresolved_entry_identity(entry: &DecommissionUnresolvedEntry) -> DecommissionUnresolvedEntryIdentity {
    (entry.set_index, entry.bucket.clone(), entry.object.clone())
}

fn reconcile_decommission_unresolved_entries_for_completion(
    meta: &mut PoolMeta,
    idx: usize,
    verified_generation: Option<OffsetDateTime>,
    verified_entries: Option<&[DecommissionUnresolvedEntry]>,
) -> Result<()> {
    let pool_count = meta.pools.len();
    let Some(pool) = meta.pools.get(idx) else {
        return Err(invalid_decommission_pool_index_error(pool_count, idx));
    };
    let Some(info) = pool.decommission.as_ref() else {
        return Err(decommission_metadata_not_initialized_error("reconcile decommission unresolved entries"));
    };
    if info.unresolved_entries.is_empty() {
        return Ok(());
    }

    let Some(generation) = verified_generation else {
        return Err(Error::other(format!(
            "failed to complete decommission for pool {idx}: {} unresolved listing entries remain",
            info.unresolved_entries.len()
        )));
    };
    ensure_decommission_generation(meta, idx, generation)?;
    if info
        .unresolved_entries
        .iter()
        .any(|entry| entry.source_generation != generation)
    {
        return Err(Error::other(format!(
            "failed to complete decommission for pool {idx}: unresolved listing ledger contains a different generation"
        )));
    }

    let verified_entries = verified_entries.unwrap_or_default();
    let unverified_count = info
        .unresolved_entries
        .iter()
        .filter(|entry| !verified_entries.contains(entry))
        .count();
    if unverified_count > 0 {
        return Err(Error::other(format!(
            "failed to complete decommission for pool {idx}: {unverified_count} unresolved listing entries were not individually verified"
        )));
    }

    let Some(info) = meta.pools.get_mut(idx).and_then(|pool| pool.decommission.as_mut()) else {
        return Err(decommission_metadata_not_initialized_error("reconcile decommission unresolved entries"));
    };
    info.unresolved_entries.clear();
    Ok(())
}

#[cfg(test)]
async fn run_decommission_side_effect<T, E, F, Fut>(
    rx: &CancellationToken,
    operation_gate: &Arc<tokio::sync::RwLock<()>>,
    operation: F,
) -> std::result::Result<T, E>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = std::result::Result<T, E>>,
    E: From<Error>,
{
    let _operation_guard = tokio::select! {
        biased;
        _ = rx.cancelled() => return Err(Error::OperationCanceled.into()),
        guard = operation_gate.read() => guard,
    };

    if rx.is_cancelled() {
        return Err(Error::OperationCanceled.into());
    }

    let result = operation().await;
    if rx.is_cancelled() {
        return Err(Error::OperationCanceled.into());
    }
    result
}

fn track_decommission_current_object_stage(
    meta: &mut PoolMeta,
    idx: usize,
    bucket: &str,
    object: &str,
    stage: &str,
) -> Result<()> {
    let pool_count = meta.pools.len();
    ensure_valid_decommission_pool_index(pool_count, idx)?;

    let Some(pool) = meta.pools.get_mut(idx) else {
        return Err(invalid_decommission_pool_index_error(pool_count, idx));
    };
    let Some(info) = pool.decommission.as_mut() else {
        return Err(decommission_metadata_not_initialized_error("track decommission current object"));
    };

    info.object = object.to_string();
    info.bucket = bucket.to_string();
    info.stage = stage.to_string();
    Ok(())
}

fn track_decommission_current_object(meta: &mut PoolMeta, idx: usize, bucket: &str, object: &str) -> Result<()> {
    track_decommission_current_object_stage(meta, idx, bucket, object, "")
}

fn resolve_decommission_update_after_result(result: Result<bool>) -> Result<bool> {
    result.map_err(|err| Error::other(format!("decommission metadata update failed: {err}")))
}

fn resolve_decommission_progress_save_result(result: Result<()>) -> Option<Error> {
    result
        .err()
        .map(|err| Error::other(format!("decommission progress save failed: {err}")))
}

fn resolve_decommission_preflight_heal_result<T>(bucket: &str, result: Result<T>) -> Result<T> {
    result.map_err(|err| Error::other(format!("decommission preflight heal failed for bucket {bucket}: {err}")))
}

fn resolve_decommission_optional_bucket_config_result<T>(bucket: &str, stage: &str, result: Result<T>) -> Result<Option<T>> {
    match result {
        Ok(config) => Ok(Some(config)),
        Err(Error::ConfigNotFound) => Ok(None),
        Err(err) => Err(Error::other(format!(
            "decommission {stage} config load failed for bucket {bucket}: {err}"
        ))),
    }
}

fn resolve_decommission_entry_cleanup_delete_result<T>(result: Result<T>, bucket: &str, object_name: &str) -> Result<()> {
    match result {
        Ok(_) => Ok(()),
        Err(err) if is_err_object_not_found(&err) || is_err_version_not_found(&err) => Ok(()),
        Err(err) => Err(Error::other(format!(
            "decommission cleanup_delete_object failed for {bucket}/{object_name}: {err}"
        ))),
    }
}

fn resolve_decommission_entry_reload_result(result: Result<()>, bucket: &str, object_name: &str) -> Result<()> {
    result.map_err(|err| Error::other(format!("decommission reload_pool_meta failed for {bucket}/{object_name}: {err}")))
}

fn resolve_decommission_terminal_mark_result(result: Result<()>, stage: &str, pool_label: &str) -> Result<()> {
    result.map_err(|err| Error::other(format!("decommission terminal mark {stage} failed for pool {pool_label}: {err}")))
}

fn resolve_decommission_terminal_mark_after_error_result(result: Result<()>, idx: usize, primary_err: &Error) -> Result<()> {
    result.map_err(|err| {
        Error::other(format!(
            "decommission terminal mark failed after background error on pool {idx}: {primary_err}; mark error: {err}"
        ))
    })
}

fn observe_decommission_terminal_reload_result(result: Result<()>, stage: &str) -> Option<Error> {
    result
        .err()
        .map(|err| Error::other(format!("decommission terminal pool meta reload failed during {stage}: {err}")))
}

fn decommission_item_size<T>(size: T) -> usize
where
    usize: TryFrom<T>,
{
    usize::try_from(size).unwrap_or_default()
}

fn with_decommission_entry_context<E: Display>(stage: &str, bucket: &str, object: &str, err: E) -> Error {
    Error::other(format!("decommission entry {stage} failed for bucket {bucket} object {object}: {err}"))
}

#[cfg(test)]
fn load_decommission_entry_versions(entry: &MetaCacheEntry, bucket: &str, stage: &str) -> Result<FileInfoVersions> {
    entry
        .file_info_versions(bucket)
        .map_err(|err| with_decommission_entry_context(stage, bucket, &entry.name, err))
}

fn empty_decommission_entry_versions(bucket: &str, object: &str) -> FileInfoVersions {
    FileInfoVersions {
        volume: bucket.to_string(),
        name: object.to_string(),
        versions: Vec::new(),
        ..Default::default()
    }
}

fn resolve_decommission_entry_exact_versions(
    result: Result<Option<FileInfoVersions>>,
    entry: &MetaCacheEntry,
    bucket: &str,
    stage: &str,
) -> Result<FileInfoVersions> {
    match result {
        Ok(Some(fivs)) => Ok(fivs),
        Ok(None) => Ok(empty_decommission_entry_versions(bucket, &entry.name)),
        Err(err) => Err(with_decommission_entry_context(stage, bucket, &entry.name, err)),
    }
}

async fn load_decommission_entry_exact_versions(
    set: &SetDisks,
    entry: &MetaCacheEntry,
    bucket: &str,
    stage: &str,
) -> Result<FileInfoVersions> {
    resolve_decommission_entry_exact_versions(set.load_file_info_versions_exact(bucket, &entry.name).await, entry, bucket, stage)
}

fn resolve_decommission_check_after_list_result(list_result: Result<()>, entry_error: Option<Error>) -> Result<()> {
    match list_result {
        Ok(()) => entry_error.map_or(Ok(()), Err),
        Err(list_err) => resolve_decommission_listing_error(Some(list_err), entry_error).map_or(Ok(()), Err),
    }
}

fn resolve_decommission_listing_error(listing_error: Option<Error>, entry_error: Option<Error>) -> Option<Error> {
    match (listing_error, entry_error) {
        (Some(listing_error), Some(entry_error)) if is_err_operation_canceled(&listing_error) => Some(entry_error),
        (Some(listing_error), Some(entry_error)) if is_err_operation_canceled(&entry_error) => Some(listing_error),
        (Some(listing_error), _) => Some(listing_error),
        (None, entry_error) => entry_error,
    }
}

fn decommission_unresolved_listing_error(entry: &DecommissionUnresolvedEntry) -> Error {
    Error::other(format!(
        "decommission listing could not resolve metadata for {bucket}/{object} on pool {pool_index} set {set_index} ({candidate_count} candidate(s), {disk_error_count} disk error(s))",
        bucket = entry.bucket,
        object = entry.object,
        pool_index = entry.pool_index,
        set_index = entry.set_index,
        candidate_count = entry.candidate_count,
        disk_error_count = entry.disk_error_count,
    ))
}

// The unresolved-entry payload carries listing context by design; boxing it
// would churn every caller without changing behavior.
#[allow(clippy::result_large_err)]
#[allow(clippy::too_many_arguments)]
fn resolve_decommission_partial_listing_entry(
    entries: MetaCacheEntries,
    resolver: MetadataResolutionParams,
    bucket: &str,
    prefix: &str,
    disk_error_count: usize,
    pool_index: usize,
    set_index: usize,
    source_generation: OffsetDateTime,
) -> std::result::Result<MetaCacheEntry, DecommissionUnresolvedEntry> {
    let candidate_count = entries.as_ref().iter().flatten().count();
    if let Some(entry) = entries.resolve(resolver) {
        return Ok(entry);
    }

    let object = entries
        .as_ref()
        .iter()
        .flatten()
        .map(|entry| entry.name.as_str())
        .next()
        .unwrap_or(prefix)
        .to_string();
    Err(DecommissionUnresolvedEntry {
        bucket: bucket.to_string(),
        object,
        candidate_count,
        disk_error_count,
        pool_index,
        set_index,
        source_generation,
        observed_at: OffsetDateTime::now_utc(),
        reason: "metadata_resolution_failed".to_string(),
    })
}

fn validate_decommission_durable_ilm_copy(
    path: &str,
    source_record: &ValidatedDurableIlmRecord,
    target: &[u8],
) -> Result<ValidatedDurableIlmRecord> {
    let target_record = validate_durable_ilm_record(path, target).map_err(|err| {
        Error::other(format!(
            "target durable ILM record is invalid at path `{path}` {}: {err}",
            source_record.context()
        ))
    })?;
    source_record
        .checkpoint
        .validate_successor(&target_record.checkpoint)
        .map_err(|err| {
            Error::other(format!(
                "target durable ILM record generation mismatch at path `{path}` {}: {err}",
                source_record.context()
            ))
        })?;
    Ok(target_record)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DecommissionDurableIlmReceipt {
    source_path: String,
    namespace: String,
    id_kind: String,
    id: String,
    checkpoint: DurableIlmRecordCheckpoint,
    terminal_checkpoint: Option<DurableIlmRecordCheckpoint>,
}

impl DecommissionDurableIlmReceipt {
    fn new(path: &str, record: &ValidatedDurableIlmRecord) -> Self {
        Self {
            source_path: path.to_string(),
            namespace: record.namespace.to_string(),
            id_kind: record.id_kind.to_string(),
            id: record.id.clone(),
            checkpoint: record.checkpoint.clone(),
            terminal_checkpoint: None,
        }
    }

    fn context(&self) -> String {
        format!("namespace `{}` {} `{}`", self.namespace, self.id_kind, self.id)
    }

    fn validate(&self) -> Result<()> {
        let namespace = classify_durable_ilm_record(&self.source_path)?
            .ok_or_else(|| Error::other(format!("receipt source path `{}` is not a durable ILM record", self.source_path)))?;
        if namespace.name != self.namespace {
            return Err(Error::other(format!(
                "receipt namespace `{}` does not match source path `{}`",
                self.namespace, self.source_path
            )));
        }
        if self.id_kind.is_empty() || self.id.is_empty() {
            return Err(Error::other(format!(
                "receipt identity is missing for source path `{}`",
                self.source_path
            )));
        }
        if !is_sha256_checksum(self.checkpoint.content_sha256()) {
            return Err(Error::other(format!(
                "receipt target checksum is invalid for source path `{}` {}",
                self.source_path,
                self.context()
            )));
        }
        if let Some(terminal_checkpoint) = &self.terminal_checkpoint {
            self.checkpoint.validate_successor(terminal_checkpoint).map_err(|err| {
                Error::other(format!(
                    "receipt terminal checkpoint is invalid for source path `{}` {}: {err}",
                    self.source_path,
                    self.context()
                ))
            })?;
        }
        Ok(())
    }

    fn encode(&self) -> Result<Vec<u8>> {
        let mut receipt = self.clone();
        receipt.checkpoint = receipt.checkpoint.compacted()?;
        receipt.terminal_checkpoint = receipt
            .terminal_checkpoint
            .as_ref()
            .map(DurableIlmRecordCheckpoint::compacted)
            .transpose()?;
        receipt.validate()?;
        let receipt_bytes = serde_json::to_vec(&receipt)?;
        let persisted = PersistedDecommissionDurableIlmReceipt {
            schema: DECOMMISSION_DURABLE_ILM_RECEIPT_SCHEMA.to_string(),
            content_sha256: hex_sha256(&receipt_bytes, ToOwned::to_owned),
            receipt,
        };
        let encoded = serde_json::to_vec(&persisted)?;
        if encoded.len() > DECOMMISSION_DURABLE_ILM_RECEIPT_MAX_SIZE {
            return Err(Error::other(format!(
                "durable ILM receipt exceeds maximum size for source path `{}` {}",
                self.source_path,
                self.context()
            )));
        }
        Ok(encoded)
    }

    fn decode(data: &[u8]) -> Result<Self> {
        if data.len() > DECOMMISSION_DURABLE_ILM_RECEIPT_MAX_SIZE {
            return Err(Error::other("durable ILM receipt exceeds maximum size"));
        }
        let persisted: PersistedDecommissionDurableIlmReceipt = serde_json::from_slice(data)?;
        if persisted.schema != DECOMMISSION_DURABLE_ILM_RECEIPT_SCHEMA {
            return Err(Error::other(format!("unsupported durable ILM receipt schema `{}`", persisted.schema)));
        }
        if !is_sha256_checksum(&persisted.content_sha256) {
            return Err(Error::other("durable ILM receipt checksum is invalid"));
        }
        let receipt_bytes = serde_json::to_vec(&persisted.receipt)?;
        let actual_checksum = hex_sha256(&receipt_bytes, ToOwned::to_owned);
        if persisted.content_sha256 != actual_checksum {
            return Err(Error::other("durable ILM receipt checksum mismatch"));
        }
        persisted.receipt.validate()?;
        Ok(persisted.receipt)
    }
}

fn merge_decommission_durable_ilm_receipts(
    existing: &DecommissionDurableIlmReceipt,
    incoming: &DecommissionDurableIlmReceipt,
) -> Result<DecommissionDurableIlmReceipt> {
    if existing.source_path != incoming.source_path
        || existing.namespace != incoming.namespace
        || existing.id_kind != incoming.id_kind
        || existing.id != incoming.id
    {
        return Err(Error::other(format!(
            "durable ILM receipt identity conflict for source path `{}` {}; incoming {}",
            existing.source_path,
            existing.context(),
            incoming.context()
        )));
    }

    let checkpoint =
        if existing.checkpoint == incoming.checkpoint || incoming.checkpoint.validate_successor(&existing.checkpoint).is_ok() {
            existing.checkpoint.clone()
        } else {
            existing.checkpoint.validate_successor(&incoming.checkpoint).map_err(|err| {
                Error::other(format!(
                    "durable ILM receipt checkpoint conflict for source path `{}` {}: {err}",
                    existing.source_path,
                    existing.context()
                ))
            })?;
            incoming.checkpoint.clone()
        };
    let terminal_checkpoint = match (&existing.terminal_checkpoint, &incoming.terminal_checkpoint) {
        (Some(existing_terminal), Some(incoming_terminal)) if existing_terminal == incoming_terminal => {
            Some(existing_terminal.clone())
        }
        (Some(existing_terminal), Some(incoming_terminal)) if incoming_terminal.validate_successor(existing_terminal).is_ok() => {
            Some(existing_terminal.clone())
        }
        (Some(existing_terminal), Some(incoming_terminal)) => {
            existing_terminal.validate_successor(incoming_terminal).map_err(|err| {
                Error::other(format!(
                    "durable ILM receipt terminal checkpoint conflict for source path `{}` {}: {err}",
                    existing.source_path,
                    existing.context()
                ))
            })?;
            Some(incoming_terminal.clone())
        }
        (Some(existing_terminal), None) => Some(existing_terminal.clone()),
        (None, Some(incoming_terminal)) => Some(incoming_terminal.clone()),
        (None, None) => None,
    };
    let merged = DecommissionDurableIlmReceipt {
        source_path: existing.source_path.clone(),
        namespace: existing.namespace.clone(),
        id_kind: existing.id_kind.clone(),
        id: existing.id.clone(),
        checkpoint,
        terminal_checkpoint,
    };
    merged.validate()?;
    Ok(merged)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PersistedDecommissionDurableIlmReceipt {
    schema: String,
    content_sha256: String,
    receipt: DecommissionDurableIlmReceipt,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DecommissionDurableIlmManifest {
    schema: String,
    run_token: String,
    receipt_count: u64,
    receipt_paths_sha256: String,
}

impl DecommissionDurableIlmManifest {
    fn new(run_token: &str, receipt_paths: &[String]) -> Result<Self> {
        let manifest = Self {
            schema: DECOMMISSION_DURABLE_ILM_MANIFEST_SCHEMA.to_string(),
            run_token: run_token.to_string(),
            receipt_count: u64::try_from(receipt_paths.len())
                .map_err(|_| Error::other("durable ILM expected manifest receipt count exceeds u64"))?,
            receipt_paths_sha256: decommission_durable_ilm_manifest_paths_sha256(receipt_paths)?,
        };
        manifest.validate(run_token, receipt_paths)?;
        Ok(manifest)
    }

    fn validate(&self, run_token: &str, receipt_paths: &[String]) -> Result<()> {
        if self.schema != DECOMMISSION_DURABLE_ILM_MANIFEST_SCHEMA {
            return Err(Error::other(format!(
                "unsupported durable ILM expected manifest schema `{}`",
                self.schema
            )));
        }
        if self.run_token != run_token || !is_sha256_checksum(&self.run_token) {
            return Err(Error::other("durable ILM expected manifest run token is invalid"));
        }
        let receipt_count = u64::try_from(receipt_paths.len())
            .map_err(|_| Error::other("durable ILM expected manifest receipt count exceeds u64"))?;
        if self.receipt_count != receipt_count {
            return Err(Error::other(format!(
                "durable ILM expected manifest receipt count mismatch: expected {}, found {receipt_count}",
                self.receipt_count
            )));
        }
        if !is_sha256_checksum(&self.receipt_paths_sha256)
            || self.receipt_paths_sha256 != decommission_durable_ilm_manifest_paths_sha256(receipt_paths)?
        {
            return Err(Error::other("durable ILM expected manifest receipt paths checksum mismatch"));
        }
        Ok(())
    }

    fn encode(&self) -> Result<Vec<u8>> {
        let encoded = serde_json::to_vec(self)?;
        if encoded.len() > DECOMMISSION_DURABLE_ILM_MANIFEST_MAX_SIZE {
            return Err(Error::other("durable ILM expected manifest exceeds maximum size"));
        }
        Ok(encoded)
    }

    fn decode(data: &[u8], run_token: &str, receipt_paths: &[String]) -> Result<Self> {
        if data.len() > DECOMMISSION_DURABLE_ILM_MANIFEST_MAX_SIZE {
            return Err(Error::other("durable ILM expected manifest exceeds maximum size"));
        }
        let manifest: Self = serde_json::from_slice(data)?;
        manifest.validate(run_token, receipt_paths)?;
        Ok(manifest)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DecommissionDurableIlmReceiptLocator {
    run_token: String,
    source_path: String,
    id_kind: String,
    id: String,
}

impl DecommissionDurableIlmReceiptLocator {
    fn context(&self) -> String {
        format!("source path `{}` {} `{}`", self.source_path, self.id_kind, self.id)
    }
}

fn decommission_durable_ilm_receipt_run_token(cmd_line: &str, start_time: OffsetDateTime) -> String {
    let identity = format!("{cmd_line}\0{}", start_time.unix_timestamp_nanos());
    hex_sha256(identity.as_bytes(), ToOwned::to_owned)
}

fn decommission_durable_ilm_receipt_run_prefix(run_token: &str) -> String {
    format!("{DECOMMISSION_DURABLE_ILM_RECEIPT_ROOT}/{run_token}/")
}

fn decommission_durable_ilm_receipt_path(run_token: &str, source_path: &str, id_kind: &str, id: &str) -> String {
    format!(
        "{}{}/{}/{}.json",
        decommission_durable_ilm_receipt_run_prefix(run_token),
        source_path,
        id_kind,
        id
    )
}

fn decommission_durable_ilm_manifest_path(run_token: &str) -> String {
    format!("{DECOMMISSION_DURABLE_ILM_MANIFEST_ROOT}/{run_token}.json")
}

fn decommission_durable_ilm_manifest_paths_sha256(receipt_paths: &[String]) -> Result<String> {
    let mut sorted_paths = receipt_paths.iter().map(String::as_str).collect::<Vec<_>>();
    sorted_paths.sort_unstable();
    let encoded = serde_json::to_vec(&sorted_paths)?;
    Ok(hex_sha256(&encoded, ToOwned::to_owned))
}

fn parse_decommission_durable_ilm_receipt_path(path: &str) -> Result<DecommissionDurableIlmReceiptLocator> {
    let prefix = format!("{DECOMMISSION_DURABLE_ILM_RECEIPT_ROOT}/");
    let suffix = path
        .strip_prefix(&prefix)
        .ok_or_else(|| Error::other(format!("durable ILM receipt path `{path}` has the wrong root")))?;
    let (run_token, record_path) = suffix
        .split_once('/')
        .ok_or_else(|| Error::other(format!("durable ILM receipt path `{path}` is missing its record path")))?;
    let mut parts = record_path.rsplitn(3, '/');
    let id = parts
        .next()
        .and_then(|file| file.strip_suffix(".json"))
        .filter(|id| !id.is_empty())
        .ok_or_else(|| Error::other(format!("durable ILM receipt path `{path}` is missing its record id")))?;
    let id_kind = parts
        .next()
        .filter(|id_kind| matches!(*id_kind, "operation_id" | "transaction_id" | "job_id"))
        .ok_or_else(|| Error::other(format!("durable ILM receipt path `{path}` has an invalid id kind")))?;
    let source_path = parts
        .next()
        .filter(|source_path| !source_path.is_empty())
        .ok_or_else(|| Error::other(format!("durable ILM receipt path `{path}` is missing its source path")))?;
    if !is_sha256_checksum(run_token) {
        return Err(Error::other(format!("durable ILM receipt path `{path}` has an invalid run token")));
    }
    match id_kind {
        "operation_id" if !is_sha256_checksum(id) => {
            return Err(Error::other(format!("durable ILM receipt path `{path}` has an invalid operation id")));
        }
        "transaction_id" | "job_id" if uuid::Uuid::parse_str(id).is_err() => {
            return Err(Error::other(format!("durable ILM receipt path `{path}` has an invalid UUID")));
        }
        _ => {}
    }
    classify_durable_ilm_record(source_path)?
        .ok_or_else(|| Error::other(format!("durable ILM receipt path `{path}` does not identify a durable ILM source path")))?;
    Ok(DecommissionDurableIlmReceiptLocator {
        run_token: run_token.to_string(),
        source_path: source_path.to_string(),
        id_kind: id_kind.to_string(),
        id: id.to_string(),
    })
}

fn resolve_decommission_pool_meta_reload_result(result: Result<()>, stage: &str) -> Result<()> {
    result.map_err(|err| Error::other(format!("decommission pool meta reload failed during {stage}: {err}")))
}

fn apply_decommission_status_space_info(mut pool_info: PoolStatus, space_info: PoolSpaceInfo) -> PoolStatus {
    match pool_info.decommission.as_mut() {
        Some(d) => {
            d.total_size = space_info.total;
            d.current_size = space_info.free;
        }
        None => {
            pool_info.decommission = Some(PoolDecommissionInfo {
                total_size: space_info.total,
                current_size: space_info.free,
                ..Default::default()
            });
        }
    }

    pool_info
}

fn should_replace_pool_status_for_status_refresh(
    current: Option<&PoolStatus>,
    persisted: &PoolStatus,
    has_active_worker: bool,
) -> bool {
    let Some(current) = current else {
        return true;
    };

    !has_active_worker && persisted.last_update > current.last_update
}

fn pool_decommission_movement_snapshot(
    info: Option<&PoolDecommissionInfo>,
) -> (bool, bool, bool, bool, bool, Option<OffsetDateTime>) {
    info.map(|info| {
        (
            info.has_decommission_state(),
            info.complete,
            info.failed,
            info.canceled,
            info.queued,
            info.start_time,
        )
    })
    .unwrap_or_default()
}

pub(crate) fn pool_meta_movement_snapshot_changed(before: &PoolMeta, after: &PoolMeta) -> bool {
    before.pools.len() != after.pools.len()
        || before.pools.iter().zip(after.pools.iter()).any(|(before, after)| {
            pool_decommission_movement_snapshot(before.decommission.as_ref())
                != pool_decommission_movement_snapshot(after.decommission.as_ref())
        })
}

/// Merges a persisted pool metadata snapshot into `current` monotonically:
/// a pool entry is replaced only when no active worker covers it and the
/// snapshot is strictly newer, so delayed snapshots never roll back local
/// queued/terminal progressions. Returns whether any entry was replaced or
/// appended.
pub(crate) fn merge_pool_status_refresh(current: &mut PoolMeta, persisted: PoolMeta, active_workers: &[bool]) -> bool {
    let observed_version = current.version.max(persisted.version);
    if persisted.pools.is_empty() {
        current.version = observed_version;
        return false;
    }

    if current.pools.is_empty() {
        *current = persisted;
        current.version = observed_version;
        return true;
    }

    current.version = observed_version;

    let mut merged_newer = false;
    for (idx, persisted_pool) in persisted.pools.into_iter().enumerate() {
        if persisted_pool.id != idx {
            continue;
        }

        let has_active_worker = active_workers.get(idx).copied().unwrap_or(false);
        if idx < current.pools.len() {
            if should_replace_pool_status_for_status_refresh(current.pools.get(idx), &persisted_pool, has_active_worker) {
                current.pools[idx] = persisted_pool;
                merged_newer = true;
            }
        } else if idx == current.pools.len() && !has_active_worker {
            current.pools.push(persisted_pool);
            merged_newer = true;
        }
    }
    merged_newer
}

fn merge_pool_meta_updates_for_save(
    persisted: &mut PoolMeta,
    current: &PoolMeta,
    indices: &[usize],
    operation: &str,
) -> Result<()> {
    if persisted.pools.is_empty() {
        *persisted = current.clone();
        return Ok(());
    }
    if persisted.version != current.version {
        return Err(Error::other(format!(
            "{operation}: pool metadata version changed from {} to {}",
            current.version, persisted.version
        )));
    }

    for &idx in indices {
        let current_pool = current
            .pools
            .get(idx)
            .ok_or_else(|| invalid_decommission_pool_index_error(current.pools.len(), idx))?;
        let persisted_count = persisted.pools.len();
        let persisted_pool = persisted
            .pools
            .get_mut(idx)
            .ok_or_else(|| invalid_decommission_pool_index_error(persisted_count, idx))?;
        if current_pool.id != idx || persisted_pool.id != idx || current_pool.cmd_line != persisted_pool.cmd_line {
            return Err(Error::other(format!("{operation}: pool metadata layout changed for pool {idx}")));
        }
        if current_pool.decommission.is_none()
            && persisted_pool.decommission.as_ref().is_some_and(|info| {
                info.has_decommission_state() && is_decommission_active(info.complete, info.failed, info.canceled)
            })
        {
            record_pool_meta_stale_write_rejection("nonterminal_decommission_clear");
            return Err(Error::other(format!(
                "{operation}: stale pool metadata update rejected for pool {idx}; persisted active or queued decommission cannot be cleared"
            )));
        }
        if current_pool.last_update < persisted_pool.last_update {
            record_pool_meta_stale_write_rejection("older_pool_revision");
            return Err(Error::other(format!(
                "{operation}: stale pool metadata update rejected for pool {idx}; persisted update is newer"
            )));
        }
        if let (Some(persisted_info), Some(current_info)) =
            (persisted_pool.decommission.as_ref(), current_pool.decommission.as_ref())
        {
            let persisted_terminal = (persisted_info.complete, persisted_info.failed, persisted_info.canceled);
            let current_terminal = (current_info.complete, current_info.failed, current_info.canceled);
            if persisted_terminal != (false, false, false) && persisted_terminal != current_terminal {
                record_pool_meta_stale_write_rejection("terminal_state_regression");
                return Err(Error::other(format!(
                    "{operation}: stale pool metadata update rejected for pool {idx}; terminal state cannot be replaced"
                )));
            }
            if current_info.items_decommissioned < persisted_info.items_decommissioned
                || current_info.items_decommission_failed < persisted_info.items_decommission_failed
                || current_info.bytes_done < persisted_info.bytes_done
                || current_info.bytes_failed < persisted_info.bytes_failed
            {
                record_pool_meta_stale_write_rejection("progress_regression");
                return Err(Error::other(format!(
                    "{operation}: stale pool metadata update rejected for pool {idx}; durable progress cannot decrease"
                )));
            }
        }
        *persisted_pool = current_pool.clone();
    }

    Ok(())
}

fn publish_pool_meta_updates(current: &mut PoolMeta, saved: &PoolMeta, indices: &[usize]) {
    current.version = current.version.max(saved.version);
    for &idx in indices {
        let Some(saved_pool) = saved.pools.get(idx) else {
            continue;
        };
        let Some(current_pool) = current.pools.get_mut(idx) else {
            continue;
        };
        if current_pool.id == idx && saved_pool.id == idx && current_pool.cmd_line == saved_pool.cmd_line {
            *current_pool = saved_pool.clone();
        }
    }
}

fn resolve_start_decommission_pool_meta_reload_result(result: Result<()>) -> Result<()> {
    resolve_decommission_pool_meta_reload_result(result, "start_decommission")
}

fn activation_rebalance_meta_lock_error(err: rustfs_lock::LockError) -> Error {
    match err {
        rustfs_lock::LockError::QuorumNotReached { required, achieved } => Error::NamespaceLockQuorumUnavailable {
            mode: "write",
            bucket: RUSTFS_META_BUCKET.to_string(),
            object: REBAL_META_NAME.to_string(),
            required,
            achieved,
        },
        other => Error::other(format!(
            "failed to acquire rebalance activation lock on {RUSTFS_META_BUCKET}/{REBAL_META_NAME}: {other}"
        )),
    }
}

fn activation_pool_meta_lock_error(err: rustfs_lock::LockError) -> Error {
    match err {
        rustfs_lock::LockError::QuorumNotReached { required, achieved } => Error::NamespaceLockQuorumUnavailable {
            mode: "write",
            bucket: RUSTFS_META_BUCKET.to_string(),
            object: POOL_META_NAME.to_string(),
            required,
            achieved,
        },
        other => Error::other(format!(
            "failed to acquire pool activation lock on {RUSTFS_META_BUCKET}/{POOL_META_NAME}: {other}"
        )),
    }
}

pub(crate) struct PoolRebalanceActivationFence {
    pool_meta_guard: rustfs_lock::NamespaceLockGuard,
    rebalance_meta_guard: rustfs_lock::NamespaceLockGuard,
    fleet_proof: Option<crate::services::notification_sys::CrossPoolFenceFleetProofToken>,
    #[cfg(test)]
    forced_lost: Arc<AtomicBool>,
}

impl PoolRebalanceActivationFence {
    pub(crate) fn set_fleet_proof(
        &mut self,
        fleet_proof: Option<crate::services::notification_sys::CrossPoolFenceFleetProofToken>,
    ) {
        self.fleet_proof = fleet_proof;
    }

    pub(crate) fn ensure_held(&self) -> Result<()> {
        #[cfg(test)]
        let forced_lost = self.forced_lost.load(Ordering::Acquire);
        #[cfg(not(test))]
        let forced_lost = false;
        if forced_lost || self.pool_meta_guard.is_lock_lost() || self.rebalance_meta_guard.is_lock_lost() {
            return Err(Error::other("activation lock lost before metadata commit or worker admission"));
        }
        if self
            .fleet_proof
            .as_ref()
            .is_some_and(|proof| !crate::services::notification_sys::cross_pool_fence_fleet_proof_matches(proof))
        {
            return Err(Error::other(POOL_ACTIVATION_FLEET_PROOF_EXPIRED));
        }

        Ok(())
    }

    pub(crate) fn add_namespace_lock_fence(&self, opts: &mut ObjectOptions) {
        opts.add_namespace_lock_guard(&self.pool_meta_guard);
        opts.add_namespace_lock_guard(&self.rebalance_meta_guard);
    }

    #[cfg(test)]
    fn force_lost_for_test(&self) {
        self.forced_lost.store(true, Ordering::Release);
    }
}

pub(crate) async fn acquire_pool_rebalance_activation_locks<S>(
    pool: Arc<S>,
    fleet_proof: Option<crate::services::notification_sys::CrossPoolFenceFleetProofToken>,
) -> Result<PoolRebalanceActivationFence>
where
    S: crate::storage_api_contracts::namespace::NamespaceLocking<
            Error = Error,
            NamespaceLock = rustfs_lock::NamespaceLockWrapper,
        >,
{
    // Activation lock order is always pool.bin -> rebalance.bin.
    let pool_meta_lock = pool.new_ns_lock(RUSTFS_META_BUCKET, POOL_META_NAME).await?;
    let pool_meta_guard = pool_meta_lock
        .get_write_lock(get_lock_acquire_timeout())
        .await
        .map_err(activation_pool_meta_lock_error)?;
    let rebalance_meta_lock = pool.new_ns_lock(RUSTFS_META_BUCKET, REBAL_META_NAME).await?;
    let rebalance_meta_guard = rebalance_meta_lock
        .get_write_lock(get_lock_acquire_timeout())
        .await
        .map_err(activation_rebalance_meta_lock_error)?;

    Ok(PoolRebalanceActivationFence {
        pool_meta_guard,
        rebalance_meta_guard,
        fleet_proof,
        #[cfg(test)]
        forced_lost: Arc::new(AtomicBool::new(false)),
    })
}

pub(crate) async fn acquire_pool_activation_fleet_proof(
    ctx: &crate::runtime::instance::InstanceContext,
) -> Result<Option<crate::services::notification_sys::CrossPoolFenceFleetProofToken>> {
    if !ctx.is_dist_erasure().await {
        return Ok(None);
    }
    crate::services::notification_sys::acquire_cross_pool_fence_fleet_proof()
        .map(Some)
        .ok_or_else(|| Error::other(POOL_ACTIVATION_FLEET_PROOF_REQUIRED))
}

pub(crate) fn is_pool_activation_fleet_proof_error(err: &Error) -> bool {
    // Save-stage helpers add context by formatting the original error, so the
    // marker may be nested in the display string. Restrict matching to the
    // `Error::other` I/O shape used by this activation path.
    matches!(err, Error::Io(io_error) if io_error.kind() == std::io::ErrorKind::Other && {
        let message = io_error.to_string();
        message.contains(POOL_ACTIVATION_FLEET_PROOF_REQUIRED) || message.contains(POOL_ACTIVATION_FLEET_PROOF_EXPIRED)
    })
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PoolActivationStartKind {
    Rebalance,
    Decommission,
}

#[cfg(test)]
struct PoolActivationDurableSaveBarrierState {
    pool_key: usize,
    arrived: tokio::sync::Notify,
    release: tokio::sync::Notify,
    force_fence_loss: AtomicBool,
}

#[cfg(test)]
static POOL_ACTIVATION_DURABLE_SAVE_BARRIER: std::sync::OnceLock<
    std::sync::Mutex<Option<Arc<PoolActivationDurableSaveBarrierState>>>,
> = std::sync::OnceLock::new();

#[cfg(test)]
pub(crate) struct PoolActivationDurableSaveBarrier {
    state: Arc<PoolActivationDurableSaveBarrierState>,
}

#[cfg(test)]
fn pool_activation_test_pool_key<S>(pool: &Arc<S>) -> usize {
    Arc::as_ptr(pool).cast::<()>() as usize
}

#[cfg(test)]
impl PoolActivationDurableSaveBarrier {
    pub(crate) fn install<S>(pool: &Arc<S>) -> Self {
        let state = Arc::new(PoolActivationDurableSaveBarrierState {
            pool_key: pool_activation_test_pool_key(pool),
            arrived: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
            force_fence_loss: AtomicBool::new(false),
        });
        let mut barrier = POOL_ACTIVATION_DURABLE_SAVE_BARRIER
            .get_or_init(|| std::sync::Mutex::new(None))
            .lock()
            .expect("pool activation durable save barrier should not be poisoned");
        assert!(barrier.is_none(), "pool activation durable save barrier must be unique");
        *barrier = Some(Arc::clone(&state));
        Self { state }
    }

    pub(crate) async fn wait_until_paused(&self) {
        tokio::time::timeout(std::time::Duration::from_secs(30), self.state.arrived.notified())
            .await
            .expect("activation should reach the post-durable-save barrier");
    }

    pub(crate) fn release_after_fence_loss(&self) {
        self.state.force_fence_loss.store(true, Ordering::Release);
        self.state.release.notify_one();
    }

    pub(crate) fn release_without_fence_loss(&self) {
        self.state.release.notify_one();
    }
}

#[cfg(test)]
impl Drop for PoolActivationDurableSaveBarrier {
    fn drop(&mut self) {
        self.state.release.notify_one();
        let mut barrier = POOL_ACTIVATION_DURABLE_SAVE_BARRIER
            .get_or_init(|| std::sync::Mutex::new(None))
            .lock()
            .expect("pool activation durable save barrier should not be poisoned");
        if barrier.as_ref().is_some_and(|state| Arc::ptr_eq(state, &self.state)) {
            *barrier = None;
        }
    }
}

#[cfg(test)]
pub(crate) async fn pause_pool_activation_after_durable_save<S>(pool: &Arc<S>, fence: &PoolRebalanceActivationFence) {
    let pool_key = pool_activation_test_pool_key(pool);
    let barrier = {
        let mut barrier = POOL_ACTIVATION_DURABLE_SAVE_BARRIER
            .get_or_init(|| std::sync::Mutex::new(None))
            .lock()
            .expect("pool activation durable save barrier should not be poisoned");
        if barrier.as_ref().is_some_and(|state| state.pool_key == pool_key) {
            barrier.take()
        } else {
            None
        }
    };
    if let Some(barrier) = barrier {
        barrier.arrived.notify_one();
        barrier.release.notified().await;
        if barrier.force_fence_loss.load(Ordering::Acquire) {
            fence.force_lost_for_test();
        }
    }
}

#[cfg(test)]
struct PoolActivationStartProbeState {
    kind: PoolActivationStartKind,
    attempted: std::sync::atomic::AtomicBool,
    notify: tokio::sync::Notify,
}

#[cfg(test)]
static POOL_ACTIVATION_START_PROBES: std::sync::OnceLock<std::sync::Mutex<Vec<Arc<PoolActivationStartProbeState>>>> =
    std::sync::OnceLock::new();

#[cfg(test)]
pub(crate) struct PoolActivationStartProbe {
    state: Arc<PoolActivationStartProbeState>,
}

#[cfg(test)]
impl PoolActivationStartProbe {
    pub(crate) fn install(kind: PoolActivationStartKind) -> Self {
        let state = Arc::new(PoolActivationStartProbeState {
            kind,
            attempted: std::sync::atomic::AtomicBool::new(false),
            notify: tokio::sync::Notify::new(),
        });
        POOL_ACTIVATION_START_PROBES
            .get_or_init(|| std::sync::Mutex::new(Vec::new()))
            .lock()
            .expect("pool activation start probe should not be poisoned")
            .push(Arc::clone(&state));
        Self { state }
    }

    pub(crate) async fn wait_until_attempted(&self) {
        while !self.state.attempted.load(Ordering::Acquire) {
            self.state.notify.notified().await;
        }
    }
}

#[cfg(test)]
impl Drop for PoolActivationStartProbe {
    fn drop(&mut self) {
        let mut probes = POOL_ACTIVATION_START_PROBES
            .get_or_init(|| std::sync::Mutex::new(Vec::new()))
            .lock()
            .expect("pool activation start probe should not be poisoned");
        probes.retain(|state| !Arc::ptr_eq(state, &self.state));
    }
}

#[cfg(test)]
pub(crate) fn observe_pool_activation_start_attempt(kind: PoolActivationStartKind) {
    let probes = POOL_ACTIVATION_START_PROBES
        .get_or_init(|| std::sync::Mutex::new(Vec::new()))
        .lock()
        .expect("pool activation start probe should not be poisoned")
        .iter()
        .filter(|state| state.kind == kind)
        .cloned()
        .collect::<Vec<_>>();
    for state in probes {
        state.attempted.store(true, Ordering::Release);
        state.notify.notify_one();
    }
}

fn rollback_decommission_pool_meta(pool_meta: &mut PoolMeta, previous_pool_meta: &PoolMeta, indices: &[usize]) {
    publish_pool_meta_updates(pool_meta, previous_pool_meta, indices);
}

#[derive(Debug)]
struct DecommissionCancelCommit {
    previous_start_time: Option<OffsetDateTime>,
    previous_queued: bool,
    previous_last_update: OffsetDateTime,
    canceled_pool: PoolStatus,
}

fn commit_decommission_cancel(pool_meta: &mut PoolMeta, idx: usize, commit: DecommissionCancelCommit) -> Result<()> {
    let pool_count = pool_meta.pools.len();
    let Some(current) = pool_meta.pools.get(idx) else {
        return Err(invalid_decommission_pool_index_error(pool_count, idx));
    };
    // A peer reload can install the saved cancel while runtime-only fields are reconstructed.
    let cancel_already_published = commit
        .canceled_pool
        .decommission
        .as_ref()
        .is_some_and(|info| info.canceled && !info.complete && !info.failed && info.start_time.is_none())
        && PersistedPoolStatus::from(current) == PersistedPoolStatus::from(&commit.canceled_pool);
    if cancel_already_published {
        return Ok(());
    }

    let matches_generation = current.id == commit.canceled_pool.id
        && current.cmd_line == commit.canceled_pool.cmd_line
        && current.decommission.as_ref().is_some_and(|info| {
            info.start_time == commit.previous_start_time
                && info.queued == commit.previous_queued
                && is_decommission_active(info.complete, info.failed, info.canceled)
                && (commit.previous_start_time.is_some() || current.last_update == commit.previous_last_update)
        });
    if !matches_generation {
        return Err(Error::other(format!(
            "failed to publish decommission cancel for pool {idx}: operation generation changed"
        )));
    }

    pool_meta.pools[idx] = commit.canceled_pool;
    Ok(())
}

fn rollback_start_decommission_pool_meta(pool_meta: &mut PoolMeta, previous_pool_meta: &PoolMeta, indices: &[usize]) {
    let active_updates = indices
        .iter()
        .filter_map(|&idx| pool_meta.pools.get(idx).map(|pool| (idx, pool.last_update)))
        .collect::<Vec<_>>();
    rollback_decommission_pool_meta(pool_meta, previous_pool_meta, indices);
    let rollback_at = OffsetDateTime::now_utc();
    for (idx, active_update) in active_updates {
        if let Some(pool) = pool_meta.pools.get_mut(idx) {
            pool.last_update = std::cmp::max(rollback_at, active_update + Duration::nanoseconds(1));
        }
    }
}

fn ensure_pool_meta_write_fence(guard: &rustfs_lock::NamespaceLockGuard, operation: &str) -> Result<()> {
    if guard.is_lock_lost() {
        return Err(Error::other(format!("{operation}: pool metadata distributed fence was lost")));
    }
    Ok(())
}

fn ensure_pool_not_left_in_cmdline_after_decommission(position: usize, cmd_line: &str, completed: bool) -> Result<()> {
    if completed {
        return Err(Error::other(format!(
            "pool({}) = {} is decommissioned, please remove from server command line",
            position + 1,
            cmd_line
        )));
    }

    Ok(())
}

fn resolve_decommission_listing_worker_result(
    set_idx: usize,
    worker_result: std::result::Result<Result<()>, tokio::task::JoinError>,
) -> Result<()> {
    worker_result.map_err(|err| Error::other(format!("decommission listing worker {set_idx} task join error: {err}")))?
}

fn should_retry_decommission_listing(err: &Error, attempt: usize, max_attempts: usize) -> bool {
    !is_err_bucket_not_found(err) && attempt + 1 < max_attempts
}

async fn wait_decommission_retry_backoff(rx: &CancellationToken, delay: std::time::Duration) -> bool {
    tokio::select! {
        _ = rx.cancelled() => true,
        _ = tokio::time::sleep(delay) => false,
    }
}

fn decommission_retry_backoff_delay(base: std::time::Duration, attempt: usize) -> std::time::Duration {
    base.saturating_mul(u32::try_from(attempt).unwrap_or(u32::MAX))
}

#[cfg(test)]
async fn run_decommission_listing_with_retry<List, ListFuture>(
    rx: CancellationToken,
    bucket: String,
    cb: ListCallback,
    pool_idx: usize,
    set_idx: usize,
    max_attempts: usize,
    list: List,
) -> Result<()>
where
    List: FnMut(ListCallback) -> ListFuture,
    ListFuture: std::future::Future<Output = Result<()>>,
{
    run_decommission_listing_with_retry_and_drain(rx, bucket, cb, pool_idx, set_idx, max_attempts, list, || async { false }).await
}

#[allow(clippy::too_many_arguments)]
async fn run_decommission_listing_with_retry_and_drain<List, ListFuture, Drain, DrainFuture>(
    rx: CancellationToken,
    bucket: String,
    cb: ListCallback,
    pool_idx: usize,
    set_idx: usize,
    max_attempts: usize,
    mut list: List,
    mut drain: Drain,
) -> Result<()>
where
    List: FnMut(ListCallback) -> ListFuture,
    ListFuture: std::future::Future<Output = Result<()>>,
    Drain: FnMut() -> DrainFuture,
    DrainFuture: std::future::Future<Output = bool>,
{
    let max_attempts = max_attempts.max(1);

    for attempt in 0..max_attempts {
        if rx.is_cancelled() {
            debug!(
                event = EVENT_DECOMMISSION_BUCKET,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_POOLS,
                pool_index = pool_idx,
                set_index = set_idx,
                bucket = %bucket,
                state = "listing_worker_cancelled",
                "Decommission listing worker cancelled"
            );
            return Ok(());
        }

        debug!(
            event = EVENT_DECOMMISSION_BUCKET,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_POOLS,
            pool_index = pool_idx,
            set_index = set_idx,
            bucket = %bucket,
            attempt = attempt + 1,
            max_attempts,
            state = "listing_started",
            "Decommission listing started"
        );

        let list_result = list(cb.clone()).await;
        if drain().await {
            return Ok(());
        }

        match list_result {
            Ok(()) => {
                debug!(
                    event = EVENT_DECOMMISSION_BUCKET,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_POOLS,
                    pool_index = pool_idx,
                    set_index = set_idx,
                    bucket = %bucket,
                    attempt = attempt + 1,
                    max_attempts,
                    state = "listing_completed",
                    "Decommission listing completed"
                );
                return Ok(());
            }
            Err(err) if is_err_bucket_not_found(&err) => {
                warn!(
                    event = EVENT_DECOMMISSION_BUCKET,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_POOLS,
                    pool_index = pool_idx,
                    set_index = set_idx,
                    bucket = %bucket,
                    attempt = attempt + 1,
                    max_attempts,
                    state = "listing_bucket_missing",
                    "Decommission listing bucket missing"
                );
                return Ok(());
            }
            Err(err) if should_retry_decommission_listing(&err, attempt, max_attempts) => {
                error!(
                    event = EVENT_DECOMMISSION_BUCKET,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_POOLS,
                    pool_index = pool_idx,
                    set_index = set_idx,
                    bucket = %bucket,
                    attempt = attempt + 1,
                    max_attempts,
                    retry_delay_ms = DECOMMISSION_LISTING_RETRY_DELAY.as_millis(),
                    state = "listing_failed_retrying",
                    error = ?err,
                    "Decommission listing failed; retrying"
                );
                if wait_decommission_retry_backoff(&rx, DECOMMISSION_LISTING_RETRY_DELAY).await {
                    debug!(
                        event = EVENT_DECOMMISSION_BUCKET,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_POOLS,
                        pool_index = pool_idx,
                        set_index = set_idx,
                        bucket = %bucket,
                        state = "listing_worker_cancelled",
                        "Decommission listing worker cancelled during retry wait"
                    );
                    return Ok(());
                }
            }
            Err(err) => {
                error!(
                    event = EVENT_DECOMMISSION_BUCKET,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_POOLS,
                    pool_index = pool_idx,
                    set_index = set_idx,
                    bucket = %bucket,
                    attempt = attempt + 1,
                    max_attempts,
                    state = "listing_failed",
                    error = ?err,
                    "Decommission listing failed"
                );
                return Err(Error::other(format!(
                    "decommission listing failed for bucket {bucket} pool {pool_idx} set {set_idx} attempt {}/{}: {err}",
                    attempt + 1,
                    max_attempts
                )));
            }
        }
    }

    Ok(())
}

fn should_count_decommission_version_complete(ignore: bool, cleanup_ignored: bool, failure: bool) -> bool {
    cleanup_ignored || (!ignore && !failure)
}

fn is_decommission_copy_cleanup_safe_error(err: &Error) -> bool {
    // DataMovementOverwriteErr only means source and destination pool resolved to
    // the same pool. Without a target equivalence check it is not cleanup-safe.
    if is_err_object_not_found(err) || is_err_version_not_found(err) {
        return true;
    }

    // A not-found surfacing from inside a data-movement stage is the same
    // condition once the wrapper is unwrapped (backlog#1827 T2).
    data_movement::data_movement_stage_source(err).is_some_and(is_decommission_copy_cleanup_safe_error)
}

fn is_decommission_target_capacity_error(err: &Error) -> bool {
    if matches!(err, Error::DiskFull | Error::StorageFull) {
        return true;
    }

    // A stage failure keeps the error it wrapped, so classify by type rather
    // than by the rendered message (backlog#1827 T2). The substring fallback
    // stays for errors that reached here through some other wrapper.
    if let Some(source) = data_movement::data_movement_stage_source(err) {
        return is_decommission_target_capacity_error(source);
    }

    let message = err.to_string();
    let disk_full = Error::DiskFull.to_string();
    let storage_full = Error::StorageFull.to_string();
    message.contains(&disk_full) || message.contains(&storage_full)
}

fn should_cleanup_decommission_source_entry(decommissioned: usize, total_versions: usize, expired: usize) -> bool {
    decommissioned.saturating_add(expired) == total_versions
}

fn should_fail_decommission_pool_after_exhausted_source_changed(exhausted_entries: usize) -> bool {
    exhausted_entries > DECOMMISSION_SOURCE_CHANGED_EXHAUSTION_LIMIT
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DecommissionEntryAttemptOutcome {
    Complete,
    SourceChanged,
}

#[cfg(test)]
pub(crate) type DecommissionTestFaultDecision = Arc<dyn Fn(&'static str, &str, &str, usize) -> bool + Send + Sync>;

#[cfg(test)]
static DECOMMISSION_TEST_FAULT_HOOK: std::sync::OnceLock<std::sync::Mutex<Option<DecommissionTestFaultDecision>>> =
    std::sync::OnceLock::new();

#[cfg(test)]
pub(crate) struct DecommissionTestFaultGuard(DecommissionTestFaultDecision);

#[cfg(test)]
impl DecommissionTestFaultGuard {
    pub(crate) fn install(decision: DecommissionTestFaultDecision) -> Self {
        let mut slot = DECOMMISSION_TEST_FAULT_HOOK
            .get_or_init(|| std::sync::Mutex::new(None))
            .lock()
            .expect("decommission test fault hook mutex should not poison");
        assert!(slot.is_none(), "decommission test fault hook must be unique");
        let stored = Arc::clone(&decision);
        *slot = Some(decision);
        Self(stored)
    }
}

#[cfg(test)]
impl Drop for DecommissionTestFaultGuard {
    fn drop(&mut self) {
        let mut slot = DECOMMISSION_TEST_FAULT_HOOK
            .get_or_init(|| std::sync::Mutex::new(None))
            .lock()
            .expect("decommission test fault hook mutex should not poison");
        if slot.as_ref().is_some_and(|decision| Arc::ptr_eq(decision, &self.0)) {
            *slot = None;
        }
    }
}

#[cfg(test)]
fn decommission_test_wrap_result<T>(
    stage: &'static str,
    bucket: &str,
    object: &str,
    attempt: usize,
    result: Result<T>,
) -> Result<T> {
    let decision = DECOMMISSION_TEST_FAULT_HOOK
        .get_or_init(|| std::sync::Mutex::new(None))
        .lock()
        .expect("decommission test fault hook mutex should not poison")
        .clone();
    if result.is_ok() && decision.is_some_and(|decision| decision(stage, bucket, object, attempt)) {
        return Err(Error::other(format!(
            "injected decommission test fault at {stage} attempt {attempt} for {bucket}/{object}"
        )));
    }
    result
}

#[cfg(test)]
pub(crate) type DecommissionCleanupMutationHook = Arc<dyn Fn(&str, &str, usize) -> BoxFuture<'static, ()> + Send + Sync>;

#[cfg(test)]
static DECOMMISSION_CLEANUP_MUTATION_HOOK: std::sync::OnceLock<std::sync::Mutex<Option<DecommissionCleanupMutationHook>>> =
    std::sync::OnceLock::new();

#[cfg(test)]
pub(crate) struct DecommissionCleanupMutationGuard(DecommissionCleanupMutationHook);

#[cfg(test)]
impl DecommissionCleanupMutationGuard {
    pub(crate) fn install(hook: DecommissionCleanupMutationHook) -> Self {
        let mut slot = DECOMMISSION_CLEANUP_MUTATION_HOOK
            .get_or_init(|| std::sync::Mutex::new(None))
            .lock()
            .expect("decommission cleanup mutation hook mutex should not poison");
        assert!(slot.is_none(), "decommission cleanup mutation hook must be unique");
        let stored = Arc::clone(&hook);
        *slot = Some(hook);
        Self(stored)
    }
}

#[cfg(test)]
impl Drop for DecommissionCleanupMutationGuard {
    fn drop(&mut self) {
        let mut slot = DECOMMISSION_CLEANUP_MUTATION_HOOK
            .get_or_init(|| std::sync::Mutex::new(None))
            .lock()
            .expect("decommission cleanup mutation hook mutex should not poison");
        if slot.as_ref().is_some_and(|hook| Arc::ptr_eq(hook, &self.0)) {
            *slot = None;
        }
    }
}

#[cfg(test)]
async fn run_decommission_cleanup_mutation_hook(bucket: &str, object: &str, attempt: usize) {
    let hook = DECOMMISSION_CLEANUP_MUTATION_HOOK
        .get_or_init(|| std::sync::Mutex::new(None))
        .lock()
        .expect("decommission cleanup mutation hook mutex should not poison")
        .clone();
    if let Some(hook) = hook {
        hook(bucket, object, attempt).await;
    }
}

const DECOMMISSION_FREE_VERSION_MIGRATED_REASON: &str = "tier_free_version_migrated";
const DECOMMISSION_FREE_VERSION_CONSUMED_REASON: &str = "tier_free_version_already_consumed";
const DECOMMISSION_FREE_VERSION_RETAINED_REASON: &str = "tier_free_version_migration_failed";
const DECOMMISSION_FREE_VERSION_SWEEP_REASON: &str = "tier_free_version_unresolved_after_decommission";
const DECOMMISSION_FREE_VERSION_DISPOSITION_REASON: &str = "tier_free_version_disposition_recorded";

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct DecommissionFreeVersionDisposition {
    migrated: usize,
    consumed: usize,
    retained: usize,
}

impl DecommissionFreeVersionDisposition {
    fn record_migrated(&mut self) {
        self.migrated += 1;
    }

    fn record_consumed(&mut self) {
        self.consumed += 1;
    }

    fn record_retained(&mut self) {
        self.retained += 1;
    }

    fn total(self) -> usize {
        self.migrated.saturating_add(self.consumed).saturating_add(self.retained)
    }
}

enum DecommissionFreeVersionAttempt {
    Migrated,
    Consumed,
    CapacityFailure(Error),
    Retry(Error),
}

fn classify_decommission_free_version_attempt(result: Result<()>) -> DecommissionFreeVersionAttempt {
    match result {
        Ok(()) => DecommissionFreeVersionAttempt::Migrated,
        Err(err) if is_decommission_copy_cleanup_safe_error(&err) => DecommissionFreeVersionAttempt::Consumed,
        Err(err) if is_decommission_target_capacity_error(&err) => DecommissionFreeVersionAttempt::CapacityFailure(err),
        Err(err) => DecommissionFreeVersionAttempt::Retry(err),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(
    dead_code,
    reason = "terminal-state classification asserted by this file's tests (backlog#1823)"
)]
enum DecommissionTerminalState {
    Completed,
    Failed,
}

#[allow(
    dead_code,
    reason = "terminal-state classification asserted by this file's tests (backlog#1823)"
)]
fn classify_decommission_terminal_state(failed_items_present: bool) -> DecommissionTerminalState {
    if failed_items_present {
        DecommissionTerminalState::Failed
    } else {
        DecommissionTerminalState::Completed
    }
}

fn should_preserve_decommission_canceled_state(meta_canceled: bool, _cancel_signal: bool) -> bool {
    meta_canceled
}

fn should_continue_decommission_queue(meta: &PoolMeta, idx: usize) -> bool {
    meta.pools
        .get(idx)
        .and_then(|pool| pool.decommission.as_ref())
        .is_some_and(|info| info.complete && !info.failed && !info.canceled)
}

fn decommission_cancel_signal_result(cancel_signal: bool) -> Result<()> {
    if cancel_signal {
        Err(StorageError::OperationCanceled)
    } else {
        Ok(())
    }
}

fn is_decommission_cancel_requested(cancel_signal: bool, pool: Option<&PoolStatus>) -> bool {
    cancel_signal
        || pool
            .and_then(|pool| pool.decommission.as_ref())
            .is_some_and(|info| info.canceled)
}

fn should_skip_canceled_decommission_routine(cancel_signal: bool, pool: Option<&PoolStatus>) -> bool {
    cancel_signal
        && pool
            .and_then(|pool| pool.decommission.as_ref())
            .is_some_and(|info| info.canceled)
}

async fn run_decommission_buckets_bounded<F>(
    rx: CancellationToken,
    buckets: Vec<DecomBucketInfo>,
    limit: usize,
    mut start_bucket: F,
) -> Result<()>
where
    F: FnMut(DecomBucketInfo, CancellationToken) -> BoxFuture<'static, Result<()>>,
{
    let mut pending = buckets.into_iter();
    let mut active: FuturesUnordered<BoxFuture<'static, Result<()>>> = FuturesUnordered::new();
    let mut first_err = None;
    let limit = limit.max(1);

    for _ in 0..limit {
        let Some(bucket) = pending.next() else {
            break;
        };

        active.push(start_bucket(bucket, rx.clone()));
    }

    while let Some(result) = active.next().await {
        if let Err(err) = result {
            rx.cancel();
            if first_err.is_none() {
                first_err = Some(err);
            }
            continue;
        }

        if first_err.is_some() || rx.is_cancelled() {
            continue;
        }

        let Some(bucket) = pending.next() else {
            continue;
        };

        active.push(start_bucket(bucket, rx.clone()));
    }

    if first_err.is_none() && rx.is_cancelled() && pending.len() > 0 {
        return decommission_cancel_signal_result(true);
    }

    if let Some(err) = first_err {
        return Err(err);
    }

    Ok(())
}

async fn run_decommission_phases<F>(
    rx: CancellationToken,
    regular_buckets: Vec<DecomBucketInfo>,
    meta_buckets: Vec<DecomBucketInfo>,
    bucket_concurrency: usize,
    mut start_bucket: F,
) -> Result<()>
where
    F: FnMut(DecomBucketInfo, CancellationToken) -> BoxFuture<'static, Result<()>>,
{
    decommission_cancel_signal_result(rx.is_cancelled())?;

    for bucket in meta_buckets {
        decommission_cancel_signal_result(rx.is_cancelled())?;
        start_bucket(bucket, rx.clone()).await?;
    }

    decommission_cancel_signal_result(rx.is_cancelled())?;

    if bucket_concurrency <= 1 {
        for bucket in regular_buckets {
            decommission_cancel_signal_result(rx.is_cancelled())?;
            start_bucket(bucket, rx.clone()).await?;
        }
        return Ok(());
    }

    run_decommission_buckets_bounded(rx, regular_buckets, bucket_concurrency, start_bucket).await
}

#[cfg(test)]
async fn wait_decommission_worker_drain(workers: &Semaphore, limit: usize) -> Result<()> {
    let permits = u32::try_from(limit)
        .map_err(|_| Error::other(format!("decommission worker limit {limit} exceeds semaphore drain capacity")))?;
    let _drain = workers
        .acquire_many(permits)
        .await
        .map_err(|err| Error::other(format!("decommission worker drain failed: {err}")))?;
    Ok(())
}

fn should_reject_decommission_cancel_as_terminal(complete: bool, failed: bool) -> bool {
    complete || failed
}

fn should_retry_decommission_cancel_reload(changed: bool, already_canceled: bool) -> bool {
    changed || already_canceled
}

fn ensure_decommission_cancel_allowed(pool_present: bool, decommission_present: bool, terminal: bool) -> Result<()> {
    if !pool_present {
        return Err(Error::other("failed to cancel decommission: target pool was not found"));
    }

    if !decommission_present || terminal {
        return Err(StorageError::DecommissionNotStarted);
    }

    Ok(())
}

fn ensure_decommission_clear_allowed(
    pool_present: bool,
    decommission_present: bool,
    complete: bool,
    failed: bool,
    canceled: bool,
    unresolved_entries: usize,
) -> Result<()> {
    if !pool_present {
        return Err(Error::other("failed to clear decommission: target pool was not found"));
    }

    if !decommission_present {
        return Err(StorageError::DecommissionNotStarted);
    }

    if complete {
        return Err(StorageError::DecommissionNotStarted);
    }

    if !failed && !canceled {
        return Err(StorageError::DecommissionAlreadyRunning);
    }

    if unresolved_entries > 0 {
        return Err(Error::other(format!(
            "failed to clear decommission: {unresolved_entries} unresolved listing entries must be reconciled by retrying decommission"
        )));
    }

    Ok(())
}

fn ensure_decommission_terminal_operation_supported(single_pool: bool, operation: &str) -> Result<()> {
    if single_pool {
        return Err(Error::other(format!(
            "failed to {operation}: single pool deployments do not support decommission"
        )));
    }

    Ok(())
}

fn validate_start_decommission_request(indices: &[usize], single_pool: bool) -> Result<()> {
    if indices.is_empty() {
        return Err(Error::other("failed to start decommission: no target pools were provided"));
    }

    ensure_decommission_terminal_operation_supported(single_pool, "start decommission")
}

fn require_decommission_store<T>(store: Option<T>, operation: &str) -> Result<T> {
    store.ok_or_else(|| Error::other(format!("failed to {operation}: store not initialized")))
}

fn ensure_decommission_listing_disks_available(has_disks: bool, bucket: &str) -> Result<()> {
    if !has_disks {
        return Err(Error::other(format!(
            "failed to list objects to decommission for bucket {bucket}: no disks available"
        )));
    }

    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolStatus {
    #[serde(rename = "id")]
    pub id: usize,
    #[serde(rename = "cmdline")]
    pub cmd_line: String,
    #[serde(rename = "lastUpdate", with = "time::serde::rfc3339")]
    pub last_update: OffsetDateTime,
    #[serde(rename = "decommissionInfo")]
    pub decommission: Option<PoolDecommissionInfo>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PoolMeta {
    pub version: u16,
    pub pools: Vec<PoolStatus>,
    pub dont_save: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PoolMetaRevision {
    version: u16,
    cluster_id: Option<uuid::Uuid>,
    epoch: u64,
    generation: u64,
    transaction_id: Option<uuid::Uuid>,
}

impl PoolMetaRevision {
    fn legacy(version: u16) -> Self {
        Self {
            version,
            cluster_id: None,
            epoch: 0,
            generation: 0,
            transaction_id: None,
        }
    }

    fn is_generation_protocol(self) -> bool {
        self.version == POOL_META_GENERATION_VERSION
    }
}

#[derive(Debug, Clone)]
struct PoolMetaCommittedCandidate {
    canonical: Vec<u8>,
    meta: PoolMeta,
    revision: PoolMetaRevision,
}

#[derive(Debug)]
enum PoolMetaReplica {
    Missing,
    Valid {
        raw: Vec<u8>,
        canonical: Vec<u8>,
        meta: PoolMeta,
        revision: PoolMetaRevision,
        committed: bool,
        previous: Option<Box<PoolMetaCommittedCandidate>>,
    },
    Corrupt(String),
    Incompatible(String),
    Unreadable(String),
}

#[derive(Debug, Clone)]
enum PoolMetaCasToken {
    Missing,
    Existing(String),
    Unsafe,
}

#[derive(Debug)]
struct PoolMetaReplicaRead {
    replica: PoolMetaReplica,
    cas: PoolMetaCasToken,
}

impl From<PoolMetaReplica> for PoolMetaReplicaRead {
    fn from(replica: PoolMetaReplica) -> Self {
        Self {
            replica,
            cas: PoolMetaCasToken::Unsafe,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PoolMetaReplicaState {
    pub(crate) needs_repair: bool,
    pub(crate) repair_write_safe: bool,
}

impl PoolMetaReplicaState {
    pub(crate) fn ensure_write_safe(self, operation: &str) -> Result<()> {
        if self.repair_write_safe {
            return Ok(());
        }
        Err(Error::other(format!(
            "{operation}: pool metadata update cannot overwrite an unreadable replica"
        )))
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct PoolMetaWriteState {
    write_blocked: bool,
    aborted_transaction: Arc<AtomicBool>,
    expected_cluster_id: Option<uuid::Uuid>,
    cluster_epoch: Option<u64>,
    pool_meta_absent: bool,
    fresh_bootstrap_proven: bool,
    identity_initialized: Option<bool>,
    identity_fresh_bootstrap_nonce: Option<uuid::Uuid>,
    identity_needs_repair: bool,
}

impl PoolMetaWriteState {
    pub(crate) fn for_startup(cluster_id: uuid::Uuid, fresh_bootstrap_proven: bool) -> Self {
        Self {
            expected_cluster_id: Some(cluster_id),
            fresh_bootstrap_proven,
            ..Default::default()
        }
    }

    pub(crate) fn fresh_bootstrap_proven(&self) -> bool {
        self.fresh_bootstrap_proven
    }

    pub(crate) fn identity_is_pending(&self) -> bool {
        self.identity_initialized == Some(false)
    }

    #[cfg(test)]
    pub(crate) fn aborted_transaction_latch_for_test(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.aborted_transaction)
    }

    #[cfg(any(test, feature = "test-util"))]
    fn for_test_bootstrap() -> Self {
        Self {
            fresh_bootstrap_proven: true,
            identity_initialized: Some(false),
            identity_fresh_bootstrap_nonce: Some(uuid::Uuid::new_v4()),
            ..Default::default()
        }
    }

    pub(crate) fn observe_replicas(&mut self, replica_state: PoolMetaReplicaState) {
        self.write_blocked |= !replica_state.repair_write_safe;
    }

    fn block_writes(&mut self) {
        self.write_blocked = true;
    }

    pub(crate) fn block_writes_after_fence_loss(&mut self) {
        self.block_writes();
    }

    fn arm_transaction(&self) -> PoolMetaTransactionArm {
        PoolMetaTransactionArm {
            aborted_transaction: Arc::clone(&self.aborted_transaction),
            armed: true,
        }
    }

    fn observe_selection(&mut self, selection: &PoolMetaSelection) -> Result<()> {
        self.pool_meta_absent = selection.absent;
        if let Some(expected_cluster_id) = self.expected_cluster_id
            && let Some((cluster_id, _)) = selection.generation_identity
            && cluster_id != expected_cluster_id
        {
            self.block_writes();
            return Err(Error::other(format!(
                "pool metadata incompatible: cluster identity {cluster_id} does not match deployment {expected_cluster_id}"
            )));
        }
        if let Some(identity_epoch) = self.cluster_epoch
            && let Some((_, metadata_epoch)) = selection.generation_identity
            && metadata_epoch != identity_epoch
        {
            self.block_writes();
            return Err(Error::other(format!(
                "pool metadata recovery required: committed epoch {} does not match cluster identity epoch {identity_epoch}",
                metadata_epoch
            )));
        }
        if self.cluster_epoch.is_none()
            && let Some((_, metadata_epoch)) = selection.generation_identity
        {
            self.cluster_epoch = Some(metadata_epoch);
        }
        Ok(())
    }

    fn observe_identity(&mut self, selection: &PoolMetaIdentitySelection) -> Result<()> {
        self.identity_needs_repair = selection.needs_repair;
        self.identity_initialized = selection.identity.map(|identity| identity.initialized);
        self.identity_fresh_bootstrap_nonce = selection.identity.and_then(|identity| identity.fresh_bootstrap_nonce);
        if let Some(identity) = selection.identity {
            if identity.initialized {
                self.fresh_bootstrap_proven = false;
            }
            if let Some(metadata_epoch) = self.cluster_epoch
                && metadata_epoch != identity.epoch
            {
                self.block_writes();
                return Err(Error::other(format!(
                    "pool metadata recovery required: metadata epoch {metadata_epoch} does not match cluster identity epoch {}",
                    identity.epoch
                )));
            }
            self.cluster_epoch = Some(identity.epoch);
        }
        if !selection.repair_write_safe {
            self.block_writes();
        }
        Ok(())
    }

    pub(crate) fn ensure_missing_metadata_can_initialize(&mut self) -> Result<()> {
        if !self.pool_meta_absent {
            return Ok(());
        }
        match self.identity_initialized {
            Some(false) if self.fresh_bootstrap_proven && self.identity_fresh_bootstrap_nonce.is_some() => Ok(()),
            Some(false) => {
                self.block_writes();
                Err(Error::other(
                    "pool metadata recovery required: pending cluster identity exists but this startup has no verified fresh-bootstrap proof",
                ))
            }
            Some(true) => {
                self.block_writes();
                Err(Error::other(
                    "pool metadata recovery required: initialized cluster identity exists but every pool.bin replica is missing",
                ))
            }
            None => {
                self.block_writes();
                Err(Error::other(
                    "pool metadata recovery required: no durable bootstrap identity or pool.bin replica is available",
                ))
            }
        }
    }

    pub(crate) fn identity_requires_repair(&self) -> bool {
        self.expected_cluster_id.is_some() && (self.identity_needs_repair || self.identity_initialized != Some(true))
    }

    pub(crate) fn ensure_write_safe(&self, operation: &str) -> Result<()> {
        if !self.write_blocked && !self.aborted_transaction.load(Ordering::SeqCst) {
            return Ok(());
        }
        Err(Error::other(format!(
            "{operation}: pool metadata writes remain blocked after a recovery-required replica state; restart after all replicas are readable and consistent, with compatible formats"
        )))
    }
}

#[derive(Debug)]
struct PoolMetaTransactionArm {
    aborted_transaction: Arc<AtomicBool>,
    armed: bool,
}

impl PoolMetaTransactionArm {
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for PoolMetaTransactionArm {
    fn drop(&mut self) {
        if self.armed {
            self.aborted_transaction.store(true, Ordering::SeqCst);
        }
    }
}

#[derive(Debug)]
struct PoolMetaSelection {
    meta: PoolMeta,
    revision: PoolMetaRevision,
    canonical: Option<Vec<u8>>,
    replica_state: PoolMetaReplicaState,
    cas_tokens: Vec<PoolMetaCasToken>,
    absent: bool,
    generation_protocol_observed: bool,
    generation_identity: Option<(uuid::Uuid, u64)>,
}

fn classify_pool_meta_tuple_decode_error(kind: &str, err: rmp_serde::decode::Error) -> PoolMetaReplica {
    let truncated = matches!(
        &err,
        rmp_serde::decode::Error::InvalidMarkerRead(source)
            | rmp_serde::decode::Error::InvalidDataRead(source)
            if source.kind() == std::io::ErrorKind::UnexpectedEof
    );
    if truncated {
        PoolMetaReplica::Corrupt(format!("{kind} tuple payload is truncated: {err}"))
    } else {
        PoolMetaReplica::Incompatible(format!("{kind} tuple payload is not decodable: {err}"))
    }
}

fn parse_pool_meta_uuid(value: &str, field: &str) -> Result<uuid::Uuid> {
    let parsed = uuid::Uuid::parse_str(value)
        .map_err(|err| Error::other(format!("pool metadata corrupt: invalid {field} `{value}`: {err}")))?;
    if parsed.is_nil() || parsed == uuid::Uuid::max() {
        return Err(Error::other(format!("pool metadata corrupt: {field} must be a non-reserved UUID")));
    }
    Ok(parsed)
}

fn pool_meta_generation_revision(
    cluster_id: &str,
    epoch: u64,
    generation: u64,
    transaction_id: &str,
) -> Result<PoolMetaRevision> {
    if epoch == 0 || generation == 0 {
        return Err(Error::other(
            "pool metadata corrupt: version 3 epoch and generation must both be non-zero",
        ));
    }
    Ok(PoolMetaRevision {
        version: POOL_META_GENERATION_VERSION,
        cluster_id: Some(parse_pool_meta_uuid(cluster_id, "cluster identity")?),
        epoch,
        generation,
        transaction_id: Some(parse_pool_meta_uuid(transaction_id, "transaction id")?),
    })
}

fn pool_meta_from_v3_statuses(version: u16, pools: Vec<PersistedPoolStatus>) -> Result<PoolMeta> {
    if !matches!(version, POOL_META_V1_VERSION | POOL_META_VERSION | POOL_META_GENERATION_VERSION) {
        return Err(Error::other(format!(
            "pool metadata corrupt: version 3 previous snapshot has unsupported source version {version}"
        )));
    }
    Ok(PoolMeta {
        version,
        pools: pools.into_iter().map(TryInto::try_into).collect::<Result<Vec<_>>>()?,
        dont_save: false,
    })
}

fn pool_meta_previous_candidate(value: PersistedPoolMetaV3Previous) -> Result<PoolMetaCommittedCandidate> {
    let revision = match value.version {
        POOL_META_V1_VERSION | POOL_META_VERSION => {
            if value.cluster_id.is_some() || value.epoch != 0 || value.generation != 0 || value.transaction_id.is_some() {
                return Err(Error::other(
                    "pool metadata corrupt: legacy previous snapshot carries version 3 revision fields",
                ));
            }
            PoolMetaRevision::legacy(value.version)
        }
        POOL_META_GENERATION_VERSION => pool_meta_generation_revision(
            value
                .cluster_id
                .as_deref()
                .ok_or_else(|| Error::other("pool metadata corrupt: version 3 previous snapshot has no cluster identity"))?,
            value.epoch,
            value.generation,
            value
                .transaction_id
                .as_deref()
                .ok_or_else(|| Error::other("pool metadata corrupt: version 3 previous snapshot has no transaction id"))?,
        )?,
        version => {
            return Err(Error::other(format!(
                "pool metadata corrupt: previous snapshot has unsupported version {version}"
            )));
        }
    };
    let meta = pool_meta_from_v3_statuses(value.version, value.pools)?;
    let canonical = if revision.is_generation_protocol() {
        encode_pool_meta_v3_envelope(&meta, revision, true, None)?
    } else {
        meta.encode_config_data_for_v2_gate(true)?
    };
    Ok(PoolMetaCommittedCandidate {
        canonical,
        meta,
        revision,
    })
}

fn decode_pool_meta_v3(data: Vec<u8>) -> PoolMetaReplica {
    let persisted = match rmp_serde::from_slice::<PersistedPoolMetaV3>(&data[4..]) {
        Ok(persisted) => persisted,
        Err(err) => return classify_pool_meta_tuple_decode_error("v3", err),
    };
    if persisted.version != POOL_META_GENERATION_VERSION {
        return PoolMetaReplica::Corrupt(format!(
            "v3 payload has version {}, expected {POOL_META_GENERATION_VERSION}",
            persisted.version
        ));
    }
    let revision = match pool_meta_generation_revision(
        &persisted.cluster_id,
        persisted.epoch,
        persisted.generation,
        &persisted.transaction_id,
    ) {
        Ok(revision) => revision,
        Err(err) => return PoolMetaReplica::Corrupt(err.to_string()),
    };
    let meta = match pool_meta_from_v3_statuses(POOL_META_GENERATION_VERSION, persisted.pools) {
        Ok(meta) => meta,
        Err(err) => return PoolMetaReplica::Corrupt(err.to_string()),
    };
    let previous = match persisted.previous.map(pool_meta_previous_candidate).transpose() {
        Ok(previous) => previous,
        Err(err) => return PoolMetaReplica::Corrupt(err.to_string()),
    };
    if persisted.committed && previous.is_some() {
        return PoolMetaReplica::Corrupt("committed v3 payload unexpectedly retains a previous snapshot".to_string());
    }
    if !persisted.committed {
        match previous.as_ref() {
            Some(previous) if previous.revision.is_generation_protocol() => {
                if previous.revision.cluster_id != revision.cluster_id
                    || previous.revision.epoch != revision.epoch
                    || previous.revision.generation.checked_add(1) != Some(revision.generation)
                {
                    return PoolMetaReplica::Corrupt(
                        "pending v3 payload does not advance exactly one generation from its previous snapshot".to_string(),
                    );
                }
            }
            Some(_) if revision.generation != 1 => {
                return PoolMetaReplica::Corrupt(
                    "first v3 generation must be generation 1 when migrating a legacy snapshot".to_string(),
                );
            }
            None if revision.generation != 1 => {
                return PoolMetaReplica::Corrupt(
                    "pending v3 payload without a previous snapshot must be the initial generation".to_string(),
                );
            }
            _ => {}
        }
    }
    let canonical = match encode_pool_meta_v3_envelope(&meta, revision, true, None) {
        Ok(canonical) => canonical,
        Err(err) => return PoolMetaReplica::Corrupt(err.to_string()),
    };
    PoolMetaReplica::Valid {
        raw: data,
        canonical,
        meta,
        revision,
        committed: persisted.committed,
        previous: previous.map(Box::new),
    }
}

fn decode_pool_meta_replica(data: Vec<u8>) -> PoolMetaReplica {
    if data.len() <= 4 {
        return PoolMetaReplica::Corrupt("metadata payload is empty or truncated".to_string());
    }

    let format = LittleEndian::read_u16(&data[0..2]);
    if format != POOL_META_FORMAT {
        return PoolMetaReplica::Incompatible(format!("unsupported format {format}"));
    }
    let version = LittleEndian::read_u16(&data[2..4]);
    if !matches!(version, POOL_META_V1_VERSION | POOL_META_VERSION | POOL_META_GENERATION_VERSION) {
        return PoolMetaReplica::Incompatible(format!("unsupported version {version}"));
    }
    if version == POOL_META_GENERATION_VERSION {
        return decode_pool_meta_v3(data);
    }

    let payload = &data[4..];
    let meta = match (version, rmp::decode::read_array_len(&mut &payload[..])) {
        (POOL_META_VERSION, Ok(2)) => match rmp_serde::from_slice::<PersistedPoolMeta>(payload) {
            Ok(meta) => match PoolMeta::try_from(meta) {
                Ok(meta) => meta,
                Err(err) => return PoolMetaReplica::Corrupt(err.to_string()),
            },
            Err(err) => return classify_pool_meta_tuple_decode_error("current", err),
        },
        (POOL_META_V1_VERSION, Ok(2)) => match rmp_serde::from_slice::<PersistedPoolMetaV1>(payload) {
            Ok(meta) => match PoolMeta::try_from(meta) {
                Ok(meta) => meta,
                Err(err) => return PoolMetaReplica::Corrupt(err.to_string()),
            },
            Err(err) => return classify_pool_meta_tuple_decode_error("v1", err),
        },
        // The older V1 tuple includes the runtime-only `dont_save` flag.
        (POOL_META_V1_VERSION, Ok(3)) => match rmp_serde::from_slice::<LegacyPoolMeta>(payload) {
            Ok(meta) => match PoolMeta::try_from(meta) {
                Ok(meta) => meta,
                Err(err) => return PoolMetaReplica::Corrupt(err.to_string()),
            },
            Err(err) => return classify_pool_meta_tuple_decode_error("legacy v1", err),
        },
        (_, Ok(field_count)) if field_count < 2 => {
            return PoolMetaReplica::Corrupt(format!("pool metadata tuple has only {field_count} fields"));
        }
        (_, Ok(field_count)) => {
            return PoolMetaReplica::Incompatible(format!(
                "pool metadata version {version} tuple has unsupported field count {field_count}"
            ));
        }
        (_, Err(_)) => {
            let mut meta = PoolMeta::default();
            if let Err(err) = meta.load_from_config_data(data.clone()) {
                let reason = err.to_string();
                if reason.contains("unknown field") {
                    return PoolMetaReplica::Incompatible(format!("current-version payload uses unsupported fields: {reason}"));
                }
                return PoolMetaReplica::Corrupt(reason);
            }
            meta
        }
    };

    match meta.encode_config_data_for_v2_gate(true) {
        Ok(canonical) => PoolMetaReplica::Valid {
            raw: data,
            canonical,
            meta,
            revision: PoolMetaRevision::legacy(version),
            committed: true,
            previous: None,
        },
        Err(err) => PoolMetaReplica::Corrupt(err.to_string()),
    }
}

#[cfg(test)]
pub(crate) fn pool_meta_v3_commit_state_for_test(data: Vec<u8>) -> Result<(u64, bool)> {
    match decode_pool_meta_replica(data) {
        PoolMetaReplica::Valid { revision, committed, .. } if revision.is_generation_protocol() => {
            Ok((revision.generation, committed))
        }
        _ => Err(Error::other("test pool metadata is not a valid V3 replica")),
    }
}

async fn read_pool_meta_replica<S>(pool: Arc<S>, no_lock: bool) -> PoolMetaReplicaRead
where
    S: EcstoreObjectIO,
{
    let result = if no_lock {
        read_config_no_lock_preserve_empty_with_metadata(pool, POOL_META_NAME)
            .await
            .map(|(data, object_info)| (data, object_info.etag))
    } else {
        read_config_preserve_empty(pool, POOL_META_NAME)
            .await
            .map(|data| (data, None))
    };
    match result {
        Ok((data, etag)) => PoolMetaReplicaRead {
            replica: decode_pool_meta_replica(data),
            cas: etag
                .filter(|etag| !etag.trim().is_empty())
                .map(PoolMetaCasToken::Existing)
                .unwrap_or(PoolMetaCasToken::Unsafe),
        },
        Err(Error::ConfigNotFound) => PoolMetaReplicaRead {
            replica: PoolMetaReplica::Missing,
            cas: PoolMetaCasToken::Missing,
        },
        Err(err) => PoolMetaReplicaRead {
            replica: PoolMetaReplica::Unreadable(err.to_string()),
            cas: PoolMetaCasToken::Unsafe,
        },
    }
}

fn select_pool_meta_replica_reads(reads: Vec<PoolMetaReplicaRead>) -> Result<PoolMetaSelection> {
    if reads.is_empty() {
        return Err(Error::other("pool metadata recovery required: no storage pools available"));
    }

    let cas_tokens = reads.iter().map(|read| read.cas.clone()).collect();
    let mut committed = Vec::<(usize, Vec<u8>, PoolMetaCommittedCandidate)>::new();
    let mut needs_repair = false;
    let mut repair_write_safe = true;
    let mut missing = 0usize;
    let mut unusable = Vec::new();
    let mut observed_version = POOL_META_V1_VERSION;
    let mut generation_protocol_observed = false;
    let mut generation_identity = None;

    for (idx, read) in reads.into_iter().enumerate() {
        match read.replica {
            PoolMetaReplica::Missing => {
                missing += 1;
                needs_repair = true;
            }
            PoolMetaReplica::Corrupt(reason) => {
                needs_repair = true;
                unusable.push(format!("pool {idx} is corrupt: {reason}"));
            }
            PoolMetaReplica::Unreadable(reason) => {
                needs_repair = true;
                repair_write_safe = false;
                unusable.push(format!("pool {idx} is unreadable: {reason}"));
            }
            PoolMetaReplica::Incompatible(reason) => {
                return Err(Error::other(format!(
                    "pool metadata recovery required: pool {idx} is incompatible ({reason}); upgrade or restore a compatible replica without overwriting it"
                )));
            }
            PoolMetaReplica::Valid {
                raw,
                canonical,
                meta,
                revision,
                committed: is_committed,
                previous,
            } => {
                generation_protocol_observed |= revision.is_generation_protocol();
                if revision.is_generation_protocol() {
                    let identity = (
                        revision
                            .cluster_id
                            .expect("validated V3 revision should carry a cluster identity"),
                        revision.epoch,
                    );
                    if generation_identity.is_some_and(|current| current != identity) {
                        return Err(Error::other(
                            "pool metadata recovery required: V3 replicas disagree on cluster identity or epoch",
                        ));
                    }
                    generation_identity = Some(identity);
                }
                if is_committed {
                    observed_version = observed_version.max(meta.version);
                    committed.push((
                        idx,
                        raw,
                        PoolMetaCommittedCandidate {
                            canonical,
                            meta,
                            revision,
                        },
                    ));
                } else if let Some(previous) = previous {
                    observed_version = observed_version.max(previous.meta.version);
                    needs_repair = true;
                    committed.push((idx, raw, *previous));
                } else {
                    needs_repair = true;
                    unusable.push(format!("pool {idx} contains an uncommitted initial generation"));
                }
            }
        }
    }

    if committed.is_empty() && missing > 0 && unusable.is_empty() {
        return Ok(PoolMetaSelection {
            meta: PoolMeta::default(),
            revision: PoolMetaRevision::legacy(0),
            canonical: None,
            replica_state: PoolMetaReplicaState {
                needs_repair: false,
                repair_write_safe: true,
            },
            cas_tokens,
            absent: true,
            generation_protocol_observed,
            generation_identity,
        });
    }
    if committed.is_empty() {
        return Err(Error::other(format!(
            "pool metadata recovery required: no valid committed replica is available ({})",
            unusable.join("; ")
        )));
    }

    if committed
        .iter()
        .any(|(_, _, candidate)| candidate.revision.is_generation_protocol())
    {
        let highest = committed
            .iter()
            .filter(|(_, _, candidate)| candidate.revision.is_generation_protocol())
            .map(|(_, _, candidate)| candidate.revision.generation)
            .max()
            .ok_or_else(|| Error::other("pool metadata recovery required: no committed V3 generation is available"))?;
        let mut selected: Option<(usize, &PoolMetaCommittedCandidate)> = None;
        for (idx, _, candidate) in &committed {
            if !candidate.revision.is_generation_protocol() || candidate.revision.generation != highest {
                needs_repair = true;
                continue;
            }
            if let Some((selected_idx, selected_candidate)) = selected {
                if selected_candidate.canonical != candidate.canonical
                    || selected_candidate.revision.transaction_id != candidate.revision.transaction_id
                {
                    return Err(Error::other(format!(
                        "pool metadata recovery required: committed generation {highest} diverges between pools {selected_idx} and {idx}"
                    )));
                }
            } else {
                selected = Some((*idx, candidate));
            }
        }
        let (_, selected) = selected
            .ok_or_else(|| Error::other("pool metadata recovery required: highest V3 generation has no valid snapshot"))?;
        for (_, raw, candidate) in &committed {
            needs_repair |= candidate.canonical != selected.canonical || raw != &selected.canonical;
        }
        return Ok(PoolMetaSelection {
            meta: selected.meta.clone(),
            revision: selected.revision,
            canonical: Some(selected.canonical.clone()),
            replica_state: PoolMetaReplicaState {
                needs_repair,
                repair_write_safe,
            },
            cas_tokens,
            absent: false,
            generation_protocol_observed,
            generation_identity,
        });
    }

    // Legacy V1/V2 has no durable generation. Pool zero remains the commit
    // record; divergent backups without it are ambiguous and fail closed.
    let mut selected: Option<(usize, Vec<u8>, PoolMetaCommittedCandidate)> = None;
    for (idx, raw, candidate) in committed {
        if let Some((selected_idx, selected_raw, selected_candidate)) = selected.as_ref() {
            if selected_candidate.canonical != candidate.canonical {
                if selected_candidate.meta.version == candidate.meta.version && *selected_idx == 0 {
                    needs_repair = true;
                } else {
                    return Err(Error::other(format!(
                        "pool metadata recovery required: valid replicas in pools {selected_idx} and {idx} diverge; restore one matching pool.bin snapshot before restart"
                    )));
                }
            } else {
                needs_repair |= selected_raw != &raw;
            }
        } else {
            selected = Some((idx, raw, candidate));
        }
    }
    let (_, _, mut selected) =
        selected.ok_or_else(|| Error::other("pool metadata recovery required: no valid legacy replica is available"))?;
    selected.meta.version = observed_version;
    selected.revision.version = observed_version;
    Ok(PoolMetaSelection {
        meta: selected.meta,
        revision: selected.revision,
        canonical: Some(selected.canonical),
        replica_state: PoolMetaReplicaState {
            needs_repair,
            repair_write_safe,
        },
        cas_tokens,
        absent: false,
        generation_protocol_observed,
        generation_identity,
    })
}

#[cfg(test)]
fn select_pool_meta_replica(replicas: Vec<PoolMetaReplica>) -> Result<PoolMetaSelection> {
    select_pool_meta_replica_reads(
        replicas
            .into_iter()
            .map(|replica| PoolMetaReplicaRead {
                replica,
                cas: PoolMetaCasToken::Unsafe,
            })
            .collect(),
    )
}

async fn read_pool_meta_replicas<S>(pools: Vec<Arc<S>>, no_lock: bool) -> Vec<PoolMetaReplicaRead>
where
    S: EcstoreObjectIO,
{
    join_all(pools.into_iter().map(|pool| read_pool_meta_replica(pool, no_lock))).await
}

fn select_pool_meta_replicas_observing<R>(write_state: &mut PoolMetaWriteState, replicas: Vec<R>) -> Result<PoolMetaSelection>
where
    R: Into<PoolMetaReplicaRead>,
{
    let replicas = replicas.into_iter().map(Into::into).collect::<Vec<_>>();
    if replicas
        .iter()
        .any(|replica| matches!(&replica.replica, PoolMetaReplica::Unreadable(_)))
    {
        write_state.block_writes();
    }
    match select_pool_meta_replica_reads(replicas) {
        Ok(selection) => {
            if let Err(err) = write_state.observe_selection(&selection) {
                write_state.block_writes();
                return Err(err);
            }
            Ok(selection)
        }
        Err(err) => {
            write_state.block_writes();
            Err(err)
        }
    }
}

async fn load_pool_meta_replicas<S>(pools: Vec<Arc<S>>, no_lock: bool) -> Result<PoolMetaSelection>
where
    S: EcstoreObjectIO,
{
    select_pool_meta_replica_reads(read_pool_meta_replicas(pools, no_lock).await)
}

async fn load_pool_meta_replicas_observing<S>(
    pools: Vec<Arc<S>>,
    no_lock: bool,
    write_state: &mut PoolMetaWriteState,
) -> Result<PoolMetaSelection>
where
    S: EcstoreObjectIO,
{
    let replicas = read_pool_meta_replicas(pools, no_lock).await;
    select_pool_meta_replicas_observing(write_state, replicas)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PersistedPoolMetaV3 {
    version: u16,
    cluster_id: String,
    epoch: u64,
    generation: u64,
    transaction_id: String,
    committed: bool,
    pools: Vec<PersistedPoolStatus>,
    previous: Option<PersistedPoolMetaV3Previous>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PersistedPoolMetaV3Previous {
    version: u16,
    cluster_id: Option<String>,
    epoch: u64,
    generation: u64,
    transaction_id: Option<String>,
    pools: Vec<PersistedPoolStatus>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PersistedPoolMetaIdentity {
    version: u16,
    cluster_id: uuid::Uuid,
    epoch: u64,
    initialized: bool,
    fresh_bootstrap_nonce: Option<uuid::Uuid>,
}

#[derive(Debug)]
enum PoolMetaIdentityReplica {
    Missing,
    Valid(PersistedPoolMetaIdentity),
    Corrupt(String),
    Incompatible(String),
    Unreadable(String),
}

#[derive(Debug)]
struct PoolMetaIdentityRead {
    replica: PoolMetaIdentityReplica,
    cas: PoolMetaCasToken,
}

#[derive(Debug)]
struct PoolMetaIdentitySelection {
    identity: Option<PersistedPoolMetaIdentity>,
    needs_repair: bool,
    repair_write_safe: bool,
    cas_tokens: Vec<PoolMetaCasToken>,
}

fn encode_pool_meta_identity(identity: PersistedPoolMetaIdentity) -> Result<Vec<u8>> {
    let mut data = Vec::new();
    data.write_u16::<LittleEndian>(POOL_META_IDENTITY_FORMAT)?;
    data.write_u16::<LittleEndian>(POOL_META_IDENTITY_VERSION)?;
    identity.serialize(&mut Serializer::new(&mut data))?;
    Ok(data)
}

fn decode_pool_meta_identity(data: &[u8]) -> PoolMetaIdentityReplica {
    if data.len() <= 4 {
        return PoolMetaIdentityReplica::Corrupt("identity payload is empty or truncated".to_string());
    }
    let format = LittleEndian::read_u16(&data[0..2]);
    let version = LittleEndian::read_u16(&data[2..4]);
    if format != POOL_META_IDENTITY_FORMAT || version != POOL_META_IDENTITY_VERSION {
        return PoolMetaIdentityReplica::Incompatible(format!("unsupported identity format {format} version {version}"));
    }
    let identity = match rmp_serde::from_slice::<PersistedPoolMetaIdentity>(&data[4..]) {
        Ok(identity) => identity,
        Err(err) => return PoolMetaIdentityReplica::Corrupt(format!("identity payload is not decodable: {err}")),
    };
    let invalid_bootstrap_nonce = identity
        .fresh_bootstrap_nonce
        .is_some_and(|nonce| nonce.is_nil() || nonce == uuid::Uuid::max());
    if identity.version != POOL_META_IDENTITY_VERSION
        || identity.cluster_id.is_nil()
        || identity.cluster_id == uuid::Uuid::max()
        || identity.epoch == 0
        || invalid_bootstrap_nonce
        || identity.initialized == identity.fresh_bootstrap_nonce.is_some()
    {
        return PoolMetaIdentityReplica::Corrupt(
            "identity payload contains an invalid version, UUID, epoch, or fresh-bootstrap proof".to_string(),
        );
    }
    PoolMetaIdentityReplica::Valid(identity)
}

#[cfg(test)]
pub(crate) fn pool_meta_identity_initialized_for_test(data: &[u8]) -> Result<bool> {
    match decode_pool_meta_identity(data) {
        PoolMetaIdentityReplica::Valid(identity) => Ok(identity.initialized),
        _ => Err(Error::other("test pool metadata identity is not valid")),
    }
}

#[cfg(test)]
pub(crate) fn initialized_pool_meta_identity_for_test(cluster_id: uuid::Uuid, epoch: u64) -> Result<Vec<u8>> {
    encode_pool_meta_identity(PersistedPoolMetaIdentity {
        version: POOL_META_IDENTITY_VERSION,
        cluster_id,
        epoch,
        initialized: true,
        fresh_bootstrap_nonce: None,
    })
}

async fn read_pool_meta_identity_replica<S>(pool: Arc<S>) -> PoolMetaIdentityRead
where
    S: EcstoreObjectIO,
{
    match read_config_no_lock_preserve_empty_with_metadata(pool, POOL_META_IDENTITY_NAME).await {
        Ok((data, object_info)) => PoolMetaIdentityRead {
            replica: decode_pool_meta_identity(&data),
            cas: object_info
                .etag
                .filter(|etag| !etag.trim().is_empty())
                .map(PoolMetaCasToken::Existing)
                .unwrap_or(PoolMetaCasToken::Unsafe),
        },
        Err(Error::ConfigNotFound) => PoolMetaIdentityRead {
            replica: PoolMetaIdentityReplica::Missing,
            cas: PoolMetaCasToken::Missing,
        },
        Err(err) => PoolMetaIdentityRead {
            replica: PoolMetaIdentityReplica::Unreadable(err.to_string()),
            cas: PoolMetaCasToken::Unsafe,
        },
    }
}

fn select_pool_meta_identity(
    reads: Vec<PoolMetaIdentityRead>,
    expected_cluster_id: uuid::Uuid,
) -> Result<PoolMetaIdentitySelection> {
    let cas_tokens = reads.iter().map(|read| read.cas.clone()).collect();
    let mut selected: Option<PersistedPoolMetaIdentity> = None;
    let mut needs_repair = false;
    let mut repair_write_safe = true;
    let mut unusable = Vec::new();
    for (idx, read) in reads.into_iter().enumerate() {
        match read.replica {
            PoolMetaIdentityReplica::Missing => needs_repair = true,
            PoolMetaIdentityReplica::Corrupt(reason) => {
                needs_repair = true;
                unusable.push(format!("pool {idx} identity is corrupt: {reason}"));
            }
            PoolMetaIdentityReplica::Unreadable(reason) => {
                needs_repair = true;
                repair_write_safe = false;
                unusable.push(format!("pool {idx} identity is unreadable: {reason}"));
            }
            PoolMetaIdentityReplica::Incompatible(reason) => {
                return Err(Error::other(format!("pool metadata incompatible: pool {idx} identity uses {reason}")));
            }
            PoolMetaIdentityReplica::Valid(identity) => {
                if identity.cluster_id != expected_cluster_id {
                    return Err(Error::other(format!(
                        "pool metadata incompatible: pool {idx} identity {} does not match deployment {expected_cluster_id}",
                        identity.cluster_id
                    )));
                }
                if let Some(current) = selected {
                    if current.cluster_id != identity.cluster_id || current.epoch != identity.epoch {
                        return Err(Error::other(
                            "pool metadata recovery required: identity replicas disagree on cluster identity or epoch",
                        ));
                    }
                    if current.initialized != identity.initialized {
                        needs_repair = true;
                        selected = Some(if current.initialized { current } else { identity });
                    } else if !current.initialized && current.fresh_bootstrap_nonce != identity.fresh_bootstrap_nonce {
                        return Err(Error::other(
                            "pool metadata recovery required: pending identity replicas disagree on fresh-bootstrap proof",
                        ));
                    }
                } else {
                    selected = Some(identity);
                }
            }
        }
    }
    if selected.is_none() && !unusable.is_empty() {
        return Err(Error::other(format!(
            "pool metadata recovery required: no valid cluster identity replica is available ({})",
            unusable.join("; ")
        )));
    }
    Ok(PoolMetaIdentitySelection {
        identity: selected,
        needs_repair,
        repair_write_safe,
        cas_tokens,
    })
}

async fn load_pool_meta_identity<S>(pools: Vec<Arc<S>>, expected_cluster_id: uuid::Uuid) -> Result<PoolMetaIdentitySelection>
where
    S: EcstoreObjectIO,
{
    let reads = join_all(pools.into_iter().map(read_pool_meta_identity_replica)).await;
    select_pool_meta_identity(reads, expected_cluster_id)
}

async fn load_pool_meta_identity_selection_observing<S>(
    pools: Vec<Arc<S>>,
    write_state: &mut PoolMetaWriteState,
    expected_cluster_id: uuid::Uuid,
) -> Result<PoolMetaIdentitySelection>
where
    S: EcstoreObjectIO,
{
    let selection = match load_pool_meta_identity(pools, expected_cluster_id).await {
        Ok(selection) => selection,
        Err(err) => {
            write_state.block_writes();
            return Err(err);
        }
    };
    if let Err(err) = write_state.observe_identity(&selection) {
        write_state.block_writes();
        return Err(err);
    }
    Ok(selection)
}

fn persisted_pool_meta_v3_previous(candidate: &PoolMetaCommittedCandidate) -> PersistedPoolMetaV3Previous {
    PersistedPoolMetaV3Previous {
        version: candidate.revision.version,
        cluster_id: candidate.revision.cluster_id.map(|id| id.to_string()),
        epoch: candidate.revision.epoch,
        generation: candidate.revision.generation,
        transaction_id: candidate.revision.transaction_id.map(|id| id.to_string()),
        pools: candidate.meta.pools.iter().map(Into::into).collect(),
    }
}

fn encode_pool_meta_v3_envelope(
    meta: &PoolMeta,
    revision: PoolMetaRevision,
    committed: bool,
    previous: Option<&PoolMetaCommittedCandidate>,
) -> Result<Vec<u8>> {
    if meta.dont_save {
        return Ok(Vec::new());
    }
    let cluster_id = revision
        .cluster_id
        .ok_or_else(|| Error::other("pool metadata V3 save failed: cluster identity is not initialized"))?;
    let transaction_id = revision
        .transaction_id
        .ok_or_else(|| Error::other("pool metadata V3 save failed: transaction id is not initialized"))?;
    if revision.version != POOL_META_GENERATION_VERSION || revision.epoch == 0 || revision.generation == 0 {
        return Err(Error::other("pool metadata V3 save failed: invalid durable revision"));
    }
    let persisted = PersistedPoolMetaV3 {
        version: POOL_META_GENERATION_VERSION,
        cluster_id: cluster_id.to_string(),
        epoch: revision.epoch,
        generation: revision.generation,
        transaction_id: transaction_id.to_string(),
        committed,
        pools: meta.pools.iter().map(Into::into).collect(),
        previous: previous.map(persisted_pool_meta_v3_previous),
    };
    let mut data = Vec::new();
    data.write_u16::<LittleEndian>(POOL_META_FORMAT)?;
    data.write_u16::<LittleEndian>(POOL_META_GENERATION_VERSION)?;
    persisted.serialize(&mut Serializer::new(&mut data))?;
    Ok(data)
}

enum PoolMetaPersistenceFence<'a> {
    Distributed(Option<Arc<rustfs_lock::distributed_lock::LockLostSignal>>),
    Activation(&'a PoolRebalanceActivationFence),
}

impl PoolMetaPersistenceFence<'_> {
    fn ensure_held(&self) -> Result<()> {
        match self {
            Self::Distributed(Some(signal)) if signal.is_lost() => {
                Err(Error::other("pool metadata distributed fence was lost before a replica write"))
            }
            Self::Activation(fence) => fence.ensure_held(),
            _ => Ok(()),
        }
    }

    fn add_to_options(&self, opts: &mut ObjectOptions) {
        match self {
            Self::Distributed(Some(signal)) => opts.add_namespace_lock_lost_signal(Arc::clone(signal)),
            Self::Activation(fence) => fence.add_namespace_lock_fence(opts),
            Self::Distributed(None) => {}
        }
    }

    fn is_activation(&self) -> bool {
        matches!(self, Self::Activation(_))
    }
}

fn pool_meta_cas_preconditions(token: &PoolMetaCasToken, object: &str) -> Result<HTTPPreconditions> {
    match token {
        PoolMetaCasToken::Missing => Ok(HTTPPreconditions {
            if_none_match: Some("*".to_string()),
            ..Default::default()
        }),
        PoolMetaCasToken::Existing(etag) => Ok(HTTPPreconditions {
            if_match: Some(etag.clone()),
            ..Default::default()
        }),
        PoolMetaCasToken::Unsafe => Err(Error::other(format!(
            "pool metadata recovery required: {object} replica has no safe conditional-write revision"
        ))),
    }
}

async fn save_pool_meta_object_cas<S>(
    pool: Arc<S>,
    object: &str,
    data: Vec<u8>,
    token: &PoolMetaCasToken,
    fence: &PoolMetaPersistenceFence<'_>,
    phase: &'static str,
) -> Result<crate::object_api::ObjectInfo>
where
    S: EcstoreObjectIO,
{
    fence.ensure_held()?;
    let mut opts = ObjectOptions {
        max_parity: true,
        no_lock: true,
        http_preconditions: Some(pool_meta_cas_preconditions(token, object)?),
        ..Default::default()
    };
    fence.add_to_options(&mut opts);
    let result = save_config_with_opts_and_metadata(pool, object, data, &opts).await;
    if matches!(&result, Err(Error::PreconditionFailed)) {
        record_pool_meta_stale_write_rejection(phase);
    }
    let object_info = result?;
    fence.ensure_held()?;
    Ok(object_info)
}

async fn persist_pool_meta_identity<S>(
    pools: Vec<Arc<S>>,
    write_state: &mut PoolMetaWriteState,
    initialized: bool,
    fence: &PoolMetaPersistenceFence<'_>,
) -> Result<()>
where
    S: EcstoreObjectIO,
{
    let Some(cluster_id) = write_state.expected_cluster_id else {
        return Ok(());
    };
    for attempt in 0..POOL_META_CAS_MAX_ATTEMPTS {
        let selection = load_pool_meta_identity_selection_observing(pools.clone(), write_state, cluster_id).await?;
        if !selection.repair_write_safe {
            write_state.block_writes();
            return Err(Error::other(
                "pool metadata recovery required: cluster identity has an unreadable replica",
            ));
        }
        let identity = match selection.identity {
            Some(identity) if identity.initialized || initialized => PersistedPoolMetaIdentity {
                initialized: true,
                fresh_bootstrap_nonce: None,
                ..identity
            },
            Some(identity) => identity,
            None if !initialized && !write_state.fresh_bootstrap_proven() => {
                write_state.block_writes();
                return Err(Error::other(
                    "pool metadata recovery required: cannot create a pending cluster identity without verified fresh-bootstrap proof",
                ));
            }
            None => PersistedPoolMetaIdentity {
                version: POOL_META_IDENTITY_VERSION,
                cluster_id,
                epoch: write_state.cluster_epoch.unwrap_or(POOL_META_INITIAL_EPOCH),
                initialized,
                fresh_bootstrap_nonce: (!initialized).then(uuid::Uuid::new_v4),
            },
        };
        if selection.identity == Some(identity) && !selection.needs_repair {
            return Ok(());
        }
        let data = encode_pool_meta_identity(identity)?;
        let mut conflict = false;
        for (pool, token) in pools.iter().cloned().zip(&selection.cas_tokens) {
            match save_pool_meta_object_cas(pool, POOL_META_IDENTITY_NAME, data.clone(), token, fence, "identity_cas").await {
                Ok(_) => {}
                Err(Error::PreconditionFailed) => {
                    conflict = true;
                    break;
                }
                Err(err) => return Err(err),
            }
        }
        if conflict {
            if attempt + 1 < POOL_META_CAS_MAX_ATTEMPTS {
                continue;
            }
            return Err(Error::PreconditionFailed);
        }
        let confirmed = load_pool_meta_identity_selection_observing(pools.clone(), write_state, cluster_id).await?;
        if confirmed.identity == Some(identity) && !confirmed.needs_repair {
            return Ok(());
        }
    }
    Err(Error::PreconditionFailed)
}

pub(crate) async fn load_pool_meta_identity_observing<S>(pools: Vec<Arc<S>>, write_state: &mut PoolMetaWriteState) -> Result<()>
where
    S: EcstoreObjectIO,
{
    let Some(cluster_id) = write_state.expected_cluster_id else {
        return Ok(());
    };
    load_pool_meta_identity_selection_observing(pools, write_state, cluster_id)
        .await
        .map(|_| ())
}

pub(crate) async fn persist_pool_meta_identity_for_startup<S>(
    pools: Vec<Arc<S>>,
    write_state: &mut PoolMetaWriteState,
    initialized: bool,
) -> Result<()>
where
    S: EcstoreObjectIO,
{
    persist_pool_meta_identity(pools, write_state, initialized, &PoolMetaPersistenceFence::Distributed(None)).await
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PersistedPoolMeta {
    pub version: u16,
    pub pools: Vec<PersistedPoolStatus>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PersistedPoolStatus {
    #[serde(rename = "id")]
    pub id: usize,
    #[serde(rename = "cmdline")]
    pub cmd_line: String,
    #[serde(rename = "lastUpdate", with = "time::serde::rfc3339")]
    pub last_update: OffsetDateTime,
    #[serde(rename = "decommissionInfo")]
    pub decommission: Option<PersistedPoolDecommissionInfo>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PersistedPoolDecommissionInfo {
    #[serde(rename = "startTime", with = "time::serde::rfc3339::option")]
    pub start_time: Option<OffsetDateTime>,
    #[serde(rename = "startSize")]
    pub start_size: usize,
    #[serde(rename = "totalSize")]
    pub total_size: usize,
    #[serde(rename = "currentSize")]
    pub current_size: usize,
    #[serde(rename = "complete")]
    pub complete: bool,
    #[serde(rename = "failed")]
    pub failed: bool,
    #[serde(rename = "canceled")]
    pub canceled: bool,
    #[serde(rename = "queued", default)]
    pub queued: bool,
    #[serde(rename = "queuedBuckets", default)]
    pub queued_buckets: Vec<String>,
    #[serde(rename = "decommissionedBuckets", default)]
    pub decommissioned_buckets: Vec<String>,
    #[serde(rename = "bucket", default)]
    pub bucket: String,
    #[serde(rename = "prefix", default)]
    pub prefix: String,
    #[serde(rename = "object", default)]
    pub object: String,
    #[serde(rename = "objectsDecommissioned")]
    pub items_decommissioned: usize,
    #[serde(rename = "objectsDecommissionedFailed")]
    pub items_decommission_failed: usize,
    #[serde(rename = "bytesDecommissioned")]
    pub bytes_done: usize,
    #[serde(rename = "bytesDecommissionedFailed")]
    pub bytes_failed: usize,
    #[serde(rename = "terminalReloadAttemptAt", with = "time::serde::rfc3339::option", default)]
    pub terminal_reload_attempt_at: Option<OffsetDateTime>,
    #[serde(rename = "terminalReloadFailures", default)]
    pub terminal_reload_failures: Vec<String>,
    #[serde(rename = "unresolvedEntries", default)]
    pub unresolved_entries: Vec<DecommissionUnresolvedEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PersistedPoolMetaV1 {
    pub version: u16,
    pub pools: Vec<PersistedPoolStatusV1>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PersistedPoolStatusV1 {
    #[serde(rename = "id")]
    pub id: usize,
    #[serde(rename = "cmdline")]
    pub cmd_line: String,
    #[serde(rename = "lastUpdate", with = "time::serde::rfc3339")]
    pub last_update: OffsetDateTime,
    #[serde(rename = "decommissionInfo")]
    pub decommission: Option<PersistedPoolDecommissionInfoV1>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PersistedPoolDecommissionInfoV1 {
    #[serde(rename = "startTime", with = "time::serde::rfc3339::option")]
    pub start_time: Option<OffsetDateTime>,
    #[serde(rename = "startSize")]
    pub start_size: usize,
    #[serde(rename = "totalSize")]
    pub total_size: usize,
    #[serde(rename = "currentSize")]
    pub current_size: usize,
    #[serde(rename = "complete")]
    pub complete: bool,
    #[serde(rename = "failed")]
    pub failed: bool,
    #[serde(rename = "canceled")]
    pub canceled: bool,
    #[serde(rename = "queued", default)]
    pub queued: bool,
    #[serde(rename = "queuedBuckets", default)]
    pub queued_buckets: Vec<String>,
    #[serde(rename = "decommissionedBuckets", default)]
    pub decommissioned_buckets: Vec<String>,
    #[serde(rename = "bucket", default)]
    pub bucket: String,
    #[serde(rename = "prefix", default)]
    pub prefix: String,
    #[serde(rename = "object", default)]
    pub object: String,
    #[serde(rename = "objectsDecommissioned")]
    pub items_decommissioned: usize,
    #[serde(rename = "objectsDecommissionedFailed")]
    pub items_decommission_failed: usize,
    #[serde(rename = "bytesDecommissioned")]
    pub bytes_done: usize,
    #[serde(rename = "bytesDecommissionedFailed")]
    pub bytes_failed: usize,
    #[serde(rename = "terminalReloadAttemptAt", with = "time::serde::rfc3339::option", default)]
    pub terminal_reload_attempt_at: Option<OffsetDateTime>,
    #[serde(rename = "terminalReloadFailures", default)]
    pub terminal_reload_failures: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LegacyPoolMeta {
    pub version: u16,
    pub pools: Vec<LegacyPoolStatus>,
    pub dont_save: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LegacyPoolStatus {
    #[serde(rename = "id")]
    pub id: usize,
    #[serde(rename = "cmdline")]
    pub cmd_line: String,
    #[serde(rename = "lastUpdate", with = "time::serde::rfc3339")]
    pub last_update: OffsetDateTime,
    #[serde(rename = "decommissionInfo")]
    pub decommission: Option<LegacyPoolDecommissionInfo>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LegacyPoolDecommissionInfo {
    #[serde(rename = "startTime", with = "time::serde::rfc3339::option")]
    pub start_time: Option<OffsetDateTime>,
    #[serde(rename = "startSize")]
    pub start_size: usize,
    #[serde(rename = "totalSize")]
    pub total_size: usize,
    #[serde(rename = "currentSize")]
    pub current_size: usize,
    #[serde(rename = "complete")]
    pub complete: bool,
    #[serde(rename = "failed")]
    pub failed: bool,
    #[serde(rename = "canceled")]
    pub canceled: bool,
    #[serde(rename = "objectsDecommissioned")]
    pub items_decommissioned: usize,
    #[serde(rename = "objectsDecommissionedFailed")]
    pub items_decommission_failed: usize,
    #[serde(rename = "bytesDecommissioned")]
    pub bytes_done: usize,
    #[serde(rename = "bytesDecommissionedFailed")]
    pub bytes_failed: usize,
}

fn ensure_pool_meta_payload_version(actual: u16, expected: u16, kind: &str) -> Result<()> {
    if actual == expected {
        return Ok(());
    }
    Err(Error::other(format!(
        "pool metadata {kind} payload has version {actual}, expected {expected}"
    )))
}

impl TryFrom<PersistedPoolMeta> for PoolMeta {
    type Error = Error;

    fn try_from(value: PersistedPoolMeta) -> Result<Self> {
        ensure_pool_meta_payload_version(value.version, POOL_META_VERSION, "current")?;
        Ok(Self {
            version: POOL_META_VERSION,
            pools: value.pools.into_iter().map(TryInto::try_into).collect::<Result<Vec<_>>>()?,
            dont_save: false,
        })
    }
}

impl TryFrom<PersistedPoolMetaV1> for PoolMeta {
    type Error = Error;

    fn try_from(value: PersistedPoolMetaV1) -> Result<Self> {
        ensure_pool_meta_payload_version(value.version, POOL_META_V1_VERSION, "v1")?;
        Ok(Self {
            version: POOL_META_V1_VERSION,
            pools: value.pools.into_iter().map(TryInto::try_into).collect::<Result<Vec<_>>>()?,
            dont_save: false,
        })
    }
}

impl TryFrom<LegacyPoolMeta> for PoolMeta {
    type Error = Error;

    fn try_from(value: LegacyPoolMeta) -> Result<Self> {
        let LegacyPoolMeta {
            version,
            pools,
            dont_save: _,
        } = value;
        ensure_pool_meta_payload_version(version, POOL_META_V1_VERSION, "legacy v1")?;
        Ok(Self {
            version: POOL_META_V1_VERSION,
            pools: pools.into_iter().map(TryInto::try_into).collect::<Result<Vec<_>>>()?,
            dont_save: false,
        })
    }
}

impl TryFrom<PersistedPoolStatus> for PoolStatus {
    type Error = Error;

    fn try_from(value: PersistedPoolStatus) -> Result<Self> {
        Ok(Self {
            id: value.id,
            cmd_line: value.cmd_line,
            last_update: value.last_update,
            decommission: value.decommission.map(TryInto::try_into).transpose()?,
        })
    }
}

impl TryFrom<PersistedPoolStatusV1> for PoolStatus {
    type Error = Error;

    fn try_from(value: PersistedPoolStatusV1) -> Result<Self> {
        Ok(Self {
            id: value.id,
            cmd_line: value.cmd_line,
            last_update: value.last_update,
            decommission: value.decommission.map(TryInto::try_into).transpose()?,
        })
    }
}

impl TryFrom<LegacyPoolStatus> for PoolStatus {
    type Error = Error;

    fn try_from(value: LegacyPoolStatus) -> Result<Self> {
        Ok(Self {
            id: value.id,
            cmd_line: value.cmd_line,
            last_update: value.last_update,
            decommission: value.decommission.map(TryInto::try_into).transpose()?,
        })
    }
}

impl TryFrom<PersistedPoolDecommissionInfo> for PoolDecommissionInfo {
    type Error = Error;

    fn try_from(value: PersistedPoolDecommissionInfo) -> Result<Self> {
        validate_decommission_terminal_state(value.complete, value.failed, value.canceled)?;
        Ok(Self {
            start_time: value.start_time,
            start_size: value.start_size,
            total_size: value.total_size,
            current_size: value.current_size,
            complete: value.complete,
            failed: value.failed,
            canceled: value.canceled,
            queued: value.queued,
            queued_buckets: value.queued_buckets,
            decommissioned_buckets: value.decommissioned_buckets,
            bucket: value.bucket,
            prefix: value.prefix,
            object: value.object,
            stage: String::new(),
            items_decommissioned: value.items_decommissioned,
            items_decommission_failed: value.items_decommission_failed,
            bytes_done: value.bytes_done,
            bytes_failed: value.bytes_failed,
            terminal_reload_attempt_at: value.terminal_reload_attempt_at,
            terminal_reload_failures: value.terminal_reload_failures,
            unresolved_entries: value.unresolved_entries,
            progress_save_item_baseline: value.items_decommissioned.saturating_add(value.items_decommission_failed),
            progress_save_retry_after: None,
        })
    }
}

impl TryFrom<PersistedPoolDecommissionInfoV1> for PoolDecommissionInfo {
    type Error = Error;

    fn try_from(value: PersistedPoolDecommissionInfoV1) -> Result<Self> {
        validate_decommission_terminal_state(value.complete, value.failed, value.canceled)?;
        Ok(Self {
            start_time: value.start_time,
            start_size: value.start_size,
            total_size: value.total_size,
            current_size: value.current_size,
            complete: value.complete,
            failed: value.failed,
            canceled: value.canceled,
            queued: value.queued,
            queued_buckets: value.queued_buckets,
            decommissioned_buckets: value.decommissioned_buckets,
            bucket: value.bucket,
            prefix: value.prefix,
            object: value.object,
            stage: String::new(),
            items_decommissioned: value.items_decommissioned,
            items_decommission_failed: value.items_decommission_failed,
            bytes_done: value.bytes_done,
            bytes_failed: value.bytes_failed,
            terminal_reload_attempt_at: value.terminal_reload_attempt_at,
            terminal_reload_failures: value.terminal_reload_failures,
            unresolved_entries: Vec::new(),
            progress_save_item_baseline: value.items_decommissioned.saturating_add(value.items_decommission_failed),
            progress_save_retry_after: None,
        })
    }
}

impl TryFrom<LegacyPoolDecommissionInfo> for PoolDecommissionInfo {
    type Error = Error;

    fn try_from(value: LegacyPoolDecommissionInfo) -> Result<Self> {
        validate_decommission_terminal_state(value.complete, value.failed, value.canceled)?;
        Ok(Self {
            start_time: value.start_time,
            start_size: value.start_size,
            total_size: value.total_size,
            current_size: value.current_size,
            complete: value.complete,
            failed: value.failed,
            canceled: value.canceled,
            queued: false,
            queued_buckets: Vec::new(),
            decommissioned_buckets: Vec::new(),
            bucket: String::new(),
            prefix: String::new(),
            object: String::new(),
            stage: String::new(),
            items_decommissioned: value.items_decommissioned,
            items_decommission_failed: value.items_decommission_failed,
            bytes_done: value.bytes_done,
            bytes_failed: value.bytes_failed,
            terminal_reload_attempt_at: None,
            terminal_reload_failures: Vec::new(),
            unresolved_entries: Vec::new(),
            progress_save_item_baseline: value.items_decommissioned.saturating_add(value.items_decommission_failed),
            progress_save_retry_after: None,
        })
    }
}

impl From<&PoolMeta> for PersistedPoolMeta {
    fn from(value: &PoolMeta) -> Self {
        Self {
            version: POOL_META_VERSION,
            pools: value.pools.iter().map(Into::into).collect(),
        }
    }
}

impl From<&PoolMeta> for PersistedPoolMetaV1 {
    fn from(value: &PoolMeta) -> Self {
        Self {
            version: POOL_META_V1_VERSION,
            pools: value.pools.iter().map(Into::into).collect(),
        }
    }
}

impl From<&PoolStatus> for PersistedPoolStatus {
    fn from(value: &PoolStatus) -> Self {
        Self {
            id: value.id,
            cmd_line: value.cmd_line.clone(),
            last_update: value.last_update,
            decommission: value.decommission.as_ref().map(Into::into),
        }
    }
}

impl From<&PoolStatus> for PersistedPoolStatusV1 {
    fn from(value: &PoolStatus) -> Self {
        Self {
            id: value.id,
            cmd_line: value.cmd_line.clone(),
            last_update: value.last_update,
            decommission: value.decommission.as_ref().map(Into::into),
        }
    }
}

impl From<&PoolDecommissionInfo> for PersistedPoolDecommissionInfo {
    fn from(value: &PoolDecommissionInfo) -> Self {
        Self {
            start_time: value.start_time,
            start_size: value.start_size,
            total_size: value.total_size,
            current_size: value.current_size,
            complete: value.complete,
            failed: value.failed,
            canceled: value.canceled,
            queued: value.queued,
            queued_buckets: value.queued_buckets.clone(),
            decommissioned_buckets: value.decommissioned_buckets.clone(),
            bucket: value.bucket.clone(),
            prefix: value.prefix.clone(),
            object: value.object.clone(),
            items_decommissioned: value.items_decommissioned,
            items_decommission_failed: value.items_decommission_failed,
            bytes_done: value.bytes_done,
            bytes_failed: value.bytes_failed,
            terminal_reload_attempt_at: value.terminal_reload_attempt_at,
            terminal_reload_failures: value.terminal_reload_failures.clone(),
            unresolved_entries: value.unresolved_entries.clone(),
        }
    }
}

impl From<&PoolDecommissionInfo> for PersistedPoolDecommissionInfoV1 {
    fn from(value: &PoolDecommissionInfo) -> Self {
        Self {
            start_time: value.start_time,
            start_size: value.start_size,
            total_size: value.total_size,
            current_size: value.current_size,
            complete: value.complete,
            failed: value.failed,
            canceled: value.canceled,
            queued: value.queued,
            queued_buckets: value.queued_buckets.clone(),
            decommissioned_buckets: value.decommissioned_buckets.clone(),
            bucket: value.bucket.clone(),
            prefix: value.prefix.clone(),
            object: value.object.clone(),
            items_decommissioned: value.items_decommissioned,
            items_decommission_failed: value.items_decommission_failed,
            bytes_done: value.bytes_done,
            bytes_failed: value.bytes_failed,
            terminal_reload_attempt_at: value.terminal_reload_attempt_at,
            terminal_reload_failures: value.terminal_reload_failures.clone(),
        }
    }
}

#[derive(Debug)]
struct PoolMetaSaveOutcome {
    transaction_arm: PoolMetaTransactionArm,
    committed: PoolMeta,
}

impl PoolMetaSaveOutcome {
    fn disarm(mut self) {
        self.transaction_arm.disarm();
    }

    fn into_committed(mut self) -> PoolMeta {
        self.transaction_arm.disarm();
        self.committed
    }
}

impl PoolMeta {
    fn decode_pool_meta_payload(version: u16, payload: &[u8]) -> Result<Self> {
        match version {
            POOL_META_VERSION => rmp_serde::from_slice::<PersistedPoolMeta>(payload)
                .map_err(|err| Error::other(format!("PoolMeta v{POOL_META_VERSION} decode failed: {err}")))?
                .try_into(),
            POOL_META_V1_VERSION => match rmp_serde::from_slice::<PersistedPoolMetaV1>(payload) {
                Ok(meta) => meta.try_into(),
                Err(persisted_err) => {
                    let legacy: LegacyPoolMeta = rmp_serde::from_slice(payload).map_err(|legacy_err| {
                        Error::other(format!(
                            "PoolMeta v1 decode failed for both persisted and legacy formats: persisted={persisted_err}; legacy={legacy_err}"
                        ))
                    })?;
                    legacy.try_into()
                }
            },
            _ => Err(Error::other(format!("pool metadata load failed: unknown version {version}"))),
        }
    }

    pub fn new(pools: &[Arc<Sets>], prev_meta: &PoolMeta) -> Self {
        let mut new_meta = Self {
            version: if prev_meta.version == POOL_META_GENERATION_VERSION || pool_meta_v3_writer_enabled() {
                POOL_META_GENERATION_VERSION
            } else if prev_meta.version == POOL_META_VERSION || pool_meta_v2_writer_enabled() {
                POOL_META_VERSION
            } else {
                POOL_META_V1_VERSION
            },
            pools: Vec::new(),
            ..Default::default()
        };

        for (idx, pool) in pools.iter().enumerate() {
            let mut skip = false;

            for current_pool in prev_meta.pools.iter() {
                if current_pool.cmd_line == pool.endpoints.cmd_line {
                    new_meta.pools.push(current_pool.clone());
                    skip = true;
                    break;
                }
            }

            if skip {
                continue;
            }

            new_meta.pools.push(PoolStatus {
                cmd_line: pool.endpoints.cmd_line.clone(),
                id: idx,
                last_update: OffsetDateTime::now_utc(),
                decommission: None,
            });
        }

        new_meta
    }

    pub fn is_suspended(&self, idx: usize) -> bool {
        self.pools
            .get(idx)
            .and_then(|pool| pool.decommission.as_ref())
            .is_some_and(is_decommission_suspended)
    }

    fn mark_decommission_progress_saved(&mut self) {
        for pool in &mut self.pools {
            if let Some(info) = pool.decommission.as_mut() {
                info.mark_progress_saved();
            }
        }
    }

    fn decommission_progress_checkpoint(
        &self,
        idx: usize,
        duration: Duration,
        now: OffsetDateTime,
    ) -> Result<Option<DecommissionProgressCheckpoint>> {
        let pool_count = self.pools.len();
        ensure_valid_decommission_pool_index(pool_count, idx)?;

        let Some(pool) = self.pools.get(idx) else {
            return Err(invalid_decommission_pool_index_error(pool_count, idx));
        };
        let Some(info) = pool.decommission.as_ref() else {
            return Err(decommission_metadata_not_initialized_error("update decommission metadata timestamp"));
        };

        if info.progress_save_retry_after.is_some_and(|retry_after| now < retry_after) {
            return Ok(None);
        }

        let time_threshold_reached = now.unix_timestamp() - pool.last_update.unix_timestamp() >= duration.whole_seconds();
        let item_threshold_reached = info.items_since_last_progress_save() >= DECOMMISSION_PROGRESS_SAVE_ITEM_THRESHOLD;
        if !time_threshold_reached && !item_threshold_reached {
            return Ok(None);
        }

        Ok(Some(DecommissionProgressCheckpoint {
            start_time: info.start_time,
            queued: info.queued,
            counted_items: info.counted_items(),
            checkpoint_at: now,
        }))
    }

    fn commit_decommission_progress_checkpoint(&mut self, idx: usize, checkpoint: DecommissionProgressCheckpoint) -> bool {
        let Some(pool) = self.pools.get_mut(idx) else {
            return false;
        };
        let Some(info) = pool.decommission.as_mut() else {
            return false;
        };

        if info.start_time != checkpoint.start_time
            || info.queued != checkpoint.queued
            || !is_decommission_active(info.complete, info.failed, info.canceled)
        {
            return false;
        }

        info.progress_save_item_baseline = info.progress_save_item_baseline.max(checkpoint.counted_items);
        info.progress_save_retry_after = None;
        pool.last_update = pool.last_update.max(checkpoint.checkpoint_at);
        true
    }

    fn defer_decommission_progress_checkpoint(
        &mut self,
        idx: usize,
        checkpoint: DecommissionProgressCheckpoint,
        retry_after: OffsetDateTime,
    ) {
        let Some(pool) = self.pools.get_mut(idx) else {
            return;
        };
        let Some(info) = pool.decommission.as_mut() else {
            return;
        };

        if info.start_time == checkpoint.start_time
            && info.queued == checkpoint.queued
            && is_decommission_active(info.complete, info.failed, info.canceled)
        {
            info.progress_save_retry_after = Some(retry_after);
        }
    }

    fn load_from_config_data(&mut self, data: Vec<u8>) -> Result<()> {
        if data.is_empty() {
            return Ok(());
        } else if data.len() <= 4 {
            return Err(Error::other("pool metadata load failed: metadata payload is too short"));
        }

        let format = LittleEndian::read_u16(&data[0..2]);
        if format != POOL_META_FORMAT {
            return Err(Error::other(format!("pool metadata load failed: unknown format {format}")));
        }
        let version = LittleEndian::read_u16(&data[2..4]);
        if !matches!(version, POOL_META_V1_VERSION | POOL_META_VERSION | POOL_META_GENERATION_VERSION) {
            return Err(Error::other(format!("pool metadata load failed: unknown version {version}")));
        }

        if version == POOL_META_GENERATION_VERSION {
            let PoolMetaReplica::Valid {
                meta, committed: true, ..
            } = decode_pool_meta_replica(data)
            else {
                return Err(Error::other(
                    "pool metadata load failed: V3 payload is corrupt, incompatible, or not committed",
                ));
            };
            *self = meta;
            return Ok(());
        }

        *self = Self::decode_pool_meta_payload(version, &data[4..])?;

        if !matches!(self.version, POOL_META_V1_VERSION | POOL_META_VERSION | POOL_META_GENERATION_VERSION) {
            return Err(Error::other(format!(
                "pool metadata load failed: unexpected decoded version {}",
                self.version
            )));
        }
        Ok(())
    }

    pub async fn load(&mut self, pool: Arc<Sets>, pools: Vec<Arc<Sets>>) -> Result<()> {
        let pool_meta_lock = pool.new_ns_lock(RUSTFS_META_BUCKET, POOL_META_NAME).await?;
        let _pool_meta_guard = pool_meta_lock.get_read_lock(get_lock_acquire_timeout()).await?;
        let replica_state = self.load_no_lock_from_replicas(pools).await?;
        replica_state.ensure_write_safe("pool metadata load failed")?;
        Ok(())
    }

    /// Loads every pool metadata replica while the caller owns the metadata fence
    /// or before the namespace-lock RPC surface is ready during startup.
    pub(crate) async fn load_no_lock_from_replicas<S>(&mut self, pools: Vec<Arc<S>>) -> Result<PoolMetaReplicaState>
    where
        S: EcstoreObjectIO,
    {
        let selection = load_pool_meta_replicas(pools, true).await?;
        *self = selection.meta;
        Ok(selection.replica_state)
    }

    pub(crate) async fn load_no_lock_from_replicas_observing<S>(
        &mut self,
        pools: Vec<Arc<S>>,
        write_state: &mut PoolMetaWriteState,
    ) -> Result<PoolMetaReplicaState>
    where
        S: EcstoreObjectIO,
    {
        let selection = load_pool_meta_replicas_observing(pools, true, write_state).await?;
        *self = selection.meta;
        Ok(selection.replica_state)
    }

    #[cfg(test)]
    fn encode_config_data(&self) -> Result<Vec<u8>> {
        self.encode_config_data_for_v2_gate(pool_meta_v2_writer_enabled())
    }

    fn encode_config_data_for_v2_gate(&self, v2_enabled: bool) -> Result<Vec<u8>> {
        if self.dont_save {
            return Ok(Vec::new());
        }
        if self.version == POOL_META_GENERATION_VERSION {
            return Err(Error::other(
                "pool metadata V3 save requires cluster identity and generation transaction context",
            ));
        }
        if !matches!(self.version, 0 | POOL_META_V1_VERSION | POOL_META_VERSION) {
            return Err(Error::other(format!(
                "pool metadata save failed: unexpected runtime version {}",
                self.version
            )));
        }
        let version = if self.version == POOL_META_VERSION || v2_enabled {
            POOL_META_VERSION
        } else {
            POOL_META_V1_VERSION
        };
        if version == POOL_META_V1_VERSION
            && self
                .pools
                .iter()
                .filter_map(|pool| pool.decommission.as_ref())
                .any(|info| !info.unresolved_entries.is_empty())
        {
            return Err(Error::other(format!(
                "pool metadata V2 is required to persist unresolved decommission entries; enable both {} and {} only after every reader and writer supports V2",
                rustfs_config::ENV_POOL_META_V2_WRITE,
                rustfs_config::ENV_POOL_META_V2_FLEET_CONFIRMED,
            )));
        }
        let mut data = Vec::new();
        data.write_u16::<LittleEndian>(POOL_META_FORMAT)?;
        data.write_u16::<LittleEndian>(version)?;
        let mut buf = Vec::new();
        match version {
            POOL_META_V1_VERSION => PersistedPoolMetaV1::from(self).serialize(&mut Serializer::new(&mut buf))?,
            POOL_META_VERSION => PersistedPoolMeta::from(self).serialize(&mut Serializer::new(&mut buf))?,
            _ => unreachable!("pool metadata writer selected an unsupported version"),
        }
        data.write_all(&buf)?;
        Ok(data)
    }

    #[cfg(test)]
    pub(crate) fn encode_config_data_for_test(&self) -> Result<Vec<u8>> {
        self.encode_config_data_for_v2_gate(true)
    }

    pub async fn save(&self, pools: Vec<Arc<Sets>>) -> Result<()> {
        let pool = pools
            .first()
            .cloned()
            .ok_or_else(|| Error::other("pool metadata save failed: no storage pools available"))?;
        let pool_meta_lock = pool.new_ns_lock(RUSTFS_META_BUCKET, POOL_META_NAME).await?;
        let pool_meta_guard = pool_meta_lock.get_write_lock(get_lock_acquire_timeout()).await?;
        let mut write_state = PoolMetaWriteState::default();
        let indices = (0..self.pools.len()).collect::<Vec<_>>();
        let outcome = self
            .save_no_lock_armed_scoped(pools, &mut write_state, pool_meta_guard.lock_lost_signal(), Some(&indices))
            .await?;
        outcome.disarm();
        Ok(())
    }

    /// Startup has a single elected local writer, so it must not depend on namespace locks here.
    #[cfg(any(test, feature = "test-util"))]
    pub(crate) async fn save_for_startup<S>(&self, pools: Vec<Arc<S>>) -> Result<()>
    where
        S: EcstoreObjectIO,
    {
        let mut write_state = PoolMetaWriteState::for_test_bootstrap();
        self.save_for_startup_observing(pools, &mut write_state).await.map(|_| ())
    }

    pub(crate) async fn save_for_startup_observing<S>(
        &self,
        pools: Vec<Arc<S>>,
        write_state: &mut PoolMetaWriteState,
    ) -> Result<PoolMeta>
    where
        S: EcstoreObjectIO,
    {
        let outcome = self.save_no_lock_armed_scoped(pools, write_state, None, None).await?;
        Ok(outcome.into_committed())
    }

    async fn save_no_lock_with_fence<S>(
        &self,
        pools: Vec<Arc<S>>,
        lock_lost: Option<Arc<rustfs_lock::distributed_lock::LockLostSignal>>,
        indices: &[usize],
    ) -> Result<()>
    where
        S: EcstoreObjectIO,
    {
        let mut write_state = PoolMetaWriteState::default();
        let outcome = self
            .save_no_lock_armed_scoped(pools, &mut write_state, lock_lost, Some(indices))
            .await?;
        outcome.disarm();
        Ok(())
    }

    async fn save_no_lock_with_activation_fence<S>(
        &self,
        pools: Vec<Arc<S>>,
        write_state: &mut PoolMetaWriteState,
        activation_fence: &PoolRebalanceActivationFence,
        indices: &[usize],
    ) -> Result<PoolMetaSaveOutcome>
    where
        S: EcstoreObjectIO,
    {
        write_state.ensure_write_safe("pool metadata activation save failed")?;
        let transaction_arm = write_state.arm_transaction();
        let fence = PoolMetaPersistenceFence::Activation(activation_fence);
        let committed = self
            .save_no_lock_transaction(pools, write_state, &fence, Some(indices))
            .await?;
        Ok(PoolMetaSaveOutcome {
            transaction_arm,
            committed,
        })
    }

    async fn save_no_lock_armed<S>(
        &self,
        pools: Vec<Arc<S>>,
        write_state: &mut PoolMetaWriteState,
        lock_lost: Option<Arc<rustfs_lock::distributed_lock::LockLostSignal>>,
        indices: &[usize],
    ) -> Result<PoolMetaSaveOutcome>
    where
        S: EcstoreObjectIO,
    {
        self.save_no_lock_armed_scoped(pools, write_state, lock_lost, Some(indices))
            .await
    }

    async fn save_no_lock_armed_scoped<S>(
        &self,
        pools: Vec<Arc<S>>,
        write_state: &mut PoolMetaWriteState,
        lock_lost: Option<Arc<rustfs_lock::distributed_lock::LockLostSignal>>,
        indices: Option<&[usize]>,
    ) -> Result<PoolMetaSaveOutcome>
    where
        S: EcstoreObjectIO,
    {
        write_state.ensure_write_safe("pool metadata save failed")?;
        // The arm does not block its own transaction. If this future or its
        // returned outcome is dropped before publication, Drop latches the
        // sticky recovery gate.
        let transaction_arm = write_state.arm_transaction();
        let fence = PoolMetaPersistenceFence::Distributed(lock_lost);
        let committed = self.save_no_lock_transaction(pools, write_state, &fence, indices).await?;
        Ok(PoolMetaSaveOutcome {
            transaction_arm,
            committed,
        })
    }

    async fn save_no_lock_transaction<S>(
        &self,
        pools: Vec<Arc<S>>,
        write_state: &mut PoolMetaWriteState,
        fence: &PoolMetaPersistenceFence<'_>,
        indices: Option<&[usize]>,
    ) -> Result<PoolMeta>
    where
        S: EcstoreObjectIO,
    {
        if pools.is_empty() {
            return Err(Error::other("pool metadata save failed: no storage pools available"));
        }
        if self.dont_save {
            return Ok(self.clone());
        }
        for attempt in 0..POOL_META_CAS_MAX_ATTEMPTS {
            match self
                .save_no_lock_transaction_once(pools.clone(), write_state, fence, indices)
                .await
            {
                Ok(committed) => return Ok(committed),
                Err(Error::PreconditionFailed) if attempt + 1 < POOL_META_CAS_MAX_ATTEMPTS => continue,
                Err(err) => return Err(err),
            }
        }
        Err(Error::PreconditionFailed)
    }

    async fn save_no_lock_transaction_once<S>(
        &self,
        pools: Vec<Arc<S>>,
        write_state: &mut PoolMetaWriteState,
        fence: &PoolMetaPersistenceFence<'_>,
        indices: Option<&[usize]>,
    ) -> Result<PoolMeta>
    where
        S: EcstoreObjectIO,
    {
        fence.ensure_held()?;
        let selection = load_pool_meta_replicas_observing(pools.clone(), true, write_state).await?;
        write_state.observe_replicas(selection.replica_state);
        write_state.ensure_write_safe("pool metadata save failed")?;
        let mut bootstrap_generation_required = false;
        if let Some(cluster_id) = write_state.expected_cluster_id {
            let identity = load_pool_meta_identity_selection_observing(pools.clone(), write_state, cluster_id).await?;
            bootstrap_generation_required = selection.absent || write_state.identity_initialized == Some(false);
            if !identity.repair_write_safe {
                write_state.block_writes();
                return Err(Error::other(
                    "pool metadata recovery required: cluster identity has an unreadable replica",
                ));
            }
            if let Some(identity) = identity.identity
                && selection.revision.is_generation_protocol()
                && identity.epoch != selection.revision.epoch
            {
                write_state.block_writes();
                return Err(Error::other(format!(
                    "pool metadata recovery required: committed epoch {} does not match cluster identity epoch {}",
                    selection.revision.epoch, identity.epoch
                )));
            }
            if !selection.absent && write_state.identity_requires_repair() {
                let initialized = write_state.identity_initialized != Some(false) || selection.revision.is_generation_protocol();
                persist_pool_meta_identity(pools.clone(), write_state, initialized, fence).await?;
            }
        }
        // Startup is the only path allowed to create an all-missing metadata
        // set. Runtime callers without an identity context must fail closed.
        write_state.ensure_missing_metadata_can_initialize()?;

        let mut committed = if let Some(indices) = indices {
            if selection.meta.pools.is_empty() {
                self.clone()
            } else {
                let mut requested = self.clone();
                requested.version = selection.meta.version;
                let mut latest = selection.meta.clone();
                merge_pool_meta_updates_for_save(&mut latest, &requested, indices, "pool metadata save failed")?;
                latest
            }
        } else {
            self.clone()
        };
        let target_version = if selection.generation_protocol_observed
            || selection.revision.is_generation_protocol()
            || pool_meta_v3_writer_enabled()
            || bootstrap_generation_required
        {
            POOL_META_GENERATION_VERSION
        } else if selection.meta.version == POOL_META_VERSION
            || self.version == POOL_META_VERSION
            || pool_meta_v2_writer_enabled()
        {
            POOL_META_VERSION
        } else {
            POOL_META_V1_VERSION
        };
        committed.version = target_version;

        if target_version != POOL_META_GENERATION_VERSION {
            let data = committed.encode_config_data_for_v2_gate(target_version == POOL_META_VERSION)?;
            let mut canonical_saved = false;
            for (pool_index, (pool, token)) in pools.iter().cloned().zip(&selection.cas_tokens).enumerate() {
                let result =
                    save_pool_meta_object_cas(pool.clone(), POOL_META_NAME, data.clone(), token, fence, "legacy_cas").await;
                if result.is_ok() && pool_index == 0 {
                    canonical_saved = true;
                    #[cfg(test)]
                    if let PoolMetaPersistenceFence::Activation(activation_fence) = fence {
                        pause_pool_activation_after_durable_save(&pool, activation_fence).await;
                    }
                }
                if let Err(err) = result {
                    if canonical_saved && fence.is_activation() {
                        warn!(
                            event = EVENT_DECOMMISSION_STATE,
                            component = LOG_COMPONENT_ECSTORE,
                            subsystem = LOG_SUBSYSTEM_POOLS,
                            pool_index,
                            state = "activation_replica_repair_pending",
                            error = %err,
                            "Decommission activation replica repair pending"
                        );
                        continue;
                    }
                    return Err(err);
                }
            }
            let confirmed = if fence.is_activation() {
                load_pool_meta_replicas(pools, true).await?
            } else {
                let confirmed = load_pool_meta_replicas_observing(pools, true, write_state).await?;
                write_state.observe_replicas(confirmed.replica_state);
                confirmed
            };
            let expected = committed.encode_config_data_for_v2_gate(true)?;
            if confirmed.canonical.as_ref() != Some(&expected) {
                record_pool_meta_stale_write_rejection("legacy_verify");
                return Err(Error::PreconditionFailed);
            }
            return Ok(confirmed.meta);
        }

        let cluster_id = selection
            .revision
            .cluster_id
            .or(selection.generation_identity.map(|(cluster_id, _)| cluster_id))
            .or(write_state.expected_cluster_id)
            .ok_or_else(|| Error::other("pool metadata V3 save failed: cluster identity is not initialized"))?;
        let epoch = if selection.revision.is_generation_protocol() {
            selection.revision.epoch
        } else {
            selection
                .generation_identity
                .map(|(_, epoch)| epoch)
                .or(write_state.cluster_epoch)
                .unwrap_or(POOL_META_INITIAL_EPOCH)
        };
        let generation = if selection.revision.is_generation_protocol() {
            selection
                .revision
                .generation
                .checked_add(1)
                .ok_or_else(|| Error::other("pool metadata V3 generation exhausted"))?
        } else {
            1
        };
        let revision = PoolMetaRevision {
            version: POOL_META_GENERATION_VERSION,
            cluster_id: Some(cluster_id),
            epoch,
            generation,
            transaction_id: Some(uuid::Uuid::new_v4()),
        };
        let previous = if let Some(canonical) = selection.canonical.clone() {
            PoolMetaCommittedCandidate {
                canonical,
                meta: selection.meta.clone(),
                revision: selection.revision,
            }
        } else {
            let empty = PoolMeta {
                version: POOL_META_V1_VERSION,
                ..Default::default()
            };
            PoolMetaCommittedCandidate {
                canonical: empty.encode_config_data_for_v2_gate(true)?,
                meta: empty,
                revision: PoolMetaRevision::legacy(POOL_META_V1_VERSION),
            }
        };
        let pending = encode_pool_meta_v3_envelope(&committed, revision, false, Some(&previous))?;
        let durable = encode_pool_meta_v3_envelope(&committed, revision, true, None)?;
        let mut pending_tokens = Vec::with_capacity(pools.len());
        for (pool, token) in pools.iter().cloned().zip(&selection.cas_tokens) {
            let object_info =
                save_pool_meta_object_cas(pool, POOL_META_NAME, pending.clone(), token, fence, "prepare_cas").await?;
            let etag = object_info
                .etag
                .filter(|etag| !etag.trim().is_empty())
                .ok_or_else(|| Error::other("pool metadata V3 prepare succeeded without a conditional-write revision"))?;
            pending_tokens.push(PoolMetaCasToken::Existing(etag));
        }

        let mut commit_error = None;
        let mut commit_succeeded = false;
        #[cfg(test)]
        let mut first_pool = true;
        for (pool, token) in pools.iter().cloned().zip(&pending_tokens) {
            match save_pool_meta_object_cas(pool.clone(), POOL_META_NAME, durable.clone(), token, fence, "commit_cas").await {
                Ok(_) => {
                    commit_succeeded = true;
                    #[cfg(test)]
                    if first_pool && let PoolMetaPersistenceFence::Activation(activation_fence) = fence {
                        pause_pool_activation_after_durable_save(&pool, activation_fence).await;
                    }
                }
                Err(err) => {
                    commit_error.get_or_insert(err);
                }
            }
            #[cfg(test)]
            {
                first_pool = false;
            }
        }
        let confirmed = if fence.is_activation() {
            load_pool_meta_replicas(pools.clone(), true).await?
        } else {
            let confirmed = load_pool_meta_replicas_observing(pools.clone(), true, write_state).await?;
            write_state.observe_replicas(confirmed.replica_state);
            confirmed
        };
        if confirmed.revision == revision && confirmed.canonical.as_ref() == Some(&durable) {
            persist_pool_meta_identity(pools, write_state, true, fence).await?;
            return Ok(confirmed.meta);
        }
        if !commit_succeeded {
            return Err(commit_error.unwrap_or(Error::PreconditionFailed));
        }
        Err(commit_error.unwrap_or_else(|| {
            Error::other("pool metadata recovery required: committed V3 transaction was not selected after write")
        }))
    }

    #[cfg(test)]
    async fn save_no_lock_observing<S>(&self, pools: Vec<Arc<S>>, write_state: &mut PoolMetaWriteState) -> Result<()>
    where
        S: EcstoreObjectIO,
    {
        let indices = (0..self.pools.len()).collect::<Vec<_>>();
        let outcome = self.save_no_lock_armed(pools, write_state, None, &indices).await?;
        outcome.disarm();
        Ok(())
    }

    pub fn decommission_cancel(&mut self, idx: usize) -> bool {
        if let Some(stats) = self.pools.get_mut(idx) {
            if let Some(d) = &stats.decommission {
                if !d.canceled {
                    stats.last_update = OffsetDateTime::now_utc();

                    let mut pd = d.clone();
                    pd.canceled = true;
                    pd.failed = false;
                    pd.complete = false;
                    pd.start_time = None;
                    pd.terminal_reload_attempt_at = None;
                    pd.terminal_reload_failures.clear();

                    stats.decommission = Some(pd);
                    true
                } else {
                    false
                }
            } else {
                false
            }
        } else {
            false
        }
    }
    pub fn decommission_failed(&mut self, idx: usize) -> bool {
        if let Some(stats) = self.pools.get_mut(idx) {
            if let Some(d) = &stats.decommission {
                if is_decommission_active(d.complete, d.failed, d.canceled) {
                    stats.last_update = OffsetDateTime::now_utc();

                    let mut pd = d.clone();
                    pd.canceled = false;
                    pd.failed = true;
                    pd.complete = false;
                    pd.start_time = None;
                    pd.terminal_reload_attempt_at = None;
                    pd.terminal_reload_failures.clear();

                    stats.decommission = Some(pd);
                    true
                } else {
                    false
                }
            } else {
                false
            }
        } else {
            false
        }
    }

    pub fn clear_decommission(&mut self, idx: usize) -> Result<bool> {
        let pool_count = self.pools.len();
        ensure_valid_decommission_pool_index(pool_count, idx)?;

        let Some(pool) = self.pools.get_mut(idx) else {
            return Err(invalid_decommission_pool_index_error(pool_count, idx));
        };

        let (decommission_present, complete, failed, canceled, unresolved_entries) = pool
            .decommission
            .as_ref()
            .map(|info| {
                (
                    info.has_decommission_state(),
                    info.complete,
                    info.failed,
                    info.canceled,
                    info.unresolved_entries.len(),
                )
            })
            .unwrap_or((false, false, false, false, 0));

        ensure_decommission_clear_allowed(true, decommission_present, complete, failed, canceled, unresolved_entries)?;

        pool.last_update = OffsetDateTime::now_utc();
        pool.decommission = None;
        Ok(true)
    }

    pub fn decommission_complete(&mut self, idx: usize) -> bool {
        if let Some(stats) = self.pools.get_mut(idx) {
            if let Some(d) = &stats.decommission {
                if is_decommission_active(d.complete, d.failed, d.canceled) {
                    stats.last_update = OffsetDateTime::now_utc();

                    let mut pd = d.clone();
                    pd.canceled = false;
                    pd.failed = false;
                    pd.complete = true;
                    pd.terminal_reload_attempt_at = None;
                    pd.terminal_reload_failures.clear();

                    stats.decommission = Some(pd);
                    true
                } else {
                    false
                }
            } else {
                false
            }
        } else {
            false
        }
    }
    fn set_decommission_state(&mut self, idx: usize, pi: PoolSpaceInfo, queued: bool) -> Result<()> {
        let pool_count = self.pools.len();
        ensure_valid_decommission_pool_index(pool_count, idx)?;

        let Some(pool) = self.pools.get_mut(idx) else {
            return Err(invalid_decommission_pool_index_error(pool_count, idx));
        };

        ensure_decommission_start_allowed(decommission_start_pool_state(Some(pool)))?;

        let previous = pool.decommission.as_ref();
        let now = OffsetDateTime::now_utc();
        pool.last_update = now;
        pool.decommission = Some(build_decommission_start_state(pi, queued, now, previous));

        Ok(())
    }

    pub fn decommission(&mut self, idx: usize, pi: PoolSpaceInfo) -> Result<()> {
        self.set_decommission_state(idx, pi, false)
    }

    pub fn queue_decommission(&mut self, idx: usize, pi: PoolSpaceInfo) -> Result<()> {
        self.set_decommission_state(idx, pi, true)
    }

    pub fn record_decommission_terminal_reload_failure(&mut self, idx: usize, stage: &str, message: String) -> Result<bool> {
        let pool_count = self.pools.len();
        ensure_valid_decommission_pool_index(pool_count, idx)?;

        let Some(pool) = self.pools.get_mut(idx) else {
            return Err(invalid_decommission_pool_index_error(pool_count, idx));
        };
        let Some(info) = pool.decommission.as_mut() else {
            return Err(decommission_metadata_not_initialized_error("record decommission terminal reload failure"));
        };

        let failure = format!("{stage}: {message}");
        if info.terminal_reload_failures.last().is_some_and(|last| last == &failure) {
            return Ok(false);
        }

        pool.last_update = OffsetDateTime::now_utc();
        info.terminal_reload_attempt_at = Some(pool.last_update);
        info.terminal_reload_failures.push(failure);
        Ok(true)
    }

    pub fn promote_queued_decommission(&mut self, idx: usize) -> bool {
        if let Some(pool) = self.pools.get_mut(idx)
            && let Some(info) = pool.decommission.as_mut()
            && info.queued
            && is_decommission_active(info.complete, info.failed, info.canceled)
        {
            let now = OffsetDateTime::now_utc();
            pool.last_update = now;
            info.queued = false;
            let generation = *info.start_time.get_or_insert(now);
            for entry in &mut info.unresolved_entries {
                entry.source_generation = generation;
            }
            return true;
        }

        false
    }
    pub fn queue_buckets(&mut self, idx: usize, bks: Vec<DecomBucketInfo>) {
        if let Some(pool) = self.pools.get_mut(idx)
            && let Some(dec) = pool.decommission.as_mut()
        {
            for bk in bks.iter() {
                dec.bucket_push(bk);
            }
        }
    }
    pub fn pending_buckets(&self, idx: usize) -> Vec<DecomBucketInfo> {
        let mut list = Vec::new();

        if let Some(pool) = self.pools.get(idx)
            && let Some(ref info) = pool.decommission
        {
            for bk in info.queued_buckets.iter() {
                let (name, prefix) = path2_bucket_object(bk);
                list.push(DecomBucketInfo { name, prefix });
            }
        }

        list
    }

    pub fn is_bucket_decommissioned(&self, idx: usize, bucket: String) -> bool {
        self.pools
            .get(idx)
            .and_then(|pool| pool.decommission.as_ref())
            .is_some_and(|info| info.is_bucket_decommissioned(&bucket))
    }

    pub fn bucket_done(&mut self, idx: usize, bucket: String) -> bool {
        if let Some(pool) = self.pools.get_mut(idx) {
            if let Some(info) = pool.decommission.as_mut() {
                info.bucket_pop(&bucket)
            } else {
                false
            }
        } else {
            false
        }
    }

    pub fn count_item(&mut self, idx: usize, size: usize, failed: bool) {
        if let Some(pool) = self.pools.get_mut(idx)
            && let Some(info) = pool.decommission.as_mut()
        {
            if failed {
                info.items_decommission_failed += 1;
                info.bytes_failed += size;
            } else {
                info.items_decommissioned += 1;
                info.bytes_done += size;
            }
        }
    }

    pub fn track_current_bucket_object(&mut self, idx: usize, bucket: String, object: String) {
        self.track_current_bucket_object_stage(idx, bucket, object, String::new());
    }

    pub fn track_current_bucket_object_stage(&mut self, idx: usize, bucket: String, object: String, stage: String) {
        if self.pools.get(idx).is_none_or(|v| v.decommission.is_none()) {
            return;
        }

        if let Some(pool) = self.pools.get_mut(idx)
            && let Some(info) = pool.decommission.as_mut()
        {
            info.object = object;
            info.bucket = bucket;
            info.stage = stage;
        }
    }

    pub fn update_after(&mut self, idx: usize, duration: Duration) -> Result<bool> {
        Ok(self
            .decommission_progress_checkpoint(idx, duration, OffsetDateTime::now_utc())?
            .is_some())
    }

    pub fn validate(&self, pools: Vec<Arc<Sets>>) -> Result<bool> {
        struct PoolInfo {
            position: usize,
            completed: bool,
            #[allow(dead_code, reason = "written but never read back (backlog#1823)")]
            decom_started: bool,
        }

        let mut remembered_pools = HashMap::new();
        for (idx, pool) in self.pools.iter().enumerate() {
            let mut complete = false;
            let mut decom_started = false;
            if let Some(decommission) = &pool.decommission {
                if decommission.complete {
                    complete = true;
                }
                decom_started = true;
            }
            remembered_pools.insert(
                pool.cmd_line.clone(),
                PoolInfo {
                    position: idx,
                    completed: complete,
                    decom_started,
                },
            );
        }

        let mut specified_pools = HashMap::new();
        for (idx, pool) in pools.iter().enumerate() {
            specified_pools.insert(pool.endpoints.cmd_line.clone(), idx);
        }

        let mut update = false;

        // Determine whether the selected pool should be removed from the retired list.
        for k in specified_pools.keys() {
            if let Some(pi) = remembered_pools.get(k) {
                ensure_pool_not_left_in_cmdline_after_decommission(pi.position, k, pi.completed)?;
            } else {
                // If the previous pool no longer exists, allow updates because a new pool may have been added.
                update = true;
            }
        }

        if specified_pools.len() == remembered_pools.len() {
            for (k, pi) in remembered_pools.iter() {
                if let Some(pos) = specified_pools.get(k)
                    && *pos != pi.position
                {
                    update = true; // Pool order changed, allow the update.
                }
            }
        }

        if !update {
            update = specified_pools.len() != remembered_pools.len();
        }

        Ok(update)
    }

    pub fn return_resumable_pools(&self) -> Vec<PoolStatus> {
        resumable_decommission_queue_indices(self)
            .into_iter()
            .map(|idx| self.pools[idx].clone())
            .collect()
    }
}

pub fn path2_bucket_object(name: &str) -> (String, String) {
    path_to_bucket_object(name)
}

pub fn path2_bucket_object_with_base_path(base_path: &str, path: &str) -> (String, String) {
    path_to_bucket_object_with_base_path(base_path, path)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DecommissionUnresolvedEntry {
    pub bucket: String,
    pub object: String,
    #[serde(rename = "poolIndex")]
    pub pool_index: usize,
    #[serde(rename = "setIndex")]
    pub set_index: usize,
    #[serde(rename = "sourceGeneration", with = "time::serde::rfc3339")]
    pub source_generation: OffsetDateTime,
    #[serde(rename = "candidateCount")]
    pub candidate_count: usize,
    #[serde(rename = "diskErrorCount")]
    pub disk_error_count: usize,
    #[serde(rename = "observedAt", with = "time::serde::rfc3339")]
    pub observed_at: OffsetDateTime,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PoolDecommissionInfo {
    #[serde(rename = "startTime", with = "time::serde::rfc3339::option")]
    pub start_time: Option<OffsetDateTime>,
    #[serde(rename = "startSize")]
    pub start_size: usize,
    #[serde(rename = "totalSize")]
    pub total_size: usize,
    #[serde(rename = "currentSize")]
    pub current_size: usize,
    #[serde(rename = "complete")]
    pub complete: bool,
    #[serde(rename = "failed")]
    pub failed: bool,
    #[serde(rename = "canceled")]
    pub canceled: bool,
    #[serde(skip)]
    pub queued: bool,

    #[serde(skip)]
    pub queued_buckets: Vec<String>,
    #[serde(skip)]
    pub decommissioned_buckets: Vec<String>,
    #[serde(skip)]
    pub bucket: String,
    #[serde(skip)]
    pub prefix: String,
    #[serde(skip)]
    pub object: String,
    #[serde(skip)]
    pub stage: String,

    #[serde(rename = "objectsDecommissioned")]
    pub items_decommissioned: usize,
    #[serde(rename = "objectsDecommissionedFailed")]
    pub items_decommission_failed: usize,
    #[serde(rename = "bytesDecommissioned")]
    pub bytes_done: usize,
    #[serde(rename = "bytesDecommissionedFailed")]
    pub bytes_failed: usize,
    #[serde(rename = "terminalReloadAttemptAt", with = "time::serde::rfc3339::option", default)]
    pub terminal_reload_attempt_at: Option<OffsetDateTime>,
    #[serde(rename = "terminalReloadFailures", default)]
    pub terminal_reload_failures: Vec<String>,
    #[serde(skip)]
    pub unresolved_entries: Vec<DecommissionUnresolvedEntry>,
    #[serde(skip)]
    pub progress_save_item_baseline: usize,
    #[serde(skip)]
    pub progress_save_retry_after: Option<OffsetDateTime>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DecommissionProgressCheckpoint {
    start_time: Option<OffsetDateTime>,
    queued: bool,
    counted_items: usize,
    checkpoint_at: OffsetDateTime,
}

impl PoolDecommissionInfo {
    pub fn has_decommission_state(&self) -> bool {
        self.complete
            || self.failed
            || self.canceled
            || self.queued
            || self.start_time.is_some()
            || self.start_size > 0
            || !self.queued_buckets.is_empty()
            || !self.decommissioned_buckets.is_empty()
            || !self.bucket.is_empty()
            || !self.prefix.is_empty()
            || !self.object.is_empty()
            || !self.stage.is_empty()
            || self.items_decommissioned > 0
            || self.items_decommission_failed > 0
            || self.bytes_done > 0
            || self.bytes_failed > 0
            || self.terminal_reload_attempt_at.is_some()
            || !self.terminal_reload_failures.is_empty()
            || !self.unresolved_entries.is_empty()
    }

    fn counted_items(&self) -> usize {
        self.items_decommissioned.saturating_add(self.items_decommission_failed)
    }

    fn items_since_last_progress_save(&self) -> usize {
        self.counted_items().saturating_sub(self.progress_save_item_baseline)
    }

    fn mark_progress_saved(&mut self) {
        self.progress_save_item_baseline = self.counted_items();
        self.progress_save_retry_after = None;
    }

    pub fn bucket_push(&mut self, bucket: &DecomBucketInfo) {
        let bucket_key = bucket.to_string();
        if self.is_bucket_decommissioned(&bucket_key) {
            return;
        }

        for b in self.queued_buckets.iter() {
            if b == &bucket_key {
                return;
            }
        }

        self.queued_buckets.push(bucket_key);

        self.bucket = bucket.name.clone();
        self.prefix = bucket.prefix.clone();
    }
    pub fn is_bucket_decommissioned(&self, bucket: &String) -> bool {
        for b in self.decommissioned_buckets.iter() {
            if b == bucket {
                return true;
            }
        }
        false
    }
    pub fn bucket_pop(&mut self, bucket: &String) -> bool {
        self.decommissioned_buckets.push(bucket.clone());

        let mut found = None;
        for (i, b) in self.queued_buckets.iter().enumerate() {
            if b == bucket {
                found = Some(i);
                break;
            }
        }

        if let Some(i) = found {
            self.queued_buckets.remove(i);
            if &self.bucket == bucket {
                self.bucket = "".to_owned();
                self.prefix = "".to_owned();
                self.object = "".to_owned();
            }

            return true;
        }
        false
    }
}

#[derive(Debug, Clone, Copy)]
pub struct PoolSpaceInfo {
    pub free: usize,
    pub total: usize,
    pub used: usize,
}

#[cfg(test)]
type DecommissionSpaceInfoOverrides = std::sync::Mutex<HashMap<uuid::Uuid, Vec<(usize, PoolSpaceInfo)>>>;

#[cfg(test)]
static DECOMMISSION_SPACE_INFO_OVERRIDES: std::sync::OnceLock<DecommissionSpaceInfoOverrides> = std::sync::OnceLock::new();

#[cfg(test)]
pub(crate) fn set_decommission_space_info_override_for_test(store_id: uuid::Uuid, space_infos: Vec<(usize, PoolSpaceInfo)>) {
    DECOMMISSION_SPACE_INFO_OVERRIDES
        .get_or_init(|| std::sync::Mutex::new(HashMap::new()))
        .lock()
        .expect("decommission space info override should not be poisoned")
        .insert(store_id, space_infos);
}

#[cfg(test)]
fn take_decommission_space_info_override_for_test(store_id: uuid::Uuid) -> Option<Vec<(usize, PoolSpaceInfo)>> {
    DECOMMISSION_SPACE_INFO_OVERRIDES
        .get_or_init(|| std::sync::Mutex::new(HashMap::new()))
        .lock()
        .expect("decommission space info override should not be poisoned")
        .remove(&store_id)
}

#[derive(Debug, Default, Clone)]
pub struct DecomBucketInfo {
    pub name: String,
    pub prefix: String,
}

impl Display for DecomBucketInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            path_join(&[PathBuf::from(self.name.clone()), PathBuf::from(self.prefix.clone())]).to_string_lossy()
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DecommissionFinalState {
    Complete,
    Failed,
}

fn determine_decommission_final_state(items_failed: usize, was_cancelled: bool) -> DecommissionFinalState {
    if items_failed > 0 || was_cancelled {
        DecommissionFinalState::Failed
    } else {
        DecommissionFinalState::Complete
    }
}

fn decommission_remaining_version_count(versions: &[rustfs_filemeta::FileInfo], expired: usize) -> usize {
    versions
        .iter()
        .filter(|version| !version.tier_free_version())
        .count()
        .saturating_sub(expired)
}

fn should_skip_decommission_delete_marker(
    version: &rustfs_filemeta::FileInfo,
    remaining_versions: usize,
    replication_configured: bool,
) -> bool {
    // Match MinIO decommission behavior: an empty delete marker is not moved to
    // another pool unless replication is configured and its marker state matters.
    version.deleted && remaining_versions == 1 && !replication_configured
}

fn decommission_delete_marker_opts(
    version: &rustfs_filemeta::FileInfo,
    version_id: Option<String>,
    src_pool_idx: usize,
    expected_bucket_incarnation_id: Option<uuid::Uuid>,
) -> ObjectOptions {
    let version_suspended = version.version_id.is_none() && version_id.is_none();
    ObjectOptions {
        versioned: !version_suspended,
        version_suspended,
        version_id: version_id.or_else(|| version_suspended.then(|| uuid::Uuid::nil().to_string())),
        mod_time: version.mod_time,
        src_pool_idx,
        data_movement: true,
        delete_marker: true,
        skip_decommissioned: true,
        expected_bucket_incarnation_id,
        delete_replication: version
            .replication_state_internal
            .as_ref()
            .map(replication_state_from_filemeta),
        ..Default::default()
    }
}

fn decommission_object_migration_read_opts(version_id: Option<String>) -> ObjectOptions {
    ObjectOptions {
        version_id,
        no_lock: true,
        data_movement: true,
        raw_data_movement_read: true,
        skip_decommissioned: true,
        skip_rebalancing: true,
        ..Default::default()
    }
}

fn decommission_remote_tiered_opts(
    version: &rustfs_filemeta::FileInfo,
    version_id: Option<String>,
    src_pool_idx: usize,
    expected_bucket_incarnation_id: Option<uuid::Uuid>,
) -> ObjectOptions {
    ObjectOptions {
        versioned: version_id.is_some(),
        version_id,
        mod_time: version.mod_time,
        user_defined: version.metadata.clone(),
        src_pool_idx,
        data_movement: true,
        incl_free_versions: version.tier_free_version(),
        include_part_checksums: true,
        http_preconditions: Some(data_movement::data_movement_target_precondition()),
        expected_bucket_incarnation_id,
        ..Default::default()
    }
}

fn lifecycle_action_removes_data_movement_version(action: IlmAction) -> bool {
    matches!(
        action,
        IlmAction::DeleteVersionAction | IlmAction::DeleteAllVersionsAction | IlmAction::DelMarkerDeleteAllVersionsAction
    )
}

fn lifecycle_action_skips_heal_version(action: IlmAction) -> bool {
    action.delete()
}

#[cfg(test)]
struct LifecycleDataMovementMutationBarrierState {
    bucket: String,
    object: String,
    arrived: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

#[cfg(test)]
pub(crate) struct LifecycleDataMovementMutationBarrier {
    state: Arc<LifecycleDataMovementMutationBarrierState>,
}

#[cfg(test)]
static LIFECYCLE_DATA_MOVEMENT_MUTATION_BARRIER: std::sync::OnceLock<
    std::sync::Mutex<Option<Arc<LifecycleDataMovementMutationBarrierState>>>,
> = std::sync::OnceLock::new();

#[cfg(test)]
impl LifecycleDataMovementMutationBarrier {
    pub(crate) fn install(bucket: &str, object: &str) -> Self {
        let state = Arc::new(LifecycleDataMovementMutationBarrierState {
            bucket: bucket.to_string(),
            object: object.to_string(),
            arrived: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        });
        let mut slot = LIFECYCLE_DATA_MOVEMENT_MUTATION_BARRIER
            .get_or_init(|| std::sync::Mutex::new(None))
            .lock()
            .expect("lifecycle data movement mutation barrier should not be poisoned");
        assert!(slot.is_none(), "lifecycle data movement mutation barrier must be unique");
        *slot = Some(Arc::clone(&state));
        Self { state }
    }

    pub(crate) async fn wait_until_paused(&self) {
        tokio::time::timeout(std::time::Duration::from_secs(30), self.state.arrived.notified())
            .await
            .expect("lifecycle data movement should reach its mutation boundary");
    }

    pub(crate) fn release(&self) {
        self.state.release.notify_one();
    }
}

#[cfg(test)]
impl Drop for LifecycleDataMovementMutationBarrier {
    fn drop(&mut self) {
        self.state.release.notify_one();
        let mut slot = LIFECYCLE_DATA_MOVEMENT_MUTATION_BARRIER
            .get_or_init(|| std::sync::Mutex::new(None))
            .lock()
            .expect("lifecycle data movement mutation barrier should not be poisoned");
        if slot.as_ref().is_some_and(|state| Arc::ptr_eq(state, &self.state)) {
            *slot = None;
        }
    }
}

#[cfg(test)]
async fn pause_lifecycle_data_movement_mutation(bucket: &str, object: &str, has_run_fence_signal: bool) {
    if !has_run_fence_signal {
        return;
    }
    let barrier = LIFECYCLE_DATA_MOVEMENT_MUTATION_BARRIER
        .get_or_init(|| std::sync::Mutex::new(None))
        .lock()
        .expect("lifecycle data movement mutation barrier should not be poisoned")
        .as_ref()
        .filter(|barrier| barrier.bucket == bucket && barrier.object == object)
        .cloned();
    if let Some(barrier) = barrier {
        barrier.arrived.notify_one();
        barrier.release.notified().await;
    }
}

fn resolve_data_movement_lifecycle_expiry_result(action: IlmAction, apply_actions: bool, applied: bool) -> Result<bool> {
    if !apply_actions || applied {
        return Ok(true);
    }

    Err(Error::other(format!(
        "failed to apply lifecycle expiry action {action:?} during data movement"
    )))
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn should_skip_lifecycle_for_data_movement(
    store: Arc<ECStore>,
    bucket: &str,
    version: &rustfs_filemeta::FileInfo,
    lifecycle_config: Option<&BucketLifecycleConfiguration>,
    object_lock_config: Option<&ObjectLockConfiguration>,
    apply_actions: bool,
    event_source: &LcEventSrc,
    lock_lost_signal: Option<Arc<rustfs_lock::distributed_lock::LockLostSignal>>,
) -> Result<bool> {
    let Some(lifecycle_config) = lifecycle_config else {
        return Ok(false);
    };

    let versioned = BucketVersioningSys::prefix_enabled(bucket, &version.name).await;
    let object_info = crate::object_api::ObjectInfo::from_file_info(version, bucket, &version.name, versioned);
    let event = eval_action_from_lifecycle(lifecycle_config, object_lock_config, &object_info).await;

    match event.action {
        IlmAction::DeleteRestoredAction | IlmAction::DeleteRestoredVersionAction => {
            if apply_actions && object_info.is_remote() {
                let Ok(bucket_incarnation_id) = store.bucket_incarnation_id_from_disk(bucket).await else {
                    return Ok(false);
                };
                let _ = match lock_lost_signal {
                    Some(signal) => {
                        apply_expiry_rule_for_data_movement(store, &event, event_source, &object_info, Some(signal)).await
                    }
                    None => {
                        apply_expiry_on_transitioned_object(store, &object_info, &event, event_source, bucket_incarnation_id)
                            .await
                    }
                };
            }
            Ok(false)
        }
        action if lifecycle_action_removes_data_movement_version(action) => {
            #[cfg(test)]
            pause_lifecycle_data_movement_mutation(bucket, &version.name, lock_lost_signal.is_some()).await;
            if lifecycle_delete_all_versions_blocked_by_replication(store.clone(), bucket, &object_info.name, action).await? {
                return Ok(false);
            }
            let applied = !apply_actions
                || match lock_lost_signal {
                    Some(signal) => {
                        apply_expiry_rule_for_data_movement(store, &event, event_source, &object_info, Some(signal)).await
                    }
                    None => apply_expiry_rule_in(store, &event, event_source, &object_info).await,
                };
            resolve_data_movement_lifecycle_expiry_result(action, apply_actions, applied)
        }
        _ => Ok(false),
    }
}

pub struct HealLifecycleExpiryContext {
    configs: LifecycleExpiryConfigs,
}

impl ECStore {
    pub async fn load_heal_lifecycle_expiry_context(&self, bucket: &str) -> Result<Option<HealLifecycleExpiryContext>> {
        if bucket == RUSTFS_META_BUCKET {
            return Ok(None);
        }

        let configs = get_expiry_configs(self, bucket).await?;
        if configs.lifecycle.is_none() {
            return Ok(None);
        }

        Ok(Some(HealLifecycleExpiryContext { configs }))
    }

    pub async fn enqueue_heal_lifecycle_expiry(
        self: &Arc<Self>,
        context: &HealLifecycleExpiryContext,
        bucket: &str,
        object: &str,
        version_id: Option<&str>,
        object_info: Option<&crate::object_api::ObjectInfo>,
    ) -> Result<bool> {
        let Some(lifecycle_config) = context.configs.lifecycle.as_ref() else {
            return Ok(false);
        };

        let object_info = if let Some(object_info) = object_info {
            if object_info.bucket != bucket || object_info.name != object {
                return Ok(false);
            }
            let snapshot_version_id = object_info
                .version_id
                .filter(|version_id| !version_id.is_nil())
                .map(|version_id| version_id.to_string());
            if snapshot_version_id.as_deref() != version_id {
                return Ok(false);
            }
            object_info.clone()
        } else {
            match self
                .get_object_info(
                    bucket,
                    object,
                    &ObjectOptions {
                        version_id: version_id.map(str::to_string),
                        versioned: version_id.is_some(),
                        expected_bucket_incarnation_id: Some(context.configs.bucket_incarnation_id),
                        ..Default::default()
                    },
                )
                .await
            {
                Ok(object_info) => object_info,
                Err(err) if is_err_object_not_found(&err) || is_err_version_not_found(&err) => return Ok(false),
                Err(err) => return Err(err),
            }
        };

        let event = eval_action_from_lifecycle(lifecycle_config, context.configs.object_lock.as_deref(), &object_info).await;
        if !lifecycle_action_skips_heal_version(event.action) {
            return Ok(false);
        }

        if lifecycle_delete_all_versions_blocked_by_replication(self.clone(), bucket, &object_info.name, event.action).await? {
            return Ok(false);
        }

        Ok(apply_expiry_rule_in(self.clone(), &event, &LcEventSrc::Scanner, &object_info).await)
    }

    async fn acquire_pool_meta_write_guard(
        &self,
        write_state: &mut PoolMetaWriteState,
        operation: &str,
    ) -> Result<(rustfs_lock::NamespaceLockGuard, PoolMeta)> {
        write_state.ensure_write_safe(operation)?;
        load_pool_meta_identity_observing(self.pools.clone(), write_state).await?;
        let pool = self
            .pools
            .first()
            .cloned()
            .ok_or_else(|| Error::other(format!("{operation}: no storage pools available")))?;
        let pool_meta_lock = pool.new_ns_lock(RUSTFS_META_BUCKET, POOL_META_NAME).await?;
        let pool_meta_guard = pool_meta_lock
            .get_write_lock(get_lock_acquire_timeout())
            .await
            .map_err(activation_pool_meta_lock_error)?;
        let selection = load_pool_meta_replicas_observing(self.pools.clone(), true, write_state).await?;
        write_state.observe_replicas(selection.replica_state);
        write_state.ensure_write_safe(operation)?;
        Ok((pool_meta_guard, selection.meta))
    }

    pub(crate) async fn ensure_pool_meta_side_effects_safe(&self, operation: &str) -> Result<()> {
        self.pool_meta_save_gate.lock().await.ensure_write_safe(operation)
    }

    async fn load_runtime_pool_meta_observing(&self, write_state: &mut PoolMetaWriteState, operation: &str) -> Result<PoolMeta> {
        write_state.ensure_write_safe(operation)?;
        load_pool_meta_identity_observing(self.pools.clone(), write_state).await?;
        let mut pool_meta = PoolMeta::default();
        let replica_state = pool_meta
            .load_no_lock_from_replicas_observing(self.pools.clone(), write_state)
            .await?;
        write_state.observe_replicas(replica_state);
        write_state.ensure_missing_metadata_can_initialize()?;
        write_state.ensure_write_safe(operation)?;
        Ok(pool_meta)
    }

    pub(crate) async fn load_runtime_pool_meta_under_activation_fence(
        &self,
        write_state: &mut PoolMetaWriteState,
        activation_fence: &PoolRebalanceActivationFence,
        operation: &str,
    ) -> Result<PoolMeta> {
        activation_fence.ensure_held()?;
        let pool_meta = self.load_runtime_pool_meta_observing(write_state, operation).await?;
        activation_fence.ensure_held()?;
        Ok(pool_meta)
    }

    pub(crate) async fn load_runtime_pool_meta(&self, operation: &str) -> Result<PoolMeta> {
        let mut write_state = self.pool_meta_save_gate.lock().await;
        write_state.ensure_write_safe(operation)?;
        let pool = self
            .pools
            .first()
            .cloned()
            .ok_or_else(|| Error::other(format!("{operation}: no storage pools available")))?;
        let pool_meta_lock = pool.new_ns_lock(RUSTFS_META_BUCKET, POOL_META_NAME).await?;
        let _pool_meta_guard = pool_meta_lock.get_read_lock(get_lock_acquire_timeout()).await?;
        self.load_runtime_pool_meta_observing(&mut write_state, operation).await
    }

    async fn run_guarded_decommission_side_effect<T, E, F, Fut>(
        &self,
        rx: &CancellationToken,
        operation_gate: &Arc<tokio::sync::RwLock<()>>,
        operation: F,
    ) -> std::result::Result<T, E>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = std::result::Result<T, E>>,
        E: From<Error>,
    {
        let _operation_guard = tokio::select! {
            biased;
            _ = rx.cancelled() => return Err(Error::OperationCanceled.into()),
            guard = operation_gate.read() => guard,
        };

        if rx.is_cancelled() {
            return Err(Error::OperationCanceled.into());
        }
        self.ensure_pool_meta_side_effects_safe("decommission side effect blocked because pool metadata requires recovery")
            .await
            .map_err(E::from)?;
        if rx.is_cancelled() {
            return Err(Error::OperationCanceled.into());
        }

        let result = operation().await;
        if rx.is_cancelled() {
            return Err(Error::OperationCanceled.into());
        }
        result
    }

    async fn save_current_pool_meta(&self, indices: &[usize]) -> Result<()> {
        let mut save_guard = self.pool_meta_save_gate.lock().await;
        let (pool_meta_guard, mut snapshot) = self
            .acquire_pool_meta_write_guard(&mut save_guard, "pool metadata save failed")
            .await?;
        {
            let pool_meta = self.pool_meta.read().await;
            merge_pool_meta_updates_for_save(&mut snapshot, &pool_meta, indices, "pool metadata save failed")?;
        }
        let outcome = snapshot
            .save_no_lock_armed(self.pools.clone(), &mut save_guard, pool_meta_guard.lock_lost_signal(), indices)
            .await?;
        let mut pool_meta = self.pool_meta.write().await;
        ensure_pool_meta_write_fence(&pool_meta_guard, "pool metadata save failed")?;
        publish_pool_meta_updates(&mut pool_meta, &outcome.committed, indices);
        ensure_pool_meta_write_fence(&pool_meta_guard, "pool metadata save failed")?;
        drop(pool_meta);
        outcome.disarm();
        Ok(())
    }

    #[cfg(test)]
    pub(crate) async fn save_current_pool_meta_for_test(&self, indices: &[usize]) -> Result<()> {
        self.save_current_pool_meta(indices).await
    }

    async fn persist_decommission_unresolved_entry(
        &self,
        idx: usize,
        generation: OffsetDateTime,
        entry: DecommissionUnresolvedEntry,
    ) -> Result<()> {
        {
            let mut pool_meta = self.pool_meta.write().await;
            record_decommission_unresolved_entry(&mut pool_meta, idx, generation, entry)?;
        }
        self.save_current_pool_meta(&[idx])
            .await
            .map_err(|err| Error::other(format!("decommission unresolved entry ledger save failed: {err}")))
    }

    async fn save_decommission_progress_checkpoint(&self, idx: usize, generation: OffsetDateTime) -> Result<bool> {
        // Lock order: save gate, then the short pool metadata read/write sections. Peer
        // reloads are intentionally performed by the caller after both locks are released.
        let mut save_guard = self.pool_meta_save_gate.lock().await;
        let (pool_meta_guard, mut snapshot) = self
            .acquire_pool_meta_write_guard(&mut save_guard, "decommission progress save failed")
            .await?;
        let (snapshot, checkpoint) = {
            let pool_meta = self.pool_meta.read().await;
            ensure_decommission_generation(&pool_meta, idx, generation)?;
            let Some(checkpoint) = pool_meta.decommission_progress_checkpoint(
                idx,
                DECOMMISSION_PROGRESS_SAVE_INTERVAL,
                OffsetDateTime::now_utc(),
            )?
            else {
                return Ok(false);
            };

            let mut current = pool_meta.clone();
            let current_count = current.pools.len();
            let Some(pool) = current.pools.get_mut(idx) else {
                return Err(invalid_decommission_pool_index_error(current_count, idx));
            };
            pool.last_update = checkpoint.checkpoint_at;
            merge_pool_meta_updates_for_save(&mut snapshot, &current, &[idx], "decommission progress save failed")?;
            (snapshot, checkpoint)
        };

        let outcome = match snapshot
            .save_no_lock_armed(self.pools.clone(), &mut save_guard, pool_meta_guard.lock_lost_signal(), &[idx])
            .await
        {
            Ok(outcome) => outcome,
            Err(err) => {
                let retry_after = OffsetDateTime::now_utc() + DECOMMISSION_PROGRESS_SAVE_RETRY_BACKOFF;
                let mut pool_meta = self.pool_meta.write().await;
                pool_meta.defer_decommission_progress_checkpoint(idx, checkpoint, retry_after);
                return Err(err);
            }
        };

        let mut pool_meta = self.pool_meta.write().await;
        ensure_pool_meta_write_fence(&pool_meta_guard, "decommission progress save failed")?;
        pool_meta.version = pool_meta.version.max(outcome.committed.version);
        let committed = pool_meta.commit_decommission_progress_checkpoint(idx, checkpoint);
        ensure_pool_meta_write_fence(&pool_meta_guard, "decommission progress save failed")?;
        drop(pool_meta);
        outcome.disarm();
        Ok(committed)
    }

    async fn mark_decommission_bucket_done_and_save(&self, idx: usize, bucket: &DecomBucketInfo) -> Result<bool> {
        let mut save_guard = self.pool_meta_save_gate.lock().await;
        let (pool_meta_guard, mut snapshot) = self
            .acquire_pool_meta_write_guard(&mut save_guard, "decommission bucket completion save failed")
            .await?;
        let changed = {
            let mut pool_meta = self.pool_meta.write().await;
            let changed = mark_decommission_bucket_done(&mut pool_meta, idx, bucket)?;
            if changed {
                merge_pool_meta_updates_for_save(
                    &mut snapshot,
                    &pool_meta,
                    &[idx],
                    "decommission bucket completion save failed",
                )?;
            }
            changed
        };
        if !changed {
            return Ok(false);
        }

        let outcome = snapshot
            .save_no_lock_armed(self.pools.clone(), &mut save_guard, pool_meta_guard.lock_lost_signal(), &[idx])
            .await
            .map_err(|err| {
                Error::other(format!("decommission metadata save failed for pool {idx} bucket {}: {err}", bucket.name))
            })?;
        let mut pool_meta = self.pool_meta.write().await;
        ensure_pool_meta_write_fence(&pool_meta_guard, "decommission bucket completion save failed")?;
        pool_meta.version = pool_meta.version.max(outcome.committed.version);
        pool_meta.mark_decommission_progress_saved();
        ensure_pool_meta_write_fence(&pool_meta_guard, "decommission bucket completion save failed")?;
        drop(pool_meta);
        outcome.disarm();
        Ok(true)
    }

    async fn save_current_pool_meta_for_decommission_start(
        &self,
        indices: &[usize],
        space_infos: Vec<(usize, PoolSpaceInfo)>,
        decom_buckets: Vec<DecomBucketInfo>,
    ) -> Result<PoolMeta> {
        let mut save_guard = self.pool_meta_save_gate.lock().await;
        save_guard.ensure_write_safe("decommission start failed")?;
        let rebalance_pool = self
            .pools
            .first()
            .cloned()
            .ok_or_else(|| Error::other("decommission start rebalance metadata load failed: no storage pools available"))?;
        #[cfg(test)]
        observe_pool_activation_start_attempt(PoolActivationStartKind::Decommission);
        let fleet_proof = acquire_pool_activation_fleet_proof(&self.ctx).await?;
        let activation_fence = acquire_pool_rebalance_activation_locks(rebalance_pool.clone(), fleet_proof).await?;

        let mut rebalance_meta = RebalanceMeta::new();
        match rebalance_meta
            .load_with_opts(
                rebalance_pool.clone(),
                ObjectOptions {
                    no_lock: true,
                    ..Default::default()
                },
            )
            .await
        {
            Ok(()) => ensure_decommission_start_rebalance_meta_allowed(Some(&rebalance_meta))?,
            Err(Error::ConfigNotFound) => {}
            Err(err) => {
                return Err(Error::other(format!(
                    "rebalance metadata load before decommission start save failed: {err}"
                )));
            }
        }

        let current_pool_meta = {
            let pool_meta = self.pool_meta.read().await;
            pool_meta.clone()
        };
        let mut latest_pool_meta = PoolMeta::default();
        let replica_state = latest_pool_meta
            .load_no_lock_from_replicas_observing(self.pools.clone(), &mut save_guard)
            .await?;
        save_guard.observe_replicas(replica_state);
        save_guard.ensure_write_safe("decommission start failed")?;
        if latest_pool_meta.pools.is_empty() {
            latest_pool_meta = current_pool_meta;
        }

        ensure_decommission_start_pool_states(&latest_pool_meta, indices)?;

        let previous_pool_meta = latest_pool_meta.clone();
        let first_idx = indices.first().copied();
        for (idx, pi) in space_infos {
            if Some(idx) == first_idx {
                latest_pool_meta.decommission(idx, pi)?;
            } else {
                latest_pool_meta.queue_decommission(idx, pi)?;
            }
            latest_pool_meta.queue_buckets(idx, decom_buckets.clone());
        }

        activation_fence.ensure_held()?;
        let outcome = latest_pool_meta
            .save_no_lock_with_activation_fence(self.pools.clone(), &mut save_guard, &activation_fence, indices)
            .await?;
        activation_fence.ensure_held()?;
        {
            let mut pool_meta = self.pool_meta.write().await;
            publish_pool_meta_updates(&mut pool_meta, &outcome.committed, indices);
        }
        activation_fence.ensure_held()?;
        outcome.disarm();

        Ok(previous_pool_meta)
    }

    async fn rollback_decommission_start_after_reload_failure(
        &self,
        movement_gate: &Arc<tokio::sync::RwLock<()>>,
        previous_pool_meta: &PoolMeta,
        indices: &[usize],
    ) -> Result<()> {
        let _movement_guard = movement_gate.write().await;
        let mut save_guard = self.pool_meta_save_gate.lock().await;
        let (pool_meta_guard, mut snapshot) = self
            .acquire_pool_meta_write_guard(&mut save_guard, "decommission start rollback failed")
            .await?;
        rollback_start_decommission_pool_meta(&mut snapshot, previous_pool_meta, indices);
        let outcome = snapshot
            .save_no_lock_armed(self.pools.clone(), &mut save_guard, pool_meta_guard.lock_lost_signal(), indices)
            .await?;
        let mut pool_meta = self.pool_meta.write().await;
        ensure_pool_meta_write_fence(&pool_meta_guard, "decommission start rollback failed")?;
        publish_pool_meta_updates(&mut pool_meta, &outcome.committed, indices);
        ensure_pool_meta_write_fence(&pool_meta_guard, "decommission start rollback failed")?;
        drop(pool_meta);
        outcome.disarm();
        self.ctx.advance_data_movement_operation_epoch();
        Ok(())
    }

    async fn ensure_decommission_rebalance_idle_after_refresh(&self) -> Result<()> {
        self.load_rebalance_meta().await?;
        ensure_decommission_not_rebalancing(self.is_rebalance_conflicting_with_decommission().await)
    }

    async fn ensure_decommission_rebalance_idle_after_refresh_under_start_gate(&self) -> Result<()> {
        self.load_rebalance_meta_under_start_gate().await?;
        ensure_decommission_not_rebalancing(self.is_rebalance_conflicting_with_decommission().await)
    }

    pub async fn status(&self, idx: usize) -> Result<PoolStatus> {
        let space_info = self.get_decommission_pool_space_info(idx).await?;

        let pool_meta = self.pool_meta.read().await;

        let pool_info = get_by_index(pool_meta.pools.as_slice(), idx, "fetch decommission status")?.clone();
        Ok(apply_decommission_status_space_info(pool_info, space_info))
    }

    #[tracing::instrument(skip_all)]
    pub async fn refresh_pool_status_meta(&self) -> Result<()> {
        let movement_gate = self.ctx.data_movement_operation_gate();
        let _movement_guard = movement_gate.write().await;
        let persisted = self.load_runtime_pool_meta("refresh pool status metadata failed").await?;

        let active_workers = {
            let cancelers = self.decommission_cancelers.read().await;
            cancelers
                .iter()
                .map(|canceler| canceler.as_ref().is_some_and(DecommissionCanceler::is_active))
                .collect::<Vec<_>>()
        };

        let mut pool_meta = self.pool_meta.write().await;
        if merge_pool_status_refresh(&mut pool_meta, persisted, &active_workers) {
            self.ctx.advance_data_movement_operation_epoch();
        }
        Ok(())
    }

    async fn get_decommission_pool_space_info(&self, idx: usize) -> Result<PoolSpaceInfo> {
        if let Some(sets) = self.pools.get(idx) {
            let mut info = sets.storage_info_snapshot().await;
            info.backend = StorageAdminApi::backend_info(self).await;

            let total = get_total_usable_capacity(&info.disks, &info);
            let free = get_total_usable_capacity_free(&info.disks, &info);

            Ok(PoolSpaceInfo {
                free,
                total,
                used: total - free,
            })
        } else {
            Err(invalid_decommission_pool_index_error(self.pools.len(), idx))
        }
    }

    async fn get_decommission_all_pool_space_infos(&self) -> Result<Vec<(usize, PoolSpaceInfo)>> {
        #[cfg(test)]
        if let Some(space_infos) = take_decommission_space_info_override_for_test(self.id) {
            return Ok(space_infos);
        }

        let mut space_infos = Vec::with_capacity(self.pools.len());
        for idx in 0..self.pools.len() {
            space_infos.push((idx, self.get_decommission_pool_space_info(idx).await?));
        }
        Ok(space_infos)
    }

    #[tracing::instrument(skip(self))]
    pub async fn decommission_cancel(self: &Arc<Self>, idx: usize) -> Result<()> {
        self.decommission_cancel_with_owner(idx, None).await
    }

    async fn decommission_cancel_for_operation(self: &Arc<Self>, idx: usize, owner: &DecommissionCanceler) -> Result<()> {
        self.decommission_cancel_with_owner(idx, Some(owner)).await
    }

    #[cfg(test)]
    async fn decommission_cancel_with_owner_and_save<Save, SaveFuture>(
        self: &Arc<Self>,
        idx: usize,
        owner: Option<&DecommissionCanceler>,
        save_pool_meta: Save,
    ) -> Result<()>
    where
        Save: FnOnce(PoolMeta, Option<Arc<rustfs_lock::distributed_lock::LockLostSignal>>) -> SaveFuture + Send + 'static,
        SaveFuture: Future<Output = Result<()>> + Send + 'static,
    {
        let store = self.clone();
        let owner = owner.cloned();
        // Dropping the RPC waiter detaches this task; the transaction retains
        // the store and exact owner until persistence is resolved.
        tokio::spawn(async move { store.decommission_cancel_transaction(idx, owner, false, save_pool_meta).await })
            .await
            .map_err(|err| Error::other(format!("decommission cancel transaction task join error: {err}")))?
    }

    async fn decommission_cancel_transaction<Save, SaveFuture>(
        &self,
        idx: usize,
        owner: Option<DecommissionCanceler>,
        acquire_runtime_fence: bool,
        save_pool_meta: Save,
    ) -> Result<()>
    where
        Save: FnOnce(PoolMeta, Option<Arc<rustfs_lock::distributed_lock::LockLostSignal>>) -> SaveFuture,
        SaveFuture: Future<Output = Result<()>>,
    {
        let owner = owner.as_ref();
        ensure_decommission_terminal_operation_supported(self.single_pool(), "cancel decommission")?;
        let _start_guard = self.start_gate.lock().await;
        let mut save_guard = self.pool_meta_save_gate.lock().await;
        let (_pool_meta_guard, mut persisted_pool_meta) = if acquire_runtime_fence {
            let (guard, pool_meta) = self
                .acquire_pool_meta_write_guard(&mut save_guard, "decommission cancel failed")
                .await?;
            (Some(guard), Some(pool_meta))
        } else {
            save_guard.ensure_write_safe("decommission cancel failed")?;
            (None, None)
        };
        let pool_meta_fence = _pool_meta_guard
            .as_ref()
            .and_then(rustfs_lock::NamespaceLockGuard::lock_lost_signal);

        // Lock order: start gate, save gate, distributed pool metadata fence,
        // decommission_cancelers, then pool_meta. The state guards stay held
        // across persistence so the active generation cannot change before
        // the cancel is published.
        let mut cancelers = self.decommission_cancelers.write().await;
        let mut pool_meta = self.pool_meta.write().await;
        let (pending, should_reload_pool_meta, already_canceled, terminal_canceler) = {
            let mut already_canceled = false;
            let (pool_present, decommission_present, terminal) = if let Some(pool) = pool_meta.pools.get(idx) {
                if let Some(info) = pool.decommission.as_ref() {
                    already_canceled = info.canceled;
                    (
                        true,
                        info.has_decommission_state(),
                        should_reject_decommission_cancel_as_terminal(info.complete, info.failed),
                    )
                } else {
                    (true, false, false)
                }
            } else {
                (false, false, false)
            };

            ensure_decommission_cancel_allowed(pool_present, decommission_present, terminal)?;
            let previous_pool = pool_meta
                .pools
                .get(idx)
                .cloned()
                .ok_or_else(|| invalid_decommission_pool_index_error(pool_meta.pools.len(), idx))?;
            let previous_decommission = previous_pool
                .decommission
                .as_ref()
                .ok_or_else(|| decommission_metadata_not_initialized_error("cancel decommission"))?;
            let mut snapshot = pool_meta.clone();
            let Some(changed) = update_decommission_for_operation(cancelers.as_slice(), &mut snapshot, idx, owner, |pool_meta| {
                pool_meta.decommission_cancel(idx)
            }) else {
                return Ok(());
            };
            let pending = if changed {
                let canceled_pool = snapshot
                    .pools
                    .get(idx)
                    .cloned()
                    .ok_or_else(|| invalid_decommission_pool_index_error(pool_meta.pools.len(), idx))?;
                if let Some(persisted) = persisted_pool_meta.as_mut() {
                    merge_pool_meta_updates_for_save(persisted, &snapshot, &[idx], "decommission cancel failed")?;
                    snapshot = persisted.clone();
                }
                Some((
                    snapshot,
                    DecommissionCancelCommit {
                        previous_start_time: previous_decommission.start_time,
                        previous_queued: previous_decommission.queued,
                        previous_last_update: previous_pool.last_update,
                        canceled_pool,
                    },
                ))
            } else {
                None
            };
            let terminal_canceler = if let Some(owner) = owner {
                Some(owner.clone())
            } else {
                cancelers.get(idx).and_then(Option::as_ref).cloned()
            };
            (
                pending,
                should_retry_decommission_cancel_reload(changed, already_canceled),
                already_canceled,
                terminal_canceler,
            )
        };
        let active_worker = terminal_canceler.as_ref().is_some_and(DecommissionCanceler::is_active);
        if !active_worker && !already_canceled {
            warn!(
                event = EVENT_DECOMMISSION_STATE,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_POOLS,
                pool_index = idx,
                state = "cancel_skipped",
                reason = "no_active_canceler",
                "Decommission cancel skipped"
            );
        }

        let changed = pending.is_some();
        let commit_result = if let Some((snapshot, commit)) = pending {
            if let Err(err) = save_pool_meta(snapshot, pool_meta_fence).await {
                save_guard.block_writes();
                return Err(err);
            }
            if let Some(pool_meta_guard) = _pool_meta_guard.as_ref()
                && let Err(err) = ensure_pool_meta_write_fence(pool_meta_guard, "decommission cancel failed")
            {
                save_guard.block_writes();
                return Err(err);
            }
            commit_decommission_cancel(&mut pool_meta, idx, commit)
        } else {
            Ok(())
        };

        commit_result?;
        if let Some(pool_meta_guard) = _pool_meta_guard.as_ref()
            && let Err(err) = ensure_pool_meta_write_fence(pool_meta_guard, "decommission cancel failed")
        {
            save_guard.block_writes();
            return Err(err);
        }
        if let Some(canceler) = terminal_canceler.as_ref() {
            take_and_cancel_decommission_canceler_for_operation(cancelers.as_mut_slice(), idx, canceler);
        }
        drop(pool_meta);
        drop(cancelers);
        drop(_pool_meta_guard);
        drop(save_guard);

        if changed {
            // Persistence must commit before the cancellation signal. Wait for
            // in-flight movement only after signaling so readers can release
            // the shared gate without deadlocking the terminal transition.
            let movement_gate = self.ctx.data_movement_operation_gate();
            let _movement_guard = movement_gate.write().await;
            self.ctx.advance_data_movement_operation_epoch();
        }

        if should_reload_pool_meta && let Some(notification_sys) = runtime_sources::notification_sys() {
            let stage = format!("decommission_cancel for pool {idx}");
            if let Err(err) =
                resolve_decommission_pool_meta_reload_result(notification_sys.reload_pool_meta().await, stage.as_str())
            {
                warn!(
                    event = EVENT_DECOMMISSION_STATE,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_POOLS,
                    pool_index = idx,
                    state = "terminal_reload_failed",
                    error = %err,
                    "Decommission cancel saved locally but pool meta reload failed"
                );
            }
        }

        Ok(())
    }

    async fn release_decommission_canceler_slot(&self, idx: usize, owner: &DecommissionCanceler) {
        let mut cancelers = self.decommission_cancelers.write().await;
        take_and_cancel_decommission_canceler_for_operation(cancelers.as_mut_slice(), idx, owner);
    }

    async fn decommission_terminal_retryable_for_operation(&self, idx: usize, owner: &DecommissionCanceler) -> bool {
        let _start_guard = self.start_gate.lock().await;
        let mut cancelers = self.decommission_cancelers.write().await;
        if !decommission_canceler_is_owned_by(cancelers.as_slice(), idx, owner) {
            owner.release();
            return false;
        }

        let retryable = {
            let pool_meta = self.pool_meta.read().await;
            pool_meta
                .pools
                .get(idx)
                .and_then(|pool| pool.decommission.as_ref())
                .is_some_and(|info| info.has_decommission_state() && !info.complete && !info.failed && !info.canceled)
        };
        if !retryable {
            take_and_cancel_decommission_canceler_for_operation(cancelers.as_mut_slice(), idx, owner);
        }
        retryable
    }

    async fn retry_decommission_cancel_for_operation(self: &Arc<Self>, idx: usize, owner: &DecommissionCanceler) {
        if !self.decommission_terminal_retryable_for_operation(idx, owner).await {
            return;
        }

        let mut attempt = 0usize;
        loop {
            if self
                .ensure_pool_meta_side_effects_safe("decommission cancel retry paused because pool metadata requires recovery")
                .await
                .is_err()
            {
                self.release_decommission_canceler_slot(idx, owner).await;
                return;
            }
            let Err(err) = self.decommission_cancel_for_operation(idx, owner).await else {
                return;
            };
            if !self.decommission_terminal_retryable_for_operation(idx, owner).await {
                return;
            }
            attempt += 1;
            warn!(
                event = EVENT_DECOMMISSION_STATE,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_POOLS,
                pool_index = idx,
                state = "terminal_save_retry",
                terminal = "canceled",
                attempt,
                error = %err,
                "Decommission terminal save will be retried"
            );
            tokio::time::sleep(DECOMMISSION_TERMINAL_RETRY_DELAY).await;
        }
    }

    async fn retry_decommission_failed_for_operation(&self, idx: usize, owner: &DecommissionCanceler) {
        let mut attempt = 0usize;
        loop {
            if self
                .ensure_pool_meta_side_effects_safe("decommission failure retry paused because pool metadata requires recovery")
                .await
                .is_err()
            {
                self.release_decommission_canceler_slot(idx, owner).await;
                return;
            }
            let Err(err) = self.decommission_failed_for_operation(idx, owner).await else {
                return;
            };
            if !self.decommission_terminal_retryable_for_operation(idx, owner).await {
                return;
            }
            attempt += 1;
            warn!(
                event = EVENT_DECOMMISSION_STATE,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_POOLS,
                pool_index = idx,
                state = "terminal_save_retry",
                terminal = "failed",
                attempt,
                error = %err,
                "Decommission terminal save will be retried"
            );
            tokio::time::sleep(DECOMMISSION_TERMINAL_RETRY_DELAY).await;
        }
    }

    async fn decommission_cancel_with_owner(self: &Arc<Self>, idx: usize, owner: Option<&DecommissionCanceler>) -> Result<()> {
        let pools = self.pools.clone();
        let store = self.clone();
        let owner = owner.cloned();
        tokio::spawn(async move {
            store
                .decommission_cancel_transaction(idx, owner, true, move |snapshot, pool_meta_fence| async move {
                    snapshot.save_no_lock_with_fence(pools, pool_meta_fence, &[idx]).await
                })
                .await
        })
        .await
        .map_err(|err| Error::other(format!("decommission cancel transaction task join error: {err}")))?
    }

    #[cfg(test)]
    async fn clear_decommission_with_save<Save, SaveFuture>(self: &Arc<Self>, idx: usize, save_pool_meta: Save) -> Result<()>
    where
        Save: FnOnce() -> SaveFuture + Send + 'static,
        SaveFuture: Future<Output = Result<()>> + Send + 'static,
    {
        let store = self.clone();
        tokio::spawn(async move { store.clear_decommission_transaction(idx, save_pool_meta).await })
            .await
            .map_err(|err| Error::other(format!("clear decommission transaction task join error: {err}")))?
    }

    #[tracing::instrument(skip(self))]
    pub async fn clear_decommission(self: &Arc<Self>, idx: usize) -> Result<()> {
        let store = self.clone();
        let save_store = store.clone();
        // Dropping the RPC waiter detaches this task; once in-memory state can
        // change, the transaction must persist or roll it back before ending.
        tokio::spawn(async move {
            store
                .clear_decommission_transaction(idx, move || async move { save_store.save_current_pool_meta(&[idx]).await })
                .await
        })
        .await
        .map_err(|err| Error::other(format!("clear decommission transaction task join error: {err}")))?
    }

    async fn clear_decommission_transaction<Save, SaveFuture>(&self, idx: usize, save_pool_meta: Save) -> Result<()>
    where
        Save: FnOnce() -> SaveFuture,
        SaveFuture: Future<Output = Result<()>>,
    {
        ensure_decommission_terminal_operation_supported(self.single_pool(), "clear decommission")?;
        let _start_guard = self.start_gate.lock().await;

        {
            let pool_meta = self.pool_meta.read().await;
            let pool_count = pool_meta.pools.len();
            ensure_valid_decommission_pool_index(pool_count, idx)?;
            let Some(pool) = pool_meta.pools.get(idx) else {
                return Err(invalid_decommission_pool_index_error(pool_count, idx));
            };
            let (decommission_present, complete, failed, canceled, unresolved_entries) = pool
                .decommission
                .as_ref()
                .map(|info| {
                    (
                        info.has_decommission_state(),
                        info.complete,
                        info.failed,
                        info.canceled,
                        info.unresolved_entries.len(),
                    )
                })
                .unwrap_or((false, false, false, false, 0));
            ensure_decommission_clear_allowed(true, decommission_present, complete, failed, canceled, unresolved_entries)?;
        }
        // Cancel workers before waiting for the movement writer so active
        // object operations can observe the signal and release read guards.
        self.cancel_decommission_routines(&[idx]).await;
        let movement_gate = self.ctx.data_movement_operation_gate();
        let _movement_guard = movement_gate.write().await;

        let (should_reload_pool_meta, previous_pool_meta) = {
            let mut pool_meta = self.pool_meta.write().await;
            let previous_pool_meta = pool_meta.clone();
            let changed = pool_meta.clear_decommission(idx)?;
            (changed, changed.then_some(previous_pool_meta))
        };

        if should_reload_pool_meta && let Err(err) = save_pool_meta().await {
            if let Some(previous_pool_meta) = previous_pool_meta {
                let mut pool_meta = self.pool_meta.write().await;
                rollback_decommission_pool_meta(&mut pool_meta, &previous_pool_meta, &[idx]);
            }
            return Err(err);
        }

        if should_reload_pool_meta {
            self.ctx.advance_data_movement_operation_epoch();
        }
        drop(_movement_guard);

        if should_reload_pool_meta && let Some(notification_sys) = runtime_sources::notification_sys() {
            let stage = format!("clear_decommission for pool {idx}");
            if let Err(err) =
                resolve_decommission_pool_meta_reload_result(notification_sys.reload_pool_meta().await, stage.as_str())
            {
                warn!(
                    event = EVENT_DECOMMISSION_STATE,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_POOLS,
                    pool_index = idx,
                    state = "terminal_reload_failed",
                    error = %err,
                    "Decommission clear saved locally but pool meta reload failed"
                );
            }
        }

        Ok(())
    }

    async fn promote_queued_decommission(&self, idx: usize, owner: &DecommissionCanceler) -> Result<OffsetDateTime> {
        // Serialize promotion and generation capture with clear/restart transitions.
        let (changed, generation, save_error) = {
            let _start_guard = self.start_gate.lock().await;
            let movement_gate = self.ctx.data_movement_operation_gate();
            let _movement_guard = movement_gate.write().await;
            let mut save_guard = self.pool_meta_save_gate.lock().await;
            let (pool_meta_guard, mut snapshot) = self
                .acquire_pool_meta_write_guard(&mut save_guard, "decommission promotion failed")
                .await?;
            let mut pool_meta = self.pool_meta.write().await;
            if pool_meta.pools.get(idx).is_none() {
                return Err(Error::other("failed to start decommission: target pool was not found"));
            }
            let reconciled = reconcile_decommission_meta_buckets(&mut pool_meta, idx);
            let promoted = pool_meta.promote_queued_decommission(idx);
            let changed = reconciled || promoted;
            if changed {
                merge_pool_meta_updates_for_save(&mut snapshot, &pool_meta, &[idx], "decommission promotion failed")?;
            }
            drop(pool_meta);

            let (save_outcome, save_error) = if changed {
                match snapshot
                    .save_no_lock_armed(self.pools.clone(), &mut save_guard, pool_meta_guard.lock_lost_signal(), &[idx])
                    .await
                {
                    Ok(outcome) => (Some(outcome), None),
                    Err(err) => (None, Some(err)),
                }
            } else {
                (None, None)
            };
            let generation = self.active_decommission_generation(idx).await?;
            ensure_pool_meta_write_fence(&pool_meta_guard, "decommission promotion failed")?;
            if let Some(outcome) = save_outcome {
                let mut pool_meta = self.pool_meta.write().await;
                pool_meta.version = pool_meta.version.max(outcome.committed.version);
                drop(pool_meta);
                outcome.disarm();
            }
            (changed, generation, save_error)
        };

        if let Some(err) = save_error {
            resolve_decommission_terminal_mark_after_error_result(
                self.decommission_failed_for_operation(idx, owner).await,
                idx,
                &err,
            )?;
            return Err(err);
        }

        if changed {
            self.ctx.advance_data_movement_operation_epoch();
        }
        if changed && let Some(notification_sys) = runtime_sources::notification_sys() {
            let stage = format!("promote_queued_decommission for pool {idx}");
            if let Err(err) =
                resolve_decommission_pool_meta_reload_result(notification_sys.reload_pool_meta().await, stage.as_str())
            {
                resolve_decommission_terminal_mark_after_error_result(
                    self.decommission_failed_for_operation(idx, owner).await,
                    idx,
                    &err,
                )?;
                return Err(err);
            }
        }

        Ok(generation)
    }

    #[cfg(test)]
    pub(crate) async fn promote_queued_decommission_for_test(&self, idx: usize) -> Result<()> {
        let owner = DecommissionCanceler::new(CancellationToken::new());
        self.promote_queued_decommission(idx, &owner).await.map(|_| ())
    }

    async fn record_decommission_terminal_reload_failure(&self, idx: usize, stage: &str, err: Error) -> Result<()> {
        let movement_gate = self.ctx.data_movement_operation_gate();
        let _movement_guard = movement_gate.write().await;
        let changed = {
            let mut pool_meta = self.pool_meta.write().await;
            pool_meta.record_decommission_terminal_reload_failure(idx, stage, err.to_string())?
        };

        if changed {
            self.save_current_pool_meta(&[idx]).await?;
        }

        Ok(())
    }

    pub async fn is_decommission_running(&self) -> bool {
        {
            let cancelers = self.decommission_cancelers.read().await;
            if has_active_decommission_canceler(cancelers.as_slice()) {
                return true;
            }
        }

        let pool_meta = self.pool_meta.read().await;
        for pool in pool_meta.pools.iter() {
            if let Some(ref info) = pool.decommission
                && info.has_decommission_state()
                && !info.complete
                && !info.failed
                && !info.canceled
            {
                return true;
            }
        }

        false
    }

    async fn decommission_cancel_requested(&self, idx: usize, rx: &CancellationToken) -> bool {
        let pool_meta = self.pool_meta.read().await;
        is_decommission_cancel_requested(rx.is_cancelled(), pool_meta.pools.get(idx))
    }

    #[cfg(test)]
    async fn cancel_decommission_routines_and_wait(&self, indices: &[usize]) {
        self.cancel_decommission_routines(indices).await;
        let movement_gate = self.ctx.data_movement_operation_gate();
        let _movement_guard = movement_gate.write().await;
    }

    async fn cancel_decommission_routines(&self, indices: &[usize]) {
        {
            let mut cancelers = self.decommission_cancelers.write().await;
            for idx in indices {
                take_and_cancel_decommission_canceler(cancelers.as_mut_slice(), *idx);
            }
        }
    }

    async fn quiesce_decommission_worker_after_join_error(&self, canceler: &DecommissionCanceler) {
        canceler.cancel();
        let movement_gate = self.ctx.data_movement_operation_gate();
        let _movement_guard = movement_gate.write().await;
    }

    async fn reserve_decommission_routines(
        &self,
        rx: &CancellationToken,
        indices: &[usize],
    ) -> Result<Vec<(usize, DecommissionCancelerGuard)>> {
        let indices = dedup_indices(indices);
        if indices.is_empty() {
            return Ok(Vec::new());
        }

        let _start_guard = self.start_gate.lock().await;
        let save_guard = self.pool_meta_save_gate.lock().await;
        save_guard.ensure_write_safe("decommission cannot be scheduled while pool metadata requires recovery")?;
        let indices = {
            let pool_meta = self.pool_meta.read().await;
            resumable_decommission_queue_indices(&pool_meta)
                .into_iter()
                .filter(|idx| indices.contains(idx))
                .collect::<Vec<_>>()
        };
        if indices.is_empty() {
            return Ok(Vec::new());
        }

        let index_cancelers = {
            let mut cancelers = self.decommission_cancelers.write().await;
            let missing = missing_decommission_worker_prefix(indices.as_slice(), cancelers.as_slice());
            if missing.is_empty() {
                return Ok(Vec::new());
            }
            let bound = bind_missing_decommission_cancelers(missing.as_slice(), rx, cancelers.as_mut_slice());
            let guards = guard_decommission_cancelers(bound);
            ensure_decommission_routines_scheduled(guards.len(), missing.len())?;
            guards
        };
        Ok(index_cancelers)
    }

    async fn reserve_missing_local_decommission_routines(
        &self,
        rx: &CancellationToken,
        endpoints: &EndpointServerPools,
    ) -> Result<Vec<(usize, DecommissionCancelerGuard)>> {
        let indices = {
            let pool_meta = self.pool_meta.read().await;
            resumable_decommission_queue_indices(&pool_meta)
        };
        let indices = local_decommission_queue_prefix(endpoints, &indices)?;
        self.reserve_decommission_routines(rx, indices.as_slice()).await
    }

    pub(crate) async fn spawn_decommission_routines(
        &self,
        store: Arc<ECStore>,
        rx: CancellationToken,
        indices: Vec<usize>,
    ) -> Result<()> {
        let index_cancelers = self.reserve_decommission_routines(&rx, indices.as_slice()).await?;
        if !index_cancelers.is_empty() {
            drop(spawn_decommission_index_cancelers(
                store,
                rx,
                index_cancelers,
                Arc::new(Semaphore::new(decommission_entry_concurrency_limit())),
            ));
        }

        Ok(())
    }

    pub async fn spawn_missing_local_decommission_routines(self: &Arc<Self>) -> Result<()> {
        let rx = CancellationToken::new();
        let endpoints = self.endpoints();
        let index_cancelers = self.reserve_missing_local_decommission_routines(&rx, &endpoints).await?;
        if index_cancelers.is_empty() {
            return Ok(());
        }

        drop(spawn_decommission_index_cancelers(
            self.clone(),
            rx,
            index_cancelers,
            Arc::new(Semaphore::new(decommission_entry_concurrency_limit())),
        ));
        Ok(())
    }

    #[tracing::instrument(skip(self, rx))]
    pub async fn decommission(&self, rx: CancellationToken, indices: Vec<usize>) -> Result<()> {
        let indices = dedup_indices(&indices);

        info!(
            event = EVENT_DECOMMISSION_STATE,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_POOLS,
            pool_indices = ?indices,
            state = "requested",
            "Decommission requested"
        );
        validate_start_decommission_request(&indices, self.single_pool())?;

        self.ensure_decommission_rebalance_idle_after_refresh().await?;

        let store = require_decommission_store(runtime_sources::object_store_handle(), "start decommission")?;
        let local_indices = local_decommission_queue_prefix(&self.endpoints(), &indices)?;
        let index_cancelers = self
            .start_decommission_with_routines(indices, &rx, local_indices.as_slice())
            .await?;
        drop(spawn_decommission_index_cancelers(
            store,
            rx,
            index_cancelers,
            Arc::new(Semaphore::new(decommission_entry_concurrency_limit())),
        ));

        Ok(())
    }

    async fn active_decommission_generation(&self, idx: usize) -> Result<OffsetDateTime> {
        let pool_meta = self.pool_meta.read().await;
        let Some(pool) = pool_meta.pools.get(idx) else {
            return Err(invalid_decommission_pool_index_error(pool_meta.pools.len(), idx));
        };
        let Some(info) = pool.decommission.as_ref() else {
            return Err(decommission_metadata_not_initialized_error("load decommission generation"));
        };
        let Some(generation) = info.start_time else {
            return Err(Error::OperationCanceled);
        };
        ensure_decommission_generation(&pool_meta, idx, generation)?;
        Ok(generation)
    }

    async fn ensure_decommission_generation_current(&self, idx: usize, generation: OffsetDateTime) -> Result<()> {
        let pool_meta = self.pool_meta.read().await;
        ensure_decommission_generation(&pool_meta, idx, generation)
    }

    #[allow(clippy::too_many_arguments)]
    async fn decommission_entry_worker(
        self: Arc<Self>,
        rx: CancellationToken,
        idx: usize,
        set_idx: usize,
        generation: OffsetDateTime,
        bucket: String,
        set: Arc<SetDisks>,
        lifecycle_config: Option<BucketLifecycleConfiguration>,
        object_lock_config: Option<ObjectLockConfiguration>,
        replication_config: Option<(ReplicationConfiguration, OffsetDateTime)>,
        expected_bucket_incarnation_id: Option<uuid::Uuid>,
        source_changed_exhaustions: Arc<AtomicUsize>,
        entry_budget: Arc<Semaphore>,
        queue: Arc<tokio::sync::Mutex<mpsc::Receiver<QueuedDecommissionEntry>>>,
        entry_error: Arc<tokio::sync::Mutex<Option<Error>>>,
    ) {
        loop {
            let queued = tokio::select! {
                biased;
                _ = rx.cancelled() => return,
                item = async {
                    let mut queue = queue.lock().await;
                    queue.recv().await
                } => item,
            };
            let Some(QueuedDecommissionEntry { entry, queue_permit }) = queued else {
                return;
            };
            let object_name = entry.name.clone();

            if entry_error.lock().await.is_some() {
                drop(queue_permit);
                continue;
            }

            if let Err(err) = self.ensure_decommission_generation_current(idx, generation).await {
                if matches!(err, Error::OperationCanceled) {
                    rx.cancel();
                } else {
                    record_decommission_entry_error(&entry_error, &rx, err).await;
                }
                return;
            }

            if let Err(err) = backpressure::wait_for_data_movement_admission(DataMovementOperation::Decommission, idx, &rx).await
            {
                if matches!(err, Error::OperationCanceled) {
                    return;
                }
                error!(
                    event = EVENT_DECOMMISSION_ENTRY,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_POOLS,
                    pool_index = idx,
                    set_index = set_idx,
                    bucket = %bucket,
                    object = %object_name,
                    state = "entry_admission_failed",
                    error = %err,
                    "Decommission entry admission failed"
                );
                record_decommission_entry_error(&entry_error, &rx, err).await;
                return;
            }

            let entry_budget_permit = match tokio::select! {
                biased;
                _ = rx.cancelled() => return,
                permit = entry_budget.clone().acquire_owned() => permit,
            } {
                Ok(permit) => permit,
                Err(err) => {
                    let err = Error::other(format!("decommission entry budget permit acquire failed: {err}"));
                    error!(
                        event = EVENT_DECOMMISSION_ENTRY,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_POOLS,
                        pool_index = idx,
                        set_index = set_idx,
                        bucket = %bucket,
                        object = %object_name,
                        state = "entry_budget_acquire_failed",
                        error = %err,
                        "Decommission entry budget permit acquire failed"
                    );
                    record_decommission_entry_error(&entry_error, &rx, err).await;
                    return;
                }
            };

            let result = self
                .decommission_entry(
                    rx.clone(),
                    idx,
                    generation,
                    entry,
                    bucket.clone(),
                    set.clone(),
                    lifecycle_config.clone(),
                    object_lock_config.clone(),
                    replication_config.clone(),
                    expected_bucket_incarnation_id,
                    Arc::clone(&source_changed_exhaustions),
                )
                .await;
            drop(entry_budget_permit);
            drop(queue_permit);

            if let Err(err) = result {
                error!(
                    event = EVENT_DECOMMISSION_ENTRY,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_POOLS,
                    pool_index = idx,
                    set_index = set_idx,
                    bucket = %bucket,
                    object = %object_name,
                    state = "entry_failed",
                    error = %err,
                    "Decommission entry failed"
                );
                record_decommission_entry_error(&entry_error, &rx, err).await;
                return;
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn decommission_set(
        self: Arc<Self>,
        rx: CancellationToken,
        idx: usize,
        set_idx: usize,
        generation: OffsetDateTime,
        set: Arc<SetDisks>,
        bi: DecomBucketInfo,
        lifecycle_config: Option<BucketLifecycleConfiguration>,
        object_lock_config: Option<ObjectLockConfiguration>,
        replication_config: Option<(ReplicationConfiguration, OffsetDateTime)>,
        expected_bucket_incarnation_id: Option<uuid::Uuid>,
        source_changed_exhaustions: Arc<AtomicUsize>,
        entry_budget: Arc<Semaphore>,
        entry_error: Arc<tokio::sync::Mutex<Option<Error>>>,
    ) -> Result<()> {
        let worker_count = DECOMMISSION_ENTRY_WORKERS_PER_SET;
        let queue_capacity = decommission_entry_queue_capacity(worker_count);
        let outstanding_capacity = queue_capacity.saturating_add(worker_count);
        let outstanding = Arc::new(Semaphore::new(outstanding_capacity));
        let (tx, rx_queue) = mpsc::channel(queue_capacity);
        let queue = Arc::new(tokio::sync::Mutex::new(rx_queue));

        let mut entry_workers = tokio::task::JoinSet::new();
        for _ in 0..worker_count {
            let this = self.clone();
            let rx = rx.clone();
            let bucket = bi.name.clone();
            let set = set.clone();
            let lifecycle_config = lifecycle_config.clone();
            let object_lock_config = object_lock_config.clone();
            let replication_config = replication_config.clone();
            let source_changed_exhaustions = Arc::clone(&source_changed_exhaustions);
            let queue = queue.clone();
            let entry_budget = entry_budget.clone();
            let entry_error = entry_error.clone();
            entry_workers.spawn(async move {
                this.decommission_entry_worker(
                    rx,
                    idx,
                    set_idx,
                    generation,
                    bucket,
                    set,
                    lifecycle_config,
                    object_lock_config,
                    replication_config,
                    expected_bucket_incarnation_id,
                    source_changed_exhaustions,
                    entry_budget,
                    queue,
                    entry_error,
                )
                .await;
            });
        }

        let callback: ListCallback = Arc::new({
            let tx = tx.clone();
            let outstanding = outstanding.clone();
            let callback_rx = rx.clone();
            let entry_error = entry_error.clone();
            let bucket = bi.name.clone();
            move |entry: MetaCacheEntry| {
                let tx = tx.clone();
                let outstanding = outstanding.clone();
                let callback_rx = callback_rx.clone();
                let entry_error = entry_error.clone();
                let bucket = bucket.clone();
                Box::pin(async move {
                    if callback_rx.is_cancelled() || entry_error.lock().await.is_some() {
                        return;
                    }

                    if matches!(
                        enqueue_decommission_entry(&callback_rx, &outstanding, &tx, entry).await,
                        DecommissionEntryEnqueueResult::Closed
                    ) {
                        let err = Error::other("decommission entry queue closed");
                        error!(
                            event = EVENT_DECOMMISSION_ENTRY,
                            component = LOG_COMPONENT_ECSTORE,
                            subsystem = LOG_SUBSYSTEM_POOLS,
                            pool_index = idx,
                            set_index = set_idx,
                            bucket = %bucket,
                            state = "entry_queue_closed",
                            error = %err,
                            "Decommission entry queue closed"
                        );
                        record_decommission_entry_error(&entry_error, &callback_rx, err).await;
                    }
                })
            }
        });

        let list_set = set.clone();
        let list_rx = rx.clone();
        let list_rx_for_list = list_rx.clone();
        let list_rx_for_drain = list_rx.clone();
        let list_bi = bi.clone();
        let list_outstanding = outstanding.clone();
        let list_entry_error = entry_error.clone();
        let list_store = self.clone();
        let mut listing = tokio::spawn(async move {
            run_decommission_listing_with_retry_and_drain(
                list_rx.clone(),
                list_bi.name.clone(),
                callback,
                idx,
                set_idx,
                DECOMMISSION_LISTING_MAX_ATTEMPTS,
                move |callback| {
                    let set = list_set.clone();
                    let rx = list_rx_for_list.clone();
                    let bucket = list_bi.clone();
                    let entry_error = list_entry_error.clone();
                    let store = list_store.clone();
                    async move {
                        set.list_objects_to_decommission(
                            store,
                            rx,
                            bucket,
                            callback,
                            entry_error,
                            idx,
                            set_idx,
                            generation,
                            false,
                        )
                        .await
                    }
                },
                move || {
                    let rx = list_rx_for_drain.clone();
                    let outstanding = list_outstanding.clone();
                    async move { drain_decommission_entry_queue(&rx, &outstanding, outstanding_capacity).await }
                },
            )
            .await
        });

        let mut listing_result = None;
        let mut workers_left = worker_count;
        let mut sender = Some(tx);
        while listing_result.is_none() || workers_left > 0 {
            tokio::select! {
                biased;
                result = &mut listing, if listing_result.is_none() => {
                    let result = resolve_decommission_listing_worker_result(set_idx, result);
                    if result.is_err() {
                        rx.cancel();
                    }
                    listing_result = Some(result);
                    drop(sender.take());
                }
                worker_result = entry_workers.join_next(), if workers_left > 0 => {
                    workers_left -= 1;
                    if let Some(Err(err)) = worker_result {
                        let err = Error::other(format!("decommission entry worker {set_idx} task join error: {err}"));
                        error!(
                            event = EVENT_DECOMMISSION_ENTRY,
                            component = LOG_COMPONENT_ECSTORE,
                            subsystem = LOG_SUBSYSTEM_POOLS,
                            pool_index = idx,
                            set_index = set_idx,
                            bucket = %bi.name,
                            state = "entry_worker_join_failed",
                            error = %err,
                            "Decommission entry worker task failed"
                        );
                        record_decommission_entry_error(&entry_error, &rx, err).await;
                    }
                }
            }
        }

        let listing_result = listing_result.unwrap_or_else(|| Err(Error::other("decommission listing task did not complete")));
        if let Some(err) = entry_error.lock().await.clone() {
            return Err(err);
        }
        listing_result
    }

    async fn track_decommission_entry_progress_stage(
        &self,
        idx: usize,
        generation: OffsetDateTime,
        bucket: &str,
        object: &str,
        stage: &'static str,
    ) -> Result<()> {
        {
            let mut pool_meta = self.pool_meta.write().await;
            ensure_decommission_generation(&pool_meta, idx, generation)?;
            track_decommission_current_object_stage(&mut pool_meta, idx, bucket, object, stage)
                .map_err(|err| with_decommission_entry_context(stage, bucket, object, err))?;
        }

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    #[tracing::instrument(skip(
        self,
        set,
        lifecycle_config,
        object_lock_config,
        replication_config,
        source_changed_exhaustions
    ))]
    async fn decommission_entry(
        self: &Arc<Self>,
        rx: CancellationToken,
        idx: usize,
        generation: OffsetDateTime,
        entry: MetaCacheEntry,
        bucket: String,
        set: Arc<SetDisks>,
        lifecycle_config: Option<BucketLifecycleConfiguration>,
        object_lock_config: Option<ObjectLockConfiguration>,
        replication_config: Option<(ReplicationConfiguration, OffsetDateTime)>,
        expected_bucket_incarnation_id: Option<uuid::Uuid>,
        source_changed_exhaustions: Arc<AtomicUsize>,
    ) -> Result<()> {
        let mut counted_versions = HashSet::new();

        for entry_attempt in 1..=DECOMMISSION_ENTRY_MAX_ATTEMPTS {
            match self
                .decommission_entry_attempt(
                    rx.clone(),
                    idx,
                    generation,
                    entry.clone(),
                    bucket.clone(),
                    Arc::clone(&set),
                    lifecycle_config.clone(),
                    object_lock_config.clone(),
                    replication_config.clone(),
                    expected_bucket_incarnation_id,
                    entry_attempt,
                    source_changed_exhaustions.as_ref(),
                    &mut counted_versions,
                )
                .await?
            {
                DecommissionEntryAttemptOutcome::Complete => return Ok(()),
                DecommissionEntryAttemptOutcome::SourceChanged => {
                    let retry_delay = decommission_retry_backoff_delay(DECOMMISSION_SOURCE_CLEANUP_RETRY_DELAY, entry_attempt);
                    warn!(
                        event = EVENT_DECOMMISSION_ENTRY,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_POOLS,
                        state = "source_changed_retry",
                        pool_index = idx,
                        bucket = %bucket,
                        object = %entry.name,
                        attempt = entry_attempt,
                        max_attempts = DECOMMISSION_ENTRY_MAX_ATTEMPTS,
                        retry_delay_ms = retry_delay.as_millis(),
                        "Decommission source changed during cleanup preflight; retrying entry"
                    );
                    if wait_decommission_retry_backoff(&rx, retry_delay).await {
                        decommission_cancel_signal_result(rx.is_cancelled())?;
                    }
                }
            }
        }

        Err(Error::other(format!(
            "decommission entry retry loop ended without a terminal result for {bucket}/{}",
            entry.name
        )))
    }

    #[allow(unused_assignments, clippy::too_many_arguments)]
    async fn decommission_entry_attempt(
        self: &Arc<Self>,
        rx: CancellationToken,
        idx: usize,
        generation: OffsetDateTime,
        entry: MetaCacheEntry,
        bucket: String,
        set: Arc<SetDisks>,
        lifecycle_config: Option<BucketLifecycleConfiguration>,
        object_lock_config: Option<ObjectLockConfiguration>,
        replication_config: Option<(ReplicationConfiguration, OffsetDateTime)>,
        expected_bucket_incarnation_id: Option<uuid::Uuid>,
        entry_attempt: usize,
        source_changed_exhaustions: &AtomicUsize,
        counted_versions: &mut HashSet<(Option<uuid::Uuid>, bool)>,
    ) -> Result<DecommissionEntryAttemptOutcome> {
        debug!(
            event = EVENT_DECOMMISSION_ENTRY,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_POOLS,
            state = "started",
            pool_index = idx,
            bucket = %bucket,
            object = %entry.name,
            attempt = entry_attempt,
            max_attempts = DECOMMISSION_ENTRY_MAX_ATTEMPTS,
            "Decommission entry started"
        );
        if entry.is_dir() {
            debug!(
                event = EVENT_DECOMMISSION_ENTRY,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_POOLS,
                pool_index = idx,
                bucket = %bucket,
                object = %entry.name,
                state = "skipped_directory",
                "Decommission entry skipped directory"
            );
            return Ok(DecommissionEntryAttemptOutcome::Complete);
        }
        let durable_ilm_record = if bucket == RUSTFS_META_BUCKET {
            classify_durable_ilm_record(&entry.name)
                .map_err(|err| with_decommission_entry_context("durable_ilm_namespace", &bucket, &entry.name, err))?
        } else {
            None
        };
        if self.decommission_cancel_requested(idx, &rx).await {
            rx.cancel();
        }
        decommission_cancel_signal_result(rx.is_cancelled())?;
        self.ensure_decommission_generation_current(idx, generation).await?;
        let operation_gate = self.ctx.data_movement_operation_gate();

        let bucket_incarnation_fence = match expected_bucket_incarnation_id {
            Some(expected) => Some(self.acquire_bucket_incarnation_fence(&bucket, expected).await?),
            None => None,
        };

        let mut fivs = load_decommission_entry_exact_versions(&set, &entry, &bucket, "file_info_versions").await?;

        fivs.versions
            .sort_by_key(|v| (v.mod_time.is_none(), std::cmp::Reverse(v.mod_time)));

        let mut decommissioned: usize = 0;
        let mut expired: usize = 0;
        let mut free_version_disposition = DecommissionFreeVersionDisposition::default();
        let mut cleanup_preflight_allowed_missing = Vec::new();
        let mut entry_blocked = false;

        for version in fivs.versions.iter() {
            if self.decommission_cancel_requested(idx, &rx).await {
                rx.cancel();
            }
            decommission_cancel_signal_result(rx.is_cancelled())?;

            if version.tier_free_version() {
                let version_id = version.version_id.map(|v| v.to_string());
                let mut migration_error = None;
                let mut migrated = false;
                let mut consumed = false;
                let mut capacity_failure = false;
                for _ in 0..3 {
                    match classify_decommission_free_version_attempt(
                        self.run_guarded_decommission_side_effect(&rx, &operation_gate, || async {
                            self.decommission_tiered_object(
                                bucket.as_str(),
                                &version.name,
                                version,
                                &decommission_remote_tiered_opts(
                                    version,
                                    version_id.clone(),
                                    idx,
                                    expected_bucket_incarnation_id,
                                ),
                            )
                            .await
                        })
                        .await,
                    ) {
                        DecommissionFreeVersionAttempt::Migrated => {
                            migrated = true;
                            migration_error = None;
                            break;
                        }
                        DecommissionFreeVersionAttempt::Consumed => {
                            consumed = true;
                            migration_error = None;
                            break;
                        }
                        DecommissionFreeVersionAttempt::CapacityFailure(err) => {
                            capacity_failure = true;
                            migration_error = Some(err);
                            break;
                        }
                        DecommissionFreeVersionAttempt::Retry(err) => migration_error = Some(err),
                    }
                }

                if counted_versions.insert((version.version_id, version.deleted)) {
                    let mut pool_meta = self.pool_meta.write().await;
                    ensure_decommission_generation(&pool_meta, idx, generation)?;
                    if let Err(err) = count_decommission_item(&mut pool_meta, idx, 0, !migrated && !consumed) {
                        return Err(with_decommission_entry_context(
                            "count_decommission_item",
                            bucket.as_str(),
                            entry.name.as_str(),
                            err,
                        ));
                    }
                }

                if migrated || consumed {
                    decommissioned += 1;
                    cleanup_preflight_allowed_missing.push(data_movement::source_cleanup_version_identity(version));
                }
                if migrated {
                    free_version_disposition.record_migrated();
                } else if consumed {
                    free_version_disposition.record_consumed();
                } else {
                    free_version_disposition.record_retained();
                }

                debug!(
                    event = EVENT_DECOMMISSION_ENTRY,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_POOLS,
                    pool_index = idx,
                    bucket = %bucket,
                    object = %version.name,
                    version_id = ?version_id,
                    result = ?migration_error,
                    reason = if migrated {
                        DECOMMISSION_FREE_VERSION_MIGRATED_REASON
                    } else if consumed {
                        DECOMMISSION_FREE_VERSION_CONSUMED_REASON
                    } else {
                        DECOMMISSION_FREE_VERSION_RETAINED_REASON
                    },
                    state = if migrated {
                        "free_version_migrated"
                    } else if consumed {
                        "free_version_consumed"
                    } else {
                        "free_version_retained"
                    },
                    "Decommission free-version disposition recorded"
                );

                if capacity_failure {
                    return Err(with_decommission_entry_context(
                        "decommission_tier_free_version",
                        bucket.as_str(),
                        version.name.as_str(),
                        migration_error.expect("capacity failure must retain its error"),
                    ));
                }

                if !migrated && !consumed {
                    break;
                }
                continue;
            }

            if self
                .run_guarded_decommission_side_effect(&rx, &operation_gate, || async {
                    should_skip_lifecycle_for_data_movement(
                        self.clone(),
                        &bucket,
                        version,
                        lifecycle_config.as_ref(),
                        object_lock_config.as_ref(),
                        true,
                        &LcEventSrc::Decom,
                        None,
                    )
                    .await
                })
                .await
                .map_err(|err| with_decommission_entry_context("lifecycle_expiry", bucket.as_str(), version.name.as_str(), err))?
            {
                expired += 1;
                cleanup_preflight_allowed_missing.push(data_movement::source_cleanup_version_identity(version));
                continue;
            }

            let remaining_versions = decommission_remaining_version_count(&fivs.versions, expired);
            if should_skip_decommission_delete_marker(version, remaining_versions, replication_config.is_some()) {
                //
                decommissioned += 1;
                debug!(
                    event = EVENT_DECOMMISSION_ENTRY,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_POOLS,
                    pool_index = idx,
                    bucket = %bucket,
                    object = %version.name,
                    state = "skipped_delete_marker",
                    "Decommission delete marker skipped"
                );
                continue;
            }

            let version_id = version.version_id.map(|v| v.to_string());

            let mut ignore = false;
            let mut cleanup_ignored = false;
            let mut failure = false;
            let mut error = None;
            if version.deleted {
                for version_attempt in 1..=DECOMMISSION_VERSION_COPY_ATTEMPTS {
                    let result = self
                        .run_guarded_decommission_side_effect(&rx, &operation_gate, || async {
                            self.delete_object(
                                bucket.as_str(),
                                &version.name,
                                decommission_delete_marker_opts(version, version_id.clone(), idx, expected_bucket_incarnation_id),
                            )
                            .await
                        })
                        .await;
                    #[cfg(test)]
                    let result = decommission_test_wrap_result(
                        "delete_marker_copy",
                        bucket.as_str(),
                        version.name.as_str(),
                        version_attempt,
                        result,
                    );

                    match result {
                        Ok(_) => {
                            failure = false;
                            error = None;
                            break;
                        }
                        Err(err) if is_decommission_copy_cleanup_safe_error(&err) => {
                            warn!(
                                event = EVENT_DECOMMISSION_ENTRY,
                                component = LOG_COMPONENT_ECSTORE,
                                subsystem = LOG_SUBSYSTEM_POOLS,
                                state = "ignored_delete_marker_copy",
                                pool_index = idx,
                                bucket = %bucket,
                                object = %version.name,
                                version_id = ?version_id,
                                error = ?err,
                                "Decommission delete marker copy ignored"
                            );
                            ignore = true;
                            cleanup_ignored = true;
                            break;
                        }
                        Err(err) if is_decommission_target_capacity_error(&err) => {
                            return Err(with_decommission_entry_context(
                                "delete_marker_copy",
                                bucket.as_str(),
                                version.name.as_str(),
                                err,
                            ));
                        }
                        Err(err) => {
                            failure = true;
                            if version_attempt == DECOMMISSION_VERSION_COPY_ATTEMPTS {
                                error!(
                                    event = EVENT_DECOMMISSION_ENTRY,
                                    component = LOG_COMPONENT_ECSTORE,
                                    subsystem = LOG_SUBSYSTEM_POOLS,
                                    state = "delete_marker_copy_failed",
                                    pool_index = idx,
                                    bucket = %bucket,
                                    object = %version.name,
                                    version_id = ?version_id,
                                    attempts = DECOMMISSION_VERSION_COPY_ATTEMPTS,
                                    error = ?err,
                                    "Decommission delete marker copy failed"
                                );
                                error = Some(err);
                                break;
                            }

                            let retry_delay = decommission_retry_backoff_delay(DECOMMISSION_COPY_RETRY_DELAY, version_attempt);
                            warn!(
                                event = EVENT_DECOMMISSION_ENTRY,
                                component = LOG_COMPONENT_ECSTORE,
                                subsystem = LOG_SUBSYSTEM_POOLS,
                                state = "delete_marker_copy_retry",
                                pool_index = idx,
                                bucket = %bucket,
                                object = %version.name,
                                version_id = ?version_id,
                                attempt = version_attempt,
                                max_attempts = DECOMMISSION_VERSION_COPY_ATTEMPTS,
                                retry_delay_ms = retry_delay.as_millis(),
                                error = ?err,
                                "Decommission delete marker copy failed; retrying"
                            );
                            error = Some(err);
                            if wait_decommission_retry_backoff(&rx, retry_delay).await {
                                decommission_cancel_signal_result(rx.is_cancelled())?;
                            }
                        }
                    }
                }

                if ignore {
                    if should_count_decommission_version_complete(ignore, cleanup_ignored, failure) {
                        decommissioned += 1;
                    }
                    debug!(
                        event = EVENT_DECOMMISSION_ENTRY,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_POOLS,
                        pool_index = idx,
                        bucket = %bucket,
                        object = %version.name,
                        state = "ignored",
                        "Decommission entry ignored"
                    );
                    continue;
                }

                if counted_versions.insert((version.version_id, version.deleted)) {
                    let mut pool_meta = self.pool_meta.write().await;
                    ensure_decommission_generation(&pool_meta, idx, generation)?;
                    if let Err(err) = count_decommission_item(&mut pool_meta, idx, 0, failure) {
                        return Err(with_decommission_entry_context(
                            "count_decommission_item",
                            bucket.as_str(),
                            entry.name.as_str(),
                            err,
                        ));
                    }
                }

                if !failure {
                    decommissioned += 1;
                }

                if !failure {
                    debug!(
                        event = EVENT_DECOMMISSION_ENTRY,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_POOLS,
                        state = "delete_marker_copied",
                        pool_index = idx,
                        bucket = %bucket,
                        object = %version.name,
                        version_id = ?version_id,
                        result = ?error,
                        "Decommission delete marker copied"
                    );
                }
                continue;
            }

            for version_attempt in 1..=DECOMMISSION_VERSION_COPY_ATTEMPTS {
                if version.is_remote() {
                    let result = self
                        .run_guarded_decommission_side_effect(&rx, &operation_gate, || async {
                            self.decommission_tiered_object(
                                bucket.as_str(),
                                &version.name,
                                version,
                                &decommission_remote_tiered_opts(
                                    version,
                                    version_id.clone(),
                                    idx,
                                    expected_bucket_incarnation_id,
                                ),
                            )
                            .await
                        })
                        .await;
                    #[cfg(test)]
                    let result = decommission_test_wrap_result(
                        "decommission_tiered_object",
                        bucket.as_str(),
                        version.name.as_str(),
                        version_attempt,
                        result,
                    );

                    match result {
                        Ok(_) => {
                            failure = false;
                            error = None;
                        }
                        Err(err) if is_decommission_copy_cleanup_safe_error(&err) => {
                            ignore = true;
                            cleanup_ignored = true;
                        }
                        Err(err) if is_decommission_target_capacity_error(&err) => {
                            return Err(with_decommission_entry_context(
                                "decommission_tiered_object",
                                bucket.as_str(),
                                version.name.as_str(),
                                err,
                            ));
                        }
                        Err(err) => {
                            failure = true;
                            if version_attempt == DECOMMISSION_VERSION_COPY_ATTEMPTS {
                                error!(
                                    event = EVENT_DECOMMISSION_ENTRY,
                                    component = LOG_COMPONENT_ECSTORE,
                                    subsystem = LOG_SUBSYSTEM_POOLS,
                                    state = "tiered_copy_failed",
                                    pool_index = idx,
                                    bucket = %bucket,
                                    object = %version.name,
                                    version_id = ?version_id,
                                    attempts = DECOMMISSION_VERSION_COPY_ATTEMPTS,
                                    error = ?err,
                                    "Decommission tiered version copy failed"
                                );
                                error = Some(err);
                            } else {
                                let retry_delay =
                                    decommission_retry_backoff_delay(DECOMMISSION_COPY_RETRY_DELAY, version_attempt);
                                warn!(
                                    event = EVENT_DECOMMISSION_ENTRY,
                                    component = LOG_COMPONENT_ECSTORE,
                                    subsystem = LOG_SUBSYSTEM_POOLS,
                                    state = "tiered_copy_retry",
                                    pool_index = idx,
                                    bucket = %bucket,
                                    object = %version.name,
                                    version_id = ?version_id,
                                    attempt = version_attempt,
                                    max_attempts = DECOMMISSION_VERSION_COPY_ATTEMPTS,
                                    retry_delay_ms = retry_delay.as_millis(),
                                    error = ?err,
                                    "Decommission tiered version copy failed; retrying"
                                );
                                error = Some(err);
                                if wait_decommission_retry_backoff(&rx, retry_delay).await {
                                    decommission_cancel_signal_result(rx.is_cancelled())?;
                                }
                                continue;
                            }
                        }
                    }
                    break;
                }

                let bucket = bucket.clone();

                let rd = match set
                    .get_object_reader(
                        bucket.as_str(),
                        &encode_dir_object(&version.name),
                        None,
                        HeaderMap::new(),
                        &decommission_object_migration_read_opts(version_id.clone()),
                    )
                    .await
                {
                    Ok(rd) => rd,
                    Err(err) => {
                        if is_err_object_not_found(&err) || is_err_version_not_found(&err) {
                            ignore = true;
                            cleanup_ignored = true;
                            break;
                        }

                        if !ignore {
                            //
                            if bucket == RUSTFS_META_BUCKET && version.name.contains(DATA_USAGE_CACHE_NAME) {
                                ignore = true;
                                error!("decommission_pool: ignore data usage cache {}", &version.name);
                                break;
                            }
                        }

                        failure = true;
                        if version_attempt == DECOMMISSION_VERSION_COPY_ATTEMPTS {
                            error!(
                                event = EVENT_DECOMMISSION_ENTRY,
                                component = LOG_COMPONENT_ECSTORE,
                                subsystem = LOG_SUBSYSTEM_POOLS,
                                state = "object_read_failed",
                                pool_index = idx,
                                bucket = %bucket,
                                object = %version.name,
                                version_id = ?version_id,
                                attempts = DECOMMISSION_VERSION_COPY_ATTEMPTS,
                                error = ?err,
                                "Decommission source object read failed"
                            );
                            error = Some(err);
                            continue;
                        }

                        let retry_delay = decommission_retry_backoff_delay(DECOMMISSION_COPY_RETRY_DELAY, version_attempt);
                        warn!(
                            event = EVENT_DECOMMISSION_ENTRY,
                            component = LOG_COMPONENT_ECSTORE,
                            subsystem = LOG_SUBSYSTEM_POOLS,
                            state = "object_read_retry",
                            pool_index = idx,
                            bucket = %bucket,
                            object = %version.name,
                            version_id = ?version_id,
                            attempt = version_attempt,
                            max_attempts = DECOMMISSION_VERSION_COPY_ATTEMPTS,
                            retry_delay_ms = retry_delay.as_millis(),
                            error = ?err,
                            "Decommission source object read failed; retrying"
                        );
                        error = Some(err);
                        if wait_decommission_retry_backoff(&rx, retry_delay).await {
                            decommission_cancel_signal_result(rx.is_cancelled())?;
                        }
                        continue;
                    }
                };

                let bucket_name = bucket.clone();
                let object_name = rd.object_info.name.clone();

                self.track_decommission_entry_progress_stage(
                    idx,
                    generation,
                    bucket_name.as_str(),
                    object_name.as_str(),
                    DECOMMISSION_STAGE_MIGRATE_OBJECT,
                )
                .await?;

                let migrate_result = self
                    .run_guarded_decommission_side_effect(&rx, &operation_gate, || async {
                        self.clone()
                            .decommission_object(idx, bucket, rd, expected_bucket_incarnation_id)
                            .await
                    })
                    .await;
                #[cfg(test)]
                let migrate_result = decommission_test_wrap_result(
                    DECOMMISSION_STAGE_MIGRATE_OBJECT,
                    bucket_name.as_str(),
                    object_name.as_str(),
                    version_attempt,
                    migrate_result,
                );

                if let Err(err) = migrate_result {
                    if is_decommission_copy_cleanup_safe_error(&err) {
                        ignore = true;
                        cleanup_ignored = true;
                        break;
                    }

                    if is_decommission_target_capacity_error(&err) {
                        return Err(with_decommission_entry_context(
                            DECOMMISSION_STAGE_MIGRATE_OBJECT,
                            bucket_name.as_str(),
                            object_name.as_str(),
                            err,
                        ));
                    }

                    failure = true;
                    if version_attempt == DECOMMISSION_VERSION_COPY_ATTEMPTS {
                        error!(
                            event = EVENT_DECOMMISSION_ENTRY,
                            component = LOG_COMPONENT_ECSTORE,
                            subsystem = LOG_SUBSYSTEM_POOLS,
                            state = "object_migration_failed",
                            pool_index = idx,
                            bucket = %bucket_name,
                            object = %object_name,
                            version = %version.name,
                            attempt = version_attempt,
                            max_attempts = DECOMMISSION_VERSION_COPY_ATTEMPTS,
                            error = ?err,
                            "Decommission object migration failed"
                        );
                        error = Some(err);
                        continue;
                    }

                    let retry_delay = decommission_retry_backoff_delay(DECOMMISSION_COPY_RETRY_DELAY, version_attempt);
                    warn!(
                        event = EVENT_DECOMMISSION_ENTRY,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_POOLS,
                        state = "object_migration_retry",
                        pool_index = idx,
                        bucket = %bucket_name,
                        object = %object_name,
                        version = %version.name,
                        attempt = version_attempt,
                        max_attempts = DECOMMISSION_VERSION_COPY_ATTEMPTS,
                        retry_delay_ms = retry_delay.as_millis(),
                        error = ?err,
                        "Decommission object migration failed; retrying"
                    );
                    error = Some(err);
                    if wait_decommission_retry_backoff(&rx, retry_delay).await {
                        decommission_cancel_signal_result(rx.is_cancelled())?;
                    }
                    continue;
                }

                warn!(
                    event = EVENT_DECOMMISSION_ENTRY,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_POOLS,
                    pool_index = idx,
                    bucket = %bucket_name,
                    object = %object_name,
                    version = %version.name,
                    state = "object_migrated",
                    "Decommission object migrated"
                );

                failure = false;
                break;
            }

            if ignore {
                if should_count_decommission_version_complete(ignore, cleanup_ignored, failure) {
                    decommissioned += 1;
                }
                debug!(
                    event = EVENT_DECOMMISSION_ENTRY,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_POOLS,
                    pool_index = idx,
                    bucket = %bucket,
                    object = %version.name,
                    state = "ignored",
                    "Decommission entry ignored"
                );
                continue;
            }

            if counted_versions.insert((version.version_id, version.deleted)) {
                let mut pool_meta = self.pool_meta.write().await;
                ensure_decommission_generation(&pool_meta, idx, generation)?;
                if let Err(err) = count_decommission_item(&mut pool_meta, idx, decommission_item_size(version.size), failure) {
                    return Err(with_decommission_entry_context(
                        "count_decommission_item",
                        bucket.as_str(),
                        entry.name.as_str(),
                        err,
                    ));
                }
            }

            if failure {
                break;
            }

            if should_count_decommission_version_complete(ignore, cleanup_ignored, failure) {
                decommissioned += 1;
            }
        }

        if free_version_disposition.total() > 0 {
            debug!(
                event = EVENT_DECOMMISSION_ENTRY,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_POOLS,
                pool_index = idx,
                bucket = %bucket,
                object = %entry.name,
                free_versions_migrated = free_version_disposition.migrated,
                free_versions_consumed = free_version_disposition.consumed,
                free_versions_retained = free_version_disposition.retained,
                free_versions_total = free_version_disposition.total(),
                reason = DECOMMISSION_FREE_VERSION_DISPOSITION_REASON,
                state = "free_version_disposition",
                "Decommission free-version disposition summary"
            );
        }

        if should_cleanup_decommission_source_entry(decommissioned, fivs.versions.len(), expired) && durable_ilm_record.is_none()
        {
            if bucket_incarnation_fence.as_ref().is_some_and(|guard| guard.is_lock_lost()) {
                return Err(Error::other("decommission bucket incarnation fence was lost before source cleanup"));
            }
            decommission_cancel_signal_result(rx.is_cancelled())?;
            self.ensure_decommission_generation_current(idx, generation).await?;

            self.track_decommission_entry_progress_stage(
                idx,
                generation,
                bucket.as_str(),
                entry.name.as_str(),
                DECOMMISSION_STAGE_CLEANUP_PREFLIGHT,
            )
            .await?;

            self.track_decommission_entry_progress_stage(
                idx,
                generation,
                bucket.as_str(),
                entry.name.as_str(),
                DECOMMISSION_STAGE_SOURCE_CLEANUP,
            )
            .await?;

            #[cfg(test)]
            run_decommission_cleanup_mutation_hook(bucket.as_str(), entry.name.as_str(), entry_attempt).await;

            let source_cleanup_mutation_fence = self
                .acquire_decommission_source_cleanup_fence(bucket.as_str(), entry.name.as_str(), set.as_ref())
                .await?;
            let cleanup_result = self
                .run_guarded_decommission_side_effect(&rx, &operation_gate, || async {
                    data_movement::cleanup_source_entry_if_unchanged(
                        set.clone(),
                        bucket.as_str(),
                        entry.name.as_str(),
                        &fivs,
                        &cleanup_preflight_allowed_missing,
                        data_movement::SourceCleanupBucketFence {
                            expected_incarnation_id: expected_bucket_incarnation_id,
                            lifecycle_guard: bucket_incarnation_fence
                                .as_ref()
                                .and_then(|guard| guard.namespace_lock_guard()),
                            namespace_lock_lost_signal: None,
                            object_mutation_fence: Some(&source_cleanup_mutation_fence),
                        },
                        "decommission",
                    )
                    .await
                })
                .await;
            match cleanup_result {
                Ok(_) => {}
                Err(data_movement::SourceCleanupError::Storage(err)) => {
                    resolve_decommission_entry_cleanup_delete_result(
                        Err::<(), Error>(err),
                        bucket.as_str(),
                        entry.name.as_str(),
                    )?;
                }
                Err(data_movement::SourceCleanupError::SourceChanged) if entry_attempt < DECOMMISSION_ENTRY_MAX_ATTEMPTS => {
                    return Ok(DecommissionEntryAttemptOutcome::SourceChanged);
                }
                Err(data_movement::SourceCleanupError::SourceChanged) => {
                    let exhausted_entries = source_changed_exhaustions.fetch_add(1, Ordering::Relaxed) + 1;
                    if should_fail_decommission_pool_after_exhausted_source_changed(exhausted_entries) {
                        return Err(Error::other(format!(
                            "decommission source cleanup retries exhausted for {}/{} on all {} attempts; exhausted entries exceed pool limit {}",
                            bucket, entry.name, DECOMMISSION_ENTRY_MAX_ATTEMPTS, DECOMMISSION_SOURCE_CHANGED_EXHAUSTION_LIMIT
                        )));
                    }

                    {
                        let mut pool_meta = self.pool_meta.write().await;
                        ensure_decommission_generation(&pool_meta, idx, generation)?;
                        count_decommission_item(&mut pool_meta, idx, 0, true).map_err(|err| {
                            with_decommission_entry_context(
                                "count_source_changed_exhaustion",
                                bucket.as_str(),
                                entry.name.as_str(),
                                err,
                            )
                        })?;
                    }

                    error!(
                        event = EVENT_DECOMMISSION_ENTRY,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_POOLS,
                        state = "source_cleanup_exhausted",
                        pool_index = idx,
                        bucket = %bucket,
                        object = %entry.name,
                        attempts = DECOMMISSION_ENTRY_MAX_ATTEMPTS,
                        exhausted_entries,
                        exhaustion_limit = DECOMMISSION_SOURCE_CHANGED_EXHAUSTION_LIMIT,
                        "Decommission source cleanup retries exhausted; source retained and entry marked failed"
                    );
                    entry_blocked = true;
                }
            }
        } else if durable_ilm_record.is_some() {
            debug!(
                event = EVENT_DECOMMISSION_ENTRY,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_POOLS,
                pool_index = idx,
                bucket = %bucket,
                object = %entry.name,
                state = "retained_for_final_verification",
                "Decommission durable ILM source retained for final verification"
            );
        } else if decommissioned != fivs.versions.len() || expired > 0 {
            warn!(
                event = EVENT_DECOMMISSION_ENTRY,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_POOLS,
                pool_index = idx,
                bucket = %bucket,
                object = %entry.name,
                decommissioned,
                total_versions = fivs.versions.len(),
                expired,
                state = "source_retained",
                "Decommission source object retained"
            );
        }

        let should_save_progress = {
            let mut pool_meta = self.pool_meta.write().await;
            ensure_decommission_generation(&pool_meta, idx, generation)?;

            if let Err(err) = track_decommission_current_object(&mut pool_meta, idx, bucket.as_str(), entry.name.as_str()) {
                return Err(with_decommission_entry_context(
                    "track_decommission_current_object",
                    bucket.as_str(),
                    entry.name.as_str(),
                    err,
                ));
            }

            match resolve_decommission_update_after_result(pool_meta.update_after(idx, DECOMMISSION_PROGRESS_SAVE_INTERVAL)) {
                Ok(ok) => ok,
                Err(err) => {
                    return Err(with_decommission_entry_context("update_after", bucket.as_str(), entry.name.as_str(), err));
                }
            }
        };

        self.track_decommission_entry_progress_stage(
            idx,
            generation,
            bucket.as_str(),
            entry.name.as_str(),
            DECOMMISSION_STAGE_ENTRY_FINISHED,
        )
        .await?;

        if should_save_progress {
            match self.save_decommission_progress_checkpoint(idx, generation).await {
                Ok(true) => {
                    if let Some(notification_sys) = runtime_sources::notification_sys()
                        && let Err(err) = resolve_decommission_entry_reload_result(
                            notification_sys.reload_pool_meta().await,
                            bucket.as_str(),
                            entry.name.as_str(),
                        )
                    {
                        warn!("{err}");
                    }
                }
                Ok(false) => {}
                Err(err) => {
                    if let Some(err) = resolve_decommission_progress_save_result(Err(err)) {
                        warn!(
                            event = EVENT_DECOMMISSION_ENTRY,
                            component = LOG_COMPONENT_ECSTORE,
                            subsystem = LOG_SUBSYSTEM_POOLS,
                            pool_index = idx,
                            bucket = %bucket,
                            object = %entry.name,
                            state = "progress_save_failed",
                            error = %err,
                            "Decommission progress save failed; continuing and will retry at the next checkpoint"
                        );
                    }
                }
            }
        }

        debug!(
            event = EVENT_DECOMMISSION_ENTRY,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_POOLS,
            state = if entry_blocked { "blocked" } else { "completed" },
            pool_index = idx,
            bucket = %bucket,
            object = %entry.name,
            "Decommission entry finished"
        );
        Ok(DecommissionEntryAttemptOutcome::Complete)
    }

    #[cfg(test)]
    pub(crate) async fn decommission_entry_for_test(
        self: &Arc<Self>,
        idx: usize,
        entry: MetaCacheEntry,
        bucket: String,
        set: Arc<SetDisks>,
    ) -> Result<()> {
        self.decommission_entry_with_retry_state_for_test(
            CancellationToken::new(),
            idx,
            entry,
            bucket,
            set,
            None,
            Arc::new(AtomicUsize::new(0)),
        )
        .await
    }

    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn decommission_entry_with_retry_state_for_test(
        self: &Arc<Self>,
        rx: CancellationToken,
        idx: usize,
        entry: MetaCacheEntry,
        bucket: String,
        set: Arc<SetDisks>,
        expected_bucket_incarnation_id: Option<uuid::Uuid>,
        source_changed_exhaustions: Arc<AtomicUsize>,
    ) -> Result<()> {
        let generation = self.active_decommission_generation(idx).await?;
        self.decommission_entry(
            rx,
            idx,
            generation,
            entry,
            bucket,
            set,
            None,
            None,
            None,
            expected_bucket_incarnation_id,
            source_changed_exhaustions,
        )
        .await
    }

    #[cfg(all(test, feature = "test-util"))]
    pub(crate) async fn decommission_entry_for_test_with_bucket_incarnation(
        self: &Arc<Self>,
        idx: usize,
        entry: MetaCacheEntry,
        bucket: String,
        set: Arc<SetDisks>,
    ) -> Result<()> {
        let expected_bucket_incarnation_id = if is_meta_bucketname(&bucket) {
            None
        } else {
            Some(self.bucket_incarnation_id_from_disk(&bucket).await?)
        };
        let generation = self.active_decommission_generation(idx).await?;
        self.decommission_entry(
            CancellationToken::new(),
            idx,
            generation,
            entry,
            bucket,
            set,
            None,
            None,
            None,
            expected_bucket_incarnation_id,
            Arc::new(AtomicUsize::new(0)),
        )
        .await
    }

    #[tracing::instrument(skip(self, rx))]
    async fn decommission_pool(
        self: &Arc<Self>,
        rx: CancellationToken,
        idx: usize,
        pool: Arc<Sets>,
        bi: DecomBucketInfo,
        entry_budget: Arc<Semaphore>,
        source_changed_exhaustions: Arc<AtomicUsize>,
    ) -> Result<()> {
        let entry_error = Arc::new(tokio::sync::Mutex::new(None::<Error>));
        let generation = self.active_decommission_generation(idx).await?;
        let mut listing_workers = Vec::with_capacity(pool.disk_set.len());

        let mut lifecycle_config = None;
        let mut object_lock_config = None;
        let mut replication_config = None;
        let expected_bucket_incarnation_id = if bi.name == RUSTFS_META_BUCKET {
            None
        } else {
            Some(self.bucket_incarnation_id_from_disk(&bi.name).await?)
        };

        if bi.name != RUSTFS_META_BUCKET {
            let _ = resolve_decommission_optional_bucket_config_result(
                &bi.name,
                "versioning",
                BucketVersioningSys::get(&bi.name).await,
            )?;
            let expiry_configs = get_expiry_configs(self, &bi.name).await?;
            lifecycle_config = expiry_configs.lifecycle.map(|config| (*config).clone());
            object_lock_config = expiry_configs.object_lock.map(|config| (*config).clone());
            replication_config = resolve_decommission_optional_bucket_config_result(
                &bi.name,
                "replication",
                metadata_sys::get_replication_config(&bi.name).await,
            )?;
        }

        for (set_idx, set) in pool.disk_set.iter().enumerate() {
            debug!(
                event = EVENT_DECOMMISSION_BUCKET,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_POOLS,
                pool_index = idx,
                set_index = set_idx,
                bucket = %bi.name,
                state = "listing_worker_started",
                "Decommission listing worker started"
            );

            let set = set.clone();
            let store = Arc::clone(self);
            let rx_clone = rx.clone();
            let bi_clone = bi.clone();
            let lifecycle_config = lifecycle_config.clone();
            let object_lock_config = object_lock_config.clone();
            let replication_config = replication_config.clone();
            let entry_budget = entry_budget.clone();
            let source_changed_exhaustions = Arc::clone(&source_changed_exhaustions);
            let entry_error = entry_error.clone();
            let worker = tokio::spawn(async move {
                store
                    .decommission_set(
                        rx_clone,
                        idx,
                        set_idx,
                        generation,
                        set,
                        bi_clone,
                        lifecycle_config,
                        object_lock_config,
                        replication_config,
                        expected_bucket_incarnation_id,
                        source_changed_exhaustions,
                        entry_budget,
                        entry_error,
                    )
                    .await
            });
            listing_workers.push((set_idx, worker));
        }

        debug!(
            event = EVENT_DECOMMISSION_BUCKET,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_POOLS,
            pool_index = idx,
            bucket = %bi.name,
            state = "waiting_for_workers",
            "Decommission waiting for workers"
        );

        let mut listing_worker_error = None;
        for (set_id, worker) in listing_workers {
            if let Err(err) = resolve_decommission_listing_worker_result(set_id, worker.await) {
                rx.cancel();
                if listing_worker_error.is_none() {
                    listing_worker_error = Some(err);
                }
            }
        }

        if let Some(err) = listing_worker_error {
            return Err(err);
        }

        if let Some(err) = entry_error.lock().await.clone() {
            return Err(err);
        }

        if let Err(err) = decommission_cancel_signal_result(rx.is_cancelled()) {
            warn!(
                event = EVENT_DECOMMISSION_BUCKET,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_POOLS,
                pool_index = idx,
                bucket = %bi.name,
                state = "cancelled_after_wait",
                error = %err,
                "Decommission bucket cancelled after wait"
            );
            return Err(err);
        }

        debug!(
            event = EVENT_DECOMMISSION_BUCKET,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_POOLS,
            pool_index = idx,
            bucket = %bi.name,
            state = "completed",
            "Decommission bucket completed"
        );

        Ok(())
    }

    #[cfg(all(test, feature = "test-util"))]
    pub(crate) async fn decommission_pool_for_test(
        self: &Arc<Self>,
        rx: CancellationToken,
        idx: usize,
        pool: Arc<Sets>,
        bucket: DecomBucketInfo,
    ) -> Result<()> {
        self.decommission_pool(
            rx,
            idx,
            pool,
            bucket,
            Arc::new(Semaphore::new(decommission_entry_concurrency_limit())),
            Arc::new(AtomicUsize::new(0)),
        )
        .await
    }

    #[tracing::instrument(skip(self, canceler))]
    pub async fn do_decommission_in_routine(
        self: &Arc<Self>,
        canceler: DecommissionCanceler,
        idx: usize,
        entry_budget: Arc<Semaphore>,
    ) -> Result<()> {
        let rx = canceler.token().clone();
        self.run_decommission_in_routine(rx, idx, &canceler, entry_budget).await
    }

    async fn run_decommission_in_routine(
        self: &Arc<Self>,
        rx: CancellationToken,
        idx: usize,
        canceler: &DecommissionCanceler,
        entry_budget: Arc<Semaphore>,
    ) -> Result<()> {
        self.ensure_pool_meta_side_effects_safe("decommission cannot run while pool metadata requires recovery")
            .await?;
        let generation = match self.promote_queued_decommission(idx, canceler).await {
            Ok(generation) => generation,
            Err(Error::OperationCanceled) => return Ok(()),
            Err(err) => {
                resolve_decommission_terminal_mark_after_error_result(
                    self.decommission_failed_for_operation(idx, canceler).await,
                    idx,
                    &err,
                )?;
                return Err(err);
            }
        };
        if rx.is_cancelled() {
            let already_canceled = {
                let pool_meta = self.pool_meta.read().await;
                should_skip_canceled_decommission_routine(true, pool_meta.pools.get(idx))
            };
            if already_canceled {
                warn!(
                    event = EVENT_DECOMMISSION_STATE,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_POOLS,
                    pool_index = idx,
                    state = "canceled_preserved",
                    "Decommission routine skipped because pool is already canceled"
                );
                return Ok(());
            }
            if let Err(err) = self.decommission_cancel_for_operation(idx, canceler).await {
                resolve_decommission_terminal_mark_after_error_result(
                    self.decommission_failed_for_operation(idx, canceler).await,
                    idx,
                    &err,
                )?;
                return Err(err);
            }
            return Ok(());
        }
        let result = self.decommission_in_background(rx.clone(), idx, entry_budget).await;

        let (final_state, canceled, cmd_line) = {
            let pool_meta = self.pool_meta.read().await;
            let Some(pool) = pool_meta.pools.get(idx) else {
                error!(
                    event = EVENT_DECOMMISSION_STATE,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_POOLS,
                    pool_index = idx,
                    state = "pool_metadata_missing",
                    "Decommission pool metadata missing"
                );
                return Err(Error::other(format!(
                    "failed to resolve decommission final state: pool metadata missing for idx {idx}"
                )));
            };

            let (final_state, canceled) = if let Some(info) = &pool.decommission {
                (
                    determine_decommission_final_state(info.items_decommission_failed, info.canceled),
                    info.canceled,
                )
            } else {
                (DecommissionFinalState::Failed, false)
            };
            let cmd_line = pool.cmd_line.clone();
            (final_state, canceled, cmd_line)
        };

        if let Err(err) = result {
            error!(
                event = EVENT_DECOMMISSION_STATE,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_POOLS,
                pool_index = idx,
                state = "background_failed",
                error = ?err,
                "Decommission background routine failed"
            );

            if should_preserve_decommission_canceled_state(canceled, rx.is_cancelled()) {
                warn!(
                    event = EVENT_DECOMMISSION_STATE,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_POOLS,
                    pool_index = idx,
                    cmd_line = %cmd_line,
                    state = "cancelled_preserved",
                    "Decommission cancelled; preserving canceled state"
                );
                return Ok(());
            }

            resolve_decommission_terminal_mark_after_error_result(
                self.decommission_failed_for_operation(idx, canceler).await,
                idx,
                &err,
            )?;
            warn!(
                event = EVENT_DECOMMISSION_STATE,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_POOLS,
                pool_index = idx,
                state = "marked_failed",
                "Decommission marked failed"
            );

            return Ok(());
        }

        debug!(
            event = EVENT_DECOMMISSION_STATE,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_POOLS,
            pool_index = idx,
            state = "background_complete",
            "Decommission background routine completed"
        );

        if should_preserve_decommission_canceled_state(canceled, rx.is_cancelled()) {
            warn!(
                event = EVENT_DECOMMISSION_STATE,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_POOLS,
                pool_index = idx,
                cmd_line = %cmd_line,
                state = "terminal_state_preserved",
                "Decommission terminal state preserved after cancellation"
            );
            return Ok(());
        }

        match final_state {
            DecommissionFinalState::Complete => {
                debug!(
                    event = EVENT_DECOMMISSION_STATE,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_POOLS,
                    pool_index = idx,
                    cmd_line = %cmd_line,
                    state = "verifying_completion",
                    "Decommission completion verification started"
                );
                let verified_unresolved_entries = match self.check_after_decommission(idx, &rx, generation).await {
                    Ok(verified_entries) => verified_entries,
                    Err(err) => {
                        if is_err_operation_canceled(&err) {
                            return Err(err);
                        }
                        resolve_decommission_terminal_mark_result(
                            self.decommission_failed_for_operation(idx, canceler).await,
                            "failed",
                            &cmd_line,
                        )?;
                        return Err(Error::other(format!(
                            "failed to finalize decommission for pool {cmd_line}: post-check failed: {err}"
                        )));
                    }
                };
                info!(
                    event = EVENT_DECOMMISSION_STATE,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_POOLS,
                    pool_index = idx,
                    cmd_line = %cmd_line,
                    state = "marking_completed",
                    "Decommission marking completed state"
                );
                if let Err(err) = self
                    .complete_decommission_for_operation(idx, canceler, generation, verified_unresolved_entries)
                    .await
                {
                    resolve_decommission_terminal_mark_result(
                        self.decommission_failed_for_operation(idx, canceler).await,
                        "failed",
                        &cmd_line,
                    )?;
                    return Err(Error::other(format!("failed to finalize decommission for pool {cmd_line}: {err}")));
                }
            }
            DecommissionFinalState::Failed => {
                warn!(
                    event = EVENT_DECOMMISSION_STATE,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_POOLS,
                    pool_index = idx,
                    cmd_line = %cmd_line,
                    state = "marking_failed",
                    "Decommission marking failed state"
                );
                resolve_decommission_terminal_mark_result(
                    self.decommission_failed_for_operation(idx, canceler).await,
                    "failed",
                    &cmd_line,
                )?;
            }
        }

        info!(
            event = EVENT_DECOMMISSION_STATE,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_POOLS,
            pool_index = idx,
            cmd_line = %cmd_line,
            state = "completed",
            "Decommission completed"
        );
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub async fn decommission_failed(&self, idx: usize) -> Result<()> {
        self.decommission_failed_with_owner(idx, None).await
    }

    async fn decommission_failed_for_operation(&self, idx: usize, owner: &DecommissionCanceler) -> Result<()> {
        self.decommission_failed_with_owner(idx, Some(owner)).await
    }

    async fn decommission_failed_with_owner(&self, idx: usize, owner: Option<&DecommissionCanceler>) -> Result<()> {
        self.decommission_failed_with_owner_and_save(idx, owner, self.save_current_pool_meta(&[idx]))
            .await
    }

    async fn decommission_failed_with_owner_and_save<SaveFuture>(
        &self,
        idx: usize,
        owner: Option<&DecommissionCanceler>,
        save_pool_meta: SaveFuture,
    ) -> Result<()>
    where
        SaveFuture: Future<Output = Result<()>>,
    {
        ensure_decommission_terminal_operation_supported(self.single_pool(), "mark decommission failed")?;
        let _start_guard = self.start_gate.lock().await;
        let movement_gate = self.ctx.data_movement_operation_gate();
        let _movement_guard = movement_gate.write().await;

        // Lock order: movement gate, then decommission_cancelers, then pool_meta.
        // Holding both state locks makes owner validation and the terminal
        // transition one atomic operation.
        let (should_reload_pool_meta, previous_pool_meta, terminal_canceler) = {
            let cancelers = self.decommission_cancelers.read().await;
            let mut pool_meta = self.pool_meta.write().await;
            let previous_pool_meta = pool_meta.clone();
            let Some(changed) =
                update_decommission_for_operation(cancelers.as_slice(), &mut pool_meta, idx, owner, |pool_meta| {
                    pool_meta.decommission_failed(idx)
                })
            else {
                return Ok(());
            };
            let terminal_canceler = if let Some(owner) = owner {
                Some(owner.clone())
            } else {
                cancelers.get(idx).and_then(Option::as_ref).cloned()
            };
            (changed, changed.then_some(previous_pool_meta), terminal_canceler)
        };

        if should_reload_pool_meta && let Err(err) = save_pool_meta.await {
            if let Some(previous_pool_meta) = previous_pool_meta {
                let mut pool_meta = self.pool_meta.write().await;
                rollback_decommission_pool_meta(&mut pool_meta, &previous_pool_meta, &[idx]);
            }
            return Err(err);
        }
        if should_reload_pool_meta {
            {
                let mut pool_meta = self.pool_meta.write().await;
                pool_meta.mark_decommission_progress_saved();
            }
        }
        if let Some(canceler) = terminal_canceler.as_ref() {
            self.release_decommission_canceler_slot(idx, canceler).await;
        }

        if should_reload_pool_meta {
            self.ctx.advance_data_movement_operation_epoch();
        }
        drop(_movement_guard);

        if should_reload_pool_meta && let Some(notification_sys) = runtime_sources::notification_sys() {
            let stage = format!("decommission_failed for pool {idx}");
            if let Some(err) = observe_decommission_terminal_reload_result(
                resolve_decommission_pool_meta_reload_result(notification_sys.reload_pool_meta().await, stage.as_str()),
                stage.as_str(),
            ) {
                if let Err(record_err) = self
                    .record_decommission_terminal_reload_failure(idx, stage.as_str(), err.clone())
                    .await
                {
                    warn!(
                        event = EVENT_DECOMMISSION_STATE,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_POOLS,
                        pool_index = idx,
                        state = "terminal_reload_record_failed",
                        error = %record_err,
                        original_error = %err,
                        "Decommission terminal reload failure record failed"
                    );
                }
                warn!(
                    event = EVENT_DECOMMISSION_STATE,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_POOLS,
                    pool_index = idx,
                    state = "terminal_reload_failed",
                    error = %err,
                    "Decommission terminal state saved but pool meta reload failed"
                );
            }
        }

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub async fn complete_decommission(&self, idx: usize) -> Result<()> {
        self.complete_decommission_with_owner(idx, None, None, None).await
    }

    async fn complete_decommission_for_operation(
        &self,
        idx: usize,
        owner: &DecommissionCanceler,
        verified_generation: OffsetDateTime,
        verified_unresolved_entries: Vec<DecommissionUnresolvedEntry>,
    ) -> Result<()> {
        self.complete_decommission_with_owner(idx, Some(owner), Some(verified_generation), Some(verified_unresolved_entries))
            .await
    }

    async fn complete_decommission_with_owner(
        &self,
        idx: usize,
        owner: Option<&DecommissionCanceler>,
        verified_generation: Option<OffsetDateTime>,
        verified_unresolved_entries: Option<Vec<DecommissionUnresolvedEntry>>,
    ) -> Result<()> {
        ensure_decommission_terminal_operation_supported(self.single_pool(), "complete decommission")?;
        ensure_valid_decommission_pool_index(self.pools.len(), idx)?;
        if let Some(owner) = owner {
            let cancelers = self.decommission_cancelers.read().await;
            if !decommission_canceler_is_owned_by(cancelers.as_slice(), idx, owner) {
                owner.release();
                return Ok(());
            }
        }
        self.verify_decommission_durable_ilm_receipts(idx).await?;
        let _start_guard = self.start_gate.lock().await;
        let movement_gate = self.ctx.data_movement_operation_gate();
        let _movement_guard = movement_gate.write().await;

        // Lock order: movement gate, then decommission_cancelers, then pool_meta.
        // Holding both state locks makes owner validation and the terminal
        // transition one atomic operation.
        let (should_reload_pool_meta, completed, previous_pool_meta, terminal_canceler) = {
            let cancelers = self.decommission_cancelers.read().await;
            let mut pool_meta = self.pool_meta.write().await;
            let previous_pool_meta = pool_meta.clone();
            let Some(changed) =
                update_decommission_for_operation(cancelers.as_slice(), &mut pool_meta, idx, owner, |pool_meta| {
                    reconcile_decommission_unresolved_entries_for_completion(
                        pool_meta,
                        idx,
                        verified_generation,
                        verified_unresolved_entries.as_deref(),
                    )?;
                    Ok::<bool, Error>(pool_meta.decommission_complete(idx))
                })
            else {
                return Ok(());
            };
            let changed = changed?;
            let completed = pool_meta
                .pools
                .get(idx)
                .and_then(|pool| pool.decommission.as_ref())
                .is_some_and(|decommission| decommission.complete);
            let terminal_canceler = if let Some(owner) = owner {
                Some(owner.clone())
            } else {
                cancelers.get(idx).and_then(Option::as_ref).cloned()
            };
            (changed, completed, changed.then_some(previous_pool_meta), terminal_canceler)
        };

        if should_reload_pool_meta && let Err(err) = self.save_current_pool_meta(&[idx]).await {
            if let Some(previous_pool_meta) = previous_pool_meta {
                let mut pool_meta = self.pool_meta.write().await;
                rollback_decommission_pool_meta(&mut pool_meta, &previous_pool_meta, &[idx]);
            }
            return Err(err);
        }
        if should_reload_pool_meta {
            {
                let mut pool_meta = self.pool_meta.write().await;
                pool_meta.mark_decommission_progress_saved();
            }
        }
        if let Some(canceler) = terminal_canceler.as_ref() {
            self.release_decommission_canceler_slot(idx, canceler).await;
        }

        if should_reload_pool_meta {
            self.ctx.advance_data_movement_operation_epoch();
        }
        drop(_movement_guard);

        if should_reload_pool_meta && let Some(notification_sys) = runtime_sources::notification_sys() {
            let stage = format!("complete_decommission for pool {idx}");
            if let Some(err) = observe_decommission_terminal_reload_result(
                resolve_decommission_pool_meta_reload_result(notification_sys.reload_pool_meta().await, stage.as_str()),
                stage.as_str(),
            ) {
                if let Err(record_err) = self
                    .record_decommission_terminal_reload_failure(idx, stage.as_str(), err.clone())
                    .await
                {
                    warn!(
                        event = EVENT_DECOMMISSION_STATE,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_POOLS,
                        pool_index = idx,
                        state = "terminal_reload_record_failed",
                        error = %record_err,
                        original_error = %err,
                        "Decommission terminal reload failure record failed"
                    );
                }
                warn!(
                    event = EVENT_DECOMMISSION_STATE,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_POOLS,
                    pool_index = idx,
                    state = "terminal_reload_failed",
                    error = %err,
                    "Decommission terminal state saved but pool meta reload failed"
                );
            }
        }

        if completed && let Err(err) = self.cleanup_decommission_durable_ilm_receipts(idx).await {
            warn!(
                event = EVENT_DECOMMISSION_STATE,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_POOLS,
                pool_index = idx,
                state = "receipt_cleanup_failed",
                error = %err,
                "Decommission durable ILM receipt cleanup failed"
            );
        }

        Ok(())
    }

    async fn decommission_pending_bucket(
        self: &Arc<Self>,
        rx: CancellationToken,
        idx: usize,
        pool: Arc<Sets>,
        bucket: DecomBucketInfo,
        entry_budget: Arc<Semaphore>,
        source_changed_exhaustions: Arc<AtomicUsize>,
    ) -> Result<()> {
        let is_decommissioned = {
            let pool_meta = self.pool_meta.read().await;
            resolve_decommission_bucket_state(&pool_meta, idx, &bucket)?
        };

        if is_decommissioned {
            warn!("decommission: already done, moving on {}", bucket.to_string());

            self.mark_decommission_bucket_done_and_save(idx, &bucket).await?;
            return Ok(());
        }

        warn!("decommission: currently on bucket {}", &bucket.name);

        if let Err(err) = self
            .decommission_pool(rx.clone(), idx, pool, bucket.clone(), entry_budget, source_changed_exhaustions)
            .await
        {
            error!("decommission: decommission_pool err {:?}", &err);
            return Err(err);
        } else {
            warn!("decommission: decommission_pool done {}", &bucket.name);
        }

        if let Err(err) = decommission_cancel_signal_result(rx.is_cancelled()) {
            warn!("decommission: cancellation observed after decommission_pool {}", &bucket.name);
            return Err(err);
        }

        self.mark_decommission_bucket_done_and_save(idx, &bucket).await?;

        warn!("decommission: decommission_pool bucket_done {}", &bucket.name);

        Ok(())
    }

    #[tracing::instrument(skip(self, rx))]
    async fn decommission_in_background(
        self: &Arc<Self>,
        rx: CancellationToken,
        idx: usize,
        entry_budget: Arc<Semaphore>,
    ) -> Result<()> {
        let pool = get_by_index(self.pools.as_slice(), idx, "load decommission background pool")?.clone();

        let pending = {
            let pool_meta = self.pool_meta.read().await;
            pool_meta.pending_buckets(idx)
        };
        let source_changed_exhaustions = Arc::new(AtomicUsize::new(0));
        let bucket_concurrency = decommission_bucket_concurrency_limit();
        let (regular_buckets, meta_buckets) = split_decommission_buckets(pending);
        let store = Arc::clone(self);
        run_decommission_phases(rx.clone(), regular_buckets, meta_buckets, bucket_concurrency, move |bucket, rx| {
            let store = Arc::clone(&store);
            let pool = pool.clone();
            let entry_budget = entry_budget.clone();
            let source_changed_exhaustions = Arc::clone(&source_changed_exhaustions);
            Box::pin(async move {
                store
                    .decommission_pending_bucket(rx, idx, pool, bucket, entry_budget, source_changed_exhaustions)
                    .await
            })
        })
        .await
    }

    #[tracing::instrument(skip(self))]
    pub async fn start_decommission(&self, indices: Vec<usize>) -> Result<()> {
        self.start_decommission_inner(indices, None).await.map(|_| ())
    }

    async fn start_decommission_with_routines(
        &self,
        indices: Vec<usize>,
        rx: &CancellationToken,
        local_indices: &[usize],
    ) -> Result<Vec<(usize, DecommissionCancelerGuard)>> {
        self.start_decommission_inner(indices, Some((rx, local_indices))).await
    }

    async fn start_decommission_inner(
        &self,
        indices: Vec<usize>,
        reservation: Option<(&CancellationToken, &[usize])>,
    ) -> Result<Vec<(usize, DecommissionCancelerGuard)>> {
        let indices = dedup_indices(&indices);
        validate_start_decommission_request(&indices, self.single_pool())?;

        self.ensure_decommission_rebalance_idle_after_refresh().await?;
        #[cfg(test)]
        let endpoints = self.instance_endpoints().unwrap_or_else(|| self.endpoints());
        #[cfg(not(test))]
        let endpoints = self.endpoints();
        ensure_decommission_start_local_leader(&endpoints, &indices)?;

        for idx in indices.iter().copied() {
            ensure_valid_decommission_pool_index(self.pools.len(), idx)?;
        }

        {
            let pool_meta = self.pool_meta.read().await;
            ensure_decommission_start_pool_states(&pool_meta, &indices)?;
        }

        let decom_buckets = self.get_buckets_to_decommission().await?;

        let mut healed_buckets = HashSet::with_capacity(decom_buckets.len());
        for bk in decom_buckets.iter() {
            if healed_buckets.insert(bk.name.as_str()) {
                resolve_decommission_preflight_heal_result(&bk.name, self.heal_bucket(&bk.name, &HealOpts::default()).await)?;
            }
        }

        let meta_bucket_opts = decommission_meta_bucket_options();
        for prefix in DECOMMISSION_META_PREFIXES {
            let bk = path_join(&[PathBuf::from(RUSTFS_META_BUCKET), PathBuf::from(prefix)]);
            if let Err(err) = self
                .make_bucket(bk.to_string_lossy().to_string().as_str(), &meta_bucket_opts)
                .await
                && !is_err_bucket_exists(&err)
            {
                error!("decommission: make bucket failed: {err}");
                return Err(err);
            }
        }

        let _start_guard = self.start_gate.lock().await;
        self.ensure_decommission_rebalance_idle_after_refresh_under_start_gate()
            .await?;

        let all_space_infos = self.get_decommission_all_pool_space_infos().await?;
        // Signal cancellation before waiting for the movement writer so active
        // object operations can observe the signal and release read guards.
        self.cancel_decommission_routines(&indices).await;
        let movement_gate = self.ctx.data_movement_operation_gate();
        let _movement_guard = movement_gate.write().await;

        let index_cancelers = if let Some((rx, local_indices)) = reservation {
            // Lock order matches terminal transitions: movement gate, then
            // decommission_cancelers, then pool_meta while start_gate excludes
            // another start.
            let mut cancelers = self.decommission_cancelers.write().await;
            let pool_meta = self.pool_meta.read().await;
            ensure_decommission_start_target_capacity(&pool_meta, &indices, &all_space_infos)?;
            reserve_decommission_start_cancelers(&pool_meta, &indices, local_indices, rx, cancelers.as_mut_slice())?
        } else {
            let pool_meta = self.pool_meta.read().await;
            ensure_decommission_start_pool_states(&pool_meta, &indices)?;
            ensure_decommission_start_target_capacity(&pool_meta, &indices, &all_space_infos)?;
            Vec::new()
        };

        let mut space_infos = Vec::with_capacity(indices.len());
        for (idx, pi) in all_space_infos.iter().copied() {
            if indices.contains(&idx) {
                space_infos.push((idx, pi));
            }
        }

        let previous_pool_meta = self
            .save_current_pool_meta_for_decommission_start(&indices, space_infos, decom_buckets)
            .await?;
        self.ctx.advance_data_movement_operation_epoch();
        // The local durable transition is now fenced. Release the writer
        // before any peer RPC; remote reload must not block scanner admission.
        drop(_movement_guard);

        if let Some(notification_sys) = runtime_sources::notification_sys()
            && let Err(err) = resolve_start_decommission_pool_meta_reload_result(notification_sys.reload_pool_meta().await)
        {
            warn!(
                event = EVENT_DECOMMISSION_STATE,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_POOLS,
                state = "start_failed",
                stage = "reload_pool_meta",
                error = %err,
                "Decommission start failed after pool metadata save"
            );

            let rollback_result = self
                .rollback_decommission_start_after_reload_failure(&movement_gate, &previous_pool_meta, &indices)
                .await;
            if let Err(rollback_save_err) = rollback_result {
                error!(
                    event = EVENT_DECOMMISSION_STATE,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_POOLS,
                    state = "rollback_failed",
                    stage = "save_pool_meta",
                    error = %rollback_save_err,
                    original_error = %err,
                    "Decommission rollback failed after pool metadata reload failure"
                );
                return Err(Error::other(format!(
                    "{err}; decommission start rollback save failed: {rollback_save_err}"
                )));
            }

            if let Err(rollback_reload_err) = resolve_decommission_pool_meta_reload_result(
                notification_sys.reload_pool_meta().await,
                "start_decommission_rollback",
            ) {
                error!(
                    event = EVENT_DECOMMISSION_STATE,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_POOLS,
                    state = "rollback_partial",
                    stage = "reload_pool_meta",
                    error = %rollback_reload_err,
                    original_error = %err,
                    "Decommission rollback metadata reload failed after local rollback save"
                );
                return Err(Error::other(format!(
                    "{err}; decommission start rollback saved locally but peer reload failed: {rollback_reload_err}"
                )));
            }

            warn!(
                event = EVENT_DECOMMISSION_STATE,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_POOLS,
                state = "rollback_success",
                original_error = %err,
                "Decommission start rolled back after pool metadata reload failure"
            );
            return Err(Error::other(format!("{err}; decommission start rollback succeeded")));
        }

        Ok(index_cancelers)
    }

    async fn get_buckets_to_decommission(&self) -> Result<Vec<DecomBucketInfo>> {
        let buckets = self.list_bucket(&BucketOptions::default()).await?;

        let mut ret: Vec<DecomBucketInfo> = buckets
            .iter()
            .map(|v| DecomBucketInfo {
                name: v.name.clone(),
                ..Default::default()
            })
            .collect();

        ret.extend(decommission_meta_buckets());

        Ok(ret)
    }

    async fn durable_ilm_receipt_run_token(&self, source_pool_idx: usize) -> Result<String> {
        let pool_meta = self.pool_meta.read().await;
        let pool = pool_meta
            .pools
            .get(source_pool_idx)
            .ok_or_else(|| invalid_decommission_pool_index_error(pool_meta.pools.len(), source_pool_idx))?;
        let start_time = pool
            .decommission
            .as_ref()
            .and_then(|info| info.start_time)
            .ok_or_else(|| Error::other(format!("decommission run identity is missing for pool {source_pool_idx}")))?;
        Ok(decommission_durable_ilm_receipt_run_token(&pool.cmd_line, start_time))
    }

    async fn durable_ilm_receipt_run_token_for_generation(
        &self,
        source_pool_idx: usize,
        generation: OffsetDateTime,
    ) -> Result<String> {
        let pool_meta = self.pool_meta.read().await;
        ensure_decommission_generation(&pool_meta, source_pool_idx, generation)?;
        let pool = pool_meta
            .pools
            .get(source_pool_idx)
            .ok_or_else(|| invalid_decommission_pool_index_error(pool_meta.pools.len(), source_pool_idx))?;
        Ok(decommission_durable_ilm_receipt_run_token(&pool.cmd_line, generation))
    }

    async fn load_decommissioned_durable_ilm_target(
        &self,
        source_pool_idx: usize,
        path: &str,
        max_record_size: usize,
        record_context: &str,
    ) -> Result<Option<(usize, Vec<u8>)>> {
        let mut target = None::<(usize, Vec<u8>)>;
        let mut first_read_error = None;
        for (target_pool_idx, pool) in self.pools.iter().enumerate() {
            if target_pool_idx == source_pool_idx {
                continue;
            }
            match read_config_limited_preserve_empty(pool.clone(), path, max_record_size).await {
                Ok(data) => {
                    if let Some((existing_pool_idx, existing)) = target.as_ref()
                        && existing != &data
                    {
                        return Err(Error::other(format!(
                            "divergent target durable ILM records at path `{path}` {record_context} in pools {existing_pool_idx} and {target_pool_idx}"
                        )));
                    }
                    target = Some((target_pool_idx, data));
                }
                Err(err)
                    if matches!(&err, Error::ConfigNotFound | Error::FileNotFound | Error::FileVersionNotFound)
                        || is_err_object_not_found(&err)
                        || is_err_version_not_found(&err) => {}
                Err(err) => {
                    first_read_error.get_or_insert_with(|| {
                        Error::other(format!(
                            "failed to read target durable ILM record at path `{path}` {record_context} from pool {target_pool_idx}: {err}"
                        ))
                    });
                }
            }
        }

        if let Some(err) = first_read_error {
            return Err(err);
        }
        Ok(target)
    }

    async fn list_decommission_durable_ilm_receipt_paths_in_pool(&self, pool_idx: usize, prefix: &str) -> Result<Vec<String>> {
        let pool = self
            .pools
            .get(pool_idx)
            .ok_or_else(|| invalid_decommission_pool_index_error(self.pools.len(), pool_idx))?;
        let mut receipts = Vec::new();
        let mut continuation = None;
        loop {
            let page = pool
                .clone()
                .list_objects_v2(RUSTFS_META_BUCKET, prefix, continuation, None, 1000, false, None, false)
                .await
                .map_err(|err| {
                    Error::other(format!(
                        "failed to list durable ILM decommission receipts under `{prefix}` in pool {pool_idx}: {err}"
                    ))
                })?;
            receipts.extend(page.objects.into_iter().map(|object| object.name));
            if !page.is_truncated {
                break;
            }
            continuation = Some(page.next_continuation_token.ok_or_else(|| {
                Error::other(format!(
                    "durable ILM decommission receipt listing under `{prefix}` in pool {pool_idx} was truncated without a continuation token"
                ))
            })?);
        }
        Ok(receipts)
    }

    async fn list_decommission_durable_ilm_receipts(&self, source_pool_idx: usize) -> Result<Vec<(usize, String)>> {
        let run_token = self.durable_ilm_receipt_run_token(source_pool_idx).await?;
        self.list_decommission_durable_ilm_receipts_for_run(source_pool_idx, &run_token)
            .await
    }

    async fn list_decommission_durable_ilm_receipts_for_run(
        &self,
        source_pool_idx: usize,
        run_token: &str,
    ) -> Result<Vec<(usize, String)>> {
        let prefix = decommission_durable_ilm_receipt_run_prefix(run_token);
        let mut receipts = Vec::new();
        for pool_idx in 0..self.pools.len() {
            if pool_idx == source_pool_idx {
                continue;
            }
            for receipt_path in self
                .list_decommission_durable_ilm_receipt_paths_in_pool(pool_idx, &prefix)
                .await?
            {
                let locator = parse_decommission_durable_ilm_receipt_path(&receipt_path)?;
                if locator.run_token != run_token {
                    return Err(Error::other(format!(
                        "durable ILM receipt path `{receipt_path}` has an unexpected run token"
                    )));
                }
                receipts.push((pool_idx, receipt_path));
            }
        }
        Ok(receipts)
    }

    async fn list_decommission_durable_ilm_manifest_receipts(&self, source_pool_idx: usize) -> Result<Vec<String>> {
        let run_token = self.durable_ilm_receipt_run_token(source_pool_idx).await?;
        self.list_decommission_durable_ilm_manifest_receipts_for_run(source_pool_idx, &run_token)
            .await
    }

    async fn list_decommission_durable_ilm_manifest_receipts_for_run(
        &self,
        source_pool_idx: usize,
        run_token: &str,
    ) -> Result<Vec<String>> {
        let prefix = decommission_durable_ilm_receipt_run_prefix(run_token);
        let receipt_paths = self
            .list_decommission_durable_ilm_receipt_paths_in_pool(source_pool_idx, &prefix)
            .await?;
        for receipt_path in &receipt_paths {
            let locator = parse_decommission_durable_ilm_receipt_path(receipt_path)?;
            if locator.run_token != run_token {
                return Err(Error::other(format!(
                    "durable ILM expected manifest receipt path `{receipt_path}` has an unexpected run token"
                )));
            }
        }
        Ok(receipt_paths)
    }

    #[cfg(all(test, feature = "test-util"))]
    async fn persist_decommission_durable_ilm_manifest(&self, source_pool_idx: usize) -> Result<()> {
        let run_token = self.durable_ilm_receipt_run_token(source_pool_idx).await?;
        self.persist_decommission_durable_ilm_manifest_for_run(source_pool_idx, &run_token)
            .await
    }

    async fn persist_decommission_durable_ilm_manifest_for_run(&self, source_pool_idx: usize, run_token: &str) -> Result<()> {
        let receipt_paths = self
            .list_decommission_durable_ilm_manifest_receipts_for_run(source_pool_idx, run_token)
            .await?;
        for receipt_path in &receipt_paths {
            self.read_decommission_durable_ilm_receipt(source_pool_idx, receipt_path)
                .await?;
        }
        let manifest = DecommissionDurableIlmManifest::new(run_token, &receipt_paths)?;
        let manifest_path = decommission_durable_ilm_manifest_path(run_token);
        let encoded = manifest.encode()?;
        let mut attempt = 1;
        loop {
            match read_config_limited_preserve_empty(
                self.pools[source_pool_idx].clone(),
                &manifest_path,
                DECOMMISSION_DURABLE_ILM_MANIFEST_MAX_SIZE,
            )
            .await
            {
                Ok(existing) => {
                    DecommissionDurableIlmManifest::decode(&existing, run_token, &receipt_paths).map_err(|err| {
                        Error::other(format!(
                            "durable ILM expected manifest `{manifest_path}` in source pool {source_pool_idx} is invalid: {err}"
                        ))
                    })?;
                    return Ok(());
                }
                Err(err)
                    if matches!(&err, Error::ConfigNotFound | Error::FileNotFound | Error::FileVersionNotFound)
                        || is_err_object_not_found(&err)
                        || is_err_version_not_found(&err) => {}
                Err(err) => {
                    return Err(Error::other(format!(
                        "failed to read durable ILM expected manifest `{manifest_path}` from source pool {source_pool_idx}: {err}"
                    )));
                }
            }
            match save_config_with_opts(
                self.pools[source_pool_idx].clone(),
                &manifest_path,
                encoded.clone(),
                &ObjectOptions {
                    max_parity: true,
                    http_preconditions: Some(HTTPPreconditions {
                        if_none_match: Some("*".to_string()),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            )
            .await
            {
                Ok(()) => return Ok(()),
                Err(Error::PreconditionFailed) if attempt < DECOMMISSION_DURABLE_ILM_RECEIPT_CAS_ATTEMPTS => {
                    attempt += 1;
                }
                Err(Error::PreconditionFailed) => {
                    return Err(Error::other(format!(
                        "failed to persist durable ILM expected manifest `{manifest_path}` after concurrent updates"
                    )));
                }
                Err(err) => {
                    return Err(Error::other(format!(
                        "failed to persist durable ILM expected manifest `{manifest_path}` in source pool {source_pool_idx}: {err}"
                    )));
                }
            }
        }
    }

    async fn load_decommission_durable_ilm_manifest_for_run(
        &self,
        source_pool_idx: usize,
        run_token: &str,
    ) -> Result<HashMap<String, DecommissionDurableIlmReceipt>> {
        let receipt_paths = self
            .list_decommission_durable_ilm_manifest_receipts_for_run(source_pool_idx, run_token)
            .await?;
        let manifest_path = decommission_durable_ilm_manifest_path(run_token);
        let data = read_config_limited_preserve_empty(
            self.pools[source_pool_idx].clone(),
            &manifest_path,
            DECOMMISSION_DURABLE_ILM_MANIFEST_MAX_SIZE,
        )
        .await
        .map_err(|err| {
            Error::other(format!(
                "failed to read durable ILM expected manifest `{manifest_path}` from source pool {source_pool_idx}: {err}"
            ))
        })?;
        DecommissionDurableIlmManifest::decode(&data, run_token, &receipt_paths).map_err(|err| {
            Error::other(format!(
                "durable ILM expected manifest `{manifest_path}` in source pool {source_pool_idx} is invalid: {err}"
            ))
        })?;

        let mut receipts = HashMap::with_capacity(receipt_paths.len());
        for receipt_path in receipt_paths {
            let receipt = self
                .read_decommission_durable_ilm_receipt(source_pool_idx, &receipt_path)
                .await?;
            if receipts.insert(receipt_path.clone(), receipt).is_some() {
                return Err(Error::other(format!(
                    "durable ILM expected manifest contains duplicate receipt path `{receipt_path}`"
                )));
            }
        }
        Ok(receipts)
    }

    #[cfg(all(test, feature = "test-util"))]
    async fn persist_decommission_durable_ilm_receipt(
        &self,
        source_pool_idx: usize,
        target_pool_idx: usize,
        receipt: &DecommissionDurableIlmReceipt,
    ) -> Result<()> {
        let run_token = self.durable_ilm_receipt_run_token(source_pool_idx).await?;
        self.persist_decommission_durable_ilm_receipt_for_run(target_pool_idx, receipt, &run_token)
            .await
    }

    async fn persist_decommission_durable_ilm_receipt_for_run(
        &self,
        target_pool_idx: usize,
        receipt: &DecommissionDurableIlmReceipt,
        run_token: &str,
    ) -> Result<()> {
        let receipt_path = decommission_durable_ilm_receipt_path(run_token, &receipt.source_path, &receipt.id_kind, &receipt.id);
        let locator = parse_decommission_durable_ilm_receipt_path(&receipt_path)?;
        let mut attempt = 1;
        loop {
            let (merged, http_preconditions) = match read_config_limited_preserve_empty_with_metadata(
                self.pools[target_pool_idx].clone(),
                &receipt_path,
                DECOMMISSION_DURABLE_ILM_RECEIPT_MAX_SIZE,
            )
            .await
            {
                Ok((existing_data, metadata)) => {
                    let existing = DecommissionDurableIlmReceipt::decode(&existing_data).map_err(|err| {
                        Error::other(format!(
                            "durable ILM decommission receipt `{receipt_path}` in pool {target_pool_idx} for {} is invalid: {err}",
                            locator.context()
                        ))
                    })?;
                    Self::validate_decommission_durable_ilm_receipt_locator(&receipt_path, &locator, &existing)?;
                    let merged = merge_decommission_durable_ilm_receipts(&existing, receipt)?;
                    if merged == existing {
                        return Ok(());
                    }
                    let etag = metadata.etag.filter(|etag| !etag.trim().is_empty()).ok_or_else(|| {
                        Error::other(format!(
                            "durable ILM decommission receipt `{receipt_path}` in pool {target_pool_idx} is missing an ETag"
                        ))
                    })?;
                    (
                        merged,
                        HTTPPreconditions {
                            if_match: Some(etag),
                            ..Default::default()
                        },
                    )
                }
                Err(err)
                    if matches!(&err, Error::ConfigNotFound | Error::FileNotFound | Error::FileVersionNotFound)
                        || is_err_object_not_found(&err)
                        || is_err_version_not_found(&err) =>
                {
                    (
                        receipt.clone(),
                        HTTPPreconditions {
                            if_none_match: Some("*".to_string()),
                            ..Default::default()
                        },
                    )
                }
                Err(err) => {
                    return Err(Error::other(format!(
                        "failed to read durable ILM decommission receipt `{receipt_path}` from pool {target_pool_idx} for {}: {err}",
                        locator.context()
                    )));
                }
            };
            let encoded = merged.encode().map_err(|err| {
                Error::other(format!(
                    "failed to encode durable ILM decommission receipt `{receipt_path}` for source path `{}` {}: {err}",
                    receipt.source_path,
                    receipt.context()
                ))
            })?;
            match save_config_with_opts(
                self.pools[target_pool_idx].clone(),
                &receipt_path,
                encoded,
                &ObjectOptions {
                    max_parity: true,
                    http_preconditions: Some(http_preconditions),
                    ..Default::default()
                },
            )
            .await
            {
                Ok(()) => return Ok(()),
                Err(Error::PreconditionFailed) if attempt < DECOMMISSION_DURABLE_ILM_RECEIPT_CAS_ATTEMPTS => {
                    attempt += 1;
                }
                Err(Error::PreconditionFailed) => {
                    return Err(Error::other(format!(
                        "failed to persist durable ILM decommission receipt `{receipt_path}` for {} after concurrent updates",
                        locator.context()
                    )));
                }
                Err(err) => {
                    return Err(Error::other(format!(
                        "failed to persist durable ILM decommission receipt `{receipt_path}` for {}: {err}",
                        locator.context()
                    )));
                }
            }
        }
    }

    fn validate_decommission_durable_ilm_receipt_locator(
        receipt_path: &str,
        locator: &DecommissionDurableIlmReceiptLocator,
        receipt: &DecommissionDurableIlmReceipt,
    ) -> Result<()> {
        if locator.source_path != receipt.source_path || locator.id_kind != receipt.id_kind || locator.id != receipt.id {
            return Err(Error::other(format!(
                "durable ILM decommission receipt path `{receipt_path}` identity {} does not match receipt {}",
                locator.context(),
                receipt.context()
            )));
        }
        Ok(())
    }

    async fn read_decommission_durable_ilm_receipt(
        &self,
        receipt_pool_idx: usize,
        receipt_path: &str,
    ) -> Result<DecommissionDurableIlmReceipt> {
        let locator = parse_decommission_durable_ilm_receipt_path(receipt_path)?;
        let data = read_config_limited_preserve_empty(
            self.pools[receipt_pool_idx].clone(),
            receipt_path,
            DECOMMISSION_DURABLE_ILM_RECEIPT_MAX_SIZE,
        )
        .await
        .map_err(|err| {
            Error::other(format!(
                "failed to read durable ILM decommission receipt `{receipt_path}` from pool {receipt_pool_idx} for {}: {err}",
                locator.context()
            ))
        })?;
        let receipt = DecommissionDurableIlmReceipt::decode(&data).map_err(|err| {
            Error::other(format!(
                "durable ILM decommission receipt `{receipt_path}` in pool {receipt_pool_idx} for {} is invalid: {err}",
                locator.context()
            ))
        })?;
        Self::validate_decommission_durable_ilm_receipt_locator(receipt_path, &locator, &receipt)?;
        Ok(receipt)
    }

    async fn load_decommission_durable_ilm_terminal_receipt_for_run(
        &self,
        source_pool_idx: usize,
        path: &str,
        source_record: &ValidatedDurableIlmRecord,
        run_token: &str,
    ) -> Result<Option<DecommissionDurableIlmReceipt>> {
        let receipt_path = decommission_durable_ilm_receipt_path(run_token, path, source_record.id_kind, &source_record.id);
        let locator = parse_decommission_durable_ilm_receipt_path(&receipt_path)?;
        let mut proof = None::<DecommissionDurableIlmReceipt>;
        for pool_idx in 0..self.pools.len() {
            if pool_idx == source_pool_idx {
                continue;
            }
            let data = match read_config_limited_preserve_empty(
                self.pools[pool_idx].clone(),
                &receipt_path,
                DECOMMISSION_DURABLE_ILM_RECEIPT_MAX_SIZE,
            )
            .await
            {
                Ok(data) => data,
                Err(err)
                    if matches!(&err, Error::ConfigNotFound | Error::FileNotFound | Error::FileVersionNotFound)
                        || is_err_object_not_found(&err)
                        || is_err_version_not_found(&err) =>
                {
                    continue;
                }
                Err(err) => {
                    return Err(Error::other(format!(
                        "failed to read terminal durable ILM decommission receipt `{receipt_path}` from pool {pool_idx} for {}: {err}",
                        source_record.context()
                    )));
                }
            };
            let receipt = DecommissionDurableIlmReceipt::decode(&data).map_err(|err| {
                Error::other(format!(
                    "terminal durable ILM decommission receipt `{receipt_path}` in pool {pool_idx} for {} is invalid: {err}",
                    source_record.context()
                ))
            })?;
            Self::validate_decommission_durable_ilm_receipt_locator(&receipt_path, &locator, &receipt)?;
            if receipt.namespace != source_record.namespace
                || receipt.id_kind != source_record.id_kind
                || receipt.id != source_record.id
            {
                return Err(Error::other(format!(
                    "terminal durable ILM decommission receipt identity mismatch at path `{path}` {}; receipt {}",
                    source_record.context(),
                    receipt.context()
                )));
            }
            source_record
                .checkpoint
                .validate_successor(&receipt.checkpoint)
                .map_err(|err| {
                    Error::other(format!(
                        "terminal durable ILM decommission receipt does not cover source at path `{path}` {}: {err}",
                        source_record.context()
                    ))
                })?;
            if receipt.terminal_checkpoint.is_some() {
                proof = Some(match proof {
                    Some(existing) => merge_decommission_durable_ilm_receipts(&existing, &receipt)?,
                    None => receipt,
                });
            }
        }
        Ok(proof)
    }

    async fn verify_decommission_durable_ilm_receipts(&self, source_pool_idx: usize) -> Result<()> {
        let run_token = self.durable_ilm_receipt_run_token(source_pool_idx).await?;
        self.verify_decommission_durable_ilm_receipts_for_run(source_pool_idx, &run_token)
            .await
    }

    async fn verify_decommission_durable_ilm_receipts_for_run(&self, source_pool_idx: usize, run_token: &str) -> Result<()> {
        let expected_receipts = self
            .load_decommission_durable_ilm_manifest_for_run(source_pool_idx, run_token)
            .await?;
        let receipt_paths = self
            .list_decommission_durable_ilm_receipts_for_run(source_pool_idx, run_token)
            .await?;
        let present_receipt_paths = receipt_paths
            .iter()
            .map(|(_, receipt_path)| receipt_path.as_str())
            .collect::<HashSet<_>>();
        for (expected_path, expected) in &expected_receipts {
            if !present_receipt_paths.contains(expected_path.as_str()) {
                return Err(Error::other(format!(
                    "durable ILM decommission receipt is missing at `{expected_path}` for source path `{}` {}",
                    expected.source_path,
                    expected.context()
                )));
            }
        }

        for (receipt_pool_idx, receipt_path) in receipt_paths {
            let expected = expected_receipts.get(&receipt_path).ok_or_else(|| {
                Error::other(format!(
                    "durable ILM decommission receipt `{receipt_path}` in pool {receipt_pool_idx} is absent from the expected manifest"
                ))
            })?;
            let receipt = self
                .read_decommission_durable_ilm_receipt(receipt_pool_idx, &receipt_path)
                .await?;
            if receipt.source_path != expected.source_path
                || receipt.namespace != expected.namespace
                || receipt.id_kind != expected.id_kind
                || receipt.id != expected.id
            {
                return Err(Error::other(format!(
                    "durable ILM decommission receipt identity mismatch at `{receipt_path}` for source path `{}` {}; decoded {}",
                    expected.source_path,
                    expected.context(),
                    receipt.context()
                )));
            }
            expected.checkpoint.validate_successor(&receipt.checkpoint).map_err(|err| {
                Error::other(format!(
                    "durable ILM decommission receipt generation mismatch at `{receipt_path}` for source path `{}` {}: {err}",
                    expected.source_path,
                    expected.context()
                ))
            })?;
            match (&expected.terminal_checkpoint, &receipt.terminal_checkpoint) {
                (Some(expected_terminal), Some(receipt_terminal)) => {
                    expected_terminal.validate_successor(receipt_terminal).map_err(|err| {
                        Error::other(format!(
                            "durable ILM decommission terminal receipt generation mismatch at `{receipt_path}` for source path `{}` {}: {err}",
                            expected.source_path,
                            expected.context()
                        ))
                    })?;
                }
                (Some(_), None) => {
                    return Err(Error::other(format!(
                        "durable ILM decommission terminal receipt is missing at `{receipt_path}` for source path `{}` {}",
                        expected.source_path,
                        expected.context()
                    )));
                }
                (None, _) => {}
            }
            let namespace = classify_durable_ilm_record(&receipt.source_path)?
                .ok_or_else(|| Error::other(format!("path `{}` is not a durable ILM record", receipt.source_path)))?;
            let target = self
                .load_decommissioned_durable_ilm_target(
                    source_pool_idx,
                    &receipt.source_path,
                    namespace.max_record_size,
                    &receipt.context(),
                )
                .await?;
            if let Some((_, target)) = target {
                let target_record = validate_durable_ilm_record(&receipt.source_path, &target).map_err(|err| {
                    Error::other(format!(
                        "target durable ILM record is invalid at path `{}` {}: {err}",
                        receipt.source_path,
                        receipt.context()
                    ))
                })?;
                let identity_matches = target_record.namespace == receipt.namespace
                    && target_record.id_kind == receipt.id_kind
                    && target_record.id == receipt.id;
                let reused_manual_scope = receipt.terminal_checkpoint.is_some()
                    && matches!(
                        (&receipt.checkpoint, &target_record.checkpoint),
                        (
                            DurableIlmRecordCheckpoint::ManualTransitionScope { .. },
                            DurableIlmRecordCheckpoint::ManualTransitionScope { .. }
                        )
                    );
                if !identity_matches && !reused_manual_scope {
                    return Err(Error::other(format!(
                        "target durable ILM record identity mismatch at path `{}` {}; decoded {}",
                        receipt.source_path,
                        receipt.context(),
                        target_record.context()
                    )));
                }
                if identity_matches {
                    receipt
                        .terminal_checkpoint
                        .as_ref()
                        .unwrap_or(&receipt.checkpoint)
                        .validate_successor(&target_record.checkpoint)
                        .map_err(|err| {
                            Error::other(format!(
                                "target durable ILM record generation mismatch at path `{}` {}: {err}",
                                receipt.source_path,
                                receipt.context()
                            ))
                        })?;
                }
            } else if receipt.terminal_checkpoint.is_none() {
                return Err(Error::other(format!(
                    "target durable ILM record is missing at path `{}` {} without a recovery terminal checkpoint",
                    receipt.source_path,
                    receipt.context()
                )));
            }
        }
        Ok(())
    }

    async fn advance_durable_ilm_decommission_receipt(
        &self,
        pool_idx: usize,
        receipt_path: &str,
        record: &ValidatedDurableIlmRecord,
        terminal: bool,
    ) -> Result<bool> {
        let stage = if terminal { "terminal" } else { "progress" };
        let locator = parse_decommission_durable_ilm_receipt_path(receipt_path)?;
        let mut attempt = 1;
        loop {
            let (receipt_data, metadata) = match read_config_limited_preserve_empty_with_metadata(
                self.pools[pool_idx].clone(),
                receipt_path,
                DECOMMISSION_DURABLE_ILM_RECEIPT_MAX_SIZE,
            )
            .await
            {
                Ok(receipt) => receipt,
                Err(err)
                    if matches!(&err, Error::ConfigNotFound | Error::FileNotFound | Error::FileVersionNotFound)
                        || is_err_object_not_found(&err)
                        || is_err_version_not_found(&err) =>
                {
                    return Ok(false);
                }
                Err(err) => {
                    return Err(Error::other(format!(
                        "failed to read durable ILM decommission receipt `{receipt_path}` from pool {pool_idx} for {}: {err}",
                        locator.context()
                    )));
                }
            };
            let mut receipt = DecommissionDurableIlmReceipt::decode(&receipt_data).map_err(|err| {
                Error::other(format!(
                    "durable ILM decommission receipt `{receipt_path}` in pool {pool_idx} for {} is invalid: {err}",
                    locator.context()
                ))
            })?;
            Self::validate_decommission_durable_ilm_receipt_locator(receipt_path, &locator, &receipt)?;
            receipt.checkpoint.validate_successor(&record.checkpoint).map_err(|err| {
                Error::other(format!(
                    "{stage} durable ILM record generation mismatch at path `{}` {}: {err}",
                    receipt.source_path,
                    receipt.context()
                ))
            })?;

            if terminal {
                if let Some(existing) = &receipt.terminal_checkpoint {
                    if existing == &record.checkpoint || record.checkpoint.validate_successor(existing).is_ok() {
                        return Ok(true);
                    }
                    existing.validate_successor(&record.checkpoint).map_err(|err| {
                        Error::other(format!(
                            "terminal durable ILM record checkpoint conflicts at path `{}` {}: {err}",
                            receipt.source_path,
                            receipt.context()
                        ))
                    })?;
                }
                receipt.terminal_checkpoint = Some(record.checkpoint.clone());
            } else {
                if let Some(existing) = &receipt.terminal_checkpoint {
                    if existing == &record.checkpoint {
                        return Ok(true);
                    }
                    existing.validate_successor(&record.checkpoint).map_err(|err| {
                        Error::other(format!(
                            "progress durable ILM record conflicts with terminal checkpoint at path `{}` {}: {err}",
                            receipt.source_path,
                            receipt.context()
                        ))
                    })?;
                    receipt.terminal_checkpoint = None;
                }
                if receipt.checkpoint == record.checkpoint {
                    return Ok(true);
                }
                receipt.checkpoint = record.checkpoint.clone();
            }

            let etag = metadata.etag.filter(|etag| !etag.trim().is_empty()).ok_or_else(|| {
                Error::other(format!(
                    "durable ILM decommission receipt `{receipt_path}` in pool {pool_idx} is missing an ETag"
                ))
            })?;
            let encoded = receipt.encode()?;
            match save_config_with_opts(
                self.pools[pool_idx].clone(),
                receipt_path,
                encoded,
                &ObjectOptions {
                    max_parity: true,
                    http_preconditions: Some(HTTPPreconditions {
                        if_match: Some(etag),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            )
            .await
            {
                Ok(()) => return Ok(true),
                Err(Error::PreconditionFailed) if attempt < DECOMMISSION_DURABLE_ILM_RECEIPT_CAS_ATTEMPTS => {
                    attempt += 1;
                    continue;
                }
                Err(Error::PreconditionFailed) => {
                    return Err(Error::other(format!(
                        "failed to persist {stage} durable ILM decommission receipt `{receipt_path}` for {} after concurrent updates",
                        locator.context()
                    )));
                }
                Err(err) => {
                    return Err(Error::other(format!(
                        "failed to persist {stage} durable ILM decommission receipt `{receipt_path}` for {}: {err}",
                        locator.context()
                    )));
                }
            }
        }
    }

    async fn advance_durable_ilm_decommission_receipts(
        &self,
        path: &str,
        data: &[u8],
        terminal: bool,
    ) -> Result<Option<Vec<usize>>> {
        let active_runs = {
            let pool_meta = self.pool_meta.read().await;
            pool_meta
                .pools
                .iter()
                .enumerate()
                .filter_map(|(pool_idx, pool)| {
                    pool.decommission
                        .as_ref()
                        .filter(|info| info.has_decommission_state() && !info.complete)
                        .and_then(|info| info.start_time)
                        .map(|start_time| (pool_idx, decommission_durable_ilm_receipt_run_token(&pool.cmd_line, start_time)))
                })
                .collect::<Vec<_>>()
        };
        if active_runs.is_empty() {
            return Ok(None);
        }

        let stage = if terminal { "terminal" } else { "progress" };
        let record = validate_durable_ilm_record(path, data)
            .map_err(|err| Error::other(format!("{stage} durable ILM record is invalid at path `{path}`: {err}")))?;
        let active_source_pool_indices = active_runs.iter().map(|(pool_idx, _)| *pool_idx).collect::<Vec<_>>();
        let mut terminal_target_pool_indices = Vec::new();
        for (source_pool_idx, run_token) in active_runs {
            let receipt_path = decommission_durable_ilm_receipt_path(&run_token, path, record.id_kind, &record.id);
            let mut receipt_found = false;
            for pool_idx in 0..self.pools.len() {
                if pool_idx != source_pool_idx {
                    let found = self
                        .advance_durable_ilm_decommission_receipt(pool_idx, &receipt_path, &record, terminal)
                        .await?;
                    receipt_found |= found;
                    if terminal
                        && found
                        && !active_source_pool_indices.contains(&pool_idx)
                        && !terminal_target_pool_indices.contains(&pool_idx)
                    {
                        terminal_target_pool_indices.push(pool_idx);
                    }
                }
            }
            if terminal && !receipt_found {
                return Err(Error::other(format!(
                    "terminal durable ILM record at path `{path}` {} is retained until its decommission receipt is committed",
                    record.context()
                )));
            }
        }
        Ok(Some(terminal_target_pool_indices))
    }

    pub(crate) async fn record_durable_ilm_decommission_progress(&self, path: &str, data: &[u8]) -> Result<()> {
        self.advance_durable_ilm_decommission_receipts(path, data, false)
            .await
            .map(|_| ())
    }

    pub(crate) async fn record_durable_ilm_decommission_terminal(&self, path: &str, data: &[u8]) -> Result<()> {
        self.record_durable_ilm_decommission_terminal_target_pools(path, data)
            .await
            .map(|_| ())
    }

    /// Record terminal proof and return its non-source receipt pools for targeted cleanup.
    pub(crate) async fn record_durable_ilm_decommission_terminal_target_pools(
        &self,
        path: &str,
        data: &[u8],
    ) -> Result<Option<Vec<usize>>> {
        self.advance_durable_ilm_decommission_receipts(path, data, true).await
    }

    async fn cleanup_decommission_durable_ilm_receipts(&self, source_pool_idx: usize) -> Result<()> {
        for (pool_idx, receipt_path) in self.list_decommission_durable_ilm_receipts(source_pool_idx).await? {
            match delete_config(self.pools[pool_idx].clone(), &receipt_path).await {
                Ok(()) | Err(Error::ConfigNotFound | Error::FileNotFound | Error::FileVersionNotFound) => {}
                Err(err) if is_err_object_not_found(&err) || is_err_version_not_found(&err) => {}
                Err(err) => {
                    return Err(Error::other(format!(
                        "failed to clean durable ILM decommission receipt `{receipt_path}` from pool {pool_idx}: {err}"
                    )));
                }
            }
        }
        for receipt_path in self.list_decommission_durable_ilm_manifest_receipts(source_pool_idx).await? {
            match delete_config(self.pools[source_pool_idx].clone(), &receipt_path).await {
                Ok(()) | Err(Error::ConfigNotFound | Error::FileNotFound | Error::FileVersionNotFound) => {}
                Err(err) if is_err_object_not_found(&err) || is_err_version_not_found(&err) => {}
                Err(err) => {
                    return Err(Error::other(format!(
                        "failed to clean durable ILM expected manifest receipt `{receipt_path}` from source pool {source_pool_idx}: {err}"
                    )));
                }
            }
        }
        let run_token = self.durable_ilm_receipt_run_token(source_pool_idx).await?;
        let manifest_path = decommission_durable_ilm_manifest_path(&run_token);
        match delete_config(self.pools[source_pool_idx].clone(), &manifest_path).await {
            Ok(()) | Err(Error::ConfigNotFound | Error::FileNotFound | Error::FileVersionNotFound) => {}
            Err(err) if is_err_object_not_found(&err) || is_err_version_not_found(&err) => {}
            Err(err) => {
                return Err(Error::other(format!(
                    "failed to clean durable ILM expected manifest `{manifest_path}` from source pool {source_pool_idx}: {err}"
                )));
            }
        }
        Ok(())
    }

    #[cfg(all(test, feature = "test-util"))]
    async fn verify_and_cleanup_decommissioned_durable_ilm_record(
        &self,
        source_pool_idx: usize,
        source_set: Arc<SetDisks>,
        path: &str,
    ) -> Result<()> {
        let run_token = self.durable_ilm_receipt_run_token(source_pool_idx).await?;
        self.verify_and_cleanup_decommissioned_durable_ilm_record_for_run(source_pool_idx, source_set, path, &run_token)
            .await
    }

    async fn verify_and_cleanup_decommissioned_durable_ilm_record_for_run(
        &self,
        source_pool_idx: usize,
        source_set: Arc<SetDisks>,
        path: &str,
        run_token: &str,
    ) -> Result<()> {
        let namespace = classify_durable_ilm_record(path)?
            .ok_or_else(|| Error::other(format!("path `{path}` is not a durable ILM record")))?;
        let source_versions = source_set
            .load_file_info_versions_exact(RUSTFS_META_BUCKET, path)
            .await
            .map_err(|err| Error::other(format!("failed to load source durable ILM versions at path `{path}`: {err}")))?
            .ok_or_else(|| Error::other(format!("source durable ILM record is missing at path `{path}`")))?;
        let source = read_config_limited_preserve_empty(source_set.clone(), path, namespace.max_record_size)
            .await
            .map_err(|err| Error::other(format!("failed to read source durable ILM record at path `{path}`: {err}")))?;
        let source_record = validate_durable_ilm_record(path, &source)
            .map_err(|err| Error::other(format!("source durable ILM record is invalid at path `{path}`: {err}")))?;
        let target = self
            .load_decommissioned_durable_ilm_target(source_pool_idx, path, namespace.max_record_size, &source_record.context())
            .await?;
        let manifest_receipt = if let Some((target_pool_idx, target)) = target {
            let target_record = validate_decommission_durable_ilm_copy(path, &source_record, &target)?;
            let receipt = DecommissionDurableIlmReceipt::new(path, &target_record);
            self.persist_decommission_durable_ilm_receipt_for_run(target_pool_idx, &receipt, run_token)
                .await?;
            receipt
        } else {
            self.load_decommission_durable_ilm_terminal_receipt_for_run(source_pool_idx, path, &source_record, run_token)
                .await?
                .ok_or_else(|| {
                    Error::other(format!(
                        "target durable ILM record is missing at path `{path}` {} without a matching terminal receipt",
                        source_record.context()
                    ))
                })?
        };
        self.persist_decommission_durable_ilm_receipt_for_run(source_pool_idx, &manifest_receipt, run_token)
            .await?;

        let cleanup_result = data_movement::cleanup_source_entry_if_unchanged(
            source_set,
            RUSTFS_META_BUCKET,
            path,
            &source_versions,
            &[],
            data_movement::SourceCleanupBucketFence::default(),
            "decommission durable ILM final sweep",
        )
        .await
        .map_err(|err| {
            Error::other(format!(
                "source durable ILM cleanup failed at path `{path}` {}: {err}",
                source_record.context()
            ))
        });
        resolve_decommission_entry_cleanup_delete_result(cleanup_result, RUSTFS_META_BUCKET, path)
    }

    #[cfg(all(test, feature = "test-util"))]
    pub(crate) async fn verify_and_cleanup_decommissioned_durable_ilm_record_for_test(
        &self,
        source_pool_idx: usize,
        source_set: Arc<SetDisks>,
        path: &str,
    ) -> Result<()> {
        self.verify_and_cleanup_decommissioned_durable_ilm_record(source_pool_idx, source_set, path)
            .await
    }

    #[cfg(all(test, feature = "test-util"))]
    pub(crate) async fn decommission_durable_ilm_receipt_count_for_test(&self, source_pool_idx: usize) -> Result<usize> {
        Ok(self.list_decommission_durable_ilm_receipts(source_pool_idx).await?.len())
    }

    #[cfg(all(test, feature = "test-util"))]
    pub(crate) async fn decommission_durable_ilm_receipt_paths_for_test(
        &self,
        source_pool_idx: usize,
    ) -> Result<Vec<(usize, String)>> {
        self.list_decommission_durable_ilm_receipts(source_pool_idx).await
    }

    #[cfg(all(test, feature = "test-util"))]
    pub(crate) async fn persist_decommission_durable_ilm_receipt_for_test(
        &self,
        source_pool_idx: usize,
        target_pool_idx: usize,
        source_path: &str,
        record: &ValidatedDurableIlmRecord,
        terminal: bool,
    ) -> Result<String> {
        let mut receipt = DecommissionDurableIlmReceipt::new(source_path, record);
        if terminal {
            receipt.terminal_checkpoint = Some(record.checkpoint.clone());
        }
        self.persist_decommission_durable_ilm_receipt(source_pool_idx, target_pool_idx, &receipt)
            .await?;
        let run_token = self.durable_ilm_receipt_run_token(source_pool_idx).await?;
        Ok(decommission_durable_ilm_receipt_path(&run_token, source_path, record.id_kind, &record.id))
    }

    #[cfg(all(test, feature = "test-util"))]
    pub(crate) async fn persist_decommission_durable_ilm_manifest_for_test(&self, source_pool_idx: usize) -> Result<()> {
        self.persist_decommission_durable_ilm_manifest(source_pool_idx).await
    }

    #[cfg(all(test, feature = "test-util"))]
    pub(crate) async fn cleanup_decommission_durable_ilm_receipts_for_test(&self, source_pool_idx: usize) -> Result<()> {
        self.cleanup_decommission_durable_ilm_receipts(source_pool_idx).await
    }

    async fn check_after_decommission(
        self: &Arc<Self>,
        idx: usize,
        rx: &CancellationToken,
        generation: OffsetDateTime,
    ) -> Result<Vec<DecommissionUnresolvedEntry>> {
        let run_token = self.durable_ilm_receipt_run_token_for_generation(idx, generation).await?;
        let operation_gate = self.ctx.data_movement_operation_gate();
        self.run_guarded_decommission_side_effect(rx, &operation_gate, || {
            self.check_after_decommission_unfenced(idx, generation, run_token)
        })
        .await
    }

    async fn check_after_decommission_unfenced(
        self: &Arc<Self>,
        idx: usize,
        generation: OffsetDateTime,
        run_token: String,
    ) -> Result<Vec<DecommissionUnresolvedEntry>> {
        let unresolved_entries = {
            let pool_meta = self.pool_meta.read().await;
            ensure_decommission_generation(&pool_meta, idx, generation)?;
            let info = pool_meta.pools[idx]
                .decommission
                .as_ref()
                .ok_or_else(|| decommission_metadata_not_initialized_error("verify unresolved decommission entries"))?;
            if info
                .unresolved_entries
                .iter()
                .any(|entry| entry.pool_index != idx || entry.source_generation != generation)
            {
                return Err(Error::other(format!(
                    "failed to verify decommission for pool {idx}: unresolved listing ledger contains a different pool or generation"
                )));
            }
            info.unresolved_entries.clone()
        };
        let unresolved_entries_by_identity = Arc::new(
            unresolved_entries
                .iter()
                .map(|entry| (decommission_unresolved_entry_identity(entry), entry.clone()))
                .collect::<HashMap<_, _>>(),
        );
        let resolved_unresolved_entries = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let buckets = self.get_buckets_to_decommission().await?;
        let pool = self.pools[idx].clone();

        self.ensure_decommission_multipart_uploads_drained(idx, &pool, &buckets)
            .await?;

        for (set_index, set) in pool.disk_set.iter().enumerate() {
            let require_all_disks = unresolved_entries.iter().any(|entry| entry.set_index == set_index);
            for bucket_info in &buckets {
                let mut lifecycle_config = None;
                let mut object_lock_config = None;
                let mut replication_configured = false;
                if bucket_info.name != RUSTFS_META_BUCKET {
                    let expiry_configs = get_expiry_configs(self, &bucket_info.name).await?;
                    lifecycle_config = expiry_configs.lifecycle.map(|config| (*config).clone());
                    object_lock_config = expiry_configs.object_lock.map(|config| (*config).clone());
                    replication_configured = resolve_decommission_optional_bucket_config_result(
                        &bucket_info.name,
                        "replication",
                        metadata_sys::get_replication_config(&bucket_info.name).await,
                    )?
                    .is_some();
                }

                let versions_found = Arc::new(AtomicUsize::new(0));
                let entry_error = Arc::new(tokio::sync::Mutex::new(None::<Error>));
                let first_remaining_path = Arc::new(tokio::sync::Mutex::new(None::<String>));
                let callback_rx = CancellationToken::new();
                let versions_found_cb = versions_found.clone();
                let entry_error_cb = entry_error.clone();
                let first_remaining_path_cb = first_remaining_path.clone();
                let bucket_name = bucket_info.name.clone();
                let lifecycle_config_cb = lifecycle_config.clone();
                let object_lock_config_cb = object_lock_config.clone();
                let replication_configured_cb = replication_configured;
                let store = Arc::clone(self);
                let source_set = set.clone();
                let callback_rx_cb = callback_rx.clone();
                let unresolved_entries_by_identity_cb = unresolved_entries_by_identity.clone();
                let resolved_unresolved_entries_cb = resolved_unresolved_entries.clone();
                let run_token_cb = run_token.clone();

                let callback: ListCallback = Arc::new(move |entry: MetaCacheEntry| {
                    let versions_found = versions_found_cb.clone();
                    let entry_error = entry_error_cb.clone();
                    let first_remaining_path = first_remaining_path_cb.clone();
                    let bucket_name = bucket_name.clone();
                    let lifecycle_config = lifecycle_config_cb.clone();
                    let object_lock_config = object_lock_config_cb.clone();
                    let replication_configured = replication_configured_cb;
                    let store = Arc::clone(&store);
                    let source_set = source_set.clone();
                    let callback_rx = callback_rx_cb.clone();
                    let unresolved_entries_by_identity = unresolved_entries_by_identity_cb.clone();
                    let resolved_unresolved_entries = resolved_unresolved_entries_cb.clone();
                    let run_token = run_token_cb.clone();
                    Box::pin(async move {
                        if callback_rx.is_cancelled() {
                            return;
                        }

                        if !entry.is_object() {
                            return;
                        }

                        if bucket_name == RUSTFS_META_BUCKET && entry.name.contains(DATA_USAGE_CACHE_NAME) {
                            return;
                        }

                        let durable_ilm_record = if bucket_name == RUSTFS_META_BUCKET {
                            match classify_durable_ilm_record(&entry.name) {
                                Ok(record) => record,
                                Err(err) => {
                                    let mut first_err = entry_error.lock().await;
                                    if first_err.is_none() {
                                        *first_err = Some(with_decommission_entry_context(
                                            "check_after_decommission.durable_ilm_namespace",
                                            &bucket_name,
                                            &entry.name,
                                            err,
                                        ));
                                        callback_rx.cancel();
                                    }
                                    return;
                                }
                            }
                        } else {
                            None
                        };

                        if durable_ilm_record.is_some() {
                            if let Err(err) = store
                                .verify_and_cleanup_decommissioned_durable_ilm_record_for_run(
                                    idx,
                                    source_set,
                                    &entry.name,
                                    &run_token,
                                )
                                .await
                            {
                                let mut first_err = entry_error.lock().await;
                                if first_err.is_none() {
                                    *first_err = Some(err);
                                    callback_rx.cancel();
                                }
                            }
                            return;
                        }

                        let mut fivs = match load_decommission_entry_exact_versions(
                            &source_set,
                            &entry,
                            &bucket_name,
                            "check_after_decommission.file_info_versions",
                        )
                        .await
                        {
                            Ok(fivs) => fivs,
                            Err(err) => {
                                let mut first_err = entry_error.lock().await;
                                if first_err.is_none() {
                                    *first_err = Some(err);
                                    callback_rx.cancel();
                                }
                                return;
                            }
                        };

                        fivs.versions
                            .sort_by_key(|version| (version.mod_time.is_none(), std::cmp::Reverse(version.mod_time)));

                        let mut remaining = 0;
                        let mut expired = 0;
                        for version in fivs.versions.iter().chain(fivs.free_versions.iter()) {
                            if version.tier_free_version() {
                                remaining += 1;
                                debug!(
                                    event = EVENT_DECOMMISSION_ENTRY,
                                    component = LOG_COMPONENT_ECSTORE,
                                    subsystem = LOG_SUBSYSTEM_POOLS,
                                    pool_index = idx,
                                    bucket = %bucket_name,
                                    object = %entry.name,
                                    version_id = ?version.version_id,
                                    reason = DECOMMISSION_FREE_VERSION_SWEEP_REASON,
                                    state = "free_version_retained",
                                    "Decommission final sweep retained a free version"
                                );
                                continue;
                            }
                            let skip_lifecycle = match should_skip_lifecycle_for_data_movement(
                                Arc::clone(&store),
                                &bucket_name,
                                version,
                                lifecycle_config.as_ref(),
                                object_lock_config.as_ref(),
                                false,
                                &LcEventSrc::Decom,
                                None,
                            )
                            .await
                            {
                                Ok(skip_lifecycle) => skip_lifecycle,
                                Err(err) => {
                                    let mut first_err = entry_error.lock().await;
                                    if first_err.is_none() {
                                        *first_err = Some(err);
                                        callback_rx.cancel();
                                    }
                                    return;
                                }
                            };
                            if skip_lifecycle {
                                expired += 1;
                                continue;
                            }
                            let remaining_versions = decommission_remaining_version_count(&fivs.versions, expired);
                            if should_skip_decommission_delete_marker(version, remaining_versions, replication_configured) {
                                continue;
                            }
                            remaining += 1;
                        }

                        if remaining > 0 {
                            let mut first_path = first_remaining_path.lock().await;
                            if first_path.is_none() {
                                *first_path = Some(format!("{bucket_name}/{}", entry.name));
                            }
                        } else {
                            let identity = (set_index, bucket_name.clone(), entry.name.clone());
                            if let Some(unresolved_entry) = unresolved_entries_by_identity.get(&identity) {
                                let mut resolved = resolved_unresolved_entries.lock().await;
                                if !resolved.contains(unresolved_entry) {
                                    resolved.push(unresolved_entry.clone());
                                }
                            }
                        }

                        versions_found.fetch_add(remaining, Ordering::Relaxed);
                    })
                });

                let list_result = set
                    .list_objects_to_decommission(
                        self.clone(),
                        callback_rx,
                        bucket_info.clone(),
                        callback,
                        entry_error.clone(),
                        idx,
                        set_index,
                        generation,
                        require_all_disks,
                    )
                    .await;
                let entry_error = entry_error.lock().await.clone();
                resolve_decommission_check_after_list_result(list_result, entry_error)?;

                let versions_found = versions_found.load(Ordering::Relaxed);
                if versions_found > 0 {
                    let first_remaining_path = first_remaining_path
                        .lock()
                        .await
                        .clone()
                        .unwrap_or_else(|| format!("{}/<unknown>", bucket_info.name));
                    return Err(Error::other(format!(
                        "at least {versions_found} object(s)/version(s) were found in bucket `{}` after decommissioning; first remaining path `{first_remaining_path}`",
                        bucket_info.name,
                    )));
                }
            }
        }

        let mut verified_unresolved_entries = resolved_unresolved_entries.lock().await.clone();
        for entry in &unresolved_entries {
            let set = pool.disk_set.get(entry.set_index).ok_or_else(|| {
                Error::other(format!(
                    "failed to verify decommission for pool {idx}: unresolved listing entry references missing set {}",
                    entry.set_index
                ))
            })?;
            if set.decommission_unresolved_entry_absent_on_all_disks(idx, entry).await?
                && !verified_unresolved_entries.contains(entry)
            {
                verified_unresolved_entries.push(entry.clone());
            }
            if !verified_unresolved_entries.contains(entry) {
                return Err(Error::other(format!(
                    "failed to verify decommission for pool {idx}: unresolved listing entry {}/{} in set {} was neither re-observed as resolved nor absent on every source disk",
                    entry.bucket, entry.object, entry.set_index
                )));
            }
        }

        self.persist_decommission_durable_ilm_manifest_for_run(idx, &run_token)
            .await?;
        self.verify_decommission_durable_ilm_receipts_for_run(idx, &run_token).await?;

        Ok(verified_unresolved_entries)
    }

    async fn ensure_decommission_multipart_uploads_drained(
        &self,
        idx: usize,
        pool: &Sets,
        buckets: &[DecomBucketInfo],
    ) -> Result<()> {
        let mut bucket_names = buckets
            .iter()
            .filter(|bucket| bucket.name != RUSTFS_META_BUCKET)
            .map(|bucket| bucket.name.as_str())
            .collect::<Vec<_>>();
        bucket_names.sort_unstable();
        bucket_names.dedup();

        // Take one bucket fence at a time so cross-bucket COPY cannot form an
        // ABBA cycle. Suspension prevents new source uploads after each fence.
        for bucket in bucket_names {
            let lifecycle_guard = self.acquire_bucket_lifecycle_write_lock(bucket).await?;
            if lifecycle_guard.is_lock_lost() {
                return Err(Error::other(format!(
                    "decommission multipart drain lost the bucket lifecycle fence for `{bucket}`"
                )));
            }
            for set in &pool.disk_set {
                if let Some(upload_path) = set.first_multipart_upload_path_for_decommission(bucket).await? {
                    return Err(Error::other(format!(
                        "pool {idx} still contains multipart upload `{upload_path}` for bucket `{bucket}`; resolve it before retrying decommission"
                    )));
                }
            }
        }

        Ok(())
    }

    #[cfg(test)]
    pub(crate) async fn ensure_decommission_multipart_uploads_drained_for_test(self: &Arc<Self>, idx: usize) -> Result<()> {
        let buckets = self.get_buckets_to_decommission().await?;
        let pool = self.pools[idx].clone();
        self.ensure_decommission_multipart_uploads_drained(idx, pool.as_ref(), &buckets)
            .await
    }
    #[cfg(all(test, feature = "test-util"))]
    pub(crate) async fn check_after_decommission_for_test(self: &Arc<Self>, idx: usize) -> Result<()> {
        let generation = self.active_decommission_generation(idx).await?;
        self.check_after_decommission(idx, &CancellationToken::new(), generation)
            .await
            .map(|_| ())
    }

    #[tracing::instrument(skip(self, rd))]
    async fn decommission_object(
        self: Arc<Self>,
        pool_idx: usize,
        bucket: String,
        rd: GetObjectReader,
        expected_bucket_incarnation_id: Option<uuid::Uuid>,
    ) -> Result<()> {
        warn!("decommission_object: start {} {}", &bucket, &rd.object_info.name);
        let object_name = rd.object_info.name.clone();
        let mut migration = tokio::task::JoinSet::new();
        migration.spawn(data_movement::migrate_decommission_object(
            self,
            pool_idx,
            bucket.clone(),
            rd,
            expected_bucket_incarnation_id,
            "decommission_object",
        ));
        let result = migration
            .join_next()
            .await
            .ok_or_else(|| Error::other("decommission migration task was not started"))?
            .map_err(|err| Error::other(format!("decommission migration task join error: {err}")))?;
        if result.is_ok() {
            warn!("decommission_object: migrated {} {}", &bucket, &object_name);
        }
        result
    }
}

#[cfg(test)]
#[allow(clippy::items_after_test_module)]
mod tests {
    use super::*;
    use crate::bucket::replication::{ReplicationState, ReplicationStatusType};
    use serde::Serialize;

    #[test]
    fn pool_activation_fleet_proof_error_classifier_matches_only_retryable_proof_failures() {
        assert!(is_pool_activation_fleet_proof_error(&Error::other(POOL_ACTIVATION_FLEET_PROOF_REQUIRED)));
        assert!(is_pool_activation_fleet_proof_error(&Error::other(POOL_ACTIVATION_FLEET_PROOF_EXPIRED)));
        let wrapped = format!("rebalance meta save failed during start_rebalance: {POOL_ACTIVATION_FLEET_PROOF_EXPIRED}");
        assert!(is_pool_activation_fleet_proof_error(&Error::other(wrapped)));
        assert!(!is_pool_activation_fleet_proof_error(&Error::ConfigNotFound));
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn decommission_activation_fence_loss_after_durable_save_blocks_publication() {
        let (_temp_dirs, store, _other_store) = crate::services::rebalance::test_two_pool_stores(None).await;
        let barrier = PoolActivationDurableSaveBarrier::install(&store.pools[0]);
        let start_store = Arc::clone(&store);
        let start_task = tokio::spawn(async move {
            start_store
                .save_current_pool_meta_for_decommission_start(
                    &[0],
                    vec![(
                        0,
                        PoolSpaceInfo {
                            free: 50,
                            total: 100,
                            used: 50,
                        },
                    )],
                    Vec::new(),
                )
                .await
        });

        barrier.wait_until_paused().await;
        barrier.release_after_fence_loss();
        let err = tokio::time::timeout(std::time::Duration::from_secs(30), start_task)
            .await
            .expect("decommission activation should stop after losing its fence")
            .expect("decommission activation task should not panic")
            .expect_err("post-durable-save fence loss must reject in-memory publication");
        assert!(err.to_string().contains("activation lock lost"));

        let local = store.pool_meta.read().await;
        assert!(
            !pool_meta_has_active_decommission(&local),
            "the activation must not publish after its durable fence is lost"
        );
        drop(local);
        store
            .ensure_pool_meta_side_effects_safe("post-durable-save activation fence loss")
            .await
            .expect_err("the undisarmed transaction must latch the sticky pool metadata gate");

        let mut persisted = PoolMeta::default();
        persisted
            .load_no_lock_from_replicas(vec![store.pools[0].clone()])
            .await
            .expect("the durable replica should remain readable for recovery");
        assert!(
            pool_meta_has_active_decommission(&persisted),
            "the test must lose the fence only after one durable replica commit"
        );
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn decommission_activation_adopts_canonical_commit_after_replica_failure() {
        let (_temp_dirs, store, _other_store) = crate::services::rebalance::test_two_pool_stores(None).await;
        let barrier = PoolActivationDurableSaveBarrier::install(&store.pools[0]);
        let start_store = Arc::clone(&store);
        let start_task = tokio::spawn(async move {
            start_store
                .save_current_pool_meta_for_decommission_start(
                    &[0],
                    vec![(
                        0,
                        PoolSpaceInfo {
                            free: 50,
                            total: 100,
                            used: 50,
                        },
                    )],
                    Vec::new(),
                )
                .await
        });

        barrier.wait_until_paused().await;
        let mut replica_disks = Vec::new();
        for set in &store.pools[1].disk_set {
            let mut disks = set.disks.write().await;
            let saved = std::mem::take(&mut *disks);
            *disks = vec![None; saved.len()];
            replica_disks.push(saved);
        }
        barrier.release_without_fence_loss();

        tokio::time::timeout(std::time::Duration::from_secs(30), start_task)
            .await
            .expect("decommission activation should finish after its canonical commit")
            .expect("decommission activation task should not panic")
            .expect("a replica save failure must not report the committed activation as failed");

        for (set, disks) in store.pools[1].disk_set.iter().zip(replica_disks) {
            *set.disks.write().await = disks;
        }

        let local = store.pool_meta.read().await;
        assert!(pool_meta_has_active_decommission(&local));
        drop(local);

        let mut canonical = PoolMeta::default();
        canonical
            .load_no_lock_from_replicas(vec![store.pools[0].clone()])
            .await
            .expect("the canonical committed decommission metadata should remain readable");
        assert!(pool_meta_has_active_decommission(&canonical));

        let mut replica = PoolMeta::default();
        replica
            .load_no_lock_from_replicas(vec![store.pools[1].clone()])
            .await
            .expect("the stale replica metadata should remain readable after disks recover");
        assert!(!pool_meta_has_active_decommission(&replica));

        let mut reloaded = PoolMeta::default();
        let replica_state = reloaded
            .load_no_lock_from_replicas(store.pools.clone())
            .await
            .expect("the canonical commit should remain reloadable after a replica recovers stale");
        assert!(pool_meta_has_active_decommission(&reloaded));
        assert!(replica_state.needs_repair);
        assert!(replica_state.repair_write_safe);

        let worker_cancel = CancellationToken::new();
        store
            .spawn_decommission_routines(Arc::clone(&store), worker_cancel.clone(), vec![0])
            .await
            .expect("the committed activation should admit its decommission worker");
        let admitted_cancel = store.decommission_cancelers.read().await[0]
            .clone()
            .expect("the admitted decommission worker should have a cancellation token");
        assert!(!admitted_cancel.is_cancelled());
        worker_cancel.cancel();
        assert!(admitted_cancel.is_cancelled());
    }

    fn pool_meta_replica_test_meta(cmd_line: &str) -> PoolMeta {
        PoolMeta {
            version: POOL_META_VERSION,
            pools: vec![PoolStatus {
                id: 0,
                cmd_line: cmd_line.to_string(),
                last_update: OffsetDateTime::UNIX_EPOCH,
                decommission: None,
            }],
            dont_save: false,
        }
    }

    fn pool_meta_replica_test_data(cmd_line: &str) -> Vec<u8> {
        pool_meta_replica_test_meta(cmd_line)
            .encode_config_data()
            .expect("pool metadata should encode")
    }

    fn pool_meta_v1_replica_test_data(meta: &PoolMeta) -> Vec<u8> {
        let mut data = Vec::new();
        data.write_u16::<LittleEndian>(POOL_META_FORMAT)
            .expect("pool metadata format should encode");
        data.write_u16::<LittleEndian>(POOL_META_V1_VERSION)
            .expect("pool metadata version should encode");
        PersistedPoolMetaV1::from(meta)
            .serialize(&mut Serializer::new(&mut data))
            .expect("legacy pool metadata should encode");
        data
    }

    fn pool_meta_persisted_v1_replica_test_data(cmd_line: &str) -> Vec<u8> {
        pool_meta_v1_replica_test_data(&pool_meta_replica_test_meta(cmd_line))
    }

    fn pool_meta_legacy_v1_replica_test_data(cmd_line: &str) -> Vec<u8> {
        let mut data = Vec::new();
        data.write_u16::<LittleEndian>(POOL_META_FORMAT)
            .expect("pool metadata format should encode");
        data.write_u16::<LittleEndian>(POOL_META_V1_VERSION)
            .expect("pool metadata version should encode");
        LegacyPoolMeta {
            version: POOL_META_V1_VERSION,
            pools: vec![LegacyPoolStatus {
                id: 0,
                cmd_line: cmd_line.to_string(),
                last_update: OffsetDateTime::UNIX_EPOCH,
                decommission: None,
            }],
            dont_save: false,
        }
        .serialize(&mut Serializer::new(&mut data))
        .expect("legacy v1 pool metadata should encode");
        data
    }

    #[test]
    fn pool_meta_replica_selection_falls_back_from_corrupt_first_copy() {
        let selection = select_pool_meta_replica(vec![
            PoolMetaReplica::Corrupt("truncated".to_string()),
            decode_pool_meta_replica(pool_meta_replica_test_data("pool-0")),
        ])
        .expect("a validated backup replica should be selected");

        assert!(selection.replica_state.needs_repair);
        assert!(selection.replica_state.repair_write_safe);
        assert_eq!(selection.meta.pools.len(), 1);
        assert_eq!(selection.meta.pools[0].cmd_line, "pool-0");
    }

    #[test]
    fn pool_meta_replica_selection_rejects_incompatible_copy() {
        let valid = pool_meta_replica_test_data("pool-0");
        let mut incompatible = valid.clone();
        LittleEndian::write_u16(&mut incompatible[2..4], POOL_META_VERSION + 1);

        let err = select_pool_meta_replica(vec![decode_pool_meta_replica(valid), decode_pool_meta_replica(incompatible)])
            .expect_err("an incompatible replica must block fallback and repair writes");

        assert!(err.to_string().contains("pool 1 is incompatible"));
        assert!(err.to_string().contains("without overwriting it"));
    }

    #[test]
    fn pool_meta_replica_selection_prefers_valid_canonical_first_copy() {
        let selection = select_pool_meta_replica(vec![
            decode_pool_meta_replica(pool_meta_replica_test_data("pool-canonical")),
            decode_pool_meta_replica(pool_meta_replica_test_data("pool-stale")),
        ])
        .expect("pool zero is the durable commit record when a later replica is stale");

        assert_eq!(selection.meta.pools[0].cmd_line, "pool-canonical");
        assert!(selection.replica_state.needs_repair);
        assert!(selection.replica_state.repair_write_safe);
    }

    #[test]
    fn pool_meta_replica_selection_rejects_divergent_backups_without_canonical() {
        let err = select_pool_meta_replica(vec![
            PoolMetaReplica::Corrupt("canonical unavailable".to_string()),
            decode_pool_meta_replica(pool_meta_replica_test_data("pool-old")),
            decode_pool_meta_replica(pool_meta_replica_test_data("pool-new")),
        ])
        .expect_err("divergent backups have no safe ordering when the canonical copy is unavailable");

        assert!(err.to_string().contains("valid replicas in pools 1 and 2 diverge"));
    }

    #[test]
    fn pool_meta_replica_selection_normalizes_equivalent_legacy_copy() {
        let selection = select_pool_meta_replica(vec![
            decode_pool_meta_replica(pool_meta_legacy_v1_replica_test_data("pool-0")),
            decode_pool_meta_replica(pool_meta_replica_test_data("pool-0")),
        ])
        .expect("equivalent legacy and current encodings should be compatible");

        assert!(selection.replica_state.needs_repair);
        assert!(selection.replica_state.repair_write_safe);
        assert_eq!(selection.meta.pools[0].cmd_line, "pool-0");
    }

    #[test]
    fn pool_meta_v1_replica_migrates_to_v2_canonical_form() {
        let mut source = pool_meta_replica_test_meta("pool-0");
        source.pools[0].decommission = Some(PoolDecommissionInfo {
            start_time: Some(OffsetDateTime::UNIX_EPOCH),
            queued_buckets: vec!["bucket-a".to_string()],
            ..Default::default()
        });
        let data = pool_meta_v1_replica_test_data(&source);
        let PoolMetaReplica::Valid {
            raw, canonical, meta, ..
        } = decode_pool_meta_replica(data)
        else {
            panic!("v1 pool metadata should remain readable");
        };

        assert_eq!(LittleEndian::read_u16(&raw[2..4]), POOL_META_V1_VERSION);
        assert_eq!(LittleEndian::read_u16(&canonical[2..4]), POOL_META_VERSION);
        assert_eq!(meta.version, POOL_META_V1_VERSION);
        let info = meta.pools[0]
            .decommission
            .as_ref()
            .expect("v1 decommission state should migrate");
        assert_eq!(info.queued_buckets, vec!["bucket-a".to_string()]);
        assert!(info.unresolved_entries.is_empty());
    }

    #[test]
    fn pool_meta_legacy_v1_replica_migrates_to_v2_canonical_form() {
        let data = pool_meta_legacy_v1_replica_test_data("pool-0");
        let PoolMetaReplica::Valid {
            raw, canonical, meta, ..
        } = decode_pool_meta_replica(data)
        else {
            panic!("legacy v1 pool metadata should remain readable");
        };

        assert_eq!(LittleEndian::read_u16(&raw[2..4]), POOL_META_V1_VERSION);
        assert_eq!(LittleEndian::read_u16(&canonical[2..4]), POOL_META_VERSION);
        assert_eq!(meta.version, POOL_META_V1_VERSION);
        assert_eq!(meta.pools[0].cmd_line, "pool-0");
    }

    #[test]
    fn pool_meta_replica_selection_rejects_v1_v2_divergence() {
        let err = select_pool_meta_replica(vec![
            decode_pool_meta_replica(pool_meta_persisted_v1_replica_test_data("pool-old")),
            decode_pool_meta_replica(pool_meta_replica_test_data("pool-new")),
        ])
        .expect_err("different v1 and v2 snapshots must remain fail-closed");

        assert!(err.to_string().contains("valid replicas in pools 0 and 1 diverge"));
    }

    #[test]
    fn pool_meta_replica_rejects_header_payload_version_mismatch() {
        let mut v1_with_v2_header = pool_meta_persisted_v1_replica_test_data("pool-0");
        LittleEndian::write_u16(&mut v1_with_v2_header[2..4], POOL_META_VERSION);
        assert!(matches!(decode_pool_meta_replica(v1_with_v2_header), PoolMetaReplica::Corrupt(_)));

        let mut v2_with_v1_header = pool_meta_replica_test_data("pool-0");
        LittleEndian::write_u16(&mut v2_with_v1_header[2..4], POOL_META_V1_VERSION);
        assert!(matches!(
            decode_pool_meta_replica(v2_with_v1_header),
            PoolMetaReplica::Corrupt(_) | PoolMetaReplica::Incompatible(_)
        ));
    }

    #[test]
    fn pool_meta_v2_header_guards_v1_readers_from_tuple_extension() {
        let mut meta = pool_meta_replica_test_meta("pool-0");
        meta.pools[0].decommission = Some(PoolDecommissionInfo::default());
        let data = meta.encode_config_data().expect("v2 pool metadata should encode");

        assert_eq!(LittleEndian::read_u16(&data[2..4]), POOL_META_VERSION);
        assert_ne!(POOL_META_VERSION, POOL_META_V1_VERSION);
        assert!(rmp_serde::from_slice::<PersistedPoolMetaV1>(&data[4..]).is_err());
    }

    #[test]
    fn pool_meta_writer_stays_v1_until_v2_is_fleet_confirmed() {
        let mut meta = pool_meta_replica_test_meta("pool-0");
        meta.version = POOL_META_V1_VERSION;

        let v1 = meta
            .encode_config_data_for_v2_gate(false)
            .expect("the default writer should preserve v1");
        let v2 = meta
            .encode_config_data_for_v2_gate(true)
            .expect("the confirmed writer should emit v2");

        assert_eq!(LittleEndian::read_u16(&v1[2..4]), POOL_META_V1_VERSION);
        assert_eq!(LittleEndian::read_u16(&v2[2..4]), POOL_META_VERSION);
        assert!(rmp_serde::from_slice::<PersistedPoolMetaV1>(&v1[4..]).is_ok());
    }

    #[test]
    fn pool_meta_v2_writer_requires_both_gates() {
        assert!(!pool_meta_v2_writer_enabled_for(false, false));
        assert!(!pool_meta_v2_writer_enabled_for(true, false));
        assert!(!pool_meta_v2_writer_enabled_for(false, true));
        assert!(pool_meta_v2_writer_enabled_for(true, true));
    }

    #[test]
    fn pool_meta_v3_writer_requires_both_gates() {
        assert!(!pool_meta_v3_writer_enabled_for(false, false));
        assert!(!pool_meta_v3_writer_enabled_for(true, false));
        assert!(!pool_meta_v3_writer_enabled_for(false, true));
        assert!(pool_meta_v3_writer_enabled_for(true, true));
    }

    #[test]
    fn pool_meta_stale_write_rejection_metric_is_countable() {
        let recorder = metrics_util::debugging::DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        metrics::with_local_recorder(&recorder, || {
            record_pool_meta_stale_write_rejection("prepare_cas");
            record_pool_meta_stale_write_rejection("prepare_cas");
        });

        let total = snapshotter
            .snapshot()
            .into_vec()
            .into_iter()
            .filter(|(composite, _, _, _)| composite.key().name() == METRIC_POOL_META_STALE_WRITE_REJECTIONS_TOTAL)
            .filter_map(|(_, _, _, value)| match value {
                metrics_util::debugging::DebugValue::Counter(value) => Some(value),
                _ => None,
            })
            .sum::<u64>();
        assert_eq!(total, 2);
    }

    fn pool_meta_v3_test_revision(cluster_id: uuid::Uuid, generation: u64, transaction_id: uuid::Uuid) -> PoolMetaRevision {
        PoolMetaRevision {
            version: POOL_META_GENERATION_VERSION,
            cluster_id: Some(cluster_id),
            epoch: POOL_META_INITIAL_EPOCH,
            generation,
            transaction_id: Some(transaction_id),
        }
    }

    fn pool_meta_legacy_candidate(meta: PoolMeta) -> PoolMetaCommittedCandidate {
        PoolMetaCommittedCandidate {
            canonical: meta
                .encode_config_data_for_v2_gate(true)
                .expect("legacy pool metadata should encode"),
            revision: PoolMetaRevision::legacy(meta.version),
            meta,
        }
    }

    #[test]
    fn pool_meta_v3_pending_prepare_recovers_previous_snapshot_after_restart() {
        let previous = pool_meta_replica_test_meta("pool-before");
        let mut next = pool_meta_replica_test_meta("pool-after");
        next.version = POOL_META_GENERATION_VERSION;
        let revision = pool_meta_v3_test_revision(uuid::Uuid::new_v4(), 1, uuid::Uuid::new_v4());
        let pending = encode_pool_meta_v3_envelope(&next, revision, false, Some(&pool_meta_legacy_candidate(previous.clone())))
            .expect("pending generation should encode");

        let selected = select_pool_meta_replica(vec![
            decode_pool_meta_replica(pending),
            decode_pool_meta_replica(
                previous
                    .encode_config_data_for_v2_gate(true)
                    .expect("previous snapshot should encode"),
            ),
        ])
        .expect("a prepare-only transaction should expose its committed predecessor");

        assert_eq!(selected.meta.pools[0].cmd_line, "pool-before");
        assert_eq!(selected.revision.version, POOL_META_VERSION);
        assert!(selected.replica_state.needs_repair);
    }

    #[test]
    fn pool_meta_v3_partial_commit_selects_new_generation_after_restart() {
        let previous = pool_meta_replica_test_meta("pool-before");
        let mut next = pool_meta_replica_test_meta("pool-after");
        next.version = POOL_META_GENERATION_VERSION;
        let revision = pool_meta_v3_test_revision(uuid::Uuid::new_v4(), 1, uuid::Uuid::new_v4());
        let pending = encode_pool_meta_v3_envelope(&next, revision, false, Some(&pool_meta_legacy_candidate(previous)))
            .expect("pending generation should encode");
        let committed = encode_pool_meta_v3_envelope(&next, revision, true, None).expect("committed generation should encode");

        let selected = select_pool_meta_replica(vec![decode_pool_meta_replica(committed), decode_pool_meta_replica(pending)])
            .expect("one committed replica should make the cross-pool transaction durable");

        assert_eq!(selected.meta.pools[0].cmd_line, "pool-after");
        assert_eq!(selected.revision, revision);
        assert!(selected.replica_state.needs_repair);
    }

    #[test]
    fn pool_meta_v3_uses_valid_committed_backup_but_rejects_same_generation_fork() {
        let cluster_id = uuid::Uuid::new_v4();
        let mut next = pool_meta_replica_test_meta("pool-after");
        next.version = POOL_META_GENERATION_VERSION;
        let revision = pool_meta_v3_test_revision(cluster_id, 7, uuid::Uuid::new_v4());
        let committed = encode_pool_meta_v3_envelope(&next, revision, true, None).expect("committed generation should encode");
        let selected = select_pool_meta_replica(vec![
            PoolMetaReplica::Corrupt("truncated".to_string()),
            decode_pool_meta_replica(committed.clone()),
        ])
        .expect("a corrupt first copy should fall back to a valid committed generation");
        assert_eq!(selected.revision, revision);
        assert!(selected.replica_state.needs_repair);

        let fork_revision = pool_meta_v3_test_revision(cluster_id, 7, uuid::Uuid::new_v4());
        let fork = encode_pool_meta_v3_envelope(&next, fork_revision, true, None).expect("fork generation should encode");
        let err = select_pool_meta_replica(vec![decode_pool_meta_replica(committed), decode_pool_meta_replica(fork)])
            .expect_err("same-generation transactions must never be selected by pool order");
        assert!(err.to_string().contains("committed generation 7 diverges"));
    }

    #[test]
    fn pool_meta_v3_migration_keeps_legacy_committed_until_commit_record_exists() {
        let previous = pool_meta_replica_test_meta("pool-before");
        let mut next = pool_meta_replica_test_meta("pool-after");
        next.version = POOL_META_GENERATION_VERSION;
        let revision = pool_meta_v3_test_revision(uuid::Uuid::new_v4(), 1, uuid::Uuid::new_v4());
        let pending = encode_pool_meta_v3_envelope(&next, revision, false, Some(&pool_meta_legacy_candidate(previous.clone())))
            .expect("pending migration should encode");
        let legacy = previous
            .encode_config_data_for_v2_gate(true)
            .expect("legacy snapshot should encode");

        let prepared = select_pool_meta_replica(vec![decode_pool_meta_replica(pending), decode_pool_meta_replica(legacy)])
            .expect("prepared migration should retain the legacy commit");
        assert_eq!(prepared.meta.version, POOL_META_VERSION);
        assert_eq!(prepared.meta.pools[0].cmd_line, "pool-before");

        let committed = encode_pool_meta_v3_envelope(&next, revision, true, None).expect("committed migration should encode");
        let migrated = select_pool_meta_replica(vec![decode_pool_meta_replica(committed)])
            .expect("committed migration should establish the V3 floor");
        assert_eq!(migrated.meta.version, POOL_META_GENERATION_VERSION);
        assert_eq!(migrated.meta.pools[0].cmd_line, "pool-after");
    }

    #[test]
    fn pool_meta_v3_unknown_fields_remain_incompatible_not_silently_ignored() {
        #[derive(Serialize)]
        struct FuturePoolMetaV3 {
            version: u16,
            cluster_id: String,
            epoch: u64,
            generation: u64,
            transaction_id: String,
            committed: bool,
            pools: Vec<PersistedPoolStatus>,
            previous: Option<PersistedPoolMetaV3Previous>,
            future_guard: u64,
        }

        let mut data = Vec::new();
        data.write_u16::<LittleEndian>(POOL_META_FORMAT)
            .expect("pool metadata format should encode");
        data.write_u16::<LittleEndian>(POOL_META_GENERATION_VERSION)
            .expect("pool metadata version should encode");
        FuturePoolMetaV3 {
            version: POOL_META_GENERATION_VERSION,
            cluster_id: uuid::Uuid::new_v4().to_string(),
            epoch: POOL_META_INITIAL_EPOCH,
            generation: 1,
            transaction_id: uuid::Uuid::new_v4().to_string(),
            committed: true,
            pools: Vec::new(),
            previous: None,
            future_guard: 1,
        }
        .serialize(&mut Serializer::new(&mut data))
        .expect("future V3 payload should encode");

        assert!(matches!(decode_pool_meta_replica(data), PoolMetaReplica::Incompatible(_)));
    }

    #[test]
    fn pool_meta_identity_classifies_corrupt_incompatible_and_divergent_replicas() {
        let cluster_id = uuid::Uuid::new_v4();
        let identity = PersistedPoolMetaIdentity {
            version: POOL_META_IDENTITY_VERSION,
            cluster_id,
            epoch: POOL_META_INITIAL_EPOCH,
            initialized: true,
            fresh_bootstrap_nonce: None,
        };
        let selected = select_pool_meta_identity(
            vec![
                PoolMetaIdentityRead {
                    replica: PoolMetaIdentityReplica::Corrupt("truncated".to_string()),
                    cas: PoolMetaCasToken::Existing("corrupt".to_string()),
                },
                PoolMetaIdentityRead {
                    replica: PoolMetaIdentityReplica::Valid(identity),
                    cas: PoolMetaCasToken::Existing("valid".to_string()),
                },
            ],
            cluster_id,
        )
        .expect("a verified identity backup should survive a corrupt first replica");
        assert_eq!(selected.identity, Some(identity));
        assert!(selected.needs_repair);

        let incompatible = select_pool_meta_identity(
            vec![PoolMetaIdentityRead {
                replica: PoolMetaIdentityReplica::Incompatible("future version".to_string()),
                cas: PoolMetaCasToken::Existing("future".to_string()),
            }],
            cluster_id,
        )
        .expect_err("an incompatible identity must fail closed");
        assert!(incompatible.to_string().contains("incompatible"));

        let divergent = select_pool_meta_identity(
            vec![
                PoolMetaIdentityRead {
                    replica: PoolMetaIdentityReplica::Valid(identity),
                    cas: PoolMetaCasToken::Existing("epoch-1".to_string()),
                },
                PoolMetaIdentityRead {
                    replica: PoolMetaIdentityReplica::Valid(PersistedPoolMetaIdentity {
                        epoch: POOL_META_INITIAL_EPOCH + 1,
                        ..identity
                    }),
                    cas: PoolMetaCasToken::Existing("epoch-2".to_string()),
                },
            ],
            cluster_id,
        )
        .expect_err("identity epoch divergence must require recovery");
        assert!(divergent.to_string().contains("recovery required"));
    }

    #[test]
    fn pool_meta_v2_floor_is_sticky_after_observation() {
        let meta = pool_meta_replica_test_meta("pool-0");
        let data = meta
            .encode_config_data_for_v2_gate(false)
            .expect("an observed v2 snapshot must not be downgraded");

        assert_eq!(LittleEndian::read_u16(&data[2..4]), POOL_META_VERSION);
    }

    #[test]
    fn pool_meta_v1_writer_rejects_unresolved_ledger() {
        let mut meta = pool_meta_replica_test_meta("pool-0");
        meta.version = POOL_META_V1_VERSION;
        meta.pools[0].decommission = Some(PoolDecommissionInfo {
            start_time: Some(OffsetDateTime::UNIX_EPOCH),
            unresolved_entries: vec![DecommissionUnresolvedEntry {
                bucket: "bucket-a".to_string(),
                object: "object-a".to_string(),
                pool_index: 0,
                set_index: 0,
                source_generation: OffsetDateTime::UNIX_EPOCH,
                candidate_count: 1,
                disk_error_count: 1,
                observed_at: OffsetDateTime::UNIX_EPOCH,
                reason: "metadata_resolution_failed".to_string(),
            }],
            ..Default::default()
        });

        let err = meta
            .encode_config_data_for_v2_gate(false)
            .expect_err("v1 must not drop the unresolved ledger");
        assert!(err.to_string().contains("pool metadata V2 is required"));
    }

    #[test]
    fn pool_meta_replica_selection_preserves_observed_v2_floor() {
        let selection = select_pool_meta_replica(vec![
            decode_pool_meta_replica(pool_meta_persisted_v1_replica_test_data("pool-0")),
            decode_pool_meta_replica(pool_meta_replica_test_data("pool-0")),
        ])
        .expect("equivalent v1 and v2 replicas should converge");

        assert_eq!(selection.meta.version, POOL_META_VERSION);
        let data = selection
            .meta
            .encode_config_data_for_v2_gate(false)
            .expect("the selected v2 floor must remain writable");
        assert_eq!(LittleEndian::read_u16(&data[2..4]), POOL_META_VERSION);
    }

    #[test]
    fn pool_meta_replica_selection_distinguishes_absent_from_unrecoverable() {
        assert!(matches!(decode_pool_meta_replica(Vec::new()), PoolMetaReplica::Corrupt(_)));

        let empty = select_pool_meta_replica(vec![PoolMetaReplica::Missing, PoolMetaReplica::Missing])
            .expect("all missing replicas should preserve new-deployment behavior");
        assert!(empty.meta.pools.is_empty());
        assert!(!empty.replica_state.needs_repair);
        assert!(empty.replica_state.repair_write_safe);

        let err = select_pool_meta_replica(vec![
            PoolMetaReplica::Missing,
            PoolMetaReplica::Unreadable("read quorum unavailable".to_string()),
        ])
        .expect_err("an unreadable replica must not be treated as a new deployment");
        assert!(err.to_string().contains("no valid committed replica is available"));
        assert!(err.to_string().contains("pool 1 is unreadable"));
    }

    #[test]
    fn pool_meta_replica_selection_blocks_repair_for_unreadable_copy() {
        let selection = select_pool_meta_replica(vec![
            decode_pool_meta_replica(pool_meta_replica_test_data("pool-0")),
            PoolMetaReplica::Unreadable("read quorum unavailable".to_string()),
        ])
        .expect("a validated replica should remain usable while another copy is unreadable");

        assert!(selection.replica_state.needs_repair);
        assert!(!selection.replica_state.repair_write_safe);
    }

    #[test]
    fn pool_meta_write_state_remains_blocked_after_unreadable_replica() {
        let mut write_state = PoolMetaWriteState::default();
        write_state.observe_replicas(PoolMetaReplicaState {
            needs_repair: true,
            repair_write_safe: false,
        });
        write_state.observe_replicas(PoolMetaReplicaState {
            needs_repair: false,
            repair_write_safe: true,
        });

        let err = write_state
            .ensure_write_safe("pool metadata save failed")
            .expect_err("a later clean read must not clear the startup write block");
        assert!(
            err.to_string()
                .contains("restart after all replicas are readable and consistent")
        );
    }

    #[test]
    fn pool_meta_write_state_blocks_when_selection_has_no_valid_replica() {
        let replicas = vec![
            PoolMetaReplica::Unreadable("pool 0 read quorum unavailable".to_string()),
            PoolMetaReplica::Unreadable("pool 1 read quorum unavailable".to_string()),
        ];
        let mut write_state = PoolMetaWriteState::default();

        select_pool_meta_replicas_observing(&mut write_state, replicas).expect_err("all unreadable replicas must fail selection");

        let err = write_state
            .ensure_write_safe("pool metadata save failed")
            .expect_err("an all-unreadable runtime read must latch the write block");
        assert!(
            err.to_string()
                .contains("restart after all replicas are readable and consistent")
        );
    }

    #[test]
    fn pool_meta_write_state_blocks_on_any_recovery_required_selection() {
        fn assert_selection_blocks(replicas: Vec<PoolMetaReplica>) {
            let mut write_state = PoolMetaWriteState::default();
            select_pool_meta_replicas_observing(&mut write_state, replicas)
                .expect_err("recovery-required replicas must fail selection");
            write_state
                .ensure_write_safe("pool metadata save failed")
                .expect_err("a recovery-required selection must latch the write block");
        }

        assert_selection_blocks(vec![PoolMetaReplica::Corrupt("truncated".to_string())]);
        assert_selection_blocks(vec![PoolMetaReplica::Incompatible("future format".to_string())]);
        assert_selection_blocks(vec![
            PoolMetaReplica::Corrupt("canonical unavailable".to_string()),
            decode_pool_meta_replica(pool_meta_replica_test_data("pool-old")),
            decode_pool_meta_replica(pool_meta_replica_test_data("pool-new")),
        ]);
    }

    #[test]
    fn pool_meta_replica_selection_rejects_same_version_tuple_extension() {
        #[derive(Serialize)]
        struct FuturePersistedPoolMeta {
            version: u16,
            pools: Vec<PersistedPoolStatus>,
            generation: u64,
        }

        let mut data = Vec::new();
        data.write_u16::<LittleEndian>(POOL_META_FORMAT)
            .expect("pool metadata format should encode");
        data.write_u16::<LittleEndian>(POOL_META_VERSION)
            .expect("pool metadata version should encode");
        FuturePersistedPoolMeta {
            version: POOL_META_VERSION,
            pools: Vec::new(),
            generation: 2,
        }
        .serialize(&mut Serializer::new(&mut data))
        .expect("extended tuple pool metadata should encode");

        let err = select_pool_meta_replica(vec![
            decode_pool_meta_replica(pool_meta_replica_test_data("pool-0")),
            decode_pool_meta_replica(data),
        ])
        .expect_err("same-version tuple extensions must block fallback repair writes");

        assert!(err.to_string().contains("pool 1 is incompatible"));
        assert!(err.to_string().contains("version 2 tuple has unsupported field count"));
    }

    #[test]
    fn pool_meta_replica_selection_falls_back_from_truncated_current_tuple() {
        let mut truncated = pool_meta_replica_test_data("pool-truncated");
        truncated.pop();

        let selection = select_pool_meta_replica(vec![
            decode_pool_meta_replica(truncated),
            decode_pool_meta_replica(pool_meta_replica_test_data("pool-valid")),
        ])
        .expect("a truncated tuple should not block a validated backup replica");

        assert!(selection.replica_state.needs_repair);
        assert!(selection.replica_state.repair_write_safe);
        assert_eq!(selection.meta.pools[0].cmd_line, "pool-valid");
    }

    #[test]
    fn ensure_pool_not_left_in_cmdline_after_decommission_allows_active_pool() {
        assert!(ensure_pool_not_left_in_cmdline_after_decommission(0, "http://node{1...4}/disk{1...4}", false).is_ok());
    }

    #[test]
    fn ensure_pool_not_left_in_cmdline_after_decommission_rejects_completed_pool() {
        let err = ensure_pool_not_left_in_cmdline_after_decommission(1, "http://node{1...4}/disk{1...4}", true)
            .expect_err("completed decommissioned pool should fail validation");

        assert!(
            err.to_string()
                .contains("pool(2) = http://node{1...4}/disk{1...4} is decommissioned, please remove from server command line")
        );
    }

    #[test]
    fn determine_decommission_final_state_marks_failures_and_cancellations() {
        assert_eq!(determine_decommission_final_state(0, false), DecommissionFinalState::Complete);
        assert_eq!(determine_decommission_final_state(1, false), DecommissionFinalState::Failed);
        assert_eq!(determine_decommission_final_state(0, true), DecommissionFinalState::Failed);
    }

    #[test]
    fn lifecycle_action_removes_data_movement_version_rejects_delete_marker_action() {
        assert!(!lifecycle_action_removes_data_movement_version(IlmAction::DeleteAction));
    }

    #[test]
    fn lifecycle_action_removes_data_movement_version_accepts_version_delete_actions() {
        assert!(lifecycle_action_removes_data_movement_version(IlmAction::DeleteVersionAction));
        assert!(lifecycle_action_removes_data_movement_version(IlmAction::DeleteAllVersionsAction));
        assert!(lifecycle_action_removes_data_movement_version(
            IlmAction::DelMarkerDeleteAllVersionsAction
        ));
    }

    #[test]
    fn lifecycle_action_skips_heal_version_for_every_delete_action() {
        assert!(lifecycle_action_skips_heal_version(IlmAction::DeleteAction));
        assert!(lifecycle_action_skips_heal_version(IlmAction::DeleteVersionAction));
        assert!(lifecycle_action_skips_heal_version(IlmAction::DeleteRestoredAction));
        assert!(lifecycle_action_skips_heal_version(IlmAction::DeleteRestoredVersionAction));
        assert!(lifecycle_action_skips_heal_version(IlmAction::DeleteAllVersionsAction));
        assert!(lifecycle_action_skips_heal_version(IlmAction::DelMarkerDeleteAllVersionsAction));
        assert!(!lifecycle_action_skips_heal_version(IlmAction::TransitionAction));
        assert!(!lifecycle_action_skips_heal_version(IlmAction::TransitionVersionAction));
        assert!(!lifecycle_action_skips_heal_version(IlmAction::NoneAction));
    }

    #[test]
    fn resolve_data_movement_lifecycle_expiry_result_allows_dry_run_skip() {
        let skip = resolve_data_movement_lifecycle_expiry_result(IlmAction::DeleteVersionAction, false, false)
            .expect("dry-run lifecycle evaluation should not require expiry enqueue");

        assert!(skip);
    }

    #[test]
    fn resolve_data_movement_lifecycle_expiry_result_rejects_apply_failure() {
        let err = resolve_data_movement_lifecycle_expiry_result(IlmAction::DeleteVersionAction, true, false)
            .expect_err("failed lifecycle expiry enqueue should not be treated as skipped");

        assert!(err.to_string().contains("failed to apply lifecycle expiry action"));
    }

    #[test]
    fn decommission_copy_cleanup_safe_error_accepts_missing_source_errors() {
        assert!(is_decommission_copy_cleanup_safe_error(&Error::ObjectNotFound(
            "bucket".to_string(),
            "object".to_string()
        )));
        assert!(is_decommission_copy_cleanup_safe_error(&Error::VersionNotFound(
            "bucket".to_string(),
            "object".to_string(),
            "version".to_string()
        )));
    }

    #[test]
    fn decommission_free_version_attempt_treats_missing_source_as_consumed() {
        let attempt =
            classify_decommission_free_version_attempt(Err(Error::ObjectNotFound("bucket".to_string(), "object".to_string())));

        assert!(matches!(attempt, DecommissionFreeVersionAttempt::Consumed));
    }

    #[test]
    fn decommission_free_version_attempt_preserves_capacity_failure() {
        let attempt = classify_decommission_free_version_attempt(Err(Error::DiskFull));

        assert!(matches!(attempt, DecommissionFreeVersionAttempt::CapacityFailure(Error::DiskFull)));
    }

    #[test]
    fn decommission_delete_marker_copy_error_rejects_data_movement_overwrite() {
        let err = Error::DataMovementOverwriteErr("bucket".to_string(), "object".to_string(), "version".to_string());

        assert!(!is_decommission_copy_cleanup_safe_error(&err));
    }

    #[test]
    fn decommission_remote_tiered_copy_error_rejects_data_movement_overwrite() {
        let err = Error::DataMovementOverwriteErr("bucket".to_string(), "object".to_string(), "version".to_string());

        assert!(!is_decommission_copy_cleanup_safe_error(&err));
    }

    #[test]
    fn decommission_target_capacity_error_accepts_direct_capacity_errors() {
        assert!(is_decommission_target_capacity_error(&Error::DiskFull));
        assert!(is_decommission_target_capacity_error(&Error::StorageFull));
    }

    /// The decommission loop classifies errors that came back through a
    /// data-movement stage wrapper. Before backlog#1827 T2 the wrapper flattened
    /// everything into `Error::other(String)`, so these two classifiers had to
    /// match on rendered text; now the wrapped error is recoverable by type.
    #[test]
    fn decommission_classifiers_see_through_a_stage_wrapper() {
        let wrap = |inner: Error| {
            data_movement::data_movement_stage_error_for_test("decommission_object", "put_object", "bucket-a", "object-a", inner)
        };

        // Capacity: the target pool filling up must still stop the loop.
        assert!(is_decommission_target_capacity_error(&wrap(Error::DiskFull)));
        assert!(is_decommission_target_capacity_error(&wrap(Error::StorageFull)));
        assert!(!is_decommission_target_capacity_error(&wrap(Error::SlowDown)));

        // Cleanup safety: a not-found surfacing from inside a stage is the same
        // condition as one surfacing directly, so the source entry stays
        // eligible for cleanup.
        let not_found = Error::ObjectNotFound("bucket-a".to_string(), "object-a".to_string());
        assert!(is_decommission_copy_cleanup_safe_error(&not_found));
        assert!(is_decommission_copy_cleanup_safe_error(&wrap(not_found)));
        assert!(!is_decommission_copy_cleanup_safe_error(&wrap(Error::SlowDown)));
    }

    #[test]
    fn decommission_target_capacity_error_accepts_wrapped_capacity_errors() {
        let disk_full = Error::other(format!("decommission_object: put_object failed for bucket/object: {}", Error::DiskFull));
        let storage_full = Error::other(format!(
            "decommission_object: put_object failed for bucket/object: {}",
            Error::StorageFull
        ));

        assert!(is_decommission_target_capacity_error(&disk_full));
        assert!(is_decommission_target_capacity_error(&storage_full));
    }

    #[test]
    fn decommission_target_capacity_error_rejects_unrelated_errors() {
        assert!(!is_decommission_target_capacity_error(&Error::SlowDown));
    }

    #[test]
    fn should_skip_decommission_delete_marker_characterizes_empty_marker_without_replication() {
        let version = rustfs_filemeta::FileInfo {
            deleted: true,
            ..Default::default()
        };

        assert!(should_skip_decommission_delete_marker(&version, 1, false));
    }

    #[test]
    fn should_skip_decommission_delete_marker_characterizes_replication_configured() {
        let version = rustfs_filemeta::FileInfo {
            deleted: true,
            ..Default::default()
        };

        assert!(!should_skip_decommission_delete_marker(&version, 1, true));
    }

    #[test]
    fn should_skip_decommission_delete_marker_rejects_non_deleted_versions() {
        let version = rustfs_filemeta::FileInfo::default();

        assert!(!should_skip_decommission_delete_marker(&version, 1, false));
    }

    #[test]
    fn should_skip_decommission_delete_marker_rejects_multiple_remaining_versions() {
        let version = rustfs_filemeta::FileInfo {
            deleted: true,
            ..Default::default()
        };

        assert!(!should_skip_decommission_delete_marker(&version, 2, false));
    }

    #[test]
    fn decommission_delete_marker_opts_preserves_replication_state() {
        let mod_time = OffsetDateTime::now_utc();
        let version = rustfs_filemeta::FileInfo {
            mod_time: Some(mod_time),
            replication_state_internal: Some(crate::bucket::replication::replication_state_to_filemeta(&ReplicationState {
                replica_status: ReplicationStatusType::Replica,
                delete_marker: true,
                replicate_decision_str: "existing".to_string(),
                ..Default::default()
            })),
            ..Default::default()
        };

        let incarnation = uuid::Uuid::new_v4();
        let opts = decommission_delete_marker_opts(&version, Some("version-id".to_string()), 7, Some(incarnation));
        let replication = opts.delete_replication.expect("replication state should be preserved");

        assert!(opts.versioned);
        assert!(opts.data_movement);
        assert!(opts.delete_marker);
        assert!(opts.skip_decommissioned);
        assert_eq!(opts.src_pool_idx, 7);
        assert_eq!(opts.version_id.as_deref(), Some("version-id"));
        assert_eq!(opts.mod_time, Some(mod_time));
        assert_eq!(opts.expected_bucket_incarnation_id, Some(incarnation));
        assert_eq!(replication.replica_status, ReplicationStatusType::Replica);
        assert!(replication.delete_marker);
        assert_eq!(replication.replicate_decision_str, "existing");
    }

    #[test]
    fn decommission_delete_marker_opts_preserves_suspended_null_version() {
        let version = rustfs_filemeta::FileInfo {
            deleted: true,
            ..Default::default()
        };
        let opts = decommission_delete_marker_opts(&version, None, 7, None);

        assert!(!opts.versioned);
        assert!(opts.version_suspended);
        assert_eq!(opts.version_id.as_deref(), Some(uuid::Uuid::nil().to_string().as_str()));
    }

    #[test]
    fn test_decommission_object_migration_read_opts_are_raw_data_movement() {
        let opts = decommission_object_migration_read_opts(Some("vid-1".to_string()));

        assert_eq!(opts.version_id.as_deref(), Some("vid-1"));
        assert!(opts.no_lock);
        assert!(opts.data_movement);
        assert!(opts.raw_data_movement_read);
        assert!(opts.skip_rebalancing);
        assert!(opts.skip_decommissioned);
    }

    #[test]
    fn decommission_remote_tiered_opts_preserves_versioning_context() {
        let mod_time = OffsetDateTime::now_utc();
        let version = rustfs_filemeta::FileInfo {
            mod_time: Some(mod_time),
            metadata: HashMap::from([("x-amz-meta-key".to_string(), "value".to_string())]),
            ..Default::default()
        };

        let incarnation = uuid::Uuid::new_v4();
        let opts = decommission_remote_tiered_opts(&version, Some("version-id".to_string()), 9, Some(incarnation));

        assert!(opts.versioned);
        assert!(opts.data_movement);
        assert_eq!(opts.src_pool_idx, 9);
        assert_eq!(opts.version_id.as_deref(), Some("version-id"));
        assert_eq!(opts.mod_time, Some(mod_time));
        assert_eq!(opts.user_defined.get("x-amz-meta-key").map(String::as_str), Some("value"));
        assert!(opts.include_part_checksums);
        assert!(opts.http_preconditions.is_some());
        assert_eq!(opts.expected_bucket_incarnation_id, Some(incarnation));
        assert!(!opts.incl_free_versions);

        let mut free_version = version;
        free_version.set_tier_free_version();
        let free_opts = decommission_remote_tiered_opts(&free_version, Some("free-version-id".to_string()), 9, Some(incarnation));
        assert!(free_opts.incl_free_versions);
    }

    #[test]
    fn decommission_terminal_state_transitions_update_start_time() {
        let start_time = OffsetDateTime::now_utc();
        let build_pool_meta = || PoolMeta {
            version: POOL_META_VERSION,
            pools: vec![PoolStatus {
                id: 0,
                cmd_line: "/tmp/pool".to_string(),
                last_update: start_time,
                decommission: Some(PoolDecommissionInfo {
                    start_time: Some(start_time),
                    ..Default::default()
                }),
            }],
            dont_save: true,
        };

        let mut pool_meta = build_pool_meta();
        assert!(pool_meta.decommission_failed(0));
        assert_eq!(pool_meta.pools[0].decommission.as_ref().and_then(|info| info.start_time), None);

        let mut pool_meta = build_pool_meta();
        assert!(pool_meta.decommission_complete(0));
        assert_eq!(
            pool_meta.pools[0].decommission.as_ref().and_then(|info| info.start_time),
            Some(start_time)
        );

        let mut pool_meta = build_pool_meta();
        assert!(pool_meta.decommission_cancel(0));
        assert_eq!(pool_meta.pools[0].decommission.as_ref().and_then(|info| info.start_time), None);

        let mut pool_meta = build_pool_meta();
        assert!(pool_meta.decommission_cancel(0));
        assert!(!pool_meta.decommission_complete(0));

        let mut pool_meta = build_pool_meta();
        assert!(pool_meta.decommission_failed(0));
        assert!(!pool_meta.decommission_complete(0));
    }

    #[test]
    fn pool_meta_persists_decommission_resume_queues() {
        let start_time = OffsetDateTime::now_utc();
        let unresolved_entry = DecommissionUnresolvedEntry {
            bucket: "bucket-b".to_string(),
            object: "prefix/unresolved.txt".to_string(),
            pool_index: 0,
            set_index: 1,
            source_generation: start_time,
            candidate_count: 2,
            disk_error_count: 1,
            observed_at: start_time,
            reason: "metadata_resolution_failed".to_string(),
        };
        let pool_meta = PoolMeta {
            version: POOL_META_VERSION,
            pools: vec![PoolStatus {
                id: 1,
                cmd_line: "/data/pool1/disk{1...4}".to_string(),
                last_update: start_time,
                decommission: Some(PoolDecommissionInfo {
                    start_time: Some(start_time),
                    queued: true,
                    queued_buckets: vec!["bucket-a".to_string(), "bucket-b/prefix".to_string()],
                    decommissioned_buckets: vec!["bucket-done".to_string()],
                    bucket: "bucket-b".to_string(),
                    prefix: "prefix".to_string(),
                    object: "object.txt".to_string(),
                    items_decommissioned: 7,
                    items_decommission_failed: 1,
                    bytes_done: 1024,
                    bytes_failed: 128,
                    terminal_reload_attempt_at: Some(start_time),
                    terminal_reload_failures: vec!["complete_decommission: peer node-a failed".to_string()],
                    unresolved_entries: vec![unresolved_entry.clone()],
                    ..Default::default()
                }),
            }],
            dont_save: false,
        };

        let mut buf = Vec::new();
        PersistedPoolMeta::from(&pool_meta)
            .serialize(&mut Serializer::new(&mut buf))
            .expect("pool meta should serialize");

        let mut deserializer = Deserializer::new(Cursor::new(&buf));
        let restored: PoolMeta = PersistedPoolMeta::deserialize(&mut deserializer)
            .expect("pool meta should deserialize")
            .try_into()
            .expect("pool meta should validate");

        let restored_decommission = restored.pools[0]
            .decommission
            .as_ref()
            .expect("decommission info should survive round-trip");
        assert_eq!(
            restored_decommission.queued_buckets,
            vec!["bucket-a".to_string(), "bucket-b/prefix".to_string()]
        );
        assert_eq!(restored_decommission.decommissioned_buckets, vec!["bucket-done".to_string()]);
        assert_eq!(restored_decommission.bucket, "bucket-b");
        assert_eq!(restored_decommission.prefix, "prefix");
        assert_eq!(restored_decommission.object, "object.txt");
        assert!(restored_decommission.stage.is_empty());
        assert_eq!(restored_decommission.items_decommissioned, 7);
        assert_eq!(restored_decommission.items_decommission_failed, 1);
        assert_eq!(restored_decommission.bytes_done, 1024);
        assert_eq!(restored_decommission.bytes_failed, 128);
        assert_eq!(restored_decommission.terminal_reload_attempt_at, Some(start_time));
        assert_eq!(
            restored_decommission.terminal_reload_failures,
            vec!["complete_decommission: peer node-a failed".to_string()]
        );
        assert_eq!(restored_decommission.unresolved_entries, vec![unresolved_entry]);
        assert!(restored_decommission.queued);
        assert_eq!(restored_decommission.items_since_last_progress_save(), 0);
    }

    #[test]
    fn pool_meta_records_decommission_terminal_reload_failure_once() {
        let start_time = OffsetDateTime::now_utc();
        let mut pool_meta = PoolMeta {
            version: POOL_META_VERSION,
            pools: vec![PoolStatus {
                id: 1,
                cmd_line: "/data/pool1/disk{1...4}".to_string(),
                last_update: start_time,
                decommission: Some(PoolDecommissionInfo {
                    complete: true,
                    ..Default::default()
                }),
            }],
            dont_save: false,
        };

        assert!(
            pool_meta
                .record_decommission_terminal_reload_failure(0, "complete_decommission", "peer node-a failed".to_string())
                .expect("terminal reload failure should be recorded")
        );
        assert!(
            !pool_meta
                .record_decommission_terminal_reload_failure(0, "complete_decommission", "peer node-a failed".to_string())
                .expect("duplicate terminal reload failure should be ignored")
        );

        let decommission = pool_meta.pools[0].decommission.as_ref().expect("decommission should exist");
        assert!(decommission.terminal_reload_attempt_at.is_some());
        assert_eq!(
            decommission.terminal_reload_failures,
            vec!["complete_decommission: peer node-a failed".to_string()]
        );
    }

    #[test]
    fn pool_meta_decode_supports_legacy_payload() {
        let start_time = OffsetDateTime::now_utc();
        let legacy_meta = LegacyPoolMeta {
            version: POOL_META_V1_VERSION,
            pools: vec![LegacyPoolStatus {
                id: 3,
                cmd_line: "/legacy/pool".to_string(),
                last_update: start_time,
                decommission: Some(LegacyPoolDecommissionInfo {
                    start_time: Some(start_time),
                    items_decommissioned: 9,
                    items_decommission_failed: 2,
                    bytes_done: 2048,
                    bytes_failed: 256,
                    ..Default::default()
                }),
            }],
            dont_save: true,
        };

        let mut legacy_payload = Vec::new();
        legacy_meta
            .serialize(&mut Serializer::new(&mut legacy_payload))
            .expect("legacy payload should serialize");

        // New persisted schema has fewer top-level fields and should not decode this legacy struct payload.
        let persisted_decode: std::result::Result<PersistedPoolMeta, _> = rmp_serde::from_slice(&legacy_payload);
        assert!(persisted_decode.is_err());

        let decoded =
            PoolMeta::decode_pool_meta_payload(POOL_META_V1_VERSION, &legacy_payload).expect("legacy payload should decode");
        assert_eq!(decoded.version, POOL_META_V1_VERSION);
        assert!(!decoded.dont_save, "runtime-only flag should reset on load");
        assert_eq!(decoded.pools.len(), 1);
        assert_eq!(decoded.pools[0].id, 3);
        assert_eq!(decoded.pools[0].cmd_line, "/legacy/pool");
        assert_eq!(decoded.pools[0].last_update, start_time);

        let decommission = decoded.pools[0].decommission.as_ref().expect("decommission should decode");
        assert_eq!(decommission.start_time, Some(start_time));
        assert_eq!(decommission.items_decommissioned, 9);
        assert_eq!(decommission.items_decommission_failed, 2);
        assert_eq!(decommission.bytes_done, 2048);
        assert_eq!(decommission.bytes_failed, 256);
        assert_eq!(decommission.items_since_last_progress_save(), 0);
        // These fields were skipped in legacy payload and should be defaulted.
        assert!(decommission.queued_buckets.is_empty());
        assert!(decommission.decommissioned_buckets.is_empty());
        assert!(decommission.bucket.is_empty());
        assert!(decommission.prefix.is_empty());
        assert!(decommission.object.is_empty());
        assert!(decommission.unresolved_entries.is_empty());
    }

    #[test]
    fn pool_meta_decode_rejects_unknown_legacy_fields() {
        #[derive(Serialize)]
        struct LegacyPoolMetaWithUnknownField {
            version: u16,
            pools: Vec<LegacyPoolStatus>,
            dont_save: bool,
            unexpected: bool,
        }

        let payload = rmp_serde::to_vec_named(&LegacyPoolMetaWithUnknownField {
            version: POOL_META_V1_VERSION,
            pools: Vec::new(),
            dont_save: true,
            unexpected: true,
        })
        .expect("legacy pool metadata with unknown field should serialize");

        let err = PoolMeta::decode_pool_meta_payload(POOL_META_V1_VERSION, payload.as_slice())
            .expect_err("unknown legacy pool metadata field should fail decode");
        let rendered = err.to_string();
        assert!(rendered.contains("PoolMeta v1 decode failed for both persisted and legacy formats"));
        assert!(rendered.contains("unknown field") || rendered.contains("missing field"));
    }

    #[test]
    fn pool_meta_decode_rejects_unknown_persisted_fields() {
        #[derive(Serialize)]
        struct PersistedPoolMetaWithUnknownField {
            version: u16,
            pools: Vec<PersistedPoolStatus>,
            unexpected: bool,
        }

        let payload = rmp_serde::to_vec_named(&PersistedPoolMetaWithUnknownField {
            version: POOL_META_VERSION,
            pools: Vec::new(),
            unexpected: true,
        })
        .expect("pool metadata with unknown field should serialize");

        let err = PoolMeta::decode_pool_meta_payload(POOL_META_VERSION, payload.as_slice())
            .expect_err("unknown persisted pool metadata field should fail decode");
        let rendered = err.to_string();
        assert!(rendered.contains("PoolMeta v2 decode failed"));
        assert!(rendered.contains("unknown field") || rendered.contains("missing field"));
    }

    #[test]
    fn pool_meta_decode_rejects_missing_critical_persisted_fields() {
        #[derive(Serialize)]
        struct PersistedPoolMetaWithoutPools {
            version: u16,
        }

        let payload = rmp_serde::to_vec_named(&PersistedPoolMetaWithoutPools {
            version: POOL_META_VERSION,
        })
        .expect("pool metadata without pools should serialize");

        let err = PoolMeta::decode_pool_meta_payload(POOL_META_VERSION, payload.as_slice())
            .expect_err("missing persisted pool metadata pools should fail decode");
        assert!(err.to_string().contains("PoolMeta v2 decode failed"));
    }

    #[test]
    fn pool_meta_decode_rejects_unknown_decommission_fields() {
        #[derive(Serialize)]
        struct PersistedPoolStatusWithUnknownDecommission {
            #[serde(rename = "id")]
            id: usize,
            #[serde(rename = "cmdline")]
            cmd_line: String,
            #[serde(rename = "lastUpdate", with = "time::serde::rfc3339")]
            last_update: OffsetDateTime,
            #[serde(rename = "decommissionInfo")]
            decommission: Option<PersistedPoolDecommissionInfoWithUnknownField>,
        }

        #[derive(Serialize)]
        struct PersistedPoolDecommissionInfoWithUnknownField {
            #[serde(rename = "startTime", with = "time::serde::rfc3339::option")]
            start_time: Option<OffsetDateTime>,
            #[serde(rename = "startSize")]
            start_size: usize,
            #[serde(rename = "totalSize")]
            total_size: usize,
            #[serde(rename = "currentSize")]
            current_size: usize,
            #[serde(rename = "complete")]
            complete: bool,
            #[serde(rename = "failed")]
            failed: bool,
            #[serde(rename = "canceled")]
            canceled: bool,
            #[serde(rename = "queuedBuckets")]
            queued_buckets: Vec<String>,
            #[serde(rename = "decommissionedBuckets")]
            decommissioned_buckets: Vec<String>,
            #[serde(rename = "bucket")]
            bucket: String,
            #[serde(rename = "prefix")]
            prefix: String,
            #[serde(rename = "object")]
            object: String,
            #[serde(rename = "objectsDecommissioned")]
            items_decommissioned: usize,
            #[serde(rename = "objectsDecommissionedFailed")]
            items_decommission_failed: usize,
            #[serde(rename = "bytesDecommissioned")]
            bytes_done: usize,
            #[serde(rename = "bytesDecommissionedFailed")]
            bytes_failed: usize,
            #[serde(rename = "unexpected")]
            unexpected: bool,
        }

        #[derive(Serialize)]
        struct PersistedPoolMetaWithUnknownDecommission {
            version: u16,
            pools: Vec<PersistedPoolStatusWithUnknownDecommission>,
        }

        let start_time = OffsetDateTime::now_utc();
        let payload = rmp_serde::to_vec_named(&PersistedPoolMetaWithUnknownDecommission {
            version: POOL_META_VERSION,
            pools: vec![PersistedPoolStatusWithUnknownDecommission {
                id: 0,
                cmd_line: "/data/pool".to_string(),
                last_update: start_time,
                decommission: Some(PersistedPoolDecommissionInfoWithUnknownField {
                    start_time: Some(start_time),
                    start_size: 0,
                    total_size: 0,
                    current_size: 0,
                    complete: false,
                    failed: false,
                    canceled: false,
                    queued_buckets: Vec::new(),
                    decommissioned_buckets: Vec::new(),
                    bucket: String::new(),
                    prefix: String::new(),
                    object: String::new(),
                    items_decommissioned: 0,
                    items_decommission_failed: 0,
                    bytes_done: 0,
                    bytes_failed: 0,
                    unexpected: true,
                }),
            }],
        })
        .expect("pool metadata with unknown decommission field should serialize");

        let err = PoolMeta::decode_pool_meta_payload(POOL_META_VERSION, payload.as_slice())
            .expect_err("unknown persisted decommission metadata field should fail decode");
        assert!(err.to_string().contains("PoolMeta v2 decode failed"));
    }

    #[test]
    fn pool_meta_decode_rejects_invalid_decommission_terminal_state() {
        let start_time = OffsetDateTime::now_utc();
        let persisted_meta = PersistedPoolMeta {
            version: POOL_META_VERSION,
            pools: vec![PersistedPoolStatus {
                id: 1,
                cmd_line: "/data/pool1/disk{1...4}".to_string(),
                last_update: start_time,
                decommission: Some(PersistedPoolDecommissionInfo {
                    start_time: Some(start_time),
                    complete: true,
                    failed: true,
                    canceled: false,
                    ..Default::default()
                }),
            }],
        };

        let mut payload = Vec::new();
        persisted_meta
            .serialize(&mut Serializer::new(&mut payload))
            .expect("persisted payload should serialize");

        let err = PoolMeta::decode_pool_meta_payload(POOL_META_VERSION, &payload)
            .expect_err("invalid terminal state should fail decode");
        assert!(err.to_string().contains("invalid decommission terminal state"));
    }

    #[test]
    fn pool_meta_decode_rejects_invalid_legacy_decommission_terminal_state() {
        let start_time = OffsetDateTime::now_utc();
        let legacy_meta = LegacyPoolMeta {
            version: POOL_META_V1_VERSION,
            pools: vec![LegacyPoolStatus {
                id: 1,
                cmd_line: "/legacy/pool".to_string(),
                last_update: start_time,
                decommission: Some(LegacyPoolDecommissionInfo {
                    start_time: Some(start_time),
                    complete: true,
                    failed: false,
                    canceled: true,
                    ..Default::default()
                }),
            }],
            dont_save: false,
        };

        let mut payload = Vec::new();
        legacy_meta
            .serialize(&mut Serializer::new(&mut payload))
            .expect("legacy payload should serialize");

        let err = PoolMeta::decode_pool_meta_payload(POOL_META_V1_VERSION, &payload)
            .expect_err("invalid legacy terminal state should fail decode");
        assert!(err.to_string().contains("invalid decommission terminal state"));
    }
}

// impl Fn(MetaCacheEntry) -> impl Future<Output = Result<(), Error>>

pub type ListCallback = Arc<dyn Fn(MetaCacheEntry) -> BoxFuture<'static, ()> + Send + Sync + 'static>;

const DECOMMISSION_ENTRY_QUEUE_HARD_CAP: usize = 256;

struct QueuedDecommissionEntry {
    entry: MetaCacheEntry,
    queue_permit: OwnedSemaphorePermit,
}

enum DecommissionEntryEnqueueResult {
    Enqueued,
    Canceled,
    Closed,
}

fn decommission_entry_queue_capacity(worker_limit: usize) -> usize {
    worker_limit.saturating_mul(2).clamp(1, DECOMMISSION_ENTRY_QUEUE_HARD_CAP)
}

async fn enqueue_decommission_entry(
    rx: &CancellationToken,
    outstanding: &Arc<Semaphore>,
    tx: &mpsc::Sender<QueuedDecommissionEntry>,
    entry: MetaCacheEntry,
) -> DecommissionEntryEnqueueResult {
    let queue_permit = match tokio::select! {
        biased;
        _ = rx.cancelled() => return DecommissionEntryEnqueueResult::Canceled,
        permit = outstanding.clone().acquire_owned() => permit,
    } {
        Ok(permit) => permit,
        Err(_) => return DecommissionEntryEnqueueResult::Closed,
    };

    let queued = QueuedDecommissionEntry { entry, queue_permit };
    tokio::select! {
        biased;
        _ = rx.cancelled() => DecommissionEntryEnqueueResult::Canceled,
        result = tx.send(queued) => {
            if result.is_ok() {
                DecommissionEntryEnqueueResult::Enqueued
            } else {
                DecommissionEntryEnqueueResult::Closed
            }
        }
    }
}

async fn drain_decommission_entry_queue(rx: &CancellationToken, outstanding: &Arc<Semaphore>, capacity: usize) -> bool {
    let Ok(permits) = u32::try_from(capacity) else {
        return true;
    };

    tokio::select! {
        _ = rx.cancelled() => true,
        result = outstanding.acquire_many(permits) => result.is_err(),
    }
}

async fn record_decommission_entry_error(
    entry_error: &Arc<tokio::sync::Mutex<Option<Error>>>,
    rx: &CancellationToken,
    err: Error,
) -> bool {
    if rx.is_cancelled() {
        return false;
    }

    let mut first_err = entry_error.lock().await;
    if first_err.is_none() && !rx.is_cancelled() {
        *first_err = Some(err);
        rx.cancel();
        return true;
    }
    false
}

fn ensure_decommission_unresolved_verification_disk_count(
    expected: usize,
    actual: usize,
    pool_index: usize,
    set_index: usize,
) -> Result<()> {
    if actual == expected {
        return Ok(());
    }
    Err(Error::other(format!(
        "decommission unresolved-entry verification for pool {pool_index} set {set_index} requires all {expected} source disks, but only {actual} are online"
    )))
}

impl SetDisks {
    async fn decommission_unresolved_entry_absent_on_all_disks(
        &self,
        pool_index: usize,
        entry: &DecommissionUnresolvedEntry,
    ) -> Result<bool> {
        let (disks, _) = self.get_online_disks_with_healing(false).await;
        ensure_decommission_unresolved_verification_disk_count(self.set_drive_count, disks.len(), pool_index, entry.set_index)?;

        let object = encode_dir_object(&entry.object);
        let reads = join_all(disks.iter().map(|disk| disk.read_xl(&entry.bucket, &object, false))).await;
        let mut found = false;
        for read in reads {
            match read {
                Ok(_) => found = true,
                Err(DiskError::FileNotFound | DiskError::FileVersionNotFound | DiskError::VolumeNotFound) => {}
                Err(err) => {
                    return Err(Error::other(format!(
                        "decommission unresolved-entry verification failed for pool {pool_index} set {} path {}/{}: {err}",
                        entry.set_index, entry.bucket, entry.object
                    )));
                }
            }
        }
        Ok(!found)
    }

    #[tracing::instrument(skip(self, store, rx, cb_func, entry_error))]
    #[allow(clippy::too_many_arguments)]
    async fn list_objects_to_decommission(
        self: &Arc<Self>,
        store: Arc<ECStore>,
        rx: CancellationToken,
        bucket_info: DecomBucketInfo,
        cb_func: ListCallback,
        entry_error: Arc<tokio::sync::Mutex<Option<Error>>>,
        pool_index: usize,
        set_index: usize,
        source_generation: OffsetDateTime,
        require_all_disks: bool,
    ) -> Result<()> {
        let (disks, _) = self.get_online_disks_with_healing(false).await;
        ensure_decommission_listing_disks_available(!disks.is_empty(), &bucket_info.name)?;
        if require_all_disks {
            ensure_decommission_unresolved_verification_disk_count(self.set_drive_count, disks.len(), pool_index, set_index)?;
        }

        let listing_quorum = self.set_drive_count.div_ceil(2);

        let resolver = MetadataResolutionParams {
            dir_quorum: listing_quorum,
            obj_quorum: listing_quorum,
            bucket: bucket_info.name.clone(),
            ..Default::default()
        };

        let cb1 = cb_func.clone();
        let unresolved_error = entry_error.clone();
        let unresolved_rx = rx.clone();
        let unresolved_bucket = bucket_info.name.clone();
        let unresolved_prefix = bucket_info.prefix.clone();
        let unresolved_pool_index = pool_index;
        let unresolved_set_index = set_index;
        let unresolved_generation = source_generation;
        let unresolved_store = store;

        list_path_raw(
            rx,
            ListPathRawOptions {
                disks: disks.iter().cloned().map(Some).collect(),
                bucket: bucket_info.name.clone(),
                path: bucket_info.prefix.clone(),
                recursive: true,
                min_disks: listing_quorum,
                skip_walkdir_total_timeout: true,
                walkdir_stall_timeout: Some(DECOMMISSION_BACKGROUND_WALKDIR_STALL_TIMEOUT),
                agreed: Some(Box::new(move |entry: MetaCacheEntry| Box::pin(cb1(entry)))),
                partial: Some(Box::new(move |entries: MetaCacheEntries, errs: &[Option<DiskError>]| {
                    let resolver = resolver.clone();
                    let cb_func = cb_func.clone();
                    let bucket = unresolved_bucket.clone();
                    let prefix = unresolved_prefix.clone();
                    let unresolved_error = unresolved_error.clone();
                    let unresolved_rx = unresolved_rx.clone();
                    let unresolved_store = unresolved_store.clone();
                    let pool_index = unresolved_pool_index;
                    let set_index = unresolved_set_index;
                    let source_generation = unresolved_generation;
                    let disk_error_count = errs.iter().flatten().count();
                    if unresolved_rx.is_cancelled() {
                        return Box::pin(async {});
                    }

                    match resolve_decommission_partial_listing_entry(
                        entries,
                        resolver,
                        &bucket,
                        &prefix,
                        disk_error_count,
                        pool_index,
                        set_index,
                        source_generation,
                    ) {
                        Ok(entry) => {
                            warn!("decommission_pool: list_objects_to_decommission get {}", &entry.name);
                            Box::pin(async move {
                                cb_func(entry).await;
                            })
                        }
                        Err(unresolved_entry) => Box::pin(async move {
                            if unresolved_rx.is_cancelled() {
                                return;
                            }
                            let err = decommission_unresolved_listing_error(&unresolved_entry);
                            warn!(
                                event = EVENT_DECOMMISSION_BUCKET,
                                component = LOG_COMPONENT_ECSTORE,
                                subsystem = LOG_SUBSYSTEM_POOLS,
                                bucket = %bucket,
                                prefix = %prefix,
                                state = "unresolved_entry",
                                error = %err,
                                "Decommission listing failed closed on unresolved metadata"
                            );
                            let err = match unresolved_store
                                .persist_decommission_unresolved_entry(pool_index, source_generation, unresolved_entry)
                                .await
                            {
                                Ok(()) => err,
                                Err(ledger_err) => Error::other(format!("{err}; {ledger_err}")),
                            };
                            record_decommission_entry_error(&unresolved_error, &unresolved_rx, err).await;
                        }),
                    }
                })),
                ..Default::default()
            },
        )
        .await?;

        if let Some(err) = entry_error.lock().await.clone() {
            return Err(err);
        }

        Ok(())
    }
}

fn is_disk_online_state(state: &str) -> bool {
    // The disk state strings are produced from rustfs_utils::os::get_drive_stats or DiskError::to_string().
    // Conventionally, online is "ok"/"online" (may evolve). Be conservative:
    // - Treat empty as unknown -> include it (to avoid dropping capacity).
    // - Exclude explicit offline-ish states.
    let s = state.trim().to_lowercase();
    if s.is_empty() {
        return true;
    }
    if s.contains("offline") {
        return false;
    }
    if s.contains("not found") || s.contains("disk not found") {
        return false;
    }
    true
}

#[deprecated(since = "0.1.0", note = "Use fallback_total_capacity_dedup instead")]
#[allow(
    dead_code,
    reason = "superseded by the replacement named in the comment at pools.rs:5071 (backlog#1823)"
)]
fn fallback_total_capacity(disks: &[rustfs_madmin::Disk]) -> usize {
    fallback_total_capacity_dedup(disks)
}

#[deprecated(since = "0.1.0", note = "Use fallback_free_capacity_dedup instead")]
#[allow(
    dead_code,
    reason = "superseded by the replacement named in the comment at pools.rs:5071 (backlog#1823)"
)]
fn fallback_free_capacity(disks: &[rustfs_madmin::Disk]) -> usize {
    fallback_free_capacity_dedup(disks)
}

pub fn get_total_usable_capacity(disks: &[rustfs_madmin::Disk], info: &rustfs_madmin::StorageInfo) -> usize {
    // If backend info is missing or inconsistent, do a safe fallback to avoid reporting nonsense.
    if info.backend.standard_sc_data.is_empty() {
        return fallback_total_capacity_dedup(disks);
    }
    let mut capacity = 0usize;
    let mut matched_any = false;
    let mut counted_disks: HashSet<String> = HashSet::new();

    for disk in disks.iter() {
        if disk.pool_index < 0 {
            continue;
        }
        let pool_idx = disk.pool_index as usize;
        if info.backend.standard_sc_data.len() <= pool_idx {
            continue;
        }

        let usable_disks_per_set = info.backend.standard_sc_data[pool_idx];
        if usable_disks_per_set == 0 {
            continue;
        }

        if (disk.disk_index as usize) < usable_disks_per_set {
            // 🔧 Generate a unique identity using a combination of fields
            let disk_key = format!(
                "{}|{}|p{}s{}d{}",
                disk.endpoint,   // Node address
                disk.drive_path, // mount path
                disk.pool_index, // Pool index
                disk.set_index,  // Collection index
                disk.disk_index  // Disk index
            );
            debug!("get_total_usable_capacity disk_key: {}", disk_key);
            // 🔧 Only disks that have not been counted are counted towards capacity
            if counted_disks.insert(disk_key) {
                matched_any = true;
                capacity += disk.total_space as usize;
            } else {
                // Log duplicate disks: this likely indicates a configuration issue and should always be visible.
                warn!(
                    "Duplicate disk detected in capacity calculation: {} at {}",
                    disk.endpoint, disk.drive_path
                );
            }
        }
    }

    if matched_any {
        capacity
    } else {
        // Even if standard_sc_data exists, it might not match disk indexes due to upstream bugs.
        // Fallback to summing all online disks to prevent under-reporting.
        fallback_total_capacity_dedup(disks)
    }
}

pub fn get_total_usable_capacity_free(disks: &[rustfs_madmin::Disk], info: &rustfs_madmin::StorageInfo) -> usize {
    if info.backend.standard_sc_data.is_empty() {
        return fallback_free_capacity_dedup(disks);
    }

    let mut capacity = 0usize;
    let mut matched_any = false;
    let mut counted_disks: HashSet<String> = HashSet::new();

    for disk in disks.iter() {
        if disk.pool_index < 0 {
            continue;
        }
        let pool_idx = disk.pool_index as usize;
        if info.backend.standard_sc_data.len() <= pool_idx {
            continue;
        }

        let usable_disks_per_set = info.backend.standard_sc_data[pool_idx];
        if usable_disks_per_set == 0 {
            continue;
        }

        if (disk.disk_index as usize) < usable_disks_per_set {
            let disk_key = format!(
                "{}|{}|p{}s{}d{}",
                disk.endpoint, disk.drive_path, disk.pool_index, disk.set_index, disk.disk_index
            );

            if counted_disks.insert(disk_key) {
                matched_any = true;
                capacity += disk.available_space as usize;
            }
        }
    }

    if matched_any {
        capacity
    } else {
        fallback_free_capacity_dedup(disks)
    }
}

/// Total fallback capacity calculation with deweight
///
/// Replace original function: fallback_total_capacity()
pub(crate) fn fallback_total_capacity_dedup(disks: &[rustfs_madmin::Disk]) -> usize {
    let mut counted_disks: HashSet<String> = HashSet::new();
    let mut total = 0usize;

    for disk in disks.iter() {
        // Only online disks are counted
        if !is_disk_online_state(&disk.state) {
            continue;
        }

        // Use endpoint + drive_path as a unique identifier
        let disk_key = format!("{}|{}", disk.endpoint, disk.drive_path);

        // Capacity is counted only when the disk is encountered for the first time
        if counted_disks.insert(disk_key) {
            total += disk.total_space as usize;
        }
    }

    total
}

/// Remove the heavy fallback idle capacity calculation
///
/// Replace original function: fallback_free_capacity()
pub(crate) fn fallback_free_capacity_dedup(disks: &[rustfs_madmin::Disk]) -> usize {
    let mut counted_disks: HashSet<String> = HashSet::new();
    let mut total = 0usize;

    for disk in disks.iter() {
        if !is_disk_online_state(&disk.state) {
            continue;
        }

        let disk_key = format!("{}|{}", disk.endpoint, disk.drive_path);

        if counted_disks.insert(disk_key) {
            total += disk.available_space as usize;
        }
    }

    total
}

#[cfg(test)]
mod pools_tests {
    use super::DECOMMISSION_PROGRESS_SAVE_RETRY_BACKOFF;
    use super::record_decommission_entry_error;
    use super::resolve_decommission_listing_error;
    use super::resolve_decommission_partial_listing_entry;
    use super::{
        DECOMMISSION_DURABLE_ILM_RECEIPT_MAX_SIZE, DECOMMISSION_ENTRY_CONCURRENCY_DEFAULT_CAP,
        DECOMMISSION_ENTRY_CONCURRENCY_HARD_CAP, DECOMMISSION_ENTRY_QUEUE_HARD_CAP, DECOMMISSION_META_PREFIXES,
        DECOMMISSION_PROGRESS_SAVE_INTERVAL, DECOMMISSION_PROGRESS_SAVE_ITEM_THRESHOLD,
        DECOMMISSION_SOURCE_CHANGED_EXHAUSTION_LIMIT, DecomBucketInfo, DecommissionCanceler, DecommissionDurableIlmReceipt,
        DecommissionEntryEnqueueResult, DecommissionStartPoolState, DecommissionTerminalState, DecommissionUnresolvedEntry,
        ListCallback, POOL_META_IDENTITY_NAME, POOL_META_NAME, POOL_META_V1_VERSION, POOL_META_VERSION, PoolDecommissionInfo,
        PoolMeta, PoolMetaCasToken, PoolMetaPersistenceFence, PoolSpaceInfo, PoolStatus, QueuedDecommissionEntry,
        REBAL_META_NAME, acquire_pool_rebalance_activation_locks, apply_decommission_status_space_info,
        await_decommission_worker, bind_decommission_cancelers, bind_missing_decommission_cancelers,
        cancel_decommission_canceler, clamp_decommission_entry_concurrency, classify_decommission_terminal_state,
        count_decommission_item, decommission_cancel_signal_result, decommission_durable_ilm_receipt_path,
        decommission_durable_ilm_receipt_run_prefix, decommission_durable_ilm_receipt_run_token,
        decommission_entry_queue_capacity, decommission_item_size, decommission_meta_bucket_options,
        decommission_retry_backoff_delay, decommission_start_pool_state, decommission_unresolved_listing_error, dedup_indices,
        default_decommission_bucket_concurrency, default_decommission_entry_concurrency, drain_decommission_entry_queue,
        enqueue_decommission_entry, ensure_decommission_cancel_allowed, ensure_decommission_clear_allowed,
        ensure_decommission_generation, ensure_decommission_listing_disks_available, ensure_decommission_not_rebalancing,
        ensure_decommission_start_allowed, ensure_decommission_start_keeps_active_pool, ensure_decommission_start_local_leader,
        ensure_decommission_start_pool_states, ensure_decommission_start_rebalance_meta_allowed,
        ensure_decommission_start_target_capacity, ensure_decommission_terminal_operation_supported,
        ensure_decommission_unresolved_verification_disk_count, ensure_local_decommission_pool_leaders,
        ensure_pool_meta_write_fence, ensure_valid_decommission_pool_index, get_by_index, guard_decommission_cancelers,
        has_active_decommission_canceler, is_decommission_active, is_decommission_cancel_requested,
        load_decommission_entry_versions, local_decommission_queue_prefix, mark_decommission_bucket_done,
        merge_decommission_durable_ilm_receipts, merge_pool_meta_updates_for_save, merge_pool_status_refresh,
        missing_decommission_worker_prefix, observe_decommission_terminal_reload_result, pool_meta_has_active_decommission,
        publish_pool_meta_updates, read_pool_meta_replica, reconcile_decommission_meta_buckets,
        reconcile_decommission_unresolved_entries_for_completion, record_decommission_unresolved_entry,
        require_decommission_store, reserve_decommission_start_cancelers, resolve_decommission_bucket_state,
        resolve_decommission_check_after_list_result, resolve_decommission_entry_cleanup_delete_result,
        resolve_decommission_entry_exact_versions, resolve_decommission_entry_reload_result,
        resolve_decommission_listing_worker_result, resolve_decommission_optional_bucket_config_result,
        resolve_decommission_pool_meta_reload_result, resolve_decommission_preflight_heal_result,
        resolve_decommission_progress_save_result, resolve_decommission_terminal_mark_after_error_result,
        resolve_decommission_terminal_mark_result, resolve_decommission_update_after_result,
        resolve_start_decommission_pool_meta_reload_result, resumable_decommission_queue_indices,
        rollback_start_decommission_pool_meta, run_decommission_buckets_bounded, run_decommission_listing_with_retry,
        run_decommission_listing_with_retry_and_drain, run_decommission_phases, run_decommission_side_effect,
        save_pool_meta_object_cas, should_cleanup_decommission_source_entry, should_continue_decommission_queue,
        should_count_decommission_version_complete, should_fail_decommission_pool_after_exhausted_source_changed,
        should_preserve_decommission_canceled_state, should_reject_decommission_cancel_as_terminal,
        should_retry_decommission_cancel_reload, should_retry_decommission_listing, should_skip_canceled_decommission_routine,
        spawn_decommission_index_cancelers, split_decommission_buckets, take_and_cancel_decommission_canceler,
        take_decommission_canceler, track_decommission_current_object, track_decommission_current_object_stage,
        update_decommission_for_operation, validate_start_decommission_request, wait_decommission_retry_backoff,
        wait_decommission_worker_drain, with_decommission_entry_context,
    };
    use crate::bucket::lifecycle::{
        DurableIlmRecordCheckpoint,
        bucket_lifecycle_ops::{ManualTransitionQueueSnapshot, ManualTransitionRunOptions},
        manual_transition_job::{ManualTransitionJobRecord, manual_transition_job_record_object_name},
        validate_durable_ilm_record,
    };
    use crate::bucket::metadata_sys;
    use crate::data_movement;
    use crate::disk::{STORAGE_FORMAT_FILE, endpoint::Endpoint};
    use crate::error::{Error, StorageError};
    use crate::layout::endpoints::{EndpointServerPools, Endpoints, PoolEndpoints};
    use crate::runtime::instance::InstanceContext;
    use crate::services::rebalance::{RebalStatus, RebalanceInfo, RebalanceMeta, RebalanceStats};
    use crate::storage_api_contracts::bucket::{BucketOperations, MakeBucketOptions};
    use crate::storage_api_contracts::object::HTTPPreconditions;
    use crate::storage_api_contracts::{object::ObjectIO, range::HTTPRangeSpec};
    use crate::store::ECStore;
    use byteorder::{ByteOrder, LittleEndian};
    use rustfs_filemeta::{FileInfo, FileInfoVersions, MetaCacheEntry, ObjectPartInfo};
    use rustfs_filemeta::{MetaCacheEntries, MetadataResolutionParams};
    use rustfs_lock::{GlobalLockManager, LocalClient, LockRequest, LockType, NamespaceLock, ObjectKey};
    use rustfs_rio::Index;
    use std::future::Future;
    use std::io::Cursor;
    use std::sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };
    use std::task::{Context, Poll};
    use std::time::Duration as StdDuration;
    use time::{Duration, OffsetDateTime};
    use tokio::io::AsyncReadExt;
    use tokio::sync::Semaphore;
    use tokio_util::sync::CancellationToken;

    #[derive(Debug)]
    struct ActivationLockRecorder {
        lock_manager: Arc<rustfs_lock::GlobalLockManager>,
        owner: &'static str,
        resources: StdMutex<Vec<String>>,
    }

    #[async_trait::async_trait]
    impl crate::storage_api_contracts::namespace::NamespaceLocking for ActivationLockRecorder {
        type Error = Error;
        type NamespaceLock = rustfs_lock::NamespaceLockWrapper;

        async fn new_ns_lock(&self, bucket: &str, object: &str) -> crate::error::Result<Self::NamespaceLock> {
            self.resources
                .lock()
                .expect("activation lock recorder should not be poisoned")
                .push(object.to_string());
            Ok(rustfs_lock::NamespaceLockWrapper::new(
                rustfs_lock::NamespaceLock::with_local_manager(
                    "activation-lock-test".to_string(),
                    Arc::clone(&self.lock_manager),
                ),
                rustfs_lock::ObjectKey::new(bucket, object),
                self.owner.to_string(),
            ))
        }
    }

    fn noop_decommission_list_callback() -> ListCallback {
        Arc::new(|_| Box::pin(async {}))
    }

    fn decommission_worker_test_store(pool_meta: PoolMeta, cancelers: Vec<Option<DecommissionCanceler>>) -> Arc<ECStore> {
        let ctx = Arc::new(InstanceContext::new());
        let endpoint_pools = EndpointServerPools::default();
        Arc::new(ECStore {
            id: uuid::Uuid::new_v4(),
            disk_map: std::collections::HashMap::new(),
            pools: Vec::new(),
            peer_sys: crate::cluster::rpc::S3PeerSys::new_with_instance_ctx(&endpoint_pools, ctx.clone()),
            pool_meta: tokio::sync::RwLock::new(pool_meta),
            rebalance_meta: tokio::sync::RwLock::new(None),
            decommission_cancelers: tokio::sync::RwLock::new(cancelers),
            start_gate: tokio::sync::Mutex::new(()),
            pool_meta_save_gate: tokio::sync::Mutex::new(super::PoolMetaWriteState::for_test_bootstrap()),
            ctx,
            bucket_fence_registry: Arc::default(),
        })
    }

    #[derive(Debug)]
    struct PartialPoolMetaWriteStorage {
        fail_write: bool,
        fail_after_first_write: bool,
        pending_write: bool,
        write_started: tokio::sync::Notify,
        wrote: AtomicBool,
        revision: AtomicUsize,
        stored: StdMutex<Option<(Vec<u8>, String)>>,
        identity: StdMutex<Option<(Vec<u8>, String)>>,
    }

    #[async_trait::async_trait]
    impl ObjectIO for PartialPoolMetaWriteStorage {
        type Error = Error;
        type RangeSpec = HTTPRangeSpec;
        type HeaderMap = http::HeaderMap;
        type ObjectOptions = crate::object_api::ObjectOptions;
        type ObjectInfo = crate::object_api::ObjectInfo;
        type GetObjectReader = crate::object_api::GetObjectReader;
        type PutObjectReader = crate::object_api::PutObjReader;

        async fn get_object_reader(
            &self,
            bucket: &str,
            object: &str,
            _range: Option<Self::RangeSpec>,
            _h: Self::HeaderMap,
            _opts: &Self::ObjectOptions,
        ) -> std::result::Result<Self::GetObjectReader, Error> {
            let stored = if object == POOL_META_IDENTITY_NAME {
                &self.identity
            } else {
                &self.stored
            };
            let Some((data, etag)) = stored
                .lock()
                .expect("pool metadata test storage should not be poisoned")
                .clone()
            else {
                return Err(Error::FileNotFound);
            };
            Ok(crate::object_api::GetObjectReader {
                stream: Box::new(Cursor::new(data.clone())),
                object_info: crate::object_api::ObjectInfo {
                    bucket: bucket.to_string(),
                    name: object.to_string(),
                    size: data.len() as i64,
                    etag: Some(etag),
                    ..Default::default()
                },
                buffered_body: None,
                body_source: Default::default(),
            })
        }

        async fn put_object(
            &self,
            _bucket: &str,
            object: &str,
            data: &mut Self::PutObjectReader,
            opts: &Self::ObjectOptions,
        ) -> std::result::Result<Self::ObjectInfo, Error> {
            self.write_started.notify_one();
            if self.pending_write {
                std::future::pending().await
            }
            if object == POOL_META_NAME
                && (self.fail_write || (self.fail_after_first_write && self.revision.load(Ordering::SeqCst) > 0))
            {
                return Err(Error::Timeout);
            }
            let stored = if object == POOL_META_IDENTITY_NAME {
                &self.identity
            } else {
                &self.stored
            };
            let current_etag = stored
                .lock()
                .expect("pool metadata test storage should not be poisoned")
                .as_ref()
                .map(|(_, etag)| etag.clone());
            if opts
                .http_preconditions
                .as_ref()
                .and_then(HTTPPreconditions::if_none_match_value)
                == Some("*")
                && current_etag.is_some()
            {
                return Err(Error::PreconditionFailed);
            }
            if let Some(expected) = opts.http_preconditions.as_ref().and_then(HTTPPreconditions::if_match_value)
                && current_etag.as_deref() != Some(expected)
            {
                return Err(Error::PreconditionFailed);
            }
            let mut payload = Vec::new();
            data.stream.read_to_end(&mut payload).await?;
            let etag = format!("pool-meta-test-{}", self.revision.fetch_add(1, Ordering::SeqCst) + 1);
            *stored.lock().expect("pool metadata test storage should not be poisoned") = Some((payload, etag.clone()));
            self.wrote.store(true, Ordering::SeqCst);
            Ok(crate::object_api::ObjectInfo {
                etag: Some(etag),
                ..Default::default()
            })
        }
    }

    #[tokio::test]
    async fn test_pool_meta_cas_deterministically_rejects_stale_writer() {
        let storage = Arc::new(PartialPoolMetaWriteStorage {
            fail_write: false,
            fail_after_first_write: false,
            pending_write: false,
            write_started: tokio::sync::Notify::new(),
            wrote: AtomicBool::new(false),
            revision: AtomicUsize::new(0),
            stored: StdMutex::new(None),
            identity: StdMutex::new(None),
        });
        let stale_token = read_pool_meta_replica(storage.clone(), true).await.cas;
        assert!(matches!(&stale_token, PoolMetaCasToken::Missing));
        let winner = PoolMeta {
            version: POOL_META_VERSION,
            pools: vec![decommission_test_pool_status(0, None)],
            ..Default::default()
        }
        .encode_config_data_for_test()
        .expect("winning pool metadata should encode");
        let stale = PoolMeta {
            version: POOL_META_VERSION,
            pools: vec![decommission_test_pool_status(
                0,
                Some(PoolDecommissionInfo {
                    complete: true,
                    ..Default::default()
                }),
            )],
            ..Default::default()
        }
        .encode_config_data_for_test()
        .expect("stale pool metadata should encode");
        let fence = PoolMetaPersistenceFence::Distributed(None);

        save_pool_meta_object_cas(storage.clone(), POOL_META_NAME, winner.clone(), &stale_token, &fence, "prepare_cas")
            .await
            .expect("the first writer should create pool metadata");
        let err = save_pool_meta_object_cas(storage.clone(), POOL_META_NAME, stale, &stale_token, &fence, "prepare_cas")
            .await
            .expect_err("the second writer must not reuse the stale missing-object revision");

        assert_eq!(err, Error::PreconditionFailed);
        let stored = storage
            .stored
            .lock()
            .expect("pool metadata test storage should not be poisoned")
            .as_ref()
            .map(|(data, _)| data.clone())
            .expect("the winning pool metadata should remain stored");
        assert_eq!(stored, winner);
    }

    #[tokio::test]
    async fn test_pool_meta_v3_single_replica_commit_failure_recovers_on_restart() {
        let committed_replica = Arc::new(PartialPoolMetaWriteStorage {
            fail_write: false,
            fail_after_first_write: false,
            pending_write: false,
            write_started: tokio::sync::Notify::new(),
            wrote: AtomicBool::new(false),
            revision: AtomicUsize::new(0),
            stored: StdMutex::new(None),
            identity: StdMutex::new(None),
        });
        let pending_replica = Arc::new(PartialPoolMetaWriteStorage {
            fail_write: false,
            fail_after_first_write: true,
            pending_write: false,
            write_started: tokio::sync::Notify::new(),
            wrote: AtomicBool::new(false),
            revision: AtomicUsize::new(0),
            stored: StdMutex::new(None),
            identity: StdMutex::new(None),
        });
        let cluster_id = uuid::Uuid::new_v4();
        let fresh_bootstrap_nonce = uuid::Uuid::new_v4();
        let identity = super::encode_pool_meta_identity(super::PersistedPoolMetaIdentity {
            version: super::POOL_META_IDENTITY_VERSION,
            cluster_id,
            epoch: super::POOL_META_INITIAL_EPOCH,
            initialized: false,
            fresh_bootstrap_nonce: Some(fresh_bootstrap_nonce),
        })
        .expect("pending bootstrap identity should encode");
        *committed_replica
            .identity
            .lock()
            .expect("identity storage should not be poisoned") = Some((identity.clone(), "identity-0".to_string()));
        *pending_replica
            .identity
            .lock()
            .expect("identity storage should not be poisoned") = Some((identity, "identity-0".to_string()));
        let mut write_state = super::PoolMetaWriteState::for_startup(cluster_id, true);
        let snapshot = PoolMeta {
            version: super::POOL_META_GENERATION_VERSION,
            pools: vec![decommission_test_pool_status(
                0,
                Some(PoolDecommissionInfo {
                    queued: true,
                    ..Default::default()
                }),
            )],
            ..Default::default()
        };

        snapshot
            .save_no_lock_observing(vec![committed_replica.clone(), pending_replica.clone()], &mut write_state)
            .await
            .expect("one committed replica should durably complete the V3 transaction");

        let mut restarted = PoolMeta::default();
        let replica_state = restarted
            .load_no_lock_from_replicas(vec![committed_replica, pending_replica.clone()])
            .await
            .expect("restart should select the committed generation over a pending replica");
        assert_eq!(restarted.version, super::POOL_META_GENERATION_VERSION);
        assert!(restarted.pools[0].decommission.as_ref().is_some_and(|info| info.queued));
        assert!(replica_state.needs_repair);

        let mut pending_only = PoolMeta::default();
        pending_only
            .load_no_lock_from_replicas(vec![pending_replica])
            .await
            .expect("a prepare-only replica should expose the embedded predecessor");
        assert!(pending_only.pools.is_empty());
    }

    #[tokio::test]
    async fn test_partial_pool_meta_save_failure_blocks_following_side_effect() {
        let store = decommission_worker_test_store(PoolMeta::default(), Vec::new());
        let committed = Arc::new(PartialPoolMetaWriteStorage {
            fail_write: false,
            fail_after_first_write: false,
            pending_write: false,
            write_started: tokio::sync::Notify::new(),
            wrote: AtomicBool::new(false),
            revision: AtomicUsize::new(0),
            stored: StdMutex::new(None),
            identity: StdMutex::new(None),
        });
        let failed = Arc::new(PartialPoolMetaWriteStorage {
            fail_write: true,
            fail_after_first_write: false,
            pending_write: false,
            write_started: tokio::sync::Notify::new(),
            wrote: AtomicBool::new(false),
            revision: AtomicUsize::new(0),
            stored: StdMutex::new(None),
            identity: StdMutex::new(None),
        });
        let snapshot = PoolMeta {
            version: super::POOL_META_VERSION,
            pools: vec![decommission_test_pool_status(0, None)],
            ..Default::default()
        };

        {
            let mut save_guard = store.pool_meta_save_gate.lock().await;
            snapshot
                .save_no_lock_observing(vec![committed.clone(), failed], &mut save_guard)
                .await
                .expect_err("the second replica write should fail after the first commits");
        }
        assert!(committed.wrote.load(Ordering::SeqCst));

        let ran = Arc::new(AtomicBool::new(false));
        let ran_by_operation = ran.clone();
        let movement_gate = store.ctx.data_movement_operation_gate();
        let result: std::result::Result<(), Error> = store
            .run_guarded_decommission_side_effect(&CancellationToken::new(), &movement_gate, move || async move {
                ran_by_operation.store(true, Ordering::SeqCst);
                Ok(())
            })
            .await;

        assert!(result.is_err(), "a partial pool metadata save must latch the sticky safety gate");
        assert!(!ran.load(Ordering::SeqCst), "the side effect must not run after a partial save");
    }

    #[tokio::test]
    async fn test_cancelled_pool_meta_save_blocks_following_side_effect() {
        let store = decommission_worker_test_store(PoolMeta::default(), Vec::new());
        let committed = Arc::new(PartialPoolMetaWriteStorage {
            fail_write: false,
            fail_after_first_write: false,
            pending_write: false,
            write_started: tokio::sync::Notify::new(),
            wrote: AtomicBool::new(false),
            revision: AtomicUsize::new(0),
            stored: StdMutex::new(None),
            identity: StdMutex::new(None),
        });
        let pending = Arc::new(PartialPoolMetaWriteStorage {
            fail_write: false,
            fail_after_first_write: false,
            pending_write: true,
            write_started: tokio::sync::Notify::new(),
            wrote: AtomicBool::new(false),
            revision: AtomicUsize::new(0),
            stored: StdMutex::new(None),
            identity: StdMutex::new(None),
        });
        let snapshot = PoolMeta {
            version: super::POOL_META_VERSION,
            pools: vec![decommission_test_pool_status(0, None)],
            ..Default::default()
        };
        let save_store = store.clone();
        let save_committed = committed.clone();
        let save_pending = pending.clone();
        let save_task = tokio::spawn(async move {
            let mut save_guard = save_store.pool_meta_save_gate.lock().await;
            snapshot
                .save_no_lock_observing(vec![save_committed, save_pending], &mut save_guard)
                .await
        });

        tokio::time::timeout(StdDuration::from_secs(1), pending.write_started.notified())
            .await
            .expect("the second replica write should start");
        assert!(committed.wrote.load(Ordering::SeqCst));
        save_task.abort();
        assert!(
            save_task.await.expect_err("the save task should be cancelled").is_cancelled(),
            "the pending replica write should be aborted"
        );

        {
            let save_guard = store.pool_meta_save_gate.lock().await;
            save_guard
                .ensure_write_safe("cancelled pool metadata save")
                .expect_err("a cancelled replica update must latch the sticky safety gate");
        }

        let ran = Arc::new(AtomicBool::new(false));
        let ran_by_operation = ran.clone();
        let movement_gate = store.ctx.data_movement_operation_gate();
        let result: std::result::Result<(), Error> = store
            .run_guarded_decommission_side_effect(&CancellationToken::new(), &movement_gate, move || async move {
                ran_by_operation.store(true, Ordering::SeqCst);
                Ok(())
            })
            .await;

        assert!(result.is_err(), "a cancelled pool metadata save must block following side effects");
        assert!(!ran.load(Ordering::SeqCst), "the side effect must not run after a cancelled save");
    }

    #[tokio::test]
    async fn test_cancelled_pool_meta_publish_keeps_write_gate_blocked() {
        let store = decommission_worker_test_store(PoolMeta::default(), Vec::new());
        let committed = Arc::new(PartialPoolMetaWriteStorage {
            fail_write: false,
            fail_after_first_write: false,
            pending_write: false,
            write_started: tokio::sync::Notify::new(),
            wrote: AtomicBool::new(false),
            revision: AtomicUsize::new(0),
            stored: StdMutex::new(None),
            identity: StdMutex::new(None),
        });
        let snapshot = PoolMeta {
            version: super::POOL_META_VERSION,
            pools: vec![decommission_test_pool_status(0, None)],
            ..Default::default()
        };
        let publish_started = Arc::new(tokio::sync::Notify::new());
        let publish_release = Arc::new(tokio::sync::Notify::new());
        let save_store = store.clone();
        let save_committed = committed.clone();
        let task_publish_started = publish_started.clone();
        let task_publish_release = publish_release.clone();
        let save_task = tokio::spawn(async move {
            let mut save_guard = save_store.pool_meta_save_gate.lock().await;
            let outcome = snapshot
                .save_no_lock_armed(vec![save_committed], &mut save_guard, None, &[0])
                .await?;
            task_publish_started.notify_one();
            task_publish_release.notified().await;
            outcome.disarm();
            Ok::<(), Error>(())
        });

        tokio::time::timeout(StdDuration::from_secs(1), publish_started.notified())
            .await
            .expect("the replica save should complete before publication");
        assert!(committed.wrote.load(Ordering::SeqCst));
        save_task.abort();
        assert!(
            save_task
                .await
                .expect_err("the publication task should be cancelled")
                .is_cancelled(),
            "the task should be aborted while publication is pending"
        );

        let save_guard = store.pool_meta_save_gate.lock().await;
        save_guard
            .ensure_write_safe("cancelled pool metadata publication")
            .expect_err("cancellation after replica save but before publication must keep writes blocked");
    }

    #[tokio::test]
    async fn test_lost_pool_meta_fence_rejects_replica_write() {
        let client = Arc::new(LocalClient::with_manager(Arc::new(GlobalLockManager::new())));
        let lock = NamespaceLock::with_clients_and_quorum("pool-meta-fence-loss".to_string(), vec![client], 1);
        let request = LockRequest::new(
            ObjectKey::new(super::RUSTFS_META_BUCKET, super::POOL_META_NAME),
            LockType::Exclusive,
            "stale-writer",
        )
        .with_acquire_timeout(StdDuration::from_secs(1))
        .with_ttl(StdDuration::from_millis(50))
        .with_refresh_interval(StdDuration::from_millis(50));
        let guard = lock
            .acquire_guard(&request)
            .await
            .expect("pool metadata fence acquisition should not error")
            .expect("the stale writer should acquire the pool metadata fence");
        tokio::time::timeout(StdDuration::from_secs(2), guard.lock_lost_notified())
            .await
            .expect("the stale writer lease should expire");

        let storage = Arc::new(PartialPoolMetaWriteStorage {
            fail_write: false,
            fail_after_first_write: false,
            pending_write: false,
            write_started: tokio::sync::Notify::new(),
            wrote: AtomicBool::new(false),
            revision: AtomicUsize::new(0),
            stored: StdMutex::new(None),
            identity: StdMutex::new(None),
        });
        let snapshot = PoolMeta {
            version: super::POOL_META_VERSION,
            pools: vec![decommission_test_pool_status(0, None)],
            ..Default::default()
        };
        let mut write_state = super::PoolMetaWriteState::default();
        let err = snapshot
            .save_no_lock_armed(vec![storage.clone()], &mut write_state, guard.lock_lost_signal(), &[0])
            .await
            .expect_err("a writer must not persist after losing the distributed pool metadata fence");

        assert!(err.to_string().contains("distributed fence was lost"));
        assert!(!storage.wrote.load(Ordering::SeqCst), "the stale writer must not reach replica storage");
        write_state
            .ensure_write_safe("lost pool metadata fence")
            .expect_err("a lost distributed fence must latch the sticky write gate");
    }

    #[tokio::test]
    async fn test_pool_meta_fence_loss_after_publish_keeps_write_gate_blocked() {
        let client = Arc::new(LocalClient::with_manager(Arc::new(GlobalLockManager::new())));
        let lock = NamespaceLock::with_clients_and_quorum("pool-meta-publish-fence-loss".to_string(), vec![client], 1);
        let request = LockRequest::new(
            ObjectKey::new(super::RUSTFS_META_BUCKET, super::POOL_META_NAME),
            LockType::Exclusive,
            "publishing-writer",
        )
        .with_acquire_timeout(StdDuration::from_secs(1))
        .with_ttl(StdDuration::from_secs(1))
        .with_refresh_interval(StdDuration::from_secs(1));
        let guard = lock
            .acquire_guard(&request)
            .await
            .expect("pool metadata fence acquisition should not error")
            .expect("the publishing writer should acquire the pool metadata fence");
        let storage = Arc::new(PartialPoolMetaWriteStorage {
            fail_write: false,
            fail_after_first_write: false,
            pending_write: false,
            write_started: tokio::sync::Notify::new(),
            wrote: AtomicBool::new(false),
            revision: AtomicUsize::new(0),
            stored: StdMutex::new(None),
            identity: StdMutex::new(None),
        });
        let mut saved = PoolMeta {
            version: super::POOL_META_VERSION,
            pools: vec![decommission_test_pool_status(0, None)],
            ..Default::default()
        };
        saved.pools[0].last_update = OffsetDateTime::UNIX_EPOCH + Duration::seconds(1);
        let mut current = saved.clone();
        current.pools[0].last_update = OffsetDateTime::UNIX_EPOCH;
        let mut write_state = super::PoolMetaWriteState::for_test_bootstrap();
        let outcome = saved
            .save_no_lock_armed(vec![storage], &mut write_state, guard.lock_lost_signal(), &[0])
            .await
            .expect("the replica save should finish while the fence is valid");

        ensure_pool_meta_write_fence(&guard, "test pool metadata publish")
            .expect("the fence should remain valid before publication");
        publish_pool_meta_updates(&mut current, &saved, &[0]);
        tokio::time::timeout(StdDuration::from_secs(2), guard.lock_lost_notified())
            .await
            .expect("the fence should expire after publication");
        ensure_pool_meta_write_fence(&guard, "test pool metadata publish")
            .expect_err("the post-publication fence check must observe the loss");

        assert_eq!(current.pools[0].last_update, saved.pools[0].last_update);
        drop(outcome);
        write_state
            .ensure_write_safe("lost pool metadata publish fence")
            .expect_err("the sticky write gate must remain armed after post-publication fence loss");
    }

    #[tokio::test]
    async fn test_clear_decommission_transaction_survives_caller_abort() {
        let pool_meta = PoolMeta {
            version: super::POOL_META_VERSION,
            pools: vec![decommission_test_pool_status(
                0,
                Some(PoolDecommissionInfo {
                    failed: true,
                    ..Default::default()
                }),
            )],
            ..Default::default()
        };
        let store = decommission_worker_test_store(pool_meta, vec![None]);
        let save_started = Arc::new(tokio::sync::Notify::new());
        let save_release = Arc::new(tokio::sync::Notify::new());
        let save_completed = Arc::new(AtomicBool::new(false));
        let caller_store = store.clone();
        let caller_save_started = save_started.clone();
        let caller_save_release = save_release.clone();
        let caller_save_completed = save_completed.clone();
        let caller_task = tokio::spawn(async move {
            caller_store
                .clear_decommission_with_save(0, move || async move {
                    caller_save_started.notify_one();
                    caller_save_release.notified().await;
                    caller_save_completed.store(true, Ordering::SeqCst);
                    Ok(())
                })
                .await
        });

        tokio::time::timeout(StdDuration::from_secs(1), save_started.notified())
            .await
            .expect("the clear transaction should reach persistence");
        caller_task.abort();
        assert!(
            caller_task
                .await
                .expect_err("the RPC waiter should be cancelled")
                .is_cancelled(),
            "the clear caller should be aborted while persistence is pending"
        );
        save_release.notify_one();

        let _start_guard = tokio::time::timeout(StdDuration::from_secs(1), store.start_gate.lock())
            .await
            .expect("the detached clear transaction should finish");
        assert!(
            save_completed.load(Ordering::SeqCst),
            "the detached transaction must finish persistence after the caller is aborted"
        );
        let pool_meta = store.pool_meta.read().await;
        assert!(
            pool_meta.pools[0].decommission.is_none(),
            "the detached transaction should publish the persisted clear"
        );
    }

    fn decommission_test_pool_endpoint(idx: usize, is_local: bool) -> PoolEndpoints {
        let port = 9000usize + idx;
        let mut endpoint =
            Endpoint::try_from(format!("http://127.0.0.1:{port}/disk").as_str()).expect("test endpoint should parse");
        endpoint.is_local = is_local;
        endpoint.pool_idx = i32::try_from(idx).expect("test pool index should fit i32");

        PoolEndpoints {
            legacy: false,
            set_count: 1,
            drives_per_set: 1,
            endpoints: Endpoints::from(vec![endpoint]),
            cmd_line: format!("pool-{idx}"),
            platform: String::new(),
        }
    }

    fn decommission_test_pool_status(idx: usize, decommission: Option<PoolDecommissionInfo>) -> PoolStatus {
        PoolStatus {
            id: idx,
            cmd_line: format!("pool-{idx}"),
            last_update: OffsetDateTime::now_utc(),
            decommission,
        }
    }

    #[tokio::test]
    async fn test_activation_fence_uses_one_lock_order_and_serializes_callers() {
        let manager = Arc::new(rustfs_lock::GlobalLockManager::new());
        let first = Arc::new(ActivationLockRecorder {
            lock_manager: Arc::clone(&manager),
            owner: "first",
            resources: StdMutex::new(Vec::new()),
        });
        let second = Arc::new(ActivationLockRecorder {
            lock_manager: manager,
            owner: "second",
            resources: StdMutex::new(Vec::new()),
        });

        let first_guards = acquire_pool_rebalance_activation_locks(first.clone(), None)
            .await
            .expect("first activation should acquire both locks");
        assert_eq!(
            *first
                .resources
                .lock()
                .expect("activation lock recorder should not be poisoned"),
            vec![POOL_META_NAME.to_string(), REBAL_META_NAME.to_string()]
        );

        let mut second_acquire = Box::pin(acquire_pool_rebalance_activation_locks(second.clone(), None));
        let mut context = Context::from_waker(futures::task::noop_waker_ref());
        assert!(matches!(second_acquire.as_mut().poll(&mut context), Poll::Pending));

        drop(first_guards);
        second_acquire
            .await
            .expect("second activation should acquire both locks after the first releases them");
        assert_eq!(
            *second
                .resources
                .lock()
                .expect("activation lock recorder should not be poisoned"),
            vec![POOL_META_NAME.to_string(), REBAL_META_NAME.to_string()]
        );
    }

    #[test]
    fn decommission_receipt_run_token_changes_with_persisted_start_time() {
        let first = OffsetDateTime::from_unix_timestamp(1_000).expect("first run timestamp should be valid");
        let second = OffsetDateTime::from_unix_timestamp(2_000).expect("second run timestamp should be valid");
        let first_token = decommission_durable_ilm_receipt_run_token("pool-0", first);
        let second_token = decommission_durable_ilm_receipt_run_token("pool-0", second);

        assert_ne!(first_token, second_token);
        assert_eq!(first_token, decommission_durable_ilm_receipt_run_token("pool-0", first));
        let operation_id = "a".repeat(64);
        let old_receipt = decommission_durable_ilm_receipt_path(
            &first_token,
            &format!("ilm/tier-delete-journal/{operation_id}.json"),
            "operation_id",
            &operation_id,
        );
        assert!(!old_receipt.starts_with(&decommission_durable_ilm_receipt_run_prefix(&second_token)));
    }

    #[test]
    fn decommission_receipt_merge_preserves_terminal_proof() {
        let operation_id = "a".repeat(64);
        let source_path = format!("ilm/tier-delete-journal/{operation_id}.json");
        let checkpoint = DurableIlmRecordCheckpoint::TierDeleteJournal {
            content_sha256: "b".repeat(64),
            identity_sha256: "c".repeat(64),
            committed: false,
        };
        let terminal_checkpoint = DurableIlmRecordCheckpoint::TierDeleteJournal {
            content_sha256: "d".repeat(64),
            identity_sha256: "c".repeat(64),
            committed: true,
        };
        let incoming = DecommissionDurableIlmReceipt {
            source_path,
            namespace: "tier-delete-journal".to_string(),
            id_kind: "operation_id".to_string(),
            id: operation_id,
            checkpoint: checkpoint.clone(),
            terminal_checkpoint: None,
        };
        let existing = DecommissionDurableIlmReceipt {
            terminal_checkpoint: Some(terminal_checkpoint.clone()),
            ..incoming.clone()
        };

        let merged = merge_decommission_durable_ilm_receipts(&existing, &incoming)
            .expect("retry receipt must merge with a terminal receipt");

        assert_eq!(merged.checkpoint, checkpoint);
        assert_eq!(merged.terminal_checkpoint, Some(terminal_checkpoint));
    }

    #[test]
    fn decommission_manual_job_receipt_compacts_large_progress() {
        let prefix = "p".repeat(12 * 1024);
        let options = ManualTransitionRunOptions {
            prefix,
            ..Default::default()
        };
        let mut job = ManualTransitionJobRecord::new(uuid::Uuid::new_v4(), "bounded-receipt-bucket", &options, "owner");
        let token_bytes = serde_json::to_vec(&serde_json::json!({
            "marker": "m".repeat(12 * 1024),
            "version_marker": "opaque-version"
        }))
        .expect("large continuation token should encode");
        let mut report = job.report.clone();
        report.scanned = 1;
        report.continuation_token = Some(base64_simd::URL_SAFE_NO_PAD.encode_to_string(&token_bytes));
        job.update_running_progress(report, ManualTransitionQueueSnapshot::default());
        let path = manual_transition_job_record_object_name(job.job_id).expect("manual job path should build");
        let job_bytes = job.encode().expect("large manual job should remain within its record limit");
        assert!(job_bytes.len() > DECOMMISSION_DURABLE_ILM_RECEIPT_MAX_SIZE);
        let record = validate_durable_ilm_record(&path, &job_bytes).expect("large manual job should validate");
        let expected_checkpoint = record.checkpoint.clone();
        let mut receipt = DecommissionDurableIlmReceipt::new(&path, &record);
        receipt.terminal_checkpoint = Some(record.checkpoint);

        let encoded = receipt.encode().expect("bounded progress proof should fit the receipt limit");
        let decoded = DecommissionDurableIlmReceipt::decode(&encoded).expect("bounded receipt should round trip");

        assert!(encoded.len() <= DECOMMISSION_DURABLE_ILM_RECEIPT_MAX_SIZE);
        assert_eq!(decoded.source_path, path);
        assert_eq!(decoded.checkpoint, expected_checkpoint);
        assert_eq!(decoded.terminal_checkpoint, Some(expected_checkpoint));
    }

    #[test]
    fn test_apply_decommission_status_space_info_adds_idle_pool_usage() {
        let status = apply_decommission_status_space_info(
            decommission_test_pool_status(0, None),
            PoolSpaceInfo {
                free: 25,
                total: 100,
                used: 75,
            },
        );

        let decommission = status.decommission.expect("idle pool status should include usage info");
        assert_eq!(decommission.total_size, 100);
        assert_eq!(decommission.current_size, 25);
        assert!(decommission.start_time.is_none());
        assert!(!decommission.complete);
        assert!(!decommission.failed);
        assert!(!decommission.canceled);
    }

    #[test]
    fn test_apply_decommission_status_space_info_refreshes_active_decommission_sizes() {
        let status = apply_decommission_status_space_info(
            decommission_test_pool_status(
                0,
                Some(PoolDecommissionInfo {
                    total_size: 1,
                    current_size: 1,
                    ..Default::default()
                }),
            ),
            PoolSpaceInfo {
                free: 25,
                total: 100,
                used: 75,
            },
        );

        let decommission = status.decommission.expect("active decommission info should remain present");
        assert_eq!(decommission.total_size, 100);
        assert_eq!(decommission.current_size, 25);
    }

    #[test]
    fn test_merge_pool_status_refresh_uses_persisted_terminal_decommission() {
        let older = OffsetDateTime::from_unix_timestamp(1_000).expect("test timestamp should be valid");
        let newer = OffsetDateTime::from_unix_timestamp(2_000).expect("test timestamp should be valid");
        let mut current = PoolMeta {
            pools: vec![PoolStatus {
                id: 0,
                cmd_line: "pool-0".to_string(),
                last_update: older,
                decommission: Some(PoolDecommissionInfo {
                    start_time: Some(older),
                    ..Default::default()
                }),
            }],
            ..Default::default()
        };
        let persisted = PoolMeta {
            pools: vec![PoolStatus {
                id: 0,
                cmd_line: "pool-0".to_string(),
                last_update: newer,
                decommission: Some(PoolDecommissionInfo {
                    complete: true,
                    ..Default::default()
                }),
            }],
            ..Default::default()
        };

        assert!(merge_pool_status_refresh(&mut current, persisted, &[false]));

        let info = current.pools[0]
            .decommission
            .as_ref()
            .expect("decommission info should be present");
        assert!(info.complete);
        assert!(!info.failed);
        assert!(!info.canceled);
    }

    #[test]
    fn test_merge_pool_status_refresh_keeps_newer_local_active_progress() {
        let older = OffsetDateTime::from_unix_timestamp(1_000).expect("test timestamp should be valid");
        let newer = OffsetDateTime::from_unix_timestamp(2_000).expect("test timestamp should be valid");
        let mut current = PoolMeta {
            pools: vec![PoolStatus {
                id: 0,
                cmd_line: "pool-0".to_string(),
                last_update: newer,
                decommission: Some(PoolDecommissionInfo {
                    start_time: Some(older),
                    items_decommissioned: 10,
                    bytes_done: 1_024,
                    ..Default::default()
                }),
            }],
            ..Default::default()
        };
        let persisted = PoolMeta {
            pools: vec![PoolStatus {
                id: 0,
                cmd_line: "pool-0".to_string(),
                last_update: older,
                decommission: Some(PoolDecommissionInfo {
                    start_time: Some(older),
                    items_decommissioned: 1,
                    bytes_done: 128,
                    ..Default::default()
                }),
            }],
            ..Default::default()
        };

        assert!(!merge_pool_status_refresh(&mut current, persisted, &[true]));

        let info = current.pools[0]
            .decommission
            .as_ref()
            .expect("local decommission info should remain present");
        assert_eq!(info.items_decommissioned, 10);
        assert_eq!(info.bytes_done, 1_024);
    }

    #[test]
    fn test_merge_pool_status_refresh_preserves_observed_v2_floor() {
        let timestamp = OffsetDateTime::from_unix_timestamp(1_000).expect("test timestamp should be valid");
        let mut current = PoolMeta {
            version: POOL_META_V1_VERSION,
            pools: vec![PoolStatus {
                id: 0,
                cmd_line: "pool-0".to_string(),
                last_update: timestamp,
                decommission: Some(PoolDecommissionInfo::default()),
            }],
            dont_save: false,
        };
        let persisted = PoolMeta {
            version: POOL_META_VERSION,
            pools: current.pools.clone(),
            dont_save: false,
        };

        assert!(!merge_pool_status_refresh(&mut current, persisted, &[true]));
        assert_eq!(current.version, POOL_META_VERSION);
        let encoded = current
            .encode_config_data_for_v2_gate(false)
            .expect("a peer-observed v2 floor must remain sticky");
        assert_eq!(LittleEndian::read_u16(&encoded[2..4]), POOL_META_VERSION);
    }

    #[test]
    fn test_merge_pool_status_refresh_keeps_newer_local_active_over_older_terminal() {
        let older = OffsetDateTime::from_unix_timestamp(1_000).expect("test timestamp should be valid");
        let newer = OffsetDateTime::from_unix_timestamp(2_000).expect("test timestamp should be valid");
        let mut current = PoolMeta {
            pools: vec![PoolStatus {
                id: 0,
                cmd_line: "pool-0".to_string(),
                last_update: newer,
                decommission: Some(PoolDecommissionInfo {
                    start_time: Some(older),
                    items_decommissioned: 10,
                    bytes_done: 1_024,
                    ..Default::default()
                }),
            }],
            ..Default::default()
        };
        let persisted = PoolMeta {
            pools: vec![PoolStatus {
                id: 0,
                cmd_line: "pool-0".to_string(),
                last_update: older,
                decommission: Some(PoolDecommissionInfo {
                    failed: true,
                    ..Default::default()
                }),
            }],
            ..Default::default()
        };

        assert!(!merge_pool_status_refresh(&mut current, persisted, &[true]));

        let info = current.pools[0]
            .decommission
            .as_ref()
            .expect("local active decommission info should remain present");
        assert!(!info.failed);
        assert_eq!(info.items_decommissioned, 10);
        assert_eq!(info.bytes_done, 1_024);
    }

    #[test]
    fn test_merge_pool_status_refresh_fails_closed_on_missing_persisted_pools() {
        let newer = OffsetDateTime::from_unix_timestamp(2_000).expect("test timestamp should be valid");
        let mut current = PoolMeta {
            pools: vec![decommission_test_pool_status(
                0,
                Some(PoolDecommissionInfo {
                    complete: true,
                    ..Default::default()
                }),
            )],
            ..Default::default()
        };
        current.pools[0].last_update = newer;

        assert!(
            !merge_pool_status_refresh(&mut current, PoolMeta::default(), &[false]),
            "an empty persisted snapshot must fail closed instead of replacing local state"
        );

        let info = current.pools[0]
            .decommission
            .as_ref()
            .expect("local decommission info should survive a missing snapshot");
        assert!(info.complete);
        assert_eq!(current.pools[0].last_update, newer);
    }

    #[test]
    fn test_merge_pool_status_refresh_ignores_mislabeled_pool_entries() {
        let older = OffsetDateTime::from_unix_timestamp(1_000).expect("test timestamp should be valid");
        let mut current = PoolMeta {
            pools: vec![decommission_test_pool_status(0, None)],
            ..Default::default()
        };
        let mut persisted = PoolMeta {
            pools: vec![decommission_test_pool_status(0, Some(PoolDecommissionInfo::default()))],
            ..Default::default()
        };
        persisted.pools[0].id = 7;
        persisted.pools[0].last_update = older;

        assert!(
            !merge_pool_status_refresh(&mut current, persisted, &[false]),
            "a pool entry whose id does not match its index must be ignored"
        );
        assert!(current.pools[0].decommission.is_none());
    }

    #[test]
    fn test_pool_meta_save_merge_preserves_newer_untouched_pool() {
        let older = OffsetDateTime::from_unix_timestamp(1_000).expect("test timestamp should be valid");
        let newer = OffsetDateTime::from_unix_timestamp(2_000).expect("test timestamp should be valid");
        let mut current = PoolMeta {
            pools: vec![
                PoolStatus {
                    id: 0,
                    cmd_line: "pool-0".to_string(),
                    last_update: older,
                    decommission: Some(PoolDecommissionInfo {
                        failed: true,
                        ..Default::default()
                    }),
                },
                PoolStatus {
                    id: 1,
                    cmd_line: "pool-1".to_string(),
                    last_update: older,
                    decommission: None,
                },
            ],
            ..Default::default()
        };
        let mut persisted = current.clone();
        persisted.pools[1].last_update = newer;
        persisted.pools[1].decommission = Some(PoolDecommissionInfo {
            failed: true,
            ..Default::default()
        });

        assert!(current.clear_decommission(0).expect("terminal decommission should clear"));
        merge_pool_meta_updates_for_save(&mut persisted, &current, &[0], "clear decommission")
            .expect("the target pool update should merge into the latest snapshot");

        assert!(persisted.pools[0].decommission.is_none());
        assert_eq!(persisted.pools[1].last_update, newer);
        assert!(persisted.pools[1].decommission.as_ref().is_some_and(|info| info.failed));
    }

    #[test]
    fn test_pool_meta_save_merge_rejects_stale_terminal_resurrection() {
        let older = OffsetDateTime::from_unix_timestamp(1_000).expect("test timestamp should be valid");
        let newer = OffsetDateTime::from_unix_timestamp(2_000).expect("test timestamp should be valid");
        let mut persisted = PoolMeta {
            pools: vec![PoolStatus {
                id: 0,
                cmd_line: "pool-0".to_string(),
                last_update: newer,
                decommission: Some(PoolDecommissionInfo {
                    canceled: true,
                    ..Default::default()
                }),
            }],
            ..Default::default()
        };
        let stale = PoolMeta {
            pools: vec![PoolStatus {
                id: 0,
                cmd_line: "pool-0".to_string(),
                last_update: older,
                decommission: Some(PoolDecommissionInfo {
                    start_time: Some(older),
                    ..Default::default()
                }),
            }],
            ..Default::default()
        };

        let err = merge_pool_meta_updates_for_save(&mut persisted, &stale, &[0], "stale writer")
            .expect_err("a stale active snapshot must not resurrect a canceled pool");

        assert!(err.to_string().contains("stale pool metadata update rejected"));
        assert!(persisted.pools[0].decommission.as_ref().is_some_and(|info| info.canceled));
    }

    #[test]
    fn test_pool_meta_publish_preserves_untouched_runtime_progress() {
        let mut current = PoolMeta {
            pools: vec![
                decommission_test_pool_status(0, None),
                decommission_test_pool_status(
                    1,
                    Some(PoolDecommissionInfo {
                        items_decommissioned: 10,
                        bytes_done: 1_024,
                        ..Default::default()
                    }),
                ),
            ],
            ..Default::default()
        };
        let mut saved = current.clone();
        saved.pools[0].decommission = Some(PoolDecommissionInfo {
            complete: true,
            ..Default::default()
        });
        let saved_pool_1 = saved.pools[1].decommission.as_mut().expect("pool 1 progress should exist");
        saved_pool_1.items_decommissioned = 1;
        saved_pool_1.bytes_done = 128;

        publish_pool_meta_updates(&mut current, &saved, &[0]);

        assert!(current.pools[0].decommission.as_ref().is_some_and(|info| info.complete));
        let pool_1 = current.pools[1]
            .decommission
            .as_ref()
            .expect("pool 1 progress should remain present");
        assert_eq!(pool_1.items_decommissioned, 10);
        assert_eq!(pool_1.bytes_done, 1_024);
    }

    #[test]
    fn test_dedup_indices_removes_duplicates_preserving_order() {
        assert_eq!(dedup_indices(&[0, 2, 1, 2, 3, 0]), vec![0, 2, 1, 3]);
    }

    #[test]
    fn test_dedup_indices_handles_empty_input() {
        let empty: Vec<usize> = Vec::new();
        assert!(dedup_indices(&empty).is_empty());
    }

    #[test]
    fn test_default_decommission_bucket_concurrency_is_conservative() {
        assert_eq!(default_decommission_bucket_concurrency(0), 1);
        assert_eq!(default_decommission_bucket_concurrency(1), 1);
        assert_eq!(default_decommission_bucket_concurrency(2), 2);
        assert_eq!(default_decommission_bucket_concurrency(8), 4);
    }

    #[test]
    fn test_default_decommission_entry_concurrency_is_conservative() {
        assert_eq!(default_decommission_entry_concurrency(0), 1);
        assert_eq!(default_decommission_entry_concurrency(1), 1);
        assert_eq!(default_decommission_entry_concurrency(4), 4);
        assert_eq!(default_decommission_entry_concurrency(16), DECOMMISSION_ENTRY_CONCURRENCY_DEFAULT_CAP);
    }

    #[test]
    fn test_decommission_entry_concurrency_clamps_operator_configuration() {
        assert_eq!(clamp_decommission_entry_concurrency(0), 1);
        assert_eq!(clamp_decommission_entry_concurrency(1), 1);
        assert_eq!(
            clamp_decommission_entry_concurrency(DECOMMISSION_ENTRY_CONCURRENCY_HARD_CAP),
            DECOMMISSION_ENTRY_CONCURRENCY_HARD_CAP
        );
        assert_eq!(clamp_decommission_entry_concurrency(usize::MAX), DECOMMISSION_ENTRY_CONCURRENCY_HARD_CAP);
    }

    #[test]
    fn test_split_decommission_buckets_keeps_meta_buckets_last() {
        let (regular, meta) = split_decommission_buckets(vec![
            DecomBucketInfo {
                name: "bucket-a".to_string(),
                ..Default::default()
            },
            DecomBucketInfo {
                name: crate::disk::RUSTFS_META_BUCKET.to_string(),
                prefix: crate::config::com::CONFIG_PREFIX.to_string(),
            },
            DecomBucketInfo {
                name: "bucket-b".to_string(),
                ..Default::default()
            },
            DecomBucketInfo {
                name: crate::disk::RUSTFS_META_BUCKET.to_string(),
                prefix: crate::disk::BUCKET_META_PREFIX.to_string(),
            },
            DecomBucketInfo {
                name: crate::disk::RUSTFS_META_BUCKET.to_string(),
                prefix: crate::bucket::lifecycle::ILM_META_PREFIX.to_string(),
            },
        ]);

        assert_eq!(
            regular.iter().map(|bucket| bucket.name.as_str()).collect::<Vec<_>>(),
            vec!["bucket-a", "bucket-b",]
        );
        assert_eq!(
            meta.iter().map(|bucket| bucket.prefix.as_str()).collect::<Vec<_>>(),
            vec![
                crate::config::com::CONFIG_PREFIX,
                crate::disk::BUCKET_META_PREFIX,
                crate::bucket::lifecycle::ILM_META_PREFIX,
            ]
        );
    }

    #[test]
    fn test_resume_reconciles_missing_decommission_meta_prefixes() {
        let mut meta = PoolMeta {
            pools: vec![decommission_test_pool_status(
                0,
                Some(PoolDecommissionInfo {
                    queued_buckets: vec![
                        format!("{}/{}", crate::disk::RUSTFS_META_BUCKET, crate::config::com::CONFIG_PREFIX),
                        format!("{}/{}", crate::disk::RUSTFS_META_BUCKET, crate::disk::BUCKET_META_PREFIX),
                    ],
                    ..Default::default()
                }),
            )],
            ..Default::default()
        };

        assert!(reconcile_decommission_meta_buckets(&mut meta, 0));
        assert_eq!(
            meta.pending_buckets(0)
                .iter()
                .filter(|bucket| bucket.name == crate::disk::RUSTFS_META_BUCKET)
                .map(|bucket| bucket.prefix.as_str())
                .collect::<Vec<_>>(),
            DECOMMISSION_META_PREFIXES
        );
        assert!(!reconcile_decommission_meta_buckets(&mut meta, 0));
    }

    #[tokio::test]
    async fn test_decommission_metadata_phase_precedes_regular_failure() {
        let events = Arc::new(StdMutex::new(Vec::new()));
        let err = run_decommission_phases(
            CancellationToken::new(),
            vec![
                DecomBucketInfo {
                    name: "regular-fails".to_string(),
                    ..Default::default()
                },
                DecomBucketInfo {
                    name: "regular-not-started".to_string(),
                    ..Default::default()
                },
            ],
            vec![
                DecomBucketInfo {
                    name: crate::disk::RUSTFS_META_BUCKET.to_string(),
                    prefix: crate::config::com::CONFIG_PREFIX.to_string(),
                },
                DecomBucketInfo {
                    name: crate::disk::RUSTFS_META_BUCKET.to_string(),
                    prefix: crate::disk::BUCKET_META_PREFIX.to_string(),
                },
            ],
            1,
            {
                let events = Arc::clone(&events);
                move |bucket, _rx| {
                    let events = Arc::clone(&events);
                    Box::pin(async move {
                        let event = if bucket.name == crate::disk::RUSTFS_META_BUCKET {
                            format!("meta:{}", bucket.prefix)
                        } else {
                            format!("regular:{}", bucket.name)
                        };
                        events.lock().expect("phase event lock should not be poisoned").push(event);
                        if bucket.name == "regular-fails" {
                            Err(Error::SlowDown)
                        } else {
                            Ok(())
                        }
                    })
                }
            },
        )
        .await
        .expect_err("regular failure should remain fatal after metadata completes");

        assert!(matches!(err, Error::SlowDown));
        assert_eq!(
            *events.lock().expect("phase event lock should not be poisoned"),
            vec![
                format!("meta:{}", crate::config::com::CONFIG_PREFIX),
                format!("meta:{}", crate::disk::BUCKET_META_PREFIX),
                "regular:regular-fails".to_string(),
            ]
        );
    }

    #[tokio::test]
    async fn test_run_decommission_buckets_bounded_respects_limit() {
        let rx = CancellationToken::new();
        let running = Arc::new(AtomicUsize::new(0));
        let max_running = Arc::new(AtomicUsize::new(0));
        let started = Arc::new(AtomicUsize::new(0));
        let buckets = (0..8)
            .map(|idx| DecomBucketInfo {
                name: format!("bucket-{idx}"),
                ..Default::default()
            })
            .collect::<Vec<_>>();

        run_decommission_buckets_bounded(rx, buckets, 2, {
            let running = Arc::clone(&running);
            let max_running = Arc::clone(&max_running);
            let started = Arc::clone(&started);
            move |_bucket, _rx| {
                let running = Arc::clone(&running);
                let max_running = Arc::clone(&max_running);
                let started = Arc::clone(&started);
                Box::pin(async move {
                    started.fetch_add(1, Ordering::SeqCst);
                    let current = running.fetch_add(1, Ordering::SeqCst) + 1;
                    max_running.fetch_max(current, Ordering::SeqCst);
                    tokio::time::sleep(StdDuration::from_millis(10)).await;
                    running.fetch_sub(1, Ordering::SeqCst);
                    Ok(())
                })
            }
        })
        .await
        .expect("bounded bucket scheduler should complete");

        assert_eq!(started.load(Ordering::SeqCst), 8);
        assert_eq!(max_running.load(Ordering::SeqCst), 2);
        assert_eq!(running.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_run_decommission_buckets_bounded_cancels_and_stops_launching_after_failure() {
        let rx = CancellationToken::new();
        let started = Arc::new(AtomicUsize::new(0));
        let observed_cancel = Arc::new(AtomicBool::new(false));
        let buckets = (0..5)
            .map(|idx| DecomBucketInfo {
                name: format!("bucket-{idx}"),
                ..Default::default()
            })
            .collect::<Vec<_>>();

        let err = tokio::time::timeout(
            StdDuration::from_secs(2),
            run_decommission_buckets_bounded(rx.clone(), buckets, 2, {
                let started = Arc::clone(&started);
                let observed_cancel = Arc::clone(&observed_cancel);
                move |bucket, rx| {
                    let started = Arc::clone(&started);
                    let observed_cancel = Arc::clone(&observed_cancel);
                    Box::pin(async move {
                        started.fetch_add(1, Ordering::SeqCst);
                        if bucket.name == "bucket-0" {
                            while started.load(Ordering::SeqCst) < 2 {
                                tokio::task::yield_now().await;
                            }
                            return Err(Error::SlowDown);
                        }

                        rx.cancelled().await;
                        observed_cancel.store(true, Ordering::SeqCst);
                        Ok(())
                    })
                }
            }),
        )
        .await
        .expect("bucket scheduler should not hang after a bucket failure")
        .expect_err("first bucket failure should be returned");

        assert!(matches!(err, Error::SlowDown));
        assert!(rx.is_cancelled());
        assert!(observed_cancel.load(Ordering::SeqCst));
        assert_eq!(started.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_run_decommission_buckets_bounded_external_cancel_stops_pending_buckets() {
        let rx = CancellationToken::new();
        let started = Arc::new(AtomicUsize::new(0));
        let buckets = (0..4)
            .map(|idx| DecomBucketInfo {
                name: format!("bucket-{idx}"),
                ..Default::default()
            })
            .collect::<Vec<_>>();

        let err = run_decommission_buckets_bounded(rx.clone(), buckets, 1, {
            let started = Arc::clone(&started);
            move |_bucket, rx| {
                let started = Arc::clone(&started);
                Box::pin(async move {
                    started.fetch_add(1, Ordering::SeqCst);
                    rx.cancel();
                    Ok(())
                })
            }
        })
        .await
        .expect_err("external cancellation with pending buckets should stop the scheduler");

        assert!(matches!(err, Error::OperationCanceled));
        assert!(rx.is_cancelled());
        assert_eq!(started.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_wait_decommission_worker_drain_waits_for_entry_permit() {
        let workers = Arc::new(Semaphore::new(1));
        let permit = workers
            .clone()
            .acquire_owned()
            .await
            .expect("test worker permit should acquire");

        let drain = tokio::spawn({
            let workers = workers.clone();
            async move { wait_decommission_worker_drain(&workers, 1).await }
        });

        tokio::task::yield_now().await;
        assert!(!drain.is_finished(), "drain should wait while a worker permit is held");

        drop(permit);
        let result = tokio::time::timeout(StdDuration::from_secs(1), drain)
            .await
            .expect("drain should finish after permit release")
            .expect("drain task should not panic");
        assert!(result.is_ok());
    }

    #[test]
    fn test_decommission_entry_queue_capacity_is_bounded() {
        assert_eq!(decommission_entry_queue_capacity(0), 1);
        assert_eq!(decommission_entry_queue_capacity(1), 2);
        assert_eq!(
            decommission_entry_queue_capacity(DECOMMISSION_ENTRY_QUEUE_HARD_CAP),
            DECOMMISSION_ENTRY_QUEUE_HARD_CAP
        );
        assert_eq!(decommission_entry_queue_capacity(usize::MAX), DECOMMISSION_ENTRY_QUEUE_HARD_CAP);
    }

    #[tokio::test]
    async fn test_drain_decommission_entry_queue_waits_for_all_outstanding_entries() {
        let outstanding = Arc::new(Semaphore::new(1));
        let held = outstanding
            .clone()
            .acquire_owned()
            .await
            .expect("test outstanding permit should acquire");
        let rx = CancellationToken::new();
        let drain = tokio::spawn({
            let outstanding = outstanding.clone();
            let rx = rx.clone();
            async move { drain_decommission_entry_queue(&rx, &outstanding, 1).await }
        });

        tokio::task::yield_now().await;
        assert!(!drain.is_finished(), "queue drain must wait for active entry work");
        drop(held);

        let drained = tokio::time::timeout(StdDuration::from_secs(1), drain)
            .await
            .expect("queue drain should finish after entry completion")
            .expect("queue drain task should not panic");
        assert!(!drained);
    }

    #[tokio::test]
    async fn test_enqueue_decommission_entry_observes_cancellation_when_queue_is_full() {
        let outstanding = Arc::new(Semaphore::new(2));
        let (tx, mut queue) = tokio::sync::mpsc::channel(1);
        let held = outstanding
            .clone()
            .acquire_owned()
            .await
            .expect("first queue permit should acquire");
        tx.send(QueuedDecommissionEntry {
            entry: MetaCacheEntry::default(),
            queue_permit: held,
        })
        .await
        .expect("first entry should fill the queue");

        let rx = CancellationToken::new();
        let enqueue = tokio::spawn({
            let rx = rx.clone();
            let outstanding = outstanding.clone();
            let tx = tx.clone();
            async move { enqueue_decommission_entry(&rx, &outstanding, &tx, MetaCacheEntry::default()).await }
        });

        tokio::task::yield_now().await;
        rx.cancel();
        let result = tokio::time::timeout(StdDuration::from_secs(1), enqueue)
            .await
            .expect("full queue enqueue should observe cancellation")
            .expect("enqueue task should not panic");
        assert!(matches!(result, DecommissionEntryEnqueueResult::Canceled));
        drop(queue.recv().await);
    }

    #[tokio::test]
    async fn test_decommission_side_effect_gate_quiesces_before_transition() {
        let operation_gate = Arc::new(tokio::sync::RwLock::new(()));
        let rx = CancellationToken::new();
        let started = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let operation = tokio::spawn({
            let operation_gate = operation_gate.clone();
            let rx = rx.clone();
            let started = started.clone();
            let release = release.clone();
            async move {
                run_decommission_side_effect(&rx, &operation_gate, || async {
                    started.notify_one();
                    release.notified().await;
                    Ok::<_, Error>(())
                })
                .await
            }
        });

        started.notified().await;
        rx.cancel();
        let transition = tokio::spawn({
            let operation_gate = operation_gate.clone();
            async move {
                let _guard = operation_gate.write().await;
            }
        });
        tokio::task::yield_now().await;
        assert!(!transition.is_finished(), "transition must wait for the in-flight side effect");

        release.notify_one();
        let operation_result = operation.await.expect("operation task should not panic");
        assert!(matches!(operation_result, Err(Error::OperationCanceled)));
        transition.await.expect("transition task should not panic");

        let called = Arc::new(AtomicBool::new(false));
        let result = run_decommission_side_effect(&rx, &operation_gate, {
            let called = called.clone();
            move || async move {
                called.store(true, Ordering::SeqCst);
                Ok::<_, Error>(())
            }
        })
        .await;
        assert!(matches!(result, Err(Error::OperationCanceled)));
        assert!(!called.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_decommission_side_effect_stops_after_pool_meta_write_block() {
        let store = decommission_worker_test_store(PoolMeta::default(), Vec::new());
        store
            .pool_meta_save_gate
            .lock()
            .await
            .observe_replicas(super::PoolMetaReplicaState {
                needs_repair: true,
                repair_write_safe: false,
            });
        let called = Arc::new(AtomicBool::new(false));
        let operation_gate = store.ctx.data_movement_operation_gate();

        let result = store
            .run_guarded_decommission_side_effect(&CancellationToken::new(), &operation_gate, {
                let called = called.clone();
                move || async move {
                    called.store(true, Ordering::SeqCst);
                    Ok::<_, Error>(())
                }
            })
            .await;

        assert!(
            result
                .expect_err("sticky pool metadata state must block new movement")
                .to_string()
                .contains("restart after all replicas are readable and consistent")
        );
        assert!(!called.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_decommission_reservation_stops_before_canceler_slot_when_pool_meta_is_blocked() {
        let generation = OffsetDateTime::UNIX_EPOCH;
        let pool_meta = PoolMeta {
            version: super::POOL_META_VERSION,
            pools: vec![decommission_test_pool_status(
                0,
                Some(PoolDecommissionInfo {
                    start_time: Some(generation),
                    ..Default::default()
                }),
            )],
            ..Default::default()
        };
        let store = decommission_worker_test_store(pool_meta, vec![None]);
        store
            .pool_meta_save_gate
            .lock()
            .await
            .observe_replicas(super::PoolMetaReplicaState {
                needs_repair: true,
                repair_write_safe: false,
            });

        let err = store
            .reserve_decommission_routines(&CancellationToken::new(), &[0])
            .await
            .err()
            .expect("sticky pool metadata state must block worker reservation");

        assert!(
            err.to_string()
                .contains("restart after all replicas are readable and consistent")
        );
        assert!(store.decommission_cancelers.read().await[0].is_none());
    }

    #[tokio::test]
    async fn test_decommission_transition_waits_without_registered_canceler() {
        let store = decommission_worker_test_store(PoolMeta::default(), vec![None]);
        let operation_gate = store.ctx.data_movement_operation_gate();
        let operation_guard = operation_gate.read().await;
        let transition = tokio::spawn({
            let store = store.clone();
            async move { store.cancel_decommission_routines_and_wait(&[0]).await }
        });

        tokio::task::yield_now().await;
        assert!(
            !transition.is_finished(),
            "a transition must wait for an in-flight side effect even after its canceler slot is gone"
        );

        drop(operation_guard);
        tokio::time::timeout(StdDuration::from_secs(1), transition)
            .await
            .expect("transition should finish after the side effect")
            .expect("transition task should not panic");
    }

    #[tokio::test]
    async fn test_join_error_quiesces_side_effects_before_failure_transition() {
        let canceler = DecommissionCanceler::new(CancellationToken::new());
        let store = decommission_worker_test_store(PoolMeta::default(), vec![Some(canceler.clone())]);
        let operation_gate = store.ctx.data_movement_operation_gate();
        let operation_guard = operation_gate.read().await;
        let transition = tokio::spawn({
            let store = store.clone();
            let canceler = canceler.clone();
            async move { store.quiesce_decommission_worker_after_join_error(&canceler).await }
        });

        tokio::time::timeout(StdDuration::from_secs(1), canceler.token().cancelled())
            .await
            .expect("join error handling should cancel the detached worker");
        assert!(
            !transition.is_finished(),
            "failure transition must wait for the detached worker's in-flight side effect"
        );

        drop(operation_guard);
        tokio::time::timeout(StdDuration::from_secs(1), transition)
            .await
            .expect("failure transition should finish after the side effect")
            .expect("failure transition task should not panic");
    }

    #[tokio::test(start_paused = true)]
    async fn test_run_decommission_listing_with_retry_drains_before_each_retry() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let drains = Arc::new(AtomicUsize::new(0));
        let err = run_decommission_listing_with_retry_and_drain(
            CancellationToken::new(),
            "bucket-a".to_string(),
            noop_decommission_list_callback(),
            1,
            2,
            2,
            {
                let attempts = attempts.clone();
                move |_| {
                    let attempts = attempts.clone();
                    async move {
                        attempts.fetch_add(1, Ordering::SeqCst);
                        Err(Error::SlowDown)
                    }
                }
            },
            {
                let drains = drains.clone();
                move || {
                    let drains = drains.clone();
                    async move {
                        drains.fetch_add(1, Ordering::SeqCst);
                        false
                    }
                }
            },
        )
        .await
        .expect_err("permanent listing failure must be returned");

        assert!(err.to_string().contains("attempt 2/2"));
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
        assert_eq!(drains.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn test_get_by_index_returns_value_when_in_range() {
        let values = vec!["a", "b", "c"];
        let value = get_by_index(values.as_slice(), 1, "fetch decommission status").expect("in-range index should return value");
        assert_eq!(*value, "b");
    }

    #[test]
    fn test_get_by_index_returns_error_when_out_of_range() {
        let values = vec![1_u8];
        let err =
            get_by_index(values.as_slice(), 2, "load decommission background pool").expect_err("out-of-range index should fail");
        assert!(
            err.to_string()
                .contains("failed to load decommission background pool: invalid decommission pool index 2 for 1 pools")
        );
    }

    #[test]
    fn test_pool_meta_is_suspended_returns_false_for_out_of_range() {
        let meta = PoolMeta::default();
        assert!(!meta.is_suspended(1));
    }

    #[test]
    fn test_rollback_start_decommission_pool_meta_clears_active_state() {
        let previous = PoolMeta {
            pools: vec![PoolStatus {
                id: 0,
                cmd_line: "pool-0".to_string(),
                last_update: OffsetDateTime::UNIX_EPOCH,
                decommission: None,
            }],
            ..Default::default()
        };
        let mut active = previous.clone();
        let active_update = OffsetDateTime::UNIX_EPOCH + Duration::seconds(1);
        active.pools[0].last_update = active_update;
        active.pools[0].decommission = Some(PoolDecommissionInfo {
            start_time: Some(OffsetDateTime::UNIX_EPOCH),
            ..Default::default()
        });
        let mut peer = active.clone();

        assert!(active.is_suspended(0));
        assert_eq!(
            decommission_start_pool_state(active.pools.first()),
            DecommissionStartPoolState::Decommissioning
        );

        rollback_start_decommission_pool_meta(&mut active, &previous, &[0]);

        assert!(!active.is_suspended(0));
        assert_eq!(decommission_start_pool_state(active.pools.first()), DecommissionStartPoolState::Active);
        assert!(active.pools[0].last_update > active_update);
        assert!(merge_pool_status_refresh(&mut peer, active, &[false]));
        assert!(peer.pools[0].decommission.is_none());
    }

    #[test]
    fn test_pool_meta_queue_buckets_ignores_out_of_range_index() {
        let mut meta = PoolMeta {
            pools: vec![PoolStatus {
                id: 0,
                cmd_line: "pool-0".to_string(),
                last_update: OffsetDateTime::UNIX_EPOCH,
                decommission: Some(PoolDecommissionInfo::default()),
            }],
            ..Default::default()
        };

        meta.queue_buckets(
            9,
            vec![DecomBucketInfo {
                name: "bucket-a".to_string(),
                prefix: String::new(),
            }],
        );

        let queued = meta.pools[0]
            .decommission
            .as_ref()
            .expect("pool should have decommission info")
            .queued_buckets
            .clone();
        assert!(queued.is_empty());
    }

    #[test]
    fn test_pool_meta_is_bucket_decommissioned_returns_false_for_out_of_range() {
        let meta = PoolMeta::default();
        assert!(!meta.is_bucket_decommissioned(7, "bucket-a".to_string()));
    }

    #[test]
    fn test_resolve_decommission_bucket_state_rejects_out_of_range_index() {
        let meta = PoolMeta {
            pools: vec![PoolStatus {
                id: 0,
                cmd_line: "pool-0".to_string(),
                last_update: OffsetDateTime::UNIX_EPOCH,
                decommission: Some(PoolDecommissionInfo::default()),
            }],
            ..Default::default()
        };

        let bucket = DecomBucketInfo {
            name: "bucket-a".to_string(),
            prefix: String::new(),
        };
        let err =
            resolve_decommission_bucket_state(&meta, 3, &bucket).expect_err("out-of-range index should return invalid argument");
        assert!(err.to_string().contains("invalid decommission pool index 3 for 1 pools"));
    }

    #[test]
    fn test_resolve_decommission_bucket_state_rejects_missing_decommission_meta() {
        let meta = PoolMeta {
            pools: vec![PoolStatus {
                id: 0,
                cmd_line: "pool-0".to_string(),
                last_update: OffsetDateTime::UNIX_EPOCH,
                decommission: None,
            }],
            ..Default::default()
        };

        let bucket = DecomBucketInfo {
            name: "bucket-a".to_string(),
            prefix: String::new(),
        };
        let err = resolve_decommission_bucket_state(&meta, 0, &bucket)
            .expect_err("missing decommission metadata should return explicit error");
        assert!(
            err.to_string()
                .contains("failed to resolve decommission bucket state: decommission metadata not initialized")
        );
    }

    #[test]
    fn test_resolve_decommission_bucket_state_returns_true_for_done_bucket() {
        let meta = PoolMeta {
            pools: vec![PoolStatus {
                id: 0,
                cmd_line: "pool-0".to_string(),
                last_update: OffsetDateTime::UNIX_EPOCH,
                decommission: Some(PoolDecommissionInfo {
                    decommissioned_buckets: vec!["bucket-a".to_string()],
                    ..Default::default()
                }),
            }],
            ..Default::default()
        };

        let bucket = DecomBucketInfo {
            name: "bucket-a".to_string(),
            prefix: String::new(),
        };
        let done = resolve_decommission_bucket_state(&meta, 0, &bucket).expect("valid state should resolve");
        assert!(done);
    }

    #[test]
    fn test_mark_decommission_bucket_done_rejects_missing_decommission_meta() {
        let mut meta = PoolMeta {
            pools: vec![PoolStatus {
                id: 0,
                cmd_line: "pool-0".to_string(),
                last_update: OffsetDateTime::UNIX_EPOCH,
                decommission: None,
            }],
            ..Default::default()
        };

        let bucket = DecomBucketInfo {
            name: "bucket-a".to_string(),
            prefix: String::new(),
        };
        let err = mark_decommission_bucket_done(&mut meta, 0, &bucket)
            .expect_err("missing decommission metadata should return explicit error");
        assert!(
            err.to_string()
                .contains("failed to mark decommission bucket done: decommission metadata not initialized")
        );
    }

    #[test]
    fn test_mark_decommission_bucket_done_rejects_out_of_range_index() {
        let mut meta = PoolMeta {
            pools: vec![PoolStatus {
                id: 0,
                cmd_line: "pool-0".to_string(),
                last_update: OffsetDateTime::UNIX_EPOCH,
                decommission: Some(PoolDecommissionInfo::default()),
            }],
            ..Default::default()
        };

        let bucket = DecomBucketInfo {
            name: "bucket-a".to_string(),
            prefix: String::new(),
        };
        let err =
            mark_decommission_bucket_done(&mut meta, 1, &bucket).expect_err("out-of-range index should return invalid argument");
        assert!(err.to_string().contains("invalid decommission pool index 1 for 1 pools"));
    }

    #[test]
    fn test_mark_decommission_bucket_done_pops_bucket_when_present() {
        let mut meta = PoolMeta {
            pools: vec![PoolStatus {
                id: 0,
                cmd_line: "pool-0".to_string(),
                last_update: OffsetDateTime::UNIX_EPOCH,
                decommission: Some(PoolDecommissionInfo {
                    queued_buckets: vec!["bucket-a".to_string()],
                    ..Default::default()
                }),
            }],
            ..Default::default()
        };

        let bucket = DecomBucketInfo {
            name: "bucket-a".to_string(),
            prefix: String::new(),
        };
        let popped = mark_decommission_bucket_done(&mut meta, 0, &bucket).expect("valid state should mark bucket done");
        assert!(popped);
    }

    #[test]
    fn test_count_decommission_item_rejects_missing_decommission_meta() {
        let mut meta = PoolMeta {
            pools: vec![PoolStatus {
                id: 0,
                cmd_line: "pool-0".to_string(),
                last_update: OffsetDateTime::UNIX_EPOCH,
                decommission: None,
            }],
            ..Default::default()
        };

        let err = count_decommission_item(&mut meta, 0, 64, true)
            .expect_err("missing decommission metadata should return explicit error");
        assert!(
            err.to_string()
                .contains("failed to count decommission item: decommission metadata not initialized")
        );
    }

    #[test]
    fn test_count_decommission_item_updates_done_and_failed_counters() {
        let mut meta = PoolMeta {
            pools: vec![PoolStatus {
                id: 0,
                cmd_line: "pool-0".to_string(),
                last_update: OffsetDateTime::UNIX_EPOCH,
                decommission: Some(PoolDecommissionInfo::default()),
            }],
            ..Default::default()
        };

        count_decommission_item(&mut meta, 0, 32, false).expect("success counter should be updated");
        count_decommission_item(&mut meta, 0, 16, true).expect("failed counter should be updated");

        let info = meta.pools[0].decommission.as_ref().expect("decommission info should exist");
        assert_eq!(info.items_decommissioned, 1);
        assert_eq!(info.bytes_done, 32);
        assert_eq!(info.items_decommission_failed, 1);
        assert_eq!(info.bytes_failed, 16);
    }

    #[test]
    fn test_track_decommission_current_object_rejects_missing_decommission_meta() {
        let mut meta = PoolMeta {
            pools: vec![PoolStatus {
                id: 0,
                cmd_line: "pool-0".to_string(),
                last_update: OffsetDateTime::UNIX_EPOCH,
                decommission: None,
            }],
            ..Default::default()
        };

        let err = track_decommission_current_object(&mut meta, 0, "bucket-a", "object-a")
            .expect_err("missing decommission metadata should return explicit error");
        assert!(
            err.to_string()
                .contains("failed to track decommission current object: decommission metadata not initialized")
        );
    }

    #[test]
    fn test_track_decommission_current_object_updates_bucket_and_object() {
        let mut meta = PoolMeta {
            pools: vec![PoolStatus {
                id: 0,
                cmd_line: "pool-0".to_string(),
                last_update: OffsetDateTime::UNIX_EPOCH,
                decommission: Some(PoolDecommissionInfo::default()),
            }],
            ..Default::default()
        };

        track_decommission_current_object(&mut meta, 0, "bucket-a", "object-a").expect("valid state should track bucket/object");

        let info = meta.pools[0].decommission.as_ref().expect("decommission info should exist");
        assert_eq!(info.bucket, "bucket-a");
        assert_eq!(info.object, "object-a");
        assert!(info.stage.is_empty());
    }

    #[test]
    fn test_track_decommission_current_object_stage_updates_stage() {
        let mut meta = PoolMeta {
            pools: vec![PoolStatus {
                id: 0,
                cmd_line: "pool-0".to_string(),
                last_update: OffsetDateTime::UNIX_EPOCH,
                decommission: Some(PoolDecommissionInfo::default()),
            }],
            ..Default::default()
        };

        track_decommission_current_object_stage(&mut meta, 0, "bucket-a", "object-a", "cleanup_preflight")
            .expect("valid state should track bucket/object stage");

        let info = meta.pools[0].decommission.as_ref().expect("decommission info should exist");
        assert_eq!(info.bucket, "bucket-a");
        assert_eq!(info.object, "object-a");
        assert_eq!(info.stage, "cleanup_preflight");
    }

    #[test]
    fn test_resolve_decommission_update_after_result_passthrough_ok() {
        let ok = resolve_decommission_update_after_result(Ok(true)).expect("ok value should pass through");
        assert!(ok);
    }

    #[test]
    fn test_resolve_decommission_update_after_result_wraps_error_context() {
        let err = resolve_decommission_update_after_result(ensure_valid_decommission_pool_index(0, 0).map(|_| false))
            .expect_err("invalid argument should be wrapped with context");
        assert!(err.to_string().contains("decommission metadata update failed"));
        assert!(err.to_string().contains("invalid decommission pool index 0 for 0 pools"));
    }

    #[test]
    fn test_resolve_decommission_progress_save_result_returns_none_on_success() {
        assert!(resolve_decommission_progress_save_result(Ok(())).is_none());
    }

    #[test]
    fn test_resolve_decommission_progress_save_result_returns_error_for_best_effort_failure() {
        let err = resolve_decommission_progress_save_result(Err(Error::SlowDown))
            .expect("progress save failure should be returned for logging");

        assert!(err.to_string().contains("decommission progress save failed"));
        assert!(err.to_string().contains(Error::SlowDown.to_string().as_str()));
    }

    #[test]
    fn test_resolve_decommission_preflight_heal_result_passthrough_ok() {
        assert!(resolve_decommission_preflight_heal_result::<()>("bucket-a", Ok(())).is_ok());
    }

    #[test]
    fn test_resolve_decommission_preflight_heal_result_wraps_error_context() {
        let err = resolve_decommission_preflight_heal_result::<()>("bucket-a", Err(Error::SlowDown))
            .expect_err("heal failure should carry preflight context");
        assert!(
            err.to_string()
                .contains("decommission preflight heal failed for bucket bucket-a")
        );
    }

    #[test]
    fn test_resolve_decommission_optional_bucket_config_result_passthrough() {
        let result = resolve_decommission_optional_bucket_config_result("bucket-a", "replication", Ok(42_u8))
            .expect("bucket config should pass through");
        assert_eq!(result, Some(42));
    }

    #[test]
    fn test_resolve_decommission_optional_bucket_config_result_returns_none_for_missing_config() {
        let result =
            resolve_decommission_optional_bucket_config_result::<()>("bucket-a", "versioning", Err(Error::ConfigNotFound))
                .expect("missing bucket config should map to None");
        assert!(result.is_none());
    }

    #[test]
    fn test_resolve_decommission_optional_bucket_config_result_wraps_other_errors() {
        let err = resolve_decommission_optional_bucket_config_result::<()>("bucket-a", "replication", Err(Error::SlowDown))
            .expect_err("unexpected bucket config errors should be wrapped with context");
        assert!(
            err.to_string()
                .contains("decommission replication config load failed for bucket bucket-a")
        );
    }

    #[test]
    fn test_resolve_decommission_entry_cleanup_delete_result_passthrough_ok() {
        assert!(resolve_decommission_entry_cleanup_delete_result(Ok(()), "bucket-a", "obj.txt").is_ok());
    }

    #[test]
    fn test_resolve_decommission_entry_cleanup_delete_result_ignores_not_found() {
        assert!(resolve_decommission_entry_cleanup_delete_result::<()>(Err(Error::FileNotFound), "bucket-a", "obj.txt").is_ok());
    }

    #[test]
    fn test_resolve_decommission_entry_cleanup_delete_result_wraps_error_context() {
        let err = resolve_decommission_entry_cleanup_delete_result::<()>(Err(Error::SlowDown), "bucket-a", "obj.txt")
            .expect_err("cleanup delete failure should be wrapped with explicit context");
        assert!(
            err.to_string()
                .contains("decommission cleanup_delete_object failed for bucket-a/obj.txt")
        );
    }

    #[test]
    fn test_resolve_decommission_entry_reload_result_passthrough_ok() {
        assert!(resolve_decommission_entry_reload_result(Ok(()), "bucket-a", "obj.txt").is_ok());
    }

    #[test]
    fn test_resolve_decommission_entry_reload_result_wraps_error_context() {
        let err = resolve_decommission_entry_reload_result(Err(Error::SlowDown), "bucket-a", "obj.txt")
            .expect_err("reload failure should be wrapped with explicit context");
        assert!(
            err.to_string()
                .contains("decommission reload_pool_meta failed for bucket-a/obj.txt")
        );
    }

    #[test]
    fn test_resolve_decommission_terminal_mark_result_passthrough_ok() {
        assert!(resolve_decommission_terminal_mark_result(Ok(()), "completed", "pool-a").is_ok());
    }

    #[test]
    fn test_resolve_decommission_terminal_mark_result_wraps_error_context() {
        let err = resolve_decommission_terminal_mark_result(Err(Error::SlowDown), "failed", "pool-a")
            .expect_err("terminal mark failure should include stage and pool context");
        let message = err.to_string();
        assert!(message.contains("decommission terminal mark failed failed for pool pool-a"));
    }

    #[test]
    fn test_resolve_decommission_terminal_mark_after_error_result_passthrough_ok() {
        assert!(resolve_decommission_terminal_mark_after_error_result(Ok(()), 3, &Error::SlowDown).is_ok());
    }

    #[test]
    fn test_resolve_decommission_terminal_mark_after_error_result_wraps_error_context() {
        let err = resolve_decommission_terminal_mark_after_error_result(Err(Error::OperationCanceled), 3, &Error::SlowDown)
            .expect_err("terminal mark after-error failure should include both errors");
        let message = err.to_string();
        assert!(message.contains("decommission terminal mark failed after background error on pool 3"));
        assert!(message.contains("mark error"));
    }

    #[test]
    fn test_observe_decommission_terminal_reload_result_returns_none_on_success() {
        assert!(observe_decommission_terminal_reload_result(Ok(()), "complete_decommission for pool 3").is_none());
    }

    #[test]
    fn test_observe_decommission_terminal_reload_result_keeps_failure_for_logging() {
        let err = observe_decommission_terminal_reload_result(Err(Error::SlowDown), "decommission_failed for pool 3")
            .expect("reload failure should be observable");
        let message = err.to_string();
        assert!(message.contains("decommission terminal pool meta reload failed during decommission_failed for pool 3"));
        assert!(message.contains(Error::SlowDown.to_string().as_str()));
    }

    #[test]
    fn test_decommission_item_size_converts_positive_values() {
        assert_eq!(decommission_item_size(42_i64), 42);
    }

    #[test]
    fn test_decommission_item_size_clamps_negative_values_to_zero() {
        assert_eq!(decommission_item_size(-1_i64), 0);
    }

    #[test]
    fn test_new_multipart_abort_flag_defaults_to_abort_enabled() {
        let flag = data_movement::new_multipart_abort_flag();
        assert!(data_movement::should_abort_multipart_upload(&flag));
    }

    #[test]
    fn test_mark_multipart_upload_completed_disables_abort_cleanup() {
        let flag = data_movement::new_multipart_abort_flag();
        data_movement::mark_multipart_upload_completed(&flag);
        assert!(!data_movement::should_abort_multipart_upload(&flag));
    }

    #[test]
    fn test_decode_part_index_returns_some_for_valid_payload() {
        let mut index = Index::new();
        index.add(0, 0).expect("first index entry should be accepted");
        index
            .add(2_097_152, 2_097_152)
            .expect("second index entry should advance totals");

        let encoded = index.into_vec();
        let decoded = data_movement::decode_part_index(Some(&encoded)).expect("valid index payload should decode");

        assert_eq!(decoded.total_uncompressed, 2_097_152);
        assert_eq!(decoded.total_compressed, 2_097_152);
    }

    #[test]
    fn test_with_decommission_entry_context_formats_stage_bucket_and_object() {
        let err = with_decommission_entry_context("update_after", "bucket-a", "obj.txt", Error::SlowDown);
        let message = err.to_string();
        assert!(message.contains("decommission entry update_after failed"));
        assert!(message.contains("bucket bucket-a"));
        assert!(message.contains("object obj.txt"));
    }

    #[test]
    fn test_load_decommission_entry_versions_wraps_parse_errors_with_context() {
        let entry = MetaCacheEntry {
            name: "obj.txt".to_string(),
            metadata: vec![1, 2, 3],
            cached: None,
            reusable: false,
        };

        let err = load_decommission_entry_versions(&entry, "bucket-a", "check_after_decommission.file_info_versions")
            .expect_err("invalid metadata should fail");
        let message = err.to_string();
        assert!(message.contains("decommission entry check_after_decommission.file_info_versions failed"));
        assert!(message.contains("bucket bucket-a"));
        assert!(message.contains("object obj.txt"));
    }

    #[test]
    fn test_resolve_decommission_entry_exact_versions_preserves_full_parts() {
        let entry = MetaCacheEntry {
            name: "obj.txt".to_string(),
            metadata: Vec::new(),
            cached: None,
            reusable: false,
        };
        let fivs = FileInfoVersions {
            volume: "bucket-a".to_string(),
            name: "obj.txt".to_string(),
            versions: vec![FileInfo {
                name: "obj.txt".to_string(),
                parts: vec![ObjectPartInfo {
                    number: 1,
                    etag: "part-etag".to_string(),
                    size: 128,
                    actual_size: 128,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };

        let resolved = resolve_decommission_entry_exact_versions(Ok(Some(fivs)), &entry, "bucket-a", "file_info_versions")
            .expect("exact versions should be preserved");

        assert_eq!(resolved.versions[0].parts.len(), 1);
        assert_eq!(resolved.versions[0].parts[0].etag, "part-etag");
    }

    #[test]
    fn test_resolve_decommission_entry_exact_versions_uses_empty_when_source_missing() {
        let entry = MetaCacheEntry {
            name: "obj.txt".to_string(),
            metadata: Vec::new(),
            cached: None,
            reusable: false,
        };

        let resolved = resolve_decommission_entry_exact_versions(Ok(None), &entry, "bucket-a", "file_info_versions")
            .expect("missing source metadata should be treated as empty");

        assert_eq!(resolved.volume, "bucket-a");
        assert_eq!(resolved.name, "obj.txt");
        assert!(resolved.versions.is_empty());
    }

    #[test]
    fn test_resolve_decommission_check_after_list_result_prefers_entry_error() {
        let err = resolve_decommission_check_after_list_result(Err(Error::OperationCanceled), Some(Error::SlowDown))
            .expect_err("entry error should win over cancellation");
        assert!(matches!(err, Error::SlowDown));
    }

    #[test]
    fn test_resolve_decommission_partial_listing_entry_rejects_unresolved_metadata() {
        let generation = OffsetDateTime::now_utc();
        let unresolved_entry = resolve_decommission_partial_listing_entry(
            MetaCacheEntries(vec![None]),
            MetadataResolutionParams {
                dir_quorum: 2,
                obj_quorum: 2,
                bucket: "bucket-a".to_string(),
                ..Default::default()
            },
            "bucket-a",
            "prefix/",
            1,
            2,
            3,
            generation,
        )
        .expect_err("unresolved partial listing must fail closed");

        assert_eq!(unresolved_entry.bucket, "bucket-a");
        assert_eq!(unresolved_entry.object, "prefix/");
        assert_eq!(unresolved_entry.pool_index, 2);
        assert_eq!(unresolved_entry.set_index, 3);
        assert_eq!(unresolved_entry.source_generation, generation);
        assert_eq!(unresolved_entry.candidate_count, 0);
        assert_eq!(unresolved_entry.disk_error_count, 1);
        assert_eq!(unresolved_entry.reason, "metadata_resolution_failed");

        let message = decommission_unresolved_listing_error(&unresolved_entry).to_string();
        assert!(message.contains("decommission listing could not resolve metadata"));
        assert!(message.contains("bucket-a/prefix/"));
        assert!(message.contains("pool 2 set 3"));
        assert!(message.contains("1 disk error(s)"));
    }

    #[test]
    fn unresolved_entry_ledger_requires_individual_verification_before_completion() {
        let generation = OffsetDateTime::now_utc();
        let mut pool_meta = PoolMeta {
            version: POOL_META_VERSION,
            pools: vec![PoolStatus {
                id: 0,
                cmd_line: "/data/pool".to_string(),
                last_update: generation,
                decommission: Some(PoolDecommissionInfo {
                    start_time: Some(generation),
                    ..Default::default()
                }),
            }],
            dont_save: true,
        };
        let unresolved_entry = DecommissionUnresolvedEntry {
            bucket: "bucket-a".to_string(),
            object: "object-a".to_string(),
            pool_index: 0,
            set_index: 0,
            source_generation: generation,
            candidate_count: 1,
            disk_error_count: 0,
            observed_at: generation,
            reason: "metadata_resolution_failed".to_string(),
        };

        assert!(
            record_decommission_unresolved_entry(&mut pool_meta, 0, generation, unresolved_entry.clone())
                .expect("active generation should accept unresolved entry")
        );
        let err = reconcile_decommission_unresolved_entries_for_completion(&mut pool_meta, 0, None, None)
            .expect_err("unverified completion must retain unresolved entries");
        assert!(err.to_string().contains("1 unresolved listing entries remain"));
        assert_eq!(
            pool_meta.pools[0]
                .decommission
                .as_ref()
                .expect("decommission should exist")
                .unresolved_entries
                .len(),
            1
        );

        let err = reconcile_decommission_unresolved_entries_for_completion(&mut pool_meta, 0, Some(generation), None)
            .expect_err("a same-generation sweep without entry proof must retain the ledger");
        assert!(err.to_string().contains("not individually verified"));

        let stale_verified = vec![unresolved_entry.clone()];
        let mut replacement = unresolved_entry;
        replacement.disk_error_count = 2;
        replacement.observed_at = generation + Duration::seconds(1);
        assert!(
            record_decommission_unresolved_entry(&mut pool_meta, 0, generation, replacement.clone())
                .expect("a newer observation should replace the ledger entry")
        );
        let err =
            reconcile_decommission_unresolved_entries_for_completion(&mut pool_meta, 0, Some(generation), Some(&stale_verified))
                .expect_err("stale entry verification must not clear a concurrent replacement");
        assert!(err.to_string().contains("not individually verified"));

        let verified = vec![replacement];
        reconcile_decommission_unresolved_entries_for_completion(&mut pool_meta, 0, Some(generation), Some(&verified))
            .expect("individually verified entries should reconcile");
        assert!(
            pool_meta.pools[0]
                .decommission
                .as_ref()
                .expect("decommission should exist")
                .unresolved_entries
                .is_empty()
        );
    }

    #[tokio::test]
    async fn final_sweep_persists_unresolved_entry_from_real_listing() {
        let (dirs, store) = metadata_sys::test_support::isolated_store_over_temp_disks().await;
        let bucket = "decommission-final-sweep-unresolved";
        let object = "corrupt-object";
        metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        store
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("test bucket should be created");

        let generation = OffsetDateTime::now_utc();
        {
            let mut pool_meta = store.pool_meta.write().await;
            pool_meta.dont_save = false;
            pool_meta.pools[0].decommission = Some(PoolDecommissionInfo {
                start_time: Some(generation),
                ..Default::default()
            });
        }
        store
            .save_current_pool_meta_for_test(&[0])
            .await
            .expect("active decommission metadata should be persisted before the final sweep");

        for (disk_index, dir) in dirs.iter().enumerate() {
            let object_dir = dir.path().join(bucket).join(object);
            tokio::fs::create_dir_all(&object_dir)
                .await
                .expect("corrupt object directory should be created");
            tokio::fs::write(object_dir.join(STORAGE_FORMAT_FILE), format!("corrupt-xl-meta-{disk_index}").into_bytes())
                .await
                .expect("divergent corrupt metadata should be written");
        }

        let err = store
            .check_after_decommission(0, &CancellationToken::new(), generation)
            .await
            .expect_err("real final sweep must fail closed on unresolved listing metadata");
        assert!(
            err.to_string().contains("decommission listing could not resolve metadata"),
            "unexpected final sweep error: {err:?}"
        );

        let in_memory_entries = store.pool_meta.read().await.pools[0]
            .decommission
            .as_ref()
            .expect("decommission should remain active")
            .unresolved_entries
            .clone();
        assert_eq!(in_memory_entries.len(), 1);
        let unresolved_entry = &in_memory_entries[0];
        assert_eq!(unresolved_entry.bucket, bucket);
        assert_eq!(unresolved_entry.object, object);
        assert_eq!(unresolved_entry.pool_index, 0);
        assert_eq!(unresolved_entry.set_index, 0);
        assert_eq!(unresolved_entry.source_generation, generation);
        assert_eq!(unresolved_entry.candidate_count, 4);
        assert_eq!(unresolved_entry.disk_error_count, 0);
        assert_eq!(unresolved_entry.reason, "metadata_resolution_failed");

        let mut restored = PoolMeta::default();
        restored
            .load_no_lock_from_replicas(vec![store.pools[0].clone()])
            .await
            .expect("persisted pool metadata should reload after the final-sweep failure");
        assert_eq!(
            restored.pools[0]
                .decommission
                .as_ref()
                .expect("reloaded decommission should exist")
                .unresolved_entries,
            in_memory_entries
        );
    }

    #[tokio::test]
    async fn unresolved_entry_probe_verifies_absence_on_every_source_disk() {
        let (_dirs, store) = metadata_sys::test_support::isolated_store_over_temp_disks().await;
        let bucket = "decommission-final-sweep-absent-ledger";
        store
            .peer_sys
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("test bucket should be created");
        metadata_sys::init_bucket_metadata_sys(store.clone(), vec![bucket.to_string()]).await;

        let generation = OffsetDateTime::now_utc();
        let unresolved_entry = DecommissionUnresolvedEntry {
            bucket: bucket.to_string(),
            object: "absent-object".to_string(),
            pool_index: 0,
            set_index: 0,
            source_generation: generation,
            candidate_count: 1,
            disk_error_count: 1,
            observed_at: generation,
            reason: "metadata_resolution_failed".to_string(),
        };
        {
            let mut pool_meta = store.pool_meta.write().await;
            pool_meta.version = POOL_META_VERSION;
            pool_meta.pools[0].decommission = Some(PoolDecommissionInfo {
                start_time: Some(generation),
                unresolved_entries: vec![unresolved_entry.clone()],
                ..Default::default()
            });
        }

        assert!(
            store.pools[0].disk_set[0]
                .decommission_unresolved_entry_absent_on_all_disks(0, &unresolved_entry)
                .await
                .expect("all source disks should be readable")
        );
        let verified = vec![unresolved_entry.clone()];

        let mut pool_meta = store.pool_meta.write().await;
        reconcile_decommission_unresolved_entries_for_completion(&mut pool_meta, 0, Some(generation), Some(&verified))
            .expect("the individually verified entry should reconcile");
        assert!(
            pool_meta.pools[0]
                .decommission
                .as_ref()
                .expect("decommission should remain present")
                .unresolved_entries
                .is_empty()
        );
    }

    #[tokio::test]
    async fn test_record_decommission_entry_error_cancels_listing_and_preserves_first_error() {
        let entry_error = Arc::new(tokio::sync::Mutex::new(None));
        let rx = CancellationToken::new();

        assert!(record_decommission_entry_error(&entry_error, &rx, Error::SlowDown).await);
        assert!(!record_decommission_entry_error(&entry_error, &rx, Error::OperationCanceled).await);

        assert!(rx.is_cancelled());
        assert!(matches!(*entry_error.lock().await, Some(Error::SlowDown)));
    }

    #[tokio::test]
    async fn test_record_decommission_entry_error_ignores_already_canceled_listing() {
        let entry_error = Arc::new(tokio::sync::Mutex::new(None));
        let rx = CancellationToken::new();
        rx.cancel();

        assert!(!record_decommission_entry_error(&entry_error, &rx, Error::SlowDown).await);

        assert!(entry_error.lock().await.is_none());
    }

    #[test]
    fn unresolved_entry_verification_requires_every_source_disk() {
        assert!(ensure_decommission_unresolved_verification_disk_count(4, 4, 0, 1).is_ok());

        let err = ensure_decommission_unresolved_verification_disk_count(4, 3, 0, 1)
            .expect_err("an offline source disk must block ledger reconciliation");
        assert!(err.to_string().contains("requires all 4 source disks, but only 3 are online"));
    }

    #[test]
    fn test_resolve_decommission_listing_error_preserves_real_listing_failure() {
        let err = resolve_decommission_listing_error(Some(Error::SlowDown), Some(Error::OperationCanceled))
            .expect("listing failure should be returned");
        assert!(matches!(err, Error::SlowDown));

        let err = resolve_decommission_listing_error(Some(Error::OperationCanceled), Some(Error::SlowDown))
            .expect("entry failure should be returned");
        assert!(matches!(err, Error::SlowDown));
    }

    #[test]
    fn test_resolve_decommission_check_after_list_result_returns_list_result_without_entry_error() {
        let err = resolve_decommission_check_after_list_result(Err(Error::OperationCanceled), None)
            .expect_err("list result should be preserved without entry error");
        assert!(matches!(err, Error::OperationCanceled));
    }

    #[test]
    fn test_resolve_decommission_pool_meta_reload_result_passthrough_ok() {
        assert!(resolve_decommission_pool_meta_reload_result(Ok(()), "start_decommission").is_ok());
    }

    #[test]
    fn test_resolve_decommission_pool_meta_reload_result_wraps_error_context() {
        let err = resolve_decommission_pool_meta_reload_result(Err(Error::SlowDown), "decommission_failed for pool 3")
            .expect_err("reload failure should be wrapped with stage context");
        let message = err.to_string();
        assert!(message.contains("decommission pool meta reload failed during decommission_failed for pool 3"));
        assert!(message.contains(Error::SlowDown.to_string().as_str()));
    }

    #[test]
    fn test_resolve_start_decommission_pool_meta_reload_result_returns_failure() {
        let err = resolve_start_decommission_pool_meta_reload_result(Err(Error::other(
            "reload_pool_meta encountered 1 failure(s): peer[0] reload_pool_meta failed",
        )))
        .expect_err("start_decommission must fail when peer pool meta reload fails");
        let message = err.to_string();

        assert!(message.contains("decommission pool meta reload failed during start_decommission"));
        assert!(message.contains("reload_pool_meta encountered 1 failure(s)"));
        assert!(message.contains("peer[0]"));
    }

    #[test]
    fn test_resolve_decommission_listing_worker_result_passthrough_ok() {
        assert!(resolve_decommission_listing_worker_result(2, Ok(Ok(()))).is_ok());
    }

    #[test]
    fn test_resolve_decommission_listing_worker_result_passthrough_worker_error() {
        let err = resolve_decommission_listing_worker_result(2, Ok(Err(Error::SlowDown)))
            .expect_err("listing worker error should be returned");

        assert!(matches!(err, Error::SlowDown));
    }

    #[tokio::test]
    async fn test_resolve_decommission_listing_worker_result_wraps_join_error_context() {
        let join_error = tokio::spawn(async {
            panic!("listing worker panic");
        })
        .await
        .expect_err("panic task should return JoinError");

        let err = resolve_decommission_listing_worker_result(4, Err(join_error))
            .expect_err("join error should be wrapped with context");
        let message = err.to_string();
        assert!(message.contains("decommission listing worker 4 task join error"));
        assert!(message.contains("panic"));
    }

    #[test]
    fn test_should_retry_decommission_listing_respects_attempt_limit_and_bucket_missing() {
        assert!(should_retry_decommission_listing(&Error::SlowDown, 0, 2));
        assert!(!should_retry_decommission_listing(&Error::SlowDown, 1, 2));
        assert!(!should_retry_decommission_listing(
            &StorageError::BucketNotFound("bucket".to_string()),
            0,
            2
        ));
    }

    #[tokio::test]
    async fn test_wait_decommission_retry_backoff_reports_canceled_without_sleeping() {
        let token = CancellationToken::new();
        token.cancel();

        assert!(wait_decommission_retry_backoff(&token, StdDuration::from_secs(30)).await);
    }

    #[test]
    fn test_decommission_retry_backoff_delay_grows_linearly() {
        let base = StdDuration::from_millis(100);

        assert_eq!(decommission_retry_backoff_delay(base, 1), base);
        assert_eq!(decommission_retry_backoff_delay(base, 3), StdDuration::from_millis(300));
        assert_eq!(decommission_retry_backoff_delay(base, usize::MAX), base.saturating_mul(u32::MAX));
    }

    #[test]
    fn test_source_changed_exhaustion_fails_only_after_pool_limit() {
        assert!(!should_fail_decommission_pool_after_exhausted_source_changed(
            DECOMMISSION_SOURCE_CHANGED_EXHAUSTION_LIMIT
        ));
        assert!(should_fail_decommission_pool_after_exhausted_source_changed(
            DECOMMISSION_SOURCE_CHANGED_EXHAUSTION_LIMIT + 1
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn test_run_decommission_listing_with_retry_stops_after_attempt_limit() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let err = run_decommission_listing_with_retry(
            CancellationToken::new(),
            "bucket-a".to_string(),
            noop_decommission_list_callback(),
            1,
            2,
            3,
            {
                let attempts = attempts.clone();
                move |_| {
                    let attempts = attempts.clone();
                    async move {
                        attempts.fetch_add(1, Ordering::SeqCst);
                        Err(Error::SlowDown)
                    }
                }
            },
        )
        .await
        .expect_err("permanent listing failure must not retry forever");

        assert_eq!(attempts.load(Ordering::SeqCst), 3);
        assert!(err.to_string().contains("attempt 3/3"));
    }

    #[tokio::test]
    async fn test_run_decommission_listing_with_retry_treats_bucket_missing_as_complete() {
        let attempts = Arc::new(AtomicUsize::new(0));
        run_decommission_listing_with_retry(
            CancellationToken::new(),
            "bucket-a".to_string(),
            noop_decommission_list_callback(),
            1,
            2,
            3,
            {
                let attempts = attempts.clone();
                move |_| {
                    let attempts = attempts.clone();
                    async move {
                        attempts.fetch_add(1, Ordering::SeqCst);
                        Err(StorageError::BucketNotFound("bucket-a".to_string()))
                    }
                }
            },
        )
        .await
        .expect("missing bucket should keep previous decommission listing behavior");

        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_should_count_decommission_version_complete_for_cleanup_safe_ignored_result() {
        assert!(should_count_decommission_version_complete(true, true, false));
    }

    #[test]
    fn test_should_count_decommission_version_complete_rejects_skip_only_ignored_result() {
        assert!(!should_count_decommission_version_complete(true, false, false));
    }

    #[test]
    fn test_should_count_decommission_version_complete_for_completed_result() {
        assert!(should_count_decommission_version_complete(false, false, false));
    }

    #[test]
    fn test_should_count_decommission_version_complete_rejects_failed_result() {
        assert!(!should_count_decommission_version_complete(false, false, true));
    }

    #[test]
    fn test_should_cleanup_decommission_source_entry_accepts_all_versions_completed() {
        assert!(should_cleanup_decommission_source_entry(3, 3, 0));
    }

    #[test]
    fn test_should_cleanup_decommission_source_entry_accepts_migrated_and_safely_expired_versions() {
        assert!(should_cleanup_decommission_source_entry(1, 2, 1));
    }

    #[test]
    fn test_should_cleanup_decommission_source_entry_accepts_versions_only_safely_expired_by_lifecycle() {
        assert!(should_cleanup_decommission_source_entry(0, 2, 2));
    }

    #[test]
    fn test_should_cleanup_decommission_source_entry_rejects_object_lock_retained_version() {
        assert!(!should_cleanup_decommission_source_entry(1, 2, 0));
    }

    #[test]
    fn test_should_cleanup_decommission_source_entry_rejects_replication_pending_version() {
        assert!(!should_cleanup_decommission_source_entry(2, 3, 0));
    }

    #[test]
    fn test_should_cleanup_decommission_source_entry_rejects_counter_overrun() {
        assert!(!should_cleanup_decommission_source_entry(2, 2, 1));
    }

    #[test]
    fn test_pool_meta_update_after_rejects_out_of_range_index() {
        let mut meta = PoolMeta::default();
        let err = meta
            .update_after(1, Duration::seconds(1))
            .expect_err("out-of-range index should fail");
        assert!(err.to_string().contains("invalid decommission pool index 1 for 0 pools"));
    }

    #[test]
    fn test_pool_meta_update_after_rejects_when_decommission_missing() {
        let mut meta = PoolMeta {
            pools: vec![PoolStatus {
                id: 0,
                cmd_line: "pool-0".to_string(),
                last_update: OffsetDateTime::UNIX_EPOCH,
                decommission: None,
            }],
            ..Default::default()
        };

        let err = meta
            .update_after(0, Duration::seconds(1))
            .expect_err("pool without decommission should fail");
        assert!(
            err.to_string()
                .contains("failed to update decommission metadata timestamp: decommission metadata not initialized")
        );
    }

    #[test]
    fn test_track_decommission_stage_does_not_advance_checkpoint_state() {
        let mut meta = PoolMeta {
            pools: vec![PoolStatus {
                id: 0,
                cmd_line: "pool-0".to_string(),
                last_update: OffsetDateTime::UNIX_EPOCH,
                decommission: Some(PoolDecommissionInfo {
                    items_decommissioned: 3,
                    items_decommission_failed: 2,
                    ..Default::default()
                }),
            }],
            ..Default::default()
        };

        track_decommission_current_object_stage(&mut meta, 0, "bucket", "object", "migrate_object")
            .expect("valid decommission progress should be tracked");

        assert_eq!(meta.pools[0].last_update, OffsetDateTime::UNIX_EPOCH);
        let info = meta.pools[0].decommission.as_ref().expect("decommission info should exist");
        assert_eq!(info.items_since_last_progress_save(), 5);
        assert_eq!(info.stage, "migrate_object");
    }

    #[test]
    fn test_pool_meta_update_after_skips_before_time_and_item_thresholds() {
        let mut meta = PoolMeta {
            pools: vec![PoolStatus {
                id: 0,
                cmd_line: "pool-0".to_string(),
                last_update: OffsetDateTime::now_utc(),
                decommission: Some(PoolDecommissionInfo {
                    items_decommissioned: DECOMMISSION_PROGRESS_SAVE_ITEM_THRESHOLD - 1,
                    ..Default::default()
                }),
            }],
            ..Default::default()
        };

        let saved = meta
            .update_after(0, DECOMMISSION_PROGRESS_SAVE_INTERVAL)
            .expect("valid decommission state should update");

        assert!(!saved);
        let info = meta.pools[0].decommission.as_ref().expect("decommission info should exist");
        assert_eq!(info.items_since_last_progress_save(), DECOMMISSION_PROGRESS_SAVE_ITEM_THRESHOLD - 1);
    }

    #[test]
    fn test_pool_meta_update_after_requests_save_when_item_threshold_reached() {
        let mut meta = PoolMeta {
            pools: vec![PoolStatus {
                id: 0,
                cmd_line: "pool-0".to_string(),
                last_update: OffsetDateTime::now_utc(),
                decommission: Some(PoolDecommissionInfo {
                    items_decommissioned: DECOMMISSION_PROGRESS_SAVE_ITEM_THRESHOLD,
                    ..Default::default()
                }),
            }],
            ..Default::default()
        };

        let saved = meta
            .update_after(0, DECOMMISSION_PROGRESS_SAVE_INTERVAL)
            .expect("item threshold should save progress");

        assert!(saved);
        let info = meta.pools[0].decommission.as_ref().expect("decommission info should exist");
        assert_eq!(info.items_since_last_progress_save(), DECOMMISSION_PROGRESS_SAVE_ITEM_THRESHOLD);
    }

    #[test]
    fn test_pool_meta_update_after_requests_save_when_time_threshold_reached() {
        let mut meta = PoolMeta {
            pools: vec![PoolStatus {
                id: 0,
                cmd_line: "pool-0".to_string(),
                last_update: OffsetDateTime::now_utc() - DECOMMISSION_PROGRESS_SAVE_INTERVAL,
                decommission: Some(PoolDecommissionInfo {
                    items_decommissioned: 1,
                    ..Default::default()
                }),
            }],
            ..Default::default()
        };

        let saved = meta
            .update_after(0, DECOMMISSION_PROGRESS_SAVE_INTERVAL)
            .expect("time threshold should save progress");

        assert!(saved);
        let info = meta.pools[0].decommission.as_ref().expect("decommission info should exist");
        assert_eq!(info.items_since_last_progress_save(), 1);
    }

    #[test]
    fn test_pool_meta_update_after_does_not_advance_last_update_before_save() {
        let last_update = OffsetDateTime::UNIX_EPOCH;
        let mut meta = PoolMeta {
            pools: vec![PoolStatus {
                id: 0,
                cmd_line: "pool-0".to_string(),
                last_update,
                decommission: Some(PoolDecommissionInfo {
                    start_time: Some(last_update),
                    items_decommissioned: DECOMMISSION_PROGRESS_SAVE_ITEM_THRESHOLD,
                    ..Default::default()
                }),
            }],
            ..Default::default()
        };

        assert!(
            meta.update_after(0, DECOMMISSION_PROGRESS_SAVE_INTERVAL)
                .expect("item threshold should request a checkpoint")
        );
        assert_eq!(meta.pools[0].last_update, last_update);
    }

    #[test]
    fn test_decommission_progress_checkpoint_commits_exact_snapshot_watermark() {
        let start_time = OffsetDateTime::UNIX_EPOCH;
        let checkpoint_at = start_time + Duration::seconds(30);
        let mut meta = PoolMeta {
            pools: vec![PoolStatus {
                id: 0,
                cmd_line: "pool-0".to_string(),
                last_update: start_time,
                decommission: Some(PoolDecommissionInfo {
                    start_time: Some(start_time),
                    items_decommissioned: DECOMMISSION_PROGRESS_SAVE_ITEM_THRESHOLD,
                    ..Default::default()
                }),
            }],
            ..Default::default()
        };

        let checkpoint = meta
            .decommission_progress_checkpoint(0, DECOMMISSION_PROGRESS_SAVE_INTERVAL, checkpoint_at)
            .expect("valid decommission state should produce a checkpoint")
            .expect("item threshold should produce a checkpoint");
        meta.count_item(0, 1, false);

        assert!(meta.commit_decommission_progress_checkpoint(0, checkpoint));
        let info = meta.pools[0].decommission.as_ref().expect("decommission info should exist");
        assert_eq!(info.progress_save_item_baseline, checkpoint.counted_items);
        assert_eq!(info.items_since_last_progress_save(), 1);
        assert_eq!(meta.pools[0].last_update, checkpoint_at);
    }

    #[test]
    fn test_decommission_progress_checkpoint_backoff_does_not_advance_baseline() {
        let start_time = OffsetDateTime::UNIX_EPOCH;
        let checkpoint_at = start_time + Duration::seconds(30);
        let retry_after = checkpoint_at + DECOMMISSION_PROGRESS_SAVE_RETRY_BACKOFF;
        let mut meta = PoolMeta {
            pools: vec![PoolStatus {
                id: 0,
                cmd_line: "pool-0".to_string(),
                last_update: start_time,
                decommission: Some(PoolDecommissionInfo {
                    start_time: Some(start_time),
                    items_decommissioned: DECOMMISSION_PROGRESS_SAVE_ITEM_THRESHOLD,
                    ..Default::default()
                }),
            }],
            ..Default::default()
        };

        let checkpoint = meta
            .decommission_progress_checkpoint(0, DECOMMISSION_PROGRESS_SAVE_INTERVAL, checkpoint_at)
            .expect("valid decommission state should produce a checkpoint")
            .expect("item threshold should produce a checkpoint");
        meta.defer_decommission_progress_checkpoint(0, checkpoint, retry_after);

        assert!(
            meta.decommission_progress_checkpoint(0, DECOMMISSION_PROGRESS_SAVE_INTERVAL, checkpoint_at)
                .expect("retry backoff check should succeed")
                .is_none()
        );
        assert_eq!(meta.pools[0].last_update, start_time);
        assert_eq!(
            meta.pools[0]
                .decommission
                .as_ref()
                .expect("decommission info should exist")
                .progress_save_item_baseline,
            0
        );
    }

    #[test]
    fn test_decommission_progress_checkpoint_count_scales_with_threshold() {
        let start_time = OffsetDateTime::UNIX_EPOCH;
        let checkpoint_at = start_time;
        let mut meta = PoolMeta {
            pools: vec![PoolStatus {
                id: 0,
                cmd_line: "pool-0".to_string(),
                last_update: start_time,
                decommission: Some(PoolDecommissionInfo {
                    start_time: Some(start_time),
                    ..Default::default()
                }),
            }],
            ..Default::default()
        };
        let mut checkpoint_count = 0;

        for _ in 0..(DECOMMISSION_PROGRESS_SAVE_ITEM_THRESHOLD * 10) {
            meta.count_item(0, 1, false);
            if let Some(checkpoint) = meta
                .decommission_progress_checkpoint(0, DECOMMISSION_PROGRESS_SAVE_INTERVAL, checkpoint_at)
                .expect("valid decommission state should produce a checkpoint")
            {
                checkpoint_count += 1;
                assert!(meta.commit_decommission_progress_checkpoint(0, checkpoint));
            }
        }

        assert_eq!(checkpoint_count, 10);
    }

    #[test]
    fn test_ensure_decommission_not_rebalancing_rejects_running_rebalance() {
        let err = ensure_decommission_not_rebalancing(true).expect_err("rebalance running should be rejected");
        assert!(matches!(err, Error::RebalanceAlreadyRunning));
    }

    #[test]
    fn test_ensure_decommission_not_rebalancing_allows_idle() {
        assert!(ensure_decommission_not_rebalancing(false).is_ok());
    }

    #[test]
    fn test_ensure_decommission_start_rebalance_meta_allowed_rejects_active_rebalance() {
        let meta = RebalanceMeta {
            pool_stats: vec![RebalanceStats {
                participating: true,
                info: RebalanceInfo {
                    status: RebalStatus::Started,
                    ..Default::default()
                },
                ..Default::default()
            }],
            ..Default::default()
        };

        let err = ensure_decommission_start_rebalance_meta_allowed(Some(&meta))
            .expect_err("persisted active rebalance should block decommission start");

        assert!(matches!(err, Error::RebalanceAlreadyRunning));
    }

    #[test]
    fn test_ensure_decommission_start_rebalance_meta_allowed_rejects_stopping_rebalance() {
        let meta = RebalanceMeta {
            pool_stats: vec![RebalanceStats {
                info: RebalanceInfo {
                    status: RebalStatus::Started,
                    stopping: true,
                    ..Default::default()
                },
                ..Default::default()
            }],
            ..Default::default()
        };

        let err = ensure_decommission_start_rebalance_meta_allowed(Some(&meta))
            .expect_err("persisted stopping rebalance should block decommission start");

        assert!(matches!(err, Error::RebalanceAlreadyRunning));
    }

    #[test]
    fn test_ensure_decommission_start_rebalance_meta_allowed_allows_terminal_or_missing_rebalance() {
        let terminal_meta = RebalanceMeta {
            pool_stats: vec![RebalanceStats {
                participating: true,
                info: RebalanceInfo {
                    status: RebalStatus::Completed,
                    ..Default::default()
                },
                ..Default::default()
            }],
            ..Default::default()
        };

        assert!(ensure_decommission_start_rebalance_meta_allowed(Some(&terminal_meta)).is_ok());
        assert!(ensure_decommission_start_rebalance_meta_allowed(None).is_ok());
    }

    #[test]
    fn test_ensure_local_decommission_pool_leaders_allows_local_first_endpoint() {
        let endpoints = EndpointServerPools::from(vec![
            decommission_test_pool_endpoint(0, false),
            decommission_test_pool_endpoint(1, true),
        ]);

        assert!(ensure_local_decommission_pool_leaders(&endpoints, &[1]).is_ok());
    }

    #[test]
    fn test_ensure_local_decommission_pool_leaders_rejects_remote_first_endpoint() {
        let endpoints = EndpointServerPools::from(vec![decommission_test_pool_endpoint(0, false)]);

        let err = ensure_local_decommission_pool_leaders(&endpoints, &[0])
            .expect_err("remote first endpoint should reject local decommission start");

        assert!(err.to_string().contains("must run on the pool first endpoint"));
    }

    #[test]
    fn test_ensure_local_decommission_pool_leaders_rejects_empty_endpoints() {
        let endpoints = EndpointServerPools::from(vec![PoolEndpoints {
            legacy: false,
            set_count: 1,
            drives_per_set: 1,
            endpoints: Endpoints::from(Vec::<Endpoint>::new()),
            cmd_line: "pool-0".to_string(),
            platform: String::new(),
        }]);

        let err = ensure_local_decommission_pool_leaders(&endpoints, &[0])
            .expect_err("pool without endpoints should reject local decommission start");

        assert!(err.to_string().contains("has no configured endpoints"));
    }

    #[test]
    fn test_decommission_meta_bucket_options_are_idempotent() {
        let opts = decommission_meta_bucket_options();

        assert!(opts.force_create);
    }

    #[test]
    fn test_is_decommission_active_true_only_when_not_terminal() {
        assert!(is_decommission_active(false, false, false));
        assert!(!is_decommission_active(true, false, false));
        assert!(!is_decommission_active(false, true, false));
        assert!(!is_decommission_active(false, false, true));
    }

    #[test]
    fn test_ensure_decommission_generation_rejects_stale_or_queued_workers() {
        let generation = OffsetDateTime::UNIX_EPOCH;
        let mut meta = PoolMeta {
            pools: vec![PoolStatus {
                id: 0,
                cmd_line: "pool-0".to_string(),
                last_update: generation,
                decommission: Some(PoolDecommissionInfo {
                    start_time: Some(generation),
                    ..Default::default()
                }),
            }],
            ..Default::default()
        };

        assert!(ensure_decommission_generation(&meta, 0, generation).is_ok());
        assert!(ensure_decommission_generation(&meta, 0, generation + Duration::seconds(1)).is_err());

        meta.pools[0]
            .decommission
            .as_mut()
            .expect("decommission metadata should exist")
            .queued = true;
        assert!(ensure_decommission_generation(&meta, 0, generation).is_err());

        let replacement_generation = generation + Duration::seconds(2);
        let info = meta.pools[0]
            .decommission
            .as_mut()
            .expect("decommission metadata should exist");
        info.queued = false;
        info.start_time = Some(replacement_generation);
        assert!(ensure_decommission_generation(&meta, 0, generation).is_err());
        assert!(ensure_decommission_generation(&meta, 0, replacement_generation).is_ok());
    }

    #[test]
    fn test_pool_meta_has_active_decommission_counts_running_and_queued_states() {
        let active_meta = PoolMeta {
            pools: vec![PoolStatus {
                id: 0,
                cmd_line: "pool-0".to_string(),
                last_update: OffsetDateTime::UNIX_EPOCH,
                decommission: Some(PoolDecommissionInfo {
                    start_time: Some(OffsetDateTime::UNIX_EPOCH),
                    ..Default::default()
                }),
            }],
            ..Default::default()
        };
        let queued_meta = PoolMeta {
            pools: vec![PoolStatus {
                id: 0,
                cmd_line: "pool-0".to_string(),
                last_update: OffsetDateTime::UNIX_EPOCH,
                decommission: Some(PoolDecommissionInfo {
                    queued: true,
                    ..Default::default()
                }),
            }],
            ..Default::default()
        };

        assert!(pool_meta_has_active_decommission(&active_meta));
        assert!(pool_meta_has_active_decommission(&queued_meta));
    }

    #[test]
    fn test_pool_meta_has_active_decommission_ignores_capacity_placeholder() {
        let meta = PoolMeta {
            pools: vec![PoolStatus {
                id: 0,
                cmd_line: "pool-0".to_string(),
                last_update: OffsetDateTime::UNIX_EPOCH,
                decommission: Some(PoolDecommissionInfo {
                    total_size: 100,
                    current_size: 75,
                    ..Default::default()
                }),
            }],
            ..Default::default()
        };

        assert!(!pool_meta_has_active_decommission(&meta));
        assert!(!meta.is_suspended(0));
        assert_eq!(decommission_start_pool_state(meta.pools.first()), DecommissionStartPoolState::Active);
    }

    #[test]
    fn test_pool_meta_has_active_decommission_ignores_terminal_states() {
        let terminal_meta = PoolMeta {
            pools: vec![
                PoolStatus {
                    id: 0,
                    cmd_line: "pool-0".to_string(),
                    last_update: OffsetDateTime::UNIX_EPOCH,
                    decommission: Some(PoolDecommissionInfo {
                        complete: true,
                        ..Default::default()
                    }),
                },
                PoolStatus {
                    id: 1,
                    cmd_line: "pool-1".to_string(),
                    last_update: OffsetDateTime::UNIX_EPOCH,
                    decommission: Some(PoolDecommissionInfo {
                        failed: true,
                        ..Default::default()
                    }),
                },
                PoolStatus {
                    id: 2,
                    cmd_line: "pool-2".to_string(),
                    last_update: OffsetDateTime::UNIX_EPOCH,
                    decommission: Some(PoolDecommissionInfo {
                        canceled: true,
                        ..Default::default()
                    }),
                },
            ],
            ..Default::default()
        };

        assert!(!pool_meta_has_active_decommission(&terminal_meta));
    }

    #[test]
    fn test_ensure_decommission_start_allowed_rejects_missing_pool() {
        let err =
            ensure_decommission_start_allowed(DecommissionStartPoolState::Missing).expect_err("missing pool should be invalid");
        assert!(
            err.to_string()
                .contains("failed to start decommission: target pool was not found")
        );
    }

    #[test]
    fn test_ensure_decommission_start_allowed_rejects_running_state() {
        let err = ensure_decommission_start_allowed(DecommissionStartPoolState::Decommissioning)
            .expect_err("active decommission should be rejected");
        assert!(matches!(err, Error::DecommissionAlreadyRunning));
    }

    #[test]
    fn test_ensure_decommission_start_allowed_rejects_completed_state() {
        let err = ensure_decommission_start_allowed(DecommissionStartPoolState::Decommissioned)
            .expect_err("completed decommission should be rejected");
        assert!(err.to_string().contains("target pool is already decommissioned"));
    }

    #[test]
    fn test_ensure_decommission_start_allowed_rejects_blocked_state() {
        let err = ensure_decommission_start_allowed(DecommissionStartPoolState::Blocked)
            .expect_err("blocked decommission should be rejected");
        assert!(err.to_string().contains("target pool decommission is blocked"));
    }

    #[test]
    fn test_ensure_decommission_start_allowed_allows_active_state() {
        assert!(ensure_decommission_start_allowed(DecommissionStartPoolState::Active).is_ok());
    }

    #[test]
    fn test_ensure_decommission_start_allowed_allows_retryable_state() {
        assert!(ensure_decommission_start_allowed(DecommissionStartPoolState::Retryable).is_ok());
    }

    #[test]
    fn test_decommission_start_pool_state_reports_missing_pool() {
        assert_eq!(decommission_start_pool_state(None), DecommissionStartPoolState::Missing);
    }

    #[test]
    fn test_decommission_start_pool_state_reports_idle_pool_without_decommission_info() {
        let pool = PoolStatus {
            id: 0,
            cmd_line: "pool-0".to_string(),
            last_update: OffsetDateTime::UNIX_EPOCH,
            decommission: None,
        };

        assert_eq!(decommission_start_pool_state(Some(&pool)), DecommissionStartPoolState::Active);
    }

    #[test]
    fn test_decommission_start_pool_state_reports_decommissioning_pool_when_not_terminal() {
        let pool = PoolStatus {
            id: 0,
            cmd_line: "pool-0".to_string(),
            last_update: OffsetDateTime::UNIX_EPOCH,
            decommission: Some(PoolDecommissionInfo {
                start_time: Some(OffsetDateTime::UNIX_EPOCH),
                complete: false,
                failed: false,
                canceled: false,
                ..Default::default()
            }),
        };

        assert_eq!(decommission_start_pool_state(Some(&pool)), DecommissionStartPoolState::Decommissioning);
    }

    #[test]
    fn test_decommission_start_pool_state_reports_canceled_pool_as_blocked() {
        let pool = PoolStatus {
            id: 0,
            cmd_line: "pool-0".to_string(),
            last_update: OffsetDateTime::UNIX_EPOCH,
            decommission: Some(PoolDecommissionInfo {
                complete: false,
                failed: false,
                canceled: true,
                ..Default::default()
            }),
        };

        assert_eq!(decommission_start_pool_state(Some(&pool)), DecommissionStartPoolState::Blocked);
    }

    #[test]
    fn test_decommission_start_pool_state_reports_failed_pool_as_blocked() {
        let pool = PoolStatus {
            id: 0,
            cmd_line: "pool-0".to_string(),
            last_update: OffsetDateTime::UNIX_EPOCH,
            decommission: Some(PoolDecommissionInfo {
                failed: true,
                ..Default::default()
            }),
        };

        assert_eq!(decommission_start_pool_state(Some(&pool)), DecommissionStartPoolState::Blocked);
    }

    #[test]
    fn test_decommission_start_pool_state_reports_terminal_pool_with_unresolved_entries_as_retryable() {
        let pool = PoolStatus {
            id: 0,
            cmd_line: "pool-0".to_string(),
            last_update: OffsetDateTime::UNIX_EPOCH,
            decommission: Some(PoolDecommissionInfo {
                failed: true,
                unresolved_entries: vec![DecommissionUnresolvedEntry {
                    bucket: "bucket-a".to_string(),
                    object: "object-a".to_string(),
                    pool_index: 0,
                    set_index: 0,
                    source_generation: OffsetDateTime::UNIX_EPOCH,
                    candidate_count: 1,
                    disk_error_count: 1,
                    observed_at: OffsetDateTime::UNIX_EPOCH,
                    reason: "metadata_resolution_failed".to_string(),
                }],
                ..Default::default()
            }),
        };

        assert_eq!(decommission_start_pool_state(Some(&pool)), DecommissionStartPoolState::Retryable);
    }

    #[test]
    fn test_decommission_start_pool_state_reports_completed_pool() {
        let pool = PoolStatus {
            id: 0,
            cmd_line: "pool-0".to_string(),
            last_update: OffsetDateTime::UNIX_EPOCH,
            decommission: Some(PoolDecommissionInfo {
                complete: true,
                ..Default::default()
            }),
        };

        assert_eq!(decommission_start_pool_state(Some(&pool)), DecommissionStartPoolState::Decommissioned);
    }

    #[test]
    fn test_ensure_decommission_start_keeps_active_pool_rejects_last_active_pool() {
        let meta = PoolMeta {
            pools: vec![PoolStatus {
                id: 0,
                cmd_line: "pool-0".to_string(),
                last_update: OffsetDateTime::UNIX_EPOCH,
                decommission: None,
            }],
            ..Default::default()
        };

        let err = ensure_decommission_start_keeps_active_pool(&meta, &[0]).expect_err("last active pool should be rejected");

        assert!(err.to_string().contains("at least one active pool must remain"));
    }

    #[test]
    fn test_ensure_decommission_start_target_capacity_allows_sufficient_free_space() {
        let meta = PoolMeta {
            pools: vec![decommission_test_pool_status(0, None), decommission_test_pool_status(1, None)],
            ..Default::default()
        };
        let space_infos = vec![
            (
                0,
                PoolSpaceInfo {
                    free: 100,
                    total: 1_000,
                    used: 900,
                },
            ),
            (
                1,
                PoolSpaceInfo {
                    free: 1_170,
                    total: 2_000,
                    used: 830,
                },
            ),
        ];

        assert!(ensure_decommission_start_target_capacity(&meta, &[0], &space_infos).is_ok());
    }

    #[test]
    fn test_ensure_decommission_start_target_capacity_rejects_insufficient_free_space() {
        let meta = PoolMeta {
            pools: vec![decommission_test_pool_status(0, None), decommission_test_pool_status(1, None)],
            ..Default::default()
        };
        let space_infos = vec![
            (
                0,
                PoolSpaceInfo {
                    free: 100,
                    total: 1_000,
                    used: 900,
                },
            ),
            (
                1,
                PoolSpaceInfo {
                    free: 1_169,
                    total: 2_000,
                    used: 831,
                },
            ),
        ];

        let err = ensure_decommission_start_target_capacity(&meta, &[0], &space_infos)
            .expect_err("target free capacity below 130% of source used should be rejected");

        assert!(err.to_string().contains("insufficient target pool capacity"));
        assert!(err.to_string().contains("required 1170 bytes available 1169 bytes"));
    }

    #[test]
    fn test_ensure_decommission_start_target_capacity_ignores_non_active_target_pool() {
        let meta = PoolMeta {
            pools: vec![
                decommission_test_pool_status(0, None),
                decommission_test_pool_status(
                    1,
                    Some(PoolDecommissionInfo {
                        complete: true,
                        ..Default::default()
                    }),
                ),
                decommission_test_pool_status(2, None),
            ],
            ..Default::default()
        };
        let space_infos = vec![
            (
                0,
                PoolSpaceInfo {
                    free: 100,
                    total: 1_000,
                    used: 900,
                },
            ),
            (
                1,
                PoolSpaceInfo {
                    free: 10_000,
                    total: 10_000,
                    used: 0,
                },
            ),
            (
                2,
                PoolSpaceInfo {
                    free: 1_169,
                    total: 2_000,
                    used: 831,
                },
            ),
        ];

        let err = ensure_decommission_start_target_capacity(&meta, &[0], &space_infos)
            .expect_err("completed pools must not contribute target free capacity");

        assert!(err.to_string().contains("required 1170 bytes available 1169 bytes"));
    }

    #[test]
    fn test_ensure_decommission_start_pool_states_rejects_blocked_pool() {
        let meta = PoolMeta {
            pools: vec![
                PoolStatus {
                    id: 0,
                    cmd_line: "pool-0".to_string(),
                    last_update: OffsetDateTime::UNIX_EPOCH,
                    decommission: Some(PoolDecommissionInfo {
                        failed: true,
                        ..Default::default()
                    }),
                },
                PoolStatus {
                    id: 1,
                    cmd_line: "pool-1".to_string(),
                    last_update: OffsetDateTime::UNIX_EPOCH,
                    decommission: None,
                },
            ],
            ..Default::default()
        };

        let err = ensure_decommission_start_pool_states(&meta, &[0]).expect_err("blocked pool should be rejected");

        assert!(err.to_string().contains("target pool decommission is blocked"));
    }

    #[test]
    fn test_ensure_decommission_start_pool_states_allows_active_pool_with_remaining_active_pool() {
        let meta = PoolMeta {
            pools: vec![
                PoolStatus {
                    id: 0,
                    cmd_line: "pool-0".to_string(),
                    last_update: OffsetDateTime::UNIX_EPOCH,
                    decommission: None,
                },
                PoolStatus {
                    id: 1,
                    cmd_line: "pool-1".to_string(),
                    last_update: OffsetDateTime::UNIX_EPOCH,
                    decommission: None,
                },
            ],
            ..Default::default()
        };

        assert!(ensure_decommission_start_pool_states(&meta, &[0]).is_ok());
    }

    #[test]
    fn test_ensure_decommission_start_pool_states_allows_retryable_pool_with_active_peer() {
        let meta = PoolMeta {
            pools: vec![
                PoolStatus {
                    id: 0,
                    cmd_line: "pool-0".to_string(),
                    last_update: OffsetDateTime::UNIX_EPOCH,
                    decommission: Some(PoolDecommissionInfo {
                        failed: true,
                        unresolved_entries: vec![DecommissionUnresolvedEntry {
                            bucket: "bucket-a".to_string(),
                            object: "object-a".to_string(),
                            pool_index: 0,
                            set_index: 0,
                            source_generation: OffsetDateTime::UNIX_EPOCH,
                            candidate_count: 1,
                            disk_error_count: 1,
                            observed_at: OffsetDateTime::UNIX_EPOCH,
                            reason: "metadata_resolution_failed".to_string(),
                        }],
                        ..Default::default()
                    }),
                },
                PoolStatus {
                    id: 1,
                    cmd_line: "pool-1".to_string(),
                    last_update: OffsetDateTime::UNIX_EPOCH,
                    decommission: None,
                },
            ],
            ..Default::default()
        };

        assert!(ensure_decommission_start_pool_states(&meta, &[0]).is_ok());
    }

    #[test]
    fn test_ensure_valid_decommission_pool_index_accepts_in_range_index() {
        assert!(ensure_valid_decommission_pool_index(4, 3).is_ok());
    }

    #[test]
    fn test_ensure_valid_decommission_pool_index_rejects_out_of_range_index() {
        let err = ensure_valid_decommission_pool_index(2, 2).expect_err("out-of-range index should fail");
        assert!(err.to_string().contains("invalid decommission pool index 2 for 2 pools"));
    }

    #[test]
    fn test_ensure_valid_decommission_pool_index_rejects_when_pool_count_zero() {
        let err = ensure_valid_decommission_pool_index(0, 0).expect_err("empty pool list should reject all indices");
        assert!(err.to_string().contains("invalid decommission pool index 0 for 0 pools"));
    }

    #[test]
    fn test_classify_decommission_terminal_state_completed_when_no_failures() {
        assert_eq!(classify_decommission_terminal_state(false), DecommissionTerminalState::Completed);
    }

    #[test]
    fn test_classify_decommission_terminal_state_failed_when_failures_present() {
        assert_eq!(classify_decommission_terminal_state(true), DecommissionTerminalState::Failed);
    }

    #[test]
    fn test_should_preserve_decommission_canceled_state_when_meta_canceled() {
        assert!(should_preserve_decommission_canceled_state(true, false));
    }

    #[test]
    fn test_should_preserve_decommission_canceled_state_when_signal_canceled() {
        assert!(!should_preserve_decommission_canceled_state(false, true));
    }

    #[test]
    fn test_should_preserve_decommission_canceled_state_when_not_canceled() {
        assert!(!should_preserve_decommission_canceled_state(false, false));
    }

    #[test]
    fn test_should_continue_decommission_queue_requires_clean_completion() {
        let meta = PoolMeta {
            pools: vec![
                PoolStatus {
                    id: 0,
                    cmd_line: "pool-0".to_string(),
                    last_update: OffsetDateTime::UNIX_EPOCH,
                    decommission: Some(PoolDecommissionInfo {
                        complete: true,
                        ..Default::default()
                    }),
                },
                PoolStatus {
                    id: 1,
                    cmd_line: "pool-1".to_string(),
                    last_update: OffsetDateTime::UNIX_EPOCH,
                    decommission: Some(PoolDecommissionInfo::default()),
                },
                PoolStatus {
                    id: 2,
                    cmd_line: "pool-2".to_string(),
                    last_update: OffsetDateTime::UNIX_EPOCH,
                    decommission: Some(PoolDecommissionInfo {
                        failed: true,
                        ..Default::default()
                    }),
                },
                PoolStatus {
                    id: 3,
                    cmd_line: "pool-3".to_string(),
                    last_update: OffsetDateTime::UNIX_EPOCH,
                    decommission: Some(PoolDecommissionInfo {
                        canceled: true,
                        ..Default::default()
                    }),
                },
                PoolStatus {
                    id: 4,
                    cmd_line: "pool-4".to_string(),
                    last_update: OffsetDateTime::UNIX_EPOCH,
                    decommission: None,
                },
            ],
            ..Default::default()
        };

        assert!(should_continue_decommission_queue(&meta, 0));
        assert!(!should_continue_decommission_queue(&meta, 1));
        assert!(!should_continue_decommission_queue(&meta, 2));
        assert!(!should_continue_decommission_queue(&meta, 3));
        assert!(!should_continue_decommission_queue(&meta, 4));
        assert!(!should_continue_decommission_queue(&meta, 5));
    }

    #[test]
    fn test_decommission_cancel_signal_result_returns_err_when_canceled() {
        let err = decommission_cancel_signal_result(true).expect_err("canceled signal should return operation-canceled");
        assert!(matches!(err, Error::OperationCanceled));
    }

    #[test]
    fn test_decommission_cancel_signal_result_returns_ok_when_not_canceled() {
        assert!(decommission_cancel_signal_result(false).is_ok());
    }

    #[test]
    fn test_is_decommission_cancel_requested_accepts_signal_or_metadata() {
        let pool = PoolStatus {
            id: 0,
            cmd_line: "pool-0".to_string(),
            last_update: OffsetDateTime::UNIX_EPOCH,
            decommission: Some(PoolDecommissionInfo {
                canceled: true,
                ..Default::default()
            }),
        };

        assert!(is_decommission_cancel_requested(false, Some(&pool)));
        assert!(is_decommission_cancel_requested(true, None));
    }

    #[test]
    fn test_is_decommission_cancel_requested_rejects_active_without_signal() {
        let pool = PoolStatus {
            id: 0,
            cmd_line: "pool-0".to_string(),
            last_update: OffsetDateTime::UNIX_EPOCH,
            decommission: Some(PoolDecommissionInfo::default()),
        };

        assert!(!is_decommission_cancel_requested(false, Some(&pool)));
        assert!(!is_decommission_cancel_requested(false, None));
    }

    #[test]
    fn test_skip_canceled_decommission_routine_only_for_terminal_canceled_state() {
        let canceled = PoolStatus {
            id: 0,
            cmd_line: "pool-0".to_string(),
            last_update: OffsetDateTime::UNIX_EPOCH,
            decommission: Some(PoolDecommissionInfo {
                canceled: true,
                ..Default::default()
            }),
        };
        let active = PoolStatus {
            id: 1,
            cmd_line: "pool-1".to_string(),
            last_update: OffsetDateTime::UNIX_EPOCH,
            decommission: Some(PoolDecommissionInfo::default()),
        };

        assert!(should_skip_canceled_decommission_routine(true, Some(&canceled)));
        assert!(!should_skip_canceled_decommission_routine(false, Some(&canceled)));
        assert!(!should_skip_canceled_decommission_routine(true, Some(&active)));
        assert!(!should_skip_canceled_decommission_routine(true, None));
    }

    #[test]
    fn test_ensure_decommission_cancel_allowed_rejects_missing_pool() {
        let err = ensure_decommission_cancel_allowed(false, false, false).expect_err("missing pool should be invalid");
        assert!(
            err.to_string()
                .contains("failed to cancel decommission: target pool was not found")
        );
    }

    #[test]
    fn test_should_reject_decommission_cancel_as_terminal_true_when_completed() {
        assert!(should_reject_decommission_cancel_as_terminal(true, false));
    }

    #[test]
    fn test_should_reject_decommission_cancel_as_terminal_true_when_failed() {
        assert!(should_reject_decommission_cancel_as_terminal(false, true));
    }

    #[test]
    fn test_should_reject_decommission_cancel_as_terminal_false_when_active_or_canceled() {
        assert!(!should_reject_decommission_cancel_as_terminal(false, false));
    }

    #[test]
    fn test_should_retry_decommission_cancel_reload_when_changed_or_already_canceled() {
        assert!(should_retry_decommission_cancel_reload(true, false));
        assert!(should_retry_decommission_cancel_reload(false, true));
        assert!(!should_retry_decommission_cancel_reload(false, false));
    }

    #[test]
    fn test_ensure_decommission_cancel_allowed_rejects_not_started() {
        let err =
            ensure_decommission_cancel_allowed(true, false, false).expect_err("not-started decommission should be rejected");
        assert!(matches!(err, Error::DecommissionNotStarted));
    }

    #[test]
    fn test_ensure_decommission_cancel_allowed_rejects_terminal() {
        let err = ensure_decommission_cancel_allowed(true, true, true).expect_err("terminal decommission should be rejected");
        assert!(matches!(err, Error::DecommissionNotStarted));
    }

    #[test]
    fn test_ensure_decommission_cancel_allowed_allows_active() {
        assert!(ensure_decommission_cancel_allowed(true, true, false).is_ok());
    }

    #[test]
    fn test_ensure_decommission_clear_allowed_allows_failed_or_canceled() {
        assert!(ensure_decommission_clear_allowed(true, true, false, true, false, 0).is_ok());
        assert!(ensure_decommission_clear_allowed(true, true, false, false, true, 0).is_ok());
    }

    #[test]
    fn test_ensure_decommission_clear_allowed_rejects_active_or_completed() {
        let active = ensure_decommission_clear_allowed(true, true, false, false, false, 0)
            .expect_err("active decommission should not be clearable");
        assert!(matches!(active, Error::DecommissionAlreadyRunning));

        let complete = ensure_decommission_clear_allowed(true, true, true, false, false, 0)
            .expect_err("completed decommission should not be clearable");
        assert!(matches!(complete, Error::DecommissionNotStarted));
    }

    #[test]
    fn test_ensure_decommission_clear_allowed_rejects_unresolved_entries() {
        let err = ensure_decommission_clear_allowed(true, true, false, true, false, 1)
            .expect_err("unresolved entries must survive until a retry reconciles them");

        assert!(err.to_string().contains("must be reconciled by retrying decommission"));
    }

    #[test]
    fn test_pool_meta_clear_decommission_restores_failed_or_canceled_pool() {
        for decommission in [
            PoolDecommissionInfo {
                failed: true,
                ..Default::default()
            },
            PoolDecommissionInfo {
                canceled: true,
                ..Default::default()
            },
        ] {
            let mut meta = PoolMeta {
                pools: vec![PoolStatus {
                    id: 0,
                    cmd_line: "pool-0".to_string(),
                    last_update: OffsetDateTime::UNIX_EPOCH,
                    decommission: Some(decommission),
                }],
                ..Default::default()
            };

            assert!(meta.is_suspended(0));
            assert!(meta.clear_decommission(0).expect("terminal decommission should clear"));
            assert!(meta.pools[0].decommission.is_none());
            assert!(!meta.is_suspended(0));
        }
    }

    #[test]
    fn test_pool_meta_clear_decommission_preserves_unresolved_entries_for_retry() {
        let generation = OffsetDateTime::UNIX_EPOCH;
        let unresolved_entry = DecommissionUnresolvedEntry {
            bucket: "bucket-a".to_string(),
            object: "object-a".to_string(),
            pool_index: 0,
            set_index: 0,
            source_generation: generation,
            candidate_count: 1,
            disk_error_count: 1,
            observed_at: generation,
            reason: "metadata_resolution_failed".to_string(),
        };
        let mut meta = PoolMeta {
            pools: vec![PoolStatus {
                id: 0,
                cmd_line: "pool-0".to_string(),
                last_update: generation,
                decommission: Some(PoolDecommissionInfo {
                    failed: true,
                    unresolved_entries: vec![unresolved_entry.clone()],
                    ..Default::default()
                }),
            }],
            ..Default::default()
        };

        let err = meta
            .clear_decommission(0)
            .expect_err("clear must not discard the unresolved-entry recovery ledger");

        assert!(err.to_string().contains("must be reconciled by retrying decommission"));
        assert_eq!(
            meta.pools[0]
                .decommission
                .as_ref()
                .expect("failed state should remain retryable")
                .unresolved_entries,
            vec![unresolved_entry]
        );
    }

    #[test]
    fn test_pool_meta_clear_decommission_rejects_active_or_completed_pool() {
        for decommission in [
            PoolDecommissionInfo::default(),
            PoolDecommissionInfo {
                complete: true,
                ..Default::default()
            },
        ] {
            let mut meta = PoolMeta {
                pools: vec![PoolStatus {
                    id: 0,
                    cmd_line: "pool-0".to_string(),
                    last_update: OffsetDateTime::UNIX_EPOCH,
                    decommission: Some(decommission),
                }],
                ..Default::default()
            };

            assert!(meta.clear_decommission(0).is_err());
            assert!(meta.pools[0].decommission.is_some());
        }
    }

    #[test]
    fn test_contextualized_decommission_terminal_operation_supported_rejects_single_pool() {
        let err = ensure_decommission_terminal_operation_supported(true, "complete decommission")
            .expect_err("single-pool decommission terminal operations should be rejected");
        assert!(
            err.to_string()
                .contains("failed to complete decommission: single pool deployments do not support decommission")
        );
    }

    #[test]
    fn test_contextualized_decommission_terminal_operation_supported_allows_multi_pool() {
        assert!(ensure_decommission_terminal_operation_supported(false, "mark decommission failed").is_ok());
    }

    #[test]
    fn test_contextualized_decommission_start_request_rejects_empty_indices() {
        let err = validate_start_decommission_request(&[], false).expect_err("empty decommission target list should be rejected");
        assert!(
            err.to_string()
                .contains("failed to start decommission: no target pools were provided")
        );
    }

    #[test]
    fn test_contextualized_decommission_start_request_rejects_single_pool() {
        let err = validate_start_decommission_request(&[0], true)
            .expect_err("single-pool deployments should reject decommission start");
        assert!(
            err.to_string()
                .contains("failed to start decommission: single pool deployments do not support decommission")
        );
    }

    #[test]
    fn test_contextualized_decommission_start_request_allows_multiple_target_pools() {
        assert!(validate_start_decommission_request(&[0, 1], false).is_ok());
    }

    #[test]
    fn test_contextualized_decommission_start_request_allows_one_target_pool() {
        assert!(validate_start_decommission_request(&[0], false).is_ok());
    }

    #[test]
    fn test_decommission_retry_preserves_and_rebinds_unresolved_entries() {
        let previous_generation = OffsetDateTime::UNIX_EPOCH;
        let unresolved_entry = DecommissionUnresolvedEntry {
            bucket: "bucket-a".to_string(),
            object: "object-a".to_string(),
            pool_index: 0,
            set_index: 0,
            source_generation: previous_generation,
            candidate_count: 1,
            disk_error_count: 1,
            observed_at: previous_generation,
            reason: "metadata_resolution_failed".to_string(),
        };
        let mut meta = PoolMeta {
            pools: vec![PoolStatus {
                id: 0,
                cmd_line: "pool-0".to_string(),
                last_update: previous_generation,
                decommission: Some(PoolDecommissionInfo {
                    failed: true,
                    decommissioned_buckets: vec!["bucket-a".to_string()],
                    items_decommissioned: 7,
                    bytes_done: 1024,
                    unresolved_entries: vec![unresolved_entry],
                    ..Default::default()
                }),
            }],
            ..Default::default()
        };

        meta.decommission(
            0,
            PoolSpaceInfo {
                total: 200,
                free: 50,
                used: 150,
            },
        )
        .expect("failed decommission with unresolved entries should retry atomically");

        let info = meta.pools[0]
            .decommission
            .as_ref()
            .expect("retried decommission metadata should exist");
        let next_generation = info.start_time.expect("retry should assign a new generation");
        assert!(next_generation > previous_generation);
        assert_eq!(info.decommissioned_buckets, vec!["bucket-a".to_string()]);
        assert_eq!(info.items_decommissioned, 7);
        assert_eq!(info.bytes_done, 1024);
        assert_eq!(info.unresolved_entries.len(), 1);
        assert_eq!(info.unresolved_entries[0].source_generation, next_generation);
    }

    #[test]
    fn test_queued_decommission_retry_rebinds_unresolved_entries_when_promoted() {
        let previous_generation = OffsetDateTime::UNIX_EPOCH;
        let mut meta = PoolMeta {
            pools: vec![PoolStatus {
                id: 0,
                cmd_line: "pool-0".to_string(),
                last_update: previous_generation,
                decommission: Some(PoolDecommissionInfo {
                    canceled: true,
                    unresolved_entries: vec![DecommissionUnresolvedEntry {
                        bucket: "bucket-a".to_string(),
                        object: "object-a".to_string(),
                        pool_index: 0,
                        set_index: 0,
                        source_generation: previous_generation,
                        candidate_count: 1,
                        disk_error_count: 1,
                        observed_at: previous_generation,
                        reason: "metadata_resolution_failed".to_string(),
                    }],
                    ..Default::default()
                }),
            }],
            ..Default::default()
        };

        meta.queue_decommission(
            0,
            PoolSpaceInfo {
                total: 100,
                free: 25,
                used: 75,
            },
        )
        .expect("canceled decommission with unresolved entries should queue an atomic retry");

        assert!(meta.is_suspended(0));
        assert!(meta.promote_queued_decommission(0));
        let promoted = meta.pools[0]
            .decommission
            .as_ref()
            .expect("promoted decommission metadata should exist");
        let generation = promoted.start_time.expect("promotion should assign a generation");
        assert_eq!(promoted.unresolved_entries.len(), 1);
        assert_eq!(promoted.unresolved_entries[0].source_generation, generation);
    }

    #[test]
    fn test_pool_meta_queued_decommission_is_not_suspended_until_promoted() {
        let mut meta = PoolMeta {
            pools: vec![PoolStatus {
                id: 0,
                cmd_line: "pool-0".to_string(),
                last_update: OffsetDateTime::UNIX_EPOCH,
                decommission: None,
            }],
            ..Default::default()
        };

        meta.queue_decommission(
            0,
            PoolSpaceInfo {
                total: 100,
                free: 10,
                used: 90,
            },
        )
        .expect("queued decommission should be stored");

        assert!(!meta.is_suspended(0));
        assert!(meta.promote_queued_decommission(0));
        assert!(meta.is_suspended(0));
    }

    #[test]
    fn test_pool_meta_promoted_queued_decommission_can_be_canceled() {
        let mut meta = PoolMeta {
            pools: vec![PoolStatus {
                id: 0,
                cmd_line: "pool-0".to_string(),
                last_update: OffsetDateTime::UNIX_EPOCH,
                decommission: None,
            }],
            ..Default::default()
        };

        meta.queue_decommission(
            0,
            PoolSpaceInfo {
                total: 100,
                free: 10,
                used: 90,
            },
        )
        .expect("queued decommission should be stored");

        assert!(pool_meta_has_active_decommission(&meta));
        assert!(meta.promote_queued_decommission(0));
        assert!(meta.decommission_cancel(0));

        let info = meta.pools[0]
            .decommission
            .as_ref()
            .expect("canceled decommission state should be kept for clear");
        assert!(info.canceled);
        assert!(!info.queued);
        assert!(!info.failed);
        assert!(!info.complete);
        assert!(!pool_meta_has_active_decommission(&meta));
    }

    #[test]
    fn test_pool_meta_failed_decommission_requires_clear_before_restart() {
        let mut meta = PoolMeta {
            pools: vec![PoolStatus {
                id: 0,
                cmd_line: "pool-0".to_string(),
                last_update: OffsetDateTime::UNIX_EPOCH,
                decommission: Some(PoolDecommissionInfo {
                    failed: true,
                    decommissioned_buckets: vec!["bucket-done".to_string()],
                    queued_buckets: vec!["bucket-pending".to_string()],
                    bucket: "bucket-pending".to_string(),
                    prefix: "prefix".to_string(),
                    object: "object.txt".to_string(),
                    items_decommissioned: 7,
                    items_decommission_failed: 3,
                    bytes_done: 1024,
                    bytes_failed: 256,
                    progress_save_item_baseline: 10,
                    ..Default::default()
                }),
            }],
            ..Default::default()
        };

        let err = meta
            .decommission(
                0,
                PoolSpaceInfo {
                    total: 200,
                    free: 50,
                    used: 150,
                },
            )
            .expect_err("failed decommission should be blocked until cleared");
        assert!(err.to_string().contains("target pool decommission is blocked"));
        let blocked = meta.pools[0]
            .decommission
            .as_ref()
            .expect("blocked metadata should remain until clear");
        assert!(blocked.failed);
        assert_eq!(blocked.decommissioned_buckets, vec!["bucket-done".to_string()]);
        assert_eq!(blocked.items_decommissioned, 7);
        assert_eq!(blocked.bytes_done, 1024);

        assert!(meta.clear_decommission(0).expect("failed decommission should clear"));
        assert!(meta.pools[0].decommission.is_none());

        meta.decommission(
            0,
            PoolSpaceInfo {
                total: 200,
                free: 50,
                used: 150,
            },
        )
        .expect("cleared decommission should be restartable");
        meta.queue_buckets(
            0,
            vec![
                DecomBucketInfo {
                    name: "bucket-done".to_string(),
                    prefix: String::new(),
                },
                DecomBucketInfo {
                    name: "bucket-pending".to_string(),
                    prefix: String::new(),
                },
            ],
        );

        let info = meta.pools[0]
            .decommission
            .as_ref()
            .expect("decommission info should be rebuilt");
        assert!(!info.failed);
        assert!(!info.canceled);
        assert!(!info.complete);
        assert!(info.decommissioned_buckets.is_empty());
        assert_eq!(info.queued_buckets, vec!["bucket-done".to_string(), "bucket-pending".to_string()]);
        assert_eq!(info.items_decommissioned, 0);
        assert_eq!(info.items_decommission_failed, 0);
        assert_eq!(info.bytes_done, 0);
        assert_eq!(info.bytes_failed, 0);
        assert_eq!(info.items_since_last_progress_save(), 0);
        assert_eq!(info.start_size, 50);
        assert_eq!(info.total_size, 200);
        assert_eq!(info.current_size, 50);
        assert_eq!(info.bucket, "bucket-pending");
        assert!(info.prefix.is_empty());
        assert!(info.object.is_empty());
        assert!(info.start_time.is_some());
    }

    #[test]
    fn test_pool_meta_canceled_queued_decommission_requires_clear_before_restart() {
        let mut meta = PoolMeta {
            pools: vec![PoolStatus {
                id: 0,
                cmd_line: "pool-0".to_string(),
                last_update: OffsetDateTime::UNIX_EPOCH,
                decommission: Some(PoolDecommissionInfo {
                    canceled: true,
                    decommissioned_buckets: vec!["bucket-done".to_string()],
                    items_decommissioned: 5,
                    bytes_done: 512,
                    ..Default::default()
                }),
            }],
            ..Default::default()
        };

        let err = meta
            .queue_decommission(
                0,
                PoolSpaceInfo {
                    total: 100,
                    free: 25,
                    used: 75,
                },
            )
            .expect_err("canceled queued decommission should be blocked until cleared");
        assert!(err.to_string().contains("target pool decommission is blocked"));
        let blocked = meta.pools[0]
            .decommission
            .as_ref()
            .expect("blocked metadata should remain until clear");
        assert!(blocked.canceled);
        assert_eq!(blocked.decommissioned_buckets, vec!["bucket-done".to_string()]);
        assert_eq!(blocked.items_decommissioned, 5);
        assert_eq!(blocked.bytes_done, 512);

        assert!(meta.clear_decommission(0).expect("canceled decommission should clear"));
        assert!(meta.pools[0].decommission.is_none());

        meta.queue_decommission(
            0,
            PoolSpaceInfo {
                total: 100,
                free: 25,
                used: 75,
            },
        )
        .expect("cleared queued decommission should be restartable");

        let info = meta.pools[0]
            .decommission
            .as_ref()
            .expect("decommission info should be rebuilt");
        assert!(info.queued);
        assert!(info.start_time.is_none());
        assert!(info.decommissioned_buckets.is_empty());
        assert_eq!(info.items_decommissioned, 0);
        assert_eq!(info.bytes_done, 0);
    }

    #[test]
    fn test_contextualized_decommission_listing_disks_available_rejects_empty_set() {
        let err = ensure_decommission_listing_disks_available(false, "bucket-a")
            .expect_err("missing online disks should be reported with bucket context");
        assert!(
            err.to_string()
                .contains("failed to list objects to decommission for bucket bucket-a: no disks available")
        );
    }

    #[test]
    fn test_contextualized_decommission_listing_disks_available_allows_online_disks() {
        assert!(ensure_decommission_listing_disks_available(true, "bucket-a").is_ok());
    }

    #[test]
    fn test_require_decommission_store_returns_value_when_present() {
        let store = require_decommission_store(Some(7_u8), "start decommission").expect("present store should be returned");
        assert_eq!(store, 7);
    }

    #[test]
    fn test_require_decommission_store_returns_error_when_missing() {
        let err = require_decommission_store::<u8>(None, "start decommission").expect_err("missing store should return error");
        assert!(
            err.to_string()
                .contains("failed to start decommission: store not initialized")
        );
    }

    #[test]
    fn test_bind_decommission_cancelers_binds_existing_slots_only() {
        let parent = CancellationToken::new();
        let mut cancelers = vec![None, None];

        let bound = bind_decommission_cancelers(&[0, 3, 1], &parent, cancelers.as_mut_slice());

        assert_eq!(bound.len(), 2);
        assert_eq!(bound[0].0, 0);
        assert_eq!(bound[1].0, 1);
        assert!(cancelers[0].is_some());
        assert!(cancelers[1].is_some());
    }

    #[test]
    fn test_bind_decommission_cancelers_child_tokens_follow_parent_cancel() {
        let parent = CancellationToken::new();
        let mut cancelers = vec![None];

        let bound = bind_decommission_cancelers(&[0], &parent, cancelers.as_mut_slice());
        assert_eq!(bound.len(), 1);
        assert!(!bound[0].1.is_cancelled());

        parent.cancel();
        assert!(bound[0].1.is_cancelled());
    }

    #[test]
    fn test_bind_decommission_cancelers_replaces_existing_slot() {
        let parent = CancellationToken::new();
        let existing = DecommissionCanceler::new(CancellationToken::new());
        let mut cancelers = vec![Some(existing.clone())];

        let bound = bind_decommission_cancelers(&[0], &parent, cancelers.as_mut_slice());

        assert_eq!(bound.len(), 1);
        assert_eq!(bound[0].0, 0);
        assert!(existing.is_cancelled());
        let replacement = cancelers[0].as_ref().expect("replacement token should be stored");
        assert!(!replacement.is_cancelled());
        parent.cancel();
        assert!(replacement.is_cancelled());
    }

    #[test]
    fn test_bind_missing_decommission_cancelers_stops_at_existing_slot() {
        let parent = CancellationToken::new();
        let existing = DecommissionCanceler::new(CancellationToken::new());
        let mut cancelers = vec![None, Some(existing.clone()), None];

        let bound = bind_missing_decommission_cancelers(&[0, 1, 2], &parent, cancelers.as_mut_slice());

        assert_eq!(bound.len(), 1);
        assert_eq!(bound[0].0, 0);
        assert!(cancelers[0].is_some());
        assert!(cancelers[1].is_some());
        assert!(cancelers[2].is_none());
        assert!(!existing.is_cancelled());
    }

    #[test]
    fn test_serialized_decommission_double_start_preserves_first_operation() {
        let mut pool_meta = PoolMeta {
            pools: vec![decommission_test_pool_status(0, None), decommission_test_pool_status(1, None)],
            ..Default::default()
        };
        let first_parent = CancellationToken::new();
        let second_parent = CancellationToken::new();
        let mut cancelers = vec![None, None];

        let first = reserve_decommission_start_cancelers(&pool_meta, &[0], &[0], &first_parent, cancelers.as_mut_slice())
            .expect("first start should reserve its worker");
        pool_meta
            .decommission(
                0,
                PoolSpaceInfo {
                    total: 100,
                    free: 40,
                    used: 60,
                },
            )
            .expect("first start should install active metadata");

        let second = reserve_decommission_start_cancelers(&pool_meta, &[0], &[0], &second_parent, cancelers.as_mut_slice());

        assert!(matches!(second, Err(Error::DecommissionAlreadyRunning)));
        let current = cancelers[0].as_ref().expect("first operation should retain the slot");
        assert!(current.owns_same_operation(first[0].1.canceler()));
        assert!(current.is_active());
        assert!(!first_parent.is_cancelled());
    }

    #[test]
    fn test_local_decommission_queue_prefix_stops_at_remote_leader() {
        let endpoints = EndpointServerPools::from(vec![
            decommission_test_pool_endpoint(0, true),
            decommission_test_pool_endpoint(1, true),
            decommission_test_pool_endpoint(2, false),
            decommission_test_pool_endpoint(3, true),
        ]);

        let local = local_decommission_queue_prefix(&endpoints, &[0, 1, 2, 3]).expect("prefix should resolve");

        assert_eq!(local, vec![0, 1]);
    }

    #[test]
    fn test_local_decommission_queue_prefix_empty_when_first_leader_remote() {
        let endpoints = EndpointServerPools::from(vec![
            decommission_test_pool_endpoint(0, false),
            decommission_test_pool_endpoint(1, true),
        ]);

        let local = local_decommission_queue_prefix(&endpoints, &[0, 1]).expect("prefix should resolve");

        assert!(local.is_empty());
    }

    #[test]
    fn test_decommission_start_local_leader_allows_remote_queued_pool() {
        let endpoints = EndpointServerPools::from(vec![
            decommission_test_pool_endpoint(0, true),
            decommission_test_pool_endpoint(1, false),
        ]);

        assert!(ensure_decommission_start_local_leader(&endpoints, &[0, 1]).is_ok());
    }

    #[test]
    fn test_decommission_start_local_leader_rejects_remote_active_pool() {
        let endpoints = EndpointServerPools::from(vec![decommission_test_pool_endpoint(0, false)]);

        let err = ensure_decommission_start_local_leader(&endpoints, &[0]).expect_err("remote active pool should be rejected");

        assert!(
            err.to_string()
                .contains("decommission for pool 0 must run on the pool first endpoint")
        );
    }

    #[test]
    fn test_missing_decommission_worker_prefix_stops_at_active_worker() {
        let cancelers = vec![None, Some(DecommissionCanceler::new(CancellationToken::new())), None];

        let missing = missing_decommission_worker_prefix(&[0, 1, 2], cancelers.as_slice());

        assert_eq!(missing, vec![0]);
    }

    #[test]
    fn test_resumable_decommission_queue_indices_skip_terminal_predecessors() {
        let meta = PoolMeta {
            pools: vec![
                decommission_test_pool_status(
                    0,
                    Some(PoolDecommissionInfo {
                        complete: true,
                        ..Default::default()
                    }),
                ),
                decommission_test_pool_status(
                    1,
                    Some(PoolDecommissionInfo {
                        canceled: true,
                        ..Default::default()
                    }),
                ),
                decommission_test_pool_status(
                    2,
                    Some(PoolDecommissionInfo {
                        failed: true,
                        ..Default::default()
                    }),
                ),
                decommission_test_pool_status(
                    3,
                    Some(PoolDecommissionInfo {
                        queued: true,
                        ..Default::default()
                    }),
                ),
                decommission_test_pool_status(4, None),
            ],
            ..Default::default()
        };

        assert_eq!(resumable_decommission_queue_indices(&meta), vec![3]);
    }

    #[test]
    fn test_resumable_decommission_queue_indices_preserve_active_predecessor_order() {
        let meta = PoolMeta {
            pools: vec![
                decommission_test_pool_status(
                    0,
                    Some(PoolDecommissionInfo {
                        complete: true,
                        ..Default::default()
                    }),
                ),
                decommission_test_pool_status(
                    1,
                    Some(PoolDecommissionInfo {
                        start_time: Some(OffsetDateTime::UNIX_EPOCH),
                        ..Default::default()
                    }),
                ),
                decommission_test_pool_status(
                    2,
                    Some(PoolDecommissionInfo {
                        queued: true,
                        ..Default::default()
                    }),
                ),
            ],
            ..Default::default()
        };

        assert_eq!(resumable_decommission_queue_indices(&meta), vec![1, 2]);
    }

    #[tokio::test]
    async fn test_runtime_recovery_reserves_the_startup_resumable_queue() {
        let meta = PoolMeta {
            pools: vec![
                decommission_test_pool_status(
                    0,
                    Some(PoolDecommissionInfo {
                        failed: true,
                        ..Default::default()
                    }),
                ),
                decommission_test_pool_status(
                    1,
                    Some(PoolDecommissionInfo {
                        canceled: true,
                        ..Default::default()
                    }),
                ),
                decommission_test_pool_status(
                    2,
                    Some(PoolDecommissionInfo {
                        start_time: Some(OffsetDateTime::UNIX_EPOCH),
                        ..Default::default()
                    }),
                ),
                decommission_test_pool_status(
                    3,
                    Some(PoolDecommissionInfo {
                        queued: true,
                        ..Default::default()
                    }),
                ),
            ],
            ..Default::default()
        };
        let startup_ids = meta
            .return_resumable_pools()
            .into_iter()
            .map(|pool| pool.id)
            .collect::<Vec<_>>();
        let store = decommission_worker_test_store(meta, vec![None, None, None, None]);
        let endpoints = EndpointServerPools::from(vec![
            decommission_test_pool_endpoint(0, true),
            decommission_test_pool_endpoint(1, true),
            decommission_test_pool_endpoint(2, true),
            decommission_test_pool_endpoint(3, true),
        ]);
        let reserved = store
            .reserve_missing_local_decommission_routines(&CancellationToken::new(), &endpoints)
            .await
            .expect("runtime recovery reservation should succeed");
        let runtime_indices = reserved.iter().map(|(idx, _)| *idx).collect::<Vec<_>>();

        assert_eq!(runtime_indices, vec![2, 3]);
        assert_eq!(startup_ids, vec![2, 3]);
    }

    #[tokio::test]
    async fn test_runtime_recovery_does_not_reserve_behind_active_predecessor() {
        let meta = PoolMeta {
            pools: vec![
                decommission_test_pool_status(
                    0,
                    Some(PoolDecommissionInfo {
                        failed: true,
                        ..Default::default()
                    }),
                ),
                decommission_test_pool_status(
                    1,
                    Some(PoolDecommissionInfo {
                        canceled: true,
                        ..Default::default()
                    }),
                ),
                decommission_test_pool_status(
                    2,
                    Some(PoolDecommissionInfo {
                        start_time: Some(OffsetDateTime::UNIX_EPOCH),
                        ..Default::default()
                    }),
                ),
                decommission_test_pool_status(
                    3,
                    Some(PoolDecommissionInfo {
                        queued: true,
                        ..Default::default()
                    }),
                ),
            ],
            ..Default::default()
        };
        let active = DecommissionCanceler::new(CancellationToken::new());
        let store = decommission_worker_test_store(meta, vec![None, None, Some(active.clone()), None]);
        let endpoints = EndpointServerPools::from(vec![
            decommission_test_pool_endpoint(0, true),
            decommission_test_pool_endpoint(1, true),
            decommission_test_pool_endpoint(2, true),
            decommission_test_pool_endpoint(3, true),
        ]);

        let reserved = store
            .reserve_missing_local_decommission_routines(&CancellationToken::new(), &endpoints)
            .await
            .expect("runtime recovery reservation should succeed");

        assert!(reserved.is_empty());
        assert!(active.is_active());
    }

    #[test]
    fn test_take_decommission_canceler_takes_and_clears_slot() {
        let canceler = DecommissionCanceler::new(CancellationToken::new());
        let mut cancelers = vec![Some(canceler)];

        let taken = take_decommission_canceler(cancelers.as_mut_slice(), 0);
        assert!(taken.is_some());
        assert!(cancelers[0].is_none());
    }

    #[test]
    fn test_take_decommission_canceler_returns_none_for_missing_slot() {
        let mut cancelers: Vec<Option<DecommissionCanceler>> = Vec::new();
        assert!(take_decommission_canceler(cancelers.as_mut_slice(), 0).is_none());
    }

    #[test]
    fn test_has_active_decommission_canceler_true_when_any_slot_present() {
        let cancelers = vec![None, Some(DecommissionCanceler::new(CancellationToken::new()))];
        assert!(has_active_decommission_canceler(cancelers.as_slice()));
    }

    #[test]
    fn test_has_active_decommission_canceler_false_when_all_empty() {
        let cancelers = vec![None, None];
        assert!(!has_active_decommission_canceler(cancelers.as_slice()));
    }

    #[test]
    fn test_cancel_decommission_canceler_cancels_when_present() {
        let canceler = DecommissionCanceler::new(CancellationToken::new());
        let canceled = cancel_decommission_canceler(Some(canceler.clone()));

        assert!(canceled);
        assert!(canceler.is_cancelled());
        assert!(!canceler.is_active());
    }

    #[test]
    fn test_cancel_decommission_canceler_returns_false_when_missing() {
        assert!(!cancel_decommission_canceler(None));
    }

    #[test]
    fn test_take_and_cancel_decommission_canceler_clears_slot() {
        let canceler = DecommissionCanceler::new(CancellationToken::new());
        let mut cancelers = vec![Some(canceler.clone())];

        assert!(take_and_cancel_decommission_canceler(cancelers.as_mut_slice(), 0));
        assert!(cancelers[0].is_none());
        assert!(canceler.is_cancelled());
        assert!(!canceler.is_active());
    }

    #[test]
    fn test_take_and_cancel_decommission_canceler_missing_slot_is_false() {
        let mut cancelers = vec![None];

        assert!(!take_and_cancel_decommission_canceler(cancelers.as_mut_slice(), 0));
        assert!(cancelers[0].is_none());
    }

    #[test]
    fn test_guarded_decommission_future_releases_without_first_poll() {
        let canceler = DecommissionCanceler::new(CancellationToken::new());
        let cancelers = vec![Some(canceler.clone())];
        let guards = guard_decommission_cancelers(vec![(0, canceler.clone())]);
        let unpolled = async move {
            let _guards = guards;
            std::future::pending::<()>().await;
        };

        drop(unpolled);

        assert!(canceler.is_cancelled());
        assert!(!has_active_decommission_canceler(cancelers.as_slice()));
    }

    #[test]
    fn test_partial_decommission_spawn_reservation_releases_bound_slot() {
        let parent = CancellationToken::new();
        let mut cancelers = vec![None];
        let bound = bind_decommission_cancelers(&[0, 1], &parent, cancelers.as_mut_slice());
        let guards = guard_decommission_cancelers(bound);

        let result = super::ensure_decommission_routines_scheduled(guards.len(), 2);
        drop(guards);

        assert!(result.is_err());
        assert!(!has_active_decommission_canceler(cancelers.as_slice()));
    }

    #[tokio::test]
    async fn test_decommission_supervisor_observes_worker_abort() {
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let worker = tokio::spawn(async move {
            started_tx.send(()).expect("worker start should be observed");
            std::future::pending::<()>().await;
            #[allow(unreachable_code)]
            Ok(())
        });

        started_rx.await.expect("worker start should be observed");
        worker.abort();
        let err = await_decommission_worker(3, worker)
            .await
            .expect_err("supervisor should observe aborted worker");

        assert!(err.to_string().contains("decommission worker 3 task join error"));
    }

    #[tokio::test]
    async fn test_decommission_supervisor_observes_worker_panic() {
        let worker = tokio::spawn(async move {
            panic!("injected decommission worker panic");
        });

        let err = await_decommission_worker(4, worker)
            .await
            .expect_err("supervisor should observe panicked worker");

        assert!(err.to_string().contains("decommission worker 4 task join error"));
    }

    #[tokio::test]
    async fn test_decommission_worker_metadata_missing_releases_owned_slot() {
        let (_temp_dirs, store, _other_store) = crate::services::rebalance::test_two_pool_stores(None).await;
        let canceler = DecommissionCanceler::new(CancellationToken::new());
        *store.pool_meta.write().await = PoolMeta::default();
        store.decommission_cancelers.write().await[0] = Some(canceler.clone());

        let err = store
            .do_decommission_in_routine(canceler.clone(), 0, Arc::new(Semaphore::new(1)))
            .await
            .expect_err("missing worker metadata should fail the routine");

        assert!(err.to_string().contains("target pool was not found"));
        assert!(!canceler.is_active());
        assert!(store.decommission_cancelers.read().await[0].is_none());
    }

    #[tokio::test]
    async fn test_decommission_supervisor_failure_cancels_queued_successor() {
        let first = DecommissionCanceler::new(CancellationToken::new());
        let queued = DecommissionCanceler::new(CancellationToken::new());
        let store = decommission_worker_test_store(PoolMeta::default(), vec![Some(first.clone()), Some(queued.clone())]);
        let guards = guard_decommission_cancelers(vec![(0, first.clone()), (1, queued.clone())]);

        spawn_decommission_index_cancelers(store.clone(), CancellationToken::new(), guards, Arc::new(Semaphore::new(1)))
            .await
            .expect("decommission supervisor should finish after queued cleanup");

        assert!(!first.is_active());
        assert!(!queued.is_active());
        assert!(queued.is_cancelled());
        assert!(store.decommission_cancelers.read().await.iter().all(Option::is_none));
    }

    #[tokio::test]
    async fn test_decommission_supervisor_releases_slot_without_terminal_retry_when_pool_meta_is_blocked() {
        let generation = OffsetDateTime::UNIX_EPOCH;
        let canceler = DecommissionCanceler::new(CancellationToken::new());
        let pool_meta = PoolMeta {
            version: super::POOL_META_VERSION,
            pools: vec![decommission_test_pool_status(
                0,
                Some(PoolDecommissionInfo {
                    start_time: Some(generation),
                    ..Default::default()
                }),
            )],
            ..Default::default()
        };
        let store = decommission_worker_test_store(pool_meta, vec![Some(canceler.clone())]);
        store
            .pool_meta_save_gate
            .lock()
            .await
            .observe_replicas(super::PoolMetaReplicaState {
                needs_repair: true,
                repair_write_safe: false,
            });
        let guards = guard_decommission_cancelers(vec![(0, canceler.clone())]);

        tokio::time::timeout(
            StdDuration::from_secs(1),
            spawn_decommission_index_cancelers(store.clone(), CancellationToken::new(), guards, Arc::new(Semaphore::new(1))),
        )
        .await
        .expect("blocked supervisor must not enter terminal retry")
        .expect("blocked supervisor task should not panic");

        assert!(store.decommission_cancelers.read().await[0].is_none());
        assert!(!canceler.is_active());
        let pool_meta = store.pool_meta.read().await;
        let info = pool_meta.pools[0]
            .decommission
            .as_ref()
            .expect("blocked decommission metadata should remain active");
        assert!(!info.failed);
        assert!(!info.canceled);
        assert!(!info.complete);
        assert_eq!(info.start_time, Some(generation));
    }

    #[tokio::test]
    async fn test_decommission_promotion_rechecks_sticky_gate_after_start_wait() {
        let pool_meta = PoolMeta {
            pools: vec![decommission_test_pool_status(
                0,
                Some(PoolDecommissionInfo {
                    queued: true,
                    ..Default::default()
                }),
            )],
            ..Default::default()
        };
        let store = decommission_worker_test_store(pool_meta, vec![None]);
        let start_guard = store.start_gate.lock().await;
        let mut promotion = tokio::spawn({
            let store = store.clone();
            async move { store.promote_queued_decommission_for_test(0).await }
        });
        tokio::task::yield_now().await;
        assert!(!promotion.is_finished(), "promotion should be waiting for the start gate");

        store
            .pool_meta_save_gate
            .lock()
            .await
            .observe_replicas(super::PoolMetaReplicaState {
                needs_repair: true,
                repair_write_safe: false,
            });
        drop(start_guard);

        let err = tokio::time::timeout(StdDuration::from_secs(1), &mut promotion)
            .await
            .expect("blocked promotion should finish")
            .expect("promotion task should not panic")
            .expect_err("promotion must recheck the sticky gate after waiting");
        assert!(
            err.to_string()
                .contains("restart after all replicas are readable and consistent")
        );

        let pool_meta = store.pool_meta.read().await;
        let info = pool_meta.pools[0]
            .decommission
            .as_ref()
            .expect("queued metadata should remain present");
        assert!(info.queued);
        assert!(info.start_time.is_none());
    }

    #[tokio::test]
    async fn test_decommission_bucket_done_rechecks_sticky_gate_before_mutation() {
        let bucket = DecomBucketInfo {
            name: "bucket-a".to_string(),
            prefix: String::new(),
        };
        let pool_meta = PoolMeta {
            pools: vec![decommission_test_pool_status(
                0,
                Some(PoolDecommissionInfo {
                    queued_buckets: vec![bucket.to_string()],
                    ..Default::default()
                }),
            )],
            ..Default::default()
        };
        let store = decommission_worker_test_store(pool_meta, vec![None]);
        store
            .pool_meta_save_gate
            .lock()
            .await
            .observe_replicas(super::PoolMetaReplicaState {
                needs_repair: true,
                repair_write_safe: false,
            });

        let err = store
            .mark_decommission_bucket_done_and_save(0, &bucket)
            .await
            .expect_err("bucket completion must stop before mutating sticky pool metadata");
        assert!(
            err.to_string()
                .contains("restart after all replicas are readable and consistent")
        );

        let pool_meta = store.pool_meta.read().await;
        let info = pool_meta.pools[0]
            .decommission
            .as_ref()
            .expect("decommission metadata should remain present");
        assert_eq!(info.queued_buckets, vec![bucket.to_string()]);
        assert!(info.decommissioned_buckets.is_empty());
    }

    #[tokio::test]
    async fn test_decommission_cancel_save_failure_preserves_generation_and_blocks_retry() {
        let generation = OffsetDateTime::UNIX_EPOCH;
        let canceler = DecommissionCanceler::new(CancellationToken::new());
        let pool_meta = PoolMeta {
            version: super::POOL_META_VERSION,
            pools: vec![decommission_test_pool_status(
                0,
                Some(PoolDecommissionInfo {
                    start_time: Some(generation),
                    ..Default::default()
                }),
            )],
            ..Default::default()
        };
        let store = decommission_worker_test_store(pool_meta, vec![Some(canceler.clone())]);

        let err = store
            .decommission_cancel_with_owner_and_save(0, Some(&canceler), |_, _| async { Err(Error::Timeout) })
            .await
            .expect_err("injected pool metadata timeout should fail cancel");
        assert!(matches!(err, Error::Timeout));

        {
            let pool_meta = store.pool_meta.read().await;
            let info = pool_meta.pools[0]
                .decommission
                .as_ref()
                .expect("failed cancel must retain decommission metadata");
            assert_eq!(info.start_time, Some(generation));
            assert!(!info.canceled);
            assert!(!info.complete);
            assert!(!info.failed);
            assert!(ensure_decommission_generation(&pool_meta, 0, generation).is_ok());
        }
        {
            let cancelers = store.decommission_cancelers.read().await;
            let current = cancelers[0].as_ref().expect("failed cancel must retain the worker owner");
            assert!(current.owns_same_operation(&canceler));
            assert!(current.is_active());
        }
        assert!(!canceler.is_cancelled());
        assert!(store.decommission_terminal_retryable_for_operation(0, &canceler).await);

        let retry_save_called = Arc::new(AtomicBool::new(false));
        let retry_save_called_by_closure = retry_save_called.clone();
        let retry_err = store
            .decommission_cancel_with_owner_and_save(0, Some(&canceler), move |_, _| async move {
                retry_save_called_by_closure.store(true, Ordering::SeqCst);
                Ok(())
            })
            .await
            .expect_err("an ambiguous save failure must block same-process retry");
        assert!(
            retry_err
                .to_string()
                .contains("restart after all replicas are readable and consistent")
        );
        assert!(!retry_save_called.load(Ordering::SeqCst));

        {
            let pool_meta = store.pool_meta.read().await;
            let info = pool_meta.pools[0]
                .decommission
                .as_ref()
                .expect("blocked retry must retain decommission metadata");
            assert_eq!(info.start_time, Some(generation));
            assert!(!info.canceled);
            assert!(!info.complete);
            assert!(!info.failed);
        }
        assert!(store.decommission_cancelers.read().await[0].is_some());
        assert!(canceler.is_active());
        assert!(!canceler.is_cancelled());
    }

    #[tokio::test]
    async fn test_decommission_cancel_stays_blocked_after_unreadable_pool_meta_replica() {
        let generation = OffsetDateTime::UNIX_EPOCH;
        let canceler = DecommissionCanceler::new(CancellationToken::new());
        let pool_meta = PoolMeta {
            version: super::POOL_META_VERSION,
            pools: vec![decommission_test_pool_status(
                0,
                Some(PoolDecommissionInfo {
                    start_time: Some(generation),
                    ..Default::default()
                }),
            )],
            ..Default::default()
        };
        let store = decommission_worker_test_store(pool_meta, vec![Some(canceler.clone())]);
        store
            .pool_meta_save_gate
            .lock()
            .await
            .observe_replicas(super::PoolMetaReplicaState {
                needs_repair: true,
                repair_write_safe: false,
            });
        let save_called = Arc::new(AtomicBool::new(false));
        let save_called_by_closure = save_called.clone();

        let err = store
            .decommission_cancel_with_owner_and_save(0, Some(&canceler), move |_, _| async move {
                save_called_by_closure.store(true, Ordering::SeqCst);
                Ok(())
            })
            .await
            .expect_err("cancel must remain blocked until restart after an unreadable replica");

        assert!(
            err.to_string()
                .contains("restart after all replicas are readable and consistent")
        );
        assert!(!save_called.load(Ordering::SeqCst));
        let pool_meta = store.pool_meta.read().await;
        let info = pool_meta.pools[0]
            .decommission
            .as_ref()
            .expect("blocked cancel must preserve decommission metadata");
        assert_eq!(info.start_time, Some(generation));
        assert!(!info.canceled);
        drop(pool_meta);
        assert!(canceler.is_active());
        assert!(!canceler.is_cancelled());
        let cancelers = store.decommission_cancelers.read().await;
        assert!(
            cancelers[0]
                .as_ref()
                .is_some_and(|current| current.owns_same_operation(&canceler))
        );
    }

    #[tokio::test]
    async fn test_decommission_cancel_serializes_reload_until_local_commit() {
        let generation = OffsetDateTime::UNIX_EPOCH;
        let canceler = DecommissionCanceler::new(CancellationToken::new());
        let pool_meta = PoolMeta {
            version: super::POOL_META_VERSION,
            pools: vec![decommission_test_pool_status(
                0,
                Some(PoolDecommissionInfo {
                    start_time: Some(generation),
                    stage: "migrate_object".to_string(),
                    ..Default::default()
                }),
            )],
            ..Default::default()
        };
        let store = decommission_worker_test_store(pool_meta, vec![Some(canceler.clone())]);
        let (persisted_tx, persisted_rx) = tokio::sync::oneshot::channel();
        let save_release = Arc::new(tokio::sync::Notify::new());

        let cancel = tokio::spawn({
            let store = store.clone();
            let canceler = canceler.clone();
            let save_release = save_release.clone();
            async move {
                store
                    .decommission_cancel_with_owner_and_save(0, Some(&canceler), move |snapshot, _| async move {
                        persisted_tx
                            .send(snapshot.encode_config_data()?)
                            .map_err(|_| Error::other("failed to expose saved cancel snapshot"))?;
                        save_release.notified().await;
                        Ok(())
                    })
                    .await
            }
        });
        let persisted = persisted_rx.await.expect("save should expose the canceled snapshot");
        let reload_started = Arc::new(tokio::sync::Notify::new());
        let reload = tokio::spawn({
            let store = store.clone();
            let reload_started = reload_started.clone();
            async move {
                let mut reloaded = PoolMeta::default();
                reloaded.load_from_config_data(persisted)?;
                reload_started.notify_one();
                *store.pool_meta.write().await = reloaded;
                Ok::<(), Error>(())
            }
        });
        reload_started.notified().await;
        assert!(!reload.is_finished(), "peer reload must wait for cancel publication");

        save_release.notify_one();
        cancel
            .await
            .expect("cancel task should not panic")
            .expect("cancel should commit before releasing the reload");
        reload
            .await
            .expect("reload task should not panic")
            .expect("reload should install the saved cancel after publication");

        let pool_meta = store.pool_meta.read().await;
        let info = pool_meta.pools[0]
            .decommission
            .as_ref()
            .expect("reloaded cancel metadata should remain present");
        assert!(info.canceled);
        assert!(!info.complete);
        assert!(!info.failed);
        assert!(info.start_time.is_none());
        assert!(info.stage.is_empty(), "the persisted snapshot should have been decoded before commit");
        drop(pool_meta);
        assert!(store.decommission_cancelers.read().await[0].is_none());
        assert!(!canceler.is_active());
        assert!(canceler.is_cancelled());
    }

    #[tokio::test]
    async fn test_decommission_cancel_transaction_survives_caller_abort_after_durable_save() {
        let generation = OffsetDateTime::UNIX_EPOCH;
        let canceler = DecommissionCanceler::new(CancellationToken::new());
        let pool_meta = PoolMeta {
            version: super::POOL_META_VERSION,
            pools: vec![decommission_test_pool_status(
                0,
                Some(PoolDecommissionInfo {
                    start_time: Some(generation),
                    ..Default::default()
                }),
            )],
            ..Default::default()
        };
        let store = decommission_worker_test_store(pool_meta, vec![Some(canceler.clone())]);
        let persisted = Arc::new(std::sync::Mutex::new(None));
        let (durable_tx, durable_rx) = tokio::sync::oneshot::channel();
        let save_release = Arc::new(tokio::sync::Notify::new());
        let cancel = tokio::spawn({
            let store = store.clone();
            let canceler = canceler.clone();
            let persisted = persisted.clone();
            let save_release = save_release.clone();
            async move {
                store
                    .decommission_cancel_with_owner_and_save(0, Some(&canceler), move |snapshot, _| async move {
                        assert!(
                            snapshot.pools[0]
                                .decommission
                                .as_ref()
                                .is_some_and(|info| info.canceled && info.start_time.is_none())
                        );
                        *persisted.lock().expect("persisted cancel lock should not be poisoned") =
                            Some(snapshot.encode_config_data()?);
                        durable_tx
                            .send(())
                            .map_err(|_| Error::other("failed to report durable cancel snapshot"))?;
                        save_release.notified().await;
                        Ok(())
                    })
                    .await
            }
        });
        durable_rx.await.expect("save hook should report the durable cancel snapshot");
        cancel.abort();
        let join_err = cancel.await.expect_err("caller cancel future should be aborted");
        assert!(join_err.is_cancelled());
        assert!(
            store.pool_meta.try_read().is_err(),
            "the detached transaction must retain its state guard"
        );
        assert!(
            store.decommission_cancelers.try_read().is_err(),
            "the detached transaction must retain its owner guard"
        );

        save_release.notify_one();
        tokio::time::timeout(StdDuration::from_secs(1), canceler.token().cancelled())
            .await
            .expect("detached cancel transaction should terminate the old token");

        let persisted = persisted
            .lock()
            .expect("persisted cancel lock should not be poisoned")
            .take()
            .expect("save should capture the durable cancel snapshot");
        let mut durable = PoolMeta::default();
        durable
            .load_from_config_data(persisted)
            .expect("durable cancel snapshot should decode");
        assert!(
            durable.pools[0]
                .decommission
                .as_ref()
                .is_some_and(|info| info.canceled && info.start_time.is_none())
        );
        assert!(store.decommission_cancelers.read().await[0].is_none());
        assert!(!canceler.is_active());
        assert!(canceler.is_cancelled());

        let side_effect_ran = Arc::new(AtomicBool::new(false));
        let operation_gate = store.ctx.data_movement_operation_gate();
        let result = run_decommission_side_effect(canceler.token(), &operation_gate, {
            let side_effect_ran = side_effect_ran.clone();
            move || async move {
                side_effect_ran.store(true, Ordering::SeqCst);
                Ok(())
            }
        })
        .await;
        assert!(matches!(result, Err(Error::OperationCanceled)));
        assert!(!side_effect_ran.load(Ordering::SeqCst));
        assert!(
            store.pool_meta.read().await.pools[0]
                .decommission
                .as_ref()
                .is_some_and(|info| info.canceled && !info.failed)
        );
    }

    #[tokio::test]
    async fn test_decommission_cancel_persists_before_signaling_and_quiesces_before_return() {
        let generation = OffsetDateTime::UNIX_EPOCH;
        let canceler = DecommissionCanceler::new(CancellationToken::new());
        let pool_meta = PoolMeta {
            pools: vec![decommission_test_pool_status(
                0,
                Some(PoolDecommissionInfo {
                    start_time: Some(generation),
                    ..Default::default()
                }),
            )],
            ..Default::default()
        };
        let store = decommission_worker_test_store(pool_meta, vec![Some(canceler.clone())]);
        let operation_gate = store.ctx.data_movement_operation_gate();
        let side_effect = operation_gate.read().await;
        let save_started = Arc::new(tokio::sync::Notify::new());
        let save_release = Arc::new(tokio::sync::Notify::new());
        let save_entered = Arc::new(AtomicBool::new(false));

        let mut cancel = tokio::spawn({
            let store = store.clone();
            let canceler = canceler.clone();
            let save_started = save_started.clone();
            let save_release = save_release.clone();
            let save_entered = save_entered.clone();
            async move {
                store
                    .decommission_cancel_with_owner_and_save(0, Some(&canceler), move |_, _| async move {
                        save_entered.store(true, Ordering::SeqCst);
                        save_started.notify_one();
                        save_release.notified().await;
                        Ok(())
                    })
                    .await
            }
        });

        tokio::time::timeout(StdDuration::from_secs(1), save_started.notified())
            .await
            .expect("cancel should persist while the side effect is still in flight");
        assert!(save_entered.load(Ordering::SeqCst));
        assert!(!cancel.is_finished(), "cancel must wait for the injected save");
        assert!(
            store.pool_meta_save_gate.try_lock().is_err(),
            "the cancel save must exclude stale full-document saves until publication"
        );
        assert!(
            store.pool_meta.try_read().is_err(),
            "the active generation must stay write-locked through persistence"
        );
        assert!(!canceler.is_cancelled(), "the token must remain live until persistence commits");

        let mut fail = tokio::spawn({
            let store = store.clone();
            async move { store.decommission_failed(0).await }
        });
        tokio::task::yield_now().await;
        assert!(!fail.is_finished(), "fail must serialize behind the pending cancel");

        save_release.notify_one();
        tokio::time::timeout(StdDuration::from_secs(1), canceler.token().cancelled())
            .await
            .expect("the durable cancel must signal the active worker");
        assert!(!cancel.is_finished(), "cancel must wait for in-flight movement after the durable signal");
        {
            let pool_meta = store.pool_meta.read().await;
            let info = pool_meta.pools[0]
                .decommission
                .as_ref()
                .expect("the durable cancel must be published before quiescence");
            assert!(info.canceled);
            assert!(!info.complete);
            assert!(!info.failed);
            assert!(info.start_time.is_none());
        }

        drop(side_effect);
        tokio::time::timeout(StdDuration::from_secs(1), &mut cancel)
            .await
            .expect("cancel should finish after in-flight movement quiesces")
            .expect("cancel task should not panic")
            .expect("cancel should commit");
        tokio::time::timeout(StdDuration::from_secs(1), &mut fail)
            .await
            .expect("fail should finish after cancel commits")
            .expect("fail task should not panic")
            .expect("stale fail should be a no-op");

        let pool_meta = store.pool_meta.read().await;
        let info = pool_meta.pools[0]
            .decommission
            .as_ref()
            .expect("cancel metadata should remain present");
        assert!(info.canceled);
        assert!(!info.complete);
        assert!(!info.failed);
        assert!(info.start_time.is_none());
        drop(pool_meta);
        assert!(canceler.is_cancelled());
        assert!(!canceler.is_active());
        assert!(store.decommission_cancelers.read().await[0].is_none());
    }

    #[test]
    fn test_decommission_cancel_commit_rejects_a_replaced_generation() {
        let old_generation = OffsetDateTime::UNIX_EPOCH;
        let new_generation = old_generation + Duration::seconds(1);
        let mut canceled = PoolMeta {
            pools: vec![decommission_test_pool_status(
                0,
                Some(PoolDecommissionInfo {
                    start_time: Some(old_generation),
                    ..Default::default()
                }),
            )],
            ..Default::default()
        };
        let previous_last_update = canceled.pools[0].last_update;
        assert!(canceled.decommission_cancel(0));
        let canceled_pool = canceled.pools.remove(0);
        let commit = super::DecommissionCancelCommit {
            previous_start_time: Some(old_generation),
            previous_queued: false,
            previous_last_update,
            canceled_pool,
        };
        let mut current = PoolMeta {
            pools: vec![decommission_test_pool_status(
                0,
                Some(PoolDecommissionInfo {
                    start_time: Some(new_generation),
                    ..Default::default()
                }),
            )],
            ..Default::default()
        };

        let err = super::commit_decommission_cancel(&mut current, 0, commit)
            .expect_err("an old cancel must not publish over a replacement generation");
        assert!(err.to_string().contains("operation generation changed"));
        let info = current.pools[0]
            .decommission
            .as_ref()
            .expect("replacement generation should remain present");
        assert_eq!(info.start_time, Some(new_generation));
        assert!(!info.canceled);
    }

    #[tokio::test]
    async fn test_decommission_cancel_rejects_stale_retry_after_queued_replacement() {
        let old_generation = OffsetDateTime::UNIX_EPOCH;
        let canceler = DecommissionCanceler::new(CancellationToken::new());
        let pool_meta = PoolMeta {
            pools: vec![decommission_test_pool_status(
                0,
                Some(PoolDecommissionInfo {
                    start_time: Some(old_generation),
                    ..Default::default()
                }),
            )],
            ..Default::default()
        };
        let store = decommission_worker_test_store(pool_meta, vec![Some(canceler.clone())]);
        let queued_replacement = Arc::new(std::sync::Mutex::new(None));
        let queued_replacement_for_save = queued_replacement.clone();

        store
            .decommission_cancel_with_owner_and_save(0, Some(&canceler), move |snapshot, _| async move {
                let saved_cancel = snapshot.pools[0]
                    .decommission
                    .as_ref()
                    .expect("saved snapshot should contain decommission metadata");
                assert!(saved_cancel.canceled);
                assert!(saved_cancel.start_time.is_none());
                let mut queued_replacement = snapshot.clone();
                let replacement = queued_replacement
                    .pools
                    .get_mut(0)
                    .expect("cancel snapshot should contain the pool");
                replacement.last_update += Duration::seconds(1);
                let info = replacement
                    .decommission
                    .as_mut()
                    .expect("cancel snapshot should contain decommission metadata");
                info.canceled = false;
                info.queued = true;
                *queued_replacement_for_save
                    .lock()
                    .expect("queued replacement lock should not be poisoned") = Some(queued_replacement);
                Ok(())
            })
            .await
            .expect("cancel should commit before a queued replacement is installed");
        assert!(store.decommission_cancelers.read().await[0].is_none());
        assert!(!canceler.is_active());
        assert!(canceler.is_cancelled());

        let queued_replacement = queued_replacement
            .lock()
            .expect("queued replacement lock should not be poisoned")
            .take()
            .expect("save should prepare the queued replacement");
        *store.pool_meta.write().await = queued_replacement;

        let replacement_revision = {
            let pool_meta = store.pool_meta.read().await;
            let info = pool_meta.pools[0]
                .decommission
                .as_ref()
                .expect("queued replacement should remain present");
            assert!(info.queued);
            assert!(!info.canceled);
            assert!(info.start_time.is_none());
            pool_meta.pools[0].last_update
        };

        store.retry_decommission_cancel_for_operation(0, &canceler).await;

        let pool_meta = store.pool_meta.read().await;
        let info = pool_meta.pools[0]
            .decommission
            .as_ref()
            .expect("stale retry must preserve the queued replacement");
        assert_eq!(pool_meta.pools[0].last_update, replacement_revision);
        assert!(info.queued);
        assert!(!info.canceled);
        assert!(info.start_time.is_none());
    }

    #[tokio::test]
    async fn test_decommission_failed_save_failure_preserves_owner_until_retry_succeeds() {
        let canceler = DecommissionCanceler::new(CancellationToken::new());
        let pool_meta = PoolMeta {
            pools: vec![decommission_test_pool_status(
                0,
                Some(PoolDecommissionInfo {
                    start_time: Some(OffsetDateTime::UNIX_EPOCH),
                    ..Default::default()
                }),
            )],
            ..Default::default()
        };
        let store = decommission_worker_test_store(pool_meta, vec![Some(canceler.clone())]);

        store
            .decommission_failed_with_owner_and_save(0, Some(&canceler), async { Err(Error::SlowDown) })
            .await
            .expect_err("injected terminal save failure should be returned");

        {
            let cancelers = store.decommission_cancelers.read().await;
            let current = cancelers[0].as_ref().expect("failed save must retain the exact owner slot");
            assert!(current.owns_same_operation(&canceler));
            assert!(current.is_active());
        }
        {
            let pool_meta = store.pool_meta.read().await;
            let info = pool_meta.pools[0]
                .decommission
                .as_ref()
                .expect("rollback must retain active decommission metadata");
            assert!(info.has_decommission_state());
            assert!(!info.failed);
            assert!(!info.complete);
            assert!(!info.canceled);
        }
        assert!(store.decommission_terminal_retryable_for_operation(0, &canceler).await);

        store
            .decommission_failed_with_owner_and_save(0, Some(&canceler), async { Ok(()) })
            .await
            .expect("terminal retry should commit");

        let pool_meta = store.pool_meta.read().await;
        assert!(
            pool_meta.pools[0]
                .decommission
                .as_ref()
                .expect("terminal metadata should remain")
                .failed
        );
        drop(pool_meta);
        assert!(store.decommission_cancelers.read().await[0].is_none());
        assert!(!canceler.is_active());
        assert!(canceler.is_cancelled());
        assert_eq!(store.ctx.data_movement_operation_epoch(), 1);
    }

    #[test]
    fn test_stale_decommission_operation_cannot_cancel_replacement() {
        let stale = DecommissionCanceler::new(CancellationToken::new());
        let replacement = DecommissionCanceler::new(CancellationToken::new());
        let cancelers = vec![Some(replacement.clone())];
        let mut pool_meta = PoolMeta {
            pools: vec![decommission_test_pool_status(
                0,
                Some(PoolDecommissionInfo {
                    start_time: Some(OffsetDateTime::UNIX_EPOCH),
                    ..Default::default()
                }),
            )],
            ..Default::default()
        };

        let changed = update_decommission_for_operation(cancelers.as_slice(), &mut pool_meta, 0, Some(&stale), |pool_meta| {
            pool_meta.decommission_cancel(0)
        });

        assert!(changed.is_none());
        assert!(
            !pool_meta.pools[0]
                .decommission
                .as_ref()
                .expect("replacement metadata should remain")
                .canceled
        );
        assert!(replacement.is_active());
        assert!(!replacement.is_cancelled());
        assert!(!stale.is_active());
        assert!(stale.is_cancelled());
    }

    #[test]
    fn test_ensure_decommission_routines_scheduled_accepts_positive_bound_count() {
        assert!(super::ensure_decommission_routines_scheduled(2, 2).is_ok());
    }

    #[test]
    fn test_ensure_decommission_routines_scheduled_rejects_zero_bound_count() {
        let err = super::ensure_decommission_routines_scheduled(0, 1).expect_err("zero bound count should be rejected");
        assert!(
            err.to_string()
                .contains("failed to start decommission routines: scheduled 0 of 1 expected workers")
        );
    }

    #[test]
    fn test_ensure_decommission_routines_scheduled_rejects_partial_binding() {
        let err = super::ensure_decommission_routines_scheduled(1, 2).expect_err("partial binding should be rejected");
        assert!(
            err.to_string()
                .contains("failed to start decommission routines: scheduled 1 of 2 expected workers")
        );
    }

    #[test]
    #[cfg(windows)]
    fn test_path2_bucket_object_with_base_path_supports_windows_separators() {
        let (bucket, object) = super::path2_bucket_object_with_base_path("C:\\data", "C:\\data\\my-bucket\\nested\\object.txt");

        assert_eq!(bucket, "my-bucket");
        assert_eq!(object, "nested/object.txt");
    }
}
