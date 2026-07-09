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
use crate::bucket::versioning_sys::BucketVersioningSys;
use crate::bucket::{
    lifecycle::{
        bucket_lifecycle_audit::LcEventSrc,
        bucket_lifecycle_ops::{
            LifecycleOps, apply_expiry_on_transitioned_object, apply_expiry_rule, eval_action_from_lifecycle,
            lifecycle_delete_all_versions_blocked_by_replication,
        },
        lifecycle::IlmAction,
    },
    metadata_sys,
    object_lock::objectlock_sys::BucketObjectLockSys,
};
use crate::cache_value::metacache_set::{ListPathRawOptions, list_path_raw};
use crate::config::com::{CONFIG_PREFIX, read_config, read_config_no_lock, save_config, save_config_with_opts};
use crate::data_movement;
use crate::data_movement::backpressure::{self, DataMovementOperation};
use crate::data_usage::DATA_USAGE_CACHE_NAME;
use crate::disk::error::DiskError;
use crate::disk::{BUCKET_META_PREFIX, RUSTFS_META_BUCKET};
use crate::error::{Error, Result};
use crate::error::{
    StorageError, is_err_bucket_exists, is_err_bucket_not_found, is_err_object_not_found, is_err_version_not_found,
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
    namespace::NamespaceLocking as _,
    object::{EcstoreObjectIO, ObjectIO as _, ObjectOperations as _},
};
use crate::{core::sets::Sets, store::ECStore};
use byteorder::{ByteOrder, LittleEndian, WriteBytesExt};
use futures::{StreamExt, future::BoxFuture, stream::FuturesUnordered};
use http::HeaderMap;
#[cfg(test)]
use rmp_serde::Deserializer;
use rmp_serde::Serializer;
use rustfs_common::defer;
use rustfs_common::heal_channel::HealOpts;
use rustfs_filemeta::{FileInfoVersions, MetaCacheEntries, MetaCacheEntry, MetadataResolutionParams};
use rustfs_utils::path::{encode_dir_object, path_join, path_to_bucket_object, path_to_bucket_object_with_base_path};
use s3s::dto::{BucketLifecycleConfiguration, DefaultRetention, ReplicationConfiguration};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fmt::Display;
#[cfg(test)]
use std::io::Cursor;
use std::io::Write;
use std::path::PathBuf;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use time::{Duration, OffsetDateTime};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

const LOG_COMPONENT_ECSTORE: &str = "ecstore";
const LOG_SUBSYSTEM_POOLS: &str = "pools";
const EVENT_DECOMMISSION_STATE: &str = "decommission_state";
const EVENT_DECOMMISSION_BUCKET: &str = "decommission_bucket";
const EVENT_DECOMMISSION_ENTRY: &str = "decommission_entry";
const DECOMMISSION_STAGE_MIGRATE_OBJECT: &str = "migrate_object";
const DECOMMISSION_STAGE_CLEANUP_PREFLIGHT: &str = "cleanup_preflight";
const DECOMMISSION_STAGE_SOURCE_CLEANUP: &str = "source_cleanup";
const DECOMMISSION_STAGE_ENTRY_FINISHED: &str = "entry_finished";
const DECOMMISSION_PROGRESS_SAVE_INTERVAL: Duration = Duration::seconds(30);
const DECOMMISSION_PROGRESS_SAVE_ITEM_THRESHOLD: usize = 1000;
const DECOMMISSION_BUCKET_CONCURRENCY_ENV: &str = "RUSTFS_DECOMMISSION_BUCKET_CONCURRENCY";
const DECOMMISSION_BUCKET_CONCURRENCY_DEFAULT_CAP: usize = 4;
const DECOMMISSION_TARGET_CAPACITY_OVERHEAD_PERCENT: usize = 30;

pub const POOL_META_NAME: &str = "pool.bin";
pub const POOL_META_FORMAT: u16 = 1;
pub const POOL_META_VERSION: u16 = 1;

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
    cancelers: &mut [Option<CancellationToken>],
) -> Vec<(usize, CancellationToken)> {
    let mut bound = Vec::with_capacity(indices.len());

    for idx in indices {
        if let Some(slot) = cancelers.get_mut(*idx) {
            if let Some(existing) = slot.take() {
                existing.cancel();
            }
            let token = parent.child_token();
            *slot = Some(token.clone());
            bound.push((*idx, token));
        }
    }

    bound
}

fn bind_missing_decommission_cancelers(
    indices: &[usize],
    parent: &CancellationToken,
    cancelers: &mut [Option<CancellationToken>],
) -> Vec<(usize, CancellationToken)> {
    let mut bound = Vec::with_capacity(indices.len());

    for idx in indices {
        let Some(slot) = cancelers.get_mut(*idx) else {
            continue;
        };
        if slot.is_some() {
            break;
        }
        let token = parent.child_token();
        *slot = Some(token.clone());
        bound.push((*idx, token));
    }

    bound
}

fn take_decommission_canceler(cancelers: &mut [Option<CancellationToken>], idx: usize) -> Option<CancellationToken> {
    cancelers.get_mut(idx).and_then(Option::take)
}

fn has_active_decommission_canceler(cancelers: &[Option<CancellationToken>]) -> bool {
    cancelers.iter().any(Option::is_some)
}

fn cancel_decommission_canceler(canceler: Option<CancellationToken>) -> bool {
    if let Some(canceler) = canceler {
        canceler.cancel();
        true
    } else {
        false
    }
}

fn take_and_cancel_decommission_canceler(cancelers: &mut [Option<CancellationToken>], idx: usize) -> bool {
    let canceler = take_decommission_canceler(cancelers, idx);
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

fn default_decommission_bucket_concurrency(cpu_count: usize) -> usize {
    cpu_count.clamp(1, DECOMMISSION_BUCKET_CONCURRENCY_DEFAULT_CAP)
}

fn decommission_bucket_concurrency_limit() -> usize {
    let default_limit = default_decommission_bucket_concurrency(num_cpus::get());
    rustfs_utils::get_env_usize(DECOMMISSION_BUCKET_CONCURRENCY_ENV, default_limit).max(1)
}

fn is_decommission_meta_bucket(bucket: &DecomBucketInfo) -> bool {
    bucket.name == RUSTFS_META_BUCKET
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

fn first_resumable_decommission_queue_indices(meta: &PoolMeta) -> Vec<usize> {
    let mut indices = Vec::new();
    for (idx, pool) in meta.pools.iter().enumerate() {
        if let Some(decommission) = &pool.decommission {
            if !decommission.has_decommission_state() {
                continue;
            }
            if decommission.complete {
                continue;
            }
            if decommission.failed || decommission.canceled {
                break;
            }
            indices.push(idx);
        }
    }

    indices
}

fn missing_decommission_worker_prefix(indices: &[usize], cancelers: &[Option<CancellationToken>]) -> Vec<usize> {
    let mut missing = Vec::with_capacity(indices.len());

    for idx in indices {
        if cancelers.get(*idx).and_then(Option::as_ref).is_some() {
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
        info.mark_progress_saved();
    }

    info
}

fn spawn_decommission_index_cancelers(
    store: Arc<ECStore>,
    rx: CancellationToken,
    index_cancelers: Vec<(usize, CancellationToken)>,
) {
    tokio::spawn(async move {
        let mut stop_queue = false;

        for (idx, canceler) in index_cancelers {
            if stop_queue || rx.is_cancelled() {
                canceler.cancel();
                if let Err(err) = store.decommission_cancel(idx).await {
                    warn!(
                        event = EVENT_DECOMMISSION_STATE,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_POOLS,
                        pool_index = idx,
                        state = "queued_cancel_failed",
                        error = %err,
                        "Failed to cancel queued decommission"
                    );
                }
                continue;
            }

            if let Err(err) = store.do_decommission_in_routine(canceler, idx).await {
                error!(
                    event = EVENT_DECOMMISSION_STATE,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_POOLS,
                    pool_index = idx,
                    state = "routine_failed",
                    error = %err,
                    "Decommission routine failed"
                );
                stop_queue = true;
                continue;
            }

            stop_queue = {
                let pool_meta = store.pool_meta.read().await;
                !should_continue_decommission_queue(&pool_meta, idx)
            };
        }
    });
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
    info.has_decommission_state() && !info.queued
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
        DecommissionStartPoolState::Blocked
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
        DecommissionStartPoolState::Active => Ok(()),
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
    if active_count <= indices.len() {
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

fn touch_decommission_progress(meta: &mut PoolMeta, idx: usize) -> Result<()> {
    let pool_count = meta.pools.len();
    ensure_valid_decommission_pool_index(pool_count, idx)?;

    let Some(pool) = meta.pools.get_mut(idx) else {
        return Err(invalid_decommission_pool_index_error(pool_count, idx));
    };
    let Some(info) = pool.decommission.as_mut() else {
        return Err(decommission_metadata_not_initialized_error("touch decommission progress"));
    };

    pool.last_update = OffsetDateTime::now_utc();
    info.mark_progress_saved();
    Ok(())
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

fn resolve_decommission_bucket_done_save_result(result: Result<()>, idx: usize, bucket: &str) -> Result<()> {
    result.map_err(|err| Error::other(format!("decommission metadata save failed for pool {idx} bucket {bucket}: {err}")))
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

fn resolve_decommission_spawn_failure_result(spawn_err: Error, rollback_err: Option<Error>) -> Error {
    if let Some(rollback_err) = rollback_err {
        Error::other(format!(
            "decommission spawn routines failed: {spawn_err}; rollback failed: {rollback_err}"
        ))
    } else {
        spawn_err
    }
}

fn decommission_item_size<T>(size: T) -> usize
where
    usize: TryFrom<T>,
{
    usize::try_from(size).unwrap_or_default()
}

fn with_decommission_entry_context<E: std::fmt::Display>(stage: &str, bucket: &str, object: &str, err: E) -> Error {
    Error::other(format!("decommission entry {stage} failed for bucket {bucket} object {object}: {err}"))
}

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
    if let Some(err) = entry_error { Err(err) } else { list_result }
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

fn merge_pool_status_refresh(current: &mut PoolMeta, persisted: PoolMeta, active_workers: &[bool]) {
    if persisted.pools.is_empty() {
        return;
    }

    if current.pools.is_empty() {
        *current = persisted;
        return;
    }

    for (idx, persisted_pool) in persisted.pools.into_iter().enumerate() {
        if persisted_pool.id != idx {
            continue;
        }

        let has_active_worker = active_workers.get(idx).copied().unwrap_or(false);
        if idx < current.pools.len() {
            if should_replace_pool_status_for_status_refresh(current.pools.get(idx), &persisted_pool, has_active_worker) {
                current.pools[idx] = persisted_pool;
            }
        } else if idx == current.pools.len() && !has_active_worker {
            current.pools.push(persisted_pool);
        }
    }
}

fn resolve_start_decommission_pool_meta_reload_result(result: Result<()>) -> Result<()> {
    resolve_decommission_pool_meta_reload_result(result, "start_decommission")
}

fn decommission_rebalance_meta_lock_error(err: rustfs_lock::LockError) -> Error {
    match err {
        rustfs_lock::LockError::QuorumNotReached { required, achieved } => Error::NamespaceLockQuorumUnavailable {
            mode: "write",
            bucket: RUSTFS_META_BUCKET.to_string(),
            object: REBAL_META_NAME.to_string(),
            required,
            achieved,
        },
        other => Error::other(format!(
            "failed to acquire rebalance metadata write lock before decommission start on {RUSTFS_META_BUCKET}/{REBAL_META_NAME}: {other}"
        )),
    }
}

fn decommission_pool_meta_lock_error(err: rustfs_lock::LockError) -> Error {
    match err {
        rustfs_lock::LockError::QuorumNotReached { required, achieved } => Error::NamespaceLockQuorumUnavailable {
            mode: "write",
            bucket: RUSTFS_META_BUCKET.to_string(),
            object: POOL_META_NAME.to_string(),
            required,
            achieved,
        },
        other => Error::other(format!(
            "failed to acquire pool metadata write lock before decommission start on {RUSTFS_META_BUCKET}/{POOL_META_NAME}: {other}"
        )),
    }
}

fn rollback_decommission_pool_meta(pool_meta: &mut PoolMeta, previous_pool_meta: PoolMeta) {
    *pool_meta = previous_pool_meta;
}

fn rollback_start_decommission_pool_meta(pool_meta: &mut PoolMeta, previous_pool_meta: PoolMeta) {
    rollback_decommission_pool_meta(pool_meta, previous_pool_meta);
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
    worker_result: std::result::Result<(), tokio::task::JoinError>,
) -> Result<()> {
    worker_result.map_err(|err| Error::other(format!("decommission listing worker {set_idx} task join error: {err}")))
}

fn should_count_decommission_version_complete(ignore: bool, cleanup_ignored: bool, failure: bool) -> bool {
    cleanup_ignored || (!ignore && !failure)
}

fn is_decommission_copy_cleanup_safe_error(err: &Error) -> bool {
    // DataMovementOverwriteErr only means source and destination pool resolved to
    // the same pool. Without a target equivalence check it is not cleanup-safe.
    is_err_object_not_found(err) || is_err_version_not_found(err)
}

fn is_decommission_target_capacity_error(err: &Error) -> bool {
    if matches!(err, Error::DiskFull | Error::StorageFull) {
        return true;
    }

    let message = err.to_string();
    let disk_full = Error::DiskFull.to_string();
    let storage_full = Error::StorageFull.to_string();
    message.contains(&disk_full) || message.contains(&storage_full)
}

fn should_cleanup_decommission_source_entry(decommissioned: usize, total_versions: usize, expired: usize) -> bool {
    decommissioned.saturating_add(expired) == total_versions
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DecommissionTerminalState {
    Completed,
    Failed,
}

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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PersistedPoolMeta {
    pub version: u16,
    pub pools: Vec<PersistedPoolStatus>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
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

impl TryFrom<PersistedPoolMeta> for PoolMeta {
    type Error = Error;

    fn try_from(value: PersistedPoolMeta) -> Result<Self> {
        Ok(Self {
            version: value.version,
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
        Ok(Self {
            version,
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
            progress_save_item_baseline: value.items_decommissioned.saturating_add(value.items_decommission_failed),
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
            progress_save_item_baseline: value.items_decommissioned.saturating_add(value.items_decommission_failed),
        })
    }
}

impl From<&PoolMeta> for PersistedPoolMeta {
    fn from(value: &PoolMeta) -> Self {
        Self {
            version: value.version,
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
        }
    }
}

impl PoolMeta {
    fn decode_pool_meta_payload(payload: &[u8]) -> Result<Self> {
        match rmp_serde::from_slice::<PersistedPoolMeta>(payload) {
            Ok(meta) => meta.try_into(),
            Err(persisted_err) => {
                let legacy: LegacyPoolMeta = rmp_serde::from_slice(payload).map_err(|legacy_err| {
                    Error::other(format!(
                        "PoolMeta decode failed for both persisted and legacy formats: persisted={persisted_err}; legacy={legacy_err}"
                    ))
                })?;
                legacy.try_into()
            }
        }
    }

    pub fn new(pools: &[Arc<Sets>], prev_meta: &PoolMeta) -> Self {
        let mut new_meta = Self {
            version: POOL_META_VERSION,
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
        if version != POOL_META_VERSION {
            return Err(Error::other(format!("pool metadata load failed: unknown version {version}")));
        }

        *self = Self::decode_pool_meta_payload(&data[4..])?;

        if self.version != POOL_META_VERSION {
            return Err(Error::other(format!(
                "pool metadata load failed: unexpected decoded version {}",
                self.version
            )));
        }
        Ok(())
    }

    pub async fn load(&mut self, pool: Arc<Sets>, _pools: Vec<Arc<Sets>>) -> Result<()> {
        let data = match read_config(pool, POOL_META_NAME).await {
            Ok(data) => data,
            Err(err) => {
                if err == Error::ConfigNotFound {
                    return Ok(());
                }
                return Err(err);
            }
        };
        self.load_from_config_data(data)
    }

    /// Startup loads pool metadata before the full namespace-lock RPC surface is ready.
    pub(crate) async fn load_for_startup<S>(&mut self, pool: Arc<S>) -> Result<()>
    where
        S: EcstoreObjectIO,
    {
        self.load_no_lock(pool).await
    }

    async fn load_no_lock<S>(&mut self, pool: Arc<S>) -> Result<()>
    where
        S: EcstoreObjectIO,
    {
        let data = match read_config_no_lock(pool, POOL_META_NAME).await {
            Ok(data) => data,
            Err(err) => {
                if err == Error::ConfigNotFound {
                    return Ok(());
                }
                return Err(err);
            }
        };
        self.load_from_config_data(data)
    }

    fn encode_config_data(&self) -> Result<Vec<u8>> {
        if self.dont_save {
            return Ok(Vec::new());
        }
        let mut data = Vec::new();
        data.write_u16::<LittleEndian>(POOL_META_FORMAT)?;
        data.write_u16::<LittleEndian>(POOL_META_VERSION)?;
        let mut buf = Vec::new();
        PersistedPoolMeta::from(self).serialize(&mut Serializer::new(&mut buf))?;
        data.write_all(&buf)?;
        Ok(data)
    }

    pub async fn save(&self, pools: Vec<Arc<Sets>>) -> Result<()> {
        let data = self.encode_config_data()?;
        if data.is_empty() {
            return Ok(());
        }
        for pool in pools {
            save_config(pool, POOL_META_NAME, data.clone()).await?;
        }

        Ok(())
    }

    /// Startup has a single elected local writer, so it must not depend on namespace locks here.
    pub(crate) async fn save_for_startup<S>(&self, pools: Vec<Arc<S>>) -> Result<()>
    where
        S: EcstoreObjectIO,
    {
        self.save_no_lock(pools).await
    }

    async fn save_no_lock<S>(&self, pools: Vec<Arc<S>>) -> Result<()>
    where
        S: EcstoreObjectIO,
    {
        let data = self.encode_config_data()?;
        if data.is_empty() {
            return Ok(());
        }
        for pool in pools {
            save_config_with_opts(
                pool,
                POOL_META_NAME,
                data.clone(),
                &ObjectOptions {
                    max_parity: true,
                    no_lock: true,
                    ..Default::default()
                },
            )
            .await?;
        }

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
                if !d.failed {
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

        let (decommission_present, complete, failed, canceled) = pool
            .decommission
            .as_ref()
            .map(|info| (info.has_decommission_state(), info.complete, info.failed, info.canceled))
            .unwrap_or((false, false, false, false));

        ensure_decommission_clear_allowed(true, decommission_present, complete, failed, canceled)?;

        pool.last_update = OffsetDateTime::now_utc();
        pool.decommission = None;
        Ok(true)
    }

    pub fn decommission_complete(&mut self, idx: usize) -> bool {
        if let Some(stats) = self.pools.get_mut(idx) {
            if let Some(d) = &stats.decommission {
                if !d.complete {
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
            info.start_time.get_or_insert(now);
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
        let pool_count = self.pools.len();
        ensure_valid_decommission_pool_index(pool_count, idx)?;

        let (last_update, item_threshold_reached) = match self.pools.get(idx) {
            Some(pool) if let Some(info) = pool.decommission.as_ref() => (
                pool.last_update,
                info.items_since_last_progress_save() >= DECOMMISSION_PROGRESS_SAVE_ITEM_THRESHOLD,
            ),
            Some(_) => {
                return Err(decommission_metadata_not_initialized_error("update decommission metadata timestamp"));
            }
            None => return Err(invalid_decommission_pool_index_error(pool_count, idx)),
        };
        let now = OffsetDateTime::now_utc();

        if now.unix_timestamp() - last_update.unix_timestamp() >= duration.whole_seconds() || item_threshold_reached {
            let Some(pool) = self.pools.get_mut(idx) else {
                return Err(invalid_decommission_pool_index_error(pool_count, idx));
            };
            pool.last_update = now;
            return Ok(true);
        }

        Ok(false)
    }

    #[allow(dead_code)]
    pub fn validate(&self, pools: Vec<Arc<Sets>>) -> Result<bool> {
        struct PoolInfo {
            position: usize,
            completed: bool,
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
        let mut new_pools = Vec::new();
        for pool in &self.pools {
            if let Some(decommission) = &pool.decommission {
                if !decommission.has_decommission_state() {
                    continue;
                }
                if decommission.complete || decommission.failed || decommission.canceled {
                    // Recovery is not required when:
                    // - Decommissioning completed
                    // - Decommissioning failed and must be explicitly restarted or cleared
                    // - Decommissioning was cancelled
                    continue;
                }
                // All other scenarios require recovery
                new_pools.push(pool.clone());
            }
        }
        new_pools
    }
}

pub fn path2_bucket_object(name: &str) -> (String, String) {
    path_to_bucket_object(name)
}

pub fn path2_bucket_object_with_base_path(base_path: &str, path: &str) -> (String, String) {
    path_to_bucket_object_with_base_path(base_path, path)
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
    pub progress_save_item_baseline: usize,
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
    }

    fn counted_items(&self) -> usize {
        self.items_decommissioned.saturating_add(self.items_decommission_failed)
    }

    fn items_since_last_progress_save(&self) -> usize {
        self.counted_items().saturating_sub(self.progress_save_item_baseline)
    }

    fn mark_progress_saved(&mut self) {
        self.progress_save_item_baseline = self.counted_items();
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

fn decommission_remaining_version_count(total_versions: usize, expired: usize) -> usize {
    total_versions.saturating_sub(expired)
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
) -> ObjectOptions {
    ObjectOptions {
        versioned: true,
        version_id,
        mod_time: version.mod_time,
        src_pool_idx,
        data_movement: true,
        delete_marker: true,
        skip_decommissioned: true,
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
) -> ObjectOptions {
    ObjectOptions {
        versioned: version_id.is_some(),
        version_id,
        mod_time: version.mod_time,
        user_defined: version.metadata.clone(),
        src_pool_idx,
        data_movement: true,
        ..Default::default()
    }
}

fn lifecycle_action_removes_data_movement_version(action: IlmAction) -> bool {
    matches!(
        action,
        IlmAction::DeleteVersionAction | IlmAction::DeleteAllVersionsAction | IlmAction::DelMarkerDeleteAllVersionsAction
    )
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
    lock_retention: Option<DefaultRetention>,
    apply_actions: bool,
    event_source: &LcEventSrc,
) -> Result<bool> {
    let Some(lifecycle_config) = lifecycle_config else {
        return Ok(false);
    };

    let versioned = BucketVersioningSys::prefix_enabled(bucket, &version.name).await;
    let object_info = crate::object_api::ObjectInfo::from_file_info(version, bucket, &version.name, versioned);
    let event = eval_action_from_lifecycle(lifecycle_config, lock_retention, &object_info).await;

    match event.action {
        IlmAction::DeleteRestoredAction | IlmAction::DeleteRestoredVersionAction => {
            if apply_actions && object_info.is_remote() {
                let _ = apply_expiry_on_transitioned_object(store, &object_info, &event, event_source).await;
            }
            Ok(false)
        }
        action if lifecycle_action_removes_data_movement_version(action) => {
            if lifecycle_delete_all_versions_blocked_by_replication(store.clone(), bucket, &object_info.name, action).await? {
                return Ok(false);
            }
            let applied = !apply_actions || apply_expiry_rule(&event, event_source, &object_info).await;
            resolve_data_movement_lifecycle_expiry_result(action, apply_actions, applied)
        }
        _ => Ok(false),
    }
}

impl ECStore {
    async fn save_current_pool_meta(&self) -> Result<()> {
        let _save_guard = self.pool_meta_save_gate.lock().await;
        let snapshot = {
            let pool_meta = self.pool_meta.read().await;
            pool_meta.clone()
        };
        snapshot.save(self.pools.clone()).await
    }

    async fn save_current_pool_meta_for_decommission_start(
        &self,
        indices: &[usize],
        space_infos: Vec<(usize, PoolSpaceInfo)>,
        decom_buckets: Vec<DecomBucketInfo>,
    ) -> Result<PoolMeta> {
        let _save_guard = self.pool_meta_save_gate.lock().await;
        let rebalance_pool = self
            .pools
            .first()
            .cloned()
            .ok_or_else(|| Error::other("decommission start rebalance metadata load failed: no storage pools available"))?;
        let pool_meta_lock = rebalance_pool.new_ns_lock(RUSTFS_META_BUCKET, POOL_META_NAME).await?;
        let _pool_meta_guard = pool_meta_lock
            .get_write_lock(get_lock_acquire_timeout())
            .await
            .map_err(decommission_pool_meta_lock_error)?;
        let ns_lock = rebalance_pool.new_ns_lock(RUSTFS_META_BUCKET, REBAL_META_NAME).await?;
        let _guard = ns_lock
            .get_write_lock(get_lock_acquire_timeout())
            .await
            .map_err(decommission_rebalance_meta_lock_error)?;

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
        latest_pool_meta.load_no_lock(rebalance_pool).await?;
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

        latest_pool_meta.save_no_lock(self.pools.clone()).await?;
        {
            let mut pool_meta = self.pool_meta.write().await;
            *pool_meta = latest_pool_meta;
        }

        Ok(previous_pool_meta)
    }

    async fn ensure_decommission_rebalance_idle_after_refresh(&self) -> Result<()> {
        self.load_rebalance_meta().await?;
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
        let pool = self
            .pools
            .first()
            .cloned()
            .ok_or_else(|| Error::other("refresh_pool_status_meta: no pools available"))?;
        let mut persisted = PoolMeta::default();
        persisted.load(pool, self.pools.clone()).await?;

        let active_workers = {
            let cancelers = self.decommission_cancelers.read().await;
            cancelers.iter().map(Option::is_some).collect::<Vec<_>>()
        };

        let mut pool_meta = self.pool_meta.write().await;
        merge_pool_status_refresh(&mut pool_meta, persisted, &active_workers);
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
        let mut space_infos = Vec::with_capacity(self.pools.len());
        for idx in 0..self.pools.len() {
            space_infos.push((idx, self.get_decommission_pool_space_info(idx).await?));
        }
        Ok(space_infos)
    }

    #[tracing::instrument(skip(self))]
    pub async fn decommission_cancel(&self, idx: usize) -> Result<()> {
        ensure_decommission_terminal_operation_supported(self.single_pool(), "cancel decommission")?;

        let (should_save_pool_meta, should_reload_pool_meta, already_canceled, previous_pool_meta) = {
            let mut lock = self.pool_meta.write().await;
            let mut already_canceled = false;
            let (pool_present, decommission_present, terminal) = if let Some(pool) = lock.pools.get(idx) {
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
            let previous_pool_meta = lock.clone();
            let changed = lock.decommission_cancel(idx);
            (
                changed,
                should_retry_decommission_cancel_reload(changed, already_canceled),
                already_canceled,
                changed.then_some(previous_pool_meta),
            )
        };

        let canceled_worker = {
            let mut cancelers = self.decommission_cancelers.write().await;
            take_and_cancel_decommission_canceler(cancelers.as_mut_slice(), idx)
        };
        if !canceled_worker && !already_canceled {
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

        if should_save_pool_meta && let Err(err) = self.save_current_pool_meta().await {
            if let Some(previous_pool_meta) = previous_pool_meta {
                let mut pool_meta = self.pool_meta.write().await;
                rollback_decommission_pool_meta(&mut pool_meta, previous_pool_meta);
            }
            return Err(err);
        }

        if should_reload_pool_meta && let Some(notification_sys) = runtime_sources::notification_sys() {
            let stage = format!("decommission_cancel for pool {idx}");
            resolve_decommission_pool_meta_reload_result(notification_sys.reload_pool_meta().await, stage.as_str())?;
        }

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub async fn clear_decommission(&self, idx: usize) -> Result<()> {
        ensure_decommission_terminal_operation_supported(self.single_pool(), "clear decommission")?;

        let (should_reload_pool_meta, previous_pool_meta) = {
            let mut pool_meta = self.pool_meta.write().await;
            let previous_pool_meta = pool_meta.clone();
            let changed = pool_meta.clear_decommission(idx)?;
            (changed, changed.then_some(previous_pool_meta))
        };

        {
            let mut cancelers = self.decommission_cancelers.write().await;
            take_and_cancel_decommission_canceler(cancelers.as_mut_slice(), idx);
        }

        if should_reload_pool_meta && let Err(err) = self.save_current_pool_meta().await {
            if let Some(previous_pool_meta) = previous_pool_meta {
                let mut pool_meta = self.pool_meta.write().await;
                rollback_decommission_pool_meta(&mut pool_meta, previous_pool_meta);
            }
            return Err(err);
        }

        if should_reload_pool_meta && let Some(notification_sys) = runtime_sources::notification_sys() {
            let stage = format!("clear_decommission for pool {idx}");
            resolve_decommission_pool_meta_reload_result(notification_sys.reload_pool_meta().await, stage.as_str())?;
        }

        Ok(())
    }

    async fn promote_queued_decommission(&self, idx: usize) -> Result<()> {
        let promoted = {
            let mut pool_meta = self.pool_meta.write().await;
            pool_meta.promote_queued_decommission(idx)
        };

        if promoted {
            self.save_current_pool_meta().await?;
            if let Some(notification_sys) = runtime_sources::notification_sys() {
                let stage = format!("promote_queued_decommission for pool {idx}");
                resolve_decommission_pool_meta_reload_result(notification_sys.reload_pool_meta().await, stage.as_str())?;
            }
        }

        Ok(())
    }

    async fn record_decommission_terminal_reload_failure(&self, idx: usize, stage: &str, err: Error) -> Result<()> {
        let changed = {
            let mut pool_meta = self.pool_meta.write().await;
            pool_meta.record_decommission_terminal_reload_failure(idx, stage, err.to_string())?
        };

        if changed {
            self.save_current_pool_meta().await?;
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

    pub(crate) async fn spawn_decommission_routines(
        &self,
        store: Arc<ECStore>,
        rx: CancellationToken,
        indices: Vec<usize>,
    ) -> Result<()> {
        let indices = dedup_indices(&indices);
        if indices.is_empty() {
            return Ok(());
        }

        let index_cancelers = {
            let mut cancelers = self.decommission_cancelers.write().await;
            bind_decommission_cancelers(indices.as_slice(), &rx, cancelers.as_mut_slice())
        };

        ensure_decommission_routines_scheduled(index_cancelers.len(), indices.len())?;

        spawn_decommission_index_cancelers(store, rx, index_cancelers);

        Ok(())
    }

    pub async fn spawn_missing_local_decommission_routines(self: &Arc<Self>) -> Result<()> {
        let indices = {
            let pool_meta = self.pool_meta.read().await;
            first_resumable_decommission_queue_indices(&pool_meta)
        };
        let indices = local_decommission_queue_prefix(&self.endpoints(), &indices)?;
        if indices.is_empty() {
            return Ok(());
        }

        let rx = CancellationToken::new();
        let index_cancelers = {
            let mut cancelers = self.decommission_cancelers.write().await;
            let missing = missing_decommission_worker_prefix(indices.as_slice(), cancelers.as_slice());
            bind_missing_decommission_cancelers(missing.as_slice(), &rx, cancelers.as_mut_slice())
        };

        if index_cancelers.is_empty() {
            return Ok(());
        }

        spawn_decommission_index_cancelers(self.clone(), rx, index_cancelers);
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

        self.start_decommission(indices.clone()).await?;
        if let Err(err) = self.spawn_decommission_routines(store, rx, local_indices).await {
            let mut rollback_err: Option<Error> = None;
            for idx in indices {
                if let Err(cancel_err) = self.decommission_cancel(idx).await {
                    error!(
                        event = EVENT_DECOMMISSION_STATE,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_POOLS,
                        pool_index = idx,
                        state = "rollback_failed",
                        error = ?cancel_err,
                        "Decommission rollback failed after spawn error"
                    );
                    if rollback_err.is_none() {
                        rollback_err = Some(Error::other(format!("decommission rollback failed for idx {idx}: {cancel_err}")));
                    }
                }
            }
            return Err(resolve_decommission_spawn_failure_result(err, rollback_err));
        }

        Ok(())
    }

    async fn save_decommission_entry_progress_stage(
        &self,
        idx: usize,
        bucket: &str,
        object: &str,
        stage: &'static str,
    ) -> Result<()> {
        {
            let mut pool_meta = self.pool_meta.write().await;
            track_decommission_current_object_stage(&mut pool_meta, idx, bucket, object, stage)
                .map_err(|err| with_decommission_entry_context(stage, bucket, object, err))?;
            touch_decommission_progress(&mut pool_meta, idx)
                .map_err(|err| with_decommission_entry_context(stage, bucket, object, err))?;
        }

        if let Some(err) = resolve_decommission_progress_save_result(self.save_current_pool_meta().await) {
            warn!(
                event = EVENT_DECOMMISSION_ENTRY,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_POOLS,
                pool_index = idx,
                bucket = %bucket,
                object = %object,
                stage,
                error = ?err,
                "Decommission progress stage save failed"
            );
        }

        Ok(())
    }

    #[allow(unused_assignments, clippy::too_many_arguments)]
    #[tracing::instrument(skip(self, set, _worker_permit, lifecycle_config, lock_retention, replication_config))]
    async fn decommission_entry(
        self: &Arc<Self>,
        rx: CancellationToken,
        idx: usize,
        entry: MetaCacheEntry,
        bucket: String,
        set: Arc<SetDisks>,
        _worker_permit: OwnedSemaphorePermit,
        lifecycle_config: Option<BucketLifecycleConfiguration>,
        lock_retention: Option<DefaultRetention>,
        replication_config: Option<(ReplicationConfiguration, OffsetDateTime)>,
    ) -> Result<()> {
        debug!(
            event = EVENT_DECOMMISSION_ENTRY,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_POOLS,
            pool_index = idx,
            bucket = %bucket,
            object = %entry.name,
            state = "started",
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
            return Ok(());
        }
        if self.decommission_cancel_requested(idx, &rx).await {
            rx.cancel();
        }
        decommission_cancel_signal_result(rx.is_cancelled())?;

        let mut fivs = load_decommission_entry_exact_versions(&set, &entry, &bucket, "file_info_versions").await?;

        fivs.versions
            .sort_by_key(|v| (v.mod_time.is_none(), std::cmp::Reverse(v.mod_time)));

        let mut decommissioned: usize = 0;
        let mut expired: usize = 0;
        let mut cleanup_preflight_allowed_missing = Vec::new();

        for version in fivs.versions.iter() {
            if self.decommission_cancel_requested(idx, &rx).await {
                rx.cancel();
            }
            decommission_cancel_signal_result(rx.is_cancelled())?;

            if should_skip_lifecycle_for_data_movement(
                self.clone(),
                &bucket,
                version,
                lifecycle_config.as_ref(),
                lock_retention.clone(),
                true,
                &LcEventSrc::Decom,
            )
            .await
            .map_err(|err| with_decommission_entry_context("lifecycle_expiry", bucket.as_str(), version.name.as_str(), err))?
            {
                expired += 1;
                cleanup_preflight_allowed_missing.push(data_movement::source_cleanup_version_identity(version));
                continue;
            }

            let remaining_versions = decommission_remaining_version_count(fivs.versions.len(), expired);
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
                if let Err(err) = self
                    .delete_object(
                        bucket.as_str(),
                        &version.name,
                        decommission_delete_marker_opts(version, version_id.clone(), idx),
                    )
                    .await
                {
                    if is_decommission_copy_cleanup_safe_error(&err) {
                        warn!(
                            event = EVENT_DECOMMISSION_ENTRY,
                            component = LOG_COMPONENT_ECSTORE,
                            subsystem = LOG_SUBSYSTEM_POOLS,
                            pool_index = idx,
                            bucket = %bucket,
                            object = %version.name,
                            version_id = ?version_id,
                            state = "ignored_delete_marker_copy",
                            error = ?err,
                            "Decommission delete marker copy ignored"
                        );
                        ignore = true;
                        cleanup_ignored = true;
                    } else {
                        if is_decommission_target_capacity_error(&err) {
                            return Err(with_decommission_entry_context(
                                "delete_marker_copy",
                                bucket.as_str(),
                                version.name.as_str(),
                                err,
                            ));
                        }

                        failure = true;

                        error = Some(err)
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

                {
                    let mut pool_meta = self.pool_meta.write().await;
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

                debug!(
                    event = EVENT_DECOMMISSION_ENTRY,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_POOLS,
                    pool_index = idx,
                    bucket = %bucket,
                    object = %version.name,
                    version_id = ?version_id,
                    result = ?error,
                    state = "delete_marker_copied",
                    "Decommission delete marker copied"
                );
                continue;
            }

            for _i in 0..3 {
                if version.is_remote() {
                    if let Err(err) = self
                        .decommission_tiered_object(
                            bucket.as_str(),
                            &version.name,
                            version,
                            &decommission_remote_tiered_opts(version, version_id.clone(), idx),
                        )
                        .await
                    {
                        if is_decommission_copy_cleanup_safe_error(&err) {
                            ignore = true;
                            cleanup_ignored = true;
                            break;
                        }

                        if is_decommission_target_capacity_error(&err) {
                            return Err(with_decommission_entry_context(
                                "decommission_tiered_object",
                                bucket.as_str(),
                                version.name.as_str(),
                                err,
                            ));
                        }

                        failure = true;
                        error!("decommission_pool: decommission_tiered_object err {:?}", &err);
                        error = Some(err);
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
                        error!("decommission_pool: get_object_reader err {:?}", &err);
                        continue;
                    }
                };

                let bucket_name = bucket.clone();
                let object_name = rd.object_info.name.clone();

                self.save_decommission_entry_progress_stage(
                    idx,
                    bucket_name.as_str(),
                    object_name.as_str(),
                    DECOMMISSION_STAGE_MIGRATE_OBJECT,
                )
                .await?;

                if let Err(err) = self.clone().decommission_object(idx, bucket, rd).await {
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

                    error!("decommission_pool: decommission_object err {:?}", &err);
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

            {
                let mut pool_meta = self.pool_meta.write().await;
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

        if should_cleanup_decommission_source_entry(decommissioned, fivs.versions.len(), expired) {
            decommission_cancel_signal_result(rx.is_cancelled())?;

            self.save_decommission_entry_progress_stage(
                idx,
                bucket.as_str(),
                entry.name.as_str(),
                DECOMMISSION_STAGE_CLEANUP_PREFLIGHT,
            )
            .await?;

            self.save_decommission_entry_progress_stage(
                idx,
                bucket.as_str(),
                entry.name.as_str(),
                DECOMMISSION_STAGE_SOURCE_CLEANUP,
            )
            .await?;

            let cleanup_result = data_movement::cleanup_source_entry_if_unchanged(
                set.clone(),
                bucket.as_str(),
                entry.name.as_str(),
                &fivs,
                &cleanup_preflight_allowed_missing,
                "decommission",
            )
            .await;
            resolve_decommission_entry_cleanup_delete_result(cleanup_result, bucket.as_str(), entry.name.as_str())?
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

        self.save_decommission_entry_progress_stage(idx, bucket.as_str(), entry.name.as_str(), DECOMMISSION_STAGE_ENTRY_FINISHED)
            .await?;

        if should_save_progress {
            let save_result = self.save_current_pool_meta().await;
            if let Some(err) = resolve_decommission_progress_save_result(save_result) {
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
            } else {
                let mut pool_meta = self.pool_meta.write().await;
                pool_meta.mark_decommission_progress_saved();
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
        }

        debug!(
            event = EVENT_DECOMMISSION_ENTRY,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_POOLS,
            pool_index = idx,
            bucket = %bucket,
            object = %entry.name,
            state = "completed",
            "Decommission entry completed"
        );
        Ok(())
    }

    #[tracing::instrument(skip(self, rx))]
    async fn decommission_pool(
        self: &Arc<Self>,
        rx: CancellationToken,
        idx: usize,
        pool: Arc<Sets>,
        bi: DecomBucketInfo,
    ) -> Result<()> {
        let worker_limit = pool.disk_set.len() * 2;
        if worker_limit == 0 {
            return Err(Error::other("decommission worker limit must be greater than zero"));
        }
        let workers = Arc::new(Semaphore::new(worker_limit));
        let entry_error = Arc::new(tokio::sync::Mutex::new(None::<Error>));
        let mut listing_workers = Vec::with_capacity(pool.disk_set.len());

        let mut lifecycle_config = None;
        let mut lock_retention = None;
        let mut replication_config = None;

        if bi.name != RUSTFS_META_BUCKET {
            let _ = resolve_decommission_optional_bucket_config_result(
                &bi.name,
                "versioning",
                BucketVersioningSys::get(&bi.name).await,
            )?;
            lifecycle_config = runtime_sources::bucket_lifecycle_config(&bi.name).await;
            lock_retention = BucketObjectLockSys::get(&bi.name).await;
            replication_config = resolve_decommission_optional_bucket_config_result(
                &bi.name,
                "replication",
                metadata_sys::get_replication_config(&bi.name).await,
            )?;
        }

        for (set_idx, set) in pool.disk_set.iter().enumerate() {
            let listing_permit = workers
                .clone()
                .acquire_owned()
                .await
                .map_err(|err| Error::other(format!("decommission listing worker permit acquire failed: {err}")))?;

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

            let decommission_entry: ListCallback = Arc::new({
                let this = Arc::clone(self);
                let bucket = bi.name.clone();
                let workers = workers.clone();
                let set = set.clone();
                let lifecycle_config = lifecycle_config.clone();
                let lock_retention = lock_retention.clone();
                let replication_config = replication_config.clone();
                let entry_error = entry_error.clone();
                let callback_rx = rx.clone();
                move |entry: MetaCacheEntry| {
                    let this = this.clone();
                    let bucket = bucket.clone();
                    let workers = workers.clone();
                    let set = set.clone();
                    let lifecycle_config = lifecycle_config.clone();
                    let lock_retention = lock_retention.clone();
                    let replication_config = replication_config.clone();
                    let entry_error = entry_error.clone();
                    let callback_rx = callback_rx.clone();

                    Box::pin(async move {
                        if callback_rx.is_cancelled() {
                            return;
                        }
                        if entry_error.lock().await.is_some() {
                            return;
                        }

                        if let Err(err) =
                            backpressure::wait_for_data_movement_admission(DataMovementOperation::Decommission, idx, &callback_rx)
                                .await
                        {
                            if matches!(err, Error::OperationCanceled) {
                                return;
                            }
                            error!("decommission_pool: data movement admission failed: {err}");
                            let mut first_err = entry_error.lock().await;
                            if first_err.is_none() {
                                *first_err = Some(err);
                                callback_rx.cancel();
                            }
                            return;
                        }

                        if entry_error.lock().await.is_some() {
                            return;
                        }

                        let worker_permit = match tokio::select! {
                            _ = callback_rx.cancelled() => return,
                            permit = workers.clone().acquire_owned() => permit,
                        } {
                            Ok(permit) => permit,
                            Err(err) => {
                                let err = Error::other(format!("decommission entry worker permit acquire failed: {err}"));
                                error!("decommission_pool: decommission_entry failed: {err}");
                                let mut first_err = entry_error.lock().await;
                                if first_err.is_none() {
                                    *first_err = Some(err);
                                    callback_rx.cancel();
                                }
                                return;
                            }
                        };
                        if entry_error.lock().await.is_some() {
                            return;
                        }
                        let entry_rx = callback_rx.clone();
                        if let Err(err) = this
                            .decommission_entry(
                                entry_rx,
                                idx,
                                entry,
                                bucket,
                                set,
                                worker_permit,
                                lifecycle_config,
                                lock_retention,
                                replication_config,
                            )
                            .await
                        {
                            error!("decommission_pool: decommission_entry failed: {err}");
                            let mut first_err = entry_error.lock().await;
                            if first_err.is_none() {
                                *first_err = Some(err);
                                callback_rx.cancel();
                            }
                        }
                    })
                }
            });

            let set = set.clone();
            let rx_clone = rx.clone();
            let bi = bi.clone();
            let set_id = set_idx;
            let worker = tokio::spawn(async move {
                let _listing_permit = listing_permit;
                loop {
                    if rx_clone.is_cancelled() {
                        debug!(
                            event = EVENT_DECOMMISSION_BUCKET,
                            component = LOG_COMPONENT_ECSTORE,
                            subsystem = LOG_SUBSYSTEM_POOLS,
                            pool_index = idx,
                            set_index = set_id,
                            bucket = %bi.name,
                            state = "listing_worker_cancelled",
                            "Decommission listing worker cancelled"
                        );
                        break;
                    }
                    debug!(
                        event = EVENT_DECOMMISSION_BUCKET,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_POOLS,
                        pool_index = idx,
                        set_index = set_id,
                        bucket = %bi.name,
                        state = "listing_started",
                        "Decommission listing started"
                    );

                    match set
                        .list_objects_to_decommission(rx_clone.clone(), bi.clone(), decommission_entry.clone())
                        .await
                    {
                        Ok(_) => {
                            debug!(
                                event = EVENT_DECOMMISSION_BUCKET,
                                component = LOG_COMPONENT_ECSTORE,
                                subsystem = LOG_SUBSYSTEM_POOLS,
                                pool_index = idx,
                                set_index = set_id,
                                bucket = %bi.name,
                                state = "listing_completed",
                                "Decommission listing completed"
                            );
                            break;
                        }
                        Err(err) => {
                            error!(
                                event = EVENT_DECOMMISSION_BUCKET,
                                component = LOG_COMPONENT_ECSTORE,
                                subsystem = LOG_SUBSYSTEM_POOLS,
                                pool_index = idx,
                                set_index = set_id,
                                bucket = %bi.name,
                                state = "listing_failed",
                                error = ?err,
                                "Decommission listing failed"
                            );
                            if is_err_bucket_not_found(&err) {
                                warn!(
                                    event = EVENT_DECOMMISSION_BUCKET,
                                    component = LOG_COMPONENT_ECSTORE,
                                    subsystem = LOG_SUBSYSTEM_POOLS,
                                    pool_index = idx,
                                    set_index = set_id,
                                    bucket = %bi.name,
                                    state = "listing_bucket_missing",
                                    "Decommission listing bucket missing"
                                );
                                break;
                            }

                            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                        }
                    }
                }
            });
            listing_workers.push((set_id, worker));
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

        wait_decommission_worker_drain(&workers, worker_limit).await?;

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

    #[tracing::instrument(skip(self, rx))]
    pub async fn do_decommission_in_routine(self: &Arc<Self>, rx: CancellationToken, idx: usize) -> Result<()> {
        defer!(|| async {
            let mut cancelers = self.decommission_cancelers.write().await;
            if take_decommission_canceler(cancelers.as_mut_slice(), idx).is_none() {
                warn!(
                    event = EVENT_DECOMMISSION_STATE,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_POOLS,
                    pool_index = idx,
                    state = "canceler_already_cleared",
                    "Decommission canceler already cleared"
                );
            }
        });

        if let Err(err) = self.promote_queued_decommission(idx).await {
            resolve_decommission_terminal_mark_after_error_result(self.decommission_failed(idx).await, idx, &err)?;
            return Err(err);
        }
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
            if let Err(err) = self.decommission_cancel(idx).await {
                resolve_decommission_terminal_mark_after_error_result(self.decommission_failed(idx).await, idx, &err)?;
                return Err(err);
            }
            return Ok(());
        }
        let result = self.decommission_in_background(rx.clone(), idx).await;

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

            resolve_decommission_terminal_mark_after_error_result(self.decommission_failed(idx).await, idx, &err)?;
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
                if let Err(err) = self.check_after_decommission(idx).await {
                    resolve_decommission_terminal_mark_result(self.decommission_failed(idx).await, "failed", &cmd_line)?;
                    return Err(Error::other(format!(
                        "failed to finalize decommission for pool {cmd_line}: post-check failed: {err}"
                    )));
                }

                info!(
                    event = EVENT_DECOMMISSION_STATE,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_POOLS,
                    pool_index = idx,
                    cmd_line = %cmd_line,
                    state = "marking_completed",
                    "Decommission marking completed state"
                );
                resolve_decommission_terminal_mark_result(self.complete_decommission(idx).await, "completed", &cmd_line)?;
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
                resolve_decommission_terminal_mark_result(self.decommission_failed(idx).await, "failed", &cmd_line)?;
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
        ensure_decommission_terminal_operation_supported(self.single_pool(), "mark decommission failed")?;

        let (should_reload_pool_meta, previous_pool_meta) = {
            let mut pool_meta = self.pool_meta.write().await;
            let previous_pool_meta = pool_meta.clone();
            let changed = pool_meta.decommission_failed(idx);
            (changed, changed.then_some(previous_pool_meta))
        };

        {
            let mut cancelers = self.decommission_cancelers.write().await;
            take_and_cancel_decommission_canceler(cancelers.as_mut_slice(), idx);
        }

        if should_reload_pool_meta {
            if let Err(err) = self.save_current_pool_meta().await {
                if let Some(previous_pool_meta) = previous_pool_meta {
                    let mut pool_meta = self.pool_meta.write().await;
                    rollback_decommission_pool_meta(&mut pool_meta, previous_pool_meta);
                }
                return Err(err);
            }
            {
                let mut pool_meta = self.pool_meta.write().await;
                pool_meta.mark_decommission_progress_saved();
            }
            if let Some(notification_sys) = runtime_sources::notification_sys() {
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
        }

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub async fn complete_decommission(&self, idx: usize) -> Result<()> {
        ensure_decommission_terminal_operation_supported(self.single_pool(), "complete decommission")?;

        let (should_reload_pool_meta, previous_pool_meta) = {
            let mut pool_meta = self.pool_meta.write().await;
            let previous_pool_meta = pool_meta.clone();
            let changed = pool_meta.decommission_complete(idx);
            (changed, changed.then_some(previous_pool_meta))
        };

        {
            let mut cancelers = self.decommission_cancelers.write().await;
            take_and_cancel_decommission_canceler(cancelers.as_mut_slice(), idx);
        }

        if should_reload_pool_meta {
            if let Err(err) = self.save_current_pool_meta().await {
                if let Some(previous_pool_meta) = previous_pool_meta {
                    let mut pool_meta = self.pool_meta.write().await;
                    rollback_decommission_pool_meta(&mut pool_meta, previous_pool_meta);
                }
                return Err(err);
            }
            {
                let mut pool_meta = self.pool_meta.write().await;
                pool_meta.mark_decommission_progress_saved();
            }
            if let Some(notification_sys) = runtime_sources::notification_sys() {
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
        }

        Ok(())
    }

    async fn decommission_pending_bucket(
        self: &Arc<Self>,
        rx: CancellationToken,
        idx: usize,
        pool: Arc<Sets>,
        bucket: DecomBucketInfo,
    ) -> Result<()> {
        let is_decommissioned = {
            let pool_meta = self.pool_meta.read().await;
            resolve_decommission_bucket_state(&pool_meta, idx, &bucket)?
        };

        if is_decommissioned {
            warn!("decommission: already done, moving on {}", bucket.to_string());

            let bucket_done = {
                let mut pool_meta = self.pool_meta.write().await;
                mark_decommission_bucket_done(&mut pool_meta, idx, &bucket)?
            };
            if bucket_done {
                resolve_decommission_bucket_done_save_result(self.save_current_pool_meta().await, idx, bucket.name.as_str())?;
                {
                    let mut pool_meta = self.pool_meta.write().await;
                    pool_meta.mark_decommission_progress_saved();
                }
            }
            return Ok(());
        }

        warn!("decommission: currently on bucket {}", &bucket.name);

        if let Err(err) = self.decommission_pool(rx.clone(), idx, pool, bucket.clone()).await {
            error!("decommission: decommission_pool err {:?}", &err);
            return Err(err);
        } else {
            warn!("decommission: decommission_pool done {}", &bucket.name);
        }

        if let Err(err) = decommission_cancel_signal_result(rx.is_cancelled()) {
            warn!("decommission: cancellation observed after decommission_pool {}", &bucket.name);
            return Err(err);
        }

        let bucket_done = {
            let mut pool_meta = self.pool_meta.write().await;
            mark_decommission_bucket_done(&mut pool_meta, idx, &bucket)?
        };
        if bucket_done {
            resolve_decommission_bucket_done_save_result(self.save_current_pool_meta().await, idx, bucket.name.as_str())?;
            let mut pool_meta = self.pool_meta.write().await;
            pool_meta.mark_decommission_progress_saved();
        }

        warn!("decommission: decommission_pool bucket_done {}", &bucket.name);

        Ok(())
    }

    async fn decommission_buckets_concurrently(
        self: &Arc<Self>,
        rx: CancellationToken,
        idx: usize,
        pool: Arc<Sets>,
        buckets: Vec<DecomBucketInfo>,
        limit: usize,
    ) -> Result<()> {
        let store = Arc::clone(self);
        run_decommission_buckets_bounded(rx, buckets, limit, move |bucket, rx| {
            let store = Arc::clone(&store);
            let pool = pool.clone();
            Box::pin(async move { store.decommission_pending_bucket(rx, idx, pool, bucket).await })
        })
        .await
    }

    #[tracing::instrument(skip(self, rx))]
    async fn decommission_in_background(self: &Arc<Self>, rx: CancellationToken, idx: usize) -> Result<()> {
        let pool = get_by_index(self.pools.as_slice(), idx, "load decommission background pool")?.clone();

        let pending = {
            let pool_meta = self.pool_meta.read().await;
            pool_meta.pending_buckets(idx)
        };

        let bucket_concurrency = decommission_bucket_concurrency_limit();
        if bucket_concurrency <= 1 {
            for bucket in pending {
                self.decommission_pending_bucket(rx.clone(), idx, pool.clone(), bucket)
                    .await?;
            }
            return Ok(());
        }

        let (regular_buckets, meta_buckets) = split_decommission_buckets(pending);
        self.decommission_buckets_concurrently(rx.clone(), idx, pool.clone(), regular_buckets, bucket_concurrency)
            .await?;

        for bucket in meta_buckets {
            self.decommission_pending_bucket(rx.clone(), idx, pool.clone(), bucket)
                .await?;
        }

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub async fn start_decommission(&self, indices: Vec<usize>) -> Result<()> {
        let indices = dedup_indices(&indices);
        validate_start_decommission_request(&indices, self.single_pool())?;

        self.ensure_decommission_rebalance_idle_after_refresh().await?;
        ensure_decommission_start_local_leader(&self.endpoints(), &indices)?;

        for idx in indices.iter().copied() {
            ensure_valid_decommission_pool_index(self.pools.len(), idx)?;
        }

        {
            let pool_meta = self.pool_meta.read().await;
            ensure_decommission_start_pool_states(&pool_meta, &indices)?;
        }

        let decom_buckets = self.get_buckets_to_decommission().await?;

        for bk in decom_buckets.iter() {
            resolve_decommission_preflight_heal_result(&bk.name, self.heal_bucket(&bk.name, &HealOpts::default()).await)?;
        }

        let meta_buckets = [
            path_join(&[PathBuf::from(RUSTFS_META_BUCKET), PathBuf::from(CONFIG_PREFIX)]),
            path_join(&[PathBuf::from(RUSTFS_META_BUCKET), PathBuf::from(BUCKET_META_PREFIX)]),
        ];

        let meta_bucket_opts = decommission_meta_bucket_options();
        for bk in meta_buckets.iter() {
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
        self.ensure_decommission_rebalance_idle_after_refresh().await?;

        let all_space_infos = self.get_decommission_all_pool_space_infos().await?;
        {
            let pool_meta = self.pool_meta.read().await;
            ensure_decommission_start_pool_states(&pool_meta, &indices)?;
            ensure_decommission_start_target_capacity(&pool_meta, &indices, &all_space_infos)?;
        }

        let mut space_infos = Vec::with_capacity(indices.len());
        for (idx, pi) in all_space_infos.iter().copied() {
            if indices.contains(&idx) {
                space_infos.push((idx, pi));
            }
        }

        let previous_pool_meta = self
            .save_current_pool_meta_for_decommission_start(&indices, space_infos, decom_buckets)
            .await?;

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

            {
                let mut pool_meta = self.pool_meta.write().await;
                rollback_start_decommission_pool_meta(&mut pool_meta, previous_pool_meta.clone());
            }
            if let Err(rollback_save_err) = self.save_current_pool_meta().await {
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

        Ok(())
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

        ret.push(DecomBucketInfo {
            name: RUSTFS_META_BUCKET.to_owned(),
            prefix: CONFIG_PREFIX.to_owned(),
        });
        ret.push(DecomBucketInfo {
            name: RUSTFS_META_BUCKET.to_owned(),
            prefix: BUCKET_META_PREFIX.to_owned(),
        });

        Ok(ret)
    }

    async fn check_after_decommission(self: &Arc<Self>, idx: usize) -> Result<()> {
        let buckets = self.get_buckets_to_decommission().await?;
        let pool = self.pools[idx].clone();

        for set in &pool.disk_set {
            for bucket_info in &buckets {
                let mut lifecycle_config = None;
                let mut lock_retention = None;
                if bucket_info.name != RUSTFS_META_BUCKET {
                    lifecycle_config = runtime_sources::bucket_lifecycle_config(&bucket_info.name).await;
                    lock_retention = BucketObjectLockSys::get(&bucket_info.name).await;
                }

                let versions_found = Arc::new(AtomicUsize::new(0));
                let entry_error = Arc::new(tokio::sync::Mutex::new(None::<Error>));
                let callback_rx = CancellationToken::new();
                let versions_found_cb = versions_found.clone();
                let entry_error_cb = entry_error.clone();
                let bucket_name = bucket_info.name.clone();
                let lifecycle_config_cb = lifecycle_config.clone();
                let lock_retention_cb = lock_retention.clone();
                let store = Arc::clone(self);
                let callback_rx_cb = callback_rx.clone();

                let callback: ListCallback = Arc::new(move |entry: MetaCacheEntry| {
                    let versions_found = versions_found_cb.clone();
                    let entry_error = entry_error_cb.clone();
                    let bucket_name = bucket_name.clone();
                    let lifecycle_config = lifecycle_config_cb.clone();
                    let lock_retention = lock_retention_cb.clone();
                    let store = Arc::clone(&store);
                    let callback_rx = callback_rx_cb.clone();
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

                        let fivs = match load_decommission_entry_versions(
                            &entry,
                            &bucket_name,
                            "check_after_decommission.file_info_versions",
                        ) {
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

                        let mut remaining = 0;
                        for version in &fivs.versions {
                            if version.deleted {
                                continue;
                            }
                            let skip_lifecycle = match should_skip_lifecycle_for_data_movement(
                                Arc::clone(&store),
                                &bucket_name,
                                version,
                                lifecycle_config.as_ref(),
                                lock_retention.clone(),
                                false,
                                &LcEventSrc::Decom,
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
                                continue;
                            }
                            remaining += 1;
                        }

                        versions_found.fetch_add(remaining, Ordering::Relaxed);
                    })
                });

                let list_result = set
                    .list_objects_to_decommission(callback_rx, bucket_info.clone(), callback)
                    .await;
                let entry_error = entry_error.lock().await.clone();
                resolve_decommission_check_after_list_result(list_result, entry_error)?;

                let versions_found = versions_found.load(Ordering::Relaxed);
                if versions_found > 0 {
                    return Err(Error::other(format!(
                        "at least {versions_found} object(s)/version(s) were found in bucket `{}` after decommissioning",
                        bucket_info.name
                    )));
                }
            }
        }

        Ok(())
    }

    #[tracing::instrument(skip(self, rd))]
    async fn decommission_object(self: Arc<Self>, pool_idx: usize, bucket: String, rd: GetObjectReader) -> Result<()> {
        warn!("decommission_object: start {} {}", &bucket, &rd.object_info.name);
        let object_name = rd.object_info.name.clone();
        let result = data_movement::migrate_object(self, pool_idx, bucket.clone(), rd, "decommission_object").await;
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
    fn decommission_remaining_version_count_excludes_only_expired_versions() {
        assert_eq!(decommission_remaining_version_count(1, 0), 1);
        assert_eq!(decommission_remaining_version_count(2, 1), 1);
        assert_eq!(decommission_remaining_version_count(1, 1), 0);
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

        let opts = decommission_delete_marker_opts(&version, Some("version-id".to_string()), 7);
        let replication = opts.delete_replication.expect("replication state should be preserved");

        assert!(opts.versioned);
        assert!(opts.data_movement);
        assert!(opts.delete_marker);
        assert!(opts.skip_decommissioned);
        assert_eq!(opts.src_pool_idx, 7);
        assert_eq!(opts.version_id.as_deref(), Some("version-id"));
        assert_eq!(opts.mod_time, Some(mod_time));
        assert_eq!(replication.replica_status, ReplicationStatusType::Replica);
        assert!(replication.delete_marker);
        assert_eq!(replication.replicate_decision_str, "existing");
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
            metadata: std::collections::HashMap::from([("x-amz-meta-key".to_string(), "value".to_string())]),
            ..Default::default()
        };

        let opts = decommission_remote_tiered_opts(&version, Some("version-id".to_string()), 9);

        assert!(opts.versioned);
        assert!(opts.data_movement);
        assert_eq!(opts.src_pool_idx, 9);
        assert_eq!(opts.version_id.as_deref(), Some("version-id"));
        assert_eq!(opts.mod_time, Some(mod_time));
        assert_eq!(opts.user_defined.get("x-amz-meta-key").map(String::as_str), Some("value"));
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
    }

    #[test]
    fn pool_meta_persists_decommission_resume_queues() {
        let start_time = OffsetDateTime::now_utc();
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
            version: POOL_META_VERSION,
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

        let decoded = PoolMeta::decode_pool_meta_payload(&legacy_payload).expect("legacy payload should decode");
        assert_eq!(decoded.version, POOL_META_VERSION);
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
            version: POOL_META_VERSION,
            pools: Vec::new(),
            dont_save: true,
            unexpected: true,
        })
        .expect("legacy pool metadata with unknown field should serialize");

        let err = PoolMeta::decode_pool_meta_payload(payload.as_slice())
            .expect_err("unknown legacy pool metadata field should fail decode");
        let rendered = err.to_string();
        assert!(rendered.contains("PoolMeta decode failed for both persisted and legacy formats"));
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

        let err = PoolMeta::decode_pool_meta_payload(payload.as_slice())
            .expect_err("unknown persisted pool metadata field should fail decode");
        let rendered = err.to_string();
        assert!(rendered.contains("PoolMeta decode failed for both persisted and legacy formats"));
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

        let err = PoolMeta::decode_pool_meta_payload(payload.as_slice())
            .expect_err("missing persisted pool metadata pools should fail decode");
        assert!(
            err.to_string()
                .contains("PoolMeta decode failed for both persisted and legacy formats")
        );
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

        let err = PoolMeta::decode_pool_meta_payload(payload.as_slice())
            .expect_err("unknown persisted decommission metadata field should fail decode");
        assert!(
            err.to_string()
                .contains("PoolMeta decode failed for both persisted and legacy formats")
        );
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

        let err = PoolMeta::decode_pool_meta_payload(&payload).expect_err("invalid terminal state should fail decode");
        assert!(err.to_string().contains("invalid decommission terminal state"));
    }

    #[test]
    fn pool_meta_decode_rejects_invalid_legacy_decommission_terminal_state() {
        let start_time = OffsetDateTime::now_utc();
        let legacy_meta = LegacyPoolMeta {
            version: POOL_META_VERSION,
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

        let err = PoolMeta::decode_pool_meta_payload(&payload).expect_err("invalid legacy terminal state should fail decode");
        assert!(err.to_string().contains("invalid decommission terminal state"));
    }
}

// impl Fn(MetaCacheEntry) -> impl Future<Output = Result<(), Error>>

pub type ListCallback = Arc<dyn Fn(MetaCacheEntry) -> BoxFuture<'static, ()> + Send + Sync + 'static>;

impl SetDisks {
    #[tracing::instrument(skip(self, rx, cb_func))]
    async fn list_objects_to_decommission(
        self: &Arc<Self>,
        rx: CancellationToken,
        bucket_info: DecomBucketInfo,
        cb_func: ListCallback,
    ) -> Result<()> {
        let (disks, _) = self.get_online_disks_with_healing(false).await;
        ensure_decommission_listing_disks_available(!disks.is_empty(), &bucket_info.name)?;

        let listing_quorum = self.set_drive_count.div_ceil(2);

        let resolver = MetadataResolutionParams {
            dir_quorum: listing_quorum,
            obj_quorum: listing_quorum,
            bucket: bucket_info.name.clone(),
            ..Default::default()
        };

        let cb1 = cb_func.clone();

        list_path_raw(
            rx,
            ListPathRawOptions {
                disks: disks.iter().cloned().map(Some).collect(),
                bucket: bucket_info.name.clone(),
                path: bucket_info.prefix.clone(),
                recursive: true,
                min_disks: listing_quorum,
                agreed: Some(Box::new(move |entry: MetaCacheEntry| Box::pin(cb1(entry)))),
                partial: Some(Box::new(move |entries: MetaCacheEntries, _: &[Option<DiskError>]| {
                    let resolver = resolver.clone();
                    let cb_func = cb_func.clone();
                    match entries.resolve(resolver) {
                        Some(entry) => {
                            warn!("decommission_pool: list_objects_to_decommission get {}", &entry.name);
                            Box::pin(async move {
                                cb_func(entry).await;
                            })
                        }
                        None => {
                            warn!("decommission_pool: list_objects_to_decommission get none");
                            Box::pin(async {})
                        }
                    }
                })),
                ..Default::default()
            },
        )
        .await?;

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
#[allow(dead_code)]
fn fallback_total_capacity(disks: &[rustfs_madmin::Disk]) -> usize {
    fallback_total_capacity_dedup(disks)
}

#[deprecated(since = "0.1.0", note = "Use fallback_free_capacity_dedup instead")]
#[allow(dead_code)]
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
    use super::{
        DECOMMISSION_PROGRESS_SAVE_INTERVAL, DECOMMISSION_PROGRESS_SAVE_ITEM_THRESHOLD, DecomBucketInfo,
        DecommissionStartPoolState, DecommissionTerminalState, PoolDecommissionInfo, PoolMeta, PoolSpaceInfo, PoolStatus,
        apply_decommission_status_space_info, bind_decommission_cancelers, bind_missing_decommission_cancelers,
        cancel_decommission_canceler, classify_decommission_terminal_state, count_decommission_item,
        decommission_cancel_signal_result, decommission_item_size, decommission_meta_bucket_options,
        decommission_start_pool_state, dedup_indices, default_decommission_bucket_concurrency,
        ensure_decommission_cancel_allowed, ensure_decommission_clear_allowed, ensure_decommission_listing_disks_available,
        ensure_decommission_not_rebalancing, ensure_decommission_start_allowed, ensure_decommission_start_keeps_active_pool,
        ensure_decommission_start_local_leader, ensure_decommission_start_pool_states,
        ensure_decommission_start_rebalance_meta_allowed, ensure_decommission_start_target_capacity,
        ensure_decommission_terminal_operation_supported, ensure_local_decommission_pool_leaders,
        ensure_valid_decommission_pool_index, first_resumable_decommission_queue_indices, get_by_index,
        has_active_decommission_canceler, is_decommission_active, is_decommission_cancel_requested,
        load_decommission_entry_versions, local_decommission_queue_prefix, mark_decommission_bucket_done,
        merge_pool_status_refresh, missing_decommission_worker_prefix, observe_decommission_terminal_reload_result,
        pool_meta_has_active_decommission, require_decommission_store, resolve_decommission_bucket_done_save_result,
        resolve_decommission_bucket_state, resolve_decommission_check_after_list_result,
        resolve_decommission_entry_cleanup_delete_result, resolve_decommission_entry_exact_versions,
        resolve_decommission_entry_reload_result, resolve_decommission_listing_worker_result,
        resolve_decommission_optional_bucket_config_result, resolve_decommission_pool_meta_reload_result,
        resolve_decommission_preflight_heal_result, resolve_decommission_progress_save_result,
        resolve_decommission_spawn_failure_result, resolve_decommission_terminal_mark_after_error_result,
        resolve_decommission_terminal_mark_result, resolve_decommission_update_after_result,
        resolve_start_decommission_pool_meta_reload_result, rollback_start_decommission_pool_meta,
        run_decommission_buckets_bounded, should_cleanup_decommission_source_entry, should_continue_decommission_queue,
        should_count_decommission_version_complete, should_preserve_decommission_canceled_state,
        should_reject_decommission_cancel_as_terminal, should_retry_decommission_cancel_reload,
        should_skip_canceled_decommission_routine, split_decommission_buckets, take_and_cancel_decommission_canceler,
        take_decommission_canceler, touch_decommission_progress, track_decommission_current_object,
        track_decommission_current_object_stage, validate_start_decommission_request, wait_decommission_worker_drain,
        with_decommission_entry_context,
    };
    use crate::data_movement;
    use crate::disk::endpoint::Endpoint;
    use crate::error::Error;
    use crate::layout::endpoints::{EndpointServerPools, Endpoints, PoolEndpoints};
    use crate::services::rebalance::{RebalStatus, RebalanceInfo, RebalanceMeta, RebalanceStats};
    use rustfs_filemeta::{FileInfo, FileInfoVersions, MetaCacheEntry, ObjectPartInfo};
    use rustfs_rio::Index;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };
    use std::time::Duration as StdDuration;
    use time::{Duration, OffsetDateTime};
    use tokio::sync::Semaphore;
    use tokio_util::sync::CancellationToken;

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

        merge_pool_status_refresh(&mut current, persisted, &[false]);

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

        merge_pool_status_refresh(&mut current, persisted, &[true]);

        let info = current.pools[0]
            .decommission
            .as_ref()
            .expect("local decommission info should remain present");
        assert_eq!(info.items_decommissioned, 10);
        assert_eq!(info.bytes_done, 1_024);
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

        merge_pool_status_refresh(&mut current, persisted, &[true]);

        let info = current.pools[0]
            .decommission
            .as_ref()
            .expect("local active decommission info should remain present");
        assert!(!info.failed);
        assert_eq!(info.items_decommissioned, 10);
        assert_eq!(info.bytes_done, 1_024);
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
        ]);

        assert_eq!(
            regular.iter().map(|bucket| bucket.name.as_str()).collect::<Vec<_>>(),
            vec!["bucket-a", "bucket-b",]
        );
        assert_eq!(
            meta.iter().map(|bucket| bucket.prefix.as_str()).collect::<Vec<_>>(),
            vec![crate::config::com::CONFIG_PREFIX, crate::disk::BUCKET_META_PREFIX,]
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
        active.pools[0].decommission = Some(PoolDecommissionInfo {
            start_time: Some(OffsetDateTime::UNIX_EPOCH),
            ..Default::default()
        });

        assert!(active.is_suspended(0));
        assert_eq!(
            decommission_start_pool_state(active.pools.first()),
            DecommissionStartPoolState::Decommissioning
        );

        rollback_start_decommission_pool_meta(&mut active, previous);

        assert!(!active.is_suspended(0));
        assert_eq!(decommission_start_pool_state(active.pools.first()), DecommissionStartPoolState::Active);
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
    fn test_resolve_decommission_bucket_done_save_result_passthrough_ok() {
        assert!(resolve_decommission_bucket_done_save_result(Ok(()), 1, "bucket-a").is_ok());
    }

    #[test]
    fn test_resolve_decommission_bucket_done_save_result_wraps_error_context() {
        let err = resolve_decommission_bucket_done_save_result(Err(Error::SlowDown), 2, "bucket-a")
            .expect_err("metadata save failure should carry pool/bucket context");
        assert!(
            err.to_string()
                .contains("decommission metadata save failed for pool 2 bucket bucket-a")
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
    fn test_resolve_decommission_spawn_failure_result_keeps_primary_without_rollback_error() {
        let err = resolve_decommission_spawn_failure_result(Error::SlowDown, None);
        assert!(matches!(err, Error::SlowDown));
    }

    #[test]
    fn test_resolve_decommission_spawn_failure_result_wraps_rollback_error() {
        let err = resolve_decommission_spawn_failure_result(Error::SlowDown, Some(Error::OperationCanceled));
        let message = err.to_string();
        assert!(message.contains("decommission spawn routines failed"));
        assert!(message.contains("rollback failed"));
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
        assert!(resolve_decommission_listing_worker_result(2, Ok(())).is_ok());
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
    fn test_touch_decommission_progress_updates_last_update_and_save_baseline() {
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

        touch_decommission_progress(&mut meta, 0).expect("valid decommission progress should be touched");

        assert!(meta.pools[0].last_update > OffsetDateTime::UNIX_EPOCH);
        let info = meta.pools[0].decommission.as_ref().expect("decommission info should exist");
        assert_eq!(info.items_since_last_progress_save(), 0);
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
        assert!(ensure_decommission_clear_allowed(true, true, false, true, false).is_ok());
        assert!(ensure_decommission_clear_allowed(true, true, false, false, true).is_ok());
    }

    #[test]
    fn test_ensure_decommission_clear_allowed_rejects_active_or_completed() {
        let active = ensure_decommission_clear_allowed(true, true, false, false, false)
            .expect_err("active decommission should not be clearable");
        assert!(matches!(active, Error::DecommissionAlreadyRunning));

        let complete = ensure_decommission_clear_allowed(true, true, true, false, false)
            .expect_err("completed decommission should not be clearable");
        assert!(matches!(complete, Error::DecommissionNotStarted));
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
        let existing = CancellationToken::new();
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
        let existing = CancellationToken::new();
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
        let cancelers = vec![None, Some(CancellationToken::new()), None];

        let missing = missing_decommission_worker_prefix(&[0, 1, 2], cancelers.as_slice());

        assert_eq!(missing, vec![0]);
    }

    #[test]
    fn test_first_resumable_decommission_queue_indices_stops_at_failed_or_canceled_state() {
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

        assert!(first_resumable_decommission_queue_indices(&meta).is_empty());
    }

    #[test]
    fn test_first_resumable_decommission_queue_indices_allows_after_completed_prefix() {
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
                        queued: true,
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

        assert_eq!(first_resumable_decommission_queue_indices(&meta), vec![1, 2]);
    }

    #[test]
    fn test_return_resumable_pools_skips_failed_decommission() {
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
                        queued: true,
                        ..Default::default()
                    }),
                ),
            ],
            ..Default::default()
        };

        let resumable = meta.return_resumable_pools();

        assert_eq!(resumable.len(), 1);
        assert_eq!(resumable[0].id, 1);
    }

    #[test]
    fn test_take_decommission_canceler_takes_and_clears_slot() {
        let token = CancellationToken::new();
        let mut cancelers = vec![Some(token)];

        let taken = take_decommission_canceler(cancelers.as_mut_slice(), 0);
        assert!(taken.is_some());
        assert!(cancelers[0].is_none());
    }

    #[test]
    fn test_take_decommission_canceler_returns_none_for_missing_slot() {
        let mut cancelers: Vec<Option<CancellationToken>> = Vec::new();
        assert!(take_decommission_canceler(cancelers.as_mut_slice(), 0).is_none());
    }

    #[test]
    fn test_has_active_decommission_canceler_true_when_any_slot_present() {
        let cancelers = vec![None, Some(CancellationToken::new())];
        assert!(has_active_decommission_canceler(cancelers.as_slice()));
    }

    #[test]
    fn test_has_active_decommission_canceler_false_when_all_empty() {
        let cancelers = vec![None, None];
        assert!(!has_active_decommission_canceler(cancelers.as_slice()));
    }

    #[test]
    fn test_cancel_decommission_canceler_cancels_when_present() {
        let token = CancellationToken::new();
        let canceled = cancel_decommission_canceler(Some(token.clone()));

        assert!(canceled);
        assert!(token.is_cancelled());
    }

    #[test]
    fn test_cancel_decommission_canceler_returns_false_when_missing() {
        assert!(!cancel_decommission_canceler(None));
    }

    #[test]
    fn test_take_and_cancel_decommission_canceler_clears_slot() {
        let token = CancellationToken::new();
        let mut cancelers = vec![Some(token.clone())];

        assert!(take_and_cancel_decommission_canceler(cancelers.as_mut_slice(), 0));
        assert!(cancelers[0].is_none());
        assert!(token.is_cancelled());
    }

    #[test]
    fn test_take_and_cancel_decommission_canceler_missing_slot_is_false() {
        let mut cancelers = vec![None];

        assert!(!take_and_cancel_decommission_canceler(cancelers.as_mut_slice(), 0));
        assert!(cancelers[0].is_none());
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
