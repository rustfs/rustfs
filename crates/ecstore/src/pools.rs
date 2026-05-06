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

use crate::bucket::versioning_sys::BucketVersioningSys;
use crate::bucket::{
    lifecycle::{
        bucket_lifecycle_audit::LcEventSrc,
        bucket_lifecycle_ops::{
            LifecycleOps, apply_expiry_on_transitioned_object, apply_expiry_rule, eval_action_from_lifecycle,
        },
        lifecycle::IlmAction,
    },
    metadata_sys,
    object_lock::objectlock_sys::BucketObjectLockSys,
};
use crate::cache_value::metacache_set::{ListPathRawOptions, list_path_raw};
use crate::config::com::{CONFIG_PREFIX, read_config, save_config};
use crate::data_movement;
use crate::data_usage::DATA_USAGE_CACHE_NAME;
use crate::disk::error::DiskError;
use crate::disk::{BUCKET_META_PREFIX, RUSTFS_META_BUCKET};
use crate::error::{Error, Result};
use crate::error::{
    StorageError, is_err_bucket_exists, is_err_bucket_not_found, is_err_data_movement_overwrite, is_err_object_not_found,
    is_err_operation_canceled, is_err_version_not_found,
};
use crate::new_object_layer_fn;
use crate::notification_sys::get_global_notification_sys;
use crate::set_disk::SetDisks;
use crate::store_api::{
    BucketOperations, BucketOptions, GetObjectReader, HealOperations, MakeBucketOptions, ObjectIO, ObjectOperations,
    ObjectOptions, StorageAPI,
};
use crate::{global::GLOBAL_LifecycleSys, sets::Sets, store::ECStore};
use byteorder::{ByteOrder, LittleEndian, WriteBytesExt};
use futures::future::BoxFuture;
use http::HeaderMap;
#[cfg(test)]
use rmp_serde::Deserializer;
use rmp_serde::Serializer;
use rustfs_common::defer;
use rustfs_common::heal_channel::HealOpts;
use rustfs_filemeta::{FileInfoVersions, MetaCacheEntries, MetaCacheEntry, MetadataResolutionParams};
use rustfs_utils::path::{encode_dir_object, path_join, path_to_bucket_object, path_to_bucket_object_with_base_path};
use rustfs_workers::workers::Workers;
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
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

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

fn ensure_decommission_routines_scheduled(bound_count: usize, expected_count: usize) -> Result<()> {
    if bound_count == 0 || bound_count != expected_count {
        return Err(Error::other(format!(
            "failed to start decommission routines: scheduled {bound_count} of {expected_count} expected workers"
        )));
    }

    Ok(())
}

fn ensure_decommission_not_rebalancing(rebalance_running: bool) -> Result<()> {
    if rebalance_running {
        return Err(Error::RebalanceAlreadyRunning);
    }

    Ok(())
}

fn is_decommission_active(complete: bool, failed: bool, canceled: bool) -> bool {
    !complete && !failed && !canceled
}

fn invalid_decommission_pool_index_error(pool_count: usize, idx: usize) -> Error {
    Error::other(format!("invalid decommission pool index {idx} for {pool_count} pools"))
}

fn ensure_decommission_start_allowed(pool_present: bool, decommission_active: bool) -> Result<()> {
    if !pool_present {
        return Err(Error::other("failed to start decommission: target pool was not found"));
    }

    if decommission_active {
        return Err(StorageError::DecommissionAlreadyRunning);
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

fn track_decommission_current_object(meta: &mut PoolMeta, idx: usize, bucket: &str, object: &str) -> Result<()> {
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
    Ok(())
}

fn resolve_decommission_update_after_result(result: Result<bool>) -> Result<bool> {
    result.map_err(|err| Error::other(format!("decommission metadata update failed: {err}")))
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

fn resolve_decommission_check_after_list_result(list_result: Result<()>, entry_error: Option<Error>) -> Result<()> {
    if let Some(err) = entry_error { Err(err) } else { list_result }
}

fn resolve_decommission_pool_meta_reload_result(result: Result<()>, stage: &str) -> Result<()> {
    result.map_err(|err| Error::other(format!("decommission pool meta reload failed during {stage}: {err}")))
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

fn should_cleanup_decommission_source_entry(decommissioned: usize, total_versions: usize, expired: usize) -> bool {
    expired == 0 && decommissioned == total_versions
}

fn decommission_start_guard_state(pool: Option<&PoolStatus>) -> (bool, bool) {
    if let Some(pool) = pool {
        let active = pool
            .decommission
            .as_ref()
            .is_some_and(|info| is_decommission_active(info.complete, info.failed, info.canceled));
        (true, active)
    } else {
        (false, false)
    }
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

fn should_preserve_decommission_canceled_state(meta_canceled: bool, cancel_signal: bool) -> bool {
    meta_canceled || cancel_signal
}

fn decommission_cancel_signal_result(cancel_signal: bool) -> Result<()> {
    if cancel_signal {
        Err(StorageError::OperationCanceled)
    } else {
        Ok(())
    }
}

fn is_decommission_cancel_terminal(complete: bool, failed: bool, canceled: bool) -> bool {
    complete || failed || canceled
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
struct PersistedPoolMeta {
    pub version: u16,
    pub pools: Vec<PersistedPoolStatus>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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
}

impl From<PersistedPoolMeta> for PoolMeta {
    fn from(value: PersistedPoolMeta) -> Self {
        Self {
            version: value.version,
            pools: value.pools.into_iter().map(Into::into).collect(),
            dont_save: false,
        }
    }
}

impl From<PersistedPoolStatus> for PoolStatus {
    fn from(value: PersistedPoolStatus) -> Self {
        Self {
            id: value.id,
            cmd_line: value.cmd_line,
            last_update: value.last_update,
            decommission: value.decommission.map(Into::into),
        }
    }
}

impl From<PersistedPoolDecommissionInfo> for PoolDecommissionInfo {
    fn from(value: PersistedPoolDecommissionInfo) -> Self {
        Self {
            start_time: value.start_time,
            start_size: value.start_size,
            total_size: value.total_size,
            current_size: value.current_size,
            complete: value.complete,
            failed: value.failed,
            canceled: value.canceled,
            queued_buckets: value.queued_buckets,
            decommissioned_buckets: value.decommissioned_buckets,
            bucket: value.bucket,
            prefix: value.prefix,
            object: value.object,
            items_decommissioned: value.items_decommissioned,
            items_decommission_failed: value.items_decommission_failed,
            bytes_done: value.bytes_done,
            bytes_failed: value.bytes_failed,
        }
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
            queued_buckets: value.queued_buckets.clone(),
            decommissioned_buckets: value.decommissioned_buckets.clone(),
            bucket: value.bucket.clone(),
            prefix: value.prefix.clone(),
            object: value.object.clone(),
            items_decommissioned: value.items_decommissioned,
            items_decommission_failed: value.items_decommission_failed,
            bytes_done: value.bytes_done,
            bytes_failed: value.bytes_failed,
        }
    }
}

impl PoolMeta {
    fn decode_pool_meta_payload(payload: &[u8]) -> Result<Self> {
        match rmp_serde::from_slice::<PersistedPoolMeta>(payload) {
            Ok(meta) => Ok(meta.into()),
            Err(persisted_err) => {
                let mut legacy: PoolMeta = rmp_serde::from_slice(payload).map_err(|legacy_err| {
                    Error::other(format!(
                        "PoolMeta decode failed for both persisted and legacy formats: persisted={persisted_err}; legacy={legacy_err}"
                    ))
                })?;
                // Runtime-only flag must not be restored from on-disk payload.
                legacy.dont_save = false;
                Ok(legacy)
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
        self.pools.get(idx).is_some_and(|pool| pool.decommission.is_some())
    }

    pub async fn load(&mut self, pool: Arc<Sets>, _pools: Vec<Arc<Sets>>) -> Result<()> {
        let data = match read_config(pool, POOL_META_NAME).await {
            Ok(data) => {
                if data.is_empty() {
                    return Ok(());
                } else if data.len() <= 4 {
                    return Err(Error::other("pool metadata load failed: metadata payload is too short"));
                }
                data
            }
            Err(err) => {
                if err == Error::ConfigNotFound {
                    return Ok(());
                }
                return Err(err);
            }
        };
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

    pub async fn save(&self, pools: Vec<Arc<Sets>>) -> Result<()> {
        if self.dont_save {
            return Ok(());
        }
        let mut data = Vec::new();
        data.write_u16::<LittleEndian>(POOL_META_FORMAT)?;
        data.write_u16::<LittleEndian>(POOL_META_VERSION)?;
        let mut buf = Vec::new();
        PersistedPoolMeta::from(self).serialize(&mut Serializer::new(&mut buf))?;
        data.write_all(&buf)?;

        for pool in pools {
            save_config(pool, POOL_META_NAME, data.clone()).await?;
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
    pub fn decommission_complete(&mut self, idx: usize) -> bool {
        if let Some(stats) = self.pools.get_mut(idx) {
            if let Some(d) = &stats.decommission {
                if !d.complete {
                    stats.last_update = OffsetDateTime::now_utc();

                    let mut pd = d.clone();
                    pd.canceled = false;
                    pd.failed = false;
                    pd.complete = true;

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
    pub fn decommission(&mut self, idx: usize, pi: PoolSpaceInfo) -> Result<()> {
        let pool_count = self.pools.len();
        ensure_valid_decommission_pool_index(pool_count, idx)?;

        let Some(pool) = self.pools.get_mut(idx) else {
            return Err(invalid_decommission_pool_index_error(pool_count, idx));
        };

        let decommission_active = pool
            .decommission
            .as_ref()
            .is_some_and(|info| is_decommission_active(info.complete, info.failed, info.canceled));
        ensure_decommission_start_allowed(true, decommission_active)?;

        let now = OffsetDateTime::now_utc();
        pool.last_update = now;
        pool.decommission = Some(PoolDecommissionInfo {
            start_time: Some(now),
            start_size: pi.free,
            total_size: pi.total,
            current_size: pi.free,
            ..Default::default()
        });

        Ok(())
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
        if self.pools.get(idx).is_none_or(|v| v.decommission.is_none()) {
            return;
        }

        if let Some(pool) = self.pools.get_mut(idx)
            && let Some(info) = pool.decommission.as_mut()
        {
            info.object = object;
            info.bucket = bucket;
        }
    }

    pub async fn update_after(&mut self, idx: usize, pools: Vec<Arc<Sets>>, duration: Duration) -> Result<bool> {
        let pool_count = self.pools.len();
        ensure_valid_decommission_pool_index(pool_count, idx)?;

        let last_update = match self.pools.get(idx) {
            Some(pool) if pool.decommission.is_some() => pool.last_update,
            Some(_) => {
                return Err(decommission_metadata_not_initialized_error("update decommission metadata timestamp"));
            }
            None => return Err(invalid_decommission_pool_index_error(pool_count, idx)),
        };
        let now = OffsetDateTime::now_utc();

        if now.unix_timestamp() - last_update.unix_timestamp() > duration.whole_seconds() {
            let Some(pool) = self.pools.get_mut(idx) else {
                return Err(invalid_decommission_pool_index_error(pool_count, idx));
            };
            pool.last_update = now;
            self.save(pools).await?;

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
                if decommission.complete || decommission.canceled {
                    // Recovery is not required when:
                    // - Decommissioning completed
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
    pub queued_buckets: Vec<String>,
    #[serde(skip)]
    pub decommissioned_buckets: Vec<String>,
    #[serde(skip)]
    pub bucket: String,
    #[serde(skip)]
    pub prefix: String,
    #[serde(skip)]
    pub object: String,

    #[serde(rename = "objectsDecommissioned")]
    pub items_decommissioned: usize,
    #[serde(rename = "objectsDecommissionedFailed")]
    pub items_decommission_failed: usize,
    #[serde(rename = "bytesDecommissioned")]
    pub bytes_done: usize,
    #[serde(rename = "bytesDecommissionedFailed")]
    pub bytes_failed: usize,
}

impl PoolDecommissionInfo {
    pub fn bucket_push(&mut self, bucket: &DecomBucketInfo) {
        for b in self.queued_buckets.iter() {
            if self.is_bucket_decommissioned(b) {
                return;
            }

            if b == &bucket.to_string() {
                return;
            }
        }

        self.queued_buckets.push(bucket.to_string());

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

#[derive(Debug)]
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
        delete_replication: version.replication_state_internal.clone(),
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

#[allow(clippy::too_many_arguments)]
pub(crate) async fn should_skip_lifecycle_for_data_movement(
    store: Arc<ECStore>,
    bucket: &str,
    version: &rustfs_filemeta::FileInfo,
    lifecycle_config: Option<&BucketLifecycleConfiguration>,
    lock_retention: Option<DefaultRetention>,
    replication_config: Option<(ReplicationConfiguration, OffsetDateTime)>,
    apply_actions: bool,
    event_source: &LcEventSrc,
) -> bool {
    let Some(lifecycle_config) = lifecycle_config else {
        return false;
    };

    let versioned = BucketVersioningSys::prefix_enabled(bucket, &version.name).await;
    let object_info = crate::store_api::ObjectInfo::from_file_info(version, bucket, &version.name, versioned);
    let event = eval_action_from_lifecycle(lifecycle_config, lock_retention, replication_config, &object_info).await;

    match event.action {
        IlmAction::DeleteRestoredAction | IlmAction::DeleteRestoredVersionAction => {
            if apply_actions && object_info.is_remote() {
                let _ = apply_expiry_on_transitioned_object(store, &object_info, &event, event_source).await;
            }
            false
        }
        IlmAction::DeleteAction
        | IlmAction::DeleteVersionAction
        | IlmAction::DeleteAllVersionsAction
        | IlmAction::DelMarkerDeleteAllVersionsAction => {
            if apply_actions {
                let _ = apply_expiry_rule(&event, event_source, &object_info).await;
            }
            true
        }
        _ => false,
    }
}

impl ECStore {
    pub async fn status(&self, idx: usize) -> Result<PoolStatus> {
        let space_info = self.get_decommission_pool_space_info(idx).await?;

        let pool_meta = self.pool_meta.read().await;

        let mut pool_info = get_by_index(pool_meta.pools.as_slice(), idx, "fetch decommission status")?.clone();
        if let Some(d) = pool_info.decommission.as_mut() {
            d.total_size = space_info.total;
            d.current_size = space_info.free;
        } else {
            pool_info.decommission = Some(PoolDecommissionInfo {
                total_size: space_info.total,
                current_size: space_info.free,
                ..Default::default()
            });
        }

        Ok(pool_info)
    }

    async fn get_decommission_pool_space_info(&self, idx: usize) -> Result<PoolSpaceInfo> {
        if let Some(sets) = self.pools.get(idx) {
            let mut info = sets.storage_info().await;
            info.backend = self.backend_info().await;

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

    #[tracing::instrument(skip(self))]
    pub async fn decommission_cancel(&self, idx: usize) -> Result<()> {
        ensure_decommission_terminal_operation_supported(self.single_pool(), "cancel decommission")?;

        let mut lock = self.pool_meta.write().await;
        let (pool_present, decommission_present, terminal) = if let Some(pool) = lock.pools.get(idx) {
            if let Some(info) = pool.decommission.as_ref() {
                (true, true, is_decommission_cancel_terminal(info.complete, info.failed, info.canceled))
            } else {
                (true, false, false)
            }
        } else {
            (false, false, false)
        };

        ensure_decommission_cancel_allowed(pool_present, decommission_present, terminal)?;

        let should_reload_pool_meta = if lock.decommission_cancel(idx) {
            lock.save(self.pools.clone()).await?;
            true
        } else {
            false
        };
        drop(lock);

        let canceler = {
            let mut cancelers = self.decommission_cancelers.write().await;
            take_decommission_canceler(cancelers.as_mut_slice(), idx)
        };
        if !cancel_decommission_canceler(canceler) {
            warn!("decommission_cancel: no active canceler found for pool {}", idx);
        }

        if should_reload_pool_meta && let Some(notification_sys) = get_global_notification_sys() {
            let stage = format!("decommission_cancel for pool {idx}");
            if let Err(err) =
                resolve_decommission_pool_meta_reload_result(notification_sys.reload_pool_meta().await, stage.as_str())
            {
                warn!("{err}");
            }
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
                && !info.complete
                && !info.failed
                && !info.canceled
            {
                return true;
            }
        }

        false
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

        for (idx, canceler) in index_cancelers {
            let store = store.clone();
            tokio::spawn(async move {
                if let Err(err) = store.do_decommission_in_routine(canceler, idx).await {
                    error!("decommission: routine failed for idx {}: {err}", idx);
                }
            });
        }

        Ok(())
    }

    #[tracing::instrument(skip(self, rx))]
    pub async fn decommission(&self, rx: CancellationToken, indices: Vec<usize>) -> Result<()> {
        let indices = dedup_indices(&indices);

        warn!("decommission: {:?}", indices);
        validate_start_decommission_request(&indices, self.single_pool())?;

        ensure_decommission_not_rebalancing(self.is_rebalance_conflicting_with_decommission().await)?;

        let store = require_decommission_store(new_object_layer_fn(), "start decommission")?;

        self.start_decommission(indices.clone()).await?;
        if let Err(err) = self.spawn_decommission_routines(store, rx, indices.clone()).await {
            let mut rollback_err: Option<Error> = None;
            for idx in indices {
                if let Err(cancel_err) = self.decommission_cancel(idx).await {
                    error!(
                        "decommission: failed to rollback decommission state for idx {} after spawn error: {:?}",
                        idx, cancel_err
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

    #[allow(unused_assignments, clippy::too_many_arguments)]
    #[tracing::instrument(skip(self, set, wk, lifecycle_config, lock_retention, replication_config))]
    async fn decommission_entry(
        self: &Arc<Self>,
        idx: usize,
        entry: MetaCacheEntry,
        bucket: String,
        set: Arc<SetDisks>,
        wk: Arc<Workers>,
        lifecycle_config: Option<BucketLifecycleConfiguration>,
        lock_retention: Option<DefaultRetention>,
        replication_config: Option<(ReplicationConfiguration, OffsetDateTime)>,
    ) -> Result<()> {
        warn!("decommission_entry: {} {}", &bucket, &entry.name);
        wk.give().await;
        if entry.is_dir() {
            warn!("decommission_entry: skip dir {}", &entry.name);
            return Ok(());
        }

        let mut fivs = load_decommission_entry_versions(&entry, &bucket, "file_info_versions")?;

        fivs.versions
            .sort_by_key(|v| (v.mod_time.is_none(), std::cmp::Reverse(v.mod_time)));

        let mut decommissioned: usize = 0;
        let mut expired: usize = 0;

        for version in fivs.versions.iter() {
            if should_skip_lifecycle_for_data_movement(
                self.clone(),
                &bucket,
                version,
                lifecycle_config.as_ref(),
                lock_retention.clone(),
                replication_config.clone(),
                true,
                &LcEventSrc::Decom,
            )
            .await
            {
                expired += 1;
                continue;
            }

            let remaining_versions = decommission_remaining_version_count(fivs.versions.len(), expired);
            if should_skip_decommission_delete_marker(version, remaining_versions, replication_config.is_some()) {
                //
                decommissioned += 1;
                info!("decommission_pool: DELETE marked object with no other non-current versions will be skipped");
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
                    if is_err_object_not_found(&err) || is_err_version_not_found(&err) || is_err_data_movement_overwrite(&err) {
                        warn!(
                            "decommission_pool: ignore delete-marker copy for {}/{} version {:?}: {:?}",
                            &bucket, &version.name, &version_id, &err
                        );
                        ignore = true;
                        cleanup_ignored = true;
                    } else {
                        failure = true;

                        error = Some(err)
                    }
                }

                if ignore {
                    if should_count_decommission_version_complete(ignore, cleanup_ignored, failure) {
                        decommissioned += 1;
                    }
                    info!("decommission_pool: ignore {}", &version.name);
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

                info!(
                    "decommission_pool: DecomCopyDeleteMarker  {} {} {:?} {:?}",
                    &bucket, &version.name, &version_id, error
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
                        if is_err_object_not_found(&err) || is_err_version_not_found(&err) || is_err_data_movement_overwrite(&err)
                        {
                            ignore = true;
                            cleanup_ignored = true;
                            break;
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
                        &ObjectOptions {
                            version_id: version_id.clone(),
                            no_lock: true,
                            ..Default::default()
                        },
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

                if let Err(err) = self.clone().decommission_object(idx, bucket, rd).await {
                    if is_err_object_not_found(&err) || is_err_version_not_found(&err) || is_err_data_movement_overwrite(&err) {
                        ignore = true;
                        cleanup_ignored = true;
                        break;
                    }

                    failure = true;

                    error!("decommission_pool: decommission_object err {:?}", &err);
                    continue;
                }

                warn!(
                    "decommission_pool: decommission_object done {}/{} {}",
                    &bucket_name, &object_name, &version.name
                );

                failure = false;
                break;
            }

            if ignore {
                if should_count_decommission_version_complete(ignore, cleanup_ignored, failure) {
                    decommissioned += 1;
                }
                info!("decommission_pool: ignore {}", &version.name);
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
            let cleanup_result = set
                .delete_object(
                    bucket.as_str(),
                    &encode_dir_object(&entry.name),
                    ObjectOptions {
                        delete_prefix: true,
                        delete_prefix_object: true,

                        ..Default::default()
                    },
                )
                .await;
            resolve_decommission_entry_cleanup_delete_result(cleanup_result, bucket.as_str(), entry.name.as_str())?
        } else if decommissioned != fivs.versions.len() || expired > 0 {
            warn!(
                "decommission_pool: source object retained for {}/{} because only {}/{} versions were decommissioned and {} expired by lifecycle",
                &bucket,
                &entry.name,
                decommissioned,
                fivs.versions.len(),
                expired
            );
        }

        {
            let mut pool_meta = self.pool_meta.write().await;

            if let Err(err) = track_decommission_current_object(&mut pool_meta, idx, bucket.as_str(), entry.name.as_str()) {
                return Err(with_decommission_entry_context(
                    "track_decommission_current_object",
                    bucket.as_str(),
                    entry.name.as_str(),
                    err,
                ));
            }

            let ok = match resolve_decommission_update_after_result(
                pool_meta.update_after(idx, self.pools.clone(), Duration::seconds(30)).await,
            ) {
                Ok(ok) => ok,
                Err(err) => {
                    return Err(with_decommission_entry_context("update_after", bucket.as_str(), entry.name.as_str(), err));
                }
            };

            drop(pool_meta);
            if ok
                && let Some(notification_sys) = get_global_notification_sys()
                && let Err(err) = resolve_decommission_entry_reload_result(
                    notification_sys.reload_pool_meta().await,
                    bucket.as_str(),
                    entry.name.as_str(),
                )
            {
                warn!("{err}");
            }
        }

        warn!("decommission_pool: decommission_entry done {} {}", &bucket, &entry.name);
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
        let wk = Workers::new(pool.disk_set.len() * 2).map_err(Error::other)?;
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
            lifecycle_config = GLOBAL_LifecycleSys.get(&bi.name).await;
            lock_retention = BucketObjectLockSys::get(&bi.name).await;
            replication_config = resolve_decommission_optional_bucket_config_result(
                &bi.name,
                "replication",
                metadata_sys::get_replication_config(&bi.name).await,
            )?;
        }

        for (set_idx, set) in pool.disk_set.iter().enumerate() {
            wk.clone().take().await;

            warn!("decommission_pool: decommission_pool {} {}", set_idx, &bi.name);

            let decommission_entry: ListCallback = Arc::new({
                let this = Arc::clone(self);
                let bucket = bi.name.clone();
                let wk = wk.clone();
                let set = set.clone();
                let lifecycle_config = lifecycle_config.clone();
                let lock_retention = lock_retention.clone();
                let replication_config = replication_config.clone();
                let entry_error = entry_error.clone();
                let callback_rx = rx.clone();
                move |entry: MetaCacheEntry| {
                    let this = this.clone();
                    let bucket = bucket.clone();
                    let wk = wk.clone();
                    let set = set.clone();
                    let lifecycle_config = lifecycle_config.clone();
                    let lock_retention = lock_retention.clone();
                    let replication_config = replication_config.clone();
                    let entry_error = entry_error.clone();
                    let callback_rx = callback_rx.clone();

                    Box::pin(async move {
                        wk.take().await;
                        if let Err(err) = this
                            .decommission_entry(idx, entry, bucket, set, wk, lifecycle_config, lock_retention, replication_config)
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
            let wk_clone = wk.clone();
            let worker = tokio::spawn(async move {
                loop {
                    if rx_clone.is_cancelled() {
                        warn!("decommission_pool: cancel {}", set_id);
                        break;
                    }
                    warn!("decommission_pool: list_objects_to_decommission {} {}", set_id, &bi.name);

                    match set
                        .list_objects_to_decommission(rx_clone.clone(), bi.clone(), decommission_entry.clone())
                        .await
                    {
                        Ok(_) => {
                            warn!("decommission_pool: list_objects_to_decommission {} done", set_id);
                            break;
                        }
                        Err(err) => {
                            error!("decommission_pool: list_objects_to_decommission {} err {:?}", set_id, &err);
                            if is_err_bucket_not_found(&err) {
                                warn!("decommission_pool: list_objects_to_decommission {} volume not found", set_id);
                                break;
                            }

                            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                        }
                    }
                }

                wk_clone.give().await;
            });
            listing_workers.push((set_id, worker));
        }

        warn!("decommission_pool: decommission_pool wait {} {}", idx, &bi.name);

        let mut listing_worker_error = None;
        for (set_id, worker) in listing_workers {
            if let Err(err) = resolve_decommission_listing_worker_result(set_id, worker.await) {
                rx.cancel();
                wk.give().await;
                if listing_worker_error.is_none() {
                    listing_worker_error = Some(err);
                }
            }
        }

        wk.wait().await;

        if let Some(err) = listing_worker_error {
            return Err(err);
        }

        if let Some(err) = entry_error.lock().await.clone() {
            return Err(err);
        }

        if let Err(err) = decommission_cancel_signal_result(rx.is_cancelled()) {
            warn!("decommission_pool: canceled after wait {} {}", idx, &bi.name);
            return Err(err);
        }

        warn!("decommission_pool: decommission_pool done {} {}", idx, &bi.name);

        Ok(())
    }

    #[tracing::instrument(skip(self, rx))]
    pub async fn do_decommission_in_routine(self: &Arc<Self>, rx: CancellationToken, idx: usize) -> Result<()> {
        defer!(|| async {
            let mut cancelers = self.decommission_cancelers.write().await;
            if take_decommission_canceler(cancelers.as_mut_slice(), idx).is_none() {
                warn!("decommission: canceler already cleared for pool {}", idx);
            }
        });

        let result = self.decommission_in_background(rx.clone(), idx).await;

        let (final_state, canceled, cmd_line) = {
            let pool_meta = self.pool_meta.read().await;
            let Some(pool) = pool_meta.pools.get(idx) else {
                error!("decommission: pool metadata missing for idx {}", idx);
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
            error!("decom err {:?}", &err);

            if is_err_operation_canceled(&err) || should_preserve_decommission_canceled_state(canceled, rx.is_cancelled()) {
                warn!("decommission: canceled for pool {}, preserving canceled state", cmd_line);
                return Ok(());
            }

            resolve_decommission_terminal_mark_after_error_result(self.decommission_failed(idx).await, idx, &err)?;
            warn!("decommission: decommission_failed {}", idx);

            return Ok(());
        }

        warn!("decommission: decommission_in_background complete {}", idx);

        if should_preserve_decommission_canceled_state(canceled, rx.is_cancelled()) {
            warn!("decommission: canceled for pool {}, skipping terminal state overwrite", cmd_line);
            return Ok(());
        }

        match final_state {
            DecommissionFinalState::Complete => {
                warn!("Decommissioning complete for pool {}, verifying for any pending objects", cmd_line);
                if let Err(err) = self.check_after_decommission(idx).await {
                    resolve_decommission_terminal_mark_result(self.decommission_failed(idx).await, "failed", &cmd_line)?;
                    return Err(Error::other(format!(
                        "failed to finalize decommission for pool {cmd_line}: post-check failed: {err}"
                    )));
                }

                warn!("Decommissioning complete for pool {}, marking completed state", cmd_line);
                resolve_decommission_terminal_mark_result(self.complete_decommission(idx).await, "completed", &cmd_line)?;
            }
            DecommissionFinalState::Failed => {
                warn!("Decommissioning finished with failed items for pool {}, marking failed state", cmd_line);
                resolve_decommission_terminal_mark_result(self.decommission_failed(idx).await, "failed", &cmd_line)?;
            }
        }

        warn!("Decommissioning complete for pool {}", cmd_line);
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub async fn decommission_failed(&self, idx: usize) -> Result<()> {
        ensure_decommission_terminal_operation_supported(self.single_pool(), "mark decommission failed")?;

        let mut pool_meta = self.pool_meta.write().await;
        if pool_meta.decommission_failed(idx) {
            pool_meta.save(self.pools.clone()).await?;

            drop(pool_meta);

            if let Some(notification_sys) = get_global_notification_sys() {
                let stage = format!("decommission_failed for pool {idx}");
                if let Err(err) =
                    resolve_decommission_pool_meta_reload_result(notification_sys.reload_pool_meta().await, stage.as_str())
                {
                    warn!("{err}");
                }
            }
        }

        let canceler = {
            let mut cancelers = self.decommission_cancelers.write().await;
            cancelers.get_mut(idx).and_then(Option::take)
        };
        if let Some(canceler) = canceler {
            canceler.cancel();
        }

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub async fn complete_decommission(&self, idx: usize) -> Result<()> {
        ensure_decommission_terminal_operation_supported(self.single_pool(), "complete decommission")?;

        let mut pool_meta = self.pool_meta.write().await;
        if pool_meta.decommission_complete(idx) {
            pool_meta.save(self.pools.clone()).await?;
            drop(pool_meta);
            if let Some(notification_sys) = get_global_notification_sys() {
                let stage = format!("complete_decommission for pool {idx}");
                if let Err(err) =
                    resolve_decommission_pool_meta_reload_result(notification_sys.reload_pool_meta().await, stage.as_str())
                {
                    warn!("{err}");
                }
            }
        }

        let canceler = {
            let mut cancelers = self.decommission_cancelers.write().await;
            cancelers.get_mut(idx).and_then(Option::take)
        };
        if let Some(canceler) = canceler {
            canceler.cancel();
        }

        Ok(())
    }

    #[tracing::instrument(skip(self, rx))]
    async fn decommission_in_background(self: &Arc<Self>, rx: CancellationToken, idx: usize) -> Result<()> {
        let pool = get_by_index(self.pools.as_slice(), idx, "load decommission background pool")?.clone();

        let pending = {
            let pool_meta = self.pool_meta.read().await;
            pool_meta.pending_buckets(idx)
        };

        for bucket in pending.iter() {
            let is_decommissioned = {
                let pool_meta = self.pool_meta.read().await;
                resolve_decommission_bucket_state(&pool_meta, idx, bucket)?
            };

            if is_decommissioned {
                warn!("decommission: already done, moving on {}", bucket.to_string());

                {
                    let mut pool_meta = self.pool_meta.write().await;
                    if mark_decommission_bucket_done(&mut pool_meta, idx, bucket)? {
                        resolve_decommission_bucket_done_save_result(
                            pool_meta.save(self.pools.clone()).await,
                            idx,
                            bucket.name.as_str(),
                        )?;
                    }
                }
                continue;
            }

            warn!("decommission: currently on bucket {}", &bucket.name);

            if let Err(err) = self.decommission_pool(rx.clone(), idx, pool.clone(), bucket.clone()).await {
                error!("decommission: decommission_pool err {:?}", &err);
                return Err(err);
            } else {
                warn!("decommission: decommission_pool done {}", &bucket.name);
            }

            if let Err(err) = decommission_cancel_signal_result(rx.is_cancelled()) {
                warn!("decommission: cancellation observed after decommission_pool {}", &bucket.name);
                return Err(err);
            }

            {
                let mut pool_meta = self.pool_meta.write().await;
                if mark_decommission_bucket_done(&mut pool_meta, idx, bucket)? {
                    resolve_decommission_bucket_done_save_result(
                        pool_meta.save(self.pools.clone()).await,
                        idx,
                        bucket.name.as_str(),
                    )?;
                }

                warn!("decommission: decommission_pool bucket_done {}", &bucket.name);
            }
        }

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub async fn start_decommission(&self, indices: Vec<usize>) -> Result<()> {
        let indices = dedup_indices(&indices);
        validate_start_decommission_request(&indices, self.single_pool())?;

        ensure_decommission_not_rebalancing(self.is_rebalance_conflicting_with_decommission().await)?;

        for idx in indices.iter().copied() {
            ensure_valid_decommission_pool_index(self.pools.len(), idx)?;
        }

        {
            let pool_meta = self.pool_meta.read().await;
            for idx in indices.iter().copied() {
                let (pool_present, decommission_active) = decommission_start_guard_state(pool_meta.pools.get(idx));
                ensure_decommission_start_allowed(pool_present, decommission_active)?;
            }
        }

        let decom_buckets = self.get_buckets_to_decommission().await?;

        for bk in decom_buckets.iter() {
            resolve_decommission_preflight_heal_result(&bk.name, self.heal_bucket(&bk.name, &HealOpts::default()).await)?;
        }

        let meta_buckets = [
            path_join(&[PathBuf::from(RUSTFS_META_BUCKET), PathBuf::from(CONFIG_PREFIX)]),
            path_join(&[PathBuf::from(RUSTFS_META_BUCKET), PathBuf::from(BUCKET_META_PREFIX)]),
        ];

        for bk in meta_buckets.iter() {
            if let Err(err) = self
                .make_bucket(bk.to_string_lossy().to_string().as_str(), &MakeBucketOptions::default())
                .await
                && !is_err_bucket_exists(&err)
            {
                error!("decommission: make bucket failed: {err}");
                return Err(err);
            }
        }

        let mut space_infos = Vec::with_capacity(indices.len());
        for idx in indices.iter().copied() {
            let pi = self.get_decommission_pool_space_info(idx).await?;
            space_infos.push((idx, pi));
        }

        ensure_decommission_not_rebalancing(self.is_rebalance_conflicting_with_decommission().await)?;

        let mut pool_meta = self.pool_meta.write().await;
        for idx in indices.iter().copied() {
            let (pool_present, decommission_active) = decommission_start_guard_state(pool_meta.pools.get(idx));
            ensure_decommission_start_allowed(pool_present, decommission_active)?;
        }

        for (idx, pi) in space_infos {
            pool_meta.decommission(idx, pi)?;
            pool_meta.queue_buckets(idx, decom_buckets.clone());
        }

        pool_meta.save(self.pools.clone()).await?;

        if let Some(notification_sys) = get_global_notification_sys()
            && let Err(err) =
                resolve_decommission_pool_meta_reload_result(notification_sys.reload_pool_meta().await, "start_decommission")
        {
            warn!("{err}");
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
                let mut replication_config = None;
                if bucket_info.name != RUSTFS_META_BUCKET {
                    lifecycle_config = GLOBAL_LifecycleSys.get(&bucket_info.name).await;
                    lock_retention = BucketObjectLockSys::get(&bucket_info.name).await;
                    replication_config = resolve_decommission_optional_bucket_config_result(
                        &bucket_info.name,
                        "replication",
                        metadata_sys::get_replication_config(&bucket_info.name).await,
                    )?;
                }

                let versions_found = Arc::new(AtomicUsize::new(0));
                let entry_error = Arc::new(tokio::sync::Mutex::new(None::<Error>));
                let callback_rx = CancellationToken::new();
                let versions_found_cb = versions_found.clone();
                let entry_error_cb = entry_error.clone();
                let bucket_name = bucket_info.name.clone();
                let lifecycle_config_cb = lifecycle_config.clone();
                let lock_retention_cb = lock_retention.clone();
                let replication_config_cb = replication_config.clone();
                let store = Arc::clone(self);
                let callback_rx_cb = callback_rx.clone();

                let callback: ListCallback = Arc::new(move |entry: MetaCacheEntry| {
                    let versions_found = versions_found_cb.clone();
                    let entry_error = entry_error_cb.clone();
                    let bucket_name = bucket_name.clone();
                    let lifecycle_config = lifecycle_config_cb.clone();
                    let lock_retention = lock_retention_cb.clone();
                    let replication_config = replication_config_cb.clone();
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
                            if should_skip_lifecycle_for_data_movement(
                                Arc::clone(&store),
                                &bucket_name,
                                version,
                                lifecycle_config.as_ref(),
                                lock_retention.clone(),
                                replication_config.clone(),
                                false,
                                &LcEventSrc::Decom,
                            )
                            .await
                            {
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
    fn should_skip_decommission_delete_marker_when_last_remaining_without_replication() {
        let version = rustfs_filemeta::FileInfo {
            deleted: true,
            ..Default::default()
        };

        assert!(should_skip_decommission_delete_marker(&version, 1, false));
    }

    #[test]
    fn should_skip_decommission_delete_marker_rejects_configured_replication() {
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
            replication_state_internal: Some(rustfs_filemeta::ReplicationState {
                replica_status: rustfs_filemeta::ReplicationStatusType::Replica,
                delete_marker: true,
                replicate_decision_str: "existing".to_string(),
                ..Default::default()
            }),
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
        assert_eq!(replication.replica_status, rustfs_filemeta::ReplicationStatusType::Replica);
        assert!(replication.delete_marker);
        assert_eq!(replication.replicate_decision_str, "existing");
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
    fn decommission_state_transitions_preserve_start_time() {
        let start_time = OffsetDateTime::now_utc();
        let mut pool_meta = PoolMeta {
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

        assert!(pool_meta.decommission_failed(0));
        assert_eq!(
            pool_meta.pools[0].decommission.as_ref().and_then(|info| info.start_time),
            Some(start_time)
        );

        assert!(pool_meta.decommission_complete(0));
        assert_eq!(
            pool_meta.pools[0].decommission.as_ref().and_then(|info| info.start_time),
            Some(start_time)
        );

        assert!(pool_meta.decommission_cancel(0));
        assert_eq!(
            pool_meta.pools[0].decommission.as_ref().and_then(|info| info.start_time),
            Some(start_time)
        );
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
                    queued_buckets: vec!["bucket-a".to_string(), "bucket-b/prefix".to_string()],
                    decommissioned_buckets: vec!["bucket-done".to_string()],
                    bucket: "bucket-b".to_string(),
                    prefix: "prefix".to_string(),
                    object: "object.txt".to_string(),
                    items_decommissioned: 7,
                    items_decommission_failed: 1,
                    bytes_done: 1024,
                    bytes_failed: 128,
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
            .into();

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
        assert_eq!(restored_decommission.items_decommissioned, 7);
        assert_eq!(restored_decommission.items_decommission_failed, 1);
        assert_eq!(restored_decommission.bytes_done, 1024);
        assert_eq!(restored_decommission.bytes_failed, 128);
    }

    #[test]
    fn pool_meta_decode_supports_legacy_payload() {
        let start_time = OffsetDateTime::now_utc();
        let legacy_meta = PoolMeta {
            version: POOL_META_VERSION,
            pools: vec![PoolStatus {
                id: 3,
                cmd_line: "/legacy/pool".to_string(),
                last_update: start_time,
                decommission: Some(PoolDecommissionInfo {
                    start_time: Some(start_time),
                    items_decommissioned: 9,
                    items_decommission_failed: 2,
                    bytes_done: 2048,
                    bytes_failed: 256,
                    queued_buckets: vec!["not-persisted".to_string()],
                    decommissioned_buckets: vec!["not-persisted".to_string()],
                    bucket: "not-persisted".to_string(),
                    prefix: "not-persisted".to_string(),
                    object: "not-persisted".to_string(),
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
        // These fields were skipped in legacy payload and should be defaulted.
        assert!(decommission.queued_buckets.is_empty());
        assert!(decommission.decommissioned_buckets.is_empty());
        assert!(decommission.bucket.is_empty());
        assert!(decommission.prefix.is_empty());
        assert!(decommission.object.is_empty());
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
        DecomBucketInfo, DecommissionTerminalState, PoolDecommissionInfo, PoolMeta, PoolStatus, bind_decommission_cancelers,
        cancel_decommission_canceler, classify_decommission_terminal_state, count_decommission_item,
        decommission_cancel_signal_result, decommission_item_size, decommission_start_guard_state, dedup_indices,
        ensure_decommission_cancel_allowed, ensure_decommission_listing_disks_available, ensure_decommission_not_rebalancing,
        ensure_decommission_start_allowed, ensure_decommission_terminal_operation_supported,
        ensure_valid_decommission_pool_index, get_by_index, has_active_decommission_canceler, is_decommission_active,
        is_decommission_cancel_terminal, load_decommission_entry_versions, mark_decommission_bucket_done,
        require_decommission_store, resolve_decommission_bucket_done_save_result, resolve_decommission_bucket_state,
        resolve_decommission_check_after_list_result, resolve_decommission_entry_cleanup_delete_result,
        resolve_decommission_entry_reload_result, resolve_decommission_listing_worker_result,
        resolve_decommission_optional_bucket_config_result, resolve_decommission_pool_meta_reload_result,
        resolve_decommission_preflight_heal_result, resolve_decommission_spawn_failure_result,
        resolve_decommission_terminal_mark_after_error_result, resolve_decommission_terminal_mark_result,
        resolve_decommission_update_after_result, should_cleanup_decommission_source_entry,
        should_count_decommission_version_complete, should_preserve_decommission_canceled_state, take_decommission_canceler,
        track_decommission_current_object, validate_start_decommission_request, with_decommission_entry_context,
    };
    use crate::data_movement;
    use crate::error::Error;
    use rustfs_filemeta::MetaCacheEntry;
    use rustfs_rio::Index;
    use time::{Duration, OffsetDateTime};
    use tokio_util::sync::CancellationToken;

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
    fn test_should_cleanup_decommission_source_entry_rejects_versions_only_expired_by_lifecycle() {
        assert!(!should_cleanup_decommission_source_entry(2, 3, 1));
    }

    #[tokio::test]
    async fn test_pool_meta_update_after_rejects_out_of_range_index() {
        let mut meta = PoolMeta::default();
        let err = meta
            .update_after(1, Vec::new(), Duration::seconds(1))
            .await
            .expect_err("out-of-range index should fail");
        assert!(err.to_string().contains("invalid decommission pool index 1 for 0 pools"));
    }

    #[tokio::test]
    async fn test_pool_meta_update_after_rejects_when_decommission_missing() {
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
            .update_after(0, Vec::new(), Duration::seconds(1))
            .await
            .expect_err("pool without decommission should fail");
        assert!(
            err.to_string()
                .contains("failed to update decommission metadata timestamp: decommission metadata not initialized")
        );
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
    fn test_is_decommission_active_true_only_when_not_terminal() {
        assert!(is_decommission_active(false, false, false));
        assert!(!is_decommission_active(true, false, false));
        assert!(!is_decommission_active(false, true, false));
        assert!(!is_decommission_active(false, false, true));
    }

    #[test]
    fn test_ensure_decommission_start_allowed_rejects_missing_pool() {
        let err = ensure_decommission_start_allowed(false, false).expect_err("missing pool should be invalid");
        assert!(
            err.to_string()
                .contains("failed to start decommission: target pool was not found")
        );
    }

    #[test]
    fn test_ensure_decommission_start_allowed_rejects_running_state() {
        let err = ensure_decommission_start_allowed(true, true).expect_err("active decommission should be rejected");
        assert!(matches!(err, Error::DecommissionAlreadyRunning));
    }

    #[test]
    fn test_ensure_decommission_start_allowed_allows_terminal_state() {
        assert!(ensure_decommission_start_allowed(true, false).is_ok());
    }

    #[test]
    fn test_decommission_start_guard_state_reports_missing_pool() {
        assert_eq!(decommission_start_guard_state(None), (false, false));
    }

    #[test]
    fn test_decommission_start_guard_state_reports_idle_pool_without_decommission_info() {
        let pool = PoolStatus {
            id: 0,
            cmd_line: "pool-0".to_string(),
            last_update: OffsetDateTime::UNIX_EPOCH,
            decommission: None,
        };

        assert_eq!(decommission_start_guard_state(Some(&pool)), (true, false));
    }

    #[test]
    fn test_decommission_start_guard_state_reports_active_pool_when_not_terminal() {
        let pool = PoolStatus {
            id: 0,
            cmd_line: "pool-0".to_string(),
            last_update: OffsetDateTime::UNIX_EPOCH,
            decommission: Some(PoolDecommissionInfo {
                complete: false,
                failed: false,
                canceled: false,
                ..Default::default()
            }),
        };

        assert_eq!(decommission_start_guard_state(Some(&pool)), (true, true));
    }

    #[test]
    fn test_decommission_start_guard_state_reports_terminal_pool_as_not_active() {
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

        assert_eq!(decommission_start_guard_state(Some(&pool)), (true, false));
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
        assert!(should_preserve_decommission_canceled_state(false, true));
    }

    #[test]
    fn test_should_preserve_decommission_canceled_state_when_not_canceled() {
        assert!(!should_preserve_decommission_canceled_state(false, false));
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
    fn test_ensure_decommission_cancel_allowed_rejects_missing_pool() {
        let err = ensure_decommission_cancel_allowed(false, false, false).expect_err("missing pool should be invalid");
        assert!(
            err.to_string()
                .contains("failed to cancel decommission: target pool was not found")
        );
    }

    #[test]
    fn test_is_decommission_cancel_terminal_true_when_completed() {
        assert!(is_decommission_cancel_terminal(true, false, false));
    }

    #[test]
    fn test_is_decommission_cancel_terminal_true_when_failed() {
        assert!(is_decommission_cancel_terminal(false, true, false));
    }

    #[test]
    fn test_is_decommission_cancel_terminal_true_when_canceled() {
        assert!(is_decommission_cancel_terminal(false, false, true));
    }

    #[test]
    fn test_is_decommission_cancel_terminal_false_when_active() {
        assert!(!is_decommission_cancel_terminal(false, false, false));
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
    fn test_contextualized_decommission_start_request_allows_non_empty_multi_pool() {
        assert!(validate_start_decommission_request(&[0, 1], false).is_ok());
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
