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

use super::*;
use crate::set_disk::{
    get_lock_acquire_timeout, get_object_lock_diag_slow_acquire_threshold, get_object_lock_diag_slow_hold_threshold,
    is_lock_optimization_enabled, is_object_lock_diag_enabled,
};
use crate::storage_api_contracts::object::{ObjectIO as _, ObjectOperations as _};
use rustfs_io_metrics::{
    record_object_lock_diag_acquire_duration, record_object_lock_diag_hold_duration, record_object_lock_diag_slow_acquire,
    record_object_lock_diag_slow_hold,
};
use std::{
    fmt,
    pin::Pin,
    task::{Context, Poll},
    time::{Duration, Instant},
};
use tokio::io::{AsyncRead, ReadBuf};

struct LockGuardedReader {
    inner: Box<dyn AsyncRead + Unpin + Send + Sync>,
    guard: Option<ObjectLockDiagGuard>,
}

impl AsyncRead for LockGuardedReader {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        let had_capacity = buf.remaining() > 0;
        let filled_before = buf.filled().len();
        let poll = Pin::new(&mut self.inner).poll_read(cx, buf);
        if had_capacity && matches!(poll, Poll::Ready(Ok(()))) && buf.filled().len() == filled_before {
            self.guard.take();
        }
        poll
    }
}

#[derive(Clone, Copy, Debug)]
enum ObjectLockDiagMode {
    Read,
    Write,
}

impl ObjectLockDiagMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Write => "write",
        }
    }
}

impl fmt::Display for ObjectLockDiagMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

struct ObjectLockDiagGuard {
    guard: rustfs_lock::NamespaceLockGuard,
    enabled: bool,
    op: &'static str,
    bucket: Option<String>,
    object: Option<String>,
    owner: Option<String>,
    mode: ObjectLockDiagMode,
    acquired_at: Instant,
}

impl ObjectLockDiagGuard {
    fn new(
        guard: rustfs_lock::NamespaceLockGuard,
        enabled: bool,
        op: &'static str,
        bucket: Option<String>,
        object: Option<String>,
        owner: Option<String>,
        mode: ObjectLockDiagMode,
    ) -> Self {
        Self {
            guard,
            enabled,
            op,
            bucket,
            object,
            owner,
            mode,
            acquired_at: Instant::now(),
        }
    }
}

impl Drop for ObjectLockDiagGuard {
    fn drop(&mut self) {
        if !self.enabled || self.guard.is_released() {
            return;
        }

        let hold = self.acquired_at.elapsed();
        record_object_lock_diag_hold_duration(self.op, self.mode.as_str(), hold);
        let threshold = get_object_lock_diag_slow_hold_threshold();
        if hold >= threshold {
            record_object_lock_diag_slow_hold(self.op, self.mode.as_str());
            warn!(
                target: "rustfs_ecstore::object_lock_diag",
                op = self.op,
                bucket = %self.bucket.as_deref().unwrap_or_default(),
                object = %self.object.as_deref().unwrap_or_default(),
                mode = %self.mode,
                owner = %self.owner.as_deref().unwrap_or_default(),
                hold_ms = hold.as_millis(),
                threshold_ms = threshold.as_millis(),
                "object namespace lock held longer than threshold"
            );
        }
    }
}

fn log_object_lock_acquire_if_slow(
    op: &'static str,
    bucket: &str,
    object: &str,
    owner: Option<&str>,
    mode: ObjectLockDiagMode,
    elapsed: Duration,
    diag_enabled: bool,
) {
    if !diag_enabled {
        return;
    }

    let threshold = get_object_lock_diag_slow_acquire_threshold();
    record_object_lock_diag_acquire_duration(op, mode.as_str(), elapsed);
    if elapsed >= threshold {
        record_object_lock_diag_slow_acquire(op, mode.as_str());
        warn!(
            target: "rustfs_ecstore::object_lock_diag",
            op,
            bucket,
            object,
            mode = %mode,
            owner = owner.unwrap_or_default(),
            acquire_ms = elapsed.as_millis(),
            threshold_ms = threshold.as_millis(),
            "object namespace lock acquisition exceeded threshold"
        );
    }
}

fn select_data_movement_target_pool(
    existing_pool_idx: Result<usize>,
    src_pool_idx: usize,
    delete_marker: bool,
) -> Result<Option<usize>> {
    match existing_pool_idx {
        Ok(pool_idx) => {
            if delete_marker && pool_idx == src_pool_idx {
                Ok(None)
            } else {
                Ok(Some(pool_idx))
            }
        }
        Err(err) => {
            if is_err_read_quorum(&err) {
                return Err(StorageError::ErasureWriteQuorum);
            }
            if delete_marker && (is_err_object_not_found(&err) || is_err_version_not_found(&err)) {
                Ok(None)
            } else {
                Err(err)
            }
        }
    }
}

fn latest_object_access_delete_marker_error(
    bucket: &str,
    object: &str,
    info: &ObjectInfo,
    opts: &ObjectOptions,
) -> Option<Error> {
    if !info.delete_marker {
        return None;
    }

    Some(if opts.version_id.is_none() || opts.delete_marker {
        to_object_err(StorageError::FileNotFound, vec![bucket, object])
    } else {
        to_object_err(StorageError::MethodNotAllowed, vec![bucket, object])
    })
}

fn resolve_latest_object_access(
    bucket: &str,
    object: &str,
    info: ObjectInfo,
    idx: usize,
    opts: &ObjectOptions,
) -> Result<(ObjectInfo, usize)> {
    if let Some(err) = latest_object_access_delete_marker_error(bucket, object, &info, opts) {
        return Err(err);
    }

    Ok((info, idx))
}

fn should_create_delete_marker_for_missing_object(opts: &ObjectOptions) -> bool {
    opts.versioned && opts.version_id.is_none() && !opts.delete_marker && !opts.data_movement
}

/// Whether a delete-time lookup miss on a directory key should trigger an orphan
/// empty-directory tree purge (issue #4189).
///
/// The lookup surfaces *version*-not-found here, not object-not-found: `del_opts`
/// pins `version_id = Uuid::nil()` for directory keys, so a missing dir object fails
/// the specific-version lookup. Both misses must be accepted, otherwise the real
/// HTTP delete path (which always sets the nil version) never reaches the purge and
/// the ghost folder survives with a fake 204 — the exact #4189 symptom.
fn should_purge_orphan_dir_on_missing(err: &Error, object: &str) -> bool {
    (is_err_object_not_found(err) || is_err_version_not_found(err)) && rustfs_utils::path::is_dir_object(object)
}

fn version_aware_lookup_opts(opts: &ObjectOptions, no_lock: bool) -> ObjectOptions {
    let mut lookup_opts = opts.clone();
    lookup_opts.no_lock = no_lock;
    if lookup_opts.version_id.is_some() {
        lookup_opts.metadata_chg = true;
    }

    lookup_opts
}

fn data_movement_pool_lookup_opts(opts: &ObjectOptions, no_lock: bool) -> ObjectOptions {
    let mut lookup_opts = version_aware_lookup_opts(opts, no_lock);
    lookup_opts.skip_decommissioned = true;
    lookup_opts.skip_rebalancing = true;

    lookup_opts
}

fn transition_restore_pool_opts(opts: &ObjectOptions) -> ObjectOptions {
    let mut lookup_opts = opts.clone();
    lookup_opts.skip_decommissioned = true;
    lookup_opts
}

fn effective_object_actual_size(info: &ObjectInfo) -> Option<i64> {
    info.get_actual_size().ok()
}

fn is_equivalent_data_movement_delete_marker(source: &ObjectInfo, target: &ObjectInfo) -> bool {
    is_data_movement_delete_marker(source)
        && is_data_movement_delete_marker(target)
        && source.version_id == target.version_id
        && source.mod_time == target.mod_time
        && source.user_defined == target.user_defined
        && source.user_tags == target.user_tags
        && source.replication_status_internal == target.replication_status_internal
        && source.replication_status == target.replication_status
        && source.version_purge_status_internal == target.version_purge_status_internal
        && source.version_purge_status == target.version_purge_status
}

fn is_data_movement_delete_marker(info: &ObjectInfo) -> bool {
    info.delete_marker
}

fn expected_data_movement_tiered_object(source: &rustfs_filemeta::FileInfo) -> ObjectInfo {
    ObjectInfo::from_file_info(source, "", &source.name, source.version_id.is_some())
}

fn is_equivalent_data_movement_tiered_object(source: &rustfs_filemeta::FileInfo, target: &ObjectInfo) -> bool {
    let expected = expected_data_movement_tiered_object(source);

    source.version_id == target.version_id
        && !target.delete_marker
        && source.size == target.size
        && source.get_etag() == target.etag
        && source.checksum == target.checksum
        && source.mod_time == target.mod_time
        && expected.user_defined == target.user_defined
        && expected.user_tags == target.user_tags
        && expected.expires == target.expires
        && expected.storage_class == target.storage_class
        && expected.replication_status_internal == target.replication_status_internal
        && expected.replication_status == target.replication_status
        && expected.version_purge_status_internal == target.version_purge_status_internal
        && expected.version_purge_status == target.version_purge_status
        && expected.transitioned_object.status == target.transitioned_object.status
        && expected.transitioned_object.name == target.transitioned_object.name
        && expected.transitioned_object.tier == target.transitioned_object.tier
        && expected.transitioned_object.version_id == target.transitioned_object.version_id
        && expected.transitioned_object.free_version == target.transitioned_object.free_version
        && effective_object_actual_size(target) == Some(source.size)
}

fn should_check_data_movement_resume_target(src_pool_idx: usize, target_pool_idx: usize) -> bool {
    target_pool_idx != src_pool_idx
}

fn resolve_data_movement_resume_target_pool(
    selected_target_pool_idx: usize,
    resume_target_pool_idx: Option<usize>,
    src_pool_idx: usize,
) -> usize {
    if should_check_data_movement_resume_target(src_pool_idx, selected_target_pool_idx) {
        selected_target_pool_idx
    } else {
        resume_target_pool_idx.unwrap_or(selected_target_pool_idx)
    }
}

fn resolve_data_movement_delete_marker_resume_result(
    target_result: Result<Option<ObjectInfo>>,
    source: &ObjectInfo,
    src_pool_idx: usize,
    target_pool_idx: usize,
) -> Result<bool> {
    if !should_check_data_movement_resume_target(src_pool_idx, target_pool_idx) {
        return Ok(false);
    }

    let Some(target) = target_result? else {
        return Ok(false);
    };

    Ok(is_equivalent_data_movement_delete_marker(source, &target))
}

fn resolve_data_movement_tiered_resume_result(
    target_result: Result<Option<ObjectInfo>>,
    source: &rustfs_filemeta::FileInfo,
    src_pool_idx: usize,
    target_pool_idx: usize,
) -> Result<bool> {
    if !should_check_data_movement_resume_target(src_pool_idx, target_pool_idx) {
        return Ok(false);
    }

    let Some(target) = target_result? else {
        return Ok(false);
    };

    Ok(is_equivalent_data_movement_tiered_object(source, &target))
}

fn return_batch_delete_lock_error(objects: &[ObjectToDelete], err: Error) -> (Vec<DeletedObject>, Vec<Option<Error>>) {
    let del_objects = objects
        .iter()
        .map(|object| DeletedObject {
            object_name: decode_dir_object(&object.object_name),
            version_id: object.version_id,
            ..Default::default()
        })
        .collect();
    let del_errs = objects.iter().map(|_| Some(err.clone())).collect();

    (del_objects, del_errs)
}

fn sorted_unique_delete_object_names(objects: &[ObjectToDelete]) -> Vec<&str> {
    let mut object_names: Vec<&str> = objects.iter().map(|object| object.object_name.as_str()).collect();
    object_names.sort_unstable();
    object_names.dedup();
    object_names
}

impl ECStore {
    fn map_namespace_lock_error(bucket: &str, object: &str, mode: &'static str, err: rustfs_lock::LockError) -> StorageError {
        match err {
            rustfs_lock::LockError::QuorumNotReached { required, achieved } => StorageError::NamespaceLockQuorumUnavailable {
                mode,
                bucket: bucket.to_string(),
                object: object.to_string(),
                required,
                achieved,
            },
            other => StorageError::Lock(other),
        }
    }

    async fn acquire_object_write_lock(&self, op: &'static str, bucket: &str, object: &str) -> Result<ObjectLockDiagGuard> {
        let diag_enabled = is_object_lock_diag_enabled();
        let ns_lock = self.handle_new_ns_lock(bucket, object).await?;
        let acquire_start = Instant::now();
        let guard = ns_lock
            .get_write_lock(get_lock_acquire_timeout())
            .await
            .map_err(|err| Self::map_namespace_lock_error(bucket, object, "write", err))?;
        let owner = diag_enabled.then(|| ns_lock.owner().to_string());
        log_object_lock_acquire_if_slow(
            op,
            bucket,
            object,
            owner.as_deref(),
            ObjectLockDiagMode::Write,
            acquire_start.elapsed(),
            diag_enabled,
        );

        Ok(ObjectLockDiagGuard::new(
            guard,
            diag_enabled,
            op,
            diag_enabled.then(|| bucket.to_string()),
            diag_enabled.then(|| object.to_string()),
            owner,
            ObjectLockDiagMode::Write,
        ))
    }

    async fn acquire_object_write_lock_if_needed(
        &self,
        op: &'static str,
        bucket: &str,
        object: &str,
        opts: &mut ObjectOptions,
    ) -> Result<Option<ObjectLockDiagGuard>> {
        if opts.no_lock {
            return Ok(None);
        }

        let guard = self.acquire_object_write_lock(op, bucket, object).await?;
        opts.no_lock = true;

        Ok(Some(guard))
    }

    async fn acquire_delete_objects_write_locks(
        &self,
        bucket: &str,
        objects: &[ObjectToDelete],
        opts: &mut ObjectOptions,
    ) -> Result<Vec<ObjectLockDiagGuard>> {
        if opts.no_lock || objects.is_empty() {
            return Ok(Vec::new());
        }

        let object_names = sorted_unique_delete_object_names(objects);
        // Lock order: encoded object names are acquired in ascending order, then
        // the set-layer calls receive no_lock so they do not reacquire them.
        let mut guards = Vec::with_capacity(object_names.len());
        for object in object_names {
            guards.push(self.acquire_object_write_lock("delete_objects", bucket, object).await?);
        }
        opts.no_lock = true;

        Ok(guards)
    }

    async fn acquire_object_read_lock_if_needed(
        &self,
        op: &'static str,
        bucket: &str,
        object: &str,
        opts: &mut ObjectOptions,
    ) -> Result<Option<ObjectLockDiagGuard>> {
        if opts.no_lock {
            return Ok(None);
        }

        let diag_enabled = is_object_lock_diag_enabled();
        let ns_lock = self.handle_new_ns_lock(bucket, object).await?;
        let acquire_start = Instant::now();
        let guard = ns_lock
            .get_read_lock(get_lock_acquire_timeout())
            .await
            .map_err(|err| Self::map_namespace_lock_error(bucket, object, "read", err))?;
        let owner = diag_enabled.then(|| ns_lock.owner().to_string());
        log_object_lock_acquire_if_slow(
            op,
            bucket,
            object,
            owner.as_deref(),
            ObjectLockDiagMode::Read,
            acquire_start.elapsed(),
            diag_enabled,
        );
        opts.no_lock = true;
        opts.metadata_cache_safe = true;

        Ok(Some(ObjectLockDiagGuard::new(
            guard,
            diag_enabled,
            op,
            diag_enabled.then(|| bucket.to_string()),
            diag_enabled.then(|| object.to_string()),
            owner,
            ObjectLockDiagMode::Read,
        )))
    }

    fn attach_read_lock_guard(mut reader: GetObjectReader, guard: Option<ObjectLockDiagGuard>) -> GetObjectReader {
        if is_lock_optimization_enabled() || reader.buffered_body.is_some() {
            return reader;
        }

        if let Some(guard) = guard {
            reader.stream = Box::new(LockGuardedReader {
                inner: reader.stream,
                guard: Some(guard),
            });
        }

        reader
    }

    async fn get_latest_accessible_object_info_with_idx(
        &self,
        bucket: &str,
        object: &str,
        opts: &ObjectOptions,
    ) -> Result<(ObjectInfo, usize)> {
        let (info, idx) = self.get_latest_object_info_with_idx(bucket, object, opts).await?;
        resolve_latest_object_access(bucket, object, info, idx, opts)
    }

    pub(super) async fn select_data_movement_pool_idx(
        &self,
        bucket: &str,
        object: &str,
        size: i64,
        opts: &ObjectOptions,
        no_lock: bool,
    ) -> Result<usize> {
        match self
            .get_pool_info_existing_with_opts(bucket, object, &data_movement_pool_lookup_opts(opts, no_lock))
            .await
        {
            Ok((pinfo, _)) => Ok(pinfo.index),
            Err(err) => {
                if !is_err_object_not_found(&err) && !is_err_version_not_found(&err) {
                    return Err(err);
                }

                self.get_available_pool_idx(bucket, object, size).await.ok_or(Error::DiskFull)
            }
        }
    }

    async fn find_data_movement_target_info(
        &self,
        bucket: &str,
        object: &str,
        target_pool_idx: usize,
        opts: &ObjectOptions,
    ) -> Result<Option<ObjectInfo>> {
        let lookup_opts = version_aware_lookup_opts(opts, true);

        let Some(pool) = self.pools.get(target_pool_idx) else {
            return Err(Error::other(format!(
                "data movement resume target pool {target_pool_idx} is out of range for {bucket}/{object}"
            )));
        };

        match pool.get_object_info(bucket, object, &lookup_opts).await {
            Ok(info) => Ok(Some(info)),
            Err(err) if is_err_object_not_found(&err) || is_err_version_not_found(&err) => Ok(None),
            Err(err) => Err(err),
        }
    }

    async fn has_equivalent_data_movement_delete_marker(
        &self,
        bucket: &str,
        object: &str,
        source: &ObjectInfo,
        opts: &ObjectOptions,
        target_pool_idx: usize,
    ) -> Result<bool> {
        resolve_data_movement_delete_marker_resume_result(
            self.find_data_movement_target_info(bucket, object, target_pool_idx, opts)
                .await,
            source,
            opts.src_pool_idx,
            target_pool_idx,
        )
    }

    async fn has_equivalent_data_movement_tiered_object(
        &self,
        bucket: &str,
        object: &str,
        source: &rustfs_filemeta::FileInfo,
        opts: &ObjectOptions,
        target_pool_idx: usize,
    ) -> Result<bool> {
        resolve_data_movement_tiered_resume_result(
            self.find_data_movement_target_info(bucket, object, target_pool_idx, opts)
                .await,
            source,
            opts.src_pool_idx,
            target_pool_idx,
        )
    }

    fn resolve_decommission_target_pool_idx_result(result: Result<usize>, bucket: &str, object: &str) -> Result<usize> {
        result.map_err(|err| Error::other(format!("failed to select decommission target pool for {bucket}/{object}: {err}")))
    }

    fn resolve_decommission_tiered_object_result(result: Result<()>, bucket: &str, object: &str) -> Result<()> {
        result.map_err(|err| Error::other(format!("failed to decommission tiered object for {bucket}/{object}: {err}")))
    }

    #[instrument(skip(self, fi, opts))]
    pub(crate) async fn decommission_tiered_object(
        &self,
        bucket: &str,
        object: &str,
        fi: &rustfs_filemeta::FileInfo,
        opts: &ObjectOptions,
    ) -> Result<()> {
        check_put_object_args(bucket, object)?;

        let object = encode_dir_object(object);

        if self.single_pool() {
            return Self::resolve_decommission_tiered_object_result(
                Err(Error::other("single pool deployments cannot decommission tiered objects")),
                bucket,
                &object,
            );
        }

        let idx = if opts.data_movement && opts.version_id.is_some() {
            Self::resolve_decommission_target_pool_idx_result(
                self.select_data_movement_pool_idx(bucket, &object, fi.size, opts, true).await,
                bucket,
                &object,
            )?
        } else {
            Self::resolve_decommission_target_pool_idx_result(
                self.get_pool_idx_no_lock(bucket, &object, fi.size).await,
                bucket,
                &object,
            )?
        };
        if opts.data_movement && idx == opts.src_pool_idx {
            let resume_target_pool_idx = self
                .get_available_pool_idx_excluding(bucket, &object, fi.size, opts.src_pool_idx)
                .await;
            let target_pool_idx = resolve_data_movement_resume_target_pool(idx, resume_target_pool_idx, opts.src_pool_idx);
            if self
                .has_equivalent_data_movement_tiered_object(bucket, &object, fi, opts, target_pool_idx)
                .await?
            {
                return Ok(());
            }

            return Err(StorageError::DataMovementOverwriteErr(
                bucket.to_owned(),
                object.to_owned(),
                opts.version_id.clone().unwrap_or_default(),
            ));
        }

        Self::resolve_decommission_tiered_object_result(
            self.pools[idx]
                .get_disks_by_key(&object)
                .decommission_tiered_object(bucket, &object, fi, opts)
                .await,
            bucket,
            &object,
        )
    }

    #[instrument(level = "debug", skip(self))]
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub(super) async fn handle_get_object_reader(
        &self,
        bucket: &str,
        object: &str,
        range: Option<HTTPRangeSpec>,
        h: HeaderMap,
        opts: &ObjectOptions,
    ) -> Result<GetObjectReader> {
        check_get_obj_args(bucket, object)?;

        let object = encode_dir_object(object);
        let mut opts = opts.clone();
        let read_lock_guard = self
            .acquire_object_read_lock_if_needed("get_object", bucket, &object, &mut opts)
            .await?;

        let reader = if self.single_pool() {
            self.pools[0]
                .get_object_reader(bucket, object.as_str(), range, h, &opts)
                .await?
        } else {
            let (_, idx) = self
                .get_latest_accessible_object_info_with_idx(bucket, &object, &opts)
                .await?;
            self.pools[idx]
                .get_object_reader(bucket, object.as_str(), range, h, &opts)
                .await?
        };

        Ok(Self::attach_read_lock_guard(reader, read_lock_guard))
    }

    #[instrument(level = "debug", skip(self, data))]
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub(super) async fn handle_put_object(
        &self,
        bucket: &str,
        object: &str,
        data: &mut PutObjReader,
        opts: &ObjectOptions,
    ) -> Result<ObjectInfo> {
        check_put_object_args(bucket, object)?;

        let object = encode_dir_object(object);

        // Keep PUT atomic-read friendly: SetDisks takes the object write lock only
        // around precondition checks and the final rename/commit.
        if self.single_pool() {
            return self.pools[0].put_object(bucket, object.as_str(), data, opts).await;
        }

        let idx = if opts.data_movement && opts.version_id.is_some() {
            self.select_data_movement_pool_idx(bucket, &object, data.size(), opts, false)
                .await?
        } else {
            self.get_pool_idx(bucket, &object, data.size()).await?
        };

        if opts.data_movement && idx == opts.src_pool_idx {
            return Err(StorageError::DataMovementOverwriteErr(
                bucket.to_owned(),
                object.to_owned(),
                opts.version_id.clone().unwrap_or_default(),
            ));
        }

        self.pools[idx].put_object(bucket, &object, data, opts).await
    }

    #[instrument(skip(self))]
    pub(super) async fn handle_get_object_info(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<ObjectInfo> {
        check_object_args(bucket, object)?;

        let object = encode_dir_object(object);
        let mut opts = opts.clone();
        let _object_lock_guard = self
            .acquire_object_read_lock_if_needed("get_object_info", bucket, &object, &mut opts)
            .await?;

        let info = if self.single_pool() {
            self.pools[0].get_object_info(bucket, object.as_str(), &opts).await?
        } else {
            self.get_latest_accessible_object_info_with_idx(bucket, object.as_str(), &opts)
                .await?
                .0
        };
        opts.precondition_check(&info)?;
        Ok(info)
    }

    #[instrument(skip(self))]
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn handle_copy_object(
        &self,
        src_bucket: &str,
        src_object: &str,
        dst_bucket: &str,
        dst_object: &str,
        src_info: &mut ObjectInfo,
        src_opts: &ObjectOptions,
        dst_opts: &ObjectOptions,
    ) -> Result<ObjectInfo> {
        check_copy_obj_args(src_bucket, src_object)?;
        check_copy_obj_args(dst_bucket, dst_object)?;

        let src_object = encode_dir_object(src_object);
        let dst_object = encode_dir_object(dst_object);

        let cp_src_dst_same = path_join_buf(&[src_bucket, &src_object]) == path_join_buf(&[dst_bucket, &dst_object]);

        let mut dst_opts = dst_opts.clone();
        let _dst_lock_guard = if cp_src_dst_same {
            self.acquire_object_write_lock_if_needed("copy_object", dst_bucket, &dst_object, &mut dst_opts)
                .await?
        } else {
            None
        };

        if cp_src_dst_same {
            let pool_idx = self
                .get_pool_info_existing_with_opts(src_bucket, &src_object, &version_aware_lookup_opts(src_opts, true))
                .await?
                .0
                .index;

            if let (Some(src_vid), Some(dst_vid)) = (&src_opts.version_id, &dst_opts.version_id)
                && src_vid == dst_vid
            {
                return self.pools[pool_idx]
                    .copy_object(src_bucket, &src_object, dst_bucket, &dst_object, src_info, src_opts, &dst_opts)
                    .await;
            }

            if !dst_opts.versioned && src_opts.version_id.is_none() {
                if src_info.metadata_only {
                    return self.pools[pool_idx]
                        .copy_object(src_bucket, &src_object, dst_bucket, &dst_object, src_info, src_opts, &dst_opts)
                        .await;
                }
                // Transitioned object self-copy: restore from tier into the same pool.
                let put_opts = ObjectOptions {
                    user_defined: (*src_info.user_defined).clone(),
                    versioned: dst_opts.versioned,
                    version_id: dst_opts.version_id.clone(),
                    no_lock: dst_opts.no_lock,
                    mod_time: dst_opts.mod_time,
                    http_preconditions: dst_opts.http_preconditions.clone(),
                    ..Default::default()
                };
                return if let Some(reader) = src_info.put_object_reader.as_mut() {
                    self.pools[pool_idx]
                        .put_object(dst_bucket, &dst_object, reader, &put_opts)
                        .await
                } else {
                    Err(StorageError::InvalidArgument(
                        src_bucket.to_owned(),
                        src_object.to_owned(),
                        "put_object_reader is none".to_owned(),
                    ))
                };
            }

            if dst_opts.versioned && src_opts.version_id != dst_opts.version_id {
                // Restoring a specific historical version onto the current key creates a NEW
                // version. When the caller supplies a reader (S3 CopyObject), write the fetched
                // bytes through put_object so any re-encryption/compression applied to the reader
                // stays consistent with the new version's metadata. Sharing the source data_dir via
                // a metadata-only version copy would corrupt SSE/compressed objects (issue #4238).
                if let Some(reader) = src_info.put_object_reader.as_mut() {
                    let put_opts = ObjectOptions {
                        user_defined: (*src_info.user_defined).clone(),
                        versioned: dst_opts.versioned,
                        version_id: dst_opts.version_id.clone(),
                        no_lock: dst_opts.no_lock,
                        mod_time: dst_opts.mod_time,
                        http_preconditions: dst_opts.http_preconditions.clone(),
                        ..Default::default()
                    };
                    return self.pools[pool_idx]
                        .put_object(dst_bucket, &dst_object, reader, &put_opts)
                        .await;
                }
                src_info.version_only = true;
                return self.pools[pool_idx]
                    .copy_object(src_bucket, &src_object, dst_bucket, &dst_object, src_info, src_opts, &dst_opts)
                    .await;
            }
        }

        let pool_idx = if dst_opts.no_lock {
            self.get_pool_idx_no_lock(dst_bucket, &dst_object, src_info.size).await?
        } else {
            self.get_pool_idx(dst_bucket, &dst_object, src_info.size).await?
        };

        let put_opts = ObjectOptions {
            user_defined: (*src_info.user_defined).clone(),
            versioned: dst_opts.versioned,
            version_id: dst_opts.version_id.clone(),
            no_lock: dst_opts.no_lock,
            mod_time: dst_opts.mod_time,
            http_preconditions: dst_opts.http_preconditions.clone(),
            ..Default::default()
        };

        if let Some(put_object_reader) = src_info.put_object_reader.as_mut() {
            return self.pools[pool_idx]
                .put_object(dst_bucket, &dst_object, put_object_reader, &put_opts)
                .await;
        }

        Err(StorageError::InvalidArgument(
            src_bucket.to_owned(),
            src_object.to_owned(),
            "put_object_reader is none".to_owned(),
        ))
    }

    /// Best-effort purge of an orphan directory prefix — an on-disk tree of empty
    /// directories with no `xl.meta` anywhere (issue #4189). Orphan fragments can sit
    /// on any erasure set of any pool (they are left behind by whichever sets stored
    /// the now-deleted children), so every set is swept. Returns true when at least
    /// one set removed an orphan tree. Hard per-set failures are logged and skipped:
    /// the caller falls back to surfacing the original NotFound.
    async fn purge_orphan_dir_object(&self, bucket: &str, object: &str) -> bool {
        let prefix = decode_dir_object(object);
        let mut purged = false;
        for pool in self.pools.iter() {
            for set in pool.disk_set.iter() {
                match set.purge_orphan_dir_object(bucket, &prefix).await {
                    Ok(set_purged) => purged |= set_purged,
                    Err(err) => {
                        warn!(
                            bucket,
                            prefix,
                            pool_index = pool.pool_idx,
                            error = ?err,
                            "failed to purge orphan directory prefix"
                        );
                    }
                }
            }
        }
        purged
    }

    #[instrument(skip(self))]
    pub(super) async fn handle_delete_object(&self, bucket: &str, object: &str, opts: ObjectOptions) -> Result<ObjectInfo> {
        check_del_obj_args(bucket, object)?;

        let object = if opts.delete_prefix && !opts.delete_prefix_object {
            object.to_owned()
        } else {
            encode_dir_object(object)
        };
        let object = object.as_str();
        let mut opts = opts;

        if opts.delete_prefix && !opts.delete_prefix_object {
            // Prefix deletes cover multiple object keys; an exact lock on the prefix string
            // would not protect child objects.
            self.delete_prefix(bucket, object, &opts).await?;
            return Ok(ObjectInfo::default());
        }

        let _object_lock_guard = self
            .acquire_object_write_lock_if_needed("delete_object", bucket, object, &mut opts)
            .await?;

        if opts.delete_prefix {
            self.delete_prefix(bucket, object, &opts).await?;
            return Ok(ObjectInfo::default());
        }

        let gopts = version_aware_lookup_opts(&opts, true);

        if opts.data_movement {
            let existing_pool_info = self.get_pool_info_existing_with_opts(bucket, object, &gopts).await;
            let existing_pool_idx = existing_pool_info
                .as_ref()
                .map(|(pinfo, _)| pinfo.index)
                .map_err(Clone::clone);
            let selected_target_pool_idx =
                match select_data_movement_target_pool(existing_pool_idx, opts.src_pool_idx, opts.delete_marker)? {
                    Some(pool_idx) => pool_idx,
                    None => self.get_pool_idx_no_lock(bucket, object, 0).await?,
                };
            let resume_target_pool_idx = if selected_target_pool_idx == opts.src_pool_idx {
                self.get_available_pool_idx_excluding(bucket, object, 0, opts.src_pool_idx)
                    .await
            } else {
                None
            };
            let target_pool_idx =
                resolve_data_movement_resume_target_pool(selected_target_pool_idx, resume_target_pool_idx, opts.src_pool_idx);

            if !should_check_data_movement_resume_target(opts.src_pool_idx, target_pool_idx) {
                if let Ok((source_pool_info, _)) = existing_pool_info
                    && opts.delete_marker
                    && is_data_movement_delete_marker(&source_pool_info.object_info)
                    && self
                        .has_equivalent_data_movement_delete_marker(
                            bucket,
                            object,
                            &source_pool_info.object_info,
                            &opts,
                            target_pool_idx,
                        )
                        .await?
                {
                    let mut obj = source_pool_info.object_info;
                    obj.name = decode_dir_object(object);
                    return Ok(obj);
                }

                return Err(StorageError::DataMovementOverwriteErr(
                    bucket.to_owned(),
                    object.to_owned(),
                    opts.version_id.unwrap_or_default(),
                ));
            }

            let mut obj = self.pools[target_pool_idx].delete_object(bucket, object, opts).await?;
            obj.name = decode_dir_object(obj.name.as_str());
            return Ok(obj);
        }

        // Determine which pool contains it
        let (mut pinfo, errs) = match self.get_pool_info_existing_with_opts(bucket, object, &gopts).await {
            Ok(res) => res,
            Err(err) if is_err_read_quorum(&err) => return Err(StorageError::ErasureWriteQuorum),
            Err(err) if is_err_object_not_found(&err) && should_create_delete_marker_for_missing_object(&opts) => {
                let target_pool_idx = self.get_pool_idx_no_lock(bucket, object, 0).await?;
                let mut obj = self.pools[target_pool_idx].delete_object(bucket, object, opts).await?;
                obj.name = decode_dir_object(object);
                return Ok(obj);
            }
            Err(err) => {
                // A folder key (`prefix/`) with no object metadata may still exist on
                // disk as an orphan empty-directory tree (issue #4189): listings show
                // it as a common prefix, but no regular delete path can remove it.
                // Purge the orphan tree so folder deletes actually take effect.
                if should_purge_orphan_dir_on_missing(&err, object) && self.purge_orphan_dir_object(bucket, object).await {
                    return Ok(ObjectInfo {
                        bucket: bucket.to_owned(),
                        name: decode_dir_object(object),
                        ..Default::default()
                    });
                }
                return Err(err);
            }
        };

        if pinfo.object_info.delete_marker && opts.version_id.is_none() {
            pinfo.object_info.name = decode_dir_object(object);
            return Ok(pinfo.object_info);
        }

        if opts.data_movement && opts.src_pool_idx == pinfo.index {
            return Err(StorageError::DataMovementOverwriteErr(
                bucket.to_owned(),
                object.to_owned(),
                opts.version_id.unwrap_or_default(),
            ));
        }

        if !errs.is_empty() && !opts.versioned && !opts.version_suspended {
            let mut obj = self.delete_object_from_all_pools(bucket, object, &opts, errs).await?;
            obj.name = decode_dir_object(object);
            return Ok(obj);
        }

        for pool in self.pools.iter() {
            match pool.delete_object(bucket, object, opts.clone()).await {
                Ok(res) => {
                    let mut obj = res;
                    obj.name = decode_dir_object(object);
                    return Ok(obj);
                }
                Err(err) => {
                    if !is_err_object_not_found(&err) && !is_err_version_not_found(&err) {
                        return Err(err);
                    }
                }
            }
        }

        if let Some(ver) = opts.version_id {
            return Err(StorageError::VersionNotFound(bucket.to_owned(), object.to_owned(), ver));
        }

        Err(StorageError::ObjectNotFound(bucket.to_owned(), object.to_owned()))
    }

    #[instrument(skip(self))]
    pub(super) async fn handle_delete_objects(
        &self,
        bucket: &str,
        objects: Vec<ObjectToDelete>,
        opts: ObjectOptions,
    ) -> (Vec<DeletedObject>, Vec<Option<Error>>) {
        // encode object name
        let objects: Vec<ObjectToDelete> = objects
            .iter()
            .map(|v| {
                let mut v = v.clone();
                v.object_name = encode_dir_object(v.object_name.as_str());
                v
            })
            .collect();

        // Default return value
        let mut del_objects = vec![DeletedObject::default(); objects.len()];

        let mut del_errs = Vec::with_capacity(objects.len());
        for _ in 0..objects.len() {
            del_errs.push(None)
        }

        let mut opts = opts;
        let _object_lock_guards = match self.acquire_delete_objects_write_locks(bucket, &objects, &mut opts).await {
            Ok(guards) => guards,
            Err(err) => return return_batch_delete_lock_error(objects.as_slice(), err),
        };

        let mut futures = Vec::with_capacity(self.pools.len());

        for pool in self.pools.iter() {
            futures.push(pool.delete_objects(bucket, objects.clone(), opts.clone()));
        }

        let results = join_all(futures).await;

        for idx in 0..del_objects.len() {
            for (dels, errs) in results.iter() {
                if errs[idx].is_none() && dels[idx].found {
                    del_errs[idx] = None;
                    del_objects[idx] = dels[idx].clone();
                    break;
                }

                if del_errs[idx].is_none() {
                    del_errs[idx] = errs[idx].clone();
                    del_objects[idx] = dels[idx].clone();
                }
            }
        }

        del_objects.iter_mut().for_each(|v| {
            v.object_name = decode_dir_object(&v.object_name);
        });

        (del_objects, del_errs)

        // let mut futures = Vec::with_capacity(objects.len());

        // for obj in objects.iter() {
        //     futures.push(async move {
        //         self.internal_get_pool_info_existing_with_opts(
        //             bucket,
        //             &obj.object_name,
        //             &ObjectOptions {
        //                 no_lock: true,
        //                 ..Default::default()
        //             },
        //         )
        //         .await
        //     });
        // }

        // let results = join_all(futures).await;

        // // let mut jhs = Vec::new();
        // // let semaphore = Arc::new(Semaphore::new(num_cpus::get()));
        // // let pools = Arc::new(self.pools.clone());

        // // for obj in objects.iter() {
        // //     let (semaphore, pools, bucket, object_name, opt) = (
        // //         semaphore.clone(),
        // //         pools.clone(),
        // //         bucket.to_string(),
        // //         obj.object_name.to_string(),
        // //         ObjectOptions::default(),
        // //     );

        // //     let jh = tokio::spawn(async move {
        // //         let _permit = semaphore.acquire().await.unwrap();
        // //         self.internal_get_pool_info_existing_with_opts(pools.as_ref(), &bucket, &object_name, &opt)
        // //             .await
        // //     });
        // //     jhs.push(jh);
        // // }
        // // let mut results = Vec::new();
        // // for jh in jhs {
        // //     results.push(jh.await.unwrap());
        // // }

        // // Record the mapping pool_idx -> object index
        // let mut pool_obj_idx_map = HashMap::new();
        // let mut orig_index_map = HashMap::new();

        // for (i, res) in results.into_iter().enumerate() {
        //     match res {
        //         Ok((pinfo, _)) => {
        //             if let Some(obj) = objects.get(i) {
        //                 if pinfo.object_info.delete_marker && obj.version_id.is_none() {
        //                     del_objects[i] = DeletedObject {
        //                         delete_marker: pinfo.object_info.delete_marker,
        //                         delete_marker_version_id: pinfo.object_info.version_id.map(|v| v.to_string()),
        //                         object_name: decode_dir_object(&pinfo.object_info.name),
        //                         delete_marker_mtime: pinfo.object_info.mod_time,
        //                         ..Default::default()
        //                     };
        //                     continue;
        //                 }

        //                 if !pool_obj_idx_map.contains_key(&pinfo.index) {
        //                     pool_obj_idx_map.insert(pinfo.index, vec![obj.clone()]);
        //                 } else if let Some(val) = pool_obj_idx_map.get_mut(&pinfo.index) {
        //                     val.push(obj.clone());
        //                 }

        //                 if !orig_index_map.contains_key(&pinfo.index) {
        //                     orig_index_map.insert(pinfo.index, vec![i]);
        //                 } else if let Some(val) = orig_index_map.get_mut(&pinfo.index) {
        //                     val.push(i);
        //                 }
        //             }
        //         }
        //         Err(e) => {
        //             if !is_err_object_not_found(&e) && is_err_version_not_found(&e) {
        //                 del_errs[i] = Some(e)
        //             }

        //             if let Some(obj) = objects.get(i) {
        //                 del_objects[i] = DeletedObject {
        //                     object_name: decode_dir_object(&obj.object_name),
        //                     version_id: obj.version_id.map(|v| v.to_string()),
        //                     ..Default::default()
        //                 }
        //             }
        //         }
        //     }
        // }

        // if !pool_obj_idx_map.is_empty() {
        //     for (i, sets) in self.pools.iter().enumerate() {
        //         // Retrieve the object index for a pool idx
        //         if let Some(objs) = pool_obj_idx_map.get(&i) {
        //             // Fetch the corresponding object (should never be None)
        //             // let objs: Vec<ObjectToDelete> = obj_idxs.iter().filter_map(|&idx| objects.get(idx).cloned()).collect();

        //             if objs.is_empty() {
        //                 continue;
        //             }

        //             let (pdel_objs, perrs) = sets.delete_objects(bucket, objs.clone(), opts.clone()).await?;

        //             // Insert simultaneously (should never be None)
        //             let org_indexes = orig_index_map.get(&i).unwrap();

        //             // perrs should follow the same order as obj_idxs
        //             for (i, err) in perrs.into_iter().enumerate() {
        //                 let obj_idx = org_indexes[i];

        //                 if err.is_some() {
        //                     del_errs[obj_idx] = err;
        //                 }

        //                 let mut dobj = pdel_objs.get(i).unwrap().clone();
        //                 dobj.object_name = decode_dir_object(&dobj.object_name);

        //                 del_objects[obj_idx] = dobj;
        //             }
        //         }
        //     }
        // }

        // Ok((del_objects, del_errs))
    }

    #[instrument(skip(self))]
    pub(super) async fn handle_add_partial(&self, bucket: &str, object: &str, version_id: &str) -> Result<()> {
        let object = encode_dir_object(object);

        if self.single_pool() {
            let _ = self.pools[0].add_partial(bucket, object.as_str(), version_id).await;
            return Ok(());
        }

        let opts = ObjectOptions {
            version_id: Some(version_id.to_string()),
            ..Default::default()
        };
        let (_, idx) = self
            .get_latest_accessible_object_info_with_idx(bucket, object.as_str(), &opts)
            .await?;

        let _ = self.pools[idx].add_partial(bucket, object.as_str(), version_id).await;
        Ok(())
    }

    #[instrument(skip(self))]
    pub(super) async fn handle_transition_object(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<()> {
        let object = encode_dir_object(object);
        if self.single_pool() {
            return self.pools[0].transition_object(bucket, &object, opts).await;
        }

        let opts = transition_restore_pool_opts(opts);
        let (_, idx) = self
            .get_latest_accessible_object_info_with_idx(bucket, &object, &opts)
            .await?;

        self.pools[idx].transition_object(bucket, &object, &opts).await
    }

    #[instrument(skip(self))]
    pub(super) async fn handle_restore_transitioned_object(
        self: Arc<Self>,
        bucket: &str,
        object: &str,
        opts: &ObjectOptions,
    ) -> Result<()> {
        let object = encode_dir_object(object);
        if self.single_pool() {
            return self.pools[0].clone().restore_transitioned_object(bucket, &object, opts).await;
        }

        let opts = transition_restore_pool_opts(opts);
        let (_, idx) = self
            .get_latest_accessible_object_info_with_idx(bucket, object.as_str(), &opts)
            .await?;

        self.pools[idx]
            .clone()
            .restore_transitioned_object(bucket, &object, &opts)
            .await
    }

    #[instrument(skip(self))]
    pub(super) async fn handle_put_object_metadata(
        &self,
        bucket: &str,
        object: &str,
        opts: &ObjectOptions,
    ) -> Result<ObjectInfo> {
        let object = encode_dir_object(object);
        if self.single_pool() {
            return self.pools[0].put_object_metadata(bucket, object.as_str(), opts).await;
        }

        let mut opts = opts.clone();
        opts.metadata_chg = true;

        let (_, idx) = self
            .get_latest_accessible_object_info_with_idx(bucket, object.as_str(), &opts)
            .await?;

        self.pools[idx].put_object_metadata(bucket, object.as_str(), &opts).await
    }

    #[instrument(skip(self))]
    pub(super) async fn handle_get_object_tags(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<String> {
        let object = encode_dir_object(object);

        if self.single_pool() {
            return self.pools[0].get_object_tags(bucket, object.as_str(), opts).await;
        }

        let (oi, _) = self.get_latest_accessible_object_info_with_idx(bucket, &object, opts).await?;
        Ok((*oi.user_tags).clone())
    }

    #[instrument(level = "debug", skip(self))]
    pub(super) async fn handle_put_object_tags(
        &self,
        bucket: &str,
        object: &str,
        tags: &str,
        opts: &ObjectOptions,
    ) -> Result<ObjectInfo> {
        let object = encode_dir_object(object);

        if self.single_pool() {
            return self.pools[0].put_object_tags(bucket, object.as_str(), tags, opts).await;
        }

        let (_, idx) = self
            .get_latest_accessible_object_info_with_idx(bucket, object.as_str(), opts)
            .await?;

        self.pools[idx].put_object_tags(bucket, object.as_str(), tags, opts).await
    }

    #[instrument(skip(self))]
    pub(super) async fn handle_delete_object_version(
        &self,
        bucket: &str,
        object: &str,
        fi: &FileInfo,
        force_del_marker: bool,
    ) -> Result<()> {
        check_del_obj_args(bucket, object)?;

        let object = encode_dir_object(object);

        if self.single_pool() {
            return self.pools[0]
                .delete_object_version(bucket, object.as_str(), fi, force_del_marker)
                .await;
        }
        Err(StorageError::NotImplemented)
    }

    #[instrument(skip(self))]
    pub(super) async fn handle_delete_object_tags(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<ObjectInfo> {
        let object = encode_dir_object(object);

        if self.single_pool() {
            return self.pools[0].delete_object_tags(bucket, object.as_str(), opts).await;
        }

        let (_, idx) = self
            .get_latest_accessible_object_info_with_idx(bucket, object.as_str(), opts)
            .await?;

        self.pools[idx].delete_object_tags(bucket, object.as_str(), opts).await
    }

    pub(super) async fn handle_verify_object_integrity(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<()> {
        let get_object_reader = <Self as crate::storage_api_contracts::object::ObjectIO>::get_object_reader(
            self,
            bucket,
            object,
            None,
            HeaderMap::new(),
            opts,
        )
        .await?;
        // Stream to sink to avoid loading entire object into memory during verification
        let mut reader = get_object_reader.stream;
        tokio::io::copy(&mut reader, &mut tokio::io::sink()).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bucket::lifecycle::core::TRANSITION_COMPLETE;
    use crate::bucket::replication::{
        ReplicationState, ReplicationStatusType, VersionPurgeStatusType, replication_state_to_filemeta, replication_statuses_map,
        version_purge_statuses_map,
    };
    use crate::layout::{
        endpoints::{Endpoints, PoolEndpoints},
        format::FormatV3,
    };
    use bytes::Bytes;
    use std::io::Cursor;
    use std::sync::Arc;
    use tokio::io::AsyncReadExt;

    #[test]
    fn delete_marker_data_movement_falls_back_when_only_source_pool_has_object() {
        let target = select_data_movement_target_pool(Ok(1), 1, true).unwrap();
        assert_eq!(target, None);
    }

    #[test]
    fn delete_marker_data_movement_falls_back_when_version_does_not_exist_yet() {
        let err = StorageError::ObjectNotFound("bucket".to_string(), "object".to_string());
        let target = select_data_movement_target_pool(Err(err), 1, true).unwrap();
        assert_eq!(target, None);
    }

    #[test]
    fn non_delete_marker_data_movement_keeps_existing_pool() {
        let target = select_data_movement_target_pool(Ok(0), 1, false).unwrap();
        assert_eq!(target, Some(0));
    }

    #[test]
    fn equivalent_data_movement_delete_marker_requires_same_version_and_mod_time() {
        let version_id = Uuid::nil();
        let mod_time = OffsetDateTime::UNIX_EPOCH;
        let source = ObjectInfo {
            version_id: Some(version_id),
            delete_marker: true,
            mod_time: Some(mod_time),
            ..Default::default()
        };
        let target = source.clone();

        assert!(is_equivalent_data_movement_delete_marker(&source, &target));

        let mismatched = ObjectInfo {
            mod_time: Some(mod_time + Duration::from_secs(1)),
            ..target
        };
        assert!(!is_equivalent_data_movement_delete_marker(&source, &mismatched));
    }

    #[test]
    fn equivalent_data_movement_delete_marker_rejects_metadata_and_replication_mismatch() {
        let version_id = Uuid::nil();
        let mod_time = OffsetDateTime::UNIX_EPOCH;
        let source = ObjectInfo {
            version_id: Some(version_id),
            delete_marker: true,
            mod_time: Some(mod_time),
            user_defined: Arc::new(HashMap::from([("x-amz-meta-source".to_string(), "true".to_string())])),
            replication_status_internal: Some("arn:minio:replication:target=COMPLETED;".to_string()),
            version_purge_status_internal: Some("arn:minio:replication:target=PENDING;".to_string()),
            ..Default::default()
        };

        let mut target = source.clone();
        target.user_defined = Arc::new(HashMap::from([("x-amz-meta-source".to_string(), "false".to_string())]));
        assert!(!is_equivalent_data_movement_delete_marker(&source, &target));

        let mut target = source.clone();
        target.replication_status_internal = Some("arn:minio:replication:target=FAILED;".to_string());
        assert!(!is_equivalent_data_movement_delete_marker(&source, &target));

        let mut target = source.clone();
        target.version_purge_status_internal = Some("arn:minio:replication:target=COMPLETE;".to_string());
        assert!(!is_equivalent_data_movement_delete_marker(&source, &target));
    }

    #[test]
    fn equivalent_data_movement_delete_marker_rejects_live_object() {
        let source = ObjectInfo {
            delete_marker: true,
            ..Default::default()
        };
        let target = ObjectInfo {
            delete_marker: false,
            ..source.clone()
        };

        assert!(!is_equivalent_data_movement_delete_marker(&source, &target));
    }

    #[test]
    fn data_movement_delete_marker_resume_accepts_equivalent_target() {
        let source = ObjectInfo {
            version_id: Some(Uuid::nil()),
            delete_marker: true,
            mod_time: Some(OffsetDateTime::UNIX_EPOCH),
            ..Default::default()
        };

        let should_resume = resolve_data_movement_delete_marker_resume_result(Ok(Some(source.clone())), &source, 0, 1)
            .expect("equivalent delete marker target should be evaluated");

        assert!(should_resume);
    }

    #[test]
    fn data_movement_delete_marker_resume_rejects_source_pool_target() {
        let source = ObjectInfo {
            version_id: Some(Uuid::nil()),
            delete_marker: true,
            mod_time: Some(OffsetDateTime::UNIX_EPOCH),
            ..Default::default()
        };

        let should_resume = resolve_data_movement_delete_marker_resume_result(Ok(Some(source.clone())), &source, 0, 0)
            .expect("source-pool target should be rejected before target lookup");

        assert!(!should_resume);
    }

    #[test]
    fn data_movement_resume_target_prefers_selected_non_source_pool() {
        let target_pool_idx = resolve_data_movement_resume_target_pool(2, Some(3), 1);
        assert_eq!(target_pool_idx, 2);
    }

    #[test]
    fn data_movement_resume_target_uses_resolved_non_source_pool_when_selected_is_source() {
        let target_pool_idx = resolve_data_movement_resume_target_pool(1, Some(3), 1);
        assert_eq!(target_pool_idx, 3);
        assert!(should_check_data_movement_resume_target(1, target_pool_idx));
    }

    #[test]
    fn data_movement_resume_target_keeps_source_when_no_other_pool_is_available() {
        let target_pool_idx = resolve_data_movement_resume_target_pool(1, None, 1);
        assert_eq!(target_pool_idx, 1);
    }

    #[test]
    fn data_movement_delete_marker_resume_propagates_target_lookup_error() {
        let source = ObjectInfo {
            delete_marker: true,
            ..Default::default()
        };
        let result = resolve_data_movement_delete_marker_resume_result(Err(Error::SlowDown), &source, 0, 1);

        assert!(matches!(result, Err(Error::SlowDown)));
    }

    fn tiered_equivalence_source() -> FileInfo {
        let version_id = Uuid::nil();
        let transition_version_id = Uuid::new_v4();
        let mod_time = OffsetDateTime::UNIX_EPOCH;

        FileInfo {
            version_id: Some(version_id),
            size: 1024,
            mod_time: Some(mod_time),
            checksum: Some(Bytes::from_static(b"checksum")),
            transition_status: TRANSITION_COMPLETE.to_string(),
            transitioned_objname: "remote/object".to_string(),
            transition_tier: "WARM".to_string(),
            transition_version_id: Some(transition_version_id),
            replication_state_internal: Some(replication_state_to_filemeta(&ReplicationState {
                replication_status_internal: Some("arn:minio:replication:target=COMPLETED;".to_string()),
                targets: replication_statuses_map("arn:minio:replication:target=COMPLETED;"),
                version_purge_status_internal: Some("arn:minio:replication:target=PENDING;".to_string()),
                purge_targets: version_purge_statuses_map("arn:minio:replication:target=PENDING;"),
                ..Default::default()
            })),
            metadata: HashMap::from([
                ("etag".to_string(), "etag-value".to_string()),
                ("x-amz-meta-key".to_string(), "metadata-value".to_string()),
                (rustfs_utils::http::AMZ_OBJECT_TAGGING.to_string(), "tag=value".to_string()),
                ("expires".to_string(), "1970-01-01T00:33:20Z".to_string()),
            ]),
            ..Default::default()
        }
    }

    fn tiered_equivalence_target(source: &FileInfo) -> ObjectInfo {
        ObjectInfo::from_file_info(source, "bucket", "object", source.version_id.is_some())
    }

    #[test]
    fn equivalent_data_movement_tiered_object_accepts_matching_persisted_metadata() {
        let source = tiered_equivalence_source();
        let target = tiered_equivalence_target(&source);

        assert!(is_equivalent_data_movement_tiered_object(&source, &target));
    }

    #[test]
    fn equivalent_data_movement_tiered_object_rejects_transition_mismatch() {
        let source = tiered_equivalence_source();
        let mut target = tiered_equivalence_target(&source);
        target.transitioned_object.name = "remote/target".to_string();

        assert!(!is_equivalent_data_movement_tiered_object(&source, &target));
    }

    #[test]
    fn equivalent_data_movement_tiered_object_rejects_user_metadata_mismatch() {
        let source = tiered_equivalence_source();
        let mut target = tiered_equivalence_target(&source);
        target.user_defined = Arc::new(HashMap::from([("x-amz-meta-key".to_string(), "target-value".to_string())]));

        assert!(!is_equivalent_data_movement_tiered_object(&source, &target));
    }

    #[test]
    fn equivalent_data_movement_tiered_object_rejects_tag_mismatch() {
        let source = tiered_equivalence_source();
        let mut target = tiered_equivalence_target(&source);
        target.user_tags = Arc::new("tag=target".to_string());

        assert!(!is_equivalent_data_movement_tiered_object(&source, &target));
    }

    #[test]
    fn equivalent_data_movement_tiered_object_rejects_replication_mismatch() {
        let source = tiered_equivalence_source();
        let mut target = tiered_equivalence_target(&source);
        target.replication_status_internal = Some("arn:minio:replication:target=FAILED;".to_string());
        target.replication_status = ReplicationStatusType::Failed;

        assert!(!is_equivalent_data_movement_tiered_object(&source, &target));
    }

    #[test]
    fn equivalent_data_movement_tiered_object_rejects_version_purge_mismatch() {
        let source = tiered_equivalence_source();
        let mut target = tiered_equivalence_target(&source);
        target.version_purge_status_internal = Some("arn:minio:replication:target=COMPLETE;".to_string());
        target.version_purge_status = VersionPurgeStatusType::Complete;

        assert!(!is_equivalent_data_movement_tiered_object(&source, &target));
    }

    #[test]
    fn data_movement_tiered_resume_accepts_equivalent_target() {
        let source = tiered_equivalence_source();
        let target = tiered_equivalence_target(&source);

        let should_resume = resolve_data_movement_tiered_resume_result(Ok(Some(target)), &source, 0, 1)
            .expect("equivalent tiered target should be evaluated");

        assert!(should_resume);
    }

    #[test]
    fn data_movement_tiered_resume_rejects_source_pool_target() {
        let source = tiered_equivalence_source();
        let target = tiered_equivalence_target(&source);

        let should_resume = resolve_data_movement_tiered_resume_result(Ok(Some(target)), &source, 0, 0)
            .expect("source-pool target should be rejected before target lookup");

        assert!(!should_resume);
    }

    #[test]
    fn data_movement_tiered_resume_rejects_missing_target() {
        let source = FileInfo {
            version_id: Some(Uuid::nil()),
            size: 1024,
            ..Default::default()
        };

        let should_resume = resolve_data_movement_tiered_resume_result(Ok(None), &source, 0, 1)
            .expect("missing tiered target should be evaluated");

        assert!(!should_resume);
    }

    #[test]
    fn latest_object_access_delete_marker_error_returns_none_for_live_object() {
        let info = ObjectInfo::default();
        let opts = ObjectOptions::default();

        assert!(latest_object_access_delete_marker_error("bucket", "object", &info, &opts).is_none());
    }

    #[test]
    fn latest_object_access_delete_marker_error_returns_not_found_without_version_id() {
        let info = ObjectInfo {
            delete_marker: true,
            ..Default::default()
        };
        let opts = ObjectOptions::default();

        let err = latest_object_access_delete_marker_error("bucket", "object", &info, &opts)
            .expect("delete marker should stop latest-object reads");

        assert!(crate::error::is_err_object_not_found(&err));
    }

    #[test]
    fn latest_object_access_delete_marker_error_returns_method_not_allowed_for_version_read() {
        let info = ObjectInfo {
            delete_marker: true,
            ..Default::default()
        };
        let opts = ObjectOptions {
            version_id: Some("vid-1".to_string()),
            ..Default::default()
        };

        let err = latest_object_access_delete_marker_error("bucket", "object", &info, &opts)
            .expect("delete marker version reads should be rejected");

        assert!(matches!(err, Error::MethodNotAllowed));
    }

    #[test]
    fn latest_object_access_delete_marker_error_returns_not_found_for_delete_marker_lookup() {
        let info = ObjectInfo {
            delete_marker: true,
            ..Default::default()
        };
        let opts = ObjectOptions {
            version_id: Some("vid-1".to_string()),
            delete_marker: true,
            ..Default::default()
        };

        let err = latest_object_access_delete_marker_error("bucket", "object", &info, &opts)
            .expect("delete marker lookup should keep not-found semantics");

        assert!(crate::error::is_err_object_not_found(&err));
    }

    #[test]
    fn resolve_latest_object_access_returns_live_object_and_pool_idx() {
        let info = ObjectInfo::default();
        let opts = ObjectOptions::default();

        let (resolved, idx) = resolve_latest_object_access("bucket", "object", info, 7, &opts).unwrap();

        assert_eq!(idx, 7);
        assert!(!resolved.delete_marker);
    }

    #[test]
    fn resolve_latest_object_access_rejects_delete_marker_without_version_id() {
        let info = ObjectInfo {
            delete_marker: true,
            ..Default::default()
        };
        let opts = ObjectOptions::default();

        let err = resolve_latest_object_access("bucket", "object", info, 2, &opts).unwrap_err();

        assert!(crate::error::is_err_object_not_found(&err));
    }

    #[test]
    fn resolve_latest_object_access_rejects_delete_marker_version_read() {
        let info = ObjectInfo {
            delete_marker: true,
            ..Default::default()
        };
        let opts = ObjectOptions {
            version_id: Some("vid-1".to_string()),
            ..Default::default()
        };

        let err = resolve_latest_object_access("bucket", "object", info, 2, &opts).unwrap_err();

        assert!(matches!(err, Error::MethodNotAllowed));
    }

    #[test]
    fn should_create_delete_marker_for_missing_object_allows_latest_versioned_delete() {
        let opts = ObjectOptions {
            versioned: true,
            ..Default::default()
        };

        assert!(should_create_delete_marker_for_missing_object(&opts));
    }

    #[test]
    fn should_create_delete_marker_for_missing_object_rejects_specialized_deletes() {
        let version_delete = ObjectOptions {
            versioned: true,
            version_id: Some("vid-1".to_string()),
            ..Default::default()
        };
        let delete_marker_replication = ObjectOptions {
            versioned: true,
            delete_marker: true,
            ..Default::default()
        };
        let data_movement = ObjectOptions {
            versioned: true,
            data_movement: true,
            ..Default::default()
        };

        assert!(!should_create_delete_marker_for_missing_object(&version_delete));
        assert!(!should_create_delete_marker_for_missing_object(&delete_marker_replication));
        assert!(!should_create_delete_marker_for_missing_object(&data_movement));
    }

    // issue #4189 regression: `del_opts` pins `version_id = Uuid::nil()` on directory
    // keys, so deleting a ghost folder over HTTP fails the lookup with *version*-not-found
    // (not object-not-found). The orphan-purge guard must accept both misses, or the
    // ghost tree survives behind a fake 204 — the exact reported symptom.
    #[test]
    fn should_purge_orphan_dir_on_version_not_found_for_dir_key() {
        assert!(
            should_purge_orphan_dir_on_missing(&StorageError::FileVersionNotFound, "ghost/"),
            "the real HTTP delete path yields version-not-found on dir keys and must reach the purge"
        );
        assert!(
            should_purge_orphan_dir_on_missing(
                &StorageError::VersionNotFound("bucket".into(), "ghost/".into(), Uuid::nil().to_string()),
                "ghost/"
            ),
            "typed VersionNotFound on a dir key must also reach the purge"
        );
    }

    #[test]
    fn should_purge_orphan_dir_on_object_not_found_for_dir_key() {
        assert!(should_purge_orphan_dir_on_missing(&StorageError::FileNotFound, "ghost/"));
        assert!(should_purge_orphan_dir_on_missing(
            &StorageError::ObjectNotFound("bucket".into(), "ghost/".into()),
            "ghost/"
        ));
    }

    #[test]
    fn should_not_purge_orphan_dir_for_regular_key_or_other_errors() {
        // A regular (non-directory) key must never trigger a prefix purge, even on a miss.
        assert!(!should_purge_orphan_dir_on_missing(&StorageError::FileVersionNotFound, "regular.txt"));
        assert!(!should_purge_orphan_dir_on_missing(&StorageError::FileNotFound, "regular.txt"));
        // Non-miss errors (e.g. quorum failures) must not be masked by a purge attempt.
        assert!(!should_purge_orphan_dir_on_missing(&StorageError::ErasureReadQuorum, "ghost/"));
    }

    #[test]
    fn resolve_decommission_target_pool_idx_result_passthrough_ok() {
        let idx = ECStore::resolve_decommission_target_pool_idx_result(Ok(3), "bucket", "object").unwrap();

        assert_eq!(idx, 3);
    }

    #[test]
    fn resolve_decommission_target_pool_idx_result_wraps_error_context() {
        let err = ECStore::resolve_decommission_target_pool_idx_result(Err(Error::other("boom")), "bucket", "object")
            .expect_err("expected contextual error");
        let rendered = err.to_string();

        assert!(rendered.contains("failed to select decommission target pool"), "{rendered}");
        assert!(rendered.contains("bucket"), "{rendered}");
        assert!(rendered.contains("object"), "{rendered}");
        assert!(rendered.contains("boom"), "{rendered}");
    }

    #[test]
    fn resolve_decommission_tiered_object_result_passthrough_ok() {
        ECStore::resolve_decommission_tiered_object_result(Ok(()), "bucket", "object")
            .expect("successful decommission result should pass through");
    }

    #[test]
    fn resolve_decommission_tiered_object_result_wraps_error_context() {
        let err = ECStore::resolve_decommission_tiered_object_result(Err(Error::other("boom")), "bucket", "object")
            .expect_err("expected contextual error");
        let rendered = err.to_string();

        assert!(rendered.contains("failed to decommission tiered object"), "{rendered}");
        assert!(rendered.contains("bucket"), "{rendered}");
        assert!(rendered.contains("object"), "{rendered}");
        assert!(rendered.contains("boom"), "{rendered}");
    }

    #[test]
    fn version_aware_lookup_opts_enables_version_aware_lookup() {
        let opts = ObjectOptions {
            version_id: Some("vid-1".to_string()),
            ..Default::default()
        };

        let lookup_opts = version_aware_lookup_opts(&opts, true);

        assert!(lookup_opts.no_lock);
        assert!(lookup_opts.metadata_chg);
        assert_eq!(lookup_opts.version_id.as_deref(), Some("vid-1"));
    }

    #[test]
    fn version_aware_lookup_opts_keeps_latest_lookup_for_unversioned_requests() {
        let lookup_opts = version_aware_lookup_opts(&ObjectOptions::default(), true);

        assert!(lookup_opts.no_lock);
        assert!(!lookup_opts.metadata_chg);
        assert!(lookup_opts.version_id.is_none());
    }

    #[test]
    fn data_movement_pool_lookup_opts_enables_version_aware_lookup_and_skip_flags() {
        let opts = ObjectOptions {
            version_id: Some("vid-1".to_string()),
            ..Default::default()
        };

        let lookup_opts = data_movement_pool_lookup_opts(&opts, false);

        assert!(!lookup_opts.no_lock);
        assert!(lookup_opts.metadata_chg);
        assert!(lookup_opts.skip_decommissioned);
        assert!(lookup_opts.skip_rebalancing);
        assert_eq!(lookup_opts.version_id.as_deref(), Some("vid-1"));
    }

    #[test]
    fn data_movement_pool_lookup_opts_keeps_no_lock_for_tiered_moves() {
        let lookup_opts = data_movement_pool_lookup_opts(
            &ObjectOptions {
                version_id: Some("vid-1".to_string()),
                ..Default::default()
            },
            true,
        );

        assert!(lookup_opts.no_lock);
        assert!(lookup_opts.metadata_chg);
        assert!(lookup_opts.skip_decommissioned);
        assert!(lookup_opts.skip_rebalancing);
    }

    #[test]
    fn transition_restore_pool_opts_skips_decommissioned_and_preserves_locking() {
        let lookup_opts = transition_restore_pool_opts(&ObjectOptions {
            no_lock: false,
            skip_decommissioned: false,
            ..Default::default()
        });

        assert!(lookup_opts.skip_decommissioned);
        assert!(!lookup_opts.no_lock);
    }

    #[test]
    fn transition_restore_pool_opts_preserves_existing_no_lock() {
        let lookup_opts = transition_restore_pool_opts(&ObjectOptions {
            no_lock: true,
            ..Default::default()
        });

        assert!(lookup_opts.skip_decommissioned);
        assert!(lookup_opts.no_lock);
    }

    #[test]
    fn delete_objects_lock_names_are_sorted_and_unique() {
        let objects = vec![
            ObjectToDelete {
                object_name: "beta".to_string(),
                ..Default::default()
            },
            ObjectToDelete {
                object_name: "alpha".to_string(),
                ..Default::default()
            },
            ObjectToDelete {
                object_name: "beta".to_string(),
                ..Default::default()
            },
        ];

        assert_eq!(sorted_unique_delete_object_names(&objects), vec!["alpha", "beta"]);
    }

    async fn new_read_lock_test_store() -> ECStore {
        let format = FormatV3::new(1, 2);
        let endpoints = vec![
            Endpoint::try_from("http://127.0.0.1:9000/data0").expect("first endpoint should parse"),
            Endpoint::try_from("http://127.0.0.1:9001/data1").expect("second endpoint should parse"),
        ];
        let pool_endpoints = PoolEndpoints {
            legacy: false,
            set_count: 1,
            drives_per_set: 2,
            endpoints: Endpoints::from(endpoints),
            cmd_line: "read-lock-metadata-cache-safe-test".to_string(),
            platform: "test".to_string(),
        };
        let endpoint_pools = EndpointServerPools::from(vec![pool_endpoints.clone()]);
        let sets = Sets::new(vec![None, None], &pool_endpoints, &format, 0, 1)
            .await
            .expect("test sets should be created with empty disks");

        ECStore {
            id: Uuid::new_v4(),
            disk_map: HashMap::new(),
            pools: vec![sets],
            peer_sys: S3PeerSys::new(&endpoint_pools),
            pool_meta: RwLock::new(PoolMeta::default()),
            rebalance_meta: RwLock::new(None),
            decommission_cancelers: RwLock::new(Vec::new()),
            start_gate: Mutex::new(()),
            pool_meta_save_gate: Mutex::new(()),
        }
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn delete_objects_write_locks_cover_each_unique_object() {
        let store = new_read_lock_test_store().await;
        let objects = vec![
            ObjectToDelete {
                object_name: "beta".to_string(),
                ..Default::default()
            },
            ObjectToDelete {
                object_name: "alpha".to_string(),
                ..Default::default()
            },
            ObjectToDelete {
                object_name: "beta".to_string(),
                ..Default::default()
            },
        ];
        let mut opts = ObjectOptions::default();

        let guards = store
            .acquire_delete_objects_write_locks("bucket", &objects, &mut opts)
            .await
            .expect("delete object locks should be acquired");

        assert_eq!(guards.len(), 2, "duplicate object names should share one namespace lock");
        assert!(opts.no_lock, "set layer should not reacquire locks already held by ECStore");

        let alpha_lock = store
            .handle_new_ns_lock("bucket", "alpha")
            .await
            .expect("alpha namespace lock should be created");
        let err = alpha_lock
            .get_read_lock(Duration::from_millis(20))
            .await
            .expect_err("batch delete write guard should block alpha readers");
        assert!(matches!(err, rustfs_lock::LockError::Timeout { .. }));

        drop(guards);
        alpha_lock
            .get_read_lock(Duration::from_secs(1))
            .await
            .expect("alpha read lock should be available after dropping batch guards");
    }

    #[tokio::test]
    async fn acquired_read_lock_marks_metadata_cache_safe_for_set_layer() {
        let store = new_read_lock_test_store().await;
        let mut opts = ObjectOptions::default();

        let guard = store
            .acquire_object_read_lock_if_needed("get_object", "bucket", "object", &mut opts)
            .await
            .expect("read lock should be acquired");

        assert!(guard.is_some(), "read lock should be held by the outer store layer");
        assert!(opts.no_lock, "set layer should not reacquire the object lock");
        assert!(
            opts.metadata_cache_safe,
            "metadata cache is safe only because the outer store layer acquired the read lock"
        );
    }

    #[tokio::test]
    async fn prelocked_read_request_does_not_mark_metadata_cache_safe() {
        let store = new_read_lock_test_store().await;
        let mut opts = ObjectOptions {
            no_lock: true,
            ..Default::default()
        };

        let guard = store
            .acquire_object_read_lock_if_needed("get_object", "bucket", "object", &mut opts)
            .await
            .expect("prelocked read should not acquire another lock");

        assert!(guard.is_none(), "prelocked caller should keep lock ownership outside ECStore");
        assert!(
            !opts.metadata_cache_safe,
            "generic no_lock callers must stay ineligible for metadata cache unless explicitly marked safe"
        );
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn reader_lock_is_held_when_optimization_is_disabled() {
        temp_env::async_with_vars([(rustfs_config::ENV_OBJECT_LOCK_OPTIMIZATION_ENABLE, Some("false"))], async {
            let manager = Arc::new(rustfs_lock::GlobalLockManager::new());
            let lock = rustfs_lock::NamespaceLock::with_local_manager("test".to_string(), manager);
            let key = rustfs_lock::ObjectKey::new("bucket", "object");
            let read_guard = lock
                .get_read_lock(key.clone(), "reader", Duration::from_secs(1))
                .await
                .expect("read lock should be acquired");
            let read_guard = ObjectLockDiagGuard::new(
                read_guard,
                true,
                "test_get_object",
                Some("bucket".to_string()),
                Some("object".to_string()),
                Some("reader".to_string()),
                ObjectLockDiagMode::Read,
            );
            let reader = GetObjectReader {
                stream: Box::new(Cursor::new(Vec::<u8>::new())),
                object_info: ObjectInfo::default(),
                buffered_body: None,
            };

            let reader = ECStore::attach_read_lock_guard(reader, Some(read_guard));

            lock.get_write_lock(key.clone(), "writer", Duration::from_millis(20))
                .await
                .expect_err("reader should hold the read lock");
            drop(reader);
            lock.get_write_lock(key, "writer", Duration::from_secs(1))
                .await
                .expect("dropping the reader should release the read lock");
        })
        .await;
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn reader_lock_is_not_held_for_stream_when_optimization_is_enabled() {
        temp_env::async_with_vars([(rustfs_config::ENV_OBJECT_LOCK_OPTIMIZATION_ENABLE, Some("true"))], async {
            let manager = Arc::new(rustfs_lock::GlobalLockManager::new());
            let lock = rustfs_lock::NamespaceLock::with_local_manager("test".to_string(), manager);
            let key = rustfs_lock::ObjectKey::new("bucket", "object");
            let read_guard = lock
                .get_read_lock(key.clone(), "reader", Duration::from_secs(1))
                .await
                .expect("read lock should be acquired");
            let read_guard = ObjectLockDiagGuard::new(
                read_guard,
                true,
                "test_get_object",
                Some("bucket".to_string()),
                Some("object".to_string()),
                Some("reader".to_string()),
                ObjectLockDiagMode::Read,
            );
            let reader = GetObjectReader {
                stream: Box::new(Cursor::new(vec![1, 2, 3])),
                object_info: ObjectInfo::default(),
                buffered_body: None,
            };

            let reader = ECStore::attach_read_lock_guard(reader, Some(read_guard));

            lock.get_write_lock(key, "writer", Duration::from_secs(1))
                .await
                .expect("lock optimization should release the read lock before returning the stream");
            drop(reader);
        })
        .await;
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn reader_lock_is_not_held_for_buffered_body_when_optimization_is_enabled() {
        temp_env::async_with_vars([(rustfs_config::ENV_OBJECT_LOCK_OPTIMIZATION_ENABLE, Some("true"))], async {
            let manager = Arc::new(rustfs_lock::GlobalLockManager::new());
            let lock = rustfs_lock::NamespaceLock::with_local_manager("test".to_string(), manager);
            let key = rustfs_lock::ObjectKey::new("bucket", "object");
            let read_guard = lock
                .get_read_lock(key.clone(), "reader", Duration::from_secs(1))
                .await
                .expect("read lock should be acquired");
            let read_guard = ObjectLockDiagGuard::new(
                read_guard,
                true,
                "test_get_object",
                Some("bucket".to_string()),
                Some("object".to_string()),
                Some("reader".to_string()),
                ObjectLockDiagMode::Read,
            );
            let reader = GetObjectReader {
                stream: Box::new(Cursor::new(vec![1, 2, 3])),
                object_info: ObjectInfo::default(),
                buffered_body: Some(Bytes::from_static(b"123")),
            };

            let reader = ECStore::attach_read_lock_guard(reader, Some(read_guard));

            lock.get_write_lock(key, "writer", Duration::from_secs(1))
                .await
                .expect("buffered reader should release the read lock immediately");
            drop(reader);
        })
        .await;
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn reader_lock_is_released_after_stream_eof() {
        temp_env::async_with_vars([(rustfs_config::ENV_OBJECT_LOCK_OPTIMIZATION_ENABLE, Some("false"))], async {
            let manager = Arc::new(rustfs_lock::GlobalLockManager::new());
            let lock = rustfs_lock::NamespaceLock::with_local_manager("test".to_string(), manager);
            let key = rustfs_lock::ObjectKey::new("bucket", "object");
            let read_guard = lock
                .get_read_lock(key.clone(), "reader", Duration::from_secs(1))
                .await
                .expect("read lock should be acquired");
            let read_guard = ObjectLockDiagGuard::new(
                read_guard,
                true,
                "test_get_object",
                Some("bucket".to_string()),
                Some("object".to_string()),
                Some("reader".to_string()),
                ObjectLockDiagMode::Read,
            );
            let reader = GetObjectReader {
                stream: Box::new(Cursor::new(vec![1, 2, 3])),
                object_info: ObjectInfo::default(),
                buffered_body: None,
            };

            let mut reader = ECStore::attach_read_lock_guard(reader, Some(read_guard));
            let mut output = Vec::new();
            reader.stream.read_to_end(&mut output).await.expect("reader should reach EOF");
            assert_eq!(output, vec![1, 2, 3]);

            lock.get_write_lock(key, "writer", Duration::from_secs(1))
                .await
                .expect("EOF should release the read lock before the reader is dropped");
            drop(reader);
        })
        .await;
    }
}
