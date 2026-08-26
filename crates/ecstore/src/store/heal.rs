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
use crate::core::pools::{POOL_META_NAME, load_pool_meta_identity_observing};
use crate::services::rebalance::{REBAL_META_NAME, RebalStatus};
use crate::set_disk::get_lock_acquire_timeout;
use crate::storage_api_contracts::heal::HealOperations as _;
use crate::storage_api_contracts::namespace::NamespaceLocking as _;
use rustfs_lock::NamespaceLockGuard;
use std::collections::BTreeSet;
use tracing::trace;

const LOG_COMPONENT_ECSTORE: &str = "ecstore";
const LOG_SUBSYSTEM_HEAL: &str = "heal";
const EVENT_HEAL_ABANDONED_PARTS: &str = "heal_abandoned_parts";
const EVENT_HEAL_FORMAT_COMPLETED: &str = "heal_format_completed";
const EVENT_HEAL_OBJECT_STARTED: &str = "heal_object_started";

fn invalid_heal_pool_index(pool_idx: usize, pool_count: usize) -> Error {
    StorageError::InvalidArgument(
        "heal".to_string(),
        "pool".to_string(),
        format!("invalid heal pool index {pool_idx} for {pool_count} pools"),
    )
}

#[derive(Debug, Clone, Copy)]
enum HealFormatPoolSkip {
    Completed,
    Retryable,
}

fn classify_heal_format_pool(
    pool_idx: usize,
    pool_cmd_line: &str,
    pool_meta: &PoolMeta,
    rebalance_meta: Option<&RebalanceMeta>,
) -> Option<HealFormatPoolSkip> {
    let Some(pool) = pool_meta.pools.get(pool_idx) else {
        return Some(HealFormatPoolSkip::Retryable);
    };

    if pool.id != pool_idx || pool_cmd_line.is_empty() || pool.cmd_line.is_empty() || pool.cmd_line != pool_cmd_line {
        return Some(HealFormatPoolSkip::Retryable);
    }

    if let Some(decommission) = pool.decommission.as_ref() {
        if decommission.complete {
            return Some(HealFormatPoolSkip::Completed);
        }
        if decommission.failed || decommission.canceled || decommission.queued || pool_meta.is_suspended(pool_idx) {
            return Some(HealFormatPoolSkip::Retryable);
        }
    }

    if let Some(meta) = rebalance_meta {
        let Some(pool_stats) = meta.pool_stats.get(pool_idx) else {
            return Some(HealFormatPoolSkip::Retryable);
        };
        if pool_stats.info.stopping || (pool_stats.participating && pool_stats.info.status == RebalStatus::Started) {
            return Some(HealFormatPoolSkip::Retryable);
        }
    }

    None
}

fn heal_format_pool_skip_error(skip: HealFormatPoolSkip) -> Error {
    match skip {
        HealFormatPoolSkip::Completed => StorageError::NoHealRequired,
        HealFormatPoolSkip::Retryable => StorageError::SlowDown,
    }
}

fn heal_format_fence_lost_error() -> Error {
    StorageError::SlowDown
}

impl ECStore {
    async fn acquire_heal_format_fence(
        &self,
    ) -> Result<(
        tokio::sync::MutexGuard<'_, PoolMetaWriteState>,
        NamespaceLockGuard,
        NamespaceLockGuard,
        PoolMeta,
        Option<RebalanceMeta>,
    )> {
        let metadata_pool = self
            .pools
            .first()
            .cloned()
            .ok_or_else(|| Error::other("heal format requires at least one storage pool"))?;
        let mut write_state = self.pool_meta_save_gate.lock().await;
        write_state.ensure_write_safe("heal format fence failed")?;

        // Metadata fence order is part of the decommission/rebalance protocol:
        // pool.bin must always be acquired before rebalance.bin.
        let pool_lock = metadata_pool.new_ns_lock(RUSTFS_META_BUCKET, POOL_META_NAME).await?;
        let pool_guard = pool_lock.get_write_lock(get_lock_acquire_timeout()).await?;
        let rebalance_lock = metadata_pool.new_ns_lock(RUSTFS_META_BUCKET, REBAL_META_NAME).await?;
        let rebalance_guard = rebalance_lock.get_write_lock(get_lock_acquire_timeout()).await?;

        if pool_guard.is_lock_lost() || rebalance_guard.is_lock_lost() {
            return Err(heal_format_fence_lost_error());
        }

        load_pool_meta_identity_observing(self.pools.clone(), &mut write_state).await?;
        let mut pool_meta = PoolMeta::default();
        let replica_state = pool_meta
            .load_no_lock_from_replicas_observing(self.pools.clone(), &mut write_state)
            .await?;
        write_state.observe_replicas(replica_state);
        write_state.ensure_missing_metadata_can_initialize()?;
        write_state.ensure_write_safe("heal format fence failed")?;
        if pool_meta.pools.len() != self.pools.len()
            || pool_meta.pools.iter().enumerate().any(|(pool_idx, pool)| {
                pool.id != pool_idx || pool.cmd_line.is_empty() || pool.cmd_line != self.pools[pool_idx].endpoints.cmd_line
            })
        {
            return Err(heal_format_fence_lost_error());
        }

        let mut rebalance_meta = RebalanceMeta::new();
        let rebalance_meta = match rebalance_meta
            .load_with_opts(
                metadata_pool,
                ObjectOptions {
                    no_lock: true,
                    ..Default::default()
                },
            )
            .await
        {
            Ok(()) => Some(rebalance_meta),
            Err(Error::ConfigNotFound) => None,
            Err(err) => return Err(err),
        };

        if rebalance_meta
            .as_ref()
            .is_some_and(|meta| meta.pool_stats.len() != self.pools.len())
        {
            return Err(heal_format_fence_lost_error());
        }

        write_state.ensure_write_safe("heal format fence failed")?;
        if pool_guard.is_lock_lost() || rebalance_guard.is_lock_lost() {
            return Err(heal_format_fence_lost_error());
        }

        Ok((write_state, pool_guard, rebalance_guard, pool_meta, rebalance_meta))
    }

    fn get_pools_for_heal_object(&self, opts: &HealOpts) -> Result<Vec<Arc<Sets>>> {
        match opts.pool {
            Some(pool_idx) => Ok(vec![
                self.pools
                    .get(pool_idx)
                    .cloned()
                    .ok_or_else(|| invalid_heal_pool_index(pool_idx, self.pools.len()))?,
            ]),
            None => Ok(self.pools.clone()),
        }
    }

    /// Return every live erasure set selected by an object-heal scope.
    pub async fn heal_erasure_set_scopes(&self, opts: &HealOpts) -> Result<Vec<(usize, usize)>> {
        let pools = self.get_pools_for_heal_object(opts)?;
        let pool_meta = self.pool_meta.read().await;
        let mut scopes = Vec::new();

        for pool in pools {
            let suspended_complete = pool_meta.is_suspended(pool.pool_idx).then(|| {
                pool_meta
                    .pools
                    .get(pool.pool_idx)
                    .and_then(|status| status.decommission.as_ref())
                    .is_some_and(|decommission| decommission.complete)
            });
            if let Some(complete) = suspended_complete {
                if opts.pool.is_some() {
                    return Err(if complete {
                        StorageError::InvalidArgument(
                            "heal".to_string(),
                            "pool".to_string(),
                            format!("heal pool {} has completed decommission", pool.pool_idx),
                        )
                    } else {
                        Error::SlowDown
                    });
                }
                continue;
            }

            if let Some(set_idx) = opts.set {
                if set_idx >= pool.disk_set.len() {
                    return Err(StorageError::InvalidArgument(
                        "heal".to_string(),
                        "set".to_string(),
                        format!(
                            "invalid heal set index {set_idx} for pool {} with {} sets",
                            pool.pool_idx,
                            pool.disk_set.len()
                        ),
                    ));
                }
                scopes.push((pool.pool_idx, set_idx));
            } else {
                scopes.extend((0..pool.disk_set.len()).map(|set_idx| (pool.pool_idx, set_idx)));
            }
        }

        if scopes.is_empty() {
            return Err(Error::SlowDown);
        }

        Ok(scopes)
    }

    #[instrument(skip(self))]
    pub(super) async fn handle_heal_format(&self, dry_run: bool) -> Result<(HealResultItem, Option<Error>)> {
        let mut r = HealResultItem {
            heal_item_type: HealItemType::Metadata.to_string(),
            detail: "disk-format".to_string(),
            ..Default::default()
        };

        let mut count_no_heal = 0;
        let mut count_completed = 0;
        let mut first_error = None;
        for (pool_idx, pool) in self.pools.iter().enumerate() {
            let (mut write_state, pool_guard, rebalance_guard, pool_meta, rebalance_meta) =
                self.acquire_heal_format_fence().await?;
            if pool_guard.is_lock_lost() || rebalance_guard.is_lock_lost() {
                first_error.get_or_insert(heal_format_fence_lost_error());
                break;
            }
            if let Some(skip) = classify_heal_format_pool(pool_idx, &pool.endpoints.cmd_line, &pool_meta, rebalance_meta.as_ref())
            {
                if matches!(skip, HealFormatPoolSkip::Completed) {
                    count_completed += 1;
                } else {
                    first_error.get_or_insert(heal_format_pool_skip_error(skip));
                }
                continue;
            }

            let fence_lost = || {
                let lost = pool_guard.is_lock_lost()
                    || rebalance_guard.is_lock_lost()
                    || write_state.ensure_write_safe("heal format write fence failed").is_err();
                if lost {
                    write_state.block_writes_after_fence_loss();
                }
                lost
            };
            let (mut result, err) = pool.heal_format_with_fence(dry_run, fence_lost).await?;
            if let Some(err) = err {
                match err {
                    StorageError::NoHealRequired => {
                        count_no_heal += 1;
                    }
                    err => {
                        first_error.get_or_insert(err);
                    }
                }
            }
            r.disk_count += result.disk_count;
            r.set_count += result.set_count;
            r.before.drives.append(&mut result.before.drives);
            r.after.drives.append(&mut result.after.drives);

            // A lease can be lost after the final write; fail closed before
            // reporting the pool as successfully healed.
            let fence_lost = pool_guard.is_lock_lost()
                || rebalance_guard.is_lock_lost()
                || write_state.ensure_write_safe("heal format publication fence failed").is_err();
            if fence_lost {
                write_state.block_writes_after_fence_loss();
                first_error.get_or_insert(heal_format_fence_lost_error());
                break;
            }
        }
        if let Some(err) = first_error {
            return Ok((r, Some(err)));
        }
        if count_no_heal + count_completed == self.pools.len() {
            info!(
                event = EVENT_HEAL_FORMAT_COMPLETED,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_HEAL,
                dry_run,
                result = "no_heal_required",
                pool_count = self.pools.len(),
                "Heal format completed"
            );
            return Ok((r, Some(StorageError::NoHealRequired)));
        }
        info!(
            event = EVENT_HEAL_FORMAT_COMPLETED,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_HEAL,
            dry_run,
            result = "healed_or_inspected",
            disk_count = r.disk_count,
            set_count = r.set_count,
            "Heal format completed"
        );
        Ok((r, None))
    }

    #[instrument(skip(self, targets), fields(pool_index, set_index, target_count = targets.len()))]
    pub async fn heal_replacement_format(
        &self,
        dry_run: bool,
        pool_index: usize,
        set_index: usize,
        targets: &[String],
    ) -> Result<(HealResultItem, Option<Error>)> {
        let pool = self
            .pools
            .get(pool_index)
            .ok_or_else(|| invalid_heal_pool_index(pool_index, self.pools.len()))?;
        let set = pool.disk_set.get(set_index).cloned().ok_or_else(|| {
            StorageError::InvalidArgument(
                "heal".to_string(),
                "set".to_string(),
                format!("invalid heal set index {set_index} for pool {pool_index}"),
            )
        })?;

        let (mut write_state, pool_guard, rebalance_guard, pool_meta, rebalance_meta) = self.acquire_heal_format_fence().await?;
        if let Some(skip) = classify_heal_format_pool(pool_index, &pool.endpoints.cmd_line, &pool_meta, rebalance_meta.as_ref()) {
            return Ok((HealResultItem::default(), Some(heal_format_pool_skip_error(skip))));
        }

        let fence_lost = || {
            let lost = pool_guard.is_lock_lost()
                || rebalance_guard.is_lock_lost()
                || write_state
                    .ensure_write_safe("replacement format write fence failed")
                    .is_err();
            if lost {
                write_state.block_writes_after_fence_loss();
            }
            lost
        };
        let result = set.heal_replacement_format_with_fence(dry_run, targets, fence_lost).await?;
        let fence_lost = pool_guard.is_lock_lost()
            || rebalance_guard.is_lock_lost()
            || write_state
                .ensure_write_safe("replacement format publication fence failed")
                .is_err();
        if fence_lost {
            write_state.block_writes_after_fence_loss();
            return Ok((result.0, Some(heal_format_fence_lost_error())));
        }
        Ok(result)
    }

    #[instrument(skip(self, targets), fields(pool_index, set_index, target_count = targets.len()))]
    pub async fn replacement_targets_have_version(
        &self,
        bucket: &str,
        object: &str,
        version_id: &str,
        pool_index: usize,
        set_index: usize,
        targets: &[String],
    ) -> Result<bool> {
        let pool = self
            .pools
            .get(pool_index)
            .ok_or_else(|| invalid_heal_pool_index(pool_index, self.pools.len()))?;
        let set = pool.disk_set.get(set_index).cloned().ok_or_else(|| {
            StorageError::InvalidArgument(
                "heal".to_string(),
                "set".to_string(),
                format!("invalid heal set index {set_index} for pool {pool_index}"),
            )
        })?;

        set.replacement_targets_have_version(bucket, object, version_id, targets)
            .await
            .map_err(Into::into)
    }

    #[instrument(skip(self))]
    pub(super) async fn handle_heal_bucket(&self, bucket: &str, opts: &HealOpts) -> Result<HealResultItem> {
        let movement_gate = self.ctx.data_movement_operation_gate();
        let _movement_guard = movement_gate.read().await;
        let save_guard = self.pool_meta_save_gate.lock().await;
        save_guard.ensure_write_safe("bucket heal cannot run while pool metadata requires recovery")?;
        let mut fenced_pools = BTreeSet::new();
        {
            let pool_meta = self.pool_meta.read().await;
            fenced_pools.extend((0..pool_meta.pools.len()).filter(|pool_idx| pool_meta.is_suspended(*pool_idx)));
            if let Some(pool_idx) = opts.pool {
                if pool_idx >= pool_meta.pools.len() {
                    return Err(invalid_heal_pool_index(pool_idx, pool_meta.pools.len()));
                }
                if pool_meta.is_suspended(pool_idx) {
                    let complete = pool_meta.pools[pool_idx]
                        .decommission
                        .as_ref()
                        .is_some_and(|decommission| decommission.complete);
                    return Err(if complete {
                        StorageError::InvalidArgument(
                            "heal".to_string(),
                            "pool".to_string(),
                            format!("heal pool {pool_idx} has completed decommission"),
                        )
                    } else {
                        Error::SlowDown
                    });
                }
            }
        }

        let dispatch_fenced_pools = fenced_pools.iter().copied().collect::<Vec<_>>();
        drop(save_guard);
        let mut res = self
            .peer_sys
            .heal_bucket_with_fence_from_movement_guarded_coordinator(bucket, opts, &dispatch_fenced_pools)
            .await?;
        {
            let pool_meta = self.pool_meta.read().await;
            fenced_pools.extend((0..pool_meta.pools.len()).filter(|pool_idx| pool_meta.is_suspended(*pool_idx)));
        }
        if !fenced_pools.is_empty() {
            let pools = fenced_pools.iter().map(usize::to_string).collect::<Vec<_>>().join(", ");
            res.detail = format!("skipped: bucket-volume heal fenced on decommission-suspended pool(s): {pools}");
        }

        Ok(res)
    }

    #[instrument(level = "trace", skip(self, opts), fields(bucket = %bucket, object = %object, version_id = %version_id))]
    pub(super) async fn handle_heal_object(
        &self,
        bucket: &str,
        object: &str,
        version_id: &str,
        opts: &HealOpts,
    ) -> Result<(HealResultItem, Option<Error>)> {
        trace!(
            event = EVENT_HEAL_OBJECT_STARTED,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_HEAL,
            bucket = %bucket,
            object = %object,
            version_id = %version_id,
            remove = opts.remove,
            scan_mode = ?opts.scan_mode,
            "Heal object started"
        );
        let object = encode_dir_object(object);

        let pools = self.get_pools_for_heal_object(opts)?;

        let mut futures = Vec::with_capacity(pools.len());
        for pool in pools.iter() {
            let suspended_complete = {
                let pool_meta = self.pool_meta.read().await;
                pool_meta.is_suspended(pool.pool_idx).then(|| {
                    pool_meta
                        .pools
                        .get(pool.pool_idx)
                        .and_then(|status| status.decommission.as_ref())
                        .is_some_and(|decommission| decommission.complete)
                })
            };
            if let Some(complete) = suspended_complete {
                if opts.pool.is_some() {
                    let _ = pool.get_disks_for_heal_object(&object, opts)?;
                    let err = if complete {
                        StorageError::InvalidArgument(
                            "heal".to_string(),
                            "pool".to_string(),
                            format!("heal pool {} has completed decommission", pool.pool_idx),
                        )
                    } else {
                        Error::SlowDown
                    };
                    return Ok((HealResultItem::default(), Some(err)));
                }
                continue;
            }
            futures.push(pool.heal_object(bucket, &object, version_id, opts));
        }
        let results = join_all(futures).await;

        let mut errs = Vec::with_capacity(self.pools.len());
        let mut ress = Vec::with_capacity(self.pools.len());

        for res in results.into_iter() {
            match res {
                Ok((result, err)) => {
                    let mut result = result;
                    result.object = decode_dir_object(&result.object);
                    ress.push(result);
                    errs.push(err);
                }
                Err(err) => {
                    errs.push(Some(err));
                    ress.push(HealResultItem::default());
                }
            }
        }

        for (idx, err) in errs.iter().enumerate() {
            if err.is_none() {
                return Ok((ress.remove(idx), None));
            }
        }

        // No pool returned a nil error, return the first non 'not found' error
        for (index, err) in errs.iter().enumerate() {
            return match err {
                Some(err) => {
                    if is_err_object_not_found(err) || is_err_version_not_found(err) {
                        continue;
                    }
                    Ok((ress.remove(index), Some(err.clone())))
                }
                None => Ok((ress.remove(index), None)),
            };
        }

        // At this stage, all errors are 'not found'
        if !version_id.is_empty() {
            return Ok((HealResultItem::default(), Some(Error::FileVersionNotFound)));
        }

        Ok((HealResultItem::default(), Some(Error::FileNotFound)))
    }

    #[instrument(skip(self))]
    pub(super) async fn handle_check_abandoned_parts(&self, bucket: &str, object: &str, opts: &HealOpts) -> Result<()> {
        let object = encode_dir_object(object);
        let pools = self.get_pools_for_heal_object(opts)?;

        let mut futures = Vec::with_capacity(pools.len());
        for pool in pools.iter() {
            futures.push(pool.check_abandoned_parts(bucket, &object, opts));
        }

        let mut first_error = None;
        for result in join_all(futures).await {
            if let Err(err) = result
                && first_error.is_none()
            {
                first_error = Some(err);
            }
        }

        if let Some(err) = first_error {
            return Err(err);
        }

        trace!(
            event = EVENT_HEAL_ABANDONED_PARTS,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_HEAL,
            state = "completed",
            result = "ok",
            bucket,
            object,
            dry_run = opts.dry_run,
            "Heal abandoned parts completed"
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bucket::metadata_sys;
    use crate::cluster::rpc::PeerS3Client;
    use crate::config::com::{delete_config, read_config_no_lock_preserve_empty_with_metadata, save_config};
    use crate::core::pools::{
        POOL_META_IDENTITY_NAME, PoolDecommissionInfo, PoolMetaReplicaState, PoolStatus, initialized_pool_meta_identity_for_test,
    };
    use crate::core::sets::HealFormatAfterSaveBarrier;
    use crate::disk::error::Result as DiskResult;
    use crate::disk::{DeleteOptions, DiskOption, FORMAT_CONFIG_FILE, format::FormatV3, new_disk};
    use crate::layout::endpoints::{EndpointServerPools, Endpoints, PoolEndpoints};
    use crate::runtime::instance::InstanceContext;
    use crate::services::rebalance::{RebalanceInfo, RebalanceStats};
    use crate::storage_api_contracts::bucket::{
        BucketInfo, BucketOperations, BucketOptions, DeleteBucketOptions, MakeBucketOptions,
    };
    use crate::storage_api_contracts::object::{ObjectIO as _, ObjectOperations};
    use crate::store::init_format::{load_format_erasure, save_format_file};
    use crate::store::init_local_disks_with_instance_ctx;
    use rustfs_heal_contracts::heal_channel::DriveState;
    use tokio_util::sync::CancellationToken;

    #[derive(Debug)]
    struct BlockingHealPeer {
        started: Arc<tokio::sync::Notify>,
        release: Arc<tokio::sync::Notify>,
    }

    #[async_trait::async_trait]
    impl PeerS3Client for BlockingHealPeer {
        async fn heal_bucket(&self, _bucket: &str, _opts: &HealOpts) -> DiskResult<HealResultItem> {
            self.started.notify_one();
            self.release.notified().await;
            Ok(HealResultItem::default())
        }

        async fn make_bucket(&self, _bucket: &str, _opts: &MakeBucketOptions) -> DiskResult<()> {
            Ok(())
        }

        async fn list_bucket(&self, _opts: &BucketOptions) -> DiskResult<Vec<BucketInfo>> {
            Ok(Vec::new())
        }

        async fn delete_bucket(&self, _bucket: &str, _opts: &DeleteBucketOptions) -> DiskResult<()> {
            Ok(())
        }

        async fn get_bucket_info(&self, _bucket: &str, _opts: &BucketOptions) -> DiskResult<BucketInfo> {
            Ok(BucketInfo::default())
        }

        fn get_pools(&self) -> Option<Vec<usize>> {
            Some(vec![0, 1])
        }
    }

    #[derive(Debug)]
    struct WriterQueuedLocalHealPeer {
        movement_gate: Arc<tokio::sync::RwLock<()>>,
        writer_queued: Arc<tokio::sync::Notify>,
        writer_acquired: Arc<tokio::sync::Notify>,
    }

    #[async_trait::async_trait]
    impl PeerS3Client for WriterQueuedLocalHealPeer {
        async fn heal_bucket(&self, _bucket: &str, _opts: &HealOpts) -> DiskResult<HealResultItem> {
            let _movement_guard = self
                .movement_gate
                .try_read()
                .map_err(|_| crate::disk::error::DiskError::TooManyOpenFiles)?;
            Ok(HealResultItem::default())
        }

        async fn heal_bucket_with_fence_from_movement_guarded_coordinator(
            &self,
            _bucket: &str,
            _opts: &HealOpts,
            _fenced_pools: &[usize],
        ) -> DiskResult<HealResultItem> {
            Ok(HealResultItem::default())
        }

        async fn make_bucket(&self, _bucket: &str, _opts: &MakeBucketOptions) -> DiskResult<()> {
            Ok(())
        }

        async fn list_bucket(&self, _opts: &BucketOptions) -> DiskResult<Vec<BucketInfo>> {
            Ok(Vec::new())
        }

        async fn delete_bucket(&self, _bucket: &str, _opts: &DeleteBucketOptions) -> DiskResult<()> {
            Ok(())
        }

        async fn get_bucket_info(&self, _bucket: &str, _opts: &BucketOptions) -> DiskResult<BucketInfo> {
            let movement_gate = self.movement_gate.clone();
            let writer_acquired = self.writer_acquired.clone();
            tokio::spawn(async move {
                let _movement_guard = movement_gate.write().await;
                writer_acquired.notify_one();
            });
            while self.movement_gate.try_read().is_ok() {
                tokio::task::yield_now().await;
            }
            self.writer_queued.notify_one();
            Ok(BucketInfo::default())
        }

        fn get_pools(&self) -> Option<Vec<usize>> {
            Some(vec![0, 1])
        }
    }

    async fn minimal_heal_pool(pool_idx: usize) -> Arc<Sets> {
        let format = FormatV3::new(1, 1);
        let endpoint_url = format!("http://127.0.0.1:{}/data", 19000 + pool_idx);
        let mut endpoint = Endpoint::try_from(endpoint_url.as_str()).expect("endpoint should parse");
        endpoint.set_pool_index(pool_idx);
        endpoint.set_set_index(0);
        endpoint.set_disk_index(0);

        Sets::new(
            vec![None],
            &PoolEndpoints {
                legacy: false,
                set_count: 1,
                drives_per_set: 1,
                endpoints: Endpoints::from(vec![endpoint]),
                cmd_line: String::new(),
                platform: String::new(),
            },
            &format,
            pool_idx,
            0,
        )
        .await
        .expect("minimal pool should build")
    }

    async fn minimal_heal_store() -> ECStore {
        ECStore {
            id: Uuid::new_v4(),
            disk_map: HashMap::new(),
            pools: vec![minimal_heal_pool(0).await, minimal_heal_pool(1).await],
            peer_sys: S3PeerSys {
                clients: Vec::new(),
                pools_count: 2,
            },
            pool_meta: RwLock::new(PoolMeta::default()),
            rebalance_meta: RwLock::new(None),
            decommission_cancelers: RwLock::new(Vec::new()),
            start_gate: Mutex::new(()),
            pool_meta_save_gate: Mutex::default(),
            ctx: crate::runtime::instance::bootstrap_ctx(),
            bucket_fence_registry: std::sync::Arc::default(),
        }
    }

    #[tokio::test]
    async fn heal_erasure_set_scopes_follow_requested_pool_and_set() {
        let store = minimal_heal_store().await;

        assert_eq!(
            store
                .heal_erasure_set_scopes(&HealOpts::default())
                .await
                .expect("unscoped heal should enumerate every live set"),
            vec![(0, 0), (1, 0)]
        );
        assert_eq!(
            store
                .heal_erasure_set_scopes(&HealOpts {
                    pool: Some(1),
                    set: Some(0),
                    ..Default::default()
                })
                .await
                .expect("scoped heal should enumerate only its requested set"),
            vec![(1, 0)]
        );

        let err = store
            .heal_erasure_set_scopes(&HealOpts {
                pool: Some(0),
                set: Some(1),
                ..Default::default()
            })
            .await
            .expect_err("an invalid set scope must fail closed");
        assert!(matches!(err, Error::InvalidArgument(..)));
    }

    fn pool_meta_with_decommission(info: PoolDecommissionInfo) -> PoolMeta {
        PoolMeta {
            pools: vec![PoolStatus {
                id: 0,
                cmd_line: "pool-0".to_string(),
                last_update: OffsetDateTime::UNIX_EPOCH,
                decommission: Some(info),
            }],
            ..Default::default()
        }
    }

    #[test]
    fn heal_format_pool_state_barriers_are_classified() {
        let active = pool_meta_with_decommission(PoolDecommissionInfo {
            start_time: Some(OffsetDateTime::UNIX_EPOCH),
            ..Default::default()
        });
        assert!(matches!(
            classify_heal_format_pool(0, "pool-0", &active, None),
            Some(HealFormatPoolSkip::Retryable)
        ));

        for info in [
            PoolDecommissionInfo {
                failed: true,
                ..Default::default()
            },
            PoolDecommissionInfo {
                canceled: true,
                ..Default::default()
            },
        ] {
            assert!(matches!(
                classify_heal_format_pool(0, "pool-0", &pool_meta_with_decommission(info), None),
                Some(HealFormatPoolSkip::Retryable)
            ));
        }

        let completed = pool_meta_with_decommission(PoolDecommissionInfo {
            complete: true,
            ..Default::default()
        });
        assert!(matches!(
            classify_heal_format_pool(0, "pool-0", &completed, None),
            Some(HealFormatPoolSkip::Completed)
        ));
    }

    #[test]
    fn heal_format_pool_rebalance_barriers_and_identity_are_fail_closed() {
        let identity_meta = pool_meta_with_decommission(PoolDecommissionInfo::default());
        let rebalance = RebalanceMeta {
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
        assert!(matches!(
            classify_heal_format_pool(0, "pool-0", &identity_meta, Some(&rebalance)),
            Some(HealFormatPoolSkip::Retryable)
        ));

        let stopping = RebalanceMeta {
            pool_stats: vec![RebalanceStats {
                info: RebalanceInfo {
                    stopping: true,
                    ..Default::default()
                },
                ..Default::default()
            }],
            ..Default::default()
        };
        assert!(matches!(
            classify_heal_format_pool(0, "pool-0", &identity_meta, Some(&stopping)),
            Some(HealFormatPoolSkip::Retryable)
        ));

        let identity = pool_meta_with_decommission(PoolDecommissionInfo::default());
        assert!(matches!(
            classify_heal_format_pool(0, "pool-new", &identity, None),
            Some(HealFormatPoolSkip::Retryable)
        ));

        let identity_without_decommission = PoolMeta {
            pools: vec![PoolStatus {
                id: 0,
                cmd_line: "pool-0".to_string(),
                last_update: OffsetDateTime::UNIX_EPOCH,
                decommission: None,
            }],
            ..Default::default()
        };
        assert!(matches!(
            classify_heal_format_pool(0, "pool-new", &identity_without_decommission, None),
            Some(HealFormatPoolSkip::Retryable)
        ));

        assert!(matches!(
            classify_heal_format_pool(0, "", &identity_meta, None),
            Some(HealFormatPoolSkip::Retryable)
        ));

        assert!(matches!(
            classify_heal_format_pool(0, "pool-0", &PoolMeta::default(), None),
            Some(HealFormatPoolSkip::Retryable)
        ));

        let stopped = RebalanceMeta {
            stopped_at: Some(OffsetDateTime::UNIX_EPOCH),
            pool_stats: vec![RebalanceStats {
                participating: true,
                info: RebalanceInfo {
                    status: RebalStatus::Stopped,
                    ..Default::default()
                },
                ..Default::default()
            }],
            ..Default::default()
        };
        assert!(classify_heal_format_pool(0, "pool-0", &identity_meta, Some(&stopped)).is_none());

        let stopping_after_stop = RebalanceMeta {
            stopped_at: Some(OffsetDateTime::UNIX_EPOCH),
            pool_stats: vec![RebalanceStats {
                participating: true,
                info: RebalanceInfo {
                    status: RebalStatus::Started,
                    stopping: true,
                    ..Default::default()
                },
                ..Default::default()
            }],
            ..Default::default()
        };
        assert!(matches!(
            classify_heal_format_pool(0, "pool-0", &identity_meta, Some(&stopping_after_stop)),
            Some(HealFormatPoolSkip::Retryable)
        ));
    }

    #[test]
    fn skipped_heal_format_pool_is_never_reported_as_success() {
        assert!(matches!(
            heal_format_pool_skip_error(HealFormatPoolSkip::Retryable),
            StorageError::SlowDown
        ));
        assert!(matches!(
            heal_format_pool_skip_error(HealFormatPoolSkip::Completed),
            StorageError::NoHealRequired
        ));
    }

    async fn multi_pool_heal_store() -> (tempfile::TempDir, Arc<ECStore>, CancellationToken) {
        let temp_dir = tempfile::tempdir().expect("multi-pool heal test directory should be created");
        let mut pool_endpoints = Vec::new();
        for pool_index in 0..2 {
            let mut endpoints = Vec::new();
            for disk_index in 0..4 {
                let disk_path = temp_dir.path().join(format!("pool{pool_index}-disk{disk_index}"));
                tokio::fs::create_dir_all(&disk_path)
                    .await
                    .expect("multi-pool heal test disk should be created");
                let mut endpoint = Endpoint::try_from(disk_path.to_str().expect("disk path should be utf8"))
                    .expect("test endpoint should parse");
                endpoint.set_pool_index(pool_index);
                endpoint.set_set_index(0);
                endpoint.set_disk_index(disk_index);
                endpoints.push(endpoint);
            }
            pool_endpoints.push(PoolEndpoints {
                legacy: false,
                set_count: 1,
                drives_per_set: 4,
                endpoints: Endpoints::from(endpoints),
                cmd_line: format!("heal-owner-pool-{pool_index}"),
                platform: "test".to_string(),
            });
        }

        let endpoint_pools = EndpointServerPools::from(pool_endpoints);
        let instance_ctx = Arc::new(InstanceContext::new());
        init_local_disks_with_instance_ctx(&instance_ctx, endpoint_pools.clone())
            .await
            .expect("multi-pool local disks should initialize");
        let shutdown = CancellationToken::new();
        let store = ECStore::new_with_instance_ctx(
            "127.0.0.1:0".parse().expect("test address should parse"),
            endpoint_pools,
            shutdown.clone(),
            instance_ctx,
        )
        .await
        .expect("multi-pool test store should initialize");
        metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        (temp_dir, store, shutdown)
    }

    fn heal_test_format_path(temp_dir: &tempfile::TempDir, pool_index: usize, disk_index: usize) -> std::path::PathBuf {
        temp_dir
            .path()
            .join(format!("pool{pool_index}-disk{disk_index}"))
            .join(crate::disk::RUSTFS_META_BUCKET)
            .join(FORMAT_CONFIG_FILE)
    }

    async fn remove_heal_test_format(
        temp_dir: &tempfile::TempDir,
        store: &ECStore,
        pool_index: usize,
        disk_index: usize,
    ) -> String {
        let target = store.pools[pool_index].endpoints.endpoints.as_ref()[disk_index].to_string();
        let format_path = heal_test_format_path(temp_dir, pool_index, disk_index);
        tokio::fs::remove_file(&format_path)
            .await
            .expect("replacement target format should be removable");
        assert!(
            !tokio::fs::try_exists(&format_path)
                .await
                .expect("replacement target format path should be inspectable")
        );
        target
    }

    async fn assert_heal_test_format_missing(temp_dir: &tempfile::TempDir, pool_index: usize, disk_index: usize) {
        assert!(
            !tokio::fs::try_exists(heal_test_format_path(temp_dir, pool_index, disk_index))
                .await
                .expect("replacement target format path should be inspectable"),
            "format heal must not write the replacement target"
        );
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn full_format_heal_preblocked_pool_metadata_never_writes_format() {
        let (temp_dir, store, shutdown) = multi_pool_heal_store().await;
        remove_heal_test_format(&temp_dir, &store, 0, 3).await;
        store.pool_meta_save_gate.lock().await.observe_replicas(PoolMetaReplicaState {
            needs_repair: true,
            repair_write_safe: false,
        });

        let err = store
            .handle_heal_format(false)
            .await
            .expect_err("a preblocked pool metadata state must reject full format heal");
        assert!(
            err.to_string()
                .contains("restart after all replicas are readable and consistent")
        );
        assert_heal_test_format_missing(&temp_dir, 0, 3).await;
        store
            .ensure_pool_meta_side_effects_safe("preblocked format heal side effect")
            .await
            .expect_err("the preblocked state must remain sticky");
        shutdown.cancel();
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn full_format_heal_future_identity_never_writes_format_and_latches() {
        let (temp_dir, store, shutdown) = multi_pool_heal_store().await;
        remove_heal_test_format(&temp_dir, &store, 0, 3).await;
        let (mut future_identity, _) =
            read_config_no_lock_preserve_empty_with_metadata(store.pools[1].clone(), POOL_META_IDENTITY_NAME)
                .await
                .expect("current identity should be readable");
        let future_version = u16::from_le_bytes([future_identity[2], future_identity[3]])
            .checked_add(1)
            .expect("identity version should have a future value");
        future_identity[2..4].copy_from_slice(&future_version.to_le_bytes());
        save_config(store.pools[1].clone(), POOL_META_IDENTITY_NAME, future_identity)
            .await
            .expect("future identity should be persisted");

        let err = store
            .handle_heal_format(false)
            .await
            .expect_err("a future identity must reject full format heal");
        assert!(err.to_string().contains("pool metadata incompatible"));
        assert_heal_test_format_missing(&temp_dir, 0, 3).await;
        store
            .ensure_pool_meta_side_effects_safe("future identity format heal side effect")
            .await
            .expect_err("the future identity rejection must latch the write gate");
        shutdown.cancel();
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn replacement_format_heal_epoch_conflict_never_writes_format_and_latches() {
        let (temp_dir, store, shutdown) = multi_pool_heal_store().await;
        let target = remove_heal_test_format(&temp_dir, &store, 0, 3).await;
        let conflicting_identity =
            initialized_pool_meta_identity_for_test(store.id, 2).expect("conflicting identity should encode");
        save_config(store.pools[1].clone(), POOL_META_IDENTITY_NAME, conflicting_identity)
            .await
            .expect("conflicting identity should be persisted");

        let err = store
            .heal_replacement_format(false, 0, 0, &[target])
            .await
            .expect_err("an identity epoch conflict must reject replacement format heal");
        assert!(
            err.to_string()
                .contains("identity replicas disagree on cluster identity or epoch")
        );
        assert_heal_test_format_missing(&temp_dir, 0, 3).await;
        store
            .ensure_pool_meta_side_effects_safe("epoch conflict replacement format side effect")
            .await
            .expect_err("the identity epoch conflict must latch the write gate");
        shutdown.cancel();
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn replacement_format_heal_initialized_identity_without_pool_meta_never_writes_and_latches() {
        let (temp_dir, store, shutdown) = multi_pool_heal_store().await;
        let target = remove_heal_test_format(&temp_dir, &store, 0, 3).await;
        for pool in &store.pools {
            delete_config(pool.clone(), POOL_META_NAME)
                .await
                .expect("pool metadata replica should be removable");
        }

        let err = store
            .heal_replacement_format(false, 0, 0, &[target])
            .await
            .expect_err("initialized identity without pool metadata must reject replacement format heal");
        assert!(err.to_string().contains("initialized cluster identity exists"));
        assert_heal_test_format_missing(&temp_dir, 0, 3).await;
        store
            .ensure_pool_meta_side_effects_safe("missing pool metadata replacement format side effect")
            .await
            .expect_err("missing initialized pool metadata must latch the write gate");
        shutdown.cancel();
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn full_format_heal_lost_after_last_save_does_not_publish_or_renew_and_latches() {
        let (temp_dir, store, shutdown) = multi_pool_heal_store().await;
        let target = remove_heal_test_format(&temp_dir, &store, 0, 3).await;
        store.pools[0].disk_set[0].disks.write().await[3] = None;
        let barrier = HealFormatAfterSaveBarrier::install(&store.pools[0], 3);
        let recovery_latch = store.pool_meta_save_gate.lock().await.aborted_transaction_latch_for_test();
        let mut heal = tokio::spawn({
            let store = Arc::clone(&store);
            async move { store.handle_heal_format(false).await }
        });

        barrier.wait_until_paused().await;
        let saved = tokio::fs::read(heal_test_format_path(&temp_dir, 0, 3))
            .await
            .expect("the last replacement format must be durable before fence loss");
        let saved = FormatV3::try_from(saved.as_slice()).expect("the durable replacement format should decode");
        assert_eq!(saved.erasure.this, store.pools[0].format.erasure.sets[0][3]);
        recovery_latch.store(true, std::sync::atomic::Ordering::SeqCst);
        barrier.release();

        let (result, err) = tokio::time::timeout(std::time::Duration::from_secs(30), &mut heal)
            .await
            .expect("format heal should stop after the lost fence")
            .expect("format heal task should not panic")
            .expect("format heal should return its fenced result");
        assert!(matches!(err, Some(StorageError::SlowDown)));
        assert!(
            result
                .after
                .drives
                .iter()
                .any(|drive| drive.endpoint == target && drive.state == DriveState::Missing.to_string()),
            "the lost fence must prevent the durable format from being published in the heal result"
        );
        assert!(
            store.pools[0].disk_set[0].disks.read().await[3].is_none(),
            "the lost fence must prevent renew_disk from attaching the replacement"
        );
        recovery_latch.store(false, std::sync::atomic::Ordering::SeqCst);
        store
            .ensure_pool_meta_side_effects_safe("post-save format fence loss side effect")
            .await
            .expect_err("post-save format fence loss must remain sticky");
        shutdown.cancel();
    }

    #[tokio::test]
    async fn heal_object_pool_scope_selects_only_requested_pool() {
        let store = minimal_heal_store().await;
        let pools = store
            .get_pools_for_heal_object(&HealOpts {
                pool: Some(1),
                ..Default::default()
            })
            .expect("requested pool should be selected");

        assert_eq!(pools.len(), 1);
        assert!(Arc::ptr_eq(&pools[0], &store.pools[1]));
    }

    #[tokio::test]
    async fn heal_object_pool_scope_rejects_invalid_pool() {
        let store = minimal_heal_store().await;
        let err = store
            .get_pools_for_heal_object(&HealOpts {
                pool: Some(2),
                ..Default::default()
            })
            .expect_err("out-of-range pool scope must fail closed");

        assert!(
            matches!(err, StorageError::InvalidArgument(_, ref field, ref reason)
                if field == "pool" && reason.contains("invalid heal pool index 2 for 2 pools")),
            "unexpected invalid pool error: {err:?}"
        );
    }

    #[tokio::test]
    async fn scoped_heal_object_defers_when_requested_pool_is_suspended() {
        let mut store = minimal_heal_store().await;
        store.pool_meta = RwLock::new(PoolMeta {
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
                    decommission: Some(PoolDecommissionInfo {
                        start_time: Some(OffsetDateTime::UNIX_EPOCH),
                        ..Default::default()
                    }),
                },
            ],
            ..Default::default()
        });

        let (_, err) = store
            .handle_heal_object(
                "bucket",
                "object",
                "",
                &HealOpts {
                    pool: Some(1),
                    set: Some(0),
                    ..Default::default()
                },
            )
            .await
            .expect("suspended pool should return a deferred heal result");

        assert!(matches!(err, Some(StorageError::SlowDown)));

        let (_, err) = store
            .handle_heal_object(
                "bucket",
                "object",
                "",
                &HealOpts {
                    set: Some(1),
                    ..Default::default()
                },
            )
            .await
            .expect("unscoped heal should return the active pool result");

        assert!(matches!(err, Some(StorageError::InvalidArgument(_, ref field, _)) if field == "set"));

        let err = store
            .handle_heal_object(
                "bucket",
                "object",
                "",
                &HealOpts {
                    pool: Some(1),
                    set: Some(1),
                    ..Default::default()
                },
            )
            .await
            .expect_err("invalid set scope should fail before suspended pool deferral");

        assert!(matches!(err, StorageError::InvalidArgument(_, ref field, _) if field == "set"));

        {
            let mut pool_meta = store.pool_meta.write().await;
            let decommission = pool_meta.pools[1]
                .decommission
                .as_mut()
                .expect("test pool should have decommission state");
            decommission.complete = true;
        }
        let (_, err) = store
            .handle_heal_object(
                "bucket",
                "object",
                "",
                &HealOpts {
                    pool: Some(1),
                    set: Some(0),
                    ..Default::default()
                },
            )
            .await
            .expect("completed pool should return a terminal heal result");

        assert!(matches!(
            err,
            Some(StorageError::InvalidArgument(_, ref field, ref reason))
                if field == "pool" && reason.contains("completed decommission")
        ));

        for canceled in [false, true] {
            {
                let mut pool_meta = store.pool_meta.write().await;
                let decommission = pool_meta.pools[1]
                    .decommission
                    .as_mut()
                    .expect("test pool should have decommission state");
                decommission.complete = false;
                decommission.failed = !canceled;
                decommission.canceled = canceled;
            }
            let (_, err) = store
                .handle_heal_object(
                    "bucket",
                    "object",
                    "",
                    &HealOpts {
                        pool: Some(1),
                        set: Some(0),
                        ..Default::default()
                    },
                )
                .await
                .expect("clearable terminal pool should return a deferred heal result");

            assert!(matches!(err, Some(StorageError::SlowDown)));
        }
    }

    #[tokio::test]
    async fn scoped_heal_bucket_blocks_before_dispatch_when_pool_is_suspended() {
        let mut store = minimal_heal_store().await;
        store.pool_meta = RwLock::new(PoolMeta {
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
                    decommission: Some(PoolDecommissionInfo {
                        start_time: Some(OffsetDateTime::UNIX_EPOCH),
                        ..Default::default()
                    }),
                },
            ],
            ..Default::default()
        });

        let err = store
            .handle_heal_bucket(
                "bucket",
                &HealOpts {
                    pool: Some(1),
                    ..Default::default()
                },
            )
            .await
            .expect_err("suspended pool must be blocked before bucket-heal fan-out");
        assert_eq!(err, Error::SlowDown);

        store.pool_meta.write().await.pools[1]
            .decommission
            .as_mut()
            .expect("decommission state should exist")
            .complete = true;
        let err = store
            .handle_heal_bucket(
                "bucket",
                &HealOpts {
                    pool: Some(1),
                    ..Default::default()
                },
            )
            .await
            .expect_err("completed pool must remain fenced from bucket heal");
        assert!(
            matches!(err, StorageError::InvalidArgument(_, ref field, ref reason)
                if field == "pool" && reason.contains("completed decommission")),
            "unexpected completed-pool error: {err:?}"
        );
    }

    #[tokio::test]
    async fn bucket_heal_blocks_before_dispatch_after_unreadable_pool_meta_replica() {
        let store = minimal_heal_store().await;
        store.pool_meta_save_gate.lock().await.observe_replicas(PoolMetaReplicaState {
            needs_repair: true,
            repair_write_safe: false,
        });

        let err = store
            .handle_heal_bucket("bucket", &HealOpts::default())
            .await
            .expect_err("bucket heal must stay blocked until restart after an unreadable replica");

        assert!(
            err.to_string()
                .contains("restart after all replicas are readable and consistent")
        );
    }

    #[tokio::test]
    async fn bucket_heal_releases_save_gate_before_peer_dispatch_and_holds_movement_snapshot() {
        let mut store = minimal_heal_store().await;
        let started = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let peer: Box<dyn PeerS3Client> = Box::new(BlockingHealPeer {
            started: started.clone(),
            release: release.clone(),
        });
        store.peer_sys.clients = vec![Arc::new(peer)];
        let store = Arc::new(store);
        let movement_gate = store.ctx.data_movement_operation_gate();
        let mut heal = tokio::spawn({
            let store = store.clone();
            async move { store.handle_heal_bucket("bucket", &HealOpts::default()).await }
        });

        tokio::time::timeout(std::time::Duration::from_secs(1), started.notified())
            .await
            .expect("peer dispatch should start");
        assert!(
            store.pool_meta_save_gate.try_lock().is_ok(),
            "coordinator must release its local save gate before waiting for peers"
        );
        assert!(
            movement_gate.try_write().is_err(),
            "bucket heal must hold the movement snapshot through peer dispatch"
        );

        release.notify_one();
        tokio::time::timeout(std::time::Duration::from_secs(1), &mut heal)
            .await
            .expect("bucket heal should finish after peer release")
            .expect("bucket heal task should not panic")
            .expect("bucket heal should succeed");
    }

    #[tokio::test]
    async fn bucket_heal_local_fanout_does_not_reenter_movement_read_behind_queued_writer() {
        let mut store = minimal_heal_store().await;
        let movement_gate = store.ctx.data_movement_operation_gate();
        let writer_queued = Arc::new(tokio::sync::Notify::new());
        let writer_acquired = Arc::new(tokio::sync::Notify::new());
        let peer: Box<dyn PeerS3Client> = Box::new(WriterQueuedLocalHealPeer {
            movement_gate: movement_gate.clone(),
            writer_queued: writer_queued.clone(),
            writer_acquired: writer_acquired.clone(),
        });
        store.peer_sys.clients = vec![Arc::new(peer)];
        let store = Arc::new(store);
        let mut heal = tokio::spawn({
            let store = store.clone();
            async move { store.handle_heal_bucket("bucket", &HealOpts::default()).await }
        });

        tokio::time::timeout(std::time::Duration::from_secs(1), writer_queued.notified())
            .await
            .expect("movement writer should queue during local peer lookup");
        tokio::time::timeout(std::time::Duration::from_secs(1), &mut heal)
            .await
            .expect("local fan-out must not reenter movement read behind the queued writer")
            .expect("bucket heal task should not panic")
            .expect("bucket heal should succeed");
        tokio::time::timeout(std::time::Duration::from_secs(1), writer_acquired.notified())
            .await
            .expect("queued movement writer should proceed after bucket heal releases its read guard");
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn unscoped_heal_object_suspended_owner_semantics() {
        let (_temp_dir, store, shutdown) = multi_pool_heal_store().await;
        let bucket = format!("heal-owner-{}", Uuid::new_v4().simple());
        let active_object = "active-owner";
        let suspended_only_object = "suspended-only";
        let duplicate_object = "duplicate-owner";
        let marker_object = "marker-owner";
        let quorum_object = "quorum-owner";
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket should be created in all pools");

        let mut active_reader = PutObjReader::from_vec(b"active owner".to_vec());
        store.pools[0]
            .put_object(&bucket, active_object, &mut active_reader, &ObjectOptions::default())
            .await
            .expect("active owner object should be written");
        let active_disks = store.pools[0].disk_set[0].disks.read().await.clone();
        let missing_active_disk = active_disks[0].clone().expect("active disk should be online");
        missing_active_disk
            .delete(
                &bucket,
                active_object,
                DeleteOptions {
                    recursive: true,
                    immediate: true,
                    ..Default::default()
                },
            )
            .await
            .expect("active owner shard should be removed for repair");
        assert!(
            missing_active_disk.read_xl(&bucket, active_object, false).await.is_err(),
            "the active owner fixture must start with one missing metadata copy"
        );

        let mut suspended_reader = PutObjReader::from_vec(b"suspended owner".to_vec());
        store.pools[1]
            .put_object(&bucket, suspended_only_object, &mut suspended_reader, &ObjectOptions::default())
            .await
            .expect("suspended owner object should be written");
        for (pool_index, mod_time) in [1_i64, 2_i64].into_iter().enumerate() {
            let mut duplicate_reader = PutObjReader::from_vec(format!("duplicate-pool-{pool_index}").into_bytes());
            store.pools[pool_index]
                .put_object(
                    &bucket,
                    duplicate_object,
                    &mut duplicate_reader,
                    &ObjectOptions {
                        mod_time: Some(OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(mod_time)),
                        ..Default::default()
                    },
                )
                .await
                .expect("duplicate owner object should be written");
        }
        let duplicate_missing_disk = store.pools[0].disk_set[0].disks.read().await[0]
            .clone()
            .expect("duplicate active owner disk should be online");
        duplicate_missing_disk
            .delete(
                &bucket,
                duplicate_object,
                DeleteOptions {
                    recursive: true,
                    immediate: true,
                    ..Default::default()
                },
            )
            .await
            .expect("duplicate active owner shard should be removed for repair");
        let history_version = Uuid::new_v4();
        let mut history_reader = PutObjReader::from_vec(b"marker history".to_vec());
        store.pools[0]
            .put_object(
                &bucket,
                marker_object,
                &mut history_reader,
                &ObjectOptions {
                    versioned: true,
                    version_id: Some(history_version.to_string()),
                    mod_time: Some(OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(1)),
                    ..Default::default()
                },
            )
            .await
            .expect("versioned marker history should be written");
        store.pools[0]
            .delete_object(
                &bucket,
                marker_object,
                ObjectOptions {
                    versioned: true,
                    mod_time: Some(OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(2)),
                    ..Default::default()
                },
            )
            .await
            .expect("delete marker should be written");
        let mut quorum_reader = PutObjReader::from_vec(b"quorum boundary".to_vec());
        store.pools[0]
            .put_object(&bucket, quorum_object, &mut quorum_reader, &ObjectOptions::default())
            .await
            .expect("quorum boundary object should be written");
        {
            let mut pool_meta = store.pool_meta.write().await;
            let mut next = PoolMeta::new(&store.pools, &pool_meta);
            next.pools[1].decommission = Some(PoolDecommissionInfo {
                start_time: Some(OffsetDateTime::UNIX_EPOCH),
                ..Default::default()
            });
            *pool_meta = next;
        }

        let (_, duplicate_owner) = store
            .get_latest_object_info_with_idx(&bucket, duplicate_object, &ObjectOptions::default())
            .await
            .expect("duplicate owner should resolve");
        assert_eq!(duplicate_owner, 1, "latest duplicate must win when all pools are eligible");
        let (_, active_duplicate_owner) = store
            .get_latest_object_info_with_idx(
                &bucket,
                duplicate_object,
                &ObjectOptions {
                    skip_decommissioned: true,
                    ..Default::default()
                },
            )
            .await
            .expect("active duplicate owner should resolve");
        assert_eq!(
            active_duplicate_owner, 0,
            "suspended duplicate must be excluded from active owner selection"
        );
        let (duplicate_result, duplicate_err) = store
            .handle_heal_object(&bucket, duplicate_object, "", &HealOpts::default())
            .await
            .expect("duplicate owner heal should complete through the production path");
        assert_eq!(duplicate_result.object, duplicate_object);
        assert!(duplicate_err.is_none(), "active duplicate should be repaired: {duplicate_err:?}");
        assert!(
            duplicate_missing_disk.read_xl(&bucket, duplicate_object, false).await.is_ok(),
            "production heal must repair the active duplicate owner rather than the suspended owner"
        );
        let (marker_info, marker_owner) = store
            .get_latest_object_info_with_idx(
                &bucket,
                marker_object,
                &ObjectOptions {
                    skip_decommissioned: true,
                    versioned: true,
                    ..Default::default()
                },
            )
            .await
            .expect("latest delete marker should resolve");
        assert_eq!(marker_owner, 0);
        assert!(marker_info.delete_marker, "latest version must preserve delete-marker semantics");

        let (active_result, active_err) = store
            .handle_heal_object(&bucket, active_object, "", &HealOpts::default())
            .await
            .expect("unscoped active-owner heal should complete");
        assert_eq!(active_result.object, active_object);
        assert!(active_err.is_none(), "active owner must be selected even with a suspended pool");
        assert!(
            missing_active_disk.read_xl(&bucket, active_object, false).await.is_ok(),
            "active owner heal must write the missing disk metadata: result={active_result:?}, err={active_err:?}"
        );
        assert!(
            store.pools[1]
                .get_object_info(&bucket, active_object, &ObjectOptions::default())
                .await
                .is_err(),
            "the suspended pool must not be written for an active-owner object"
        );

        let (suspended_result, suspended_err) = store
            .handle_heal_object(&bucket, suspended_only_object, "", &HealOpts::default())
            .await
            .expect("unscoped suspended-only heal should return a terminal result");
        assert!(suspended_result.object.is_empty());
        assert!(matches!(suspended_err, Some(Error::FileNotFound)));
        assert!(
            store.pools[1]
                .get_object_info(&bucket, suspended_only_object, &ObjectOptions::default())
                .await
                .is_ok(),
            "suspended-only data must remain untouched when unscoped heal reports absent"
        );

        let (_, explicit_err) = store
            .handle_heal_object(
                &bucket,
                suspended_only_object,
                "",
                &HealOpts {
                    pool: Some(1),
                    ..Default::default()
                },
            )
            .await
            .expect("explicit suspended-owner heal should return a mapped error");
        assert!(matches!(explicit_err, Some(Error::SlowDown)));

        let original_quorum_disks = store.pools[0].disk_set[0].disks.read().await.clone();
        let surviving_quorum_disk = original_quorum_disks[3].clone();
        *store.pools[0].disk_set[0].disks.write().await = vec![None, None, None, surviving_quorum_disk];
        let (_, quorum_err) = store
            .handle_heal_object(&bucket, quorum_object, "", &HealOpts::default())
            .await
            .expect("quorum boundary heal should return a mapped result");
        *store.pools[0].disk_set[0].disks.write().await = original_quorum_disks;
        assert!(
            matches!(quorum_err, Some(Error::ErasureReadQuorum)),
            "quorum-boundary heal must preserve quorum error, got {quorum_err:?}"
        );
        shutdown.cancel();
    }

    #[tokio::test]
    async fn handle_heal_format_continues_after_a_pool_error() {
        let canonical_format = FormatV3::new(1, 3);
        let mut foreign_format = canonical_format.clone();
        foreign_format.id = Uuid::new_v4();
        let mut temp_dirs = Vec::new();
        let mut endpoints = Vec::new();
        let mut disks = Vec::new();

        for disk_index in 0..3 {
            let temp_dir = tempfile::tempdir().expect("temporary disk root should be created");
            let mut endpoint = Endpoint::try_from(temp_dir.path().to_str().expect("temporary path should be UTF-8"))
                .expect("temporary endpoint should parse");
            endpoint.set_pool_index(0);
            endpoint.set_set_index(0);
            endpoint.set_disk_index(disk_index);
            let disk = new_disk(
                &endpoint,
                &DiskOption {
                    cleanup: false,
                    health_check: false,
                },
            )
            .await
            .expect("temporary disk should open");
            let mut disk_format = foreign_format.clone();
            disk_format.erasure.this = foreign_format.erasure.sets[0][disk_index];
            save_format_file(&Some(disk.clone()), &Some(disk_format))
                .await
                .expect("foreign format should be written");
            temp_dirs.push(temp_dir);
            endpoints.push(endpoint);
            disks.push(Some(disk));
        }

        let pool_endpoints = PoolEndpoints {
            legacy: false,
            set_count: 1,
            drives_per_set: 3,
            endpoints: Endpoints::from(endpoints),
            cmd_line: "foreign-format-majority-test".to_string(),
            platform: "test".to_string(),
        };
        let pool = Sets::new(disks, &pool_endpoints, &canonical_format, 0, 1)
            .await
            .expect("test pool should build around the cached canonical format");

        let mut recoverable_format = FormatV3::new(1, 3);
        recoverable_format.id = canonical_format.id;
        let mut recoverable_temp_dirs = Vec::new();
        let mut recoverable_endpoints = Vec::new();
        let mut recoverable_disks = Vec::new();
        let mut unformatted_disk = None;
        for disk_index in 0..3 {
            let temp_dir = tempfile::tempdir().expect("temporary disk root should be created");
            let mut endpoint = Endpoint::try_from(temp_dir.path().to_str().expect("temporary path should be UTF-8"))
                .expect("temporary endpoint should parse");
            endpoint.set_pool_index(1);
            endpoint.set_set_index(0);
            endpoint.set_disk_index(disk_index);
            let disk = new_disk(
                &endpoint,
                &DiskOption {
                    cleanup: false,
                    health_check: false,
                },
            )
            .await
            .expect("temporary disk should open");
            if disk_index < 2 {
                let mut disk_format = recoverable_format.clone();
                disk_format.erasure.this = recoverable_format.erasure.sets[0][disk_index];
                save_format_file(&Some(disk.clone()), &Some(disk_format))
                    .await
                    .expect("recoverable format should be written");
            } else {
                unformatted_disk = Some(disk.clone());
            }
            recoverable_temp_dirs.push(temp_dir);
            recoverable_endpoints.push(endpoint);
            recoverable_disks.push(Some(disk));
        }
        let recoverable_pool_endpoints = PoolEndpoints {
            legacy: false,
            set_count: 1,
            drives_per_set: 3,
            endpoints: Endpoints::from(recoverable_endpoints),
            cmd_line: "recoverable-format-test".to_string(),
            platform: "test".to_string(),
        };
        let recoverable_pool = Sets::new(recoverable_disks, &recoverable_pool_endpoints, &recoverable_format, 1, 1)
            .await
            .expect("recoverable test pool should build");

        let endpoint_pools = EndpointServerPools::from(vec![pool_endpoints.clone(), recoverable_pool_endpoints.clone()]);
        let store = ECStore {
            id: canonical_format.id,
            disk_map: HashMap::new(),
            pools: vec![pool, recoverable_pool],
            peer_sys: S3PeerSys::new(&endpoint_pools),
            pool_meta: RwLock::new(PoolMeta::default()),
            rebalance_meta: RwLock::new(None),
            decommission_cancelers: RwLock::new(Vec::new()),
            start_gate: Mutex::new(()),
            pool_meta_save_gate: Mutex::default(),
            ctx: crate::runtime::instance::bootstrap_ctx(),
            bucket_fence_registry: std::sync::Arc::default(),
        };

        let err = store
            .handle_heal_format(false)
            .await
            .expect_err("missing pool metadata must fail closed before format writes");
        assert!(err.to_string().contains("no durable bootstrap identity or pool.bin replica"));
        store
            .ensure_pool_meta_side_effects_safe("missing format-heal metadata side effect")
            .await
            .expect_err("missing metadata must latch the format-heal write gate");

        let pool_meta = PoolMeta::new(&store.pools, &PoolMeta::default());
        pool_meta
            .save_for_startup(store.pools.clone())
            .await
            .expect("pool metadata should be persisted before format heal");
        *store.pool_meta_save_gate.lock().await = PoolMetaWriteState::default();

        let (result, err) = store
            .handle_heal_format(false)
            .await
            .expect("format heal should return the typed pool error");
        assert!(
            matches!(err, Some(StorageError::CorruptedFormat)),
            "foreign format majority must not be downgraded to a successful heal: {err:?}"
        );
        assert_eq!(result.disk_count, 3, "the recoverable pool should still be inspected");
        let healed = load_format_erasure(&unformatted_disk.expect("the unformatted disk handle should be retained"), true)
            .await
            .expect("the later pool should be healed despite the first pool error");
        assert_eq!(healed.erasure.this, recoverable_format.erasure.sets[0][2]);

        let mut completed_meta = PoolMeta::new(&store.pools, &PoolMeta::default());
        for status in &mut completed_meta.pools {
            status.decommission = Some(PoolDecommissionInfo {
                complete: true,
                ..Default::default()
            });
        }
        completed_meta
            .save(store.pools.clone())
            .await
            .expect("completed pool metadata should be persisted");
        let (_, err) = store
            .handle_heal_format(false)
            .await
            .expect("completed pools should be reported as a no-op");
        assert!(matches!(err, Some(StorageError::NoHealRequired)));
    }
}
