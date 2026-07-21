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
use crate::bucket::{
    metadata::{BUCKET_TABLE_RESERVED_PREFIX, table_bucket_catalog_metadata_prefix},
    utils::is_meta_bucketname,
};
use crate::error::is_err_bucket_not_found;
use crate::runtime::sources as runtime_sources;
use crate::set_disk::get_lock_acquire_timeout;
use crate::storage_api_contracts::bucket::SRBucketDeleteOp;
use crate::storage_api_contracts::namespace::NamespaceLocking as _;
use futures::stream::{self, StreamExt};
use std::collections::BTreeMap;
use std::future::Future;

const DELETED_BUCKETS_PREFIX: &str = ".deleted";
const SCANNER_BUCKET_LIST_SET_CONCURRENCY: usize = 4;

fn scanner_bucket_list_set_concurrency(set_count: usize) -> usize {
    set_count.clamp(1, SCANNER_BUCKET_LIST_SET_CONCURRENCY)
}

fn should_override_created_from_metadata(created: OffsetDateTime) -> bool {
    created != OffsetDateTime::UNIX_EPOCH
}

fn validate_table_bucket_delete_allowed(
    bucket: &str,
    table_bucket_enabled: bool,
    table_catalog_metadata_exists: bool,
) -> Result<()> {
    if table_bucket_enabled && table_catalog_metadata_exists {
        return Err(StorageError::BucketNotEmpty(bucket.to_string()));
    }

    Ok(())
}

async fn table_catalog_metadata_exists(ctx: &crate::runtime::instance::InstanceContext, bucket: &str) -> Result<bool> {
    let local_disks = runtime_sources::local_disks_in(ctx).await;
    for disk in local_disks.iter() {
        let catalog_path = disk.path().join(bucket).join(BUCKET_TABLE_RESERVED_PREFIX);
        if has_xlmeta_files(&catalog_path).await? {
            return Ok(true);
        }
    }

    Ok(false)
}

async fn validate_table_bucket_delete_guard(ctx: &crate::runtime::instance::InstanceContext, bucket: &str) -> Result<()> {
    let table_bucket_enabled = metadata_sys::get_in(ctx, bucket)
        .await
        .is_ok_and(|metadata| metadata.table_bucket_enabled());
    if table_bucket_enabled {
        validate_table_bucket_delete_allowed(bucket, true, table_catalog_metadata_exists(ctx, bucket).await?)?;
    }

    Ok(())
}

fn bucket_delete_metadata_cleanup_prefixes(bucket: &str) -> [String; 2] {
    [
        table_bucket_catalog_metadata_prefix(bucket),
        format!("{BUCKET_META_PREFIX}/{bucket}"),
    ]
}

fn bucket_deleted_marker_prefix(bucket: &str) -> String {
    format!("{BUCKET_META_PREFIX}/{DELETED_BUCKETS_PREFIX}/{bucket}")
}

fn bucket_deleted_marker_volume(bucket: &str) -> String {
    format!("{RUSTFS_META_BUCKET}/{}", bucket_deleted_marker_prefix(bucket))
}

async fn await_bucket_namespace_operation<T, F>(
    guard: Option<&rustfs_lock::NamespaceLockGuard>,
    bucket: &str,
    operation: &'static str,
    future: F,
) -> Result<T>
where
    F: Future<Output = Result<T>>,
{
    let Some(guard) = guard else {
        return future.await;
    };
    if guard.is_lock_lost() {
        return Err(StorageError::other(format!(
            "bucket namespace lock was lost before {operation}: {bucket}"
        )));
    }
    tokio::select! {
        biased;
        _ = guard.lock_lost_notified() => Err(StorageError::other(format!(
            "bucket namespace lock was lost during {operation}: {bucket}"
        ))),
        result = future => result,
    }
}

impl ECStore {
    async fn mark_bucket_deleted(&self, bucket: &str) -> Result<()> {
        let marker_volume = bucket_deleted_marker_volume(bucket);

        self.peer_sys
            .make_bucket(
                marker_volume.as_str(),
                &MakeBucketOptions {
                    force_create: true,
                    ..Default::default()
                },
            )
            .await
            .map_err(|e| to_object_err(e.into(), vec![bucket]))?;

        Ok(())
    }

    async fn cleanup_deleted_bucket_metadata(
        &self,
        bucket: &str,
        include_deleted_marker: bool,
        guard: Option<&rustfs_lock::NamespaceLockGuard>,
    ) -> Result<()> {
        for prefix in bucket_delete_metadata_cleanup_prefixes(bucket) {
            await_bucket_namespace_operation(
                guard,
                bucket,
                "deleted bucket metadata cleanup",
                self.delete_all(RUSTFS_META_BUCKET, prefix.as_str()),
            )
            .await?;
        }

        if include_deleted_marker {
            let marker_prefix = bucket_deleted_marker_prefix(bucket);
            await_bucket_namespace_operation(
                guard,
                bucket,
                "deleted bucket marker cleanup",
                self.delete_all(RUSTFS_META_BUCKET, marker_prefix.as_str()),
            )
            .await?;
        }

        await_bucket_namespace_operation(
            guard,
            bucket,
            "deleted bucket metadata cache cleanup",
            metadata_sys::remove_bucket_metadata_in(&self.ctx, bucket),
        )
        .await?;
        runtime_sources::delete_bucket_monitor_entry(bucket);
        Ok(())
    }

    async fn cleanup_bucket_usage_best_effort(&self, bucket: &str, guard: Option<&rustfs_lock::NamespaceLockGuard>) {
        if let Err(err) = await_bucket_namespace_operation(
            guard,
            bucket,
            "bucket usage cleanup",
            crate::data_usage::remove_bucket_usage_for_namespace_change(self, bucket),
        )
        .await
        {
            warn!(
                bucket = %bucket,
                error = ?err,
                "bucket data usage cleanup deferred to scanner reconciliation"
            );
        }
    }

    #[instrument(skip(self))]
    pub(super) async fn handle_make_bucket(&self, bucket: &str, opts: &MakeBucketOptions) -> Result<()> {
        if !is_meta_bucketname(bucket)
            && let Err(err) = check_valid_bucket_name_strict(bucket)
        {
            return Err(StorageError::BucketNameInvalid(err.to_string()));
        }

        let ns_guard = if !opts.no_lock {
            let ns_lock = self.new_ns_lock(bucket, bucket).await?;
            Some(
                ns_lock
                    .get_write_lock(get_lock_acquire_timeout())
                    .await
                    .map_err(|e| match e {
                        rustfs_lock::error::LockError::QuorumNotReached { required, achieved } => {
                            StorageError::NamespaceLockQuorumUnavailable {
                                mode: "write",
                                bucket: bucket.to_string(),
                                object: bucket.to_string(),
                                required,
                                achieved,
                            }
                        }
                        other => StorageError::Lock(other),
                    })?,
            )
        } else {
            None
        };

        let confirmed_missing = match self.peer_sys.get_bucket_info(bucket, &BucketOptions::default()).await {
            Ok(_) => false,
            Err(err) => {
                let err: StorageError = err.into();
                if is_err_bucket_not_found(&err) {
                    true
                } else {
                    return Err(to_object_err(err, vec![bucket]));
                }
            }
        };

        if confirmed_missing && !is_meta_bucketname(bucket) {
            self.cleanup_bucket_usage_best_effort(bucket, ns_guard.as_ref()).await;
        }

        if let Err(err) = await_bucket_namespace_operation(ns_guard.as_ref(), bucket, "physical bucket creation", async {
            self.peer_sys
                .make_bucket(bucket, opts)
                .await
                .map_err(|err| to_object_err(err.into(), vec![bucket]))
        })
        .await
        {
            if is_err_bucket_exists(&err)
                && let Err(heal_err) = self
                    .handle_heal_bucket(
                        bucket,
                        &HealOpts {
                            recreate: true,
                            ..Default::default()
                        },
                    )
                    .await
            {
                warn!("best-effort bucket heal after BucketExists failed: {heal_err}");
            }
            if !is_err_bucket_exists(&err) && ns_guard.as_ref().is_none_or(|guard| !guard.is_lock_lost()) {
                error!("make bucket failed: {err}");
                let _ = self
                    .delete_bucket(
                        bucket,
                        &DeleteBucketOptions {
                            no_lock: true,
                            no_recreate: true,
                            ..Default::default()
                        },
                    )
                    .await;
            }
            return Err(err);
        };

        let mut meta = BucketMetadata::new(bucket);

        meta.set_created(opts.created_at);

        if opts.lock_enabled {
            meta.object_lock_config_xml = crate::bucket::utils::serialize::<ObjectLockConfiguration>(&enableObjcetLockConfig)?;
            meta.versioning_config_xml = crate::bucket::utils::serialize::<VersioningConfiguration>(&enableVersioningConfig)?;
        }

        if opts.versioning_enabled {
            meta.versioning_config_xml = crate::bucket::utils::serialize::<VersioningConfiguration>(&enableVersioningConfig)?;
        }

        await_bucket_namespace_operation(
            ns_guard.as_ref(),
            bucket,
            "bucket metadata initialization",
            metadata_sys::set_bucket_metadata_in(&self.ctx, meta),
        )
        .await?;

        Ok(())
    }

    #[instrument(skip(self))]
    pub(super) async fn handle_get_bucket_info(&self, bucket: &str, opts: &BucketOptions) -> Result<BucketInfo> {
        let mut info = self.peer_sys.get_bucket_info(bucket, opts).await?;

        if let Ok(sys) = metadata_sys::get_in(&self.ctx, bucket).await {
            if should_override_created_from_metadata(sys.created) {
                info.created = Some(sys.created);
            }
            info.versioning = sys.versioning();
            info.object_locking = sys.object_locking();
        }

        Ok(info)
    }

    #[instrument(skip(self))]
    pub(super) async fn handle_list_bucket(&self, opts: &BucketOptions) -> Result<Vec<BucketInfo>> {
        // TODO: opts.cached

        let mut buckets = self.peer_sys.list_bucket(opts).await?;

        if !opts.no_metadata {
            for bucket in buckets.iter_mut() {
                if let Ok(created) = metadata_sys::created_at_in(&self.ctx, &bucket.name).await
                    && should_override_created_from_metadata(created)
                {
                    bucket.created = Some(created);
                }
            }
        }
        Ok(buckets)
    }

    pub async fn list_bucket_for_scanner(&self, opts: &BucketOptions) -> Result<crate::cluster::rpc::ScannerBucketListing> {
        let sets = self
            .pools
            .iter()
            .flat_map(|pool| {
                pool.disk_set
                    .iter()
                    .map(|set| (set.pool_index, set.set_index, Arc::clone(set)))
            })
            .collect::<Vec<_>>();
        let set_count = sets.len();
        let deleted = opts.deleted;
        let cached = opts.cached;
        let no_metadata = opts.no_metadata;
        let mut set_listings = stream::iter(sets.into_iter().map(move |(pool_index, set_index, set)| {
            let opts = BucketOptions {
                deleted,
                cached,
                no_metadata,
            };
            async move {
                set.list_bucket_for_scanner(&opts)
                    .await
                    .map(|(buckets, complete)| (pool_index, set_index, buckets, complete))
            }
        }))
        .buffer_unordered(scanner_bucket_list_set_concurrency(set_count));
        let mut topology_complete = set_count != 0;
        let mut bucket_map = BTreeMap::<String, BucketInfo>::new();
        let mut scoped_buckets = Vec::with_capacity(set_count);
        while let Some(set_listing) = set_listings.next().await {
            let (pool_index, set_index, buckets, set_complete) = set_listing?;
            topology_complete &= set_complete;
            for bucket in &buckets {
                bucket_map.entry(bucket.name.clone()).or_insert_with(|| bucket.clone());
            }
            scoped_buckets.push(crate::cluster::rpc::ScannerSetBucketListing {
                pool_index,
                set_index,
                buckets,
            });
        }
        scoped_buckets.sort_unstable_by_key(|scope| (scope.pool_index, scope.set_index));
        let mut listing = crate::cluster::rpc::ScannerBucketListing {
            buckets: bucket_map.into_values().collect(),
            set_buckets: scoped_buckets,
            topology_complete,
        };

        if !opts.no_metadata {
            for bucket in &mut listing.buckets {
                if let Ok(created) = metadata_sys::created_at_in(&self.ctx, &bucket.name).await
                    && should_override_created_from_metadata(created)
                {
                    bucket.created = Some(created);
                }
            }
            let created_by_bucket = listing
                .buckets
                .iter()
                .map(|bucket| (bucket.name.as_str(), bucket.created))
                .collect::<BTreeMap<_, _>>();
            for scope in &mut listing.set_buckets {
                for bucket in &mut scope.buckets {
                    if let Some(created) = created_by_bucket.get(bucket.name.as_str()) {
                        bucket.created = *created;
                    }
                }
            }
        }

        Ok(listing)
    }

    #[instrument(skip(self))]
    pub(super) async fn handle_delete_bucket(&self, bucket: &str, opts: &DeleteBucketOptions) -> Result<()> {
        if is_meta_bucketname(bucket) {
            return Err(StorageError::BucketNameInvalid(bucket.to_string()));
        }

        if let Err(err) = check_valid_bucket_name(bucket) {
            return Err(StorageError::BucketNameInvalid(err.to_string()));
        }

        let ns_guard = if !opts.no_lock {
            let ns_lock = self.new_ns_lock(bucket, bucket).await?;
            Some(
                ns_lock
                    .get_write_lock(get_lock_acquire_timeout())
                    .await
                    .map_err(|e| match e {
                        rustfs_lock::error::LockError::QuorumNotReached { required, achieved } => {
                            StorageError::NamespaceLockQuorumUnavailable {
                                mode: "write",
                                bucket: bucket.to_string(),
                                object: bucket.to_string(),
                                required,
                                achieved,
                            }
                        }
                        other => StorageError::Lock(other),
                    })?,
            )
        } else {
            None
        };

        let sr_mark_delete = opts.srdelete_op == SRBucketDeleteOp::MarkDelete;
        let sr_purge = opts.srdelete_op == SRBucketDeleteOp::Purge;
        let sr_delete = sr_mark_delete || sr_purge;
        let mut delete_opts = opts.clone();
        let bucket_exists = match self.peer_sys.get_bucket_info(bucket, &BucketOptions::default()).await {
            Ok(_) => true,
            Err(err) => {
                let storage_err: StorageError = err.into();
                if is_err_strict_volume_not_found(&storage_err) && sr_delete {
                    false
                } else if is_err_strict_volume_not_found(&storage_err) {
                    return Err(StorageError::BucketNotFound(bucket.to_string()));
                } else {
                    return Err(to_object_err(storage_err, vec![bucket]));
                }
            }
        };

        if bucket_exists {
            validate_table_bucket_delete_guard(&self.ctx, bucket).await?;

            // Check bucket is empty before deletion (per S3 API spec)
            // If bucket is not empty (contains actual objects with xl.meta files) and force
            // is not set, return BucketNotEmpty error.
            // Note: Empty directories (left after object deletion) should NOT count as objects.
            if !opts.force {
                let local_disks = runtime_sources::local_disks_in(&self.ctx).await;
                for disk in local_disks.iter() {
                    let bucket_path = disk.path().join(bucket);
                    if has_xlmeta_files(&bucket_path).await? {
                        return Err(StorageError::BucketNotEmpty(bucket.to_string()));
                    }
                }
                delete_opts.force_if_empty = true;
            }
        }

        if sr_delete && !bucket_exists {
            delete_opts.force_if_empty = true;
        }

        if sr_mark_delete {
            await_bucket_namespace_operation(
                ns_guard.as_ref(),
                bucket,
                "bucket delete marker creation",
                self.mark_bucket_deleted(bucket),
            )
            .await?;
        }

        let delete_result = await_bucket_namespace_operation(ns_guard.as_ref(), bucket, "physical bucket deletion", async {
            self.peer_sys
                .delete_bucket(bucket, &delete_opts)
                .await
                .map_err(|err| to_object_err(err.into(), vec![bucket]))
        })
        .await;
        if let Err(err) = delete_result
            && (!sr_delete || !is_err_strict_volume_not_found(&err))
        {
            list_objects::observe_scanner_namespace_mutations(bucket, 1);
            return Err(err);
        }

        self.cleanup_bucket_usage_best_effort(bucket, ns_guard.as_ref()).await;

        if let Err(err) = self
            .cleanup_deleted_bucket_metadata(bucket, sr_purge, ns_guard.as_ref())
            .await
        {
            warn!(
                bucket = %bucket,
                error = ?err,
                "physical bucket deletion succeeded but metadata cleanup remains pending"
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{
        SCANNER_BUCKET_LIST_SET_CONCURRENCY, await_bucket_namespace_operation, bucket_delete_metadata_cleanup_prefixes,
        bucket_deleted_marker_prefix, bucket_deleted_marker_volume, scanner_bucket_list_set_concurrency,
        should_override_created_from_metadata, validate_table_bucket_delete_allowed,
    };
    use crate::bucket::metadata::table_bucket_catalog_metadata_prefix;
    use crate::bucket::metadata_sys;
    use crate::disk::{BUCKET_META_PREFIX, RUSTFS_META_BUCKET};
    use crate::error::StorageError;
    use crate::object_api::{ObjectOptions, PutObjReader};
    use crate::runtime::instance::InstanceContext;
    use crate::storage_api_contracts::{
        bucket::{BucketOperations as _, BucketOptions, DeleteBucketOptions, MakeBucketOptions, SRBucketDeleteOp},
        object::{ObjectIO as _, ObjectOperations as _},
    };
    use crate::store::{ECStore, init_local_disks_with_instance_ctx};
    use crate::{
        disk::endpoint::Endpoint,
        layout::endpoints::{EndpointServerPools, Endpoints, PoolEndpoints},
    };
    use rustfs_data_usage::{BucketUsageInfo, DataUsageInfo};
    use rustfs_lock::{LocalClient, LockRequest, LockType, NamespaceLock, ObjectKey};
    use serial_test::serial;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::{Duration, SystemTime};
    use time::OffsetDateTime;
    use tokio::sync::OnceCell;
    use tokio_util::sync::CancellationToken;
    use uuid::Uuid;

    static BUCKET_DELETE_TEST_ENV: OnceCell<(Vec<PathBuf>, Arc<ECStore>)> = OnceCell::const_new();

    #[tokio::test(start_paused = true)]
    async fn bucket_namespace_operation_fails_closed_after_lease_expiry() {
        let ttl = Duration::from_millis(20);
        let lock = NamespaceLock::new("bucket-operation-test".to_string(), Arc::new(LocalClient::new()));
        let request = LockRequest::new(ObjectKey::new("bucket", ""), LockType::Exclusive, "test-owner")
            .with_acquire_timeout(Duration::from_secs(1))
            .with_ttl(ttl)
            .with_refresh_interval(ttl);
        let guard = lock
            .acquire_guard(&request)
            .await
            .expect("namespace lock acquisition should not fail")
            .expect("namespace lock should be acquired");
        tokio::time::advance(ttl + Duration::from_millis(1)).await;

        let operation_ran = Arc::new(AtomicBool::new(false));
        let operation_ran_for_future = operation_ran.clone();
        let result = await_bucket_namespace_operation(Some(&guard), "bucket", "test operation", async move {
            operation_ran_for_future.store(true, Ordering::SeqCst);
            Ok(())
        })
        .await;

        assert!(result.is_err(), "an expired namespace lease must fence the operation");
        assert!(
            !operation_ran.load(Ordering::SeqCst),
            "a fenced namespace operation must not poll its mutation future"
        );
    }

    async fn setup_bucket_delete_test_env() -> (Vec<PathBuf>, Arc<ECStore>) {
        BUCKET_DELETE_TEST_ENV
            .get_or_init(|| async {
                let temp_dir = std::env::temp_dir().join(format!("rustfs_bucket_delete_test_{}", Uuid::new_v4()));
                tokio::fs::create_dir_all(&temp_dir)
                    .await
                    .expect("test base directory should be created");

                let disk_paths = (0..4)
                    .map(|disk_idx| temp_dir.join(format!("disk{disk_idx}")))
                    .collect::<Vec<_>>();

                for disk_path in &disk_paths {
                    tokio::fs::create_dir_all(disk_path)
                        .await
                        .expect("disk directory should be created");
                }

                let mut endpoints = Vec::with_capacity(disk_paths.len());
                for (disk_idx, disk_path) in disk_paths.iter().enumerate() {
                    let mut endpoint =
                        Endpoint::try_from(disk_path.to_str().expect("disk path should be utf8")).expect("endpoint should parse");
                    endpoint.set_pool_index(0);
                    endpoint.set_set_index(0);
                    endpoint.set_disk_index(disk_idx);
                    endpoints.push(endpoint);
                }

                let endpoint_pools = EndpointServerPools(vec![PoolEndpoints {
                    legacy: false,
                    set_count: 1,
                    drives_per_set: 4,
                    endpoints: Endpoints::from(endpoints),
                    cmd_line: "bucket-delete-test".to_string(),
                    platform: format!("OS: {} | Arch: {}", std::env::consts::OS, std::env::consts::ARCH),
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
                let storage_class = crate::config::storageclass::lookup_config_for_pools_without_env(
                    &rustfs_config::server_config::KVS::new(),
                    &[4],
                )
                .expect("bucket test storage class should match its four-disk pool");
                for pool in &ecstore.pools {
                    for set in &pool.disk_set {
                        set.set_test_storage_class_config(storage_class.clone());
                    }
                }

                metadata_sys::init_bucket_metadata_sys(ecstore.clone(), Vec::new()).await;

                (disk_paths, ecstore)
            })
            .await
            .clone()
    }

    async fn setup_multi_pool_scanner_listing_test_env() -> (tempfile::TempDir, Arc<ECStore>) {
        let temp_dir = tempfile::tempdir().expect("multi-pool scanner test directory should be created");
        let mut pools = Vec::new();
        for pool_index in 0..2 {
            let mut endpoints = Vec::new();
            for disk_index in 0..4 {
                let disk_path = temp_dir.path().join(format!("pool{pool_index}-disk{disk_index}"));
                tokio::fs::create_dir_all(&disk_path)
                    .await
                    .expect("multi-pool scanner test disk should be created");
                let mut endpoint =
                    Endpoint::try_from(disk_path.to_str().expect("disk path should be utf8")).expect("endpoint should parse");
                endpoint.set_pool_index(pool_index);
                endpoint.set_set_index(0);
                endpoint.set_disk_index(disk_index);
                endpoints.push(endpoint);
            }
            pools.push(PoolEndpoints {
                legacy: false,
                set_count: 1,
                drives_per_set: 4,
                endpoints: Endpoints::from(endpoints),
                cmd_line: format!("scanner-listing-pool-{pool_index}"),
                platform: format!("OS: {} | Arch: {}", std::env::consts::OS, std::env::consts::ARCH),
            });
        }

        let endpoint_pools = EndpointServerPools(pools);
        let instance_ctx = Arc::new(InstanceContext::new());
        init_local_disks_with_instance_ctx(&instance_ctx, endpoint_pools.clone())
            .await
            .expect("multi-pool local disks should initialize");
        let ecstore = ECStore::new_with_instance_ctx(
            "127.0.0.1:0".parse().expect("test address"),
            endpoint_pools,
            CancellationToken::new(),
            instance_ctx,
        )
        .await
        .expect("multi-pool ECStore should initialize");
        let storage_class =
            crate::config::storageclass::lookup_config_for_pools_without_env(&rustfs_config::server_config::KVS::new(), &[4, 4])
                .expect("multi-pool storage class should match both four-disk pools");
        for pool in &ecstore.pools {
            for set in &pool.disk_set {
                set.set_test_storage_class_config(storage_class.clone());
            }
        }

        (temp_dir, ecstore)
    }

    async fn create_bucket_with_object(ecstore: &Arc<ECStore>, bucket: &str, object: &str) {
        let generation_before_make = ecstore.scanner_namespace_mutation_generation();
        ecstore
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket should be created");
        assert_eq!(
            ecstore.scanner_namespace_mutation_generation(),
            generation_before_make.saturating_add(1),
            "successful bucket creation should advance scanner namespace activity"
        );

        let generation_before_put = ecstore.scanner_namespace_mutation_generation();
        let mut reader = PutObjReader::from_vec(b"delete bucket semantics".to_vec());
        ecstore
            .put_object(bucket, object, &mut reader, &ObjectOptions::default())
            .await
            .expect("object should be written");
        assert_eq!(
            ecstore.scanner_namespace_mutation_generation(),
            generation_before_put.saturating_add(1),
            "successful object creation should advance scanner namespace activity"
        );
        ecstore
            .get_object_info(bucket, object, &ObjectOptions::default())
            .await
            .expect("object should be readable before bucket delete");
    }

    async fn any_disk_path_exists(disk_paths: &[PathBuf], relative_path: impl AsRef<Path>) -> bool {
        for disk_path in disk_paths {
            if tokio::fs::try_exists(disk_path.join(relative_path.as_ref()))
                .await
                .expect("test disk path should be stat-able")
            {
                return true;
            }
        }
        false
    }

    async fn any_disk_has_object_metadata(disk_paths: &[PathBuf], bucket: &str) -> bool {
        for disk_path in disk_paths {
            if super::has_xlmeta_files(&disk_path.join(bucket))
                .await
                .expect("object metadata scan should succeed")
            {
                return true;
            }
        }
        false
    }

    async fn write_bucket_metadata_marker(disk_paths: &[PathBuf], metadata_prefix: &str) {
        for disk_path in disk_paths {
            let marker_path = disk_path.join(metadata_prefix).join("config.json");
            tokio::fs::create_dir_all(marker_path.parent().expect("metadata marker path should have a parent"))
                .await
                .expect("metadata marker parent should be created");
            tokio::fs::write(marker_path, b"bucket metadata")
                .await
                .expect("metadata marker should be written");
        }
    }

    #[test]
    fn should_not_override_when_metadata_created_is_unix_epoch() {
        assert!(!should_override_created_from_metadata(OffsetDateTime::UNIX_EPOCH));
    }

    #[test]
    fn should_override_when_metadata_created_is_valid_time() {
        let created = OffsetDateTime::from_unix_timestamp(1704067200).expect("valid timestamp");
        assert!(should_override_created_from_metadata(created));
    }

    #[test]
    fn table_bucket_delete_guard_rejects_remaining_catalog_metadata() {
        let err = validate_table_bucket_delete_allowed("table-bucket", true, true).unwrap_err();

        assert!(matches!(err, StorageError::BucketNotEmpty(bucket) if bucket == "table-bucket"));
        assert!(validate_table_bucket_delete_allowed("table-bucket", true, false).is_ok());
        assert!(validate_table_bucket_delete_allowed("regular-bucket", false, true).is_ok());
    }

    #[test]
    fn bucket_delete_metadata_cleanup_removes_internal_table_catalog_prefix() {
        let prefixes = bucket_delete_metadata_cleanup_prefixes("analytics");

        assert!(prefixes.contains(&table_bucket_catalog_metadata_prefix("analytics")));
        assert!(prefixes.contains(&"buckets/analytics".to_string()));
    }

    #[test]
    fn bucket_delete_marker_path_uses_internal_deleted_bucket_metadata_prefix() {
        assert_eq!(bucket_deleted_marker_prefix("analytics"), "buckets/.deleted/analytics");
        assert_eq!(
            bucket_deleted_marker_volume("analytics"),
            format!("{RUSTFS_META_BUCKET}/{BUCKET_META_PREFIX}/.deleted/analytics")
        );
    }

    #[test]
    fn scanner_bucket_listing_bounds_set_fanout() {
        assert_eq!(scanner_bucket_list_set_concurrency(0), 1);
        assert_eq!(scanner_bucket_list_set_concurrency(2), 2);
        assert_eq!(scanner_bucket_list_set_concurrency(100), SCANNER_BUCKET_LIST_SET_CONCURRENCY);
    }

    #[tokio::test]
    #[serial]
    async fn scanner_bucket_listing_unions_every_erasure_set() {
        let (_temp_dir, ecstore) = setup_multi_pool_scanner_listing_test_env().await;
        let bucket = format!("second-pool-only-{}", Uuid::new_v4().simple());
        ecstore.pools[1].disk_set[0]
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket should be created in the second pool only");

        let listing = ecstore
            .list_bucket_for_scanner(&crate::storage_api_contracts::bucket::BucketOptions {
                no_metadata: true,
                ..Default::default()
            })
            .await
            .expect("scanner should enumerate every pool and set");

        assert!(listing.topology_complete);
        assert!(listing.buckets.iter().any(|entry| entry.name == bucket));
        assert_eq!(listing.set_buckets.len(), 2);
        assert!(
            listing
                .set_buckets
                .iter()
                .find(|scope| scope.pool_index == 0 && scope.set_index == 0)
                .is_some_and(|scope| scope.buckets.is_empty())
        );
        assert!(
            listing
                .set_buckets
                .iter()
                .find(|scope| scope.pool_index == 1 && scope.set_index == 0)
                .is_some_and(|scope| scope.buckets.iter().any(|entry| entry.name == bucket))
        );
    }

    #[tokio::test]
    async fn scanner_bucket_listing_marks_degraded_set_incomplete() {
        let (_temp_dir, ecstore) = setup_multi_pool_scanner_listing_test_env().await;
        let bucket = format!("degraded-set-{}", Uuid::new_v4().simple());
        let set = &ecstore.pools[0].disk_set[0];
        set.make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket should be created before a disk is removed");
        set.disks.write().await[0] = None;

        let listing = ecstore
            .list_bucket_for_scanner(&crate::storage_api_contracts::bucket::BucketOptions {
                no_metadata: true,
                ..Default::default()
            })
            .await
            .expect("a degraded set with quorum should still return candidate buckets");

        assert!(listing.buckets.iter().any(|entry| entry.name == bucket));
        assert!(!listing.topology_complete);
    }

    #[tokio::test]
    async fn scanner_bucket_listing_marks_divergent_disk_views_incomplete() {
        let (temp_dir, ecstore) = setup_multi_pool_scanner_listing_test_env().await;
        let bucket = format!("divergent-set-{}", Uuid::new_v4().simple());
        ecstore.pools[0].disk_set[0]
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket should be created before disk views diverge");
        for disk_index in 0..2 {
            tokio::fs::remove_dir_all(temp_dir.path().join(format!("pool0-disk{disk_index}")).join(&bucket))
                .await
                .expect("test bucket directory should be removed from a minority disk view");
        }

        let listing = ecstore
            .list_bucket_for_scanner(&crate::storage_api_contracts::bucket::BucketOptions {
                no_metadata: true,
                ..Default::default()
            })
            .await
            .expect("responsive disks should still produce a scanner candidate listing");

        assert!(listing.buckets.iter().all(|entry| entry.name != bucket));
        assert!(!listing.topology_complete);
    }

    // These tests share one isolated instance and mutate its bucket metadata;
    // serialize them so their assertions cannot observe each other's operations.
    #[tokio::test]
    #[serial]
    async fn bucket_delete_mark_delete_removes_empty_bucket_and_keeps_deleted_marker() {
        let (disk_paths, ecstore) = setup_bucket_delete_test_env().await;
        let bucket = format!("bucket-mark-delete-{}", Uuid::new_v4().simple());

        ecstore
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket should be created");
        for disk_path in &disk_paths {
            tokio::fs::create_dir_all(disk_path.join(&bucket).join("empty-directory"))
                .await
                .expect("empty directory remnant should be created");
        }
        assert!(metadata_sys::get_in(&ecstore.ctx, &bucket).await.is_ok());

        let generation_before_delete = ecstore.scanner_namespace_mutation_generation();
        ecstore
            .delete_bucket(
                &bucket,
                &DeleteBucketOptions {
                    srdelete_op: SRBucketDeleteOp::MarkDelete,
                    ..Default::default()
                },
            )
            .await
            .expect("MarkDelete should remove an empty bucket");
        assert_eq!(
            ecstore.scanner_namespace_mutation_generation(),
            generation_before_delete.saturating_add(1),
            "successful bucket deletion should advance scanner namespace activity"
        );

        assert!(
            !any_disk_path_exists(&disk_paths, &bucket).await,
            "MarkDelete should remove the bucket volume"
        );
        assert!(
            any_disk_path_exists(&disk_paths, bucket_deleted_marker_volume(&bucket)).await,
            "MarkDelete should persist the deleted-bucket marker"
        );
        assert!(
            metadata_sys::get_in(&ecstore.ctx, &bucket).await.is_err(),
            "deleted bucket metadata must be removed from the local cache"
        );
        let buckets = ecstore
            .list_bucket(&BucketOptions::default())
            .await
            .expect("bucket listing should succeed after MarkDelete");
        assert!(!buckets.iter().any(|info| info.name == bucket));
        ecstore
            .delete_all(RUSTFS_META_BUCKET, &bucket_deleted_marker_prefix(&bucket))
            .await
            .expect("deleted-bucket marker should be removed to simulate a partial failure");
        assert!(!any_disk_path_exists(&disk_paths, bucket_deleted_marker_volume(&bucket)).await);
        ecstore
            .delete_bucket(
                &bucket,
                &DeleteBucketOptions {
                    srdelete_op: SRBucketDeleteOp::MarkDelete,
                    ..Default::default()
                },
            )
            .await
            .expect("retried MarkDelete should recreate a missing tombstone");
        assert!(any_disk_path_exists(&disk_paths, bucket_deleted_marker_volume(&bucket)).await);
    }

    #[tokio::test]
    #[serial]
    async fn bucket_delete_mark_delete_rejects_non_empty_bucket_without_force() {
        let (disk_paths, ecstore) = setup_bucket_delete_test_env().await;
        let bucket = format!("bucket-mark-delete-non-empty-{}", Uuid::new_v4().simple());

        create_bucket_with_object(&ecstore, &bucket, "object.txt").await;

        let err = ecstore
            .delete_bucket(
                &bucket,
                &DeleteBucketOptions {
                    srdelete_op: SRBucketDeleteOp::MarkDelete,
                    ..Default::default()
                },
            )
            .await
            .expect_err("MarkDelete should reject a non-empty bucket without force");

        assert!(matches!(err, StorageError::BucketNotEmpty(name) if name == bucket));
        assert!(any_disk_has_object_metadata(&disk_paths, &bucket).await);
        assert!(!any_disk_path_exists(&disk_paths, bucket_deleted_marker_volume(&bucket)).await);
    }

    #[tokio::test]
    #[serial]
    async fn bucket_delete_mark_delete_rejects_hidden_object_paths_without_force() {
        let (disk_paths, ecstore) = setup_bucket_delete_test_env().await;
        let bucket = format!("bucket-mark-delete-hidden-{}", Uuid::new_v4().simple());

        create_bucket_with_object(&ecstore, &bucket, ".well-known/acme-challenge").await;
        let mut reader = PutObjReader::from_vec(b"delete bucket semantics".to_vec());
        ecstore
            .put_object(&bucket, ".rustfs.sys/object", &mut reader, &ObjectOptions::default())
            .await
            .expect("second hidden object should be written");

        let err = ecstore
            .delete_bucket(
                &bucket,
                &DeleteBucketOptions {
                    srdelete_op: SRBucketDeleteOp::MarkDelete,
                    ..Default::default()
                },
            )
            .await
            .expect_err("MarkDelete should reject a hidden object path without force");

        assert!(matches!(err, StorageError::BucketNotEmpty(name) if name == bucket));
        assert!(any_disk_has_object_metadata(&disk_paths, &bucket).await);
    }

    #[tokio::test]
    #[serial]
    async fn bucket_delete_purge_removes_bucket_data_and_internal_metadata() {
        let (disk_paths, ecstore) = setup_bucket_delete_test_env().await;
        let bucket = format!("bucket-purge-{}", Uuid::new_v4().simple());
        let object = "object.txt";
        let metadata_prefix = format!("{RUSTFS_META_BUCKET}/{BUCKET_META_PREFIX}/{bucket}");

        create_bucket_with_object(&ecstore, &bucket, object).await;
        write_bucket_metadata_marker(&disk_paths, &metadata_prefix).await;
        ecstore
            .mark_bucket_deleted(&bucket)
            .await
            .expect("deleted-bucket marker should be created");
        assert!(any_disk_path_exists(&disk_paths, &metadata_prefix).await);
        assert!(any_disk_path_exists(&disk_paths, bucket_deleted_marker_volume(&bucket)).await);

        let generation_before_delete = ecstore.scanner_namespace_mutation_generation();
        ecstore
            .delete_bucket(
                &bucket,
                &DeleteBucketOptions {
                    force: true,
                    srdelete_op: SRBucketDeleteOp::Purge,
                    ..Default::default()
                },
            )
            .await
            .expect("Purge should force-delete bucket data");
        assert_eq!(
            ecstore.scanner_namespace_mutation_generation(),
            generation_before_delete.saturating_add(1),
            "successful bucket purge should advance scanner namespace activity"
        );

        assert!(!any_disk_path_exists(&disk_paths, &bucket).await, "Purge should remove the bucket volume");
        assert!(
            !any_disk_path_exists(&disk_paths, &metadata_prefix).await,
            "Purge should remove bucket metadata prefix"
        );
        assert!(
            !any_disk_path_exists(&disk_paths, bucket_deleted_marker_volume(&bucket)).await,
            "Purge should remove the deleted-bucket marker"
        );
        assert!(
            metadata_sys::get_in(&ecstore.ctx, &bucket).await.is_err(),
            "purged bucket metadata must be removed from the local cache"
        );
        write_bucket_metadata_marker(&disk_paths, &metadata_prefix).await;
        ecstore
            .mark_bucket_deleted(&bucket)
            .await
            .expect("stale deleted-bucket marker should be recreated");
        ecstore
            .delete_bucket(
                &bucket,
                &DeleteBucketOptions {
                    force: true,
                    srdelete_op: SRBucketDeleteOp::Purge,
                    ..Default::default()
                },
            )
            .await
            .expect("retried Purge should remove stale metadata without a bucket volume");
        assert!(!any_disk_path_exists(&disk_paths, &metadata_prefix).await);
        assert!(!any_disk_path_exists(&disk_paths, bucket_deleted_marker_volume(&bucket)).await);
    }

    #[tokio::test]
    #[serial]
    async fn bucket_delete_default_s3_delete_still_rejects_non_empty_bucket() {
        let (disk_paths, ecstore) = setup_bucket_delete_test_env().await;
        let bucket = format!("bucket-s3-delete-{}", Uuid::new_v4().simple());
        let object = "object.txt";

        create_bucket_with_object(&ecstore, &bucket, object).await;

        let generation_before_delete = ecstore.scanner_namespace_mutation_generation();
        let err = ecstore
            .delete_bucket(&bucket, &DeleteBucketOptions::default())
            .await
            .expect_err("default S3 DeleteBucket should reject non-empty buckets");

        assert!(matches!(err, StorageError::BucketNotEmpty(name) if name == bucket));
        assert_eq!(
            ecstore.scanner_namespace_mutation_generation(),
            generation_before_delete,
            "failed bucket deletion must not advance scanner namespace activity"
        );
        assert!(
            any_disk_has_object_metadata(&disk_paths, &bucket).await,
            "failed default S3 DeleteBucket must keep object data"
        );
        assert!(
            metadata_sys::get_in(&ecstore.ctx, &bucket).await.is_ok(),
            "failed default S3 DeleteBucket must keep metadata cache"
        );
    }

    #[tokio::test]
    #[serial]
    async fn bucket_delete_finishes_usage_cleanup_before_same_name_recreation() {
        let (_, ecstore) = setup_bucket_delete_test_env().await;
        let bucket = format!("bucket-usage-generation-{}", Uuid::new_v4().simple());
        ecstore
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket should be created");

        let mut snapshot = DataUsageInfo {
            last_update: Some(SystemTime::now()),
            buckets_count: 1,
            ..Default::default()
        };
        snapshot.buckets_usage.insert(
            bucket.clone(),
            BucketUsageInfo {
                size: 42,
                objects_count: 1,
                versions_count: 1,
                ..Default::default()
            },
        );
        snapshot.bucket_sizes.insert(bucket.clone(), 42);
        snapshot.calculate_totals();
        crate::data_usage::store_data_usage_in_backend(snapshot, ecstore.clone())
            .await
            .expect("usage snapshot should be stored");

        ecstore
            .delete_bucket(&bucket, &DeleteBucketOptions::default())
            .await
            .expect("empty bucket should be deleted");
        let deleted = crate::data_usage::load_data_usage_from_backend(ecstore.clone())
            .await
            .expect("usage snapshot should remain readable");
        assert!(!deleted.buckets_usage.contains_key(&bucket));

        ecstore
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("same bucket name should be recreated after delete returns");
        crate::data_usage::record_bucket_object_write_memory(&bucket, None, 84).await;

        let mut recreated = crate::data_usage::load_data_usage_from_backend(ecstore.clone())
            .await
            .expect("recreated bucket usage base should load");
        crate::data_usage::apply_bucket_usage_memory_overlay(&mut recreated).await;
        assert_eq!(
            recreated
                .buckets_usage
                .get(&bucket)
                .map(|usage| (usage.objects_count, usage.versions_count, usage.size)),
            Some((1, 1, 84))
        );
    }

    #[tokio::test]
    #[serial]
    async fn bucket_generation_operations_do_not_depend_on_usage_snapshot_health() {
        let (_, ecstore) = setup_bucket_delete_test_env().await;
        let deleted_bucket = format!("bucket-delete-corrupt-usage-{}", Uuid::new_v4().simple());
        ecstore
            .make_bucket(&deleted_bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket should be created before corrupting usage");

        let usage_path = format!("{BUCKET_META_PREFIX}/.usage.json");
        crate::config::com::save_config(ecstore.clone(), &usage_path, b"{".to_vec())
            .await
            .expect("corrupt usage fixture should be stored");

        ecstore
            .delete_bucket(&deleted_bucket, &DeleteBucketOptions::default())
            .await
            .expect("usage snapshot corruption must not block DeleteBucket");
        assert!(
            ecstore
                .get_bucket_info(&deleted_bucket, &crate::storage_api_contracts::bucket::BucketOptions::default())
                .await
                .is_err(),
            "successful DeleteBucket must remove the physical bucket"
        );

        let new_bucket = format!("bucket-create-corrupt-usage-{}", Uuid::new_v4().simple());
        ecstore
            .make_bucket(&new_bucket, &MakeBucketOptions::default())
            .await
            .expect("usage snapshot corruption must not block MakeBucket");
        assert!(
            ecstore
                .get_bucket_info(&new_bucket, &crate::storage_api_contracts::bucket::BucketOptions::default())
                .await
                .is_ok(),
            "successful MakeBucket must create the physical bucket"
        );

        let restored = serde_json::to_vec(&DataUsageInfo {
            last_update: Some(SystemTime::now()),
            ..Default::default()
        })
        .expect("restored usage fixture should encode");
        crate::config::com::save_config(ecstore, &usage_path, restored)
            .await
            .expect("usage fixture should be restored after the failure-path test");
    }
}
