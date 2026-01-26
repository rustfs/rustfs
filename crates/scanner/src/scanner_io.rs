use crate::scanner_folder::{ScannerItem, scan_data_folder};
use crate::{
    DATA_USAGE_CACHE_NAME, DATA_USAGE_ROOT, DataUsageCache, DataUsageCacheInfo, DataUsageEntry, DataUsageEntryInfo,
    DataUsageInfo, SizeSummary, TierStats,
};
use futures::future::join_all;
use rand::seq::SliceRandom as _;
use rustfs_common::heal_channel::HealScanMode;
use rustfs_ecstore::bucket::bucket_target_sys::BucketTargetSys;
use rustfs_ecstore::bucket::lifecycle::lifecycle::Lifecycle;
use rustfs_ecstore::bucket::metadata_sys::{get_lifecycle_config, get_object_lock_config, get_replication_config};
use rustfs_ecstore::bucket::replication::{ReplicationConfig, ReplicationConfigurationExt};
use rustfs_ecstore::bucket::versioning::VersioningApi as _;
use rustfs_ecstore::bucket::versioning_sys::BucketVersioningSys;
use rustfs_ecstore::config::storageclass;
use rustfs_ecstore::disk::STORAGE_FORMAT_FILE;
use rustfs_ecstore::disk::{Disk, DiskAPI};
use rustfs_ecstore::error::{Error, StorageError};
use rustfs_ecstore::global::GLOBAL_TierConfigMgr;
use rustfs_ecstore::new_object_layer_fn;
use rustfs_ecstore::set_disk::SetDisks;
use rustfs_ecstore::store_api::{BucketInfo, BucketOptions, ObjectInfo};
use rustfs_ecstore::{StorageAPI, error::Result, store::ECStore};
use rustfs_filemeta::FileMeta;
use rustfs_utils::path::{SLASH_SEPARATOR, path_join_buf};
use s3s::dto::{BucketLifecycleConfiguration, ReplicationConfiguration};
use std::collections::HashMap;
use std::time::SystemTime;
use std::{fmt::Debug, sync::Arc};
use time::OffsetDateTime;
use tokio::sync::{Mutex, mpsc};
use tokio::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, warn};

#[async_trait::async_trait]
pub trait ScannerIO: Send + Sync + Debug + 'static {
    async fn nsscanner(
        &self,
        ctx: CancellationToken,
        updates: mpsc::Sender<DataUsageInfo>,
        want_cycle: u64,
        scan_mode: HealScanMode,
    ) -> Result<()>;
}

#[async_trait::async_trait]
pub trait ScannerIOCache: Send + Sync + Debug + 'static {
    async fn nsscanner_cache(
        self: Arc<Self>,
        ctx: CancellationToken,
        buckets: Vec<BucketInfo>,
        updates: mpsc::Sender<DataUsageCache>,
        want_cycle: u64,
        scan_mode: HealScanMode,
    ) -> Result<()>;
}

#[async_trait::async_trait]
pub trait ScannerIODisk: Send + Sync + Debug + 'static {
    async fn nsscanner_disk(
        &self,
        ctx: CancellationToken,
        cache: DataUsageCache,
        updates: Option<mpsc::Sender<DataUsageEntry>>,
        scan_mode: HealScanMode,
    ) -> Result<DataUsageCache>;

    async fn get_size(&self, item: ScannerItem) -> Result<SizeSummary>;
}

#[async_trait::async_trait]
impl ScannerIO for ECStore {
    async fn nsscanner(
        &self,
        ctx: CancellationToken,
        updates: mpsc::Sender<DataUsageInfo>,
        want_cycle: u64,
        scan_mode: HealScanMode,
    ) -> Result<()> {
        let child_token = ctx.child_token();

        let all_buckets = self.list_bucket(&BucketOptions::default()).await?;

        if all_buckets.is_empty() {
            if let Err(e) = updates.send(DataUsageInfo::default()).await {
                error!("Failed to send data usage info: {}", e);
            }
            return Ok(());
        }

        let mut total_results = 0;
        for pool in self.pools.iter() {
            total_results += pool.disk_set.len();
        }

        let results = vec![DataUsageCache::default(); total_results];
        let results_mutex: Arc<Mutex<Vec<DataUsageCache>>> = Arc::new(Mutex::new(results));
        let first_err_mutex: Arc<Mutex<Option<Error>>> = Arc::new(Mutex::new(None));
        let mut results_index: i32 = -1_i32;
        let mut wait_futs = Vec::new();

        for pool in self.pools.iter() {
            for set in pool.disk_set.iter() {
                results_index += 1;

                let results_index_clone = results_index as usize;
                // Clone the Arc to move it into the spawned task
                let set_clone: Arc<SetDisks> = Arc::clone(set);

                let child_token_clone = child_token.clone();
                let want_cycle_clone = want_cycle;
                let scan_mode_clone = scan_mode;
                let results_mutex_clone = results_mutex.clone();
                let first_err_mutex_clone = first_err_mutex.clone();

                let (tx, mut rx) = tokio::sync::mpsc::channel::<DataUsageCache>(1);

                // Spawn task to receive and store results
                let receiver_fut = tokio::spawn(async move {
                    while let Some(result) = rx.recv().await {
                        let mut results = results_mutex_clone.lock().await;
                        results[results_index_clone] = result;
                    }
                });
                wait_futs.push(receiver_fut);

                let all_buckets_clone = all_buckets.clone();
                // Spawn task to run the scanner
                let scanner_fut = tokio::spawn(async move {
                    if let Err(e) = set_clone
                        .nsscanner_cache(child_token_clone.clone(), all_buckets_clone, tx, want_cycle_clone, scan_mode_clone)
                        .await
                    {
                        error!("Failed to scan set: {e}");
                        let _ = first_err_mutex_clone.lock().await.insert(e);
                        child_token_clone.cancel();
                    }
                });
                wait_futs.push(scanner_fut);
            }
        }

        let (update_tx, mut update_rx) = tokio::sync::oneshot::channel::<()>();

        let all_buckets_clone = all_buckets.iter().map(|b| b.name.clone()).collect::<Vec<String>>();
        tokio::spawn(async move {
            let mut last_update = SystemTime::now();

            let mut ticker = tokio::time::interval(Duration::from_secs(30));
            loop {
                tokio::select! {
                    _ = child_token.cancelled() => {
                        break;
                    }
                    res = &mut update_rx => {
                        if res.is_err() {
                            break;
                        }

                        let results = results_mutex.lock().await;
                        let mut all_merged = DataUsageCache::default();
                        for result in results.iter() {
                            if result.info.last_update.is_none() {
                                return;
                            }
                            all_merged.merge(result);
                        }

                        if all_merged.root().is_some() && all_merged.info.last_update.unwrap() > last_update
                           && let Err(e) = updates
                                .send(all_merged.dui(&all_merged.info.name, &all_buckets_clone))
                                .await {
                                error!("Failed to send data usage info: {}", e);
                            }
                        break;
                    }
                    _ = ticker.tick() => {
                        let results = results_mutex.lock().await;
                        let mut all_merged = DataUsageCache::default();
                        for result in results.iter() {
                            if result.info.last_update.is_none() {
                                return;
                            }
                            all_merged.merge(result);
                        }

                        if all_merged.root().is_some() && all_merged.info.last_update.unwrap() > last_update {
                           if let Err(e) = updates
                                .send(all_merged.dui(&all_merged.info.name, &all_buckets_clone))
                                .await {
                                error!("Failed to send data usage info: {}", e);
                            }
                            last_update = all_merged.info.last_update.unwrap();
                        }
                    }
                }
            }
        });

        let _ = join_all(wait_futs).await;

        let _ = update_tx.send(());

        Ok(())
    }
}

#[async_trait::async_trait]
impl ScannerIOCache for SetDisks {
    async fn nsscanner_cache(
        self: Arc<Self>,
        ctx: CancellationToken,
        buckets: Vec<BucketInfo>,
        updates: mpsc::Sender<DataUsageCache>,
        want_cycle: u64,
        scan_mode: HealScanMode,
    ) -> Result<()> {
        if buckets.is_empty() {
            return Ok(());
        }

        let (disks, healing) = self.get_online_disks_with_healing(false).await;
        if disks.is_empty() {
            debug!("nsscanner_cache: no online disks available for set");
            return Ok(());
        }

        let mut old_cache = DataUsageCache::default();
        old_cache.load(self.clone(), DATA_USAGE_CACHE_NAME).await?;

        let mut cache = DataUsageCache {
            info: DataUsageCacheInfo {
                name: DATA_USAGE_ROOT.to_string(),
                next_cycle: old_cache.info.next_cycle,
                ..Default::default()
            },
            cache: HashMap::new(),
        };

        let (bucket_tx, bucket_rx) = mpsc::channel::<BucketInfo>(buckets.len());

        let mut permutes = buckets.clone();
        permutes.shuffle(&mut rand::rng());

        for bucket in permutes.iter() {
            if old_cache.find(&bucket.name).is_none()
                && let Err(e) = bucket_tx.send(bucket.clone()).await
            {
                error!("Failed to send bucket info: {}", e);
            }
        }

        for bucket in permutes.iter() {
            if let Some(c) = old_cache.find(&bucket.name) {
                cache.replace(&bucket.name, DATA_USAGE_ROOT, c.clone());

                if let Err(e) = bucket_tx.send(bucket.clone()).await {
                    error!("Failed to send bucket info: {}", e);
                }
            }
        }

        drop(bucket_tx);

        let cache_mutex: Arc<Mutex<DataUsageCache>> = Arc::new(Mutex::new(cache));

        let (bucket_result_tx, mut bucket_result_rx) = mpsc::channel::<DataUsageEntryInfo>(disks.len());

        let cache_mutex_clone = cache_mutex.clone();
        let store_clone = self.clone();
        let ctx_clone = ctx.clone();
        let send_update_fut = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(3 + rand::random::<u64>() % 10));

            let mut last_update = None;

            loop {
                tokio::select! {
                    _ = ctx_clone.cancelled() => {
                        break;
                    }
                    _ = ticker.tick() => {

                       let cache = cache_mutex_clone.lock().await;
                       if cache.info.last_update == last_update {
                           continue;
                       }

                       if let Err(e) = cache.save(store_clone.clone(), DATA_USAGE_CACHE_NAME).await {
                           error!("Failed to save data usage cache: {}", e);
                       }

                       if let Err(e) = updates.send(cache.clone()).await {
                           error!("Failed to send data usage cache: {}", e);

                       }

                       last_update = cache.info.last_update;
                    }
                    res =  bucket_result_rx.recv() => {
                        if let Some(result) = res {
                            let mut cache = cache_mutex_clone.lock().await;
                            cache.replace(&result.name, &result.parent, result.entry);
                            cache.info.last_update = Some(SystemTime::now());

                        } else {
                            let mut cache = cache_mutex_clone.lock().await;
                            cache.info.next_cycle =want_cycle;
                            cache.info.last_update = Some(SystemTime::now());

                            if let Err(e) = cache.save(store_clone.clone(), DATA_USAGE_CACHE_NAME).await {
                                error!("Failed to save data usage cache: {}", e);
                            }

                            if let Err(e) = updates.send(cache.clone()).await {
                                error!("Failed to send data usage cache: {}", e);

                            }

                            return;
                        }
                    }
                }
            }
        });

        let mut futs = Vec::new();

        let bucket_rx_mutex: Arc<Mutex<mpsc::Receiver<BucketInfo>>> = Arc::new(Mutex::new(bucket_rx));
        let bucket_result_tx_clone: Arc<Mutex<mpsc::Sender<DataUsageEntryInfo>>> = Arc::new(Mutex::new(bucket_result_tx));
        for disk in disks.into_iter() {
            let bucket_rx_mutex_clone = bucket_rx_mutex.clone();
            let ctx_clone = ctx.clone();
            let store_clone_clone = self.clone();
            let bucket_result_tx_clone_clone = bucket_result_tx_clone.clone();
            let disk_clone = disk.clone();
            futs.push(tokio::spawn(async move {
                while let Some(bucket) = bucket_rx_mutex_clone.lock().await.recv().await {
                    if ctx_clone.is_cancelled() {
                        break;
                    }

                    debug!("nsscanner_disk: got bucket: {}", bucket.name);

                    let cache_name = path_join_buf(&[&bucket.name, DATA_USAGE_CACHE_NAME]);

                    let mut cache = DataUsageCache::default();
                    if let Err(e) = cache.load(store_clone_clone.clone(), &cache_name).await {
                        error!("Failed to load data usage cache: {}", e);
                    }

                    if cache.info.name.is_empty() {
                        cache.info.name = bucket.name.clone();
                    }

                    cache.info.skip_healing = healing;
                    cache.info.next_cycle = want_cycle;
                    if cache.info.name != bucket.name {
                        cache.info = DataUsageCacheInfo {
                            name: bucket.name.clone(),
                            next_cycle: want_cycle,
                            ..Default::default()
                        };
                    }

                    debug!("nsscanner_disk: cache.info.name: {:?}", cache.info.name);

                    let (updates_tx, mut updates_rx) = mpsc::channel::<DataUsageEntry>(1);

                    let ctx_clone_clone = ctx_clone.clone();
                    let bucket_name_clone = bucket.name.clone();
                    let bucket_result_tx_clone_clone_clone = bucket_result_tx_clone_clone.clone();
                    let update_fut = tokio::spawn(async move {
                        while let Some(result) = updates_rx.recv().await {
                            if ctx_clone_clone.is_cancelled() {
                                break;
                            }

                            if let Err(e) = bucket_result_tx_clone_clone_clone
                                .lock()
                                .await
                                .send(DataUsageEntryInfo {
                                    name: bucket_name_clone.clone(),
                                    parent: DATA_USAGE_ROOT.to_string(),
                                    entry: result,
                                })
                                .await
                            {
                                error!("Failed to send data usage entry info: {}", e);
                            }
                        }
                    });

                    let before = cache.info.last_update;

                    cache = match disk_clone
                        .nsscanner_disk(ctx_clone.clone(), cache.clone(), Some(updates_tx), scan_mode)
                        .await
                    {
                        Ok(cache) => cache,
                        Err(e) => {
                            error!("Failed to scan disk: {}", e);

                            if let (Some(last_update), Some(before_update)) = (cache.info.last_update, before)
                                && last_update > before_update
                                && let Err(e) = cache.save(store_clone_clone.clone(), cache_name.as_str()).await
                            {
                                error!("Failed to save data usage cache: {}", e);
                            }

                            if let Err(e) = update_fut.await {
                                error!("Failed to update data usage cache: {}", e);
                            }
                            continue;
                        }
                    };

                    debug!("nsscanner_disk: got cache: {}", cache.info.name);

                    if let Err(e) = update_fut.await {
                        error!("nsscanner_disk: Failed to update data usage cache: {}", e);
                    }

                    let root = if let Some(r) = cache.root() {
                        cache.flatten(&r)
                    } else {
                        DataUsageEntry::default()
                    };

                    if ctx_clone.is_cancelled() {
                        break;
                    }

                    debug!("nsscanner_disk: sending data usage entry info: {}", cache.info.name);

                    if let Err(e) = bucket_result_tx_clone_clone
                        .lock()
                        .await
                        .send(DataUsageEntryInfo {
                            name: cache.info.name.clone(),
                            parent: DATA_USAGE_ROOT.to_string(),
                            entry: root,
                        })
                        .await
                    {
                        error!("nsscanner_disk: Failed to send data usage entry info: {}", e);
                    }

                    if let Err(e) = cache.save(store_clone_clone.clone(), &cache_name).await {
                        error!("nsscanner_disk: Failed to save data usage cache: {}", e);
                    }
                }
            }));
        }

        let _ = join_all(futs).await;

        drop(bucket_result_tx_clone);

        send_update_fut.await?;

        debug!("nsscanner_cache: done");

        Ok(())
    }
}

#[async_trait::async_trait]
impl ScannerIODisk for Disk {
    async fn get_size(&self, mut item: ScannerItem) -> Result<SizeSummary> {
        if !item.path.ends_with(&format!("{SLASH_SEPARATOR}{STORAGE_FORMAT_FILE}")) {
            return Err(StorageError::other("skip file".to_string()));
        }

        let data = match self.read_metadata(&item.bucket, &item.object_path()).await {
            Ok(data) => data,
            Err(e) => {
                warn!(
                    "Failed to read metadata: {e}, bucket={}, object_path={}",
                    &item.bucket,
                    &item.object_path()
                );

                return Err(StorageError::other("skip file".to_string()));
            }
        };

        item.transform_meta_dir();

        let meta = FileMeta::load(&data)?;
        let fivs = match meta.get_file_info_versions(item.bucket.as_str(), item.object_path().as_str(), false) {
            Ok(versions) => versions,
            Err(e) => {
                error!("Failed to get file info versions: {}", e);
                return Err(StorageError::other("skip file".to_string()));
            }
        };

        let versioned = BucketVersioningSys::get(&item.bucket)
            .await
            .map(|v| v.versioned(&item.object_path()))
            .unwrap_or(false);

        let object_infos = fivs
            .versions
            .iter()
            .map(|v| ObjectInfo::from_file_info(v, item.bucket.as_str(), item.object_path().as_str(), versioned))
            .collect::<Vec<ObjectInfo>>();

        let mut size_summary = SizeSummary::default();

        let tiers = {
            let tier_config_mgr = GLOBAL_TierConfigMgr.read().await;
            tier_config_mgr.list_tiers()
        };

        for tier in tiers.iter() {
            size_summary.tier_stats.insert(tier.name.clone(), TierStats::default());
        }
        if !size_summary.tier_stats.is_empty() {
            size_summary
                .tier_stats
                .insert(storageclass::STANDARD.to_string(), TierStats::default());
            size_summary
                .tier_stats
                .insert(storageclass::RRS.to_string(), TierStats::default());
        }

        let lock_config = match get_object_lock_config(&item.bucket).await {
            Ok((cfg, _)) => Some(Arc::new(cfg)),
            Err(_) => None,
        };

        let Some(ecstore) = new_object_layer_fn() else {
            error!("ECStore not available");
            return Err(StorageError::other("ECStore not available".to_string()));
        };

        item.apply_actions(ecstore, object_infos, lock_config, &mut size_summary)
            .await;

        // TODO: enqueueFreeVersion

        Ok(size_summary)
    }
    async fn nsscanner_disk(
        &self,
        ctx: CancellationToken,
        cache: DataUsageCache,
        updates: Option<mpsc::Sender<DataUsageEntry>>,
        scan_mode: HealScanMode,
    ) -> Result<DataUsageCache> {
        let _guard = self.start_scan();

        let mut cache = cache;

        let (lifecycle_config, _) = get_lifecycle_config(&cache.info.name)
            .await
            .unwrap_or((BucketLifecycleConfiguration::default(), OffsetDateTime::now_utc()));

        if lifecycle_config.has_active_rules("") {
            cache.info.lifecycle = Some(Arc::new(lifecycle_config));
        }

        let (replication_config, _) = get_replication_config(&cache.info.name).await.unwrap_or((
            ReplicationConfiguration {
                role: "".to_string(),
                rules: vec![],
            },
            OffsetDateTime::now_utc(),
        ));

        if replication_config.has_active_rules("", true)
            && let Ok(targets) = BucketTargetSys::get().list_bucket_targets(&cache.info.name).await
        {
            cache.info.replication = Some(Arc::new(ReplicationConfig {
                config: Some(replication_config),
                remotes: Some(targets),
            }));
        }

        // TODO: object lock

        let Some(ecstore) = new_object_layer_fn() else {
            error!("ECStore not available");
            return Err(StorageError::other("ECStore not available".to_string()));
        };

        let disk_location = self.get_disk_location();

        let (Some(pool_idx), Some(set_idx)) = (disk_location.pool_idx, disk_location.set_idx) else {
            error!("Disk location not available");
            return Err(StorageError::other("Disk location not available".to_string()));
        };

        let disks_result = ecstore.get_disks(pool_idx, set_idx).await?;

        let Some(disk_idx) = disk_location.disk_idx else {
            error!("Disk index not available");
            return Err(StorageError::other("Disk index not available".to_string()));
        };

        let local_disk = if let Some(Some(local_disk)) = disks_result.get(disk_idx) {
            local_disk.clone()
        } else {
            error!("Local disk not available");
            return Err(StorageError::other("Local disk not available".to_string()));
        };

        let disks = disks_result.into_iter().flatten().collect::<Vec<Arc<Disk>>>();

        // Create we_sleep function (always return false for now, can be enhanced later)
        let we_sleep: Box<dyn Fn() -> bool + Send + Sync> = Box::new(|| false);

        let result = scan_data_folder(ctx, disks, local_disk, cache, updates, scan_mode, we_sleep).await;

        match result {
            Ok(mut data_usage_info) => {
                data_usage_info.info.last_update = Some(SystemTime::now());
                Ok(data_usage_info)
            }
            Err(e) => Err(StorageError::other(format!("Failed to scan data folder: {e}"))),
        }
    }
}
