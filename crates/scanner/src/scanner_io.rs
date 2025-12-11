use std::collections::HashMap;
use std::sync::atomic::{AtomicI32, AtomicU32, Ordering};
use std::time::SystemTime;
use std::{fmt::Debug, sync::Arc};

use crate::metrics::global_metrics;
use crate::scanner_folder::ScannerItem;
use crate::{
    DATA_USAGE_CACHE_NAME, DATA_USAGE_ROOT, DataUsageCache, DataUsageCacheInfo, DataUsageEntry, DataUsageEntryInfo, DataUsageInfo,
};
use futures::future::join_all;
use rand::seq::SliceRandom as _;
use rustfs_common::heal_channel::HealScanMode;
use rustfs_common::last_minute::AccElem;
use rustfs_ecstore::bucket::bucket_target_sys::BucketTargetSys;
use rustfs_ecstore::bucket::lifecycle::lifecycle::Lifecycle;
use rustfs_ecstore::bucket::metadata_sys::{get_lifecycle_config, get_replication_config};
use rustfs_ecstore::bucket::replication::{ReplicationConfig, ReplicationConfigurationExt};
use rustfs_ecstore::bucket::versioning_sys::BucketVersioningSys;
use rustfs_ecstore::disk::STORAGE_FORMAT_FILE;
use rustfs_ecstore::disk::{Disk, DiskAPI, DiskStore, local::LocalDisk};
use rustfs_ecstore::error::{Error, StorageError};
use rustfs_ecstore::rpc::RemoteDisk;
use rustfs_ecstore::set_disk::SetDisks;
use rustfs_ecstore::store_api::{BucketInfo, BucketOptions};
use rustfs_ecstore::{StorageAPI, error::Result, store::ECStore};
use rustfs_ecstore::{bucket, new_object_layer_fn};
use rustfs_utils::path::{SLASH_SEPARATOR, path_join, path_join_buf};
use s3s::dto::{BucketLifecycleConfiguration, ReplicationConfiguration, VersioningConfiguration};
use time::OffsetDateTime;
use tokio::sync::{Mutex, mpsc};
use tokio::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

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
        updates: mpsc::Sender<DataUsageEntry>,
        scan_mode: HealScanMode,
    ) -> Result<DataUsageCache>;
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

                        if all_merged.root().is_some() && all_merged.info.last_update.unwrap() > last_update {
                           if let Err(e) = updates
                                .send(all_merged.dui(&all_merged.info.name, &all_buckets_clone))
                                .await {
                                error!("Failed to send data usage info: {}", e);
                            }
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
            info!("No online disks available for set");
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
            if old_cache.find(&bucket.name).is_none() {
                if let Err(e) = bucket_tx.send(bucket.clone()).await {
                    error!("Failed to send bucket info: {}", e);
                }
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
        let update_fut = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(30 + rand::random::<u64>() % 10));

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
            futs.push(tokio::spawn(async move {
                while let Some(bucket) = bucket_rx_mutex_clone.lock().await.recv().await {
                    if ctx_clone.is_cancelled() {
                        break;
                    }

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

                    cache = match disk
                        .clone()
                        .nsscanner_disk(ctx_clone.clone(), cache.clone(), updates_tx, scan_mode)
                        .await
                    {
                        Ok(cache) => cache,
                        Err(e) => {
                            error!("Failed to scan disk: {}", e);

                            if let (Some(last_update), Some(before_update)) = (cache.info.last_update, before) {
                                if last_update > before_update {
                                    if let Err(e) = cache.save(store_clone_clone.clone(), cache_name.as_str()).await {
                                        error!("Failed to save data usage cache: {}", e);
                                    }
                                }
                            }

                            if let Err(e) = update_fut.await {
                                error!("Failed to update data usage cache: {}", e);
                            }
                            continue;
                        }
                    };

                    if let Err(e) = update_fut.await {
                        error!("Failed to update data usage cache: {}", e);
                    }

                    let root = if let Some(r) = cache.root() {
                        cache.flatten(&r)
                    } else {
                        DataUsageEntry::default()
                    };

                    if ctx_clone.is_cancelled() {
                        break;
                    }

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
                        error!("Failed to send data usage entry info: {}", e);
                    }

                    if let Err(e) = cache.save(store_clone_clone.clone(), &cache_name).await {
                        error!("Failed to save data usage cache: {}", e);
                    }
                }
            }));
        }

        let _ = join_all(futs).await;

        update_fut.await?;

        Ok(())
    }
}

#[async_trait::async_trait]
impl ScannerIODisk for Disk {
    async fn nsscanner_disk(
        &self,
        ctx: CancellationToken,
        cache: DataUsageCache,
        updates: mpsc::Sender<DataUsageEntry>,
        scan_mode: HealScanMode,
    ) -> Result<DataUsageCache> {
        match self {
            Disk::Local(local_disk) => local_disk.nsscanner_disk(ctx, cache, updates, scan_mode).await,
            Disk::Remote(remote_disk) => remote_disk.nsscanner_disk(ctx, cache, updates, scan_mode).await,
        }
    }
}

#[async_trait::async_trait]
impl ScannerIODisk for LocalDisk {
    async fn nsscanner_disk(
        &self,
        ctx: CancellationToken,
        cache: DataUsageCache,
        updates: mpsc::Sender<DataUsageEntry>,
        scan_mode: HealScanMode,
    ) -> Result<DataUsageCache> {
        let _guard = self.start_scan();

        let mut cache = cache;

        let (lifecycle_config, _) = get_lifecycle_config(&cache.info.name)
            .await
            .unwrap_or((BucketLifecycleConfiguration::default(), OffsetDateTime::now_utc()));

        // if lifecycle_config.has_active_rules("").await {
        //     cache.info.lifecycle = Some(lifecycle_config);
        // }

        let (replication_config, _) = get_replication_config(&cache.info.name).await.unwrap_or((
            ReplicationConfiguration {
                role: "".to_string(),
                rules: vec![],
            },
            OffsetDateTime::now_utc(),
        ));

        // if replication_config.has_active_rules("", true) {
        //     if let Ok(targets) = BucketTargetSys::get().list_bucket_targets(&cache.info.name).await {
        //         cache.info.replication = Some(ReplicationConfig {
        //             config: Some(replication_config),
        //             remotes: Some(targets),
        //         });
        //     }
        // }

        // TODO: object lock

        let versioning_config = BucketVersioningSys::get(&cache.info.name).await.unwrap_or_default();

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

        let disks = disks_result.into_iter().flatten().collect::<Vec<Arc<Disk>>>();

        cache.info.updates = Some(Arc::new(Mutex::new(updates)));

        // Get disk path
        let disk_path = self.path().to_string_lossy().to_string();

        // Check if erasure mode (if we have multiple disks in the set)
        let is_erasure = disks.len() > 1;

        // Create we_sleep function (always return false for now, can be enhanced later)
        let we_sleep: Box<dyn Fn() -> bool + Send + Sync> = Box::new(|| false);

        // Create update_current_path function (no-op for now, can be enhanced with metrics)
        let update_current_path: Box<dyn Fn(String) + Send + Sync> = Box::new(|_path| {
            // TODO: Update metrics if needed
        });

        // Create check_disk_healing function
        let check_disk_healing: Option<Box<dyn Fn() -> bool + Send + Sync>> = None; // TODO: Implement disk healing check

        // Create get_size function
        // Clone necessary data to avoid lifetime issues
        // Note: LocalDisk may not implement Clone, so we'll use a different approach
        // We'll pass the disk path and use it to read metadata
        let disk_path_for_get_size = disk_path.clone();
        let lifecycle_config_clone = lifecycle_config.clone();
        let versioning_config_clone = versioning_config.clone();
        let get_size: crate::scanner_folder::GetSizeFn = Box::new(move |item: ScannerItem| {
            // Check if path ends with xl.meta
            if !item.path.ends_with(&format!("{}{}", SLASH_SEPARATOR, STORAGE_FORMAT_FILE)) {
                return Err(StorageError::other("skip file".to_string()));
            }

            // Read metadata
            // Note: We can't use disk_clone here due to lifetime issues
            // For now, we'll return an error indicating this needs to be implemented differently
            // TODO: Implement proper metadata reading using async context or refactor GetSizeFn to be async
            // The proper implementation would:
            // 1. Read metadata from disk using async context
            // 2. Transform meta dir (remove the last component from prefix)
            // 3. Get file info versions from metadata
            // 4. Process versions and apply lifecycle/healing/replication checks
            // 5. Return SizeSummary

            // Placeholder: return skip file error for now
            Err(StorageError::other("skip file - metadata reading needs async implementation".to_string()))
        });

        // Call scan_data_folder
        use crate::scanner_folder::scan_data_folder;
        let result = scan_data_folder(
            ctx,
            disks,
            disk_path,
            cache,
            get_size,
            scan_mode,
            we_sleep,
            is_erasure,
            update_current_path,
            check_disk_healing,
        )
        .await;

        match result {
            Ok(mut data_usage_info) => {
                data_usage_info.info.last_update = Some(SystemTime::now());
                Ok(data_usage_info)
            }
            Err(e) => Err(StorageError::other(format!("Failed to scan data folder: {}", e))),
        }
    }
}

#[async_trait::async_trait]
impl ScannerIODisk for RemoteDisk {
    async fn nsscanner_disk(
        &self,
        ctx: CancellationToken,
        cache: DataUsageCache,
        updates: mpsc::Sender<DataUsageEntry>,
        scan_mode: HealScanMode,
    ) -> Result<DataUsageCache> {
        todo!()
    }
}
