use s3s::{S3Error, S3ErrorCode};
use std::{cmp::Ordering, env, path::PathBuf, sync::Arc, time::Duration};
use tokio::{
    sync::{
        mpsc::{self, Receiver, Sender},
        RwLock,
    },
    time::interval,
};
use tracing::{error, info};
use uuid::Uuid;

use super::{
    heal_commands::{HealOpts, HealResultItem},
    heal_ops::{new_bg_heal_sequence, HealSequence},
};
use crate::heal::error::ERR_RETRY_HEALING;
use crate::{
    config::RUSTFS_CONFIG_PREFIX,
    disk::{endpoint::Endpoint, error::DiskError, DiskAPI, DiskInfoOptions, BUCKET_META_PREFIX, RUSTFS_META_BUCKET},
    error::{Error, Result},
    global::{GLOBAL_BackgroundHealRoutine, GLOBAL_BackgroundHealState, GLOBAL_LOCAL_DISK_MAP},
    heal::{
        data_usage::{DATA_USAGE_CACHE_NAME, DATA_USAGE_ROOT},
        data_usage_cache::DataUsageCache,
        heal_commands::{init_healing_tracker, load_healing_tracker},
        heal_ops::NOP_HEAL,
    },
    new_object_layer_fn,
    store::get_disk_via_endpoint,
    store_api::{BucketInfo, BucketOptions, StorageAPI},
    utils::path::{path_join, SLASH_SEPARATOR},
};

pub static DEFAULT_MONITOR_NEW_DISK_INTERVAL: Duration = Duration::from_secs(10);

pub async fn init_auto_heal() {
    init_background_healing().await;
    if let Ok(v) = env::var("_RUSTFS_AUTO_DRIVE_HEALING") {
        if v == "on" {
            GLOBAL_BackgroundHealState
                .write()
                .await
                .push_heal_local_disks(&get_local_disks_to_heal().await)
                .await;
            tokio::spawn(async {
                monitor_local_disks_and_heal().await;
            });
        }
    }
}

async fn init_background_healing() {
    let bg_seq = Arc::new(RwLock::new(new_bg_heal_sequence()));
    for _ in 0..GLOBAL_BackgroundHealRoutine.read().await.workers {
        let bg_seq_clone = bg_seq.clone();
        tokio::spawn(async {
            GLOBAL_BackgroundHealRoutine.write().await.add_worker(bg_seq_clone).await;
        });
    }
    let _ = GLOBAL_BackgroundHealState
        .write()
        .await
        .launch_new_heal_sequence(bg_seq)
        .await;
}

pub async fn get_local_disks_to_heal() -> Vec<Endpoint> {
    let mut disks_to_heal = Vec::new();
    for (_, disk) in GLOBAL_LOCAL_DISK_MAP.read().await.iter() {
        if let Some(disk) = disk {
            if let Err(err) = disk.disk_info(&DiskInfoOptions::default()).await {
                if let Some(DiskError::UnformattedDisk) = err.downcast_ref() {
                    disks_to_heal.push(disk.endpoint());
                }
            }
            let h = disk.healing().await;
            if let Some(h) = h {
                if !h.finished {
                    disks_to_heal.push(disk.endpoint());
                }
            }
        }
    }

    // todo
    // if disks_to_heal.len() == GLOBAL_Endpoints.read().await.n {

    // }
    disks_to_heal
}

async fn monitor_local_disks_and_heal() {
    let mut interval = interval(DEFAULT_MONITOR_NEW_DISK_INTERVAL);

    loop {
        interval.tick().await;
        let heal_disks = GLOBAL_BackgroundHealState.read().await.get_heal_local_disk_endpoints().await;
        if heal_disks.is_empty() {
            interval.reset();
            continue;
        }
        let layer = new_object_layer_fn();
        let lock = layer.read().await;
        let store = lock.as_ref().expect("errServerNotInitialized");
        if let (_, Some(err)) = store.heal_format(false).await.expect("heal format failed") {
            if let Some(DiskError::NoHealRequired) = err.downcast_ref() {
            } else {
                info!("heal format err: {}", err.to_string());
                interval.reset();
                continue;
            }
        }

        for disk in heal_disks.into_ref().iter() {
            let disk_clone = disk.clone();
            tokio::spawn(async move {
                GLOBAL_BackgroundHealState
                    .write()
                    .await
                    .set_disk_healing_status(disk_clone.clone(), true)
                    .await;
                if heal_fresh_disk(&disk_clone).await.is_err() {
                    GLOBAL_BackgroundHealState
                        .write()
                        .await
                        .set_disk_healing_status(disk_clone.clone(), false)
                        .await;
                    return;
                }
                GLOBAL_BackgroundHealState
                    .write()
                    .await
                    .pop_heal_local_disks(&[disk_clone])
                    .await;
            });
        }
        interval.reset();
    }
}

async fn heal_fresh_disk(endpoint: &Endpoint) -> Result<()> {
    let (pool_idx, set_idx) = (endpoint.pool_idx as usize, endpoint.disk_idx as usize);
    let disk = match get_disk_via_endpoint(endpoint).await {
        Some(disk) => disk,
        None => {
            return Err(Error::from_string(format!(
                "Unexpected error disk must be initialized by now after formatting: {}",
                endpoint
            )))
        }
    };

    if let Err(err) = disk.disk_info(&DiskInfoOptions::default()).await {
        match err.downcast_ref() {
            Some(DiskError::DriveIsRoot) => {
                return Ok(());
            }
            Some(DiskError::UnformattedDisk) => {}
            _ => {
                return Err(err);
            }
        }
    }

    let mut tracker = match load_healing_tracker(&Some(disk.clone())).await {
        Ok(tracker) => tracker,
        Err(err) => {
            match err.downcast_ref() {
                Some(DiskError::FileNotFound) => {
                    return Ok(());
                }
                _ => {
                    info!(
                        "Unable to load healing tracker on '{}': {}, re-initializing..",
                        disk.to_string(),
                        err.to_string()
                    );
                }
            }
            init_healing_tracker(disk.clone(), &Uuid::new_v4().to_string()).await?
        }
    };

    info!(
        "Healing drive '{}' - 'mc admin heal alias/ --verbose' to check the current status.",
        endpoint.to_string()
    );

    let layer = new_object_layer_fn();
    let lock = layer.read().await;
    let store = match lock.as_ref() {
        Some(s) => s,
        None => return Err(Error::msg("errServerNotInitialized")),
    };
    let mut buckets = store.list_bucket(&BucketOptions::default()).await?;
    buckets.push(BucketInfo {
        name: path_join(&[PathBuf::from(RUSTFS_META_BUCKET), PathBuf::from(RUSTFS_CONFIG_PREFIX)])
            .to_string_lossy()
            .to_string(),
        ..Default::default()
    });
    buckets.push(BucketInfo {
        name: path_join(&[PathBuf::from(RUSTFS_META_BUCKET), PathBuf::from(BUCKET_META_PREFIX)])
            .to_string_lossy()
            .to_string(),
        ..Default::default()
    });

    buckets.sort_by(|a, b| {
        let a_has_prefix = a.name.starts_with(RUSTFS_META_BUCKET);
        let b_has_prefix = b.name.starts_with(RUSTFS_META_BUCKET);

        match (a_has_prefix, b_has_prefix) {
            (true, false) => Ordering::Less,
            (false, true) => Ordering::Greater,
            _ => b.created.cmp(&a.created),
        }
    });

    if let Ok(cache) = DataUsageCache::load(&store.pools[pool_idx].disk_set[set_idx], DATA_USAGE_CACHE_NAME).await {
        let data_usage_info = cache.dui(DATA_USAGE_ROOT, &Vec::new());
        tracker.objects_total_count = data_usage_info.objects_total_count;
        tracker.objects_total_size = data_usage_info.objects_total_size;
    };

    tracker.set_queue_buckets(&buckets).await;
    tracker.save().await?;

    let tracker = Arc::new(RwLock::new(tracker));
    let qb = tracker.read().await.queue_buckets.clone();
    store.pools[pool_idx].disk_set[set_idx]
        .clone()
        .heal_erasure_set(&qb, tracker.clone())
        .await?;
    let mut tracker_w = tracker.write().await;
    if tracker_w.items_failed > 0 && tracker_w.retry_attempts < 4 {
        tracker_w.retry_attempts += 1;
        tracker_w.reset_healing().await;
        if let Err(err) = tracker_w.update().await {
            info!("update tracker failed: {}", err.to_string());
        }
        return Err(Error::from_string(ERR_RETRY_HEALING));
    }

    if tracker_w.items_failed > 0 {
        info!(
            "Healing of drive '{}' is incomplete, retried {} times (healed: {}, skipped: {}, failed: {}).",
            disk.to_string(),
            tracker_w.retry_attempts,
            tracker_w.items_healed,
            tracker_w.item_skipped,
            tracker_w.items_failed
        );
    } else if tracker_w.retry_attempts > 0 {
        info!(
            "Healing of drive '{}' is incomplete, retried {} times (healed: {}, skipped: {}).",
            disk.to_string(),
            tracker_w.retry_attempts,
            tracker_w.items_healed,
            tracker_w.item_skipped
        );
    } else {
        info!(
            "Healing of drive '{}' is finished (healed: {}, skipped: {}).",
            disk.to_string(),
            tracker_w.items_healed,
            tracker_w.item_skipped
        );
    }

    if tracker_w.heal_id.is_empty() {
        if let Err(err) = tracker_w.delete().await {
            error!("delete tracker failed: {}", err.to_string());
        }
    }
    let layer = new_object_layer_fn();
    let lock = layer.read().await;
    let store = match lock.as_ref() {
        Some(s) => s,
        None => return Err(Error::from(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()))),
    };
    let disks = store.get_disks(pool_idx, set_idx).await?;
    for disk in disks.into_iter() {
        if disk.is_none() {
            continue;
        }
        let mut tracker = match load_healing_tracker(&disk).await {
            Ok(tracker) => tracker,
            Err(err) => {
                match err.downcast_ref() {
                    Some(DiskError::FileNotFound) => {}
                    _ => {
                        info!("Unable to load healing tracker on '{:?}': {}, re-initializing..", disk, err.to_string());
                    }
                }
                continue;
            }
        };
        if tracker.heal_id == tracker_w.heal_id {
            tracker.finished = true;
            tracker.update().await?;
        }
    }
    Ok(())
}

#[derive(Debug)]
pub struct HealTask {
    pub bucket: String,
    pub object: String,
    pub version_id: String,
    pub opts: HealOpts,
    pub resp_tx: Option<Sender<HealResult>>,
    pub resp_rx: Option<Receiver<HealResult>>,
}

impl HealTask {
    pub fn new(bucket: &str, object: &str, version_id: &str, opts: &HealOpts) -> Self {
        Self {
            bucket: bucket.to_string(),
            object: object.to_string(),
            version_id: version_id.to_string(),
            opts: *opts,
            resp_tx: None,
            resp_rx: None,
        }
    }
}

pub struct HealResult {
    pub result: HealResultItem,
    pub err: Option<Error>,
}

pub struct HealRoutine {
    pub tasks_tx: Sender<HealTask>,
    tasks_rx: Receiver<HealTask>,
    workers: usize,
}

impl HealRoutine {
    pub fn new() -> Arc<RwLock<Self>> {
        let mut workers = num_cpus::get() / 2;
        if let Ok(env_heal_workers) = env::var("_RUSTFS_HEAL_WORKERS") {
            if let Ok(num_healers) = env_heal_workers.parse::<usize>() {
                workers = num_healers;
            }
        }

        if workers == 0 {
            workers = 4;
        }

        let (tx, rx) = mpsc::channel(100);
        Arc::new(RwLock::new(Self {
            tasks_tx: tx,
            tasks_rx: rx,
            workers,
        }))
    }

    pub async fn add_worker(&mut self, bgseq: Arc<RwLock<HealSequence>>) {
        loop {
            let mut d_res = HealResultItem::default();
            let d_err: Option<Error>;
            match self.tasks_rx.recv().await {
                Some(task) => {
                    if task.bucket == NOP_HEAL {
                        d_err = Some(Error::from_string("skip file"));
                    } else if task.bucket == SLASH_SEPARATOR {
                        match heal_disk_format(task.opts).await {
                            Ok((res, err)) => {
                                d_res = res;
                                d_err = err;
                            }
                            Err(err) => d_err = Some(err),
                        }
                    } else {
                        let layer = new_object_layer_fn();
                        let lock = layer.read().await;
                        let store = lock.as_ref().expect("Not init");
                        if task.object.is_empty() {
                            match store.heal_bucket(&task.bucket, &task.opts).await {
                                Ok(res) => {
                                    d_res = res;
                                    d_err = None;
                                }
                                Err(err) => d_err = Some(err),
                            }
                        } else {
                            match store
                                .heal_object(&task.bucket, &task.object, &task.version_id, &task.opts)
                                .await
                            {
                                Ok((res, err)) => {
                                    d_res = res;
                                    d_err = err;
                                }
                                Err(err) => d_err = Some(err),
                            }
                        }
                    }
                    if let Some(resp_tx) = task.resp_tx {
                        let _ = resp_tx
                            .send(HealResult {
                                result: d_res,
                                err: d_err,
                            })
                            .await;
                    } else {
                        // when respCh is not set caller is not waiting but we
                        // update the relevant metrics for them
                        if d_err.is_none() {
                            bgseq.write().await.count_healed(d_res.heal_item_type);
                        } else {
                            bgseq.write().await.count_failed(d_res.heal_item_type);
                        }
                    }
                }
                None => return,
            }
        }
    }
}

// pub fn active_listeners() -> Result<usize> {

// }

async fn heal_disk_format(opts: HealOpts) -> Result<(HealResultItem, Option<Error>)> {
    let layer = new_object_layer_fn();
    let lock = layer.read().await;
    let store = lock.as_ref().expect("Not init");
    let (res, err) = store.heal_format(opts.dry_run).await?;
    // return any error, ignore error returned when disks have
    // already healed.
    if err.is_some() {
        return Ok((HealResultItem::default(), err));
    }
    Ok((res, err))
}
