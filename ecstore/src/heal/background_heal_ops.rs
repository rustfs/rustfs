use std::{env, sync::Arc};

use tokio::sync::{
    mpsc::{self, Receiver, Sender},
    RwLock,
};

use crate::{
    endpoints::Endpoints,
    error::{Error, Result},
    global::{GLOBAL_BackgroundHealRoutine, GLOBAL_BackgroundHealState, GLOBAL_LOCAL_DISK_MAP},
    heal::heal_ops::NOP_HEAL,
    new_object_layer_fn,
    store_api::StorageAPI,
    utils::path::SLASH_SEPARATOR,
};

use super::{
    heal_commands::{HealOpts, HealResultItem},
    heal_ops::{new_bg_heal_sequence, HealSequence},
};

pub async fn init_auto_heal() {
    init_background_healing().await;
    if let Ok(v) = env::var("_RUSTFS_AUTO_DRIVE_HEALING") {
        if v == "on" {
            // GLOBAL_BackgroundHealState.write().await.push_heal_local_disks(heal_local_disks).await;
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

async fn get_local_disks_to_heal() -> Endpoints {
    for (_, disk) in GLOBAL_LOCAL_DISK_MAP.read().await.iter() {}
    todo!()
}

#[derive(Clone, Debug)]
pub struct HealTask {
    pub bucket: String,
    pub object: String,
    pub version_id: String,
    pub opts: HealOpts,
    pub resp_tx: Option<Arc<Sender<HealResult>>>,
    pub resp_rx: Option<Arc<Receiver<HealResult>>>,
}

impl HealTask {
    pub fn new(bucket: &str, object: &str, version_id: &str, opts: &HealOpts) -> Self {
        let (tx, rx) = mpsc::channel(10);
        Self {
            bucket: bucket.to_string(),
            object: object.to_string(),
            version_id: version_id.to_string(),
            opts: opts.clone(),
            resp_tx: Some(tx.into()),
            resp_rx: Some(rx.into()),
        }
    }
}

pub struct HealResult {
    pub result: HealResultItem,
    _err: Option<Error>,
}

pub struct HealRoutine {
    tasks_tx: Sender<HealTask>,
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
                                _err: d_err,
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
    return Ok((res, err));
}
