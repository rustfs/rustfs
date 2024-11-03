use std::sync::Arc;

use tokio::{
    select,
    sync::{
        broadcast::Receiver as B_Receiver,
        mpsc::{self, Receiver, Sender},
    },
};

use crate::{error::Error, heal::heal_ops::NOP_HEAL, utils::path::SLASH_SEPARATOR};

use super::{
    heal_commands::{HealOpts, HealResultItem},
    heal_ops::HealSequence,
};

#[derive(Clone, Debug)]
pub struct HealTask {
    pub bucket: String,
    pub object: String,
    pub version_id: String,
    pub opts: HealOpts,
    pub resp_tx: Arc<Sender<HealResult>>,
    pub resp_rx: Arc<Receiver<HealResult>>,
}

impl HealTask {
    pub fn new(bucket: &str, object: &str, version_id: &str, opts: &HealOpts) -> Self {
        let (tx, rx) = mpsc::channel(10);
        Self {
            bucket: bucket.to_string(),
            object: object.to_string(),
            version_id: version_id.to_string(),
            opts: opts.clone(),
            resp_tx: tx.into(),
            resp_rx: rx.into(),
        }
    }
}

pub struct HealResult {
    pub result: HealResultItem,
    err: Error,
}

pub struct HealRoutine {
    tasks_tx: Sender<HealTask>,
    tasks_rx: Receiver<HealTask>,
    workers: usize,
}

impl HealRoutine {
    pub async fn add_worker(&mut self, mut ctx: B_Receiver<bool>, bgseq: &HealSequence) {
        loop {
            select! {
                task = self.tasks_rx.recv() => {
                    let mut res = HealResultItem::default();
                    let mut err: Error;
                    match task {
                        Some(task) => {
                            if task.bucket == NOP_HEAL {
                                err = Error::from_string("skip file");
                            } else if task.bucket == SLASH_SEPARATOR {
                                (res, err) = heal_disk_format(task.opts).await;
                            }
                        },
                        None => return,
                    }
                }
                _ = ctx.recv() => {
                    return;
                }
            }
        }
    }
}

// pub fn active_listeners() -> Result<usize> {

// }

async fn heal_disk_format(opts: HealOpts) -> (HealResultItem, Error) {
    todo!()
}
