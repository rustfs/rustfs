// use std::collections::HashMap;
// use std::fmt;
// use std::sync::Arc;
// use std::sync::atomic::AtomicI32;

// use crate::resyncer::ReplicationResyncer;
// use crate::types::MRFReplicateEntry;
// use crate::types::ReplicationWorkerOperation;
// use rustfs_ecstore::StorageAPI;
// use serde::Deserialize;
// use serde::Serialize;
// use time::OffsetDateTime;
// use tokio::sync::Mutex;
// use tokio::sync::RwLock;
// use tokio::sync::mpsc;
// use tokio_util::sync::CancellationToken;

// pub struct ReplicationPool {
//     // 原子操作字段
//     active_workers: Arc<AtomicI32>,
//     active_lrg_workers: Arc<AtomicI32>,
//     active_mrf_workers: Arc<AtomicI32>,

//     // 基础配置
//     obj_layer: Arc<dyn StorageAPI>,
//     cancellation_token: CancellationToken,
//     priority: String,
//     max_workers: i32,
//     max_l_workers: i32,
//     // stats: Arc<ReplicationStats>,

//     // 互斥锁
//     mu: Arc<RwLock<()>>,
//     mrf_mu: Arc<Mutex<()>>,
//     resyncer: Arc<ReplicationResyncer>,

//     // 工作者通道
//     workers: Arc<RwLock<Vec<mpsc::UnboundedSender<Box<dyn ReplicationWorkerOperation>>>>>,
//     lrg_workers: Arc<RwLock<Vec<mpsc::UnboundedSender<Box<dyn ReplicationWorkerOperation>>>>>,

//     // MRF（Most Recent Failures）相关通道
//     mrf_worker_kill_ch: mpsc::UnboundedSender<()>,
//     mrf_replica_ch: mpsc::UnboundedSender<Box<dyn ReplicationWorkerOperation>>,
//     mrf_save_ch: mpsc::UnboundedSender<MRFReplicateEntry>,
//     mrf_stop_ch: mpsc::UnboundedSender<()>,
//     mrf_worker_size: AtomicI32,
// }
