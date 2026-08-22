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

use crate::bucket::metadata_sys;
use crate::cluster::rpc::client::{
    AuthenticatedChannel, TonicInterceptor, gen_tonic_signature_interceptor, is_network_like_disk_error,
    node_service_time_out_client,
};
use crate::cluster::rpc::set_tonic_mutation_body_digest;
use crate::disk::error::DiskError;
use crate::disk::error::{Error, Result};
use crate::disk::error_reduce::{BUCKET_OP_IGNORED_ERRS, is_all_buckets_not_found, reduce_write_quorum_errs};
use crate::disk::{DiskAPI, DiskStore, disk_store::get_max_timeout_duration};
use crate::runtime::instance::{InstanceContext, bootstrap_ctx};
use crate::runtime::sources as runtime_sources;
use crate::storage_api_contracts::bucket::{BucketInfo, BucketOptions, DeleteBucketOptions, MakeBucketOptions};
use crate::store::{has_xlmeta_files, utils::is_reserved_or_invalid_bucket};
use crate::{
    disk::{
        self, VolumeInfo,
        disk_store::{DiskHealthTracker, get_drive_active_check_interval, get_drive_active_check_timeout},
    },
    layout::endpoints::{EndpointServerPools, Node},
};
use async_trait::async_trait;
use futures::future::join_all;
use rustfs_common::heal_channel::{DriveState, HealItemType, HealOpts, RUSTFS_RESERVED_BUCKET};
use rustfs_madmin::heal_commands::{HealDriveInfo, HealResultItem};
use rustfs_protos::proto_gen::node_service::node_service_client::NodeServiceClient;
use rustfs_protos::proto_gen::node_service::{
    DeleteBucketRequest, GetBucketInfoRequest, HealBucketRequest, ListBucketRequest, MakeBucketRequest,
};
#[cfg(test)]
use std::sync::{
    Mutex as StdMutex,
    atomic::{AtomicBool, Ordering},
};
use std::{collections::HashMap, fmt::Debug, sync::Arc, time::Duration};
#[cfg(test)]
use tokio::sync::Notify;
use tokio::{net::TcpStream, sync::RwLock, time};
use tokio_util::sync::CancellationToken;
use tonic::Request;
use tonic::service::interceptor::InterceptedService;
use tracing::{debug, info, warn};

type Client = Arc<Box<dyn PeerS3Client>>;

#[cfg(test)]
#[derive(Default)]
pub(crate) struct DeleteBucketEmptyScanBarrier {
    arrived: AtomicBool,
    arrived_notify: Notify,
    released: AtomicBool,
    release_notify: Notify,
}

#[cfg(test)]
impl DeleteBucketEmptyScanBarrier {
    pub(crate) async fn wait_until_paused(&self) {
        loop {
            let notified = self.arrived_notify.notified();
            if self.arrived.load(Ordering::Acquire) {
                return;
            }
            notified.await;
        }
    }

    pub(crate) fn release(&self) {
        self.released.store(true, Ordering::Release);
        self.release_notify.notify_waiters();
    }

    async fn pause(&self) {
        self.arrived.store(true, Ordering::Release);
        self.arrived_notify.notify_waiters();
        loop {
            let notified = self.release_notify.notified();
            if self.released.load(Ordering::Acquire) {
                return;
            }
            notified.await;
        }
    }
}

#[cfg(test)]
static DELETE_BUCKET_EMPTY_SCAN_BARRIER: StdMutex<Option<Arc<DeleteBucketEmptyScanBarrier>>> = StdMutex::new(None);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum HealBucketOperation {
    Make,
    Delete,
}

#[cfg(test)]
struct HealBucketOperationFailure {
    bucket: String,
    disk_index: usize,
    operation: HealBucketOperation,
}

#[cfg(test)]
type HealBucketOperationFailureKey = (String, usize, HealBucketOperation);

#[cfg(test)]
fn heal_bucket_operation_failures() -> &'static StdMutex<HashMap<HealBucketOperationFailureKey, Error>> {
    static FAILURES: std::sync::OnceLock<StdMutex<HashMap<HealBucketOperationFailureKey, Error>>> = std::sync::OnceLock::new();
    FAILURES.get_or_init(|| StdMutex::new(HashMap::new()))
}

#[cfg(test)]
impl HealBucketOperationFailure {
    fn install(bucket: &str, disk_index: usize, operation: HealBucketOperation, error: Error) -> Self {
        let key = (bucket.to_string(), disk_index, operation);
        let previous = heal_bucket_operation_failures()
            .lock()
            .expect("heal bucket failure registry should not poison")
            .insert(key, error);
        assert!(previous.is_none(), "heal bucket operation failure already installed");
        Self {
            bucket: bucket.to_string(),
            disk_index,
            operation,
        }
    }
}

#[cfg(test)]
impl Drop for HealBucketOperationFailure {
    fn drop(&mut self) {
        heal_bucket_operation_failures()
            .lock()
            .expect("heal bucket failure registry should not poison")
            .remove(&(self.bucket.clone(), self.disk_index, self.operation));
    }
}

#[cfg(test)]
fn injected_heal_bucket_operation_error(bucket: &str, disk_index: usize, operation: HealBucketOperation) -> Option<Error> {
    heal_bucket_operation_failures()
        .lock()
        .expect("heal bucket failure registry should not poison")
        .get(&(bucket.to_string(), disk_index, operation))
        .cloned()
}

#[cfg(not(test))]
fn injected_heal_bucket_operation_error(_bucket: &str, _disk_index: usize, _operation: HealBucketOperation) -> Option<Error> {
    None
}

#[cfg(test)]
pub(crate) fn install_delete_bucket_empty_scan_barrier() -> Arc<DeleteBucketEmptyScanBarrier> {
    let barrier = Arc::new(DeleteBucketEmptyScanBarrier::default());
    *DELETE_BUCKET_EMPTY_SCAN_BARRIER
        .lock()
        .expect("empty scan barrier lock should not be poisoned") = Some(barrier.clone());
    barrier
}

#[cfg(test)]
async fn pause_after_delete_bucket_empty_scan() {
    let barrier = DELETE_BUCKET_EMPTY_SCAN_BARRIER
        .lock()
        .expect("empty scan barrier lock should not be poisoned")
        .take();
    if let Some(barrier) = barrier {
        barrier.pause().await;
    }
}

#[derive(Clone, Debug)]
pub struct ScannerBucketListing {
    pub buckets: Vec<BucketInfo>,
    pub set_buckets: Vec<ScannerSetBucketListing>,
    pub topology_complete: bool,
}

#[derive(Clone, Debug)]
pub struct ScannerSetBucketListing {
    pub pool_index: usize,
    pub set_index: usize,
    pub buckets: Vec<BucketInfo>,
}

fn pool_participant_errors(clients: &[Client], errors: &[Option<Error>], pool_idx: usize) -> Vec<Option<Error>> {
    clients
        .iter()
        .zip(errors.iter())
        .filter_map(|(client, err)| {
            if client.get_pools().unwrap_or_default().contains(&pool_idx) {
                Some(err.clone())
            } else {
                None
            }
        })
        .collect()
}

fn pool_write_quorum(participant_count: usize) -> usize {
    (participant_count / 2) + 1
}

/// Error for a peer that reported `success = false` without an error payload.
///
/// The message must stay identical across the peers of one operation: `reduce_errs`
/// buckets `Error::Io` by kind plus rendered message, so any per-peer detail (address,
/// timing) would split one shared failure into single-count buckets and downgrade a real
/// dominant error into `ErasureWriteQuorum`.
///
/// `peer_rest_client` carries the same helper over `StorageError` for the same response shape.
fn peer_failure_without_details(op: &str, bucket: Option<&str>) -> Error {
    match bucket {
        Some(bucket) => Error::other(format!("{op}({bucket}): peer returned failure without error details")),
        None => Error::other(format!("{op}: peer returned failure without error details")),
    }
}

fn reduce_pool_write_quorum_errs(per_pool_errs: &[Option<Error>]) -> Option<Error> {
    if per_pool_errs.is_empty() {
        return Some(Error::ErasureWriteQuorum);
    }

    reduce_write_quorum_errs(per_pool_errs, BUCKET_OP_IGNORED_ERRS, pool_write_quorum(per_pool_errs.len()))
}

fn resolve_heal_bucket_mode(opts: &mut HealOpts, pool_errs: &[Option<Error>]) -> Result<()> {
    if opts.recreate {
        return Ok(());
    }
    if let Some(err) = pool_errs
        .iter()
        .flatten()
        .find(|err| **err != Error::DiskNotFound && **err != Error::VolumeNotFound)
    {
        return Err(err.clone());
    }
    opts.remove = is_all_buckets_not_found(pool_errs);
    opts.recreate = !opts.remove;
    Ok(())
}

#[async_trait]
pub trait PeerS3Client: Debug + Sync + Send + 'static {
    async fn heal_bucket(&self, bucket: &str, opts: &HealOpts) -> Result<HealResultItem>;
    async fn make_bucket(&self, bucket: &str, opts: &MakeBucketOptions) -> Result<()>;
    async fn list_bucket(&self, opts: &BucketOptions) -> Result<Vec<BucketInfo>>;
    async fn delete_bucket(&self, bucket: &str, opts: &DeleteBucketOptions) -> Result<()>;
    async fn get_bucket_info(&self, bucket: &str, opts: &BucketOptions) -> Result<BucketInfo>;
    fn get_pools(&self) -> Option<Vec<usize>>;
}

#[derive(Debug, Clone)]
pub struct S3PeerSys {
    pub clients: Vec<Client>,
    pub pools_count: usize,
}

impl S3PeerSys {
    pub fn new(eps: &EndpointServerPools) -> Self {
        Self::new_with_instance_ctx(eps, bootstrap_ctx())
    }

    /// Build the peer system bound to an explicit instance context
    /// (backlog#1052 S7): the local peer client operates on that instance's
    /// disks. [`S3PeerSys::new`] keeps the ambient bootstrap default.
    pub fn new_with_instance_ctx(eps: &EndpointServerPools, instance_ctx: Arc<InstanceContext>) -> Self {
        Self {
            clients: Self::new_clients(eps, instance_ctx),
            pools_count: eps.as_ref().len(),
        }
    }

    fn new_clients(eps: &EndpointServerPools, instance_ctx: Arc<InstanceContext>) -> Vec<Client> {
        let nodes = eps.get_nodes();
        let v: Vec<Client> = nodes
            .iter()
            .map(|e| {
                if e.is_local {
                    let cli: Box<dyn PeerS3Client> = Box::new(LocalPeerS3Client::new_with_instance_ctx(
                        Some(e.clone()),
                        Some(e.pools.clone()),
                        instance_ctx.clone(),
                    ));
                    Arc::new(cli)
                } else {
                    let cli: Box<dyn PeerS3Client> = Box::new(RemotePeerS3Client::new(Some(e.clone()), Some(e.pools.clone())));
                    Arc::new(cli)
                }
            })
            .collect();

        v
    }
}

impl S3PeerSys {
    pub async fn heal_bucket(&self, bucket: &str, opts: &HealOpts) -> Result<HealResultItem> {
        let mut opts = *opts;
        let mut futures = Vec::with_capacity(self.clients.len());
        for client in self.clients.iter() {
            // client_clon
            futures.push(async move { (client.get_bucket_info(bucket, &BucketOptions::default()).await).err() });
        }
        let errs = join_all(futures).await;

        let mut pool_errs = Vec::new();
        for pool_idx in 0..self.pools_count {
            let per_pool_errs = pool_participant_errors(&self.clients, &errs, pool_idx);
            pool_errs.push(reduce_pool_write_quorum_errs(&per_pool_errs));
        }

        resolve_heal_bucket_mode(&mut opts, &pool_errs)?;

        let mut futures = Vec::new();
        let heal_bucket_results = Arc::new(RwLock::new(vec![HealResultItem::default(); self.clients.len()]));
        for (idx, client) in self.clients.iter().enumerate() {
            let opts_clone = opts;
            let heal_bucket_results_clone = heal_bucket_results.clone();
            futures.push(async move {
                match client.heal_bucket(bucket, &opts_clone).await {
                    Ok(res) => {
                        heal_bucket_results_clone.write().await[idx] = res;
                        None
                    }
                    Err(err) => Some(err),
                }
            });
        }
        let errs = join_all(futures).await;

        for pool_idx in 0..self.pools_count {
            let per_pool_errs = pool_participant_errors(&self.clients, &errs, pool_idx);
            if let Some(pool_err) = reduce_pool_write_quorum_errs(&per_pool_errs) {
                tracing::error!("heal_bucket per_pool_errs: {per_pool_errs:?}");
                tracing::error!("heal_bucket reduce_write_quorum_errs: {pool_err}");
                return Err(pool_err);
            }
        }

        for (i, err) in errs.iter().enumerate() {
            if err.is_none() {
                return Ok(heal_bucket_results.read().await[i].clone());
            }
        }
        Err(Error::VolumeNotFound)
    }

    pub async fn make_bucket(&self, bucket: &str, opts: &MakeBucketOptions) -> Result<()> {
        let mut futures = Vec::with_capacity(self.clients.len());
        for cli in self.clients.iter() {
            futures.push(cli.make_bucket(bucket, opts));
        }

        let mut errors = vec![None; self.clients.len()];

        let results = join_all(futures).await;
        for (i, result) in results.into_iter().enumerate() {
            match result {
                Ok(_) => {
                    errors[i] = None;
                }
                Err(e) => {
                    errors[i] = Some(e);
                }
            }
        }

        for i in 0..self.pools_count {
            let per_pool_errs = pool_participant_errors(&self.clients, &errors, i);
            if let Some(pool_err) = reduce_pool_write_quorum_errs(&per_pool_errs) {
                tracing::error!("make_bucket per_pool_errs: {per_pool_errs:?}");
                tracing::error!("make_bucket reduce_write_quorum_errs: {pool_err}");
                return Err(pool_err);
            }
        }

        Ok(())
    }
    pub async fn list_bucket(&self, opts: &BucketOptions) -> Result<Vec<BucketInfo>> {
        Ok(self.list_bucket_for_scanner(opts).await?.buckets)
    }

    pub async fn list_bucket_for_scanner(&self, opts: &BucketOptions) -> Result<ScannerBucketListing> {
        let mut futures = Vec::with_capacity(self.clients.len());
        for cli in self.clients.iter() {
            futures.push(cli.list_bucket(opts));
        }

        let mut errors = vec![None; self.clients.len()];
        let mut node_buckets = vec![None; self.clients.len()];

        let results = join_all(futures).await;
        for (i, result) in results.into_iter().enumerate() {
            match result {
                Ok(res) => {
                    node_buckets[i] = Some(res);
                    errors[i] = None;
                }
                Err(e) => {
                    node_buckets[i] = None;
                    errors[i] = Some(e);
                }
            }
        }

        let mut result_map: HashMap<&String, BucketInfo> = HashMap::new();
        let mut topology_complete = true;
        for i in 0..self.pools_count {
            let per_pool_errs = pool_participant_errors(&self.clients, &errors, i);
            let quorum = pool_write_quorum(per_pool_errs.len());
            topology_complete &=
                !per_pool_errs.is_empty() && per_pool_errs.iter().all(|participant_error| participant_error.is_none());

            if let Some(pool_err) = reduce_pool_write_quorum_errs(&per_pool_errs) {
                tracing::error!("list_bucket per_pool_errs: {per_pool_errs:?}");
                tracing::error!("list_bucket reduce_write_quorum_errs: {pool_err}");
                return Err(pool_err);
            }

            let mut bucket_map: HashMap<&String, usize> = HashMap::new();
            for (j, node_bucket) in node_buckets.iter().enumerate() {
                if let Some(buckets) = node_bucket.as_ref() {
                    if buckets.is_empty() {
                        continue;
                    }

                    if !self.clients[j].get_pools().unwrap_or_default().contains(&i) {
                        continue;
                    }

                    for bucket in buckets.iter() {
                        // incr bucket_map count create if not exists
                        let count = bucket_map.entry(&bucket.name).or_insert(0usize);
                        *count += 1;

                        if *count >= quorum {
                            result_map.entry(&bucket.name).or_insert_with(|| bucket.clone());
                        }
                    }
                }
            }
            topology_complete &= bucket_map.values().all(|count| *count >= quorum);
            // TODO(backlog): integrate MRF backlog stats into scanner bucket listing
        }

        let mut buckets: Vec<BucketInfo> = result_map.into_values().collect();

        buckets.sort_by_key(|b| b.name.clone());

        Ok(ScannerBucketListing {
            buckets,
            set_buckets: Vec::new(),
            topology_complete,
        })
    }
    pub async fn delete_bucket(&self, bucket: &str, opts: &DeleteBucketOptions) -> Result<()> {
        let mut futures = Vec::with_capacity(self.clients.len());
        for cli in self.clients.iter() {
            futures.push(cli.delete_bucket(bucket, opts));
        }

        let mut errors = vec![None; self.clients.len()];

        let results = join_all(futures).await;

        for (i, result) in results.into_iter().enumerate() {
            match result {
                Ok(_) => {
                    errors[i] = None;
                }
                Err(e) => {
                    errors[i] = Some(e);
                }
            }
        }

        for i in 0..self.pools_count {
            let per_pool_errs = pool_participant_errors(&self.clients, &errors, i);
            if let Some(err) = reduce_pool_write_quorum_errs(&per_pool_errs) {
                if !Error::is_err_object_not_found(&err) && !opts.no_recreate {
                    let make_bucket_opts = MakeBucketOptions::default();
                    let mut rollback_futures = Vec::new();
                    for (client, delete_err) in self.clients.iter().zip(errors.iter()) {
                        if delete_err.is_none() {
                            rollback_futures.push(client.make_bucket(bucket, &make_bucket_opts));
                        }
                    }
                    for rollback_result in join_all(rollback_futures).await {
                        if let Err(rollback_err) = rollback_result {
                            warn!("delete_bucket rollback make_bucket failed: {rollback_err}");
                        }
                    }
                }
                return Err(err);
            }
        }

        if self.pools_count == 0
            && let Some(err) = reduce_write_quorum_errs(&errors, BUCKET_OP_IGNORED_ERRS, (errors.len() / 2) + 1)
        {
            if !Error::is_err_object_not_found(&err) && !opts.no_recreate {
                let make_bucket_opts = MakeBucketOptions::default();
                let mut rollback_futures = Vec::new();
                for (client, delete_err) in self.clients.iter().zip(errors.iter()) {
                    if delete_err.is_none() {
                        rollback_futures.push(client.make_bucket(bucket, &make_bucket_opts));
                    }
                }
                for rollback_result in join_all(rollback_futures).await {
                    if let Err(rollback_err) = rollback_result {
                        warn!("delete_bucket rollback make_bucket failed: {rollback_err}");
                    }
                }
            }
            return Err(err);
        }

        Ok(())
    }
    pub async fn get_bucket_info(&self, bucket: &str, opts: &BucketOptions) -> Result<BucketInfo> {
        let mut futures = Vec::with_capacity(self.clients.len());
        for cli in self.clients.iter() {
            futures.push(cli.get_bucket_info(bucket, opts));
        }

        let mut ress = vec![None; self.clients.len()];
        let mut errors = vec![None; self.clients.len()];

        let results = join_all(futures).await;
        for (i, result) in results.into_iter().enumerate() {
            match result {
                Ok(res) => {
                    ress[i] = Some(res);
                    errors[i] = None;
                }
                Err(e) => {
                    ress[i] = None;
                    errors[i] = Some(e);
                }
            }
        }

        for i in 0..self.pools_count {
            let per_pool_errs = pool_participant_errors(&self.clients, &errors, i);
            if let Some(pool_err) = reduce_pool_write_quorum_errs(&per_pool_errs) {
                return Err(pool_err);
            }
        }

        ress.into_iter()
            .filter(|op| op.is_some())
            .find_map(|op| op)
            .ok_or(Error::VolumeNotFound)
    }

    pub fn get_pools(&self) -> Option<Vec<usize>> {
        None
    }
}

#[derive(Debug)]
pub struct LocalPeerS3Client {
    #[cfg(test)]
    local_disks: Option<Vec<DiskStore>>,
    // pub node: Node,
    pub pools: Option<Vec<usize>>,
    /// The owning store's runtime context (backlog#1052 S7): local bucket
    /// operations list/create/delete on THIS instance's registered disks, not
    /// on whatever the ambient process default resolves to.
    instance_ctx: Arc<InstanceContext>,
}

impl LocalPeerS3Client {
    pub fn new(node: Option<Node>, pools: Option<Vec<usize>>) -> Self {
        Self::new_with_instance_ctx(node, pools, bootstrap_ctx())
    }

    pub fn new_with_instance_ctx(_node: Option<Node>, pools: Option<Vec<usize>>, instance_ctx: Arc<InstanceContext>) -> Self {
        Self {
            #[cfg(test)]
            local_disks: None,
            // node,
            pools,
            instance_ctx,
        }
    }

    #[cfg(test)]
    fn new_with_local_disks(_node: Option<Node>, pools: Option<Vec<usize>>, local_disks: Vec<DiskStore>) -> Self {
        Self {
            local_disks: Some(local_disks),
            pools,
            instance_ctx: bootstrap_ctx(),
        }
    }

    async fn local_disks_for_pools(&self) -> Vec<DiskStore> {
        #[cfg(test)]
        let local_disks = if let Some(local_disks) = self.local_disks.as_ref() {
            local_disks.clone()
        } else {
            runtime_sources::local_disks_in(&self.instance_ctx).await
        };
        #[cfg(not(test))]
        let local_disks = runtime_sources::local_disks_in(&self.instance_ctx).await;
        let Some(pools) = self.pools.as_ref() else {
            return local_disks;
        };

        local_disks
            .into_iter()
            .filter(|disk| usize::try_from(disk.endpoint().pool_idx).is_ok_and(|pool_idx| pools.contains(&pool_idx)))
            .collect()
    }
}

#[async_trait]
impl PeerS3Client for LocalPeerS3Client {
    fn get_pools(&self) -> Option<Vec<usize>> {
        self.pools.clone()
    }

    async fn heal_bucket(&self, bucket: &str, opts: &HealOpts) -> Result<HealResultItem> {
        let disks = self.local_disks_for_pools().await.into_iter().map(Some).collect();
        heal_bucket_local_on_disks(bucket, opts, disks).await
    }

    async fn list_bucket(&self, _opts: &BucketOptions) -> Result<Vec<BucketInfo>> {
        let local_disks = self.local_disks_for_pools().await;
        if local_disks.is_empty() {
            return Err(Error::ErasureWriteQuorum);
        }

        let mut futures = Vec::with_capacity(local_disks.len());
        for disk in local_disks.iter() {
            futures.push(disk.list_volumes());
        }

        let results = join_all(futures).await;

        let mut ress = Vec::with_capacity(local_disks.len());
        let mut errs = Vec::with_capacity(local_disks.len());

        for result in results {
            match result {
                Ok(res) => {
                    ress.push(Some(res));
                    errs.push(None);
                }
                Err(e) => {
                    ress.push(None);
                    errs.push(Some(e));
                }
            }
        }

        if let Some(err) = reduce_write_quorum_errs(&errs, BUCKET_OP_IGNORED_ERRS, (local_disks.len() / 2) + 1) {
            return Err(err);
        }

        let quorum = (local_disks.len() / 2) + 1;
        let mut count_map: HashMap<&String, (usize, &VolumeInfo)> = HashMap::new();
        for info_list in ress.iter().flatten() {
            for info in info_list.iter() {
                if is_reserved_or_invalid_bucket(&info.name, false) {
                    continue;
                }

                let entry = count_map.entry(&info.name).or_insert((0, info));
                entry.0 += 1;
            }
        }

        let buckets: Vec<BucketInfo> = count_map
            .values()
            .filter_map(|(count, info)| {
                if *count < quorum {
                    return None;
                }

                Some(BucketInfo {
                    name: info.name.clone(),
                    created: info.created,
                    ..Default::default()
                })
            })
            .collect();

        Ok(buckets)
    }
    async fn make_bucket(&self, bucket: &str, opts: &MakeBucketOptions) -> Result<()> {
        let local_disks = self.local_disks_for_pools().await;
        if local_disks.is_empty() {
            return Err(Error::ErasureWriteQuorum);
        }

        let mut futures = Vec::with_capacity(local_disks.len());
        for disk in local_disks.iter() {
            futures.push(async move {
                match disk.make_volume(bucket).await {
                    Ok(_) => Ok(()),
                    Err(e) => {
                        if opts.force_create && matches!(e, Error::VolumeExists) {
                            return Ok(());
                        }
                        Err(e)
                    }
                }
            });
        }

        let results = join_all(futures).await;

        let mut errs = Vec::new();

        for res in results {
            match res {
                Ok(_) => errs.push(None),
                Err(e) => errs.push(Some(e)),
            }
        }

        if let Some(err) = reduce_write_quorum_errs(&errs, BUCKET_OP_IGNORED_ERRS, (local_disks.len() / 2) + 1) {
            return Err(err);
        }

        Ok(())
    }

    async fn get_bucket_info(&self, bucket: &str, _opts: &BucketOptions) -> Result<BucketInfo> {
        let local_disks = self.local_disks_for_pools().await;
        if local_disks.is_empty() {
            return Err(Error::ErasureWriteQuorum);
        }

        let mut futures = Vec::with_capacity(local_disks.len());
        for disk in local_disks.iter() {
            futures.push(disk.stat_volume(bucket));
        }

        let results = join_all(futures).await;

        let mut ress = Vec::with_capacity(local_disks.len());
        let mut errs = Vec::with_capacity(local_disks.len());

        for res in results {
            match res {
                Ok(r) => {
                    errs.push(None);
                    ress.push(Some(r));
                }
                Err(e) => {
                    errs.push(Some(e));
                    ress.push(None);
                }
            }
        }

        if let Some(err) = reduce_write_quorum_errs(&errs, BUCKET_OP_IGNORED_ERRS, (local_disks.len() / 2) + 1) {
            return Err(err);
        }

        let mut versioned = false;
        if let Ok(sys) = metadata_sys::get(bucket).await {
            versioned = sys.versioning();
        }

        ress.iter()
            .find_map(|op| {
                op.as_ref().map(|v| BucketInfo {
                    name: v.name.clone(),
                    created: v.created,
                    versioning: versioned,
                    ..Default::default()
                })
            })
            .ok_or(Error::VolumeNotFound)
    }

    async fn delete_bucket(&self, bucket: &str, opts: &DeleteBucketOptions) -> Result<()> {
        let local_disks = self.local_disks_for_pools().await;
        if local_disks.is_empty() {
            return Err(Error::ErasureWriteQuorum);
        }

        if opts.force_if_empty && !opts.force {
            for disk in local_disks.iter() {
                let Some(bucket_path) = disk.get_bucket_path_for_io_if_local(bucket) else {
                    continue;
                };
                let bucket_path = bucket_path?;
                if has_xlmeta_files(&bucket_path).await.map_err(Error::Io)? {
                    return Err(Error::VolumeNotEmpty);
                }
            }
            #[cfg(test)]
            pause_after_delete_bucket_empty_scan().await;
        }

        let mut futures = Vec::with_capacity(local_disks.len());

        for disk in local_disks.iter() {
            // `force_if_empty` is validation-only. Passing it as force would let
            // a PutObject committed after the scan be removed recursively.
            futures.push(disk.delete_volume(bucket, opts.force));
        }

        let results = join_all(futures).await;

        let mut errs = Vec::new();

        let mut recreate = false;

        for res in results {
            match res {
                Ok(_) => errs.push(None),
                Err(e) => {
                    if matches!(e, Error::VolumeNotEmpty) {
                        recreate = true;
                    }
                    errs.push(Some(e))
                }
            }
        }

        for (idx, err) in errs.iter().enumerate() {
            if err.is_none()
                && recreate
                && let Err(rollback_err) = local_disks[idx].make_volume(bucket).await
            {
                warn!("local delete_bucket rollback make_volume failed: {rollback_err}");
            }
        }

        if recreate {
            return Err(Error::VolumeNotEmpty);
        }

        if let Some(err) = reduce_write_quorum_errs(&errs, BUCKET_OP_IGNORED_ERRS, (local_disks.len() / 2) + 1) {
            if !Error::is_err_object_not_found(&err) && !opts.no_recreate {
                for (idx, delete_err) in errs.iter().enumerate() {
                    if delete_err.is_none()
                        && let Err(rollback_err) = local_disks[idx].make_volume(bucket).await
                    {
                        warn!("local delete_bucket rollback make_volume failed: {rollback_err}");
                    }
                }
            }
            return Err(err);
        }

        Ok(())
    }
}

#[derive(Debug)]
pub struct RemotePeerS3Client {
    pub pools: Option<Vec<usize>>,
    addr: String,
    /// Health tracker for connection monitoring
    health: Arc<DiskHealthTracker>,
    /// Cancellation token for monitoring tasks
    cancel_token: CancellationToken,
}

impl RemotePeerS3Client {
    fn encode_delete_bucket_options(opts: &DeleteBucketOptions) -> Result<String> {
        let mut remote_opts = opts.clone();
        // Older peers promote `force_if_empty` to recursive force after their
        // metadata scan. Keep this coordinator-only hint off the wire so a
        // mixed-version delete fails closed on non-empty directory remnants.
        remote_opts.force_if_empty = false;
        serde_json::to_string(&remote_opts).map_err(Into::into)
    }

    fn recovery_monitor_span(addr: &str) -> tracing::Span {
        tracing::info_span!(
            "recovery-monitor",
            component = "ecstore",
            subsystem = "peer_s3_client",
            kind = "peer_s3",
            addr = %addr
        )
    }

    pub fn new(node: Option<Node>, pools: Option<Vec<usize>>) -> Self {
        let addr = node.as_ref().map(|v| v.url.to_string()).unwrap_or_default();
        let client = Self {
            pools,
            addr,
            health: Arc::new(DiskHealthTracker::new()),
            cancel_token: CancellationToken::new(),
        };

        // Start health monitoring
        client.start_health_monitoring();

        client
    }

    pub async fn get_client(&self) -> Result<NodeServiceClient<InterceptedService<AuthenticatedChannel, TonicInterceptor>>> {
        node_service_time_out_client(&self.addr, TonicInterceptor::Signature(gen_tonic_signature_interceptor()))
            .await
            .map_err(|err| Error::other(format!("can not get client, err: {err}")))
    }

    /// Start health monitoring for the remote peer
    fn start_health_monitoring(&self) {
        let health = Arc::clone(&self.health);
        let cancel_token = self.cancel_token.clone();
        let addr = self.addr.clone();

        tokio::spawn(async move {
            Self::monitor_remote_peer_health(addr, health, cancel_token).await;
        });
    }

    /// Monitor remote peer health periodically
    async fn monitor_remote_peer_health(addr: String, health: Arc<DiskHealthTracker>, cancel_token: CancellationToken) {
        let mut interval = time::interval(get_drive_active_check_interval());

        loop {
            tokio::select! {
                _ = cancel_token.cancelled() => {
                    debug!("Health monitoring cancelled for remote peer: {}", addr);
                    return;
                }
                _ = interval.tick() => {
                    if cancel_token.is_cancelled() {
                        return;
                    }

                    // Skip health check if peer is already marked as faulty
                    if health.is_faulty() {
                        continue;
                    }

                    // Perform basic connectivity check
                    if Self::perform_connectivity_check(&addr).await.is_err() && health.swap_ok_to_faulty() {
                        warn!("Remote peer health check failed for {}: marking as faulty", addr);

                        // Start recovery monitoring
                        let health_clone = Arc::clone(&health);
                        let addr_clone = addr.clone();
                        let cancel_clone = cancel_token.clone();
                        let span = Self::recovery_monitor_span(&addr_clone);

                        super::spawn_background_monitor(span, async move {
                            Self::monitor_remote_peer_recovery(addr_clone, health_clone, cancel_clone).await;
                        });
                    }
                }
            }
        }
    }

    /// Monitor remote peer recovery and mark as healthy when recovered
    async fn monitor_remote_peer_recovery(addr: String, health: Arc<DiskHealthTracker>, cancel_token: CancellationToken) {
        let mut interval = time::interval(Duration::from_secs(5)); // Check every 5 seconds

        loop {
            tokio::select! {
                _ = cancel_token.cancelled() => {
                    return;
                }
                _ = interval.tick() => {
                    if Self::perform_connectivity_check(&addr).await.is_ok() {
                        info!("Remote peer recovered: {}", addr);
                        health.set_ok();
                        return;
                    }
                }
            }
        }
    }

    /// Perform basic connectivity check for remote peer
    async fn perform_connectivity_check(addr: &str) -> Result<()> {
        use tokio::time::timeout;

        let url = url::Url::parse(addr).map_err(|e| Error::other(format!("Invalid URL: {e}")))?;

        let Some(host) = url.host_str() else {
            return Err(Error::other("No host in URL".to_string()));
        };

        let port = url.port_or_known_default().unwrap_or(80);

        // Try to establish TCP connection
        match timeout(get_drive_active_check_timeout(), TcpStream::connect((host, port))).await {
            Ok(Ok(_)) => Ok(()),
            _ => Err(Error::other(format!("Cannot connect to {host}:{port}"))),
        }
    }

    /// Execute operation with timeout and health tracking
    async fn execute_with_timeout<T, F, Fut>(&self, operation: F, timeout_duration: Duration) -> Result<T>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        // Check if peer is faulty
        if self.health.is_faulty() {
            return Err(DiskError::FaultyDisk);
        }

        // Record operation start
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as i64;
        self.health.last_started.store(now, std::sync::atomic::Ordering::Relaxed);
        self.health.increment_waiting();

        // Execute operation with timeout
        let result = time::timeout(timeout_duration, operation()).await;

        match result {
            Ok(operation_result) => {
                // Log success and decrement waiting counter
                if operation_result.is_ok() {
                    self.health.log_success();
                }
                self.health.decrement_waiting();
                if let Err(err) = &operation_result
                    && is_network_like_disk_error(err)
                {
                    self.mark_faulty_and_start_recovery("operation_network_error").await;
                }
                operation_result
            }
            Err(_) => {
                // Timeout occurred, mark peer as potentially faulty
                self.health.decrement_waiting();
                self.mark_faulty_and_start_recovery("operation_timeout").await;
                warn!("Remote peer operation timeout after {:?}", timeout_duration);
                Err(Error::other(format!("Remote peer operation timeout after {timeout_duration:?}")))
            }
        }
    }

    async fn mark_faulty_and_start_recovery(&self, reason: &'static str) {
        if self.health.swap_ok_to_faulty() {
            warn!(
                addr = %self.addr,
                reason,
                "Remote peer marked faulty after network failure"
            );

            let health = Arc::clone(&self.health);
            let cancel_token = self.cancel_token.clone();
            let addr = self.addr.clone();
            let span = Self::recovery_monitor_span(&addr);
            super::spawn_background_monitor(span, async move {
                Self::monitor_remote_peer_recovery(addr, health, cancel_token).await;
            });
        }
    }
}

#[async_trait]
impl PeerS3Client for RemotePeerS3Client {
    fn get_pools(&self) -> Option<Vec<usize>> {
        self.pools.clone()
    }

    async fn heal_bucket(&self, bucket: &str, opts: &HealOpts) -> Result<HealResultItem> {
        self.execute_with_timeout(
            || async {
                let options: String = serde_json::to_string(opts)?;
                let mut client = self.get_client().await?;
                let mut request = Request::new(HealBucketRequest {
                    bucket: bucket.to_string(),
                    options,
                });
                set_tonic_mutation_body_digest(&mut request)?;
                let response = client.heal_bucket(request).await?.into_inner();
                if !response.success {
                    return if let Some(err) = response.error {
                        Err(err.into())
                    } else {
                        Err(peer_failure_without_details("heal_bucket", Some(bucket)))
                    };
                }

                Ok(HealResultItem {
                    heal_item_type: HealItemType::Bucket.to_string(),
                    bucket: bucket.to_string(),
                    set_count: 0,
                    ..Default::default()
                })
            },
            get_max_timeout_duration(),
        )
        .await
    }

    async fn list_bucket(&self, opts: &BucketOptions) -> Result<Vec<BucketInfo>> {
        self.execute_with_timeout(
            || async {
                let options = serde_json::to_string(opts)?;
                let mut client = self.get_client().await?;
                let request = Request::new(ListBucketRequest { options });
                let response = client.list_bucket(request).await?.into_inner();
                if !response.success {
                    return if let Some(err) = response.error {
                        Err(err.into())
                    } else {
                        Err(peer_failure_without_details("list_bucket", None))
                    };
                }
                let bucket_infos = response
                    .bucket_infos
                    .into_iter()
                    .filter_map(|json_str| serde_json::from_str::<BucketInfo>(&json_str).ok())
                    .collect();

                Ok(bucket_infos)
            },
            get_max_timeout_duration(),
        )
        .await
    }
    async fn make_bucket(&self, bucket: &str, opts: &MakeBucketOptions) -> Result<()> {
        self.execute_with_timeout(
            || async {
                let options = serde_json::to_string(opts)?;
                let mut client = self.get_client().await?;
                let mut request = Request::new(MakeBucketRequest {
                    name: bucket.to_string(),
                    options,
                });
                set_tonic_mutation_body_digest(&mut request)?;
                let response = client.make_bucket(request).await?.into_inner();

                if !response.success {
                    return if let Some(err) = response.error {
                        Err(err.into())
                    } else {
                        Err(peer_failure_without_details("make_bucket", Some(bucket)))
                    };
                }

                Ok(())
            },
            get_max_timeout_duration(),
        )
        .await
    }
    async fn get_bucket_info(&self, bucket: &str, opts: &BucketOptions) -> Result<BucketInfo> {
        self.execute_with_timeout(
            || async {
                let options = serde_json::to_string(opts)?;
                let mut client = self.get_client().await?;
                let request = Request::new(GetBucketInfoRequest {
                    bucket: bucket.to_string(),
                    options,
                });
                let response = client.get_bucket_info(request).await?.into_inner();
                if !response.success {
                    return if let Some(err) = response.error {
                        Err(err.into())
                    } else {
                        Err(peer_failure_without_details("get_bucket_info", Some(bucket)))
                    };
                }
                let bucket_info = serde_json::from_str::<BucketInfo>(&response.bucket_info)?;

                Ok(bucket_info)
            },
            get_max_timeout_duration(),
        )
        .await
    }

    async fn delete_bucket(&self, bucket: &str, opts: &DeleteBucketOptions) -> Result<()> {
        self.execute_with_timeout(
            || async {
                let options = Self::encode_delete_bucket_options(opts)?;
                let mut client = self.get_client().await?;

                let mut request = Request::new(DeleteBucketRequest {
                    bucket: bucket.to_string(),
                    options,
                });
                set_tonic_mutation_body_digest(&mut request)?;
                let response = client.delete_bucket(request).await?.into_inner();
                if !response.success {
                    return if let Some(err) = response.error {
                        Err(err.into())
                    } else {
                        Err(peer_failure_without_details("delete_bucket", Some(bucket)))
                    };
                }

                Ok(())
            },
            get_max_timeout_duration(),
        )
        .await
    }
}

#[allow(
    dead_code,
    reason = "local bucket-heal path reached only by this file's tests (backlog#1823)"
)]
pub async fn heal_bucket_local(bucket: &str, opts: &HealOpts) -> Result<HealResultItem> {
    let disks = clone_drives().await;
    heal_bucket_local_on_disks(bucket, opts, disks).await
}

pub(crate) async fn heal_bucket_local_on_disks(
    bucket: &str,
    opts: &HealOpts,
    disks: Vec<Option<DiskStore>>,
) -> Result<HealResultItem> {
    let before_state = Arc::new(RwLock::new(vec![String::new(); disks.len()]));
    let after_state = Arc::new(RwLock::new(vec![String::new(); disks.len()]));

    let mut futures = Vec::new();
    for (index, disk) in disks.iter().enumerate() {
        let disk = disk.clone();
        let bucket = bucket.to_string();
        let bs_clone = before_state.clone();
        let as_clone = after_state.clone();
        futures.push(async move {
            let disk = match disk {
                Some(disk) => disk,
                None => {
                    bs_clone.write().await[index] = DriveState::Offline.to_string();
                    as_clone.write().await[index] = DriveState::Offline.to_string();
                    return Some(Error::DiskNotFound);
                }
            };
            bs_clone.write().await[index] = DriveState::Ok.to_string();
            as_clone.write().await[index] = DriveState::Ok.to_string();

            if bucket == RUSTFS_RESERVED_BUCKET {
                return None;
            }

            match disk.stat_volume(&bucket).await {
                Ok(_) => None,
                Err(err) => match err {
                    Error::DiskNotFound => {
                        bs_clone.write().await[index] = DriveState::Offline.to_string();
                        as_clone.write().await[index] = DriveState::Offline.to_string();
                        Some(err)
                    }
                    Error::VolumeNotFound => {
                        bs_clone.write().await[index] = DriveState::Missing.to_string();
                        as_clone.write().await[index] = DriveState::Missing.to_string();
                        Some(err)
                    }
                    _ => {
                        bs_clone.write().await[index] = DriveState::Corrupt.to_string();
                        as_clone.write().await[index] = DriveState::Corrupt.to_string();
                        Some(err)
                    }
                },
            }
        });
    }
    let errs = join_all(futures).await;
    let mut res = HealResultItem {
        heal_item_type: HealItemType::Bucket.to_string(),
        bucket: bucket.to_string(),
        disk_count: disks.len(),
        set_count: 0,
        ..Default::default()
    };

    for (disk, state) in disks.iter().zip(before_state.read().await.iter()) {
        res.before.drives.push(HealDriveInfo {
            uuid: "".to_string(),
            endpoint: disk.clone().map(|s| s.to_string()).unwrap_or_default(),
            state: state.to_string(),
        });
    }

    if opts.dry_run {
        for (disk, state) in disks.iter().zip(after_state.read().await.iter()) {
            res.after.drives.push(HealDriveInfo {
                uuid: "".to_string(),
                endpoint: disk.clone().map(|s| s.to_string()).unwrap_or_default(),
                state: state.to_string(),
            });
        }
        return Ok(res);
    }

    let mut operation_error = errs
        .iter()
        .filter_map(|err| match err {
            Some(Error::VolumeNotFound) | None => None,
            Some(err) => Some(err.clone()),
        })
        .next();

    if opts.remove && !bucket.starts_with(disk::RUSTFS_META_BUCKET) && !is_all_buckets_not_found(&errs) {
        let mut futures = Vec::new();
        for (index, disk) in disks.iter().enumerate() {
            if matches!(errs[index].as_ref(), Some(Error::DiskNotFound | Error::VolumeNotFound)) {
                continue;
            }
            let Some(disk) = disk.clone() else {
                continue;
            };
            let bucket = bucket.to_string();
            futures.push(async move {
                if let Some(err) = injected_heal_bucket_operation_error(&bucket, index, HealBucketOperation::Delete) {
                    return (index, Err(err));
                }
                (index, disk.delete_volume(&bucket, false).await)
            });
        }

        for (index, result) in join_all(futures).await {
            match result {
                Ok(()) | Err(Error::VolumeNotFound) => {
                    after_state.write().await[index] = DriveState::Missing.to_string();
                }
                Err(Error::VolumeNotEmpty) => {
                    warn!(
                        bucket,
                        operation = "heal_bucket_delete_volume",
                        result = "preserved_non_empty_bucket",
                        "heal declined to remove non-empty bucket"
                    );
                    after_state.write().await[index] = DriveState::Ok.to_string();
                }
                Err(err) => {
                    after_state.write().await[index] = match &err {
                        Error::DiskNotFound => DriveState::Offline.to_string(),
                        _ => DriveState::Corrupt.to_string(),
                    };
                    if operation_error.is_none() {
                        operation_error = Some(err);
                    }
                }
            }
        }
    }

    if !opts.remove {
        let mut futures = Vec::new();
        for (idx, disk) in disks.iter().enumerate() {
            let disk = disk.clone();
            let bucket = bucket.to_string();
            let bs_clone = before_state.clone();
            futures.push(async move {
                if bs_clone.read().await[idx] == DriveState::Missing.to_string() {
                    let Some(disk) = disk.as_ref() else {
                        return (idx, Some(Error::DiskNotFound));
                    };

                    if let Some(err) = injected_heal_bucket_operation_error(&bucket, idx, HealBucketOperation::Make) {
                        return (idx, Some(err));
                    }
                    match disk.make_volume(&bucket).await {
                        Ok(()) | Err(Error::VolumeExists) => return (idx, None),
                        Err(err) => return (idx, Some(err)),
                    }
                }
                (idx, None)
            });
        }

        for (index, result) in join_all(futures).await {
            match result {
                None => {
                    if before_state.read().await[index] == DriveState::Missing.to_string() {
                        after_state.write().await[index] = DriveState::Ok.to_string();
                    }
                }
                Some(err) => {
                    after_state.write().await[index] = match &err {
                        Error::DiskNotFound => DriveState::Offline.to_string(),
                        _ => DriveState::Corrupt.to_string(),
                    };
                    if operation_error.is_none() {
                        operation_error = Some(err);
                    }
                }
            }
        }
    }

    for (disk, state) in disks.iter().zip(after_state.read().await.iter()) {
        res.after.drives.push(HealDriveInfo {
            uuid: "".to_string(),
            endpoint: disk.clone().map(|s| s.to_string()).unwrap_or_default(),
            state: state.to_string(),
        });
    }

    match operation_error {
        Some(err) => Err(err),
        None => Ok(res),
    }
}

#[allow(
    dead_code,
    reason = "reached only through heal_bucket_local, which only tests call (backlog#1823)"
)]
async fn clone_drives() -> Vec<Option<DiskStore>> {
    runtime_sources::local_disk_entries().await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::disk::WalkDirOptions;
    use crate::disk::disk_store::LocalDiskWrapper;
    use crate::disk::endpoint::Endpoint;
    use crate::disk::local::LocalDisk;
    use crate::layout::endpoints::{Endpoints, PoolEndpoints};
    use crate::runtime::global::reset_local_disk_test_state;
    use crate::store::init_local_disks;
    use rustfs_filemeta::FileInfo;
    use serial_test::serial;
    use std::{
        io,
        pin::Pin,
        sync::atomic::{AtomicUsize, Ordering},
        task::{Context, Poll},
    };
    use tempfile::TempDir;
    use tokio::io::AsyncWrite;

    struct PendingWriter;

    impl AsyncWrite for PendingWriter {
        fn poll_write(self: Pin<&mut Self>, _cx: &mut Context<'_>, _buf: &[u8]) -> Poll<io::Result<usize>> {
            Poll::Pending
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    #[derive(Debug)]
    struct TestPeerS3Client {
        pools: Option<Vec<usize>>,
        make_bucket_result: Result<()>,
        list_bucket_result: Result<Vec<BucketInfo>>,
        delete_bucket_result: Result<()>,
        make_bucket_calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl PeerS3Client for TestPeerS3Client {
        async fn heal_bucket(&self, _bucket: &str, _opts: &HealOpts) -> Result<HealResultItem> {
            unreachable!("not used by quorum tests")
        }

        async fn make_bucket(&self, _bucket: &str, _opts: &MakeBucketOptions) -> Result<()> {
            self.make_bucket_calls.fetch_add(1, Ordering::SeqCst);
            self.make_bucket_result.clone()
        }

        async fn list_bucket(&self, _opts: &BucketOptions) -> Result<Vec<BucketInfo>> {
            self.list_bucket_result.clone()
        }

        async fn delete_bucket(&self, _bucket: &str, _opts: &DeleteBucketOptions) -> Result<()> {
            self.delete_bucket_result.clone()
        }

        async fn get_bucket_info(&self, _bucket: &str, _opts: &BucketOptions) -> Result<BucketInfo> {
            unreachable!("not used by quorum tests")
        }

        fn get_pools(&self) -> Option<Vec<usize>> {
            self.pools.clone()
        }
    }

    fn test_peer(pools: &[usize]) -> Client {
        test_peer_with_make_bucket(pools, Ok(()))
    }

    fn test_peer_with_make_bucket(pools: &[usize], make_bucket_result: Result<()>) -> Client {
        test_peer_with_results(pools, make_bucket_result, Ok(Vec::new()))
    }

    fn test_peer_with_list_bucket(pools: &[usize], list_bucket_result: Result<Vec<BucketInfo>>) -> Client {
        test_peer_with_results(pools, Ok(()), list_bucket_result)
    }

    fn test_peer_with_delete_bucket(pools: &[usize], delete_bucket_result: Result<()>) -> Client {
        test_peer_with_delete_bucket_and_make_counter(pools, delete_bucket_result, Arc::new(AtomicUsize::new(0)))
    }

    fn test_peer_with_delete_bucket_and_make_counter(
        pools: &[usize],
        delete_bucket_result: Result<()>,
        make_bucket_calls: Arc<AtomicUsize>,
    ) -> Client {
        test_peer_with_all_results(pools, Ok(()), Ok(Vec::new()), delete_bucket_result, make_bucket_calls)
    }

    fn test_peer_with_results(
        pools: &[usize],
        make_bucket_result: Result<()>,
        list_bucket_result: Result<Vec<BucketInfo>>,
    ) -> Client {
        test_peer_with_all_results(pools, make_bucket_result, list_bucket_result, Ok(()), Arc::new(AtomicUsize::new(0)))
    }

    fn test_peer_with_all_results(
        pools: &[usize],
        make_bucket_result: Result<()>,
        list_bucket_result: Result<Vec<BucketInfo>>,
        delete_bucket_result: Result<()>,
        make_bucket_calls: Arc<AtomicUsize>,
    ) -> Client {
        Arc::new(Box::new(TestPeerS3Client {
            pools: Some(pools.to_vec()),
            make_bucket_result,
            list_bucket_result,
            delete_bucket_result,
            make_bucket_calls,
        }))
    }

    fn test_endpoint(path: &std::path::Path, pool_index: usize, set_index: usize, disk_index: usize) -> Endpoint {
        let mut endpoint = Endpoint::try_from(path.to_str().expect("disk path to str")).expect("endpoint");
        endpoint.set_pool_index(pool_index);
        endpoint.set_set_index(set_index);
        endpoint.set_disk_index(disk_index);
        endpoint
    }

    async fn init_test_local_disks(temp_dir: &TempDir, disk_count: usize, cmd_line: &str) -> Vec<DiskStore> {
        init_test_local_disks_for_pools(temp_dir, &[(0, disk_count)], cmd_line).await
    }

    async fn init_test_local_disks_for_pools(
        temp_dir: &TempDir,
        pool_disk_counts: &[(usize, usize)],
        cmd_line: &str,
    ) -> Vec<DiskStore> {
        let total_disk_count = pool_disk_counts.iter().map(|(_, disk_count)| *disk_count).sum();
        let mut endpoints = Vec::with_capacity(total_disk_count);
        let mut pool_endpoints = Vec::with_capacity(pool_disk_counts.len());
        for (pool_idx, disk_count) in pool_disk_counts.iter().copied() {
            let mut endpoints_for_pool = Vec::with_capacity(disk_count);
            for disk_idx in 0..disk_count {
                let disk_path = temp_dir.path().join(format!("pool{pool_idx}-disk{disk_idx}"));
                std::fs::create_dir_all(&disk_path).expect("create disk path");
                let endpoint = test_endpoint(&disk_path, pool_idx, 0, disk_idx);
                endpoints.push(endpoint.clone());
                endpoints_for_pool.push(endpoint);
            }

            pool_endpoints.push(PoolEndpoints {
                legacy: false,
                set_count: 1,
                drives_per_set: disk_count,
                endpoints: Endpoints::from(endpoints_for_pool),
                cmd_line: cmd_line.to_string(),
                platform: "test".to_string(),
            });
        }

        let endpoint_pools = EndpointServerPools(pool_endpoints);

        init_local_disks(endpoint_pools).await.expect("init local disks");

        let mut disks = Vec::with_capacity(total_disk_count);
        for endpoint in endpoints.iter() {
            let disk = Arc::new(LocalDisk::new(endpoint, false).await.expect("local disk should be created"));
            let wrapper = crate::disk::Disk::Local(Box::new(LocalDiskWrapper::new(disk, false)));
            disks.push(Arc::new(wrapper) as DiskStore);
        }

        disks
    }

    fn test_remote_peer(addr: &str) -> RemotePeerS3Client {
        RemotePeerS3Client {
            pools: Some(vec![0]),
            addr: addr.to_string(),
            health: Arc::new(DiskHealthTracker::new()),
            cancel_token: CancellationToken::new(),
        }
    }

    #[test]
    fn remote_delete_bucket_options_fail_closed_for_legacy_peers() {
        let encoded = RemotePeerS3Client::encode_delete_bucket_options(&DeleteBucketOptions {
            no_lock: true,
            no_recreate: true,
            force_if_empty: true,
            ..Default::default()
        })
        .expect("remote delete options should serialize");
        let legacy_opts: DeleteBucketOptions =
            serde_json::from_str(&encoded).expect("legacy peer should decode remote delete options");

        assert!(legacy_opts.no_lock);
        assert!(legacy_opts.no_recreate);
        assert!(!legacy_opts.force);
        assert!(!legacy_opts.force_if_empty);

        let legacy_recursive_force = if legacy_opts.force_if_empty && !legacy_opts.force {
            true
        } else {
            legacy_opts.force
        };
        assert!(
            !legacy_recursive_force,
            "legacy peer must not upgrade empty-only delete to recursive force"
        );
    }

    #[test]
    fn remote_delete_bucket_options_preserve_explicit_force() {
        let encoded = RemotePeerS3Client::encode_delete_bucket_options(&DeleteBucketOptions {
            force: true,
            force_if_empty: true,
            ..Default::default()
        })
        .expect("remote force-delete options should serialize");
        let remote_opts: DeleteBucketOptions =
            serde_json::from_str(&encoded).expect("remote peer should decode force-delete options");

        assert!(remote_opts.force);
        assert!(!remote_opts.force_if_empty);
    }

    #[tokio::test]
    async fn test_execute_with_timeout_marks_remote_peer_faulty_on_network_like_error() {
        let client = test_remote_peer("http://peer-network-error:9000");

        let err = client
            .execute_with_timeout(
                || async {
                    Err::<(), Error>(DiskError::Io(std::io::Error::new(
                        std::io::ErrorKind::ConnectionRefused,
                        "connection refused",
                    )))
                },
                Duration::from_secs(1),
            )
            .await
            .expect_err("network-like error should fail");

        assert_eq!(
            match &err {
                DiskError::Io(io_err) => io_err.kind(),
                other => panic!("expected io network error, got {other:?}"),
            },
            std::io::ErrorKind::ConnectionRefused
        );
        assert!(client.health.is_faulty(), "network-like errors should mark remote peer faulty");

        client.cancel_token.cancel();
    }

    #[tokio::test]
    async fn test_execute_with_timeout_keeps_remote_peer_online_for_business_error() {
        let client = test_remote_peer("http://peer-business-error:9000");

        let err = client
            .execute_with_timeout(|| async { Err::<(), Error>(DiskError::FileNotFound) }, Duration::from_secs(1))
            .await
            .expect_err("business error should fail");

        assert_eq!(err, DiskError::FileNotFound);
        assert!(!client.health.is_faulty(), "business errors should not mark remote peer faulty");

        client.cancel_token.cancel();
    }

    #[tokio::test]
    #[serial]
    async fn local_get_bucket_info_survives_prior_walk_timeout() {
        reset_local_disk_test_state().await;

        let temp_dir = TempDir::new().expect("create temp dir for local peer listing regression");
        let disks = init_test_local_disks(&temp_dir, 1, "local-get-bucket-info-survives-prior-walk-timeout").await;
        let disk_store = disks[0].clone();
        let bucket = "test-bucket";
        let object = "test-object";

        disk_store.make_volume(bucket).await.expect("bucket should be created");

        let mut file_info = FileInfo::new(&format!("{bucket}/{object}"), 1, 0);
        file_info.volume = bucket.to_string();
        file_info.name = object.to_string();
        file_info.mod_time = Some(::time::OffsetDateTime::now_utc());
        file_info.erasure.index = 1;

        disk_store
            .write_metadata("", bucket, object, file_info)
            .await
            .expect("object metadata should be written");

        temp_env::async_with_vars([(rustfs_config::ENV_DRIVE_WALKDIR_TIMEOUT_SECS, Some("1"))], async {
            let mut writer = PendingWriter;
            let walk_err = disk_store
                .walk_dir(
                    WalkDirOptions {
                        bucket: bucket.to_string(),
                        recursive: true,
                        ..Default::default()
                    },
                    &mut writer,
                )
                .await
                .expect_err("walk_dir should time out against a non-draining writer");

            assert_eq!(walk_err, DiskError::Timeout);

            let info = LocalPeerS3Client::new_with_local_disks(None, Some(vec![0]), disks.clone())
                .get_bucket_info(bucket, &BucketOptions::default())
                .await
                .expect("bucket info should still succeed after prior walk timeout");
            assert_eq!(info.name, bucket);
        })
        .await;

        reset_local_disk_test_state().await;
    }

    #[tokio::test]
    #[serial]
    async fn local_get_bucket_info_requires_local_write_quorum() {
        reset_local_disk_test_state().await;

        let temp_dir = TempDir::new().expect("create temp dir for partial bucket regression");
        let disks = init_test_local_disks(&temp_dir, 2, "local-get-bucket-info-requires-local-write-quorum").await;

        disks[0]
            .make_volume("partial-bucket")
            .await
            .expect("bucket should be created on one disk");

        let err = LocalPeerS3Client::new_with_local_disks(None, Some(vec![0]), disks.clone())
            .get_bucket_info("partial-bucket", &BucketOptions::default())
            .await
            .expect_err("partial bucket should not satisfy local write quorum");

        assert_eq!(err, Error::ErasureWriteQuorum);

        reset_local_disk_test_state().await;
    }

    #[tokio::test]
    #[serial]
    async fn local_peer_filters_disks_by_pool() {
        reset_local_disk_test_state().await;

        let temp_dir = TempDir::new().expect("create temp dir for pool filtered local peer regression");
        let disks = init_test_local_disks_for_pools(&temp_dir, &[(0, 2), (1, 2)], "local-peer-filters-disks-by-pool").await;
        let bucket = "pool0-bucket";

        disks[0]
            .make_volume(bucket)
            .await
            .expect("bucket should be created on pool 0 disk 0");
        disks[1]
            .make_volume(bucket)
            .await
            .expect("bucket should be created on pool 0 disk 1");

        let pool0_info = LocalPeerS3Client::new_with_local_disks(None, Some(vec![0]), disks.clone())
            .get_bucket_info(bucket, &BucketOptions::default())
            .await
            .expect("pool 0 peer should see bucket on pool 0 disks");
        assert_eq!(pool0_info.name, bucket);

        let pool1_err = LocalPeerS3Client::new_with_local_disks(None, Some(vec![1]), disks.clone())
            .get_bucket_info(bucket, &BucketOptions::default())
            .await
            .expect_err("pool 1 peer should not count pool 0 disks");
        assert_eq!(pool1_err, Error::VolumeNotFound);

        let pool1_buckets = LocalPeerS3Client::new_with_local_disks(None, Some(vec![1]), disks.clone())
            .list_bucket(&BucketOptions::default())
            .await
            .expect("pool 1 local listing should succeed against its own disks");
        assert!(pool1_buckets.is_empty());

        reset_local_disk_test_state().await;
    }

    #[tokio::test]
    #[serial]
    async fn local_peer_force_if_empty_preserves_unclassified_file_in_selected_pool() {
        reset_local_disk_test_state().await;

        let temp_dir = TempDir::new().expect("create temp dir for empty-only delete regression");
        let disks = init_test_local_disks_for_pools(
            &temp_dir,
            &[(0, 1), (1, 1)],
            "local-peer-force-if-empty-preserves-unclassified-file",
        )
        .await;
        let bucket = "empty-only-delete-bucket";
        let marker = "object/commit-marker";
        let data = bytes::Bytes::from_static(b"committed object data");

        disks[1]
            .make_volume(bucket)
            .await
            .expect("bucket should be created in the selected pool");
        disks[1]
            .write_all(bucket, marker, data.clone())
            .await
            .expect("unclassified committed file should be written");

        let err = LocalPeerS3Client::new_with_local_disks(None, Some(vec![1]), disks.clone())
            .delete_bucket(
                bucket,
                &DeleteBucketOptions {
                    force_if_empty: true,
                    ..Default::default()
                },
            )
            .await
            .expect_err("empty-only delete must not recursively remove an unclassified file");

        assert_eq!(err, Error::VolumeNotEmpty);
        assert_eq!(
            disks[1]
                .read_all(bucket, marker)
                .await
                .expect("unclassified committed file should be preserved"),
            data
        );

        reset_local_disk_test_state().await;
    }

    #[tokio::test]
    #[serial]
    async fn heal_bucket_local_recreates_missing_bucket_volumes() {
        reset_local_disk_test_state().await;

        let temp_dir = TempDir::new().expect("create temp dir for bucket heal regression");
        let disks = init_test_local_disks(&temp_dir, 2, "heal-bucket-local-recreates-missing-bucket-volumes").await;
        let bucket = "healed-bucket";

        disks[0]
            .make_volume(bucket)
            .await
            .expect("bucket should be created on one disk");
        disks[1]
            .stat_volume(bucket)
            .await
            .expect_err("second disk should start missing the bucket");

        let result = heal_bucket_local(
            bucket,
            &HealOpts {
                recreate: true,
                ..Default::default()
            },
        )
        .await
        .expect("bucket heal should recreate missing volumes");

        assert_eq!(result.before.drives.len(), 2);
        assert_eq!(result.after.drives.len(), 2);
        assert!(
            result
                .before
                .drives
                .iter()
                .any(|drive| drive.state == DriveState::Missing.to_string()),
            "one bucket volume must be reported missing before heal"
        );
        assert!(
            result
                .after
                .drives
                .iter()
                .all(|drive| drive.state == DriveState::Ok.to_string()),
            "all bucket volumes must be reported healthy after heal"
        );

        for disk in disks {
            disk.stat_volume(bucket).await.expect("bucket should exist after heal");
        }

        reset_local_disk_test_state().await;
    }

    #[tokio::test]
    #[serial]
    async fn heal_bucket_local_dry_run_reports_discovered_drive_states() {
        reset_local_disk_test_state().await;

        let temp_dir = TempDir::new().expect("create temp dir for bucket heal dry-run regression");
        let disks = init_test_local_disks(&temp_dir, 2, "heal-bucket-local-dry-run-reports-state").await;
        let bucket = "dry-run-healed-bucket";
        disks[0]
            .make_volume(bucket)
            .await
            .expect("bucket should exist on the first disk");

        let result = heal_bucket_local_on_disks(
            bucket,
            &HealOpts {
                dry_run: true,
                ..Default::default()
            },
            vec![Some(disks[0].clone()), Some(disks[1].clone()), None],
        )
        .await
        .expect("dry-run bucket heal should inspect disks");

        assert_eq!(result.before.drives.len(), 3);
        assert_eq!(result.after.drives.len(), 3);
        assert_eq!(result.before.drives[0].state, DriveState::Ok.to_string());
        assert_eq!(result.before.drives[1].state, DriveState::Missing.to_string());
        assert_eq!(result.before.drives[2].state, DriveState::Offline.to_string());
        for (before, after) in result.before.drives.iter().zip(&result.after.drives) {
            assert_eq!(after.endpoint, before.endpoint);
            assert_eq!(after.state, before.state);
        }
        assert!(matches!(disks[1].stat_volume(bucket).await, Err(Error::VolumeNotFound)));

        reset_local_disk_test_state().await;
    }

    #[tokio::test]
    #[serial]
    async fn heal_bucket_local_propagates_recreate_failure() {
        reset_local_disk_test_state().await;

        let temp_dir = TempDir::new().expect("create temp dir for bucket recreate failure regression");
        let disks = init_test_local_disks(&temp_dir, 2, "heal-bucket-local-propagates-recreate-failure").await;
        let bucket = "recreate-failure-bucket";
        disks[0]
            .make_volume(bucket)
            .await
            .expect("bucket should exist on the first disk");
        let _failure = HealBucketOperationFailure::install(bucket, 1, HealBucketOperation::Make, Error::DiskAccessDenied);

        let error = heal_bucket_local_on_disks(
            bucket,
            &HealOpts {
                recreate: true,
                ..Default::default()
            },
            disks.iter().cloned().map(Some).collect(),
        )
        .await
        .expect_err("failed volume recreation must fail bucket heal");

        assert_eq!(error, Error::DiskAccessDenied);
        assert!(matches!(disks[1].stat_volume(bucket).await, Err(Error::VolumeNotFound)));

        reset_local_disk_test_state().await;
    }

    #[tokio::test]
    #[serial]
    async fn heal_bucket_local_propagates_delete_failure() {
        reset_local_disk_test_state().await;

        let temp_dir = TempDir::new().expect("create temp dir for bucket delete failure regression");
        let disks = init_test_local_disks(&temp_dir, 2, "heal-bucket-local-propagates-delete-failure").await;
        let bucket = "delete-failure-bucket";
        disks[0]
            .make_volume(bucket)
            .await
            .expect("bucket should exist on the first disk");
        let _failure = HealBucketOperationFailure::install(bucket, 0, HealBucketOperation::Delete, Error::DiskAccessDenied);

        let error = heal_bucket_local_on_disks(
            bucket,
            &HealOpts {
                remove: true,
                ..Default::default()
            },
            disks.iter().cloned().map(Some).collect(),
        )
        .await
        .expect_err("failed volume deletion must fail bucket heal");

        assert_eq!(error, Error::DiskAccessDenied);
        disks[0]
            .stat_volume(bucket)
            .await
            .expect("failed deletion must leave the bucket volume present");

        reset_local_disk_test_state().await;
    }

    #[tokio::test]
    #[serial]
    async fn heal_bucket_local_preserves_non_empty_bucket() {
        reset_local_disk_test_state().await;

        let temp_dir = TempDir::new().expect("create temp dir for non-empty bucket heal regression");
        let disks = init_test_local_disks(&temp_dir, 1, "heal-bucket-local-preserves-non-empty").await;
        let bucket = "non-empty-bucket";
        disks[0]
            .make_volume(bucket)
            .await
            .expect("bucket should exist on the first disk");
        let _failure = HealBucketOperationFailure::install(bucket, 0, HealBucketOperation::Delete, Error::VolumeNotEmpty);

        let result = heal_bucket_local_on_disks(
            bucket,
            &HealOpts {
                remove: true,
                ..Default::default()
            },
            disks.iter().cloned().map(Some).collect(),
        )
        .await
        .expect("a non-empty bucket refusal is an expected safety result");

        assert_eq!(result.after.drives.len(), 1);
        assert_eq!(result.after.drives[0].state, DriveState::Ok.to_string());
        disks[0]
            .stat_volume(bucket)
            .await
            .expect("the non-empty bucket must remain present");

        reset_local_disk_test_state().await;
    }

    #[tokio::test]
    #[serial]
    async fn heal_bucket_local_propagates_preexisting_offline_disk() {
        reset_local_disk_test_state().await;

        let temp_dir = TempDir::new().expect("create temp dir for offline bucket heal regression");
        let disks = init_test_local_disks(&temp_dir, 1, "heal-bucket-local-preexisting-offline").await;
        let bucket = "offline-disk-bucket";
        disks[0]
            .make_volume(bucket)
            .await
            .expect("bucket should exist on the online disk");

        let error = heal_bucket_local_on_disks(bucket, &HealOpts::default(), vec![Some(disks[0].clone()), None])
            .await
            .expect_err("a prepass offline disk must keep the bucket heal incomplete");

        assert_eq!(error, Error::DiskNotFound);

        reset_local_disk_test_state().await;
    }

    #[test]
    fn test_reduce_pool_write_quorum_uses_only_pool_participants() {
        let clients = vec![
            test_peer(&[0]),
            test_peer(&[0]),
            test_peer(&[0]),
            test_peer(&[0]),
            test_peer(&[1]),
            test_peer(&[1]),
            test_peer(&[1]),
            test_peer(&[1]),
        ];
        let errors = vec![
            Some(Error::VolumeExists),
            Some(Error::VolumeExists),
            Some(Error::VolumeExists),
            Some(Error::VolumeExists),
            None,
            None,
            None,
            None,
        ];

        let per_pool_errs = pool_participant_errors(&clients, &errors, 0);
        let err = reduce_pool_write_quorum_errs(&per_pool_errs).expect("all pool participants returned VolumeExists");

        assert_eq!(err, Error::VolumeExists);
    }

    #[test]
    fn heal_bucket_mode_fails_closed_on_incomplete_topology() {
        let mut opts = HealOpts::default();
        assert_eq!(
            resolve_heal_bucket_mode(&mut opts, &[Some(Error::ErasureWriteQuorum)]),
            Err(Error::ErasureWriteQuorum)
        );
        assert!(!opts.recreate);
        assert!(!opts.remove);
    }

    #[test]
    fn heal_bucket_mode_distinguishes_deleted_and_partial_buckets() {
        let mut deleted = HealOpts::default();
        resolve_heal_bucket_mode(&mut deleted, &[Some(Error::VolumeNotFound)]).unwrap();
        assert!(deleted.remove);
        assert!(!deleted.recreate);

        let mut partial = HealOpts::default();
        resolve_heal_bucket_mode(&mut partial, &[None, Some(Error::VolumeNotFound)]).unwrap();
        assert!(!partial.remove);
        assert!(partial.recreate);
    }

    #[tokio::test]
    async fn test_make_bucket_reduces_quorum_by_pool_participants() {
        let peer_sys = S3PeerSys {
            clients: vec![
                test_peer_with_make_bucket(&[0], Err(Error::VolumeExists)),
                test_peer_with_make_bucket(&[0], Err(Error::VolumeExists)),
                test_peer_with_make_bucket(&[0], Err(Error::VolumeExists)),
                test_peer_with_make_bucket(&[0], Err(Error::VolumeExists)),
                test_peer(&[1]),
                test_peer(&[1]),
                test_peer(&[1]),
                test_peer(&[1]),
            ],
            pools_count: 2,
        };

        let err = peer_sys
            .make_bucket("existing-bucket", &MakeBucketOptions::default())
            .await
            .expect_err("existing bucket should surface as VolumeExists, not quorum failure");

        assert_eq!(err, Error::VolumeExists);
    }

    #[tokio::test]
    async fn test_list_bucket_reduces_visibility_quorum_by_pool_participants() {
        let bucket = BucketInfo {
            name: "existing-bucket".to_string(),
            ..Default::default()
        };
        let peer_sys = S3PeerSys {
            clients: vec![
                test_peer_with_list_bucket(&[0], Ok(vec![bucket.clone()])),
                test_peer_with_list_bucket(&[1], Ok(vec![bucket.clone()])),
                test_peer_with_list_bucket(&[2], Ok(vec![bucket.clone()])),
                test_peer_with_list_bucket(&[3], Ok(vec![bucket.clone()])),
            ],
            pools_count: 4,
        };

        let buckets = peer_sys
            .list_bucket(&BucketOptions::default())
            .await
            .expect("single-participant pools should still expose visible buckets");

        assert_eq!(buckets.len(), 1);
        assert_eq!(buckets[0].name, bucket.name);
    }

    #[tokio::test]
    async fn scanner_bucket_listing_marks_quorum_result_incomplete_when_a_peer_is_missing() {
        let bucket = BucketInfo {
            name: "bucket-hidden-by-quorum".to_string(),
            ..Default::default()
        };
        let peer_sys = S3PeerSys {
            clients: vec![
                test_peer_with_list_bucket(&[0], Ok(vec![bucket])),
                test_peer_with_list_bucket(&[0], Ok(Vec::new())),
                test_peer_with_list_bucket(&[0], Ok(Vec::new())),
                test_peer_with_list_bucket(&[0], Err(Error::DiskAccessDenied)),
            ],
            pools_count: 1,
        };

        let listing = peer_sys
            .list_bucket_for_scanner(&BucketOptions::default())
            .await
            .expect("peer quorum should still produce a scanner candidate listing");

        assert!(listing.buckets.is_empty());
        assert!(!listing.topology_complete);
    }

    #[tokio::test]
    async fn scanner_bucket_listing_marks_divergent_successful_peers_incomplete() {
        let bucket = BucketInfo {
            name: "bucket-below-quorum".to_string(),
            ..Default::default()
        };
        let peer_sys = S3PeerSys {
            clients: vec![
                test_peer_with_list_bucket(&[0], Ok(vec![bucket.clone()])),
                test_peer_with_list_bucket(&[0], Ok(vec![bucket])),
                test_peer_with_list_bucket(&[0], Ok(Vec::new())),
                test_peer_with_list_bucket(&[0], Ok(Vec::new())),
            ],
            pools_count: 1,
        };

        let listing = peer_sys
            .list_bucket_for_scanner(&BucketOptions::default())
            .await
            .expect("successful peer responses should still produce a scanner candidate listing");

        assert!(listing.buckets.is_empty());
        assert!(!listing.topology_complete);
    }

    #[tokio::test]
    async fn scanner_bucket_listing_checks_same_bucket_in_every_pool() {
        let bucket = BucketInfo {
            name: "shared-bucket".to_string(),
            ..Default::default()
        };
        let peer_sys = S3PeerSys {
            clients: vec![
                test_peer_with_list_bucket(&[0], Ok(vec![bucket.clone()])),
                test_peer_with_list_bucket(&[0], Ok(vec![bucket.clone()])),
                test_peer_with_list_bucket(&[0], Ok(vec![bucket.clone()])),
                test_peer_with_list_bucket(&[0], Ok(vec![bucket.clone()])),
                test_peer_with_list_bucket(&[1], Ok(vec![bucket.clone()])),
                test_peer_with_list_bucket(&[1], Ok(vec![bucket.clone()])),
                test_peer_with_list_bucket(&[1], Ok(Vec::new())),
                test_peer_with_list_bucket(&[1], Ok(Vec::new())),
            ],
            pools_count: 2,
        };

        let listing = peer_sys
            .list_bucket_for_scanner(&BucketOptions::default())
            .await
            .expect("a bucket visible in one pool should remain a scan candidate");

        assert_eq!(listing.buckets.len(), 1);
        assert_eq!(listing.buckets[0].name, bucket.name);
        assert!(!listing.topology_complete);
    }

    #[tokio::test]
    async fn test_delete_bucket_fails_when_any_pool_misses_write_quorum() {
        let peer_sys = S3PeerSys {
            clients: vec![
                test_peer_with_delete_bucket(&[0], Ok(())),
                test_peer_with_delete_bucket(&[0], Ok(())),
                test_peer_with_delete_bucket(&[0], Err(Error::VolumeNotEmpty)),
                test_peer_with_delete_bucket(&[0], Err(Error::VolumeNotEmpty)),
                test_peer_with_delete_bucket(&[1], Ok(())),
                test_peer_with_delete_bucket(&[1], Ok(())),
                test_peer_with_delete_bucket(&[1], Ok(())),
                test_peer_with_delete_bucket(&[1], Ok(())),
            ],
            pools_count: 2,
        };

        let err = peer_sys
            .delete_bucket("partially-deleted-bucket", &DeleteBucketOptions::default())
            .await
            .expect_err("pool 0 should fail because it did not reach write quorum");

        assert_eq!(err, Error::ErasureWriteQuorum);
    }

    #[tokio::test]
    async fn test_delete_bucket_succeeds_when_every_pool_reaches_write_quorum() {
        let peer_sys = S3PeerSys {
            clients: vec![
                test_peer_with_delete_bucket(&[0], Ok(())),
                test_peer_with_delete_bucket(&[0], Ok(())),
                test_peer_with_delete_bucket(&[0], Ok(())),
                test_peer_with_delete_bucket(&[0], Err(Error::DiskNotFound)),
                test_peer_with_delete_bucket(&[1], Ok(())),
                test_peer_with_delete_bucket(&[1], Ok(())),
                test_peer_with_delete_bucket(&[1], Ok(())),
                test_peer_with_delete_bucket(&[1], Err(Error::DiskNotFound)),
            ],
            pools_count: 2,
        };

        peer_sys
            .delete_bucket("deleted-bucket", &DeleteBucketOptions::default())
            .await
            .expect("each pool reached write quorum");
    }

    #[tokio::test]
    async fn test_delete_bucket_rolls_back_only_successful_deletes_on_failure() {
        let make_bucket_calls = (0..8).map(|_| Arc::new(AtomicUsize::new(0))).collect::<Vec<_>>();
        let peer_sys = S3PeerSys {
            clients: vec![
                test_peer_with_delete_bucket_and_make_counter(&[0], Ok(()), make_bucket_calls[0].clone()),
                test_peer_with_delete_bucket_and_make_counter(&[0], Ok(()), make_bucket_calls[1].clone()),
                test_peer_with_delete_bucket_and_make_counter(&[0], Err(Error::DiskAccessDenied), make_bucket_calls[2].clone()),
                test_peer_with_delete_bucket_and_make_counter(&[0], Err(Error::DiskAccessDenied), make_bucket_calls[3].clone()),
                test_peer_with_delete_bucket_and_make_counter(&[1], Err(Error::DiskAccessDenied), make_bucket_calls[4].clone()),
                test_peer_with_delete_bucket_and_make_counter(&[1], Err(Error::DiskAccessDenied), make_bucket_calls[5].clone()),
                test_peer_with_delete_bucket_and_make_counter(&[1], Err(Error::DiskAccessDenied), make_bucket_calls[6].clone()),
                test_peer_with_delete_bucket_and_make_counter(&[1], Err(Error::DiskAccessDenied), make_bucket_calls[7].clone()),
            ],
            pools_count: 2,
        };

        let err = peer_sys
            .delete_bucket("rolled-back-bucket", &DeleteBucketOptions::default())
            .await
            .expect_err("delete failure should return the quorum error");

        assert_eq!(err, Error::ErasureWriteQuorum);
        let calls = make_bucket_calls
            .iter()
            .map(|call_count| call_count.load(Ordering::SeqCst))
            .collect::<Vec<_>>();
        assert_eq!(calls, vec![1, 1, 0, 0, 0, 0, 0, 0]);
    }

    #[test]
    fn peer_failure_without_details_names_operation_and_bucket() {
        for op in ["heal_bucket", "make_bucket", "get_bucket_info", "delete_bucket"] {
            let message = peer_failure_without_details(op, Some("ops-bucket")).to_string();
            assert!(message.contains(op), "{op} message must name the operation: {message}");
            assert!(message.contains("ops-bucket"), "{op} message must name the bucket: {message}");
        }

        let message = peer_failure_without_details("list_bucket", None).to_string();
        assert!(message.contains("list_bucket"), "cluster-wide message must name the operation");
        assert!(!message.trim().is_empty());
    }

    #[test]
    fn peer_failure_without_details_keeps_one_reduce_errs_bucket_per_operation() {
        // reduce_errs groups Io errors by kind plus rendered message: peers failing the
        // same operation on the same bucket must still reach quorum as one dominant error.
        let per_pool_errs = vec![
            Some(peer_failure_without_details("delete_bucket", Some("shared"))),
            Some(peer_failure_without_details("delete_bucket", Some("shared"))),
            Some(peer_failure_without_details("delete_bucket", Some("shared"))),
        ];
        assert_eq!(
            reduce_pool_write_quorum_errs(&per_pool_errs),
            Some(peer_failure_without_details("delete_bucket", Some("shared")))
        );

        assert_ne!(
            peer_failure_without_details("delete_bucket", Some("shared")),
            peer_failure_without_details("get_bucket_info", Some("shared"))
        );
    }
}
