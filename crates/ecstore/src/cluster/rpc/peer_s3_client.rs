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
    TonicInterceptor, gen_tonic_signature_interceptor, is_network_like_disk_error, node_service_time_out_client,
};
use crate::disk::error::DiskError;
use crate::disk::error::{Error, Result};
use crate::disk::error_reduce::{BUCKET_OP_IGNORED_ERRS, is_all_buckets_not_found, reduce_write_quorum_errs};
use crate::disk::{DiskAPI, DiskStore, disk_store::get_max_timeout_duration};
use crate::runtime::sources as runtime_sources;
use crate::storage_api_contracts::bucket::{BucketInfo, BucketOptions, DeleteBucketOptions, MakeBucketOptions};
use crate::store::all_local_disk;
use crate::store::utils::is_reserved_or_invalid_bucket;
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
use std::{collections::HashMap, fmt::Debug, sync::Arc, time::Duration};
use tokio::{net::TcpStream, sync::RwLock, time};
use tokio_util::sync::CancellationToken;
use tonic::Request;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;
use tracing::{debug, info, warn};

type Client = Arc<Box<dyn PeerS3Client>>;

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

fn reduce_pool_write_quorum_errs(per_pool_errs: &[Option<Error>]) -> Option<Error> {
    if per_pool_errs.is_empty() {
        return Some(Error::ErasureWriteQuorum);
    }

    reduce_write_quorum_errs(per_pool_errs, BUCKET_OP_IGNORED_ERRS, pool_write_quorum(per_pool_errs.len()))
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
        Self {
            clients: Self::new_clients(eps),
            pools_count: eps.as_ref().len(),
        }
    }

    fn new_clients(eps: &EndpointServerPools) -> Vec<Client> {
        let nodes = eps.get_nodes();
        let v: Vec<Client> = nodes
            .iter()
            .map(|e| {
                if e.is_local {
                    let cli: Box<dyn PeerS3Client> = Box::new(LocalPeerS3Client::new(Some(e.clone()), Some(e.pools.clone())));
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

        if !opts.recreate {
            opts.remove = is_all_buckets_not_found(&pool_errs);
            opts.recreate = !opts.remove;
        }

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
        for i in 0..self.pools_count {
            let per_pool_errs = pool_participant_errors(&self.clients, &errors, i);
            let quorum = pool_write_quorum(per_pool_errs.len());

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
                        if result_map.contains_key(&bucket.name) {
                            continue;
                        }

                        // incr bucket_map count create if not exists
                        let count = bucket_map.entry(&bucket.name).or_insert(0usize);
                        *count += 1;

                        if *count >= quorum {
                            result_map.insert(&bucket.name, bucket.clone());
                        }
                    }
                }
            }
            // TODO: MRF
        }

        let mut buckets: Vec<BucketInfo> = result_map.into_values().collect();

        buckets.sort_by_key(|b| b.name.clone());

        Ok(buckets)
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

        if let Some(err) = reduce_write_quorum_errs(&errors, BUCKET_OP_IGNORED_ERRS, (errors.len() / 2) + 1) {
            if !Error::is_err_object_not_found(&err) && !opts.no_recreate {
                let _ = self.make_bucket(bucket, &MakeBucketOptions::default()).await;
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
}

impl LocalPeerS3Client {
    pub fn new(_node: Option<Node>, pools: Option<Vec<usize>>) -> Self {
        Self {
            #[cfg(test)]
            local_disks: None,
            // node,
            pools,
        }
    }

    #[cfg(test)]
    fn new_with_local_disks(_node: Option<Node>, pools: Option<Vec<usize>>, local_disks: Vec<DiskStore>) -> Self {
        Self {
            local_disks: Some(local_disks),
            pools,
        }
    }

    async fn local_disks_for_pools(&self) -> Vec<DiskStore> {
        #[cfg(test)]
        let local_disks = if let Some(local_disks) = self.local_disks.as_ref() {
            local_disks.clone()
        } else {
            all_local_disk().await
        };
        #[cfg(not(test))]
        let local_disks = all_local_disk().await;
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

        let mut futures = Vec::with_capacity(local_disks.len());

        for disk in local_disks.iter() {
            // Non-force delete refuses a non-empty bucket (VolumeNotEmpty), which
            // the recreate loop below turns into BucketNotEmpty; only an explicit
            // force delete removes recursively (backlog#799 B1).
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

        // For errVolumeNotEmpty, do not delete; recreate only the entries already removed

        for (idx, err) in errs.into_iter().enumerate() {
            if err.is_none() && recreate {
                let _ = local_disks[idx].make_volume(bucket).await;
            }
        }

        if recreate {
            return Err(Error::VolumeNotEmpty);
        }

        // TODO: reduceWriteQuorumErrs

        Ok(())
    }
}

#[derive(Debug)]
pub struct RemotePeerS3Client {
    pub node: Option<Node>,
    pub pools: Option<Vec<usize>>,
    addr: String,
    /// Health tracker for connection monitoring
    health: Arc<DiskHealthTracker>,
    /// Cancellation token for monitoring tasks
    cancel_token: CancellationToken,
}

impl RemotePeerS3Client {
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
            node,
            pools,
            addr,
            health: Arc::new(DiskHealthTracker::new()),
            cancel_token: CancellationToken::new(),
        };

        // Start health monitoring
        client.start_health_monitoring();

        client
    }

    pub async fn get_client(&self) -> Result<NodeServiceClient<InterceptedService<Channel, TonicInterceptor>>> {
        node_service_time_out_client(&self.addr, TonicInterceptor::Signature(gen_tonic_signature_interceptor()))
            .await
            .map_err(|err| Error::other(format!("can not get client, err: {err}")))
    }

    pub fn get_addr(&self) -> String {
        self.addr.clone()
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
                let request = Request::new(HealBucketRequest {
                    bucket: bucket.to_string(),
                    options,
                });
                let response = client.heal_bucket(request).await?.into_inner();
                if !response.success {
                    return if let Some(err) = response.error {
                        Err(err.into())
                    } else {
                        Err(Error::other(""))
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
                        Err(Error::other(""))
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
                let request = Request::new(MakeBucketRequest {
                    name: bucket.to_string(),
                    options,
                });
                let response = client.make_bucket(request).await?.into_inner();

                if !response.success {
                    return if let Some(err) = response.error {
                        Err(err.into())
                    } else {
                        Err(Error::other(format!(
                            "make_bucket({bucket}): peer returned failure without error details"
                        )))
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
                        Err(Error::other(""))
                    };
                }
                let bucket_info = serde_json::from_str::<BucketInfo>(&response.bucket_info)?;

                Ok(bucket_info)
            },
            get_max_timeout_duration(),
        )
        .await
    }

    async fn delete_bucket(&self, bucket: &str, _opts: &DeleteBucketOptions) -> Result<()> {
        self.execute_with_timeout(
            || async {
                let mut client = self.get_client().await?;

                let request = Request::new(DeleteBucketRequest {
                    bucket: bucket.to_string(),
                });
                let response = client.delete_bucket(request).await?.into_inner();
                if !response.success {
                    return if let Some(err) = response.error {
                        Err(err.into())
                    } else {
                        Err(Error::other(""))
                    };
                }

                Ok(())
            },
            get_max_timeout_duration(),
        )
        .await
    }
}

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

    if opts.dry_run {
        return Ok(res);
    }

    for (disk, state) in disks.iter().zip(before_state.read().await.iter()) {
        res.before.drives.push(HealDriveInfo {
            uuid: "".to_string(),
            endpoint: disk.clone().map(|s| s.to_string()).unwrap_or_default(),
            state: state.to_string(),
        });
    }

    if opts.remove && !bucket.starts_with(disk::RUSTFS_META_BUCKET) && !is_all_buckets_not_found(&errs) {
        let mut futures = Vec::new();
        for disk in disks.iter() {
            let disk = disk.clone();
            let bucket = bucket.to_string();
            info!("heal_bucket_local, errs: {:?}, opts: {:?}", errs, opts);
            futures.push(async move {
                match disk {
                    Some(disk) => {
                        // Non-force: a bucket that still holds object data refuses
                        // deletion (VolumeNotEmpty) instead of being recursively
                        // wiped, so a misclassified "dangling" bucket cannot lose
                        // data (backlog#799 B1). Surface that refusal instead of
                        // discarding it — it signals the bucket is not dangling.
                        match disk.delete_volume(&bucket, false).await {
                            Ok(()) => None,
                            Err(Error::VolumeNotEmpty) => {
                                warn!("heal declined to remove non-empty bucket {bucket} (not dangling)");
                                None
                            }
                            Err(e) => Some(e),
                        }
                    }
                    None => Some(Error::DiskNotFound),
                }
            });
        }

        let _ = join_all(futures).await;
    }

    if !opts.remove {
        let mut futures = Vec::new();
        for (idx, disk) in disks.iter().enumerate() {
            let disk = disk.clone();
            let bucket = bucket.to_string();
            let bs_clone = before_state.clone();
            let as_clone = after_state.clone();
            let errs_clone = errs.to_vec();
            futures.push(async move {
                if bs_clone.read().await[idx] == DriveState::Missing.to_string() {
                    let Some(disk) = disk.as_ref() else {
                        return Some(Error::DiskNotFound);
                    };

                    info!("bucket not find, will recreate");
                    match disk.make_volume(&bucket).await {
                        Ok(_) => {
                            as_clone.write().await[idx] = DriveState::Ok.to_string();
                            return None;
                        }
                        Err(err) => {
                            return Some(err);
                        }
                    }
                }
                errs_clone[idx].clone()
            });
        }

        let _ = join_all(futures).await;
    }

    for (disk, state) in disks.iter().zip(after_state.read().await.iter()) {
        res.before.drives.push(HealDriveInfo {
            uuid: "".to_string(),
            endpoint: disk.clone().map(|s| s.to_string()).unwrap_or_default(),
            state: state.to_string(),
        });
    }

    Ok(res)
}

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
    }

    #[async_trait]
    impl PeerS3Client for TestPeerS3Client {
        async fn heal_bucket(&self, _bucket: &str, _opts: &HealOpts) -> Result<HealResultItem> {
            unreachable!("not used by quorum tests")
        }

        async fn make_bucket(&self, _bucket: &str, _opts: &MakeBucketOptions) -> Result<()> {
            self.make_bucket_result.clone()
        }

        async fn list_bucket(&self, _opts: &BucketOptions) -> Result<Vec<BucketInfo>> {
            self.list_bucket_result.clone()
        }

        async fn delete_bucket(&self, _bucket: &str, _opts: &DeleteBucketOptions) -> Result<()> {
            unreachable!("not used by quorum tests")
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

    fn test_peer_with_results(
        pools: &[usize],
        make_bucket_result: Result<()>,
        list_bucket_result: Result<Vec<BucketInfo>>,
    ) -> Client {
        Arc::new(Box::new(TestPeerS3Client {
            pools: Some(pools.to_vec()),
            make_bucket_result,
            list_bucket_result,
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
        let node = Node {
            url: url::Url::parse(addr).expect("test peer URL should parse"),
            pools: vec![0],
            is_local: false,
            grid_host: addr.to_string(),
        };

        RemotePeerS3Client {
            node: Some(node),
            pools: Some(vec![0]),
            addr: addr.to_string(),
            health: Arc::new(DiskHealthTracker::new()),
            cancel_token: CancellationToken::new(),
        }
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

        heal_bucket_local(
            bucket,
            &HealOpts {
                recreate: true,
                ..Default::default()
            },
        )
        .await
        .expect("bucket heal should recreate missing volumes");

        for disk in disks {
            disk.stat_volume(bucket).await.expect("bucket should exist after heal");
        }

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
}
