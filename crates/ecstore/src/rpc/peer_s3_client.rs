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
use crate::disk::error::{Error, Result};
use crate::disk::error_reduce::{BUCKET_OP_IGNORED_ERRS, is_all_buckets_not_found, reduce_write_quorum_errs};
use crate::disk::{DiskAPI, DiskStore};
use crate::global::GLOBAL_LOCAL_DISK_MAP;
use crate::store::all_local_disk;
use crate::store_utils::is_reserved_or_invalid_bucket;
use crate::{
    disk::{self, VolumeInfo},
    endpoints::{EndpointServerPools, Node},
    store_api::{BucketInfo, BucketOptions, DeleteBucketOptions, MakeBucketOptions},
};
use async_trait::async_trait;
use futures::future::join_all;
use rustfs_common::heal_channel::{DriveState, HealItemType, HealOpts, RUSTFS_RESERVED_BUCKET};
use rustfs_madmin::heal_commands::{HealDriveInfo, HealResultItem};
use rustfs_protos::node_service_time_out_client;
use rustfs_protos::proto_gen::node_service::{
    DeleteBucketRequest, GetBucketInfoRequest, HealBucketRequest, ListBucketRequest, MakeBucketRequest,
};
use std::{collections::HashMap, fmt::Debug, sync::Arc};
use tokio::sync::RwLock;
use tonic::Request;
use tracing::info;

type Client = Arc<Box<dyn PeerS3Client>>;

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
            let mut per_pool_errs = vec![None; self.clients.len()];
            for (i, client) in self.clients.iter().enumerate() {
                if let Some(v) = client.get_pools() {
                    if v.contains(&pool_idx) {
                        per_pool_errs[i] = errs[i].clone();
                    }
                }
            }
            let qu = per_pool_errs.len() / 2;
            pool_errs.push(reduce_write_quorum_errs(&per_pool_errs, BUCKET_OP_IGNORED_ERRS, qu));
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
            let mut per_pool_errs = vec![None; self.clients.len()];
            for (i, client) in self.clients.iter().enumerate() {
                if let Some(v) = client.get_pools() {
                    if v.contains(&pool_idx) {
                        per_pool_errs[i] = errs[i].clone();
                    }
                }
            }
            let qu = per_pool_errs.len() / 2;
            if let Some(pool_err) = reduce_write_quorum_errs(&per_pool_errs, BUCKET_OP_IGNORED_ERRS, qu) {
                tracing::error!("heal_bucket per_pool_errs: {per_pool_errs:?}");
                tracing::error!("heal_bucket reduce_write_quorum_errs: {pool_err}");
                return Err(pool_err);
            }
        }

        if let Some(err) = reduce_write_quorum_errs(&errs, BUCKET_OP_IGNORED_ERRS, (errs.len() / 2) + 1) {
            tracing::error!("heal_bucket errs: {errs:?}");
            tracing::error!("heal_bucket reduce_write_quorum_errs: {err}");
            return Err(err);
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
            let mut per_pool_errs = vec![None; self.clients.len()];
            for (j, cli) in self.clients.iter().enumerate() {
                let pools = cli.get_pools();
                let idx = i;
                if pools.unwrap_or_default().contains(&idx) {
                    per_pool_errs[j] = errors[j].clone();
                }
            }

            if let Some(pool_err) =
                reduce_write_quorum_errs(&per_pool_errs, BUCKET_OP_IGNORED_ERRS, (per_pool_errs.len() / 2) + 1)
            {
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
            let mut per_pool_errs = vec![None; self.clients.len()];
            for (j, cli) in self.clients.iter().enumerate() {
                let pools = cli.get_pools();
                let idx = i;
                if pools.unwrap_or_default().contains(&idx) {
                    per_pool_errs[j] = errors[j].clone();
                }
            }

            let quorum = per_pool_errs.len() / 2;

            if let Some(pool_err) = reduce_write_quorum_errs(&per_pool_errs, BUCKET_OP_IGNORED_ERRS, quorum) {
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
            let mut per_pool_errs = vec![None; self.clients.len()];
            for (j, cli) in self.clients.iter().enumerate() {
                let pools = cli.get_pools();
                let idx = i;
                if pools.unwrap_or_default().contains(&idx) {
                    per_pool_errs[j] = errors[j].clone();
                }
            }

            if let Some(pool_err) =
                reduce_write_quorum_errs(&per_pool_errs, BUCKET_OP_IGNORED_ERRS, (per_pool_errs.len() / 2) + 1)
            {
                return Err(pool_err);
            }
        }

        ress.into_iter()
            .filter(|op| op.is_some())
            .find_map(|op| op.clone())
            .ok_or(Error::VolumeNotFound)
    }

    pub fn get_pools(&self) -> Option<Vec<usize>> {
        unimplemented!()
    }
}

#[derive(Debug)]
pub struct LocalPeerS3Client {
    // pub local_disks: Vec<DiskStore>,
    // pub node: Node,
    pub pools: Option<Vec<usize>>,
}

impl LocalPeerS3Client {
    pub fn new(_node: Option<Node>, pools: Option<Vec<usize>>) -> Self {
        Self {
            // local_disks,
            // node,
            pools,
        }
    }
}

#[async_trait]
impl PeerS3Client for LocalPeerS3Client {
    fn get_pools(&self) -> Option<Vec<usize>> {
        self.pools.clone()
    }

    async fn heal_bucket(&self, bucket: &str, opts: &HealOpts) -> Result<HealResultItem> {
        heal_bucket_local(bucket, opts).await
    }

    async fn list_bucket(&self, _opts: &BucketOptions) -> Result<Vec<BucketInfo>> {
        let local_disks = all_local_disk().await;

        let mut futures = Vec::with_capacity(local_disks.len());
        for disk in local_disks.iter() {
            futures.push(disk.list_volumes());
        }

        let results = join_all(futures).await;

        let mut ress = Vec::new();
        let mut errs = Vec::new();

        for result in results {
            match result {
                Ok(res) => {
                    ress.push(res);
                    errs.push(None);
                }
                Err(e) => errs.push(Some(e)),
            }
        }

        let mut uniq_map: HashMap<&String, &VolumeInfo> = HashMap::new();

        for info_list in ress.iter() {
            for info in info_list.iter() {
                if is_reserved_or_invalid_bucket(&info.name, false) {
                    continue;
                }
                if !uniq_map.contains_key(&info.name) {
                    uniq_map.insert(&info.name, info);
                }
            }
        }

        let buckets: Vec<BucketInfo> = uniq_map
            .values()
            .map(|&v| BucketInfo {
                name: v.name.clone(),
                created: v.created,
                ..Default::default()
            })
            .collect();

        Ok(buckets)
    }
    async fn make_bucket(&self, bucket: &str, opts: &MakeBucketOptions) -> Result<()> {
        let local_disks = all_local_disk().await;
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
        let local_disks = all_local_disk().await;
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

        // TODO: reduceWriteQuorumErrs
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

    async fn delete_bucket(&self, bucket: &str, _opts: &DeleteBucketOptions) -> Result<()> {
        let local_disks = all_local_disk().await;
        let mut futures = Vec::with_capacity(local_disks.len());

        for disk in local_disks.iter() {
            futures.push(disk.delete_volume(bucket));
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
}

impl RemotePeerS3Client {
    pub fn new(node: Option<Node>, pools: Option<Vec<usize>>) -> Self {
        let addr = node.as_ref().map(|v| v.url.to_string()).unwrap_or_default().to_string();
        Self { node, pools, addr }
    }
    pub fn get_addr(&self) -> String {
        self.addr.clone()
    }
}

#[async_trait]
impl PeerS3Client for RemotePeerS3Client {
    fn get_pools(&self) -> Option<Vec<usize>> {
        self.pools.clone()
    }

    async fn heal_bucket(&self, bucket: &str, opts: &HealOpts) -> Result<HealResultItem> {
        let options: String = serde_json::to_string(opts)?;
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| Error::other(format!("can not get client, err: {err}")))?;
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
    }

    async fn list_bucket(&self, opts: &BucketOptions) -> Result<Vec<BucketInfo>> {
        let options = serde_json::to_string(opts)?;
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| Error::other(format!("can not get client, err: {err}")))?;
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
    }
    async fn make_bucket(&self, bucket: &str, opts: &MakeBucketOptions) -> Result<()> {
        let options = serde_json::to_string(opts)?;
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| Error::other(format!("can not get client, err: {err}")))?;
        let request = Request::new(MakeBucketRequest {
            name: bucket.to_string(),
            options,
        });
        let response = client.make_bucket(request).await?.into_inner();

        // TODO: deal with error
        if !response.success {
            return if let Some(err) = response.error {
                Err(err.into())
            } else {
                Err(Error::other(""))
            };
        }

        Ok(())
    }
    async fn get_bucket_info(&self, bucket: &str, opts: &BucketOptions) -> Result<BucketInfo> {
        let options = serde_json::to_string(opts)?;
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| Error::other(format!("can not get client, err: {err}")))?;
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
    }

    async fn delete_bucket(&self, bucket: &str, _opts: &DeleteBucketOptions) -> Result<()> {
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| Error::other(format!("can not get client, err: {err}")))?;

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
    }
}

pub async fn heal_bucket_local(bucket: &str, opts: &HealOpts) -> Result<HealResultItem> {
    let disks = clone_drives().await;
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
                        info!("will call delete_volume, volume: {}", bucket);
                        let _ = disk.delete_volume(&bucket).await;
                        None
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
                    info!("bucket not find, will recreate");
                    match disk.as_ref().unwrap().make_volume(&bucket).await {
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
    GLOBAL_LOCAL_DISK_MAP.read().await.values().cloned().collect::<Vec<_>>()
}
