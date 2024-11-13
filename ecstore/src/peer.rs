use async_trait::async_trait;
use futures::future::join_all;
use protos::node_service_time_out_client;
use protos::proto_gen::node_service::{DeleteBucketRequest, GetBucketInfoRequest, ListBucketRequest, MakeBucketRequest};
use regex::Regex;
use std::{collections::HashMap, fmt::Debug, sync::Arc};
use tonic::Request;
use tracing::warn;

use crate::disk::DiskAPI;
use crate::store::all_local_disk;
use crate::{
    disk::{self, error::DiskError, VolumeInfo},
    endpoints::{EndpointServerPools, Node},
    error::{Error, Result},
    store_api::{BucketInfo, BucketOptions, DeleteBucketOptions, MakeBucketOptions},
};

type Client = Arc<Box<dyn PeerS3Client>>;

#[async_trait]
pub trait PeerS3Client: Debug + Sync + Send + 'static {
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
    pub async fn make_bucket(&self, bucket: &str, opts: &MakeBucketOptions) -> Result<()> {
        let mut futures = Vec::with_capacity(self.clients.len());
        for cli in self.clients.iter() {
            futures.push(cli.make_bucket(bucket, opts));
        }

        let mut errors = Vec::with_capacity(self.clients.len());

        let results = join_all(futures).await;
        for result in results {
            match result {
                Ok(_) => {
                    errors.push(None);
                }
                Err(e) => {
                    errors.push(Some(e));
                }
            }
        }

        for i in 0..self.pools_count {
            let mut per_pool_errs = Vec::with_capacity(self.clients.len());
            for (j, cli) in self.clients.iter().enumerate() {
                let pools = cli.get_pools();
                let idx = i;
                if pools.unwrap_or_default().contains(&idx) {
                    per_pool_errs.push(errors[j].as_ref());
                }

                // TODO: reduceWriteQuorumErrs
            }
        }

        // TODO:

        Ok(())
    }
    pub async fn list_bucket(&self, opts: &BucketOptions) -> Result<Vec<BucketInfo>> {
        let mut futures = Vec::with_capacity(self.clients.len());
        for cli in self.clients.iter() {
            futures.push(cli.list_bucket(opts));
        }

        let mut errors = Vec::with_capacity(self.clients.len());
        let mut ress = Vec::with_capacity(self.clients.len());

        let results = join_all(futures).await;
        for result in results {
            match result {
                Ok(res) => {
                    ress.push(Some(res));
                    errors.push(None);
                }
                Err(e) => {
                    ress.push(None);
                    errors.push(Some(e));
                }
            }
        }
        // TODO: reduceWriteQuorumErrs
        // for i in 0..self.pools_count {}

        let mut uniq_map: HashMap<&String, &BucketInfo> = HashMap::new();

        for res in ress.iter() {
            if res.is_none() {
                continue;
            }

            let buckets = res.as_ref().unwrap();

            for bucket in buckets.iter() {
                if !uniq_map.contains_key(&bucket.name) {
                    uniq_map.insert(&bucket.name, bucket);
                }
            }
        }

        let buckets: Vec<BucketInfo> = uniq_map.values().map(|&v| v.clone()).collect();

        Ok(buckets)
    }
    pub async fn delete_bucket(&self, bucket: &str, opts: &DeleteBucketOptions) -> Result<()> {
        let mut futures = Vec::with_capacity(self.clients.len());
        for cli in self.clients.iter() {
            futures.push(cli.delete_bucket(bucket, opts));
        }

        let mut errors = Vec::with_capacity(self.clients.len());

        let results = join_all(futures).await;

        for result in results {
            match result {
                Ok(_) => {
                    errors.push(None);
                }
                Err(e) => {
                    errors.push(Some(e));
                }
            }
        }

        // TODO: reduceWriteQuorumErrs

        Ok(())
    }
    pub async fn get_bucket_info(&self, bucket: &str, opts: &BucketOptions) -> Result<BucketInfo> {
        let mut futures = Vec::with_capacity(self.clients.len());
        for cli in self.clients.iter() {
            futures.push(cli.get_bucket_info(bucket, opts));
        }

        let mut ress = Vec::with_capacity(self.clients.len());
        let mut errors = Vec::with_capacity(self.clients.len());

        let results = join_all(futures).await;
        for result in results {
            match result {
                Ok(res) => {
                    ress.push(Some(res));
                    errors.push(None);
                }
                Err(e) => {
                    ress.push(None);
                    errors.push(Some(e));
                }
            }
        }

        for i in 0..self.pools_count {
            let mut per_pool_errs = Vec::with_capacity(self.clients.len());
            for (j, cli) in self.clients.iter().enumerate() {
                let pools = cli.get_pools();
                let idx = i;
                if pools.unwrap_or_default().contains(&idx) {
                    per_pool_errs.push(errors[j].as_ref());
                }

                // TODO: reduceWriteQuorumErrs
            }
        }

        ress.iter()
            .find_map(|op| op.clone())
            .ok_or(Error::new(DiskError::VolumeNotFound))
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
                        if opts.force_create && DiskError::VolumeExists.is(&e) {
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

        // TODO: reduceWriteQuorumErrs

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

        // debug!("get_bucket_info errs:{:?}", errs);

        ress.iter()
            .find_map(|op| {
                op.as_ref().map(|v| BucketInfo {
                    name: v.name.clone(),
                    created: v.created,
                    ..Default::default()
                })
            })
            .ok_or(Error::new(DiskError::VolumeNotFound))
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
                    if DiskError::VolumeNotEmpty.is(&e) {
                        recreate = true;
                    }
                    errs.push(Some(e))
                }
            }
        }

        // errVolumeNotEmpty 不删除，把已经删除的重新创建

        for (idx, err) in errs.into_iter().enumerate() {
            if err.is_none() && recreate {
                let _ = local_disks[idx].make_volume(bucket).await;
            }
        }

        if recreate {
            return Err(Error::new(DiskError::VolumeNotEmpty));
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
    fn new(node: Option<Node>, pools: Option<Vec<usize>>) -> Self {
        let addr = node.as_ref().map(|v| v.url.to_string()).unwrap_or_default().to_string();
        Self { node, pools, addr }
    }
}

#[async_trait]
impl PeerS3Client for RemotePeerS3Client {
    fn get_pools(&self) -> Option<Vec<usize>> {
        self.pools.clone()
    }
    async fn list_bucket(&self, opts: &BucketOptions) -> Result<Vec<BucketInfo>> {
        let options = serde_json::to_string(opts)?;
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| Error::from_string(format!("can not get client, err: {}", err)))?;
        let request = Request::new(ListBucketRequest { options });
        let response = client.list_bucket(request).await?.into_inner();
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
            .map_err(|err| Error::from_string(format!("can not get client, err: {}", err)))?;
        let request = Request::new(MakeBucketRequest {
            name: bucket.to_string(),
            options,
        });
        let response = client.make_bucket(request).await?.into_inner();

        // TODO: deal with error
        if !response.success {
            warn!("make bucket error: {:?}", response.error_info);
        }

        Ok(())
    }
    async fn get_bucket_info(&self, bucket: &str, opts: &BucketOptions) -> Result<BucketInfo> {
        let options = serde_json::to_string(opts)?;
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| Error::from_string(format!("can not get client, err: {}", err)))?;
        let request = Request::new(GetBucketInfoRequest {
            bucket: bucket.to_string(),
            options,
        });
        let response = client.get_bucket_info(request).await?.into_inner();
        let bucket_info = serde_json::from_str::<BucketInfo>(&response.bucket_info)?;

        Ok(bucket_info)
    }

    async fn delete_bucket(&self, bucket: &str, _opts: &DeleteBucketOptions) -> Result<()> {
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| Error::from_string(format!("can not get client, err: {}", err)))?;

        let request = Request::new(DeleteBucketRequest {
            bucket: bucket.to_string(),
        });
        let _response = client.delete_bucket(request).await?.into_inner();

        Ok(())
    }
}

// 检查桶名是否有效
fn check_bucket_name(bucket_name: &str, strict: bool) -> Result<()> {
    if bucket_name.trim().is_empty() {
        return Err(Error::msg("Bucket name cannot be empty"));
    }
    if bucket_name.len() < 3 {
        return Err(Error::msg("Bucket name cannot be shorter than 3 characters"));
    }
    if bucket_name.len() > 63 {
        return Err(Error::msg("Bucket name cannot be longer than 63 characters"));
    }

    let ip_address_regex = Regex::new(r"^(\d+\.){3}\d+$").unwrap();
    if ip_address_regex.is_match(bucket_name) {
        return Err(Error::msg("Bucket name cannot be an IP address"));
    }

    let valid_bucket_name_regex = if strict {
        Regex::new(r"^[a-z0-9][a-z0-9\.\-]{1,61}[a-z0-9]$").unwrap()
    } else {
        Regex::new(r"^[A-Za-z0-9][A-Za-z0-9\.\-_:]{1,61}[A-Za-z0-9]$").unwrap()
    };

    if !valid_bucket_name_regex.is_match(bucket_name) {
        return Err(Error::msg("Bucket name contains invalid characters"));
    }

    // 检查包含 "..", ".-", "-."
    if bucket_name.contains("..") || bucket_name.contains(".-") || bucket_name.contains("-.") {
        return Err(Error::msg("Bucket name contains invalid characters"));
    }

    Ok(())
}

// 检查是否为  元数据桶
fn is_meta_bucket(bucket_name: &str) -> bool {
    bucket_name == disk::RUSTFS_META_BUCKET
}

// 检查是否为 保留桶
fn is_reserved_bucket(bucket_name: &str) -> bool {
    bucket_name == "rustfs"
}

// 检查桶名是否为保留名或无效名
pub fn is_reserved_or_invalid_bucket(bucket_entry: &str, strict: bool) -> bool {
    if bucket_entry.is_empty() {
        return true;
    }

    let bucket_entry = bucket_entry.trim_end_matches('/');
    let result = check_bucket_name(bucket_entry, strict).is_err();

    result || is_meta_bucket(bucket_entry) || is_reserved_bucket(bucket_entry)
}
