use async_trait::async_trait;
use futures::future::join_all;
use protos::proto_gen::node_service::MakeBucketRequest;
use protos::{
    node_service_time_out_client, proto_gen::node_service::MakeBucketOptions as proto_MakeBucketOptions,
    DEFAULT_GRPC_SERVER_MESSAGE_LEN,
};
use regex::Regex;
use std::{collections::HashMap, fmt::Debug, sync::Arc, time::Duration};
use tonic::transport::{Channel, Endpoint};
use tonic::Request;
use tracing::warn;

use crate::{
    disk::{self, error::DiskError, DiskStore, VolumeInfo},
    endpoints::{EndpointServerPools, Node},
    error::{Error, Result},
    store_api::{BucketInfo, BucketOptions, MakeBucketOptions},
};

type Client = Arc<Box<dyn PeerS3Client>>;

#[async_trait]
pub trait PeerS3Client: Debug + Sync + Send + 'static {
    async fn make_bucket(&self, bucket: &str, opts: &MakeBucketOptions) -> Result<()>;
    async fn list_bucket(&self, opts: &BucketOptions) -> Result<Vec<BucketInfo>>;
    async fn delete_bucket(&self, bucket: &str) -> Result<()>;
    async fn get_bucket_info(&self, bucket: &str, opts: &BucketOptions) -> Result<BucketInfo>;
    fn get_pools(&self) -> Vec<usize>;
}

#[derive(Debug)]
pub struct S3PeerSys {
    pub clients: Vec<Client>,
    pub pools_count: usize,
}

impl S3PeerSys {
    pub fn new(eps: &EndpointServerPools, local_disks: Vec<DiskStore>) -> Self {
        Self {
            clients: Self::new_clients(eps, local_disks),
            pools_count: eps.as_ref().len(),
        }
    }

    fn new_clients(eps: &EndpointServerPools, local_disks: Vec<DiskStore>) -> Vec<Client> {
        let nodes = eps.get_nodes();
        let v: Vec<Client> = nodes
            .iter()
            .map(|e| {
                if e.is_local {
                    let cli: Box<dyn PeerS3Client> =
                        Box::new(LocalPeerS3Client::new(local_disks.clone(), e.clone(), e.pools.clone()));
                    Arc::new(cli)
                } else {
                    let cli: Box<dyn PeerS3Client> = Box::new(RemotePeerS3Client::new(e.clone(), e.pools.clone()));
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
                if pools.contains(&idx) {
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
    pub async fn delete_bucket(&self, bucket: &str) -> Result<()> {
        let mut futures = Vec::with_capacity(self.clients.len());
        for cli in self.clients.iter() {
            futures.push(cli.delete_bucket(bucket));
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
                if pools.contains(&idx) {
                    per_pool_errs.push(errors[j].as_ref());
                }

                // TODO: reduceWriteQuorumErrs
            }
        }

        ress.iter()
            .find_map(|op| op.clone())
            .ok_or(Error::new(DiskError::VolumeNotFound))
    }

    pub fn get_pools(&self) -> Vec<usize> {
        unimplemented!()
    }
}

#[derive(Debug)]
pub struct LocalPeerS3Client {
    pub local_disks: Vec<DiskStore>,
    // pub node: Node,
    pub pools: Vec<usize>,
}

impl LocalPeerS3Client {
    fn new(local_disks: Vec<DiskStore>, _node: Node, pools: Vec<usize>) -> Self {
        Self {
            local_disks,
            // node,
            pools,
        }
    }
}

#[async_trait]
impl PeerS3Client for LocalPeerS3Client {
    fn get_pools(&self) -> Vec<usize> {
        self.pools.clone()
    }
    async fn list_bucket(&self, _opts: &BucketOptions) -> Result<Vec<BucketInfo>> {
        let mut futures = Vec::with_capacity(self.local_disks.len());
        for disk in self.local_disks.iter() {
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

        warn!("list_bucket errs {:?}", &errs);

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
            })
            .collect();

        Ok(buckets)
    }
    async fn make_bucket(&self, bucket: &str, opts: &MakeBucketOptions) -> Result<()> {
        let mut futures = Vec::with_capacity(self.local_disks.len());
        for disk in self.local_disks.iter() {
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
        let mut futures = Vec::with_capacity(self.local_disks.len());
        for disk in self.local_disks.iter() {
            futures.push(disk.stat_volume(bucket));
        }

        let results = join_all(futures).await;

        let mut ress = Vec::with_capacity(self.local_disks.len());
        let mut errs = Vec::with_capacity(self.local_disks.len());

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
                })
            })
            .ok_or(Error::new(DiskError::VolumeNotFound))
    }

    async fn delete_bucket(&self, bucket: &str) -> Result<()> {
        let mut futures = Vec::with_capacity(self.local_disks.len());

        for disk in self.local_disks.iter() {
            futures.push(disk.delete_volume(bucket));
        }

        let results = join_all(futures).await;

        let mut errs = Vec::new();

        for res in results {
            match res {
                Ok(_) => errs.push(None),
                Err(e) => errs.push(Some(e)),
            }
        }

        // TODO: errVolumeNotEmpty 不删除，把已经删除的重新创建

        // TODO: reduceWriteQuorumErrs

        Ok(())
    }
}

#[derive(Debug)]
pub struct RemotePeerS3Client {
    pub _node: Node,
    pub pools: Vec<usize>,
    pub channel: Channel,
}

impl RemotePeerS3Client {
    fn new(node: Node, pools: Vec<usize>) -> Self {
        let connector = Endpoint::from_shared(format!("{}", node.url.clone().as_str())).unwrap();
        let channel = tokio::runtime::Runtime::new().unwrap().block_on(connector.connect()).unwrap();
        Self {
            _node: node,
            pools,
            channel,
        }
    }
}

#[async_trait]
impl PeerS3Client for RemotePeerS3Client {
    fn get_pools(&self) -> Vec<usize> {
        self.pools.clone()
    }
    async fn list_bucket(&self, _opts: &BucketOptions) -> Result<Vec<BucketInfo>> {
        unimplemented!()
    }
    async fn make_bucket(&self, bucket: &str, opts: &MakeBucketOptions) -> Result<()> {
        let mut client = node_service_time_out_client(
            self.channel.clone(),
            Duration::new(30, 0), // TODO: use config setting
            DEFAULT_GRPC_SERVER_MESSAGE_LEN,
            // grpc_enable_gzip,
            false, // TODO: use config setting
        );
        let request = Request::new(MakeBucketRequest {
            name: bucket.to_string(),
            options: Some(proto_MakeBucketOptions {
                force_create: opts.force_create,
            }),
        });
        let _response = client.make_bucket(request).await?.into_inner();

        // TODO: deal with error

        Ok(())
    }
    async fn get_bucket_info(&self, _bucket: &str, _opts: &BucketOptions) -> Result<BucketInfo> {
        unimplemented!()
    }

    async fn delete_bucket(&self, _bucket: &str) -> Result<()> {
        unimplemented!()
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
fn is_reserved_or_invalid_bucket(bucket_entry: &str, strict: bool) -> bool {
    if bucket_entry.is_empty() {
        return true;
    }

    let bucket_entry = bucket_entry.trim_end_matches('/');
    let result = check_bucket_name(bucket_entry, strict).is_err();

    result || is_meta_bucket(bucket_entry) || is_reserved_bucket(bucket_entry)
}
