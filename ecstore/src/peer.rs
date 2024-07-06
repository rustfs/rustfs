use anyhow::Result;
use async_trait::async_trait;
use futures::future::join_all;
use std::{fmt::Debug, sync::Arc};

use crate::{
    disk::{DiskError, DiskStore},
    endpoint::{EndpointServerPools, Node},
    store_api::MakeBucketOptions,
};

type Client = Arc<Box<dyn PeerS3Client>>;

#[async_trait]
pub trait PeerS3Client: Debug + Sync + Send + 'static {
    async fn make_bucket(&self, bucket: &str, opts: &MakeBucketOptions) -> Result<()>;
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

#[async_trait]
impl PeerS3Client for S3PeerSys {
    fn get_pools(&self) -> Vec<usize> {
        unimplemented!()
    }
    async fn make_bucket(&self, bucket: &str, opts: &MakeBucketOptions) -> Result<()> {
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
}

#[derive(Debug)]
pub struct LocalPeerS3Client {
    pub local_disks: Vec<DiskStore>,
    pub node: Node,
    pub pools: Vec<usize>,
}

impl LocalPeerS3Client {
    fn new(local_disks: Vec<DiskStore>, node: Node, pools: Vec<usize>) -> Self {
        Self {
            local_disks,
            node,
            pools,
        }
    }
}

#[async_trait]
impl PeerS3Client for LocalPeerS3Client {
    fn get_pools(&self) -> Vec<usize> {
        self.pools.clone()
    }
    async fn make_bucket(&self, bucket: &str, opts: &MakeBucketOptions) -> Result<()> {
        let mut futures = Vec::with_capacity(self.local_disks.len());
        for disk in self.local_disks.iter() {
            futures.push(async move {
                match disk.make_volume(bucket).await {
                    Ok(_) => Ok(()),
                    Err(e) => {
                        if opts.force_create && DiskError::is_err(&e, &DiskError::VolumeExists) {
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
}

#[derive(Debug)]
pub struct RemotePeerS3Client {
    pub node: Node,
    pub pools: Vec<usize>,
}

impl RemotePeerS3Client {
    fn new(node: Node, pools: Vec<usize>) -> Self {
        Self { node, pools }
    }
}

#[async_trait]
impl PeerS3Client for RemotePeerS3Client {
    fn get_pools(&self) -> Vec<usize> {
        unimplemented!()
    }
    async fn make_bucket(&self, _bucket: &str, _opts: &MakeBucketOptions) -> Result<()> {
        unimplemented!()
    }
}
