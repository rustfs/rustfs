use std::collections::HashMap;

use anyhow::{Error, Result};

use s3s::{dto::StreamingBlob, Body};
use uuid::Uuid;

use crate::{
    bucket_meta::BucketMetadata,
    disk::{self, DiskError, DiskOption, DiskStore, RUSTFS_META_BUCKET},
    disks_layout::DisksLayout,
    endpoint::EndpointServerPools,
    peer::{PeerS3Client, S3PeerSys},
    sets::Sets,
    store_api::{MakeBucketOptions, ObjectOptions, PutObjReader, StorageAPI},
    store_init, utils,
};

#[derive(Debug)]
pub struct ECStore {
    pub id: uuid::Uuid,
    // pub disks: Vec<DiskStore>,
    pub disk_map: HashMap<usize, Vec<Option<DiskStore>>>,
    pub pools: Vec<Sets>,
    pub peer_sys: S3PeerSys,
    pub local_disks: Vec<DiskStore>,
}

impl ECStore {
    pub async fn new(address: String, endpoints: Vec<String>) -> Result<Self> {
        let layouts = DisksLayout::new(&endpoints)?;

        let mut deployment_id = None;

        let (endpoint_pools, _) = EndpointServerPools::create_server_endpoints(address, &layouts.pools, layouts.legacy)?;

        let mut pools = Vec::with_capacity(endpoint_pools.len());
        let mut disk_map = HashMap::with_capacity(endpoint_pools.len());

        let first_is_local = endpoint_pools.first_is_local();

        let mut local_disks = Vec::new();

        for (i, pool_eps) in endpoint_pools.iter().enumerate() {
            // TODO: read from config parseStorageClass
            let partiy_count = store_init::default_partiy_count(pool_eps.drives_per_set);

            let (disks, errs) = disk::init_disks(
                &pool_eps.endpoints,
                &DiskOption {
                    cleanup: true,
                    health_check: true,
                },
            )
            .await;

            DiskError::check_disk_fatal_errs(&errs)?;

            let fm = store_init::do_init_format_file(
                first_is_local,
                &disks,
                pool_eps.set_count,
                pool_eps.drives_per_set,
                deployment_id,
            )
            .await?;

            if deployment_id.is_none() {
                deployment_id = Some(fm.id.clone());
            }

            if deployment_id != Some(fm.id) {
                return Err(Error::msg("deployment_id not same in one pool"));
            }

            if deployment_id.is_some() && deployment_id.unwrap().is_nil() {
                deployment_id = Some(Uuid::new_v4());
            }

            for disk in disks.iter() {
                if disk.is_some() && disk.as_ref().unwrap().is_local() {
                    local_disks.push(disk.as_ref().unwrap().clone());
                }
            }

            let sets = Sets::new(disks.clone(), pool_eps, &fm, i, partiy_count)?;

            pools.push(sets);

            disk_map.insert(i, disks);
        }

        let peer_sys = S3PeerSys::new(&endpoint_pools, local_disks.clone());

        Ok(ECStore {
            id: deployment_id.unwrap(),
            disk_map,
            pools,
            local_disks,
            peer_sys,
        })
    }

    fn single_pool(&self) -> bool {
        self.pools.len() == 1
    }
}

#[async_trait::async_trait]
impl StorageAPI for ECStore {
    async fn make_bucket(&self, bucket: &str, opts: &MakeBucketOptions) -> Result<()> {
        // TODO:  check valid bucket name

        // TODO: delete created bucket when error
        self.peer_sys.make_bucket(bucket, opts).await?;

        let meta = BucketMetadata::new(bucket);
        let data = meta.marshal_msg()?;
        let file_path = meta.save_file_path();

        // TODO: wrap hash reader

        let content_len = data.len();

        let body = Body::from(data);

        let reader = PutObjReader::new(StreamingBlob::from(body), content_len);

        self.put_object(RUSTFS_META_BUCKET, &file_path, reader, ObjectOptions { max_parity: true })
            .await?;

        // TODO: toObjectErr

        Ok(())
    }
    async fn put_object(&self, bucket: &str, object: &str, data: PutObjReader, opts: ObjectOptions) -> Result<()> {
        // checkPutObjectArgs

        let object = utils::path::encode_dir_object(object);

        if self.single_pool() {
            self.pools[0].put_object(bucket, object.as_str(), data, opts).await?;
            return Ok(());
        }

        unimplemented!()
    }
}
