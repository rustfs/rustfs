use std::collections::HashMap;

use anyhow::{Error, Result};
use uuid::Uuid;

use crate::{
    disk::{self, DiskError, DiskOption, DiskStore},
    disks_layout::DisksLayout,
    endpoint::EndpointServerPools,
    sets::Sets,
    store_api::{MakeBucketOptions, StorageAPI},
    store_init,
};

#[derive(Debug)]
pub struct ECStore {
    pub id: uuid::Uuid,
    // pub disks: Vec<DiskStore>,
    pub disk_map: HashMap<usize, Vec<Option<DiskStore>>>,
    pub pools: Vec<Sets>,
    pub peer: Vec<String>,
}

impl ECStore {
    pub async fn new(address: String, endpoints: Vec<String>) -> Result<Self> {
        let layouts = DisksLayout::new(&endpoints)?;

        let mut deployment_id = None;

        let (endpoint_pools, _) =
            EndpointServerPools::create_server_endpoints(address, &layouts.pools, layouts.legacy)?;

        let mut pools = Vec::with_capacity(endpoint_pools.len());
        let mut disk_map = HashMap::with_capacity(endpoint_pools.len());

        let first_is_local = endpoint_pools.first_is_local();

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

            disk_map.insert(i, disks);

            let sets = Sets::new(pool_eps, &fm, i, partiy_count)?;

            pools.push(sets);
        }

        Ok(ECStore {
            id: deployment_id.unwrap(),
            disk_map,
            pools,
            peer: Vec::new(),
        })
    }
}

impl StorageAPI for ECStore {
    async fn make_bucket(&self, bucket: &str, opts: &MakeBucketOptions) -> Result<()> {
        // TODO:  check valid bucket name
        unimplemented!()
    }
    async fn put_object(&self, bucket: &str, objcet: &str) -> Result<()> {
        unimplemented!()
    }
}
