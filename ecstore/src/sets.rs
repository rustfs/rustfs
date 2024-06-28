use anyhow::Result;
use futures::StreamExt;
use uuid::Uuid;

use crate::{
    disk::DiskStore,
    endpoint::PoolEndpoints,
    format::FormatV3,
    store_api::{MakeBucketOptions, ObjectOptions, PutObjReader, StorageAPI},
};

#[derive(Debug)]
pub struct Sets {
    pub id: Uuid,
    // pub sets: Vec<Objects>,
    pub disk_indexs: Vec<Vec<Option<DiskStore>>>, // [set_count_idx][set_drive_count_idx] = disk_idx
    pub pool_idx: usize,
    pub endpoints: PoolEndpoints,
    pub format: FormatV3,
    pub partiy_count: usize,
    pub set_count: usize,
    pub set_drive_count: usize,
}

impl Sets {
    pub fn new(
        disks: Vec<Option<DiskStore>>,
        endpoints: &PoolEndpoints,
        fm: &FormatV3,
        pool_idx: usize,
        partiy_count: usize,
    ) -> Result<Self> {
        let set_count = fm.erasure.sets.len();
        let set_drive_count = fm.erasure.sets[0].len();

        let mut disk_indexs = Vec::with_capacity(set_count);

        for i in 0..set_count {
            let mut set_drive = Vec::with_capacity(set_drive_count);
            for j in 0..set_drive_count {
                let idx = i * set_drive_count + j;
                if disks[idx].is_none() {
                    set_drive.push(None);
                } else {
                    let disk = disks[idx].clone();
                    set_drive.push(disk);
                }
            }

            disk_indexs.push(set_drive);
        }

        let sets = Self {
            id: fm.id.clone(),
            // sets: todo!(),
            disk_indexs,
            pool_idx,
            endpoints: endpoints.clone(),
            format: fm.clone(),
            partiy_count,
            set_count,
            set_drive_count,
        };

        Ok(sets)
    }
    pub fn get_disks(&self, set_idx: usize) -> Vec<Option<DiskStore>> {
        self.disk_indexs[set_idx].clone()
    }
}

// #[derive(Debug)]
// pub struct Objects {
//     pub endpoints: Vec<Endpoint>,
//     pub disks: Vec<usize>,
//     pub set_index: usize,
//     pub pool_index: usize,
//     pub set_drive_count: usize,
//     pub default_parity_count: usize,
// }

#[async_trait::async_trait]
impl StorageAPI for Sets {
    async fn make_bucket(&self, bucket: &str, opts: &MakeBucketOptions) -> Result<()> {
        unimplemented!()
    }

    async fn put_object(
        &self,
        bucket: &str,
        object: &str,
        data: &PutObjReader,
        opts: &ObjectOptions,
    ) -> Result<()> {
        // data.stream.next();
        unimplemented!()
    }
}
