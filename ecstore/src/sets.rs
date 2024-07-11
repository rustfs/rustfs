use anyhow::Result;
use uuid::Uuid;

use crate::{
    disk::DiskStore,
    endpoint::PoolEndpoints,
    format::{DistributionAlgoVersion, FormatV3},
    set_disk::SetDisks,
    store_api::{
        BucketInfo, BucketOptions, CompletePart, MakeBucketOptions, MultipartUploadResult, ObjectInfo, ObjectOptions, PartInfo,
        PutObjReader, StorageAPI,
    },
    utils::hash,
};

#[derive(Debug)]
pub struct Sets {
    pub id: Uuid,
    // pub sets: Vec<Objects>,
    // pub disk_set: Vec<Vec<Option<DiskStore>>>, // [set_count_idx][set_drive_count_idx] = disk_idx
    pub disk_set: Vec<SetDisks>, // [set_count_idx][set_drive_count_idx] = disk_idx
    pub pool_idx: usize,
    pub endpoints: PoolEndpoints,
    pub format: FormatV3,
    pub partiy_count: usize,
    pub set_count: usize,
    pub set_drive_count: usize,
    pub distribution_algo: DistributionAlgoVersion,
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

        let mut disk_set = Vec::with_capacity(set_count);

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

            let set_disks = SetDisks {
                disks: set_drive,
                set_drive_count,
                parity_count: partiy_count,
                set_index: i,
                pool_index: pool_idx,
            };

            disk_set.push(set_disks);
        }

        let sets = Self {
            id: fm.id.clone(),
            // sets: todo!(),
            disk_set,
            pool_idx,
            endpoints: endpoints.clone(),
            format: fm.clone(),
            partiy_count,
            set_count,
            set_drive_count,
            distribution_algo: fm.erasure.distribution_algo.clone(),
        };

        Ok(sets)
    }
    pub fn get_disks(&self, set_idx: usize) -> SetDisks {
        self.disk_set[set_idx].clone()
    }

    pub fn get_disks_by_key(&self, key: &str) -> SetDisks {
        self.get_disks(self.get_hashed_set_index(key))
    }

    fn get_hashed_set_index(&self, input: &str) -> usize {
        match self.distribution_algo {
            DistributionAlgoVersion::V1 => hash::crc_hash(input, self.disk_set.len()),

            DistributionAlgoVersion::V2 | DistributionAlgoVersion::V3 => {
                hash::sip_hash(input, self.disk_set.len(), self.id.as_bytes())
            }
        }
    }

    // async fn commit_rename_data_dir(
    //     &self,
    //     disks: &Vec<Option<DiskStore>>,
    //     bucket: &str,
    //     object: &str,
    //     data_dir: &str,
    //     // write_quorum: usize,
    // ) -> Vec<Option<Error>> {
    //     unimplemented!()
    // }
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
    async fn make_bucket(&self, _bucket: &str, _opts: &MakeBucketOptions) -> Result<()> {
        unimplemented!()
    }

    async fn get_bucket_info(&self, _bucket: &str, _opts: &BucketOptions) -> Result<BucketInfo> {
        unimplemented!()
    }

    async fn put_object(&self, bucket: &str, object: &str, data: PutObjReader, opts: &ObjectOptions) -> Result<()> {
        self.get_disks_by_key(object).put_object(bucket, object, data, opts).await
    }

    async fn put_object_part(
        &self,
        bucket: &str,
        object: &str,
        upload_id: &str,
        part_id: usize,
        data: PutObjReader,
        opts: &ObjectOptions,
    ) -> Result<PartInfo> {
        self.get_disks_by_key(object)
            .put_object_part(bucket, object, upload_id, part_id, data, opts)
            .await
    }

    async fn new_multipart_upload(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<MultipartUploadResult> {
        self.get_disks_by_key(object).new_multipart_upload(bucket, object, opts).await
    }

    async fn complete_multipart_upload(
        &self,
        bucket: &str,
        object: &str,
        upload_id: &str,
        uploaded_parts: Vec<CompletePart>,
        opts: &ObjectOptions,
    ) -> Result<ObjectInfo> {
        self.get_disks_by_key(object)
            .complete_multipart_upload(bucket, object, upload_id, uploaded_parts, opts)
            .await
    }
}
