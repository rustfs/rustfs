use std::sync::Arc;

use anyhow::Result;

use futures::{AsyncWrite, StreamExt};
use time::OffsetDateTime;
use tracing::debug;
use uuid::Uuid;

use crate::{
    disk::{self, DiskStore},
    endpoint::PoolEndpoints,
    erasure::Erasure,
    format::{DistributionAlgoVersion, FormatV3},
    store_api::{FileInfo, MakeBucketOptions, ObjectOptions, PutObjReader, StorageAPI},
    utils::hash,
};

#[derive(Debug)]
pub struct Sets {
    pub id: Uuid,
    // pub sets: Vec<Objects>,
    pub disk_set: Vec<Vec<Option<DiskStore>>>, // [set_count_idx][set_drive_count_idx] = disk_idx
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

            disk_set.push(set_drive);
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
    pub fn get_disks(&self, set_idx: usize) -> Vec<Option<DiskStore>> {
        self.disk_set[set_idx].clone()
    }

    pub fn get_disks_by_key(&self, key: &str) -> Vec<Option<DiskStore>> {
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

    async fn put_object(&self, bucket: &str, object: &str, data: PutObjReader, opts: ObjectOptions) -> Result<()> {
        let disks = self.get_disks_by_key(object);

        let mut parity_drives = self.partiy_count;
        if opts.max_parity {
            parity_drives = disks.len() / 2;
        }

        let data_drives = disks.len() - parity_drives;
        let mut write_quorum = data_drives;
        if data_drives == parity_drives {
            write_quorum += 1
        }

        let mut fi = FileInfo::new([bucket, object].join("/").as_str(), data_drives, parity_drives);

        fi.data_dir = Uuid::new_v4().to_string();

        let parts_metadata = vec![fi.clone(); disks.len()];

        let (shuffle_disks, shuffle_parts_metadata) = shuffle_disks_and_parts_metadata(&disks, &parts_metadata, &fi);

        let mut writers = Vec::with_capacity(disks.len());

        for disk in shuffle_disks.iter() {
            let (reader, writer) = tokio::io::duplex(fi.erasure.block_size);

            let disk = disk.as_ref().unwrap().clone();
            let bucket = bucket.to_string();
            let object = object.to_string();
            tokio::spawn(async move {
                debug!("do createfile");
                match disk
                    .CreateFile("", bucket.as_str(), object.as_str(), data.content_length, reader)
                    .await
                {
                    Ok(_) => (),
                    Err(e) => debug!("creatfile err :{:?}", e),
                }
            });

            writers.push(writer);
        }

        let erasure = Erasure::new(fi.erasure.data_blocks, fi.erasure.parity_blocks);

        erasure
            .encode(data.stream, &mut writers, fi.erasure.block_size, data.content_length, write_quorum)
            .await?;

        unimplemented!()
    }
}

// 打乱顺序
fn shuffle_disks_and_parts_metadata(
    disks: &Vec<Option<DiskStore>>,
    parts_metadata: &Vec<FileInfo>,
    fi: &FileInfo,
) -> (Vec<Option<DiskStore>>, Vec<FileInfo>) {
    let init = fi.mod_time == OffsetDateTime::UNIX_EPOCH;

    let mut shuffled_disks = vec![None; disks.len()];
    let mut shuffled_parts_metadata = vec![FileInfo::default(); parts_metadata.len()];
    let distribution = &fi.erasure.distribution;

    for (k, v) in disks.iter().enumerate() {
        if v.is_none() {
            continue;
        }

        if !init && !parts_metadata[k].is_valid() {
            continue;
        }

        // if !init && fi.xlv1 != parts_metadata[k].xlv1 {
        //     continue;
        // }

        let block_idx = distribution[k];
        shuffled_parts_metadata[block_idx - 1] = parts_metadata[k].clone();
        shuffled_disks[block_idx - 1] = disks[k].clone();
    }

    (shuffled_disks, shuffled_parts_metadata)
}
