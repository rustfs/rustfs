use anyhow::{Error, Result};
use futures::future::join_all;
use time::OffsetDateTime;
use tracing::debug;
use uuid::Uuid;

use crate::{
    disk::{DiskStore, RUSTFS_META_MULTIPART_BUCKET, RUSTFS_META_TMP_BUCKET},
    endpoint::PoolEndpoints,
    erasure::Erasure,
    format::{DistributionAlgoVersion, FormatV3},
    store_api::{
        BucketInfo, BucketOptions, FileInfo, MakeBucketOptions, MultipartUploadResult, ObjectOptions, PartInfo, PutObjReader,
        StorageAPI,
    },
    utils::{
        crypto::{base64_decode, base64_encode, hex, sha256},
        hash,
    },
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

    async fn rename_data(
        &self,
        disks: &Vec<Option<DiskStore>>,
        src_bucket: &str,
        src_object: &str,
        file_infos: &Vec<FileInfo>,
        dst_bucket: &str,
        dst_object: &str,
        // write_quorum: usize,
    ) -> Vec<Option<Error>> {
        let mut futures = Vec::with_capacity(disks.len());

        for (i, disk) in disks.iter().enumerate() {
            let disk = disk.as_ref().unwrap();
            let file_info = &file_infos[i];
            futures.push(async move {
                disk.rename_data(src_bucket, src_object, file_info, dst_bucket, dst_object)
                    .await
            })
        }

        let mut errors = Vec::with_capacity(disks.len());

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
        errors
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

async fn write_unique_file_info(
    disks: &Vec<Option<DiskStore>>,
    org_bucket: &str,
    bucket: &str,
    prefix: &str,
    files: &Vec<FileInfo>,
    // write_quorum: usize,
) -> Vec<Option<Error>> {
    let mut futures = Vec::with_capacity(disks.len());

    for (i, disk) in disks.iter().enumerate() {
        let disk = disk.as_ref().unwrap();
        let mut file_info = files[i].clone();
        file_info.erasure.index = i + 1;
        futures.push(async move { disk.write_metadata(org_bucket, bucket, prefix, file_info).await })
    }

    let mut errors = Vec::with_capacity(disks.len());

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
    errors
}

fn get_upload_id_dir(bucket: &str, object: &str, upload_id: &str) -> String {
    let upload_uuid = match base64_decode(upload_id.as_bytes()) {
        Ok(res) => {
            let decoded_str = String::from_utf8(res).expect("Failed to convert decoded bytes to a UTF-8 string");
            let parts: Vec<&str> = decoded_str.splitn(2, '.').collect();
            if parts.len() == 2 {
                parts[1].to_string()
            } else {
                upload_id.to_string()
            }
        }
        Err(_) => upload_id.to_string(),
    };

    format!("{}/{}", get_multipart_sha_dir(bucket, object), upload_uuid)
}

fn get_multipart_sha_dir(bucket: &str, object: &str) -> String {
    let path = format!("{}/{}", bucket, object);
    hex(sha256(path.as_bytes()).as_ref())
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

        fi.data_dir = Uuid::new_v4();

        let parts_metadata = vec![fi.clone(); disks.len()];

        let (shuffle_disks, mut shuffle_parts_metadata) = shuffle_disks_and_parts_metadata(&disks, &parts_metadata, &fi);

        let mut writers = Vec::with_capacity(disks.len());

        let mut futures = Vec::with_capacity(disks.len());

        let tmp_dir = Uuid::new_v4().to_string();

        let tmp_object = format!("{}/{}/part.1", tmp_dir, fi.data_dir);

        for disk in shuffle_disks.iter() {
            let (reader, writer) = tokio::io::duplex(fi.erasure.block_size);

            let disk = disk.as_ref().unwrap().clone();
            let tmp_object = tmp_object.clone();

            // TODO: save small file in fileinfo.data instead of write file;

            futures.push(async move {
                disk.create_file("", RUSTFS_META_TMP_BUCKET, tmp_object.as_str(), data.content_length, reader)
                    .await
            });
            // futures.push(tokio::spawn(async move {
            //     debug!("do createfile");
            //     disk.CreateFile("", bucket.as_str(), object.as_str(), data.content_length, reader)
            //         .await;
            // }));

            writers.push(writer);
        }

        let erasure = Erasure::new(fi.erasure.data_blocks, fi.erasure.parity_blocks);

        let w_size = erasure
            .encode(data.stream, &mut writers, fi.erasure.block_size, data.content_length, write_quorum)
            .await?;

        // close reader in create_file
        drop(writers);

        let mut errors = Vec::with_capacity(disks.len());

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

        debug!("CreateFile errs:{:?}", errors);

        // TODO: reduceWriteQuorumErrs
        // evalDisks

        for fi in shuffle_parts_metadata.iter_mut() {
            fi.mod_time = OffsetDateTime::now_utc();
            fi.size = w_size;
        }

        let rename_errs = self
            .rename_data(
                &shuffle_disks,
                RUSTFS_META_TMP_BUCKET,
                tmp_dir.as_str(),
                &shuffle_parts_metadata,
                &bucket,
                &object,
            )
            .await;

        // TODO: reduceWriteQuorumErrs

        debug!("put_object rename_errs:{:?}", rename_errs);

        // self.commit_rename_data_dir(&shuffle_disks,&bucket,&object,)

        Ok(())
    }

    async fn put_object_part(
        &self,
        bucket: &str,
        object: &str,
        upload_id: &str,
        _part_id: usize,
        _data: PutObjReader,
        _opts: &ObjectOptions,
    ) -> Result<PartInfo> {
        let _upload_path = get_upload_id_dir(bucket, object, upload_id);

        // TODO: checkUploadIDExists

        unimplemented!()
    }

    async fn new_multipart_upload(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<MultipartUploadResult> {
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

        let _ = write_quorum;

        let mut fi = FileInfo::new([bucket, object].join("/").as_str(), data_drives, parity_drives);

        fi.data_dir = Uuid::new_v4();
        fi.fresh = true;

        let parts_metadata = vec![fi.clone(); disks.len()];

        let (shuffle_disks, mut shuffle_parts_metadata) = shuffle_disks_and_parts_metadata(&disks, &parts_metadata, &fi);

        for fi in shuffle_parts_metadata.iter_mut() {
            fi.mod_time = OffsetDateTime::now_utc();
        }

        let upload_uuid = format!("{}x{}", Uuid::new_v4(), fi.mod_time);

        let upload_id = base64_encode(format!("{}.{}", "globalDeploymentID", upload_uuid).as_bytes());

        let upload_path = get_upload_id_dir(bucket, object, upload_uuid.as_str());

        let errs = write_unique_file_info(
            &shuffle_disks,
            bucket,
            RUSTFS_META_MULTIPART_BUCKET,
            upload_path.as_str(),
            &shuffle_parts_metadata,
        )
        .await;

        debug!("write_unique_file_info errs :{:?}", &errs);
        // TODO: reduceWriteQuorumErrs
        // evalDisks

        Ok(MultipartUploadResult { upload_id })
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
