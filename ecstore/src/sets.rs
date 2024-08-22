use std::collections::HashMap;

use futures::future::join_all;
use http::HeaderMap;
use uuid::Uuid;

use crate::{
    disk::{
        format::{DistributionAlgoVersion, FormatV3},
        DiskStore,
    },
    endpoints::PoolEndpoints,
    error::{Error, Result},
    set_disk::SetDisks,
    store_api::{
        BucketInfo, BucketOptions, CompletePart, DeletedObject, GetObjectReader, HTTPRangeSpec, ListObjectsV2Info,
        MakeBucketOptions, MultipartUploadResult, ObjectInfo, ObjectOptions, ObjectToDelete, PartInfo, PutObjReader, StorageAPI,
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
            id: fm.id,
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

    async fn delete_prefix(&self, bucket: &str, object: &str) -> Result<()> {
        let mut futures = Vec::new();
        let opt = ObjectOptions {
            delete_prefix: true,
            ..Default::default()
        };

        for set in self.disk_set.iter() {
            futures.push(set.delete_object(bucket, object, opt.clone()));
        }

        let _results = join_all(futures).await;

        Ok(())
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

struct DelObj {
    // set_idx: usize,
    orig_idx: usize,
    obj: ObjectToDelete,
}

#[async_trait::async_trait]
impl StorageAPI for Sets {
    async fn list_bucket(&self, _opts: &BucketOptions) -> Result<Vec<BucketInfo>> {
        unimplemented!()
    }
    async fn make_bucket(&self, _bucket: &str, _opts: &MakeBucketOptions) -> Result<()> {
        unimplemented!()
    }

    async fn get_bucket_info(&self, _bucket: &str, _opts: &BucketOptions) -> Result<BucketInfo> {
        unimplemented!()
    }
    async fn delete_objects(
        &self,
        bucket: &str,
        objects: Vec<ObjectToDelete>,
        opts: ObjectOptions,
    ) -> Result<(Vec<DeletedObject>, Vec<Option<Error>>)> {
        // 默认返回值
        let mut del_objects = vec![DeletedObject::default(); objects.len()];

        let mut del_errs = Vec::with_capacity(objects.len());
        for _ in 0..objects.len() {
            del_errs.push(None)
        }

        let mut set_obj_map = HashMap::new();

        // hash key
        let mut i = 0;
        for obj in objects.iter() {
            let idx = self.get_hashed_set_index(obj.object_name.as_str());

            if !set_obj_map.contains_key(&idx) {
                set_obj_map.insert(
                    idx,
                    vec![DelObj {
                        // set_idx: idx,
                        orig_idx: i,
                        obj: obj.clone(),
                    }],
                );
            } else {
                if let Some(val) = set_obj_map.get_mut(&idx) {
                    val.push(DelObj {
                        // set_idx: idx,
                        orig_idx: i,
                        obj: obj.clone(),
                    });
                }
            }

            i += 1;
        }

        // TODO: 并发
        for (k, v) in set_obj_map {
            let disks = self.get_disks(k);
            let objs: Vec<ObjectToDelete> = v.iter().map(|v| v.obj.clone()).collect();
            let (dobjects, errs) = disks.delete_objects(bucket, objs, opts.clone()).await?;

            let mut i = 0;
            for err in errs {
                let obj = v.get(i).unwrap();

                del_errs[obj.orig_idx] = err;

                del_objects[obj.orig_idx] = dobjects.get(i).unwrap().clone();

                i += 1;
            }
        }

        Ok((del_objects, del_errs))
    }
    async fn delete_object(&self, bucket: &str, object: &str, opts: ObjectOptions) -> Result<ObjectInfo> {
        if opts.delete_prefix {
            self.delete_prefix(bucket, object).await?;
            return Ok(ObjectInfo::default());
        }

        self.get_disks_by_key(object).delete_object(bucket, object, opts).await
    }
    async fn list_objects_v2(
        &self,
        _bucket: &str,
        _prefix: &str,
        _continuation_token: &str,
        _delimiter: &str,
        _max_keys: i32,
        _fetch_owner: bool,
        _start_after: &str,
    ) -> Result<ListObjectsV2Info> {
        unimplemented!()
    }

    async fn get_object_info(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<ObjectInfo> {
        self.get_disks_by_key(object).get_object_info(bucket, object, opts).await
    }
    async fn get_object_reader(
        &self,
        bucket: &str,
        object: &str,
        range: HTTPRangeSpec,
        h: HeaderMap,
        opts: &ObjectOptions,
    ) -> Result<GetObjectReader> {
        self.get_disks_by_key(object)
            .get_object_reader(bucket, object, range, h, opts)
            .await
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
    async fn abort_multipart_upload(&self, bucket: &str, object: &str, upload_id: &str, opts: &ObjectOptions) -> Result<()> {
        self.get_disks_by_key(object)
            .abort_multipart_upload(bucket, object, upload_id, opts)
            .await
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

    async fn delete_bucket(&self, _bucket: &str) -> Result<()> {
        unimplemented!()
    }
}
