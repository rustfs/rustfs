use std::{
    collections::{HashMap, HashSet},
    io::{Cursor, Write},
    path::Path,
    sync::Arc,
    time::Duration,
};

use crate::{
    bitrot::{bitrot_verify, close_bitrot_writers, new_bitrot_filereader, new_bitrot_filewriter, BitrotFileWriter},
    cache_value::metacache_set::{list_path_raw, ListPathRawOptions},
    config::{storageclass, GLOBAL_StorageClass},
    disk::{
        endpoint::Endpoint,
        error::{is_all_not_found, DiskError},
        format::FormatV3,
        new_disk, BufferReader, BufferWriter, CheckPartsResp, DeleteOptions, DiskAPI, DiskInfo, DiskInfoOptions, DiskOption,
        DiskStore, FileInfoVersions, FileReader, FileWriter, MetaCacheEntries, MetaCacheEntry, MetadataResolutionParams,
        ReadMultipleReq, ReadMultipleResp, ReadOptions, UpdateMetadataOpts, WalkDirOptions, RUSTFS_META_BUCKET,
        RUSTFS_META_MULTIPART_BUCKET, RUSTFS_META_TMP_BUCKET,
    },
    erasure::Erasure,
    error::{Error, Result},
    file_meta::{merge_file_meta_versions, FileMeta, FileMetaShallowVersion},
    global::{
        get_global_deployment_id, is_dist_erasure, GLOBAL_BackgroundHealState, GLOBAL_LOCAL_DISK_MAP,
        GLOBAL_LOCAL_DISK_SET_DRIVES,
    },
    heal::{
        data_usage::{DATA_USAGE_CACHE_NAME, DATA_USAGE_ROOT},
        data_usage_cache::{DataUsageCacheInfo, DataUsageEntry, DataUsageEntryInfo},
        heal_commands::{
            HealOpts, HealScanMode, HealingTracker, DRIVE_STATE_CORRUPT, DRIVE_STATE_MISSING, DRIVE_STATE_OFFLINE,
            DRIVE_STATE_OK, HEAL_DEEP_SCAN, HEAL_ITEM_OBJECT, HEAL_NORMAL_SCAN,
        },
        heal_ops::BG_HEALING_UUID,
    },
    quorum::{object_op_ignored_errs, reduce_read_quorum_errs, reduce_write_quorum_errs, QuorumError},
    store_api::{
        BucketInfo, BucketOptions, CompletePart, DeleteBucketOptions, DeletedObject, FileInfo, GetObjectReader, HTTPRangeSpec,
        ListMultipartsInfo, ListObjectsV2Info, MakeBucketOptions, MultipartInfo, MultipartUploadResult, ObjectIO, ObjectInfo,
        ObjectOptions, ObjectPartInfo, ObjectToDelete, PartInfo, PutObjReader, RawFileInfo, StorageAPI, DEFAULT_BITROT_ALGO,
    },
    store_err::{to_object_err, StorageError},
    store_init::{load_format_erasure, ErasureError},
    utils::{
        self,
        crypto::{base64_decode, base64_encode, hex, sha256},
        path::{encode_dir_object, has_suffix, SLASH_SEPARATOR},
    },
    xhttp,
};
use crate::{file_meta::file_info_from_raw, heal::data_usage_cache::DataUsageCache};
use crate::{
    heal::data_scanner::{globalHealConfig, HEAL_DELETE_DANGLING},
    store_api::ListObjectVersionsInfo,
};
use crate::{
    heal::heal_ops::{HealEntryFn, HealSequence},
    io::Writer,
};
use futures::future::join_all;
use glob::Pattern;
use http::HeaderMap;
use lock::{
    drwmutex::Options,
    namespace_lock::{new_nslock, NsLockMap},
    LockApi,
};
use madmin::heal_commands::{HealDriveInfo, HealResultItem};
use rand::{
    thread_rng,
    {seq::SliceRandom, Rng},
};
use reader::reader::EtagReader;
use s3s::dto::StreamingBlob;
use sha2::{Digest, Sha256};
use std::hash::Hash;
use std::time::SystemTime;
use time::OffsetDateTime;
use tokio::{
    io::DuplexStream,
    sync::{broadcast, RwLock},
};
use tokio::{
    select,
    sync::mpsc::{self, Sender},
    time::interval,
};
use tracing::error;
use tracing::{debug, info, warn};
use uuid::Uuid;
use workers::workers::Workers;

pub const CHECK_PART_UNKNOWN: usize = 0;
// Changing the order can cause a data loss
// when running two nodes with incompatible versions
pub const CHECK_PART_SUCCESS: usize = 1;
pub const CHECK_PART_DISK_NOT_FOUND: usize = 2;
pub const CHECK_PART_VOLUME_NOT_FOUND: usize = 3;
pub const CHECK_PART_FILE_NOT_FOUND: usize = 4;
pub const CHECK_PART_FILE_CORRUPT: usize = 5;

#[derive(Debug)]
pub struct SetDisks {
    pub lockers: Vec<LockApi>,
    pub locker_owner: String,
    pub ns_mutex: Arc<RwLock<NsLockMap>>,
    pub disks: RwLock<Vec<Option<DiskStore>>>,
    pub set_endpoints: Vec<Endpoint>,
    pub set_drive_count: usize,
    pub default_parity_count: usize,
    pub set_index: usize,
    pub pool_index: usize,
    pub format: FormatV3,
}

impl SetDisks {
    async fn get_disks_internal(&self) -> Vec<Option<DiskStore>> {
        let rl = self.disks.read().await;

        rl.clone()
    }

    async fn get_online_disks(&self) -> Vec<Option<DiskStore>> {
        let mut disks = self.get_disks_internal().await;

        // TODO: diskinfo filter online

        let mut new_disk = Vec::with_capacity(disks.len());

        for disk in disks.iter() {
            if let Some(d) = disk {
                if d.is_online().await {
                    new_disk.push(disk.clone());
                }
            }
        }

        let mut rng = thread_rng();

        disks.shuffle(&mut rng);

        new_disk
    }
    async fn get_online_local_disks(&self) -> Vec<Option<DiskStore>> {
        let mut disks = self.get_online_disks().await;

        let mut rng = thread_rng();

        disks.shuffle(&mut rng);

        let disks = disks
            .into_iter()
            .filter(|v| v.as_ref().is_some_and(|d| d.is_local()))
            .collect();

        disks
    }
    async fn _get_local_disks(&self) -> Vec<Option<DiskStore>> {
        let mut disks = self.get_disks_internal().await;

        let mut rng = thread_rng();

        disks.shuffle(&mut rng);

        let disks = disks
            .into_iter()
            .filter(|v| v.as_ref().is_some_and(|d| d.is_local()))
            .collect();

        disks
    }
    fn default_write_quorum(&self) -> usize {
        let mut data_count = self.set_drive_count - self.default_parity_count;
        if data_count == self.default_parity_count {
            data_count += 1
        }

        data_count
    }

    #[tracing::instrument(level = "debug", skip(disks, file_infos))]
    #[allow(clippy::type_complexity)]
    async fn rename_data(
        disks: &[Option<DiskStore>],
        src_bucket: &str,
        src_object: &str,
        file_infos: &[FileInfo],
        dst_bucket: &str,
        dst_object: &str,
        write_quorum: usize,
    ) -> Result<(Vec<Option<DiskStore>>, Option<Vec<u8>>, Option<Uuid>)> {
        let mut futures = Vec::with_capacity(disks.len());

        // let mut ress = Vec::with_capacity(disks.len());
        let mut errs = Vec::with_capacity(disks.len());

        for (i, disk) in disks.iter().enumerate() {
            let mut file_info = file_infos[i].clone();

            futures.push(async move {
                if file_info.erasure.index == 0 {
                    file_info.erasure.index = i + 1;
                }

                if !file_info.is_valid() {
                    return Err(Error::new(DiskError::FileCorrupt));
                }

                if let Some(disk) = disk {
                    disk.rename_data(src_bucket, src_object, file_info, dst_bucket, dst_object)
                        .await
                } else {
                    Err(Error::new(DiskError::DiskNotFound))
                }
            })
        }

        let mut disk_versions = vec![None; disks.len()];
        let mut data_dirs = vec![None; disks.len()];

        let results = join_all(futures).await;

        for (idx, result) in results.iter().enumerate() {
            match result {
                Ok(res) => {
                    data_dirs[idx] = res.old_data_dir;
                    disk_versions[idx].clone_from(&res.sign);
                    // ress.push(Some(res));
                    errs.push(None);
                }
                Err(e) => {
                    // ress.push(None);
                    errs.push(Some(e.clone()));
                }
            }
        }

        if let Some(err) = reduce_write_quorum_errs(&errs, object_op_ignored_errs().as_ref(), write_quorum) {
            // TODO: 并发
            for (i, err) in errs.iter().enumerate() {
                if err.is_some() {
                    continue;
                }

                if let Some(disk) = disks[i].as_ref() {
                    let fi = file_infos[i].clone();
                    let _ = disk
                        .delete_version(
                            src_bucket,
                            src_object,
                            fi,
                            false,
                            DeleteOptions {
                                undo_write: true,
                                old_data_dir: data_dirs[i],
                                ..Default::default()
                            },
                        )
                        .await
                        .map_err(|e| {
                            debug!("rename_data delete_version err {:?}", e);
                            e
                        });
                }
            }

            return Err(err);
        }

        let versions = None;
        // TODO: reduceCommonVersions

        let data_dir = Self::reduce_common_data_dir(&data_dirs, write_quorum);

        // // TODO: reduce_common_data_dir
        // if let Some(old_dir) = rename_ress
        //     .iter()
        //     .filter_map(|v| if v.is_some() { v.as_ref().unwrap().old_data_dir } else { None })
        //     .map(|v| v.to_string())
        //     .next()
        // {
        //     let cm_errs = self.commit_rename_data_dir(&shuffle_disks, &bucket, &object, &old_dir).await;
        //     warn!("put_object commit_rename_data_dir:{:?}", &cm_errs);
        // }

        // self.delete_all(RUSTFS_META_TMP_BUCKET, &tmp_dir).await?;

        Ok((Self::eval_disks(disks, &errs), versions, data_dir))
    }

    fn reduce_common_data_dir(data_dirs: &Vec<Option<Uuid>>, write_quorum: usize) -> Option<Uuid> {
        let mut data_dirs_count = std::collections::HashMap::new();

        for ddir in data_dirs {
            *data_dirs_count.entry(ddir).or_insert(0) += 1;
        }

        let mut max = 0;
        let mut data_dir = None;
        for (ddir, count) in data_dirs_count {
            if count > max {
                max = count;
                data_dir = *ddir;
            }
        }

        if max >= write_quorum {
            data_dir
        } else {
            None
        }
    }

    #[allow(dead_code)]
    async fn commit_rename_data_dir(
        &self,
        disks: &[Option<DiskStore>],
        bucket: &str,
        object: &str,
        data_dir: &str,
        write_quorum: usize,
    ) -> Result<()> {
        let file_path = format!("{}/{}", object, data_dir);

        let mut futures = Vec::with_capacity(disks.len());
        let mut errs = Vec::with_capacity(disks.len());

        for disk in disks.iter() {
            let file_path = file_path.clone();
            futures.push(async move {
                if let Some(disk) = disk {
                    disk.delete(
                        bucket,
                        &file_path,
                        DeleteOptions {
                            recursive: true,
                            ..Default::default()
                        },
                    )
                    .await
                } else {
                    Err(Error::new(DiskError::DiskNotFound))
                }
            });
        }

        let results = join_all(futures).await;
        for result in results {
            match result {
                Ok(_) => {
                    errs.push(None);
                }
                Err(e) => {
                    errs.push(Some(e));
                }
            }
        }

        if let Some(err) = reduce_write_quorum_errs(&errs, object_op_ignored_errs().as_ref(), write_quorum) {
            return Err(err);
        }

        Ok(())
    }

    async fn cleanup_multipart_path(disks: &[Option<DiskStore>], paths: &[&str]) {
        let mut futures = Vec::with_capacity(disks.len());

        let mut errs = Vec::with_capacity(disks.len());

        for disk in disks.iter() {
            futures.push(async move {
                if let Some(disk) = disk {
                    disk.delete_paths(RUSTFS_META_MULTIPART_BUCKET, paths).await
                } else {
                    Err(Error::new(DiskError::DiskNotFound))
                }
            })
        }

        let results = join_all(futures).await;
        for result in results {
            match result {
                Ok(_) => {
                    errs.push(None);
                }
                Err(e) => {
                    errs.push(Some(e));
                }
            }
        }
    }
    async fn rename_part(
        disks: &[Option<DiskStore>],
        src_bucket: &str,
        src_object: &str,
        dst_bucket: &str,
        dst_object: &str,
        meta: Vec<u8>,
        write_quorum: usize,
    ) -> Result<Vec<Option<DiskStore>>> {
        let mut futures = Vec::with_capacity(disks.len());

        let mut errs = Vec::with_capacity(disks.len());

        for disk in disks.iter() {
            let meta = meta.clone();
            futures.push(async move {
                if let Some(disk) = disk {
                    disk.rename_part(src_bucket, src_object, dst_bucket, dst_object, meta).await
                } else {
                    Err(Error::new(DiskError::DiskNotFound))
                }
            })
        }

        let results = join_all(futures).await;
        for result in results {
            match result {
                Ok(_) => {
                    errs.push(None);
                }
                Err(e) => {
                    errs.push(Some(e));
                }
            }
        }

        if let Some(err) = reduce_write_quorum_errs(&errs, object_op_ignored_errs().as_ref(), write_quorum) {
            warn!("rename_part errs {:?}", &errs);
            Self::cleanup_multipart_path(disks, vec![dst_object, format!("{}.meta", dst_object).as_str()].as_slice()).await;
            return Err(err);
        }

        let disks = Self::eval_disks(disks, &errs);
        Ok(disks)
    }

    fn eval_disks(disks: &[Option<DiskStore>], errs: &[Option<Error>]) -> Vec<Option<DiskStore>> {
        if disks.len() != errs.len() {
            return Vec::new();
        }

        let mut online_disks = vec![None; disks.len()];

        for (i, err_op) in errs.iter().enumerate() {
            if err_op.is_none() {
                online_disks[i].clone_from(&disks[i]);
            }
        }

        online_disks
    }

    // async fn write_all(disks: &[Option<DiskStore>], bucket: &str, object: &str, buff: Vec<u8>) -> Vec<Option<Error>> {
    //     let mut futures = Vec::with_capacity(disks.len());

    //     let mut errors = Vec::with_capacity(disks.len());

    //     for disk in disks.iter() {
    //         if disk.is_none() {
    //             errors.push(Some(Error::new(DiskError::DiskNotFound)));
    //             continue;
    //         }
    //         let disk = disk.as_ref().unwrap();
    //         futures.push(disk.write_all(bucket, object, buff.clone()));
    //     }

    //     let results = join_all(futures).await;
    //     for result in results {
    //         match result {
    //             Ok(_) => {
    //                 errors.push(None);
    //             }
    //             Err(e) => {
    //                 errors.push(Some(e));
    //             }
    //         }
    //     }
    //     errors
    // }

    async fn write_unique_file_info(
        disks: &[Option<DiskStore>],
        org_bucket: &str,
        bucket: &str,
        prefix: &str,
        files: &[FileInfo],
        // write_quorum: usize,
    ) -> Vec<Option<Error>> {
        let mut futures = Vec::with_capacity(disks.len());
        let mut errors = Vec::with_capacity(disks.len());

        for (i, disk) in disks.iter().enumerate() {
            let mut file_info = files[i].clone();
            file_info.erasure.index = i + 1;
            futures.push(async move {
                if let Some(disk) = disk {
                    disk.write_metadata(org_bucket, bucket, prefix, file_info).await
                } else {
                    Err(Error::new(DiskError::DiskNotFound))
                }
            });
        }

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
        // warn!("get_upload_id_dir upload_id {:?}", upload_id);
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

        format!("{}/{}", Self::get_multipart_sha_dir(bucket, object), upload_uuid)
    }

    fn get_multipart_sha_dir(bucket: &str, object: &str) -> String {
        let path = format!("{}/{}", bucket, object);
        hex(sha256(path.as_bytes()).as_ref())
    }

    fn common_parity(parities: &[i32], default_parity_count: i32) -> i32 {
        let n = parities.len() as i32;

        let mut occ_map: HashMap<i32, i32> = HashMap::new();
        for &p in parities {
            *occ_map.entry(p).or_insert(0) += 1;
        }

        let mut max_occ = 0;
        let mut cparity = 0;
        for (&parity, &occ) in &occ_map {
            if parity == -1 {
                // Ignore non defined parity
                continue;
            }

            let mut read_quorum = n - parity;
            if default_parity_count > 0 && parity == 0 {
                // In this case, parity == 0 implies that this object version is a
                // delete marker
                read_quorum = n / 2 + 1;
            }
            if occ < read_quorum {
                // Ignore this parity since we don't have enough shards for read quorum
                continue;
            }

            if occ > max_occ {
                max_occ = occ;
                cparity = parity;
            }
        }

        if max_occ == 0 {
            // Did not found anything useful
            return -1;
        }
        cparity
    }

    fn list_object_modtimes(parts_metadata: &[FileInfo], errs: &[Option<Error>]) -> Vec<Option<OffsetDateTime>> {
        let mut times = vec![None; parts_metadata.len()];

        for (i, metadata) in parts_metadata.iter().enumerate() {
            if errs[i].is_some() {
                continue;
            }

            times[i] = metadata.mod_time
        }

        times
    }

    fn common_time(times: &[Option<OffsetDateTime>], quorum: usize) -> Option<OffsetDateTime> {
        let (time, count) = Self::common_time_and_occurrence(times);
        if count >= quorum {
            time
        } else {
            None
        }
    }

    fn common_time_and_occurrence(times: &[Option<OffsetDateTime>]) -> (Option<OffsetDateTime>, usize) {
        let mut time_occurrence_map = HashMap::new();

        // Ignore the uuid sentinel and count the rest.
        for time in times.iter().flatten() {
            *time_occurrence_map.entry(time.unix_timestamp_nanos()).or_insert(0) += 1;
        }

        let mut maxima = 0; // Counter for remembering max occurrence of elements.
        let mut latest = 0;

        // Find the common cardinality from previously collected
        // occurrences of elements.
        for (&nano, &count) in &time_occurrence_map {
            if count < maxima {
                continue;
            }

            // We are at or above maxima
            if count > maxima || nano > latest {
                maxima = count;
                latest = nano;
            }
        }

        if latest == 0 {
            return (None, maxima);
        }

        if let Ok(time) = OffsetDateTime::from_unix_timestamp_nanos(latest) {
            (Some(time), maxima)
        } else {
            (None, maxima)
        }
    }

    fn common_etag(etags: &[Option<String>], quorum: usize) -> Option<String> {
        let (etag, count) = Self::common_etags(etags);
        if count >= quorum {
            etag
        } else {
            None
        }
    }

    fn common_etags(etags: &[Option<String>]) -> (Option<String>, usize) {
        let mut etags_map = HashMap::new();

        for etag in etags.iter().flatten() {
            *etags_map.entry(etag).or_insert(0) += 1;
        }

        let mut maxima = 0; // Counter for remembering max occurrence of elements.
        let mut latest = None;

        for (&etag, &count) in &etags_map {
            if count < maxima {
                continue;
            }

            // We are at or above maxima
            if count > maxima {
                maxima = count;
                latest = Some(etag.clone());
            }
        }

        (latest, maxima)
    }

    fn list_object_etags(parts_metadata: &[FileInfo], errs: &[Option<Error>]) -> Vec<Option<String>> {
        let mut etags = vec![None; parts_metadata.len()];

        for (i, metadata) in parts_metadata.iter().enumerate() {
            if errs[i].is_some() {
                continue;
            }

            if let Some(meta) = &metadata.metadata {
                if let Some(etag) = meta.get("etag") {
                    etags[i] = Some(etag.clone())
                }
            }
        }

        etags
    }

    fn list_object_parities(parts_metadata: &[FileInfo], errs: &[Option<Error>]) -> Vec<i32> {
        let total_shards = parts_metadata.len();
        let half = total_shards as i32 / 2;
        let mut parities: Vec<i32> = vec![-1; total_shards];

        for (index, metadata) in parts_metadata.iter().enumerate() {
            if errs[index].is_some() {
                parities[index] = -1;
                continue;
            }

            if !metadata.is_valid() {
                parities[index] = -1;
                continue;
            }

            if metadata.deleted || metadata.size == 0 {
                parities[index] = half;
            // } else if metadata.transition_status == "TransitionComplete" {
            // TODO: metadata.transition_status
            //     parities[index] = total_shards - (total_shards / 2 + 1);
            } else {
                parities[index] = metadata.erasure.parity_blocks as i32;
            }
        }
        parities
    }

    // Returns per object readQuorum and writeQuorum
    // readQuorum is the min required disks to read data.
    // writeQuorum is the min required disks to write data.
    fn object_quorum_from_meta(
        parts_metadata: &[FileInfo],
        errs: &[Option<Error>],
        default_parity_count: usize,
    ) -> Result<(i32, i32)> {
        let expected_rquorum = if default_parity_count == 0 {
            parts_metadata.len()
        } else {
            parts_metadata.len() / 2
        };

        if let Some(err) = reduce_read_quorum_errs(errs, object_op_ignored_errs().as_ref(), expected_rquorum) {
            return Err(err);
        }

        if default_parity_count == 0 {
            return Ok((parts_metadata.len() as i32, parts_metadata.len() as i32));
        }

        let parities = Self::list_object_parities(parts_metadata, errs);

        let parity_blocks = Self::common_parity(&parities, default_parity_count as i32);

        if parity_blocks < 0 {
            warn!("QuorumError::Read, common_parity < 0 ");
            return Err(Error::new(QuorumError::Read));
        }

        let data_blocks = parts_metadata.len() as i32 - parity_blocks;
        let write_quorum = if data_blocks == parity_blocks {
            data_blocks + 1
        } else {
            data_blocks
        };

        Ok((data_blocks, write_quorum))
    }

    #[tracing::instrument(level = "debug", skip(disks, parts_metadata))]
    fn list_online_disks(
        disks: &[Option<DiskStore>],
        parts_metadata: &[FileInfo],
        errs: &[Option<Error>],
        quorum: usize,
    ) -> (Vec<Option<DiskStore>>, Option<OffsetDateTime>, Option<String>) {
        let mod_times = Self::list_object_modtimes(parts_metadata, errs);
        let etags = Self::list_object_etags(parts_metadata, errs);

        let mod_time = Self::common_time(&mod_times, quorum);
        let etag = Self::common_etag(&etags, quorum);

        let mut new_disk = vec![None; disks.len()];

        for (i, &t) in mod_times.iter().enumerate() {
            if parts_metadata[i].is_valid() && mod_time == t {
                new_disk[i].clone_from(&disks[i]);
            }
        }

        (new_disk, mod_time, etag)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn check_upload_id_exists(
        &self,
        bucket: &str,
        object: &str,
        upload_id: &str,
        write: bool,
    ) -> Result<(FileInfo, Vec<FileInfo>)> {
        let upload_id_path = Self::get_upload_id_dir(bucket, object, upload_id);
        let disks = self.disks.read().await;

        let disks = disks.clone();

        let (parts_metadata, errs) =
            Self::read_all_fileinfo(&disks, bucket, RUSTFS_META_MULTIPART_BUCKET, &upload_id_path, "", false, false).await;

        let (read_quorum, write_quorum) = Self::object_quorum_from_meta(&parts_metadata, &errs, self.default_parity_count)?;

        if read_quorum < 0 {
            return Err(Error::new(QuorumError::Read));
        }

        if write_quorum < 0 {
            return Err(Error::new(QuorumError::Write));
        }

        let mut quorum = read_quorum as usize;
        if write {
            quorum = write_quorum as usize;

            if let Some(err) = reduce_write_quorum_errs(&errs, object_op_ignored_errs().as_ref(), quorum) {
                return Err(err);
            }
        } else if let Some(err) = reduce_read_quorum_errs(&errs, object_op_ignored_errs().as_ref(), quorum) {
            return Err(err);
        }

        let (_, mod_time, etag) = Self::list_online_disks(&disks, &parts_metadata, &errs, quorum);

        let fi = Self::pick_valid_fileinfo(&parts_metadata, mod_time, etag, quorum)?;

        Ok((fi, parts_metadata))
    }

    fn pick_valid_fileinfo(
        metas: &[FileInfo],
        mod_time: Option<OffsetDateTime>,
        etag: Option<String>,
        quorum: usize,
    ) -> Result<FileInfo> {
        Self::find_file_info_in_quorum(metas, &mod_time, &etag, quorum)
    }

    fn find_file_info_in_quorum(
        metas: &[FileInfo],
        mod_time: &Option<OffsetDateTime>,
        etag: &Option<String>,
        quorum: usize,
    ) -> Result<FileInfo> {
        if quorum < 1 {
            return Err(Error::new(StorageError::InsufficientReadQuorum));
        }

        let mut meta_hashs = vec![None; metas.len()];
        let mut hasher = Sha256::new();

        for (i, meta) in metas.iter().enumerate() {
            if !meta.is_valid() {
                continue;
            }

            let etag_only = mod_time.is_none() && etag.is_some() && meta.get_etag().is_some_and(|v| &v == etag.as_ref().unwrap());
            let mod_valid = mod_time == &meta.mod_time;

            if etag_only || mod_valid {
                for part in meta.parts.iter() {
                    let _ = hasher.write(format!("part.{}", part.number).as_bytes())?;
                    let _ = hasher.write(format!("part.{}", part.size).as_bytes())?;
                }

                if !meta.deleted && meta.size != 0 {
                    let _ = hasher.write(format!("{}+{}", meta.erasure.data_blocks, meta.erasure.parity_blocks).as_bytes())?;
                    let _ = hasher.write(format!("{:?}", meta.erasure.distribution).as_bytes())?;
                }

                if meta.is_remote() {
                    // TODO:
                }

                // TODO: IsEncrypted

                // TODO: IsCompressed

                hasher.flush()?;

                meta_hashs[i] = Some(utils::crypto::hex(hasher.clone().finalize().as_slice()));

                hasher.reset();
            }
        }

        let mut count_map = HashMap::new();

        for hash in meta_hashs.iter().flatten() {
            *count_map.entry(hash).or_insert(0) += 1;
        }

        let mut max_val = None;
        let mut max_count = 0;

        for (&val, &count) in &count_map {
            if count > max_count {
                max_val = Some(val);
                max_count = count;
            }
        }

        if max_count < quorum {
            return Err(Error::new(StorageError::InsufficientReadQuorum));
        }

        let mut found_fi = None;
        let mut found = false;

        let mut valid_obj_map = HashMap::new();

        for (i, op_hash) in meta_hashs.iter().enumerate() {
            if let Some(hash) = op_hash {
                if let Some(max_hash) = max_val {
                    if hash == max_hash {
                        if metas[i].is_valid() && !found {
                            found_fi = Some(metas[i].clone());
                            found = true;
                        }

                        let props = ObjProps {
                            mod_time: metas[i].mod_time,
                            num_versions: metas[i].num_versions,
                        };

                        *valid_obj_map.entry(props).or_insert(0) += 1;
                    }
                }
            }
        }

        if found {
            let mut fi = found_fi.unwrap();

            for (val, &count) in &valid_obj_map {
                if count > quorum {
                    fi.mod_time = val.mod_time;
                    fi.num_versions = val.num_versions;
                    fi.is_latest = val.mod_time.is_none();

                    break;
                }
            }

            return Ok(fi);
        }

        warn!("QuorumError::Read, find_file_info_in_quorum fileinfo not found");

        Err(Error::new(StorageError::InsufficientReadQuorum))
    }

    #[tracing::instrument(level = "debug", skip(disks))]
    async fn read_all_fileinfo(
        disks: &[Option<DiskStore>],
        org_bucket: &str,
        bucket: &str,
        object: &str,
        version_id: &str,
        read_data: bool,
        healing: bool,
    ) -> (Vec<FileInfo>, Vec<Option<Error>>) {
        let mut futures = Vec::with_capacity(disks.len());
        let mut ress = Vec::with_capacity(disks.len());
        let mut errors = Vec::with_capacity(disks.len());

        for disk in disks.iter() {
            let opts = ReadOptions { read_data, healing };
            futures.push(async move {
                if let Some(disk) = disk {
                    if version_id.is_empty() {
                        match disk.read_xl(bucket, object, read_data).await {
                            Ok(info) => {
                                let fi = file_info_from_raw(info, bucket, object, read_data).await?;
                                Ok(fi)
                            }
                            Err(err) => Err(err),
                        }
                    } else {
                        disk.read_version(org_bucket, bucket, object, version_id, &opts).await
                    }
                } else {
                    Err(Error::new(DiskError::DiskNotFound))
                }
            })
        }

        let results = join_all(futures).await;
        for result in results {
            match result {
                Ok(res) => {
                    ress.push(res);
                    errors.push(None);
                }
                Err(e) => {
                    ress.push(FileInfo::default());
                    errors.push(Some(e));
                }
            }
        }
        (ress, errors)
    }

    async fn read_all_xl(
        disks: &[Option<DiskStore>],
        bucket: &str,
        object: &str,
        read_data: bool,
        incl_free_vers: bool,
    ) -> (Vec<FileInfo>, Vec<Option<Error>>) {
        let (fileinfos, errs) = Self::read_all_raw_file_info(disks, bucket, object, read_data).await;

        Self::pick_latest_quorum_files_info(fileinfos, errs, bucket, object, read_data, incl_free_vers).await
    }

    async fn read_all_raw_file_info(
        disks: &[Option<DiskStore>],
        bucket: &str,
        object: &str,
        read_data: bool,
    ) -> (Vec<Option<RawFileInfo>>, Vec<Option<Error>>) {
        let mut futures = Vec::with_capacity(disks.len());
        let mut ress = Vec::with_capacity(disks.len());
        let mut errors = Vec::with_capacity(disks.len());

        for disk in disks.iter() {
            futures.push(async move {
                if let Some(disk) = disk {
                    disk.read_xl(bucket, object, read_data).await
                } else {
                    Err(Error::new(DiskError::DiskNotFound))
                }
            });
        }

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

        (ress, errors)
    }

    async fn pick_latest_quorum_files_info(
        fileinfos: Vec<Option<RawFileInfo>>,
        errs: Vec<Option<Error>>,
        bucket: &str,
        object: &str,
        read_data: bool,
        _incl_free_vers: bool,
    ) -> (Vec<FileInfo>, Vec<Option<Error>>) {
        let mut metadata_array = vec![None; fileinfos.len()];
        let mut meta_file_infos = vec![FileInfo::default(); fileinfos.len()];
        let mut metadata_shallow_versions = vec![None; fileinfos.len()];

        let mut v2_bufs = {
            if !read_data {
                vec![Vec::new(); fileinfos.len()]
            } else {
                Vec::new()
            }
        };

        let mut errs = errs;

        for (idx, info_op) in fileinfos.iter().enumerate() {
            if let Some(info) = info_op {
                if !read_data {
                    v2_bufs[idx] = info.buf.clone();
                }

                let xlmeta = match FileMeta::load(&info.buf) {
                    Ok(res) => res,
                    Err(err) => {
                        errs[idx] = Some(err);
                        continue;
                    }
                };

                metadata_array[idx] = Some(xlmeta);
                meta_file_infos[idx] = FileInfo::default();
            }
        }

        for (idx, info_op) in metadata_array.iter().enumerate() {
            if let Some(info) = info_op {
                metadata_shallow_versions[idx] = Some(info.versions.clone());
            }
        }

        let shallow_versions: Vec<Vec<FileMetaShallowVersion>> = metadata_shallow_versions.iter().flatten().cloned().collect();

        let read_quorum = (fileinfos.len() + 1) / 2;
        let versions = merge_file_meta_versions(read_quorum, false, 1, &shallow_versions);
        let meta = FileMeta {
            versions,
            ..Default::default()
        };

        let finfo = match meta.into_fileinfo(bucket, object, "", true, true) {
            Ok(res) => res,
            Err(err) => {
                for item in errs.iter_mut() {
                    if item.is_none() {
                        *item = Some(err.clone())
                    }
                }

                return (meta_file_infos, errs);
            }
        };

        if !finfo.is_valid() {
            for item in errs.iter_mut() {
                if item.is_none() {
                    *item = Some(Error::new(DiskError::FileCorrupt));
                }
            }

            return (meta_file_infos, errs);
        }

        let vid = finfo.version_id.unwrap_or(Uuid::nil());

        for (idx, meta_op) in metadata_array.iter().enumerate() {
            if let Some(meta) = meta_op {
                match meta.into_fileinfo(bucket, object, vid.to_string().as_str(), read_data, true) {
                    Ok(res) => meta_file_infos[idx] = res,
                    Err(err) => errs[idx] = Some(err),
                }
            }
        }

        (meta_file_infos, errs)
    }

    async fn read_multiple_files(disks: &[Option<DiskStore>], req: ReadMultipleReq, read_quorum: usize) -> Vec<ReadMultipleResp> {
        let mut futures = Vec::with_capacity(disks.len());
        let mut ress = Vec::with_capacity(disks.len());
        let mut errors = Vec::with_capacity(disks.len());

        for disk in disks.iter() {
            let req = req.clone();
            futures.push(async move {
                if let Some(disk) = disk {
                    disk.read_multiple(req).await
                } else {
                    Err(Error::new(DiskError::DiskNotFound))
                }
            });
        }

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

        // debug!("ReadMultipleResp ress {:?}", ress);
        // debug!("ReadMultipleResp errors {:?}", errors);

        let mut ret = Vec::with_capacity(req.files.len());

        for want in req.files.iter() {
            let mut quorum = 0;

            let mut get_res = ReadMultipleResp::default();

            for res in ress.iter() {
                if res.is_none() {
                    continue;
                }

                let disk_res = res.as_ref().unwrap();

                for resp in disk_res.iter() {
                    if !resp.error.is_empty() || !resp.exists {
                        continue;
                    }

                    if &resp.file != want || resp.bucket != req.bucket || resp.prefix != req.prefix {
                        continue;
                    }
                    quorum += 1;

                    if get_res.mod_time > resp.mod_time || get_res.data.len() > resp.data.len() {
                        continue;
                    }

                    get_res = resp.clone();
                }
            }

            if quorum < read_quorum {
                // debug!("quorum < read_quorum: {} < {}", quorum, read_quorum);
                get_res.exists = false;
                get_res.error = ErasureError::ErasureReadQuorum.to_string();
                get_res.data = Vec::new();
            }

            ret.push(get_res);
        }

        // log err

        ret
    }

    pub async fn connect_disks(&self) {
        let rl = self.disks.read().await;

        let disks = rl.clone();

        //  主动释放锁
        drop(rl);

        for (i, opdisk) in disks.iter().enumerate() {
            if let Some(disk) = opdisk {
                if disk.is_online().await && disk.get_disk_location().set_idx.is_some() {
                    continue;
                }

                let _ = disk.close().await;
            }

            if let Some(endpoint) = self.set_endpoints.get(i) {
                self.renew_disk(endpoint).await;
            }
        }
    }

    pub async fn renew_disk(&self, ep: &Endpoint) {
        debug!("renew_disk start {:?}", ep);

        let (new_disk, fm) = match Self::connect_endpoint(ep).await {
            Ok(res) => res,
            Err(e) => {
                warn!("connect_endpoint err {:?}", &e);
                if ep.is_local && DiskError::UnformattedDisk.is(&e) {
                    // TODO: pushHealLocalDisks
                }
                return;
            }
        };

        let (set_idx, disk_idx) = match self.find_disk_index(&fm) {
            Ok(res) => res,
            Err(e) => {
                warn!("find_disk_index err {:?}", e);
                return;
            }
        };

        // check endpoint是否一致

        let _ = new_disk.set_disk_id(Some(fm.erasure.this)).await;

        if new_disk.is_local() {
            let mut global_local_disk_map = GLOBAL_LOCAL_DISK_MAP.write().await;
            let path = new_disk.endpoint().to_string();
            global_local_disk_map.insert(path, Some(new_disk.clone()));

            if is_dist_erasure().await {
                let mut local_set_drives = GLOBAL_LOCAL_DISK_SET_DRIVES.write().await;
                local_set_drives[self.pool_index][set_idx][disk_idx] = Some(new_disk.clone());
            }
        }

        debug!("renew_disk update {:?}", fm.erasure.this);

        let mut disk_lock = self.disks.write().await;
        disk_lock[disk_idx] = Some(new_disk);
    }

    fn find_disk_index(&self, fm: &FormatV3) -> Result<(usize, usize)> {
        self.format.check_other(fm)?;

        if fm.erasure.this.is_nil() {
            return Err(Error::msg("DriveID: offline"));
        }

        for i in 0..self.format.erasure.sets.len() {
            for j in 0..self.format.erasure.sets[0].len() {
                if fm.erasure.this == self.format.erasure.sets[i][j] {
                    return Ok((i, j));
                }
            }
        }

        Err(Error::msg("DriveID: not found"))
    }

    async fn connect_endpoint(ep: &Endpoint) -> Result<(DiskStore, FormatV3)> {
        let disk = new_disk(ep, &DiskOption::default()).await?;

        let fm = load_format_erasure(&disk, false).await?;

        Ok((disk, fm))
    }

    pub async fn walk_dir(&self, opts: &WalkDirOptions) -> (Vec<Option<Vec<MetaCacheEntry>>>, Vec<Option<Error>>) {
        let disks = self.disks.read().await;

        let disks = disks.clone();
        let mut futures = Vec::new();
        let mut errs = Vec::new();
        let mut ress = Vec::new();

        for disk in disks.iter() {
            let opts = opts.clone();
            futures.push(async move {
                if let Some(disk) = disk {
                    disk.walk_dir(opts, &mut Writer::NotUse).await
                } else {
                    Err(Error::new(DiskError::DiskNotFound))
                }
            });
        }

        let results = join_all(futures).await;

        for res in results {
            match res {
                Ok(entrys) => {
                    ress.push(Some(entrys));
                    errs.push(None);
                }
                Err(e) => {
                    ress.push(None);
                    errs.push(Some(e));
                }
            }
        }

        (ress, errs)
    }

    async fn remove_object_part(
        &self,
        bucket: &str,
        object: &str,
        upload_id: &str,
        data_dir: &str,
        part_num: usize,
    ) -> Result<()> {
        let upload_id_path = Self::get_upload_id_dir(bucket, object, upload_id);
        let disks = self.disks.read().await;

        let disks = disks.clone();

        let file_path = format!("{}/{}/part.{}", upload_id_path, data_dir, part_num);

        let mut futures = Vec::with_capacity(disks.len());
        let mut errors = Vec::with_capacity(disks.len());

        for disk in disks.iter() {
            let file_path = file_path.clone();
            let meta_file_path = format!("{}.meta", file_path);

            futures.push(async move {
                if let Some(disk) = disk {
                    disk.delete(RUSTFS_META_MULTIPART_BUCKET, &file_path, DeleteOptions::default())
                        .await?;
                    disk.delete(RUSTFS_META_MULTIPART_BUCKET, &meta_file_path, DeleteOptions::default())
                        .await
                } else {
                    Err(Error::new(DiskError::DiskNotFound))
                }
            });
        }

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

        Ok(())
    }
    async fn remove_part_meta(&self, bucket: &str, object: &str, upload_id: &str, data_dir: &str, part_num: usize) -> Result<()> {
        let upload_id_path = Self::get_upload_id_dir(bucket, object, upload_id);
        let disks = self.disks.read().await;

        let disks = disks.clone();
        // let disks = Self::shuffle_disks(&disks, &fi.erasure.distribution);

        let file_path = format!("{}/{}/part.{}.meta", upload_id_path, data_dir, part_num);

        let mut futures = Vec::with_capacity(disks.len());
        let mut errors = Vec::with_capacity(disks.len());

        for disk in disks.iter() {
            let file_path = file_path.clone();
            futures.push(async move {
                if let Some(disk) = disk {
                    disk.delete(RUSTFS_META_MULTIPART_BUCKET, &file_path, DeleteOptions::default())
                        .await
                } else {
                    Err(Error::new(DiskError::DiskNotFound))
                }
            });
        }

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

        Ok(())
    }

    // #[tracing::instrument(skip(self))]
    pub async fn delete_all(&self, bucket: &str, prefix: &str) -> Result<()> {
        let disks = self.disks.read().await;

        let disks = disks.clone();

        let mut futures = Vec::with_capacity(disks.len());
        let mut errors = Vec::with_capacity(disks.len());

        for disk in disks.iter() {
            futures.push(async move {
                if let Some(disk) = disk {
                    disk.delete(
                        bucket,
                        prefix,
                        DeleteOptions {
                            recursive: true,
                            ..Default::default()
                        },
                    )
                    .await
                } else {
                    Err(Error::new(DiskError::DiskNotFound))
                }
            });
        }

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

        // debug!("delete_all errs {:?}", &errors);

        Ok(())
    }

    // 打乱顺序
    fn shuffle_disks_and_parts_metadata_by_index(
        disks: &[Option<DiskStore>],
        parts_metadata: &[FileInfo],
        fi: &FileInfo,
    ) -> (Vec<Option<DiskStore>>, Vec<FileInfo>) {
        let mut shuffled_disks = vec![None; disks.len()];
        let mut shuffled_parts_metadata = vec![FileInfo::default(); parts_metadata.len()];
        let distribution = &fi.erasure.distribution;

        let mut inconsistent = 0;
        for (k, v) in parts_metadata.iter().enumerate() {
            if disks[k].is_none() {
                inconsistent += 1;
                continue;
            }

            if !v.is_valid() {
                inconsistent += 1;
                continue;
            }

            if distribution[k] != v.erasure.index {
                inconsistent += 1;
                continue;
            }

            let block_idx = distribution[k];
            shuffled_parts_metadata[block_idx - 1] = parts_metadata[k].clone();
            shuffled_disks[block_idx - 1].clone_from(&disks[k]);
        }

        if inconsistent < fi.erasure.parity_blocks {
            return (shuffled_disks, shuffled_parts_metadata);
        }

        Self::shuffle_disks_and_parts_metadata(disks, parts_metadata, fi)
    }

    // 打乱顺序
    fn shuffle_disks_and_parts_metadata(
        disks: &[Option<DiskStore>],
        parts_metadata: &[FileInfo],
        fi: &FileInfo,
    ) -> (Vec<Option<DiskStore>>, Vec<FileInfo>) {
        let init = fi.mod_time.is_none();

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
            shuffled_disks[block_idx - 1].clone_from(&disks[k]);
        }

        (shuffled_disks, shuffled_parts_metadata)
    }

    // Return shuffled partsMetadata depending on distribution.
    fn shuffle_parts_metadata(parts_metadata: &[FileInfo], distribution: &[usize]) -> Vec<FileInfo> {
        if distribution.is_empty() {
            return parts_metadata.to_vec();
        }
        let mut shuffled_parts_metadata = vec![FileInfo::default(); parts_metadata.len()];
        // Shuffle slice xl metadata for expected distribution.
        for index in 0..parts_metadata.len() {
            let block_index = distribution[index];
            shuffled_parts_metadata[block_index - 1] = parts_metadata[index].clone();
        }
        shuffled_parts_metadata
    }

    // shuffle_disks TODO: use origin value
    fn shuffle_disks(disks: &[Option<DiskStore>], distribution: &[usize]) -> Vec<Option<DiskStore>> {
        if distribution.is_empty() {
            return disks.to_vec();
        }

        let mut shuffled_disks = vec![None; disks.len()];

        for (i, v) in disks.iter().enumerate() {
            let idx = distribution[i];
            shuffled_disks[idx - 1].clone_from(v);
        }

        shuffled_disks
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn get_object_fileinfo(
        &self,
        bucket: &str,
        object: &str,
        opts: &ObjectOptions,
        read_data: bool,
    ) -> Result<(FileInfo, Vec<FileInfo>, Vec<Option<DiskStore>>)> {
        let disks = self.disks.read().await;

        let disks = disks.clone();

        let vid = opts.version_id.clone().unwrap_or_default();

        // TODO: 优化并发 可用数量中断
        let (parts_metadata, errs) = Self::read_all_fileinfo(&disks, "", bucket, object, vid.as_str(), read_data, false).await;
        // warn!("get_object_fileinfo parts_metadata {:?}", &parts_metadata);
        // warn!("get_object_fileinfo {}/{} errs {:?}", bucket, object, &errs);

        let _min_disks = self.set_drive_count - self.default_parity_count;

        let (read_quorum, _) = Self::object_quorum_from_meta(&parts_metadata, &errs, self.default_parity_count)?;

        if let Some(err) = reduce_read_quorum_errs(&errs, object_op_ignored_errs().as_ref(), read_quorum as usize) {
            error!(
                "reduce_read_quorum_errs disks: {} read_quorum {} \n {:?}",
                disks.len(),
                read_quorum,
                &errs
            );
            return Err(err);
        }

        let (op_online_disks, mot_time, etag) = Self::list_online_disks(&disks, &parts_metadata, &errs, read_quorum as usize);

        let fi = Self::pick_valid_fileinfo(&parts_metadata, mot_time, etag, read_quorum as usize)?;
        // debug!("get_object_fileinfo pick fi {:?}", &fi);

        // let online_disks: Vec<Option<DiskStore>> = op_online_disks.iter().filter(|v| v.is_some()).cloned().collect();

        Ok((fi, parts_metadata, op_online_disks))
    }

    #[allow(clippy::too_many_arguments)]
    async fn get_object_with_fileinfo(
        // &self,
        bucket: &str,
        object: &str,
        offset: i64,
        length: i64,
        writer: &mut DuplexStream,
        fi: FileInfo,
        files: Vec<FileInfo>,
        disks: &[Option<DiskStore>],
    ) -> Result<()> {
        let (disks, files) = Self::shuffle_disks_and_parts_metadata_by_index(disks, &files, &fi);

        let total_size = fi.size as i64;

        let length = {
            if length < 0 {
                total_size - offset
            } else {
                length
            }
        };

        if offset > total_size || offset + length > total_size {
            return Err(Error::msg("offset out of range"));
        }

        let (part_index, mut part_offset) = fi.to_part_offset(offset)?;

        // debug!(
        //     "get_object_with_fileinfo start offset:{}, part_index:{},part_offset:{}",
        //     offset, part_index, part_offset
        // );

        let mut end_offset = offset;
        if length > 0 {
            end_offset += length - 1
        }

        let (last_part_index, _) = fi.to_part_offset(end_offset)?;

        // debug!(
        //     "get_object_with_fileinfo end offset:{}, part_index:{},part_offset:{}",
        //     end_offset, last_part_index, 0
        // );

        let erasure = Erasure::new(fi.erasure.data_blocks, fi.erasure.parity_blocks, fi.erasure.block_size);

        let mut total_readed: i64 = 0;
        for i in part_index..=last_part_index {
            if total_readed == length {
                break;
            }

            let part_number = fi.parts[i].number;
            let part_size = fi.parts[i].size;
            let mut part_length = part_size - part_offset as usize;
            if part_length > (length - total_readed) as usize {
                part_length = (length - total_readed) as usize
            }

            let till_offset = erasure.shard_file_offset(part_offset.try_into().unwrap(), part_length, part_size);
            let mut readers = Vec::with_capacity(disks.len());
            for (idx, disk_op) in disks.iter().enumerate() {
                // debug!("read part_path {}", &part_path);

                if let Some(disk) = disk_op {
                    let filereader = {
                        if let Some(ref data) = files[idx].data {
                            FileReader::Buffer(BufferReader::new(data.clone()))
                        } else {
                            let disk = disk.clone();
                            let part_path =
                                format!("{}/{}/part.{}", object, files[idx].data_dir.unwrap_or(Uuid::nil()), part_number);

                            disk.read_file(bucket, &part_path).await?
                        }
                    };
                    let checksum_info = files[idx].erasure.get_checksum_info(part_number);
                    let reader = new_bitrot_filereader(
                        filereader,
                        till_offset,
                        checksum_info.algorithm,
                        erasure.shard_size(erasure.block_size),
                    );
                    readers.push(Some(reader));
                } else {
                    readers.push(None)
                }
            }

            // debug!(
            //     "read part {} part_offset {},part_length {},part_size {}  ",
            //     part_number, part_offset, part_length, part_size
            // );
            let _n = erasure
                .decode(writer, readers, part_offset as usize, part_length, part_size)
                .await?;

            // debug!("ec decode {} writed size {}", part_number, n);

            total_readed += part_length as i64;
            part_offset = 0;
        }

        // debug!("read end");

        Ok(())
    }

    async fn update_object_meta(&self, bucket: &str, object: &str, fi: FileInfo, disks: &[Option<DiskStore>]) -> Result<()> {
        self.update_object_meta_with_opts(bucket, object, fi, disks, &UpdateMetadataOpts::default())
            .await
    }
    async fn update_object_meta_with_opts(
        &self,
        bucket: &str,
        object: &str,
        fi: FileInfo,
        disks: &[Option<DiskStore>],
        opts: &UpdateMetadataOpts,
    ) -> Result<()> {
        if fi.metadata.is_none() {
            return Ok(());
        }

        let mut futures = Vec::with_capacity(disks.len());

        let mut errs = Vec::with_capacity(disks.len());

        for disk in disks.iter() {
            let fi = fi.clone();
            futures.push(async move {
                if let Some(disk) = disk {
                    disk.update_metadata(bucket, object, fi, opts).await
                } else {
                    Err(Error::new(DiskError::DiskNotFound))
                }
            })
        }

        let results = join_all(futures).await;
        for result in results {
            match result {
                Ok(_) => {
                    errs.push(None);
                }
                Err(e) => {
                    errs.push(Some(e));
                }
            }
        }

        if let Some(err) =
            reduce_write_quorum_errs(&errs, &object_op_ignored_errs(), fi.write_quorum(self.default_write_quorum()))
        {
            return Err(err);
        }

        Ok(())
    }
    pub async fn list_and_heal(&self, bucket: &str, prefix: &str, opts: &HealOpts, heal_entry: HealEntryFn) -> Result<()> {
        let bucket = bucket.to_string();
        let (disks, _) = self.get_online_disk_with_healing(false).await?;
        if disks.is_empty() {
            return Err(Error::from_string("listAndHeal: No non-healing drives found"));
        }

        let expected_disks = disks.len() / 2 + 1;
        let fallback_disks = &disks[expected_disks..];
        let disks = &disks[..expected_disks];
        let resolver = MetadataResolutionParams {
            dir_quorum: 1,
            obj_quorum: 1,
            bucket: bucket.clone(),
            strict: false,
            ..Default::default()
        };
        let path = Path::new(prefix).parent().map_or("", |p| p.to_str().unwrap());
        let filter_prefix = prefix.trim_start_matches(path).trim_matches('/');
        let opts_clone = *opts;
        let bucket_agreed = bucket.clone();
        let bucket_partial = bucket.to_string();
        let (tx, rx) = broadcast::channel(1);
        let tx_agreed = tx.clone();
        let tx_partial = tx.clone();
        let func_agreed = heal_entry.clone();
        let func_partial = heal_entry.clone();
        let lopts = ListPathRawOptions {
            disks: disks.to_vec(),
            fallback_disks: fallback_disks.to_vec(),
            bucket: bucket.to_string(),
            path: path.to_string(),
            filter_prefix: filter_prefix.to_string(),
            recursice: true,
            forward_to: "".to_string(),
            min_disks: 1,
            report_not_found: false,
            per_disk_limit: 0,
            agreed: Some(Box::new(move |entry: MetaCacheEntry| {
                let heal_entry = func_agreed.clone();
                let tx_agreed = tx_agreed.clone();

                Box::pin({
                    let bucket_agreed = bucket_agreed.clone();
                    async move {
                        if heal_entry(bucket_agreed.clone(), entry.clone(), opts_clone.scan_mode)
                            .await
                            .is_err()
                        {
                            let _ = tx_agreed.send(true);
                        }
                    }
                })
            })),
            partial: Some(Box::new(move |entries: MetaCacheEntries, _: &[Option<Error>]| {
                let heal_entry = func_partial.clone();
                let tx_partial = tx_partial.clone();

                Box::pin({
                    let resolver_partial = resolver.clone();
                    let bucket_partial = bucket_partial.clone();
                    async move {
                        let entry = match entries.resolve(resolver_partial) {
                            Ok(Some(entry)) => entry,
                            _ => match entries.first_found() {
                                (Some(entry), _) => entry,
                                _ => return,
                            },
                        };

                        if heal_entry(bucket_partial.clone(), entry.clone(), opts_clone.scan_mode)
                            .await
                            .is_err()
                        {
                            let _ = tx_partial.send(true);
                        }
                    }
                })
            })),
            finished: None,
        };

        _ = list_path_raw(rx, lopts)
            .await
            .map_err(|err| Error::from_string(format!("listPathRaw returned {}: bucket: {}, path: {}", err, bucket, path)));
        Ok(())
    }

    async fn get_online_disk_with_healing(&self, incl_healing: bool) -> Result<(Vec<Option<DiskStore>>, bool)> {
        let (new_disks, _, healing) = self.get_online_disk_with_healing_and_info(incl_healing).await?;
        Ok((new_disks, healing > 0))
    }

    async fn get_online_disk_with_healing_and_info(
        &self,
        incl_healing: bool,
    ) -> Result<(Vec<Option<DiskStore>>, Vec<DiskInfo>, usize)> {
        let mut infos = vec![DiskInfo::default(); self.disks.read().await.len()];
        for (idx, disk) in self.disks.write().await.iter().enumerate() {
            if let Some(disk) = disk {
                match disk.disk_info(&DiskInfoOptions::default()).await {
                    Ok(disk_info) => infos[idx] = disk_info,
                    Err(err) => infos[idx].error = err.to_string(),
                }
            } else {
                infos[idx].error = "disk not found".to_string();
            }
        }

        let mut new_disks = Vec::new();
        let mut healing_disks = Vec::new();
        let mut scanning_disks = Vec::new();
        let mut new_infos = Vec::new();
        let mut healing_infos = Vec::new();
        let mut scanning_infos = Vec::new();
        let mut healing = 0;

        infos.iter().zip(self.disks.write().await.iter()).for_each(|(info, disk)| {
            if info.error.is_empty() {
                if info.healing {
                    healing += 1;
                    if incl_healing {
                        healing_disks.push(disk.clone());
                        healing_infos.push(info.clone());
                    }
                } else if !info.scanning {
                    new_disks.push(disk.clone());
                    new_infos.push(info.clone());
                } else {
                    scanning_disks.push(disk.clone());
                    scanning_infos.push(info.clone());
                }
            }
        });

        // Prefer non-scanning disks over disks which are currently being scanned.
        new_disks.extend(scanning_disks);
        new_infos.extend(scanning_infos);

        // Then add healing disks.
        new_disks.extend(healing_disks);
        new_infos.extend(healing_infos);

        Ok((new_disks, new_infos, healing))
    }

    async fn heal_object(
        &self,
        bucket: &str,
        object: &str,
        version_id: &str,
        opts: &HealOpts,
    ) -> Result<(HealResultItem, Option<Error>)> {
        info!("SetDisks heal_object");
        let mut result = HealResultItem {
            heal_item_type: HEAL_ITEM_OBJECT.to_string(),
            bucket: bucket.to_string(),
            object: object.to_string(),
            version_id: version_id.to_string(),
            disk_count: self.disks.read().await.len(),
            ..Default::default()
        };

        if !opts.no_lock {
            // TODO: locker
        }

        let version_id_op = {
            if version_id.is_empty() {
                None
            } else {
                Some(version_id.to_string())
            }
        };

        let disks = { self.disks.read().await.clone() };

        let (mut parts_metadata, errs) = Self::read_all_fileinfo(&disks, "", bucket, object, version_id, true, true).await;
        if is_all_not_found(&errs) {
            warn!(
                "heal_object failed, all obj part not found, bucket: {}, obj: {}, version_id: {}",
                bucket, object, version_id
            );
            let err = if !version_id.is_empty() {
                Error::new(DiskError::FileVersionNotFound)
            } else {
                Error::new(DiskError::FileNotFound)
            };
            // Nothing to do, file is already gone.
            return Ok((
                self.default_heal_result(FileInfo::default(), &errs, bucket, object, version_id)
                    .await,
                Some(err),
            ));
        }

        match Self::object_quorum_from_meta(&parts_metadata, &errs, self.default_parity_count) {
            Ok((read_quorum, _)) => {
                result.parity_blocks = result.disk_count - read_quorum as usize;
                result.data_blocks = read_quorum as usize;

                let ((online_disks, mod_time, etag), disk_len) = {
                    let disks = self.disks.read().await;
                    let disk_len = disks.len();
                    (Self::list_online_disks(&disks, &parts_metadata, &errs, read_quorum as usize), disk_len)
                };

                match Self::pick_valid_fileinfo(&parts_metadata, mod_time, etag, read_quorum as usize) {
                    Ok(lastest_meta) => {
                        let (available_disks, data_errs_by_disk, data_errs_by_part) = disks_with_all_parts(
                            &online_disks,
                            &mut parts_metadata,
                            &errs,
                            &lastest_meta,
                            bucket,
                            object,
                            opts.scan_mode,
                        )
                        .await?;

                        // info!(
                        //     "disks_with_all_parts: got available_disks: {:?}, data_errs_by_disk: {:?}, data_errs_by_part: {:?}, lastest_meta: {:?}",
                        //     available_disks, data_errs_by_disk, data_errs_by_part, lastest_meta
                        // );
                        let erasure = if !lastest_meta.deleted && !lastest_meta.is_remote() {
                            // Initialize erasure coding
                            Erasure::new(
                                lastest_meta.erasure.data_blocks,
                                lastest_meta.erasure.parity_blocks,
                                lastest_meta.erasure.block_size,
                            )
                        } else {
                            Erasure::default()
                        };

                        result.object_size = lastest_meta.to_object_info(bucket, object, true).get_actual_size()?;
                        // Loop to find number of disks with valid data, per-drive
                        // data state and a list of outdated disks on which data needs
                        // to be healed.
                        let mut outdate_disks = vec![None; disk_len];
                        let mut disks_to_heal_count = 0;

                        info!(
                            "errs: {:?}, data_errs_by_disk: {:?}, lastest_meta: {:?}",
                            errs, data_errs_by_disk, lastest_meta
                        );
                        for index in 0..available_disks.len() {
                            let (yes, reason) = should_heal_object_on_disk(
                                &errs[index],
                                &data_errs_by_disk[&index],
                                &parts_metadata[index],
                                &lastest_meta,
                            );
                            if yes {
                                outdate_disks[index] = disks[index].clone();
                                disks_to_heal_count += 1;
                            }

                            let drive_state = match reason {
                                Some(reason) => match reason.downcast_ref::<DiskError>() {
                                    Some(DiskError::DiskNotFound) => DRIVE_STATE_OFFLINE,
                                    Some(DiskError::FileNotFound)
                                    | Some(DiskError::FileVersionNotFound)
                                    | Some(DiskError::VolumeNotFound)
                                    | Some(DiskError::PartMissingOrCorrupt)
                                    | Some(DiskError::OutdatedXLMeta) => DRIVE_STATE_MISSING,
                                    _ => DRIVE_STATE_CORRUPT,
                                },
                                None => DRIVE_STATE_OK,
                            };
                            result.before.drives.push(HealDriveInfo {
                                uuid: "".to_string(),
                                endpoint: self.set_endpoints[index].to_string(),
                                state: drive_state.to_string(),
                            });

                            result.after.drives.push(HealDriveInfo {
                                uuid: "".to_string(),
                                endpoint: self.set_endpoints[index].to_string(),
                                state: drive_state.to_string(),
                            });
                        }

                        if is_all_not_found(&errs) {
                            warn!(
                                "heal_object failed, all obj part not found, bucket: {}, obj: {}, version_id: {}",
                                bucket, object, version_id
                            );
                            let err = if !version_id.is_empty() {
                                Error::new(DiskError::FileVersionNotFound)
                            } else {
                                Error::new(DiskError::FileNotFound)
                            };

                            return Ok((
                                self.default_heal_result(FileInfo::default(), &errs, bucket, object, version_id)
                                    .await,
                                Some(err),
                            ));
                        }

                        if disks_to_heal_count == 0 {
                            return Ok((result, None));
                        }

                        if opts.dry_run {
                            return Ok((result, None));
                        }

                        if !lastest_meta.deleted && disks_to_heal_count > lastest_meta.erasure.parity_blocks {
                            error!(
                                "file part corrupt too much, can not to fix, disks_to_heal_count: {}, parity_blocks: {}",
                                disks_to_heal_count, lastest_meta.erasure.parity_blocks
                            );
                            let mut t_errs = vec![None; errs.len()];
                            // Allow for dangling deletes, on versions that have DataDir missing etc.
                            // this would end up restoring the correct readable versions.
                            match self
                                .delete_if_dang_ling(
                                    bucket,
                                    object,
                                    &parts_metadata,
                                    &errs,
                                    &data_errs_by_part,
                                    ObjectOptions {
                                        version_id: version_id_op.clone(),
                                        ..Default::default()
                                    },
                                )
                                .await
                            {
                                Ok(m) => {
                                    let derr = if !version_id.is_empty() {
                                        Error::new(DiskError::FileVersionNotFound)
                                    } else {
                                        Error::new(DiskError::FileNotFound)
                                    };
                                    return Ok((
                                        self.default_heal_result(m, &t_errs, bucket, object, version_id).await,
                                        Some(derr),
                                    ));
                                }
                                Err(err) => {
                                    t_errs = vec![Some(err.clone()); errs.len()];
                                    return Ok((
                                        self.default_heal_result(FileInfo::default(), &t_errs, bucket, object, version_id)
                                            .await,
                                        Some(err),
                                    ));
                                }
                            }
                        }

                        if !lastest_meta.deleted && lastest_meta.erasure.distribution.len() != available_disks.len() {
                            let err_str = format!("unexpected file distribution ({:?}) from available disks ({:?}), looks like backend disks have been manually modified refusing to heal {}/{}({})",
                                lastest_meta.erasure.distribution, available_disks, bucket, object, version_id);
                            warn!(err_str);
                            let err = Error::from_string(err_str);
                            return Ok((
                                self.default_heal_result(lastest_meta, &errs, bucket, object, version_id)
                                    .await,
                                Some(err),
                            ));
                        }

                        let latest_disks = Self::shuffle_disks(&available_disks, &lastest_meta.erasure.distribution);
                        if !lastest_meta.deleted && lastest_meta.erasure.distribution.len() != outdate_disks.len() {
                            let err_str = format!("unexpected file distribution ({:?}) from outdated disks ({:?}), looks like backend disks have been manually modified refusing to heal {}/{}({})",
                                lastest_meta.erasure.distribution, outdate_disks, bucket, object, version_id);
                            warn!(err_str);
                            let err = Error::from_string(err_str);
                            return Ok((
                                self.default_heal_result(lastest_meta, &errs, bucket, object, version_id)
                                    .await,
                                Some(err),
                            ));
                        }

                        if !lastest_meta.deleted && lastest_meta.erasure.distribution.len() != parts_metadata.len() {
                            let err_str = format!("unexpected file distribution ({:?}) from metadata entries ({:?}), looks like backend disks have been manually modified refusing to heal {}/{}({})",
                                lastest_meta.erasure.distribution, parts_metadata.len(), bucket, object, version_id);
                            warn!(err_str);
                            let err = Error::from_string(err_str);
                            return Ok((
                                self.default_heal_result(lastest_meta, &errs, bucket, object, version_id)
                                    .await,
                                Some(err),
                            ));
                        }

                        let out_dated_disks = Self::shuffle_disks(&outdate_disks, &lastest_meta.erasure.distribution);
                        let mut parts_metadata =
                            Self::shuffle_parts_metadata(&parts_metadata, &lastest_meta.erasure.distribution);
                        let mut copy_parts_metadata = vec![None; parts_metadata.len()];
                        for (index, disk) in latest_disks.iter().enumerate() {
                            if disk.is_some() {
                                copy_parts_metadata[index] = Some(parts_metadata[index].clone());
                            }
                        }

                        let clean_file_info = |fi: &FileInfo| -> FileInfo {
                            let mut nfi = fi.clone();
                            if !nfi.is_remote() {
                                nfi.data = None;
                                nfi.erasure.index = 0;
                                nfi.erasure.checksums = Vec::new();
                            }
                            nfi
                        };
                        for (index, disk) in out_dated_disks.iter().enumerate() {
                            if disk.is_some() {
                                // Make sure to write the FileInfo information
                                // that is expected to be in quorum.
                                parts_metadata[index] = clean_file_info(&lastest_meta);
                            }
                        }

                        // We write at temporary location and then rename to final location.
                        let tmp_id = Uuid::new_v4().to_string();
                        let src_data_dir = lastest_meta.data_dir.unwrap().to_string();
                        let dst_data_dir = lastest_meta.data_dir.unwrap();

                        if !lastest_meta.deleted && !lastest_meta.is_remote() {
                            let erasure_info = lastest_meta.erasure;
                            for part in lastest_meta.parts.iter() {
                                let till_offset = erasure.shard_file_offset(0, part.size, part.size);
                                let checksum_algo = erasure_info.get_checksum_info(part.number).algorithm;
                                let mut readers = Vec::with_capacity(latest_disks.len());
                                let mut writers = Vec::with_capacity(out_dated_disks.len());
                                let mut prefer = vec![false; latest_disks.len()];
                                for (index, disk) in latest_disks.iter().enumerate() {
                                    if let (Some(disk), Some(metadata)) = (disk, &copy_parts_metadata[index]) {
                                        let filereader = {
                                            if let Some(ref data) = metadata.data {
                                                FileReader::Buffer(BufferReader::new(data.clone()))
                                            } else {
                                                let disk = disk.clone();
                                                let part_path = format!("{}/{}/part.{}", object, src_data_dir, part.number);

                                                disk.read_file(bucket, &part_path).await?
                                            }
                                        };
                                        let reader = new_bitrot_filereader(
                                            filereader,
                                            till_offset,
                                            checksum_algo.clone(),
                                            erasure.shard_size(erasure.block_size),
                                        );
                                        readers.push(Some(reader));
                                        prefer[index] = disk.host_name().is_empty();
                                    } else {
                                        readers.push(None);
                                    }
                                }

                                let is_inline_buffer = {
                                    if let Some(sc) = GLOBAL_StorageClass.get() {
                                        sc.should_inline(erasure.shard_file_size(lastest_meta.size), false)
                                    } else {
                                        false
                                    }
                                };

                                for disk in out_dated_disks.iter() {
                                    if let Some(disk) = disk {
                                        let filewriter = {
                                            if is_inline_buffer {
                                                FileWriter::Buffer(BufferWriter::new(Vec::new()))
                                            } else {
                                                let disk = disk.clone();
                                                let part_path = format!("{}/{}/part.{}", tmp_id, dst_data_dir, part.number);
                                                disk.create_file("", RUSTFS_META_TMP_BUCKET, &part_path, 0).await?
                                            }
                                        };

                                        let writer = new_bitrot_filewriter(
                                            filewriter,
                                            DEFAULT_BITROT_ALGO,
                                            erasure.shard_size(erasure.block_size),
                                        );

                                        writers.push(Some(writer));
                                    } else {
                                        writers.push(None);
                                    }
                                }

                                // Heal each part. erasure.Heal() will write the healed
                                // part to .rustfs/tmp/uuid/ which needs to be renamed
                                // later to the final location.
                                erasure.heal(&mut writers, readers, part.size, &prefer).await?;
                                close_bitrot_writers(&mut writers).await?;

                                for (index, disk) in out_dated_disks.iter().enumerate() {
                                    if disk.is_none() {
                                        continue;
                                    }

                                    if writers[index].is_none() {
                                        outdate_disks[index] = None;
                                        disks_to_heal_count -= 1;
                                        continue;
                                    }

                                    parts_metadata[index].data_dir = Some(dst_data_dir);
                                    parts_metadata[index].add_object_part(
                                        part.number,
                                        part.etag.clone(),
                                        part.size,
                                        part.mod_time,
                                        part.actual_size,
                                    );
                                    if is_inline_buffer {
                                        if let Some(ref writer) = writers[index] {
                                            if let Some(w) = writer.as_any().downcast_ref::<BitrotFileWriter>() {
                                                if let FileWriter::Buffer(buffer_writer) = w.writer() {
                                                    parts_metadata[index].data = Some(buffer_writer.as_ref().to_vec());
                                                }
                                            }
                                        }
                                        parts_metadata[index].set_inline_data();
                                    } else {
                                        parts_metadata[index].data = None;
                                    }
                                }

                                if disks_to_heal_count == 0 {
                                    return Ok((
                                        result,
                                        Some(Error::from_string(format!(
                                            "all drives had write errors, unable to heal {}/{}",
                                            bucket, object
                                        ))),
                                    ));
                                }
                            }
                        }

                        // Rename from tmp location to the actual location.
                        for (index, disk) in out_dated_disks.iter().enumerate() {
                            if let Some(disk) = disk {
                                // record the index of the updated disks
                                parts_metadata[index].erasure.index = index + 1;
                                // Attempt a rename now from healed data to final location.
                                parts_metadata[index].set_healing();

                                info!(
                                    "rename temp data, src_volume: {}, src_path: {}, dst_volume: {}, dst_path: {}",
                                    RUSTFS_META_TMP_BUCKET, tmp_id, bucket, object
                                );
                                if let Err(err) = disk
                                    .rename_data(RUSTFS_META_TMP_BUCKET, &tmp_id, parts_metadata[index].clone(), bucket, object)
                                    .await
                                {
                                    info!("rename temp data err: {}", err.to_string());
                                    // self.delete_all(RUSTFS_META_TMP_BUCKET, &tmp_id).await?;
                                    return Ok((result, Some(err)));
                                }

                                info!("remove temp object, volume: {}, path: {}", RUSTFS_META_TMP_BUCKET, tmp_id);
                                self.delete_all(RUSTFS_META_TMP_BUCKET, &tmp_id).await?;
                                if parts_metadata[index].is_remote() {
                                    let rm_data_dir = parts_metadata[index].data_dir.unwrap().to_string();
                                    let d_path = Path::new(&encode_dir_object(object)).join(rm_data_dir);
                                    disk.delete(
                                        bucket,
                                        d_path.to_str().unwrap(),
                                        DeleteOptions {
                                            immediate: true,
                                            recursive: true,
                                            ..Default::default()
                                        },
                                    )
                                    .await?;
                                }

                                for (i, v) in result.before.drives.iter().enumerate() {
                                    if v.endpoint == disk.endpoint().to_string() {
                                        result.after.drives[i].state = DRIVE_STATE_OK.to_string();
                                    }
                                }
                            }
                        }

                        Ok((result, None))
                    }
                    Err(err) => {
                        warn!("heal_object can not pick valid file info");
                        Ok((result, Some(err)))
                    }
                }
            }
            Err(err) => {
                warn!("object_quorum_from_meta failed, err: {}", err.to_string());
                let data_errs_by_part = HashMap::new();
                match self
                    .delete_if_dang_ling(
                        bucket,
                        object,
                        &parts_metadata,
                        &errs,
                        &data_errs_by_part,
                        ObjectOptions {
                            version_id: version_id_op.clone(),
                            ..Default::default()
                        },
                    )
                    .await
                {
                    Ok(m) => {
                        let err = if !version_id.is_empty() {
                            Error::new(DiskError::FileVersionNotFound)
                        } else {
                            Error::new(DiskError::FileNotFound)
                        };
                        Ok((self.default_heal_result(m, &errs, bucket, object, version_id).await, Some(err)))
                    }
                    Err(_) => Ok((
                        self.default_heal_result(FileInfo::default(), &errs, bucket, object, version_id)
                            .await,
                        Some(err),
                    )),
                }
            }
        }
    }

    async fn heal_object_dir(
        &self,
        bucket: &str,
        object: &str,
        dry_run: bool,
        remove: bool,
    ) -> Result<(HealResultItem, Option<Error>)> {
        let disks = {
            let disks = self.disks.read().await;
            disks.clone()
        };
        let mut result = HealResultItem {
            heal_item_type: HEAL_ITEM_OBJECT.to_string(),
            bucket: bucket.to_string(),
            object: object.to_string(),
            disk_count: self.disks.read().await.len(),
            parity_blocks: self.default_parity_count,
            data_blocks: disks.len() - self.default_parity_count,
            object_size: 0,
            ..Default::default()
        };

        result.before.drives = vec![HealDriveInfo::default(); disks.len()];
        result.after.drives = vec![HealDriveInfo::default(); disks.len()];

        let errs = stat_all_dirs(&disks, bucket, object).await;
        let dang_ling_object = is_object_dir_dang_ling(&errs);
        if dang_ling_object && !dry_run && remove {
            let mut futures = Vec::with_capacity(disks.len());
            for disk in disks.iter().flatten() {
                let disk = disk.clone();
                let bucket = bucket.to_string();
                let object = object.to_string();
                futures.push(tokio::spawn(async move {
                    let _ = disk
                        .delete(
                            &bucket,
                            &object,
                            DeleteOptions {
                                recursive: false,
                                immediate: false,
                                ..Default::default()
                            },
                        )
                        .await;
                }));
            }

            // ignore errors
            let _ = join_all(futures).await;
        }

        for (err, drive) in errs.iter().zip(self.set_endpoints.iter()) {
            let endpoint = drive.to_string();
            let drive_state = match err {
                Some(err) => match err.downcast_ref::<DiskError>() {
                    Some(DiskError::DiskNotFound) => DRIVE_STATE_OFFLINE,
                    Some(DiskError::FileNotFound) | Some(DiskError::VolumeNotFound) => DRIVE_STATE_MISSING,
                    _ => DRIVE_STATE_CORRUPT,
                },
                None => DRIVE_STATE_OK,
            };
            result.before.drives.push(HealDriveInfo {
                uuid: "".to_string(),
                endpoint: endpoint.clone(),
                state: drive_state.to_string(),
            });

            result.after.drives.push(HealDriveInfo {
                uuid: "".to_string(),
                endpoint,
                state: drive_state.to_string(),
            });
        }

        if dang_ling_object || is_all_not_found(&errs) {
            return Ok((result, Some(Error::new(DiskError::FileNotFound))));
        }

        if dry_run {
            // Quit without try to heal the object dir
            return Ok((result, None));
        }
        for (index, (err, disk)) in errs.iter().zip(disks.iter()).enumerate() {
            if let (Some(err), Some(disk)) = (err, disk) {
                match err.downcast_ref::<DiskError>() {
                    Some(DiskError::VolumeNotFound) | Some(DiskError::FileNotFound) => {
                        let vol_path = Path::new(bucket).join(object);
                        let drive_state = match disk.make_volume(vol_path.to_str().unwrap()).await {
                            Ok(_) => DRIVE_STATE_OK,
                            Err(merr) => match merr.downcast_ref::<DiskError>() {
                                Some(DiskError::VolumeExists) => DRIVE_STATE_OK,
                                Some(DiskError::DiskNotFound) => DRIVE_STATE_OFFLINE,
                                _ => DRIVE_STATE_CORRUPT,
                            },
                        };
                        result.after.drives[index].state = drive_state.to_string();
                    }
                    _ => {}
                }
            }
        }

        Ok((result, None))
    }

    async fn default_heal_result(
        &self,
        lfi: FileInfo,
        errs: &[Option<Error>],
        bucket: &str,
        object: &str,
        version_id: &str,
    ) -> HealResultItem {
        let disk_len = { self.disks.read().await.len() };
        let mut result = HealResultItem {
            heal_item_type: HEAL_ITEM_OBJECT.to_string(),
            bucket: bucket.to_string(),
            object: object.to_string(),
            object_size: lfi.size,
            version_id: version_id.to_string(),
            disk_count: disk_len,
            ..Default::default()
        };

        if lfi.is_valid() {
            result.parity_blocks = lfi.erasure.parity_blocks;
        } else {
            result.parity_blocks = self.default_parity_count;
        }

        result.data_blocks = disk_len - result.parity_blocks;

        for (index, disk) in self.disks.read().await.iter().enumerate() {
            if disk.is_none() {
                result.before.drives.push(HealDriveInfo {
                    uuid: "".to_string(),
                    endpoint: self.set_endpoints[index].to_string(),
                    state: DRIVE_STATE_OFFLINE.to_string(),
                });

                result.after.drives.push(HealDriveInfo {
                    uuid: "".to_string(),
                    endpoint: self.set_endpoints[index].to_string(),
                    state: DRIVE_STATE_OFFLINE.to_string(),
                });
            }

            let mut drive_state = DRIVE_STATE_CORRUPT;
            if let Some(err) = &errs[index] {
                if let Some(DiskError::FileNotFound | DiskError::VolumeNotFound) = err.downcast_ref::<DiskError>() {
                    drive_state = DRIVE_STATE_MISSING;
                }
            } else {
                drive_state = DRIVE_STATE_OK;
            }

            result.before.drives.push(HealDriveInfo {
                uuid: "".to_string(),
                endpoint: self.set_endpoints[index].to_string(),
                state: drive_state.to_string(),
            });
            result.after.drives.push(HealDriveInfo {
                uuid: "".to_string(),
                endpoint: self.set_endpoints[index].to_string(),
                state: drive_state.to_string(),
            });
        }
        result
    }

    async fn delete_if_dang_ling(
        &self,
        bucket: &str,
        object: &str,
        meta_arr: &[FileInfo],
        errs: &[Option<Error>],
        data_errs_by_part: &HashMap<usize, Vec<usize>>,
        opts: ObjectOptions,
    ) -> Result<FileInfo> {
        if let Ok(m) = is_object_dang_ling(meta_arr, errs, data_errs_by_part) {
            let mut tags = HashMap::new();
            tags.insert("set", self.set_index.to_string());
            tags.insert("pool", self.pool_index.to_string());
            tags.insert("merrs", join_errs(errs));
            tags.insert("derrs", format!("{:?}", data_errs_by_part));
            if m.is_valid() {
                tags.insert("sz", m.size.to_string());
                tags.insert(
                    "mt",
                    m.mod_time
                        .as_ref()
                        .map_or(String::new(), |mod_time| mod_time.unix_timestamp().to_string()),
                );
                tags.insert("d:p", format!("{}:{}", m.erasure.data_blocks, m.erasure.parity_blocks));
            } else {
                tags.insert("invalid", "1".to_string());
                tags.insert(
                    "d:p",
                    format!("{}:{}", self.set_drive_count - self.default_parity_count, self.default_parity_count),
                );
            }
            let mut offline = 0;
            for (i, err) in errs.iter().enumerate() {
                let mut found = false;
                if let Some(err) = err {
                    if let Some(DiskError::DiskNotFound) = err.downcast_ref::<DiskError>() {
                        found = true;
                    }
                }
                for p in data_errs_by_part {
                    if let Some(v) = p.1.get(i) {
                        if *v == CHECK_PART_DISK_NOT_FOUND {
                            found = true;
                            break;
                        }
                    }
                }

                if found {
                    offline += 1;
                }
            }

            if offline > 0 {
                tags.insert("offline", offline.to_string());
            }

            // TODO: audit
            let mut fi = FileInfo::default();
            if let Some(ref version_id) = opts.version_id {
                fi.version_id = Uuid::parse_str(version_id).ok();
            }
            // TODO: tier
            for disk in self.disks.read().await.iter().flatten() {
                let _ = disk
                    .delete_version(bucket, object, fi.clone(), false, DeleteOptions::default())
                    .await;
            }
            Ok(m)
        } else {
            Err(Error::new(ErasureError::ErasureReadQuorum))
        }
    }

    pub async fn ns_scanner(
        &self,
        buckets: &[BucketInfo],
        want_cycle: u32,
        updates: Sender<DataUsageCache>,
        heal_scan_mode: HealScanMode,
    ) -> Result<()> {
        info!("ns_scanner");
        if buckets.is_empty() {
            return Ok(());
        }

        let (mut disks, healing) = self.get_online_disk_with_healing(false).await?;
        if disks.is_empty() {
            info!("data-scanner: all drives are offline or being healed, skipping scanner cycle");
            return Ok(());
        }

        let old_cache = DataUsageCache::load(self, DATA_USAGE_CACHE_NAME).await?;
        let mut cache = DataUsageCache {
            info: DataUsageCacheInfo {
                name: DATA_USAGE_ROOT.to_string(),
                next_cycle: old_cache.info.next_cycle,
                ..Default::default()
            },
            cache: HashMap::new(),
        };

        // Put all buckets into channel.
        let (bucket_tx, bucket_rx) = mpsc::channel(buckets.len());
        // Shuffle buckets to ensure total randomness of buckets, being scanned.
        // Otherwise same set of buckets get scanned across erasure sets always.
        // at any given point in time. This allows different buckets to be scanned
        // in different order per erasure set, this wider spread is needed when
        // there are lots of buckets with different order of objects in them.
        let permutes = {
            let mut rng = thread_rng();
            let mut permutes: Vec<usize> = (0..buckets.len()).collect();
            permutes.shuffle(&mut rng);
            permutes
        };
        // Add new buckets first
        for idx in permutes.iter() {
            let b = buckets[*idx].clone();
            match old_cache.find(&b.name) {
                Some(e) => {
                    cache.replace(&b.name, DATA_USAGE_ROOT, e);
                    let _ = bucket_tx.send(b).await;
                }
                None => {
                    let _ = bucket_tx.send(b).await;
                }
            }
        }

        let (buckets_results_tx, mut buckets_results_rx) = mpsc::channel::<DataUsageEntryInfo>(disks.len());
        let update_time = {
            let mut rng = thread_rng();
            Duration::from_secs(30) + Duration::from_secs_f64(10.0 * rng.gen_range(0.0..1.0))
        };
        let mut ticker = interval(update_time);
        let task = tokio::spawn(async move {
            let last_save = Some(SystemTime::now());
            let mut need_loop = true;
            while need_loop {
                select! {
                    _ = ticker.tick() => {
                        if !cache.info.last_update.eq(&last_save) {
                            let _ = cache.save(DATA_USAGE_CACHE_NAME).await;
                            let _ = updates.send(cache.clone()).await;
                        }
                    }
                    result = buckets_results_rx.recv() => {
                        match result {
                            Some(result) => {
                                cache.replace(&result.name, &result.parent, result.entry);
                                cache.info.last_update = Some(SystemTime::now());
                            },
                            None => {
                                need_loop = false;
                                cache.info.next_cycle = want_cycle;
                                cache.info.last_update = Some(SystemTime::now());
                                let _ = cache.save(DATA_USAGE_CACHE_NAME).await;
                                let _ = updates.send(cache.clone()).await;
                            }
                        }
                    }
                }
            }
        });
        // Restrict parallelism for disk usage scanner
        // upto GOMAXPROCS if GOMAXPROCS is < len(disks)
        let max_procs = num_cpus::get();
        if max_procs < disks.len() {
            disks = disks[0..max_procs].to_vec();
        }

        let mut futures = Vec::new();
        let bucket_rx = Arc::new(RwLock::new(bucket_rx));
        for disk in disks.iter() {
            let disk = match disk {
                Some(disk) => disk.clone(),
                None => continue,
            };
            let bucket_rx_clone = bucket_rx.clone();
            let buckets_results_tx_clone = buckets_results_tx.clone();
            futures.push(async move {
                loop {
                    match bucket_rx_clone.write().await.try_recv() {
                        Err(_) => return,
                        Ok(bucket_info) => {
                            let cache_name = Path::new(&bucket_info.name).join(DATA_USAGE_CACHE_NAME);
                            let mut cache = match DataUsageCache::load(self, &cache_name.to_string_lossy()).await {
                                Ok(cache) => cache,
                                Err(_) => continue,
                            };
                            if cache.info.name.is_empty() {
                                cache.info.name = bucket_info.name.clone();
                            }
                            cache.info.skip_healing = healing;
                            cache.info.next_cycle = want_cycle;
                            if cache.info.name != bucket_info.name {
                                cache.info = DataUsageCacheInfo {
                                    name: bucket_info.name,
                                    last_update: Some(SystemTime::now()),
                                    next_cycle: want_cycle,
                                    ..Default::default()
                                };
                            }
                            // Collect updates.
                            let (tx, mut rx) = mpsc::channel(1);
                            let buckets_results_tx_inner_clone = buckets_results_tx_clone.clone();
                            let name = cache.info.name.clone();
                            let task = tokio::spawn(async move {
                                loop {
                                    match rx.recv().await {
                                        Some(entry) => {
                                            let _ = buckets_results_tx_inner_clone
                                                .send(DataUsageEntryInfo {
                                                    name: name.clone(),
                                                    parent: DATA_USAGE_ROOT.to_string(),
                                                    entry,
                                                })
                                                .await;
                                        }
                                        None => return,
                                    }
                                }
                            });
                            // Calc usage
                            let before = cache.info.last_update;
                            let cache = match disk.clone().ns_scanner(&cache, tx, heal_scan_mode, None).await {
                                Ok(cache) => cache,
                                Err(_) => {
                                    if cache.info.last_update > before {
                                        let _ = cache.save(&cache_name.to_string_lossy()).await;
                                    }
                                    let _ = task.await;
                                    continue;
                                }
                            };
                            let mut root = DataUsageEntry::default();
                            if let Some(r) = cache.root() {
                                root = cache.flatten(&r);
                                if let Some(r) = &root.replication_stats {
                                    if r.empty() {
                                        root.replication_stats = None;
                                    }
                                }
                            }
                            let _ = buckets_results_tx_clone
                                .send(DataUsageEntryInfo {
                                    name: cache.info.name.clone(),
                                    parent: DATA_USAGE_ROOT.to_string(),
                                    entry: root,
                                })
                                .await;
                            let _ = task.await;
                            let _ = cache.save(&cache_name.to_string_lossy()).await;
                        }
                    }
                    info!("continue scanner");
                }
            });
        }
        info!("ns_scanner start");
        let _ = join_all(futures).await;
        drop(buckets_results_tx);
        info!("1");
        let _ = task.await;
        info!("ns_scanner completed");
        Ok(())
    }

    pub async fn heal_erasure_set(self: Arc<Self>, buckets: &[String], tracker: Arc<RwLock<HealingTracker>>) -> Result<()> {
        let (bg_seq, found) = GLOBAL_BackgroundHealState.get_heal_sequence_by_token(BG_HEALING_UUID).await;
        if !found {
            return Err(Error::from_string("no local healing sequence initialized, unable to heal the drive"));
        }
        let bg_seq = bg_seq.unwrap();
        let scan_mode = HEAL_NORMAL_SCAN;

        let tracker_defer = tracker.clone();
        let defer = async move {
            let mut w = tracker_defer.write().await;
            w.set_object("").await;
            w.set_bucket("").await;
            let _ = w.update().await;
        };

        for bucket in buckets.iter() {
            if let Err(err) = HealSequence::heal_bucket(bg_seq.clone(), bucket, true).await {
                info!("{}", err.to_string());
            }
        }

        let info = match tracker
            .read()
            .await
            .disk
            .as_ref()
            .unwrap()
            .disk_info(&DiskInfoOptions::default())
            .await
        {
            Ok(info) => info,
            Err(err) => {
                defer.await;
                return Err(Error::from_string(format!("unable to get disk information before healing it: {}", err)));
            }
        };
        let num_cores = num_cpus::get(); // 使用 num_cpus crate 获取核心数
        let mut num_healers: usize;

        if info.nr_requests as usize > num_cores {
            num_healers = num_cores / 4;
        } else {
            num_healers = (info.nr_requests / 4) as usize;
        }

        if num_healers < 4 {
            num_healers = 4;
        }

        let v = globalHealConfig.read().await.get_workers();
        if v > 0 {
            num_healers = v;
        }
        info!(
            "Healing drive '{}' - use {} parallel workers.",
            tracker.read().await.disk.as_ref().unwrap().to_string(),
            num_healers
        );

        let jt = Workers::new(num_healers).map_err(|err| Error::from_string(err.to_string()))?;

        let heal_entry_done = |name: String| HealEntryResult {
            entry_done: true,
            name,
            ..Default::default()
        };

        let heal_entry_success = |sz: usize| HealEntryResult {
            bytes: sz,
            success: true,
            ..Default::default()
        };

        let heal_entry_failure = |sz: usize| HealEntryResult {
            bytes: sz,
            ..Default::default()
        };

        let heal_entry_skipped = |sz: usize| HealEntryResult {
            bytes: sz,
            skipped: true,
            ..Default::default()
        };

        let (result_tx, mut result_rx) = mpsc::channel::<HealEntryResult>(1000);
        let tracker_task = tracker.clone();
        let task = tokio::spawn(async move {
            loop {
                match result_rx.recv().await {
                    Some(entry) => {
                        if entry.entry_done {
                            tracker_task.write().await.set_object(entry.name.as_str()).await;
                            if let Some(last_update) = tracker_task.read().await.get_last_update().await {
                                if SystemTime::now().duration_since(last_update).unwrap() > Duration::from_secs(60) {
                                    if let Err(err) = tracker_task.write().await.update().await {
                                        info!("tracker update failed, err: {}", err.to_string());
                                    }
                                }
                            }
                            continue;
                        }

                        tracker_task
                            .write()
                            .await
                            .update_progress(entry.success, entry.skipped, entry.bytes as u64)
                            .await;
                    }
                    None => {
                        if let Err(err) = tracker_task.write().await.update().await {
                            info!("tracker update failed, err: {}", err.to_string());
                        }
                        return;
                    }
                }
            }
        });

        let started = tracker.read().await.started;
        let mut ret_err = None;
        for bucket in buckets.iter() {
            if tracker.read().await.is_healed(bucket).await {
                continue;
            }

            let mut forward_to = "".to_string();
            let b = tracker.read().await.get_bucket().await;
            if b == *bucket {
                forward_to = tracker.read().await.get_object().await;
            }

            if !b.is_empty() {
                tracker.write().await.resume().await;
            }

            tracker.write().await.set_object("").await;
            tracker.write().await.set_bucket("").await;

            if let Err(err) = HealSequence::heal_bucket(bg_seq.clone(), bucket, true).await {
                info!("heal bucket failed: {}", err.to_string());
                ret_err = Some(err);
                continue;
            }

            // let vc: VersioningConfiguration;
            // let lc: BucketLifecycleConfiguration;
            // let lr: ObjectLockConfiguration;
            // let rcfg: ReplicationConfiguration;
            // if !is_rustfs_meta_bucket_name(bucket) {
            //     vc = match get_versioning_config(bucket).await {
            //         Ok((r, _)) => r,
            //         Err(err) => {
            //             ret_err = Some(err);
            //             info!("get versioning config failed, err: {}", err.to_string());
            //             continue;
            //         }
            //     };
            //     lc = match get_lifecycle_config(bucket).await {
            //         Ok((r, _)) => r,
            //         Err(err) => {
            //             ret_err = Some(err);
            //             info!("get lifecycle config failed, err: {}", err.to_string());
            //             continue;
            //         }
            //     };
            //     lr = match get_object_lock_config(bucket).await {
            //         Ok((r, _)) => r,
            //         Err(err) => {
            //             ret_err = Some(err);
            //             info!("get object lock config failed, err: {}", err.to_string());
            //             continue;
            //         }
            //     };
            //     rcfg = match get_replication_config(bucket).await {
            //         Ok((r, _)) => r,
            //         Err(err) => {
            //             ret_err = Some(err);
            //             info!("get replication config failed, err: {}", err.to_string());
            //             continue;
            //         }
            //     };
            // }
            let (mut disks, _, healing) = self.get_online_disk_with_healing_and_info(true).await?;
            if disks.len() == healing {
                info!("all drives are in healing state, aborting..");
                defer.await;
                return Ok(());
            }

            disks = disks[0..disks.len() - healing].to_vec();
            if disks.len() < self.set_drive_count / 2 {
                defer.await;
                return Err(Error::from_string(format!(
                    "not enough drives (found={}, healing={}, total={}) are available to heal `{}`",
                    disks.len(),
                    healing,
                    self.set_drive_count,
                    tracker.read().await.disk.as_ref().unwrap().to_string()
                )));
            }

            {
                let mut rng = thread_rng();

                // 随机洗牌
                disks.shuffle(&mut rng);
            }

            let expected_disk = disks.len() / 2 + 1;
            let fallback_disks = disks[expected_disk..].to_vec();
            disks = disks[..expected_disk].to_vec();

            //todo
            // let filter_life_cycle = |bucket: &str, object: &str, fi: FileInfo| {
            //     if lc.rules.is_empty() {
            //         return false;
            //     }
            //     // todo: versioning
            //     let versioned = false;
            //     let obj_info = fi.to_object_info(bucket, object, versioned);
            //
            // };

            let result_tx_send = result_tx.clone();
            let bg_seq_send = bg_seq.clone();
            let send = Box::new(move |result: HealEntryResult| {
                let result_tx_send = result_tx_send.clone();
                let bg_seq_send = bg_seq_send.clone();
                Box::pin(async move {
                    let _ = result_tx_send.send(result).await;
                    bg_seq_send.count_scanned(HEAL_ITEM_OBJECT.to_string()).await;
                    true
                })
            });

            let jt_clone = jt.clone();
            let self_clone = self.clone();
            let started_clone = started;
            let tracker_heal = tracker.clone();
            let bg_seq_clone = bg_seq.clone();
            let send_clone = send.clone();
            let heal_entry = Arc::new(move |bucket: String, entry: MetaCacheEntry| {
                let jt_clone = jt_clone.clone();
                let self_clone = self_clone.clone();
                let started = started_clone;
                let tracker_heal = tracker_heal.clone();
                let bg_seq = bg_seq_clone.clone();
                let send = send_clone.clone();
                Box::pin(async move {
                    let defer = async {
                        jt_clone.give().await;
                    };
                    if entry.name.is_empty() && entry.metadata.is_empty() {
                        defer.await;
                        return;
                    }
                    if entry.is_dir() {
                        defer.await;
                        return;
                    }
                    if bucket == RUSTFS_META_BUCKET
                        && (Pattern::new("buckets/*/.metacache/*")
                            .map(|p| p.matches(&entry.name))
                            .unwrap_or(false)
                            || Pattern::new("tmp/.trash/*").map(|p| p.matches(&entry.name)).unwrap_or(false)
                            || Pattern::new("multipart/*").map(|p| p.matches(&entry.name)).unwrap_or(false))
                    {
                        defer.await;
                        return;
                    }
                    let encoded_entry_name = encode_dir_object(entry.name.as_str());
                    let mut result: HealEntryResult;
                    let fivs = match entry.file_info_versions(bucket.as_str()) {
                        Ok(fivs) => fivs,
                        Err(err) => {
                            match self_clone
                                .heal_object(
                                    &bucket,
                                    &encoded_entry_name,
                                    "",
                                    &HealOpts {
                                        scan_mode,
                                        remove: HEAL_DELETE_DANGLING,
                                        ..Default::default()
                                    },
                                )
                                .await
                            {
                                Ok((res, None)) => {
                                    bg_seq.count_healed(HEAL_ITEM_OBJECT.to_string()).await;
                                    result = heal_entry_success(res.object_size);
                                }
                                Ok((_, Some(err))) => match err.downcast_ref() {
                                    Some(DiskError::FileNotFound) | Some(DiskError::FileVersionNotFound) => {
                                        defer.await;
                                        return;
                                    }
                                    _ => {
                                        result = heal_entry_failure(0);
                                        bg_seq.count_failed(HEAL_ITEM_OBJECT.to_string()).await;
                                        info!("unable to heal object {}/{}: {}", bucket, entry.name, err.to_string());
                                    }
                                },
                                Err(_) => {
                                    result = heal_entry_failure(0);
                                    bg_seq.count_failed(HEAL_ITEM_OBJECT.to_string()).await;
                                    info!("unable to heal object {}/{}: {}", bucket, entry.name, err.to_string());
                                }
                            }
                            send(result.clone()).await;
                            defer.await;
                            return;
                        }
                    };
                    let mut version_not_found = 0;
                    for version in fivs.versions.iter() {
                        if let (Some(started), Some(mod_time)) = (started, version.mod_time) {
                            if mod_time > started {
                                version_not_found += 1;
                                if send(heal_entry_skipped(version.size)).await {
                                    defer.await;
                                    return;
                                }
                                continue;
                            }
                        }

                        let mut version_healed = false;
                        match self_clone
                            .heal_object(
                                &bucket,
                                &encoded_entry_name,
                                version
                                    .version_id
                                    .as_ref()
                                    .map(|v| v.to_string())
                                    .unwrap_or("".to_string())
                                    .as_str(),
                                &HealOpts {
                                    scan_mode,
                                    remove: HEAL_DELETE_DANGLING,
                                    ..Default::default()
                                },
                            )
                            .await
                        {
                            Ok((res, None)) => {
                                if res.after.drives[tracker_heal.read().await.disk_index.unwrap()].state == DRIVE_STATE_OK {
                                    version_healed = true;
                                }
                            }
                            Ok((_, Some(err))) => match err.downcast_ref() {
                                Some(DiskError::FileNotFound) | Some(DiskError::FileVersionNotFound) => {
                                    version_not_found += 1;
                                    continue;
                                }
                                _ => {}
                            },
                            Err(_) => {}
                        }

                        if version_healed {
                            bg_seq.count_healed(HEAL_ITEM_OBJECT.to_string()).await;
                            result = heal_entry_success(version.size);
                        } else {
                            bg_seq.count_failed(HEAL_ITEM_OBJECT.to_string()).await;
                            result = heal_entry_failure(version.size);
                            match version.version_id {
                                Some(version_id) => {
                                    info!("unable to heal object {}/{}-v({})", bucket, version.name, version_id);
                                }
                                None => {
                                    info!("unable to heal object {}/{}", bucket, version.name);
                                }
                            }
                        }

                        if !send(result).await {
                            defer.await;
                            return;
                        }
                    }
                    if version_not_found == fivs.versions.len() {
                        defer.await;
                        return;
                    }
                    send(heal_entry_done(entry.name.clone())).await;
                    defer.await;
                })
            });
            let resolver = MetadataResolutionParams {
                dir_quorum: 1,
                obj_quorum: 1,
                bucket: bucket.clone(),
                ..Default::default()
            };
            let (_, rx) = broadcast::channel(1);
            let jt_agree = jt.clone();
            let jt_partial = jt.clone();
            let bucket_agree = bucket.clone();
            let bucket_partial = bucket.clone();
            let heal_entry_agree = heal_entry.clone();
            let heal_entry_partial = heal_entry.clone();
            if let Err(err) = list_path_raw(
                rx,
                ListPathRawOptions {
                    disks,
                    fallback_disks,
                    bucket: bucket.clone(),
                    recursice: true,
                    forward_to,
                    min_disks: 1,
                    report_not_found: false,
                    agreed: Some(Box::new(move |entry: MetaCacheEntry| {
                        let jt = jt_agree.clone();
                        let bucket = bucket_agree.clone();
                        let heal_entry = heal_entry_agree.clone();
                        Box::pin(async move {
                            jt.take().await;
                            let bucket = bucket.clone();
                            tokio::spawn(async move {
                                heal_entry(bucket, entry).await;
                            });
                        })
                    })),
                    partial: Some(Box::new(move |entries: MetaCacheEntries, _: &[Option<Error>]| {
                        let jt = jt_partial.clone();
                        let bucket = bucket_partial.clone();
                        let heal_entry = heal_entry_partial.clone();
                        Box::pin({
                            let heal_entry = heal_entry.clone();
                            let resolver = resolver.clone();
                            async move {
                                let entry = if let Ok(Some(entry)) = entries.resolve(resolver) {
                                    entry
                                } else if let (Some(entry), _) = entries.first_found() {
                                    entry
                                } else {
                                    return;
                                };
                                jt.take().await;
                                let bucket = bucket.clone();
                                let heal_entry = heal_entry.clone();
                                tokio::spawn(async move {
                                    heal_entry(bucket, entry).await;
                                });
                            }
                        })
                    })),
                    finished: None,
                    ..Default::default()
                },
            )
            .await
            {
                ret_err = Some(err);
            }

            jt.wait().await;
            if let Some(err) = ret_err.as_ref() {
                info!("listing failed with: {} on bucket: {}", err.to_string(), bucket);
                continue;
            }
            tracker.write().await.bucket_done(bucket).await;
            if let Err(err) = tracker.write().await.update().await {
                info!("tracker update failed, err: {}", err.to_string());
            }
        }

        if let Some(err) = ret_err.as_ref() {
            return Err(err.clone());
        }
        if !tracker.read().await.queue_buckets.is_empty() {
            return Err(Error::from_string(format!(
                "not all buckets were healed: {:?}",
                tracker.read().await.queue_buckets
            )));
        }
        let _ = task.await;
        defer.await;
        Ok(())
    }
}

#[async_trait::async_trait]
impl ObjectIO for SetDisks {
    async fn get_object_reader(
        &self,
        bucket: &str,
        object: &str,
        _rs: HTTPRangeSpec,
        _h: HeaderMap,
        opts: &ObjectOptions,
    ) -> Result<GetObjectReader> {
        let (fi, files, disks) = self.get_object_fileinfo(bucket, object, opts, true).await?;
        let object_info = fi.to_object_info(bucket, object, opts.versioned || opts.version_suspended);

        if object_info.delete_marker {
            return Err(Error::new(DiskError::FileNotFound));
        }

        // if object_info.size == 0 {
        //     let empty_rd: Box<dyn AsyncRead> = Box::new(Bytes::new());

        //     return Ok(GetObjectReader {
        //         stream: empty_rd,
        //         object_info,
        //     });
        // }

        let rs = HTTPRangeSpec::from_object_info(&object_info, opts.part_number);
        let (offset, length) = rs.get_offset_length(object_info.size.try_into().unwrap())?;

        // debug!("get_object_reader offset:{}, length:{}", offset, length);

        let (rd, mut wd) = tokio::io::duplex(fi.erasure.block_size);
        // let disks = self.disks.read().await;

        // let disks = disks.clone();
        let bucket = String::from(bucket);
        let object = String::from(object);
        tokio::spawn(async move {
            if let Err(e) = Self::get_object_with_fileinfo(&bucket, &object, offset, length, &mut wd, fi, files, &disks).await {
                error!("get_object_with_fileinfo err {:?}", e);
            };
        });

        let read_stream = tokio_util::io::ReaderStream::new(rd);

        // let rd: Box<dyn AsyncRead> = Box::new(rd);

        let reader = GetObjectReader {
            stream: StreamingBlob::wrap(read_stream),
            object_info,
        };
        Ok(reader)
    }

    #[tracing::instrument(level = "debug", skip(self, data))]
    async fn put_object(&self, bucket: &str, object: &str, data: &mut PutObjReader, opts: &ObjectOptions) -> Result<ObjectInfo> {
        let disks = self.disks.read().await;

        let mut _ns = None;
        if !opts.no_lock {
            let paths = vec![object.to_string()];
            let ns_lock = new_nslock(
                Arc::clone(&self.ns_mutex),
                self.locker_owner.clone(),
                bucket.to_string(),
                paths,
                self.lockers.clone(),
            )
            .await;
            if !ns_lock
                .0
                .write()
                .await
                .get_lock(&Options {
                    timeout: Duration::from_secs(5),
                    retry_interval: Duration::from_secs(1),
                })
                .await
                .map_err(|err| Error::from_string(err.to_string()))?
            {
                return Err(Error::from_string("can not get lock. please retry".to_string()));
            }

            _ns = Some(ns_lock);
        }

        let mut user_defined = opts.user_defined.clone();

        let sc_parity_drives = {
            if let Some(sc) = GLOBAL_StorageClass.get() {
                let a = sc.get_parity_for_sc(
                    user_defined
                        .get(xhttp::AMZ_STORAGE_CLASS)
                        .cloned()
                        .unwrap_or_default()
                        .as_str(),
                );
                a
            } else {
                None
            }
        };

        let mut parity_drives = sc_parity_drives.unwrap_or(self.default_parity_count);
        if opts.max_parity {
            parity_drives = disks.len() / 2;
        }

        let data_drives = disks.len() - parity_drives;
        let mut write_quorum = data_drives;
        if data_drives == parity_drives {
            write_quorum += 1
        }

        let mut fi = FileInfo::new([bucket, object].join("/").as_str(), data_drives, parity_drives);

        fi.version_id = {
            if let Some(ref vid) = opts.version_id {
                Some(Uuid::parse_str(vid.as_str())?)
            } else {
                None
            }
        };

        if opts.versioned && fi.version_id.is_none() {
            fi.version_id = Some(Uuid::new_v4());
        }

        fi.data_dir = Some(Uuid::new_v4());

        let parts_metadata = vec![fi.clone(); disks.len()];

        let (shuffle_disks, mut parts_metadatas) = Self::shuffle_disks_and_parts_metadata(&disks, &parts_metadata, &fi);

        let mut writers = Vec::with_capacity(shuffle_disks.len());

        let tmp_dir = Uuid::new_v4().to_string();

        let tmp_object = format!("{}/{}/part.1", tmp_dir, fi.data_dir.unwrap());

        let mut erasure = Erasure::new(fi.erasure.data_blocks, fi.erasure.parity_blocks, fi.erasure.block_size);

        let is_inline_buffer = {
            if let Some(sc) = GLOBAL_StorageClass.get() {
                sc.should_inline(erasure.shard_file_size(data.content_length), opts.versioned)
            } else {
                false
            }
        };

        for disk_op in shuffle_disks.iter() {
            if let Some(disk) = disk_op {
                let filewriter = {
                    if is_inline_buffer {
                        FileWriter::Buffer(BufferWriter::new(Vec::new()))
                    } else {
                        let disk = disk.clone();

                        disk.create_file("", RUSTFS_META_TMP_BUCKET, &tmp_object, 0).await?
                    }
                };

                let writer = new_bitrot_filewriter(filewriter, DEFAULT_BITROT_ALGO, erasure.shard_size(erasure.block_size));

                writers.push(Some(writer));
            } else {
                writers.push(None);
            }
        }

        // TODO: etag from header
        let mut etag_stream = EtagReader::new(&mut data.stream, None, None);

        let w_size = erasure
            .encode(&mut etag_stream, &mut writers, data.content_length, write_quorum)
            .await?; // TODO: 出错，删除临时目录

        let etag = etag_stream.etag();
        //TODO: userDefined

        user_defined.insert("etag".to_owned(), etag.clone());

        if !user_defined.contains_key("content-type") {
            //  get content-type
        }

        if let Some(sc) = user_defined.get(xhttp::AMZ_STORAGE_CLASS) {
            if sc == storageclass::STANDARD {
                let _ = user_defined.remove(xhttp::AMZ_STORAGE_CLASS);
            }
        }

        let now = OffsetDateTime::now_utc();

        for (i, fi) in parts_metadatas.iter_mut().enumerate() {
            if is_inline_buffer {
                if let Some(ref writer) = writers[i] {
                    if let Some(w) = writer.as_any().downcast_ref::<BitrotFileWriter>() {
                        if let FileWriter::Buffer(buffer_writer) = w.writer() {
                            fi.data = Some(buffer_writer.as_ref().to_vec());
                        }
                    }
                }
            }

            fi.metadata = Some(user_defined.clone());
            fi.mod_time = Some(now);
            fi.size = w_size;
            fi.versioned = opts.versioned || opts.version_suspended;
            fi.add_object_part(1, Some(etag.clone()), w_size, fi.mod_time, w_size);

            fi.set_inline_data();

            // debug!("put_object fi {:?}", &fi)
        }

        let (online_disks, _, op_old_dir) = Self::rename_data(
            &shuffle_disks,
            RUSTFS_META_TMP_BUCKET,
            tmp_dir.as_str(),
            &parts_metadatas,
            bucket,
            object,
            write_quorum,
        )
        .await?;

        if let Some(old_dir) = op_old_dir {
            self.commit_rename_data_dir(&shuffle_disks, bucket, object, &old_dir.to_string(), write_quorum)
                .await?;
        }

        self.delete_all(RUSTFS_META_TMP_BUCKET, &tmp_dir).await?;

        // if let Some(mut locker) = ns {
        //     locker.un_lock().await.map_err(|err| Error::from_string(err.to_string()))?;
        // }

        for (i, op_disk) in online_disks.iter().enumerate() {
            if let Some(disk) = op_disk {
                if disk.is_online().await {
                    fi = parts_metadatas[i].clone();
                    break;
                }
            }
        }

        fi.is_latest = true;

        // TODO: version suport
        Ok(fi.to_object_info(bucket, object, opts.versioned || opts.version_suspended))
    }
}

#[async_trait::async_trait]
impl StorageAPI for SetDisks {
    async fn backend_info(&self) -> madmin::BackendInfo {
        unimplemented!()
    }
    async fn storage_info(&self) -> madmin::StorageInfo {
        let disks = self.get_disks_internal().await;

        get_storage_info(&disks, &self.set_endpoints).await
    }
    async fn local_storage_info(&self) -> madmin::StorageInfo {
        let disks = self.get_disks_internal().await;

        let mut local_disks: Vec<Option<Arc<crate::disk::Disk>>> = Vec::new();
        let mut local_endpoints = Vec::new();

        for (i, ep) in self.set_endpoints.iter().enumerate() {
            if ep.is_local {
                local_disks.push(disks[i].clone());
                local_endpoints.push(ep.clone());
            }
        }

        get_storage_info(&local_disks, &local_endpoints).await
    }
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

        // let mut del_fvers = Vec::with_capacity(objects.len());

        let mut vers_map: HashMap<&String, FileInfoVersions> = HashMap::new();

        for (i, dobj) in objects.iter().enumerate() {
            let mut vr = FileInfo {
                name: dobj.object_name.clone(),
                version_id: dobj.version_id,
                ..Default::default()
            };

            // 删除
            del_objects[i].object_name.clone_from(&vr.name);
            del_objects[i].version_id = vr.version_id.map(|v| v.to_string());

            if del_objects[i].version_id.is_none() {
                let (suspended, versioned) = (opts.version_suspended, opts.versioned);
                if suspended || versioned {
                    vr.mod_time = Some(OffsetDateTime::now_utc());
                    vr.deleted = true;
                    if versioned {
                        vr.version_id = Some(Uuid::new_v4());
                    }
                }
            }

            let v = {
                if vers_map.contains_key(&dobj.object_name) {
                    let val = vers_map.get_mut(&dobj.object_name).unwrap();
                    val.versions.push(vr.clone());
                    val.clone()
                } else {
                    FileInfoVersions {
                        name: vr.name.clone(),
                        versions: vec![vr.clone()],
                        ..Default::default()
                    }
                }
            };

            if vr.deleted {
                error!("delete marker {:?}", &vr);
                del_objects[i] = DeletedObject {
                    delete_marker: vr.deleted,
                    delete_marker_version_id: vr.version_id.map(|v| v.to_string()),
                    delete_marker_mtime: vr.mod_time,
                    object_name: vr.name.clone(),
                    ..Default::default()
                }
            } else {
                del_objects[i] = DeletedObject {
                    object_name: vr.name.clone(),
                    version_id: vr.version_id.map(|v| v.to_string()),
                    ..Default::default()
                }
            }

            vers_map.insert(&dobj.object_name, v);
        }

        let mut vers = Vec::with_capacity(vers_map.len());

        for (_, ver) in vers_map {
            vers.push(ver);
        }

        let disks = self.disks.read().await;

        let disks = disks.clone();

        let mut futures = Vec::with_capacity(disks.len());

        // let mut errors = Vec::with_capacity(disks.len());

        for disk in disks.iter() {
            let vers = vers.clone();
            futures.push(async move {
                if let Some(disk) = disk {
                    disk.delete_versions(bucket, vers, DeleteOptions::default()).await
                } else {
                    Err(Error::new(DiskError::DiskNotFound))
                }
            });
        }

        let results = join_all(futures).await;

        for errs in results.into_iter().flatten() {
            // TODO: handle err reduceWriteQuorumErrs
            for err in errs.iter() {
                warn!("result err {:?}", err);
            }
        }

        Ok((del_objects, del_errs))
    }
    async fn delete_object(&self, _bucket: &str, _object: &str, _opts: ObjectOptions) -> Result<ObjectInfo> {
        unimplemented!()
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
    async fn list_object_versions(
        &self,
        _bucket: &str,
        _prefix: &str,
        _marker: &str,
        _version_marker: &str,
        _delimiter: &str,
        _max_keys: i32,
    ) -> Result<ListObjectVersionsInfo> {
        unimplemented!()
    }
    async fn get_object_info(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<ObjectInfo> {
        let mut _ns = None;
        if !opts.no_lock {
            let paths = vec![object.to_string()];
            let ns_lock = new_nslock(
                Arc::clone(&self.ns_mutex),
                self.locker_owner.clone(),
                bucket.to_string(),
                paths,
                self.lockers.clone(),
            )
            .await;
            if !ns_lock
                .0
                .write()
                .await
                .get_lock(&Options {
                    timeout: Duration::from_secs(5),
                    retry_interval: Duration::from_secs(1),
                })
                .await
                .map_err(|err| Error::from_string(err.to_string()))?
            {
                return Err(Error::from_string("can not get lock. please retry".to_string()));
            }

            _ns = Some(ns_lock);
        }
        let (fi, _, _) = self.get_object_fileinfo(bucket, object, opts, false).await?;

        let oi = fi.to_object_info(bucket, object, opts.versioned || opts.version_suspended);

        Ok(oi)
    }

    async fn put_object_metadata(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<ObjectInfo> {
        // TODO: nslock

        let disks = self.get_disks_internal().await;

        let (metas, errs) = {
            if opts.version_id.is_some() {
                Self::read_all_fileinfo(
                    &disks,
                    "",
                    bucket,
                    object,
                    opts.version_id.as_ref().unwrap().to_string().as_str(),
                    false,
                    false,
                )
                .await
            } else {
                Self::read_all_xl(&disks, bucket, object, false, false).await
            }
        };

        let read_quorum = match Self::object_quorum_from_meta(&metas, &errs, self.default_parity_count) {
            Ok((res, _)) => res,
            Err(mut err) => {
                if ErasureError::ErasureReadQuorum.is(&err)
                    && !bucket.starts_with(RUSTFS_META_BUCKET)
                    && self
                        .delete_if_dang_ling(bucket, object, &metas, &errs, &HashMap::new(), opts.clone())
                        .await
                        .is_ok()
                {
                    if opts.version_id.is_some() {
                        err = Error::new(DiskError::FileVersionNotFound)
                    } else {
                        err = Error::new(DiskError::FileNotFound)
                    }
                }
                return Err(to_object_err(err, vec![bucket, object]));
            }
        };

        let read_quorum = read_quorum as usize;

        let (online_disks, mod_time, etag) = Self::list_online_disks(&disks, &metas, &errs, read_quorum);

        let mut fi =
            Self::pick_valid_fileinfo(&metas, mod_time, etag, read_quorum).map_err(|e| to_object_err(e, vec![bucket, object]))?;

        if fi.deleted {
            return Err(Error::new(StorageError::MethodNotAllowed));
        }

        let obj_info = fi.to_object_info(bucket, object, opts.versioned || opts.version_suspended);

        if let Some(ref mut metadata) = fi.metadata {
            for (k, v) in obj_info.user_defined {
                metadata.insert(k, v);
            }
            fi.metadata = Some(metadata.clone())
        } else {
            let mut metadata = HashMap::new();

            for (k, v) in obj_info.user_defined {
                metadata.insert(k, v);
            }

            fi.metadata = Some(metadata)
        }

        fi.mod_time = opts.mod_time;
        if let Some(ref version_id) = opts.version_id {
            fi.version_id = Uuid::parse_str(version_id).ok();
        }

        self.update_object_meta(bucket, object, fi.clone(), &online_disks)
            .await
            .map_err(|e| to_object_err(e, vec![bucket, object]))?;

        Ok(fi.to_object_info(bucket, object, opts.versioned || opts.version_suspended))
    }

    async fn get_object_tags(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<String> {
        let oi = self.get_object_info(bucket, object, opts).await?;
        Ok(oi.user_tags)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn put_object_tags(&self, bucket: &str, object: &str, tags: &str, opts: &ObjectOptions) -> Result<ObjectInfo> {
        let (mut fi, _, disks) = self.get_object_fileinfo(bucket, object, opts, false).await?;

        if let Some(ref mut metadata) = fi.metadata {
            metadata.insert(xhttp::AMZ_OBJECT_TAGGING.to_owned(), tags.to_owned());
            fi.metadata = Some(metadata.clone())
        } else {
            let mut metadata = HashMap::new();
            metadata.insert(xhttp::AMZ_OBJECT_TAGGING.to_owned(), tags.to_owned());

            fi.metadata = Some(metadata)
        }

        // TODO: userdeefined

        self.update_object_meta(bucket, object, fi.clone(), disks.as_slice()).await?;

        // TODO: versioned
        Ok(fi.to_object_info(bucket, object, opts.versioned || opts.version_suspended))
    }
    async fn delete_object_tags(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<ObjectInfo> {
        self.put_object_tags(bucket, object, "", opts).await
    }

    async fn copy_object_part(
        &self,
        _src_bucket: &str,
        _src_object: &str,
        _dst_bucket: &str,
        _dst_object: &str,
        _upload_id: &str,
        _part_id: usize,
        _start_offset: i64,
        _length: i64,
        _src_info: &ObjectInfo,
        _src_opts: &ObjectOptions,
        _dst_opts: &ObjectOptions,
    ) -> Result<()> {
        unimplemented!()
    }

    async fn put_object_part(
        &self,
        bucket: &str,
        object: &str,
        upload_id: &str,
        part_id: usize,
        data: &mut PutObjReader,
        opts: &ObjectOptions,
    ) -> Result<PartInfo> {
        let upload_id_path = Self::get_upload_id_dir(bucket, object, upload_id);

        let (mut fi, _) = self.check_upload_id_exists(bucket, object, upload_id, true).await?;

        let write_quorum = fi.write_quorum(self.default_write_quorum());

        let disks = self.disks.read().await;

        let disks = disks.clone();
        let disks = Self::shuffle_disks(&disks, &fi.erasure.distribution);

        let part_suffix = format!("part.{}", part_id);
        let tmp_part = format!("{}x{}", Uuid::new_v4(), OffsetDateTime::now_utc().unix_timestamp());
        let tmp_part_path = format!("{}/{}", tmp_part, part_suffix);

        let mut writers = Vec::with_capacity(disks.len());
        let erasure = Erasure::new(fi.erasure.data_blocks, fi.erasure.parity_blocks, fi.erasure.block_size);

        for disk in disks.iter() {
            if let Some(disk) = disk {
                // let writer = disk.append_file(RUSTFS_META_TMP_BUCKET, &tmp_part_path).await?;
                let filewriter = disk
                    .create_file("", RUSTFS_META_TMP_BUCKET, &tmp_part_path, data.content_length)
                    .await?;
                let writer = new_bitrot_filewriter(filewriter, DEFAULT_BITROT_ALGO, erasure.shard_size(erasure.block_size));
                writers.push(Some(writer));
            } else {
                writers.push(None);
            }
        }

        let mut erasure = Erasure::new(fi.erasure.data_blocks, fi.erasure.parity_blocks, fi.erasure.block_size);

        let mut etag_stream = EtagReader::new(&mut data.stream, None, None);

        let w_size = erasure
            .encode(&mut etag_stream, &mut writers, data.content_length, write_quorum)
            .await?;

        let mut etag = etag_stream.etag();

        if let Some(ref tag) = opts.preserve_etag {
            etag = tag.clone();
        }

        let part_info = ObjectPartInfo {
            etag: Some(etag.clone()),
            number: part_id,
            size: w_size,
            mod_time: Some(OffsetDateTime::now_utc()),
            actual_size: data.content_length,
        };

        // debug!("put_object_part part_info {:?}", part_info);

        fi.parts = vec![part_info];

        let fi_buff = fi.marshal_msg()?;

        let part_path = format!("{}/{}/{}", upload_id_path, fi.data_dir.unwrap_or(Uuid::nil()), part_suffix);
        let _ = Self::rename_part(
            &disks,
            RUSTFS_META_TMP_BUCKET,
            &tmp_part_path,
            RUSTFS_META_MULTIPART_BUCKET,
            &part_path,
            fi_buff,
            write_quorum,
        )
        .await?;

        let ret: PartInfo = PartInfo {
            etag: Some(etag.clone()),
            part_num: part_id,
            last_mod: Some(OffsetDateTime::now_utc()),
            size: w_size,
        };

        // error!("put_object_part ret {:?}", &ret);

        Ok(ret)
    }
    async fn list_multipart_uploads(
        &self,
        bucket: &str,
        object: &str,
        key_marker: &str,
        upload_id_marker: &str,
        delimiter: &str,
        max_uploads: usize,
    ) -> Result<ListMultipartsInfo> {
        let disks = {
            let disks = self.get_online_local_disks().await;
            if disks.is_empty() {
                // TODO: getOnlineDisksWithHealing
                self.get_online_disks().await
            } else {
                disks
            }
        };

        let mut upload_ids = Vec::new();

        for disk in disks.iter().flatten() {
            if !disk.is_online().await {
                continue;
            }

            let has_uoload_ids = match disk
                .list_dir(
                    bucket,
                    RUSTFS_META_MULTIPART_BUCKET,
                    Self::get_multipart_sha_dir(bucket, object).as_str(),
                    -1,
                )
                .await
            {
                Ok(res) => Some(res),
                Err(err) => {
                    if DiskError::DiskNotFound.is(&err) {
                        None
                    } else if DiskError::FileNotFound.is(&err) {
                        return Ok(ListMultipartsInfo {
                            key_marker: key_marker.to_owned(),
                            max_uploads,
                            prefix: object.to_owned(),
                            delimiter: delimiter.to_owned(),
                            ..Default::default()
                        });
                    } else {
                        return Err(err);
                    }
                }
            };

            if let Some(ids) = has_uoload_ids {
                upload_ids = ids;
                break;
            }
        }

        let mut uploads = Vec::new();

        let mut populated_upload_ids = HashSet::new();

        for upload_id in upload_ids.iter() {
            let upload_id = upload_id.trim_end_matches(SLASH_SEPARATOR).to_string();
            if populated_upload_ids.contains(&upload_id) {
                continue;
            }

            let start_time = {
                let now = OffsetDateTime::now_utc();

                let splits: Vec<&str> = upload_id.split("x").collect();
                if splits.len() == 2 {
                    if let Ok(unix) = splits[1].parse::<i64>() {
                        OffsetDateTime::from_unix_timestamp(unix)?
                    } else {
                        now
                    }
                } else {
                    now
                }
            };

            let deployment_id = { get_global_deployment_id().await };

            uploads.push(MultipartInfo {
                bucket: bucket.to_owned(),
                object: object.to_owned(),
                upload_id: base64_encode(format!("{}.{}", deployment_id, upload_id).as_bytes()),
                initiated: Some(start_time),
                ..Default::default()
            });

            populated_upload_ids.insert(upload_id);
        }

        uploads.sort_by(|a, b| a.initiated.cmp(&b.initiated));

        let mut upload_idx = 0;
        if !upload_id_marker.is_empty() {
            while upload_idx < uploads.len() {
                if uploads[upload_idx].upload_id != upload_id_marker {
                    upload_idx += 1;
                    continue;
                }

                if uploads[upload_idx].upload_id == upload_id_marker {
                    upload_idx += 1;
                    break;
                }

                upload_idx += 1;
            }
        }

        let mut ret_uploads = Vec::new();
        let mut next_upload_id_marker = String::new();
        while upload_idx < uploads.len() {
            ret_uploads.push(uploads[upload_idx].clone());
            next_upload_id_marker = uploads[upload_idx].upload_id.clone();
            upload_idx += 1;

            if ret_uploads.len() > max_uploads {
                break;
            }
        }

        let is_truncated = ret_uploads.len() < uploads.len();

        if !is_truncated {
            next_upload_id_marker = "".to_owned();
        }

        Ok(ListMultipartsInfo {
            key_marker: key_marker.to_owned(),
            next_upload_id_marker,
            max_uploads,
            is_truncated,
            uploads: ret_uploads,
            prefix: object.to_owned(),
            delimiter: delimiter.to_owned(),
            ..Default::default()
        })
    }
    async fn new_multipart_upload(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<MultipartUploadResult> {
        let disks = self.disks.read().await;

        let disks = disks.clone();

        let mut user_defined = opts.user_defined.clone();

        if let Some(ref etag) = opts.preserve_etag {
            user_defined.insert("etag".to_owned(), etag.clone());
        }

        if let Some(sc) = user_defined.get(xhttp::AMZ_STORAGE_CLASS) {
            if sc == storageclass::STANDARD {
                let _ = user_defined.remove(xhttp::AMZ_STORAGE_CLASS);
            }
        }

        let sc_parity_drives = {
            if let Some(sc) = GLOBAL_StorageClass.get() {
                let a = sc.get_parity_for_sc(
                    user_defined
                        .get(xhttp::AMZ_STORAGE_CLASS)
                        .cloned()
                        .unwrap_or_default()
                        .as_str(),
                );
                a
            } else {
                None
            }
        };

        let mut parity_drives = sc_parity_drives.unwrap_or(self.default_parity_count);
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

        fi.data_dir = Some(Uuid::new_v4());
        fi.fresh = true;

        let parts_metadata = vec![fi.clone(); disks.len()];

        let (shuffle_disks, mut parts_metadatas) = Self::shuffle_disks_and_parts_metadata(&disks, &parts_metadata, &fi);

        let now: OffsetDateTime = OffsetDateTime::now_utc();
        for fi in parts_metadatas.iter_mut() {
            fi.metadata = Some(user_defined.clone());
            fi.mod_time = Some(now);
            fi.fresh = true;
        }

        fi.mod_time = Some(now);

        let upload_uuid = format!("{}x{}", Uuid::new_v4(), fi.mod_time.unwrap().unix_timestamp());

        let upload_id = base64_encode(format!("{}.{}", "globalDeploymentID", upload_uuid).as_bytes());

        let upload_path = Self::get_upload_id_dir(bucket, object, upload_uuid.as_str());

        let errs = Self::write_unique_file_info(
            &shuffle_disks,
            bucket,
            RUSTFS_META_MULTIPART_BUCKET,
            upload_path.as_str(),
            &parts_metadatas,
        )
        .await;

        if let Some(err) = reduce_write_quorum_errs(&errs, object_op_ignored_errs().as_ref(), write_quorum) {
            // TODO: 并发
            for (i, err) in errs.iter().enumerate() {
                if err.is_some() {
                    continue;
                }

                if let Some(disk) = shuffle_disks[i].as_ref() {
                    let _ = disk
                        .delete(
                            RUSTFS_META_MULTIPART_BUCKET,
                            upload_path.as_str(),
                            DeleteOptions {
                                recursive: true,
                                ..Default::default()
                            },
                        )
                        .await
                        .map_err(|e| {
                            warn!("write meta revert err {:?}", e);
                            e
                        });
                }
            }

            return Err(err);
        }
        // evalDisks

        Ok(MultipartUploadResult { upload_id })
    }
    async fn get_multipart_info(
        &self,
        bucket: &str,
        object: &str,
        upload_id: &str,
        _opts: &ObjectOptions,
    ) -> Result<MultipartInfo> {
        // TODO: nslock
        let (fi, _) = self
            .check_upload_id_exists(bucket, object, upload_id, false)
            .await
            .map_err(|e| to_object_err(e, vec![bucket, object, upload_id]))?;

        Ok(MultipartInfo {
            bucket: bucket.to_owned(),
            object: object.to_owned(),
            upload_id: upload_id.to_owned(),
            user_defined: fi.metadata.unwrap_or_default(),
            ..Default::default()
        })
    }
    async fn abort_multipart_upload(&self, bucket: &str, object: &str, upload_id: &str, _opts: &ObjectOptions) -> Result<()> {
        self.check_upload_id_exists(bucket, object, upload_id, false).await?;
        let upload_id_path = Self::get_upload_id_dir(bucket, object, upload_id);

        self.delete_all(RUSTFS_META_MULTIPART_BUCKET, &upload_id_path).await
    }
    // complete_multipart_upload 完成
    // #[tracing::instrument(skip(self))]
    async fn complete_multipart_upload(
        &self,
        bucket: &str,
        object: &str,
        upload_id: &str,
        uploaded_parts: Vec<CompletePart>,
        opts: &ObjectOptions,
    ) -> Result<ObjectInfo> {
        let (mut fi, files_metas) = self.check_upload_id_exists(bucket, object, upload_id, true).await?;
        let upload_id_path = Self::get_upload_id_dir(bucket, object, upload_id);

        let write_quorum = fi.write_quorum(self.default_write_quorum());

        let disks = self.disks.read().await;

        let disks = disks.clone();
        // let disks = Self::shuffle_disks(&disks, &fi.erasure.distribution);

        let part_path = format!("{}/{}/", upload_id_path, fi.data_dir.unwrap_or(Uuid::nil()));

        let files: Vec<String> = uploaded_parts.iter().map(|v| format!("part.{}.meta", v.part_num)).collect();

        // readMultipleFiles

        let req = ReadMultipleReq {
            bucket: RUSTFS_META_MULTIPART_BUCKET.to_string(),
            prefix: part_path,
            files,
            max_size: 1 << 20,
            metadata_only: true,
            abort404: true,
            max_results: 0,
        };

        let part_files_resp = Self::read_multiple_files(&disks, req, write_quorum).await;

        if part_files_resp.len() != uploaded_parts.len() {
            return Err(Error::msg("part result number err"));
        }

        for (i, res) in part_files_resp.iter().enumerate() {
            let part_id = uploaded_parts[i].part_num;
            if !res.error.is_empty() || !res.exists {
                // error!("complete_multipart_upload part_id err {:?}", res);
                return Err(Error::new(ErasureError::InvalidPart(part_id)));
            }

            let part_fi = FileInfo::unmarshal(&res.data).map_err(|_e| {
                // error!("complete_multipart_upload FileInfo::unmarshal err {:?}", e);
                Error::new(ErasureError::InvalidPart(part_id))
            })?;
            let part = &part_fi.parts[0];
            let part_num = part.number;

            // debug!("complete part {} file info {:?}", part_num, &part_fi);
            // debug!("complete part {} object info {:?}", part_num, &part);

            if part_id != part_num {
                // error!("complete_multipart_upload part_id err part_id != part_num {} != {}", part_id, part_num);
                return Err(Error::new(ErasureError::InvalidPart(part_id)));
            }

            fi.add_object_part(part.number, part.etag.clone(), part.size, part.mod_time, part.actual_size);
        }

        let (shuffle_disks, mut parts_metadatas) = Self::shuffle_disks_and_parts_metadata_by_index(&disks, &files_metas, &fi);

        let curr_fi = fi.clone();

        fi.parts = Vec::with_capacity(uploaded_parts.len());

        let mut total_size: usize = 0;

        for (i, p) in uploaded_parts.iter().enumerate() {
            let has_part = curr_fi.parts.iter().find(|v| v.number == p.part_num);
            if has_part.is_none() {
                // error!("complete_multipart_upload has_part.is_none() {:?}", has_part);
                return Err(Error::new(ErasureError::InvalidPart(p.part_num)));
            }

            let ext_part = &curr_fi.parts[i];

            // TODO: crypto

            total_size += ext_part.size;

            fi.parts.push(ObjectPartInfo {
                etag: ext_part.etag.clone(),
                number: p.part_num,
                size: ext_part.size,
                mod_time: ext_part.mod_time,
                actual_size: ext_part.actual_size,
            });
        }

        fi.size = total_size;
        fi.mod_time = opts.mod_time;
        if fi.mod_time.is_none() {
            fi.mod_time = Some(OffsetDateTime::now_utc());
        }

        for meta in parts_metadatas.iter_mut() {
            if meta.is_valid() {
                meta.size = fi.size;
                meta.mod_time = fi.mod_time;
                meta.parts.clone_from(&fi.parts);
            }
        }

        for p in curr_fi.parts.iter() {
            self.remove_part_meta(
                bucket,
                object,
                upload_id,
                curr_fi.data_dir.unwrap_or(Uuid::nil()).to_string().as_str(),
                p.number,
            )
            .await?;

            if !fi.parts.iter().any(|v| v.number == p.number) {
                self.remove_object_part(
                    bucket,
                    object,
                    upload_id,
                    curr_fi.data_dir.unwrap_or(Uuid::nil()).to_string().as_str(),
                    p.number,
                )
                .await?;
            }
        }

        let (online_disks, _, op_old_dir) = Self::rename_data(
            &shuffle_disks,
            RUSTFS_META_MULTIPART_BUCKET,
            &upload_id_path,
            &parts_metadatas,
            bucket,
            object,
            write_quorum,
        )
        .await?;

        for (i, op_disk) in online_disks.iter().enumerate() {
            if let Some(disk) = op_disk {
                if disk.is_online().await {
                    fi = parts_metadatas[i].clone();
                    break;
                }
            }
        }

        fi.is_latest = true;

        // debug!("complete fileinfo {:?}", &fi);

        // TODO: reduce_common_data_dir
        if let Some(old_dir) = op_old_dir {
            self.commit_rename_data_dir(&shuffle_disks, bucket, object, &old_dir.to_string(), write_quorum)
                .await?;
        }

        let _ = self.delete_all(RUSTFS_META_MULTIPART_BUCKET, &upload_id_path).await;

        Ok(fi.to_object_info(bucket, object, opts.versioned || opts.version_suspended))
    }

    async fn get_disks(&self, _pool_idx: usize, _set_idx: usize) -> Result<Vec<Option<DiskStore>>> {
        Ok(self.get_disks_internal().await)
    }

    fn set_drive_counts(&self) -> Vec<usize> {
        unimplemented!()
    }

    async fn delete_bucket(&self, _bucket: &str, _opts: &DeleteBucketOptions) -> Result<()> {
        unimplemented!()
    }

    async fn heal_format(&self, _dry_run: bool) -> Result<(HealResultItem, Option<Error>)> {
        unimplemented!()
    }
    async fn heal_bucket(&self, _bucket: &str, _opts: &HealOpts) -> Result<HealResultItem> {
        unimplemented!()
    }
    async fn heal_object(
        &self,
        bucket: &str,
        object: &str,
        version_id: &str,
        opts: &HealOpts,
    ) -> Result<(HealResultItem, Option<Error>)> {
        if has_suffix(object, SLASH_SEPARATOR) {
            let (result, err) = self.heal_object_dir(bucket, object, opts.dry_run, opts.remove).await?;
            return Ok((result, err));
        }

        let disks = self.disks.read().await;

        let disks = disks.clone();
        let (_, errs) = Self::read_all_fileinfo(&disks, "", bucket, object, version_id, false, false).await;
        if is_all_not_found(&errs) {
            warn!(
                "heal_object failed, all obj part not found, bucket: {}, obj: {}, version_id: {}",
                bucket, object, version_id
            );
            let err = if !version_id.is_empty() {
                Error::new(DiskError::FileVersionNotFound)
            } else {
                Error::new(DiskError::FileNotFound)
            };
            return Ok((
                self.default_heal_result(FileInfo::default(), &errs, bucket, object, version_id)
                    .await,
                Some(err),
            ));
        }

        // Heal the object.
        let (result, err) = self.heal_object(bucket, object, version_id, opts).await?;
        if let Some(err) = &err {
            match err.downcast_ref::<DiskError>() {
                Some(DiskError::FileCorrupt) if opts.scan_mode != HEAL_DEEP_SCAN => {
                    // Instead of returning an error when a bitrot error is detected
                    // during a normal heal scan, heal again with bitrot flag enabled.
                    let mut opts = *opts;
                    opts.scan_mode = HEAL_DEEP_SCAN;
                    let (result, err) = self.heal_object(bucket, object, version_id, &opts).await?;
                    return Ok((result, err));
                }
                _ => {}
            }
        }
        return Ok((result, err));
    }
    async fn heal_objects(
        &self,
        _bucket: &str,
        _prefix: &str,
        _opts: &HealOpts,
        _hs: Arc<HealSequence>,
        _is_meta: bool,
    ) -> Result<()> {
        unimplemented!()
    }
    async fn get_pool_and_set(&self, _id: &str) -> Result<(Option<usize>, Option<usize>, Option<usize>)> {
        unimplemented!()
    }
    async fn check_abandoned_parts(&self, _bucket: &str, _object: &str, _opts: &HealOpts) -> Result<()> {
        unimplemented!()
    }
}

#[derive(Debug, PartialEq, Eq)]
struct ObjProps {
    mod_time: Option<OffsetDateTime>,
    num_versions: usize,
}

impl Hash for ObjProps {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.mod_time.hash(state);
        self.num_versions.hash(state);
    }
}

#[derive(Default, Clone, Debug)]
pub struct HealEntryResult {
    pub bytes: usize,
    pub success: bool,
    pub skipped: bool,
    pub entry_done: bool,
    pub name: String,
}

fn is_object_dang_ling(
    meta_arr: &[FileInfo],
    errs: &[Option<Error>],
    data_errs_by_part: &HashMap<usize, Vec<usize>>,
) -> Result<FileInfo> {
    let mut valid_meta = FileInfo::default();
    let (not_found_meta_errs, non_actionable_meta_errs) = dang_ling_meta_errs_count(errs);

    let (mut not_found_parts_errs, mut non_actionable_parts_errs) = (0, 0);

    data_errs_by_part.iter().for_each(|(_, v)| {
        let (nf, na) = dang_ling_part_errs_count(v);
        if nf > not_found_parts_errs {
            (not_found_parts_errs, non_actionable_parts_errs) = (nf, na);
        }
    });

    meta_arr.iter().for_each(|fi| {
        if fi.is_valid() {
            valid_meta = fi.clone();
        }
    });

    if !valid_meta.is_valid() {
        let data_blocks = (meta_arr.len() + 1) / 2;
        if not_found_parts_errs > data_blocks {
            return Ok(valid_meta);
        }

        return Err(Error::from_string("not ok"));
    }

    if non_actionable_meta_errs > 0 || non_actionable_parts_errs > 0 {
        return Err(Error::from_string("not ok"));
    }

    if valid_meta.deleted {
        let data_blocks = (errs.len() + 1) / 2;
        if not_found_meta_errs > data_blocks {
            return Ok(valid_meta);
        }
        return Err(Error::from_string("not ok"));
    }

    if not_found_meta_errs > 0 && not_found_meta_errs > valid_meta.erasure.parity_blocks {
        return Ok(valid_meta);
    }

    if !valid_meta.is_remote() && not_found_parts_errs > 0 && not_found_parts_errs > valid_meta.erasure.parity_blocks {
        return Ok(valid_meta);
    }

    Err(Error::from_string("not ok"))
}

fn dang_ling_meta_errs_count(cerrs: &[Option<Error>]) -> (usize, usize) {
    let (mut not_found_count, mut non_actionable_count) = (0, 0);
    cerrs.iter().for_each(|err| {
        if let Some(err) = err {
            match err.downcast_ref::<DiskError>() {
                Some(DiskError::FileNotFound) | Some(DiskError::FileVersionNotFound) => not_found_count += 1,
                _ => non_actionable_count += 1,
            }
        }
    });

    (not_found_count, non_actionable_count)
}

fn dang_ling_part_errs_count(results: &[usize]) -> (usize, usize) {
    let (mut not_found_count, mut non_actionable_count) = (0, 0);
    results.iter().for_each(|result| {
        if *result == CHECK_PART_SUCCESS {
            // skip
        } else if *result == CHECK_PART_FILE_NOT_FOUND {
            not_found_count += 1;
        } else {
            non_actionable_count += 1;
        }
    });

    (not_found_count, non_actionable_count)
}

fn is_object_dir_dang_ling(errs: &[Option<Error>]) -> bool {
    let mut found = 0;
    let mut not_found = 0;
    let mut found_not_empty = 0;
    let mut other_found = 0;
    errs.iter().for_each(|err| {
        if err.is_none() {
            found += 1;
        } else if let Some(err) = err {
            match err.downcast_ref::<DiskError>() {
                Some(DiskError::FileNotFound) | Some(DiskError::VolumeNotFound) => {
                    not_found += 1;
                }
                Some(DiskError::VolumeNotEmpty) => {
                    found_not_empty += 1;
                }
                _ => {
                    other_found += 1;
                }
            }
        }
    });

    found = found + found_not_empty + other_found;
    found < not_found && found > 0
}

fn join_errs(errs: &[Option<Error>]) -> String {
    let errs = errs
        .iter()
        .map(|err| {
            if let Some(err) = err {
                return err.to_string();
            }
            "<nil>".to_string()
        })
        .collect::<Vec<_>>();

    errs.join(", ")
}

pub fn conv_part_err_to_int(err: &Option<Error>) -> usize {
    if let Some(err) = err {
        match err.downcast_ref::<DiskError>() {
            Some(DiskError::FileNotFound) | Some(DiskError::FileVersionNotFound) => CHECK_PART_FILE_NOT_FOUND,
            Some(DiskError::FileCorrupt) => CHECK_PART_FILE_CORRUPT,
            Some(DiskError::VolumeNotFound) => CHECK_PART_VOLUME_NOT_FOUND,
            Some(DiskError::DiskNotFound) => CHECK_PART_DISK_NOT_FOUND,
            None => CHECK_PART_SUCCESS,
            _ => CHECK_PART_UNKNOWN,
        }
    } else {
        CHECK_PART_SUCCESS
    }
}

pub fn has_part_err(part_errs: &[usize]) -> bool {
    part_errs.iter().any(|err| *err != CHECK_PART_SUCCESS)
}

async fn disks_with_all_parts(
    online_disks: &[Option<DiskStore>],
    parts_metadata: &mut [FileInfo],
    errs: &[Option<Error>],
    lastest_meta: &FileInfo,
    bucket: &str,
    object: &str,
    scan_mode: HealScanMode,
) -> Result<(Vec<Option<DiskStore>>, HashMap<usize, Vec<usize>>, HashMap<usize, Vec<usize>>)> {
    let mut available_disks = vec![None; online_disks.len()];
    let mut data_errs_by_disk: HashMap<usize, Vec<usize>> = HashMap::new();
    for i in 0..online_disks.len() {
        data_errs_by_disk.insert(i, vec![1; lastest_meta.parts.len()]);
    }
    let mut data_errs_by_part: HashMap<usize, Vec<usize>> = HashMap::new();
    for i in 0..lastest_meta.parts.len() {
        data_errs_by_part.insert(i, vec![1; online_disks.len()]);
    }

    let mut inconsistent = 0;
    parts_metadata.iter().enumerate().for_each(|(index, meta)| {
        if meta.is_valid() && !meta.deleted && meta.erasure.distribution.len() != online_disks.len()
            || (!meta.erasure.distribution.is_empty() && meta.erasure.distribution[index] != meta.erasure.index)
        {
            warn!("file info inconsistent, meta: {:?}", meta);
            inconsistent += 1;
        }
    });

    let erasure_distribution_reliable = inconsistent <= parts_metadata.len() / 2;

    let mut meta_errs = vec![None; errs.len()];

    for (index, disk) in online_disks.iter().enumerate() {
        let disk = if let Some(disk) = disk {
            disk
        } else {
            meta_errs[index] = Some(Error::new(DiskError::DiskNotFound));
            continue;
        };

        if let Some(err) = errs[index].clone() {
            meta_errs[index] = Some(err);
            continue;
        }
        if !disk.is_online().await {
            meta_errs[index] = Some(Error::new(DiskError::DiskNotFound));
            continue;
        }
        let meta = &parts_metadata[index];
        if !meta.mod_time.eq(&lastest_meta.mod_time) || !meta.data_dir.eq(&lastest_meta.data_dir) {
            warn!("mod_time is not Eq, file corrupt, index: {index}");
            meta_errs[index] = Some(Error::new(DiskError::FileCorrupt));
            parts_metadata[index] = FileInfo::default();
            continue;
        }
        if erasure_distribution_reliable {
            if !meta.is_valid() {
                warn!("file info is not valid, file corrupt, index: {index}");
                parts_metadata[index] = FileInfo::default();
                meta_errs[index] = Some(Error::new(DiskError::FileCorrupt));
                continue;
            }

            if !meta.deleted && meta.erasure.distribution.len() != online_disks.len() {
                warn!("file info distribution len not Eq online_disks len, file corrupt, index: {index}");
                parts_metadata[index] = FileInfo::default();
                meta_errs[index] = Some(Error::new(DiskError::FileCorrupt));
                continue;
            }
        }
    }
    info!("meta_errs: {:?}, errs: {:?}", meta_errs, errs);
    meta_errs.iter().enumerate().for_each(|(index, err)| {
        if err.is_some() {
            let part_err = conv_part_err_to_int(err);
            for p in 0..lastest_meta.parts.len() {
                data_errs_by_part.entry(p).or_insert(vec![0; meta_errs.len()])[index] = part_err;
            }
        }
    });

    info!("data_errs_by_part: {:?}, data_errs_by_disk: {:?}", data_errs_by_part, data_errs_by_disk);
    for (index, disk) in online_disks.iter().enumerate() {
        if meta_errs[index].is_some() {
            continue;
        }

        let disk = if let Some(disk) = disk {
            disk
        } else {
            meta_errs[index] = Some(Error::new(DiskError::DiskNotFound));
            continue;
        };

        let meta = &mut parts_metadata[index];
        if meta.deleted || meta.is_remote() {
            continue;
        }

        // Always check data, if we got it.
        if (meta.data.is_some() || meta.size == 0) && !meta.parts.is_empty() {
            if let Some(data) = &meta.data {
                let checksum_info = meta.erasure.get_checksum_info(meta.parts[0].number);
                let data_len = data.len();
                let verify_err = match bitrot_verify(
                    &mut Cursor::new(data.to_vec()),
                    data_len,
                    meta.erasure.shard_file_size(meta.size),
                    checksum_info.algorithm,
                    checksum_info.hash,
                    meta.erasure.shard_size(meta.erasure.block_size),
                ) {
                    Ok(_) => None,
                    Err(err) => Some(err),
                };

                if let Some(vec) = data_errs_by_part.get_mut(&0) {
                    if index < vec.len() {
                        vec[index] = conv_part_err_to_int(&verify_err);
                        info!("bitrot check result: {}", vec[index]);
                    }
                }
            }
            continue;
        }

        let mut verify_resp = CheckPartsResp::default();
        let mut verify_err = None;
        meta.data_dir = lastest_meta.data_dir;
        if scan_mode == HEAL_DEEP_SCAN {
            // disk has a valid xl.meta but may not have all the
            // parts. This is considered an outdated disk, since
            // it needs healing too.
            match disk.verify_file(bucket, object, meta).await {
                Ok(v) => {
                    verify_resp = v;
                }
                Err(err) => {
                    verify_err = Some(err);
                }
            }
        } else {
            match disk.check_parts(bucket, object, meta).await {
                Ok(v) => {
                    verify_resp = v;
                }
                Err(err) => {
                    verify_err = Some(err);
                }
            }
        }

        for p in 0..lastest_meta.parts.len() {
            if let Some(vec) = data_errs_by_part.get_mut(&p) {
                if index < vec.len() {
                    if verify_err.is_some() {
                        info!("verify_err");
                        vec[index] = conv_part_err_to_int(&verify_err);
                    } else {
                        info!("verify_resp, verify_resp.results {}", verify_resp.results[p]);
                        vec[index] = verify_resp.results[p];
                    }
                }
            }
        }
    }
    info!("data_errs_by_part: {:?}, data_errs_by_disk: {:?}", data_errs_by_part, data_errs_by_disk);
    for (part, disks) in data_errs_by_part.iter() {
        for (idx, disk) in disks.iter().enumerate() {
            if let Some(vec) = data_errs_by_disk.get_mut(&idx) {
                vec[*part] = *disk;
            }
        }
    }
    info!("data_errs_by_part: {:?}, data_errs_by_disk: {:?}", data_errs_by_part, data_errs_by_disk);
    for (i, disk) in online_disks.iter().enumerate() {
        if meta_errs[i].is_none() && disk.is_some() && !has_part_err(&data_errs_by_disk[&i]) {
            available_disks[i] = Some(disk.clone().unwrap());
        } else {
            parts_metadata[i] = FileInfo::default();
        }
    }

    Ok((available_disks, data_errs_by_disk, data_errs_by_part))
}

pub fn should_heal_object_on_disk(
    err: &Option<Error>,
    parts_errs: &[usize],
    meta: &FileInfo,
    lastest_meta: &FileInfo,
) -> (bool, Option<Error>) {
    match err {
        Some(err) => match err.downcast_ref::<DiskError>() {
            Some(DiskError::FileNotFound) | Some(DiskError::FileVersionNotFound) | Some(DiskError::FileCorrupt) => {
                return (true, Some(err.clone()));
            }
            _ => {}
        },
        None => {
            if lastest_meta.volume != meta.volume
                || lastest_meta.name != meta.name
                || lastest_meta.version_id != meta.version_id
                || lastest_meta.deleted != meta.deleted
            {
                info!("lastest_meta not Eq meta, lastest_meta: {:?}, meta: {:?}", lastest_meta, meta);
                return (true, Some(Error::new(DiskError::OutdatedXLMeta)));
            }
            if !meta.deleted && !meta.is_remote() {
                let err_vec = [CHECK_PART_FILE_NOT_FOUND, CHECK_PART_FILE_CORRUPT];
                for part_err in parts_errs.iter() {
                    if err_vec.contains(part_err) {
                        return (true, Some(Error::new(DiskError::PartMissingOrCorrupt)));
                    }
                }
            }
        }
    }
    (false, err.clone())
}

async fn get_disks_info(disks: &[Option<DiskStore>], eps: &[Endpoint]) -> Vec<madmin::Disk> {
    let mut ret = Vec::new();

    for (i, pool) in disks.iter().enumerate() {
        if let Some(disk) = pool {
            match disk.disk_info(&DiskInfoOptions::default()).await {
                Ok(res) => ret.push(madmin::Disk {
                    endpoint: eps[i].to_string(),
                    local: eps[i].is_local,
                    pool_index: eps[i].pool_idx,
                    set_index: eps[i].set_idx,
                    disk_index: eps[i].disk_idx,
                    state: "ok".to_owned(),

                    root_disk: res.root_disk,
                    drive_path: res.mount_path.clone(),
                    healing: res.healing,
                    scanning: res.scanning,

                    uuid: res.id.clone(),
                    major: res.major as u32,
                    minor: res.minor as u32,
                    model: None,
                    total_space: res.total,
                    used_space: res.used,
                    available_space: res.free,
                    utilization: {
                        if res.total > 0 {
                            res.used as f64 / res.total as f64 * 100_f64
                        } else {
                            0_f64
                        }
                    },
                    used_inodes: res.used_inodes,
                    free_inodes: res.free_inodes,
                    ..Default::default()
                }),
                Err(err) => ret.push(madmin::Disk {
                    state: err.to_string(),
                    endpoint: eps[i].to_string(),
                    local: eps[i].is_local,
                    pool_index: eps[i].pool_idx,
                    set_index: eps[i].set_idx,
                    disk_index: eps[i].disk_idx,
                    ..Default::default()
                }),
            }
        } else {
            ret.push(madmin::Disk {
                endpoint: eps[i].to_string(),
                local: eps[i].is_local,
                pool_index: eps[i].pool_idx,
                set_index: eps[i].set_idx,
                disk_index: eps[i].disk_idx,
                state: DiskError::DiskNotFound.to_string(),
                ..Default::default()
            })
        }
    }

    ret
}
async fn get_storage_info(disks: &[Option<DiskStore>], eps: &[Endpoint]) -> madmin::StorageInfo {
    let mut disks = get_disks_info(disks, eps).await;
    disks.sort_by(|a, b| a.total_space.cmp(&b.total_space));

    madmin::StorageInfo {
        disks,
        backend: madmin::BackendInfo {
            backend_type: madmin::BackendByte::Erasure,
            ..Default::default()
        },
    }
}
pub async fn stat_all_dirs(disks: &[Option<DiskStore>], bucket: &str, prefix: &str) -> Vec<Option<Error>> {
    let mut errs = Vec::with_capacity(disks.len());
    let mut futures = Vec::with_capacity(disks.len());
    for disk in disks.iter().flatten() {
        let disk = disk.clone();
        let bucket = bucket.to_string();
        let prefix = prefix.to_string();
        futures.push(tokio::spawn(async move {
            match disk.list_dir("", &bucket, &prefix, 1).await {
                Ok(entries) => {
                    if !entries.is_empty() {
                        return Some(Error::new(DiskError::VolumeNotEmpty));
                    }
                    None
                }
                Err(err) => Some(err),
            }
        }));
    }

    let results = join_all(futures).await;

    for err in results.into_iter().flatten() {
        errs.push(err);
    }
    errs
}
