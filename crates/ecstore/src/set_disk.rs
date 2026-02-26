// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(unused_imports)]
#![allow(unused_variables)]

use crate::batch_processor::{AsyncBatchProcessor, get_global_processors};
use crate::bitrot::{create_bitrot_reader, create_bitrot_writer};
use crate::bucket::lifecycle::lifecycle::TRANSITION_COMPLETE;
use crate::bucket::replication::check_replicate_delete;
use crate::bucket::versioning::VersioningApi;
use crate::bucket::versioning_sys::BucketVersioningSys;
use crate::client::{object_api_utils::get_raw_etag, transition_api::ReaderImpl};
use crate::disk::error_reduce::{OBJECT_OP_IGNORED_ERRS, reduce_read_quorum_errs, reduce_write_quorum_errs};
use crate::disk::{
    self, CHECK_PART_DISK_NOT_FOUND, CHECK_PART_FILE_CORRUPT, CHECK_PART_FILE_NOT_FOUND, CHECK_PART_SUCCESS, CHECK_PART_UNKNOWN,
    conv_part_err_to_int, has_part_err,
};
use crate::disk::{STORAGE_FORMAT_FILE, count_part_not_success};
use crate::erasure_coding;
use crate::erasure_coding::bitrot_verify;
use crate::error::{Error, Result, is_err_version_not_found};
use crate::error::{GenericError, ObjectApiError, is_err_object_not_found};
use crate::global::{GLOBAL_LocalNodeName, GLOBAL_TierConfigMgr};
use crate::store_api::ListObjectVersionsInfo;
use crate::store_api::{ListPartsInfo, ObjectOptions, ObjectToDelete};
use crate::store_api::{ObjectInfoOrErr, WalkOptions};
use crate::{
    bucket::lifecycle::bucket_lifecycle_ops::{
        LifecycleOps, gen_transition_objname, get_transitioned_object_reader, put_restore_opts,
    },
    cache_value::metacache_set::{ListPathRawOptions, list_path_raw},
    config::{GLOBAL_STORAGE_CLASS, storageclass},
    disk::{
        CheckPartsResp, DeleteOptions, DiskAPI, DiskInfo, DiskInfoOptions, DiskOption, DiskStore, FileInfoVersions,
        RUSTFS_META_BUCKET, RUSTFS_META_MULTIPART_BUCKET, RUSTFS_META_TMP_BUCKET, ReadMultipleReq, ReadMultipleResp, ReadOptions,
        UpdateMetadataOpts, endpoint::Endpoint, error::DiskError, format::FormatV3, new_disk,
    },
    error::{StorageError, to_object_err},
    event::name::EventName,
    event_notification::{EventArgs, send_event},
    global::{GLOBAL_LOCAL_DISK_MAP, GLOBAL_LOCAL_DISK_SET_DRIVES, get_global_deployment_id, is_dist_erasure},
    store_api::{
        BucketInfo, BucketOptions, CompletePart, DeleteBucketOptions, DeletedObject, GetObjectReader, HTTPRangeSpec,
        ListMultipartsInfo, ListObjectsV2Info, MakeBucketOptions, MultipartInfo, MultipartUploadResult, ObjectIO, ObjectInfo,
        PartInfo, PutObjReader, StorageAPI,
    },
    store_init::load_format_erasure,
};
use bytes::Bytes;
use bytesize::ByteSize;
use chrono::Utc;
use futures::future::join_all;
use glob::Pattern;
use http::HeaderMap;
use md5::{Digest as Md5Digest, Md5};
use rand::{Rng, seq::SliceRandom};
use regex::Regex;
use rustfs_common::heal_channel::{DriveState, HealChannelPriority, HealItemType, HealOpts, HealScanMode, send_heal_disk};
use rustfs_config::MI_B;
use rustfs_filemeta::{
    FileInfo, FileMeta, FileMetaShallowVersion, MetaCacheEntries, MetaCacheEntry, MetadataResolutionParams, ObjectPartInfo,
    RawFileInfo, ReplicateDecision, ReplicationStatusType, VersionPurgeStatusType, file_info_from_raw, merge_file_meta_versions,
};
use rustfs_lock::LockClient;
use rustfs_lock::fast_lock::types::LockResult;
use rustfs_lock::local_lock::LocalLock;
use rustfs_lock::{FastLockGuard, NamespaceLock, NamespaceLockGuard, NamespaceLockWrapper, ObjectKey};
use rustfs_madmin::heal_commands::{HealDriveInfo, HealResultItem};
use rustfs_rio::{EtagResolvable, HashReader, HashReaderMut, TryGetIndex as _, WarpReader};
use rustfs_utils::http::RUSTFS_BUCKET_REPLICATION_SSEC_CHECKSUM;
use rustfs_utils::http::headers::AMZ_STORAGE_CLASS;
use rustfs_utils::http::headers::{AMZ_OBJECT_TAGGING, RESERVED_METADATA_PREFIX, RESERVED_METADATA_PREFIX_LOWER};
use rustfs_utils::{
    HashAlgorithm,
    crypto::hex,
    path::{SLASH_SEPARATOR, encode_dir_object, has_suffix, path_join_buf},
};
use rustfs_workers::workers::Workers;
use s3s::header::X_AMZ_RESTORE;
use sha2::{Digest, Sha256};
use std::hash::Hash;
use std::mem::{self};
use std::time::{Instant, SystemTime};
use std::{
    collections::{HashMap, HashSet},
    io::{Cursor, Write},
    path::Path,
    sync::Arc,
    time::Duration,
};
use time::OffsetDateTime;
use tokio::{
    io::{AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader},
    sync::{RwLock, broadcast},
};
use tokio::{
    select,
    sync::mpsc::{self, Sender},
    time::{interval, timeout},
};
use tokio_util::sync::CancellationToken;
use tracing::error;
use tracing::{debug, info, warn};
use uuid::Uuid;

pub const DEFAULT_READ_BUFFER_SIZE: usize = MI_B; // 1 MiB = 1024 * 1024;
pub const MAX_PARTS_COUNT: usize = 10000;
const DISK_ONLINE_TIMEOUT: Duration = Duration::from_secs(1);
const DISK_HEALTH_CACHE_TTL: Duration = Duration::from_millis(750);

mod heal;
mod list;
mod lock;
mod metadata;
mod multipart;
mod read;
mod replication;
mod write;

/// Get lock acquire timeout from environment variable RUSTFS_LOCK_ACQUIRE_TIMEOUT (in seconds)
/// Defaults to 30 seconds if not set or invalid
pub fn get_lock_acquire_timeout() -> Duration {
    Duration::from_secs(rustfs_utils::get_env_u64("RUSTFS_LOCK_ACQUIRE_TIMEOUT", 5))
}

#[derive(Clone, Debug)]
pub struct SetDisks {
    pub locker_owner: String,
    pub disks: Arc<RwLock<Vec<Option<DiskStore>>>>,
    pub set_endpoints: Vec<Endpoint>,
    pub set_drive_count: usize,
    pub default_parity_count: usize,
    pub set_index: usize,
    pub pool_index: usize,
    pub format: FormatV3,
    disk_health_cache: Arc<RwLock<Vec<Option<DiskHealthEntry>>>>,
    pub lockers: Vec<Arc<dyn LockClient>>,
    local_lock_manager: Arc<rustfs_lock::GlobalLockManager>,
}

#[derive(Clone, Debug)]
struct DiskHealthEntry {
    last_check: Instant,
    online: bool,
}

impl DiskHealthEntry {
    fn cached_value(&self) -> Option<bool> {
        if self.last_check.elapsed() <= DISK_HEALTH_CACHE_TTL {
            Some(self.online)
        } else {
            None
        }
    }
}

impl SetDisks {
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        locker_owner: String,
        disks: Arc<RwLock<Vec<Option<DiskStore>>>>,
        set_drive_count: usize,
        default_parity_count: usize,
        set_index: usize,
        pool_index: usize,
        set_endpoints: Vec<Endpoint>,
        format: FormatV3,
        lockers: Vec<Arc<dyn LockClient>>,
    ) -> Arc<Self> {
        Arc::new(SetDisks {
            locker_owner,
            disks,
            set_drive_count,
            default_parity_count,
            set_index,
            pool_index,
            format,
            set_endpoints,
            disk_health_cache: Arc::new(RwLock::new(Vec::new())),
            lockers,
            local_lock_manager: rustfs_lock::get_global_lock_manager(),
        })
    }

    // async fn cached_disk_health(&self, index: usize) -> Option<bool> {
    //     let cache = self.disk_health_cache.read().await;
    //     cache
    //         .get(index)
    //         .and_then(|entry| entry.as_ref().and_then(|state| state.cached_value()))
    // }

    // async fn update_disk_health(&self, index: usize, online: bool) {
    //     let mut cache = self.disk_health_cache.write().await;
    //     if cache.len() <= index {
    //         cache.resize(index + 1, None);
    //     }
    //     cache[index] = Some(DiskHealthEntry {
    //         last_check: Instant::now(),
    //         online,
    //     });
    // }

    // async fn is_disk_online_cached(&self, index: usize, disk: &DiskStore) -> bool {
    //     if let Some(online) = self.cached_disk_health(index).await {
    //         return online;
    //     }

    //     let disk_clone = disk.clone();
    //     let online = timeout(DISK_ONLINE_TIMEOUT, async move { disk_clone.is_online().await })
    //         .await
    //         .unwrap_or(false);
    //     self.update_disk_health(index, online).await;
    //     online
    // }

    // async fn filter_online_disks(&self, disks: Vec<Option<DiskStore>>) -> (Vec<Option<DiskStore>>, usize) {
    //     let mut filtered = Vec::with_capacity(disks.len());
    //     let mut online_count = 0;

    //     for (idx, disk) in disks.into_iter().enumerate() {
    //         if let Some(disk_store) = disk {
    //             if self.is_disk_online_cached(idx, &disk_store).await {
    //                 filtered.push(Some(disk_store));
    //                 online_count += 1;
    //             } else {
    //                 filtered.push(None);
    //             }
    //         } else {
    //             filtered.push(None);
    //         }
    //     }

    //     (filtered, online_count)
    // }

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

    // Returns per object readQuorum and writeQuorum
    // readQuorum is the min required disks to read data.
    // writeQuorum is the min required disks to write data.

    // Optimized version using batch processor with quorum support

    // pub async fn walk_dir(&self, opts: &WalkDirOptions) -> (Vec<Option<Vec<MetaCacheEntry>>>, Vec<Option<Error>>) {
    //     let disks = self.disks.read().await;

    //     let disks = disks.clone();
    //     let mut futures = Vec::new();
    //     let mut errs = Vec::new();
    //     let mut ress = Vec::new();

    //     for disk in disks.iter() {
    //         let opts = opts.clone();
    //         futures.push(async move {
    //             if let Some(disk) = disk {
    //                 disk.walk_dir(opts, &mut Writer::NotUse).await
    //             } else {
    //                 Err(DiskError::DiskNotFound)
    //             }
    //         });
    //     }

    //     let results = join_all(futures).await;

    //     for res in results {
    //         match res {
    //             Ok(entries) => {
    //                 ress.push(Some(entries));
    //                 errs.push(None);
    //             }
    //             Err(e) => {
    //                 ress.push(None);
    //                 errs.push(Some(e));
    //             }
    //         }
    //     }

    //     (ress, errs)
    // }

    // async fn remove_object_part(
    //     &self,
    //     bucket: &str,
    //     object: &str,
    //     upload_id: &str,
    //     data_dir: &str,
    //     part_num: usize,
    // ) -> Result<()> {
    //     let upload_id_path = Self::get_upload_id_dir(bucket, object, upload_id);
    //     let disks = self.disks.read().await;

    //     let disks = disks.clone();

    //     let file_path = format!("{}/{}/part.{}", upload_id_path, data_dir, part_num);

    //     let mut futures = Vec::with_capacity(disks.len());
    //     let mut errors = Vec::with_capacity(disks.len());

    //     for disk in disks.iter() {
    //         let file_path = file_path.clone();
    //         let meta_file_path = format!("{}.meta", file_path);

    //         futures.push(async move {
    //             if let Some(disk) = disk {
    //                 disk.delete(RUSTFS_META_MULTIPART_BUCKET, &file_path, DeleteOptions::default())
    //                     .await?;
    //                 disk.delete(RUSTFS_META_MULTIPART_BUCKET, &meta_file_path, DeleteOptions::default())
    //                     .await
    //             } else {
    //                 Err(DiskError::DiskNotFound)
    //             }
    //         });
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

    //     Ok(())
    // }
    // async fn remove_part_meta(&self, bucket: &str, object: &str, upload_id: &str, data_dir: &str, part_num: usize) -> Result<()> {
    //     let upload_id_path = Self::get_upload_id_dir(bucket, object, upload_id);
    //     let disks = self.disks.read().await;

    //     let disks = disks.clone();
    //     // let disks = Self::shuffle_disks(&disks, &fi.erasure.distribution);

    //     let file_path = format!("{}/{}/part.{}.meta", upload_id_path, data_dir, part_num);

    //     let mut futures = Vec::with_capacity(disks.len());
    //     let mut errors = Vec::with_capacity(disks.len());

    //     for disk in disks.iter() {
    //         let file_path = file_path.clone();
    //         futures.push(async move {
    //             if let Some(disk) = disk {
    //                 disk.delete(RUSTFS_META_MULTIPART_BUCKET, &file_path, DeleteOptions::default())
    //                     .await
    //             } else {
    //                 Err(DiskError::DiskNotFound)
    //             }
    //         });
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

    //     Ok(())
    // }

    // Shuffle the order

    // Shuffle the order

    // Return shuffled partsMetadata depending on distribution.

    // shuffle_disks TODO: use origin value
}

#[async_trait::async_trait]
impl ObjectIO for SetDisks {
    #[tracing::instrument(level = "debug", skip(self))]
    async fn get_object_reader(
        &self,
        bucket: &str,
        object: &str,
        range: Option<HTTPRangeSpec>,
        h: HeaderMap,
        opts: &ObjectOptions,
    ) -> Result<GetObjectReader> {
        // Acquire a shared read-lock early to protect read consistency
        let read_lock_guard = if !opts.no_lock {
            Some(
                self.new_ns_lock(bucket, object)
                    .await?
                    .get_read_lock(get_lock_acquire_timeout())
                    .await
                    .map_err(|e| {
                        Error::other(format!(
                            "Failed to acquire read lock: {}",
                            self.format_lock_error_from_error(bucket, object, "read", &e)
                        ))
                    })?,
            )
        } else {
            None
        };

        let (fi, files, disks) = self
            .get_object_fileinfo(bucket, object, opts, true)
            .await
            .map_err(|err| to_object_err(err, vec![bucket, object]))?;
        let object_info = ObjectInfo::from_file_info(&fi, bucket, object, opts.versioned || opts.version_suspended);

        if object_info.delete_marker {
            if opts.version_id.is_none() {
                return Err(to_object_err(Error::FileNotFound, vec![bucket, object]));
            }
            return Err(to_object_err(Error::MethodNotAllowed, vec![bucket, object]));
        }

        // if object_info.size == 0 {
        //     let empty_rd: Box<dyn AsyncRead> = Box::new(Bytes::new());

        //     return Ok(GetObjectReader {
        //         stream: empty_rd,
        //         object_info,
        //     });
        // }

        if object_info.size == 0 {
            // if let Some(rs) = range {
            //     let _ = rs.get_offset_length(object_info.size)?;
            // }

            let reader = GetObjectReader {
                stream: Box::new(Cursor::new(Vec::new())),
                object_info,
            };
            return Ok(reader);
        }

        if object_info.is_remote() {
            let mut opts = opts.clone();
            if object_info.parts.len() == 1 {
                opts.part_number = Some(1);
            }
            let gr = get_transitioned_object_reader(bucket, object, &range, &h, &object_info, &opts).await?;
            return Ok(gr);
        }

        let (rd, wd) = tokio::io::duplex(DEFAULT_READ_BUFFER_SIZE);

        let (reader, offset, length) = GetObjectReader::new(Box::new(rd), range, &object_info, opts, &h)?;

        // let disks = disks.clone();
        let bucket = bucket.to_owned();
        let object = object.to_owned();
        let set_index = self.set_index;
        let pool_index = self.pool_index;
        // Move the read-lock guard into the task so it lives for the duration of the read
        // let _guard_to_hold = _read_lock_guard; // moved into closure below
        tokio::spawn(async move {
            let _guard = read_lock_guard; // keep guard alive until task ends
            let mut writer = wd;
            if let Err(e) = Self::get_object_with_fileinfo(
                &bucket,
                &object,
                offset,
                length,
                &mut writer,
                fi,
                files,
                &disks,
                set_index,
                pool_index,
            )
            .await
            {
                error!("get_object_with_fileinfo  {bucket}/{object} err {:?}", e);
            };
        });

        Ok(reader)
    }

    #[tracing::instrument(level = "debug", skip(self, data,))]
    async fn put_object(&self, bucket: &str, object: &str, data: &mut PutObjReader, opts: &ObjectOptions) -> Result<ObjectInfo> {
        let disks = self.get_disks_internal().await;

        let mut object_lock_guard = None;

        if let Some(http_preconditions) = opts.http_preconditions.clone() {
            if !opts.no_lock {
                let ns_lock = self.new_ns_lock(bucket, object).await?;
                object_lock_guard = Some(ns_lock.get_write_lock(get_lock_acquire_timeout()).await.map_err(|e| {
                    StorageError::other(format!(
                        "Failed to acquire write lock: {}",
                        self.format_lock_error_from_error(bucket, object, "write", &e)
                    ))
                })?);
            }

            if let Some(err) = self.check_write_precondition(bucket, object, opts).await {
                return Err(err);
            }
        }

        let mut user_defined = opts.user_defined.clone();

        let sc_parity_drives = {
            if let Some(sc) = GLOBAL_STORAGE_CLASS.get() {
                sc.get_parity_for_sc(user_defined.get(AMZ_STORAGE_CLASS).cloned().unwrap_or_default().as_str())
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

        // if filtered_online < write_quorum {
        //     warn!(
        //         "online disk snapshot {} below write quorum {} for {}/{}; returning erasure write quorum error",
        //         filtered_online, write_quorum, bucket, object
        //     );
        //     return Err(to_object_err(Error::ErasureWriteQuorum, vec![bucket, object]));
        // }

        let mut fi = FileInfo::new([bucket, object].join("/").as_str(), data_drives, parity_drives);

        fi.version_id = {
            if let Some(ref vid) = opts.version_id {
                Some(Uuid::parse_str(vid.as_str()).map_err(Error::other)?)
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

        let tmp_dir = Uuid::new_v4().to_string();

        let tmp_object = format!("{}/{}/part.1", tmp_dir, fi.data_dir.unwrap());

        let erasure = erasure_coding::Erasure::new(fi.erasure.data_blocks, fi.erasure.parity_blocks, fi.erasure.block_size);

        let is_inline_buffer = {
            if let Some(sc) = GLOBAL_STORAGE_CLASS.get() {
                sc.should_inline(erasure.shard_file_size(data.size()), opts.versioned)
            } else {
                false
            }
        };

        let mut writers = Vec::with_capacity(shuffle_disks.len());
        let mut errors = Vec::with_capacity(shuffle_disks.len());
        for disk_op in shuffle_disks.iter() {
            if let Some(disk) = disk_op
                && disk.is_online().await
            {
                let writer = match create_bitrot_writer(
                    is_inline_buffer,
                    Some(disk),
                    RUSTFS_META_TMP_BUCKET,
                    &tmp_object,
                    erasure.shard_file_size(data.size()),
                    erasure.shard_size(),
                    HashAlgorithm::HighwayHash256,
                )
                .await
                {
                    Ok(writer) => writer,
                    Err(err) => {
                        warn!("create_bitrot_writer  disk {}, err {:?}, skipping operation", disk.to_string(), err);
                        errors.push(Some(err));
                        writers.push(None);
                        continue;
                    }
                };

                writers.push(Some(writer));
                errors.push(None);
            } else {
                errors.push(Some(DiskError::DiskNotFound));
                writers.push(None);
            }
        }

        let nil_count = errors.iter().filter(|&e| e.is_none()).count();
        if nil_count < write_quorum {
            error!("not enough disks to write: {:?}", errors);
            if let Some(write_err) = reduce_write_quorum_errs(&errors, OBJECT_OP_IGNORED_ERRS, write_quorum) {
                return Err(to_object_err(write_err.into(), vec![bucket, object]));
            }

            return Err(Error::other(format!("not enough disks to write: {errors:?}")));
        }

        let stream = mem::replace(
            &mut data.stream,
            HashReader::new(Box::new(WarpReader::new(Cursor::new(Vec::new()))), 0, 0, None, None, false)?,
        );

        let (reader, w_size) = match Arc::new(erasure).encode(stream, &mut writers, write_quorum).await {
            Ok((r, w)) => (r, w),
            Err(e) => {
                error!("encode err {:?}", e);
                return Err(e.into());
            }
        }; // TODO: delete temporary directory on error

        let _ = mem::replace(&mut data.stream, reader);
        // if let Err(err) = close_bitrot_writers(&mut writers).await {
        //     error!("close_bitrot_writers err {:?}", err);
        // }

        if (w_size as i64) < data.size() {
            warn!("put_object write size < data.size(), w_size={}, data.size={}", w_size, data.size());
            return Err(Error::other(format!(
                "put_object write size < data.size(), w_size={}, data.size={}",
                w_size,
                data.size()
            )));
        }

        if user_defined.contains_key(&format!("{RESERVED_METADATA_PREFIX_LOWER}compression")) {
            user_defined.insert(format!("{RESERVED_METADATA_PREFIX_LOWER}compression-size"), w_size.to_string());
        }

        let index_op = data.stream.try_get_index().map(|v| v.clone().into_vec());

        //TODO: userDefined

        let etag = data.stream.try_resolve_etag().unwrap_or_default();

        user_defined.insert("etag".to_owned(), etag.clone());

        if !user_defined.contains_key("content-type") {
            //  get content-type
        }

        let mut actual_size = data.actual_size();
        if actual_size < 0 {
            let is_compressed = fi.is_compressed();
            if !is_compressed {
                actual_size = w_size as i64;
            }
        }

        if fi.checksum.is_none()
            && let Some(content_hash) = data.as_hash_reader().content_hash()
        {
            fi.checksum = Some(content_hash.to_bytes(&[]));
        }

        if let Some(sc) = user_defined.get(AMZ_STORAGE_CLASS)
            && sc == storageclass::STANDARD
        {
            let _ = user_defined.remove(AMZ_STORAGE_CLASS);
        }

        let mod_time = if let Some(mod_time) = opts.mod_time {
            Some(mod_time)
        } else {
            Some(OffsetDateTime::now_utc())
        };

        for (i, pfi) in parts_metadatas.iter_mut().enumerate() {
            pfi.metadata = user_defined.clone();
            if is_inline_buffer {
                if let Some(writer) = writers[i].take() {
                    pfi.data = Some(writer.into_inline_data().map(bytes::Bytes::from).unwrap_or_default());
                }

                pfi.set_inline_data();
            }

            pfi.mod_time = mod_time;
            pfi.size = w_size as i64;
            pfi.versioned = opts.versioned || opts.version_suspended;
            pfi.add_object_part(1, etag.clone(), w_size, mod_time, actual_size, index_op.clone(), None);
            pfi.checksum = fi.checksum.clone();

            if opts.data_movement {
                pfi.set_data_moved();
            }
        }

        drop(writers); // drop writers to close all files, this is to prevent FileAccessDenied errors when renaming data

        if !opts.no_lock && object_lock_guard.is_none() {
            let ns_lock = self.new_ns_lock(bucket, object).await?;
            object_lock_guard = Some(ns_lock.get_write_lock(get_lock_acquire_timeout()).await.map_err(|e| {
                StorageError::other(format!(
                    "Failed to acquire write lock: {}",
                    self.format_lock_error_from_error(bucket, object, "write", &e)
                ))
            })?);
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

        drop(object_lock_guard); // drop object lock guard to release the lock

        self.delete_all(RUSTFS_META_TMP_BUCKET, &tmp_dir).await?;

        for (i, op_disk) in online_disks.iter().enumerate() {
            if let Some(disk) = op_disk
                && disk.is_online().await
            {
                fi = parts_metadatas[i].clone();
                break;
            }
        }

        fi.replication_state_internal = Some(opts.put_replication_state());

        fi.is_latest = true;

        Ok(ObjectInfo::from_file_info(&fi, bucket, object, opts.versioned || opts.version_suspended))
    }
}

#[async_trait::async_trait]
impl StorageAPI for SetDisks {
    #[tracing::instrument(skip(self))]
    async fn new_ns_lock(&self, bucket: &str, object: &str) -> Result<NamespaceLockWrapper> {
        let set_lock = if is_dist_erasure().await {
            // Calculate quorum based on lockers count (majority)
            let lockers_count = self.lockers.len();
            let write_quorum = if lockers_count > 1 { (lockers_count / 2) + 1 } else { 1 };
            NamespaceLock::with_clients_and_quorum(
                format!("set-{}-{}", self.pool_index, self.set_index),
                self.lockers.clone(),
                write_quorum,
            )
        } else {
            NamespaceLock::Local(LocalLock::new(
                format!("set-{}-{}", self.pool_index, self.set_index),
                self.local_lock_manager.clone(),
            ))
        };

        let resource = ObjectKey {
            bucket: Arc::from(bucket),
            object: Arc::from(object),
            version: None,
        };

        Ok(NamespaceLockWrapper::new(set_lock, resource, self.locker_owner.clone()))
    }

    #[tracing::instrument(skip(self))]
    async fn backend_info(&self) -> rustfs_madmin::BackendInfo {
        unimplemented!()
    }
    #[tracing::instrument(skip(self))]
    async fn storage_info(&self) -> rustfs_madmin::StorageInfo {
        let disks = self.get_disks_internal().await;

        get_storage_info(&disks, &self.set_endpoints).await
    }
    #[tracing::instrument(skip(self))]
    async fn local_storage_info(&self) -> rustfs_madmin::StorageInfo {
        let disks = self.get_disks_internal().await;

        let mut local_disks: Vec<Option<DiskStore>> = Vec::new();
        let mut local_endpoints = Vec::new();

        for (i, ep) in self.set_endpoints.iter().enumerate() {
            if ep.is_local {
                local_disks.push(disks[i].clone());
                local_endpoints.push(ep.clone());
            }
        }

        get_storage_info(&local_disks, &local_endpoints).await
    }
    #[tracing::instrument(skip(self))]
    async fn list_bucket(&self, _opts: &BucketOptions) -> Result<Vec<BucketInfo>> {
        unimplemented!()
    }
    #[tracing::instrument(skip(self))]
    async fn make_bucket(&self, _bucket: &str, _opts: &MakeBucketOptions) -> Result<()> {
        unimplemented!()
    }

    #[tracing::instrument(skip(self))]
    async fn get_bucket_info(&self, _bucket: &str, _opts: &BucketOptions) -> Result<BucketInfo> {
        unimplemented!()
    }

    #[tracing::instrument(skip(self))]
    async fn copy_object(
        &self,
        src_bucket: &str,
        src_object: &str,
        _dst_bucket: &str,
        _dst_object: &str,
        src_info: &mut ObjectInfo,
        src_opts: &ObjectOptions,
        dst_opts: &ObjectOptions,
    ) -> Result<ObjectInfo> {
        // FIXME: TODO:

        if !src_info.metadata_only {
            return Err(StorageError::NotImplemented);
        }

        // Guard lock for source object metadata update
        let _lock_guard = self
            .new_ns_lock(src_bucket, src_object)
            .await?
            .get_write_lock(get_lock_acquire_timeout())
            .await
            .map_err(|e| {
                Error::other(format!(
                    "Failed to acquire write lock: {}",
                    self.format_lock_error_from_error(src_bucket, src_object, "write", &e)
                ))
            })?;

        let disks = self.get_disks_internal().await;

        let (mut metas, errs) = {
            if let Some(vid) = &src_opts.version_id {
                Self::read_all_fileinfo(&disks, "", src_bucket, src_object, vid, true, false).await?
            } else {
                Self::read_all_xl(&disks, src_bucket, src_object, true, false).await
            }
        };

        let (read_quorum, write_quorum) = match Self::object_quorum_from_meta(&metas, &errs, self.default_parity_count) {
            Ok((r, w)) => (r as usize, w as usize),
            Err(mut err) => {
                if err == DiskError::ErasureReadQuorum
                    && !src_bucket.starts_with(RUSTFS_META_BUCKET)
                    && self
                        .delete_if_dangling(src_bucket, src_object, &metas, &errs, &HashMap::new(), src_opts.clone())
                        .await
                        .is_ok()
                {
                    if src_opts.version_id.is_some() {
                        err = DiskError::FileVersionNotFound
                    } else {
                        err = DiskError::FileNotFound
                    }
                }
                return Err(to_object_err(err.into(), vec![src_bucket, src_object]));
            }
        };

        let (online_disks, mod_time, etag) = Self::list_online_disks(&disks, &metas, &errs, read_quorum);

        let mut fi = Self::pick_valid_fileinfo(&metas, mod_time, etag, read_quorum)
            .map_err(|e| to_object_err(e.into(), vec![src_bucket, src_object]))?;

        if fi.deleted {
            if src_opts.version_id.is_none() {
                return Err(to_object_err(Error::FileNotFound, vec![src_bucket, src_object]));
            }
            return Err(to_object_err(Error::MethodNotAllowed, vec![src_bucket, src_object]));
        }

        let version_id = {
            if src_info.version_only {
                if let Some(vid) = &dst_opts.version_id {
                    Some(Uuid::parse_str(vid)?)
                } else {
                    Some(Uuid::new_v4())
                }
            } else {
                src_info.version_id
            }
        };

        let inline_data = fi.inline_data();
        fi.metadata = src_info.user_defined.clone();

        if let Some(etag) = &src_info.etag {
            fi.metadata.insert("etag".to_owned(), etag.clone());
        }

        let mod_time = OffsetDateTime::now_utc();

        for fi in metas.iter_mut() {
            if fi.is_valid() {
                fi.metadata = src_info.user_defined.clone();
                fi.mod_time = Some(mod_time);
                fi.version_id = version_id;
                fi.versioned = src_opts.versioned || src_opts.version_suspended;

                if !fi.inline_data() {
                    fi.data = None;
                }

                if inline_data {
                    fi.set_inline_data();
                }
            }
        }

        Self::write_unique_file_info(&online_disks, "", src_bucket, src_object, &metas, write_quorum)
            .await
            .map_err(|e| to_object_err(e.into(), vec![src_bucket, src_object]))?;

        Ok(ObjectInfo::from_file_info(
            &fi,
            src_bucket,
            src_object,
            src_opts.versioned || src_opts.version_suspended,
        ))
    }
    #[tracing::instrument(skip(self))]
    async fn delete_object_version(&self, bucket: &str, object: &str, fi: &FileInfo, force_del_marker: bool) -> Result<()> {
        let disks = self.get_disks(0, 0).await?;
        let write_quorum = disks.len() / 2 + 1;

        let mut futures = Vec::with_capacity(disks.len());
        let mut errs = Vec::with_capacity(disks.len());

        for disk in disks.iter() {
            futures.push(async move {
                if let Some(disk) = disk {
                    match disk
                        .delete_version(bucket, object, fi.clone(), force_del_marker, DeleteOptions::default())
                        .await
                    {
                        Ok(r) => Ok(r),
                        Err(e) => Err(e),
                    }
                } else {
                    Err(DiskError::DiskNotFound)
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

        if let Some(err) = reduce_write_quorum_errs(&errs, OBJECT_OP_IGNORED_ERRS, write_quorum) {
            return Err(err.into());
        }
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn delete_objects(
        &self,
        bucket: &str,
        objects: Vec<ObjectToDelete>,
        opts: ObjectOptions,
    ) -> (Vec<DeletedObject>, Vec<Option<Error>>) {
        // Default return value
        let mut del_objects = vec![DeletedObject::default(); objects.len()];

        let mut del_errs = Vec::with_capacity(objects.len());

        for _ in 0..objects.len() {
            del_errs.push(None)
        }

        // Acquire locks in batch mode (best effort, matching previous behavior)
        let mut batch = rustfs_lock::BatchLockRequest::new(self.locker_owner.as_str()).with_all_or_nothing(false);
        let mut unique_objects: HashSet<String> = HashSet::new();
        for dobj in &objects {
            if unique_objects.insert(dobj.object_name.clone()) {
                batch = batch.add_write_lock(rustfs_lock::ObjectKey::new(bucket, dobj.object_name.clone()));
            }
        }

        let mut failed_map = HashMap::new();
        let mut batch_guards = Vec::with_capacity(batch.requests.len());

        let mut locked_objects = HashSet::new();

        for req in batch.requests.iter() {
            let ns_lock = match self.new_ns_lock(req.key.bucket.as_ref(), req.key.object.as_ref()).await {
                Ok(ns_lock) => ns_lock,
                Err(e) => {
                    failed_map.insert((req.key.bucket.as_ref().to_string(), req.key.object.as_ref().to_string()), e.to_string());
                    continue;
                }
            };
            let _lock_guard = match ns_lock.get_write_lock(get_lock_acquire_timeout()).await {
                Ok(lock_guard) => lock_guard,
                Err(e) => {
                    failed_map.insert((req.key.bucket.as_ref().to_string(), req.key.object.as_ref().to_string()), e.to_string());
                    continue;
                }
            };
            batch_guards.push(_lock_guard);
            locked_objects.insert(req.key.object.as_ref().to_string());
        }

        // Mark failures for objects that could not be locked
        for (i, dobj) in objects.iter().enumerate() {
            if let Some(err) = failed_map.get(&(bucket.to_string(), dobj.object_name.clone())) {
                del_errs[i] = Some(Error::other(err.to_string()));
            }
        }

        let ver_cfg = BucketVersioningSys::get(bucket).await.unwrap_or_default();

        let mut vers_map: HashMap<&String, FileInfoVersions> = HashMap::new();

        for (i, dobj) in objects.iter().enumerate() {
            let mut vr = FileInfo {
                name: dobj.object_name.clone(),
                version_id: dobj.version_id,
                idx: i,
                replication_state_internal: Some(dobj.replication_state()),
                ..Default::default()
            };

            vr.set_tier_free_version_id(&Uuid::new_v4().to_string());

            // Delete
            // del_objects[i].object_name.clone_from(&vr.name);
            // del_objects[i].version_id = vr.version_id.map(|v| v.to_string());

            if dobj.version_id.is_none() {
                let (suspended, versioned) = (ver_cfg.suspended(), ver_cfg.prefix_enabled(dobj.object_name.as_str()));
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
                del_objects[i] = DeletedObject {
                    delete_marker: vr.deleted,
                    delete_marker_version_id: vr.version_id,
                    delete_marker_mtime: vr.mod_time,
                    object_name: vr.name.clone(),
                    replication_state: vr.replication_state_internal.clone(),
                    ..Default::default()
                }
            } else {
                del_objects[i] = DeletedObject {
                    object_name: vr.name.clone(),
                    version_id: vr.version_id,
                    replication_state: vr.replication_state_internal.clone(),
                    ..Default::default()
                }
            }

            // Only add to vers_map if we hold the lock
            if locked_objects.contains(&dobj.object_name) {
                vers_map.insert(&dobj.object_name, v);
            }
        }

        let mut vers = Vec::with_capacity(vers_map.len());

        for (_, mut fi_vers) in vers_map {
            fi_vers.versions.sort_by(|a, b| a.deleted.cmp(&b.deleted));

            if let Some(index) = fi_vers.versions.iter().position(|fi| fi.deleted) {
                fi_vers.versions.truncate(index + 1);
            }

            vers.push(fi_vers);
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
                    let mut errs = Vec::with_capacity(vers.len());
                    for _ in 0..vers.len() {
                        errs.push(Some(DiskError::DiskNotFound));
                    }
                    errs
                }
            });
        }

        let results = join_all(futures).await;

        let mut del_obj_errs: Vec<Vec<Option<DiskError>>> = vec![vec![None; objects.len()]; disks.len()];

        // For each disk delete all objects
        for (disk_idx, errors) in results.into_iter().enumerate() {
            // Deletion results for all objects
            for idx in 0..vers.len() {
                if errors[idx].is_some() {
                    for fi in vers[idx].versions.iter() {
                        del_obj_errs[disk_idx][fi.idx] = errors[idx].clone();
                    }
                }
            }
        }

        for obj_idx in 0..objects.len() {
            let mut disk_err = vec![None; disks.len()];

            for disk_idx in 0..disks.len() {
                if del_obj_errs[disk_idx][obj_idx].is_some() {
                    disk_err[disk_idx] = del_obj_errs[disk_idx][obj_idx].clone();
                }
            }

            let mut has_err = reduce_write_quorum_errs(&disk_err, OBJECT_OP_IGNORED_ERRS, disks.len() / 2 + 1);
            if let Some(err) = has_err.clone() {
                let er = err.into();
                if (is_err_object_not_found(&er) || is_err_version_not_found(&er)) && !del_objects[obj_idx].delete_marker {
                    has_err = None;
                }
            } else {
                del_objects[obj_idx].found = true;
            }

            if let Some(err) = has_err {
                if del_objects[obj_idx].version_id.is_some() {
                    del_errs[obj_idx] = Some(to_object_err(
                        err.into(),
                        vec![
                            bucket,
                            &objects[obj_idx].object_name.clone(),
                            &objects[obj_idx].version_id.unwrap_or_default().to_string(),
                        ],
                    ));
                } else {
                    del_errs[obj_idx] = Some(to_object_err(err.into(), vec![bucket, &objects[obj_idx].object_name.clone()]));
                }
            }
        }

        // TODO: add_partial

        (del_objects, del_errs)
    }

    #[tracing::instrument(skip(self))]
    async fn delete_object(&self, bucket: &str, object: &str, mut opts: ObjectOptions) -> Result<ObjectInfo> {
        // Guard lock for single object delete
        let _lock_guard = if !opts.delete_prefix {
            Some(
                self.new_ns_lock(bucket, object)
                    .await?
                    .get_write_lock(get_lock_acquire_timeout())
                    .await
                    .map_err(|e| {
                        Error::other(format!(
                            "Failed to acquire write lock: {}",
                            self.format_lock_error_from_error(bucket, object, "write", &e)
                        ))
                    })?,
            )
        } else {
            None
        };
        if opts.delete_prefix {
            self.delete_prefix(bucket, object)
                .await
                .map_err(|e| to_object_err(e.into(), vec![bucket, object]))?;

            return Ok(ObjectInfo::default());
        }

        // TODO: Lifecycle

        let mut version_found = true;
        let (mut goi, write_quorum, gerr) = self.get_object_info_and_quorum(bucket, object, &opts).await;
        if let Some(err) = &gerr
            && goi.name.is_empty()
        {
            if opts.delete_marker {
                version_found = false;
            } else {
                return Err(err.clone());
            }
        }

        let otd = ObjectToDelete {
            object_name: object.to_string(),
            version_id: opts
                .version_id
                .clone()
                .map(|v| Uuid::parse_str(v.as_str()).ok().unwrap_or_default()),
            ..Default::default()
        };

        let dsc = if opts
            .delete_replication
            .as_ref()
            .map(|v| v.replica_status == ReplicationStatusType::Replica)
            == Some(true)
            || opts.version_purge_status() == VersionPurgeStatusType::Complete
        {
            ReplicateDecision::default()
        } else {
            check_replicate_delete(bucket, &otd, &goi, &opts, gerr.map(|e| e.to_string())).await
        };

        if dsc.replicate_any() {
            opts.set_delete_replication_state(dsc);
            goi.replication_decision = opts
                .delete_replication
                .as_ref()
                .map(|v| v.replicate_decision_str.clone())
                .unwrap_or_default();
        }

        let mut mark_delete = goi.version_id.is_some();

        let mut delete_marker = opts.versioned;

        if opts.version_id.is_some() {
            if version_found && opts.delete_marker_replication_status() == ReplicationStatusType::Replica {
                mark_delete = false;
            }

            if opts.version_purge_status().is_empty() && opts.delete_marker_replication_status().is_empty() {
                mark_delete = false;
            }

            if opts.version_purge_status() == VersionPurgeStatusType::Complete {
                mark_delete = false;
            }

            if version_found && (!goi.version_purge_status.is_empty() || !goi.delete_marker) {
                delete_marker = false;
            }
        }

        let mod_time = if let Some(mt) = opts.mod_time {
            mt
        } else {
            OffsetDateTime::now_utc()
        };

        let find_vid = Uuid::new_v4();

        if mark_delete && (opts.versioned || opts.version_suspended) {
            if !delete_marker {
                delete_marker = opts.version_suspended && opts.version_id.is_none();
            }

            let mut fi = FileInfo {
                name: object.to_string(),
                deleted: delete_marker,
                mark_deleted: mark_delete,
                mod_time: Some(mod_time),
                replication_state_internal: opts.delete_replication.clone(),
                ..Default::default() // TODO: Transition
            };

            fi.set_tier_free_version_id(&find_vid.to_string());

            if opts.skip_free_version {
                fi.set_skip_tier_free_version();
            }

            fi.version_id = if let Some(vid) = opts.version_id {
                Some(Uuid::parse_str(vid.as_str())?)
            } else if opts.versioned {
                Some(Uuid::new_v4())
            } else {
                None
            };

            self.delete_object_version(bucket, object, &fi, opts.delete_marker)
                .await
                .map_err(|e| to_object_err(e, vec![bucket, object]))?;

            let mut oi = ObjectInfo::from_file_info(&fi, bucket, object, opts.versioned || opts.version_suspended);
            oi.replication_decision = goi.replication_decision;
            return Ok(oi);
        }

        let version_id = opts.version_id.as_ref().and_then(|v| Uuid::parse_str(v).ok());

        // Create a single object deletion request
        let mut dfi = FileInfo {
            name: object.to_string(),
            version_id: opts.version_id.as_ref().and_then(|v| Uuid::parse_str(v).ok()),
            mark_deleted: mark_delete,
            deleted: delete_marker,
            mod_time: Some(mod_time),
            replication_state_internal: opts.delete_replication.clone(),
            ..Default::default()
        };

        dfi.set_tier_free_version_id(&find_vid.to_string());

        if opts.skip_free_version {
            dfi.set_skip_tier_free_version();
        }

        self.delete_object_version(bucket, object, &dfi, opts.delete_marker)
            .await
            .map_err(|e| to_object_err(e, vec![bucket, object]))?;

        let mut obj_info = ObjectInfo::from_file_info(&dfi, bucket, object, opts.versioned || opts.version_suspended);
        obj_info.size = goi.size;
        Ok(obj_info)
    }

    #[tracing::instrument(skip(self))]
    async fn list_objects_v2(
        self: Arc<Self>,
        _bucket: &str,
        _prefix: &str,
        _continuation_token: Option<String>,
        _delimiter: Option<String>,
        _max_keys: i32,
        _fetch_owner: bool,
        _start_after: Option<String>,
        _incl_deleted: bool,
    ) -> Result<ListObjectsV2Info> {
        unimplemented!()
    }

    #[tracing::instrument(skip(self))]
    async fn list_object_versions(
        self: Arc<Self>,
        _bucket: &str,
        _prefix: &str,
        _marker: Option<String>,
        _version_marker: Option<String>,
        _delimiter: Option<String>,
        _max_keys: i32,
    ) -> Result<ListObjectVersionsInfo> {
        unimplemented!()
    }

    async fn walk(
        self: Arc<Self>,
        _rx: CancellationToken,
        _bucket: &str,
        _prefix: &str,
        _result: Sender<ObjectInfoOrErr>,
        _opts: WalkOptions,
    ) -> Result<()> {
        unimplemented!()
    }

    #[tracing::instrument(skip(self))]
    async fn get_object_info(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<ObjectInfo> {
        // Acquire a shared read-lock to protect consistency during info fetch
        let _read_lock_guard = if !opts.no_lock {
            Some(
                self.new_ns_lock(bucket, object)
                    .await?
                    .get_read_lock(get_lock_acquire_timeout())
                    .await
                    .map_err(|e| {
                        Error::other(format!(
                            "Failed to acquire read lock: {}",
                            self.format_lock_error_from_error(bucket, object, "read", &e)
                        ))
                    })?,
            )
        } else {
            None
        };

        let (fi, _, _) = self
            .get_object_fileinfo(bucket, object, opts, false)
            .await
            .map_err(|e| to_object_err(e, vec![bucket, object]))?;

        let oi = ObjectInfo::from_file_info(&fi, bucket, object, opts.versioned || opts.version_suspended);

        Ok(oi)
    }

    #[tracing::instrument(skip(self))]
    async fn add_partial(&self, bucket: &str, object: &str, version_id: &str) -> Result<()> {
        if let Err(e) =
            rustfs_common::heal_channel::send_heal_request(rustfs_common::heal_channel::create_heal_request_with_options(
                bucket.to_string(),
                Some(object.to_string()),
                false,
                Some(HealChannelPriority::Normal),
                Some(self.pool_index),
                Some(self.set_index),
            ))
            .await
        {
            warn!(
                bucket,
                object,
                version_id,
                error = %e,
                "Failed to enqueue heal request for partial object"
            );
        }
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn put_object_metadata(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<ObjectInfo> {
        // TODO: nslock

        // Guard lock for metadata update
        let _lock_guard = if !opts.no_lock {
            Some(
                self.new_ns_lock(bucket, object)
                    .await?
                    .get_write_lock(get_lock_acquire_timeout())
                    .await
                    .map_err(|e| {
                        Error::other(format!(
                            "Failed to acquire write lock: {}",
                            self.format_lock_error_from_error(bucket, object, "write", &e)
                        ))
                    })?,
            )
        } else {
            None
        };

        let disks = self.get_disks_internal().await;

        let (metas, errs) = {
            if let Some(version_id) = &opts.version_id {
                Self::read_all_fileinfo(&disks, "", bucket, object, version_id.to_string().as_str(), false, false).await?
            } else {
                Self::read_all_xl(&disks, bucket, object, false, false).await
            }
        };

        let read_quorum = match Self::object_quorum_from_meta(&metas, &errs, self.default_parity_count) {
            Ok((res, _)) => res,
            Err(mut err) => {
                if err == DiskError::ErasureReadQuorum
                    && !bucket.starts_with(RUSTFS_META_BUCKET)
                    && self
                        .delete_if_dangling(bucket, object, &metas, &errs, &HashMap::new(), opts.clone())
                        .await
                        .is_ok()
                {
                    if opts.version_id.is_some() {
                        err = DiskError::FileVersionNotFound
                    } else {
                        err = DiskError::FileNotFound
                    }
                }
                return Err(to_object_err(err.into(), vec![bucket, object]));
            }
        };

        let read_quorum = read_quorum as usize;

        let (online_disks, mod_time, etag) = Self::list_online_disks(&disks, &metas, &errs, read_quorum);

        let mut fi = Self::pick_valid_fileinfo(&metas, mod_time, etag, read_quorum)
            .map_err(|e| to_object_err(e.into(), vec![bucket, object]))?;

        if fi.deleted {
            return Err(to_object_err(Error::MethodNotAllowed, vec![bucket, object]));
        }

        let obj_info = ObjectInfo::from_file_info(&fi, bucket, object, opts.versioned || opts.version_suspended);

        for (k, v) in obj_info.user_defined {
            fi.metadata.insert(k, v);
        }

        if let Some(mt) = &opts.eval_metadata {
            for (k, v) in mt {
                fi.metadata.insert(k.clone(), v.clone());
            }
        }

        if opts.mod_time.is_some() {
            fi.mod_time = opts.mod_time;
        }
        if let Some(ref version_id) = opts.version_id {
            fi.version_id = Uuid::parse_str(version_id).ok();
        }

        self.update_object_meta(bucket, object, fi.clone(), &online_disks)
            .await
            .map_err(|e| to_object_err(e.into(), vec![bucket, object]))?;

        Ok(ObjectInfo::from_file_info(&fi, bucket, object, opts.versioned || opts.version_suspended))
    }

    #[tracing::instrument(skip(self))]
    async fn get_object_tags(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<String> {
        let oi = self.get_object_info(bucket, object, opts).await?;
        Ok(oi.user_tags)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn transition_object(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<()> {
        let mut tier_config_mgr = GLOBAL_TierConfigMgr.write().await;
        let tgt_client = match tier_config_mgr.get_driver(&opts.transition.tier).await {
            Ok(client) => client,
            Err(err) => {
                return Err(Error::other(format!("remote tier error: {err}")));
            }
        };

        // Acquire write-lock early; hold for the whole transition operation scope
        // if !opts.no_lock {
        //     let guard_opt = self
        //         .namespace_lock
        //         .lock_guard(object, &self.locker_owner, Duration::from_secs(5), Duration::from_secs(10))
        //         .await?;
        //     if guard_opt.is_none() {
        //         return Err(Error::other("can not get lock. please retry".to_string()));
        //     }
        //     _lock_guard = guard_opt;
        // }

        let (mut fi, meta_arr, online_disks) = self.get_object_fileinfo(bucket, object, opts, true).await?;
        /*if err != nil {
            return Err(to_object_err(err, vec![bucket, object]));
        }*/
        /*if fi.deleted {
            if opts.version_id.is_none() {
                return Err(to_object_err(DiskError::FileNotFound, vec![bucket, object]));
            }
            return Err(to_object_err(ERR_METHOD_NOT_ALLOWED, vec![bucket, object]));
        }*/
        // Normalize ETags by removing quotes before comparison (PR #592 compatibility)
        let transition_etag = rustfs_utils::path::trim_etag(&opts.transition.etag);
        let stored_etag = rustfs_utils::path::trim_etag(&get_raw_etag(&fi.metadata));
        if let Some(mod_time1) = opts.mod_time {
            if let Some(mod_time2) = fi.mod_time.as_ref() {
                if mod_time1.unix_timestamp() != mod_time2.unix_timestamp()
                /*|| transition_etag != stored_etag*/
                {
                    return Err(to_object_err(Error::other(DiskError::FileNotFound), vec![bucket, object]));
                }
            } else {
                return Err(Error::other("mod_time 2 error.".to_string()));
            }
        } else {
            return Err(Error::other("mod_time 1 error.".to_string()));
        }
        if fi.transition_status == TRANSITION_COMPLETE {
            return Ok(());
        }

        /*if fi.xlv1 {
            if let Err(err) = self.heal_object(bucket, object, "", &HealOpts {no_lock: true, ..Default::default()}) {
                return err.expect("err");
            }
            (fi, meta_arr, online_disks) = self.get_object_fileinfo(&bucket, &object, &opts, true);
            if err != nil {
                return to_object_err(err, vec![bucket, object]);
            }
        }*/

        let dest_obj = gen_transition_objname(bucket);
        if let Err(err) = dest_obj {
            return Err(to_object_err(err, vec![]));
        }
        let dest_obj = dest_obj.unwrap();

        let oi = ObjectInfo::from_file_info(&fi, bucket, object, opts.versioned || opts.version_suspended);

        let (pr, mut pw) = tokio::io::duplex(fi.erasure.block_size);
        let reader = ReaderImpl::ObjectBody(GetObjectReader {
            stream: Box::new(pr),
            object_info: oi,
        });

        let cloned_bucket = bucket.to_string();
        let cloned_object = object.to_string();
        let cloned_fi = fi.clone();
        let set_index = self.set_index;
        let pool_index = self.pool_index;
        tokio::spawn(async move {
            if let Err(e) = Self::get_object_with_fileinfo(
                &cloned_bucket,
                &cloned_object,
                0,
                cloned_fi.size,
                &mut pw,
                cloned_fi,
                meta_arr,
                &online_disks,
                set_index,
                pool_index,
            )
            .await
            {
                error!("get_object_with_fileinfo err {:?}", e);
            };
        });

        let rv = tgt_client
            .put_with_meta(&dest_obj, reader, fi.size, {
                let mut m = HashMap::<String, String>::new();
                m.insert("name".to_string(), object.to_string());
                m
            })
            .await;
        if let Err(err) = rv {
            return Err(StorageError::Io(err));
        }
        let rv = rv.unwrap();
        fi.transition_status = TRANSITION_COMPLETE.to_string();
        fi.transitioned_objname = dest_obj;
        fi.transition_tier = opts.transition.tier.clone();
        fi.transition_version_id = if rv.is_empty() { None } else { Some(Uuid::parse_str(&rv)?) };
        let mut event_name = EventName::ObjectTransitionComplete.as_ref();

        let disks = self.get_disks(0, 0).await?;

        if let Err(err) = self.delete_object_version(bucket, object, &fi, false).await {
            event_name = EventName::ObjectTransitionFailed.as_ref();
        }

        for disk in disks.iter() {
            if let Some(disk) = disk {
                continue;
            }
            let _ = self.add_partial(bucket, object, opts.version_id.as_ref().expect("err")).await;
            break;
        }

        let obj_info = ObjectInfo::from_file_info(&fi, bucket, object, opts.versioned || opts.version_suspended);
        send_event(EventArgs {
            event_name: event_name.to_string(),
            bucket_name: bucket.to_string(),
            object: obj_info,
            user_agent: "Internal: [ILM-Transition]".to_string(),
            host: GLOBAL_LocalNodeName.to_string(),
            ..Default::default()
        });
        //let tags = opts.lifecycle_audit_event.tags();
        //auditLogLifecycle(ctx, objInfo, ILMTransition, tags, traceFn)
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn restore_transitioned_object(self: Arc<Self>, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<()> {
        // Acquire write-lock early for the restore operation
        // if !opts.no_lock {
        //     let guard_opt = self
        //         .namespace_lock
        //         .lock_guard(object, &self.locker_owner, Duration::from_secs(5), Duration::from_secs(10))
        //         .await?;
        //     if guard_opt.is_none() {
        //         return Err(Error::other("can not get lock. please retry".to_string()));
        //     }
        //     _lock_guard = guard_opt;
        // }
        let self_ = self.clone();
        let set_restore_header_fn = async move |oi: &mut ObjectInfo, rerr: Option<Error>| -> Result<()> {
            if rerr.is_none() {
                return Ok(());
            }
            self.update_restore_metadata(bucket, object, oi, opts).await?;
            Err(rerr.unwrap())
        };
        let mut oi = ObjectInfo::default();
        let fi = self_.clone().get_object_fileinfo(bucket, object, opts, true).await;
        if let Err(err) = fi {
            return set_restore_header_fn(&mut oi, Some(to_object_err(err, vec![bucket, object]))).await;
        }
        let (actual_fi, _, _) = fi.unwrap();

        oi = ObjectInfo::from_file_info(&actual_fi, bucket, object, opts.versioned || opts.version_suspended);
        let ropts = put_restore_opts(bucket, object, &opts.transition.restore_request, &oi).await?;
        if oi.parts.len() == 1 {
            let mut opts = opts.clone();
            opts.part_number = Some(1);
            let rs: Option<HTTPRangeSpec> = None;
            let gr = get_transitioned_object_reader(bucket, object, &rs, &HeaderMap::new(), &oi, &opts).await;
            if let Err(err) = gr {
                return set_restore_header_fn(&mut oi, Some(to_object_err(err.into(), vec![bucket, object]))).await;
            }
            let gr = gr.unwrap();
            let reader = BufReader::new(gr.stream);
            let hash_reader = HashReader::new(
                Box::new(WarpReader::new(reader)),
                gr.object_info.size,
                gr.object_info.size,
                None,
                None,
                false,
            )?;
            let mut p_reader = PutObjReader::new(hash_reader);
            return if let Err(err) = self_.clone().put_object(bucket, object, &mut p_reader, &ropts).await {
                set_restore_header_fn(&mut oi, Some(to_object_err(err, vec![bucket, object]))).await
            } else {
                Ok(())
            };
        }

        let res = self_.clone().new_multipart_upload(bucket, object, &ropts).await?;
        //if err != nil {
        //    return set_restore_header_fn(&mut oi, err).await;
        //}

        let mut uploaded_parts: Vec<CompletePart> = vec![];
        let rs: Option<HTTPRangeSpec> = None;
        let gr = get_transitioned_object_reader(bucket, object, &rs, &HeaderMap::new(), &oi, opts).await;
        if let Err(err) = gr {
            return set_restore_header_fn(&mut oi, Some(StorageError::Io(err))).await;
        }
        let gr = gr.unwrap();

        for part_info in &oi.parts {
            let reader = BufReader::new(Cursor::new(vec![] /*gr.stream*/));
            let hash_reader = HashReader::new(
                Box::new(WarpReader::new(reader)),
                part_info.size as i64,
                part_info.size as i64,
                None,
                None,
                false,
            )?;
            let mut p_reader = PutObjReader::new(hash_reader);
            let p_info = self_
                .clone()
                .put_object_part(bucket, object, &res.upload_id, part_info.number, &mut p_reader, &ObjectOptions::default())
                .await?;
            //if let Err(err) = p_info {
            //    return set_restore_header_fn(&mut oi, err).await;
            //}
            if p_info.size != part_info.size {
                return set_restore_header_fn(
                    &mut oi,
                    Some(Error::other(ObjectApiError::InvalidObjectState(GenericError {
                        bucket: bucket.to_string(),
                        object: object.to_string(),
                        ..Default::default()
                    }))),
                )
                .await;
            }
            uploaded_parts.push(CompletePart {
                part_num: p_info.part_num,
                etag: p_info.etag,
                checksum_crc32: None,
                checksum_crc32c: None,
                checksum_sha1: None,
                checksum_sha256: None,
                checksum_crc64nvme: None,
            });
        }
        if let Err(err) = self_
            .clone()
            .complete_multipart_upload(
                bucket,
                object,
                &res.upload_id,
                uploaded_parts,
                &ObjectOptions {
                    mod_time: oi.mod_time,
                    ..Default::default()
                },
            )
            .await
        {
            return set_restore_header_fn(&mut oi, Some(err)).await;
        }
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn put_object_tags(&self, bucket: &str, object: &str, tags: &str, opts: &ObjectOptions) -> Result<ObjectInfo> {
        // Acquire write-lock for tag update (metadata write)
        // if !opts.no_lock {
        //     let guard_opt = self
        //         .namespace_lock
        //         .lock_guard(object, &self.locker_owner, Duration::from_secs(5), Duration::from_secs(10))
        //         .await?;
        //     if guard_opt.is_none() {
        //         return Err(Error::other("can not get lock. please retry".to_string()));
        //     }
        //     _lock_guard = guard_opt;
        // }
        let (mut fi, _, disks) = self.get_object_fileinfo(bucket, object, opts, false).await?;

        fi.metadata.insert(AMZ_OBJECT_TAGGING.to_owned(), tags.to_owned());

        // TODO: userdeefined

        self.update_object_meta(bucket, object, fi.clone(), disks.as_slice()).await?;

        // TODO: versioned
        Ok(ObjectInfo::from_file_info(&fi, bucket, object, opts.versioned || opts.version_suspended))
    }

    #[tracing::instrument(skip(self))]
    async fn delete_object_tags(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<ObjectInfo> {
        self.put_object_tags(bucket, object, "", opts).await
    }

    #[tracing::instrument(skip(self))]
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

    #[tracing::instrument(level = "debug", skip(self, data, opts))]
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

        let (fi, _) = self.check_upload_id_exists(bucket, object, upload_id, true).await?;

        let write_quorum = fi.write_quorum(self.default_write_quorum());

        if let Some(checksum) = fi.metadata.get(rustfs_rio::RUSTFS_MULTIPART_CHECKSUM)
            && !checksum.is_empty()
            && data
                .as_hash_reader()
                .content_crc_type()
                .is_none_or(|v| v.to_string() != *checksum)
        {
            return Err(Error::other(format!("checksum mismatch: {checksum}")));
        }

        let disks = self.get_disks_internal().await;
        // let (disks, filtered_online) = self.filter_online_disks(disks_snapshot).await;

        // if filtered_online < write_quorum {
        //     warn!(
        //         "online disk snapshot {} below write quorum {} for multipart {}/{}; returning erasure write quorum error",
        //         filtered_online, write_quorum, bucket, object
        //     );
        //     return Err(to_object_err(Error::ErasureWriteQuorum, vec![bucket, object]));
        // }

        let shuffle_disks = Self::shuffle_disks(&disks, &fi.erasure.distribution);

        let part_suffix = format!("part.{part_id}");
        let tmp_part = format!("{}x{}", Uuid::new_v4(), OffsetDateTime::now_utc().unix_timestamp());
        let tmp_part_path = Arc::new(format!("{tmp_part}/{part_suffix}"));

        let erasure = erasure_coding::Erasure::new(fi.erasure.data_blocks, fi.erasure.parity_blocks, fi.erasure.block_size);

        let mut writers = Vec::with_capacity(shuffle_disks.len());
        let mut errors = Vec::with_capacity(shuffle_disks.len());
        for disk_op in shuffle_disks.iter() {
            if let Some(disk) = disk_op {
                let writer = match create_bitrot_writer(
                    false,
                    Some(disk),
                    RUSTFS_META_TMP_BUCKET,
                    &tmp_part_path,
                    erasure.shard_file_size(data.size()),
                    erasure.shard_size(),
                    HashAlgorithm::HighwayHash256,
                )
                .await
                {
                    Ok(writer) => writer,
                    Err(err) => {
                        warn!("create_bitrot_writer  disk {}, err {:?}, skipping operation", disk.to_string(), err);
                        errors.push(Some(err));
                        writers.push(None);
                        continue;
                    }
                };

                writers.push(Some(writer));
                errors.push(None);
            } else {
                errors.push(Some(DiskError::DiskNotFound));
                writers.push(None);
            }
        }

        let nil_count = errors.iter().filter(|&e| e.is_none()).count();
        if nil_count < write_quorum {
            if let Some(write_err) = reduce_write_quorum_errs(&errors, OBJECT_OP_IGNORED_ERRS, write_quorum) {
                return Err(to_object_err(write_err.into(), vec![bucket, object]));
            }

            return Err(Error::other(format!("not enough disks to write: {errors:?}")));
        }

        let stream = mem::replace(
            &mut data.stream,
            HashReader::new(Box::new(WarpReader::new(Cursor::new(Vec::new()))), 0, 0, None, None, false)?,
        );

        let (reader, w_size) = Arc::new(erasure).encode(stream, &mut writers, write_quorum).await?; // TODO: delete temporary directory on error

        let _ = mem::replace(&mut data.stream, reader);

        if (w_size as i64) < data.size() {
            warn!("put_object_part write size < data.size(), w_size={}, data.size={}", w_size, data.size());
            return Err(Error::other(format!(
                "put_object_part write size < data.size(), w_size={}, data.size={}",
                w_size,
                data.size()
            )));
        }

        let index_op = data.stream.try_get_index().map(|v| v.clone().into_vec());

        let mut etag = data.stream.try_resolve_etag().unwrap_or_default();

        if let Some(ref tag) = opts.preserve_etag {
            etag = tag.clone();
        }

        let mut actual_size = data.actual_size();
        if actual_size < 0 {
            let is_compressed = fi.is_compressed();
            if !is_compressed {
                actual_size = w_size as i64;
            }
        }

        let checksums = data.as_hash_reader().content_crc();

        let part_info = ObjectPartInfo {
            etag: etag.clone(),
            number: part_id,
            size: w_size,
            mod_time: Some(OffsetDateTime::now_utc()),
            actual_size,
            index: index_op,
            checksums: if checksums.is_empty() { None } else { Some(checksums) },
            ..Default::default()
        };

        let part_info_buff = part_info.marshal_msg()?;

        drop(writers); // drop writers to close all files

        let part_path = format!("{}/{}/{}", upload_id_path, fi.data_dir.unwrap_or_default(), part_suffix);
        let _ = self
            .rename_part(
                &disks,
                RUSTFS_META_TMP_BUCKET,
                &tmp_part_path,
                RUSTFS_META_MULTIPART_BUCKET,
                &part_path,
                part_info_buff.into(),
                write_quorum,
            )
            .await?;

        let ret: PartInfo = PartInfo {
            etag: Some(etag.clone()),
            part_num: part_id,
            last_mod: Some(OffsetDateTime::now_utc()),
            size: w_size,
            actual_size,
        };

        // error!("put_object_part ret {:?}", &ret);

        Ok(ret)
    }

    #[tracing::instrument(skip(self))]
    async fn list_object_parts(
        &self,
        bucket: &str,
        object: &str,
        upload_id: &str,
        part_number_marker: Option<usize>,
        mut max_parts: usize,
        opts: &ObjectOptions,
    ) -> Result<ListPartsInfo> {
        let (fi, _) = self.check_upload_id_exists(bucket, object, upload_id, false).await?;

        let upload_id_path = Self::get_upload_id_dir(bucket, object, upload_id);

        if max_parts > MAX_PARTS_COUNT {
            max_parts = MAX_PARTS_COUNT;
        }

        let part_number_marker = part_number_marker.unwrap_or_default();

        // Extract storage class from metadata, default to STANDARD if not found
        let storage_class = fi
            .metadata
            .get(AMZ_STORAGE_CLASS)
            .cloned()
            .unwrap_or_else(|| storageclass::STANDARD.to_string());

        let mut ret = ListPartsInfo {
            bucket: bucket.to_owned(),
            object: object.to_owned(),
            upload_id: upload_id.to_owned(),
            storage_class,
            max_parts,
            part_number_marker,
            user_defined: fi.metadata.clone(),
            ..Default::default()
        };

        if max_parts == 0 {
            return Ok(ret);
        }

        let online_disks = self.get_disks_internal().await;

        let read_quorum = fi.read_quorum(self.default_read_quorum());

        let part_path = format!(
            "{}{}",
            path_join_buf(&[
                &upload_id_path,
                fi.data_dir.map(|v| v.to_string()).unwrap_or_default().as_str(),
            ]),
            SLASH_SEPARATOR
        );

        let mut part_numbers = match Self::list_parts(&online_disks, &part_path, read_quorum).await {
            Ok(parts) => parts,
            Err(err) => {
                if err == DiskError::FileNotFound {
                    return Ok(ret);
                }

                return Err(to_object_err(err.into(), vec![bucket, object]));
            }
        };

        if part_numbers.is_empty() {
            return Ok(ret);
        }
        let start_op = part_numbers.iter().find(|&&v| v != 0 && v == part_number_marker);
        if part_number_marker > 0 && start_op.is_none() {
            return Ok(ret);
        }

        if let Some(start) = start_op {
            if start + 1 > part_numbers.len() {
                return Ok(ret);
            }

            part_numbers = part_numbers[start + 1..].to_vec();
        }

        let mut parts = Vec::with_capacity(part_numbers.len());

        let part_meta_paths = part_numbers
            .iter()
            .map(|v| format!("{part_path}part.{v}.meta"))
            .collect::<Vec<String>>();

        let object_parts =
            Self::read_parts(&online_disks, RUSTFS_META_MULTIPART_BUCKET, &part_meta_paths, &part_numbers, read_quorum)
                .await
                .map_err(|e| to_object_err(e.into(), vec![bucket, object, upload_id]))?;

        let mut count = max_parts;

        for (i, part) in object_parts.iter().enumerate() {
            if let Some(err) = &part.error {
                warn!("list_object_parts part error: {:?}", &err);
            }

            parts.push(PartInfo {
                etag: Some(part.etag.clone()),
                part_num: part.number,
                last_mod: part.mod_time,
                size: part.size,
                actual_size: part.actual_size,
            });

            count -= 1;
            if count == 0 {
                break;
            }
        }

        ret.parts = parts;

        if object_parts.len() > ret.parts.len() {
            ret.is_truncated = true;
            ret.next_part_number_marker = ret.parts.last().map(|v| v.part_num).unwrap_or_default();
        }

        Ok(ret)
    }

    #[tracing::instrument(skip(self))]
    async fn list_multipart_uploads(
        &self,
        bucket: &str,
        object: &str,
        key_marker: Option<String>,
        upload_id_marker: Option<String>,
        delimiter: Option<String>,
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

        let mut upload_ids: Vec<String> = Vec::new();

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
                    if err == DiskError::DiskNotFound {
                        None
                    } else if err == DiskError::FileNotFound {
                        return Ok(ListMultipartsInfo {
                            key_marker: key_marker.to_owned(),
                            max_uploads,
                            prefix: object.to_owned(),
                            delimiter: delimiter.to_owned(),
                            ..Default::default()
                        });
                    } else {
                        return Err(to_object_err(err.into(), vec![bucket, object]));
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
                    if let Ok(unix) = splits[1].parse::<i128>() {
                        OffsetDateTime::from_unix_timestamp_nanos(unix)?
                    } else {
                        now
                    }
                } else {
                    now
                }
            };

            uploads.push(MultipartInfo {
                bucket: bucket.to_owned(),
                object: object.to_owned(),
                upload_id: base64_simd::URL_SAFE_NO_PAD
                    .encode_to_string(format!("{}.{}", get_global_deployment_id().unwrap_or_default(), upload_id).as_bytes()),
                initiated: Some(start_time),
                ..Default::default()
            });

            populated_upload_ids.insert(upload_id);
        }

        uploads.sort_by(|a, b| a.initiated.cmp(&b.initiated));

        let mut upload_idx = 0;
        if let Some(upload_id_marker) = &upload_id_marker {
            while upload_idx < uploads.len() {
                if &uploads[upload_idx].upload_id != upload_id_marker {
                    upload_idx += 1;
                    continue;
                }

                if &uploads[upload_idx].upload_id == upload_id_marker {
                    upload_idx += 1;
                    break;
                }

                upload_idx += 1;
            }
        }

        let mut ret_uploads = Vec::new();
        let mut next_upload_id_marker = None;
        while upload_idx < uploads.len() {
            ret_uploads.push(uploads[upload_idx].clone());
            next_upload_id_marker = Some(uploads[upload_idx].upload_id.clone());
            upload_idx += 1;

            if ret_uploads.len() > max_uploads {
                break;
            }
        }

        let is_truncated = ret_uploads.len() < uploads.len();

        if !is_truncated {
            next_upload_id_marker = None;
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

    #[tracing::instrument(skip(self))]
    async fn new_multipart_upload(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<MultipartUploadResult> {
        if let Some(http_preconditions) = opts.http_preconditions.clone() {
            let object_lock_guard = if !opts.no_lock {
                let ns_lock = self.new_ns_lock(bucket, object).await?;
                Some(ns_lock.get_write_lock(get_lock_acquire_timeout()).await.map_err(|e| {
                    StorageError::other(format!(
                        "Failed to acquire write lock: {}",
                        self.format_lock_error_from_error(bucket, object, "write", &e)
                    ))
                })?)
            } else {
                None
            };

            if let Some(err) = self.check_write_precondition(bucket, object, opts).await {
                return Err(err);
            }
        }

        let disks = self.disks.read().await;

        let disks = disks.clone();

        let mut user_defined = opts.user_defined.clone();

        if let Some(ref etag) = opts.preserve_etag {
            user_defined.insert("etag".to_owned(), etag.clone());
        }

        if let Some(sc) = user_defined.get(AMZ_STORAGE_CLASS)
            && sc == storageclass::STANDARD
        {
            let _ = user_defined.remove(AMZ_STORAGE_CLASS);
        }

        let sc_parity_drives = {
            if let Some(sc) = GLOBAL_STORAGE_CLASS.get() {
                sc.get_parity_for_sc(user_defined.get(AMZ_STORAGE_CLASS).cloned().unwrap_or_default().as_str())
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

        fi.version_id = if let Some(vid) = &opts.version_id {
            Some(Uuid::parse_str(vid)?)
        } else {
            None
        };

        if opts.versioned && opts.version_id.is_none() {
            fi.version_id = Some(Uuid::new_v4());
        }

        fi.data_dir = Some(Uuid::new_v4());

        if let Some(cssum) = user_defined.get(RUSTFS_BUCKET_REPLICATION_SSEC_CHECKSUM)
            && !cssum.is_empty()
        {
            fi.checksum = base64_simd::STANDARD.decode_to_vec(cssum).ok().map(Bytes::from);
            user_defined.remove(RUSTFS_BUCKET_REPLICATION_SSEC_CHECKSUM);
        }

        let parts_metadata = vec![fi.clone(); disks.len()];

        if !user_defined.contains_key("content-type") {
            // TODO: get content-type
        }

        if let Some(sc) = user_defined.get(AMZ_STORAGE_CLASS)
            && sc == storageclass::STANDARD
        {
            let _ = user_defined.remove(AMZ_STORAGE_CLASS);
        }

        if let Some(checksum) = &opts.want_checksum {
            user_defined.insert(rustfs_rio::RUSTFS_MULTIPART_CHECKSUM.to_string(), checksum.checksum_type.to_string());
            user_defined.insert(
                rustfs_rio::RUSTFS_MULTIPART_CHECKSUM_TYPE.to_string(),
                checksum.checksum_type.obj_type().to_string(),
            );
        }

        let (shuffle_disks, mut parts_metadatas) = Self::shuffle_disks_and_parts_metadata(&disks, &parts_metadata, &fi);

        let mod_time = opts.mod_time.unwrap_or(OffsetDateTime::now_utc());

        for f in parts_metadatas.iter_mut() {
            f.metadata = user_defined.clone();
            f.mod_time = Some(mod_time);
            f.fresh = true;
        }

        // fi.mod_time = Some(now);

        let upload_uuid = format!("{}x{}", Uuid::new_v4(), mod_time.unix_timestamp_nanos());

        let upload_id = base64_simd::URL_SAFE_NO_PAD
            .encode_to_string(format!("{}.{}", get_global_deployment_id().unwrap_or_default(), upload_uuid).as_bytes());

        let upload_path = Self::get_upload_id_dir(bucket, object, upload_uuid.as_str());

        Self::write_unique_file_info(
            &shuffle_disks,
            bucket,
            RUSTFS_META_MULTIPART_BUCKET,
            upload_path.as_str(),
            &parts_metadatas,
            write_quorum,
        )
        .await
        .map_err(|e| to_object_err(e.into(), vec![bucket, object]))?;

        // evalDisks

        Ok(MultipartUploadResult {
            upload_id,
            checksum_algo: user_defined.get(rustfs_rio::RUSTFS_MULTIPART_CHECKSUM).cloned(),
            checksum_type: user_defined.get(rustfs_rio::RUSTFS_MULTIPART_CHECKSUM_TYPE).cloned(),
        })
    }

    #[tracing::instrument(skip(self))]
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
            user_defined: fi.metadata.clone(),
            ..Default::default()
        })
    }

    #[tracing::instrument(skip(self))]
    async fn abort_multipart_upload(&self, bucket: &str, object: &str, upload_id: &str, _opts: &ObjectOptions) -> Result<()> {
        self.check_upload_id_exists(bucket, object, upload_id, false).await?;
        let upload_id_path = Self::get_upload_id_dir(bucket, object, upload_id);

        self.delete_all(RUSTFS_META_MULTIPART_BUCKET, &upload_id_path).await
    }
    // complete_multipart_upload finished
    #[tracing::instrument(skip(self))]
    async fn complete_multipart_upload(
        self: Arc<Self>,
        bucket: &str,
        object: &str,
        upload_id: &str,
        uploaded_parts: Vec<CompletePart>,
        opts: &ObjectOptions,
    ) -> Result<ObjectInfo> {
        let mut object_lock_guard = None;

        // Acquire per-object exclusive lock via RAII guard. It auto-releases asynchronously on drop.
        if let Some(http_preconditions) = opts.http_preconditions.clone() {
            if !opts.no_lock {
                let ns_lock = self.new_ns_lock(bucket, object).await?;
                object_lock_guard = Some(ns_lock.get_write_lock(get_lock_acquire_timeout()).await.map_err(|e| {
                    StorageError::other(format!(
                        "Failed to acquire write lock: {}",
                        self.format_lock_error_from_error(bucket, object, "write", &e)
                    ))
                })?);
            }

            if let Some(err) = self.check_write_precondition(bucket, object, opts).await {
                return Err(err);
            }
        }

        let (mut fi, files_metas) = self.check_upload_id_exists(bucket, object, upload_id, true).await?;
        let upload_id_path = Self::get_upload_id_dir(bucket, object, upload_id);

        let write_quorum = fi.write_quorum(self.default_write_quorum());

        let disks = self.disks.read().await;

        let disks = disks.clone();
        // let disks = Self::shuffle_disks(&disks, &fi.erasure.distribution);

        let part_path = format!("{}/{}/", upload_id_path, fi.data_dir.unwrap_or(Uuid::nil()));

        let part_meta_paths = uploaded_parts
            .iter()
            .map(|v| format!("{part_path}part.{0}.meta", v.part_num))
            .collect::<Vec<String>>();

        let part_numbers = uploaded_parts.iter().map(|v| v.part_num).collect::<Vec<usize>>();

        let object_parts =
            Self::read_parts(&disks, RUSTFS_META_MULTIPART_BUCKET, &part_meta_paths, &part_numbers, write_quorum).await?;

        if object_parts.len() != uploaded_parts.len() {
            return Err(Error::other("part result number err"));
        }

        let mut checksum_type = rustfs_rio::ChecksumType::NONE;

        if let Some(cs) = fi.metadata.get(rustfs_rio::RUSTFS_MULTIPART_CHECKSUM) {
            let Some(ct) = fi.metadata.get(rustfs_rio::RUSTFS_MULTIPART_CHECKSUM_TYPE) else {
                return Err(Error::other("checksum type not found"));
            };

            checksum_type = rustfs_rio::ChecksumType::from_string_with_obj_type(cs, ct);
            if let Some(want) = opts.want_checksum.as_ref()
                && !want.checksum_type.is(checksum_type)
            {
                return Err(Error::other(format!("checksum type mismatch, got {:?}, want {:?}", want, checksum_type)));
            }
        }

        for (i, part) in object_parts.iter().enumerate() {
            if let Some(err) = &part.error {
                error!("complete_multipart_upload part error: {:?}", &err);
            }

            if uploaded_parts[i].part_num != part.number {
                error!(
                    "complete_multipart_upload part_id err part_id != part_num {} != {}",
                    uploaded_parts[i].part_num, part.number
                );
                return Err(Error::InvalidPart(uploaded_parts[i].part_num, bucket.to_owned(), object.to_owned()));
            }

            fi.add_object_part(
                part.number,
                part.etag.clone(),
                part.size,
                part.mod_time,
                part.actual_size,
                part.index.clone(),
                part.checksums.clone(),
            );
        }

        let (shuffle_disks, mut parts_metadatas) = Self::shuffle_disks_and_parts_metadata_by_index(&disks, &files_metas, &fi);

        let curr_fi = fi.clone();

        fi.parts = Vec::with_capacity(uploaded_parts.len());

        let mut object_size: usize = 0;
        let mut object_actual_size: i64 = 0;

        let mut checksum_combined = bytes::BytesMut::new();
        let mut checksum = rustfs_rio::Checksum {
            checksum_type,
            ..Default::default()
        };

        // Build a lookup map for O(1) part resolution instead of O(n) find() in the loop
        // This optimizes from O(n^2) to O(n) when processing many parts
        use std::collections::HashMap;
        let part_lookup: HashMap<usize, &rustfs_filemeta::ObjectPartInfo> =
            curr_fi.parts.iter().map(|part| (part.number, part)).collect();

        for (i, p) in uploaded_parts.iter().enumerate() {
            let Some(ext_part) = part_lookup.get(&p.part_num) else {
                error!(
                    "complete_multipart_upload part not found: part_id={}, bucket={}, object={}",
                    p.part_num, bucket, object
                );
                return Err(Error::InvalidPart(p.part_num, "".to_owned(), p.etag.clone().unwrap_or_default()));
            };
            info!(target:"rustfs_ecstore::set_disk", part_number = p.part_num, part_size = ext_part.size, part_actual_size = ext_part.actual_size, "Completing multipart part");

            // Normalize ETags by removing quotes before comparison (PR #592 compatibility)
            let client_etag = p.etag.as_ref().map(|e| rustfs_utils::path::trim_etag(e));
            let stored_etag = Some(rustfs_utils::path::trim_etag(&ext_part.etag));
            if client_etag != stored_etag {
                error!(
                    "complete_multipart_upload etag err client={:?}, stored={:?}, part_id={}, bucket={}, object={}",
                    p.etag, ext_part.etag, p.part_num, bucket, object
                );
                return Err(Error::InvalidPart(p.part_num, ext_part.etag.clone(), p.etag.clone().unwrap_or_default()));
            }

            // TODO: crypto

            if (i < uploaded_parts.len() - 1) && !is_min_allowed_part_size(ext_part.actual_size) {
                error!(
                    "complete_multipart_upload part size too small: part {} size {} is less than minimum {}",
                    p.part_num,
                    ext_part.actual_size,
                    GLOBAL_MIN_PART_SIZE.as_u64()
                );
                return Err(Error::EntityTooSmall(
                    p.part_num,
                    ext_part.actual_size,
                    GLOBAL_MIN_PART_SIZE.as_u64() as i64,
                ));
            }

            if checksum_type.is_set() {
                let Some(crc) = ext_part
                    .checksums
                    .as_ref()
                    .and_then(|f| f.get(checksum_type.to_string().as_str()))
                    .cloned()
                else {
                    error!(
                        "complete_multipart_upload fi.checksum not found type={checksum_type}, part_id={}, bucket={}, object={}",
                        p.part_num, bucket, object
                    );
                    return Err(Error::InvalidPart(p.part_num, ext_part.etag.clone(), p.etag.clone().unwrap_or_default()));
                };

                let part_crc = match checksum_type {
                    rustfs_rio::ChecksumType::SHA256 => p.checksum_sha256.clone(),
                    rustfs_rio::ChecksumType::SHA1 => p.checksum_sha1.clone(),
                    rustfs_rio::ChecksumType::CRC32 => p.checksum_crc32.clone(),
                    rustfs_rio::ChecksumType::CRC32C => p.checksum_crc32c.clone(),
                    rustfs_rio::ChecksumType::CRC64_NVME => p.checksum_crc64nvme.clone(),
                    _ => {
                        error!(
                            "complete_multipart_upload checksum type={checksum_type}, part_id={}, bucket={}, object={}",
                            p.part_num, bucket, object
                        );
                        return Err(Error::InvalidPart(p.part_num, ext_part.etag.clone(), p.etag.clone().unwrap_or_default()));
                    }
                };

                if part_crc.clone().unwrap_or_default() != crc {
                    error!("complete_multipart_upload checksum_type={checksum_type:?}, part_crc={part_crc:?}, crc={crc:?}");
                    error!(
                        "complete_multipart_upload checksum mismatch part_id={}, bucket={}, object={}",
                        p.part_num, bucket, object
                    );
                    return Err(Error::InvalidPart(p.part_num, ext_part.etag.clone(), p.etag.clone().unwrap_or_default()));
                }

                let Some(cs) = rustfs_rio::Checksum::new_with_type(checksum_type, &crc) else {
                    error!(
                        "complete_multipart_upload checksum new_with_type failed part_id={}, bucket={}, object={}",
                        p.part_num, bucket, object
                    );
                    return Err(Error::InvalidPart(p.part_num, ext_part.etag.clone(), p.etag.clone().unwrap_or_default()));
                };

                if !cs.valid() {
                    error!(
                        "complete_multipart_upload checksum valid failed part_id={}, bucket={}, object={}",
                        p.part_num, bucket, object
                    );
                    return Err(Error::InvalidPart(p.part_num, ext_part.etag.clone(), p.etag.clone().unwrap_or_default()));
                }

                if checksum_type.full_object_requested()
                    && let Err(err) = checksum.add_part(&cs, ext_part.actual_size)
                {
                    error!(
                        "complete_multipart_upload checksum add_part failed part_id={}, bucket={}, object={}",
                        p.part_num, bucket, object
                    );
                    return Err(Error::InvalidPart(p.part_num, ext_part.etag.clone(), p.etag.clone().unwrap_or_default()));
                }

                checksum_combined.extend_from_slice(cs.raw.as_slice());
            }

            // TODO: check min part size

            object_size += ext_part.size;
            object_actual_size += ext_part.actual_size;

            fi.parts.push(ObjectPartInfo {
                etag: ext_part.etag.clone(),
                number: p.part_num,
                size: ext_part.size,
                mod_time: ext_part.mod_time,
                actual_size: ext_part.actual_size,
                index: ext_part.index.clone(),
                ..Default::default()
            });
        }

        if let Some(wtcs) = opts.want_checksum.as_ref() {
            if checksum_type.full_object_requested() {
                if wtcs.encoded != checksum.encoded {
                    error!(
                        "complete_multipart_upload checksum mismatch want={}, got={}",
                        wtcs.encoded, checksum.encoded
                    );
                    return Err(Error::other(format!(
                        "complete_multipart_upload checksum mismatch want={}, got={}",
                        wtcs.encoded, checksum.encoded
                    )));
                }
            } else if let Err(err) = wtcs.matches(&checksum_combined, uploaded_parts.len() as i32) {
                error!(
                    "complete_multipart_upload checksum matches failed want={}, got={}",
                    wtcs.encoded, checksum.encoded
                );
                return Err(Error::other(format!(
                    "complete_multipart_upload checksum matches failed want={}, got={}",
                    wtcs.encoded, checksum.encoded
                )));
            }
        }

        if let Some(rc_crc) = opts.user_defined.get(RUSTFS_BUCKET_REPLICATION_SSEC_CHECKSUM) {
            if let Ok(rc_crc_bytes) = base64_simd::STANDARD.decode_to_vec(rc_crc) {
                fi.checksum = Some(Bytes::from(rc_crc_bytes));
            } else {
                error!("complete_multipart_upload decode rc_crc failed rc_crc={}", rc_crc);
            }
        }

        if checksum_type.is_set() {
            checksum_type
                .merge(rustfs_rio::ChecksumType::MULTIPART)
                .merge(rustfs_rio::ChecksumType::INCLUDES_MULTIPART);
            if !checksum_type.full_object_requested() {
                checksum = rustfs_rio::Checksum::new_from_data(checksum_type, &checksum_combined)
                    .ok_or_else(|| Error::other("checksum new_from_data failed"))?;
            }
            fi.checksum = Some(checksum.to_bytes(&checksum_combined));
        }

        fi.metadata.remove(rustfs_rio::RUSTFS_MULTIPART_CHECKSUM);
        fi.metadata.remove(rustfs_rio::RUSTFS_MULTIPART_CHECKSUM_TYPE);

        fi.size = object_size as i64;
        fi.mod_time = opts.mod_time;
        if fi.mod_time.is_none() {
            fi.mod_time = Some(OffsetDateTime::now_utc());
        }

        // etag
        let etag = {
            if let Some(etag) = opts.user_defined.get("etag") {
                etag.clone()
            } else {
                get_complete_multipart_md5(&uploaded_parts)
            }
        };

        fi.metadata.insert("etag".to_owned(), etag);

        if opts.replication_request {
            if let Some(actual_size) = opts
                .user_defined
                .get(format!("{RESERVED_METADATA_PREFIX_LOWER}Actual-Object-Size").as_str())
            {
                fi.metadata
                    .insert(format!("{RESERVED_METADATA_PREFIX}actual-size"), actual_size.clone());
                fi.metadata
                    .insert("x-rustfs-encryption-original-size".to_string(), actual_size.to_string());
            }
        } else {
            fi.metadata
                .insert(format!("{RESERVED_METADATA_PREFIX}actual-size"), object_actual_size.to_string());
            fi.metadata
                .insert("x-rustfs-encryption-original-size".to_string(), object_actual_size.to_string());
        }

        if fi.is_compressed() {
            fi.metadata
                .insert(format!("{RESERVED_METADATA_PREFIX_LOWER}compression-size"), object_size.to_string());
        }

        if opts.data_movement {
            fi.set_data_moved();
        }

        for meta in parts_metadatas.iter_mut() {
            if meta.is_valid() {
                meta.size = fi.size;
                meta.mod_time = fi.mod_time;
                meta.parts.clone_from(&fi.parts);
                meta.metadata = fi.metadata.clone();
                meta.versioned = opts.versioned || opts.version_suspended;
                meta.checksum = fi.checksum.clone();
            }
        }

        let mut parts = Vec::with_capacity(curr_fi.parts.len());

        for p in curr_fi.parts.iter() {
            parts.push(path_join_buf(&[
                &upload_id_path,
                curr_fi.data_dir.unwrap_or(Uuid::nil()).to_string().as_str(),
                format!("part.{}.meta", p.number).as_str(),
            ]));

            if !fi.parts.iter().any(|v| v.number == p.number) {
                parts.push(path_join_buf(&[
                    &upload_id_path,
                    curr_fi.data_dir.unwrap_or(Uuid::nil()).to_string().as_str(),
                    format!("part.{}", p.number).as_str(),
                ]));
            }
        }

        if !opts.no_lock && object_lock_guard.is_none() {
            let ns_lock = self.new_ns_lock(bucket, object).await?;
            object_lock_guard = Some(ns_lock.get_write_lock(get_lock_acquire_timeout()).await.map_err(|e| {
                StorageError::other(format!(
                    "Failed to acquire write lock: {}",
                    self.format_lock_error_from_error(bucket, object, "write", &e)
                ))
            })?);
        }

        self.cleanup_multipart_path(&parts).await;

        let (online_disks, versions, op_old_dir) = Self::rename_data(
            &shuffle_disks,
            RUSTFS_META_MULTIPART_BUCKET,
            &upload_id_path,
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

        drop(object_lock_guard); // drop object lock guard to release the lock

        if let Some(versions) = versions {
            let _ =
                rustfs_common::heal_channel::send_heal_request(rustfs_common::heal_channel::create_heal_request_with_options(
                    bucket.to_string(),
                    Some(object.to_string()),
                    false,
                    Some(HealChannelPriority::Normal),
                    Some(self.pool_index),
                    Some(self.set_index),
                ))
                .await;
        }

        let upload_id_path = upload_id_path.clone();
        let store = self.clone();
        let _cleanup_handle = tokio::spawn(async move {
            let _ = store.delete_all(RUSTFS_META_MULTIPART_BUCKET, &upload_id_path).await;
        });

        for (i, op_disk) in online_disks.iter().enumerate() {
            if let Some(disk) = op_disk
                && disk.is_online().await
            {
                fi = parts_metadatas[i].clone();
                break;
            }
        }

        fi.is_latest = true;

        Ok(ObjectInfo::from_file_info(&fi, bucket, object, opts.versioned || opts.version_suspended))
    }

    #[tracing::instrument(skip(self))]
    async fn get_disks(&self, _pool_idx: usize, _set_idx: usize) -> Result<Vec<Option<DiskStore>>> {
        Ok(self.get_disks_internal().await)
    }

    #[tracing::instrument(skip(self))]
    fn set_drive_counts(&self) -> Vec<usize> {
        unimplemented!()
    }

    #[tracing::instrument(skip(self))]
    async fn delete_bucket(&self, _bucket: &str, _opts: &DeleteBucketOptions) -> Result<()> {
        unimplemented!()
    }

    #[tracing::instrument(skip(self))]
    async fn heal_format(&self, _dry_run: bool) -> Result<(HealResultItem, Option<Error>)> {
        unimplemented!()
    }

    #[tracing::instrument(skip(self))]
    async fn heal_bucket(&self, _bucket: &str, _opts: &HealOpts) -> Result<HealResultItem> {
        unimplemented!()
    }

    #[tracing::instrument(skip(self))]
    async fn heal_object(
        &self,
        bucket: &str,
        object: &str,
        version_id: &str,
        opts: &HealOpts,
    ) -> Result<(HealResultItem, Option<Error>)> {
        let _write_lock_guard = if !opts.no_lock {
            let ns_lock = self.new_ns_lock(bucket, object).await?;
            Some(ns_lock.get_write_lock(get_lock_acquire_timeout()).await.map_err(|e| {
                StorageError::other(format!(
                    "Failed to acquire write lock: {}",
                    self.format_lock_error_from_error(bucket, object, "write", &e)
                ))
            })?)
        } else {
            None
        };

        if has_suffix(object, SLASH_SEPARATOR) {
            let (result, err) = self.heal_object_dir_locked(bucket, object, opts.dry_run, opts.remove).await?;
            return Ok((result, err.map(|e| e.into())));
        }

        let disks = self.disks.read().await;

        let disks = disks.clone();
        let (_, errs) = Self::read_all_fileinfo(&disks, "", bucket, object, version_id, false, false).await?;
        if DiskError::is_all_not_found(&errs) {
            warn!(
                "heal_object failed, all obj part not found, bucket: {}, obj: {}, version_id: {}",
                bucket, object, version_id
            );
            let err = if !version_id.is_empty() {
                Error::FileVersionNotFound
            } else {
                Error::FileNotFound
            };
            return Ok((
                self.default_heal_result(FileInfo::default(), &errs, bucket, object, version_id)
                    .await,
                Some(err),
            ));
        }

        // Heal the object.
        // Pass no_lock=true since we already obtained write lock (or are already called with no_lock=true)
        let mut inner_opts = *opts;
        inner_opts.no_lock = true;
        let (result, err) = self.heal_object(bucket, object, version_id, &inner_opts).await?;
        if let Some(err) = err.as_ref() {
            match err {
                &DiskError::FileCorrupt if opts.scan_mode != HealScanMode::Deep => {
                    // Instead of returning an error when a bitrot error is detected
                    // during a normal heal scan, heal again with bitrot flag enabled.
                    let mut opts = *opts;
                    opts.scan_mode = HealScanMode::Deep;
                    inner_opts.scan_mode = HealScanMode::Deep;
                    let (result, err) = self.heal_object(bucket, object, version_id, &inner_opts).await?;
                    return Ok((result, err.map(|e| e.into())));
                }
                _ => {}
            }
        }
        Ok((result, err.map(|e| e.into())))
    }

    #[tracing::instrument(skip(self))]
    async fn get_pool_and_set(&self, _id: &str) -> Result<(Option<usize>, Option<usize>, Option<usize>)> {
        unimplemented!()
    }

    #[tracing::instrument(skip(self))]
    async fn check_abandoned_parts(&self, _bucket: &str, _object: &str, _opts: &HealOpts) -> Result<()> {
        unimplemented!()
    }

    #[tracing::instrument(skip(self))]
    async fn verify_object_integrity(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<()> {
        let get_object_reader = <Self as ObjectIO>::get_object_reader(self, bucket, object, None, HeaderMap::new(), opts).await?;
        // Stream to sink to avoid loading entire object into memory during verification
        let mut reader = get_object_reader.stream;
        tokio::io::copy(&mut reader, &mut tokio::io::sink()).await?;
        Ok(())
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

fn is_object_dangling(
    meta_arr: &[FileInfo],
    errs: &[Option<DiskError>],
    data_errs_by_part: &HashMap<usize, Vec<usize>>,
) -> (FileInfo, bool) {
    let (not_found_meta_errs, non_actionable_meta_errs) = dangling_meta_errs_count(errs);

    let (mut not_found_parts_errs, mut non_actionable_parts_errs) = (0, 0);

    data_errs_by_part.iter().for_each(|(_, v)| {
        let (nf, na) = dangling_part_errs_count(v);
        if nf > not_found_parts_errs {
            (not_found_parts_errs, non_actionable_parts_errs) = (nf, na);
        }
    });

    let mut valid_meta = FileInfo::default();

    for fi in meta_arr.iter() {
        if fi.is_valid() {
            valid_meta = fi.clone();
            break;
        }
    }

    if !valid_meta.is_valid() {
        let data_blocks = meta_arr.len().div_ceil(2);
        if not_found_parts_errs > data_blocks {
            return (valid_meta, true);
        }

        return (valid_meta, false);
    }

    if non_actionable_meta_errs > 0 || non_actionable_parts_errs > 0 {
        return (valid_meta, false);
    }

    if valid_meta.deleted {
        let data_blocks = errs.len().div_ceil(2);
        return (valid_meta, not_found_meta_errs > data_blocks);
    }

    if not_found_meta_errs > 0 && not_found_meta_errs > valid_meta.erasure.parity_blocks {
        return (valid_meta, true);
    }

    if !valid_meta.is_remote() && not_found_parts_errs > 0 && not_found_parts_errs > valid_meta.erasure.parity_blocks {
        return (valid_meta, true);
    }

    (valid_meta, false)
}

fn dangling_meta_errs_count(cerrs: &[Option<DiskError>]) -> (usize, usize) {
    let (mut not_found_count, mut non_actionable_count) = (0, 0);
    cerrs.iter().for_each(|err| {
        if let Some(err) = err {
            if err == &DiskError::FileNotFound || err == &DiskError::FileVersionNotFound {
                not_found_count += 1;
            } else {
                non_actionable_count += 1;
            }
        }
    });

    (not_found_count, non_actionable_count)
}

fn dangling_part_errs_count(results: &[usize]) -> (usize, usize) {
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

fn is_object_dir_dangling(errs: &[Option<DiskError>]) -> bool {
    let mut found = 0;
    let mut not_found = 0;
    let mut found_not_empty = 0;
    let mut other_found = 0;
    errs.iter().for_each(|err| {
        if err.is_none() {
            found += 1;
        } else if let Some(err) = err {
            if err == &DiskError::FileNotFound || err == &DiskError::VolumeNotFound {
                not_found += 1;
            } else if err == &DiskError::VolumeNotEmpty {
                found_not_empty += 1;
            } else {
                other_found += 1;
            }
        }
    });

    found = found + found_not_empty + other_found;
    found < not_found && found > 0
}

fn join_errs(errs: &[Option<DiskError>]) -> String {
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

/// disks_with_all_partsv2 is a corrected version based on Go implementation.
/// It sets partsMetadata and onlineDisks when xl.meta is inexistant/corrupted or outdated.
/// It also checks if the status of each part (corrupted, missing, ok) in each drive.
/// Returns (availableDisks, dataErrsByDisk, dataErrsByPart).
#[allow(clippy::too_many_arguments)]
async fn disks_with_all_parts(
    online_disks: &mut [Option<DiskStore>],
    parts_metadata: &mut [FileInfo],
    errs: &[Option<DiskError>],
    latest_meta: &FileInfo,
    filter_by_etag: bool,
    bucket: &str,
    object: &str,
    scan_mode: HealScanMode,
) -> disk::error::Result<(HashMap<usize, Vec<usize>>, HashMap<usize, Vec<usize>>)> {
    let object_name = latest_meta.name.clone();

    // Initialize dataErrsByDisk and dataErrsByPart with 0 (CHECK_PART_UNKNOWN) to match Go
    let mut data_errs_by_disk: HashMap<usize, Vec<usize>> = HashMap::new();
    for i in 0..online_disks.len() {
        data_errs_by_disk.insert(i, vec![CHECK_PART_UNKNOWN; latest_meta.parts.len()]);
    }
    let mut data_errs_by_part: HashMap<usize, Vec<usize>> = HashMap::new();
    for i in 0..latest_meta.parts.len() {
        data_errs_by_part.insert(i, vec![CHECK_PART_UNKNOWN; online_disks.len()]);
    }

    // Check for inconsistent erasure distribution
    let mut inconsistent = 0;
    for (index, meta) in parts_metadata.iter().enumerate() {
        if !meta.is_valid() {
            // Since for majority of the cases erasure.Index matches with erasure.Distribution we can
            // consider the offline disks as consistent.
            continue;
        }
        if !meta.deleted {
            if meta.erasure.distribution.len() != online_disks.len() {
                // Erasure distribution seems to have lesser
                // number of items than number of online disks.
                inconsistent += 1;
                continue;
            }
            if !meta.erasure.distribution.is_empty()
                && index < meta.erasure.distribution.len()
                && meta.erasure.distribution[index] != meta.erasure.index
            {
                // Mismatch indexes with distribution order
                inconsistent += 1;
            }
        }
    }

    let erasure_distribution_reliable = inconsistent <= parts_metadata.len() / 2;

    // Initialize metaErrs
    let mut meta_errs = Vec::with_capacity(errs.len());
    for _ in 0..errs.len() {
        meta_errs.push(None);
    }

    let online_disks_len = online_disks.len();

    // Process meta errors
    for (index, disk_op) in online_disks.iter_mut().enumerate() {
        if let Some(err) = &errs[index] {
            meta_errs[index] = Some(err.clone());
            continue;
        }

        if disk_op.is_none() {
            meta_errs[index] = Some(DiskError::DiskNotFound);
            continue;
        }

        let meta = &parts_metadata[index];

        let corrupted = if filter_by_etag {
            latest_meta.get_etag() != meta.get_etag()
        } else {
            !meta.mod_time.eq(&latest_meta.mod_time) || !meta.data_dir.eq(&latest_meta.data_dir)
        };

        if corrupted {
            info!(
                "disks_with_all_partsv2: metadata is corrupted, object_name={}, index: {index}",
                object_name
            );
            meta_errs[index] = Some(DiskError::FileCorrupt);
            parts_metadata[index] = FileInfo::default();
            *disk_op = None;

            continue;
        }

        if erasure_distribution_reliable {
            if !meta.is_valid() {
                info!(
                    "disks_with_all_partsv2: metadata is not valid, object_name={}, index: {index}",
                    object_name
                );
                parts_metadata[index] = FileInfo::default();
                meta_errs[index] = Some(DiskError::FileCorrupt);
                *disk_op = None;
                continue;
            }

            if !meta.deleted && meta.erasure.distribution.len() != online_disks_len {
                // Erasure distribution is not the same as onlineDisks
                // attempt a fix if possible, assuming other entries
                // might have the right erasure distribution.
                info!(
                    "disks_with_all_partsv2: erasure distribution is not the same as onlineDisks, object_name={}, index: {index}",
                    object_name
                );
                parts_metadata[index] = FileInfo::default();
                meta_errs[index] = Some(DiskError::FileCorrupt);
                *disk_op = None;
                continue;
            }
        }
    }

    // Copy meta errors to part errors
    for (index, err) in meta_errs.iter().enumerate() {
        if err.is_some() {
            let part_err = conv_part_err_to_int(err);
            for p in 0..latest_meta.parts.len() {
                if let Some(vec) = data_errs_by_part.get_mut(&p)
                    && index < vec.len()
                {
                    vec[index] = part_err;
                }
            }
        }
    }

    // Check data for each disk
    for (index, disk) in online_disks.iter().enumerate() {
        if meta_errs[index].is_some() {
            continue;
        }

        let disk = if let Some(disk) = disk {
            disk
        } else {
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
                let verify_err = bitrot_verify(
                    Box::new(Cursor::new(data.clone())),
                    data_len,
                    meta.erasure.shard_file_size(meta.size) as usize,
                    checksum_info.algorithm,
                    checksum_info.hash,
                    meta.erasure.shard_size(),
                )
                .await
                .err();

                if let Some(vec) = data_errs_by_part.get_mut(&0)
                    && index < vec.len()
                {
                    vec[index] = conv_part_err_to_int(&verify_err.map(|e| e.into()));
                }
            }
            continue;
        }

        // Verify file or check parts
        let mut verify_resp = CheckPartsResp::default();
        let mut verify_err = None;
        meta.data_dir = latest_meta.data_dir;

        if scan_mode == HealScanMode::Deep {
            // disk has a valid xl.meta but may not have all the
            // parts. This is considered an outdated disk, since
            // it needs healing too.
            match disk.verify_file(bucket, object, meta).await {
                Ok(v) => {
                    verify_resp = v;
                }
                Err(err) => {
                    info!("verify_file failed: {err:?}, object_name={}, index: {index}", object_name);
                    verify_err = Some(err);
                }
            }
        } else {
            match disk.check_parts(bucket, object, meta).await {
                Ok(v) => {
                    verify_resp = v;
                }
                Err(err) => {
                    info!("check_parts failed: {err:?}, object_name={}, index: {index}", object_name);
                    verify_err = Some(err);
                }
            }
        }

        // Update dataErrsByPart for all parts
        for p in 0..latest_meta.parts.len() {
            if let Some(vec) = data_errs_by_part.get_mut(&p)
                && index < vec.len()
            {
                if verify_err.is_some() {
                    vec[index] = conv_part_err_to_int(&verify_err.clone());
                } else {
                    // Fix: verify_resp.results length is based on meta.parts, not latest_meta.parts
                    // We need to check bounds to avoid panic
                    if p < verify_resp.results.len() {
                        vec[index] = verify_resp.results[p];
                    } else {
                        vec[index] = CHECK_PART_SUCCESS;
                    }
                }
            }
        }
    }

    // Build dataErrsByDisk from dataErrsByPart
    for (part, disks) in data_errs_by_part.iter() {
        for disk_idx in disks.iter() {
            if let Some(parts) = data_errs_by_disk.get_mut(disk_idx)
                && *part < parts.len()
            {
                parts[*part] = disks[*disk_idx];
            }
        }
    }

    Ok((data_errs_by_disk, data_errs_by_part))
}

pub fn should_heal_object_on_disk(
    err: &Option<DiskError>,
    parts_errs: &[usize],
    meta: &FileInfo,
    latest_meta: &FileInfo,
) -> (bool, bool, Option<DiskError>) {
    if let Some(err) = err
        && (err == &DiskError::FileNotFound || err == &DiskError::FileVersionNotFound || err == &DiskError::FileCorrupt)
    {
        return (true, true, Some(err.clone()));
    }

    if err.is_some() {
        return (false, false, err.clone());
    }

    if !meta.equals(latest_meta) {
        warn!(
            "should_heal_object_on_disk: metadata is outdated, object_name={}, meta: {:?}, latest_meta: {:?}",
            meta.name, meta, latest_meta
        );
        return (true, true, Some(DiskError::OutdatedXLMeta));
    }

    if !meta.deleted && !meta.is_remote() {
        let err_vec = [CHECK_PART_FILE_NOT_FOUND, CHECK_PART_FILE_CORRUPT];
        for part_err in parts_errs.iter() {
            if err_vec.contains(part_err) {
                return (true, false, Some(DiskError::PartMissingOrCorrupt));
            }
        }
    }
    (false, false, None)
}

async fn get_disks_info(disks: &[Option<DiskStore>], eps: &[Endpoint]) -> Vec<rustfs_madmin::Disk> {
    let mut ret = Vec::new();

    for (i, pool) in disks.iter().enumerate() {
        if let Some(disk) = pool {
            match disk.disk_info(&DiskInfoOptions::default()).await {
                Ok(res) => ret.push(rustfs_madmin::Disk {
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

                    uuid: res.id.map_or("".to_string(), |id| id.to_string()),
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
                Err(err) => ret.push(rustfs_madmin::Disk {
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
            ret.push(rustfs_madmin::Disk {
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
async fn get_storage_info(disks: &[Option<DiskStore>], eps: &[Endpoint]) -> rustfs_madmin::StorageInfo {
    // let mut disks = get_disks_info(disks, eps).await;
    // disks.sort_by(|a, b| a.total_space.cmp(&b.total_space));
    //
    // rustfs_madmin::StorageInfo {
    //     disks,
    //     backend: rustfs_madmin::BackendInfo {
    //         backend_type: rustfs_madmin::BackendByte::Erasure,
    //         ..Default::default()
    //     },
    // }
    let mut disks = get_disks_info(disks, eps).await;
    disks.sort_by(|a, b| a.total_space.cmp(&b.total_space));

    // Provide minimal backend shape for callers. Do NOT guess parity here since it belongs to higher-level config.
    // Missing/empty standard_sc_data will be handled by capacity fallback logic.
    let drives_per_set = vec![eps.len()];
    let total_sets = vec![1];

    rustfs_madmin::StorageInfo {
        disks,
        backend: rustfs_madmin::BackendInfo {
            backend_type: rustfs_madmin::BackendByte::Erasure,
            drives_per_set,
            total_sets,
            ..Default::default()
        },
    }
}
pub async fn stat_all_dirs(disks: &[Option<DiskStore>], bucket: &str, prefix: &str) -> Vec<Option<DiskError>> {
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
                        return Some(DiskError::VolumeNotEmpty);
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

const GLOBAL_MIN_PART_SIZE: ByteSize = ByteSize::mib(5);
fn is_min_allowed_part_size(size: i64) -> bool {
    size >= GLOBAL_MIN_PART_SIZE.as_u64() as i64
}

fn get_complete_multipart_md5(parts: &[CompletePart]) -> String {
    let mut buf = Vec::new();

    for part in parts.iter() {
        if let Some(etag) = &part.etag {
            if let Ok(etag_bytes) = hex_simd::decode_to_vec(etag.as_bytes()) {
                buf.extend(etag_bytes);
            } else {
                buf.extend(etag.bytes());
            }
        }
    }

    let mut hasher = Md5::new();
    hasher.update(&buf);

    let digest = hasher.finalize();
    let etag_hex = faster_hex::hex_string(digest.as_slice());
    format!("{}-{}", etag_hex, parts.len())
}

pub fn canonicalize_etag(etag: &str) -> String {
    let re = Regex::new("\"*?([^\"]*?)\"*?$").unwrap();
    re.replace_all(etag, "$1").to_string()
}

pub fn e_tag_matches(etag: &str, condition: &str) -> bool {
    if condition.trim() == "*" {
        return true;
    }
    canonicalize_etag(etag) == canonicalize_etag(condition)
}

pub fn should_prevent_write(oi: &ObjectInfo, if_none_match: Option<String>, if_match: Option<String>) -> bool {
    match &oi.etag {
        Some(etag) => {
            if let Some(if_none_match) = if_none_match
                && e_tag_matches(etag, &if_none_match)
            {
                return true;
            }
            if let Some(if_match) = if_match
                && !e_tag_matches(etag, &if_match)
            {
                return true;
            }
            false
        }
        // If we can't obtain the etag of the object, perevent the write only when we have at least one condition
        None => if_none_match.is_some() || if_match.is_some(),
    }
}

/// Validates if the given storage class is supported
pub fn is_valid_storage_class(storage_class: &str) -> bool {
    matches!(
        storage_class,
        storageclass::STANDARD
            | storageclass::RRS
            | storageclass::DEEP_ARCHIVE
            | storageclass::EXPRESS_ONEZONE
            | storageclass::GLACIER
            | storageclass::GLACIER_IR
            | storageclass::INTELLIGENT_TIERING
            | storageclass::ONEZONE_IA
            | storageclass::OUTPOSTS
            | storageclass::SNOW
            | storageclass::STANDARD_IA
    )
}

/// Returns true if the storage class is a cold storage tier that requires special handling
pub fn is_cold_storage_class(storage_class: &str) -> bool {
    matches!(
        storage_class,
        storageclass::DEEP_ARCHIVE | storageclass::GLACIER | storageclass::GLACIER_IR
    )
}

/// Returns true if the storage class is an infrequent access tier
pub fn is_infrequent_access_class(storage_class: &str) -> bool {
    matches!(
        storage_class,
        storageclass::ONEZONE_IA | storageclass::STANDARD_IA | storageclass::INTELLIGENT_TIERING
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::disk::CHECK_PART_UNKNOWN;
    use crate::disk::CHECK_PART_VOLUME_NOT_FOUND;
    use crate::disk::error::DiskError;
    use crate::store_api::{CompletePart, ObjectInfo};
    use rustfs_filemeta::ErasureInfo;
    use std::collections::HashMap;
    use time::OffsetDateTime;

    #[test]
    fn disk_health_entry_returns_cached_value_within_ttl() {
        let entry = DiskHealthEntry {
            last_check: Instant::now(),
            online: true,
        };

        assert_eq!(entry.cached_value(), Some(true));
    }

    #[test]
    fn disk_health_entry_expires_after_ttl() {
        let entry = DiskHealthEntry {
            last_check: Instant::now() - (DISK_HEALTH_CACHE_TTL + Duration::from_millis(100)),
            online: true,
        };

        assert!(entry.cached_value().is_none());
    }

    #[test]
    fn test_check_part_constants() {
        // Test that all CHECK_PART constants have expected values
        assert_eq!(CHECK_PART_UNKNOWN, 0);
        assert_eq!(CHECK_PART_SUCCESS, 1);
        assert_eq!(CHECK_PART_FILE_NOT_FOUND, 4); // The actual value is 4, not 2
        assert_eq!(CHECK_PART_VOLUME_NOT_FOUND, 3);
        assert_eq!(CHECK_PART_FILE_CORRUPT, 5);
    }

    #[test]
    fn test_is_min_allowed_part_size() {
        // Test minimum part size validation
        assert!(!is_min_allowed_part_size(0));
        assert!(!is_min_allowed_part_size(1024)); // 1KB - too small
        assert!(!is_min_allowed_part_size(1024 * 1024)); // 1MB - too small
        assert!(is_min_allowed_part_size(5 * 1024 * 1024)); // 5MB - minimum allowed
        assert!(is_min_allowed_part_size(10 * 1024 * 1024)); // 10MB - allowed
        assert!(is_min_allowed_part_size(100 * 1024 * 1024)); // 100MB - allowed
    }

    #[test]
    fn test_get_complete_multipart_md5() {
        // Test MD5 calculation for multipart upload
        let parts = vec![
            CompletePart {
                part_num: 1,
                etag: Some("d41d8cd98f00b204e9800998ecf8427e".to_string()),
                checksum_crc32: None,
                checksum_crc32c: None,
                checksum_sha1: None,
                checksum_sha256: None,
                checksum_crc64nvme: None,
            },
            CompletePart {
                part_num: 2,
                etag: Some("098f6bcd4621d373cade4e832627b4f6".to_string()),
                checksum_crc32: None,
                checksum_crc32c: None,
                checksum_sha1: None,
                checksum_sha256: None,
                checksum_crc64nvme: None,
            },
        ];

        let md5 = get_complete_multipart_md5(&parts);
        assert!(md5.ends_with("-2")); // Should end with part count
        assert!(md5.len() > 10); // Should have reasonable length

        // Test with empty parts
        let empty_parts = vec![];
        let empty_result = get_complete_multipart_md5(&empty_parts);
        assert!(empty_result.ends_with("-0"));

        // Test with single part
        let single_part = vec![CompletePart {
            part_num: 1,
            etag: Some("d41d8cd98f00b204e9800998ecf8427e".to_string()),
            checksum_crc32: None,
            checksum_crc32c: None,
            checksum_sha1: None,
            checksum_sha256: None,
            checksum_crc64nvme: None,
        }];
        let single_result = get_complete_multipart_md5(&single_part);
        assert!(single_result.ends_with("-1"));
    }

    #[test]
    fn test_get_upload_id_dir() {
        // Test upload ID directory path generation
        let dir = SetDisks::get_upload_id_dir("bucket", "object", "upload-id");
        // The function returns SHA256 hash of bucket/object + upload_id processing
        assert!(dir.len() > 64); // Should be longer than just SHA256 hash
        assert!(dir.contains("/")); // Should contain path separator

        // Test with base64 encoded upload ID
        let result2 = SetDisks::get_upload_id_dir("bucket", "object", "dXBsb2FkLWlk"); // base64 for "upload-id"
        assert!(!result2.is_empty());
        assert!(result2.len() > 10);
    }

    #[test]
    fn test_get_multipart_sha_dir() {
        // Test multipart SHA directory path generation
        let dir = SetDisks::get_multipart_sha_dir("bucket", "object");
        // The function returns SHA256 hash of "bucket/object"
        assert_eq!(dir.len(), 64); // SHA256 hash length
        assert!(!dir.contains("bucket")); // Should be hash, not original text
        assert!(!dir.contains("object")); // Should be hash, not original text

        // Test with empty strings
        let result2 = SetDisks::get_multipart_sha_dir("", "");
        assert!(!result2.is_empty());
        assert_eq!(result2.len(), 64); // SHA256 hex string length

        // Test that different inputs produce different hashes
        let result3 = SetDisks::get_multipart_sha_dir("bucket1", "object1");
        let result4 = SetDisks::get_multipart_sha_dir("bucket2", "object2");
        assert_ne!(result3, result4);
    }

    #[test]
    fn test_common_parity() {
        // Test common parity calculation
        // For parities [2, 2, 2, 3] with n=4, default_parity_count=1:
        // - parity=2: read_quorum = 4-2 = 2, occ=3 >= 2, so valid
        // - parity=3: read_quorum = 4-3 = 1, occ=1 >= 1, so valid
        // - max_occ=3 for parity=2, so returns 2
        let parities = vec![2, 2, 2, 3];
        assert_eq!(SetDisks::common_parity(&parities, 1), 2);

        // For parities [1, 2, 3] with n=3, default_parity_count=2:
        // - parity=1: read_quorum = 3-1 = 2, occ=1 < 2, so invalid
        // - parity=2: read_quorum = 3-2 = 1, occ=1 >= 1, so valid
        // - parity=3: read_quorum = 3-3 = 0, occ=1 >= 0, so valid
        // - max_occ=1, both parity=2 and parity=3 have same occurrence
        // - HashMap iteration order is not guaranteed, so result could be either 2 or 3
        let parities = vec![1, 2, 3];
        let result = SetDisks::common_parity(&parities, 2);
        assert!(result == 2 || result == 3); // Either 2 or 3 is valid

        let empty_parities = vec![];
        assert_eq!(SetDisks::common_parity(&empty_parities, 3), -1); // Empty returns -1

        let invalid_parities = vec![-1, -1, -1];
        assert_eq!(SetDisks::common_parity(&invalid_parities, 2), -1); // all invalid

        let single_parity = vec![4];
        assert_eq!(SetDisks::common_parity(&single_parity, 1), 4);

        // Test with -1 values (ignored)
        let parities_with_invalid = vec![-1, 2, 2, -1];
        assert_eq!(SetDisks::common_parity(&parities_with_invalid, 1), 2);
    }

    #[test]
    fn test_common_time() {
        // Test common time calculation
        let now = OffsetDateTime::now_utc();
        let later = now + Duration::from_secs(60);

        let times = vec![Some(now), Some(now), Some(later)];
        assert_eq!(SetDisks::common_time(&times, 2), Some(now));

        let times2 = vec![Some(now), Some(later), Some(later)];
        assert_eq!(SetDisks::common_time(&times2, 2), Some(later));

        let times_with_none = vec![Some(now), None, Some(now)];
        assert_eq!(SetDisks::common_time(&times_with_none, 2), Some(now));

        let times = vec![None, None, None];
        assert_eq!(SetDisks::common_time(&times, 2), None);

        let empty_times = vec![];
        assert_eq!(SetDisks::common_time(&empty_times, 1), None);
    }

    #[test]
    fn test_common_time_and_occurrence() {
        // Test common time with occurrence count
        let now = OffsetDateTime::now_utc();
        let times = vec![Some(now), Some(now), None];
        let (time, count) = SetDisks::common_time_and_occurrence(&times);
        assert_eq!(time, Some(now));
        assert_eq!(count, 2);

        let times = vec![None, None, None];
        let (time, count) = SetDisks::common_time_and_occurrence(&times);
        assert_eq!(time, None);
        assert_eq!(count, 0); // No valid times, so count is 0
    }

    #[test]
    fn test_common_etag() {
        // Test common etag calculation
        let etags = vec![Some("etag1".to_string()), Some("etag1".to_string()), None];
        assert_eq!(SetDisks::common_etag(&etags, 2), Some("etag1".to_string()));

        let etags = vec![None, None, None];
        assert_eq!(SetDisks::common_etag(&etags, 2), None);
    }

    #[test]
    fn test_common_etags() {
        // Test common etags with occurrence count
        let etags = vec![Some("etag1".to_string()), Some("etag1".to_string()), None];
        let (etag, count) = SetDisks::common_etags(&etags);
        assert_eq!(etag, Some("etag1".to_string()));
        assert_eq!(count, 2);
    }

    #[test]
    fn test_list_object_modtimes() {
        // Test extracting modification times from file info
        let now = OffsetDateTime::now_utc();
        let file_info = FileInfo {
            mod_time: Some(now),
            ..Default::default()
        };
        let parts_metadata = vec![file_info];
        let errs = vec![None];

        let modtimes = SetDisks::list_object_modtimes(&parts_metadata, &errs);
        assert_eq!(modtimes.len(), 1);
        assert_eq!(modtimes[0], Some(now));
    }

    #[test]
    fn test_list_object_etags() {
        // Test extracting etags from file info metadata
        let mut metadata = HashMap::new();
        metadata.insert("etag".to_string(), "test-etag".to_string());

        let file_info = FileInfo {
            metadata,
            ..Default::default()
        };
        let parts_metadata = vec![file_info];
        let errs = vec![None];

        let etags = SetDisks::list_object_etags(&parts_metadata, &errs);
        assert_eq!(etags.len(), 1);
        assert_eq!(etags[0], Some("test-etag".to_string()));
    }

    #[test]
    fn test_list_object_parities() {
        // Test extracting parity counts from file info
        let file_info1 = FileInfo {
            erasure: ErasureInfo {
                data_blocks: 4,
                parity_blocks: 2,
                index: 1,                             // Must be > 0 for is_valid() to return true
                distribution: vec![1, 2, 3, 4, 5, 6], // Must match data_blocks + parity_blocks
                ..Default::default()
            },
            size: 100, // Non-zero size
            deleted: false,
            ..Default::default()
        };
        let file_info2 = FileInfo {
            erasure: ErasureInfo {
                data_blocks: 6,
                parity_blocks: 3,
                index: 1,                                      // Must be > 0 for is_valid() to return true
                distribution: vec![1, 2, 3, 4, 5, 6, 7, 8, 9], // Must match data_blocks + parity_blocks
                ..Default::default()
            },
            size: 200, // Non-zero size
            deleted: false,
            ..Default::default()
        };
        let file_info3 = FileInfo {
            erasure: ErasureInfo {
                data_blocks: 2,
                parity_blocks: 1,
                index: 1,                    // Must be > 0 for is_valid() to return true
                distribution: vec![1, 2, 3], // Must match data_blocks + parity_blocks
                ..Default::default()
            },
            size: 0, // Zero size - function returns half of total shards
            deleted: false,
            ..Default::default()
        };

        let parts_metadata = vec![file_info1, file_info2, file_info3];
        let errs = vec![None, None, None];

        let parities = SetDisks::list_object_parities(&parts_metadata, &errs);
        assert_eq!(parities.len(), 3);
        assert_eq!(parities[0], 2); // parity_blocks from first file
        assert_eq!(parities[1], 3); // parity_blocks from second file
        assert_eq!(parities[2], 1); // half of total shards (3/2 = 1) for zero size file
    }

    #[test]
    fn test_conv_part_err_to_int() {
        // Test error conversion to integer codes
        assert_eq!(conv_part_err_to_int(&None), CHECK_PART_SUCCESS);

        let disk_err = DiskError::FileNotFound;
        assert_eq!(conv_part_err_to_int(&Some(disk_err)), CHECK_PART_FILE_NOT_FOUND);

        let other_err = DiskError::other("other error");
        assert_eq!(conv_part_err_to_int(&Some(other_err)), CHECK_PART_UNKNOWN); // Other errors should return UNKNOWN, not SUCCESS
    }

    #[test]
    fn test_has_part_err() {
        // Test checking for part errors
        let no_errors = vec![CHECK_PART_SUCCESS, CHECK_PART_SUCCESS];
        assert!(!has_part_err(&no_errors));

        let with_errors = vec![CHECK_PART_SUCCESS, CHECK_PART_FILE_NOT_FOUND];
        assert!(has_part_err(&with_errors));

        let unknown_errors = vec![CHECK_PART_UNKNOWN, CHECK_PART_SUCCESS];
        assert!(has_part_err(&unknown_errors));
    }

    #[test]
    fn test_should_heal_object_on_disk() {
        // Test healing decision logic
        let meta = FileInfo::default();
        let latest_meta = FileInfo::default();

        // Test with file not found error
        let err = Some(DiskError::FileNotFound);
        let (should_heal, _, _) = should_heal_object_on_disk(&err, &[], &meta, &latest_meta);
        assert!(should_heal);

        // Test with no error and no part errors
        let (should_heal, _, _) = should_heal_object_on_disk(&None, &[CHECK_PART_SUCCESS], &meta, &latest_meta);
        assert!(!should_heal);

        // Test with part corruption
        let (should_heal, _, _) = should_heal_object_on_disk(&None, &[CHECK_PART_FILE_CORRUPT], &meta, &latest_meta);
        assert!(should_heal);
    }

    #[test]
    fn test_dangling_meta_errs_count() {
        // Test counting dangling metadata errors
        let errs = vec![None, Some(DiskError::FileNotFound), None];
        let (not_found_count, non_actionable_count) = dangling_meta_errs_count(&errs);
        assert_eq!(not_found_count, 1); // One FileNotFound error
        assert_eq!(non_actionable_count, 0); // No other errors
    }

    #[test]
    fn test_dangling_part_errs_count() {
        // Test counting dangling part errors
        let results = vec![CHECK_PART_SUCCESS, CHECK_PART_FILE_NOT_FOUND, CHECK_PART_SUCCESS];
        let (not_found_count, non_actionable_count) = dangling_part_errs_count(&results);
        assert_eq!(not_found_count, 1); // One FILE_NOT_FOUND error
        assert_eq!(non_actionable_count, 0); // No other errors
    }

    #[test]
    fn test_is_object_dir_dangling() {
        // Test object directory dangling detection
        let errs = vec![Some(DiskError::FileNotFound), Some(DiskError::FileNotFound), None];
        assert!(is_object_dir_dangling(&errs));
        let errs2 = vec![None, None, None];
        assert!(!is_object_dir_dangling(&errs2));

        let errs3 = vec![Some(DiskError::FileCorrupt), Some(DiskError::FileNotFound)];
        assert!(!is_object_dir_dangling(&errs3)); // Mixed errors, not all not found
    }

    #[test]
    fn test_join_errs() {
        // Test joining error messages
        let errs = vec![None, Some(DiskError::other("error1")), Some(DiskError::other("error2"))];
        let joined = join_errs(&errs);
        assert!(joined.contains("<nil>"));
        assert!(joined.contains("io error")); // DiskError::other is rendered as "io error"

        // Test with different error types
        let errs2 = vec![None, Some(DiskError::FileNotFound), Some(DiskError::FileCorrupt)];
        let joined2 = join_errs(&errs2);
        assert!(joined2.contains("<nil>"));
        assert!(joined2.contains("file not found"));
        assert!(joined2.contains("file is corrupted"));
    }

    #[test]
    fn test_reduce_common_data_dir() {
        // Test reducing common data directory
        use uuid::Uuid;

        let uuid1 = Uuid::new_v4();
        let uuid2 = Uuid::new_v4();

        let data_dirs = vec![Some(uuid1), Some(uuid1), Some(uuid2)];
        let result = SetDisks::reduce_common_data_dir(&data_dirs, 2);
        assert_eq!(result, Some(uuid1)); // uuid1 appears twice, meets quorum

        let data_dirs = vec![Some(uuid1), Some(uuid2), None];
        let result = SetDisks::reduce_common_data_dir(&data_dirs, 2);
        assert_eq!(result, None); // No UUID meets quorum of 2
    }

    #[test]
    fn test_shuffle_parts_metadata() {
        // Test metadata shuffling
        let metadata = vec![
            FileInfo {
                name: "file1".to_string(),
                ..Default::default()
            },
            FileInfo {
                name: "file2".to_string(),
                ..Default::default()
            },
            FileInfo {
                name: "file3".to_string(),
                ..Default::default()
            },
        ];

        // Distribution uses 1-based indexing
        let distribution = vec![3, 1, 2]; // 1-based shuffle order
        let result = SetDisks::shuffle_parts_metadata(&metadata, &distribution);

        assert_eq!(result.len(), 3);
        assert_eq!(result[0].name, "file2"); // distribution[1] = 1, so metadata[1] goes to index 0
        assert_eq!(result[1].name, "file3"); // distribution[2] = 2, so metadata[2] goes to index 1
        assert_eq!(result[2].name, "file1"); // distribution[0] = 3, so metadata[0] goes to index 2

        // Test with empty distribution
        let empty_distribution = vec![];
        let result2 = SetDisks::shuffle_parts_metadata(&metadata, &empty_distribution);
        assert_eq!(result2.len(), 3);
        assert_eq!(result2[0].name, "file1"); // Should return original order
    }

    #[test]
    fn test_shuffle_disks() {
        // Test disk shuffling
        let disks = vec![None, None, None]; // Mock disks
        let distribution = vec![3, 1, 2]; // 1-based indexing

        let result = SetDisks::shuffle_disks(&disks, &distribution);
        assert_eq!(result.len(), 3);
        // All disks are None, so result should be all None
        assert!(result.iter().all(|d| d.is_none()));

        // Test with empty distribution
        let empty_distribution = vec![];
        let result2 = SetDisks::shuffle_disks(&disks, &empty_distribution);
        assert_eq!(result2.len(), 3);
        assert!(result2.iter().all(|d| d.is_none()));
    }

    #[test]
    fn test_etag_matches() {
        assert!(e_tag_matches("abc", "abc"));
        assert!(e_tag_matches("\"abc\"", "abc"));
        assert!(e_tag_matches("\"abc\"", "*"));
    }

    #[test]
    fn test_should_prevent_write() {
        let oi = ObjectInfo {
            etag: Some("abc".to_string()),
            ..Default::default()
        };
        let if_none_match = Some("abc".to_string());
        let if_match = None;
        assert!(should_prevent_write(&oi, if_none_match, if_match));

        let if_none_match = Some("*".to_string());
        let if_match = None;
        assert!(should_prevent_write(&oi, if_none_match, if_match));

        let if_none_match = None;
        let if_match = Some("def".to_string());
        assert!(should_prevent_write(&oi, if_none_match, if_match));

        let if_none_match = None;
        let if_match = Some("*".to_string());
        assert!(!should_prevent_write(&oi, if_none_match, if_match));

        let if_none_match = Some("def".to_string());
        let if_match = None;
        assert!(!should_prevent_write(&oi, if_none_match, if_match));

        let if_none_match = Some("def".to_string());
        let if_match = Some("*".to_string());
        assert!(!should_prevent_write(&oi, if_none_match, if_match));

        let if_none_match = Some("def".to_string());
        let if_match = Some("\"abc\"".to_string());
        assert!(!should_prevent_write(&oi, if_none_match, if_match));

        let if_none_match = Some("*".to_string());
        let if_match = Some("\"abc\"".to_string());
        assert!(should_prevent_write(&oi, if_none_match, if_match));

        let oi = ObjectInfo {
            etag: None,
            ..Default::default()
        };
        let if_none_match = Some("*".to_string());
        let if_match = Some("\"abc\"".to_string());
        assert!(should_prevent_write(&oi, if_none_match, if_match));

        let if_none_match = None;
        let if_match = None;
        assert!(!should_prevent_write(&oi, if_none_match, if_match));
    }

    #[test]
    fn test_is_valid_storage_class() {
        // Test valid storage classes
        assert!(is_valid_storage_class(storageclass::STANDARD));
        assert!(is_valid_storage_class(storageclass::RRS));
        assert!(is_valid_storage_class(storageclass::DEEP_ARCHIVE));
        assert!(is_valid_storage_class(storageclass::EXPRESS_ONEZONE));
        assert!(is_valid_storage_class(storageclass::GLACIER));
        assert!(is_valid_storage_class(storageclass::GLACIER_IR));
        assert!(is_valid_storage_class(storageclass::INTELLIGENT_TIERING));
        assert!(is_valid_storage_class(storageclass::ONEZONE_IA));
        assert!(is_valid_storage_class(storageclass::OUTPOSTS));
        assert!(is_valid_storage_class(storageclass::SNOW));
        assert!(is_valid_storage_class(storageclass::STANDARD_IA));

        // Test invalid storage classes
        assert!(!is_valid_storage_class("INVALID"));
        assert!(!is_valid_storage_class(""));
        assert!(!is_valid_storage_class("standard")); // lowercase
    }

    #[test]
    fn test_is_cold_storage_class() {
        // Test cold storage classes
        assert!(is_cold_storage_class(storageclass::DEEP_ARCHIVE));
        assert!(is_cold_storage_class(storageclass::GLACIER));
        assert!(is_cold_storage_class(storageclass::GLACIER_IR));

        // Test non-cold storage classes
        assert!(!is_cold_storage_class(storageclass::STANDARD));
        assert!(!is_cold_storage_class(storageclass::RRS));
        assert!(!is_cold_storage_class(storageclass::STANDARD_IA));
        assert!(!is_cold_storage_class(storageclass::EXPRESS_ONEZONE));
    }

    #[test]
    fn test_is_infrequent_access_class() {
        // Test infrequent access classes
        assert!(is_infrequent_access_class(storageclass::ONEZONE_IA));
        assert!(is_infrequent_access_class(storageclass::STANDARD_IA));
        assert!(is_infrequent_access_class(storageclass::INTELLIGENT_TIERING));

        // Test frequent access classes
        assert!(!is_infrequent_access_class(storageclass::STANDARD));
        assert!(!is_infrequent_access_class(storageclass::RRS));
        assert!(!is_infrequent_access_class(storageclass::DEEP_ARCHIVE));
        assert!(!is_infrequent_access_class(storageclass::EXPRESS_ONEZONE));
    }
}
