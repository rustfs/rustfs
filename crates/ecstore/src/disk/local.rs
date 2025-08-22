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

use super::error::{Error, Result};
use super::os::{is_root_disk, rename_all};
use super::{
    BUCKET_META_PREFIX, CheckPartsResp, DeleteOptions, DiskAPI, DiskInfo, DiskInfoOptions, DiskLocation, DiskMetrics,
    FileInfoVersions, RUSTFS_META_BUCKET, ReadMultipleReq, ReadMultipleResp, ReadOptions, RenameDataResp,
    STORAGE_FORMAT_FILE_BACKUP, UpdateMetadataOpts, VolumeInfo, WalkDirOptions, os,
};
use super::{endpoint::Endpoint, error::DiskError, format::FormatV3};

use crate::disk::error::FileAccessDeniedWithContext;
use crate::disk::error_conv::{to_access_error, to_file_error, to_unformatted_disk_error, to_volume_error};
use crate::disk::fs::{
    O_APPEND, O_CREATE, O_RDONLY, O_TRUNC, O_WRONLY, access, lstat, lstat_std, remove, remove_all_std, remove_std, rename,
};
use crate::disk::os::{check_path_length, is_empty_dir};
use crate::disk::{
    CHECK_PART_FILE_CORRUPT, CHECK_PART_FILE_NOT_FOUND, CHECK_PART_SUCCESS, CHECK_PART_UNKNOWN, CHECK_PART_VOLUME_NOT_FOUND,
    FileReader, RUSTFS_META_TMP_DELETED_BUCKET, conv_part_err_to_int,
};
use crate::disk::{FileWriter, STORAGE_FORMAT_FILE};
use crate::global::{GLOBAL_IsErasureSD, GLOBAL_RootDiskThreshold};
use rustfs_utils::path::{
    GLOBAL_DIR_SUFFIX, GLOBAL_DIR_SUFFIX_WITH_SLASH, SLASH_SEPARATOR, clean, decode_dir_object, encode_dir_object, has_suffix,
    path_join, path_join_buf,
};
use tokio::time::interval;

use crate::erasure_coding::bitrot_verify;
use bytes::Bytes;
use path_absolutize::Absolutize;
use rustfs_filemeta::{
    Cache, FileInfo, FileInfoOpts, FileMeta, MetaCacheEntry, MetacacheWriter, ObjectPartInfo, Opts, RawFileInfo, UpdateFn,
    get_file_info, read_xl_meta_no_data,
};
use rustfs_utils::HashAlgorithm;
use rustfs_utils::os::get_info;
use std::collections::HashSet;
use std::fmt::Debug;
use std::io::SeekFrom;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;
use std::{
    fs::Metadata,
    path::{Path, PathBuf},
};
use time::OffsetDateTime;
use tokio::fs::{self, File};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWrite, AsyncWriteExt, ErrorKind};
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use super::fsync_batcher::{FsyncBatcher, FsyncBatcherConfig, FsyncMode}; // 引入fsync批处理器

#[derive(Debug)]
pub struct FormatInfo {
    pub id: Option<Uuid>,
    pub data: Bytes,
    pub file_info: Option<Metadata>,
    pub last_check: Option<OffsetDateTime>,
}

impl FormatInfo {
    pub fn last_check_valid(&self) -> bool {
        let now = OffsetDateTime::now_utc();
        self.file_info.is_some()
            && self.id.is_some()
            && self.last_check.is_some()
            && (now.unix_timestamp() - self.last_check.unwrap().unix_timestamp() <= 1)
    }
}

/// A helper enum to handle internal buffer types for writing data.
pub enum InternalBuf<'a> {
    Ref(&'a [u8]),
    Owned(Bytes),
}

pub struct LocalDisk {
    pub root: PathBuf,
    pub format_path: PathBuf,
    pub format_info: Arc<RwLock<FormatInfo>>,
    pub endpoint: Endpoint,
    pub disk_info_cache: Arc<Cache<DiskInfo>>,
    pub scanning: AtomicU32,
    pub rotational: bool,
    pub fstype: String,
    pub major: u64,
    pub minor: u64,
    pub nrrequests: u64,
    // pub id: Mutex<Option<Uuid>>,
    // pub format_data: Mutex<Vec<u8>>,
    // pub format_file_info: Mutex<Option<Metadata>>,
    // pub format_last_check: Mutex<Option<OffsetDateTime>>,
    exit_signal: Option<tokio::sync::broadcast::Sender<()>>,
    // 添加fsync批处理器
    // 这个批处理器会根据配置智能地批量执行fsync操作，减少系统调用开销
    fsync_batcher: FsyncBatcher,
}

impl Drop for LocalDisk {
    fn drop(&mut self) {
        if let Some(exit_signal) = self.exit_signal.take() {
            let _ = exit_signal.send(());
        }
    }
}

impl Debug for LocalDisk {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalDisk")
            .field("root", &self.root)
            .field("format_path", &self.format_path)
            .field("format_info", &self.format_info)
            .field("endpoint", &self.endpoint)
            .finish()
    }
}

impl LocalDisk {
    pub async fn new(ep: &Endpoint, cleanup: bool) -> Result<Self> {
        debug!("Creating local disk");
        let root = match PathBuf::from(ep.get_file_path()).absolutize() {
            Ok(path) => path.into_owned(),
            Err(e) => {
                if e.kind() == ErrorKind::NotFound {
                    return Err(DiskError::VolumeNotFound);
                }
                return Err(to_file_error(e).into());
            }
        };

        if cleanup {
            // TODO: 删除 tmp 数据
        }

        let format_path = Path::new(RUSTFS_META_BUCKET)
            .join(Path::new(super::FORMAT_CONFIG_FILE))
            .absolutize_virtually(&root)?
            .into_owned();
        debug!("format_path: {:?}", format_path);
        let (format_data, format_meta) = read_file_exists(&format_path).await?;

        let mut id = None;
        // let mut format_legacy = false;
        let mut format_last_check = None;

        if !format_data.is_empty() {
            let s = format_data.as_ref();
            let fm = FormatV3::try_from(s).map_err(Error::other)?;
            let (set_idx, disk_idx) = fm.find_disk_index_by_disk_id(fm.erasure.this)?;

            if set_idx as i32 != ep.set_idx || disk_idx as i32 != ep.disk_idx {
                return Err(DiskError::InconsistentDisk);
            }

            id = Some(fm.erasure.this);
            // format_legacy = fm.erasure.distribution_algo == DistributionAlgoVersion::V1;
            format_last_check = Some(OffsetDateTime::now_utc());
        }

        let format_info = FormatInfo {
            id,
            data: format_data,
            file_info: format_meta,
            last_check: format_last_check,
        };
        let root_clone = root.clone();
        let update_fn: UpdateFn<DiskInfo> = Box::new(move || {
            let disk_id = id.map_or("".to_string(), |id| id.to_string());
            let root = root_clone.clone();
            Box::pin(async move {
                match get_disk_info(root.clone()).await {
                    Ok((info, root)) => {
                        let disk_info = DiskInfo {
                            total: info.total,
                            free: info.free,
                            used: info.used,
                            used_inodes: info.files - info.ffree,
                            free_inodes: info.ffree,
                            major: info.major,
                            minor: info.minor,
                            fs_type: info.fstype,
                            root_disk: root,
                            id: disk_id.to_string(),
                            ..Default::default()
                        };
                        // if root {
                        //     return Err(Error::new(DiskError::DriveIsRoot));
                        // }

                        // disk_info.healing =
                        Ok(disk_info)
                    }
                    Err(err) => Err(err.into()),
                }
            })
        });

        let cache = Cache::new(update_fn, Duration::from_secs(1), Opts::default());

        // TODO: DIRECT support
        // TODD: DiskInfo
        let mut disk = Self {
            root: root.clone(),
            endpoint: ep.clone(),
            format_path,
            format_info: Arc::new(RwLock::new(format_info)),
            disk_info_cache: Arc::new(cache),
            scanning: AtomicU32::new(0),
            rotational: Default::default(),
            fstype: Default::default(),
            minor: Default::default(),
            major: Default::default(),
            nrrequests: Default::default(),
            // // format_legacy,
            // format_file_info: Mutex::new(format_meta),
            // format_data: Mutex::new(format_data),
            // format_last_check: Mutex::new(format_last_check),
            exit_signal: None,
            fsync_batcher: FsyncBatcher::new(FsyncBatcherConfig {
                mode: FsyncMode::Batch,                    // 默认使用批量模式
                batch_size: 32,                            // 批量大小，可根据系统配置调整
                batch_timeout: Duration::from_millis(100), // 100ms超时
                adaptive: true,                            // 启用自适应调整
                max_pending: 1000,                         // 最大等待文件数
            }),
        };
        let (info, _root) = get_disk_info(root).await?;
        disk.major = info.major;
        disk.minor = info.minor;
        disk.fstype = info.fstype;

        // if root {
        //     return Err(Error::new(DiskError::DriveIsRoot));
        // }

        if info.nrrequests > 0 {
            disk.nrrequests = info.nrrequests;
        }

        if info.rotational {
            disk.rotational = true;
        }

        disk.make_meta_volumes().await?;

        let (exit_tx, exit_rx) = tokio::sync::broadcast::channel(1);
        disk.exit_signal = Some(exit_tx);

        let root = disk.root.clone();
        tokio::spawn(Self::cleanup_deleted_objects_loop(root, exit_rx));
        debug!("LocalDisk created: {:?}", disk);
        Ok(disk)
    }

    async fn cleanup_deleted_objects_loop(root: PathBuf, mut exit_rx: tokio::sync::broadcast::Receiver<()>) {
        let mut interval = interval(Duration::from_secs(60 * 5));
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if let Err(err) = Self::cleanup_deleted_objects(root.clone()).await {
                        error!("cleanup_deleted_objects error: {:?}", err);
                    }
                }
                _ = exit_rx.recv() => {
                    info!("cleanup_deleted_objects_loop exit");
                    break;
                }
            }
        }
    }

    async fn cleanup_deleted_objects(root: PathBuf) -> Result<()> {
        let trash = path_join(&[root, RUSTFS_META_TMP_DELETED_BUCKET.into()]);
        let mut entries = fs::read_dir(&trash).await?;
        while let Some(entry) = entries.next_entry().await? {
            let name = entry.file_name().to_string_lossy().to_string();
            if name.is_empty() || name == "." || name == ".." {
                continue;
            }

            let file_type = entry.file_type().await?;

            let path = path_join(&[trash.clone(), name.into()]);

            if file_type.is_dir() {
                if let Err(e) = tokio::fs::remove_dir_all(path).await {
                    if e.kind() != ErrorKind::NotFound {
                        return Err(e.into());
                    }
                }
            } else if let Err(e) = tokio::fs::remove_file(path).await {
                if e.kind() != ErrorKind::NotFound {
                    return Err(e.into());
                }
            }
        }

        Ok(())
    }

    fn is_valid_volname(volname: &str) -> bool {
        if volname.len() < 3 {
            return false;
        }

        if cfg!(target_os = "windows") {
            // 在 Windows 上，卷名不应该包含保留字符。
            // 这个正则表达式匹配了不允许的字符。
            if volname.contains('|')
                || volname.contains('<')
                || volname.contains('>')
                || volname.contains('?')
                || volname.contains('*')
                || volname.contains(':')
                || volname.contains('"')
                || volname.contains('\\')
            {
                return false;
            }
        } else {
            // 对于非 Windows 系统，可能需要其他的验证逻辑。
        }

        true
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn check_format_json(&self) -> Result<Metadata> {
        let md = std::fs::metadata(&self.format_path).map_err(to_unformatted_disk_error)?;
        Ok(md)
    }
    async fn make_meta_volumes(&self) -> Result<()> {
        let buckets = format!("{RUSTFS_META_BUCKET}/{BUCKET_META_PREFIX}");
        let multipart = format!("{}/{}", RUSTFS_META_BUCKET, "multipart");
        let config = format!("{}/{}", RUSTFS_META_BUCKET, "config");
        let tmp = format!("{}/{}", RUSTFS_META_BUCKET, "tmp");

        let defaults = vec![
            buckets.as_str(),
            multipart.as_str(),
            config.as_str(),
            tmp.as_str(),
            RUSTFS_META_TMP_DELETED_BUCKET,
        ];

        self.make_volumes(defaults).await
    }

    pub fn resolve_abs_path(&self, path: impl AsRef<Path>) -> Result<PathBuf> {
        Ok(path.as_ref().absolutize_virtually(&self.root)?.into_owned())
    }

    pub fn get_object_path(&self, bucket: &str, key: &str) -> Result<PathBuf> {
        let dir = Path::new(&bucket);
        let file_path = Path::new(&key);
        self.resolve_abs_path(dir.join(file_path))
    }

    pub fn get_bucket_path(&self, bucket: &str) -> Result<PathBuf> {
        let dir = Path::new(&bucket);
        self.resolve_abs_path(dir)
    }

    // /// Write to the filesystem atomically.
    // /// This is done by first writing to a temporary location and then moving the file.
    // pub(crate) async fn prepare_file_write<'a>(&self, path: &'a PathBuf) -> Result<FileWriter<'a>> {
    //     let tmp_path = self.get_object_path(RUSTFS_META_TMP_BUCKET, Uuid::new_v4().to_string().as_str())?;

    //     debug!("prepare_file_write tmp_path:{:?}, path:{:?}", &tmp_path, &path);

    //     let file = File::create(&tmp_path).await?;
    //     let writer = BufWriter::new(file);
    //     Ok(FileWriter {
    //         tmp_path,
    //         dest_path: path,
    //         writer,
    //         clean_tmp: true,
    //     })
    // }

    #[allow(unreachable_code)]
    #[allow(unused_variables)]
    pub async fn move_to_trash(&self, delete_path: &PathBuf, recursive: bool, immediate_purge: bool) -> Result<()> {
        // if recursive {
        //     remove_all_std(delete_path).map_err(to_volume_error)?;
        // } else {
        //     remove_std(delete_path).map_err(to_file_error)?;
        // }

        // return Ok(());

        // TODO: 异步通知 检测硬盘空间 清空回收站

        let trash_path = self.get_object_path(super::RUSTFS_META_TMP_DELETED_BUCKET, Uuid::new_v4().to_string().as_str())?;
        // if let Some(parent) = trash_path.parent() {
        //     if !parent.exists() {
        //         fs::create_dir_all(parent).await?;
        //     }
        // }

        let err = if recursive {
            rename_all(delete_path, trash_path, self.get_bucket_path(super::RUSTFS_META_TMP_DELETED_BUCKET)?)
                .await
                .err()
        } else {
            rename(&delete_path, &trash_path)
                .await
                .map_err(|e| to_file_error(e).into())
                .err()
        };

        if immediate_purge || delete_path.to_string_lossy().ends_with(SLASH_SEPARATOR) {
            let trash_path2 = self.get_object_path(super::RUSTFS_META_TMP_DELETED_BUCKET, Uuid::new_v4().to_string().as_str())?;
            let _ = rename_all(
                encode_dir_object(delete_path.to_string_lossy().as_ref()),
                trash_path2,
                self.get_bucket_path(super::RUSTFS_META_TMP_DELETED_BUCKET)?,
            )
            .await;
        }

        if let Some(err) = err {
            if err == Error::DiskFull {
                if recursive {
                    remove_all_std(delete_path).map_err(to_volume_error)?;
                } else {
                    remove_std(delete_path).map_err(to_file_error)?;
                }
            }

            return Ok(());
        }

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    #[async_recursion::async_recursion]
    pub async fn delete_file(
        &self,
        base_path: &PathBuf,
        delete_path: &PathBuf,
        recursive: bool,
        immediate_purge: bool,
    ) -> Result<()> {
        // debug!("delete_file {:?}\n base_path:{:?}", &delete_path, &base_path);

        if is_root_path(base_path) || is_root_path(delete_path) {
            // debug!("delete_file skip {:?}", &delete_path);
            return Ok(());
        }

        if !delete_path.starts_with(base_path) || base_path == delete_path {
            // debug!("delete_file skip {:?}", &delete_path);
            return Ok(());
        }

        if recursive {
            self.move_to_trash(delete_path, recursive, immediate_purge).await?;
        } else if delete_path.is_dir() {
            // debug!("delete_file remove_dir {:?}", &delete_path);
            if let Err(err) = fs::remove_dir(&delete_path).await {
                // debug!("remove_dir err {:?} when {:?}", &err, &delete_path);
                match err.kind() {
                    ErrorKind::NotFound => (),
                    ErrorKind::DirectoryNotEmpty => (),
                    kind => {
                        warn!("delete_file remove_dir {:?} err {}", &delete_path, kind.to_string());
                        return Err(Error::other(FileAccessDeniedWithContext {
                            path: delete_path.clone(),
                            source: err,
                        }));
                    }
                }
            }
            // debug!("delete_file remove_dir done {:?}", &delete_path);
        } else if let Err(err) = fs::remove_file(&delete_path).await {
            // debug!("remove_file err {:?} when {:?}", &err, &delete_path);
            match err.kind() {
                ErrorKind::NotFound => (),
                _ => {
                    warn!("delete_file remove_file {:?}  err {:?}", &delete_path, &err);
                    return Err(Error::other(FileAccessDeniedWithContext {
                        path: delete_path.clone(),
                        source: err,
                    }));
                }
            }
        }

        if let Some(dir_path) = delete_path.parent() {
            Box::pin(self.delete_file(base_path, &PathBuf::from(dir_path), false, false)).await?;
        }

        // debug!("delete_file done {:?}", &delete_path);
        Ok(())
    }

    /// read xl.meta raw data
    #[tracing::instrument(level = "debug", skip(self, volume_dir, file_path))]
    async fn read_raw(
        &self,
        bucket: &str,
        volume_dir: impl AsRef<Path>,
        file_path: impl AsRef<Path>,
        read_data: bool,
    ) -> Result<(Vec<u8>, Option<OffsetDateTime>)> {
        if file_path.as_ref().as_os_str().is_empty() {
            return Err(DiskError::FileNotFound);
        }

        let meta_path = file_path.as_ref().join(Path::new(STORAGE_FORMAT_FILE));

        let res = {
            if read_data {
                self.read_all_data_with_dmtime(bucket, volume_dir, meta_path).await
            } else {
                match self.read_metadata_with_dmtime(meta_path).await {
                    Ok(res) => Ok(res),
                    Err(err) => {
                        if err == Error::FileNotFound
                            && !skip_access_checks(volume_dir.as_ref().to_string_lossy().to_string().as_str())
                        {
                            if let Err(e) = access(volume_dir.as_ref()).await {
                                if e.kind() == ErrorKind::NotFound {
                                    // warn!("read_metadata_with_dmtime os err {:?}", &aerr);
                                    return Err(DiskError::VolumeNotFound);
                                }
                            }
                        }

                        Err(err)
                    }
                }
            }
        };

        let (buf, mtime) = res?;
        if buf.is_empty() {
            return Err(DiskError::FileNotFound);
        }

        Ok((buf, mtime))
    }

    async fn read_metadata(&self, file_path: impl AsRef<Path>) -> Result<Vec<u8>> {
        // TODO: support timeout
        let (data, _) = self.read_metadata_with_dmtime(file_path.as_ref()).await?;
        Ok(data)
    }

    async fn read_metadata_with_dmtime(&self, file_path: impl AsRef<Path>) -> Result<(Vec<u8>, Option<OffsetDateTime>)> {
        check_path_length(file_path.as_ref().to_string_lossy().as_ref())?;

        let mut f = super::fs::open_file(file_path.as_ref(), O_RDONLY)
            .await
            .map_err(to_file_error)?;

        let meta = f.metadata().await.map_err(to_file_error)?;

        if meta.is_dir() {
            // fix use io::Error
            return Err(Error::FileNotFound);
        }

        let size = meta.len() as usize;

        let data = read_xl_meta_no_data(&mut f, size).await?;

        let modtime = match meta.modified() {
            Ok(md) => Some(OffsetDateTime::from(md)),
            Err(_) => None,
        };

        Ok((data, modtime))
    }

    async fn read_all_data(&self, volume: &str, volume_dir: impl AsRef<Path>, file_path: impl AsRef<Path>) -> Result<Vec<u8>> {
        // TODO: timeout support
        let (data, _) = self.read_all_data_with_dmtime(volume, volume_dir, file_path).await?;
        Ok(data)
    }

    #[tracing::instrument(level = "debug", skip(self, volume_dir, file_path))]
    async fn read_all_data_with_dmtime(
        &self,
        volume: &str,
        volume_dir: impl AsRef<Path>,
        file_path: impl AsRef<Path>,
    ) -> Result<(Vec<u8>, Option<OffsetDateTime>)> {
        let mut f = match super::fs::open_file(file_path.as_ref(), O_RDONLY).await {
            Ok(f) => f,
            Err(e) => {
                if e.kind() == ErrorKind::NotFound && !skip_access_checks(volume) {
                    if let Err(er) = access(volume_dir.as_ref()).await {
                        if er.kind() == ErrorKind::NotFound {
                            warn!("read_all_data_with_dmtime os err {:?}", &er);
                            return Err(DiskError::VolumeNotFound);
                        }
                    }
                }

                return Err(to_file_error(e).into());
            }
        };

        let meta = f.metadata().await.map_err(to_file_error)?;

        if meta.is_dir() {
            return Err(DiskError::FileNotFound);
        }

        let size = meta.len() as usize;
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(size).map_err(Error::other)?;

        f.read_to_end(&mut bytes).await.map_err(to_file_error)?;

        let modtime = match meta.modified() {
            Ok(md) => Some(OffsetDateTime::from(md)),
            Err(_) => None,
        };

        Ok((bytes, modtime))
    }

    async fn delete_versions_internal(&self, volume: &str, path: &str, fis: &Vec<FileInfo>) -> Result<()> {
        let volume_dir = self.get_bucket_path(volume)?;
        let xlpath = self.get_object_path(volume, format!("{path}/{STORAGE_FORMAT_FILE}").as_str())?;

        let (data, _) = self.read_all_data_with_dmtime(volume, volume_dir.as_path(), &xlpath).await?;

        if data.is_empty() {
            return Err(DiskError::FileNotFound);
        }

        let mut fm = FileMeta::default();

        fm.unmarshal_msg(&data)?;

        for fi in fis {
            let data_dir = match fm.delete_version(fi) {
                Ok(res) => res,
                Err(err) => {
                    let err: DiskError = err.into();
                    if !fi.deleted && (err == DiskError::FileNotFound || err == DiskError::FileVersionNotFound) {
                        continue;
                    }

                    return Err(err);
                }
            };

            if let Some(dir) = data_dir {
                let vid = fi.version_id.unwrap_or_default();
                let _ = fm.data.remove(vec![vid, dir]);

                let dir_path = self.get_object_path(volume, format!("{path}/{dir}").as_str())?;
                if let Err(err) = self.move_to_trash(&dir_path, true, false).await {
                    if !(err == DiskError::FileNotFound || err == DiskError::VolumeNotFound) {
                        return Err(err);
                    }
                };
            }
        }

        // 没有版本了，删除 xl.meta
        if fm.versions.is_empty() {
            self.delete_file(&volume_dir, &xlpath, true, false).await?;
            return Ok(());
        }

        // 更新 xl.meta
        let buf = fm.marshal_msg()?;

        let volume_dir = self.get_bucket_path(volume)?;

        self.write_all_private(volume, format!("{path}/{STORAGE_FORMAT_FILE}").as_str(), buf.into(), true, &volume_dir)
            .await?;

        Ok(())
    }

    async fn write_all_meta(&self, volume: &str, path: &str, buf: &[u8], sync: bool) -> Result<()> {
        let volume_dir = self.get_bucket_path(volume)?;
        let file_path = volume_dir.join(Path::new(&path));
        check_path_length(file_path.to_string_lossy().as_ref())?;

        let tmp_volume_dir = self.get_bucket_path(super::RUSTFS_META_TMP_BUCKET)?;
        let tmp_file_path = tmp_volume_dir.join(Path::new(Uuid::new_v4().to_string().as_str()));

        self.write_all_internal(&tmp_file_path, InternalBuf::Ref(buf), sync, &tmp_volume_dir)
            .await?;

        rename_all(tmp_file_path, file_path, volume_dir).await
    }

    // write_all_public for trail
    async fn write_all_public(&self, volume: &str, path: &str, data: Bytes) -> Result<()> {
        if volume == RUSTFS_META_BUCKET && path == super::FORMAT_CONFIG_FILE {
            let mut format_info = self.format_info.write().await;
            format_info.data.clone_from(&data);
        }

        let volume_dir = self.get_bucket_path(volume)?;

        self.write_all_private(volume, path, data, true, &volume_dir).await?;

        Ok(())
    }

    // write_all_private with check_path_length
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn write_all_private(&self, volume: &str, path: &str, buf: Bytes, sync: bool, skip_parent: &Path) -> Result<()> {
        let volume_dir = self.get_bucket_path(volume)?;
        let file_path = volume_dir.join(Path::new(&path));
        check_path_length(file_path.to_string_lossy().as_ref())?;

        self.write_all_internal(&file_path, InternalBuf::Owned(buf), sync, skip_parent)
            .await
    }
    // write_all_internal do write file
    pub async fn write_all_internal(
        &self,
        file_path: &Path,
        data: InternalBuf<'_>,
        sync: bool,
        skip_parent: &Path,
    ) -> Result<()> {
        let flags = O_CREATE | O_WRONLY | O_TRUNC;

        let mut f = self.open_file(file_path, flags, skip_parent).await?;

        match data {
            InternalBuf::Ref(buf) => {
                f.write_all(buf).await.map_err(to_file_error)?;
            }
            InternalBuf::Owned(buf) => {
                // Reduce one copy by using the owned buffer directly.
                // It may be more efficient for larger writes.
                let std_file = f.into_std().await;
                let task = tokio::task::spawn_blocking(move || {
                    use std::io::Write as _;
                    let mut f = std_file;
                    f.write_all(buf.as_ref()).map_err(to_file_error)?;
                    Ok::<std::fs::File, Error>(f) // 返回文件句柄以便后续使用
                });
                let std_file = task.await??;

                // 将文件句柄转回tokio::fs::File以支持异步fsync
                f = tokio::fs::File::from_std(std_file);
            }
        }

        // 使用批处理器处理fsync
        // 根据sync参数决定是否需要同步，如果需要则加入批处理队列
        if sync {
            // 将文件加入fsync批处理队列
            // 批处理器会智能地决定何时执行实际的fsync操作
            self.fsync_batcher
                .add_file(f, file_path.to_string_lossy().to_string())
                .await
                .map_err(to_file_error)?;
        }

        Ok(())
    }

    async fn open_file(&self, path: impl AsRef<Path>, mode: usize, skip_parent: impl AsRef<Path>) -> Result<File> {
        let mut skip_parent = skip_parent.as_ref();
        if skip_parent.as_os_str().is_empty() {
            skip_parent = self.root.as_path();
        }

        if let Some(parent) = path.as_ref().parent() {
            super::os::make_dir_all(parent, skip_parent).await?;
        }

        let f = super::fs::open_file(path.as_ref(), mode).await.map_err(to_file_error)?;

        Ok(f)
    }

    #[allow(dead_code)]
    fn get_metrics(&self) -> DiskMetrics {
        DiskMetrics::default()
    }

    async fn bitrot_verify(
        &self,
        part_path: &PathBuf,
        part_size: usize,
        algo: HashAlgorithm,
        sum: &[u8],
        shard_size: usize,
    ) -> Result<()> {
        let file = super::fs::open_file(part_path, O_CREATE | O_WRONLY)
            .await
            .map_err(to_file_error)?;

        let meta = file.metadata().await.map_err(to_file_error)?;
        let file_size = meta.len() as usize;

        bitrot_verify(Box::new(file), file_size, part_size, algo, bytes::Bytes::copy_from_slice(sum), shard_size)
            .await
            .map_err(to_file_error)?;

        Ok(())
    }

    #[async_recursion::async_recursion]
    async fn scan_dir<W>(
        &self,
        current: &mut String,
        opts: &WalkDirOptions,
        out: &mut MetacacheWriter<W>,
        objs_returned: &mut i32,
    ) -> Result<()>
    where
        W: AsyncWrite + Unpin + Send,
    {
        let forward = {
            opts.forward_to.as_ref().filter(|v| v.starts_with(&*current)).map(|v| {
                let forward = v.trim_start_matches(&*current);
                if let Some(idx) = forward.find('/') {
                    forward[..idx].to_owned()
                } else {
                    forward.to_owned()
                }
            })
            // if let Some(forward_to) = &opts.forward_to {

            // } else {
            //     None
            // }
            // if !opts.forward_to.is_empty() && opts.forward_to.starts_with(&*current) {
            //     let forward = opts.forward_to.trim_start_matches(&*current);
            //     if let Some(idx) = forward.find('/') {
            //         &forward[..idx]
            //     } else {
            //         forward
            //     }
            // } else {
            //     ""
            // }
        };

        if opts.limit > 0 && *objs_returned >= opts.limit {
            return Ok(());
        }

        let mut entries = match self.list_dir("", &opts.bucket, current, -1).await {
            Ok(res) => res,
            Err(e) => {
                if e != DiskError::VolumeNotFound && e != Error::FileNotFound {
                    debug!("scan list_dir {}, err {:?}", &current, &e);
                }

                if opts.report_notfound && e == Error::FileNotFound && current == &opts.base_dir {
                    return Err(DiskError::FileNotFound);
                }

                return Ok(());
            }
        };

        if entries.is_empty() {
            return Ok(());
        }

        let s = SLASH_SEPARATOR.chars().next().unwrap_or_default();
        *current = current.trim_matches(s).to_owned();

        let bucket = opts.bucket.as_str();

        let mut dir_objes = HashSet::new();

        // 第一层过滤
        for item in entries.iter_mut() {
            let entry = item.clone();
            // check limit
            if opts.limit > 0 && *objs_returned >= opts.limit {
                return Ok(());
            }
            // check prefix
            if let Some(filter_prefix) = &opts.filter_prefix {
                if !entry.starts_with(filter_prefix) {
                    *item = "".to_owned();
                    continue;
                }
            }

            if let Some(forward) = &forward {
                if &entry < forward {
                    *item = "".to_owned();
                    continue;
                }
            }

            if entry.ends_with(SLASH_SEPARATOR) {
                if entry.ends_with(GLOBAL_DIR_SUFFIX_WITH_SLASH) {
                    let entry = format!("{}{}", entry.as_str().trim_end_matches(GLOBAL_DIR_SUFFIX_WITH_SLASH), SLASH_SEPARATOR);
                    dir_objes.insert(entry.clone());
                    *item = entry;
                    continue;
                }

                *item = entry.trim_end_matches(SLASH_SEPARATOR).to_owned();
                continue;
            }

            *item = "".to_owned();

            if entry.ends_with(STORAGE_FORMAT_FILE) {
                //
                let metadata = self
                    .read_metadata(self.get_object_path(bucket, format!("{}/{}", &current, &entry).as_str())?)
                    .await?;

                // 用 strip_suffix 只删除一次
                let entry = entry.strip_suffix(STORAGE_FORMAT_FILE).unwrap_or_default().to_owned();
                let name = entry.trim_end_matches(SLASH_SEPARATOR);
                let name = decode_dir_object(format!("{}/{}", &current, &name).as_str());

                out.write_obj(&MetaCacheEntry {
                    name: name.clone(),
                    metadata,
                    ..Default::default()
                })
                .await?;
                *objs_returned += 1;

                // warn!("scan list_dir {}, write_obj done, name: {:?}", &current, &name);
                return Ok(());
            }
        }

        entries.sort();

        let mut entries = entries.as_slice();
        if let Some(forward) = &forward {
            for (i, entry) in entries.iter().enumerate() {
                if entry >= forward || forward.starts_with(entry.as_str()) {
                    entries = &entries[i..];
                    break;
                }
            }
        }

        let mut dir_stack: Vec<String> = Vec::with_capacity(5);

        for entry in entries.iter() {
            if opts.limit > 0 && *objs_returned >= opts.limit {
                // warn!("scan list_dir {}, limit reached 2", &current);
                return Ok(());
            }

            if entry.is_empty() {
                continue;
            }

            let name = path_join_buf(&[current, entry]);

            if !dir_stack.is_empty() {
                if let Some(pop) = dir_stack.last().cloned() {
                    if pop < name {
                        out.write_obj(&MetaCacheEntry {
                            name: pop.clone(),
                            ..Default::default()
                        })
                        .await?;

                        if opts.recursive {
                            let mut opts = opts.clone();
                            opts.filter_prefix = None;
                            if let Err(er) = Box::pin(self.scan_dir(&mut pop.clone(), &opts, out, objs_returned)).await {
                                error!("scan_dir err {:?}", er);
                            }
                        }
                        dir_stack.pop();
                    }
                }
            }

            let mut meta = MetaCacheEntry {
                name,
                ..Default::default()
            };

            let mut is_dir_obj = false;

            if let Some(_dir) = dir_objes.get(entry) {
                is_dir_obj = true;
                meta.name
                    .truncate(meta.name.len() - meta.name.chars().last().unwrap().len_utf8());
                meta.name.push_str(GLOBAL_DIR_SUFFIX_WITH_SLASH);
            }

            let fname = format!("{}/{}", &meta.name, STORAGE_FORMAT_FILE);

            match self.read_metadata(self.get_object_path(&opts.bucket, fname.as_str())?).await {
                Ok(res) => {
                    if is_dir_obj {
                        meta.name = meta.name.trim_end_matches(GLOBAL_DIR_SUFFIX_WITH_SLASH).to_owned();
                        meta.name.push_str(SLASH_SEPARATOR);
                    }

                    meta.metadata = res;

                    out.write_obj(&meta).await?;
                    *objs_returned += 1;
                }
                Err(err) => {
                    if err == Error::FileNotFound || err == Error::IsNotRegular {
                        // NOT an object, append to stack (with slash)
                        // If dirObject, but no metadata (which is unexpected) we skip it.
                        if !is_dir_obj && !is_empty_dir(self.get_object_path(&opts.bucket, &meta.name)?).await {
                            meta.name.push_str(SLASH_SEPARATOR);
                            dir_stack.push(meta.name);
                        }
                    }

                    continue;
                }
            };
        }

        while let Some(dir) = dir_stack.pop() {
            if opts.limit > 0 && *objs_returned >= opts.limit {
                // warn!("scan list_dir {}, limit reached 3", &current);
                return Ok(());
            }

            out.write_obj(&MetaCacheEntry {
                name: dir.clone(),
                ..Default::default()
            })
            .await?;
            *objs_returned += 1;

            if opts.recursive {
                let mut dir = dir;
                let mut opts = opts.clone();
                opts.filter_prefix = None;
                if let Err(er) = Box::pin(self.scan_dir(&mut dir, &opts, out, objs_returned)).await {
                    warn!("scan_dir err {:?}", &er);
                }
            }
        }

        // warn!("scan list_dir {}, done", &current);
        Ok(())
    }
}

fn is_root_path(path: impl AsRef<Path>) -> bool {
    path.as_ref().components().count() == 1 && path.as_ref().has_root()
}

// 过滤 std::io::ErrorKind::NotFound
pub async fn read_file_exists(path: impl AsRef<Path>) -> Result<(Bytes, Option<Metadata>)> {
    let p = path.as_ref();
    let (data, meta) = match read_file_all(&p).await {
        Ok((data, meta)) => (data, Some(meta)),
        Err(e) => {
            if e == Error::FileNotFound {
                (Bytes::new(), None)
            } else {
                return Err(e);
            }
        }
    };

    // let mut data = Vec::new();
    // if meta.is_some() {
    //     data = fs::read(&p).await?;
    // }

    Ok((data, meta))
}

pub async fn read_file_all(path: impl AsRef<Path>) -> Result<(Bytes, Metadata)> {
    let p = path.as_ref();
    let meta = read_file_metadata(&path).await?;

    let data = fs::read(&p).await.map_err(to_file_error)?;

    Ok((data.into(), meta))
}

pub async fn read_file_metadata(p: impl AsRef<Path>) -> Result<Metadata> {
    let meta = fs::metadata(&p).await.map_err(to_file_error)?;

    Ok(meta)
}

fn skip_access_checks(p: impl AsRef<str>) -> bool {
    let vols = [
        super::RUSTFS_META_TMP_DELETED_BUCKET,
        super::RUSTFS_META_TMP_BUCKET,
        super::RUSTFS_META_MULTIPART_BUCKET,
        RUSTFS_META_BUCKET,
    ];

    for v in vols.iter() {
        if p.as_ref().starts_with(v) {
            return true;
        }
    }

    false
}

#[async_trait::async_trait]
impl DiskAPI for LocalDisk {
    #[tracing::instrument(skip(self))]
    fn to_string(&self) -> String {
        self.root.to_string_lossy().to_string()
    }
    #[tracing::instrument(skip(self))]
    fn is_local(&self) -> bool {
        true
    }
    #[tracing::instrument(skip(self))]
    fn host_name(&self) -> String {
        self.endpoint.host_port()
    }
    #[tracing::instrument(skip(self))]
    async fn is_online(&self) -> bool {
        self.check_format_json().await.is_ok()
    }

    #[tracing::instrument(skip(self))]
    fn endpoint(&self) -> Endpoint {
        self.endpoint.clone()
    }

    #[tracing::instrument(skip(self))]
    async fn close(&self) -> Result<()> {
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    fn path(&self) -> PathBuf {
        self.root.clone()
    }

    #[tracing::instrument(skip(self))]
    fn get_disk_location(&self) -> DiskLocation {
        DiskLocation {
            pool_idx: {
                if self.endpoint.pool_idx < 0 {
                    None
                } else {
                    Some(self.endpoint.pool_idx as usize)
                }
            },
            set_idx: {
                if self.endpoint.set_idx < 0 {
                    None
                } else {
                    Some(self.endpoint.set_idx as usize)
                }
            },
            disk_idx: {
                if self.endpoint.disk_idx < 0 {
                    None
                } else {
                    Some(self.endpoint.disk_idx as usize)
                }
            },
        }
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn get_disk_id(&self) -> Result<Option<Uuid>> {
        let mut format_info = self.format_info.write().await;

        let id = format_info.id;

        if format_info.last_check_valid() {
            return Ok(id);
        }

        let file_meta = self.check_format_json().await?;

        if let Some(file_info) = &format_info.file_info {
            if super::fs::same_file(&file_meta, file_info) {
                format_info.last_check = Some(OffsetDateTime::now_utc());

                return Ok(id);
            }
        }

        let b = fs::read(&self.format_path).await.map_err(to_unformatted_disk_error)?;

        let fm = FormatV3::try_from(b.as_slice()).map_err(|e| {
            warn!("decode format.json  err {:?}", e);
            DiskError::CorruptedBackend
        })?;

        let (m, n) = fm.find_disk_index_by_disk_id(fm.erasure.this)?;

        let disk_id = fm.erasure.this;

        if m as i32 != self.endpoint.set_idx || n as i32 != self.endpoint.disk_idx {
            return Err(DiskError::InconsistentDisk);
        }

        format_info.id = Some(disk_id);
        format_info.file_info = Some(file_meta);
        format_info.data = b.into();
        format_info.last_check = Some(OffsetDateTime::now_utc());

        Ok(Some(disk_id))
    }

    #[tracing::instrument(skip(self))]
    async fn set_disk_id(&self, id: Option<Uuid>) -> Result<()> {
        // No setup is required locally
        // TODO: add check_id_store
        let mut format_info = self.format_info.write().await;
        format_info.id = id;
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn read_all(&self, volume: &str, path: &str) -> Result<Bytes> {
        if volume == RUSTFS_META_BUCKET && path == super::FORMAT_CONFIG_FILE {
            let format_info = self.format_info.read().await;
            if !format_info.data.is_empty() {
                return Ok(format_info.data.clone());
            }
        }
        // TOFIX:
        let p = self.get_object_path(volume, path)?;
        let (data, _) = read_file_all(&p).await?;

        Ok(data)
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn write_all(&self, volume: &str, path: &str, data: Bytes) -> Result<()> {
        self.write_all_public(volume, path, data).await
    }

    #[tracing::instrument(skip(self))]
    async fn delete(&self, volume: &str, path: &str, opt: DeleteOptions) -> Result<()> {
        let volume_dir = self.get_bucket_path(volume)?;
        if !skip_access_checks(volume) {
            if let Err(e) = access(&volume_dir).await {
                return Err(to_access_error(e, DiskError::VolumeAccessDenied).into());
            }
        }

        let file_path = volume_dir.join(Path::new(&path));
        check_path_length(file_path.to_string_lossy().to_string().as_str())?;

        self.delete_file(&volume_dir, &file_path, opt.recursive, opt.immediate)
            .await?;

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn verify_file(&self, volume: &str, path: &str, fi: &FileInfo) -> Result<CheckPartsResp> {
        let volume_dir = self.get_bucket_path(volume)?;
        if !skip_access_checks(volume) {
            if let Err(e) = access(&volume_dir).await {
                return Err(to_access_error(e, DiskError::VolumeAccessDenied).into());
            }
        }

        let mut resp = CheckPartsResp {
            results: vec![0; fi.parts.len()],
        };

        let erasure = &fi.erasure;
        for (i, part) in fi.parts.iter().enumerate() {
            let checksum_info = erasure.get_checksum_info(part.number);
            let part_path = Path::new(&volume_dir)
                .join(path)
                .join(fi.data_dir.map_or("".to_string(), |dir| dir.to_string()))
                .join(format!("part.{}", part.number));
            let err = self
                .bitrot_verify(
                    &part_path,
                    erasure.shard_file_size(part.size as i64) as usize,
                    checksum_info.algorithm,
                    &checksum_info.hash,
                    erasure.shard_size(),
                )
                .await
                .err();
            resp.results[i] = conv_part_err_to_int(&err);
            if resp.results[i] == CHECK_PART_UNKNOWN {
                if let Some(err) = err {
                    if err == DiskError::FileAccessDenied {
                        continue;
                    }
                    info!("part unknown, disk: {}, path: {:?}", self.to_string(), part_path);
                }
            }
        }

        Ok(resp)
    }

    #[tracing::instrument(skip(self))]
    async fn read_parts(&self, bucket: &str, paths: &[String]) -> Result<Vec<ObjectPartInfo>> {
        let volume_dir = self.get_bucket_path(bucket)?;

        let mut ret = vec![ObjectPartInfo::default(); paths.len()];

        for (i, path_str) in paths.iter().enumerate() {
            let path = Path::new(path_str);
            let file_name = path.file_name().and_then(|v| v.to_str()).unwrap_or_default();
            let num = file_name
                .strip_prefix("part.")
                .and_then(|v| v.strip_suffix(".meta"))
                .and_then(|v| v.parse::<usize>().ok())
                .unwrap_or_default();

            if let Err(err) = access(
                volume_dir
                    .clone()
                    .join(path.parent().unwrap_or(Path::new("")).join(format!("part.{num}"))),
            )
            .await
            {
                ret[i] = ObjectPartInfo {
                    number: num,
                    error: Some(err.to_string()),
                    ..Default::default()
                };
                continue;
            }

            let data = match self
                .read_all_data(bucket, volume_dir.clone(), volume_dir.clone().join(path))
                .await
            {
                Ok(data) => data,
                Err(err) => {
                    ret[i] = ObjectPartInfo {
                        number: num,
                        error: Some(err.to_string()),
                        ..Default::default()
                    };
                    continue;
                }
            };

            match ObjectPartInfo::unmarshal(&data) {
                Ok(meta) => {
                    ret[i] = meta;
                }
                Err(err) => {
                    ret[i] = ObjectPartInfo {
                        number: num,
                        error: Some(err.to_string()),
                        ..Default::default()
                    };
                }
            };
        }

        Ok(ret)
    }
    #[tracing::instrument(skip(self))]
    async fn check_parts(&self, volume: &str, path: &str, fi: &FileInfo) -> Result<CheckPartsResp> {
        let volume_dir = self.get_bucket_path(volume)?;
        check_path_length(volume_dir.join(path).to_string_lossy().as_ref())?;
        let mut resp = CheckPartsResp {
            results: vec![0; fi.parts.len()],
        };

        for (i, part) in fi.parts.iter().enumerate() {
            let file_path = Path::new(&volume_dir)
                .join(path)
                .join(fi.data_dir.map_or("".to_string(), |dir| dir.to_string()))
                .join(format!("part.{}", part.number));

            match lstat(file_path).await {
                Ok(st) => {
                    if st.is_dir() {
                        resp.results[i] = CHECK_PART_FILE_NOT_FOUND;
                        continue;
                    }
                    if (st.len() as i64) < fi.erasure.shard_file_size(part.size as i64) {
                        resp.results[i] = CHECK_PART_FILE_CORRUPT;
                        continue;
                    }

                    resp.results[i] = CHECK_PART_SUCCESS;
                }
                Err(err) => {
                    let e: DiskError = to_file_error(err).into();

                    if e == DiskError::FileNotFound {
                        if !skip_access_checks(volume) {
                            if let Err(err) = access(&volume_dir).await {
                                if err.kind() == ErrorKind::NotFound {
                                    resp.results[i] = CHECK_PART_VOLUME_NOT_FOUND;
                                    continue;
                                }
                            }
                        }
                        resp.results[i] = CHECK_PART_FILE_NOT_FOUND;
                    }
                    continue;
                }
            }
        }

        Ok(resp)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn rename_part(&self, src_volume: &str, src_path: &str, dst_volume: &str, dst_path: &str, meta: Bytes) -> Result<()> {
        let src_volume_dir = self.get_bucket_path(src_volume)?;
        let dst_volume_dir = self.get_bucket_path(dst_volume)?;
        if !skip_access_checks(src_volume) {
            super::fs::access_std(&src_volume_dir).map_err(|e| to_access_error(e, DiskError::VolumeAccessDenied))?
        }
        if !skip_access_checks(dst_volume) {
            super::fs::access_std(&dst_volume_dir).map_err(|e| to_access_error(e, DiskError::VolumeAccessDenied))?
        }

        let src_is_dir = has_suffix(src_path, SLASH_SEPARATOR);
        let dst_is_dir = has_suffix(dst_path, SLASH_SEPARATOR);

        if !src_is_dir && dst_is_dir || src_is_dir && !dst_is_dir {
            warn!(
                "rename_part src and dst must be both dir or file src_is_dir:{}, dst_is_dir:{}",
                src_is_dir, dst_is_dir
            );
            return Err(DiskError::FileAccessDenied);
        }

        let src_file_path = src_volume_dir.join(Path::new(src_path));
        let dst_file_path = dst_volume_dir.join(Path::new(dst_path));

        // warn!("rename_part src_file_path:{:?}, dst_file_path:{:?}", &src_file_path, &dst_file_path);

        check_path_length(src_file_path.to_string_lossy().as_ref())?;
        check_path_length(dst_file_path.to_string_lossy().as_ref())?;

        if src_is_dir {
            let meta_op = match lstat_std(&src_file_path).map_err(|e| to_file_error(e).into()) {
                Ok(meta) => Some(meta),
                Err(e) => {
                    if e != DiskError::FileNotFound {
                        return Err(e);
                    }

                    None
                }
            };

            if let Some(meta) = meta_op {
                if !meta.is_dir() {
                    warn!("rename_part src is not dir {:?}", &src_file_path);
                    return Err(DiskError::FileAccessDenied);
                }
            }

            remove_std(&dst_file_path).map_err(to_file_error)?;
        }

        rename_all(&src_file_path, &dst_file_path, &dst_volume_dir).await?;

        self.write_all(dst_volume, format!("{dst_path}.meta").as_str(), meta).await?;

        if let Some(parent) = src_file_path.parent() {
            self.delete_file(&src_volume_dir, &parent.to_path_buf(), false, false).await?;
        }

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn rename_file(&self, src_volume: &str, src_path: &str, dst_volume: &str, dst_path: &str) -> Result<()> {
        let src_volume_dir = self.get_bucket_path(src_volume)?;
        let dst_volume_dir = self.get_bucket_path(dst_volume)?;
        if !skip_access_checks(src_volume) {
            access(&src_volume_dir)
                .await
                .map_err(|e| to_access_error(e, DiskError::VolumeAccessDenied))?;
        }
        if !skip_access_checks(dst_volume) {
            access(&dst_volume_dir)
                .await
                .map_err(|e| to_access_error(e, DiskError::VolumeAccessDenied))?;
        }

        let src_is_dir = has_suffix(src_path, SLASH_SEPARATOR);
        let dst_is_dir = has_suffix(dst_path, SLASH_SEPARATOR);
        if (dst_is_dir || src_is_dir) && (!dst_is_dir || !src_is_dir) {
            return Err(Error::from(DiskError::FileAccessDenied));
        }

        let src_file_path = src_volume_dir.join(Path::new(&src_path));
        check_path_length(src_file_path.to_string_lossy().to_string().as_str())?;

        let dst_file_path = dst_volume_dir.join(Path::new(&dst_path));
        check_path_length(dst_file_path.to_string_lossy().to_string().as_str())?;

        if src_is_dir {
            let meta_op = match lstat(&src_file_path).await {
                Ok(meta) => Some(meta),
                Err(e) => {
                    let e: DiskError = to_file_error(e).into();
                    if e != DiskError::FileNotFound {
                        return Err(e);
                    } else {
                        None
                    }
                }
            };

            if let Some(meta) = meta_op {
                if !meta.is_dir() {
                    return Err(DiskError::FileAccessDenied);
                }
            }

            remove(&dst_file_path).await.map_err(to_file_error)?;
        }

        rename_all(&src_file_path, &dst_file_path, &dst_volume_dir).await?;

        if let Some(parent) = src_file_path.parent() {
            let _ = self.delete_file(&src_volume_dir, &parent.to_path_buf(), false, false).await;
        }

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn create_file(&self, origvolume: &str, volume: &str, path: &str, _file_size: i64) -> Result<FileWriter> {
        if !origvolume.is_empty() {
            let origvolume_dir = self.get_bucket_path(origvolume)?;
            if !skip_access_checks(origvolume) {
                access(origvolume_dir)
                    .await
                    .map_err(|e| to_access_error(e, DiskError::VolumeAccessDenied))?;
            }
        }

        let volume_dir = self.get_bucket_path(volume)?;
        let file_path = volume_dir.join(Path::new(&path));
        check_path_length(file_path.to_string_lossy().to_string().as_str())?;

        //  TODO: writeAllDirect io.copy
        // info!("file_path: {:?}", file_path);
        if let Some(parent) = file_path.parent() {
            os::make_dir_all(parent, &volume_dir).await?;
        }
        let f = super::fs::open_file(&file_path, O_CREATE | O_WRONLY)
            .await
            .map_err(to_file_error)?;

        Ok(Box::new(f))

        // Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    // async fn append_file(&self, volume: &str, path: &str, mut r: DuplexStream) -> Result<File> {
    async fn append_file(&self, volume: &str, path: &str) -> Result<FileWriter> {
        let volume_dir = self.get_bucket_path(volume)?;
        if !skip_access_checks(volume) {
            access(&volume_dir)
                .await
                .map_err(|e| to_access_error(e, DiskError::VolumeAccessDenied))?;
        }

        let file_path = volume_dir.join(Path::new(&path));
        check_path_length(file_path.to_string_lossy().to_string().as_str())?;

        let f = self.open_file(file_path, O_CREATE | O_APPEND | O_WRONLY, volume_dir).await?;

        Ok(Box::new(f))
    }

    // TODO: io verifier
    #[tracing::instrument(level = "debug", skip(self))]
    async fn read_file(&self, volume: &str, path: &str) -> Result<FileReader> {
        // warn!("disk read_file: volume: {}, path: {}", volume, path);
        let volume_dir = self.get_bucket_path(volume)?;
        if !skip_access_checks(volume) {
            access(&volume_dir)
                .await
                .map_err(|e| to_access_error(e, DiskError::VolumeAccessDenied))?;
        }

        let file_path = volume_dir.join(Path::new(&path));
        check_path_length(file_path.to_string_lossy().to_string().as_str())?;

        let f = self.open_file(file_path, O_RDONLY, volume_dir).await?;

        Ok(Box::new(f))
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn read_file_stream(&self, volume: &str, path: &str, offset: usize, length: usize) -> Result<FileReader> {
        let volume_dir = self.get_bucket_path(volume)?;
        if !skip_access_checks(volume) {
            access(&volume_dir)
                .await
                .map_err(|e| to_access_error(e, DiskError::VolumeAccessDenied))?;
        }

        let file_path = volume_dir.join(Path::new(&path));
        check_path_length(file_path.to_string_lossy().to_string().as_str())?;

        let mut f = self.open_file(file_path, O_RDONLY, volume_dir).await?;

        let meta = f.metadata().await?;
        if meta.len() < (offset + length) as u64 {
            error!(
                "read_file_stream: file size is less than offset + length {} + {} = {}",
                offset,
                length,
                meta.len()
            );
            return Err(DiskError::FileCorrupt);
        }

        if offset > 0 {
            f.seek(SeekFrom::Start(offset as u64)).await?;
        }

        Ok(Box::new(f))
    }
    #[tracing::instrument(level = "debug", skip(self))]
    async fn list_dir(&self, origvolume: &str, volume: &str, dir_path: &str, count: i32) -> Result<Vec<String>> {
        if !origvolume.is_empty() {
            let origvolume_dir = self.get_bucket_path(origvolume)?;
            if !skip_access_checks(origvolume) {
                if let Err(e) = access(origvolume_dir).await {
                    return Err(to_access_error(e, DiskError::VolumeAccessDenied).into());
                }
            }
        }

        let volume_dir = self.get_bucket_path(volume)?;
        let dir_path_abs = volume_dir.join(Path::new(&dir_path.trim_start_matches(SLASH_SEPARATOR)));

        let entries = match os::read_dir(&dir_path_abs, count).await {
            Ok(res) => res,
            Err(e) => {
                if e.kind() == std::io::ErrorKind::NotFound && !skip_access_checks(volume) {
                    if let Err(e) = access(&volume_dir).await {
                        return Err(to_access_error(e, DiskError::VolumeAccessDenied).into());
                    }
                }

                return Err(to_file_error(e).into());
            }
        };

        Ok(entries)
    }

    // FIXME: TODO: io.writer TODO cancel
    #[tracing::instrument(level = "debug", skip(self, wr))]
    async fn walk_dir<W: AsyncWrite + Unpin + Send>(&self, opts: WalkDirOptions, wr: &mut W) -> Result<()> {
        let volume_dir = self.get_bucket_path(&opts.bucket)?;

        if !skip_access_checks(&opts.bucket) {
            if let Err(e) = access(&volume_dir).await {
                return Err(to_access_error(e, DiskError::VolumeAccessDenied).into());
            }
        }

        let mut wr = wr;

        let mut out = MetacacheWriter::new(&mut wr);

        let mut objs_returned = 0;

        if opts.base_dir.ends_with(SLASH_SEPARATOR) {
            let fpath = self.get_object_path(
                &opts.bucket,
                path_join_buf(&[
                    format!("{}{}", opts.base_dir.trim_end_matches(SLASH_SEPARATOR), GLOBAL_DIR_SUFFIX).as_str(),
                    STORAGE_FORMAT_FILE,
                ])
                .as_str(),
            )?;

            if let Ok(data) = self.read_metadata(fpath).await {
                let meta = MetaCacheEntry {
                    name: opts.base_dir.clone(),
                    metadata: data,
                    ..Default::default()
                };
                out.write_obj(&meta).await?;
                objs_returned += 1;
            } else {
                let fpath =
                    self.get_object_path(&opts.bucket, path_join_buf(&[opts.base_dir.as_str(), STORAGE_FORMAT_FILE]).as_str())?;

                if let Ok(meta) = tokio::fs::metadata(fpath).await
                    && meta.is_file()
                {
                    return Err(DiskError::FileNotFound);
                }
            }
        }

        let mut current = opts.base_dir.clone();
        self.scan_dir(&mut current, &opts, &mut out, &mut objs_returned).await?;

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self, fi))]
    async fn rename_data(
        &self,
        src_volume: &str,
        src_path: &str,
        fi: FileInfo,
        dst_volume: &str,
        dst_path: &str,
    ) -> Result<RenameDataResp> {
        let src_volume_dir = self.get_bucket_path(src_volume)?;
        if !skip_access_checks(src_volume) {
            if let Err(e) = super::fs::access_std(&src_volume_dir) {
                info!("access checks failed, src_volume_dir: {:?}, err: {}", src_volume_dir, e.to_string());
                return Err(to_access_error(e, DiskError::VolumeAccessDenied).into());
            }
        }

        let dst_volume_dir = self.get_bucket_path(dst_volume)?;
        if !skip_access_checks(dst_volume) {
            if let Err(e) = super::fs::access_std(&dst_volume_dir) {
                info!("access checks failed, dst_volume_dir: {:?}, err: {}", dst_volume_dir, e.to_string());
                return Err(to_access_error(e, DiskError::VolumeAccessDenied).into());
            }
        }

        // xl.meta 路径
        let src_file_path = src_volume_dir.join(Path::new(format!("{}/{}", &src_path, STORAGE_FORMAT_FILE).as_str()));
        let dst_file_path = dst_volume_dir.join(Path::new(format!("{}/{}", &dst_path, STORAGE_FORMAT_FILE).as_str()));

        // data_dir 路径
        let has_data_dir_path = {
            let has_data_dir = {
                if !fi.is_remote() {
                    fi.data_dir
                        .map(|dir| rustfs_utils::path::retain_slash(dir.to_string().as_str()))
                } else {
                    None
                }
            };

            if let Some(data_dir) = has_data_dir {
                let src_data_path = src_volume_dir.join(Path::new(
                    rustfs_utils::path::retain_slash(format!("{}/{}", &src_path, data_dir).as_str()).as_str(),
                ));
                let dst_data_path = dst_volume_dir.join(Path::new(
                    rustfs_utils::path::retain_slash(format!("{}/{}", &dst_path, data_dir).as_str()).as_str(),
                ));

                Some((src_data_path, dst_data_path))
            } else {
                None
            }
        };

        check_path_length(src_file_path.to_string_lossy().to_string().as_str())?;
        check_path_length(dst_file_path.to_string_lossy().to_string().as_str())?;

        // 读旧 xl.meta

        let has_dst_buf = match super::fs::read_file(&dst_file_path).await {
            Ok(res) => Some(res),
            Err(e) => {
                let e: DiskError = to_file_error(e).into();

                if e != DiskError::FileNotFound {
                    return Err(e);
                }

                None
            }
        };

        let mut xlmeta = FileMeta::new();

        if let Some(dst_buf) = has_dst_buf.as_ref() {
            if FileMeta::is_xl2_v1_format(dst_buf) {
                if let Ok(nmeta) = FileMeta::load(dst_buf) {
                    xlmeta = nmeta
                }
            }
        }

        let mut skip_parent = dst_volume_dir.clone();
        if has_dst_buf.as_ref().is_some() {
            if let Some(parent) = dst_file_path.parent() {
                skip_parent = parent.to_path_buf();
            }
        }

        // TODO: Healing

        let has_old_data_dir = {
            if let Ok((_, ver)) = xlmeta.find_version(fi.version_id) {
                let has_data_dir = ver.get_data_dir();
                if let Some(data_dir) = has_data_dir {
                    if xlmeta.shard_data_dir_count(&fi.version_id, &Some(data_dir)) == 0 {
                        // TODO: Healing
                        // remove inlinedata\
                        Some(data_dir)
                    } else {
                        None
                    }
                } else {
                    None
                }
            } else {
                None
            }
        };

        xlmeta.add_version(fi.clone())?;

        if xlmeta.versions.len() <= 10 {
            // TODO: Sign
        }

        let new_dst_buf = xlmeta.marshal_msg()?;

        self.write_all(src_volume, format!("{}/{}", &src_path, STORAGE_FORMAT_FILE).as_str(), new_dst_buf.into())
            .await?;
        if let Some((src_data_path, dst_data_path)) = has_data_dir_path.as_ref() {
            let no_inline = fi.data.is_none() && fi.size > 0;
            if no_inline {
                if let Err(err) = rename_all(&src_data_path, &dst_data_path, &skip_parent).await {
                    let _ = self.delete_file(&dst_volume_dir, dst_data_path, false, false).await;
                    info!(
                        "rename all failed src_data_path: {:?}, dst_data_path: {:?}, err: {:?}",
                        src_data_path, dst_data_path, err
                    );
                    return Err(err);
                }
            }
        }

        if let Some(old_data_dir) = has_old_data_dir {
            // preserve current xl.meta inside the oldDataDir.
            if let Some(dst_buf) = has_dst_buf {
                if let Err(err) = self
                    .write_all_private(
                        dst_volume,
                        format!("{}/{}/{}", &dst_path, &old_data_dir.to_string(), STORAGE_FORMAT_FILE).as_str(),
                        dst_buf.into(),
                        true,
                        &skip_parent,
                    )
                    .await
                {
                    info!("write_all_private failed err: {:?}", err);
                    return Err(err);
                }
            }
        }

        if let Err(err) = rename_all(&src_file_path, &dst_file_path, &skip_parent).await {
            if let Some((_, dst_data_path)) = has_data_dir_path.as_ref() {
                let _ = self.delete_file(&dst_volume_dir, dst_data_path, false, false).await;
            }
            info!("rename all failed err: {:?}", err);
            return Err(err);
        }

        if let Some(src_file_path_parent) = src_file_path.parent() {
            if src_volume != super::RUSTFS_META_MULTIPART_BUCKET {
                let _ = remove_std(src_file_path_parent);
            } else {
                let _ = self
                    .delete_file(&dst_volume_dir, &src_file_path_parent.to_path_buf(), true, false)
                    .await;
            }
        }

        Ok(RenameDataResp {
            old_data_dir: has_old_data_dir,
            sign: None, // TODO:
        })
    }

    #[tracing::instrument(skip(self))]
    async fn make_volumes(&self, volumes: Vec<&str>) -> Result<()> {
        for vol in volumes {
            if let Err(e) = self.make_volume(vol).await {
                if e != DiskError::VolumeExists {
                    return Err(e);
                }
            }
            // TODO: health check
        }
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn make_volume(&self, volume: &str) -> Result<()> {
        if !Self::is_valid_volname(volume) {
            return Err(Error::other("Invalid arguments specified"));
        }

        let volume_dir = self.get_bucket_path(volume)?;

        if let Err(e) = access(&volume_dir).await {
            if e.kind() == std::io::ErrorKind::NotFound {
                os::make_dir_all(&volume_dir, self.root.as_path()).await?;
                return Ok(());
            }
            return Err(to_volume_error(e).into());
        }

        Err(DiskError::VolumeExists)
    }

    #[tracing::instrument(skip(self))]
    async fn list_volumes(&self) -> Result<Vec<VolumeInfo>> {
        let mut volumes = Vec::new();

        let entries = os::read_dir(&self.root, -1).await.map_err(to_volume_error)?;

        for entry in entries {
            if !has_suffix(&entry, SLASH_SEPARATOR) || !Self::is_valid_volname(clean(&entry).as_str()) {
                continue;
            }

            volumes.push(VolumeInfo {
                name: clean(&entry),
                created: None,
            });
        }

        Ok(volumes)
    }

    #[tracing::instrument(skip(self))]
    async fn stat_volume(&self, volume: &str) -> Result<VolumeInfo> {
        let volume_dir = self.get_bucket_path(volume)?;
        let meta = lstat(&volume_dir).await.map_err(to_volume_error)?;

        let modtime = match meta.modified() {
            Ok(md) => Some(OffsetDateTime::from(md)),
            Err(_) => None,
        };

        Ok(VolumeInfo {
            name: volume.to_string(),
            created: modtime,
        })
    }

    #[tracing::instrument(skip(self))]
    async fn delete_paths(&self, volume: &str, paths: &[String]) -> Result<()> {
        let volume_dir = self.get_bucket_path(volume)?;
        if !skip_access_checks(volume) {
            access(&volume_dir)
                .await
                .map_err(|e| to_access_error(e, DiskError::VolumeAccessDenied))?;
        }

        for path in paths.iter() {
            let file_path = volume_dir.join(Path::new(path));

            check_path_length(file_path.to_string_lossy().as_ref())?;

            self.move_to_trash(&file_path, false, false).await?;
        }

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn update_metadata(&self, volume: &str, path: &str, fi: FileInfo, opts: &UpdateMetadataOpts) -> Result<()> {
        if !fi.metadata.is_empty() {
            let volume_dir = self.get_bucket_path(volume)?;
            let file_path = volume_dir.join(Path::new(&path));

            check_path_length(file_path.to_string_lossy().as_ref())?;

            let buf = self
                .read_all(volume, format!("{}/{}", &path, STORAGE_FORMAT_FILE).as_str())
                .await
                .map_err(|e| {
                    if e == DiskError::FileNotFound && fi.version_id.is_some() {
                        DiskError::FileVersionNotFound
                    } else {
                        e
                    }
                })?;

            if !FileMeta::is_xl2_v1_format(buf.as_ref()) {
                return Err(DiskError::FileVersionNotFound);
            }

            let mut xl_meta = FileMeta::load(buf.as_ref())?;

            xl_meta.update_object_version(fi)?;

            let wbuf = xl_meta.marshal_msg()?;

            return self
                .write_all_meta(volume, format!("{path}/{STORAGE_FORMAT_FILE}").as_str(), &wbuf, !opts.no_persistence)
                .await;
        }

        Err(Error::other("Invalid Argument"))
    }

    #[tracing::instrument(skip(self))]
    async fn write_metadata(&self, _org_volume: &str, volume: &str, path: &str, fi: FileInfo) -> Result<()> {
        let p = self.get_object_path(volume, format!("{path}/{STORAGE_FORMAT_FILE}").as_str())?;

        let mut meta = FileMeta::new();
        if !fi.fresh {
            let (buf, _) = read_file_exists(&p).await?;
            if !buf.is_empty() {
                let _ = meta.unmarshal_msg(&buf).map_err(|_| {
                    meta = FileMeta::new();
                });
            }
        }

        meta.add_version(fi)?;

        let fm_data = meta.marshal_msg()?;

        self.write_all(volume, format!("{path}/{STORAGE_FORMAT_FILE}").as_str(), fm_data.into())
            .await?;

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn read_version(
        &self,
        _org_volume: &str,
        volume: &str,
        path: &str,
        version_id: &str,
        opts: &ReadOptions,
    ) -> Result<FileInfo> {
        let file_path = self.get_object_path(volume, path)?;
        let file_dir = self.get_bucket_path(volume)?;

        let read_data = opts.read_data;

        let (data, _) = self.read_raw(volume, file_dir, file_path, read_data).await?;

        let fi = get_file_info(&data, volume, path, version_id, FileInfoOpts { data: read_data }).await?;

        Ok(fi)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn read_xl(&self, volume: &str, path: &str, read_data: bool) -> Result<RawFileInfo> {
        let file_path = self.get_object_path(volume, path)?;
        let file_dir = self.get_bucket_path(volume)?;

        let (buf, _) = self.read_raw(volume, file_dir, file_path, read_data).await?;

        Ok(RawFileInfo { buf })
    }

    #[tracing::instrument(skip(self))]
    async fn delete_version(
        &self,
        volume: &str,
        path: &str,
        fi: FileInfo,
        force_del_marker: bool,
        opts: DeleteOptions,
    ) -> Result<()> {
        if path.starts_with(SLASH_SEPARATOR) {
            return self
                .delete(
                    volume,
                    path,
                    DeleteOptions {
                        recursive: false,
                        immediate: false,
                        ..Default::default()
                    },
                )
                .await;
        }

        let volume_dir = self.get_bucket_path(volume)?;

        let file_path = volume_dir.join(Path::new(&path));

        check_path_length(file_path.to_string_lossy().as_ref())?;

        let xl_path = file_path.join(Path::new(STORAGE_FORMAT_FILE));
        let buf = match self.read_all_data(volume, &volume_dir, &xl_path).await {
            Ok(res) => res,
            Err(err) => {
                //
                if err != DiskError::FileNotFound {
                    return Err(err);
                }

                if fi.deleted && force_del_marker {
                    return self.write_metadata("", volume, path, fi).await;
                }

                return if fi.version_id.is_some() {
                    Err(DiskError::FileVersionNotFound)
                } else {
                    Err(DiskError::FileNotFound)
                };
            }
        };

        let mut meta = FileMeta::load(&buf)?;
        let old_dir = meta.delete_version(&fi)?;

        if let Some(uuid) = old_dir {
            let vid = fi.version_id.unwrap_or_default();
            let _ = meta.data.remove(vec![vid, uuid])?;

            let old_path = file_path.join(Path::new(uuid.to_string().as_str()));
            check_path_length(old_path.to_string_lossy().as_ref())?;

            if let Err(err) = self.move_to_trash(&old_path, true, false).await {
                if err != DiskError::FileNotFound && err != DiskError::VolumeNotFound {
                    return Err(err);
                }
            }
        }

        if !meta.versions.is_empty() {
            let buf = meta.marshal_msg()?;
            return self
                .write_all_meta(volume, format!("{path}{SLASH_SEPARATOR}{STORAGE_FORMAT_FILE}").as_str(), &buf, true)
                .await;
        }

        // opts.undo_write && opts.old_data_dir.is_some_and(f)
        if let Some(old_data_dir) = opts.old_data_dir {
            if opts.undo_write {
                let src_path =
                    file_path.join(Path::new(format!("{old_data_dir}{SLASH_SEPARATOR}{STORAGE_FORMAT_FILE_BACKUP}").as_str()));
                let dst_path = file_path.join(Path::new(format!("{path}{SLASH_SEPARATOR}{STORAGE_FORMAT_FILE}").as_str()));
                return rename_all(src_path, dst_path, file_path).await;
            }
        }

        self.delete_file(&volume_dir, &xl_path, true, false).await
    }
    #[tracing::instrument(level = "debug", skip(self))]
    async fn delete_versions(
        &self,
        volume: &str,
        versions: Vec<FileInfoVersions>,
        _opts: DeleteOptions,
    ) -> Result<Vec<Option<Error>>> {
        let mut errs = Vec::with_capacity(versions.len());
        for _ in 0..versions.len() {
            errs.push(None);
        }

        for (i, ver) in versions.iter().enumerate() {
            if let Err(e) = self.delete_versions_internal(volume, ver.name.as_str(), &ver.versions).await {
                errs[i] = Some(e);
            } else {
                errs[i] = None;
            }
        }

        Ok(errs)
    }

    #[tracing::instrument(skip(self))]
    async fn read_multiple(&self, req: ReadMultipleReq) -> Result<Vec<ReadMultipleResp>> {
        let mut results = Vec::new();
        let mut found = 0;

        for v in req.files.iter() {
            let fpath = self.get_object_path(&req.bucket, format!("{}/{}", &req.prefix, v).as_str())?;
            let mut res = ReadMultipleResp {
                bucket: req.bucket.clone(),
                prefix: req.prefix.clone(),
                file: v.clone(),
                ..Default::default()
            };

            // if req.metadata_only {}
            match read_file_all(&fpath).await {
                Ok((data, meta)) => {
                    found += 1;

                    if req.max_size > 0 && data.len() > req.max_size {
                        res.exists = true;
                        res.error = format!("max size ({}) exceeded: {}", req.max_size, data.len());
                        results.push(res);
                        break;
                    }

                    res.exists = true;
                    res.data = data.into();
                    res.mod_time = match meta.modified() {
                        Ok(md) => Some(OffsetDateTime::from(md)),
                        Err(_) => {
                            warn!("Not supported modified on this platform");
                            None
                        }
                    };
                    results.push(res);

                    if req.max_results > 0 && found >= req.max_results {
                        break;
                    }
                }
                Err(e) => {
                    if e != DiskError::FileNotFound && e != DiskError::VolumeNotFound {
                        res.exists = true;
                        res.error = e.to_string();
                    }

                    if req.abort404 && !res.exists {
                        results.push(res);
                        break;
                    }

                    results.push(res);
                }
            }
        }

        Ok(results)
    }

    #[tracing::instrument(skip(self))]
    async fn delete_volume(&self, volume: &str) -> Result<()> {
        let p = self.get_bucket_path(volume)?;

        // TODO: 不能用递归删除，如果目录下面有文件，返回 errVolumeNotEmpty

        if let Err(err) = fs::remove_dir_all(&p).await {
            let e: DiskError = to_volume_error(err).into();
            if e != DiskError::VolumeNotFound {
                return Err(e);
            }
        }

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn disk_info(&self, _: &DiskInfoOptions) -> Result<DiskInfo> {
        let mut info = Cache::get(self.disk_info_cache.clone()).await?;
        // TODO: nr_requests, rotational
        info.nr_requests = self.nrrequests;
        info.rotational = self.rotational;
        info.mount_path = self.path().to_str().unwrap().to_string();
        info.endpoint = self.endpoint.to_string();
        info.scanning = self.scanning.load(Ordering::SeqCst) == 1;

        Ok(info)
    }
}

async fn get_disk_info(drive_path: PathBuf) -> Result<(rustfs_utils::os::DiskInfo, bool)> {
    let drive_path = drive_path.to_string_lossy().to_string();
    check_path_length(&drive_path)?;

    let disk_info = get_info(&drive_path)?;
    let root_drive = if !*GLOBAL_IsErasureSD.read().await {
        let root_disk_threshold = *GLOBAL_RootDiskThreshold.read().await;
        if root_disk_threshold > 0 {
            disk_info.total <= root_disk_threshold
        } else {
            is_root_disk(&drive_path, SLASH_SEPARATOR).unwrap_or_default()
        }
    } else {
        false
    };

    Ok((disk_info, root_drive))
}

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    async fn test_skip_access_checks() {
        // let arr = Vec::new();

        let vols = [
            super::super::RUSTFS_META_TMP_DELETED_BUCKET,
            super::super::RUSTFS_META_TMP_BUCKET,
            super::super::RUSTFS_META_MULTIPART_BUCKET,
            RUSTFS_META_BUCKET,
        ];

        let paths: Vec<_> = vols.iter().map(|v| Path::new(v).join("test")).collect();

        for p in paths.iter() {
            assert!(skip_access_checks(p.to_str().unwrap()));
        }
    }

    #[tokio::test]
    async fn test_make_volume() {
        let p = "./testv0";
        fs::create_dir_all(&p).await.unwrap();

        let ep = match Endpoint::try_from(p) {
            Ok(e) => e,
            Err(e) => {
                println!("{e}");
                return;
            }
        };

        let disk = LocalDisk::new(&ep, false).await.unwrap();

        let tmpp = disk
            .resolve_abs_path(Path::new(super::super::RUSTFS_META_TMP_DELETED_BUCKET))
            .unwrap();

        println!("ppp :{:?}", &tmpp);

        let volumes = vec!["a123", "b123", "c123"];

        disk.make_volumes(volumes.clone()).await.unwrap();

        disk.make_volumes(volumes.clone()).await.unwrap();

        let _ = fs::remove_dir_all(&p).await;
    }

    #[tokio::test]
    async fn test_delete_volume() {
        let p = "./testv1";
        fs::create_dir_all(&p).await.unwrap();

        let ep = match Endpoint::try_from(p) {
            Ok(e) => e,
            Err(e) => {
                println!("{e}");
                return;
            }
        };

        let disk = LocalDisk::new(&ep, false).await.unwrap();

        let tmpp = disk
            .resolve_abs_path(Path::new(super::super::RUSTFS_META_TMP_DELETED_BUCKET))
            .unwrap();

        println!("ppp :{:?}", &tmpp);

        let volumes = vec!["a123", "b123", "c123"];

        disk.make_volumes(volumes.clone()).await.unwrap();

        disk.delete_volume("a").await.unwrap();

        let _ = fs::remove_dir_all(&p).await;
    }

    #[tokio::test]
    async fn test_local_disk_basic_operations() {
        let test_dir = "./test_local_disk_basic";
        fs::create_dir_all(&test_dir).await.unwrap();

        let endpoint = Endpoint::try_from(test_dir).unwrap();
        let disk = LocalDisk::new(&endpoint, false).await.unwrap();

        // Test basic properties
        assert!(disk.is_local());
        // Note: host_name() for local disks might be empty or contain localhost/hostname
        // assert!(!disk.host_name().is_empty());
        assert!(!disk.to_string().is_empty());

        // Test path resolution
        let abs_path = disk.resolve_abs_path("test/path").unwrap();
        assert!(abs_path.is_absolute());

        // Test bucket path
        let bucket_path = disk.get_bucket_path("test-bucket").unwrap();
        assert!(bucket_path.to_string_lossy().contains("test-bucket"));

        // Test object path
        let object_path = disk.get_object_path("test-bucket", "test-object").unwrap();
        assert!(object_path.to_string_lossy().contains("test-bucket"));
        assert!(object_path.to_string_lossy().contains("test-object"));

        // 清理测试目录
        let _ = fs::remove_dir_all(&test_dir).await;
    }

    #[tokio::test]
    async fn test_local_disk_file_operations() {
        let test_dir = "./test_local_disk_file_ops";
        fs::create_dir_all(&test_dir).await.unwrap();

        let endpoint = Endpoint::try_from(test_dir).unwrap();
        let disk = LocalDisk::new(&endpoint, false).await.unwrap();

        // Create test volume
        disk.make_volume("test-volume").await.unwrap();

        // Test write and read operations
        let test_data: Vec<u8> = vec![1, 2, 3, 4, 5];
        disk.write_all("test-volume", "test-file.txt", test_data.clone().into())
            .await
            .unwrap();

        let read_data = disk.read_all("test-volume", "test-file.txt").await.unwrap();
        assert_eq!(read_data, test_data);

        // Test file deletion
        let delete_opts = DeleteOptions {
            recursive: false,
            immediate: true,
            undo_write: false,
            old_data_dir: None,
        };
        disk.delete("test-volume", "test-file.txt", delete_opts).await.unwrap();

        // Clean up
        disk.delete_volume("test-volume").await.unwrap();
        let _ = fs::remove_dir_all(&test_dir).await;
    }

    #[tokio::test]
    async fn test_local_disk_volume_operations() {
        let test_dir = "./test_local_disk_volumes";
        fs::create_dir_all(&test_dir).await.unwrap();

        let endpoint = Endpoint::try_from(test_dir).unwrap();
        let disk = LocalDisk::new(&endpoint, false).await.unwrap();

        // Test creating multiple volumes
        let volumes = vec!["vol1", "vol2", "vol3"];
        disk.make_volumes(volumes.clone()).await.unwrap();

        // Test listing volumes
        let volume_list = disk.list_volumes().await.unwrap();
        assert!(!volume_list.is_empty());

        // Test volume stats
        for vol in &volumes {
            let vol_info = disk.stat_volume(vol).await.unwrap();
            assert_eq!(vol_info.name, *vol);
        }

        // Test deleting volumes
        for vol in &volumes {
            disk.delete_volume(vol).await.unwrap();
        }

        // 清理测试目录
        let _ = fs::remove_dir_all(&test_dir).await;
    }

    #[tokio::test]
    async fn test_local_disk_disk_info() {
        let test_dir = "./test_local_disk_info";
        fs::create_dir_all(&test_dir).await.unwrap();

        let endpoint = Endpoint::try_from(test_dir).unwrap();
        let disk = LocalDisk::new(&endpoint, false).await.unwrap();

        let disk_info_opts = DiskInfoOptions {
            disk_id: "test-disk".to_string(),
            metrics: true,
            noop: false,
        };

        let disk_info = disk.disk_info(&disk_info_opts).await.unwrap();

        // Basic checks on disk info
        assert!(!disk_info.fs_type.is_empty());
        assert!(disk_info.total > 0);

        // 清理测试目录
        let _ = fs::remove_dir_all(&test_dir).await;
    }

    #[test]
    fn test_is_valid_volname() {
        // Valid volume names (length >= 3)
        assert!(LocalDisk::is_valid_volname("valid-name"));
        assert!(LocalDisk::is_valid_volname("test123"));
        assert!(LocalDisk::is_valid_volname("my-bucket"));

        // Test minimum length requirement
        assert!(!LocalDisk::is_valid_volname(""));
        assert!(!LocalDisk::is_valid_volname("a"));
        assert!(!LocalDisk::is_valid_volname("ab"));
        assert!(LocalDisk::is_valid_volname("abc"));

        // Note: The current implementation doesn't check for system volume names
        // It only checks length and platform-specific special characters
        // System volume names are valid according to the current implementation
        assert!(LocalDisk::is_valid_volname(RUSTFS_META_BUCKET));
        assert!(LocalDisk::is_valid_volname(super::super::RUSTFS_META_TMP_BUCKET));

        // Testing platform-specific behavior for special characters
        #[cfg(windows)]
        {
            // On Windows systems, these should be invalid
            assert!(!LocalDisk::is_valid_volname("invalid\\name"));
            assert!(!LocalDisk::is_valid_volname("invalid:name"));
            assert!(!LocalDisk::is_valid_volname("invalid|name"));
            assert!(!LocalDisk::is_valid_volname("invalid<name"));
            assert!(!LocalDisk::is_valid_volname("invalid>name"));
            assert!(!LocalDisk::is_valid_volname("invalid?name"));
            assert!(!LocalDisk::is_valid_volname("invalid*name"));
            assert!(!LocalDisk::is_valid_volname("invalid\"name"));
        }

        #[cfg(not(windows))]
        {
            // On non-Windows systems, the current implementation doesn't check special characters
            // So these would be considered valid
            assert!(LocalDisk::is_valid_volname("valid/name"));
            assert!(LocalDisk::is_valid_volname("valid:name"));
        }
    }

    #[tokio::test]
    async fn test_format_info_last_check_valid() {
        let now = OffsetDateTime::now_utc();

        // Valid format info
        let valid_format_info = FormatInfo {
            id: Some(Uuid::new_v4()),
            data: vec![1, 2, 3].into(),
            file_info: Some(fs::metadata("../../../..").await.unwrap()),
            last_check: Some(now),
        };
        assert!(valid_format_info.last_check_valid());

        // Invalid format info (missing id)
        let invalid_format_info = FormatInfo {
            id: None,
            data: vec![1, 2, 3].into(),
            file_info: Some(fs::metadata("../../../..").await.unwrap()),
            last_check: Some(now),
        };
        assert!(!invalid_format_info.last_check_valid());

        // Invalid format info (old timestamp)
        let old_time = OffsetDateTime::now_utc() - time::Duration::seconds(10);
        let old_format_info = FormatInfo {
            id: Some(Uuid::new_v4()),
            data: vec![1, 2, 3].into(),
            file_info: Some(fs::metadata("../../../..").await.unwrap()),
            last_check: Some(old_time),
        };
        assert!(!old_format_info.last_check_valid());
    }

    #[tokio::test]
    async fn test_read_file_exists() {
        let test_file = "./test_read_exists.txt";

        // Test non-existent file
        let (data, metadata) = read_file_exists(test_file).await.unwrap();
        assert!(data.is_empty());
        assert!(metadata.is_none());

        // Create test file
        fs::write(test_file, b"test content").await.unwrap();

        // Test existing file
        let (data, metadata) = read_file_exists(test_file).await.unwrap();
        assert_eq!(data.as_ref(), b"test content");
        assert!(metadata.is_some());

        // Clean up
        let _ = fs::remove_file(test_file).await;
    }

    #[tokio::test]
    async fn test_read_file_all() {
        let test_file = "./test_read_all.txt";
        let test_content = b"test content for read_all";

        // Create test file
        fs::write(test_file, test_content).await.unwrap();

        // Test reading file
        let (data, metadata) = read_file_all(test_file).await.unwrap();
        assert_eq!(data.as_ref(), test_content);
        assert!(metadata.is_file());
        assert_eq!(metadata.len(), test_content.len() as u64);

        // Clean up
        let _ = fs::remove_file(test_file).await;
    }

    #[tokio::test]
    async fn test_read_file_metadata() {
        let test_file = "./test_metadata.txt";

        // Create test file
        fs::write(test_file, b"test").await.unwrap();

        // Test reading metadata
        let metadata = read_file_metadata(test_file).await.unwrap();
        assert!(metadata.is_file());
        assert_eq!(metadata.len(), 4); // "test" is 4 bytes

        // Clean up
        let _ = fs::remove_file(test_file).await;
    }

    #[test]
    fn test_is_root_path() {
        // Unix root path
        assert!(is_root_path("/"));

        // Windows root path (only on Windows)
        #[cfg(windows)]
        assert!(is_root_path("\\"));

        // Non-root paths
        assert!(!is_root_path("/home"));
        assert!(!is_root_path("/tmp"));
        assert!(!is_root_path("relative/path"));

        // On non-Windows systems, backslash is not a root path
        #[cfg(not(windows))]
        assert!(!is_root_path("\\"));
    }
}
