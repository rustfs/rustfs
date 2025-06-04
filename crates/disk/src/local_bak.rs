use super::error::{
    is_err_file_not_found, is_err_file_version_not_found, is_err_os_disk_full, is_sys_err_io, is_sys_err_not_empty,
    is_sys_err_too_many_files, os_is_permission,
};
use super::metacache::MetaCacheEntry;
use super::os::{is_root_disk, rename_all};
use super::utils::{self, read_file_all, read_file_exists};
use super::{endpoint::Endpoint, error::DiskError, format::FormatV3};
use super::{
    os, CheckPartsResp, DeleteOptions, DiskAPI, DiskInfo, DiskInfoOptions, DiskLocation, DiskMetrics, Info, ReadMultipleReq,
    ReadMultipleResp, ReadOptions, RenameDataResp, UpdateMetadataOpts, VolumeInfo, WalkDirOptions, BUCKET_META_PREFIX,
    RUSTFS_META_BUCKET, STORAGE_FORMAT_FILE_BACKUP,
};

use crate::global::{GLOBAL_IsErasureSD, GLOBAL_RootDiskThreshold};
use crate::io::{FileReader, FileWriter};
// use crate::new_object_layer_fn;

use crate::utils::path::{
    self, clean, decode_dir_object, encode_dir_object, has_suffix, path_join, path_join_buf, GLOBAL_DIR_SUFFIX,
    GLOBAL_DIR_SUFFIX_WITH_SLASH, SLASH_SEPARATOR,
};

use path_absolutize::Absolutize;
use rustfs_error::{
    conv_part_err_to_int, to_access_error, to_disk_error, to_file_error, to_unformatted_disk_error, to_volume_error, Error,
    Result, CHECK_PART_FILE_CORRUPT, CHECK_PART_FILE_NOT_FOUND, CHECK_PART_SUCCESS, CHECK_PART_UNKNOWN,
    CHECK_PART_VOLUME_NOT_FOUND,
};
use rustfs_filemeta::{get_file_info, read_xl_meta_no_data, FileInfo, FileInfoOpts, FileInfoVersions, FileMeta, RawFileInfo};
use rustfs_rio::bitrot_verify;
use rustfs_utils::os::get_info;
use rustfs_utils::HashAlgorithm;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::io::SeekFrom;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use std::{
    fs::Metadata,
    path::{Path, PathBuf},
};
use time::OffsetDateTime;
use tokio::fs::{self, File};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWrite, AsyncWriteExt, ErrorKind};
use tokio::sync::mpsc::Sender;
use tokio::sync::RwLock;
use tracing::{error, info, warn};
use uuid::Uuid;

#[derive(Debug)]
pub struct FormatInfo {
    pub id: Option<Uuid>,
    pub data: Vec<u8>,
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

pub struct LocalDisk {
    pub root: PathBuf,
    pub format_path: PathBuf,
    pub format_info: RwLock<FormatInfo>,
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
        let root = fs::canonicalize(ep.get_file_path()).await?;

        if cleanup {
            // TODO: 删除 tmp 数据
        }

        let format_path = Path::new(super::RUSTFS_META_BUCKET)
            .join(Path::new(super::FORMAT_CONFIG_FILE))
            .absolutize_virtually(&root)?
            .into_owned();

        let (format_data, format_meta) = read_file_exists(&format_path).await?;

        let mut id = None;
        // let mut format_legacy = false;
        let mut format_last_check = None;

        if !format_data.is_empty() {
            let s = format_data.as_slice();
            let fm = FormatV3::try_from(s)?;
            let (set_idx, disk_idx) = fm.find_disk_index_by_disk_id(fm.erasure.this)?;

            if set_idx as i32 != ep.set_idx || disk_idx as i32 != ep.disk_idx {
                return Err(Error::InconsistentDisk);
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
                    Err(err) => Err(err),
                }
            })
        });

        let cache = Cache::new(update_fn, Duration::from_secs(1), Opts::default());

        // TODO: DIRECT suport
        // TODD: DiskInfo
        let mut disk = Self {
            root: root.clone(),
            endpoint: ep.clone(),
            format_path,
            format_info: RwLock::new(format_info),
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

        Ok(disk)
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
        let md = std::fs::metadata(&self.format_path).map_err(|e| to_unformatted_disk_error(e))?;
        Ok(md)
    }
    async fn make_meta_volumes(&self) -> Result<()> {
        let buckets = format!("{}/{}", super::RUSTFS_META_BUCKET, super::BUCKET_META_PREFIX);
        let multipart = format!("{}/{}", super::RUSTFS_META_BUCKET, "multipart");
        let config = format!("{}/{}", super::RUSTFS_META_BUCKET, "config");
        let tmp = format!("{}/{}", super::RUSTFS_META_BUCKET, "tmp");
        let defaults = vec![buckets.as_str(), multipart.as_str(), config.as_str(), tmp.as_str()];

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
        if recursive {
            remove_all_std(delete_path).map_err(to_file_error)?;
        } else {
            remove_std(delete_path).map_err(to_file_error)?;
        }

        return Ok(());

        // TODO: 异步通知 检测硬盘空间 清空回收站

        let trash_path = self.get_object_path(super::RUSTFS_META_TMP_DELETED_BUCKET, Uuid::new_v4().to_string().as_str())?;
        if let Some(parent) = trash_path.parent() {
            if !parent.exists() {
                fs::create_dir_all(parent).await?;
            }
        }

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

        if immediate_purge || delete_path.to_string_lossy().ends_with(path::SLASH_SEPARATOR) {
            warn!("move_to_trash immediate_purge {:?}", &delete_path.to_string_lossy());
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
                    remove_all(delete_path).await.map_err(to_file_error)?;
                } else {
                    remove(delete_path).await.map_err(to_file_error)?;
                }
            }

            return Err(err);
        }

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
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
                    ErrorKind::DirectoryNotEmpty => {
                        warn!("delete_file remove_dir {:?} err {}", &delete_path, err.to_string());
                        return Err(Error::FileAccessDenied.into());
                    }
                    _ => (),
                }
            }
            // debug!("delete_file remove_dir done {:?}", &delete_path);
        } else if let Err(err) = fs::remove_file(&delete_path).await {
            // debug!("remove_file err {:?} when {:?}", &err, &delete_path);
            match err.kind() {
                ErrorKind::NotFound => (),
                _ => {
                    warn!("delete_file remove_file {:?}  err {:?}", &delete_path, &err);
                    return Err(Error::FileAccessDenied.into());
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
            return Err(Error::FileNotFound.into());
        }

        let meta_path = file_path.as_ref().join(Path::new(super::STORAGE_FORMAT_FILE));

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
                            if let Err(aerr) = access(volume_dir.as_ref()).await {
                                if aerr.kind() == ErrorKind::NotFound {
                                    warn!("read_metadata_with_dmtime os err {:?}", &aerr);
                                    return Err(Error::VolumeNotFound.into());
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
            return Err(Error::FileNotFound.into());
        }

        Ok((buf, mtime))
    }

    async fn read_metadata(&self, file_path: impl AsRef<Path>) -> Result<Vec<u8>> {
        // TODO: suport timeout
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
            return Err(Error::FileNotFound.into());
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
        // TODO: timeout suport
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
                if e.kind() == ErrorKind::NotFound {
                    if !skip_access_checks(volume) {
                        if let Err(er) = super::fs::access(volume_dir.as_ref()).await {
                            if er.kind() == ErrorKind::NotFound {
                                warn!("read_all_data_with_dmtime os err {:?}", &er);
                                return Err(Error::VolumeNotFound.into());
                            }
                        }
                    }

                    return Err(Error::FileNotFound.into());
                }

                return Err(to_file_error(e).into());
            }
        };

        let meta = f.metadata().await.map_err(to_file_error)?;

        if meta.is_dir() {
            return Err(Error::FileNotFound.into());
        }

        let size = meta.len() as usize;
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(size)?;

        f.read_to_end(&mut bytes).await.map_err(to_file_error)?;

        let modtime = match meta.modified() {
            Ok(md) => Some(OffsetDateTime::from(md)),
            Err(_) => None,
        };

        Ok((bytes, modtime))
    }

    async fn delete_versions_internal(&self, volume: &str, path: &str, fis: &Vec<FileInfo>) -> Result<()> {
        let volume_dir = self.get_bucket_path(volume)?;
        let xlpath = self.get_object_path(volume, format!("{}/{}", path, super::STORAGE_FORMAT_FILE).as_str())?;

        let (data, _) = self.read_all_data_with_dmtime(volume, volume_dir.as_path(), &xlpath).await?;

        let mut fm = FileMeta::default();

        fm.unmarshal_msg(&data)?;

        for fi in fis {
            let data_dir = match fm.delete_version(fi) {
                Ok(res) => res,
                Err(err) => {
                    if !fi.deleted && (err == Error::FileVersionNotFound || err == Error::FileNotFound) {
                        continue;
                    }

                    return Err(err);
                }
            };

            if let Some(dir) = data_dir {
                let vid = fi.version_id.unwrap_or_default();
                let _ = fm.data.remove(vec![vid, dir]);

                let dir_path = self.get_object_path(volume, format!("{}/{}", path, dir).as_str())?;
                if let Err(err) = self.move_to_trash(&dir_path, true, false).await {
                    if !(err == Error::FileNotFound || err == Error::DiskNotFound) {
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

        self.write_all_private(
            volume,
            format!("{}/{}", path, super::STORAGE_FORMAT_FILE).as_str(),
            &buf,
            true,
            volume_dir,
        )
        .await?;

        Ok(())
    }

    async fn write_all_meta(&self, volume: &str, path: &str, buf: &[u8], sync: bool) -> Result<()> {
        let volume_dir = self.get_bucket_path(volume)?;
        let file_path = volume_dir.join(Path::new(&path));
        check_path_length(file_path.to_string_lossy().as_ref())?;

        let tmp_volume_dir = self.get_bucket_path(super::RUSTFS_META_TMP_BUCKET)?;
        let tmp_file_path = tmp_volume_dir.join(Path::new(Uuid::new_v4().to_string().as_str()));

        self.write_all_internal(&tmp_file_path, buf, sync, tmp_volume_dir).await?;

        super::os::rename_all(tmp_file_path, file_path, volume_dir).await?;

        Ok(())
    }

    // write_all_public for trail
    async fn write_all_public(&self, volume: &str, path: &str, data: Vec<u8>) -> Result<()> {
        if volume == super::RUSTFS_META_BUCKET && path == super::FORMAT_CONFIG_FILE {
            let mut format_info = self.format_info.write().await;
            format_info.data.clone_from(&data);
        }

        let volume_dir = self.get_bucket_path(volume)?;

        self.write_all_private(volume, path, &data, true, volume_dir).await?;

        Ok(())
    }

    // write_all_private with check_path_length
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn write_all_private(
        &self,
        volume: &str,
        path: &str,
        buf: &[u8],
        sync: bool,
        skip_parent: impl AsRef<Path>,
    ) -> Result<()> {
        let volume_dir = self.get_bucket_path(volume)?;
        let file_path = volume_dir.join(Path::new(&path));
        check_path_length(file_path.to_string_lossy().as_ref())?;

        self.write_all_internal(file_path, buf, sync, skip_parent)
            .await
            .map_err(to_file_error)?;

        Ok(())
    }
    // write_all_internal do write file
    pub async fn write_all_internal(
        &self,
        file_path: impl AsRef<Path>,
        data: impl AsRef<[u8]>,
        sync: bool,
        skip_parent: impl AsRef<Path>,
    ) -> std::io::Result<()> {
        let flags = super::fs::O_CREATE | super::fs::O_WRONLY | super::fs::O_TRUNC;

        let mut f = {
            if sync {
                // TODO: suport sync
                self.open_file(file_path.as_ref(), flags, skip_parent.as_ref()).await?
            } else {
                self.open_file(file_path.as_ref(), flags, skip_parent.as_ref()).await?
            }
        };

        f.write_all(data.as_ref()).await?;

        Ok(())
    }

    async fn open_file(&self, path: impl AsRef<Path>, mode: usize, skip_parent: impl AsRef<Path>) -> Result<File> {
        let mut skip_parent = skip_parent.as_ref();
        if skip_parent.as_os_str().is_empty() {
            skip_parent = self.root.as_path();
        }

        if let Some(parent) = path.as_ref().parent() {
            os::make_dir_all(parent, skip_parent).await?;
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

        let meta = file.metadata().await?;
        let file_size = meta.len() as usize;

        bitrot_verify(file, file_size, part_size, algo, sum.to_vec(), shard_size)
            .await
            .map_err(to_file_error)?;

        Ok(())
    }

    async fn scan_dir<W: AsyncWrite + Unpin>(
        &self,
        current: &mut String,
        opts: &WalkDirOptions,
        out: &mut MetacacheWriter<W>,
        objs_returned: &mut i32,
    ) -> Result<()> {
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
                if e != Error::VolumeNotFound && e != Error::FileNotFound {
                    info!("scan list_dir {}, err {:?}", &current, &e);
                }

                if opts.report_notfound && (e == Error::VolumeNotFound || e == Error::FileNotFound) && current == &opts.base_dir {
                    return Err(Error::FileNotFound.into());
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
                    name,
                    metadata,
                    ..Default::default()
                })
                .await?;
                *objs_returned += 1;

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
                return Ok(());
            }

            if entry.is_empty() {
                continue;
            }

            let name = path::path_join_buf(&[current, entry]);

            if !dir_stack.is_empty() {
                if let Some(pop) = dir_stack.pop() {
                    if pop < name {
                        //
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

        Ok(())
    }
}

fn is_root_path(path: impl AsRef<Path>) -> bool {
    path.as_ref().components().count() == 1 && path.as_ref().has_root()
}

fn skip_access_checks(p: impl AsRef<str>) -> bool {
    let vols = [
        super::RUSTFS_META_TMP_DELETED_BUCKET,
        super::RUSTFS_META_TMP_BUCKET,
        super::RUSTFS_META_MULTIPART_BUCKET,
        super::RUSTFS_META_BUCKET,
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

        let b = tokio::fs::read(&self.format_path)
            .await
            .map_err(|e| to_unformatted_disk_error(e))?;

        let fm = FormatV3::try_from(b.as_slice()).map_err(|e| {
            warn!("decode format.json  err {:?}", e);
            Error::CorruptedBackend
        })?;

        let (m, n) = fm.find_disk_index_by_disk_id(fm.erasure.this)?;

        let disk_id = fm.erasure.this;

        if m as i32 != self.endpoint.set_idx || n as i32 != self.endpoint.disk_idx {
            return Err(Error::InconsistentDisk.into());
        }

        format_info.id = Some(disk_id);
        format_info.file_info = Some(file_meta);
        format_info.data = b;
        format_info.last_check = Some(OffsetDateTime::now_utc());

        Ok(Some(disk_id))
    }

    #[tracing::instrument(skip(self))]
    async fn set_disk_id(&self, id: Option<Uuid>) -> Result<()> {
        // 本地不需要设置
        // TODO: add check_id_store
        let mut format_info = self.format_info.write().await;
        format_info.id = id;
        Ok(())
    }

    #[must_use]
    #[tracing::instrument(skip(self))]
    async fn read_all(&self, volume: &str, path: &str) -> Result<Vec<u8>> {
        if volume == super::RUSTFS_META_BUCKET && path == super::FORMAT_CONFIG_FILE {
            let format_info = self.format_info.read().await;
            if !format_info.data.is_empty() {
                return Ok(format_info.data.clone());
            }
        }
        // TOFIX:
        let p = self.get_object_path(volume, path)?;
        let data = utils::read_all(&p).await?;

        Ok(data)
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn write_all(&self, volume: &str, path: &str, data: Vec<u8>) -> Result<()> {
        self.write_all_public(volume, path, data).await
    }

    #[tracing::instrument(skip(self))]
    async fn delete(&self, volume: &str, path: &str, opt: DeleteOptions) -> Result<()> {
        let volume_dir = self.get_bucket_path(volume)?;
        if !skip_access_checks(volume) {
            if let Err(e) = super::fs::access(&volume_dir).await {
                return Err(to_access_error(e, Error::VolumeAccessDenied).into());
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
            if let Err(e) = super::fs::access(&volume_dir).await {
                return Err(to_access_error(e, Error::VolumeAccessDenied).into());
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
            let err = (self
                .bitrot_verify(
                    &part_path,
                    erasure.shard_file_size(part.size),
                    checksum_info.algorithm,
                    &checksum_info.hash,
                    erasure.shard_size(),
                )
                .await)
                .err();
            resp.results[i] = conv_part_err_to_int(&err);
            if resp.results[i] == CHECK_PART_UNKNOWN {
                if let Some(err) = err {
                    match err {
                        Error::FileAccessDenied => {}
                        _ => {
                            info!("part unknown, disk: {}, path: {:?}", self.to_string(), part_path);
                        }
                    }
                }
            }
        }

        Ok(resp)
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
                    if (st.len() as usize) < fi.erasure.shard_file_size(part.size) {
                        resp.results[i] = CHECK_PART_FILE_CORRUPT;
                        continue;
                    }

                    resp.results[i] = CHECK_PART_SUCCESS;
                }
                Err(err) => {
                    if err.kind() == ErrorKind::NotFound {
                        if !skip_access_checks(volume) {
                            if let Err(err) = super::fs::access(&volume_dir).await {
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
    async fn rename_part(&self, src_volume: &str, src_path: &str, dst_volume: &str, dst_path: &str, meta: Vec<u8>) -> Result<()> {
        let src_volume_dir = self.get_bucket_path(src_volume)?;
        let dst_volume_dir = self.get_bucket_path(dst_volume)?;
        if !skip_access_checks(src_volume) {
            super::fs::access_std(&src_volume_dir).map_err(to_file_error)?
        }
        if !skip_access_checks(dst_volume) {
            super::fs::access_std(&dst_volume_dir).map_err(to_file_error)?
        }

        let src_is_dir = has_suffix(src_path, SLASH_SEPARATOR);
        let dst_is_dir = has_suffix(dst_path, SLASH_SEPARATOR);

        if !src_is_dir && dst_is_dir || src_is_dir && !dst_is_dir {
            warn!(
                "rename_part src and dst must be both dir or file src_is_dir:{}, dst_is_dir:{}",
                src_is_dir, dst_is_dir
            );
            return Err(Error::FileAccessDenied.into());
        }

        let src_file_path = src_volume_dir.join(Path::new(src_path));
        let dst_file_path = dst_volume_dir.join(Path::new(dst_path));

        // warn!("rename_part src_file_path:{:?}, dst_file_path:{:?}", &src_file_path, &dst_file_path);

        check_path_length(src_file_path.to_string_lossy().as_ref())?;
        check_path_length(dst_file_path.to_string_lossy().as_ref())?;

        if src_is_dir {
            let meta_op = match lstat_std(&src_file_path) {
                Ok(meta) => Some(meta),
                Err(e) => {
                    let err = to_file_error(e).into();

                    if err == Error::FaultyDisk {
                        return Err(err);
                    }

                    if err != Error::FileNotFound {
                        return Err(err);
                    }
                    None
                }
            };

            if let Some(meta) = meta_op {
                if !meta.is_dir() {
                    warn!("rename_part src is not dir {:?}", &src_file_path);
                    return Err(Error::FileAccessDenied.into());
                }
            }

            super::fs::remove_std(&dst_file_path).map_err(to_file_error)?;
        }
        os::rename_all(&src_file_path, &dst_file_path, &dst_volume_dir).await?;

        self.write_all(dst_volume, format!("{}.meta", dst_path).as_str(), meta)
            .await?;

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
            if let Err(e) = super::fs::access(&src_volume_dir).await {
                return Err(to_access_error(e, Error::VolumeAccessDenied).into());
            }
        }
        if !skip_access_checks(dst_volume) {
            if let Err(e) = super::fs::access(&dst_volume_dir).await {
                return Err(to_access_error(e, Error::VolumeAccessDenied).into());
            }
        }

        let src_is_dir = has_suffix(src_path, SLASH_SEPARATOR);
        let dst_is_dir = has_suffix(dst_path, SLASH_SEPARATOR);
        if (dst_is_dir || src_is_dir) && (!dst_is_dir || !src_is_dir) {
            return Err(Error::FileAccessDenied.into());
        }

        let src_file_path = src_volume_dir.join(Path::new(&src_path));
        check_path_length(src_file_path.to_string_lossy().to_string().as_str())?;

        let dst_file_path = dst_volume_dir.join(Path::new(&dst_path));
        check_path_length(dst_file_path.to_string_lossy().to_string().as_str())?;

        if src_is_dir {
            let meta_op = match lstat(&src_file_path).await {
                Ok(meta) => Some(meta),
                Err(e) => {
                    if is_sys_err_io(&e) {
                        return Err(Error::FaultyDisk.into());
                    }

                    if e.kind() != ErrorKind::NotFound {
                        return Err(to_file_error(e).into());
                    }
                    None
                }
            };

            if let Some(meta) = meta_op {
                if !meta.is_dir() {
                    return Err(Error::FileAccessDenied.into());
                }
            }

            super::fs::remove(&dst_file_path).await.map_err(to_file_error)?;
        }

        os::rename_all(&src_file_path, &dst_file_path, &dst_volume_dir).await?;

        if let Some(parent) = src_file_path.parent() {
            let _ = self.delete_file(&src_volume_dir, &parent.to_path_buf(), false, false).await;
        }

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn create_file(&self, origvolume: &str, volume: &str, path: &str, _file_size: usize) -> Result<FileWriter> {
        // warn!("disk create_file: origvolume: {}, volume: {}, path: {}", origvolume, volume, path);

        if !origvolume.is_empty() {
            let origvolume_dir = self.get_bucket_path(origvolume)?;
            if !skip_access_checks(origvolume) {
                if let Err(e) = super::fs::access(&origvolume_dir).await {
                    return Err(to_access_error(e, Error::VolumeAccessDenied).into());
                }
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
        warn!("disk append_file:  volume: {}, path: {}", volume, path);

        let volume_dir = self.get_bucket_path(volume)?;
        if !skip_access_checks(volume) {
            if let Err(e) = super::fs::access(&volume_dir).await {
                return Err(to_access_error(e, Error::VolumeAccessDenied).into());
            }
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
            if let Err(e) = super::fs::access(&volume_dir).await {
                return Err(to_access_error(e, Error::VolumeAccessDenied).into());
            }
        }

        let file_path = volume_dir.join(Path::new(&path));
        check_path_length(file_path.to_string_lossy().to_string().as_str())?;

        let f = self.open_file(file_path, O_RDONLY, volume_dir).await?;

        Ok(Box::new(f))
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn read_file_stream(&self, volume: &str, path: &str, offset: usize, length: usize) -> Result<FileReader> {
        // warn!(
        //     "disk read_file_stream: volume: {}, path: {}, offset: {}, length: {}",
        //     volume, path, offset, length
        // );

        let volume_dir = self.get_bucket_path(volume)?;
        if !skip_access_checks(volume) {
            if let Err(e) = super::fs::access(&volume_dir).await {
                return Err(to_access_error(e, Error::VolumeAccessDenied).into());
            }
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
            return Err(Error::FileCorrupt.into());
        }

        f.seek(SeekFrom::Start(offset as u64)).await?;

        Ok(Box::new(f))
    }
    #[tracing::instrument(level = "debug", skip(self))]
    async fn list_dir(&self, origvolume: &str, volume: &str, dir_path: &str, count: i32) -> Result<Vec<String>> {
        if !origvolume.is_empty() {
            let origvolume_dir = self.get_bucket_path(origvolume)?;
            if !skip_access_checks(origvolume) {
                if let Err(e) = super::fs::access(origvolume_dir).await {
                    return Err(to_access_error(e, Error::VolumeAccessDenied).into());
                }
            }
        }

        let volume_dir = self.get_bucket_path(volume)?;
        let dir_path_abs = volume_dir.join(Path::new(&dir_path.trim_start_matches(SLASH_SEPARATOR)));

        let entries = match os::read_dir(&dir_path_abs, count).await {
            Ok(res) => res,
            Err(e) => {
                if e.kind() == ErrorKind::NotFound && !skip_access_checks(volume) {
                    if let Err(e) = super::fs::access(&volume_dir).await {
                        return Err(to_access_error(e, Error::VolumeAccessDenied).into());
                    }
                }

                return Err(to_volume_error(e).into());
            }
        };

        Ok(entries)
    }

    // FIXME: TODO: io.writer TODO cancel
    #[tracing::instrument(level = "debug", skip(self, wr))]
    async fn walk_dir<W: AsyncWrite + Unpin + Send>(&self, opts: WalkDirOptions, wr: &mut W) -> Result<()> {
        let volume_dir = self.get_bucket_path(&opts.bucket)?;

        if !skip_access_checks(&opts.bucket) {
            if let Err(e) = super::fs::access(&volume_dir).await {
                return Err(to_access_error(e, Error::VolumeAccessDenied).into());
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
            }
        }

        let mut current = opts.base_dir.clone();
        self.scan_dir(&mut current, &opts, &mut out, &mut objs_returned).await?;

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
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
                return Err(to_access_error(e, Error::VolumeAccessDenied).into());
            }
        }

        let dst_volume_dir = self.get_bucket_path(dst_volume)?;
        if !skip_access_checks(dst_volume) {
            if let Err(e) = super::fs::access_std(&dst_volume_dir) {
                info!("access checks failed, dst_volume_dir: {:?}, err: {}", dst_volume_dir, e.to_string());
                return Err(to_access_error(e, Error::VolumeAccessDenied).into());
            }
        }

        // xl.meta 路径
        let src_file_path = src_volume_dir.join(Path::new(format!("{}/{}", &src_path, super::STORAGE_FORMAT_FILE).as_str()));
        let dst_file_path = dst_volume_dir.join(Path::new(format!("{}/{}", &dst_path, super::STORAGE_FORMAT_FILE).as_str()));

        // data_dir 路径
        let has_data_dir_path = {
            let has_data_dir = {
                if !fi.is_remote() {
                    fi.data_dir.map(|dir| super::path::retain_slash(dir.to_string().as_str()))
                } else {
                    None
                }
            };

            if let Some(data_dir) = has_data_dir {
                let src_data_path = src_volume_dir.join(Path::new(
                    super::path::retain_slash(format!("{}/{}", &src_path, data_dir).as_str()).as_str(),
                ));
                let dst_data_path = dst_volume_dir.join(Path::new(
                    super::path::retain_slash(format!("{}/{}", &dst_path, data_dir).as_str()).as_str(),
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
                if e.kind() == ErrorKind::NotADirectory && !cfg!(target_os = "windows") {
                    return Err(Error::FileAccessDenied.into());
                }

                if e.kind() != ErrorKind::NotFound {
                    return Err(to_file_error(e).into());
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

        self.write_all(src_volume, format!("{}/{}", &src_path, super::STORAGE_FORMAT_FILE).as_str(), new_dst_buf)
            .await?;

        if let Some((src_data_path, dst_data_path)) = has_data_dir_path.as_ref() {
            let no_inline = fi.data.is_none() && fi.size > 0;
            if no_inline {
                if let Err(err) = os::rename_all(&src_data_path, &dst_data_path, &skip_parent).await {
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
                        format!("{}/{}/{}", &dst_path, &old_data_dir.to_string(), super::STORAGE_FORMAT_FILE).as_str(),
                        &dst_buf,
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

        if let Err(err) = os::rename_all(&src_file_path, &dst_file_path, &skip_parent).await {
            if let Some((_, dst_data_path)) = has_data_dir_path.as_ref() {
                let _ = self.delete_file(&dst_volume_dir, dst_data_path, false, false).await;
            }
            info!("rename all failed err: {:?}", err);
            return Err(err);
        }

        if let Some(src_file_path_parent) = src_file_path.parent() {
            if src_volume != super::RUSTFS_META_MULTIPART_BUCKET {
                let _ = super::fs::remove_std(src_file_path_parent);
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
                if e != Error::VolumeExists {
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
            return Err(Error::msg("Invalid arguments specified"));
        }

        let volume_dir = self.get_bucket_path(volume)?;

        if let Err(e) = super::fs::access(&volume_dir).await {
            if e.kind() == std::io::ErrorKind::NotFound {
                os::make_dir_all(&volume_dir, self.root.as_path()).await?;
                return Ok(());
            }

            return Err(to_disk_error(e).into());
        }

        Err(Error::VolumeExists)
    }

    #[tracing::instrument(skip(self))]
    async fn list_volumes(&self) -> Result<Vec<VolumeInfo>> {
        let mut volumes = Vec::new();

        let entries = os::read_dir(&self.root, -1)
            .await
            .map_err(|e| to_access_error(e, Error::DiskAccessDenied))?;

        for entry in entries {
            if !super::path::has_suffix(&entry, SLASH_SEPARATOR) || !Self::is_valid_volname(super::path::clean(&entry).as_str()) {
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
        let meta = super::fs::lstat(&volume_dir).await.map_err(to_volume_error)?;

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
            super::fs::access(&volume_dir)
                .await
                .map_err(|e| to_access_error(e, Error::VolumeAccessDenied))?;
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
                .read_all(volume, format!("{}/{}", &path, super::STORAGE_FORMAT_FILE).as_str())
                .await
                .map_err(|e| {
                    if e == Error::FileNotFound && fi.version_id.is_some() {
                        Error::FileVersionNotFound.into()
                    } else {
                        e
                    }
                })?;

            if !FileMeta::is_xl2_v1_format(buf.as_slice()) {
                return Err(Error::FileVersionNotFound.into());
            }

            let mut xl_meta = FileMeta::load(buf.as_slice())?;

            xl_meta.update_object_version(fi)?;

            let wbuf = xl_meta.marshal_msg()?;

            return self
                .write_all_meta(
                    volume,
                    format!("{}/{}", path, super::STORAGE_FORMAT_FILE).as_str(),
                    &wbuf,
                    !opts.no_persistence,
                )
                .await;
        }

        Err(Error::msg("Invalid Argument"))
    }

    #[tracing::instrument(skip(self))]
    async fn write_metadata(&self, _org_volume: &str, volume: &str, path: &str, fi: FileInfo) -> Result<()> {
        let p = self.get_object_path(volume, format!("{}/{}", path, super::STORAGE_FORMAT_FILE).as_str())?;

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

        self.write_all(volume, format!("{}/{}", path, super::STORAGE_FORMAT_FILE).as_str(), fm_data)
            .await?;

        return Ok(());
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
                if err != Error::FileNotFound {
                    return Err(err);
                }

                if fi.deleted && force_del_marker {
                    return self.write_metadata("", volume, path, fi).await;
                }

                if fi.version_id.is_some() {
                    return Err(Error::FileVersionNotFound.into());
                } else {
                    return Err(Error::FileNotFound.into());
                }
            }
        };

        let mut meta = FileMeta::load(&buf)?;
        let old_dir = meta.delete_version(&fi)?;

        if let Some(uuid) = old_dir {
            let vid = fi.version_id.unwrap_or(Uuid::nil());
            let _ = meta.data.remove(vec![vid, uuid])?;

            let old_path = file_path.join(Path::new(uuid.to_string().as_str()));
            check_path_length(old_path.to_string_lossy().as_ref())?;

            if let Err(err) = self.move_to_trash(&old_path, true, false).await {
                if err != Error::FileNotFound {
                    return Err(err);
                }
            }
        }

        if !meta.versions.is_empty() {
            let buf = meta.marshal_msg()?;
            return self
                .write_all_meta(volume, format!("{}{}{}", path, SLASH_SEPARATOR, STORAGE_FORMAT_FILE).as_str(), &buf, true)
                .await;
        }

        // opts.undo_write && opts.old_data_dir.is_some_and(f)
        if let Some(old_data_dir) = opts.old_data_dir {
            if opts.undo_write {
                let src_path = file_path.join(Path::new(
                    format!("{}{}{}", old_data_dir, SLASH_SEPARATOR, STORAGE_FORMAT_FILE_BACKUP).as_str(),
                ));
                let dst_path = file_path.join(Path::new(format!("{}{}{}", path, SLASH_SEPARATOR, STORAGE_FORMAT_FILE).as_str()));
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
                    res.data = data;
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
                    if !(e == Error::FileNotFound || e == Error::VolumeNotFound) {
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
            match err.kind() {
                ErrorKind::NotFound => (),
                // ErrorKind::DirectoryNotEmpty => (),
                kind => {
                    if kind.to_string() == "directory not empty" {
                        return Err(Error::VolumeNotEmpty.into());
                    }

                    return Err(Error::from(err));
                }
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

    // #[tracing::instrument(level = "info", skip_all)]
    // async fn ns_scanner(
    //     &self,
    //     cache: &DataUsageCache,
    //     updates: Sender<DataUsageEntry>,
    //     scan_mode: HealScanMode,
    //     we_sleep: ShouldSleepFn,
    // ) -> Result<DataUsageCache> {
    //     self.scanning.fetch_add(1, Ordering::SeqCst);
    //     defer!(|| { self.scanning.fetch_sub(1, Ordering::SeqCst) });

    //     // must befor metadata_sys
    //     let Some(store) = new_object_layer_fn() else { return Err(Error::msg("errServerNotInitialized")) };

    //     let mut cache = cache.clone();
    //     // Check if the current bucket has a configured lifecycle policy
    //     if let Ok((lc, _)) = metadata_sys::get_lifecycle_config(&cache.info.name).await {
    //         if lc_has_active_rules(&lc, "") {
    //             cache.info.life_cycle = Some(lc);
    //         }
    //     }

    //     // Check if the current bucket has replication configuration
    //     if let Ok((rcfg, _)) = metadata_sys::get_replication_config(&cache.info.name).await {
    //         if rep_has_active_rules(&rcfg, "", true) {
    //             // TODO: globalBucketTargetSys
    //         }
    //     }

    //     let vcfg = (BucketVersioningSys::get(&cache.info.name).await).ok();

    //     let loc = self.get_disk_location();
    //     let disks = store
    //         .get_disks(loc.pool_idx.unwrap(), loc.disk_idx.unwrap())
    //         .await
    //         .map_err(Error::from)?;
    //     let disk = Arc::new(LocalDisk::new(&self.endpoint(), false).await?);
    //     let disk_clone = disk.clone();
    //     cache.info.updates = Some(updates.clone());
    //     let mut data_usage_info = scan_data_folder(
    //         &disks,
    //         disk,
    //         &cache,
    //         Box::new(move |item: &ScannerItem| {
    //             let mut item = item.clone();
    //             let disk = disk_clone.clone();
    //             let vcfg = vcfg.clone();
    //             Box::pin(async move {
    //                 if !item.path.ends_with(&format!("{}{}", SLASH_SEPARATOR, STORAGE_FORMAT_FILE)) {
    //                     return Err(Error::ScanSkipFile.into());
    //                 }
    //                 let stop_fn = ScannerMetrics::log(ScannerMetric::ScanObject);
    //                 let mut res = HashMap::new();
    //                 let done_sz = ScannerMetrics::time_size(ScannerMetric::ReadMetadata).await;
    //                 let buf = match disk.read_metadata(item.path.clone()).await {
    //                     Ok(buf) => buf,
    //                     Err(err) => {
    //                         res.insert("err".to_string(), err.to_string());
    //                         stop_fn(&res).await;
    //                         return Err(Error::ScanSkipFile.into());
    //                     }
    //                 };
    //                 done_sz(buf.len() as u64).await;
    //                 res.insert("metasize".to_string(), buf.len().to_string());
    //                 item.transform_meda_dir();
    //                 let meta_cache = MetaCacheEntry {
    //                     name: item.object_path().to_string_lossy().to_string(),
    //                     metadata: buf,
    //                     ..Default::default()
    //                 };
    //                 let fivs = match meta_cache.file_info_versions(&item.bucket) {
    //                     Ok(fivs) => fivs,
    //                     Err(err) => {
    //                         res.insert("err".to_string(), err.to_string());
    //                         stop_fn(&res).await;
    //                         return Err(Error::ScanSkipFile.into());
    //                     }
    //                 };
    //                 let mut size_s = SizeSummary::default();
    //                 let done = ScannerMetrics::time(ScannerMetric::ApplyAll);
    //                 let obj_infos = match item.apply_versions_actions(&fivs.versions).await {
    //                     Ok(obj_infos) => obj_infos,
    //                     Err(err) => {
    //                         res.insert("err".to_string(), err.to_string());
    //                         stop_fn(&res).await;
    //                         return Err(Error::ScanSkipFile.into());
    //                     }
    //                 };

    //                 let versioned = if let Some(vcfg) = vcfg.as_ref() {
    //                     vcfg.versioned(item.object_path().to_str().unwrap_or_default())
    //                 } else {
    //                     false
    //                 };

    //                 let mut obj_deleted = false;
    //                 for info in obj_infos.iter() {
    //                     let done = ScannerMetrics::time(ScannerMetric::ApplyVersion);
    //                     let sz: usize;
    //                     (obj_deleted, sz) = item.apply_actions(info, &size_s).await;
    //                     done().await;

    //                     if obj_deleted {
    //                         break;
    //                     }

    //                     let actual_sz = match info.get_actual_size() {
    //                         Ok(size) => size,
    //                         Err(_) => continue,
    //                     };

    //                     if info.delete_marker {
    //                         size_s.delete_markers += 1;
    //                     }

    //                     if info.version_id.is_some() && sz == actual_sz {
    //                         size_s.versions += 1;
    //                     }

    //                     size_s.total_size += sz;

    //                     if info.delete_marker {
    //                         continue;
    //                     }
    //                 }

    //                 for frer_version in fivs.free_versions.iter() {
    //                     let _obj_info = ObjectInfo::from_file_info(
    //                         frer_version,
    //                         &item.bucket,
    //                         &item.object_path().to_string_lossy(),
    //                         versioned,
    //                     );
    //                     let done = ScannerMetrics::time(ScannerMetric::TierObjSweep);
    //                     done().await;
    //                 }

    //                 // todo: global trace
    //                 if obj_deleted {
    //                     return Err(Error::ScanIgnoreFileContrib.into());
    //                 }
    //                 done().await;
    //                 Ok(size_s)
    //             })
    //         }),
    //         scan_mode,
    //         we_sleep,
    //     )
    //     .await
    //     .map_err(|e| Error::from(e.to_string()))?; // TODO: Error::from(e.to_string())
    //     data_usage_info.info.last_update = Some(SystemTime::now());
    //     info!("ns_scanner completed: {data_usage_info:?}");
    //     Ok(data_usage_info)
    // }

    // #[tracing::instrument(skip(self))]
    // async fn healing(&self) -> Option<HealingTracker> {
    //     let healing_file = path_join(&[
    //         self.path(),
    //         PathBuf::from(RUSTFS_META_BUCKET),
    //         PathBuf::from(BUCKET_META_PREFIX),
    //         PathBuf::from(HEALING_TRACKER_FILENAME),
    //     ]);
    //     let b = match fs::read(healing_file).await {
    //         Ok(b) => b,
    //         Err(_) => return None,
    //     };
    //     if b.is_empty() {
    //         return None;
    //     }
    //     match HealingTracker::unmarshal_msg(&b) {
    //         Ok(h) => Some(h),
    //         Err(_) => Some(HealingTracker::default()),
    //     }
    // }
}

async fn get_disk_info(drive_path: PathBuf) -> Result<(Info, bool)> {
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
            super::super::RUSTFS_META_BUCKET,
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
}
