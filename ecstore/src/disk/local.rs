use super::error::{
    is_err_file_not_found, is_sys_err_io, is_sys_err_not_empty, is_sys_err_too_many_files, os_is_not_exist, os_is_permission,
};
use super::os::is_root_disk;
use super::{endpoint::Endpoint, error::DiskError, format::FormatV3};
use super::{
    os, CheckPartsResp, DeleteOptions, DiskAPI, DiskInfo, DiskInfoOptions, DiskLocation, DiskMetrics, FileInfoVersions,
    FileReader, FileWriter, Info, MetaCacheEntry, ReadMultipleReq, ReadMultipleResp, ReadOptions, RenameDataResp,
    UpdateMetadataOpts, VolumeInfo, WalkDirOptions,
};
use crate::bitrot::bitrot_verify;
use crate::cache_value::cache::{Cache, Opts};
use crate::disk::error::{
    convert_access_error, is_sys_err_handle_invalid, is_sys_err_invalid_arg, is_sys_err_is_dir, is_sys_err_not_dir,
    map_err_not_exists, os_err_to_file_err,
};
use crate::disk::os::check_path_length;
use crate::disk::{LocalFileReader, LocalFileWriter, STORAGE_FORMAT_FILE};
use crate::error::{Error, Result};
use crate::global::{GLOBAL_IsErasureSD, GLOBAL_RootDiskThreshold};
use crate::set_disk::{
    conv_part_err_to_int, CHECK_PART_FILE_CORRUPT, CHECK_PART_FILE_NOT_FOUND, CHECK_PART_SUCCESS, CHECK_PART_UNKNOWN,
    CHECK_PART_VOLUME_NOT_FOUND,
};
use crate::store_api::BitrotAlgorithm;
use crate::utils::fs::{access, lstat, O_APPEND, O_CREATE, O_RDONLY, O_WRONLY};
use crate::utils::os::get_info;
use crate::utils::path::{clean, has_suffix, GLOBAL_DIR_SUFFIX_WITH_SLASH, SLASH_SEPARATOR};
use crate::{
    file_meta::FileMeta,
    store_api::{FileInfo, RawFileInfo},
    utils,
};
use path_absolutize::Absolutize;
use std::collections::HashSet;
use std::fmt::Debug;
use std::io::Cursor;
use std::os::unix::fs::MetadataExt;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;
use std::{
    fs::Metadata,
    path::{Path, PathBuf},
};
use time::OffsetDateTime;
use tokio::fs::{self, File};
use tokio::io::{AsyncReadExt, AsyncWriteExt, ErrorKind};
use tokio::runtime::Runtime;
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
        let root = fs::canonicalize(ep.url.path()).await?;

        if cleanup {
            // TODO: 删除tmp数据
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

            if Some(set_idx) != ep.set_idx || Some(disk_idx) != ep.disk_idx {
                return Err(Error::from(DiskError::InconsistentDisk));
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
        let disk_id = Arc::new(id.map_or("".to_string(), |id| id.to_string()));
        let update_fn = move || {
            let rt = Runtime::new().unwrap();
            rt.block_on(async {
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
        };

        let cache = Cache::new(Box::new(update_fn), Duration::from_secs(1), Opts::default());

        // TODO: DIRECT suport
        // TODD: DiskInfo
        let mut disk = Self {
            root: root_clone.clone(),
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
        let (info, _root) = get_disk_info(root_clone).await?;
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

    async fn check_format_json(&self) -> Result<Metadata> {
        let md = fs::metadata(&self.format_path).await.map_err(|e| match e.kind() {
            ErrorKind::NotFound => DiskError::DiskNotFound,
            ErrorKind::PermissionDenied => DiskError::FileAccessDenied,
            _ => {
                warn!("check_format_json err {:?}", e);
                DiskError::CorruptedBackend
            }
        })?;
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

    pub async fn move_to_trash(&self, delete_path: &PathBuf, _recursive: bool, _immediate_purge: bool) -> Result<()> {
        let trash_path = self.get_object_path(super::RUSTFS_META_TMP_DELETED_BUCKET, Uuid::new_v4().to_string().as_str())?;
        if let Some(parent) = trash_path.parent() {
            if !parent.exists() {
                fs::create_dir_all(parent).await?;
            }
        }
        // debug!("move_to_trash from:{:?} to {:?}", &delete_path, &trash_path);
        // TODO: 清空回收站
        if let Err(err) = fs::rename(&delete_path, &trash_path).await {
            match err.kind() {
                ErrorKind::NotFound => (),
                _ => {
                    warn!("delete_file rename {:?} err {:?}", &delete_path, &err);
                    return Err(Error::from(err));
                }
            }
        }

        // FIXME: 先清空回收站吧，有时间再添加判断逻辑

        if let Err(err) = {
            if trash_path.is_dir() {
                fs::remove_dir_all(&trash_path).await
            } else {
                fs::remove_file(&trash_path).await
            }
        } {
            match err.kind() {
                ErrorKind::NotFound => (),
                _ => {
                    warn!("delete_file remove trash {:?} err {:?}", &trash_path, &err);
                    return Err(Error::from(err));
                }
            }
        }

        // TODO: immediate
        Ok(())
    }

    // #[tracing::instrument(skip(self))]
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
                    // ErrorKind::DirectoryNotEmpty => (),
                    kind => {
                        if kind.to_string() != "directory not empty" {
                            warn!("delete_file remove_dir {:?} err {}", &delete_path, kind.to_string());
                            return Err(Error::from(err));
                        }
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
                    return Err(Error::from(err));
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
    async fn read_raw(
        &self,
        bucket: &str,
        volume_dir: impl AsRef<Path>,
        path: impl AsRef<Path>,
        read_data: bool,
    ) -> Result<(Vec<u8>, Option<OffsetDateTime>)> {
        let meta_path = path.as_ref().join(Path::new(super::STORAGE_FORMAT_FILE));
        if read_data {
            self.read_all_data_with_dmtime(bucket, volume_dir, meta_path).await
        } else {
            self.read_all_data_with_dmtime(bucket, volume_dir, meta_path).await
            // FIXME: read_metadata only suport
            // self.read_metadata_with_dmtime(meta_path).await
        }
    }

    async fn read_metadata(&self, file_path: impl AsRef<Path>) -> Result<Vec<u8>> {
        // TODO: suport timeout
        let (data, _) = self.read_metadata_with_dmtime(file_path.as_ref()).await?;
        Ok(data)
    }

    // FIXME: read_metadata only suport
    async fn read_metadata_with_dmtime(&self, file_path: impl AsRef<Path>) -> Result<(Vec<u8>, Option<OffsetDateTime>)> {
        check_path_length(file_path.as_ref().to_string_lossy().as_ref())?;

        let mut f = utils::fs::open_file(file_path.as_ref(), O_RDONLY).await?;

        let meta = f.metadata().await?;

        if meta.is_dir() {
            return Err(Error::new(DiskError::FileNotFound));
        }

        let meta = f.metadata().await.map_err(os_err_to_file_err)?;

        if meta.is_dir() {
            return Err(Error::new(DiskError::FileNotFound));
        }

        let size = meta.len() as usize;
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(size)?;

        f.read_to_end(&mut bytes).await.map_err(os_err_to_file_err)?;

        let modtime = match meta.modified() {
            Ok(md) => Some(OffsetDateTime::from(md)),
            Err(_) => None,
        };

        Ok((bytes, modtime))
    }

    async fn read_all_data(&self, volume: &str, volume_dir: impl AsRef<Path>, file_path: impl AsRef<Path>) -> Result<Vec<u8>> {
        // TODO: timeout suport
        let (data, _) = self.read_all_data_with_dmtime(volume, volume_dir, file_path).await?;
        Ok(data)
    }

    async fn read_all_data_with_dmtime(
        &self,
        volume: &str,
        volume_dir: impl AsRef<Path>,
        file_path: impl AsRef<Path>,
    ) -> Result<(Vec<u8>, Option<OffsetDateTime>)> {
        let mut f = match utils::fs::open_file(file_path.as_ref(), utils::fs::O_RDONLY).await {
            Ok(f) => f,
            Err(e) => {
                if os_is_not_exist(&e) {
                    if !skip_access_checks(volume) {
                        if let Err(er) = utils::fs::access(volume_dir.as_ref()).await {
                            if os_is_not_exist(&er) {
                                return Err(Error::new(DiskError::VolumeNotFound));
                            }
                        }
                    }

                    return Err(Error::new(DiskError::FileNotFound));
                } else if os_is_permission(&e) {
                    return Err(Error::new(DiskError::FileAccessDenied));
                } else if is_sys_err_not_dir(&e) || is_sys_err_is_dir(&e) || is_sys_err_handle_invalid(&e) {
                    return Err(Error::new(DiskError::FileNotFound));
                } else if is_sys_err_io(&e) {
                    return Err(Error::new(DiskError::FaultyDisk));
                } else if is_sys_err_too_many_files(&e) {
                    return Err(Error::new(DiskError::TooManyOpenFiles));
                } else if is_sys_err_invalid_arg(&e) {
                    if let Ok(meta) = utils::fs::lstat(file_path.as_ref()).await {
                        if meta.is_dir() {
                            return Err(Error::new(DiskError::FileNotFound));
                        }
                    }
                    return Err(Error::new(DiskError::UnsupportedDisk));
                }

                return Err(Error::new(e));
            }
        };

        let meta = f.metadata().await.map_err(os_err_to_file_err)?;

        if meta.is_dir() {
            return Err(Error::new(DiskError::FileNotFound));
        }

        let size = meta.len() as usize;
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(size)?;

        f.read_to_end(&mut bytes).await.map_err(os_err_to_file_err)?;

        let modtime = match meta.modified() {
            Ok(md) => Some(OffsetDateTime::from(md)),
            Err(_) => None,
        };

        Ok((bytes, modtime))
    }

    async fn delete_versions_internal(&self, volume: &str, path: &str, fis: &Vec<FileInfo>) -> Result<()> {
        let volume_dir = self.get_bucket_path(volume)?;
        let xlpath = self.get_object_path(volume, format!("{}/{}", path, super::STORAGE_FORMAT_FILE).as_str())?;

        let data = self
            .read_all_data(volume, volume_dir.as_path(), &xlpath)
            .await
            .unwrap_or_default();

        if data.is_empty() {
            return Err(Error::new(DiskError::FileNotFound));
        }

        let mut fm = FileMeta::default();

        fm.unmarshal_msg(&data)?;

        for fi in fis {
            let data_dir = fm.delete_version(fi)?;
            warn!("删除版本号 对应data_dir {:?}", &data_dir);
            if data_dir.is_some() {
                let dir_path = self.get_object_path(volume, format!("{}/{}", path, data_dir.unwrap()).as_str())?;
                self.move_to_trash(&dir_path, true, false).await?;
            }
        }

        // 没有版本了，删除xl.meta
        if fm.versions.is_empty() {
            warn!("没有版本了，删除xl.meta");

            self.delete_file(&volume_dir, &xlpath, true, false).await?;
            return Ok(());
        }

        // 更新xl.meta
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

        os::rename_all(tmp_file_path, file_path, volume_dir).await
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

        self.write_all_internal(file_path, buf, sync, skip_parent).await
    }
    // write_all_internal do write file
    pub async fn write_all_internal(
        &self,
        file_path: impl AsRef<Path>,
        data: impl AsRef<[u8]>,
        sync: bool,
        skip_parent: impl AsRef<Path>,
    ) -> Result<()> {
        let flags = utils::fs::O_CREATE | utils::fs::O_WRONLY | utils::fs::O_TRUNC;

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

        let f = utils::fs::open_file(path.as_ref(), mode).await.map_err(|e| {
            if is_sys_err_io(&e) {
                Error::new(DiskError::IsNotRegular)
            } else if os_is_permission(&e) || is_sys_err_not_dir(&e) {
                Error::new(DiskError::FileAccessDenied)
            } else if is_sys_err_io(&e) {
                Error::new(DiskError::FaultyDisk)
            } else if is_sys_err_too_many_files(&e) {
                Error::new(DiskError::TooManyOpenFiles)
            } else {
                Error::new(e)
            }
        })?;

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
        algo: BitrotAlgorithm,
        sum: &[u8],
        shard_size: usize,
    ) -> Result<()> {
        let mut file = utils::fs::open_file(part_path, O_CREATE | O_WRONLY)
            .await
            .map_err(os_err_to_file_err)?;

        let mut data = Vec::new();
        let n = file.read_to_end(&mut data).await?;
        bitrot_verify(&mut Cursor::new(data), n, part_size, algo, sum.to_vec(), shard_size)
    }
}

fn is_root_path(path: impl AsRef<Path>) -> bool {
    path.as_ref().components().count() == 1 && path.as_ref().has_root()
}

// 过滤 std::io::ErrorKind::NotFound
pub async fn read_file_exists(path: impl AsRef<Path>) -> Result<(Vec<u8>, Option<Metadata>)> {
    let p = path.as_ref();
    let (data, meta) = match read_file_all(&p).await {
        Ok((data, meta)) => (data, Some(meta)),
        Err(e) => {
            if is_err_file_not_found(&e) {
                (Vec::new(), None)
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

pub async fn read_file_all(path: impl AsRef<Path>) -> Result<(Vec<u8>, Metadata)> {
    let p = path.as_ref();
    let meta = read_file_metadata(&path).await?;

    let data = fs::read(&p).await?;

    Ok((data, meta))
}

pub async fn read_file_metadata(p: impl AsRef<Path>) -> Result<Metadata> {
    let meta = fs::metadata(&p).await.map_err(|e| match e.kind() {
        ErrorKind::NotFound => Error::from(DiskError::FileNotFound),
        ErrorKind::PermissionDenied => Error::from(DiskError::FileAccessDenied),
        _ => Error::from(e),
    })?;

    Ok(meta)
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
    fn to_string(&self) -> String {
        self.root.to_string_lossy().to_string()
    }
    fn is_local(&self) -> bool {
        true
    }
    fn host_name(&self) -> String {
        self.endpoint.host_port()
    }
    async fn is_online(&self) -> bool {
        true
    }

    fn endpoint(&self) -> Endpoint {
        self.endpoint.clone()
    }

    async fn close(&self) -> Result<()> {
        Ok(())
    }
    fn path(&self) -> PathBuf {
        self.root.clone()
    }

    fn get_disk_location(&self) -> DiskLocation {
        DiskLocation {
            pool_idx: self.endpoint.pool_idx,
            set_idx: self.endpoint.set_idx,
            disk_idx: self.endpoint.pool_idx,
        }
    }

    async fn get_disk_id(&self) -> Result<Option<Uuid>> {
        let mut format_info = self.format_info.write().await;

        let id = format_info.id;

        if format_info.last_check_valid() {
            return Ok(id);
        }

        let file_meta = self.check_format_json().await?;

        if let Some(file_info) = &format_info.file_info {
            if utils::fs::same_file(&file_meta, file_info) {
                format_info.last_check = Some(OffsetDateTime::now_utc());

                return Ok(id);
            }
        }

        let b = fs::read(&self.format_path).await.map_err(|e| match e.kind() {
            ErrorKind::NotFound => DiskError::DiskNotFound,
            ErrorKind::PermissionDenied => DiskError::FileAccessDenied,
            _ => {
                warn!("check_format_json err {:?}", e);
                DiskError::CorruptedBackend
            }
        })?;

        let fm = FormatV3::try_from(b.as_slice()).map_err(|e| {
            warn!("decode format.json  err {:?}", e);
            DiskError::CorruptedBackend
        })?;

        let (m, n) = fm.find_disk_index_by_disk_id(fm.erasure.this)?;

        let disk_id = fm.erasure.this;

        match (self.endpoint.set_idx, self.endpoint.disk_idx) {
            (Some(set_idx), Some(disk_idx)) => {
                if m != set_idx || n != disk_idx {
                    return Err(Error::new(DiskError::InconsistentDisk));
                }
            }
            _ => return Err(Error::new(DiskError::InconsistentDisk)),
        }

        format_info.id = Some(disk_id);
        format_info.file_info = Some(file_meta);
        format_info.data = b;
        format_info.last_check = Some(OffsetDateTime::now_utc());

        Ok(Some(disk_id))
    }

    async fn set_disk_id(&self, id: Option<Uuid>) -> Result<()> {
        // 本地不需要设置
        // TODO: add check_id_store
        let mut format_info = self.format_info.write().await;
        format_info.id = id;
        Ok(())
    }

    #[must_use]
    async fn read_all(&self, volume: &str, path: &str) -> Result<Vec<u8>> {
        if volume == super::RUSTFS_META_BUCKET && path == super::FORMAT_CONFIG_FILE {
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

    async fn write_all(&self, volume: &str, path: &str, data: Vec<u8>) -> Result<()> {
        self.write_all_public(volume, path, data).await
    }

    async fn delete(&self, volume: &str, path: &str, opt: DeleteOptions) -> Result<()> {
        let volume_dir = self.get_bucket_path(volume)?;
        if !skip_access_checks(volume) {
            if let Err(e) = utils::fs::access(&volume_dir).await {
                return Err(convert_access_error(e, DiskError::VolumeAccessDenied));
            }
        }

        let file_path = volume_dir.join(Path::new(&path));
        check_path_length(file_path.to_string_lossy().to_string().as_str())?;

        self.delete_file(&volume_dir, &file_path, opt.recursive, opt.immediate)
            .await?;

        Ok(())
    }

    async fn verify_file(&self, volume: &str, path: &str, fi: &FileInfo) -> Result<CheckPartsResp> {
        let volume_dir = self.get_bucket_path(volume)?;
        if !skip_access_checks(volume) {
            if let Err(e) = utils::fs::access(&volume_dir).await {
                return Err(convert_access_error(e, DiskError::VolumeAccessDenied));
            }
        }

        let mut resp = CheckPartsResp {
            results: Vec::with_capacity(fi.parts.len()),
        };

        let erasure = &fi.erasure;
        for (i, part) in fi.parts.iter().enumerate() {
            let checksum_info = erasure.get_checksum_info(part.number);
            let part_path = Path::new(&volume_dir)
                .join(path)
                .join(fi.data_dir.map_or("".to_string(), |dir| dir.to_string()))
                .join(format!("part.{}", part.number));
            let err = match self
                .bitrot_verify(
                    &part_path,
                    erasure.shard_file_size(part.size),
                    checksum_info.algorithm,
                    &checksum_info.hash,
                    erasure.shard_size(erasure.block_size),
                )
                .await
            {
                Ok(_) => None,
                Err(err) => Some(err),
            };
            resp.results[i] = conv_part_err_to_int(&err);
            if resp.results[i] == CHECK_PART_UNKNOWN {
                if let Some(err) = err {
                    match err.downcast_ref::<DiskError>() {
                        Some(DiskError::FileAccessDenied) => {}
                        _ => {
                            info!("part unknown, disk: {}, path: {:?}", self.to_string(), part_path);
                        }
                    }
                }
            }
        }

        Ok(resp)
    }

    async fn check_parts(&self, volume: &str, path: &str, fi: &FileInfo) -> Result<CheckPartsResp> {
        let volume_dir = self.get_bucket_path(&volume)?;
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
                    if (st.size() as usize) < fi.erasure.shard_file_size(part.size) {
                        resp.results[i] = CHECK_PART_FILE_CORRUPT;
                        continue;
                    }

                    resp.results[i] = CHECK_PART_SUCCESS;
                }
                Err(err) => {
                    match os_err_to_file_err(err).downcast_ref() {
                        Some(DiskError::FileNotFound) => {
                            if !skip_access_checks(volume) {
                                if let Err(err) = access(&volume_dir).await {
                                    match err.kind() {
                                        ErrorKind::NotFound => {
                                            resp.results[i] = CHECK_PART_VOLUME_NOT_FOUND;
                                            continue;
                                        }
                                        _ => {}
                                    }
                                }
                            }
                            resp.results[i] = CHECK_PART_FILE_NOT_FOUND;
                        }
                        _ => {}
                    }
                    continue;
                }
            }
        }

        Ok(resp)
    }

    async fn rename_part(&self, src_volume: &str, src_path: &str, dst_volume: &str, dst_path: &str, meta: Vec<u8>) -> Result<()> {
        let src_volume_dir = self.get_bucket_path(src_volume)?;
        let dst_volume_dir = self.get_bucket_path(dst_volume)?;
        if !skip_access_checks(src_volume) {
            utils::fs::access(&src_volume_dir).await.map_err(map_err_not_exists)?
        }
        if !skip_access_checks(dst_volume) {
            utils::fs::access(&dst_volume_dir).await.map_err(map_err_not_exists)?
        }

        let src_is_dir = has_suffix(src_path, SLASH_SEPARATOR);
        let dst_is_dir = has_suffix(dst_path, SLASH_SEPARATOR);

        if !src_is_dir && dst_is_dir || src_is_dir && !dst_is_dir {
            return Err(Error::from(DiskError::FileAccessDenied));
        }

        let src_file_path = src_volume_dir.join(Path::new(src_path));
        let dst_file_path = dst_volume_dir.join(Path::new(dst_path));

        check_path_length(src_file_path.to_string_lossy().as_ref())?;
        check_path_length(dst_file_path.to_string_lossy().as_ref())?;

        if src_is_dir {
            let meta_op = match lstat(&src_file_path).await {
                Ok(meta) => Some(meta),
                Err(e) => {
                    if is_sys_err_io(&e) {
                        return Err(Error::new(DiskError::FaultyDisk));
                    }

                    if !os_is_not_exist(&e) {
                        return Err(Error::new(e));
                    }
                    None
                }
            };

            if let Some(meta) = meta_op {
                if !meta.is_dir() {
                    return Err(Error::new(DiskError::FileAccessDenied));
                }
            }

            if let Err(e) = utils::fs::remove(&dst_file_path).await {
                if is_sys_err_not_empty(&e) || is_sys_err_not_dir(&e) {
                    return Err(Error::new(DiskError::FileAccessDenied));
                } else if is_sys_err_io(&e) {
                    return Err(Error::new(DiskError::FaultyDisk));
                }

                return Err(Error::new(e));
            }
        }

        if let Err(err) = os::rename_all(&src_file_path, &dst_file_path, &dst_volume_dir).await {
            if let Some(e) = err.to_io_err() {
                if is_sys_err_not_empty(&e) || is_sys_err_not_dir(&e) {
                    return Err(Error::new(DiskError::FileAccessDenied));
                }

                return Err(os_err_to_file_err(e));
            }

            return Err(err);
        }

        if let Err(err) = self.write_all(dst_volume, format!("{}.meta", dst_path).as_str(), meta).await {
            if let Some(e) = err.to_io_err() {
                return Err(os_err_to_file_err(e));
            }

            return Err(err);
        }

        if let Some(parent) = src_file_path.parent() {
            self.delete_file(&src_volume_dir, &parent.to_path_buf(), false, false).await?;
        }

        Ok(())
    }
    async fn rename_file(&self, src_volume: &str, src_path: &str, dst_volume: &str, dst_path: &str) -> Result<()> {
        let src_volume_dir = self.get_bucket_path(src_volume)?;
        let dst_volume_dir = self.get_bucket_path(dst_volume)?;
        if !skip_access_checks(src_volume) {
            if let Err(e) = utils::fs::access(&src_volume_dir).await {
                if os_is_not_exist(&e) {
                    return Err(Error::from(DiskError::VolumeNotFound));
                } else if is_sys_err_io(&e) {
                    return Err(Error::from(DiskError::FaultyDisk));
                }

                return Err(Error::new(e));
            }
        }
        if !skip_access_checks(dst_volume) {
            if let Err(e) = utils::fs::access(&dst_volume_dir).await {
                if os_is_not_exist(&e) {
                    return Err(Error::from(DiskError::VolumeNotFound));
                } else if is_sys_err_io(&e) {
                    return Err(Error::from(DiskError::FaultyDisk));
                }

                return Err(Error::new(e));
            }
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
                    if is_sys_err_io(&e) {
                        return Err(Error::new(DiskError::FaultyDisk));
                    }

                    if !os_is_not_exist(&e) {
                        return Err(Error::new(e));
                    }
                    None
                }
            };

            if let Some(meta) = meta_op {
                if !meta.is_dir() {
                    return Err(Error::new(DiskError::FileAccessDenied));
                }
            }

            if let Err(e) = utils::fs::remove(&dst_file_path).await {
                if is_sys_err_not_empty(&e) || is_sys_err_not_dir(&e) {
                    return Err(Error::new(DiskError::FileAccessDenied));
                } else if is_sys_err_io(&e) {
                    return Err(Error::new(DiskError::FaultyDisk));
                }

                return Err(Error::new(e));
            }
        }

        if let Err(err) = os::rename_all(&src_file_path, &dst_file_path, &dst_volume_dir).await {
            if let Some(e) = err.to_io_err() {
                if is_sys_err_not_empty(&e) || is_sys_err_not_dir(&e) {
                    return Err(Error::new(DiskError::FileAccessDenied));
                }

                return Err(os_err_to_file_err(e));
            }

            return Err(err);
        }

        if let Some(parent) = src_file_path.parent() {
            let _ = self.delete_file(&src_volume_dir, &parent.to_path_buf(), false, false).await;
        }

        Ok(())
    }

    // TODO: use io.reader
    async fn create_file(&self, origvolume: &str, volume: &str, path: &str, _file_size: usize) -> Result<FileWriter> {
        if !origvolume.is_empty() {
            let origvolume_dir = self.get_bucket_path(origvolume)?;
            if !skip_access_checks(origvolume) {
                if let Err(e) = utils::fs::access(origvolume_dir).await {
                    return Err(convert_access_error(e, DiskError::VolumeAccessDenied));
                }
            }
        }

        let volume_dir = self.get_bucket_path(volume)?;
        let file_path = volume_dir.join(Path::new(&path));
        check_path_length(file_path.to_string_lossy().to_string().as_str())?;

        //  TODO: writeAllDirect io.copy
        if let Some(parent) = file_path.parent() {
            os::make_dir_all(parent, &volume_dir).await?;
        }
        let f = utils::fs::open_file(&file_path, O_CREATE | O_WRONLY)
            .await
            .map_err(os_err_to_file_err)?;

        Ok(FileWriter::Local(LocalFileWriter::new(f)))

        // Ok(())
    }
    // async fn append_file(&self, volume: &str, path: &str, mut r: DuplexStream) -> Result<File> {
    async fn append_file(&self, volume: &str, path: &str) -> Result<FileWriter> {
        let volume_dir = self.get_bucket_path(volume)?;
        if !skip_access_checks(volume) {
            if let Err(e) = utils::fs::access(&volume_dir).await {
                return Err(convert_access_error(e, DiskError::VolumeAccessDenied));
            }
        }

        let file_path = volume_dir.join(Path::new(&path));
        check_path_length(file_path.to_string_lossy().to_string().as_str())?;

        let f = self.open_file(file_path, O_CREATE | O_APPEND | O_WRONLY, volume_dir).await?;

        Ok(FileWriter::Local(LocalFileWriter::new(f)))
    }

    // TODO: io verifier
    async fn read_file(&self, volume: &str, path: &str) -> Result<FileReader> {
        let volume_dir = self.get_bucket_path(volume)?;
        if !skip_access_checks(volume) {
            if let Err(e) = utils::fs::access(&volume_dir).await {
                return Err(convert_access_error(e, DiskError::VolumeAccessDenied));
            }
        }

        let file_path = volume_dir.join(Path::new(&path));
        check_path_length(file_path.to_string_lossy().to_string().as_str())?;

        let f = self.open_file(file_path, O_RDONLY, volume_dir).await.map_err(|err| {
            if let Some(e) = err.to_io_err() {
                if os_is_not_exist(&e) {
                    Error::new(DiskError::FileNotFound)
                } else if os_is_permission(&e) || is_sys_err_not_dir(&e) {
                    Error::new(DiskError::FileAccessDenied)
                } else if is_sys_err_io(&e) {
                    Error::new(DiskError::FaultyDisk)
                } else if is_sys_err_too_many_files(&e) {
                    Error::new(DiskError::TooManyOpenFiles)
                } else {
                    Error::new(e)
                }
            } else {
                err
            }
        })?;

        Ok(FileReader::Local(LocalFileReader::new(f)))
    }

    async fn list_dir(&self, origvolume: &str, volume: &str, dir_path: &str, count: i32) -> Result<Vec<String>> {
        if !origvolume.is_empty() {
            let origvolume_dir = self.get_bucket_path(origvolume)?;
            if !skip_access_checks(origvolume) {
                if let Err(e) = utils::fs::access(origvolume_dir).await {
                    return Err(convert_access_error(e, DiskError::VolumeAccessDenied));
                }
            }
        }

        let volume_dir = self.get_bucket_path(volume)?;
        let dir_path_abs = volume_dir.join(Path::new(&dir_path));

        let entries = match os::read_dir(&dir_path_abs, count).await {
            Ok(res) => res,
            Err(e) => {
                if is_err_file_not_found(&e) && !skip_access_checks(volume) {
                    if let Err(e) = utils::fs::access(&volume_dir).await {
                        return Err(convert_access_error(e, DiskError::VolumeAccessDenied));
                    }
                }

                return Err(e);
            }
        };
        Ok(entries)
    }

    // TODO: io.writer
    async fn walk_dir(&self, opts: WalkDirOptions) -> Result<Vec<MetaCacheEntry>> {
        let mut entries = match self.list_dir("", &opts.bucket, &opts.base_dir, -1).await {
            Ok(res) => res,
            Err(e) => {
                if !DiskError::VolumeNotFound.is(&e) && !is_err_file_not_found(&e) {
                    error!("list_dir err {:?}", &e);
                }

                if opts.report_notfound && is_err_file_not_found(&e) {
                    return Err(e);
                }
                return Ok(Vec::new());
            }
        };

        if entries.is_empty() {
            return Ok(Vec::new());
        }

        entries.sort();

        // 已读计数
        let objs_returned = 0;

        let bucket = opts.bucket.as_str();

        let mut metas = Vec::new();

        let mut dir_objes = HashSet::new();

        // 第一层过滤
        for entry in entries.iter() {
            // check limit
            if opts.limit > 0 && objs_returned >= opts.limit {
                return Ok(metas);
            }
            // check prefix
            if !opts.filter_prefix.is_empty() && !entry.starts_with(&opts.filter_prefix) {
                continue;
            }

            // warn!("walk_dir entry {}", entry);

            let mut meta = MetaCacheEntry { ..Default::default() };

            let fpath = self.get_object_path(bucket, format!("{}/{}", &entry, STORAGE_FORMAT_FILE).as_str())?;

            if let Ok(data) = self.read_metadata(&fpath).await {
                meta.metadata = data;
            }

            let mut name = entry.clone();
            if name.ends_with(SLASH_SEPARATOR) {
                if name.ends_with(GLOBAL_DIR_SUFFIX_WITH_SLASH) {
                    name = format!("{}{}", name.as_str().trim_end_matches(GLOBAL_DIR_SUFFIX_WITH_SLASH), SLASH_SEPARATOR);
                    dir_objes.insert(name.clone());
                } else {
                    name = name.as_str().trim_end_matches(SLASH_SEPARATOR).to_owned();
                }
            }
            meta.name = name;

            metas.push(meta);
        }

        Ok(metas)
    }

    // #[tracing::instrument(skip(self))]
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
            if let Err(e) = utils::fs::access(&src_volume_dir).await {
                return Err(convert_access_error(e, DiskError::VolumeAccessDenied));
            }
        }

        let dst_volume_dir = self.get_bucket_path(dst_volume)?;
        if !skip_access_checks(dst_volume) {
            if let Err(e) = utils::fs::access(&dst_volume_dir).await {
                return Err(convert_access_error(e, DiskError::VolumeAccessDenied));
            }
        }

        // xl.meta路径
        let src_file_path = src_volume_dir.join(Path::new(format!("{}/{}", &src_path, super::STORAGE_FORMAT_FILE).as_str()));
        let dst_file_path = dst_volume_dir.join(Path::new(format!("{}/{}", &dst_path, super::STORAGE_FORMAT_FILE).as_str()));

        // data_dir 路径
        let has_data_dir_path = {
            let has_data_dir = {
                if !fi.is_remote() {
                    fi.data_dir.map(|dir| utils::path::retain_slash(dir.to_string().as_str()))
                } else {
                    None
                }
            };

            if let Some(data_dir) = has_data_dir {
                let src_data_path = src_volume_dir.join(Path::new(
                    utils::path::retain_slash(format!("{}/{}", &src_path, data_dir).as_str()).as_str(),
                ));
                let dst_data_path = dst_volume_dir.join(Path::new(
                    utils::path::retain_slash(format!("{}/{}", &dst_path, data_dir).as_str()).as_str(),
                ));

                Some((src_data_path, dst_data_path))
            } else {
                None
            }
        };

        check_path_length(src_file_path.to_string_lossy().to_string().as_str())?;
        check_path_length(dst_file_path.to_string_lossy().to_string().as_str())?;

        // 读旧xl.meta

        let has_dst_buf = match utils::fs::read_file(&dst_file_path).await {
            Ok(res) => Some(res),
            Err(e) => {
                if is_sys_err_not_dir(&e) && !cfg!(target_os = "windows") {
                    return Err(Error::new(DiskError::FileAccessDenied));
                }

                if !os_is_not_exist(&e) {
                    return Err(os_err_to_file_err(e));
                }

                None
            }
        };

        // let current_data_path = dst_volume_dir.join(Path::new(&dst_path));

        let mut xlmeta = FileMeta::new();

        if let Some(dst_buf) = has_dst_buf.as_ref() {
            if FileMeta::is_xl_format(dst_buf) {
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
            .await
            .map_err(|err| {
                if let Some(e) = err.to_io_err() {
                    os_err_to_file_err(e)
                } else {
                    err
                }
            })?;

        if let Some((src_data_path, dst_data_path)) = has_data_dir_path.as_ref() {
            let no_inline = fi.data.is_none() && fi.size > 0;
            if no_inline {
                if let Err(err) = os::rename_all(&src_data_path, &dst_data_path, &skip_parent).await {
                    let _ = self.delete_file(&dst_volume_dir, dst_data_path, false, false).await;

                    return Err({
                        if let Some(e) = err.to_io_err() {
                            os_err_to_file_err(e)
                        } else {
                            err
                        }
                    });
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
                    return Err({
                        if let Some(e) = err.to_io_err() {
                            os_err_to_file_err(e)
                        } else {
                            err
                        }
                    });
                }
            }
        }

        if let Err(err) = os::rename_all(&src_file_path, &dst_file_path, &skip_parent).await {
            if let Some((_, dst_data_path)) = has_data_dir_path.as_ref() {
                let _ = self.delete_file(&dst_volume_dir, dst_data_path, false, false).await;
            }
            return Err({
                if let Some(e) = err.to_io_err() {
                    os_err_to_file_err(e)
                } else {
                    err
                }
            });
        }

        if let Some(src_file_path_parent) = src_file_path.parent() {
            if src_volume != super::RUSTFS_META_MULTIPART_BUCKET {
                let _ = utils::fs::remove(src_file_path_parent).await;
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

    async fn make_volumes(&self, volumes: Vec<&str>) -> Result<()> {
        for vol in volumes {
            if let Err(e) = self.make_volume(vol).await {
                if !DiskError::VolumeExists.is(&e) {
                    return Err(e);
                }
            }
            // TODO: health check
        }
        Ok(())
    }
    async fn make_volume(&self, volume: &str) -> Result<()> {
        if !Self::is_valid_volname(volume) {
            return Err(Error::msg("Invalid arguments specified"));
        }

        let volume_dir = self.get_bucket_path(volume)?;

        if let Err(e) = utils::fs::access(&volume_dir).await {
            if os_is_not_exist(&e) {
                os::make_dir_all(&volume_dir, self.root.as_path()).await?;
                return Ok(());
            }
            if os_is_permission(&e) {
                return Err(Error::new(DiskError::DiskAccessDenied));
            } else if is_sys_err_io(&e) {
                return Err(Error::new(DiskError::FaultyDisk));
            }

            return Err(Error::new(e));
        }

        Err(Error::from(DiskError::VolumeExists))
    }
    async fn list_volumes(&self) -> Result<Vec<VolumeInfo>> {
        let mut volumes = Vec::new();

        let entries = os::read_dir(&self.root, -1).await.map_err(|e| {
            if DiskError::FileAccessDenied.is(&e) || is_err_file_not_found(&e) {
                Error::new(DiskError::DiskAccessDenied)
            } else {
                e
            }
        })?;

        for entry in entries {
            if !utils::path::has_suffix(&entry, SLASH_SEPARATOR) || !Self::is_valid_volname(utils::path::clean(&entry).as_str()) {
                continue;
            }

            volumes.push(VolumeInfo {
                name: clean(&entry),
                created: None,
            });
        }

        Ok(volumes)
    }
    async fn stat_volume(&self, volume: &str) -> Result<VolumeInfo> {
        let volume_dir = self.get_bucket_path(volume)?;
        let meta = match utils::fs::lstat(&volume_dir).await {
            Ok(res) => res,
            Err(e) => {
                if os_is_not_exist(&e) {
                    return Err(Error::new(DiskError::VolumeNotFound));
                } else if os_is_permission(&e) {
                    return Err(Error::new(DiskError::DiskAccessDenied));
                } else if is_sys_err_io(&e) {
                    return Err(Error::new(DiskError::FaultyDisk));
                } else {
                    return Err(Error::new(e));
                }
            }
        };

        let modtime = match meta.modified() {
            Ok(md) => Some(OffsetDateTime::from(md)),
            Err(_) => None,
        };

        Ok(VolumeInfo {
            name: volume.to_string(),
            created: modtime,
        })
    }
    async fn delete_paths(&self, volume: &str, paths: &[&str]) -> Result<()> {
        let volume_dir = self.get_bucket_path(volume)?;
        if !skip_access_checks(volume) {
            utils::fs::access(&volume_dir)
                .await
                .map_err(|e| convert_access_error(e, DiskError::VolumeAccessDenied))?
        }

        for path in paths.iter() {
            let file_path = volume_dir.join(Path::new(path));

            check_path_length(file_path.to_string_lossy().as_ref())?;

            self.move_to_trash(&file_path, false, false).await?;
        }

        Ok(())
    }
    async fn update_metadata(&self, volume: &str, path: &str, fi: FileInfo, opts: &UpdateMetadataOpts) -> Result<()> {
        if fi.metadata.is_some() {
            let volume_dir = self.get_bucket_path(volume)?;
            let file_path = volume_dir.join(Path::new(&path));

            check_path_length(file_path.to_string_lossy().as_ref())?;

            let buf = self
                .read_all(volume, format!("{}/{}", &path, super::STORAGE_FORMAT_FILE).as_str())
                .await
                .map_err(|e| {
                    if is_err_file_not_found(&e) && fi.version_id.is_some() {
                        Error::new(DiskError::FileVersionNotFound)
                    } else {
                        e
                    }
                })?;

            if !FileMeta::is_xl_format(buf.as_slice()) {
                return Err(Error::new(DiskError::FileVersionNotFound));
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
    async fn write_metadata(&self, _org_volume: &str, volume: &str, path: &str, fi: FileInfo) -> Result<()> {
        let p = self.get_object_path(volume, format!("{}/{}", path, super::STORAGE_FORMAT_FILE).as_str())?;

        warn!("write_metadata {:?} {:?}", &p, &fi);

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

        let mut meta = FileMeta::default();
        meta.unmarshal_msg(&data)?;

        let fi = meta.into_fileinfo(volume, path, version_id, read_data, true)?;
        Ok(fi)
    }
    async fn read_xl(&self, volume: &str, path: &str, read_data: bool) -> Result<RawFileInfo> {
        let file_path = self.get_object_path(volume, path)?;
        let file_dir = self.get_bucket_path(volume)?;

        let (buf, _) = self.read_raw(volume, file_dir, file_path, read_data).await?;

        Ok(RawFileInfo { buf })
    }
    async fn delete_version(
        &self,
        volume: &str,
        _path: &str,
        _fi: FileInfo,
        _force_del_marker: bool,
        _opts: DeleteOptions,
    ) -> Result<RawFileInfo> {
        let _volume_dir = self.get_bucket_path(volume)?;

        // self.read_all_data(bucket, volume_dir, path);

        unimplemented!()
    }
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
                    if !(is_err_file_not_found(&e) || DiskError::VolumeNotFound.is(&e)) {
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

    async fn delete_volume(&self, volume: &str) -> Result<()> {
        let p = self.get_bucket_path(volume)?;

        // TODO: 不能用递归删除，如果目录下面有文件，返回errVolumeNotEmpty

        if let Err(err) = fs::remove_dir_all(&p).await {
            match err.kind() {
                ErrorKind::NotFound => (),
                // ErrorKind::DirectoryNotEmpty => (),
                kind => {
                    if kind.to_string() == "directory not empty" {
                        return Err(Error::new(DiskError::VolumeNotEmpty));
                    }

                    return Err(Error::from(err));
                }
            }
        }

        Ok(())
    }

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

async fn get_disk_info(drive_path: PathBuf) -> Result<(Info, bool)> {
    let drive_path = drive_path.to_string_lossy().to_string();
    check_path_length(&drive_path)?;

    let disk_info = get_info(&drive_path)?;
    let root_drive = if !*GLOBAL_IsErasureSD.read().await {
        let root_disk_threshold = *GLOBAL_RootDiskThreshold.read().await;
        if root_disk_threshold > 0 {
            disk_info.total <= root_disk_threshold
        } else {
            match is_root_disk(&drive_path, SLASH_SEPARATOR) {
                Ok(result) => result,
                Err(_) => false,
            }
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
