use super::error::{is_sys_err_io, is_sys_err_not_empty, os_is_not_exist};
use super::{endpoint::Endpoint, error::DiskError, format::FormatV3};
use super::{
    os, DeleteOptions, DiskAPI, DiskLocation, FileInfoVersions, FileReader, FileWriter, MetaCacheEntry, ReadMultipleReq,
    ReadMultipleResp, ReadOptions, RenameDataResp, UpdateMetadataOpts, VolumeInfo, WalkDirOptions,
};
use crate::disk::error::{is_sys_err_not_dir, map_err_not_exists};
use crate::disk::os::check_path_length;
use crate::disk::{LocalFileReader, LocalFileWriter, STORAGE_FORMAT_FILE};
use crate::utils::fs::lstat;
use crate::utils::path::{has_suffix, SLASH_SEPARATOR};
use crate::{
    error::{Error, Result},
    file_meta::FileMeta,
    store_api::{FileInfo, RawFileInfo},
    utils,
};
use bytes::Bytes;
use path_absolutize::Absolutize;
use std::{
    fs::Metadata,
    path::{Path, PathBuf},
};
use time::OffsetDateTime;
use tokio::fs::{self, File};
use tokio::io::ErrorKind;
use tokio::sync::Mutex;
use tower::layer::util;
use tracing::{debug, warn};
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

#[derive(Debug)]
pub struct LocalDisk {
    pub root: PathBuf,
    pub format_path: PathBuf,
    pub format_info: Mutex<FormatInfo>,
    pub endpoint: Endpoint,
    // pub id: Mutex<Option<Uuid>>,
    // pub format_data: Mutex<Vec<u8>>,
    // pub format_file_info: Mutex<Option<Metadata>>,
    // pub format_last_check: Mutex<Option<OffsetDateTime>>,
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

        // TODO: DIRECT suport
        // TODD: DiskInfo
        let disk = Self {
            root,
            endpoint: ep.clone(),
            format_path: format_path,
            format_info: Mutex::new(format_info),
            // // format_legacy,
            // format_file_info: Mutex::new(format_meta),
            // format_data: Mutex::new(format_data),
            // format_last_check: Mutex::new(format_last_check),
        };

        disk.make_meta_volumes().await?;

        Ok(disk)
    }

    // fn check_path_length(_path_name: &str) -> Result<()> {
    //     unimplemented!()
    // }

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

    pub async fn rename_all(&self, src_data_path: &PathBuf, dst_data_path: &PathBuf, skip: &PathBuf) -> Result<()> {
        if !skip.starts_with(src_data_path) {
            fs::create_dir_all(dst_data_path.parent().unwrap_or(Path::new("/"))).await?;
        }

        debug!(
            "rename_all from \n {:?} \n to \n {:?} \n skip:{:?}",
            &src_data_path, &dst_data_path, &skip
        );

        fs::rename(&src_data_path, &dst_data_path).await?;

        Ok(())
    }

    pub async fn move_to_trash(&self, delete_path: &PathBuf, _recursive: bool, _immediate_purge: bool) -> Result<()> {
        let trash_path = self.get_object_path(super::RUSTFS_META_TMP_DELETED_BUCKET, Uuid::new_v4().to_string().as_str())?;
        if let Some(parent) = trash_path.parent() {
            if !parent.exists() {
                fs::create_dir_all(parent).await?;
            }
        }
        debug!("move_to_trash from:{:?} to {:?}", &delete_path, &trash_path);
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
        debug!("delete_file {:?}\n base_path:{:?}", &delete_path, &base_path);

        if is_root_path(base_path) || is_root_path(delete_path) {
            debug!("delete_file skip {:?}", &delete_path);
            return Ok(());
        }

        if !delete_path.starts_with(base_path) || base_path == delete_path {
            debug!("delete_file skip {:?}", &delete_path);
            return Ok(());
        }

        if recursive {
            self.move_to_trash(delete_path, recursive, immediate_purge).await?;
        } else {
            if delete_path.is_dir() {
                debug!("delete_file remove_dir {:?}", &delete_path);
                if let Err(err) = fs::remove_dir(&delete_path).await {
                    debug!("remove_dir err {:?} when {:?}", &err, &delete_path);
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
                debug!("delete_file remove_dir done {:?}", &delete_path);
            } else {
                if let Err(err) = fs::remove_file(&delete_path).await {
                    debug!("remove_file err {:?} when {:?}", &err, &delete_path);
                    match err.kind() {
                        ErrorKind::NotFound => (),
                        _ => {
                            warn!("delete_file remove_file {:?}  err {:?}", &delete_path, &err);
                            return Err(Error::from(err));
                        }
                    }
                }
            }
        }

        if let Some(dir_path) = delete_path.parent() {
            Box::pin(self.delete_file(base_path, &PathBuf::from(dir_path), false, false)).await?;
        }

        debug!("delete_file done {:?}", &delete_path);
        Ok(())
    }

    /// read xl.meta raw data
    async fn read_raw(
        &self,
        bucket: &str,
        volume_dir: impl AsRef<Path>,
        path: impl AsRef<Path>,
        read_data: bool,
    ) -> Result<(Vec<u8>, OffsetDateTime)> {
        let meta_path = path.as_ref().join(Path::new(super::STORAGE_FORMAT_FILE));
        if read_data {
            self.read_all_data(bucket, volume_dir, meta_path).await
        } else {
            self.read_metadata_with_dmtime(meta_path).await
        }
    }

    async fn read_metadata_with_dmtime(&self, path: impl AsRef<Path>) -> Result<(Vec<u8>, OffsetDateTime)> {
        let (data, meta) = read_file_all(path).await?;

        let modtime = match meta.modified() {
            Ok(md) => OffsetDateTime::from(md),
            Err(_) => return Err(Error::msg("Not supported modified on this platform")),
        };

        Ok((data, modtime))
    }

    async fn read_all_data(
        &self,
        _bucket: &str,
        _volume_dir: impl AsRef<Path>,
        path: impl AsRef<Path>,
    ) -> Result<(Vec<u8>, OffsetDateTime)> {
        let (data, meta) = read_file_all(path).await?;

        let modtime = match meta.modified() {
            Ok(md) => OffsetDateTime::from(md),
            Err(_) => return Err(Error::msg("Not supported modified on this platform")),
        };

        Ok((data, modtime))
    }

    async fn delete_versions_internal(&self, volume: &str, path: &str, fis: &Vec<FileInfo>) -> Result<()> {
        let volume_dir = self.get_bucket_path(volume)?;
        let xlpath = self.get_object_path(volume, format!("{}/{}", path, super::STORAGE_FORMAT_FILE).as_str())?;

        let (data, _) = match self.read_all_data(volume, volume_dir.as_path(), &xlpath).await {
            Ok(res) => res,
            Err(_err) => {
                //  TODO: check if not found return err

                (Vec::new(), OffsetDateTime::UNIX_EPOCH)
            }
        };

        if data.is_empty() {
            return Err(Error::new(DiskError::FileNotFound));
        }

        let mut fm = FileMeta::default();

        fm.unmarshal_msg(&data)?;

        for fi in fis {
            let data_dir = fm.delete_version(fi)?;
            warn!("删除版本号 对应data_dir {:?}", &data_dir);
            if data_dir.is_some() {
                let dir_path = self.get_object_path(volume, format!("{}/{}", path, data_dir.unwrap().to_string()).as_str())?;
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

        self.write_all(volume, format!("{}/{}", path, super::STORAGE_FORMAT_FILE).as_str(), buf)
            .await?;

        Ok(())
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
            if DiskError::FileNotFound.is(&e) {
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

pub async fn write_all_internal(p: impl AsRef<Path>, data: impl AsRef<[u8]>) -> Result<()> {
    // create top dir if not exists
    fs::create_dir_all(&p.as_ref().parent().unwrap_or_else(|| Path::new("."))).await?;

    fs::write(&p, data).await?;
    Ok(())
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

pub async fn check_volume_exists(p: impl AsRef<Path>) -> Result<()> {
    fs::metadata(&p).await.map_err(|e| match e.kind() {
        ErrorKind::NotFound => Error::from(DiskError::VolumeNotFound),
        ErrorKind::PermissionDenied => Error::from(DiskError::FileAccessDenied),
        _ => Error::from(e),
    })?;
    Ok(())
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
        // TODO: check format file
        let mut format_info = self.format_info.lock().await;

        let id = format_info.id.clone();

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
        // TODO: 判断源文件id,是否有效
    }

    async fn set_disk_id(&self, id: Option<Uuid>) -> Result<()> {
        // 本地不需要设置
        let mut format_info = self.format_info.lock().await;
        format_info.id = id;
        Ok(())
    }

    #[must_use]
    async fn read_all(&self, volume: &str, path: &str) -> Result<Bytes> {
        let p = self.get_object_path(volume, path)?;
        let (data, _) = read_file_all(&p).await?;

        Ok(Bytes::from(data))
    }

    async fn write_all(&self, volume: &str, path: &str, data: Vec<u8>) -> Result<()> {
        let p = self.get_object_path(volume, path)?;

        write_all_internal(p, data).await?;

        Ok(())
    }

    async fn delete(&self, volume: &str, path: &str, opt: DeleteOptions) -> Result<()> {
        let vol_path = self.get_bucket_path(volume)?;
        if !skip_access_checks(volume) {
            check_volume_exists(&vol_path).await?;
        }

        let fpath = self.get_object_path(volume, path)?;

        self.delete_file(&vol_path, &fpath, opt.recursive, opt.immediate).await?;

        // if opt.recursive {
        //     let trash_path = self.get_object_path(RUSTFS_META_TMP_DELETED_BUCKET, Uuid::new_v4().to_string().as_str())?;
        //     fs::create_dir_all(&trash_path).await?;
        //     fs::rename(&fpath, &trash_path).await?;

        //     // TODO: immediate

        //     return Ok(());
        // }

        // fs::remove_file(fpath).await?;

        Ok(())
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

        let src_is_dir = has_suffix(&src_path, SLASH_SEPARATOR);
        let dst_is_dir = has_suffix(&dst_path, SLASH_SEPARATOR);

        if !(src_is_dir && dst_is_dir || !src_is_dir && !dst_is_dir) {
            return Err(Error::from(DiskError::FileAccessDenied));
        }

        let src_file_path = src_volume_dir.join(Path::new(src_path));
        let dst_file_path = dst_volume_dir.join(Path::new(dst_path));

        check_path_length(&src_file_path.to_string_lossy().to_string())?;
        check_path_length(&dst_file_path.to_string_lossy().to_string())?;

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

        unimplemented!()
    }
    async fn rename_file(&self, src_volume: &str, src_path: &str, dst_volume: &str, dst_path: &str) -> Result<()> {
        let src_volume_path = self.get_bucket_path(src_volume)?;
        let dst_volume_path = self.get_bucket_path(dst_volume)?;
        if !skip_access_checks(src_volume) {
            utils::fs::access(&src_volume_path).await.map_err(map_err_not_exists)?;
        }
        if !skip_access_checks(dst_volume) {
            utils::fs::access(&dst_volume_path).await.map_err(map_err_not_exists)?;
        }

        let srcp = self.get_object_path(src_volume, src_path)?;
        let dstp = self.get_object_path(dst_volume, dst_path)?;

        let src_is_dir = srcp.is_dir();
        let dst_is_dir = dstp.is_dir();
        if !src_is_dir && dst_is_dir || src_is_dir && !dst_is_dir {
            return Err(Error::from(DiskError::FileAccessDenied));
        }

        // TODO: check path length

        if src_is_dir {
            // TODO: remove dst_dir
        }

        fs::create_dir_all(dstp.parent().unwrap_or_else(|| Path::new("."))).await?;

        let mut idx = 0;
        loop {
            if let Err(e) = fs::rename(&srcp, &dstp).await {
                if e.kind() == ErrorKind::NotFound && idx == 0 {
                    idx += 1;
                    continue;
                }
            };

            break;
        }

        if let Some(dir_path) = srcp.parent() {
            self.delete_file(&src_volume_path, &PathBuf::from(dir_path), false, false)
                .await?;
        }

        Ok(())
    }

    async fn create_file(&self, _origvolume: &str, volume: &str, path: &str, _file_size: usize) -> Result<FileWriter> {
        let volpath = self.get_bucket_path(&volume)?;
        // check exists
        fs::metadata(&volpath).await.map_err(|e| match e.kind() {
            ErrorKind::NotFound => Error::new(DiskError::VolumeNotFound),
            _ => Error::new(e),
        })?;

        let fpath = self.get_object_path(volume, path)?;

        debug!("CreateFile fpath: {:?}", fpath);

        if let Some(_dir_path) = fpath.parent() {
            fs::create_dir_all(&_dir_path).await?;
        }

        let file = File::create(&fpath).await?;

        Ok(FileWriter::Local(LocalFileWriter::new(file)))
        // Ok(FileWriter::new(file))

        // let mut writer = BufWriter::new(file);

        // io::copy(&mut r, &mut writer).await?;

        // Ok(())
    }
    // async fn append_file(&self, volume: &str, path: &str, mut r: DuplexStream) -> Result<File> {
    async fn append_file(&self, volume: &str, path: &str) -> Result<FileWriter> {
        let p = self.get_object_path(volume, path)?;

        if let Some(dir_path) = p.parent() {
            fs::create_dir_all(&dir_path).await?;
        }

        // debug!("append_file open {} {:?}", self.id(), &p);

        let file = File::options()
            .read(true)
            .create(true)
            .write(true)
            .append(true)
            .open(&p)
            .await?;

        Ok(FileWriter::Local(LocalFileWriter::new(file)))
        // Ok(FileWriter::new(file))

        // let mut writer = BufWriter::new(file);

        // io::copy(&mut r, &mut writer).await?;

        // debug!("append_file end {} {}", self.id(), path);
        // io::copy(&mut r, &mut file).await?;

        // Ok(())
    }
    async fn read_file(&self, volume: &str, path: &str) -> Result<FileReader> {
        let p = self.get_object_path(volume, path)?;

        debug!("read_file {:?}", &p);
        let file = File::options().read(true).open(&p).await?;

        Ok(FileReader::Local(LocalFileReader::new(file)))

        // file.seek(SeekFrom::Start(offset as u64)).await?;

        // let mut buffer = vec![0; length];

        // let bytes_read = file.read(&mut buffer).await?;

        // buffer.truncate(bytes_read);

        // Ok((buffer, bytes_read))
    }
    async fn list_dir(&self, _origvolume: &str, volume: &str, _dir_path: &str, _count: i32) -> Result<Vec<String>> {
        let p = self.get_bucket_path(volume)?;

        let mut entries = fs::read_dir(&p).await?;

        let mut volumes = Vec::new();

        while let Some(entry) = entries.next_entry().await? {
            if let Ok(_metadata) = entry.metadata().await {
                // if !metadata.is_dir() {
                //     continue;
                // }

                let name = entry.file_name().to_string_lossy().to_string();

                // let created = match metadata.created() {
                //     Ok(md) => OffsetDateTime::from(md),
                //     Err(_) => return Err(Error::msg("Not supported created on this platform")),
                // };

                volumes.push(name);
            }
        }

        Ok(volumes)
    }

    async fn walk_dir(&self, opts: WalkDirOptions) -> Result<Vec<MetaCacheEntry>> {
        let mut entries = self.list_dir("", &opts.bucket, &opts.base_dir, -1).await?;

        entries.sort();

        // 已读计数
        let objs_returned = 0;

        let bucket = opts.bucket.as_str();

        let mut metas = Vec::new();

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

            let mut meta = MetaCacheEntry {
                name: entry.clone(),
                ..Default::default()
            };

            let fpath = self.get_object_path(bucket, format!("{}/{}", &meta.name, STORAGE_FORMAT_FILE).as_str())?;

            let (fdata, _) = match self.read_metadata_with_dmtime(&fpath).await {
                Ok(res) => res,
                Err(_) => {
                    // TODO: check err
                    (Vec::new(), OffsetDateTime::UNIX_EPOCH)
                }
            };

            meta.metadata = fdata;

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
        let src_volume_path = self.get_bucket_path(src_volume)?;
        if !skip_access_checks(src_volume) {
            check_volume_exists(&src_volume_path).await?;
        }

        let dst_volume_path = self.get_bucket_path(dst_volume)?;
        if !skip_access_checks(dst_volume) {
            check_volume_exists(&dst_volume_path).await?;
        }

        // xl.meta路径
        let src_file_path = self.get_object_path(src_volume, format!("{}/{}", &src_path, super::STORAGE_FORMAT_FILE).as_str())?;
        let dst_file_path = self.get_object_path(dst_volume, format!("{}/{}", &dst_path, super::STORAGE_FORMAT_FILE).as_str())?;

        // data_dir 路径
        let (src_data_path, dst_data_path) = {
            let mut data_dir = String::new();
            if !fi.is_remote() {
                data_dir = utils::path::retain_slash(fi.data_dir.unwrap_or(Uuid::nil()).to_string().as_str());
            }

            if !data_dir.is_empty() {
                let src_data_path = self.get_object_path(
                    src_volume,
                    utils::path::retain_slash(format!("{}/{}", &src_path, data_dir).as_str()).as_str(),
                )?;
                let dst_data_path = self.get_object_path(dst_volume, format!("{}/{}", &dst_path, data_dir).as_str())?;

                (src_data_path, dst_data_path)
            } else {
                (PathBuf::new(), PathBuf::new())
            }
        };

        // 读旧xl.meta
        let mut meta = FileMeta::new();

        let (dst_buf, _) = read_file_exists(&dst_file_path).await?;
        if !dst_buf.is_empty() {
            // 有旧文件，加载
            match meta.unmarshal_msg(&dst_buf) {
                Ok(_) => {}
                Err(_) => meta = FileMeta::new(),
            }
        }

        let mut skip_parent = dst_volume_path.clone();
        if !dst_buf.is_empty() {
            skip_parent = PathBuf::from(&dst_file_path.parent().unwrap_or(Path::new("/")));
        }

        // 查找版本是否已存在
        let old_data_dir = meta
            .find_version(fi.version_id)
            .map(|(_, version)| {
                version
                    .get_data_dir()
                    .filter(|data_dir| meta.shard_data_dir_count(&fi.version_id, &Some(data_dir.clone())) == 0)
            })
            .unwrap_or_default();

        // 添加版本，写入xl.meta文件
        meta.add_version(fi.clone())?;

        let fm_data = meta.marshal_msg()?;

        // 写入xl.meta
        write_all_internal(&src_file_path, fm_data).await?;

        let no_inline = src_data_path.has_root() && fi.data.is_none() && fi.size > 0;
        if no_inline {
            self.rename_all(&src_data_path, &dst_data_path, &skip_parent).await?;
        }

        // warn!("old_data_dir {:?}", old_data_dir);
        // 有旧目录，把old xl.meta存到旧目录里
        if old_data_dir.is_some() {
            self.write_all(
                &dst_volume,
                format!("{}/{}/{}", &dst_path, &old_data_dir.unwrap().to_string(), super::STORAGE_FORMAT_FILE).as_str(),
                dst_buf,
            )
            .await?;
        }

        if let Err(e) = self.rename_all(&src_file_path, &dst_file_path, &skip_parent).await {
            // 如果 失败删除目标目录
            let _ = self.delete_file(&dst_volume_path, &dst_data_path, false, false).await;
            return Err(e);
        }

        if src_volume != super::RUSTFS_META_MULTIPART_BUCKET {
            fs::remove_dir(&src_file_path.parent().unwrap()).await?;
        } else {
            self.delete_file(&src_volume_path, &PathBuf::from(src_file_path.parent().unwrap()), true, false)
                .await?;
        }

        Ok(RenameDataResp {
            old_data_dir: old_data_dir,
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

        let p = self.get_bucket_path(volume)?;

        if let Err(err) = utils::fs::access(&p).await {
            if os_is_not_exist(err) {
                os::make_dir_all(&p).await?;
            }
        }

        Err(Error::from(DiskError::VolumeExists))
    }
    async fn list_volumes(&self) -> Result<Vec<VolumeInfo>> {
        let mut volumes = Vec::new();

        let entries = os::read_dir(&self.root, 0).await.map_err(|e| {
            if DiskError::FileAccessDenied.is(&e) {
                Error::new(DiskError::DiskAccessDenied)
            } else if DiskError::FileNotFound.is(&e) {
                Error::new(DiskError::DiskAccessDenied)
            } else {
                e
            }
        })?;

        for entry in entries {
            if !utils::path::has_suffix(&entry, SLASH_SEPARATOR) || !Self::is_valid_volname(&entry) {
                continue;
            }

            volumes.push(VolumeInfo {
                name: entry,
                created: None,
            });
        }

        Ok(volumes)
    }
    async fn stat_volume(&self, volume: &str) -> Result<VolumeInfo> {
        let p = self.get_bucket_path(volume)?;

        let m = read_file_metadata(&p).await?;
        let modtime = match m.modified() {
            Ok(md) => Some(OffsetDateTime::from(md)),
            Err(_) => {
                warn!("Not supported modified on this platform");
                None
            }
        };

        Ok(VolumeInfo {
            name: volume.to_string(),
            created: modtime,
        })
    }
    async fn delete_paths(&self, _volume: &str, _paths: &[&str]) -> Result<()> {
        unimplemented!()
    }
    async fn update_metadata(&self, _volume: &str, _path: &str, _fi: FileInfo, _opts: UpdateMetadataOpts) {
        unimplemented!()
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
                    ()
                });
            }
        }

        meta.add_version(fi)?;

        let fm_data = meta.marshal_msg()?;

        write_all_internal(p, fm_data).await?;

        return Ok(());
    }

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

        let fi = meta.into_fileinfo(volume, path, version_id, false, true)?;
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
        _volume: &str,
        _path: &str,
        _fi: FileInfo,
        _force_del_marker: bool,
        _opts: DeleteOptions,
    ) -> Result<RawFileInfo> {
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
                    if !(DiskError::FileNotFound.is(&e) || DiskError::VolumeNotFound.is(&e)) {
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
        let p = "./testv";
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

        let volumes = vec!["a", "b", "c"];

        disk.make_volumes(volumes.clone()).await.unwrap();

        disk.make_volumes(volumes.clone()).await.unwrap();

        fs::remove_dir_all(&p).await.unwrap();
    }

    #[tokio::test]
    async fn test_delete_volume() {
        let p = "./testv";
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

        let volumes = vec!["a", "b", "c"];

        disk.make_volumes(volumes.clone()).await.unwrap();

        disk.delete_volume("a").await.unwrap();

        fs::remove_dir_all(&p).await.unwrap();
    }
}
