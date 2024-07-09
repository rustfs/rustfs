use std::{
    fs::Metadata,
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
};

use anyhow::{Error, Result};
use bytes::Bytes;
use futures::{future::join_all, Stream};
use path_absolutize::Absolutize;
use s3s::StdError;
use time::OffsetDateTime;
use tokio::io::{self, AsyncRead, AsyncReadExt, AsyncSeekExt};
use tokio::io::{AsyncWrite, BufWriter, ErrorKind};
use tokio::{
    fs::{self, File},
    io::DuplexStream,
};
use tracing::debug;
use uuid::Uuid;

use crate::{
    disk_api::{DiskAPI, DiskError, ReadOptions, VolumeInfo},
    endpoint::{Endpoint, Endpoints},
    file_meta::FileMeta,
    format::{DistributionAlgoVersion, FormatV3},
    store_api::{FileInfo, RawFileInfo},
    utils,
};

pub type DiskStore = Arc<Box<dyn DiskAPI>>;

pub const RUSTFS_META_BUCKET: &str = ".rustfs.sys";
pub const RUSTFS_META_MULTIPART_BUCKET: &str = ".rustfs.sys/multipart";
pub const RUSTFS_META_TMP_BUCKET: &str = ".rustfs.sys/tmp";
pub const RUSTFS_META_TMP_DELETED_BUCKET: &str = ".rustfs.sys/tmp/.trash";
pub const BUCKET_META_PREFIX: &str = "buckets";
pub const FORMAT_CONFIG_FILE: &str = "format.json";
const STORAGE_FORMAT_FILE: &str = "xl.meta";

pub struct DiskOption {
    pub cleanup: bool,
    pub health_check: bool,
}

pub async fn new_disk(ep: &Endpoint, opt: &DiskOption) -> Result<DiskStore> {
    if ep.is_local {
        let s = LocalDisk::new(ep, opt.cleanup).await?;
        Ok(Arc::new(Box::new(s)))
    } else {
        unimplemented!()
        // Ok(Disk::Remote(RemoteDisk::new(ep, opt.health_check)?))
    }
}

pub async fn init_disks(eps: &Endpoints, opt: &DiskOption) -> (Vec<Option<DiskStore>>, Vec<Option<Error>>) {
    let mut futures = Vec::with_capacity(eps.as_ref().len());

    for ep in eps.as_ref().iter() {
        futures.push(new_disk(ep, opt));
    }

    let mut res = Vec::with_capacity(eps.as_ref().len());
    let mut errors = Vec::with_capacity(eps.as_ref().len());

    let results = join_all(futures).await;
    for result in results {
        match result {
            Ok(s) => {
                res.push(Some(s));
                errors.push(None);
            }
            Err(e) => {
                res.push(None);
                errors.push(Some(e));
            }
        }
    }

    (res, errors)
}

// pub async fn load_format(&self, heal: bool) -> Result<FormatV3> {
//     unimplemented!()
// }

#[derive(Debug)]
pub struct LocalDisk {
    pub root: PathBuf,
    pub id: Uuid,
    pub format_data: Vec<u8>,
    pub format_meta: Option<Metadata>,
    pub format_path: PathBuf,
    // pub format_legacy: bool, // drop
    pub format_last_check: OffsetDateTime,
}

impl LocalDisk {
    pub async fn new(ep: &Endpoint, cleanup: bool) -> Result<Self> {
        let root = fs::canonicalize(ep.url.path()).await?;

        if cleanup {
            // TODO: 删除tmp数据
        }

        let format_path = Path::new(RUSTFS_META_BUCKET)
            .join(Path::new(FORMAT_CONFIG_FILE))
            .absolutize_virtually(&root)?
            .into_owned();

        let (format_data, format_meta) = read_file_exists(&format_path).await?;

        let mut id = Uuid::nil();
        // let mut format_legacy = false;
        let mut format_last_check = OffsetDateTime::UNIX_EPOCH;

        if !format_data.is_empty() {
            let s = format_data.as_slice();
            let fm = FormatV3::try_from(s)?;
            let (set_idx, disk_idx) = fm.find_disk_index_by_disk_id(fm.erasure.this)?;

            if Some(set_idx) != ep.set_idx || Some(disk_idx) != ep.disk_idx {
                return Err(Error::new(DiskError::InconsistentDisk));
            }

            id = fm.erasure.this;
            // format_legacy = fm.erasure.distribution_algo == DistributionAlgoVersion::V1;
            format_last_check = OffsetDateTime::now_utc();
        }

        let disk = Self {
            root,
            id,
            format_meta,
            format_data: format_data,
            format_path,
            // format_legacy,
            format_last_check,
        };

        disk.make_meta_volumes().await?;

        Ok(disk)
    }

    async fn make_meta_volumes(&self) -> Result<()> {
        let buckets = format!("{}/{}", RUSTFS_META_BUCKET, BUCKET_META_PREFIX);
        let multipart = format!("{}/{}", RUSTFS_META_BUCKET, "multipart");
        let config = format!("{}/{}", RUSTFS_META_BUCKET, "config");
        let tmp = format!("{}/{}", RUSTFS_META_BUCKET, "tmp");
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

    /// Write to the filesystem atomically.
    /// This is done by first writing to a temporary location and then moving the file.
    pub(crate) async fn prepare_file_write<'a>(&self, path: &'a PathBuf) -> Result<FileWriter<'a>> {
        let tmp_path = self.get_object_path(RUSTFS_META_TMP_BUCKET, Uuid::new_v4().to_string().as_str())?;

        debug!("prepare_file_write tmp_path:{:?}, path:{:?}", &tmp_path, &path);

        let file = File::create(&tmp_path).await?;
        let writer = BufWriter::new(file);
        Ok(FileWriter {
            tmp_path,
            dest_path: path,
            writer,
            clean_tmp: true,
        })
    }

    pub async fn rename_all(&self, src_data_path: &PathBuf, dst_data_path: &PathBuf, skip: &PathBuf) -> Result<()> {
        if !skip.starts_with(&src_data_path) {
            fs::create_dir_all(dst_data_path.parent().unwrap_or(Path::new("/"))).await?;
        }

        debug!(
            "rename_all from \n {:?} \n to \n {:?} \n skip:{:?}",
            &src_data_path, &dst_data_path, &skip
        );

        fs::rename(&src_data_path, &dst_data_path).await?;

        Ok(())
    }

    pub async fn delete_file(&self, base_path: &PathBuf, delete_path: &PathBuf) -> Result<()> {
        if is_root_path(base_path) || is_root_path(delete_path) {
            return Ok(());
        }

        if !delete_path.starts_with(base_path) || base_path == delete_path {
            return Ok(());
        }

        if delete_path.is_dir() {
            fs::remove_dir(delete_path).await?;
        } else {
            fs::remove_file(delete_path).await?;
        }

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
        let meta_path = path.as_ref().join(Path::new(STORAGE_FORMAT_FILE));
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
        bucket: &str,
        volume_dir: impl AsRef<Path>,
        path: impl AsRef<Path>,
    ) -> Result<(Vec<u8>, OffsetDateTime)> {
        let (data, meta) = read_file_all(path).await?;

        let modtime = match meta.modified() {
            Ok(md) => OffsetDateTime::from(md),
            Err(_) => return Err(Error::msg("Not supported modified on this platform")),
        };

        Ok((data, modtime))
    }
}

fn is_root_path(path: &PathBuf) -> bool {
    path.components().count() == 1 && path.has_root()
}

// 过滤 std::io::ErrorKind::NotFound
pub async fn read_file_exists(path: impl AsRef<Path>) -> Result<(Vec<u8>, Option<Metadata>)> {
    let p = path.as_ref();
    let (data, meta) = match read_file_all(&p).await {
        Ok((data, meta)) => (data, Some(meta)),
        Err(e) => {
            if DiskError::is_err(&e, &DiskError::FileNotFound) {
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
        ErrorKind::NotFound => Error::new(DiskError::FileNotFound),
        ErrorKind::PermissionDenied => Error::new(DiskError::FileAccessDenied),
        _ => Error::new(e),
    })?;

    Ok(meta)
}

pub async fn check_volume_exists(p: impl AsRef<Path>) -> Result<()> {
    fs::metadata(&p).await.map_err(|e| match e.kind() {
        ErrorKind::NotFound => Error::new(DiskError::VolumeNotFound),
        ErrorKind::PermissionDenied => Error::new(DiskError::FileAccessDenied),
        _ => Error::new(e),
    })?;
    Ok(())
}

fn skip_access_checks(p: impl AsRef<str>) -> bool {
    let vols = vec![
        RUSTFS_META_TMP_DELETED_BUCKET,
        RUSTFS_META_TMP_BUCKET,
        RUSTFS_META_MULTIPART_BUCKET,
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
    fn is_local(&self) -> bool {
        true
    }

    #[must_use]
    async fn read_all(&self, volume: &str, path: &str) -> Result<Bytes> {
        let p = self.get_object_path(&volume, &path)?;
        let (data, _) = read_file_all(&p).await?;

        Ok(Bytes::from(data))
    }

    async fn write_all(&self, volume: &str, path: &str, data: Bytes) -> Result<()> {
        let p = self.get_object_path(&volume, &path)?;

        write_all_internal(p, data).await?;

        Ok(())
    }

    async fn rename_file(&self, src_volume: &str, src_path: &str, dst_volume: &str, dst_path: &str) -> Result<()> {
        if !skip_access_checks(&src_volume) {
            let vol_path = self.get_bucket_path(&src_volume)?;
            check_volume_exists(&vol_path).await?;
        }
        if !skip_access_checks(&dst_volume) {
            let vol_path = self.get_bucket_path(&dst_volume)?;
            check_volume_exists(&vol_path).await?;
        }

        let srcp = self.get_object_path(&src_volume, &src_path)?;
        let dstp = self.get_object_path(&dst_volume, &dst_path)?;

        let src_is_dir = srcp.is_dir();
        let dst_is_dir = dstp.is_dir();
        if !(src_is_dir && dst_is_dir || !src_is_dir && !dst_is_dir) {
            return Err(Error::new(DiskError::FileAccessDenied));
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
        Ok(())
    }

    async fn create_file(&self, origvolume: &str, volume: &str, path: &str, fileSize: usize, mut r: DuplexStream) -> Result<()> {
        let fpath = self.get_object_path(volume, path)?;

        debug!("CreateFile fpath: {:?}", fpath);

        if let Some(_dir_path) = fpath.parent() {
            fs::create_dir_all(&_dir_path).await?;
        }

        let file = File::create(&fpath).await?;

        let mut writer = BufWriter::new(file);

        io::copy(&mut r, &mut writer).await?;

        Ok(())
    }

    async fn rename_data(&self, src_volume: &str, src_path: &str, fi: &FileInfo, dst_volume: &str, dst_path: &str) -> Result<()> {
        let src_volume_path = self.get_bucket_path(&src_volume)?;
        if !skip_access_checks(&src_volume) {
            check_volume_exists(&src_volume_path).await?;
        }

        let dst_volume_path = self.get_bucket_path(&dst_volume)?;
        if !skip_access_checks(&dst_volume) {
            check_volume_exists(&dst_volume_path).await?;
        }

        let src_file_path = self.get_object_path(&src_volume, format!("{}/{}", &src_path, STORAGE_FORMAT_FILE).as_str())?;
        let dst_file_path = self.get_object_path(&dst_volume, format!("{}/{}", &dst_path, STORAGE_FORMAT_FILE).as_str())?;

        let (src_data_path, dst_data_path) = {
            let mut data_dir = String::new();
            if !fi.is_remote() {
                data_dir = utils::path::retain_slash(fi.data_dir.to_string().as_str());
            }

            if !data_dir.is_empty() {
                let src_data_path = self.get_object_path(
                    &src_volume,
                    utils::path::retain_slash(format!("{}/{}", &src_path, data_dir).as_str()).as_str(),
                )?;
                let dst_data_path = self.get_object_path(&dst_volume, format!("{}/{}", &dst_path, data_dir).as_str())?;

                (src_data_path, dst_data_path)
            } else {
                (PathBuf::new(), PathBuf::new())
            }
        };

        // let curreng_data_path = self.get_object_path(&dst_volume, &dst_path);

        let mut meta = FileMeta::new();

        let (dst_buf, _) = read_file_exists(&dst_file_path).await?;

        let mut skipParent = dst_volume_path;
        if !&dst_buf.is_empty() {
            skipParent = PathBuf::from(&dst_file_path.parent().unwrap_or(Path::new("/")));
        }

        if !dst_buf.is_empty() {
            meta = match FileMeta::unmarshal(&dst_buf) {
                Ok(m) => m,
                Err(e) => FileMeta::new(),
            }
            // xl.load
            // meta.from(dst_buf);
        }

        meta.add_version(fi.clone())?;

        let fm_data = meta.marshal_msg()?;

        fs::write(&src_file_path, fm_data).await?;

        let no_inline = src_data_path.has_root() && fi.data.is_none() && fi.size > 0;
        if no_inline {
            self.rename_all(&src_data_path, &dst_data_path, &skipParent).await?;
        }

        self.rename_all(&src_file_path, &dst_file_path, &skipParent).await?;

        if src_volume != RUSTFS_META_MULTIPART_BUCKET {
            fs::remove_dir(&src_file_path.parent().unwrap()).await?;
        } else {
            self.delete_file(&src_volume_path, &PathBuf::from(src_file_path.parent().unwrap()))
                .await?;
        }

        Ok(())
    }

    async fn make_volumes(&self, volumes: Vec<&str>) -> Result<()> {
        for vol in volumes {
            if let Err(e) = self.make_volume(vol).await {
                match &e.downcast_ref::<DiskError>() {
                    Some(DiskError::VolumeExists) => Ok(()),
                    Some(_) => Err(e),
                    None => Err(e),
                }?;
            }
            // TODO: health check
        }
        Ok(())
    }
    async fn make_volume(&self, volume: &str) -> Result<()> {
        let p = self.get_bucket_path(&volume)?;
        match File::open(&p).await {
            Ok(_) => (),
            Err(e) => match e.kind() {
                ErrorKind::NotFound => {
                    fs::create_dir_all(&p).await?;
                    return Ok(());
                }
                _ => return Err(Error::new(e)),
            },
        }

        Err(Error::new(DiskError::VolumeExists))
    }

    async fn stat_volume(&self, volume: &str) -> Result<VolumeInfo> {
        let p = self.get_bucket_path(volume)?;

        let m = read_file_metadata(&p).await?;
        let modtime = match m.modified() {
            Ok(md) => OffsetDateTime::from(md),
            Err(_) => return Err(Error::msg("Not supported modified on this platform")),
        };

        Ok(VolumeInfo {
            name: volume.to_string(),
            created: modtime,
        })
    }

    async fn write_metadata(&self, org_volume: &str, volume: &str, path: &str, fi: FileInfo) -> Result<()> {
        let p = self.get_object_path(&volume, format!("{}/{}", path, STORAGE_FORMAT_FILE).as_str())?;

        let mut meta = FileMeta::new();
        if !fi.fresh {
            let (buf, _) = read_file_exists(&p).await?;
            if !buf.is_empty() {
                meta = FileMeta::unmarshal(&buf)?;
            }
        }

        meta.add_version(fi)?;

        let fm_data = meta.marshal_msg()?;

        write_all_internal(p, fm_data).await?;

        return Ok(());
    }

    async fn read_version(
        &self,
        org_volume: &str,
        volume: &str,
        path: &str,
        version_id: Uuid,
        opts: ReadOptions,
    ) -> Result<FileInfo> {
        let file_path = self.get_object_path(volume, path)?;
        let file_dir = self.get_bucket_path(volume)?;

        let read_data = opts.read_data;

        let (data, _) = self.read_raw(volume, file_dir, file_path, read_data).await?;

        let meta = FileMeta::unmarshal(&data)?;

        let fi = meta.into_fileinfo(volume, path, version_id, false, true)?;
        Ok(fi)
    }
    async fn read_xl(&self, volume: &str, path: &str, read_data: bool) -> Result<RawFileInfo> {
        let file_path = self.get_object_path(volume, path)?;
        let file_dir = self.get_bucket_path(volume)?;

        let (buf, _) = self.read_raw(volume, file_dir, file_path, read_data).await?;

        Ok(RawFileInfo { buf })
    }
}

// pub async fn copy_bytes<S, W>(mut stream: S, writer: &mut W) -> Result<u64>
// where
//     S: Stream<Item = Result<Bytes, StdError>> + Unpin,
//     W: AsyncWrite + Unpin,
// {
//     let mut nwritten: u64 = 0;
//     while let Some(result) = stream.next().await {
//         let bytes = match result {
//             Ok(x) => x,
//             Err(e) => return Err(Error::new(e)),
//         };
//         writer.write_all(&bytes).await?;
//         nwritten += bytes.len() as u64;
//     }
//     writer.flush().await?;
//     Ok(nwritten)
// }

// pub struct RemoteDisk {}

// impl RemoteDisk {
//     pub fn new(_ep: &Endpoint, _health_check: bool) -> Result<Self> {
//         Ok(Self {})
//     }
// }

pub(crate) struct FileWriter<'a> {
    tmp_path: PathBuf,
    dest_path: &'a Path,
    writer: BufWriter<File>,
    clean_tmp: bool,
}

impl<'a> FileWriter<'a> {
    pub(crate) fn tmp_path(&self) -> &Path {
        &self.tmp_path
    }

    pub(crate) fn dest_path(&self) -> &'a Path {
        self.dest_path
    }

    pub(crate) fn writer(&mut self) -> &mut BufWriter<File> {
        &mut self.writer
    }

    pub(crate) async fn done(mut self) -> Result<()> {
        if let Some(final_dir_path) = self.dest_path().parent() {
            fs::create_dir_all(&final_dir_path).await?;
        }

        fs::rename(&self.tmp_path, self.dest_path()).await?;
        self.clean_tmp = false;
        Ok(())
    }
}

impl<'a> Drop for FileWriter<'a> {
    fn drop(&mut self) {
        if self.clean_tmp {
            let _ = std::fs::remove_file(&self.tmp_path);
        }
    }
}

#[cfg(test)]
mod test {

    use super::*;

    #[tokio::test]
    async fn test_skip_access_checks() {
        // let arr = Vec::new();

        let vols = vec![
            RUSTFS_META_TMP_DELETED_BUCKET,
            RUSTFS_META_TMP_BUCKET,
            RUSTFS_META_MULTIPART_BUCKET,
            RUSTFS_META_BUCKET,
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

        let tmpp = disk.resolve_abs_path(Path::new(RUSTFS_META_TMP_DELETED_BUCKET)).unwrap();

        println!("ppp :{:?}", &tmpp);

        let volumes = vec!["a", "b", "c"];

        disk.make_volumes(volumes.clone()).await.unwrap();

        disk.make_volumes(volumes.clone()).await.unwrap();

        fs::remove_dir_all(&p).await.unwrap();
    }
}
