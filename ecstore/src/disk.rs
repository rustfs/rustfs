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
use uuid::Uuid;

use crate::{
    disk_api::DiskAPI,
    endpoint::{Endpoint, Endpoints},
    format::{DistributionAlgoVersion, FormatV3},
};

pub const RUSTFS_META_BUCKET: &str = ".rustfs.sys";
pub const RUSTFS_META_MULTIPART_BUCKET: &str = ".rustfs.sys/multipart";
pub const RUSTFS_META_TMP_BUCKET: &str = ".rustfs.sys/tmp";
pub const RUSTFS_META_TMP_DELETED_BUCKET: &str = ".rustfs.sys/tmp/.trash";
pub const BUCKET_META_PREFIX: &str = "buckets";
pub const FORMAT_CONFIG_FILE: &str = "format.json";

pub type DiskStore = Arc<Box<dyn DiskAPI>>;

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
    let mut futures = Vec::with_capacity(eps.len());

    for ep in eps.iter() {
        futures.push(new_disk(ep, opt));
    }

    let mut res = Vec::with_capacity(eps.len());
    let mut errors = Vec::with_capacity(eps.len());

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
    pub format_legacy: bool,
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
        let mut format_legacy = false;
        let mut format_last_check = OffsetDateTime::UNIX_EPOCH;

        if !format_data.is_empty() {
            let s = format_data.as_slice();
            let fm = FormatV3::try_from(s)?;
            let (set_idx, disk_idx) = fm.find_disk_index_by_disk_id(fm.erasure.this)?;

            if set_idx as i32 != ep.set_idx || disk_idx as i32 != ep.disk_idx {
                return Err(Error::new(DiskError::InconsistentDisk));
            }

            id = fm.erasure.this;
            format_legacy = fm.erasure.distribution_algo == DistributionAlgoVersion::V1;
            format_last_check = OffsetDateTime::now_utc();
        }

        let disk = Self {
            root,
            id,
            format_meta,
            format_data: format_data,
            format_path,
            format_legacy,
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
        let file = File::create(&path).await?;
        let writer = BufWriter::new(file);
        Ok(FileWriter {
            tmp_path,
            dest_path: path,
            writer,
            clean_tmp: true,
        })
    }
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

        // create top dir if not exists
        fs::create_dir_all(&p.parent().unwrap_or_else(|| Path::new("."))).await?;

        fs::write(&p, data).await?;
        Ok(())
    }

    async fn rename_file(&self, src_volume: &str, src_path: &str, dst_volume: &str, dst_path: &str) -> Result<()> {
        if !skip_access_checks(&src_volume) {
            check_volume_exists(&src_volume).await?;
        }
        if !skip_access_checks(&dst_volume) {
            check_volume_exists(&dst_volume).await?;
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

    async fn CreateFile(&self, origvolume: &str, volume: &str, path: &str, fileSize: usize, mut r: DuplexStream) -> Result<()> {
        let fpath = self.get_object_path(volume, path)?;

        let mut writer = self.prepare_file_write(&fpath).await?;

        io::copy(&mut r, writer.writer()).await?;

        writer.done().await?;

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

#[derive(Debug, thiserror::Error)]
pub enum DiskError {
    #[error("file not found")]
    FileNotFound,
    #[error("disk not found")]
    DiskNotFound,

    #[error("disk access denied")]
    FileAccessDenied,

    #[error("InconsistentDisk")]
    InconsistentDisk,

    #[error("volume already exists")]
    VolumeExists,

    #[error("unformatted disk error")]
    UnformattedDisk,

    #[error("unsupport disk")]
    UnsupportedDisk,

    #[error("disk not a dir")]
    DiskNotDir,

    #[error("volume not found")]
    VolumeNotFound,
}

impl DiskError {
    pub fn check_disk_fatal_errs(errs: &Vec<Option<Error>>) -> Result<()> {
        println!("errs: {:?}", errs);

        if Self::count_errs(errs, &DiskError::UnsupportedDisk) == errs.len() {
            return Err(Error::new(DiskError::UnsupportedDisk));
        }

        // if count_errs(errs, &DiskError::DiskAccessDenied) == errs.len() {
        //     return Err(Error::new(DiskError::DiskAccessDenied));
        // }

        if Self::count_errs(errs, &DiskError::FileAccessDenied) == errs.len() {
            return Err(Error::new(DiskError::FileAccessDenied));
        }

        // if count_errs(errs, &DiskError::FaultyDisk) == errs.len() {
        //     return Err(Error::new(DiskError::FaultyDisk));
        // }

        if Self::count_errs(errs, &DiskError::DiskNotDir) == errs.len() {
            return Err(Error::new(DiskError::DiskNotDir));
        }

        // if count_errs(errs, &DiskError::XLBackend) == errs.len() {
        //     return Err(Error::new(DiskError::XLBackend));
        // }
        Ok(())
    }
    pub fn count_errs(errs: &Vec<Option<Error>>, err: &DiskError) -> usize {
        return errs
            .iter()
            .filter(|&e| {
                if e.is_some() {
                    let e = e.as_ref().unwrap();
                    let cast = e.downcast_ref::<DiskError>();
                    if cast.is_some() {
                        let cast = cast.unwrap();
                        return cast == err;
                    }
                }
                false
            })
            .count();
    }

    pub fn quorum_unformatted_disks(errs: &Vec<Option<Error>>) -> bool {
        Self::count_errs(errs, &DiskError::UnformattedDisk) >= (errs.len() / 2) + 1
    }

    pub fn is_err(err: &Error, disk_err: &DiskError) -> bool {
        let cast = err.downcast_ref::<DiskError>();
        if cast.is_none() {
            return false;
        }

        let e = cast.unwrap();

        e == disk_err
    }

    // pub fn match_err(err: Error, matchs: Vec<DiskError>) -> bool {
    //     let cast = err.downcast_ref::<DiskError>();
    //     if cast.is_none() {
    //         return false;
    //     }

    //     let e = cast.unwrap();

    //     for i in matchs.iter() {
    //         if e == i {
    //             return true;
    //         }
    //     }

    //     return false;
    // }
}

impl PartialEq for DiskError {
    fn eq(&self, other: &Self) -> bool {
        core::mem::discriminant(self) == core::mem::discriminant(other)
    }
}

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

        let ep = match Endpoint::new(&p) {
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
