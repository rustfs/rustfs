use std::{
    fs::Metadata,
    path::{Path, PathBuf},
};

use anyhow::Error;
use futures::future::join_all;
use path_absolutize::Absolutize;
use time::OffsetDateTime;
use tokio::fs::{self, File};
use tokio::io::ErrorKind;
use uuid::Uuid;

use crate::{
    endpoint::{Endpoint, Endpoints},
    format::{DistributionAlgoVersion, FormatV3},
};

pub const RUSTFS_META_BUCKET: &str = ".rustfs.sys";
pub const BUCKET_META_PREFIX: &str = "buckets";
pub const FORMAT_CONFIG_FILE: &str = "format.json";

pub struct DiskOption {
    pub cleanup: bool,
    pub _health_check: bool,
}

pub async fn new_disk(ep: &Endpoint, opt: &DiskOption) -> Result<impl DiskAPI, Error> {
    if ep.is_local {
        Ok(LocalDisk::new(ep, opt.cleanup).await?)
    } else {
        unimplemented!()
        // Ok(Disk::Remote(RemoteDisk::new(ep, opt.health_check)?))
    }
}

pub async fn init_disks(
    eps: &Endpoints,
    opt: &DiskOption,
) -> (Vec<Option<impl DiskAPI>>, Vec<Option<Error>>) {
    let mut futures = Vec::with_capacity(eps.len());

    for ep in eps.iter() {
        futures.push(new_disk(ep, opt));
    }

    let mut storages = Vec::with_capacity(eps.len());
    let mut errors = Vec::with_capacity(eps.len());

    let results = join_all(futures).await;
    for result in results {
        match result {
            Ok(s) => {
                storages.push(Some(s));
                errors.push(None);
            }
            Err(e) => {
                storages.push(None);
                errors.push(Some(e));
            }
        }
    }

    (storages, errors)
}

// pub async fn load_format(&self, heal: bool) -> Result<FormatV3, Error> {
//     unimplemented!()
// }

pub struct LocalDisk {
    root: PathBuf,
    id: Uuid,
    format_data: Vec<u8>,
    format_meta: Option<Metadata>,
    format_path: PathBuf,
    format_legacy: bool,
    format_last_check: OffsetDateTime,
}

impl LocalDisk {
    pub async fn new(ep: &Endpoint, cleanup: bool) -> Result<Self, Error> {
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

    async fn make_meta_volumes(&self) -> Result<(), Error> {
        let buckets = format!("{}/{}", RUSTFS_META_BUCKET, BUCKET_META_PREFIX);
        let multipart = format!("{}/{}", RUSTFS_META_BUCKET, "multipart");
        let config = format!("{}/{}", RUSTFS_META_BUCKET, "config");
        let tmp = format!("{}/{}", RUSTFS_META_BUCKET, "tmp");
        let defaults = vec![
            buckets.as_str(),
            multipart.as_str(),
            config.as_str(),
            tmp.as_str(),
        ];

        self.make_volumes(defaults).await
    }

    pub fn resolve_abs_path(&self, path: impl AsRef<Path>) -> Result<PathBuf, Error> {
        Ok(path.as_ref().absolutize_virtually(&self.root)?.into_owned())
    }

    pub fn get_object_path(&self, bucket: &str, key: &str) -> Result<PathBuf, Error> {
        let dir = Path::new(&bucket);
        let file_path = Path::new(&key);
        self.resolve_abs_path(dir.join(file_path))
    }

    pub fn get_bucket_path(&self, bucket: &str) -> Result<PathBuf, Error> {
        let dir = Path::new(&bucket);
        self.resolve_abs_path(dir)
    }

    // pub async fn load_format(&self) -> Result<Option<FormatV3>, Error> {
    //     let p = self.get_object_path(RUSTFS_META_BUCKET, FORMAT_CONFIG_FILE)?;
    //     let content = fs::read(&p).await?;

    //     unimplemented!()
    // }
}

// 过滤 std::io::ErrorKind::NotFound
pub async fn read_file_exists(
    path: impl AsRef<Path>,
) -> Result<(Vec<u8>, Option<Metadata>), Error> {
    let p = path.as_ref();
    let (data, meta) = match read_file_all(&p).await {
        Ok((data, meta)) => (data, Some(meta)),
        Err(e) => {
            if is_err(&e, &DiskError::FileNotFound) {
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

pub async fn read_file_all(path: impl AsRef<Path>) -> Result<(Vec<u8>, Metadata), Error> {
    let p = path.as_ref();
    let meta = fs::metadata(&p).await.map_err(|e| match e.kind() {
        ErrorKind::NotFound => Error::new(DiskError::FileNotFound),
        ErrorKind::PermissionDenied => Error::new(DiskError::FileAccessDenied),
        _ => Error::new(e),
    })?;

    let data = fs::read(&p).await?;

    Ok((data, meta))
}

#[async_trait::async_trait]
impl DiskAPI for LocalDisk {
    #[must_use]
    async fn read_all(&self, volume: &str, path: &str) -> Result<Vec<u8>, Error> {
        let p = self.get_object_path(&volume, &path)?;
        let (data, _) = read_file_all(&p).await?;

        Ok(data)
    }

    async fn make_volumes(&self, volumes: Vec<&str>) -> Result<(), Error> {
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
    async fn make_volume(&self, volume: &str) -> Result<(), Error> {
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

pub struct RemoteDisk {}

impl RemoteDisk {
    pub fn new(_ep: &Endpoint, _health_check: bool) -> Result<Self, Error> {
        Ok(Self {})
    }
}

#[async_trait::async_trait]
pub trait DiskAPI {
    async fn read_all(&self, volume: &str, path: &str) -> Result<Vec<u8>, Error>;

    async fn make_volumes(&self, volume: Vec<&str>) -> Result<(), Error>;
    async fn make_volume(&self, volume: &str) -> Result<(), Error>;
}

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
}

impl PartialEq for DiskError {
    fn eq(&self, other: &Self) -> bool {
        core::mem::discriminant(self) == core::mem::discriminant(other)
    }
}

pub fn check_disk_fatal_errs(errs: &Vec<Option<Error>>) -> Result<(), Error> {
    if count_errs(errs, &DiskError::UnsupportedDisk) == errs.len() {
        return Err(Error::new(DiskError::UnsupportedDisk));
    }

    // if count_errs(errs, &DiskError::DiskAccessDenied) == errs.len() {
    //     return Err(Error::new(DiskError::DiskAccessDenied));
    // }

    if count_errs(errs, &DiskError::FileAccessDenied) == errs.len() {
        return Err(Error::new(DiskError::FileAccessDenied));
    }

    // if count_errs(errs, &DiskError::FaultyDisk) == errs.len() {
    //     return Err(Error::new(DiskError::FaultyDisk));
    // }

    if count_errs(errs, &DiskError::DiskNotDir) == errs.len() {
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
            true
        })
        .count();
}

pub fn is_err(err: &Error, disk_err: &DiskError) -> bool {
    let cast = err.downcast_ref::<DiskError>();
    if cast.is_none() {
        return false;
    }

    let e = cast.unwrap();

    e == disk_err
}

pub fn match_err(err: Error, matchs: Vec<DiskError>) -> bool {
    let cast = err.downcast_ref::<DiskError>();
    if cast.is_none() {
        return false;
    }

    let e = cast.unwrap();

    for i in matchs.iter() {
        if e == i {
            return true;
        }
    }

    return false;
}

#[cfg(test)]
mod test {

    use super::*;

    #[tokio::test]
    async fn test_check_errs() {}

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

        let volumes = vec!["a", "b", "c"];

        disk.make_volumes(volumes.clone()).await.unwrap();

        disk.make_volumes(volumes.clone()).await.unwrap();

        fs::remove_dir_all(&p).await.unwrap();
    }
}
