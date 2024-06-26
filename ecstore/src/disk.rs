use std::{
    fs::Metadata,
    path::{Path, PathBuf},
};

use anyhow::Error;
use bytes::Bytes;
use futures::future::join_all;
use path_absolutize::Absolutize;
use time::OffsetDateTime;
use tokio::fs;
use uuid::Uuid;

use crate::{
    endpoint::{Endpoint, Endpoints},
    format::{DistributionAlgoVersion, FormatV3},
};

pub const RUSTFS_META_BUCKET: &str = ".rustfs.sys";
pub const FORMAT_CONFIG_FILE: &str = "format.json";

pub struct DiskOption {
    pub cleanup: bool,
    pub health_check: bool,
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

        Ok(disk)
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
    let meta = match fs::metadata(&p).await {
        Ok(meta) => Some(meta),
        Err(e) => match e.kind() {
            std::io::ErrorKind::NotFound => None,
            _ => return Err(e.into()),
        },
    };

    let mut data = Vec::new();
    if meta.is_some() {
        data = fs::read(&p).await?;
    }

    Ok((data, meta))
}

#[async_trait::async_trait]
impl DiskAPI for LocalDisk {
    #[must_use]
    async fn read_all(&self, volume: &str, path: &str) -> Result<Vec<u8>, Error> {
        let p = self.get_object_path(&volume, &path)?;
        if p.exists() {}
        let content = fs::read(p).await?;

        unimplemented!()
    }
}

pub struct RemoteDisk {}

impl RemoteDisk {
    pub fn new(ep: &Endpoint, health_check: bool) -> Result<Self, Error> {
        Ok(Self {})
    }
}

#[async_trait::async_trait]
pub trait DiskAPI {
    async fn read_all(&self, volume: &str, path: &str) -> Result<Vec<u8>, Error>;
}

#[derive(Debug, thiserror::Error)]
pub enum DiskError {
    #[error("disk not found")]
    DiskNotFound,
    #[error("InconsistentDisk")]
    InconsistentDisk,
}
