use std::fmt::Debug;

use anyhow::{Error, Result};
use bytes::Bytes;
use time::OffsetDateTime;
use tokio::io::DuplexStream;
use uuid::Uuid;

use crate::store_api::{FileInfo, RawFileInfo};

#[async_trait::async_trait]
pub trait DiskAPI: Debug + Send + Sync + 'static {
    fn is_local(&self) -> bool;

    async fn read_all(&self, volume: &str, path: &str) -> Result<Bytes>;
    async fn write_all(&self, volume: &str, path: &str, data: Bytes) -> Result<()>;
    async fn rename_file(&self, src_volume: &str, src_path: &str, dst_volume: &str, dst_path: &str) -> Result<()>;
    async fn create_file(&self, origvolume: &str, volume: &str, path: &str, fileSize: usize, r: DuplexStream) -> Result<()>;
    async fn rename_data(
        &self,
        src_volume: &str,
        src_path: &str,
        file_info: &FileInfo,
        dst_volume: &str,
        dst_path: &str,
    ) -> Result<()>;

    async fn make_volumes(&self, volume: Vec<&str>) -> Result<()>;
    async fn make_volume(&self, volume: &str) -> Result<()>;
    async fn stat_volume(&self, volume: &str) -> Result<VolumeInfo>;

    async fn write_metadata(&self, org_volume: &str, volume: &str, path: &str, fi: FileInfo) -> Result<()>;
    async fn read_version(
        &self,
        org_volume: &str,
        volume: &str,
        path: &str,
        version_id: Uuid,
        opts: ReadOptions,
    ) -> Result<FileInfo>;
    async fn read_xl(&self, volume: &str, path: &str, read_data: bool) -> Result<RawFileInfo>;
}

pub struct VolumeInfo {
    pub name: String,
    pub created: OffsetDateTime,
}

pub struct ReadOptions {
    pub read_data: bool,
    // pub healing: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum DiskError {
    #[error("file not found")]
    FileNotFound,

    #[error("file version not found")]
    FileVersionNotFound,

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
