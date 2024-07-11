use std::{fmt::Debug, pin::Pin};

use anyhow::{Error, Result};
use bytes::Bytes;
use time::OffsetDateTime;
use tokio::io::AsyncWrite;
use uuid::Uuid;

use crate::store_api::{FileInfo, RawFileInfo};

#[async_trait::async_trait]
pub trait DiskAPI: Debug + Send + Sync + 'static {
    fn is_local(&self) -> bool;
    fn id(&self) -> Uuid;

    async fn read_all(&self, volume: &str, path: &str) -> Result<Bytes>;
    async fn write_all(&self, volume: &str, path: &str, data: Vec<u8>) -> Result<()>;
    async fn rename_file(&self, src_volume: &str, src_path: &str, dst_volume: &str, dst_path: &str) -> Result<()>;
    async fn create_file(&self, origvolume: &str, volume: &str, path: &str, file_size: usize) -> Result<FileWriter>;
    async fn append_file(&self, volume: &str, path: &str) -> Result<FileWriter>;
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
        opts: &ReadOptions,
    ) -> Result<FileInfo>;
    async fn read_xl(&self, volume: &str, path: &str, read_data: bool) -> Result<RawFileInfo>;
    async fn read_multiple(&self, req: ReadMultipleReq) -> Result<Vec<ReadMultipleResp>>;
}

pub struct ReadMultipleReq {
    pub bucket: String,
    pub prefix: String,
    pub files: Vec<String>,
    pub max_size: usize,
    pub metadata_only: bool,
    pub abort404: bool,
    pub max_results: usize,
}

pub struct ReadMultipleResp {
    pub bucket: String,
    pub prefix: String,
    pub file: String,
    pub exists: bool,
    pub error: String,
    pub data: Vec<u8>,
    pub mod_time: OffsetDateTime,
}

impl Default for ReadMultipleResp {
    fn default() -> Self {
        Self {
            bucket: Default::default(),
            prefix: Default::default(),
            file: Default::default(),
            exists: Default::default(),
            error: Default::default(),
            data: Default::default(),
            mod_time: OffsetDateTime::UNIX_EPOCH,
        }
    }
}

pub struct VolumeInfo {
    pub name: String,
    pub created: OffsetDateTime,
}

pub struct ReadOptions {
    pub read_data: bool,
    pub healing: bool,
}

pub struct FileWriter {
    pub inner: Pin<Box<dyn AsyncWrite + Send + Sync + 'static>>,
}

impl AsyncWrite for FileWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::result::Result<usize, std::io::Error>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::result::Result<(), std::io::Error>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::result::Result<(), std::io::Error>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

impl FileWriter {
    pub fn new<W>(inner: W) -> Self
    where
        W: AsyncWrite + Send + Sync + 'static,
    {
        Self { inner: Box::pin(inner) }
    }
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
