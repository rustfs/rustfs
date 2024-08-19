pub mod endpoint;
pub mod error;
pub mod format;
mod local;

pub const RUSTFS_META_BUCKET: &str = ".rustfs.sys";
pub const RUSTFS_META_MULTIPART_BUCKET: &str = ".rustfs.sys/multipart";
pub const RUSTFS_META_TMP_BUCKET: &str = ".rustfs.sys/tmp";
pub const RUSTFS_META_TMP_DELETED_BUCKET: &str = ".rustfs.sys/tmp/.trash";
pub const BUCKET_META_PREFIX: &str = "buckets";
pub const FORMAT_CONFIG_FILE: &str = "format.json";
const STORAGE_FORMAT_FILE: &str = "xl.meta";

use crate::{
    erasure::ReadAt,
    error::Result,
    file_meta::FileMeta,
    store_api::{FileInfo, RawFileInfo},
};
use bytes::Bytes;
use std::{fmt::Debug, io::SeekFrom, pin::Pin, sync::Arc};
use time::OffsetDateTime;
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt, AsyncWrite, DuplexStream},
};
use uuid::Uuid;

pub type DiskStore = Arc<Box<dyn DiskAPI>>;

pub async fn new_disk(ep: &endpoint::Endpoint, opt: &DiskOption) -> Result<DiskStore> {
    if ep.is_local {
        let s = local::LocalDisk::new(ep, opt.cleanup).await?;
        Ok(Arc::new(Box::new(s)))
    } else {
        let _ = opt.health_check;
        unimplemented!()
    }
}

#[async_trait::async_trait]
pub trait DiskAPI: Debug + Send + Sync + 'static {
    fn is_local(&self) -> bool;
    fn id(&self) -> Uuid;

    async fn delete(&self, volume: &str, path: &str, opt: DeleteOptions) -> Result<()>;
    async fn read_all(&self, volume: &str, path: &str) -> Result<Bytes>;
    async fn write_all(&self, volume: &str, path: &str, data: Vec<u8>) -> Result<()>;
    async fn rename_file(&self, src_volume: &str, src_path: &str, dst_volume: &str, dst_path: &str) -> Result<()>;
    async fn create_file(&self, origvolume: &str, volume: &str, path: &str, file_size: usize) -> Result<FileWriter>;
    async fn append_file(&self, volume: &str, path: &str) -> Result<FileWriter>;
    async fn read_file(&self, volume: &str, path: &str) -> Result<FileReader>;
    // 读目录下的所有文件、目录
    async fn list_dir(&self, origvolume: &str, volume: &str, dir_path: &str, count: i32) -> Result<Vec<String>>;
    // 并发边读边写 TODO: wr io.Writer
    async fn walk_dir(&self, opts: WalkDirOptions, wr: Arc<DuplexStream>) -> Result<()>;
    async fn rename_data(
        &self,
        src_volume: &str,
        src_path: &str,
        file_info: FileInfo,
        dst_volume: &str,
        dst_path: &str,
    ) -> Result<RenameDataResp>;

    async fn make_volumes(&self, volume: Vec<&str>) -> Result<()>;
    async fn delete_volume(&self, volume: &str) -> Result<()>;
    async fn list_volumes(&self) -> Result<Vec<VolumeInfo>>;
    async fn make_volume(&self, volume: &str) -> Result<()>;
    async fn stat_volume(&self, volume: &str) -> Result<VolumeInfo>;

    async fn write_metadata(&self, org_volume: &str, volume: &str, path: &str, fi: FileInfo) -> Result<()>;
    async fn read_version(
        &self,
        org_volume: &str,
        volume: &str,
        path: &str,
        version_id: &str,
        opts: &ReadOptions,
    ) -> Result<FileInfo>;
    async fn read_xl(&self, volume: &str, path: &str, read_data: bool) -> Result<RawFileInfo>;
    async fn read_multiple(&self, req: ReadMultipleReq) -> Result<Vec<ReadMultipleResp>>;
}

#[derive(Debug, Default, Clone)]
pub struct WalkDirOptions {
    // Bucket to scanner
    pub bucket: String,
    // Directory inside the bucket.
    pub base_dir: String,
    // Do a full recursive scan.
    pub recursive: bool,

    // ReportNotFound will return errFileNotFound if all disks reports the BaseDir cannot be found.
    pub report_notfound: bool,

    // FilterPrefix will only return results with given prefix within folder.
    // Should never contain a slash.
    pub filter_prefix: String,

    // ForwardTo will forward to the given object path.
    pub forward_to: String,

    // Limit the number of returned objects if > 0.
    pub limit: i32,

    // DiskID contains the disk ID of the disk.
    // Leave empty to not check disk ID.
    pub disk_id: String,
}

#[derive(Debug, Default)]
pub struct MetaCacheEntry {
    // name is the full name of the object including prefixes
    name: String,
    // Metadata. If none is present it is not an object but only a prefix.
    // Entries without metadata will only be present in non-recursive scans.
    metadata: Vec<u8>,

    // cached contains the metadata if decoded.
    cached: Option<FileMeta>,

    // Indicates the entry can be reused and only one reference to metadata is expected.
    reusable: bool,
}

pub struct DiskOption {
    pub cleanup: bool,
    pub health_check: bool,
}

pub struct RenameDataResp {
    pub old_data_dir: Option<Uuid>,
}

#[derive(Debug, Clone, Default)]
pub struct DeleteOptions {
    pub recursive: bool,
    pub immediate: bool,
}

#[derive(Debug, Clone)]
pub struct ReadMultipleReq {
    pub bucket: String,
    pub prefix: String,
    pub files: Vec<String>,
    pub max_size: usize,
    pub metadata_only: bool,
    pub abort404: bool,
    pub max_results: usize,
}

#[derive(Debug, Clone, Default)]
pub struct ReadMultipleResp {
    pub bucket: String,
    pub prefix: String,
    pub file: String,
    pub exists: bool,
    pub error: String,
    pub data: Vec<u8>,
    pub mod_time: Option<OffsetDateTime>,
}

// impl Default for ReadMultipleResp {
//     fn default() -> Self {
//         Self {
//             bucket: String::new(),
//             prefix: String::new(),
//             file: String::new(),
//             exists: false,
//             error: String::new(),
//             data: Vec::new(),
//             mod_time: OffsetDateTime::UNIX_EPOCH,
//         }
//     }
// }

pub struct VolumeInfo {
    pub name: String,
    pub created: Option<OffsetDateTime>,
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

#[derive(Debug)]
pub struct FileReader {
    pub inner: File,
}

impl FileReader {
    pub fn new(inner: File) -> Self {
        Self { inner }
    }
}

impl ReadAt for FileReader {
    async fn read_at(&mut self, offset: usize, length: usize) -> Result<(Vec<u8>, usize)> {
        self.inner.seek(SeekFrom::Start(offset as u64)).await?;

        let mut buffer = vec![0; length];

        let bytes_read = self.inner.read(&mut buffer).await?;

        buffer.truncate(bytes_read);

        Ok((buffer, bytes_read))
    }
}
