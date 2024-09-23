pub mod endpoint;
pub mod error;
pub mod format;
mod local;
mod remote;

pub const RUSTFS_META_BUCKET: &str = ".rustfs.sys";
pub const RUSTFS_META_MULTIPART_BUCKET: &str = ".rustfs.sys/multipart";
pub const RUSTFS_META_TMP_BUCKET: &str = ".rustfs.sys/tmp";
pub const RUSTFS_META_TMP_DELETED_BUCKET: &str = ".rustfs.sys/tmp/.trash";
pub const BUCKET_META_PREFIX: &str = "buckets";
pub const FORMAT_CONFIG_FILE: &str = "format.json";
const STORAGE_FORMAT_FILE: &str = "xl.meta";

use crate::{
    erasure::{ReadAt, Write},
    error::{Error, Result},
    file_meta::FileMeta,
    store_api::{FileInfo, RawFileInfo},
};
use bytes::Bytes;
use endpoint::Endpoint;
use protos::proto_gen::node_service::{node_service_client::NodeServiceClient, ReadAtRequest, WriteRequest};
use serde::{Deserialize, Serialize};
use std::{fmt::Debug, io::SeekFrom, path::PathBuf, sync::Arc};
use time::OffsetDateTime;
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
};
use tonic::{transport::Channel, Request};
use uuid::Uuid;

pub type DiskStore = Arc<Box<dyn DiskAPI>>;

pub async fn new_disk(ep: &endpoint::Endpoint, opt: &DiskOption) -> Result<DiskStore> {
    if ep.is_local {
        let s = local::LocalDisk::new(ep, opt.cleanup).await?;
        Ok(Arc::new(Box::new(s)))
    } else {
        let remote_disk = remote::RemoteDisk::new(ep, opt).await?;
        Ok(Arc::new(Box::new(remote_disk)))
    }
}

#[async_trait::async_trait]
pub trait DiskAPI: Debug + Send + Sync + 'static {
    fn to_string(&self) -> String;
    async fn is_online(&self) -> bool;
    fn is_local(&self) -> bool;
    // LastConn
    fn host_name(&self) -> String;
    fn endpoint(&self) -> Endpoint;
    async fn close(&self) -> Result<()>;
    async fn get_disk_id(&self) -> Result<Option<Uuid>>;
    async fn set_disk_id(&self, id: Option<Uuid>) -> Result<()>;

    fn path(&self) -> PathBuf;
    fn get_location(&self) -> DiskLocation;

    // Healing
    // DiskInfo
    // NSScanner

    // Volume operations.
    async fn make_volume(&self, volume: &str) -> Result<()>;
    async fn make_volumes(&self, volume: Vec<&str>) -> Result<()>;
    async fn list_volumes(&self) -> Result<Vec<VolumeInfo>>;
    async fn stat_volume(&self, volume: &str) -> Result<VolumeInfo>;
    async fn delete_volume(&self, volume: &str) -> Result<()>;

    // 并发边读边写 TODO: wr io.Writer
    async fn walk_dir(&self, opts: WalkDirOptions) -> Result<Vec<MetaCacheEntry>>;

    // Metadata operations
    async fn delete_version(
        &self,
        volume: &str,
        path: &str,
        fi: FileInfo,
        force_del_marker: bool,
        opts: DeleteOptions,
    ) -> Result<RawFileInfo>;
    async fn delete_versions(
        &self,
        volume: &str,
        versions: Vec<FileInfoVersions>,
        opts: DeleteOptions,
    ) -> Result<Vec<Option<Error>>>;
    async fn delete_paths(&self, volume: &str, paths: &[&str]) -> Result<()>;
    async fn write_metadata(&self, org_volume: &str, volume: &str, path: &str, fi: FileInfo) -> Result<()>;
    async fn update_metadata(&self, volume: &str, path: &str, fi: FileInfo, opts: UpdateMetadataOpts);
    async fn read_version(
        &self,
        org_volume: &str,
        volume: &str,
        path: &str,
        version_id: &str,
        opts: &ReadOptions,
    ) -> Result<FileInfo>;
    async fn read_xl(&self, volume: &str, path: &str, read_data: bool) -> Result<RawFileInfo>;
    async fn rename_data(
        &self,
        src_volume: &str,
        src_path: &str,
        file_info: FileInfo,
        dst_volume: &str,
        dst_path: &str,
    ) -> Result<RenameDataResp>;

    // File operations.
    // 读目录下的所有文件、目录
    async fn list_dir(&self, origvolume: &str, volume: &str, dir_path: &str, count: i32) -> Result<Vec<String>>;
    async fn read_file(&self, volume: &str, path: &str) -> Result<FileReader>;
    async fn append_file(&self, volume: &str, path: &str) -> Result<FileWriter>;
    async fn create_file(&self, origvolume: &str, volume: &str, path: &str, file_size: usize) -> Result<FileWriter>;
    // ReadFileStream
    async fn rename_file(&self, src_volume: &str, src_path: &str, dst_volume: &str, dst_path: &str) -> Result<()>;
    // RenamePart
    // CheckParts
    async fn delete(&self, volume: &str, path: &str, opt: DeleteOptions) -> Result<()>;
    // VerifyFile
    // StatInfoFile
    // ReadParts
    async fn read_multiple(&self, req: ReadMultipleReq) -> Result<Vec<ReadMultipleResp>>;
    // CleanAbandonedData
    async fn write_all(&self, volume: &str, path: &str, data: Vec<u8>) -> Result<()>;
    async fn read_all(&self, volume: &str, path: &str) -> Result<Bytes>;
    // GetDiskLoc
}

pub struct UpdateMetadataOpts {
    pub no_persistence: bool,
}

pub struct DiskLocation {
    pub pool_idx: Option<usize>,
    pub set_idx: Option<usize>,
    pub disk_idx: Option<usize>,
}

impl DiskLocation {
    pub fn valid(&self) -> bool {
        self.pool_idx.is_some() && self.set_idx.is_some() && self.disk_idx.is_some()
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct FileInfoVersions {
    // Name of the volume.
    pub volume: String,

    // Name of the file.
    pub name: String,

    // Represents the latest mod time of the
    // latest version.
    pub latest_mod_time: Option<OffsetDateTime>,

    pub versions: Vec<FileInfo>,
    pub free_versions: Vec<FileInfo>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
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

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct MetaCacheEntry {
    // name is the full name of the object including prefixes
    pub name: String,
    // Metadata. If none is present it is not an object but only a prefix.
    // Entries without metadata will only be present in non-recursive scans.
    pub metadata: Vec<u8>,

    // cached contains the metadata if decoded.
    cached: Option<FileMeta>,

    // Indicates the entry can be reused and only one reference to metadata is expected.
    _reusable: bool,
}

impl MetaCacheEntry {
    pub fn marshal_msg(&self) -> Result<Vec<u8>> {
        let mut wr = Vec::new();
        rmp::encode::write_bool(&mut wr, true)?;

        rmp::encode::write_str(&mut wr, &self.name)?;

        rmp::encode::write_bin(&mut wr, &self.metadata)?;

        Ok(wr)
    }
    pub fn is_dir(&self) -> bool {
        self.metadata.is_empty() && self.name.ends_with("/")
    }
    pub fn is_object(&self) -> bool {
        !self.metadata.is_empty()
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub fn to_fileinfo(&self, bucket: &str) -> Result<Option<FileInfo>> {
        if self.is_dir() {
            return Ok(Some(FileInfo {
                volume: bucket.to_owned(),
                name: self.name.clone(),
                ..Default::default()
            }));
        }

        if self.cached.is_some() {
            let fm = self.cached.as_ref().unwrap();
            if fm.versions.is_empty() {
                return Ok(Some(FileInfo {
                    volume: bucket.to_owned(),
                    name: self.name.clone(),
                    deleted: true,
                    is_latest: true,
                    mod_time: Some(OffsetDateTime::UNIX_EPOCH),
                    ..Default::default()
                }));
            }

            let fi = fm.into_fileinfo(bucket, self.name.as_str(), "", false, false)?;

            return Ok(Some(fi));
        }

        let mut fm = FileMeta::new();
        fm.unmarshal_msg(&self.metadata)?;

        let fi = fm.into_fileinfo(bucket, self.name.as_str(), "", false, false)?;

        return Ok(Some(fi));
    }
}

#[derive(Debug, Default)]
pub struct DiskOption {
    pub cleanup: bool,
    pub health_check: bool,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct RenameDataResp {
    pub old_data_dir: Option<Uuid>,
    pub sign: Option<Vec<u8>>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DeleteOptions {
    pub recursive: bool,
    pub immediate: bool,
    pub undo_write: bool,
    pub old_data_dir: Option<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadMultipleReq {
    pub bucket: String,
    pub prefix: String,
    pub files: Vec<String>,
    pub max_size: usize,
    pub metadata_only: bool,
    pub abort404: bool,
    pub max_results: usize,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
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

#[derive(Debug, Deserialize, Serialize)]
pub struct VolumeInfo {
    pub name: String,
    pub created: Option<OffsetDateTime>,
}

#[derive(Deserialize, Serialize)]
pub struct ReadOptions {
    pub read_data: bool,
    pub healing: bool,
}

// pub struct FileWriter {
//     pub inner: Pin<Box<dyn AsyncWrite + Send + Sync + 'static>>,
// }

// impl AsyncWrite for FileWriter {
//     fn poll_write(
//         mut self: Pin<&mut Self>,
//         cx: &mut std::task::Context<'_>,
//         buf: &[u8],
//     ) -> std::task::Poll<std::result::Result<usize, std::io::Error>> {
//         Pin::new(&mut self.inner).poll_write(cx, buf)
//     }

//     fn poll_flush(
//         mut self: Pin<&mut Self>,
//         cx: &mut std::task::Context<'_>,
//     ) -> std::task::Poll<std::result::Result<(), std::io::Error>> {
//         Pin::new(&mut self.inner).poll_flush(cx)
//     }

//     fn poll_shutdown(
//         mut self: Pin<&mut Self>,
//         cx: &mut std::task::Context<'_>,
//     ) -> std::task::Poll<std::result::Result<(), std::io::Error>> {
//         Pin::new(&mut self.inner).poll_shutdown(cx)
//     }
// }

// impl FileWriter {
//     pub fn new<W>(inner: W) -> Self
//     where
//         W: AsyncWrite + Send + Sync + 'static,
//     {
//         Self { inner: Box::pin(inner) }
//     }
// }

pub enum FileWriter {
    Local(LocalFileWriter),
    Remote(RemoteFileWriter),
}

#[async_trait::async_trait]
impl Write for FileWriter {
    async fn write(&mut self, buf: &[u8]) -> Result<()> {
        match self {
            Self::Local(local_file_writer) => local_file_writer.write(buf).await,
            Self::Remote(remote_file_writer) => remote_file_writer.write(buf).await,
        }
    }
}

pub struct LocalFileWriter {
    pub inner: File,
}

impl LocalFileWriter {
    pub fn new(inner: File) -> Self {
        Self { inner }
    }
}

#[async_trait::async_trait]
impl Write for LocalFileWriter {
    async fn write(&mut self, buf: &[u8]) -> Result<()> {
        self.inner.write(buf).await?;
        self.inner.flush().await?;

        Ok(())
    }
}

pub struct RemoteFileWriter {
    pub root: PathBuf,
    pub volume: String,
    pub path: String,
    pub is_append: bool,
    client: NodeServiceClient<Channel>,
}

impl RemoteFileWriter {
    pub fn new(root: PathBuf, volume: String, path: String, is_append: bool, client: NodeServiceClient<Channel>) -> Self {
        Self {
            root,
            volume,
            path,
            is_append,
            client,
        }
    }
}

#[async_trait::async_trait]
impl Write for RemoteFileWriter {
    async fn write(&mut self, buf: &[u8]) -> Result<()> {
        let request = Request::new(WriteRequest {
            disk: self.root.to_string_lossy().to_string(),
            volume: self.volume.to_string(),
            path: self.path.to_string(),
            is_append: self.is_append,
            data: buf.to_vec(),
        });
        let _response = self.client.write(request).await?.into_inner();
        Ok(())
    }
}

#[derive(Debug)]
pub enum FileReader {
    Local(LocalFileReader),
    Remote(RemoteFileReader),
}

#[async_trait::async_trait]
impl ReadAt for FileReader {
    async fn read_at(&mut self, offset: usize, length: usize) -> Result<(Vec<u8>, usize)> {
        match self {
            Self::Local(local_file_writer) => local_file_writer.read_at(offset, length).await,
            Self::Remote(remote_file_writer) => remote_file_writer.read_at(offset, length).await,
        }
    }
}

#[derive(Debug)]
pub struct LocalFileReader {
    pub inner: File,
}

impl LocalFileReader {
    pub fn new(inner: File) -> Self {
        Self { inner }
    }
}

#[async_trait::async_trait]
impl ReadAt for LocalFileReader {
    async fn read_at(&mut self, offset: usize, length: usize) -> Result<(Vec<u8>, usize)> {
        self.inner.seek(SeekFrom::Start(offset as u64)).await?;

        let mut buffer = vec![0; length];

        let bytes_read = self.inner.read(&mut buffer).await?;

        buffer.truncate(bytes_read);

        Ok((buffer, bytes_read))
    }
}

#[derive(Debug)]
pub struct RemoteFileReader {
    pub root: PathBuf,
    pub volume: String,
    pub path: String,
    client: NodeServiceClient<Channel>,
}

impl RemoteFileReader {
    pub fn new(root: PathBuf, volume: String, path: String, client: NodeServiceClient<Channel>) -> Self {
        Self {
            root,
            volume,
            path,
            client,
        }
    }
}

#[async_trait::async_trait]
impl ReadAt for RemoteFileReader {
    async fn read_at(&mut self, offset: usize, length: usize) -> Result<(Vec<u8>, usize)> {
        let request = Request::new(ReadAtRequest {
            disk: self.root.to_string_lossy().to_string(),
            volume: self.volume.to_string(),
            path: self.path.to_string(),
            offset: offset.try_into().unwrap(),
            length: length.try_into().unwrap(),
        });
        let response = self.client.read_at(request).await?.into_inner();

        Ok((response.data, response.read_size.try_into().unwrap()))
    }
}
