pub mod endpoint;
pub mod error;
pub mod format;
pub mod local;
pub mod os;
pub mod remote;

pub const RUSTFS_META_BUCKET: &str = ".rustfs.sys";
pub const RUSTFS_META_MULTIPART_BUCKET: &str = ".rustfs.sys/multipart";
pub const RUSTFS_META_TMP_BUCKET: &str = ".rustfs.sys/tmp";
pub const RUSTFS_META_TMP_DELETED_BUCKET: &str = ".rustfs.sys/tmp/.trash";
pub const BUCKET_META_PREFIX: &str = "buckets";
pub const FORMAT_CONFIG_FILE: &str = "format.json";
pub const STORAGE_FORMAT_FILE: &str = "xl.meta";
pub const STORAGE_FORMAT_FILE_BACKUP: &str = "xl.meta.bkp";

use crate::{
    bucket::{metadata_sys::get_versioning_config, versioning::VersioningApi},
    erasure::Writer,
    error::{Error, Result},
    file_meta::{merge_file_meta_versions, FileMeta, FileMetaShallowVersion, VersionType},
    heal::{
        data_scanner::ShouldSleepFn,
        data_usage_cache::{DataUsageCache, DataUsageEntry},
        heal_commands::{HealScanMode, HealingTracker},
    },
    store_api::{FileInfo, ObjectInfo, RawFileInfo},
};
use endpoint::Endpoint;
use error::DiskError;
use futures::StreamExt;
use local::LocalDisk;
use madmin::info_commands::DiskMetrics;
use protos::proto_gen::node_service::{
    node_service_client::NodeServiceClient, ReadAtRequest, ReadAtResponse, WriteRequest, WriteResponse,
};
use remote::RemoteDisk;
use serde::{Deserialize, Serialize};
use std::{
    any::Any,
    cmp::Ordering,
    fmt::Debug,
    io::{Cursor, SeekFrom},
    path::PathBuf,
    sync::Arc,
};
use time::OffsetDateTime;
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt, AsyncWrite, AsyncWriteExt},
    sync::mpsc::{self, Sender},
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{service::interceptor::InterceptedService, transport::Channel, Request, Status, Streaming};
use tracing::info;
use tracing::warn;
use uuid::Uuid;

pub type DiskStore = Arc<Disk>;

#[derive(Debug)]
pub enum Disk {
    Local(Box<LocalDisk>),
    Remote(Box<RemoteDisk>),
}

#[async_trait::async_trait]
impl DiskAPI for Disk {
    fn to_string(&self) -> String {
        match self {
            Disk::Local(local_disk) => local_disk.to_string(),
            Disk::Remote(remote_disk) => remote_disk.to_string(),
        }
    }

    fn is_local(&self) -> bool {
        match self {
            Disk::Local(local_disk) => local_disk.is_local(),
            Disk::Remote(remote_disk) => remote_disk.is_local(),
        }
    }

    fn host_name(&self) -> String {
        match self {
            Disk::Local(local_disk) => local_disk.host_name(),
            Disk::Remote(remote_disk) => remote_disk.host_name(),
        }
    }
    async fn is_online(&self) -> bool {
        match self {
            Disk::Local(local_disk) => local_disk.is_online().await,
            Disk::Remote(remote_disk) => remote_disk.is_online().await,
        }
    }
    fn endpoint(&self) -> Endpoint {
        match self {
            Disk::Local(local_disk) => local_disk.endpoint(),
            Disk::Remote(remote_disk) => remote_disk.endpoint(),
        }
    }
    async fn close(&self) -> Result<()> {
        match self {
            Disk::Local(local_disk) => local_disk.close().await,
            Disk::Remote(remote_disk) => remote_disk.close().await,
        }
    }
    fn path(&self) -> PathBuf {
        match self {
            Disk::Local(local_disk) => local_disk.path(),
            Disk::Remote(remote_disk) => remote_disk.path(),
        }
    }

    fn get_disk_location(&self) -> DiskLocation {
        match self {
            Disk::Local(local_disk) => local_disk.get_disk_location(),
            Disk::Remote(remote_disk) => remote_disk.get_disk_location(),
        }
    }

    async fn get_disk_id(&self) -> Result<Option<Uuid>> {
        match self {
            Disk::Local(local_disk) => local_disk.get_disk_id().await,
            Disk::Remote(remote_disk) => remote_disk.get_disk_id().await,
        }
    }
    async fn set_disk_id(&self, id: Option<Uuid>) -> Result<()> {
        match self {
            Disk::Local(local_disk) => local_disk.set_disk_id(id).await,
            Disk::Remote(remote_disk) => remote_disk.set_disk_id(id).await,
        }
    }

    async fn read_all(&self, volume: &str, path: &str) -> Result<Vec<u8>> {
        match self {
            Disk::Local(local_disk) => local_disk.read_all(volume, path).await,
            Disk::Remote(remote_disk) => remote_disk.read_all(volume, path).await,
        }
    }

    async fn write_all(&self, volume: &str, path: &str, data: Vec<u8>) -> Result<()> {
        match self {
            Disk::Local(local_disk) => local_disk.write_all(volume, path, data).await,
            Disk::Remote(remote_disk) => remote_disk.write_all(volume, path, data).await,
        }
    }

    async fn delete(&self, volume: &str, path: &str, opt: DeleteOptions) -> Result<()> {
        match self {
            Disk::Local(local_disk) => local_disk.delete(volume, path, opt).await,
            Disk::Remote(remote_disk) => remote_disk.delete(volume, path, opt).await,
        }
    }

    async fn verify_file(&self, volume: &str, path: &str, fi: &FileInfo) -> Result<CheckPartsResp> {
        match self {
            Disk::Local(local_disk) => local_disk.verify_file(volume, path, fi).await,
            Disk::Remote(remote_disk) => remote_disk.verify_file(volume, path, fi).await,
        }
    }

    async fn check_parts(&self, volume: &str, path: &str, fi: &FileInfo) -> Result<CheckPartsResp> {
        match self {
            Disk::Local(local_disk) => local_disk.check_parts(volume, path, fi).await,
            Disk::Remote(remote_disk) => remote_disk.check_parts(volume, path, fi).await,
        }
    }

    async fn rename_part(&self, src_volume: &str, src_path: &str, dst_volume: &str, dst_path: &str, meta: Vec<u8>) -> Result<()> {
        match self {
            Disk::Local(local_disk) => local_disk.rename_part(src_volume, src_path, dst_volume, dst_path, meta).await,
            Disk::Remote(remote_disk) => {
                remote_disk
                    .rename_part(src_volume, src_path, dst_volume, dst_path, meta)
                    .await
            }
        }
    }
    async fn rename_file(&self, src_volume: &str, src_path: &str, dst_volume: &str, dst_path: &str) -> Result<()> {
        match self {
            Disk::Local(local_disk) => local_disk.rename_file(src_volume, src_path, dst_volume, dst_path).await,
            Disk::Remote(remote_disk) => remote_disk.rename_file(src_volume, src_path, dst_volume, dst_path).await,
        }
    }

    async fn create_file(&self, _origvolume: &str, volume: &str, path: &str, _file_size: usize) -> Result<FileWriter> {
        match self {
            Disk::Local(local_disk) => local_disk.create_file(_origvolume, volume, path, _file_size).await,
            Disk::Remote(remote_disk) => remote_disk.create_file(_origvolume, volume, path, _file_size).await,
        }
    }

    async fn append_file(&self, volume: &str, path: &str) -> Result<FileWriter> {
        match self {
            Disk::Local(local_disk) => local_disk.append_file(volume, path).await,
            Disk::Remote(remote_disk) => remote_disk.append_file(volume, path).await,
        }
    }

    async fn read_file(&self, volume: &str, path: &str) -> Result<FileReader> {
        match self {
            Disk::Local(local_disk) => local_disk.read_file(volume, path).await,
            Disk::Remote(remote_disk) => remote_disk.read_file(volume, path).await,
        }
    }

    async fn list_dir(&self, _origvolume: &str, volume: &str, _dir_path: &str, _count: i32) -> Result<Vec<String>> {
        match self {
            Disk::Local(local_disk) => local_disk.list_dir(_origvolume, volume, _dir_path, _count).await,
            Disk::Remote(remote_disk) => remote_disk.list_dir(_origvolume, volume, _dir_path, _count).await,
        }
    }

    async fn walk_dir<W: AsyncWrite + Unpin + Send>(&self, opts: WalkDirOptions, wr: &mut W) -> Result<()> {
        match self {
            Disk::Local(local_disk) => local_disk.walk_dir(opts, wr).await,
            Disk::Remote(remote_disk) => remote_disk.walk_dir(opts, wr).await,
        }
    }

    async fn rename_data(
        &self,
        src_volume: &str,
        src_path: &str,
        fi: FileInfo,
        dst_volume: &str,
        dst_path: &str,
    ) -> Result<RenameDataResp> {
        match self {
            Disk::Local(local_disk) => local_disk.rename_data(src_volume, src_path, fi, dst_volume, dst_path).await,
            Disk::Remote(remote_disk) => remote_disk.rename_data(src_volume, src_path, fi, dst_volume, dst_path).await,
        }
    }

    async fn make_volumes(&self, volumes: Vec<&str>) -> Result<()> {
        match self {
            Disk::Local(local_disk) => local_disk.make_volumes(volumes).await,
            Disk::Remote(remote_disk) => remote_disk.make_volumes(volumes).await,
        }
    }

    async fn make_volume(&self, volume: &str) -> Result<()> {
        match self {
            Disk::Local(local_disk) => local_disk.make_volume(volume).await,
            Disk::Remote(remote_disk) => remote_disk.make_volume(volume).await,
        }
    }

    async fn list_volumes(&self) -> Result<Vec<VolumeInfo>> {
        match self {
            Disk::Local(local_disk) => local_disk.list_volumes().await,
            Disk::Remote(remote_disk) => remote_disk.list_volumes().await,
        }
    }

    async fn stat_volume(&self, volume: &str) -> Result<VolumeInfo> {
        match self {
            Disk::Local(local_disk) => local_disk.stat_volume(volume).await,
            Disk::Remote(remote_disk) => remote_disk.stat_volume(volume).await,
        }
    }

    async fn delete_paths(&self, volume: &str, paths: &[&str]) -> Result<()> {
        match self {
            Disk::Local(local_disk) => local_disk.delete_paths(volume, paths).await,
            Disk::Remote(remote_disk) => remote_disk.delete_paths(volume, paths).await,
        }
    }
    async fn update_metadata(&self, volume: &str, path: &str, fi: FileInfo, opts: &UpdateMetadataOpts) -> Result<()> {
        match self {
            Disk::Local(local_disk) => local_disk.update_metadata(volume, path, fi, opts).await,
            Disk::Remote(remote_disk) => remote_disk.update_metadata(volume, path, fi, opts).await,
        }
    }

    async fn write_metadata(&self, _org_volume: &str, volume: &str, path: &str, fi: FileInfo) -> Result<()> {
        match self {
            Disk::Local(local_disk) => local_disk.write_metadata(_org_volume, volume, path, fi).await,
            Disk::Remote(remote_disk) => remote_disk.write_metadata(_org_volume, volume, path, fi).await,
        }
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
        match self {
            Disk::Local(local_disk) => local_disk.read_version(_org_volume, volume, path, version_id, opts).await,
            Disk::Remote(remote_disk) => remote_disk.read_version(_org_volume, volume, path, version_id, opts).await,
        }
    }

    async fn read_xl(&self, volume: &str, path: &str, read_data: bool) -> Result<RawFileInfo> {
        match self {
            Disk::Local(local_disk) => local_disk.read_xl(volume, path, read_data).await,
            Disk::Remote(remote_disk) => remote_disk.read_xl(volume, path, read_data).await,
        }
    }
    async fn delete_version(
        &self,
        volume: &str,
        path: &str,
        fi: FileInfo,
        force_del_marker: bool,
        opts: DeleteOptions,
    ) -> Result<()> {
        match self {
            Disk::Local(local_disk) => local_disk.delete_version(volume, path, fi, force_del_marker, opts).await,
            Disk::Remote(remote_disk) => remote_disk.delete_version(volume, path, fi, force_del_marker, opts).await,
        }
    }
    async fn delete_versions(
        &self,
        volume: &str,
        versions: Vec<FileInfoVersions>,
        opts: DeleteOptions,
    ) -> Result<Vec<Option<Error>>> {
        match self {
            Disk::Local(local_disk) => local_disk.delete_versions(volume, versions, opts).await,
            Disk::Remote(remote_disk) => remote_disk.delete_versions(volume, versions, opts).await,
        }
    }

    async fn read_multiple(&self, req: ReadMultipleReq) -> Result<Vec<ReadMultipleResp>> {
        match self {
            Disk::Local(local_disk) => local_disk.read_multiple(req).await,
            Disk::Remote(remote_disk) => remote_disk.read_multiple(req).await,
        }
    }

    async fn delete_volume(&self, volume: &str) -> Result<()> {
        info!("delete_volume, volume: {}", volume);
        match self {
            Disk::Local(local_disk) => local_disk.delete_volume(volume).await,
            Disk::Remote(remote_disk) => remote_disk.delete_volume(volume).await,
        }
    }

    async fn disk_info(&self, opts: &DiskInfoOptions) -> Result<DiskInfo> {
        match self {
            Disk::Local(local_disk) => local_disk.disk_info(opts).await,
            Disk::Remote(remote_disk) => remote_disk.disk_info(opts).await,
        }
    }

    async fn ns_scanner(
        &self,
        cache: &DataUsageCache,
        updates: Sender<DataUsageEntry>,
        scan_mode: HealScanMode,
        we_sleep: ShouldSleepFn,
    ) -> Result<DataUsageCache> {
        info!("ns_scanner");
        match self {
            Disk::Local(local_disk) => local_disk.ns_scanner(cache, updates, scan_mode, we_sleep).await,
            Disk::Remote(remote_disk) => remote_disk.ns_scanner(cache, updates, scan_mode, we_sleep).await,
        }
    }

    async fn healing(&self) -> Option<HealingTracker> {
        match self {
            Disk::Local(local_disk) => local_disk.healing().await,
            Disk::Remote(remote_disk) => remote_disk.healing().await,
        }
    }
}

pub async fn new_disk(ep: &endpoint::Endpoint, opt: &DiskOption) -> Result<DiskStore> {
    if ep.is_local {
        let s = local::LocalDisk::new(ep, opt.cleanup).await?;
        Ok(Arc::new(Disk::Local(Box::new(s))))
    } else {
        let remote_disk = remote::RemoteDisk::new(ep, opt).await?;
        Ok(Arc::new(Disk::Remote(Box::new(remote_disk))))
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
    fn get_disk_location(&self) -> DiskLocation;

    // Healing
    // DiskInfo
    // NSScanner

    // Volume operations.
    async fn make_volume(&self, volume: &str) -> Result<()>;
    async fn make_volumes(&self, volume: Vec<&str>) -> Result<()>;
    async fn list_volumes(&self) -> Result<Vec<VolumeInfo>>;
    async fn stat_volume(&self, volume: &str) -> Result<VolumeInfo>;
    async fn delete_volume(&self, volume: &str) -> Result<()>;

    // 并发边读边写 w <- MetaCacheEntry
    async fn walk_dir<W: AsyncWrite + Unpin + Send>(&self, opts: WalkDirOptions, wr: &mut W) -> Result<()>;

    // Metadata operations
    async fn delete_version(
        &self,
        volume: &str,
        path: &str,
        fi: FileInfo,
        force_del_marker: bool,
        opts: DeleteOptions,
    ) -> Result<()>;
    async fn delete_versions(
        &self,
        volume: &str,
        versions: Vec<FileInfoVersions>,
        opts: DeleteOptions,
    ) -> Result<Vec<Option<Error>>>;
    async fn delete_paths(&self, volume: &str, paths: &[&str]) -> Result<()>;
    async fn write_metadata(&self, org_volume: &str, volume: &str, path: &str, fi: FileInfo) -> Result<()>;
    async fn update_metadata(&self, volume: &str, path: &str, fi: FileInfo, opts: &UpdateMetadataOpts) -> Result<()>;
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
    async fn rename_part(&self, src_volume: &str, src_path: &str, dst_volume: &str, dst_path: &str, meta: Vec<u8>) -> Result<()>;
    async fn delete(&self, volume: &str, path: &str, opt: DeleteOptions) -> Result<()>;
    // VerifyFile
    async fn verify_file(&self, volume: &str, path: &str, fi: &FileInfo) -> Result<CheckPartsResp>;
    // CheckParts
    async fn check_parts(&self, volume: &str, path: &str, fi: &FileInfo) -> Result<CheckPartsResp>;
    // StatInfoFile
    // ReadParts
    async fn read_multiple(&self, req: ReadMultipleReq) -> Result<Vec<ReadMultipleResp>>;
    // CleanAbandonedData
    async fn write_all(&self, volume: &str, path: &str, data: Vec<u8>) -> Result<()>;
    async fn read_all(&self, volume: &str, path: &str) -> Result<Vec<u8>>;
    async fn disk_info(&self, opts: &DiskInfoOptions) -> Result<DiskInfo>;
    async fn ns_scanner(
        &self,
        cache: &DataUsageCache,
        updates: Sender<DataUsageEntry>,
        scan_mode: HealScanMode,
        we_sleep: ShouldSleepFn,
    ) -> Result<DataUsageCache>;
    async fn healing(&self) -> Option<HealingTracker>;
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct CheckPartsResp {
    pub results: Vec<usize>,
}

#[derive(Debug, Serialize, Deserialize, Default)]
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

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct DiskInfoOptions {
    pub disk_id: String,
    pub metrics: bool,
    pub noop: bool,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct DiskInfo {
    pub total: u64,
    pub free: u64,
    pub used: u64,
    pub used_inodes: u64,
    pub free_inodes: u64,
    pub major: u64,
    pub minor: u64,
    pub nr_requests: u64,
    pub fs_type: String,
    pub root_disk: bool,
    pub healing: bool,
    pub scanning: bool,
    pub endpoint: String,
    pub mount_path: String,
    pub id: String,
    pub rotational: bool,
    pub metrics: DiskMetrics,
    pub error: String,
}

#[derive(Clone, Debug, Default)]
pub struct Info {
    pub total: u64,
    pub free: u64,
    pub used: u64,
    pub files: u64,
    pub ffree: u64,
    pub fstype: String,
    pub major: u64,
    pub minor: u64,
    pub name: String,
    pub rotational: bool,
    pub nrrequests: u64,
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

impl FileInfoVersions {
    pub fn find_version_index(&self, v: &str) -> Option<usize> {
        if v.is_empty() {
            return None;
        }

        let vid = Uuid::parse_str(v).unwrap_or(Uuid::nil());

        for ver in self.versions.iter() {}

        self.versions.iter().position(|v| v.version_id == Some(vid))
    }
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

#[derive(Clone, Debug, Default)]
pub struct MetadataResolutionParams {
    pub dir_quorum: usize,
    pub obj_quorum: usize,
    pub requested_versions: usize,
    pub bucket: String,
    pub strict: bool,
    pub candidates: Vec<Vec<FileMetaShallowVersion>>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct MetaCacheEntry {
    // name is the full name of the object including prefixes
    pub name: String,
    // Metadata. If none is present it is not an object but only a prefix.
    // Entries without metadata will only be present in non-recursive scans.
    pub metadata: Vec<u8>,

    // cached contains the metadata if decoded.
    pub cached: Option<FileMeta>,

    // Indicates the entry can be reused and only one reference to metadata is expected.
    pub reusable: bool,
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
        self.metadata.is_empty() && self.name.ends_with('/')
    }
    pub fn is_object(&self) -> bool {
        !self.metadata.is_empty()
    }

    pub fn is_latest_deletemarker(&mut self) -> bool {
        if let Some(cached) = &self.cached {
            if cached.versions.is_empty() {
                return true;
            }

            return cached.versions[0].header.version_type == VersionType::Delete;
        }

        if !FileMeta::is_xl2_v1_format(&self.metadata) {
            return false;
        }

        match FileMeta::check_xl2_v1(&self.metadata) {
            Ok((meta, _, _)) => {
                if !meta.is_empty() {
                    // TODO: IsLatestDeleteMarker
                }
            }
            Err(_) => return true,
        }

        match self.xl_meta() {
            Ok(res) => {
                if res.versions.is_empty() {
                    return true;
                }
                res.versions[0].header.version_type == VersionType::Delete
            }
            Err(_) => true,
        }
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub fn to_fileinfo(&self, bucket: &str) -> Result<FileInfo> {
        if self.is_dir() {
            return Ok(FileInfo {
                volume: bucket.to_owned(),
                name: self.name.clone(),
                ..Default::default()
            });
        }

        if self.cached.is_some() {
            let fm = self.cached.as_ref().unwrap();
            if fm.versions.is_empty() {
                return Ok(FileInfo {
                    volume: bucket.to_owned(),
                    name: self.name.clone(),
                    deleted: true,
                    is_latest: true,
                    mod_time: Some(OffsetDateTime::UNIX_EPOCH),
                    ..Default::default()
                });
            }

            let fi = fm.into_fileinfo(bucket, self.name.as_str(), "", false, false)?;

            return Ok(fi);
        }

        let mut fm = FileMeta::new();
        fm.unmarshal_msg(&self.metadata)?;

        let fi = fm.into_fileinfo(bucket, self.name.as_str(), "", false, false)?;

        return Ok(fi);
    }

    pub fn file_info_versions(&self, bucket: &str) -> Result<FileInfoVersions> {
        if self.is_dir() {
            return Ok(FileInfoVersions {
                volume: bucket.to_string(),
                name: self.name.clone(),
                versions: vec![FileInfo {
                    volume: bucket.to_string(),
                    name: self.name.clone(),
                    ..Default::default()
                }],
                ..Default::default()
            });
        }

        let mut fm = FileMeta::new();
        fm.unmarshal_msg(&self.metadata)?;

        fm.into_file_info_versions(bucket, self.name.as_str(), false)
    }

    pub fn matches(&self, other: &MetaCacheEntry, strict: bool) -> Result<(Option<MetaCacheEntry>, bool)> {
        let mut prefer = None;
        if self.name != other.name {
            if self.name < other.name {
                return Ok((Some(self.clone()), false));
            }
            return Ok((Some(other.clone()), false));
        }

        if other.is_dir() || self.is_dir() {
            if self.is_dir() {
                return Ok((Some(self.clone()), other.is_dir()));
            }

            return Ok((Some(other.clone()), other.is_dir() == self.is_dir()));
        }
        let self_vers = match &self.cached {
            Some(file_meta) => file_meta.clone(),
            None => FileMeta::load(&self.metadata)?,
        };
        let other_vers = match &other.cached {
            Some(file_meta) => file_meta.clone(),
            None => FileMeta::load(&other.metadata)?,
        };

        if self_vers.versions.len() != other_vers.versions.len() {
            match self_vers.lastest_mod_time().cmp(&other_vers.lastest_mod_time()) {
                Ordering::Greater => {
                    return Ok((Some(self.clone()), false));
                }
                Ordering::Less => {
                    return Ok((Some(self.clone()), false));
                }
                _ => {}
            }

            if self_vers.versions.len() > other_vers.versions.len() {
                return Ok((Some(self.clone()), false));
            }
            return Ok((Some(self.clone()), false));
        }

        for (s_version, o_version) in self_vers.versions.iter().zip(other_vers.versions.iter()) {
            if s_version.header != o_version.header {
                if s_version.header.has_ec() != o_version.header.has_ec() {
                    // One version has EC and the other doesn't - may have been written later.
                    // Compare without considering EC.
                    let (mut a, mut b) = (s_version.header.clone(), o_version.header.clone());
                    (a.ec_n, a.ec_m, b.ec_n, b.ec_m) = (0, 0, 0, 0);
                    if a == b {
                        continue;
                    }
                }

                if !strict && s_version.header.matches_not_strict(&o_version.header) {
                    if prefer.is_none() {
                        if s_version.header.sorts_before(&o_version.header) {
                            prefer = Some(self.clone());
                        } else {
                            prefer = Some(other.clone());
                        }
                    }

                    continue;
                }

                if prefer.is_some() {
                    return Ok((prefer, false));
                }

                if s_version.header.sorts_before(&o_version.header) {
                    return Ok((Some(self.clone()), false));
                }

                return Ok((Some(other.clone()), false));
            }
        }

        if prefer.is_none() {
            prefer = Some(self.clone());
        }

        Ok((prefer, true))
    }

    pub fn xl_meta(&mut self) -> Result<FileMeta> {
        if self.is_dir() {
            return Err(Error::new(DiskError::FileNotFound));
        }

        if let Some(meta) = &self.cached {
            Ok(meta.clone())
        } else {
            if self.metadata.is_empty() {
                return Err(Error::new(DiskError::FileNotFound));
            }

            let meta = FileMeta::load(&self.metadata)?;

            self.cached = Some(meta.clone());

            Ok(meta)
        }
    }
}

#[derive(Debug)]
pub struct MetaCacheEntries(pub Vec<Option<MetaCacheEntry>>);

impl MetaCacheEntries {
    #[allow(clippy::should_implement_trait)]
    pub fn as_ref(&self) -> &[Option<MetaCacheEntry>] {
        &self.0
    }
    pub fn resolve(&self, mut params: MetadataResolutionParams) -> Result<Option<MetaCacheEntry>> {
        if self.0.is_empty() {
            return Ok(None);
        }

        let mut dir_exists = 0;
        let mut selected = None;

        params.candidates.clear();
        let mut objs_agree = 0;
        let mut objs_valid = 0;

        for entry in self.0.iter().flatten() {
            if entry.name.is_empty() {
                continue;
            }
            if entry.is_dir() {
                dir_exists += 1;
                selected = Some(entry.clone());
                continue;
            }

            objs_valid += 1;

            match &entry.cached {
                Some(file_meta) => {
                    params.candidates.push(file_meta.versions.clone());
                }
                None => {
                    params.candidates.push(FileMeta::load(&entry.metadata)?.versions);
                }
            }

            if selected.is_none() {
                selected = Some(entry.clone());
                objs_agree = 1;
                continue;
            }

            if let (Some(prefer), true) = entry.matches(selected.as_ref().unwrap(), params.strict)? {
                selected = Some(prefer);
                objs_agree += 1;
                continue;
            }
        }

        // Return dir entries, if enough...
        if selected.is_some() && selected.as_ref().unwrap().is_dir() && dir_exists >= params.dir_quorum {
            return Ok(selected);
        }
        // If we would never be able to reach read quorum.
        if objs_valid < params.obj_quorum {
            return Ok(None);
        }
        // If all objects agree.
        if selected.is_some() && objs_agree == objs_valid {
            return Ok(selected);
        }
        // If cached is nil we shall skip the entry.
        if selected.is_none() || (selected.is_some() && selected.as_ref().unwrap().cached.is_none()) {
            return Ok(None);
        }
        // Merge if we have disagreement.
        // Create a new merged result.
        selected = Some(MetaCacheEntry {
            name: selected.as_ref().unwrap().name.clone(),
            cached: Some(FileMeta {
                meta_ver: selected.as_ref().unwrap().cached.as_ref().unwrap().meta_ver,
                ..Default::default()
            }),
            reusable: true,
            ..Default::default()
        });

        selected.as_mut().unwrap().cached.as_mut().unwrap().versions =
            merge_file_meta_versions(params.obj_quorum, params.strict, params.requested_versions, &params.candidates);
        if selected.as_ref().unwrap().cached.as_ref().unwrap().versions.is_empty() {
            return Ok(None);
        }

        selected.as_mut().unwrap().metadata = selected.as_ref().unwrap().cached.as_ref().unwrap().marshal_msg()?;

        Ok(selected)
    }

    pub fn first_found(&self) -> (Option<MetaCacheEntry>, usize) {
        (self.0.iter().find(|x| x.is_some()).cloned().unwrap_or_default(), self.0.len())
    }
}

#[derive(Debug)]
pub struct MetaCacheEntriesSorted {
    pub o: MetaCacheEntries,
    // pub list_id: String,
    // pub reuse: bool,
    // pub lastSkippedEntry: String,
}

impl MetaCacheEntriesSorted {
    pub fn entries(&self) -> Vec<MetaCacheEntry> {
        let entries: Vec<MetaCacheEntry> = self.o.0.iter().flatten().cloned().collect();
        entries
    }
    pub async fn file_infos(&self, bucket: &str, prefix: &str, delimiter: &str) -> Vec<ObjectInfo> {
        let vcfg = get_versioning_config(bucket).await.ok();
        let mut objects = Vec::with_capacity(self.o.as_ref().len());
        let mut prev_prefix = "";
        for entry in self.o.as_ref().iter().flatten() {
            if entry.is_object() {
                if !delimiter.is_empty() {
                    if let Some(idx) = entry.name.trim_start_matches(prefix).find(delimiter) {
                        let idx = prefix.len() + idx + delimiter.len();
                        if let Some(curr_prefix) = entry.name.get(0..idx) {
                            if curr_prefix == prev_prefix {
                                continue;
                            }

                            prev_prefix = curr_prefix;

                            objects.push(ObjectInfo {
                                is_dir: true,
                                bucket: bucket.to_owned(),
                                name: curr_prefix.to_owned(),
                                ..Default::default()
                            });
                        }
                        continue;
                    }
                }

                if let Ok(fi) = entry.to_fileinfo(bucket) {
                    // TODO:VersionPurgeStatus
                    let versioned = vcfg.clone().map(|v| v.0.versioned(&entry.name)).unwrap_or_default();
                    objects.push(fi.to_object_info(bucket, &entry.name, versioned));
                }
                continue;
            }

            if entry.is_dir() {
                if delimiter.is_empty() {
                    continue;
                }

                if let Some(idx) = entry.name.trim_start_matches(prefix).find(delimiter) {
                    let idx = prefix.len() + idx + delimiter.len();
                    if let Some(curr_prefix) = entry.name.get(0..idx) {
                        if curr_prefix == prev_prefix {
                            continue;
                        }

                        prev_prefix = curr_prefix;

                        objects.push(ObjectInfo {
                            is_dir: true,
                            bucket: bucket.to_owned(),
                            name: curr_prefix.to_owned(),
                            ..Default::default()
                        });
                    }
                }
            }
        }

        objects
    }

    pub async fn file_info_versions(&self, bucket: &str, prefix: &str, delimiter: &str, after_v: &str) -> Vec<ObjectInfo> {
        let vcfg = get_versioning_config(bucket).await.ok();
        let mut objects = Vec::with_capacity(self.o.as_ref().len());
        let mut prev_prefix = "";
        let mut after_v = after_v;
        for entry in self.o.as_ref().iter().flatten() {
            if entry.is_object() {
                if !delimiter.is_empty() {
                    if let Some(idx) = entry.name.trim_start_matches(prefix).find(delimiter) {
                        let idx = prefix.len() + idx + delimiter.len();
                        if let Some(curr_prefix) = entry.name.get(0..idx) {
                            if curr_prefix == prev_prefix {
                                continue;
                            }

                            prev_prefix = curr_prefix;

                            objects.push(ObjectInfo {
                                is_dir: true,
                                bucket: bucket.to_owned(),
                                name: curr_prefix.to_owned(),
                                ..Default::default()
                            });
                        }
                        continue;
                    }
                }

                let mut fiv = match entry.file_info_versions(bucket) {
                    Ok(res) => res,
                    Err(_err) => {
                        //
                        continue;
                    }
                };

                let fi_versions = 'c: {
                    if !after_v.is_empty() {
                        if let Some(idx) = fiv.find_version_index(after_v) {
                            after_v = "";
                            break 'c fiv.versions.split_off(idx + 1);
                        }

                        after_v = "";
                        break 'c fiv.versions;
                    } else {
                        break 'c fiv.versions;
                    }
                };

                for fi in fi_versions.into_iter() {
                    // VersionPurgeStatus

                    let versioned = vcfg.clone().map(|v| v.0.versioned(&entry.name)).unwrap_or_default();
                    objects.push(fi.to_object_info(bucket, &entry.name, versioned));
                }

                continue;
            }

            if entry.is_dir() {
                if delimiter.is_empty() {
                    continue;
                }

                if let Some(idx) = entry.name.trim_start_matches(prefix).find(delimiter) {
                    let idx = prefix.len() + idx + delimiter.len();
                    if let Some(curr_prefix) = entry.name.get(0..idx) {
                        if curr_prefix == prev_prefix {
                            continue;
                        }

                        prev_prefix = curr_prefix;

                        objects.push(ObjectInfo {
                            is_dir: true,
                            bucket: bucket.to_owned(),
                            name: curr_prefix.to_owned(),
                            ..Default::default()
                        });
                    }
                }
            }
        }

        objects
    }
}

#[derive(Clone, Debug, Default)]
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

#[derive(Deserialize, Serialize, Debug)]
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

#[derive(Debug)]
pub enum FileWriter {
    Local(LocalFileWriter),
    Remote(RemoteFileWriter),
    Buffer(BufferWriter),
}

#[async_trait::async_trait]
impl Writer for FileWriter {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn write(&mut self, buf: &[u8]) -> Result<()> {
        match self {
            Self::Local(writer) => writer.write(buf).await,
            Self::Remote(writter) => writter.write(buf).await,
            Self::Buffer(writer) => writer.write(buf).await,
        }
    }
}

#[derive(Debug)]
pub struct BufferWriter {
    pub inner: Vec<u8>,
}

impl BufferWriter {
    pub fn new(inner: Vec<u8>) -> Self {
        Self { inner }
    }
    #[allow(clippy::should_implement_trait)]
    pub fn as_ref(&self) -> &[u8] {
        self.inner.as_ref()
    }
}

#[async_trait::async_trait]
impl Writer for BufferWriter {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn write(&mut self, buf: &[u8]) -> Result<()> {
        let _ = self.inner.write(buf).await?;
        self.inner.flush().await?;

        Ok(())
    }
}

#[derive(Debug)]
pub struct LocalFileWriter {
    pub inner: File,
}

impl LocalFileWriter {
    pub fn new(inner: File) -> Self {
        Self { inner }
    }
}

#[async_trait::async_trait]
impl Writer for LocalFileWriter {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn write(&mut self, buf: &[u8]) -> Result<()> {
        let _ = self.inner.write(buf).await?;
        self.inner.flush().await?;

        Ok(())
    }
}

type NodeClient = NodeServiceClient<
    InterceptedService<Channel, Box<dyn Fn(Request<()>) -> Result<Request<()>, Status> + Send + Sync + 'static>>,
>;

#[derive(Debug)]
pub struct RemoteFileWriter {
    pub endpoint: Endpoint,
    pub volume: String,
    pub path: String,
    pub is_append: bool,
    tx: Sender<WriteRequest>,
    resp_stream: Streaming<WriteResponse>,
}

impl RemoteFileWriter {
    pub async fn new(endpoint: Endpoint, volume: String, path: String, is_append: bool, mut client: NodeClient) -> Result<Self> {
        let (tx, rx) = mpsc::channel(128);
        let in_stream = ReceiverStream::new(rx);

        let response = client.write_stream(in_stream).await.unwrap();

        let resp_stream = response.into_inner();

        Ok(Self {
            endpoint,
            volume,
            path,
            is_append,
            tx,
            resp_stream,
        })
    }
}

#[async_trait::async_trait]
impl Writer for RemoteFileWriter {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn write(&mut self, buf: &[u8]) -> Result<()> {
        let request = WriteRequest {
            disk: self.endpoint.to_string(),
            volume: self.volume.to_string(),
            path: self.path.to_string(),
            is_append: self.is_append,
            data: buf.to_vec(),
        };
        self.tx.send(request).await?;

        if let Some(resp) = self.resp_stream.next().await {
            // match resp {
            //     Ok(resp) => {
            //         if resp.success {
            //             info!("write stream success");
            //         } else {
            //             info!("write stream failed: {}", resp.error_info.unwrap_or("".to_string()));
            //         }
            //     }
            //     Err(_err) => {

            //     }
            // }
            let resp = resp?;
            if resp.success {
                info!("write stream success");
            } else {
                let error_info = resp.error_info.unwrap_or("".to_string());
                info!("write stream failed: {}", error_info);
                return Err(Error::from_string(error_info));
            }
        } else {
            let error_info = "can not get response";
            info!("write stream failed: {}", error_info);
            return Err(Error::from_string(error_info));
        }

        Ok(())
    }
}

#[async_trait::async_trait]
pub trait Reader {
    async fn read_at(&mut self, offset: usize, buf: &mut [u8]) -> Result<usize>;
    async fn seek(&mut self, offset: usize) -> Result<()>;
    async fn read_exact(&mut self, buf: &mut [u8]) -> Result<usize>;
}

#[derive(Debug)]
pub enum FileReader {
    Local(LocalFileReader),
    Remote(RemoteFileReader),
    Buffer(BufferReader),
}

#[async_trait::async_trait]
impl Reader for FileReader {
    async fn read_at(&mut self, offset: usize, buf: &mut [u8]) -> Result<usize> {
        match self {
            Self::Local(reader) => reader.read_at(offset, buf).await,
            Self::Remote(reader) => reader.read_at(offset, buf).await,
            Self::Buffer(reader) => reader.read_at(offset, buf).await,
        }
    }
    async fn seek(&mut self, offset: usize) -> Result<()> {
        match self {
            Self::Local(reader) => reader.seek(offset).await,
            Self::Remote(reader) => reader.seek(offset).await,
            Self::Buffer(reader) => reader.seek(offset).await,
        }
    }
    async fn read_exact(&mut self, buf: &mut [u8]) -> Result<usize> {
        match self {
            Self::Local(reader) => reader.read_exact(buf).await,
            Self::Remote(reader) => reader.read_exact(buf).await,
            Self::Buffer(reader) => reader.read_exact(buf).await,
        }
    }
}

#[derive(Debug)]
pub struct BufferReader {
    pub inner: Cursor<Vec<u8>>,
    pos: usize,
}

impl BufferReader {
    pub fn new(inner: Vec<u8>) -> Self {
        Self {
            inner: Cursor::new(inner),
            pos: 0,
        }
    }
}

#[async_trait::async_trait]
impl Reader for BufferReader {
    #[tracing::instrument(level = "debug", skip(self, buf))]
    async fn read_at(&mut self, offset: usize, buf: &mut [u8]) -> Result<usize> {
        self.seek(offset).await?;
        self.read_exact(buf).await
    }
    #[tracing::instrument(level = "debug", skip(self))]
    async fn seek(&mut self, offset: usize) -> Result<()> {
        if self.pos != offset {
            self.inner.set_position(offset as u64);
        }

        Ok(())
    }
    #[tracing::instrument(level = "debug", skip(self))]
    async fn read_exact(&mut self, buf: &mut [u8]) -> Result<usize> {
        let bytes_read = self.inner.read_exact(buf).await?;
        self.pos += buf.len();
        Ok(bytes_read)
    }
}

#[derive(Debug)]
pub struct LocalFileReader {
    pub inner: File,
    pos: usize,
}

impl LocalFileReader {
    pub fn new(inner: File) -> Self {
        Self { inner, pos: 0 }
    }
}

#[async_trait::async_trait]
impl Reader for LocalFileReader {
    #[tracing::instrument(level = "debug", skip(self, buf))]
    async fn read_at(&mut self, offset: usize, buf: &mut [u8]) -> Result<usize> {
        self.seek(offset).await?;
        self.read_exact(buf).await
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn seek(&mut self, offset: usize) -> Result<()> {
        if self.pos != offset {
            self.inner.seek(SeekFrom::Start(offset as u64)).await?;
            self.pos = offset;
        }

        Ok(())
    }
    #[tracing::instrument(level = "debug", skip(self, buf))]
    async fn read_exact(&mut self, buf: &mut [u8]) -> Result<usize> {
        let bytes_read = self.inner.read_exact(buf).await?;
        self.pos += buf.len();
        Ok(bytes_read)
    }
}

#[derive(Debug)]
pub struct RemoteFileReader {
    pub endpoint: Endpoint,
    pub volume: String,
    pub path: String,
    tx: Sender<ReadAtRequest>,
    resp_stream: Streaming<ReadAtResponse>,
}

impl RemoteFileReader {
    pub async fn new(endpoint: Endpoint, volume: String, path: String, mut client: NodeClient) -> Result<Self> {
        let (tx, rx) = mpsc::channel(128);
        let in_stream = ReceiverStream::new(rx);

        let response = client.read_at(in_stream).await.unwrap();

        let resp_stream = response.into_inner();

        Ok(Self {
            endpoint,
            volume,
            path,
            tx,
            resp_stream,
        })
    }
}

#[async_trait::async_trait]
impl Reader for RemoteFileReader {
    async fn read_at(&mut self, offset: usize, buf: &mut [u8]) -> Result<usize> {
        let request = ReadAtRequest {
            disk: self.endpoint.to_string(),
            volume: self.volume.to_string(),
            path: self.path.to_string(),
            offset: offset.try_into().unwrap(),
            // length: length.try_into().unwrap(),
            length: 0,
        };
        self.tx.send(request).await?;

        if let Some(resp) = self.resp_stream.next().await {
            let resp = resp?;
            if resp.success {
                info!("read at stream success");

                buf.copy_from_slice(&resp.data);

                Ok(resp.read_size.try_into().unwrap())
            } else {
                let error_info = resp.error_info.unwrap_or("".to_string());
                info!("read at stream failed: {}", error_info);
                Err(Error::from_string(error_info))
            }
        } else {
            let error_info = "can not get response";
            info!("read at stream failed: {}", error_info);
            Err(Error::from_string(error_info))
        }
    }
    async fn seek(&mut self, _offset: usize) -> Result<()> {
        unimplemented!()
    }
    async fn read_exact(&mut self, _buf: &mut [u8]) -> Result<usize> {
        unimplemented!()
    }
}
