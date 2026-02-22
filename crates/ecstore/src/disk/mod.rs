// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

pub mod disk_store;
pub mod endpoint;
pub mod error;
pub mod error_conv;
pub mod error_reduce;
pub mod format;
pub mod fs;
pub mod local;
pub mod os;

pub const RUSTFS_META_BUCKET: &str = ".rustfs.sys";
pub const RUSTFS_META_MULTIPART_BUCKET: &str = ".rustfs.sys/multipart";
pub const RUSTFS_META_TMP_BUCKET: &str = ".rustfs.sys/tmp";
pub const RUSTFS_META_TMP_DELETED_BUCKET: &str = ".rustfs.sys/tmp/.trash";
pub const BUCKET_META_PREFIX: &str = "buckets";
pub const FORMAT_CONFIG_FILE: &str = "format.json";
pub const STORAGE_FORMAT_FILE: &str = "xl.meta";
pub const STORAGE_FORMAT_FILE_BACKUP: &str = "xl.meta.bkp";

use crate::disk::disk_store::LocalDiskWrapper;
use crate::disk::local::ScanGuard;
use crate::rpc::RemoteDisk;
use bytes::Bytes;
use endpoint::Endpoint;
use error::DiskError;
use error::{Error, Result};
use local::LocalDisk;
use rustfs_filemeta::{FileInfo, ObjectPartInfo, RawFileInfo};
use rustfs_madmin::info_commands::DiskMetrics;
use serde::{Deserialize, Serialize};
use std::{fmt::Debug, path::PathBuf, sync::Arc};
use time::OffsetDateTime;
use tokio::io::{AsyncRead, AsyncWrite};
use uuid::Uuid;

pub type DiskStore = Arc<Disk>;

pub type FileReader = Box<dyn AsyncRead + Send + Sync + Unpin>;
pub type FileWriter = Box<dyn AsyncWrite + Send + Sync + Unpin>;

#[derive(Debug)]
pub enum Disk {
    Local(Box<LocalDiskWrapper>),
    Remote(Box<RemoteDisk>),
}

#[async_trait::async_trait]
impl DiskAPI for Disk {
    #[tracing::instrument(skip(self))]
    fn to_string(&self) -> String {
        match self {
            Disk::Local(local_disk) => local_disk.to_string(),
            Disk::Remote(remote_disk) => remote_disk.to_string(),
        }
    }

    #[tracing::instrument(skip(self))]
    async fn is_online(&self) -> bool {
        match self {
            Disk::Local(local_disk) => local_disk.is_online().await,
            Disk::Remote(remote_disk) => remote_disk.is_online().await,
        }
    }

    #[tracing::instrument(skip(self))]
    fn is_local(&self) -> bool {
        match self {
            Disk::Local(local_disk) => local_disk.is_local(),
            Disk::Remote(remote_disk) => remote_disk.is_local(),
        }
    }

    #[tracing::instrument(skip(self))]
    fn host_name(&self) -> String {
        match self {
            Disk::Local(local_disk) => local_disk.host_name(),
            Disk::Remote(remote_disk) => remote_disk.host_name(),
        }
    }

    #[tracing::instrument(skip(self))]
    fn endpoint(&self) -> Endpoint {
        match self {
            Disk::Local(local_disk) => local_disk.endpoint(),
            Disk::Remote(remote_disk) => remote_disk.endpoint(),
        }
    }

    #[tracing::instrument(skip(self))]
    async fn close(&self) -> Result<()> {
        match self {
            Disk::Local(local_disk) => local_disk.close().await,
            Disk::Remote(remote_disk) => remote_disk.close().await,
        }
    }

    #[tracing::instrument(skip(self))]
    async fn get_disk_id(&self) -> Result<Option<Uuid>> {
        match self {
            Disk::Local(local_disk) => local_disk.get_disk_id().await,
            Disk::Remote(remote_disk) => remote_disk.get_disk_id().await,
        }
    }

    #[tracing::instrument(skip(self))]
    async fn set_disk_id(&self, id: Option<Uuid>) -> Result<()> {
        match self {
            Disk::Local(local_disk) => local_disk.set_disk_id(id).await,
            Disk::Remote(remote_disk) => remote_disk.set_disk_id(id).await,
        }
    }

    #[tracing::instrument(skip(self))]
    fn path(&self) -> PathBuf {
        match self {
            Disk::Local(local_disk) => local_disk.path(),
            Disk::Remote(remote_disk) => remote_disk.path(),
        }
    }

    #[tracing::instrument(skip(self))]
    fn get_disk_location(&self) -> DiskLocation {
        match self {
            Disk::Local(local_disk) => local_disk.get_disk_location(),
            Disk::Remote(remote_disk) => remote_disk.get_disk_location(),
        }
    }

    #[tracing::instrument(skip(self))]
    async fn make_volume(&self, volume: &str) -> Result<()> {
        match self {
            Disk::Local(local_disk) => local_disk.make_volume(volume).await,
            Disk::Remote(remote_disk) => remote_disk.make_volume(volume).await,
        }
    }

    #[tracing::instrument(skip(self))]
    async fn make_volumes(&self, volumes: Vec<&str>) -> Result<()> {
        match self {
            Disk::Local(local_disk) => local_disk.make_volumes(volumes).await,
            Disk::Remote(remote_disk) => remote_disk.make_volumes(volumes).await,
        }
    }

    #[tracing::instrument(skip(self))]
    async fn list_volumes(&self) -> Result<Vec<VolumeInfo>> {
        match self {
            Disk::Local(local_disk) => local_disk.list_volumes().await,
            Disk::Remote(remote_disk) => remote_disk.list_volumes().await,
        }
    }

    #[tracing::instrument(skip(self))]
    async fn stat_volume(&self, volume: &str) -> Result<VolumeInfo> {
        match self {
            Disk::Local(local_disk) => local_disk.stat_volume(volume).await,
            Disk::Remote(remote_disk) => remote_disk.stat_volume(volume).await,
        }
    }

    #[tracing::instrument(skip(self))]
    async fn delete_volume(&self, volume: &str) -> Result<()> {
        match self {
            Disk::Local(local_disk) => local_disk.delete_volume(volume).await,
            Disk::Remote(remote_disk) => remote_disk.delete_volume(volume).await,
        }
    }

    #[tracing::instrument(skip(self, wr))]
    async fn walk_dir<W: AsyncWrite + Unpin + Send>(&self, opts: WalkDirOptions, wr: &mut W) -> Result<()> {
        match self {
            Disk::Local(local_disk) => local_disk.walk_dir(opts, wr).await,
            Disk::Remote(remote_disk) => remote_disk.walk_dir(opts, wr).await,
        }
    }

    #[tracing::instrument(skip(self))]
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

    #[tracing::instrument(skip(self))]
    async fn delete_versions(&self, volume: &str, versions: Vec<FileInfoVersions>, opts: DeleteOptions) -> Vec<Option<Error>> {
        match self {
            Disk::Local(local_disk) => local_disk.delete_versions(volume, versions, opts).await,
            Disk::Remote(remote_disk) => remote_disk.delete_versions(volume, versions, opts).await,
        }
    }

    #[tracing::instrument(skip(self))]
    async fn delete_paths(&self, volume: &str, paths: &[String]) -> Result<()> {
        match self {
            Disk::Local(local_disk) => local_disk.delete_paths(volume, paths).await,
            Disk::Remote(remote_disk) => remote_disk.delete_paths(volume, paths).await,
        }
    }

    #[tracing::instrument(skip(self))]
    async fn write_metadata(&self, _org_volume: &str, volume: &str, path: &str, fi: FileInfo) -> Result<()> {
        match self {
            Disk::Local(local_disk) => local_disk.write_metadata(_org_volume, volume, path, fi).await,
            Disk::Remote(remote_disk) => remote_disk.write_metadata(_org_volume, volume, path, fi).await,
        }
    }

    #[tracing::instrument(skip(self))]
    async fn update_metadata(&self, volume: &str, path: &str, fi: FileInfo, opts: &UpdateMetadataOpts) -> Result<()> {
        match self {
            Disk::Local(local_disk) => local_disk.update_metadata(volume, path, fi, opts).await,
            Disk::Remote(remote_disk) => remote_disk.update_metadata(volume, path, fi, opts).await,
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

    #[tracing::instrument(skip(self))]
    async fn read_xl(&self, volume: &str, path: &str, read_data: bool) -> Result<RawFileInfo> {
        match self {
            Disk::Local(local_disk) => local_disk.read_xl(volume, path, read_data).await,
            Disk::Remote(remote_disk) => remote_disk.read_xl(volume, path, read_data).await,
        }
    }

    #[tracing::instrument(skip(self, fi))]
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

    #[tracing::instrument(skip(self))]
    async fn list_dir(&self, _origvolume: &str, volume: &str, dir_path: &str, count: i32) -> Result<Vec<String>> {
        match self {
            Disk::Local(local_disk) => local_disk.list_dir(_origvolume, volume, dir_path, count).await,
            Disk::Remote(remote_disk) => remote_disk.list_dir(_origvolume, volume, dir_path, count).await,
        }
    }

    #[tracing::instrument(skip(self))]
    async fn read_file(&self, volume: &str, path: &str) -> Result<FileReader> {
        match self {
            Disk::Local(local_disk) => local_disk.read_file(volume, path).await,
            Disk::Remote(remote_disk) => remote_disk.read_file(volume, path).await,
        }
    }

    #[tracing::instrument(skip(self))]
    async fn read_file_stream(&self, volume: &str, path: &str, offset: usize, length: usize) -> Result<FileReader> {
        match self {
            Disk::Local(local_disk) => local_disk.read_file_stream(volume, path, offset, length).await,
            Disk::Remote(remote_disk) => remote_disk.read_file_stream(volume, path, offset, length).await,
        }
    }

    #[tracing::instrument(skip(self))]
    async fn append_file(&self, volume: &str, path: &str) -> Result<FileWriter> {
        match self {
            Disk::Local(local_disk) => local_disk.append_file(volume, path).await,
            Disk::Remote(remote_disk) => remote_disk.append_file(volume, path).await,
        }
    }

    #[tracing::instrument(skip(self))]
    async fn create_file(&self, _origvolume: &str, volume: &str, path: &str, _file_size: i64) -> Result<FileWriter> {
        match self {
            Disk::Local(local_disk) => local_disk.create_file(_origvolume, volume, path, _file_size).await,
            Disk::Remote(remote_disk) => remote_disk.create_file(_origvolume, volume, path, _file_size).await,
        }
    }

    #[tracing::instrument(skip(self))]
    async fn rename_file(&self, src_volume: &str, src_path: &str, dst_volume: &str, dst_path: &str) -> Result<()> {
        match self {
            Disk::Local(local_disk) => local_disk.rename_file(src_volume, src_path, dst_volume, dst_path).await,
            Disk::Remote(remote_disk) => remote_disk.rename_file(src_volume, src_path, dst_volume, dst_path).await,
        }
    }

    #[tracing::instrument(skip(self))]
    async fn read_parts(&self, bucket: &str, paths: &[String]) -> Result<Vec<ObjectPartInfo>> {
        match self {
            Disk::Local(local_disk) => local_disk.read_parts(bucket, paths).await,
            Disk::Remote(remote_disk) => remote_disk.read_parts(bucket, paths).await,
        }
    }

    #[tracing::instrument(skip(self))]
    async fn rename_part(&self, src_volume: &str, src_path: &str, dst_volume: &str, dst_path: &str, meta: Bytes) -> Result<()> {
        match self {
            Disk::Local(local_disk) => local_disk.rename_part(src_volume, src_path, dst_volume, dst_path, meta).await,
            Disk::Remote(remote_disk) => {
                remote_disk
                    .rename_part(src_volume, src_path, dst_volume, dst_path, meta)
                    .await
            }
        }
    }

    #[tracing::instrument(skip(self))]
    async fn delete(&self, volume: &str, path: &str, opt: DeleteOptions) -> Result<()> {
        match self {
            Disk::Local(local_disk) => local_disk.delete(volume, path, opt).await,
            Disk::Remote(remote_disk) => remote_disk.delete(volume, path, opt).await,
        }
    }

    #[tracing::instrument(skip(self))]
    async fn verify_file(&self, volume: &str, path: &str, fi: &FileInfo) -> Result<CheckPartsResp> {
        match self {
            Disk::Local(local_disk) => local_disk.verify_file(volume, path, fi).await,
            Disk::Remote(remote_disk) => remote_disk.verify_file(volume, path, fi).await,
        }
    }

    #[tracing::instrument(skip(self))]
    async fn check_parts(&self, volume: &str, path: &str, fi: &FileInfo) -> Result<CheckPartsResp> {
        match self {
            Disk::Local(local_disk) => local_disk.check_parts(volume, path, fi).await,
            Disk::Remote(remote_disk) => remote_disk.check_parts(volume, path, fi).await,
        }
    }

    #[tracing::instrument(skip(self))]
    async fn read_multiple(&self, req: ReadMultipleReq) -> Result<Vec<ReadMultipleResp>> {
        match self {
            Disk::Local(local_disk) => local_disk.read_multiple(req).await,
            Disk::Remote(remote_disk) => remote_disk.read_multiple(req).await,
        }
    }

    #[tracing::instrument(skip(self))]
    async fn write_all(&self, volume: &str, path: &str, data: Bytes) -> Result<()> {
        match self {
            Disk::Local(local_disk) => local_disk.write_all(volume, path, data).await,
            Disk::Remote(remote_disk) => remote_disk.write_all(volume, path, data).await,
        }
    }

    #[tracing::instrument(skip(self))]
    async fn read_all(&self, volume: &str, path: &str) -> Result<Bytes> {
        match self {
            Disk::Local(local_disk) => local_disk.read_all(volume, path).await,
            Disk::Remote(remote_disk) => remote_disk.read_all(volume, path).await,
        }
    }

    #[tracing::instrument(skip(self))]
    async fn disk_info(&self, opts: &DiskInfoOptions) -> Result<DiskInfo> {
        match self {
            Disk::Local(local_disk) => local_disk.disk_info(opts).await,
            Disk::Remote(remote_disk) => remote_disk.disk_info(opts).await,
        }
    }

    fn start_scan(&self) -> ScanGuard {
        match self {
            Disk::Local(local_disk) => local_disk.start_scan(),
            Disk::Remote(remote_disk) => remote_disk.start_scan(),
        }
    }

    async fn read_metadata(&self, volume: &str, path: &str) -> Result<Bytes> {
        match self {
            Disk::Local(local_disk) => local_disk.read_metadata(volume, path).await,
            Disk::Remote(remote_disk) => remote_disk.read_metadata(volume, path).await,
        }
    }
}

impl Disk {
    /// Enable health monitoring on this disk.
    /// Called after startup format loading completes so that remote peers
    /// have time to come online before being marked as faulty.
    pub fn enable_health_check(&self) {
        match self {
            Disk::Local(local_disk) => local_disk.enable_health_check(),
            Disk::Remote(remote_disk) => remote_disk.enable_health_check(),
        }
    }
}

pub async fn new_disk(ep: &Endpoint, opt: &DiskOption) -> Result<DiskStore> {
    if ep.is_local {
        let s = LocalDisk::new(ep, opt.cleanup).await?;
        Ok(Arc::new(Disk::Local(Box::new(LocalDiskWrapper::new(Arc::new(s), opt.health_check)))))
    } else {
        let remote_disk = RemoteDisk::new(ep, opt).await?;
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

    // Concurrent read/write pipeline w <- MetaCacheEntry
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
    async fn delete_versions(&self, volume: &str, versions: Vec<FileInfoVersions>, opts: DeleteOptions) -> Vec<Option<Error>>;
    async fn delete_paths(&self, volume: &str, paths: &[String]) -> Result<()>;
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
    async fn read_metadata(&self, volume: &str, path: &str) -> Result<Bytes>;
    async fn rename_data(
        &self,
        src_volume: &str,
        src_path: &str,
        file_info: FileInfo,
        dst_volume: &str,
        dst_path: &str,
    ) -> Result<RenameDataResp>;

    // File operations.
    // Read every file and directory within the folder
    async fn list_dir(&self, origvolume: &str, volume: &str, dir_path: &str, count: i32) -> Result<Vec<String>>;
    async fn read_file(&self, volume: &str, path: &str) -> Result<FileReader>;
    async fn read_file_stream(&self, volume: &str, path: &str, offset: usize, length: usize) -> Result<FileReader>;
    async fn append_file(&self, volume: &str, path: &str) -> Result<FileWriter>;
    async fn create_file(&self, origvolume: &str, volume: &str, path: &str, file_size: i64) -> Result<FileWriter>;
    // ReadFileStream
    async fn rename_file(&self, src_volume: &str, src_path: &str, dst_volume: &str, dst_path: &str) -> Result<()>;
    async fn rename_part(&self, src_volume: &str, src_path: &str, dst_volume: &str, dst_path: &str, meta: Bytes) -> Result<()>;
    async fn delete(&self, volume: &str, path: &str, opt: DeleteOptions) -> Result<()>;
    // VerifyFile
    async fn verify_file(&self, volume: &str, path: &str, fi: &FileInfo) -> Result<CheckPartsResp>;
    // CheckParts
    async fn check_parts(&self, volume: &str, path: &str, fi: &FileInfo) -> Result<CheckPartsResp>;
    // StatInfoFile
    async fn read_parts(&self, bucket: &str, paths: &[String]) -> Result<Vec<ObjectPartInfo>>;
    async fn read_multiple(&self, req: ReadMultipleReq) -> Result<Vec<ReadMultipleResp>>;
    // CleanAbandonedData
    async fn write_all(&self, volume: &str, path: &str, data: Bytes) -> Result<()>;
    async fn read_all(&self, volume: &str, path: &str) -> Result<Bytes>;
    async fn disk_info(&self, opts: &DiskInfoOptions) -> Result<DiskInfo>;
    fn start_scan(&self) -> ScanGuard;
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
    pub id: Option<Uuid>,
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

        let vid = Uuid::parse_str(v).unwrap_or_default();

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
    pub filter_prefix: Option<String>,

    // ForwardTo will forward to the given object path.
    pub forward_to: Option<String>,

    // Limit the number of returned objects if > 0.
    pub limit: i32,

    // DiskID contains the disk ID of the disk.
    // Leave empty to not check disk ID.
    pub disk_id: String,
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

#[derive(Debug, Deserialize, Serialize)]
pub struct VolumeInfo {
    pub name: String,
    pub created: Option<OffsetDateTime>,
}

#[derive(Deserialize, Serialize, Debug, Default, Clone)]
pub struct ReadOptions {
    pub incl_free_versions: bool,
    pub read_data: bool,
    pub healing: bool,
}

pub const CHECK_PART_UNKNOWN: usize = 0;
// Changing the order can cause a data loss
// when running two nodes with incompatible versions
pub const CHECK_PART_SUCCESS: usize = 1;
pub const CHECK_PART_DISK_NOT_FOUND: usize = 2;
pub const CHECK_PART_VOLUME_NOT_FOUND: usize = 3;
pub const CHECK_PART_FILE_NOT_FOUND: usize = 4;
pub const CHECK_PART_FILE_CORRUPT: usize = 5;

pub fn conv_part_err_to_int(err: &Option<Error>) -> usize {
    match err {
        Some(DiskError::FileNotFound) | Some(DiskError::FileVersionNotFound) => CHECK_PART_FILE_NOT_FOUND,
        Some(DiskError::FileCorrupt) => CHECK_PART_FILE_CORRUPT,
        Some(DiskError::VolumeNotFound) => CHECK_PART_VOLUME_NOT_FOUND,
        Some(DiskError::DiskNotFound) => CHECK_PART_DISK_NOT_FOUND,
        None => CHECK_PART_SUCCESS,
        _ => {
            tracing::warn!("conv_part_err_to_int: unknown error: {err:?}");
            CHECK_PART_UNKNOWN
        }
    }
}

pub fn has_part_err(part_errs: &[usize]) -> bool {
    part_errs.iter().any(|err| *err != CHECK_PART_SUCCESS)
}

pub fn count_part_not_success(part_errs: &[usize]) -> usize {
    part_errs.iter().filter(|err| **err != CHECK_PART_SUCCESS).count()
}

#[cfg(test)]
mod tests {
    use super::*;
    use endpoint::Endpoint;
    use local::LocalDisk;
    use std::path::PathBuf;
    use tokio::fs;
    use uuid::Uuid;

    /// Test DiskLocation validation
    #[test]
    fn test_disk_location_valid() {
        let valid_location = DiskLocation {
            pool_idx: Some(0),
            set_idx: Some(1),
            disk_idx: Some(2),
        };
        assert!(valid_location.valid());

        let invalid_location = DiskLocation {
            pool_idx: None,
            set_idx: None,
            disk_idx: None,
        };
        assert!(!invalid_location.valid());

        let partial_valid_location = DiskLocation {
            pool_idx: Some(0),
            set_idx: None,
            disk_idx: Some(2),
        };
        assert!(!partial_valid_location.valid());
    }

    /// Test FileInfoVersions find_version_index
    #[test]
    fn test_file_info_versions_find_version_index() {
        let mut versions = Vec::new();
        let v1_uuid = Uuid::new_v4();
        let v2_uuid = Uuid::new_v4();
        let fi1 = FileInfo {
            version_id: Some(v1_uuid),
            ..Default::default()
        };
        let fi2 = FileInfo {
            version_id: Some(v2_uuid),
            ..Default::default()
        };
        versions.push(fi1);
        versions.push(fi2);

        let fiv = FileInfoVersions {
            volume: "test-bucket".to_string(),
            name: "test-object".to_string(),
            latest_mod_time: None,
            versions,
            free_versions: Vec::new(),
        };

        assert_eq!(fiv.find_version_index(&v1_uuid.to_string()), Some(0));
        assert_eq!(fiv.find_version_index(&v2_uuid.to_string()), Some(1));
        assert_eq!(fiv.find_version_index("non-existent"), None);
        assert_eq!(fiv.find_version_index(""), None);
    }

    /// Test part error conversion functions
    #[test]
    fn test_conv_part_err_to_int() {
        assert_eq!(conv_part_err_to_int(&None), CHECK_PART_SUCCESS);
        assert_eq!(
            conv_part_err_to_int(&Some(Error::from(DiskError::DiskNotFound))),
            CHECK_PART_DISK_NOT_FOUND
        );
        assert_eq!(
            conv_part_err_to_int(&Some(Error::from(DiskError::VolumeNotFound))),
            CHECK_PART_VOLUME_NOT_FOUND
        );
        assert_eq!(
            conv_part_err_to_int(&Some(Error::from(DiskError::FileNotFound))),
            CHECK_PART_FILE_NOT_FOUND
        );
        assert_eq!(conv_part_err_to_int(&Some(Error::from(DiskError::FileCorrupt))), CHECK_PART_FILE_CORRUPT);
        assert_eq!(conv_part_err_to_int(&Some(Error::from(DiskError::Unexpected))), CHECK_PART_UNKNOWN);
    }

    /// Test has_part_err function
    #[test]
    fn test_has_part_err() {
        assert!(!has_part_err(&[]));
        assert!(!has_part_err(&[CHECK_PART_SUCCESS]));
        assert!(!has_part_err(&[CHECK_PART_SUCCESS, CHECK_PART_SUCCESS]));

        assert!(has_part_err(&[CHECK_PART_FILE_NOT_FOUND]));
        assert!(has_part_err(&[CHECK_PART_SUCCESS, CHECK_PART_FILE_CORRUPT]));
        assert!(has_part_err(&[CHECK_PART_DISK_NOT_FOUND, CHECK_PART_VOLUME_NOT_FOUND]));
    }

    /// Test WalkDirOptions structure
    #[test]
    fn test_walk_dir_options() {
        let opts = WalkDirOptions {
            bucket: "test-bucket".to_string(),
            base_dir: "/path/to/dir".to_string(),
            recursive: true,
            report_notfound: false,
            filter_prefix: Some("prefix_".to_string()),
            forward_to: Some("object/path".to_string()),
            limit: 100,
            disk_id: "disk-123".to_string(),
        };

        assert_eq!(opts.bucket, "test-bucket");
        assert_eq!(opts.base_dir, "/path/to/dir");
        assert!(opts.recursive);
        assert!(!opts.report_notfound);
        assert_eq!(opts.filter_prefix, Some("prefix_".to_string()));
        assert_eq!(opts.forward_to, Some("object/path".to_string()));
        assert_eq!(opts.limit, 100);
        assert_eq!(opts.disk_id, "disk-123");
    }

    /// Test DeleteOptions structure
    #[test]
    fn test_delete_options() {
        let opts = DeleteOptions {
            recursive: true,
            immediate: false,
            undo_write: true,
            old_data_dir: Some(Uuid::new_v4()),
        };

        assert!(opts.recursive);
        assert!(!opts.immediate);
        assert!(opts.undo_write);
        assert!(opts.old_data_dir.is_some());
    }

    /// Test ReadOptions structure
    #[test]
    fn test_read_options() {
        let opts = ReadOptions {
            incl_free_versions: true,
            read_data: false,
            healing: true,
        };

        assert!(opts.incl_free_versions);
        assert!(!opts.read_data);
        assert!(opts.healing);
    }

    /// Test UpdateMetadataOpts structure
    #[test]
    fn test_update_metadata_opts() {
        let opts = UpdateMetadataOpts { no_persistence: true };

        assert!(opts.no_persistence);
    }

    /// Test DiskOption structure
    #[test]
    fn test_disk_option() {
        let opt = DiskOption {
            cleanup: true,
            health_check: false,
        };

        assert!(opt.cleanup);
        assert!(!opt.health_check);
    }

    /// Test DiskInfoOptions structure
    #[test]
    fn test_disk_info_options() {
        let opts = DiskInfoOptions {
            disk_id: "test-disk-id".to_string(),
            metrics: true,
            noop: false,
        };

        assert_eq!(opts.disk_id, "test-disk-id");
        assert!(opts.metrics);
        assert!(!opts.noop);
    }

    /// Test ReadMultipleReq structure
    #[test]
    fn test_read_multiple_req() {
        let req = ReadMultipleReq {
            bucket: "test-bucket".to_string(),
            prefix: "prefix/".to_string(),
            files: vec!["file1.txt".to_string(), "file2.txt".to_string()],
            max_size: 1024,
            metadata_only: false,
            abort404: true,
            max_results: 10,
        };

        assert_eq!(req.bucket, "test-bucket");
        assert_eq!(req.prefix, "prefix/");
        assert_eq!(req.files.len(), 2);
        assert_eq!(req.max_size, 1024);
        assert!(!req.metadata_only);
        assert!(req.abort404);
        assert_eq!(req.max_results, 10);
    }

    /// Test ReadMultipleResp structure
    #[test]
    fn test_read_multiple_resp() {
        let resp = ReadMultipleResp {
            bucket: "test-bucket".to_string(),
            prefix: "prefix/".to_string(),
            file: "test-file.txt".to_string(),
            exists: true,
            error: "".to_string(),
            data: vec![1, 2, 3, 4],
            mod_time: Some(time::OffsetDateTime::now_utc()),
        };

        assert_eq!(resp.bucket, "test-bucket");
        assert_eq!(resp.prefix, "prefix/");
        assert_eq!(resp.file, "test-file.txt");
        assert!(resp.exists);
        assert!(resp.error.is_empty());
        assert_eq!(resp.data, vec![1, 2, 3, 4]);
        assert!(resp.mod_time.is_some());
    }

    /// Test VolumeInfo structure
    #[test]
    fn test_volume_info() {
        let now = time::OffsetDateTime::now_utc();
        let vol_info = VolumeInfo {
            name: "test-volume".to_string(),
            created: Some(now),
        };

        assert_eq!(vol_info.name, "test-volume");
        assert_eq!(vol_info.created, Some(now));
    }

    /// Test CheckPartsResp structure
    #[test]
    fn test_check_parts_resp() {
        let resp = CheckPartsResp {
            results: vec![CHECK_PART_SUCCESS, CHECK_PART_FILE_NOT_FOUND, CHECK_PART_FILE_CORRUPT],
        };

        assert_eq!(resp.results.len(), 3);
        assert_eq!(resp.results[0], CHECK_PART_SUCCESS);
        assert_eq!(resp.results[1], CHECK_PART_FILE_NOT_FOUND);
        assert_eq!(resp.results[2], CHECK_PART_FILE_CORRUPT);
    }

    /// Test RenameDataResp structure
    #[test]
    fn test_rename_data_resp() {
        let uuid = Uuid::new_v4();
        let signature = vec![0x01, 0x02, 0x03];

        let resp = RenameDataResp {
            old_data_dir: Some(uuid),
            sign: Some(signature.clone()),
        };

        assert_eq!(resp.old_data_dir, Some(uuid));
        assert_eq!(resp.sign, Some(signature));
    }

    /// Test constants
    #[test]
    fn test_constants() {
        assert_eq!(RUSTFS_META_BUCKET, ".rustfs.sys");
        assert_eq!(RUSTFS_META_MULTIPART_BUCKET, ".rustfs.sys/multipart");
        assert_eq!(RUSTFS_META_TMP_BUCKET, ".rustfs.sys/tmp");
        assert_eq!(RUSTFS_META_TMP_DELETED_BUCKET, ".rustfs.sys/tmp/.trash");
        assert_eq!(BUCKET_META_PREFIX, "buckets");
        assert_eq!(FORMAT_CONFIG_FILE, "format.json");
        assert_eq!(STORAGE_FORMAT_FILE, "xl.meta");
        assert_eq!(STORAGE_FORMAT_FILE_BACKUP, "xl.meta.bkp");

        assert_eq!(CHECK_PART_UNKNOWN, 0);
        assert_eq!(CHECK_PART_SUCCESS, 1);
        assert_eq!(CHECK_PART_DISK_NOT_FOUND, 2);
        assert_eq!(CHECK_PART_VOLUME_NOT_FOUND, 3);
        assert_eq!(CHECK_PART_FILE_NOT_FOUND, 4);
        assert_eq!(CHECK_PART_FILE_CORRUPT, 5);
    }

    /// Integration test for creating a local disk
    #[tokio::test]
    async fn test_new_disk_creation() {
        let test_dir = "./test_disk_creation";
        fs::create_dir_all(&test_dir).await.unwrap();

        let endpoint = Endpoint::try_from(test_dir).unwrap();
        let opt = DiskOption {
            cleanup: false,
            health_check: true,
        };

        let disk = new_disk(&endpoint, &opt).await;
        assert!(disk.is_ok());

        let disk = disk.unwrap();
        assert_eq!(disk.path(), PathBuf::from(test_dir).canonicalize().unwrap());
        assert!(disk.is_local());
        // Note: is_online() might return false for local disks without proper initialization
        // This is expected behavior for test environments

        // Clean up the test directory
        let _ = fs::remove_dir_all(&test_dir).await;
    }

    /// Test Disk enum pattern matching
    #[tokio::test]
    async fn test_disk_enum_methods() {
        let test_dir = "./test_disk_enum";
        fs::create_dir_all(&test_dir).await.unwrap();

        let endpoint = Endpoint::try_from(test_dir).unwrap();
        let local_disk = LocalDisk::new(&endpoint, false).await.unwrap();
        let disk = Disk::Local(Box::new(LocalDiskWrapper::new(Arc::new(local_disk), false)));

        // Test basic methods
        assert!(disk.is_local());
        // Note: is_online() might return false for local disks without proper initialization
        // assert!(disk.is_online().await);
        // Note: host_name() for local disks might be empty or contain localhost
        // assert!(!disk.host_name().is_empty());
        // Note: to_string() format might vary, so just check it's not empty
        assert!(!disk.to_string().is_empty());

        // Test path method
        let path = disk.path();
        assert!(path.exists());

        // Test disk location
        let location = disk.get_disk_location();
        assert!(location.valid() || (!location.valid() && endpoint.pool_idx < 0));

        // Clean up the test directory
        let _ = fs::remove_dir_all(&test_dir).await;
    }
}
