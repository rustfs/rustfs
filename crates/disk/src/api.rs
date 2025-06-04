use crate::{endpoint::Endpoint, local::LocalDisk, remote::RemoteDisk};
use madmin::DiskMetrics;
use rustfs_error::{Error, Result};
use rustfs_filemeta::{FileInfo, FileInfoVersions, RawFileInfo};
use serde::{Deserialize, Serialize};
use std::{fmt::Debug, path::PathBuf, sync::Arc};
use time::OffsetDateTime;
use tokio::io::{AsyncRead, AsyncWrite};
use uuid::Uuid;

pub const RUSTFS_META_BUCKET: &str = ".rustfs.sys";
pub const RUSTFS_META_MULTIPART_BUCKET: &str = ".rustfs.sys/multipart";
pub const RUSTFS_META_TMP_BUCKET: &str = ".rustfs.sys/tmp";
pub const RUSTFS_META_TMP_DELETED_BUCKET: &str = ".rustfs.sys/tmp/.trash";
pub const BUCKET_META_PREFIX: &str = "buckets";
pub const FORMAT_CONFIG_FILE: &str = "format.json";
pub const STORAGE_FORMAT_FILE: &str = "xl.meta";
pub const STORAGE_FORMAT_FILE_BACKUP: &str = "xl.meta.bkp";

pub type DiskStore = Arc<Disk>;

#[derive(Debug)]
pub enum Disk {
    Local(LocalDisk),
    Remote(RemoteDisk),
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
    async fn is_online(&self) -> bool {
        match self {
            Disk::Local(local_disk) => local_disk.is_online().await,
            Disk::Remote(remote_disk) => remote_disk.is_online().await,
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
    async fn read_all(&self, volume: &str, path: &str) -> Result<Vec<u8>> {
        match self {
            Disk::Local(local_disk) => local_disk.read_all(volume, path).await,
            Disk::Remote(remote_disk) => remote_disk.read_all(volume, path).await,
        }
    }

    #[tracing::instrument(skip(self))]
    async fn write_all(&self, volume: &str, path: &str, data: Vec<u8>) -> Result<()> {
        match self {
            Disk::Local(local_disk) => local_disk.write_all(volume, path, data).await,
            Disk::Remote(remote_disk) => remote_disk.write_all(volume, path, data).await,
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

    #[tracing::instrument(skip(self))]
    async fn rename_file(&self, src_volume: &str, src_path: &str, dst_volume: &str, dst_path: &str) -> Result<()> {
        match self {
            Disk::Local(local_disk) => local_disk.rename_file(src_volume, src_path, dst_volume, dst_path).await,
            Disk::Remote(remote_disk) => remote_disk.rename_file(src_volume, src_path, dst_volume, dst_path).await,
        }
    }

    #[tracing::instrument(skip(self))]
    async fn create_file(&self, _origvolume: &str, volume: &str, path: &str, _file_size: usize) -> Result<Box<dyn AsyncWrite>> {
        match self {
            Disk::Local(local_disk) => local_disk.create_file(_origvolume, volume, path, _file_size).await,
            Disk::Remote(remote_disk) => remote_disk.create_file(_origvolume, volume, path, _file_size).await,
        }
    }

    #[tracing::instrument(skip(self))]
    async fn append_file(&self, volume: &str, path: &str) -> Result<Box<dyn AsyncWrite>> {
        match self {
            Disk::Local(local_disk) => local_disk.append_file(volume, path).await,
            Disk::Remote(remote_disk) => remote_disk.append_file(volume, path).await,
        }
    }

    #[tracing::instrument(skip(self))]
    async fn read_file(&self, volume: &str, path: &str) -> Result<Box<dyn AsyncRead>> {
        match self {
            Disk::Local(local_disk) => local_disk.read_file(volume, path).await,
            Disk::Remote(remote_disk) => remote_disk.read_file(volume, path).await,
        }
    }

    #[tracing::instrument(skip(self))]
    async fn read_file_stream(&self, volume: &str, path: &str, offset: usize, length: usize) -> Result<Box<dyn AsyncRead>> {
        match self {
            Disk::Local(local_disk) => local_disk.read_file_stream(volume, path, offset, length).await,
            Disk::Remote(remote_disk) => remote_disk.read_file_stream(volume, path, offset, length).await,
        }
    }

    #[tracing::instrument(skip(self))]
    async fn list_dir(&self, _origvolume: &str, volume: &str, _dir_path: &str, _count: i32) -> Result<Vec<String>> {
        match self {
            Disk::Local(local_disk) => local_disk.list_dir(_origvolume, volume, _dir_path, _count).await,
            Disk::Remote(remote_disk) => remote_disk.list_dir(_origvolume, volume, _dir_path, _count).await,
        }
    }

    #[tracing::instrument(skip(self, wr))]
    async fn walk_dir<W: AsyncWrite + Unpin + Send + Sync + 'static>(&self, opts: WalkDirOptions, wr: &mut W) -> Result<()> {
        match self {
            Disk::Local(local_disk) => local_disk.walk_dir(opts, wr).await,
            Disk::Remote(remote_disk) => remote_disk.walk_dir(opts, wr).await,
        }
    }

    #[tracing::instrument(skip(self))]
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
    async fn make_volumes(&self, volumes: Vec<&str>) -> Result<()> {
        match self {
            Disk::Local(local_disk) => local_disk.make_volumes(volumes).await,
            Disk::Remote(remote_disk) => remote_disk.make_volumes(volumes).await,
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
    async fn delete_paths(&self, volume: &str, paths: &[String]) -> Result<()> {
        match self {
            Disk::Local(local_disk) => local_disk.delete_paths(volume, paths).await,
            Disk::Remote(remote_disk) => remote_disk.delete_paths(volume, paths).await,
        }
    }

    #[tracing::instrument(skip(self))]
    async fn update_metadata(&self, volume: &str, path: &str, fi: FileInfo, opts: &UpdateMetadataOpts) -> Result<()> {
        match self {
            Disk::Local(local_disk) => local_disk.update_metadata(volume, path, fi, opts).await,
            Disk::Remote(remote_disk) => remote_disk.update_metadata(volume, path, fi, opts).await,
        }
    }

    #[tracing::instrument(skip(self))]
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

    #[tracing::instrument(skip(self))]
    async fn read_xl(&self, volume: &str, path: &str, read_data: bool) -> Result<RawFileInfo> {
        match self {
            Disk::Local(local_disk) => local_disk.read_xl(volume, path, read_data).await,
            Disk::Remote(remote_disk) => remote_disk.read_xl(volume, path, read_data).await,
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

    #[tracing::instrument(skip(self))]
    async fn read_multiple(&self, req: ReadMultipleReq) -> Result<Vec<ReadMultipleResp>> {
        match self {
            Disk::Local(local_disk) => local_disk.read_multiple(req).await,
            Disk::Remote(remote_disk) => remote_disk.read_multiple(req).await,
        }
    }

    #[tracing::instrument(skip(self))]
    async fn delete_volume(&self, volume: &str) -> Result<()> {
        match self {
            Disk::Local(local_disk) => local_disk.delete_volume(volume).await,
            Disk::Remote(remote_disk) => remote_disk.delete_volume(volume).await,
        }
    }

    #[tracing::instrument(skip(self))]
    async fn disk_info(&self, opts: &DiskInfoOptions) -> Result<DiskInfo> {
        match self {
            Disk::Local(local_disk) => local_disk.disk_info(opts).await,
            Disk::Remote(remote_disk) => remote_disk.disk_info(opts).await,
        }
    }

    // #[tracing::instrument(skip(self, cache, we_sleep, scan_mode))]
    // async fn ns_scanner(
    //     &self,
    //     cache: &DataUsageCache,
    //     updates: Sender<DataUsageEntry>,
    //     scan_mode: HealScanMode,
    //     we_sleep: ShouldSleepFn,
    // ) -> Result<DataUsageCache> {
    //     match self {
    //         Disk::Local(local_disk) => local_disk.ns_scanner(cache, updates, scan_mode, we_sleep).await,
    //         Disk::Remote(remote_disk) => remote_disk.ns_scanner(cache, updates, scan_mode, we_sleep).await,
    //     }
    // }

    // #[tracing::instrument(skip(self))]
    // async fn healing(&self) -> Option<HealingTracker> {
    //     match self {
    //         Disk::Local(local_disk) => local_disk.healing().await,
    //         Disk::Remote(remote_disk) => remote_disk.healing().await,
    //     }
    // }
}

pub async fn new_disk(ep: &Endpoint, opt: &DiskOption) -> Result<DiskStore> {
    if ep.is_local {
        Ok(Arc::new(Disk::Local(LocalDisk::new(ep, opt.cleanup).await?)))
    } else {
        Ok(Arc::new(Disk::Remote(RemoteDisk::new(ep, opt).await?)))
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
    async fn walk_dir<W: AsyncWrite + Unpin + Send + Sync + 'static>(&self, opts: WalkDirOptions, wr: &mut W) -> Result<()>;

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
    async fn read_file(&self, volume: &str, path: &str) -> Result<Box<dyn AsyncRead>>;
    async fn read_file_stream(&self, volume: &str, path: &str, offset: usize, length: usize) -> Result<Box<dyn AsyncRead>>;
    async fn append_file(&self, volume: &str, path: &str) -> Result<Box<dyn AsyncWrite>>;
    async fn create_file(&self, origvolume: &str, volume: &str, path: &str, file_size: usize) -> Result<Box<dyn AsyncWrite>>;
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
    // async fn ns_scanner(
    //     &self,
    //     cache: &DataUsageCache,
    //     updates: Sender<DataUsageEntry>,
    //     scan_mode: HealScanMode,
    //     we_sleep: ShouldSleepFn,
    // ) -> Result<DataUsageCache>;
    // async fn healing(&self) -> Option<HealingTracker>;
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

// #[derive(Debug, Default, Clone, Serialize, Deserialize)]
// pub struct FileInfoVersions {
//     // Name of the volume.
//     pub volume: String,

//     // Name of the file.
//     pub name: String,

//     // Represents the latest mod time of the
//     // latest version.
//     pub latest_mod_time: Option<OffsetDateTime>,

//     pub versions: Vec<FileInfo>,
//     pub free_versions: Vec<FileInfo>,
// }

// impl FileInfoVersions {
//     pub fn find_version_index(&self, v: &str) -> Option<usize> {
//         if v.is_empty() {
//             return None;
//         }

//         let vid = Uuid::parse_str(v).unwrap_or(Uuid::nil());

//         self.versions.iter().position(|v| v.version_id == Some(vid))
//     }
// }

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
// move  metacache to metacache.rs

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

#[derive(Deserialize, Serialize, Debug, Default)]
pub struct ReadOptions {
    pub incl_free_versions: bool,
    pub read_data: bool,
    pub healing: bool,
}
