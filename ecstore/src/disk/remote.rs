use std::path::PathBuf;

use futures::lock::Mutex;
use protos::{
    node_service_time_out_client,
    proto_gen::node_service::{
        CheckPartsRequest, DeletePathsRequest, DeleteRequest, DeleteVersionRequest, DeleteVersionsRequest, DeleteVolumeRequest,
        DiskInfoRequest, ListDirRequest, ListVolumesRequest, MakeVolumeRequest, MakeVolumesRequest, NsScannerRequest,
        ReadAllRequest, ReadMultipleRequest, ReadVersionRequest, ReadXlRequest, RenameDataRequest, RenameFileRequst,
        StatVolumeRequest, UpdateMetadataRequest, VerifyFileRequest, WalkDirRequest, WriteAllRequest, WriteMetadataRequest,
    },
};
use tokio::sync::mpsc::{self, Sender};
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tonic::Request;
use tracing::info;
use uuid::Uuid;

use super::{
    endpoint::Endpoint, CheckPartsResp, DeleteOptions, DiskAPI, DiskInfo, DiskInfoOptions, DiskLocation, DiskOption,
    FileInfoVersions, FileReader, FileWriter, MetaCacheEntry, ReadMultipleReq, ReadMultipleResp, ReadOptions, RemoteFileReader,
    RemoteFileWriter, RenameDataResp, UpdateMetadataOpts, VolumeInfo, WalkDirOptions,
};
use crate::utils::proto_err_to_err;
use crate::{
    disk::error::DiskError,
    error::{Error, Result},
    heal::{
        data_scanner::ShouldSleepFn,
        data_usage_cache::{DataUsageCache, DataUsageEntry},
        heal_commands::{HealScanMode, HealingTracker},
    },
    store_api::{FileInfo, RawFileInfo},
};
use protos::proto_gen::node_service::RenamePartRequst;

#[derive(Debug)]
pub struct RemoteDisk {
    pub id: Mutex<Option<Uuid>>,
    pub addr: String,
    pub url: url::Url,
    pub root: PathBuf,
    endpoint: Endpoint,
}

impl RemoteDisk {
    pub async fn new(ep: &Endpoint, _opt: &DiskOption) -> Result<Self> {
        // let root = fs::canonicalize(ep.url.path()).await?;
        let root = PathBuf::from(ep.url.path());
        let addr = format!("{}://{}:{}", ep.url.scheme(), ep.url.host_str().unwrap(), ep.url.port().unwrap());
        Ok(Self {
            id: Mutex::new(None),
            addr,
            url: ep.url.clone(),
            root,
            endpoint: ep.clone(),
        })
    }
}

// TODO: all api need to handle errors
#[async_trait::async_trait]
impl DiskAPI for RemoteDisk {
    fn to_string(&self) -> String {
        self.endpoint.to_string()
    }

    fn is_local(&self) -> bool {
        false
    }

    fn host_name(&self) -> String {
        self.endpoint.host_port()
    }
    async fn is_online(&self) -> bool {
        // TODO: 连接状态
        if (node_service_time_out_client(&self.addr).await).is_ok() {
            return true;
        }
        false
    }
    fn endpoint(&self) -> Endpoint {
        self.endpoint.clone()
    }
    async fn close(&self) -> Result<()> {
        Ok(())
    }
    fn path(&self) -> PathBuf {
        self.root.clone()
    }

    fn get_disk_location(&self) -> DiskLocation {
        DiskLocation {
            pool_idx: {
                if self.endpoint.pool_idx < 0 {
                    None
                } else {
                    Some(self.endpoint.pool_idx as usize)
                }
            },
            set_idx: {
                if self.endpoint.set_idx < 0 {
                    None
                } else {
                    Some(self.endpoint.set_idx as usize)
                }
            },
            disk_idx: {
                if self.endpoint.disk_idx < 0 {
                    None
                } else {
                    Some(self.endpoint.disk_idx as usize)
                }
            },
        }
    }

    async fn get_disk_id(&self) -> Result<Option<Uuid>> {
        Ok(*self.id.lock().await)
    }
    async fn set_disk_id(&self, id: Option<Uuid>) -> Result<()> {
        let mut lock = self.id.lock().await;
        *lock = id;

        Ok(())
    }

    async fn read_all(&self, volume: &str, path: &str) -> Result<Vec<u8>> {
        info!("read_all");
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| Error::from_string(format!("can not get client, err: {}", err)))?;
        let request = Request::new(ReadAllRequest {
            disk: self.endpoint.to_string(),
            volume: volume.to_string(),
            path: path.to_string(),
        });

        let response = client.read_all(request).await?.into_inner();

        info!("read_all success");

        if !response.success {
            return Err(Error::new(DiskError::FileNotFound));
        }

        Ok(response.data)
    }

    async fn write_all(&self, volume: &str, path: &str, data: Vec<u8>) -> Result<()> {
        info!("write_all");
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| Error::from_string(format!("can not get client, err: {}", err)))?;
        let request = Request::new(WriteAllRequest {
            disk: self.endpoint.to_string(),
            volume: volume.to_string(),
            path: path.to_string(),
            data,
        });

        let response = client.write_all(request).await?.into_inner();

        if !response.success {
            return if let Some(err) = &response.error {
                Err(proto_err_to_err(err))
            } else {
                Err(Error::from_string(""))
            };
        }

        Ok(())
    }

    async fn delete(&self, volume: &str, path: &str, opt: DeleteOptions) -> Result<()> {
        info!("delete");
        let options = serde_json::to_string(&opt)?;
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| Error::from_string(format!("can not get client, err: {}", err)))?;
        let request = Request::new(DeleteRequest {
            disk: self.endpoint.to_string(),
            volume: volume.to_string(),
            path: path.to_string(),
            options,
        });

        let response = client.delete(request).await?.into_inner();

        if !response.success {
            return if let Some(err) = &response.error {
                Err(proto_err_to_err(err))
            } else {
                Err(Error::from_string(""))
            };
        }

        Ok(())
    }

    async fn verify_file(&self, volume: &str, path: &str, fi: &FileInfo) -> Result<CheckPartsResp> {
        info!("verify_file");
        let file_info = serde_json::to_string(&fi)?;
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| Error::from_string(format!("can not get client, err: {}", err)))?;
        let request = Request::new(VerifyFileRequest {
            disk: self.endpoint.to_string(),
            volume: volume.to_string(),
            path: path.to_string(),
            file_info,
        });

        let response = client.verify_file(request).await?.into_inner();

        if !response.success {
            return if let Some(err) = &response.error {
                Err(proto_err_to_err(err))
            } else {
                Err(Error::from_string(""))
            };
        }

        let check_parts_resp = serde_json::from_str::<CheckPartsResp>(&response.check_parts_resp)?;

        Ok(check_parts_resp)
    }

    async fn check_parts(&self, volume: &str, path: &str, fi: &FileInfo) -> Result<CheckPartsResp> {
        info!("check_parts");
        let file_info = serde_json::to_string(&fi)?;
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| Error::from_string(format!("can not get client, err: {}", err)))?;
        let request = Request::new(CheckPartsRequest {
            disk: self.endpoint.to_string(),
            volume: volume.to_string(),
            path: path.to_string(),
            file_info,
        });

        let response = client.check_parts(request).await?.into_inner();

        if !response.success {
            return if let Some(err) = &response.error {
                Err(proto_err_to_err(err))
            } else {
                Err(Error::from_string(""))
            };
        }

        let check_parts_resp = serde_json::from_str::<CheckPartsResp>(&response.check_parts_resp)?;

        Ok(check_parts_resp)
    }

    async fn rename_part(&self, src_volume: &str, src_path: &str, dst_volume: &str, dst_path: &str, meta: Vec<u8>) -> Result<()> {
        info!("rename_part");
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| Error::from_string(format!("can not get client, err: {}", err)))?;
        let request = Request::new(RenamePartRequst {
            disk: self.endpoint.to_string(),
            src_volume: src_volume.to_string(),
            src_path: src_path.to_string(),
            dst_volume: dst_volume.to_string(),
            dst_path: dst_path.to_string(),
            meta,
        });

        let response = client.rename_part(request).await?.into_inner();

        if !response.success {
            return if let Some(err) = &response.error {
                Err(proto_err_to_err(err))
            } else {
                Err(Error::from_string(""))
            };
        }

        Ok(())
    }
    async fn rename_file(&self, src_volume: &str, src_path: &str, dst_volume: &str, dst_path: &str) -> Result<()> {
        info!("rename_file");
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| Error::from_string(format!("can not get client, err: {}", err)))?;
        let request = Request::new(RenameFileRequst {
            disk: self.endpoint.to_string(),
            src_volume: src_volume.to_string(),
            src_path: src_path.to_string(),
            dst_volume: dst_volume.to_string(),
            dst_path: dst_path.to_string(),
        });

        let response = client.rename_file(request).await?.into_inner();

        if !response.success {
            return if let Some(err) = &response.error {
                Err(proto_err_to_err(err))
            } else {
                Err(Error::from_string(""))
            };
        }

        Ok(())
    }

    async fn create_file(&self, _origvolume: &str, volume: &str, path: &str, _file_size: usize) -> Result<FileWriter> {
        info!("create_file");
        Ok(FileWriter::Remote(
            RemoteFileWriter::new(
                self.endpoint.clone(),
                volume.to_string(),
                path.to_string(),
                false,
                node_service_time_out_client(&self.addr)
                    .await
                    .map_err(|err| Error::from_string(format!("can not get client, err: {}", err)))?,
            )
            .await?,
        ))
    }

    async fn append_file(&self, volume: &str, path: &str) -> Result<FileWriter> {
        info!("append_file");
        Ok(FileWriter::Remote(
            RemoteFileWriter::new(
                self.endpoint.clone(),
                volume.to_string(),
                path.to_string(),
                true,
                node_service_time_out_client(&self.addr)
                    .await
                    .map_err(|err| Error::from_string(format!("can not get client, err: {}", err)))?,
            )
            .await?,
        ))
    }

    async fn read_file(&self, volume: &str, path: &str) -> Result<FileReader> {
        info!("read_file");
        Ok(FileReader::Remote(
            RemoteFileReader::new(
                self.endpoint.clone(),
                volume.to_string(),
                path.to_string(),
                node_service_time_out_client(&self.addr)
                    .await
                    .map_err(|err| Error::from_string(format!("can not get client, err: {}", err)))?,
            )
            .await?,
        ))
    }

    async fn list_dir(&self, _origvolume: &str, volume: &str, _dir_path: &str, _count: i32) -> Result<Vec<String>> {
        info!("list_dir");
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| Error::from_string(format!("can not get client, err: {}", err)))?;
        let request = Request::new(ListDirRequest {
            disk: self.endpoint.to_string(),
            volume: volume.to_string(),
        });

        let response = client.list_dir(request).await?.into_inner();

        if !response.success {
            return if let Some(err) = &response.error {
                Err(proto_err_to_err(err))
            } else {
                Err(Error::from_string(""))
            };
        }

        Ok(response.volumes)
    }

    async fn walk_dir(&self, opts: WalkDirOptions) -> Result<Vec<MetaCacheEntry>> {
        info!("walk_dir");
        let walk_dir_options = serde_json::to_string(&opts)?;
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| Error::from_string(format!("can not get client, err: {}", err)))?;
        let request = Request::new(WalkDirRequest {
            disk: self.endpoint.to_string(),
            walk_dir_options,
        });

        let response = client.walk_dir(request).await?.into_inner();

        if !response.success {
            return if let Some(err) = &response.error {
                Err(proto_err_to_err(err))
            } else {
                Err(Error::from_string(""))
            };
        }

        let entries = response
            .meta_cache_entry
            .into_iter()
            .filter_map(|json_str| serde_json::from_str::<MetaCacheEntry>(&json_str).ok())
            .collect();

        Ok(entries)
    }

    async fn rename_data(
        &self,
        src_volume: &str,
        src_path: &str,
        fi: FileInfo,
        dst_volume: &str,
        dst_path: &str,
    ) -> Result<RenameDataResp> {
        info!("rename_data");
        let file_info = serde_json::to_string(&fi)?;
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| Error::from_string(format!("can not get client, err: {}", err)))?;
        let request = Request::new(RenameDataRequest {
            disk: self.endpoint.to_string(),
            src_volume: src_volume.to_string(),
            src_path: src_path.to_string(),
            file_info,
            dst_volume: dst_volume.to_string(),
            dst_path: dst_path.to_string(),
        });

        let response = client.rename_data(request).await?.into_inner();

        if !response.success {
            return if let Some(err) = &response.error {
                Err(proto_err_to_err(err))
            } else {
                Err(Error::from_string(""))
            };
        }

        let rename_data_resp = serde_json::from_str::<RenameDataResp>(&response.rename_data_resp)?;

        Ok(rename_data_resp)
    }

    async fn make_volumes(&self, volumes: Vec<&str>) -> Result<()> {
        info!("make_volumes");
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| Error::from_string(format!("can not get client, err: {}", err)))?;
        let request = Request::new(MakeVolumesRequest {
            disk: self.endpoint.to_string(),
            volumes: volumes.iter().map(|s| (*s).to_string()).collect(),
        });

        let response = client.make_volumes(request).await?.into_inner();

        if !response.success {
            return if let Some(err) = &response.error {
                Err(proto_err_to_err(err))
            } else {
                Err(Error::from_string(""))
            };
        }

        Ok(())
    }

    async fn make_volume(&self, volume: &str) -> Result<()> {
        info!("make_volume");
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| Error::from_string(format!("can not get client, err: {}", err)))?;
        let request = Request::new(MakeVolumeRequest {
            disk: self.endpoint.to_string(),
            volume: volume.to_string(),
        });

        let response = client.make_volume(request).await?.into_inner();

        if !response.success {
            return if let Some(err) = &response.error {
                Err(proto_err_to_err(err))
            } else {
                Err(Error::from_string(""))
            };
        }

        Ok(())
    }

    async fn list_volumes(&self) -> Result<Vec<VolumeInfo>> {
        info!("list_volumes");
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| Error::from_string(format!("can not get client, err: {}", err)))?;
        let request = Request::new(ListVolumesRequest {
            disk: self.endpoint.to_string(),
        });

        let response = client.list_volumes(request).await?.into_inner();

        if !response.success {
            return if let Some(err) = &response.error {
                Err(proto_err_to_err(err))
            } else {
                Err(Error::from_string(""))
            };
        }

        let infos = response
            .volume_infos
            .into_iter()
            .filter_map(|json_str| serde_json::from_str::<VolumeInfo>(&json_str).ok())
            .collect();

        Ok(infos)
    }

    async fn stat_volume(&self, volume: &str) -> Result<VolumeInfo> {
        info!("stat_volume");
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| Error::from_string(format!("can not get client, err: {}", err)))?;
        let request = Request::new(StatVolumeRequest {
            disk: self.endpoint.to_string(),
            volume: volume.to_string(),
        });

        let response = client.stat_volume(request).await?.into_inner();

        if !response.success {
            return if let Some(err) = &response.error {
                Err(proto_err_to_err(err))
            } else {
                Err(Error::from_string(""))
            };
        }

        let volume_info = serde_json::from_str::<VolumeInfo>(&response.volume_info)?;

        Ok(volume_info)
    }

    async fn delete_paths(&self, volume: &str, paths: &[&str]) -> Result<()> {
        info!("delete_paths");
        let paths = paths.iter().map(|s| s.to_string()).collect::<Vec<String>>();
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| Error::from_string(format!("can not get client, err: {}", err)))?;
        let request = Request::new(DeletePathsRequest {
            disk: self.endpoint.to_string(),
            volume: volume.to_string(),
            paths,
        });

        let response = client.delete_paths(request).await?.into_inner();

        if !response.success {
            return if let Some(err) = &response.error {
                Err(proto_err_to_err(err))
            } else {
                Err(Error::from_string(""))
            };
        }

        Ok(())
    }
    async fn update_metadata(&self, volume: &str, path: &str, fi: FileInfo, opts: &UpdateMetadataOpts) -> Result<()> {
        info!("update_metadata");
        let file_info = serde_json::to_string(&fi)?;
        let opts = serde_json::to_string(&opts)?;

        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| Error::from_string(format!("can not get client, err: {}", err)))?;
        let request = Request::new(UpdateMetadataRequest {
            disk: self.endpoint.to_string(),
            volume: volume.to_string(),
            path: path.to_string(),
            file_info,
            opts,
        });

        let response = client.update_metadata(request).await?.into_inner();

        if !response.success {
            return if let Some(err) = &response.error {
                Err(proto_err_to_err(err))
            } else {
                Err(Error::from_string(""))
            };
        }

        Ok(())
    }

    async fn write_metadata(&self, _org_volume: &str, volume: &str, path: &str, fi: FileInfo) -> Result<()> {
        info!("write_metadata");
        let file_info = serde_json::to_string(&fi)?;
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| Error::from_string(format!("can not get client, err: {}", err)))?;
        let request = Request::new(WriteMetadataRequest {
            disk: self.endpoint.to_string(),
            volume: volume.to_string(),
            path: path.to_string(),
            file_info,
        });

        let response = client.write_metadata(request).await?.into_inner();

        if !response.success {
            return if let Some(err) = &response.error {
                Err(proto_err_to_err(err))
            } else {
                Err(Error::from_string(""))
            };
        }

        Ok(())
    }

    async fn read_version(
        &self,
        _org_volume: &str,
        volume: &str,
        path: &str,
        version_id: &str,
        opts: &ReadOptions,
    ) -> Result<FileInfo> {
        info!("read_version");
        let opts = serde_json::to_string(opts)?;
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| Error::from_string(format!("can not get client, err: {}", err)))?;
        let request = Request::new(ReadVersionRequest {
            disk: self.endpoint.to_string(),
            volume: volume.to_string(),
            path: path.to_string(),
            version_id: version_id.to_string(),
            opts,
        });

        let response = client.read_version(request).await?.into_inner();

        if !response.success {
            return if let Some(err) = &response.error {
                Err(proto_err_to_err(err))
            } else {
                Err(Error::from_string(""))
            };
        }

        let file_info = serde_json::from_str::<FileInfo>(&response.file_info)?;

        Ok(file_info)
    }

    async fn read_xl(&self, volume: &str, path: &str, read_data: bool) -> Result<RawFileInfo> {
        info!("read_xl");
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| Error::from_string(format!("can not get client, err: {}", err)))?;
        let request = Request::new(ReadXlRequest {
            disk: self.endpoint.to_string(),
            volume: volume.to_string(),
            path: path.to_string(),
            read_data,
        });

        let response = client.read_xl(request).await?.into_inner();

        if !response.success {
            return if let Some(err) = &response.error {
                Err(proto_err_to_err(err))
            } else {
                Err(Error::from_string(""))
            };
        }

        let raw_file_info = serde_json::from_str::<RawFileInfo>(&response.raw_file_info)?;

        Ok(raw_file_info)
    }
    async fn delete_version(
        &self,
        volume: &str,
        path: &str,
        fi: FileInfo,
        force_del_marker: bool,
        opts: DeleteOptions,
    ) -> Result<()> {
        info!("delete_version");
        let file_info = serde_json::to_string(&fi)?;
        let opts = serde_json::to_string(&opts)?;

        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| Error::from_string(format!("can not get client, err: {}", err)))?;
        let request = Request::new(DeleteVersionRequest {
            disk: self.endpoint.to_string(),
            volume: volume.to_string(),
            path: path.to_string(),
            file_info,
            force_del_marker,
            opts,
        });

        let response = client.delete_version(request).await?.into_inner();

        if !response.success {
            return if let Some(err) = &response.error {
                Err(proto_err_to_err(err))
            } else {
                Err(Error::from_string(""))
            };
        }

        // let raw_file_info = serde_json::from_str::<RawFileInfo>(&response.raw_file_info)?;

        Ok(())
    }
    async fn delete_versions(
        &self,
        volume: &str,
        versions: Vec<FileInfoVersions>,
        opts: DeleteOptions,
    ) -> Result<Vec<Option<Error>>> {
        info!("delete_versions");
        let opts = serde_json::to_string(&opts)?;
        let mut versions_str = Vec::with_capacity(versions.len());
        for file_info_versions in versions.iter() {
            versions_str.push(serde_json::to_string(file_info_versions)?);
        }
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| Error::from_string(format!("can not get client, err: {}", err)))?;
        let request = Request::new(DeleteVersionsRequest {
            disk: self.endpoint.to_string(),
            volume: volume.to_string(),
            versions: versions_str,
            opts,
        });

        let response = client.delete_versions(request).await?.into_inner();
        if !response.success {
            return if let Some(err) = &response.error {
                Err(proto_err_to_err(err))
            } else {
                Err(Error::from_string(""))
            };
        }
        let errors = response
            .errors
            .iter()
            .map(|error| {
                if error.is_empty() {
                    None
                } else {
                    Some(Error::from_string(error))
                }
            })
            .collect();

        Ok(errors)
    }

    async fn read_multiple(&self, req: ReadMultipleReq) -> Result<Vec<ReadMultipleResp>> {
        info!("read_multiple");
        let read_multiple_req = serde_json::to_string(&req)?;
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| Error::from_string(format!("can not get client, err: {}", err)))?;
        let request = Request::new(ReadMultipleRequest {
            disk: self.endpoint.to_string(),
            read_multiple_req,
        });

        let response = client.read_multiple(request).await?.into_inner();

        if !response.success {
            return if let Some(err) = &response.error {
                Err(proto_err_to_err(err))
            } else {
                Err(Error::from_string(""))
            };
        }

        let read_multiple_resps = response
            .read_multiple_resps
            .into_iter()
            .filter_map(|json_str| serde_json::from_str::<ReadMultipleResp>(&json_str).ok())
            .collect();

        Ok(read_multiple_resps)
    }

    async fn delete_volume(&self, volume: &str) -> Result<()> {
        info!("delete_volume");
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| Error::from_string(format!("can not get client, err: {}", err)))?;
        let request = Request::new(DeleteVolumeRequest {
            disk: self.endpoint.to_string(),
            volume: volume.to_string(),
        });

        let response = client.delete_volume(request).await?.into_inner();

        if !response.success {
            return if let Some(err) = &response.error {
                Err(proto_err_to_err(err))
            } else {
                Err(Error::from_string(""))
            };
        }

        Ok(())
    }

    async fn disk_info(&self, opts: &DiskInfoOptions) -> Result<DiskInfo> {
        info!("delete_volume");
        let opts = serde_json::to_string(&opts)?;
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| Error::from_string(format!("can not get client, err: {}", err)))?;
        let request = Request::new(DiskInfoRequest {
            disk: self.endpoint.to_string(),
            opts,
        });

        let response = client.disk_info(request).await?.into_inner();

        if !response.success {
            return if let Some(err) = &response.error {
                Err(proto_err_to_err(err))
            } else {
                Err(Error::from_string(""))
            };
        }

        let disk_info = serde_json::from_str::<DiskInfo>(&response.disk_info)?;

        Ok(disk_info)
    }

    async fn ns_scanner(
        &self,
        cache: &DataUsageCache,
        updates: Sender<DataUsageEntry>,
        scan_mode: HealScanMode,
        _we_sleep: ShouldSleepFn,
    ) -> Result<DataUsageCache> {
        info!("ns_scanner");
        let cache = serde_json::to_string(cache)?;
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| Error::from_string(format!("can not get client, err: {}", err)))?;

        let (tx, rx) = mpsc::channel(10);
        let in_stream = ReceiverStream::new(rx);
        let mut response = client.ns_scanner(in_stream).await?.into_inner();
        let request = NsScannerRequest {
            disk: self.endpoint.to_string(),
            cache,
            scan_mode: scan_mode as u64,
        };
        tx.send(request).await?;

        loop {
            match response.next().await {
                Some(Ok(resp)) => {
                    if !resp.update.is_empty() {
                        let data_usage_cache = serde_json::from_str::<DataUsageEntry>(&resp.update)?;
                        let _ = updates.send(data_usage_cache).await;
                    } else if !resp.data_usage_cache.is_empty() {
                        let data_usage_cache = serde_json::from_str::<DataUsageCache>(&resp.data_usage_cache)?;
                        return Ok(data_usage_cache);
                    } else {
                        return Err(Error::from_string("scan was interrupted"));
                    }
                }
                _ => return Err(Error::from_string("scan was interrupted")),
            }
        }
    }

    async fn healing(&self) -> Option<HealingTracker> {
        None
    }
}
