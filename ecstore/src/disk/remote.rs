use std::{path::PathBuf, sync::Arc, time::Duration};

use bytes::Bytes;
use futures::lock::Mutex;
use protos::{
    node_service_time_out_client,
    proto_gen::node_service::{
        node_service_client::NodeServiceClient, DeleteRequest, DeleteVersionsRequest, DeleteVolumeRequest, ListDirRequest,
        ListVolumesRequest, MakeVolumeRequest, MakeVolumesRequest, ReadAllRequest, ReadMultipleRequest, ReadVersionRequest,
        ReadXlRequest, RenameDataRequest, RenameFileRequst, StatVolumeRequest, WalkDirRequest, WriteAllRequest,
        WriteMetadataRequest,
    },
    DEFAULT_GRPC_SERVER_MESSAGE_LEN,
};
use tokio::{fs, sync::RwLock};
use tonic::{
    transport::{Channel, Endpoint as tonic_Endpoint},
    Request,
};
use tower::timeout::Timeout;
use tracing::info;
use uuid::Uuid;

use crate::{
    disk::error::DiskError,
    error::{Error, Result},
    store_api::{FileInfo, RawFileInfo},
};

use super::{
    endpoint::Endpoint, DeleteOptions, DiskAPI, DiskLocation, DiskOption, FileInfoVersions, FileReader, FileWriter,
    MetaCacheEntry, ReadMultipleReq, ReadMultipleResp, ReadOptions, RemoteFileReader, RemoteFileWriter, RenameDataResp,
    UpdateMetadataOpts, VolumeInfo, WalkDirOptions,
};

#[derive(Debug)]
pub struct RemoteDisk {
    id: Mutex<Option<Uuid>>,
    channel: Arc<RwLock<Option<Channel>>>,
    url: url::Url,
    pub root: PathBuf,
    endpoint: Endpoint,
}

impl RemoteDisk {
    pub async fn new(ep: &Endpoint, _opt: &DiskOption) -> Result<Self> {
        let root = fs::canonicalize(ep.url.path()).await?;

        Ok(Self {
            channel: Arc::new(RwLock::new(None)),
            url: ep.url.clone(),
            root,
            id: Mutex::new(None),
            endpoint: ep.clone(),
        })
    }

    #[allow(dead_code)]
    async fn get_client(&self) -> Result<NodeServiceClient<Timeout<Channel>>> {
        let channel_clone = self.channel.clone();
        let channel = {
            let read_lock = channel_clone.read().await;

            if let Some(ref channel) = *read_lock {
                channel.clone()
            } else {
                let addr = format!("{}://{}:{}", self.url.scheme(), self.url.host_str().unwrap(), self.url.port().unwrap());
                info!("disk url: {}", addr);
                let connector = tonic_Endpoint::from_shared(addr.clone())?;

                let new_channel = connector.connect().await.map_err(|_err| DiskError::DiskNotFound)?;

                info!("get channel success");

                *self.channel.write().await = Some(new_channel.clone());

                new_channel
            }
        };

        Ok(node_service_time_out_client(
            channel,
            Duration::new(30, 0), // TODO: use config setting
            DEFAULT_GRPC_SERVER_MESSAGE_LEN,
            // grpc_enable_gzip,
            false, // TODO: use config setting
        ))
    }

    async fn get_client_v2(&self) -> Result<NodeServiceClient<tonic::transport::Channel>> {
        // Ok(NodeServiceClient::connect("http://220.181.1.138:9000").await?)
        let addr = format!("{}://{}:{}", self.url.scheme(), self.url.host_str().unwrap(), self.url.port().unwrap());
        Ok(NodeServiceClient::connect(addr).await?)
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
        if let Ok(_) = self.get_client_v2().await {
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
            pool_idx: self.endpoint.pool_idx,
            set_idx: self.endpoint.set_idx,
            disk_idx: self.endpoint.pool_idx,
        }
    }

    async fn get_disk_id(&self) -> Result<Option<Uuid>> {
        Ok(self.id.lock().await.clone())
    }
    async fn set_disk_id(&self, id: Option<Uuid>) -> Result<()> {
        let mut lock = self.id.lock().await;
        *lock = id;

        Ok(())
    }

    async fn read_all(&self, volume: &str, path: &str) -> Result<Bytes> {
        info!("read_all");
        let mut client = self.get_client_v2().await?;
        let request = Request::new(ReadAllRequest {
            disk: self.root.to_string_lossy().to_string(),
            volume: volume.to_string(),
            path: path.to_string(),
        });

        let response = client.read_all(request).await?.into_inner();

        info!("read_all success");

        if !response.success {
            return Err(DiskError::FileNotFound.into());
        }

        Ok(Bytes::from(response.data))
    }

    async fn write_all(&self, volume: &str, path: &str, data: Vec<u8>) -> Result<()> {
        info!("write_all");
        let mut client = self.get_client_v2().await?;
        let request = Request::new(WriteAllRequest {
            disk: self.root.to_string_lossy().to_string(),
            volume: volume.to_string(),
            path: path.to_string(),
            data,
        });

        let response = client.write_all(request).await?.into_inner();

        if !response.success {
            return Err(Error::from_string(response.error_info.unwrap_or("".to_string())));
        }

        Ok(())
    }

    async fn delete(&self, volume: &str, path: &str, opt: DeleteOptions) -> Result<()> {
        info!("delete");
        let options = serde_json::to_string(&opt)?;
        let mut client = self.get_client_v2().await?;
        let request = Request::new(DeleteRequest {
            disk: self.root.to_string_lossy().to_string(),
            volume: volume.to_string(),
            path: path.to_string(),
            options,
        });

        let response = client.delete(request).await?.into_inner();

        if !response.success {
            return Err(Error::from_string(response.error_info.unwrap_or("".to_string())));
        }

        Ok(())
    }
    async fn rename_part(
        &self,
        _src_volume: &str,
        _src_path: &str,
        _dst_volume: &str,
        _dst_path: &str,
        _meta: Vec<u8>,
    ) -> Result<()> {
        unimplemented!()
    }
    async fn rename_file(&self, src_volume: &str, src_path: &str, dst_volume: &str, dst_path: &str) -> Result<()> {
        info!("rename_file");
        let mut client = self.get_client_v2().await?;
        let request = Request::new(RenameFileRequst {
            disk: self.root.to_string_lossy().to_string(),
            src_volume: src_volume.to_string(),
            src_path: src_path.to_string(),
            dst_volume: dst_volume.to_string(),
            dst_path: dst_path.to_string(),
        });

        let response = client.rename_file(request).await?.into_inner();

        if !response.success {
            return Err(Error::from_string(response.error_info.unwrap_or("".to_string())));
        }

        Ok(())
    }

    async fn create_file(&self, _origvolume: &str, volume: &str, path: &str, _file_size: usize) -> Result<FileWriter> {
        info!("create_file");
        Ok(FileWriter::Remote(RemoteFileWriter::new(
            self.root.clone(),
            volume.to_string(),
            path.to_string(),
            false,
            self.get_client_v2().await?,
        )))
    }

    async fn append_file(&self, volume: &str, path: &str) -> Result<FileWriter> {
        info!("append_file");
        Ok(FileWriter::Remote(RemoteFileWriter::new(
            self.root.clone(),
            volume.to_string(),
            path.to_string(),
            true,
            self.get_client_v2().await?,
        )))
    }

    async fn read_file(&self, volume: &str, path: &str) -> Result<FileReader> {
        info!("read_file");
        Ok(FileReader::Remote(RemoteFileReader::new(
            self.root.clone(),
            volume.to_string(),
            path.to_string(),
            self.get_client_v2().await?,
        )))
    }

    async fn list_dir(&self, _origvolume: &str, volume: &str, _dir_path: &str, _count: i32) -> Result<Vec<String>> {
        info!("list_dir");
        let mut client = self.get_client_v2().await?;
        let request = Request::new(ListDirRequest {
            disk: self.root.to_string_lossy().to_string(),
            volume: volume.to_string(),
        });

        let response = client.list_dir(request).await?.into_inner();

        if !response.success {
            return Err(Error::from_string(response.error_info.unwrap_or("".to_string())));
        }

        Ok(response.volumes)
    }

    async fn walk_dir(&self, opts: WalkDirOptions) -> Result<Vec<MetaCacheEntry>> {
        info!("walk_dir");
        let walk_dir_options = serde_json::to_string(&opts)?;
        let mut client = self.get_client_v2().await?;
        let request = Request::new(WalkDirRequest {
            disk: self.root.to_string_lossy().to_string(),
            walk_dir_options,
        });

        let response = client.walk_dir(request).await?.into_inner();

        if !response.success {
            return Err(Error::from_string(response.error_info.unwrap_or("".to_string())));
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
        let mut client = self.get_client_v2().await?;
        let request = Request::new(RenameDataRequest {
            disk: self.root.to_string_lossy().to_string(),
            src_volume: src_volume.to_string(),
            src_path: src_path.to_string(),
            file_info,
            dst_volume: dst_volume.to_string(),
            dst_path: dst_path.to_string(),
        });

        let response = client.rename_data(request).await?.into_inner();

        if !response.success {
            return Err(Error::from_string(response.error_info.unwrap_or("".to_string())));
        }

        let rename_data_resp = serde_json::from_str::<RenameDataResp>(&response.rename_data_resp)?;

        Ok(rename_data_resp)
    }

    async fn make_volumes(&self, volumes: Vec<&str>) -> Result<()> {
        info!("make_volumes");
        let mut client = self.get_client_v2().await?;
        let request = Request::new(MakeVolumesRequest {
            disk: self.root.to_string_lossy().to_string(),
            volumes: volumes.iter().map(|s| (*s).to_string()).collect(),
        });

        let response = client.make_volumes(request).await?.into_inner();

        if !response.success {
            return Err(Error::from_string(response.error_info.unwrap_or("".to_string())));
        }

        Ok(())
    }

    async fn make_volume(&self, volume: &str) -> Result<()> {
        info!("make_volume");
        let mut client = self.get_client_v2().await?;
        let request = Request::new(MakeVolumeRequest {
            disk: self.root.to_string_lossy().to_string(),
            volume: volume.to_string(),
        });

        let response = client.make_volume(request).await?.into_inner();

        if !response.success {
            return Err(Error::from_string(response.error_info.unwrap_or("".to_string())));
        }

        Ok(())
    }

    async fn list_volumes(&self) -> Result<Vec<VolumeInfo>> {
        info!("list_volumes");
        let mut client = self.get_client_v2().await?;
        let request = Request::new(ListVolumesRequest {
            disk: self.root.to_string_lossy().to_string(),
        });

        let response = client.list_volumes(request).await?.into_inner();

        if !response.success {
            return Err(Error::from_string(response.error_info.unwrap_or("".to_string())));
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
        let mut client = self.get_client_v2().await?;
        let request = Request::new(StatVolumeRequest {
            disk: self.root.to_string_lossy().to_string(),
            volume: volume.to_string(),
        });

        let response = client.stat_volume(request).await?.into_inner();

        if !response.success {
            return Err(Error::from_string(response.error_info.unwrap_or("".to_string())));
        }

        let volume_info = serde_json::from_str::<VolumeInfo>(&response.volume_info)?;

        Ok(volume_info)
    }

    async fn delete_paths(&self, _volume: &str, _paths: &[&str]) -> Result<()> {
        // TODO:
        unimplemented!()
    }
    async fn update_metadata(&self, _volume: &str, _path: &str, _fi: FileInfo, _opts: UpdateMetadataOpts) {
        // TODO:
        unimplemented!()
    }

    async fn write_metadata(&self, _org_volume: &str, volume: &str, path: &str, fi: FileInfo) -> Result<()> {
        info!("write_metadata");
        let file_info = serde_json::to_string(&fi)?;
        let mut client = self.get_client_v2().await?;
        let request = Request::new(WriteMetadataRequest {
            disk: self.root.to_string_lossy().to_string(),
            volume: volume.to_string(),
            path: path.to_string(),
            file_info,
        });

        let response = client.write_metadata(request).await?.into_inner();

        if !response.success {
            return Err(Error::from_string(response.error_info.unwrap_or("".to_string())));
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
        let mut client = self.get_client_v2().await?;
        let request = Request::new(ReadVersionRequest {
            disk: self.root.to_string_lossy().to_string(),
            volume: volume.to_string(),
            path: path.to_string(),
            version_id: version_id.to_string(),
            opts,
        });

        let response = client.read_version(request).await?.into_inner();

        if !response.success {
            return Err(Error::from_string(response.error_info.unwrap_or("".to_string())));
        }

        let file_info = serde_json::from_str::<FileInfo>(&response.file_info)?;

        Ok(file_info)
    }

    async fn read_xl(&self, volume: &str, path: &str, read_data: bool) -> Result<RawFileInfo> {
        info!("read_xl");
        let mut client = self.get_client_v2().await?;
        let request = Request::new(ReadXlRequest {
            disk: self.root.to_string_lossy().to_string(),
            volume: volume.to_string(),
            path: path.to_string(),
            read_data,
        });

        let response = client.read_xl(request).await?.into_inner();

        if !response.success {
            return Err(Error::from_string(response.error_info.unwrap_or("".to_string())));
        }

        let raw_file_info = serde_json::from_str::<RawFileInfo>(&response.raw_file_info)?;

        Ok(raw_file_info)
    }
    async fn delete_version(
        &self,
        _volume: &str,
        _path: &str,
        _fi: FileInfo,
        _force_del_marker: bool,
        _opts: DeleteOptions,
    ) -> Result<RawFileInfo> {
        unimplemented!()
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
        let mut client = self.get_client_v2().await?;
        let request = Request::new(DeleteVersionsRequest {
            disk: self.root.to_string_lossy().to_string(),
            volume: volume.to_string(),
            versions: versions_str,
            opts,
        });

        let response = client.delete_versions(request).await?.into_inner();
        if !response.success {
            return Err(Error::from_string(format!(
                "delete versions remote err: {}",
                response.error_info.unwrap_or("None".to_string())
            )));
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
        let mut client = self.get_client_v2().await?;
        let request = Request::new(ReadMultipleRequest {
            disk: self.root.to_string_lossy().to_string(),
            read_multiple_req,
        });

        let response = client.read_multiple(request).await?.into_inner();

        if !response.success {
            return Err(Error::from_string(response.error_info.unwrap_or("".to_string())));
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
        let mut client = self.get_client_v2().await?;
        let request = Request::new(DeleteVolumeRequest {
            disk: self.root.to_string_lossy().to_string(),
            volume: volume.to_string(),
        });

        let response = client.delete_volume(request).await?.into_inner();

        if !response.success {
            return Err(Error::from_string(response.error_info.unwrap_or("".to_string())));
        }

        Ok(())
    }
}
