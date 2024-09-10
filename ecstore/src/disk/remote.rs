use std::{path::PathBuf, sync::RwLock, time::Duration};

use bytes::Bytes;
use protos::{
    node_service_time_out_client,
    proto_gen::node_service::{
        node_service_client::NodeServiceClient, DeleteRequest, DeleteVolumeRequest, ListDirRequest, ListVolumesRequest,
        MakeVolumeRequest, MakeVolumesRequest, ReadAllRequest, ReadMultipleRequest, ReadVersionRequest, ReadXlRequest,
        RenameDataRequest, RenameFileRequst, StatVolumeRequest, WalkDirRequest, WriteAllRequest, WriteMetadataRequest,
    },
    DEFAULT_GRPC_SERVER_MESSAGE_LEN,
};
use tokio::fs;
use tonic::{
    transport::{Channel, Endpoint as tonic_Endpoint},
    Request,
};
use tower::timeout::Timeout;
use tracing::info;
use uuid::Uuid;

use crate::{
    error::{Error, Result},
    store_api::{FileInfo, RawFileInfo},
};

use super::{
    endpoint::Endpoint, DeleteOptions, DiskAPI, DiskOption, FileInfoVersions, FileReader, FileWriter, MetaCacheEntry,
    ReadMultipleReq, ReadMultipleResp, ReadOptions, RemoteFileReader, RemoteFileWriter, RenameDataResp, VolumeInfo,
    WalkDirOptions,
};

#[derive(Debug)]
pub struct RemoteDisk {
    channel: RwLock<Option<Channel>>,
    url: url::Url,
    pub root: PathBuf,
}

impl RemoteDisk {
    pub async fn new(ep: &Endpoint, _opt: &DiskOption) -> Result<Self> {
        let root = fs::canonicalize(ep.url.path()).await?;

        Ok(Self {
            channel: RwLock::new(None),
            url: ep.url.clone(),
            root,
        })
    }

    fn get_client(&self) -> NodeServiceClient<Timeout<Channel>> {
        let channel = {
            let read_lock = self.channel.read().unwrap();

            if let Some(ref channel) = *read_lock {
                channel.clone()
            } else {
                let addr = format!("{}://{}:{}", self.url.scheme(), self.url.host_str().unwrap(), self.url.port().unwrap());
                info!("disk url: {:?}", addr);
                let connector = tonic_Endpoint::from_shared(addr).unwrap();

                let new_channel = tokio::runtime::Runtime::new().unwrap().block_on(connector.connect()).unwrap();

                *self.channel.write().unwrap() = Some(new_channel.clone());

                new_channel
            }
        };

        node_service_time_out_client(
            channel,
            Duration::new(30, 0), // TODO: use config setting
            DEFAULT_GRPC_SERVER_MESSAGE_LEN,
            // grpc_enable_gzip,
            false, // TODO: use config setting
        )
    }
}

// TODO: all api need to handle errors
#[async_trait::async_trait]
impl DiskAPI for RemoteDisk {
    fn is_local(&self) -> bool {
        false
    }

    fn id(&self) -> Uuid {
        Uuid::nil()
    }

    fn path(&self) -> PathBuf {
        self.root.clone()
    }

    async fn read_all(&self, volume: &str, path: &str) -> Result<Bytes> {
        let mut client = self.get_client();
        let request = Request::new(ReadAllRequest {
            disk: self.root.to_string_lossy().to_string(),
            volume: volume.to_string(),
            path: path.to_string(),
        });

        let response = client.read_all(request).await?.into_inner();

        Ok(Bytes::from(response.data))
    }

    async fn write_all(&self, volume: &str, path: &str, data: Vec<u8>) -> Result<()> {
        let mut client = self.get_client();
        let request = Request::new(WriteAllRequest {
            disk: self.root.to_string_lossy().to_string(),
            volume: volume.to_string(),
            path: path.to_string(),
            data,
        });

        let _response = client.write_all(request).await?.into_inner();

        Ok(())
    }

    async fn delete(&self, volume: &str, path: &str, opt: DeleteOptions) -> Result<()> {
        let options = serde_json::to_string(&opt)?;
        let mut client = self.get_client();
        let request = Request::new(DeleteRequest {
            disk: self.root.to_string_lossy().to_string(),
            volume: volume.to_string(),
            path: path.to_string(),
            options,
        });

        let _response = client.delete(request).await?.into_inner();

        Ok(())
    }

    async fn rename_file(&self, src_volume: &str, src_path: &str, dst_volume: &str, dst_path: &str) -> Result<()> {
        let mut client = self.get_client();
        let request = Request::new(RenameFileRequst {
            disk: self.root.to_string_lossy().to_string(),
            src_volume: src_volume.to_string(),
            src_path: src_path.to_string(),
            dst_volume: dst_volume.to_string(),
            dst_path: dst_path.to_string(),
        });

        let _response = client.rename_file(request).await?.into_inner();

        Ok(())
    }

    async fn create_file(&self, _origvolume: &str, volume: &str, path: &str, _file_size: usize) -> Result<FileWriter> {
        Ok(FileWriter::Remote(RemoteFileWriter::new(
            self.root.clone(),
            volume.to_string(),
            path.to_string(),
            false,
            self.get_client(),
        )))
    }

    async fn append_file(&self, volume: &str, path: &str) -> Result<FileWriter> {
        Ok(FileWriter::Remote(RemoteFileWriter::new(
            self.root.clone(),
            volume.to_string(),
            path.to_string(),
            true,
            self.get_client(),
        )))
    }

    async fn read_file(&self, volume: &str, path: &str) -> Result<FileReader> {
        Ok(FileReader::Remote(RemoteFileReader::new(
            self.root.clone(),
            volume.to_string(),
            path.to_string(),
            self.get_client(),
        )))
    }

    async fn list_dir(&self, _origvolume: &str, volume: &str, _dir_path: &str, _count: i32) -> Result<Vec<String>> {
        let mut client = self.get_client();
        let request = Request::new(ListDirRequest {
            disk: self.root.to_string_lossy().to_string(),
            volume: volume.to_string(),
        });

        let response = client.list_dir(request).await?.into_inner();

        Ok(response.volumes)
    }

    async fn walk_dir(&self, opts: WalkDirOptions) -> Result<Vec<MetaCacheEntry>> {
        let walk_dir_options = serde_json::to_string(&opts)?;
        let mut client = self.get_client();
        let request = Request::new(WalkDirRequest {
            disk: self.root.to_string_lossy().to_string(),
            walk_dir_options,
        });

        let response = client.walk_dir(request).await?.into_inner();
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
        let file_info = serde_json::to_string(&fi)?;
        let mut client = self.get_client();
        let request = Request::new(RenameDataRequest {
            disk: self.root.to_string_lossy().to_string(),
            src_volume: src_volume.to_string(),
            src_path: src_path.to_string(),
            file_info,
            dst_volume: dst_volume.to_string(),
            dst_path: dst_path.to_string(),
        });

        let response = client.rename_data(request).await?.into_inner();
        let rename_data_resp = serde_json::from_str::<RenameDataResp>(&response.rename_data_resp)?;

        Ok(rename_data_resp)
    }

    async fn make_volumes(&self, volumes: Vec<&str>) -> Result<()> {
        let mut client = self.get_client();
        let request = Request::new(MakeVolumesRequest {
            disk: self.root.to_string_lossy().to_string(),
            volumes: volumes.iter().map(|s| (*s).to_string()).collect(),
        });

        let _response = client.make_volumes(request).await?.into_inner();

        Ok(())
    }

    async fn make_volume(&self, volume: &str) -> Result<()> {
        let mut client = self.get_client();
        let request = Request::new(MakeVolumeRequest {
            disk: self.root.to_string_lossy().to_string(),
            volume: volume.to_string(),
        });

        let _response = client.make_volume(request).await?.into_inner();

        Ok(())
    }

    async fn list_volumes(&self) -> Result<Vec<VolumeInfo>> {
        let mut client = self.get_client();
        let request = Request::new(ListVolumesRequest {
            disk: self.root.to_string_lossy().to_string(),
        });

        let response = client.list_volumes(request).await?.into_inner();
        let infos = response
            .volume_infos
            .into_iter()
            .filter_map(|json_str| serde_json::from_str::<VolumeInfo>(&json_str).ok())
            .collect();

        Ok(infos)
    }

    async fn stat_volume(&self, volume: &str) -> Result<VolumeInfo> {
        let mut client = self.get_client();
        let request = Request::new(StatVolumeRequest {
            disk: self.root.to_string_lossy().to_string(),
            volume: volume.to_string(),
        });

        let response = client.stat_volume(request).await?.into_inner();
        let volume_info = serde_json::from_str::<VolumeInfo>(&response.volume_info)?;

        Ok(volume_info)
    }

    async fn write_metadata(&self, _org_volume: &str, volume: &str, path: &str, fi: FileInfo) -> Result<()> {
        let file_info = serde_json::to_string(&fi)?;
        let mut client = self.get_client();
        let request = Request::new(WriteMetadataRequest {
            disk: self.root.to_string_lossy().to_string(),
            volume: volume.to_string(),
            path: path.to_string(),
            file_info,
        });

        let _response = client.write_metadata(request).await?.into_inner();

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
        let opts = serde_json::to_string(opts)?;
        let mut client = self.get_client();
        let request = Request::new(ReadVersionRequest {
            disk: self.root.to_string_lossy().to_string(),
            volume: volume.to_string(),
            path: path.to_string(),
            version_id: version_id.to_string(),
            opts,
        });

        let response = client.read_version(request).await?.into_inner();
        let file_info = serde_json::from_str::<FileInfo>(&response.file_info)?;

        Ok(file_info)
    }

    async fn read_xl(&self, volume: &str, path: &str, read_data: bool) -> Result<RawFileInfo> {
        let mut client = self.get_client();
        let request = Request::new(ReadXlRequest {
            disk: self.root.to_string_lossy().to_string(),
            volume: volume.to_string(),
            path: path.to_string(),
            read_data,
        });

        let response = client.read_xl(request).await?.into_inner();
        let raw_file_info = serde_json::from_str::<RawFileInfo>(&response.raw_file_info)?;

        Ok(raw_file_info)
    }

    async fn delete_versions(
        &self,
        _volume: &str,
        _versions: Vec<FileInfoVersions>,
        _opts: DeleteOptions,
    ) -> Result<Vec<Option<Error>>> {
        unimplemented!()
    }

    async fn read_multiple(&self, req: ReadMultipleReq) -> Result<Vec<ReadMultipleResp>> {
        let read_multiple_req = serde_json::to_string(&req)?;
        let mut client = self.get_client();
        let request = Request::new(ReadMultipleRequest {
            disk: self.root.to_string_lossy().to_string(),
            read_multiple_req,
        });

        let response = client.read_multiple(request).await?.into_inner();
        let read_multiple_resps = response
            .read_multiple_resps
            .into_iter()
            .filter_map(|json_str| serde_json::from_str::<ReadMultipleResp>(&json_str).ok())
            .collect();

        Ok(read_multiple_resps)
    }

    async fn delete_volume(&self, volume: &str) -> Result<()> {
        let mut client = self.get_client();
        let request = Request::new(DeleteVolumeRequest {
            disk: self.root.to_string_lossy().to_string(),
            volume: volume.to_string(),
        });

        let _response = client.delete_volume(request).await?.into_inner();

        Ok(())
    }
}
