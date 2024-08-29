use ecstore::{
    disk::{DeleteOptions, DiskStore, ReadMultipleReq, ReadOptions, WalkDirOptions},
    erasure::{ReadAt, Write},
    peer::{LocalPeerS3Client, PeerS3Client},
    store_api::{BucketOptions, FileInfo, MakeBucketOptions},
};
use tokio::fs;
use tonic::{Request, Response, Status};
use tracing::{debug, error, info};

use protos::{
    models::{PingBody, PingBodyBuilder},
    proto_gen::node_service::{
        node_service_server::{NodeService as Node, NodeServiceServer as NodeServer},
        DeleteBucketRequest, DeleteBucketResponse, DeleteRequest, DeleteResponse, DeleteVolumeRequest, DeleteVolumeResponse,
        GetBucketInfoRequest, GetBucketInfoResponse, ListBucketRequest, ListBucketResponse, ListDirRequest, ListDirResponse,
        ListVolumesRequest, ListVolumesResponse, MakeBucketRequest, MakeBucketResponse, MakeVolumeRequest, MakeVolumeResponse,
        MakeVolumesRequest, MakeVolumesResponse, PingRequest, PingResponse, ReadAllRequest, ReadAllResponse, ReadAtRequest,
        ReadAtResponse, ReadMultipleRequest, ReadMultipleResponse, ReadVersionRequest, ReadVersionResponse, ReadXlRequest,
        ReadXlResponse, RenameDataRequest, RenameDataResponse, RenameFileRequst, RenameFileResponse, StatVolumeRequest,
        StatVolumeResponse, WalkDirRequest, WalkDirResponse, WriteAllRequest, WriteAllResponse, WriteMetadataRequest,
        WriteMetadataResponse, WriteRequest, WriteResponse,
    },
};

#[derive(Debug)]
struct NodeService {
    pub local_peer: LocalPeerS3Client,
}

pub fn make_server(local_disks: Vec<DiskStore>) -> NodeServer<impl Node> {
    let local_peer = LocalPeerS3Client::new(local_disks, None, None);
    NodeServer::new(NodeService { local_peer })
}

impl NodeService {
    async fn find_disk(&self, disk_path: &String) -> Option<DiskStore> {
        let disk_path = match fs::canonicalize(disk_path).await {
            Ok(disk_path) => disk_path,
            Err(_) => return None,
        };
        self.local_peer.local_disks.iter().find(|&x| x.path() == disk_path).cloned()
    }

    fn all_disk(&self) -> Vec<String> {
        self.local_peer
            .local_disks
            .iter()
            .map(|disk| disk.path().to_string_lossy().to_string())
            .collect()
    }
}

#[tonic::async_trait]
impl Node for NodeService {
    async fn ping(&self, request: Request<PingRequest>) -> Result<Response<PingResponse>, Status> {
        debug!("PING");

        let ping_req = request.into_inner();
        let ping_body = flatbuffers::root::<PingBody>(&ping_req.body);
        if let Err(e) = ping_body {
            error!("{}", e);
        } else {
            info!("ping_req:body(flatbuffer): {:?}", ping_body);
        }

        let mut fbb = flatbuffers::FlatBufferBuilder::new();
        let payload = fbb.create_vector(b"hello, caller");

        let mut builder = PingBodyBuilder::new(&mut fbb);
        builder.add_payload(payload);
        let root = builder.finish();
        fbb.finish(root, None);

        let finished_data = fbb.finished_data();

        Ok(tonic::Response::new(PingResponse {
            version: 1,
            body: finished_data.to_vec(),
        }))
    }

    async fn list_bucket(&self, request: Request<ListBucketRequest>) -> Result<Response<ListBucketResponse>, Status> {
        debug!("list bucket");

        let request = request.into_inner();
        let options = match serde_json::from_str::<BucketOptions>(&request.options) {
            Ok(options) => options,
            Err(err) => {
                return Ok(tonic::Response::new(ListBucketResponse {
                    success: false,
                    bucket_infos: Vec::new(),
                    error_info: Some(format!("decode BucketOptions failed: {}", err.to_string())),
                }))
            }
        };
        match self.local_peer.list_bucket(&options).await {
            Ok(bucket_infos) => {
                let bucket_infos = bucket_infos
                    .into_iter()
                    .filter_map(|bucket_info| serde_json::to_string(&bucket_info).ok())
                    .collect();
                Ok(tonic::Response::new(ListBucketResponse {
                    success: true,
                    bucket_infos,
                    error_info: None,
                }))
            }

            Err(err) => Ok(tonic::Response::new(ListBucketResponse {
                success: false,
                bucket_infos: Vec::new(),
                error_info: Some(format!("make failed: {}", err.to_string())),
            })),
        }
    }

    async fn make_bucket(&self, request: Request<MakeBucketRequest>) -> Result<Response<MakeBucketResponse>, Status> {
        debug!("make bucket");

        let request = request.into_inner();
        let options = match serde_json::from_str::<MakeBucketOptions>(&request.options) {
            Ok(options) => options,
            Err(err) => {
                return Ok(tonic::Response::new(MakeBucketResponse {
                    success: false,
                    error_info: Some(format!("decode MakeBucketOptions failed: {}", err.to_string())),
                }))
            }
        };
        match self.local_peer.make_bucket(&request.name, &options).await {
            Ok(_) => Ok(tonic::Response::new(MakeBucketResponse {
                success: true,
                error_info: None,
            })),
            Err(err) => Ok(tonic::Response::new(MakeBucketResponse {
                success: false,
                error_info: Some(format!("make failed: {}", err.to_string())),
            })),
        }
    }

    async fn get_bucket_info(&self, request: Request<GetBucketInfoRequest>) -> Result<Response<GetBucketInfoResponse>, Status> {
        debug!("get bucket info");

        let request = request.into_inner();
        let options = match serde_json::from_str::<BucketOptions>(&request.options) {
            Ok(options) => options,
            Err(err) => {
                return Ok(tonic::Response::new(GetBucketInfoResponse {
                    success: false,
                    bucket_info: String::new(),
                    error_info: Some(format!("decode BucketOptions failed: {}", err.to_string())),
                }))
            }
        };
        match self.local_peer.get_bucket_info(&request.bucket, &options).await {
            Ok(bucket_info) => {
                let bucket_info = match serde_json::to_string(&bucket_info) {
                    Ok(bucket_info) => bucket_info,
                    Err(err) => {
                        return Ok(tonic::Response::new(GetBucketInfoResponse {
                            success: false,
                            bucket_info: String::new(),
                            error_info: Some(format!("encode BucketInfo failed: {}", err.to_string())),
                        }));
                    }
                };
                Ok(tonic::Response::new(GetBucketInfoResponse {
                    success: true,
                    bucket_info,
                    error_info: None,
                }))
            }

            Err(err) => Ok(tonic::Response::new(GetBucketInfoResponse {
                success: false,
                bucket_info: String::new(),
                error_info: Some(format!("make failed: {}", err.to_string())),
            })),
        }
    }

    async fn delete_bucket(&self, request: Request<DeleteBucketRequest>) -> Result<Response<DeleteBucketResponse>, Status> {
        debug!("make bucket");

        let request = request.into_inner();
        match self.local_peer.delete_bucket(&request.bucket).await {
            Ok(_) => Ok(tonic::Response::new(DeleteBucketResponse {
                success: true,
                error_info: None,
            })),
            Err(err) => Ok(tonic::Response::new(DeleteBucketResponse {
                success: false,
                error_info: Some(format!("make failed: {}", err.to_string())),
            })),
        }
    }

    async fn read_all(&self, request: Request<ReadAllRequest>) -> Result<Response<ReadAllResponse>, Status> {
        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
            match disk.read_all(&request.volume, &request.path).await {
                Ok(data) => Ok(tonic::Response::new(ReadAllResponse {
                    success: true,
                    data: data.to_vec(),
                    error_info: None,
                })),
                Err(err) => Ok(tonic::Response::new(ReadAllResponse {
                    success: false,
                    data: Vec::new(),
                    error_info: Some(err.to_string()),
                })),
            }
        } else {
            Ok(tonic::Response::new(ReadAllResponse {
                success: false,
                data: Vec::new(),
                error_info: Some("can not find disk".to_string()),
            }))
        }
    }

    async fn write_all(&self, request: Request<WriteAllRequest>) -> Result<Response<WriteAllResponse>, Status> {
        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
            match disk.write_all(&request.volume, &request.path, request.data).await {
                Ok(_) => Ok(tonic::Response::new(WriteAllResponse {
                    success: true,
                    error_info: None,
                })),
                Err(err) => Ok(tonic::Response::new(WriteAllResponse {
                    success: false,
                    error_info: Some(err.to_string()),
                })),
            }
        } else {
            Ok(tonic::Response::new(WriteAllResponse {
                success: false,
                error_info: Some("can not find disk".to_string()),
            }))
        }
    }

    async fn delete(&self, request: Request<DeleteRequest>) -> Result<Response<DeleteResponse>, Status> {
        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
            let options = match serde_json::from_str::<DeleteOptions>(&request.options) {
                Ok(options) => options,
                Err(_) => {
                    return Ok(tonic::Response::new(DeleteResponse {
                        success: false,
                        error_info: Some("can not decode DeleteOptions".to_string()),
                    }));
                }
            };
            match disk.delete(&request.volume, &request.path, options).await {
                Ok(_) => Ok(tonic::Response::new(DeleteResponse {
                    success: true,
                    error_info: None,
                })),
                Err(err) => Ok(tonic::Response::new(DeleteResponse {
                    success: false,
                    error_info: Some(err.to_string()),
                })),
            }
        } else {
            Ok(tonic::Response::new(DeleteResponse {
                success: false,
                error_info: Some("can not find disk".to_string()),
            }))
        }
    }

    async fn rename_file(&self, request: Request<RenameFileRequst>) -> Result<Response<RenameFileResponse>, Status> {
        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
            match disk
                .rename_file(&request.src_volume, &request.src_path, &request.dst_volume, &request.dst_path)
                .await
            {
                Ok(_) => Ok(tonic::Response::new(RenameFileResponse {
                    success: true,
                    error_info: None,
                })),
                Err(err) => Ok(tonic::Response::new(RenameFileResponse {
                    success: false,
                    error_info: Some(err.to_string()),
                })),
            }
        } else {
            Ok(tonic::Response::new(RenameFileResponse {
                success: false,
                error_info: Some("can not find disk".to_string()),
            }))
        }
    }

    async fn write(&self, request: Request<WriteRequest>) -> Result<Response<WriteResponse>, Status> {
        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
            let file_writer = if request.is_append {
                disk.append_file(&request.volume, &request.path).await
            } else {
                disk.create_file("", &request.volume, &request.path, 0).await
            };

            match file_writer {
                Ok(mut file_writer) => match file_writer.write(&request.data).await {
                    Ok(_) => Ok(tonic::Response::new(WriteResponse {
                        success: true,
                        error_info: None,
                    })),
                    Err(err) => Ok(tonic::Response::new(WriteResponse {
                        success: false,
                        error_info: Some(err.to_string()),
                    })),
                },
                Err(err) => Ok(tonic::Response::new(WriteResponse {
                    success: false,
                    error_info: Some(err.to_string()),
                })),
            }
        } else {
            Ok(tonic::Response::new(WriteResponse {
                success: false,
                error_info: Some("can not find disk".to_string()),
            }))
        }
    }

    async fn read_at(&self, request: Request<ReadAtRequest>) -> Result<Response<ReadAtResponse>, Status> {
        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
            match disk.read_file(&request.volume, &request.path).await {
                Ok(mut file_reader) => {
                    match file_reader
                        .read_at(request.offset.try_into().unwrap(), request.length.try_into().unwrap())
                        .await
                    {
                        Ok((data, read_size)) => Ok(tonic::Response::new(ReadAtResponse {
                            success: true,
                            data,
                            read_size: read_size.try_into().unwrap(),
                            error_info: None,
                        })),
                        Err(err) => Ok(tonic::Response::new(ReadAtResponse {
                            success: false,
                            data: Vec::new(),
                            read_size: -1,
                            error_info: Some(err.to_string()),
                        })),
                    }
                }
                Err(err) => Ok(tonic::Response::new(ReadAtResponse {
                    success: false,
                    data: Vec::new(),
                    read_size: -1,
                    error_info: Some(err.to_string()),
                })),
            }
        } else {
            Ok(tonic::Response::new(ReadAtResponse {
                success: false,
                data: Vec::new(),
                read_size: -1,
                error_info: Some("can not find disk".to_string()),
            }))
        }
    }

    async fn list_dir(&self, request: Request<ListDirRequest>) -> Result<Response<ListDirResponse>, Status> {
        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
            match disk.list_dir("", &request.volume, "", 0).await {
                Ok(volumes) => Ok(tonic::Response::new(ListDirResponse {
                    success: true,
                    volumes,
                    error_info: None,
                })),
                Err(err) => Ok(tonic::Response::new(ListDirResponse {
                    success: false,
                    volumes: Vec::new(),
                    error_info: Some(err.to_string()),
                })),
            }
        } else {
            Ok(tonic::Response::new(ListDirResponse {
                success: false,
                volumes: Vec::new(),
                error_info: Some("can not find disk".to_string()),
            }))
        }
    }

    async fn walk_dir(&self, request: Request<WalkDirRequest>) -> Result<Response<WalkDirResponse>, Status> {
        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
            let opts = match serde_json::from_str::<WalkDirOptions>(&request.walk_dir_options) {
                Ok(options) => options,
                Err(_) => {
                    return Ok(tonic::Response::new(WalkDirResponse {
                        success: false,
                        meta_cache_entry: Vec::new(),
                        error_info: Some("can not decode DeleteOptions".to_string()),
                    }));
                }
            };
            match disk.walk_dir(opts).await {
                Ok(entries) => {
                    let entries = entries
                        .into_iter()
                        .filter_map(|entry| serde_json::to_string(&entry).ok())
                        .collect();
                    Ok(tonic::Response::new(WalkDirResponse {
                        success: true,
                        meta_cache_entry: entries,
                        error_info: None,
                    }))
                }
                Err(err) => Ok(tonic::Response::new(WalkDirResponse {
                    success: false,
                    meta_cache_entry: Vec::new(),
                    error_info: Some(err.to_string()),
                })),
            }
        } else {
            Ok(tonic::Response::new(WalkDirResponse {
                success: false,
                meta_cache_entry: Vec::new(),
                error_info: Some("can not find disk".to_string()),
            }))
        }
    }

    async fn rename_data(&self, request: Request<RenameDataRequest>) -> Result<Response<RenameDataResponse>, Status> {
        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
            let file_info = match serde_json::from_str::<FileInfo>(&request.file_info) {
                Ok(file_info) => file_info,
                Err(_) => {
                    return Ok(tonic::Response::new(RenameDataResponse {
                        success: false,
                        rename_data_resp: String::new(),
                        error_info: Some("can not decode DeleteOptions".to_string()),
                    }));
                }
            };
            match disk
                .rename_data(&request.src_volume, &request.src_path, file_info, &request.dst_volume, &request.dst_path)
                .await
            {
                Ok(rename_data_resp) => {
                    let rename_data_resp = match serde_json::to_string(&rename_data_resp) {
                        Ok(file_info) => file_info,
                        Err(_) => {
                            return Ok(tonic::Response::new(RenameDataResponse {
                                success: false,
                                rename_data_resp: String::new(),
                                error_info: Some("can not encode RenameDataResp".to_string()),
                            }));
                        }
                    };
                    Ok(tonic::Response::new(RenameDataResponse {
                        success: true,
                        rename_data_resp,
                        error_info: None,
                    }))
                }
                Err(err) => Ok(tonic::Response::new(RenameDataResponse {
                    success: false,
                    rename_data_resp: String::new(),
                    error_info: Some(err.to_string()),
                })),
            }
        } else {
            Ok(tonic::Response::new(RenameDataResponse {
                success: false,
                rename_data_resp: String::new(),
                error_info: Some("can not find disk".to_string()),
            }))
        }
    }

    async fn make_volumes(&self, request: Request<MakeVolumesRequest>) -> Result<Response<MakeVolumesResponse>, Status> {
        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
            match disk.make_volumes(request.volumes.iter().map(|s| &**s).collect()).await {
                Ok(_) => Ok(tonic::Response::new(MakeVolumesResponse {
                    success: true,
                    error_info: None,
                })),
                Err(err) => Ok(tonic::Response::new(MakeVolumesResponse {
                    success: false,
                    error_info: Some(err.to_string()),
                })),
            }
        } else {
            Ok(tonic::Response::new(MakeVolumesResponse {
                success: false,
                error_info: Some(format!("can not find disk, all disks: {:?}", self.all_disk())),
            }))
        }
    }

    async fn make_volume(&self, request: Request<MakeVolumeRequest>) -> Result<Response<MakeVolumeResponse>, Status> {
        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
            match disk.make_volume(&request.volume).await {
                Ok(_) => Ok(tonic::Response::new(MakeVolumeResponse {
                    success: true,
                    error_info: None,
                })),
                Err(err) => Ok(tonic::Response::new(MakeVolumeResponse {
                    success: false,
                    error_info: Some(err.to_string()),
                })),
            }
        } else {
            Ok(tonic::Response::new(MakeVolumeResponse {
                success: false,
                error_info: Some(format!("can not find disk, all disks: {:?}", self.all_disk())),
            }))
        }
    }

    async fn list_volumes(&self, request: Request<ListVolumesRequest>) -> Result<Response<ListVolumesResponse>, Status> {
        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
            match disk.list_volumes().await {
                Ok(volume_infos) => {
                    let volume_infos = volume_infos
                        .into_iter()
                        .filter_map(|volume_info| serde_json::to_string(&volume_info).ok())
                        .collect();
                    Ok(tonic::Response::new(ListVolumesResponse {
                        success: true,
                        volume_infos,
                        error_info: None,
                    }))
                }
                Err(err) => Ok(tonic::Response::new(ListVolumesResponse {
                    success: false,
                    volume_infos: Vec::new(),
                    error_info: Some(err.to_string()),
                })),
            }
        } else {
            Ok(tonic::Response::new(ListVolumesResponse {
                success: false,
                volume_infos: Vec::new(),
                error_info: Some("can not find disk".to_string()),
            }))
        }
    }

    async fn stat_volume(&self, request: Request<StatVolumeRequest>) -> Result<Response<StatVolumeResponse>, Status> {
        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
            match disk.stat_volume(&request.volume).await {
                Ok(volume_info) => match serde_json::to_string(&volume_info) {
                    Ok(volume_info) => Ok(tonic::Response::new(StatVolumeResponse {
                        success: true,
                        volume_info,
                        error_info: None,
                    })),
                    Err(err) => Ok(tonic::Response::new(StatVolumeResponse {
                        success: false,
                        volume_info: String::new(),
                        error_info: Some(format!("encode VolumeInfo failed, {}", err.to_string())),
                    })),
                },
                Err(err) => Ok(tonic::Response::new(StatVolumeResponse {
                    success: false,
                    volume_info: String::new(),
                    error_info: Some(err.to_string()),
                })),
            }
        } else {
            Ok(tonic::Response::new(StatVolumeResponse {
                success: false,
                volume_info: String::new(),
                error_info: Some("can not find disk".to_string()),
            }))
        }
    }

    async fn write_metadata(&self, request: Request<WriteMetadataRequest>) -> Result<Response<WriteMetadataResponse>, Status> {
        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
            let file_info = match serde_json::from_str::<FileInfo>(&request.file_info) {
                Ok(file_info) => file_info,
                Err(err) => {
                    return Ok(tonic::Response::new(WriteMetadataResponse {
                        success: false,
                        error_info: Some(format!("decode FileInfo failed, {}", err.to_string())),
                    }));
                }
            };
            match disk.write_metadata("", &request.volume, &request.path, file_info).await {
                Ok(_) => Ok(tonic::Response::new(WriteMetadataResponse {
                    success: true,
                    error_info: None,
                })),
                Err(err) => Ok(tonic::Response::new(WriteMetadataResponse {
                    success: false,
                    error_info: Some(err.to_string()),
                })),
            }
        } else {
            Ok(tonic::Response::new(WriteMetadataResponse {
                success: false,
                error_info: Some("can not find disk".to_string()),
            }))
        }
    }

    async fn read_version(&self, request: Request<ReadVersionRequest>) -> Result<Response<ReadVersionResponse>, Status> {
        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
            let opts = match serde_json::from_str::<ReadOptions>(&request.opts) {
                Ok(options) => options,
                Err(_) => {
                    return Ok(tonic::Response::new(ReadVersionResponse {
                        success: false,
                        file_info: String::new(),
                        error_info: Some("can not decode DeleteOptions".to_string()),
                    }));
                }
            };
            match disk
                .read_version("", &request.volume, &request.path, &request.version_id, &opts)
                .await
            {
                Ok(file_info) => match serde_json::to_string(&file_info) {
                    Ok(file_info) => Ok(tonic::Response::new(ReadVersionResponse {
                        success: true,
                        file_info,
                        error_info: None,
                    })),
                    Err(err) => Ok(tonic::Response::new(ReadVersionResponse {
                        success: false,
                        file_info: String::new(),
                        error_info: Some(format!("encode VolumeInfo failed, {}", err.to_string())),
                    })),
                },
                Err(err) => Ok(tonic::Response::new(ReadVersionResponse {
                    success: false,
                    file_info: String::new(),
                    error_info: Some(err.to_string()),
                })),
            }
        } else {
            Ok(tonic::Response::new(ReadVersionResponse {
                success: false,
                file_info: String::new(),
                error_info: Some("can not find disk".to_string()),
            }))
        }
    }

    async fn read_xl(&self, request: Request<ReadXlRequest>) -> Result<Response<ReadXlResponse>, Status> {
        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
            match disk.read_xl(&request.volume, &request.path, request.read_data).await {
                Ok(raw_file_info) => match serde_json::to_string(&raw_file_info) {
                    Ok(raw_file_info) => Ok(tonic::Response::new(ReadXlResponse {
                        success: true,
                        raw_file_info,
                        error_info: None,
                    })),
                    Err(err) => Ok(tonic::Response::new(ReadXlResponse {
                        success: false,
                        raw_file_info: String::new(),
                        error_info: Some(format!("encode RawFileInfo failed, {}", err.to_string())),
                    })),
                },
                Err(err) => Ok(tonic::Response::new(ReadXlResponse {
                    success: false,
                    raw_file_info: String::new(),
                    error_info: Some(err.to_string()),
                })),
            }
        } else {
            Ok(tonic::Response::new(ReadXlResponse {
                success: false,
                raw_file_info: String::new(),
                error_info: Some("can not find disk".to_string()),
            }))
        }
    }

    async fn read_multiple(&self, request: Request<ReadMultipleRequest>) -> Result<Response<ReadMultipleResponse>, Status> {
        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
            let read_multiple_req = match serde_json::from_str::<ReadMultipleReq>(&request.read_multiple_req) {
                Ok(read_multiple_req) => read_multiple_req,
                Err(_) => {
                    return Ok(tonic::Response::new(ReadMultipleResponse {
                        success: false,
                        read_multiple_resps: Vec::new(),
                        error_info: Some("can not decode ReadMultipleReq".to_string()),
                    }));
                }
            };
            match disk.read_multiple(read_multiple_req).await {
                Ok(read_multiple_resps) => {
                    let read_multiple_resps = read_multiple_resps
                        .into_iter()
                        .filter_map(|read_multiple_resp| serde_json::to_string(&read_multiple_resp).ok())
                        .collect();

                    Ok(tonic::Response::new(ReadMultipleResponse {
                        success: true,
                        read_multiple_resps,
                        error_info: None,
                    }))
                }
                Err(err) => Ok(tonic::Response::new(ReadMultipleResponse {
                    success: false,
                    read_multiple_resps: Vec::new(),
                    error_info: Some(err.to_string()),
                })),
            }
        } else {
            Ok(tonic::Response::new(ReadMultipleResponse {
                success: false,
                read_multiple_resps: Vec::new(),
                error_info: Some("can not find disk".to_string()),
            }))
        }
    }

    async fn delete_volume(&self, request: Request<DeleteVolumeRequest>) -> Result<Response<DeleteVolumeResponse>, Status> {
        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
            match disk.delete_volume(&request.volume).await {
                Ok(_) => Ok(tonic::Response::new(DeleteVolumeResponse {
                    success: true,
                    error_info: None,
                })),
                Err(err) => Ok(tonic::Response::new(DeleteVolumeResponse {
                    success: false,
                    error_info: Some(err.to_string()),
                })),
            }
        } else {
            Ok(tonic::Response::new(DeleteVolumeResponse {
                success: false,
                error_info: Some("can not find disk".to_string()),
            }))
        }
    }
}
