use std::{error::Error, io::ErrorKind, pin::Pin};

use ecstore::{
    disk::{
        DeleteOptions, DiskAPI, DiskInfoOptions, DiskStore, FileInfoVersions, ReadMultipleReq, ReadOptions, Reader,
        UpdateMetadataOpts, WalkDirOptions,
    },
    erasure::{ReadAt, Writer},
    peer::{LocalPeerS3Client, PeerS3Client},
    store::{all_local_disk_path, find_local_disk},
    store_api::{BucketOptions, DeleteBucketOptions, FileInfo, MakeBucketOptions},
};
use futures::{Stream, StreamExt};
use lock::{lock_args::LockArgs, Locker, GLOBAL_LOCAL_SERVER};

use protos::{
    models::{PingBody, PingBodyBuilder},
    proto_gen::node_service::{
        node_service_server::NodeService as Node, CheckPartsRequest, CheckPartsResponse, DeleteBucketRequest,
        DeleteBucketResponse, DeletePathsRequest, DeletePathsResponse, DeleteRequest, DeleteResponse, DeleteVersionRequest,
        DeleteVersionResponse, DeleteVersionsRequest, DeleteVersionsResponse, DeleteVolumeRequest, DeleteVolumeResponse,
        DiskInfoRequest, DiskInfoResponse, GenerallyLockRequest, GenerallyLockResponse, GetBucketInfoRequest,
        GetBucketInfoResponse, ListBucketRequest, ListBucketResponse, ListDirRequest, ListDirResponse, ListVolumesRequest,
        ListVolumesResponse, MakeBucketRequest, MakeBucketResponse, MakeVolumeRequest, MakeVolumeResponse, MakeVolumesRequest,
        MakeVolumesResponse, PingRequest, PingResponse, ReadAllRequest, ReadAllResponse, ReadAtRequest, ReadAtResponse,
        ReadMultipleRequest, ReadMultipleResponse, ReadVersionRequest, ReadVersionResponse, ReadXlRequest, ReadXlResponse,
        RenameDataRequest, RenameDataResponse, RenameFileRequst, RenameFileResponse, RenamePartRequst, RenamePartResponse,
        StatVolumeRequest, StatVolumeResponse, UpdateMetadataRequest, UpdateMetadataResponse, VerifyFileRequest,
        VerifyFileResponse, WalkDirRequest, WalkDirResponse, WriteAllRequest, WriteAllResponse, WriteMetadataRequest,
        WriteMetadataResponse, WriteRequest, WriteResponse,
    },
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, error, info};

type ResponseStream<T> = Pin<Box<dyn Stream<Item = Result<T, tonic::Status>> + Send>>;

fn match_for_io_error(err_status: &Status) -> Option<&std::io::Error> {
    let mut err: &(dyn Error + 'static) = err_status;

    loop {
        if let Some(io_err) = err.downcast_ref::<std::io::Error>() {
            return Some(io_err);
        }

        // h2::Error do not expose std::io::Error with `source()`
        // https://github.com/hyperium/h2/pull/462
        if let Some(h2_err) = err.downcast_ref::<h2::Error>() {
            if let Some(io_err) = h2_err.get_io() {
                return Some(io_err);
            }
        }

        err = match err.source() {
            Some(err) => err,
            None => return None,
        };
    }
}

#[derive(Debug)]
pub struct NodeService {
    local_peer: LocalPeerS3Client,
}

pub fn make_server() -> NodeService {
    let local_peer = LocalPeerS3Client::new(None, None);
    NodeService { local_peer }
}

impl NodeService {
    async fn find_disk(&self, disk_path: &String) -> Option<DiskStore> {
        find_local_disk(disk_path).await
    }

    async fn all_disk(&self) -> Vec<String> {
        all_local_disk_path().await
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
                    error_info: Some(format!("decode BucketOptions failed: {}", err)),
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
                error_info: Some(format!("make failed: {}", err)),
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
                    error_info: Some(format!("decode MakeBucketOptions failed: {}", err)),
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
                error_info: Some(format!("make failed: {}", err)),
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
                    error_info: Some(format!("decode BucketOptions failed: {}", err)),
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
                            error_info: Some(format!("encode BucketInfo failed: {}", err)),
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
                error_info: Some(format!("make failed: {}", err)),
            })),
        }
    }

    async fn delete_bucket(&self, request: Request<DeleteBucketRequest>) -> Result<Response<DeleteBucketResponse>, Status> {
        debug!("make bucket");

        let request = request.into_inner();
        match self
            .local_peer
            .delete_bucket(
                &request.bucket,
                &DeleteBucketOptions {
                    force: false,
                    ..Default::default()
                },
            )
            .await
        {
            Ok(_) => Ok(tonic::Response::new(DeleteBucketResponse {
                success: true,
                error_info: None,
            })),
            Err(err) => Ok(tonic::Response::new(DeleteBucketResponse {
                success: false,
                error_info: Some(format!("make failed: {}", err)),
            })),
        }
    }

    async fn read_all(&self, request: Request<ReadAllRequest>) -> Result<Response<ReadAllResponse>, Status> {
        debug!("read all");

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

    async fn verify_file(&self, request: Request<VerifyFileRequest>) -> Result<Response<VerifyFileResponse>, Status> {
        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
            let file_info = match serde_json::from_str::<FileInfo>(&request.file_info) {
                Ok(file_info) => file_info,
                Err(_) => {
                    return Ok(tonic::Response::new(VerifyFileResponse {
                        success: false,
                        check_parts_resp: "".to_string(),
                        error_info: Some("can not decode FileInfo".to_string()),
                    }));
                }
            };
            match disk.verify_file(&request.volume, &request.path, &file_info).await {
                Ok(check_parts_resp) => {
                    let check_parts_resp = match serde_json::to_string(&check_parts_resp) {
                        Ok(check_parts_resp) => check_parts_resp,
                        Err(_) => {
                            return Ok(tonic::Response::new(VerifyFileResponse {
                                success: false,
                                check_parts_resp: String::new(),
                                error_info: Some("can not encode RenameDataResp".to_string()),
                            }));
                        }
                    };
                    Ok(tonic::Response::new(VerifyFileResponse {
                        success: true,
                        check_parts_resp,
                        error_info: None,
                    }))
                }
                Err(err) => Ok(tonic::Response::new(VerifyFileResponse {
                    success: false,
                    check_parts_resp: "".to_string(),
                    error_info: Some(err.to_string()),
                })),
            }
        } else {
            Ok(tonic::Response::new(VerifyFileResponse {
                success: false,
                check_parts_resp: "".to_string(),
                error_info: Some("can not find disk".to_string()),
            }))
        }
    }

    async fn check_parts(&self, request: Request<CheckPartsRequest>) -> Result<Response<CheckPartsResponse>, Status> {
        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
            let file_info = match serde_json::from_str::<FileInfo>(&request.file_info) {
                Ok(file_info) => file_info,
                Err(_) => {
                    return Ok(tonic::Response::new(CheckPartsResponse {
                        success: false,
                        check_parts_resp: "".to_string(),
                        error_info: Some("can not decode FileInfo".to_string()),
                    }));
                }
            };
            match disk.verify_file(&request.volume, &request.path, &file_info).await {
                Ok(check_parts_resp) => {
                    let check_parts_resp = match serde_json::to_string(&check_parts_resp) {
                        Ok(check_parts_resp) => check_parts_resp,
                        Err(_) => {
                            return Ok(tonic::Response::new(CheckPartsResponse {
                                success: false,
                                check_parts_resp: String::new(),
                                error_info: Some("can not encode RenameDataResp".to_string()),
                            }));
                        }
                    };
                    Ok(tonic::Response::new(CheckPartsResponse {
                        success: true,
                        check_parts_resp,
                        error_info: None,
                    }))
                }
                Err(err) => Ok(tonic::Response::new(CheckPartsResponse {
                    success: false,
                    check_parts_resp: "".to_string(),
                    error_info: Some(err.to_string()),
                })),
            }
        } else {
            Ok(tonic::Response::new(CheckPartsResponse {
                success: false,
                check_parts_resp: "".to_string(),
                error_info: Some("can not find disk".to_string()),
            }))
        }
    }

    async fn rename_part(&self, request: Request<RenamePartRequst>) -> Result<Response<RenamePartResponse>, Status> {
        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
            match disk
                .rename_part(
                    &request.src_volume,
                    &request.src_path,
                    &request.dst_volume,
                    &request.dst_path,
                    request.meta,
                )
                .await
            {
                Ok(_) => Ok(tonic::Response::new(RenamePartResponse {
                    success: true,
                    error_info: None,
                })),
                Err(err) => Ok(tonic::Response::new(RenamePartResponse {
                    success: false,
                    error_info: Some(err.to_string()),
                })),
            }
        } else {
            Ok(tonic::Response::new(RenamePartResponse {
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

    type WriteStreamStream = ResponseStream<WriteResponse>;
    async fn write_stream(&self, request: Request<Streaming<WriteRequest>>) -> Result<Response<Self::WriteStreamStream>, Status> {
        info!("write_stream");

        let mut in_stream = request.into_inner();
        let (tx, rx) = mpsc::channel(128);

        tokio::spawn(async move {
            let mut file_ref = None;
            while let Some(result) = in_stream.next().await {
                match result {
                    // Ok(v) => tx
                    //     .send(Ok(EchoResponse { message: v.message }))
                    //     .await
                    //     .expect("working rx"),
                    Ok(v) => {
                        match file_ref.as_ref() {
                            Some(_) => (),
                            None => {
                                if let Some(disk) = find_local_disk(&v.disk).await {
                                    let file_writer = if v.is_append {
                                        disk.append_file(&v.volume, &v.path).await
                                    } else {
                                        disk.create_file("", &v.volume, &v.path, 0).await
                                    };

                                    match file_writer {
                                        Ok(file_writer) => file_ref = Some(file_writer),
                                        Err(err) => {
                                            tx.send(Ok(WriteResponse {
                                                success: false,
                                                error_info: Some(err.to_string()),
                                            }))
                                            .await
                                            .expect("working rx");
                                            break;
                                        }
                                    }
                                } else {
                                    tx.send(Ok(WriteResponse {
                                        success: false,
                                        error_info: Some("can not find disk".to_string()),
                                    }))
                                    .await
                                    .expect("working rx");
                                    break;
                                }
                            }
                        };

                        match file_ref.as_mut().unwrap().write(&v.data).await {
                            Ok(_) => tx.send(Ok(WriteResponse {
                                success: true,
                                error_info: None,
                            })),
                            Err(err) => tx.send(Ok(WriteResponse {
                                success: false,
                                error_info: Some(err.to_string()),
                            })),
                        }
                        .await
                        .unwrap();
                    }
                    Err(err) => {
                        if let Some(io_err) = match_for_io_error(&err) {
                            if io_err.kind() == ErrorKind::BrokenPipe {
                                // here you can handle special case when client
                                // disconnected in unexpected way
                                eprintln!("\tclient disconnected: broken pipe");
                                break;
                            }
                        }

                        match tx.send(Err(err)).await {
                            Ok(_) => (),
                            Err(_err) => break, // response was dropped
                        }
                    }
                }
            }
            println!("\tstream ended");
        });

        let out_stream = ReceiverStream::new(rx);

        Ok(tonic::Response::new(Box::pin(out_stream)))
    }

    type ReadAtStream = ResponseStream<ReadAtResponse>;
    async fn read_at(&self, request: Request<Streaming<ReadAtRequest>>) -> Result<Response<Self::ReadAtStream>, Status> {
        info!("read_at");

        let mut in_stream = request.into_inner();
        let (tx, rx) = mpsc::channel(128);

        tokio::spawn(async move {
            let mut file_ref = None;
            while let Some(result) = in_stream.next().await {
                match result {
                    Ok(v) => {
                        match file_ref.as_ref() {
                            Some(_) => (),
                            None => {
                                if let Some(disk) = find_local_disk(&v.disk).await {
                                    match disk.read_file(&v.volume, &v.path).await {
                                        Ok(file_reader) => file_ref = Some(file_reader),
                                        Err(err) => {
                                            tx.send(Ok(ReadAtResponse {
                                                success: false,
                                                data: Vec::new(),
                                                error_info: Some(err.to_string()),
                                                read_size: -1,
                                            }))
                                            .await
                                            .expect("working rx");
                                            break;
                                        }
                                    }
                                } else {
                                    tx.send(Ok(ReadAtResponse {
                                        success: false,
                                        data: Vec::new(),
                                        error_info: Some("can not find disk".to_string()),
                                        read_size: -1,
                                    }))
                                    .await
                                    .expect("working rx");
                                    break;
                                }
                            }
                        };

                        let mut data = vec![0u8; v.length.try_into().unwrap()];

                        match file_ref
                            .as_mut()
                            .unwrap()
                            .read_at(v.offset.try_into().unwrap(), &mut data)
                            .await
                        {
                            Ok(read_size) => tx.send(Ok(ReadAtResponse {
                                success: true,
                                data,
                                read_size: read_size.try_into().unwrap(),
                                error_info: None,
                            })),
                            Err(err) => tx.send(Ok(ReadAtResponse {
                                success: false,
                                data: Vec::new(),
                                error_info: Some(err.to_string()),
                                read_size: -1,
                            })),
                        }
                        .await
                        .unwrap();
                    }
                    Err(err) => {
                        if let Some(io_err) = match_for_io_error(&err) {
                            if io_err.kind() == ErrorKind::BrokenPipe {
                                // here you can handle special case when client
                                // disconnected in unexpected way
                                eprintln!("\tclient disconnected: broken pipe");
                                break;
                            }
                        }

                        match tx.send(Err(err)).await {
                            Ok(_) => (),
                            Err(_err) => break, // response was dropped
                        }
                    }
                }
            }
            println!("\tstream ended");
        });

        let out_stream = ReceiverStream::new(rx);

        Ok(tonic::Response::new(Box::pin(out_stream)))
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
                error_info: Some(format!("can not find disk, all disks: {:?}", self.all_disk().await)),
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
                error_info: Some(format!("can not find disk, all disks: {:?}", self.all_disk().await)),
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
                        error_info: Some(format!("encode VolumeInfo failed, {}", err)),
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

    async fn delete_paths(&self, request: Request<DeletePathsRequest>) -> Result<Response<DeletePathsResponse>, Status> {
        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
            let paths = request.paths.iter().map(|s| s.as_str()).collect::<Vec<&str>>();
            match disk.delete_paths(&request.volume, &paths).await {
                Ok(_) => Ok(tonic::Response::new(DeletePathsResponse {
                    success: true,
                    error_info: None,
                })),
                Err(err) => Ok(tonic::Response::new(DeletePathsResponse {
                    success: false,
                    error_info: Some(err.to_string()),
                })),
            }
        } else {
            Ok(tonic::Response::new(DeletePathsResponse {
                success: false,
                error_info: Some("can not find disk".to_string()),
            }))
        }
    }

    async fn update_metadata(&self, request: Request<UpdateMetadataRequest>) -> Result<Response<UpdateMetadataResponse>, Status> {
        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
            let file_info = match serde_json::from_str::<FileInfo>(&request.file_info) {
                Ok(file_info) => file_info,
                Err(_) => {
                    return Ok(tonic::Response::new(UpdateMetadataResponse {
                        success: false,
                        error_info: Some("can not decode FileInfoVersions".to_string()),
                    }));
                }
            };
            let opts = match serde_json::from_str::<UpdateMetadataOpts>(&request.opts) {
                Ok(opts) => opts,
                Err(_) => {
                    return Ok(tonic::Response::new(UpdateMetadataResponse {
                        success: false,
                        error_info: Some("can not decode UpdateMetadataOpts".to_string()),
                    }));
                }
            };

            match disk.update_metadata(&request.volume, &request.path, file_info, &opts).await {
                Ok(_) => Ok(tonic::Response::new(UpdateMetadataResponse {
                    success: true,
                    error_info: None,
                })),
                Err(err) => Ok(tonic::Response::new(UpdateMetadataResponse {
                    success: false,
                    error_info: Some(err.to_string()),
                })),
            }
        } else {
            Ok(tonic::Response::new(UpdateMetadataResponse {
                success: false,
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
                        error_info: Some(format!("decode FileInfo failed, {}", err)),
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
                        error_info: Some(format!("encode VolumeInfo failed, {}", err)),
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
                        error_info: Some(format!("encode RawFileInfo failed, {}", err)),
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

    async fn delete_version(&self, request: Request<DeleteVersionRequest>) -> Result<Response<DeleteVersionResponse>, Status> {
        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
            let file_info = match serde_json::from_str::<FileInfo>(&request.file_info) {
                Ok(file_info) => file_info,
                Err(_) => {
                    return Ok(tonic::Response::new(DeleteVersionResponse {
                        success: false,
                        raw_file_info: "".to_string(),
                        error_info: Some("can not decode FileInfoVersions".to_string()),
                    }));
                }
            };
            let opts = match serde_json::from_str::<DeleteOptions>(&request.opts) {
                Ok(opts) => opts,
                Err(_) => {
                    return Ok(tonic::Response::new(DeleteVersionResponse {
                        success: false,
                        raw_file_info: "".to_string(),
                        error_info: Some("can not decode DeleteOptions".to_string()),
                    }));
                }
            };
            match disk
                .delete_version(&request.volume, &request.path, file_info, request.force_del_marker, opts)
                .await
            {
                Ok(raw_file_info) => match serde_json::to_string(&raw_file_info) {
                    Ok(raw_file_info) => Ok(tonic::Response::new(DeleteVersionResponse {
                        success: true,
                        raw_file_info,
                        error_info: None,
                    })),
                    Err(err) => Ok(tonic::Response::new(DeleteVersionResponse {
                        success: false,
                        raw_file_info: "".to_string(),
                        error_info: Some(err.to_string()),
                    })),
                },
                Err(err) => Ok(tonic::Response::new(DeleteVersionResponse {
                    success: false,
                    raw_file_info: "".to_string(),
                    error_info: Some(err.to_string()),
                })),
            }
        } else {
            Ok(tonic::Response::new(DeleteVersionResponse {
                success: false,
                raw_file_info: "".to_string(),
                error_info: Some("can not find disk".to_string()),
            }))
        }
    }

    async fn delete_versions(&self, request: Request<DeleteVersionsRequest>) -> Result<Response<DeleteVersionsResponse>, Status> {
        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
            let mut versions = Vec::with_capacity(request.versions.len());
            for version in request.versions.iter() {
                match serde_json::from_str::<FileInfoVersions>(version) {
                    Ok(version) => versions.push(version),
                    Err(_) => {
                        return Ok(tonic::Response::new(DeleteVersionsResponse {
                            success: false,
                            errors: Vec::new(),
                            error_info: Some("can not decode FileInfoVersions".to_string()),
                        }));
                    }
                };
            }
            let opts = match serde_json::from_str::<DeleteOptions>(&request.opts) {
                Ok(opts) => opts,
                Err(_) => {
                    return Ok(tonic::Response::new(DeleteVersionsResponse {
                        success: false,
                        errors: Vec::new(),
                        error_info: Some("can not decode DeleteOptions".to_string()),
                    }));
                }
            };
            match disk.delete_versions(&request.volume, versions, opts).await {
                Ok(errors) => {
                    let errors = errors
                        .into_iter()
                        .map(|error| match error {
                            Some(e) => e.to_string(),
                            None => "".to_string(),
                        })
                        .collect();

                    Ok(tonic::Response::new(DeleteVersionsResponse {
                        success: true,
                        errors,
                        error_info: None,
                    }))
                }
                Err(err) => Ok(tonic::Response::new(DeleteVersionsResponse {
                    success: false,
                    errors: Vec::new(),
                    error_info: Some(err.to_string()),
                })),
            }
        } else {
            Ok(tonic::Response::new(DeleteVersionsResponse {
                success: false,
                errors: Vec::new(),
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

    async fn disk_info(&self, request: Request<DiskInfoRequest>) -> Result<Response<DiskInfoResponse>, Status> {
        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
            let opts = match serde_json::from_str::<DiskInfoOptions>(&request.opts) {
                Ok(opts) => opts,
                Err(_) => {
                    return Ok(tonic::Response::new(DiskInfoResponse {
                        success: false,
                        disk_info: "".to_string(),
                        error_info: Some("can not decode DiskInfoOptions".to_string()),
                    }));
                }
            };
            match disk.disk_info(&opts).await {
                Ok(disk_info) => match serde_json::to_string(&disk_info) {
                    Ok(disk_info) => Ok(tonic::Response::new(DiskInfoResponse {
                        success: true,
                        disk_info,
                        error_info: None,
                    })),
                    Err(err) => Ok(tonic::Response::new(DiskInfoResponse {
                        success: false,
                        disk_info: "".to_string(),
                        error_info: Some(err.to_string()),
                    })),
                },
                Err(err) => Ok(tonic::Response::new(DiskInfoResponse {
                    success: false,
                    disk_info: "".to_string(),
                    error_info: Some(err.to_string()),
                })),
            }
        } else {
            Ok(tonic::Response::new(DiskInfoResponse {
                success: false,
                disk_info: "".to_string(),
                error_info: Some("can not find disk".to_string()),
            }))
        }
    }

    async fn lock(&self, request: Request<GenerallyLockRequest>) -> Result<Response<GenerallyLockResponse>, Status> {
        let request = request.into_inner();
        match &serde_json::from_str::<LockArgs>(&request.args) {
            Ok(args) => match GLOBAL_LOCAL_SERVER.write().await.lock(args).await {
                Ok(result) => Ok(tonic::Response::new(GenerallyLockResponse {
                    success: result,
                    error_info: None,
                })),
                Err(err) => Ok(tonic::Response::new(GenerallyLockResponse {
                    success: false,
                    error_info: Some(format!("can not lock, args: {}, err: {}", args, err)),
                })),
            },
            Err(err) => Ok(tonic::Response::new(GenerallyLockResponse {
                success: false,
                error_info: Some(format!("can not decode args, err: {}", err)),
            })),
        }
    }

    async fn un_lock(&self, request: Request<GenerallyLockRequest>) -> Result<Response<GenerallyLockResponse>, Status> {
        let request = request.into_inner();
        match &serde_json::from_str::<LockArgs>(&request.args) {
            Ok(args) => match GLOBAL_LOCAL_SERVER.write().await.unlock(args).await {
                Ok(result) => Ok(tonic::Response::new(GenerallyLockResponse {
                    success: result,
                    error_info: None,
                })),
                Err(err) => Ok(tonic::Response::new(GenerallyLockResponse {
                    success: false,
                    error_info: Some(format!("can not unlock, args: {}, err: {}", args, err)),
                })),
            },
            Err(err) => Ok(tonic::Response::new(GenerallyLockResponse {
                success: false,
                error_info: Some(format!("can not decode args, err: {}", err)),
            })),
        }
    }

    async fn r_lock(&self, request: Request<GenerallyLockRequest>) -> Result<Response<GenerallyLockResponse>, Status> {
        let request = request.into_inner();
        match &serde_json::from_str::<LockArgs>(&request.args) {
            Ok(args) => match GLOBAL_LOCAL_SERVER.write().await.rlock(args).await {
                Ok(result) => Ok(tonic::Response::new(GenerallyLockResponse {
                    success: result,
                    error_info: None,
                })),
                Err(err) => Ok(tonic::Response::new(GenerallyLockResponse {
                    success: false,
                    error_info: Some(format!("can not rlock, args: {}, err: {}", args, err)),
                })),
            },
            Err(err) => Ok(tonic::Response::new(GenerallyLockResponse {
                success: false,
                error_info: Some(format!("can not decode args, err: {}", err)),
            })),
        }
    }

    async fn r_un_lock(&self, request: Request<GenerallyLockRequest>) -> Result<Response<GenerallyLockResponse>, Status> {
        let request = request.into_inner();
        match &serde_json::from_str::<LockArgs>(&request.args) {
            Ok(args) => match GLOBAL_LOCAL_SERVER.write().await.runlock(args).await {
                Ok(result) => Ok(tonic::Response::new(GenerallyLockResponse {
                    success: result,
                    error_info: None,
                })),
                Err(err) => Ok(tonic::Response::new(GenerallyLockResponse {
                    success: false,
                    error_info: Some(format!("can not runlock, args: {}, err: {}", args, err)),
                })),
            },
            Err(err) => Ok(tonic::Response::new(GenerallyLockResponse {
                success: false,
                error_info: Some(format!("can not decode args, err: {}", err)),
            })),
        }
    }

    async fn force_un_lock(&self, request: Request<GenerallyLockRequest>) -> Result<Response<GenerallyLockResponse>, Status> {
        let request = request.into_inner();
        match &serde_json::from_str::<LockArgs>(&request.args) {
            Ok(args) => match GLOBAL_LOCAL_SERVER.write().await.force_unlock(args).await {
                Ok(result) => Ok(tonic::Response::new(GenerallyLockResponse {
                    success: result,
                    error_info: None,
                })),
                Err(err) => Ok(tonic::Response::new(GenerallyLockResponse {
                    success: false,
                    error_info: Some(format!("can not force_unlock, args: {}, err: {}", args, err)),
                })),
            },
            Err(err) => Ok(tonic::Response::new(GenerallyLockResponse {
                success: false,
                error_info: Some(format!("can not decode args, err: {}", err)),
            })),
        }
    }

    async fn refresh(&self, request: Request<GenerallyLockRequest>) -> Result<Response<GenerallyLockResponse>, Status> {
        let request = request.into_inner();
        match &serde_json::from_str::<LockArgs>(&request.args) {
            Ok(args) => match GLOBAL_LOCAL_SERVER.write().await.refresh(args).await {
                Ok(result) => Ok(tonic::Response::new(GenerallyLockResponse {
                    success: result,
                    error_info: None,
                })),
                Err(err) => Ok(tonic::Response::new(GenerallyLockResponse {
                    success: false,
                    error_info: Some(format!("can not refresh, args: {}, err: {}", args, err)),
                })),
            },
            Err(err) => Ok(tonic::Response::new(GenerallyLockResponse {
                success: false,
                error_info: Some(format!("can not decode args, err: {}", err)),
            })),
        }
    }
}
