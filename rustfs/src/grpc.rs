use std::{
    collections::HashMap,
    error::Error,
    io::{Cursor, ErrorKind},
    pin::Pin,
};

use ecstore::{
    admin_server_info::get_local_server_property,
    bucket::{metadata::load_bucket_metadata, metadata_sys},
    disk::{
        DeleteOptions, DiskAPI, DiskInfoOptions, DiskStore, FileInfoVersions, ReadMultipleReq, ReadOptions, UpdateMetadataOpts,
    },
    erasure::Writer,
    error::Error as EcsError,
    heal::{
        data_usage_cache::DataUsageCache,
        heal_commands::{get_local_background_heal_status, HealOpts},
    },
    metrics_realtime::{collect_local_metrics, CollectMetricsOpts, MetricType},
    new_object_layer_fn,
    peer::{LocalPeerS3Client, PeerS3Client},
    store::{all_local_disk_path, find_local_disk},
    store_api::{BucketOptions, DeleteBucketOptions, FileInfo, MakeBucketOptions, StorageAPI},
    store_err::StorageError,
    utils::err_to_proto_err,
};
use futures::{Stream, StreamExt};
use futures_util::future::join_all;
use lock::{lock_args::LockArgs, Locker, GLOBAL_LOCAL_SERVER};

use common::globals::GLOBAL_Local_Node_Name;
use ecstore::disk::error::is_err_eof;
use ecstore::metacache::writer::MetacacheReader;
use madmin::health::{
    get_cpus, get_mem_info, get_os_info, get_partitions, get_proc_info, get_sys_config, get_sys_errors, get_sys_services,
};
use madmin::net::get_net_info;
use protos::{
    models::{PingBody, PingBodyBuilder},
    proto_gen::node_service::{node_service_server::NodeService as Node, *},
};
use rmp_serde::{Deserializer, Serializer};
use serde::{Deserialize, Serialize};
use tokio::spawn;
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

        err = err.source()?;
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

    async fn heal_bucket(&self, request: Request<HealBucketRequest>) -> Result<Response<HealBucketResponse>, Status> {
        debug!("heal bucket");
        let request = request.into_inner();
        let options = match serde_json::from_str::<HealOpts>(&request.options) {
            Ok(options) => options,
            Err(err) => {
                return Ok(tonic::Response::new(HealBucketResponse {
                    success: false,
                    error: Some(err_to_proto_err(
                        &EcsError::new(StorageError::InvalidArgument(Default::default(), Default::default(), Default::default())),
                        &format!("decode HealOpts failed: {}", err),
                    )),
                }))
            }
        };

        match self.local_peer.heal_bucket(&request.bucket, &options).await {
            Ok(_) => Ok(tonic::Response::new(HealBucketResponse {
                success: true,
                error: None,
            })),

            Err(err) => Ok(tonic::Response::new(HealBucketResponse {
                success: false,
                error: Some(err_to_proto_err(&err, &format!("heal bucket failed: {}", err))),
            })),
        }
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
                    error: Some(err_to_proto_err(
                        &EcsError::new(StorageError::InvalidArgument(Default::default(), Default::default(), Default::default())),
                        &format!("decode BucketOptions failed: {}", err),
                    )),
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
                    error: None,
                }))
            }

            Err(err) => Ok(tonic::Response::new(ListBucketResponse {
                success: false,
                bucket_infos: Vec::new(),
                error: Some(err_to_proto_err(&err, &format!("list bucket failed: {}", err))),
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
                    error: Some(err_to_proto_err(
                        &EcsError::new(StorageError::InvalidArgument(Default::default(), Default::default(), Default::default())),
                        &format!("decode MakeBucketOptions failed: {}", err),
                    )),
                }))
            }
        };
        match self.local_peer.make_bucket(&request.name, &options).await {
            Ok(_) => Ok(tonic::Response::new(MakeBucketResponse {
                success: true,
                error: None,
            })),
            Err(err) => Ok(tonic::Response::new(MakeBucketResponse {
                success: false,
                error: Some(err_to_proto_err(&err, &format!("make bucket failed: {}", err))),
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
                    error: Some(err_to_proto_err(
                        &EcsError::new(StorageError::InvalidArgument(Default::default(), Default::default(), Default::default())),
                        &format!("decode BucketOptions failed: {}", err),
                    )),
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
                            error: Some(err_to_proto_err(
                                &EcsError::from_string("encode data failed"),
                                &format!("encode data failed: {}", err),
                            )),
                        }));
                    }
                };
                Ok(tonic::Response::new(GetBucketInfoResponse {
                    success: true,
                    bucket_info,
                    error: None,
                }))
            }

            Err(err) => Ok(tonic::Response::new(GetBucketInfoResponse {
                success: false,
                bucket_info: String::new(),
                error: Some(err_to_proto_err(&err, &format!("get bucket info failed: {}", err))),
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
                error: None,
            })),
            Err(err) => Ok(tonic::Response::new(DeleteBucketResponse {
                success: false,
                error: Some(err_to_proto_err(&err, &format!("delete bucket failed: {}", err))),
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
                    error: None,
                })),
                Err(err) => Ok(tonic::Response::new(ReadAllResponse {
                    success: false,
                    data: Vec::new(),
                    error: Some(err_to_proto_err(&err, &format!("read all failed: {}", err))),
                })),
            }
        } else {
            Ok(tonic::Response::new(ReadAllResponse {
                success: false,
                data: Vec::new(),
                error: Some(err_to_proto_err(
                    &EcsError::new(StorageError::InvalidArgument(Default::default(), Default::default(), Default::default())),
                    "can not find disk",
                )),
            }))
        }
    }

    async fn write_all(&self, request: Request<WriteAllRequest>) -> Result<Response<WriteAllResponse>, Status> {
        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
            match disk.write_all(&request.volume, &request.path, request.data).await {
                Ok(_) => Ok(tonic::Response::new(WriteAllResponse {
                    success: true,
                    error: None,
                })),
                Err(err) => Ok(tonic::Response::new(WriteAllResponse {
                    success: false,
                    error: Some(err_to_proto_err(&err, &format!("write all failed: {}", err))),
                })),
            }
        } else {
            Ok(tonic::Response::new(WriteAllResponse {
                success: false,
                error: Some(err_to_proto_err(
                    &EcsError::new(StorageError::InvalidArgument(Default::default(), Default::default(), Default::default())),
                    "can not find disk",
                )),
            }))
        }
    }

    async fn delete(&self, request: Request<DeleteRequest>) -> Result<Response<DeleteResponse>, Status> {
        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
            let options = match serde_json::from_str::<DeleteOptions>(&request.options) {
                Ok(options) => options,
                Err(err) => {
                    return Ok(tonic::Response::new(DeleteResponse {
                        success: false,
                        error: Some(err_to_proto_err(
                            &EcsError::new(StorageError::InvalidArgument(
                                Default::default(),
                                Default::default(),
                                Default::default(),
                            )),
                            &format!("decode DeleteOptions failed: {}", err),
                        )),
                    }));
                }
            };
            match disk.delete(&request.volume, &request.path, options).await {
                Ok(_) => Ok(tonic::Response::new(DeleteResponse {
                    success: true,
                    error: None,
                })),
                Err(err) => Ok(tonic::Response::new(DeleteResponse {
                    success: false,
                    error: Some(err_to_proto_err(&err, &format!("delete failed: {}", err))),
                })),
            }
        } else {
            Ok(tonic::Response::new(DeleteResponse {
                success: false,
                error: Some(err_to_proto_err(
                    &EcsError::new(StorageError::InvalidArgument(Default::default(), Default::default(), Default::default())),
                    "can not find disk",
                )),
            }))
        }
    }

    async fn verify_file(&self, request: Request<VerifyFileRequest>) -> Result<Response<VerifyFileResponse>, Status> {
        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
            let file_info = match serde_json::from_str::<FileInfo>(&request.file_info) {
                Ok(file_info) => file_info,
                Err(err) => {
                    return Ok(tonic::Response::new(VerifyFileResponse {
                        success: false,
                        check_parts_resp: "".to_string(),
                        error: Some(err_to_proto_err(
                            &EcsError::new(StorageError::InvalidArgument(
                                Default::default(),
                                Default::default(),
                                Default::default(),
                            )),
                            &format!("decode FileInfo failed: {}", err),
                        )),
                    }));
                }
            };
            match disk.verify_file(&request.volume, &request.path, &file_info).await {
                Ok(check_parts_resp) => {
                    let check_parts_resp = match serde_json::to_string(&check_parts_resp) {
                        Ok(check_parts_resp) => check_parts_resp,
                        Err(err) => {
                            return Ok(tonic::Response::new(VerifyFileResponse {
                                success: false,
                                check_parts_resp: String::new(),
                                error: Some(err_to_proto_err(
                                    &EcsError::from_string("encode data failed"),
                                    &format!("encode data failed: {}", err),
                                )),
                            }));
                        }
                    };
                    Ok(tonic::Response::new(VerifyFileResponse {
                        success: true,
                        check_parts_resp,
                        error: None,
                    }))
                }
                Err(err) => Ok(tonic::Response::new(VerifyFileResponse {
                    success: false,
                    check_parts_resp: "".to_string(),
                    error: Some(err_to_proto_err(&err, &format!("verify file failed: {}", err))),
                })),
            }
        } else {
            Ok(tonic::Response::new(VerifyFileResponse {
                success: false,
                check_parts_resp: "".to_string(),
                error: Some(err_to_proto_err(
                    &EcsError::new(StorageError::InvalidArgument(Default::default(), Default::default(), Default::default())),
                    "can not find disk",
                )),
            }))
        }
    }

    async fn check_parts(&self, request: Request<CheckPartsRequest>) -> Result<Response<CheckPartsResponse>, Status> {
        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
            let file_info = match serde_json::from_str::<FileInfo>(&request.file_info) {
                Ok(file_info) => file_info,
                Err(err) => {
                    return Ok(tonic::Response::new(CheckPartsResponse {
                        success: false,
                        check_parts_resp: "".to_string(),
                        error: Some(err_to_proto_err(
                            &EcsError::new(StorageError::InvalidArgument(
                                Default::default(),
                                Default::default(),
                                Default::default(),
                            )),
                            &format!("decode FileInfo failed: {}", err),
                        )),
                    }));
                }
            };
            match disk.verify_file(&request.volume, &request.path, &file_info).await {
                Ok(check_parts_resp) => {
                    let check_parts_resp = match serde_json::to_string(&check_parts_resp) {
                        Ok(check_parts_resp) => check_parts_resp,
                        Err(err) => {
                            return Ok(tonic::Response::new(CheckPartsResponse {
                                success: false,
                                check_parts_resp: String::new(),
                                error: Some(err_to_proto_err(
                                    &EcsError::from_string("encode data failed"),
                                    &format!("encode data failed: {}", err),
                                )),
                            }));
                        }
                    };
                    Ok(tonic::Response::new(CheckPartsResponse {
                        success: true,
                        check_parts_resp,
                        error: None,
                    }))
                }
                Err(err) => Ok(tonic::Response::new(CheckPartsResponse {
                    success: false,
                    check_parts_resp: "".to_string(),
                    error: Some(err_to_proto_err(&err, &format!("check parts failed: {}", err))),
                })),
            }
        } else {
            Ok(tonic::Response::new(CheckPartsResponse {
                success: false,
                check_parts_resp: "".to_string(),
                error: Some(err_to_proto_err(
                    &EcsError::new(StorageError::InvalidArgument(Default::default(), Default::default(), Default::default())),
                    "can not find disk",
                )),
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
                    error: None,
                })),
                Err(err) => Ok(tonic::Response::new(RenamePartResponse {
                    success: false,
                    error: Some(err_to_proto_err(&err, &format!("rename part failed: {}", err))),
                })),
            }
        } else {
            Ok(tonic::Response::new(RenamePartResponse {
                success: false,
                error: Some(err_to_proto_err(
                    &EcsError::new(StorageError::InvalidArgument(Default::default(), Default::default(), Default::default())),
                    "can not find disk",
                )),
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
                    error: None,
                })),
                Err(err) => Ok(tonic::Response::new(RenameFileResponse {
                    success: false,
                    error: Some(err_to_proto_err(&err, &format!("rename file failed: {}", err))),
                })),
            }
        } else {
            Ok(tonic::Response::new(RenameFileResponse {
                success: false,
                error: Some(err_to_proto_err(
                    &EcsError::new(StorageError::InvalidArgument(Default::default(), Default::default(), Default::default())),
                    "can not find disk",
                )),
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
                        error: None,
                    })),
                    Err(err) => Ok(tonic::Response::new(WriteResponse {
                        success: false,
                        error: Some(err_to_proto_err(&err, &format!("write failed: {}", err))),
                    })),
                },
                Err(err) => Ok(tonic::Response::new(WriteResponse {
                    success: false,
                    error: Some(err_to_proto_err(&err, &format!("get writer failed: {}", err))),
                })),
            }
        } else {
            Ok(tonic::Response::new(WriteResponse {
                success: false,
                error: Some(err_to_proto_err(
                    &EcsError::new(StorageError::InvalidArgument(Default::default(), Default::default(), Default::default())),
                    "can not find disk",
                )),
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
                                                error: Some(err_to_proto_err(
                                                    &err,
                                                    &format!("get get file writer failed: {}", err),
                                                )),
                                            }))
                                            .await
                                            .expect("working rx");
                                            break;
                                        }
                                    }
                                } else {
                                    tx.send(Ok(WriteResponse {
                                        success: false,
                                        error: Some(err_to_proto_err(
                                            &EcsError::new(StorageError::InvalidArgument(
                                                Default::default(),
                                                Default::default(),
                                                Default::default(),
                                            )),
                                            "can not find disk",
                                        )),
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
                                error: None,
                            })),
                            Err(err) => tx.send(Ok(WriteResponse {
                                success: false,
                                error: Some(err_to_proto_err(&err, &format!("write failed: {}", err))),
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
    async fn read_at(&self, _request: Request<Streaming<ReadAtRequest>>) -> Result<Response<Self::ReadAtStream>, Status> {
        info!("read_at");
        unimplemented!("read_at");

        // let mut in_stream = request.into_inner();
        // let (tx, rx) = mpsc::channel(128);

        // tokio::spawn(async move {
        //     let mut file_ref = None;
        //     while let Some(result) = in_stream.next().await {
        //         match result {
        //             Ok(v) => {
        //                 match file_ref.as_ref() {
        //                     Some(_) => (),
        //                     None => {
        //                         if let Some(disk) = find_local_disk(&v.disk).await {
        //                             match disk.read_file(&v.volume, &v.path).await {
        //                                 Ok(file_reader) => file_ref = Some(file_reader),
        //                                 Err(err) => {
        //                                     tx.send(Ok(ReadAtResponse {
        //                                         success: false,
        //                                         data: Vec::new(),
        //                                         error: Some(err_to_proto_err(&err, &format!("read file failed: {}", err))),
        //                                         read_size: -1,
        //                                     }))
        //                                     .await
        //                                     .expect("working rx");
        //                                     break;
        //                                 }
        //                             }
        //                         } else {
        //                             tx.send(Ok(ReadAtResponse {
        //                                 success: false,
        //                                 data: Vec::new(),
        //                                 error: Some(err_to_proto_err(
        //                                     &EcsError::new(StorageError::InvalidArgument(
        //                                         Default::default(),
        //                                         Default::default(),
        //                                         Default::default(),
        //                                     )),
        //                                     "can not find disk",
        //                                 )),
        //                                 read_size: -1,
        //                             }))
        //                             .await
        //                             .expect("working rx");
        //                             break;
        //                         }
        //                     }
        //                 };

        //                 let mut data = vec![0u8; v.length.try_into().unwrap()];

        //                 match file_ref
        //                     .as_mut()
        //                     .unwrap()
        //                     .read_at(v.offset.try_into().unwrap(), &mut data)
        //                     .await
        //                 {
        //                     Ok(read_size) => tx.send(Ok(ReadAtResponse {
        //                         success: true,
        //                         data,
        //                         read_size: read_size.try_into().unwrap(),
        //                         error: None,
        //                     })),
        //                     Err(err) => tx.send(Ok(ReadAtResponse {
        //                         success: false,
        //                         data: Vec::new(),
        //                         error: Some(err_to_proto_err(&err, &format!("read at failed: {}", err))),
        //                         read_size: -1,
        //                     })),
        //                 }
        //                 .await
        //                 .unwrap();
        //             }
        //             Err(err) => {
        //                 if let Some(io_err) = match_for_io_error(&err) {
        //                     if io_err.kind() == ErrorKind::BrokenPipe {
        //                         // here you can handle special case when client
        //                         // disconnected in unexpected way
        //                         eprintln!("\tclient disconnected: broken pipe");
        //                         break;
        //                     }
        //                 }

        //                 match tx.send(Err(err)).await {
        //                     Ok(_) => (),
        //                     Err(_err) => break, // response was dropped
        //                 }
        //             }
        //         }
        //     }
        //     println!("\tstream ended");
        // });

        // let out_stream = ReceiverStream::new(rx);

        // Ok(tonic::Response::new(Box::pin(out_stream)))
    }

    async fn list_dir(&self, request: Request<ListDirRequest>) -> Result<Response<ListDirResponse>, Status> {
        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
            match disk.list_dir("", &request.volume, "", 0).await {
                Ok(volumes) => Ok(tonic::Response::new(ListDirResponse {
                    success: true,
                    volumes,
                    error: None,
                })),
                Err(err) => Ok(tonic::Response::new(ListDirResponse {
                    success: false,
                    volumes: Vec::new(),
                    error: Some(err_to_proto_err(&err, &format!("list dir failed: {}", err))),
                })),
            }
        } else {
            Ok(tonic::Response::new(ListDirResponse {
                success: false,
                volumes: Vec::new(),
                error: Some(err_to_proto_err(
                    &EcsError::new(StorageError::InvalidArgument(Default::default(), Default::default(), Default::default())),
                    "can not find disk",
                )),
            }))
        }
    }

    type WalkDirStream = ResponseStream<WalkDirResponse>;
    async fn walk_dir(&self, request: Request<WalkDirRequest>) -> Result<Response<Self::WalkDirStream>, Status> {
        info!("walk_dir");
        let request = request.into_inner();
        let (tx, rx) = mpsc::channel(128);
        if let Some(disk) = self.find_disk(&request.disk).await {
            let mut buf = Deserializer::new(Cursor::new(request.walk_dir_options));
            let opts = match Deserialize::deserialize(&mut buf) {
                Ok(options) => options,
                Err(_) => {
                    return Err(Status::invalid_argument("invalid WalkDirOptions"));
                }
            };
            spawn(async {
                let (rd, mut wr) = tokio::io::duplex(64);
                let job1 = spawn(async move {
                    if let Err(err) = disk.walk_dir(opts, &mut wr).await {
                        println!("walk_dir err {:?}", err);
                    }
                });
                let job2 = spawn(async move {
                    let mut reader = MetacacheReader::new(rd);

                    loop {
                        match reader.peek().await {
                            Ok(res) => {
                                if let Some(info) = res {
                                    match serde_json::to_string(&info) {
                                        Ok(meta_cache_entry) => tx
                                            .send(Ok(WalkDirResponse {
                                                success: true,
                                                meta_cache_entry,
                                                error_info: None,
                                            }))
                                            .await
                                            .expect("working rx"),
                                        Err(e) => tx
                                            .send(Ok(WalkDirResponse {
                                                success: false,
                                                meta_cache_entry: "".to_string(),
                                                error_info: Some(e.to_string()),
                                            }))
                                            .await
                                            .expect("working rx"),
                                    }
                                } else {
                                    break;
                                }
                            }
                            Err(err) => {
                                if is_err_eof(&err) {
                                    break;
                                }

                                println!("get err {:?}", err);
                                break;
                            }
                        }
                    }
                });
                join_all(vec![job1, job2]).await;
            });
        } else {
            return Err(Status::invalid_argument(format!("invalid disk, all disk: {:?}", self.all_disk().await)));
        }

        let out_stream = ReceiverStream::new(rx);
        Ok(tonic::Response::new(Box::pin(out_stream)))
    }

    async fn rename_data(&self, request: Request<RenameDataRequest>) -> Result<Response<RenameDataResponse>, Status> {
        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
            let file_info = match serde_json::from_str::<FileInfo>(&request.file_info) {
                Ok(file_info) => file_info,
                Err(err) => {
                    return Ok(tonic::Response::new(RenameDataResponse {
                        success: false,
                        rename_data_resp: String::new(),
                        error: Some(err_to_proto_err(
                            &EcsError::new(StorageError::InvalidArgument(
                                Default::default(),
                                Default::default(),
                                Default::default(),
                            )),
                            &format!("decode FileInfo failed: {}", err),
                        )),
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
                        Err(err) => {
                            return Ok(tonic::Response::new(RenameDataResponse {
                                success: false,
                                rename_data_resp: String::new(),
                                error: Some(err_to_proto_err(
                                    &EcsError::from_string("encode data failed"),
                                    &format!("encode data failed: {}", err),
                                )),
                            }));
                        }
                    };
                    Ok(tonic::Response::new(RenameDataResponse {
                        success: true,
                        rename_data_resp,
                        error: None,
                    }))
                }
                Err(err) => Ok(tonic::Response::new(RenameDataResponse {
                    success: false,
                    rename_data_resp: String::new(),
                    error: Some(err_to_proto_err(&err, &format!("rename data failed: {}", err))),
                })),
            }
        } else {
            Ok(tonic::Response::new(RenameDataResponse {
                success: false,
                rename_data_resp: String::new(),
                error: Some(err_to_proto_err(
                    &EcsError::new(StorageError::InvalidArgument(Default::default(), Default::default(), Default::default())),
                    "can not find disk",
                )),
            }))
        }
    }

    async fn make_volumes(&self, request: Request<MakeVolumesRequest>) -> Result<Response<MakeVolumesResponse>, Status> {
        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
            match disk.make_volumes(request.volumes.iter().map(|s| &**s).collect()).await {
                Ok(_) => Ok(tonic::Response::new(MakeVolumesResponse {
                    success: true,
                    error: None,
                })),
                Err(err) => Ok(tonic::Response::new(MakeVolumesResponse {
                    success: false,
                    error: Some(err_to_proto_err(&err, &format!("make volume failed: {}", err))),
                })),
            }
        } else {
            Ok(tonic::Response::new(MakeVolumesResponse {
                success: false,
                error: Some(err_to_proto_err(
                    &EcsError::new(StorageError::InvalidArgument(Default::default(), Default::default(), Default::default())),
                    "can not find disk",
                )),
            }))
        }
    }

    async fn make_volume(&self, request: Request<MakeVolumeRequest>) -> Result<Response<MakeVolumeResponse>, Status> {
        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
            match disk.make_volume(&request.volume).await {
                Ok(_) => Ok(tonic::Response::new(MakeVolumeResponse {
                    success: true,
                    error: None,
                })),
                Err(err) => Ok(tonic::Response::new(MakeVolumeResponse {
                    success: false,
                    error: Some(err_to_proto_err(&err, &format!("make volume failed: {}", err))),
                })),
            }
        } else {
            Ok(tonic::Response::new(MakeVolumeResponse {
                success: false,
                error: Some(err_to_proto_err(
                    &EcsError::new(StorageError::InvalidArgument(Default::default(), Default::default(), Default::default())),
                    "can not find disk",
                )),
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
                        error: None,
                    }))
                }
                Err(err) => Ok(tonic::Response::new(ListVolumesResponse {
                    success: false,
                    volume_infos: Vec::new(),
                    error: Some(err_to_proto_err(&err, &format!("list volume failed: {}", err))),
                })),
            }
        } else {
            Ok(tonic::Response::new(ListVolumesResponse {
                success: false,
                volume_infos: Vec::new(),
                error: Some(err_to_proto_err(
                    &EcsError::new(StorageError::InvalidArgument(Default::default(), Default::default(), Default::default())),
                    "can not find disk",
                )),
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
                        error: None,
                    })),
                    Err(err) => Ok(tonic::Response::new(StatVolumeResponse {
                        success: false,
                        volume_info: String::new(),
                        error: Some(err_to_proto_err(
                            &EcsError::from_string("encode data failed"),
                            &format!("encode data failed: {}", err),
                        )),
                    })),
                },
                Err(err) => Ok(tonic::Response::new(StatVolumeResponse {
                    success: false,
                    volume_info: String::new(),
                    error: Some(err_to_proto_err(&err, &format!("state volume failed: {}", err))),
                })),
            }
        } else {
            Ok(tonic::Response::new(StatVolumeResponse {
                success: false,
                volume_info: String::new(),
                error: Some(err_to_proto_err(
                    &EcsError::new(StorageError::InvalidArgument(Default::default(), Default::default(), Default::default())),
                    "can not find disk",
                )),
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
                    error: None,
                })),
                Err(err) => Ok(tonic::Response::new(DeletePathsResponse {
                    success: false,
                    error: Some(err_to_proto_err(&err, &format!("delte paths failed: {}", err))),
                })),
            }
        } else {
            Ok(tonic::Response::new(DeletePathsResponse {
                success: false,
                error: Some(err_to_proto_err(
                    &EcsError::new(StorageError::InvalidArgument(Default::default(), Default::default(), Default::default())),
                    "can not find disk",
                )),
            }))
        }
    }

    async fn update_metadata(&self, request: Request<UpdateMetadataRequest>) -> Result<Response<UpdateMetadataResponse>, Status> {
        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
            let file_info = match serde_json::from_str::<FileInfo>(&request.file_info) {
                Ok(file_info) => file_info,
                Err(err) => {
                    return Ok(tonic::Response::new(UpdateMetadataResponse {
                        success: false,
                        error: Some(err_to_proto_err(
                            &EcsError::new(StorageError::InvalidArgument(
                                Default::default(),
                                Default::default(),
                                Default::default(),
                            )),
                            &format!("decode FileInfo failed: {}", err),
                        )),
                    }));
                }
            };
            let opts = match serde_json::from_str::<UpdateMetadataOpts>(&request.opts) {
                Ok(opts) => opts,
                Err(err) => {
                    return Ok(tonic::Response::new(UpdateMetadataResponse {
                        success: false,
                        error: Some(err_to_proto_err(
                            &EcsError::new(StorageError::InvalidArgument(
                                Default::default(),
                                Default::default(),
                                Default::default(),
                            )),
                            &format!("decode UpdateMetadataOpts failed: {}", err),
                        )),
                    }));
                }
            };

            match disk.update_metadata(&request.volume, &request.path, file_info, &opts).await {
                Ok(_) => Ok(tonic::Response::new(UpdateMetadataResponse {
                    success: true,
                    error: None,
                })),
                Err(err) => Ok(tonic::Response::new(UpdateMetadataResponse {
                    success: false,
                    error: Some(err_to_proto_err(&err, &format!("update metadata failed: {}", err))),
                })),
            }
        } else {
            Ok(tonic::Response::new(UpdateMetadataResponse {
                success: false,
                error: Some(err_to_proto_err(
                    &EcsError::new(StorageError::InvalidArgument(Default::default(), Default::default(), Default::default())),
                    "can not find disk",
                )),
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
                        error: Some(err_to_proto_err(
                            &EcsError::new(StorageError::InvalidArgument(
                                Default::default(),
                                Default::default(),
                                Default::default(),
                            )),
                            &format!("decode FileInfo failed: {}", err),
                        )),
                    }));
                }
            };
            match disk.write_metadata("", &request.volume, &request.path, file_info).await {
                Ok(_) => Ok(tonic::Response::new(WriteMetadataResponse {
                    success: true,
                    error: None,
                })),
                Err(err) => Ok(tonic::Response::new(WriteMetadataResponse {
                    success: false,
                    error: Some(err_to_proto_err(&err, &format!("write metadata failed: {}", err))),
                })),
            }
        } else {
            Ok(tonic::Response::new(WriteMetadataResponse {
                success: false,
                error: Some(err_to_proto_err(
                    &EcsError::new(StorageError::InvalidArgument(Default::default(), Default::default(), Default::default())),
                    "can not find disk",
                )),
            }))
        }
    }

    async fn read_version(&self, request: Request<ReadVersionRequest>) -> Result<Response<ReadVersionResponse>, Status> {
        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
            let opts = match serde_json::from_str::<ReadOptions>(&request.opts) {
                Ok(options) => options,
                Err(err) => {
                    return Ok(tonic::Response::new(ReadVersionResponse {
                        success: false,
                        file_info: String::new(),
                        error: Some(err_to_proto_err(
                            &EcsError::new(StorageError::InvalidArgument(
                                Default::default(),
                                Default::default(),
                                Default::default(),
                            )),
                            &format!("decode ReadOptions failed: {}", err),
                        )),
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
                        error: None,
                    })),
                    Err(err) => Ok(tonic::Response::new(ReadVersionResponse {
                        success: false,
                        file_info: String::new(),
                        error: Some(err_to_proto_err(
                            &EcsError::from_string("encode data failed"),
                            &format!("encode data failed: {}", err),
                        )),
                    })),
                },
                Err(err) => Ok(tonic::Response::new(ReadVersionResponse {
                    success: false,
                    file_info: String::new(),
                    error: Some(err_to_proto_err(&err, &format!("read version failed: {}", err))),
                })),
            }
        } else {
            Ok(tonic::Response::new(ReadVersionResponse {
                success: false,
                file_info: String::new(),
                error: Some(err_to_proto_err(
                    &EcsError::new(StorageError::InvalidArgument(Default::default(), Default::default(), Default::default())),
                    "can not find disk",
                )),
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
                        error: None,
                    })),
                    Err(err) => Ok(tonic::Response::new(ReadXlResponse {
                        success: false,
                        raw_file_info: String::new(),
                        error: Some(err_to_proto_err(
                            &EcsError::from_string("encode data failed"),
                            &format!("encode data failed: {}", err),
                        )),
                    })),
                },
                Err(err) => Ok(tonic::Response::new(ReadXlResponse {
                    success: false,
                    raw_file_info: String::new(),
                    error: Some(err_to_proto_err(&err, &format!("read xl failed: {}", err))),
                })),
            }
        } else {
            Ok(tonic::Response::new(ReadXlResponse {
                success: false,
                raw_file_info: String::new(),
                error: Some(err_to_proto_err(
                    &EcsError::new(StorageError::InvalidArgument(Default::default(), Default::default(), Default::default())),
                    "can not find disk",
                )),
            }))
        }
    }

    async fn delete_version(&self, request: Request<DeleteVersionRequest>) -> Result<Response<DeleteVersionResponse>, Status> {
        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
            let file_info = match serde_json::from_str::<FileInfo>(&request.file_info) {
                Ok(file_info) => file_info,
                Err(err) => {
                    return Ok(tonic::Response::new(DeleteVersionResponse {
                        success: false,
                        raw_file_info: "".to_string(),
                        error: Some(err_to_proto_err(
                            &EcsError::new(StorageError::InvalidArgument(
                                Default::default(),
                                Default::default(),
                                Default::default(),
                            )),
                            &format!("decode FileInfo failed: {}", err),
                        )),
                    }));
                }
            };
            let opts = match serde_json::from_str::<DeleteOptions>(&request.opts) {
                Ok(opts) => opts,
                Err(err) => {
                    return Ok(tonic::Response::new(DeleteVersionResponse {
                        success: false,
                        raw_file_info: "".to_string(),
                        error: Some(err_to_proto_err(
                            &EcsError::new(StorageError::InvalidArgument(
                                Default::default(),
                                Default::default(),
                                Default::default(),
                            )),
                            &format!("decode DeleteOptions failed: {}", err),
                        )),
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
                        error: None,
                    })),
                    Err(err) => Ok(tonic::Response::new(DeleteVersionResponse {
                        success: false,
                        raw_file_info: "".to_string(),
                        error: Some(err_to_proto_err(
                            &EcsError::from_string("encode data failed"),
                            &format!("encode data failed: {}", err),
                        )),
                    })),
                },
                Err(err) => Ok(tonic::Response::new(DeleteVersionResponse {
                    success: false,
                    raw_file_info: "".to_string(),
                    error: Some(err_to_proto_err(&err, &format!("read version failed: {}", err))),
                })),
            }
        } else {
            Ok(tonic::Response::new(DeleteVersionResponse {
                success: false,
                raw_file_info: "".to_string(),
                error: Some(err_to_proto_err(
                    &EcsError::new(StorageError::InvalidArgument(Default::default(), Default::default(), Default::default())),
                    "can not find disk",
                )),
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
                    Err(err) => {
                        return Ok(tonic::Response::new(DeleteVersionsResponse {
                            success: false,
                            errors: Vec::new(),
                            error: Some(err_to_proto_err(
                                &EcsError::new(StorageError::InvalidArgument(
                                    Default::default(),
                                    Default::default(),
                                    Default::default(),
                                )),
                                &format!("decode FileInfoVersions failed: {}", err),
                            )),
                        }));
                    }
                };
            }
            let opts = match serde_json::from_str::<DeleteOptions>(&request.opts) {
                Ok(opts) => opts,
                Err(err) => {
                    return Ok(tonic::Response::new(DeleteVersionsResponse {
                        success: false,
                        errors: Vec::new(),
                        error: Some(err_to_proto_err(
                            &EcsError::new(StorageError::InvalidArgument(
                                Default::default(),
                                Default::default(),
                                Default::default(),
                            )),
                            &format!("decode DeleteOptions failed: {}", err),
                        )),
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
                        error: None,
                    }))
                }
                Err(err) => Ok(tonic::Response::new(DeleteVersionsResponse {
                    success: false,
                    errors: Vec::new(),
                    error: Some(err_to_proto_err(&err, &format!("delete version failed: {}", err))),
                })),
            }
        } else {
            Ok(tonic::Response::new(DeleteVersionsResponse {
                success: false,
                errors: Vec::new(),
                error: Some(err_to_proto_err(
                    &EcsError::new(StorageError::InvalidArgument(Default::default(), Default::default(), Default::default())),
                    "can not find disk",
                )),
            }))
        }
    }

    async fn read_multiple(&self, request: Request<ReadMultipleRequest>) -> Result<Response<ReadMultipleResponse>, Status> {
        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
            let read_multiple_req = match serde_json::from_str::<ReadMultipleReq>(&request.read_multiple_req) {
                Ok(read_multiple_req) => read_multiple_req,
                Err(err) => {
                    return Ok(tonic::Response::new(ReadMultipleResponse {
                        success: false,
                        read_multiple_resps: Vec::new(),
                        error: Some(err_to_proto_err(
                            &EcsError::new(StorageError::InvalidArgument(
                                Default::default(),
                                Default::default(),
                                Default::default(),
                            )),
                            &format!("decode ReadMultipleReq failed: {}", err),
                        )),
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
                        error: None,
                    }))
                }
                Err(err) => Ok(tonic::Response::new(ReadMultipleResponse {
                    success: false,
                    read_multiple_resps: Vec::new(),
                    error: Some(err_to_proto_err(&err, &format!("read multiple failed: {}", err))),
                })),
            }
        } else {
            Ok(tonic::Response::new(ReadMultipleResponse {
                success: false,
                read_multiple_resps: Vec::new(),
                error: Some(err_to_proto_err(
                    &EcsError::new(StorageError::InvalidArgument(Default::default(), Default::default(), Default::default())),
                    "can not find disk",
                )),
            }))
        }
    }

    async fn delete_volume(&self, request: Request<DeleteVolumeRequest>) -> Result<Response<DeleteVolumeResponse>, Status> {
        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
            match disk.delete_volume(&request.volume).await {
                Ok(_) => Ok(tonic::Response::new(DeleteVolumeResponse {
                    success: true,
                    error: None,
                })),
                Err(err) => Ok(tonic::Response::new(DeleteVolumeResponse {
                    success: false,
                    error: Some(err_to_proto_err(&err, &format!("delete volume failed: {}", err))),
                })),
            }
        } else {
            Ok(tonic::Response::new(DeleteVolumeResponse {
                success: false,
                error: Some(err_to_proto_err(
                    &EcsError::new(StorageError::InvalidArgument(Default::default(), Default::default(), Default::default())),
                    "can not find disk",
                )),
            }))
        }
    }

    async fn disk_info(&self, request: Request<DiskInfoRequest>) -> Result<Response<DiskInfoResponse>, Status> {
        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
            let opts = match serde_json::from_str::<DiskInfoOptions>(&request.opts) {
                Ok(opts) => opts,
                Err(err) => {
                    return Ok(tonic::Response::new(DiskInfoResponse {
                        success: false,
                        disk_info: "".to_string(),
                        error: Some(err_to_proto_err(
                            &EcsError::new(StorageError::InvalidArgument(
                                Default::default(),
                                Default::default(),
                                Default::default(),
                            )),
                            &format!("decode DiskInfoOptions failed: {}", err),
                        )),
                    }));
                }
            };
            match disk.disk_info(&opts).await {
                Ok(disk_info) => match serde_json::to_string(&disk_info) {
                    Ok(disk_info) => Ok(tonic::Response::new(DiskInfoResponse {
                        success: true,
                        disk_info,
                        error: None,
                    })),
                    Err(err) => Ok(tonic::Response::new(DiskInfoResponse {
                        success: false,
                        disk_info: "".to_string(),
                        error: Some(err_to_proto_err(
                            &EcsError::from_string("encode data failed"),
                            &format!("encode data failed: {}", err),
                        )),
                    })),
                },
                Err(err) => Ok(tonic::Response::new(DiskInfoResponse {
                    success: false,
                    disk_info: "".to_string(),
                    error: Some(err_to_proto_err(&err, &format!("disk info failed: {}", err))),
                })),
            }
        } else {
            Ok(tonic::Response::new(DiskInfoResponse {
                success: false,
                disk_info: "".to_string(),
                error: Some(err_to_proto_err(
                    &EcsError::new(StorageError::InvalidArgument(Default::default(), Default::default(), Default::default())),
                    "can not find disk",
                )),
            }))
        }
    }

    type NsScannerStream = ResponseStream<NsScannerResponse>;
    async fn ns_scanner(&self, request: Request<Streaming<NsScannerRequest>>) -> Result<Response<Self::NsScannerStream>, Status> {
        info!("ns_scanner");

        let mut in_stream = request.into_inner();
        let (tx, rx) = mpsc::channel(10);

        tokio::spawn(async move {
            match in_stream.next().await {
                Some(Ok(request)) => {
                    if let Some(disk) = find_local_disk(&request.disk).await {
                        let cache = match serde_json::from_str::<DataUsageCache>(&request.cache) {
                            Ok(cache) => cache,
                            Err(err) => {
                                tx.send(Ok(NsScannerResponse {
                                    success: false,
                                    update: "".to_string(),
                                    data_usage_cache: "".to_string(),
                                    error: Some(err_to_proto_err(
                                        &EcsError::new(StorageError::InvalidArgument(
                                            Default::default(),
                                            Default::default(),
                                            Default::default(),
                                        )),
                                        &format!("decode DataUsageCache failed: {}", err),
                                    )),
                                }))
                                .await
                                .expect("working rx");
                                return;
                            }
                        };
                        let (updates_tx, mut updates_rx) = mpsc::channel(100);
                        let tx_clone = tx.clone();
                        let task = tokio::spawn(async move {
                            loop {
                                match updates_rx.recv().await {
                                    Some(update) => {
                                        let update = serde_json::to_string(&update).expect("encode failed");
                                        tx_clone
                                            .send(Ok(NsScannerResponse {
                                                success: true,
                                                update,
                                                data_usage_cache: "".to_string(),
                                                error: None,
                                            }))
                                            .await
                                            .expect("working rx");
                                    }
                                    None => return,
                                }
                            }
                        });
                        let data_usage_cache = disk.ns_scanner(&cache, updates_tx, request.scan_mode as usize, None).await;
                        let _ = task.await;
                        match data_usage_cache {
                            Ok(data_usage_cache) => {
                                let data_usage_cache = serde_json::to_string(&data_usage_cache).expect("encode failed");
                                tx.send(Ok(NsScannerResponse {
                                    success: true,
                                    update: "".to_string(),
                                    data_usage_cache,
                                    error: None,
                                }))
                                .await
                                .expect("working rx");
                            }
                            Err(err) => {
                                tx.send(Ok(NsScannerResponse {
                                    success: false,
                                    update: "".to_string(),
                                    data_usage_cache: "".to_string(),
                                    error: Some(err_to_proto_err(&err, &format!("scanner failed: {}", err))),
                                }))
                                .await
                                .expect("working rx");
                            }
                        }
                    } else {
                        tx.send(Ok(NsScannerResponse {
                            success: false,
                            update: "".to_string(),
                            data_usage_cache: "".to_string(),
                            error: Some(err_to_proto_err(
                                &EcsError::new(StorageError::InvalidArgument(
                                    Default::default(),
                                    Default::default(),
                                    Default::default(),
                                )),
                                "can not find disk",
                            )),
                        }))
                        .await
                        .expect("working rx");
                    }
                }
                _ => todo!(),
            }
        });

        let out_stream = ReceiverStream::new(rx);
        Ok(tonic::Response::new(Box::pin(out_stream)))
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

    async fn local_storage_info(
        &self,
        _request: Request<LocalStorageInfoRequest>,
    ) -> Result<Response<LocalStorageInfoResponse>, Status> {
        // let request = request.into_inner();

        let Some(store) = new_object_layer_fn() else {
            return Ok(tonic::Response::new(LocalStorageInfoResponse {
                success: false,
                storage_info: vec![],
                error_info: Some("errServerNotInitialized".to_string()),
            }));
        };

        let info = store.local_storage_info().await;
        let mut buf = Vec::new();
        if let Err(err) = info.serialize(&mut Serializer::new(&mut buf)) {
            return Ok(tonic::Response::new(LocalStorageInfoResponse {
                success: false,
                storage_info: vec![],
                error_info: Some(err.to_string()),
            }));
        }

        Ok(tonic::Response::new(LocalStorageInfoResponse {
            success: true,
            storage_info: buf,
            error_info: None,
        }))
    }

    async fn server_info(&self, _request: Request<ServerInfoRequest>) -> Result<Response<ServerInfoResponse>, Status> {
        let info = get_local_server_property().await;
        let mut buf = Vec::new();
        if let Err(err) = info.serialize(&mut Serializer::new(&mut buf)) {
            return Ok(tonic::Response::new(ServerInfoResponse {
                success: false,
                server_properties: vec![],
                error_info: Some(err.to_string()),
            }));
        }
        Ok(tonic::Response::new(ServerInfoResponse {
            success: true,
            server_properties: buf,
            error_info: None,
        }))
    }

    async fn get_cpus(&self, _request: Request<GetCpusRequest>) -> Result<Response<GetCpusResponse>, Status> {
        let info = get_cpus();
        let mut buf = Vec::new();
        if let Err(err) = info.serialize(&mut Serializer::new(&mut buf)) {
            return Ok(tonic::Response::new(GetCpusResponse {
                success: false,
                cpus: vec![],
                error_info: Some(err.to_string()),
            }));
        }
        Ok(tonic::Response::new(GetCpusResponse {
            success: true,
            cpus: buf,
            error_info: None,
        }))
    }

    async fn get_net_info(&self, _request: Request<GetNetInfoRequest>) -> Result<Response<GetNetInfoResponse>, Status> {
        let addr = GLOBAL_Local_Node_Name.read().await.clone();
        let info = get_net_info(&addr, "");
        let mut buf = Vec::new();
        if let Err(err) = info.serialize(&mut Serializer::new(&mut buf)) {
            return Ok(tonic::Response::new(GetNetInfoResponse {
                success: false,
                net_info: vec![],
                error_info: Some(err.to_string()),
            }));
        }
        Ok(tonic::Response::new(GetNetInfoResponse {
            success: true,
            net_info: buf,
            error_info: None,
        }))
    }

    async fn get_partitions(&self, _request: Request<GetPartitionsRequest>) -> Result<Response<GetPartitionsResponse>, Status> {
        let partitions = get_partitions();
        let mut buf = Vec::new();
        if let Err(err) = partitions.serialize(&mut Serializer::new(&mut buf)) {
            return Ok(tonic::Response::new(GetPartitionsResponse {
                success: false,
                partitions: vec![],
                error_info: Some(err.to_string()),
            }));
        }
        Ok(tonic::Response::new(GetPartitionsResponse {
            success: true,
            partitions: buf,
            error_info: None,
        }))
    }

    async fn get_os_info(&self, _request: Request<GetOsInfoRequest>) -> Result<Response<GetOsInfoResponse>, Status> {
        let os_info = get_os_info();
        let mut buf = Vec::new();
        if let Err(err) = os_info.serialize(&mut Serializer::new(&mut buf)) {
            return Ok(tonic::Response::new(GetOsInfoResponse {
                success: false,
                os_info: vec![],
                error_info: Some(err.to_string()),
            }));
        }
        Ok(tonic::Response::new(GetOsInfoResponse {
            success: true,
            os_info: buf,
            error_info: None,
        }))
    }

    async fn get_se_linux_info(
        &self,
        _request: Request<GetSeLinuxInfoRequest>,
    ) -> Result<Response<GetSeLinuxInfoResponse>, Status> {
        let addr = GLOBAL_Local_Node_Name.read().await.clone();
        let info = get_sys_services(&addr);
        let mut buf = Vec::new();
        if let Err(err) = info.serialize(&mut Serializer::new(&mut buf)) {
            return Ok(tonic::Response::new(GetSeLinuxInfoResponse {
                success: false,
                sys_services: vec![],
                error_info: Some(err.to_string()),
            }));
        }
        Ok(tonic::Response::new(GetSeLinuxInfoResponse {
            success: true,
            sys_services: buf,
            error_info: None,
        }))
    }

    async fn get_sys_config(&self, _request: Request<GetSysConfigRequest>) -> Result<Response<GetSysConfigResponse>, Status> {
        let addr = GLOBAL_Local_Node_Name.read().await.clone();
        let info = get_sys_config(&addr);
        let mut buf = Vec::new();
        if let Err(err) = info.serialize(&mut Serializer::new(&mut buf)) {
            return Ok(tonic::Response::new(GetSysConfigResponse {
                success: false,
                sys_config: vec![],
                error_info: Some(err.to_string()),
            }));
        }
        Ok(tonic::Response::new(GetSysConfigResponse {
            success: true,
            sys_config: buf,
            error_info: None,
        }))
    }

    async fn get_sys_errors(&self, _request: Request<GetSysErrorsRequest>) -> Result<Response<GetSysErrorsResponse>, Status> {
        let addr = GLOBAL_Local_Node_Name.read().await.clone();
        let info = get_sys_errors(&addr);
        let mut buf = Vec::new();
        if let Err(err) = info.serialize(&mut Serializer::new(&mut buf)) {
            return Ok(tonic::Response::new(GetSysErrorsResponse {
                success: false,
                sys_errors: vec![],
                error_info: Some(err.to_string()),
            }));
        }
        Ok(tonic::Response::new(GetSysErrorsResponse {
            success: true,
            sys_errors: buf,
            error_info: None,
        }))
    }

    async fn get_mem_info(&self, _request: Request<GetMemInfoRequest>) -> Result<Response<GetMemInfoResponse>, Status> {
        let addr = GLOBAL_Local_Node_Name.read().await.clone();
        let info = get_mem_info(&addr);
        let mut buf = Vec::new();
        if let Err(err) = info.serialize(&mut Serializer::new(&mut buf)) {
            return Ok(tonic::Response::new(GetMemInfoResponse {
                success: false,
                mem_info: vec![],
                error_info: Some(err.to_string()),
            }));
        }
        Ok(tonic::Response::new(GetMemInfoResponse {
            success: true,
            mem_info: buf,
            error_info: None,
        }))
    }

    async fn get_metrics(&self, request: Request<GetMetricsRequest>) -> Result<Response<GetMetricsResponse>, Status> {
        let request = request.into_inner();
        let mut buf_t = Deserializer::new(Cursor::new(request.metric_type));
        let t: MetricType = Deserialize::deserialize(&mut buf_t).unwrap();

        let mut buf_o = Deserializer::new(Cursor::new(request.opts));
        let opts: CollectMetricsOpts = Deserialize::deserialize(&mut buf_o).unwrap();

        let info = collect_local_metrics(t, &opts).await;

        let mut buf = Vec::new();
        if let Err(err) = info.serialize(&mut Serializer::new(&mut buf)) {
            return Ok(tonic::Response::new(GetMetricsResponse {
                success: false,
                realtime_metrics: vec![],
                error_info: Some(err.to_string()),
            }));
        }
        Ok(tonic::Response::new(GetMetricsResponse {
            success: true,
            realtime_metrics: buf,
            error_info: None,
        }))
    }

    async fn get_proc_info(&self, _request: Request<GetProcInfoRequest>) -> Result<Response<GetProcInfoResponse>, Status> {
        let addr = GLOBAL_Local_Node_Name.read().await.clone();
        let info = get_proc_info(&addr);
        let mut buf = Vec::new();
        if let Err(err) = info.serialize(&mut Serializer::new(&mut buf)) {
            return Ok(tonic::Response::new(GetProcInfoResponse {
                success: false,
                proc_info: vec![],
                error_info: Some(err.to_string()),
            }));
        }
        Ok(tonic::Response::new(GetProcInfoResponse {
            success: true,
            proc_info: buf,
            error_info: None,
        }))
    }

    async fn start_profiling(
        &self,
        _request: Request<StartProfilingRequest>,
    ) -> Result<Response<StartProfilingResponse>, Status> {
        todo!()
    }

    async fn download_profile_data(
        &self,
        _request: Request<DownloadProfileDataRequest>,
    ) -> Result<Response<DownloadProfileDataResponse>, Status> {
        todo!()
    }

    async fn get_bucket_stats(
        &self,
        _request: Request<GetBucketStatsDataRequest>,
    ) -> Result<Response<GetBucketStatsDataResponse>, Status> {
        todo!()
    }

    async fn get_sr_metrics(
        &self,
        _request: Request<GetSrMetricsDataRequest>,
    ) -> Result<Response<GetSrMetricsDataResponse>, Status> {
        todo!()
    }

    async fn get_all_bucket_stats(
        &self,
        _request: Request<GetAllBucketStatsRequest>,
    ) -> Result<Response<GetAllBucketStatsResponse>, Status> {
        todo!()
    }

    async fn load_bucket_metadata(
        &self,
        request: Request<LoadBucketMetadataRequest>,
    ) -> Result<Response<LoadBucketMetadataResponse>, Status> {
        let request = request.into_inner();
        let bucket = request.bucket;
        if bucket.is_empty() {
            return Ok(tonic::Response::new(LoadBucketMetadataResponse {
                success: false,
                error_info: Some("bucket name is missing".to_string()),
            }));
        }

        let Some(store) = new_object_layer_fn() else {
            return Ok(tonic::Response::new(LoadBucketMetadataResponse {
                success: false,
                error_info: Some("errServerNotInitialized".to_string()),
            }));
        };

        match load_bucket_metadata(store, &bucket).await {
            Ok(meta) => {
                metadata_sys::set_bucket_metadata(bucket, meta).await;
                Ok(tonic::Response::new(LoadBucketMetadataResponse {
                    success: true,
                    error_info: None,
                }))
            }
            Err(err) => Ok(tonic::Response::new(LoadBucketMetadataResponse {
                success: false,
                error_info: Some(err.to_string()),
            })),
        }
    }

    async fn delete_bucket_metadata(
        &self,
        request: Request<DeleteBucketMetadataRequest>,
    ) -> Result<Response<DeleteBucketMetadataResponse>, Status> {
        let request = request.into_inner();
        let _bucket = request.bucket;

        //todo
        Ok(tonic::Response::new(DeleteBucketMetadataResponse {
            success: true,
            error_info: None,
        }))
    }

    async fn delete_policy(&self, request: Request<DeletePolicyRequest>) -> Result<Response<DeletePolicyResponse>, Status> {
        let request = request.into_inner();
        let policy = request.policy_name;
        if policy.is_empty() {
            return Ok(tonic::Response::new(DeletePolicyResponse {
                success: false,
                error_info: Some("policy name is missing".to_string()),
            }));
        }

        let Some(_store) = new_object_layer_fn() else {
            return Ok(tonic::Response::new(DeletePolicyResponse {
                success: false,
                error_info: Some("errServerNotInitialized".to_string()),
            }));
        };

        todo!()
    }

    async fn load_policy(&self, request: Request<LoadPolicyRequest>) -> Result<Response<LoadPolicyResponse>, Status> {
        let request = request.into_inner();
        let policy = request.policy_name;
        if policy.is_empty() {
            return Ok(tonic::Response::new(LoadPolicyResponse {
                success: false,
                error_info: Some("policy name is missing".to_string()),
            }));
        }
        let Some(_store) = new_object_layer_fn() else {
            return Ok(tonic::Response::new(LoadPolicyResponse {
                success: false,
                error_info: Some("errServerNotInitialized".to_string()),
            }));
        };
        todo!()
    }

    async fn load_policy_mapping(
        &self,
        request: Request<LoadPolicyMappingRequest>,
    ) -> Result<Response<LoadPolicyMappingResponse>, Status> {
        let request = request.into_inner();
        let user_or_group = request.user_or_group;
        if user_or_group.is_empty() {
            return Ok(tonic::Response::new(LoadPolicyMappingResponse {
                success: false,
                error_info: Some("user_or_group name is missing".to_string()),
            }));
        }
        let _user_type = request.user_type;
        let _is_group = request.is_group;
        let Some(_store) = new_object_layer_fn() else {
            return Ok(tonic::Response::new(LoadPolicyMappingResponse {
                success: false,
                error_info: Some("errServerNotInitialized".to_string()),
            }));
        };
        todo!()
    }

    async fn delete_user(&self, request: Request<DeleteUserRequest>) -> Result<Response<DeleteUserResponse>, Status> {
        let request = request.into_inner();
        let access_key = request.access_key;
        if access_key.is_empty() {
            return Ok(tonic::Response::new(DeleteUserResponse {
                success: false,
                error_info: Some("access_key name is missing".to_string()),
            }));
        }
        let Some(_store) = new_object_layer_fn() else {
            return Ok(tonic::Response::new(DeleteUserResponse {
                success: false,
                error_info: Some("errServerNotInitialized".to_string()),
            }));
        };

        todo!()
    }

    async fn delete_service_account(
        &self,
        request: Request<DeleteServiceAccountRequest>,
    ) -> Result<Response<DeleteServiceAccountResponse>, Status> {
        let request = request.into_inner();
        let access_key = request.access_key;
        if access_key.is_empty() {
            return Ok(tonic::Response::new(DeleteServiceAccountResponse {
                success: false,
                error_info: Some("access_key name is missing".to_string()),
            }));
        }
        let Some(_store) = new_object_layer_fn() else {
            return Ok(tonic::Response::new(DeleteServiceAccountResponse {
                success: false,
                error_info: Some("errServerNotInitialized".to_string()),
            }));
        };
        todo!()
    }

    async fn load_user(&self, request: Request<LoadUserRequest>) -> Result<Response<LoadUserResponse>, Status> {
        let request = request.into_inner();
        let access_key = request.access_key;
        let _temp = request.temp;
        if access_key.is_empty() {
            return Ok(tonic::Response::new(LoadUserResponse {
                success: false,
                error_info: Some("access_key name is missing".to_string()),
            }));
        }

        let Some(_store) = new_object_layer_fn() else {
            return Ok(tonic::Response::new(LoadUserResponse {
                success: false,
                error_info: Some("errServerNotInitialized".to_string()),
            }));
        };

        todo!()
    }

    async fn load_service_account(
        &self,
        request: Request<LoadServiceAccountRequest>,
    ) -> Result<Response<LoadServiceAccountResponse>, Status> {
        let request = request.into_inner();
        let access_key = request.access_key;
        if access_key.is_empty() {
            return Ok(tonic::Response::new(LoadServiceAccountResponse {
                success: false,
                error_info: Some("access_key name is missing".to_string()),
            }));
        }

        let Some(_store) = new_object_layer_fn() else {
            return Ok(tonic::Response::new(LoadServiceAccountResponse {
                success: false,
                error_info: Some("errServerNotInitialized".to_string()),
            }));
        };
        todo!()
    }

    async fn load_group(&self, request: Request<LoadGroupRequest>) -> Result<Response<LoadGroupResponse>, Status> {
        let request = request.into_inner();
        let group = request.group;
        if group.is_empty() {
            return Ok(tonic::Response::new(LoadGroupResponse {
                success: false,
                error_info: Some("group name is missing".to_string()),
            }));
        }

        let Some(_store) = new_object_layer_fn() else {
            return Ok(tonic::Response::new(LoadGroupResponse {
                success: false,
                error_info: Some("errServerNotInitialized".to_string()),
            }));
        };
        todo!()
    }

    async fn reload_site_replication_config(
        &self,
        _request: Request<ReloadSiteReplicationConfigRequest>,
    ) -> Result<Response<ReloadSiteReplicationConfigResponse>, Status> {
        let Some(_store) = new_object_layer_fn() else {
            return Ok(tonic::Response::new(ReloadSiteReplicationConfigResponse {
                success: false,
                error_info: Some("errServerNotInitialized".to_string()),
            }));
        };
        todo!()
    }

    async fn signal_service(&self, request: Request<SignalServiceRequest>) -> Result<Response<SignalServiceResponse>, Status> {
        let request = request.into_inner();
        let _vars = match request.vars {
            Some(vars) => vars.value,
            None => HashMap::new(),
        };
        todo!()
    }

    async fn background_heal_status(
        &self,
        _request: Request<BackgroundHealStatusRequest>,
    ) -> Result<Response<BackgroundHealStatusResponse>, Status> {
        let (state, ok) = get_local_background_heal_status().await;
        if !ok {
            return Ok(tonic::Response::new(BackgroundHealStatusResponse {
                success: false,
                bg_heal_state: vec![],
                error_info: Some("errServerNotInitialized".to_string()),
            }));
        }

        let mut buf = Vec::new();
        if let Err(err) = state.serialize(&mut Serializer::new(&mut buf)) {
            return Ok(tonic::Response::new(BackgroundHealStatusResponse {
                success: false,
                bg_heal_state: vec![],
                error_info: Some(err.to_string()),
            }));
        }
        Ok(tonic::Response::new(BackgroundHealStatusResponse {
            success: true,
            bg_heal_state: buf,
            error_info: None,
        }))
    }

    async fn get_metacache_listing(
        &self,
        _request: Request<GetMetacacheListingRequest>,
    ) -> Result<Response<GetMetacacheListingResponse>, Status> {
        todo!()
    }

    async fn update_metacache_listing(
        &self,
        _request: Request<UpdateMetacacheListingRequest>,
    ) -> Result<Response<UpdateMetacacheListingResponse>, Status> {
        todo!()
    }

    async fn reload_pool_meta(
        &self,
        _request: Request<ReloadPoolMetaRequest>,
    ) -> Result<Response<ReloadPoolMetaResponse>, Status> {
        let Some(store) = new_object_layer_fn() else {
            return Ok(tonic::Response::new(ReloadPoolMetaResponse {
                success: false,
                error_info: Some("errServerNotInitialized".to_string()),
            }));
        };
        match store.reload_pool_meta().await {
            Ok(_) => Ok(tonic::Response::new(ReloadPoolMetaResponse {
                success: true,
                error_info: None,
            })),
            Err(err) => Ok(tonic::Response::new(ReloadPoolMetaResponse {
                success: false,
                error_info: Some(err.to_string()),
            })),
        }
    }

    async fn stop_rebalance(&self, _request: Request<StopRebalanceRequest>) -> Result<Response<StopRebalanceResponse>, Status> {
        let Some(_store) = new_object_layer_fn() else {
            return Ok(tonic::Response::new(StopRebalanceResponse {
                success: false,
                error_info: Some("errServerNotInitialized".to_string()),
            }));
        };

        // todo
        // store.stop_rebalance().await;
        todo!()
    }

    async fn load_rebalance_meta(
        &self,
        _request: Request<LoadRebalanceMetaRequest>,
    ) -> Result<Response<LoadRebalanceMetaResponse>, Status> {
        todo!()
    }

    async fn load_transition_tier_config(
        &self,
        _request: Request<LoadTransitionTierConfigRequest>,
    ) -> Result<Response<LoadTransitionTierConfigResponse>, Status> {
        todo!()
    }
}
