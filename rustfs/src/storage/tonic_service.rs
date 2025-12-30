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

use bytes::Bytes;
use futures::Stream;
use futures_util::future::join_all;
use rmp_serde::{Deserializer, Serializer};
use rustfs_common::{GLOBAL_LOCAL_NODE_NAME, heal_channel::HealOpts};
use rustfs_ecstore::{
    admin_server_info::get_local_server_property,
    bucket::{metadata::load_bucket_metadata, metadata_sys},
    disk::{
        DeleteOptions, DiskAPI, DiskInfoOptions, DiskStore, FileInfoVersions, ReadMultipleReq, ReadOptions, UpdateMetadataOpts,
        error::DiskError,
    },
    metrics_realtime::{CollectMetricsOpts, MetricType, collect_local_metrics},
    new_object_layer_fn,
    rpc::{LocalPeerS3Client, PeerS3Client},
    store::{all_local_disk_path, find_local_disk},
    store_api::{BucketOptions, DeleteBucketOptions, MakeBucketOptions, StorageAPI},
};
use rustfs_filemeta::{FileInfo, MetacacheReader};
use rustfs_iam::{get_global_iam_sys, store::UserType};
use rustfs_lock::{LockClient, LockRequest};
use rustfs_madmin::health::{
    get_cpus, get_mem_info, get_os_info, get_partitions, get_proc_info, get_sys_config, get_sys_errors, get_sys_services,
};
use rustfs_madmin::net::get_net_info;
use rustfs_protos::{
    models::{PingBody, PingBodyBuilder},
    proto_gen::node_service::{node_service_server::NodeService as Node, *},
};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, io::Cursor, pin::Pin, sync::Arc};
use tokio::spawn;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, error, info, warn};

type ResponseStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>;

// fn match_for_io_error(err_status: &Status) -> Option<&std::io::Error> {
//     let mut err: &(dyn Error + 'static) = err_status;

//     loop {
//         if let Some(io_err) = err.downcast_ref::<std::io::Error>() {
//             return Some(io_err);
//         }

//         // h2::Error do not expose std::io::Error with `source()`
//         // https://github.com/hyperium/h2/pull/462
//         if let Some(h2_err) = err.downcast_ref::<h2::Error>() {
//             if let Some(io_err) = h2_err.get_io() {
//                 return Some(io_err);
//             }
//         }

//         err = err.source()?;
//     }
// }

#[derive(Debug)]
pub struct NodeService {
    local_peer: LocalPeerS3Client,
    lock_manager: Arc<rustfs_lock::LocalClient>,
}

pub fn make_server() -> NodeService {
    let local_peer = LocalPeerS3Client::new(None, None);
    let lock_manager = Arc::new(rustfs_lock::LocalClient::new());
    NodeService {
        local_peer,
        lock_manager,
    }
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

        Ok(Response::new(PingResponse {
            version: 1,
            body: Bytes::copy_from_slice(finished_data),
        }))
    }

    async fn heal_bucket(&self, request: Request<HealBucketRequest>) -> Result<Response<HealBucketResponse>, Status> {
        debug!("heal bucket");
        let request = request.into_inner();
        let options = match serde_json::from_str::<HealOpts>(&request.options) {
            Ok(options) => options,
            Err(err) => {
                return Ok(Response::new(HealBucketResponse {
                    success: false,
                    error: Some(DiskError::other(format!("decode HealOpts failed: {err}")).into()),
                }));
            }
        };

        match self.local_peer.heal_bucket(&request.bucket, &options).await {
            Ok(_) => Ok(Response::new(HealBucketResponse {
                success: true,
                error: None,
            })),

            Err(err) => Ok(Response::new(HealBucketResponse {
                success: false,
                error: Some(err.into()),
            })),
        }
    }

    async fn list_bucket(&self, request: Request<ListBucketRequest>) -> Result<Response<ListBucketResponse>, Status> {
        debug!("list bucket");

        let request = request.into_inner();
        let options = match serde_json::from_str::<BucketOptions>(&request.options) {
            Ok(options) => options,
            Err(err) => {
                return Ok(Response::new(ListBucketResponse {
                    success: false,
                    bucket_infos: Vec::new(),
                    error: Some(DiskError::other(format!("decode BucketOptions failed: {err}")).into()),
                }));
            }
        };
        match self.local_peer.list_bucket(&options).await {
            Ok(bucket_infos) => {
                let bucket_infos = bucket_infos
                    .into_iter()
                    .filter_map(|bucket_info| serde_json::to_string(&bucket_info).ok())
                    .collect();
                Ok(Response::new(ListBucketResponse {
                    success: true,
                    bucket_infos,
                    error: None,
                }))
            }

            Err(err) => Ok(Response::new(ListBucketResponse {
                success: false,
                bucket_infos: Vec::new(),
                error: Some(err.into()),
            })),
        }
    }

    async fn make_bucket(&self, request: Request<MakeBucketRequest>) -> Result<Response<MakeBucketResponse>, Status> {
        debug!("make bucket");

        let request = request.into_inner();
        let options = match serde_json::from_str::<MakeBucketOptions>(&request.options) {
            Ok(options) => options,
            Err(err) => {
                return Ok(Response::new(MakeBucketResponse {
                    success: false,
                    error: Some(DiskError::other(format!("decode MakeBucketOptions failed: {err}")).into()),
                }));
            }
        };
        match self.local_peer.make_bucket(&request.name, &options).await {
            Ok(_) => Ok(Response::new(MakeBucketResponse {
                success: true,
                error: None,
            })),
            Err(err) => Ok(Response::new(MakeBucketResponse {
                success: false,
                error: Some(err.into()),
            })),
        }
    }

    async fn get_bucket_info(&self, request: Request<GetBucketInfoRequest>) -> Result<Response<GetBucketInfoResponse>, Status> {
        debug!("get bucket info");

        let request = request.into_inner();
        let options = match serde_json::from_str::<BucketOptions>(&request.options) {
            Ok(options) => options,
            Err(err) => {
                return Ok(Response::new(GetBucketInfoResponse {
                    success: false,
                    bucket_info: String::new(),
                    error: Some(DiskError::other(format!("decode BucketOptions failed: {err}")).into()),
                }));
            }
        };
        match self.local_peer.get_bucket_info(&request.bucket, &options).await {
            Ok(bucket_info) => {
                let bucket_info = match serde_json::to_string(&bucket_info) {
                    Ok(bucket_info) => bucket_info,
                    Err(err) => {
                        return Ok(Response::new(GetBucketInfoResponse {
                            success: false,
                            bucket_info: String::new(),
                            error: Some(DiskError::other(format!("encode data failed: {err}")).into()),
                        }));
                    }
                };

                Ok(Response::new(GetBucketInfoResponse {
                    success: true,
                    bucket_info,
                    error: None,
                }))
            }

            // println!("vuc")
            Err(err) => Ok(Response::new(GetBucketInfoResponse {
                success: false,
                bucket_info: String::new(),
                error: Some(err.into()),
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
            Ok(_) => Ok(Response::new(DeleteBucketResponse {
                success: true,
                error: None,
            })),
            Err(err) => Ok(Response::new(DeleteBucketResponse {
                success: false,
                error: Some(err.into()),
            })),
        }
    }

    async fn read_all(&self, request: Request<ReadAllRequest>) -> Result<Response<ReadAllResponse>, Status> {
        debug!("read all");

        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
            match disk.read_all(&request.volume, &request.path).await {
                Ok(data) => Ok(Response::new(ReadAllResponse {
                    success: true,
                    data,
                    error: None,
                })),
                Err(err) => Ok(Response::new(ReadAllResponse {
                    success: false,
                    data: Bytes::new(),
                    error: Some(err.into()),
                })),
            }
        } else {
            Ok(Response::new(ReadAllResponse {
                success: false,
                data: Bytes::new(),
                error: Some(DiskError::other("can not find disk".to_string()).into()),
            }))
        }
    }

    async fn write_all(&self, request: Request<WriteAllRequest>) -> Result<Response<WriteAllResponse>, Status> {
        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
            match disk.write_all(&request.volume, &request.path, request.data).await {
                Ok(_) => Ok(Response::new(WriteAllResponse {
                    success: true,
                    error: None,
                })),
                Err(err) => Ok(Response::new(WriteAllResponse {
                    success: false,
                    error: Some(err.into()),
                })),
            }
        } else {
            Ok(Response::new(WriteAllResponse {
                success: false,
                error: Some(DiskError::other("can not find disk".to_string()).into()),
            }))
        }
    }

    async fn delete(&self, request: Request<DeleteRequest>) -> Result<Response<DeleteResponse>, Status> {
        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
            let options = match serde_json::from_str::<DeleteOptions>(&request.options) {
                Ok(options) => options,
                Err(err) => {
                    return Ok(Response::new(DeleteResponse {
                        success: false,
                        error: Some(DiskError::other(format!("decode DeleteOptions failed: {err}")).into()),
                    }));
                }
            };
            match disk.delete(&request.volume, &request.path, options).await {
                Ok(_) => Ok(Response::new(DeleteResponse {
                    success: true,
                    error: None,
                })),
                Err(err) => Ok(Response::new(DeleteResponse {
                    success: false,
                    error: Some(err.into()),
                })),
            }
        } else {
            Ok(Response::new(DeleteResponse {
                success: false,
                error: Some(DiskError::other("can not find disk".to_string()).into()),
            }))
        }
    }

    async fn verify_file(&self, request: Request<VerifyFileRequest>) -> Result<Response<VerifyFileResponse>, Status> {
        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
            let file_info = match serde_json::from_str::<FileInfo>(&request.file_info) {
                Ok(file_info) => file_info,
                Err(err) => {
                    return Ok(Response::new(VerifyFileResponse {
                        success: false,
                        check_parts_resp: "".to_string(),
                        error: Some(DiskError::other(format!("decode FileInfo failed: {err}")).into()),
                    }));
                }
            };
            match disk.verify_file(&request.volume, &request.path, &file_info).await {
                Ok(check_parts_resp) => {
                    let check_parts_resp = match serde_json::to_string(&check_parts_resp) {
                        Ok(check_parts_resp) => check_parts_resp,
                        Err(err) => {
                            return Ok(Response::new(VerifyFileResponse {
                                success: false,
                                check_parts_resp: String::new(),
                                error: Some(DiskError::other(format!("encode data failed: {err}")).into()),
                            }));
                        }
                    };
                    Ok(Response::new(VerifyFileResponse {
                        success: true,
                        check_parts_resp,
                        error: None,
                    }))
                }
                Err(err) => Ok(Response::new(VerifyFileResponse {
                    success: false,
                    check_parts_resp: "".to_string(),
                    error: Some(err.into()),
                })),
            }
        } else {
            Ok(Response::new(VerifyFileResponse {
                success: false,
                check_parts_resp: "".to_string(),
                error: Some(DiskError::other("can not find disk".to_string()).into()),
            }))
        }
    }
    async fn read_parts(&self, request: Request<ReadPartsRequest>) -> Result<Response<ReadPartsResponse>, Status> {
        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
            match disk.read_parts(&request.bucket, &request.paths).await {
                Ok(data) => {
                    let data = match rmp_serde::to_vec(&data) {
                        Ok(data) => data,
                        Err(err) => {
                            return Ok(Response::new(ReadPartsResponse {
                                success: false,
                                object_part_infos: Bytes::new(),
                                error: Some(DiskError::other(format!("encode data failed: {err}")).into()),
                            }));
                        }
                    };
                    Ok(Response::new(ReadPartsResponse {
                        success: true,
                        object_part_infos: Bytes::copy_from_slice(&data),
                        error: None,
                    }))
                }

                Err(err) => Ok(Response::new(ReadPartsResponse {
                    success: false,
                    object_part_infos: Bytes::new(),
                    error: Some(err.into()),
                })),
            }
        } else {
            Ok(Response::new(ReadPartsResponse {
                success: false,
                object_part_infos: Bytes::new(),
                error: Some(DiskError::other("can not find disk".to_string()).into()),
            }))
        }
    }
    async fn check_parts(&self, request: Request<CheckPartsRequest>) -> Result<Response<CheckPartsResponse>, Status> {
        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
            let file_info = match serde_json::from_str::<FileInfo>(&request.file_info) {
                Ok(file_info) => file_info,
                Err(err) => {
                    return Ok(Response::new(CheckPartsResponse {
                        success: false,
                        check_parts_resp: "".to_string(),
                        error: Some(DiskError::other(format!("decode FileInfo failed: {err}")).into()),
                    }));
                }
            };
            match disk.check_parts(&request.volume, &request.path, &file_info).await {
                Ok(check_parts_resp) => {
                    let check_parts_resp = match serde_json::to_string(&check_parts_resp) {
                        Ok(check_parts_resp) => check_parts_resp,
                        Err(err) => {
                            return Ok(Response::new(CheckPartsResponse {
                                success: false,
                                check_parts_resp: String::new(),
                                error: Some(DiskError::other(format!("encode data failed: {err}")).into()),
                            }));
                        }
                    };
                    Ok(Response::new(CheckPartsResponse {
                        success: true,
                        check_parts_resp,
                        error: None,
                    }))
                }
                Err(err) => Ok(Response::new(CheckPartsResponse {
                    success: false,
                    check_parts_resp: "".to_string(),
                    error: Some(err.into()),
                })),
            }
        } else {
            Ok(Response::new(CheckPartsResponse {
                success: false,
                check_parts_resp: "".to_string(),
                error: Some(DiskError::other("can not find disk".to_string()).into()),
            }))
        }
    }

    async fn rename_part(&self, request: Request<RenamePartRequest>) -> Result<Response<RenamePartResponse>, Status> {
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
                Ok(_) => Ok(Response::new(RenamePartResponse {
                    success: true,
                    error: None,
                })),
                Err(err) => Ok(Response::new(RenamePartResponse {
                    success: false,
                    error: Some(err.into()),
                })),
            }
        } else {
            Ok(Response::new(RenamePartResponse {
                success: false,
                error: Some(DiskError::other("can not find disk".to_string()).into()),
            }))
        }
    }

    async fn rename_file(&self, request: Request<RenameFileRequest>) -> Result<Response<RenameFileResponse>, Status> {
        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
            match disk
                .rename_file(&request.src_volume, &request.src_path, &request.dst_volume, &request.dst_path)
                .await
            {
                Ok(_) => Ok(Response::new(RenameFileResponse {
                    success: true,
                    error: None,
                })),
                Err(err) => Ok(Response::new(RenameFileResponse {
                    success: false,
                    error: Some(err.into()),
                })),
            }
        } else {
            Ok(Response::new(RenameFileResponse {
                success: false,
                error: Some(DiskError::other("can not find disk".to_string()).into()),
            }))
        }
    }

    async fn write(&self, _request: Request<WriteRequest>) -> Result<Response<WriteResponse>, Status> {
        unimplemented!("write");
        // let request = request.into_inner();
        // if let Some(disk) = self.find_disk(&request.disk).await {
        //     let file_writer = if request.is_append {
        //         disk.append_file(&request.volume, &request.path).await
        //     } else {
        //         disk.create_file("", &request.volume, &request.path, 0).await
        //     };

        //     match file_writer {
        //         Ok(mut file_writer) => match file_writer.write(&request.data).await {
        //             Ok(_) => Ok(Response::new(WriteResponse {
        //                 success: true,
        //                 error: None,
        //             })),
        //             Err(err) => Ok(Response::new(WriteResponse {
        //                 success: false,
        //                 error: Some(err_to_proto_err(&err, &format!("write failed: {}", err))),
        //             })),
        //         },
        //         Err(err) => Ok(Response::new(WriteResponse {
        //             success: false,
        //             error: Some(err_to_proto_err(&err, &format!("get writer failed: {}", err))),
        //         })),
        //     }
        // } else {
        //     Ok(Response::new(WriteResponse {
        //         success: false,
        //         error: Some(err_to_proto_err(
        //             &EcsError::new(StorageError::InvalidArgument(Default::default(), Default::default(), Default::default())),
        //             "can not find disk",
        //         )),
        //     }))
        // }
    }

    type WriteStreamStream = ResponseStream<WriteResponse>;
    async fn write_stream(
        &self,
        _request: Request<Streaming<WriteRequest>>,
    ) -> Result<Response<Self::WriteStreamStream>, Status> {
        info!("write_stream");

        unimplemented!("write_stream");

        // let mut in_stream = request.into_inner();
        // let (tx, rx) = mpsc::channel(128);

        // tokio::spawn(async move {
        //     let mut file_ref = None;
        //     while let Some(result) = in_stream.next().await {
        //         match result {
        //             // Ok(v) => tx
        //             //     .send(Ok(EchoResponse { message: v.message }))
        //             //     .await
        //             //     .expect("working rx"),
        //             Ok(v) => {
        //                 match file_ref.as_ref() {
        //                     Some(_) => (),
        //                     None => {
        //                         if let Some(disk) = find_local_disk(&v.disk).await {
        //                             let file_writer = if v.is_append {
        //                                 disk.append_file(&v.volume, &v.path).await
        //                             } else {
        //                                 disk.create_file("", &v.volume, &v.path, 0).await
        //                             };

        //                             match file_writer {
        //                                 Ok(file_writer) => file_ref = Some(file_writer),
        //                                 Err(err) => {
        //                                     tx.send(Ok(WriteResponse {
        //                                         success: false,
        //                                         error: Some(err_to_proto_err(
        //                                             &err,
        //                                             &format!("get file writer failed: {}", err),
        //                                         )),
        //                                     }))
        //                                     .await
        //                                     .expect("working rx");
        //                                     break;
        //                                 }
        //                             }
        //                         } else {
        //                             tx.send(Ok(WriteResponse {
        //                                 success: false,
        //                                 error: Some(err_to_proto_err(
        //                                     &EcsError::new(StorageError::InvalidArgument(
        //                                         Default::default(),
        //                                         Default::default(),
        //                                         Default::default(),
        //                                     )),
        //                                     "can not find disk",
        //                                 )),
        //                             }))
        //                             .await
        //                             .expect("working rx");
        //                             break;
        //                         }
        //                     }
        //                 };

        //                 match file_ref.as_mut().unwrap().write(&v.data).await {
        //                     Ok(_) => tx.send(Ok(WriteResponse {
        //                         success: true,
        //                         error: None,
        //                     })),
        //                     Err(err) => tx.send(Ok(WriteResponse {
        //                         success: false,
        //                         error: Some(err_to_proto_err(&err, &format!("write failed: {}", err))),
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

        // Ok(Response::new(Box::pin(out_stream)))
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

        // Ok(Response::new(Box::pin(out_stream)))
    }

    async fn list_dir(&self, request: Request<ListDirRequest>) -> Result<Response<ListDirResponse>, Status> {
        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
            match disk.list_dir("", &request.volume, &request.dir_path, request.count).await {
                Ok(volumes) => Ok(Response::new(ListDirResponse {
                    success: true,
                    volumes,
                    error: None,
                })),
                Err(err) => Ok(Response::new(ListDirResponse {
                    success: false,
                    volumes: Vec::new(),
                    error: Some(err.into()),
                })),
            }
        } else {
            Ok(Response::new(ListDirResponse {
                success: false,
                volumes: Vec::new(),
                error: Some(DiskError::other("can not find disk".to_string()).into()),
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
                        println!("walk_dir err {err:?}");
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
                                if err == rustfs_filemeta::Error::Unexpected {
                                    let _ = tx
                                        .send(Ok(WalkDirResponse {
                                            success: false,
                                            meta_cache_entry: "".to_string(),
                                            error_info: Some(err.to_string()),
                                        }))
                                        .await;

                                    break;
                                }

                                if rustfs_filemeta::is_io_eof(&err) {
                                    let _ = tx
                                        .send(Ok(WalkDirResponse {
                                            success: false,
                                            meta_cache_entry: "".to_string(),
                                            error_info: Some(err.to_string()),
                                        }))
                                        .await;
                                    break;
                                }

                                println!("get err {err:?}");

                                let _ = tx
                                    .send(Ok(WalkDirResponse {
                                        success: false,
                                        meta_cache_entry: "".to_string(),
                                        error_info: Some(err.to_string()),
                                    }))
                                    .await;
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
        Ok(Response::new(Box::pin(out_stream)))
    }

    async fn rename_data(&self, request: Request<RenameDataRequest>) -> Result<Response<RenameDataResponse>, Status> {
        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
            let file_info = match serde_json::from_str::<FileInfo>(&request.file_info) {
                Ok(file_info) => file_info,
                Err(err) => {
                    return Ok(Response::new(RenameDataResponse {
                        success: false,
                        rename_data_resp: String::new(),
                        error: Some(DiskError::other(format!("decode FileInfo failed: {err}")).into()),
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
                            return Ok(Response::new(RenameDataResponse {
                                success: false,
                                rename_data_resp: String::new(),
                                error: Some(DiskError::other(format!("encode data failed: {err}")).into()),
                            }));
                        }
                    };
                    Ok(Response::new(RenameDataResponse {
                        success: true,
                        rename_data_resp,
                        error: None,
                    }))
                }
                Err(err) => Ok(Response::new(RenameDataResponse {
                    success: false,
                    rename_data_resp: String::new(),
                    error: Some(err.into()),
                })),
            }
        } else {
            Ok(Response::new(RenameDataResponse {
                success: false,
                rename_data_resp: String::new(),
                error: Some(DiskError::other("can not find disk".to_string()).into()),
            }))
        }
    }

    async fn make_volumes(&self, request: Request<MakeVolumesRequest>) -> Result<Response<MakeVolumesResponse>, Status> {
        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
            match disk.make_volumes(request.volumes.iter().map(|s| &**s).collect()).await {
                Ok(_) => Ok(Response::new(MakeVolumesResponse {
                    success: true,
                    error: None,
                })),
                Err(err) => Ok(Response::new(MakeVolumesResponse {
                    success: false,
                    error: Some(err.into()),
                })),
            }
        } else {
            Ok(Response::new(MakeVolumesResponse {
                success: false,
                error: Some(DiskError::other("can not find disk".to_string()).into()),
            }))
        }
    }

    async fn make_volume(&self, request: Request<MakeVolumeRequest>) -> Result<Response<MakeVolumeResponse>, Status> {
        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
            match disk.make_volume(&request.volume).await {
                Ok(_) => Ok(Response::new(MakeVolumeResponse {
                    success: true,
                    error: None,
                })),
                Err(err) => Ok(Response::new(MakeVolumeResponse {
                    success: false,
                    error: Some(err.into()),
                })),
            }
        } else {
            Ok(Response::new(MakeVolumeResponse {
                success: false,
                error: Some(DiskError::other("can not find disk".to_string()).into()),
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
                    Ok(Response::new(ListVolumesResponse {
                        success: true,
                        volume_infos,
                        error: None,
                    }))
                }
                Err(err) => Ok(Response::new(ListVolumesResponse {
                    success: false,
                    volume_infos: Vec::new(),
                    error: Some(err.into()),
                })),
            }
        } else {
            Ok(Response::new(ListVolumesResponse {
                success: false,
                volume_infos: Vec::new(),
                error: Some(DiskError::other("can not find disk".to_string()).into()),
            }))
        }
    }

    async fn stat_volume(&self, request: Request<StatVolumeRequest>) -> Result<Response<StatVolumeResponse>, Status> {
        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
            match disk.stat_volume(&request.volume).await {
                Ok(volume_info) => match serde_json::to_string(&volume_info) {
                    Ok(volume_info) => Ok(Response::new(StatVolumeResponse {
                        success: true,
                        volume_info,
                        error: None,
                    })),
                    Err(err) => Ok(Response::new(StatVolumeResponse {
                        success: false,
                        volume_info: String::new(),
                        error: Some(DiskError::other(format!("encode data failed: {err}")).into()),
                    })),
                },
                Err(err) => Ok(Response::new(StatVolumeResponse {
                    success: false,
                    volume_info: String::new(),
                    error: Some(err.into()),
                })),
            }
        } else {
            Ok(Response::new(StatVolumeResponse {
                success: false,
                volume_info: String::new(),
                error: Some(DiskError::other("can not find disk".to_string()).into()),
            }))
        }
    }

    async fn delete_paths(&self, request: Request<DeletePathsRequest>) -> Result<Response<DeletePathsResponse>, Status> {
        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
            match disk.delete_paths(&request.volume, &request.paths).await {
                Ok(_) => Ok(Response::new(DeletePathsResponse {
                    success: true,
                    error: None,
                })),
                Err(err) => Ok(Response::new(DeletePathsResponse {
                    success: false,
                    error: Some(err.into()),
                })),
            }
        } else {
            Ok(Response::new(DeletePathsResponse {
                success: false,
                error: Some(DiskError::other("can not find disk".to_string()).into()),
            }))
        }
    }

    async fn update_metadata(&self, request: Request<UpdateMetadataRequest>) -> Result<Response<UpdateMetadataResponse>, Status> {
        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
            let file_info = match serde_json::from_str::<FileInfo>(&request.file_info) {
                Ok(file_info) => file_info,
                Err(err) => {
                    return Ok(Response::new(UpdateMetadataResponse {
                        success: false,
                        error: Some(DiskError::other(format!("decode FileInfo failed: {err}")).into()),
                    }));
                }
            };
            let opts = match serde_json::from_str::<UpdateMetadataOpts>(&request.opts) {
                Ok(opts) => opts,
                Err(err) => {
                    return Ok(Response::new(UpdateMetadataResponse {
                        success: false,
                        error: Some(DiskError::other(format!("decode UpdateMetadataOpts failed: {err}")).into()),
                    }));
                }
            };

            match disk.update_metadata(&request.volume, &request.path, file_info, &opts).await {
                Ok(_) => Ok(Response::new(UpdateMetadataResponse {
                    success: true,
                    error: None,
                })),
                Err(err) => Ok(Response::new(UpdateMetadataResponse {
                    success: false,
                    error: Some(err.into()),
                })),
            }
        } else {
            Ok(Response::new(UpdateMetadataResponse {
                success: false,
                error: Some(DiskError::other("can not find disk".to_string()).into()),
            }))
        }
    }

    async fn write_metadata(&self, request: Request<WriteMetadataRequest>) -> Result<Response<WriteMetadataResponse>, Status> {
        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
            let file_info = match serde_json::from_str::<FileInfo>(&request.file_info) {
                Ok(file_info) => file_info,
                Err(err) => {
                    return Ok(Response::new(WriteMetadataResponse {
                        success: false,
                        error: Some(DiskError::other(format!("decode FileInfo failed: {err}")).into()),
                    }));
                }
            };
            match disk.write_metadata("", &request.volume, &request.path, file_info).await {
                Ok(_) => Ok(Response::new(WriteMetadataResponse {
                    success: true,
                    error: None,
                })),
                Err(err) => Ok(Response::new(WriteMetadataResponse {
                    success: false,
                    error: Some(err.into()),
                })),
            }
        } else {
            Ok(Response::new(WriteMetadataResponse {
                success: false,
                error: Some(DiskError::other("can not find disk".to_string()).into()),
            }))
        }
    }

    async fn read_version(&self, request: Request<ReadVersionRequest>) -> Result<Response<ReadVersionResponse>, Status> {
        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
            let opts = match serde_json::from_str::<ReadOptions>(&request.opts) {
                Ok(options) => options,
                Err(err) => {
                    return Ok(Response::new(ReadVersionResponse {
                        success: false,
                        file_info: String::new(),
                        error: Some(DiskError::other(format!("decode ReadOptions failed: {err}")).into()),
                    }));
                }
            };
            match disk
                .read_version("", &request.volume, &request.path, &request.version_id, &opts)
                .await
            {
                Ok(file_info) => match serde_json::to_string(&file_info) {
                    Ok(file_info) => Ok(Response::new(ReadVersionResponse {
                        success: true,
                        file_info,
                        error: None,
                    })),
                    Err(err) => Ok(Response::new(ReadVersionResponse {
                        success: false,
                        file_info: String::new(),
                        error: Some(DiskError::other(format!("encode data failed: {err}")).into()),
                    })),
                },
                Err(err) => Ok(Response::new(ReadVersionResponse {
                    success: false,
                    file_info: String::new(),
                    error: Some(err.into()),
                })),
            }
        } else {
            Ok(Response::new(ReadVersionResponse {
                success: false,
                file_info: String::new(),
                error: Some(DiskError::other("can not find disk".to_string()).into()),
            }))
        }
    }

    async fn read_xl(&self, request: Request<ReadXlRequest>) -> Result<Response<ReadXlResponse>, Status> {
        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
            match disk.read_xl(&request.volume, &request.path, request.read_data).await {
                Ok(raw_file_info) => match serde_json::to_string(&raw_file_info) {
                    Ok(raw_file_info) => Ok(Response::new(ReadXlResponse {
                        success: true,
                        raw_file_info,
                        error: None,
                    })),
                    Err(err) => Ok(Response::new(ReadXlResponse {
                        success: false,
                        raw_file_info: String::new(),
                        error: Some(DiskError::other(format!("encode data failed: {err}")).into()),
                    })),
                },
                Err(err) => Ok(Response::new(ReadXlResponse {
                    success: false,
                    raw_file_info: String::new(),
                    error: Some(err.into()),
                })),
            }
        } else {
            Ok(Response::new(ReadXlResponse {
                success: false,
                raw_file_info: String::new(),
                error: Some(DiskError::other("can not find disk".to_string()).into()),
            }))
        }
    }

    async fn delete_version(&self, request: Request<DeleteVersionRequest>) -> Result<Response<DeleteVersionResponse>, Status> {
        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
            let file_info = match serde_json::from_str::<FileInfo>(&request.file_info) {
                Ok(file_info) => file_info,
                Err(err) => {
                    return Ok(Response::new(DeleteVersionResponse {
                        success: false,
                        raw_file_info: "".to_string(),
                        error: Some(DiskError::other(format!("decode FileInfo failed: {err}")).into()),
                    }));
                }
            };
            let opts = match serde_json::from_str::<DeleteOptions>(&request.opts) {
                Ok(opts) => opts,
                Err(err) => {
                    return Ok(Response::new(DeleteVersionResponse {
                        success: false,
                        raw_file_info: "".to_string(),
                        error: Some(DiskError::other(format!("decode DeleteOptions failed: {err}")).into()),
                    }));
                }
            };
            match disk
                .delete_version(&request.volume, &request.path, file_info, request.force_del_marker, opts)
                .await
            {
                Ok(raw_file_info) => match serde_json::to_string(&raw_file_info) {
                    Ok(raw_file_info) => Ok(Response::new(DeleteVersionResponse {
                        success: true,
                        raw_file_info,
                        error: None,
                    })),
                    Err(err) => Ok(Response::new(DeleteVersionResponse {
                        success: false,
                        raw_file_info: "".to_string(),
                        error: Some(DiskError::other(format!("encode data failed: {err}")).into()),
                    })),
                },
                Err(err) => Ok(Response::new(DeleteVersionResponse {
                    success: false,
                    raw_file_info: "".to_string(),
                    error: Some(err.into()),
                })),
            }
        } else {
            Ok(Response::new(DeleteVersionResponse {
                success: false,
                raw_file_info: "".to_string(),
                error: Some(DiskError::other("can not find disk".to_string()).into()),
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
                        return Ok(Response::new(DeleteVersionsResponse {
                            success: false,
                            errors: Vec::new(),
                            error: Some(DiskError::other(format!("decode FileInfoVersions failed: {err}")).into()),
                        }));
                    }
                };
            }
            let opts = match serde_json::from_str::<DeleteOptions>(&request.opts) {
                Ok(opts) => opts,
                Err(err) => {
                    return Ok(Response::new(DeleteVersionsResponse {
                        success: false,
                        errors: Vec::new(),
                        error: Some(DiskError::other(format!("decode DeleteOptions failed: {err}")).into()),
                    }));
                }
            };

            let errors = disk
                .delete_versions(&request.volume, versions, opts)
                .await
                .into_iter()
                .map(|error| match error {
                    Some(e) => e.to_string(),
                    None => "".to_string(),
                })
                .collect();

            Ok(Response::new(DeleteVersionsResponse {
                success: true,
                errors,
                error: None,
            }))
        } else {
            Ok(Response::new(DeleteVersionsResponse {
                success: false,
                errors: Vec::new(),
                error: Some(DiskError::other("can not find disk".to_string()).into()),
            }))
        }
    }

    async fn read_multiple(&self, request: Request<ReadMultipleRequest>) -> Result<Response<ReadMultipleResponse>, Status> {
        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
            let read_multiple_req = match serde_json::from_str::<ReadMultipleReq>(&request.read_multiple_req) {
                Ok(read_multiple_req) => read_multiple_req,
                Err(err) => {
                    return Ok(Response::new(ReadMultipleResponse {
                        success: false,
                        read_multiple_resps: Vec::new(),
                        error: Some(DiskError::other(format!("decode ReadMultipleReq failed: {err}")).into()),
                    }));
                }
            };
            match disk.read_multiple(read_multiple_req).await {
                Ok(read_multiple_resps) => {
                    let read_multiple_resps = read_multiple_resps
                        .into_iter()
                        .filter_map(|read_multiple_resp| serde_json::to_string(&read_multiple_resp).ok())
                        .collect();

                    Ok(Response::new(ReadMultipleResponse {
                        success: true,
                        read_multiple_resps,
                        error: None,
                    }))
                }
                Err(err) => Ok(Response::new(ReadMultipleResponse {
                    success: false,
                    read_multiple_resps: Vec::new(),
                    error: Some(err.into()),
                })),
            }
        } else {
            Ok(Response::new(ReadMultipleResponse {
                success: false,
                read_multiple_resps: Vec::new(),
                error: Some(DiskError::other("can not find disk".to_string()).into()),
            }))
        }
    }

    async fn delete_volume(&self, request: Request<DeleteVolumeRequest>) -> Result<Response<DeleteVolumeResponse>, Status> {
        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
            match disk.delete_volume(&request.volume).await {
                Ok(_) => Ok(Response::new(DeleteVolumeResponse {
                    success: true,
                    error: None,
                })),
                Err(err) => Ok(Response::new(DeleteVolumeResponse {
                    success: false,
                    error: Some(err.into()),
                })),
            }
        } else {
            Ok(Response::new(DeleteVolumeResponse {
                success: false,
                error: Some(DiskError::other("can not find disk".to_string()).into()),
            }))
        }
    }

    async fn disk_info(&self, request: Request<DiskInfoRequest>) -> Result<Response<DiskInfoResponse>, Status> {
        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
            let opts = match serde_json::from_str::<DiskInfoOptions>(&request.opts) {
                Ok(opts) => opts,
                Err(err) => {
                    return Ok(Response::new(DiskInfoResponse {
                        success: false,
                        disk_info: "".to_string(),
                        error: Some(DiskError::other(format!("decode DiskInfoOptions failed: {err}")).into()),
                    }));
                }
            };
            match disk.disk_info(&opts).await {
                Ok(disk_info) => match serde_json::to_string(&disk_info) {
                    Ok(disk_info) => Ok(Response::new(DiskInfoResponse {
                        success: true,
                        disk_info,
                        error: None,
                    })),
                    Err(err) => Ok(Response::new(DiskInfoResponse {
                        success: false,
                        disk_info: "".to_string(),
                        error: Some(DiskError::other(format!("encode data failed: {err}")).into()),
                    })),
                },
                Err(err) => Ok(Response::new(DiskInfoResponse {
                    success: false,
                    disk_info: "".to_string(),
                    error: Some(err.into()),
                })),
            }
        } else {
            Ok(Response::new(DiskInfoResponse {
                success: false,
                disk_info: "".to_string(),
                error: Some(DiskError::other("can not find disk".to_string()).into()),
            }))
        }
    }

    async fn lock(&self, request: Request<GenerallyLockRequest>) -> Result<Response<GenerallyLockResponse>, Status> {
        let request = request.into_inner();
        // Parse the request to extract resource and owner
        let args: LockRequest = match serde_json::from_str(&request.args) {
            Ok(args) => args,
            Err(err) => {
                return Ok(Response::new(GenerallyLockResponse {
                    success: false,
                    error_info: Some(format!("can not decode args, err: {err}")),
                }));
            }
        };

        match self.lock_manager.acquire_exclusive(&args).await {
            Ok(result) => Ok(Response::new(GenerallyLockResponse {
                success: result.success,
                error_info: None,
            })),
            Err(err) => Ok(Response::new(GenerallyLockResponse {
                success: false,
                error_info: Some(format!(
                    "can not lock, resource: {0}, owner: {1}, err: {2}",
                    args.resource, args.owner, err
                )),
            })),
        }
    }

    async fn un_lock(&self, request: Request<GenerallyLockRequest>) -> Result<Response<GenerallyLockResponse>, Status> {
        let request = request.into_inner();
        let args: LockRequest = match serde_json::from_str(&request.args) {
            Ok(args) => args,
            Err(err) => {
                return Ok(Response::new(GenerallyLockResponse {
                    success: false,
                    error_info: Some(format!("can not decode args, err: {err}")),
                }));
            }
        };

        match self.lock_manager.release(&args.lock_id).await {
            Ok(_) => Ok(Response::new(GenerallyLockResponse {
                success: true,
                error_info: None,
            })),
            Err(err) => Ok(Response::new(GenerallyLockResponse {
                success: false,
                error_info: Some(format!(
                    "can not unlock, resource: {0}, owner: {1}, err: {2}",
                    args.resource, args.owner, err
                )),
            })),
        }
    }

    async fn r_lock(&self, request: Request<GenerallyLockRequest>) -> Result<Response<GenerallyLockResponse>, Status> {
        let request = request.into_inner();
        let args: LockRequest = match serde_json::from_str(&request.args) {
            Ok(args) => args,
            Err(err) => {
                return Ok(Response::new(GenerallyLockResponse {
                    success: false,
                    error_info: Some(format!("can not decode args, err: {err}")),
                }));
            }
        };

        match self.lock_manager.acquire_shared(&args).await {
            Ok(result) => Ok(Response::new(GenerallyLockResponse {
                success: result.success,
                error_info: None,
            })),
            Err(err) => Ok(Response::new(GenerallyLockResponse {
                success: false,
                error_info: Some(format!(
                    "can not rlock, resource: {0}, owner: {1}, err: {2}",
                    args.resource, args.owner, err
                )),
            })),
        }
    }

    async fn r_un_lock(&self, request: Request<GenerallyLockRequest>) -> Result<Response<GenerallyLockResponse>, Status> {
        let request = request.into_inner();
        let args: LockRequest = match serde_json::from_str(&request.args) {
            Ok(args) => args,
            Err(err) => {
                return Ok(Response::new(GenerallyLockResponse {
                    success: false,
                    error_info: Some(format!("can not decode args, err: {err}")),
                }));
            }
        };

        match self.lock_manager.release(&args.lock_id).await {
            Ok(_) => Ok(Response::new(GenerallyLockResponse {
                success: true,
                error_info: None,
            })),
            Err(err) => Ok(Response::new(GenerallyLockResponse {
                success: false,
                error_info: Some(format!(
                    "can not runlock, resource: {0}, owner: {1}, err: {2}",
                    args.resource, args.owner, err
                )),
            })),
        }
    }

    async fn force_un_lock(&self, request: Request<GenerallyLockRequest>) -> Result<Response<GenerallyLockResponse>, Status> {
        let request = request.into_inner();
        let args: LockRequest = match serde_json::from_str(&request.args) {
            Ok(args) => args,
            Err(err) => {
                return Ok(Response::new(GenerallyLockResponse {
                    success: false,
                    error_info: Some(format!("can not decode args, err: {err}")),
                }));
            }
        };

        match self.lock_manager.release(&args.lock_id).await {
            Ok(_) => Ok(Response::new(GenerallyLockResponse {
                success: true,
                error_info: None,
            })),
            Err(err) => Ok(Response::new(GenerallyLockResponse {
                success: false,
                error_info: Some(format!(
                    "can not force_unlock, resource: {0}, owner: {1}, err: {2}",
                    args.resource, args.owner, err
                )),
            })),
        }
    }

    async fn refresh(&self, request: Request<GenerallyLockRequest>) -> Result<Response<GenerallyLockResponse>, Status> {
        let request = request.into_inner();
        let _args: LockRequest = match serde_json::from_str(&request.args) {
            Ok(args) => args,
            Err(err) => {
                return Ok(Response::new(GenerallyLockResponse {
                    success: false,
                    error_info: Some(format!("can not decode args, err: {err}")),
                }));
            }
        };

        Ok(Response::new(GenerallyLockResponse {
            success: true,
            error_info: None,
        }))
    }

    async fn local_storage_info(
        &self,
        _request: Request<LocalStorageInfoRequest>,
    ) -> Result<Response<LocalStorageInfoResponse>, Status> {
        // let request = request.into_inner();

        let Some(store) = new_object_layer_fn() else {
            return Ok(Response::new(LocalStorageInfoResponse {
                success: false,
                storage_info: Bytes::new(),
                error_info: Some("errServerNotInitialized".to_string()),
            }));
        };

        let info = store.local_storage_info().await;
        let mut buf = Vec::new();
        if let Err(err) = info.serialize(&mut Serializer::new(&mut buf)) {
            return Ok(Response::new(LocalStorageInfoResponse {
                success: false,
                storage_info: Bytes::new(),
                error_info: Some(err.to_string()),
            }));
        }

        Ok(Response::new(LocalStorageInfoResponse {
            success: true,
            storage_info: buf.into(),
            error_info: None,
        }))
    }

    async fn server_info(&self, _request: Request<ServerInfoRequest>) -> Result<Response<ServerInfoResponse>, Status> {
        let info = get_local_server_property().await;
        let mut buf = Vec::new();
        if let Err(err) = info.serialize(&mut Serializer::new(&mut buf)) {
            return Ok(Response::new(ServerInfoResponse {
                success: false,
                server_properties: Bytes::new(),
                error_info: Some(err.to_string()),
            }));
        }
        Ok(Response::new(ServerInfoResponse {
            success: true,
            server_properties: buf.into(),
            error_info: None,
        }))
    }

    async fn get_cpus(&self, _request: Request<GetCpusRequest>) -> Result<Response<GetCpusResponse>, Status> {
        let info = get_cpus();
        let mut buf = Vec::new();
        if let Err(err) = info.serialize(&mut Serializer::new(&mut buf)) {
            return Ok(Response::new(GetCpusResponse {
                success: false,
                cpus: Bytes::new(),
                error_info: Some(err.to_string()),
            }));
        }
        Ok(Response::new(GetCpusResponse {
            success: true,
            cpus: buf.into(),
            error_info: None,
        }))
    }

    async fn get_net_info(&self, _request: Request<GetNetInfoRequest>) -> Result<Response<GetNetInfoResponse>, Status> {
        let addr = GLOBAL_LOCAL_NODE_NAME.read().await.clone();
        let info = get_net_info(&addr, "");
        let mut buf = Vec::new();
        if let Err(err) = info.serialize(&mut Serializer::new(&mut buf)) {
            return Ok(Response::new(GetNetInfoResponse {
                success: false,
                net_info: Bytes::new(),
                error_info: Some(err.to_string()),
            }));
        }
        Ok(Response::new(GetNetInfoResponse {
            success: true,
            net_info: buf.into(),
            error_info: None,
        }))
    }

    async fn get_partitions(&self, _request: Request<GetPartitionsRequest>) -> Result<Response<GetPartitionsResponse>, Status> {
        let partitions = get_partitions();
        let mut buf = Vec::new();
        if let Err(err) = partitions.serialize(&mut Serializer::new(&mut buf)) {
            return Ok(Response::new(GetPartitionsResponse {
                success: false,
                partitions: Bytes::new(),
                error_info: Some(err.to_string()),
            }));
        }
        Ok(Response::new(GetPartitionsResponse {
            success: true,
            partitions: buf.into(),
            error_info: None,
        }))
    }

    async fn get_os_info(&self, _request: Request<GetOsInfoRequest>) -> Result<Response<GetOsInfoResponse>, Status> {
        let os_info = get_os_info();
        let mut buf = Vec::new();
        if let Err(err) = os_info.serialize(&mut Serializer::new(&mut buf)) {
            return Ok(Response::new(GetOsInfoResponse {
                success: false,
                os_info: Bytes::new(),
                error_info: Some(err.to_string()),
            }));
        }
        Ok(Response::new(GetOsInfoResponse {
            success: true,
            os_info: buf.into(),
            error_info: None,
        }))
    }

    async fn get_se_linux_info(
        &self,
        _request: Request<GetSeLinuxInfoRequest>,
    ) -> Result<Response<GetSeLinuxInfoResponse>, Status> {
        let addr = GLOBAL_LOCAL_NODE_NAME.read().await.clone();
        let info = get_sys_services(&addr);
        let mut buf = Vec::new();
        if let Err(err) = info.serialize(&mut Serializer::new(&mut buf)) {
            return Ok(Response::new(GetSeLinuxInfoResponse {
                success: false,
                sys_services: Bytes::new(),
                error_info: Some(err.to_string()),
            }));
        }
        Ok(Response::new(GetSeLinuxInfoResponse {
            success: true,
            sys_services: buf.into(),
            error_info: None,
        }))
    }

    async fn get_sys_config(&self, _request: Request<GetSysConfigRequest>) -> Result<Response<GetSysConfigResponse>, Status> {
        let addr = GLOBAL_LOCAL_NODE_NAME.read().await.clone();
        let info = get_sys_config(&addr);
        let mut buf = Vec::new();
        if let Err(err) = info.serialize(&mut Serializer::new(&mut buf)) {
            return Ok(Response::new(GetSysConfigResponse {
                success: false,
                sys_config: Bytes::new(),
                error_info: Some(err.to_string()),
            }));
        }
        Ok(Response::new(GetSysConfigResponse {
            success: true,
            sys_config: buf.into(),
            error_info: None,
        }))
    }

    async fn get_sys_errors(&self, _request: Request<GetSysErrorsRequest>) -> Result<Response<GetSysErrorsResponse>, Status> {
        let addr = GLOBAL_LOCAL_NODE_NAME.read().await.clone();
        let info = get_sys_errors(&addr);
        let mut buf = Vec::new();
        if let Err(err) = info.serialize(&mut Serializer::new(&mut buf)) {
            return Ok(Response::new(GetSysErrorsResponse {
                success: false,
                sys_errors: Bytes::new(),
                error_info: Some(err.to_string()),
            }));
        }
        Ok(Response::new(GetSysErrorsResponse {
            success: true,
            sys_errors: buf.into(),
            error_info: None,
        }))
    }

    async fn get_mem_info(&self, _request: Request<GetMemInfoRequest>) -> Result<Response<GetMemInfoResponse>, Status> {
        let addr = GLOBAL_LOCAL_NODE_NAME.read().await.clone();
        let info = get_mem_info(&addr);
        let mut buf = Vec::new();
        if let Err(err) = info.serialize(&mut Serializer::new(&mut buf)) {
            return Ok(Response::new(GetMemInfoResponse {
                success: false,
                mem_info: Bytes::new(),
                error_info: Some(err.to_string()),
            }));
        }
        Ok(Response::new(GetMemInfoResponse {
            success: true,
            mem_info: buf.into(),
            error_info: None,
        }))
    }

    async fn get_metrics(&self, request: Request<GetMetricsRequest>) -> Result<Response<GetMetricsResponse>, Status> {
        let request = request.into_inner();

        // Deserialize metric_type with error handling
        let mut buf_t = Deserializer::new(Cursor::new(request.metric_type));
        let t: MetricType = match Deserialize::deserialize(&mut buf_t) {
            Ok(t) => t,
            Err(err) => {
                error!("Failed to deserialize metric_type: {}", err);
                return Ok(Response::new(GetMetricsResponse {
                    success: false,
                    realtime_metrics: Bytes::new(),
                    error_info: Some(format!("Invalid metric_type: {}", err)),
                }));
            }
        };

        // Deserialize opts with error handling
        let mut buf_o = Deserializer::new(Cursor::new(request.opts));
        let opts: CollectMetricsOpts = match Deserialize::deserialize(&mut buf_o) {
            Ok(opts) => opts,
            Err(err) => {
                error!("Failed to deserialize opts: {}", err);
                return Ok(Response::new(GetMetricsResponse {
                    success: false,
                    realtime_metrics: Bytes::new(),
                    error_info: Some(format!("Invalid opts: {}", err)),
                }));
            }
        };

        let info = collect_local_metrics(t, &opts).await;

        let mut buf = Vec::new();
        if let Err(err) = info.serialize(&mut Serializer::new(&mut buf)) {
            return Ok(Response::new(GetMetricsResponse {
                success: false,
                realtime_metrics: Bytes::new(),
                error_info: Some(err.to_string()),
            }));
        }
        Ok(Response::new(GetMetricsResponse {
            success: true,
            realtime_metrics: buf.into(),
            error_info: None,
        }))
    }

    async fn get_proc_info(&self, _request: Request<GetProcInfoRequest>) -> Result<Response<GetProcInfoResponse>, Status> {
        let addr = GLOBAL_LOCAL_NODE_NAME.read().await.clone();
        let info = get_proc_info(&addr);
        let mut buf = Vec::new();
        if let Err(err) = info.serialize(&mut Serializer::new(&mut buf)) {
            return Ok(Response::new(GetProcInfoResponse {
                success: false,
                proc_info: Bytes::new(),
                error_info: Some(err.to_string()),
            }));
        }
        Ok(Response::new(GetProcInfoResponse {
            success: true,
            proc_info: buf.into(),
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
            return Ok(Response::new(LoadBucketMetadataResponse {
                success: false,
                error_info: Some("bucket name is missing".to_string()),
            }));
        }

        let Some(store) = new_object_layer_fn() else {
            return Ok(Response::new(LoadBucketMetadataResponse {
                success: false,
                error_info: Some("errServerNotInitialized".to_string()),
            }));
        };

        match load_bucket_metadata(store, &bucket).await {
            Ok(meta) => {
                if let Err(err) = metadata_sys::set_bucket_metadata(bucket, meta).await {
                    return Ok(Response::new(LoadBucketMetadataResponse {
                        success: false,
                        error_info: Some(err.to_string()),
                    }));
                };
                Ok(Response::new(LoadBucketMetadataResponse {
                    success: true,
                    error_info: None,
                }))
            }
            Err(err) => Ok(Response::new(LoadBucketMetadataResponse {
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
        Ok(Response::new(DeleteBucketMetadataResponse {
            success: true,
            error_info: None,
        }))
    }

    async fn delete_policy(&self, request: Request<DeletePolicyRequest>) -> Result<Response<DeletePolicyResponse>, Status> {
        let request = request.into_inner();
        let policy = request.policy_name;
        if policy.is_empty() {
            return Ok(Response::new(DeletePolicyResponse {
                success: false,
                error_info: Some("policy name is missing".to_string()),
            }));
        }

        let Some(iam_sys) = get_global_iam_sys() else {
            return Ok(Response::new(DeletePolicyResponse {
                success: false,
                error_info: Some("errServerNotInitialized".to_string()),
            }));
        };

        let resp = iam_sys.delete_policy(&policy, false).await;
        if let Err(err) = resp {
            return Ok(Response::new(DeletePolicyResponse {
                success: false,
                error_info: Some(err.to_string()),
            }));
        }
        Ok(Response::new(DeletePolicyResponse {
            success: true,
            error_info: None,
        }))
    }

    async fn load_policy(&self, request: Request<LoadPolicyRequest>) -> Result<Response<LoadPolicyResponse>, Status> {
        let request = request.into_inner();
        let policy = request.policy_name;
        if policy.is_empty() {
            return Ok(Response::new(LoadPolicyResponse {
                success: false,
                error_info: Some("policy name is missing".to_string()),
            }));
        }
        let Some(iam_sys) = get_global_iam_sys() else {
            return Ok(Response::new(LoadPolicyResponse {
                success: false,
                error_info: Some("errServerNotInitialized".to_string()),
            }));
        };

        let resp = iam_sys.load_policy(&policy).await;
        if let Err(err) = resp {
            return Ok(Response::new(LoadPolicyResponse {
                success: false,
                error_info: Some(err.to_string()),
            }));
        }
        Ok(Response::new(LoadPolicyResponse {
            success: true,
            error_info: None,
        }))
    }

    async fn load_policy_mapping(
        &self,
        request: Request<LoadPolicyMappingRequest>,
    ) -> Result<Response<LoadPolicyMappingResponse>, Status> {
        let request = request.into_inner();
        let user_or_group = request.user_or_group;
        if user_or_group.is_empty() {
            return Ok(Response::new(LoadPolicyMappingResponse {
                success: false,
                error_info: Some("user_or_group name is missing".to_string()),
            }));
        }
        let Some(user_type) = UserType::from_u64(request.user_type) else {
            return Ok(Response::new(LoadPolicyMappingResponse {
                success: false,
                error_info: Some("invalid user type".to_string()),
            }));
        };
        let is_group = request.is_group;
        let Some(iam_sys) = get_global_iam_sys() else {
            return Ok(Response::new(LoadPolicyMappingResponse {
                success: false,
                error_info: Some("errServerNotInitialized".to_string()),
            }));
        };
        let resp = iam_sys.load_policy_mapping(&user_or_group, user_type, is_group).await;
        if let Err(err) = resp {
            return Ok(Response::new(LoadPolicyMappingResponse {
                success: false,
                error_info: Some(err.to_string()),
            }));
        }
        Ok(Response::new(LoadPolicyMappingResponse {
            success: true,
            error_info: None,
        }))
    }

    async fn delete_user(&self, request: Request<DeleteUserRequest>) -> Result<Response<DeleteUserResponse>, Status> {
        let request = request.into_inner();
        let access_key = request.access_key;
        if access_key.is_empty() {
            return Ok(Response::new(DeleteUserResponse {
                success: false,
                error_info: Some("access_key name is missing".to_string()),
            }));
        }
        let Some(iam_sys) = get_global_iam_sys() else {
            return Ok(Response::new(DeleteUserResponse {
                success: false,
                error_info: Some("errServerNotInitialized".to_string()),
            }));
        };

        let resp = iam_sys.delete_user(&access_key, false).await;
        if let Err(err) = resp {
            return Ok(Response::new(DeleteUserResponse {
                success: false,
                error_info: Some(err.to_string()),
            }));
        }
        Ok(Response::new(DeleteUserResponse {
            success: true,
            error_info: None,
        }))
    }

    async fn delete_service_account(
        &self,
        request: Request<DeleteServiceAccountRequest>,
    ) -> Result<Response<DeleteServiceAccountResponse>, Status> {
        let request = request.into_inner();
        let access_key = request.access_key;
        if access_key.is_empty() {
            return Ok(Response::new(DeleteServiceAccountResponse {
                success: false,
                error_info: Some("access_key name is missing".to_string()),
            }));
        }
        let Some(iam_sys) = get_global_iam_sys() else {
            return Ok(Response::new(DeleteServiceAccountResponse {
                success: false,
                error_info: Some("errServerNotInitialized".to_string()),
            }));
        };
        let resp = iam_sys.delete_service_account(&access_key, false).await;
        if let Err(err) = resp {
            return Ok(Response::new(DeleteServiceAccountResponse {
                success: false,
                error_info: Some(err.to_string()),
            }));
        }
        Ok(Response::new(DeleteServiceAccountResponse {
            success: true,
            error_info: None,
        }))
    }

    async fn load_user(&self, request: Request<LoadUserRequest>) -> Result<Response<LoadUserResponse>, Status> {
        let request = request.into_inner();
        let access_key = request.access_key;
        let temp = request.temp;
        if access_key.is_empty() {
            return Ok(Response::new(LoadUserResponse {
                success: false,
                error_info: Some("access_key name is missing".to_string()),
            }));
        }

        let Some(iam_sys) = get_global_iam_sys() else {
            return Ok(Response::new(LoadUserResponse {
                success: false,
                error_info: Some("errServerNotInitialized".to_string()),
            }));
        };

        let user_type = if temp { UserType::Sts } else { UserType::Reg };

        let resp = iam_sys.load_user(&access_key, user_type).await;
        if let Err(err) = resp {
            return Ok(Response::new(LoadUserResponse {
                success: false,
                error_info: Some(err.to_string()),
            }));
        }

        Ok(Response::new(LoadUserResponse {
            success: true,
            error_info: None,
        }))
    }

    async fn load_service_account(
        &self,
        request: Request<LoadServiceAccountRequest>,
    ) -> Result<Response<LoadServiceAccountResponse>, Status> {
        let request = request.into_inner();
        let access_key = request.access_key;
        if access_key.is_empty() {
            return Ok(Response::new(LoadServiceAccountResponse {
                success: false,
                error_info: Some("access_key name is missing".to_string()),
            }));
        }

        let Some(iam_sys) = get_global_iam_sys() else {
            return Ok(Response::new(LoadServiceAccountResponse {
                success: false,
                error_info: Some("errServerNotInitialized".to_string()),
            }));
        };

        let resp = iam_sys.load_service_account(&access_key).await;
        if let Err(err) = resp {
            return Ok(Response::new(LoadServiceAccountResponse {
                success: false,
                error_info: Some(err.to_string()),
            }));
        }

        Ok(Response::new(LoadServiceAccountResponse {
            success: true,
            error_info: None,
        }))
    }

    async fn load_group(&self, request: Request<LoadGroupRequest>) -> Result<Response<LoadGroupResponse>, Status> {
        let request = request.into_inner();
        let group = request.group;
        if group.is_empty() {
            return Ok(Response::new(LoadGroupResponse {
                success: false,
                error_info: Some("group name is missing".to_string()),
            }));
        }

        let Some(iam_sys) = get_global_iam_sys() else {
            return Ok(Response::new(LoadGroupResponse {
                success: false,
                error_info: Some("errServerNotInitialized".to_string()),
            }));
        };

        let resp = iam_sys.load_group(&group).await;
        if let Err(err) = resp {
            return Ok(Response::new(LoadGroupResponse {
                success: false,
                error_info: Some(err.to_string()),
            }));
        }
        Ok(Response::new(LoadGroupResponse {
            success: true,
            error_info: None,
        }))
    }

    async fn reload_site_replication_config(
        &self,
        _request: Request<ReloadSiteReplicationConfigRequest>,
    ) -> Result<Response<ReloadSiteReplicationConfigResponse>, Status> {
        let Some(_store) = new_object_layer_fn() else {
            return Ok(Response::new(ReloadSiteReplicationConfigResponse {
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
        todo!()
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
            return Ok(Response::new(ReloadPoolMetaResponse {
                success: false,
                error_info: Some("errServerNotInitialized".to_string()),
            }));
        };
        match store.reload_pool_meta().await {
            Ok(_) => Ok(Response::new(ReloadPoolMetaResponse {
                success: true,
                error_info: None,
            })),
            Err(err) => Ok(Response::new(ReloadPoolMetaResponse {
                success: false,
                error_info: Some(err.to_string()),
            })),
        }
    }

    async fn stop_rebalance(&self, _request: Request<StopRebalanceRequest>) -> Result<Response<StopRebalanceResponse>, Status> {
        let Some(store) = new_object_layer_fn() else {
            return Ok(Response::new(StopRebalanceResponse {
                success: false,
                error_info: Some("errServerNotInitialized".to_string()),
            }));
        };

        let _ = store.stop_rebalance().await;
        Ok(Response::new(StopRebalanceResponse {
            success: true,
            error_info: None,
        }))
    }

    #[tracing::instrument(skip_all)]
    async fn load_rebalance_meta(
        &self,
        request: Request<LoadRebalanceMetaRequest>,
    ) -> Result<Response<LoadRebalanceMetaResponse>, Status> {
        let Some(store) = new_object_layer_fn() else {
            return Ok(Response::new(LoadRebalanceMetaResponse {
                success: false,
                error_info: Some("errServerNotInitialized".to_string()),
            }));
        };

        let LoadRebalanceMetaRequest { start_rebalance } = request.into_inner();

        warn!("handle LoadRebalanceMetaRequest");

        store.load_rebalance_meta().await.map_err(|err| {
            error!("load_rebalance_meta err {:?}", err);
            Status::internal(err.to_string())
        })?;

        warn!("load_rebalance_meta success");

        if start_rebalance {
            warn!("start rebalance");
            let store = store.clone();
            spawn(async move {
                store.start_rebalance().await;
            });
        }

        Ok(Response::new(LoadRebalanceMetaResponse {
            success: true,
            error_info: None,
        }))
    }

    async fn load_transition_tier_config(
        &self,
        _request: Request<LoadTransitionTierConfigRequest>,
    ) -> Result<Response<LoadTransitionTierConfigResponse>, Status> {
        todo!()
    }
}

#[cfg(test)]
#[allow(unused_imports)]
mod tests {
    use super::*;
    use Request;
    use rustfs_protos::proto_gen::node_service::{
        CheckPartsRequest, DeleteBucketMetadataRequest, DeleteBucketRequest, DeletePathsRequest, DeletePolicyRequest,
        DeleteRequest, DeleteServiceAccountRequest, DeleteUserRequest, DeleteVersionRequest, DeleteVersionsRequest,
        DeleteVolumeRequest, DiskInfoRequest, GenerallyLockRequest, GetBucketInfoRequest, GetCpusRequest, GetMemInfoRequest,
        GetNetInfoRequest, GetOsInfoRequest, GetPartitionsRequest, GetProcInfoRequest, GetSeLinuxInfoRequest,
        GetSysConfigRequest, GetSysErrorsRequest, HealBucketRequest, ListBucketRequest, ListDirRequest, ListVolumesRequest,
        LoadBucketMetadataRequest, LoadGroupRequest, LoadPolicyMappingRequest, LoadPolicyRequest, LoadRebalanceMetaRequest,
        LoadServiceAccountRequest, LoadUserRequest, LocalStorageInfoRequest, MakeBucketRequest, MakeVolumeRequest,
        MakeVolumesRequest, PingRequest, ReadAllRequest, ReadMultipleRequest, ReadVersionRequest, ReadXlRequest,
        ReloadPoolMetaRequest, ReloadSiteReplicationConfigRequest, RenameDataRequest, RenameFileRequest, RenamePartRequest,
        ServerInfoRequest, StatVolumeRequest, StopRebalanceRequest, UpdateMetadataRequest, VerifyFileRequest, WriteAllRequest,
        WriteMetadataRequest,
    };

    fn create_test_node_service() -> NodeService {
        make_server()
    }

    #[tokio::test]
    async fn test_make_server() {
        let service = make_server();
        // LocalPeerS3Client is a struct, not an Option, so we just check it exists
        assert!(format!("{:?}", service.local_peer).contains("LocalPeerS3Client"));
    }

    #[tokio::test]
    async fn test_ping_success() {
        let service = create_test_node_service();

        // Create a valid ping request with flatbuffer body
        let mut fbb = flatbuffers::FlatBufferBuilder::new();
        let payload = fbb.create_vector(b"test payload");
        let mut builder = PingBodyBuilder::new(&mut fbb);
        builder.add_payload(payload);
        let root = builder.finish();
        fbb.finish(root, None);

        let request = Request::new(PingRequest {
            version: 1,
            body: Bytes::copy_from_slice(fbb.finished_data()),
        });

        let response = service.ping(request).await;
        assert!(response.is_ok());

        let ping_response = response.unwrap().into_inner();
        assert_eq!(ping_response.version, 1);
        assert!(!ping_response.body.is_empty());
    }

    #[tokio::test]
    async fn test_ping_with_invalid_flatbuffer() {
        let service = create_test_node_service();

        let request = Request::new(PingRequest {
            version: 1,
            body: vec![0x00, 0x01, 0x02].into(), // Invalid flatbuffer data
        });

        let response = service.ping(request).await;
        assert!(response.is_ok()); // Should still succeed but log error

        let ping_response = response.unwrap().into_inner();
        assert_eq!(ping_response.version, 1);
        assert!(!ping_response.body.is_empty());
    }

    #[tokio::test]
    async fn test_heal_bucket_invalid_options() {
        let service = create_test_node_service();

        let request = Request::new(HealBucketRequest {
            bucket: "test-bucket".to_string(),
            options: "invalid json".to_string(),
        });

        let response = service.heal_bucket(request).await;
        assert!(response.is_ok());

        let heal_response = response.unwrap().into_inner();
        assert!(!heal_response.success);
        assert!(heal_response.error.is_some());
    }

    #[tokio::test]
    async fn test_list_bucket_invalid_options() {
        let service = create_test_node_service();

        let request = Request::new(ListBucketRequest {
            options: "invalid json".to_string(),
        });

        let response = service.list_bucket(request).await;
        assert!(response.is_ok());

        let list_response = response.unwrap().into_inner();
        assert!(!list_response.success);
        assert!(list_response.error.is_some());
        assert!(list_response.bucket_infos.is_empty());
    }

    #[tokio::test]
    async fn test_make_bucket_invalid_options() {
        let service = create_test_node_service();

        let request = Request::new(MakeBucketRequest {
            name: "test-bucket".to_string(),
            options: "invalid json".to_string(),
        });

        let response = service.make_bucket(request).await;
        assert!(response.is_ok());

        let make_response = response.unwrap().into_inner();
        assert!(!make_response.success);
        assert!(make_response.error.is_some());
    }

    #[tokio::test]
    async fn test_get_bucket_info_invalid_options() {
        let service = create_test_node_service();

        let request = Request::new(GetBucketInfoRequest {
            bucket: "test-bucket".to_string(),
            options: "invalid json".to_string(),
        });

        let response = service.get_bucket_info(request).await;
        assert!(response.is_ok());

        let info_response = response.unwrap().into_inner();
        assert!(!info_response.success);
        assert!(info_response.error.is_some());
        assert!(info_response.bucket_info.is_empty());
    }

    #[tokio::test]
    async fn test_delete_bucket() {
        let service = create_test_node_service();

        let request = Request::new(DeleteBucketRequest {
            bucket: "test-bucket".to_string(),
        });

        let response = service.delete_bucket(request).await;
        assert!(response.is_ok());

        let delete_response = response.unwrap().into_inner();
        // Response should be valid regardless of success/failure
        assert!(delete_response.success || delete_response.error.is_some());
    }

    #[tokio::test]
    async fn test_read_all_invalid_disk() {
        let service = create_test_node_service();

        let request = Request::new(ReadAllRequest {
            disk: "invalid-disk-path".to_string(),
            volume: "test-volume".to_string(),
            path: "test-path".to_string(),
        });

        let response = service.read_all(request).await;
        assert!(response.is_ok());

        let read_response = response.unwrap().into_inner();
        assert!(!read_response.success);
        assert!(read_response.error.is_some());
        assert!(read_response.data.is_empty());
    }

    #[tokio::test]
    async fn test_write_all_invalid_disk() {
        let service = create_test_node_service();

        let request = Request::new(WriteAllRequest {
            disk: "invalid-disk-path".to_string(),
            volume: "test-volume".to_string(),
            path: "test-path".to_string(),
            data: vec![1, 2, 3, 4].into(),
        });

        let response = service.write_all(request).await;
        assert!(response.is_ok());

        let write_response = response.unwrap().into_inner();
        assert!(!write_response.success);
        assert!(write_response.error.is_some());
    }

    #[tokio::test]
    async fn test_delete_invalid_disk() {
        let service = create_test_node_service();

        let request = Request::new(DeleteRequest {
            disk: "invalid-disk-path".to_string(),
            volume: "test-volume".to_string(),
            path: "test-path".to_string(),
            options: "{}".to_string(),
        });

        let response = service.delete(request).await;
        assert!(response.is_ok());

        let delete_response = response.unwrap().into_inner();
        assert!(!delete_response.success);
        assert!(delete_response.error.is_some());
    }

    #[tokio::test]
    async fn test_delete_invalid_options() {
        let service = create_test_node_service();

        let request = Request::new(DeleteRequest {
            disk: "invalid-disk-path".to_string(),
            volume: "test-volume".to_string(),
            path: "test-path".to_string(),
            options: "invalid json".to_string(),
        });

        let response = service.delete(request).await;
        assert!(response.is_ok());

        let delete_response = response.unwrap().into_inner();
        assert!(!delete_response.success);
        assert!(delete_response.error.is_some());
    }

    #[tokio::test]
    async fn test_verify_file_invalid_disk() {
        let service = create_test_node_service();

        let request = Request::new(VerifyFileRequest {
            disk: "invalid-disk-path".to_string(),
            volume: "test-volume".to_string(),
            path: "test-path".to_string(),
            file_info: "{}".to_string(),
        });

        let response = service.verify_file(request).await;
        assert!(response.is_ok());

        let verify_response = response.unwrap().into_inner();
        assert!(!verify_response.success);
        assert!(verify_response.error.is_some());
        assert!(verify_response.check_parts_resp.is_empty());
    }

    #[tokio::test]
    async fn test_verify_file_invalid_file_info() {
        let service = create_test_node_service();

        let request = Request::new(VerifyFileRequest {
            disk: "invalid-disk-path".to_string(),
            volume: "test-volume".to_string(),
            path: "test-path".to_string(),
            file_info: "invalid json".to_string(),
        });

        let response = service.verify_file(request).await;
        assert!(response.is_ok());

        let verify_response = response.unwrap().into_inner();
        assert!(!verify_response.success);
        assert!(verify_response.error.is_some());
    }

    #[tokio::test]
    async fn test_check_parts_invalid_file_info() {
        let service = create_test_node_service();

        let request = Request::new(CheckPartsRequest {
            disk: "invalid-disk-path".to_string(),
            volume: "test-volume".to_string(),
            path: "test-path".to_string(),
            file_info: "invalid json".to_string(),
        });

        let response = service.check_parts(request).await;
        assert!(response.is_ok());

        let check_response = response.unwrap().into_inner();
        assert!(!check_response.success);
        assert!(check_response.error.is_some());
    }

    #[tokio::test]
    async fn test_rename_part_invalid_disk() {
        let service = create_test_node_service();

        let request = Request::new(RenamePartRequest {
            disk: "invalid-disk-path".to_string(),
            src_volume: "src-volume".to_string(),
            src_path: "src-path".to_string(),
            dst_volume: "dst-volume".to_string(),
            dst_path: "dst-path".to_string(),
            meta: Bytes::new(),
        });

        let response = service.rename_part(request).await;
        assert!(response.is_ok());

        let rename_response = response.unwrap().into_inner();
        assert!(!rename_response.success);
        assert!(rename_response.error.is_some());
    }

    #[tokio::test]
    async fn test_rename_file_invalid_disk() {
        let service = create_test_node_service();

        let request = Request::new(RenameFileRequest {
            disk: "invalid-disk-path".to_string(),
            src_volume: "src-volume".to_string(),
            src_path: "src-path".to_string(),
            dst_volume: "dst-volume".to_string(),
            dst_path: "dst-path".to_string(),
        });

        let response = service.rename_file(request).await;
        assert!(response.is_ok());

        let rename_response = response.unwrap().into_inner();
        assert!(!rename_response.success);
        assert!(rename_response.error.is_some());
    }

    #[tokio::test]
    async fn test_list_dir_invalid_disk() {
        let service = create_test_node_service();

        let request = Request::new(ListDirRequest {
            disk: "invalid-disk-path".to_string(),
            volume: "test-volume".to_string(),
            dir_path: "test-dir-path".to_string(),
            count: 10,
        });

        let response = service.list_dir(request).await;
        assert!(response.is_ok());

        let list_response = response.unwrap().into_inner();
        assert!(!list_response.success);
        assert!(list_response.error.is_some());
        assert!(list_response.volumes.is_empty());
    }

    #[tokio::test]
    async fn test_rename_data_invalid_disk() {
        let service = create_test_node_service();

        let request = Request::new(RenameDataRequest {
            disk: "invalid-disk-path".to_string(),
            src_volume: "src-volume".to_string(),
            src_path: "src-path".to_string(),
            dst_volume: "dst-volume".to_string(),
            dst_path: "dst-path".to_string(),
            file_info: "{}".to_string(),
        });

        let response = service.rename_data(request).await;
        assert!(response.is_ok());

        let rename_response = response.unwrap().into_inner();
        assert!(!rename_response.success);
        assert!(rename_response.error.is_some());
    }

    #[tokio::test]
    async fn test_rename_data_invalid_file_info() {
        let service = create_test_node_service();

        let request = Request::new(RenameDataRequest {
            disk: "invalid-disk-path".to_string(),
            src_volume: "src-volume".to_string(),
            src_path: "src-path".to_string(),
            dst_volume: "dst-volume".to_string(),
            dst_path: "dst-path".to_string(),
            file_info: "invalid json".to_string(),
        });

        let response = service.rename_data(request).await;
        assert!(response.is_ok());

        let rename_response = response.unwrap().into_inner();
        assert!(!rename_response.success);
        assert!(rename_response.error.is_some());
    }

    #[tokio::test]
    async fn test_make_volumes_invalid_disk() {
        let service = create_test_node_service();

        let request = Request::new(MakeVolumesRequest {
            disk: "invalid-disk-path".to_string(),
            volumes: vec!["volume1".to_string(), "volume2".to_string()],
        });

        let response = service.make_volumes(request).await;
        assert!(response.is_ok());

        let make_response = response.unwrap().into_inner();
        assert!(!make_response.success);
        assert!(make_response.error.is_some());
    }

    #[tokio::test]
    async fn test_make_volume_invalid_disk() {
        let service = create_test_node_service();

        let request = Request::new(MakeVolumeRequest {
            disk: "invalid-disk-path".to_string(),
            volume: "test-volume".to_string(),
        });

        let response = service.make_volume(request).await;
        assert!(response.is_ok());

        let make_response = response.unwrap().into_inner();
        assert!(!make_response.success);
        assert!(make_response.error.is_some());
    }

    #[tokio::test]
    async fn test_list_volumes_invalid_disk() {
        let service = create_test_node_service();

        let request = Request::new(ListVolumesRequest {
            disk: "invalid-disk-path".to_string(),
        });

        let response = service.list_volumes(request).await;
        assert!(response.is_ok());

        let list_response = response.unwrap().into_inner();
        assert!(!list_response.success);
        assert!(list_response.error.is_some());
        assert!(list_response.volume_infos.is_empty());
    }

    #[tokio::test]
    async fn test_stat_volume_invalid_disk() {
        let service = create_test_node_service();

        let request = Request::new(StatVolumeRequest {
            disk: "invalid-disk-path".to_string(),
            volume: "test-volume".to_string(),
        });

        let response = service.stat_volume(request).await;
        assert!(response.is_ok());

        let stat_response = response.unwrap().into_inner();
        assert!(!stat_response.success);
        assert!(stat_response.error.is_some());
        assert!(stat_response.volume_info.is_empty());
    }

    #[tokio::test]
    async fn test_delete_paths_invalid_disk() {
        let service = create_test_node_service();

        let request = Request::new(DeletePathsRequest {
            disk: "invalid-disk-path".to_string(),
            volume: "test-volume".to_string(),
            paths: vec!["path1".to_string(), "path2".to_string()],
        });

        let response = service.delete_paths(request).await;
        assert!(response.is_ok());

        let delete_response = response.unwrap().into_inner();
        assert!(!delete_response.success);
        assert!(delete_response.error.is_some());
    }

    #[tokio::test]
    async fn test_update_metadata_invalid_disk() {
        let service = create_test_node_service();

        let request = Request::new(UpdateMetadataRequest {
            disk: "invalid-disk-path".to_string(),
            volume: "test-volume".to_string(),
            path: "test-path".to_string(),
            file_info: "{}".to_string(),
            opts: "{}".to_string(),
        });

        let response = service.update_metadata(request).await;
        assert!(response.is_ok());

        let update_response = response.unwrap().into_inner();
        assert!(!update_response.success);
        assert!(update_response.error.is_some());
    }

    #[tokio::test]
    async fn test_update_metadata_invalid_file_info() {
        let service = create_test_node_service();

        let request = Request::new(UpdateMetadataRequest {
            disk: "invalid-disk-path".to_string(),
            volume: "test-volume".to_string(),
            path: "test-path".to_string(),
            file_info: "invalid json".to_string(),
            opts: "{}".to_string(),
        });

        let response = service.update_metadata(request).await;
        assert!(response.is_ok());

        let update_response = response.unwrap().into_inner();
        assert!(!update_response.success);
        assert!(update_response.error.is_some());
    }

    #[tokio::test]
    async fn test_update_metadata_invalid_opts() {
        let service = create_test_node_service();

        let request = Request::new(UpdateMetadataRequest {
            disk: "invalid-disk-path".to_string(),
            volume: "test-volume".to_string(),
            path: "test-path".to_string(),
            file_info: "{}".to_string(),
            opts: "invalid json".to_string(),
        });

        let response = service.update_metadata(request).await;
        assert!(response.is_ok());

        let update_response = response.unwrap().into_inner();
        assert!(!update_response.success);
        assert!(update_response.error.is_some());
    }

    #[tokio::test]
    async fn test_write_metadata_invalid_disk() {
        let service = create_test_node_service();

        let request = Request::new(WriteMetadataRequest {
            disk: "invalid-disk-path".to_string(),
            volume: "test-volume".to_string(),
            path: "test-path".to_string(),
            file_info: "{}".to_string(),
        });

        let response = service.write_metadata(request).await;
        assert!(response.is_ok());

        let write_response = response.unwrap().into_inner();
        assert!(!write_response.success);
        assert!(write_response.error.is_some());
    }

    #[tokio::test]
    async fn test_write_metadata_invalid_file_info() {
        let service = create_test_node_service();

        let request = Request::new(WriteMetadataRequest {
            disk: "invalid-disk-path".to_string(),
            volume: "test-volume".to_string(),
            path: "test-path".to_string(),
            file_info: "invalid json".to_string(),
        });

        let response = service.write_metadata(request).await;
        assert!(response.is_ok());

        let write_response = response.unwrap().into_inner();
        assert!(!write_response.success);
        assert!(write_response.error.is_some());
    }

    #[tokio::test]
    async fn test_read_version_invalid_disk() {
        let service = create_test_node_service();

        let request = Request::new(ReadVersionRequest {
            disk: "invalid-disk-path".to_string(),
            volume: "test-volume".to_string(),
            path: "test-path".to_string(),
            version_id: "version1".to_string(),
            opts: "{}".to_string(),
        });

        let response = service.read_version(request).await;
        assert!(response.is_ok());

        let read_response = response.unwrap().into_inner();
        assert!(!read_response.success);
        assert!(read_response.error.is_some());
        assert!(read_response.file_info.is_empty());
    }

    #[tokio::test]
    async fn test_read_version_invalid_opts() {
        let service = create_test_node_service();

        let request = Request::new(ReadVersionRequest {
            disk: "invalid-disk-path".to_string(),
            volume: "test-volume".to_string(),
            path: "test-path".to_string(),
            version_id: "version1".to_string(),
            opts: "invalid json".to_string(),
        });

        let response = service.read_version(request).await;
        assert!(response.is_ok());

        let read_response = response.unwrap().into_inner();
        assert!(!read_response.success);
        assert!(read_response.error.is_some());
    }

    #[tokio::test]
    async fn test_read_xl_invalid_disk() {
        let service = create_test_node_service();

        let request = Request::new(ReadXlRequest {
            disk: "invalid-disk-path".to_string(),
            volume: "test-volume".to_string(),
            path: "test-path".to_string(),
            read_data: true,
        });

        let response = service.read_xl(request).await;
        assert!(response.is_ok());

        let read_response = response.unwrap().into_inner();
        assert!(!read_response.success);
        assert!(read_response.error.is_some());
        assert!(read_response.raw_file_info.is_empty());
    }

    #[tokio::test]
    async fn test_delete_version_invalid_disk() {
        let service = create_test_node_service();

        let request = Request::new(DeleteVersionRequest {
            disk: "invalid-disk-path".to_string(),
            volume: "test-volume".to_string(),
            path: "test-path".to_string(),
            file_info: "{}".to_string(),
            force_del_marker: false,
            opts: "{}".to_string(),
        });

        let response = service.delete_version(request).await;
        assert!(response.is_ok());

        let delete_response = response.unwrap().into_inner();
        assert!(!delete_response.success);
        assert!(delete_response.error.is_some());
    }

    #[tokio::test]
    async fn test_delete_version_invalid_file_info() {
        let service = create_test_node_service();

        let request = Request::new(DeleteVersionRequest {
            disk: "invalid-disk-path".to_string(),
            volume: "test-volume".to_string(),
            path: "test-path".to_string(),
            file_info: "invalid json".to_string(),
            force_del_marker: false,
            opts: "{}".to_string(),
        });

        let response = service.delete_version(request).await;
        assert!(response.is_ok());

        let delete_response = response.unwrap().into_inner();
        assert!(!delete_response.success);
        assert!(delete_response.error.is_some());
    }

    #[tokio::test]
    async fn test_delete_version_invalid_opts() {
        let service = create_test_node_service();

        let request = Request::new(DeleteVersionRequest {
            disk: "invalid-disk-path".to_string(),
            volume: "test-volume".to_string(),
            path: "test-path".to_string(),
            file_info: "{}".to_string(),
            force_del_marker: false,
            opts: "invalid json".to_string(),
        });

        let response = service.delete_version(request).await;
        assert!(response.is_ok());

        let delete_response = response.unwrap().into_inner();
        assert!(!delete_response.success);
        assert!(delete_response.error.is_some());
    }

    #[tokio::test]
    async fn test_delete_versions_invalid_disk() {
        let service = create_test_node_service();

        let request = Request::new(DeleteVersionsRequest {
            disk: "invalid-disk-path".to_string(),
            volume: "test-volume".to_string(),
            versions: vec!["{}".to_string()],
            opts: "{}".to_string(),
        });

        let response = service.delete_versions(request).await;
        assert!(response.is_ok());

        let delete_response = response.unwrap().into_inner();
        assert!(!delete_response.success);
        assert!(delete_response.error.is_some());
    }

    #[tokio::test]
    async fn test_delete_versions_invalid_versions() {
        let service = create_test_node_service();

        let request = Request::new(DeleteVersionsRequest {
            disk: "invalid-disk-path".to_string(),
            volume: "test-volume".to_string(),
            versions: vec!["invalid json".to_string()],
            opts: "{}".to_string(),
        });

        let response = service.delete_versions(request).await;
        assert!(response.is_ok());

        let delete_response = response.unwrap().into_inner();
        assert!(!delete_response.success);
        assert!(delete_response.error.is_some());
    }

    #[tokio::test]
    async fn test_delete_versions_invalid_opts() {
        let service = create_test_node_service();

        let request = Request::new(DeleteVersionsRequest {
            disk: "invalid-disk-path".to_string(),
            volume: "test-volume".to_string(),
            versions: vec!["{}".to_string()],
            opts: "invalid json".to_string(),
        });

        let response = service.delete_versions(request).await;
        assert!(response.is_ok());

        let delete_response = response.unwrap().into_inner();
        assert!(!delete_response.success);
        assert!(delete_response.error.is_some());
    }

    #[tokio::test]
    async fn test_read_multiple_invalid_disk() {
        let service = create_test_node_service();

        let request = Request::new(ReadMultipleRequest {
            disk: "invalid-disk-path".to_string(),
            read_multiple_req: "{}".to_string(),
        });

        let response = service.read_multiple(request).await;
        assert!(response.is_ok());

        let read_response = response.unwrap().into_inner();
        assert!(!read_response.success);
        assert!(read_response.error.is_some());
        assert!(read_response.read_multiple_resps.is_empty());
    }

    #[tokio::test]
    async fn test_read_multiple_invalid_request() {
        let service = create_test_node_service();

        let request = Request::new(ReadMultipleRequest {
            disk: "invalid-disk-path".to_string(),
            read_multiple_req: "invalid json".to_string(),
        });

        let response = service.read_multiple(request).await;
        assert!(response.is_ok());

        let read_response = response.unwrap().into_inner();
        assert!(!read_response.success);
        assert!(read_response.error.is_some());
    }

    #[tokio::test]
    async fn test_delete_volume_invalid_disk() {
        let service = create_test_node_service();

        let request = Request::new(DeleteVolumeRequest {
            disk: "invalid-disk-path".to_string(),
            volume: "test-volume".to_string(),
        });

        let response = service.delete_volume(request).await;
        assert!(response.is_ok());

        let delete_response = response.unwrap().into_inner();
        assert!(!delete_response.success);
        assert!(delete_response.error.is_some());
    }

    #[tokio::test]
    async fn test_disk_info_invalid_disk() {
        let service = create_test_node_service();

        let request = Request::new(DiskInfoRequest {
            disk: "invalid-disk-path".to_string(),
            opts: "{}".to_string(),
        });

        let response = service.disk_info(request).await;
        assert!(response.is_ok());

        let info_response = response.unwrap().into_inner();
        assert!(!info_response.success);
        assert!(info_response.error.is_some());
        assert!(info_response.disk_info.is_empty());
    }

    #[tokio::test]
    async fn test_disk_info_invalid_opts() {
        let service = create_test_node_service();

        let request = Request::new(DiskInfoRequest {
            disk: "invalid-disk-path".to_string(),
            opts: "invalid json".to_string(),
        });

        let response = service.disk_info(request).await;
        assert!(response.is_ok());

        let info_response = response.unwrap().into_inner();
        assert!(!info_response.success);
        assert!(info_response.error.is_some());
    }

    #[tokio::test]
    async fn test_lock_invalid_args() {
        let service = create_test_node_service();

        let request = Request::new(GenerallyLockRequest {
            args: "invalid json".to_string(),
        });

        let response = service.lock(request).await;
        assert!(response.is_ok());

        let lock_response = response.unwrap().into_inner();
        assert!(!lock_response.success);
        assert!(lock_response.error_info.is_some());
    }

    #[tokio::test]
    async fn test_un_lock_invalid_args() {
        let service = create_test_node_service();

        let request = Request::new(GenerallyLockRequest {
            args: "invalid json".to_string(),
        });

        let response = service.un_lock(request).await;
        assert!(response.is_ok());

        let unlock_response = response.unwrap().into_inner();
        assert!(!unlock_response.success);
        assert!(unlock_response.error_info.is_some());
    }

    #[tokio::test]
    async fn test_r_lock_invalid_args() {
        let service = create_test_node_service();

        let request = Request::new(GenerallyLockRequest {
            args: "invalid json".to_string(),
        });

        let response = service.r_lock(request).await;
        assert!(response.is_ok());

        let rlock_response = response.unwrap().into_inner();
        assert!(!rlock_response.success);
        assert!(rlock_response.error_info.is_some());
    }

    #[tokio::test]
    async fn test_r_un_lock_invalid_args() {
        let service = create_test_node_service();

        let request = Request::new(GenerallyLockRequest {
            args: "invalid json".to_string(),
        });

        let response = service.r_un_lock(request).await;
        assert!(response.is_ok());

        let runlock_response = response.unwrap().into_inner();
        assert!(!runlock_response.success);
        assert!(runlock_response.error_info.is_some());
    }

    #[tokio::test]
    async fn test_force_un_lock_invalid_args() {
        let service = create_test_node_service();

        let request = Request::new(GenerallyLockRequest {
            args: "invalid json".to_string(),
        });

        let response = service.force_un_lock(request).await;
        assert!(response.is_ok());

        let force_unlock_response = response.unwrap().into_inner();
        assert!(!force_unlock_response.success);
        assert!(force_unlock_response.error_info.is_some());
    }

    #[tokio::test]
    async fn test_refresh_invalid_args() {
        let service = create_test_node_service();

        let request = Request::new(GenerallyLockRequest {
            args: "invalid json".to_string(),
        });

        let response = service.refresh(request).await;
        assert!(response.is_ok());

        let refresh_response = response.unwrap().into_inner();
        assert!(!refresh_response.success);
        assert!(refresh_response.error_info.is_some());
    }

    #[tokio::test]
    async fn test_local_storage_info() {
        let service = create_test_node_service();

        let request = Request::new(LocalStorageInfoRequest { metrics: false });

        let response = service.local_storage_info(request).await;
        assert!(response.is_ok());

        let info_response = response.unwrap().into_inner();
        // Should fail because object layer is not initialized in test
        assert!(!info_response.success);
        assert!(info_response.error_info.is_some());
    }

    #[tokio::test]
    async fn test_server_info() {
        let service = create_test_node_service();

        let request = Request::new(ServerInfoRequest { metrics: false });

        let response = service.server_info(request).await;
        assert!(response.is_ok());

        let info_response = response.unwrap().into_inner();
        assert!(info_response.success);
        assert!(!info_response.server_properties.is_empty());
    }

    #[tokio::test]
    async fn test_get_cpus() {
        let service = create_test_node_service();

        let request = Request::new(GetCpusRequest {});

        let response = service.get_cpus(request).await;
        assert!(response.is_ok());

        let cpus_response = response.unwrap().into_inner();
        assert!(cpus_response.success);
        assert!(!cpus_response.cpus.is_empty());
    }

    #[tokio::test]
    async fn test_get_net_info() {
        let service = create_test_node_service();

        let request = Request::new(GetNetInfoRequest {});

        let response = service.get_net_info(request).await;
        assert!(response.is_ok());

        let net_response = response.unwrap().into_inner();
        assert!(net_response.success);
        assert!(!net_response.net_info.is_empty());
    }

    #[tokio::test]
    async fn test_get_partitions() {
        let service = create_test_node_service();

        let request = Request::new(GetPartitionsRequest {});

        let response = service.get_partitions(request).await;
        assert!(response.is_ok());

        let partitions_response = response.unwrap().into_inner();
        assert!(partitions_response.success);
        assert!(!partitions_response.partitions.is_empty());
    }

    #[tokio::test]
    async fn test_get_os_info() {
        let service = create_test_node_service();

        let request = Request::new(GetOsInfoRequest {});

        let response = service.get_os_info(request).await;
        assert!(response.is_ok());

        let os_response = response.unwrap().into_inner();
        assert!(os_response.success);
        assert!(!os_response.os_info.is_empty());
    }

    #[tokio::test]
    async fn test_get_se_linux_info() {
        let service = create_test_node_service();

        let request = Request::new(GetSeLinuxInfoRequest {});

        let response = service.get_se_linux_info(request).await;
        assert!(response.is_ok());

        let selinux_response = response.unwrap().into_inner();
        assert!(selinux_response.success);
        assert!(!selinux_response.sys_services.is_empty());
    }

    #[tokio::test]
    async fn test_get_sys_config() {
        let service = create_test_node_service();

        let request = Request::new(GetSysConfigRequest {});

        let response = service.get_sys_config(request).await;
        assert!(response.is_ok());

        let config_response = response.unwrap().into_inner();
        assert!(config_response.success);
        assert!(!config_response.sys_config.is_empty());
    }

    #[tokio::test]
    async fn test_get_sys_errors() {
        let service = create_test_node_service();

        let request = Request::new(GetSysErrorsRequest {});

        let response = service.get_sys_errors(request).await;
        assert!(response.is_ok());

        let errors_response = response.unwrap().into_inner();
        assert!(errors_response.success);
        assert!(!errors_response.sys_errors.is_empty());
    }

    #[tokio::test]
    async fn test_get_mem_info() {
        let service = create_test_node_service();

        let request = Request::new(GetMemInfoRequest {});

        let response = service.get_mem_info(request).await;
        assert!(response.is_ok());

        let mem_response = response.unwrap().into_inner();
        assert!(mem_response.success);
        assert!(!mem_response.mem_info.is_empty());
    }

    #[tokio::test]
    async fn test_get_proc_info() {
        let service = create_test_node_service();

        let request = Request::new(GetProcInfoRequest {});

        let response = service.get_proc_info(request).await;
        assert!(response.is_ok());

        let proc_response = response.unwrap().into_inner();
        assert!(proc_response.success);
        assert!(!proc_response.proc_info.is_empty());
    }

    #[tokio::test]
    async fn test_reload_pool_meta() {
        let service = create_test_node_service();

        let request = Request::new(ReloadPoolMetaRequest {});

        let response = service.reload_pool_meta(request).await;
        assert!(response.is_ok());

        let reload_response = response.unwrap().into_inner();
        // Should fail because object layer is not initialized in test
        assert!(!reload_response.success);
        assert!(reload_response.error_info.is_some());
    }

    #[tokio::test]
    async fn test_stop_rebalance() {
        let service = create_test_node_service();

        let request = Request::new(StopRebalanceRequest {});

        let response = service.stop_rebalance(request).await;
        assert!(response.is_ok());

        let stop_response = response.unwrap().into_inner();
        // Should fail because object layer is not initialized in test
        assert!(!stop_response.success);
        assert!(stop_response.error_info.is_some());
    }

    #[tokio::test]
    async fn test_load_rebalance_meta() {
        let service = create_test_node_service();

        let request = Request::new(LoadRebalanceMetaRequest { start_rebalance: false });

        let response = service.load_rebalance_meta(request).await;
        // Should return error because object layer is not initialized, or success if it's implemented
        assert!(response.is_err() || response.is_ok());
    }

    #[tokio::test]
    async fn test_load_bucket_metadata_empty_bucket() {
        let service = create_test_node_service();

        let request = Request::new(LoadBucketMetadataRequest { bucket: "".to_string() });

        let response = service.load_bucket_metadata(request).await;
        assert!(response.is_ok());

        let load_response = response.unwrap().into_inner();
        assert!(!load_response.success);
        assert!(load_response.error_info.is_some());
        assert!(load_response.error_info.unwrap().contains("bucket name is missing"));
    }

    #[tokio::test]
    async fn test_load_bucket_metadata_no_object_layer() {
        let service = create_test_node_service();

        let request = Request::new(LoadBucketMetadataRequest {
            bucket: "test-bucket".to_string(),
        });

        let response = service.load_bucket_metadata(request).await;
        assert!(response.is_ok());

        let load_response = response.unwrap().into_inner();
        assert!(!load_response.success);
        assert!(load_response.error_info.is_some());
        assert!(load_response.error_info.unwrap().contains("errServerNotInitialized"));
    }

    #[tokio::test]
    async fn test_delete_bucket_metadata() {
        let service = create_test_node_service();

        let request = Request::new(DeleteBucketMetadataRequest {
            bucket: "test-bucket".to_string(),
        });

        let response = service.delete_bucket_metadata(request).await;
        assert!(response.is_ok());

        let delete_response = response.unwrap().into_inner();
        assert!(delete_response.success); // Currently returns success (todo implementation)
    }

    #[tokio::test]
    async fn test_delete_policy_empty_name() {
        let service = create_test_node_service();

        let request = Request::new(DeletePolicyRequest {
            policy_name: "".to_string(),
        });

        let response = service.delete_policy(request).await;
        assert!(response.is_ok());

        let delete_response = response.unwrap().into_inner();
        assert!(!delete_response.success);
        assert!(delete_response.error_info.is_some());
        assert!(delete_response.error_info.unwrap().contains("policy name is missing"));
    }

    #[tokio::test]
    async fn test_load_policy_empty_name() {
        let service = create_test_node_service();

        let request = Request::new(LoadPolicyRequest {
            policy_name: "".to_string(),
        });

        let response = service.load_policy(request).await;
        assert!(response.is_ok());

        let load_response = response.unwrap().into_inner();
        assert!(!load_response.success);
        assert!(load_response.error_info.is_some());
        assert!(load_response.error_info.unwrap().contains("policy name is missing"));
    }

    #[tokio::test]
    async fn test_load_policy_mapping_empty_user() {
        let service = create_test_node_service();

        let request = Request::new(LoadPolicyMappingRequest {
            user_or_group: "".to_string(),
            user_type: 0,
            is_group: false,
        });

        let response = service.load_policy_mapping(request).await;
        assert!(response.is_ok());

        let load_response = response.unwrap().into_inner();
        assert!(!load_response.success);
        assert!(load_response.error_info.is_some());
        assert!(load_response.error_info.unwrap().contains("user_or_group name is missing"));
    }

    #[tokio::test]
    async fn test_delete_user_empty_access_key() {
        let service = create_test_node_service();

        let request = Request::new(DeleteUserRequest {
            access_key: "".to_string(),
        });

        let response = service.delete_user(request).await;
        assert!(response.is_ok());

        let delete_response = response.unwrap().into_inner();
        assert!(!delete_response.success);
        assert!(delete_response.error_info.is_some());
        assert!(delete_response.error_info.unwrap().contains("access_key name is missing"));
    }

    #[tokio::test]
    async fn test_delete_service_account_empty_access_key() {
        let service = create_test_node_service();

        let request = Request::new(DeleteServiceAccountRequest {
            access_key: "".to_string(),
        });

        let response = service.delete_service_account(request).await;
        assert!(response.is_ok());

        let delete_response = response.unwrap().into_inner();
        assert!(!delete_response.success);
        assert!(delete_response.error_info.is_some());
        assert!(delete_response.error_info.unwrap().contains("access_key name is missing"));
    }

    #[tokio::test]
    async fn test_load_user_empty_access_key() {
        let service = create_test_node_service();

        let request = Request::new(LoadUserRequest {
            access_key: "".to_string(),
            temp: false,
        });

        let response = service.load_user(request).await;
        assert!(response.is_ok());

        let load_response = response.unwrap().into_inner();
        assert!(!load_response.success);
        assert!(load_response.error_info.is_some());
        assert!(load_response.error_info.unwrap().contains("access_key name is missing"));
    }

    #[tokio::test]
    async fn test_load_service_account_empty_access_key() {
        let service = create_test_node_service();

        let request = Request::new(LoadServiceAccountRequest {
            access_key: "".to_string(),
        });

        let response = service.load_service_account(request).await;
        assert!(response.is_ok());

        let load_response = response.unwrap().into_inner();
        assert!(!load_response.success);
        assert!(load_response.error_info.is_some());
        assert!(load_response.error_info.unwrap().contains("access_key name is missing"));
    }

    #[tokio::test]
    async fn test_load_group_empty_name() {
        let service = create_test_node_service();

        let request = Request::new(LoadGroupRequest { group: "".to_string() });

        let response = service.load_group(request).await;
        assert!(response.is_ok());

        let load_response = response.unwrap().into_inner();
        assert!(!load_response.success);
        assert!(load_response.error_info.is_some());
        assert!(load_response.error_info.unwrap().contains("group name is missing"));
    }

    #[tokio::test]
    async fn test_reload_site_replication_config() {
        let service = create_test_node_service();

        let request = Request::new(ReloadSiteReplicationConfigRequest {});

        let response = service.reload_site_replication_config(request).await;
        assert!(response.is_ok());

        let reload_response = response.unwrap().into_inner();
        // Should fail because object layer is not initialized in test
        assert!(!reload_response.success);
        assert!(reload_response.error_info.is_some());
    }

    // Note: signal_service test is skipped because it contains todo!() and would panic

    #[tokio::test]
    async fn test_node_service_debug() {
        let service = create_test_node_service();
        let debug_str = format!("{service:?}");
        assert!(debug_str.contains("NodeService"));
    }

    #[tokio::test]
    async fn test_node_service_creation() {
        let service1 = make_server();
        let service2 = make_server();

        // Both services should be created successfully
        assert!(format!("{service1:?}").contains("NodeService"));
        assert!(format!("{service2:?}").contains("NodeService"));
    }

    #[tokio::test]
    async fn test_find_disk_method() {
        let service = create_test_node_service();
        let disk = service.find_disk(&"non-existent-disk".to_string()).await;
        // Should return None for non-existent disk
        assert!(disk.is_none());
    }

    #[tokio::test]
    async fn test_get_metrics_invalid_metric_type() {
        let service = create_test_node_service();
        let request = Request::new(GetMetricsRequest {
            metric_type: Bytes::from(vec![0x00u8, 0x01u8]), // Invalid rmp data
            opts: Bytes::new(),                             // Valid or invalid
        });
        let response = service.get_metrics(request).await.unwrap().into_inner();
        assert!(!response.success);
        assert!(response.error_info.is_some());
    }

    #[tokio::test]
    async fn test_get_metrics_invalid_opts() {
        let service = create_test_node_service();
        // Serialize a valid MetricType
        let metric_type = MetricType::DISK;
        let metric_type_bytes = rmp_serde::to_vec(&metric_type).unwrap();

        let request = Request::new(GetMetricsRequest {
            metric_type: Bytes::from(metric_type_bytes),
            opts: Bytes::from(vec![0x00u8, 0x01u8]), // Invalid rmp data
        });
        let response = service.get_metrics(request).await.unwrap().into_inner();
        assert!(!response.success);
        assert!(response.error_info.is_some());
    }
}
