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

// Used by test_distributed_lock_4_nodes_grpc in lock.rs
#![allow(dead_code)]

use bytes::Bytes;
use futures::Stream;
use rustfs_lock::{LockClient, LockRequest};
use rustfs_protos::{
    models::PingBodyBuilder,
    proto_gen::node_service::{
        GenerallyLockRequest, GenerallyLockResponse, PingRequest, PingResponse, node_service_server::NodeService,
    },
};
use std::pin::Pin;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response, Status};
use tracing::debug;

type ResponseStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>;

/// Minimal NodeService implementation that only supports Lock RPCs
/// Used for testing distributed lock scenarios with real gRPC
#[derive(Debug)]
pub struct MinimalLockNodeService {
    lock_client: Arc<dyn LockClient>,
}

impl MinimalLockNodeService {
    pub fn new(lock_client: Arc<dyn LockClient>) -> Self {
        Self { lock_client }
    }
}

#[tonic::async_trait]
impl NodeService for MinimalLockNodeService {
    async fn ping(&self, _request: Request<PingRequest>) -> Result<Response<PingResponse>, Status> {
        debug!("MinimalLockNodeService: PING");

        let mut fbb = flatbuffers::FlatBufferBuilder::new();
        let payload = fbb.create_vector(b"pong");

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

    async fn lock(&self, request: Request<GenerallyLockRequest>) -> Result<Response<GenerallyLockResponse>, Status> {
        let request = request.into_inner();
        let args: LockRequest = match serde_json::from_str(&request.args) {
            Ok(args) => args,
            Err(err) => {
                return Ok(Response::new(GenerallyLockResponse {
                    success: false,
                    error_info: Some(format!("can not decode args, err: {err}")),
                    lock_info: None,
                }));
            }
        };

        match self.lock_client.acquire_lock(&args).await {
            Ok(result) => {
                let lock_info_json = result.lock_info.as_ref().and_then(|info| serde_json::to_string(info).ok());
                Ok(Response::new(GenerallyLockResponse {
                    success: result.success,
                    error_info: None,
                    lock_info: lock_info_json,
                }))
            }
            Err(err) => Ok(Response::new(GenerallyLockResponse {
                success: false,
                error_info: Some(format!(
                    "can not lock, resource: {0}, owner: {1}, err: {2}",
                    args.resource, args.owner, err
                )),
                lock_info: None,
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
                    lock_info: None,
                }));
            }
        };

        match self.lock_client.release(&args.lock_id).await {
            Ok(success) => Ok(Response::new(GenerallyLockResponse {
                success,
                error_info: None,
                lock_info: None,
            })),
            Err(err) => Ok(Response::new(GenerallyLockResponse {
                success: false,
                error_info: Some(format!(
                    "can not unlock, resource: {0}, owner: {1}, err: {2}",
                    args.resource, args.owner, err
                )),
                lock_info: None,
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
                    lock_info: None,
                }));
            }
        };

        match self.lock_client.force_release(&args.lock_id).await {
            Ok(success) => Ok(Response::new(GenerallyLockResponse {
                success,
                error_info: None,
                lock_info: None,
            })),
            Err(err) => Ok(Response::new(GenerallyLockResponse {
                success: false,
                error_info: Some(format!(
                    "can not force_unlock, resource: {0}, owner: {1}, err: {2}",
                    args.resource, args.owner, err
                )),
                lock_info: None,
            })),
        }
    }

    async fn refresh(&self, request: Request<GenerallyLockRequest>) -> Result<Response<GenerallyLockResponse>, Status> {
        let request = request.into_inner();
        let args: LockRequest = match serde_json::from_str(&request.args) {
            Ok(args) => args,
            Err(err) => {
                return Ok(Response::new(GenerallyLockResponse {
                    success: false,
                    error_info: Some(format!("can not decode args, err: {err}")),
                    lock_info: None,
                }));
            }
        };

        match self.lock_client.refresh(&args.lock_id).await {
            Ok(success) => Ok(Response::new(GenerallyLockResponse {
                success,
                error_info: None,
                lock_info: None,
            })),
            Err(err) => Ok(Response::new(GenerallyLockResponse {
                success: false,
                error_info: Some(format!("can not refresh, err: {err}")),
                lock_info: None,
            })),
        }
    }

    // All other methods return unimplemented
    async fn heal_bucket(
        &self,
        _request: Request<rustfs_protos::proto_gen::node_service::HealBucketRequest>,
    ) -> Result<Response<rustfs_protos::proto_gen::node_service::HealBucketResponse>, Status> {
        Err(Status::unimplemented("lock-only test server"))
    }

    async fn list_bucket(
        &self,
        _request: Request<rustfs_protos::proto_gen::node_service::ListBucketRequest>,
    ) -> Result<Response<rustfs_protos::proto_gen::node_service::ListBucketResponse>, Status> {
        Err(Status::unimplemented("lock-only test server"))
    }

    async fn make_bucket(
        &self,
        _request: Request<rustfs_protos::proto_gen::node_service::MakeBucketRequest>,
    ) -> Result<Response<rustfs_protos::proto_gen::node_service::MakeBucketResponse>, Status> {
        Err(Status::unimplemented("lock-only test server"))
    }

    async fn get_bucket_info(
        &self,
        _request: Request<rustfs_protos::proto_gen::node_service::GetBucketInfoRequest>,
    ) -> Result<Response<rustfs_protos::proto_gen::node_service::GetBucketInfoResponse>, Status> {
        Err(Status::unimplemented("lock-only test server"))
    }

    async fn delete_bucket(
        &self,
        _request: Request<rustfs_protos::proto_gen::node_service::DeleteBucketRequest>,
    ) -> Result<Response<rustfs_protos::proto_gen::node_service::DeleteBucketResponse>, Status> {
        Err(Status::unimplemented("lock-only test server"))
    }

    async fn read_all(
        &self,
        _request: Request<rustfs_protos::proto_gen::node_service::ReadAllRequest>,
    ) -> Result<Response<rustfs_protos::proto_gen::node_service::ReadAllResponse>, Status> {
        Err(Status::unimplemented("lock-only test server"))
    }

    async fn write_all(
        &self,
        _request: Request<rustfs_protos::proto_gen::node_service::WriteAllRequest>,
    ) -> Result<Response<rustfs_protos::proto_gen::node_service::WriteAllResponse>, Status> {
        Err(Status::unimplemented("lock-only test server"))
    }

    async fn delete(
        &self,
        _request: Request<rustfs_protos::proto_gen::node_service::DeleteRequest>,
    ) -> Result<Response<rustfs_protos::proto_gen::node_service::DeleteResponse>, Status> {
        Err(Status::unimplemented("lock-only test server"))
    }

    async fn verify_file(
        &self,
        _request: Request<rustfs_protos::proto_gen::node_service::VerifyFileRequest>,
    ) -> Result<Response<rustfs_protos::proto_gen::node_service::VerifyFileResponse>, Status> {
        Err(Status::unimplemented("lock-only test server"))
    }

    async fn read_parts(
        &self,
        _request: Request<rustfs_protos::proto_gen::node_service::ReadPartsRequest>,
    ) -> Result<Response<rustfs_protos::proto_gen::node_service::ReadPartsResponse>, Status> {
        Err(Status::unimplemented("lock-only test server"))
    }

    async fn check_parts(
        &self,
        _request: Request<rustfs_protos::proto_gen::node_service::CheckPartsRequest>,
    ) -> Result<Response<rustfs_protos::proto_gen::node_service::CheckPartsResponse>, Status> {
        Err(Status::unimplemented("lock-only test server"))
    }

    async fn rename_part(
        &self,
        _request: Request<rustfs_protos::proto_gen::node_service::RenamePartRequest>,
    ) -> Result<Response<rustfs_protos::proto_gen::node_service::RenamePartResponse>, Status> {
        Err(Status::unimplemented("lock-only test server"))
    }

    async fn rename_file(
        &self,
        _request: Request<rustfs_protos::proto_gen::node_service::RenameFileRequest>,
    ) -> Result<Response<rustfs_protos::proto_gen::node_service::RenameFileResponse>, Status> {
        Err(Status::unimplemented("lock-only test server"))
    }

    async fn write(
        &self,
        _request: Request<rustfs_protos::proto_gen::node_service::WriteRequest>,
    ) -> Result<Response<rustfs_protos::proto_gen::node_service::WriteResponse>, Status> {
        Err(Status::unimplemented("lock-only test server"))
    }

    type WriteStreamStream = ResponseStream<rustfs_protos::proto_gen::node_service::WriteResponse>;

    async fn write_stream(
        &self,
        _request: Request<tonic::Streaming<rustfs_protos::proto_gen::node_service::WriteRequest>>,
    ) -> Result<Response<Self::WriteStreamStream>, Status> {
        Err(Status::unimplemented("lock-only test server"))
    }

    type ReadAtStream = ResponseStream<rustfs_protos::proto_gen::node_service::ReadAtResponse>;

    async fn read_at(
        &self,
        _request: Request<tonic::Streaming<rustfs_protos::proto_gen::node_service::ReadAtRequest>>,
    ) -> Result<Response<Self::ReadAtStream>, Status> {
        Err(Status::unimplemented("lock-only test server"))
    }

    async fn list_dir(
        &self,
        _request: Request<rustfs_protos::proto_gen::node_service::ListDirRequest>,
    ) -> Result<Response<rustfs_protos::proto_gen::node_service::ListDirResponse>, Status> {
        Err(Status::unimplemented("lock-only test server"))
    }

    type WalkDirStream = ResponseStream<rustfs_protos::proto_gen::node_service::WalkDirResponse>;

    async fn walk_dir(
        &self,
        _request: Request<rustfs_protos::proto_gen::node_service::WalkDirRequest>,
    ) -> Result<Response<Self::WalkDirStream>, Status> {
        Err(Status::unimplemented("lock-only test server"))
    }

    async fn rename_data(
        &self,
        _request: Request<rustfs_protos::proto_gen::node_service::RenameDataRequest>,
    ) -> Result<Response<rustfs_protos::proto_gen::node_service::RenameDataResponse>, Status> {
        Err(Status::unimplemented("lock-only test server"))
    }

    async fn make_volumes(
        &self,
        _request: Request<rustfs_protos::proto_gen::node_service::MakeVolumesRequest>,
    ) -> Result<Response<rustfs_protos::proto_gen::node_service::MakeVolumesResponse>, Status> {
        Err(Status::unimplemented("lock-only test server"))
    }

    async fn make_volume(
        &self,
        _request: Request<rustfs_protos::proto_gen::node_service::MakeVolumeRequest>,
    ) -> Result<Response<rustfs_protos::proto_gen::node_service::MakeVolumeResponse>, Status> {
        Err(Status::unimplemented("lock-only test server"))
    }

    async fn list_volumes(
        &self,
        _request: Request<rustfs_protos::proto_gen::node_service::ListVolumesRequest>,
    ) -> Result<Response<rustfs_protos::proto_gen::node_service::ListVolumesResponse>, Status> {
        Err(Status::unimplemented("lock-only test server"))
    }

    async fn stat_volume(
        &self,
        _request: Request<rustfs_protos::proto_gen::node_service::StatVolumeRequest>,
    ) -> Result<Response<rustfs_protos::proto_gen::node_service::StatVolumeResponse>, Status> {
        Err(Status::unimplemented("lock-only test server"))
    }

    async fn delete_paths(
        &self,
        _request: Request<rustfs_protos::proto_gen::node_service::DeletePathsRequest>,
    ) -> Result<Response<rustfs_protos::proto_gen::node_service::DeletePathsResponse>, Status> {
        Err(Status::unimplemented("lock-only test server"))
    }

    async fn update_metadata(
        &self,
        _request: Request<rustfs_protos::proto_gen::node_service::UpdateMetadataRequest>,
    ) -> Result<Response<rustfs_protos::proto_gen::node_service::UpdateMetadataResponse>, Status> {
        Err(Status::unimplemented("lock-only test server"))
    }

    async fn read_metadata(
        &self,
        _request: Request<rustfs_protos::proto_gen::node_service::ReadMetadataRequest>,
    ) -> Result<Response<rustfs_protos::proto_gen::node_service::ReadMetadataResponse>, Status> {
        Err(Status::unimplemented("lock-only test server"))
    }

    async fn write_metadata(
        &self,
        _request: Request<rustfs_protos::proto_gen::node_service::WriteMetadataRequest>,
    ) -> Result<Response<rustfs_protos::proto_gen::node_service::WriteMetadataResponse>, Status> {
        Err(Status::unimplemented("lock-only test server"))
    }

    async fn read_version(
        &self,
        _request: Request<rustfs_protos::proto_gen::node_service::ReadVersionRequest>,
    ) -> Result<Response<rustfs_protos::proto_gen::node_service::ReadVersionResponse>, Status> {
        Err(Status::unimplemented("lock-only test server"))
    }

    async fn read_xl(
        &self,
        _request: Request<rustfs_protos::proto_gen::node_service::ReadXlRequest>,
    ) -> Result<Response<rustfs_protos::proto_gen::node_service::ReadXlResponse>, Status> {
        Err(Status::unimplemented("lock-only test server"))
    }

    async fn delete_version(
        &self,
        _request: Request<rustfs_protos::proto_gen::node_service::DeleteVersionRequest>,
    ) -> Result<Response<rustfs_protos::proto_gen::node_service::DeleteVersionResponse>, Status> {
        Err(Status::unimplemented("lock-only test server"))
    }

    async fn delete_versions(
        &self,
        _request: Request<rustfs_protos::proto_gen::node_service::DeleteVersionsRequest>,
    ) -> Result<Response<rustfs_protos::proto_gen::node_service::DeleteVersionsResponse>, Status> {
        Err(Status::unimplemented("lock-only test server"))
    }

    async fn read_multiple(
        &self,
        _request: Request<rustfs_protos::proto_gen::node_service::ReadMultipleRequest>,
    ) -> Result<Response<rustfs_protos::proto_gen::node_service::ReadMultipleResponse>, Status> {
        Err(Status::unimplemented("lock-only test server"))
    }

    async fn delete_volume(
        &self,
        _request: Request<rustfs_protos::proto_gen::node_service::DeleteVolumeRequest>,
    ) -> Result<Response<rustfs_protos::proto_gen::node_service::DeleteVolumeResponse>, Status> {
        Err(Status::unimplemented("lock-only test server"))
    }

    async fn disk_info(
        &self,
        _request: Request<rustfs_protos::proto_gen::node_service::DiskInfoRequest>,
    ) -> Result<Response<rustfs_protos::proto_gen::node_service::DiskInfoResponse>, Status> {
        Err(Status::unimplemented("lock-only test server"))
    }

    async fn local_storage_info(
        &self,
        _request: Request<rustfs_protos::proto_gen::node_service::LocalStorageInfoRequest>,
    ) -> Result<Response<rustfs_protos::proto_gen::node_service::LocalStorageInfoResponse>, Status> {
        Err(Status::unimplemented("lock-only test server"))
    }

    async fn server_info(
        &self,
        _request: Request<rustfs_protos::proto_gen::node_service::ServerInfoRequest>,
    ) -> Result<Response<rustfs_protos::proto_gen::node_service::ServerInfoResponse>, Status> {
        Err(Status::unimplemented("lock-only test server"))
    }

    async fn get_cpus(
        &self,
        _request: Request<rustfs_protos::proto_gen::node_service::GetCpusRequest>,
    ) -> Result<Response<rustfs_protos::proto_gen::node_service::GetCpusResponse>, Status> {
        Err(Status::unimplemented("lock-only test server"))
    }

    async fn get_net_info(
        &self,
        _request: Request<rustfs_protos::proto_gen::node_service::GetNetInfoRequest>,
    ) -> Result<Response<rustfs_protos::proto_gen::node_service::GetNetInfoResponse>, Status> {
        Err(Status::unimplemented("lock-only test server"))
    }

    async fn get_partitions(
        &self,
        _request: Request<rustfs_protos::proto_gen::node_service::GetPartitionsRequest>,
    ) -> Result<Response<rustfs_protos::proto_gen::node_service::GetPartitionsResponse>, Status> {
        Err(Status::unimplemented("lock-only test server"))
    }

    async fn get_os_info(
        &self,
        _request: Request<rustfs_protos::proto_gen::node_service::GetOsInfoRequest>,
    ) -> Result<Response<rustfs_protos::proto_gen::node_service::GetOsInfoResponse>, Status> {
        Err(Status::unimplemented("lock-only test server"))
    }

    async fn get_se_linux_info(
        &self,
        _request: Request<rustfs_protos::proto_gen::node_service::GetSeLinuxInfoRequest>,
    ) -> Result<Response<rustfs_protos::proto_gen::node_service::GetSeLinuxInfoResponse>, Status> {
        Err(Status::unimplemented("lock-only test server"))
    }

    async fn get_sys_config(
        &self,
        _request: Request<rustfs_protos::proto_gen::node_service::GetSysConfigRequest>,
    ) -> Result<Response<rustfs_protos::proto_gen::node_service::GetSysConfigResponse>, Status> {
        Err(Status::unimplemented("lock-only test server"))
    }

    async fn get_sys_errors(
        &self,
        _request: Request<rustfs_protos::proto_gen::node_service::GetSysErrorsRequest>,
    ) -> Result<Response<rustfs_protos::proto_gen::node_service::GetSysErrorsResponse>, Status> {
        Err(Status::unimplemented("lock-only test server"))
    }

    async fn get_mem_info(
        &self,
        _request: Request<rustfs_protos::proto_gen::node_service::GetMemInfoRequest>,
    ) -> Result<Response<rustfs_protos::proto_gen::node_service::GetMemInfoResponse>, Status> {
        Err(Status::unimplemented("lock-only test server"))
    }

    async fn get_proc_info(
        &self,
        _request: Request<rustfs_protos::proto_gen::node_service::GetProcInfoRequest>,
    ) -> Result<Response<rustfs_protos::proto_gen::node_service::GetProcInfoResponse>, Status> {
        Err(Status::unimplemented("lock-only test server"))
    }

    async fn load_bucket_metadata(
        &self,
        _request: Request<rustfs_protos::proto_gen::node_service::LoadBucketMetadataRequest>,
    ) -> Result<Response<rustfs_protos::proto_gen::node_service::LoadBucketMetadataResponse>, Status> {
        Err(Status::unimplemented("lock-only test server"))
    }

    async fn load_policy(
        &self,
        _request: Request<rustfs_protos::proto_gen::node_service::LoadPolicyRequest>,
    ) -> Result<Response<rustfs_protos::proto_gen::node_service::LoadPolicyResponse>, Status> {
        Err(Status::unimplemented("lock-only test server"))
    }

    async fn load_group(
        &self,
        _request: Request<rustfs_protos::proto_gen::node_service::LoadGroupRequest>,
    ) -> Result<Response<rustfs_protos::proto_gen::node_service::LoadGroupResponse>, Status> {
        Err(Status::unimplemented("lock-only test server"))
    }

    async fn load_policy_mapping(
        &self,
        _request: Request<rustfs_protos::proto_gen::node_service::LoadPolicyMappingRequest>,
    ) -> Result<Response<rustfs_protos::proto_gen::node_service::LoadPolicyMappingResponse>, Status> {
        Err(Status::unimplemented("lock-only test server"))
    }

    async fn load_rebalance_meta(
        &self,
        _request: Request<rustfs_protos::proto_gen::node_service::LoadRebalanceMetaRequest>,
    ) -> Result<Response<rustfs_protos::proto_gen::node_service::LoadRebalanceMetaResponse>, Status> {
        Err(Status::unimplemented("lock-only test server"))
    }

    async fn get_metrics(
        &self,
        _request: Request<rustfs_protos::proto_gen::node_service::GetMetricsRequest>,
    ) -> Result<Response<rustfs_protos::proto_gen::node_service::GetMetricsResponse>, Status> {
        Err(Status::unimplemented("lock-only test server"))
    }

    async fn start_profiling(
        &self,
        _request: Request<rustfs_protos::proto_gen::node_service::StartProfilingRequest>,
    ) -> Result<Response<rustfs_protos::proto_gen::node_service::StartProfilingResponse>, Status> {
        Err(Status::unimplemented("lock-only test server"))
    }

    async fn download_profile_data(
        &self,
        _request: Request<rustfs_protos::proto_gen::node_service::DownloadProfileDataRequest>,
    ) -> Result<Response<rustfs_protos::proto_gen::node_service::DownloadProfileDataResponse>, Status> {
        Err(Status::unimplemented("lock-only test server"))
    }

    async fn get_bucket_stats(
        &self,
        _request: Request<rustfs_protos::proto_gen::node_service::GetBucketStatsDataRequest>,
    ) -> Result<Response<rustfs_protos::proto_gen::node_service::GetBucketStatsDataResponse>, Status> {
        Err(Status::unimplemented("lock-only test server"))
    }

    async fn get_sr_metrics(
        &self,
        _request: Request<rustfs_protos::proto_gen::node_service::GetSrMetricsDataRequest>,
    ) -> Result<Response<rustfs_protos::proto_gen::node_service::GetSrMetricsDataResponse>, Status> {
        Err(Status::unimplemented("lock-only test server"))
    }

    async fn get_all_bucket_stats(
        &self,
        _request: Request<rustfs_protos::proto_gen::node_service::GetAllBucketStatsRequest>,
    ) -> Result<Response<rustfs_protos::proto_gen::node_service::GetAllBucketStatsResponse>, Status> {
        Err(Status::unimplemented("lock-only test server"))
    }

    async fn delete_bucket_metadata(
        &self,
        _request: Request<rustfs_protos::proto_gen::node_service::DeleteBucketMetadataRequest>,
    ) -> Result<Response<rustfs_protos::proto_gen::node_service::DeleteBucketMetadataResponse>, Status> {
        Err(Status::unimplemented("lock-only test server"))
    }

    async fn delete_policy(
        &self,
        _request: Request<rustfs_protos::proto_gen::node_service::DeletePolicyRequest>,
    ) -> Result<Response<rustfs_protos::proto_gen::node_service::DeletePolicyResponse>, Status> {
        Err(Status::unimplemented("lock-only test server"))
    }

    async fn delete_user(
        &self,
        _request: Request<rustfs_protos::proto_gen::node_service::DeleteUserRequest>,
    ) -> Result<Response<rustfs_protos::proto_gen::node_service::DeleteUserResponse>, Status> {
        Err(Status::unimplemented("lock-only test server"))
    }

    async fn delete_service_account(
        &self,
        _request: Request<rustfs_protos::proto_gen::node_service::DeleteServiceAccountRequest>,
    ) -> Result<Response<rustfs_protos::proto_gen::node_service::DeleteServiceAccountResponse>, Status> {
        Err(Status::unimplemented("lock-only test server"))
    }

    async fn load_user(
        &self,
        _request: Request<rustfs_protos::proto_gen::node_service::LoadUserRequest>,
    ) -> Result<Response<rustfs_protos::proto_gen::node_service::LoadUserResponse>, Status> {
        Err(Status::unimplemented("lock-only test server"))
    }

    async fn load_service_account(
        &self,
        _request: Request<rustfs_protos::proto_gen::node_service::LoadServiceAccountRequest>,
    ) -> Result<Response<rustfs_protos::proto_gen::node_service::LoadServiceAccountResponse>, Status> {
        Err(Status::unimplemented("lock-only test server"))
    }

    async fn reload_site_replication_config(
        &self,
        _request: Request<rustfs_protos::proto_gen::node_service::ReloadSiteReplicationConfigRequest>,
    ) -> Result<Response<rustfs_protos::proto_gen::node_service::ReloadSiteReplicationConfigResponse>, Status> {
        Err(Status::unimplemented("lock-only test server"))
    }

    async fn signal_service(
        &self,
        _request: Request<rustfs_protos::proto_gen::node_service::SignalServiceRequest>,
    ) -> Result<Response<rustfs_protos::proto_gen::node_service::SignalServiceResponse>, Status> {
        Err(Status::unimplemented("lock-only test server"))
    }

    async fn background_heal_status(
        &self,
        _request: Request<rustfs_protos::proto_gen::node_service::BackgroundHealStatusRequest>,
    ) -> Result<Response<rustfs_protos::proto_gen::node_service::BackgroundHealStatusResponse>, Status> {
        Err(Status::unimplemented("lock-only test server"))
    }

    async fn get_metacache_listing(
        &self,
        _request: Request<rustfs_protos::proto_gen::node_service::GetMetacacheListingRequest>,
    ) -> Result<Response<rustfs_protos::proto_gen::node_service::GetMetacacheListingResponse>, Status> {
        Err(Status::unimplemented("lock-only test server"))
    }

    async fn update_metacache_listing(
        &self,
        _request: Request<rustfs_protos::proto_gen::node_service::UpdateMetacacheListingRequest>,
    ) -> Result<Response<rustfs_protos::proto_gen::node_service::UpdateMetacacheListingResponse>, Status> {
        Err(Status::unimplemented("lock-only test server"))
    }

    async fn reload_pool_meta(
        &self,
        _request: Request<rustfs_protos::proto_gen::node_service::ReloadPoolMetaRequest>,
    ) -> Result<Response<rustfs_protos::proto_gen::node_service::ReloadPoolMetaResponse>, Status> {
        Err(Status::unimplemented("lock-only test server"))
    }

    async fn stop_rebalance(
        &self,
        _request: Request<rustfs_protos::proto_gen::node_service::StopRebalanceRequest>,
    ) -> Result<Response<rustfs_protos::proto_gen::node_service::StopRebalanceResponse>, Status> {
        Err(Status::unimplemented("lock-only test server"))
    }

    async fn load_transition_tier_config(
        &self,
        _request: Request<rustfs_protos::proto_gen::node_service::LoadTransitionTierConfigRequest>,
    ) -> Result<Response<rustfs_protos::proto_gen::node_service::LoadTransitionTierConfigResponse>, Status> {
        Err(Status::unimplemented("lock-only test server"))
    }
}

/// Spawn a gRPC lock server on a random port
/// Returns the address and a shutdown handle
pub async fn spawn_lock_server(
    lock_client: Arc<dyn LockClient>,
) -> std::result::Result<(String, tokio::task::JoinHandle<()>), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let addr_str = format!("http://127.0.0.1:{}", addr.port());

    let service = MinimalLockNodeService::new(lock_client);
    let server = tonic::transport::Server::builder()
        .add_service(rustfs_protos::proto_gen::node_service::node_service_server::NodeServiceServer::new(
            service,
        ))
        .serve_with_incoming(TcpListenerStream::new(listener));

    let handle = tokio::spawn(async move {
        if let Err(e) = server.await {
            eprintln!("gRPC server error: {}", e);
        }
    });

    Ok((addr_str, handle))
}
