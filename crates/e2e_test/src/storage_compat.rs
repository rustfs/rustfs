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

#![allow(dead_code, unused_imports)]

use rustfs_ecstore::api::{bucket as ecstore_bucket, disk as ecstore_disk, rpc as ecstore_rpc};

pub(crate) type BucketTargetSys = ecstore_bucket::bucket_target_sys::BucketTargetSys;
pub(crate) type TonicInterceptor = ecstore_rpc::TonicInterceptor;
pub(crate) type VolumeInfo = ecstore_disk::VolumeInfo;
pub(crate) type WalkDirOptions = ecstore_disk::WalkDirOptions;

pub(crate) fn gen_tonic_signature_interceptor() -> TonicInterceptor {
    TonicInterceptor::Signature(ecstore_rpc::gen_tonic_signature_interceptor())
}

pub(crate) async fn node_service_time_out_client(
    addr: &String,
    interceptor: TonicInterceptor,
) -> Result<
    rustfs_protos::proto_gen::node_service::node_service_client::NodeServiceClient<
        tonic::service::interceptor::InterceptedService<tonic::transport::Channel, TonicInterceptor>,
    >,
    Box<dyn std::error::Error>,
> {
    ecstore_rpc::node_service_time_out_client(addr, interceptor).await
}

pub(crate) async fn node_service_time_out_client_no_auth(
    addr: &String,
) -> Result<
    rustfs_protos::proto_gen::node_service::node_service_client::NodeServiceClient<
        tonic::service::interceptor::InterceptedService<tonic::transport::Channel, TonicInterceptor>,
    >,
    Box<dyn std::error::Error>,
> {
    ecstore_rpc::node_service_time_out_client_no_auth(addr).await
}
