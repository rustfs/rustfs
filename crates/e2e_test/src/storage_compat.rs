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

pub(crate) type BucketTargetSys = rustfs_ecstore::api::bucket::bucket_target_sys::BucketTargetSys;
pub(crate) type TonicInterceptor = rustfs_ecstore::api::rpc::TonicInterceptor;
pub(crate) type VolumeInfo = rustfs_ecstore::api::disk::VolumeInfo;
pub(crate) type WalkDirOptions = rustfs_ecstore::api::disk::WalkDirOptions;

pub(crate) use rustfs_ecstore::api::rpc::{
    gen_tonic_signature_interceptor, node_service_time_out_client, node_service_time_out_client_no_auth,
};
