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

#[cfg(test)]
pub(crate) use rustfs_ecstore::api::bucket::bucket_target_sys::BucketTargetSys;
#[cfg(test)]
pub(crate) use rustfs_ecstore::api::disk::{VolumeInfo, WalkDirOptions};
pub(crate) use rustfs_ecstore::api::rpc::{TonicInterceptor, node_service_time_out_client_no_auth};
#[cfg(test)]
pub(crate) use rustfs_ecstore::api::rpc::{gen_tonic_signature_interceptor, node_service_time_out_client};

#[cfg(test)]
pub(crate) mod node_interact {
    pub(crate) use super::{
        TonicInterceptor, VolumeInfo, WalkDirOptions, gen_tonic_signature_interceptor, node_service_time_out_client,
    };
}

pub(crate) mod grpc_lock {
    pub(crate) use super::{TonicInterceptor, node_service_time_out_client_no_auth};
}

#[cfg(test)]
pub(crate) mod replication_extension {
    pub(crate) use super::BucketTargetSys;
}
