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

mod client;
mod http_auth;
mod peer_rest_client;
mod peer_s3_client;
mod remote_disk;
mod remote_locker;

pub use client::{
    TonicInterceptor, gen_tonic_signature_interceptor, node_service_time_out_client, node_service_time_out_client_no_auth,
};
pub use http_auth::{TONIC_RPC_PREFIX, build_auth_headers, gen_signature_headers, verify_rpc_signature};
pub use peer_rest_client::PeerRestClient;
pub use peer_s3_client::{LocalPeerS3Client, PeerS3Client, RemotePeerS3Client, S3PeerSys};
pub use remote_disk::RemoteDisk;
pub use remote_locker::RemoteClient;
