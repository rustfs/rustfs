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

pub(crate) use super::super::storage_compat::{
    CollectMetricsOpts, DEFAULT_READ_BUFFER_SIZE, DeleteOptions, DiskError, DiskInfoOptions, DiskStore, FileInfoVersions,
    LocalPeerS3Client, MetricType, PEER_RESTSIGNAL, PEER_RESTSUB_SYS, ReadMultipleReq, ReadMultipleResp, ReadOptions, Result,
    SERVICE_SIGNAL_REFRESH_CONFIG, SERVICE_SIGNAL_RELOAD_DYNAMIC, StorageDiskRpcExt, StoragePeerS3ClientExt, UpdateMetadataOpts,
    WalkDirOptions, all_local_disk_path, collect_local_metrics, find_local_disk_by_ref, get_global_lock_client,
    get_local_server_property, load_bucket_metadata, reload_transition_tier_config, resolve_object_store_handle,
    set_bucket_metadata, verify_rpc_signature,
};

#[cfg(test)]
pub(crate) use super::super::storage_compat::{Error, STORAGE_CLASS_SUB_SYS};
