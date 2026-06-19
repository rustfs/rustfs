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

pub(crate) use rustfs_ecstore::bucket::metadata_sys::{get_global_bucket_metadata_sys, init_bucket_metadata_sys};
pub(crate) use rustfs_ecstore::bucket::migration::{try_migrate_bucket_metadata, try_migrate_iam_config};
pub(crate) use rustfs_ecstore::bucket::replication::{
    GLOBAL_REPLICATION_STATS, get_global_replication_pool, init_background_replication,
};
pub(crate) use rustfs_ecstore::bucket::{metadata, metadata_sys, quota};
pub(crate) use rustfs_ecstore::config::{com, init, init_global_config_sys, try_migrate_server_config};
pub(crate) use rustfs_ecstore::disk::{DiskAPI, RUSTFS_META_BUCKET, endpoint::Endpoint};
#[cfg(test)]
pub(crate) use rustfs_ecstore::disks_layout::DisksLayout;
pub(crate) use rustfs_ecstore::endpoints::EndpointServerPools;
#[cfg(test)]
pub(crate) use rustfs_ecstore::endpoints::{Endpoints, PoolEndpoints};
pub(crate) use rustfs_ecstore::error::{Error as EcstoreError, Result as EcstoreResult, StorageError};
pub(crate) use rustfs_ecstore::event_notification::{EventArgs as EcstoreEventArgs, register_event_dispatch_hook};
pub(crate) use rustfs_ecstore::global::{
    get_global_endpoints_opt, get_global_lock_clients, get_global_region, is_dist_erasure, resolve_object_store_handle,
    set_global_endpoints, set_global_region, set_global_rustfs_port, shutdown_background_services, update_erasure_type,
};
pub(crate) use rustfs_ecstore::notification_sys::new_global_notification_sys;
pub(crate) use rustfs_ecstore::rpc::{TONIC_RPC_PREFIX, verify_rpc_signature};
pub(crate) use rustfs_ecstore::set_disk::get_lock_acquire_timeout;
pub(crate) use rustfs_ecstore::store::{ECStore, all_local_disk, init_local_disks, init_lock_clients, prewarm_local_disk_id_map};
