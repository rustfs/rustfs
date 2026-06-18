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

pub(crate) mod ecstore {
    pub(crate) use rustfs_ecstore::global::{resolve_object_store_handle, set_global_endpoints, update_erasure_type};

    pub(crate) mod bucket {
        pub(crate) use rustfs_ecstore::bucket::{metadata, metadata_sys, migration, quota, replication};
    }

    pub(crate) mod config {
        pub(crate) use rustfs_ecstore::config::{com, init, init_global_config_sys, try_migrate_server_config};
    }

    pub(crate) mod disk {
        pub(crate) use rustfs_ecstore::disk::{DiskAPI, RUSTFS_META_BUCKET, endpoint};
    }

    #[cfg(test)]
    pub(crate) mod disks_layout {
        pub(crate) use rustfs_ecstore::disks_layout::DisksLayout;
    }

    pub(crate) mod endpoints {
        pub(crate) use rustfs_ecstore::endpoints::EndpointServerPools;
        #[cfg(test)]
        pub(crate) use rustfs_ecstore::endpoints::{Endpoints, PoolEndpoints};
    }

    pub(crate) mod error {
        pub(crate) use rustfs_ecstore::error::{Error, Result, StorageError};
    }

    pub(crate) mod event_notification {
        pub(crate) use rustfs_ecstore::event_notification::{EventArgs, register_event_dispatch_hook};
    }

    pub(crate) mod global {
        pub(crate) use rustfs_ecstore::global::{
            get_global_endpoints_opt, get_global_lock_clients, get_global_region, is_dist_erasure, set_global_region,
            set_global_rustfs_port, shutdown_background_services,
        };
    }

    pub(crate) mod notification_sys {
        pub(crate) use rustfs_ecstore::notification_sys::new_global_notification_sys;
    }

    pub(crate) mod rpc {
        pub(crate) use rustfs_ecstore::rpc::{TONIC_RPC_PREFIX, verify_rpc_signature};
    }

    pub(crate) mod set_disk {
        pub(crate) use rustfs_ecstore::set_disk::get_lock_acquire_timeout;
    }

    pub(crate) mod store {
        pub(crate) use rustfs_ecstore::store::{
            ECStore, all_local_disk, init_local_disks, init_lock_clients, prewarm_local_disk_id_map,
        };
    }
}
