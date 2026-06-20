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

//! Explicit ECStore public facades for outer crate compatibility boundaries.

pub mod admin {
    pub use crate::admin_server_info::{get_local_server_property, get_server_info};
}

pub mod capacity {
    pub use crate::pools::{
        PoolDecommissionInfo, PoolStatus, get_total_usable_capacity, get_total_usable_capacity_free, path2_bucket_object,
        path2_bucket_object_with_base_path,
    };
    pub use crate::store_utils::is_reserved_or_invalid_bucket;
}

pub mod layout {
    pub use crate::disks_layout::DisksLayout;
    pub use crate::endpoints::{EndpointServerPools, Endpoints, PoolEndpoints};
}

pub mod metrics {
    pub use crate::metrics_realtime::{CollectMetricsOpts, MetricType, collect_local_metrics};
}

pub mod notification {
    pub use crate::notification_sys::{
        NotificationPeerErr, NotificationSys, get_global_notification_sys, new_global_notification_sys,
    };
}

pub mod storage {
    pub use crate::store::{
        ECStore, all_local_disk, all_local_disk_path, find_local_disk_by_ref, init_local_disks, init_lock_clients,
        prewarm_local_disk_id_map,
    };
}
