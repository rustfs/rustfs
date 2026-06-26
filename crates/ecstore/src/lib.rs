#![allow(dead_code)]
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

extern crate core;

#[path = "diagnostics/admin_server_info.rs"]
mod admin_server_info;
pub mod api;
#[path = "services/batch_processor.rs"]
mod batch_processor;
#[path = "io_support/bitrot.rs"]
mod bitrot;
mod bucket;
mod cache_value;
mod cluster;
#[path = "io_support/compress.rs"]
mod compress;
mod config;
mod data_movement;
#[path = "data_movement/backpressure.rs"]
mod data_movement_backpressure;
mod data_usage;
mod disk;
#[path = "layout/disks_layout_facade.rs"]
mod disks_layout;
#[path = "layout/endpoints_facade.rs"]
mod endpoints;
mod erasure_codec;
mod erasure_coding;
mod error;
#[path = "diagnostics/get.rs"]
mod get_diagnostics;
#[path = "runtime/global.rs"]
mod global;
pub(crate) mod layout;
#[path = "services/metrics_realtime.rs"]
mod metrics_realtime;
#[path = "services/notification_sys.rs"]
mod notification_sys;
mod object_api;
#[path = "core/pools.rs"]
mod pools;
mod rebalance;
#[path = "io_support/rio.rs"]
mod rio;
mod rpc;
#[path = "runtime/sources.rs"]
mod runtime_sources;
mod set_disk;
#[path = "core/sets.rs"]
mod sets;
mod storage_api_contracts;
mod store;
#[path = "store/init_format.rs"]
mod store_init;
#[path = "store/list_objects.rs"]
mod store_list_objects;
#[path = "store/utils.rs"]
mod store_utils;

// pub mod checksum;
mod client;
mod event;
#[path = "services/event_notification.rs"]
mod event_notification;
#[cfg(test)]
#[path = "core/pools_test.rs"]
mod pools_test;
#[cfg(test)]
#[path = "core/store_test.rs"]
mod store_test;
mod tier;

use rustfs_concurrency::WorkloadAdmissionSnapshotProvider;
use std::sync::Arc;

pub type WorkloadAdmissionSnapshotProviderRef = Arc<dyn WorkloadAdmissionSnapshotProvider + Send + Sync>;

pub fn set_workload_admission_snapshot_provider(
    provider: WorkloadAdmissionSnapshotProviderRef,
) -> std::result::Result<(), WorkloadAdmissionSnapshotProviderRef> {
    runtime_sources::set_workload_admission_snapshot_provider(provider)
}

#[cfg(test)]
mod rio_tests {
    #[test]
    fn uses_expected_rio_backend() {
        let expected = if cfg!(feature = "rio-v2") { "rio-v2" } else { "legacy-rio" };
        assert_eq!(crate::rio::backend_name(), expected);
    }
}
