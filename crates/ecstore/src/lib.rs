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

pub mod api;
mod bucket;
mod cache_value;
mod cluster;
mod config;
mod data_movement;
#[path = "data_movement/backpressure.rs"]
mod data_movement_backpressure;
mod data_usage;
mod diagnostics;
mod disk;
#[path = "layout/disks_layout_facade.rs"]
mod disks_layout;
#[path = "layout/endpoints_facade.rs"]
mod endpoints;
mod erasure_codec;
mod erasure_coding;
mod error;
mod io_support;
pub(crate) mod layout;
mod object_api;
#[path = "core/pools.rs"]
mod pools;
mod rebalance;
mod rpc;
mod runtime;
mod services;
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
    runtime::sources::set_workload_admission_snapshot_provider(provider)
}

#[cfg(test)]
mod rio_tests {
    #[test]
    fn uses_expected_rio_backend() {
        let expected = if cfg!(feature = "rio-v2") { "rio-v2" } else { "legacy-rio" };
        assert_eq!(crate::io_support::rio::backend_name(), expected);
    }
}
