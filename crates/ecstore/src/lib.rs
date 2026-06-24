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

mod admin_server_info;
pub mod api;
mod batch_processor;
mod bitrot;
mod bucket;
mod cache_value;
mod cluster;
mod compress;
mod config;
mod data_movement;
mod data_usage;
mod disk;
mod disks_layout;
mod endpoints;
mod erasure_codec;
mod erasure_coding;
mod error;
mod get_diagnostics;
mod global;
pub(crate) mod layout;
mod metrics_realtime;
mod notification_sys;
mod object_api;
mod pools;
mod rebalance;
mod rio;
mod rpc;
mod set_disk;
mod sets;
mod storage_api_contracts;
mod store;
mod store_init;
mod store_list_objects;
mod store_utils;

// pub mod checksum;
mod client;
mod event;
mod event_notification;
#[cfg(test)]
mod pools_test;
#[cfg(test)]
mod store_test;
mod tier;

#[cfg(test)]
mod rio_tests {
    #[test]
    fn uses_expected_rio_backend() {
        let expected = if cfg!(feature = "rio-v2") { "rio-v2" } else { "legacy-rio" };
        assert_eq!(crate::rio::backend_name(), expected);
    }
}
