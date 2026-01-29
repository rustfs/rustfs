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

pub mod admin_server_info;
pub mod batch_processor;
pub mod bitrot;
pub mod bucket;
pub mod cache_value;
pub mod compress;
pub mod config;
pub mod data_usage;
pub mod disk;
pub mod disks_layout;
pub mod endpoints;
pub mod erasure_coding;
pub mod error;
pub mod file_cache;
pub mod global;
pub mod metrics_realtime;
pub mod notification_sys;
pub mod pools;
pub mod rebalance;
pub mod rpc;
pub mod set_disk;
mod sets;
pub mod store;
pub mod store_api;
mod store_init;
pub mod store_list_objects;
pub mod store_utils;

// pub mod checksum;
pub mod client;
pub mod event;
pub mod event_notification;
#[cfg(test)]
mod pools_test;
#[cfg(test)]
mod store_test;
pub mod tier;

pub use global::new_object_layer_fn;
pub use global::set_global_endpoints;
pub use global::update_erasure_type;

pub use global::GLOBAL_Endpoints;
pub use store_api::StorageAPI;
