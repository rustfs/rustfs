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

//! Test-only ECStore compatibility boundary for `rustfs-test-utils`.
//!
//! All direct `rustfs_ecstore` facade imports used by this crate must go
//! through this module (architecture migration rule:
//! `check_architecture_migration_rules.sh`; this file name is the sanctioned
//! test-compat boundary pattern, mirroring
//! `crates/iam/tests/ecstore_test_compat/mod.rs`). Keep the surface minimal —
//! only what the environment builder needs to assemble a temp-disk ECStore.

#[allow(unused_imports)]
pub(crate) mod fixture {
    pub(crate) use rustfs_ecstore::api::bucket::metadata_sys::init_bucket_metadata_sys;
    pub(crate) use rustfs_ecstore::api::disk::endpoint::Endpoint;
    pub(crate) use rustfs_ecstore::api::layout::{EndpointServerPools, Endpoints, PoolEndpoints};
    pub(crate) use rustfs_ecstore::api::storage::{ECStore, init_local_disks};
    pub(crate) use rustfs_storage_api::{BucketOperations, BucketOptions, MakeBucketOptions};

    #[cfg(test)]
    pub(crate) use rustfs_ecstore::api::config::com::{delete_config, read_config, save_config};
    #[cfg(test)]
    pub(crate) use rustfs_ecstore::api::data_usage::{
        load_data_usage_from_backend, load_data_usage_from_backend_cached, remove_bucket_usage_from_backend,
        store_data_usage_in_backend,
    };
    #[cfg(test)]
    pub(crate) use rustfs_ecstore::api::disk::BUCKET_META_PREFIX;
}
