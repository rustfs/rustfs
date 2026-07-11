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

pub(crate) use rustfs_ecstore::api::bucket::lifecycle::{
    bucket_lifecycle_ops::{enqueue_transition_for_existing_objects, init_background_expiry},
    lifecycle::TransitionOptions,
};
pub(crate) use rustfs_ecstore::api::bucket::metadata::BUCKET_LIFECYCLE_CONFIG;
pub(crate) use rustfs_ecstore::api::bucket::metadata_sys::{
    get as get_bucket_metadata, init_bucket_metadata_sys, update as update_bucket_metadata,
};
pub(crate) use rustfs_ecstore::api::bucket::versioning_sys::BucketVersioningSys;
pub(crate) use rustfs_ecstore::api::capacity::path2_bucket_object_with_base_path;
pub(crate) use rustfs_ecstore::api::disk::{DiskOption, STORAGE_FORMAT_FILE, endpoint::Endpoint, new_disk};
pub(crate) use rustfs_ecstore::api::layout::{EndpointServerPools, Endpoints, PoolEndpoints};
pub(crate) use rustfs_ecstore::api::runtime::global_tier_config_mgr as get_global_tier_config_mgr;
pub(crate) use rustfs_ecstore::api::storage::{ECStore, init_local_disks};
// Shared lifecycle/tier test utilities (rustfs/backlog#1148 ilm-6). The mock
// backend and xl.meta assertion helpers now live in ecstore behind the
// `test-util` feature instead of being copied into this crate.
pub(crate) use rustfs_ecstore::api::tier::test_util::{
    MockWarmBackend, assert_transition_meta_consistent, free_version_count, register_mock_tier as register_mock_tier_util,
    wait_for_free_version_absence,
};
use rustfs_storage_api as storage_contracts;

pub(crate) mod lifecycle {
    pub(crate) use super::storage_contracts::{
        BucketOperations, BucketOptions, CompletePart, ListOperations, MakeBucketOptions, MultipartOperations, ObjectIO,
        ObjectOperations,
    };

    pub(crate) use super::{
        BUCKET_LIFECYCLE_CONFIG, BucketVersioningSys, DiskOption, ECStore, Endpoint, EndpointServerPools, Endpoints,
        MockWarmBackend, PoolEndpoints, STORAGE_FORMAT_FILE, TransitionOptions, assert_transition_meta_consistent,
        enqueue_transition_for_existing_objects, free_version_count, get_bucket_metadata, get_global_tier_config_mgr,
        init_background_expiry, init_bucket_metadata_sys, init_local_disks, new_disk, path2_bucket_object_with_base_path,
        register_mock_tier_util, update_bucket_metadata, wait_for_free_version_absence,
    };
}
