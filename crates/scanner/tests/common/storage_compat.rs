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

#![allow(dead_code, unused_imports)]

pub(crate) const BUCKET_LIFECYCLE_CONFIG: &str = rustfs_ecstore::bucket::metadata::BUCKET_LIFECYCLE_CONFIG;
pub(crate) const STORAGE_FORMAT_FILE: &str = rustfs_ecstore::disk::STORAGE_FORMAT_FILE;

pub(crate) type TransitionOptions = rustfs_ecstore::bucket::lifecycle::lifecycle::TransitionOptions;

pub(crate) use rustfs_ecstore::api::capacity::path2_bucket_object_with_base_path;
pub(crate) use rustfs_ecstore::api::layout::{EndpointServerPools, Endpoints, PoolEndpoints};
pub(crate) use rustfs_ecstore::api::storage::ECStore;
pub(crate) use rustfs_ecstore::api::storage::init_local_disks;
pub(crate) use rustfs_ecstore::bucket::lifecycle::bucket_lifecycle_ops::{
    enqueue_transition_for_existing_objects, init_background_expiry,
};
pub(crate) use rustfs_ecstore::bucket::metadata_sys::{
    get as get_bucket_metadata, init_bucket_metadata_sys, update as update_bucket_metadata,
};
pub(crate) use rustfs_ecstore::bucket::versioning_sys::BucketVersioningSys;
pub(crate) use rustfs_ecstore::client::transition_api::{ReadCloser, ReaderImpl};
pub(crate) use rustfs_ecstore::disk::{DiskAPI, DiskOption, endpoint::Endpoint, new_disk};
pub(crate) use rustfs_ecstore::global::GLOBAL_TierConfigMgr;
pub(crate) use rustfs_ecstore::tier::tier_config::{TierConfig, TierMinIO, TierType};
pub(crate) use rustfs_ecstore::tier::warm_backend::WarmBackendGetOpts;
pub(crate) use rustfs_ecstore::tier::warm_backend::{WarmBackend, build_transition_put_options};
