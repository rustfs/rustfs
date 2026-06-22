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

pub(crate) use super::super::storage_compat::{
    ECStore, EndpointServerPools, TierConfigMgr, get_global_endpoints_opt, get_global_region, get_global_tier_config_mgr,
    new_object_layer_fn, set_object_store_resolver,
};

pub(crate) mod metadata_sys {
    pub(crate) use super::super::super::storage_compat::metadata_sys::{BucketMetadataSys, get_global_bucket_metadata_sys};
}

#[cfg(test)]
pub(crate) use super::super::storage_compat::{Endpoint, Endpoints, PoolEndpoints, init_local_disks};
