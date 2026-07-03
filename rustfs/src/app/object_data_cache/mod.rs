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

//! App-layer object data cache adapter boundary.

mod adapter;
mod body;
mod hook;
mod invalidation;
mod planner;

pub(crate) use adapter::ObjectDataCacheAdapter;
pub(crate) use body::{
    GetObjectBodyCacheLookup, fill_get_object_body_cache_from_buffered_body, fill_get_object_body_cache_from_materialized_body,
    lookup_get_object_body_cache_hit,
};
pub(crate) use hook::register_object_data_cache_body_hook;
pub(crate) use invalidation::{
    invalidate_object_data_cache_after_complete_multipart_success, invalidate_object_data_cache_after_copy_success,
    invalidate_object_data_cache_after_delete_success, invalidate_object_data_cache_after_put_success,
    invalidate_object_data_cache_before_mutation, invalidate_object_data_cache_objects_after_delete_success,
    invalidate_object_data_cache_objects_before_mutation,
};
pub(crate) use planner::{GetObjectBodyCachePlan, GetObjectBodyCacheRequest, build_get_object_body_cache_plan};
