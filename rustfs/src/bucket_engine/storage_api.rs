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

//! Storage capabilities used by the bucket engine registry.

pub(super) use crate::storage_api::bucket_engine::{
    BUCKET_ENGINE_CONFIG, BucketMetadata, ECStore, ListOperations, capture_bucket_metadata_incarnation, delete_if_incarnation,
    get_bucket_metadata, get_global_endpoints_opt, get_global_notification_sys, register_bucket_config_subscriber,
    update_if_incarnation,
};
