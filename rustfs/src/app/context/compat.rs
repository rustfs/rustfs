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

use super::global::get_global_app_context;
use super::handles::{
    default_bucket_metadata_interface, default_endpoints_interface, default_kms_runtime_interface,
    default_server_config_interface, default_tier_config_interface,
};
use rustfs_config::server_config::Config;
use rustfs_ecstore::bucket::metadata_sys::BucketMetadataSys;
use rustfs_ecstore::endpoints::EndpointServerPools;
use rustfs_ecstore::store::ECStore;
use rustfs_ecstore::tier::tier::TierConfigMgr;
use rustfs_kms::KmsServiceManager;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Resolve KMS runtime service manager using AppContext-first precedence.
pub fn resolve_kms_runtime_service_manager() -> Option<Arc<KmsServiceManager>> {
    get_global_app_context()
        .and_then(|context| context.kms_runtime().service_manager())
        .or_else(|| default_kms_runtime_interface().service_manager())
}

/// Resolve bucket metadata handle using AppContext-first precedence.
pub fn resolve_bucket_metadata_handle() -> Option<Arc<RwLock<BucketMetadataSys>>> {
    get_global_app_context()
        .and_then(|context| context.bucket_metadata().handle())
        .or_else(|| default_bucket_metadata_interface().handle())
}

/// Resolve object store handle from AppContext.
pub fn resolve_object_store_handle() -> Option<Arc<ECStore>> {
    get_global_app_context().map(|context| context.object_store())
}

/// Resolve endpoints using AppContext-first precedence.
pub fn resolve_endpoints_handle() -> Option<EndpointServerPools> {
    get_global_app_context()
        .and_then(|context| context.endpoints().handle())
        .or_else(|| default_endpoints_interface().handle())
}

/// Resolve tier config handle using AppContext-first precedence.
pub fn resolve_tier_config_handle() -> Arc<RwLock<TierConfigMgr>> {
    get_global_app_context()
        .map(|context| context.tier_config().handle())
        .unwrap_or_else(|| default_tier_config_interface().handle())
}

/// Resolve server config using AppContext-first precedence.
pub fn resolve_server_config() -> Option<Config> {
    match get_global_app_context() {
        Some(context) => context.server_config().get(),
        None => default_server_config_interface().get(),
    }
}
