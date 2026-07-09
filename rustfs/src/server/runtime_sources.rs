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

use crate::runtime_sources::NotifyInterface;
use crate::storage_api::server::runtime_sources::{ECStore, EndpointServerPools};
use rustfs_kms::KmsServiceManager;
use rustfs_lock::LockClient;
use std::collections::HashMap;
use std::sync::Arc;

pub(crate) fn current_kms_runtime_service_manager() -> Option<Arc<KmsServiceManager>> {
    crate::runtime_sources::current_kms_runtime_service_manager()
}

pub(crate) fn current_server_config() -> Option<rustfs_config::server_config::Config> {
    crate::runtime_sources::current_server_config()
}

pub(crate) fn current_notify_interface() -> Arc<dyn NotifyInterface> {
    crate::runtime_sources::current_notify_interface().unwrap_or_else(crate::runtime_sources::fallback_notify_interface)
}

pub(crate) fn current_object_store_handle() -> Option<Arc<ECStore>> {
    crate::runtime_sources::current_object_store_handle()
}

pub(crate) fn current_iam_ready() -> bool {
    crate::runtime_sources::current_iam_ready()
}

pub(crate) fn current_endpoints_handle() -> Option<EndpointServerPools> {
    crate::runtime_sources::current_endpoints_handle()
}

pub(crate) fn current_lock_clients_handle() -> Option<HashMap<String, Arc<dyn LockClient>>> {
    crate::runtime_sources::current_lock_clients_handle()
}
