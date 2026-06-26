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

use crate::runtime_sources::{
    NotifyInterface, resolve_endpoints_handle, resolve_iam_ready, resolve_kms_runtime_service_manager,
    resolve_lock_clients_handle, resolve_notify_interface, resolve_object_store_handle, resolve_server_config,
};
use crate::storage_api::server::{ECStore, EndpointServerPools};
use rustfs_kms::KmsServiceManager;
use rustfs_lock::LockClient;
use std::collections::HashMap;
use std::sync::Arc;

pub(crate) fn kms_runtime_service_manager() -> Option<Arc<KmsServiceManager>> {
    resolve_kms_runtime_service_manager()
}

pub(crate) fn server_config() -> Option<rustfs_config::server_config::Config> {
    resolve_server_config()
}

pub(crate) fn notify_interface() -> Arc<dyn NotifyInterface> {
    resolve_notify_interface()
}

pub(crate) fn object_store_handle() -> Option<Arc<ECStore>> {
    resolve_object_store_handle()
}

pub(crate) fn iam_ready() -> bool {
    resolve_iam_ready()
}

pub(crate) fn endpoints_handle() -> Option<EndpointServerPools> {
    resolve_endpoints_handle()
}

pub(crate) fn lock_clients_handle() -> Option<HashMap<String, Arc<dyn LockClient>>> {
    resolve_lock_clients_handle()
}
