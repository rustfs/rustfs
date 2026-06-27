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

use crate::config::RustFSBufferConfig;
use crate::runtime_sources as root_runtime_sources;
use crate::storage::storage_api::runtime_sources_consumer::ECStore;
use rustfs_credentials::Credentials;
use rustfs_iam::{error::Result as IamResult, store::object::ObjectStore, sys::IamSys};
use rustfs_io_metrics::{PerformanceMetrics, internode_metrics::InternodeMetrics};
use rustfs_kms::ObjectEncryptionService;
use rustfs_lock::LockClient;
use std::sync::Arc;

pub(crate) use crate::runtime_sources::AppContext;

pub(crate) fn current_app_context() -> Option<Arc<AppContext>> {
    root_runtime_sources::current_app_context()
}

pub(crate) fn current_object_store_handle() -> Option<Arc<ECStore>> {
    root_runtime_sources::current_object_store_handle()
}

pub(crate) fn current_object_store_handle_for_context(context: Option<&AppContext>) -> Option<Arc<ECStore>> {
    root_runtime_sources::current_object_store_handle_for_context(context)
}

pub(crate) fn current_buffer_config() -> RustFSBufferConfig {
    root_runtime_sources::current_buffer_config()
}

pub(crate) fn current_internode_metrics() -> Arc<InternodeMetrics> {
    root_runtime_sources::current_internode_metrics()
}

pub(crate) async fn current_local_node_name() -> String {
    root_runtime_sources::current_local_node_name().await
}

pub(crate) fn current_action_credentials() -> Option<Credentials> {
    root_runtime_sources::current_action_credentials()
}

pub(crate) fn current_notify_interface() -> Arc<dyn root_runtime_sources::NotifyInterface> {
    root_runtime_sources::current_notify_interface()
}

pub(crate) fn current_performance_metrics() -> Arc<PerformanceMetrics> {
    root_runtime_sources::current_performance_metrics()
}

pub(crate) async fn current_encryption_service() -> Option<Arc<ObjectEncryptionService>> {
    root_runtime_sources::current_encryption_service().await
}

pub(crate) fn current_region() -> Option<s3s::region::Region> {
    root_runtime_sources::current_region()
}

pub(crate) fn current_ready_iam_handle() -> IamResult<Arc<IamSys<ObjectStore>>> {
    root_runtime_sources::current_ready_iam_handle()
}

pub(crate) fn current_iam_handle() -> Option<Arc<IamSys<ObjectStore>>> {
    root_runtime_sources::current_iam_handle()
}

pub(crate) fn current_lock_client() -> Option<Arc<dyn LockClient>> {
    root_runtime_sources::current_lock_client()
}
