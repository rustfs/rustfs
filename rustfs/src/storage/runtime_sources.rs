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

use crate::app::context;
use crate::config::RustFSBufferConfig;
use crate::storage::ECStore;
use rustfs_credentials::Credentials;
use rustfs_iam::{error::Result as IamResult, store::object::ObjectStore, sys::IamSys};
use rustfs_io_metrics::{PerformanceMetrics, internode_metrics::InternodeMetrics};
use rustfs_kms::ObjectEncryptionService;
use rustfs_lock::LockClient;
use std::sync::Arc;

pub(crate) use context::{AppContext, get_global_app_context};

pub(crate) fn object_store_handle() -> Option<Arc<ECStore>> {
    context::resolve_object_store_handle()
}

pub(crate) fn object_store_handle_for_context(context: Option<&AppContext>) -> Option<Arc<ECStore>> {
    context::resolve_object_store_handle_for_context(context)
}

pub(crate) fn buffer_config() -> RustFSBufferConfig {
    context::resolve_buffer_config()
}

pub(crate) fn internode_metrics() -> Arc<InternodeMetrics> {
    context::resolve_internode_metrics()
}

pub(crate) async fn local_node_name() -> String {
    context::resolve_local_node_name().await
}

pub(crate) fn action_credentials() -> Option<Credentials> {
    context::resolve_action_credentials()
}

pub(crate) fn notify_interface() -> Arc<dyn context::NotifyInterface> {
    context::resolve_notify_interface()
}

pub(crate) fn performance_metrics() -> Arc<PerformanceMetrics> {
    context::resolve_performance_metrics()
}

pub(crate) async fn encryption_service() -> Option<Arc<ObjectEncryptionService>> {
    context::resolve_encryption_service().await
}

pub(crate) fn region() -> Option<s3s::region::Region> {
    context::resolve_region()
}

pub(crate) fn ready_iam_handle() -> IamResult<Arc<IamSys<ObjectStore>>> {
    context::resolve_ready_iam_handle()
}

pub(crate) fn iam_handle() -> Option<Arc<IamSys<ObjectStore>>> {
    context::resolve_iam_handle()
}

pub(crate) fn lock_client() -> Option<Arc<dyn LockClient>> {
    context::resolve_lock_client()
}
