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

use crate::app::object_data_cache::ObjectDataCacheAdapter;
use crate::app::storage_api::runtime_sources::ExpiryState;
#[cfg(test)]
use crate::app::storage_api::runtime_sources::TierConfigMgr;
use crate::runtime_sources as root_runtime_sources;
pub(crate) use crate::runtime_sources::{
    AppContext, current_encryption_service, current_endpoints_handle, current_notification_system,
    current_object_data_cache_handle_for_context, current_object_store_handle_for_context,
};
use rustfs_s3select_api::{QueryResult, server::dbms::DatabaseManagerSystem};
use s3s::dto::SelectObjectContentInput;
use std::sync::Arc;
use tokio::sync::RwLock;

pub(crate) fn current_app_context() -> Option<Arc<AppContext>> {
    crate::runtime_sources::current_app_context()
}

pub(crate) fn current_notify_interface_for_context(
    app_context: Option<&AppContext>,
) -> Arc<dyn root_runtime_sources::NotifyInterface> {
    root_runtime_sources::current_notify_interface_for_context(app_context)
        .unwrap_or_else(root_runtime_sources::fallback_notify_interface)
}

pub(crate) fn current_object_data_cache_for_context(app_context: Option<&AppContext>) -> Arc<ObjectDataCacheAdapter> {
    current_object_data_cache_handle_for_context(app_context)
        .unwrap_or_else(root_runtime_sources::fallback_object_data_cache_handle)
}

pub(crate) async fn current_s3select_db(
    input: SelectObjectContentInput,
    enable_debug: bool,
) -> QueryResult<Arc<dyn DatabaseManagerSystem + Send + Sync>> {
    if let Some(result) = root_runtime_sources::current_s3select_db(input.clone(), enable_debug).await {
        return result;
    }

    root_runtime_sources::fallback_s3select_db_interface()
        .get(input, enable_debug)
        .await
}

pub(crate) fn current_expiry_state_handle() -> Arc<RwLock<ExpiryState>> {
    root_runtime_sources::current_expiry_state_handle().unwrap_or_else(ExpiryState::new)
}

#[cfg(test)]
pub(crate) fn current_tier_config_handle() -> Arc<RwLock<TierConfigMgr>> {
    root_runtime_sources::current_tier_config_handle().unwrap_or_else(TierConfigMgr::new)
}
