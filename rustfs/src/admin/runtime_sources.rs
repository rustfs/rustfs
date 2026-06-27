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

use crate::admin::storage_api::runtime::{ECStore, NotificationSys};
pub(crate) use crate::app::admin_usecase::{
    AdminPoolStatus, DefaultAdminUsecase, QueryPoolStatusRequest, QueryServerInfoRequest,
};
use crate::app::object_usecase::DefaultObjectUsecase;
pub(crate) use crate::runtime_sources::{
    AppContext, current_action_credentials, current_boot_time, current_bucket_metadata_handle, current_bucket_monitor_handle,
    current_daily_tier_stats, current_deployment_id, current_endpoints_handle, current_iam_handle,
    current_kms_runtime_service_manager, current_notification_system_for_context, current_object_store_handle_for_context,
    current_oidc_handle, current_or_init_kms_runtime_service_manager, current_outbound_tls_generation,
    current_outbound_tls_state, current_ready_iam_handle, current_region, current_replication_pool_handle,
    current_replication_stats_handle, current_runtime_port, current_scanner_metrics_report, current_server_config_for_context,
    current_tier_config_handle, current_token_signing_key, publish_server_config, publish_storage_class_config,
};
use rustfs_config::server_config::Config;
use std::sync::Arc;

#[cfg(test)]
pub(crate) use crate::runtime_sources::set_test_outbound_tls_generation;

pub(crate) fn default_admin_usecase() -> DefaultAdminUsecase {
    DefaultAdminUsecase::from_global()
}

pub(crate) fn default_object_usecase() -> DefaultObjectUsecase {
    DefaultObjectUsecase::from_global()
}

pub(crate) fn current_app_context() -> Option<Arc<AppContext>> {
    crate::runtime_sources::current_app_context()
}

pub(crate) fn current_object_store_handle() -> Option<Arc<ECStore>> {
    let context = current_app_context();
    current_object_store_handle_for_context(context.as_deref())
}

pub(crate) fn current_notification_system() -> Option<&'static NotificationSys> {
    let context = current_app_context();
    current_notification_system_for_context(context.as_deref())
}

pub(crate) fn current_server_config() -> Option<Config> {
    let context = current_app_context();
    current_server_config_for_context(context.as_deref())
}
