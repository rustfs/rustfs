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

use crate::admin::storage_api::runtime_sources::{
    DailyAllTierStats, ECStore, NotificationSys, ScannerMetricsReport, StorageClassConfig, TierConfigMgr,
};
pub(crate) use crate::app::admin_usecase::{
    AdminPoolStatus, DefaultAdminUsecase, QueryPoolStatusRequest, QueryServerInfoRequest,
};
use crate::app::object_usecase::DefaultObjectUsecase;
use crate::runtime_sources as root_runtime_sources;
pub(crate) use crate::runtime_sources::{
    AppContext, current_action_credentials, current_boot_time, current_bucket_metadata_handle, current_bucket_monitor_handle,
    current_deployment_id, current_endpoints_handle, current_iam_handle, current_kms_runtime_service_manager,
    current_notification_system_for_context, current_object_store_handle_for_context, current_oidc_handle,
    current_ready_iam_handle, current_region, current_replication_pool_handle, current_replication_stats_handle,
    current_server_config_for_context, current_token_signing_key,
};
use rustfs_config::server_config::Config;
use rustfs_kms::KmsServiceManager;
use rustfs_tls_runtime::{GlobalPublishedOutboundTlsState, TlsGeneration};
use std::sync::Arc;
#[cfg(test)]
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::RwLock;

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

pub(crate) fn current_or_init_kms_runtime_service_manager() -> Arc<KmsServiceManager> {
    root_runtime_sources::current_or_init_kms_runtime_service_manager()
        .unwrap_or_else(rustfs_kms::init_global_kms_service_manager)
}

#[cfg(test)]
static TEST_OUTBOUND_TLS_GENERATION: AtomicU64 = AtomicU64::new(0);

#[cfg(test)]
pub(crate) fn set_test_outbound_tls_generation(generation: u64) {
    root_runtime_sources::set_test_outbound_tls_generation(generation);
    TEST_OUTBOUND_TLS_GENERATION.store(generation, Ordering::Relaxed);
}

pub(crate) fn current_outbound_tls_generation() -> TlsGeneration {
    root_runtime_sources::current_outbound_tls_generation().unwrap_or_else(empty_outbound_tls_generation)
}

#[cfg(test)]
fn empty_outbound_tls_generation() -> TlsGeneration {
    TlsGeneration(TEST_OUTBOUND_TLS_GENERATION.load(Ordering::Relaxed))
}

#[cfg(not(test))]
fn empty_outbound_tls_generation() -> TlsGeneration {
    TlsGeneration(0)
}

pub(crate) async fn current_outbound_tls_state() -> GlobalPublishedOutboundTlsState {
    if let Some(state) = root_runtime_sources::current_outbound_tls_state().await {
        return state;
    }

    root_runtime_sources::fallback_outbound_tls_runtime_interface().state().await
}

pub(crate) fn current_daily_tier_stats() -> DailyAllTierStats {
    root_runtime_sources::current_daily_tier_stats().unwrap_or_default()
}

pub(crate) fn current_runtime_port() -> u16 {
    root_runtime_sources::current_runtime_port().unwrap_or(rustfs_config::DEFAULT_PORT)
}

pub(crate) async fn current_scanner_metrics_report() -> ScannerMetricsReport {
    if let Some(report) = root_runtime_sources::current_scanner_metrics_report().await {
        return report;
    }

    root_runtime_sources::fallback_scanner_metrics_interface().report().await
}

pub(crate) fn current_tier_config_handle() -> Arc<RwLock<TierConfigMgr>> {
    root_runtime_sources::current_tier_config_handle().unwrap_or_else(TierConfigMgr::new)
}

pub(crate) fn publish_server_config(config: Config) {
    if !root_runtime_sources::publish_server_config(config.clone()) {
        root_runtime_sources::fallback_server_config_interface().set(config);
    }
}

pub(crate) fn publish_storage_class_config(config: StorageClassConfig) {
    if !root_runtime_sources::publish_storage_class_config(config.clone()) {
        root_runtime_sources::fallback_storage_class_interface().set(config);
    }
}
