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
use crate::app::storage_api::runtime::{ScannerMetricsReport, StorageClassConfig};
use crate::config::RustFSBufferConfig;
use crate::storage_api::server::runtime_sources::{DailyAllTierStats, ExpiryState, TierConfigMgr};
use rustfs_config::server_config::Config;
use rustfs_io_metrics::{PerformanceMetrics, internode_metrics::InternodeMetrics};
use rustfs_kms::KmsServiceManager;
use rustfs_s3select_api::{QueryResult, server::dbms::DatabaseManagerSystem};
use rustfs_tls_runtime::TlsGeneration;
use s3s::dto::SelectObjectContentInput;
use std::sync::Arc;
#[cfg(test)]
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::RwLock;

pub(crate) use context::{
    AppContext, NotifyInterface, publish_oidc_handle, resolve_action_credentials as current_action_credentials,
    resolve_boot_time as current_boot_time, resolve_bucket_metadata_handle as current_bucket_metadata_handle,
    resolve_bucket_monitor_handle as current_bucket_monitor_handle, resolve_deployment_id as current_deployment_id,
    resolve_encryption_service as current_encryption_service, resolve_endpoints_handle as current_endpoints_handle,
    resolve_iam_handle as current_iam_handle, resolve_iam_ready as current_iam_ready,
    resolve_kms_runtime_service_manager as current_kms_runtime_service_manager, resolve_lock_client as current_lock_client,
    resolve_lock_clients_handle as current_lock_clients_handle, resolve_notification_system as current_notification_system,
    resolve_notification_system_for_context as current_notification_system_for_context,
    resolve_object_store_handle as current_object_store_handle,
    resolve_object_store_handle_for_context as current_object_store_handle_for_context,
    resolve_oidc_handle as current_oidc_handle, resolve_ready_iam_handle as current_ready_iam_handle,
    resolve_region as current_region, resolve_replication_pool_handle as current_replication_pool_handle,
    resolve_replication_stats_handle as current_replication_stats_handle, resolve_server_config as current_server_config,
    resolve_server_config_for_context as current_server_config_for_context,
    resolve_token_signing_key as current_token_signing_key,
};

#[cfg(test)]
static TEST_OUTBOUND_TLS_GENERATION: AtomicU64 = AtomicU64::new(0);

#[cfg(test)]
pub(crate) fn set_test_outbound_tls_generation(generation: u64) {
    context::set_test_outbound_tls_generation(generation);
    TEST_OUTBOUND_TLS_GENERATION.store(generation, Ordering::Relaxed);
}

pub(crate) fn current_app_context() -> Option<Arc<AppContext>> {
    context::get_global_app_context()
}

pub(crate) fn current_or_init_kms_runtime_service_manager() -> Arc<KmsServiceManager> {
    context::resolve_or_init_kms_runtime_service_manager().unwrap_or_else(rustfs_kms::init_global_kms_service_manager)
}

pub(crate) fn current_notify_interface() -> Arc<dyn NotifyInterface> {
    context::resolve_notify_interface().unwrap_or_else(context::default_notify_interface)
}

pub(crate) fn current_notify_interface_for_context(context: Option<&AppContext>) -> Arc<dyn NotifyInterface> {
    context::resolve_notify_interface_for_context(context).unwrap_or_else(context::default_notify_interface)
}

pub(crate) fn current_outbound_tls_generation() -> TlsGeneration {
    context::resolve_outbound_tls_generation().unwrap_or_else(empty_outbound_tls_generation)
}

pub(crate) async fn current_outbound_tls_state() -> rustfs_tls_runtime::GlobalPublishedOutboundTlsState {
    if let Some(state) = context::resolve_outbound_tls_state().await {
        return state;
    }

    context::default_outbound_tls_runtime_interface().state().await
}

#[cfg(test)]
fn empty_outbound_tls_generation() -> TlsGeneration {
    TlsGeneration(TEST_OUTBOUND_TLS_GENERATION.load(Ordering::Relaxed))
}

#[cfg(not(test))]
fn empty_outbound_tls_generation() -> TlsGeneration {
    TlsGeneration(0)
}

pub(crate) fn current_daily_tier_stats() -> DailyAllTierStats {
    context::resolve_daily_tier_stats().unwrap_or_default()
}

pub(crate) fn current_runtime_port() -> u16 {
    context::resolve_runtime_port().unwrap_or(rustfs_config::DEFAULT_PORT)
}

pub(crate) fn current_performance_metrics() -> Arc<PerformanceMetrics> {
    context::resolve_performance_metrics().unwrap_or_else(|| Arc::new(PerformanceMetrics::new()))
}

pub(crate) fn current_internode_metrics() -> Arc<InternodeMetrics> {
    context::resolve_internode_metrics().unwrap_or_else(|| Arc::new(InternodeMetrics::default()))
}

pub(crate) async fn current_scanner_metrics_report() -> ScannerMetricsReport {
    if let Some(report) = context::resolve_scanner_metrics_report().await {
        return report;
    }

    context::default_scanner_metrics_interface().report().await
}

pub(crate) async fn current_s3select_db(
    input: SelectObjectContentInput,
    enable_debug: bool,
) -> QueryResult<Arc<dyn DatabaseManagerSystem + Send + Sync>> {
    if let Some(result) = context::resolve_s3select_db(input.clone(), enable_debug).await {
        return result;
    }

    context::default_s3select_db_interface().get(input, enable_debug).await
}

pub(crate) async fn current_local_node_name() -> String {
    context::resolve_local_node_name().await.unwrap_or_default()
}

pub(crate) fn current_tier_config_handle() -> Arc<RwLock<TierConfigMgr>> {
    context::resolve_tier_config_handle().unwrap_or_else(TierConfigMgr::new)
}

pub(crate) fn current_expiry_state_handle() -> Arc<RwLock<ExpiryState>> {
    context::resolve_expiry_state_handle().unwrap_or_else(ExpiryState::new)
}

pub(crate) fn current_buffer_config() -> RustFSBufferConfig {
    context::resolve_buffer_config().unwrap_or_default()
}

pub(crate) fn publish_server_config(config: Config) {
    if !context::publish_server_config(config.clone()) {
        context::default_server_config_interface().set(config);
    }
}

pub(crate) fn publish_storage_class_config(config: StorageClassConfig) {
    if !context::publish_storage_class_config(config.clone()) {
        context::default_storage_class_interface().set(config);
    }
}
