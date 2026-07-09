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
use crate::runtime_sources::{
    current_outbound_tls_generation as runtime_current_outbound_tls_generation, current_replication_pool_handle,
};
use crate::storage_api::startup::runtime_sources::{DynReplicationPool, set_global_region, set_global_rustfs_port};
use rustfs_kms::KmsServiceManager;
use rustfs_obs::{GlobalError as ObservabilityError, OtelGuard};
use rustfs_tls_runtime::{OutboundTlsMaterial, TlsGeneration};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

pub(crate) fn init_action_credentials(
    access_key: String,
    secret_key: String,
) -> Result<(), rustfs_credentials::CredentialsError> {
    rustfs_credentials::init_global_action_credentials(Some(access_key), Some(secret_key))
}

pub(crate) fn publish_region(region: s3s::region::Region) {
    set_global_region(region);
}

pub(crate) fn publish_server_port(port: u16) {
    set_global_rustfs_port(port);
}

pub(crate) async fn publish_server_addr(addr: &str) {
    rustfs_common::set_global_addr(addr).await;
}

pub(crate) async fn publish_init_time_now() {
    rustfs_common::set_global_init_time_now().await;
}

pub(crate) fn init_kms_service_manager() -> Arc<KmsServiceManager> {
    rustfs_kms::init_global_kms_service_manager()
}

pub(crate) fn init_buffer_config(buffer_config: RustFSBufferConfig) {
    crate::config::init_global_buffer_config(buffer_config);
}

pub(crate) fn set_buffer_profile_enabled(enabled: bool) {
    crate::config::set_buffer_profile_enabled(enabled);
}

pub(crate) async fn init_observability_guard(obs_endpoint: String) -> Result<OtelGuard, ObservabilityError> {
    rustfs_obs::init_obs(Some(obs_endpoint)).await
}

pub(crate) fn set_observability_guard(guard: OtelGuard) -> Result<(), ObservabilityError> {
    rustfs_obs::set_global_guard(guard)
}

pub(crate) fn shutdown_observability_guard() -> Result<(), ObservabilityError> {
    rustfs_obs::shutdown_global_guard()
}

pub(crate) fn observability_metric_enabled() -> bool {
    rustfs_obs::observability_metric_enabled()
}

pub(crate) fn init_metrics_runtime(ctx: CancellationToken) {
    rustfs_obs::init_metrics_runtime(ctx);
}

pub(crate) fn replication_pool_handle() -> Option<Arc<DynReplicationPool>> {
    current_replication_pool_handle()
}

pub(crate) fn set_put_stage_metrics_enabled(enabled: bool) {
    rustfs_io_metrics::set_put_stage_metrics_enabled(enabled);
}

pub(crate) fn set_get_stage_metrics_enabled(enabled: bool) {
    rustfs_io_metrics::set_get_stage_metrics_enabled(enabled);
}

pub(crate) fn init_tls_metrics() {
    rustfs_tls_runtime::init_tls_metrics();
}

pub(crate) fn current_outbound_tls_generation() -> u64 {
    runtime_current_outbound_tls_generation()
        .map(|generation| generation.0)
        .unwrap_or_default()
}

pub(crate) async fn publish_outbound_tls_state(generation: TlsGeneration, outbound: &OutboundTlsMaterial) {
    rustfs_tls_runtime::publish_global_outbound_tls_state(generation, outbound).await;
}

pub(crate) fn record_tls_generation(consumer: &'static str, generation: u64) {
    rustfs_tls_runtime::record_tls_generation(consumer, generation);
}

pub(crate) fn record_tls_reload_result(
    consumer: &'static str,
    result: &'static str,
    duration_secs: Option<f64>,
    generation: Option<u64>,
) {
    rustfs_tls_runtime::record_tls_reload_result(consumer, result, duration_secs, generation);
}

pub(crate) fn record_tls_reload_skipped(consumer: &'static str, reason: &'static str) {
    rustfs_tls_runtime::record_tls_reload_skipped(consumer, reason);
}
