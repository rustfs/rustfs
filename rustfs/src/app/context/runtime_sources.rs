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

use super::super::storage_api::bucket::metadata_sys::BucketMetadataSys;
use super::super::storage_api::runtime::{
    BucketBandwidthMonitor, DailyAllTierStats, DynReplicationPool, ExpiryState, NotificationSys, ReplicationStats,
    ScannerMetricsReport, StorageClassConfig, TierConfigMgr, collect_scanner_metrics_report, get_daily_all_tier_stats,
    get_global_boot_time, get_global_bucket_monitor, get_global_deployment_id, get_global_endpoints_opt, get_global_expiry_state,
    get_global_lock_client, get_global_lock_clients, get_global_notification_sys, get_global_region, get_global_replication_pool,
    get_global_replication_stats, get_global_tier_config_mgr, global_rustfs_port, new_object_layer_fn, set_global_storage_class,
};
use super::super::storage_api::{ECStore, EndpointServerPools};
use crate::config::{RustFSBufferConfig, get_global_buffer_config};
use rustfs_config::server_config::{Config, get_global_server_config, set_global_server_config};
use rustfs_credentials::{Credentials, get_global_action_cred};
use rustfs_iam::{error::Result as IamResult, oidc::OidcSys, store::object::ObjectStore, sys::IamSys};
use rustfs_io_metrics::{
    PerformanceMetrics,
    global_metrics::get_global_metrics,
    internode_metrics::{InternodeMetrics, global_internode_metrics},
};
use rustfs_kms::{KmsServiceManager, ObjectEncryptionService};
use rustfs_lock::LockClient;
use rustfs_notify::{EventArgs, NotificationError, notifier_global};
use rustfs_s3select_api::{QueryResult, server::dbms::DatabaseManagerSystem};
use rustfs_targets::{EventName, arn::TargetID};
use rustfs_tls_runtime::{
    GlobalPublishedOutboundTlsState, TlsGeneration, load_global_outbound_tls_generation, load_global_outbound_tls_state,
};
use s3s::dto::SelectObjectContentInput;
use std::{collections::HashMap, sync::Arc, time::SystemTime};
use tokio::sync::RwLock;

pub fn kms_service_manager() -> Option<Arc<KmsServiceManager>> {
    rustfs_kms::get_global_kms_service_manager()
}

pub fn init_kms_service_manager() -> Arc<KmsServiceManager> {
    rustfs_kms::init_global_kms_service_manager()
}

pub async fn encryption_service() -> Option<Arc<ObjectEncryptionService>> {
    rustfs_kms::get_global_encryption_service().await
}

pub fn outbound_tls_generation() -> TlsGeneration {
    load_global_outbound_tls_generation()
}

#[cfg(test)]
pub fn set_outbound_tls_generation(generation: u64) {
    rustfs_common::set_global_outbound_tls_generation(generation);
}

pub async fn outbound_tls_state() -> GlobalPublishedOutboundTlsState {
    load_global_outbound_tls_state().await
}

pub fn iam_is_ready() -> bool {
    rustfs_iam::get_global_iam_sys().is_some_and(|sys| sys.is_ready())
}

pub fn iam_handle() -> Option<Arc<IamSys<ObjectStore>>> {
    rustfs_iam::get_global_iam_sys()
}

pub fn ready_iam_handle() -> IamResult<Arc<IamSys<ObjectStore>>> {
    rustfs_iam::get()
}

pub fn oidc_handle() -> Option<Arc<OidcSys>> {
    rustfs_iam::get_oidc()
}

pub fn token_signing_key() -> Option<String> {
    rustfs_iam::manager::get_token_signing_key()
}

pub async fn notify(args: EventArgs) {
    notifier_global::notify(args).await;
}

pub async fn add_event_specific_rules(
    bucket_name: &str,
    region: &str,
    event_rules: &[(Vec<EventName>, String, String, Vec<TargetID>)],
) -> Result<(), NotificationError> {
    notifier_global::add_event_specific_rules(bucket_name, region, event_rules).await
}

pub async fn clear_bucket_notification_rules(bucket_name: &str) -> Result<(), NotificationError> {
    notifier_global::clear_bucket_notification_rules(bucket_name).await
}

pub fn notification_system() -> Option<&'static NotificationSys> {
    get_global_notification_sys()
}

pub fn bucket_metadata() -> Option<Arc<RwLock<BucketMetadataSys>>> {
    super::super::storage_api::bucket::metadata_sys::get_global_bucket_metadata_sys()
}

pub fn bucket_monitor() -> Option<Arc<BucketBandwidthMonitor>> {
    get_global_bucket_monitor()
}

pub fn replication_pool() -> Option<Arc<DynReplicationPool>> {
    get_global_replication_pool()
}

pub fn replication_stats() -> Option<Arc<ReplicationStats>> {
    get_global_replication_stats()
}

pub fn boot_time() -> Option<SystemTime> {
    get_global_boot_time()
}

pub fn daily_tier_stats() -> DailyAllTierStats {
    get_daily_all_tier_stats()
}

pub async fn scanner_metrics_report() -> ScannerMetricsReport {
    collect_scanner_metrics_report().await
}

pub fn endpoints() -> Option<EndpointServerPools> {
    get_global_endpoints_opt()
}

pub fn deployment_id() -> Option<String> {
    get_global_deployment_id()
}

pub fn runtime_port() -> u16 {
    global_rustfs_port()
}

pub fn lock_client() -> Option<Arc<dyn LockClient>> {
    get_global_lock_client()
}

pub fn lock_clients() -> Option<HashMap<String, Arc<dyn LockClient>>> {
    get_global_lock_clients().cloned()
}

pub fn performance_metrics() -> Arc<PerformanceMetrics> {
    get_global_metrics()
}

pub fn internode_metrics() -> Arc<InternodeMetrics> {
    global_internode_metrics().clone()
}

pub async fn s3select_db(
    input: SelectObjectContentInput,
    enable_debug: bool,
) -> QueryResult<Arc<dyn DatabaseManagerSystem + Send + Sync>> {
    rustfs_s3select_query::get_global_db(input, enable_debug).await
}

pub async fn local_node_name() -> String {
    rustfs_common::get_global_local_node_name().await
}

pub fn action_credentials() -> Option<Credentials> {
    get_global_action_cred()
}

pub fn region() -> Option<s3s::region::Region> {
    get_global_region()
}

pub fn tier_config() -> Arc<RwLock<TierConfigMgr>> {
    get_global_tier_config_mgr()
}

pub fn expiry_state() -> Arc<RwLock<ExpiryState>> {
    get_global_expiry_state()
}

pub fn server_config() -> Option<Config> {
    get_global_server_config()
}

pub fn set_server_config(config: Config) {
    set_global_server_config(config);
}

pub fn set_storage_class(config: StorageClassConfig) {
    set_global_storage_class(config);
}

pub fn buffer_config() -> RustFSBufferConfig {
    get_global_buffer_config().clone()
}

pub fn object_store() -> Option<Arc<ECStore>> {
    new_object_layer_fn()
}
