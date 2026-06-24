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

use super::super::EndpointServerPools;
use super::super::StorageClassConfig;
use super::super::TierConfigMgr;
use super::super::metadata_sys::{BucketMetadataSys, get_global_bucket_metadata_sys};
use super::super::{
    collect_scanner_metrics_report, get_daily_all_tier_stats, get_global_boot_time, get_global_bucket_monitor,
    get_global_deployment_id, get_global_endpoints_opt, get_global_expiry_state, get_global_lock_client, get_global_lock_clients,
    get_global_notification_sys, get_global_region, get_global_replication_pool, get_global_replication_stats,
    get_global_tier_config_mgr, global_rustfs_port, set_global_storage_class,
};
use super::interfaces::{
    ActionCredentialInterface, BootTimeInterface, BucketMetadataInterface, BucketMonitorInterface, BufferConfigInterface,
    DeploymentIdInterface, EndpointsInterface, ExpiryStateInterface, IamInterface, InternodeMetricsInterface, KmsInterface,
    KmsRuntimeInterface, LocalNodeNameInterface, LockClientInterface, LockClientsInterface, NotificationSystemInterface,
    NotifyInterface, OidcInterface, OutboundTlsRuntimeInterface, PerformanceMetricsInterface, RegionInterface,
    ReplicationPoolInterface, ReplicationStatsInterface, RuntimePortInterface, S3SelectDbInterface, ScannerMetricsInterface,
    ServerConfigInterface, StorageClassInterface, TierConfigInterface, TierStatsInterface,
};
use crate::config::{RustFSBufferConfig, get_global_buffer_config};
use async_trait::async_trait;
use rustfs_common::get_global_local_node_name;
use rustfs_config::server_config::Config;
use rustfs_config::server_config::{get_global_server_config, set_global_server_config};
use rustfs_credentials::{Credentials, get_global_action_cred};
use rustfs_iam::{get_oidc, oidc::OidcSys, store::object::ObjectStore, sys::IamSys};
use rustfs_io_metrics::{
    PerformanceMetrics,
    global_metrics::get_global_metrics,
    internode_metrics::{InternodeMetrics, global_internode_metrics},
};
use rustfs_kms::{KmsServiceManager, get_global_kms_service_manager};
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

/// Default IAM interface adapter.
pub struct IamHandle {
    #[allow(dead_code)]
    iam: Arc<IamSys<ObjectStore>>,
}

impl IamHandle {
    pub fn new(iam: Arc<IamSys<ObjectStore>>) -> Self {
        Self { iam }
    }
}

impl IamInterface for IamHandle {
    fn handle(&self) -> Arc<IamSys<ObjectStore>> {
        self.iam.clone()
    }

    fn is_ready(&self) -> bool {
        rustfs_iam::get().is_ok()
    }

    fn oidc(&self) -> Option<Arc<OidcSys>> {
        rustfs_iam::get_oidc()
    }

    fn token_signing_key(&self) -> Option<String> {
        rustfs_iam::manager::get_token_signing_key()
    }
}

/// Default OIDC interface adapter.
#[derive(Default)]
pub struct OidcHandle;

impl OidcInterface for OidcHandle {
    fn handle(&self) -> Option<Arc<OidcSys>> {
        get_oidc()
    }
}

/// Default KMS interface adapter.
#[allow(dead_code)]
pub struct KmsHandle {
    kms: Arc<KmsServiceManager>,
}

impl KmsHandle {
    pub fn new(kms: Arc<KmsServiceManager>) -> Self {
        Self { kms }
    }
}

impl KmsInterface for KmsHandle {
    fn handle(&self) -> Arc<KmsServiceManager> {
        self.kms.clone()
    }
}

/// Default KMS runtime interface adapter.
#[derive(Default)]
pub struct KmsRuntimeHandle;

impl KmsRuntimeInterface for KmsRuntimeHandle {
    fn service_manager(&self) -> Option<Arc<KmsServiceManager>> {
        get_global_kms_service_manager()
    }
}

/// Default outbound TLS runtime interface adapter.
#[derive(Default)]
pub struct OutboundTlsRuntimeHandle;

#[async_trait]
impl OutboundTlsRuntimeInterface for OutboundTlsRuntimeHandle {
    fn generation(&self) -> TlsGeneration {
        load_global_outbound_tls_generation()
    }

    async fn state(&self) -> GlobalPublishedOutboundTlsState {
        load_global_outbound_tls_state().await
    }
}

/// Default notify interface adapter.
#[derive(Default)]
pub struct NotifyHandle;

#[async_trait]
impl NotifyInterface for NotifyHandle {
    async fn notify(&self, args: EventArgs) {
        notifier_global::notify(args).await;
    }

    async fn add_event_specific_rules(
        &self,
        bucket_name: &str,
        region: &str,
        event_rules: &[(Vec<EventName>, String, String, Vec<TargetID>)],
    ) -> Result<(), NotificationError> {
        notifier_global::add_event_specific_rules(bucket_name, region, event_rules).await
    }

    async fn clear_bucket_notification_rules(&self, bucket_name: &str) -> Result<(), NotificationError> {
        notifier_global::clear_bucket_notification_rules(bucket_name).await
    }
}

/// Default notification system handle adapter.
#[derive(Default)]
pub struct NotificationSystemHandle;

impl NotificationSystemInterface for NotificationSystemHandle {
    fn handle(&self) -> Option<&'static super::super::NotificationSys> {
        get_global_notification_sys()
    }
}

/// Default bucket metadata interface adapter.
#[derive(Default)]
pub struct BucketMetadataHandle;

impl BucketMetadataInterface for BucketMetadataHandle {
    fn handle(&self) -> Option<Arc<RwLock<BucketMetadataSys>>> {
        get_global_bucket_metadata_sys()
    }
}

/// Default bucket monitor interface adapter.
#[derive(Default)]
pub struct BucketMonitorHandle;

impl BucketMonitorInterface for BucketMonitorHandle {
    fn handle(&self) -> Option<Arc<super::super::BucketBandwidthMonitor>> {
        get_global_bucket_monitor()
    }
}

/// Default replication pool interface adapter.
#[derive(Default)]
pub struct ReplicationPoolHandle;

impl ReplicationPoolInterface for ReplicationPoolHandle {
    fn handle(&self) -> Option<Arc<super::super::DynReplicationPool>> {
        get_global_replication_pool()
    }
}

/// Default replication statistics interface adapter.
#[derive(Default)]
pub struct ReplicationStatsHandle;

impl ReplicationStatsInterface for ReplicationStatsHandle {
    fn handle(&self) -> Option<Arc<super::super::ReplicationStats>> {
        get_global_replication_stats()
    }
}

/// Default boot time interface adapter.
#[derive(Default)]
pub struct BootTimeHandle;

impl BootTimeInterface for BootTimeHandle {
    fn get(&self) -> Option<SystemTime> {
        get_global_boot_time()
    }
}

/// Default tier transition statistics interface adapter.
#[derive(Default)]
pub struct TierStatsHandle;

impl TierStatsInterface for TierStatsHandle {
    fn daily_all(&self) -> super::super::DailyAllTierStats {
        get_daily_all_tier_stats()
    }
}

/// Default scanner metrics report interface adapter.
#[derive(Default)]
pub struct ScannerMetricsHandle;

#[async_trait]
impl ScannerMetricsInterface for ScannerMetricsHandle {
    async fn report(&self) -> super::super::ScannerMetricsReport {
        collect_scanner_metrics_report().await
    }
}

/// Default endpoints interface adapter.
#[derive(Default)]
pub struct EndpointsHandle;

impl EndpointsInterface for EndpointsHandle {
    fn handle(&self) -> Option<EndpointServerPools> {
        get_global_endpoints_opt()
    }
}

/// Default deployment identity interface adapter.
#[derive(Default)]
pub struct DeploymentIdHandle;

impl DeploymentIdInterface for DeploymentIdHandle {
    fn get(&self) -> Option<String> {
        get_global_deployment_id()
    }
}

/// Default runtime port interface adapter.
#[derive(Default)]
pub struct RuntimePortHandle;

impl RuntimePortInterface for RuntimePortHandle {
    fn get(&self) -> u16 {
        global_rustfs_port()
    }
}

/// Default lock client interface adapter.
#[derive(Default)]
pub struct LockClientHandle;

impl LockClientInterface for LockClientHandle {
    fn handle(&self) -> Option<Arc<dyn LockClient>> {
        get_global_lock_client()
    }
}

/// Default lock clients interface adapter.
#[derive(Default)]
pub struct LockClientsHandle;

impl LockClientsInterface for LockClientsHandle {
    fn handle(&self) -> Option<HashMap<String, Arc<dyn LockClient>>> {
        get_global_lock_clients().cloned()
    }
}

/// Default performance metrics interface adapter.
#[derive(Default)]
pub struct PerformanceMetricsHandle;

impl PerformanceMetricsInterface for PerformanceMetricsHandle {
    fn handle(&self) -> Arc<PerformanceMetrics> {
        get_global_metrics()
    }
}

/// Default internode metrics interface adapter.
#[derive(Default)]
pub struct InternodeMetricsHandle;

impl InternodeMetricsInterface for InternodeMetricsHandle {
    fn handle(&self) -> Arc<InternodeMetrics> {
        global_internode_metrics().clone()
    }
}

/// Default S3 Select database interface adapter.
#[derive(Default)]
pub struct S3SelectDbHandle;

#[async_trait]
impl S3SelectDbInterface for S3SelectDbHandle {
    async fn get(
        &self,
        input: SelectObjectContentInput,
        enable_debug: bool,
    ) -> QueryResult<Arc<dyn DatabaseManagerSystem + Send + Sync>> {
        rustfs_s3select_query::get_global_db(input, enable_debug).await
    }
}

/// Default local node name interface adapter.
#[derive(Default)]
pub struct LocalNodeNameHandle;

#[async_trait]
impl LocalNodeNameInterface for LocalNodeNameHandle {
    async fn get(&self) -> String {
        get_global_local_node_name().await
    }
}

/// Default action credentials interface adapter.
#[derive(Default)]
pub struct ActionCredentialHandle;

impl ActionCredentialInterface for ActionCredentialHandle {
    fn get(&self) -> Option<Credentials> {
        get_global_action_cred()
    }
}

/// Default region interface adapter.
#[derive(Default)]
pub struct RegionHandle;

impl RegionInterface for RegionHandle {
    fn get(&self) -> Option<s3s::region::Region> {
        get_global_region()
    }
}

/// Default tier config interface adapter.
#[derive(Default)]
pub struct TierConfigHandle;

impl TierConfigInterface for TierConfigHandle {
    fn handle(&self) -> Arc<RwLock<TierConfigMgr>> {
        get_global_tier_config_mgr()
    }
}

/// Default lifecycle expiry state interface adapter.
#[derive(Default)]
pub struct ExpiryStateHandle;

impl ExpiryStateInterface for ExpiryStateHandle {
    fn handle(&self) -> Arc<RwLock<super::super::ExpiryState>> {
        get_global_expiry_state()
    }
}

/// Default server config interface adapter.
#[derive(Default)]
pub struct ServerConfigHandle;

impl ServerConfigInterface for ServerConfigHandle {
    fn get(&self) -> Option<Config> {
        get_global_server_config()
    }

    fn set(&self, config: Config) {
        set_global_server_config(config);
    }
}

/// Default storage class config interface adapter.
#[derive(Default)]
pub struct StorageClassHandle;

impl StorageClassInterface for StorageClassHandle {
    fn set(&self, config: StorageClassConfig) {
        set_global_storage_class(config);
    }
}

/// Default buffer profile config interface adapter.
#[derive(Default)]
pub struct BufferConfigHandle;

impl BufferConfigInterface for BufferConfigHandle {
    fn get(&self) -> RustFSBufferConfig {
        get_global_buffer_config().clone()
    }
}

pub fn default_notify_interface() -> Arc<dyn NotifyInterface> {
    Arc::new(NotifyHandle)
}

pub fn default_notification_system_interface() -> Arc<dyn NotificationSystemInterface> {
    Arc::new(NotificationSystemHandle)
}

pub fn default_kms_runtime_interface() -> Arc<dyn KmsRuntimeInterface> {
    Arc::new(KmsRuntimeHandle)
}

pub fn default_outbound_tls_runtime_interface() -> Arc<dyn OutboundTlsRuntimeInterface> {
    Arc::new(OutboundTlsRuntimeHandle)
}

pub fn default_bucket_metadata_interface() -> Arc<dyn BucketMetadataInterface> {
    Arc::new(BucketMetadataHandle)
}

pub fn default_bucket_monitor_interface() -> Arc<dyn BucketMonitorInterface> {
    Arc::new(BucketMonitorHandle)
}

pub fn default_replication_pool_interface() -> Arc<dyn ReplicationPoolInterface> {
    Arc::new(ReplicationPoolHandle)
}

pub fn default_replication_stats_interface() -> Arc<dyn ReplicationStatsInterface> {
    Arc::new(ReplicationStatsHandle)
}

pub fn default_boot_time_interface() -> Arc<dyn BootTimeInterface> {
    Arc::new(BootTimeHandle)
}

pub fn default_tier_stats_interface() -> Arc<dyn TierStatsInterface> {
    Arc::new(TierStatsHandle)
}

pub fn default_scanner_metrics_interface() -> Arc<dyn ScannerMetricsInterface> {
    Arc::new(ScannerMetricsHandle)
}

pub fn default_endpoints_interface() -> Arc<dyn EndpointsInterface> {
    Arc::new(EndpointsHandle)
}

pub fn default_deployment_id_interface() -> Arc<dyn DeploymentIdInterface> {
    Arc::new(DeploymentIdHandle)
}

pub fn default_runtime_port_interface() -> Arc<dyn RuntimePortInterface> {
    Arc::new(RuntimePortHandle)
}

pub fn default_lock_client_interface() -> Arc<dyn LockClientInterface> {
    Arc::new(LockClientHandle)
}

pub fn default_lock_clients_interface() -> Arc<dyn LockClientsInterface> {
    Arc::new(LockClientsHandle)
}

pub fn default_performance_metrics_interface() -> Arc<dyn PerformanceMetricsInterface> {
    Arc::new(PerformanceMetricsHandle)
}

pub fn default_internode_metrics_interface() -> Arc<dyn InternodeMetricsInterface> {
    Arc::new(InternodeMetricsHandle)
}

pub fn default_s3select_db_interface() -> Arc<dyn S3SelectDbInterface> {
    Arc::new(S3SelectDbHandle)
}

pub fn default_local_node_name_interface() -> Arc<dyn LocalNodeNameInterface> {
    Arc::new(LocalNodeNameHandle)
}

pub fn default_action_credential_interface() -> Arc<dyn ActionCredentialInterface> {
    Arc::new(ActionCredentialHandle)
}

pub fn default_oidc_interface() -> Arc<dyn OidcInterface> {
    Arc::new(OidcHandle)
}

pub fn default_region_interface() -> Arc<dyn RegionInterface> {
    Arc::new(RegionHandle)
}

pub fn default_tier_config_interface() -> Arc<dyn TierConfigInterface> {
    Arc::new(TierConfigHandle)
}

pub fn default_expiry_state_interface() -> Arc<dyn ExpiryStateInterface> {
    Arc::new(ExpiryStateHandle)
}

pub fn default_server_config_interface() -> Arc<dyn ServerConfigInterface> {
    Arc::new(ServerConfigHandle)
}

pub fn default_storage_class_interface() -> Arc<dyn StorageClassInterface> {
    Arc::new(StorageClassHandle)
}

pub fn default_buffer_config_interface() -> Arc<dyn BufferConfigInterface> {
    Arc::new(BufferConfigHandle)
}
