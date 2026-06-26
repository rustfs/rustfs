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

use super::super::storage_api::context::EndpointServerPools;
use super::super::storage_api::context::bucket::metadata_sys::BucketMetadataSys;
use super::super::storage_api::context::runtime::{
    BucketBandwidthMonitor, DailyAllTierStats, DynReplicationPool, ExpiryState, NotificationSys, ReplicationStats,
    ScannerMetricsReport, StorageClassConfig, TierConfigMgr,
};
use super::interfaces::{
    ActionCredentialInterface, BootTimeInterface, BucketMetadataInterface, BucketMonitorInterface, BufferConfigInterface,
    DeploymentIdInterface, EndpointsInterface, ExpiryStateInterface, IamInterface, InternodeMetricsInterface, KmsInterface,
    KmsRuntimeInterface, LocalNodeNameInterface, LockClientInterface, LockClientsInterface, NotificationSystemInterface,
    NotifyInterface, OidcInterface, OutboundTlsRuntimeInterface, PerformanceMetricsInterface, RegionInterface,
    ReplicationPoolInterface, ReplicationStatsInterface, RuntimePortInterface, S3SelectDbInterface, ScannerMetricsInterface,
    ServerConfigInterface, StorageClassInterface, TierConfigInterface, TierStatsInterface,
};
use super::runtime_sources;
use crate::config::RustFSBufferConfig;
use async_trait::async_trait;
use rustfs_config::server_config::Config;
use rustfs_credentials::Credentials;
use rustfs_iam::{oidc::OidcSys, store::object::ObjectStore, sys::IamSys};
use rustfs_io_metrics::{PerformanceMetrics, internode_metrics::InternodeMetrics};
use rustfs_kms::KmsServiceManager;
use rustfs_lock::LockClient;
use rustfs_notify::{EventArgs, NotificationError};
use rustfs_s3select_api::{QueryResult, server::dbms::DatabaseManagerSystem};
use rustfs_targets::{EventName, arn::TargetID};
use rustfs_tls_runtime::{GlobalPublishedOutboundTlsState, TlsGeneration};
use s3s::dto::SelectObjectContentInput;
use std::{
    collections::HashMap,
    sync::{Arc, RwLock as StdRwLock},
    time::SystemTime,
};
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
        runtime_sources::ready_iam_handle().is_ok()
    }

    fn token_signing_key(&self) -> Option<String> {
        runtime_sources::token_signing_key()
    }
}

/// Default OIDC interface adapter.
pub struct OidcHandle {
    oidc: StdRwLock<Option<Arc<OidcSys>>>,
}

impl OidcHandle {
    pub fn new(oidc: Option<Arc<OidcSys>>) -> Self {
        Self {
            oidc: StdRwLock::new(oidc),
        }
    }
}

impl Default for OidcHandle {
    fn default() -> Self {
        Self::new(None)
    }
}

impl OidcInterface for OidcHandle {
    fn handle(&self) -> Option<Arc<OidcSys>> {
        self.oidc.read().ok().and_then(|oidc| oidc.as_ref().cloned())
    }

    fn publish_handle(&self, oidc: Arc<OidcSys>) -> bool {
        let Ok(mut published_oidc) = self.oidc.write() else {
            return false;
        };
        *published_oidc = Some(oidc);
        true
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
        runtime_sources::kms_service_manager()
    }
}

/// Default outbound TLS runtime interface adapter.
#[derive(Default)]
pub struct OutboundTlsRuntimeHandle;

#[async_trait]
impl OutboundTlsRuntimeInterface for OutboundTlsRuntimeHandle {
    fn generation(&self) -> TlsGeneration {
        runtime_sources::outbound_tls_generation()
    }

    async fn state(&self) -> GlobalPublishedOutboundTlsState {
        runtime_sources::outbound_tls_state().await
    }
}

/// Default notify interface adapter.
#[derive(Default)]
pub struct NotifyHandle;

#[async_trait]
impl NotifyInterface for NotifyHandle {
    async fn notify(&self, args: EventArgs) {
        runtime_sources::notify(args).await;
    }

    async fn add_event_specific_rules(
        &self,
        bucket_name: &str,
        region: &str,
        event_rules: &[(Vec<EventName>, String, String, Vec<TargetID>)],
    ) -> Result<(), NotificationError> {
        runtime_sources::add_event_specific_rules(bucket_name, region, event_rules).await
    }

    async fn clear_bucket_notification_rules(&self, bucket_name: &str) -> Result<(), NotificationError> {
        runtime_sources::clear_bucket_notification_rules(bucket_name).await
    }
}

/// Default notification system handle adapter.
#[derive(Default)]
pub struct NotificationSystemHandle;

impl NotificationSystemInterface for NotificationSystemHandle {
    fn handle(&self) -> Option<&'static NotificationSys> {
        runtime_sources::notification_system()
    }
}

/// Default bucket metadata interface adapter.
#[derive(Default)]
pub struct BucketMetadataHandle;

impl BucketMetadataInterface for BucketMetadataHandle {
    fn handle(&self) -> Option<Arc<RwLock<BucketMetadataSys>>> {
        runtime_sources::bucket_metadata()
    }
}

/// Default bucket monitor interface adapter.
#[derive(Default)]
pub struct BucketMonitorHandle;

impl BucketMonitorInterface for BucketMonitorHandle {
    fn handle(&self) -> Option<Arc<BucketBandwidthMonitor>> {
        runtime_sources::bucket_monitor()
    }
}

/// Default replication pool interface adapter.
#[derive(Default)]
pub struct ReplicationPoolHandle;

impl ReplicationPoolInterface for ReplicationPoolHandle {
    fn handle(&self) -> Option<Arc<DynReplicationPool>> {
        runtime_sources::replication_pool()
    }
}

/// Default replication statistics interface adapter.
#[derive(Default)]
pub struct ReplicationStatsHandle;

impl ReplicationStatsInterface for ReplicationStatsHandle {
    fn handle(&self) -> Option<Arc<ReplicationStats>> {
        runtime_sources::replication_stats()
    }
}

/// Default boot time interface adapter.
#[derive(Default)]
pub struct BootTimeHandle;

impl BootTimeInterface for BootTimeHandle {
    fn get(&self) -> Option<SystemTime> {
        runtime_sources::boot_time()
    }
}

/// Default tier transition statistics interface adapter.
#[derive(Default)]
pub struct TierStatsHandle;

impl TierStatsInterface for TierStatsHandle {
    fn daily_all(&self) -> DailyAllTierStats {
        runtime_sources::daily_tier_stats()
    }
}

/// Default scanner metrics report interface adapter.
#[derive(Default)]
pub struct ScannerMetricsHandle;

#[async_trait]
impl ScannerMetricsInterface for ScannerMetricsHandle {
    async fn report(&self) -> ScannerMetricsReport {
        runtime_sources::scanner_metrics_report().await
    }
}

/// Default endpoints interface adapter.
#[derive(Default)]
pub struct EndpointsHandle;

impl EndpointsInterface for EndpointsHandle {
    fn handle(&self) -> Option<EndpointServerPools> {
        runtime_sources::endpoints()
    }
}

/// Default deployment identity interface adapter.
#[derive(Default)]
pub struct DeploymentIdHandle;

impl DeploymentIdInterface for DeploymentIdHandle {
    fn get(&self) -> Option<String> {
        runtime_sources::deployment_id()
    }
}

/// Default runtime port interface adapter.
#[derive(Default)]
pub struct RuntimePortHandle;

impl RuntimePortInterface for RuntimePortHandle {
    fn get(&self) -> u16 {
        runtime_sources::runtime_port()
    }
}

/// Default lock client interface adapter.
#[derive(Default)]
pub struct LockClientHandle;

impl LockClientInterface for LockClientHandle {
    fn handle(&self) -> Option<Arc<dyn LockClient>> {
        runtime_sources::lock_client()
    }
}

/// Default lock clients interface adapter.
#[derive(Default)]
pub struct LockClientsHandle;

impl LockClientsInterface for LockClientsHandle {
    fn handle(&self) -> Option<HashMap<String, Arc<dyn LockClient>>> {
        runtime_sources::lock_clients()
    }
}

/// Default performance metrics interface adapter.
#[derive(Default)]
pub struct PerformanceMetricsHandle;

impl PerformanceMetricsInterface for PerformanceMetricsHandle {
    fn handle(&self) -> Arc<PerformanceMetrics> {
        runtime_sources::performance_metrics()
    }
}

/// Default internode metrics interface adapter.
#[derive(Default)]
pub struct InternodeMetricsHandle;

impl InternodeMetricsInterface for InternodeMetricsHandle {
    fn handle(&self) -> Arc<InternodeMetrics> {
        runtime_sources::internode_metrics()
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
        runtime_sources::s3select_db(input, enable_debug).await
    }
}

/// Default local node name interface adapter.
#[derive(Default)]
pub struct LocalNodeNameHandle;

#[async_trait]
impl LocalNodeNameInterface for LocalNodeNameHandle {
    async fn get(&self) -> String {
        runtime_sources::local_node_name().await
    }
}

/// Default action credentials interface adapter.
#[derive(Default)]
pub struct ActionCredentialHandle;

impl ActionCredentialInterface for ActionCredentialHandle {
    fn get(&self) -> Option<Credentials> {
        runtime_sources::action_credentials()
    }
}

/// Default region interface adapter.
#[derive(Default)]
pub struct RegionHandle;

impl RegionInterface for RegionHandle {
    fn get(&self) -> Option<s3s::region::Region> {
        runtime_sources::region()
    }
}

/// Default tier config interface adapter.
#[derive(Default)]
pub struct TierConfigHandle;

impl TierConfigInterface for TierConfigHandle {
    fn handle(&self) -> Arc<RwLock<TierConfigMgr>> {
        runtime_sources::tier_config()
    }
}

/// Default lifecycle expiry state interface adapter.
#[derive(Default)]
pub struct ExpiryStateHandle;

impl ExpiryStateInterface for ExpiryStateHandle {
    fn handle(&self) -> Arc<RwLock<ExpiryState>> {
        runtime_sources::expiry_state()
    }
}

/// Default server config interface adapter.
#[derive(Default)]
pub struct ServerConfigHandle;

impl ServerConfigInterface for ServerConfigHandle {
    fn get(&self) -> Option<Config> {
        runtime_sources::server_config()
    }

    fn set(&self, config: Config) {
        runtime_sources::set_server_config(config);
    }
}

/// Default storage class config interface adapter.
#[derive(Default)]
pub struct StorageClassHandle;

impl StorageClassInterface for StorageClassHandle {
    fn set(&self, config: StorageClassConfig) {
        runtime_sources::set_storage_class(config);
    }
}

/// Default buffer profile config interface adapter.
#[derive(Default)]
pub struct BufferConfigHandle;

impl BufferConfigInterface for BufferConfigHandle {
    fn get(&self) -> RustFSBufferConfig {
        runtime_sources::buffer_config()
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
    Arc::new(OidcHandle::default())
}

pub fn oidc_interface(oidc: Option<Arc<OidcSys>>) -> Arc<dyn OidcInterface> {
    Arc::new(OidcHandle::new(oidc))
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
