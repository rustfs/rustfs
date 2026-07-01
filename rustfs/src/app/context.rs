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

//! Application-layer dependency context.
//! This module introduces explicit dependency injection entry points
//! for storage, IAM, and KMS handles.

mod global;
mod handles;
mod interfaces;
mod runtime_sources;
mod startup;

pub use global::*;
pub use handles::*;
pub use interfaces::*;

use super::storage_api::context::bucket::metadata_sys::BucketMetadataSys;
use super::storage_api::context::runtime::{
    BucketBandwidthMonitor, DailyAllTierStats, DynReplicationPool, ExpiryState, NotificationSys, ReplicationStats,
    ScannerMetricsReport, StorageClassConfig, TierConfigMgr, TransitionState,
};
use super::storage_api::context::{ECStore, EndpointServerPools};
use crate::config::RustFSBufferConfig;
use rustfs_config::server_config::Config;
use rustfs_credentials::Credentials;
use rustfs_iam::{error::Error as IamError, oidc::OidcSys, store::object::ObjectStore, sys::IamSys};
use rustfs_io_metrics::{PerformanceMetrics, internode_metrics::InternodeMetrics};
use rustfs_kms::{KmsServiceManager, ObjectEncryptionService};
use rustfs_lock::LockClient;
use rustfs_s3select_api::{QueryResult, server::dbms::DatabaseManagerSystem};
use rustfs_tls_runtime::{GlobalPublishedOutboundTlsState, TlsGeneration};
use s3s::dto::SelectObjectContentInput;
use std::{collections::HashMap, sync::Arc, time::SystemTime};
use tokio::sync::RwLock;

/// Resolve KMS runtime service manager using AppContext-first precedence.
pub fn resolve_kms_runtime_service_manager() -> Option<Arc<KmsServiceManager>> {
    resolve_kms_runtime_service_manager_with(get_global_app_context())
}

/// Resolve or initialize the KMS runtime service manager using AppContext-first precedence.
pub fn resolve_or_init_kms_runtime_service_manager() -> Option<Arc<KmsServiceManager>> {
    resolve_or_init_kms_runtime_service_manager_with(get_global_app_context())
}

/// Resolve KMS encryption service using AppContext-first precedence.
pub async fn resolve_encryption_service() -> Option<Arc<ObjectEncryptionService>> {
    resolve_encryption_service_with(get_global_app_context()).await
}

/// Resolve outbound TLS generation using AppContext-first precedence.
pub fn resolve_outbound_tls_generation() -> Option<TlsGeneration> {
    resolve_outbound_tls_generation_with(get_global_app_context())
}

#[cfg(test)]
pub(crate) fn set_test_outbound_tls_generation(generation: u64) {
    runtime_sources::set_outbound_tls_generation(generation);
}

/// Resolve outbound TLS state using AppContext-first precedence.
pub async fn resolve_outbound_tls_state() -> Option<GlobalPublishedOutboundTlsState> {
    resolve_outbound_tls_state_with(get_global_app_context()).await
}

/// Resolve IAM readiness using AppContext-first precedence.
pub fn resolve_iam_ready() -> bool {
    resolve_iam_ready_with(get_global_app_context()).unwrap_or(false)
}

/// Resolve IAM system handle using AppContext-first precedence.
pub fn resolve_iam_handle() -> Option<Arc<IamSys<ObjectStore>>> {
    resolve_iam_handle_with(get_global_app_context())
}

/// Resolve OIDC system handle using AppContext-first precedence.
pub fn resolve_oidc_handle() -> Option<Arc<OidcSys>> {
    resolve_oidc_handle_with(get_global_app_context())
}

/// Publish the initialized OIDC system handle into the global AppContext.
pub fn publish_oidc_handle(oidc: Arc<OidcSys>) -> bool {
    publish_oidc_handle_with(get_global_app_context(), oidc)
}

/// Resolve a ready IAM system handle using AppContext-first precedence.
pub fn resolve_ready_iam_handle() -> rustfs_iam::error::Result<Arc<IamSys<ObjectStore>>> {
    if let Some(context) = get_global_app_context() {
        return resolve_ready_iam_handle_with(context);
    }

    Err(IamError::IamSysNotInitialized)
}

/// Resolve token signing key using AppContext-first precedence.
pub fn resolve_token_signing_key() -> Option<String> {
    resolve_token_signing_key_with(get_global_app_context())
}

/// Resolve bucket metadata handle using AppContext-first precedence.
pub fn resolve_bucket_metadata_handle() -> Option<Arc<RwLock<BucketMetadataSys>>> {
    resolve_bucket_metadata_handle_with(get_global_app_context())
}

/// Resolve object store handle using AppContext-first precedence.
pub fn resolve_object_store_handle() -> Option<Arc<ECStore>> {
    let context = get_global_app_context();
    resolve_object_store_handle_for_context(context.as_deref())
}

/// Resolve object store handle using an explicit AppContext.
pub fn resolve_object_store_handle_for_context(context: Option<&AppContext>) -> Option<Arc<ECStore>> {
    context.map(|context| context.object_store())
}

/// Resolve notify interface using AppContext-first precedence.
pub fn resolve_notify_interface() -> Option<Arc<dyn NotifyInterface>> {
    let context = get_global_app_context();
    resolve_notify_interface_for_context(context.as_deref())
}

/// Resolve notify interface using an explicit AppContext.
pub fn resolve_notify_interface_for_context(context: Option<&AppContext>) -> Option<Arc<dyn NotifyInterface>> {
    context.map(|context| context.notify())
}

/// Resolve notification system handle using AppContext-first precedence.
pub fn resolve_notification_system() -> Option<&'static NotificationSys> {
    resolve_notification_system_with(get_global_app_context())
}

/// Resolve notification system handle using an explicit AppContext.
pub fn resolve_notification_system_for_context(context: Option<&AppContext>) -> Option<&'static NotificationSys> {
    context.and_then(|context| context.notification_system().handle())
}

/// Resolve endpoints using AppContext-first precedence.
pub fn resolve_endpoints_handle() -> Option<EndpointServerPools> {
    resolve_endpoints_handle_with(get_global_app_context())
}

/// Resolve bucket bandwidth monitor using AppContext-first precedence.
pub fn resolve_bucket_monitor_handle() -> Option<Arc<BucketBandwidthMonitor>> {
    resolve_bucket_monitor_handle_with(get_global_app_context())
}

/// Resolve replication pool handle using AppContext-first precedence.
pub(crate) fn resolve_replication_pool_handle() -> Option<Arc<DynReplicationPool>> {
    resolve_replication_pool_handle_with(get_global_app_context())
}

/// Resolve replication statistics handle using AppContext-first precedence.
pub(crate) fn resolve_replication_stats_handle() -> Option<Arc<ReplicationStats>> {
    resolve_replication_stats_handle_with(get_global_app_context())
}

/// Resolve boot time using AppContext-first precedence.
pub fn resolve_boot_time() -> Option<SystemTime> {
    resolve_boot_time_with(get_global_app_context())
}

/// Resolve daily tier transition statistics using AppContext-first precedence.
pub fn resolve_daily_tier_stats() -> Option<DailyAllTierStats> {
    resolve_daily_tier_stats_with(get_global_app_context())
}

/// Resolve scanner metrics report using AppContext-first precedence.
pub async fn resolve_scanner_metrics_report() -> Option<ScannerMetricsReport> {
    resolve_scanner_metrics_report_with(get_global_app_context()).await
}

/// Resolve deployment identity using AppContext-first precedence.
pub fn resolve_deployment_id() -> Option<String> {
    resolve_deployment_id_with(get_global_app_context())
}

/// Resolve runtime port using AppContext-first precedence.
pub fn resolve_runtime_port() -> Option<u16> {
    resolve_runtime_port_with(get_global_app_context())
}

/// Resolve lock client using AppContext-first precedence.
pub fn resolve_lock_client() -> Option<Arc<dyn LockClient>> {
    resolve_lock_client_with_startup_fallback(get_global_app_context(), runtime_sources::lock_client)
}

/// Resolve lock clients using AppContext-first precedence.
pub fn resolve_lock_clients_handle() -> Option<HashMap<String, Arc<dyn LockClient>>> {
    resolve_lock_clients_handle_with_startup_fallback(get_global_app_context(), runtime_sources::lock_clients)
}

/// Resolve performance metrics using AppContext-first precedence.
pub fn resolve_performance_metrics() -> Option<Arc<PerformanceMetrics>> {
    resolve_performance_metrics_with(get_global_app_context())
}

/// Resolve internode metrics using AppContext-first precedence.
pub fn resolve_internode_metrics() -> Option<Arc<InternodeMetrics>> {
    resolve_internode_metrics_with(get_global_app_context())
}

/// Resolve S3 Select database using AppContext-first precedence.
pub async fn resolve_s3select_db(
    input: SelectObjectContentInput,
    enable_debug: bool,
) -> Option<QueryResult<Arc<dyn DatabaseManagerSystem + Send + Sync>>> {
    if let Some(context) = get_global_app_context() {
        return Some(resolve_s3select_db_with(context, input, enable_debug).await);
    }

    None
}

/// Resolve local node name using AppContext-first precedence.
pub async fn resolve_local_node_name() -> Option<String> {
    resolve_local_node_name_with(get_global_app_context()).await
}

/// Resolve action credentials using AppContext-first precedence.
pub fn resolve_action_credentials() -> Option<Credentials> {
    get_global_app_context().and_then(resolve_action_credentials_with)
}

/// Resolve region using AppContext-first precedence.
pub fn resolve_region() -> Option<s3s::region::Region> {
    resolve_region_with(get_global_app_context())
}

/// Resolve tier config handle using AppContext-first precedence.
pub fn resolve_tier_config_handle() -> Option<Arc<RwLock<TierConfigMgr>>> {
    resolve_tier_config_handle_with(get_global_app_context())
}

/// Resolve lifecycle expiry state using AppContext-first precedence.
pub fn resolve_expiry_state_handle() -> Option<Arc<RwLock<ExpiryState>>> {
    resolve_expiry_state_handle_with(get_global_app_context())
}

/// Resolve lifecycle transition state using AppContext-first precedence.
pub fn resolve_transition_state_handle() -> Option<Arc<TransitionState>> {
    resolve_transition_state_handle_with(get_global_app_context())
}

/// Resolve server config using AppContext-first precedence.
pub fn resolve_server_config() -> Option<Config> {
    resolve_server_config_with(get_global_app_context())
}

/// Resolve server config using an explicit AppContext.
pub fn resolve_server_config_for_context(context: Option<&AppContext>) -> Option<Config> {
    context.and_then(|context| context.server_config().get())
}

/// Publish server config using AppContext-first precedence.
pub fn publish_server_config(config: Config) -> bool {
    publish_server_config_with(get_global_app_context(), config)
}

/// Publish storage class config using AppContext-first precedence.
pub fn publish_storage_class_config(config: StorageClassConfig) -> bool {
    publish_storage_class_config_with(get_global_app_context(), config)
}

/// Resolve buffer profile config using AppContext-first precedence.
pub fn resolve_buffer_config() -> Option<RustFSBufferConfig> {
    resolve_buffer_config_with(get_global_app_context())
}

fn resolve_kms_runtime_service_manager_with(context: Option<Arc<AppContext>>) -> Option<Arc<KmsServiceManager>> {
    context.and_then(|context| context.kms_runtime().service_manager())
}

fn resolve_or_init_kms_runtime_service_manager_with(context: Option<Arc<AppContext>>) -> Option<Arc<KmsServiceManager>> {
    context.and_then(|context| context.kms_runtime().service_manager())
}

async fn resolve_encryption_service_with(context: Option<Arc<AppContext>>) -> Option<Arc<ObjectEncryptionService>> {
    let manager = context.and_then(|context| context.kms_runtime().service_manager())?;
    manager.get_encryption_service().await
}

fn resolve_outbound_tls_generation_with(context: Option<Arc<AppContext>>) -> Option<TlsGeneration> {
    context.map(|context| context.outbound_tls_runtime().generation())
}

async fn resolve_outbound_tls_state_with(context: Option<Arc<AppContext>>) -> Option<GlobalPublishedOutboundTlsState> {
    if let Some(context) = context {
        return Some(context.outbound_tls_runtime().state().await);
    }

    None
}

fn resolve_iam_ready_with(context: Option<Arc<AppContext>>) -> Option<bool> {
    context.map(|context| context.iam().is_ready())
}

fn resolve_iam_handle_with(context: Option<Arc<AppContext>>) -> Option<Arc<IamSys<ObjectStore>>> {
    context.map(|context| context.iam().handle())
}

fn resolve_oidc_handle_with(context: Option<Arc<AppContext>>) -> Option<Arc<OidcSys>> {
    context.and_then(|context| context.oidc().handle())
}

fn publish_oidc_handle_with(context: Option<Arc<AppContext>>, oidc: Arc<OidcSys>) -> bool {
    context.is_some_and(|context| context.publish_oidc_handle(oidc))
}

fn resolve_ready_iam_handle_with(context: Arc<AppContext>) -> rustfs_iam::error::Result<Arc<IamSys<ObjectStore>>> {
    if context.iam().is_ready() {
        return Ok(context.iam().handle());
    }

    Err(IamError::IamSysNotInitialized)
}

fn resolve_token_signing_key_with(context: Option<Arc<AppContext>>) -> Option<String> {
    context.and_then(|context| context.iam().token_signing_key())
}

fn resolve_bucket_metadata_handle_with(context: Option<Arc<AppContext>>) -> Option<Arc<RwLock<BucketMetadataSys>>> {
    context.and_then(|context| context.bucket_metadata().handle())
}

fn resolve_notification_system_with(context: Option<Arc<AppContext>>) -> Option<&'static NotificationSys> {
    context.and_then(|context| context.notification_system().handle())
}

fn resolve_bucket_monitor_handle_with(context: Option<Arc<AppContext>>) -> Option<Arc<BucketBandwidthMonitor>> {
    context.and_then(|context| context.bucket_monitor().handle())
}

fn resolve_replication_pool_handle_with(context: Option<Arc<AppContext>>) -> Option<Arc<DynReplicationPool>> {
    context.and_then(|context| context.replication_pool().handle())
}

fn resolve_replication_stats_handle_with(context: Option<Arc<AppContext>>) -> Option<Arc<ReplicationStats>> {
    context.and_then(|context| context.replication_stats().handle())
}

fn resolve_boot_time_with(context: Option<Arc<AppContext>>) -> Option<SystemTime> {
    context.and_then(|context| context.boot_time().get())
}

fn resolve_daily_tier_stats_with(context: Option<Arc<AppContext>>) -> Option<DailyAllTierStats> {
    context.map(|context| context.transition_state().daily_tier_stats())
}

async fn resolve_scanner_metrics_report_with(context: Option<Arc<AppContext>>) -> Option<ScannerMetricsReport> {
    if let Some(context) = context {
        return Some(context.scanner_metrics().report().await);
    }

    None
}

#[cfg(test)]
fn resolve_object_store_handle_with(context: Option<Arc<AppContext>>) -> Option<Arc<ECStore>> {
    context.map(|context| context.object_store())
}

fn resolve_endpoints_handle_with(context: Option<Arc<AppContext>>) -> Option<EndpointServerPools> {
    context.and_then(|context| context.endpoints().handle())
}

fn resolve_deployment_id_with(context: Option<Arc<AppContext>>) -> Option<String> {
    context.and_then(|context| context.deployment_id().get())
}

fn resolve_runtime_port_with(context: Option<Arc<AppContext>>) -> Option<u16> {
    context.map(|context| context.runtime_port().get())
}

fn resolve_lock_client_with(context: Option<Arc<AppContext>>) -> Option<Arc<dyn LockClient>> {
    context.and_then(|context| context.lock_client().handle())
}

fn resolve_lock_client_with_startup_fallback<F>(context: Option<Arc<AppContext>>, fallback: F) -> Option<Arc<dyn LockClient>>
where
    F: FnOnce() -> Option<Arc<dyn LockClient>>,
{
    resolve_lock_client_with(context).or_else(fallback)
}

fn resolve_lock_clients_handle_with(context: Option<Arc<AppContext>>) -> Option<HashMap<String, Arc<dyn LockClient>>> {
    context.and_then(|context| context.lock_clients().handle())
}

fn resolve_lock_clients_handle_with_startup_fallback<F>(
    context: Option<Arc<AppContext>>,
    fallback: F,
) -> Option<HashMap<String, Arc<dyn LockClient>>>
where
    F: FnOnce() -> Option<HashMap<String, Arc<dyn LockClient>>>,
{
    resolve_lock_clients_handle_with(context).or_else(fallback)
}

fn resolve_performance_metrics_with(context: Option<Arc<AppContext>>) -> Option<Arc<PerformanceMetrics>> {
    context.map(|context| context.performance_metrics().handle())
}

fn resolve_internode_metrics_with(context: Option<Arc<AppContext>>) -> Option<Arc<InternodeMetrics>> {
    context.map(|context| context.internode_metrics().handle())
}

async fn resolve_s3select_db_with(
    context: Arc<AppContext>,
    input: SelectObjectContentInput,
    enable_debug: bool,
) -> QueryResult<Arc<dyn DatabaseManagerSystem + Send + Sync>> {
    context.s3select_db().get(input, enable_debug).await
}

async fn resolve_local_node_name_with(context: Option<Arc<AppContext>>) -> Option<String> {
    if let Some(context) = context {
        return Some(context.local_node_name().get().await);
    }

    None
}

fn resolve_action_credentials_with(context: Arc<AppContext>) -> Option<Credentials> {
    context.action_credentials().get()
}

fn resolve_region_with(context: Option<Arc<AppContext>>) -> Option<s3s::region::Region> {
    context.and_then(|context| context.region().get())
}

fn resolve_tier_config_handle_with(context: Option<Arc<AppContext>>) -> Option<Arc<RwLock<TierConfigMgr>>> {
    context.map(|context| context.tier_config().handle())
}

fn resolve_expiry_state_handle_with(context: Option<Arc<AppContext>>) -> Option<Arc<RwLock<ExpiryState>>> {
    context.map(|context| context.expiry_state().handle())
}

fn resolve_transition_state_handle_with(context: Option<Arc<AppContext>>) -> Option<Arc<TransitionState>> {
    context.map(|context| context.transition_state().handle())
}

fn resolve_server_config_with(context: Option<Arc<AppContext>>) -> Option<Config> {
    context.and_then(|context| context.server_config().get())
}

fn publish_server_config_with(context: Option<Arc<AppContext>>, config: Config) -> bool {
    if let Some(context) = context {
        context.server_config().set(config);
        return true;
    }

    false
}

fn publish_storage_class_config_with(context: Option<Arc<AppContext>>, config: StorageClassConfig) -> bool {
    if let Some(context) = context {
        context.storage_class().set(config);
        return true;
    }

    false
}

fn resolve_buffer_config_with(context: Option<Arc<AppContext>>) -> Option<RustFSBufferConfig> {
    context.map(|context| context.buffer_config().get())
}

#[cfg(test)]
mod tests {
    use super::super::storage_api::context::runtime::init_local_disks;
    use super::super::storage_api::context::{Endpoint, Endpoints, PoolEndpoints};
    use super::*;
    use crate::app::context::global::AppContextTestInterfaces;
    use crate::app::context::handles::{
        default_bucket_monitor_interface, default_notification_system_interface, default_notify_interface,
        default_replication_pool_interface,
    };
    use crate::app::context::interfaces::{
        ActionCredentialInterface, BootTimeInterface, BucketMetadataInterface, BufferConfigInterface, DeploymentIdInterface,
        EndpointsInterface, IamInterface, InternodeMetricsInterface, KmsInterface, KmsRuntimeInterface, LocalNodeNameInterface,
        LockClientInterface, LockClientsInterface, OidcInterface, OutboundTlsRuntimeInterface, PerformanceMetricsInterface,
        RegionInterface, ReplicationStatsInterface, RuntimePortInterface, S3SelectDbInterface, ScannerMetricsInterface,
        ServerConfigInterface, StorageClassInterface, TierConfigInterface, TransitionStateInterface,
    };
    use crate::config::{RustFSBufferConfig, WorkloadProfile};
    use async_trait::async_trait;
    use rustfs_iam::{oidc::OidcSys, store::object::ObjectStore, sys::IamSys};
    use rustfs_io_metrics::{PerformanceMetrics, internode_metrics::InternodeMetrics};
    use rustfs_lock::{LocalClient, LockClient};
    use rustfs_s3select_api::{
        QueryResult,
        query::{Query, execution::QueryStateMachineRef, logical_planner::Plan},
        server::dbms::{DatabaseManagerSystem, QueryHandle},
    };
    use std::path::PathBuf;
    use std::sync::{
        RwLock as StdRwLock,
        atomic::{AtomicUsize, Ordering},
    };
    use std::time::{Duration, SystemTime};
    use tempfile::TempDir;
    use tokio_util::sync::CancellationToken;

    struct TestIamInterface {
        ready: bool,
        token_signing_key: Option<String>,
    }

    impl IamInterface for TestIamInterface {
        fn handle(&self) -> Arc<IamSys<ObjectStore>> {
            unreachable!("resolver tests do not need an IAM handle")
        }

        fn is_ready(&self) -> bool {
            self.ready
        }

        fn token_signing_key(&self) -> Option<String> {
            self.token_signing_key.clone()
        }
    }

    struct TestOidcInterface {
        oidc: StdRwLock<Option<Arc<OidcSys>>>,
    }

    impl TestOidcInterface {
        fn new(oidc: Option<Arc<OidcSys>>) -> Self {
            Self {
                oidc: StdRwLock::new(oidc),
            }
        }
    }

    impl OidcInterface for TestOidcInterface {
        fn handle(&self) -> Option<Arc<rustfs_iam::oidc::OidcSys>> {
            self.oidc.read().ok().and_then(|oidc| oidc.as_ref().cloned())
        }

        fn publish_handle(&self, oidc: Arc<rustfs_iam::oidc::OidcSys>) -> bool {
            let Ok(mut published_oidc) = self.oidc.write() else {
                return false;
            };
            *published_oidc = Some(oidc);
            true
        }
    }

    struct TestKmsInterface {
        kms: Arc<KmsServiceManager>,
    }

    impl KmsInterface for TestKmsInterface {
        fn handle(&self) -> Arc<KmsServiceManager> {
            self.kms.clone()
        }
    }

    struct TestKmsRuntimeInterface {
        kms: Option<Arc<KmsServiceManager>>,
    }

    impl KmsRuntimeInterface for TestKmsRuntimeInterface {
        fn service_manager(&self) -> Option<Arc<KmsServiceManager>> {
            self.kms.clone()
        }
    }

    struct TestOutboundTlsRuntimeInterface {
        state: GlobalPublishedOutboundTlsState,
    }

    #[async_trait]
    impl OutboundTlsRuntimeInterface for TestOutboundTlsRuntimeInterface {
        fn generation(&self) -> TlsGeneration {
            self.state.generation
        }

        async fn state(&self) -> GlobalPublishedOutboundTlsState {
            self.state.clone()
        }
    }

    struct TestBucketMetadataInterface {
        metadata: Option<Arc<RwLock<BucketMetadataSys>>>,
    }

    impl BucketMetadataInterface for TestBucketMetadataInterface {
        fn handle(&self) -> Option<Arc<RwLock<BucketMetadataSys>>> {
            self.metadata.clone()
        }
    }

    struct TestReplicationStatsInterface {
        stats: Option<Arc<ReplicationStats>>,
    }

    impl ReplicationStatsInterface for TestReplicationStatsInterface {
        fn handle(&self) -> Option<Arc<ReplicationStats>> {
            self.stats.clone()
        }
    }

    struct TestBootTimeInterface {
        boot_time: Option<SystemTime>,
    }

    impl BootTimeInterface for TestBootTimeInterface {
        fn get(&self) -> Option<SystemTime> {
            self.boot_time
        }
    }

    struct TestScannerMetricsInterface {
        report: ScannerMetricsReport,
    }

    #[async_trait]
    impl ScannerMetricsInterface for TestScannerMetricsInterface {
        async fn report(&self) -> ScannerMetricsReport {
            self.report.clone()
        }
    }

    struct TestEndpointsInterface {
        endpoints: Option<EndpointServerPools>,
    }

    impl EndpointsInterface for TestEndpointsInterface {
        fn handle(&self) -> Option<EndpointServerPools> {
            self.endpoints.clone()
        }
    }

    struct TestDeploymentIdInterface {
        id: Option<String>,
    }

    impl DeploymentIdInterface for TestDeploymentIdInterface {
        fn get(&self) -> Option<String> {
            self.id.clone()
        }
    }

    struct TestRuntimePortInterface {
        port: u16,
    }

    impl RuntimePortInterface for TestRuntimePortInterface {
        fn get(&self) -> u16 {
            self.port
        }
    }

    struct TestLockClientInterface {
        client: Option<Arc<dyn LockClient>>,
    }

    impl LockClientInterface for TestLockClientInterface {
        fn handle(&self) -> Option<Arc<dyn LockClient>> {
            self.client.clone()
        }
    }

    struct TestLockClientsInterface {
        clients: Option<HashMap<String, Arc<dyn LockClient>>>,
    }

    impl LockClientsInterface for TestLockClientsInterface {
        fn handle(&self) -> Option<HashMap<String, Arc<dyn LockClient>>> {
            self.clients.clone()
        }
    }

    struct TestPerformanceMetricsInterface {
        metrics: Arc<PerformanceMetrics>,
    }

    impl PerformanceMetricsInterface for TestPerformanceMetricsInterface {
        fn handle(&self) -> Arc<PerformanceMetrics> {
            self.metrics.clone()
        }
    }

    struct TestInternodeMetricsInterface {
        metrics: Arc<InternodeMetrics>,
    }

    impl InternodeMetricsInterface for TestInternodeMetricsInterface {
        fn handle(&self) -> Arc<InternodeMetrics> {
            self.metrics.clone()
        }
    }

    struct TestS3SelectDbInterface {
        db: Arc<dyn DatabaseManagerSystem + Send + Sync>,
    }

    #[async_trait]
    impl S3SelectDbInterface for TestS3SelectDbInterface {
        async fn get(
            &self,
            _input: SelectObjectContentInput,
            _enable_debug: bool,
        ) -> QueryResult<Arc<dyn DatabaseManagerSystem + Send + Sync>> {
            Ok(self.db.clone())
        }
    }

    struct TestS3SelectDb;

    #[async_trait]
    impl DatabaseManagerSystem for TestS3SelectDb {
        async fn execute(&self, _query: &Query) -> QueryResult<QueryHandle> {
            unreachable!("resolver tests only compare database handles")
        }

        async fn build_query_state_machine(&self, _query: Query) -> QueryResult<QueryStateMachineRef> {
            unreachable!("resolver tests only compare database handles")
        }

        async fn build_logical_plan(&self, _query_state_machine: QueryStateMachineRef) -> QueryResult<Option<Plan>> {
            unreachable!("resolver tests only compare database handles")
        }

        async fn execute_logical_plan(
            &self,
            _logical_plan: Plan,
            _query_state_machine: QueryStateMachineRef,
        ) -> QueryResult<QueryHandle> {
            unreachable!("resolver tests only compare database handles")
        }
    }

    struct TestLocalNodeNameInterface {
        name: String,
    }

    #[async_trait]
    impl LocalNodeNameInterface for TestLocalNodeNameInterface {
        async fn get(&self) -> String {
            self.name.clone()
        }
    }

    struct TestActionCredentialInterface {
        credentials: Option<Credentials>,
    }

    impl ActionCredentialInterface for TestActionCredentialInterface {
        fn get(&self) -> Option<Credentials> {
            self.credentials.clone()
        }
    }

    struct TestRegionInterface {
        region: Option<s3s::region::Region>,
    }

    impl RegionInterface for TestRegionInterface {
        fn get(&self) -> Option<s3s::region::Region> {
            self.region.clone()
        }
    }

    struct TestTierConfigInterface {
        tier_config: Arc<RwLock<TierConfigMgr>>,
    }

    impl TierConfigInterface for TestTierConfigInterface {
        fn handle(&self) -> Arc<RwLock<TierConfigMgr>> {
            self.tier_config.clone()
        }
    }

    struct TestExpiryStateInterface {
        expiry_state: Arc<RwLock<ExpiryState>>,
    }

    impl ExpiryStateInterface for TestExpiryStateInterface {
        fn handle(&self) -> Arc<RwLock<ExpiryState>> {
            self.expiry_state.clone()
        }
    }

    struct TestTransitionStateInterface {
        transition_state: Arc<TransitionState>,
        daily_stats: DailyAllTierStats,
    }

    impl TransitionStateInterface for TestTransitionStateInterface {
        fn handle(&self) -> Arc<TransitionState> {
            self.transition_state.clone()
        }

        fn daily_tier_stats(&self) -> DailyAllTierStats {
            self.daily_stats.clone()
        }
    }

    struct TestServerConfigInterface {
        config: Option<Config>,
        published: Arc<AtomicUsize>,
    }

    impl ServerConfigInterface for TestServerConfigInterface {
        fn get(&self) -> Option<Config> {
            self.config.clone()
        }

        fn set(&self, _config: Config) {
            self.published.fetch_add(1, Ordering::SeqCst);
        }
    }

    struct TestStorageClassInterface {
        published: Arc<AtomicUsize>,
    }

    impl StorageClassInterface for TestStorageClassInterface {
        fn set(&self, _config: StorageClassConfig) {
            self.published.fetch_add(1, Ordering::SeqCst);
        }
    }

    struct TestBufferConfigInterface {
        config: RustFSBufferConfig,
    }

    impl BufferConfigInterface for TestBufferConfigInterface {
        fn get(&self) -> RustFSBufferConfig {
            self.config.clone()
        }
    }

    async fn test_store() -> (TempDir, Arc<ECStore>, EndpointServerPools) {
        let temp_dir = tempfile::tempdir().expect("test temp dir");
        let disk_paths = (0..4)
            .map(|index| temp_dir.path().join(format!("disk{index}")))
            .collect::<Vec<PathBuf>>();

        for disk_path in &disk_paths {
            tokio::fs::create_dir_all(disk_path).await.expect("test disk dir");
        }

        let mut endpoints = Vec::with_capacity(disk_paths.len());
        for (index, disk_path) in disk_paths.iter().enumerate() {
            let mut endpoint = Endpoint::try_from(disk_path.to_str().expect("utf-8 test path")).expect("test endpoint");
            endpoint.set_pool_index(0);
            endpoint.set_set_index(0);
            endpoint.set_disk_index(index);
            endpoints.push(endpoint);
        }

        let pool_endpoints = PoolEndpoints {
            legacy: false,
            set_count: 1,
            drives_per_set: 4,
            endpoints: Endpoints::from(endpoints),
            cmd_line: "test".to_string(),
            platform: format!("OS: {} | Arch: {}", std::env::consts::OS, std::env::consts::ARCH),
        };
        let endpoint_pools = EndpointServerPools(vec![pool_endpoints]);

        init_local_disks(endpoint_pools.clone()).await.expect("test local disks");
        let store = ECStore::new(
            "127.0.0.1:0".parse().expect("test addr"),
            endpoint_pools.clone(),
            CancellationToken::new(),
        )
        .await
        .expect("test ecstore");

        (temp_dir, store, endpoint_pools)
    }

    fn test_select_input() -> SelectObjectContentInput {
        SelectObjectContentInput {
            bucket: "test-bucket".to_string(),
            expected_bucket_owner: None,
            key: "test.csv".to_string(),
            sse_customer_algorithm: None,
            sse_customer_key: None,
            sse_customer_key_md5: None,
            request: s3s::dto::SelectObjectContentRequest {
                expression: "SELECT * FROM S3Object".to_string(),
                expression_type: s3s::dto::ExpressionType::from_static("SQL"),
                input_serialization: s3s::dto::InputSerialization::default(),
                output_serialization: s3s::dto::OutputSerialization::default(),
                request_progress: None,
                scan_range: None,
            },
        }
    }

    #[test]
    fn lock_client_resolver_uses_startup_fallback_before_app_context() {
        let fallback_lock_client: Arc<dyn LockClient> = Arc::new(LocalClient::new());

        let resolved = resolve_lock_client_with_startup_fallback(None, || Some(fallback_lock_client.clone()))
            .expect("startup fallback lock client");

        assert!(Arc::ptr_eq(&resolved, &fallback_lock_client));
    }

    #[test]
    fn lock_clients_resolver_uses_startup_fallback_before_app_context() {
        let fallback_lock_client: Arc<dyn LockClient> = Arc::new(LocalClient::new());
        let mut fallback_lock_clients = HashMap::new();
        fallback_lock_clients.insert("startup-node:9000".to_string(), fallback_lock_client.clone());

        let resolved = resolve_lock_clients_handle_with_startup_fallback(None, || Some(fallback_lock_clients.clone()))
            .expect("startup fallback lock clients");

        assert!(Arc::ptr_eq(
            resolved.get("startup-node:9000").expect("startup fallback lock client entry"),
            &fallback_lock_client
        ));
    }

    #[tokio::test]
    async fn resolver_helpers_are_context_first_and_empty_when_context_is_absent() {
        let (_temp_dir, object_store, endpoints) = test_store().await;
        let context_kms = Arc::new(KmsServiceManager::new());
        let bucket_metadata = Arc::new(RwLock::new(BucketMetadataSys::new(object_store.clone())));
        let context_replication_stats = Arc::new(ReplicationStats::new());
        let context_boot_time = SystemTime::UNIX_EPOCH + Duration::from_secs(10);
        let context_scanner_metrics = ScannerMetricsReport {
            current_cycle: 7,
            ..Default::default()
        };
        let tier_config = TierConfigMgr::new();
        let context_expiry_state = ExpiryState::new();
        let context_transition_state = TransitionState::new();
        let mut context_daily_tier_stats = DailyAllTierStats::new();
        context_daily_tier_stats.insert("CONTEXT".to_string(), Default::default());
        let server_config = Config::new();
        let context_server_config_published = Arc::new(AtomicUsize::new(0));
        let context_storage_class_published = Arc::new(AtomicUsize::new(0));
        let buffer_config = RustFSBufferConfig::new(WorkloadProfile::AiTraining);
        let context_lock_client: Arc<dyn LockClient> = Arc::new(LocalClient::new());
        let context_node_name = "context-node".to_string();
        let context_deployment_id = "context-deployment".to_string();
        let context_runtime_port = 19000;
        let mut context_lock_clients = HashMap::new();
        context_lock_clients.insert("context-node:9000".to_string(), context_lock_client.clone());
        let context_performance_metrics = Arc::new(PerformanceMetrics::new());
        let context_internode_metrics = Arc::new(InternodeMetrics::default());
        let context_s3select_db: Arc<dyn DatabaseManagerSystem + Send + Sync> = Arc::new(TestS3SelectDb);
        let context_outbound_tls_state = GlobalPublishedOutboundTlsState {
            generation: TlsGeneration(41),
            root_ca_pem: Some(b"context-root-ca".to_vec()),
            mtls_identity: None,
        };
        let context_credentials = Credentials {
            access_key: "context-access-key".to_string(),
            ..Default::default()
        };
        let context_region: s3s::region::Region = "context-region".parse().expect("test region");
        let context_oidc_sys = match OidcSys::empty() {
            Ok(sys) => sys,
            Err(err) => unreachable!("test OIDC sys should initialize: {err}"),
        };
        let fallback_oidc_sys = match OidcSys::empty() {
            Ok(sys) => sys,
            Err(err) => unreachable!("test OIDC fallback sys should initialize: {err}"),
        };
        let context_oidc = Arc::new(context_oidc_sys);
        let fallback_oidc = Arc::new(fallback_oidc_sys);
        let context_token_signing_key = "context-token-signing-key".to_string();

        let context = Arc::new(AppContext::with_test_interfaces(
            object_store.clone(),
            AppContextTestInterfaces {
                iam: Arc::new(TestIamInterface {
                    ready: true,
                    token_signing_key: Some(context_token_signing_key.clone()),
                }),
                oidc: Arc::new(TestOidcInterface::new(Some(context_oidc.clone()))),
                kms: Arc::new(TestKmsInterface {
                    kms: context_kms.clone(),
                }),
                kms_runtime: Arc::new(TestKmsRuntimeInterface {
                    kms: Some(context_kms.clone()),
                }),
                outbound_tls_runtime: Arc::new(TestOutboundTlsRuntimeInterface {
                    state: context_outbound_tls_state.clone(),
                }),
                notify: default_notify_interface(),
                notification_system: default_notification_system_interface(),
                bucket_metadata: Arc::new(TestBucketMetadataInterface {
                    metadata: Some(bucket_metadata.clone()),
                }),
                bucket_monitor: default_bucket_monitor_interface(),
                replication_pool: default_replication_pool_interface(),
                replication_stats: Arc::new(TestReplicationStatsInterface {
                    stats: Some(context_replication_stats.clone()),
                }),
                boot_time: Arc::new(TestBootTimeInterface {
                    boot_time: Some(context_boot_time),
                }),
                scanner_metrics: Arc::new(TestScannerMetricsInterface {
                    report: context_scanner_metrics.clone(),
                }),
                endpoints: Arc::new(TestEndpointsInterface {
                    endpoints: Some(endpoints.clone()),
                }),
                deployment_id: Arc::new(TestDeploymentIdInterface {
                    id: Some(context_deployment_id.clone()),
                }),
                runtime_port: Arc::new(TestRuntimePortInterface {
                    port: context_runtime_port,
                }),
                lock_client: Arc::new(TestLockClientInterface {
                    client: Some(context_lock_client.clone()),
                }),
                lock_clients: Arc::new(TestLockClientsInterface {
                    clients: Some(context_lock_clients.clone()),
                }),
                performance_metrics: Arc::new(TestPerformanceMetricsInterface {
                    metrics: context_performance_metrics.clone(),
                }),
                internode_metrics: Arc::new(TestInternodeMetricsInterface {
                    metrics: context_internode_metrics.clone(),
                }),
                s3select_db: Arc::new(TestS3SelectDbInterface {
                    db: context_s3select_db.clone(),
                }),
                local_node_name: Arc::new(TestLocalNodeNameInterface {
                    name: context_node_name.clone(),
                }),
                action_credentials: Arc::new(TestActionCredentialInterface {
                    credentials: Some(context_credentials.clone()),
                }),
                region: Arc::new(TestRegionInterface {
                    region: Some(context_region.clone()),
                }),
                tier_config: Arc::new(TestTierConfigInterface {
                    tier_config: tier_config.clone(),
                }),
                expiry_state: Arc::new(TestExpiryStateInterface {
                    expiry_state: context_expiry_state.clone(),
                }),
                transition_state: Arc::new(TestTransitionStateInterface {
                    transition_state: context_transition_state.clone(),
                    daily_stats: context_daily_tier_stats.clone(),
                }),
                server_config: Arc::new(TestServerConfigInterface {
                    config: Some(server_config.clone()),
                    published: context_server_config_published.clone(),
                }),
                storage_class: Arc::new(TestStorageClassInterface {
                    published: context_storage_class_published.clone(),
                }),
                buffer_config: Arc::new(TestBufferConfigInterface { config: buffer_config }),
            },
        ));

        assert!(Arc::ptr_eq(
            &resolve_kms_runtime_service_manager_with(Some(context.clone())).expect("context KMS runtime"),
            &context_kms
        ));
        assert!(Arc::ptr_eq(
            &resolve_or_init_kms_runtime_service_manager_with(Some(context.clone())).expect("context KMS runtime"),
            &context_kms
        ));
        assert_eq!(
            resolve_outbound_tls_generation_with(Some(context.clone())).expect("context outbound TLS generation"),
            context_outbound_tls_state.generation
        );
        assert_eq!(
            resolve_outbound_tls_state_with(Some(context.clone()))
                .await
                .expect("context outbound TLS state")
                .generation,
            context_outbound_tls_state.generation
        );
        assert!(resolve_iam_ready_with(Some(context.clone())).expect("context IAM ready"));
        let resolved_oidc = resolve_oidc_handle_with(Some(context.clone()));
        assert!(resolved_oidc.as_ref().is_some_and(|oidc| Arc::ptr_eq(oidc, &context_oidc)));
        assert!(publish_oidc_handle_with(Some(context.clone()), fallback_oidc.clone()));
        let resolved_oidc = resolve_oidc_handle_with(Some(context.clone()));
        assert!(resolved_oidc.as_ref().is_some_and(|oidc| Arc::ptr_eq(oidc, &fallback_oidc)));
        assert_eq!(
            resolve_token_signing_key_with(Some(context.clone())).as_deref(),
            Some(context_token_signing_key.as_str())
        );
        assert!(Arc::ptr_eq(
            &resolve_bucket_metadata_handle_with(Some(context.clone())).expect("context bucket metadata"),
            &bucket_metadata
        ));
        assert!(Arc::ptr_eq(
            &resolve_object_store_handle_with(Some(context.clone())).expect("context object store"),
            &object_store
        ));
        assert!(Arc::ptr_eq(
            &resolve_replication_stats_handle_with(Some(context.clone())).expect("context replication stats"),
            &context_replication_stats
        ));
        assert_eq!(
            resolve_boot_time_with(Some(context.clone())).expect("context boot time"),
            context_boot_time
        );
        assert!(
            resolve_daily_tier_stats_with(Some(context.clone()))
                .expect("context daily tier stats")
                .contains_key("CONTEXT")
        );
        assert_eq!(
            resolve_scanner_metrics_report_with(Some(context.clone()))
                .await
                .expect("context scanner metrics")
                .current_cycle,
            context_scanner_metrics.current_cycle
        );
        assert_eq!(
            resolve_endpoints_handle_with(Some(context.clone()))
                .expect("context endpoints")
                .as_ref()[0]
                .drives_per_set,
            endpoints.as_ref()[0].drives_per_set
        );
        assert_eq!(
            resolve_deployment_id_with(Some(context.clone())).expect("context deployment id"),
            context_deployment_id
        );
        assert_eq!(
            resolve_runtime_port_with(Some(context.clone())).expect("context runtime port"),
            context_runtime_port
        );
        assert!(Arc::ptr_eq(
            &resolve_lock_client_with(Some(context.clone())).expect("context lock client"),
            &context_lock_client
        ));
        assert!(Arc::ptr_eq(
            resolve_lock_clients_handle_with(Some(context.clone()))
                .expect("context lock clients")
                .get("context-node:9000")
                .expect("context lock client entry"),
            &context_lock_client
        ));
        assert!(Arc::ptr_eq(
            &resolve_performance_metrics_with(Some(context.clone())).expect("context performance metrics"),
            &context_performance_metrics
        ));
        assert!(Arc::ptr_eq(
            &resolve_internode_metrics_with(Some(context.clone())).expect("context internode metrics"),
            &context_internode_metrics
        ));
        assert!(Arc::ptr_eq(
            &resolve_s3select_db_with(context.clone(), test_select_input(), false)
                .await
                .expect("context S3 Select DB"),
            &context_s3select_db
        ));
        assert_eq!(
            resolve_local_node_name_with(Some(context.clone()))
                .await
                .expect("context local node name"),
            context_node_name
        );
        assert_eq!(
            resolve_action_credentials_with(context.clone())
                .expect("context action credentials")
                .access_key,
            context_credentials.access_key
        );
        assert_eq!(resolve_region_with(Some(context.clone())).expect("context region"), context_region);
        assert!(Arc::ptr_eq(
            &resolve_tier_config_handle_with(Some(context.clone())).expect("context tier config"),
            &tier_config
        ));
        assert!(Arc::ptr_eq(
            &resolve_expiry_state_handle_with(Some(context.clone())).expect("context expiry state"),
            &context_expiry_state
        ));
        assert!(Arc::ptr_eq(
            &resolve_transition_state_handle_with(Some(context.clone())).expect("context transition state"),
            &context_transition_state
        ));
        assert_eq!(
            resolve_server_config_with(Some(context.clone())).expect("context server config"),
            server_config
        );
        assert!(publish_server_config_with(Some(context.clone()), Config::new()));
        assert_eq!(context_server_config_published.load(Ordering::SeqCst), 1);
        assert!(publish_storage_class_config_with(Some(context.clone()), StorageClassConfig::default()));
        assert_eq!(context_storage_class_published.load(Ordering::SeqCst), 1);
        assert_eq!(
            resolve_buffer_config_with(Some(context))
                .expect("context buffer config")
                .workload,
            WorkloadProfile::AiTraining
        );

        assert!(resolve_kms_runtime_service_manager_with(None).is_none());
        assert!(resolve_or_init_kms_runtime_service_manager_with(None).is_none());
        assert!(resolve_outbound_tls_generation_with(None).is_none());
        assert!(resolve_outbound_tls_state_with(None).await.is_none());
        assert!(resolve_iam_ready_with(None).is_none());
        assert!(resolve_iam_handle_with(None).is_none());
        assert!(resolve_oidc_handle_with(None).is_none());
        assert!(resolve_token_signing_key_with(None).is_none());
        assert!(resolve_notify_interface_for_context(None).is_none());
        assert!(!publish_oidc_handle_with(None, context_oidc));
        assert!(resolve_bucket_metadata_handle_with(None).is_none());
        assert!(resolve_bucket_monitor_handle_with(None).is_none());
        assert!(resolve_replication_pool_handle_with(None).is_none());
        assert!(resolve_object_store_handle_with(None).is_none());
        assert!(resolve_replication_stats_handle_with(None).is_none());
        assert!(resolve_boot_time_with(None).is_none());
        assert!(resolve_daily_tier_stats_with(None).is_none());
        assert!(resolve_scanner_metrics_report_with(None).await.is_none());
        assert!(resolve_endpoints_handle_with(None).is_none());
        assert!(resolve_deployment_id_with(None).is_none());
        assert!(resolve_runtime_port_with(None).is_none());
        assert!(resolve_lock_client_with(None).is_none());
        assert!(resolve_lock_clients_handle_with(None).is_none());
        assert!(resolve_performance_metrics_with(None).is_none());
        assert!(resolve_internode_metrics_with(None).is_none());
        assert!(resolve_local_node_name_with(None).await.is_none());
        assert!(resolve_action_credentials().is_none());
        assert!(resolve_region_with(None).is_none());
        assert!(resolve_tier_config_handle_with(None).is_none());
        assert!(resolve_expiry_state_handle_with(None).is_none());
        assert!(resolve_transition_state_handle_with(None).is_none());
        assert!(resolve_server_config_with(None).is_none());
        assert!(!publish_server_config_with(None, Config::new()));
        assert!(!publish_storage_class_config_with(None, StorageClassConfig::default()));
        assert!(resolve_buffer_config_with(None).is_none());
    }
}
