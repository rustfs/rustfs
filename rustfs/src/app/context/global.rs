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

use super::super::storage_api::context::ECStore;
use super::super::storage_api::context::runtime::set_object_store_resolver;
use super::handles::{
    IamHandle, KmsHandle, default_action_credential_interface, default_boot_time_interface, default_bucket_metadata_interface,
    default_bucket_monitor_interface, default_buffer_config_interface, default_deployment_id_interface,
    default_endpoints_interface, default_expiry_state_interface, default_internode_metrics_interface,
    default_kms_runtime_interface, default_local_node_name_interface, default_lock_client_interface,
    default_lock_clients_interface, default_notification_system_interface, default_notify_interface, default_oidc_interface,
    default_outbound_tls_runtime_interface, default_performance_metrics_interface, default_region_interface,
    default_replication_pool_interface, default_replication_stats_interface, default_runtime_port_interface,
    default_s3select_db_interface, default_scanner_metrics_interface, default_server_config_interface,
    default_storage_class_interface, default_tier_config_interface, default_transition_state_interface, oidc_interface,
};
use super::interfaces::{
    ActionCredentialInterface, BootTimeInterface, BucketMetadataInterface, BucketMonitorInterface, BufferConfigInterface,
    DeploymentIdInterface, EndpointsInterface, ExpiryStateInterface, IamInterface, InternodeMetricsInterface, KmsInterface,
    KmsRuntimeInterface, LocalNodeNameInterface, LockClientInterface, LockClientsInterface, NotificationSystemInterface,
    NotifyInterface, OidcInterface, OutboundTlsRuntimeInterface, PerformanceMetricsInterface, RegionInterface,
    ReplicationPoolInterface, ReplicationStatsInterface, RuntimePortInterface, S3SelectDbInterface, ScannerMetricsInterface,
    ServerConfigInterface, StorageClassInterface, TierConfigInterface, TransitionStateInterface,
};
use crate::app::object_data_cache::ObjectDataCacheAdapter;
use rustfs_iam::{oidc::OidcSys, store::object::ObjectStore, sys::IamSys};
use rustfs_kms::KmsServiceManager;
use std::sync::{Arc, OnceLock};

/// Application-layer context with explicit dependencies.
#[derive(Clone)]
pub struct AppContext {
    object_store: Arc<ECStore>,
    iam: Arc<dyn IamInterface>,
    oidc: Arc<dyn OidcInterface>,
    #[allow(dead_code)]
    kms: Arc<dyn KmsInterface>,
    kms_runtime: Arc<dyn KmsRuntimeInterface>,
    outbound_tls_runtime: Arc<dyn OutboundTlsRuntimeInterface>,
    notify: Arc<dyn NotifyInterface>,
    notification_system: Arc<dyn NotificationSystemInterface>,
    bucket_metadata: Arc<dyn BucketMetadataInterface>,
    bucket_monitor: Arc<dyn BucketMonitorInterface>,
    replication_pool: Arc<dyn ReplicationPoolInterface>,
    replication_stats: Arc<dyn ReplicationStatsInterface>,
    boot_time: Arc<dyn BootTimeInterface>,
    scanner_metrics: Arc<dyn ScannerMetricsInterface>,
    endpoints: Arc<dyn EndpointsInterface>,
    deployment_id: Arc<dyn DeploymentIdInterface>,
    runtime_port: Arc<dyn RuntimePortInterface>,
    lock_client: Arc<dyn LockClientInterface>,
    lock_clients: Arc<dyn LockClientsInterface>,
    performance_metrics: Arc<dyn PerformanceMetricsInterface>,
    internode_metrics: Arc<dyn InternodeMetricsInterface>,
    s3select_db: Arc<dyn S3SelectDbInterface>,
    local_node_name: Arc<dyn LocalNodeNameInterface>,
    action_credentials: Arc<dyn ActionCredentialInterface>,
    region: Arc<dyn RegionInterface>,
    tier_config: Arc<dyn TierConfigInterface>,
    expiry_state: Arc<dyn ExpiryStateInterface>,
    transition_state: Arc<dyn TransitionStateInterface>,
    server_config: Arc<dyn ServerConfigInterface>,
    storage_class: Arc<dyn StorageClassInterface>,
    buffer_config: Arc<dyn BufferConfigInterface>,
    object_data_cache: Arc<ObjectDataCacheAdapter>,
}

impl AppContext {
    pub fn new(object_store: Arc<ECStore>, iam: Arc<dyn IamInterface>, kms: Arc<dyn KmsInterface>) -> Self {
        let object_data_cache = ObjectDataCacheAdapter::from_env_or_disabled();
        // Let ecstore probe this cache inside get_object_reader, after
        // metadata resolution but before the erasure data read (backlog#802).
        crate::app::object_data_cache::register_object_data_cache_body_hook(Arc::clone(&object_data_cache));

        Self {
            object_store,
            iam,
            oidc: default_oidc_interface(),
            kms,
            kms_runtime: default_kms_runtime_interface(),
            outbound_tls_runtime: default_outbound_tls_runtime_interface(),
            notify: default_notify_interface(),
            notification_system: default_notification_system_interface(),
            bucket_metadata: default_bucket_metadata_interface(),
            bucket_monitor: default_bucket_monitor_interface(),
            replication_pool: default_replication_pool_interface(),
            replication_stats: default_replication_stats_interface(),
            boot_time: default_boot_time_interface(),
            scanner_metrics: default_scanner_metrics_interface(),
            endpoints: default_endpoints_interface(),
            deployment_id: default_deployment_id_interface(),
            runtime_port: default_runtime_port_interface(),
            lock_client: default_lock_client_interface(),
            lock_clients: default_lock_clients_interface(),
            performance_metrics: default_performance_metrics_interface(),
            internode_metrics: default_internode_metrics_interface(),
            s3select_db: default_s3select_db_interface(),
            local_node_name: default_local_node_name_interface(),
            action_credentials: default_action_credential_interface(),
            region: default_region_interface(),
            tier_config: default_tier_config_interface(),
            expiry_state: default_expiry_state_interface(),
            transition_state: default_transition_state_interface(),
            server_config: default_server_config_interface(),
            storage_class: default_storage_class_interface(),
            buffer_config: default_buffer_config_interface(),
            object_data_cache,
        }
    }

    pub fn with_default_interfaces(
        object_store: Arc<ECStore>,
        iam: Arc<IamSys<ObjectStore>>,
        kms: Arc<KmsServiceManager>,
    ) -> Self {
        let mut context = Self::new(object_store, Arc::new(IamHandle::new(iam)), Arc::new(KmsHandle::new(kms)));
        context.oidc = oidc_interface(super::runtime_sources::oidc_handle());
        context
    }

    pub fn object_store(&self) -> Arc<ECStore> {
        self.object_store.clone()
    }

    pub fn iam(&self) -> Arc<dyn IamInterface> {
        self.iam.clone()
    }

    pub fn oidc(&self) -> Arc<dyn OidcInterface> {
        self.oidc.clone()
    }

    pub fn publish_oidc_handle(&self, oidc: Arc<OidcSys>) -> bool {
        self.oidc.publish_handle(oidc)
    }

    #[allow(dead_code)]
    pub fn kms(&self) -> Arc<dyn KmsInterface> {
        self.kms.clone()
    }

    pub fn kms_runtime(&self) -> Arc<dyn KmsRuntimeInterface> {
        self.kms_runtime.clone()
    }

    pub fn outbound_tls_runtime(&self) -> Arc<dyn OutboundTlsRuntimeInterface> {
        self.outbound_tls_runtime.clone()
    }

    pub fn notify(&self) -> Arc<dyn NotifyInterface> {
        self.notify.clone()
    }

    pub fn notification_system(&self) -> Arc<dyn NotificationSystemInterface> {
        self.notification_system.clone()
    }

    pub fn bucket_metadata(&self) -> Arc<dyn BucketMetadataInterface> {
        self.bucket_metadata.clone()
    }

    pub fn bucket_monitor(&self) -> Arc<dyn BucketMonitorInterface> {
        self.bucket_monitor.clone()
    }

    pub(crate) fn replication_pool(&self) -> Arc<dyn ReplicationPoolInterface> {
        self.replication_pool.clone()
    }

    pub(crate) fn replication_stats(&self) -> Arc<dyn ReplicationStatsInterface> {
        self.replication_stats.clone()
    }

    pub fn boot_time(&self) -> Arc<dyn BootTimeInterface> {
        self.boot_time.clone()
    }

    pub fn scanner_metrics(&self) -> Arc<dyn ScannerMetricsInterface> {
        self.scanner_metrics.clone()
    }

    pub fn endpoints(&self) -> Arc<dyn EndpointsInterface> {
        self.endpoints.clone()
    }

    pub fn deployment_id(&self) -> Arc<dyn DeploymentIdInterface> {
        self.deployment_id.clone()
    }

    pub fn runtime_port(&self) -> Arc<dyn RuntimePortInterface> {
        self.runtime_port.clone()
    }

    pub fn lock_client(&self) -> Arc<dyn LockClientInterface> {
        self.lock_client.clone()
    }

    pub fn lock_clients(&self) -> Arc<dyn LockClientsInterface> {
        self.lock_clients.clone()
    }

    pub fn performance_metrics(&self) -> Arc<dyn PerformanceMetricsInterface> {
        self.performance_metrics.clone()
    }

    pub fn internode_metrics(&self) -> Arc<dyn InternodeMetricsInterface> {
        self.internode_metrics.clone()
    }

    pub fn s3select_db(&self) -> Arc<dyn S3SelectDbInterface> {
        self.s3select_db.clone()
    }

    pub fn local_node_name(&self) -> Arc<dyn LocalNodeNameInterface> {
        self.local_node_name.clone()
    }

    pub fn action_credentials(&self) -> Arc<dyn ActionCredentialInterface> {
        self.action_credentials.clone()
    }

    pub fn region(&self) -> Arc<dyn RegionInterface> {
        self.region.clone()
    }

    pub fn tier_config(&self) -> Arc<dyn TierConfigInterface> {
        self.tier_config.clone()
    }

    pub fn expiry_state(&self) -> Arc<dyn ExpiryStateInterface> {
        self.expiry_state.clone()
    }

    pub fn transition_state(&self) -> Arc<dyn TransitionStateInterface> {
        self.transition_state.clone()
    }

    pub fn server_config(&self) -> Arc<dyn ServerConfigInterface> {
        self.server_config.clone()
    }

    pub fn storage_class(&self) -> Arc<dyn StorageClassInterface> {
        self.storage_class.clone()
    }

    pub fn buffer_config(&self) -> Arc<dyn BufferConfigInterface> {
        self.buffer_config.clone()
    }

    pub(crate) fn object_data_cache(&self) -> Arc<ObjectDataCacheAdapter> {
        Arc::clone(&self.object_data_cache)
    }
}

#[cfg(test)]
pub(super) struct AppContextTestInterfaces {
    pub(super) iam: Arc<dyn IamInterface>,
    pub(super) oidc: Arc<dyn OidcInterface>,
    pub(super) kms: Arc<dyn KmsInterface>,
    pub(super) kms_runtime: Arc<dyn KmsRuntimeInterface>,
    pub(super) outbound_tls_runtime: Arc<dyn OutboundTlsRuntimeInterface>,
    pub(super) notify: Arc<dyn NotifyInterface>,
    pub(super) notification_system: Arc<dyn NotificationSystemInterface>,
    pub(super) bucket_metadata: Arc<dyn BucketMetadataInterface>,
    pub(super) bucket_monitor: Arc<dyn BucketMonitorInterface>,
    pub(super) replication_pool: Arc<dyn ReplicationPoolInterface>,
    pub(super) replication_stats: Arc<dyn ReplicationStatsInterface>,
    pub(super) boot_time: Arc<dyn BootTimeInterface>,
    pub(super) scanner_metrics: Arc<dyn ScannerMetricsInterface>,
    pub(super) endpoints: Arc<dyn EndpointsInterface>,
    pub(super) deployment_id: Arc<dyn DeploymentIdInterface>,
    pub(super) runtime_port: Arc<dyn RuntimePortInterface>,
    pub(super) lock_client: Arc<dyn LockClientInterface>,
    pub(super) lock_clients: Arc<dyn LockClientsInterface>,
    pub(super) performance_metrics: Arc<dyn PerformanceMetricsInterface>,
    pub(super) internode_metrics: Arc<dyn InternodeMetricsInterface>,
    pub(super) s3select_db: Arc<dyn S3SelectDbInterface>,
    pub(super) local_node_name: Arc<dyn LocalNodeNameInterface>,
    pub(super) action_credentials: Arc<dyn ActionCredentialInterface>,
    pub(super) region: Arc<dyn RegionInterface>,
    pub(super) tier_config: Arc<dyn TierConfigInterface>,
    pub(super) expiry_state: Arc<dyn ExpiryStateInterface>,
    pub(super) transition_state: Arc<dyn TransitionStateInterface>,
    pub(super) server_config: Arc<dyn ServerConfigInterface>,
    pub(super) storage_class: Arc<dyn StorageClassInterface>,
    pub(super) buffer_config: Arc<dyn BufferConfigInterface>,
}

#[cfg(test)]
impl AppContext {
    pub(super) fn with_test_interfaces(object_store: Arc<ECStore>, interfaces: AppContextTestInterfaces) -> Self {
        Self {
            object_store,
            iam: interfaces.iam,
            oidc: interfaces.oidc,
            kms: interfaces.kms,
            kms_runtime: interfaces.kms_runtime,
            outbound_tls_runtime: interfaces.outbound_tls_runtime,
            notify: interfaces.notify,
            notification_system: interfaces.notification_system,
            bucket_metadata: interfaces.bucket_metadata,
            bucket_monitor: interfaces.bucket_monitor,
            replication_pool: interfaces.replication_pool,
            replication_stats: interfaces.replication_stats,
            boot_time: interfaces.boot_time,
            scanner_metrics: interfaces.scanner_metrics,
            endpoints: interfaces.endpoints,
            deployment_id: interfaces.deployment_id,
            runtime_port: interfaces.runtime_port,
            lock_client: interfaces.lock_client,
            lock_clients: interfaces.lock_clients,
            performance_metrics: interfaces.performance_metrics,
            internode_metrics: interfaces.internode_metrics,
            s3select_db: interfaces.s3select_db,
            local_node_name: interfaces.local_node_name,
            action_credentials: interfaces.action_credentials,
            region: interfaces.region,
            tier_config: interfaces.tier_config,
            expiry_state: interfaces.expiry_state,
            transition_state: interfaces.transition_state,
            server_config: interfaces.server_config,
            storage_class: interfaces.storage_class,
            buffer_config: interfaces.buffer_config,
            object_data_cache: ObjectDataCacheAdapter::disabled_arc(),
        }
    }
}

static APP_CONTEXT_SINGLETON: OnceLock<Arc<AppContext>> = OnceLock::new();

/// Initialize global application context once and return the canonical instance.
pub fn init_global_app_context(context: AppContext) -> Arc<AppContext> {
    let context = APP_CONTEXT_SINGLETON.get_or_init(|| Arc::new(context)).clone();
    let resolver_context = context.clone();
    let _ = set_object_store_resolver(Arc::new(move || Some(resolver_context.object_store())));
    context
}

/// Get global application context if it has been initialized.
pub fn get_global_app_context() -> Option<Arc<AppContext>> {
    APP_CONTEXT_SINGLETON.get().cloned()
}
