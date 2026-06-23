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

use super::super::{ECStore, set_object_store_resolver};
use super::handles::{
    IamHandle, KmsHandle, default_action_credential_interface, default_boot_time_interface, default_bucket_metadata_interface,
    default_bucket_monitor_interface, default_buffer_config_interface, default_deployment_id_interface,
    default_endpoints_interface, default_kms_runtime_interface, default_local_node_name_interface, default_lock_client_interface,
    default_notification_system_interface, default_notify_interface, default_outbound_tls_runtime_interface,
    default_region_interface, default_replication_pool_interface, default_replication_stats_interface,
    default_runtime_port_interface, default_scanner_metrics_interface, default_server_config_interface,
    default_storage_class_interface, default_tier_config_interface, default_tier_stats_interface,
};
use super::interfaces::{
    ActionCredentialInterface, BootTimeInterface, BucketMetadataInterface, BucketMonitorInterface, BufferConfigInterface,
    DeploymentIdInterface, EndpointsInterface, IamInterface, KmsInterface, KmsRuntimeInterface, LocalNodeNameInterface,
    LockClientInterface, NotificationSystemInterface, NotifyInterface, OutboundTlsRuntimeInterface, RegionInterface,
    ReplicationPoolInterface, ReplicationStatsInterface, RuntimePortInterface, ScannerMetricsInterface, ServerConfigInterface,
    StorageClassInterface, TierConfigInterface, TierStatsInterface,
};
use rustfs_iam::{store::object::ObjectStore, sys::IamSys};
use rustfs_kms::KmsServiceManager;
use std::sync::{Arc, OnceLock};

/// Application-layer context with explicit dependencies.
#[derive(Clone)]
pub struct AppContext {
    object_store: Arc<ECStore>,
    iam: Arc<dyn IamInterface>,
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
    tier_stats: Arc<dyn TierStatsInterface>,
    scanner_metrics: Arc<dyn ScannerMetricsInterface>,
    endpoints: Arc<dyn EndpointsInterface>,
    deployment_id: Arc<dyn DeploymentIdInterface>,
    runtime_port: Arc<dyn RuntimePortInterface>,
    lock_client: Arc<dyn LockClientInterface>,
    local_node_name: Arc<dyn LocalNodeNameInterface>,
    action_credentials: Arc<dyn ActionCredentialInterface>,
    region: Arc<dyn RegionInterface>,
    tier_config: Arc<dyn TierConfigInterface>,
    server_config: Arc<dyn ServerConfigInterface>,
    storage_class: Arc<dyn StorageClassInterface>,
    buffer_config: Arc<dyn BufferConfigInterface>,
}

impl AppContext {
    pub fn new(object_store: Arc<ECStore>, iam: Arc<dyn IamInterface>, kms: Arc<dyn KmsInterface>) -> Self {
        Self {
            object_store,
            iam,
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
            tier_stats: default_tier_stats_interface(),
            scanner_metrics: default_scanner_metrics_interface(),
            endpoints: default_endpoints_interface(),
            deployment_id: default_deployment_id_interface(),
            runtime_port: default_runtime_port_interface(),
            lock_client: default_lock_client_interface(),
            local_node_name: default_local_node_name_interface(),
            action_credentials: default_action_credential_interface(),
            region: default_region_interface(),
            tier_config: default_tier_config_interface(),
            server_config: default_server_config_interface(),
            storage_class: default_storage_class_interface(),
            buffer_config: default_buffer_config_interface(),
        }
    }

    pub fn with_default_interfaces(
        object_store: Arc<ECStore>,
        iam: Arc<IamSys<ObjectStore>>,
        kms: Arc<KmsServiceManager>,
    ) -> Self {
        Self::new(object_store, Arc::new(IamHandle::new(iam)), Arc::new(KmsHandle::new(kms)))
    }

    pub fn object_store(&self) -> Arc<ECStore> {
        self.object_store.clone()
    }

    pub fn iam(&self) -> Arc<dyn IamInterface> {
        self.iam.clone()
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

    pub fn replication_pool(&self) -> Arc<dyn ReplicationPoolInterface> {
        self.replication_pool.clone()
    }

    pub fn replication_stats(&self) -> Arc<dyn ReplicationStatsInterface> {
        self.replication_stats.clone()
    }

    pub fn boot_time(&self) -> Arc<dyn BootTimeInterface> {
        self.boot_time.clone()
    }

    pub fn tier_stats(&self) -> Arc<dyn TierStatsInterface> {
        self.tier_stats.clone()
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

    pub fn server_config(&self) -> Arc<dyn ServerConfigInterface> {
        self.server_config.clone()
    }

    pub fn storage_class(&self) -> Arc<dyn StorageClassInterface> {
        self.storage_class.clone()
    }

    pub fn buffer_config(&self) -> Arc<dyn BufferConfigInterface> {
        self.buffer_config.clone()
    }
}

#[cfg(test)]
pub(super) struct AppContextTestInterfaces {
    pub(super) iam: Arc<dyn IamInterface>,
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
    pub(super) tier_stats: Arc<dyn TierStatsInterface>,
    pub(super) scanner_metrics: Arc<dyn ScannerMetricsInterface>,
    pub(super) endpoints: Arc<dyn EndpointsInterface>,
    pub(super) deployment_id: Arc<dyn DeploymentIdInterface>,
    pub(super) runtime_port: Arc<dyn RuntimePortInterface>,
    pub(super) lock_client: Arc<dyn LockClientInterface>,
    pub(super) local_node_name: Arc<dyn LocalNodeNameInterface>,
    pub(super) action_credentials: Arc<dyn ActionCredentialInterface>,
    pub(super) region: Arc<dyn RegionInterface>,
    pub(super) tier_config: Arc<dyn TierConfigInterface>,
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
            tier_stats: interfaces.tier_stats,
            scanner_metrics: interfaces.scanner_metrics,
            endpoints: interfaces.endpoints,
            deployment_id: interfaces.deployment_id,
            runtime_port: interfaces.runtime_port,
            lock_client: interfaces.lock_client,
            local_node_name: interfaces.local_node_name,
            action_credentials: interfaces.action_credentials,
            region: interfaces.region,
            tier_config: interfaces.tier_config,
            server_config: interfaces.server_config,
            storage_class: interfaces.storage_class,
            buffer_config: interfaces.buffer_config,
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
