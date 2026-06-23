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
use super::super::TierConfigMgr;
use super::super::metadata_sys::{BucketMetadataSys, get_global_bucket_metadata_sys};
use super::super::{
    get_global_bucket_monitor, get_global_deployment_id, get_global_endpoints_opt, get_global_lock_client,
    get_global_notification_sys, get_global_region, get_global_replication_pool, get_global_tier_config_mgr, global_rustfs_port,
};
use super::interfaces::{
    ActionCredentialInterface, BucketMetadataInterface, BucketMonitorInterface, BufferConfigInterface, DeploymentIdInterface,
    EndpointsInterface, IamInterface, KmsInterface, KmsRuntimeInterface, LocalNodeNameInterface, LockClientInterface,
    NotificationSystemInterface, NotifyInterface, RegionInterface, ReplicationPoolInterface, RuntimePortInterface,
    ServerConfigInterface, TierConfigInterface,
};
use crate::config::{RustFSBufferConfig, get_global_buffer_config};
use async_trait::async_trait;
use rustfs_common::get_global_local_node_name;
use rustfs_config::server_config::Config;
use rustfs_config::server_config::get_global_server_config;
use rustfs_credentials::{Credentials, get_global_action_cred};
use rustfs_iam::{store::object::ObjectStore, sys::IamSys};
use rustfs_kms::{KmsServiceManager, get_global_kms_service_manager};
use rustfs_lock::LockClient;
use rustfs_notify::{EventArgs, NotificationError, notifier_global};
use rustfs_targets::{EventName, arn::TargetID};
use std::sync::Arc;
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

/// Default server config interface adapter.
#[derive(Default)]
pub struct ServerConfigHandle;

impl ServerConfigInterface for ServerConfigHandle {
    fn get(&self) -> Option<Config> {
        get_global_server_config()
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

pub fn default_bucket_metadata_interface() -> Arc<dyn BucketMetadataInterface> {
    Arc::new(BucketMetadataHandle)
}

pub fn default_bucket_monitor_interface() -> Arc<dyn BucketMonitorInterface> {
    Arc::new(BucketMonitorHandle)
}

pub fn default_replication_pool_interface() -> Arc<dyn ReplicationPoolInterface> {
    Arc::new(ReplicationPoolHandle)
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

pub fn default_local_node_name_interface() -> Arc<dyn LocalNodeNameInterface> {
    Arc::new(LocalNodeNameHandle)
}

pub fn default_action_credential_interface() -> Arc<dyn ActionCredentialInterface> {
    Arc::new(ActionCredentialHandle)
}

pub fn default_region_interface() -> Arc<dyn RegionInterface> {
    Arc::new(RegionHandle)
}

pub fn default_tier_config_interface() -> Arc<dyn TierConfigInterface> {
    Arc::new(TierConfigHandle)
}

pub fn default_server_config_interface() -> Arc<dyn ServerConfigInterface> {
    Arc::new(ServerConfigHandle)
}

pub fn default_buffer_config_interface() -> Arc<dyn BufferConfigInterface> {
    Arc::new(BufferConfigHandle)
}
