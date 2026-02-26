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
#![allow(dead_code)]

use crate::config::workload_profiles::{RustFSBufferConfig, get_global_buffer_config};
use async_trait::async_trait;
use rustfs_ecstore::bucket::metadata_sys::{BucketMetadataSys, get_global_bucket_metadata_sys};
use rustfs_ecstore::config::{Config, get_global_server_config};
use rustfs_ecstore::endpoints::EndpointServerPools;
use rustfs_ecstore::global::{get_global_endpoints_opt, get_global_region, get_global_tier_config_mgr};
use rustfs_ecstore::store::ECStore;
use rustfs_ecstore::tier::tier::TierConfigMgr;
use rustfs_iam::{store::object::ObjectStore, sys::IamSys};
use rustfs_kms::{KmsServiceManager, get_global_kms_service_manager};
use rustfs_notify::{EventArgs, NotificationError, notifier_global};
use rustfs_targets::{EventName, arn::TargetID};
use std::sync::{Arc, OnceLock};
use tokio::sync::RwLock;

/// IAM interface for application-layer use-cases.
pub trait IamInterface: Send + Sync {
    fn handle(&self) -> Arc<IamSys<ObjectStore>>;
    fn is_ready(&self) -> bool;
}

/// KMS interface for application-layer use-cases.
pub trait KmsInterface: Send + Sync {
    fn handle(&self) -> Arc<KmsServiceManager>;
}

/// KMS runtime interface for application-layer and admin handler integration.
pub trait KmsRuntimeInterface: Send + Sync {
    fn service_manager(&self) -> Option<Arc<KmsServiceManager>>;
}

/// Notify interface for application-layer use-cases.
#[async_trait]
pub trait NotifyInterface: Send + Sync {
    async fn notify(&self, args: EventArgs);

    async fn add_event_specific_rules(
        &self,
        bucket_name: &str,
        region: &str,
        event_rules: &[(Vec<EventName>, String, String, Vec<TargetID>)],
    ) -> Result<(), NotificationError>;

    async fn clear_bucket_notification_rules(&self, bucket_name: &str) -> Result<(), NotificationError>;
}

/// Bucket metadata interface for application-layer use-cases.
pub trait BucketMetadataInterface: Send + Sync {
    fn handle(&self) -> Option<Arc<RwLock<BucketMetadataSys>>>;
}

/// Endpoints interface for application-layer use-cases.
pub trait EndpointsInterface: Send + Sync {
    fn handle(&self) -> Option<EndpointServerPools>;
}

/// Region interface for application-layer use-cases.
pub trait RegionInterface: Send + Sync {
    fn get(&self) -> Option<s3s::region::Region>;
}

/// Tier config interface for application-layer and admin handlers.
pub trait TierConfigInterface: Send + Sync {
    fn handle(&self) -> Arc<RwLock<TierConfigMgr>>;
}

/// Server config interface for application-layer and server modules.
pub trait ServerConfigInterface: Send + Sync {
    fn get(&self) -> Option<Config>;
}

/// Buffer profile config interface for application-layer use-cases.
pub trait BufferConfigInterface: Send + Sync {
    fn get(&self) -> RustFSBufferConfig;
}

/// Default IAM interface adapter.
pub struct IamHandle {
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

/// Default bucket metadata interface adapter.
#[derive(Default)]
pub struct BucketMetadataHandle;

impl BucketMetadataInterface for BucketMetadataHandle {
    fn handle(&self) -> Option<Arc<RwLock<BucketMetadataSys>>> {
        get_global_bucket_metadata_sys()
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

/// Application-layer context with explicit dependencies.
#[derive(Clone)]
pub struct AppContext {
    object_store: Arc<ECStore>,
    iam: Arc<dyn IamInterface>,
    kms: Arc<dyn KmsInterface>,
    kms_runtime: Arc<dyn KmsRuntimeInterface>,
    notify: Arc<dyn NotifyInterface>,
    bucket_metadata: Arc<dyn BucketMetadataInterface>,
    endpoints: Arc<dyn EndpointsInterface>,
    region: Arc<dyn RegionInterface>,
    tier_config: Arc<dyn TierConfigInterface>,
    server_config: Arc<dyn ServerConfigInterface>,
    buffer_config: Arc<dyn BufferConfigInterface>,
}

impl AppContext {
    pub fn new(object_store: Arc<ECStore>, iam: Arc<dyn IamInterface>, kms: Arc<dyn KmsInterface>) -> Self {
        Self {
            object_store,
            iam,
            kms,
            kms_runtime: default_kms_runtime_interface(),
            notify: default_notify_interface(),
            bucket_metadata: default_bucket_metadata_interface(),
            endpoints: default_endpoints_interface(),
            region: default_region_interface(),
            tier_config: default_tier_config_interface(),
            server_config: default_server_config_interface(),
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

    pub fn kms(&self) -> Arc<dyn KmsInterface> {
        self.kms.clone()
    }

    pub fn kms_runtime(&self) -> Arc<dyn KmsRuntimeInterface> {
        self.kms_runtime.clone()
    }

    pub fn notify(&self) -> Arc<dyn NotifyInterface> {
        self.notify.clone()
    }

    pub fn bucket_metadata(&self) -> Arc<dyn BucketMetadataInterface> {
        self.bucket_metadata.clone()
    }

    pub fn endpoints(&self) -> Arc<dyn EndpointsInterface> {
        self.endpoints.clone()
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

    pub fn buffer_config(&self) -> Arc<dyn BufferConfigInterface> {
        self.buffer_config.clone()
    }
}

pub fn default_notify_interface() -> Arc<dyn NotifyInterface> {
    Arc::new(NotifyHandle)
}

pub fn default_kms_runtime_interface() -> Arc<dyn KmsRuntimeInterface> {
    Arc::new(KmsRuntimeHandle)
}

/// Resolve KMS runtime service manager using AppContext-first precedence.
pub fn resolve_kms_runtime_service_manager() -> Option<Arc<KmsServiceManager>> {
    get_global_app_context()
        .and_then(|context| context.kms_runtime().service_manager())
        .or_else(|| default_kms_runtime_interface().service_manager())
}

pub fn default_bucket_metadata_interface() -> Arc<dyn BucketMetadataInterface> {
    Arc::new(BucketMetadataHandle)
}

pub fn default_endpoints_interface() -> Arc<dyn EndpointsInterface> {
    Arc::new(EndpointsHandle)
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

static GLOBAL_APP_CONTEXT: OnceLock<Arc<AppContext>> = OnceLock::new();

/// Initialize global application context once and return the canonical instance.
pub fn init_global_app_context(context: AppContext) -> Arc<AppContext> {
    GLOBAL_APP_CONTEXT.get_or_init(|| Arc::new(context)).clone()
}

/// Get global application context if it has been initialized.
pub fn get_global_app_context() -> Option<Arc<AppContext>> {
    GLOBAL_APP_CONTEXT.get().cloned()
}
