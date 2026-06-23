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
use super::super::metadata_sys::BucketMetadataSys;
use super::super::{BucketBandwidthMonitor, DynReplicationPool, NotificationSys};
use crate::config::RustFSBufferConfig;
use async_trait::async_trait;
use rustfs_config::server_config::Config;
use rustfs_credentials::Credentials;
use rustfs_iam::{store::object::ObjectStore, sys::IamSys};
use rustfs_kms::KmsServiceManager;
use rustfs_lock::LockClient;
use rustfs_notify::{EventArgs, NotificationError};
use rustfs_targets::{EventName, arn::TargetID};
use std::sync::Arc;
use tokio::sync::RwLock;

/// IAM interface for application-layer use-cases.
pub trait IamInterface: Send + Sync {
    #[allow(dead_code)]
    fn handle(&self) -> Arc<IamSys<ObjectStore>>;
    fn is_ready(&self) -> bool;
}

/// KMS interface for application-layer use-cases.
#[allow(dead_code)]
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

/// Notification system handle interface for admin peer orchestration.
pub trait NotificationSystemInterface: Send + Sync {
    fn handle(&self) -> Option<&'static NotificationSys>;
}

/// Bucket metadata interface for application-layer use-cases.
pub trait BucketMetadataInterface: Send + Sync {
    fn handle(&self) -> Option<Arc<RwLock<BucketMetadataSys>>>;
}

/// Bucket bandwidth monitor interface for admin metric integration.
pub trait BucketMonitorInterface: Send + Sync {
    fn handle(&self) -> Option<Arc<BucketBandwidthMonitor>>;
}

/// Replication pool interface for admin resync integration.
pub trait ReplicationPoolInterface: Send + Sync {
    fn handle(&self) -> Option<Arc<DynReplicationPool>>;
}

/// Endpoints interface for application-layer use-cases.
pub trait EndpointsInterface: Send + Sync {
    fn handle(&self) -> Option<EndpointServerPools>;
}

/// Deployment identity interface for admin topology integration.
pub trait DeploymentIdInterface: Send + Sync {
    fn get(&self) -> Option<String>;
}

/// Runtime port interface for admin topology integration.
pub trait RuntimePortInterface: Send + Sync {
    fn get(&self) -> u16;
}

/// Lock client interface for application-layer use-cases.
pub trait LockClientInterface: Send + Sync {
    fn handle(&self) -> Option<Arc<dyn LockClient>>;
}

/// Local node name interface for application-layer use-cases.
#[async_trait]
pub trait LocalNodeNameInterface: Send + Sync {
    async fn get(&self) -> String;
}

/// Action credentials interface for admin handler integration.
pub trait ActionCredentialInterface: Send + Sync {
    fn get(&self) -> Option<Credentials>;
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
