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

use super::global::{AppContext, get_global_app_context};
use super::handles::{
    default_bucket_metadata_interface, default_endpoints_interface, default_kms_runtime_interface,
    default_server_config_interface, default_tier_config_interface,
};
#[cfg(test)]
use crate::config::RustFSBufferConfig;
use rustfs_config::server_config::Config;
use rustfs_ecstore::bucket::metadata_sys::BucketMetadataSys;
use rustfs_ecstore::endpoints::EndpointServerPools;
use rustfs_ecstore::new_object_layer_fn;
use rustfs_ecstore::store::ECStore;
use rustfs_ecstore::tier::tier::TierConfigMgr;
use rustfs_kms::KmsServiceManager;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Resolve KMS runtime service manager using AppContext-first precedence.
pub fn resolve_kms_runtime_service_manager() -> Option<Arc<KmsServiceManager>> {
    resolve_kms_runtime_service_manager_with(get_global_app_context(), || default_kms_runtime_interface().service_manager())
}

/// Resolve bucket metadata handle using AppContext-first precedence.
pub fn resolve_bucket_metadata_handle() -> Option<Arc<RwLock<BucketMetadataSys>>> {
    resolve_bucket_metadata_handle_with(get_global_app_context(), || default_bucket_metadata_interface().handle())
}

/// Resolve object store handle using AppContext-first precedence.
pub fn resolve_object_store_handle() -> Option<Arc<ECStore>> {
    resolve_object_store_handle_with(get_global_app_context(), new_object_layer_fn)
}

/// Resolve endpoints using AppContext-first precedence.
pub fn resolve_endpoints_handle() -> Option<EndpointServerPools> {
    resolve_endpoints_handle_with(get_global_app_context(), || default_endpoints_interface().handle())
}

/// Resolve tier config handle using AppContext-first precedence.
pub fn resolve_tier_config_handle() -> Arc<RwLock<TierConfigMgr>> {
    resolve_tier_config_handle_with(get_global_app_context(), || default_tier_config_interface().handle())
}

/// Resolve server config using AppContext-first precedence.
pub fn resolve_server_config() -> Option<Config> {
    resolve_server_config_with(get_global_app_context(), || default_server_config_interface().get())
}

fn resolve_kms_runtime_service_manager_with(
    context: Option<Arc<AppContext>>,
    fallback: impl FnOnce() -> Option<Arc<KmsServiceManager>>,
) -> Option<Arc<KmsServiceManager>> {
    context
        .and_then(|context| context.kms_runtime().service_manager())
        .or_else(fallback)
}

fn resolve_bucket_metadata_handle_with(
    context: Option<Arc<AppContext>>,
    fallback: impl FnOnce() -> Option<Arc<RwLock<BucketMetadataSys>>>,
) -> Option<Arc<RwLock<BucketMetadataSys>>> {
    context
        .and_then(|context| context.bucket_metadata().handle())
        .or_else(fallback)
}

fn resolve_object_store_handle_with(
    context: Option<Arc<AppContext>>,
    fallback: impl FnOnce() -> Option<Arc<ECStore>>,
) -> Option<Arc<ECStore>> {
    context.map(|context| context.object_store()).or_else(fallback)
}

fn resolve_endpoints_handle_with(
    context: Option<Arc<AppContext>>,
    fallback: impl FnOnce() -> Option<EndpointServerPools>,
) -> Option<EndpointServerPools> {
    context.and_then(|context| context.endpoints().handle()).or_else(fallback)
}

fn resolve_tier_config_handle_with(
    context: Option<Arc<AppContext>>,
    fallback: impl FnOnce() -> Arc<RwLock<TierConfigMgr>>,
) -> Arc<RwLock<TierConfigMgr>> {
    context.map(|context| context.tier_config().handle()).unwrap_or_else(fallback)
}

fn resolve_server_config_with(context: Option<Arc<AppContext>>, fallback: impl FnOnce() -> Option<Config>) -> Option<Config> {
    context.map_or_else(fallback, |context| context.server_config().get())
}

#[cfg(test)]
fn resolve_buffer_config_with(
    context: Option<Arc<AppContext>>,
    fallback: impl FnOnce() -> RustFSBufferConfig,
) -> RustFSBufferConfig {
    context.map_or_else(fallback, |context| context.buffer_config().get())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::context::global::AppContextTestInterfaces;
    use crate::app::context::handles::{default_notify_interface, default_region_interface};
    use crate::app::context::interfaces::{
        BucketMetadataInterface, BufferConfigInterface, EndpointsInterface, IamInterface, KmsInterface, KmsRuntimeInterface,
        ServerConfigInterface, TierConfigInterface,
    };
    use crate::config::{RustFSBufferConfig, WorkloadProfile};
    use rustfs_ecstore::disk::endpoint::Endpoint;
    use rustfs_ecstore::endpoints::{Endpoints, PoolEndpoints};
    use rustfs_ecstore::store::init_local_disks;
    use rustfs_iam::{store::object::ObjectStore, sys::IamSys};
    use std::path::PathBuf;
    use tempfile::TempDir;
    use tokio_util::sync::CancellationToken;

    struct TestIamInterface;

    impl IamInterface for TestIamInterface {
        fn handle(&self) -> Arc<IamSys<ObjectStore>> {
            unreachable!("resolver tests do not need an IAM handle")
        }

        fn is_ready(&self) -> bool {
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

    struct TestBucketMetadataInterface {
        metadata: Option<Arc<RwLock<BucketMetadataSys>>>,
    }

    impl BucketMetadataInterface for TestBucketMetadataInterface {
        fn handle(&self) -> Option<Arc<RwLock<BucketMetadataSys>>> {
            self.metadata.clone()
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

    struct TestTierConfigInterface {
        tier_config: Arc<RwLock<TierConfigMgr>>,
    }

    impl TierConfigInterface for TestTierConfigInterface {
        fn handle(&self) -> Arc<RwLock<TierConfigMgr>> {
            self.tier_config.clone()
        }
    }

    struct TestServerConfigInterface {
        config: Option<Config>,
    }

    impl ServerConfigInterface for TestServerConfigInterface {
        fn get(&self) -> Option<Config> {
            self.config.clone()
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

        if let Some(store) = new_object_layer_fn() {
            return (temp_dir, store, endpoint_pools);
        }

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

    #[tokio::test]
    async fn resolver_helpers_are_context_first_and_fallback_when_context_is_absent() {
        let (_temp_dir, object_store, endpoints) = test_store().await;
        let context_kms = Arc::new(KmsServiceManager::new());
        let fallback_kms = Arc::new(KmsServiceManager::new());
        let bucket_metadata = Arc::new(RwLock::new(BucketMetadataSys::new(object_store.clone())));
        let tier_config = TierConfigMgr::new();
        let server_config = Config::new();
        let buffer_config = RustFSBufferConfig::new(WorkloadProfile::AiTraining);

        let context = Arc::new(AppContext::with_test_interfaces(
            object_store.clone(),
            AppContextTestInterfaces {
                iam: Arc::new(TestIamInterface),
                kms: Arc::new(TestKmsInterface {
                    kms: context_kms.clone(),
                }),
                kms_runtime: Arc::new(TestKmsRuntimeInterface {
                    kms: Some(context_kms.clone()),
                }),
                notify: default_notify_interface(),
                bucket_metadata: Arc::new(TestBucketMetadataInterface {
                    metadata: Some(bucket_metadata.clone()),
                }),
                endpoints: Arc::new(TestEndpointsInterface {
                    endpoints: Some(endpoints.clone()),
                }),
                region: default_region_interface(),
                tier_config: Arc::new(TestTierConfigInterface {
                    tier_config: tier_config.clone(),
                }),
                server_config: Arc::new(TestServerConfigInterface {
                    config: Some(server_config.clone()),
                }),
                buffer_config: Arc::new(TestBufferConfigInterface { config: buffer_config }),
            },
        ));

        assert!(Arc::ptr_eq(
            &resolve_kms_runtime_service_manager_with(Some(context.clone()), || Some(fallback_kms.clone()))
                .expect("context KMS runtime"),
            &context_kms
        ));
        assert!(Arc::ptr_eq(
            &resolve_bucket_metadata_handle_with(Some(context.clone()), || None).expect("context bucket metadata"),
            &bucket_metadata
        ));
        assert!(Arc::ptr_eq(
            &resolve_object_store_handle_with(Some(context.clone()), || None).expect("context object store"),
            &object_store
        ));
        assert_eq!(
            resolve_endpoints_handle_with(Some(context.clone()), || None)
                .expect("context endpoints")
                .as_ref()[0]
                .drives_per_set,
            endpoints.as_ref()[0].drives_per_set
        );
        assert!(Arc::ptr_eq(
            &resolve_tier_config_handle_with(Some(context.clone()), TierConfigMgr::new),
            &tier_config
        ));
        assert_eq!(
            resolve_server_config_with(Some(context.clone()), || None).expect("context server config"),
            server_config
        );
        assert_eq!(
            resolve_buffer_config_with(Some(context), || RustFSBufferConfig::new(WorkloadProfile::GeneralPurpose)).workload,
            WorkloadProfile::AiTraining
        );

        assert!(Arc::ptr_eq(
            &resolve_kms_runtime_service_manager_with(None, || Some(fallback_kms.clone())).expect("fallback KMS runtime"),
            &fallback_kms
        ));
        assert!(Arc::ptr_eq(
            &resolve_bucket_metadata_handle_with(None, || Some(bucket_metadata.clone())).expect("fallback bucket metadata"),
            &bucket_metadata
        ));
        assert!(Arc::ptr_eq(
            &resolve_object_store_handle_with(None, || Some(object_store.clone())).expect("fallback object store"),
            &object_store
        ));
        assert_eq!(
            resolve_endpoints_handle_with(None, || Some(endpoints.clone()))
                .expect("fallback endpoints")
                .as_ref()[0]
                .drives_per_set,
            endpoints.as_ref()[0].drives_per_set
        );
        assert!(Arc::ptr_eq(&resolve_tier_config_handle_with(None, || tier_config.clone()), &tier_config));
        assert_eq!(
            resolve_server_config_with(None, || Some(server_config.clone())).expect("fallback server config"),
            server_config
        );
        assert_eq!(
            resolve_buffer_config_with(None, || RustFSBufferConfig::new(WorkloadProfile::DataAnalytics)).workload,
            WorkloadProfile::DataAnalytics
        );
    }
}
