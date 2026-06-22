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
    IamHandle, KmsHandle, default_bucket_metadata_interface, default_buffer_config_interface, default_endpoints_interface,
    default_kms_runtime_interface, default_notify_interface, default_region_interface, default_server_config_interface,
    default_tier_config_interface,
};
use super::interfaces::{
    BucketMetadataInterface, BufferConfigInterface, EndpointsInterface, IamInterface, KmsInterface, KmsRuntimeInterface,
    NotifyInterface, RegionInterface, ServerConfigInterface, TierConfigInterface,
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

    #[allow(dead_code)]
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

#[cfg(test)]
pub(super) struct AppContextTestInterfaces {
    pub(super) iam: Arc<dyn IamInterface>,
    pub(super) kms: Arc<dyn KmsInterface>,
    pub(super) kms_runtime: Arc<dyn KmsRuntimeInterface>,
    pub(super) notify: Arc<dyn NotifyInterface>,
    pub(super) bucket_metadata: Arc<dyn BucketMetadataInterface>,
    pub(super) endpoints: Arc<dyn EndpointsInterface>,
    pub(super) region: Arc<dyn RegionInterface>,
    pub(super) tier_config: Arc<dyn TierConfigInterface>,
    pub(super) server_config: Arc<dyn ServerConfigInterface>,
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
            notify: interfaces.notify,
            bucket_metadata: interfaces.bucket_metadata,
            endpoints: interfaces.endpoints,
            region: interfaces.region,
            tier_config: interfaces.tier_config,
            server_config: interfaces.server_config,
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
