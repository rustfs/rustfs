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

use rustfs_ecstore::store::ECStore;
use rustfs_iam::{store::object::ObjectStore, sys::IamSys};
use rustfs_kms::KmsServiceManager;
use std::sync::{Arc, OnceLock};

/// IAM interface for application-layer use-cases.
pub trait IamInterface: Send + Sync {
    fn handle(&self) -> Arc<IamSys<ObjectStore>>;
}

/// KMS interface for application-layer use-cases.
pub trait KmsInterface: Send + Sync {
    fn handle(&self) -> Arc<KmsServiceManager>;
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

/// Application-layer context with explicit dependencies.
#[derive(Clone)]
pub struct AppContext {
    object_store: Arc<ECStore>,
    iam: Arc<dyn IamInterface>,
    kms: Arc<dyn KmsInterface>,
}

impl AppContext {
    pub fn new(object_store: Arc<ECStore>, iam: Arc<dyn IamInterface>, kms: Arc<dyn KmsInterface>) -> Self {
        Self { object_store, iam, kms }
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
