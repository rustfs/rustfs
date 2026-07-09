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
use super::global::{AppContext, publish_global_app_context};
use super::runtime_sources;
use super::server_slot::ServerContextSlot;
use rustfs_kms::KmsServiceManager;
use std::io::Result;
use std::sync::Arc;

impl AppContext {
    pub(crate) fn ensure_startup_kms_interface() -> Arc<KmsServiceManager> {
        ensure_startup_kms_interface_with(runtime_sources::kms_service_manager, runtime_sources::init_kms_service_manager)
    }

    pub(crate) fn ensure_startup_after_iam(
        store: Arc<ECStore>,
        kms_interface: Arc<KmsServiceManager>,
        server_ctx: &ServerContextSlot,
        iam: Arc<rustfs_iam::sys::IamSys<rustfs_iam::store::object::ObjectStore>>,
    ) -> Result<()> {
        // Each server constructs its OWN application context around its own
        // store, IAM system, and KMS interface (backlog#1052 S6), then installs
        // it into its own slot so its request path resolves its own state. It
        // also publishes to the process default (first server wins) so legacy
        // free-function readers keep resolving the first server's context.
        let context = Arc::new(AppContext::with_default_interfaces(store, iam, kms_interface));
        publish_global_app_context(context.clone());
        let _ = server_ctx.install(context);
        Ok(())
    }
}

fn ensure_startup_kms_interface_with<T, GetManager, InitManager>(get_manager: GetManager, init_manager: InitManager) -> Arc<T>
where
    GetManager: FnOnce() -> Option<Arc<T>>,
    InitManager: FnOnce() -> Arc<T>,
{
    get_manager().unwrap_or_else(init_manager)
}

#[cfg(test)]
mod tests {
    use super::ensure_startup_kms_interface_with;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    #[test]
    fn startup_kms_interface_reuses_existing_manager() {
        let existing = Arc::new(7usize);
        let init_calls = Arc::new(AtomicUsize::new(0));
        let init_calls_for_assert = init_calls.clone();

        let manager = ensure_startup_kms_interface_with(
            || Some(existing.clone()),
            move || {
                init_calls.fetch_add(1, Ordering::SeqCst);
                Arc::new(9usize)
            },
        );

        assert_eq!(*manager, 7);
        assert_eq!(init_calls_for_assert.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn startup_kms_interface_initializes_missing_manager() {
        let init_calls = Arc::new(AtomicUsize::new(0));
        let init_calls_for_assert = init_calls.clone();

        let manager = ensure_startup_kms_interface_with(
            || None,
            move || {
                init_calls.fetch_add(1, Ordering::SeqCst);
                Arc::new(9usize)
            },
        );

        assert_eq!(*manager, 9);
        assert_eq!(init_calls_for_assert.load(Ordering::SeqCst), 1);
    }
}
