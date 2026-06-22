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

mod audit;
mod compress;
pub mod cors;
mod event;
mod http;
mod hybrid;
mod layer;
mod module_switch;
mod prefix;
mod readiness;
mod runtime;
mod service_state;
mod storage_compat;
pub mod tls_material;

use tracing::warn;

// Items used by main.rs (binary crate) and/or embedded.rs — must be fully pub.
pub use audit::{is_audit_module_enabled, refresh_audit_module_enabled, start_audit_system, stop_audit_system};
pub use event::{init_event_notifier, is_notify_module_enabled, refresh_notify_module_enabled, shutdown_event_notifier};
pub use http::start_http_server;
pub use prefix::LOGO;
pub use runtime::build_tokio_runtime;
pub use service_state::SHUTDOWN_TIMEOUT;
pub use service_state::ServiceState;
pub use service_state::ServiceStateManager;
pub use service_state::ShutdownSignal;
pub use service_state::wait_for_shutdown;

// Items only used within the library crate (admin handlers, server/http.rs, etc.).
pub(crate) use event::convert_ecstore_object_info;
pub(crate) use http::HeaderMapCarrier;
pub(crate) use http::active_http_requests;
pub(crate) use layer::RequestContextLayer;
pub(crate) use module_switch::{
    ModuleSwitchSnapshot, ModuleSwitchSource, PersistedModuleSwitches, current_module_switch_snapshot,
    refresh_persisted_module_switches_from_store, save_persisted_module_switches_to_store, validate_module_switch_update,
};
pub(crate) use prefix::{
    ADMIN_PREFIX, CONSOLE_PREFIX, FAVICON_PATH, HEALTH_COMPAT_LIVE_PATH, HEALTH_PREFIX, HEALTH_READY_PATH, LICENSE,
    MINIO_ADMIN_PREFIX, MINIO_ADMIN_V3_PREFIX, MINIO_HEALTH_LIVE_PATH, MINIO_HEALTH_READY_PATH, PROFILE_CPU_PATH,
    PROFILE_MEMORY_PATH, RPC_PREFIX, RUSTFS_ADMIN_PREFIX, TABLE_CATALOG_COMPAT_PREFIX, TABLE_CATALOG_PREFIX, TONIC_PREFIX,
    VERSION, has_path_prefix, is_admin_path, is_table_catalog_path,
};
pub(crate) use readiness::DependencyReadiness;
pub(crate) use readiness::DependencyReadinessReport;
pub(crate) use readiness::ReadinessDegradedReason;
pub(crate) use readiness::ReadinessGateLayer;
pub(crate) use readiness::collect_dependency_readiness;
pub(crate) use readiness::collect_dependency_readiness_report;
pub use readiness::publish_ready_when_runtime_ready;

#[derive(Clone, Copy, Debug)]
pub struct RemoteAddr(pub std::net::SocketAddr);

pub struct ShutdownHandle {
    shutdown_tx: Option<tokio::sync::broadcast::Sender<()>>,
    task_handle: Option<tokio::task::JoinHandle<()>>,
}

impl ShutdownHandle {
    pub fn new(shutdown_tx: tokio::sync::broadcast::Sender<()>, task_handle: tokio::task::JoinHandle<()>) -> Self {
        Self {
            shutdown_tx: Some(shutdown_tx),
            task_handle: Some(task_handle),
        }
    }

    pub fn signal(&self) {
        if let Some(tx) = &self.shutdown_tx {
            let _ = tx.send(());
        }
    }

    pub async fn shutdown(self) {
        self.signal();
        self.wait().await;
    }

    pub async fn wait(mut self) {
        if let Some(task_handle) = self.task_handle.take()
            && let Err(err) = task_handle.await
        {
            warn!(?err, "Server task join failed during shutdown");
        }
    }
}

impl Drop for ShutdownHandle {
    fn drop(&mut self) {
        if let Some(tx) = &self.shutdown_tx {
            let _ = tx.send(());
        }
    }
}
