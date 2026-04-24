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
mod prefix;
mod readiness;
mod runtime;
mod service_state;
pub mod tls_material;

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
pub(crate) use prefix::{
    ADMIN_PREFIX, CONSOLE_PREFIX, FAVICON_PATH, HEALTH_PREFIX, HEALTH_READY_PATH, LICENSE, MINIO_ADMIN_PREFIX,
    MINIO_ADMIN_V3_PREFIX, PROFILE_CPU_PATH, PROFILE_MEMORY_PATH, RPC_PREFIX, RUSTFS_ADMIN_PREFIX, TONIC_PREFIX, VERSION,
};
pub(crate) use readiness::ReadinessGateLayer;

#[derive(Clone, Copy, Debug)]
pub struct RemoteAddr(pub std::net::SocketAddr);
