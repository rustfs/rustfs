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

use crate::storage::ECStore;
use crate::{
    server::{ServiceStateManager, ShutdownHandle, wait_for_shutdown},
    startup_iam::{IamBootstrapDisposition, publish_ready_for_iam_bootstrap},
    startup_runtime_sources,
    startup_services::StartupServiceRuntime,
    startup_shutdown::run_startup_shutdown_sequence,
};
use rustfs_common::GlobalReadiness;
use rustfs_scanner::init_data_scanner;
use std::{
    io::Result,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio_util::sync::CancellationToken;
use tracing::info;

const LOG_COMPONENT_MAIN: &str = "main";
const LOG_COMPONENT_EMBEDDED: &str = "embedded";
const LOG_SUBSYSTEM_STARTUP: &str = "startup";
const LOG_SUBSYSTEM_EMBEDDED: &str = "embedded";
const EVENT_SERVER_READY: &str = "server_ready";
const EVENT_SERVER_SHUTDOWN_STATE: &str = "server_shutdown_state";
const EVENT_EMBEDDED_SERVER_STATE: &str = "embedded_server_state";

static EMBEDDED_SERVER_STARTED: AtomicBool = AtomicBool::new(false);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct EmbeddedStartupAlreadyStarted;

pub(crate) struct EmbeddedStartupGuard {
    global_init_started: bool,
}

impl EmbeddedStartupGuard {
    pub(crate) fn new() -> Self {
        Self {
            global_init_started: false,
        }
    }

    pub(crate) fn mark_global_init_started(&mut self) -> std::result::Result<(), EmbeddedStartupAlreadyStarted> {
        mark_embedded_global_init_started(&EMBEDDED_SERVER_STARTED, &mut self.global_init_started)
    }
}

impl Default for EmbeddedStartupGuard {
    fn default() -> Self {
        Self::new()
    }
}

fn mark_embedded_global_init_started(
    server_started: &AtomicBool,
    global_init_started: &mut bool,
) -> std::result::Result<(), EmbeddedStartupAlreadyStarted> {
    if *global_init_started {
        return Ok(());
    }

    server_started
        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        .map_err(|_| EmbeddedStartupAlreadyStarted)?;
    *global_init_started = true;
    Ok(())
}

pub(crate) struct StartupRuntimeLifecycle {
    pub(crate) server_address: String,
    pub(crate) state_manager: Arc<ServiceStateManager>,
    pub(crate) s3_shutdown_tx: Option<ShutdownHandle>,
    pub(crate) console_shutdown_tx: Option<ShutdownHandle>,
    pub(crate) service_runtime: StartupServiceRuntime,
    pub(crate) store: Arc<ECStore>,
    pub(crate) shutdown_token: CancellationToken,
    pub(crate) readiness: Arc<GlobalReadiness>,
}

pub(crate) async fn run_startup_runtime_lifecycle(lifecycle: StartupRuntimeLifecycle) -> Result<()> {
    let StartupRuntimeLifecycle {
        server_address,
        state_manager,
        s3_shutdown_tx,
        console_shutdown_tx,
        service_runtime,
        store,
        shutdown_token,
        readiness,
    } = lifecycle;
    let StartupServiceRuntime {
        optional_runtimes,
        iam_bootstrap,
        enable_scanner,
    } = service_runtime;

    info!(
        target: "rustfs::main::run",
        event = EVENT_SERVER_READY,
        component = LOG_COMPONENT_MAIN,
        subsystem = LOG_SUBSYSTEM_STARTUP,
        version = %crate::version::get_version(),
        server_address = %server_address,
        started_at = %jiff::Zoned::now(),
        iam_bootstrap = ?iam_bootstrap,
        "RustFS server ready"
    );
    publish_ready_for_iam_bootstrap(iam_bootstrap, readiness.as_ref(), Some(state_manager.as_ref())).await?;
    startup_runtime_sources::publish_init_time_now().await;

    if enable_scanner {
        init_data_scanner(shutdown_token.clone(), store).await;
    }

    let shutdown_signal = wait_for_shutdown().await;
    run_startup_shutdown_sequence(
        &state_manager,
        shutdown_signal,
        s3_shutdown_tx,
        console_shutdown_tx,
        optional_runtimes,
        shutdown_token,
    )
    .await;

    info!(
        target: "rustfs::main::run",
        event = EVENT_SERVER_SHUTDOWN_STATE,
        component = LOG_COMPONENT_MAIN,
        subsystem = LOG_SUBSYSTEM_STARTUP,
        state = ?state_manager.current_state(),
        result = "stopped",
        "RustFS server stopped"
    );
    Ok(())
}

pub(crate) async fn publish_embedded_startup_ready(
    iam_bootstrap: IamBootstrapDisposition,
    readiness: &GlobalReadiness,
) -> Result<()> {
    publish_ready_for_iam_bootstrap(iam_bootstrap, readiness, None).await?;
    startup_runtime_sources::publish_init_time_now().await;
    Ok(())
}

pub(crate) fn embedded_endpoint_address(address: SocketAddr) -> SocketAddr {
    let ip = match address.ip() {
        ip @ IpAddr::V4(v4) if !v4.is_unspecified() => ip,
        IpAddr::V4(_) => IpAddr::V4(Ipv4Addr::LOCALHOST),
        ip @ IpAddr::V6(v6) if !v6.is_unspecified() => ip,
        IpAddr::V6(_) => IpAddr::V6(Ipv6Addr::LOCALHOST),
    };

    SocketAddr::new(ip, address.port())
}

pub(crate) fn log_embedded_server_ready(endpoint_address: SocketAddr) {
    info!(
        target: "rustfs::embedded",
        component = LOG_COMPONENT_EMBEDDED,
        subsystem = LOG_SUBSYSTEM_EMBEDDED,
        event = EVENT_EMBEDDED_SERVER_STATE,
        state = "ready",
        "RustFS embedded server ready at http://{}",
        endpoint_address
    );
}

#[cfg(test)]
mod tests {
    use super::{embedded_endpoint_address, mark_embedded_global_init_started};
    use std::{
        net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
        sync::atomic::{AtomicBool, Ordering},
    };

    #[test]
    fn embedded_global_init_guard_allows_local_retry_before_mark() {
        let server_started = AtomicBool::new(false);
        let mut global_init_started = false;

        mark_embedded_global_init_started(&server_started, &mut global_init_started)
            .expect("first irreversible startup should mark global init");
        mark_embedded_global_init_started(&server_started, &mut global_init_started)
            .expect("repeated mark in same startup should be idempotent");

        assert!(global_init_started);
        assert!(server_started.load(Ordering::SeqCst));
    }

    #[test]
    fn embedded_global_init_guard_rejects_second_startup_after_mark() {
        let server_started = AtomicBool::new(true);
        let mut global_init_started = false;

        let result = mark_embedded_global_init_started(&server_started, &mut global_init_started);

        assert!(result.is_err());
        assert!(!global_init_started);
    }

    #[test]
    fn embedded_endpoint_address_rewrites_unspecified_hosts() {
        assert_eq!(
            embedded_endpoint_address(SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 9000)),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9000)
        );
        assert_eq!(
            embedded_endpoint_address(SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), 9001)),
            SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 9001)
        );
    }

    #[test]
    fn embedded_endpoint_address_preserves_bound_hosts() {
        assert_eq!(
            embedded_endpoint_address(SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 10)), 9000)),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 10)), 9000)
        );
        assert_eq!(
            embedded_endpoint_address(SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 9001)),
            SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 9001)
        );
    }
}
