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

use crate::storage_api::startup::lifecycle::ECStore;
use crate::{
    connect::runtime::shutdown_connect_runtimes,
    server::{
        SHUTDOWN_TIMEOUT, ServiceStateManager, ShutdownHandle, start_persisted_event_notifier_reconciler, wait_for_shutdown,
    },
    startup_iam::{IamBootstrapDisposition, publish_ready_for_iam_bootstrap},
    startup_runtime_sources,
    startup_services::StartupServiceRuntime,
    startup_shutdown::{StartupBackgroundTasks, run_startup_shutdown_sequence},
};
use rustfs_common::GlobalReadiness;
use rustfs_object_capacity::capacity_manager::CapacityBackgroundTasks;
use rustfs_scanner::init_scanner_with_recovery;
use std::{
    io::{Error, Result},
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

/// Guards against a second embedded server startup racing the *irreversible*
/// stages of the first one. backlog#1052 S1–S3 made the storage layer, the
/// request path, and the app subsystems either per-instance or tolerant of
/// double-init, so two embedded servers can coexist in the same process (the
/// legacy hard rejection is gone). The guard now only serializes access to
/// the shared bootstrap-context write-once cells during startup: the first
/// startup runs, and any startup that overlaps with it before it hands off
/// the bootstrap context is rejected — a narrow window that only matters if
/// two `RustFSServerBuilder::build()` futures race, which no supported API
/// path does today.
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

/// Release the embedded startup guard so a subsequent startup in the same
/// process can proceed (backlog#1052 S5). The guard only serializes the
/// bootstrap-context write window; once startup has handed off, another
/// server can begin.
pub(crate) fn release_embedded_startup_guard() {
    EMBEDDED_SERVER_STARTED.store(false, Ordering::SeqCst);
}

pub(crate) struct StartupRuntimeLifecycle {
    pub(crate) server_address: String,
    pub(crate) state_manager: Arc<ServiceStateManager>,
    pub(crate) s3_shutdown_tx: Option<ShutdownHandle>,
    pub(crate) console_shutdown_tx: Option<ShutdownHandle>,
    pub(crate) capacity_tasks: Option<CapacityBackgroundTasks>,
    pub(crate) service_runtime: StartupServiceRuntime,
    pub(crate) store: Arc<ECStore>,
    pub(crate) shutdown_token: CancellationToken,
    pub(crate) readiness: Arc<GlobalReadiness>,
}

#[derive(Debug)]
struct StartupShutdownFailure {
    capacity: Option<Error>,
    maintenance: Option<Error>,
    fronts: Option<crate::startup_shutdown::FrontShutdownFailure>,
    observability: Option<Box<dyn std::error::Error + Send + Sync>>,
}

impl StartupShutdownFailure {
    fn into_result(self) -> Result<()> {
        if self.capacity.is_none() && self.maintenance.is_none() && self.fronts.is_none() && self.observability.is_none() {
            return Ok(());
        }
        let kind = if self
            .capacity
            .as_ref()
            .is_some_and(|error| error.kind() == std::io::ErrorKind::TimedOut)
            || self
                .maintenance
                .as_ref()
                .is_some_and(|error| error.kind() == std::io::ErrorKind::TimedOut)
        {
            std::io::ErrorKind::TimedOut
        } else {
            self.capacity
                .as_ref()
                .or(self.maintenance.as_ref())
                .map_or(std::io::ErrorKind::Other, Error::kind)
        };
        Err(Error::new(kind, self))
    }
}

impl std::fmt::Display for StartupShutdownFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(capacity) = &self.capacity {
            write!(f, "{capacity}")?;
        }
        if let Some(maintenance) = &self.maintenance {
            if self.capacity.is_some() {
                write!(f, "; ")?;
            }
            write!(f, "{maintenance}")?;
        }
        if let Some(fronts) = &self.fronts {
            if self.capacity.is_some() || self.maintenance.is_some() {
                write!(f, "; ")?;
            }
            write!(f, "{fronts}")?;
        }
        if let Some(observability) = &self.observability {
            if self.capacity.is_some() || self.maintenance.is_some() || self.fronts.is_some() {
                write!(f, "; ")?;
            }
            write!(f, "shutdown_global_guard: {observability}")?;
        }
        Ok(())
    }
}

impl std::error::Error for StartupShutdownFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self.capacity.as_ref().or(self.maintenance.as_ref()) {
            Some(error) => Some(error),
            None => self
                .fronts
                .as_ref()
                .map(|error| error as &(dyn std::error::Error + 'static))
                .or_else(|| self.observability.as_ref().map(|error| error.as_ref() as _)),
        }
    }
}

pub(crate) async fn run_startup_runtime_lifecycle(lifecycle: StartupRuntimeLifecycle) -> Result<()> {
    let StartupRuntimeLifecycle {
        server_address,
        state_manager,
        s3_shutdown_tx,
        console_shutdown_tx,
        capacity_tasks,
        service_runtime,
        store,
        shutdown_token,
        readiness,
    } = lifecycle;
    let StartupServiceRuntime {
        table_maintenance,
        oidc_recovery,
        optional_runtimes,
        heartbeat,
        inventory,
        local_trace_capture,
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
    let event_notifier_reconciler = start_persisted_event_notifier_reconciler(store.clone(), shutdown_token.clone());

    let scanner_cleanup = init_scanner_with_recovery(shutdown_token.clone(), store, enable_scanner).await;

    let shutdown_signal = wait_for_shutdown().await;
    let (capacity_report, maintenance_result, front_result) = run_startup_shutdown_sequence(
        &state_manager,
        shutdown_signal,
        s3_shutdown_tx,
        console_shutdown_tx,
        optional_runtimes,
        StartupBackgroundTasks {
            capacity: capacity_tasks,
            table_maintenance,
        },
        shutdown_token,
    )
    .await;
    if let Some(recovery) = oidc_recovery
        && let Err(error) = recovery.await
    {
        tracing::warn!(
            event = EVENT_SERVER_SHUTDOWN_STATE,
            component = LOG_COMPONENT_MAIN,
            subsystem = LOG_SUBSYSTEM_STARTUP,
            result = "oidc_discovery_recovery_join_failed",
            error = %error,
            "OIDC discovery recovery task failed to join"
        );
    }
    shutdown_connect_runtimes(heartbeat, inventory).await;
    if let Some(runtime) = local_trace_capture {
        runtime.shutdown().await;
    }
    if let Some(cleanup) = scanner_cleanup {
        let _ = wait_for_scanner_cleanup(cleanup).await;
    }
    if let Err(err) = event_notifier_reconciler.await {
        tracing::warn!(
            target: "rustfs::main::run",
            event = "notify_runtime_reconcile",
            component = LOG_COMPONENT_MAIN,
            subsystem = LOG_SUBSYSTEM_STARTUP,
            state = "join_failed",
            reason = if err.is_cancelled() { "task_cancelled" } else { "task_panicked" },
            "Persisted notification runtime reconciler task failed to join"
        );
    }

    let capacity_error = capacity_report.finish().await.err();

    info!(
        target: "rustfs::main::run",
        event = EVENT_SERVER_SHUTDOWN_STATE,
        component = LOG_COMPONENT_MAIN,
        subsystem = LOG_SUBSYSTEM_STARTUP,
        state = ?state_manager.current_state(),
        result = "stopped",
        "RustFS server stopped"
    );
    let observability_error = startup_runtime_sources::shutdown_observability_guard()
        .err()
        .map(|error| Box::new(error) as Box<dyn std::error::Error + Send + Sync>);
    StartupShutdownFailure {
        capacity: capacity_error,
        maintenance: maintenance_result.err(),
        fronts: front_result.err(),
        observability: observability_error,
    }
    .into_result()
}

async fn wait_for_scanner_cleanup(cleanup: tokio::task::JoinHandle<()>) -> bool {
    // Dropping a JoinHandle detaches rather than aborts the task. Keep an
    // in-flight reset's guards owned while this runtime remains alive. This
    // bounded wait does not prove I/O drain at a later runtime/process exit.
    match tokio::time::timeout(SHUTDOWN_TIMEOUT, cleanup).await {
        Ok(Ok(())) => true,
        Ok(Err(error)) => {
            tracing::warn!(
                target: "rustfs::main::run",
                event = EVENT_SERVER_SHUTDOWN_STATE,
                component = LOG_COMPONENT_MAIN,
                subsystem = LOG_SUBSYSTEM_STARTUP,
                state = "scanner_cleanup_join_failed",
                reason = if error.is_cancelled() { "task_cancelled" } else { "task_panicked" },
                "Scanner cleanup task failed to join"
            );
            false
        }
        Err(_) => {
            tracing::warn!(
                target: "rustfs::main::run",
                event = EVENT_SERVER_SHUTDOWN_STATE,
                component = LOG_COMPONENT_MAIN,
                subsystem = LOG_SUBSYSTEM_STARTUP,
                state = "scanner_cleanup_not_joined",
                reason = "timeout",
                timeout_secs = SHUTDOWN_TIMEOUT.as_secs(),
                "Scanner cleanup completion is unconfirmed; task was not aborted"
            );
            false
        }
    }
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
    use super::{StartupShutdownFailure, embedded_endpoint_address, mark_embedded_global_init_started, wait_for_scanner_cleanup};
    use std::{
        net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
        sync::atomic::{AtomicBool, Ordering},
    };

    #[test]
    fn startup_shutdown_result_accepts_no_failures() {
        StartupShutdownFailure {
            capacity: None,
            maintenance: None,
            fronts: None,
            observability: None,
        }
        .into_result()
        .expect("all observed stages succeeded");
    }

    #[tokio::test]
    async fn startup_shutdown_result_preserves_maintenance_only_join_error() {
        let task = tokio::spawn(async { panic!("expected maintenance shutdown aggregation failure") });
        let task_id = task.id();
        let join_error = task.await.expect_err("actual maintenance task must fail");
        let error = StartupShutdownFailure {
            capacity: None,
            maintenance: Some(std::io::Error::other(join_error)),
            fronts: None,
            observability: None,
        }
        .into_result()
        .expect_err("maintenance failure must reach the lifecycle result");
        let failure = error
            .get_ref()
            .expect("typed startup failure")
            .downcast_ref::<StartupShutdownFailure>()
            .expect("startup failure type retained");
        let source = std::error::Error::source(failure).expect("maintenance source must survive without a capacity failure");
        let source = source.downcast_ref::<std::io::Error>().expect("original maintenance error");
        let join_error = source
            .get_ref()
            .expect("actual JoinError attached")
            .downcast_ref::<tokio::task::JoinError>()
            .expect("actual JoinError type");
        assert!(join_error.is_panic());
        assert_eq!(join_error.id(), task_id);
    }

    #[tokio::test]
    async fn startup_shutdown_result_retains_capacity_maintenance_and_observability_failures() {
        let capacity = tokio::spawn(async { panic!("expected capacity shutdown aggregation failure") });
        let capacity_id = capacity.id();
        let capacity_error = capacity.await.expect_err("actual capacity failure");
        let maintenance = tokio::spawn(async { panic!("expected maintenance shutdown aggregation failure") });
        let maintenance_id = maintenance.id();
        let maintenance_error = maintenance.await.expect_err("actual maintenance failure");
        let error = StartupShutdownFailure {
            capacity: Some(std::io::Error::other(capacity_error)),
            maintenance: Some(std::io::Error::new(std::io::ErrorKind::TimedOut, maintenance_error)),
            fronts: None,
            observability: Some(Box::new(std::io::Error::other("observability fixture failure"))),
        }
        .into_result()
        .expect_err("no observed failure may be overwritten by another stage");
        assert_eq!(error.kind(), std::io::ErrorKind::TimedOut, "maintenance's original deadline stays failed");
        let failure = error
            .get_ref()
            .expect("typed startup failure")
            .downcast_ref::<StartupShutdownFailure>()
            .expect("startup failure type retained");
        let capacity_error = failure.capacity.as_ref().expect("capacity failure retained");
        let capacity_join = capacity_error
            .get_ref()
            .expect("capacity JoinError attached")
            .downcast_ref::<tokio::task::JoinError>()
            .expect("capacity JoinError type retained");
        assert_eq!(capacity_join.id(), capacity_id);
        let maintenance_error = failure.maintenance.as_ref().expect("maintenance failure retained");
        let maintenance_join = maintenance_error
            .get_ref()
            .expect("maintenance JoinError attached")
            .downcast_ref::<tokio::task::JoinError>()
            .expect("maintenance JoinError type retained");
        assert_eq!(maintenance_join.id(), maintenance_id);
        assert!(failure.observability.is_some(), "observability cleanup failure must also survive");
        let display = error.to_string();
        assert!(display.contains("expected capacity shutdown aggregation failure"));
        assert!(display.contains("expected maintenance shutdown aggregation failure"));
        assert!(display.contains("observability fixture failure"));
    }

    #[tokio::test]
    async fn disabled_cleanup_shutdown_joins_a_finished_task() {
        assert!(wait_for_scanner_cleanup(tokio::spawn(async {})).await);
    }

    #[tokio::test]
    async fn disabled_cleanup_shutdown_reports_task_failure() {
        assert!(!wait_for_scanner_cleanup(tokio::spawn(async { panic!("fixture cleanup failure") })).await);
    }

    #[tokio::test(start_paused = true)]
    async fn disabled_cleanup_shutdown_timeout_does_not_abort_owned_work() {
        struct OwnedWork(std::sync::Arc<AtomicBool>);
        impl Drop for OwnedWork {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }
        let dropped = std::sync::Arc::new(AtomicBool::new(false));
        let owned = OwnedWork(dropped.clone());
        let (started, ready) = tokio::sync::oneshot::channel();
        let (release, resume) = tokio::sync::oneshot::channel();
        let (finished, done) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let owned = owned;
            started.send(()).expect("work started");
            resume.await.expect("fixture releases owned work");
            drop(owned);
            finished.send(()).expect("work completed");
        });
        ready.await.expect("task must actually be in flight");
        assert!(!wait_for_scanner_cleanup(task).await, "timeout is not a completed join");
        assert!(!dropped.load(Ordering::SeqCst), "timeout must not abort the reset's owned guards");
        release.send(()).expect("detached task remains alive");
        done.await.expect("owned work should finish after explicit release");
        assert!(dropped.load(Ordering::SeqCst));
    }

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
    #[tokio::test]
    async fn startup_shutdown_front_only_retains_actual_join_error_source() {
        let (tx, _rx) = tokio::sync::broadcast::channel(1);
        let task = tokio::spawn(async { panic!("expected lifecycle front failure") });
        let task_id = task.id();
        let mut s3 = Some(crate::server::ShutdownHandle::new(tx, task));
        let mut console = None;
        let mut optional = Vec::new();
        let mut fronts = crate::startup_shutdown::FrontShutdownFailure::default();
        crate::startup_shutdown::join_startup_fronts(&mut s3, &mut console, &mut optional, &mut fronts).await;
        let error = StartupShutdownFailure {
            capacity: None,
            maintenance: None,
            fronts: fronts.into_result().err(),
            observability: None,
        }
        .into_result()
        .expect_err("front failure must reach lifecycle result");
        assert!(error.to_string().contains("join_panicked"));
        assert!(
            !error.to_string().contains("expected lifecycle front failure"),
            "front Display keeps panic payload out"
        );
        let failure = error
            .get_ref()
            .expect("typed lifecycle failure")
            .downcast_ref::<StartupShutdownFailure>()
            .expect("lifecycle failure type");
        let source = std::error::Error::source(failure).expect("front failure source");
        let source = std::error::Error::source(source).expect("checked receipt source");
        let source = std::error::Error::source(source).expect("actual task source");
        let join = source
            .downcast_ref::<tokio::task::JoinError>()
            .expect("original JoinError type");
        assert!(join.is_panic());
        assert_eq!(join.id(), task_id);
        assert!(s3.is_none() && console.is_none() && optional.is_empty());
    }

    #[tokio::test]
    async fn startup_shutdown_result_retains_front_capacity_maintenance_and_observability() {
        let capacity = tokio::spawn(async { panic!("expected capacity combination failure") });
        let capacity_id = capacity.id();
        let capacity = capacity.await.expect_err("actual capacity task panic");
        let maintenance = tokio::spawn(async { panic!("expected maintenance combination failure") });
        let maintenance_id = maintenance.id();
        let maintenance = maintenance.await.expect_err("actual maintenance task panic");
        let (tx, _rx) = tokio::sync::broadcast::channel(1);
        let front = tokio::spawn(async { panic!("expected front combination failure") });
        let front_id = front.id();
        let mut s3 = Some(crate::server::ShutdownHandle::new(tx, front));
        let mut console = None;
        let mut optional = Vec::new();
        let mut fronts = crate::startup_shutdown::FrontShutdownFailure::default();
        crate::startup_shutdown::join_startup_fronts(&mut s3, &mut console, &mut optional, &mut fronts).await;
        let error = StartupShutdownFailure {
            capacity: Some(std::io::Error::new(std::io::ErrorKind::TimedOut, capacity)),
            maintenance: Some(std::io::Error::other(maintenance)),
            fronts: fronts.into_result().err(),
            observability: Some(Box::new(std::io::Error::other("observability combination failure"))),
        }
        .into_result()
        .expect_err("every failure remains represented");
        assert_eq!(error.kind(), std::io::ErrorKind::TimedOut);
        let failure = error
            .get_ref()
            .expect("typed lifecycle failure")
            .downcast_ref::<StartupShutdownFailure>()
            .expect("lifecycle failure type");
        let capacity = failure
            .capacity
            .as_ref()
            .expect("capacity retained")
            .get_ref()
            .expect("capacity source")
            .downcast_ref::<tokio::task::JoinError>()
            .expect("actual capacity JoinError");
        assert_eq!(capacity.id(), capacity_id);
        let maintenance = failure
            .maintenance
            .as_ref()
            .expect("maintenance retained")
            .get_ref()
            .expect("maintenance source")
            .downcast_ref::<tokio::task::JoinError>()
            .expect("actual maintenance JoinError");
        assert_eq!(maintenance.id(), maintenance_id);
        let source = std::error::Error::source(failure.fronts.as_ref().expect("front retained")).expect("checked receipt");
        let join = std::error::Error::source(source)
            .expect("actual front source")
            .downcast_ref::<tokio::task::JoinError>()
            .expect("actual front JoinError");
        assert_eq!(join.id(), front_id);
        assert!(failure.observability.is_some());
        assert!(s3.is_none() && console.is_none() && optional.is_empty());
    }
}
