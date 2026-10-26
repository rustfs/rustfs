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
mod health;
mod http;
mod hybrid;
mod layer;
mod module_switch;
mod prefix;
pub mod rate_limit;
mod readiness;
mod runtime;
pub(crate) mod runtime_sources;
mod service_state;
mod ssec_transport;
pub mod tls_material;
pub(crate) mod tls_publish;

#[cfg(feature = "http3")]
mod http3;

use tracing::warn;

const EVENT_SERVER_SHUTDOWN_STATE: &str = "server_shutdown_state";
const LOG_COMPONENT_SERVER: &str = "server";
const LOG_SUBSYSTEM_TRANSPORT: &str = "transport";

// Items used by main.rs (binary crate) and/or embedded.rs — must be fully pub.
pub(crate) use audit::apply_audit_module_switch_for_context;
pub use audit::{is_audit_module_enabled, refresh_audit_module_enabled, start_audit_system, stop_audit_system};
pub use event::{init_event_notifier, is_notify_module_enabled, refresh_notify_module_enabled, shutdown_event_notifier};
pub use http::start_http_server;
#[cfg(test)]
pub(crate) use http::test_retire_http12_scope;
pub use prefix::LOGO;
pub use runtime::build_tokio_runtime;
pub use service_state::SHUTDOWN_TIMEOUT;
pub use service_state::ServiceState;
pub use service_state::ServiceStateManager;
pub use service_state::ShutdownSignal;
pub use service_state::wait_for_shutdown;

// Items only used within the library crate (admin handlers, server/http.rs, etc.).
pub(crate) use event::{
    is_event_notifier_reconciled, mark_event_notifier_reconciled, mark_event_notifier_unreconciled,
    reconcile_event_notifier_from_store, start_persisted_event_notifier_reconciler, with_notify_runtime_reconcile_lock,
};
#[cfg(test)]
pub(crate) use health::{
    HealthPayloadContext, HealthReadinessSource, build_component_details, build_health_payload, health_check_state,
    readiness_source_for_probe,
};
pub(crate) use health::{
    HealthProbe, build_health_response_parts, collect_probe_readiness, kms_probe_staleness_limit, kms_ready_from_probe,
    probe_from_path,
};
pub(crate) use http::HeaderMapCarrier;
pub(crate) use http::active_http_requests;
pub use layer::s3_http_request_guard;
pub(crate) use layer::{RequestContextLayer, is_sts_query_request};
pub(crate) use module_switch::{
    MODULE_SWITCHES_SIGNAL_SUBSYSTEM, ModuleSwitchSnapshot, ModuleSwitchSource, PersistedModuleSwitches,
    current_module_switch_snapshot, refresh_persisted_module_switches_from, refresh_persisted_module_switches_from_store,
    save_persisted_module_switches_to, validate_module_switch_update,
};
pub(crate) use prefix::{
    ADMIN_PREFIX, APPLE_TOUCH_ICON_PATH, APPLE_TOUCH_ICON_PRECOMPOSED_PATH, CONSOLE_PREFIX, FAVICON_PATH,
    HEALTH_COMPAT_LIVE_PATH, HEALTH_PREFIX, HEALTH_READY_PATH, LICENSE, MINIO_ADMIN_PREFIX, MINIO_ADMIN_V3_PREFIX,
    MINIO_HEALTH_CLUSTER_PATH, MINIO_HEALTH_CLUSTER_READ_PATH, MINIO_HEALTH_LIVE_PATH, MINIO_HEALTH_READY_PATH, PROFILE_CPU_PATH,
    PROFILE_MEMORY_PATH, RPC_PREFIX, RUSTFS_ADMIN_PREFIX, TABLE_CATALOG_COMPAT_PREFIX, TABLE_CATALOG_PREFIX, TONIC_PREFIX,
    VERSION, console_prefix, has_path_prefix, init_console_prefix, is_admin_path, is_table_catalog_path,
};
pub(crate) use readiness::ReadinessDegradedReason;
pub(crate) use readiness::ReadinessGateLayer;
pub(crate) use readiness::collect_dependency_readiness_report;
pub(crate) use readiness::collect_node_readiness_report;
pub(crate) use readiness::observe_peer_drive_admission;
pub use readiness::publish_ready_when_runtime_ready;
pub(crate) use readiness::snapshot_dependency_readiness_report;
pub(crate) use readiness::{collect_cluster_read_health_report, collect_cluster_write_health_report};
pub(crate) use ssec_transport::NativeTlsTransport;

pub use crate::shared_types::RemoteAddr;

pub(crate) fn strip_valid_port_suffix(host: &str) -> &str {
    if host.ends_with(']') {
        return host;
    }

    match host.rsplit_once(':') {
        Some((host, port)) if !port.is_empty() && port.bytes().all(|b| b.is_ascii_digit()) && port.parse::<u16>().is_ok() => host,
        _ => host,
    }
}

/// Observations produced by the actual accepted HTTP/1-2 task owner.
#[derive(Debug, Clone)]
pub(crate) struct Http12ShutdownOutcome {
    deadline_missed: bool,
    first_child_error: Option<std::sync::Arc<tokio::task::JoinError>>,
    child_failures: u64,
    unverified_h3: bool,
    unverified_tls_reloader: bool,
}

impl Http12ShutdownOutcome {
    pub(crate) fn deadline_missed(&self) -> bool {
        self.deadline_missed
    }

    pub(crate) fn verified_scope_complete(&self) -> bool {
        !self.unverified_h3 && !self.unverified_tls_reloader
    }

    fn failed(&self) -> bool {
        self.deadline_missed || self.child_failures != 0
    }
}

#[derive(Debug, Clone)]
pub(crate) enum ShutdownReceipt {
    OuterTaskJoined,
    Http12ChildrenRetired(Http12ShutdownOutcome),
}

/// A failed or unknown observation retains the actual task error, when known.
#[derive(Debug, Clone)]
pub(crate) enum ShutdownWaitError {
    Join(std::sync::Arc<tokio::task::JoinError>),
    MissingTask,
    Http12(Http12ShutdownOutcome),
}

impl ShutdownWaitError {
    fn reason(&self) -> &'static str {
        match self {
            Self::Join(error) if error.is_cancelled() => "join_cancelled",
            Self::Join(_) => "join_panicked",
            Self::MissingTask => "join_unconfirmed",
            Self::Http12(outcome) if outcome.deadline_missed => "http12_deadline_missed",
            Self::Http12(_) => "http12_child_join_failed",
        }
    }

    pub(crate) fn http12_outcome(&self) -> Option<&Http12ShutdownOutcome> {
        match self {
            Self::Http12(outcome) => Some(outcome),
            _ => None,
        }
    }
}

impl std::fmt::Display for ShutdownWaitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "server task shutdown {}", self.reason())?;
        if let Self::Http12(outcome) = self {
            write!(f, " ({} child join failures)", outcome.child_failures)?;
        }
        Ok(())
    }
}

impl std::error::Error for ShutdownWaitError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Join(error) => Some(error.as_ref()),
            Self::Http12(outcome) => outcome.first_child_error.as_ref().map(|error| error.as_ref() as _),
            Self::MissingTask => None,
        }
    }
}

enum ShutdownTask {
    Outer(tokio::task::JoinHandle<()>),
    Http12(tokio::task::JoinHandle<Http12ShutdownOutcome>),
}

impl ShutdownTask {
    #[cfg(test)]
    fn is_finished(&self) -> bool {
        match self {
            Self::Outer(task) => task.is_finished(),
            Self::Http12(task) => task.is_finished(),
        }
    }

    #[cfg(test)]
    fn id(&self) -> tokio::task::Id {
        match self {
            Self::Outer(task) => task.id(),
            Self::Http12(task) => task.id(),
        }
    }
}

pub struct ShutdownHandle {
    shutdown_tx: Option<tokio::sync::broadcast::Sender<()>>,
    task_handle: Option<ShutdownTask>,
    failure: Option<ShutdownWaitError>,
}

impl ShutdownHandle {
    pub fn new(shutdown_tx: tokio::sync::broadcast::Sender<()>, task_handle: tokio::task::JoinHandle<()>) -> Self {
        Self {
            shutdown_tx: Some(shutdown_tx),
            task_handle: Some(ShutdownTask::Outer(task_handle)),
            failure: None,
        }
    }

    pub(crate) fn new_http12(
        shutdown_tx: tokio::sync::broadcast::Sender<()>,
        task_handle: tokio::task::JoinHandle<Http12ShutdownOutcome>,
    ) -> Self {
        Self {
            shutdown_tx: Some(shutdown_tx),
            task_handle: Some(ShutdownTask::Http12(task_handle)),
            failure: None,
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

    /// A cancelled borrowed observer leaves the same original task in this handle.
    /// A legacy outer join never certifies HTTP children. Failed observations are sticky.
    pub(crate) async fn wait_checked(&mut self) -> Result<ShutdownReceipt, ShutdownWaitError> {
        if let Some(error) = &self.failure {
            return Err(error.clone());
        }
        let Some(task) = self.task_handle.as_mut() else {
            return Err(ShutdownWaitError::MissingTask);
        };
        let result = match task {
            ShutdownTask::Outer(task) => task.await.map(|()| ShutdownReceipt::OuterTaskJoined),
            ShutdownTask::Http12(task) => task.await.map(ShutdownReceipt::Http12ChildrenRetired),
        }
        .map_err(|error| ShutdownWaitError::Join(std::sync::Arc::new(error)))
        .and_then(|receipt| match receipt {
            ShutdownReceipt::Http12ChildrenRetired(outcome) if outcome.failed() => Err(ShutdownWaitError::Http12(outcome)),
            receipt => Ok(receipt),
        });
        let _ = self.task_handle.take();
        if let Err(error) = &result {
            self.failure = Some(error.clone());
        }
        result
    }

    pub async fn wait(mut self) {
        if let Err(err) = self.wait_checked().await {
            warn!(
                event = EVENT_SERVER_SHUTDOWN_STATE,
                component = LOG_COMPONENT_SERVER,
                subsystem = LOG_SUBSYSTEM_TRANSPORT,
                state = "join_failed",
                reason = err.reason(),
                "Server task join failed during shutdown"
            );
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

#[cfg(test)]
mod shutdown_handle_tests {
    use super::{ShutdownHandle, ShutdownWaitError};
    use std::{future::Future, task::Poll, time::Duration};
    use tokio::sync::{broadcast, oneshot};

    #[tokio::test]
    async fn checked_wait_cancelled_observer_keeps_original_held_task() {
        let (tx, mut rx) = broadcast::channel(1);
        let (entered_tx, entered_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let (done_tx, done_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            let _ = rx.recv().await;
            let _ = entered_tx.send(());
            let _ = release_rx.await;
            let _ = done_tx.send(());
        });
        let id = task.id();
        let mut handle = ShutdownHandle::new(tx, task);
        handle.signal();
        let entered = tokio::time::timeout(Duration::from_secs(5), entered_rx)
            .await
            .is_ok_and(|r| r.is_ok());
        let mut observer = Box::pin(handle.wait_checked());
        let pending = std::future::poll_fn(|cx| Poll::Ready(observer.as_mut().poll(cx).is_pending())).await;
        drop(observer);
        let retained = handle.task_handle.as_ref().is_some_and(|task| task.id() == id);
        let _ = release_tx.send(());
        let completed = tokio::time::timeout(Duration::from_secs(5), done_rx)
            .await
            .is_ok_and(|r| r.is_ok());
        let joined = handle.wait_checked().await;
        assert!(entered && pending && retained && completed);
        joined.expect("same original task joined after observer cancellation");
        assert!(handle.task_handle.is_none());
    }

    #[tokio::test]
    async fn checked_wait_preserves_actual_panic_and_cancel_sources() {
        let (tx, _rx) = broadcast::channel(1);
        let task = tokio::spawn(async { panic!("expected checked server task panic") });
        let panic_id = task.id();
        let mut handle = ShutdownHandle::new(tx, task);
        let panic = handle.wait_checked().await.expect_err("actual panic must survive");
        assert!(panic.to_string().contains("join_panicked"));
        assert!(
            !panic.to_string().contains("expected checked server task panic"),
            "Display must not emit the panic payload"
        );
        let source = std::error::Error::source(&panic).expect("actual JoinError source");
        let source = source.downcast_ref::<tokio::task::JoinError>().expect("typed JoinError");
        assert!(source.is_panic());
        assert_eq!(source.id(), panic_id);

        let (tx, _rx) = broadcast::channel(1);
        let (entered_tx, entered_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            let _ = entered_tx.send(());
            std::future::pending::<()>().await;
        });
        let cancel_id = task.id();
        let abort = task.abort_handle();
        let mut handle = ShutdownHandle::new(tx, task);
        let entered = tokio::time::timeout(Duration::from_secs(5), entered_rx)
            .await
            .is_ok_and(|r| r.is_ok());
        abort.abort();
        let cancelled = handle.wait_checked().await.expect_err("actual cancellation must survive");
        let source = std::error::Error::source(&cancelled).expect("actual JoinError source");
        let source = source.downcast_ref::<tokio::task::JoinError>().expect("typed JoinError");
        assert!(entered && source.is_cancelled());
        assert_eq!(source.id(), cancel_id);
    }

    #[tokio::test]
    async fn checked_wait_missing_or_consumed_receipt_stays_unknown() {
        let mut missing = ShutdownHandle {
            shutdown_tx: None,
            task_handle: None,
            failure: None,
        };
        assert!(matches!(missing.wait_checked().await, Err(ShutdownWaitError::MissingTask)));
        let (tx, _rx) = broadcast::channel(1);
        let mut completed = ShutdownHandle::new(tx, tokio::spawn(async {}));
        completed.wait_checked().await.expect("actual task joined once");
        assert!(matches!(completed.wait_checked().await, Err(ShutdownWaitError::MissingTask)));
    }

    #[tokio::test]
    async fn checked_outer_join_does_not_certify_accepted_child_retirement() {
        let (tx, _rx) = broadcast::channel(1);
        let (child_tx, child_rx) = oneshot::channel();
        let (entered_tx, entered_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let outer = tokio::spawn(async move {
            let child = tokio::spawn(async move {
                let _ = entered_tx.send(());
                let _ = release_rx.await;
            });
            let _ = child_tx.send(child);
        });
        let mut handle = ShutdownHandle::new(tx, outer);
        let child = child_rx.await.expect("actual accepted child owner");
        let entered = tokio::time::timeout(Duration::from_secs(5), entered_rx)
            .await
            .is_ok_and(|r| r.is_ok());
        let outer_joined = handle.wait_checked().await.is_ok();
        let child_still_held = !child.is_finished();
        let _ = release_tx.send(());
        child.await.expect("accepted child joined only after its own release");
        assert!(entered && outer_joined && child_still_held);
    }
}
