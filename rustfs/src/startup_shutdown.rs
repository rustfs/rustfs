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

use crate::storage_api::startup::shutdown::shutdown_background_services;
use crate::{
    server::{ServiceState, ServiceStateManager, ShutdownHandle, ShutdownSignal, shutdown_event_notifier, stop_audit_system},
    startup_optional_runtime_sidecars::{
        OptionalRuntimeServices, prepare_optional_runtime_shutdowns, shutdown_optional_runtime_services,
    },
    startup_runtime_sources,
};
use rustfs_heal::shutdown_ahm_services;
use rustfs_notify::NotificationLifecycleTransition;
use rustfs_utils::get_env_bool_with_aliases;
use std::future::Future;
use std::path::PathBuf;
use std::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

const ENV_SCANNER_ENABLED: &str = "RUSTFS_SCANNER_ENABLED";
const ENV_SCANNER_ENABLED_DEPRECATED: &str = "RUSTFS_ENABLE_SCANNER";
const ENV_HEAL_ENABLED: &str = "RUSTFS_HEAL_ENABLED";
const ENV_HEAL_ENABLED_DEPRECATED: &str = "RUSTFS_ENABLE_HEAL";
const LOG_COMPONENT_MAIN: &str = "main";
const LOG_COMPONENT_EMBEDDED: &str = "embedded";
const LOG_SUBSYSTEM_STARTUP: &str = "startup";
const LOG_SUBSYSTEM_EMBEDDED: &str = "embedded";
const EVENT_AUDIT_SYSTEM_STATE: &str = "audit_system_state";
const EVENT_EMBEDDED_SERVER_STATE: &str = "embedded_server_state";
const EVENT_EMBEDDED_SHUTDOWN_CLEANUP_FAILED: &str = "embedded_shutdown_cleanup_failed";
const EVENT_SHUTDOWN_SIGNAL_RECEIVED: &str = "shutdown_signal_received";
const EVENT_BACKGROUND_SERVICE_SHUTDOWN: &str = "background_service_shutdown";
const EVENT_EVENT_NOTIFIER_SHUTDOWN: &str = "event_notifier_shutdown";
const EVENT_PROFILING_SHUTDOWN: &str = "profiling_shutdown";
const EVENT_SERVER_SHUTDOWN_STATE: &str = "server_shutdown_state";

fn join_failure_reason(error: &tokio::task::JoinError) -> &'static str {
    if error.is_cancelled() {
        "join_cancelled"
    } else {
        "join_panicked"
    }
}

struct EmbeddedRuntimeOwnerState {
    owners: usize,
    pending_cleanup: Option<CancellationToken>,
}

struct EmbeddedRuntimeOwners {
    state: Mutex<EmbeddedRuntimeOwnerState>,
}

impl EmbeddedRuntimeOwners {
    const fn new() -> Self {
        Self {
            state: Mutex::new(EmbeddedRuntimeOwnerState {
                owners: 0,
                pending_cleanup: None,
            }),
        }
    }

    fn register(&self) -> Option<CancellationToken> {
        let mut state = self.state.lock().unwrap_or_else(|err| err.into_inner());
        state.owners += 1;
        state
            .pending_cleanup
            .as_ref()
            .filter(|cleanup| !cleanup.is_cancelled())
            .cloned()
    }

    fn release_with<T>(&self, prepare_cleanup: impl FnOnce(CancellationToken) -> T) -> Option<T> {
        let mut state = self.state.lock().unwrap_or_else(|err| err.into_inner());
        if state.owners == 0 {
            return None;
        }
        state.owners -= 1;
        if state.owners != 0 {
            return None;
        }
        if state.pending_cleanup.as_ref().is_some_and(|cleanup| !cleanup.is_cancelled()) {
            return None;
        }

        let completion = CancellationToken::new();
        // The disable generation must be accepted while registration is excluded;
        // otherwise a concurrently starting owner can enable targets first and be
        // overwritten by this last-owner release.
        let cleanup = prepare_cleanup(completion.clone());
        state.pending_cleanup = Some(completion);
        Some(cleanup)
    }

    #[cfg(test)]
    fn owner_count(&self) -> usize {
        self.state.lock().unwrap_or_else(|err| err.into_inner()).owners
    }
}

static EMBEDDED_RUNTIME_OWNERS: EmbeddedRuntimeOwners = EmbeddedRuntimeOwners::new();

pub(crate) struct EmbeddedRuntimeOwner {
    active: bool,
    runtime: tokio::runtime::Handle,
}

pub(crate) struct EmbeddedRuntimeCleanup {
    completion: CancellationToken,
    notification: Option<NotificationLifecycleTransition>,
    runtime: tokio::runtime::Handle,
}

impl EmbeddedRuntimeCleanup {
    fn prepare(completion: CancellationToken, runtime: tokio::runtime::Handle) -> Self {
        let _runtime_guard = runtime.enter();
        let system = rustfs_notify::ensure_live_events();
        Self {
            completion,
            notification: Some(system.publish_targets_enabled(false, None)),
            runtime,
        }
    }
}

impl Drop for EmbeddedRuntimeCleanup {
    fn drop(&mut self) {
        self.completion.cancel();
    }
}

impl EmbeddedRuntimeOwner {
    pub(crate) fn cleanup_runtime_handle(&self) -> tokio::runtime::Handle {
        tokio::runtime::Handle::try_current().unwrap_or_else(|_| self.runtime.clone())
    }

    pub(crate) fn release(&mut self) -> Option<EmbeddedRuntimeCleanup> {
        if !self.active {
            return None;
        }
        self.active = false;
        let runtime = self.cleanup_runtime_handle();
        EMBEDDED_RUNTIME_OWNERS.release_with(move |completion| EmbeddedRuntimeCleanup::prepare(completion, runtime))
    }
}

impl Drop for EmbeddedRuntimeOwner {
    fn drop(&mut self) {
        if let Some(cleanup) = self.release() {
            schedule_embedded_runtime_cleanup(cleanup);
        }
    }
}

pub(crate) async fn register_embedded_runtime_owner() -> EmbeddedRuntimeOwner {
    let runtime = tokio::runtime::Handle::current();
    let pending_cleanup = EMBEDDED_RUNTIME_OWNERS.register();
    let owner = EmbeddedRuntimeOwner { active: true, runtime };
    // Reserve ownership before waiting so another release cannot start a second
    // process-runtime cleanup while this startup is queued behind the first.
    if let Some(cleanup) = pending_cleanup {
        cleanup.cancelled().await;
    }
    owner
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BackgroundShutdownStep {
    DataScanner,
    Ahm,
}

fn background_shutdown_steps(enable_scanner: bool, enable_heal: bool) -> Vec<BackgroundShutdownStep> {
    let mut steps = Vec::with_capacity(2);
    if enable_scanner {
        steps.push(BackgroundShutdownStep::DataScanner);
    }
    if enable_heal || enable_scanner {
        steps.push(BackgroundShutdownStep::Ahm);
    }
    steps
}

pub(crate) async fn run_startup_shutdown_sequence(
    state_manager: &ServiceStateManager,
    shutdown_signal: ShutdownSignal,
    s3_shutdown_handle: Option<ShutdownHandle>,
    console_shutdown_handle: Option<ShutdownHandle>,
    optional_runtimes: OptionalRuntimeServices,
    ctx: CancellationToken,
) {
    ctx.cancel();

    // Stop long-lived peer/disk background monitors while the runtime and tracing
    // subscriber are still alive, so the `tracing::Span` each monitor holds is
    // dropped here instead of during worker-thread TLS destruction at runtime
    // teardown (which can panic in the fmt layer's `on_close`; see issue #4264).
    crate::storage_api::startup::shutdown::shutdown_background_monitors();

    info!(
        target: "rustfs::main::handle_shutdown",
        event = EVENT_SHUTDOWN_SIGNAL_RECEIVED,
        component = LOG_COMPONENT_MAIN,
        subsystem = LOG_SUBSYSTEM_STARTUP,
        signal = shutdown_signal.log_label(),
        "Shutdown signal received"
    );
    state_manager.update(ServiceState::Stopping);

    let enable_scanner = get_env_bool_with_aliases(ENV_SCANNER_ENABLED, &[ENV_SCANNER_ENABLED_DEPRECATED], true);
    let enable_heal = get_env_bool_with_aliases(ENV_HEAL_ENABLED, &[ENV_HEAL_ENABLED_DEPRECATED], true);

    let background_steps = background_shutdown_steps(enable_scanner, enable_heal);
    for step in &background_steps {
        match step {
            BackgroundShutdownStep::DataScanner => {
                info!(
                    target: "rustfs::main::handle_shutdown",
                    event = EVENT_BACKGROUND_SERVICE_SHUTDOWN,
                    component = LOG_COMPONENT_MAIN,
                    subsystem = LOG_SUBSYSTEM_STARTUP,
                    service = "data_scanner",
                    state = "stopping",
                    "Background service shutdown started"
                );
                shutdown_background_services();
            }
            BackgroundShutdownStep::Ahm => {
                info!(
                    target: "rustfs::main::handle_shutdown",
                    event = EVENT_BACKGROUND_SERVICE_SHUTDOWN,
                    component = LOG_COMPONENT_MAIN,
                    subsystem = LOG_SUBSYSTEM_STARTUP,
                    service = "ahm",
                    state = "stopping",
                    "Background service shutdown started"
                );
                shutdown_ahm_services();
            }
        }
    }

    if background_steps.is_empty() {
        info!(
            target: "rustfs::main::handle_shutdown",
            event = EVENT_BACKGROUND_SERVICE_SHUTDOWN,
            component = LOG_COMPONENT_MAIN,
            subsystem = LOG_SUBSYSTEM_STARTUP,
            service = "ahm",
            state = "skipped",
            reason = "disabled",
            "Background service shutdown skipped"
        );
    }

    // Persist in-memory compression totals to backend before subsystems shut down.
    info!(
        target: "rustfs::main::handle_shutdown",
        event = "compression_total_persist",
        component = LOG_COMPONENT_MAIN,
        subsystem = LOG_SUBSYSTEM_STARTUP,
        state = "persisting",
        "Compression total persist started"
    );
    crate::storage_api::startup::shutdown::store_compression_total_in_backend().await;

    let optional_runtime_shutdowns = prepare_optional_runtime_shutdowns(optional_runtimes);

    info!(
        target: "rustfs::main::handle_shutdown",
        event = EVENT_EVENT_NOTIFIER_SHUTDOWN,
        component = LOG_COMPONENT_MAIN,
        subsystem = LOG_SUBSYSTEM_STARTUP,
        state = "stopping",
        "Event notifier shutdown started"
    );
    if let Err(err) = shutdown_event_notifier().await {
        error!(
            target: "rustfs::main::handle_shutdown",
            event = EVENT_EVENT_NOTIFIER_SHUTDOWN,
            component = LOG_COMPONENT_MAIN,
            subsystem = LOG_SUBSYSTEM_STARTUP,
            state = "stop_failed",
            error = %err,
            "Event notifier shutdown failed"
        );
    }

    info!(
        target: "rustfs::main::handle_shutdown",
        event = EVENT_AUDIT_SYSTEM_STATE,
        component = LOG_COMPONENT_MAIN,
        subsystem = LOG_SUBSYSTEM_STARTUP,
        state = "stopping",
        "Audit runtime stopping"
    );
    match stop_audit_system().await {
        Ok(_) => info!(
            target: "rustfs::main::handle_shutdown",
            event = EVENT_AUDIT_SYSTEM_STATE,
            component = LOG_COMPONENT_MAIN,
            subsystem = LOG_SUBSYSTEM_STARTUP,
            state = "stopped",
            "Audit runtime stopped"
        ),
        Err(e) => error!(
            target: "rustfs::main::handle_shutdown",
            event = EVENT_AUDIT_SYSTEM_STATE,
            component = LOG_COMPONENT_MAIN,
            subsystem = LOG_SUBSYSTEM_STARTUP,
            state = "stop_failed",
            error = %e,
            "Audit runtime failed to stop"
        ),
    }

    info!(
        target: "rustfs::main::handle_shutdown",
        event = EVENT_PROFILING_SHUTDOWN,
        component = LOG_COMPONENT_MAIN,
        subsystem = LOG_SUBSYSTEM_STARTUP,
        state = "stopping",
        "Profiling shutdown started"
    );
    crate::startup_runtime_hooks::shutdown_profiling_runtime();

    info!(
        target: "rustfs::main::handle_shutdown",
        event = EVENT_SERVER_SHUTDOWN_STATE,
        component = LOG_COMPONENT_MAIN,
        subsystem = LOG_SUBSYSTEM_STARTUP,
        state = "stopping",
        "RustFS server stopping"
    );
    if let Some(s3_shutdown_handle) = s3_shutdown_handle {
        s3_shutdown_handle.shutdown().await;
    }
    if let Some(console_shutdown_handle) = console_shutdown_handle {
        console_shutdown_handle.shutdown().await;
    }
    shutdown_optional_runtime_services(optional_runtime_shutdowns).await;
    // The data plane is drained: record this shutdown as clean so the next
    // startup skips the unclean-restart erasure-set heal.
    rustfs_heal::heal::clear_unclean_shutdown_markers().await;
    state_manager.update(ServiceState::Stopped);
    info!(
        target: "rustfs::main::handle_shutdown",
        event = EVENT_SERVER_SHUTDOWN_STATE,
        component = LOG_COMPONENT_MAIN,
        subsystem = LOG_SUBSYSTEM_STARTUP,
        state = "stopped",
        "RustFS server stopped"
    );
}

async fn run_embedded_runtime_cleanup(mut cleanup: EmbeddedRuntimeCleanup) {
    if let Some(notification) = cleanup.notification.take()
        && let Err(err) = notification.wait().await
    {
        warn!(
            component = LOG_COMPONENT_EMBEDDED,
            subsystem = LOG_SUBSYSTEM_EMBEDDED,
            event = EVENT_EMBEDDED_SHUTDOWN_CLEANUP_FAILED,
            service = "notification",
            error = %err,
            "Embedded shutdown cleanup failed"
        );
    }
    if let Err(err) = stop_audit_system().await {
        warn!(
            component = LOG_COMPONENT_EMBEDDED,
            subsystem = LOG_SUBSYSTEM_EMBEDDED,
            event = EVENT_EMBEDDED_SHUTDOWN_CLEANUP_FAILED,
            service = "audit",
            error = %err,
            "Embedded shutdown cleanup failed"
        );
    }
    if let Err(err) = startup_runtime_sources::shutdown_observability_guard() {
        warn!(
            component = LOG_COMPONENT_EMBEDDED,
            subsystem = LOG_SUBSYSTEM_EMBEDDED,
            event = EVENT_EMBEDDED_SHUTDOWN_CLEANUP_FAILED,
            service = "observability",
            error = %err,
            "Embedded shutdown cleanup failed"
        );
    }
    cleanup.completion.cancel();
}

pub(crate) async fn run_embedded_shutdown_cleanup(cleanup: EmbeddedRuntimeCleanup) {
    let completion = cleanup.completion.clone();
    let runtime = cleanup.runtime.clone();
    if let Err(err) = runtime.spawn(run_embedded_runtime_cleanup(cleanup)).await {
        completion.cancel();
        warn!(
            component = LOG_COMPONENT_EMBEDDED,
            subsystem = LOG_SUBSYSTEM_EMBEDDED,
            event = EVENT_EMBEDDED_SHUTDOWN_CLEANUP_FAILED,
            service = "process_runtime",
            reason = join_failure_reason(&err),
            "Embedded shutdown cleanup failed"
        );
    }
}

fn schedule_embedded_runtime_cleanup(cleanup: EmbeddedRuntimeCleanup) {
    let runtime = cleanup.runtime.clone();
    runtime.spawn(run_embedded_shutdown_cleanup(cleanup));
}

pub(crate) fn signal_embedded_startup_shutdown(shutdown_handle: &ShutdownHandle, ctx: &CancellationToken) {
    shutdown_handle.signal();
    ctx.cancel();
}

async fn release_embedded_runtime_after_drain(mut runtime_owner: Option<EmbeddedRuntimeOwner>) {
    if let Some(runtime_owner) = runtime_owner.as_mut()
        && let Some(cleanup) = runtime_owner.release()
    {
        run_embedded_shutdown_cleanup(cleanup).await;
    }
}

pub(crate) fn run_embedded_server_drop_cleanup(
    runtime: &tokio::runtime::Handle,
    ctx: &CancellationToken,
    shutdown_handle: &mut Option<ShutdownHandle>,
    temp_dir: &mut Option<PathBuf>,
    runtime_owner: Option<EmbeddedRuntimeOwner>,
) {
    ctx.cancel();
    if let Some(shutdown_handle) = shutdown_handle.as_ref() {
        shutdown_handle.signal();
    }

    let shutdown_handle = shutdown_handle.take();
    let temp_dir = temp_dir.take();
    runtime.spawn(finish_embedded_server_cleanup(
        shutdown_handle,
        temp_dir,
        release_embedded_runtime_after_drain(runtime_owner),
    ));
}

async fn finish_embedded_server_cleanup<F>(shutdown_handle: Option<ShutdownHandle>, temp_dir: Option<PathBuf>, process_cleanup: F)
where
    F: Future<Output = ()>,
{
    if let Some(shutdown_handle) = shutdown_handle {
        shutdown_handle.shutdown().await;
    }

    process_cleanup.await;

    if let Some(dir) = temp_dir
        && let Err(err) = tokio::fs::remove_dir_all(&dir).await
    {
        warn!(
            component = LOG_COMPONENT_EMBEDDED,
            subsystem = LOG_SUBSYSTEM_EMBEDDED,
            event = EVENT_EMBEDDED_SHUTDOWN_CLEANUP_FAILED,
            service = "temp_dir",
            path = %dir.display(),
            error = %err,
            "Embedded shutdown cleanup failed"
        );
    }
}

pub(crate) async fn run_embedded_server_shutdown(
    runtime: &tokio::runtime::Handle,
    ctx: &CancellationToken,
    shutdown_handle: &mut Option<ShutdownHandle>,
    temp_dir: &mut Option<PathBuf>,
    runtime_owner: Option<EmbeddedRuntimeOwner>,
) {
    info!(
        target: "rustfs::embedded",
        component = LOG_COMPONENT_EMBEDDED,
        subsystem = LOG_SUBSYSTEM_EMBEDDED,
        event = EVENT_EMBEDDED_SERVER_STATE,
        state = "stopping",
        "Embedded server state changed"
    );

    ctx.cancel();

    if let Some(shutdown_handle) = shutdown_handle.as_ref() {
        shutdown_handle.signal();
    }

    let cleanup_task = runtime.spawn(finish_embedded_server_cleanup(
        shutdown_handle.take(),
        temp_dir.take(),
        release_embedded_runtime_after_drain(runtime_owner),
    ));
    if let Err(err) = cleanup_task.await {
        warn!(
            component = LOG_COMPONENT_EMBEDDED,
            subsystem = LOG_SUBSYSTEM_EMBEDDED,
            event = EVENT_EMBEDDED_SHUTDOWN_CLEANUP_FAILED,
            service = "server_cleanup",
            reason = join_failure_reason(&err),
            "Embedded shutdown cleanup failed"
        );
    }

    info!(
        target: "rustfs::embedded",
        component = LOG_COMPONENT_EMBEDDED,
        subsystem = LOG_SUBSYSTEM_EMBEDDED,
        event = EVENT_EMBEDDED_SERVER_STATE,
        state = "stopped",
        "Embedded server state changed"
    );
}

#[cfg(test)]
mod tests {
    use super::{
        BackgroundShutdownStep, EmbeddedRuntimeOwners, background_shutdown_steps, finish_embedded_server_cleanup,
        run_embedded_server_drop_cleanup, signal_embedded_startup_shutdown,
    };
    use crate::server::ShutdownHandle;
    use std::sync::{Arc, mpsc};
    use std::time::Duration;
    use tokio::sync::broadcast;
    use tokio_util::sync::CancellationToken;

    #[test]
    fn background_shutdown_plan_keeps_scanner_before_ahm() {
        assert_eq!(
            background_shutdown_steps(true, true),
            vec![BackgroundShutdownStep::DataScanner, BackgroundShutdownStep::Ahm]
        );
        assert_eq!(
            background_shutdown_steps(true, false),
            vec![BackgroundShutdownStep::DataScanner, BackgroundShutdownStep::Ahm]
        );
        assert_eq!(background_shutdown_steps(false, true), vec![BackgroundShutdownStep::Ahm]);
        assert!(background_shutdown_steps(false, false).is_empty());
    }

    #[tokio::test]
    async fn signal_embedded_startup_shutdown_signals_handle_and_cancels_token() {
        let (shutdown_tx, mut shutdown_rx) = broadcast::channel(1);
        let shutdown_task = tokio::spawn(async move {
            let _ = shutdown_rx.recv().await;
        });
        let shutdown_handle = ShutdownHandle::new(shutdown_tx, shutdown_task);
        let cancel_token = CancellationToken::new();

        signal_embedded_startup_shutdown(&shutdown_handle, &cancel_token);

        tokio::time::timeout(Duration::from_secs(1), shutdown_handle.wait())
            .await
            .expect("shutdown task should observe startup signal");
        assert!(cancel_token.is_cancelled());
    }

    #[tokio::test]
    async fn embedded_drop_cleanup_signals_handle_cancels_token_and_removes_temp_dir() {
        let (shutdown_tx, mut shutdown_rx) = broadcast::channel(1);
        let (observed_tx, observed_rx) = tokio::sync::oneshot::channel();
        let shutdown_task = tokio::spawn(async move {
            let _ = shutdown_rx.recv().await;
            let _ = observed_tx.send(());
        });
        let shutdown_handle = ShutdownHandle::new(shutdown_tx, shutdown_task);
        let mut shutdown_handle = Some(shutdown_handle);
        let cancel_token = CancellationToken::new();
        let temp_dir = tempfile::tempdir().expect("temp dir should create");
        let temp_path = temp_dir.path().to_path_buf();
        let mut owned_temp_path = Some(temp_path.clone());

        run_embedded_server_drop_cleanup(
            &tokio::runtime::Handle::current(),
            &cancel_token,
            &mut shutdown_handle,
            &mut owned_temp_path,
            None,
        );

        tokio::time::timeout(Duration::from_secs(1), observed_rx)
            .await
            .expect("drop cleanup should signal shutdown")
            .expect("shutdown signal should be delivered");
        tokio::time::timeout(Duration::from_secs(1), async {
            while temp_path.exists() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("drop cleanup should remove the temporary directory");
        assert!(cancel_token.is_cancelled());
        assert!(shutdown_handle.is_none());
        assert!(owned_temp_path.is_none());
        assert!(!temp_path.exists());
    }

    #[test]
    fn only_the_last_embedded_owner_cleans_process_runtime() {
        let owners = EmbeddedRuntimeOwners::new();
        assert!(owners.register().is_none());
        assert!(owners.register().is_none());
        assert!(owners.release_with(|_| ()).is_none());
        assert!(owners.release_with(|_| ()).is_some());
        assert!(owners.release_with(|_| ()).is_none());
    }

    #[test]
    fn last_owner_cleanup_intent_is_serialized_before_new_registration() {
        let owners = Arc::new(EmbeddedRuntimeOwners::new());
        assert!(owners.register().is_none());

        let (cleanup_entered_tx, cleanup_entered_rx) = mpsc::channel();
        let (allow_cleanup_tx, allow_cleanup_rx) = mpsc::channel();
        let release_owners = owners.clone();
        let release_task = std::thread::spawn(move || {
            release_owners.release_with(|completion| {
                cleanup_entered_tx.send(()).expect("cleanup preparation should be observed");
                allow_cleanup_rx.recv().expect("cleanup preparation should be released");
                completion.cancel();
            })
        });
        cleanup_entered_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("last-owner cleanup should enter preparation");

        let (register_started_tx, register_started_rx) = mpsc::channel();
        let (register_done_tx, register_done_rx) = mpsc::channel();
        let register_owners = owners.clone();
        let register_task = std::thread::spawn(move || {
            register_started_tx.send(()).expect("registration should start");
            let pending_cleanup = register_owners.register();
            register_done_tx
                .send(pending_cleanup)
                .expect("registration result should be observed");
        });
        register_started_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("registration should start");
        let early_registration = match register_done_rx.try_recv() {
            Ok(result) => Some(result),
            Err(mpsc::TryRecvError::Empty) => None,
            Err(mpsc::TryRecvError::Disconnected) => panic!("registration thread disconnected"),
        };
        let registered_before_intent_published = early_registration.is_some();

        allow_cleanup_tx.send(()).expect("cleanup preparation should finish");
        assert!(release_task.join().expect("release thread should not panic").is_some());
        let registration = match early_registration {
            Some(result) => result,
            None => register_done_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("registration should complete"),
        };
        register_task.join().expect("register thread should not panic");
        assert!(!registered_before_intent_published);
        assert!(registration.is_none());
        assert_eq!(owners.owner_count(), 1);
    }

    #[tokio::test]
    async fn new_owner_waits_for_prior_last_owner_cleanup() {
        let owners = EmbeddedRuntimeOwners::new();
        assert!(owners.register().is_none());
        let cleanup_finished = owners
            .release_with(|completion| completion)
            .expect("last owner should publish a cleanup barrier");
        let pending_cleanup = owners.register().expect("new owner should observe unfinished cleanup");

        let wait_task = tokio::spawn(async move {
            pending_cleanup.cancelled().await;
        });
        tokio::task::yield_now().await;
        assert!(!wait_task.is_finished());

        cleanup_finished.cancel();
        wait_task.await.expect("cleanup waiter should not panic");
    }

    #[tokio::test]
    async fn server_drain_and_process_runtime_cleanup_finish_before_temp_dir_removal() {
        let temp_dir = tempfile::tempdir().expect("temp dir should create");
        let temp_path = temp_dir.path().to_path_buf();
        let (shutdown_tx, mut shutdown_rx) = broadcast::channel(1);
        let (shutdown_entered_tx, shutdown_entered_rx) = tokio::sync::oneshot::channel();
        let (allow_shutdown_tx, allow_shutdown_rx) = tokio::sync::oneshot::channel();
        let shutdown_task = tokio::spawn(async move {
            let _ = shutdown_rx.recv().await;
            shutdown_entered_tx.send(()).expect("shutdown should be observed");
            allow_shutdown_rx.await.expect("shutdown should be released");
        });
        let shutdown_handle = ShutdownHandle::new(shutdown_tx, shutdown_task);
        let (cleanup_entered_tx, mut cleanup_entered_rx) = tokio::sync::oneshot::channel();
        let (allow_cleanup_tx, allow_cleanup_rx) = tokio::sync::oneshot::channel();

        let cleanup_task = tokio::spawn(finish_embedded_server_cleanup(
            Some(shutdown_handle),
            Some(temp_path.clone()),
            async move {
                cleanup_entered_tx.send(()).expect("cleanup should be observed");
                allow_cleanup_rx.await.expect("cleanup should be released");
            },
        ));
        shutdown_entered_rx.await.expect("shutdown should start");
        assert!(
            matches!(cleanup_entered_rx.try_recv(), Err(tokio::sync::oneshot::error::TryRecvError::Empty)),
            "process runtime cleanup must wait for the server to drain"
        );
        assert!(temp_path.exists(), "temporary data must remain available while the server drains");

        allow_shutdown_tx.send(()).expect("shutdown should finish");
        cleanup_entered_rx.await.expect("cleanup should start");
        assert!(temp_path.exists(), "temporary data must remain available during runtime cleanup");

        allow_cleanup_tx.send(()).expect("cleanup should finish");
        cleanup_task.await.expect("cleanup task should not panic");
        assert!(!temp_path.exists(), "temporary data should be removed only after runtime cleanup");
    }
}
