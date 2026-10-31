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

use crate::storage_api::startup::shutdown::{shutdown_background_services, wait_for_detached_mutations};
use crate::{
    server::{
        SHUTDOWN_TIMEOUT, ServiceState, ServiceStateManager, ShutdownHandle, ShutdownReceipt, ShutdownSignal, ShutdownWaitError,
        shutdown_event_notifier, stop_audit_system,
    },
    startup_optional_runtime_sidecars::{
        OptionalRuntimeServices, prepare_optional_runtime_shutdowns, shutdown_optional_runtime_services,
    },
    startup_runtime_sources,
};
use rustfs_heal::shutdown_ahm_services;
use rustfs_notify::NotificationLifecycleTransition;
use rustfs_object_capacity::capacity_manager::CapacityBackgroundTasks;
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
const BACKGROUND_SERVICE_CAPACITY: &str = "capacity";
const EVENT_EVENT_NOTIFIER_SHUTDOWN: &str = "event_notifier_shutdown";
const EVENT_PROFILING_SHUTDOWN: &str = "profiling_shutdown";
const EVENT_SERVER_SHUTDOWN_STATE: &str = "server_shutdown_state";
const DETACHED_MUTATION_SHUTDOWN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

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

/// Observe detached work after front-task joins and maintenance cleanup.
///
/// A delete whose client disconnected keeps running on its own task; wait for
/// those to reach the end of their on-disk steps. Then record the shutdown as
/// clean, so the next startup skips the unclean-restart erasure-set heal, but
/// only if nothing was left unfinished: a detached mutation still running at
/// the deadline can be cut short with the runtime, and a background handoff
/// may not have completed. Returns whether the markers were cleared.
async fn finish_detached_mutations_and_clear_unclean_shutdown_markers<W, WaitFut, ClearFut>(
    background_handoff_complete: bool,
    maintenance_result: &std::io::Result<()>,
    front_result: &FrontShutdownReport,
    detached_mutation_timeout: std::time::Duration,
    wait_for_detached_mutations: W,
    clear_unclean_shutdown_markers: ClearFut,
) -> bool
where
    W: FnOnce(std::time::Duration) -> WaitFut,
    WaitFut: Future<Output = bool>,
    ClearFut: Future<Output = ()>,
{
    let maintenance_retired = match maintenance_result {
        Ok(()) => true,
        Err(error) => error
            .get_ref()
            .and_then(|error| error.downcast_ref::<MaintenanceShutdownFailure>())
            .is_some_and(|failure| failure.joined),
    };
    if !maintenance_retired || !front_result.producers_retired() {
        return false;
    }
    let detached_mutations_drained = wait_for_detached_mutations(detached_mutation_timeout).await;
    if !detached_mutations_drained {
        warn!(
            target: "rustfs::main::handle_shutdown",
            event = EVENT_BACKGROUND_SERVICE_SHUTDOWN,
            component = LOG_COMPONENT_MAIN,
            subsystem = LOG_SUBSYSTEM_STARTUP,
            service = "detached_mutations",
            state = "stop_failed",
            reason = "timeout",
            timeout_secs = detached_mutation_timeout.as_secs(),
            "Detached mutations did not finish before shutdown; retaining unclean-shutdown markers"
        );
    }
    if !(background_handoff_complete
        && maintenance_result.is_ok()
        && front_result.allows_marker_clear()
        && detached_mutations_drained)
    {
        return false;
    }
    clear_unclean_shutdown_markers.await;
    true
}

type PendingCapacityShutdown = std::pin::Pin<Box<dyn Future<Output = std::result::Result<(), tokio::task::JoinError>> + Send>>;

/// Own the same accepted capacity shutdown through the later lifecycle cleanup.
pub(crate) struct CapacityShutdownReport {
    configured: bool,
    timed_out: bool,
    result: Option<std::result::Result<(), tokio::task::JoinError>>,
    pending: Option<PendingCapacityShutdown>,
}

#[derive(Debug)]
struct CapacityShutdownFailure {
    timed_out: bool,
    joined: bool,
    join_error: Option<tokio::task::JoinError>,
}

impl std::fmt::Display for CapacityShutdownFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let reason = match &self.join_error {
            Some(error) => join_failure_reason(error),
            None if self.joined => "owned tasks joined",
            None => "actual join unconfirmed",
        };
        if self.timed_out {
            write!(f, "capacity shutdown exceeded its original deadline: {reason}")
        } else {
            write!(f, "capacity shutdown failed: {reason}")
        }
    }
}

impl std::error::Error for CapacityShutdownFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.join_error.as_ref().map(|error| error as _)
    }
}

impl CapacityShutdownReport {
    async fn observe_deadline(&mut self) {
        let Some(shutdown) = self.pending.as_mut() else {
            return;
        };
        info!(
            target: "rustfs::main::handle_shutdown",
            event = EVENT_BACKGROUND_SERVICE_SHUTDOWN,
            component = LOG_COMPONENT_MAIN,
            subsystem = LOG_SUBSYSTEM_STARTUP,
            state = "stopping",
            service = BACKGROUND_SERVICE_CAPACITY,
            "Background service shutdown started"
        );
        let deadline = tokio::time::Instant::now() + SHUTDOWN_TIMEOUT;
        let observed = tokio::time::timeout_at(deadline, shutdown.as_mut()).await;
        // Ready wins inside Tokio Timeout, so a late observation must fail closed.
        self.timed_out |= observed.is_err() || tokio::time::Instant::now() >= deadline;
        match &observed {
            Ok(Ok(())) if !self.timed_out => info!(
                target: "rustfs::main::handle_shutdown",
                event = EVENT_BACKGROUND_SERVICE_SHUTDOWN,
                component = LOG_COMPONENT_MAIN,
                subsystem = LOG_SUBSYSTEM_STARTUP,
                state = "stopped",
                service = BACKGROUND_SERVICE_CAPACITY,
                "Background service shutdown completed"
            ),
            Ok(Err(err)) if !self.timed_out => error!(
                target: "rustfs::main::handle_shutdown",
                event = EVENT_BACKGROUND_SERVICE_SHUTDOWN,
                component = LOG_COMPONENT_MAIN,
                subsystem = LOG_SUBSYSTEM_STARTUP,
                state = "stop_failed",
                service = BACKGROUND_SERVICE_CAPACITY,
                reason = join_failure_reason(err),
                "Background service shutdown failed"
            ),
            _ => error!(
                target: "rustfs::main::handle_shutdown",
                event = EVENT_BACKGROUND_SERVICE_SHUTDOWN,
                component = LOG_COMPONENT_MAIN,
                subsystem = LOG_SUBSYSTEM_STARTUP,
                state = "stop_failed",
                service = BACKGROUND_SERVICE_CAPACITY,
                reason = "timeout",
                timeout_secs = SHUTDOWN_TIMEOUT.as_secs(),
                "Background service shutdown timed out"
            ),
        }
        if let Ok(result) = observed {
            self.result = Some(result);
            self.pending = None;
        }
    }

    fn allows_marker_clear(&self) -> bool {
        !self.configured || (!self.timed_out && matches!(self.result, Some(Ok(()))))
    }

    pub(crate) async fn finish(mut self) -> std::io::Result<()> {
        if let Some(pending) = self.pending.take() {
            // Cleanup-only observation of the same future never resets its deadline.
            self.result = Some(pending.await);
        }
        if !self.configured || (!self.timed_out && matches!(self.result, Some(Ok(())))) {
            return Ok(());
        }
        let joined = self.result.is_some();
        let join_error = self.result.and_then(std::result::Result::err);
        Err(std::io::Error::new(
            if self.timed_out {
                std::io::ErrorKind::TimedOut
            } else {
                std::io::ErrorKind::Other
            },
            CapacityShutdownFailure {
                timed_out: self.timed_out,
                joined,
                join_error,
            },
        ))
    }
}

#[derive(Debug)]
struct MaintenanceShutdownFailure {
    timed_out: bool,
    joined: bool,
    join_error: Option<tokio::task::JoinError>,
}

impl std::fmt::Display for MaintenanceShutdownFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let reason = match &self.join_error {
            Some(error) => join_failure_reason(error),
            None if self.joined => "owned task joined",
            None => "actual join unconfirmed",
        };
        if self.timed_out {
            write!(f, "maintenance shutdown exceeded its original deadline: {reason}")
        } else {
            write!(f, "maintenance shutdown failed: {reason}")
        }
    }
}

impl std::error::Error for MaintenanceShutdownFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.join_error.as_ref().map(|error| error as _)
    }
}

pub(crate) async fn stop_table_maintenance(
    runtime: Option<crate::table_catalog::PeriodicMaintenanceRuntime>,
) -> std::io::Result<()> {
    let Some(mut runtime) = runtime else {
        return Ok(());
    };
    runtime.request_stop();
    let join = runtime.join();
    tokio::pin!(join);
    let deadline = tokio::time::Instant::now() + SHUTDOWN_TIMEOUT;
    let observed = tokio::time::timeout_at(deadline, join.as_mut()).await;
    // Tokio polls Ready first, so confirm observation against the original deadline.
    let timed_out = observed.is_err() || tokio::time::Instant::now() >= deadline;
    let observed_join = match observed {
        Ok(result) => result,
        // The original deadline stays failed while this same owner finishes.
        Err(_) => join.await,
    };
    if !timed_out && matches!(observed_join.as_ref(), Some(Ok(()))) {
        return Ok(());
    }
    Err(std::io::Error::new(
        if timed_out {
            std::io::ErrorKind::TimedOut
        } else {
            std::io::ErrorKind::Other
        },
        MaintenanceShutdownFailure {
            timed_out,
            joined: observed_join.is_some(),
            join_error: observed_join.and_then(std::result::Result::err),
        },
    ))
}

#[derive(Debug, Default)]
enum FrontObservation {
    #[default]
    NotConfigured,
    Unconfirmed,
    Observed(Result<ShutdownReceipt, ShutdownWaitError>),
}

impl FrontObservation {
    fn http12(&self) -> Option<&crate::server::Http12ShutdownOutcome> {
        match self {
            Self::Observed(Ok(ShutdownReceipt::Http12ChildrenRetired(outcome))) => Some(outcome),
            Self::Observed(Err(error)) => error.http12_outcome(),
            _ => None,
        }
    }

    fn error(&self) -> Option<ShutdownWaitError> {
        match self {
            Self::Observed(Err(error)) => Some(error.clone()),
            Self::Observed(Ok(ShutdownReceipt::Http12ChildrenRetired(outcome))) => outcome.late_failure(),
            _ => None,
        }
    }

    fn retired(&self) -> bool {
        matches!(self, Self::NotConfigured) || self.http12().is_some_and(|outcome| outcome.verified_scope_complete())
    }

    fn allows_marker_clear(&self) -> bool {
        matches!(self, Self::NotConfigured)
            || matches!(self, Self::Observed(Ok(ShutdownReceipt::Http12ChildrenRetired(outcome))) if outcome.verified_scope_complete())
    }
}

/// Retain positive receipts as well as failed/unknown observations until Root consumes them.
#[derive(Debug, Default)]
pub(crate) struct FrontShutdownReport {
    s3: FrontObservation,
    console: FrontObservation,
    optional: Vec<ShutdownWaitError>,
    optional_expected: Option<usize>,
    optional_joined: bool,
}

impl FrontShutdownReport {
    fn producers_retired(&self) -> bool {
        self.s3.retired()
            && self.console.retired()
            && self.optional_expected.is_none_or(|count| count == 0 && self.optional_joined)
    }

    fn allows_marker_clear(&self) -> bool {
        self.s3.allows_marker_clear()
            && self.console.allows_marker_clear()
            && self.optional_expected.is_none_or(|count| count == 0 && self.optional_joined)
            && self.optional.is_empty()
    }

    pub(crate) fn into_result(self) -> Result<(), Box<FrontShutdownFailure>> {
        if self.allows_marker_clear() {
            return Ok(());
        }
        let deadline_missed = [&self.s3, &self.console]
            .into_iter()
            .any(|observation| observation.http12().is_some_and(|outcome| outcome.deadline_missed()));
        let incomplete_leaf_proof = !self.producers_retired();
        Err(Box::new(FrontShutdownFailure {
            s3: self.s3.error(),
            console: self.console.error(),
            optional: self.optional,
            deadline_missed,
            incomplete_leaf_proof,
        }))
    }
}

#[derive(Debug)]
pub(crate) struct FrontShutdownFailure {
    s3: Option<ShutdownWaitError>,
    console: Option<ShutdownWaitError>,
    optional: Vec<ShutdownWaitError>,
    deadline_missed: bool,
    incomplete_leaf_proof: bool,
}

impl FrontShutdownFailure {
    pub(crate) fn deadline_missed(&self) -> bool {
        self.deadline_missed
    }
}

impl std::fmt::Display for FrontShutdownFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("front task shutdown failed")?;
        if self.incomplete_leaf_proof {
            f.write_str("; configured front child retirement unconfirmed")?;
        }
        for (service, error) in [("s3", self.s3.as_ref()), ("console", self.console.as_ref())] {
            if let Some(error) = error {
                write!(f, "; {service}: {error}")?;
            }
        }
        for error in &self.optional {
            write!(f, "; optional_protocol: {error}")?;
        }
        Ok(())
    }
}

impl std::error::Error for FrontShutdownFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.s3
            .as_ref()
            .or(self.console.as_ref())
            .or(self.optional.first())
            .map(|error| error as _)
    }
}

pub(crate) async fn join_startup_fronts(
    s3: &mut Option<ShutdownHandle>,
    console: &mut Option<ShutdownHandle>,
    optional: &mut Vec<ShutdownHandle>,
    report: &mut FrontShutdownReport,
) {
    for handle in s3.iter().chain(console.iter()).chain(optional.iter()) {
        handle.signal();
    }
    if s3.is_some() && matches!(report.s3, FrontObservation::NotConfigured) {
        report.s3 = FrontObservation::Unconfirmed;
    }
    if console.is_some() && matches!(report.console, FrontObservation::NotConfigured) {
        report.console = FrontObservation::Unconfirmed;
    }
    if report.optional_expected.is_none() {
        report.optional_expected = Some(optional.len());
    }
    if let Some(handle) = s3.as_mut() {
        report.s3 = FrontObservation::Observed(handle.wait_checked().await);
        let _ = s3.take();
    }
    if let Some(handle) = console.as_mut() {
        report.console = FrontObservation::Observed(handle.wait_checked().await);
        let _ = console.take();
    }
    shutdown_optional_runtime_services(optional, &mut report.optional).await;
    report.optional_joined = true;
}

pub(crate) struct StartupBackgroundTasks {
    pub(crate) capacity: Option<CapacityBackgroundTasks>,
    pub(crate) table_maintenance: Option<crate::table_catalog::PeriodicMaintenanceRuntime>,
}

pub(crate) async fn run_startup_shutdown_sequence(
    state_manager: &ServiceStateManager,
    shutdown_signal: ShutdownSignal,
    mut s3_shutdown_handle: Option<ShutdownHandle>,
    mut console_shutdown_handle: Option<ShutdownHandle>,
    optional_runtimes: OptionalRuntimeServices,
    background_tasks: StartupBackgroundTasks,
    ctx: CancellationToken,
) -> (CapacityShutdownReport, std::io::Result<()>, FrontShutdownReport) {
    let StartupBackgroundTasks {
        capacity: capacity_tasks,
        table_maintenance,
    } = background_tasks;
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

    if let Some(handle) = &s3_shutdown_handle {
        handle.signal();
    }
    if let Some(handle) = &console_shutdown_handle {
        handle.signal();
    }

    let mut optional_runtime_shutdowns = prepare_optional_runtime_shutdowns(optional_runtimes);
    for handle in &optional_runtime_shutdowns {
        handle.signal();
    }

    let mut capacity_report = CapacityShutdownReport {
        configured: capacity_tasks.is_some(),
        timed_out: false,
        result: None,
        pending: capacity_tasks.map(|tasks| Box::pin(tasks.shutdown()) as _),
    };
    capacity_report.observe_deadline().await;

    let enable_scanner = get_env_bool_with_aliases(ENV_SCANNER_ENABLED, &[ENV_SCANNER_ENABLED_DEPRECATED], true);
    let enable_heal = get_env_bool_with_aliases(ENV_HEAL_ENABLED, &[ENV_HEAL_ENABLED_DEPRECATED], true);

    let mut heal_handoff_complete = true;
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
                if let Err(error) = shutdown_ahm_services().await {
                    heal_handoff_complete = false;
                    warn!(
                        target: "rustfs::main::handle_shutdown",
                        event = EVENT_BACKGROUND_SERVICE_SHUTDOWN,
                        component = LOG_COMPONENT_MAIN,
                        subsystem = LOG_SUBSYSTEM_STARTUP,
                        service = "ahm",
                        state = "handoff_failed",
                        error = %error,
                        "Heal shutdown handoff failed; retaining unclean-shutdown markers"
                    );
                }
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
    let mut front_failures = FrontShutdownReport::default();
    join_startup_fronts(
        &mut s3_shutdown_handle,
        &mut console_shutdown_handle,
        &mut optional_runtime_shutdowns,
        &mut front_failures,
    )
    .await;
    let front_result = front_failures;
    let maintenance_result = stop_table_maintenance(table_maintenance).await;
    // Unknown front leaves cannot authorize the downstream fence. A retired HTTP
    // scope may finish cleanup after a late/failed receipt, while its marker stays dirty.
    finish_detached_mutations_and_clear_unclean_shutdown_markers(
        heal_handoff_complete && capacity_report.allows_marker_clear(),
        &maintenance_result,
        &front_result,
        DETACHED_MUTATION_SHUTDOWN_TIMEOUT,
        wait_for_detached_mutations,
        rustfs_heal::heal::clear_unclean_shutdown_markers(),
    )
    .await;
    state_manager.update(ServiceState::Stopped);
    info!(
        target: "rustfs::main::handle_shutdown",
        event = EVENT_SERVER_SHUTDOWN_STATE,
        component = LOG_COMPONENT_MAIN,
        subsystem = LOG_SUBSYSTEM_STARTUP,
        state = "stopped",
        "RustFS server stopped"
    );
    (capacity_report, maintenance_result, front_result)
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
        BackgroundShutdownStep, EmbeddedRuntimeOwners, background_shutdown_steps,
        finish_detached_mutations_and_clear_unclean_shutdown_markers, finish_embedded_server_cleanup,
        run_embedded_server_drop_cleanup, signal_embedded_startup_shutdown,
    };
    use crate::server::ShutdownHandle;
    use std::sync::{Arc, mpsc};
    use std::time::Duration;
    use tokio::sync::broadcast;
    use tokio_util::sync::CancellationToken;

    #[tokio::test(start_paused = true)]
    async fn capacity_report_late_ready_observation_keeps_deadline_failure_and_original_join() {
        for panics in [false, true] {
            let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
            let (release_tx, release_rx) = tokio::sync::oneshot::channel();
            let (completed_tx, completed_rx) = tokio::sync::oneshot::channel();
            let task = tokio::spawn(async move {
                let _ = entered_tx.send(());
                if release_rx.await.is_ok() {
                    let _ = completed_tx.send(());
                    assert!(!panics, "expected late capacity task failure");
                }
            });
            let task_id = task.id();
            let task_finished = task.abort_handle();
            entered_rx.await.expect("the real task must start before observation");
            let mut report = super::CapacityShutdownReport {
                configured: true,
                timed_out: false,
                result: None,
                pending: Some(Box::pin(task)),
            };
            let (first_pending, released, completed) = {
                let mut observation = Box::pin(report.observe_deadline());
                let first_pending = std::future::poll_fn(|cx| {
                    std::task::Poll::Ready(std::future::Future::poll(observation.as_mut(), cx).is_pending())
                })
                .await;
                // Leave the actual observer unpolled until the original deadline
                // has passed and the same real task has completely finished.
                tokio::time::advance(super::SHUTDOWN_TIMEOUT + Duration::from_millis(1)).await;
                let released = release_tx.send(()).is_ok();
                let completed = completed_rx.await.is_ok();
                while !task_finished.is_finished() {
                    tokio::task::yield_now().await;
                }
                if first_pending {
                    observation.await;
                }
                (first_pending, released, completed)
            };
            let timed_out = report.timed_out;
            let marker_allowed = report.allows_marker_clear();
            let consumed = report.pending.is_none();
            let joined = report.result.is_some();
            let error = report.finish().await.expect_err("a late Ready observation must stay failed");
            // Every assertion follows release, actual task exit and final observation.
            assert!(first_pending && released && completed && consumed && joined);
            assert!(timed_out && !marker_allowed);
            assert_eq!(error.kind(), std::io::ErrorKind::TimedOut);
            if panics {
                let failure = error.get_ref().expect("typed capacity failure");
                let source = std::error::Error::source(failure).expect("late actual JoinError source");
                let source = source
                    .downcast_ref::<tokio::task::JoinError>()
                    .expect("actual JoinError type");
                assert!(source.is_panic());
                assert_eq!(source.id(), task_id);
            }
        }
    }

    async fn assert_early_tls_receipt_final_join(fail: bool) {
        use super::{FrontObservation, FrontShutdownReport};
        use crate::server::ShutdownReceipt;

        let release = tokio_util::sync::CancellationToken::new();
        let child_release = release.clone();
        // This unit fixture tests Root propagation only, not TLS/native retirement.
        let actual_task = tokio::spawn(async move {
            child_release.cancelled().await;
            assert!(!fail, "test late TLS driver panic");
        });
        let receipt = crate::server::tls_material::test_retirement_from_task(actual_task);
        let outcome =
            crate::server::test_retire_http12_scope(std::future::ready(()), tokio::time::Instant::now() + Duration::from_secs(5))
                .await
                .with_test_tls_receipt(receipt.clone());
        let before_join = outcome.verified_scope_complete();
        let original_deadline_missed = outcome.deadline_missed();
        let report = FrontShutdownReport {
            s3: FrontObservation::Observed(Ok(ShutdownReceipt::Http12ChildrenRetired(outcome))),
            ..Default::default()
        };
        release.cancel();
        receipt.test_observe_join().await;
        let result = report.into_result();
        assert!(!before_join, "an own-round receipt cannot certify an unjoined group");
        assert!(!original_deadline_missed, "this original scope observation was timely");
        if fail {
            let failure = result.expect_err("late real group failure vetoes marker clearing");
            let error = failure.s3.expect("the earlier positive receipt retains the late failure");
            let source = std::error::Error::source(&error).expect("actual JoinError source");
            assert!(
                source
                    .downcast_ref::<tokio::task::JoinError>()
                    .expect("typed JoinError")
                    .is_panic()
            );
            assert!(!failure.deadline_missed, "late error does not rewrite the frozen scope deadline");
        } else {
            result.expect("retained early receipt sees the eventual original successful group join");
        }
    }

    #[tokio::test]
    async fn early_positive_tls_receipt_preserves_late_original_join_error() {
        assert_early_tls_receipt_final_join(true).await;
    }

    #[tokio::test]
    async fn early_positive_tls_receipt_sees_original_group_join_success() {
        assert_early_tls_receipt_final_join(false).await;
    }

    #[tokio::test]
    async fn capacity_report_ready_future_is_polled_once_and_cached_success_finishes() {
        let polls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let observed_polls = polls.clone();
        let report = super::CapacityShutdownReport {
            configured: true,
            timed_out: false,
            result: None,
            pending: Some(Box::pin(std::future::poll_fn(move |_| {
                assert_eq!(observed_polls.fetch_add(1, std::sync::atomic::Ordering::SeqCst), 0);
                std::task::Poll::Ready(Ok(()))
            }))),
        };
        assert!(!report.allows_marker_clear(), "unobserved completion cannot clear markers");
        report.finish().await.expect("the ready future should finish once");
        assert_eq!(polls.load(std::sync::atomic::Ordering::SeqCst), 1);

        let completed = super::CapacityShutdownReport {
            configured: true,
            timed_out: false,
            result: Some(Ok(())),
            pending: None,
        };
        assert!(completed.allows_marker_clear());
        completed
            .finish()
            .await
            .expect("consumed timely completion needs no second poll");
    }

    #[tokio::test]
    async fn capacity_report_configured_unknown_completion_fails_closed() {
        let report = super::CapacityShutdownReport {
            configured: true,
            timed_out: false,
            result: None,
            pending: None,
        };
        assert!(!report.allows_marker_clear());
        assert_eq!(
            report.finish().await.expect_err("unknown completion must fail").kind(),
            std::io::ErrorKind::Other
        );
    }

    #[tokio::test]
    async fn capacity_report_timeout_waits_for_held_future_and_stays_failed() {
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let (completed_tx, completed_rx) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            started_tx.send(()).expect("the test should observe the task start");
            release_rx.await.expect("the same held task must remain alive until release");
            completed_tx.send(()).expect("the test should observe actual completion");
        });
        started_rx.await.expect("the real task should start");
        let report = super::CapacityShutdownReport {
            configured: true,
            timed_out: true,
            result: None,
            pending: Some(Box::pin(task)),
        };
        assert!(!report.allows_marker_clear());
        let mut finish = Box::pin(report.finish());
        tokio::select! {
            biased;
            result = finish.as_mut() => panic!("finish returned before the held task was released: {result:?}"),
            _ = std::future::ready(()) => {}
        }
        release_tx.send(()).expect("finish must not cancel the held task");
        let error = finish
            .await
            .expect_err("late successful join must retain the timeout failure");
        assert_eq!(error.kind(), std::io::ErrorKind::TimedOut);
        completed_rx
            .await
            .expect("the same task must really complete before finish returns");

        let completed_late = super::CapacityShutdownReport {
            configured: true,
            timed_out: true,
            result: Some(Ok(())),
            pending: None,
        };
        assert!(
            !completed_late.allows_marker_clear(),
            "late completion must not restore marker eligibility"
        );
        assert_eq!(
            completed_late
                .finish()
                .await
                .expect_err("cached late completion must remain failed")
                .kind(),
            std::io::ErrorKind::TimedOut
        );
    }

    #[tokio::test]
    async fn capacity_report_preserves_actual_join_error_source() {
        let task = tokio::spawn(async { panic!("expected capacity report task failure") });
        let task_id = task.id();
        let join_error = task.await.expect_err("the real task should panic");
        let report = super::CapacityShutdownReport {
            configured: true,
            timed_out: false,
            result: Some(Err(join_error)),
            pending: None,
        };
        assert!(!report.allows_marker_clear());
        let error = report
            .finish()
            .await
            .expect_err("the observed task failure must remain a failure");
        assert_eq!(error.kind(), std::io::ErrorKind::Other);
        let failure = error.get_ref().expect("the typed capacity failure must remain attached");
        let source = std::error::Error::source(failure).expect("the real JoinError must remain the source");
        let source = source
            .downcast_ref::<tokio::task::JoinError>()
            .expect("the source must retain its JoinError type");
        assert!(source.is_panic());
        assert_eq!(source.id(), task_id, "the original task identity must survive error propagation");
    }

    #[tokio::test]
    async fn maintenance_failures_keep_markers_after_detached_mutations_finish() {
        let task = tokio::spawn(async { panic!("expected maintenance marker fixture failure") });
        let join_error = task.await.expect_err("actual task should fail");
        for failure in [
            super::MaintenanceShutdownFailure {
                timed_out: true,
                joined: true,
                join_error: None,
            },
            super::MaintenanceShutdownFailure {
                timed_out: false,
                joined: false,
                join_error: None,
            },
            super::MaintenanceShutdownFailure {
                timed_out: false,
                joined: true,
                join_error: Some(join_error),
            },
        ] {
            let retired = failure.joined;
            let temp_dir = tempfile::tempdir().expect("marker fixture directory");
            let marker = temp_dir.path().join("unclean-shutdown");
            std::fs::write(&marker, b"").expect("marker fixture must exist");
            let maintenance_result = Err(std::io::Error::new(
                if failure.timed_out {
                    std::io::ErrorKind::TimedOut
                } else {
                    std::io::ErrorKind::Other
                },
                failure,
            ));
            let waited = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let observed_wait = waited.clone();
            let cleared = finish_detached_mutations_and_clear_unclean_shutdown_markers(
                true,
                &maintenance_result,
                &super::FrontShutdownReport::default(),
                super::DETACHED_MUTATION_SHUTDOWN_TIMEOUT,
                |timeout| async move {
                    assert_eq!(timeout, super::DETACHED_MUTATION_SHUTDOWN_TIMEOUT);
                    observed_wait.store(true, std::sync::atomic::Ordering::SeqCst);
                    true
                },
                async {
                    std::fs::remove_file(&marker).expect("only a verified shutdown may clear the marker");
                },
            )
            .await;
            assert!(!cleared, "late, unconfirmed and failed maintenance must retain markers");
            assert!(marker.exists(), "completed detached mutations cannot erase the maintenance failure");
            assert_eq!(
                waited.load(std::sync::atomic::Ordering::SeqCst),
                retired,
                "detached waiting requires an actual maintenance join"
            );
        }
    }

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

    async fn finish_shutdown_with_marker(
        marker: &std::path::Path,
        heal_handoff_complete: bool,
        timeout: Duration,
        detached_mutation: &mut tokio::task::JoinHandle<()>,
    ) -> bool {
        finish_detached_mutations_and_clear_unclean_shutdown_markers(
            heal_handoff_complete,
            &Ok(()),
            &super::FrontShutdownReport::default(),
            timeout,
            |timeout| async move { tokio::time::timeout(timeout, detached_mutation).await.is_ok() },
            async {
                std::fs::remove_file(marker).expect("unclean-shutdown marker should be removable");
            },
        )
        .await
    }

    #[tokio::test]
    async fn unclean_shutdown_markers_are_kept_while_a_detached_mutation_runs() {
        let temp_dir = tempfile::tempdir().expect("temp dir should create");
        let marker = temp_dir.path().join("unclean-shutdown");
        std::fs::write(&marker, b"").expect("unclean-shutdown marker should be written");
        let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
        let mut detached_mutation = tokio::spawn(async move {
            let _ = release_rx.await;
        });

        assert!(
            !finish_shutdown_with_marker(&marker, true, Duration::from_millis(50), &mut detached_mutation).await,
            "a detached mutation still running at the deadline must keep the markers"
        );
        assert!(marker.exists(), "the next startup must still see the unclean-shutdown marker");

        release_tx.send(()).expect("detached mutation should be released");
        assert!(
            finish_shutdown_with_marker(&marker, true, Duration::from_secs(5), &mut detached_mutation).await,
            "the markers should be cleared once every detached mutation has finished"
        );
        assert!(!marker.exists(), "a clean shutdown should clear the unclean-shutdown marker");
    }

    #[tokio::test]
    async fn unclean_shutdown_markers_are_kept_after_a_failed_heal_handoff() {
        let temp_dir = tempfile::tempdir().expect("temp dir should create");
        let marker = temp_dir.path().join("unclean-shutdown");
        std::fs::write(&marker, b"").expect("unclean-shutdown marker should be written");
        let mut detached_mutation = tokio::spawn(async {});

        assert!(
            !finish_shutdown_with_marker(&marker, false, Duration::from_secs(5), &mut detached_mutation).await,
            "a failed heal handoff must keep the markers"
        );
        assert!(marker.exists(), "the next startup must still see the unclean-shutdown marker");
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
    #[tokio::test]
    async fn front_join_collector_retains_panic_and_console_owner_after_cancelled_observer() {
        use std::{future::Future, task::Poll};
        let (tx, _rx) = broadcast::channel(1);
        let panic = tokio::spawn(async { panic!("expected s3 outer task panic") });
        let panic_id = panic.id();
        let ready = tokio::time::timeout(Duration::from_secs(5), async {
            while !panic.is_finished() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .is_ok();
        let mut s3 = Some(ShutdownHandle::new(tx, panic));
        let (tx, mut rx) = broadcast::channel(1);
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        let mut console = Some(ShutdownHandle::new(
            tx,
            tokio::spawn(async move {
                let _ = rx.recv().await;
                let _ = entered_tx.send(());
                let _ = release_rx.await;
                let _ = done_tx.send(());
            }),
        ));
        let mut optional = Vec::new();
        let mut failures = super::FrontShutdownReport::default();
        let mut observer = Box::pin(super::join_startup_fronts(&mut s3, &mut console, &mut optional, &mut failures));
        let pending = std::future::poll_fn(|cx| Poll::Ready(observer.as_mut().poll(cx).is_pending())).await;
        drop(observer);
        let retained = s3.is_none() && console.is_some() && failures.s3.error().is_some();
        let entered = tokio::time::timeout(Duration::from_secs(5), entered_rx)
            .await
            .is_ok_and(|r| r.is_ok());
        if let Some(handle) = &console {
            handle.signal();
        }
        let _ = release_tx.send(());
        let completed = tokio::time::timeout(Duration::from_secs(5), done_rx)
            .await
            .is_ok_and(|r| r.is_ok());
        super::join_startup_fronts(&mut s3, &mut console, &mut optional, &mut failures).await;
        let failure = failures
            .into_result()
            .expect_err("the earlier panic cannot be overwritten on retry");
        assert!(ready && pending && retained && entered && completed);
        assert!(s3.is_none() && console.is_none() && optional.is_empty());
        match failure.s3.expect("s3 failure remains owned") {
            crate::server::ShutdownWaitError::Join(error) => {
                assert!(error.is_panic());
                assert_eq!(error.id(), panic_id);
            }
            error => panic!("actual panic was erased: {error}"),
        }
    }

    #[tokio::test]
    async fn front_join_collector_joins_held_optional_after_other_front_failures() {
        use std::{future::Future, task::Poll};
        let (tx, _rx) = broadcast::channel(1);
        let s3 = tokio::spawn(async { panic!("expected s3 aggregate failure") });
        let s3_id = s3.id();
        let (optional_tx, _rx) = broadcast::channel(1);
        let optional_panic = tokio::spawn(async { panic!("expected optional aggregate failure") });
        let optional_id = optional_panic.id();
        let ready = tokio::time::timeout(Duration::from_secs(5), async {
            while !s3.is_finished() || !optional_panic.is_finished() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .is_ok();
        let mut s3 = Some(ShutdownHandle::new(tx, s3));
        let mut console = None;
        let (held_tx, mut held_rx) = broadcast::channel(1);
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        let held = ShutdownHandle::new(
            held_tx,
            tokio::spawn(async move {
                let _ = held_rx.recv().await;
                let _ = entered_tx.send(());
                let _ = release_rx.await;
                let _ = done_tx.send(());
            }),
        );
        let mut optional = vec![ShutdownHandle::new(optional_tx, optional_panic), held];
        let mut failures = super::FrontShutdownReport::default();
        let mut observer = Box::pin(super::join_startup_fronts(&mut s3, &mut console, &mut optional, &mut failures));
        let pending = std::future::poll_fn(|cx| Poll::Ready(observer.as_mut().poll(cx).is_pending())).await;
        drop(observer);
        let retained = optional.len() == 1 && failures.s3.error().is_some() && failures.optional.len() == 1;
        let entered = tokio::time::timeout(Duration::from_secs(5), entered_rx)
            .await
            .is_ok_and(|r| r.is_ok());
        for handle in &optional {
            handle.signal();
        }
        let _ = release_tx.send(());
        let completed = tokio::time::timeout(Duration::from_secs(5), done_rx)
            .await
            .is_ok_and(|r| r.is_ok());
        super::join_startup_fronts(&mut s3, &mut console, &mut optional, &mut failures).await;
        let failure = failures.into_result().expect_err("all real join failures are retained");
        assert!(ready && pending && retained && entered && completed);
        assert!(s3.is_none() && console.is_none() && optional.is_empty());
        match failure.s3.expect("s3 error") {
            crate::server::ShutdownWaitError::Join(error) => assert_eq!(error.id(), s3_id),
            error => panic!("lost s3 error: {error}"),
        }
        match failure.optional.first().expect("optional error") {
            crate::server::ShutdownWaitError::Join(error) => assert_eq!(error.id(), optional_id),
            error => panic!("lost optional error: {error}"),
        }
    }

    #[tokio::test]
    async fn front_panic_and_unknown_receipts_keep_actual_marker_after_detached_cleanup() {
        for unknown in [false, true] {
            let (tx, _rx) = broadcast::channel(1);
            let task = tokio::spawn(async move {
                if !unknown {
                    panic!("expected front marker failure");
                }
            });
            let mut handle = ShutdownHandle::new(tx, task);
            if unknown {
                handle.wait_checked().await.expect("consume the real initial receipt");
            }
            let mut s3 = Some(handle);
            let mut console = None;
            let mut optional = Vec::new();
            let mut failures = super::FrontShutdownReport::default();
            super::join_startup_fronts(&mut s3, &mut console, &mut optional, &mut failures).await;
            let front_result = failures;
            let temp = tempfile::tempdir().expect("owned marker directory");
            let marker = temp.path().join("unclean-shutdown");
            std::fs::write(&marker, b"retained unclean marker").expect("real marker");
            let waited = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let observed = waited.clone();
            let cleared = finish_detached_mutations_and_clear_unclean_shutdown_markers(
                true,
                &Ok(()),
                &front_result,
                super::DETACHED_MUTATION_SHUTDOWN_TIMEOUT,
                move |_| async move {
                    observed.store(true, std::sync::atomic::Ordering::SeqCst);
                    true
                },
                async {
                    std::fs::remove_file(&marker).expect("only successful receipts may clear");
                },
            )
            .await;
            assert!(!cleared && front_result.into_result().is_err());
            assert!(
                !waited.load(std::sync::atomic::Ordering::SeqCst),
                "unknown front leaves must not close the downstream fence"
            );
            assert_eq!(std::fs::read(&marker).expect("marker must remain"), b"retained unclean marker");
            assert!(s3.is_none() && console.is_none() && optional.is_empty());
        }
    }
    #[tokio::test]
    async fn http12_retired_late_failure_allows_cleanup_but_never_clears_marker() {
        let (tx, _rx) = broadcast::channel(1);
        let task = tokio::spawn(crate::server::test_retire_http12_scope(
            async {
                panic!("expected original scoped HTTP child failure");
            },
            tokio::time::Instant::now(),
        ));
        let mut s3 = Some(ShutdownHandle::new_http12(tx, task));
        let mut console = None;
        let mut optional = Vec::new();
        let mut report = super::FrontShutdownReport::default();
        super::join_startup_fronts(&mut s3, &mut console, &mut optional, &mut report).await;
        assert!(report.producers_retired(), "actual scope joined despite its failed deadline");
        let dir = tempfile::tempdir().expect("owned marker fixture");
        let marker = dir.path().join("unclean-shutdown");
        std::fs::write(&marker, b"retained").expect("real marker");
        let waited = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let observed = waited.clone();
        let cleared = finish_detached_mutations_and_clear_unclean_shutdown_markers(
            true,
            &Ok(()),
            &report,
            super::DETACHED_MUTATION_SHUTDOWN_TIMEOUT,
            move |_| async move {
                observed.store(true, std::sync::atomic::Ordering::SeqCst);
                true
            },
            async {
                std::fs::remove_file(&marker).expect("only successful proof can clear");
            },
        )
        .await;
        assert!(
            waited.load(std::sync::atomic::Ordering::SeqCst),
            "retired-but-failed scope permits cleanup only"
        );
        assert!(!cleared);
        assert_eq!(std::fs::read(&marker).expect("marker remains"), b"retained");
        let error = report.into_result().expect_err("original failure remains");
        assert!(error.deadline_missed());
        let source = std::error::Error::source(&error).expect("HTTP wait error");
        assert!(
            std::error::Error::source(source)
                .expect("original child JoinError")
                .is::<tokio::task::JoinError>()
        );
    }
    #[tokio::test]
    async fn configured_optional_outer_join_does_not_fabricate_leaf_proof() {
        let (tx, _rx) = broadcast::channel(1);
        let mut optional = vec![ShutdownHandle::new(tx, tokio::spawn(async {}))];
        let mut report = super::FrontShutdownReport::default();
        super::join_startup_fronts(&mut None, &mut None, &mut optional, &mut report).await;
        assert!(optional.is_empty() && report.optional_joined, "actual outer handle joined");
        assert!(!report.producers_retired() && !report.allows_marker_clear());
        let failure = report
            .into_result()
            .expect_err("actual configured instance still lacks child proof");
        assert!(failure.incomplete_leaf_proof && failure.optional.is_empty());
        assert!(
            std::error::Error::source(&failure).is_none(),
            "do not fabricate a JoinError for unknown children"
        );
    }

    #[tokio::test]
    async fn front_signals_every_actual_instance_before_pending_first_join() {
        let (s3_tx, mut s3_rx) = broadcast::channel(1);
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let mut s3 = Some(ShutdownHandle::new(
            s3_tx,
            tokio::spawn(async move {
                let _ = s3_rx.recv().await;
                let _ = release_rx.await;
            }),
        ));
        let (console_tx, mut console_rx) = broadcast::channel(1);
        let (console_seen_tx, console_seen_rx) = tokio::sync::oneshot::channel();
        let mut console = Some(ShutdownHandle::new(
            console_tx,
            tokio::spawn(async move {
                let _ = console_rx.recv().await;
                let _ = console_seen_tx.send(());
            }),
        ));
        let (optional_tx, mut optional_rx) = broadcast::channel(1);
        let (optional_seen_tx, optional_seen_rx) = tokio::sync::oneshot::channel();
        let mut optional = vec![ShutdownHandle::new(
            optional_tx,
            tokio::spawn(async move {
                let _ = optional_rx.recv().await;
                let _ = optional_seen_tx.send(());
            }),
        )];
        let mut report = super::FrontShutdownReport::default();
        let mut observer = Box::pin(super::join_startup_fronts(&mut s3, &mut console, &mut optional, &mut report));
        let pending =
            std::future::poll_fn(|cx| std::task::Poll::Ready(std::future::Future::poll(observer.as_mut(), cx).is_pending()))
                .await;
        drop(observer);
        let console_signalled = tokio::time::timeout(Duration::from_secs(5), console_seen_rx).await;
        let optional_signalled = tokio::time::timeout(Duration::from_secs(5), optional_seen_rx).await;
        let _ = release_tx.send(());
        super::join_startup_fronts(&mut s3, &mut console, &mut optional, &mut report).await;
        assert!(pending && matches!(console_signalled, Ok(Ok(()))) && matches!(optional_signalled, Ok(Ok(()))));
        assert!(s3.is_none() && console.is_none() && optional.is_empty());
        assert!(report.into_result().is_err(), "legacy outer joins are not leaf receipts");
    }
}
