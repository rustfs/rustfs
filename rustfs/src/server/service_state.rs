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

use atomic_enum::atomic_enum;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::{info, warn};

// a configurable shutdown timeout
pub const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(1);

const SERVICE_STATUS_STARTING: &str = "Starting";
const SERVICE_STATUS_RUNNING: &str = "Running";
const SERVICE_STATUS_STOPPING: &str = "Stopping";
const SERVICE_STATUS_STOPPED: &str = "Stopped";

#[derive(Debug)]
pub enum ShutdownSignal {
    CtrlC,
    #[cfg(unix)]
    Sigterm,
    #[cfg(unix)]
    Sigint,
}

#[atomic_enum]
#[derive(PartialEq)]
pub enum ServiceState {
    Starting,
    Ready,
    Stopping,
    Stopped,
}

#[cfg(unix)]
pub async fn wait_for_shutdown() -> ShutdownSignal {
    use tokio::signal::unix::{SignalKind, signal};
    let mut sigterm = signal(SignalKind::terminate()).expect("failed to create SIGTERM signal handler");
    let mut sigint = signal(SignalKind::interrupt()).expect("failed to create SIGINT signal handler");

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!("RustFS Received Ctrl-C signal");
            ShutdownSignal::CtrlC
        }
        _ = sigint.recv() => {
            info!("RustFS Received SIGINT signal");
            ShutdownSignal::Sigint
        }
        _ = sigterm.recv() => {
            info!("RustFS Received SIGTERM signal");
            ShutdownSignal::Sigterm
        }
    }
}

#[cfg(not(unix))]
pub async fn wait_for_shutdown() -> ShutdownSignal {
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!("Received Ctrl-C signal");
            ShutdownSignal::CtrlC
        }
    }
}

#[derive(Clone)]
pub struct ServiceStateManager {
    state: Arc<AtomicServiceState>,
    published_state: Arc<Mutex<Option<ServiceState>>>,
}

impl ServiceStateManager {
    pub fn new() -> Self {
        Self {
            state: Arc::new(AtomicServiceState::new(ServiceState::Starting)),
            published_state: Arc::new(Mutex::new(None)),
        }
    }

    pub fn update(&self, new_state: ServiceState) {
        // Serialize transition check + state write + publish dedupe + notify as one
        // critical section to keep notification order monotonic under concurrency.
        let mut published_state = self.published_state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let current_state = self.current_state();
        if service_state_rank(new_state) < service_state_rank(current_state) {
            warn!(
                current = ?current_state,
                attempted = ?new_state,
                "Ignoring regressive service state transition"
            );
            return;
        }

        self.state.store(new_state, Ordering::SeqCst);

        if *published_state != Some(new_state) {
            *published_state = Some(new_state);
            self.notify_systemd(new_state);
        }
    }

    pub fn current_state(&self) -> ServiceState {
        self.state.load(Ordering::SeqCst)
    }

    fn notify_systemd(&self, state: ServiceState) {
        match state {
            ServiceState::Starting => {
                info!("RustFS Service is starting...");
                notify_systemd_daemon(state);
            }
            ServiceState::Ready => {
                info!("RustFS Service is running");
                notify_systemd_daemon(state);
            }
            ServiceState::Stopping => {
                info!("RustFS Service is stopping...");
                notify_systemd_daemon(state);
            }
            ServiceState::Stopped => {
                info!("RustFS Service has stopped");
                notify_systemd_daemon(state);
            }
        }
    }
}

fn service_state_rank(state: ServiceState) -> u8 {
    match state {
        ServiceState::Starting => 0,
        ServiceState::Ready => 1,
        ServiceState::Stopping => 2,
        ServiceState::Stopped => 3,
    }
}

fn systemd_status_text(state: ServiceState) -> &'static str {
    match state {
        ServiceState::Starting => SERVICE_STATUS_STARTING,
        ServiceState::Ready => SERVICE_STATUS_RUNNING,
        ServiceState::Stopping => SERVICE_STATUS_STOPPING,
        ServiceState::Stopped => SERVICE_STATUS_STOPPED,
    }
}

#[cfg(target_os = "linux")]
fn notify_systemd_daemon(state: ServiceState) {
    use libsystemd::daemon::{NotifyState, notify};
    use tracing::{debug, error};

    let status = systemd_status_text(state);
    let result = match state {
        ServiceState::Starting => notify(false, &[NotifyState::Status(status.to_string())]),
        ServiceState::Ready => notify(false, &[NotifyState::Ready, NotifyState::Status(status.to_string())]),
        ServiceState::Stopping => notify(false, &[NotifyState::Stopping, NotifyState::Status(status.to_string())]),
        ServiceState::Stopped => notify(false, &[NotifyState::Status(status.to_string())]),
    };

    if let Err(e) = result {
        error!(%status, ?state, "Failed to notify systemd: {}", e);
    } else {
        debug!(%status, ?state, "Successfully notified systemd");
    }
}

#[cfg(not(target_os = "linux"))]
fn notify_systemd_daemon(state: ServiceState) {
    info!(
        status = systemd_status_text(state),
        ?state,
        "Systemd notifications are not available on this platform"
    );
}

impl Default for ServiceStateManager {
    fn default() -> Self {
        Self::new()
    }
}

// Example of use
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_service_state_manager() {
        let manager = ServiceStateManager::new();

        // The initial state should be Starting
        assert_eq!(manager.current_state(), ServiceState::Starting);

        // Update the status to Ready
        manager.update(ServiceState::Ready);
        assert_eq!(manager.current_state(), ServiceState::Ready);

        // Update the status to Stopping
        manager.update(ServiceState::Stopping);
        assert_eq!(manager.current_state(), ServiceState::Stopping);

        // Update the status to Stopped
        manager.update(ServiceState::Stopped);
        assert_eq!(manager.current_state(), ServiceState::Stopped);
    }

    #[test]
    fn test_service_state_manager_ignores_regression() {
        let manager = ServiceStateManager::new();

        manager.update(ServiceState::Starting);
        manager.update(ServiceState::Ready);
        manager.update(ServiceState::Starting);
        assert_eq!(manager.current_state(), ServiceState::Ready);

        manager.update(ServiceState::Stopping);
        manager.update(ServiceState::Ready);
        assert_eq!(manager.current_state(), ServiceState::Stopping);
    }

    #[test]
    fn test_ready_maps_to_running_status() {
        assert_eq!(systemd_status_text(ServiceState::Ready), SERVICE_STATUS_RUNNING);
    }
}
