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
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tracing::info;

// a configurable shutdown timeout
pub(crate) const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(1);

#[cfg(target_os = "linux")]
fn notify_systemd(state: &str) {
    use libsystemd::daemon::{NotifyState, notify};
    use tracing::{debug, error};
    let notify_state = match state {
        "ready" => NotifyState::Ready,
        "stopping" => NotifyState::Stopping,
        _ => {
            info!("Unsupported state passed to notify_systemd: {}", state);
            return;
        }
    };

    if let Err(e) = notify(false, &[notify_state]) {
        error!("Failed to notify systemd: {}", e);
    } else {
        debug!("Successfully notified systemd: {}", state);
    }
    info!("Systemd notifications are enabled on linux (state: {})", state);
}

#[cfg(not(target_os = "linux"))]
fn notify_systemd(state: &str) {
    info!("Systemd notifications are not available on this platform not linux (state: {})", state);
}

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
pub(crate) enum ServiceState {
    Starting,
    Ready,
    Stopping,
    Stopped,
}

#[cfg(unix)]
pub(crate) async fn wait_for_shutdown() -> ShutdownSignal {
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
pub(crate) async fn wait_for_shutdown() -> ShutdownSignal {
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!("Received Ctrl-C signal");
            ShutdownSignal::CtrlC
        }
    }
}

#[derive(Clone)]
pub(crate) struct ServiceStateManager {
    state: Arc<AtomicServiceState>,
}

impl ServiceStateManager {
    pub fn new() -> Self {
        Self {
            state: Arc::new(AtomicServiceState::new(ServiceState::Starting)),
        }
    }

    pub fn update(&self, new_state: ServiceState) {
        self.state.store(new_state, Ordering::SeqCst);
        self.notify_systemd(&new_state);
    }

    pub fn current_state(&self) -> ServiceState {
        self.state.load(Ordering::SeqCst)
    }

    fn notify_systemd(&self, state: &ServiceState) {
        match state {
            ServiceState::Starting => {
                info!("RustFS Service is starting...");
                #[cfg(target_os = "linux")]
                if let Err(e) =
                    libsystemd::daemon::notify(false, &[libsystemd::daemon::NotifyState::Status("Starting...".to_string())])
                {
                    tracing::error!("Failed to notify systemd of starting state: {}", e);
                }
            }
            ServiceState::Ready => {
                info!("RustFS Service is ready");
                notify_systemd("ready");
            }
            ServiceState::Stopping => {
                info!("RustFS Service is stopping...");
                notify_systemd("stopping");
            }
            ServiceState::Stopped => {
                info!("RustFS Service has stopped");
                #[cfg(target_os = "linux")]
                if let Err(e) =
                    libsystemd::daemon::notify(false, &[libsystemd::daemon::NotifyState::Status("Stopped".to_string())])
                {
                    tracing::error!("Failed to notify systemd of stopped state: {}", e);
                }
            }
        }
    }
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
}
