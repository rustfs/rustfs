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

use crate::{
    server::ShutdownHandle,
    startup_protocols::{ProtocolShutdownSenders, init_protocol_shutdown_senders},
};
use futures_util::future::join_all;
use std::io::Result;
use tracing::info;

const LOG_COMPONENT_MAIN: &str = "main";
const LOG_SUBSYSTEM_STARTUP: &str = "startup";
const EVENT_PROTOCOL_SYSTEM_STATE: &str = "protocol_system_state";

pub(crate) struct OptionalRuntimeServices {
    pub(crate) protocols: ProtocolShutdownSenders,
}

impl OptionalRuntimeServices {
    pub(crate) fn new(protocols: ProtocolShutdownSenders) -> Self {
        Self { protocols }
    }
}

pub(crate) async fn init_optional_runtime_services() -> Result<OptionalRuntimeServices> {
    let protocols = init_protocol_shutdown_senders().await?;
    Ok(OptionalRuntimeServices::new(protocols))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OptionalRuntimeShutdownStep {
    Ftp,
    Ftps,
    Webdav,
    Sftp,
}

impl OptionalRuntimeShutdownStep {
    const fn protocol(self) -> &'static str {
        match self {
            Self::Ftp => "ftp",
            Self::Ftps => "ftps",
            Self::Webdav => "webdav",
            Self::Sftp => "sftp",
        }
    }
}

#[cfg(test)]
fn optional_runtime_shutdown_steps(protocols: &ProtocolShutdownSenders) -> Vec<OptionalRuntimeShutdownStep> {
    let mut steps = Vec::with_capacity(4);
    if protocols.ftp.is_some() {
        steps.push(OptionalRuntimeShutdownStep::Ftp);
    }
    if protocols.ftps.is_some() {
        steps.push(OptionalRuntimeShutdownStep::Ftps);
    }
    if protocols.webdav.is_some() {
        steps.push(OptionalRuntimeShutdownStep::Webdav);
    }
    if protocols.sftp.is_some() {
        steps.push(OptionalRuntimeShutdownStep::Sftp);
    }
    steps
}

pub(crate) fn prepare_optional_runtime_shutdowns(optional_runtimes: OptionalRuntimeServices) -> Vec<ShutdownHandle> {
    let ProtocolShutdownSenders { ftp, ftps, webdav, sftp } = optional_runtimes.protocols;

    let mut protocol_shutdowns = Vec::new();
    if let Some(handle) = ftp {
        log_protocol_stopping(OptionalRuntimeShutdownStep::Ftp);
        protocol_shutdowns.push(handle);
    }
    if let Some(handle) = ftps {
        log_protocol_stopping(OptionalRuntimeShutdownStep::Ftps);
        protocol_shutdowns.push(handle);
    }
    if let Some(handle) = webdav {
        log_protocol_stopping(OptionalRuntimeShutdownStep::Webdav);
        protocol_shutdowns.push(handle);
    }
    if let Some(handle) = sftp {
        log_protocol_stopping(OptionalRuntimeShutdownStep::Sftp);
        protocol_shutdowns.push(handle);
    }

    protocol_shutdowns
}

pub(crate) async fn shutdown_optional_runtime_services(protocol_shutdowns: Vec<ShutdownHandle>) {
    join_all(protocol_shutdowns.into_iter().map(ShutdownHandle::shutdown)).await;
}

fn log_protocol_stopping(step: OptionalRuntimeShutdownStep) {
    info!(
        target: "rustfs::main::handle_shutdown",
        event = EVENT_PROTOCOL_SYSTEM_STATE,
        component = LOG_COMPONENT_MAIN,
        subsystem = LOG_SUBSYSTEM_STARTUP,
        protocol = step.protocol(),
        state = "stopping",
        "Protocol runtime stopping"
    );
}

#[cfg(test)]
mod tests {
    use super::{
        OptionalRuntimeServices, OptionalRuntimeShutdownStep, optional_runtime_shutdown_steps,
        prepare_optional_runtime_shutdowns, shutdown_optional_runtime_services,
    };
    use crate::{server::ShutdownHandle, startup_protocols::ProtocolShutdownSenders};
    use std::sync::{Arc, Mutex};
    use tokio::sync::broadcast;

    fn completed_shutdown_handle() -> ShutdownHandle {
        let (tx, _rx) = broadcast::channel(1);
        ShutdownHandle::new(tx, tokio::spawn(async {}))
    }

    fn observed_shutdown_handle(label: &'static str, events: Arc<Mutex<Vec<&'static str>>>) -> ShutdownHandle {
        let (tx, mut rx) = broadcast::channel(1);
        let task_handle = tokio::spawn(async move {
            let _ = rx.recv().await;
            events.lock().unwrap_or_else(|err| err.into_inner()).push(label);
        });
        ShutdownHandle::new(tx, task_handle)
    }

    #[tokio::test]
    async fn optional_runtime_shutdown_plan_preserves_protocol_order() {
        let protocols = ProtocolShutdownSenders {
            ftp: Some(completed_shutdown_handle()),
            ftps: Some(completed_shutdown_handle()),
            webdav: Some(completed_shutdown_handle()),
            sftp: Some(completed_shutdown_handle()),
        };

        assert_eq!(
            optional_runtime_shutdown_steps(&protocols),
            vec![
                OptionalRuntimeShutdownStep::Ftp,
                OptionalRuntimeShutdownStep::Ftps,
                OptionalRuntimeShutdownStep::Webdav,
                OptionalRuntimeShutdownStep::Sftp,
            ]
        );
    }

    #[tokio::test]
    async fn optional_runtime_shutdown_signals_registered_protocols() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let protocols = ProtocolShutdownSenders {
            ftp: Some(observed_shutdown_handle("ftp", events.clone())),
            ftps: None,
            webdav: Some(observed_shutdown_handle("webdav", events.clone())),
            sftp: None,
        };

        let protocol_shutdowns = prepare_optional_runtime_shutdowns(OptionalRuntimeServices::new(protocols));
        tokio::task::yield_now().await;
        assert!(events.lock().unwrap_or_else(|err| err.into_inner()).is_empty());

        shutdown_optional_runtime_services(protocol_shutdowns).await;

        let mut events = events.lock().unwrap_or_else(|err| err.into_inner()).clone();
        events.sort_unstable();
        assert_eq!(events, ["ftp", "webdav"]);
    }
}
