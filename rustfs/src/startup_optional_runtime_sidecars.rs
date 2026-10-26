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
    server::{ShutdownHandle, ShutdownWaitError},
    startup_protocols::{ProtocolShutdownSenders, init_protocol_shutdown_senders},
    storage_api::startup::services::ServerContextSlot,
};
use std::{io::Result, sync::Arc};
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

pub(crate) async fn init_optional_runtime_services(server_ctx: Arc<ServerContextSlot>) -> Result<OptionalRuntimeServices> {
    let protocols = init_protocol_shutdown_senders(server_ctx).await?;
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

pub(crate) async fn shutdown_optional_runtime_services(
    protocol_shutdowns: &mut Vec<ShutdownHandle>,
    failures: &mut Vec<ShutdownWaitError>,
) {
    for handle in protocol_shutdowns.iter() {
        handle.signal();
    }
    // At most four configured fronts; keep each actual owner until its join is observed.
    while let Some(handle) = protocol_shutdowns.first_mut() {
        let result = handle.wait_checked().await;
        protocol_shutdowns.remove(0);
        if let Err(error) = result {
            failures.push(error);
        }
    }
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

        let mut protocol_shutdowns = prepare_optional_runtime_shutdowns(OptionalRuntimeServices::new(protocols));
        tokio::task::yield_now().await;
        assert!(events.lock().unwrap_or_else(|err| err.into_inner()).is_empty());

        let mut failures = Vec::new();
        shutdown_optional_runtime_services(&mut protocol_shutdowns, &mut failures).await;
        assert!(failures.is_empty(), "all configured front task joins succeeded");
        assert!(protocol_shutdowns.is_empty(), "all actual owners were joined");

        let mut events = events.lock().unwrap_or_else(|err| err.into_inner()).clone();
        events.sort_unstable();
        assert_eq!(events, ["ftp", "webdav"]);
    }
    #[tokio::test]
    async fn optional_front_join_observer_retains_errors_and_all_remaining_owners() {
        use crate::server::ShutdownWaitError;
        use std::{future::Future, task::Poll, time::Duration};
        let (tx, _rx) = broadcast::channel(1);
        let panic = tokio::spawn(async { panic!("expected optional front panic") });
        let panic_id = panic.id();
        let ready = tokio::time::timeout(Duration::from_secs(5), async {
            while !panic.is_finished() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .is_ok();
        let panic = ShutdownHandle::new(tx, panic);
        let (tx, mut rx) = broadcast::channel(1);
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        let held = ShutdownHandle::new(
            tx,
            tokio::spawn(async move {
                let _ = rx.recv().await;
                let _ = entered_tx.send(());
                let _ = release_rx.await;
                let _ = done_tx.send(());
            }),
        );
        let (tx, _rx) = broadcast::channel(1);
        let cancelled = tokio::spawn(std::future::pending::<()>());
        let cancel_id = cancelled.id();
        cancelled.abort();
        let cancelled = ShutdownHandle::new(tx, cancelled);
        let mut handles = vec![panic, held, cancelled];
        let mut failures = Vec::new();
        let mut observer = Box::pin(shutdown_optional_runtime_services(&mut handles, &mut failures));
        let pending = std::future::poll_fn(|cx| Poll::Ready(observer.as_mut().poll(cx).is_pending())).await;
        drop(observer);
        let retained = handles.len() == 2 && failures.len() == 1;
        let entered = tokio::time::timeout(Duration::from_secs(5), entered_rx)
            .await
            .is_ok_and(|r| r.is_ok());
        for handle in &handles {
            handle.signal();
        }
        let _ = release_tx.send(());
        let completed = tokio::time::timeout(Duration::from_secs(5), done_rx)
            .await
            .is_ok_and(|r| r.is_ok());
        shutdown_optional_runtime_services(&mut handles, &mut failures).await;
        assert!(ready && pending && retained && entered && completed);
        assert!(handles.is_empty());
        assert_eq!(failures.len(), 2);
        match &failures[0] {
            ShutdownWaitError::Join(error) => {
                assert!(error.is_panic());
                assert_eq!(error.id(), panic_id);
            }
            error => panic!("lost real panic: {error}"),
        }
        match &failures[1] {
            ShutdownWaitError::Join(error) => {
                assert!(error.is_cancelled());
                assert_eq!(error.id(), cancel_id);
            }
            error => panic!("lost real cancellation: {error}"),
        }
    }

    #[tokio::test]
    async fn optional_front_consumed_handle_is_not_reported_as_success() {
        let (tx, _rx) = broadcast::channel(1);
        let mut handle = ShutdownHandle::new(tx, tokio::spawn(async {}));
        handle.wait_checked().await.expect("actual initial join");
        let mut handles = vec![handle];
        let mut failures = Vec::new();
        shutdown_optional_runtime_services(&mut handles, &mut failures).await;
        assert!(handles.is_empty());
        assert!(matches!(failures.as_slice(), [crate::server::ShutdownWaitError::MissingTask]));
    }
}
