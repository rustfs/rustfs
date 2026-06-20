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

use crate::server::{init_event_notifier, start_audit_system};
use rustfs_audit::AuditResult;
use std::future::Future;
use tracing::{error, info};

const LOG_COMPONENT_MAIN: &str = "main";
const LOG_SUBSYSTEM_STARTUP: &str = "startup";
const EVENT_AUDIT_SYSTEM_STATE: &str = "audit_system_state";

pub(crate) async fn init_audit_runtime() {
    match init_event_notifier_and_audit().await {
        Ok(()) => info!(
            target: "rustfs::main::run",
            event = EVENT_AUDIT_SYSTEM_STATE,
            component = LOG_COMPONENT_MAIN,
            subsystem = LOG_SUBSYSTEM_STARTUP,
            state = "started",
            "Audit runtime started"
        ),
        Err(e) => error!(
            target: "rustfs::main::run",
            event = EVENT_AUDIT_SYSTEM_STATE,
            component = LOG_COMPONENT_MAIN,
            subsystem = LOG_SUBSYSTEM_STARTUP,
            state = "start_failed",
            error = %e,
            "Audit runtime failed to start"
        ),
    }
}

pub(crate) async fn init_event_notifier_and_audit() -> AuditResult<()> {
    init_event_notifier_and_audit_with(init_event_notifier, start_audit_system).await
}

async fn init_event_notifier_and_audit_with<NotifyFn, NotifyFuture, AuditFn, AuditFuture>(
    notify: NotifyFn,
    start_audit: AuditFn,
) -> AuditResult<()>
where
    NotifyFn: FnOnce() -> NotifyFuture,
    NotifyFuture: Future<Output = ()>,
    AuditFn: FnOnce() -> AuditFuture,
    AuditFuture: Future<Output = AuditResult<()>>,
{
    notify().await;
    start_audit().await
}

#[cfg(test)]
mod tests {
    use super::init_event_notifier_and_audit_with;
    use rustfs_audit::AuditError;
    use std::sync::{Arc, Mutex};

    #[tokio::test]
    async fn event_notifier_runs_before_successful_audit_start() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let notify_events = events.clone();
        let audit_events = events.clone();
        let result = init_event_notifier_and_audit_with(
            move || async move {
                notify_events.lock().unwrap_or_else(|err| err.into_inner()).push("notify");
            },
            move || async move {
                audit_events.lock().unwrap_or_else(|err| err.into_inner()).push("audit");
                Ok(())
            },
        )
        .await;

        assert!(result.is_ok());
        let events = events.lock().unwrap_or_else(|err| err.into_inner()).clone();
        assert_eq!(events, ["notify", "audit"]);
    }

    #[tokio::test]
    async fn event_notifier_runs_before_failed_audit_result() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let notify_events = events.clone();
        let audit_events = events.clone();

        let result = init_event_notifier_and_audit_with(
            move || async move {
                notify_events.lock().unwrap_or_else(|err| err.into_inner()).push("notify");
            },
            move || async move {
                audit_events.lock().unwrap_or_else(|err| err.into_inner()).push("audit");
                Err(AuditError::ConfigNotLoaded)
            },
        )
        .await;

        assert!(result.is_err());
        let events = events.lock().unwrap_or_else(|err| err.into_inner()).clone();
        assert_eq!(events, ["notify", "audit"]);
    }
}
