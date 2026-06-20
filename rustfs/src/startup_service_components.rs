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
    config::Config,
    init::{init_buffer_profile_system, init_kms_system},
    server::{init_event_notifier, start_audit_system},
};
use rustfs_audit::AuditResult;
use std::future::Future;
use tracing::{error, info, warn};

const LOG_COMPONENT_MAIN: &str = "main";
const LOG_COMPONENT_EMBEDDED: &str = "embedded";
const LOG_SUBSYSTEM_STARTUP: &str = "startup";
const LOG_SUBSYSTEM_EMBEDDED: &str = "embedded";
const EVENT_AUDIT_SYSTEM_STATE: &str = "audit_system_state";
const EVENT_DEADLOCK_DETECTOR_STATE: &str = "deadlock_detector_state";
const EVENT_EMBEDDED_OPTIONAL_SERVICE_SKIPPED: &str = "embedded_optional_service_skipped";

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

pub(crate) async fn init_embedded_optional_service_runtime(config: &Config) {
    if let Err(err) = init_kms_system(config).await {
        log_embedded_optional_service_skipped("kms", err);
    }

    init_buffer_profile_system(config);

    if let Err(err) = init_event_notifier_and_audit().await {
        log_embedded_optional_service_skipped("audit", err);
    }
}

pub(crate) fn init_deadlock_detector_runtime() {
    let detector = crate::storage::deadlock_detector::get_deadlock_detector();
    if detector.is_enabled() {
        detector.start();
        info!(
            target: "rustfs::main::run",
            event = EVENT_DEADLOCK_DETECTOR_STATE,
            component = LOG_COMPONENT_MAIN,
            subsystem = LOG_SUBSYSTEM_STARTUP,
            state = "started",
            "Deadlock detector started"
        );
    } else {
        info!(
            target: "rustfs::main::run",
            event = EVENT_DEADLOCK_DETECTOR_STATE,
            component = LOG_COMPONENT_MAIN,
            subsystem = LOG_SUBSYSTEM_STARTUP,
            state = "disabled",
            "Deadlock detector disabled"
        );
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

fn log_embedded_optional_service_skipped(service: &str, err: impl std::fmt::Display) {
    warn!(
        component = LOG_COMPONENT_EMBEDDED,
        subsystem = LOG_SUBSYSTEM_EMBEDDED,
        event = EVENT_EMBEDDED_OPTIONAL_SERVICE_SKIPPED,
        service,
        error = %err,
        "Embedded optional service initialization skipped"
    );
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
