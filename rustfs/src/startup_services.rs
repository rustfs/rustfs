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
use rustfs_ecstore::endpoints::EndpointServerPools;
use rustfs_ecstore::notification_sys::new_global_notification_sys;
use std::future::Future;

pub async fn init_event_notifier_and_audit() -> AuditResult<()> {
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

pub async fn init_notification_system(endpoint_pools: EndpointServerPools) -> rustfs_ecstore::error::Result<()> {
    init_notification_system_with(|| new_global_notification_sys(endpoint_pools)).await
}

async fn init_notification_system_with<InitFn, InitFuture>(init_notification: InitFn) -> rustfs_ecstore::error::Result<()>
where
    InitFn: FnOnce() -> InitFuture,
    InitFuture: Future<Output = rustfs_ecstore::error::Result<()>>,
{
    init_notification().await
}

#[cfg(test)]
mod tests {
    use super::{init_event_notifier_and_audit_with, init_notification_system_with};
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

    #[tokio::test]
    async fn notification_system_returns_source_error() {
        let result = init_notification_system_with(|| async { Err(rustfs_ecstore::error::Error::FaultyDisk) }).await;

        assert!(result.is_err());
    }
}
