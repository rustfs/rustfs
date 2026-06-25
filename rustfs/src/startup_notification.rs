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

use crate::init::add_bucket_notification_configuration;
use crate::storage_api::{EndpointServerPools, Result as StorageResult, new_global_notification_sys};
use std::{
    future::Future,
    io::{Error, Result},
};
use tracing::{error, warn};

const LOG_COMPONENT_EMBEDDED: &str = "embedded";
const LOG_COMPONENT_MAIN: &str = "main";
const LOG_SUBSYSTEM_EMBEDDED: &str = "embedded";
const LOG_SUBSYSTEM_STARTUP: &str = "startup";
const EVENT_EMBEDDED_OPTIONAL_SERVICE_SKIPPED: &str = "embedded_optional_service_skipped";
const EVENT_NOTIFICATION_SYSTEM_INITIALIZATION_FAILED: &str = "notification_system_initialization_failed";

pub(crate) async fn init_embedded_notification_runtime(endpoint_pools: EndpointServerPools, buckets: Vec<String>) {
    add_bucket_notification_configuration(buckets).await;

    if let Err(err) = init_notification_system(endpoint_pools).await {
        log_embedded_optional_service_skipped("notification", err);
    }
}

pub(crate) async fn init_notification_runtime(endpoint_pools: EndpointServerPools, buckets: Vec<String>) -> Result<()> {
    add_bucket_notification_configuration(buckets).await;

    init_notification_system(endpoint_pools).await.map_err(|err| {
        error!(
            event = EVENT_NOTIFICATION_SYSTEM_INITIALIZATION_FAILED,
            component = LOG_COMPONENT_MAIN,
            subsystem = LOG_SUBSYSTEM_STARTUP,
            error = ?err,
            "Failed to initialize notification system"
        );
        Error::other(err)
    })
}

pub(crate) async fn init_notification_system(endpoint_pools: EndpointServerPools) -> StorageResult<()> {
    init_notification_system_with(|| new_global_notification_sys(endpoint_pools)).await
}

async fn init_notification_system_with<InitFn, InitFuture>(init_notification: InitFn) -> StorageResult<()>
where
    InitFn: FnOnce() -> InitFuture,
    InitFuture: Future<Output = StorageResult<()>>,
{
    init_notification().await
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
    use super::init_notification_system_with;
    use crate::storage_api::Error as EcstoreError;

    #[tokio::test]
    async fn notification_system_returns_source_error() {
        let result = init_notification_system_with(|| async { Err(EcstoreError::FaultyDisk) }).await;

        assert!(result.is_err());
    }
}
