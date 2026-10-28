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

use crate::admin::{
    handlers::target_descriptor::{
        AdminTargetSpec, EndpointKey, RuntimeHealthStatus, collect_instance_runtime_statuses, collect_runtime_statuses,
    },
    runtime_sources::{AppContext, current_app_context, current_object_store_handle_for_context},
    storage_api::{
        config::read_admin_config_without_migrate,
        s3::{self, S3Error, S3ErrorCode, S3Result},
    },
};
use rustfs_config::server_config::Config;
use std::{collections::HashMap, future::Future};

pub(crate) async fn load_server_config_from_store_for_context(context: Option<&AppContext>) -> S3Result<Config> {
    let Some(store) = current_object_store_handle_for_context(context) else {
        return Ok(Config::new());
    };

    read_admin_config_without_migrate(store)
        .await
        .map_err(|e| s3::error(S3ErrorCode::InternalError, format!("failed to read server config: {e}")))
}

pub(crate) async fn load_server_config_from_store() -> S3Result<Config> {
    let context = current_app_context();
    load_server_config_from_store_for_context(context.as_deref()).await
}

/// Reads one durable configuration alongside passive runtime observations.
/// Neither source waits for target initialization or lifecycle convergence.
pub(crate) async fn load_target_read_snapshot<T>(runtime: impl Future<Output = T>) -> S3Result<(Config, T)> {
    tokio::try_join!(load_server_config_from_store(), async { Ok::<_, S3Error>(runtime.await) })
}

pub(crate) async fn collect_notification_runtime_statuses() -> HashMap<EndpointKey, RuntimeHealthStatus> {
    if let Some(system) = rustfs_notify::notification_system() {
        collect_runtime_statuses(system.get_target_values().await).await
    } else {
        HashMap::new()
    }
}

pub(crate) async fn collect_notification_instance_runtime_statuses(
    specs: &[AdminTargetSpec],
    instance_id: &str,
) -> HashMap<EndpointKey, RuntimeHealthStatus> {
    if let Some(system) = rustfs_notify::notification_system() {
        collect_instance_runtime_statuses(
            system.get_target_values().await,
            specs,
            rustfs_targets::TargetDomain::Notify,
            instance_id,
        )
        .await
    } else {
        HashMap::new()
    }
}

pub(crate) async fn collect_audit_runtime_statuses() -> HashMap<EndpointKey, RuntimeHealthStatus> {
    if let Some(system) = rustfs_audit::audit_system() {
        collect_runtime_statuses(system.get_target_values().await).await
    } else {
        HashMap::new()
    }
}
