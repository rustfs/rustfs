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

use crate::server::{init_event_notifier, is_event_notifier_reconciled};
use rustfs_config::server_config::Config;
use s3s::{S3Result, s3_error};
use std::sync::Arc;

pub(crate) async fn get_notification_system() -> S3Result<Arc<rustfs_notify::NotificationSystem>> {
    if is_event_notifier_reconciled()
        && let Some(system) = rustfs_notify::notification_system()
        && system.runtime_lifecycle_is_converged()
    {
        return Ok(system);
    }

    init_event_notifier()
        .await
        .map_err(|err| s3_error!(InternalError, "failed to reconcile notification runtime: {}", err))?;
    let system =
        rustfs_notify::notification_system().ok_or_else(|| s3_error!(InternalError, "notification system not initialized"))?;
    if !system.runtime_lifecycle_is_converged() {
        return Err(s3_error!(InternalError, "latest notification lifecycle generation has not converged"));
    }
    Ok(system)
}

pub(crate) async fn load_notification_config_snapshot() -> S3Result<(Arc<rustfs_notify::NotificationSystem>, Config)> {
    let system = get_notification_system().await?;
    let config = system.config_snapshot().await;
    Ok((system, config))
}

pub(crate) async fn set_notification_target_config(
    subsystem: &str,
    target_name: &str,
    kvs: rustfs_config::server_config::KVS,
) -> S3Result<()> {
    let system = get_notification_system().await?;
    system
        .set_target_config(subsystem, target_name, kvs)
        .await
        .map_err(|e| s3_error!(InternalError, "failed to set notification target config: {}", e))
}

pub(crate) async fn remove_notification_target_config(subsystem: &str, target_name: &str) -> S3Result<()> {
    let system = get_notification_system().await?;
    system
        .remove_target_config(subsystem, target_name)
        .await
        .map_err(|e| s3_error!(InternalError, "failed to remove notification target config: {}", e))
}
