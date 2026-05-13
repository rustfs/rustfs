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

use rustfs_ecstore::config::Config;
use s3s::{S3Result, s3_error};
use std::sync::Arc;

pub(crate) fn get_notification_system() -> S3Result<Arc<rustfs_notify::NotificationSystem>> {
    rustfs_notify::notification_system().ok_or_else(|| s3_error!(InternalError, "notification system not initialized"))
}

pub(crate) async fn load_notification_config_snapshot() -> S3Result<(Arc<rustfs_notify::NotificationSystem>, Config)> {
    let system = get_notification_system()?;
    let config = system.config.read().await.clone();
    Ok((system, config))
}

pub(crate) async fn set_notification_target_config(
    subsystem: &str,
    target_name: &str,
    kvs: rustfs_ecstore::config::KVS,
) -> S3Result<()> {
    let system = get_notification_system()?;
    system
        .set_target_config(subsystem, target_name, kvs)
        .await
        .map_err(|e| s3_error!(InternalError, "failed to set plugin instance config: {}", e))
}

pub(crate) async fn remove_notification_target_config(subsystem: &str, target_name: &str) -> S3Result<()> {
    let system = get_notification_system()?;
    system
        .remove_target_config(subsystem, target_name)
        .await
        .map_err(|e| s3_error!(InternalError, "failed to remove plugin instance config: {}", e))
}
