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

//! RustFS Notify - A flexible and extensible event notification system for object storage.
//!
//! This library provides a Rust implementation of a storage bucket notification system.
//! It supports sending events to various targets
//! (like Webhook and MQTT) and includes features like event persistence and retry on failure.

use std::sync::Arc;

mod bucket_config_manager;
mod config_manager;
mod error;
mod event;
mod event_bridge;
pub mod factory;
mod global;
pub mod integration;
mod notification_system_subscriber;
pub mod notifier;
mod pipeline;
pub mod registry;
mod rule_engine;
pub mod rules;
mod runtime_facade;
mod runtime_view;
mod services;
mod status_view;

use rustfs_ecstore::api::config::com::{
    read_config_without_migrate as read_notify_config_without_migrate_from_backend,
    save_server_config as save_notify_server_config_to_backend,
};
use rustfs_ecstore::api::global::resolve_object_store_handle as resolve_notify_object_store_handle_from_backend;
pub(crate) use rustfs_ecstore::api::storage::ECStore as NotifyStore;

pub use bucket_config_manager::NotifyBucketConfigManager;
pub use config_manager::{NotifyConfigManager, runtime_target_id_for_subsystem};
pub use error::{LifecycleError, NotificationError};
pub use event::{Event, EventArgs, EventArgsBuilder, NotifyObjectInfo};
pub use event_bridge::{LiveEventHistory, NotifyEventBridge};
pub use global::{
    initialize, initialize_live_events, is_notification_system_initialized, notification_metrics_snapshot, notification_system,
    notification_target_metrics, notifier_global,
};
pub use integration::{NotificationMetricSnapshot, NotificationSystem, NotificationTargetMetricSnapshot};
pub use pipeline::NotifyPipeline;
pub use rule_engine::NotifyRuleEngine;
pub use rules::BucketNotificationConfig;
pub use runtime_facade::NotifyRuntimeFacade;
pub use runtime_view::NotifyRuntimeView;
pub use services::NotifyServices;
pub use status_view::NotifyStatusView;

pub(crate) fn resolve_notify_object_store_handle() -> Option<Arc<NotifyStore>> {
    resolve_notify_object_store_handle_from_backend()
}

pub(crate) async fn read_notify_server_config_without_migrate(
    store: Arc<NotifyStore>,
) -> Result<rustfs_config::server_config::Config, String> {
    read_notify_config_without_migrate_from_backend(store)
        .await
        .map_err(|err| err.to_string())
}

pub(crate) async fn save_notify_server_config(
    store: Arc<NotifyStore>,
    config: &rustfs_config::server_config::Config,
) -> Result<(), String> {
    save_notify_server_config_to_backend(store, config)
        .await
        .map_err(|err| err.to_string())
}
