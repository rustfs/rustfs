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
pub mod registry;
pub mod rules;
mod runtime_facade;
mod runtime_view;
mod status_view;
pub mod stream;

pub use bucket_config_manager::NotifyBucketConfigManager;
pub use config_manager::{NotifyConfigManager, runtime_target_id_for_subsystem};
pub use error::{LifecycleError, NotificationError};
pub use event::{Event, EventArgs, EventArgsBuilder};
pub use event_bridge::{LiveEventHistory, NotifyEventBridge};
pub use global::{
    initialize, is_notification_system_initialized, notification_metrics_snapshot, notification_system,
    notification_target_metrics, notifier_global,
};
pub use integration::{NotificationMetricSnapshot, NotificationSystem, NotificationTargetMetricSnapshot};
pub use rules::BucketNotificationConfig;
pub use runtime_facade::NotifyRuntimeFacade;
pub use runtime_view::NotifyRuntimeView;
pub use status_view::NotifyStatusView;
