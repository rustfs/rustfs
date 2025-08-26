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

pub mod error;
pub mod event;
pub mod factory;
pub mod global;
pub mod integration;
pub mod notifier;
pub mod registry;
pub mod rules;
pub mod stream;
// Re-exports
pub use error::NotificationError;
pub use event::{Event, EventArgs};
pub use global::{initialize, is_notification_system_initialized, notification_system};
pub use integration::NotificationSystem;
pub use rules::BucketNotificationConfig;
