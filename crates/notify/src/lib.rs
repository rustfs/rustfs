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

pub mod arn;
pub mod error;
pub mod event;
pub mod factory;
pub mod global;
pub mod integration;
pub mod notifier;
pub mod registry;
pub mod rules;
pub mod store;
pub mod stream;
pub mod target;

// Re-exports
pub use error::{NotificationError, StoreError, TargetError};
pub use event::{Event, EventArgs, EventLog, EventName};
pub use global::{initialize, is_notification_system_initialized, notification_system};
pub use integration::NotificationSystem;
pub use rules::BucketNotificationConfig;
use std::io::IsTerminal;
pub use target::Target;

use tracing_subscriber::{fmt, prelude::*, util::SubscriberInitExt, EnvFilter};

/// Initialize the tracing log system
///
/// # Example
/// ```
/// rustfs_notify::init_logger(rustfs_notify::LogLevel::Info);
/// ```
pub fn init_logger(level: LogLevel) {
    let filter = EnvFilter::default().add_directive(level.into());
    tracing_subscriber::registry()
        .with(filter)
        .with(
            fmt::layer()
                .with_target(true)
                .with_target(true)
                .with_ansi(std::io::stdout().is_terminal())
                .with_thread_names(true)
                .with_thread_ids(true)
                .with_file(true)
                .with_line_number(true),
        )
        .init();
}

/// Log level definition
pub enum LogLevel {
    Debug,
    Info,
    Warn,
    Error,
}

impl From<LogLevel> for tracing_subscriber::filter::Directive {
    fn from(level: LogLevel) -> Self {
        match level {
            LogLevel::Debug => "debug".parse().unwrap(),
            LogLevel::Info => "info".parse().unwrap(),
            LogLevel::Warn => "warn".parse().unwrap(),
            LogLevel::Error => "error".parse().unwrap(),
        }
    }
}
