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

mod arn;
mod mqtt;
mod store;
mod webhook;

pub use arn::*;
pub use mqtt::*;
pub use store::*;
pub use webhook::*;

use crate::DEFAULT_DELIMITER;

/// Default target identifier for notifications,
/// Used in notification system when no specific target is provided,
/// Represents the default target stream or endpoint for notifications when no specific target is provided.
pub const DEFAULT_TARGET: &str = "1";
/// Notification prefix for routing and identification,
/// Used in notification system,
/// This prefix is utilized in constructing routes and identifiers related to notifications within the system.
pub const NOTIFY_PREFIX: &str = "notify";

/// Notification route prefix combining the notification prefix and default delimiter
/// Combines the notification prefix with the default delimiter
/// Used in notification system for defining routes related to notifications.
/// Example: "notify:/"
pub const NOTIFY_ROUTE_PREFIX: &str = const_str::concat!(NOTIFY_PREFIX, DEFAULT_DELIMITER);

/// Name of the environment variable that configures target stream concurrency.
/// Controls how many target streams are processed in parallel by the notification system.
/// Defaults to [`DEFAULT_NOTIFY_TARGET_STREAM_CONCURRENCY`] if not set.
/// Example: `RUSTFS_NOTIFY_TARGET_STREAM_CONCURRENCY=20`.
pub const ENV_NOTIFY_TARGET_STREAM_CONCURRENCY: &str = "RUSTFS_NOTIFY_TARGET_STREAM_CONCURRENCY";

/// Default concurrency for target stream processing in the notification system
/// This value is used if the environment variable `RUSTFS_NOTIFY_TARGET_STREAM_CONCURRENCY` is not set.
/// It defines how many target streams can be processed in parallel by the notification system at any given time.
/// Adjust this value based on your system's capabilities and expected load.
pub const DEFAULT_NOTIFY_TARGET_STREAM_CONCURRENCY: usize = 20;

/// Name of the environment variable that configures send concurrency.
/// Controls how many send operations are processed in parallel by the notification system.
/// Defaults to [`DEFAULT_NOTIFY_SEND_CONCURRENCY`] if not set.
/// Example: `RUSTFS_NOTIFY_SEND_CONCURRENCY=64`.
pub const ENV_NOTIFY_SEND_CONCURRENCY: &str = "RUSTFS_NOTIFY_SEND_CONCURRENCY";

/// Default concurrency for send operations in the notification system
/// This value is used if the environment variable `RUSTFS_NOTIFY_SEND_CONCURRENCY` is not set.
/// It defines how many send operations can be processed in parallel by the notification system at any given time.
/// Adjust this value based on your system's capabilities and expected load.
pub const DEFAULT_NOTIFY_SEND_CONCURRENCY: usize = 64;

#[allow(dead_code)]
pub const NOTIFY_SUB_SYSTEMS: &[&str] = &[NOTIFY_MQTT_SUB_SYS, NOTIFY_WEBHOOK_SUB_SYS];

#[allow(dead_code)]
pub const NOTIFY_KAFKA_SUB_SYS: &str = "notify_kafka";
pub const NOTIFY_MQTT_SUB_SYS: &str = "notify_mqtt";
#[allow(dead_code)]
pub const NOTIFY_MY_SQL_SUB_SYS: &str = "notify_mysql";
#[allow(dead_code)]
pub const NOTIFY_NATS_SUB_SYS: &str = "notify_nats";
#[allow(dead_code)]
pub const NOTIFY_NSQ_SUB_SYS: &str = "notify_nsq";
#[allow(dead_code)]
pub const NOTIFY_ES_SUB_SYS: &str = "notify_elasticsearch";
#[allow(dead_code)]
pub const NOTIFY_AMQP_SUB_SYS: &str = "notify_amqp";
#[allow(dead_code)]
pub const NOTIFY_POSTGRES_SUB_SYS: &str = "notify_postgres";
#[allow(dead_code)]
pub const NOTIFY_REDIS_SUB_SYS: &str = "notify_redis";
pub const NOTIFY_WEBHOOK_SUB_SYS: &str = "notify_webhook";
