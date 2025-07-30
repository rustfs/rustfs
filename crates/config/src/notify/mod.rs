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

// --- Configuration Constants ---
pub const DEFAULT_TARGET: &str = "1";

pub const NOTIFY_PREFIX: &str = "notify";

pub const NOTIFY_ROUTE_PREFIX: &str = const_str::concat!(NOTIFY_PREFIX, "_");

/// Standard config keys and values.
pub const ENABLE_KEY: &str = "enable";
pub const COMMENT_KEY: &str = "comment";

/// Enable values
pub const ENABLE_ON: &str = "on";
pub const ENABLE_OFF: &str = "off";

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
