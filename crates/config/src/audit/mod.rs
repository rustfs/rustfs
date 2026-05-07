//  Copyright 2024 RustFS Team
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

//! Audit configuration module
//! This module defines the configuration for audit systems, including
//! webhook and MQTT audit-related settings.

mod kafka;
mod mqtt;
mod mysql;
mod nats;
mod postgres;
mod pulsar;
mod redis;
mod webhook;

pub use kafka::*;
pub use mqtt::*;
pub use mysql::*;
pub use nats::*;
pub use postgres::*;
pub use pulsar::*;
pub use redis::*;
pub use webhook::*;

use crate::DEFAULT_DELIMITER;
// --- Audit subsystem identifiers ---
pub const AUDIT_PREFIX: &str = "audit";

pub const AUDIT_ROUTE_PREFIX: &str = const_str::concat!(AUDIT_PREFIX, DEFAULT_DELIMITER);

pub const AUDIT_WEBHOOK_SUB_SYS: &str = "audit_webhook";
pub const AUDIT_KAFKA_SUB_SYS: &str = "audit_kafka";
pub const AUDIT_MQTT_SUB_SYS: &str = "audit_mqtt";
pub const AUDIT_MYSQL_SUB_SYS: &str = "audit_mysql";
pub const AUDIT_NATS_SUB_SYS: &str = "audit_nats";
pub const AUDIT_POSTGRES_SUB_SYS: &str = "audit_postgres";
pub const AUDIT_PULSAR_SUB_SYS: &str = "audit_pulsar";
pub const AUDIT_REDIS_SUB_SYS: &str = "audit_redis";
pub const AUDIT_REDIS_DEFAULT_CHANNEL: &str = "rustfs_audit_channel";

pub const AUDIT_STORE_EXTENSION: &str = ".audit";
#[allow(dead_code)]
pub const AUDIT_SUB_SYSTEMS: &[&str] = &[
    AUDIT_KAFKA_SUB_SYS,
    AUDIT_MQTT_SUB_SYS,
    AUDIT_MYSQL_SUB_SYS,
    AUDIT_NATS_SUB_SYS,
    AUDIT_POSTGRES_SUB_SYS,
    AUDIT_PULSAR_SUB_SYS,
    AUDIT_REDIS_SUB_SYS,
    AUDIT_WEBHOOK_SUB_SYS,
];
