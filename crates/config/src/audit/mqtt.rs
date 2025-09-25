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

// MQTT Environment Variables
pub const ENV_AUDIT_MQTT_ENABLE: &str = "RUSTFS_AUDIT_MQTT_ENABLE";
pub const ENV_AUDIT_MQTT_BROKER: &str = "RUSTFS_AUDIT_MQTT_BROKER";
pub const ENV_AUDIT_MQTT_TOPIC: &str = "RUSTFS_AUDIT_MQTT_TOPIC";
pub const ENV_AUDIT_MQTT_QOS: &str = "RUSTFS_AUDIT_MQTT_QOS";
pub const ENV_AUDIT_MQTT_USERNAME: &str = "RUSTFS_AUDIT_MQTT_USERNAME";
pub const ENV_AUDIT_MQTT_PASSWORD: &str = "RUSTFS_AUDIT_MQTT_PASSWORD";
pub const ENV_AUDIT_MQTT_RECONNECT_INTERVAL: &str = "RUSTFS_AUDIT_MQTT_RECONNECT_INTERVAL";
pub const ENV_AUDIT_MQTT_KEEP_ALIVE_INTERVAL: &str = "RUSTFS_AUDIT_MQTT_KEEP_ALIVE_INTERVAL";
pub const ENV_AUDIT_MQTT_QUEUE_DIR: &str = "RUSTFS_AUDIT_MQTT_QUEUE_DIR";
pub const ENV_AUDIT_MQTT_QUEUE_LIMIT: &str = "RUSTFS_AUDIT_MQTT_QUEUE_LIMIT";

/// A list of all valid configuration keys for an MQTT target.
pub const ENV_AUDIT_MQTT_KEYS: &[&str; 10] = &[
    ENV_AUDIT_MQTT_ENABLE,
    ENV_AUDIT_MQTT_BROKER,
    ENV_AUDIT_MQTT_TOPIC,
    ENV_AUDIT_MQTT_QOS,
    ENV_AUDIT_MQTT_USERNAME,
    ENV_AUDIT_MQTT_PASSWORD,
    ENV_AUDIT_MQTT_RECONNECT_INTERVAL,
    ENV_AUDIT_MQTT_KEEP_ALIVE_INTERVAL,
    ENV_AUDIT_MQTT_QUEUE_DIR,
    ENV_AUDIT_MQTT_QUEUE_LIMIT,
];

/// A list of all valid configuration keys for an MQTT target.
pub const AUDIT_MQTT_KEYS: &[&str] = &[
    crate::ENABLE_KEY,
    crate::MQTT_BROKER,
    crate::MQTT_TOPIC,
    crate::MQTT_QOS,
    crate::MQTT_USERNAME,
    crate::MQTT_PASSWORD,
    crate::MQTT_RECONNECT_INTERVAL,
    crate::MQTT_KEEP_ALIVE_INTERVAL,
    crate::MQTT_QUEUE_DIR,
    crate::MQTT_QUEUE_LIMIT,
    crate::COMMENT_KEY,
];
