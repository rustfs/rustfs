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

use crate::notify::{COMMENT_KEY, ENABLE_KEY};

// MQTT Keys
pub const MQTT_BROKER: &str = "broker";
pub const MQTT_TOPIC: &str = "topic";
pub const MQTT_QOS: &str = "qos";
pub const MQTT_USERNAME: &str = "username";
pub const MQTT_PASSWORD: &str = "password";
pub const MQTT_RECONNECT_INTERVAL: &str = "reconnect_interval";
pub const MQTT_KEEP_ALIVE_INTERVAL: &str = "keep_alive_interval";
pub const MQTT_QUEUE_DIR: &str = "queue_dir";
pub const MQTT_QUEUE_LIMIT: &str = "queue_limit";

/// A list of all valid configuration keys for an MQTT target.
pub const NOTIFY_MQTT_KEYS: &[&str] = &[
    ENABLE_KEY, // "enable" is a common key
    MQTT_BROKER,
    MQTT_TOPIC,
    MQTT_QOS,
    MQTT_USERNAME,
    MQTT_PASSWORD,
    MQTT_RECONNECT_INTERVAL,
    MQTT_KEEP_ALIVE_INTERVAL,
    MQTT_QUEUE_DIR,
    MQTT_QUEUE_LIMIT,
    COMMENT_KEY,
];

// MQTT Environment Variables
pub const ENV_MQTT_ENABLE: &str = "RUSTFS_NOTIFY_MQTT_ENABLE";
pub const ENV_MQTT_BROKER: &str = "RUSTFS_NOTIFY_MQTT_BROKER";
pub const ENV_MQTT_TOPIC: &str = "RUSTFS_NOTIFY_MQTT_TOPIC";
pub const ENV_MQTT_QOS: &str = "RUSTFS_NOTIFY_MQTT_QOS";
pub const ENV_MQTT_USERNAME: &str = "RUSTFS_NOTIFY_MQTT_USERNAME";
pub const ENV_MQTT_PASSWORD: &str = "RUSTFS_NOTIFY_MQTT_PASSWORD";
pub const ENV_MQTT_RECONNECT_INTERVAL: &str = "RUSTFS_NOTIFY_MQTT_RECONNECT_INTERVAL";
pub const ENV_MQTT_KEEP_ALIVE_INTERVAL: &str = "RUSTFS_NOTIFY_MQTT_KEEP_ALIVE_INTERVAL";
pub const ENV_MQTT_QUEUE_DIR: &str = "RUSTFS_NOTIFY_MQTT_QUEUE_DIR";
pub const ENV_MQTT_QUEUE_LIMIT: &str = "RUSTFS_NOTIFY_MQTT_QUEUE_LIMIT";

pub const ENV_NOTIFY_MQTT_KEYS: &[&str; 10] = &[
    ENV_MQTT_ENABLE,
    ENV_MQTT_BROKER,
    ENV_MQTT_TOPIC,
    ENV_MQTT_QOS,
    ENV_MQTT_USERNAME,
    ENV_MQTT_PASSWORD,
    ENV_MQTT_RECONNECT_INTERVAL,
    ENV_MQTT_KEEP_ALIVE_INTERVAL,
    ENV_MQTT_QUEUE_DIR,
    ENV_MQTT_QUEUE_LIMIT,
];
