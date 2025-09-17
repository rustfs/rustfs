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

pub const WEBHOOK_ENDPOINT: &str = "endpoint";
pub const WEBHOOK_AUTH_TOKEN: &str = "auth_token";
pub const WEBHOOK_CLIENT_CERT: &str = "client_cert";
pub const WEBHOOK_CLIENT_KEY: &str = "client_key";
pub const WEBHOOK_BATCH_SIZE: &str = "batch_size";
pub const WEBHOOK_QUEUE_LIMIT: &str = "queue_limit";
pub const WEBHOOK_QUEUE_DIR: &str = "queue_dir";
pub const WEBHOOK_MAX_RETRY: &str = "max_retry";
pub const WEBHOOK_RETRY_INTERVAL: &str = "retry_interval";
pub const WEBHOOK_HTTP_TIMEOUT: &str = "http_timeout";

pub const MQTT_BROKER: &str = "broker";
pub const MQTT_TOPIC: &str = "topic";
pub const MQTT_QOS: &str = "qos";
pub const MQTT_USERNAME: &str = "username";
pub const MQTT_PASSWORD: &str = "password";
pub const MQTT_RECONNECT_INTERVAL: &str = "reconnect_interval";
pub const MQTT_KEEP_ALIVE_INTERVAL: &str = "keep_alive_interval";
pub const MQTT_QUEUE_DIR: &str = "queue_dir";
pub const MQTT_QUEUE_LIMIT: &str = "queue_limit";
