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

pub const AUDIT_AMQP_KEYS: &[&str] = &[
    crate::ENABLE_KEY,
    crate::AMQP_URL,
    crate::AMQP_EXCHANGE,
    crate::AMQP_ROUTING_KEY,
    crate::AMQP_MANDATORY,
    crate::AMQP_PERSISTENT,
    crate::AMQP_USERNAME,
    crate::AMQP_PASSWORD,
    crate::AMQP_TLS_CA,
    crate::AMQP_TLS_CLIENT_CERT,
    crate::AMQP_TLS_CLIENT_KEY,
    crate::AMQP_QUEUE_DIR,
    crate::AMQP_QUEUE_LIMIT,
    crate::COMMENT_KEY,
];

pub const ENV_AUDIT_AMQP_ENABLE: &str = "RUSTFS_AUDIT_AMQP_ENABLE";
pub const ENV_AUDIT_AMQP_URL: &str = "RUSTFS_AUDIT_AMQP_URL";
pub const ENV_AUDIT_AMQP_EXCHANGE: &str = "RUSTFS_AUDIT_AMQP_EXCHANGE";
pub const ENV_AUDIT_AMQP_ROUTING_KEY: &str = "RUSTFS_AUDIT_AMQP_ROUTING_KEY";
pub const ENV_AUDIT_AMQP_MANDATORY: &str = "RUSTFS_AUDIT_AMQP_MANDATORY";
pub const ENV_AUDIT_AMQP_PERSISTENT: &str = "RUSTFS_AUDIT_AMQP_PERSISTENT";
pub const ENV_AUDIT_AMQP_USERNAME: &str = "RUSTFS_AUDIT_AMQP_USERNAME";
pub const ENV_AUDIT_AMQP_PASSWORD: &str = "RUSTFS_AUDIT_AMQP_PASSWORD";
pub const ENV_AUDIT_AMQP_TLS_CA: &str = "RUSTFS_AUDIT_AMQP_TLS_CA";
pub const ENV_AUDIT_AMQP_TLS_CLIENT_CERT: &str = "RUSTFS_AUDIT_AMQP_TLS_CLIENT_CERT";
pub const ENV_AUDIT_AMQP_TLS_CLIENT_KEY: &str = "RUSTFS_AUDIT_AMQP_TLS_CLIENT_KEY";
pub const ENV_AUDIT_AMQP_QUEUE_DIR: &str = "RUSTFS_AUDIT_AMQP_QUEUE_DIR";
pub const ENV_AUDIT_AMQP_QUEUE_LIMIT: &str = "RUSTFS_AUDIT_AMQP_QUEUE_LIMIT";

pub const ENV_AUDIT_AMQP_KEYS: &[&str; 13] = &[
    ENV_AUDIT_AMQP_ENABLE,
    ENV_AUDIT_AMQP_URL,
    ENV_AUDIT_AMQP_EXCHANGE,
    ENV_AUDIT_AMQP_ROUTING_KEY,
    ENV_AUDIT_AMQP_MANDATORY,
    ENV_AUDIT_AMQP_PERSISTENT,
    ENV_AUDIT_AMQP_USERNAME,
    ENV_AUDIT_AMQP_PASSWORD,
    ENV_AUDIT_AMQP_TLS_CA,
    ENV_AUDIT_AMQP_TLS_CLIENT_CERT,
    ENV_AUDIT_AMQP_TLS_CLIENT_KEY,
    ENV_AUDIT_AMQP_QUEUE_DIR,
    ENV_AUDIT_AMQP_QUEUE_LIMIT,
];
