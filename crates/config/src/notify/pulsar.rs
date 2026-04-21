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

pub const NOTIFY_PULSAR_KEYS: &[&str] = &[
    crate::ENABLE_KEY,
    crate::PULSAR_BROKER,
    crate::PULSAR_TOPIC,
    crate::PULSAR_AUTH_TOKEN,
    crate::PULSAR_USERNAME,
    crate::PULSAR_PASSWORD,
    crate::PULSAR_TLS_CA,
    crate::PULSAR_TLS_ALLOW_INSECURE,
    crate::PULSAR_TLS_HOSTNAME_VERIFICATION,
    crate::PULSAR_QUEUE_DIR,
    crate::PULSAR_QUEUE_LIMIT,
    crate::COMMENT_KEY,
];

pub const ENV_NOTIFY_PULSAR_ENABLE: &str = "RUSTFS_NOTIFY_PULSAR_ENABLE";
pub const ENV_NOTIFY_PULSAR_BROKER: &str = "RUSTFS_NOTIFY_PULSAR_BROKER";
pub const ENV_NOTIFY_PULSAR_TOPIC: &str = "RUSTFS_NOTIFY_PULSAR_TOPIC";
pub const ENV_NOTIFY_PULSAR_AUTH_TOKEN: &str = "RUSTFS_NOTIFY_PULSAR_AUTH_TOKEN";
pub const ENV_NOTIFY_PULSAR_USERNAME: &str = "RUSTFS_NOTIFY_PULSAR_USERNAME";
pub const ENV_NOTIFY_PULSAR_PASSWORD: &str = "RUSTFS_NOTIFY_PULSAR_PASSWORD";
pub const ENV_NOTIFY_PULSAR_TLS_CA: &str = "RUSTFS_NOTIFY_PULSAR_TLS_CA";
pub const ENV_NOTIFY_PULSAR_TLS_ALLOW_INSECURE: &str = "RUSTFS_NOTIFY_PULSAR_TLS_ALLOW_INSECURE";
pub const ENV_NOTIFY_PULSAR_TLS_HOSTNAME_VERIFICATION: &str = "RUSTFS_NOTIFY_PULSAR_TLS_HOSTNAME_VERIFICATION";
pub const ENV_NOTIFY_PULSAR_QUEUE_DIR: &str = "RUSTFS_NOTIFY_PULSAR_QUEUE_DIR";
pub const ENV_NOTIFY_PULSAR_QUEUE_LIMIT: &str = "RUSTFS_NOTIFY_PULSAR_QUEUE_LIMIT";

pub const ENV_NOTIFY_PULSAR_KEYS: &[&str; 11] = &[
    ENV_NOTIFY_PULSAR_ENABLE,
    ENV_NOTIFY_PULSAR_BROKER,
    ENV_NOTIFY_PULSAR_TOPIC,
    ENV_NOTIFY_PULSAR_AUTH_TOKEN,
    ENV_NOTIFY_PULSAR_USERNAME,
    ENV_NOTIFY_PULSAR_PASSWORD,
    ENV_NOTIFY_PULSAR_TLS_CA,
    ENV_NOTIFY_PULSAR_TLS_ALLOW_INSECURE,
    ENV_NOTIFY_PULSAR_TLS_HOSTNAME_VERIFICATION,
    ENV_NOTIFY_PULSAR_QUEUE_DIR,
    ENV_NOTIFY_PULSAR_QUEUE_LIMIT,
];
