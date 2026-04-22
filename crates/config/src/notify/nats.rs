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

pub const NOTIFY_NATS_KEYS: &[&str] = &[
    crate::ENABLE_KEY,
    crate::NATS_ADDRESS,
    crate::NATS_SUBJECT,
    crate::NATS_USERNAME,
    crate::NATS_PASSWORD,
    crate::NATS_TOKEN,
    crate::NATS_CREDENTIALS_FILE,
    crate::NATS_TLS_CA,
    crate::NATS_TLS_CLIENT_CERT,
    crate::NATS_TLS_CLIENT_KEY,
    crate::NATS_TLS_REQUIRED,
    crate::NATS_QUEUE_DIR,
    crate::NATS_QUEUE_LIMIT,
    crate::COMMENT_KEY,
];

pub const ENV_NOTIFY_NATS_ENABLE: &str = "RUSTFS_NOTIFY_NATS_ENABLE";
pub const ENV_NOTIFY_NATS_ADDRESS: &str = "RUSTFS_NOTIFY_NATS_ADDRESS";
pub const ENV_NOTIFY_NATS_SUBJECT: &str = "RUSTFS_NOTIFY_NATS_SUBJECT";
pub const ENV_NOTIFY_NATS_USERNAME: &str = "RUSTFS_NOTIFY_NATS_USERNAME";
pub const ENV_NOTIFY_NATS_PASSWORD: &str = "RUSTFS_NOTIFY_NATS_PASSWORD";
pub const ENV_NOTIFY_NATS_TOKEN: &str = "RUSTFS_NOTIFY_NATS_TOKEN";
pub const ENV_NOTIFY_NATS_CREDENTIALS_FILE: &str = "RUSTFS_NOTIFY_NATS_CREDENTIALS_FILE";
pub const ENV_NOTIFY_NATS_TLS_CA: &str = "RUSTFS_NOTIFY_NATS_TLS_CA";
pub const ENV_NOTIFY_NATS_TLS_CLIENT_CERT: &str = "RUSTFS_NOTIFY_NATS_TLS_CLIENT_CERT";
pub const ENV_NOTIFY_NATS_TLS_CLIENT_KEY: &str = "RUSTFS_NOTIFY_NATS_TLS_CLIENT_KEY";
pub const ENV_NOTIFY_NATS_TLS_REQUIRED: &str = "RUSTFS_NOTIFY_NATS_TLS_REQUIRED";
pub const ENV_NOTIFY_NATS_QUEUE_DIR: &str = "RUSTFS_NOTIFY_NATS_QUEUE_DIR";
pub const ENV_NOTIFY_NATS_QUEUE_LIMIT: &str = "RUSTFS_NOTIFY_NATS_QUEUE_LIMIT";

pub const ENV_NOTIFY_NATS_KEYS: &[&str; 13] = &[
    ENV_NOTIFY_NATS_ENABLE,
    ENV_NOTIFY_NATS_ADDRESS,
    ENV_NOTIFY_NATS_SUBJECT,
    ENV_NOTIFY_NATS_USERNAME,
    ENV_NOTIFY_NATS_PASSWORD,
    ENV_NOTIFY_NATS_TOKEN,
    ENV_NOTIFY_NATS_CREDENTIALS_FILE,
    ENV_NOTIFY_NATS_TLS_CA,
    ENV_NOTIFY_NATS_TLS_CLIENT_CERT,
    ENV_NOTIFY_NATS_TLS_CLIENT_KEY,
    ENV_NOTIFY_NATS_TLS_REQUIRED,
    ENV_NOTIFY_NATS_QUEUE_DIR,
    ENV_NOTIFY_NATS_QUEUE_LIMIT,
];
