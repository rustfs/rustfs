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

// Webhook Keys
pub const WEBHOOK_ENDPOINT: &str = "endpoint";
pub const WEBHOOK_AUTH_TOKEN: &str = "auth_token";
pub const WEBHOOK_QUEUE_LIMIT: &str = "queue_limit";
pub const WEBHOOK_QUEUE_DIR: &str = "queue_dir";
pub const WEBHOOK_CLIENT_CERT: &str = "client_cert";
pub const WEBHOOK_CLIENT_KEY: &str = "client_key";

/// A list of all valid configuration keys for a webhook target.
pub const NOTIFY_WEBHOOK_KEYS: &[&str] = &[
    ENABLE_KEY, // "enable" is a common key
    WEBHOOK_ENDPOINT,
    WEBHOOK_AUTH_TOKEN,
    WEBHOOK_QUEUE_LIMIT,
    WEBHOOK_QUEUE_DIR,
    WEBHOOK_CLIENT_CERT,
    WEBHOOK_CLIENT_KEY,
    COMMENT_KEY,
];

// Webhook Environment Variables
pub const ENV_WEBHOOK_ENABLE: &str = "RUSTFS_NOTIFY_WEBHOOK_ENABLE";
pub const ENV_WEBHOOK_ENDPOINT: &str = "RUSTFS_NOTIFY_WEBHOOK_ENDPOINT";
pub const ENV_WEBHOOK_AUTH_TOKEN: &str = "RUSTFS_NOTIFY_WEBHOOK_AUTH_TOKEN";
pub const ENV_WEBHOOK_QUEUE_LIMIT: &str = "RUSTFS_NOTIFY_WEBHOOK_QUEUE_LIMIT";
pub const ENV_WEBHOOK_QUEUE_DIR: &str = "RUSTFS_NOTIFY_WEBHOOK_QUEUE_DIR";
pub const ENV_WEBHOOK_CLIENT_CERT: &str = "RUSTFS_NOTIFY_WEBHOOK_CLIENT_CERT";
pub const ENV_WEBHOOK_CLIENT_KEY: &str = "RUSTFS_NOTIFY_WEBHOOK_CLIENT_KEY";

pub const ENV_NOTIFY_WEBHOOK_KEYS: &[&str; 7] = &[
    ENV_WEBHOOK_ENABLE,
    ENV_WEBHOOK_ENDPOINT,
    ENV_WEBHOOK_AUTH_TOKEN,
    ENV_WEBHOOK_QUEUE_LIMIT,
    ENV_WEBHOOK_QUEUE_DIR,
    ENV_WEBHOOK_CLIENT_CERT,
    ENV_WEBHOOK_CLIENT_KEY,
];
