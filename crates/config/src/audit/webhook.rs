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

// Webhook Environment Variables
pub const ENV_AUDIT_WEBHOOK_ENABLE: &str = "RUSTFS_AUDIT_WEBHOOK_ENABLE";
pub const ENV_AUDIT_WEBHOOK_ENDPOINT: &str = "RUSTFS_AUDIT_WEBHOOK_ENDPOINT";
pub const ENV_AUDIT_WEBHOOK_AUTH_TOKEN: &str = "RUSTFS_AUDIT_WEBHOOK_AUTH_TOKEN";
pub const ENV_AUDIT_WEBHOOK_QUEUE_LIMIT: &str = "RUSTFS_AUDIT_WEBHOOK_QUEUE_LIMIT";
pub const ENV_AUDIT_WEBHOOK_QUEUE_DIR: &str = "RUSTFS_AUDIT_WEBHOOK_QUEUE_DIR";
pub const ENV_AUDIT_WEBHOOK_CLIENT_CERT: &str = "RUSTFS_AUDIT_WEBHOOK_CLIENT_CERT";
pub const ENV_AUDIT_WEBHOOK_CLIENT_KEY: &str = "RUSTFS_AUDIT_WEBHOOK_CLIENT_KEY";

/// List of all environment variable keys for a webhook target.
pub const ENV_AUDIT_WEBHOOK_KEYS: &[&str; 7] = &[
    ENV_AUDIT_WEBHOOK_ENABLE,
    ENV_AUDIT_WEBHOOK_ENDPOINT,
    ENV_AUDIT_WEBHOOK_AUTH_TOKEN,
    ENV_AUDIT_WEBHOOK_QUEUE_LIMIT,
    ENV_AUDIT_WEBHOOK_QUEUE_DIR,
    ENV_AUDIT_WEBHOOK_CLIENT_CERT,
    ENV_AUDIT_WEBHOOK_CLIENT_KEY,
];

/// A list of all valid configuration keys for a webhook target.
pub const AUDIT_WEBHOOK_KEYS: &[&str] = &[
    crate::ENABLE_KEY,
    crate::WEBHOOK_ENDPOINT,
    crate::WEBHOOK_AUTH_TOKEN,
    crate::WEBHOOK_QUEUE_LIMIT,
    crate::WEBHOOK_QUEUE_DIR,
    crate::WEBHOOK_CLIENT_CERT,
    crate::WEBHOOK_CLIENT_KEY,
    crate::COMMENT_KEY,
];
