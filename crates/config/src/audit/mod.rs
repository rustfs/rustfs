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
//! //! This module defines the configuration for audit systems, including
//! webhook and other audit-related settings.
pub const AUDIT_WEBHOOK_SUB_SYS: &str = "audit_webhook";

pub const AUDIT_STORE_EXTENSION: &str = ".audit";

pub const WEBHOOK_ENDPOINT: &str = "endpoint";
pub const WEBHOOK_AUTH_TOKEN: &str = "auth_token";
pub const WEBHOOK_CLIENT_CERT: &str = "client_cert";
pub const WEBHOOK_CLIENT_KEY: &str = "client_key";
pub const WEBHOOK_BATCH_SIZE: &str = "batch_size";
pub const WEBHOOK_QUEUE_SIZE: &str = "queue_size";
pub const WEBHOOK_QUEUE_DIR: &str = "queue_dir";
pub const WEBHOOK_MAX_RETRY: &str = "max_retry";
pub const WEBHOOK_RETRY_INTERVAL: &str = "retry_interval";
pub const WEBHOOK_HTTP_TIMEOUT: &str = "http_timeout";
