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

pub const AUDIT_POSTGRES_KEYS: &[&str] = &[
    crate::ENABLE_KEY,
    crate::POSTGRES_DSN_STRING,
    crate::POSTGRES_TABLE,
    crate::POSTGRES_FORMAT,
    crate::POSTGRES_TLS_REQUIRED,
    crate::POSTGRES_TLS_CA,
    crate::POSTGRES_TLS_CLIENT_CERT,
    crate::POSTGRES_TLS_CLIENT_KEY,
    crate::POSTGRES_QUEUE_DIR,
    crate::POSTGRES_QUEUE_LIMIT,
    crate::COMMENT_KEY,
];

pub const ENV_AUDIT_POSTGRES_ENABLE: &str = "RUSTFS_AUDIT_POSTGRES_ENABLE";
pub const ENV_AUDIT_POSTGRES_DSN_STRING: &str = "RUSTFS_AUDIT_POSTGRES_DSN_STRING";
pub const ENV_AUDIT_POSTGRES_TABLE: &str = "RUSTFS_AUDIT_POSTGRES_TABLE";
pub const ENV_AUDIT_POSTGRES_FORMAT: &str = "RUSTFS_AUDIT_POSTGRES_FORMAT";
pub const ENV_AUDIT_POSTGRES_TLS_REQUIRED: &str = "RUSTFS_AUDIT_POSTGRES_TLS_REQUIRED";
pub const ENV_AUDIT_POSTGRES_TLS_CA: &str = "RUSTFS_AUDIT_POSTGRES_TLS_CA";
pub const ENV_AUDIT_POSTGRES_TLS_CLIENT_CERT: &str = "RUSTFS_AUDIT_POSTGRES_TLS_CLIENT_CERT";
pub const ENV_AUDIT_POSTGRES_TLS_CLIENT_KEY: &str = "RUSTFS_AUDIT_POSTGRES_TLS_CLIENT_KEY";
pub const ENV_AUDIT_POSTGRES_QUEUE_DIR: &str = "RUSTFS_AUDIT_POSTGRES_QUEUE_DIR";
pub const ENV_AUDIT_POSTGRES_QUEUE_LIMIT: &str = "RUSTFS_AUDIT_POSTGRES_QUEUE_LIMIT";

pub const ENV_AUDIT_POSTGRES_KEYS: &[&str; 10] = &[
    ENV_AUDIT_POSTGRES_ENABLE,
    ENV_AUDIT_POSTGRES_DSN_STRING,
    ENV_AUDIT_POSTGRES_TABLE,
    ENV_AUDIT_POSTGRES_FORMAT,
    ENV_AUDIT_POSTGRES_TLS_REQUIRED,
    ENV_AUDIT_POSTGRES_TLS_CA,
    ENV_AUDIT_POSTGRES_TLS_CLIENT_CERT,
    ENV_AUDIT_POSTGRES_TLS_CLIENT_KEY,
    ENV_AUDIT_POSTGRES_QUEUE_DIR,
    ENV_AUDIT_POSTGRES_QUEUE_LIMIT,
];
