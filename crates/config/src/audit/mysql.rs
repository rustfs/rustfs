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

/// A list of all valid configuration keys for an audit MySQL target.
pub const AUDIT_MYSQL_KEYS: &[&str] = &[
    crate::ENABLE_KEY,
    crate::MYSQL_DSN_STRING,
    crate::MYSQL_TABLE,
    crate::MYSQL_FORMAT,
    crate::MYSQL_TLS_CA,
    crate::MYSQL_TLS_CLIENT_CERT,
    crate::MYSQL_TLS_CLIENT_KEY,
    crate::MYSQL_QUEUE_DIR,
    crate::MYSQL_QUEUE_LIMIT,
    crate::MYSQL_MAX_OPEN_CONNECTIONS,
    crate::COMMENT_KEY,
];

// MySQL environment variables for audit target.
pub const ENV_AUDIT_MYSQL_ENABLE: &str = "RUSTFS_AUDIT_MYSQL_ENABLE";
pub const ENV_AUDIT_MYSQL_DSN_STRING: &str = "RUSTFS_AUDIT_MYSQL_DSN_STRING";
pub const ENV_AUDIT_MYSQL_TABLE: &str = "RUSTFS_AUDIT_MYSQL_TABLE";
pub const ENV_AUDIT_MYSQL_FORMAT: &str = "RUSTFS_AUDIT_MYSQL_FORMAT";
pub const ENV_AUDIT_MYSQL_TLS_CA: &str = "RUSTFS_AUDIT_MYSQL_TLS_CA";
pub const ENV_AUDIT_MYSQL_TLS_CLIENT_CERT: &str = "RUSTFS_AUDIT_MYSQL_TLS_CLIENT_CERT";
pub const ENV_AUDIT_MYSQL_TLS_CLIENT_KEY: &str = "RUSTFS_AUDIT_MYSQL_TLS_CLIENT_KEY";
pub const ENV_AUDIT_MYSQL_QUEUE_DIR: &str = "RUSTFS_AUDIT_MYSQL_QUEUE_DIR";
pub const ENV_AUDIT_MYSQL_QUEUE_LIMIT: &str = "RUSTFS_AUDIT_MYSQL_QUEUE_LIMIT";
pub const ENV_AUDIT_MYSQL_MAX_OPEN_CONNECTIONS: &str = "RUSTFS_AUDIT_MYSQL_MAX_OPEN_CONNECTIONS";

pub const ENV_AUDIT_MYSQL_KEYS: &[&str; 10] = &[
    ENV_AUDIT_MYSQL_ENABLE,
    ENV_AUDIT_MYSQL_DSN_STRING,
    ENV_AUDIT_MYSQL_TABLE,
    ENV_AUDIT_MYSQL_FORMAT,
    ENV_AUDIT_MYSQL_TLS_CA,
    ENV_AUDIT_MYSQL_TLS_CLIENT_CERT,
    ENV_AUDIT_MYSQL_TLS_CLIENT_KEY,
    ENV_AUDIT_MYSQL_QUEUE_DIR,
    ENV_AUDIT_MYSQL_QUEUE_LIMIT,
    ENV_AUDIT_MYSQL_MAX_OPEN_CONNECTIONS,
];
