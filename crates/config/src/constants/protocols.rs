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

//! Protocol server configuration constants

/// Default FTPS server bind address
pub const DEFAULT_FTPS_ADDRESS: &str = "0.0.0.0:8021";

/// Default SFTP server bind address
pub const DEFAULT_SFTP_ADDRESS: &str = "0.0.0.0:8022";

/// Default FTPS passive ports range (optional)
pub const DEFAULT_FTPS_PASSIVE_PORTS: Option<&str> = None;

/// Default FTPS external IP (auto-detected)
pub const DEFAULT_FTPS_EXTERNAL_IP: Option<&str> = None;

/// Environment variable names
pub const ENV_FTPS_ENABLE: &str = "RUSTFS_FTPS_ENABLE";
pub const ENV_FTPS_ADDRESS: &str = "RUSTFS_FTPS_ADDRESS";
pub const ENV_FTPS_CERTS_FILE: &str = "RUSTFS_FTPS_CERTS_FILE";
pub const ENV_FTPS_KEY_FILE: &str = "RUSTFS_FTPS_KEY_FILE";
pub const ENV_FTPS_PASSIVE_PORTS: &str = "RUSTFS_FTPS_PASSIVE_PORTS";
pub const ENV_FTPS_EXTERNAL_IP: &str = "RUSTFS_FTPS_EXTERNAL_IP";

pub const ENV_SFTP_ENABLE: &str = "RUSTFS_SFTP_ENABLE";
pub const ENV_SFTP_ADDRESS: &str = "RUSTFS_SFTP_ADDRESS";
pub const ENV_SFTP_HOST_KEY: &str = "RUSTFS_SFTP_HOST_KEY";
pub const ENV_SFTP_AUTHORIZED_KEYS: &str = "RUSTFS_SFTP_AUTHORIZED_KEYS";
