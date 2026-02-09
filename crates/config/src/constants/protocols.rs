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

/// Default FTP server bind address (non-encrypted)
pub const DEFAULT_FTP_ADDRESS: &str = "0.0.0.0:8021";

/// Default FTPS server bind address (FTP over TLS)
pub const DEFAULT_FTPS_ADDRESS: &str = "0.0.0.0:8022";

/// Default FTP passive ports range (optional)
pub const DEFAULT_FTP_PASSIVE_PORTS: Option<&str> = None;

/// Default FTPS passive ports range (optional)
pub const DEFAULT_FTPS_PASSIVE_PORTS: Option<&str> = None;

/// Default FTP external IP (auto-detected)
pub const DEFAULT_FTP_EXTERNAL_IP: Option<&str> = None;

/// Default FTPS external IP (auto-detected)
pub const DEFAULT_FTPS_EXTERNAL_IP: Option<&str> = None;

/// Environment variable names
pub const ENV_FTP_ENABLE: &str = "RUSTFS_FTP_ENABLE";
pub const ENV_FTP_ADDRESS: &str = "RUSTFS_FTP_ADDRESS";
pub const ENV_FTP_PASSIVE_PORTS: &str = "RUSTFS_FTP_PASSIVE_PORTS";
pub const ENV_FTP_EXTERNAL_IP: &str = "RUSTFS_FTP_EXTERNAL_IP";

pub const ENV_FTPS_ENABLE: &str = "RUSTFS_FTPS_ENABLE";
pub const ENV_FTPS_ADDRESS: &str = "RUSTFS_FTPS_ADDRESS";
pub const ENV_FTPS_TLS_ENABLED: &str = "RUSTFS_FTPS_TLS_ENABLED";
pub const ENV_FTPS_CERTS_DIR: &str = "RUSTFS_FTPS_CERTS_DIR";
pub const ENV_FTPS_CA_FILE: &str = "RUSTFS_FTPS_CA_FILE";
pub const ENV_FTPS_PASSIVE_PORTS: &str = "RUSTFS_FTPS_PASSIVE_PORTS";
pub const ENV_FTPS_EXTERNAL_IP: &str = "RUSTFS_FTPS_EXTERNAL_IP";
