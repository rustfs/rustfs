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

//! Configuration snapshot for info command.
//!
//! This module provides a lightweight snapshot of configuration values
//! that can be accessed globally without needing the full Config struct.

use std::sync::OnceLock;

use super::Config;

/// Configuration snapshot for info command display.
/// This stores key configuration values that can be accessed without
/// needing the full Config struct.
#[derive(Clone, Debug)]
pub struct ConfigSnapshot {
    /// Server bind address
    pub address: String,
    /// Console server enabled
    pub console_enable: bool,
    /// Console server address
    pub console_address: String,
    /// Server region
    pub region: Option<String>,
    /// Access key (for display, should be masked)
    pub access_key: String,
    /// OBS endpoint
    pub obs_endpoint: String,
    /// TLS path
    pub tls_path: Option<String>,
    /// KMS enabled
    pub kms_enable: bool,
    /// KMS backend type
    pub kms_backend: String,
    /// Buffer profile
    pub buffer_profile: String,
}

impl ConfigSnapshot {
    /// Create a snapshot from Config
    pub fn from_config(config: &Config) -> Self {
        Self {
            address: config.address.clone(),
            console_enable: config.console_enable,
            console_address: config.console_address.clone(),
            region: config.region.clone(),
            access_key: config.access_key.clone(),
            obs_endpoint: config.obs_endpoint.clone(),
            tls_path: config.tls_path.clone(),
            kms_enable: config.kms_enable,
            kms_backend: config.kms_backend.clone(),
            buffer_profile: config.buffer_profile.clone(),
        }
    }

    /// Create a default snapshot from environment variables and defaults
    pub fn from_env() -> Self {
        Self {
            address: std::env::var("RUSTFS_ADDRESS").unwrap_or_else(|_| rustfs_config::DEFAULT_ADDRESS.to_string()),
            console_enable: std::env::var("RUSTFS_CONSOLE_ENABLE")
                .map(|v| v == "true")
                .unwrap_or(rustfs_config::DEFAULT_CONSOLE_ENABLE),
            console_address: std::env::var("RUSTFS_CONSOLE_ADDRESS")
                .unwrap_or_else(|_| rustfs_config::DEFAULT_CONSOLE_ADDRESS.to_string()),
            region: std::env::var("RUSTFS_REGION").ok(),
            access_key: std::env::var("RUSTFS_ACCESS_KEY")
                .or_else(|_| std::env::var("RUSTFS_ROOT_USER"))
                .unwrap_or_else(|_| rustfs_credentials::DEFAULT_ACCESS_KEY.to_string()),
            obs_endpoint: std::env::var("RUSTFS_OBS_ENDPOINT")
                .unwrap_or_else(|_| rustfs_config::DEFAULT_OBS_ENDPOINT.to_string()),
            tls_path: std::env::var("RUSTFS_TLS_PATH").ok(),
            kms_enable: std::env::var("RUSTFS_KMS_ENABLE").map(|v| v == "true").unwrap_or(false),
            kms_backend: std::env::var("RUSTFS_KMS_BACKEND").unwrap_or_else(|_| "local".to_string()),
            buffer_profile: std::env::var("RUSTFS_BUFFER_PROFILE").unwrap_or_else(|_| "GeneralPurpose".to_string()),
        }
    }
}

/// Global configuration snapshot storage
static GLOBAL_CONFIG_SNAPSHOT: OnceLock<ConfigSnapshot> = OnceLock::new();

/// Initialize the global config snapshot from a Config instance.
/// This should be called once during server startup.
pub fn init_config_snapshot(config: &Config) {
    let snapshot = ConfigSnapshot::from_config(config);
    let _ = GLOBAL_CONFIG_SNAPSHOT.set(snapshot);
}

/// Get the global config snapshot.
/// Returns None if not initialized (e.g., when running --info before server starts).
#[allow(dead_code)] // used in info command
pub fn get_config_snapshot() -> Option<&'static ConfigSnapshot> {
    GLOBAL_CONFIG_SNAPSHOT.get()
}

/// Get config snapshot, creating one from environment if not initialized.
/// This is useful for --info command which may run before server starts.
pub fn get_or_init_config_snapshot() -> &'static ConfigSnapshot {
    GLOBAL_CONFIG_SNAPSHOT.get_or_init(ConfigSnapshot::from_env)
}
