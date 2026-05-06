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

use super::Config;
use crate::config::config_struct::resolve_credential;
use rustfs_config::{
    DEFAULT_ADDRESS, DEFAULT_BUFFER_PROFILE, DEFAULT_CONSOLE_ADDRESS, DEFAULT_CONSOLE_ENABLE, DEFAULT_KMS_BACKEND,
    DEFAULT_KMS_ENABLE, DEFAULT_OBS_ENDPOINT, ENV_RUSTFS_ACCESS_KEY, ENV_RUSTFS_ACCESS_KEY_FILE, ENV_RUSTFS_ADDRESS,
    ENV_RUSTFS_BUFFER_PROFILE, ENV_RUSTFS_CONSOLE_ADDRESS, ENV_RUSTFS_CONSOLE_ENABLE, ENV_RUSTFS_KMS_BACKEND,
    ENV_RUSTFS_KMS_ENABLE, ENV_RUSTFS_OBS_ENDPOINT, ENV_RUSTFS_REGION, ENV_RUSTFS_TLS_PATH, RUSTFS_REGION,
};
use rustfs_credentials::DEFAULT_ACCESS_KEY;
use rustfs_utils::{get_env_bool, get_env_opt_str, get_env_str};
use std::sync::OnceLock;

/// Fallback snapshot used only for display when the global snapshot
/// has not yet been initialized (e.g., for the `--info` command).
/// This avoids leaking memory while still providing a `'static` reference.
static DISPLAY_CONFIG_SNAPSHOT: OnceLock<ConfigSnapshot> = OnceLock::new();

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
        let access_key = resolve_credential(
            get_env_opt_str(ENV_RUSTFS_ACCESS_KEY),
            get_env_opt_str(ENV_RUSTFS_ACCESS_KEY_FILE),
            ENV_RUSTFS_ACCESS_KEY,
            DEFAULT_ACCESS_KEY,
        )
        .unwrap_or_else(|_| DEFAULT_ACCESS_KEY.to_string());
        Self {
            address: get_env_str(ENV_RUSTFS_ADDRESS, DEFAULT_ADDRESS),
            console_enable: get_env_bool(ENV_RUSTFS_CONSOLE_ENABLE, DEFAULT_CONSOLE_ENABLE),
            console_address: get_env_str(ENV_RUSTFS_CONSOLE_ADDRESS, DEFAULT_CONSOLE_ADDRESS),
            region: Some(get_env_str(ENV_RUSTFS_REGION, RUSTFS_REGION)),
            access_key,
            obs_endpoint: get_env_str(ENV_RUSTFS_OBS_ENDPOINT, DEFAULT_OBS_ENDPOINT),
            tls_path: get_env_opt_str(ENV_RUSTFS_TLS_PATH),
            kms_enable: get_env_bool(ENV_RUSTFS_KMS_ENABLE, DEFAULT_KMS_ENABLE),
            kms_backend: get_env_str(ENV_RUSTFS_KMS_BACKEND, DEFAULT_KMS_BACKEND),
            buffer_profile: get_env_str(ENV_RUSTFS_BUFFER_PROFILE, DEFAULT_BUFFER_PROFILE),
        }
    }
}

/// Global configuration snapshot storage
static GLOBAL_CONFIG_SNAPSHOT: OnceLock<ConfigSnapshot> = OnceLock::new();

/// Initialize the global config snapshot from a Config instance.
/// This should be called once during server startup.
///
/// This is the ONLY function that can set the global snapshot.
/// Once set, it cannot be changed.
pub fn init_config_snapshot(config: &Config) {
    let snapshot = ConfigSnapshot::from_config(config);
    if GLOBAL_CONFIG_SNAPSHOT.set(snapshot).is_err() {
        // Already initialized, log a warning
        tracing::warn!("Config snapshot already initialized, ignoring re-initialization");
    }
}

/// Get the global config snapshot if initialized.
/// Returns None if not initialized (e.g., when running --info before server starts).
#[allow(dead_code)] // used in info command
pub fn get_config_snapshot() -> Option<&'static ConfigSnapshot> {
    GLOBAL_CONFIG_SNAPSHOT.get()
}

/// Check if the global config snapshot has been initialized.
#[allow(dead_code)] // may be used for debugging
pub fn is_config_snapshot_initialized() -> bool {
    GLOBAL_CONFIG_SNAPSHOT.get().is_some()
}

/// Get config snapshot for display purposes (backward-compatible wrapper).
///
/// - If the global snapshot is initialized (server has started), returns a reference to it.
/// - If not initialized (e.g., --info command before server starts), returns a temporary
///   snapshot created from environment variables WITHOUT updating the global storage.
///
/// Despite its name, this function no longer initializes the global snapshot; it simply
/// delegates to `get_config_snapshot_for_display` to ensure the global snapshot is ONLY set
/// by `init_config_snapshot` during server startup.
#[allow(dead_code)] // kept for backward compatibility
pub fn get_or_init_config_snapshot() -> &'static ConfigSnapshot {
    get_config_snapshot_for_display()
}
/// Get config snapshot for display, without modifying global state.
///
/// This function is used by the --info command to display configuration:
/// - Returns the global snapshot if initialized
/// - Otherwise creates a temporary snapshot from environment variables (does NOT store it)
///
/// Note: This returns a static reference for API compatibility. When the global snapshot
/// is not initialized, it creates a leaked Box to provide a static lifetime.
/// This is safe because it's only used for read-only display purposes.
pub fn get_config_snapshot_for_display() -> &'static ConfigSnapshot {
    if let Some(snapshot) = GLOBAL_CONFIG_SNAPSHOT.get() {
        snapshot
    } else {
        // Not initialized - create from env without storing in the global snapshot.
        // Use a dedicated OnceLock to cache a single display snapshot without leaking.
        DISPLAY_CONFIG_SNAPSHOT.get_or_init(ConfigSnapshot::from_env)
    }
}
