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

//! OpenStack Keystone authentication integration for RustFS

use rustfs_keystone::{KeystoneAuthProvider, KeystoneClient, KeystoneConfig, KeystoneIdentityMapper};
use std::sync::{Arc, OnceLock};
use tracing::info;

static KEYSTONE_AUTH: OnceLock<Arc<KeystoneAuthProvider>> = OnceLock::new();
static KEYSTONE_MAPPER: OnceLock<Arc<KeystoneIdentityMapper>> = OnceLock::new();
static KEYSTONE_CONFIG: OnceLock<KeystoneConfig> = OnceLock::new();

const LOG_COMPONENT_AUTH: &str = "auth";
const LOG_SUBSYSTEM_KEYSTONE: &str = "keystone";

/// Initialize Keystone authentication
pub async fn init_keystone_auth(config: KeystoneConfig) -> Result<(), Box<dyn std::error::Error>> {
    if !config.enable {
        info!(
            component = LOG_COMPONENT_AUTH,
            subsystem = LOG_SUBSYSTEM_KEYSTONE,
            event = "keystone_state",
            state = "disabled",
            "Keystone authentication state changed"
        );
        return Ok(());
    }

    info!(
        component = LOG_COMPONENT_AUTH,
        subsystem = LOG_SUBSYSTEM_KEYSTONE,
        event = "keystone_state",
        state = "initializing",
        auth_url = %config.auth_url,
        version = %config.version,
        enable_tenant_prefix = config.enable_tenant_prefix,
        enable_cache = config.enable_cache,
        "Keystone authentication state changed"
    );

    // Validate configuration
    config.validate()?;

    let version = config.get_version()?;
    let client = KeystoneClient::new(
        config.auth_url.clone(),
        version,
        config.admin_user.clone(),
        config.admin_password.clone(),
        config.admin_project.clone(),
        config.get_admin_domain(),
        config.verify_ssl,
        config.get_timeout(),
    );

    let auth_provider = KeystoneAuthProvider::new(client.clone(), config.cache_size, config.get_cache_ttl(), config.enable_cache);

    let mut mapper = KeystoneIdentityMapper::new(Arc::new(client), config.enable_tenant_prefix);

    // Add custom role mappings if configured
    if let Some(role_mappings) = &config.role_mappings {
        for mapping in role_mappings {
            mapper.add_role_mapping(mapping.keystone_role.clone(), mapping.rustfs_policy.clone());
        }
    }

    KEYSTONE_AUTH
        .set(Arc::new(auth_provider))
        .map_err(|_| "Keystone auth already initialized")?;

    KEYSTONE_MAPPER
        .set(Arc::new(mapper))
        .map_err(|_| "Keystone mapper already initialized")?;

    KEYSTONE_CONFIG
        .set(config.clone())
        .map_err(|_| "Keystone config already initialized")?;

    info!(
        component = LOG_COMPONENT_AUTH,
        subsystem = LOG_SUBSYSTEM_KEYSTONE,
        event = "keystone_state",
        state = "initialized",
        auth_url = %config.auth_url,
        version = %config.version,
        enable_tenant_prefix = config.enable_tenant_prefix,
        enable_cache = config.enable_cache,
        "Keystone authentication state changed"
    );

    Ok(())
}

/// Get Keystone auth provider
pub fn get_keystone_auth() -> Option<Arc<KeystoneAuthProvider>> {
    KEYSTONE_AUTH.get().cloned()
}

/// Check if Keystone is enabled
pub fn is_keystone_enabled() -> bool {
    KEYSTONE_CONFIG.get().map(|c| c.enable).unwrap_or(false)
}
