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

use crate::admin::runtime_sources::{
    current_app_context, current_object_store_handle_for_context, current_server_config_for_context,
};
use crate::admin::storage_api::config::{
    read_admin_config_without_migrate, read_admin_server_config_snapshot, save_admin_server_config_snapshot,
};
use crate::admin::storage_api::error::Error as StorageError;
use rustfs_config::server_config::Config as ServerConfig;
use rustfs_iam::oidc::{
    OidcConfigError, OidcConfigSnapshot, OidcProviderConfigInput, OidcProviderValidationInput, OidcProviderValidationResult,
    build_upsert_provider_config, build_validation_provider_config, delete_persisted_provider_config, load_oidc_config_snapshot,
    persisted_provider_secret, upsert_persisted_provider_config, validate_mutable_provider_id,
    validate_oidc_provider_config_with_extra_root_ca,
};

#[derive(Debug, thiserror::Error)]
pub(crate) enum OidcAdminConfigError {
    #[error("storage layer not initialized")]
    StorageUnavailable,
    #[error("failed to load server config: {0}")]
    Load(#[source] StorageError),
    #[error("failed to save server config: {0}")]
    Save(#[source] StorageError),
    #[error("validation failed: {0}")]
    Validation(String),
    #[error("OIDC config update task {0}")]
    Task(&'static str),
    #[error(transparent)]
    Config(#[from] OidcConfigError),
}

type Result<T> = std::result::Result<T, OidcAdminConfigError>;

pub(crate) struct OidcConfigList {
    pub snapshot: OidcConfigSnapshot,
    pub restart_required: bool,
}

pub(crate) fn ensure_mutable_provider_id(provider_id: &str) -> Result<()> {
    validate_mutable_provider_id(provider_id)?;
    Ok(())
}

fn oidc_config_store() -> Result<std::sync::Arc<crate::admin::storage_api::runtime::ECStore>> {
    let context = current_app_context();
    current_object_store_handle_for_context(context.as_deref()).ok_or(OidcAdminConfigError::StorageUnavailable)
}

async fn load_server_config_from_store() -> Result<ServerConfig> {
    let store = oidc_config_store()?;
    read_admin_config_without_migrate(store)
        .await
        .map_err(OidcAdminConfigError::Load)
}

fn restart_required_from_active_config(config: &ServerConfig, active_config: Option<&ServerConfig>) -> bool {
    load_oidc_config_snapshot(Some(config)) != load_oidc_config_snapshot(active_config)
}

pub(crate) async fn list_config() -> Result<OidcConfigList> {
    let config = load_server_config_from_store().await?;
    let context = current_app_context();
    let active_config = current_server_config_for_context(context.as_deref());
    Ok(OidcConfigList {
        snapshot: load_oidc_config_snapshot(Some(&config)),
        restart_required: restart_required_from_active_config(&config, active_config.as_ref()),
    })
}

async fn update_server_config<F>(modifier: F) -> Result<()>
where
    F: FnOnce(&mut ServerConfig) -> Result<()> + Send + 'static,
{
    let store = oidc_config_store()?;
    tokio::spawn(async move {
        let snapshot = read_admin_server_config_snapshot(store.clone())
            .await
            .map_err(OidcAdminConfigError::Load)?;
        let mut config = snapshot.config.clone();
        modifier(&mut config)?;
        save_admin_server_config_snapshot(store, &config, &snapshot)
            .await
            .map(|_| ())
            .map_err(OidcAdminConfigError::Save)
    })
    .await
    .map_err(|error| OidcAdminConfigError::Task(if error.is_cancelled() { "cancelled" } else { "panicked" }))?
}

pub(crate) async fn upsert_config(provider_id: String, input: OidcProviderConfigInput) -> Result<()> {
    ensure_mutable_provider_id(&provider_id)?;
    update_server_config(move |config| {
        let existing_secret = persisted_provider_secret(config, &provider_id);
        let provider = build_upsert_provider_config(&provider_id, input, existing_secret)?;
        upsert_persisted_provider_config(config, &provider);
        Ok(())
    })
    .await
}

pub(crate) async fn delete_config(provider_id: String) -> Result<()> {
    ensure_mutable_provider_id(&provider_id)?;
    update_server_config(move |config| {
        delete_persisted_provider_config(config, &provider_id)?;
        Ok(())
    })
    .await
}

pub(crate) async fn validate_config(input: OidcProviderValidationInput) -> Result<OidcProviderValidationResult> {
    let provider = build_validation_provider_config(input)?;
    let extra_root_ca = crate::startup_auth::current_oidc_extra_root_ca_material()
        .await
        .map_err(|error| OidcAdminConfigError::Validation(error.to_string()))?;
    validate_oidc_provider_config_with_extra_root_ca(&provider, extra_root_ca.root_ca_pem.as_deref())
        .await
        .map_err(OidcAdminConfigError::Validation)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn restart_required_detects_persisted_changes() {
        let active_config = ServerConfig::new();
        let mut persisted_config = ServerConfig::new();
        let provider = build_upsert_provider_config(
            "default",
            OidcProviderConfigInput {
                config_url: "https://example.com/.well-known/openid-configuration".to_string(),
                client_id: "console".to_string(),
                client_secret: Some("secret".to_string()),
                ..Default::default()
            },
            None,
        )
        .expect("provider config should be valid");
        upsert_persisted_provider_config(&mut persisted_config, &provider);

        assert!(restart_required_from_active_config(&persisted_config, Some(&active_config)));
        assert!(!restart_required_from_active_config(&persisted_config, Some(&persisted_config)));
    }
}
