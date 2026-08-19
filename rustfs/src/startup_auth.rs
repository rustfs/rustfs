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

use rustfs_iam::{
    federation::{FederatedIdentityRegistry, FederatedIdentityService, oidc::StandardOidcAdapter},
    get_oidc, init_oidc_sys_with_extra_root_ca_provider,
    oidc::{OidcExtraRootCaMaterial, OidcExtraRootCaProvider},
};
use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    io::{Error, Result},
    sync::Arc,
};
use tracing::{error, info, warn};

const LOG_COMPONENT_MAIN: &str = "main";
const LOG_SUBSYSTEM_AUTH: &str = "auth";
const EVENT_KEYSTONE_AUTH_INITIALIZED: &str = "keystone_auth_initialized";
const EVENT_KEYSTONE_AUTH_INITIALIZATION_FAILED: &str = "keystone_auth_initialization_failed";
const EVENT_OIDC_INITIALIZATION_FAILED: &str = "oidc_initialization_failed";

pub(crate) async fn init_auth_integrations() -> Result<()> {
    let keystone_config = rustfs_keystone::KeystoneConfig::from_env().map_err(Error::other)?;
    if keystone_config.enable {
        match crate::auth_keystone::init_keystone_auth(keystone_config).await {
            Ok(_) => info!(
                event = EVENT_KEYSTONE_AUTH_INITIALIZED,
                component = LOG_COMPONENT_MAIN,
                subsystem = LOG_SUBSYSTEM_AUTH,
                "Initialized Keystone authentication"
            ),
            Err(e) => {
                error!(
                    event = EVENT_KEYSTONE_AUTH_INITIALIZATION_FAILED,
                    component = LOG_COMPONENT_MAIN,
                    subsystem = LOG_SUBSYSTEM_AUTH,
                    error = %e,
                    "Failed to initialize Keystone authentication"
                );
            }
        }
    }

    match init_oidc_sys_with_extra_root_ca_provider(oidc_extra_root_ca_provider()).await {
        Ok(()) => {
            if let Some(oidc) = get_oidc() {
                let adapter = Arc::new(StandardOidcAdapter::new(oidc));
                let registry = FederatedIdentityRegistry::new(adapter);
                let service = Arc::new(FederatedIdentityService::new(registry));
                crate::runtime_sources::publish_federated_identity_service(service);
            }
        }
        Err(e) => {
            warn!(
                event = EVENT_OIDC_INITIALIZATION_FAILED,
                component = LOG_COMPONENT_MAIN,
                subsystem = LOG_SUBSYSTEM_AUTH,
                error = %e,
                "OIDC initialization failed; continuing without OIDC providers"
            );
        }
    }

    Ok(())
}

pub(crate) fn oidc_extra_root_ca_provider() -> OidcExtraRootCaProvider {
    OidcExtraRootCaProvider::new(current_oidc_extra_root_ca_material)
}

pub(crate) async fn current_oidc_extra_root_ca_material() -> std::result::Result<OidcExtraRootCaMaterial, String> {
    let outbound_tls = crate::runtime_sources::current_outbound_tls_state().await;
    let outbound_generation = outbound_tls.as_ref().map(|state| state.generation.0).unwrap_or_default();
    let mut root_ca_pem = outbound_tls.as_ref().and_then(|state| state.root_ca_pem.clone());

    if let Some(extra_ca_pem) = crate::server::tls_material::load_configured_oidc_extra_ca_cert()
        .await
        .map_err(|err| err.to_string())?
    {
        match root_ca_pem.as_mut() {
            Some(root_ca_pem) => {
                if !root_ca_pem.is_empty() && !root_ca_pem.ends_with(b"\n") {
                    root_ca_pem.push(b'\n');
                }
                root_ca_pem.extend_from_slice(&extra_ca_pem);
            }
            None => root_ca_pem = Some(extra_ca_pem),
        }
    }

    Ok(OidcExtraRootCaMaterial {
        generation: oidc_extra_root_ca_generation(outbound_generation, root_ca_pem.as_deref()),
        root_ca_pem,
    })
}

fn oidc_extra_root_ca_generation(outbound_generation: u64, root_ca_pem: Option<&[u8]>) -> u64 {
    let mut hasher = DefaultHasher::new();
    outbound_generation.hash(&mut hasher);
    root_ca_pem.hash(&mut hasher);
    hasher.finish()
}
