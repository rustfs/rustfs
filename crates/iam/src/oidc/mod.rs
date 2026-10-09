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
//! OpenID Connect configuration, provider discovery, runtime flows, and state.

mod config;
mod provider;
mod runtime;
mod state;
mod transport;

#[cfg(any(test, feature = "test-util"))]
mod test_support;

pub use config::{
    OidcConfigError, OidcConfigQuery, OidcConfigSnapshot, OidcProviderConfig, OidcProviderConfigInput, OidcProviderConfigSource,
    OidcProviderValidationInput, OidcProviderValidationResult, OidcSiteReplicationProvider, OidcSiteReplicationSnapshot,
    SourcedOidcProviderConfig, build_upsert_provider_config, build_validation_provider_config, delete_persisted_provider_config,
    load_oidc_config_snapshot, load_oidc_provider_configs_from_env, load_oidc_provider_configs_from_server_config,
    persisted_provider_secret, upsert_persisted_provider_config, validate_mutable_provider_id,
};
pub use provider::validate_oidc_provider_config_with_extra_root_ca;
pub use runtime::{OidcSys, StandardOidcAdapter};
pub use transport::{
    OidcExtraRootCaMaterial, OidcExtraRootCaProvider, OidcPluginAuthnMetricsSnapshot, oidc_plugin_authn_metrics_snapshot,
};

#[cfg(any(test, feature = "test-util"))]
pub use test_support::{make_test_sys, test_config};
