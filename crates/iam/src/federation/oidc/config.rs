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

use crate::{
    federation::{FederatedAuthorizationRule, FederatedAuthorizationRules, FederatedRedirectPolicy},
    oidc::{OidcConfigSnapshot, OidcSys},
};

pub(crate) fn authorization_rules(oidc: &OidcSys) -> FederatedAuthorizationRules {
    FederatedAuthorizationRules::new(oidc.provider_configs().map(|config| {
        FederatedAuthorizationRule::new(
            config.id.clone(),
            config.claim_name.clone(),
            config.claim_prefix.clone(),
            config.role_policy.clone(),
            config.groups_claim.clone(),
            config.roles_claim.clone(),
        )
    }))
}

/// Read-only access to the active standard OIDC configuration.
///
/// This interface stays separate from authentication and generic provider
/// views because its snapshots include OIDC-specific fields and secrets.
pub trait OidcConfigQuery: Send + Sync {
    fn config_snapshot(&self) -> OidcConfigSnapshot;
}

pub(super) fn redirect_policy(oidc: &OidcSys, provider_id: &str) -> Option<FederatedRedirectPolicy> {
    oidc.get_provider_config(provider_id).map(|config| FederatedRedirectPolicy {
        redirect_uri: config.redirect_uri.clone(),
        allow_request_origin: config.redirect_uri_dynamic,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redirect_policy_keeps_configured_uri_and_dynamic_setting() {
        let mut config = crate::oidc::test_config("default");
        config.redirect_uri = Some("https://console.example.test/callback".to_string());
        config.redirect_uri_dynamic = false;
        let oidc = crate::oidc::make_test_sys(vec![config]);

        assert_eq!(
            redirect_policy(&oidc, "default"),
            Some(FederatedRedirectPolicy {
                redirect_uri: Some("https://console.example.test/callback".to_string()),
                allow_request_origin: false,
            })
        );
        assert_eq!(redirect_policy(&oidc, "missing"), None);
    }
}
