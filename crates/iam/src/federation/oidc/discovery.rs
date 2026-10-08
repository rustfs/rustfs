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

use crate::{federation::FederatedProviderView, oidc::OidcSys};

fn provider_view(provider: &crate::oidc::OidcProviderConfig) -> FederatedProviderView {
    FederatedProviderView {
        provider_id: provider.id.clone(),
        display_name: provider.display_name.clone(),
    }
}

pub(super) fn list_providers(oidc: &OidcSys) -> Vec<FederatedProviderView> {
    oidc.provider_configs().map(provider_view).collect()
}

pub(super) fn list_visible_providers(oidc: &OidcSys) -> Vec<FederatedProviderView> {
    oidc.provider_configs()
        .filter(|provider| !provider.hide_from_ui)
        .map(provider_view)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provider_views_preserve_names_and_visibility() {
        let mut visible = crate::oidc::test_config("default");
        visible.display_name = "Company SSO".to_string();
        let mut hidden = crate::oidc::test_config("workload");
        hidden.hide_from_ui = true;
        let oidc = crate::oidc::make_test_sys(vec![visible, hidden]);

        let all = list_providers(&oidc);
        assert_eq!(all.len(), 2);
        assert!(
            all.iter()
                .any(|provider| { provider.provider_id == "default" && provider.display_name == "Company SSO" })
        );
        assert!(all.iter().any(|provider| provider.provider_id == "workload"));

        assert_eq!(
            list_visible_providers(&oidc),
            vec![FederatedProviderView {
                provider_id: "default".to_string(),
                display_name: "Company SSO".to_string(),
            }]
        );
    }
}
