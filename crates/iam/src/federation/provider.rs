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

use super::{OpaqueLogoutContinuation, Result, VerifiedFederatedIdentity};
use serde::{Deserialize, Serialize};

/// Provider data exposed to login discovery consumers.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FederatedProviderView {
    pub provider_id: String,
    pub display_name: String,
}

/// Redirect inputs needed by the browser authorization flow.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FederatedRedirectPolicy {
    pub redirect_uri: Option<String>,
    pub allow_request_origin: bool,
}

pub(crate) struct VerifiedFederatedCodeExchange {
    identity: VerifiedFederatedIdentity,
    redirect_after: Option<String>,
    logout_continuation: OpaqueLogoutContinuation,
}

impl VerifiedFederatedCodeExchange {
    pub(crate) fn new(
        identity: VerifiedFederatedIdentity,
        redirect_after: Option<String>,
        logout_continuation: OpaqueLogoutContinuation,
    ) -> Self {
        Self {
            identity,
            redirect_after,
            logout_continuation,
        }
    }

    pub(crate) fn into_parts(self) -> (VerifiedFederatedIdentity, Option<String>, OpaqueLogoutContinuation) {
        (self.identity, self.redirect_after, self.logout_continuation)
    }
}

#[async_trait::async_trait]
pub(crate) trait StandardOidcAuthentication: Send + Sync {
    async fn authorize_url(&self, provider_id: &str, redirect_uri: &str, redirect_after: Option<String>) -> Result<String>;
    async fn exchange_identity(&self, state: &str, code: &str, redirect_uri: &str) -> Result<VerifiedFederatedCodeExchange>;
    async fn verify_identity(&self, jwt: &str) -> Result<VerifiedFederatedIdentity>;
    async fn create_logout_token(&self, continuation: OpaqueLogoutContinuation) -> Result<String>;
    async fn build_logout_url(&self, logout_token: &str, post_logout_redirect_uri: &str) -> Result<Option<String>>;
}

pub(crate) trait FederatedProviderQuery: Send + Sync {
    fn has_providers(&self) -> bool;

    fn list_providers(&self) -> Vec<FederatedProviderView>;

    fn list_visible_providers(&self) -> Vec<FederatedProviderView>;

    fn redirect_policy(&self, provider_id: &str) -> Option<FederatedRedirectPolicy>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provider_view_preserves_login_json_shape() {
        let view = FederatedProviderView {
            provider_id: "default".to_string(),
            display_name: "Company SSO".to_string(),
        };

        assert_eq!(
            serde_json::to_string(&[view]).expect("provider view should serialize"),
            r#"[{"provider_id":"default","display_name":"Company SSO"}]"#
        );
    }
}
