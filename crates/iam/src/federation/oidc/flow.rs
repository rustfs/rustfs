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

use super::{claims, config, discovery, http};
use crate::{
    federation::{FederatedAuthorization, FederatedCodeExchange, FederatedIdentityProvider, FederationError, Result},
    oidc::{OidcProviderConfig, OidcProviderSummary, OidcSys},
};
use std::sync::Arc;

pub struct StandardOidcAdapter {
    oidc: Arc<OidcSys>,
}

impl StandardOidcAdapter {
    pub fn new(oidc: Arc<OidcSys>) -> Self {
        Self { oidc }
    }
}

#[async_trait::async_trait]
impl FederatedIdentityProvider for StandardOidcAdapter {
    fn has_providers(&self) -> bool {
        self.oidc.has_providers()
    }

    fn list_providers(&self) -> Vec<OidcProviderSummary> {
        discovery::list_providers(&self.oidc)
    }

    fn list_visible_providers(&self) -> Vec<OidcProviderSummary> {
        discovery::list_visible_providers(&self.oidc)
    }

    fn provider_config(&self, id: &str) -> Option<&OidcProviderConfig> {
        config::provider_config(&self.oidc, id)
    }

    async fn authorize_url(&self, provider_id: &str, redirect_uri: &str, redirect_after: Option<String>) -> Result<String> {
        http::authorize_url(&self.oidc, provider_id, redirect_uri, redirect_after).await
    }

    async fn exchange_code(&self, state: &str, code: &str, redirect_uri: &str) -> Result<FederatedCodeExchange> {
        let (oidc_claims, provider_id, session, id_token) = http::exchange_code(&self.oidc, state, code, redirect_uri).await?;
        Ok(FederatedCodeExchange {
            authorization: claims::authorization(&self.oidc, provider_id, oidc_claims),
            redirect_after: session.redirect_after,
            id_token,
        })
    }

    async fn verify_web_identity_token(&self, jwt: &str) -> Result<FederatedAuthorization> {
        let (oidc_claims, provider_id) = self
            .oidc
            .verify_web_identity_token(jwt)
            .await
            .map_err(FederationError::TokenVerification)?;
        Ok(claims::authorization(&self.oidc, provider_id, oidc_claims))
    }

    async fn create_logout_token(&self, provider_id: &str, id_token: &str) -> Result<String> {
        http::create_logout_token(&self.oidc, provider_id, id_token).await
    }

    async fn build_logout_url(&self, logout_token: &str, post_logout_redirect_uri: &str) -> Result<Option<String>> {
        http::build_logout_url(&self.oidc, logout_token, post_logout_redirect_uri).await
    }
}
