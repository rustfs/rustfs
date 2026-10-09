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

use super::config::OidcConfigQuery;
use super::{claims, config, discovery, http};
use crate::{
    federation::{
        CoreFederatedAuthorizationMapper, FederatedAuthorization, FederatedAuthorizationRules, FederatedCodeExchange,
        FederatedIdentityProvider, FederatedIdentityService, FederatedProviderRef, FederatedProviderView,
        FederatedRedirectPolicy, FederationError, OpaqueLogoutContinuation, Result, StandardOidcAuthentication,
        VerifiedFederatedCodeExchange,
    },
    oidc::{OidcConfigSnapshot, OidcSys},
};
use std::sync::Arc;

pub struct StandardOidcAdapter {
    oidc: Arc<OidcSys>,
    authorization_rules: FederatedAuthorizationRules,
}

impl StandardOidcAdapter {
    pub fn new(oidc: Arc<OidcSys>) -> Self {
        let authorization_rules = config::authorization_rules(&oidc);
        Self {
            oidc,
            authorization_rules,
        }
    }

    pub fn authorization_rules(&self) -> FederatedAuthorizationRules {
        self.authorization_rules.clone()
    }

    pub fn into_service(self: Arc<Self>, mapper: CoreFederatedAuthorizationMapper) -> FederatedIdentityService {
        let authentication: Arc<dyn StandardOidcAuthentication> = self.clone();
        let provider: Arc<dyn FederatedIdentityProvider> = self;
        FederatedIdentityService::from_standard_oidc_parts(provider, authentication, mapper)
    }
}

impl OidcConfigQuery for StandardOidcAdapter {
    fn config_snapshot(&self) -> OidcConfigSnapshot {
        self.oidc.config_snapshot()
    }
}

#[async_trait::async_trait]
impl StandardOidcAuthentication for StandardOidcAdapter {
    async fn exchange_identity(&self, state: &str, code: &str, redirect_uri: &str) -> Result<VerifiedFederatedCodeExchange> {
        let (oidc_claims, provider_id, session, id_token) = http::exchange_code(&self.oidc, state, code, redirect_uri).await?;
        let provider = FederatedProviderRef::new(provider_id);
        Ok(VerifiedFederatedCodeExchange::new(
            claims::verified_identity(provider.clone(), oidc_claims),
            session.redirect_after,
            OpaqueLogoutContinuation::new(provider, id_token),
        ))
    }

    async fn verify_identity(&self, jwt: &str) -> Result<crate::federation::VerifiedFederatedIdentity> {
        let (oidc_claims, provider_id) = self
            .oidc
            .verify_web_identity_token(jwt)
            .await
            .map_err(FederationError::TokenVerification)?;
        Ok(claims::verified_identity(FederatedProviderRef::new(provider_id), oidc_claims))
    }

    async fn create_logout_token(&self, continuation: OpaqueLogoutContinuation) -> Result<String> {
        let (provider, id_token) = continuation.into_parts();
        http::create_logout_token(&self.oidc, provider.as_str(), &id_token).await
    }
}

#[async_trait::async_trait]
impl FederatedIdentityProvider for StandardOidcAdapter {
    fn has_providers(&self) -> bool {
        self.oidc.has_providers()
    }

    fn list_providers(&self) -> Vec<FederatedProviderView> {
        discovery::list_providers(&self.oidc)
    }

    fn list_visible_providers(&self) -> Vec<FederatedProviderView> {
        discovery::list_visible_providers(&self.oidc)
    }

    fn provider_config(&self, provider_id: &str) -> Option<&crate::oidc::OidcProviderConfig> {
        self.oidc.get_provider_config(provider_id)
    }

    fn redirect_policy(&self, provider_id: &str) -> Option<FederatedRedirectPolicy> {
        config::redirect_policy(&self.oidc, provider_id)
    }

    async fn authorize_url(&self, provider_id: &str, redirect_uri: &str, redirect_after: Option<String>) -> Result<String> {
        http::authorize_url(&self.oidc, provider_id, redirect_uri, redirect_after).await
    }

    async fn exchange_code(&self, state: &str, code: &str, redirect_uri: &str) -> Result<FederatedCodeExchange> {
        let exchange = StandardOidcAuthentication::exchange_identity(self, state, code, redirect_uri).await?;
        let (identity, redirect_after, continuation) = exchange.into_parts();
        let authorization = CoreFederatedAuthorizationMapper::new(self.authorization_rules()).map(identity);
        let (_, id_token) = continuation.into_parts();
        Ok(FederatedCodeExchange {
            authorization,
            redirect_after,
            id_token,
        })
    }

    async fn verify_web_identity_token(&self, jwt: &str) -> Result<FederatedAuthorization> {
        let identity = StandardOidcAuthentication::verify_identity(self, jwt).await?;
        Ok(CoreFederatedAuthorizationMapper::new(self.authorization_rules()).map(identity))
    }

    async fn create_logout_token(&self, provider_id: &str, id_token: &str) -> Result<String> {
        http::create_logout_token(&self.oidc, provider_id, id_token).await
    }

    async fn build_logout_url(&self, logout_token: &str, post_logout_redirect_uri: &str) -> Result<Option<String>> {
        http::build_logout_url(&self.oidc, logout_token, post_logout_redirect_uri).await
    }
}
