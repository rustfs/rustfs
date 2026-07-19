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

use super::{FederatedAuthorization, FederatedCodeExchange, Result};
use crate::oidc::{OidcProviderConfig, OidcProviderSummary};

#[async_trait::async_trait]
pub trait FederatedIdentityProvider: Send + Sync {
    fn has_providers(&self) -> bool;

    fn list_providers(&self) -> Vec<OidcProviderSummary>;

    fn list_visible_providers(&self) -> Vec<OidcProviderSummary>;

    fn provider_config(&self, id: &str) -> Option<&OidcProviderConfig>;

    async fn authorize_url(&self, provider_id: &str, redirect_uri: &str, redirect_after: Option<String>) -> Result<String>;

    async fn exchange_code(&self, state: &str, code: &str, redirect_uri: &str) -> Result<FederatedCodeExchange>;

    async fn verify_web_identity_token(&self, jwt: &str) -> Result<FederatedAuthorization>;

    async fn create_logout_token(&self, provider_id: &str, id_token: &str) -> Result<String>;

    async fn build_logout_url(&self, logout_token: &str, post_logout_redirect_uri: &str) -> Result<Option<String>>;
}
