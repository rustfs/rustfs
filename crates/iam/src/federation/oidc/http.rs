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
    federation::{FederationError, Result},
    oidc::{OidcClaims, OidcSys},
    oidc_state::OidcAuthSession,
};

pub(super) async fn authorize_url(
    oidc: &OidcSys,
    provider_id: &str,
    redirect_uri: &str,
    redirect_after: Option<String>,
) -> Result<String> {
    oidc.authorize_url(provider_id, redirect_uri, redirect_after)
        .await
        .map_err(FederationError::Authorization)
}

pub(super) async fn exchange_code(
    oidc: &OidcSys,
    state: &str,
    code: &str,
    redirect_uri: &str,
) -> Result<(OidcClaims, String, OidcAuthSession, String)> {
    oidc.exchange_code(state, code, redirect_uri)
        .await
        .map_err(FederationError::CodeExchange)
}

pub(super) async fn create_logout_token(oidc: &OidcSys, provider_id: &str, id_token: &str) -> Result<String> {
    oidc.create_logout_token(provider_id, id_token)
        .await
        .map_err(FederationError::Logout)
}

pub(super) async fn build_logout_url(
    oidc: &OidcSys,
    logout_token: &str,
    post_logout_redirect_uri: &str,
) -> Result<Option<String>> {
    oidc.build_logout_url(logout_token, post_logout_redirect_uri)
        .await
        .map_err(FederationError::Logout)
}
