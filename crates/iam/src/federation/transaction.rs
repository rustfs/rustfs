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

use super::{
    FederatedIdentityRegistry, FederatedLoginSession, FederatedSession, FederatedSessionBinding, FederatedSessionTransaction,
    FederationError, Result,
};
use crate::oidc::{OidcProviderConfig, OidcProviderSummary};

pub struct FederatedIdentityService {
    registry: FederatedIdentityRegistry,
}

impl FederatedIdentityService {
    pub fn new(registry: FederatedIdentityRegistry) -> Self {
        Self { registry }
    }

    pub fn has_providers(&self) -> bool {
        self.registry.standard_oidc().has_providers()
    }

    pub fn list_providers(&self) -> Vec<OidcProviderSummary> {
        self.registry.standard_oidc().list_providers()
    }

    pub fn list_visible_providers(&self) -> Vec<OidcProviderSummary> {
        self.registry.standard_oidc().list_visible_providers()
    }

    pub fn get_provider_config(&self, id: &str) -> Option<&OidcProviderConfig> {
        self.registry.standard_oidc().provider_config(id)
    }

    pub async fn authorize_url(&self, provider_id: &str, redirect_uri: &str, redirect_after: Option<String>) -> Result<String> {
        self.registry
            .standard_oidc()
            .authorize_url(provider_id, redirect_uri, redirect_after)
            .await
    }

    pub async fn complete_authorization_code(
        &self,
        state: &str,
        code: &str,
        redirect_uri: &str,
        duration_seconds: usize,
        binding: &dyn FederatedSessionBinding,
    ) -> Result<FederatedLoginSession> {
        let exchange = self.registry.standard_oidc().exchange_code(state, code, redirect_uri).await?;
        let provider_id = exchange.authorization.provider_id.clone();
        let transaction = FederatedSessionTransaction {
            authorization: exchange.authorization,
            duration_seconds,
            session_policy: None,
        };
        let credentials = binding.bind(&transaction).await?;
        let logout_token = self
            .registry
            .standard_oidc()
            .create_logout_token(&provider_id, &exchange.id_token)
            .await?;

        Ok(FederatedLoginSession {
            session: FederatedSession {
                credentials,
                authorization: transaction.authorization,
            },
            redirect_after: exchange.redirect_after,
            logout_token,
        })
    }

    pub async fn assume_role_with_web_identity(
        &self,
        jwt: &str,
        duration_seconds: usize,
        session_policy: Option<String>,
        binding: &dyn FederatedSessionBinding,
    ) -> Result<FederatedSession> {
        let authorization = self.registry.standard_oidc().verify_web_identity_token(jwt).await?;
        if !authorization.has_authorization_context() {
            tracing::warn!(
                provider_id = %authorization.provider_id,
                username = %authorization.claims.username,
                sub = %authorization.claims.sub,
                policy_count = authorization.policies.len(),
                group_count = authorization.groups.len(),
                "AssumeRoleWithWebIdentity has no mapped policies or groups"
            );
            return Err(FederationError::NoAuthorizationContext);
        }
        tracing::debug!(
            provider_id = %authorization.provider_id,
            username = %authorization.claims.username,
            policy_count = authorization.policies.len(),
            group_count = authorization.groups.len(),
            policies = ?authorization.policies,
            groups = ?authorization.groups,
            "AssumeRoleWithWebIdentity mapped OIDC policies and groups"
        );

        let transaction = FederatedSessionTransaction {
            authorization,
            duration_seconds,
            session_policy,
        };
        let credentials = binding.bind(&transaction).await?;

        Ok(FederatedSession {
            credentials,
            authorization: transaction.authorization,
        })
    }

    pub async fn build_logout_url(&self, logout_token: &str, post_logout_redirect_uri: &str) -> Result<Option<String>> {
        self.registry
            .standard_oidc()
            .build_logout_url(logout_token, post_logout_redirect_uri)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::federation::{
        FederatedAuthorization, FederatedClaims, FederatedCodeExchange, FederatedIdentityProvider, FederatedSessionBindingError,
    };
    use crate::oidc::{OidcProviderConfig, OidcProviderSummary};
    use rustfs_credentials::Credentials;
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    };

    struct TestProvider {
        with_policy: bool,
        events: Arc<Mutex<Vec<&'static str>>>,
    }

    impl TestProvider {
        fn record(&self, event: &'static str) {
            self.events.lock().expect("event log should not be poisoned").push(event);
        }

        fn authorization(&self) -> FederatedAuthorization {
            FederatedAuthorization {
                provider_id: "default".to_string(),
                claims: FederatedClaims {
                    sub: "subject".to_string(),
                    email: String::new(),
                    username: "user".to_string(),
                    groups: Vec::new(),
                    raw: Default::default(),
                },
                policies: if self.with_policy {
                    vec!["readwrite".to_string()]
                } else {
                    Vec::new()
                },
                groups: Vec::new(),
                roles_claim_key: None,
                roles: Vec::new(),
            }
        }
    }

    #[async_trait::async_trait]
    impl FederatedIdentityProvider for TestProvider {
        fn has_providers(&self) -> bool {
            true
        }

        fn list_providers(&self) -> Vec<OidcProviderSummary> {
            Vec::new()
        }

        fn list_visible_providers(&self) -> Vec<OidcProviderSummary> {
            Vec::new()
        }

        fn provider_config(&self, _id: &str) -> Option<&OidcProviderConfig> {
            None
        }

        async fn authorize_url(
            &self,
            _provider_id: &str,
            _redirect_uri: &str,
            _redirect_after: Option<String>,
        ) -> Result<String> {
            Ok("https://identity.example/authorize".to_string())
        }

        async fn exchange_code(&self, _state: &str, _code: &str, _redirect_uri: &str) -> Result<FederatedCodeExchange> {
            self.record("exchange");
            Ok(FederatedCodeExchange {
                authorization: self.authorization(),
                redirect_after: Some("/browser".to_string()),
                id_token: "id-token".to_string(),
            })
        }

        async fn verify_web_identity_token(&self, _jwt: &str) -> Result<FederatedAuthorization> {
            self.record("verify");
            Ok(self.authorization())
        }

        async fn create_logout_token(&self, _provider_id: &str, _id_token: &str) -> Result<String> {
            self.record("logout");
            Ok("logout-token".to_string())
        }

        async fn build_logout_url(&self, _logout_token: &str, _post_logout_redirect_uri: &str) -> Result<Option<String>> {
            Ok(Some("https://identity.example/logout".to_string()))
        }
    }

    struct CountingBinding {
        calls: AtomicUsize,
        events: Arc<Mutex<Vec<&'static str>>>,
    }

    #[async_trait::async_trait]
    impl FederatedSessionBinding for CountingBinding {
        async fn bind(
            &self,
            transaction: &FederatedSessionTransaction,
        ) -> core::result::Result<Credentials, FederatedSessionBindingError> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            self.events.lock().expect("event log should not be poisoned").push("bind");
            Ok(Credentials {
                access_key: transaction.authorization.claims.session_identity(),
                ..Default::default()
            })
        }
    }

    #[tokio::test]
    async fn callback_and_web_identity_share_session_binding() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let provider = Arc::new(TestProvider {
            with_policy: true,
            events: events.clone(),
        });
        let binding = Arc::new(CountingBinding {
            calls: AtomicUsize::new(0),
            events: events.clone(),
        });
        let service = FederatedIdentityService::new(FederatedIdentityRegistry::new(provider));

        let login = service
            .complete_authorization_code("state", "code", "https://console.example/callback", 3600, binding.as_ref())
            .await
            .expect("callback flow should complete");
        assert_eq!(login.session.credentials.access_key, "user");
        assert_eq!(login.redirect_after.as_deref(), Some("/browser"));
        assert_eq!(login.logout_token, "logout-token");
        assert_eq!(
            events.lock().expect("event log should not be poisoned").as_slice(),
            ["exchange", "bind", "logout"]
        );
        events.lock().expect("event log should not be poisoned").clear();

        let web_identity = service
            .assume_role_with_web_identity("jwt", 3600, None, binding.as_ref())
            .await
            .expect("web identity flow should complete");
        assert_eq!(web_identity.credentials.access_key, "user");
        assert_eq!(binding.calls.load(Ordering::Relaxed), 2);
        assert_eq!(events.lock().expect("event log should not be poisoned").as_slice(), ["verify", "bind"]);
    }

    #[tokio::test]
    async fn web_identity_without_policy_or_group_is_not_bound() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let provider = Arc::new(TestProvider {
            with_policy: false,
            events: events.clone(),
        });
        let binding = Arc::new(CountingBinding {
            calls: AtomicUsize::new(0),
            events,
        });
        let service = FederatedIdentityService::new(FederatedIdentityRegistry::new(provider));

        let error = service
            .assume_role_with_web_identity("jwt", 3600, None, binding.as_ref())
            .await
            .expect_err("authorization context is required");

        assert!(matches!(error, FederationError::NoAuthorizationContext));
        assert_eq!(binding.calls.load(Ordering::Relaxed), 0);
    }
}
