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
    CoreFederatedAuthorizationMapper, FederatedAuthorization, FederatedLoginSession, FederatedProviderQuery,
    FederatedProviderView, FederatedRedirectPolicy, FederatedSession, FederatedSessionBinding, FederatedSessionTransaction,
    FederationError, OpaqueLogoutContinuation, Result, StandardOidcAuthentication,
};
use std::sync::Arc;

const DEFAULT_OIDC_PROVIDER_ID: &str = "default";

fn sorted_provider_views(mut providers: Vec<FederatedProviderView>) -> Vec<FederatedProviderView> {
    providers.sort_by(|left, right| {
        (left.provider_id != DEFAULT_OIDC_PROVIDER_ID)
            .cmp(&(right.provider_id != DEFAULT_OIDC_PROVIDER_ID))
            .then_with(|| left.provider_id.cmp(&right.provider_id))
    });
    providers
}

pub struct FederatedIdentityService {
    provider_query: Arc<dyn FederatedProviderQuery>,
    authentication: Arc<dyn StandardOidcAuthentication>,
    mapper: CoreFederatedAuthorizationMapper,
}

struct RuntimeCodeExchange {
    authorization: FederatedAuthorization,
    redirect_after: Option<String>,
    logout_continuation: OpaqueLogoutContinuation,
}

impl FederatedIdentityService {
    async fn exchange_code(&self, state: &str, code: &str, redirect_uri: &str) -> Result<RuntimeCodeExchange> {
        let exchange = self.authentication.exchange_identity(state, code, redirect_uri).await?;
        let (identity, redirect_after, logout_continuation) = exchange.into_parts();
        Ok(RuntimeCodeExchange {
            authorization: self.mapper.map(identity),
            redirect_after,
            logout_continuation,
        })
    }

    async fn verify_identity(&self, jwt: &str) -> Result<FederatedAuthorization> {
        Ok(self.mapper.map(self.authentication.verify_identity(jwt).await?))
    }

    async fn create_logout_token(&self, continuation: OpaqueLogoutContinuation) -> Result<String> {
        self.authentication.create_logout_token(continuation).await
    }

    pub(super) fn from_standard_oidc_parts(
        provider_query: Arc<dyn FederatedProviderQuery>,
        authentication: Arc<dyn StandardOidcAuthentication>,
        mapper: CoreFederatedAuthorizationMapper,
    ) -> Self {
        Self {
            provider_query,
            authentication,
            mapper,
        }
    }

    pub fn has_providers(&self) -> bool {
        self.provider_query.has_providers()
    }

    pub fn list_providers(&self) -> Vec<FederatedProviderView> {
        let providers = self.provider_query.list_providers();
        sorted_provider_views(providers)
    }

    pub fn list_visible_providers(&self) -> Vec<FederatedProviderView> {
        let providers = self.provider_query.list_visible_providers();
        sorted_provider_views(providers)
    }

    pub fn redirect_policy(&self, provider_id: &str) -> Option<FederatedRedirectPolicy> {
        self.provider_query.redirect_policy(provider_id)
    }

    pub async fn authorize_url(&self, provider_id: &str, redirect_uri: &str, redirect_after: Option<String>) -> Result<String> {
        self.authentication
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
        let exchange = self.exchange_code(state, code, redirect_uri).await?;
        let transaction = FederatedSessionTransaction {
            authorization: exchange.authorization,
            duration_seconds,
            session_policy: None,
        };
        let credentials = binding.bind(&transaction).await?;
        let logout_token = self.create_logout_token(exchange.logout_continuation).await?;
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
        let authorization = self.verify_identity(jwt).await?;
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
        self.authentication
            .build_logout_url(logout_token, post_logout_redirect_uri)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::federation::{
        FederatedAuthorizationRule, FederatedAuthorizationRules, FederatedClaims, FederatedProviderQuery, FederatedProviderRef,
        FederatedSessionBindingError, VerifiedFederatedCodeExchange, VerifiedFederatedIdentity,
    };
    use rustfs_credentials::Credentials;
    use std::sync::{Arc, Mutex};

    #[derive(Clone, Copy, PartialEq, Eq)]
    enum ProviderFailure {
        None,
        Exchange,
        Verification,
        Logout,
    }

    struct TestProvider {
        with_policy: bool,
        with_group: bool,
        browser_provider_id: &'static str,
        web_provider_id: &'static str,
        failure: ProviderFailure,
        events: Arc<Mutex<Vec<&'static str>>>,
        authorize_calls: Mutex<Vec<(String, String, Option<String>)>>,
        logout_url_calls: Mutex<Vec<(String, String)>>,
        expected_logout: (&'static str, &'static str),
        listed_provider_ids: Vec<&'static str>,
        visible_provider_ids: Vec<&'static str>,
    }

    impl TestProvider {
        fn new(events: Arc<Mutex<Vec<&'static str>>>) -> Self {
            Self {
                with_policy: true,
                with_group: false,
                browser_provider_id: DEFAULT_OIDC_PROVIDER_ID,
                web_provider_id: DEFAULT_OIDC_PROVIDER_ID,
                failure: ProviderFailure::None,
                events,
                authorize_calls: Mutex::new(Vec::new()),
                logout_url_calls: Mutex::new(Vec::new()),
                expected_logout: (DEFAULT_OIDC_PROVIDER_ID, "id-token"),
                listed_provider_ids: Vec::new(),
                visible_provider_ids: Vec::new(),
            }
        }

        fn record(&self, event: &'static str) {
            self.events.lock().expect("event log should not be poisoned").push(event);
        }

        fn identity(&self, provider_id: &str) -> VerifiedFederatedIdentity {
            VerifiedFederatedIdentity::from_claims(
                FederatedProviderRef::new(provider_id.to_string()),
                FederatedClaims {
                    sub: "subject".to_string(),
                    email: String::new(),
                    username: "user".to_string(),
                    groups: if self.with_group {
                        vec!["developers".to_string()]
                    } else {
                        Vec::new()
                    },
                    raw: Default::default(),
                },
            )
        }
    }

    fn provider_views(provider_ids: &[&str]) -> Vec<FederatedProviderView> {
        provider_ids
            .iter()
            .map(|provider_id| FederatedProviderView {
                provider_id: (*provider_id).to_string(),
                display_name: (*provider_id).to_string(),
            })
            .collect()
    }

    impl FederatedProviderQuery for TestProvider {
        fn has_providers(&self) -> bool {
            true
        }

        fn list_providers(&self) -> Vec<FederatedProviderView> {
            provider_views(&self.listed_provider_ids)
        }

        fn list_visible_providers(&self) -> Vec<FederatedProviderView> {
            provider_views(&self.visible_provider_ids)
        }

        fn redirect_policy(&self, provider_id: &str) -> Option<FederatedRedirectPolicy> {
            (provider_id == DEFAULT_OIDC_PROVIDER_ID).then_some(FederatedRedirectPolicy {
                redirect_uri: None,
                allow_request_origin: true,
            })
        }
    }

    #[async_trait::async_trait]
    impl StandardOidcAuthentication for TestProvider {
        async fn authorize_url(&self, provider_id: &str, redirect_uri: &str, redirect_after: Option<String>) -> Result<String> {
            self.authorize_calls
                .lock()
                .expect("authorize call log should not be poisoned")
                .push((provider_id.to_string(), redirect_uri.to_string(), redirect_after));
            Ok("https://identity.example/authorize".to_string())
        }

        async fn exchange_identity(
            &self,
            _state: &str,
            _code: &str,
            _redirect_uri: &str,
        ) -> Result<VerifiedFederatedCodeExchange> {
            self.record("exchange");
            if self.failure == ProviderFailure::Exchange {
                return Err(FederationError::CodeExchange("exchange failed".to_string()));
            }
            let provider = FederatedProviderRef::new(self.browser_provider_id.to_string());
            Ok(VerifiedFederatedCodeExchange::new(
                self.identity(self.browser_provider_id),
                Some("/browser".to_string()),
                OpaqueLogoutContinuation::new(provider, "id-token".to_string()),
            ))
        }

        async fn verify_identity(&self, _jwt: &str) -> Result<VerifiedFederatedIdentity> {
            self.record("verify");
            if self.failure == ProviderFailure::Verification {
                return Err(FederationError::TokenVerification("verification failed".to_string()));
            }
            Ok(self.identity(self.web_provider_id))
        }

        async fn create_logout_token(&self, continuation: OpaqueLogoutContinuation) -> Result<String> {
            self.record("logout");
            let (provider, id_token) = continuation.into_parts();
            assert_eq!((provider.as_str(), id_token.as_str()), self.expected_logout);
            if self.failure == ProviderFailure::Logout {
                return Err(FederationError::Logout("logout failed".to_string()));
            }
            Ok("logout-token".to_string())
        }

        async fn build_logout_url(&self, logout_token: &str, post_logout_redirect_uri: &str) -> Result<Option<String>> {
            self.logout_url_calls
                .lock()
                .expect("logout URL call log should not be poisoned")
                .push((logout_token.to_string(), post_logout_redirect_uri.to_string()));
            Ok(Some("https://identity.example/logout".to_string()))
        }
    }

    fn standard_service(provider: Arc<TestProvider>) -> FederatedIdentityService {
        let mut provider_ids = vec![provider.browser_provider_id, provider.web_provider_id];
        provider_ids.sort_unstable();
        provider_ids.dedup();
        let role_policy = if provider.with_policy { "readwrite" } else { "" };
        let rules = FederatedAuthorizationRules::new(provider_ids.into_iter().map(|provider_id| {
            FederatedAuthorizationRule::new(
                provider_id.to_string(),
                "groups".to_string(),
                String::new(),
                role_policy.to_string(),
                "groups".to_string(),
                String::new(),
            )
        }));
        let authentication: Arc<dyn StandardOidcAuthentication> = provider.clone();
        let provider_query: Arc<dyn FederatedProviderQuery> = provider;
        FederatedIdentityService::from_standard_oidc_parts(
            provider_query,
            authentication,
            CoreFederatedAuthorizationMapper::new(rules),
        )
    }

    struct RecordingBinding {
        fail: bool,
        events: Arc<Mutex<Vec<&'static str>>>,
        transactions: Mutex<Vec<(String, usize, Option<String>)>>,
        mapped_policies: Mutex<Vec<Vec<String>>>,
    }

    impl RecordingBinding {
        fn new(events: Arc<Mutex<Vec<&'static str>>>) -> Self {
            Self {
                fail: false,
                events,
                transactions: Mutex::new(Vec::new()),
                mapped_policies: Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait::async_trait]
    impl FederatedSessionBinding for RecordingBinding {
        async fn bind(
            &self,
            transaction: &FederatedSessionTransaction,
        ) -> core::result::Result<Credentials, FederatedSessionBindingError> {
            self.events.lock().expect("event log should not be poisoned").push("bind");
            self.transactions.lock().expect("transactions should not be poisoned").push((
                transaction.authorization.provider_id.clone(),
                transaction.duration_seconds,
                transaction.session_policy.clone(),
            ));
            self.mapped_policies
                .lock()
                .expect("mapped policies should not be poisoned")
                .push(transaction.authorization.policies.clone());
            if self.fail {
                return Err(FederatedSessionBindingError::Internal("binding failed".to_string()));
            }
            Ok(Credentials {
                access_key: transaction.authorization.claims.session_identity(),
                ..Default::default()
            })
        }
    }

    #[test]
    fn provider_listing_puts_default_first_and_sorts_named_providers() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let mut provider = TestProvider::new(events);
        provider.listed_provider_ids = vec!["zeta", "hidden", DEFAULT_OIDC_PROVIDER_ID, "alpha"];
        provider.visible_provider_ids = vec!["zeta", DEFAULT_OIDC_PROVIDER_ID, "alpha"];
        let service = standard_service(Arc::new(provider));

        assert_eq!(
            service
                .list_providers()
                .into_iter()
                .map(|provider| provider.provider_id)
                .collect::<Vec<_>>(),
            [DEFAULT_OIDC_PROVIDER_ID, "alpha", "hidden", "zeta"]
        );
        assert_eq!(
            service
                .list_visible_providers()
                .into_iter()
                .map(|provider| provider.provider_id)
                .collect::<Vec<_>>(),
            [DEFAULT_OIDC_PROVIDER_ID, "alpha", "zeta"]
        );
        assert_eq!(
            sorted_provider_views(provider_views(&["zeta", "alpha"]))
                .into_iter()
                .map(|provider| provider.provider_id)
                .collect::<Vec<_>>(),
            ["alpha", "zeta"]
        );
    }

    #[tokio::test]
    async fn authorization_and_logout_urls_preserve_service_arguments() {
        let provider = Arc::new(TestProvider::new(Arc::new(Mutex::new(Vec::new()))));
        let service = standard_service(provider.clone());

        let authorize_url = service
            .authorize_url(
                "corp",
                "https://console.example.com/oauth/callback",
                Some("/browser/path?tab=objects".to_string()),
            )
            .await
            .expect("authorization URL should be returned");
        let logout_url = service
            .build_logout_url("opaque-logout-token", "https://console.example.com/signed-out")
            .await
            .expect("logout URL should be returned");

        assert_eq!(authorize_url, "https://identity.example/authorize");
        assert_eq!(logout_url.as_deref(), Some("https://identity.example/logout"));
        assert_eq!(
            provider
                .authorize_calls
                .lock()
                .expect("authorize call log should not be poisoned")
                .as_slice(),
            [(
                "corp".to_string(),
                "https://console.example.com/oauth/callback".to_string(),
                Some("/browser/path?tab=objects".to_string()),
            )]
        );
        assert_eq!(
            provider
                .logout_url_calls
                .lock()
                .expect("logout URL call log should not be poisoned")
                .as_slice(),
            [("opaque-logout-token".to_string(), "https://console.example.com/signed-out".to_string(),)]
        );
    }

    #[tokio::test]
    async fn callback_and_web_identity_preserve_provider_and_transaction_boundaries() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let mut provider = TestProvider::new(events.clone());
        provider.browser_provider_id = "corp";
        provider.web_provider_id = "partner";
        provider.expected_logout = ("corp", "id-token");
        let binding = Arc::new(RecordingBinding::new(events.clone()));
        let service = standard_service(Arc::new(provider));

        let login = service
            .complete_authorization_code("state", "code", "https://console.example/callback", 3600, binding.as_ref())
            .await
            .expect("callback flow should complete");
        assert_eq!(login.session.credentials.access_key, "user");
        assert_eq!(login.session.authorization.provider_id, "corp");
        assert_eq!(login.redirect_after.as_deref(), Some("/browser"));
        assert_eq!(login.logout_token, "logout-token");
        assert_eq!(
            events.lock().expect("event log should not be poisoned").as_slice(),
            ["exchange", "bind", "logout"]
        );
        events.lock().expect("event log should not be poisoned").clear();

        let web_identity = service
            .assume_role_with_web_identity("jwt", 7200, Some("session-policy".to_string()), binding.as_ref())
            .await
            .expect("web identity flow should complete");
        assert_eq!(web_identity.credentials.access_key, "user");
        assert_eq!(web_identity.authorization.provider_id, "partner");
        assert_eq!(events.lock().expect("event log should not be poisoned").as_slice(), ["verify", "bind"]);
        assert_eq!(
            binding
                .transactions
                .lock()
                .expect("transactions should not be poisoned")
                .as_slice(),
            [
                ("corp".to_string(), 3600, None),
                ("partner".to_string(), 7200, Some("session-policy".to_string())),
            ]
        );
    }

    #[tokio::test]
    async fn standard_runtime_maps_before_common_browser_and_web_identity_flows() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let mut provider = TestProvider::new(events.clone());
        provider.browser_provider_id = "corp";
        provider.web_provider_id = "partner";
        provider.expected_logout = ("corp", "id-token");
        let binding = Arc::new(RecordingBinding::new(events.clone()));
        let service = standard_service(Arc::new(provider));

        let login = service
            .complete_authorization_code("state", "code", "https://console.example/callback", 3600, binding.as_ref())
            .await
            .expect("standard callback flow should complete");
        assert_eq!(login.session.authorization.provider_id, "corp");
        assert_eq!(login.session.authorization.policies, ["readwrite"]);
        assert_eq!(
            events.lock().expect("event log should not be poisoned").as_slice(),
            ["exchange", "bind", "logout"]
        );
        events.lock().expect("event log should not be poisoned").clear();

        let web_identity = service
            .assume_role_with_web_identity("jwt", 7200, Some("session-policy".to_string()), binding.as_ref())
            .await
            .expect("standard web identity flow should complete");
        assert_eq!(web_identity.authorization.provider_id, "partner");
        assert_eq!(web_identity.authorization.policies, ["readwrite"]);
        assert_eq!(events.lock().expect("event log should not be poisoned").as_slice(), ["verify", "bind"]);
        assert_eq!(
            binding
                .mapped_policies
                .lock()
                .expect("mapped policies should not be poisoned")
                .as_slice(),
            [vec!["readwrite".to_string()], vec!["readwrite".to_string()]]
        );
    }

    #[tokio::test]
    async fn web_identity_without_policy_or_group_is_not_bound() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let mut provider = TestProvider::new(events.clone());
        provider.with_policy = false;
        let binding = Arc::new(RecordingBinding::new(events.clone()));
        let service = standard_service(Arc::new(provider));

        let error = service
            .assume_role_with_web_identity("jwt", 3600, None, binding.as_ref())
            .await
            .expect_err("authorization context is required");

        assert!(matches!(error, FederationError::NoAuthorizationContext));
        assert_eq!(events.lock().expect("event log should not be poisoned").as_slice(), ["verify"]);
    }

    #[tokio::test]
    async fn web_identity_group_only_authorization_is_bound_once() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let mut provider = TestProvider::new(events.clone());
        provider.with_policy = false;
        provider.with_group = true;
        let binding = Arc::new(RecordingBinding::new(events.clone()));
        let service = standard_service(Arc::new(provider));

        let session = service
            .assume_role_with_web_identity("jwt", 3600, None, binding.as_ref())
            .await
            .expect("a mapped group is an authorization context");

        assert_eq!(session.authorization.policies, ["developers"]);
        assert_eq!(session.authorization.groups, ["developers"]);
        assert_eq!(events.lock().expect("event log should not be poisoned").as_slice(), ["verify", "bind"]);
    }

    #[tokio::test]
    async fn callback_failures_preserve_existing_side_effect_order() {
        for (provider_failure, binding_failure, expected_events) in [
            (ProviderFailure::Exchange, false, vec!["exchange"]),
            (ProviderFailure::None, true, vec!["exchange", "bind"]),
            (ProviderFailure::Logout, false, vec!["exchange", "bind", "logout"]),
        ] {
            let events = Arc::new(Mutex::new(Vec::new()));
            let mut provider = TestProvider::new(events.clone());
            provider.failure = provider_failure;
            let mut binding = RecordingBinding::new(events.clone());
            binding.fail = binding_failure;
            let binding = Arc::new(binding);
            let service = standard_service(Arc::new(provider));

            let error = service
                .complete_authorization_code("state", "code", "https://console.example/callback", 3600, binding.as_ref())
                .await
                .expect_err("the configured failure should be returned");

            if provider_failure == ProviderFailure::Exchange {
                assert!(matches!(error, FederationError::CodeExchange(ref message) if message == "exchange failed"));
            } else if binding_failure {
                assert!(matches!(
                    error,
                    FederationError::Binding(FederatedSessionBindingError::Internal(ref message))
                        if message == "binding failed"
                ));
            } else {
                assert!(matches!(error, FederationError::Logout(ref message) if message == "logout failed"));
            }

            assert_eq!(
                events.lock().expect("event log should not be poisoned").as_slice(),
                expected_events,
                "later callback steps must not run after a failure"
            );
        }
    }

    #[tokio::test]
    async fn web_identity_failures_preserve_existing_side_effect_order() {
        for (provider_failure, binding_failure, expected_events) in [
            (ProviderFailure::Verification, false, vec!["verify"]),
            (ProviderFailure::None, true, vec!["verify", "bind"]),
        ] {
            let events = Arc::new(Mutex::new(Vec::new()));
            let mut provider = TestProvider::new(events.clone());
            provider.failure = provider_failure;
            let mut binding = RecordingBinding::new(events.clone());
            binding.fail = binding_failure;
            let binding = Arc::new(binding);
            let service = standard_service(Arc::new(provider));

            let error = service
                .assume_role_with_web_identity("jwt", 3600, None, binding.as_ref())
                .await
                .expect_err("the configured failure should be returned");

            if provider_failure == ProviderFailure::Verification {
                assert!(matches!(error, FederationError::TokenVerification(_)));
            } else {
                assert!(matches!(
                    error,
                    FederationError::Binding(FederatedSessionBindingError::Internal(ref message))
                        if message == "binding failed"
                ));
            }
            assert_eq!(
                events.lock().expect("event log should not be poisoned").as_slice(),
                expected_events,
                "later web identity steps must not run after a failure"
            );
        }
    }
}
