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

use crate::app::context;
use rustfs_config::ENV_RUSTFS_BROWSER_REDIRECT_URL;
use rustfs_iam::federation::FederatedIdentityService;
use std::sync::Arc;
use tracing::warn;

const LOG_COMPONENT_MAIN: &str = "main";
const LOG_SUBSYSTEM_AUTH: &str = "auth";
const EVENT_OIDC_BROWSER_REDIRECT_FALLBACK: &str = "oidc_browser_redirect_fallback";

pub(crate) use context::{
    AppContext, NotifyInterface, ServerContextSlot, default_notify_interface as fallback_notify_interface,
    default_object_data_cache_handle as fallback_object_data_cache_handle,
    default_outbound_tls_runtime_interface as fallback_outbound_tls_runtime_interface,
    default_s3select_db_interface as fallback_s3select_db_interface,
    default_scanner_metrics_interface as fallback_scanner_metrics_interface,
    default_server_config_interface as fallback_server_config_interface,
    default_storage_class_interface as fallback_storage_class_interface, publish_server_config, publish_storage_class_config,
    resolve_action_credentials as current_action_credentials, resolve_boot_time as current_boot_time,
    resolve_bucket_metadata_handle as current_bucket_metadata_handle,
    resolve_bucket_monitor_handle as current_bucket_monitor_handle, resolve_buffer_config as current_buffer_config,
    resolve_daily_tier_stats as current_daily_tier_stats, resolve_deployment_id as current_deployment_id,
    resolve_encryption_service as current_encryption_service, resolve_endpoints_handle as current_endpoints_handle,
    resolve_federated_identity_service as current_federated_identity_service, resolve_iam_handle as current_iam_handle,
    resolve_iam_ready as current_iam_ready, resolve_internode_metrics as current_internode_metrics,
    resolve_kms_runtime_service_manager as current_kms_runtime_service_manager,
    resolve_local_node_name as current_local_node_name, resolve_lock_client as current_lock_client,
    resolve_lock_clients_handle as current_lock_clients_handle, resolve_notification_system as current_notification_system,
    resolve_notification_system_for_context as current_notification_system_for_context,
    resolve_notify_interface as current_notify_interface,
    resolve_notify_interface_for_context as current_notify_interface_for_context,
    resolve_object_data_cache_handle_for_context as current_object_data_cache_handle_for_context,
    resolve_object_store_handle as current_object_store_handle,
    resolve_object_store_handle_for_context as current_object_store_handle_for_context,
    resolve_or_init_kms_runtime_service_manager as current_or_init_kms_runtime_service_manager,
    resolve_outbound_tls_generation as current_outbound_tls_generation, resolve_outbound_tls_state as current_outbound_tls_state,
    resolve_performance_metrics as current_performance_metrics, resolve_ready_iam_handle as current_ready_iam_handle,
    resolve_region as current_region, resolve_replication_pool_handle as current_replication_pool_handle,
    resolve_replication_stats_handle as current_replication_stats_handle,
    resolve_replication_stats_handle_with as current_replication_stats_handle_for_context,
    resolve_runtime_port as current_runtime_port, resolve_s3select_db as current_s3select_db,
    resolve_scanner_metrics_report as current_scanner_metrics_report, resolve_server_config as current_server_config,
    resolve_server_config_for_context as current_server_config_for_context,
    resolve_tier_config_handle as current_tier_config_handle, resolve_token_signing_key as current_token_signing_key,
};

pub(crate) fn publish_federated_identity_service(service: Arc<FederatedIdentityService>) -> bool {
    let browser_redirect_url = rustfs_utils::get_env_opt_str(ENV_RUSTFS_BROWSER_REDIRECT_URL);
    if service.has_providers() && browser_redirect_url.as_deref().is_none_or(|value| value.trim().is_empty()) {
        warn!(
            event = EVENT_OIDC_BROWSER_REDIRECT_FALLBACK,
            component = LOG_COMPONENT_MAIN,
            subsystem = LOG_SUBSYSTEM_AUTH,
            state = "fallback_enabled",
            configuration = ENV_RUSTFS_BROWSER_REDIRECT_URL,
            fallback_source = "request_host_and_forwarded_proto",
            reason = "trusted_console_origin_not_configured",
            "OIDC browser redirect fallback enabled"
        );
    }

    context::publish_federated_identity_service(service)
}

#[cfg(test)]
pub(crate) fn set_test_outbound_tls_generation(generation: u64) {
    context::set_test_outbound_tls_generation(generation);
}

#[cfg(test)]
pub(crate) use context::install_test_app_context;
#[cfg(test)]
pub(crate) use context::publish_global_app_context as publish_test_app_context;

#[cfg(test)]
pub(crate) use context::{IamInterface, KmsInterface, NotificationSystemInterface, ServerConfigInterface, StorageClassInterface};

pub(crate) fn current_app_context() -> Option<Arc<AppContext>> {
    context::get_global_app_context()
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustfs_iam::{
        federation::{
            FederatedAuthorization, FederatedCodeExchange, FederatedIdentityProvider, FederatedIdentityRegistry,
            Result as FederationResult,
        },
        oidc::{OidcProviderConfig, OidcProviderSummary},
    };
    use std::{
        io::{self, Write},
        sync::Mutex,
    };
    use tracing_subscriber::{Registry, fmt::MakeWriter, layer::SubscriberExt};

    struct TestProvider {
        has_providers: bool,
    }

    #[async_trait::async_trait]
    impl FederatedIdentityProvider for TestProvider {
        fn has_providers(&self) -> bool {
            self.has_providers
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
        ) -> FederationResult<String> {
            unreachable!("startup publication test does not authorize")
        }

        async fn exchange_code(&self, _state: &str, _code: &str, _redirect_uri: &str) -> FederationResult<FederatedCodeExchange> {
            unreachable!("startup publication test does not exchange codes")
        }

        async fn verify_web_identity_token(&self, _jwt: &str) -> FederationResult<FederatedAuthorization> {
            unreachable!("startup publication test does not verify tokens")
        }

        async fn create_logout_token(&self, _provider_id: &str, _id_token: &str) -> FederationResult<String> {
            unreachable!("startup publication test does not create logout tokens")
        }

        async fn build_logout_url(
            &self,
            _logout_token: &str,
            _post_logout_redirect_uri: &str,
        ) -> FederationResult<Option<String>> {
            unreachable!("startup publication test does not build logout URLs")
        }
    }

    #[derive(Clone, Default)]
    struct CapturedLogs(Arc<Mutex<Vec<u8>>>);

    struct CapturedLogWriter(Arc<Mutex<Vec<u8>>>);

    impl Write for CapturedLogWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0.lock().expect("captured log lock").extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl<'writer> MakeWriter<'writer> for CapturedLogs {
        type Writer = CapturedLogWriter;

        fn make_writer(&'writer self) -> Self::Writer {
            CapturedLogWriter(self.0.clone())
        }
    }

    fn test_service(has_providers: bool) -> Arc<FederatedIdentityService> {
        let provider = Arc::new(TestProvider { has_providers });
        Arc::new(FederatedIdentityService::new(FederatedIdentityRegistry::new(provider)))
    }

    fn capture_startup_publication(has_providers: bool, browser_redirect_url: Option<&str>) -> String {
        temp_env::with_var(ENV_RUSTFS_BROWSER_REDIRECT_URL, browser_redirect_url, || {
            let logs = CapturedLogs::default();
            let captured = logs.0.clone();
            let subscriber = Registry::default().with(
                tracing_subscriber::fmt::layer()
                    .without_time()
                    .with_target(false)
                    .with_level(true)
                    .with_ansi(false)
                    .with_writer(logs),
            );

            tracing::subscriber::with_default(subscriber, || {
                assert!(publish_federated_identity_service(test_service(has_providers)));
            });

            String::from_utf8(captured.lock().expect("captured log lock").clone()).expect("captured logs must be UTF-8")
        })
    }

    #[test]
    fn oidc_startup_publication_warns_only_for_request_header_fallback() {
        let output = capture_startup_publication(true, None);

        assert!(output.contains("WARN"), "{output}");
        assert!(output.contains(EVENT_OIDC_BROWSER_REDIRECT_FALLBACK), "{output}");
        assert!(output.contains(ENV_RUSTFS_BROWSER_REDIRECT_URL), "{output}");
        assert!(output.contains("request_host_and_forwarded_proto"), "{output}");
        assert!(output.contains("trusted_console_origin_not_configured"), "{output}");
        assert!(capture_startup_publication(false, None).is_empty());
        assert!(capture_startup_publication(true, Some("https://console.example.com")).is_empty());
        assert!(!capture_startup_publication(true, Some("  ")).is_empty());
    }
}
