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

use rustfs_config::ENV_RUSTFS_BROWSER_REDIRECT_URL;
use rustfs_iam::{
    federation::{FederatedIdentityRegistry, FederatedIdentityService, oidc::StandardOidcAdapter},
    get_oidc, init_oidc_sys,
};
use std::{
    io::{Error, Result},
    sync::Arc,
};
use tracing::{error, info, warn};

const LOG_COMPONENT_MAIN: &str = "main";
const LOG_SUBSYSTEM_AUTH: &str = "auth";
const EVENT_KEYSTONE_AUTH_INITIALIZED: &str = "keystone_auth_initialized";
const EVENT_KEYSTONE_AUTH_INITIALIZATION_FAILED: &str = "keystone_auth_initialization_failed";
const EVENT_OIDC_INITIALIZATION_FAILED: &str = "oidc_initialization_failed";
const EVENT_OIDC_BROWSER_REDIRECT_FALLBACK: &str = "oidc_browser_redirect_fallback";

fn warn_if_oidc_browser_redirect_fallback(has_oidc_providers: bool, browser_redirect_url: Option<&str>) {
    if !has_oidc_providers || browser_redirect_url.is_some_and(|value| !value.trim().is_empty()) {
        return;
    }

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

pub(crate) async fn init_auth_integrations() -> Result<()> {
    let keystone_config = rustfs_keystone::KeystoneConfig::from_env().map_err(Error::other)?;
    if keystone_config.enable {
        match crate::auth_keystone::init_keystone_auth(keystone_config).await {
            Ok(_) => info!(
                event = EVENT_KEYSTONE_AUTH_INITIALIZED,
                component = LOG_COMPONENT_MAIN,
                subsystem = LOG_SUBSYSTEM_AUTH,
                "Initialized Keystone authentication"
            ),
            Err(e) => {
                error!(
                    event = EVENT_KEYSTONE_AUTH_INITIALIZATION_FAILED,
                    component = LOG_COMPONENT_MAIN,
                    subsystem = LOG_SUBSYSTEM_AUTH,
                    error = %e,
                    "Failed to initialize Keystone authentication"
                );
            }
        }
    }

    match init_oidc_sys().await {
        Ok(()) => {
            if let Some(oidc) = get_oidc() {
                let browser_redirect_url = rustfs_utils::get_env_opt_str(ENV_RUSTFS_BROWSER_REDIRECT_URL);
                warn_if_oidc_browser_redirect_fallback(oidc.has_providers(), browser_redirect_url.as_deref());

                let adapter = Arc::new(StandardOidcAdapter::new(oidc));
                let registry = FederatedIdentityRegistry::new(adapter);
                let service = Arc::new(FederatedIdentityService::new(registry));
                crate::runtime_sources::publish_federated_identity_service(service);
            }
        }
        Err(e) => {
            warn!(
                event = EVENT_OIDC_INITIALIZATION_FAILED,
                component = LOG_COMPONENT_MAIN,
                subsystem = LOG_SUBSYSTEM_AUTH,
                error = %e,
                "OIDC initialization failed; continuing without OIDC providers"
            );
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{self, Write};
    use std::sync::{Arc, Mutex};
    use tracing_subscriber::{Registry, fmt::MakeWriter, layer::SubscriberExt};

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

    fn capture_browser_redirect_warning(has_oidc_providers: bool, browser_redirect_url: Option<&str>) -> String {
        let logs = CapturedLogs::default();
        let captured = logs.0.clone();
        let subscriber = Registry::default().with(
            tracing_subscriber::fmt::layer()
                .without_time()
                .with_target(false)
                .with_level(false)
                .with_ansi(false)
                .with_writer(logs),
        );

        tracing::subscriber::with_default(subscriber, || {
            warn_if_oidc_browser_redirect_fallback(has_oidc_providers, browser_redirect_url);
        });

        String::from_utf8(captured.lock().expect("captured log lock").clone()).expect("captured logs must be UTF-8")
    }

    #[test]
    fn warns_when_oidc_uses_request_header_redirect_fallback() {
        let output = capture_browser_redirect_warning(true, None);

        assert!(output.contains(EVENT_OIDC_BROWSER_REDIRECT_FALLBACK), "{output}");
        assert!(output.contains(ENV_RUSTFS_BROWSER_REDIRECT_URL), "{output}");
        assert!(output.contains("request_host_and_forwarded_proto"), "{output}");
        assert!(output.contains("trusted_console_origin_not_configured"), "{output}");
    }

    #[test]
    fn skips_redirect_fallback_warning_when_not_applicable() {
        assert!(capture_browser_redirect_warning(false, None).is_empty());
        assert!(capture_browser_redirect_warning(true, Some("https://console.example.com")).is_empty());
        assert!(!capture_browser_redirect_warning(true, Some("  ")).is_empty());
    }
}
