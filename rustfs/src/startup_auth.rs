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

use rustfs_iam::{
    build_oidc_sys_with_extra_root_ca_provider,
    federation::{CoreFederatedAuthorizationMapper, FederatedIdentityService},
    oidc::{OidcConfigQuery, OidcExtraRootCaMaterial, OidcExtraRootCaProvider, OidcSys, StandardOidcAdapter},
};
use std::{
    collections::hash_map::DefaultHasher,
    future::Future,
    hash::{Hash, Hasher},
    io::{Error, Result},
    sync::Arc,
};
use tokio_util::{sync::CancellationToken, task::AbortOnDropHandle};
use tracing::{error, info, warn};

const LOG_COMPONENT_MAIN: &str = "main";
const LOG_SUBSYSTEM_AUTH: &str = "auth";
const EVENT_KEYSTONE_AUTH_INITIALIZED: &str = "keystone_auth_initialized";
const EVENT_KEYSTONE_AUTH_INITIALIZATION_FAILED: &str = "keystone_auth_initialization_failed";
const EVENT_OIDC_INITIALIZATION_FAILED: &str = "oidc_initialization_failed";

// Config is published after the provider and mapper. All production initialization uses
// this lock, so another listener reuses the complete publication instead of replacing it.
static KEYSTONE_INITIALIZATION: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

pub(crate) async fn init_keystone_auth_once(
    config: rustfs_keystone::KeystoneConfig,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    let _initializing = KEYSTONE_INITIALIZATION.lock().await;
    if crate::auth_keystone::get_keystone_auth().is_some() && crate::auth_keystone::is_keystone_enabled() {
        config.validate()?;
        return Ok(());
    }
    crate::auth_keystone::init_keystone_auth(config).await
}

pub(crate) async fn init_auth_integrations(shutdown: CancellationToken) -> Result<Option<AbortOnDropHandle<()>>> {
    let keystone_config = rustfs_keystone::KeystoneConfig::from_env().map_err(Error::other)?;
    if keystone_config.enable {
        match init_keystone_auth_once(keystone_config).await {
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

    match build_oidc_sys_with_extra_root_ca_provider(oidc_extra_root_ca_provider()).await {
        Ok(oidc) => {
            let (service, oidc_config_query) = standard_oidc_runtime(oidc.clone());
            if crate::runtime_sources::publish_federated_identity_runtime(service, oidc_config_query) {
                return Ok(Some(spawn_oidc_discovery_recovery(
                    async move { oidc.recover_failed_providers().await },
                    shutdown,
                )));
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

    Ok(None)
}

fn spawn_oidc_discovery_recovery(
    recovery: impl Future<Output = ()> + Send + 'static,
    shutdown: CancellationToken,
) -> AbortOnDropHandle<()> {
    AbortOnDropHandle::new(tokio::spawn(async move {
        tokio::select! {
            biased;
            () = shutdown.cancelled() => {}
            () = recovery => {}
        }
    }))
}

fn standard_oidc_runtime(oidc: Arc<OidcSys>) -> (Arc<FederatedIdentityService>, Arc<dyn OidcConfigQuery>) {
    let adapter = Arc::new(StandardOidcAdapter::new(oidc));
    let oidc_config_query = adapter.clone();
    let mapper = CoreFederatedAuthorizationMapper::new(adapter.authorization_rules());
    (Arc::new(adapter.into_service(mapper)), oidc_config_query)
}

pub(crate) fn oidc_extra_root_ca_provider() -> OidcExtraRootCaProvider {
    OidcExtraRootCaProvider::new(current_oidc_extra_root_ca_material)
}

pub(crate) async fn current_oidc_extra_root_ca_material() -> std::result::Result<OidcExtraRootCaMaterial, String> {
    let outbound_tls = crate::runtime_sources::current_outbound_tls_state().await;
    let outbound_generation = outbound_tls.as_ref().map(|state| state.generation.0).unwrap_or_default();
    let mut root_ca_pem = outbound_tls.as_ref().and_then(|state| state.root_ca_pem.clone());

    if let Some(extra_ca_pem) = crate::server::tls_material::load_configured_oidc_extra_ca_cert()
        .await
        .map_err(|err| err.to_string())?
    {
        match root_ca_pem.as_mut() {
            Some(root_ca_pem) => {
                if !root_ca_pem.is_empty() && !root_ca_pem.ends_with(b"\n") {
                    root_ca_pem.push(b'\n');
                }
                root_ca_pem.extend_from_slice(&extra_ca_pem);
            }
            None => root_ca_pem = Some(extra_ca_pem),
        }
    }

    Ok(OidcExtraRootCaMaterial {
        generation: oidc_extra_root_ca_generation(outbound_generation, root_ca_pem.as_deref()),
        root_ca_pem,
    })
}

fn oidc_extra_root_ca_generation(outbound_generation: u64, root_ca_pem: Option<&[u8]>) -> u64 {
    let mut hasher = DefaultHasher::new();
    outbound_generation.hash(&mut hasher);
    root_ca_pem.hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keystone_initialization_reuses_the_provider_for_concurrent_calls() {
        const ISOLATED: &str = "RUSTFS_TEST_KEYSTONE_INITIALIZATION_ISOLATED";
        if std::env::var_os(ISOLATED).is_none() {
            let output = std::process::Command::new(std::env::current_exe().expect("test executable"))
                .arg("--exact")
                .arg("startup_auth::tests::keystone_initialization_reuses_the_provider_for_concurrent_calls")
                .arg("--nocapture")
                .env(ISOLATED, "1")
                .output()
                .expect("start isolated Keystone initialization test");
            assert!(
                String::from_utf8_lossy(&output.stdout).contains("running 1 test"),
                "isolated initializer must actually run: stdout={} stderr={}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            assert!(
                output.status.success(),
                "stdout={} stderr={}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            return;
        }
        crate::startup_runtime_hooks::install_default_crypto_provider().expect("TLS provider");
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("initialization runtime");
        runtime.block_on(async {
            let barrier = Arc::new(tokio::sync::Barrier::new(2));
            let start = |barrier: Arc<tokio::sync::Barrier>| {
                tokio::spawn(async move {
                    barrier.wait().await;
                    let config = rustfs_keystone::KeystoneConfig {
                        enable: true,
                        auth_url: "https://keystone.example.test".into(),
                        ..Default::default()
                    };
                    init_keystone_auth_once(config)
                        .await
                        .expect("concurrent Keystone initialization");
                    crate::auth_keystone::get_keystone_auth().expect("published Keystone provider")
                })
            };
            let left = start(Arc::clone(&barrier));
            let right = start(barrier);
            let (left, right) = tokio::join!(left, right);
            let left = left.expect("left initializer");
            let right = right.expect("right initializer");
            assert!(Arc::ptr_eq(&left, &right), "concurrent listeners must share one provider");
            init_keystone_auth_once(rustfs_keystone::KeystoneConfig {
                enable: true,
                auth_url: "https://keystone.example.test".into(),
                ..Default::default()
            })
            .await
            .expect("later auth integration initialization");
            assert!(
                Arc::ptr_eq(&left, &crate::auth_keystone::get_keystone_auth().expect("retained provider")),
                "auth integrations must retain the listener's provider"
            );
        });
    }

    #[test]
    fn standard_oidc_runtime_retains_one_oidc_arc() {
        let oidc = Arc::new(OidcSys::empty().expect("empty OIDC configuration should be valid"));
        let retained = Arc::clone(&oidc);

        let (service, oidc_config_query) = standard_oidc_runtime(oidc);

        assert_eq!(Arc::strong_count(&retained), 2, "the runtime should retain one OIDC Arc");
        drop(service);
        assert_eq!(Arc::strong_count(&retained), 2, "the query should retain the shared adapter");
        drop(oidc_config_query);
        assert_eq!(Arc::strong_count(&retained), 1, "dropping the runtime should release its OIDC Arc");
    }

    #[tokio::test]
    async fn oidc_recovery_shutdown_releases_pending_work() {
        let shutdown = CancellationToken::new();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (released_tx, released_rx) = tokio::sync::oneshot::channel::<()>();
        let recovery = spawn_oidc_discovery_recovery(
            async move {
                let _release = released_tx;
                let _ = started_tx.send(());
                std::future::pending::<()>().await;
            },
            shutdown.child_token(),
        );
        started_rx.await.expect("recovery started");
        shutdown.cancel();
        tokio::time::timeout(std::time::Duration::from_secs(5), recovery)
            .await
            .expect("recovery observes shutdown")
            .expect("recovery joins cleanly");
        assert!(released_rx.await.is_err(), "shutdown drops in-flight recovery work");
    }

    #[tokio::test]
    async fn oidc_recovery_owner_drop_releases_pending_work() {
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (released_tx, released_rx) = tokio::sync::oneshot::channel::<()>();
        let recovery = spawn_oidc_discovery_recovery(
            async move {
                let _release = released_tx;
                let _ = started_tx.send(());
                std::future::pending::<()>().await;
            },
            CancellationToken::new(),
        );
        started_rx.await.expect("recovery started");
        drop(recovery);
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(5), released_rx)
                .await
                .expect("owner drop aborts recovery")
                .is_err()
        );
    }
}
