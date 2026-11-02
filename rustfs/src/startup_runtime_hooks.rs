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

use crate::license::license_status;
use crate::startup_runtime_sources;
use rustfs_tls_runtime::{CryptoProviderStatus, ensure_crypto_provider};
use std::future::Future;
use std::io::Result;
use tracing::{debug, info, warn};

const LOG_COMPONENT_EMBEDDED: &str = "embedded";
const LOG_COMPONENT_MAIN: &str = "main";
const LOG_SUBSYSTEM_EMBEDDED: &str = "embedded";
const LOG_SUBSYSTEM_LICENSE: &str = "license";
const LOG_SUBSYSTEM_STARTUP: &str = "startup";
const EVENT_CRYPTO_PROVIDER_STATE: &str = "crypto_provider_state";
const EVENT_DIAL9_RUNTIME_STATUS: &str = "dial9_runtime_status";
const EVENT_RUNTIME_LICENSE_STATUS: &str = "runtime_license_status";

pub(crate) fn log_startup_runtime_diagnostics() {
    log_dial9_runtime_status();
    log_runtime_license_status();
    debug!("{}", crate::server::LOGO);
}

fn log_dial9_runtime_status() {
    let supported = rustfs_obs::dial9::is_supported();
    let configured = rustfs_obs::dial9::is_configured();

    match (supported, configured) {
        (true, true) => info!(
            target: "rustfs::main",
            event = EVENT_DIAL9_RUNTIME_STATUS,
            component = LOG_COMPONENT_MAIN,
            subsystem = LOG_SUBSYSTEM_STARTUP,
            enabled = true,
            "Dial9 Tokio runtime telemetry is enabled"
        ),
        // Requested but unavailable. This is the case an operator most needs to
        // see: the process otherwise runs to completion recording nothing.
        (false, true) => warn!(
            target: "rustfs::main",
            event = EVENT_DIAL9_RUNTIME_STATUS,
            component = LOG_COMPONENT_MAIN,
            subsystem = LOG_SUBSYSTEM_STARTUP,
            enabled = false,
            supported = false,
            remedy = "rebuild with --features dial9 (see `make build-profiling`)",
            "Dial9 Tokio runtime telemetry is configured but not compiled into this binary"
        ),
        (_, false) => debug!(
            target: "rustfs::main",
            event = EVENT_DIAL9_RUNTIME_STATUS,
            component = LOG_COMPONENT_MAIN,
            subsystem = LOG_SUBSYSTEM_STARTUP,
            enabled = false,
            supported,
            "Dial9 Tokio runtime telemetry is disabled"
        ),
    }
}

fn log_runtime_license_status() {
    info!(
        target: "rustfs::main",
        event = EVENT_RUNTIME_LICENSE_STATUS,
        component = LOG_COMPONENT_MAIN,
        subsystem = LOG_SUBSYSTEM_LICENSE,
        license_status = %license_status(),
        "Initialized runtime license state"
    );
}

pub(crate) async fn init_profiling_runtime() {
    init_profiling_runtime_with(crate::profiling::init_from_env).await;
}

async fn init_profiling_runtime_with<InitFn, InitFuture>(init: InitFn)
where
    InitFn: FnOnce() -> InitFuture,
    InitFuture: Future<Output = ()>,
{
    init().await;
}

pub(crate) fn shutdown_profiling_runtime() {
    shutdown_profiling_runtime_with(crate::profiling::shutdown_profiling);
}

fn shutdown_profiling_runtime_with<ShutdownFn>(shutdown: ShutdownFn)
where
    ShutdownFn: FnOnce(),
{
    shutdown();
}

pub(crate) fn install_default_crypto_provider() -> Result<()> {
    if ensure_crypto_provider().map_err(std::io::Error::other)? == CryptoProviderStatus::AlreadyInstalled {
        debug!(
            target: "rustfs::main",
            event = EVENT_CRYPTO_PROVIDER_STATE,
            component = LOG_COMPONENT_MAIN,
            subsystem = LOG_SUBSYSTEM_STARTUP,
            provider = "process_default",
            state = "already_installed",
            "Rustls crypto provider state checked"
        );
    }
    Ok(())
}

pub(crate) async fn init_embedded_runtime_hooks(obs_endpoint: String) -> Result<()> {
    install_embedded_default_crypto_provider()?;

    // Observability is a process-global tracing subscriber and can only be
    // installed once. A second embedded server in the same process reuses the
    // first server's subscriber — try to publish anyway (first server wins),
    // and treat both an init failure and a set failure as "already set" so a
    // second startup does not abort (backlog#1052 S5).
    match startup_runtime_sources::init_observability_guard(obs_endpoint).await {
        Ok(guard) => {
            if let Err(err) = startup_runtime_sources::set_observability_guard(guard) {
                debug!(
                    component = LOG_COMPONENT_EMBEDDED,
                    subsystem = LOG_SUBSYSTEM_EMBEDDED,
                    event = "observability_guard_reused",
                    error = %err,
                    "process already has an observability guard installed; second server reuses it"
                );
            }
        }
        Err(err) => {
            debug!(
                component = LOG_COMPONENT_EMBEDDED,
                subsystem = LOG_SUBSYSTEM_EMBEDDED,
                event = "observability_init_skipped",
                error = %err,
                "observability already initialized; second server reuses the process subscriber"
            );
        }
    }

    rustfs_trusted_proxies::init();

    Ok(())
}

fn install_embedded_default_crypto_provider() -> Result<()> {
    if ensure_crypto_provider().map_err(std::io::Error::other)? == CryptoProviderStatus::AlreadyInstalled {
        debug!(
            event = EVENT_CRYPTO_PROVIDER_STATE,
            component = LOG_COMPONENT_EMBEDDED,
            subsystem = LOG_SUBSYSTEM_EMBEDDED,
            state = "already_installed",
            "Embedded crypto provider state changed"
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        init_profiling_runtime_with, install_default_crypto_provider, install_embedded_default_crypto_provider,
        shutdown_profiling_runtime_with,
    };
    use rustfs_tls_runtime::default_crypto_provider as build_default_crypto_provider;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    #[cfg(feature = "fips")]
    #[test]
    fn default_crypto_provider_matches_fips_feature() {
        let provider = build_default_crypto_provider();
        assert!(provider.fips());
    }

    #[cfg(feature = "fips")]
    #[test]
    fn fips_provider_builds_fips_tls_configs() {
        let provider = Arc::new(build_default_crypto_provider());
        assert!(provider.cipher_suites.iter().all(|suite| suite.fips()));
        assert!(provider.kx_groups.iter().all(|group| group.fips()));
        assert!(
            !provider
                .cipher_suites
                .iter()
                .any(|suite| { suite.suite() == rustls::CipherSuite::TLS13_CHACHA20_POLY1305_SHA256 })
        );
        assert!(
            !provider
                .kx_groups
                .iter()
                .any(|group| group.name() == rustls::NamedGroup::X25519)
        );

        let client = rustls::ClientConfig::builder_with_provider(provider.clone())
            .with_safe_default_protocol_versions()
            .expect("supported protocol versions")
            .with_root_certificates(rustls::RootCertStore::empty())
            .with_no_client_auth();
        let server = rustls::ServerConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .expect("supported protocol versions")
            .with_no_client_auth()
            .with_cert_resolver(Arc::new(rustls::server::ResolvesServerCertUsingSni::new()));
        assert!(client.fips());
        assert!(server.fips());
    }

    #[cfg(feature = "fips")]
    fn tls_handshake_with_provider(
        client_provider: rustls::crypto::CryptoProvider,
        version: &'static rustls::SupportedProtocolVersion,
    ) -> std::result::Result<(), rustls::Error> {
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).expect("generate certificate");
        let cert_der = cert.cert.der().clone();
        let private_key = rustls::pki_types::PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());
        let mut roots = rustls::RootCertStore::empty();
        roots.add(cert_der.clone()).expect("trust certificate");

        let server_config = rustls::ServerConfig::builder_with_provider(Arc::new(build_default_crypto_provider()))
            .with_protocol_versions(&[version])
            .expect("server protocol version")
            .with_no_client_auth()
            .with_single_cert(vec![cert_der], private_key.into())
            .expect("server identity");
        assert!(server_config.fips());
        let client_config = rustls::ClientConfig::builder_with_provider(Arc::new(client_provider))
            .with_protocol_versions(&[version])
            .expect("client protocol version")
            .with_root_certificates(roots)
            .with_no_client_auth();

        let mut server = rustls::ServerConnection::new(Arc::new(server_config)).expect("server connection");
        let mut client = rustls::ClientConnection::new(
            Arc::new(client_config),
            rustls::pki_types::ServerName::try_from("localhost").expect("server name"),
        )
        .expect("client connection");
        for _ in 0..8 {
            let mut client_bytes = Vec::new();
            client.write_tls(&mut client_bytes).expect("write client TLS");
            if !client_bytes.is_empty() {
                server
                    .read_tls(&mut std::io::Cursor::new(client_bytes))
                    .expect("read server TLS");
                server.process_new_packets()?;
            }

            let mut server_bytes = Vec::new();
            server.write_tls(&mut server_bytes).expect("write server TLS");
            if !server_bytes.is_empty() {
                client
                    .read_tls(&mut std::io::Cursor::new(server_bytes))
                    .expect("read client TLS");
                client.process_new_packets()?;
            }
            if !client.is_handshaking() && !server.is_handshaking() {
                return Ok(());
            }
        }
        panic!("TLS handshake did not complete within eight exchanges");
    }

    #[cfg(feature = "fips")]
    #[test]
    fn fips_provider_tls_handshake_accepts_and_rejects_algorithms() {
        for version in [&rustls::version::TLS12, &rustls::version::TLS13] {
            tls_handshake_with_provider(build_default_crypto_provider(), version).expect("FIPS TLS handshake");
            let mut client_provider = build_default_crypto_provider();
            client_provider.kx_groups = vec![rustls::crypto::aws_lc_rs::kx_group::X25519];
            assert!(tls_handshake_with_provider(client_provider, version).is_err());
        }
        let mut client_provider = build_default_crypto_provider();
        client_provider.cipher_suites = vec![rustls::crypto::aws_lc_rs::cipher_suite::TLS13_CHACHA20_POLY1305_SHA256];
        assert!(tls_handshake_with_provider(client_provider, &rustls::version::TLS13).is_err());
    }

    #[test]
    fn provider_installation_isolated() {
        const SCENARIO: &str = "RUSTFS_TEST_PROVIDER_SCENARIO";
        if let Ok(scenario) = std::env::var(SCENARIO) {
            let incompatible = || {
                let mut provider = build_default_crypto_provider();
                provider.kx_groups = vec![rustls::crypto::aws_lc_rs::kx_group::X25519];
                assert!(!provider.fips());
                provider
            };
            match scenario.as_str() {
                "fresh" => {}
                "compatible" => {
                    build_default_crypto_provider().install_default().expect("install provider");
                }
                "incompatible" => {
                    incompatible().install_default().expect("install provider");
                }
                "race" => {
                    let barrier = Arc::new(std::sync::Barrier::new(2));
                    let other_barrier = barrier.clone();
                    let other = std::thread::spawn(move || {
                        other_barrier.wait();
                        let _ = incompatible().install_default();
                    });
                    barrier.wait();
                    let result = install_default_crypto_provider();
                    other.join().expect("provider installer thread");
                    let actual_fips = rustls::crypto::CryptoProvider::get_default()
                        .expect("provider installed")
                        .fips();
                    assert_eq!(result.is_ok(), !cfg!(feature = "fips") || actual_fips);
                    assert_eq!(install_embedded_default_crypto_provider().is_ok(), !cfg!(feature = "fips") || actual_fips);
                    return;
                }
                _ => panic!("unknown provider scenario"),
            }
            let result = install_default_crypto_provider();
            let actual_fips = rustls::crypto::CryptoProvider::get_default()
                .expect("provider installed")
                .fips();
            assert_eq!(result.is_ok(), !cfg!(feature = "fips") || actual_fips);
            if cfg!(feature = "fips") && scenario == "incompatible" {
                assert!(
                    result
                        .unwrap_err()
                        .to_string()
                        .contains("process-wide rustls crypto provider")
                );
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("embedded runtime");
                let error = runtime
                    .block_on(super::init_embedded_runtime_hooks(String::new()))
                    .unwrap_err();
                assert!(error.to_string().contains("process-wide rustls crypto provider"));
                let config = crate::config::Config::new("127.0.0.1:9000", Vec::new());
                let report = rustfs_utils::ExternalEnvCompatReport::default();
                let error = runtime
                    .block_on(crate::startup_preflight::init_startup_server_preflight(&config, &report))
                    .unwrap_err();
                assert!(error.to_string().contains("process-wide rustls crypto provider"));
                assert!(rustfs_obs::get_global_guard().is_err(), "observability must not initialize first");
            }
            assert_eq!(install_embedded_default_crypto_provider().is_ok(), !cfg!(feature = "fips") || actual_fips);
            return;
        }

        for scenario in ["fresh", "compatible", "incompatible", "race"] {
            let output = std::process::Command::new(std::env::current_exe().expect("test executable"))
                .arg("provider_installation_isolated")
                .arg("--nocapture")
                .env(SCENARIO, scenario)
                .output()
                .expect("start isolated test");
            assert!(
                output.status.success(),
                "{scenario}: stdout={} stderr={}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }
    }

    #[tokio::test]
    async fn init_profiling_runtime_invokes_registered_hook() {
        let called = Arc::new(AtomicBool::new(false));
        let hook_called = called.clone();

        init_profiling_runtime_with(move || async move {
            hook_called.store(true, Ordering::SeqCst);
        })
        .await;

        assert!(called.load(Ordering::SeqCst));
    }

    #[test]
    fn shutdown_profiling_runtime_invokes_registered_hook() {
        let called = AtomicBool::new(false);

        shutdown_profiling_runtime_with(|| {
            called.store(true, Ordering::SeqCst);
        });

        assert!(called.load(Ordering::SeqCst));
    }
}
