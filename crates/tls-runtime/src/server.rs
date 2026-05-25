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

use crate::certs::{CertDirectoryLoadOptions, load_all_certs_from_directory};
use crate::config::TlsReloadOptions;
use crate::error::TlsRuntimeError;
use crate::material::{ServerTlsMaterial, server_material_fingerprint};
use crate::metrics::{record_tls_generation, record_tls_publication_fail, record_tls_reload_result, record_tls_reload_skipped};
use crate::source::TlsSource;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::{ClientHello, ResolvesServerCert, ResolvesServerCertUsingSni};
use rustls::sign::CertifiedKey;
use std::collections::HashMap;
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;
use tracing::{debug, info, warn};

#[derive(Debug)]
struct ResolverState {
    cert_resolver: ResolvesServerCertUsingSni,
    default_cert: Option<Arc<CertifiedKey>>,
    cert_count: usize,
    fingerprint: crate::fingerprint::TlsFingerprint,
}

impl ResolverState {
    fn load_from_source(source: &TlsSource) -> Result<Self, TlsRuntimeError> {
        let base_dir = source.validate_directory()?;
        let cert_key_pairs = load_all_certs_from_directory(
            CertDirectoryLoadOptions::builder(base_dir, &source.layout.server_cert_filename, &source.layout.server_key_filename)
                .build(),
        )?;
        if cert_key_pairs.is_empty() {
            return Err(TlsRuntimeError::Material("No valid certificates found in directory".to_string()));
        }

        Self::from_cert_key_pairs(cert_key_pairs)
    }

    fn from_cert_key_pairs(
        cert_key_pairs: HashMap<String, (Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)>,
    ) -> Result<Self, TlsRuntimeError> {
        let cert_count = cert_key_pairs.len();
        let mut cert_resolver = ResolvesServerCertUsingSni::new();
        let mut default_cert = None;
        let mut entries = cert_key_pairs.into_iter().collect::<Vec<_>>();
        entries.sort_by(|(left_domain, _), (right_domain, _)| left_domain.cmp(right_domain));
        let material = ServerTlsMaterial::MultiCert {
            cert_key_pairs: entries
                .iter()
                .map(|(domain, (certs, key))| (domain.clone(), (certs.clone(), key.clone_key())))
                .collect(),
        };
        let fingerprint = server_material_fingerprint(&material);

        for (domain, (certs, key)) in entries {
            let signing_key = rustls::crypto::aws_lc_rs::sign::any_supported_type(&key)
                .map_err(|e| io::Error::other(format!("unsupported private key type for {domain}: {e:?}")))?;
            let certified_key = CertifiedKey::new(certs, signing_key);

            if domain.as_str() == "default" {
                default_cert = Some(Arc::new(certified_key.clone()));
            } else {
                cert_resolver
                    .add(&domain, certified_key)
                    .map_err(|e| io::Error::other(format!("failed to add certificate for {domain}: {e:?}")))?;
            }
        }

        Ok(Self {
            cert_resolver,
            default_cert,
            cert_count,
            fingerprint,
        })
    }
}

#[derive(Debug)]
pub struct ReloadableServerCertResolver {
    source: TlsSource,
    current: RwLock<ResolverState>,
    generation: AtomicU64,
}

impl ReloadableServerCertResolver {
    pub fn load_from_source(source: TlsSource) -> Result<Arc<Self>, TlsRuntimeError> {
        let state = ResolverState::load_from_source(&source)?;
        record_tls_generation("server_resolver", 1);
        Ok(Arc::new(Self {
            source,
            current: RwLock::new(state),
            generation: AtomicU64::new(1),
        }))
    }

    pub fn load_from_directory(cert_dir: &str) -> Result<Arc<Self>, TlsRuntimeError> {
        Self::load_from_source(TlsSource::from_directory(cert_dir))
    }

    pub fn reload(&self) -> Result<Option<usize>, TlsRuntimeError> {
        let new_state = ResolverState::load_from_source(&self.source)?;

        match self.current.write() {
            Ok(mut guard) => {
                if guard.fingerprint == new_state.fingerprint {
                    return Ok(None);
                }
                let cert_count = new_state.cert_count;
                *guard = new_state;
                self.generation.fetch_add(1, Ordering::Relaxed);
                Ok(Some(cert_count))
            }
            Err(poisoned) => {
                let mut guard = poisoned.into_inner();
                if guard.fingerprint == new_state.fingerprint {
                    return Ok(None);
                }
                let cert_count = new_state.cert_count;
                *guard = new_state;
                self.generation.fetch_add(1, Ordering::Relaxed);
                Ok(Some(cert_count))
            }
        }
    }

    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Relaxed)
    }
}

impl ResolvesServerCert for ReloadableServerCertResolver {
    fn resolve(&self, client_hello: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        let guard = match self.current.read() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };

        guard
            .cert_resolver
            .resolve(client_hello)
            .or_else(|| guard.default_cert.clone())
    }
}

pub fn spawn_server_cert_reload_loop(
    protocol: &'static str,
    resolver: Arc<ReloadableServerCertResolver>,
    options: TlsReloadOptions,
    mut shutdown_rx: watch::Receiver<bool>,
) -> Option<JoinHandle<()>> {
    if !options.enabled {
        debug!(protocol, "TLS certificate hot reload is disabled");
        return None;
    }

    info!(
        protocol,
        cert_dir = %resolver.source.base_dir.display(),
        "TLS certificate hot reload enabled, checking every {}s",
        options.interval.as_secs()
    );

    Some(tokio::spawn(async move {
        let mut interval = tokio::time::interval(options.interval);
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        interval.tick().await;

        loop {
            tokio::select! {
                changed = shutdown_rx.changed() => {
                    match changed {
                        Ok(()) => {
                            if *shutdown_rx.borrow() {
                                info!(protocol, cert_dir = %resolver.source.base_dir.display(), "TLS certificate hot reload task stopped");
                                break;
                            }
                            continue;
                        }
                        Err(_) => {
                            info!(
                                protocol,
                                cert_dir = %resolver.source.base_dir.display(),
                                "TLS certificate hot reload task stopped because the shutdown channel closed"
                            );
                            break;
                        }
                    }
                }
                _ = interval.tick() => {}
            }

            match resolver.reload() {
                Ok(Some(cert_count)) => {
                    record_tls_reload_result(protocol, "ok", None, Some(resolver.generation()));
                    info!(
                        protocol,
                        cert_dir = %resolver.source.base_dir.display(),
                        cert_count,
                        "TLS certificates reloaded successfully"
                    );
                }
                Ok(None) => {
                    record_tls_reload_skipped(protocol, "unchanged");
                    debug!(
                        protocol,
                        cert_dir = %resolver.source.base_dir.display(),
                        "TLS certificate material unchanged; skipping reload"
                    );
                }
                Err(e) => {
                    record_tls_publication_fail(protocol);
                    warn!(
                        protocol,
                        cert_dir = %resolver.source.base_dir.display(),
                        "TLS certificate reload failed (will retry): {}",
                        e
                    );
                }
            }
        }
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rcgen::generate_simple_self_signed;
    use std::fs;
    use tempfile::TempDir;

    fn cert_key_pair(san: &str) -> (Vec<CertificateDer<'static>>, PrivateKeyDer<'static>) {
        let cert = generate_simple_self_signed(vec![san.to_string()]).expect("cert should generate");
        (
            vec![cert.cert.der().clone()],
            PrivateKeyDer::try_from(cert.signing_key.serialize_der()).expect("key should convert"),
        )
    }

    fn clone_cert_key_pair(
        cert_key_pair: &(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>),
    ) -> (Vec<CertificateDer<'static>>, PrivateKeyDer<'static>) {
        (cert_key_pair.0.clone(), cert_key_pair.1.clone_key())
    }

    fn write_default_cert(dir: &std::path::Path, san: &str) {
        let cert = generate_simple_self_signed(vec![san.to_string()]).expect("cert should generate");
        fs::write(dir.join(rustfs_config::RUSTFS_TLS_CERT), cert.cert.pem()).expect("cert should write");
        fs::write(dir.join(rustfs_config::RUSTFS_TLS_KEY), cert.signing_key.serialize_pem()).expect("key should write");
    }

    #[test]
    fn reload_replaces_default_certificate() {
        let temp_dir = TempDir::new().expect("tempdir should create");
        write_default_cert(temp_dir.path(), "localhost");

        let resolver = ReloadableServerCertResolver::load_from_directory(temp_dir.path().to_str().expect("path should utf8"))
            .expect("resolver should load");
        let before = {
            let guard = resolver.current.read().expect("lock should acquire");
            guard.default_cert.as_ref().expect("default cert should exist").clone()
        };

        write_default_cert(temp_dir.path(), "rotated.local");

        let cert_count = resolver.reload().expect("reload should succeed");
        assert_eq!(cert_count, Some(1));

        let after = {
            let guard = resolver.current.read().expect("lock should acquire");
            guard.default_cert.as_ref().expect("default cert should exist").clone()
        };

        assert_ne!(before.cert[0].as_ref(), after.cert[0].as_ref());
    }

    #[test]
    fn reload_skips_when_material_is_unchanged() {
        let temp_dir = TempDir::new().expect("tempdir should create");
        write_default_cert(temp_dir.path(), "localhost");

        let resolver = ReloadableServerCertResolver::load_from_directory(temp_dir.path().to_str().expect("path should utf8"))
            .expect("resolver should load");
        let outcome = resolver.reload().expect("reload should succeed");
        assert_eq!(outcome, None);
    }

    #[test]
    fn resolver_state_fingerprint_is_stable_across_domain_ordering() {
        let default_cert = cert_key_pair("localhost");
        let api_cert = cert_key_pair("api.example.com");
        let web_cert = cert_key_pair("web.example.com");

        let mut first = HashMap::new();
        first.insert("default".to_string(), clone_cert_key_pair(&default_cert));
        first.insert("api.example.com".to_string(), clone_cert_key_pair(&api_cert));
        first.insert("web.example.com".to_string(), clone_cert_key_pair(&web_cert));

        let mut second = HashMap::new();
        second.insert("web.example.com".to_string(), clone_cert_key_pair(&web_cert));
        second.insert("default".to_string(), clone_cert_key_pair(&default_cert));
        second.insert("api.example.com".to_string(), clone_cert_key_pair(&api_cert));

        let first_state = ResolverState::from_cert_key_pairs(first).expect("first state should build");
        let second_state = ResolverState::from_cert_key_pairs(second).expect("second state should build");

        assert_eq!(first_state.cert_count, 3);
        assert_eq!(second_state.cert_count, 3);
        assert_eq!(first_state.fingerprint, second_state.fingerprint);
    }
}
