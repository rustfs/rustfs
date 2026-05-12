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

use rustfs_config::{
    DEFAULT_TLS_RELOAD_ENABLE, DEFAULT_TLS_RELOAD_INTERVAL, ENV_TLS_RELOAD_ENABLE, ENV_TLS_RELOAD_INTERVAL, RUSTFS_TLS_CERT,
    RUSTFS_TLS_KEY,
};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::{ClientHello, ResolvesServerCert, ResolvesServerCertUsingSni};
use rustls::sign::CertifiedKey;
use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;
use std::io::{self, Error};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;
use tracing::{debug, info, warn};

#[derive(Debug)]
struct ResolverState {
    cert_resolver: ResolvesServerCertUsingSni,
    default_cert: Option<Arc<CertifiedKey>>,
    cert_count: usize,
    fingerprint: u64,
}

impl ResolverState {
    fn load_from_directory(cert_dir: &str) -> io::Result<Self> {
        let cert_key_pairs = rustfs_utils::load_all_certs_from_directory(
            rustfs_utils::CertDirectoryLoadOptions::builder(cert_dir, RUSTFS_TLS_CERT, RUSTFS_TLS_KEY).build(),
        )?;
        if cert_key_pairs.is_empty() {
            return Err(Error::other("No valid certificates found in directory"));
        }

        Self::from_cert_key_pairs(cert_key_pairs)
    }

    fn from_cert_key_pairs(
        cert_key_pairs: HashMap<String, (Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)>,
    ) -> io::Result<Self> {
        let cert_count = cert_key_pairs.len();
        let mut cert_resolver = ResolvesServerCertUsingSni::new();
        let mut default_cert = None;
        let mut entries = cert_key_pairs.into_iter().collect::<Vec<_>>();
        entries.sort_by(|(left_domain, _), (right_domain, _)| left_domain.cmp(right_domain));
        let fingerprint = fingerprint_tls_entries(&entries);

        for (domain, (certs, key)) in entries {
            let signing_key = rustls::crypto::aws_lc_rs::sign::any_supported_type(&key)
                .map_err(|e| Error::other(format!("unsupported private key type for {domain}: {e:?}")))?;
            let certified_key = CertifiedKey::new(certs, signing_key);

            if domain.as_str() == "default" {
                default_cert = Some(Arc::new(certified_key.clone()));
            } else {
                cert_resolver
                    .add(&domain, certified_key)
                    .map_err(|e| Error::other(format!("failed to add certificate for {domain}: {e:?}")))?;
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

fn fingerprint_tls_entries(entries: &[(String, (Vec<CertificateDer<'static>>, PrivateKeyDer<'static>))]) -> u64 {
    let mut hasher = DefaultHasher::new();

    for (domain, (certs, key)) in entries {
        hasher.write_usize(domain.len());
        hasher.write(domain.as_bytes());
        hasher.write_usize(certs.len());
        for cert in certs {
            hasher.write_usize(cert.as_ref().len());
            hasher.write(cert.as_ref());
        }
        hasher.write_usize(key.secret_der().len());
        hasher.write(key.secret_der());
    }

    hasher.finish()
}

#[derive(Debug)]
pub(crate) struct ReloadableCertResolver {
    current: RwLock<ResolverState>,
}

impl ReloadableCertResolver {
    pub(crate) fn load_from_directory(cert_dir: &str) -> io::Result<Arc<Self>> {
        let state = ResolverState::load_from_directory(cert_dir)?;
        Ok(Arc::new(Self {
            current: RwLock::new(state),
        }))
    }

    pub(crate) fn reload_from_directory(&self, cert_dir: &str) -> io::Result<Option<usize>> {
        let new_state = ResolverState::load_from_directory(cert_dir)?;

        match self.current.write() {
            Ok(mut guard) => {
                if guard.fingerprint == new_state.fingerprint {
                    return Ok(None);
                }
                let cert_count = new_state.cert_count;
                *guard = new_state;
                Ok(Some(cert_count))
            }
            Err(poisoned) => {
                let mut guard = poisoned.into_inner();
                if guard.fingerprint == new_state.fingerprint {
                    return Ok(None);
                }
                let cert_count = new_state.cert_count;
                *guard = new_state;
                Ok(Some(cert_count))
            }
        }
    }
}

impl ResolvesServerCert for ReloadableCertResolver {
    fn resolve(&self, client_hello: ClientHello) -> Option<Arc<CertifiedKey>> {
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

pub(crate) fn spawn_cert_reload_loop(
    protocol: &'static str,
    cert_dir: String,
    resolver: Arc<ReloadableCertResolver>,
    mut shutdown_rx: watch::Receiver<bool>,
) -> Option<JoinHandle<()>> {
    let enabled = rustfs_utils::get_env_bool(ENV_TLS_RELOAD_ENABLE, DEFAULT_TLS_RELOAD_ENABLE);
    if !enabled {
        debug!(
            protocol,
            "TLS certificate hot reload is disabled (set {}=1 to enable)", ENV_TLS_RELOAD_ENABLE
        );
        return None;
    }

    let interval_secs = rustfs_utils::get_env_u64(ENV_TLS_RELOAD_INTERVAL, DEFAULT_TLS_RELOAD_INTERVAL).max(5);
    info!(
        protocol,
        cert_dir = %cert_dir,
        "TLS certificate hot reload enabled, checking every {}s",
        interval_secs
    );

    Some(tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        interval.tick().await;

        loop {
            tokio::select! {
                changed = shutdown_rx.changed() => {
                    match changed {
                        Ok(()) => {
                            if *shutdown_rx.borrow() {
                                info!(protocol, cert_dir = %cert_dir, "TLS certificate hot reload task stopped");
                                break;
                            }
                            continue;
                        }
                        Err(_) => {
                            info!(
                                protocol,
                                cert_dir = %cert_dir,
                                "TLS certificate hot reload task stopped because the shutdown channel closed"
                            );
                            break;
                        }
                    }
                }
                _ = interval.tick() => {}
            }

            match resolver.reload_from_directory(&cert_dir) {
                Ok(Some(cert_count)) => {
                    info!(
                        protocol,
                        cert_dir = %cert_dir,
                        cert_count,
                        "TLS certificates reloaded successfully"
                    );
                }
                Ok(None) => {
                    debug!(protocol, cert_dir = %cert_dir, "TLS certificate material unchanged; skipping reload");
                }
                Err(e) => {
                    warn!(
                        protocol,
                        cert_dir = %cert_dir,
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
        let cert = generate_simple_self_signed(vec![san.to_string()]).unwrap();
        (
            vec![cert.cert.der().clone()],
            PrivateKeyDer::try_from(cert.signing_key.serialize_der()).unwrap(),
        )
    }

    fn clone_cert_key_pair(
        cert_key_pair: &(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>),
    ) -> (Vec<CertificateDer<'static>>, PrivateKeyDer<'static>) {
        (cert_key_pair.0.clone(), cert_key_pair.1.clone_key())
    }

    fn write_default_cert(dir: &std::path::Path, san: &str) {
        let cert = generate_simple_self_signed(vec![san.to_string()]).unwrap();
        fs::write(dir.join(RUSTFS_TLS_CERT), cert.cert.pem()).unwrap();
        fs::write(dir.join(RUSTFS_TLS_KEY), cert.signing_key.serialize_pem()).unwrap();
    }

    #[test]
    fn reload_from_directory_replaces_default_certificate() {
        let temp_dir = TempDir::new().unwrap();
        write_default_cert(temp_dir.path(), "localhost");

        let resolver = ReloadableCertResolver::load_from_directory(temp_dir.path().to_str().unwrap()).unwrap();
        let before = {
            let guard = resolver.current.read().unwrap();
            guard.default_cert.as_ref().unwrap().clone()
        };

        write_default_cert(temp_dir.path(), "rotated.local");

        let cert_count = resolver.reload_from_directory(temp_dir.path().to_str().unwrap()).unwrap();
        assert_eq!(cert_count, Some(1));

        let after = {
            let guard = resolver.current.read().unwrap();
            guard.default_cert.as_ref().unwrap().clone()
        };

        assert_ne!(before.cert[0].as_ref(), after.cert[0].as_ref());
    }

    #[test]
    fn reload_from_directory_skips_when_material_is_unchanged() {
        let temp_dir = TempDir::new().unwrap();
        write_default_cert(temp_dir.path(), "localhost");

        let resolver = ReloadableCertResolver::load_from_directory(temp_dir.path().to_str().unwrap()).unwrap();
        let outcome = resolver.reload_from_directory(temp_dir.path().to_str().unwrap()).unwrap();
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

        let first_state = ResolverState::from_cert_key_pairs(first).unwrap();
        let second_state = ResolverState::from_cert_key_pairs(second).unwrap();

        assert_eq!(first_state.cert_count, 3);
        assert_eq!(first_state.fingerprint, second_state.fingerprint);
    }
}
