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

pub mod certs;
pub mod config;
pub mod coordinator;
pub mod debug;
pub mod error;
pub mod fingerprint;
pub mod material;
pub mod metrics;
pub mod outbound;
pub mod server;
pub mod source;
pub mod state;

pub use certs::{
    CertDirectoryLoadOptions, WebPkiClientVerifierOptions, build_webpki_client_verifier, create_multi_cert_resolver,
    load_all_certs_from_directory, load_cert_bundle_der_bytes, load_certs, load_private_key,
};
pub use config::{ReloadApplyHint, ReloadDetectMode, TlsReloadOptions};
pub use coordinator::{TlsConsumer, TlsReloadCoordinator};
pub use debug::{TlsConsumerStatusItem, TlsDebugStatusResponse, TlsDebugStatusResponseBuilder};
pub use error::TlsRuntimeError;
pub use fingerprint::TlsFingerprint;
pub use material::{OutboundTlsMaterial, ServerTlsMaterial, TlsMaterialSnapshot};
pub use metrics::{
    TLS_OUTBOUND_GLOBAL_CONSUMER, TLS_RUNTIME_FOUNDATION_CONSUMER, init_tls_metrics, record_tls_consumer_stale_generation,
    record_tls_generation, record_tls_publication_fail, record_tls_reload_result, record_tls_reload_skipped,
};
pub use outbound::{
    GlobalOutboundTlsStateSummary, GlobalPublishedOutboundTlsState, load_global_outbound_tls_generation,
    load_global_outbound_tls_state, publish_global_outbound_tls_state, summarize_global_outbound_tls_state,
};
pub use server::{ReloadableServerCertResolver, spawn_server_cert_reload_loop};
pub use source::{TlsFileLayout, TlsSource, TlsSourceKind};
pub use state::OutboundOnlySnapshotArgs;
pub use state::{TlsGeneration, TlsPublishedState, TlsReloadRuntimeState, TlsRuntimeStatusSnapshot};
pub use state::{TlsRuntimeConsumerSection, TlsRuntimeOutboundSection, TlsRuntimeRuntimeSection, TlsRuntimeServerSection};

#[cfg(test)]
mod tests {
    use super::*;
    use rcgen::generate_simple_self_signed;
    use std::collections::HashMap;

    #[test]
    fn tls_source_requires_existing_directory() {
        let source = TlsSource::from_directory("/definitely/missing/rustfs/tls-runtime-test");
        let err = source.validate_directory().expect_err("missing directory should fail");
        assert!(matches!(err, TlsRuntimeError::DirectoryNotFound { .. }));
    }

    #[test]
    fn fingerprint_changes_when_server_material_changes() {
        let cert_a = generate_simple_self_signed(vec!["a.example.com".to_string()]).expect("cert A should generate");
        let cert_b = generate_simple_self_signed(vec!["b.example.com".to_string()]).expect("cert B should generate");

        let single_a = ServerTlsMaterial::SingleCert {
            certs: vec![cert_a.cert.der().clone()],
            key: rustls::pki_types::PrivateKeyDer::try_from(cert_a.signing_key.serialize_der()).expect("key A should convert"),
        };
        let single_b = ServerTlsMaterial::SingleCert {
            certs: vec![cert_b.cert.der().clone()],
            key: rustls::pki_types::PrivateKeyDer::try_from(cert_b.signing_key.serialize_der()).expect("key B should convert"),
        };

        let bytes_a = match &single_a {
            ServerTlsMaterial::SingleCert { certs, key } => {
                let mut bytes = Vec::new();
                for cert in certs {
                    bytes.extend_from_slice(cert.as_ref());
                }
                bytes.extend_from_slice(key.secret_der());
                bytes
            }
            ServerTlsMaterial::MultiCert { .. } => unreachable!(),
        };
        let bytes_b = match &single_b {
            ServerTlsMaterial::SingleCert { certs, key } => {
                let mut bytes = Vec::new();
                for cert in certs {
                    bytes.extend_from_slice(cert.as_ref());
                }
                bytes.extend_from_slice(key.secret_der());
                bytes
            }
            ServerTlsMaterial::MultiCert { .. } => unreachable!(),
        };

        let fp_a = TlsFingerprint::from_optional_bytes(Some(&bytes_a), None, None, None, None);
        let fp_b = TlsFingerprint::from_optional_bytes(Some(&bytes_b), None, None, None, None);
        assert_ne!(fp_a, fp_b);
    }

    #[tokio::test]
    async fn coordinator_can_publish_initial_state() {
        let source = TlsSource::from_directory(std::env::temp_dir());
        let coordinator = TlsReloadCoordinator::new(source.clone(), TlsReloadOptions::default());
        let snapshot = TlsMaterialSnapshot {
            source,
            server: Some(ServerTlsMaterial::MultiCert {
                cert_key_pairs: HashMap::new(),
            }),
            outbound: OutboundTlsMaterial {
                root_ca_pem: Vec::new(),
                mtls_identity: None,
            },
            fingerprint: TlsFingerprint::default(),
        };

        let published = coordinator.publish_initial_state(snapshot).await;
        assert_eq!(published.generation, TlsGeneration(1));
    }
}
