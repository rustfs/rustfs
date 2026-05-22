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

use crate::material::OutboundTlsMaterial;
use crate::metrics::record_outbound_tls_publication;
use crate::state::TlsGeneration;
use rustfs_common::{
    GLOBAL_MTLS_IDENTITY, GLOBAL_ROOT_CERT, MtlsIdentityPem, get_global_outbound_tls_generation, set_global_mtls_identity,
    set_global_outbound_tls_generation, set_global_root_cert,
};

#[derive(Debug, Clone)]
pub struct GlobalPublishedOutboundTlsState {
    pub generation: TlsGeneration,
    pub root_ca_pem: Option<Vec<u8>>,
    pub mtls_identity: Option<MtlsIdentityPem>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GlobalOutboundTlsStateSummary {
    pub generation: TlsGeneration,
    pub has_root_ca: bool,
    pub has_mtls_identity: bool,
}

pub async fn publish_global_outbound_tls_state(generation: TlsGeneration, material: &OutboundTlsMaterial) {
    if !material.root_ca_pem.is_empty() {
        set_global_root_cert(material.root_ca_pem.clone()).await;
    } else {
        *GLOBAL_ROOT_CERT.write().await = None;
    }
    set_global_mtls_identity(material.mtls_identity.clone()).await;
    set_global_outbound_tls_generation(generation.0);
    record_outbound_tls_publication(generation.0, !material.root_ca_pem.is_empty(), material.mtls_identity.is_some());
}

pub async fn load_global_outbound_tls_state() -> GlobalPublishedOutboundTlsState {
    GlobalPublishedOutboundTlsState {
        generation: TlsGeneration(get_global_outbound_tls_generation()),
        root_ca_pem: GLOBAL_ROOT_CERT.read().await.clone(),
        mtls_identity: GLOBAL_MTLS_IDENTITY.read().await.clone(),
    }
}

pub async fn summarize_global_outbound_tls_state() -> GlobalOutboundTlsStateSummary {
    let state = load_global_outbound_tls_state().await;
    GlobalOutboundTlsStateSummary {
        generation: state.generation,
        has_root_ca: state.root_ca_pem.as_ref().is_some_and(|pem| !pem.is_empty()),
        has_mtls_identity: state.mtls_identity.is_some(),
    }
}
