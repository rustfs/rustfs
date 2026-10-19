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
    MtlsIdentityPem, OutboundTlsSnapshot, get_global_outbound_tls_generation, get_global_outbound_tls_snapshot,
    publish_global_outbound_tls_snapshot,
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
    let accepted = publish_global_outbound_tls_snapshot(OutboundTlsSnapshot {
        generation: generation.0,
        root_ca_pem: (!material.root_ca_pem.is_empty()).then(|| material.root_ca_pem.clone()),
        mtls_identity: material.mtls_identity.clone(),
    })
    .await;
    record_outbound_tls_publication(accepted, !material.root_ca_pem.is_empty(), material.mtls_identity.is_some());
}

pub async fn load_global_outbound_tls_state() -> GlobalPublishedOutboundTlsState {
    let snapshot = get_global_outbound_tls_snapshot();
    GlobalPublishedOutboundTlsState {
        generation: TlsGeneration(snapshot.generation),
        root_ca_pem: snapshot.root_ca_pem.clone(),
        mtls_identity: snapshot.mtls_identity.clone(),
    }
}

pub fn load_global_outbound_tls_generation() -> TlsGeneration {
    TlsGeneration(get_global_outbound_tls_generation())
}

pub async fn summarize_global_outbound_tls_state() -> GlobalOutboundTlsStateSummary {
    let state = load_global_outbound_tls_state().await;
    GlobalOutboundTlsStateSummary {
        generation: state.generation,
        has_root_ca: state.root_ca_pem.as_ref().is_some_and(|pem| !pem.is_empty()),
        has_mtls_identity: state.mtls_identity.is_some(),
    }
}
