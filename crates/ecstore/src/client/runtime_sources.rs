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

use rustfs_tls_runtime::{GlobalPublishedOutboundTlsState, load_global_outbound_tls_state, record_tls_generation};

const ECSTORE_TRANSITION_CLIENT_TLS_CONSUMER: &str = "ecstore_transition_client";

pub(crate) async fn transition_client_outbound_tls_state() -> GlobalPublishedOutboundTlsState {
    load_global_outbound_tls_state().await
}

pub(crate) fn record_transition_client_tls_generation(generation: u64) {
    record_tls_generation(ECSTORE_TRANSITION_CLIENT_TLS_CONSUMER, generation);
}
