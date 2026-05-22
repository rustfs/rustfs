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

use crate::state::TlsRuntimeStatusSnapshot;
use serde::Serialize;

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct TlsConsumerStatusItem {
    pub consumer: &'static str,
    pub generation: u64,
    pub has_root_ca: bool,
    pub has_mtls_identity: bool,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct TlsDebugStatusResponse {
    pub foundation: TlsRuntimeStatusSnapshot,
    pub consumers: Vec<TlsConsumerStatusItem>,
}

#[derive(Debug, Clone)]
pub struct TlsDebugStatusResponseBuilder {
    foundation: TlsRuntimeStatusSnapshot,
    consumers: Vec<TlsConsumerStatusItem>,
}

impl TlsDebugStatusResponse {
    pub fn builder(foundation: TlsRuntimeStatusSnapshot) -> TlsDebugStatusResponseBuilder {
        TlsDebugStatusResponseBuilder {
            foundation,
            consumers: Vec::new(),
        }
    }
}

impl TlsDebugStatusResponseBuilder {
    pub fn push_consumers<I>(mut self, sources: I) -> Self
    where
        I: IntoIterator<Item = TlsConsumerStatusItem>,
    {
        self.consumers.extend(sources);
        self
    }

    pub fn build(self) -> TlsDebugStatusResponse {
        TlsDebugStatusResponse {
            foundation: self.foundation,
            consumers: self.consumers,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{TlsConsumerStatusItem, TlsDebugStatusResponse};
    use crate::state::{
        TlsRuntimeConsumerSection, TlsRuntimeOutboundSection, TlsRuntimeRuntimeSection, TlsRuntimeServerSection,
        TlsRuntimeStatusSnapshot,
    };

    #[test]
    fn builder_produces_structured_response() {
        let foundation = TlsRuntimeStatusSnapshot {
            runtime: TlsRuntimeRuntimeSection {
                generation: 5,
                reload_enabled: true,
                detect_mode: "poll",
                last_attempt_time: Some(1),
                last_success_time: Some(2),
                last_error: None,
                source_path: "/tmp/tls".to_string(),
            },
            outbound: TlsRuntimeOutboundSection {
                has_roots: true,
                has_mtls_identity: false,
            },
            server: TlsRuntimeServerSection { has_material: true },
            consumer: TlsRuntimeConsumerSection { stale_generation: false },
        };

        let response = TlsDebugStatusResponse::builder(foundation)
            .push_consumers([TlsConsumerStatusItem {
                consumer: "test_consumer",
                generation: 7,
                has_root_ca: true,
                has_mtls_identity: false,
            }])
            .build();

        let json = serde_json::to_value(response).expect("response should serialize");
        assert!(json.get("foundation").is_some());
        assert!(json.get("consumers").is_some());
        assert!(json["consumers"].is_array());
    }
}
