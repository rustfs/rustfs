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

pub trait TlsConsumerStatusSource {
    fn consumer_name(&self) -> &'static str;
    fn generation(&self) -> u64;
    fn has_root_ca(&self) -> bool;
    fn has_mtls_identity(&self) -> bool;

    fn into_status_item(self) -> TlsConsumerStatusItem
    where
        Self: Sized,
    {
        TlsConsumerStatusItem {
            consumer: self.consumer_name(),
            generation: self.generation(),
            has_root_ca: self.has_root_ca(),
            has_mtls_identity: self.has_mtls_identity(),
        }
    }
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
    pub fn push_consumer<S>(mut self, source: S) -> Self
    where
        S: TlsConsumerStatusSource,
    {
        self.consumers.push(source.into_status_item());
        self
    }

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
    use super::{TlsConsumerStatusSource, TlsDebugStatusResponse};
    use crate::state::{
        TlsRuntimeConsumerSection, TlsRuntimeOutboundSection, TlsRuntimeRuntimeSection, TlsRuntimeServerSection,
        TlsRuntimeStatusSnapshot,
    };

    #[derive(Clone, Copy)]
    struct TestConsumer;

    impl TlsConsumerStatusSource for TestConsumer {
        fn consumer_name(&self) -> &'static str {
            "test_consumer"
        }

        fn generation(&self) -> u64 {
            7
        }

        fn has_root_ca(&self) -> bool {
            true
        }

        fn has_mtls_identity(&self) -> bool {
            false
        }
    }

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
            .push_consumers([TestConsumer.into_status_item()])
            .build();

        let json = serde_json::to_value(response).expect("response should serialize");
        assert!(json.get("foundation").is_some());
        assert!(json.get("consumers").is_some());
        assert!(json["consumers"].is_array());
    }
}
