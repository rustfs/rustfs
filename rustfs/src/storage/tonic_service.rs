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

pub(crate) use crate::storage::rpc::node_service::make_heal_control_server_with_cache;
#[cfg(test)]
pub(crate) use crate::storage::rpc::node_service::{heal::heal_topology_fingerprint, make_heal_control_server_for_source};
pub use crate::storage::rpc::{make_heal_control_server, make_server, make_tier_mutation_control_server};
#[allow(dead_code)]
pub type NodeService = crate::storage::rpc::NodeService;
