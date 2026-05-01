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

pub mod http_service;
pub mod node_service;

pub use http_service::InternodeRpcService;
pub use node_service::{NodeService, make_server};

use rmp_serde::Serializer;
use serde::Serialize;

/// Encode a value as map-keyed msgpack for internode RPC responses.
///
/// Uses `.with_struct_map()` so structs are serialized with named fields
/// (msgpack map) instead of positional arrays. This matches what the
/// client-side `Deserializer::new()` expects.
pub(crate) fn encode_msgpack_map<T: Serialize>(value: &T) -> Result<Vec<u8>, rmp_serde::encode::Error> {
    let mut buf = Vec::new();
    value.serialize(&mut Serializer::new(&mut buf).with_struct_map())?;
    Ok(buf)
}
