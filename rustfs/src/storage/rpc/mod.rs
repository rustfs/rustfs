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

#[cfg(test)]
mod tests {
    use super::*;
    use rmp_serde::Deserializer;
    use serde::Deserialize;
    use std::collections::HashMap;
    use std::io::Cursor;

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Simple {
        name: String,
        count: u32,
    }

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Nested {
        label: String,
        tags: HashMap<String, String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        optional: Option<u64>,
    }

    #[test]
    fn encode_decode_round_trip() {
        let val = Simple {
            name: "rustfs".into(),
            count: 42,
        };
        let buf = encode_msgpack_map(&val).unwrap();
        let decoded: Simple = Deserialize::deserialize(&mut Deserializer::new(Cursor::new(&buf))).unwrap();
        assert_eq!(val, decoded);
    }

    #[test]
    fn encode_produces_map_not_array() {
        let val = Simple {
            name: "test".into(),
            count: 1,
        };
        let buf = encode_msgpack_map(&val).unwrap();
        // Map marker for 2 fields: fixmap with N=2 is 0x82
        assert_eq!(buf[0], 0x82, "expected msgpack fixmap marker, got array");
    }

    #[test]
    fn nested_struct_with_optional_and_hashmap() {
        let mut tags = HashMap::new();
        tags.insert("env".into(), "production".into());

        let val = Nested {
            label: "node1".into(),
            tags,
            optional: None,
        };
        let buf = encode_msgpack_map(&val).unwrap();
        let decoded: Nested = Deserialize::deserialize(&mut Deserializer::new(Cursor::new(&buf))).unwrap();
        assert_eq!(val, decoded);
    }
}
