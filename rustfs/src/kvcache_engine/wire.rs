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

//! Messages between a node that received a request and the key's owner,
//! carried as bodies of the framework's internode engine call: a JSON
//! header, length prefixed, followed by raw value bytes.

use bytes::{Bytes, BytesMut};
use rustfs_bucket_engine::{EngineError, EngineResult};
use serde::{Deserialize, Serialize, de::DeserializeOwned};

pub(crate) const PEER_GET: &str = "peer-get";
pub(crate) const PEER_HEAD: &str = "peer-head";
pub(crate) const PEER_PUT: &str = "peer-put";
pub(crate) const PEER_DELETE: &str = "peer-delete";
pub(crate) const PEER_LIST: &str = "peer-list";
#[cfg(all(feature = "nixl", target_os = "linux"))]
pub(crate) const PEER_GET_RDMA: &str = "peer-get-rdma";
#[cfg(all(feature = "nixl", target_os = "linux"))]
pub(crate) const PEER_PUT_RDMA: &str = "peer-put-rdma";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct KeyRequest {
    pub key: String,
    /// Placement view version the sender routed with.
    pub view: u64,
}

/// A forwarded single-key operation whose data the owner moves to or from
/// the client over RDMA. The internode call is body bound, so the client's
/// agent metadata arrives authenticated.
#[cfg(all(feature = "nixl", target_os = "linux"))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RdmaKeyRequest {
    pub key: String,
    pub view: u64,
    /// Byte range of the entry to push (reads only).
    #[serde(default)]
    pub range: Option<(u64, u64)>,
    pub agent: String,
    pub metadata: String,
    pub buffer: String,
}

#[cfg(all(feature = "nixl", target_os = "linux"))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RdmaReply {
    pub entry: EntryHeader,
    pub moved: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ListRequest {
    pub prefix: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delimiter: Option<String>,
    pub start_after: Option<String>,
    pub max_keys: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct EntryHeader {
    pub key: String,
    pub size: u64,
    pub etag: String,
    pub last_modified_unix_secs: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ListReply {
    pub entries: Vec<EntryHeader>,
    /// Common prefixes, when listed with a delimiter.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub prefixes: Vec<String>,
    pub truncated: bool,
}

/// `[u32 header length][JSON header][payload]`.
pub(crate) fn encode<T: Serialize>(header: &T, payload: &[u8]) -> EngineResult<Bytes> {
    let header = serde_json::to_vec(header).map_err(|err| EngineError::Internal(err.to_string()))?;
    let mut out = BytesMut::with_capacity(4 + header.len() + payload.len());
    out.extend_from_slice(&(header.len() as u32).to_le_bytes());
    out.extend_from_slice(&header);
    out.extend_from_slice(payload);
    Ok(out.freeze())
}

/// Splits a message into its header and payload (zero copy). The header
/// length is only checked against the message itself: nothing is allocated
/// from it, and message sizes are bounded where they enter (the custom-route
/// body limit, the internode call). Headers legitimately reach about a
/// megabyte: a list reply of 1000 entries, a batch-write of many small items.
pub(crate) fn decode<T: DeserializeOwned>(message: Bytes) -> EngineResult<(T, Bytes)> {
    let invalid = |what: &str| EngineError::InvalidRequest(format!("malformed engine message: {what}"));
    if message.len() < 4 {
        return Err(invalid("no header length"));
    }
    let len = u32::from_le_bytes(message[..4].try_into().expect("four bytes")) as usize;
    if message.len() - 4 < len {
        return Err(invalid("header length"));
    }
    let header = serde_json::from_slice(&message[4..4 + len]).map_err(|err| invalid(&err.to_string()))?;
    Ok((header, message.slice(4 + len..)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn messages_round_trip_with_their_payload() {
        let request = KeyRequest {
            key: "hicache/page".to_owned(),
            view: 7,
        };
        let message = encode(&request, b"value bytes").expect("encode");
        let (decoded, payload): (KeyRequest, Bytes) = decode(message).expect("decode");
        assert_eq!(decoded, request);
        assert_eq!(&payload[..], b"value bytes");
    }

    #[test]
    fn malformed_messages_are_rejected() {
        assert!(decode::<KeyRequest>(Bytes::from_static(b"ab")).is_err());
        assert!(decode::<KeyRequest>(Bytes::from_static(&[9, 0, 0, 0, b'{'])).is_err());
        let wrong_shape = encode(&serde_json::json!({"key": "k", "view": 1, "extra": 2}), b"").expect("encode");
        assert!(decode::<KeyRequest>(wrong_shape).is_err());
        let huge = [0xff, 0xff, 0xff, 0x7f];
        assert!(decode::<KeyRequest>(Bytes::copy_from_slice(&huge)).is_err());
        let short = [5, 0, 0, 0, b'{', b'}'];
        assert!(decode::<KeyRequest>(Bytes::copy_from_slice(&short)).is_err());
    }

    /// A full S3 list page from one peer: 1000 entries with maximum-length
    /// (1024-byte) keys, a header of over a megabyte.
    #[test]
    fn full_list_pages_round_trip() {
        let reply = ListReply {
            entries: (0..1000)
                .map(|i| EntryHeader {
                    key: format!("{i:04}{}", "k".repeat(1020)),
                    size: 29 << 20,
                    etag: "0123456789abcdef0123456789abcdef".to_owned(),
                    last_modified_unix_secs: 1_762_700_000,
                })
                .collect(),
            prefixes: Vec::new(),
            truncated: true,
        };
        let message = encode(&reply, b"").expect("encode");
        assert!(message.len() > 1 << 20, "{} bytes", message.len());
        let (decoded, payload): (ListReply, Bytes) = decode(message).expect("decode");
        assert_eq!(decoded, reply);
        assert!(payload.is_empty());
    }
}
