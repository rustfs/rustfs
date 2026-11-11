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

//! Batch operations of the `kv-cache` engine, with values carried inline:
//! the client-facing custom operations (`prefix-query`, `batch-read`,
//! `batch-write`) and the per-owner messages they fan out as. Keys of one
//! request are grouped by owner and every owner is asked once, in parallel.

use bytes::{Bytes, BytesMut};
use rustfs_bucket_engine::{EngineError, EngineResult};
use serde::{Deserialize, Serialize};

pub(crate) const PREFIX_QUERY: &str = "prefix-query";
pub(crate) const BATCH_READ: &str = "batch-read";
pub(crate) const BATCH_WRITE: &str = "batch-write";
pub(crate) const PLACEMENT_VIEW: &str = "placement-view";

/// How owners are chosen, so clients can route keys to owners themselves:
/// xxh3-64 rendezvous hashing over every (node, drive) target of the view
/// (`rustfs_bucket_engine::TargetId::score`).
pub(crate) const PLACEMENT_HASH: &str = "xxh3-64-rendezvous-v1";

pub(crate) const PEER_CONTAINS: &str = "peer-contains";
pub(crate) const PEER_GET_BATCH: &str = "peer-get-batch";
pub(crate) const PEER_PUT_BATCH: &str = "peer-put-batch";

/// Most keys in one query or read request.
pub(crate) const MAX_BATCH_KEYS: usize = 4096;
/// Request body limits the framework enforces before the engine runs.
pub(crate) const MAX_KEYS_REQUEST_BYTES: u64 = 1 << 20;
/// Inline batch writes are for small entries: the S3 layer caps every
/// custom-route request body at 1 MiB (shared with admin routes). Bulk
/// writes use single-key PUT or, with NIXL, carry buffer descriptors only.
pub(crate) const MAX_WRITE_REQUEST_BYTES: u64 = 1 << 20;
pub(crate) const MAX_VIEW_REQUEST_BYTES: u64 = 1024;
/// Largest batch-read response; a client asks for more in several requests.
pub(crate) const MAX_READ_RESPONSE_BYTES: u64 = 1 << 30;

/// `prefix-query` and `batch-read` request: keys in prefix order.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct KeysRequest {
    pub keys: Vec<String>,
    /// Placement view version the client routed with; a request with an
    /// older view is refused so the client refreshes it.
    #[serde(default)]
    pub view: Option<u64>,
}

impl KeysRequest {
    pub(crate) fn validate(&self) -> EngineResult<()> {
        if self.keys.len() > MAX_BATCH_KEYS {
            return Err(EngineError::InvalidRequest(format!(
                "{} keys in one request; the limit is {MAX_BATCH_KEYS}",
                self.keys.len()
            )));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PrefixQueryReply {
    /// Number of leading keys stored.
    pub hits: usize,
    /// Whether each requested key is stored, in request order, so a client
    /// can also check keys that do not form one prefix (group ends, sidecar
    /// state) in a single request.
    pub present: Vec<bool>,
}

/// `batch-read` response header (followed by the values of the hits,
/// concatenated in key order) and owner reply to `peer-get-batch`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ValuesHeader {
    /// Size of each requested key's value, or `null` for a miss.
    pub sizes: Vec<Option<u64>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct WriteItem {
    pub key: String,
    pub size: u64,
}

/// `batch-write` request header (followed by the values, concatenated in
/// item order) and the owner message `peer-put-batch`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct WriteHeader {
    pub items: Vec<WriteItem>,
    #[serde(default)]
    pub view: Option<u64>,
}

/// `placement-view` reply: everything a client needs to route keys.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PlacementReply {
    pub version: u64,
    pub hash: String,
    pub replicas: u8,
    pub nodes: Vec<rustfs_bucket_engine::NodeTargets>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", tag = "result", content = "message")]
pub(crate) enum WriteResult {
    Written,
    /// Already stored; nothing was written.
    Exists,
    /// The owner's drive is full; the client skips this entry.
    CacheFull,
    /// The owner could not be reached or failed; the entry is not stored.
    Error(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct WriteReply {
    pub results: Vec<WriteResult>,
}

/// Per-owner key lookups: keys and the placement view they were routed by.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PeerKeys {
    pub keys: Vec<String>,
    pub view: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ContainsReply {
    pub present: Vec<bool>,
}

/// Per-owner batch write: items, values and the routing view.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PeerWrite {
    pub items: Vec<WriteItem>,
    pub view: u64,
}

/// Splits concatenated values into one slice per item (zero copy),
/// checking that the sizes cover the payload exactly.
pub(crate) fn split_values(items: &[WriteItem], payload: &Bytes) -> EngineResult<Vec<Bytes>> {
    let mut values = Vec::with_capacity(items.len());
    let mut at = 0usize;
    for item in items {
        let size = usize::try_from(item.size).map_err(|_| EngineError::InvalidRequest("value size".to_owned()))?;
        let end = at
            .checked_add(size)
            .filter(|end| *end <= payload.len())
            .ok_or_else(|| EngineError::InvalidRequest(format!("value of {} is shorter than its size", item.key)))?;
        values.push(payload.slice(at..end));
        at = end;
    }
    if at != payload.len() {
        return Err(EngineError::InvalidRequest(format!("{} bytes after the last value", payload.len() - at)));
    }
    Ok(values)
}

/// Joins read values into a header of sizes and the concatenated payload.
pub(crate) fn join_values(values: &[Option<Bytes>]) -> (ValuesHeader, Bytes) {
    let total: usize = values.iter().flatten().map(Bytes::len).sum();
    let mut payload = BytesMut::with_capacity(total);
    for value in values.iter().flatten() {
        payload.extend_from_slice(value);
    }
    let header = ValuesHeader {
        sizes: values
            .iter()
            .map(|value| value.as_ref().map(|value| value.len() as u64))
            .collect(),
    };
    (header, payload.freeze())
}

/// Splits a values payload back into one optional value per key.
pub(crate) fn unjoin_values(header: &ValuesHeader, payload: &Bytes) -> EngineResult<Vec<Option<Bytes>>> {
    let mut values = Vec::with_capacity(header.sizes.len());
    let mut at = 0usize;
    for size in &header.sizes {
        match size {
            None => values.push(None),
            Some(size) => {
                let size = usize::try_from(*size).map_err(|_| EngineError::InvalidRequest("value size".to_owned()))?;
                let end = at
                    .checked_add(size)
                    .filter(|end| *end <= payload.len())
                    .ok_or_else(|| EngineError::InvalidRequest("values are shorter than their sizes".to_owned()))?;
                values.push(Some(payload.slice(at..end)));
                at = end;
            }
        }
    }
    if at != payload.len() {
        return Err(EngineError::InvalidRequest("bytes after the last value".to_owned()));
    }
    Ok(values)
}

/// Number of leading `true`s.
pub(crate) fn longest_prefix(present: &[bool]) -> usize {
    present.iter().take_while(|present| **present).count()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn item(key: &str, size: u64) -> WriteItem {
        WriteItem {
            key: key.to_owned(),
            size,
        }
    }

    #[test]
    fn values_split_exactly_by_their_sizes() {
        let payload = Bytes::from_static(b"aaabbbbc");
        let values = split_values(&[item("a", 3), item("b", 4), item("c", 1)], &payload).expect("split");
        assert_eq!(values, [&b"aaa"[..], &b"bbbb"[..], &b"c"[..]]);

        assert!(split_values(&[item("a", 3), item("b", 9)], &payload).is_err(), "short payload");
        assert!(split_values(&[item("a", 3)], &payload).is_err(), "trailing bytes");
        assert!(split_values(&[item("a", u64::MAX)], &payload).is_err());
    }

    #[test]
    fn read_values_round_trip_with_misses() {
        let values = vec![
            Some(Bytes::from_static(b"one")),
            None,
            Some(Bytes::new()),
            Some(Bytes::from_static(b"3")),
        ];
        let (header, payload) = join_values(&values);
        assert_eq!(header.sizes, [Some(3), None, Some(0), Some(1)]);
        assert_eq!(unjoin_values(&header, &payload).expect("unjoin"), values);

        let short = ValuesHeader { sizes: vec![Some(10)] };
        assert!(unjoin_values(&short, &payload).is_err());
    }

    #[test]
    fn prefix_stops_at_the_first_miss_and_requests_are_bounded() {
        assert_eq!(longest_prefix(&[true, true, false, true]), 2);
        assert_eq!(longest_prefix(&[]), 0);
        let too_many = KeysRequest {
            keys: vec!["k".to_owned(); MAX_BATCH_KEYS + 1],
            view: None,
        };
        assert!(too_many.validate().is_err());
        assert_eq!(
            serde_json::to_value(WriteResult::Error("down".to_owned())).expect("json"),
            serde_json::json!({"result": "error", "message": "down"})
        );
        assert_eq!(
            serde_json::to_value(WriteResult::CacheFull).expect("json"),
            serde_json::json!({"result": "cache-full"})
        );
    }
}
