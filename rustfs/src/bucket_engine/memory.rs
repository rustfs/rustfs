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

//! `memory` engine: a node-local, non-durable engine that keeps objects in
//! process memory. It exercises the framework end to end (binding, dispatch,
//! listing, holds-data and purge) and is the reference engine for the
//! dispatch-overhead benchmark. A multi-node deployment sees a different
//! namespace on every node; use it on a single node only.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::RwLock;
use rustfs_bucket_engine::{
    BucketContext, BucketEngine, BucketEngineBinding, BucketFeature, ByteRange, CustomOperationSpec, CustomRequest,
    CustomResponse, EngineCapabilities, EngineDescriptor, EngineError, EngineName, EngineObject, EngineObjectInfo,
    EngineObjectOperation, EngineResult, ListName, ListPage, ListPageBuilder, ListQuery, ObjectWriteMeta, OperationAction,
    OperationName, RdmaTransfer,
};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

pub const MEMORY_ENGINE: &str = "memory";

/// Custom operation: the longest prefix of `keys` that is stored, the
/// lookup a KV cache client makes before reading a prompt's pages.
pub const PREFIX_QUERY_OPERATION: &str = "prefix-query";
const PREFIX_QUERY_MAX_REQUEST_BYTES: u64 = 4 * 1024 * 1024;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PrefixQueryRequest {
    keys: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PrefixQueryResponse {
    /// Number of leading keys that are stored.
    pub hits: usize,
    /// Total size of those entries.
    pub bytes: u64,
}

struct StoredObject {
    body: Bytes,
    info: EngineObjectInfo,
}

pub struct MemoryEngine {
    descriptor: EngineDescriptor,
    /// Objects by bucket incarnation, then key.
    buckets: RwLock<HashMap<u128, BTreeMap<String, StoredObject>>>,
}

impl Default for MemoryEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryEngine {
    pub fn new() -> Self {
        Self {
            descriptor: EngineDescriptor {
                name: EngineName::new(MEMORY_ENGINE).expect("static engine name is valid"),
                version: env!("CARGO_PKG_VERSION").to_owned(),
                config_version: 1,
                capabilities: EngineCapabilities {
                    object_operations: BTreeSet::from([
                        EngineObjectOperation::GetObject,
                        EngineObjectOperation::HeadObject,
                        EngineObjectOperation::PutObject,
                        EngineObjectOperation::DeleteObject,
                        EngineObjectOperation::ListObjects,
                    ]),
                    custom_operations: vec![CustomOperationSpec {
                        name: OperationName::new(PREFIX_QUERY_OPERATION).expect("static operation name is valid"),
                        action: OperationAction::GetObject,
                        max_request_bytes: PREFIX_QUERY_MAX_REQUEST_BYTES,
                    }],
                    consistent_listing: true,
                    refused_features: BucketFeature::ALL
                        .into_iter()
                        .filter(|f| *f != BucketFeature::Notifications)
                        .collect(),
                    rdma: false,
                },
            },
            buckets: RwLock::default(),
        }
    }
}

#[async_trait]
impl BucketEngine for MemoryEngine {
    fn descriptor(&self) -> &EngineDescriptor {
        &self.descriptor
    }

    fn validate_config(&self, binding: &BucketEngineBinding) -> EngineResult<()> {
        match binding.config.as_object() {
            Some(config) if !config.is_empty() => Err(EngineError::InvalidConfig("the memory engine takes no config".to_owned())),
            _ => Ok(()),
        }
    }

    async fn bind(&self, _bucket: &BucketContext) -> EngineResult<()> {
        Ok(())
    }

    async fn attach(&self, bucket: &BucketContext) -> EngineResult<()> {
        self.buckets.write().entry(bucket.incarnation).or_default();
        Ok(())
    }

    async fn reconfigure(&self, _bucket: &BucketContext, _previous: &BucketEngineBinding) -> EngineResult<()> {
        Ok(())
    }

    async fn holds_data(&self, bucket: &BucketContext) -> EngineResult<bool> {
        Ok(self
            .buckets
            .read()
            .get(&bucket.incarnation)
            .is_some_and(|objects| !objects.is_empty()))
    }

    async fn status(&self, bucket: &BucketContext) -> EngineResult<serde_json::Value> {
        let buckets = self.buckets.read();
        let (objects, bytes) = buckets.get(&bucket.incarnation).map_or((0, 0), |objects| {
            (objects.len(), objects.values().map(|object| object.info.size).sum::<u64>())
        });
        Ok(serde_json::json!({ "objects": objects, "bytes": bytes }))
    }

    async fn purge(&self, bucket: &BucketContext) -> EngineResult<()> {
        self.buckets.write().remove(&bucket.incarnation);
        Ok(())
    }

    async fn get_object(
        &self,
        bucket: &BucketContext,
        key: &str,
        range: Option<ByteRange>,
        _rdma: Option<RdmaTransfer>,
    ) -> EngineResult<EngineObject> {
        let buckets = self.buckets.read();
        let object = buckets
            .get(&bucket.incarnation)
            .and_then(|objects| objects.get(key))
            .ok_or(EngineError::NotFound)?;
        let body = match range {
            None => object.body.clone(),
            Some(range) => {
                let start = usize::try_from(range.start).unwrap_or(usize::MAX).min(object.body.len());
                let end = start
                    .saturating_add(usize::try_from(range.length).unwrap_or(usize::MAX))
                    .min(object.body.len());
                object.body.slice(start..end)
            }
        };
        Ok(EngineObject {
            info: object.info.clone(),
            body,
            rdma_bytes: None,
        })
    }

    async fn head_object(&self, bucket: &BucketContext, key: &str) -> EngineResult<EngineObjectInfo> {
        self.buckets
            .read()
            .get(&bucket.incarnation)
            .and_then(|objects| objects.get(key))
            .map(|object| object.info.clone())
            .ok_or(EngineError::NotFound)
    }

    async fn put_object(
        &self,
        bucket: &BucketContext,
        key: &str,
        body: Bytes,
        _meta: ObjectWriteMeta,
    ) -> EngineResult<EngineObjectInfo> {
        let info = EngineObjectInfo {
            key: key.to_owned(),
            size: body.len() as u64,
            etag: format!("{:032x}", xxhash_rust::xxh3::xxh3_128(&body)),
            last_modified_unix_secs: OffsetDateTime::now_utc().unix_timestamp(),
        };
        let mut buckets = self.buckets.write();
        let objects = buckets
            .get_mut(&bucket.incarnation)
            .ok_or_else(|| EngineError::Unavailable("bucket is not attached".to_owned()))?;
        objects.insert(
            key.to_owned(),
            StoredObject {
                body,
                info: info.clone(),
            },
        );
        Ok(info)
    }

    async fn delete_object(&self, bucket: &BucketContext, key: &str) -> EngineResult<()> {
        self.buckets
            .write()
            .get_mut(&bucket.incarnation)
            .and_then(|objects| objects.remove(key))
            .map(|_| ())
            .ok_or(EngineError::NotFound)
    }

    async fn custom_operation(&self, bucket: &BucketContext, request: CustomRequest) -> EngineResult<CustomResponse> {
        if request.operation.as_str() != PREFIX_QUERY_OPERATION {
            return Err(EngineError::UnknownOperation {
                engine: self.descriptor.name.clone(),
                operation: request.operation,
            });
        }
        let query: PrefixQueryRequest =
            serde_json::from_slice(&request.body).map_err(|err| EngineError::InvalidRequest(err.to_string()))?;
        let buckets = self.buckets.read();
        let objects = buckets.get(&bucket.incarnation);
        let mut response = PrefixQueryResponse { hits: 0, bytes: 0 };
        for key in &query.keys {
            let Some(object) = objects.and_then(|objects| objects.get(key)) else {
                break;
            };
            response.hits += 1;
            response.bytes += object.info.size;
        }
        let body = serde_json::to_vec(&response).map_err(|err| EngineError::Internal(err.to_string()))?;
        Ok(CustomResponse {
            body: Bytes::from(body),
            headers: vec![("x-rustfs-engine-hits".to_owned(), response.hits.to_string())],
        })
    }

    async fn list_objects(&self, bucket: &BucketContext, query: &ListQuery<'_>) -> EngineResult<ListPage> {
        let buckets = self.buckets.read();
        let Some(objects) = buckets.get(&bucket.incarnation) else {
            return Ok(ListPage::default());
        };
        let mut page = ListPageBuilder::new(query.max_keys);
        let in_prefix = objects
            .range::<str, _>((std::ops::Bound::Included(query.prefix), std::ops::Bound::Unbounded))
            .take_while(|(key, _)| key.starts_with(query.prefix));
        for (key, object) in in_prefix {
            match query.name_of(key) {
                Some(name) if !query.is_after_start(name.as_str()) => {}
                Some(ListName::Prefix(prefix)) => page.push_prefix(prefix.to_owned()),
                Some(ListName::Key(_)) => page.push_object(object.info.clone()),
                None => {}
            }
        }
        Ok(page.finish())
    }
}
