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

use crate::binding::BucketEngineBinding;
use crate::descriptor::EngineDescriptor;
use crate::error::{EngineError, EngineResult};
use crate::listing::{ListPage, ListQuery};
use crate::name::OperationName;
use async_trait::async_trait;
use bytes::Bytes;

/// The bucket a call is about: its name, the incarnation the binding belongs
/// to, and the binding itself. Engines key their state by incarnation so a
/// deleted and recreated bucket never sees the old bucket's data.
#[derive(Debug, Clone, PartialEq)]
pub struct BucketContext {
    pub bucket: String,
    pub incarnation: u128,
    pub binding: BucketEngineBinding,
}

/// The client side of an RDMA transfer, as carried by the `x-rustfs-nixl-*`
/// request headers: the client's NIXL agent name, its serialized agent
/// metadata, and its buffer descriptor. The framework parses and bounds these
/// before an engine sees them; engines pass them to the server's NIXL service.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RdmaTransfer {
    pub agent: String,
    pub metadata: String,
    pub buffer: String,
}

/// An inclusive start and a length in bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ByteRange {
    pub start: u64,
    pub length: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EngineObjectInfo {
    pub key: String,
    pub size: u64,
    pub etag: String,
    pub last_modified_unix_secs: i64,
}

/// A read result. When the read was served over RDMA, `body` is empty and
/// `rdma_bytes` reports how many bytes were moved into the client's buffer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EngineObject {
    pub info: EngineObjectInfo,
    pub body: Bytes,
    pub rdma_bytes: Option<u64>,
}

/// How a PUT delivers its value: inline in `body`, or by RDMA from the
/// client's buffer when `rdma` is set (then `body` is empty and
/// `content_length` is the size to pull).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ObjectWriteMeta {
    pub content_length: u64,
    pub rdma: Option<RdmaTransfer>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CustomRequest {
    pub operation: OperationName,
    pub body: Bytes,
    pub rdma: Option<RdmaTransfer>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CustomResponse {
    pub body: Bytes,
    pub headers: Vec<(String, String)>,
}

/// A bucket engine. The framework authenticates and authorizes every request
/// before calling an engine, refuses object operations the engine's descriptor
/// does not declare, and serializes nothing on the engine's behalf: engines
/// must be safe under concurrent calls for the same bucket and key.
#[async_trait]
pub trait BucketEngine: Send + Sync + 'static {
    fn descriptor(&self) -> &EngineDescriptor;

    /// Accept or refuse a binding's config. Called before a binding is stored.
    fn validate_config(&self, binding: &BucketEngineBinding) -> EngineResult<()>;

    /// A bucket incarnation was bound to this engine; create its state.
    async fn bind(&self, bucket: &BucketContext) -> EngineResult<()>;

    /// A bound bucket was loaded (startup or peer reload); recover its state.
    async fn attach(&self, bucket: &BucketContext) -> EngineResult<()>;

    /// The config of a bound bucket changed from `previous`.
    async fn reconfigure(&self, bucket: &BucketContext, previous: &BucketEngineBinding) -> EngineResult<()>;

    /// Whether the bucket holds data, for delete-bucket emptiness checks.
    async fn holds_data(&self, bucket: &BucketContext) -> EngineResult<bool>;

    /// Remove all of the bucket incarnation's data on this node.
    async fn purge(&self, bucket: &BucketContext) -> EngineResult<()>;

    /// The bucket is going away or the server is shutting down; stop its workers.
    async fn detach(&self, _bucket: &BucketContext) -> EngineResult<()> {
        Ok(())
    }

    /// A side-effect-free snapshot of this node's state for the bucket
    /// (usage, worker progress), shown by the admin API. Must not block on
    /// I/O for long or change any state.
    async fn status(&self, _bucket: &BucketContext) -> EngineResult<serde_json::Value> {
        Ok(serde_json::Value::Null)
    }

    async fn get_object(
        &self,
        _bucket: &BucketContext,
        _key: &str,
        _range: Option<ByteRange>,
        _rdma: Option<RdmaTransfer>,
    ) -> EngineResult<EngineObject> {
        Err(self.unsupported("GetObject"))
    }

    async fn head_object(&self, _bucket: &BucketContext, _key: &str) -> EngineResult<EngineObjectInfo> {
        Err(self.unsupported("HeadObject"))
    }

    async fn put_object(
        &self,
        _bucket: &BucketContext,
        _key: &str,
        _body: Bytes,
        _meta: ObjectWriteMeta,
    ) -> EngineResult<EngineObjectInfo> {
        Err(self.unsupported("PutObject"))
    }

    async fn delete_object(&self, _bucket: &BucketContext, _key: &str) -> EngineResult<()> {
        Err(self.unsupported("DeleteObject"))
    }

    /// One page of keys in key order; with a delimiter, keys that contain it
    /// after the prefix are listed once as their common prefix.
    async fn list_objects(&self, _bucket: &BucketContext, _query: &ListQuery<'_>) -> EngineResult<ListPage> {
        Err(self.unsupported("ListObjects"))
    }

    async fn custom_operation(&self, _bucket: &BucketContext, request: CustomRequest) -> EngineResult<CustomResponse> {
        Err(EngineError::UnknownOperation {
            engine: self.descriptor().name.clone(),
            operation: request.operation,
        })
    }

    /// A message from a peer node's instance of this engine, delivered over the
    /// framework's signed internode channel.
    async fn handle_peer_call(&self, _bucket: &BucketContext, operation: &OperationName, _body: Bytes) -> EngineResult<Bytes> {
        Err(EngineError::UnknownOperation {
            engine: self.descriptor().name.clone(),
            operation: operation.clone(),
        })
    }

    #[doc(hidden)]
    fn unsupported(&self, operation: &str) -> EngineError {
        EngineError::Unsupported {
            engine: self.descriptor().name.clone(),
            operation: operation.to_owned(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::descriptor::EngineCapabilities;
    use crate::name::EngineName;

    struct LifecycleOnly(EngineDescriptor);

    #[async_trait]
    impl BucketEngine for LifecycleOnly {
        fn descriptor(&self) -> &EngineDescriptor {
            &self.0
        }
        fn validate_config(&self, _binding: &BucketEngineBinding) -> EngineResult<()> {
            Ok(())
        }
        async fn bind(&self, _bucket: &BucketContext) -> EngineResult<()> {
            Ok(())
        }
        async fn attach(&self, _bucket: &BucketContext) -> EngineResult<()> {
            Ok(())
        }
        async fn reconfigure(&self, _bucket: &BucketContext, _previous: &BucketEngineBinding) -> EngineResult<()> {
            Ok(())
        }
        async fn holds_data(&self, _bucket: &BucketContext) -> EngineResult<bool> {
            Ok(false)
        }
        async fn purge(&self, _bucket: &BucketContext) -> EngineResult<()> {
            Ok(())
        }
    }

    fn engine() -> (LifecycleOnly, BucketContext) {
        let name = EngineName::new("test-engine").expect("valid engine name");
        let engine = LifecycleOnly(EngineDescriptor {
            name: name.clone(),
            version: "0.0.0".to_owned(),
            config_version: 1,
            capabilities: EngineCapabilities::default(),
        });
        let bucket = BucketContext {
            bucket: "b".to_owned(),
            incarnation: 7,
            binding: BucketEngineBinding {
                engine: name,
                config_version: 1,
                config: serde_json::Value::Null,
            },
        };
        (engine, bucket)
    }

    #[tokio::test]
    async fn undeclared_operations_default_to_unsupported() {
        let (engine, bucket) = engine();
        assert!(matches!(
            engine.get_object(&bucket, "k", None, None).await,
            Err(EngineError::Unsupported { ref operation, .. }) if operation == "GetObject"
        ));
        assert!(matches!(engine.head_object(&bucket, "k").await, Err(EngineError::Unsupported { .. })));
        assert!(matches!(
            engine
                .put_object(&bucket, "k", Bytes::new(), ObjectWriteMeta::default())
                .await,
            Err(EngineError::Unsupported { .. })
        ));
        assert!(matches!(engine.delete_object(&bucket, "k").await, Err(EngineError::Unsupported { .. })));
        assert!(matches!(
            engine
                .list_objects(
                    &bucket,
                    &ListQuery {
                        max_keys: 10,
                        ..ListQuery::default()
                    }
                )
                .await,
            Err(EngineError::Unsupported { .. })
        ));
        let op = OperationName::new("prefix-query").expect("valid operation name");
        let request = CustomRequest {
            operation: op.clone(),
            body: Bytes::new(),
            rdma: None,
        };
        assert!(matches!(
            engine.custom_operation(&bucket, request).await,
            Err(EngineError::UnknownOperation { operation, .. }) if operation == op
        ));
        assert!(matches!(
            engine.handle_peer_call(&bucket, &op, Bytes::new()).await,
            Err(EngineError::UnknownOperation { .. })
        ));
        assert!(engine.detach(&bucket).await.is_ok());
    }

    #[test]
    fn engines_are_object_safe() {
        let (engine, _) = engine();
        let shared: std::sync::Arc<dyn BucketEngine> = std::sync::Arc::new(engine);
        assert_eq!(shared.descriptor().name.as_str(), "test-engine");
    }
}
