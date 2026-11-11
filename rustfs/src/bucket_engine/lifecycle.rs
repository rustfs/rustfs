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

//! Bucket lifecycle hooks: the CreateBucket `x-rustfs-bucket-engine` header
//! and the DeleteBucket engine check and purge.

use http::HeaderMap;
use rustfs_bucket_engine::{BucketEngineBinding, BucketFeature, CREATE_BUCKET_ENGINE_HEADER, EngineName};
use s3s::{S3Error, S3ErrorCode, S3Result, s3_error};

use super::binding::{RequestedBinding, engine_error, resolve_binding};
use super::{EngineRegistry, EngineRoute, peer};
use tracing::warn;

const EVENT_BUCKET_ENGINE_PURGE: &str = "bucket_engine_purge";
const LOG_COMPONENT_APP: &str = "app";
const LOG_SUBSYSTEM_BUCKET_ENGINE: &str = "bucket_engine";

/// The binding a CreateBucket request asks for through the
/// `x-rustfs-bucket-engine` header (an engine name; the engine's default
/// config at its current config version). Checked before the bucket is
/// created so an invalid request creates nothing. `None` for the object
/// engine.
pub(crate) fn create_bucket_binding(
    registry: &EngineRegistry,
    headers: &HeaderMap,
    object_lock: bool,
) -> S3Result<Option<RequestedBinding>> {
    let Some(value) = headers.get(CREATE_BUCKET_ENGINE_HEADER) else {
        return Ok(None);
    };
    let name = value
        .to_str()
        .map_err(|_| s3_error!(InvalidArgument, "{} must be an engine name", CREATE_BUCKET_ENGINE_HEADER))?
        .trim();
    let engine =
        EngineName::new(name).map_err(|err| s3_error!(InvalidArgument, "invalid {}: {}", CREATE_BUCKET_ENGINE_HEADER, err))?;
    let Some(implementation) = registry.engine(&engine) else {
        if engine.as_str() == rustfs_bucket_engine::OBJECT_ENGINE {
            return Ok(None);
        }
        return Err(s3_error!(InvalidArgument, "engine {} is not available on this node", engine.as_str()));
    };
    let binding = BucketEngineBinding {
        config_version: implementation.descriptor().config_version,
        engine,
        config: serde_json::Value::Null,
    };
    let requested = resolve_binding(registry, binding)?;
    if object_lock && requested.engine.descriptor().capabilities.refuses(BucketFeature::ObjectLock) {
        return Err(s3_error!(
            InvalidRequest,
            "engine {} refuses buckets with object lock enabled",
            requested.binding.engine.as_str()
        ));
    }
    Ok(Some(requested))
}

/// Runs before the object layer deletes `bucket`. An engine-bound bucket is
/// refused while its engine holds data on any node (or a node cannot say)
/// unless the delete is forced; the engine's data for the bucket is then
/// purged on every node.
pub(crate) async fn before_delete_bucket(registry: &EngineRegistry, bucket: &str, force: bool) -> S3Result<()> {
    match registry.route(bucket) {
        EngineRoute::Object => Ok(()),
        EngineRoute::Engine(bound) => {
            if !force
                && (bound.engine.holds_data(&bound.context).await.map_err(engine_error)?
                    || peer::any_peer_holds_data(&bound).await.map_err(engine_error)?)
            {
                return Err(S3Error::with_message(
                    S3ErrorCode::BucketNotEmpty,
                    format!("engine {} still holds data for this bucket", bound.context.binding.engine.as_str()),
                ));
            }
            bound.engine.purge(&bound.context).await.map_err(engine_error)?;
            registry.remove_engine_dirs(&bound.context).await.map_err(engine_error)?;
            for failed in peer::purge_on_peers(&bound).await {
                if let Err(err) = failed.result {
                    warn!(
                        event = EVENT_BUCKET_ENGINE_PURGE,
                        component = LOG_COMPONENT_APP,
                        subsystem = LOG_SUBSYSTEM_BUCKET_ENGINE,
                        result = "peer_failed",
                        bucket = %bucket,
                        peer = %failed.peer,
                        error = %err,
                        "Bucket engine purge failed on a peer; its files are left for the engine to collect"
                    );
                }
            }
            Ok(())
        }
        // A forced delete of a bucket whose engine is unavailable here
        // proceeds; the engine's files are left for the engine to collect.
        EngineRoute::Unavailable { .. } if force => Ok(()),
        EngineRoute::Unavailable { engine, reason } => Err(S3Error::with_message(
            S3ErrorCode::ServiceUnavailable,
            format!("bucket engine {engine} is unavailable: {reason}"),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bucket_engine::memory::{MEMORY_ENGINE, MemoryEngine};
    use bytes::Bytes;
    use http::HeaderValue;
    use rustfs_bucket_engine::ObjectWriteMeta;
    use std::sync::Arc;

    fn registry() -> Arc<EngineRegistry> {
        let registry = Arc::new(EngineRegistry::new());
        assert!(registry.register_engine(Arc::new(MemoryEngine::new())));
        registry
    }

    fn headers(engine: &'static str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(CREATE_BUCKET_ENGINE_HEADER, HeaderValue::from_static(engine));
        headers
    }

    fn code<T>(result: S3Result<T>) -> S3ErrorCode {
        result.err().expect("expected an error").code().clone()
    }

    #[test]
    fn create_bucket_header_resolves_engine_defaults() {
        let registry = registry();
        assert!(
            create_bucket_binding(&registry, &HeaderMap::new(), false)
                .expect("no header")
                .is_none()
        );
        assert!(
            create_bucket_binding(&registry, &headers("object"), false)
                .expect("object")
                .is_none()
        );

        let requested = create_bucket_binding(&registry, &headers(MEMORY_ENGINE), false)
            .expect("memory")
            .expect("binding");
        assert_eq!(requested.binding.engine.as_str(), MEMORY_ENGINE);
        assert_eq!(requested.binding.config_version, 1);
        assert!(requested.binding.config.is_null());

        assert_eq!(
            code(create_bucket_binding(&registry, &headers("kv-cache"), false)),
            S3ErrorCode::InvalidArgument
        );
        assert_eq!(
            code(create_bucket_binding(&registry, &headers("Bad Name"), false)),
            S3ErrorCode::InvalidArgument
        );
        // The memory engine refuses object lock.
        assert_eq!(
            code(create_bucket_binding(&registry, &headers(MEMORY_ENGINE), true)),
            S3ErrorCode::InvalidRequest
        );
    }

    #[tokio::test]
    async fn delete_bucket_refuses_engine_data_unless_forced_and_purges() {
        let registry = registry();
        let binding = br#"{"engine":"memory","config_version":1}"#;
        registry
            .publish("kv", Some((binding, 7)))
            .expect("binding changed")
            .await
            .expect("reconcile task");
        let EngineRoute::Engine(bound) = registry.route("kv") else {
            panic!("bucket should be attached");
        };

        bound
            .engine
            .put_object(&bound.context, "page", Bytes::from_static(b"x"), ObjectWriteMeta::default())
            .await
            .expect("put");
        assert_eq!(code(before_delete_bucket(&registry, "kv", false).await), S3ErrorCode::BucketNotEmpty);
        assert!(bound.engine.holds_data(&bound.context).await.expect("holds data"));

        before_delete_bucket(&registry, "kv", true).await.expect("forced delete");
        assert!(!bound.engine.holds_data(&bound.context).await.expect("holds data"));
        // An empty engine bucket deletes without force.
        before_delete_bucket(&registry, "kv", false)
            .await
            .expect("empty engine bucket deletes");
        before_delete_bucket(&registry, "plain", false)
            .await
            .expect("object buckets are untouched");
    }

    #[tokio::test]
    async fn delete_bucket_with_unavailable_engine_needs_force() {
        let registry = registry();
        registry
            .publish("kv", Some((br#"{"engine":"kv-cache","config_version":1}"#, 7)))
            .expect("binding changed")
            .await
            .expect("reconcile task");
        assert_eq!(code(before_delete_bucket(&registry, "kv", false).await), S3ErrorCode::ServiceUnavailable);
        before_delete_bucket(&registry, "kv", true)
            .await
            .expect("forced delete proceeds");
    }
}
