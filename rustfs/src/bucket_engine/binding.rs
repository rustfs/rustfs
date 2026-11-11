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

//! Binding a bucket to an engine: validation against the engine and the
//! bucket, the empty-bucket rule, and the `engine.json` metadata write.
//! Shared by the admin API and CreateBucket's `x-rustfs-bucket-engine`
//! header.

use std::sync::Arc;

use rustfs_bucket_engine::{BucketContext, BucketEngine, BucketEngineBinding, BucketFeature, EngineError};
use s3s::{S3Error, S3ErrorCode, S3Result, s3_error};

use super::storage_api::{
    BUCKET_ENGINE_CONFIG, BucketMetadata, ECStore, ListOperations as _, capture_bucket_metadata_incarnation,
    delete_if_incarnation, get_bucket_metadata, update_if_incarnation,
};
use super::{EngineRegistry, peer};
use crate::bucket_engine::EngineRoute;

/// Bucket state a binding change is checked against.
#[derive(Debug, Default)]
pub(crate) struct BucketState {
    pub current: Option<BucketEngineBinding>,
    pub versioning: bool,
    pub object_lock: bool,
    pub table_bucket: bool,
}

impl BucketState {
    fn from_metadata(meta: &BucketMetadata) -> S3Result<Self> {
        let current = meta
            .engine_config()
            .map(|(bytes, _)| BucketEngineBinding::from_slice(bytes))
            .transpose()
            .map_err(|err| s3_error!(InternalError, "stored bucket engine binding is unreadable: {}", err))?
            .filter(|binding| !binding.is_object_engine());
        Ok(Self {
            current,
            versioning: meta.versioning(),
            object_lock: meta.object_locking(),
            table_bucket: meta.table_bucket_enabled(),
        })
    }

    fn enabled_features(&self) -> impl Iterator<Item = BucketFeature> + '_ {
        [
            (BucketFeature::Versioning, self.versioning),
            (BucketFeature::ObjectLock, self.object_lock),
            (BucketFeature::TableCatalog, self.table_bucket),
        ]
        .into_iter()
        .filter_map(|(feature, enabled)| enabled.then_some(feature))
    }
}

/// What a valid binding request does.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum BindPlan {
    /// The bucket already has this binding.
    Unchanged,
    /// First binding of an object-engine bucket; the bucket must be empty.
    Bind,
    /// New config for the bucket's current engine.
    Reconfigure,
}

/// A requested binding resolved to a registered engine.
pub(crate) struct RequestedBinding {
    pub binding: BucketEngineBinding,
    pub engine: Arc<dyn BucketEngine>,
}

/// Parses a binding from a client and checks it against the engine
/// registered under its name.
pub(crate) fn requested_binding(registry: &EngineRegistry, bytes: &[u8]) -> S3Result<RequestedBinding> {
    resolve_binding(registry, BucketEngineBinding::from_slice(bytes).map_err(engine_error)?)
}

/// Checks a binding against the engine registered under its name.
pub(crate) fn resolve_binding(registry: &EngineRegistry, binding: BucketEngineBinding) -> S3Result<RequestedBinding> {
    if binding.is_object_engine() {
        return Err(s3_error!(
            InvalidArgument,
            "the object engine is the default; remove the binding to return a bucket to it"
        ));
    }
    let engine = registry
        .engine(&binding.engine)
        .ok_or_else(|| s3_error!(InvalidArgument, "engine {} is not available on this node", binding.engine.as_str()))?;
    let descriptor = engine.descriptor();
    if binding.config_version > descriptor.config_version {
        return Err(s3_error!(
            InvalidArgument,
            "engine {} accepts config versions up to {}, got {}",
            descriptor.name.as_str(),
            descriptor.config_version,
            binding.config_version
        ));
    }
    engine.validate_config(&binding).map_err(engine_error)?;
    Ok(RequestedBinding { binding, engine })
}

/// Checks a requested binding against the bucket. The emptiness of the
/// bucket is checked separately for [`BindPlan::Bind`].
pub(crate) fn plan_binding(requested: &RequestedBinding, state: &BucketState) -> S3Result<BindPlan> {
    let binding = &requested.binding;
    match &state.current {
        Some(current) if current == binding => Ok(BindPlan::Unchanged),
        Some(current) if current.engine == binding.engine => Ok(BindPlan::Reconfigure),
        Some(current) => Err(s3_error!(
            InvalidRequest,
            "bucket is bound to engine {}; remove that binding before binding engine {}",
            current.engine.as_str(),
            binding.engine.as_str()
        )),
        None => {
            let capabilities = &requested.engine.descriptor().capabilities;
            if let Some(feature) = state.enabled_features().find(|feature| capabilities.refuses(*feature)) {
                return Err(s3_error!(
                    InvalidRequest,
                    "engine {} refuses buckets with {:?} enabled",
                    binding.engine.as_str(),
                    feature
                ));
            }
            Ok(BindPlan::Bind)
        }
    }
}

pub(crate) fn engine_error(err: EngineError) -> S3Error {
    let code = match &err {
        EngineError::InvalidBinding(_) | EngineError::InvalidConfig(_) | EngineError::InvalidRequest(_) => {
            S3ErrorCode::InvalidArgument
        }
        EngineError::NotFound => S3ErrorCode::NoSuchKey,
        EngineError::BucketNotEmpty => S3ErrorCode::BucketNotEmpty,
        EngineError::Unavailable(_) | EngineError::StalePlacement { .. } | EngineError::CacheFull => {
            S3ErrorCode::ServiceUnavailable
        }
        _ => S3ErrorCode::InternalError,
    };
    S3Error::with_message(code, err.to_string())
}

pub(crate) async fn bucket_state(bucket: &str) -> S3Result<BucketState> {
    let meta = get_bucket_metadata(bucket)
        .await
        .map_err(|e| s3_error!(NoSuchBucket, "failed to read bucket metadata: {}", e))?;
    BucketState::from_metadata(&meta)
}

async fn bucket_has_objects(store: Arc<ECStore>, bucket: &str) -> S3Result<bool> {
    let page = store
        .list_object_versions(bucket, "", None, None, None, 1)
        .await
        .map_err(|e| s3_error!(InternalError, "failed to list bucket: {}", e))?;
    Ok(!page.objects.is_empty() || !page.prefixes.is_empty())
}

/// Binds `bucket` to the requested engine, or changes the config of its
/// current engine. Binding needs an empty bucket. Returns whether the stored
/// binding changed; callers notify peers when it did.
pub(crate) async fn set_binding(store: Arc<ECStore>, bucket: &str, requested: RequestedBinding) -> S3Result<bool> {
    let expected_incarnation_id = capture_bucket_metadata_incarnation(bucket)
        .await
        .map_err(|e| s3_error!(InternalError, "failed to capture bucket incarnation: {}", e))?;
    if expected_incarnation_id.is_nil() {
        return Err(s3_error!(InvalidRequest, "bucket has no incarnation id and cannot be bound to an engine"));
    }
    let state = bucket_state(bucket).await?;
    match plan_binding(&requested, &state)? {
        BindPlan::Unchanged => return Ok(false),
        BindPlan::Reconfigure => {}
        BindPlan::Bind => {
            if bucket_has_objects(store, bucket).await? {
                return Err(s3_error!(BucketNotEmpty, "only an empty bucket can be bound to an engine"));
            }
            // Every node must serve the bucket's requests; a node without the
            // engine would answer them from an empty object namespace.
            peer::require_engine_on_every_node(requested.binding.engine.as_str())
                .await
                .map_err(engine_error)?;
            let context = BucketContext {
                bucket: bucket.to_owned(),
                incarnation: expected_incarnation_id.as_u128(),
                binding: requested.binding.clone(),
            };
            requested.engine.bind(&context).await.map_err(engine_error)?;
        }
    }

    let bytes = requested.binding.to_vec().map_err(engine_error)?;
    // System buckets are rejected by the metadata layer; a bucket deleted or
    // recreated since the capture fails the incarnation check.
    update_if_incarnation(bucket, BUCKET_ENGINE_CONFIG, bytes, expected_incarnation_id)
        .await
        .map_err(|e| s3_error!(InternalError, "failed to store bucket engine binding: {}", e))?;
    Ok(true)
}

/// Returns `bucket` to the object engine. Refused while this node cannot
/// prove, through the attached engine, that the engine holds no data for it.
/// Returns the removed binding, `None` when the bucket had none.
pub(crate) async fn clear_binding(registry: &EngineRegistry, bucket: &str) -> S3Result<Option<BucketEngineBinding>> {
    let expected_incarnation_id = capture_bucket_metadata_incarnation(bucket)
        .await
        .map_err(|e| s3_error!(InternalError, "failed to capture bucket incarnation: {}", e))?;
    let Some(current) = bucket_state(bucket).await?.current else {
        return Ok(None);
    };

    match registry.route(bucket) {
        EngineRoute::Engine(bound) if bound.context.incarnation == expected_incarnation_id.as_u128() => {
            if bound.engine.holds_data(&bound.context).await.map_err(engine_error)? {
                return Err(s3_error!(
                    BucketNotEmpty,
                    "engine {} still holds data for this bucket",
                    current.engine.as_str()
                ));
            }
        }
        EngineRoute::Unavailable { reason, .. } => {
            return Err(s3_error!(
                ServiceUnavailable,
                "engine {} is unavailable on this node: {}",
                current.engine.as_str(),
                reason
            ));
        }
        _ => {
            return Err(s3_error!(
                ServiceUnavailable,
                "engine {} is not attached for this bucket on this node yet",
                current.engine.as_str()
            ));
        }
    }

    delete_if_incarnation(bucket, BUCKET_ENGINE_CONFIG, expected_incarnation_id)
        .await
        .map_err(|e| s3_error!(InternalError, "failed to clear bucket engine binding: {}", e))?;
    Ok(Some(current))
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use rustfs_bucket_engine::{EngineCapabilities, EngineDescriptor, EngineName, EngineResult};
    use std::collections::BTreeSet;

    struct StubEngine(EngineDescriptor);

    #[async_trait]
    impl BucketEngine for StubEngine {
        fn descriptor(&self) -> &EngineDescriptor {
            &self.0
        }

        fn validate_config(&self, binding: &BucketEngineBinding) -> EngineResult<()> {
            if binding.config.get("invalid").is_some() {
                return Err(EngineError::InvalidConfig("rejected by stub engine".to_owned()));
            }
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

    fn registry() -> EngineRegistry {
        let registry = EngineRegistry::new();
        let descriptor = EngineDescriptor {
            name: EngineName::new("kv-cache").expect("valid engine name"),
            version: "test".to_owned(),
            config_version: 2,
            capabilities: EngineCapabilities {
                refused_features: BTreeSet::from([BucketFeature::Versioning, BucketFeature::ObjectLock]),
                ..EngineCapabilities::default()
            },
        };
        assert!(registry.register_engine(Arc::new(StubEngine(descriptor))));
        registry
    }

    fn request(registry: &EngineRegistry, body: &str) -> S3Result<RequestedBinding> {
        requested_binding(registry, body.as_bytes())
    }

    fn code(err: S3Error) -> S3ErrorCode {
        err.code().clone()
    }

    #[test]
    fn requests_are_checked_against_the_registered_engine() {
        let registry = registry();
        assert!(request(&registry, r#"{"engine":"kv-cache","config_version":2,"config":{}}"#).is_ok());

        let refused = [
            r#"{"engine":"object","config_version":1}"#,
            r#"{"engine":"table-cache","config_version":1}"#,
            r#"{"engine":"kv-cache","config_version":3}"#,
            r#"{"engine":"kv-cache","config_version":1,"config":{"invalid":true}}"#,
            r#"{"engine":"kv-cache","config_version":1,"config":[]}"#,
            r#"{"engine":"kv-cache","config_version":1,"extra":1}"#,
            r#"{"engine":"KV","config_version":1}"#,
        ];
        for body in refused {
            let err = request(&registry, body).err().unwrap_or_else(|| panic!("accepted {body}"));
            assert_eq!(code(err), S3ErrorCode::InvalidArgument, "{body}");
        }
    }

    #[test]
    fn plan_distinguishes_bind_reconfigure_and_unchanged() {
        let registry = registry();
        let requested = request(&registry, r#"{"engine":"kv-cache","config_version":1,"config":{"a":1}}"#).expect("valid");

        assert_eq!(plan_binding(&requested, &BucketState::default()).expect("plan"), BindPlan::Bind);

        let same = BucketState {
            current: Some(requested.binding.clone()),
            ..BucketState::default()
        };
        assert_eq!(plan_binding(&requested, &same).expect("plan"), BindPlan::Unchanged);

        let mut older = requested.binding.clone();
        older.config = serde_json::json!({"a": 0});
        let other_config = BucketState {
            current: Some(older),
            // Features of an already bound bucket were checked at bind time.
            versioning: true,
            ..BucketState::default()
        };
        assert_eq!(plan_binding(&requested, &other_config).expect("plan"), BindPlan::Reconfigure);
    }

    #[test]
    fn plan_refuses_another_engine_and_refused_features() {
        let registry = registry();
        let requested = request(&registry, r#"{"engine":"kv-cache","config_version":1}"#).expect("valid");

        let mut other = requested.binding.clone();
        other.engine = EngineName::new("table-cache").expect("valid engine name");
        let bound_elsewhere = BucketState {
            current: Some(other),
            ..BucketState::default()
        };
        assert_eq!(code(plan_binding(&requested, &bound_elsewhere).unwrap_err()), S3ErrorCode::InvalidRequest);

        for state in [
            BucketState {
                versioning: true,
                ..BucketState::default()
            },
            BucketState {
                object_lock: true,
                ..BucketState::default()
            },
        ] {
            assert_eq!(code(plan_binding(&requested, &state).unwrap_err()), S3ErrorCode::InvalidRequest);
        }
        // Table buckets are not refused by this engine.
        let table = BucketState {
            table_bucket: true,
            ..BucketState::default()
        };
        assert_eq!(plan_binding(&requested, &table).expect("plan"), BindPlan::Bind);
    }

    #[test]
    fn engine_errors_map_to_stable_s3_codes() {
        assert_eq!(
            code(engine_error(EngineError::InvalidConfig(String::new()))),
            S3ErrorCode::InvalidArgument
        );
        assert_eq!(code(engine_error(EngineError::NotFound)), S3ErrorCode::NoSuchKey);
        assert_eq!(code(engine_error(EngineError::BucketNotEmpty)), S3ErrorCode::BucketNotEmpty);
        assert_eq!(code(engine_error(EngineError::CacheFull)), S3ErrorCode::ServiceUnavailable);
        assert_eq!(
            code(engine_error(EngineError::StalePlacement { client: 1, current: 2 })),
            S3ErrorCode::ServiceUnavailable
        );
        assert_eq!(code(engine_error(EngineError::Internal(String::new()))), S3ErrorCode::InternalError);
    }
}
