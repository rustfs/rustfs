// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0
//! Controller operations; receipts are storage facts, never GC authorization.
use super::plugin_delegation::{owner_for_resource, response};
use crate::{
    admin::{
        auth::AdminResourceScope,
        router::{AdminOperation, Operation, S3Router},
        runtime_sources::{self, ServerContextSlot},
    },
    server::ADMIN_PREFIX,
};
use http::{Method, StatusCode};
use matchit::Params;
use rustfs_policy::policy::action::AdminAction;
use s3s::{Body, S3Request, S3Response, S3Result, s3_error};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{sync::Arc, time::Duration};
use uuid::Uuid;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BucketIdentityRequest {
    version: u32,
    bucket: String,
    base_prefix: String,
}

impl BucketIdentityRequest {
    fn validate(&self) -> S3Result<()> {
        if self.version != 1
            || crate::storage::ecstore_bucket::utils::check_valid_bucket_name_strict(&self.bucket).is_err()
            || !crate::admin::storage_api::contract::bucket::valid_artifact_base_prefix(&self.base_prefix)
        {
            return Err(s3_error!(InvalidRequest, "invalid artifact bucket identity request"));
        }
        Ok(())
    }
}

#[derive(Serialize)]
struct BucketIdentityReceipt {
    version: u32,
    bucket: String,
    base_prefix: String,
    bucket_incarnation: Uuid,
    versioning_enabled: bool,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct AttemptIdentity {
    bucket: String,
    bucket_incarnation: Uuid,
    attempt_prefix: String,
}

impl AttemptIdentity {
    fn validate(&self) -> S3Result<()> {
        if self.bucket.is_empty()
            || self.bucket.len() > 255
            || self.bucket.contains('/')
            || self.bucket.chars().any(char::is_control)
            || self.bucket_incarnation.is_nil()
            || self.attempt_prefix.is_empty()
            || self.attempt_prefix.len() >= 1024
            || !self.attempt_prefix.ends_with('/')
            || self.attempt_prefix.chars().any(char::is_control)
        {
            return Err(s3_error!(InvalidRequest, "invalid artifact attempt identity"));
        }
        Ok(())
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SealRequest {
    version: u32,
    target: AttemptIdentity,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RetireRequest {
    version: u32,
    target: AttemptIdentity,
    object: String,
    version_id: Uuid,
}

impl RetireRequest {
    fn validate(&self) -> S3Result<()> {
        self.target.validate()?;
        if self.version != 1
            || self.version_id.is_nil()
            || self.object.len() > 1024
            || !self.object.starts_with(&self.target.attempt_prefix)
            || self.object.len() == self.target.attempt_prefix.len()
            || self.object.chars().any(char::is_control)
        {
            return Err(s3_error!(InvalidRequest, "invalid artifact version identity"));
        }
        Ok(())
    }
}

#[derive(Serialize)]
struct SealReceipt {
    version: u32,
    target: AttemptIdentity,
    ordinary_put_sealed: bool,
}

#[derive(Serialize)]
struct RepairSealReceipt {
    version: u32,
    target: AttemptIdentity,
    attempt_repairs_sealed: bool,
}

#[derive(Serialize)]
struct WritesSealReceipt {
    version: u32,
    target: AttemptIdentity,
    ordinary_put_sealed: bool,
    attempt_repairs_sealed: bool,
    all_writes_sealed: bool,
}

#[derive(Serialize)]
struct RetirementReceipt {
    version: u32,
    target: AttemptIdentity,
    object: String,
    version_id: Uuid,
    exact_version_repair_retired: bool,
}

async fn require_builtin_authorization() -> S3Result<()> {
    // The external policy backend can replace local IAM evaluation, so it
    // cannot establish the explicit Resource contract of these controls.
    rustfs_iam::sys::IamSys::<rustfs_iam::store::object::ObjectStore>::ensure_restricted_credential_vending()
        .await
        .map_err(|_| s3_error!(AccessDenied, "external policy plugin unsupported for artifact controls"))
}

async fn request_body<T: DeserializeOwned>(req: &mut S3Request<Body>) -> S3Result<T> {
    let bytes = tokio::time::timeout(Duration::from_secs(10), req.input.store_all_limited(16 * 1024))
        .await
        .map_err(|_| s3_error!(InvalidRequest, "artifact body unavailable"))?
        .map_err(|_| s3_error!(InvalidRequest, "artifact body unavailable"))?;
    serde_json::from_slice(&bytes).map_err(|_| s3_error!(InvalidRequest, "invalid artifact request"))
}

fn request_store(req: &S3Request<Body>) -> S3Result<Arc<crate::admin::storage_api::ECStore>> {
    // These new controls require the owning server slot, never an ambient store.
    if req.extensions.get::<Arc<ServerContextSlot>>().is_none() {
        return Err(s3_error!(ServiceUnavailable, "artifact application context unavailable"));
    }
    runtime_sources::object_store_from_req(req).ok_or_else(|| s3_error!(ServiceUnavailable, "artifact storage unavailable"))
}

fn unconfirmed() -> s3s::S3Error {
    // Storage may finish after a timeout or caller cancellation. Only an exact
    // successful retry confirms the durable fact; never expose internal paths.
    s3_error!(ServiceUnavailable, "artifact operation unconfirmed; retry the same identity")
}

pub fn register_plugin_artifact_routes(router: &mut S3Router<AdminOperation>) -> std::io::Result<()> {
    router.insert(
        Method::POST,
        &format!("{ADMIN_PREFIX}/v3/plugin-artifacts/buckets"),
        AdminOperation(&Create),
    )?;
    router.insert(
        Method::POST,
        &format!("{ADMIN_PREFIX}/v3/plugin-artifacts/buckets/inspect"),
        AdminOperation(&Inspect),
    )?;
    router.insert(
        Method::POST,
        &format!("{ADMIN_PREFIX}/v3/plugin-artifacts/seal-attempt"),
        AdminOperation(&Seal),
    )?;
    router.insert(
        Method::POST,
        &format!("{ADMIN_PREFIX}/v3/plugin-artifacts/seal-repairs"),
        AdminOperation(&SealRepairs),
    )?;
    router.insert(
        Method::POST,
        &format!("{ADMIN_PREFIX}/v3/plugin-artifacts/seal-writes"),
        AdminOperation(&SealWrites),
    )?;
    router.insert(
        Method::POST,
        &format!("{ADMIN_PREFIX}/v3/plugin-artifacts/retire-version"),
        AdminOperation(&Retire),
    )?;
    Ok(())
}

struct Create;
#[async_trait::async_trait]
impl Operation for Create {
    async fn call(&self, mut req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        require_builtin_authorization().await?;
        let input: BucketIdentityRequest = request_body(&mut req).await?;
        input.validate()?;
        let store = request_store(&req)?;
        owner_for_resource(
            &req,
            AdminAction::CreatePluginArtifactBucketAction,
            AdminResourceScope::bucket(&input.bucket),
        )
        .await?;
        let incarnation =
            tokio::time::timeout(Duration::from_secs(30), store.make_artifact_bucket(&input.bucket, &input.base_prefix))
                .await
                .map_err(|_| unconfirmed())?
                .map_err(|error| match error {
                    crate::admin::storage_api::Error::BucketExists(_) => {
                        s3_error!(BucketAlreadyExists, "artifact bucket already exists")
                    }
                    _ => unconfirmed(),
                })?;
        response(&BucketIdentityReceipt {
            version: 1,
            bucket: input.bucket,
            base_prefix: input.base_prefix,
            bucket_incarnation: incarnation,
            versioning_enabled: true,
        })
    }
}

struct Inspect;
#[async_trait::async_trait]
impl Operation for Inspect {
    async fn call(&self, mut req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        require_builtin_authorization().await?;
        let input: BucketIdentityRequest = request_body(&mut req).await?;
        input.validate()?;
        let store = request_store(&req)?;
        owner_for_resource(
            &req,
            AdminAction::GetPluginArtifactBucketAction,
            AdminResourceScope::bucket(&input.bucket),
        )
        .await?;
        let incarnation =
            tokio::time::timeout(Duration::from_secs(30), store.inspect_artifact_bucket(&input.bucket, &input.base_prefix))
                .await
                .map_err(|_| unconfirmed())?
                .map_err(|_| unconfirmed())?;
        response(&BucketIdentityReceipt {
            version: 1,
            bucket: input.bucket,
            base_prefix: input.base_prefix,
            bucket_incarnation: incarnation,
            versioning_enabled: true,
        })
    }
}

struct Seal;
#[async_trait::async_trait]
impl Operation for Seal {
    async fn call(&self, mut req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        require_builtin_authorization().await?;
        let input: SealRequest = request_body(&mut req).await?;
        input.target.validate()?;
        if input.version != 1 {
            return Err(s3_error!(InvalidRequest, "unsupported artifact protocol version"));
        }
        let store = request_store(&req)?;
        owner_for_resource(
            &req,
            AdminAction::SealPluginArtifactAttemptAction,
            AdminResourceScope::bucket_object(&input.target.bucket, &input.target.attempt_prefix),
        )
        .await?;
        tokio::time::timeout(
            Duration::from_secs(30),
            store.seal_artifact_attempt_puts(&input.target.bucket, input.target.bucket_incarnation, &input.target.attempt_prefix),
        )
        .await
        .map_err(|_| unconfirmed())?
        .map_err(|_| unconfirmed())?;
        response(&SealReceipt {
            version: 1,
            target: input.target,
            ordinary_put_sealed: true,
        })
    }
}

struct SealRepairs;
#[async_trait::async_trait]
impl Operation for SealRepairs {
    async fn call(&self, mut req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        require_builtin_authorization().await?;
        let input: SealRequest = request_body(&mut req).await?;
        input.target.validate()?;
        if input.version != 1 {
            return Err(s3_error!(InvalidRequest, "unsupported artifact protocol version"));
        }
        let store = request_store(&req)?;
        owner_for_resource(
            &req,
            AdminAction::SealPluginArtifactRepairsAction,
            AdminResourceScope::bucket_object(&input.target.bucket, &input.target.attempt_prefix),
        )
        .await?;
        tokio::time::timeout(
            Duration::from_secs(30),
            store.seal_artifact_attempt_repairs(
                &input.target.bucket,
                input.target.bucket_incarnation,
                &input.target.attempt_prefix,
            ),
        )
        .await
        .map_err(|_| unconfirmed())?
        .map_err(|_| unconfirmed())?;
        response(&RepairSealReceipt {
            version: 1,
            target: input.target,
            attempt_repairs_sealed: true,
        })
    }
}

struct SealWrites;
#[async_trait::async_trait]
impl Operation for SealWrites {
    async fn call(&self, mut req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        require_builtin_authorization().await?;
        let input: SealRequest = request_body(&mut req).await?;
        input.target.validate()?;
        if input.version != 1 {
            return Err(s3_error!(InvalidRequest, "unsupported artifact protocol version"));
        }
        let store = request_store(&req)?;
        owner_for_resource(
            &req,
            AdminAction::SealPluginArtifactWritesAction,
            AdminResourceScope::bucket_object(&input.target.bucket, &input.target.attempt_prefix),
        )
        .await?;
        tokio::time::timeout(
            Duration::from_secs(30),
            store.seal_artifact_attempt_writes(
                &input.target.bucket,
                input.target.bucket_incarnation,
                &input.target.attempt_prefix,
            ),
        )
        .await
        .map_err(|_| unconfirmed())?
        .map_err(|_| unconfirmed())?;
        response(&WritesSealReceipt {
            version: 1,
            target: input.target,
            ordinary_put_sealed: true,
            attempt_repairs_sealed: true,
            all_writes_sealed: true,
        })
    }
}

struct Retire;
#[async_trait::async_trait]
impl Operation for Retire {
    async fn call(&self, mut req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        require_builtin_authorization().await?;
        let input: RetireRequest = request_body(&mut req).await?;
        input.validate()?;
        let store = request_store(&req)?;
        owner_for_resource(
            &req,
            AdminAction::RetirePluginArtifactVersionAction,
            AdminResourceScope::bucket_object(&input.target.bucket, &input.object),
        )
        .await?;
        tokio::time::timeout(
            Duration::from_secs(30),
            store.retire_artifact_object_version(
                &input.target.bucket,
                input.target.bucket_incarnation,
                &input.target.attempt_prefix,
                &input.object,
                input.version_id,
            ),
        )
        .await
        .map_err(|_| unconfirmed())?
        .map_err(|_| unconfirmed())?;
        response(&RetirementReceipt {
            version: 1,
            target: input.target,
            object: input.object,
            version_id: input.version_id,
            exact_version_repair_retired: true,
        })
    }
}

#[cfg(test)]
#[path = "plugin_artifact_tests.rs"]
mod integration_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};

    fn target() -> Value {
        json!({"bucket":"artifacts", "bucket_incarnation":Uuid::new_v4(),
            "attempt_prefix":format!("out/61/{}/{}/",Uuid::new_v4(),Uuid::new_v4())})
    }

    #[test]
    fn plugin_artifact_strict_request_identity() {
        let mut seal = json!({"version":1,"target":target()});
        let request: SealRequest = serde_json::from_value(seal.clone()).expect("seal request");
        request.target.validate().expect("valid target");
        seal["unknown"] = true.into();
        assert!(serde_json::from_value::<SealRequest>(seal).is_err());
        let mut bad_target = target();
        bad_target["unknown"] = true.into();
        assert!(serde_json::from_value::<AttemptIdentity>(bad_target).is_err());
        let mut target = target();
        target["bucket_incarnation"] = Uuid::nil().to_string().into();
        assert!(
            serde_json::from_value::<AttemptIdentity>(target)
                .expect("nil parses")
                .validate()
                .is_err()
        );
        let valid = json!({"version":1,"target":request.target,
            "object":format!("{}image.png",request.target.attempt_prefix),"version_id":Uuid::new_v4()});
        serde_json::from_value::<RetireRequest>(valid.clone())
            .expect("retire request")
            .validate()
            .expect("valid version");
        for (key, value) in [
            ("object", json!("elsewhere/image.png")),
            ("version_id", json!(Uuid::nil())),
            ("version", json!(2)),
        ] {
            let mut bad = valid.clone();
            bad[key] = value;
            assert!(
                serde_json::from_value::<RetireRequest>(bad)
                    .expect("well-formed request")
                    .validate()
                    .is_err()
            );
        }
    }

    #[test]
    fn plugin_artifact_receipts_are_not_gc_permission() {
        let target: AttemptIdentity = serde_json::from_value(target()).expect("target");
        let seal = serde_json::to_value(SealReceipt {
            version: 1,
            target,
            ordinary_put_sealed: true,
        })
        .expect("receipt");
        assert_eq!(seal["ordinary_put_sealed"], true);
        for absent in ["gc_safe", "deleted", "all_writes_sealed", "attempt_repairs_sealed"] {
            assert!(seal.get(absent).is_none());
        }
        let repairs = serde_json::to_value(RepairSealReceipt {
            version: 1,
            target: serde_json::from_value(seal["target"].clone()).expect("same target"),
            attempt_repairs_sealed: true,
        })
        .expect("repair seal receipt");
        assert_eq!(repairs["attempt_repairs_sealed"], true);
        for absent in ["gc_safe", "deleted", "all_writes_sealed", "ordinary_put_sealed"] {
            assert!(repairs.get(absent).is_none());
        }
    }

    #[tokio::test]
    async fn plugin_artifact_routes_and_body_limit() {
        let mut router = S3Router::new(false);
        register_plugin_artifact_routes(&mut router).expect("register controls");
        for path in ["buckets", "buckets/inspect", "seal-attempt", "seal-repairs", "retire-version"] {
            assert!(router.contains_route(Method::POST, &format!("{ADMIN_PREFIX}/v3/plugin-artifacts/{path}")));
        }
        let mut req = S3Request {
            input: Body::from(vec![b' '; 16 * 1024 + 1]),
            method: Method::POST,
            uri: http::Uri::default(),
            headers: http::HeaderMap::new(),
            extensions: http::Extensions::new(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        };
        assert!(request_body::<SealRequest>(&mut req).await.is_err());
        assert!(request_store(&req).is_err(), "ARTIFACT_REQUIRES_REQUEST_INSTANCE");
        req.extensions.insert(Arc::new(ServerContextSlot::default()));
        assert!(request_store(&req).is_err(), "ARTIFACT_UNREADY_INSTANCE_NO_FALLBACK");
    }
}
