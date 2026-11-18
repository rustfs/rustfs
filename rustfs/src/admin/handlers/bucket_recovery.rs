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

//! Explicit, generation-fenced recovery for orphaned bucket metadata.

use crate::admin::auth::authorize_admin_envelope;
use crate::admin::operation::{AdminRequest, AdminResponse, Operation};
use crate::admin::router::{AdminOperation, S3Router};
use crate::admin::runtime_sources::current_object_store_handle;
use crate::server::ADMIN_PREFIX;
use crate::site_replication::{site_replication_enabled, with_site_replication_bucket_mutation_lock};
use hyper::{Method, StatusCode};
use matchit::Params;
use rustfs_policy::policy::action::{Action, AdminAction};
use rustfs_s3_types::{S3Error, S3ErrorCode, S3Result};
use serde::{Deserialize, Serialize};
use tracing::info;
use uuid::Uuid;
use zeroize::Zeroizing;

const EVENT_ADMIN_ORPHAN_BUCKET_RECOVERY: &str = "admin_orphan_bucket_recovery";
const LOG_COMPONENT_ADMIN: &str = "admin";
const LOG_SUBSYSTEM_BUCKET: &str = "bucket_recovery";

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RecoverOrphanedBucketRequest {
    expected_incarnation_id: Uuid,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct RecoverOrphanedBucketResponse {
    bucket: String,
    retired_incarnation_id: Uuid,
}

pub struct RecoverOrphanedBucketHandler;

pub fn register_bucket_recovery_route(router: &mut S3Router<AdminOperation>) -> std::io::Result<()> {
    router.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/recover-orphaned-bucket/{bucket}").as_str(),
        AdminOperation(&RecoverOrphanedBucketHandler {}),
    )?;
    Ok(())
}

#[async_trait::async_trait]
impl Operation for RecoverOrphanedBucketHandler {
    async fn call(&self, mut req: AdminRequest, params: Params<'_, '_>) -> S3Result<AdminResponse> {
        authorize_admin_envelope(req.envelope(), vec![Action::AdminAction(AdminAction::RecoverOrphanedBucketAction)]).await?;
        let credentials = req
            .credentials()
            .ok_or_else(|| S3Error::with_message(S3ErrorCode::InvalidRequest, "authentication required"))?;
        // Checked before the recovery runs, so a refusal leaves the bucket untouched.
        let response_secret = compat_response_secret(req.uri().path(), credentials.secret_key())?;
        let bucket = params
            .get("bucket")
            .filter(|bucket| !bucket.is_empty())
            .ok_or_else(|| S3Error::with_message(S3ErrorCode::InvalidRequest, "bucket name is required"))?;
        let body = req
            .take_body()
            .store_all_limited(rustfs_config::MAX_ADMIN_REQUEST_BODY_SIZE)
            .await
            .map_err(|_| S3Error::with_message(S3ErrorCode::InvalidRequest, "failed to read recovery request body"))?;
        let request: RecoverOrphanedBucketRequest = serde_json::from_slice(&body).map_err(|_| {
            S3Error::with_message(S3ErrorCode::InvalidRequest, "expected JSON with a non-nil expectedIncarnationId")
        })?;
        if request.expected_incarnation_id.is_nil() {
            return Err(S3Error::with_message(
                S3ErrorCode::InvalidRequest,
                "expectedIncarnationId must be non-nil",
            ));
        }
        let Some(store) = current_object_store_handle() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "object store is not initialized"));
        };

        let operation_bucket = bucket.to_owned();
        let lock_store = store.clone();
        let operation_store = store.clone();
        let expected_incarnation = request.expected_incarnation_id;
        let completed = store
            .run_detached_mutation(async move {
                let lock_bucket = operation_bucket.clone();
                with_site_replication_bucket_mutation_lock(lock_store, &lock_bucket, move || async move {
                    if site_replication_enabled().await? {
                        return Err(S3Error::with_message(
                            S3ErrorCode::OperationAborted,
                            "orphaned bucket recovery is local; reconcile every site before recreating this bucket",
                        ));
                    }
                    operation_store
                        .recover_orphaned_bucket(&operation_bucket, expected_incarnation)
                        .await
                        .map_err(|error| S3Error::from(crate::error::ApiError::from(error)))
                })
                .await??;
                Ok::<(), S3Error>(())
            })
            .await
            .map_err(|error| S3Error::from(crate::error::ApiError::from(error)))?;
        completed?;

        info!(
            event = EVENT_ADMIN_ORPHAN_BUCKET_RECOVERY,
            component = LOG_COMPONENT_ADMIN,
            subsystem = LOG_SUBSYSTEM_BUCKET,
            state = "recovered",
            bucket = %bucket,
            incarnation_id = %request.expected_incarnation_id,
            "orphaned bucket metadata recovery completed"
        );

        let response = RecoverOrphanedBucketResponse {
            bucket: bucket.to_owned(),
            retired_incarnation_id: request.expected_incarnation_id,
        };
        match response_secret {
            Some(secret_key) => super::owned_admin_json_response(req.uri().path(), &secret_key, StatusCode::OK, &response),
            None => crate::admin::utils::owned_json_response(StatusCode::OK, &response),
        }
    }
}

/// The key the response body is sealed with. Only the MinIO compat prefix
/// encrypts admin responses, so only that path needs the caller's secret.
///
/// # Errors
///
/// `AccessDenied` on the compat prefix when the verified credential carries no
/// secret key.
fn compat_response_secret(path: &str, secret: Option<&str>) -> S3Result<Option<Zeroizing<String>>> {
    if !crate::admin::utils::is_compat_admin_request(path) {
        return Ok(None);
    }
    secret
        .map(|secret| Some(Zeroizing::new(secret.to_owned())))
        .ok_or_else(|| S3Error::with_message(S3ErrorCode::AccessDenied, "the request credential carries no secret key"))
}

#[cfg(test)]
mod tests {
    use super::*;

    const OWNED_PATH: &str = "/rustfs/admin/v3/recover-orphaned-bucket/orphan";
    const COMPAT_PATH: &str = "/minio/admin/v3/recover-orphaned-bucket/orphan";

    #[test]
    fn compat_recovery_without_a_secret_key_is_refused_before_it_runs() {
        let err = compat_response_secret(COMPAT_PATH, None).expect_err("compat response needs a sealing key");
        assert_eq!(err.code(), &S3ErrorCode::AccessDenied);
    }

    #[test]
    fn owned_recovery_without_a_secret_key_proceeds_unsealed() {
        assert!(
            compat_response_secret(OWNED_PATH, None)
                .expect("owned path needs no key")
                .is_none()
        );
        assert!(
            compat_response_secret(OWNED_PATH, Some("secret"))
                .expect("owned path")
                .is_none()
        );
    }

    #[test]
    fn compat_recovery_with_a_secret_key_seals_with_it() {
        let secret = compat_response_secret(COMPAT_PATH, Some("secret")).expect("compat path with key");
        assert_eq!(secret.as_deref().map(String::as_str), Some("secret"));
    }

    #[test]
    fn recovery_request_rejects_missing_or_nil_generation_identity() {
        assert!(serde_json::from_slice::<RecoverOrphanedBucketRequest>(b"{}").is_err());
        let nil = format!(r#"{{"expectedIncarnationId":"{}"}}"#, Uuid::nil());
        assert!(
            serde_json::from_str::<RecoverOrphanedBucketRequest>(&nil)
                .expect("nil UUID parses")
                .expected_incarnation_id
                .is_nil()
        );
    }
}
