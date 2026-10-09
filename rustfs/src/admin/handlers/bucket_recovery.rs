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

use crate::admin::auth::authorize_admin_request;
use crate::admin::router::{AdminOperation, Operation, S3Router};
use crate::admin::runtime_sources::current_object_store_handle;
use crate::admin::storage_api::s3::{Body, S3Error, S3Request, S3Response, S3Result};
use crate::server::ADMIN_PREFIX;
use crate::site_replication::{site_replication_enabled, with_site_replication_bucket_mutation_lock};
use hyper::{Method, StatusCode};
use matchit::Params;
use rustfs_policy::policy::action::{Action, AdminAction};
use rustfs_s3_types::s3_error;
use serde::{Deserialize, Serialize};
use tracing::info;
use uuid::Uuid;

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
    async fn call(&self, mut req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        authorize_admin_request(&req, vec![Action::AdminAction(AdminAction::RecoverOrphanedBucketAction)]).await?;
        let secret_key = req
            .credentials
            .as_ref()
            .map(|credentials| credentials.secret_key.clone())
            .ok_or_else(|| s3_error!(InvalidRequest, "authentication required"))?;
        let bucket = params
            .get("bucket")
            .filter(|bucket| !bucket.is_empty())
            .ok_or_else(|| s3_error!(InvalidRequest, "bucket name is required"))?;
        let body = req
            .input
            .store_all_limited(rustfs_config::MAX_ADMIN_REQUEST_BODY_SIZE)
            .await
            .map_err(|_| s3_error!(InvalidRequest, "failed to read recovery request body"))?;
        let request: RecoverOrphanedBucketRequest = serde_json::from_slice(&body)
            .map_err(|_| s3_error!(InvalidRequest, "expected JSON with a non-nil expectedIncarnationId"))?;
        if request.expected_incarnation_id.is_nil() {
            return Err(s3_error!(InvalidRequest, "expectedIncarnationId must be non-nil").into());
        }
        let Some(store) = current_object_store_handle() else {
            return Err(s3_error!(InternalError, "object store is not initialized").into());
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
                        return Err(s3_error!(
                            OperationAborted,
                            "orphaned bucket recovery is local; reconcile every site before recreating this bucket"
                        )
                        .into());
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

        super::admin_json_response(
            req.uri.path(),
            secret_key.expose(),
            StatusCode::OK,
            &RecoverOrphanedBucketResponse {
                bucket: bucket.to_owned(),
                retired_incarnation_id: request.expected_incarnation_id,
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
