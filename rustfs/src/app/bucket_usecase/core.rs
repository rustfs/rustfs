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

//! Bucket create, delete, head and location use-case operations.

use super::*;
use crate::app::storage_api::s3::dto::{
    BucketLocationConstraint, CreateBucketInput, CreateBucketOutput, DeleteBucketInput, DeleteBucketOutput,
    GetBucketLocationInput, GetBucketLocationOutput, HeadBucketInput, HeadBucketOutput,
};
use crate::app::storage_api::s3::region::Region;
use crate::app::storage_api::s3::{S3Error, S3ErrorCode, S3Request, S3Response, S3Result};

/// Answer a `CreateBucket` for a bucket that already exists.
///
/// A deployment owns one bucket namespace, so every requester that passed
/// authorization belongs to the same owner as the bucket's creator; there is no
/// second account that could trigger `BucketAlreadyExists`. That leaves the AWS
/// account-owner semantics: the default region returns the legacy idempotent
/// `200 OK`, any other region returns `BucketAlreadyOwnedByYou`.
fn create_bucket_exists_response(region: &str) -> S3Result<S3Response<CreateBucketOutput>> {
    if region == RUSTFS_REGION {
        return Ok(S3Response::new(CreateBucketOutput::default()));
    }

    Err(S3Error::with_message(
        S3ErrorCode::BucketAlreadyOwnedByYou,
        "The requested bucket already exists.",
    ))
}

impl DefaultBucketUsecase {
    fn create_bucket_exists_result(&self) -> S3Result<S3Response<CreateBucketOutput>> {
        let region = self
            .global_region()
            .map(|region| region.to_string())
            .unwrap_or_else(|| RUSTFS_REGION.to_string());
        create_bucket_exists_response(&region)
    }

    #[instrument(
        level = "debug",
        skip(self, req),
        fields(start_time=?time::OffsetDateTime::now_utc())
    )]
    pub async fn execute_create_bucket(&self, req: S3Request<CreateBucketInput>) -> S3Result<S3Response<CreateBucketOutput>> {
        rustfs_io_metrics::s3_http_metrics::observe_s3_http_operation(S3Operation::CreateBucket);
        if let Some(location) = req
            .input
            .create_bucket_configuration
            .as_ref()
            .and_then(|config| config.location_constraint.as_ref())
            .map(BucketLocationConstraint::as_str)
            .filter(|location| !location.is_empty())
        {
            let location = if location == "EU" { "eu-west-1" } else { location };
            let region = self.global_region();
            if location != region.as_ref().map_or(RUSTFS_REGION, Region::as_str) {
                return Err(S3Error::with_message(
                    S3ErrorCode::InvalidLocationConstraint,
                    "The specified location constraint does not match this server's region.",
                ));
            }
        }

        // An existing bucket needs no mutation. Answer before the mutation queue and the
        // bucket's exclusive locks, which wait for every in-flight object write.
        if req_info_ref(&req).is_ok()
            && let Some(store) = self.object_store()
            && store
                .get_bucket_info(&req.input.bucket, &BucketOptions::default())
                .await
                .is_ok()
        {
            let helper = OperationHelper::new(&req, EventName::BucketCreated, S3Operation::CreateBucket);
            let result = self.create_bucket_exists_result();
            let _ = helper.complete(&result);
            return result;
        }
        let usecase = self.clone();
        await_bucket_usecase_on_fresh_task("bucket creation", move |mutation_permit| async move {
            usecase.execute_create_bucket_inner(req, mutation_permit).await
        })
        .await
    }

    async fn execute_create_bucket_inner(
        &self,
        req: S3Request<CreateBucketInput>,
        mutation_permit: OwnedSemaphorePermit,
    ) -> S3Result<S3Response<CreateBucketOutput>> {
        let helper = OperationHelper::new(&req, EventName::BucketCreated, S3Operation::CreateBucket);
        let request_context = req.extensions.get::<request_context::RequestContext>().cloned();
        if req_info_ref(&req).is_err() {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Missing request info".to_string()));
        }
        let CreateBucketInput {
            bucket,
            object_lock_enabled_for_bucket,
            ..
        } = req.input;
        let lock_enabled = object_lock_enabled_for_bucket.is_some_and(|v| v);

        let Some(store) = self.object_store() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        // Keep the local namespace mutation and its peer hook ordered across
        // every node in this site. Otherwise a delete waiting for repair
        // coordination can arrive after this create on remote sites.
        let operation_bucket = bucket.clone();
        let operation_store = store.clone();
        let make_result = with_site_replication_bucket_mutation_lock(store, &bucket, move || async move {
            let make_result = operation_store
                .make_bucket(
                    &operation_bucket,
                    &MakeBucketOptions {
                        force_create: false,
                        lock_enabled,
                        ..Default::default()
                    },
                )
                .await;
            if make_result.is_ok() {
                crate::storage::invalidate_bucket_validation_cache(&operation_bucket);
                if let Err(err) = site_replication_make_bucket_hook(&operation_bucket, lock_enabled).await {
                    warn!(bucket = %operation_bucket, error = ?err, "site replication make bucket hook failed");
                }
            }
            make_result
        })
        .await?;

        // The storage/site mutation and its lock fences have finished. Keep
        // the operation's in-flight slot while peers reload, without holding
        // an execution slot needed by other creates and deletes.
        drop(mutation_permit);

        match make_result {
            Ok(()) => {
                // A peer may have cached this bucket as missing before the
                // create. Refresh peer metadata after the namespace and
                // metadata commit fences are released so the peer reload can
                // acquire its bucket namespace read lock without an ABBA
                // interleaving with creation.
                notify_bucket_metadata_reload_with_context(
                    bucket.clone(),
                    "create bucket",
                    request_context,
                    false,
                    self.context.clone(),
                )
                .await;
            }
            Err(StorageError::BucketExists(_)) => {
                let result = self.create_bucket_exists_result();
                let _ = helper.complete(&result);
                return result;
            }
            Err(e) => return Err(ApiError::from(e).into()),
        }

        let output = CreateBucketOutput::default();
        counter!("rustfs_create_bucket_total").increment(1);
        let result = Ok(S3Response::new(output));
        let _ = helper.complete(&result);
        rustfs_scanner::record_dirty_usage_bucket_from_producer(
            &bucket,
            rustfs_scanner::SegmentInvalidationProducerIdentity::BucketMetadata,
        );
        result
    }

    #[instrument(level = "debug", skip(self, req))]
    pub async fn execute_delete_bucket(&self, req: S3Request<DeleteBucketInput>) -> S3Result<S3Response<DeleteBucketOutput>> {
        rustfs_io_metrics::s3_http_metrics::observe_s3_http_operation(S3Operation::DeleteBucket);
        let usecase = self.clone();
        await_bucket_usecase_on_fresh_task("bucket deletion", move |mutation_permit| async move {
            let _mutation_permit = mutation_permit;
            usecase.execute_delete_bucket_inner(req).await
        })
        .await
    }

    async fn execute_delete_bucket_inner(
        &self,
        mut req: S3Request<DeleteBucketInput>,
    ) -> S3Result<S3Response<DeleteBucketOutput>> {
        let helper = OperationHelper::new(&req, EventName::BucketRemoved, S3Operation::DeleteBucket);
        let input = req.input.clone();

        let Some(store) = self.object_store() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        let force = match force_delete_header(&req.headers) {
            Ok(value) => value.unwrap_or(false),
            Err(_) => return Err(S3Error::with_message(S3ErrorCode::InvalidRequest, "Invalid force-delete header value")),
        };

        if force {
            authorize_request(&mut req, Action::S3Action(S3Action::ForceDeleteBucketAction)).await?;
        }

        // Keep the local namespace mutation and its peer hook ordered across
        // every node in this site so an older delete cannot overtake a new
        // same-name make while it waits for repair coordination.
        let operation_bucket = input.bucket.clone();
        let operation_store = store.clone();
        with_site_replication_bucket_mutation_lock(store, &input.bucket, move || async move {
            let intent = prepare_site_replication_delete_bucket(&operation_bucket, force).await?;
            let delete_result = operation_store
                .delete_bucket(
                    &operation_bucket,
                    &DeleteBucketOptions {
                        force,
                        ..Default::default()
                    },
                )
                .await;
            match delete_result {
                Ok(()) => {
                    crate::storage::invalidate_bucket_validation_cache(&operation_bucket);
                    if let Some(intent) = intent
                        && let Err(err) = commit_site_replication_delete_bucket(&intent).await
                    {
                        warn!(bucket = %operation_bucket, error = ?err, "site replication delete bucket hook failed");
                    }
                    Ok::<(), S3Error>(())
                }
                Err(err) => {
                    if let Some(intent) = intent {
                        cancel_site_replication_delete_bucket(intent).await;
                    }
                    Err(S3Error::from(ApiError::from(err)))
                }
            }
        })
        .await??;

        // Drop every cached object body for the now-deleted bucket so dead
        // bytes do not sit resident until TTL. Covers both the normal and the
        // force-delete path, which share this single delete_bucket call
        // (ODC-28, backlog#1133).
        let cache_adapter = current_object_data_cache_for_context(self.context.as_deref());
        let _ = invalidate_object_data_cache_bucket_after_delete(&cache_adapter, &input.bucket).await;

        // Re-evaluate lifecycle and replication after bucket removal.
        rustfs_scanner::record_scanner_maintenance_change(&input.bucket);

        // Notify peers to drop their cached metadata for the now-deleted bucket.
        let request_context = req.extensions.get::<request_context::RequestContext>().cloned();
        notify_bucket_metadata_delete(input.bucket.clone(), request_context);

        let result = Ok(S3Response::new(DeleteBucketOutput {}));
        let _ = helper.complete(&result);
        result
    }

    #[instrument(level = "debug", skip(self, req))]
    pub async fn execute_head_bucket(&self, req: S3Request<HeadBucketInput>) -> S3Result<S3Response<HeadBucketOutput>> {
        record_s3_op(S3Operation::HeadBucket);
        let input = req.input;

        let Some(store) = self.object_store() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(&input.bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        Ok(S3Response::new(HeadBucketOutput::default()))
    }

    #[instrument(level = "debug", skip(self, req))]
    pub async fn execute_get_bucket_location(
        &self,
        req: S3Request<GetBucketLocationInput>,
    ) -> S3Result<S3Response<GetBucketLocationOutput>> {
        let input = req.input;

        let Some(store) = self.object_store() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(&input.bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        let location_constraint = self
            .global_region()
            .filter(|region| region.as_str() != RUSTFS_REGION)
            .map(|region| BucketLocationConstraint::from(region.to_string()));
        Ok(S3Response::new(GetBucketLocationOutput { location_constraint }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::bucket_usecase::test_support::{build_request, build_request_with_req_info, s3_op_total};
    use crate::app::storage_api::s3 as s3s_crate;
    use http::Method;

    #[test]
    fn create_bucket_exists_response_returns_ok_in_default_region() {
        let response = create_bucket_exists_response(RUSTFS_REGION).expect("recreate in us-east-1 should succeed");
        assert_eq!(response.output.location, None);
    }

    #[test]
    fn create_bucket_exists_response_returns_owned_by_you_outside_default_region() {
        let err = create_bucket_exists_response("eu-west-1").expect_err("recreate outside us-east-1 should report ownership");
        assert_eq!(err.code(), &S3ErrorCode::BucketAlreadyOwnedByYou);
    }

    #[tokio::test]
    async fn execute_create_bucket_returns_internal_error_when_store_uninitialized() {
        let input = CreateBucketInput::builder()
            .bucket("test-bucket".to_string())
            .build()
            .unwrap();

        let req = build_request(input, Method::PUT);
        let usecase = DefaultBucketUsecase::without_context();

        let mut request = rustfs_io_metrics::s3_http_metrics::S3HttpRequestGuard::new("PUT");
        let mut call = Box::pin(usecase.execute_create_bucket(req));
        let err = std::future::poll_fn(|cx| request.in_scope(|| call.as_mut().poll(cx)))
            .await
            .expect_err("an uninitialized store must fail");
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
        assert_eq!(
            request.in_scope(rustfs_io_metrics::s3_http_metrics::current_s3_http_operation_name),
            Some("s3:CreateBucket")
        );
    }

    #[test]
    #[serial_test::serial]
    fn execute_create_bucket_on_existing_bucket_does_not_wait_for_bucket_lifecycle_lock() {
        crate::app::gating_test_env::run_large_stack_test("create-existing-bucket", || async {
            let context = crate::app::gating_test_env::shared_gating_ambient().await;
            let store = crate::app::gating_test_env::shared_gating_ecstore().await;
            let bucket = format!("create-existing-{}", uuid::Uuid::new_v4().simple());
            store
                .make_bucket(&bucket, &MakeBucketOptions::default())
                .await
                .expect("create bucket");

            // Object writes hold this lock shared; an exclusive guard exposes any lifecycle-lock wait.
            let _lifecycle_guard = store
                .lock_bucket_for_recursive_delete(&bucket)
                .await
                .expect("hold the bucket lifecycle lock");
            let usecase = DefaultBucketUsecase::with_context(Some(context));

            // The requester's owner flag must not change the duplicate response:
            // one deployment has a single bucket owner.
            for (requester_is_owner, location, expected_error) in [
                (true, None, None),
                (false, None, None),
                (true, Some("ap-south-1"), Some(S3ErrorCode::InvalidLocationConstraint)),
            ] {
                let input = CreateBucketInput {
                    bucket: bucket.clone(),
                    create_bucket_configuration: location.map(|location| s3s_crate::dto::CreateBucketConfiguration {
                        location_constraint: Some(BucketLocationConstraint::from(location.to_string())),
                        ..Default::default()
                    }),
                    ..Default::default()
                };
                let req = build_request_with_req_info(
                    input,
                    Method::PUT,
                    ReqInfo {
                        is_owner: requester_is_owner,
                        ..Default::default()
                    },
                );
                let result = tokio::time::timeout(Duration::from_secs(2), usecase.execute_create_bucket(req))
                    .await
                    .expect("CreateBucket on an existing bucket must not wait for the bucket lifecycle lock");
                match expected_error {
                    Some(code) => assert_eq!(result.expect_err("invalid request must fail").code(), &code),
                    None => {
                        result.expect("recreating a bucket in us-east-1 returns the idempotent success");
                    }
                }
            }

            let req = build_request(
                CreateBucketInput {
                    bucket,
                    ..Default::default()
                },
                Method::PUT,
            );
            let err = tokio::time::timeout(Duration::from_secs(2), usecase.execute_create_bucket(req))
                .await
                .expect("missing request info must fail before storage mutation")
                .expect_err("an existing bucket must not bypass request-info validation");
            assert_eq!(err.code(), &S3ErrorCode::InternalError);
        });
    }

    #[test]
    #[serial_test::serial]
    fn execute_create_bucket_creates_missing_bucket() {
        crate::app::gating_test_env::run_large_stack_test("create-missing-bucket", || async {
            let context = crate::app::gating_test_env::shared_gating_ambient().await;
            let store = crate::app::gating_test_env::shared_gating_ecstore().await;
            let bucket = format!("create-missing-{}", uuid::Uuid::new_v4().simple());
            let input = CreateBucketInput::builder().bucket(bucket.clone()).build().unwrap();
            let req = build_request_with_req_info(
                input,
                Method::PUT,
                ReqInfo {
                    is_owner: true,
                    ..Default::default()
                },
            );
            let usecase = DefaultBucketUsecase::with_context(Some(context));
            usecase.execute_create_bucket(req).await.expect("create a missing bucket");
            let info = store
                .get_bucket_info(&bucket, &BucketOptions::default())
                .await
                .expect("the missing-bucket lookup must fall through to real creation");
            assert_eq!(info.name, bucket);
        });
    }

    #[tokio::test]
    async fn execute_delete_bucket_returns_internal_error_when_store_uninitialized() {
        let input = DeleteBucketInput::builder()
            .bucket("test-bucket".to_string())
            .build()
            .unwrap();

        let req = build_request(input, Method::DELETE);
        let usecase = DefaultBucketUsecase::without_context();

        let mut request = rustfs_io_metrics::s3_http_metrics::S3HttpRequestGuard::new("DELETE");
        let mut call = Box::pin(usecase.execute_delete_bucket(req));
        let err = std::future::poll_fn(|cx| request.in_scope(|| call.as_mut().poll(cx)))
            .await
            .expect_err("an uninitialized store must fail");
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
        assert_eq!(
            request.in_scope(rustfs_io_metrics::s3_http_metrics::current_s3_http_operation_name),
            Some("s3:DeleteBucket")
        );
    }

    #[tokio::test]
    async fn execute_head_bucket_returns_internal_error_when_store_uninitialized() {
        let input = HeadBucketInput::builder().bucket("test-bucket".to_string()).build().unwrap();

        let req = build_request(input, Method::HEAD);
        let usecase = DefaultBucketUsecase::without_context();
        let before = s3_op_total(S3Operation::HeadBucket);

        let err = usecase.execute_head_bucket(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
        assert_eq!(s3_op_total(S3Operation::HeadBucket), before + 1);
    }

    #[tokio::test]
    async fn execute_get_bucket_location_returns_internal_error_when_store_uninitialized() {
        let input = GetBucketLocationInput::builder()
            .bucket("test-bucket".to_string())
            .build()
            .unwrap();

        let req = build_request(input, Method::GET);
        let usecase = DefaultBucketUsecase::without_context();

        let err = usecase.execute_get_bucket_location(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }
}
