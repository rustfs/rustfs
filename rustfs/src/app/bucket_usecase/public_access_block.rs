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

//! Bucket public access block use-case operations.

use super::*;
use rustfs_s3_types::{S3Error, S3ErrorCode, S3Result};

impl DefaultBucketUsecase {
    #[instrument(level = "debug", skip(self, envelope))]
    pub(crate) async fn execute_delete_public_access_block(&self, envelope: RequestEnvelope, bucket: &str) -> S3Result<()> {
        RequestEnvelope::audit_only_operation(
            envelope,
            S3Operation::DeletePublicAccessBlock,
            StatusCode::NO_CONTENT,
            |envelope| async move {
                let expected_incarnation_id = bucket_config_mutation_incarnation(&envelope, bucket)?;
                let request_context = envelope.request_context().cloned();

                let Some(store) = self.object_store() else {
                    return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init"));
                };
                store
                    .get_bucket_info(bucket, &BucketOptions::default())
                    .await
                    .map_err(ApiError::from)?;

                delete_bucket_config_for_incarnation(bucket, BUCKET_PUBLIC_ACCESS_BLOCK_CONFIG, expected_incarnation_id)
                    .await
                    .map_err(ApiError::from)?;
                notify_bucket_metadata_reload(bucket.to_owned(), "delete public access block", request_context, false).await;
                Ok(())
            },
        )
        .await
    }

    #[instrument(level = "debug", skip(self))]
    pub(crate) async fn execute_get_public_access_block(
        &self,
        bucket: &str,
    ) -> S3Result<PersistedPublicAccessBlockConfiguration> {
        let Some(store) = self.object_store() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init"));
        };
        store
            .get_bucket_info(bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        match metadata_sys::get_public_access_block_config(bucket).await {
            Ok((config, _)) => Ok(config),
            Err(StorageError::ConfigNotFound) => {
                let mut error = S3Error::with_message(
                    S3ErrorCode::Custom("NoSuchPublicAccessBlockConfiguration".into()),
                    "Public access block configuration does not exist",
                );
                // The legacy edge used the custom code with the server-error status.
                error.set_status_code(StatusCode::INTERNAL_SERVER_ERROR);
                Err(error)
            }
            Err(err) => Err(ApiError::from(err).into()),
        }
    }

    #[instrument(level = "debug", skip(self, envelope, prepared))]
    pub(crate) async fn execute_put_public_access_block(
        &self,
        envelope: RequestEnvelope,
        bucket: &str,
        prepared: S3Result<PersistedPublicAccessBlockConfiguration>,
    ) -> S3Result<()> {
        RequestEnvelope::audit_only_operation(
            envelope,
            S3Operation::PutPublicAccessBlock,
            StatusCode::OK,
            |envelope| async move {
                let expected_incarnation_id = bucket_config_mutation_incarnation(&envelope, bucket)?;
                let request_context = envelope.request_context().cloned();

                let Some(store) = self.object_store() else {
                    return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init"));
                };
                store
                    .get_bucket_info(bucket, &BucketOptions::default())
                    .await
                    .map_err(ApiError::from)?;

                let data = serialize_config(&prepared?)?;
                update_bucket_config_for_incarnation(bucket, BUCKET_PUBLIC_ACCESS_BLOCK_CONFIG, data, expected_incarnation_id)
                    .await
                    .map_err(ApiError::from)?;
                notify_bucket_metadata_reload(bucket.to_owned(), "put public access block", request_context, false).await;
                Ok(())
            },
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::bucket_usecase::test_support::build_request;
    use crate::app::gating_test_env::{run_large_stack_test, shared_gating_ambient};
    use crate::app::runtime_sources::ServerContextSlot;
    use crate::app::storage_api::bucket_usecase::helper::MutationCompletionProbe;
    use crate::app::storage_api::owned_parity::authorize_bucket_config_mutation;
    use http::Method;
    use rustfs_credentials::Credentials;
    use rustfs_policy::policy::action::{Action, S3Action};
    use serial_test::serial;
    use std::sync::Mutex;

    const ORIGINAL_XML: &[u8] =
        b"<PublicAccessBlockConfiguration><BlockPublicPolicy>true</BlockPublicPolicy></PublicAccessBlockConfiguration>";

    async fn public_access_block_mutation(put: bool, stale: bool) {
        let (method, action, operation, reload, status) = if put {
            (
                Method::PUT,
                S3Action::PutBucketPublicAccessBlockAction,
                S3Operation::PutPublicAccessBlock,
                "put public access block",
                200,
            )
        } else {
            (
                Method::DELETE,
                S3Action::DeleteBucketPublicAccessBlockAction,
                S3Operation::DeletePublicAccessBlock,
                "delete public access block",
                204,
            )
        };
        let context = shared_gating_ambient().await;
        let store = context.object_store();
        let bucket = format!("public-access-block-{}", uuid::Uuid::new_v4().simple());
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create bucket");

        let server = ServerContextSlot::new();
        assert!(server.install(Arc::clone(&context)));
        let mut request = build_request((), method);
        request.uri = format!("/{bucket}?publicAccessBlock").parse().expect("request URI");
        request.extensions.insert(Arc::clone(&server));
        request.extensions.insert(ReqInfo {
            cred: Some(Credentials {
                access_key: "public-access-block-owner".to_owned(),
                ..Default::default()
            }),
            is_owner: true,
            ..Default::default()
        });
        request.extensions.insert(request_context::RequestContext {
            request_id: "public-access-block-fixture".to_owned(),
            x_amz_request_id: "public-access-block-fixture".to_owned(),
            trace_id: None,
            span_id: None,
            start_time: std::time::Instant::now(),
        });
        let mut envelope = RequestEnvelope::from_s3s(&request);
        authorize_bucket_config_mutation(&server, &mut envelope, &bucket, Action::S3Action(action))
            .await
            .expect("bucket owner authorization installs the incarnation snapshot");

        if stale {
            store
                .delete_bucket(&bucket, &DeleteBucketOptions::default())
                .await
                .expect("delete authorized bucket");
            store
                .make_bucket(&bucket, &MakeBucketOptions::default())
                .await
                .expect("recreate bucket");
        }
        metadata_sys::update(&bucket, BUCKET_PUBLIC_ACCESS_BLOCK_CONFIG, ORIGINAL_XML.to_vec())
            .await
            .expect("seed config");

        let usecase = DefaultBucketUsecase::with_context(Some(context));
        let audits = MutationCompletionProbe::install(&bucket);
        let reloads = Arc::new(Mutex::new(Vec::new()));
        let record = Arc::clone(&reloads);
        let result = BUCKET_METADATA_RELOAD_PROBE
            .scope(
                (
                    bucket.clone(),
                    Box::new(move |completed, label, maintenance| {
                        record.lock().expect("reload observer").push((completed, label, maintenance));
                    }),
                ),
                async {
                    if put {
                        let config = PersistedPublicAccessBlockConfiguration {
                            block_public_policy: Some(false),
                            ..Default::default()
                        };
                        usecase.execute_put_public_access_block(envelope, &bucket, Ok(config)).await
                    } else {
                        usecase.execute_delete_public_access_block(envelope, &bucket).await
                    }
                },
            )
            .await;
        let stored = metadata_sys::get_global_bucket_metadata_sys()
            .expect("initialized metadata system")
            .read()
            .await
            .get_config_from_disk(&bucket)
            .await
            .expect("persisted bucket metadata")
            .public_access_block_config_xml;
        let reloads = reloads.lock().expect("recorded reloads");
        if stale {
            assert_eq!(
                result.expect_err("stale authorization must not mutate replacement").code(),
                &S3ErrorCode::NoSuchBucket
            );
            assert_eq!(stored, ORIGINAL_XML);
            assert!(reloads.is_empty());
        } else {
            result.expect("authorized mutation");
            if put {
                assert_eq!(stored, b"<PublicAccessBlockConfiguration><BlockPublicPolicy>false</BlockPublicPolicy></PublicAccessBlockConfiguration>");
            } else {
                assert!(stored.is_empty());
            }
            assert_eq!(
                *reloads,
                [(false, reload, false), (true, reload, false)],
                "metadata reload completes without scanner maintenance"
            );
            let entries = audits.audits.lock().expect("audit entries");
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].api.name.as_deref(), Some(operation.as_str()));
            assert_eq!(entries[0].api.status_code, Some(status));
            assert!(audits.events.lock().expect("event entries").is_empty());
        }
    }

    #[test]
    #[serial]
    fn put_public_access_block_rejects_stale_authorization() {
        run_large_stack_test("public-access-block-put-stale", || public_access_block_mutation(true, true));
    }

    #[test]
    #[serial]
    fn delete_public_access_block_rejects_stale_authorization() {
        run_large_stack_test("public-access-block-delete-stale", || public_access_block_mutation(false, true));
    }

    #[test]
    #[serial]
    fn put_public_access_block_reloads_and_audits() {
        run_large_stack_test("public-access-block-put-completion", || public_access_block_mutation(true, false));
    }

    #[test]
    #[serial]
    fn delete_public_access_block_reloads_and_audits() {
        run_large_stack_test("public-access-block-delete-completion", || public_access_block_mutation(false, false));
    }

    #[tokio::test]
    async fn execute_delete_public_access_block_returns_internal_error_when_store_uninitialized() {
        let envelope = RequestEnvelope::from_s3s(&build_request((), Method::DELETE));
        let usecase = DefaultBucketUsecase::without_context();
        let err = usecase
            .execute_delete_public_access_block(envelope, "test-bucket")
            .await
            .unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }

    #[tokio::test]
    async fn execute_get_public_access_block_returns_internal_error_when_store_uninitialized() {
        let usecase = DefaultBucketUsecase::without_context();
        let err = usecase.execute_get_public_access_block("test-bucket").await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }

    #[tokio::test]
    async fn execute_put_public_access_block_returns_internal_error_when_store_uninitialized() {
        let envelope = RequestEnvelope::from_s3s(&build_request((), Method::PUT));
        let usecase = DefaultBucketUsecase::without_context();
        let err = usecase
            .execute_put_public_access_block(envelope, "test-bucket", Ok(PersistedPublicAccessBlockConfiguration::default()))
            .await
            .unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }
}
