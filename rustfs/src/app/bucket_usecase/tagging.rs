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

//! Bucket tagging use-case operations.

use super::*;
use crate::app::storage_api::s3::dto;
use crate::app::storage_api::s3::dto::{
    DeleteBucketTaggingInput, DeleteBucketTaggingOutput, GetBucketTaggingInput, PutBucketTaggingInput, PutBucketTaggingOutput,
};
use crate::app::storage_api::s3::{S3Error, S3ErrorCode, S3Request, S3Response, S3Result};

impl DefaultBucketUsecase {
    #[instrument(level = "debug", skip(self, req))]
    pub async fn execute_delete_bucket_tagging(
        &self,
        req: S3Request<DeleteBucketTaggingInput>,
    ) -> S3Result<S3Response<DeleteBucketTaggingOutput>> {
        let expected_incarnation_id = bucket_config_mutation_incarnation(&req, &req.input.bucket)?;
        let request_context = req.extensions.get::<request_context::RequestContext>().cloned();
        let DeleteBucketTaggingInput { bucket, .. } = req.input;

        metadata_sys::update_s3_tagging_config(&bucket, None, expected_incarnation_id)
            .await
            .map_err(ApiError::from)?;

        notify_bucket_metadata_reload(bucket.clone(), "delete bucket tagging", request_context, false).await;

        let item = sr_bucket_meta_item(bucket.clone(), "tags");
        if let Err(err) = site_replication_bucket_meta_hook(item).await {
            warn!(bucket = %bucket, error = ?err, "site replication bucket tagging delete hook failed");
        }

        rustfs_scanner::record_dirty_usage_bucket_from_producer(
            &bucket,
            rustfs_scanner::SegmentInvalidationProducerIdentity::BucketMetadata,
        );
        Ok(S3Response::new(DeleteBucketTaggingOutput {}))
    }

    #[instrument(level = "debug", skip(self, req))]
    pub async fn execute_get_bucket_tagging(&self, req: S3Request<GetBucketTaggingInput>) -> S3Result<PersistedTagging> {
        let GetBucketTaggingInput { bucket, .. } = req.input;

        let Some(store) = self.object_store() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        let tagging = match metadata_sys::get_tagging_config(&bucket).await {
            Ok((tags, _)) => tags,
            Err(err) => {
                if err == StorageError::ConfigNotFound {
                    return Err(S3Error::with_message(S3ErrorCode::NoSuchTagSet, "The TagSet does not exist".to_string()));
                }
                warn!(
                    component = LOG_COMPONENT_APP,
                    subsystem = LOG_SUBSYSTEM_BUCKET,
                    event = "bucket_tagging_config_load_failed",
                    bucket = %bucket,
                    error = ?err,
                    "Failed to load bucket tagging configuration"
                );
                return Err(ApiError::from(err).into());
            }
        };

        Ok(tagging)
    }

    #[instrument(level = "debug", skip(self, req))]
    pub async fn execute_put_bucket_tagging(
        &self,
        req: S3Request<PutBucketTaggingInput>,
        tagging: PersistedTagging,
    ) -> S3Result<S3Response<PutBucketTaggingOutput>> {
        let expected_incarnation_id = bucket_config_mutation_incarnation(&req, &req.input.bucket)?;
        let request_context = req.extensions.get::<request_context::RequestContext>().cloned();
        let PutBucketTaggingInput { bucket, .. } = req.input;
        let s3_tag_set: Vec<dto::Tag> = tagging
            .tag_set
            .iter()
            .map(|tag| dto::Tag {
                key: tag.key.clone(),
                value: tag.value.clone(),
            })
            .collect();
        crate::app::storage_api::bucket_usecase::s3_api::tagging::validate_bucket_tag_set(&s3_tag_set)?;

        let Some(store) = self.object_store() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        metadata_sys::update_s3_tagging_config(&bucket, Some(tagging.clone()), expected_incarnation_id)
            .await
            .map_err(ApiError::from)?;

        notify_bucket_metadata_reload(bucket.clone(), "put bucket tagging", request_context, false).await;

        let mut item = sr_bucket_meta_item(bucket.clone(), "tags");
        item.tags = Some(serialize_config(&tagging).and_then(|bytes| String::from_utf8(bytes).map_err(to_internal_error))?);
        if let Err(err) = site_replication_bucket_meta_hook(item).await {
            warn!(bucket = %bucket, error = ?err, "site replication bucket tagging hook failed");
        }

        rustfs_scanner::record_dirty_usage_bucket_from_producer(
            &bucket,
            rustfs_scanner::SegmentInvalidationProducerIdentity::BucketMetadata,
        );
        Ok(S3Response::new(PutBucketTaggingOutput::default()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::bucket_usecase::test_support::build_request;
    use crate::app::storage_api::s3::dto::{Tag, Tagging};
    use http::Method;

    #[tokio::test]
    async fn execute_get_bucket_tagging_returns_internal_error_when_store_uninitialized() {
        let input = GetBucketTaggingInput::builder()
            .bucket("test-bucket".to_string())
            .build()
            .unwrap();

        let req = build_request(input, Method::GET);
        let usecase = DefaultBucketUsecase::without_context();

        let err = usecase.execute_get_bucket_tagging(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }

    #[test]
    #[serial_test::serial]
    fn put_bucket_tagging_rejects_invalid_shapes_before_storage_mutation() {
        crate::app::gating_test_env::run_large_stack_test("bucket-tag-validation", || async {
            use crate::app::runtime_sources::{AppContext, ServerContextSlot};
            use crate::app::storage_api::s3::S3Access as _;
            use crate::app::storage_api::test::ReqInfo;
            use crate::app::storage_api::test::ecfs::FS;
            let ambient = crate::app::gating_test_env::shared_gating_ambient().await;
            let store = crate::app::gating_test_env::shared_gating_ecstore().await;
            let context = Arc::new(AppContext::new(store.clone(), ambient.iam(), ambient.kms()));
            let slot = ServerContextSlot::new();
            assert!(slot.install(context.clone()));
            let fs = FS::with_server_ctx(slot.clone());
            let usecase = DefaultBucketUsecase::with_context(Some(context));
            let bucket = format!("tag-validation-{}", uuid::Uuid::new_v4().simple());
            store.make_bucket(&bucket, &MakeBucketOptions::default()).await.unwrap();
            store
                .update_bucket_metadata_config(
                    &bucket,
                    BUCKET_POLICY_CONFIG,
                    serde_json::to_vec(
                        &serde_json::json!({"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},
                    "Action":"s3:PutBucketTagging","Resource":format!("arn:aws:s3:::{bucket}")}]}),
                    )
                    .unwrap(),
                )
                .await
                .unwrap();
            let tag = dto::Tag {
                key: Some("team".to_owned()),
                value: Some("storage".to_owned()),
            };
            for (tag_set, valid) in [
                (vec![tag.clone()], true),
                (
                    vec![dto::Tag {
                        key: Some("team".to_owned()),
                        value: None,
                    }],
                    false,
                ),
                (vec![tag.clone(), tag], false),
            ] {
                let persisted = PersistedTagging {
                    tag_set: tag_set
                        .iter()
                        .map(|tag| rustfs_gateway_types::cors_tagging::PersistedTag {
                            key: tag.key.clone(),
                            value: tag.value.clone(),
                        })
                        .collect(),
                };
                let input = PutBucketTaggingInput::builder()
                    .bucket(bucket.clone())
                    .tagging(dto::Tagging { tag_set })
                    .build()
                    .unwrap();
                let mut req = build_request(input, Method::PUT);
                req.extensions.insert(ReqInfo::default());
                req.extensions.insert(slot.clone());
                fs.put_bucket_tagging(&mut req).await.expect("permission allows tag updates");
                let result = usecase.execute_put_bucket_tagging(req, persisted).await;
                if valid {
                    result.expect("valid tags are persisted");
                } else {
                    assert_eq!(result.unwrap_err().code(), &S3ErrorCode::InvalidTag);
                }
                assert_eq!(
                    store.get_bucket_tags_for_policy(&bucket).await.unwrap(),
                    HashMap::from([("team".to_owned(), "storage".to_owned())])
                );
            }
        });
    }

    #[tokio::test]
    async fn execute_put_bucket_tagging_returns_internal_error_when_store_uninitialized() {
        let input = PutBucketTaggingInput::builder()
            .bucket("test-bucket".to_string())
            .tagging(Tagging {
                tag_set: vec![Tag {
                    key: Some("env".to_string()),
                    value: Some("prod".to_string()),
                }],
            })
            .build()
            .unwrap();

        let req = build_request(input, Method::PUT);
        let usecase = DefaultBucketUsecase::without_context();

        let err = usecase
            .execute_put_bucket_tagging(req, PersistedTagging::default())
            .await
            .unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }
}
