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

//! Bucket logging configuration shared by the S3 edges.

use super::{
    ApiError, DefaultBucketUsecase, S3Operation, StatusCode, StorageError, bucket_config_mutation_incarnation, metadata_sys,
    update_bucket_config_for_incarnation,
};
use crate::app::RequestEnvelope;
use crate::app::storage_api::bucket_usecase::bucket::metadata::BUCKET_LOGGING_CONFIG;
use rustfs_gateway_types::persistence::PersistedBucketLoggingStatus;
use rustfs_s3_types::S3Result;

impl DefaultBucketUsecase {
    pub(crate) async fn execute_put_bucket_logging(
        &self,
        envelope: RequestEnvelope,
        bucket: &str,
        serialize_configuration: impl FnOnce() -> S3Result<Vec<u8>> + Send,
    ) -> S3Result<()> {
        RequestEnvelope::audit_only_operation(envelope, S3Operation::PutBucketLogging, StatusCode::OK, |envelope| async move {
            let expected_incarnation_id = bucket_config_mutation_incarnation(&envelope, bucket)?;
            self.ensure_config_bucket(bucket).await?;

            let data = serialize_configuration()?;
            update_bucket_config_for_incarnation(bucket, BUCKET_LOGGING_CONFIG, data, expected_incarnation_id)
                .await
                .map_err(ApiError::from)?;
            Ok(())
        })
        .await
    }

    pub(crate) async fn execute_get_bucket_logging(&self, bucket: &str) -> S3Result<PersistedBucketLoggingStatus> {
        self.ensure_config_bucket(bucket).await?;
        match metadata_sys::get_logging_config(bucket).await {
            Ok((config, _)) => Ok(config),
            Err(StorageError::ConfigNotFound) => Ok(PersistedBucketLoggingStatus::default()),
            Err(err) => Err(ApiError::from(err).into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::bucket_usecase::test_support::build_request;
    use crate::app::bucket_usecase::{BucketOperations, DeleteBucketOptions, MakeBucketOptions};
    use crate::app::gating_test_env::{run_large_stack_test, shared_gating_ambient};
    use crate::app::runtime_sources::ServerContextSlot;
    use crate::app::storage_api::owned_parity::{ReqInfo, authorize_bucket_config_mutation};
    use http::Method;
    use rustfs_credentials::Credentials;
    use rustfs_policy::policy::action::{Action, S3Action};
    use rustfs_s3_types::S3ErrorCode;
    use serial_test::serial;
    use std::sync::Arc;

    #[test]
    #[serial]
    fn put_bucket_logging_checks_bucket_before_serializing() {
        run_large_stack_test("put-bucket-logging-lookup", || async {
            let context = shared_gating_ambient().await;
            let bucket = format!("logging-missing-{}", uuid::Uuid::new_v4().simple());
            let usecase = DefaultBucketUsecase::with_context(Some(context));
            let request = build_request((), Method::PUT);
            let envelope = RequestEnvelope::from_s3s(&request);
            let error = usecase
                .execute_put_bucket_logging(envelope, &bucket, || {
                    Err(rustfs_s3_types::s3_error!(MalformedXML, "invalid logging configuration"))
                })
                .await
                .unwrap_err();
            assert_eq!(error.code(), &S3ErrorCode::NoSuchBucket);
        });
    }

    #[test]
    #[serial]
    fn put_bucket_logging_preserves_replacement_bytes_after_stale_authorization() {
        run_large_stack_test("put-bucket-logging-incarnation", || async {
            const ORIGINAL: &[u8] = b"<BucketLoggingStatus></BucketLoggingStatus>";
            const UPDATE: &[u8] = b"<BucketLoggingStatus><LoggingEnabled><TargetBucket>other-bucket</TargetBucket><TargetPrefix>logs/</TargetPrefix></LoggingEnabled></BucketLoggingStatus>";
            let context = shared_gating_ambient().await;
            let store = context.object_store();
            let bucket = format!("logging-stale-{}", uuid::Uuid::new_v4().simple());
            store
                .make_bucket(&bucket, &MakeBucketOptions::default())
                .await
                .expect("create original bucket");
            let server = ServerContextSlot::new();
            assert!(server.install(Arc::clone(&context)));
            let mut request = build_request((), Method::PUT);
            request.uri = format!("/{bucket}?logging").parse().expect("logging request URI");
            request.extensions.insert(Arc::clone(&server));
            request.extensions.insert(ReqInfo {
                cred: Some(Credentials {
                    access_key: "logging-owner".to_owned(),
                    ..Default::default()
                }),
                is_owner: true,
                ..Default::default()
            });
            let mut envelope = RequestEnvelope::from_s3s(&request);
            authorize_bucket_config_mutation(&server, &mut envelope, &bucket, Action::S3Action(S3Action::PutBucketLoggingAction))
                .await
                .expect("snapshot the original bucket incarnation");

            store
                .delete_bucket(&bucket, &DeleteBucketOptions::default())
                .await
                .expect("delete original bucket");
            store
                .make_bucket(&bucket, &MakeBucketOptions::default())
                .await
                .expect("create replacement bucket");
            metadata_sys::update(&bucket, BUCKET_LOGGING_CONFIG, ORIGINAL.to_vec())
                .await
                .expect("seed replacement logging config");

            let usecase = DefaultBucketUsecase::with_context(Some(context));
            let result = usecase
                .execute_put_bucket_logging(envelope, &bucket, || Ok(UPDATE.to_vec()))
                .await;
            let stored = metadata_sys::get_global_bucket_metadata_sys()
                .expect("initialized metadata system")
                .read()
                .await
                .get_config_from_disk(&bucket)
                .await
                .expect("replacement metadata")
                .logging_config_xml;
            assert_eq!(
                (result.err().map(|err| err.code().clone()), stored.as_slice()),
                (Some(S3ErrorCode::NoSuchBucket), ORIGINAL),
                "the stale authorization must leave the replacement configuration intact"
            );
        });
    }
}
