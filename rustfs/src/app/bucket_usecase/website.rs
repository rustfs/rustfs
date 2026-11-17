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

//! Bucket website configuration shared by the S3 edges.

use super::BUCKET_WEBSITE_CONFIG;
use super::{
    ApiError, DefaultBucketUsecase, S3Operation, StatusCode, StorageError, bucket_config_mutation_incarnation,
    delete_bucket_config_for_incarnation, metadata_sys, serialize, update_bucket_config_for_incarnation,
};
use crate::app::RequestEnvelope;
use crate::app::storage_api::bucket_usecase::validate_website_configuration;
use rustfs_gateway_types::persistence::PersistedWebsiteConfiguration;
use rustfs_s3_types::S3Result;

impl DefaultBucketUsecase {
    pub(crate) async fn execute_get_bucket_website(&self, bucket: &str) -> S3Result<PersistedWebsiteConfiguration> {
        self.ensure_config_bucket(bucket).await?;
        match metadata_sys::get_website_config(bucket).await {
            Ok((config, _)) => Ok(config),
            Err(StorageError::ConfigNotFound) => Err(rustfs_s3_types::s3_error!(
                NoSuchWebsiteConfiguration,
                "The specified bucket does not have a website configuration."
            )),
            Err(err) => Err(ApiError::from(err).into()),
        }
    }

    pub(crate) async fn execute_put_bucket_website(
        &self,
        envelope: RequestEnvelope,
        bucket: &str,
        configuration: impl FnOnce() -> S3Result<PersistedWebsiteConfiguration> + Send,
    ) -> S3Result<()> {
        RequestEnvelope::audit_only_operation(envelope, S3Operation::PutBucketWebsite, StatusCode::OK, |envelope| async move {
            let expected_incarnation_id = bucket_config_mutation_incarnation(&envelope, bucket)?;
            self.ensure_config_bucket(bucket).await?;

            let config = configuration()?;
            validate_website_configuration(&config)?;
            let data = serialize(&config).map_err(|err| rustfs_s3_types::s3_error!(MalformedXML, "{err}"))?;
            update_bucket_config_for_incarnation(bucket, BUCKET_WEBSITE_CONFIG, data, expected_incarnation_id)
                .await
                .map_err(ApiError::from)?;
            Ok(())
        })
        .await
    }

    pub(crate) async fn execute_delete_bucket_website(&self, envelope: RequestEnvelope, bucket: &str) -> S3Result<()> {
        RequestEnvelope::audit_only_operation(
            envelope,
            S3Operation::DeleteBucketWebsite,
            StatusCode::NO_CONTENT,
            |envelope| async move {
                let expected_incarnation_id = bucket_config_mutation_incarnation(&envelope, bucket)?;
                self.ensure_config_bucket(bucket).await?;

                delete_bucket_config_for_incarnation(bucket, BUCKET_WEBSITE_CONFIG, expected_incarnation_id)
                    .await
                    .map_err(ApiError::from)?;
                Ok(())
            },
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::super::{BUCKET_ACCELERATE_CONFIG, BUCKET_REQUEST_PAYMENT_CONFIG};
    use super::*;
    use crate::app::bucket_usecase::test_support::{build_request, build_request_with_req_info};
    use crate::app::gating_test_env::{run_large_stack_test, shared_gating_ambient};
    use crate::app::runtime_sources::ServerContextSlot;
    use crate::app::storage_api::object_usecase::helper::MutationCompletionProbe;
    use crate::app::storage_api::owned_parity::{ReqInfo, authorize_bucket_config_mutation};
    use rustfs_credentials::Credentials;
    use rustfs_gateway_types::persistence::{parse_accelerate, parse_request_payment, parse_website};
    use rustfs_policy::policy::action::{Action, S3Action};
    use rustfs_s3_types::S3ErrorCode;
    use serial_test::serial;
    use std::sync::Arc;

    #[test]
    #[serial]
    fn core_config_mutations_emit_one_audit_entry_for_success_and_failure() {
        use super::super::{BucketOperations, MakeBucketOptions};

        run_large_stack_test("core-config-audit", || async {
            let ambient = shared_gating_ambient().await;
            let store = ambient.object_store();
            let usecase = DefaultBucketUsecase::with_context(Some(Arc::clone(&ambient)));
            for exists in [false, true] {
                let bucket = format!("core-config-audit-{}", uuid::Uuid::new_v4().simple());
                if exists {
                    store
                        .make_bucket(&bucket, &MakeBucketOptions::default())
                        .await
                        .expect("create audit fixture bucket");
                }
                for (method, operation, success_status) in [
                    (http::Method::PUT, S3Operation::PutBucketAccelerateConfiguration, StatusCode::OK),
                    (http::Method::PUT, S3Operation::PutBucketRequestPayment, StatusCode::OK),
                    (http::Method::PUT, S3Operation::PutBucketWebsite, StatusCode::OK),
                    (http::Method::DELETE, S3Operation::DeleteBucketWebsite, StatusCode::NO_CONTENT),
                ] {
                    let probe = MutationCompletionProbe::install(&bucket);
                    let mut request = build_request_with_req_info(
                        (),
                        method,
                        ReqInfo {
                            bucket: Some(bucket.clone()),
                            ..Default::default()
                        },
                    );
                    request.uri = format!("/{bucket}").parse().expect("audit request URI");
                    let envelope = RequestEnvelope::from_s3s(&request);
                    let result = match operation {
                        S3Operation::PutBucketAccelerateConfiguration => {
                            usecase
                                .execute_put_bucket_accelerate_configuration(envelope, &bucket, || {
                                    Ok(parse_accelerate(
                                        b"<AccelerateConfiguration><Status>Enabled</Status></AccelerateConfiguration>",
                                    )
                                    .expect("valid accelerate fixture"))
                                })
                                .await
                        }
                        S3Operation::PutBucketRequestPayment => {
                            usecase
                                .execute_put_bucket_request_payment(envelope, &bucket, || {
                                    Ok(parse_request_payment(
                                        b"<RequestPaymentConfiguration><Payer>BucketOwner</Payer></RequestPaymentConfiguration>",
                                    )
                                    .expect("valid request payment fixture"))
                                })
                                .await
                        }
                        S3Operation::PutBucketWebsite => {
                            usecase
                                .execute_put_bucket_website(envelope, &bucket, || {
                                    Ok(parse_website(
                                        b"<WebsiteConfiguration><IndexDocument><Suffix>index.html</Suffix></IndexDocument></WebsiteConfiguration>",
                                    )
                                    .expect("valid website fixture"))
                                })
                                .await
                        }
                        S3Operation::DeleteBucketWebsite => usecase.execute_delete_bucket_website(envelope, &bucket).await,
                        _ => unreachable!("only core bucket configuration mutations"),
                    };
                    if exists {
                        result.expect("persist bucket configuration");
                    } else {
                        assert_eq!(result.expect_err("missing bucket").code(), &S3ErrorCode::NoSuchBucket);
                    }
                    let audits = probe.audits.lock().expect("captured audits");
                    assert_eq!(audits.len(), 1, "{} exists={exists}", operation.as_str());
                    assert_eq!(audits[0].api.name.as_deref(), Some(operation.as_str()));
                    assert_eq!(audits[0].api.bucket.as_deref(), Some(bucket.as_str()));
                    assert_eq!(
                        audits[0].api.status_code,
                        Some(if exists { i32::from(success_status.as_u16()) } else { 404 })
                    );
                    assert!(
                        probe.events.lock().expect("captured events").is_empty(),
                        "audit-only operation must not emit an object event"
                    );
                }
            }
        });
    }

    #[test]
    #[serial]
    fn core_config_puts_defer_conversion_until_after_bucket_lookup() {
        run_large_stack_test("core-config-lookup-before-conversion", || async {
            let ambient = shared_gating_ambient().await;
            let usecase = DefaultBucketUsecase::with_context(Some(ambient));
            let bucket = format!("core-config-missing-{}", uuid::Uuid::new_v4().simple());
            let request = build_request((), http::Method::PUT);
            let mut converted = [false; 3];
            let accelerate = usecase
                .execute_put_bucket_accelerate_configuration(RequestEnvelope::from_s3s(&request), &bucket, || {
                    converted[0] = true;
                    Err(rustfs_s3_types::s3_error!(MalformedXML, "invalid acceleration"))
                })
                .await;
            let payment = usecase
                .execute_put_bucket_request_payment(RequestEnvelope::from_s3s(&request), &bucket, || {
                    converted[1] = true;
                    Err(rustfs_s3_types::s3_error!(MalformedXML, "invalid request payment"))
                })
                .await;
            let website = usecase
                .execute_put_bucket_website(RequestEnvelope::from_s3s(&request), &bucket, || {
                    converted[2] = true;
                    Err(rustfs_s3_types::s3_error!(MalformedXML, "invalid website"))
                })
                .await;
            assert_eq!(
                [accelerate, payment, website].map(|result| result.err().map(|error| error.code().clone())),
                [
                    Some(S3ErrorCode::NoSuchBucket),
                    Some(S3ErrorCode::NoSuchBucket),
                    Some(S3ErrorCode::NoSuchBucket)
                ],
            );
            assert_eq!(converted, [false; 3]);
        });
    }

    #[test]
    #[serial]
    fn core_config_mutations_reject_stale_incarnations() {
        use super::super::{BucketOperations, DeleteBucketOptions, MakeBucketOptions};
        const OLD_ACCELERATE: &[u8] = b"<AccelerateConfiguration><Status>Suspended</Status></AccelerateConfiguration>";
        const NEW_ACCELERATE: &[u8] = b"<AccelerateConfiguration><Status>Enabled</Status></AccelerateConfiguration>";
        const OLD_PAYMENT: &[u8] = b"<RequestPaymentConfiguration><Payer>BucketOwner</Payer></RequestPaymentConfiguration>";
        const NEW_PAYMENT: &[u8] = b"<RequestPaymentConfiguration><Payer>Requester</Payer></RequestPaymentConfiguration>";
        const OLD_WEBSITE: &[u8] =
            b"<WebsiteConfiguration><IndexDocument><Suffix>old.html</Suffix></IndexDocument></WebsiteConfiguration>";
        const NEW_WEBSITE: &[u8] =
            b"<WebsiteConfiguration><IndexDocument><Suffix>new.html</Suffix></IndexDocument></WebsiteConfiguration>";

        run_large_stack_test("core-config-stale-incarnation", || async {
            let ambient = shared_gating_ambient().await;
            let store = ambient.object_store();
            let server = ServerContextSlot::new();
            assert!(server.install(Arc::clone(&ambient)));
            let usecase = DefaultBucketUsecase::with_context(Some(Arc::clone(&ambient)));

            let mut results = Vec::new();
            for (method, query, file, before) in [
                (http::Method::PUT, "accelerate", BUCKET_ACCELERATE_CONFIG, OLD_ACCELERATE),
                (http::Method::PUT, "requestPayment", BUCKET_REQUEST_PAYMENT_CONFIG, OLD_PAYMENT),
                (http::Method::PUT, "website", BUCKET_WEBSITE_CONFIG, OLD_WEBSITE),
                (http::Method::DELETE, "website", BUCKET_WEBSITE_CONFIG, OLD_WEBSITE),
            ] {
                let bucket = format!("core-config-stale-{}", uuid::Uuid::new_v4().simple());
                store
                    .make_bucket(&bucket, &MakeBucketOptions::default())
                    .await
                    .expect("create original bucket");
                let mut request = build_request((), method.clone());
                request.uri = format!("/{bucket}?{query}").parse().expect("bucket request URI");
                request.extensions.insert(Arc::clone(&server));
                request.extensions.insert(ReqInfo {
                    cred: Some(Credentials {
                        access_key: "core-config-owner".to_owned(),
                        ..Default::default()
                    }),
                    is_owner: true,
                    ..Default::default()
                });
                let mut envelope = RequestEnvelope::from_s3s(&request);
                authorize_bucket_config_mutation(
                    &server,
                    &mut envelope,
                    &bucket,
                    Action::S3Action(S3Action::PutBucketPolicyAction),
                )
                .await
                .expect("owner authorization captures the original incarnation");
                store
                    .delete_bucket(&bucket, &DeleteBucketOptions::default())
                    .await
                    .expect("delete original bucket");
                store
                    .make_bucket(&bucket, &MakeBucketOptions::default())
                    .await
                    .expect("create replacement bucket");
                metadata_sys::update(&bucket, file, before.to_vec())
                    .await
                    .expect("seed replacement configuration");

                let result = match (method, query) {
                    (http::Method::PUT, "accelerate") => {
                        usecase
                            .execute_put_bucket_accelerate_configuration(envelope, &bucket, || {
                                Ok(parse_accelerate(NEW_ACCELERATE).expect("changed accelerate document"))
                            })
                            .await
                    }
                    (http::Method::PUT, "requestPayment") => {
                        usecase
                            .execute_put_bucket_request_payment(envelope, &bucket, || {
                                Ok(parse_request_payment(NEW_PAYMENT).expect("changed request payment document"))
                            })
                            .await
                    }
                    (http::Method::PUT, "website") => {
                        usecase
                            .execute_put_bucket_website(envelope, &bucket, || {
                                Ok(parse_website(NEW_WEBSITE).expect("changed website document"))
                            })
                            .await
                    }
                    (http::Method::DELETE, "website") => usecase.execute_delete_bucket_website(envelope, &bucket).await,
                    _ => unreachable!("only core bucket configuration mutations"),
                };

                let stored = metadata_sys::get_global_bucket_metadata_sys()
                    .expect("initialized metadata system")
                    .read()
                    .await
                    .get_config_from_disk(&bucket)
                    .await
                    .expect("replacement metadata");
                let after = match file {
                    BUCKET_ACCELERATE_CONFIG => stored.accelerate_config_xml,
                    BUCKET_REQUEST_PAYMENT_CONFIG => stored.request_payment_config_xml,
                    BUCKET_WEBSITE_CONFIG => stored.website_config_xml,
                    _ => unreachable!("only core bucket configurations"),
                };
                results.push((query, result.err().map(|err| err.code().clone()), after == before));
            }
            assert_eq!(
                results,
                ["accelerate", "requestPayment", "website", "website"].map(|query| (
                    query,
                    Some(S3ErrorCode::NoSuchBucket),
                    true
                )),
                "each operation must reject the stale authorization and preserve the replacement bytes"
            );
        });
    }
}
