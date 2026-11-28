// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

mod catalog_worker_replay;
mod delete_bucket;
mod http_socket;
mod index_metadata;
mod request_context;
mod sdk_acceptance;

use super::*;
use crate::app::gating_test_env::{run_large_stack_test, shared_gating_ambient};
use crate::app::{RequestCredentials, RequestHead};
use crate::runtime_sources::AppContext;
use rustfs_credentials::Credentials;
use rustfs_iam::store::{Store, UserType, object::ObjectStore};
use rustfs_madmin::{AccountStatus, AddOrUpdateUserReq};
use rustfs_policy::policy::Policy;
use serde_json::{Value, json};

pub(super) struct Fixture {
    pub(super) app: Arc<AppContext>,
    pub(super) service: DefaultVectorUsecase,
    pub(super) credentials: Credentials,
    policy: String,
}

impl Fixture {
    pub(super) async fn new() -> Self {
        let app = shared_gating_ambient().await;
        let suffix = Uuid::new_v4().simple().to_string();
        if app.action_credentials().get().is_none() {
            assert!(app.publish_action_credentials(Credentials {
                access_key: format!("vector-root-{suffix}"),
                secret_key: Uuid::new_v4().simple().to_string(),
                status: "on".into(),
                ..Default::default()
            }));
        }
        ObjectStore::new(app.object_store())
            .save_iam_config(
                json!({"version":1}),
                format!("{}/format.json", *rustfs_iam::store::object::IAM_CONFIG_PREFIX),
            )
            .await
            .unwrap();
        let iam = app.iam().handle();
        let user = format!("vector-app-user-{suffix}");
        iam.create_user(
            &user,
            &AddOrUpdateUserReq {
                secret_key: Uuid::new_v4().simple().to_string(),
                policy: None,
                status: AccountStatus::Enabled,
            },
        )
        .await
        .unwrap();
        let policy = format!("vector-app-policy-{suffix}");
        iam.set_policy(
            &policy,
            Policy::parse_config(
                &serde_json::to_vec(&json!({
                    "Version":"2012-10-17", "Statement":[
                        {"Effect":"Allow","Action":"s3vectors:*","Resource":"*"},
                        {"Effect":"Deny","Action":"s3:*","Resource":"*"}
                    ]
                }))
                .unwrap(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
        iam.policy_db_set(&user, UserType::Reg, false, &policy).await.unwrap();
        let (credentials, owner) = crate::auth::check_key_valid_with_context("", &user, Some(&app))
            .await
            .unwrap();
        assert!(!owner);
        let slot = ServerContextSlot::new();
        assert!(slot.install(app.clone()));
        let service = DefaultVectorUsecase::new(
            slot,
            ManifestIoLimits {
                max_manifest_bytes: std::num::NonZeroU64::new(64 * 1024 * 1024).unwrap(),
                max_concurrent_reads: NonZeroUsize::new(4).unwrap(),
            },
            NonZeroUsize::new(3).unwrap(),
        )
        .unwrap();
        Self {
            app,
            service,
            credentials,
            policy,
        }
    }

    pub(super) fn verified(&self, request: Request) -> admission::VerifiedVectorRequest {
        let mut extensions = http::Extensions::new();
        extensions.insert(self.service.server.clone());
        extensions.insert(crate::app::storage_api::vector_usecase::RequestContext::fallback());
        let envelope = RequestEnvelope::from_parts(
            RequestHead {
                method: http::Method::POST,
                uri: format!("/{}", request.operation().name()).parse().unwrap(),
                headers: http::HeaderMap::new(),
                credentials: Some(
                    RequestCredentials::new(self.credentials.access_key.clone(), self.credentials.secret_key.clone(), None)
                        .unwrap(),
                ),
                region: Some(self.service.region.clone()),
                service: Some("s3vectors".into()),
            },
            extensions,
        );
        admission::VerifiedVectorRequest::for_test(self.service.server.clone(), envelope, request, self.service.region.clone())
    }

    pub(super) async fn execute(&self, request: Request) -> Result<Response, ProtocolError> {
        let admitted = self.service.admit(self.verified(request.clone())).await?;
        self.service.execute_authorized(admitted, request).await
    }

    pub(super) async fn set_statements(&self, statements: Value) {
        self.app
            .iam()
            .handle()
            .set_policy(
                &self.policy,
                Policy::parse_config(
                    &serde_json::to_vec(&json!({
                        "Version":"2012-10-17", "Statement":statements,
                    }))
                    .unwrap(),
                )
                .unwrap(),
            )
            .await
            .unwrap();
    }
}

#[test]
#[serial_test::serial]
fn vector_application_uses_real_iam_and_managed_catalog_without_s3_permissions() {
    run_large_stack_test("vector-app-real-business", || async {
        let fixture = Fixture::new().await;
        let bucket = format!("vectors-{}", Uuid::new_v4().simple());
        fixture
            .execute(Request::CreateVectorBucket(
                serde_json::from_value(json!({"vectorBucketName":bucket})).unwrap(),
            ))
            .await
            .unwrap();
        fixture.execute(Request::CreateIndex(serde_json::from_value(json!({
            "vectorBucketName":bucket,"indexName":"index.one","dataType":"float32","dimension":2,"distanceMetric":"euclidean"
        })).unwrap())).await.unwrap();
        fixture.execute(Request::PutVectors(serde_json::from_value(json!({
            "vectorBucketName":bucket,"indexName":"index.one","vectors":[{"key":"one","data":{"float32":[1.0,2.0]},"metadata":{"kind":"first"}}]
        })).unwrap())).await.unwrap();
        let request = Request::GetVectors(
            serde_json::from_value(json!({
                "vectorBucketName":bucket,"indexName":"index.one","keys":["one"],"returnData":true,"returnMetadata":true
            }))
            .unwrap(),
        );
        let output = fixture.execute(request.clone()).await.unwrap();
        let value: Value = serde_json::from_slice(&output.body_bytes().unwrap()).unwrap();
        assert_eq!(value["vectors"][0]["data"]["float32"], json!([1.0, 2.0]));
        assert_eq!(value["vectors"][0]["metadata"]["kind"], "first");
        let repeated = fixture
            .execute(Request::GetVectors(
                serde_json::from_value(
                    json!({"vectorBucketName":bucket,"indexName":"index.one","keys":["one","missing","one"],"returnData":true}),
                )
                .unwrap(),
            ))
            .await
            .unwrap();
        let repeated: Value = serde_json::from_slice(&repeated.body_bytes().unwrap()).unwrap();
        assert_eq!(repeated["vectors"].as_array().unwrap().len(), 1);
        assert_eq!(repeated["vectors"][0]["key"], "one");
        let admitted = fixture.service.admit(fixture.verified(request.clone())).await.unwrap();
        fixture
            .set_statements(json!([
                {"Effect":"Allow","Action":"s3vectors:*","Resource":"*"},
                {"Effect":"Deny","Action":"s3vectors:GetVectors","Resource":"*"}
            ]))
            .await;
        let denied = fixture.service.execute_authorized(admitted, request).await.unwrap_err();
        assert_eq!(denied.code(), ErrorCode::AccessDenied);
    });
}

#[test]
#[serial_test::serial]
fn vector_application_requires_tag_permission_before_creating_bucket() {
    run_large_stack_test("vector-app-create-tags", || async {
        let fixture = Fixture::new().await;
        fixture
            .set_statements(json!([{"Effect":"Allow","Action":"s3vectors:CreateVectorBucket","Resource":"*"}]))
            .await;
        let bucket = format!("vectors-{}", Uuid::new_v4().simple());
        let request = Request::CreateVectorBucket(
            serde_json::from_value(json!({"vectorBucketName":bucket,"tags":{"team":"search"}})).unwrap(),
        );
        assert_eq!(fixture.execute(request).await.unwrap_err().code(), ErrorCode::AccessDenied);
        let resolver = fixture.service.buckets();
        assert!(
            resolver
                .resolve_bucket_handle(
                    BucketKind::Vector,
                    &AccountNamespaceId::try_new(String::new()).unwrap(),
                    &fixture.service.region,
                    &BucketPublicName::try_new(bucket).unwrap()
                )
                .await
                .is_err()
        );
    });
}

#[test]
fn vector_service_quota_uses_typed_storage_cause_for_modeled_writes() {
    for operation in [Operation::CreateIndex, Operation::PutVectors] {
        for wrapper in ["api", "managed", "s3"] {
            for io_wrapped in [false, true] {
                let api = crate::error::ApiError::from(crate::app::storage_api::vector_usecase::StorageError::QuotaExceeded {
                    current: 5,
                    limit: 4,
                });
                let source: Box<dyn std::error::Error + Send + Sync> = match wrapper {
                    "api" => Box::new(api),
                    "managed" => Box::new(crate::app::object::ManagedObjectError::Api(api)),
                    "s3" => Box::new(crate::app::object::ManagedObjectError::from(rustfs_s3_types::S3Error::from(api))),
                    _ => unreachable!(),
                };
                let source: Box<dyn std::error::Error + Send + Sync> = if io_wrapped {
                    Box::new(std::io::Error::other(source))
                } else {
                    source
                };
                let error = VectorServiceError::Manifest(crate::vector::lance::LanceManifestError::Storage {
                    operation: "create managed manifest",
                    source,
                });
                let response = service_error_for_operation(operation, error).into_response();
                assert_eq!(response.status(), http::StatusCode::PAYMENT_REQUIRED);
                assert_eq!(response.headers()["x-amzn-errortype"], "ServiceQuotaExceededException");
                let body: Value = serde_json::from_slice(response.body()).unwrap();
                assert_eq!(body["__type"], "ServiceQuotaExceededException");
                assert_eq!(body["message"], "Vector storage quota exceeded");
            }
        }
    }
}

#[test]
fn vector_service_quota_preserves_other_refusals_and_operations() {
    let invalid_request = VectorServiceError::Bridge {
        operation: "validate request",
        source: Box::new(crate::error::ApiError {
            code: rustfs_s3_types::S3ErrorCode::InvalidRequest,
            message: "quota words without a typed quota cause".to_owned(),
            source: None,
        }),
    };
    let error = service_error_for_operation(Operation::PutVectors, invalid_request);
    assert_eq!(error.code(), ErrorCode::Validation);
    assert_eq!(error.status_code(), http::StatusCode::BAD_REQUEST);

    for operation in [Operation::DeleteVectors, Operation::QueryVectors, Operation::GetVectors] {
        let quota = || {
            VectorServiceError::Manifest(crate::vector::lance::LanceManifestError::Storage {
                operation: "create managed manifest",
                source: Box::new(crate::app::object::ManagedObjectError::Api(crate::error::ApiError::from(
                    crate::app::storage_api::vector_usecase::StorageError::QuotaExceeded { current: 5, limit: 4 },
                ))),
            })
        };
        assert_eq!(service_error_for_operation(operation, quota()), service_error(quota()));
    }
}

#[test]
fn vector_request_selectors_cover_all_operations() {
    use crate::vector::access_core::test_support::{REGION, request_for};
    for operation in Operation::ALL {
        assert!(RequestTarget::resolve(&request_for(operation), REGION).is_ok(), "{operation:?}");
    }
}

#[test]
#[serial_test::serial]
fn vector_revoked_verified_credential_is_denied_and_audited() {
    run_large_stack_test("vector-app-revoked-audit", || async {
        let fixture = Fixture::new().await;
        let bucket = format!("vectors-{}", Uuid::new_v4().simple());
        let audit = crate::storage::helper::MutationCompletionProbe::install(&bucket);
        let request = Request::CreateVectorBucket(serde_json::from_value(json!({"vectorBucketName":bucket})).unwrap());
        let verified = fixture.verified(request);
        fixture
            .app
            .iam()
            .handle()
            .delete_user(&fixture.credentials.access_key, false)
            .await
            .unwrap();
        let error = match fixture.service.admit(verified).await {
            Ok(_) => panic!("revoked principal was authorized"),
            Err(error) => error,
        };
        assert_eq!(error.code(), ErrorCode::AccessDenied);
        audit.wait_for(1, 0).await;
        let audits = audit.audits.lock().unwrap();
        assert_eq!(audits.len(), 1);
        assert_eq!(audits[0].access_key.as_deref(), Some(fixture.credentials.access_key.as_str()));
        assert_eq!(audits[0].api.name.as_deref(), Some("CreateVectorBucket"));
        assert_eq!(audits[0].api.status_code, Some(403));
    });
}

#[test]
#[serial_test::serial]
fn vector_authorization_reuses_operation_guard_with_waiting_bucket_delete() {
    run_large_stack_test("vector-app-queued-writer", || async {
        use crate::app::storage_api::bucket_usecase::contract::bucket::{BucketOperations, DeleteBucketOptions};
        use crate::vector::service::VectorRequestAccess;
        let fixture = Fixture::new().await;
        let bucket = format!("vectors-{}", Uuid::new_v4().simple());
        fixture
            .execute(Request::CreateVectorBucket(
                serde_json::from_value(json!({"vectorBucketName":bucket})).unwrap(),
            ))
            .await
            .unwrap();
        fixture.execute(Request::CreateIndex(serde_json::from_value(json!({"vectorBucketName":bucket,"indexName":"index.one","dataType":"float32","dimension":2,"distanceMetric":"euclidean"})).unwrap())).await.unwrap();
        let request =
            Request::GetIndex(serde_json::from_value(json!({"vectorBucketName":bucket,"indexName":"index.one"})).unwrap());
        let admitted = fixture.service.admit(fixture.verified(request.clone())).await.unwrap();
        let handle = admitted.bucket.as_ref().unwrap();
        let index = CatalogName::parse("index.one").unwrap();
        let (operation, entry) = admitted
            .catalog
            .as_ref()
            .unwrap()
            .begin_vector_operation(handle, &index_namespace().unwrap(), &index)
            .await
            .unwrap();
        let counts = || {
            crate::app::storage_api::test::bootstrap_instance_ctx()
                .lock_manager()
                .as_fast_lock_manager()
                .unwrap()
                .current_lock_counts()
        };
        let before = counts().1;
        let store = fixture.app.object_store();
        let storage_bucket = handle.storage_bucket_id().as_str().to_owned();
        let writer = tokio::spawn(async move { store.delete_bucket(&storage_bucket, &DeleteBucketOptions::default()).await });
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while counts().1 <= before {
                assert!(!writer.is_finished());
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let context = dataset::IndexRequest {
            owner: &fixture.service,
            authorization: &admitted,
            index: &index,
        };
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            context.authorize_index(&request, operation.resource_identity(), entry.definition(), Some(&operation)),
        )
        .await
        .expect("authorization reuses the lifecycle guard")
        .unwrap();
        drop(operation);
        tokio::time::timeout(std::time::Duration::from_secs(5), writer)
            .await
            .unwrap()
            .unwrap()
            .expect_err("nonempty bucket remains protected");
    });
}

#[test]
#[serial_test::serial]
fn vector_candidate_reader_releases_completed_attempt_before_lifecycle_writer() {
    run_large_stack_test("vector-app-candidate-reader-release", || async {
        use crate::vector::service::VectorRequestAccess;
        let fixture = Fixture::new().await;
        let bucket = format!("vectors-{}", Uuid::new_v4().simple());
        fixture
            .execute(Request::CreateVectorBucket(
                serde_json::from_value(json!({"vectorBucketName":bucket})).unwrap(),
            ))
            .await
            .unwrap();
        fixture.execute(Request::CreateIndex(serde_json::from_value(json!({"vectorBucketName":bucket,"indexName":"index.one","dataType":"float32","dimension":2,"distanceMetric":"euclidean"})).unwrap())).await.unwrap();
        let request = Request::PutVectors(
            serde_json::from_value(
                json!({"vectorBucketName":bucket,"indexName":"index.one","vectors":[{"key":"one","data":{"float32":[1.0,2.0]}}]}),
            )
            .unwrap(),
        );
        let admitted = fixture.service.admit(fixture.verified(request)).await.unwrap();
        let handle = admitted.bucket.as_ref().unwrap();
        let catalog = admitted.catalog.as_ref().unwrap();
        let namespace = index_namespace().unwrap();
        let index = CatalogName::parse("index.one").unwrap();
        let (operation, entry) = catalog.begin_vector_operation(handle, &namespace, &index).await.unwrap();
        let context = dataset::IndexRequest {
            owner: &fixture.service,
            authorization: &admitted,
            index: &index,
        };
        let attempt = context
            .managed_dataset(Arc::new(operation), entry.definition())
            .await
            .unwrap();
        let store = fixture.app.object_store();
        let mut writer = Box::pin(store.lock_bucket_for_recursive_delete(handle.storage_bucket_id().as_str()));
        assert!(futures::poll!(writer.as_mut()).is_pending(), "the active attempt protects its bucket");
        drop(attempt);
        let writer_guard = tokio::time::timeout(std::time::Duration::from_secs(5), writer)
            .await
            .expect("the candidate reader must not retain a completed attempt's lifecycle guard")
            .unwrap();
        let mut next = Box::pin(catalog.begin_vector_operation(handle, &namespace, &index));
        assert!(futures::poll!(next.as_mut()).is_pending(), "the writer precedes the next attempt");
        drop(writer_guard);
        let (operation, entry) = tokio::time::timeout(std::time::Duration::from_secs(5), next)
            .await
            .expect("the next attempt proceeds after the writer")
            .unwrap();
        let attempt = context
            .managed_dataset(Arc::new(operation), entry.definition())
            .await
            .unwrap();
        assert_eq!(attempt.access.bucket(), handle);
        drop(attempt);
    });
}

#[test]
#[serial_test::serial]
fn vector_bucket_policy_and_tags_use_current_iam_and_exact_identity() {
    run_large_stack_test("vector-app-bucket-config", || async {
        let fixture = Fixture::new().await;
        let bucket = format!("vectors-{}", Uuid::new_v4().simple());
        fixture
            .execute(Request::CreateVectorBucket(
                serde_json::from_value(json!({"vectorBucketName":bucket})).unwrap(),
            ))
            .await
            .unwrap();
        let arn = format!("arn:aws:s3vectors:{}::bucket/{bucket}", fixture.service.region);
        fixture
            .execute(Request::TagResource(
                serde_json::from_value(json!({"resourceArn":arn,"tags":{"team":"search"}})).unwrap(),
            ))
            .await
            .unwrap();
        let tags = fixture
            .execute(Request::ListTagsForResource(serde_json::from_value(json!({"resourceArn":arn})).unwrap()))
            .await
            .unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&tags.body_bytes().unwrap()).unwrap()["tags"]["team"],
            "search"
        );
        let policy = json!({"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":"s3vectors:GetVectorBucket","Resource":arn}]}).to_string();
        fixture
            .execute(Request::PutVectorBucketPolicy(
                serde_json::from_value(json!({"vectorBucketName":bucket,"policy":policy})).unwrap(),
            ))
            .await
            .unwrap();
        assert_eq!(
            fixture
                .execute(Request::GetVectorBucket(
                    serde_json::from_value(json!({"vectorBucketName":bucket})).unwrap()
                ))
                .await
                .unwrap_err()
                .code(),
            ErrorCode::AccessDenied
        );
        fixture
            .execute(Request::DeleteVectorBucketPolicy(
                serde_json::from_value(json!({"vectorBucketName":bucket})).unwrap(),
            ))
            .await
            .unwrap();
        fixture
            .execute(Request::GetVectorBucket(
                serde_json::from_value(json!({"vectorBucketName":bucket})).unwrap(),
            ))
            .await
            .unwrap();
    });
}

#[test]
#[serial_test::serial]
fn vector_kms_requires_real_key_permissions_before_provider_access() {
    run_large_stack_test("vector-app-kms-denied", || async {
        let fixture = Fixture::new().await;
        let request = Request::CreateVectorBucket(serde_json::from_value(json!({"vectorBucketName":format!("vectors-{}", Uuid::new_v4().simple()),"encryptionConfiguration":{"sseType":"aws:kms","kmsKeyArn":"arn:aws:kms:us-east-1:123456789012:key/test-key"}})).unwrap());
        assert_eq!(fixture.execute(request).await.unwrap_err().code(), ErrorCode::AccessDenied);
        assert_eq!(
            encryption::requested(None, None).unwrap(),
            crate::table_catalog::VectorEffectiveEncryptionV1::Unencrypted
        );
        let explicit: dto::EncryptionConfiguration = serde_json::from_value(json!({})).unwrap();
        assert_eq!(
            encryption::requested(Some(&explicit), None).unwrap(),
            crate::table_catalog::VectorEffectiveEncryptionV1::Aes256
        );
    });
}

#[test]
#[serial_test::serial]
fn vector_list_indexes_returns_complete_pages_and_validates_continuation() {
    run_large_stack_test("vector-app-list-indexes", || async {
        let fixture = Fixture::new().await;
        let bucket = format!("vectors-{}", Uuid::new_v4().simple());
        fixture
            .execute(Request::CreateVectorBucket(
                serde_json::from_value(json!({"vectorBucketName":bucket})).unwrap(),
            ))
            .await
            .unwrap();
        for index in ["index.one", "other.two"] {
            fixture.execute(Request::CreateIndex(serde_json::from_value(json!({"vectorBucketName":bucket,"indexName":index,"dataType":"float32","dimension":2,"distanceMetric":"euclidean"})).unwrap())).await.unwrap();
        }
        let complete = fixture
            .execute(Request::ListIndexes(serde_json::from_value(json!({"vectorBucketName":bucket})).unwrap()))
            .await
            .unwrap();
        let output: Value = serde_json::from_slice(&complete.body_bytes().unwrap()).unwrap();
        assert_eq!(output["indexes"].as_array().unwrap().len(), 2);
        assert!(output.get("nextToken").is_none());
        let filtered = fixture
            .execute(Request::ListIndexes(
                serde_json::from_value(json!({"vectorBucketName":bucket,"prefix":"index.","maxResults":1})).unwrap(),
            ))
            .await
            .unwrap();
        let output: Value = serde_json::from_slice(&filtered.body_bytes().unwrap()).unwrap();
        assert_eq!(output["indexes"].as_array().unwrap().len(), 1);
        assert_eq!(output["indexes"][0]["indexName"], "index.one");
        let first = fixture
            .execute(Request::ListIndexes(
                serde_json::from_value(json!({"vectorBucketName":bucket,"maxResults":1})).unwrap(),
            ))
            .await
            .unwrap();
        let Response::ListIndexes(first) = first else { panic!("expected Index page") };
        assert_eq!(first.indexes.len(), 1);
        let token = first.next_token.unwrap();
        assert!(token.len() <= 512);
        let second = fixture
            .execute(Request::ListIndexes(
                serde_json::from_value(json!({"vectorBucketArn":format!("arn:aws:s3vectors:{}::bucket/{bucket}", fixture.service.region),"maxResults":500,"nextToken":token})).unwrap(),
            ))
            .await
            .unwrap();
        let Response::ListIndexes(second) = second else { panic!("expected Index page") };
        assert_eq!(second.indexes.len(), 1);
        assert_ne!(first.indexes[0].index_name, second.indexes[0].index_name);
        assert!(second.next_token.is_none());
        for input in [
            json!({"vectorBucketName":bucket,"nextToken":"unsupported"}),
            json!({"vectorBucketName":bucket,"prefix":"index.","nextToken":token}),
        ] {
            assert_eq!(
                fixture
                    .execute(Request::ListIndexes(serde_json::from_value(input).unwrap()))
                    .await
                    .unwrap_err()
                    .code(),
                ErrorCode::Validation,
            );
        }
    });
}

#[test]
fn vector_maintenance_identity_reloads_rotation_disable_and_deletion() {
    run_large_stack_test("vector-maintenance-current-identity", || async {
        let fixture = Fixture::new().await;
        let account = &fixture.credentials.access_key;
        let iam = fixture.app.iam().handle();
        let (first, _) = super::maintenance::resolve_identity(&fixture.app, account)
            .await
            .expect("ordinary maintenance identity");
        assert_eq!(first.account(), account);
        assert_ne!(first.user_generation(), &[0; 16]);
        let root = fixture.app.action_credentials().get().expect("installed root");
        assert!(
            super::maintenance::resolve_identity(&fixture.app, &root.access_key)
                .await
                .is_err()
        );
        iam.create_user(
            account,
            &AddOrUpdateUserReq {
                secret_key: Uuid::new_v4().simple().to_string(),
                policy: None,
                status: AccountStatus::Enabled,
            },
        )
        .await
        .expect("rotate selected ordinary account");
        let (rotated, _) = super::maintenance::resolve_identity(&fixture.app, account)
            .await
            .expect("new generation resolves");
        assert_ne!(rotated, first, "the running worker must reject a rotated account generation");
        iam.set_user_status(account, AccountStatus::Disabled)
            .await
            .expect("disable maintenance account");
        assert!(super::maintenance::resolve_identity(&fixture.app, account).await.is_err());
        iam.delete_user(account, true).await.expect("delete maintenance account");
        assert!(super::maintenance::resolve_identity(&fixture.app, account).await.is_err());
    });
}

#[test]
#[serial_test::serial]
fn vector_resource_bucket_pages_preserve_scope_identity_and_current_authorization() {
    run_large_stack_test("vector-resource-bucket-pages", || async {
        let fixture = Fixture::new().await;
        let prefix = format!("bucket-pages-{}", Uuid::new_v4().simple());
        let names: Vec<_> = ["aaa", "bbb", "ccc"].map(|suffix| format!("{prefix}-{suffix}")).into();
        for name in &names {
            fixture
                .execute(Request::CreateVectorBucket(
                    serde_json::from_value(json!({"vectorBucketName":name})).unwrap(),
                ))
                .await
                .unwrap();
        }
        let list = |token: Option<&str>, max| {
            Request::ListVectorBuckets(
                serde_json::from_value(json!({"prefix":prefix,"maxResults":max,"nextToken":token})).unwrap(),
            )
        };
        let Response::ListVectorBuckets(first) = fixture.execute(list(None, 1)).await.unwrap() else {
            panic!("expected bucket page")
        };
        assert_eq!(first.vector_buckets.len(), 1);
        assert_eq!(first.vector_buckets[0].vector_bucket_name, names[0]);
        let token = first.next_token.unwrap();
        assert!(token.len() <= 512);
        let Response::ListVectorBuckets(rest) = fixture.execute(list(Some(&token), 2)).await.unwrap() else {
            panic!("expected bucket page")
        };
        assert_eq!(
            rest.vector_buckets
                .iter()
                .map(|item| item.vector_bucket_name.as_str())
                .collect::<Vec<_>>(),
            vec![names[1].as_str(), names[2].as_str()]
        );
        assert!(rest.next_token.is_none());
        let replay = fixture.execute(list(Some(&token), 2)).await.unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&replay.body_bytes().unwrap()).unwrap(),
            serde_json::to_value(&rest).unwrap()
        );
        let other = Fixture::new().await;
        assert_eq!(other.execute(list(Some(&token), 1)).await.unwrap_err().code(), ErrorCode::Validation);
        assert_eq!(
            fixture
                .execute(Request::ListVectorBuckets(
                    serde_json::from_value(json!({"prefix":"different-prefix","nextToken":token})).unwrap()
                ))
                .await
                .unwrap_err()
                .code(),
            ErrorCode::Validation
        );
        assert_eq!(
            fixture
                .execute(Request::ListIndexes(
                    serde_json::from_value(json!({"vectorBucketName":names[1],"nextToken":token})).unwrap()
                ))
                .await
                .unwrap_err()
                .code(),
            ErrorCode::Validation
        );
        fixture
            .execute(Request::DeleteVectorBucket(
                serde_json::from_value(json!({"vectorBucketName":names[0]})).unwrap(),
            ))
            .await
            .unwrap();
        let Response::ListVectorBuckets(after_delete) = fixture.execute(list(Some(&token), 2)).await.unwrap() else {
            panic!("expected bucket page")
        };
        assert_eq!(
            after_delete
                .vector_buckets
                .iter()
                .map(|item| item.vector_bucket_name.as_str())
                .collect::<Vec<_>>(),
            vec![names[1].as_str(), names[2].as_str()]
        );
        fixture
            .execute(Request::CreateVectorBucket(
                serde_json::from_value(json!({"vectorBucketName":names[0]})).unwrap(),
            ))
            .await
            .unwrap();
        assert_eq!(fixture.execute(list(Some(&token), 2)).await.unwrap_err().code(), ErrorCode::Validation);
        let Response::ListVectorBuckets(current) = fixture.execute(list(None, 1)).await.unwrap() else {
            panic!("expected bucket page")
        };
        let current = current.next_token.unwrap();
        fixture
            .set_statements(json!([{"Effect":"Allow","Action":"s3vectors:ListVectorBuckets","Resource":"*"}]))
            .await;
        assert!(fixture.execute(list(Some(&current), 2)).await.is_ok());
        fixture
            .set_statements(json!([{"Effect":"Deny","Action":"s3vectors:ListVectorBuckets","Resource":"*"}]))
            .await;
        assert_eq!(
            fixture.execute(list(Some(&current), 2)).await.unwrap_err().code(),
            ErrorCode::AccessDenied
        );
    });
}

#[test]
#[serial_test::serial]
fn vector_resource_index_pages_follow_catalog_order_and_reject_recreated_anchor() {
    run_large_stack_test("vector-resource-index-pages", || async {
        let fixture = Fixture::new().await;
        let bucket = format!("index-pages-{}", Uuid::new_v4().simple());
        fixture
            .execute(Request::CreateVectorBucket(
                serde_json::from_value(json!({"vectorBucketName":bucket})).unwrap(),
            ))
            .await
            .unwrap();
        let create = |name: &str| {
            Request::CreateIndex(serde_json::from_value(json!({"vectorBucketName":bucket,"indexName":name,"dataType":"float32","dimension":2,"distanceMetric":"euclidean"})).unwrap())
        };
        for name in ["aaa", "aaa-bbb", "aaa.bbb", "zzz"] {
            fixture.execute(create(name)).await.unwrap();
        }
        let list = |token: Option<&str>, max| {
            Request::ListIndexes(
                serde_json::from_value(json!({"vectorBucketName":bucket,"maxResults":max,"nextToken":token})).unwrap(),
            )
        };
        let Response::ListIndexes(first) = fixture.execute(list(None, 1)).await.unwrap() else {
            panic!("expected Index page")
        };
        assert_eq!(first.indexes[0].index_name, "aaa-bbb");
        let token = first.next_token.unwrap();
        let Response::ListIndexes(rest) = fixture.execute(list(Some(&token), 500)).await.unwrap() else {
            panic!("expected Index page")
        };
        assert_eq!(
            rest.indexes.iter().map(|item| item.index_name.as_str()).collect::<Vec<_>>(),
            vec!["aaa.bbb", "aaa", "zzz"]
        );
        assert!(rest.next_token.is_none());
        fixture
            .execute(Request::DeleteIndex(
                serde_json::from_value(json!({"vectorBucketName":bucket,"indexName":"aaa-bbb"})).unwrap(),
            ))
            .await
            .unwrap();
        let Response::ListIndexes(after_delete) = fixture.execute(list(Some(&token), 500)).await.unwrap() else {
            panic!("expected Index page")
        };
        assert_eq!(
            after_delete
                .indexes
                .iter()
                .map(|item| item.index_name.as_str())
                .collect::<Vec<_>>(),
            vec!["aaa.bbb", "aaa", "zzz"]
        );
        fixture.execute(create("aaa-bbb")).await.unwrap();
        assert_eq!(fixture.execute(list(Some(&token), 500)).await.unwrap_err().code(), ErrorCode::Validation);
        let Response::ListIndexes(current) = fixture.execute(list(None, 1)).await.unwrap() else {
            panic!("expected Index page")
        };
        let current = current.next_token.unwrap();
        fixture.set_statements(json!([{"Effect":"Allow","Action":"s3vectors:ListIndexes","Resource":format!("arn:aws:s3vectors:{}::bucket/{bucket}",fixture.service.region)}])).await;
        assert!(fixture.execute(list(Some(&current), 500)).await.is_ok());
        fixture
            .set_statements(json!([{"Effect":"Deny","Action":"s3vectors:ListIndexes","Resource":"*"}]))
            .await;
        assert_eq!(
            fixture.execute(list(Some(&current), 500)).await.unwrap_err().code(),
            ErrorCode::AccessDenied
        );
    });
}
