// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

//! Component coverage for independent Vector permissions and bounded Managed access.
//! Request envelopes retain identities verified by check_key_valid_with_context.

use super::*;
use crate::{
    app::object::{DefaultObjectUsecase, ManagedEncryption, ManagedObjectContext, ManagedObjectError},
    app::{RequestCredentials, RequestEnvelope, RequestHead},
    storage_api::{
        protocols::client::{ReqInfo, authorize_request},
        vector::{ManagedObjectAccess, ManagedObjectOperation},
    },
};
use bytes::Bytes;
use rustfs_policy::policy::action::S3Action;

async fn ordinary_s3_iam_allowed(iam: &IamSys<ObjectStore>, credentials: &Credentials, action: S3Action) -> bool {
    let conditions = HashMap::new();
    let claims = HashMap::new();
    iam.is_allowed(&Args {
        account: &credentials.access_key,
        groups: &credentials.groups,
        action: Action::S3Action(action),
        bucket: "ordinary-general-bucket",
        object: "object.bin",
        conditions: &conditions,
        is_owner: false,
        claims: credentials.claims.as_ref().unwrap_or(&claims),
        deny_only: false,
    })
    .await
}

#[test]
#[serial_test::serial]
fn vector_single_operation_permissions_succeed_with_all_ordinary_s3_actions_denied() {
    run_large_stack_test("vector-single-action-permissions", || async {
        let fixture = ResourceFixture::new();
        for operation in [Operation::PutVectors, Operation::DeleteVectors, Operation::GetVectors] {
            let (app, credentials, _) = install_real_iam_user(
                json!([
                    {"Effect":"Allow","Action":format!("s3vectors:{}",operation.name()),"Resource":"*"},
                    {"Effect":"Deny","Action":"s3:*","Resource":"*"}
                ]),
                false,
            )
            .await;
            let iam = app.iam().handle();
            let request = request_for(operation);
            authorize_with_iam(
                iam.as_ref(),
                &credentials,
                false,
                &request,
                &fixture.target(operation),
                REGION,
                HashMap::new(),
            )
            .await
            .unwrap();
            for s3_action in [
                S3Action::GetObjectAction,
                S3Action::PutObjectAction,
                S3Action::DeleteObjectAction,
            ] {
                assert!(!ordinary_s3_iam_allowed(iam.as_ref(), &credentials, s3_action).await);
            }
            for other in [Operation::PutVectors, Operation::DeleteVectors, Operation::GetVectors] {
                if other == operation {
                    continue;
                }
                let request = request_for(other);
                assert!(matches!(
                    authorize_with_iam(
                        iam.as_ref(),
                        &credentials,
                        false,
                        &request,
                        &fixture.target(other),
                        REGION,
                        HashMap::new()
                    )
                    .await,
                    Err(VectorAccessError::Denied)
                ));
            }
        }
    });
}

#[test]
#[serial_test::serial]
fn vector_and_s3_wildcards_do_not_grant_the_other_action_family() {
    run_large_stack_test("vector-and-s3-action-families", || async {
        let fixture = ResourceFixture::new();
        let (s3_app, s3_user, _) = install_real_iam_user(
            json!([
                {"Effect":"Allow","Action":"s3:*","Resource":"*"}
            ]),
            false,
        )
        .await;
        let s3_iam = s3_app.iam().handle();
        assert!(ordinary_s3_iam_allowed(s3_iam.as_ref(), &s3_user, S3Action::GetObjectAction).await);
        for operation in Operation::ALL {
            let request = request_for(operation);
            assert!(
                matches!(
                    authorize_with_iam(
                        s3_iam.as_ref(),
                        &s3_user,
                        false,
                        &request,
                        &fixture.target(operation),
                        REGION,
                        HashMap::new()
                    )
                    .await,
                    Err(VectorAccessError::Denied)
                ),
                "{}",
                operation.name()
            );
        }
        let (vector_app, vector_user, _) = install_real_iam_user(allow_all(), false).await;
        let vector_iam = vector_app.iam().handle();
        for operation in Operation::ALL {
            let request = request_for(operation);
            authorize_with_iam(
                vector_iam.as_ref(),
                &vector_user,
                false,
                &request,
                &fixture.target(operation),
                REGION,
                HashMap::new(),
            )
            .await
            .unwrap();
        }
        for s3_action in [
            S3Action::GetObjectAction,
            S3Action::PutObjectAction,
            S3Action::DeleteObjectAction,
        ] {
            assert!(!ordinary_s3_iam_allowed(vector_iam.as_ref(), &vector_user, s3_action).await);
        }
    });
}

#[test]
#[serial_test::serial]
fn vector_query_and_list_additional_read_permissions_use_real_iam_for_every_combination() {
    run_large_stack_test("vector-query-list-permissions", || async {
        let (app, credentials, policy_name) = install_real_iam_user(
            json!([
                {"Effect":"Allow","Action":["s3vectors:QueryVectors","s3vectors:ListVectors"],"Resource":"*"},
                {"Effect":"Deny","Action":"s3:*","Resource":"*"}
            ]),
            false,
        )
        .await;
        let iam = app.iam().handle();
        let fixture = ResourceFixture::new();
        for get_vectors_allowed in [false, true] {
            if get_vectors_allowed {
                iam.set_policy(&policy_name, Policy::parse_config(&serde_json::to_vec(&json!({
                    "Version":"2012-10-17","Statement":[
                        {"Effect":"Allow","Action":["s3vectors:QueryVectors","s3vectors:ListVectors","s3vectors:GetVectors"],"Resource":"*"},
                        {"Effect":"Deny","Action":"s3:*","Resource":"*"}
                    ]
                })).unwrap()).unwrap()).await.unwrap();
            }
            for filter in [None, Some(Value::Null), Some(json!({"project":"search"}))] {
                for metadata in [None, Some(false), Some(true)] {
                    for distance in [None, Some(false), Some(true)] {
                        let Request::QueryVectors(mut input) = request_for(Operation::QueryVectors) else { unreachable!() };
                        input.filter = filter.clone();
                        input.return_metadata = metadata;
                        input.return_distance = distance;
                        let needs_get = filter.is_some() || metadata == Some(true);
                        let result = authorize_with_iam(
                            iam.as_ref(),
                            &credentials,
                            false,
                            &Request::QueryVectors(input),
                            &fixture.target(Operation::QueryVectors),
                            REGION,
                            HashMap::new(),
                        )
                        .await;
                        if needs_get && !get_vectors_allowed {
                            assert!(matches!(result, Err(VectorAccessError::Denied)));
                        } else {
                            result.unwrap();
                        }
                    }
                }
            }
            for data in [None, Some(false), Some(true)] {
                for metadata in [None, Some(false), Some(true)] {
                    let Request::ListVectors(mut input) = request_for(Operation::ListVectors) else { unreachable!() };
                    input.return_data = data;
                    input.return_metadata = metadata;
                    let result = authorize_with_iam(
                        iam.as_ref(),
                        &credentials,
                        false,
                        &Request::ListVectors(input),
                        &fixture.target(Operation::ListVectors),
                        REGION,
                        HashMap::new(),
                    )
                    .await;
                    if (data == Some(true) || metadata == Some(true)) && !get_vectors_allowed {
                        assert!(matches!(result, Err(VectorAccessError::Denied)));
                    } else {
                        result.unwrap();
                    }
                }
            }
        }
        assert!(!ordinary_s3_iam_allowed(iam.as_ref(), &credentials, S3Action::GetObjectAction).await);
        assert!(!ordinary_s3_iam_allowed(iam.as_ref(), &credentials, S3Action::PutObjectAction).await);
    });
}

fn envelope_for_verified_identity(
    source: &RequestEnvelope,
    credentials: &Credentials,
    owner: bool,
    bucket: &str,
    object: &str,
    path: &str,
    service: &str,
) -> RequestEnvelope {
    let mut extensions = http::Extensions::new();
    let request_context = source.request_context().unwrap().clone();
    extensions.insert(request_context.clone());
    extensions.insert(source.server_context().unwrap().clone());
    extensions.insert(ReqInfo {
        cred: Some(credentials.clone()),
        is_owner: owner,
        bucket: Some(bucket.to_owned()),
        object: Some(object.to_owned()),
        request_context: Some(request_context),
        ..Default::default()
    });
    RequestEnvelope::from_parts(
        RequestHead {
            method: Method::POST,
            uri: path.parse().unwrap(),
            headers: http::HeaderMap::new(),
            credentials: Some(RequestCredentials::without_secret_key(
                credentials.access_key.clone(),
                (!credentials.session_token.is_empty()).then(|| credentials.session_token.clone()),
            )),
            region: Some(REGION.into()),
            service: Some(service.into()),
        },
        extensions,
    )
}

#[test]
#[serial_test::serial]
fn ordinary_s3_authorization_refuses_vector_storage_for_the_real_owner() {
    run_large_stack_test("ordinary-s3-vector-owner-refusal", || async {
        let (app, _, _) = install_real_iam_user(allow_all(), false).await;
        let (source, access, _, _reset) =
            crate::app::object::managed_request_fixture_with_encryption(ManagedEncryption::Unencrypted).await;
        let root = app.action_credentials().get().unwrap();
        let (verified, owner) = crate::auth::check_key_valid_with_context("", &root.access_key, Some(&app))
            .await
            .unwrap();
        assert!(owner);
        let bucket = access.bucket().storage_bucket_id().as_str();
        let object = format!("{}raw.bin", access.allowed_prefix());
        for action in [
            S3Action::GetObjectAction,
            S3Action::GetObjectVersionAction,
            S3Action::PutObjectAction,
            S3Action::DeleteObjectAction,
            S3Action::DeleteObjectVersionAction,
            S3Action::ListBucketAction,
            S3Action::ListBucketVersionsAction,
            S3Action::ListBucketMultipartUploadsAction,
            S3Action::ListMultipartUploadPartsAction,
            S3Action::AbortMultipartUploadAction,
        ] {
            let mut request =
                envelope_for_verified_identity(&source, &verified, owner, bucket, &object, "/raw-vector-object", "s3");
            let error = authorize_request(&mut request, Action::S3Action(action)).await.unwrap_err();
            assert_eq!(error.code(), &s3s::S3ErrorCode::AccessDenied);
        }
    });
}

#[test]
#[serial_test::serial]
fn query_only_identity_writes_bounded_query_results_with_original_audit_identity() {
    run_large_stack_test("query-result-service-capability", || async {
        let (app, credentials, _) = install_real_iam_user(
            json!([
                {"Effect":"Allow","Action":"s3vectors:QueryVectors","Resource":"*"},
                {"Effect":"Deny","Action":"s3:*","Resource":"*"}
            ]),
            false,
        )
        .await;
        let (source, original_access, _, _reset) =
            crate::app::object::managed_request_fixture_with_encryption(ManagedEncryption::Unencrypted).await;
        let managed_app = source.server_context().unwrap().app_context().unwrap();
        assert!(Arc::ptr_eq(&app.iam().handle(), &managed_app.iam().handle()));
        let (verified, owner) = crate::auth::check_key_valid_with_context("", &credentials.access_key, Some(&managed_app))
            .await
            .unwrap();
        assert!(!owner);
        let iam = managed_app.iam().handle();
        assert!(!ordinary_s3_iam_allowed(iam.as_ref(), &verified, S3Action::PutObjectAction).await);
        let bucket = BucketAuthorizationFacts {
            handle: original_access.bucket().clone(),
            tags: BTreeMap::new(),
            policy: None,
        };
        let resource =
            CatalogResourceIdentity::try_new(bucket.handle.clone(), original_access.table_id().clone(), CatalogFormat::Lance)
                .unwrap();
        let index = CatalogName::parse(INDEX).unwrap();
        let definition = VectorIndexDefinitionV1 {
            version: 1,
            effective_encryption: VectorEffectiveEncryptionV1::Unencrypted,
            tags: BTreeMap::new(),
        };
        let target = VectorTarget::Index {
            bucket: &bucket,
            index_name: &index,
            resource: &resource,
            definition: &definition,
        };
        let request = parse(
            Operation::QueryVectors,
            json!({
                "vectorBucketName":bucket.handle.public_name().as_str(),"indexName":INDEX,
                "queryVector":{"float32":[1.0,0.0,0.0]},"topK":1,"returnDistance":true
            }),
        );
        authorize_with_iam(iam.as_ref(), &verified, owner, &request, &target, REGION, HashMap::new())
            .await
            .unwrap();
        let prefix = format!(".rustfs-table/{}/query-results/", original_access.table_id().as_str());
        let access = ManagedObjectAccess::try_from_parts(
            bucket.handle.clone(),
            original_access.table_id().clone(),
            prefix.clone(),
            [ManagedObjectOperation::CreateOnly, ManagedObjectOperation::Get]
                .into_iter()
                .collect(),
        )
        .unwrap();
        let key = format!("{prefix}test-result.bin");
        let envelope = Arc::new(envelope_for_verified_identity(
            &source,
            &verified,
            owner,
            bucket.handle.storage_bucket_id().as_str(),
            &key,
            "/QueryVectors",
            "s3vectors",
        ));
        let request_id = envelope.request_context().unwrap().request_id.clone();
        let context = ManagedObjectContext::from_authorized_request(access, envelope, ManagedEncryption::Unencrypted).unwrap();
        let usecase = DefaultObjectUsecase::with_context(Some(managed_app));
        let probe = crate::app::storage_api::object_usecase::helper::MutationCompletionProbe::install(
            bucket.handle.storage_bucket_id().as_str(),
        );
        let created = usecase
            .managed_create_object(&context, &key, 6, futures::stream::iter([Ok(Bytes::from_static(b"result"))]))
            .await
            .unwrap();
        probe.wait_for(1, 0).await;
        {
            let audits = probe.audits.lock().unwrap();
            assert_eq!(audits[0].access_key.as_deref(), Some(verified.access_key.as_str()));
            assert_eq!(audits[0].request_id.as_deref(), Some(request_id.as_str()));
            assert_eq!(audits[0].api.status.as_deref(), Some("success"));
        }
        let (_, bytes) = usecase
            .managed_get_object(&context, &key, created.data_dir, None, 16)
            .await
            .unwrap();
        assert_eq!(bytes, Bytes::from_static(b"result"));
        for outside in [
            format!("{}forbidden.bin", original_access.allowed_prefix()),
            format!(".rustfs-table/{}/query-results/forbidden.bin", Uuid::new_v4()),
        ] {
            let error = usecase
                .managed_create_object(&context, &outside, 1, futures::stream::iter([Ok(Bytes::from_static(b"x"))]))
                .await
                .unwrap_err();
            assert!(matches!(error, ManagedObjectError::Scope(_)));
        }
        assert!(matches!(
            usecase.managed_create_multipart_upload(&context, &key).await,
            Err(ManagedObjectError::Scope(_))
        ));
    });
}
