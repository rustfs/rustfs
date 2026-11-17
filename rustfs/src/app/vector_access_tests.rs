// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

//! Real IAM and Managed integration coverage for Vector authorization.

use super::gating_test_env::{run_large_stack_test, shared_gating_ambient};
use crate::{
    runtime_sources::AppContext,
    table_catalog::{CatalogFormat, CatalogName, CatalogResourceIdentity, VectorEffectiveEncryptionV1, VectorIndexDefinitionV1},
    vector::{
        access_core::{
            BucketAuthorizationFacts, VectorAccessError, VectorTarget, authorize_with_iam, primary_action,
            test_support::{BUCKET, INDEX, REGION, ResourceFixture, parse, request_for, resource_arn, target_names},
            vector_conditions,
        },
        protocol::{Operation, Request},
    },
};
use http::Method;
use rustfs_credentials::Credentials;
use rustfs_iam::{
    store::{Store, UserType, object::ObjectStore},
    sys::{IamSys, SESSION_POLICY_NAME},
};
use rustfs_madmin::{AccountStatus, AddOrUpdateUserReq};
use rustfs_policy::{
    auth::get_new_credentials_with_metadata,
    policy::{Args, BucketPolicy, Policy, action::Action},
};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};
use time::OffsetDateTime;
use uuid::Uuid;

async fn install_real_iam_user(statements: Value, group_only: bool) -> (Arc<AppContext>, Credentials, String) {
    let app = shared_gating_ambient().await;
    let suffix = Uuid::new_v4().simple().to_string();
    if app.action_credentials().get().is_none() {
        assert!(app.publish_action_credentials(Credentials {
            access_key: format!("s6-root-{suffix}"),
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
    let user = format!("s6-user-{suffix}");
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
    let policy_name = format!("s6-policy-{suffix}");
    if !statements.as_array().unwrap().is_empty() {
        iam.set_policy(
            &policy_name,
            Policy::parse_config(
                &serde_json::to_vec(&json!({
                    "Version":"2012-10-17","Statement":statements,
                }))
                .unwrap(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
        if group_only {
            let group = format!("s6-group-{suffix}");
            iam.add_users_to_group(&group, vec![user.clone()]).await.unwrap();
            iam.policy_db_set(&group, UserType::Reg, true, &policy_name).await.unwrap();
        } else {
            iam.policy_db_set(&user, UserType::Reg, false, &policy_name).await.unwrap();
        }
    }
    let (credentials, owner) = crate::auth::check_key_valid_with_context("", &user, Some(&app))
        .await
        .unwrap();
    assert!(!owner);
    (app, credentials, policy_name)
}

fn allow_all() -> Value {
    json!([{"Effect":"Allow","Action":"s3vectors:*","Resource":"*"}])
}

#[test]
#[serial_test::serial]
fn vector_real_iam_user_group_and_policy_refresh_for_all_21_actions() {
    run_large_stack_test("vector-real-iam-all-actions", || async {
        for group_only in [false, true] {
            let (app, credentials, policy_name) = install_real_iam_user(allow_all(), group_only).await;
            let iam = app.iam().handle();
            let fixture = ResourceFixture::new();
            for operation in Operation::ALL {
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
            }
            iam.set_policy(
                &policy_name,
                Policy::parse_config(
                    &serde_json::to_vec(&json!({
                        "Version":"2012-10-17", "Statement":[
                            {"Effect":"Allow","Action":"s3vectors:*","Resource":"*"},
                            {"Effect":"Deny","Action":"s3vectors:*","Resource":"*"}
                        ]
                    }))
                    .unwrap(),
                )
                .unwrap(),
            )
            .await
            .unwrap();
            for operation in Operation::ALL {
                let request = request_for(operation);
                assert!(
                    matches!(
                        authorize_with_iam(
                            iam.as_ref(),
                            &credentials,
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
        }
    });
}

#[test]
#[serial_test::serial]
fn vector_real_iam_additional_getvectors_and_tagresource_permissions() {
    run_large_stack_test("vector-real-iam-additional", || async {
        let (app, credentials, _) = install_real_iam_user(json!([{
            "Effect":"Allow","Action":["s3vectors:QueryVectors","s3vectors:ListVectors","s3vectors:CreateIndex","s3vectors:CreateVectorBucket"],"Resource":"*"
        }]), false).await;
        let iam = app.iam().handle();
        let fixture = ResourceFixture::new();
        for operation in [
            Operation::QueryVectors,
            Operation::ListVectors,
            Operation::CreateIndex,
            Operation::CreateVectorBucket,
        ] {
            let mut request = request_for(operation);
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
            match &mut request {
                Request::QueryVectors(input) => input.filter = Some(Value::Null),
                Request::ListVectors(input) => input.return_data = Some(true),
                Request::CreateIndex(input) => input.tags = Some(BTreeMap::new()),
                Request::CreateVectorBucket(input) => input.tags = Some(BTreeMap::new()),
                _ => unreachable!(),
            }
            assert!(matches!(
                authorize_with_iam(
                    iam.as_ref(),
                    &credentials,
                    false,
                    &request,
                    &fixture.target(operation),
                    REGION,
                    HashMap::new()
                )
                .await,
                Err(VectorAccessError::Denied)
            ));
        }
    });
}

#[test]
#[serial_test::serial]
fn vector_real_sts_session_policy_and_bucket_explicit_deny() {
    run_large_stack_test("vector-real-sts-boundary", || async {
        let (app, parent, _) = install_real_iam_user(allow_all(), false).await;
        let iam = app.iam().handle();
        let root = app.action_credentials().get().unwrap();
        let session_policy = json!({"Version":"2012-10-17","Statement":[
            {"Effect":"Allow","Action":"s3vectors:*","Resource":"*"},
            {"Effect":"Deny","Action":"s3vectors:GetVectors","Resource":"*"}
        ]});
        let claims = HashMap::from([
            ("parent".into(), json!(parent.access_key)),
            (
                "exp".into(),
                json!((OffsetDateTime::now_utc() + time::Duration::hours(1)).unix_timestamp()),
            ),
            (
                SESSION_POLICY_NAME.into(),
                json!(base64_simd::URL_SAFE_NO_PAD.encode_to_string(serde_json::to_vec(&session_policy).unwrap())),
            ),
        ]);
        let signing_key = rustfs_iam::manager::get_token_signing_key().unwrap_or_else(|| root.secret_key.clone());
        let mut session = get_new_credentials_with_metadata(&claims, &signing_key).unwrap();
        session.parent_user = parent.access_key.clone();
        iam.set_temp_user_from_parent(&session, &parent).await.unwrap();
        let (verified, owner) =
            crate::auth::check_key_valid_with_context(&session.session_token, &session.access_key, Some(&app))
                .await
                .unwrap();
        assert!(!owner);
        let mut fixture = ResourceFixture::new();
        let mut request = request_for(Operation::QueryVectors);
        authorize_with_iam(
            iam.as_ref(),
            &verified,
            owner,
            &request,
            &fixture.target(Operation::QueryVectors),
            REGION,
            HashMap::new(),
        )
        .await
        .unwrap();
        if let Request::QueryVectors(input) = &mut request {
            input.return_metadata = Some(true);
        }
        assert!(matches!(
            authorize_with_iam(
                iam.as_ref(),
                &verified,
                owner,
                &request,
                &fixture.target(Operation::QueryVectors),
                REGION,
                HashMap::new()
            )
            .await,
            Err(VectorAccessError::Denied)
        ));
        fixture.bucket.policy = Some(
            serde_json::from_slice(
                &serde_json::to_vec(&json!({
                    "Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":"s3vectors:*","Resource":"*"}]
                }))
                .unwrap(),
            )
            .unwrap(),
        );
        for operation in [
            Operation::GetVectors,
            Operation::PutVectorBucketPolicy,
            Operation::GetVectorBucketPolicy,
            Operation::DeleteVectorBucketPolicy,
        ] {
            let request = request_for(operation);
            assert!(matches!(
                authorize_with_iam(iam.as_ref(), &root, true, &request, &fixture.target(operation), REGION, HashMap::new()).await,
                Err(VectorAccessError::Denied)
            ));
        }
    });
}

/// A matching bucket policy grants access when the identity has no attached policy.
#[test]
#[serial_test::serial]
fn vector_real_iam_bucket_only_allow_requires_complete_resource_policy_composition() {
    run_large_stack_test("vector-real-resource-policy", || async {
        let (app, credentials, _) = install_real_iam_user(json!([]), false).await;
        let mut fixture = ResourceFixture::new();
        fixture.bucket.policy = Some(
            serde_json::from_slice(
                &serde_json::to_vec(&json!({
                    "Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":credentials.access_key},
                    "Action":"s3vectors:GetVectors","Resource":resource_arn(REGION,BUCKET,INDEX)}]
                }))
                .unwrap(),
            )
            .unwrap(),
        );
        let request = request_for(Operation::GetVectors);
        assert!(
            authorize_with_iam(
                app.iam().handle().as_ref(),
                &credentials,
                false,
                &request,
                &fixture.target(Operation::GetVectors),
                REGION,
                HashMap::new()
            )
            .await
            .is_ok()
        );
    });
}

#[test]
#[serial_test::serial]
fn vector_real_iam_tags_and_request_sse_conditions() {
    run_large_stack_test("vector-real-iam-condition-sources", || async {
        let (app, credentials, policy_name) = install_real_iam_user(
            json!([{
                "Effect":"Allow","Action":"s3vectors:GetVectors","Resource":"*",
                "Condition":{"StringEquals":{"aws:ResourceTag/project":"search",
                    "s3vectors:VectorBucketTag/department":"research","aws:RequestedRegion":REGION}}
            }]),
            false,
        )
        .await;
        let iam = app.iam().handle();
        let mut fixture = ResourceFixture::new();
        let request = request_for(Operation::GetVectors);
        authorize_with_iam(
            iam.as_ref(),
            &credentials,
            false,
            &request,
            &fixture.target(Operation::GetVectors),
            REGION,
            HashMap::new(),
        )
        .await
        .unwrap();
        fixture.definition.tags.insert("project".into(), "other".into());
        let injected = HashMap::from([("ResourceTag/project".into(), vec!["search".into()])]);
        assert!(matches!(
            authorize_with_iam(
                iam.as_ref(),
                &credentials,
                false,
                &request,
                &fixture.target(Operation::GetVectors),
                REGION,
                injected
            )
            .await,
            Err(VectorAccessError::Denied)
        ));
        let kms_arn = "arn:aws:kms:us-east-1:123456789012:key/s6-test-key";
        for (configuration, condition) in [
            (None, json!({"Null":{"s3vectors:sseType":"true","s3vectors:kmsKeyArn":"true"}})),
            (
                Some(json!({})),
                json!({"StringEquals":{"s3vectors:sseType":"AES256"},"Null":{"s3vectors:kmsKeyArn":"true"}}),
            ),
            (
                Some(json!({"sseType":"aws:kms","kmsKeyArn":kms_arn})),
                json!({"StringEquals":{"s3vectors:sseType":"aws:kms","s3vectors:kmsKeyArn":kms_arn}}),
            ),
        ] {
            iam.set_policy(&policy_name, Policy::parse_config(&serde_json::to_vec(&json!({
                "Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3vectors:CreateIndex","Resource":"*","Condition":condition}]
            })).unwrap()).unwrap()).await.unwrap();
            let mut input =
                json!({"vectorBucketName":BUCKET,"indexName":INDEX,"dataType":"float32","dimension":3,"distanceMetric":"cosine"});
            if let Some(configuration) = configuration {
                input["encryptionConfiguration"] = configuration;
            }
            let request = parse(Operation::CreateIndex, input);
            authorize_with_iam(
                iam.as_ref(),
                &credentials,
                false,
                &request,
                &fixture.target(Operation::CreateIndex),
                REGION,
                HashMap::new(),
            )
            .await
            .unwrap();
        }
    });
}

mod iam_api;
mod permission_model;
#[cfg(feature = "gateway-stack")]
mod s3_isolation_test;
