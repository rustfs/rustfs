// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

//! SDK authorization over real IAM, session credentials, and Managed ECStore data.

use super::encryption::{EncryptionFixture, snapshot};
use super::{Fixture, SdkServer, cleanup_iam, metadata, run_large_stack_test, vector};
use crate::app::vector_access_tests::issue_real_session;
use aws_sdk_s3vectors::{
    Client,
    config::Credentials as SdkCredentials,
    error::ProvideErrorMetadata as _,
    types::{DataType, DistanceMetric, EncryptionConfiguration, SseType, VectorData},
};
use futures_util::FutureExt as _;
use rustfs_credentials::Credentials;
use rustfs_iam::store::UserType;
use rustfs_policy::policy::statement::RequestedKmsKey;
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    collections::{BTreeSet, HashMap},
    panic::AssertUnwindSafe,
};
use uuid::Uuid;

macro_rules! service_error {
    ($result:expr, $code:literal, $status:literal) => {{
        let error = $result.expect_err("SDK request must be refused");
        assert_eq!(error.as_service_error().expect("service error").code(), Some($code));
        assert_eq!(error.raw_response().expect("HTTP response").status().as_u16(), $status);
    }};
}

fn client_as(server: &SdkServer, credentials: &Credentials) -> Client {
    Client::from_conf(
        server
            .client
            .config()
            .to_builder()
            .credentials_provider(SdkCredentials::new(
                credentials.access_key.clone(),
                credentials.secret_key.clone(),
                (!credentials.session_token.is_empty()).then(|| credentials.session_token.clone()),
                None,
                "real-iam-session",
            ))
            .build(),
    )
}

fn kms_resource(key: &str) -> String {
    let RequestedKmsKey::Key(key) = RequestedKmsKey::parse(key) else {
        panic!("fixture key must have a concrete identity");
    };
    assert!(!key.contains(['*', '?']));
    format!("arn:aws:kms:::key/{key}")
}

fn parent_permissions(key: &str) -> Value {
    json!([
        {"Effect":"Allow","Action":"s3vectors:*","Resource":"*"},
        {"Effect":"Allow","Action":["kms:GenerateDataKey","kms:Decrypt"],"Resource":key},
        {"Effect":"Deny","Action":"s3:*","Resource":"*"}
    ])
}

fn session_permissions(index: &str, key: &str, actions: &[&str]) -> Value {
    json!({"Version":"2012-10-17","Statement":[
        {"Effect":"Allow","Action":actions,"Resource":index},
        {"Effect":"Allow","Action":["kms:GenerateDataKey","kms:Decrypt"],"Resource":key},
        {"Effect":"Deny","Action":"s3:*","Resource":"*"}
    ]})
}

async fn assert_rows(client: &Client, index: &str) {
    assert_get_rows(client, index).await;
    let listed = client.list_vectors().index_arn(index).send().await.unwrap();
    assert!(listed.next_token().is_none());
    assert_eq!(
        listed.vectors().iter().map(|row| row.key()).collect::<BTreeSet<_>>(),
        BTreeSet::from(["alpha", "beta"])
    );
}

async fn assert_get_rows(client: &Client, index: &str) {
    let found = client
        .get_vectors()
        .index_arn(index)
        .keys("alpha")
        .keys("beta")
        .return_data(true)
        .return_metadata(true)
        .send()
        .await
        .unwrap();
    assert_eq!(found.vectors().len(), 2);
    for (key, data) in [("alpha", vec![1.0, 2.0]), ("beta", vec![3.0, 4.0])] {
        let row = found.vectors().iter().find(|row| row.key() == key).expect("saved vector");
        assert_eq!(row.data(), Some(&VectorData::Float32(data)));
        assert_eq!(row.metadata(), Some(&metadata(key)));
    }
}

#[test]
#[serial_test::serial]
fn vector_aws_sdk_encrypted_sts_sessions_enforce_current_permissions() {
    run_large_stack_test("vector-sdk-sts-current-permissions", || async {
        let fixture = EncryptionFixture::new(false).await;
        let server = SdkServer::new(&fixture.request).await;
        let observer = &server.client;
        let iam = fixture.request.app.iam().handle();
        let mut sessions = Vec::new();
        let result = AssertUnwindSafe(async {
            let bucket = format!("sdk-sts-{}", Uuid::new_v4().simple());
            let encryption = EncryptionConfiguration::builder()
                .sse_type(SseType::AwsKms)
                .kms_key_arn(&fixture.key_arn)
                .build();
            observer
                .create_vector_bucket()
                .vector_bucket_name(&bucket)
                .encryption_configuration(encryption.clone())
                .send()
                .await
                .unwrap();
            let created = observer
                .create_index()
                .vector_bucket_name(&bucket)
                .index_name("session.index")
                .data_type(DataType::Float32)
                .dimension(2)
                .distance_metric(DistanceMetric::Euclidean)
                .send()
                .await
                .unwrap();
            let index = created.index_arn().unwrap();
            assert_eq!(
                observer
                    .get_index()
                    .index_arn(index)
                    .send()
                    .await
                    .unwrap()
                    .index()
                    .unwrap()
                    .encryption_configuration(),
                Some(&encryption)
            );
            let key = kms_resource(&fixture.key_arn);
            let allowed = parent_permissions(&key);
            let session = issue_real_session(
                &fixture.request.app,
                &fixture.request.credentials,
                Some(session_permissions(
                    index,
                    &key,
                    &[
                        "s3vectors:PutVectors",
                        "s3vectors:GetVectors",
                        "s3vectors:ListVectors",
                        "s3vectors:QueryVectors",
                    ],
                )),
            )
            .await;
            sessions.push(session.access_key.clone());
            assert!(!session.session_token.is_empty());
            assert_eq!(session.parent_user, fixture.request.credentials.access_key);
            let (_, owner) = crate::auth::check_key_valid_with_context(
                &session.session_token,
                &session.access_key,
                Some(&fixture.request.app),
            )
            .await
            .unwrap();
            assert!(!owner);
            let client = client_as(&server, &session);
            client
                .put_vectors()
                .index_arn(index)
                .vectors(vector("alpha", [1.0, 2.0], "alpha"))
                .vectors(vector("beta", [3.0, 4.0], "beta"))
                .send()
                .await
                .unwrap();
            assert_rows(&client, index).await;
            let before = snapshot(&fixture, index, true).await;

            service_error!(
                client.delete_vectors().index_arn(index).keys("beta").send().await,
                "AccessDeniedException",
                403
            );
            assert_eq!(snapshot(&fixture, index, true).await, before);
            service_error!(
                client.get_vector_bucket().vector_bucket_name(&bucket).send().await,
                "AccessDeniedException",
                403
            );
            assert_eq!(snapshot(&fixture, index, true).await, before);

            let read_only = issue_real_session(
                &fixture.request.app,
                &fixture.request.credentials,
                Some(session_permissions(index, &key, &["s3vectors:GetVectors"])),
            )
            .await;
            sessions.push(read_only.access_key.clone());
            let reader = client_as(&server, &read_only);
            assert_get_rows(&reader, index).await;
            service_error!(
                reader
                    .put_vectors()
                    .index_arn(index)
                    .vectors(vector("alpha", [9.0, 9.0], "replaced"))
                    .send()
                    .await,
                "AccessDeniedException",
                403
            );
            assert_eq!(snapshot(&fixture, index, true).await, before);

            let mut denied_policy = session_permissions(index, &key, &["s3vectors:GetVectors"]);
            denied_policy["Statement"]
                .as_array_mut()
                .unwrap()
                .push(json!({"Effect":"Deny","Action":"s3vectors:GetVectors","Resource":index}));
            let denied = issue_real_session(&fixture.request.app, &fixture.request.credentials, Some(denied_policy)).await;
            sessions.push(denied.access_key.clone());
            let denied_client = client_as(&server, &denied);
            let bucket_policy = json!({"Version":"2012-10-17","Statement":[{
                "Effect":"Allow","Principal":{"AWS":denied.access_key},"Action":"s3vectors:GetVectors","Resource":index
            }]})
            .to_string();
            observer
                .put_vector_bucket_policy()
                .vector_bucket_name(&bucket)
                .policy(&bucket_policy)
                .send()
                .await
                .unwrap();
            service_error!(
                denied_client
                    .get_vectors()
                    .index_arn(index)
                    .keys("alpha")
                    .return_data(true)
                    .send()
                    .await,
                "AccessDeniedException",
                403
            );
            assert_eq!(snapshot(&fixture, index, true).await, before);
            observer
                .delete_vector_bucket_policy()
                .vector_bucket_name(&bucket)
                .send()
                .await
                .unwrap();

            #[derive(Deserialize)]
            struct AuthenticationError {
                #[serde(rename = "Code")]
                code: String,
            }
            for token in [String::new(), format!("{}x", session.session_token)] {
                let mut invalid = session.clone();
                invalid.session_token = token;
                let error = client_as(&server, &invalid)
                    .put_vectors()
                    .index_arn(index)
                    .vectors(vector("alpha", [9.0, 9.0], "invalid-session"))
                    .send()
                    .await
                    .expect_err("unusable STS token must be refused");
                let response = error.raw_response().expect("Gateway authentication response");
                assert_eq!(response.status().as_u16(), 403);
                let wire: AuthenticationError =
                    quick_xml::de::from_reader(response.body().bytes().expect("buffered XML refusal")).unwrap();
                assert_eq!(wire.code, "InvalidAccessKeyId");
                assert_eq!(snapshot(&fixture, index, true).await, before);
            }

            for action in ["s3vectors:PutVectors", "s3vectors:GetVectors"] {
                let mut revoked = allowed.clone();
                revoked
                    .as_array_mut()
                    .unwrap()
                    .push(json!({"Effect":"Deny","Action":action,"Resource":index}));
                fixture.request.set_statements(revoked).await;
                if action == "s3vectors:PutVectors" {
                    service_error!(
                        client
                            .put_vectors()
                            .index_arn(index)
                            .vectors(vector("alpha", [9.0, 9.0], "revoked"))
                            .send()
                            .await,
                        "AccessDeniedException",
                        403
                    );
                } else {
                    service_error!(
                        client
                            .get_vectors()
                            .index_arn(index)
                            .keys("alpha")
                            .return_data(true)
                            .send()
                            .await,
                        "AccessDeniedException",
                        403
                    );
                }
                fixture.request.set_statements(allowed.clone()).await;
                assert_eq!(snapshot(&fixture, index, true).await, before);
                assert_rows(&client, index).await;
            }
            observer
                .delete_vectors()
                .index_arn(index)
                .keys("alpha")
                .keys("beta")
                .send()
                .await
                .unwrap();
            observer.delete_index().index_arn(index).send().await.unwrap();
        })
        .catch_unwind()
        .await;
        server.finish().await;
        for session in sessions {
            iam.delete_temp_account(&session, false).await.unwrap();
        }
        cleanup_iam(&fixture.request).await;
        fixture.manager.stop().await.unwrap();
        drop(fixture);
        if let Err(panic) = result {
            std::panic::resume_unwind(panic);
        }
    });
}

fn group_control() -> Value {
    json!({"Effect":"Allow","Action":[
        "s3vectors:GetVectorBucket", "s3vectors:ListVectorBuckets", "s3vectors:GetIndex", "s3vectors:ListIndexes",
        "s3vectors:ListTagsForResource", "s3vectors:TagResource", "s3vectors:UntagResource",
        "s3vectors:DeleteIndex", "s3vectors:DeleteVectorBucket"
    ],"Resource":"*"})
}

fn group_creation_permissions(region: &str) -> Value {
    let tags = json!({
        "StringEquals":{"aws:RequestedRegion":region,"aws:RequestTag/project":"search"},
        "ForAllValues:StringEquals":{"aws:TagKeys":["project","department"]},
        "Null":{"aws:TagKeys":"false"}
    });
    let mut index = tags.clone();
    index["Null"]["s3vectors:sseType"] = json!("true");
    index["Null"]["s3vectors:kmsKeyArn"] = json!("true");
    json!([
        group_control(),
        {"Effect":"Allow","Action":"s3vectors:CreateVectorBucket","Resource":"*","Condition":tags},
        {"Effect":"Allow","Action":"s3vectors:CreateIndex","Resource":"*","Condition":index},
        {"Effect":"Deny","Action":"s3:*","Resource":"*"}
    ])
}

fn group_data_permissions(region: &str, index: &str) -> Value {
    json!([
        group_control(),
        {"Effect":"Allow","Action":["s3vectors:PutVectors","s3vectors:GetVectors","s3vectors:ListVectors",
            "s3vectors:QueryVectors","s3vectors:DeleteVectors"],"Resource":index,
            "Condition":{"StringEquals":{"aws:ResourceTag/project":"search",
                "s3vectors:VectorBucketTag/department":"research","aws:RequestedRegion":region}}},
        {"Effect":"Deny","Action":"s3:*","Resource":"*"}
    ])
}

#[test]
#[serial_test::serial]
fn vector_aws_sdk_group_conditions_and_membership_refresh_preserve_resources() {
    run_large_stack_test("vector-sdk-group-conditions", || async {
        let fixture = Fixture::new().await;
        let iam = fixture.app.iam().handle();
        let group = format!("vector-sdk-group-{}", Uuid::new_v4().simple());
        let server = SdkServer::new(&fixture).await;
        let client = &server.client;
        let mut group_created = false;
        let result = AssertUnwindSafe(async {
            iam.add_users_to_group(&group, vec![fixture.credentials.access_key.clone()])
                .await
                .unwrap();
            group_created = true;
            iam.policy_db_set(&group, UserType::Reg, true, &fixture.policy).await.unwrap();
            iam.policy_db_set(&fixture.credentials.access_key, UserType::Reg, false, "")
                .await
                .unwrap();
            let region = &fixture.service.region;
            let denied_region = if region == "us-west-2" { "us-east-1" } else { "us-west-2" };
            fixture.set_statements(group_creation_permissions(region)).await;
            let bucket = format!("sdk-group-{}", Uuid::new_v4().simple());
            let created = client
                .create_vector_bucket()
                .vector_bucket_name(&bucket)
                .tags("project", "search")
                .tags("department", "research")
                .send()
                .await
                .unwrap();
            let bucket_arn = created.vector_bucket_arn().unwrap();
            for (suffix, tags) in [
                ("project", HashMap::from([("project".into(), "other".into())])),
                (
                    "tagkeys",
                    HashMap::from([("project".into(), "search".into()), ("unapproved".into(), "true".into())]),
                ),
            ] {
                let absent = format!("{bucket}-{suffix}");
                service_error!(
                    client
                        .create_vector_bucket()
                        .vector_bucket_name(&absent)
                        .set_tags(Some(tags))
                        .send()
                        .await,
                    "AccessDeniedException",
                    403
                );
                service_error!(
                    client.get_vector_bucket().vector_bucket_name(&absent).send().await,
                    "NotFoundException",
                    404
                );
            }
            service_error!(
                client
                    .create_index()
                    .vector_bucket_name(&bucket)
                    .index_name("denied.tags")
                    .data_type(DataType::Float32)
                    .dimension(2)
                    .distance_metric(DistanceMetric::Euclidean)
                    .tags("project", "other")
                    .send()
                    .await,
                "AccessDeniedException",
                403
            );
            service_error!(
                client
                    .get_index()
                    .vector_bucket_name(&bucket)
                    .index_name("denied.tags")
                    .send()
                    .await,
                "NotFoundException",
                404
            );
            service_error!(
                client
                    .create_index()
                    .vector_bucket_name(&bucket)
                    .index_name("denied.encryption")
                    .data_type(DataType::Float32)
                    .dimension(2)
                    .distance_metric(DistanceMetric::Euclidean)
                    .tags("project", "search")
                    .encryption_configuration(EncryptionConfiguration::builder().sse_type(SseType::Aes256).build())
                    .send()
                    .await,
                "AccessDeniedException",
                403
            );
            service_error!(
                client
                    .get_index()
                    .vector_bucket_name(&bucket)
                    .index_name("denied.encryption")
                    .send()
                    .await,
                "NotFoundException",
                404
            );
            let created = client
                .create_index()
                .vector_bucket_name(&bucket)
                .index_name("group.index")
                .data_type(DataType::Float32)
                .dimension(2)
                .distance_metric(DistanceMetric::Euclidean)
                .tags("project", "search")
                .send()
                .await
                .unwrap();
            let index = created.index_arn().unwrap();
            assert!(
                client
                    .get_index()
                    .index_arn(index)
                    .send()
                    .await
                    .unwrap()
                    .index()
                    .unwrap()
                    .encryption_configuration()
                    .is_none()
            );
            let permitted = group_data_permissions(region, index);
            fixture.set_statements(permitted.clone()).await;
            client
                .put_vectors()
                .index_arn(index)
                .vectors(vector("alpha", [1.0, 2.0], "alpha"))
                .vectors(vector("beta", [3.0, 4.0], "beta"))
                .send()
                .await
                .unwrap();
            assert_rows(client, index).await;

            for condition in ["index_tag", "bucket_tag", "region"] {
                match condition {
                    "index_tag" => {
                        client
                            .tag_resource()
                            .resource_arn(index)
                            .tags("project", "other")
                            .send()
                            .await
                            .unwrap();
                    }
                    "bucket_tag" => {
                        client
                            .tag_resource()
                            .resource_arn(bucket_arn)
                            .tags("department", "other")
                            .send()
                            .await
                            .unwrap();
                    }
                    "region" => fixture.set_statements(group_data_permissions(denied_region, index)).await,
                    _ => unreachable!(),
                }
                service_error!(
                    client
                        .get_vectors()
                        .index_arn(index)
                        .keys("alpha")
                        .return_data(true)
                        .send()
                        .await,
                    "AccessDeniedException",
                    403
                );
                service_error!(
                    client
                        .put_vectors()
                        .index_arn(index)
                        .vectors(vector("alpha", [9.0, 9.0], "replaced"))
                        .send()
                        .await,
                    "AccessDeniedException",
                    403
                );
                service_error!(
                    client.delete_vectors().index_arn(index).keys("beta").send().await,
                    "AccessDeniedException",
                    403
                );
                match condition {
                    "index_tag" => {
                        client
                            .tag_resource()
                            .resource_arn(index)
                            .tags("project", "search")
                            .send()
                            .await
                            .unwrap();
                    }
                    "bucket_tag" => {
                        client
                            .tag_resource()
                            .resource_arn(bucket_arn)
                            .tags("department", "research")
                            .send()
                            .await
                            .unwrap();
                    }
                    "region" => fixture.set_statements(permitted.clone()).await,
                    _ => unreachable!(),
                }
                assert_rows(client, index).await;
            }
            fixture
                .set_statements(json!([{"Effect":"Allow","Action":"s3:*","Resource":"*"}]))
                .await;
            service_error!(
                client.get_vectors().index_arn(index).keys("alpha").send().await,
                "AccessDeniedException",
                403
            );
            service_error!(
                client
                    .put_vectors()
                    .index_arn(index)
                    .vectors(vector("alpha", [9.0, 9.0], "s3-only"))
                    .send()
                    .await,
                "AccessDeniedException",
                403
            );
            fixture.set_statements(permitted.clone()).await;
            assert_rows(client, index).await;
            iam.remove_users_from_group(&group, vec![fixture.credentials.access_key.clone()])
                .await
                .unwrap();
            service_error!(
                client.get_vectors().index_arn(index).keys("alpha").send().await,
                "AccessDeniedException",
                403
            );
            service_error!(
                client
                    .put_vectors()
                    .index_arn(index)
                    .vectors(vector("alpha", [9.0, 9.0], "removed-member"))
                    .send()
                    .await,
                "AccessDeniedException",
                403
            );
            iam.add_users_to_group(&group, vec![fixture.credentials.access_key.clone()])
                .await
                .unwrap();
            assert_rows(client, index).await;
            client
                .delete_vectors()
                .index_arn(index)
                .keys("alpha")
                .keys("beta")
                .send()
                .await
                .unwrap();
            client.delete_index().index_arn(index).send().await.unwrap();
        })
        .catch_unwind()
        .await;
        server.finish().await;
        if group_created {
            iam.remove_users_from_group(&group, vec![fixture.credentials.access_key.clone()])
                .await
                .unwrap();
            iam.remove_users_from_group(&group, Vec::new()).await.unwrap();
        }
        cleanup_iam(&fixture).await;
        if let Err(panic) = result {
            std::panic::resume_unwind(panic);
        }
    });
}

#[test]
#[serial_test::serial]
fn vector_aws_sdk_bucket_control_plane_enforces_abac_tags_and_deny_policies() {
    run_large_stack_test("vector-sdk-bucket-control-plane-auth", || async {
        let fixture = Fixture::new().await;
        let server = SdkServer::new(&fixture).await;
        let client = &server.client;
        let bucket = format!("sdk-bkt-auth-{}", Uuid::new_v4().simple());
        let result = AssertUnwindSafe(async {
            let created = client
                .create_vector_bucket()
                .vector_bucket_name(&bucket)
                .tags("env", "staging")
                .send()
                .await
                .unwrap();
            let bucket_arn = created.vector_bucket_arn().unwrap().to_owned();

            fixture
                .set_statements(json!([
                    {
                        "Effect": "Allow",
                        "Action": ["s3vectors:TagResource", "s3vectors:UntagResource", "s3vectors:GetVectorBucket"],
                        "Resource": "*"
                    },
                    {
                        "Effect": "Allow",
                        "Action": [
                            "s3vectors:PutVectorBucketPolicy",
                            "s3vectors:DeleteVectorBucketPolicy",
                            "s3vectors:DeleteVectorBucket"
                        ],
                        "Resource": bucket_arn,
                        "Condition": {"StringEquals": {"s3vectors:VectorBucketTag/env": "staging"}}
                    },
                    {"Effect": "Deny", "Action": "s3:*", "Resource": "*"}
                ]))
                .await;

            let allow_policy = json!({
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Allow",
                    "Principal": {"AWS": fixture.credentials.access_key},
                    "Action": "s3vectors:GetVectorBucket",
                    "Resource": bucket_arn
                }]
            })
            .to_string();
            client
                .put_vector_bucket_policy()
                .vector_bucket_name(&bucket)
                .policy(&allow_policy)
                .send()
                .await
                .unwrap();

            // Mutating the ABAC tag immediately revokes bucket policy & deletion authority.
            client
                .tag_resource()
                .resource_arn(&bucket_arn)
                .tags("env", "production")
                .send()
                .await
                .unwrap();
            service_error!(
                client.delete_vector_bucket_policy().vector_bucket_name(&bucket).send().await,
                "AccessDeniedException",
                403
            );
            service_error!(
                client.delete_vector_bucket().vector_bucket_name(&bucket).send().await,
                "AccessDeniedException",
                403
            );

            // Restore staging tag, install an explicit Deny bucket policy, and verify
            // control-plane mutations are refused with AccessDeniedException before invalid body checks.
            client
                .tag_resource()
                .resource_arn(&bucket_arn)
                .tags("env", "staging")
                .send()
                .await
                .unwrap();
            let deny_policy = json!({
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Deny",
                    "Principal": "*",
                    "Action": ["s3vectors:PutVectorBucketPolicy", "s3vectors:DeleteVectorBucket"],
                    "Resource": bucket_arn
                }]
            })
            .to_string();
            client
                .put_vector_bucket_policy()
                .vector_bucket_name(&bucket)
                .policy(&deny_policy)
                .send()
                .await
                .unwrap();
            service_error!(
                client
                    .put_vector_bucket_policy()
                    .vector_bucket_name(&bucket)
                    .policy("not-json")
                    .send()
                    .await,
                "AccessDeniedException",
                403
            );
            service_error!(
                client.delete_vector_bucket().vector_bucket_name(&bucket).send().await,
                "AccessDeniedException",
                403
            );
        })
        .catch_unwind()
        .await;
        server.finish().await;
        cleanup_iam(&fixture).await;
        if let Err(panic) = result {
            std::panic::resume_unwind(panic);
        }
    });
}
