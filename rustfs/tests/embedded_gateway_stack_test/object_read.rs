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

//! Object read wire checks and refusal-matrix rows for migration batch C (rustfs/backlog#2767).

pub(crate) const MATRIX: &[super::MatrixRow] = &[
    super::MatrixRow {
        operation: "HeadObject",
        metric: rustfs_s3_ops::S3Operation::HeadObject,
        has_error_body: false,
        call: head_object,
    },
    super::MatrixRow {
        operation: "GetObject",
        metric: rustfs_s3_ops::S3Operation::GetObject,
        has_error_body: true,
        call: get_object,
    },
    super::MatrixRow {
        operation: "GetObjectAttributes",
        metric: rustfs_s3_ops::S3Operation::GetObjectAttributes,
        has_error_body: true,
        call: get_object_attributes,
    },
];

fn get_object<'a>(client: &'a aws_sdk_s3::Client, _bucket: &'a str) -> futures::future::BoxFuture<'a, super::Outcome> {
    Box::pin(async move {
        super::put_object_in_the_store(MATRIX_BUCKET, "get-matrix").await;
        super::outcome(client.get_object().bucket(MATRIX_BUCKET).key("get-matrix").send().await)
    })
}

fn get_object_attributes<'a>(client: &'a aws_sdk_s3::Client, _bucket: &'a str) -> futures::future::BoxFuture<'a, super::Outcome> {
    Box::pin(async move {
        super::put_object_in_the_store(MATRIX_BUCKET, "attributes-matrix").await;
        super::outcome(
            client
                .get_object_attributes()
                .bucket(MATRIX_BUCKET)
                .key("attributes-matrix")
                .object_attributes(aws_sdk_s3::types::ObjectAttributes::ObjectSize)
                .send()
                .await,
        )
    })
}

pub(super) const MATRIX_BUCKET: &str = "object-read-matrix";

fn head_object<'a>(client: &'a aws_sdk_s3::Client, _bucket: &'a str) -> futures::future::BoxFuture<'a, super::Outcome> {
    Box::pin(async move {
        super::put_object_in_the_store(MATRIX_BUCKET, "head-matrix").await;
        super::outcome(client.head_object().bucket(MATRIX_BUCKET).key("head-matrix").send().await)
    })
}

#[test]
#[serial_test::serial]
fn gateway_head_object_reports_missing_bucket_without_a_body() {
    super::with_stack(super::GATEWAY, |server| async move {
        let client = super::s3_client(&server.endpoint(), server.access_key(), server.secret_key());
        let error = client
            .head_object()
            .bucket("head-missing-bucket")
            .key("missing")
            .send()
            .await
            .unwrap_err();
        assert_eq!(error.raw_response().unwrap().status().as_u16(), 404);
        assert!(
            error
                .raw_response()
                .unwrap()
                .body()
                .bytes()
                .expect("collected HEAD body")
                .is_empty()
        );
    });
}

async fn create_user_with_read_policy(user: &str, policy: &serde_json::Value) {
    super::create_user_without_a_policy(user).await;
    let iam = super::get_global_app_context()
        .expect("embedded server context")
        .iam()
        .handle();
    let policy = rustfs_policy::policy::Policy::parse_config(policy.to_string().as_bytes()).expect("parse IAM policy");
    iam.set_policy(user, policy).await.expect("store IAM policy");
    iam.policy_db_set(user, rustfs_iam::store::UserType::Reg, false, user)
        .await
        .expect("attach IAM policy");
}

async fn assert_object_read_permissions(client: &aws_sdk_s3::Client, bucket: &str, caller: &str, status: u16, code: &str) {
    let object = client
        .get_object()
        .bucket(bucket)
        .key("present")
        .send()
        .await
        .expect("read fixture object");
    assert_eq!(object.body.collect().await.expect("read response body").into_bytes().as_ref(), b"payload");
    let head = client
        .head_object()
        .bucket(bucket)
        .key("present")
        .send()
        .await
        .expect("head fixture object");
    assert_eq!(head.content_length(), Some(7));

    let get = client
        .get_object()
        .bucket(bucket)
        .key("missing")
        .send()
        .await
        .expect_err("GET missing object");
    assert_eq!(super::refusal(&get), (Some(code.to_owned()), Some(status)), "{caller} GET {bucket}");
    let head = client
        .head_object()
        .bucket(bucket)
        .key("missing")
        .send()
        .await
        .expect_err("HEAD missing object");
    let response = head.raw_response().expect("HEAD wire response");
    assert_eq!(response.status().as_u16(), status, "{caller} HEAD {bucket}");
    assert!(response.body().bytes().expect("collected HEAD body").is_empty(), "{caller} HEAD {bucket}");
}

#[test]
#[serial_test::serial]
fn missing_object_read_permissions_for_signed_and_anonymous_callers() {
    super::with_stack(super::GATEWAY, |server| async move {
        let endpoint = server.endpoint();
        let root = super::s3_client(&endpoint, server.access_key(), server.secret_key());
        let anonymous = super::anonymous_client(&endpoint).await;
        for (bucket, list_effects, list_prefix, status, code) in [
            ("read-without-list", &[][..], None, 403, "AccessDenied"),
            ("read-with-list", &["Allow"][..], None, 404, "NoSuchKey"),
            ("read-with-denied-list", &["Allow", "Deny"][..], None, 403, "AccessDenied"),
            ("read-with-prefix-list", &["Allow"][..], Some("reports/"), 403, "AccessDenied"),
        ] {
            super::create_bucket_in_the_store(bucket).await;
            super::put_object_in_the_store(bucket, "present").await;
            let mut statements = vec![serde_json::json!({
                "Effect": "Allow", "Action": "s3:GetObject", "Resource": format!("arn:aws:s3:::{bucket}/*")
            })];
            for effect in list_effects {
                let mut statement = serde_json::json!({
                    "Effect": effect, "Action": "s3:ListBucket", "Resource": format!("arn:aws:s3:::{bucket}")
                });
                if let Some(prefix) = list_prefix {
                    statement["Condition"] = serde_json::json!({"StringEquals": {"s3:prefix": prefix}});
                }
                statements.push(statement);
            }
            let mut policy = serde_json::json!({"Version": "2012-10-17", "Statement": statements});
            create_user_with_read_policy(bucket, &policy).await;
            let signed = super::s3_client(&endpoint, bucket, super::USER_SECRET_KEY);
            assert_object_read_permissions(&signed, bucket, "signed", status, code).await;

            // Install public grants only after testing the signed caller's identity policy in isolation.
            for statement in policy["Statement"].as_array_mut().expect("policy statements") {
                statement["Principal"] = serde_json::json!("*");
            }
            root.put_bucket_policy()
                .bucket(bucket)
                .policy(policy.to_string())
                .send()
                .await
                .expect("store bucket policy");
            assert_object_read_permissions(&anonymous, bucket, "anonymous", status, code).await;
        }
    });
}

#[test]
#[serial_test::serial]
fn missing_object_read_permissions_honor_iam_list_deny_with_bucket_grant() {
    super::with_stack(super::GATEWAY, |server| async move {
        let bucket = "read-list-bucket-grant";
        super::create_bucket_in_the_store(bucket).await;
        super::put_object_in_the_store(bucket, "present").await;
        let endpoint = server.endpoint();
        let root = super::s3_client(&endpoint, server.access_key(), server.secret_key());
        let bucket_arn = format!("arn:aws:s3:::{bucket}");
        let bucket_policy = serde_json::json!({
            "Version": "2012-10-17",
            "Statement": [{"Effect": "Allow", "Principal": "*", "Action": "s3:ListBucket", "Resource": bucket_arn}]
        });
        root.put_bucket_policy()
            .bucket(bucket)
            .policy(bucket_policy.to_string())
            .send()
            .await
            .expect("grant ListBucket through bucket policy");

        let read = serde_json::json!({"Effect": "Allow", "Action": "s3:GetObject", "Resource": format!("{bucket_arn}/*")});
        let deny_list = serde_json::json!({"Effect": "Deny", "Action": "s3:ListBucket", "Resource": bucket_arn});
        for (user, statements, status, code) in [
            ("bucket-grant-reader", vec![read.clone()], 404, "NoSuchKey"),
            ("deny-list-reader", vec![read, deny_list], 403, "AccessDenied"),
        ] {
            let policy = serde_json::json!({"Version": "2012-10-17", "Statement": statements});
            create_user_with_read_policy(user, &policy).await;
            let signed = super::s3_client(&endpoint, user, super::USER_SECRET_KEY);
            assert_object_read_permissions(&signed, bucket, user, status, code).await;
        }
    });
}

async fn head_answers(server: super::RustFSServer) {
    use rustfs::app::context::get_global_app_context;
    use rustfs_ecstore::api::object::{ObjectOptions, PutObjReader};
    use rustfs_storage_api::ObjectIO;
    use std::collections::HashMap;
    let client = super::s3_client(&server.endpoint(), server.access_key(), server.secret_key());
    for bucket in ["head-ordinary", "sthree-head", "head-s3alias"] {
        super::create_bucket_in_the_store(bucket).await;
        let store = get_global_app_context().unwrap().object_store();
        store
            .put_object(
                bucket,
                "object",
                &mut PutObjReader::from_vec(b"payload".to_vec()),
                &ObjectOptions {
                    user_defined: HashMap::from([
                        ("cache-control".to_owned(), "max-age=7".to_owned()),
                        ("x-amz-restore".to_owned(), "ongoing-request=\"true\"".to_owned()),
                    ]),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let head = client
            .head_object()
            .bucket(bucket)
            .key("object")
            .send()
            .await
            .expect("stored object HEAD");
        assert_eq!(head.content_length(), Some(7));
        assert_eq!(head.cache_control(), Some("max-age=7"));
        assert_eq!(head.restore(), Some("ongoing-request=\"true\""));
        for (part, range, version, condition, status) in [
            (None, None, None, None, 404),
            (Some(0), None, None, None, 400),
            (Some(1), Some("bytes=0-1"), None, None, 400),
            (None, None, Some("not-a-version"), None, 400),
            (None, None, None, Some("*"), 304),
        ] {
            let key = if status == 404 { "missing" } else { "object" };
            let error = client
                .head_object()
                .bucket(bucket)
                .key(key)
                .set_part_number(part)
                .set_range(range.map(str::to_owned))
                .set_version_id(version.map(str::to_owned))
                .set_if_none_match(condition.map(str::to_owned))
                .send()
                .await
                .unwrap_err();
            assert_eq!(
                error.raw_response().expect("wire response").status().as_u16(),
                status,
                "{bucket} {part:?} {range:?} {version:?} {condition:?}"
            );
        }
    }
}

#[test]
#[serial_test::serial]
fn gateway_head_preserves_stored_headers_and_refusal_order() {
    super::with_stack(super::GATEWAY, head_answers);
}
