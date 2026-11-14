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

//! HeadObject wire checks and refusal-matrix row for migration batch C (rustfs/backlog#2767).

pub(crate) const MATRIX: &[super::MatrixRow] = &[super::MatrixRow {
    operation: "HeadObject",
    metric: rustfs_s3_ops::S3Operation::HeadObject,
    has_error_body: false,
    call: head_object,
}];

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
fn legacy_head_preserves_stored_headers_and_refusal_order() {
    super::with_stack(super::LEGACY, head_answers);
}

#[test]
#[serial_test::serial]
fn gateway_head_preserves_stored_headers_and_refusal_order() {
    super::with_stack(super::GATEWAY, head_answers);
}
