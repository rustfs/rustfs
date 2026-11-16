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

//! ListParts wire behavior with real multipart sessions.

use super::super::{GATEWAY, LEGACY, create_bucket_in_the_store, refusal, s3_client, with_stack};
use aws_sdk_s3::error::ProvideErrorMetadata;
use rustfs::app::context::get_global_app_context;
use rustfs::embedded::RustFSServer;
use rustfs_ecstore::api::object::{ObjectOptions, PutObjReader};
use rustfs_rio::HashReader;
use rustfs_storage_api::{CompletePart, MultipartOperations as _};
use std::io::Cursor;

const BUCKET: &str = "list-parts-fixture";
const OTHER_BUCKET: &str = "list-parts-other";
const RESERVED_BUCKET: &str = "sthree-list-parts";
const KEY: &str = "dir a/object+b%雪";

async fn list_parts_wire(server: RustFSServer, legacy: bool) {
    for bucket in [BUCKET, OTHER_BUCKET, RESERVED_BUCKET] {
        create_bucket_in_the_store(bucket).await;
    }
    let client = s3_client(&server.endpoint(), server.access_key(), server.secret_key());
    let context = get_global_app_context().expect("embedded context");
    let store = context.object_store();
    let opts = ObjectOptions::default();
    let active = store
        .new_multipart_upload(BUCKET, KEY, &opts)
        .await
        .expect("create an active upload");
    let id = active.upload_id.as_str();
    let other = store
        .new_multipart_upload(BUCKET, "other", &opts)
        .await
        .expect("create a second upload");
    let aborted = store
        .new_multipart_upload(BUCKET, KEY, &opts)
        .await
        .expect("create an upload to abort");
    store
        .abort_multipart_upload(BUCKET, KEY, &aborted.upload_id, &opts)
        .await
        .expect("abort the fixture");
    let completed = store
        .new_multipart_upload(BUCKET, KEY, &opts)
        .await
        .expect("create an upload to complete");
    let completed_part = store
        .put_object_part(
            BUCKET,
            KEY,
            &completed.upload_id,
            1,
            &mut PutObjReader::from_vec(b"completed".to_vec()),
            &opts,
        )
        .await
        .expect("put a part to complete");
    store
        .clone()
        .complete_multipart_upload(
            BUCKET,
            KEY,
            &completed.upload_id,
            vec![CompletePart {
                part_num: 1,
                etag: completed_part.etag,
                ..Default::default()
            }],
            &opts,
        )
        .await
        .expect("complete the fixture");

    let list = || client.list_parts().bucket(BUCKET).key(KEY).upload_id(id);
    let empty = list().send().await.expect("empty upload");
    assert!(empty.parts().is_empty());
    assert_eq!(empty.next_part_number_marker(), None);

    for (number, tag, logical_size) in [(10, "plain10", 9), (1, "a&b", 7), (3, "plain3", 8)] {
        let data = HashReader::from_stream(Cursor::new(b"part".to_vec()), 4, logical_size, None, None, false).unwrap();
        let part_opts = ObjectOptions {
            preserve_etag: Some(tag.to_owned()),
            ..Default::default()
        };
        store
            .put_object_part(BUCKET, KEY, id, number, &mut PutObjReader::new(data), &part_opts)
            .await
            .expect("stage a real part with distinct wire fields");
    }
    for (marker, max, expected, next) in [
        (None, Some(1), &[1][..], Some("1")),
        (Some("1"), Some(2), &[3, 10][..], None),
        (Some("2"), Some(1), &[3][..], Some("3")),
        (Some("3"), Some(2), &[10][..], None),
        (Some("0"), Some(1000), &[1, 3, 10][..], None),
        (None, None, &[1, 3, 10][..], None),
        (Some("10000"), Some(2), &[][..], None),
        (Some("2147483647"), Some(2), &[][..], None),
    ] {
        let page = list()
            .set_part_number_marker(marker.map(str::to_owned))
            .set_max_parts(max)
            .send()
            .await
            .expect("page");
        assert_eq!(page.bucket(), Some(BUCKET));
        assert_eq!(page.key(), Some(KEY));
        assert!(page.upload_id() == Some(id));
        assert_eq!(page.max_parts(), Some(max.unwrap_or(1000)));
        assert_eq!(page.part_number_marker(), Some(marker.unwrap_or("0")));
        assert_eq!(page.is_truncated(), Some(next.is_some()));
        assert_eq!(page.next_part_number_marker(), next);
        assert_eq!(page.storage_class().map(|class| class.as_str()), Some("STANDARD"));
        let owner = page.owner().expect("response owner");
        let initiator = page.initiator().expect("response initiator");
        assert_eq!(owner.display_name(), Some("rustfs"));
        assert_eq!(owner.display_name(), initiator.display_name());
        assert!(owner.id().is_some());
        assert_eq!(owner.id(), initiator.id());
        assert_eq!(
            page.parts()
                .iter()
                .map(|part| part.part_number().unwrap())
                .collect::<Vec<_>>(),
            expected
        );
        for part in page.parts() {
            let (tag, size) = match part.part_number().unwrap() {
                1 => ("\"a&b\"", 7),
                3 => ("\"plain3\"", 8),
                10 => ("\"plain10\"", 9),
                _ => unreachable!("the part number was already checked"),
            };
            assert_eq!(part.e_tag(), Some(tag));
            assert_eq!(part.size(), Some(size));
            assert!(part.last_modified().is_some());
            assert!(part.checksum_sha256().is_none());
        }
    }

    for (bucket, key, upload_id) in [
        (OTHER_BUCKET, KEY, id),
        (BUCKET, "other", id),
        (BUCKET, KEY, other.upload_id.as_str()),
        (BUCKET, KEY, aborted.upload_id.as_str()),
        (BUCKET, KEY, completed.upload_id.as_str()),
        (BUCKET, KEY, "malformed"),
    ] {
        let err = client
            .list_parts()
            .bucket(bucket)
            .key(key)
            .upload_id(upload_id)
            .send()
            .await
            .expect_err("no session");
        assert_eq!(refusal(&err), (Some("NoSuchUpload".to_owned()), Some(404)));
    }
    assert_eq!(list().send().await.expect("original session").parts().len(), 3);

    for (marker, max, message) in [
        (Some("-1"), None, "part-number-marker must be non-negative"),
        (Some("invalid"), None, "invalid query: part-number-marker: invalid"),
        (Some(""), None, "invalid query: part-number-marker: "),
        (Some("2147483648"), None, "invalid query: part-number-marker: 2147483648"),
        (None, Some(-1), "max-parts must be between 1 and 1000"),
        (None, Some(0), "max-parts must be between 1 and 1000"),
        (None, Some(1001), "max-parts must be between 1 and 1000"),
        (Some("-1"), Some(0), "part-number-marker must be non-negative"),
    ] {
        let err = client
            .list_parts()
            .bucket(BUCKET)
            .key(KEY)
            .upload_id("malformed")
            .set_part_number_marker(marker.map(str::to_owned))
            .set_max_parts(max)
            .send()
            .await
            .expect_err("pagination is checked before the upload lookup");
        assert_eq!(
            refusal(&err),
            (Some("InvalidArgument".to_owned()), Some(400)),
            "marker {marker:?}, max {max:?}"
        );
        assert_eq!(err.message(), Some(message));
    }

    for bucket in [BUCKET, RESERVED_BUCKET] {
        let upload = store
            .new_multipart_upload(bucket, "dir/", &opts)
            .await
            .expect("stage one real directory upload");
        for (key, status, code) in [
            ("dir/", 200, ""),
            ("dir//", 400, "InvalidArgument"),
            ("dir__XLDIR__", if legacy { 200 } else { 404 }, "NoSuchUpload"),
            ("dir__XLDIR____XLDIR__", 404, "NoSuchUpload"),
        ] {
            let reply = client
                .list_parts()
                .bucket(bucket)
                .key(key)
                .upload_id(&upload.upload_id)
                .send()
                .await;
            if status == 200 {
                let page = reply.unwrap_or_else(|error| panic!("directory key {key:?}: {error:?}"));
                assert_eq!(page.key(), Some("dir/"));
                assert!(page.upload_id() == Some(upload.upload_id.as_str()));
            } else {
                assert_eq!(
                    refusal(&reply.expect_err("unresolved key")),
                    (Some(code.to_owned()), Some(status)),
                    "{key:?}"
                );
            }
        }
    }
    server.shutdown().await;
}

#[test]
fn list_parts_baseline_legacy() {
    with_stack(LEGACY, |server| list_parts_wire(server, true));
}

#[test]
fn list_parts_wire_gateway() {
    with_stack(GATEWAY, |server| list_parts_wire(server, false));
}

async fn list_parts_iam(server: RustFSServer) {
    use super::super::{USER_SECRET_KEY, counted_since, create_user_without_a_policy, s3_request_counts};
    use rustfs_iam::store::UserType;
    use rustfs_io_metrics::s3_op_metrics_snapshot;
    use rustfs_policy::policy::Policy;
    use rustfs_s3_ops::S3Operation;
    use serde_json::json;
    const ALLOWED: &str = "list-parts-allowed";
    const OTHER_ACTION: &str = "list-parts-put-only";
    const ALLOWED_KEY: &str = "allowed";
    const DENIED_KEY: &str = "denied";
    const MISSING: &str = "list-parts-missing";

    for bucket in [BUCKET, OTHER_BUCKET] {
        create_bucket_in_the_store(bucket).await;
    }
    let context = get_global_app_context().expect("embedded context");
    let store = context.object_store();
    let iam = context.iam().handle();
    for (user, action) in [(ALLOWED, "s3:ListMultipartUploadParts"), (OTHER_ACTION, "s3:PutObject")] {
        create_user_without_a_policy(user).await;
        let policy = json!({"Version":"2012-10-17","Statement":[
            {"Effect":"Allow","Action":action,"Resource":[
                format!("arn:aws:s3:::{BUCKET}/{ALLOWED_KEY}"), format!("arn:aws:s3:::{BUCKET}/{DENIED_KEY}")
            ]},
            {"Effect":"Deny","Action":action,"Resource":format!("arn:aws:s3:::{BUCKET}/{DENIED_KEY}")}
        ]});
        let policy = Policy::parse_config(policy.to_string().as_bytes()).expect("valid policy");
        iam.set_policy(user, policy).await.expect("store policy");
        iam.policy_db_set(user, UserType::Reg, false, user)
            .await
            .expect("attach policy");
    }
    let endpoint = server.endpoint();
    let allowed = s3_client(&endpoint, ALLOWED, USER_SECRET_KEY);
    let other_action = s3_client(&endpoint, OTHER_ACTION, USER_SECRET_KEY);
    let root = s3_client(&endpoint, server.access_key(), server.secret_key());
    let opts = ObjectOptions::default();
    let handled = || {
        s3_op_metrics_snapshot()
            .into_iter()
            .find(|row| row.op == S3Operation::ListParts.as_str())
            .map_or(0, |row| row.total)
    };
    for (client, bucket, key, fixture_bucket, code, status) in [
        (&allowed, BUCKET, ALLOWED_KEY, BUCKET, None, 200),
        (&allowed, BUCKET, DENIED_KEY, BUCKET, Some("AccessDenied"), 403),
        (&allowed, BUCKET, "other", BUCKET, Some("AccessDenied"), 403),
        (&allowed, OTHER_BUCKET, ALLOWED_KEY, OTHER_BUCKET, Some("AccessDenied"), 403),
        (&other_action, BUCKET, ALLOWED_KEY, BUCKET, Some("AccessDenied"), 403),
        (&allowed, MISSING, ALLOWED_KEY, BUCKET, Some("AccessDenied"), 403),
        (&root, MISSING, ALLOWED_KEY, BUCKET, Some("NoSuchBucket"), 404),
    ] {
        let stored = store
            .new_multipart_upload(fixture_bucket, key, &opts)
            .await
            .expect("fixture session");
        let before = s3_request_counts();
        let before_handled = handled();
        let response = client
            .list_parts()
            .bucket(bucket)
            .key(key)
            .upload_id(stored.upload_id)
            .send()
            .await;
        if let Some(code) = code {
            let error = response.expect_err("request should be refused");
            assert_eq!(refusal(&error), (Some(code.to_owned()), Some(status)), "{bucket}/{key}");
            let raw = error.raw_response().expect("wire error response");
            let request_id = raw.headers().get("x-amz-request-id").expect("error response request ID");
            assert_eq!(raw.headers().get("x-request-id"), Some(request_id));
        } else {
            assert!(response.expect("policy authorizes this object").parts().is_empty());
        }
        let counted = counted_since(&before);
        assert_eq!(counted.values().copied().sum::<u64>(), 1, "{bucket}/{key}: {counted:?}");
        assert_eq!(handled() - before_handled, u64::from(code.is_none()), "{bucket}/{key}");
    }

    let before = s3_request_counts();
    let before_handled = handled();
    let error = allowed
        .list_parts()
        .bucket(BUCKET)
        .key(ALLOWED_KEY)
        .upload_id("malformed")
        .max_parts(0)
        .send()
        .await
        .expect_err("post-authorization refusal");
    assert_eq!(refusal(&error), (Some("InvalidArgument".to_owned()), Some(400)));
    let counted = counted_since(&before);
    assert_eq!(handled() - before_handled, 1, "one handled call after authorization");
    assert_eq!(counted.values().copied().sum::<u64>(), 1, "{counted:?}");
    assert!(
        counted
            .keys()
            .all(|(_, operation, outcome)| (*operation, *outcome) == (S3Operation::ListParts.as_str(), "4xx")),
        "{counted:?}"
    );
    server.shutdown().await;
}

#[test]
fn list_parts_iam_legacy() {
    with_stack(LEGACY, list_parts_iam);
}

#[test]
fn list_parts_iam_gateway() {
    with_stack(GATEWAY, list_parts_iam);
}
