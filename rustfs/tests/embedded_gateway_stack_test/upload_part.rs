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

//! Storage-backed UploadPart wire tests.

use super::{CRC32_TRAILER_HEADERS, VALID_CRC32_TRAILER, WireAnswer, WireClient};
use rustfs_storage_api::MultipartOperations;

const BUCKET: &str = "upload-part-contract";
const OTHER: &str = "upload-part-other";
const KEY: &str = "part-object";
const ETAG: &str = "5d41402abc4b2a76b9719d911017c592";
const BOTH_CHECKSUMS: &[(&str, &str)] = &[
    ("x-amz-checksum-crc32", "NhCmhg=="),
    ("x-amz-checksum-sha256", "LPJNul+wow4m6DsqxbninhsWHlwfp0JecwQzYpOLmCQ="),
];

async fn upload(
    wire: &WireClient<'_>,
    path: &str,
    id: &str,
    number: i32,
    body: &[u8],
    headers: &[(&str, &str)],
    length: Option<usize>,
) -> WireAnswer {
    wire.request("PUT", &format!("/{path}?partNumber={number}&uploadId={id}"), body, headers, length)
        .await
}

fn xml_field<'a>(answer: &'a WireAnswer, name: &str) -> Option<&'a str> {
    let (_, tail) = answer.body.split_once(&format!("<{name}>"))?;
    let (value, _) = tail.split_once(&format!("</{name}>"))?;
    Some(value)
}

const MISSING_SESSION: &str = "The specified multipart upload does not exist. The upload ID may be invalid, or the upload may have been aborted or completed.";

fn assert_reply(reply: &WireAnswer, status: u16, code: Option<&str>, message: Option<&str>) {
    assert_eq!((reply.status, xml_field(reply, "Code")), (status, code), "{}", reply.body);
    if let Some(message) = message {
        assert_eq!(xml_field(reply, "Message"), Some(message), "{}", reply.body);
    }
}

fn assert_upload_success(reply: &WireAnswer, sha256: Option<&str>) {
    assert_reply(reply, 200, None, None);
    assert_eq!(
        reply.headers.get("etag").map(String::as_str),
        Some("\"5d41402abc4b2a76b9719d911017c592\"")
    );
    assert_eq!(reply.headers.get("x-amz-checksum-crc32").map(String::as_str), Some("NhCmhg=="));
    assert_eq!(reply.headers.get("x-amz-checksum-sha256").map(String::as_str), sha256);
}

async fn upload_part_contract(server: super::super::RustFSServer, gateway: bool) {
    for bucket in [BUCKET, OTHER] {
        super::super::create_bucket_in_the_store(bucket).await;
    }
    let endpoint = server.endpoint();
    let wire = WireClient {
        endpoint: &endpoint,
        access_key: server.access_key(),
        secret_key: server.secret_key(),
    };
    let store = super::super::get_global_app_context().unwrap().object_store();
    let options = Default::default();
    let path = format!("{BUCKET}/{KEY}");
    let id = store.new_multipart_upload(BUCKET, KEY, &options).await.unwrap().upload_id;
    let other_id = store.new_multipart_upload(OTHER, KEY, &options).await.unwrap().upload_id;
    let denied_key_id = store
        .new_multipart_upload(BUCKET, "other-key", &options)
        .await
        .unwrap()
        .upload_id;
    let aborted = store.new_multipart_upload(BUCKET, KEY, &options).await.unwrap().upload_id;
    store.abort_multipart_upload(BUCKET, KEY, &aborted, &options).await.unwrap();
    let both = upload(&wire, &path, &id, 1, b"hello", BOTH_CHECKSUMS, Some(5)).await;
    let shipped = if gateway {
        assert_reply(&both, 400, Some("InvalidRequest"), None);
        assert!(
            store
                .list_object_parts(BUCKET, KEY, &id, None, 20, &options)
                .await
                .unwrap()
                .parts
                .is_empty()
        );
        upload(&wire, &path, &id, 1, b"hello", &BOTH_CHECKSUMS[..1], Some(5)).await
    } else {
        both
    };
    assert_upload_success(&shipped, (!gateway).then_some(BOTH_CHECKSUMS[1].1));
    let max = upload(&wire, &path, &id, 10000, VALID_CRC32_TRAILER, &CRC32_TRAILER_HEADERS, None).await;
    assert_upload_success(&max, None);
    let unknown_id = store
        .new_multipart_upload(BUCKET, "another-key", &options)
        .await
        .unwrap()
        .upload_id;
    let wrong_key = format!("{BUCKET}/other-key");
    let wrong_bucket = format!("{OTHER}/{KEY}");
    let nul_key = format!("{BUCKET}/bad%00key");
    let bad_checksum = &[("x-amz-checksum-crc32", "AAAAAA==")];
    let wrong_width = &[("x-amz-checksum-crc32", "AAAA")];
    let bad_trailer = b"5\r\nhello\r\n0\r\nx-amz-checksum-crc32:AAAAAA==\r\n\r\n";
    let missing_trailer = b"5\r\nhello\r\n0\r\n\r\n";
    let mut replies = Vec::new();
    for (label, path, id, number, body, headers, length) in [
        ("zero", path.as_str(), id.as_str(), 0, b"hello".as_slice(), &[][..], Some(5)),
        ("negative", &path, &id, -1, b"hello", &[], Some(5)),
        ("above-max", &path, &id, 10001, b"hello", &[], Some(5)),
        ("malformed", &path, "malformed", 2, b"hello", &[], Some(5)),
        ("not-here", &path, &unknown_id, 2, b"hello", &[], Some(5)),
        ("aborted", &path, &aborted, 2, b"hello", &[], Some(5)),
        ("wrong-key", &wrong_key, &id, 2, b"hello", &[], Some(5)),
        ("wrong-bucket", &wrong_bucket, &id, 2, b"hello", &[], Some(5)),
        ("nul-key", &nul_key, &id, 2, b"hello", &[], Some(5)),
        ("unknown-length", &path, &id, 2, b"hello", &[], None),
        ("short", &path, &id, 2, b"abc", &[], Some(5)),
        ("bad-checksum", &path, &id, 2, b"hello", bad_checksum, Some(5)),
        ("wrong-width", &path, &id, 2, b"hello", wrong_width, Some(5)),
        ("bad-trailer", &path, &id, 2, bad_trailer, &CRC32_TRAILER_HEADERS, None),
        ("missing-trailer", &path, &id, 2, missing_trailer, &CRC32_TRAILER_HEADERS, None),
        ("bad-replacement", &path, &id, 1, b"world", bad_checksum, Some(5)),
    ] {
        replies.push((label, upload(&wire, path, id, number, body, headers, length).await));
    }
    let responses: Vec<_> = replies
        .iter()
        .map(|(label, reply)| (*label, reply.status, xml_field(reply, "Code")))
        .collect();
    for (label, reply) in &replies {
        let (status, code, message) = match *label {
            "zero" | "negative" | "above-max" => (400, "InvalidArgument", "partNumber must be between 1 and 10000"),
            "malformed" => (
                404,
                "NoSuchUpload",
                "The specified multipart upload does not exist. The upload ID might be invalid, or the multipart upload might have been aborted or completed.",
            ),
            "not-here" | "aborted" | "wrong-key" | "wrong-bucket" => (404, "NoSuchUpload", MISSING_SESSION),
            "nul-key" => (400, "InvalidArgument", "Invalid argument"),
            "unknown-length" => (411, "MissingContentLength", "missing header: content-length"),
            "short" => (
                400,
                "IncompleteBody",
                "You did not provide the number of bytes specified by the Content-Length HTTP header.",
            ),
            "bad-checksum" | "wrong-width" | "bad-trailer" | "missing-trailer" | "bad-replacement" => {
                (400, "BadDigest", "The Content-Md5 you specified did not match what we received.")
            }
            _ => panic!("unexpected case {label}"),
        };
        assert_eq!((reply.status, xml_field(reply, "Code")), (status, Some(code)), "{label}: {responses:?}");
        if !gateway {
            assert_eq!(xml_field(reply, "Message"), Some(message), "{label}");
        }
        for header in ["etag", "x-amz-checksum-crc32", "x-amz-checksum-sha256"] {
            assert!(!reply.headers.contains_key(header), "{label}: {header}");
        }
    }
    let parts = store
        .list_object_parts(BUCKET, KEY, &id, None, 20, &options)
        .await
        .unwrap()
        .parts;
    let summary: Vec<_> = parts
        .iter()
        .map(|part| (part.part_num, part.size, part.etag.as_deref()))
        .collect();
    for (bucket, key, session) in [(OTHER, KEY, &other_id), (BUCKET, "other-key", &denied_key_id)] {
        assert!(
            store
                .list_object_parts(bucket, key, session, None, 20, &options)
                .await
                .unwrap()
                .parts
                .is_empty()
        );
    }
    assert_eq!(summary, [(1, 5, Some(ETAG)), (10000, 5, Some(ETAG))]);
    let sdk = super::super::s3_client(&endpoint, server.access_key(), server.secret_key());
    let part = aws_sdk_s3::types::CompletedPart::builder().part_number(1).e_tag(ETAG).build();
    sdk.complete_multipart_upload()
        .bucket(BUCKET)
        .key(KEY)
        .upload_id(&id)
        .multipart_upload(aws_sdk_s3::types::CompletedMultipartUpload::builder().parts(part).build())
        .send()
        .await
        .unwrap();
    let completed = upload(&wire, &path, &id, 2, b"hello", &[], Some(5)).await;
    assert_reply(&completed, 404, Some("NoSuchUpload"), (!gateway).then_some(MISSING_SESSION));
    let readback = sdk.get_object().bucket(BUCKET).key(KEY).send().await.unwrap();
    assert_eq!(readback.body.collect().await.unwrap().into_bytes().as_ref(), b"hello");
    server.shutdown().await;
}

#[test]
#[serial_test::serial]
fn legacy_upload_part_fixed_main_contract() {
    super::super::with_stack(super::super::LEGACY, |server| upload_part_contract(server, false));
}

#[test]
#[serial_test::serial]
fn gateway_upload_part_wire_contract() {
    super::super::with_stack(super::super::GATEWAY, |server| upload_part_contract(server, true));
}

async fn scoped_user(server: super::super::RustFSServer) {
    const USER: &str = "uploadpartscoped";
    let endpoint = server.endpoint();
    for bucket in [BUCKET, OTHER] {
        super::super::create_bucket_in_the_store(bucket).await;
    }
    super::super::create_user_allowed_on_one_bucket(USER, "s3:PutObject", &format!("{BUCKET}/{KEY}")).await;

    let wire = WireClient {
        endpoint: &endpoint,
        access_key: USER,
        secret_key: super::super::USER_SECRET_KEY,
    };
    let store = super::super::get_global_app_context().unwrap().object_store();
    let opts = Default::default();
    for (bucket, key, allowed) in [(BUCKET, KEY, true), (BUCKET, "denied", false), (OTHER, KEY, false)] {
        let session = store.new_multipart_upload(bucket, key, &opts).await.unwrap().upload_id;
        let answer = upload(&wire, &format!("{bucket}/{key}"), &session, 1, b"hello", &[], Some(5)).await;
        let parts = store
            .list_object_parts(bucket, key, &session, None, 20, &opts)
            .await
            .unwrap()
            .parts;
        let facts: Vec<_> = parts
            .iter()
            .map(|part| (part.part_num, part.size, part.etag.as_deref()))
            .collect();
        let expected = [(1, 5, Some(ETAG))];
        assert_eq!(
            facts.as_slice(),
            if allowed { expected.as_slice() } else { &[] },
            "{bucket}/{key} after HTTP {}",
            answer.status,
        );
        if allowed {
            assert_reply(&answer, 200, None, None);
        } else {
            assert_reply(&answer, 403, Some("AccessDenied"), None);
        }
    }
    server.shutdown().await;
}

#[test]
#[serial_test::serial]
fn legacy_upload_part_scoped_policy() {
    super::super::with_stack(super::super::LEGACY, scoped_user);
}

#[test]
#[serial_test::serial]
fn gateway_upload_part_scoped_policy() {
    super::super::with_stack(super::super::GATEWAY, scoped_user);
}
