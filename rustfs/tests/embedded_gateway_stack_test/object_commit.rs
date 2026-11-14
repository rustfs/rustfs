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

//! Refusal-matrix rows of migration batch E1 (rustfs/backlog#2765), and how the legacy stack
//! answers CompleteMultipartUpload for every request shape whose conversion onto the gateway's
//! types needs care. CompleteMultipartUpload is served on both stacks.

use super::{LEGACY, create_bucket_in_the_store, s3_client, with_stack};
use aws_sdk_s3::Client;
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};

pub(crate) const MATRIX: &[super::MatrixRow] = &[super::MatrixRow {
    operation: "CompleteMultipartUpload",
    metric: rustfs_s3_ops::S3Operation::CompleteMultipartUpload,
    has_error_body: true,
    call: |client, bucket| {
        Box::pin(async move {
            use rustfs_ecstore::api::object::{ObjectOptions, PutObjReader};
            use rustfs_storage_api::MultipartOperations as _;
            let context = rustfs::app::context::get_global_app_context().expect("embedded context");
            let store = context.object_store();
            let opts = ObjectOptions::default();
            let upload = store
                .new_multipart_upload(bucket, "complete-matrix", &opts)
                .await
                .expect("matrix upload");
            let mut body = PutObjReader::from_vec(b"matrix part".to_vec());
            let stored = store
                .put_object_part(bucket, "complete-matrix", &upload.upload_id, 1, &mut body, &opts)
                .await
                .expect("matrix part");
            super::outcome(
                client
                    .complete_multipart_upload()
                    .bucket(bucket)
                    .key("complete-matrix")
                    .upload_id(upload.upload_id)
                    .multipart_upload(
                        CompletedMultipartUpload::builder()
                            .parts(part(Some(1), stored.etag.as_deref().expect("stored part ETag")))
                            .build(),
                    )
                    .send()
                    .await,
            )
        })
    },
}];

const ORDINARY_BUCKET: &str = "e1-complete";
/// Names RustFS creates and the AWS bucket grammar refuses.
const RESERVED_SHAPED_BUCKETS: [&str; 2] = ["sthree-e1", "e1-s3alias"];

/// What one CompleteMultipartUpload answered: `200` with the members a client reads, or the
/// refusal's status, code and message.
async fn complete(
    client: &Client,
    bucket: &str,
    key: &str,
    upload_id: &str,
    part: CompletedPart,
    checksums: &[(&str, &str)],
) -> String {
    let mut request = client
        .complete_multipart_upload()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .multipart_upload(CompletedMultipartUpload::builder().parts(part).build());
    for (algorithm, value) in checksums {
        request = match *algorithm {
            "crc32" => request.checksum_crc32(*value),
            "sha256" => request.checksum_sha256(*value),
            other => panic!("no setter for {other}"),
        };
    }
    match request.send().await {
        Ok(output) => format!(
            "200 bucket={:?} key_len={:?} etag={:?} crc32={:?} sha256={:?} type={:?}",
            output.bucket(),
            output.key().map(str::len),
            output.e_tag().map(|etag| etag.ends_with("-1\"")),
            output.checksum_crc32(),
            output.checksum_sha256(),
            output.checksum_type(),
        ),
        Err(error) => format!(
            "{:?} {:?} {:?}",
            error.raw_response().map(|response| response.status().as_u16()),
            error.code(),
            error.message()
        ),
    }
}

/// Creates an upload of one part on `key` and returns its id and the part's entity tag.
async fn one_part_upload(client: &Client, bucket: &str, key: &str) -> (String, String) {
    let upload_id = client
        .create_multipart_upload()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .expect("CreateMultipartUpload")
        .upload_id()
        .expect("an upload id")
        .to_owned();
    let etag = client
        .upload_part()
        .bucket(bucket)
        .key(key)
        .upload_id(&upload_id)
        .part_number(1)
        .body(ByteStream::from_static(b"one part"))
        .send()
        .await
        .expect("UploadPart")
        .e_tag()
        .expect("a part entity tag")
        .to_owned();
    (upload_id, etag)
}

fn part(number: Option<i32>, etag: &str) -> CompletedPart {
    CompletedPart::builder().set_part_number(number).e_tag(etag).build()
}

/// CompleteMultipartUpload on the legacy stack answers every request shape as before its use case
/// moved onto the gateway's types. Every answer was measured on `main` before the move.
#[test]
fn the_legacy_stack_completes_every_upload_as_before() {
    with_stack(LEGACY, |server| async move {
        let client = s3_client(&server.endpoint(), server.access_key(), server.secret_key());
        client
            .create_bucket()
            .bucket(ORDINARY_BUCKET)
            .send()
            .await
            .expect("CreateBucket");
        for bucket in RESERVED_SHAPED_BUCKETS {
            create_bucket_in_the_store(bucket).await;
        }
        let mut answered = Vec::new();

        for bucket in [ORDINARY_BUCKET].into_iter().chain(RESERVED_SHAPED_BUCKETS) {
            let (upload_id, etag) = one_part_upload(&client, bucket, "object").await;
            let answer = complete(&client, bucket, "object", &upload_id, part(Some(1), &etag), &[]).await;
            answered.push((format!("bucket {bucket}"), answer));
        }

        let too_long = "k".repeat(1025);
        for (label, key) in [("NUL key", "nul\0key"), ("1025-byte key", too_long.as_str())] {
            let answer = complete(&client, ORDINARY_BUCKET, key, "no-such-upload", part(Some(1), "\"0\""), &[]).await;
            answered.push((label.to_owned(), answer));
        }

        type ShapeCase<'a> = (&'a str, Option<i32>, fn(&str) -> String, &'a [(&'a str, &'a str)]);
        let shapes: [ShapeCase<'_>; 10] = [
            ("empty part ETag", Some(1), |_| String::new(), &[]),
            ("weak spelling of the stored part ETag", Some(1), |etag| format!("W/{etag}"), &[]),
            ("unquoted stored part ETag", Some(1), |etag| etag.trim_matches('"').to_owned(), &[]),
            (
                "quoted other part ETag",
                Some(1),
                |_| "\"00000000000000000000000000000000\"".to_owned(),
                &[],
            ),
            (
                "unquoted other part ETag",
                Some(1),
                |_| "00000000000000000000000000000000".to_owned(),
                &[],
            ),
            (
                "weak other part ETag",
                Some(1),
                |_| "W/\"00000000000000000000000000000000\"".to_owned(),
                &[],
            ),
            ("part without a number", None, str::to_owned, &[]),
            (
                "two checksum headers",
                Some(1),
                str::to_owned,
                &[
                    ("crc32", "AAAAAA=="),
                    ("sha256", "47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU="),
                ],
            ),
            ("checksum of the wrong width", Some(1), str::to_owned, &[("crc32", "AAAA")]),
            ("ordinary", Some(1), str::to_owned, &[("crc32", "AAAAAA==")]),
        ];
        for (index, (label, number, etag_for, checksums)) in shapes.into_iter().enumerate() {
            let key = format!("shape-{index}");
            let (upload_id, etag) = one_part_upload(&client, ORDINARY_BUCKET, &key).await;
            let answer = complete(&client, ORDINARY_BUCKET, &key, &upload_id, part(number, &etag_for(&etag)), checksums).await;
            answered.push((label.to_owned(), answer));
        }

        for (label, upload_id) in [
            ("upload id with a dot-dot segment", "a/../b"),
            ("upload id never minted", "bm90LWFuLXVwbG9hZA"),
        ] {
            let answer = complete(&client, ORDINARY_BUCKET, "object", upload_id, part(Some(1), "\"0\""), &[]).await;
            answered.push((label.to_owned(), answer));
        }

        // No body at all: the legacy decoder refuses it before the operation.
        let (upload_id, _) = one_part_upload(&client, ORDINARY_BUCKET, "no-body").await;
        let answer = match client
            .complete_multipart_upload()
            .bucket(ORDINARY_BUCKET)
            .key("no-body")
            .upload_id(&upload_id)
            .send()
            .await
        {
            Ok(_) => "200".to_owned(),
            Err(error) => format!(
                "{:?} {:?} {:?}",
                error.raw_response().map(|response| response.status().as_u16()),
                error.code(),
                error.message()
            ),
        };
        answered.push(("no body".to_owned(), answer));

        let expected = [
            (
                "bucket e1-complete",
                "200 bucket=Some(\"e1-complete\") key_len=Some(6) etag=Some(true) crc32=None sha256=None type=None",
            ),
            (
                "bucket sthree-e1",
                "200 bucket=Some(\"sthree-e1\") key_len=Some(6) etag=Some(true) crc32=None sha256=None type=None",
            ),
            (
                "bucket e1-s3alias",
                "200 bucket=Some(\"e1-s3alias\") key_len=Some(6) etag=Some(true) crc32=None sha256=None type=None",
            ),
            (
                "NUL key",
                "Some(400) Some(\"InvalidArgument\") Some(\"Object key contains invalid control characters: \\\"nul\\\\0key\\\"\")",
            ),
            ("1025-byte key", "Some(400) Some(\"KeyTooLongError\") Some(\"Your key is too long.\")"),
            (
                "empty part ETag",
                "Some(400) Some(\"InvalidPart\") Some(\"One or more of the specified parts could not be found.  The part may not have been uploaded, or the specified entity tag may not match the part's entity tag.\")",
            ),
            (
                "weak spelling of the stored part ETag",
                "200 bucket=Some(\"e1-complete\") key_len=Some(7) etag=Some(true) crc32=None sha256=None type=None",
            ),
            (
                "unquoted stored part ETag",
                "200 bucket=Some(\"e1-complete\") key_len=Some(7) etag=Some(true) crc32=None sha256=None type=None",
            ),
            (
                "quoted other part ETag",
                "Some(400) Some(\"InvalidPart\") Some(\"One or more of the specified parts could not be found.  The part may not have been uploaded, or the specified entity tag may not match the part's entity tag.\")",
            ),
            (
                "unquoted other part ETag",
                "Some(400) Some(\"InvalidPart\") Some(\"One or more of the specified parts could not be found.  The part may not have been uploaded, or the specified entity tag may not match the part's entity tag.\")",
            ),
            (
                "weak other part ETag",
                "Some(400) Some(\"InvalidPart\") Some(\"One or more of the specified parts could not be found.  The part may not have been uploaded, or the specified entity tag may not match the part's entity tag.\")",
            ),
            (
                "part without a number",
                "Some(400) Some(\"InvalidPart\") Some(\"Part number 0 must be between 1 and 10000\")",
            ),
            (
                "two checksum headers",
                "200 bucket=Some(\"e1-complete\") key_len=Some(7) etag=Some(true) crc32=Some(\"AAAAAA==\") sha256=Some(\"47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=\") type=None",
            ),
            (
                "checksum of the wrong width",
                "Some(400) Some(\"BadDigest\") Some(\"The Content-Md5 you specified did not match what we received.\")",
            ),
            (
                "ordinary",
                "200 bucket=Some(\"e1-complete\") key_len=Some(7) etag=Some(true) crc32=Some(\"AAAAAA==\") sha256=None type=None",
            ),
            (
                "upload id with a dot-dot segment",
                "Some(404) Some(\"NoSuchUpload\") Some(\"The specified multipart upload does not exist. The upload ID might be invalid, or the multipart upload might have been aborted or completed.\")",
            ),
            (
                "upload id never minted",
                "Some(404) Some(\"NoSuchUpload\") Some(\"The specified multipart upload does not exist. The upload ID may be invalid, or the upload may have been aborted or completed.\")",
            ),
            (
                "no body",
                "Some(400) Some(\"MalformedXML\") Some(\"This happens when the user sends malformed XML (XML that doesn't conform to the published XSD) for the configuration. The error message is, \\\"The XML you provided was not well-formed or did not validate against our published schema.\\\"\")",
            ),
        ];
        let expected: Vec<(String, String)> = expected
            .into_iter()
            .map(|(label, answer)| (label.to_owned(), answer.to_owned()))
            .collect();
        assert_eq!(answered, expected);
        server.shutdown().await;
    });
}

#[test]
fn legacy_absent_upload_refusal_order() {
    with_stack(LEGACY, |server| async move {
        use rustfs_ecstore::api::bucket::{
            metadata_sys,
            quota::{BucketQuota, checker::QuotaChecker},
        };
        use rustfs_ecstore::api::object::{ObjectOptions, PutObjReader};
        use rustfs_storage_api::{MultipartOperations, ObjectIO};
        let context = rustfs::app::context::get_global_app_context().expect("embedded context");
        let store = context.object_store();
        let client = s3_client(&server.endpoint(), server.access_key(), server.secret_key());
        for (shape, raw_id) in [("absent", None), ("dot", Some("a/../b")), ("absolute", Some("/upload"))] {
            for (label, full_quota, locked) in [
                ("wire-absent-quota", true, false),
                ("wire-absent", false, false),
                ("wire-absent-lock", false, true),
                ("wire-absent-both", true, true),
            ] {
                let bucket = format!("{shape}-{label}");
                let label = bucket.as_str();
                create_bucket_in_the_store(label).await;
                let upload = store
                    .new_multipart_upload(label, "object", &ObjectOptions::default())
                    .await
                    .expect("mint");
                store
                    .abort_multipart_upload(label, "object", &upload.upload_id, &ObjectOptions::default())
                    .await
                    .expect("abort");
                assert!(
                    store
                        .lookup_multipart_completion_binding(label, "object", &upload.upload_id, &ObjectOptions::default())
                        .await
                        .expect("binding")
                        .is_none()
                );
                if locked {
                    metadata_sys::update(
                        label,
                        rustfs_ecstore::api::bucket::metadata::OBJECT_LOCK_CONFIG,
                        b"<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled></ObjectLockConfiguration>"
                            .to_vec(),
                    )
                    .await
                    .expect("lock config");
                    let mut opts = ObjectOptions::default();
                    opts.user_defined
                        .insert("x-amz-object-lock-legal-hold".to_owned(), "ON".to_owned());
                    let mut reader = PutObjReader::from_vec(b"existing".to_vec());
                    store
                        .clone()
                        .put_object(label, "object", &mut reader, &opts)
                        .await
                        .expect("held object");
                }
                rustfs_ecstore::api::data_usage::seed_bucket_usage_memory_for_test(label, 2).await;
                if full_quota {
                    QuotaChecker::new(store.bucket_metadata_sys().expect("metadata context"))
                        .set_quota_config(
                            label,
                            BucketQuota {
                                quota: Some(1),
                                ..Default::default()
                            },
                        )
                        .await
                        .expect("quota");
                }
                let before = super::s3_request_counts();
                let answer =
                    complete(&client, label, "object", raw_id.unwrap_or(&upload.upload_id), part(Some(1), "part"), &[]).await;
                assert_eq!(
                    super::counted_since(&before),
                    std::collections::BTreeMap::from([(("POST", "s3:CompleteMultipartUpload", "4xx"), 1)]),
                    "{shape}/{label}"
                );
                let (status, code, message) = if raw_id.is_some() {
                    (
                        404,
                        "NoSuchUpload",
                        "The specified multipart upload does not exist. The upload ID might be invalid, or the multipart upload might have been aborted or completed.",
                    )
                } else if full_quota {
                    (400, "InvalidRequest", "Bucket quota exceeded. Current usage: 2 bytes, limit: 1 bytes")
                } else if locked {
                    (
                        403,
                        "AccessDenied",
                        "Object has a legal hold and cannot be overwritten. Remove the legal hold first.",
                    )
                } else {
                    (
                        404,
                        "NoSuchUpload",
                        "The specified multipart upload does not exist. The upload ID may be invalid, or the upload may have been aborted or completed.",
                    )
                };
                assert_eq!(answer, format!("{:?} {:?} {:?}", Some(status), Some(code), Some(message)), "{label}");
            }
        }
        server.shutdown().await;
    });
}

#[test]
fn gateway_completion_authorization_is_scoped_to_the_allowed_bucket() {
    super::with_stack(super::GATEWAY, |server| async move {
        const ALLOWED: &str = "e1-complete-allowed";
        const DENIED: &str = "e1-complete-denied";
        const USER: &str = "e1completescoped";
        for bucket in [ALLOWED, DENIED] {
            create_bucket_in_the_store(bucket).await;
        }
        super::create_user_allowed_on_one_bucket(USER, "s3:PutObject", &format!("{ALLOWED}/*")).await;
        let client = s3_client(&server.endpoint(), USER, super::USER_SECRET_KEY);
        assert_eq!((MATRIX[0].call)(&client, ALLOWED).await, Ok(()));
        let refusal = (MATRIX[0].call)(&client, DENIED)
            .await
            .expect_err("another bucket is not authorized");
        assert_eq!(refusal.code.as_deref(), Some("AccessDenied"));
        assert_eq!(refusal.status, Some(403));
        server.shutdown().await;
    });
}
