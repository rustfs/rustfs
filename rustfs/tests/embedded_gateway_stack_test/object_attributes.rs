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

//! Signed GetObjectAttributes observations against the embedded server.

use super::{
    GATEWAY, LEGACY, USER_SECRET_KEY, create_bucket_in_the_store, create_user_without_a_policy, s3_client, with_environment,
};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{ChecksumAlgorithm, CompletedMultipartUpload, CompletedPart, ServerSideEncryption};
use bytes::Bytes;
use http::HeaderMap;
use http_body_util::{BodyExt as _, Empty};
use hyper_util::rt::TokioIo;
use rustfs::app::context::get_global_app_context;
use rustfs::embedded::RustFSServer;
use rustfs_ecstore::api::object::{ObjectOptions, PutObjReader};
use rustfs_s3_ops::S3Operation;
use rustfs_storage_api::{
    BucketOperations as _, CompletePart, MakeBucketOptions, MultipartOperations as _, ObjectIO as _, ObjectOperations as _,
};

const BUCKET: &str = "object-attributes";
const RESERVED: &str = "sthree-object-attributes";
const VERSIONED: &str = "versioned-object-attributes";
const ATTRIBUTES: &str = "x-amz-object-attributes";
const MARKER: &str = "x-amz-part-number-marker";
const MAX: &str = "x-amz-max-parts";
const SSE_ALGORITHM: &str = "x-amz-server-side-encryption-customer-algorithm";
const SSE_KEY: &str = "x-amz-server-side-encryption-customer-key";
const SSE_MD5: &str = "x-amz-server-side-encryption-customer-key-md5";

fn attributes_count() -> u64 {
    rustfs_io_metrics::s3_op_metrics_snapshot()
        .into_iter()
        .find(|entry| entry.op == S3Operation::GetObjectAttributes.as_str())
        .map_or(0, |entry| entry.total)
}

struct Observation {
    status: u16,
    headers: HeaderMap,
    body: Bytes,
    count: u64,
}

impl Observation {
    fn xml(&self) -> &str {
        std::str::from_utf8(&self.body).expect("XML response bytes")
    }

    fn status(&self, expected: u16, count: u64, label: &str) {
        assert_eq!((self.status, self.count), (expected, count), "{label}: {}", self.xml());
    }

    fn contains(&self, field: &str, value: &str, label: &str) {
        assert!(self.xml().contains(&format!("<{field}>{value}</{field}>")), "{label}: {}", self.xml());
    }

    fn omits(&self, field: &str, label: &str) {
        assert!(!self.xml().contains(&format!("<{field}>")), "{label}: {}", self.xml());
    }
}

async fn observe(server: &RustFSServer, path: &str, headers: &[(&str, &str)], user: Option<&str>) -> Observation {
    let endpoint = server.endpoint();
    let uri: http::Uri = format!("{endpoint}{path}").parse().unwrap();
    let mut request = http::Request::get(&uri)
        .header(http::header::HOST, uri.authority().unwrap().as_str())
        .header("x-amz-content-sha256", "UNSIGNED-PAYLOAD");
    for (name, value) in headers {
        request = request.header(*name, *value);
    }
    let (key, secret) = user.map_or((server.access_key(), server.secret_key()), |user| (user, USER_SECRET_KEY));
    let request = request.body(Empty::<Bytes>::new()).unwrap();
    let mut request = rustfs_signer::try_sign_v4(request, 0, key, secret, "", "us-east-1").unwrap();
    *request.uri_mut() = path.parse().unwrap();

    let before = attributes_count();
    let stream = tokio::net::TcpStream::connect(server.address()).await.unwrap();
    let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(stream)).await.unwrap();
    let transport = tokio::spawn(connection);
    let (parts, body) = sender.send_request(request).await.unwrap().into_parts();
    let body = body.collect().await.unwrap().to_bytes();
    drop(sender);
    transport.abort();
    let _ = transport.await;
    Observation {
        status: parts.status.as_u16(),
        headers: parts.headers,
        body,
        count: attributes_count() - before,
    }
}

async fn attributes_wire(server: RustFSServer, stack: Option<&str>) {
    for bucket in [BUCKET, RESERVED] {
        create_bucket_in_the_store(bucket).await;
    }
    let context = get_global_app_context().unwrap();
    let store = context.object_store();
    store
        .make_bucket(
            VERSIONED,
            &MakeBucketOptions {
                versioning_enabled: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let sdk = s3_client(&server.endpoint(), server.access_key(), server.secret_key());

    let tags = ["abc", "\"abc\"", "W/\"abc\"", "", "\"\"", "a&b", "a\"b"];
    for (index, tag) in tags.into_iter().enumerate() {
        let bucket = if index == 0 { RESERVED } else { BUCKET };
        let key = format!("tag-{index}");
        store
            .put_object(
                bucket,
                &key,
                &mut PutObjReader::from_vec(b"payload".to_vec()),
                &ObjectOptions {
                    preserve_etag: Some(tag.to_owned()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let observed = observe(
            &server,
            &format!("/{bucket}/{key}?attributes"),
            &[(ATTRIBUTES, "ETag,ObjectSize,StorageClass")],
            None,
        )
        .await;
        assert_eq!((observed.status, observed.count), (200, 1), "{key}");
        assert!(observed.headers.contains_key(http::header::LAST_MODIFIED), "{key} date");
        let expected = ["abc", "abc", "abc", "", "", "a&amp;b", "a&quot;b"];
        if stack == LEGACY {
            let body = std::str::from_utf8(&observed.body).unwrap();
            assert_eq!(
                body,
                format!(
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?><GetObjectAttributesResponse xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><ETag>{}</ETag><ObjectSize>7</ObjectSize><StorageClass>STANDARD</StorageClass></GetObjectAttributesResponse>",
                    expected[index]
                ),
                "{key} raw XML"
            );
        }
    }

    sdk.put_object()
        .bucket(BUCKET)
        .key("prefix/child")
        .body(ByteStream::from_static(b"payload"))
        .checksum_algorithm(ChecksumAlgorithm::Sha256)
        .send()
        .await
        .unwrap();
    let version = sdk
        .put_object()
        .bucket(VERSIONED)
        .key("versioned")
        .body(ByteStream::from_static(b"versioned"))
        .send()
        .await
        .unwrap()
        .version_id
        .expect("stored version");
    let delete = sdk
        .delete_object()
        .bucket(VERSIONED)
        .key("versioned")
        .send()
        .await
        .unwrap()
        .version_id
        .expect("delete marker version");

    for (label, bucket, key, version, headers) in [
        ("absent-selection", BUCKET, "tag-1", None, vec![]),
        ("empty-selection", BUCKET, "tag-1", None, vec![(ATTRIBUTES, "")]),
        ("unknown-selection", BUCKET, "tag-1", None, vec![(ATTRIBUTES, "Unknown")]),
        (
            "normalized-selection",
            BUCKET,
            "tag-1",
            None,
            vec![(ATTRIBUTES, " 'eTAG' , \"oBJECTsIZE\" , Unknown ")],
        ),
        ("etag-only", BUCKET, "tag-1", None, vec![(ATTRIBUTES, "ETag")]),
        (
            "size-only",
            BUCKET,
            "tag-1",
            None,
            vec![(ATTRIBUTES, "ObjectSize"), (MARKER, "-1"), (MAX, "0")],
        ),
        ("class-only", BUCKET, "tag-1", None, vec![(ATTRIBUTES, "StorageClass")]),
        ("checksum-only", BUCKET, "prefix/child", None, vec![(ATTRIBUTES, "Checksum")]),
        (
            "ordinary-object-parts",
            BUCKET,
            "prefix/child",
            None,
            vec![(ATTRIBUTES, "ObjectParts"), (MARKER, "-1"), (MAX, "0")],
        ),
        (
            "preserved-short-tag-parts",
            BUCKET,
            "tag-1",
            None,
            vec![(ATTRIBUTES, "ObjectParts"), (MARKER, "-1"), (MAX, "0")],
        ),
        ("missing-bucket", "missing-attributes-bucket", "tag-1", None, vec![(ATTRIBUTES, "ETag")]),
        (
            "missing-object",
            BUCKET,
            "missing",
            None,
            vec![(ATTRIBUTES, "ETag"), (SSE_KEY, "invalid")],
        ),
        ("prefix-with-child", BUCKET, "prefix/", None, vec![(ATTRIBUTES, "ETag")]),
        ("prefix-without-child", BUCKET, "empty-prefix/", None, vec![(ATTRIBUTES, "ETag")]),
        (
            "bad-version-before-lookup",
            BUCKET,
            "missing",
            Some("invalid"),
            vec![(ATTRIBUTES, "ETag")],
        ),
        (
            "absent-real-version",
            VERSIONED,
            "versioned",
            Some("11111111-1111-4111-8111-111111111111"),
            vec![(ATTRIBUTES, "ETag")],
        ),
        ("null-version", BUCKET, "tag-1", Some("null"), vec![(ATTRIBUTES, "ETag")]),
        ("empty-version", BUCKET, "tag-1", Some(""), vec![(ATTRIBUTES, "ETag")]),
        (
            "existing-version",
            VERSIONED,
            "versioned",
            Some(version.as_str()),
            vec![(ATTRIBUTES, "ObjectSize")],
        ),
        (
            "current-marker",
            VERSIONED,
            "versioned",
            None,
            vec![(ATTRIBUTES, "ETag"), (SSE_KEY, "invalid")],
        ),
        (
            "explicit-marker",
            VERSIONED,
            "versioned",
            Some(delete.as_str()),
            vec![(ATTRIBUTES, "ETag"), (SSE_KEY, "invalid")],
        ),
    ] {
        let mut path = format!("/{bucket}/{key}?attributes");
        if let Some(version) = version {
            path.push_str(&format!("&versionId={version}"));
        }
        let observed = observe(&server, &path, &headers, None).await;
        if stack == LEGACY {
            let (status, count, code) = match label {
                "absent-selection" => (400, 0, "InvalidRequest"),
                "bad-version-before-lookup" | "empty-version" => (400, 1, "InvalidArgument"),
                "preserved-short-tag-parts" => (400, 1, "InvalidArgument"),
                "absent-real-version"
                | "missing-bucket"
                | "missing-object"
                | "current-marker"
                | "prefix-with-child"
                | "prefix-without-child" => (404, 1, "NoSuchKey"),
                "explicit-marker" => (405, 1, "MethodNotAllowed"),
                _ => (200, 1, ""),
            };
            observed.status(status, count, label);
            if !code.is_empty() {
                observed.contains("Code", code, label);
            }
        }
        match label {
            "normalized-selection" | "etag-only" if stack == LEGACY => observed.contains("ETag", "abc", label),
            "size-only" => {
                observed.contains("ObjectSize", "7", label);
                observed.omits("ObjectParts", label);
            }
            "class-only" => observed.contains("StorageClass", "STANDARD", label),
            "checksum-only" => observed.contains("ChecksumSHA256", "I59Z7VXnN8dxR89VrQwbAwttfudIp0JpUvm4UtWpNeU=", label),
            "ordinary-object-parts" => {
                observed.status(200, 1, label);
                observed.omits("ObjectParts", label);
            }
            "preserved-short-tag-parts" => {
                observed.status(400, 1, label);
                observed.contains("Message", "part-number-marker must be non-negative", label);
            }
            "existing-version" => {
                observed.contains("ObjectSize", "9", label);
                assert_eq!(observed.headers["x-amz-version-id"], version.unwrap());
            }
            "current-marker" | "explicit-marker" => {
                for header in ["x-amz-delete-marker", "x-amz-version-id", "last-modified"] {
                    assert!(!observed.headers.contains_key(header), "{label}: {header}");
                }
            }
            "prefix-with-child" => assert!(observed.xml().contains("prefix has children objects"), "{label}: {}", observed.xml()),
            "prefix-without-child" => assert!(
                observed.xml().contains("no objects exist under this prefix"),
                "{label}: {}",
                observed.xml()
            ),
            _ => {}
        }
    }
    let checksum_algorithm = ChecksumAlgorithm::Sha256;
    let upload = sdk
        .create_multipart_upload()
        .bucket(BUCKET)
        .key("multipart")
        .checksum_algorithm(checksum_algorithm.clone())
        .send()
        .await
        .unwrap()
        .upload_id
        .unwrap();
    let mut parts = Vec::new();
    for (number, data) in [(1, vec![1; 5 * 1024 * 1024]), (3, b"last".to_vec())] {
        let part = sdk
            .upload_part()
            .bucket(BUCKET)
            .key("multipart")
            .upload_id(&upload)
            .part_number(number)
            .checksum_algorithm(checksum_algorithm.clone())
            .body(ByteStream::from(data))
            .send()
            .await
            .unwrap();
        parts.push(
            CompletedPart::builder()
                .part_number(number)
                .set_e_tag(part.e_tag)
                .set_checksum_sha256(part.checksum_sha256)
                .build(),
        );
    }
    sdk.complete_multipart_upload()
        .bucket(BUCKET)
        .key("multipart")
        .upload_id(upload)
        .multipart_upload(CompletedMultipartUpload::builder().set_parts(Some(parts)).build())
        .send()
        .await
        .unwrap();

    for (label, marker, max) in [
        ("parts-default", None, None),
        ("parts-first", None, Some("1")),
        ("parts-terminal", Some("1"), Some("1")),
        ("parts-empty", Some("3"), Some("1")),
        ("parts-missing-marker", Some("2"), Some("1")),
        ("parts-zero-marker", Some("0"), Some("1000")),
        ("parts-bad-marker-first", Some("-1"), Some("0")),
        ("parts-zero-max", None, Some("0")),
        ("parts-negative-max", None, Some("-1")),
        ("parts-over-max", None, Some("1001")),
        ("parts-marker-overflow", Some("2147483648"), Some("1")),
        ("parts-marker-text", Some("bad"), Some("1")),
    ] {
        let mut headers = vec![(ATTRIBUTES, "ObjectParts,Checksum,ObjectSize")];
        headers.extend(marker.map(|value| (MARKER, value)));
        headers.extend(max.map(|value| (MAX, value)));
        let observed = observe(&server, &format!("/{BUCKET}/multipart?attributes"), &headers, None).await;
        let (parts, echoed_marker, next, truncated): (&[&str], Option<&str>, Option<&str>, &str) = match label {
            "parts-default" | "parts-zero-marker" => (&["1", "3"], None, Some("3"), "false"),
            "parts-first" | "parts-missing-marker" => (&["1"], None, Some("1"), "true"),
            "parts-terminal" => (&["3"], Some("1"), Some("3"), "false"),
            "parts-empty" => (&[], Some("3"), None, "false"),
            _ => {
                assert_eq!(observed.status, 400, "{label}: {}", observed.xml());
                observed.contains("Code", "InvalidArgument", label);
                match label {
                    "parts-bad-marker-first" => observed.contains("Message", "part-number-marker must be non-negative", label),
                    "parts-over-max" | "parts-negative-max" | "parts-zero-max" => {
                        observed.contains("Message", "max-parts must be between 1 and 1000", label);
                    }
                    _ => {}
                }
                continue;
            }
        };
        observed.status(200, 1, label);
        observed.contains("MaxParts", max.unwrap_or("1000"), label);
        observed.contains("PartsCount", "2", label);
        observed.contains("IsTruncated", truncated, label);
        observed.contains("ObjectSize", "5242884", label);
        assert_eq!(observed.xml().matches("<Part>").count(), parts.len(), "{label}: {}", observed.xml());
        for number in parts {
            observed.contains("PartNumber", number, label);
        }
        for (field, value) in [("PartNumberMarker", echoed_marker), ("NextPartNumberMarker", next)] {
            match value {
                Some(value) => observed.contains(field, value, label),
                None => observed.omits(field, label),
            }
        }
    }

    // Persist the per-part checksum so the wire response proves the read requested it.
    let checksum_key = "stored-part-checksum";
    let upload = store
        .new_multipart_upload(BUCKET, checksum_key, &Default::default())
        .await
        .unwrap();
    let part = store
        .put_object_part(
            BUCKET,
            checksum_key,
            &upload.upload_id,
            1,
            &mut PutObjReader::from_vec(b"123456789".to_vec()),
            &Default::default(),
        )
        .await
        .unwrap();
    let mut user_defined = std::collections::HashMap::new();
    rustfs_utils::http::insert_str(
        &mut user_defined,
        rustfs_utils::http::SUFFIX_PART_CHECKSUMS,
        r#"[[1,[["CRC32C","4waSgw=="]]]]"#.to_owned(),
    );
    let mut options = ObjectOptions {
        user_defined,
        ..Default::default()
    };
    options.mod_time = Some(time::OffsetDateTime::from_unix_timestamp_nanos(1_609_459_200_123_456_789).unwrap());
    store
        .clone()
        .complete_multipart_upload(
            BUCKET,
            checksum_key,
            &upload.upload_id,
            vec![CompletePart {
                part_num: part.part_num,
                etag: part.etag,
                ..Default::default()
            }],
            &options,
        )
        .await
        .unwrap();
    let stored = store
        .get_object_info(
            BUCKET,
            checksum_key,
            &ObjectOptions {
                include_part_checksums: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        stored.parts[0]
            .checksums
            .as_ref()
            .and_then(|checksums| checksums.get("CRC32C"))
            .map(String::as_str),
        Some("4waSgw==")
    );
    let observed = observe(
        &server,
        &format!("/{BUCKET}/{checksum_key}?attributes"),
        &[(ATTRIBUTES, "ObjectParts")],
        None,
    )
    .await;
    assert_eq!((observed.status, observed.count), (200, 1), "stored part checksum");
    observed.contains("ChecksumCRC32C", "4waSgw==", "stored part checksum");
    assert_eq!(observed.headers[http::header::LAST_MODIFIED], "Fri, 01 Jan 2021 00:00:00 GMT");

    let customer_key = base64_simd::STANDARD.encode_to_string([0_u8; 32]);
    let wrong_key = base64_simd::STANDARD.encode_to_string([1_u8; 32]);
    let customer_md5 = "cLyPS3KoaSFGi/joRB3OUQ==";
    let wrong_md5 = "4Funlf7OsLF0HL+vKU+fkg==";
    sdk.put_object()
        .bucket(BUCKET)
        .key("ssec")
        .body(ByteStream::from_static(b"encrypted"))
        .checksum_algorithm(ChecksumAlgorithm::Sha256)
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&customer_key)
        .sse_customer_key_md5(customer_md5)
        .send()
        .await
        .unwrap();
    for (label, key, md5) in [
        ("ssec-valid", Some(customer_key.as_str()), Some(customer_md5)),
        ("ssec-missing", None, None),
        ("ssec-wrong", Some(wrong_key.as_str()), Some(wrong_md5)),
        ("ssec-wrong-md5", Some(wrong_key.as_str()), Some("4FtyTg0JFp3p7ItXND3wOg==")),
        ("ssec-malformed-key", Some("bad"), Some(customer_md5)),
        ("ssec-malformed-md5", Some(customer_key.as_str()), Some("bad")),
    ] {
        let mut headers = vec![(ATTRIBUTES, "Checksum,ObjectParts,ObjectSize"), (MAX, "0")];
        if let Some(key) = key {
            headers.extend([(SSE_ALGORITHM, "AES256"), (SSE_KEY, key)]);
        }
        headers.extend(md5.map(|md5| (SSE_MD5, md5)));
        let observed = observe(&server, &format!("/{BUCKET}/ssec?attributes"), &headers, None).await;
        if label == "ssec-valid" {
            observed.status(200, 1, label);
            observed.contains("ChecksumSHA256", "lU0buD2Au29udGso8N4+xMTtmAz+Z+0jqRWc1GT/M5o=", label);
        } else {
            assert_eq!(observed.status, 400, "{label}: {}", observed.xml());
            if stack == LEGACY {
                observed.contains("Code", "InvalidRequest", label);
                if label == "ssec-wrong" {
                    observed.contains(
                        "Message",
                        "The provided encryption parameters did not match the ones used originally to encrypt the object.",
                        label,
                    );
                }
            }
        }
    }

    let kms = context.kms().handle();
    kms.reconfigure(rustfs_kms::config::KmsConfig::static_kms(
        "attributes-test-key".to_owned(),
        base64_simd::STANDARD.encode_to_string([1_u8; 32]),
    ))
    .await
    .unwrap();
    sdk.put_object()
        .bucket(BUCKET)
        .key("managed")
        .body(ByteStream::from_static(b"managed encryption"))
        .checksum_algorithm(ChecksumAlgorithm::Sha256)
        .server_side_encryption(ServerSideEncryption::AwsKms)
        .ssekms_key_id("attributes-test-key")
        .send()
        .await
        .unwrap();
    let observed = observe(
        &server,
        &format!("/{BUCKET}/managed?attributes"),
        &[(ATTRIBUTES, "Checksum,ObjectSize")],
        None,
    )
    .await;
    observed.status(200, 1, "managed-kms");
    observed.contains("ChecksumSHA256", "UT6FnUrN4M3d+mdIobVx+iO4EA6lvEyWBtoT7rzzVbg=", "managed-kms");
    observed.contains("ObjectSize", "18", "managed-kms");
    kms.stop().await.unwrap();

    for (index, denied_action, version, bucket, object) in [
        (0, None, None, BUCKET, "tag-1"),
        (1, Some("s3:GetObjectAttributes"), None, BUCKET, "tag-1"),
        (2, Some("s3:GetObject"), None, BUCKET, "tag-1"),
        (3, None, Some(version.as_str()), VERSIONED, "versioned"),
        (4, Some("s3:GetObjectVersionAttributes"), Some(version.as_str()), VERSIONED, "versioned"),
        (5, Some("s3:GetObjectVersion"), Some(version.as_str()), VERSIONED, "versioned"),
        (6, Some("s3:GetObjectVersionAttributes"), Some(""), BUCKET, "tag-1"),
        (7, Some("s3:GetObjectVersion"), Some("null"), BUCKET, "tag-1"),
        (8, None, None, RESERVED, "tag-0"),
        (9, None, None, BUCKET, "tag-2"),
        (10, Some("s3:GetObjectVersion"), Some(version.as_str()), VERSIONED, "versioned"),
    ] {
        let user = format!("attributes-user-{index}");
        create_user_without_a_policy(&user).await;
        let mut statements = vec![serde_json::json!({
            "Effect": "Allow",
            "Action": ["s3:GetObject", "s3:GetObjectAttributes", "s3:GetObjectVersion", "s3:GetObjectVersionAttributes"],
            "Resource": [format!("arn:aws:s3:::{BUCKET}/tag-1"), format!("arn:aws:s3:::{VERSIONED}/versioned")],
        })];
        if let Some(action) = denied_action {
            let mut denial = serde_json::json!({
                "Effect": "Deny",
                "Action": [action],
                "Resource": [format!("arn:aws:s3:::{bucket}/{object}")],
            });
            if index == 10 {
                denial["Condition"] = serde_json::json!({"StringEquals": {"s3:versionid": version}});
            }
            statements.push(denial);
        }
        let bytes = serde_json::to_vec(&serde_json::json!({"Version": "2012-10-17", "Statement": statements})).unwrap();
        let iam = context.iam().handle();
        let policy = rustfs_policy::policy::Policy::parse_config(&bytes).unwrap();
        iam.set_policy(&user, policy).await.unwrap();
        iam.policy_db_set(&user, rustfs_iam::store::UserType::Reg, false, &user)
            .await
            .unwrap();

        let mut path = format!("/{bucket}/{object}?attributes");
        if let Some(version) = version {
            path.push_str(&format!("&versionId={version}"));
        }
        let observed = observe(&server, &path, &[(ATTRIBUTES, "ObjectSize")], Some(&user)).await;
        let expected = if matches!(index, 0 | 3) { (200, 1) } else { (403, 0) };
        assert_eq!((observed.status, observed.count), expected, "IAM row {index}");
    }
    server.shutdown().await;
}

#[test]
fn legacy_get_object_attributes_wire() {
    with_environment(
        vec![(super::ENV_S3_STACK, LEGACY), ("RUSTFS_SSE_C_REQUIRE_TLS", Some("false"))],
        |server| attributes_wire(server, LEGACY),
    );
}

#[test]
fn gateway_get_object_attributes_wire() {
    with_environment(
        vec![(super::ENV_S3_STACK, GATEWAY), ("RUSTFS_SSE_C_REQUIRE_TLS", Some("false"))],
        |server| attributes_wire(server, GATEWAY),
    );
}
