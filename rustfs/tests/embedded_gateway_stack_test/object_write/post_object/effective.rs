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

use super::{WireClient, stack};

fn run_isolated(marker: &str, name: &str) -> bool {
    if std::env::var(marker).as_deref() == Ok("1") {
        return false;
    }
    let output = std::process::Command::new(std::env::current_exe().expect("test executable"))
        .args(["--exact", name])
        .env(marker, "1")
        .env("NO_PROXY", "127.0.0.1,localhost")
        .env("no_proxy", "127.0.0.1,localhost")
        .output()
        .expect("run isolated POST test");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success() && stdout.contains("test result: ok. 1 passed;"),
        "isolated POST test failed:\n{stdout}\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    true
}

#[test]
#[serial_test::serial]
fn gateway_post_effective_conditional_policy_prevents_overwrite() {
    stack::with_stack(stack::GATEWAY, |server| async move {
        const BUCKET: &str = "browser-post-effective-condition";
        const USER: &str = "postconditionwriter";
        stack::create_bucket_in_the_store(BUCKET).await;
        stack::create_user_without_a_policy(USER).await;
        let iam = stack::get_global_app_context().expect("server context").iam().handle();
        let document = serde_json::json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Action": "s3:PutObject",
                "Resource": format!("arn:aws:s3:::{BUCKET}/*"),
                "Condition": {"StringEquals": {"s3:if-none-match": "*"}}
            }]
        });
        let policy = rustfs_policy::policy::Policy::parse_config(document.to_string().as_bytes()).expect("valid policy");
        iam.set_policy(USER, policy).await.expect("install policy");
        iam.policy_db_set(USER, rustfs_iam::store::UserType::Reg, false, USER)
            .await
            .expect("attach policy");
        let endpoint = server.endpoint();
        let root = WireClient {
            endpoint: &endpoint,
            access_key: server.access_key(),
            secret_key: server.secret_key(),
        };
        let caller = WireClient {
            endpoint: &endpoint,
            access_key: USER,
            secret_key: stack::USER_SECRET_KEY,
        };
        assert_eq!(
            root.request("PUT", &format!("/{BUCKET}/existing"), b"original", &[], Some(8))
                .await
                .status,
            200
        );
        let form_only = caller
            .post_form(BUCKET, &[("key", "existing"), ("if-none-match", "*")], &[], true, true)
            .await;
        assert_eq!(form_only.status, 403, "{}", form_only.body);
        assert!(form_only.body.contains("<Code>AccessDenied</Code>"));
        assert_eq!(
            root.request("GET", &format!("/{BUCKET}/existing"), b"", &[], Some(0))
                .await
                .body,
            "original"
        );
        let conditional = [("if-none-match", "*")];
        let create = caller.post_form(BUCKET, &[("key", "new")], &conditional, true, true).await;
        assert_eq!(create.status, 204, "{}", create.body);
        assert_eq!(root.request("GET", &format!("/{BUCKET}/new"), b"", &[], Some(0)).await.body, "hello");
        let overwrite = caller
            .post_form(BUCKET, &[("key", "existing")], &conditional, true, true)
            .await;
        assert_eq!(overwrite.status, 412, "{}", overwrite.body);
        assert!(overwrite.body.contains("<Code>PreconditionFailed</Code>"));
        assert_eq!(
            root.request("GET", &format!("/{BUCKET}/existing"), b"", &[], Some(0))
                .await
                .body,
            "original"
        );
        server.shutdown().await;
    });
}

#[test]
#[serial_test::serial]
fn gateway_post_effective_encryption_policy_observes_wire_fallback() {
    stack::with_environment(
        vec![
            (stack::ENV_S3_STACK, stack::GATEWAY),
            ("RUSTFS_SSE_C_REQUIRE_TLS", Some("false")),
        ],
        |server| async move {
            const BUCKET: &str = "browser-post-effective-ssec";
            const USER: &str = "postssecwriter";
            const ALGORITHM: &str = "x-amz-server-side-encryption-customer-algorithm";
            const KEY: &str = "x-amz-server-side-encryption-customer-key";
            const MD5: &str = "x-amz-server-side-encryption-customer-key-md5";
            stack::create_bucket_in_the_store(BUCKET).await;
            stack::create_user_without_a_policy(USER).await;
            let iam = stack::get_global_app_context().expect("server context").iam().handle();
            let document = serde_json::json!({
                "Version": "2012-10-17",
                "Statement": [
                    {"Effect": "Allow", "Action": "s3:PutObject", "Resource": format!("arn:aws:s3:::{BUCKET}/*")},
                    {"Effect": "Deny", "Action": "s3:PutObject", "Resource": format!("arn:aws:s3:::{BUCKET}/*"),
                     "Condition": {"StringEquals": {"s3:x-amz-server-side-encryption-customer-algorithm": "AES256"}}}
                ]
            });
            let policy = rustfs_policy::policy::Policy::parse_config(document.to_string().as_bytes()).expect("valid policy");
            iam.set_policy(USER, policy).await.expect("install policy");
            iam.policy_db_set(USER, rustfs_iam::store::UserType::Reg, false, USER)
                .await
                .expect("attach policy");
            let endpoint = server.endpoint();
            let root = WireClient {
                endpoint: &endpoint,
                access_key: server.access_key(),
                secret_key: server.secret_key(),
            };
            let caller = WireClient {
                endpoint: &endpoint,
                access_key: USER,
                secret_key: stack::USER_SECRET_KEY,
            };
            let key = base64_simd::STANDARD.encode_to_string([0_u8; 32]);
            let ssec = [(ALGORITHM, "AES256"), (KEY, key.as_str()), (MD5, "cLyPS3KoaSFGi/joRB3OUQ==")];
            let denied = caller.post_form(BUCKET, &[("key", "denied")], &ssec, true, true).await;
            assert_eq!(denied.status, 403, "{}", denied.body);
            assert!(denied.body.contains("<Code>AccessDenied</Code>"));
            assert_eq!(
                root.request("GET", &format!("/{BUCKET}/denied"), b"", &[], Some(0))
                    .await
                    .status,
                404
            );
            let ordinary = caller.post_form(BUCKET, &[("key", "ordinary")], &[], true, true).await;
            assert_eq!(ordinary.status, 204, "{}", ordinary.body);
            let control = root.post_form(BUCKET, &[("key", "encrypted")], &ssec, true, true).await;
            assert_eq!(control.status, 204, "{}", control.body);
            let read = root
                .request("GET", &format!("/{BUCKET}/encrypted"), b"", &ssec, Some(0))
                .await;
            assert_eq!((read.status, read.body.as_str()), (200, "hello"));
            server.shutdown().await;
        },
    );
}

#[test]
#[serial_test::serial]
fn gateway_post_stores_the_authorized_form_metadata_and_bucket_retention() {
    stack::with_stack(stack::GATEWAY, |server| async move {
        use aws_sdk_s3::types::{
            DefaultRetention, ObjectLockConfiguration, ObjectLockEnabled, ObjectLockRetentionMode, ObjectLockRule,
        };
        use rustfs_storage_api::ObjectOperations as _;

        const BUCKET: &str = "browser-post-effective-metadata";
        const USER: &str = "postmetadatawriter";
        let endpoint = server.endpoint();
        let sdk = stack::s3_client(&endpoint, server.access_key(), server.secret_key());
        sdk.create_bucket()
            .bucket(BUCKET)
            .object_lock_enabled_for_bucket(true)
            .send()
            .await
            .expect("create object-lock bucket");
        let retention = DefaultRetention::builder()
            .mode(ObjectLockRetentionMode::Governance)
            .days(1)
            .build();
        let config = ObjectLockConfiguration::builder()
            .object_lock_enabled(ObjectLockEnabled::Enabled)
            .rule(ObjectLockRule::builder().default_retention(retention).build())
            .build();
        sdk.put_object_lock_configuration()
            .bucket(BUCKET)
            .object_lock_configuration(config)
            .send()
            .await
            .expect("configure default retention");
        stack::create_user_without_a_policy(USER).await;
        let iam = stack::get_global_app_context().expect("server context").iam().handle();
        let document = serde_json::json!({
            "Version": "2012-10-17",
            "Statement": [
                {"Effect": "Allow", "Action": "s3:PutObject", "Resource": format!("arn:aws:s3:::{BUCKET}/*"),
                 "Condition": {"StringEquals": {"s3:x-amz-storage-class": "STANDARD"}}},
                {"Effect": "Deny", "Action": "s3:PutObject", "Resource": format!("arn:aws:s3:::{BUCKET}/*"),
                 "Condition": {"StringEquals": {"s3:RequestObjectTag/classification": "private"}}}
            ]
        });
        let policy = rustfs_policy::policy::Policy::parse_config(document.to_string().as_bytes()).expect("valid policy");
        iam.set_policy(USER, policy).await.expect("install policy");
        iam.policy_db_set(USER, rustfs_iam::store::UserType::Reg, false, USER)
            .await
            .expect("attach policy");
        let caller = WireClient {
            endpoint: &endpoint,
            access_key: USER,
            secret_key: stack::USER_SECRET_KEY,
        };
        let rejected_wire_property = caller
            .post_form(
                BUCKET,
                &[("key", "wire-property"), ("x-amz-storage-class", "STANDARD")],
                &[("x-amz-meta-purpose", "wire")],
                true,
                true,
            )
            .await;
        assert_eq!(rejected_wire_property.status, 403, "{}", rejected_wire_property.body);
        assert!(
            sdk.head_object().bucket(BUCKET).key("wire-property").send().await.is_err(),
            "a POST object property supplied outside the form must not publish an object"
        );
        let answer = caller
            .post_form(
                BUCKET,
                &[
                    ("key", "protected"),
                    ("x-amz-storage-class", "STANDARD"),
                    ("x-amz-tagging", "classification=public"),
                    ("x-amz-meta-purpose", "form"),
                ],
                &[
                    ("x-amz-tagging", "classification=private"),
                    ("x-amz-object-lock-mode", "GOVERNANCE"),
                    ("x-amz-object-lock-retain-until-date", "2000-01-01T00:00:00Z"),
                ],
                true,
                true,
            )
            .await;
        assert_eq!(answer.status, 204, "{}", answer.body);
        let tags = sdk
            .get_object_tagging()
            .bucket(BUCKET)
            .key("protected")
            .send()
            .await
            .expect("read tags");
        assert!(
            tags.tag_set()
                .iter()
                .any(|tag| tag.key() == "classification" && tag.value() == "public"),
            "stored tags must match the authorized form"
        );
        let lock = sdk
            .get_object_retention()
            .bucket(BUCKET)
            .key("protected")
            .send()
            .await
            .expect("read retention");
        let lock = lock.retention().expect("bucket default retention");
        assert_eq!(lock.mode().map(|mode| mode.as_str()), Some("GOVERNANCE"));
        assert!(
            lock.retain_until_date().expect("retention date").secs() > time::OffsetDateTime::now_utc().unix_timestamp(),
            "the wire header must not replace the bucket default"
        );
        let head = sdk
            .head_object()
            .bucket(BUCKET)
            .key("protected")
            .send()
            .await
            .expect("read metadata");
        assert_eq!(
            head.metadata()
                .and_then(|metadata| metadata.get("purpose"))
                .map(String::as_str),
            Some("form")
        );
        let store = stack::get_global_app_context().expect("server context").object_store();
        let info = store
            .get_object_info(BUCKET, "protected", &rustfs_ecstore::api::object::ObjectOptions::default())
            .await
            .expect("read stored metadata");
        assert_eq!(info.storage_class.as_deref(), Some("STANDARD"));
        let denied = caller
            .post_form(
                BUCKET,
                &[
                    ("key", "unpermitted-retention"),
                    ("x-amz-storage-class", "STANDARD"),
                    ("x-amz-object-lock-mode", "COMPLIANCE"),
                    ("x-amz-object-lock-retain-until-date", "2099-01-01T00:00:00Z"),
                ],
                &[],
                true,
                true,
            )
            .await;
        assert_eq!(denied.status, 403, "{}", denied.body);
        assert!(denied.body.contains("<Code>AccessDenied</Code>"), "{}", denied.body);
        assert!(
            sdk.head_object()
                .bucket(BUCKET)
                .key("unpermitted-retention")
                .send()
                .await
                .is_err()
        );
        server.shutdown().await;
    });
}

#[test]
#[serial_test::serial]
fn gateway_post_form_checksum_validates_before_publication() {
    stack::with_stack(stack::GATEWAY, |server| async move {
        const BUCKET: &str = "browser-post-checksum";
        stack::create_bucket_in_the_store(BUCKET).await;
        let endpoint = server.endpoint();
        let root = WireClient {
            endpoint: &endpoint,
            access_key: server.access_key(),
            secret_key: server.secret_key(),
        };
        for known in [true, false] {
            let good_key = if known { "known-good" } else { "chunked-good" };
            let answer = root
                .post_form(BUCKET, &[("key", good_key), ("x-amz-checksum-crc32", "NhCmhg==")], &[], known, true)
                .await;
            assert_eq!(answer.status, 204, "{good_key}: {}", answer.body);
            assert_eq!(
                root.request("GET", &format!("/{BUCKET}/{good_key}"), b"", &[], Some(0))
                    .await
                    .body,
                "hello"
            );

            let bad_key = if known { "known-bad" } else { "chunked-bad" };
            let answer = root
                .post_form(BUCKET, &[("key", bad_key), ("x-amz-checksum-crc32", "AAAAAA==")], &[], known, true)
                .await;
            assert_eq!(answer.status, 400, "{bad_key}: {}", answer.body);
            assert!(answer.body.contains("<Code>BadDigest</Code>"), "{}", answer.body);
            assert_eq!(
                root.request("GET", &format!("/{BUCKET}/{bad_key}"), b"", &[], Some(0))
                    .await
                    .status,
                404
            );
        }
        assert_eq!(
            root.request("PUT", &format!("/{BUCKET}/existing"), b"original", &[], Some(8))
                .await
                .status,
            200
        );
        let answer = root
            .post_form(BUCKET, &[("key", "existing"), ("x-amz-checksum-crc32", "AAAAAA==")], &[], true, true)
            .await;
        assert_eq!(answer.status, 400, "{}", answer.body);
        assert!(answer.body.contains("<Code>BadDigest</Code>"), "{}", answer.body);
        assert_eq!(
            root.request("GET", &format!("/{BUCKET}/existing"), b"", &[], Some(0))
                .await
                .body,
            "original"
        );
        for (key, fields, headers) in [
            ("empty", vec![("x-amz-checksum-crc32", "")], vec![]),
            ("malformed", vec![("x-amz-checksum-crc32", "not-base64")], vec![]),
            ("short", vec![("x-amz-checksum-crc32", "AAAA")], vec![]),
            (
                "multiple",
                vec![("x-amz-checksum-crc32", "NhCmhg=="), ("x-amz-checksum-sha256", "bad")],
                vec![],
            ),
            (
                "algorithm-mismatch",
                vec![
                    ("x-amz-checksum-crc32", "NhCmhg=="),
                    ("x-amz-sdk-checksum-algorithm", "SHA256"),
                ],
                vec![],
            ),
            (
                "header-mismatch",
                vec![("x-amz-checksum-crc32", "NhCmhg==")],
                vec![("x-amz-checksum-crc32", "AAAAAA==")],
            ),
            (
                "trailer-conflict",
                vec![("x-amz-checksum-crc32", "NhCmhg==")],
                vec![("x-amz-trailer", "x-amz-checksum-crc32")],
            ),
            (
                "header-algorithm-mismatch",
                vec![],
                vec![("x-amz-checksum-crc32", "AAAAAA=="), ("x-amz-checksum-algorithm", "SHA256")],
            ),
            ("sdk-algorithm-only", vec![], vec![("x-amz-sdk-checksum-algorithm", "CRC32")]),
            ("multipart-suffix", vec![("x-amz-checksum-crc32", "NhCmhg==-2")], vec![]),
            (
                "extract-checksum",
                vec![
                    ("x-amz-checksum-crc32", "NhCmhg=="),
                    ("x-amz-meta-snowball-auto-extract", "true"),
                ],
                vec![],
            ),
        ] {
            let mut form = vec![("key", key)];
            form.extend(fields);
            let answer = root.post_form(BUCKET, &form, &headers, true, true).await;
            assert_eq!(answer.status, 400, "{key}: {}", answer.body);
            assert!(answer.body.contains("<Code>InvalidRequest</Code>"), "{key}: {}", answer.body);
            assert_eq!(
                root.request("GET", &format!("/{BUCKET}/{key}"), b"", &[], Some(0))
                    .await
                    .status,
                404
            );
        }
        let answer = root
            .post_form(
                BUCKET,
                &[("key", "matching-header"), ("x-amz-checksum-crc32", "NhCmhg==")],
                &[("x-amz-checksum-crc32", "NhCmhg==")],
                true,
                true,
            )
            .await;
        assert_eq!(answer.status, 204, "{}", answer.body);
        let answer = root
            .post_form(
                BUCKET,
                &[("key", "http-trailer"), ("x-amz-checksum-crc32", "NhCmhg==")],
                &[("trailer", "x-amz-checksum-crc32")],
                false,
                true,
            )
            .await;
        assert_eq!(answer.status, 400, "{}", answer.body);
        assert!(answer.body.contains("<Code>InvalidRequest</Code>"), "{}", answer.body);
        assert_eq!(
            root.request("GET", &format!("/{BUCKET}/http-trailer"), b"", &[], Some(0))
                .await
                .status,
            404
        );
        let answer = root
            .post_form(
                BUCKET,
                &[("key", "extract-unsupported"), ("x-amz-meta-snowball-auto-extract", "true")],
                &[],
                true,
                true,
            )
            .await;
        assert_eq!(answer.status, 501, "{}", answer.body);
        assert!(answer.body.contains("<Code>NotImplemented</Code>"), "{}", answer.body);
        assert_eq!(
            root.request("GET", &format!("/{BUCKET}/extract-unsupported"), b"", &[], Some(0))
                .await
                .status,
            404
        );
        server.shutdown().await;
    });
}

#[test]
#[serial_test::serial]
fn gateway_post_checks_expected_bucket_owner_in_form() {
    stack::with_stack(stack::GATEWAY, |server| async move {
        const BUCKET: &str = "browser-post-expected-owner";
        stack::create_bucket_in_the_store(BUCKET).await;
        let endpoint = server.endpoint();
        let sdk = stack::s3_client(&endpoint, server.access_key(), server.secret_key());
        let owner = sdk
            .get_bucket_acl()
            .bucket(BUCKET)
            .send()
            .await
            .expect("read bucket owner")
            .owner()
            .and_then(|owner| owner.id())
            .expect("bucket owner ID")
            .to_owned();
        let root = WireClient {
            endpoint: &endpoint,
            access_key: server.access_key(),
            secret_key: server.secret_key(),
        };
        let valid = root
            .post_form(BUCKET, &[("key", "matched"), ("x-amz-expected-bucket-owner", &owner)], &[], true, true)
            .await;
        assert_eq!(valid.status, 204, "{}", valid.body);
        let invalid = root
            .post_form(
                BUCKET,
                &[("key", "denied"), ("x-amz-expected-bucket-owner", "different-owner")],
                &[("x-amz-expected-bucket-owner", &owner)],
                false,
                true,
            )
            .await;
        assert_eq!(invalid.status, 403, "{}", invalid.body);
        assert!(invalid.body.contains("<Code>AccessDenied</Code>"), "{}", invalid.body);
        assert_eq!(
            root.request("GET", &format!("/{BUCKET}/denied"), b"", &[], Some(0))
                .await
                .status,
            404
        );
        assert_eq!(
            root.request("PUT", &format!("/{BUCKET}/existing"), b"original", &[], Some(8))
                .await
                .status,
            200
        );
        let invalid = root
            .post_form(
                BUCKET,
                &[("key", "existing"), ("x-amz-expected-bucket-owner", "different-owner")],
                &[],
                true,
                true,
            )
            .await;
        assert_eq!(invalid.status, 403, "{}", invalid.body);
        assert_eq!(
            root.request("GET", &format!("/{BUCKET}/existing"), b"", &[], Some(0))
                .await
                .body,
            "original"
        );
        let omitted = root.post_form(BUCKET, &[("key", "omitted")], &[], true, true).await;
        assert_eq!(omitted.status, 204, "{}", omitted.body);
        server.shutdown().await;
    });
}

#[test]
#[serial_test::serial]
fn gateway_post_empty_file_checks_checksum_before_publication() {
    stack::with_stack(stack::GATEWAY, empty_post_refuses_before_publication);
}

#[test]
#[serial_test::serial]
fn legacy_post_empty_file_refuses_before_publication() {
    stack::with_stack(stack::LEGACY, empty_post_refuses_before_publication);
}

async fn empty_post_refuses_before_publication(server: rustfs::embedded::RustFSServer) {
    // Legacy rejects an empty POST before checksum validation; non-empty checksum
    // success and failure are exercised by gateway_post_form_checksum_validates_before_publication.
    const BUCKET: &str = "browser-post-empty-checksum";
    stack::create_bucket_in_the_store(BUCKET).await;
    let endpoint = server.endpoint();
    let root = WireClient {
        endpoint: &endpoint,
        access_key: server.access_key(),
        secret_key: server.secret_key(),
    };
    for known in [true, false] {
        for (suffix, checksum) in [("valid", "AAAAAA=="), ("invalid", "NhCmhg==")] {
            let key = format!("{}-{suffix}", if known { "known" } else { "chunked" });
            let answer = root
                .post_form_contents(BUCKET, &[("key", &key), ("x-amz-checksum-crc32", checksum)], &[], "", known, true)
                .await;
            assert_eq!(answer.status, 400, "{key}: {}", answer.body);
            assert!(answer.body.contains("<Code>UnexpectedContent</Code>"), "{key}: {}", answer.body);
            assert_eq!(
                root.request("GET", &format!("/{BUCKET}/{key}"), b"", &[], Some(0))
                    .await
                    .status,
                404
            );
        }
    }
    server.shutdown().await;
}

#[test]
#[serial_test::serial]
fn gateway_post_compressed_form_checksum_is_verified() {
    if run_isolated(
        "RUSTFS_TEST_POST_COMPRESSED_CHECKSUM_CHILD",
        "object_write::post_object::effective::gateway_post_compressed_form_checksum_is_verified",
    ) {
        return;
    }
    stack::with_environment(
        vec![
            (stack::ENV_S3_STACK, stack::GATEWAY),
            ("RUSTFS_COMPRESSION_ENABLED", Some("true")),
        ],
        |server| async move {
            use rustfs_storage_api::ObjectOperations as _;

            const BUCKET: &str = "browser-post-compressed-checksum";
            stack::create_bucket_in_the_store(BUCKET).await;
            let endpoint = server.endpoint();
            let root = WireClient {
                endpoint: &endpoint,
                access_key: server.access_key(),
                secret_key: server.secret_key(),
            };
            let payload = "x".repeat(8192);
            let valid = root
                .post_form_contents(
                    BUCKET,
                    &[("key", "valid.txt"), ("x-amz-checksum-crc32", "EjkFxQ==")],
                    &[],
                    &payload,
                    true,
                    true,
                )
                .await;
            assert_eq!(valid.status, 204, "{}", valid.body);
            let store = stack::get_global_app_context().expect("server context").object_store();
            let info = store
                .get_object_info(BUCKET, "valid.txt", &rustfs_ecstore::api::object::ObjectOptions::default())
                .await
                .expect("stored object");
            assert!(info.is_compressed(), "the control must exercise the compressed write path");
            assert_eq!(
                root.request("GET", &format!("/{BUCKET}/valid.txt"), b"", &[], Some(0))
                    .await
                    .body,
                payload
            );
            let invalid = root
                .post_form_contents(
                    BUCKET,
                    &[("key", "invalid.txt"), ("x-amz-checksum-crc32", "AAAAAA==")],
                    &[],
                    &payload,
                    true,
                    true,
                )
                .await;
            assert_eq!(invalid.status, 400, "{}", invalid.body);
            assert!(invalid.body.contains("<Code>BadDigest</Code>"), "{}", invalid.body);
            assert_eq!(
                root.request("GET", &format!("/{BUCKET}/invalid.txt"), b"", &[], Some(0))
                    .await
                    .status,
                404
            );
            let archive = root
                .post_form_contents(
                    BUCKET,
                    &[
                        ("key", "archive.txt"),
                        ("content-type", "application/zip; charset=binary"),
                        ("x-amz-checksum-crc32", "EjkFxQ=="),
                    ],
                    &[],
                    &payload,
                    true,
                    true,
                )
                .await;
            assert_eq!(archive.status, 204, "{}", archive.body);
            let info = store
                .get_object_info(BUCKET, "archive.txt", &rustfs_ecstore::api::object::ObjectOptions::default())
                .await
                .expect("stored archive object");
            assert!(!info.is_compressed(), "archive MIME parameters must not bypass the compression exclusion");
            let read = root
                .request("GET", &format!("/{BUCKET}/archive.txt"), b"", &[], Some(0))
                .await;
            assert_eq!(read.body, payload);
            assert_eq!(
                read.headers.get("content-type").map(String::as_str),
                Some("application/zip; charset=binary")
            );
            server.shutdown().await;
        },
    );
}

#[test]
#[serial_test::serial]
fn gateway_post_archive_encoding_uses_form_content_fields() {
    if run_isolated(
        "RUSTFS_TEST_POST_ARCHIVE_ENCODING_CHILD",
        "object_write::post_object::effective::gateway_post_archive_encoding_uses_form_content_fields",
    ) {
        return;
    }
    stack::with_environment(
        vec![
            (stack::ENV_S3_STACK, stack::GATEWAY),
            ("RUSTFS_REJECT_ARCHIVE_CONTENT_ENCODING", Some("true")),
        ],
        |server| async move {
            const BUCKET: &str = "browser-post-archive-encoding";
            stack::create_bucket_in_the_store(BUCKET).await;
            let endpoint = server.endpoint();
            let root = WireClient {
                endpoint: &endpoint,
                access_key: server.access_key(),
                secret_key: server.secret_key(),
            };
            for fields in [
                vec![("key", "blocked.zip"), ("content-encoding", "gzip")],
                vec![
                    ("key", "blocked-type"),
                    ("content-type", "application/zip"),
                    ("content-encoding", "gzip"),
                ],
                vec![("key", "blocked-utf8.zip"), ("content-encoding", "gzip, é")],
            ] {
                let key = fields[0].1;
                let answer = root.post_form(BUCKET, &fields, &[], true, true).await;
                assert_eq!(answer.status, 400, "{key}: {}", answer.body);
                assert!(answer.body.contains("<Code>InvalidArgument</Code>"), "{key}: {}", answer.body);
                assert_eq!(
                    root.request("GET", &format!("/{BUCKET}/{key}"), b"", &[], Some(0))
                        .await
                        .status,
                    404
                );
            }
            let answer = root
                .post_form(BUCKET, &[("key", "wire-only.zip")], &[("content-encoding", "gzip")], true, true)
                .await;
            assert_eq!(answer.status, 204, "{}", answer.body);
            let answer = root
                .post_form(
                    BUCKET,
                    &[
                        ("key", "ordinary"),
                        ("content-type", "text/plain"),
                        ("content-encoding", "gzip"),
                    ],
                    &[],
                    true,
                    true,
                )
                .await;
            assert_eq!(answer.status, 204, "{}", answer.body);
            let answer = root
                .post_form(
                    BUCKET,
                    &[("key", "quoted-utf8"), ("content-type", "text/plain; name=\"café\"")],
                    &[],
                    true,
                    true,
                )
                .await;
            assert_eq!(answer.status, 204, "{}", answer.body);
            let read = root
                .request("GET", &format!("/{BUCKET}/quoted-utf8"), b"", &[], Some(0))
                .await;
            assert_eq!(read.headers.get("content-type").map(String::as_str), Some("text/plain; name=\"café\""));
            server.shutdown().await;
        },
    );
}

#[test]
#[serial_test::serial]
fn gateway_post_preserves_parameterized_compression_rules() {
    if run_isolated(
        "RUSTFS_TEST_POST_COMPRESSION_PATTERN_CHILD",
        "object_write::post_object::effective::gateway_post_preserves_parameterized_compression_rules",
    ) {
        return;
    }
    stack::with_environment(
        vec![
            (stack::ENV_S3_STACK, stack::GATEWAY),
            ("RUSTFS_COMPRESSION_ENABLED", Some("true")),
            ("RUSTFS_COMPRESSION_EXTENSIONS", Some("")),
            ("RUSTFS_COMPRESSION_MIME_TYPES", Some("text/plain; charset=utf-8")),
        ],
        |server| async move {
            use rustfs_storage_api::ObjectOperations as _;

            const BUCKET: &str = "browser-post-compression-pattern";
            stack::create_bucket_in_the_store(BUCKET).await;
            let endpoint = server.endpoint();
            let root = WireClient {
                endpoint: &endpoint,
                access_key: server.access_key(),
                secret_key: server.secret_key(),
            };
            let store = stack::get_global_app_context().expect("server context").object_store();
            let payload = "x".repeat(8192);
            for (key, content_type, compressed) in [("custom", "text/plain; charset=utf-8", true), ("plain", "text/plain", false)]
            {
                let answer = root
                    .post_form_contents(
                        BUCKET,
                        &[
                            ("key", key),
                            ("content-type", content_type),
                            ("x-amz-checksum-crc32", "EjkFxQ=="),
                        ],
                        &[],
                        &payload,
                        true,
                        true,
                    )
                    .await;
                assert_eq!(answer.status, 204, "{key}: {}", answer.body);
                let info = store
                    .get_object_info(BUCKET, key, &rustfs_ecstore::api::object::ObjectOptions::default())
                    .await
                    .expect("stored object");
                assert_eq!(info.is_compressed(), compressed, "{key}");
                let read = root.request("GET", &format!("/{BUCKET}/{key}"), b"", &[], Some(0)).await;
                assert_eq!(read.body, payload, "{key}");
                assert_eq!(read.headers.get("content-type").map(String::as_str), Some(content_type), "{key}");
            }
            server.shutdown().await;
        },
    );
}
