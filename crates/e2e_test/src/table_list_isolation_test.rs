// Copyright 2026 RustFS Team
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

//! End-to-end table-bucket LIST isolation and cursor-binding regression suite
//! (`rustfs/backlog#2675`).
//!
//! Drives a real `rustfs` server configured with an Iceberg object-backed table
//! catalog across all three S3 listing entry points:
//! - `ListObjectsV2`
//! - `ListObjectVersions`
//! - `ListMultipartUploads`
//!
//! Verifies:
//! 1. Visibility isolation across interleaved allowed and denied tables for
//!    `Contents`, `CommonPrefixes`, `Versions`, `DeleteMarkers`, and `Uploads`
//!    (plus exclusion of reserved catalog metadata keys).
//! 2. Opaque continuation/marker confidentiality (cursors carry the sealed
//!    `rustfs-table-list:v1:` prefix and never leak hidden key names).
//! 3. Cursor-attack rejection (`InvalidArgument`) for tampered cursors,
//!    plaintext/non-opaque continuation tokens, cross-principal replay,
//!    cross-operation replay (`ListObjectsV2` <-> `ListObjectVersions` <->
//!    `ListMultipartUploads`), cross-prefix/delimiter/max-keys replay, and
//!    post-catalog-snapshot-change replay.
//! 4. Bounded forward progress across an all-hidden raw storage page (>1000
//!    hidden objects separating visible tables).

use crate::common::{
    AdminTransport, RustFSTestEnvironment, admin_add_canned_policy_via, admin_attach_user_policy_via, admin_create_user,
    init_logging, signed_request,
};
use aws_sdk_s3::Client;
use aws_sdk_s3::error::{ProvideErrorMetadata, SdkError};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{BucketVersioningStatus, VersioningConfiguration};
use futures::{StreamExt, TryStreamExt, stream};
use http::{Method, StatusCode};
use serde_json::{Value, json};
use std::collections::BTreeSet;
use uuid::Uuid;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

const OPAQUE_CURSOR_PREFIX: &str = "rustfs-table-list:v1:";

async fn set_user_policy(env: &RustFSTestEnvironment, user: &str, policy: Value) -> TestResult {
    admin_add_canned_policy_via(
        AdminTransport::Signed,
        &env.url,
        &env.access_key,
        &env.secret_key,
        user,
        &policy.to_string(),
    )
    .await?;
    admin_attach_user_policy_via(AdminTransport::Signed, &env.url, &env.access_key, &env.secret_key, user, user).await
}

async fn catalog_request(
    env: &RustFSTestEnvironment,
    credentials: &(String, String),
    method: Method,
    path: &str,
    body: Option<Value>,
    expected: StatusCode,
) -> TestResult<Value> {
    let response = signed_request(
        method.clone(),
        &format!("{}{path}", env.url),
        &credentials.0,
        &credentials.1,
        body.map(|value| serde_json::to_vec(&value)).transpose()?,
        Some("application/json"),
    )
    .await?;
    let status = response.status();
    let text = response.text().await?;
    assert_eq!(status, expected, "{method} {path}: {text}");
    if text.is_empty() || !status.is_success() {
        Ok(Value::Null)
    } else {
        Ok(serde_json::from_str(&text)?)
    }
}

fn create_table_payload(name: &str, location: &str) -> Value {
    json!({
        "name": name,
        "location": location,
        "schema": {
            "type": "struct",
            "fields": [{"id": 1, "name": "id", "required": false, "type": "long"}]
        },
        "stage-create": false
    })
}

fn decode_v2_wire_cursor(wire_token: &str, context: &str) -> String {
    let raw = base64_simd::STANDARD
        .decode_to_vec(wire_token.as_bytes())
        .unwrap_or_else(|err| {
            panic!("{context}: ListObjectsV2 continuation token must be valid base64, got {wire_token:?}: {err}")
        });
    String::from_utf8(raw).unwrap_or_else(|err| {
        panic!("{context}: ListObjectsV2 continuation token must decode to UTF-8, got {wire_token:?}: {err}")
    })
}

fn encode_v2_wire_cursor(inner: &str) -> String {
    base64_simd::STANDARD.encode_to_string(inner.as_bytes())
}

fn tamper_opaque_cursor(token: &str) -> String {
    assert!(token.starts_with(OPAQUE_CURSOR_PREFIX), "expected sealed cursor prefix, got {token}");
    let mut bytes = token.as_bytes().to_vec();
    let idx = bytes.len() - 2;
    bytes[idx] = if bytes[idx] == b'A' { b'B' } else { b'A' };
    String::from_utf8(bytes).expect("tampered ASCII base64url cursor must be valid UTF-8")
}

fn tamper_v2_wire_cursor(wire_token: &str, context: &str) -> String {
    let inner = decode_v2_wire_cursor(wire_token, context);
    encode_v2_wire_cursor(&tamper_opaque_cursor(&inner))
}

fn assert_invalid_argument<T, E>(result: Result<T, SdkError<E>>, context: &str)
where
    T: std::fmt::Debug,
    E: ProvideErrorMetadata + std::fmt::Debug,
{
    let err = result.expect_err(&format!("{context}: expected InvalidArgument failure"));
    assert_eq!(
        err.as_service_error().and_then(ProvideErrorMetadata::code),
        Some("InvalidArgument"),
        "{context}: expected InvalidArgument, got {err:?}"
    );
}

fn assert_cursor_confidential(cursor: &str, context: &str) {
    assert!(
        cursor.starts_with(OPAQUE_CURSOR_PREFIX),
        "{context}: cursor must use opaque prefix {OPAQUE_CURSOR_PREFIX}, got {cursor}"
    );
    for forbidden in ["b_hidden", "secret", "metadata.json", "tables/"] {
        assert!(
            !cursor.contains(forbidden),
            "{context}: cursor leaked plaintext fragment {forbidden:?}: {cursor}"
        );
    }
}

fn assert_v2_cursor_confidential(wire_token: &str, context: &str) {
    for forbidden in ["b_hidden", "secret", "metadata.json", "tables/"] {
        assert!(
            !wire_token.contains(forbidden),
            "{context}: wire cursor leaked plaintext fragment {forbidden:?}: {wire_token}"
        );
    }
    let inner = decode_v2_wire_cursor(wire_token, context);
    assert_cursor_confidential(&inner, context);
}

async fn setup_table_bucket(env: &RustFSTestEnvironment, bucket: &str) -> TestResult {
    let root = (env.access_key.clone(), env.secret_key.clone());
    let root_s3 = env.create_s3_client();
    root_s3.create_bucket().bucket(bucket).send().await?;
    root_s3
        .put_bucket_versioning()
        .bucket(bucket)
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await?;

    catalog_request(env, &root, Method::PUT, &format!("/iceberg/v1/buckets/{bucket}"), None, StatusCode::OK).await?;
    catalog_request(
        env,
        &root,
        Method::POST,
        &format!("/iceberg/v1/{bucket}/namespaces"),
        Some(json!({"namespace": ["lab"]})),
        StatusCode::OK,
    )
    .await?;

    let tables_path = format!("/iceberg/v1/{bucket}/namespaces/lab/tables");
    for (name, prefix) in [
        ("a_visible", format!("s3://{bucket}/a_visible")),
        ("b_hidden", format!("s3://{bucket}/b_hidden")),
        ("c_visible", format!("s3://{bucket}/c_visible")),
    ] {
        catalog_request(
            env,
            &root,
            Method::POST,
            &tables_path,
            Some(create_table_payload(name, &prefix)),
            StatusCode::OK,
        )
        .await?;
    }
    Ok(())
}

async fn create_table_reader(env: &RustFSTestEnvironment, bucket: &str, label: &str) -> TestResult<Client> {
    let username = format!("{label}-{}", Uuid::new_v4().simple());
    let secret = Uuid::new_v4().simple().to_string();
    admin_create_user(env, &username, &secret).await?;
    set_user_policy(
        env,
        &username,
        json!({
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": [
                        "s3:ListBucket",
                        "s3:ListBucketVersions",
                        "s3:ListBucketMultipartUploads"
                    ],
                    "Resource": format!("arn:aws:s3:::{bucket}")
                },
                {
                    "Effect": "Allow",
                    "Action": "admin:GetTableMetadata",
                    "Resource": [
                        format!("arn:aws:s3:::{bucket}/namespaces/lab/tables/a_visible"),
                        format!("arn:aws:s3:::{bucket}/namespaces/lab/tables/c_visible")
                    ]
                },
                {
                    "Effect": "Deny",
                    "Action": "admin:GetTableMetadata",
                    "Resource": format!("arn:aws:s3:::{bucket}/namespaces/lab/tables/b_hidden")
                }
            ]
        }),
    )
    .await?;
    Ok(env.create_s3_client_with_credentials(&username, &secret))
}

#[tokio::test]
async fn table_list_isolation_covers_v2_versions_multipart_uploads_and_cursor_attacks() -> TestResult {
    init_logging();
    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server_without_cleanup_with_env(&[("RUSTFS_TABLE_CATALOG_BACKING", "object")])
        .await?;
    env.wait_for_server_ready().await?;

    let bucket = "table-list-isolation-matrix";
    setup_table_bucket(&env, bucket).await?;
    let root_creds = (env.access_key.clone(), env.secret_key.clone());
    let root_s3 = env.create_s3_client();

    let visible_keys = [
        "a_visible/data/01.parquet",
        "a_visible/data/02.parquet",
        "c_visible/data/01.parquet",
        "c_visible/data/02.parquet",
    ];
    let hidden_keys = [
        "b_hidden/secret-01.parquet",
        "b_hidden/secret-02.parquet",
        "b_hidden/secret-03.parquet",
    ];

    let mut expected_visible_versions: BTreeSet<(String, String, bool)> = BTreeSet::new();
    for key in visible_keys {
        let v1 = root_s3
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from_static(b"visible-v1"))
            .send()
            .await?
            .version_id()
            .ok_or("versioned put missing version_id")?
            .to_string();
        expected_visible_versions.insert((key.to_string(), v1, false));
        let v2 = root_s3
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from_static(b"visible-v2"))
            .send()
            .await?
            .version_id()
            .ok_or("versioned put missing version_id")?
            .to_string();
        expected_visible_versions.insert((key.to_string(), v2, false));
    }
    // Add a visible delete marker on a_visible/02 and hidden versions + delete marker on b_hidden.
    let vis_dm = root_s3
        .delete_object()
        .bucket(bucket)
        .key("a_visible/data/02.parquet")
        .send()
        .await?
        .version_id()
        .ok_or("delete marker missing version_id")?
        .to_string();
    expected_visible_versions.insert(("a_visible/data/02.parquet".to_string(), vis_dm, true));

    for key in hidden_keys {
        root_s3
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from_static(b"hidden-v1"))
            .send()
            .await?;
        root_s3
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from_static(b"hidden-v2"))
            .send()
            .await?;
    }
    root_s3
        .delete_object()
        .bucket(bucket)
        .key("b_hidden/secret-02.parquet")
        .send()
        .await?;

    // Seed active multipart uploads in visible AND hidden tables.
    let mut expected_visible_uploads: BTreeSet<(String, String)> = BTreeSet::new();
    for key in [
        "a_visible/uploads/part-a1.parquet",
        "a_visible/uploads/part-a2.parquet",
        "c_visible/uploads/part-c1.parquet",
    ] {
        let upload_id = root_s3
            .create_multipart_upload()
            .bucket(bucket)
            .key(key)
            .send()
            .await?
            .upload_id()
            .ok_or("missing upload_id")?
            .to_string();
        expected_visible_uploads.insert((key.to_string(), upload_id));
    }
    for key in ["b_hidden/uploads/secret-u1.parquet", "b_hidden/uploads/secret-u2.parquet"] {
        root_s3.create_multipart_upload().bucket(bucket).key(key).send().await?;
    }

    let reader_a = create_table_reader(&env, bucket, "table-reader-a").await?;
    let reader_b = create_table_reader(&env, bucket, "table-reader-b").await?;

    // -----------------------------------------------------------------------
    // 1. ListObjectsV2 pagination, delimiter isolation, and cursor capture
    //    (root prefix "" also proves reserved Iceberg catalog objects are hidden)
    // -----------------------------------------------------------------------
    let expected_v2_keys: BTreeSet<String> = [
        "a_visible/data/01.parquet",
        "c_visible/data/01.parquet",
        "c_visible/data/02.parquet",
    ]
    .into_iter()
    .map(str::to_string)
    .collect();

    let mut v2_first_token: Option<String> = None;
    for page_size in [1, 2] {
        let mut seen = BTreeSet::new();
        let mut token: Option<String> = None;
        loop {
            let page = reader_a
                .list_objects_v2()
                .bucket(bucket)
                .max_keys(page_size)
                .set_continuation_token(token.clone())
                .send()
                .await?;
            for obj in page.contents() {
                let key = obj.key().ok_or("object missing key")?.to_string();
                assert!(!key.contains("b_hidden"), "ListObjectsV2 leaked hidden key {key}");
                assert!(seen.insert(key.clone()), "ListObjectsV2 duplicated key {key}");
            }
            if page.is_truncated() == Some(true) {
                let next = page
                    .next_continuation_token()
                    .ok_or("truncated ListObjectsV2 missing next_continuation_token")?
                    .to_string();
                assert_v2_cursor_confidential(&next, "ListObjectsV2");
                if page_size == 1 && v2_first_token.is_none() {
                    v2_first_token = Some(next.clone());
                }
                token = Some(next);
            } else {
                assert!(page.next_continuation_token().is_none());
                break;
            }
        }
        assert_eq!(seen, expected_v2_keys, "ListObjectsV2 max_keys={page_size}");
    }
    let v2_token = v2_first_token.ok_or("expected ListObjectsV2 continuation token")?;

    // Delimiter="/" CommonPrefixes must fold only visible tables, never b_hidden/ or reserved catalog keys.
    let mut v2_prefixes = Vec::new();
    let mut token: Option<String> = None;
    loop {
        let page = reader_a
            .list_objects_v2()
            .bucket(bucket)
            .delimiter("/")
            .max_keys(1)
            .set_continuation_token(token.clone())
            .send()
            .await?;
        assert!(page.contents().is_empty(), "delimiter='/' should fold table keys into CommonPrefixes");
        for cp in page.common_prefixes() {
            let prefix = cp.prefix().ok_or("missing common prefix")?.to_string();
            assert_ne!(prefix, "b_hidden/", "ListObjectsV2 CommonPrefixes leaked hidden table");
            v2_prefixes.push(prefix);
        }
        if page.is_truncated() == Some(true) {
            let next = page
                .next_continuation_token()
                .ok_or("truncated delimiter ListObjectsV2 missing token")?
                .to_string();
            assert_v2_cursor_confidential(&next, "ListObjectsV2 delimiter");
            token = Some(next);
        } else {
            break;
        }
    }
    assert_eq!(v2_prefixes, vec!["a_visible/", "c_visible/"]);

    // -----------------------------------------------------------------------
    // 2. ListObjectVersions pagination, delimiter isolation, and cursor capture
    // -----------------------------------------------------------------------
    let mut versions_first_token: Option<String> = None;
    for page_size in [1, 2] {
        let mut seen = BTreeSet::new();
        let mut key_marker: Option<String> = None;
        let mut version_marker: Option<String> = None;
        loop {
            let page = reader_a
                .list_object_versions()
                .bucket(bucket)
                .max_keys(page_size)
                .set_key_marker(key_marker.clone())
                .set_version_id_marker(version_marker.clone())
                .send()
                .await?;
            for v in page.versions() {
                let key = v.key().ok_or("version missing key")?.to_string();
                let vid = v.version_id().ok_or("version missing version_id")?.to_string();
                assert!(!key.contains("b_hidden"), "ListObjectVersions leaked hidden version {key}");
                assert!(seen.insert((key, vid, false)), "ListObjectVersions duplicated version");
            }
            for dm in page.delete_markers() {
                let key = dm.key().ok_or("delete marker missing key")?.to_string();
                let vid = dm.version_id().ok_or("delete marker missing version_id")?.to_string();
                assert!(!key.contains("b_hidden"), "ListObjectVersions leaked hidden delete marker {key}");
                assert!(seen.insert((key, vid, true)), "ListObjectVersions duplicated delete marker");
            }
            if page.is_truncated() == Some(true) {
                let next_key = page
                    .next_key_marker()
                    .ok_or("truncated ListObjectVersions missing next_key_marker")?
                    .to_string();
                let next_ver = page
                    .next_version_id_marker()
                    .ok_or("truncated ListObjectVersions missing next_version_id_marker")?
                    .to_string();
                assert_eq!(next_key, next_ver, "table ListObjectVersions should pair identical sealed cursor");
                assert_cursor_confidential(&next_key, "ListObjectVersions");
                if page_size == 1 && versions_first_token.is_none() {
                    versions_first_token = Some(next_key.clone());
                }
                key_marker = Some(next_key);
                version_marker = Some(next_ver);
            } else {
                break;
            }
        }
        assert_eq!(seen, expected_visible_versions, "ListObjectVersions max_keys={page_size}");
    }
    let versions_token = versions_first_token.ok_or("expected ListObjectVersions cursor")?;

    let mut version_prefixes = Vec::new();
    let mut key_marker: Option<String> = None;
    let mut version_marker: Option<String> = None;
    loop {
        let page = reader_a
            .list_object_versions()
            .bucket(bucket)
            .delimiter("/")
            .max_keys(1)
            .set_key_marker(key_marker.clone())
            .set_version_id_marker(version_marker.clone())
            .send()
            .await?;
        for cp in page.common_prefixes() {
            let prefix = cp.prefix().ok_or("missing version common prefix")?.to_string();
            assert_ne!(prefix, "b_hidden/", "ListObjectVersions CommonPrefixes leaked hidden table");
            version_prefixes.push(prefix);
        }
        if page.is_truncated() == Some(true) {
            let next_key = page.next_key_marker().ok_or("missing next_key_marker")?.to_string();
            let next_ver = page
                .next_version_id_marker()
                .ok_or("missing next_version_id_marker")?
                .to_string();
            assert_cursor_confidential(&next_key, "ListObjectVersions delimiter");
            key_marker = Some(next_key);
            version_marker = Some(next_ver);
        } else {
            break;
        }
    }
    assert_eq!(version_prefixes, vec!["a_visible/", "c_visible/"]);

    // -----------------------------------------------------------------------
    // 3. ListMultipartUploads pagination, delimiter isolation, and cursor capture
    //    NOTE: Uses empty root prefix so opaque `rustfs-table-list:v1:` key_marker
    //    passes `parse_list_multipart_uploads_params`'s `key_marker.starts_with(prefix)`
    //    check; non-empty prefix pagination with opaque multipart cursors is tracked
    //    as a production follow-up in `rustfs/src/storage/s3_api/multipart.rs`.
    // -----------------------------------------------------------------------
    let mut uploads_first_token: Option<String> = None;
    for page_size in [1, 2] {
        let mut seen = BTreeSet::new();
        let mut key_marker: Option<String> = None;
        let mut upload_marker: Option<String> = None;
        loop {
            let page = reader_a
                .list_multipart_uploads()
                .bucket(bucket)
                .max_uploads(page_size)
                .set_key_marker(key_marker.clone())
                .set_upload_id_marker(upload_marker.clone())
                .send()
                .await?;
            for upload in page.uploads() {
                let key = upload.key().ok_or("upload missing key")?.to_string();
                let uid = upload.upload_id().ok_or("upload missing upload_id")?.to_string();
                assert!(!key.contains("b_hidden"), "ListMultipartUploads leaked hidden upload {key}");
                assert!(seen.insert((key, uid)), "ListMultipartUploads duplicated upload");
            }
            if page.is_truncated() == Some(true) {
                let next_key = page
                    .next_key_marker()
                    .ok_or("truncated ListMultipartUploads missing next_key_marker")?
                    .to_string();
                let next_uid = page
                    .next_upload_id_marker()
                    .ok_or("truncated ListMultipartUploads missing next_upload_id_marker")?
                    .to_string();
                assert_eq!(next_key, next_uid, "table ListMultipartUploads should pair identical sealed cursor");
                assert_cursor_confidential(&next_key, "ListMultipartUploads");
                if page_size == 1 && uploads_first_token.is_none() {
                    uploads_first_token = Some(next_key.clone());
                }
                key_marker = Some(next_key);
                upload_marker = Some(next_uid);
            } else {
                break;
            }
        }
        assert_eq!(seen, expected_visible_uploads, "ListMultipartUploads max_uploads={page_size}");
    }
    let uploads_token = uploads_first_token.ok_or("expected ListMultipartUploads cursor")?;

    let mut upload_prefixes = Vec::new();
    let mut key_marker: Option<String> = None;
    let mut upload_marker: Option<String> = None;
    loop {
        let page = reader_a
            .list_multipart_uploads()
            .bucket(bucket)
            .delimiter("/")
            .max_uploads(1)
            .set_key_marker(key_marker.clone())
            .set_upload_id_marker(upload_marker.clone())
            .send()
            .await?;
        for cp in page.common_prefixes() {
            let prefix = cp.prefix().ok_or("missing upload common prefix")?.to_string();
            assert_ne!(prefix, "b_hidden/", "ListMultipartUploads CommonPrefixes leaked hidden table");
            upload_prefixes.push(prefix);
        }
        if page.is_truncated() == Some(true) {
            let next_key = page.next_key_marker().ok_or("missing next_key_marker")?.to_string();
            let next_uid = page
                .next_upload_id_marker()
                .ok_or("missing next_upload_id_marker")?
                .to_string();
            assert_cursor_confidential(&next_key, "ListMultipartUploads delimiter");
            key_marker = Some(next_key);
            upload_marker = Some(next_uid);
        } else {
            break;
        }
    }
    assert_eq!(upload_prefixes, vec!["a_visible/", "c_visible/"]);

    // -----------------------------------------------------------------------
    // 4. Cursor-attack matrix across all 3 operations
    // -----------------------------------------------------------------------
    // 4a. ListObjectsV2 attacks
    let v2_inner_token = decode_v2_wire_cursor(&v2_token, "ListObjectsV2 captured cursor");
    assert_invalid_argument(
        reader_a
            .list_objects_v2()
            .bucket(bucket)
            .max_keys(1)
            .continuation_token(tamper_v2_wire_cursor(&v2_token, "ListObjectsV2 tamper"))
            .send()
            .await,
        "ListObjectsV2 tampered token",
    );
    assert_invalid_argument(
        reader_a
            .list_objects_v2()
            .bucket(bucket)
            .max_keys(1)
            .continuation_token(encode_v2_wire_cursor("a_visible/data/01.parquet"))
            .send()
            .await,
        "ListObjectsV2 plaintext non-opaque token",
    );
    assert_invalid_argument(
        reader_b
            .list_objects_v2()
            .bucket(bucket)
            .max_keys(1)
            .continuation_token(&v2_token)
            .send()
            .await,
        "ListObjectsV2 cross-principal replay",
    );
    assert_invalid_argument(
        reader_a
            .list_objects_v2()
            .bucket(bucket)
            .prefix("a_visible/")
            .max_keys(1)
            .continuation_token(&v2_token)
            .send()
            .await,
        "ListObjectsV2 cross-prefix replay",
    );
    assert_invalid_argument(
        reader_a
            .list_objects_v2()
            .bucket(bucket)
            .delimiter("/")
            .max_keys(1)
            .continuation_token(&v2_token)
            .send()
            .await,
        "ListObjectsV2 cross-delimiter replay",
    );
    assert_invalid_argument(
        reader_a
            .list_objects_v2()
            .bucket(bucket)
            .max_keys(2)
            .continuation_token(&v2_token)
            .send()
            .await,
        "ListObjectsV2 cross-max-keys replay",
    );
    assert_invalid_argument(
        reader_a
            .list_objects_v2()
            .bucket(bucket)
            .max_keys(1)
            .continuation_token(encode_v2_wire_cursor(&versions_token))
            .send()
            .await,
        "ListObjectsV2 cross-operation replay from ListObjectVersions",
    );

    // 4b. ListObjectVersions attacks
    let tampered_versions = tamper_opaque_cursor(&versions_token);
    assert_invalid_argument(
        reader_a
            .list_object_versions()
            .bucket(bucket)
            .max_keys(1)
            .key_marker(&tampered_versions)
            .version_id_marker(&tampered_versions)
            .send()
            .await,
        "ListObjectVersions tampered cursor",
    );
    assert_invalid_argument(
        reader_a
            .list_object_versions()
            .bucket(bucket)
            .max_keys(1)
            .key_marker(&v2_inner_token)
            .version_id_marker(&v2_inner_token)
            .send()
            .await,
        "ListObjectVersions cross-operation replay from ListObjectsV2",
    );
    assert_invalid_argument(
        reader_b
            .list_object_versions()
            .bucket(bucket)
            .max_keys(1)
            .key_marker(&versions_token)
            .version_id_marker(&versions_token)
            .send()
            .await,
        "ListObjectVersions cross-principal replay",
    );
    assert_invalid_argument(
        reader_a
            .list_object_versions()
            .bucket(bucket)
            .prefix("a_visible/")
            .max_keys(1)
            .key_marker(&versions_token)
            .version_id_marker(&versions_token)
            .send()
            .await,
        "ListObjectVersions cross-prefix replay",
    );
    assert_invalid_argument(
        reader_a
            .list_object_versions()
            .bucket(bucket)
            .delimiter("/")
            .max_keys(1)
            .key_marker(&versions_token)
            .version_id_marker(&versions_token)
            .send()
            .await,
        "ListObjectVersions cross-delimiter replay",
    );
    assert_invalid_argument(
        reader_a
            .list_object_versions()
            .bucket(bucket)
            .max_keys(2)
            .key_marker(&versions_token)
            .version_id_marker(&versions_token)
            .send()
            .await,
        "ListObjectVersions cross-max-keys replay",
    );

    // 4c. ListMultipartUploads attacks
    let tampered_uploads = tamper_opaque_cursor(&uploads_token);
    assert_invalid_argument(
        reader_a
            .list_multipart_uploads()
            .bucket(bucket)
            .max_uploads(1)
            .key_marker(&tampered_uploads)
            .upload_id_marker(&tampered_uploads)
            .send()
            .await,
        "ListMultipartUploads tampered cursor",
    );
    assert_invalid_argument(
        reader_a
            .list_multipart_uploads()
            .bucket(bucket)
            .max_uploads(1)
            .key_marker(&versions_token)
            .upload_id_marker(&versions_token)
            .send()
            .await,
        "ListMultipartUploads cross-operation replay from ListObjectVersions",
    );
    assert_invalid_argument(
        reader_b
            .list_multipart_uploads()
            .bucket(bucket)
            .max_uploads(1)
            .key_marker(&uploads_token)
            .upload_id_marker(&uploads_token)
            .send()
            .await,
        "ListMultipartUploads cross-principal replay",
    );
    assert_invalid_argument(
        reader_a
            .list_multipart_uploads()
            .bucket(bucket)
            .prefix("a_visible/")
            .max_uploads(1)
            .key_marker(&uploads_token)
            .upload_id_marker(&uploads_token)
            .send()
            .await,
        "ListMultipartUploads cross-prefix replay",
    );
    assert_invalid_argument(
        reader_a
            .list_multipart_uploads()
            .bucket(bucket)
            .delimiter("/")
            .max_uploads(1)
            .key_marker(&uploads_token)
            .upload_id_marker(&uploads_token)
            .send()
            .await,
        "ListMultipartUploads cross-delimiter replay",
    );
    assert_invalid_argument(
        reader_a
            .list_multipart_uploads()
            .bucket(bucket)
            .max_uploads(2)
            .key_marker(&uploads_token)
            .upload_id_marker(&uploads_token)
            .send()
            .await,
        "ListMultipartUploads cross-max-uploads replay",
    );

    // 4d. Snapshot-change invalidation: adding a new table changes the catalog
    // snapshot fingerprint and invalidates all previously issued cursors.
    catalog_request(
        &env,
        &root_creds,
        Method::POST,
        &format!("/iceberg/v1/{bucket}/namespaces/lab/tables"),
        Some(create_table_payload("d_added", &format!("s3://{bucket}/d_added"))),
        StatusCode::OK,
    )
    .await?;

    assert_invalid_argument(
        reader_a
            .list_objects_v2()
            .bucket(bucket)
            .max_keys(1)
            .continuation_token(&v2_token)
            .send()
            .await,
        "ListObjectsV2 post-snapshot-change replay",
    );
    assert_invalid_argument(
        reader_a
            .list_object_versions()
            .bucket(bucket)
            .max_keys(1)
            .key_marker(&versions_token)
            .version_id_marker(&versions_token)
            .send()
            .await,
        "ListObjectVersions post-snapshot-change replay",
    );
    assert_invalid_argument(
        reader_a
            .list_multipart_uploads()
            .bucket(bucket)
            .max_uploads(1)
            .key_marker(&uploads_token)
            .upload_id_marker(&uploads_token)
            .send()
            .await,
        "ListMultipartUploads post-snapshot-change replay",
    );

    Ok(())
}

#[tokio::test]
async fn table_list_isolation_advances_across_all_hidden_raw_page_without_leaking() -> TestResult {
    init_logging();
    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server_without_cleanup_with_env(&[("RUSTFS_TABLE_CATALOG_BACKING", "object")])
        .await?;
    env.wait_for_server_ready().await?;

    let bucket = "table-list-sparse-pages";
    setup_table_bucket(&env, bucket).await?;
    let root_s3 = env.create_s3_client();

    root_s3
        .put_object()
        .bucket(bucket)
        .key("a_visible/first.parquet")
        .body(ByteStream::from_static(b"first"))
        .send()
        .await?;
    // Seed 1,050 hidden objects (>1 raw 1000-entry storage page) between visible tables.
    stream::iter(0..1050)
        .map(|idx| {
            let root_s3 = &root_s3;
            async move {
                root_s3
                    .put_object()
                    .bucket(bucket)
                    .key(format!("b_hidden/hidden-{idx:04}.parquet"))
                    .body(ByteStream::from_static(b"h"))
                    .send()
                    .await
                    .map(|_| ())
            }
        })
        .buffer_unordered(32)
        .try_collect::<Vec<_>>()
        .await?;
    root_s3
        .put_object()
        .bucket(bucket)
        .key("c_visible/last.parquet")
        .body(ByteStream::from_static(b"last"))
        .send()
        .await?;

    let reader = create_table_reader(&env, bucket, "sparse-reader").await?;

    // Traverse ListObjectsV2 across the all-hidden gap.
    let mut v2_seen = Vec::new();
    let mut token: Option<String> = None;
    let mut pages = 0usize;
    loop {
        pages += 1;
        assert!(pages <= 16, "sparse ListObjectsV2 pagination must terminate boundedly");
        let page = reader
            .list_objects_v2()
            .bucket(bucket)
            .max_keys(1)
            .set_continuation_token(token.clone())
            .send()
            .await?;
        for obj in page.contents() {
            let key = obj.key().ok_or("missing key")?.to_string();
            assert!(!key.contains("b_hidden"), "sparse ListObjectsV2 leaked {key}");
            v2_seen.push(key);
        }
        if page.is_truncated() == Some(true) {
            let next = page
                .next_continuation_token()
                .ok_or("missing next_continuation_token")?
                .to_string();
            assert_v2_cursor_confidential(&next, "sparse ListObjectsV2");
            assert_ne!(token.as_deref(), Some(next.as_str()), "cursor must advance");
            token = Some(next);
        } else {
            break;
        }
    }
    assert_eq!(v2_seen, vec!["a_visible/first.parquet", "c_visible/last.parquet"]);

    // Traverse ListObjectVersions across the same all-hidden gap.
    let mut versions_seen = Vec::new();
    let mut key_marker: Option<String> = None;
    let mut version_marker: Option<String> = None;
    let mut version_pages = 0usize;
    loop {
        version_pages += 1;
        assert!(version_pages <= 16, "sparse ListObjectVersions pagination must terminate boundedly");
        let page = reader
            .list_object_versions()
            .bucket(bucket)
            .max_keys(1)
            .set_key_marker(key_marker.clone())
            .set_version_id_marker(version_marker.clone())
            .send()
            .await?;
        for v in page.versions() {
            let key = v.key().ok_or("missing key")?.to_string();
            assert!(!key.contains("b_hidden"), "sparse ListObjectVersions leaked {key}");
            versions_seen.push(key);
        }
        if page.is_truncated() == Some(true) {
            let next_key = page.next_key_marker().ok_or("missing next_key_marker")?.to_string();
            let next_ver = page
                .next_version_id_marker()
                .ok_or("missing next_version_id_marker")?
                .to_string();
            assert_cursor_confidential(&next_key, "sparse ListObjectVersions");
            assert_ne!(key_marker.as_deref(), Some(next_key.as_str()), "version cursor must advance");
            key_marker = Some(next_key);
            version_marker = Some(next_ver);
        } else {
            break;
        }
    }
    assert_eq!(versions_seen, vec!["a_visible/first.parquet", "c_visible/last.parquet"]);

    Ok(())
}
