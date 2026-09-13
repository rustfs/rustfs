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

// Integration test demonstrating the embedded RustFS server API.
//
// This test starts a RustFS server in-process and exercises it via the
// standard AWS S3 SDK — exactly as you would in your own integration tests.

#![recursion_limit = "256"]

use aws_sdk_s3::config::{Credentials, Region};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{BucketVersioningStatus, Delete, ObjectAttributes, ObjectIdentifier, VersioningConfiguration};
use aws_sdk_s3::{Client, Config};
use rustfs::embedded::{RustFSServerBuilder, find_available_port};

mod common;

/// Helper: create an S3 client pointed at the embedded server.
fn s3_client(endpoint: &str, access_key: &str, secret_key: &str) -> Client {
    let creds = Credentials::new(access_key, secret_key, None, None, "test");
    let config = Config::builder()
        .credentials_provider(creds)
        .region(Region::new("us-east-1"))
        .endpoint_url(endpoint)
        .force_path_style(true)
        .behavior_version_latest()
        .build();
    Client::from_conf(config)
}

#[test]
fn test_embedded_server_basic_s3_operations() {
    common::run_embedded_test(test_embedded_server_basic_s3_operations_body);
}

async fn test_embedded_server_basic_s3_operations_body() {
    // 1. Pick a free port and start the embedded server.
    let port = match find_available_port() {
        Ok(port) => port,
        Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => return,
        Err(err) => panic!("find free port: {err}"),
    };
    let server = RustFSServerBuilder::new()
        .address(format!("127.0.0.1:{port}"))
        .access_key("testaccesskey")
        .secret_key("testsecretkey")
        .build()
        .await
        .expect("start embedded server");

    let endpoint = server.endpoint();
    assert!(endpoint.contains(&port.to_string()));

    // 2. Create an S3 client and perform basic operations.
    let client = s3_client(&endpoint, server.access_key(), server.secret_key());

    client
        .create_bucket()
        .bucket("test-bucket")
        .send()
        .await
        .expect("create bucket");

    let body = ByteStream::from_static(b"hello rustfs embedded!");
    client
        .put_object()
        .bucket("test-bucket")
        .key("greeting.txt")
        .body(body)
        .send()
        .await
        .expect("put object");

    let resp = client
        .get_object()
        .bucket("test-bucket")
        .key("greeting.txt")
        .send()
        .await
        .expect("get object");

    let data = resp.body.collect().await.expect("read body").into_bytes();
    assert_eq!(data.as_ref(), b"hello rustfs embedded!");

    let list = client
        .list_objects_v2()
        .bucket("test-bucket")
        .send()
        .await
        .expect("list objects");
    assert_eq!(list.key_count(), Some(1));

    client
        .delete_object()
        .bucket("test-bucket")
        .key("greeting.txt")
        .send()
        .await
        .expect("delete object");

    client
        .delete_bucket()
        .bucket("test-bucket")
        .send()
        .await
        .expect("delete bucket");

    server.shutdown().await;
}

// Regression test for issue #6745: on a versioning-suspended bucket, a null
// delete marker's version identity must round-trip as the literal `null`
// through ListObjectVersions, DeleteObject, and DeleteObjects, and removing
// the marker by version id must carry the delete-marker flags.
#[test]
fn test_null_version_delete_marker_round_trip() {
    common::run_embedded_test(test_null_version_delete_marker_round_trip_body);
}

async fn test_null_version_delete_marker_round_trip_body() {
    let port = match find_available_port() {
        Ok(port) => port,
        Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => return,
        Err(err) => panic!("find free port: {err}"),
    };
    let server = RustFSServerBuilder::new()
        .address(format!("127.0.0.1:{port}"))
        .access_key("testaccesskey")
        .secret_key("testsecretkey")
        .build()
        .await
        .expect("start embedded server");
    let client = s3_client(&server.endpoint(), server.access_key(), server.secret_key());

    let bucket = "null-marker-bucket";
    client.create_bucket().bucket(bucket).send().await.expect("create bucket");
    for status in [BucketVersioningStatus::Enabled, BucketVersioningStatus::Suspended] {
        client
            .put_bucket_versioning()
            .bucket(bucket)
            .versioning_configuration(VersioningConfiguration::builder().status(status).build())
            .send()
            .await
            .expect("set bucket versioning state");
    }

    // A versionless DELETE on the suspended bucket mints a null delete marker
    // and must report it as version `null`.
    for key in ["doc/f1.txt", "doc/f2.txt"] {
        client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from_static(b"null-version payload"))
            .send()
            .await
            .expect("put object");
        let deleted = client
            .delete_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .expect("delete object without version id");
        assert_eq!(deleted.delete_marker(), Some(true), "suspended-bucket delete should mint a marker");
        assert_eq!(deleted.version_id(), Some("null"), "the minted marker is the null version");
    }

    // The markers must be listed under the literal `null`, never a nil UUID.
    let listed = client
        .list_object_versions()
        .bucket(bucket)
        .send()
        .await
        .expect("list object versions");
    assert!(listed.versions().is_empty(), "the null versions were replaced by markers");
    let markers = listed.delete_markers();
    assert_eq!(markers.len(), 2);
    for marker in markers {
        assert_eq!(marker.version_id(), Some("null"), "listing must advertise the null version as `null`");
    }

    // Removing one marker by its listed version id over DeleteObject must
    // acknowledge the marker identity on the wire.
    let removed = client
        .delete_object()
        .bucket(bucket)
        .key("doc/f1.txt")
        .version_id("null")
        .send()
        .await
        .expect("delete marker by null version id");
    assert_eq!(
        removed.delete_marker(),
        Some(true),
        "x-amz-delete-marker must be true for a marker removal"
    );
    assert_eq!(removed.version_id(), Some("null"));

    // Removing the other via DeleteObjects must produce an entry a client can
    // correlate with its request: same key, version `null`, marker flags set.
    let delete = Delete::builder()
        .objects(
            ObjectIdentifier::builder()
                .key("doc/f2.txt")
                .version_id("null")
                .build()
                .expect("object identifier"),
        )
        .build()
        .expect("delete payload");
    let batch = client
        .delete_objects()
        .bucket(bucket)
        .delete(delete)
        .send()
        .await
        .expect("delete objects by null version id");
    assert!(batch.errors().is_empty(), "batch delete reported errors: {:?}", batch.errors());
    let entries = batch.deleted();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].key(), Some("doc/f2.txt"));
    assert_eq!(entries[0].version_id(), Some("null"), "the entry must echo the requested identity");
    assert_eq!(entries[0].delete_marker(), Some(true));
    assert_eq!(entries[0].delete_marker_version_id(), Some("null"));

    // With identity round-tripping, one pass leaves the bucket truly empty
    // and deletable.
    let after = client
        .list_object_versions()
        .bucket(bucket)
        .send()
        .await
        .expect("list versions after cleanup");
    assert!(after.versions().is_empty() && after.delete_markers().is_empty());
    client.delete_bucket().bucket(bucket).send().await.expect("delete bucket");

    server.shutdown().await;
}

async fn assert_read_version(
    client: &Client,
    bucket: &str,
    key: &str,
    selector: Option<&str>,
    expected_version: Option<&str>,
    expected_body: &[u8],
) {
    let get = client
        .get_object()
        .bucket(bucket)
        .key(key)
        .set_version_id(selector.map(str::to_owned))
        .send()
        .await
        .expect("get selected version");
    assert_eq!(get.version_id(), expected_version, "GET identity for selector {selector:?}");
    assert_eq!(get.content_length(), Some(expected_body.len() as i64));
    let etag = get.e_tag().expect("GET ETag").to_owned();
    assert_eq!(get.body.collect().await.expect("read selected body").into_bytes().as_ref(), expected_body);

    let head = client
        .head_object()
        .bucket(bucket)
        .key(key)
        .set_version_id(selector.map(str::to_owned))
        .send()
        .await
        .expect("head selected version");
    assert_eq!(head.version_id(), expected_version, "HEAD identity for selector {selector:?}");
    assert_eq!(head.content_length(), Some(expected_body.len() as i64));
    assert_eq!(head.e_tag(), Some(etag.as_str()));

    let attributes = client
        .get_object_attributes()
        .bucket(bucket)
        .key(key)
        .set_version_id(selector.map(str::to_owned))
        .object_attributes(ObjectAttributes::ObjectSize)
        .send()
        .await
        .expect("get selected version attributes");
    assert_eq!(attributes.version_id(), expected_version, "Attributes identity for selector {selector:?}");
    assert_eq!(attributes.object_size(), Some(expected_body.len() as i64));
}

#[test]
fn test_read_version_headers_across_versioning_states() {
    common::run_embedded_test(test_read_version_headers_across_versioning_states_body);
}

async fn test_read_version_headers_across_versioning_states_body() {
    let port = find_available_port().expect("find free port");
    let server = RustFSServerBuilder::new()
        .address(format!("127.0.0.1:{port}"))
        .access_key("testaccesskey")
        .secret_key("testsecretkey")
        .build()
        .await
        .expect("start embedded server");
    let client = s3_client(&server.endpoint(), server.access_key(), server.secret_key());
    let bucket = "read-version-headers";
    let key = "history.txt";
    client.create_bucket().bucket(bucket).send().await.expect("create bucket");

    let unversioned = client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from_static(b"before-versioning"))
        .send()
        .await
        .expect("put unversioned object");
    assert_eq!(unversioned.version_id(), None);
    for selector in [None, Some("null")] {
        assert_read_version(&client, bucket, key, selector, None, b"before-versioning").await;
    }

    client
        .put_bucket_versioning()
        .bucket(bucket)
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await
        .expect("enable versioning");
    for selector in [None, Some("null")] {
        assert_read_version(&client, bucket, key, selector, Some("null"), b"before-versioning").await;
    }

    let mut history = Vec::new();
    for body in [b"enabled-one".as_slice(), b"enabled-second".as_slice()] {
        let put = client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from(body.to_vec()))
            .send()
            .await
            .expect("put enabled version");
        let version = put.version_id().expect("enabled PUT version").to_owned();
        assert!(!uuid::Uuid::parse_str(&version).expect("version UUID").is_nil());
        assert_read_version(&client, bucket, key, None, Some(&version), body).await;
        history.push((version, body));
    }

    client
        .put_bucket_versioning()
        .bucket(bucket)
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Suspended)
                .build(),
        )
        .send()
        .await
        .expect("suspend versioning");
    for (version, body) in &history {
        assert_read_version(&client, bucket, key, Some(version), Some(version), body).await;
    }
    let (latest_version, latest_body) = history.last().expect("latest UUID version");
    assert_read_version(&client, bucket, key, None, Some(latest_version), latest_body).await;
    assert_read_version(&client, bucket, key, Some("null"), Some("null"), b"before-versioning").await;

    for body in [b"null-one".as_slice(), b"null-overwritten".as_slice()] {
        let put = client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from(body.to_vec()))
            .send()
            .await
            .expect("put suspended null version");
        // S3 omits the null identity on PUT, but returns it on subsequent reads.
        assert_eq!(put.version_id(), None, "suspended PUT keeps its write response contract");
        for selector in [None, Some("null")] {
            assert_read_version(&client, bucket, key, selector, Some("null"), body).await;
        }
    }

    client
        .put_bucket_versioning()
        .bucket(bucket)
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await
        .expect("re-enable versioning");
    let enabled_again = client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from_static(b"enabled-again"))
        .send()
        .await
        .expect("put after re-enabling");
    let current = enabled_again.version_id().expect("re-enabled PUT version");
    assert_read_version(&client, bucket, key, None, Some(current), b"enabled-again").await;
    assert_read_version(&client, bucket, key, Some("null"), Some("null"), b"null-overwritten").await;
    for (version, body) in &history {
        assert_read_version(&client, bucket, key, Some(version), Some(version), body).await;
    }
    let listed = client
        .list_object_versions()
        .bucket(bucket)
        .prefix(key)
        .send()
        .await
        .expect("list history");
    let mut versions: Vec<_> = listed
        .versions()
        .iter()
        .map(|version| version.version_id().expect("listed identity"))
        .collect();
    versions.sort_unstable();
    let mut expected = vec!["null", history[0].0.as_str(), history[1].0.as_str(), current];
    expected.sort_unstable();
    assert_eq!(versions, expected, "one null slot and all UUID versions must remain");

    server.shutdown().await;
}
