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

//! Refusal-matrix rows and listing behaviour of migration batch B2 (rustfs/backlog#2766), on both
//! stacks: object listings, multipart listings, and multipart create and abort.

use super::{GATEWAY, LEGACY, MatrixRow, Outcome, create_bucket_in_the_store, outcome, refusal, s3_client, with_stack};
use aws_sdk_s3::Client;
use aws_sdk_s3::types::EncodingType;
use futures::future::BoxFuture;
use rustfs::app::context::get_global_app_context;
use rustfs::embedded::RustFSServer;
use rustfs_ecstore::api::object::{ObjectOptions, PutObjReader};
use rustfs_s3_ops::S3Operation;
use rustfs_storage_api::{MultipartOperations as _, ObjectIO as _};

pub(crate) const MATRIX: &[MatrixRow] = &[
    MatrixRow {
        operation: "ListObjectsV2",
        metric: S3Operation::ListObjectsV2,
        has_error_body: true,
        call: list_objects_v2,
    },
    MatrixRow {
        operation: "ListObjects",
        metric: S3Operation::ListObjects,
        has_error_body: true,
        call: list_objects,
    },
    MatrixRow {
        operation: "ListObjectVersions",
        metric: S3Operation::ListObjectVersions,
        has_error_body: true,
        call: list_object_versions,
    },
    MatrixRow {
        operation: "ListMultipartUploads",
        metric: S3Operation::ListMultipartUploads,
        has_error_body: true,
        call: list_multipart_uploads,
    },
];

fn list_objects_v2<'a>(client: &'a Client, bucket: &'a str) -> BoxFuture<'a, Outcome> {
    Box::pin(async move { outcome(client.list_objects_v2().bucket(bucket).send().await) })
}

fn list_objects<'a>(client: &'a Client, bucket: &'a str) -> BoxFuture<'a, Outcome> {
    Box::pin(async move { outcome(client.list_objects().bucket(bucket).send().await) })
}

fn list_object_versions<'a>(client: &'a Client, bucket: &'a str) -> BoxFuture<'a, Outcome> {
    Box::pin(async move { outcome(client.list_object_versions().bucket(bucket).send().await) })
}

fn list_multipart_uploads<'a>(client: &'a Client, bucket: &'a str) -> BoxFuture<'a, Outcome> {
    Box::pin(async move { outcome(client.list_multipart_uploads().bucket(bucket).send().await) })
}

const LISTED_BUCKET: &str = "gateway-stack-listing";

/// Stored keys, among them one at the length limit and ones that need encoding or escaping.
fn stored_keys() -> [String; 5] {
    [
        "dir a/file+b%.txt".to_owned(),
        "markup &<>\"'".to_owned(),
        "tab\tkey".to_owned(),
        // 1024 bytes in segments the store can hold as file names.
        format!("{}{}", format!("{}/", "k".repeat(200)).repeat(5), "z".repeat(19)),
        "~tilde".to_owned(),
    ]
}

/// The same encoding legacy RustFS writes: every byte outside `A-Z a-z 0-9 - . _ ~` per segment,
/// `/` literal, uppercase hex. Written here independently of either stack.
fn url_encoded(value: &str) -> String {
    value
        .split('/')
        .map(|segment| {
            segment
                .bytes()
                .map(|byte| match byte {
                    b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => char::from(byte).to_string(),
                    _ => format!("%{byte:02X}"),
                })
                .collect::<String>()
        })
        .collect::<Vec<_>>()
        .join("/")
}

async fn put_in_the_store(bucket: &str, key: &str, etag: Option<&str>) {
    let options = ObjectOptions {
        preserve_etag: etag.map(str::to_owned),
        ..Default::default()
    };
    get_global_app_context()
        .expect("embedded server context")
        .object_store()
        .put_object(bucket, key, &mut PutObjReader::from_vec(b"payload".to_vec()), &options)
        .await
        .expect("put object in the store");
}

/// A listing names every stored key as stored, percent-encodes the keys (and ListObjects' marker)
/// only for `encoding-type=url`, pages with the cursor it hands out, and refuses what legacy RustFS
/// refuses with the same code and status.
async fn the_listing_names_every_stored_key(server: RustFSServer) {
    create_bucket_in_the_store(LISTED_BUCKET).await;
    let mut keys = Vec::new();
    for key in stored_keys() {
        if cfg!(target_os = "macos") && key.len() == 1024 {
            // macOS limits the complete disk path to 1016 bytes, below this S3 key boundary.
            let error = get_global_app_context()
                .expect("embedded server context")
                .object_store()
                .put_object(
                    LISTED_BUCKET,
                    &key,
                    &mut PutObjReader::from_vec(b"payload".to_vec()),
                    &ObjectOptions::default(),
                )
                .await
                .expect_err("macOS cannot establish the 1024-byte stored-key fixture");
            assert!(matches!(error, rustfs_ecstore::api::error::StorageError::FileNameTooLong), "{error:?}");
            eprintln!("1024-byte stored-key row unavailable on macOS: full disk paths are limited to 1016 bytes");
        } else {
            put_in_the_store(LISTED_BUCKET, &key, None).await;
            keys.push(key);
        }
    }
    keys.sort_unstable();
    let client = s3_client(&server.endpoint(), server.access_key(), server.secret_key());

    for (encoding, expected) in [
        (None, keys.to_vec()),
        (Some(EncodingType::Url), keys.iter().map(|key| url_encoded(key)).collect()),
    ] {
        let listing = client
            .list_objects_v2()
            .bucket(LISTED_BUCKET)
            .set_encoding_type(encoding.clone())
            .send()
            .await
            .expect("ListObjectsV2");
        let listed: Vec<&str> = listing.contents().iter().filter_map(|object| object.key()).collect();
        assert_eq!(listed, expected, "{encoding:?}");
        assert_eq!(listing.key_count(), Some(i32::try_from(keys.len()).unwrap()));
        if cfg!(target_os = "macos") {
            assert!(listed.iter().all(|key| key.len() < 1024), "the refused fixture must not be stored");
        }
        assert_eq!(listing.encoding_type(), encoding.as_ref());

        let first = client
            .list_objects()
            .bucket(LISTED_BUCKET)
            .set_encoding_type(encoding.clone())
            .max_keys(1)
            .send()
            .await
            .expect("ListObjects");
        assert_eq!(first.is_truncated(), Some(true));
        assert_eq!(first.next_marker(), Some(expected[0].as_str()), "{encoding:?}");

        let versions = client
            .list_object_versions()
            .bucket(LISTED_BUCKET)
            .set_encoding_type(encoding.clone())
            .send()
            .await
            .expect("ListObjectVersions");
        let listed: Vec<&str> = versions.versions().iter().filter_map(|version| version.key()).collect();
        assert_eq!(listed, expected, "{encoding:?}");
        assert_eq!(versions.encoding_type(), encoding.as_ref());
    }

    // Pagination carries the cursor: dropping it would answer the first page again.
    let mut paged = Vec::new();
    let mut token = None;
    loop {
        let page = client
            .list_objects_v2()
            .bucket(LISTED_BUCKET)
            .max_keys(2)
            .set_continuation_token(token)
            .send()
            .await
            .expect("a page of ListObjectsV2");
        paged.extend(page.contents().iter().filter_map(|object| object.key().map(str::to_owned)));
        token = page.next_continuation_token().map(str::to_owned);
        if token.is_none() {
            break;
        }
    }
    assert_eq!(paged, keys);

    let missing = client
        .list_objects_v2()
        .bucket("gateway-stack-missing")
        .send()
        .await
        .expect_err("no bucket");
    assert_eq!(refusal(&missing), (Some("NoSuchBucket".to_owned()), Some(404)));
    let negative = client
        .list_objects_v2()
        .bucket(LISTED_BUCKET)
        .max_keys(-1)
        .send()
        .await
        .expect_err("negative");
    assert_eq!(refusal(&negative), (Some("InvalidArgument".to_owned()), Some(400)));
    let forged = client
        .list_objects_v2()
        .bucket(LISTED_BUCKET)
        .continuation_token("%%%")
        .send()
        .await
        .expect_err("an undecodable token");
    assert_eq!(refusal(&forged), (Some("InvalidArgument".to_owned()), Some(400)));
    let v1_missing = client
        .list_objects()
        .bucket("gateway-stack-missing")
        .send()
        .await
        .expect_err("no bucket");
    assert_eq!(refusal(&v1_missing), (Some("NoSuchBucket".to_owned()), Some(404)));
    let versions_missing = client
        .list_object_versions()
        .bucket("gateway-stack-missing")
        .send()
        .await
        .expect_err("no bucket");
    assert_eq!(refusal(&versions_missing), (Some("NoSuchBucket".to_owned()), Some(404)));
    let versions_negative = client
        .list_object_versions()
        .bucket(LISTED_BUCKET)
        .max_keys(-1)
        .send()
        .await
        .expect_err("negative");
    assert_eq!(refusal(&versions_negative), (Some("InvalidArgument".to_owned()), Some(400)));

    server.shutdown().await;
}

#[test]
fn the_gateway_stack_lists_every_stored_key() {
    with_stack(GATEWAY, the_listing_names_every_stored_key);
}

#[test]
fn the_legacy_stack_lists_every_stored_key() {
    with_stack(LEGACY, the_listing_names_every_stored_key);
}

/// Entity tags the store holds verbatim (migration write-back and replication receive) are listed
/// as stored, each on its own object.
async fn the_listing_shows_every_stored_entity_tag(server: RustFSServer) {
    create_bucket_in_the_store(LISTED_BUCKET).await;
    let stored = [
        ("empty", "\"\""),
        ("tab", "ab\tcd"),
        ("markup", "</ETag><Key>victim</Key>&amp;"),
    ];
    for (key, etag) in stored {
        put_in_the_store(LISTED_BUCKET, key, Some(etag)).await;
    }

    let client = s3_client(&server.endpoint(), server.access_key(), server.secret_key());
    let listing = client
        .list_objects_v2()
        .bucket(LISTED_BUCKET)
        .send()
        .await
        .expect("ListObjectsV2");

    let listed: Vec<(&str, Option<&str>)> = listing
        .contents()
        .iter()
        .map(|object| (object.key().unwrap_or_default(), object.e_tag()))
        .collect();
    assert_eq!(
        listed,
        [
            ("empty", Some("\"\"")),
            ("markup", Some("\"</ETag><Key>victim</Key>&amp;\"")),
            ("tab", Some("\"ab\tcd\""))
        ]
    );
    let v1 = client.list_objects().bucket(LISTED_BUCKET).send().await.expect("ListObjects");
    let v1_entries: Vec<(&str, Option<&str>)> = v1
        .contents()
        .iter()
        .map(|object| (object.key().unwrap_or_default(), object.e_tag()))
        .collect();
    assert_eq!(v1_entries, listed);
    let versions = client
        .list_object_versions()
        .bucket(LISTED_BUCKET)
        .send()
        .await
        .expect("ListObjectVersions");
    let version_entries: Vec<(&str, Option<&str>)> = versions
        .versions()
        .iter()
        .map(|object| (object.key().unwrap_or_default(), object.e_tag()))
        .collect();
    assert_eq!(version_entries, listed);
    server.shutdown().await;
}

#[test]
fn the_gateway_stack_lists_every_stored_entity_tag() {
    with_stack(GATEWAY, the_listing_shows_every_stored_entity_tag);
}

#[test]
fn the_legacy_stack_lists_every_stored_entity_tag() {
    with_stack(LEGACY, the_listing_shows_every_stored_entity_tag);
}

/// Both listing edges retain stored bucket names, exact page boundaries, and multipart cursors.
async fn listings_keep_stored_names_and_page_boundaries(server: RustFSServer) {
    let client = s3_client(&server.endpoint(), server.access_key(), server.secret_key());
    for bucket in ["listing-boundaries", "sthree-listing-boundaries"] {
        create_bucket_in_the_store(bucket).await;
        for key in ["a &b", "c/d"] {
            put_in_the_store(bucket, key, Some("a&b")).await;
            get_global_app_context()
                .expect("embedded context")
                .object_store()
                .new_multipart_upload(bucket, key, &ObjectOptions::default())
                .await
                .expect("create stored multipart upload");
        }
        for row in MATRIX {
            assert_eq!((row.call)(&client, bucket).await, Ok(()), "{} {bucket}", row.operation);
        }
        for max in [0, 1, 2, 3] {
            let page = client
                .list_objects_v2()
                .bucket(bucket)
                .max_keys(max)
                .send()
                .await
                .expect("page");
            assert_eq!(page.name(), Some(bucket));
            assert_eq!(page.contents().len(), usize::try_from(max.min(2)).unwrap());
            assert_eq!(page.is_truncated(), Some(max == 1));
        }
        let folded = client
            .list_objects_v2()
            .bucket(bucket)
            .delimiter("/")
            .send()
            .await
            .expect("delimiter");
        assert_eq!(folded.contents().len(), 1);
        assert_eq!(folded.common_prefixes().len(), 1);
        assert_eq!(folded.common_prefixes()[0].prefix(), Some("c/"));
        let versions = client.list_object_versions().bucket(bucket).send().await.expect("versions");
        assert_eq!(versions.name(), Some(bucket));
        assert_eq!(versions.versions().len(), 2);
        assert_eq!(versions.versions()[0].e_tag(), Some("\"a&b\""));
        let first = client
            .list_multipart_uploads()
            .bucket(bucket)
            .max_uploads(1)
            .encoding_type(EncodingType::Url)
            .send()
            .await
            .expect("first upload page");
        assert_eq!(first.bucket(), Some(bucket));
        assert_eq!(first.uploads().len(), 1);
        assert_eq!(first.uploads()[0].key(), Some("a%20%26b"));
        assert_eq!(first.is_truncated(), Some(true));
        let second = client
            .list_multipart_uploads()
            .bucket(bucket)
            .max_uploads(1)
            .key_marker("a &b")
            .set_upload_id_marker(first.next_upload_id_marker().map(str::to_owned))
            .send()
            .await
            .expect("second upload page");
        assert_eq!(second.uploads().len(), 1);
        assert_eq!(second.uploads()[0].key(), Some("c/d"));
        assert_eq!(second.is_truncated(), Some(false));
        let exact = client
            .list_multipart_uploads()
            .bucket(bucket)
            .max_uploads(2)
            .send()
            .await
            .expect("exact upload page");
        assert_eq!(exact.uploads().len(), 2);
        assert_eq!(exact.is_truncated(), Some(false));
    }
    server.shutdown().await;
}

#[test]
fn the_gateway_stack_keeps_listing_names_and_boundaries() {
    with_stack(GATEWAY, listings_keep_stored_names_and_page_boundaries);
}

#[test]
fn the_legacy_stack_keeps_listing_names_and_boundaries() {
    with_stack(LEGACY, listings_keep_stored_names_and_page_boundaries);
}
