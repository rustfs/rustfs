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

//! Tests of the `listing_multipart` legacy-edge handlers (batch B2), moved unchanged from `ecfs.rs`
//! and `ecfs_test.rs`.

use super::build_request;
use crate::storage::StorageError;
use crate::storage::ecfs::*;
use http::{Method, StatusCode};

#[test]
#[serial_test::serial]
fn list_apis_report_storage_read_quorum_as_unavailable() {
    crate::app::gating_test_env::run_large_stack_test("list-quorum-surface", || async {
        use crate::storage::storage_api::contract::bucket::{BucketOperations as _, MakeBucketOptions};
        crate::app::gating_test_env::shared_gating_ambient().await;
        let store = crate::app::gating_test_env::shared_gating_ecstore().await;
        let bucket = format!("list-quorum-{}", uuid::Uuid::new_v4().simple());
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create listing bucket");
        let set = &store.pools[0].disk_set[0];
        let saved_disks = set.disks.read().await.clone();
        {
            let mut disks = set.disks.write().await;
            for disk in disks.iter_mut().skip(1) {
                *disk = None;
            }
        }
        let fs = FS::new();
        let errors = [
            S3::list_objects(
                &fs,
                build_request(
                    ListObjectsInput {
                        bucket: bucket.clone(),
                        ..Default::default()
                    },
                    Method::GET,
                ),
            )
            .await
            .expect_err("V1 cannot list below storage read quorum"),
            S3::list_objects_v2(
                &fs,
                build_request(
                    ListObjectsV2Input {
                        bucket: bucket.clone(),
                        ..Default::default()
                    },
                    Method::GET,
                ),
            )
            .await
            .expect_err("V2 cannot list below storage read quorum"),
            S3::list_object_versions(
                &fs,
                build_request(
                    ListObjectVersionsInput {
                        bucket,
                        ..Default::default()
                    },
                    Method::GET,
                ),
            )
            .await
            .expect_err("versions cannot list below storage read quorum"),
        ];
        *set.disks.write().await = saved_disks;
        for error in errors {
            assert_eq!(error.code(), &S3ErrorCode::ServiceUnavailable);
            assert_eq!(error.status_code(), Some(StatusCode::SERVICE_UNAVAILABLE));
            assert_eq!(error.message(), Some("The service is unavailable. Please retry."));
            let source = error.source().expect("retain storage diagnostics");
            assert!(
                matches!(
                    source.downcast_ref::<StorageError>(),
                    Some(StorageError::ErasureReadQuorum | StorageError::InsufficientReadQuorum(_, _))
                ),
                "the production LIST boundary must expose a typed read-quorum failure: {source}"
            );
        }
    });
}

/// The legacy edge writes the stored values the use case answers raw the way legacy RustFS wrote
/// them: each key and rolled-up prefix percent-encoded segment by segment for exactly `url`, raw for
/// any other value or none, and every entity tag XML-escaped for the legacy encoder.
#[test]
fn listing_entries_are_encoded_per_segment_only_for_url_and_tags_are_escaped() {
    use super::super::listing_multipart::legacy_listing_entries;
    let (key, prefix) = ("dir a/file+b%.txt", "prefix a/sub+");
    for (encoding, expected_key, expected_prefix) in [
        (
            Some(EncodingType::from_static(EncodingType::URL)),
            "dir%20a/file%2Bb%25.txt",
            "prefix%20a/sub%2B",
        ),
        (Some(EncodingType::from("URL".to_owned())), key, prefix),
        (None, key, prefix),
    ] {
        let mut contents = Some(vec![Object {
            key: Some(key.to_owned()),
            e_tag: Some(ETag::Strong("</ETag><Key>victim</Key>&amp;".to_owned())),
            ..Default::default()
        }]);
        let mut common_prefixes = Some(vec![CommonPrefix {
            prefix: Some(prefix.to_owned()),
        }]);

        legacy_listing_entries(&mut contents, &mut common_prefixes, encoding.as_ref());

        let object = &contents.as_deref().expect("contents")[0];
        assert_eq!(object.key.as_deref(), Some(expected_key), "{encoding:?}");
        assert_eq!(
            object.e_tag,
            Some(ETag::Strong("&lt;/ETag&gt;&lt;Key&gt;victim&lt;/Key&gt;&amp;amp;".to_owned()))
        );
        assert_eq!(common_prefixes.as_deref().expect("prefixes")[0].prefix.as_deref(), Some(expected_prefix));
    }
}

/// Every key-shaped member of a version listing is encoded as legacy RustFS encodes it, for
/// exactly `url`; the version id markers never are; entity tags are escaped.
#[test]
fn version_listings_encode_every_key_shaped_member_only_for_url() {
    use super::super::listing_multipart::legacy_list_object_versions;
    let raw = |value: &str| Some(value.to_owned());
    for (encoding, encoded) in [(Some(EncodingType::from_static(EncodingType::URL)), true), (None, false)] {
        let mut output = ListObjectVersionsOutput {
            prefix: raw("logs and more/"),
            delimiter: raw("/"),
            key_marker: raw("logs and more/start marker"),
            next_key_marker: raw("logs and more/next marker"),
            version_id_marker: raw("version marker"),
            next_version_id_marker: raw("next version"),
            versions: Some(vec![ObjectVersion {
                key: raw("logs and more/file name.txt"),
                e_tag: Some(ETag::Strong("<a>".to_owned())),
                ..Default::default()
            }]),
            delete_markers: Some(vec![DeleteMarkerEntry {
                key: raw("logs and more/delete marker.txt"),
                ..Default::default()
            }]),
            common_prefixes: Some(vec![CommonPrefix {
                prefix: raw("logs and more/sub prefix/"),
            }]),
            ..Default::default()
        };

        legacy_list_object_versions(&mut output, encoding.as_ref());

        let expect = |value: &str| {
            if encoded {
                value.replace(' ', "%20")
            } else {
                value.to_owned()
            }
        };
        assert_eq!(output.prefix, Some(expect("logs and more/")));
        assert_eq!(output.delimiter.as_deref(), Some("/"));
        assert_eq!(output.key_marker, Some(expect("logs and more/start marker")));
        assert_eq!(output.next_key_marker, Some(expect("logs and more/next marker")));
        assert_eq!(output.version_id_marker.as_deref(), Some("version marker"));
        assert_eq!(output.next_version_id_marker.as_deref(), Some("next version"));
        let version = &output.versions.as_deref().expect("versions")[0];
        assert_eq!(version.key, Some(expect("logs and more/file name.txt")));
        assert_eq!(version.e_tag, Some(ETag::Strong("&lt;a&gt;".to_owned())));
        assert_eq!(
            output.delete_markers.as_deref().expect("markers")[0].key,
            Some(expect("logs and more/delete marker.txt"))
        );
        assert_eq!(
            output.common_prefixes.as_deref().expect("prefixes")[0].prefix,
            Some(expect("logs and more/sub prefix/"))
        );
    }
}

/// The upload listing's key-shaped members are encoded for exactly `url`; upload ids never are.
#[test]
fn upload_listings_encode_key_shaped_members_only_for_url() {
    use super::super::listing_multipart::legacy_list_multipart_uploads;
    let raw = |value: &str| Some(value.to_owned());
    for (encoding, encoded) in [(Some(EncodingType::from_static(EncodingType::URL)), true), (None, false)] {
        let mut output = ListMultipartUploadsOutput {
            prefix: raw("dir a/"),
            delimiter: raw("/"),
            key_marker: raw("dir a/start+1"),
            next_key_marker: raw("dir a/next+1"),
            upload_id_marker: raw("upload+id%"),
            next_upload_id_marker: raw("next+id%"),
            uploads: Some(vec![MultipartUpload {
                key: raw("dir a/file%"),
                upload_id: raw("upload+id%"),
                ..Default::default()
            }]),
            common_prefixes: Some(vec![CommonPrefix {
                prefix: raw("dir a/sub/"),
            }]),
            ..Default::default()
        };

        legacy_list_multipart_uploads(&mut output, encoding.as_ref());

        let expect = |value: &str| {
            if encoded {
                value.replace('%', "%25").replace(' ', "%20").replace('+', "%2B")
            } else {
                value.to_owned()
            }
        };
        assert_eq!(output.prefix, Some(expect("dir a/")));
        assert_eq!(output.delimiter.as_deref(), Some("/"));
        assert_eq!(output.key_marker, Some(expect("dir a/start+1")));
        assert_eq!(output.next_key_marker, Some(expect("dir a/next+1")));
        assert_eq!(output.upload_id_marker.as_deref(), Some("upload+id%"));
        assert_eq!(output.next_upload_id_marker.as_deref(), Some("next+id%"));
        let upload = &output.uploads.as_deref().expect("uploads")[0];
        assert_eq!(upload.key, Some(expect("dir a/file%")));
        assert_eq!(upload.upload_id.as_deref(), Some("upload+id%"));
        assert_eq!(output.common_prefixes.as_deref().expect("prefixes")[0].prefix, Some(expect("dir a/sub/")));
    }
}
