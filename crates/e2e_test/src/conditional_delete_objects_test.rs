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

//! S3 wire semantics for per-object conditional bulk deletion.

use crate::common::RustFSTestEnvironment;
use aws_sdk_s3::Client;
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::operation::delete_objects::DeleteObjectsOutput;
use aws_sdk_s3::operation::put_object::PutObjectOutput;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{BucketVersioningStatus, Delete, ObjectIdentifier, VersioningConfiguration};
use std::collections::BTreeSet;
use std::error::Error;

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;
type VersionSnapshot = BTreeSet<(String, bool)>;

async fn put(client: &Client, bucket: &str, key: &str, body: &'static [u8]) -> TestResult<PutObjectOutput> {
    let result = client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from_static(body))
        .send()
        .await?;
    assert!(result.e_tag().is_some(), "fixture PUT must provide an ETag for the following conditions");
    Ok(result)
}

fn identifier(key: &str, etag: Option<&str>, version: Option<&str>) -> TestResult<ObjectIdentifier> {
    Ok(ObjectIdentifier::builder()
        .key(key)
        .set_e_tag(etag.map(str::to_owned))
        .set_version_id(version.map(str::to_owned))
        .build()?)
}

async fn delete_batch(
    client: &Client,
    bucket: &str,
    objects: Vec<ObjectIdentifier>,
    quiet: bool,
) -> TestResult<DeleteObjectsOutput> {
    Ok(client
        .delete_objects()
        .bucket(bucket)
        .delete(Delete::builder().set_objects(Some(objects)).quiet(quiet).build()?)
        .send()
        .await?)
}

async fn assert_body(client: &Client, bucket: &str, key: &str, expected: &[u8]) -> TestResult {
    let result = client.get_object().bucket(bucket).key(key).send().await?;
    assert_eq!(result.body.collect().await?.into_bytes().as_ref(), expected);
    Ok(())
}

async fn assert_absent(client: &Client, bucket: &str, key: &str) {
    let error = client
        .head_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .expect_err("deleted key must be absent");
    assert_eq!(error.as_service_error().and_then(|error| error.code()), Some("NotFound"));
}

async fn versions(client: &Client, bucket: &str, key: &str) -> TestResult<VersionSnapshot> {
    let result = client.list_object_versions().bucket(bucket).prefix(key).send().await?;
    assert!(!result.is_truncated().unwrap_or_default(), "focused version fixture fits a single page");
    let mut snapshot = BTreeSet::new();
    for version in result.versions().iter().filter(|version| version.key() == Some(key)) {
        snapshot.insert((version.version_id().expect("version has an identity").to_owned(), false));
    }
    for marker in result.delete_markers().iter().filter(|marker| marker.key() == Some(key)) {
        snapshot.insert((marker.version_id().expect("delete marker has an identity").to_owned(), true));
    }
    Ok(snapshot)
}

#[tokio::test]
async fn sdk_conditional_delete_objects_mixed_results_overwrites_duplicates_and_quiet() -> TestResult {
    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;
    let client = env.create_s3_client();
    let bucket = "conditional-bulk-mixed";
    client.create_bucket().bucket(bucket).send().await?;

    let old = put(&client, bucket, "overwritten", b"generation A").await?;
    let current = put(&client, bucket, "overwritten", b"generation B").await?;
    let matched = put(&client, bucket, "matched", b"matching object").await?;
    put(&client, bucket, "exists", b"existence object").await?;
    put(&client, bucket, "unconditional", b"unconditional object").await?;
    put(&client, bucket, "empty", b"empty condition must preserve this").await?;
    put(&client, bucket, "empty-quoted", b"quoted empty condition must preserve this").await?;
    let weak = put(&client, bucket, "weak", b"weak condition must preserve this").await?;
    let weak_etag = format!("W/\"{}\"", weak.e_tag().expect("PUT supplies an ETag").trim_matches('"'));
    assert_ne!(old.e_tag(), current.e_tag(), "overwrite fixture must have distinct ETags");
    let matched_etag = matched.e_tag().expect("PUT supplies an ETag");
    let result = delete_batch(
        &client,
        bucket,
        vec![
            identifier("matched", Some(matched_etag), None)?,
            identifier("overwritten", old.e_tag(), None)?,
            identifier("matched", Some("stale-etag"), None)?,
            identifier("matched", Some(matched_etag), None)?,
            identifier("unconditional", None, None)?,
            identifier("exists", Some("*"), None)?,
            identifier("missing", Some("*"), None)?,
            identifier("empty", Some(""), None)?,
            identifier("empty-quoted", Some("\"\""), None)?,
            identifier("weak", Some(&weak_etag), None)?,
            identifier("unconditional-missing", None, None)?,
            identifier("missing-specific", Some("opaque-etag"), None)?,
        ],
        false,
    )
    .await?;
    assert_eq!(
        result.deleted().iter().map(|object| object.key()).collect::<Vec<_>>(),
        [
            Some("matched"),
            Some("matched"),
            Some("unconditional"),
            Some("exists"),
            Some("unconditional-missing")
        ],
        "duplicate successful inputs retain their separate response entries"
    );
    assert_eq!(
        result
            .errors()
            .iter()
            .map(|error| (error.key(), error.code()))
            .collect::<Vec<_>>(),
        [
            (Some("overwritten"), Some("PreconditionFailed")),
            (Some("matched"), Some("PreconditionFailed")),
            (Some("missing"), Some("NoSuchKey")),
            (Some("empty"), Some("InvalidArgument")),
            (Some("empty-quoted"), Some("InvalidArgument")),
            (Some("weak"), Some("InvalidArgument")),
            (Some("missing-specific"), Some("NoSuchKey")),
        ]
    );
    assert_body(&client, bucket, "overwritten", b"generation B").await?;
    assert_body(&client, bucket, "empty", b"empty condition must preserve this").await?;
    assert_body(&client, bucket, "empty-quoted", b"quoted empty condition must preserve this").await?;
    assert_body(&client, bucket, "weak", b"weak condition must preserve this").await?;
    for key in ["matched", "unconditional", "exists"] {
        assert_absent(&client, bucket, key).await;
    }

    let quiet = put(&client, bucket, "quiet-match", b"quiet matching object").await?;
    put(&client, bucket, "quiet-rejected", b"quiet rejected object").await?;
    let result = delete_batch(
        &client,
        bucket,
        vec![
            identifier("quiet-match", quiet.e_tag(), None)?,
            identifier("quiet-match", quiet.e_tag(), None)?,
            identifier("quiet-rejected", Some("stale-etag"), None)?,
            identifier("missing", Some("*"), None)?,
        ],
        true,
    )
    .await?;
    assert!(result.deleted().is_empty(), "Quiet suppresses successful entries");
    assert_eq!(
        result
            .errors()
            .iter()
            .map(|error| (error.key(), error.code()))
            .collect::<Vec<_>>(),
        [
            (Some("quiet-rejected"), Some("PreconditionFailed")),
            (Some("missing"), Some("NoSuchKey"))
        ]
    );
    assert_absent(&client, bucket, "quiet-match").await;
    assert_body(&client, bucket, "quiet-rejected", b"quiet rejected object").await?;
    Ok(())
}

#[tokio::test]
async fn sdk_conditional_delete_objects_versioning_current_markers_and_explicit_versions() -> TestResult {
    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;
    let client = env.create_s3_client();
    for suspended in [false, true] {
        let bucket = if suspended {
            "conditional-bulk-suspended"
        } else {
            "conditional-bulk-enabled"
        };
        client.create_bucket().bucket(bucket).send().await?;
        client
            .put_bucket_versioning()
            .bucket(bucket)
            .versioning_configuration(
                VersioningConfiguration::builder()
                    .status(BucketVersioningStatus::Enabled)
                    .build(),
            )
            .send()
            .await?;
        let historical = put(&client, bucket, "history", b"historical payload").await?;
        let invalid_historical = put(&client, bucket, "invalid", b"historical rejected payload").await?;
        if suspended {
            client
                .put_bucket_versioning()
                .bucket(bucket)
                .versioning_configuration(
                    VersioningConfiguration::builder()
                        .status(BucketVersioningStatus::Suspended)
                        .build(),
                )
                .send()
                .await?;
        }
        let current = put(&client, bucket, "history", b"current payload").await?;
        let invalid = put(&client, bucket, "invalid", b"current rejected payload").await?;
        let invalid_before = versions(&client, bucket, "invalid").await?;
        let historical_id = historical.version_id().expect("enabled fixture has a UUID version");
        let invalid_historical_id = invalid_historical
            .version_id()
            .expect("enabled rejected fixture has a UUID version");
        let result = delete_batch(
            &client,
            bucket,
            vec![
                identifier("history", current.e_tag(), None)?,
                identifier("history", None, Some(historical_id))?,
                identifier("history", current.e_tag(), None)?,
                identifier("invalid", invalid.e_tag(), Some(invalid_historical_id))?,
                identifier("invalid", Some("*"), Some("null"))?,
                identifier("invalid", Some("stale-etag"), None)?,
            ],
            false,
        )
        .await?;
        assert_eq!(result.deleted().len(), 3, "both current input entries and the historical delete succeed");
        assert_eq!(
            result
                .errors()
                .iter()
                .map(|error| (error.key(), error.code()))
                .collect::<Vec<_>>(),
            [
                (Some("invalid"), Some("InvalidRequest")),
                (Some("invalid"), Some("InvalidRequest")),
                (Some("invalid"), Some("PreconditionFailed")),
            ]
        );
        assert_eq!(result.errors()[0].version_id(), Some(invalid_historical_id));
        assert_eq!(result.errors()[1].version_id(), Some("null"));
        assert_eq!(versions(&client, bucket, "invalid").await?, invalid_before);
        assert_body(&client, bucket, "invalid", b"current rejected payload").await?;

        let marker = result.deleted()[0]
            .delete_marker_version_id()
            .expect("current conditional delete returns a marker");
        assert_eq!(result.deleted()[0].delete_marker(), Some(true));
        assert_eq!(result.deleted()[2].delete_marker_version_id(), Some(marker));
        assert_eq!(result.deleted()[1].version_id(), Some(historical_id));
        if suspended {
            assert_eq!(marker, "null", "a suspended current delete returns the created null marker identity");
        }
        let history_after = versions(&client, bucket, "history").await?;
        assert_eq!(
            history_after.iter().filter(|(_, marker)| *marker).count(),
            1,
            "duplicate actions create one marker"
        );
        assert!(
            !history_after.contains(&(historical_id.to_string(), false)),
            "distinct historical target is removed"
        );
        if suspended {
            assert_eq!(history_after, BTreeSet::from([("null".to_string(), true)]));
        } else {
            assert_eq!(
                history_after,
                BTreeSet::from([
                    (current.version_id().expect("current enabled object has a UUID").to_string(), false),
                    (marker.to_string(), true),
                ]),
                "enabled conditional current deletion retains the current data version behind the marker"
            );
        }

        let result = delete_batch(
            &client,
            bucket,
            vec![
                identifier("history", Some("*"), None)?,
                identifier("history", current.e_tag(), None)?,
                identifier("missing", Some("*"), None)?,
            ],
            false,
        )
        .await?;
        assert!(
            result.deleted().is_empty(),
            "current markers and absent objects cannot satisfy a live-object condition"
        );
        assert_eq!(result.errors().len(), 3);
        assert!(result.errors().iter().all(|error| error.code() == Some("NoSuchKey")));
        assert_eq!(
            versions(&client, bucket, "history").await?,
            history_after,
            "failed conditions create no marker"
        );
        assert!(versions(&client, bucket, "missing").await?.is_empty());

        put(&client, bucket, "exists", b"versioned existence payload").await?;
        let result = delete_batch(&client, bucket, vec![identifier("exists", Some("*"), None)?], false).await?;
        assert!(result.errors().is_empty());
        assert_eq!(result.deleted().len(), 1);
        assert_eq!(result.deleted()[0].delete_marker(), Some(true));
        assert_eq!(
            versions(&client, bucket, "exists")
                .await?
                .iter()
                .filter(|(_, marker)| *marker)
                .count(),
            1
        );

        if !suspended {
            let directory = put(&client, bucket, "dir/", b"directory null-version payload").await?;
            assert_eq!(
                versions(&client, bucket, "dir/").await?,
                BTreeSet::from([("null".to_string(), false)]),
                "the directory PUT fixture must identify the synthetic null data version"
            );
            let result = delete_batch(
                &client,
                bucket,
                vec![identifier("dir/", directory.e_tag(), None)?, identifier("dir/", None, None)?],
                false,
            )
            .await?;
            assert!(result.errors().is_empty(), "both distinct directory actions must succeed");
            assert_eq!(result.deleted().len(), 2);
            assert_eq!(result.deleted()[0].key(), Some("dir/"));
            assert_eq!(result.deleted()[1].key(), Some("dir/"));
            assert_eq!(result.deleted()[0].delete_marker(), Some(true));
            let marker = result.deleted()[0]
                .delete_marker_version_id()
                .expect("conditional current-directory deletion creates a versioned marker");
            assert!(
                !uuid::Uuid::parse_str(marker)?.is_nil(),
                "enabled current-directory deletion creates a UUID marker"
            );
            assert_eq!(result.deleted()[1].delete_marker(), None);
            assert_eq!(result.deleted()[1].delete_marker_version_id(), None);
            assert_eq!(result.deleted()[1].version_id(), None);
            assert_eq!(
                versions(&client, bucket, "dir/").await?,
                BTreeSet::from([(marker.to_string(), true)]),
                "current-marker creation and synthetic null-version removal are distinct actions: one marker remains and the null data version is gone"
            );
        }
    }
    Ok(())
}
