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

//! Basic read-through scenarios (rustfs/backlog#2156): inline pull and
//! local persistence, large-object passthrough with background backfill,
//! Range passthrough, source 404, `versionId` reads, a disabled bucket, and
//! the HEAD passthrough that stores nothing (rustfs/backlog#2155).
//! Every source-side expectation is asserted on the fake source's journal.

use super::common::{BoxError, OdmSourceSpec, OdmTestEnv, SeedObject};
use crate::fake_s3_target::{BucketMode, Operation};
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::types::{BucketVersioningStatus, ObjectAttributes, VersioningConfiguration};
use bytes::Bytes;
use std::time::Duration;

type TestResult = Result<(), BoxError>;

const SOURCE_BUCKET: &str = "odm-get-source";
const ODM_RESPONSE_HEADER: &str = "x-rustfs-on-demand-migration";
/// Background pulls run after the response; generous for a loaded CI host.
const BACKFILL_WAIT: Duration = Duration::from_secs(60);

/// Position-dependent payload so a misaligned or truncated copy is caught.
fn payload(len: usize) -> Bytes {
    (0..len).map(|index| (index % 251) as u8).collect::<Vec<u8>>().into()
}

/// RustFS with `local_bucket` migrating from `SOURCE_BUCKET` on the fake
/// source (unversioned, like a plain migration source); `adjust` tweaks the
/// policy before it is installed. Returns once the runtime consults the
/// source.
async fn configured_env(local_bucket: &str, adjust: impl FnOnce(&mut OdmSourceSpec)) -> Result<OdmTestEnv, BoxError> {
    let env = OdmTestEnv::start().await?;
    env.source.create_bucket_with_mode(SOURCE_BUCKET, BucketMode::Unversioned);
    env.rustfs.create_test_bucket(local_bucket).await?;
    let mut spec = env.fake_source_spec(SOURCE_BUCKET);
    adjust(&mut spec);
    let response = env.configure_source(local_bucket, &spec).await?;
    assert_eq!(response.status, 200, "configure on-demand migration: {}", response.body);
    env.wait_until_source_consulted(local_bucket).await?;
    Ok(env)
}

fn source_get_ranges(env: &OdmTestEnv, key: &str) -> Vec<Option<String>> {
    env.source
        .requests()
        .into_iter()
        .filter(|record| record.operation == Operation::GetObject && record.key.as_deref() == Some(key))
        .map(|record| record.range)
        .collect()
}

#[tokio::test]
async fn get_miss_pulls_inline_and_serves_locally_afterwards() -> TestResult {
    let bucket = "odm-get-inline";
    let env = configured_env(bucket, |_| {}).await?;
    let key = "inline/report.bin";
    let body = payload(200 * 1024);
    let etag = env
        .seed_source(SOURCE_BUCKET, &[SeedObject::new(key, body.clone())])
        .remove(0);
    let quoted_etag = format!("\"{etag}\"");

    let first = env.raw_get(bucket, key).await?;
    assert_eq!(first.status, 200, "{}", String::from_utf8_lossy(&first.body));
    assert_eq!(first.header(ODM_RESPONSE_HEADER), Some("source"), "a source answer is marked");
    assert_eq!(first.header("etag"), Some(quoted_etag.as_str()), "inline answers carry the source ETag");
    assert_eq!(first.header("content-length"), Some(body.len().to_string().as_str()));
    assert_eq!(first.header("accept-ranges"), Some("bytes"));
    assert_eq!(first.body, body, "the client receives the source bytes");
    assert_eq!(env.source.count_requests(Operation::GetObject, key), 1, "exactly one source GET");
    assert_eq!(env.source.count_requests(Operation::HeadObject, key), 1);

    assert!(
        env.wait_local_listed(bucket, key, BACKFILL_WAIT).await?,
        "the inline pull must store the object locally"
    );
    let second = env.raw_get(bucket, key).await?;
    assert_eq!(second.status, 200);
    assert_eq!(second.header(ODM_RESPONSE_HEADER), None, "a local hit carries no source marker");
    assert_eq!(second.body, body, "the local copy is the source bytes");
    assert_eq!(second.header("etag"), Some(quoted_etag.as_str()), "preserve_etag keeps the source ETag");
    assert_eq!(
        env.source.count_requests(Operation::GetObject, key),
        1,
        "the second GET is served locally"
    );
    assert_eq!(env.source.count_requests(Operation::HeadObject, key), 1);
    Ok(())
}

#[tokio::test]
async fn get_large_object_streams_through_and_backfills_in_background() -> TestResult {
    const PART_SIZE: usize = 5 * 1024 * 1024;
    let bucket = "odm-get-large";
    let env = configured_env(bucket, |spec| {
        spec.policy.inline_max_bytes = 4096;
        spec.policy.multipart_part_size_bytes = u64::try_from(PART_SIZE).expect("part size fits in u64");
    })
    .await?;
    let key = "large/archive.bin";
    let body = payload(PART_SIZE + 4096);
    let etag = env
        .seed_source(SOURCE_BUCKET, &[SeedObject::new(key, body.clone())])
        .remove(0);
    assert_eq!(etag.len(), 32, "the source fixture has a plain MD5 ETag");

    let response = env.raw_get(bucket, key).await?;
    assert_eq!(response.status, 200, "{}", String::from_utf8_lossy(&response.body));
    assert_eq!(response.header(ODM_RESPONSE_HEADER), Some("source"));
    assert_eq!(response.header("content-length"), Some(body.len().to_string().as_str()));
    assert_eq!(response.body, body, "the passthrough streams the whole object");

    assert!(
        env.wait_local_listed(bucket, key, BACKFILL_WAIT).await?,
        "the background pull must store the object locally"
    );
    env.assert_local_present(bucket, key, &body).await;
    assert_eq!(
        source_get_ranges(&env, key),
        vec![None, None],
        "one passthrough GET plus one background pull, both unranged"
    );

    let source_requests = env.source.requests().len();
    let second_part = env.client.get_object().bucket(bucket).key(key).part_number(2).send().await?;
    assert_eq!(second_part.content_length(), Some(4096), "the completed second part is the tail");
    assert_eq!(
        second_part.content_range(),
        Some(format!("bytes {PART_SIZE}-{}/{}", body.len() - 1, body.len()).as_str()),
        "partNumber reads the stored multipart boundary"
    );
    assert_eq!(
        second_part.body.collect().await?.into_bytes(),
        body.slice(PART_SIZE..),
        "the local second part contains the exact source tail"
    );
    let third_part = env
        .client
        .get_object()
        .bucket(bucket)
        .key(key)
        .part_number(3)
        .send()
        .await
        .expect_err("the completed object has exactly two parts");
    assert_eq!(third_part.code(), Some("InvalidPart"));

    let mut part_marker = None;
    for (part_number, part_size) in [(1, PART_SIZE), (2, 4096)] {
        let attributes = env
            .client
            .get_object_attributes()
            .bucket(bucket)
            .key(key)
            .object_attributes(ObjectAttributes::ObjectParts)
            .object_attributes(ObjectAttributes::Etag)
            .max_parts(1)
            .set_part_number_marker(part_marker.clone())
            .send()
            .await?;
        assert_eq!(
            attributes.e_tag().map(|value| value.trim_matches('"')),
            Some(etag.as_str()),
            "multipart write-back preserves the source MD5 ETag"
        );
        let parts = attributes
            .object_parts()
            .expect("RustFS must expose the stored multipart layout");
        assert_eq!(parts.total_parts_count(), Some(2));
        assert_eq!(parts.max_parts(), Some(1));
        assert_eq!(parts.is_truncated(), Some(part_number == 1));
        assert_eq!(parts.parts().len(), 1, "RustFS returns one stored part per requested page");
        assert_eq!(parts.parts()[0].part_number(), Some(part_number));
        assert_eq!(parts.parts()[0].size(), Some(i64::try_from(part_size).expect("part size fits in i64")));
        part_marker = parts.next_part_number_marker().map(str::to_owned);
        if part_number == 1 {
            assert_eq!(part_marker.as_deref(), Some("1"), "the next request continues after the first part");
        }
    }
    assert_eq!(
        env.source.requests().len(),
        source_requests,
        "local part reads must not consult the source"
    );
    Ok(())
}

#[tokio::test]
async fn get_range_streams_206_and_backfills_the_whole_object() -> TestResult {
    let bucket = "odm-get-range";
    let env = configured_env(bucket, |_| {}).await?;
    let key = "range/video.bin";
    let body = payload(100_000);
    let etag = env
        .seed_source(SOURCE_BUCKET, &[SeedObject::new(key, body.clone())])
        .remove(0);

    let response = env
        .client
        .get_object()
        .bucket(bucket)
        .key(key)
        .range("bytes=10-19")
        .send()
        .await?;
    assert_eq!(response.content_range(), Some("bytes 10-19/100000"), "the source's 206 is passed through");
    assert_eq!(response.content_length(), Some(10));
    assert_eq!(response.e_tag(), Some(format!("\"{etag}\"").as_str()));
    assert_eq!(response.body.collect().await?.into_bytes(), body.slice(10..20));
    assert_eq!(
        source_get_ranges(&env, key),
        vec![Some("bytes=10-19".to_string())],
        "the Range is forwarded"
    );

    assert!(
        env.wait_local_listed(bucket, key, BACKFILL_WAIT).await?,
        "serve_and_backfill must pull the whole object"
    );
    env.assert_local_present(bucket, key, &body).await;
    assert_eq!(
        source_get_ranges(&env, key),
        vec![Some("bytes=10-19".to_string()), None],
        "the background pull fetches the whole object"
    );
    Ok(())
}

#[tokio::test]
async fn get_source_not_found_is_404_and_negative_cached() -> TestResult {
    let bucket = "odm-get-missing";
    let env = configured_env(bucket, |_| {}).await?;
    let key = "missing/nowhere.bin";

    for attempt in 1..=2 {
        let err = env
            .client
            .get_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .expect_err("a key missing on both sides is 404");
        assert_eq!(err.code(), Some("NoSuchKey"), "attempt {attempt}: {err:?}");
    }
    assert_eq!(env.source.count_requests(Operation::GetObject, key), 0, "a source miss never pulls");
    assert_eq!(
        env.source.count_requests(Operation::HeadObject, key),
        1,
        "the second miss stops at the negative cache"
    );
    env.assert_local_absent(bucket, key).await;
    Ok(())
}

#[tokio::test]
async fn get_with_version_id_does_not_consult_the_source() -> TestResult {
    let bucket = "odm-get-versioned";
    let env = configured_env(bucket, |_| {}).await?;
    env.client
        .put_bucket_versioning()
        .bucket(bucket)
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await?;
    let key = "versioned/doc.bin";
    let body = payload(1024);
    env.seed_source(SOURCE_BUCKET, &[SeedObject::new(key, body.clone())]);

    let err = env
        .client
        .get_object()
        .bucket(bucket)
        .key(key)
        .version_id("11111111-2222-4333-8444-555555555555")
        .send()
        .await
        .expect_err("a version read cannot be answered by the source");
    assert!(
        matches!(err.code(), Some("NoSuchVersion") | Some("NoSuchKey")),
        "unexpected error: {err:?}"
    );
    assert_eq!(env.source.count_requests(Operation::HeadObject, key), 0);
    assert_eq!(env.source.count_requests(Operation::GetObject, key), 0);
    env.assert_local_absent(bucket, key).await;

    // The same key without versionId is still migrated: the gate is per request.
    let response = env.raw_get(bucket, key).await?;
    assert_eq!(response.status, 200, "{}", String::from_utf8_lossy(&response.body));
    assert_eq!(response.header(ODM_RESPONSE_HEADER), Some("source"));
    assert_eq!(response.body, body);
    assert_eq!(env.source.count_requests(Operation::GetObject, key), 1);
    Ok(())
}

#[tokio::test]
async fn get_after_disable_does_not_consult_the_source() -> TestResult {
    let bucket = "odm-get-disabled";
    let env = configured_env(bucket, |_| {}).await?;
    let key = "disabled/doc.bin";
    env.seed_source(SOURCE_BUCKET, &[SeedObject::new(key, payload(1024))]);

    let response = env.disable(bucket).await?;
    assert_eq!(response.status, 204, "{}", response.body);

    let err = env
        .client
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .expect_err("a disabled bucket answers locally");
    assert_eq!(err.code(), Some("NoSuchKey"), "{err:?}");
    assert_eq!(env.source.count_requests(Operation::HeadObject, key), 0);
    assert_eq!(env.source.count_requests(Operation::GetObject, key), 0);
    env.assert_local_absent(bucket, key).await;
    Ok(())
}

/// A HEAD miss is answered from the source but must not store anything: the
/// key stays absent locally, so a second HEAD consults the source again. This
/// is the smoke-lane guard for the HEAD passthrough (rustfs/backlog#2155).
#[tokio::test]
async fn head_miss_answers_from_the_source_without_persisting() -> TestResult {
    let bucket = "odm-head-passthrough";
    let env = configured_env(bucket, |_| {}).await?;
    let key = "head/report.bin";
    let body = payload(32 * 1024);
    env.seed_source(SOURCE_BUCKET, &[SeedObject::new(key, body.clone())]);

    let head = env.raw_object_request(http::Method::HEAD, bucket, key, &[]).await?;
    assert_eq!(head.status, 200, "{}", String::from_utf8_lossy(&head.body));
    assert_eq!(head.header(ODM_RESPONSE_HEADER), Some("source"), "a source answer is marked");
    assert_eq!(head.header("content-length"), Some(body.len().to_string().as_str()));
    assert!(head.body.is_empty(), "a HEAD answer carries no body");
    assert_eq!(env.source.count_requests(Operation::HeadObject, key), 1);
    assert_eq!(env.source.count_requests(Operation::GetObject, key), 0, "a HEAD must never pull the body");
    env.assert_local_absent(bucket, key).await;

    let again = env.raw_object_request(http::Method::HEAD, bucket, key, &[]).await?;
    assert_eq!(again.status, 200, "{}", String::from_utf8_lossy(&again.body));
    assert_eq!(
        env.source.count_requests(Operation::HeadObject, key),
        2,
        "nothing was written back, so the second HEAD consults the source again"
    );
    assert_eq!(env.source.count_requests(Operation::GetObject, key), 0);
    env.assert_local_absent(bucket, key).await;
    Ok(())
}
