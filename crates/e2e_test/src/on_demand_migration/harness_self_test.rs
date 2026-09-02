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

//! Self-test of the ODM harness (rustfs/backlog#2151): the fake source's
//! migration-facing surface (ListObjectsV2 paging, `Range`, unversioned
//! buckets, metadata replay, fault actions) and the two-server environment.
//! No ODM behavior is exercised here.

use super::common::{OdmTestEnv, SeedObject, fake_source_client, start_source_rustfs};
use crate::fake_s3_target::{BucketMode, FakeS3Target, FakeS3TargetOptions, FaultAction, Operation, SeedMetadata};
use aws_sdk_s3::Client;
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::primitives::{ByteStream, DateTime};
use bytes::Bytes;
use std::collections::BTreeSet;
use std::time::{Duration, Instant};

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

const SOURCE_BUCKET: &str = "odm-source";

/// Position-dependent payload so a misaligned range read is caught.
fn payload(len: usize) -> Bytes {
    (0..len).map(|index| (index % 251) as u8).collect::<Vec<u8>>().into()
}

async fn fake_source() -> Result<(FakeS3Target, Client), Box<dyn std::error::Error + Send + Sync>> {
    let source = FakeS3Target::start().await?;
    source.create_bucket(SOURCE_BUCKET);
    let client = fake_source_client(&source);
    Ok((source, client))
}

/// Full ListObjectsV2 traversal. Returns `(keys, common prefixes, pages)` and
/// checks the page shape on the way: every page except the last is full and
/// truncated, the last carries no continuation token.
async fn list_all(
    client: &Client,
    prefix: Option<&str>,
    delimiter: Option<&str>,
    start_after: Option<&str>,
    max_keys: i32,
) -> Result<(Vec<String>, Vec<String>, usize), Box<dyn std::error::Error + Send + Sync>> {
    let mut keys = Vec::new();
    let mut prefixes = Vec::new();
    let mut pages = 0usize;
    let mut token: Option<String> = None;
    loop {
        let page = client
            .list_objects_v2()
            .bucket(SOURCE_BUCKET)
            .set_prefix(prefix.map(str::to_string))
            .set_delimiter(delimiter.map(str::to_string))
            .set_start_after(start_after.map(str::to_string))
            .max_keys(max_keys)
            .set_continuation_token(token.clone())
            .send()
            .await?;
        pages += 1;
        let page_keys: Vec<String> = page
            .contents()
            .iter()
            .filter_map(|object| object.key().map(str::to_string))
            .collect();
        let page_prefixes: Vec<String> = page
            .common_prefixes()
            .iter()
            .filter_map(|common| common.prefix().map(str::to_string))
            .collect();
        let entries = page_keys.len() + page_prefixes.len();
        assert_eq!(page.key_count(), Some(entries as i32), "KeyCount must count keys and prefixes");
        assert_eq!(page.continuation_token(), token.as_deref(), "the request token must be echoed");
        keys.extend(page_keys);
        prefixes.extend(page_prefixes);
        if page.is_truncated() == Some(true) {
            assert_eq!(entries as i32, max_keys, "every truncated page must be full");
            token = Some(
                page.next_continuation_token()
                    .expect("truncated page must carry a continuation token")
                    .to_string(),
            );
        } else {
            assert!(page.next_continuation_token().is_none(), "final page must not carry a token");
            return Ok((keys, prefixes, pages));
        }
    }
}

#[tokio::test]
async fn fake_source_list_objects_v2_paginates_with_delimiter() -> TestResult {
    let (source, client) = fake_source().await?;
    let mut expected_keys = BTreeSet::new();
    for directory in 0..30 {
        for file in 0..30 {
            expected_keys.insert(format!("d{directory:02}/k{file:03}"));
        }
    }
    for index in 0..100 {
        expected_keys.insert(format!("top-{index:03}"));
    }
    assert_eq!(expected_keys.len(), 1000);
    for key in &expected_keys {
        source.put_seed_object(SOURCE_BUCKET, key.clone(), Bytes::from(key.clone()), &SeedMetadata::new());
    }
    // A key whose current version is a delete marker must stay hidden.
    client
        .put_object()
        .bucket(SOURCE_BUCKET)
        .key("hidden/marker")
        .body(ByteStream::from_static(b"gone"))
        .send()
        .await?;
    client
        .delete_object()
        .bucket(SOURCE_BUCKET)
        .key("hidden/marker")
        .send()
        .await?;
    let expected_sorted: Vec<String> = expected_keys.iter().cloned().collect();
    let expected_prefixes: Vec<String> = (0..30).map(|directory| format!("d{directory:02}/")).collect();
    let expected_top: Vec<String> = (0..100).map(|index| format!("top-{index:03}")).collect();

    // Flat traversal in byte order, 1000 keys in pages of 7.
    let (keys, prefixes, pages) = list_all(&client, None, None, None, 7).await?;
    assert_eq!(keys, expected_sorted);
    assert!(prefixes.is_empty());
    assert_eq!(pages, 143);

    // Delimiter folding: 30 common prefixes then 100 top-level keys, pages of 7.
    let (keys, prefixes, pages) = list_all(&client, None, Some("/"), None, 7).await?;
    assert_eq!(prefixes, expected_prefixes);
    assert_eq!(keys, expected_top);
    assert_eq!(pages, 19);

    // Empty prefix equals no prefix.
    let (keys, _, _) = list_all(&client, Some(""), None, None, 1000).await?;
    assert_eq!(keys, expected_sorted);

    // No match: empty, not truncated, no token.
    let (keys, prefixes, pages) = list_all(&client, Some("zzz/"), Some("/"), None, 7).await?;
    assert!(keys.is_empty() && prefixes.is_empty());
    assert_eq!(pages, 1);
    let (keys, _, _) = list_all(&client, Some("hidden/"), None, None, 7).await?;
    assert!(keys.is_empty(), "a current delete marker must hide its key");

    // Exact page boundary: 30 keys under one directory, max-keys=30 -> one
    // untruncated page.
    let (keys, prefixes, pages) = list_all(&client, Some("d05/"), Some("/"), None, 30).await?;
    assert_eq!(keys.len(), 30);
    assert!(prefixes.is_empty());
    assert_eq!(pages, 1);

    // start-after skips keys at or before the marker.
    let (keys, _, _) = list_all(&client, None, None, Some("top-097"), 1000).await?;
    assert_eq!(keys, ["top-098", "top-099"]);

    // max-keys is clamped to 1000; exactly 1000 keys fit in one page.
    let (keys, _, pages) = list_all(&client, None, None, None, 5000).await?;
    assert_eq!(keys.len(), 1000);
    assert_eq!(pages, 1);

    let listings: Vec<_> = source
        .requests()
        .into_iter()
        .filter(|record| record.operation == Operation::ListObjectsV2)
        .collect();
    assert!(listings.len() >= 143 + 19);
    assert!(listings.iter().any(|record| record.prefix.as_deref() == Some("d05/")));
    assert!(
        listings.iter().any(|record| record.continuation_token.is_some()),
        "resumed pages must journal their continuation token"
    );
    assert!(listings.iter().all(|record| record.user_agent.is_some()));
    source.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn fake_source_range_get_variants_and_416() -> TestResult {
    let (source, client) = fake_source().await?;
    let body = payload(1000);
    source.put_seed_object(SOURCE_BUCKET, "ranged", body.clone(), &SeedMetadata::new());

    for (range, expected_range, expected_slice) in [
        ("bytes=10-19", "bytes 10-19/1000", &body[10..20]),
        ("bytes=990-", "bytes 990-999/1000", &body[990..]),
        ("bytes=-5", "bytes 995-999/1000", &body[995..]),
        ("bytes=0-5000", "bytes 0-999/1000", &body[..]),
    ] {
        let output = client
            .get_object()
            .bucket(SOURCE_BUCKET)
            .key("ranged")
            .range(range)
            .send()
            .await?;
        assert_eq!(output.content_range(), Some(expected_range), "{range}");
        assert_eq!(output.accept_ranges(), Some("bytes"), "{range}");
        assert_eq!(output.content_length(), Some(expected_slice.len() as i64), "{range}");
        let collected = output.body.collect().await?.into_bytes();
        assert_eq!(collected.as_ref(), expected_slice, "{range}");
    }
    let head = client
        .head_object()
        .bucket(SOURCE_BUCKET)
        .key("ranged")
        .range("bytes=10-19")
        .send()
        .await?;
    assert_eq!(head.content_range(), Some("bytes 10-19/1000"));
    assert_eq!(head.content_length(), Some(10));

    for range in ["bytes=1000-", "bytes=-0"] {
        let error = client
            .get_object()
            .bucket(SOURCE_BUCKET)
            .key("ranged")
            .range(range)
            .send()
            .await
            .expect_err("unsatisfiable range must fail");
        let response = error.raw_response().expect("416 must retain the raw response");
        assert_eq!(response.status().as_u16(), 416, "{range}");
        assert_eq!(response.headers().get("content-range"), Some("bytes */1000"), "{range}");
        assert_eq!(error.code(), Some("InvalidRange"), "{range}");
    }

    let ranged = source
        .requests()
        .into_iter()
        .find(|record| record.operation == Operation::GetObject && record.range.as_deref() == Some("bytes=10-19"))
        .expect("the Range header must be journaled verbatim");
    assert_eq!(ranged.key.as_deref(), Some("ranged"));
    assert!(source.count_requests(Operation::GetObject, "ranged") >= 6);
    source.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn fake_source_unversioned_bucket_overwrites_and_deletes() -> TestResult {
    let (source, client) = fake_source().await?;
    source.create_bucket_with_mode("plain-source", BucketMode::Unversioned);
    let versioning = client.get_bucket_versioning().bucket("plain-source").send().await?;
    assert!(versioning.status().is_none(), "unversioned bucket must report no versioning status");

    let first = client
        .put_object()
        .bucket("plain-source")
        .key("doc")
        .body(ByteStream::from_static(b"first"))
        .send()
        .await?;
    assert!(first.version_id().is_none());
    let second = client
        .put_object()
        .bucket("plain-source")
        .key("doc")
        .body(ByteStream::from_static(b"second"))
        .send()
        .await?;
    assert!(second.version_id().is_none());
    let get = client.get_object().bucket("plain-source").key("doc").send().await?;
    assert!(get.version_id().is_none(), "GET must not return x-amz-version-id");
    assert_eq!(get.body.collect().await?.into_bytes().as_ref(), b"second");
    let head = client.head_object().bucket("plain-source").key("doc").send().await?;
    assert!(head.version_id().is_none(), "HEAD must not return x-amz-version-id");
    assert_eq!(source.stored_versions("plain-source", "doc").len(), 1, "overwrite must replace in place");

    let deleted = client.delete_object().bucket("plain-source").key("doc").send().await?;
    assert!(deleted.delete_marker().is_none() && deleted.version_id().is_none());
    let missing = client
        .get_object()
        .bucket("plain-source")
        .key("doc")
        .send()
        .await
        .expect_err("deleted object must be gone");
    assert_eq!(missing.raw_response().map(|response| response.status().as_u16()), Some(404));
    assert_eq!(missing.code(), Some("NoSuchKey"));
    let missing_head = client
        .head_object()
        .bucket("plain-source")
        .key("doc")
        .send()
        .await
        .expect_err("deleted object must fail HEAD");
    assert_eq!(missing_head.raw_response().map(|response| response.status().as_u16()), Some(404));
    assert!(source.stored_versions("plain-source", "doc").is_empty(), "DELETE must not leave a marker");

    // The versioned bucket on the same target keeps its version ids.
    let versioned = client
        .put_object()
        .bucket(SOURCE_BUCKET)
        .key("doc")
        .body(ByteStream::from_static(b"versioned"))
        .send()
        .await?;
    assert!(versioned.version_id().is_some());
    source.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn fake_source_replays_standard_and_user_metadata() -> TestResult {
    let (source, client) = fake_source().await?;
    let body = payload(4096);
    let expected_etag = format!("\"{}\"", {
        use md5::Digest as _;
        hex_simd::encode_to_string(md5::Md5::digest(&body), hex_simd::AsciiCase::Lower)
    });
    // 2026-01-01T00:00:00Z rendered as an HTTP date by the SDK.
    let expires = DateTime::from_secs(1_767_225_600);
    client
        .put_object()
        .bucket(SOURCE_BUCKET)
        .key("meta")
        .body(ByteStream::from(body.clone()))
        .content_type("application/x-odm")
        .content_encoding("gzip")
        .content_disposition("attachment; filename=\"meta.bin\"")
        .content_language("en-US")
        .cache_control("max-age=60")
        .expires(expires)
        .metadata("Foo-Bar", "mixed case name")
        .metadata("UPPER", "upper name")
        .metadata("already-lower", "lower name")
        .send()
        .await?;

    let head = client.head_object().bucket(SOURCE_BUCKET).key("meta").send().await?;
    let get = client.get_object().bucket(SOURCE_BUCKET).key("meta").send().await?;
    for (label, content_type, content_encoding, content_disposition, content_language, cache_control, expires_string, e_tag) in [
        (
            "HEAD",
            head.content_type(),
            head.content_encoding(),
            head.content_disposition(),
            head.content_language(),
            head.cache_control(),
            head.expires_string(),
            head.e_tag(),
        ),
        (
            "GET",
            get.content_type(),
            get.content_encoding(),
            get.content_disposition(),
            get.content_language(),
            get.cache_control(),
            get.expires_string(),
            get.e_tag(),
        ),
    ] {
        assert_eq!(content_type, Some("application/x-odm"), "{label}");
        assert_eq!(content_encoding, Some("gzip"), "{label}");
        assert_eq!(content_disposition, Some("attachment; filename=\"meta.bin\""), "{label}");
        assert_eq!(content_language, Some("en-US"), "{label}");
        assert_eq!(cache_control, Some("max-age=60"), "{label}");
        assert_eq!(expires_string, Some("Thu, 01 Jan 2026 00:00:00 GMT"), "{label}");
        assert_eq!(e_tag, Some(expected_etag.as_str()), "{label}");
    }
    for metadata in [head.metadata(), get.metadata()] {
        let metadata = metadata.expect("user metadata must be replayed");
        assert_eq!(metadata.get("foo-bar").map(String::as_str), Some("mixed case name"));
        assert_eq!(metadata.get("upper").map(String::as_str), Some("upper name"));
        assert_eq!(metadata.get("already-lower").map(String::as_str), Some("lower name"));
        assert!(!metadata.contains_key("Foo-Bar") && !metadata.contains_key("UPPER"));
    }
    assert!(head.last_modified().is_some());
    assert_eq!(head.last_modified(), get.last_modified());
    assert_eq!(head.content_length(), Some(4096));
    assert_eq!(get.body.collect().await?.into_bytes(), body);

    // Seeded objects replay the same way.
    let seeded_etag = source.put_seed_object(
        SOURCE_BUCKET,
        "seeded",
        Bytes::from_static(b"seeded"),
        &SeedMetadata::new()
            .content_type("text/plain")
            .content_encoding("identity")
            .cache_control("no-store")
            .user_metadata("Origin", "seed"),
    );
    let seeded = client.head_object().bucket(SOURCE_BUCKET).key("seeded").send().await?;
    assert_eq!(seeded.e_tag(), Some(format!("\"{seeded_etag}\"").as_str()));
    assert_eq!(seeded.content_type(), Some("text/plain"));
    assert_eq!(seeded.content_encoding(), Some("identity"));
    assert_eq!(seeded.cache_control(), Some("no-store"));
    assert_eq!(
        seeded
            .metadata()
            .and_then(|metadata| metadata.get("origin"))
            .map(String::as_str),
        Some("seed")
    );
    source.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn fake_source_fault_actions_truncate_stall_and_status() -> TestResult {
    let (source, client) = fake_source().await?;
    let body = payload(4096);
    source.put_seed_object(SOURCE_BUCKET, "faulty", body.clone(), &SeedMetadata::new());

    // TruncateBodyAt: headers promise 4096 bytes, the body ends after 100.
    source.inject_for_key(Operation::GetObject, "faulty", FaultAction::TruncateBodyAt(100), 1);
    let truncated = client.get_object().bucket(SOURCE_BUCKET).key("faulty").send().await?;
    assert_eq!(truncated.content_length(), Some(4096));
    let short_read = truncated
        .body
        .collect()
        .await
        .expect_err("a truncated body must fail to collect");
    let short_read = short_read.to_string();
    assert!(!short_read.is_empty());

    // ResponseStatus: arbitrary status with the matching S3 error code.
    for (code, expected_code) in [
        (429u16, "SlowDown"),
        (404, "NoSuchKey"),
        (500, "InternalError"),
        (503, "ServiceUnavailable"),
    ] {
        source.inject(Operation::GetObject, FaultAction::ResponseStatus(code), 1);
        let error = client
            .get_object()
            .bucket(SOURCE_BUCKET)
            .key("faulty")
            .send()
            .await
            .expect_err("scripted status must fail");
        assert_eq!(error.raw_response().map(|response| response.status().as_u16()), Some(code));
        assert_eq!(error.code(), Some(expected_code));
    }

    // Stall: the fully computed response is held before its first byte.
    source.inject(Operation::HeadObject, FaultAction::Stall(Duration::from_millis(400)), 1);
    let started = Instant::now();
    let stalled = client.head_object().bucket(SOURCE_BUCKET).key("faulty").send().await?;
    assert!(started.elapsed() >= Duration::from_millis(350), "stall must delay the first byte");
    assert_eq!(stalled.content_length(), Some(4096));
    let unstalled_started = Instant::now();
    client.head_object().bucket(SOURCE_BUCKET).key("faulty").send().await?;
    assert!(unstalled_started.elapsed() < Duration::from_millis(350), "stall is consumed once");

    // The object is intact once the script is drained.
    let intact = client.get_object().bucket(SOURCE_BUCKET).key("faulty").send().await?;
    assert_eq!(intact.body.collect().await?.into_bytes(), body);

    assert_eq!(source.count_requests(Operation::GetObject, "faulty"), 6);
    assert_eq!(source.count_requests(Operation::HeadObject, "faulty"), 2);
    assert_eq!(source.count_requests(Operation::GetObject, "other"), 0);
    let records = source.requests();
    assert!(
        records.iter().all(|record| record
            .user_agent
            .as_deref()
            .is_some_and(|agent| agent.contains("aws-sdk-rust"))),
        "the SDK user agent must be journaled"
    );
    assert!(
        records
            .iter()
            .any(|record| record.fault == Some(FaultAction::TruncateBodyAt(100)))
    );
    assert!(
        records
            .iter()
            .any(|record| record.fault == Some(FaultAction::Stall(Duration::from_millis(400))))
    );
    source.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn fake_source_raised_object_cap_accepts_large_put() -> TestResult {
    let source = FakeS3Target::start_with_options(FakeS3TargetOptions {
        max_object_bytes: 96 * 1024 * 1024,
    })
    .await?;
    source.create_bucket(SOURCE_BUCKET);
    let client = fake_source_client(&source);
    let len = 64 * 1024 * 1024 + 1;
    client
        .put_object()
        .bucket(SOURCE_BUCKET)
        .key("large")
        .body(ByteStream::from(vec![7u8; len]))
        .send()
        .await?;
    let head = client.head_object().bucket(SOURCE_BUCKET).key("large").send().await?;
    assert_eq!(head.content_length(), Some(len as i64));
    let tail = client
        .get_object()
        .bucket(SOURCE_BUCKET)
        .key("large")
        .range("bytes=-1")
        .send()
        .await?;
    assert_eq!(tail.content_range(), Some(format!("bytes {}-{}/{len}", len - 1, len - 1).as_str()));
    source.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn odm_env_starts_rustfs_and_fake_source() -> TestResult {
    let env = OdmTestEnv::start().await?;
    env.source.create_bucket(SOURCE_BUCKET);
    let local_bucket = "odm-local";
    env.rustfs.create_test_bucket(local_bucket).await?;

    let etags = env.seed_source(
        SOURCE_BUCKET,
        &[
            SeedObject::new("seed/a", Bytes::from_static(b"alpha")),
            SeedObject::new("seed/b", Bytes::from_static(b"beta"))
                .with_metadata(SeedMetadata::new().content_type("text/plain").user_metadata("Kind", "seed")),
        ],
    );
    assert_eq!(etags.len(), 2);
    assert!(env.source.requests().is_empty(), "seeding must not touch the journal");
    let source_client = env.source_client();
    let seeded = source_client.head_object().bucket(SOURCE_BUCKET).key("seed/b").send().await?;
    assert_eq!(seeded.content_type(), Some("text/plain"));
    assert_eq!(seeded.e_tag(), Some(format!("\"{}\"", etags[1]).as_str()));
    assert_eq!(env.source.count_requests(Operation::HeadObject, "seed/b"), 1);

    env.assert_local_absent(local_bucket, "seed/a").await;
    env.client
        .put_object()
        .bucket(local_bucket)
        .key("seed/a")
        .body(ByteStream::from_static(b"alpha"))
        .send()
        .await?;
    env.assert_local_present(local_bucket, "seed/a", b"alpha").await;
    env.assert_local_absent(local_bucket, "seed/b").await;

    let spec = env.fake_source_spec(SOURCE_BUCKET).to_json();
    assert_eq!(spec["version"], 1);
    assert_eq!(spec["enabled"], true);
    assert_eq!(spec["source"]["provider"], "s3");
    assert_eq!(spec["source"]["endpoint"], env.source.endpoint());
    assert_eq!(spec["source"]["bucket"], SOURCE_BUCKET);
    assert_eq!(spec["source"]["credentials"]["secret_key"], "fake-secret");
    assert_eq!(spec["policy"]["source_timeout"]["first_byte_ms"], 15_000);
    assert!(spec["policy"]["bandwidth_limit_bytes_per_sec"].is_null());
    let debug = format!("{:?}", env.fake_source_spec(SOURCE_BUCKET));
    assert!(!debug.contains("fake-secret"), "Debug output must redact the secret");
    Ok(())
}

#[tokio::test]
async fn start_source_rustfs_round_trips_put_get() -> TestResult {
    let env = OdmTestEnv::start().await?;
    let source = start_source_rustfs().await?;
    assert_ne!(source.url, env.rustfs.url, "the source must be a separate instance");

    source.create_test_bucket(SOURCE_BUCKET).await?;
    let source_client = source.create_s3_client();
    let body = payload(70_000);
    let put = source_client
        .put_object()
        .bucket(SOURCE_BUCKET)
        .key("real/object")
        .body(ByteStream::from(body.clone()))
        .content_type("application/octet-stream")
        .send()
        .await?;
    assert!(put.e_tag().is_some());
    let get = source_client
        .get_object()
        .bucket(SOURCE_BUCKET)
        .key("real/object")
        .send()
        .await?;
    assert_eq!(get.content_type(), Some("application/octet-stream"));
    assert_eq!(get.body.collect().await?.into_bytes(), body);

    let visible_to_primary = env
        .client
        .list_buckets()
        .send()
        .await?
        .buckets()
        .iter()
        .any(|bucket| bucket.name() == Some(SOURCE_BUCKET));
    assert!(!visible_to_primary, "the two servers must not share state");
    let spec = super::common::OdmSourceSpec::for_rustfs_source(&source, SOURCE_BUCKET).to_json();
    assert_eq!(spec["source"]["provider"], "rustfs");
    assert_eq!(spec["source"]["endpoint"], source.url);
    Ok(())
}
