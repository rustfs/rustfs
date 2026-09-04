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

//! Optional merged `ListObjectsV2` (`policy.list_through`, ODM-17,
//! rustfs/backlog#2164): full pagination over a source and a local namespace,
//! common-prefix union under a delimiter, the continuation-token contract, and
//! the two `source_error` behaviours when the source listing fails.

use super::common::{BoxError, OdmSourceSpec, OdmTestEnv, SeedObject, start_configured_env};
use crate::fake_s3_target::{FaultAction, Operation};
use aws_sdk_s3::types::{BucketVersioningStatus, VersioningConfiguration};
use bytes::Bytes;

type TestResult = Result<(), BoxError>;

const SOURCE_BUCKET: &str = "odm-list-source";
const LIST_HEADER: &str = "x-rustfs-on-demand-migration-list";

/// Byte lengths that tell a local object from a source one in a listing.
const SOURCE_BODY_LEN: usize = 3;
const LOCAL_BODY_LEN: usize = 11;

fn body(len: usize) -> Bytes {
    vec![b'x'; len].into()
}

/// RustFS migrating `bucket` from `SOURCE_BUCKET` with `list_through` on.
async fn list_through_env(bucket: &str, adjust: impl FnOnce(&mut OdmSourceSpec)) -> Result<OdmTestEnv, BoxError> {
    start_configured_env(bucket, SOURCE_BUCKET, |spec| {
        spec.policy.list_through = true;
        adjust(spec);
    })
    .await
}

/// Every key the bucket lists, walked through the merged continuation token.
/// Also returns the size each page reported per key and the page sizes, so a
/// caller can assert who won a shared key and that no page exceeded `max_keys`.
async fn walk_listing(
    env: &OdmTestEnv,
    bucket: &str,
    delimiter: Option<&str>,
    max_keys: i32,
) -> Result<(Vec<(String, i64)>, Vec<String>, Vec<usize>), BoxError> {
    let mut objects = Vec::new();
    let mut prefixes = Vec::new();
    let mut page_sizes = Vec::new();
    let mut token: Option<String> = None;
    for _ in 0..1000 {
        let page = env
            .client
            .list_objects_v2()
            .bucket(bucket)
            .max_keys(max_keys)
            .set_delimiter(delimiter.map(str::to_string))
            .set_continuation_token(token.take())
            .send()
            .await?;
        let listed = page.contents().len() + page.common_prefixes().len();
        page_sizes.push(listed);
        for object in page.contents() {
            objects.push((object.key().unwrap_or_default().to_string(), object.size().unwrap_or_default()));
        }
        for prefix in page.common_prefixes() {
            prefixes.push(prefix.prefix().unwrap_or_default().to_string());
        }
        if !page.is_truncated().unwrap_or(false) {
            return Ok((objects, prefixes, page_sizes));
        }
        token = Some(
            page.next_continuation_token()
                .ok_or("truncated page without a continuation token")?
                .to_string(),
        );
    }
    Err("merged listing did not terminate".into())
}

#[tokio::test]
async fn list_through_merges_the_whole_namespace_across_full_pagination() -> TestResult {
    let bucket = "odm-list-merge";
    let env = list_through_env(bucket, |_| {}).await?;

    // 2000 source keys, 80 of them also local, plus 10 local-only keys that
    // interleave between source keys ("obj-00010x" sorts after "obj-00010").
    let source_keys: Vec<String> = (0..2000).map(|index| format!("obj-{index:05}")).collect();
    let seeds: Vec<SeedObject> = source_keys
        .iter()
        .map(|key| SeedObject::new(key.clone(), body(SOURCE_BODY_LEN)))
        .collect();
    env.seed_source(SOURCE_BUCKET, &seeds);

    let shared: Vec<String> = source_keys.iter().step_by(25).cloned().collect();
    let local_only: Vec<String> = (0..10).map(|index| format!("obj-{:05}x", index * 7)).collect();
    for key in shared.iter().chain(local_only.iter()) {
        env.client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(body(LOCAL_BODY_LEN).into())
            .send()
            .await?;
    }

    let max_keys = 97;
    let (objects, prefixes, page_sizes) = walk_listing(&env, bucket, None, max_keys).await?;
    assert!(prefixes.is_empty(), "no delimiter means no common prefixes");

    let mut expected: Vec<String> = source_keys.iter().chain(local_only.iter()).cloned().collect();
    expected.sort();
    expected.dedup();
    let listed: Vec<String> = objects.iter().map(|(key, _)| key.clone()).collect();
    assert_eq!(listed, expected, "the merged listing is the sorted, deduplicated union");
    assert!(
        page_sizes.iter().all(|size| *size <= max_keys as usize),
        "no page may exceed max_keys: {page_sizes:?}"
    );

    let shared_sizes: Vec<i64> = objects
        .iter()
        .filter(|(key, _)| shared.contains(key))
        .map(|(_, size)| *size)
        .collect();
    assert_eq!(shared_sizes.len(), shared.len(), "every shared key is listed exactly once");
    assert!(
        shared_sizes.iter().all(|size| *size == LOCAL_BODY_LEN as i64),
        "the local object wins a key both sides hold"
    );
    let source_sizes: Vec<i64> = objects
        .iter()
        .filter(|(key, _)| !shared.contains(key) && !local_only.contains(key))
        .map(|(_, size)| *size)
        .collect();
    assert!(
        source_sizes.iter().all(|size| *size == SOURCE_BODY_LEN as i64),
        "source-only keys report the source's own size"
    );
    Ok(())
}

#[tokio::test]
async fn list_through_unions_common_prefixes_under_a_delimiter() -> TestResult {
    let bucket = "odm-list-delimiter";
    let env = list_through_env(bucket, |_| {}).await?;

    env.seed_source(
        SOURCE_BUCKET,
        &[
            SeedObject::new("p1/a", body(SOURCE_BODY_LEN)),
            SeedObject::new("p1/b", body(SOURCE_BODY_LEN)),
            SeedObject::new("p2/a", body(SOURCE_BODY_LEN)),
            SeedObject::new("top-s", body(SOURCE_BODY_LEN)),
        ],
    );
    for key in ["p1/c", "p3/a", "top-l"] {
        env.client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(body(LOCAL_BODY_LEN).into())
            .send()
            .await?;
    }

    // A page size of two forces the prefix union to survive page boundaries.
    let (objects, prefixes, page_sizes) = walk_listing(&env, bucket, Some("/"), 2).await?;
    assert_eq!(prefixes, vec!["p1/", "p2/", "p3/"], "prefixes are unioned and deduplicated");
    let listed: Vec<String> = objects.iter().map(|(key, _)| key.clone()).collect();
    assert_eq!(listed, vec!["top-l", "top-s"]);
    assert!(page_sizes.iter().all(|size| *size <= 2), "{page_sizes:?}");
    Ok(())
}

#[tokio::test]
async fn list_through_propagates_a_source_listing_failure() -> TestResult {
    let bucket = "odm-list-propagate";
    let env = list_through_env(bucket, |_| {}).await?;
    env.seed_source(SOURCE_BUCKET, &[SeedObject::new("remote", body(SOURCE_BODY_LEN))]);
    env.client
        .put_object()
        .bucket(bucket)
        .key("local")
        .body(body(LOCAL_BODY_LEN).into())
        .send()
        .await?;

    env.source
        .inject(Operation::ListObjectsV2, FaultAction::ResponseStatus(503), 1);
    let failure = env
        .client
        .list_objects_v2()
        .bucket(bucket)
        .send()
        .await
        .expect_err("propagate must surface the source failure");
    let failure = failure.into_service_error();
    assert_eq!(failure.meta().code(), Some("SourceUnavailable"), "{failure:?}");

    // The next listing sees a healthy source again and merges both sides.
    let (objects, _, _) = walk_listing(&env, bucket, None, 100).await?;
    let listed: Vec<String> = objects.iter().map(|(key, _)| key.clone()).collect();
    assert_eq!(listed, vec!["local", "remote"]);
    Ok(())
}

#[tokio::test]
async fn list_through_degrades_to_local_only_under_the_not_found_policy() -> TestResult {
    let bucket = "odm-list-degrade";
    let env = list_through_env(bucket, |spec| spec.policy.source_error = "not_found".to_string()).await?;
    env.seed_source(SOURCE_BUCKET, &[SeedObject::new("remote", body(SOURCE_BODY_LEN))]);
    env.client
        .put_object()
        .bucket(bucket)
        .key("local")
        .body(body(LOCAL_BODY_LEN).into())
        .send()
        .await?;

    env.source
        .inject(Operation::ListObjectsV2, FaultAction::ResponseStatus(503), 1);
    let degraded = env.raw_list_objects_v2(bucket, "max-keys=100").await?;
    assert_eq!(degraded.status, 200, "{}", String::from_utf8_lossy(&degraded.body));
    assert_eq!(
        degraded.header(LIST_HEADER),
        Some("local_only"),
        "a degraded listing must say so in the response header"
    );
    let xml = String::from_utf8_lossy(&degraded.body).to_string();
    assert!(xml.contains("<Key>local</Key>"), "{xml}");
    assert!(!xml.contains("<Key>remote</Key>"), "a degraded listing shows local state only: {xml}");

    let healthy = env.raw_list_objects_v2(bucket, "max-keys=100").await?;
    assert_eq!(healthy.status, 200);
    assert_eq!(healthy.header(LIST_HEADER), None, "a healthy merge carries no degradation marker");
    assert!(String::from_utf8_lossy(&healthy.body).contains("<Key>remote</Key>"));
    Ok(())
}

#[tokio::test]
async fn list_through_rejects_a_tampered_continuation_token() -> TestResult {
    let bucket = "odm-list-token";
    let env = list_through_env(bucket, |_| {}).await?;
    env.seed_source(
        SOURCE_BUCKET,
        &[
            SeedObject::new("a", body(SOURCE_BODY_LEN)),
            SeedObject::new("b", body(SOURCE_BODY_LEN)),
            SeedObject::new("c", body(SOURCE_BODY_LEN)),
        ],
    );

    let page = env.client.list_objects_v2().bucket(bucket).max_keys(1).send().await?;
    let token = page.next_continuation_token().ok_or("first page must be truncated")?;
    let decoded = String::from_utf8(base64_simd::STANDARD.decode_to_vec(token.as_bytes())?)?;
    assert!(decoded.contains("\"t\":\"odm-list\""), "the merged token is an envelope: {decoded}");

    let tampered = base64_simd::STANDARD.encode_to_string(decoded.replace("\"v\":1", "\"v\":2").as_bytes());
    let rejected = env
        .raw_list_objects_v2(bucket, &format!("continuation-token={tampered}"))
        .await?;
    assert_eq!(
        rejected.status,
        400,
        "a bumped token version is a client error: {}",
        String::from_utf8_lossy(&rejected.body)
    );
    Ok(())
}

#[tokio::test]
async fn a_merged_token_keeps_paginating_after_list_through_is_turned_off() -> TestResult {
    let bucket = "odm-list-token-off";
    let env = list_through_env(bucket, |_| {}).await?;
    env.seed_source(
        SOURCE_BUCKET,
        &[
            SeedObject::new("s1", body(SOURCE_BODY_LEN)),
            SeedObject::new("s2", body(SOURCE_BODY_LEN)),
        ],
    );
    for key in ["l1", "l2"] {
        env.client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(body(LOCAL_BODY_LEN).into())
            .send()
            .await?;
    }

    let page = env.client.list_objects_v2().bucket(bucket).max_keys(1).send().await?;
    assert_eq!(page.contents()[0].key(), Some("l1"));
    let token = page
        .next_continuation_token()
        .ok_or("first page must be truncated")?
        .to_string();

    let mut spec = env.fake_source_spec(SOURCE_BUCKET);
    spec.policy.list_through = false;
    env.configure_and_wait(bucket, &spec).await?;

    let resumed = env
        .client
        .list_objects_v2()
        .bucket(bucket)
        .max_keys(10)
        .continuation_token(token)
        .send()
        .await?;
    let listed: Vec<&str> = resumed.contents().iter().filter_map(|object| object.key()).collect();
    assert_eq!(listed, vec!["l2"], "a merged token falls back to its local cursor");
    Ok(())
}

#[tokio::test]
async fn a_local_delete_marker_hides_the_source_key_from_a_merged_listing() -> TestResult {
    let bucket = "odm-list-delete-marker";
    let env = list_through_env(bucket, |_| {}).await?;
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
    env.seed_source(
        SOURCE_BUCKET,
        &[
            SeedObject::new("kept", body(SOURCE_BODY_LEN)),
            SeedObject::new("shadowed", body(SOURCE_BODY_LEN)),
        ],
    );

    env.client
        .put_object()
        .bucket(bucket)
        .key("shadowed")
        .body(body(LOCAL_BODY_LEN).into())
        .send()
        .await?;
    env.client.delete_object().bucket(bucket).key("shadowed").send().await?;

    let (objects, _, _) = walk_listing(&env, bucket, None, 100).await?;
    let listed: Vec<String> = objects.iter().map(|(key, _)| key.clone()).collect();
    assert_eq!(
        listed,
        vec!["kept"],
        "a local delete marker shadows the source key the same way it does on GET"
    );
    Ok(())
}
