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

//! Concurrency limits of on-demand migration (rustfs/backlog#2158):
//! single-flight on one key, the `max_concurrent_pulls` ceiling, and a full
//! background pull queue.
//!
//! The point of each case is what the source is spared, so the source
//! journal (`count_requests`) carries the assertion in every one of them.

use super::common::{BoxError, OdmTestEnv, RawResponse, SeedObject, start_configured_env};
use crate::fake_s3_target::{FaultAction, Operation};
use aws_sdk_s3::types::{BucketVersioningStatus, VersioningConfiguration};
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use std::time::Duration;

type TestResult = Result<(), BoxError>;

const SOURCE_BUCKET: &str = "odm-concurrency-source";
/// Background pulls land after the response that queued them.
const SETTLE: Duration = Duration::from_secs(120);

fn payload(len: usize) -> Bytes {
    (0..len).map(|index| (index % 251) as u8).collect::<Vec<u8>>().into()
}

fn source_get_count(env: &OdmTestEnv, key: &str) -> usize {
    env.source.count_requests(Operation::GetObject, key)
}

/// Case 9: 32 concurrent misses on one key coalesce into a single-flight
/// pull. At most two source GETs are allowed: the leader plus one follower
/// that gave up waiting and streamed through.
#[tokio::test]
async fn test_odm_concurrent_misses_on_one_key_coalesce() -> TestResult {
    let bucket = "odm-concurrency-singleflight";
    let env = start_configured_env(bucket, SOURCE_BUCKET, |_| {}).await?;
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

    let key = "singleflight/asset.bin";
    let body = payload(512 * 1024);
    env.seed_source(SOURCE_BUCKET, &[SeedObject::new(key, body.clone())]);

    let responses: Vec<RawResponse> = futures::future::try_join_all((0..32).map(|_| env.raw_get(bucket, key))).await?;
    for (index, response) in responses.iter().enumerate() {
        assert_eq!(response.status, 200, "reader {index}: {}", String::from_utf8_lossy(&response.body));
        assert_eq!(response.body, body, "reader {index} received different bytes");
    }

    let source_gets = source_get_count(&env, key);
    assert!(
        (1..=2).contains(&source_gets),
        "32 concurrent misses must not become {source_gets} source GETs"
    );

    assert!(env.wait_local_listed(bucket, key, SETTLE).await?, "the leader stores the object");
    env.assert_local_present(bucket, key, &body).await;
    let versions = env.client.list_object_versions().bucket(bucket).prefix(key).send().await?;
    assert_eq!(
        versions.versions().len(),
        1,
        "the coalesced pull commits exactly one version: {:?}",
        versions.versions()
    );
    assert_eq!(
        source_get_count(&env, key),
        source_gets,
        "nothing pulls the object again once it is local"
    );
    Ok(())
}

/// Case 10: 64 misses on distinct keys never exceed `max_concurrent_pulls`
/// in flight, and all of them eventually land.
#[tokio::test]
async fn test_odm_concurrent_pulls_respect_the_configured_ceiling() -> TestResult {
    let bucket = "odm-concurrency-ceiling";
    const MAX_CONCURRENT_PULLS: u32 = 4;
    const KEYS: usize = 64;
    let env = start_configured_env(bucket, SOURCE_BUCKET, |spec| {
        spec.policy.max_concurrent_pulls = MAX_CONCURRENT_PULLS;
    })
    .await?;

    let body = payload(256 * 1024);
    let keys: Vec<String> = (0..KEYS).map(|index| format!("ceiling/object-{index:03}.bin")).collect();
    let seeds: Vec<SeedObject> = keys.iter().map(|key| SeedObject::new(key.clone(), body.clone())).collect();
    env.seed_source(SOURCE_BUCKET, &seeds);

    let reads = futures::future::try_join_all(keys.iter().map(|key| env.raw_get(bucket, key)));
    let (responses, peak_inflight) = env.peak_inflight_pulls(bucket, reads).await?;
    let responses = responses?;
    for (key, response) in keys.iter().zip(&responses) {
        assert_eq!(response.status, 200, "{key}: {}", String::from_utf8_lossy(&response.body));
        assert_eq!(response.body, body, "{key} received different bytes");
    }
    assert!(
        peak_inflight <= u64::from(MAX_CONCURRENT_PULLS),
        "in-flight pulls peaked at {peak_inflight}, above the configured {MAX_CONCURRENT_PULLS}"
    );
    assert!(
        peak_inflight >= 1,
        "the poll never observed a pull in flight, so the ceiling assertion proves nothing"
    );

    for key in &keys {
        assert!(env.wait_local_listed(bucket, key, SETTLE).await?, "{key} must be stored locally");
        assert_eq!(source_get_count(&env, key), 1, "{key} is pulled exactly once");
    }
    assert_eq!(env.status_counter(bucket, "/inflight_pulls").await?, 0, "every pull slot is released");
    Ok(())
}

/// Case 11: with a small background queue, a burst of Range reads overflows
/// it. The overflow is counted and dropped, never turned into a client
/// failure: every reader still gets its 206 from the source.
#[tokio::test]
async fn test_odm_range_burst_overflows_the_pull_queue_without_failing_clients() -> TestResult {
    let bucket = "odm-concurrency-queue-full";
    const REQUESTS: usize = 100;
    let env = start_configured_env(bucket, SOURCE_BUCKET, |spec| {
        spec.policy.pull_queue_capacity = 8;
        spec.policy.max_concurrent_pulls = 1;
    })
    .await?;

    let body = payload(128 * 1024);
    let blocker = "queue/blocker.bin";
    env.seed_source(SOURCE_BUCKET, &[SeedObject::new(blocker, body.clone())]);
    // The one-chunk range completes immediately; its full background pull
    // occupies the only slot while the remaining requests fill the queue.
    env.source.inject_for_key(
        Operation::GetObject,
        blocker,
        FaultAction::SlowSendBody {
            chunk_bytes: 1024,
            delay: Duration::from_millis(100),
        },
        2,
    );
    let response = env
        .raw_object_request(http::Method::GET, bucket, blocker, &[("range", "bytes=0-1023")])
        .await?;
    assert_eq!(response.status, 206);
    assert_eq!(response.body, body.slice(0..1024));
    env.wait_for_status_counter(bucket, "/inflight_pulls", 1, SETTLE).await?;

    let keys: Vec<String> = (0..REQUESTS).map(|index| format!("queue/object-{index:03}.bin")).collect();
    let seeds: Vec<SeedObject> = keys.iter().map(|key| SeedObject::new(key.clone(), body.clone())).collect();
    env.seed_source(SOURCE_BUCKET, &seeds);

    // Bound source connections below the fixture's limit while still
    // submitting all 100 requests to the eight-slot background queue.
    let responses: Vec<RawResponse> = futures::stream::iter(
        keys.iter()
            .map(|key| env.raw_object_request(http::Method::GET, bucket, key, &[("range", "bytes=0-1023")])),
    )
    .buffered(16)
    .try_collect()
    .await?;
    for (key, response) in keys.iter().zip(&responses) {
        assert_eq!(response.status, 206, "{key}: {}", String::from_utf8_lossy(&response.body));
        assert_eq!(response.body, body.slice(0..1024), "{key} served the wrong range");
        assert_eq!(
            response.header("content-range"),
            Some(format!("bytes 0-1023/{}", body.len()).as_str()),
            "{key}"
        );
    }

    let queue_full = env
        .wait_for_status_counter(bucket, "/counters/pull_failures_total/queue_full", 1, SETTLE)
        .await?;
    assert!(queue_full > 0, "a 100-deep burst must overflow an 8-slot queue");
    let queue_full = usize::try_from(queue_full)?;
    assert!(queue_full <= REQUESTS);
    env.wait_for_status_counter(
        bucket,
        "/counters/pulled_objects_total/background",
        u64::try_from(REQUESTS + 1 - queue_full)?,
        SETTLE,
    )
    .await?;

    let ranged_reads: usize = keys.iter().map(|key| source_get_count(&env, key)).sum();
    assert!(
        ranged_reads >= REQUESTS,
        "every reader is served from the source: {ranged_reads} GETs for {REQUESTS} readers"
    );
    let dropped = keys.iter().filter(|key| source_get_count(&env, key) == 1).count();
    assert_eq!(dropped, queue_full, "only overflowed keys remain without a background GET");
    Ok(())
}
