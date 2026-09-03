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

//! Source-failure scenarios for on-demand migration (rustfs/backlog#2158):
//! access denied, the circuit breaker, first-byte and mid-body stream
//! failures, ETag integrity, the negative cache, and an unsupported
//! (SSE-C) source object.
//!
//! Every case asserts what the source was asked for, not only what the
//! client received: a fault that silently turned into a second source
//! request would otherwise pass.

use super::common::{BoxError, OdmTestEnv, SeedObject, start_configured_env};
use crate::fake_s3_target::{FaultAction, Operation};
use bytes::Bytes;
use std::time::{Duration, Instant};

type TestResult = Result<(), BoxError>;

const SOURCE_BUCKET: &str = "odm-fault-source";
/// Header the GET/HEAD paths add when the answer came from the source.
const ODM_RESPONSE_HEADER: &str = "x-rustfs-on-demand-migration";
/// Status of the `SourceUnavailable` error the `propagate` policy returns.
const SOURCE_UNAVAILABLE_STATUS: u16 = 424;
/// Background pulls and their counters land after the response.
const SETTLE: Duration = Duration::from_secs(60);
/// Consecutive counted source failures that open the breaker
/// (`BREAKER_FAILURE_THRESHOLD` in ecstore).
const BREAKER_FAILURE_THRESHOLD: usize = 5;

/// Position-dependent payload so a misaligned or truncated copy is caught.
fn payload(len: usize) -> Bytes {
    (0..len).map(|index| (index % 251) as u8).collect::<Vec<u8>>().into()
}

/// A source object with a well-formed but deliberately wrong single-part
/// ETag: the fake source retains `x-rustfs-source-etag` verbatim, so HEAD
/// and GET advertise an MD5 the body does not have.
async fn seed_with_etag(env: &OdmTestEnv, key: &str, body: Bytes, etag: &str) -> TestResult {
    let response = env
        .source_client()
        .put_object()
        .bucket(SOURCE_BUCKET)
        .key(key)
        .body(aws_sdk_s3::primitives::ByteStream::from(body))
        .customize()
        .mutate_request({
            let etag = etag.to_string();
            move |request| {
                request.headers_mut().insert("x-rustfs-source-etag", etag.clone());
            }
        })
        .send()
        .await?;
    assert_eq!(
        response.e_tag(),
        Some(format!("\"{etag}\"").as_str()),
        "the fake source stores the announced ETag"
    );
    Ok(())
}

/// A source object that reports SSE-C: the fake source echoes the customer
/// algorithm it captured from the replication passthrough transport header.
async fn seed_with_ssec(env: &OdmTestEnv, key: &str, body: Bytes) -> TestResult {
    env.source_client()
        .put_object()
        .bucket(SOURCE_BUCKET)
        .key(key)
        .body(aws_sdk_s3::primitives::ByteStream::from(body))
        .customize()
        .mutate_request(|request| {
            request.headers_mut().insert("x-rustfs-replication-ssec-algorithm", "AES256");
        })
        .send()
        .await?;
    Ok(())
}

/// Case 1: a 403 from the source is a configuration error, not a health
/// signal. `propagate` answers 424 and records the class; `not_found` hides
/// it as a 404. Neither counts toward the breaker.
#[tokio::test]
async fn test_odm_source_access_denied_propagates_without_opening_the_breaker() -> TestResult {
    let propagating = "odm-fault-denied-propagate";
    let hiding = "odm-fault-denied-notfound";
    let env = start_configured_env(propagating, SOURCE_BUCKET, |_| {}).await?;
    let mut hiding_spec = env.fake_source_spec(SOURCE_BUCKET);
    hiding_spec.policy.source_error = "not_found".to_string();
    env.configure_and_wait(hiding, &hiding_spec).await?;

    let propagate_key = "denied/propagate.bin";
    let hidden_key = "denied/hidden.bin";
    env.seed_source(
        SOURCE_BUCKET,
        &[
            SeedObject::new(propagate_key, payload(4096)),
            SeedObject::new(hidden_key, payload(4096)),
        ],
    );

    env.source
        .inject_for_key(Operation::HeadObject, propagate_key, FaultAction::ResponseStatus(403), 1);
    let denied = env.raw_get(propagating, propagate_key).await?;
    assert_eq!(denied.status, SOURCE_UNAVAILABLE_STATUS, "{}", String::from_utf8_lossy(&denied.body));
    assert!(
        String::from_utf8_lossy(&denied.body).contains("SourceUnavailable"),
        "the propagated error names the ODM source code: {}",
        String::from_utf8_lossy(&denied.body)
    );
    assert_eq!(env.source.count_requests(Operation::HeadObject, propagate_key), 1);
    assert_eq!(
        env.source.count_requests(Operation::GetObject, propagate_key),
        0,
        "a denied HEAD never reaches the body"
    );

    let status = env.status_json(propagating).await?;
    assert_eq!(
        status.pointer("/last_source_error/class").and_then(|v| v.as_str()),
        Some("access_denied"),
        "{status}"
    );
    assert_eq!(
        status.pointer("/breaker/state").and_then(|v| v.as_str()),
        Some("closed"),
        "a configuration error must not open the breaker: {status}"
    );
    assert_eq!(
        status
            .pointer("/counters/requests_total/get/source_error")
            .and_then(|v| v.as_u64()),
        Some(1),
        "{status}"
    );

    env.source
        .inject_for_key(Operation::HeadObject, hidden_key, FaultAction::ResponseStatus(403), 1);
    let hidden = env.raw_get(hiding, hidden_key).await?;
    assert_eq!(hidden.status, 404, "{}", String::from_utf8_lossy(&hidden.body));
    assert_eq!(env.source.count_requests(Operation::HeadObject, hidden_key), 1);
    assert_eq!(env.source.count_requests(Operation::GetObject, hidden_key), 0);

    env.assert_local_absent(propagating, propagate_key).await;
    env.assert_local_absent(hiding, hidden_key).await;
    Ok(())
}

/// Case 2: repeated transport failures open the breaker; while it is open
/// the source is not touched at all, and the half-open probe after the open
/// window closes it again. The open window is a compiled-in 30 s constant
/// (`BREAKER_OPEN_DURATION`), so this case waits in real time.
///
/// One ODM source call is several wire requests: the SDK retries a 503 on
/// its own, and only the exhausted call counts as one breaker failure. The
/// script is therefore deep enough to cover every retry, and the open state
/// is waited for instead of being predicted from a request count.
#[tokio::test]
async fn test_odm_repeated_source_errors_open_the_breaker_and_recover() -> TestResult {
    let bucket = "odm-fault-breaker";
    let env = start_configured_env(bucket, SOURCE_BUCKET, |_| {}).await?;
    let key = "breaker/doc.bin";
    let body = payload(8192);
    env.seed_source(SOURCE_BUCKET, &[SeedObject::new(key, body.clone())]);

    env.source
        .inject_for_key(Operation::HeadObject, key, FaultAction::ResponseStatus(503), 200);
    let mut opened = false;
    for attempt in 1..=BREAKER_FAILURE_THRESHOLD * 2 {
        let response = env.raw_get(bucket, key).await?;
        assert_eq!(
            response.status,
            SOURCE_UNAVAILABLE_STATUS,
            "attempt {attempt}: {}",
            String::from_utf8_lossy(&response.body)
        );
        if env
            .status_json(bucket)
            .await?
            .pointer("/breaker/state")
            .and_then(|v| v.as_str())
            == Some("open")
        {
            opened = true;
            break;
        }
    }
    assert!(opened, "consecutive source failures must open the breaker");
    assert!(
        env.source.count_requests(Operation::HeadObject, key) >= BREAKER_FAILURE_THRESHOLD,
        "each counted failure is at least one source request"
    );

    // With the script cleared, the only thing that can still fail a read is
    // the open breaker itself.
    env.source.clear_faults();
    let source_requests = env.source.count_requests(Operation::HeadObject, key);
    let rejected = env.raw_get(bucket, key).await?;
    assert_eq!(rejected.status, SOURCE_UNAVAILABLE_STATUS, "{}", String::from_utf8_lossy(&rejected.body));
    assert_eq!(
        env.source.count_requests(Operation::HeadObject, key),
        source_requests,
        "an open breaker never touches the source"
    );
    assert!(
        env.status_counter(bucket, "/counters/requests_total/get/breaker_open")
            .await?
            >= 1,
        "the rejected request is counted as breaker_open"
    );

    // Half-open admits exactly one probe once the open window elapses.
    let deadline = Instant::now() + Duration::from_secs(120);
    let recovered = loop {
        let response = env.raw_get(bucket, key).await?;
        if response.status == 200 {
            break response;
        }
        assert_eq!(response.status, SOURCE_UNAVAILABLE_STATUS);
        assert!(Instant::now() < deadline, "the breaker never left the open state");
        tokio::time::sleep(Duration::from_secs(1)).await;
    };
    assert_eq!(recovered.body, body, "the recovered read serves the source bytes");
    assert_eq!(recovered.header(ODM_RESPONSE_HEADER), Some("source"));
    assert_eq!(
        env.source.count_requests(Operation::HeadObject, key),
        source_requests + 1,
        "only the half-open probe reached the source"
    );
    assert_eq!(env.source.count_requests(Operation::GetObject, key), 1);
    assert_eq!(
        env.status_json(bucket)
            .await?
            .pointer("/breaker/state")
            .and_then(|v| v.as_str()),
        Some("closed"),
        "a successful probe closes the breaker"
    );
    Ok(())
}

/// Case 3: a source that holds the response past `first_byte_ms` is a
/// timeout, and the client never sees a 200 head. Every attempt the SDK
/// makes on its own is stalled too, so the ODM call really does give up.
#[tokio::test]
async fn test_odm_source_stall_times_out_before_the_first_byte() -> TestResult {
    let bucket = "odm-fault-stall";
    let env = start_configured_env(bucket, SOURCE_BUCKET, |spec| {
        spec.policy.source_timeout.first_byte_ms = 500;
    })
    .await?;
    let key = "stall/doc.bin";
    env.seed_source(SOURCE_BUCKET, &[SeedObject::new(key, payload(4096))]);

    env.source
        .inject_for_key(Operation::HeadObject, key, FaultAction::Stall(Duration::from_secs(5)), 8);
    let started = Instant::now();
    let response = env.raw_get(bucket, key).await?;
    let elapsed = started.elapsed();
    assert_eq!(response.status, SOURCE_UNAVAILABLE_STATUS, "{}", String::from_utf8_lossy(&response.body));
    assert!(
        elapsed < Duration::from_secs(30),
        "the read timeout must cut every attempt short, took {elapsed:?}"
    );
    let attempts = env.source.count_requests(Operation::HeadObject, key);
    assert!(attempts >= 1, "the stalled HEAD is the only source request");
    assert!(
        elapsed < Duration::from_secs(5) * u32::try_from(attempts).unwrap_or(1),
        "no attempt waited the stall out ({attempts} attempts in {elapsed:?})"
    );
    assert_eq!(
        env.source.count_requests(Operation::GetObject, key),
        0,
        "a timed-out HEAD never starts a body read"
    );
    assert_eq!(
        env.status_json(bucket)
            .await?
            .pointer("/last_source_error/class")
            .and_then(|v| v.as_str()),
        Some("timeout"),
    );
    env.assert_local_absent(bucket, key).await;
    Ok(())
}

/// Case 4: the source cuts the body of an inline pull. The client sees a
/// short read, nothing is stored, and no multipart upload is left behind.
#[tokio::test]
async fn test_odm_inline_pull_aborts_when_the_source_body_is_cut() -> TestResult {
    let bucket = "odm-fault-inline-cut";
    let env = start_configured_env(bucket, SOURCE_BUCKET, |_| {}).await?;
    let key = "cut/inline.bin";
    let body = payload(256 * 1024);
    env.seed_source(SOURCE_BUCKET, &[SeedObject::new(key, body.clone())]);

    env.source
        .inject_for_key(Operation::GetObject, key, FaultAction::TruncateBodyAt(1024), 1);
    // The client sees a transport failure while reading the body: the
    // announced Content-Length is never delivered.
    env.raw_get(bucket, key)
        .await
        .expect_err("a cut source body must not read back as a complete object");

    assert_eq!(env.source.count_requests(Operation::HeadObject, key), 1);
    assert_eq!(
        env.source.count_requests(Operation::GetObject, key),
        1,
        "an aborted inline pull is not retried on the same request"
    );
    // Give a stray background pull time to appear before asserting absence.
    tokio::time::sleep(Duration::from_secs(3)).await;
    env.assert_local_absent(bucket, key).await;
    let uploads = env.client.list_multipart_uploads().bucket(bucket).send().await?;
    assert!(
        uploads.uploads().is_empty(),
        "an aborted pull leaves no multipart upload: {:?}",
        uploads.uploads()
    );
    assert_eq!(
        env.source.count_requests(Operation::GetObject, key),
        1,
        "nothing re-reads the source afterwards"
    );
    Ok(())
}

/// Case 5: the background pull of a large object hits a cut body, counts the
/// failure, and the retry stores the object.
#[tokio::test]
async fn test_odm_background_pull_retries_a_truncated_source_body() -> TestResult {
    let bucket = "odm-fault-background-cut";
    let env = start_configured_env(bucket, SOURCE_BUCKET, |spec| spec.policy.inline_max_bytes = 4096).await?;
    let key = "cut/background.bin";
    let body = payload(512 * 1024);
    env.seed_source(SOURCE_BUCKET, &[SeedObject::new(key, body.clone())]);

    // The faults are consumed in order by the two GETs the large-object path
    // makes: the passthrough that answers the client (unaffected), then the
    // background pull (cut).
    env.source
        .inject_for_key(Operation::GetObject, key, FaultAction::Delay(Duration::ZERO), 1);
    env.source
        .inject_for_key(Operation::GetObject, key, FaultAction::TruncateBodyAt(2048), 1);

    let response = env.raw_get(bucket, key).await?;
    assert_eq!(response.status, 200, "{}", String::from_utf8_lossy(&response.body));
    assert_eq!(response.body, body, "the passthrough is unaffected by the pull's fault");

    // The cut body ends the pull attempt as a retryable source transport
    // failure; the retry stores the object, so the pull as a whole succeeds
    // and no failure is counted (only a pull that gives up is).
    assert!(env.wait_local_listed(bucket, key, SETTLE).await?, "the retry must store the object");
    env.assert_local_present(bucket, key, &body).await;
    assert_eq!(
        env.source.count_requests(Operation::GetObject, key),
        3,
        "one passthrough, one cut pull, one successful retry"
    );
    let status = env.status_json(bucket).await?;
    assert_eq!(
        status
            .pointer("/counters/pulled_objects_total/background")
            .and_then(|v| v.as_u64()),
        Some(1),
        "{status}"
    );
    assert_eq!(
        status
            .pointer("/counters/pull_failures_total")
            .and_then(|failures| failures.as_object())
            .map(|failures| failures.values().filter_map(serde_json::Value::as_u64).sum::<u64>()),
        Some(0),
        "a retried attempt is not a failed pull: {status}"
    );
    Ok(())
}

/// Case 6: the source advertises an ETag its bytes do not match. The client
/// still gets every byte; the write-back is discarded as an integrity
/// failure and nothing is stored.
#[tokio::test]
async fn test_odm_wrong_source_etag_discards_the_write_back() -> TestResult {
    let bucket = "odm-fault-etag";
    let env = start_configured_env(bucket, SOURCE_BUCKET, |_| {}).await?;
    let key = "etag/mismatch.bin";
    let body = payload(64 * 1024);
    seed_with_etag(&env, key, body.clone(), "0123456789abcdef0123456789abcdef").await?;

    let response = env.raw_get(bucket, key).await?;
    assert_eq!(response.status, 200, "{}", String::from_utf8_lossy(&response.body));
    assert_eq!(response.header(ODM_RESPONSE_HEADER), Some("source"));
    assert_eq!(response.body, body, "the client receives the complete source bytes");

    env.wait_for_status_counter(bucket, "/counters/pull_failures_total/etag_mismatch", 1, SETTLE)
        .await?;
    env.assert_local_absent(bucket, key).await;
    assert_eq!(env.source.count_requests(Operation::HeadObject, key), 1);
    assert_eq!(
        env.source.count_requests(Operation::GetObject, key),
        1,
        "a discarded write-back is not re-read"
    );
    Ok(())
}

/// Case 7: a source miss is remembered for `negative_cache_ttl_secs`, and
/// re-checked once the entry expires.
#[tokio::test]
async fn test_odm_source_not_found_is_negative_cached_for_the_ttl() -> TestResult {
    let bucket = "odm-fault-negative-cache";
    let ttl = Duration::from_secs(3);
    let env = start_configured_env(bucket, SOURCE_BUCKET, |spec| {
        spec.policy.negative_cache_ttl_secs = ttl.as_secs();
    })
    .await?;
    let key = "negative/nowhere.bin";

    for attempt in 1..=10 {
        let response = env.raw_get(bucket, key).await?;
        assert_eq!(response.status, 404, "attempt {attempt}: {}", String::from_utf8_lossy(&response.body));
    }
    assert_eq!(
        env.source.count_requests(Operation::HeadObject, key),
        1,
        "nine of the ten misses stop at the negative cache"
    );
    assert!(
        env.status_counter(bucket, "/counters/requests_total/get/negative_cached")
            .await?
            >= 9,
        "the cached misses are counted"
    );

    tokio::time::sleep(ttl + Duration::from_secs(2)).await;
    let response = env.raw_get(bucket, key).await?;
    assert_eq!(response.status, 404);
    assert_eq!(
        env.source.count_requests(Operation::HeadObject, key),
        2,
        "an expired entry re-checks the source once"
    );
    assert_eq!(env.source.count_requests(Operation::GetObject, key), 0);
    Ok(())
}

/// Case 8: an SSE-C source object cannot be migrated (the key belongs to the
/// source's client), so the read fails as unsupported without a body read.
#[tokio::test]
async fn test_odm_ssec_source_object_is_unsupported() -> TestResult {
    let bucket = "odm-fault-ssec";
    let env = start_configured_env(bucket, SOURCE_BUCKET, |_| {}).await?;
    let key = "ssec/secret.bin";
    seed_with_ssec(&env, key, payload(4096)).await?;

    let response = env.raw_get(bucket, key).await?;
    assert_eq!(response.status, SOURCE_UNAVAILABLE_STATUS, "{}", String::from_utf8_lossy(&response.body));
    assert_eq!(env.source.count_requests(Operation::HeadObject, key), 1);
    assert_eq!(
        env.source.count_requests(Operation::GetObject, key),
        0,
        "an unsupported object is rejected on the HEAD"
    );
    assert_eq!(
        env.status_json(bucket)
            .await?
            .pointer("/counters/requests_total/get/unsupported")
            .and_then(|v| v.as_u64()),
        Some(1),
    );
    env.assert_local_absent(bucket, key).await;
    Ok(())
}
