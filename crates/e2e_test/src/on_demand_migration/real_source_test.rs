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

//! On-demand migration against a real RustFS source (rustfs/backlog#2158).
//!
//! These cases start a second (and, for the loop guard, a third) RustFS
//! process, so they carry the `_real_single_node` marker and run in the
//! nightly lane. A real source keeps no request journal, so "the source was
//! not consulted" is proven by removing the object from the source and
//! showing the read still succeeds, or by pointing the *second* server at a
//! fake source whose journal must stay empty.

use super::common::{
    AdminResponse, BoxError, ODM_ADMIN_ROUTE, OdmSourceSpec, OdmTestEnv, RawResponse, SeedObject, start_source_rustfs,
    start_source_rustfs_with_odm,
};
use crate::common::{RustFSTestEnvironment, signed_request};
use crate::fake_s3_target::{BucketMode, Operation};
use aws_sdk_s3::Client;
use bytes::Bytes;
use std::time::{Duration, Instant};

type TestResult = Result<(), BoxError>;

const ODM_RESPONSE_HEADER: &str = "x-rustfs-on-demand-migration";
/// Reinstalled configurations are applied asynchronously; every phase polls
/// for the new behavior instead of sleeping.
const APPLY_TIMEOUT: Duration = Duration::from_secs(30);
const SETTLE: Duration = Duration::from_secs(60);

fn payload(len: usize) -> Bytes {
    (0..len).map(|index| (index % 251) as u8).collect::<Vec<u8>>().into()
}

/// `PUT /rustfs/admin/v3/on-demand-migration/{bucket}` against any server,
/// not just the one under test.
async fn configure_odm(env: &RustFSTestEnvironment, bucket: &str, spec: &OdmSourceSpec) -> Result<AdminResponse, BoxError> {
    let url = format!("{}{ODM_ADMIN_ROUTE}/{bucket}", env.url);
    let body = serde_json::to_vec(&spec.to_json())?;
    let response = signed_request(
        http::Method::PUT,
        &url,
        &env.access_key,
        &env.secret_key,
        Some(body),
        Some("application/json"),
    )
    .await?;
    Ok(AdminResponse {
        status: response.status().as_u16(),
        body: response.text().await?,
    })
}

async fn put_object(client: &Client, bucket: &str, key: &str, body: Bytes) -> TestResult {
    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(aws_sdk_s3::primitives::ByteStream::from(body))
        .send()
        .await?;
    Ok(())
}

/// Polls a read against the server under test until it answers `expected`.
/// This is how a reinstalled configuration is waited for when the source
/// keeps no journal to probe.
async fn wait_for_get_status(env: &OdmTestEnv, bucket: &str, key: &str, expected: u16) -> Result<RawResponse, BoxError> {
    let deadline = Instant::now() + APPLY_TIMEOUT;
    loop {
        let response = env.raw_get(bucket, key).await?;
        if response.status == expected {
            return Ok(response);
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "GET {bucket}/{key} stayed at {} instead of {expected}: {}",
                response.status,
                String::from_utf8_lossy(&response.body)
            )
            .into());
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Case 20: a second RustFS as the migration source — the pull, a HEAD
/// passthrough, a Range read, and both prefix knobs.
#[tokio::test]
async fn test_odm_rustfs_source_serves_pull_head_range_and_prefixes_real_single_node() -> TestResult {
    let bucket = "odm-real-source";
    let source_bucket = "odm-real-origin";
    let source = start_source_rustfs().await?;
    let source_client = source.create_s3_client();
    source.create_test_bucket(source_bucket).await?;

    let env = OdmTestEnv::start().await?;
    env.rustfs.create_test_bucket(bucket).await?;
    let spec = OdmSourceSpec::for_rustfs_source(&source, source_bucket);
    let configured = configure_odm(&env.rustfs, bucket, &spec).await?;
    assert_eq!(configured.status, 200, "{}", configured.body);

    // Phase 1: a miss is pulled and stored; removing it from the source
    // afterwards proves the second read never goes back to the source.
    let pulled_key = "real/pulled.bin";
    let pulled_body = payload(256 * 1024);
    put_object(&source_client, source_bucket, pulled_key, pulled_body.clone()).await?;
    let pulled = wait_for_get_status(&env, bucket, pulled_key, 200).await?;
    assert_eq!(pulled.header(ODM_RESPONSE_HEADER), Some("source"));
    assert_eq!(pulled.body, pulled_body, "the client receives the source bytes");
    assert!(env.wait_local_listed(bucket, pulled_key, SETTLE).await?, "the pull must store the object");
    source_client
        .delete_object()
        .bucket(source_bucket)
        .key(pulled_key)
        .send()
        .await?;
    let local = env.raw_get(bucket, pulled_key).await?;
    assert_eq!(local.status, 200, "{}", String::from_utf8_lossy(&local.body));
    assert_eq!(local.header(ODM_RESPONSE_HEADER), None, "a local hit is not marked");
    assert_eq!(local.body, pulled_body, "the object is served from the local copy");

    // Phase 2: HEAD proxies metadata without storing anything.
    let head_key = "real/head-only.bin";
    let head_body = payload(9_000);
    put_object(&source_client, source_bucket, head_key, head_body.clone()).await?;
    let head = env.raw_object_request(http::Method::HEAD, bucket, head_key, &[]).await?;
    assert_eq!(head.status, 200, "HEAD must be answered from the source");
    assert_eq!(head.header(ODM_RESPONSE_HEADER), Some("source"));
    assert_eq!(head.header("content-length"), Some(head_body.len().to_string().as_str()));
    env.assert_local_absent(bucket, head_key).await;

    // Phase 3: a Range read is passed through as a 206.
    let range_key = "real/range.bin";
    let range_body = payload(100_000);
    put_object(&source_client, source_bucket, range_key, range_body.clone()).await?;
    let ranged = env
        .raw_object_request(http::Method::GET, bucket, range_key, &[("range", "bytes=100-199")])
        .await?;
    assert_eq!(ranged.status, 206, "{}", String::from_utf8_lossy(&ranged.body));
    assert_eq!(ranged.header("content-range"), Some("bytes 100-199/100000"));
    assert_eq!(ranged.body, range_body.slice(100..200));

    // Phase 4: `filter.prefix` decides which local keys may consult the
    // source at all.
    let allowed_key = "allowed/doc.bin";
    let denied_key = "denied/doc.bin";
    let filtered_body = payload(4_096);
    put_object(&source_client, source_bucket, allowed_key, filtered_body.clone()).await?;
    put_object(&source_client, source_bucket, denied_key, filtered_body.clone()).await?;
    let mut filtered = OdmSourceSpec::for_rustfs_source(&source, source_bucket);
    filtered.filter.prefix = Some("allowed/".to_string());
    let response = configure_odm(&env.rustfs, bucket, &filtered).await?;
    assert_eq!(response.status, 200, "{}", response.body);
    let denied = wait_for_get_status(&env, bucket, denied_key, 404).await?;
    assert_eq!(denied.header(ODM_RESPONSE_HEADER), None);
    env.assert_local_absent(bucket, denied_key).await;
    let allowed = wait_for_get_status(&env, bucket, allowed_key, 200).await?;
    assert_eq!(allowed.header(ODM_RESPONSE_HEADER), Some("source"));
    assert_eq!(allowed.body, filtered_body);

    // Phase 5: `filter.source_prefix` rewrites the key on the way out, so a
    // local key resolves to a different key in the source bucket.
    let rewritten_key = "rewritten/doc.bin";
    let rewritten_body = payload(2_048);
    put_object(&source_client, source_bucket, &format!("archive/{rewritten_key}"), rewritten_body.clone()).await?;
    let mut rewriting = OdmSourceSpec::for_rustfs_source(&source, source_bucket);
    rewriting.filter.source_prefix = Some("archive/".to_string());
    let response = configure_odm(&env.rustfs, bucket, &rewriting).await?;
    assert_eq!(response.status, 200, "{}", response.body);
    let rewritten = wait_for_get_status(&env, bucket, rewritten_key, 200).await?;
    assert_eq!(rewritten.header(ODM_RESPONSE_HEADER), Some("source"));
    assert_eq!(rewritten.body, rewritten_body, "the source prefix is prepended to the local key");
    Ok(())
}

/// Case 21: two migrating servers pointed at each other must not build a
/// request loop. The middle server also has a fake source of its own, whose
/// journal is the evidence: a key it would happily fetch for a direct client
/// is never fetched for a request that arrived with the anti-loop marker.
#[tokio::test]
async fn test_odm_chained_sources_stop_at_the_loop_guard_real_single_node() -> TestResult {
    let bucket = "odm-loop-guard";
    let fake_bucket = "odm-loop-fake";
    let env = OdmTestEnv::start().await?;
    env.source.create_bucket_with_mode(fake_bucket, BucketMode::Unversioned);
    env.rustfs.create_test_bucket(bucket).await?;

    let middle = start_source_rustfs_with_odm().await?;
    let middle_client = middle.create_s3_client();
    middle.create_test_bucket(bucket).await?;
    let middle_spec = OdmSourceSpec::for_fake_source(&env.source, fake_bucket);
    let configured = configure_odm(&middle, bucket, &middle_spec).await?;
    assert_eq!(configured.status, 200, "{}", configured.body);

    let chained = OdmSourceSpec::for_rustfs_source(&middle, bucket);
    let configured = configure_odm(&env.rustfs, bucket, &chained).await?;
    assert_eq!(configured.status, 200, "{}", configured.body);

    // The first hop works: an object that only the middle server holds is
    // migrated to the server under test.
    let present_key = "loop/present.bin";
    let present_body = payload(16 * 1024);
    put_object(&middle_client, bucket, present_key, present_body.clone()).await?;
    let served = wait_for_get_status(&env, bucket, present_key, 200).await?;
    assert_eq!(served.header(ODM_RESPONSE_HEADER), Some("source"));
    assert_eq!(served.body, present_body, "the first hop serves the middle server's object");

    // The second hop does not: this key exists only on the middle server's
    // own source, and the anti-loop marker stops the chain there.
    let guarded_key = "loop/chain-guard.bin";
    let guarded_body = payload(8 * 1024);
    env.seed_source(fake_bucket, &[SeedObject::new(guarded_key, guarded_body.clone())]);
    let guarded = env.raw_get(bucket, guarded_key).await?;
    assert_eq!(guarded.status, 404, "{}", String::from_utf8_lossy(&guarded.body));
    assert_eq!(
        env.source.count_requests(Operation::HeadObject, guarded_key),
        0,
        "a chained request must not reach a third source"
    );
    assert_eq!(env.source.count_requests(Operation::GetObject, guarded_key), 0);

    // Proof that the guard, and not a broken configuration, is what stopped
    // it: the same key served directly by the middle server does reach the
    // fake source.
    let direct = middle_client.get_object().bucket(bucket).key(guarded_key).send().await?;
    assert_eq!(direct.body.collect().await?.into_bytes(), guarded_body);
    assert_eq!(
        env.source.count_requests(Operation::GetObject, guarded_key),
        1,
        "an unmarked request does consult the middle server's source"
    );

    // Now make the pair mutual and prove the read still terminates.
    let mutual = OdmSourceSpec::for_rustfs_source(&env.rustfs, bucket);
    let configured = configure_odm(&middle, bucket, &mutual).await?;
    assert_eq!(configured.status, 200, "{}", configured.body);

    let mutual_key = "loop/mutual.bin";
    let started = Instant::now();
    let response = wait_for_get_status(&env, bucket, mutual_key, 404).await?;
    assert_eq!(response.header(ODM_RESPONSE_HEADER), None);
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "a mutual configuration must not loop, took {:?}",
        started.elapsed()
    );
    assert_eq!(
        env.source.count_requests(Operation::HeadObject, mutual_key),
        0,
        "the fake source is out of the chain once the pair is mutual"
    );
    Ok(())
}
