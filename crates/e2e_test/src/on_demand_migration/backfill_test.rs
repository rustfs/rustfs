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

//! On-demand migration backfill job scenarios (ODM-12, rustfs/backlog#2159):
//! a full backfill of a small-object source, cancellation, and resuming from
//! the persisted continuation token after a server restart.

use super::common::{BackfillOp, BackfillRequest, ODM_SERVER_ENV, OdmSourceSpec, OdmTestEnv, SeedObject};
use crate::fake_s3_target::Operation;
use bytes::Bytes;
use std::time::Duration;

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

const SOURCE_BUCKET: &str = "odm-backfill-source";
const LOCAL_BUCKET: &str = "odm-backfill-local";
const KEY_PREFIX: &str = "cold/";
/// Keys per scenario. The fake source retains at most 4,096 object versions
/// and 4,096 journal entries, and one pull is a HEAD plus a GET, so a
/// scenario that asserts on the journal stays below ~2,000 keys. The job
/// lists 1,000 keys per page, so this still spans several pages and exercises
/// the continuation token, which is what the scenarios are about.
const SEEDED_KEYS: usize = 1500;

fn key(i: usize) -> String {
    format!("{KEY_PREFIX}{i:05}")
}

/// Content that identifies the key so a mis-stored object is caught.
fn body(i: usize) -> Bytes {
    Bytes::from(format!("object-{i:05}-payload"))
}

fn seed(env: &OdmTestEnv, count: usize) {
    let objects: Vec<SeedObject> = (0..count).map(|i| SeedObject::new(key(i), body(i))).collect();
    let etags = env.seed_source(SOURCE_BUCKET, &objects);
    assert_eq!(etags.len(), count);
}

async fn configure(env: &OdmTestEnv, spec: &OdmSourceSpec) -> TestResult {
    let response = env.configure_source(LOCAL_BUCKET, spec).await?;
    assert_eq!(response.status, 200, "configure: {}", response.body);
    // The probe issued a one-key listing; count only the job's traffic.
    env.source.take_requests();
    Ok(())
}

fn source_lists(env: &OdmTestEnv) -> Vec<Option<String>> {
    env.source
        .requests()
        .into_iter()
        .filter(|record| record.operation == Operation::ListObjectsV2)
        .map(|record| record.continuation_token)
        .collect()
}

fn source_gets(env: &OdmTestEnv) -> usize {
    env.source
        .requests()
        .into_iter()
        .filter(|record| record.operation == Operation::GetObject)
        .count()
}

fn counter(job: &serde_json::Value, name: &str) -> u64 {
    job[name].as_u64().unwrap_or_else(|| panic!("{name} missing in {job}"))
}

#[tokio::test]
async fn backfill_pulls_every_source_object_across_list_pages() -> TestResult {
    const COUNT: usize = SEEDED_KEYS;
    let env = OdmTestEnv::start().await?;
    env.source.create_bucket(SOURCE_BUCKET);
    env.rustfs.create_test_bucket(LOCAL_BUCKET).await?;
    seed(&env, COUNT);
    configure(&env, &env.fake_source_spec(SOURCE_BUCKET)).await?;

    let started = env.start_backfill(LOCAL_BUCKET, BackfillRequest::default()).await?;
    assert_eq!(started.status, 200, "start: {}", started.body);
    let job = started.json()?["job"].clone();
    assert_eq!(job["state"], "running");
    assert_eq!(job["skip_existing"], "always");
    assert_eq!(job["dry_run"], false);
    let job_id = job["job_id"].as_str().expect("job id").to_string();

    // A second start while the job holds its lease is a conflict.
    let again = env
        .backfill(LOCAL_BUCKET, BackfillOp::Start(BackfillRequest::default()))
        .await?;
    assert_eq!(again.status, 409, "second start: {}", again.body);
    assert!(again.body.contains("OnDemandMigrationBackfillRunning"), "{}", again.body);

    let done = env
        .wait_for_backfill(LOCAL_BUCKET, Duration::from_secs(240), |job| job["state"] == "completed")
        .await?;
    assert_eq!(done["job_id"], job_id.as_str());
    assert_eq!(counter(&done, "listed"), COUNT as u64);
    assert_eq!(counter(&done, "enqueued"), COUNT as u64);
    assert_eq!(counter(&done, "pulled"), COUNT as u64);
    assert_eq!(counter(&done, "failed"), 0);
    assert_eq!(counter(&done, "skipped_existing"), 0);
    assert!(done["continuation_token"].is_null(), "a finished job carries no cursor");
    assert_eq!(done["last_key"], key(COUNT - 1));
    assert!(done["failed_keys"].as_array().is_some_and(Vec::is_empty));
    let expected_bytes: u64 = (0..COUNT).map(|i| body(i).len() as u64).sum();
    assert_eq!(counter(&done, "bytes"), expected_bytes);

    assert_eq!(env.local_key_count(LOCAL_BUCKET, KEY_PREFIX).await?, COUNT);
    for i in [0, 999, 1000, 1200, COUNT - 1] {
        env.assert_local_present(LOCAL_BUCKET, &key(i), &body(i)).await;
    }

    let lists = source_lists(&env);
    assert_eq!(lists.len(), COUNT.div_ceil(1000), "{COUNT} keys at 1000 per page: {lists:?}");
    assert!(lists[0].is_none(), "the first page starts without a cursor");
    assert!(lists[1..].iter().all(Option::is_some), "every later page carries the cursor");
    assert_eq!(source_gets(&env), COUNT, "every object is fetched exactly once");

    // The status endpoint summarises the same job.
    let status = env.status(LOCAL_BUCKET).await?;
    assert_eq!(status.status, 200);
    let summary = status.json()?["backfill"].clone();
    assert_eq!(summary["job_id"], job_id.as_str());
    assert_eq!(summary["state"], "completed");
    assert_eq!(counter(&summary, "pulled"), COUNT as u64);
    Ok(())
}

#[tokio::test]
async fn backfill_cancel_stops_enqueueing_and_persists_cancelled() -> TestResult {
    const COUNT: usize = SEEDED_KEYS;
    let env = OdmTestEnv::start().await?;
    env.source.create_bucket(SOURCE_BUCKET);
    env.rustfs.create_test_bucket(LOCAL_BUCKET).await?;
    seed(&env, COUNT);
    let mut spec = env.fake_source_spec(SOURCE_BUCKET);
    spec.policy.max_concurrent_pulls = 1;
    configure(&env, &spec).await?;

    // Cancelling before any job exists is a 404, not a silent success.
    let nothing = env.backfill(LOCAL_BUCKET, BackfillOp::Cancel).await?;
    assert_eq!(nothing.status, 404, "cancel without a job: {}", nothing.body);
    assert!(nothing.body.contains("NoSuchBackfillJob"), "{}", nothing.body);
    let unread = env.backfill(LOCAL_BUCKET, BackfillOp::Status).await?;
    assert_eq!(unread.status, 404, "status without a job: {}", unread.body);

    let started = env.start_backfill(LOCAL_BUCKET, BackfillRequest::default()).await?;
    assert_eq!(started.status, 200, "start: {}", started.body);
    env.wait_for_backfill(LOCAL_BUCKET, Duration::from_secs(60), |job| {
        job["state"] == "running" && counter(job, "enqueued") > 0
    })
    .await?;

    let cancelled = env.backfill(LOCAL_BUCKET, BackfillOp::Cancel).await?;
    assert_eq!(cancelled.status, 200, "cancel: {}", cancelled.body);
    let job = cancelled.json()?["job"].clone();
    assert_eq!(job["state"], "cancelled");
    let enqueued_at_cancel = counter(&job, "enqueued");
    assert!(enqueued_at_cancel < COUNT as u64, "the job was cancelled mid-way: {job}");

    // Nothing is queued after the cancel: the checkpoint and the source
    // traffic both stop moving once the few in-flight pulls drain.
    tokio::time::sleep(Duration::from_secs(2)).await;
    let persisted = env.backfill_job(LOCAL_BUCKET).await?.expect("checkpoint kept for inspection");
    assert_eq!(persisted["state"], "cancelled");
    assert_eq!(counter(&persisted, "enqueued"), enqueued_at_cancel);
    let gets_after_drain = source_gets(&env);
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(source_gets(&env), gets_after_drain, "no source GET after the cancel drained");
    assert!(env.local_key_count(LOCAL_BUCKET, KEY_PREFIX).await? < COUNT);

    // Cancel is idempotent and the status endpoint reports the final state.
    let again = env.backfill(LOCAL_BUCKET, BackfillOp::Cancel).await?;
    assert_eq!(again.status, 200, "second cancel: {}", again.body);
    assert_eq!(again.json()?["job"]["state"], "cancelled");
    let status = env.status(LOCAL_BUCKET).await?;
    assert_eq!(status.json()?["backfill"]["state"], "cancelled");

    // A cancelled job releases the bucket: a new job can start.
    let restarted = env.start_backfill(LOCAL_BUCKET, BackfillRequest::default()).await?;
    assert_eq!(restarted.status, 200, "restart after cancel: {}", restarted.body);
    assert_ne!(restarted.json()?["job"]["job_id"], job["job_id"]);
    let _ = env.backfill(LOCAL_BUCKET, BackfillOp::Cancel).await?;
    Ok(())
}

#[tokio::test]
async fn backfill_resumes_from_continuation_token_after_restart() -> TestResult {
    const COUNT: usize = SEEDED_KEYS;
    let mut env = OdmTestEnv::start().await?;
    env.source.create_bucket(SOURCE_BUCKET);
    env.rustfs.create_test_bucket(LOCAL_BUCKET).await?;
    seed(&env, COUNT);
    let mut spec = env.fake_source_spec(SOURCE_BUCKET);
    spec.policy.max_concurrent_pulls = 2;
    configure(&env, &spec).await?;

    let started = env.start_backfill(LOCAL_BUCKET, BackfillRequest::default()).await?;
    assert_eq!(started.status, 200, "start: {}", started.body);
    let job_id = started.json()?["job"]["job_id"].as_str().expect("job id").to_string();

    // Wait for the first page to be committed (cursor persisted), then kill
    // the server while the job is still running.
    let mid = env
        .wait_for_backfill(LOCAL_BUCKET, Duration::from_secs(120), |job| {
            job["state"] == "running" && job["continuation_token"].is_string()
        })
        .await?;
    assert!(counter(&mid, "listed") >= 1000 && counter(&mid, "listed") < COUNT as u64, "{mid}");
    env.source.take_requests();
    env.rustfs.restart_server_preserving_data(vec![], ODM_SERVER_ENV).await?;

    let done = env
        .wait_for_backfill(LOCAL_BUCKET, Duration::from_secs(300), |job| job["state"] == "completed")
        .await?;
    assert_eq!(done["job_id"], job_id.as_str(), "the same job continues after the restart");
    assert_eq!(counter(&done, "failed"), 0);
    assert!(
        counter(&done, "listed") >= COUNT as u64,
        "the resumed job listed the rest (the interrupted page is listed twice): {done}"
    );
    // Keys pulled before the crash are re-listed and skipped, never re-pulled;
    // a pull whose report died with the old process is counted neither way,
    // so only the lower bound and the queue accounting are exact.
    assert!(counter(&done, "pulled") + counter(&done, "skipped_existing") >= COUNT as u64, "{done}");
    assert!(counter(&done, "pulled") <= counter(&done, "enqueued"), "{done}");
    assert_eq!(env.local_key_count(LOCAL_BUCKET, KEY_PREFIX).await?, COUNT);
    for i in [0, 500, 999, 1000, COUNT - 1] {
        env.assert_local_present(LOCAL_BUCKET, &key(i), &body(i)).await;
    }

    let lists = source_lists(&env);
    assert!(!lists.is_empty(), "the resumed job listed the source");
    assert!(
        lists.iter().all(Option::is_some),
        "after the restart every source listing carries a continuation-token: {lists:?}"
    );
    assert!(
        lists.len() <= COUNT.div_ceil(1000),
        "the listing did not start over from the first page: {lists:?}"
    );
    Ok(())
}
