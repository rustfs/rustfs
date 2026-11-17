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

//! ILM transition-engine tests that call no S3 use-case; owned by no operation batch.

use super::common::*;
use super::*;

const ENV_GET_CODEC_STREAMING_ENABLE: &str = "RUSTFS_GET_CODEC_STREAMING_ENABLE";

const ENV_GET_CODEC_STREAMING_ROLLOUT: &str = "RUSTFS_GET_CODEC_STREAMING_ROLLOUT";

const ENV_GET_CODEC_STREAMING_BODY_COMPAT_CONFIRMED: &str = "RUSTFS_GET_CODEC_STREAMING_BODY_COMPAT_CONFIRMED";

const ENV_GET_CODEC_STREAMING_HEADER_COMPAT_CONFIRMED: &str = "RUSTFS_GET_CODEC_STREAMING_HEADER_COMPAT_CONFIRMED";

const ENV_GET_CODEC_STREAMING_MIN_SIZE: &str = "RUSTFS_GET_CODEC_STREAMING_MIN_SIZE";

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[serial]
#[ignore = "global-state ILM integration test: runs serialized in the CI ILM Integration (serial) lane, see ci.yml test-ilm-integration-serial and rustfs/backlog#1148 (ilm-1)"]
async fn transition_wait_timeout_reports_metadata_state_and_read_failure() {
    let (_disk_paths, ecstore) = setup_test_env().await;
    let bucket = format!("test-transition-wait-{}", &Uuid::new_v4().simple().to_string()[..8]);
    create_test_bucket(&ecstore, &bucket).await;
    upload_test_object(&ecstore, &bucket, "plain.txt", b"not transitioned").await;

    let pending = wait_for_transition(&ecstore, &bucket, "plain.txt", Duration::ZERO)
        .await
        .expect_err("an untransitioned object must still fail the observation deadline");
    assert!(pending.contains("transitioned_status=\"\""), "{pending}");
    assert!(pending.contains("compensation_pending="), "{pending}");
    assert!(pending.contains("compensation_running="), "{pending}");

    let missing = wait_for_transition(&ecstore, &bucket, "missing.txt", Duration::ZERO)
        .await
        .expect_err("metadata read failures must not become a completed transition");
    assert!(missing.contains("metadata_error=ObjectNotFound"), "{missing}");
}

async fn with_get_codec_streaming_remote_probe_env<F, Fut>(test_fn: F)
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let metrics_was_enabled = rustfs_io_metrics::get_stage_metrics_enabled();
    rustfs_io_metrics::set_get_stage_metrics_enabled(true);
    let result = std::panic::AssertUnwindSafe(temp_env::async_with_vars(
        [
            (ENV_GET_CODEC_STREAMING_ENABLE, Some("true")),
            (ENV_GET_CODEC_STREAMING_ROLLOUT, Some("benchmark")),
            (ENV_GET_CODEC_STREAMING_BODY_COMPAT_CONFIRMED, Some("true")),
            (ENV_GET_CODEC_STREAMING_HEADER_COMPAT_CONFIRMED, Some("true")),
            (ENV_GET_CODEC_STREAMING_MIN_SIZE, Some("1")),
        ],
        test_fn(),
    ))
    .catch_unwind()
    .await;
    rustfs_io_metrics::set_get_stage_metrics_enabled(metrics_was_enabled);
    if let Err(err) = result {
        std::panic::resume_unwind(err);
    }
}

#[test]
#[serial]
#[ignore = "global-state ILM integration test: runs serialized in the CI ILM Integration (serial) lane, see ci.yml test-ilm-integration-serial and rustfs/backlog#1148 (ilm-1)"]
fn get_transitioned_object_uses_remote_codec_fallback_path() {
    // CI's default test stack overflows this deep async path; run it on a dedicated large-stack thread.
    std::thread::Builder::new()
        .name("lifecycle-transition-remote-codec-fallback".to_string())
        .stack_size(16 * 1024 * 1024)
        .spawn(|| {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("remote codec fallback test runtime should build");

            runtime.block_on(async {
                with_get_codec_streaming_remote_probe_env(|| async {
                    let (_disk_paths, ecstore) = setup_test_env().await;

                    let tier_name = format!("COLDTIER{}", &Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
                    let backend = register_mock_tier(&tier_name).await;

                    let bucket = format!("test-api-get-remote-{}", &Uuid::new_v4().simple().to_string()[..8]);
                    let object = "test/remote-codec-fallback.txt";
                    let payload: Vec<u8> = (0..(1024 * 1024))
                        .map(|index| u8::try_from(index % 251).expect("payload byte fits in u8"))
                        .collect();

                    create_test_bucket(&ecstore, bucket.as_str()).await;

                    let uploaded = upload_test_object(&ecstore, bucket.as_str(), object, &payload).await;
                    let transition_opts = ObjectOptions {
                        transition: lifecycle::lifecycle_contract::TransitionOptions {
                            status: lifecycle::lifecycle_contract::TRANSITION_PENDING.to_string(),
                            tier: tier_name.clone(),
                            etag: uploaded.etag.clone().unwrap_or_default(),
                            ..Default::default()
                        },
                        version_id: uploaded.version_id.map(|version| version.to_string()),
                        versioned: true,
                        mod_time: uploaded.mod_time,
                        ..Default::default()
                    };
                    ecstore
                        .transition_object(bucket.as_str(), object, &transition_opts)
                        .await
                        .expect("Failed to transition object directly");

                    let transitioned = wait_for_transition(&ecstore, bucket.as_str(), object, TRANSITION_WAIT_TIMEOUT)
                        .await
                        .expect("object should transition before remote fallback GET");

                    assert_eq!(transitioned.transitioned_object.status, "complete");
                    assert_eq!(transitioned.transitioned_object.tier, tier_name);
                    assert!(!transitioned.transitioned_object.name.is_empty());
                    assert!(backend.contains(&transitioned.transitioned_object.name).await);

                    let actual = read_object_bytes(&ecstore, bucket.as_str(), object).await;
                    assert_eq!(actual, payload);
                })
                .await;
            });
        })
        .expect("remote codec fallback test thread should spawn")
        .join()
        .expect("remote codec fallback test thread should not panic");
}

/// Regression test for rustfs/rustfs#4827: a duplicate transition task that runs
/// after the winning transition completed must return early without a second
/// tier upload and without clobbering the winner's remote object / metadata.
///
/// The duplicate models the residual race left after the in-flight enqueue dedup
/// (#4839): a stale-snapshot task admitted after the winner released its claim.
/// On an unversioned bucket the duplicate's metadata re-read is eligible for the
/// GET metadata cache; before the fix the cache still held the pre-transition
/// entry (transition_object never invalidated it after persisting), so the
/// TRANSITION_COMPLETE early-return never fired and the duplicate streamed the
/// already-deleted local data to the tier again (NotFound reader errors +
/// rejected duplicate tier PUT).
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[serial]
#[ignore = "global-state ILM integration test: runs serialized in the CI ILM Integration (serial) lane, see ci.yml test-ilm-integration-serial and rustfs/backlog#1148 (ilm-1)"]
async fn duplicate_transition_task_skips_already_transitioned_version() {
    let (_disk_paths, ecstore) = setup_test_env().await;

    let tier_name = format!("COLDTIER{}", &Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
    let backend = register_mock_tier(&tier_name).await;

    let bucket = format!("test-api-dup-transition-{}", &Uuid::new_v4().simple().to_string()[..8]);
    let object = "test/duplicate-transition.txt";
    let payload = b"duplicate transition tasks must not re-upload";

    // Unversioned bucket: the duplicate's re-read goes through the GET metadata
    // cache (versioned reads bypass it), which is the stale-read window under test.
    (*ecstore)
        .make_bucket(bucket.as_str(), &MakeBucketOptions::default())
        .await
        .expect("Failed to create unversioned test bucket");

    let uploaded = upload_test_object(&ecstore, bucket.as_str(), object, payload).await;
    let transition_opts = ObjectOptions {
        transition: lifecycle::lifecycle_contract::TransitionOptions {
            status: lifecycle::lifecycle_contract::TRANSITION_PENDING.to_string(),
            tier: tier_name.clone(),
            etag: uploaded.etag.clone().unwrap_or_default(),
            ..Default::default()
        },
        version_id: uploaded.version_id.map(|version| version.to_string()),
        mod_time: uploaded.mod_time,
        ..Default::default()
    };

    // Winner: transitions the object, persists transition_status=complete, and
    // deletes the local data.
    ecstore
        .transition_object(bucket.as_str(), object, &transition_opts)
        .await
        .expect("winning transition should succeed");

    let puts_after_winner: Vec<String> = backend
        .op_log()
        .await
        .into_iter()
        .filter_map(|op| match op {
            MockWarmOp::Put { object, .. } => Some(object),
            _ => None,
        })
        .collect();
    assert_eq!(puts_after_winner.len(), 1, "winner must upload exactly one tier object");
    let winner_remote_object = puts_after_winner[0].clone();

    // Duplicate task with the same stale snapshot (same mod_time / etag), run
    // immediately so the pre-fix stale metadata-cache entry is still live. It
    // must observe transition_status=complete and return Ok without uploading.
    ecstore
        .transition_object(bucket.as_str(), object, &transition_opts)
        .await
        .expect("duplicate transition must return early without error");

    let puts_after_duplicate = backend
        .op_log()
        .await
        .into_iter()
        .filter(|op| matches!(op, MockWarmOp::Put { .. }))
        .count();
    assert_eq!(puts_after_duplicate, 1, "duplicate transition must not attempt a second tier upload");

    // The winner's remote object and local metadata must be intact.
    assert!(backend.contains(&winner_remote_object).await);
    let info = wait_for_transition(&ecstore, bucket.as_str(), object, TRANSITION_WAIT_TIMEOUT)
        .await
        .expect("object must remain transitioned after duplicate task");
    assert_eq!(info.transitioned_object.status, "complete");
    assert_eq!(info.transitioned_object.tier, tier_name);
    assert_eq!(
        info.transitioned_object.name, winner_remote_object,
        "duplicate transition must not repoint metadata at a new remote object"
    );
    assert_eq!(read_object_bytes(&ecstore, bucket.as_str(), object).await, payload);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
#[ignore = "global-state ILM integration test: runs serialized in the CI ILM Integration (serial) lane, see ci.yml test-ilm-integration-serial and rustfs/backlog#1148 (ilm-1)"]
async fn transition_rejects_stale_etag_before_remote_upload() {
    let (_disk_paths, ecstore) = setup_test_env().await;
    let tier_name = format!("COLDTIER{}", &Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
    let backend = register_mock_tier(&tier_name).await;
    let bucket = format!("test-api-transition-stale-etag-{}", &Uuid::new_v4().simple().to_string()[..8]);
    let object = "test/transition-stale-etag.txt";
    (*ecstore)
        .make_bucket(bucket.as_str(), &MakeBucketOptions::default())
        .await
        .expect("unversioned test bucket should be created");
    let original = upload_test_object(&ecstore, bucket.as_str(), object, b"original transition candidate").await;
    let replacement_payload = b"replacement committed before the stale transition starts";
    let replacement = upload_test_object(&ecstore, bucket.as_str(), object, replacement_payload).await;
    let transition_opts = ObjectOptions {
        transition: lifecycle::lifecycle_contract::TransitionOptions {
            status: lifecycle::lifecycle_contract::TRANSITION_PENDING.to_string(),
            tier: tier_name,
            etag: original.etag.clone().unwrap_or_default(),
            ..Default::default()
        },
        version_id: replacement.version_id.map(|version| version.to_string()),
        mod_time: replacement.mod_time,
        ..Default::default()
    };

    ecstore
        .transition_object(bucket.as_str(), object, &transition_opts)
        .await
        .expect_err("stale ETag must reject the transition before remote upload");
    assert_eq!(backend.put_count().await, 0, "rejected stale work must not upload a remote candidate");
    assert_eq!(
        read_object_bytes(&ecstore, bucket.as_str(), object).await,
        replacement_payload,
        "the replacement object must remain local and readable"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
#[ignore = "global-state ILM integration test: runs serialized in the CI ILM Integration (serial) lane, see ci.yml test-ilm-integration-serial and rustfs/backlog#1148 (ilm-1)"]
async fn transition_commit_rejects_replaced_source() {
    let (_disk_paths, ecstore) = setup_test_env().await;
    let tier_name = format!("COLDTIER{}", &Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
    let backend = register_mock_tier(&tier_name).await;
    let bucket = format!("test-api-transition-replace-{}", &Uuid::new_v4().simple().to_string()[..8]);
    let object = "test/transition-replaced-source.txt";
    let original_payload = b"transition source that must not replace a concurrent overwrite";
    let replacement_payload = b"concurrent replacement must remain the authoritative local object";

    (*ecstore)
        .make_bucket(bucket.as_str(), &MakeBucketOptions::default())
        .await
        .expect("unversioned test bucket should be created");
    let original = upload_test_object(&ecstore, bucket.as_str(), object, original_payload).await;
    let transition_opts = ObjectOptions {
        transition: lifecycle::lifecycle_contract::TransitionOptions {
            status: lifecycle::lifecycle_contract::TRANSITION_PENDING.to_string(),
            tier: tier_name.clone(),
            etag: original.etag.clone().unwrap_or_default(),
            ..Default::default()
        },
        version_id: original.version_id.map(|version| version.to_string()),
        mod_time: original.mod_time,
        ..Default::default()
    };

    let put_barrier = backend.arm_put_barrier().await;
    let transition_store = Arc::clone(&ecstore);
    let transition_bucket = bucket.clone();
    let transition = tokio::spawn(async move {
        transition_store
            .transition_object(&transition_bucket, object, &transition_opts)
            .await
    });
    put_barrier.wait_until_paused().await;

    upload_test_object(&ecstore, bucket.as_str(), object, replacement_payload).await;
    put_barrier.release();

    transition
        .await
        .expect("transition task should not panic")
        .expect_err("transition must reject a source replaced during remote upload");
    assert_eq!(
        backend.put_count().await,
        1,
        "the stale transition should upload exactly one remote candidate"
    );
    assert_eq!(
        backend.remove_count().await,
        1,
        "the uncommitted remote candidate must be removed after identity revalidation fails"
    );
    assert_eq!(
        backend.remove_versions().await,
        backend.put_versions().await,
        "cleanup must delete the exact object version returned by the tier PUT"
    );
    assert_eq!(
        backend.object_count().await,
        0,
        "the rejected transition must not leave a remote candidate"
    );
    assert_eq!(
        read_object_bytes(&ecstore, bucket.as_str(), object).await,
        replacement_payload,
        "the replacement object must remain readable and unmodified"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
#[ignore = "global-state ILM integration test: runs serialized in the CI ILM Integration (serial) lane, see ci.yml test-ilm-integration-serial and rustfs/backlog#1148 (ilm-1)"]
async fn cancelled_transition_waiting_for_prepared_reader_cleans_remote() {
    temp_env::async_with_vars([(ENV_OBJECT_LOCK_OPTIMIZATION_ENABLE, Some("false"))], async {
        let (_disk_paths, ecstore) = setup_test_env().await;
        let tier_name = format!("COLDTIER{}", &Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
        let backend = register_mock_tier(&tier_name).await;
        let bucket = format!("test-api-transition-reader-lock-{}", &Uuid::new_v4().simple().to_string()[..8]);
        let object = "test/transition-reader-lock.txt";
        let payload = b"prepared reader must keep transition commit behind the namespace lock".repeat(1024);

        (*ecstore)
            .make_bucket(bucket.as_str(), &MakeBucketOptions::default())
            .await
            .expect("unversioned test bucket should be created");
        let original = upload_test_object(&ecstore, bucket.as_str(), object, &payload).await;
        let transition_opts = ObjectOptions {
            transition: lifecycle::lifecycle_contract::TransitionOptions {
                status: lifecycle::lifecycle_contract::TRANSITION_PENDING.to_string(),
                tier: tier_name,
                etag: original.etag.clone().unwrap_or_default(),
                ..Default::default()
            },
            version_id: original.version_id.map(|version| version.to_string()),
            mod_time: original.mod_time,
            ..Default::default()
        };

        let put_barrier = backend.arm_put_barrier().await;
        let transition_store = Arc::clone(&ecstore);
        let transition_bucket = bucket.clone();
        let mut transition = tokio::spawn(async move {
            transition_store
                .transition_object(&transition_bucket, object, &transition_opts)
                .await
        });
        tokio::select! {
            result = &mut transition => panic!("transition ended before reaching the tier PUT barrier: {result:?}"),
            () = put_barrier.wait_until_paused() => {}
        }

        let prepared_reader = ecstore
            .prepare_get_object_reader(bucket.as_str(), object, None, HeaderMap::new(), &ObjectOptions::default())
            .await
            .expect("prepared reader should resolve while the local source still exists");
        put_barrier.release();
        assert!(
            tokio::time::timeout(Duration::from_millis(200), &mut transition)
                .await
                .is_err(),
            "transition commit must wait while a prepared reader owns the namespace read lock"
        );

        transition.abort();
        assert!(
            transition
                .await
                .expect_err("transition task should be cancelled")
                .is_cancelled(),
            "transition cancellation should not be reported as a task panic"
        );
        drop(prepared_reader);
        assert_eq!(backend.put_count().await, 1, "the transition should upload exactly one remote candidate");
        assert!(
            backend.wait_for_object_count(0, TRANSITION_WAIT_TIMEOUT).await,
            "cancelling while the commit waits for the namespace lock must clean the exact uploaded candidate"
        );
        assert_eq!(backend.remove_versions().await, backend.put_versions().await);
        assert_eq!(read_object_bytes(&ecstore, bucket.as_str(), object).await, payload);
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[serial]
#[ignore = "global-state ILM integration test: runs serialized in the CI ILM Integration (serial) lane, see ci.yml test-ilm-integration-serial and rustfs/backlog#1148 (ilm-1)"]
async fn lifecycle_transition_marks_dirty_disks_for_capacity_manager() {
    let (disk_paths, ecstore) = setup_test_env().await;
    let manager = create_isolated_manager(HybridStrategyConfig::default());
    let _ = manager.get_dirty_disks().await;

    let tier_name = format!("COLDTIER{}", &Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
    let _backend = register_mock_tier(&tier_name).await;

    let bucket = format!("test-capacity-transition-{}", &Uuid::new_v4().simple().to_string()[..8]);
    let object = "test/object.txt";
    let payload = b"transition should mark dirty scope";

    create_test_bucket(&ecstore, bucket.as_str()).await;
    set_bucket_lifecycle_transition_with_tier(bucket.as_str(), &tier_name)
        .await
        .expect("Failed to set lifecycle configuration");
    let _ = upload_test_object(&ecstore, bucket.as_str(), object, payload).await;

    lifecycle::bucket_lifecycle_ops::enqueue_transition_for_existing_objects(ecstore.clone(), bucket.as_str())
        .await
        .expect("Failed to enqueue transitioned object");

    let _ = wait_for_transition(&ecstore, bucket.as_str(), object, TRANSITION_WAIT_TIMEOUT)
        .await
        .expect("object should transition before dirty scope assertion");

    let dirty_disks = manager.get_dirty_disks().await;
    assert_eq!(dirty_disks.len(), disk_paths.len());

    let actual_paths: std::collections::HashSet<_> = dirty_disks
        .into_iter()
        .map(|disk| stdfs::canonicalize(&disk.drive_path).unwrap().to_string_lossy().into_owned())
        .collect();
    let expected_paths: std::collections::HashSet<_> = disk_paths
        .iter()
        .map(|path| stdfs::canonicalize(path).unwrap().to_string_lossy().into_owned())
        .collect();
    assert_eq!(actual_paths, expected_paths);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[serial]
#[ignore = "global-state ILM integration test: runs serialized in the CI ILM Integration (serial) lane, see ci.yml test-ilm-integration-serial and rustfs/backlog#1148 (ilm-1)"]
async fn immediate_transition_timeout_eventually_completes_via_compensation() {
    let (_disk_paths, ecstore) = setup_test_env().await;
    let tier_name = format!("COLDTIER{}", &Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
    let backend = register_mock_tier(&tier_name).await;

    let bucket = format!("test-compensation-{}", &Uuid::new_v4().simple().to_string()[..8]);
    let object = "test/object.txt";
    let payload = b"transition compensation should eventually complete";

    create_test_bucket(&ecstore, bucket.as_str()).await;
    set_bucket_lifecycle_transition_with_tier(bucket.as_str(), &tier_name)
        .await
        .expect("Failed to set lifecycle configuration");

    with_forced_immediate_enqueue_timeout(&backend, || async {
        let _ = upload_test_object(&ecstore, bucket.as_str(), object, payload).await;
    })
    .await;

    let info = wait_for_transition(&ecstore, bucket.as_str(), object, TRANSITION_WAIT_TIMEOUT)
        .await
        .expect("object should eventually transition after compensation backfill");

    assert_eq!(backend.put_count().await, 1, "compensation should upload the version once");
    assert_eq!(info.transitioned_object.status, "complete");
    assert_eq!(info.transitioned_object.tier, tier_name);
    assert!(backend.contains(&info.transitioned_object.name).await);
}
