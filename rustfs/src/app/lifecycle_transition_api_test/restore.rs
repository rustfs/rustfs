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

//! RestoreObject tests (batch E owns RestoreObject).

use super::common::*;
use super::*;

const RESTORE_SUSPENDED_WAIT_TIMEOUT: Duration = Duration::from_secs(15);

async fn suspend_test_bucket(bucket: &str) {
    DefaultBucketUsecase::from_global()
        .execute_put_bucket_versioning(
            build_request(
                PutBucketVersioningInput::builder()
                    .bucket(bucket.to_string())
                    .versioning_configuration(VersioningConfiguration {
                        status: Some(BucketVersioningStatus::from_static(BucketVersioningStatus::SUSPENDED)),
                        ..Default::default()
                    })
                    .build()
                    .expect("suspended versioning request should build"),
                Method::PUT,
            ),
            Ok(rustfs_gateway_types::persistence::PersistedVersioningConfiguration {
                status: Some("Suspended".to_owned()),
                ..Default::default()
            }),
        )
        .await
        .expect("bucket versioning should be suspended");
}

async fn transition_uploaded_object_directly(
    ecstore: &Arc<ECStore>,
    bucket: &str,
    object: &str,
    tier_name: &str,
    uploaded: &ObjectInfo,
) -> ObjectInfo {
    let transition_opts = ObjectOptions {
        transition: lifecycle::lifecycle_contract::TransitionOptions {
            status: lifecycle::lifecycle_contract::TRANSITION_PENDING.to_string(),
            tier: tier_name.to_string(),
            etag: uploaded.etag.clone().unwrap_or_default(),
            ..Default::default()
        },
        version_id: uploaded.version_id.map(|version| version.to_string()),
        versioned: uploaded.version_id.is_some(),
        mod_time: uploaded.mod_time,
        ..Default::default()
    };
    ecstore
        .transition_object(bucket, object, &transition_opts)
        .await
        .expect("Failed to transition object directly");
    wait_for_transition(ecstore, bucket, object, TRANSITION_WAIT_TIMEOUT)
        .await
        .expect("object should transition before restore assertions")
}

async fn wait_for_restore_completion(
    ecstore: &Arc<ECStore>,
    backend: &MockWarmBackend,
    bucket: &str,
    object: &str,
    timeout: Duration,
) -> Result<ObjectInfo, String> {
    let deadline = tokio::time::Instant::now() + timeout;
    let mut last_state = None;

    loop {
        if tokio::time::Instant::now() >= deadline {
            let tier_gets = backend.get_count().await;
            let op_log = backend.op_log().await;
            return Err(format!(
                "restore copy-back should complete within {timeout:?}; tier_gets={tier_gets}, op_log={op_log:?}; last observed state: {}",
                last_state.unwrap_or_else(|| "no object info observed".to_string())
            ));
        }

        match (**ecstore).get_object_info(bucket, object, &ObjectOptions::default()).await {
            Ok(info) => {
                if !info.restore_ongoing && info.restore_expires.is_some() {
                    return Ok(info);
                }
                last_state = Some(format!(
                    "restore_ongoing={}, restore_expires={:?}, transitioned_status={}",
                    info.restore_ongoing, info.restore_expires, info.transitioned_object.status
                ));
            }
            Err(err) => {
                last_state = Some(format!("get_object_info failed: {err}"));
            }
        }

        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// backlog#1148 ilm-8: the RestoreObject API surface on a transitioned object.
///
/// POST restore(days=1) is accepted and flips the object to
/// `x-amz-restore: ongoing-request="true"` while the mock tier GET barrier
/// proves the background copy-back has reached the remote read; a second POST
/// during that window is rejected with 409 `RestoreAlreadyInProgress`.
/// Synchronous SetDisks transition tests cover copy-back completion, restore
/// metadata, and local byte-identical reads.
///
/// Re-enabled in the serial lane by backlog#1304: the accept path now flips
/// the ongoing flag under a short compare-and-set guard and the copy-back
/// runs without the #4877 whole-copy-back lock, so the mid-restore
/// `ongoing-request="true"` read and the fast 409 rejection asserted here are
/// the implemented contract.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
#[ignore = "global-state ILM integration test: runs serialized in the CI ILM Integration (serial) lane, see ci.yml test-ilm-integration-serial and rustfs/backlog#1148 (ilm-8)"]
async fn restore_object_usecase_reports_ongoing_conflict() {
    let (_disk_paths, ecstore) = setup_test_env().await;
    let usecase = DefaultObjectUsecase::from_global();

    let tier_name = format!("COLDTIER{}", &Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
    let backend = register_mock_tier(&tier_name).await;

    let bucket = format!("test-api-restore-{}", &Uuid::new_v4().simple().to_string()[..8]);
    // Keep the object under the shared ILM test prefix even though this setup
    // transitions it directly; it keeps diagnostics aligned with sibling tests.
    let object = "test/restore/api-object.bin";
    let payload: Vec<u8> = (0..128 * 1024).map(|i| (i % 251) as u8).collect();

    create_test_bucket(&ecstore, bucket.as_str()).await;
    let uploaded = upload_test_object(&ecstore, bucket.as_str(), object, &payload).await;
    assert!(uploaded.version_id.is_none(), "fixture must create the null version");
    let _ = transition_uploaded_object_directly(&ecstore, bucket.as_str(), object, &tier_name, &uploaded).await;
    backend.clear_op_log().await;
    let get_barrier = backend.arm_get_barrier().await;

    let restore_request = || RestoreRequest {
        days: Some(1),
        description: None,
        glacier_job_parameters: None,
        output_location: None,
        select_parameters: None,
        tier: None,
        type_: None,
    };
    let restore_input = || {
        RestoreObjectInput::builder()
            .bucket(bucket.clone())
            .key(object.to_string())
            .restore_request(Some(restore_request()))
            .build()
            .unwrap()
    };

    Box::pin(usecase.execute_restore_object(build_request(restore_input(), Method::POST)))
        .await
        .expect("restore request should be accepted");

    get_barrier.wait_until_paused().await;

    // The barrier proves the detached copy-back reached the tier GET and is
    // still paused, so the ongoing state and conflict rejection are not timing
    // assumptions about task scheduling.
    let ongoing = ecstore
        .get_object_info(bucket.as_str(), object, &ObjectOptions::default())
        .await
        .expect("Failed to load object info during restore");
    assert!(
        ongoing.restore_ongoing,
        "x-amz-restore must report ongoing-request=true while the copy-back tier GET is paused"
    );

    // A second restore while one is in flight is rejected.
    let err = Box::pin(usecase.execute_restore_object(build_request(restore_input(), Method::POST)))
        .await
        .expect_err("a second restore while one is ongoing must be rejected");
    assert_eq!(
        err.code(),
        &S3ErrorCode::RestoreAlreadyInProgress,
        "unexpected rejection for a repeated restore: {err:?}"
    );

    get_barrier.release();
}

/// rustfs/backlog#1337: cancellation after the ongoing metadata commit but
/// before detached worker creation must not strand the object forever. The
/// replacement POST proves the abandoned v1 generation has no live worker,
/// atomically supersedes it, and performs exactly one remote copy-back.
#[serial]
#[ignore = "global-state ILM integration test: runs serialized in the CI ILM Integration (serial) lane, see ci.yml test-ilm-integration-serial and rustfs/backlog#1337"]
#[test]
fn restore_object_usecase_recovers_cancelled_post_commit_generation() {
    std::thread::Builder::new()
        .name("lifecycle-restore-orphan-recovery".to_string())
        .stack_size(32 * 1024 * 1024)
        .spawn(|| {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("orphan restore recovery test runtime should build");
            runtime.block_on(restore_object_usecase_recovers_cancelled_post_commit_generation_inner());
        })
        .expect("orphan restore recovery test thread should spawn")
        .join()
        .expect("orphan restore recovery test thread should finish");
}

async fn restore_object_usecase_recovers_cancelled_post_commit_generation_inner() {
    let (_disk_paths, ecstore) = setup_test_env().await;
    let tier_name = format!("COLDTIER{}", &Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
    let backend = register_mock_tier(&tier_name).await;
    let bucket = format!("test-api-restore-orphan-{}", &Uuid::new_v4().simple().to_string()[..8]);
    let object = "test/restore/orphaned-generation.bin";
    let payload: Vec<u8> = (0..128 * 1024).map(|i| (i % 251) as u8).collect();

    create_test_bucket(&ecstore, bucket.as_str()).await;
    let uploaded = upload_test_object(&ecstore, bucket.as_str(), object, &payload).await;
    let _ = transition_uploaded_object_directly(&ecstore, bucket.as_str(), object, &tier_name, &uploaded).await;
    backend.clear_op_log().await;
    let tier_gets_before_restore = backend.get_count().await;

    let restore_input = || {
        RestoreObjectInput::builder()
            .bucket(bucket.clone())
            .key(object.to_string())
            .restore_request(Some(RestoreRequest {
                days: Some(1),
                description: None,
                glacier_job_parameters: None,
                output_location: None,
                select_parameters: None,
                tier: None,
                type_: None,
            }))
            .build()
            .expect("restore request should build")
    };

    let commit_barrier = crate::app::object::RestoreStatusCommitBarrier::install(bucket.as_str(), object);
    let first_input = restore_input();
    let first = tokio::spawn(async move {
        DefaultObjectUsecase::from_global()
            .execute_restore_object(build_request(first_input, Method::POST))
            .await
    });
    commit_barrier.wait_until_paused().await;

    first.abort();
    assert!(first.await.expect_err("the first request must be cancelled").is_cancelled());
    drop(commit_barrier);

    let orphaned = ecstore
        .get_object_info(bucket.as_str(), object, &ObjectOptions::default())
        .await
        .expect("the committed orphan generation should remain readable");
    assert!(orphaned.restore_ongoing, "the interrupted request must have committed ongoing=true");
    assert_eq!(
        rustfs_utils::http::get_consistent_str(orphaned.user_defined.as_ref(), rustfs_utils::http::SUFFIX_RESTORE_WORKER_LOCK,),
        Some(rustfs_utils::http::RESTORE_WORKER_LOCK_PROTOCOL_V1),
        "recoverable generations must advertise the worker-lock protocol"
    );
    let orphaned_operation_id =
        rustfs_utils::http::get_consistent_str(orphaned.user_defined.as_ref(), rustfs_utils::http::SUFFIX_RESTORE_OPERATION_ID)
            .and_then(|value| Uuid::parse_str(value).ok())
            .expect("recoverable generations must persist a non-nil operation id");
    assert!(!orphaned_operation_id.is_nil());

    DefaultObjectUsecase::from_global()
        .execute_restore_object(build_request(restore_input(), Method::POST))
        .await
        .expect("a new POST must supersede the committed generation whose worker lock was released");

    let completed = wait_for_restore_completion(&ecstore, &backend, bucket.as_str(), object, TRANSITION_WAIT_TIMEOUT)
        .await
        .unwrap_or_else(|err| panic!("{err}"));
    assert!(!completed.restore_ongoing);
    assert!(completed.restore_expires.is_some());
    assert!(
        rustfs_utils::http::get_str(completed.user_defined.as_ref(), rustfs_utils::http::SUFFIX_RESTORE_OPERATION_ID,).is_none(),
        "completed replacement restore must consume its operation id"
    );
    assert!(
        rustfs_utils::http::get_str(completed.user_defined.as_ref(), rustfs_utils::http::SUFFIX_RESTORE_WORKER_LOCK,).is_none(),
        "completed replacement restore must consume its liveness marker"
    );
    assert_eq!(
        backend.get_count().await - tier_gets_before_restore,
        1,
        "the cancelled pre-spawn generation must not issue a tier GET"
    );
}

#[serial]
#[ignore = "global-state ILM integration test: runs serialized in the CI ILM Integration (serial) lane, see ci.yml test-ilm-integration-serial and rustfs/backlog#4879"]
#[test]
fn restore_object_usecase_completes_suspended_null_version_in_place() {
    std::thread::Builder::new()
        .name("lifecycle-restore-suspended-null".to_string())
        .stack_size(32 * 1024 * 1024)
        .spawn(|| {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("suspended null-version restore test runtime should build");

            runtime.block_on(restore_object_usecase_completes_suspended_null_version_in_place_inner());
        })
        .expect("suspended null-version restore test thread should spawn")
        .join()
        .expect("suspended null-version restore test thread should finish");
}

async fn restore_object_usecase_completes_suspended_null_version_in_place_inner() {
    let (_disk_paths, ecstore) = setup_test_env().await;
    let usecase = DefaultObjectUsecase::from_global();
    let tier_name = format!("COLDTIER{}", &Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
    let backend = register_mock_tier(&tier_name).await;
    let bucket = format!("test-api-restore-suspended-{}", &Uuid::new_v4().simple().to_string()[..8]);
    let object = "test/restore/suspended-null.bin";
    let payload: Vec<u8> = (0..128 * 1024).map(|i| (i % 251) as u8).collect();

    create_test_bucket(&ecstore, bucket.as_str()).await;
    let uploaded = upload_test_object(&ecstore, bucket.as_str(), object, &payload).await;
    assert!(uploaded.version_id.is_none(), "fixture must create the null version");
    let _ = transition_uploaded_object_directly(&ecstore, bucket.as_str(), object, &tier_name, &uploaded).await;
    suspend_test_bucket(bucket.as_str()).await;
    backend.clear_op_log().await;
    let tier_gets_before_restore = backend.get_count().await;
    let get_barrier = backend.arm_get_barrier().await;

    Box::pin(
        usecase.execute_restore_object(build_request(
            RestoreObjectInput::builder()
                .bucket(bucket.clone())
                .key(object.to_string())
                .restore_request(Some(RestoreRequest {
                    days: Some(1),
                    description: None,
                    glacier_job_parameters: None,
                    output_location: None,
                    select_parameters: None,
                    tier: None,
                    type_: None,
                }))
                .build()
                .expect("restore request should build"),
            Method::POST,
        )),
    )
    .await
    .expect("suspended null-version restore should be accepted");

    get_barrier.wait_until_paused().await;
    get_barrier.release();
    let completed = wait_for_restore_completion(&ecstore, &backend, bucket.as_str(), object, RESTORE_SUSPENDED_WAIT_TIMEOUT)
        .await
        .unwrap_or_else(|err| panic!("{err}"));

    assert!(!completed.restore_ongoing, "the original null version must complete in place");
    assert!(completed.restore_expires.is_some(), "completed restore must carry an expiry");
    assert!(
        completed.version_id.is_none() || completed.version_id.is_some_and(|version_id| version_id.is_nil()),
        "suspended restore must remain on the null version"
    );
    assert_eq!(
        live_object_version_count(&ecstore, bucket.as_str(), object).await,
        1,
        "suspended restore must not create a UUID version"
    );
    assert_eq!(
        backend.get_count().await - tier_gets_before_restore,
        1,
        "suspended restore copy-back must fetch the tier exactly once"
    );
}

/// backlog#1304: the restore-accept compare-and-set itself, under real
/// concurrency. Two POST ?restore for the same transitioned object race each
/// other from separate tasks; the accept guard must let exactly one through —
/// the winner flips `ongoing-request="true"`, the loser must read that flag
/// and be rejected with 409 `RestoreAlreadyInProgress` — and the tier must
/// see exactly one copy-back GET, never two. Without the accept guard both
/// requests can observe ongoing=false and double-start the copy-back (the
/// #4859 race), which this test catches via the tier GET count.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
#[ignore = "global-state ILM integration test: runs serialized in the CI ILM Integration (serial) lane, see ci.yml test-ilm-integration-serial and rustfs/backlog#1148 (ilm-8)"]
async fn restore_object_usecase_accepts_exactly_one_of_two_concurrent_restores() {
    let (_disk_paths, ecstore) = setup_test_env().await;

    let tier_name = format!("COLDTIER{}", &Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
    let backend = register_mock_tier(&tier_name).await;

    let bucket = format!("test-api-restore-cas-{}", &Uuid::new_v4().simple().to_string()[..8]);
    // Keep the object under the shared ILM test prefix for diagnostics parity.
    let object = "test/restore/cas-object.bin";
    let payload: Vec<u8> = (0..128 * 1024).map(|i| (i % 251) as u8).collect();

    create_test_bucket(&ecstore, bucket.as_str()).await;
    let uploaded = upload_test_object(&ecstore, bucket.as_str(), object, &payload).await;
    let _ = transition_uploaded_object_directly(&ecstore, bucket.as_str(), object, &tier_name, &uploaded).await;
    backend.clear_op_log().await;

    // Hold the accepted copy-back at the tier GET until both accept attempts
    // return, so the loser cannot observe an already-restored object.
    let get_barrier = backend.arm_get_barrier().await;

    let tier_gets_before_restore = backend.get_count().await;

    let restore_input = || {
        RestoreObjectInput::builder()
            .bucket(bucket.clone())
            .key(object.to_string())
            .restore_request(Some(RestoreRequest {
                days: Some(1),
                description: None,
                glacier_job_parameters: None,
                output_location: None,
                select_parameters: None,
                tier: None,
                type_: None,
            }))
            .build()
            .unwrap()
    };
    let spawn_restore = |input: RestoreObjectInput| {
        tokio::spawn(async move {
            let usecase = DefaultObjectUsecase::from_global();
            Box::pin(usecase.execute_restore_object(build_request(input, Method::POST))).await
        })
    };

    let (first, second) = tokio::join!(spawn_restore(restore_input()), spawn_restore(restore_input()));
    let results = [
        first.expect("first concurrent restore task panicked"),
        second.expect("second concurrent restore task panicked"),
    ];

    let accepted = results.iter().filter(|result| result.is_ok()).count();
    assert_eq!(accepted, 1, "exactly one of two concurrent restores must be accepted, got {accepted}");
    let rejection = results
        .iter()
        .find_map(|result| result.as_ref().err())
        .expect("the losing concurrent restore must surface an error");
    assert_eq!(
        rejection.code(),
        &S3ErrorCode::RestoreAlreadyInProgress,
        "the losing concurrent restore must be rejected as already in progress: {rejection:?}"
    );

    get_barrier.wait_until_paused().await;
    get_barrier.release();

    // This test is scoped to the accept CAS: completion, expiry metadata, and
    // local restored GET service are covered by the single-request restore test
    // above. Here it is enough to prove exactly one copy-back was admitted.
    let expected_tier_gets = tier_gets_before_restore + 1;
    let deadline = tokio::time::Instant::now() + TRANSITION_WAIT_TIMEOUT;
    loop {
        let actual_tier_gets = backend.get_count().await;
        if actual_tier_gets >= expected_tier_gets {
            assert_eq!(
                actual_tier_gets - tier_gets_before_restore,
                1,
                "two concurrent restore requests must trigger exactly one tier copy-back GET"
            );
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            let op_log = backend.op_log().await;
            panic!(
                "mock tier should record exactly one restore GET within {TRANSITION_WAIT_TIMEOUT:?}; \
                 tier_gets_before_restore={tier_gets_before_restore}, actual_tier_gets={actual_tier_gets}, op_log={op_log:?}"
            );
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
