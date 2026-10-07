// Copyright 2026 RustFS Team
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

use super::*;

#[path = "../../../../tests/storage_api.rs"]
mod real_disk_storage_api;

use real_disk_storage_api::integration::{ObjectIO as _, WriteCompletion};

fn future_deadline() -> u64 {
    now_secs() + 3600
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("test clock must be after the epoch")
        .as_secs()
}

async fn replacement_env() -> Env {
    let mut env = make_env_with_targets(vec!["replacement-a".to_owned()]).await;
    env.resume
        .cleanup()
        .await
        .expect("ordinary fixture state must be removed before binding the replacement generation");
    env.resume = ResumeManager::new_replacement_intent(
        env.healer.disk.clone(),
        env.task_id.clone(),
        "pool_0_set_0".to_owned(),
        vec!["b".to_owned()],
        vec!["replacement-a".to_owned()],
        vec![ReplacementTargetIdentity {
            endpoint: "replacement-a".to_owned(),
            canonical_path: "/mnt/replacement-a".to_owned(),
            physical_device_ids: vec!["device-a".to_owned()],
            filesystem_identity: "filesystem-a".to_owned(),
        }],
    )
    .await
    .expect("replacement intent should persist before scanning");
    env.healer = ErasureSetHealer::new(
        env.storage.clone(),
        Arc::new(RwLock::new(HealProgress::new())),
        CancellationToken::new(),
        env.healer.disk.clone(),
        HealOpts::default(),
        HealRequestSource::AutoHeal,
    )
    .with_replacement_targets(vec!["replacement-a".to_owned()], Some(env.task_id.clone()));
    env.storage
        .set_result(POOL_META_NAME, None, replacement_target_ok_result("replacement-a", POOL_META_NAME));
    env
}

async fn execute(env: &Env) -> Result<()> {
    env.healer
        .execute_heal_with_resume(&["b".to_owned()], "pool_0_set_0", &env.resume, &env.checkpoint)
        .await
}

fn grace_page(env: &Env, deadline: u64) {
    env.storage.set_page(
        None,
        Page {
            items: vec![item("dangling", Some("v1"), false)],
            next: None,
            truncated: false,
        },
    );
    env.storage
        .set_outcome("dangling", Some("v1"), HealOutcome::DanglingGrace(deadline));
    env.storage
        .set_result("dangling", Some("v1"), replacement_target_ok_result("replacement-a", "dangling"));
}

async fn persist_partial_grace_page(env: &Env, deadline: u64) {
    env.checkpoint
        .record_object_outcome(CheckpointObjectOutcomeRecord {
            object: compose_key("dangling", Some("v1")),
            outcome: CheckpointObjectOutcome::DeferredDanglingDelete {
                retry_not_before: deadline,
            },
            successful: 0,
            failed: 0,
            skipped: 1,
            bytes: 0,
            skipped_new_versions: 0,
            skipped_ilm_expired: 0,
            counter_unknown: false,
        })
        .await
        .expect("deferred object outcome should be recorded atomically");
    env.checkpoint
        .advance_page(0, 1)
        .await
        .expect("checkpoint should survive a crash before resume cursor publication");
}

#[tokio::test]
async fn dangling_grace_pass_preserves_replacement_retry_budget() {
    let env = replacement_env().await;
    env.storage.set_page(
        None,
        Page {
            items: vec![item("dangling", Some("v1"), false)],
            next: None,
            truncated: false,
        },
    );
    env.storage
        .set_outcome("dangling", Some("v1"), HealOutcome::DanglingGrace(future_deadline()));

    execute(&env)
        .await
        .expect_err("dangling cleanup must remain pending during the grace window");

    let state = ResumeManager::load_replacement_intent(env.healer.disk.clone(), &env.task_id)
        .await
        .expect("grace-protected replacement should remain recoverable")
        .get_state()
        .await;
    assert_eq!(
        state.retry_count, 0,
        "waiting for the grace window must not spend the replacement retry budget"
    );
    assert!(!state.completed, "a deferred object must prevent replacement completion");
    assert_eq!(env.storage.calls(), vec![("dangling".to_owned(), Some("v1".to_owned()))]);
}

#[tokio::test]
async fn dangling_grace_restart_waits_without_replaying_and_finishes_after_expiration() {
    let mut env = replacement_env().await;
    let deadline = future_deadline();
    grace_page(&env, deadline);
    let error = execute(&env).await.expect_err("the first pass must defer cleanup");
    assert!(matches!(error, Error::DanglingDeleteDeferred { retry_not_before } if retry_not_before == deadline));

    for _ in 0..4 {
        env.resume = ResumeManager::load_replacement_intent(env.healer.disk.clone(), &env.task_id)
            .await
            .expect("each restarted executor should reload the same replacement generation");
        env.checkpoint = CheckpointManager::load_from_disk(env.healer.disk.clone(), &env.task_id)
            .await
            .expect("each restarted executor should reload the durable checkpoint");
        let error = execute(&env).await.expect_err("early execution must preserve the grace wait");
        assert!(matches!(error, Error::DanglingDeleteDeferred { retry_not_before } if retry_not_before == deadline));
        let state = env.resume.get_state().await;
        assert_eq!(state.retry_count, 0);
        assert_eq!(state.dangling_delete_retry_not_before, Some(deadline));
        assert!(!state.completed);
    }
    assert_eq!(
        env.storage.calls(),
        vec![("dangling".to_owned(), Some("v1".to_owned()))],
        "restart and early attempts must not replay protected cleanup"
    );

    env.resume
        .defer_retry_until_dangling_delete_ready(now_secs().saturating_sub(1))
        .await
        .expect("an expired durable wait should retain the same failure budget");
    env.storage.set_outcome("dangling", Some("v1"), HealOutcome::Ok);
    env.healer
        .heal_erasure_set(&["b".to_owned()], "pool_0_set_0")
        .await
        .expect("expiration must allow a fresh scan to verify the replacement");
    let state = ResumeManager::load_replacement_intent(env.healer.disk.clone(), &env.task_id)
        .await
        .expect("verified replacement intent must survive until marker cleanup")
        .get_state()
        .await;
    assert!(state.completed);
    assert_eq!(state.replacement_phase, crate::heal::resume::ReplacementPhase::Verified);
    assert_eq!(state.retry_count, 0);
    assert_eq!(state.dangling_delete_retry_not_before, None);
    assert_eq!(
        env.storage.calls().iter().filter(|(object, _)| object == "dangling").count(),
        2,
        "completion must follow a new object heal after expiration"
    );
}

#[tokio::test]
async fn dangling_grace_partial_page_checkpoint_restores_reason_without_rehealing_identity() {
    let mut env = replacement_env().await;
    let deadline = future_deadline();
    grace_page(&env, deadline);
    env.storage.set_page(
        None,
        Page {
            items: vec![item("dangling", Some("v1"), false), item("healthy", Some("v1"), false)],
            next: None,
            truncated: false,
        },
    );
    env.storage
        .set_result("healthy", Some("v1"), replacement_target_ok_result("replacement-a", "healthy"));
    persist_partial_grace_page(&env, deadline).await;
    env.resume = ResumeManager::load_replacement_intent(env.healer.disk.clone(), &env.task_id)
        .await
        .expect("resume state should survive the partial-page crash");
    env.checkpoint = CheckpointManager::load_from_disk(env.healer.disk.clone(), &env.task_id)
        .await
        .expect("partial-page grace reason must be durable");
    let checkpoint = env.checkpoint.get_checkpoint().await;
    assert_eq!(checkpoint.dangling_delete_grace_objects, 1);
    assert_eq!(checkpoint.dangling_delete_retry_not_before, Some(deadline));

    let error = execute(&env)
        .await
        .expect_err("a checkpointed grace outcome must still defer the whole pass");
    assert!(matches!(error, Error::DanglingDeleteDeferred { retry_not_before } if retry_not_before == deadline));
    let state = env.resume.get_state().await;
    assert_eq!(state.retry_count, 0, "checkpoint replay must retain the budget exemption");
    assert_eq!(state.dangling_delete_retry_not_before, Some(deadline));
    assert_eq!(env.storage.calls(), vec![("healthy".to_owned(), Some("v1".to_owned()))]);
}

#[tokio::test]
async fn dangling_grace_resume_reset_recovers_the_stale_checkpoint_before_rescan() {
    let env = replacement_env().await;
    let deadline = now_secs().saturating_sub(1);
    grace_page(&env, deadline);
    persist_partial_grace_page(&env, deadline).await;
    env.checkpoint
        .update_position(4, 9)
        .await
        .expect("a stale scan position should persist before the resume reset");
    env.resume
        .defer_retry_until_dangling_delete_ready(deadline)
        .await
        .expect("publish the resume reset before simulating a checkpoint reset crash");

    let (resumed, checkpoint) = env
        .healer
        .initialize_resume_state(&env.task_id, "pool_0_set_0", &["b".to_owned()])
        .await
        .expect("initialization should reconcile both persistence layers after the crash");
    let snapshot = checkpoint.get_checkpoint().await;
    assert_eq!(snapshot.current_bucket_index, 0);
    assert_eq!(snapshot.current_object_index, 0);
    assert!(
        snapshot.skipped_objects.is_empty(),
        "stale dedup cannot suppress the deferred version after expiration"
    );
    assert_eq!(snapshot.dangling_delete_grace_objects, 0);
    assert_eq!(snapshot.dangling_delete_retry_not_before, None);
    assert_eq!(resumed.get_state().await.retry_count, 0);

    env.storage.set_outcome("dangling", Some("v1"), HealOutcome::Ok);
    env.healer
        .execute_heal_with_resume(&["b".to_owned()], "pool_0_set_0", &resumed, &checkpoint)
        .await
        .expect("repaired checkpoint must let the expired version heal again");
    assert!(env.storage.calls().contains(&("dangling".to_owned(), Some("v1".to_owned()))));
}

#[tokio::test]
async fn dangling_grace_mixed_transient_failures_still_exhaust_the_bounded_budget() {
    let env = replacement_env().await;
    grace_page(&env, future_deadline());
    env.storage.set_page(
        None,
        Page {
            items: vec![item("dangling", Some("v1"), false), item("offline", Some("v1"), false)],
            next: None,
            truncated: false,
        },
    );
    env.storage.set_outcome("offline", Some("v1"), HealOutcome::Transient);
    let max_retries = env.resume.get_state().await.max_retries;
    for expected in 1..=max_retries {
        let error = execute(&env)
            .await
            .expect_err("ordinary infrastructure failures must keep the bounded retry path");
        assert!(matches!(error, Error::TransientSkip { .. }));
        let state = env.resume.get_state().await;
        assert_eq!(state.retry_count, expected);
        assert_eq!(
            state.dangling_delete_retry_not_before, None,
            "grace cannot hide the ordinary failure budget"
        );
        assert!(!state.completed);
    }
    execute(&env)
        .await
        .expect_err("a grace object must not reopen an exhausted ordinary failure budget");
    let state = env.resume.get_state().await;
    assert_eq!(state.retry_count, max_retries);
    assert_eq!(state.dangling_delete_retry_not_before, None);
    assert!(!state.completed);
}

#[tokio::test]
async fn dangling_grace_expiration_still_rejects_a_remounted_replacement_target() {
    let mut env = replacement_env().await;
    grace_page(&env, future_deadline());
    execute(&env).await.expect_err("initial grace should defer the replacement");
    env.resume
        .defer_retry_until_dangling_delete_ready(now_secs().saturating_sub(1))
        .await
        .expect("make the same generation ready for a new scan");
    env.storage.set_outcome("dangling", Some("v1"), HealOutcome::Ok);
    let expected = env.resume.get_state().await.replacement_target_identities;
    let mut remounted = expected.clone();
    remounted[0].physical_device_ids = vec!["device-b".to_owned()];
    remounted[0].filesystem_identity = "filesystem-b".to_owned();
    env.storage
        .replacement_target_identity_sequences
        .lock()
        .expect("identity fixture")
        .push_back(remounted);
    env.healer = env.healer.with_replacement_identity_fence(Some(expected));
    let calls_before = env.storage.calls();
    let listings_before = env.storage.list_include_lifecycle_object_info_calls();

    let error = execute(&env)
        .await
        .expect_err("expiration cannot admit another physical replacement identity");
    assert!(
        matches!(&error, Error::TransientSkip { message } if message.contains("1 bucket(s) failed")),
        "the set-level result must retain the failed identity-fenced bucket: {error}"
    );
    assert!(
        env.storage
            .replacement_target_identity_sequences
            .lock()
            .expect("identity fixture")
            .is_empty(),
        "the new scan must check the remounted target identity"
    );
    assert_eq!(
        env.storage.list_include_lifecycle_object_info_calls(),
        listings_before,
        "the identity fence must reject the remount before listing its object page"
    );
    assert_eq!(
        env.storage.calls(),
        calls_before,
        "a remount must be rejected before replaying any object"
    );
    let state = env.resume.get_state().await;
    assert!(!state.completed);
    assert_ne!(state.replacement_phase, crate::heal::resume::ReplacementPhase::Verified);
}

#[test]
#[serial_test::serial]
fn dangling_grace_real_storage_error_persists_a_rounded_deadline_without_spending_budget() {
    // Match the real-storage integration fixtures' stack budget for composed heal futures.
    const STACK_SIZE: usize = 8 * 1024 * 1024;
    std::thread::Builder::new()
        .name("dangling-grace-raw-storage".to_owned())
        .stack_size(STACK_SIZE)
        .spawn(|| {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(4)
                .thread_stack_size(STACK_SIZE)
                .enable_all()
                .build()
                .expect("real-storage grace runtime should build");
            runtime.block_on(temp_env::async_with_vars(
                [("RUSTFS_HEAL_DANGLING_DELETE_GRACE_SECS", Some("3600"))],
                real_storage_grace_round_trip(),
            ));
        })
        .expect("real-storage grace test thread should spawn")
        .join()
        .expect("real-storage grace test thread should finish");
}

async fn real_storage_grace_round_trip() {
    use crate::heal::storage::{ECStoreHealStorage, HealObjectOptions, HealPutObjReader};

    let directory = TempDir::new().expect("real-storage grace fixture directory");
    let real = rustfs_test_utils::TestECStoreEnv::builder()
        .base_dir(directory.path())
        .disk_count(4)
        .build()
        .await;
    let bucket = "typed-grace-fixture";
    let object = "dangling";
    real.make_bucket(bucket, false).await;
    real.ecstore
        .put_object(
            bucket,
            object,
            &mut HealPutObjReader::from_vec(b"recent inline shard".to_vec()),
            &HealObjectOptions {
                write_completion: WriteCompletion::TailDrained,
                ..Default::default()
            },
        )
        .await
        .expect("real PUT must finish its rename tails before simulating missing shards");
    let metadata_path = real.disk_paths[0].join(bucket).join(object).join("xl.meta");
    let metadata_before = std::fs::read(&metadata_path).expect("capture the surviving recent inline metadata");
    for disk in &real.disk_paths[1..] {
        std::fs::remove_dir_all(disk.join(bucket).join(object)).expect("remove three completed copies below the read quorum");
    }
    let storage = ECStoreHealStorage::new(real.ecstore.clone());
    let (_, error) = storage
        .heal_object(
            bucket,
            object,
            None,
            &HealOpts {
                scan_mode: rustfs_heal_contracts::heal_channel::HealScanMode::Deep,
                recreate: true,
                pool: Some(0),
                set: Some(0),
                ..Default::default()
            },
        )
        .await
        .expect("real dangling cleanup should return a typed heal result");
    let Error::Storage(error) = error.expect("the surviving recent shard must be grace-protected") else {
        panic!("real ECStore heal must retain its Storage error wrapper");
    };
    assert!(error.is_dangling_delete_grace(), "the fixture must return a real grace marker: {error}");
    let retry_after = error
        .dangling_delete_retry_after()
        .expect("the real grace error must retain typed retry timing")
        .as_secs();
    assert!(retry_after > 0, "the new object must have a remaining grace window");
    assert_eq!(
        std::fs::read(&metadata_path).expect("read protected metadata after real heal"),
        metadata_before
    );

    let env = replacement_env().await;
    grace_page(&env, future_deadline());
    env.storage
        .set_outcome("dangling", Some("v1"), HealOutcome::StorageError(error));
    let before = now_secs();
    let result = execute(&env).await.expect_err("raw storage grace must defer the replacement");
    let after = now_secs();
    let Error::DanglingDeleteDeferred { retry_not_before } = result else {
        panic!("raw storage grace must become a typed absolute-deadline wait: {result}");
    };
    assert!(
        (before + retry_after + 1..=after + retry_after + 1).contains(&retry_not_before),
        "classification must round the raw whole-second delay up at the epoch boundary"
    );
    let state = ResumeManager::load_replacement_intent(env.healer.disk.clone(), &env.task_id)
        .await
        .expect("reload the raw-error grace wait from the survivor anchor")
        .get_state()
        .await;
    assert_eq!(
        state.retry_count, 0,
        "raw storage errors must use the same budget exemption as typed waits"
    );
    assert_eq!(state.dangling_delete_retry_not_before, Some(retry_not_before));
    assert!(!state.completed);
    assert_eq!(
        std::fs::read(&metadata_path).expect("read surviving metadata after classification"),
        metadata_before
    );
}
