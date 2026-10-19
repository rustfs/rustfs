// Copyright 2026 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! The absent verdict for a heal target is decided by the storage quorum read,
//! not by the request source or the `recreate_missing` option (backlog#2900;
//! rustfs/rustfs#7997, #8132, #8235, #5950).

use super::*;

const OBSERVATION: &str = "buckets/.usage.observed.json";

fn missing_storage(outcome: MockHealObjectOutcome) -> Arc<MockStorage> {
    Arc::new(MockStorage {
        object_exists: Mutex::new(Some(false)),
        heal_object_outcome: Mutex::new(Some(outcome)),
        ..Default::default()
    })
}

fn object_task(
    bucket: &str,
    object: &str,
    version: Option<&str>,
    source: HealRequestSource,
    recreate_missing: bool,
    storage: Arc<MockStorage>,
) -> HealTask {
    let mut request = HealRequest::new(
        HealType::Object {
            bucket: bucket.to_string(),
            object: object.to_string(),
            version_id: version.map(str::to_string),
        },
        HealOptions {
            recreate_missing,
            timeout: None,
            ..Default::default()
        },
        HealPriority::Normal,
    );
    request.source = source;
    HealTask::from_request(request, storage)
}

const NON_MRF_SOURCES: [HealRequestSource; 5] = [
    HealRequestSource::Admin,
    HealRequestSource::Internal,
    HealRequestSource::AutoHeal,
    HealRequestSource::ReadRepair,
    HealRequestSource::Scanner,
];

#[tokio::test]
async fn absent_object_completes_for_every_source_regardless_of_recreate_missing() {
    for source in NON_MRF_SOURCES {
        for recreate_missing in [false, true] {
            for outer in [false, true] {
                let storage = missing_storage(MockHealObjectOutcome::MissingObject { outer });
                let task = object_task("bucket-a", "x.rnd", None, source, recreate_missing, storage.clone());
                task.execute()
                    .await
                    .unwrap_or_else(|err| panic!("{source:?} recreate={recreate_missing} outer={outer}: {err}"));
                assert_eq!(
                    task.get_status().await,
                    HealTaskStatus::Completed,
                    "{source:?} recreate={recreate_missing}"
                );
                let outcome = task.get_outcome().await;
                assert_eq!(outcome.counters.failed, 0, "{source:?} recreate={recreate_missing}");
                // One locked storage probe decides the verdict; no second recreate call follows.
                assert_eq!(
                    storage.heal_object_calls.lock().unwrap().as_slice(),
                    ["x.rnd".to_string()],
                    "{source:?} recreate={recreate_missing}"
                );
                let opts = storage.object_heal_opts.lock().unwrap();
                assert_eq!(opts.len(), 1);
                assert_eq!(opts[0].recreate, recreate_missing, "the option is still parsed and forwarded");
            }
        }
    }
}

#[tokio::test]
async fn absent_object_with_explicit_version_is_absent_for_read_repair() {
    for outcome in [
        MockHealObjectOutcome::MissingObject { outer: false },
        MockHealObjectOutcome::MissingVersion,
    ] {
        let storage = missing_storage(outcome);
        let task = object_task(
            "bucket-a",
            "x.rnd",
            Some("version-a"),
            HealRequestSource::ReadRepair,
            true,
            storage.clone(),
        );
        task.execute()
            .await
            .expect("a missing version is absent, not recreate_failed");
        assert_eq!(task.get_status().await, HealTaskStatus::Completed);
        assert_eq!(task.get_outcome().await.counters.failed, 0);
    }
}

#[tokio::test]
async fn absent_usage_observation_follows_the_general_not_found_rule() {
    // rustfs/rustfs#8132: previously only the exact (ReadRepair, meta bucket,
    // unversioned) tuple was excused through a path whitelist.
    for (bucket, object, source, version) in [
        (RUSTFS_META_BUCKET, OBSERVATION, HealRequestSource::ReadRepair, None),
        (RUSTFS_META_BUCKET, OBSERVATION, HealRequestSource::ReadRepair, Some("version-a")),
        (RUSTFS_META_BUCKET, OBSERVATION, HealRequestSource::Admin, None),
        (RUSTFS_META_BUCKET, OBSERVATION, HealRequestSource::Scanner, None),
        (RUSTFS_META_BUCKET, "buckets/.usage.json", HealRequestSource::ReadRepair, None),
        ("user-bucket", OBSERVATION, HealRequestSource::AutoHeal, None),
    ] {
        let storage = missing_storage(MockHealObjectOutcome::MissingObject { outer: false });
        let task = object_task(bucket, object, version, source, true, storage);
        task.execute()
            .await
            .unwrap_or_else(|err| panic!("{bucket}/{object} {source:?}: {err}"));
        let outcome = task.get_outcome().await;
        assert_eq!(outcome.execution, HealExecutionOutcome::Completed, "{bucket}/{object} {source:?}");
        assert_eq!(outcome.counters.failed, 0, "{bucket}/{object} {source:?}");
    }
}

#[tokio::test]
async fn absent_object_dir_candidate_follows_the_general_not_found_rule() {
    // rustfs/rustfs#3839 excused only scanner-sourced trailing-slash candidates.
    for source in NON_MRF_SOURCES {
        for outcome in [
            MockHealObjectOutcome::MissingObject { outer: false },
            MockHealObjectOutcome::OkWithOtherError("File not found"),
            MockHealObjectOutcome::ErrOther("File not found"),
        ] {
            let storage = missing_storage(outcome);
            let task = object_task("bucket-a", "x.rnd/", None, source, true, storage);
            task.execute().await.unwrap_or_else(|err| panic!("{source:?}: {err}"));
            assert_eq!(task.get_status().await, HealTaskStatus::Completed, "{source:?}");
        }
    }
}

#[tokio::test]
async fn absent_legacy_display_not_found_is_absent_for_read_repair() {
    // An old peer can still render the typed error as its display text.
    for message in ["File not found", "File version not found", "Object not found: bucket-a/x.rnd"] {
        let storage = missing_storage(MockHealObjectOutcome::ErrOther(message));
        let task = object_task("bucket-a", "x.rnd", None, HealRequestSource::ReadRepair, true, storage);
        task.execute().await.unwrap_or_else(|err| panic!("{message:?}: {err}"));
        assert_eq!(task.get_status().await, HealTaskStatus::Completed, "{message:?}");
    }
}

#[tokio::test]
async fn not_found_verdict_is_not_granted_to_disk_or_quorum_failures() {
    // backlog#856 / #799 B7: an offline drive or a lost quorum is never absence.
    for (outcome, recoverable) in [
        (MockHealObjectOutcome::FaultyStorageDisk(false), true),
        (MockHealObjectOutcome::FaultyStorageDisk(true), true),
        (MockHealObjectOutcome::OkWithReadQuorum, true),
        (MockHealObjectOutcome::RetryableReadQuorum, true),
        (MockHealObjectOutcome::RetryableSlowDown, true),
        (MockHealObjectOutcome::ErrOther("Disk not found"), false),
        (MockHealObjectOutcome::PermissionDenied, false),
    ] {
        for source in [
            HealRequestSource::ReadRepair,
            HealRequestSource::Admin,
            HealRequestSource::Scanner,
        ] {
            let storage = missing_storage(outcome.clone());
            let task = object_task("bucket-a", "x.rnd", None, source, true, storage);
            let err = task
                .execute()
                .await
                .expect_err("non-absence failures must keep failing the attempt");
            assert_eq!(err.is_recoverable_heal(), recoverable, "{source:?}: {err}");
            assert!(matches!(task.get_status().await, HealTaskStatus::Failed { .. }), "{source:?}");
            assert_eq!(task.get_outcome().await.counters.failed, 1, "{source:?}");
        }
    }
}

#[tokio::test]
async fn not_found_from_lowercase_free_text_is_not_absent() {
    // Only the canonical typed displays convert back; arbitrary text stays a failure.
    let storage = missing_storage(MockHealObjectOutcome::ErrOther("file not found while listing"));
    let task = object_task("bucket-a", "x.rnd", None, HealRequestSource::ReadRepair, true, storage);
    task.execute()
        .await
        .expect_err("free text must not become an absence verdict");
    assert!(matches!(task.get_status().await, HealTaskStatus::Failed { .. }));
}

#[tokio::test]
async fn recreate_missing_mrf_durable_repair_still_requires_storage_proof() {
    // The durable MRF path is unchanged: absence without an exact receipt
    // cannot retire the responsibility.
    let storage = missing_storage(MockHealObjectOutcome::MissingObject { outer: false });
    *storage.bucket_incarnation_id.lock().expect("bucket incarnation") = Some(Uuid::new_v4());
    let task = object_task(RUSTFS_META_BUCKET, OBSERVATION, None, HealRequestSource::Mrf, true, storage);
    task.execute()
        .await
        .expect_err("MRF responsibility cannot retire without proof");
    assert_eq!(task.get_outcome().await.counters.failed, 1);
}
