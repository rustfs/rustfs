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

use super::*;

const OBSERVATION: &str = "buckets/.usage.observed.json";

fn task(bucket: &str, object: &str, source: HealRequestSource, version: Option<&str>, storage: Arc<MockStorage>) -> HealTask {
    let mut request = HealRequest::new(
        HealType::Object {
            bucket: bucket.to_string(),
            object: object.to_string(),
            version_id: version.map(str::to_string),
        },
        HealOptions {
            recreate_missing: true,
            ..Default::default()
        },
        HealPriority::Low,
    );
    request.source = source;
    HealTask::from_request(request, storage)
}

fn missing_storage(error: MockHealObjectOutcome) -> Arc<MockStorage> {
    Arc::new(MockStorage {
        object_exists: Mutex::new(Some(false)),
        heal_object_outcome: Mutex::new(Some(error)),
        ..Default::default()
    })
}

#[tokio::test]
async fn missing_usage_observation_completes_without_claiming_repair_or_absence_proof() {
    for outer in [false, true] {
        let storage = missing_storage(MockHealObjectOutcome::MissingObject { outer });
        let task = task(RUSTFS_META_BUCKET, OBSERVATION, HealRequestSource::ReadRepair, None, storage.clone());
        task.execute()
            .await
            .expect("a removed observation is a normal read-repair skip");
        assert_eq!(task.get_status().await, HealTaskStatus::Completed);
        let outcome = task.get_outcome().await;
        assert_eq!(outcome.execution, HealExecutionOutcome::Completed);
        assert_eq!(outcome.counters.failed, 0);
        assert_eq!(outcome.counters.skipped, 1);
        assert_eq!(outcome.objects[0].disposition, HealObjectDisposition::Unknown);
        let progress = task.get_progress().await;
        assert_eq!(progress.objects_healed, 0);
        assert_eq!(progress.skipped_objects, 1);
        let calls = storage.object_heal_opts.lock().expect("heal calls");
        assert_eq!(calls.len(), 1);
        assert!(calls[0].recreate, "the test must reach the actual recreation branch");
    }
}

#[tokio::test]
async fn missing_usage_observation_exclusion_is_limited_to_the_exact_read_repair_target() {
    for (bucket, object, source, version) in [
        ("user-bucket", OBSERVATION, HealRequestSource::ReadRepair, None),
        (RUSTFS_META_BUCKET, "buckets/.usage.json", HealRequestSource::ReadRepair, None),
        (
            RUSTFS_META_BUCKET,
            "buckets/.usage.observed.json.bkp",
            HealRequestSource::ReadRepair,
            None,
        ),
        (
            RUSTFS_META_BUCKET,
            "buckets/nested/.usage.observed.json",
            HealRequestSource::ReadRepair,
            None,
        ),
        (RUSTFS_META_BUCKET, OBSERVATION, HealRequestSource::Admin, None),
        (RUSTFS_META_BUCKET, OBSERVATION, HealRequestSource::Internal, None),
        (RUSTFS_META_BUCKET, OBSERVATION, HealRequestSource::Scanner, None),
        (RUSTFS_META_BUCKET, OBSERVATION, HealRequestSource::AutoHeal, None),
        (RUSTFS_META_BUCKET, OBSERVATION, HealRequestSource::ReadRepair, Some("version-a")),
    ] {
        let storage = missing_storage(MockHealObjectOutcome::MissingObject { outer: false });
        let task = task(bucket, object, source, version, storage);
        task.execute()
            .await
            .expect_err("other requests must retain their missing-object failure");
        assert_eq!(task.get_outcome().await.counters.failed, 1, "{bucket}/{object}: {source:?}");
    }
}

#[tokio::test]
async fn usage_observation_does_not_hide_non_absence_errors() {
    for error in [
        MockHealObjectOutcome::PermissionDenied,
        MockHealObjectOutcome::OkWithReadQuorum,
        MockHealObjectOutcome::FaultyStorageDisk(false),
        MockHealObjectOutcome::MissingVersion,
        MockHealObjectOutcome::ErrOther("File not found"),
    ] {
        let storage = missing_storage(error);
        let task = task(RUSTFS_META_BUCKET, OBSERVATION, HealRequestSource::ReadRepair, None, storage);
        task.execute().await.expect_err("only a typed object absence may be skipped");
        assert_eq!(task.get_outcome().await.counters.failed, 1);
    }
}

#[tokio::test]
async fn existing_usage_observation_still_reaches_storage_repair() {
    let storage = Arc::new(MockStorage::default());
    let task = task(RUSTFS_META_BUCKET, OBSERVATION, HealRequestSource::ReadRepair, None, storage.clone());
    task.execute().await.expect("existing observation remains repairable");
    {
        let calls = storage.object_heal_opts.lock().expect("heal calls");
        assert_eq!(calls.len(), 1);
        assert!(calls[0].read_repair);
    }
    assert_eq!(task.get_progress().await.skipped_objects, 0);
}

#[tokio::test]
async fn missing_usage_observation_mrf_still_requires_storage_proof() {
    let storage = missing_storage(MockHealObjectOutcome::MissingObject { outer: false });
    *storage.bucket_incarnation_id.lock().expect("bucket incarnation") = Some(Uuid::new_v4());
    let task = task(RUSTFS_META_BUCKET, OBSERVATION, HealRequestSource::Mrf, None, storage);
    task.execute()
        .await
        .expect_err("MRF responsibility cannot retire on a path-name exception");
    assert_eq!(task.get_outcome().await.counters.failed, 1);
}
