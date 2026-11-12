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

use super::tests::local_disks;
use super::*;

async fn reopen_disks(disks: &[Option<DiskStore>]) -> Vec<Option<DiskStore>> {
    let endpoints = Endpoints::from(
        disks
            .iter()
            .map(|disk| disk.as_ref().expect("fixture disk exists").endpoint())
            .collect::<Vec<_>>(),
    );
    let (reopened, errors) = init_disks(
        &InstanceContext::new(),
        &endpoints,
        &DiskOption {
            cleanup: false,
            health_check: false,
        },
    )
    .await;
    assert!(errors.iter().all(Option::is_none), "reopen persisted disks: {errors:?}");
    reopened
}

fn bootstrap_plan() -> Vec<Option<FormatV3>> {
    let reference = FormatV3::new(1, 4);
    (0..4)
        .map(|index| {
            let mut format = reference.clone();
            format.erasure.this = reference.erasure.sets[0][index];
            Some(format)
        })
        .collect()
}

async fn format_snapshot(disks: &[Option<DiskStore>]) -> Vec<Option<Vec<u8>>> {
    let mut snapshot = Vec::with_capacity(disks.len());
    for disk in disks {
        let path = disk
            .as_ref()
            .expect("snapshot disk exists")
            .path()
            .join(RUSTFS_META_BUCKET)
            .join(FORMAT_CONFIG_FILE);
        match tokio::fs::read(path).await {
            Ok(bytes) => snapshot.push(Some(bytes)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => snapshot.push(None),
            Err(error) => panic!("read persisted format: {error}"),
        }
    }
    snapshot
}

async fn assert_plan_published(disks: &[Option<DiskStore>], expected: &[Option<FormatV3>]) {
    let (actual, errors) = load_format_erasure_all(disks, false).await;
    assert!(errors.iter().all(Option::is_none), "load recovered formats: {errors:?}");
    assert_eq!(actual, expected, "bootstrap recovery must preserve the deployment and every disk UUID");
}

#[tokio::test]
async fn bootstrap_intent_survives_formatter_restart() {
    let (_directory, mut disks) = local_disks(4).await;
    let first = connect_load_init_formats_with_instance_ctx(&Arc::new(InstanceContext::new()), true, &mut disks, 1, 4, None)
        .await
        .expect("format fresh deployment");
    let before = format_snapshot(&disks).await;

    let mut reopened = reopen_disks(&disks).await;
    let loaded = connect_load_init_formats_with_instance_ctx(&Arc::new(InstanceContext::new()), true, &mut reopened, 1, 4, None)
        .await
        .expect("resume bootstrap after process restart");

    assert_eq!(loaded.pool_meta_bootstrap_authority, PoolMetaBootstrapAuthority::Fresh);
    assert_eq!(loaded.format.shared_identity(), first.format.shared_identity());
    assert_eq!(
        format_snapshot(&reopened).await,
        before,
        "already published formats remain byte-identical"
    );
}

#[tokio::test]
async fn bootstrap_intent_completes_interrupted_fourth_format_after_restart() {
    let (_directory, disks) = local_disks(4).await;
    let plan = bootstrap_plan();
    write_format_bootstrap_intents(&disks, &plan)
        .await
        .expect("persist bootstrap authority before formats");
    write_format_file_all(&disks[..3], &plan[..3])
        .await
        .expect("publish first three formats");
    let interrupted_disk = disks[3].as_ref().expect("fourth disk exists");
    let staged = Uuid::new_v4().to_string();
    interrupted_disk
        .write_all(
            RUSTFS_META_BUCKET,
            &staged,
            plan[3]
                .as_ref()
                .expect("fourth planned format")
                .to_json()
                .expect("encode planned format")
                .into_bytes()
                .into(),
        )
        .await
        .expect("stage the fourth format without its rename");
    let before = format_snapshot(&disks).await;
    assert!(before[3].is_none(), "reproducer has no fourth format.json");

    let mut reopened = reopen_disks(&disks).await;
    let loaded = connect_load_init_formats_with_instance_ctx(&Arc::new(InstanceContext::new()), true, &mut reopened, 1, 4, None)
        .await
        .expect("finish interrupted format publication before identity reads");

    assert_eq!(loaded.pool_meta_bootstrap_authority, PoolMetaBootstrapAuthority::Fresh);
    assert_plan_published(&reopened, &plan).await;
    assert_eq!(
        format_snapshot(&reopened).await[..3],
        before[..3],
        "recovery only publishes the unformatted slot"
    );
}

#[tokio::test]
async fn bootstrap_intent_reuses_plan_when_no_formats_were_published() {
    let (_directory, disks) = local_disks(4).await;
    let plan = bootstrap_plan();
    write_format_bootstrap_intents(&disks, &plan)
        .await
        .expect("persist the complete format plan");
    assert!(format_snapshot(&disks).await.iter().all(Option::is_none));

    let mut reopened = reopen_disks(&disks).await;
    let loaded = connect_load_init_formats_with_instance_ctx(&Arc::new(InstanceContext::new()), true, &mut reopened, 1, 4, None)
        .await
        .expect("resume before first format publication");

    assert_eq!(loaded.pool_meta_bootstrap_authority, PoolMetaBootstrapAuthority::Fresh);
    assert_plan_published(&reopened, &plan).await;
}

#[tokio::test]
async fn bootstrap_intent_absence_does_not_reinitialize_existing_formats() {
    let (_directory, mut disks) = local_disks(4).await;
    let plan = bootstrap_plan();
    write_format_file_all(&disks, &plan)
        .await
        .expect("publish historical formats without an intent");
    let before = format_snapshot(&disks).await;

    let loaded = connect_load_init_formats_with_instance_ctx(&Arc::new(InstanceContext::new()), true, &mut disks, 1, 4, None)
        .await
        .expect("load existing deployment without granting bootstrap authority");

    assert_eq!(loaded.pool_meta_bootstrap_authority, PoolMetaBootstrapAuthority::None);
    assert_eq!(format_snapshot(&disks).await, before);
}

#[tokio::test]
async fn bootstrap_intent_incomplete_or_invalid_cannot_grant_authority() {
    for invalid_state in ["missing", "corrupt", "foreign", "wrong-slot"] {
        let (_directory, mut disks) = local_disks(4).await;
        let plan = bootstrap_plan();
        write_format_bootstrap_intents(&disks, &plan)
            .await
            .expect("persist complete authority");
        write_format_file_all(&disks, &plan).await.expect("publish expected formats");
        let intent_path = disks[3]
            .as_ref()
            .expect("fourth disk exists")
            .path()
            .join(RUSTFS_META_BUCKET)
            .join(FORMAT_BOOTSTRAP_FILE);
        match invalid_state {
            "missing" => tokio::fs::remove_file(&intent_path).await.expect("remove one intent replica"),
            "corrupt" => tokio::fs::write(&intent_path, b"invalid JSON")
                .await
                .expect("corrupt one intent replica"),
            "foreign" => {
                let foreign = bootstrap_plan();
                tokio::fs::write(
                    &intent_path,
                    foreign[3]
                        .as_ref()
                        .expect("foreign fourth format")
                        .to_json()
                        .expect("encode foreign intent"),
                )
                .await
                .expect("replace one intent with another deployment");
            }
            "wrong-slot" => {
                tokio::fs::write(
                    &intent_path,
                    plan[2]
                        .as_ref()
                        .expect("third planned format")
                        .to_json()
                        .expect("encode wrong-slot intent"),
                )
                .await
                .expect("copy another slot's intent");
            }
            _ => unreachable!("all fixture states are enumerated"),
        }
        let before = format_snapshot(&disks).await;

        let result =
            connect_load_init_formats_with_instance_ctx(&Arc::new(InstanceContext::new()), true, &mut disks, 1, 4, None).await;

        if let Ok(loaded) = result {
            assert_eq!(
                loaded.pool_meta_bootstrap_authority,
                PoolMetaBootstrapAuthority::None,
                "{invalid_state} intent cannot authorize pool metadata initialization"
            );
        }
        assert_eq!(format_snapshot(&disks).await, before, "{invalid_state} intent cannot alter valid formats");
    }
}

#[tokio::test]
async fn bootstrap_intent_retirement_prevents_future_rebootstrap() {
    let (_directory, mut disks) = local_disks(4).await;
    let first = connect_load_init_formats_with_instance_ctx(&Arc::new(InstanceContext::new()), true, &mut disks, 1, 4, None)
        .await
        .expect("format fresh deployment");
    assert_eq!(first.pool_meta_bootstrap_authority, PoolMetaBootstrapAuthority::Fresh);
    clear_format_bootstrap_intents(&disks)
        .await
        .expect("retire authority after committed pool metadata");
    clear_format_bootstrap_intents(&disks)
        .await
        .expect("authority retirement is idempotent");
    let before = format_snapshot(&disks).await;

    let mut reopened = reopen_disks(&disks).await;
    let loaded = connect_load_init_formats_with_instance_ctx(&Arc::new(InstanceContext::new()), true, &mut reopened, 1, 4, None)
        .await
        .expect("load a deployment after authority retirement");

    assert_eq!(loaded.pool_meta_bootstrap_authority, PoolMetaBootstrapAuthority::None);
    assert_eq!(format_snapshot(&reopened).await, before);
}

#[tokio::test]
async fn bootstrap_intent_observer_cannot_resume_formatter_authority() {
    let (_directory, mut disks) = local_disks(4).await;
    let plan = bootstrap_plan();
    write_format_bootstrap_intents(&disks, &plan)
        .await
        .expect("persist complete format intent");
    write_format_file_all(&disks, &plan).await.expect("publish expected formats");
    let before = format_snapshot(&disks).await;

    let loaded = connect_load_init_formats_with_instance_ctx(&Arc::new(InstanceContext::new()), false, &mut disks, 1, 4, None)
        .await
        .expect("observer may read the existing format");

    assert_eq!(loaded.pool_meta_bootstrap_authority, PoolMetaBootstrapAuthority::None);
    assert_eq!(format_snapshot(&disks).await, before, "an observer cannot republish storage formats");
}

#[tokio::test]
async fn bootstrap_intent_unavailable_disk_cannot_restore_authority() {
    let (_directory, mut disks) = local_disks(4).await;
    let plan = bootstrap_plan();
    write_format_bootstrap_intents(&disks, &plan)
        .await
        .expect("persist complete format intent");
    write_format_file_all(&disks, &plan).await.expect("publish expected formats");
    let physical_disks = disks.clone();
    let before = format_snapshot(&physical_disks).await;
    disks[3] = None;

    let result =
        connect_load_init_formats_with_instance_ctx(&Arc::new(InstanceContext::new()), true, &mut disks, 1, 4, None).await;

    if let Ok(loaded) = result {
        assert_eq!(loaded.pool_meta_bootstrap_authority, PoolMetaBootstrapAuthority::None);
    }
    assert_eq!(
        format_snapshot(&physical_disks).await,
        before,
        "a missing configured disk cannot authorize format writes"
    );
}

#[tokio::test]
async fn bootstrap_intent_deployment_mismatch_cannot_publish_missing_format() {
    let (_directory, mut disks) = local_disks(4).await;
    let plan = bootstrap_plan();
    write_format_bootstrap_intents(&disks, &plan)
        .await
        .expect("persist complete format intent");
    write_format_file_all(&disks[..3], &plan[..3])
        .await
        .expect("publish three formats from the original deployment");
    let physical_disks = disks.clone();
    let before = format_snapshot(&physical_disks).await;

    let result = connect_load_init_formats_with_instance_ctx(
        &Arc::new(InstanceContext::new()),
        true,
        &mut disks,
        1,
        4,
        Some(Uuid::new_v4()),
    )
    .await;

    if let Ok(loaded) = result {
        assert_eq!(loaded.pool_meta_bootstrap_authority, PoolMetaBootstrapAuthority::None);
    }
    assert_eq!(
        format_snapshot(&physical_disks).await,
        before,
        "an intent belonging to another deployment cannot publish the fourth format"
    );
}

#[tokio::test]
async fn bootstrap_intent_conflicting_existing_format_cannot_be_overwritten() {
    let (_directory, mut disks) = local_disks(4).await;
    let plan = bootstrap_plan();
    write_format_bootstrap_intents(&disks, &plan)
        .await
        .expect("persist complete format intent");
    write_format_file_all(&disks[..3], &plan[..3])
        .await
        .expect("publish first three formats");
    let foreign = bootstrap_plan();
    write_format_file_all(&disks[3..], &foreign[3..])
        .await
        .expect("publish another deployment's format on the fourth disk");
    let physical_disks = disks.clone();
    let before = format_snapshot(&physical_disks).await;

    let result =
        connect_load_init_formats_with_instance_ctx(&Arc::new(InstanceContext::new()), true, &mut disks, 1, 4, None).await;

    if let Ok(loaded) = result {
        assert_eq!(loaded.pool_meta_bootstrap_authority, PoolMetaBootstrapAuthority::None);
    }
    assert_eq!(
        format_snapshot(&physical_disks).await,
        before,
        "a complete intent cannot overwrite a conflicting persisted format"
    );
}

#[tokio::test]
async fn bootstrap_intent_must_be_consumed_before_serving() {
    let (_directory, disks) = local_disks(4).await;
    let plan = bootstrap_plan();
    write_format_bootstrap_intents(&disks, &plan)
        .await
        .expect("persist complete format intent");

    ensure_format_bootstrap_consumed(&disks)
        .await
        .expect_err("complete fresh authority must block serving from committed pool metadata");

    clear_format_bootstrap_intents(&disks[..1])
        .await
        .expect("durably retire one intent replica");
    ensure_format_bootstrap_consumed(&disks)
        .await
        .expect("retiring one replica invalidates complete fresh authority");

    clear_format_bootstrap_intents(&disks)
        .await
        .expect("complete retirement after an interruption");
    clear_format_bootstrap_intents(&disks)
        .await
        .expect("completed retirement is idempotent");
    ensure_format_bootstrap_consumed(&disks)
        .await
        .expect("fully retired authority permits serving from committed pool metadata");
}

#[cfg(unix)]
#[tokio::test]
async fn bootstrap_intent_consumption_rejects_completion_symlink() {
    let (directory, disks) = local_disks(4).await;
    let sentinel = directory.path().join("unrelated");
    tokio::fs::write(&sentinel, b"preserve").await.expect("write sentinel");
    let done = disks[0]
        .as_ref()
        .expect("fixture disk")
        .path()
        .join(RUSTFS_META_BUCKET)
        .join("format.json.bootstrap.done");
    std::os::unix::fs::symlink(&sentinel, &done).expect("install corrupt completion symlink");
    ensure_format_bootstrap_consumed(&disks[..1])
        .await
        .expect_err("a corrupt completion symlink must fail closed");
    assert_eq!(tokio::fs::read(&sentinel).await.expect("read sentinel"), b"preserve");
    assert!(
        tokio::fs::symlink_metadata(done)
            .await
            .expect("completion symlink remains")
            .file_type()
            .is_symlink()
    );
}

#[cfg(all(unix, feature = "test-util"))]
#[tokio::test]
async fn bootstrap_intent_absence_requires_successful_directory_sync() {
    let (_directory, disks) = local_disks(4).await;
    for disk in disks.iter().flatten() {
        crate::disk::os::set_local_dir_fsync_failure(disk, RUSTFS_META_BUCKET, "", std::io::ErrorKind::Other)
            .expect("inject consumption directory-sync failure");
    }
    ensure_format_bootstrap_consumed(&disks)
        .await
        .expect_err("visible absence cannot authorize serving when every sealing sync fails");
    ensure_format_bootstrap_consumed(&disks)
        .await
        .expect("retry must establish durable absence");

    let disk = disks[0].as_ref().expect("fixture disk");
    crate::disk::os::set_local_dir_fsync_failure(disk, RUSTFS_META_BUCKET, "", std::io::ErrorKind::Other)
        .expect("inject writer retirement retry sync failure");
    clear_format_bootstrap_intents(&disks[..1])
        .await
        .expect_err("writer retries must synchronize an already absent intent");
    clear_format_bootstrap_intents(&disks[..1])
        .await
        .expect("writer retirement sync retries safely");
}
