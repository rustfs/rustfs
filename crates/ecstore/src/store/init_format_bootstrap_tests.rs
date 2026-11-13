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

#[cfg(not(windows))]
#[tokio::test]
async fn bootstrap_intent_anchor_is_durable_before_fanout() {
    use crate::disk::os::prepared_publication_test_hooks::{Stage, install_at};

    let (_directory, disks) = local_disks(4).await;
    let plan = bootstrap_plan();
    let anchor = disks[0]
        .as_ref()
        .expect("anchor disk")
        .get_object_path_for_io_if_local(RUSTFS_META_BUCKET, FORMAT_BOOTSTRAP_FILE)
        .expect("format bootstrap uses a real local backend")
        .expect("format bootstrap publication path");
    let (arrived, arrival) = tokio::sync::oneshot::channel();
    let (release, released) = std::sync::mpsc::channel();
    let _hook = install_at(Stage::PreparedRename, &anchor, move || {
        let _ = arrived.send(());
        let _ = released.recv();
    });
    let publication = tokio::spawn({
        let disks = disks.clone();
        let plan = plan.clone();
        async move { write_format_bootstrap_intents(&disks, &plan).await }
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), arrival)
        .await
        .expect("anchor reaches native publication")
        .expect("anchor publication hook");
    for disk in disks.iter().skip(1).flatten() {
        assert!(matches!(
            disk.read_all(RUSTFS_META_BUCKET, FORMAT_BOOTSTRAP_FILE).await,
            Err(DiskError::FileNotFound)
        ));
    }
    release.send(()).expect("release anchor publication");
    publication
        .await
        .expect("join anchor-first publication")
        .expect("publish the selected plan");
    assert!(format_snapshot(&disks).await.iter().all(Option::is_none));
}

#[tokio::test]
async fn bootstrap_intent_anchor_only_restart_preserves_selected_plan() {
    let (_directory, disks) = local_disks(4).await;
    let plan = bootstrap_plan();
    disks[0]
        .as_ref()
        .expect("anchor disk")
        .write_format_control(
            FORMAT_BOOTSTRAP_FILE,
            plan[0]
                .as_ref()
                .expect("anchor format")
                .to_json()
                .expect("encode selected plan")
                .into_bytes()
                .into(),
        )
        .await
        .expect("persist only the anchor before interruption");

    let mut reopened = reopen_disks(&disks).await;
    let loaded = connect_load_init_formats_with_instance_ctx(&Arc::new(InstanceContext::new()), true, &mut reopened, 1, 4, None)
        .await
        .expect("complete fanout from the accepted anchor after restart");
    assert_eq!(loaded.pool_meta_bootstrap_authority, PoolMetaBootstrapAuthority::Fresh);
    assert_plan_published(&reopened, &plan).await;
}

#[tokio::test]
async fn bootstrap_intent_missing_anchor_cannot_replace_partial_plan() {
    let (_directory, disks) = local_disks(4).await;
    let plan = bootstrap_plan();
    let existing = plan[1]
        .as_ref()
        .expect("second slot format")
        .to_json()
        .expect("encode partial plan");
    disks[1]
        .as_ref()
        .expect("second disk")
        .write_all(RUSTFS_META_BUCKET, FORMAT_BOOTSTRAP_FILE, existing.clone().into_bytes().into())
        .await
        .expect("persist a partial plan with no anchor");
    let mut reopened = reopen_disks(&disks).await;
    connect_load_init_formats_with_instance_ctx(&Arc::new(InstanceContext::new()), true, &mut reopened, 1, 4, None)
        .await
        .err()
        .expect("partial plan cannot authorize selecting another anchor");
    assert!(format_snapshot(&disks).await.iter().all(Option::is_none));
    assert!(matches!(
        disks[0]
            .as_ref()
            .expect("anchor disk")
            .read_all(RUSTFS_META_BUCKET, FORMAT_BOOTSTRAP_FILE)
            .await,
        Err(DiskError::FileNotFound)
    ));
    assert_eq!(
        disks[1]
            .as_ref()
            .expect("second disk")
            .read_all(RUSTFS_META_BUCKET, FORMAT_BOOTSTRAP_FILE)
            .await
            .expect("read preserved partial plan")
            .as_ref(),
        existing.as_bytes()
    );
}

#[tokio::test]
async fn bootstrap_intent_conflicting_fanout_cannot_change_anchor_or_formats() {
    let (_directory, disks) = local_disks(4).await;
    let selected = bootstrap_plan();
    let foreign = bootstrap_plan();
    for (index, plan) in [(0, &selected), (1, &foreign)] {
        disks[index]
            .as_ref()
            .expect("fixture disk")
            .write_all(
                RUSTFS_META_BUCKET,
                FORMAT_BOOTSTRAP_FILE,
                plan[index]
                    .as_ref()
                    .expect("planned slot")
                    .to_json()
                    .expect("encode planned slot")
                    .into_bytes()
                    .into(),
            )
            .await
            .expect("persist conflicting observed plans");
    }
    let mut reopened = reopen_disks(&disks).await;
    connect_load_init_formats_with_instance_ctx(&Arc::new(InstanceContext::new()), true, &mut reopened, 1, 4, None)
        .await
        .err()
        .expect("a conflicting intent cannot be replaced by the selected anchor");
    assert!(format_snapshot(&disks).await.iter().all(Option::is_none));
    let anchor = disks[0]
        .as_ref()
        .expect("anchor disk")
        .read_all(RUSTFS_META_BUCKET, FORMAT_BOOTSTRAP_FILE)
        .await
        .expect("read preserved anchor");
    assert_eq!(
        FormatV3::try_from(anchor.as_ref()).expect("decode preserved anchor"),
        *selected[0].as_ref().expect("selected anchor")
    );
}

#[tokio::test]
async fn bootstrap_intent_partial_proof_cannot_fill_a_partially_formatted_pool() {
    let (_directory, disks) = local_disks(4).await;
    let plan = bootstrap_plan();
    write_format_bootstrap_intents(&disks, &plan)
        .await
        .expect("persist the selected plan");
    write_format_file_all(&disks[..3], &plan[..3])
        .await
        .expect("publish only three formats");
    let missing = disks[3]
        .as_ref()
        .expect("fourth disk")
        .path()
        .join(RUSTFS_META_BUCKET)
        .join(FORMAT_BOOTSTRAP_FILE);
    tokio::fs::remove_file(&missing).await.expect("remove one proof replica");
    let before = format_snapshot(&disks).await;
    let mut reopened = reopen_disks(&disks).await;
    if let Ok(loaded) =
        connect_load_init_formats_with_instance_ctx(&Arc::new(InstanceContext::new()), true, &mut reopened, 1, 4, None).await
    {
        assert_eq!(loaded.pool_meta_bootstrap_authority, PoolMetaBootstrapAuthority::None);
    }
    assert_eq!(format_snapshot(&disks).await, before);
    assert!(!missing.exists(), "partial format state cannot recreate missing proof");
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
    retire_format_bootstrap_authority(&disks)
        .await
        .expect("retire authority after committed pool metadata");
    retire_format_bootstrap_authority(&disks)
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
    disks[3]
        .as_ref()
        .expect("fourth disk")
        .write_all(
            RUSTFS_META_BUCKET,
            FORMAT_CONFIG_FILE,
            foreign[3]
                .as_ref()
                .expect("foreign fourth format")
                .to_json()
                .expect("encode foreign physical format")
                .into_bytes()
                .into(),
        )
        .await
        .expect("inject another deployment's physical format on the fourth disk");
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
async fn bootstrap_intent_retirement_consumes_only_one_slot() {
    let (_directory, mut disks) = local_disks(4).await;
    let plan = bootstrap_plan();
    write_format_bootstrap_intents(&disks, &plan)
        .await
        .expect("persist complete format intent");
    write_format_file_all(&disks, &plan)
        .await
        .expect("publish every planned format");
    retire_format_bootstrap_authority(&disks)
        .await
        .expect("retire creator authority");
    retire_format_bootstrap_authority(&disks)
        .await
        .expect("repeat retirement after interrupted identity publication");

    let mut absent = 0;
    for disk in disks.iter().flatten() {
        match disk.read_file(RUSTFS_META_BUCKET, FORMAT_BOOTSTRAP_FILE).await {
            Err(DiskError::FileNotFound) => absent += 1,
            Ok(_) => {}
            Err(error) => panic!("read intent after retirement: {error}"),
        }
    }
    assert_eq!(absent, 1, "one consumed slot is sufficient and avoids writes to sibling disks");
    let loaded = connect_load_init_formats_with_instance_ctx(&Arc::new(InstanceContext::new()), true, &mut disks, 1, 4, None)
        .await
        .expect("load formats after creator authority consumption");
    assert_eq!(loaded.pool_meta_bootstrap_authority, PoolMetaBootstrapAuthority::None);
}

#[cfg(unix)]
#[tokio::test]
async fn bootstrap_intent_retirement_preserves_completion_symlink_target() {
    let (directory, disks) = local_disks(4).await;
    let plan = bootstrap_plan();
    write_format_bootstrap_intents(&disks, &plan)
        .await
        .expect("persist creator intent");
    let sentinel = directory.path().join("unrelated");
    tokio::fs::write(&sentinel, b"preserve").await.expect("write sentinel");
    let done = disks[0]
        .as_ref()
        .expect("fixture disk")
        .path()
        .join(RUSTFS_META_BUCKET)
        .join("format.json.bootstrap.done");
    std::os::unix::fs::symlink(&sentinel, &done).expect("install completion symlink");
    retire_format_bootstrap_authority(&disks[..1])
        .await
        .expect("replace completion entry without following it");
    assert_eq!(tokio::fs::read(&sentinel).await.expect("read sentinel"), b"preserve");
    assert!(tokio::fs::symlink_metadata(done).await.expect("completion marker").is_file());
}

#[cfg(all(unix, feature = "test-util"))]
#[tokio::test]
async fn bootstrap_intent_retirement_requires_successful_directory_sync() {
    let (_directory, disks) = local_disks(4).await;
    let disk = disks[0].as_ref().expect("fixture disk");
    crate::disk::os::set_local_dir_fsync_failure(disk, RUSTFS_META_BUCKET, "", std::io::ErrorKind::Other)
        .expect("inject absent-intent directory-sync failure");
    retire_format_bootstrap_authority(&disks[..1])
        .await
        .expect_err("visible absence requires a successful sync");
    retire_format_bootstrap_authority(&disks[..1])
        .await
        .expect("retirement sync retries safely");

    let plan = bootstrap_plan();
    write_format_bootstrap_intents(&disks, &plan)
        .await
        .expect("persist creator intent");
    crate::disk::os::set_local_dir_fsync_failure(disk, RUSTFS_META_BUCKET, "", std::io::ErrorKind::Other)
        .expect("inject post-rename directory-sync failure");
    retire_format_bootstrap_authority(&disks[..1])
        .await
        .expect_err("rename visibility cannot replace durability");
    assert!(matches!(
        disk.read_file(RUSTFS_META_BUCKET, FORMAT_BOOTSTRAP_FILE).await,
        Err(DiskError::FileNotFound)
    ));
    crate::disk::os::set_local_dir_fsync_failure(disk, RUSTFS_META_BUCKET, "", std::io::ErrorKind::Other)
        .expect("inject retirement retry sync failure");
    retire_format_bootstrap_authority(&disks[..1])
        .await
        .expect_err("retry must repeat the durability barrier");
    retire_format_bootstrap_authority(&disks[..1])
        .await
        .expect("retry consumes authority durably");
}
