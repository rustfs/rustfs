// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.

use super::*;

impl DirectoryCheckpoint {
    pub(crate) async fn wide_storage_fixture(info: &DataUsageCacheInfo) -> [Self; 2] {
        Self::storage_fixture(info, 300_000, [121_034, 121_035]).await
    }

    pub(crate) async fn storage_fixture(info: &DataUsageCacheInfo, width: usize, completed: [usize; 2]) -> [Self; 2] {
        let scope = Scope {
            bucket: info.name.clone(),
            source: info.source.expect("fixture source"),
            identity: info.scan_identity.expect("fixture identity"),
            progress: info.scan_progress.expect("fixture coverage plan"),
            mode: 0,
            prefixes: None,
            watch: None,
        };
        let slot = Saved::default();
        let endpoint = info.scan_directory_source.clone().expect("fixture physical source");
        let physical = Physical {
            endpoint: endpoint.clone(),
            device: 1,
            inode: 2,
            changed: (123, 0),
            modified: SystemTime::UNIX_EPOCH,
        };
        let mut recorder = Recorder::new(slot.clone(), &info.name, scope, endpoint, Some(physical), None);
        recorder.entries_read = u64::try_from(width).expect("fixture width");
        let mut work = DirectoryWork {
            compacted: true,
            ..Default::default()
        };
        for i in 0..width {
            work.names.add(format!("obj-{i:06}"));
            work.names.flush_full().await.expect("record real immutable name page");
        }
        work.names.finish();
        completed.map(|objects| {
            work.names.seek_after(Some(&format!("obj-{:06}", objects - 1)));
            work.compacted_total.objects = objects;
            work.compacted_total.size = objects * 1024;
            work.measured_at = info
                .last_update
                .and_then(crate::data_usage_define::scan_measurement_time_millis);
            recorder.save(&work);
            slot.lock()
                .expect("fixture slot")
                .take()
                .expect("sealed directory checkpoint")
        })
    }
}

#[derive(Serialize, Deserialize)]
struct LegacyCheckpoint {
    version: u8,
    parent: String,
    scope: Scope,
    physical: Option<Physical>,
    pages: Vec<NamePage>,
    anchor: Option<native_io::DirectoryAnchor>,
    entries_read: u64,
    sealed: bool,
    part: Option<String>,
    metadata: bool,
    aggregate: Option<Aggregate>,
    endpoint: String,
    digest: [u8; 32],
}

#[tokio::test]
async fn display_progress_measurement_metadata_keeps_version_one_checkpoint_compatibility() {
    let plan = crate::DataUsageScanPlanDigest([1; 32]);
    let info = DataUsageCacheInfo {
        name: "bucket".into(),
        source: Some(DataUsageCacheSource::new(0, 0)),
        scan_identity: Some(DataUsageScanIdentity {
            version: crate::data_usage_define::DATA_USAGE_SCAN_CHECKPOINT_VERSION,
            bucket_incarnation: uuid::Uuid::from_u128(1),
            set_layout: crate::DataUsageScanPlanDigest([7; 32]),
            publication_epoch: 0,
            tier_registry_generation: 1,
            scan_mode: rustfs_heal_contracts::heal_channel::HealScanMode::Normal,
        }),
        scan_progress: Some(DataUsageScanProgress {
            started_plan: plan,
            requested_plan: plan,
        }),
        scan_directory_source: Some("disk-a".into()),
        last_update: Some(SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(10)),
        ..Default::default()
    };
    let [checkpoint, _] = DirectoryCheckpoint::storage_fixture(&info, 4, [2, 3]).await;
    assert_eq!(checkpoint.measured_at, Some(10_000));
    let bytes = rmp_serde::to_vec(&checkpoint).expect("encode additive named checkpoint");
    let legacy: LegacyCheckpoint = rmp_serde::from_slice(&bytes).expect("version-one reader ignores measurement metadata");
    let old_bytes = rmp_serde::to_vec(&legacy).expect("encode legacy positional checkpoint");
    let recovered: DirectoryCheckpoint = rmp_serde::from_slice(&old_bytes).expect("new reader accepts legacy receipt");
    assert_eq!(recovered.measured_at, None);
    assert_eq!(
        rmp_serde::to_vec(&recovered).expect("encode receipt without measurement metadata"),
        old_bytes,
        "receipts without diagnostic metadata keep the compact legacy encoding"
    );
    assert!(
        recovered.is_bound_to(&info),
        "diagnostic metadata must not change the version-one coverage checksum"
    );
}
