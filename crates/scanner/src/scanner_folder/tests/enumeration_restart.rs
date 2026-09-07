// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.

use super::*;
use std::path::{Path, PathBuf};
use tokio::io::AsyncReadExt;

const MAX_CACHE_BYTES: u64 = 1024 * 1024;
const REQUEST_ENV: &str = "RUSTFS_ENUMERATION_REQUEST";

struct Observation {
    root: PathBuf,
    limit: u64,
    entries: u64,
    name_bytes: u64,
    first_entry: Option<String>,
    last_entry: Option<String>,
}

static OBSERVATION: Mutex<Option<Observation>> = Mutex::new(None);

// Only the selected synthetic disk is observed; concurrent unrelated scanners
// do not consume its budget. This hook is absent from non-test builds.
pub(in crate::scanner_folder) fn observe_raw_entry(dir: &str, name: &std::ffi::OsStr, budget: &ScannerCycleBudget) {
    let mut guard = OBSERVATION.lock().expect("enumeration observation lock");
    if let Some(observation) = guard.as_mut()
        && Path::new(dir).starts_with(&observation.root)
    {
        let relative_dir = Path::new(dir)
            .strip_prefix(&observation.root)
            .unwrap_or_else(|_| Path::new(""));
        let entry_marker = relative_dir.join(name).to_string_lossy().to_string();
        observation.first_entry.get_or_insert_with(|| entry_marker.clone());
        observation.last_entry = Some(entry_marker);
        observation.entries += 1;
        observation.name_bytes += u64::try_from(name.as_encoded_bytes().len()).expect("bounded entry name");
        if observation.entries >= observation.limit {
            budget.cancel_for_runtime();
        }
    }
}

struct ObservationGuard;

impl Drop for ObservationGuard {
    fn drop(&mut self) {
        *OBSERVATION.lock().expect("enumeration observation cleanup") = None;
    }
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    workspace: PathBuf,
    objects: usize,
    raw_entry_budget: u64,
    round: u32,
}

async fn read_bounded(path: &Path) -> Vec<u8> {
    let file = tokio::fs::File::open(path).await.expect("open fixture artifact");
    let mut bytes = Vec::new();
    file.take(MAX_CACHE_BYTES + 1)
        .read_to_end(&mut bytes)
        .await
        .expect("read fixture artifact");
    assert!(u64::try_from(bytes.len()).expect("artifact size") <= MAX_CACHE_BYTES);
    bytes
}

async fn round(request: &Request) -> serde_json::Value {
    assert!((1..=1024).contains(&request.objects));
    assert!((1..=4096).contains(&request.raw_entry_budget));
    assert!(request.round < 64);
    let disk_root = request.workspace.join("disk");
    let cache_path = request.workspace.join("cache.bin");
    if request.round == 0 {
        tokio::fs::create_dir(&disk_root).await.expect("create fresh synthetic disk");
        for index in 0..request.objects {
            let object = format!("object-{index:04}");
            let version = Uuid::from_u128(u128::try_from(index).expect("fixture index") + 1);
            let bytes = metadata_for_object_version("bucket", &object, Some(version));
            write_test_object_metadata_bytes(&disk_root, "bucket", &object, &bytes).await;
        }
        let mut initial = DataUsageCache::default();
        initial.info.name = "bucket".to_string();
        initial.info.skip_healing = true;
        initial.info.snapshot_complete = false;
        initial.replace("bucket", "", DataUsageEntry::default());
        tokio::fs::write(&cache_path, initial.marshal_msg().expect("initial cache codec"))
            .await
            .expect("persist initial cache");
    }
    let cache = DataUsageCache::unmarshal(&read_bounded(&cache_path).await).expect("reload cache codec before scan");
    assert_eq!(cache.info.name, "bucket");
    let before = cache.checked_flatten("bucket").expect("persisted bucket root").objects;
    let endpoint = Endpoint::try_from(disk_root.to_string_lossy().as_ref()).expect("fixture endpoint");
    let disk = new_disk(
        &endpoint,
        &DiskOption {
            cleanup: false,
            health_check: false,
        },
    )
    .await
    .expect("open synthetic disk in this process");
    let parent = CancellationToken::new();
    let budget = ScannerCycleBudget::new_with_progress_tracking(&parent, Default::default());
    *OBSERVATION.lock().expect("install observation") = Some(Observation {
        root: disk.path(),
        limit: request.raw_entry_budget,
        entries: 0,
        name_bytes: 0,
        first_entry: None,
        last_entry: None,
    });
    let _observation_guard = ObservationGuard;
    let result = scan_data_folder(
        budget.token(),
        budget.clone(),
        vec![disk.clone()],
        disk,
        cache.clone(),
        None,
        HealScanMode::Normal,
        SCANNER_SLEEPER.clone(),
    )
    .await;
    let (returned, outcome) = match result {
        Ok(cache) => (cache, "complete"),
        Err(ScannerError::PartialCache(cache)) => (*cache, "partial"),
        Err(ScannerError::Other(message)) if budget.token().is_cancelled() && message == "Operation cancelled" => {
            (cache, "cancelled_without_cache")
        }
        Err(error) => panic!("unexpected real scanner failure: {error}"),
    };
    let encoded = returned.marshal_msg().expect("returned cache codec");
    assert!(u64::try_from(encoded.len()).expect("encoded length") <= MAX_CACHE_BYTES);
    tokio::fs::write(&cache_path, encoded).await.expect("persist returned cache");
    let reloaded = DataUsageCache::unmarshal(&read_bounded(&cache_path).await).expect("reload returned cache codec");
    let retained = reloaded.checked_flatten("bucket").expect("reloaded bucket root");
    let scanned = returned.checked_flatten("bucket").expect("returned bucket root");
    assert_eq!(
        (retained.objects, retained.versions, retained.size),
        (scanned.objects, scanned.versions, scanned.size)
    );
    assert_eq!(reloaded.info.snapshot_complete, returned.info.snapshot_complete);
    let guard = OBSERVATION.lock().expect("read observation");
    let observation = guard.as_ref().expect("installed observation");
    serde_json::json!({
        "schema": 1, "pid": std::process::id(), "round": request.round,
        "objects_expected": request.objects, "raw_entry_budget": request.raw_entry_budget,
        "raw_entries": observation.entries, "raw_name_bytes": observation.name_bytes,
        "raw_first_entry": observation.first_entry, "raw_last_entry": observation.last_entry,
        "objects_processed": budget.progress().0,
        "objects_before": before, "objects_retained": retained.objects,
        "versions_retained": retained.versions, "bytes_retained": retained.size,
        "snapshot_complete": reloaded.info.snapshot_complete, "outcome": outcome,
    })
}

/// Default CI is a positive healthy control. The external driver selects the
/// same worker in a fresh OS process per round and applies its strict oracle.
#[tokio::test]
#[serial]
async fn enumeration_restart_worker() {
    if let Some(path) = std::env::var_os(REQUEST_ENV) {
        let request: Request = serde_json::from_slice(&read_bounded(Path::new(&path)).await).expect("bounded worker request");
        let report = round(&request).await;
        tokio::fs::write(
            request.workspace.join(format!("round-{}.json", request.round)),
            serde_json::to_vec(&report).expect("report JSON"),
        )
        .await
        .expect("write worker report");
    } else {
        let temp = tempfile::tempdir().expect("healthy fixture directory");
        let report = round(&Request {
            workspace: temp.path().to_path_buf(),
            objects: 4,
            raw_entry_budget: 16,
            round: 0,
        })
        .await;
        assert_eq!(report["outcome"], "complete");
        assert_eq!(report["snapshot_complete"], true);
        assert_eq!(report["objects_retained"], 4);
        assert_eq!(report["versions_retained"], 4);
        assert_eq!(report["bytes_retained"], 4);
        assert!(report["raw_entries"].as_u64().expect("observed entries") >= 8, "{report}");
    }
}
