// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.

use super::*;
use std::collections::{HashMap, VecDeque};
use std::sync::LazyLock;

#[derive(Clone, Copy)]
enum DirectoryOrder {
    PartLast,
    FailAfter(usize, std::io::ErrorKind),
    FailOpen(std::io::ErrorKind),
}

pub(in crate::scanner_folder) type OrderedEntries = VecDeque<std::io::Result<native_io::DirEntry>>;
static ORDERED_DIRECTORIES: LazyLock<Mutex<HashMap<String, DirectoryOrder>>> = LazyLock::new(|| Mutex::new(HashMap::new()));

struct OrderedDirectory(String);

impl OrderedDirectory {
    fn new(path: &std::path::Path) -> Self {
        let path = path.to_string_lossy().into_owned();
        ORDERED_DIRECTORIES
            .lock()
            .expect("ordered directory registry")
            .insert(path.clone(), DirectoryOrder::PartLast);
        Self(path)
    }
    fn failing(path: &std::path::Path, entries: usize, kind: std::io::ErrorKind) -> Self {
        let guard = Self::new(path);
        ORDERED_DIRECTORIES
            .lock()
            .expect("ordered directory registry")
            .insert(guard.0.clone(), DirectoryOrder::FailAfter(entries, kind));
        guard
    }
    fn fail_open(path: &std::path::Path, kind: std::io::ErrorKind) -> Self {
        let guard = Self::new(path);
        ORDERED_DIRECTORIES
            .lock()
            .expect("ordered directory registry")
            .insert(guard.0.clone(), DirectoryOrder::FailOpen(kind));
        guard
    }
}

impl Drop for OrderedDirectory {
    fn drop(&mut self) {
        ORDERED_DIRECTORIES.lock().expect("ordered directory cleanup").remove(&self.0);
    }
}

pub(in crate::scanner_folder) async fn ordered_entries(
    path: &str,
    reader: &mut native_io::ReadDir,
) -> std::io::Result<Option<OrderedEntries>> {
    let order = ORDERED_DIRECTORIES
        .lock()
        .expect("ordered directory registry")
        .get(path)
        .copied();
    let Some(order) = order else { return Ok(None) };
    if let DirectoryOrder::FailOpen(kind) = order {
        return Err(std::io::Error::from(kind));
    }
    let mut entries = Vec::new();
    while let Some(entry) = reader.next_entry().await? {
        entries.push(entry);
    }
    // Real entries and file types, with an explicit adversarial order rather
    // than an assumption about the filesystem's enumeration order.
    entries.sort_by_key(|entry| (entry.file_name().to_str() == Some("part.1"), entry.file_name()));
    let mut entries: OrderedEntries = entries.into_iter().map(Ok).collect();
    if let DirectoryOrder::FailAfter(count, kind) = order {
        entries.truncate(count);
        entries.push_back(Err(std::io::Error::from(kind)));
    }
    Ok(Some(entries))
}

#[tokio::test]
#[serial]
async fn erasure_candidate_finds_late_part_without_scanning_data_children() {
    let (mut scanner, root) = build_test_scanner().await;
    let _guard = TestGuard {
        temp_dir: Some(root.clone()),
    };
    let data_dir = root.join("bucket/object").join(Uuid::new_v4().to_string());
    tokio::fs::create_dir_all(&data_dir).await.expect("create candidate");
    for index in 0..65 {
        tokio::fs::write(data_dir.join(format!("part.{index}.integrity.orphan")), b"")
            .await
            .expect("write sidecar");
    }
    tokio::fs::write(data_dir.join("part.1"), b"shard")
        .await
        .expect("write late part");
    tokio::fs::create_dir(data_dir.join("namespace-child"))
        .await
        .expect("stage a child before the part");
    let _order = OrderedDirectory::new(&data_dir);
    scanner.is_erasure_mode = true;
    scanner.sleeper = DynamicSleeper::new(rustfs_config::ScannerSpeed::Fastest).for_disk_with_budget_for_tests(
        &root,
        crate::sleeper::ScannerDiskIoBudgetLimits {
            iops: Some(1_000_000),
            bytes_per_sec: None,
        },
    );
    let parent = CancellationToken::new();
    let budget = ScannerCycleBudget::new_with_progress_tracking(
        &parent,
        crate::scanner_budget::ScannerCycleBudgetConfig {
            max_directories: Some(1),
            ..Default::default()
        },
    );
    scanner.budget = budget.clone();
    let mut into = DataUsageEntry::default();
    scanner
        .scan_folder(
            budget.token(),
            CachedFolder {
                name: "bucket/object".into(),
                parent: None,
                object_heal_prob_div: 1,
            },
            &mut into,
        )
        .await
        .expect("late part must establish the parent object boundary");
    assert_eq!(into.failed_objects, 1);
    assert_eq!(into.objects, 0);
    assert!(
        scanner
            .new_cache
            .info
            .failed_objects
            .contains_key(root.join("bucket/object/xl.meta").to_string_lossy().as_ref())
    );
    assert_eq!(budget.reason(), None, "payload directories must not consume namespace directory quota");
    assert!(scanner.new_cache.info.pending_heals.iter().any(|pending| {
        pending.kind == PendingScannerHealKind::Object
            && pending.bucket == "bucket"
            && pending.object.as_deref() == Some("object")
    }));
}

async fn candidate_scanner() -> (FolderScanner, std::path::PathBuf, CachedFolder) {
    let (mut scanner, root) = build_test_scanner().await;
    for info in [
        &mut scanner.old_cache.info,
        &mut scanner.new_cache.info,
        &mut scanner.update_cache.info,
    ] {
        info.name = "bucket".into();
    }
    scanner.is_erasure_mode = true;
    let name = format!("bucket/{}", Uuid::new_v4());
    tokio::fs::create_dir_all(root.join(&name))
        .await
        .expect("create UUID namespace");
    let folder = CachedFolder {
        name,
        parent: Some(hash_path("bucket")),
        object_heal_prob_div: 1,
    };
    (scanner, root, folder)
}

fn bind_budget(scanner: &mut FolderScanner, root: &std::path::Path, iops: u64) {
    scanner.sleeper = DynamicSleeper::new(rustfs_config::ScannerSpeed::Fastest).for_disk_with_budget_for_tests(
        root,
        crate::sleeper::ScannerDiskIoBudgetLimits {
            iops: Some(iops),
            bytes_per_sec: None,
        },
    );
}

fn charged_ops(root: &std::path::Path) -> u64 {
    let status = crate::sleeper::scanner_disk_io_budget_status()
        .into_iter()
        .find(|status| status.disk == root.to_string_lossy())
        .expect("bound candidate budget");
    assert_eq!(status.charged_bytes, 0);
    status.charged_ops
}

#[tokio::test]
#[serial]
async fn erasure_directory_probe_reaches_eof_across_budgeted_batches() {
    for entry_count in [0, 1, 63, 64, 65, 129] {
        let (mut scanner, root, folder) = candidate_scanner().await;
        let _guard = TestGuard {
            temp_dir: Some(root.clone()),
        };
        for index in 0..entry_count {
            tokio::fs::write(root.join(&folder.name).join(format!("namespace-{index}")), b"")
                .await
                .expect("create non-part entry");
        }
        bind_budget(&mut scanner, &root, 1_000_000);
        let ctx = CancellationToken::new();
        scanner.budget = ScannerCycleBudget::new_with_progress_tracking(&ctx, Default::default());
        let outcome = scanner
            .scan_folder_inner(
                ctx,
                folder,
                &mut DataUsageEntry::default(),
                FolderScanMode::ErasureCandidate { reuse_namespace: false },
            )
            .await
            .expect("classify full UUID namespace");
        assert_eq!(outcome, FolderScanOutcome::Namespace);
        assert_eq!(charged_ops(&root), 1 + entry_count, "one open and each entry once, never EOF");
        assert_eq!(scanner.budget.entries_visited(), entry_count);
    }
}

#[tokio::test]
#[serial]
async fn erasure_namespace_uses_one_listing_and_no_extra_probe_charge() {
    let (mut scanner, root, folder) = candidate_scanner().await;
    let _guard = TestGuard {
        temp_dir: Some(root.clone()),
    };
    for index in 0..129 {
        tokio::fs::write(root.join(&folder.name).join(format!("namespace-{index}")), b"")
            .await
            .expect("write namespace entry");
    }
    bind_budget(&mut scanner, &root, 1_000_000);
    let mut into = DataUsageEntry::default();
    scanner
        .scan_folder(
            CancellationToken::new(),
            CachedFolder {
                name: "bucket".into(),
                parent: None,
                object_heal_prob_div: 1,
            },
            &mut into,
        )
        .await
        .expect("scan namespace through parent");
    assert_eq!(
        charged_ops(&root),
        132,
        "parent open/entry + candidate open/129 entries, no second listing"
    );
    assert_eq!(into.failed_objects, 0);
    assert!(scanner.new_cache.info.failed_objects.is_empty());
}

#[tokio::test(start_paused = true)]
#[serial]
async fn scanner_directory_probe_budget_wait_cancels_without_rate_delay() {
    let (mut scanner, root, folder) = candidate_scanner().await;
    let _guard = TestGuard {
        temp_dir: Some(root.clone()),
    };
    bind_budget(&mut scanner, &root, 0);
    scanner.sleeper.admit_disk_io(1, 0).await;
    let ctx = CancellationToken::new();
    let cancel = ctx.clone();
    let task = tokio::spawn(async move {
        tokio::task::yield_now().await;
        cancel.cancel();
    });
    let started = tokio::time::Instant::now();
    let result = scanner
        .scan_folder_inner(
            ctx.clone(),
            folder,
            &mut DataUsageEntry::default(),
            FolderScanMode::ErasureCandidate { reuse_namespace: false },
        )
        .await;
    task.await.expect("open-wait cancellation");
    assert!(result.is_err());
    assert_eq!(started.elapsed(), Duration::ZERO);
    assert!(wait_for_scanner_io(&ctx, scanner.sleeper.admit_disk_io(1, 0)).await.is_err());
    assert_eq!(charged_ops(&root), 2, "already cancelled operations must not charge");
}

#[tokio::test(start_paused = true)]
#[serial]
async fn scanner_directory_probe_cancels_during_entry_budget_wait() {
    let (mut scanner, root, folder) = candidate_scanner().await;
    let _guard = TestGuard {
        temp_dir: Some(root.clone()),
    };
    tokio::fs::write(root.join(&folder.name).join("part.1"), b"shard")
        .await
        .expect("write part");
    bind_budget(&mut scanner, &root, 0);
    let ctx = CancellationToken::new();
    let cancel = ctx.clone();
    let disk = root.clone();
    let task = tokio::spawn(async move {
        loop {
            if charged_ops(&disk) == 2 {
                cancel.cancel();
                return;
            }
            tokio::task::yield_now().await;
        }
    });
    let started = tokio::time::Instant::now();
    let result = scanner
        .scan_folder_inner(
            ctx.clone(),
            folder,
            &mut DataUsageEntry::default(),
            FolderScanMode::ErasureCandidate { reuse_namespace: false },
        )
        .await;
    assert!(ctx.is_cancelled());
    task.await.expect("entry-wait cancellation");
    assert!(result.is_err(), "cancelled classification cannot become found or EOF");
    assert_eq!(started.elapsed(), Duration::ZERO);
    assert_eq!(charged_ops(&root), 2);
}

#[test]
fn erasure_part_names_require_a_positive_u32() {
    for name in ["part.1", "part.10000", "part.4294967295"] {
        assert!(is_erasure_part_name(name));
    }
    for name in [
        "part.0",
        "part.",
        "part.-1",
        "part.4294967296",
        "part.1.integrity.orphan",
        "Part.1",
    ] {
        assert!(!is_erasure_part_name(name), "{name}");
    }
}

fn enable_checkpoint(scanner: &mut FolderScanner) {
    let plan = crate::DataUsageScanPlanDigest([11; 32]);
    let identity = crate::DataUsageScanIdentity {
        version: 1,
        bucket_incarnation: Uuid::from_u128(7),
        set_layout: crate::DataUsageScanPlanDigest([12; 32]),
        publication_epoch: 3,
        tier_registry_generation: 0,
        scan_mode: HealScanMode::Normal,
    };
    let mut cache = DataUsageCache::default();
    cache.prepare_bucket_checkpoint("bucket", 7, 3, crate::DataUsageCacheSource::new(1, 0), plan, identity);
    cache.replace("bucket", "", DataUsageEntry::default());
    scanner.old_cache = cache.clone();
    scanner.new_cache = cache.clone();
    scanner.update_cache = cache;
}

#[tokio::test]
#[serial]
async fn erasure_candidate_defers_metadata_until_part_classification_finishes() {
    let (mut scanner, root, folder) = candidate_scanner().await;
    let _guard = TestGuard {
        temp_dir: Some(root.clone()),
    };
    let path = root.join(&folder.name);
    for index in 0..65 {
        tokio::fs::write(path.join(format!("part.{index}.integrity.orphan")), b"")
            .await
            .expect("write sidecar");
    }
    let object = folder.name.strip_prefix("bucket/").expect("candidate in bucket");
    tokio::fs::write(path.join("xl.meta"), metadata_for_object("bucket", object))
        .await
        .expect("write candidate metadata");
    tokio::fs::write(path.join("part.1"), b"shard")
        .await
        .expect("write late part");
    let _order = OrderedDirectory::new(&path);
    bind_budget(&mut scanner, &root, 1_000_000);
    let ctx = CancellationToken::new();
    scanner.budget = ScannerCycleBudget::new_with_progress_tracking(&ctx, Default::default());
    let mut into = DataUsageEntry::default();
    let outcome = scanner
        .scan_folder_inner(ctx, folder, &mut into, FolderScanMode::ErasureCandidate { reuse_namespace: false })
        .await
        .expect("classify candidate");
    assert_eq!(outcome, FolderScanOutcome::ErasureData);
    assert_eq!(into.objects, 0, "candidate xl.meta cannot start accounting or maintenance before EOF");
    assert_eq!(scanner.budget.progress(), (0, 0));
    assert_eq!(scanner.budget.entries_visited(), 67);
    assert_eq!(charged_ops(&root), 68, "metadata entry only: no metadata stat or read reservation");
}

#[tokio::test]
#[serial]
async fn erasure_candidate_cancelled_batch_keeps_partial_checkpoint_and_reclassifies_on_resume() {
    let (mut scanner, root, folder) = candidate_scanner().await;
    let _guard = TestGuard {
        temp_dir: Some(root.clone()),
    };
    let path = root.join(&folder.name);
    for index in 0..65 {
        tokio::fs::write(path.join(format!("part.{index}.integrity.orphan")), b"")
            .await
            .expect("write sidecar");
    }
    tokio::fs::write(path.join("part.1"), b"shard")
        .await
        .expect("write late part");
    let _order = OrderedDirectory::new(&path);
    enable_checkpoint(&mut scanner);
    bind_budget(&mut scanner, &root, 1_000_000);
    let ctx = CancellationToken::new();
    let budget = ScannerCycleBudget::new_with_progress_tracking(&ctx, Default::default());
    scanner.budget = budget.clone();
    let cancel_budget = budget.clone();
    let cancel = tokio::spawn(async move {
        loop {
            if cancel_budget.entries_visited() >= 65 {
                cancel_budget.cancel_for_runtime();
                return;
            }
            tokio::task::yield_now().await;
        }
    });
    let parent_folder = CachedFolder {
        name: "bucket".into(),
        parent: None,
        object_heal_prob_div: 1,
    };
    let mut into = DataUsageEntry::default();
    let result = scanner.scan_folder(budget.token(), parent_folder.clone(), &mut into).await;
    cancel.await.expect("batch cancellation task");
    assert!(result.is_err());
    assert_eq!(
        budget.entries_visited(),
        65,
        "parent entry and first full candidate batch are semantic progress"
    );
    assert_eq!(charged_ops(&root), 67, "two opens and 65 entries, cancellation before the next batch");
    assert!(
        scanner.new_cache.info.failed_objects.is_empty(),
        "unfinished classification cannot manufacture missing metadata"
    );
    let snapshot = scanner
        .checkpoint_snapshot(&parent_folder, &hash_path("bucket"), &into)
        .expect("partial raw checkpoint");
    assert!(!snapshot.info.snapshot_complete);
    assert!(snapshot.info.scan_coverage_receipt.is_none());
    let cursor = snapshot
        .validated_raw_enumeration_cursor()
        .expect("validated candidate cursor");
    assert_eq!(cursor.parent, folder.name);
    assert_eq!(cursor.entries_seen, 64);
    let index = snapshot
        .validated_raw_enumeration_page_index()
        .expect("validated candidate index");
    assert_eq!(index.indexed_entries().expect("index entries").len(), 64);
    let bytes = serde_json::to_vec(&snapshot).expect("serialize partial checkpoint");
    let reloaded: DataUsageCache = serde_json::from_slice(&bytes).expect("reload partial checkpoint");
    scanner.old_cache = reloaded.clone();
    scanner.new_cache = reloaded.clone();
    scanner.update_cache = reloaded;
    scanner.raw_enumeration_progress.clear();
    let next_budget = ScannerCycleBudget::new_with_progress_tracking(&ctx, Default::default());
    scanner.budget = next_budget.clone();
    scanner
        .scan_folder(next_budget.token(), parent_folder, &mut DataUsageEntry::default())
        .await
        .expect("revalidate resumed candidate");
    assert!(
        scanner
            .new_cache
            .info
            .failed_objects
            .contains_key(root.join("bucket/xl.meta").to_string_lossy().as_ref())
    );
    assert_eq!(
        charged_ops(&root),
        136,
        "resume recharges actual reads; the raw index is not a filesystem seek"
    );
}

#[tokio::test]
#[serial]
async fn erasure_late_data_sibling_prevents_parent_coverage_certification() {
    let (mut scanner, root) = build_test_scanner().await;
    let _guard = TestGuard {
        temp_dir: Some(root.clone()),
    };
    enable_checkpoint(&mut scanner);
    scanner.is_erasure_mode = true;
    bind_budget(&mut scanner, &root, 1_000_000);
    let first = format!("object/{}", Uuid::from_u128(1));
    write_test_object_metadata(&root, "bucket", &first).await;
    let data = root.join("bucket/object").join(Uuid::from_u128(2).to_string());
    tokio::fs::create_dir_all(&data).await.expect("create later data sibling");
    tokio::fs::write(data.join("part.1"), b"shard")
        .await
        .expect("write later part");
    let mut into = DataUsageEntry::default();
    scanner
        .scan_folder(
            CancellationToken::new(),
            CachedFolder {
                name: "bucket/object".into(),
                parent: Some(hash_path("bucket")),
                object_heal_prob_div: 1,
            },
            &mut into,
        )
        .await
        .expect("scan mixed siblings");
    assert_eq!(
        scanner
            .new_cache
            .find(&format!("bucket/{first}"))
            .expect("preceding UUID object retained")
            .objects,
        1,
        "the preceding legitimate UUID object was scanned once"
    );
    assert_eq!(into.failed_objects, 1);
    assert!(scanner.coverage_gap);
    assert!(
        scanner.coverage_frontier.is_none(),
        "completed siblings cannot certify unresolved parent coverage"
    );
    assert!(
        scanner
            .new_cache
            .info
            .failed_objects
            .contains_key(root.join("bucket/object/xl.meta").to_string_lossy().as_ref())
    );
}

#[tokio::test]
#[serial]
async fn erasure_candidate_rechecks_data_below_a_restored_namespace_frontier() {
    let (mut scanner, root, folder) = candidate_scanner().await;
    let _guard = TestGuard {
        temp_dir: Some(root.clone()),
    };
    enable_checkpoint(&mut scanner);
    tokio::fs::write(root.join(&folder.name).join("part.1"), b"shard")
        .await
        .expect("write part");
    let hash = hash_path(&folder.name);
    scanner.old_cache.replace_hashed(
        &hash,
        &Some(hash_path("bucket")),
        &DataUsageEntry {
            objects: 1,
            compacted: true,
            ..Default::default()
        },
    );
    scanner.resume_frontier = Some(folder.name.clone());
    bind_budget(&mut scanner, &root, 1_000_000);
    let mut into = DataUsageEntry::default();
    scanner
        .scan_folder(
            CancellationToken::new(),
            CachedFolder {
                name: "bucket".into(),
                parent: None,
                object_heal_prob_div: 1,
            },
            &mut into,
        )
        .await
        .expect("reclassify covered UUID directory");
    assert_eq!(into.failed_objects, 1);
    assert_eq!(into.objects, 0, "stale namespace totals cannot bypass current part evidence");
    assert!(scanner.new_cache.find(&folder.name).is_none());
}

#[cfg(unix)]
#[tokio::test]
#[serial]
async fn erasure_candidate_part_symlink_is_not_data_evidence() {
    let (mut scanner, root, folder) = candidate_scanner().await;
    let _guard = TestGuard {
        temp_dir: Some(root.clone()),
    };
    let target = root.join("target");
    tokio::fs::write(&target, b"shard").await.expect("write symlink target");
    symlink(&target, root.join(&folder.name).join("part.1")).expect("create part symlink");
    bind_budget(&mut scanner, &root, 1_000_000);
    let outcome = scanner
        .scan_folder_inner(
            CancellationToken::new(),
            folder,
            &mut DataUsageEntry::default(),
            FolderScanMode::ErasureCandidate { reuse_namespace: false },
        )
        .await
        .expect("classify symlink namespace");
    assert_eq!(outcome, FolderScanOutcome::Namespace);
    assert_eq!(charged_ops(&root), 3, "open, entry, symlink target stat each once");
    assert!(scanner.new_cache.info.failed_objects.is_empty());
}

#[tokio::test]
#[serial]
async fn erasure_candidate_covered_namespace_reuses_totals_after_classification() {
    let (mut scanner, root, folder) = candidate_scanner().await;
    let _guard = TestGuard {
        temp_dir: Some(root.clone()),
    };
    let object = folder.name.strip_prefix("bucket/").expect("candidate in bucket");
    write_test_object_metadata(&root, "bucket", object).await;
    enable_checkpoint(&mut scanner);
    scanner.old_cache.replace_hashed(
        &hash_path(&folder.name),
        &folder.parent,
        &DataUsageEntry {
            objects: 1,
            ..Default::default()
        },
    );
    scanner.resume_frontier = Some(folder.name.clone());
    bind_budget(&mut scanner, &root, 1_000_000);
    let ctx = CancellationToken::new();
    let budget = ScannerCycleBudget::new_with_progress_tracking(
        &ctx,
        crate::scanner_budget::ScannerCycleBudgetConfig {
            max_objects: Some(1),
            ..Default::default()
        },
    );
    scanner.budget = budget.clone();
    scanner
        .scan_folder(
            budget.token(),
            CachedFolder {
                name: "bucket".into(),
                parent: None,
                object_heal_prob_div: 1,
            },
            &mut DataUsageEntry::default(),
        )
        .await
        .expect("covered candidate must not exhaust object quota again");
    assert_eq!(budget.reason(), None);
    assert_eq!(budget.progress(), (0, 1), "only the parent consumes namespace quota");
    assert_eq!(charged_ops(&root), 4, "list both directories once without re-reading covered metadata");
    assert_eq!(scanner.new_cache.find(&folder.name).expect("reused candidate").objects, 1);
}

#[tokio::test]
#[serial]
async fn erasure_candidate_iteration_error_cannot_replay_deferred_metadata_as_eof() {
    for kind in [std::io::ErrorKind::NotFound, std::io::ErrorKind::NotADirectory] {
        let (mut scanner, root, folder) = candidate_scanner().await;
        let _guard = TestGuard {
            temp_dir: Some(root.clone()),
        };
        let path = root.join(&folder.name);
        let object = folder.name.strip_prefix("bucket/").expect("candidate in bucket");
        write_test_object_metadata(&root, "bucket", object).await;
        tokio::fs::write(path.join("part.1"), b"shard")
            .await
            .expect("write unread part");
        let _error = OrderedDirectory::failing(&path, 1, kind);
        bind_budget(&mut scanner, &root, 1_000_000);
        let result = scanner
            .scan_folder(
                CancellationToken::new(),
                CachedFolder {
                    name: "bucket".into(),
                    parent: None,
                    object_heal_prob_div: 1,
                },
                &mut DataUsageEntry::default(),
            )
            .await;
        assert!(
            matches!(result, Err(ScannerError::Io(err)) if err.kind() == kind),
            "candidate error must remain typed and cannot become namespace completion"
        );
        assert_eq!(
            charged_ops(&root),
            4,
            "two opens, parent entry, deferred metadata entry; no metadata read"
        );
        assert!(
            scanner.new_cache.info.failed_objects.is_empty(),
            "unknown classification is not missing-metadata evidence"
        );
        assert!(scanner.new_cache.find(&folder.name).is_none());
    }
}

#[tokio::test]
#[serial]
async fn erasure_candidate_preserves_compacted_namespace_sampling_after_classification() {
    let (mut scanner, root, folder) = candidate_scanner().await;
    let _guard = TestGuard {
        temp_dir: Some(root.clone()),
    };
    let object = folder.name.strip_prefix("bucket/").expect("candidate in bucket");
    write_test_object_metadata(&root, "bucket", object).await;
    let hash = hash_path(&folder.name);
    scanner.old_cache.replace_hashed(
        &hash,
        &folder.parent,
        &DataUsageEntry {
            objects: 1,
            compacted: true,
            ..Default::default()
        },
    );
    let skipped_cycle = (0..data_usage_update_dir_cycles())
        .find(|cycle| !hash.mod_(*cycle, data_usage_update_dir_cycles()))
        .expect("at least one cycle samples another namespace");
    scanner.old_cache.info.next_cycle = u64::from(skipped_cycle);
    bind_budget(&mut scanner, &root, 1_000_000);
    let ctx = CancellationToken::new();
    let budget = ScannerCycleBudget::new_with_progress_tracking(
        &ctx,
        crate::scanner_budget::ScannerCycleBudgetConfig {
            max_objects: Some(1),
            ..Default::default()
        },
    );
    scanner.budget = budget.clone();
    scanner
        .scan_folder(
            budget.token(),
            CachedFolder {
                name: "bucket".into(),
                parent: None,
                object_heal_prob_div: 1,
            },
            &mut DataUsageEntry::default(),
        )
        .await
        .expect("sampled candidate must not consume object quota");
    assert_eq!(budget.reason(), None);
    assert_eq!(budget.progress(), (0, 1));
    assert_eq!(
        charged_ops(&root),
        4,
        "sampling still requires full current classification, without metadata re-read"
    );
    assert_eq!(scanner.new_cache.find(&folder.name).expect("sampled candidate").objects, 1);
}

#[tokio::test]
#[serial]
async fn erasure_candidate_vanished_before_open_preserves_other_namespace_progress() {
    for kind in [std::io::ErrorKind::NotFound, std::io::ErrorKind::NotADirectory] {
        let (mut scanner, root, folder) = candidate_scanner().await;
        let _guard = TestGuard {
            temp_dir: Some(root.clone()),
        };
        enable_checkpoint(&mut scanner);
        scanner.old_cache.replace_hashed(
            &hash_path(&folder.name),
            &folder.parent,
            &DataUsageEntry {
                objects: 1,
                ..Default::default()
            },
        );
        write_test_object_metadata(&root, "bucket", "remaining").await;
        let _vanished = OrderedDirectory::fail_open(&root.join(&folder.name), kind);
        bind_budget(&mut scanner, &root, 1_000_000);
        let ctx = CancellationToken::new();
        let budget = ScannerCycleBudget::new_with_progress_tracking(
            &ctx,
            crate::scanner_budget::ScannerCycleBudgetConfig {
                max_directories: Some(2),
                ..Default::default()
            },
        );
        scanner.budget = budget.clone();
        let mut into = DataUsageEntry::default();
        scanner
            .scan_folder(
                budget.token(),
                CachedFolder {
                    name: "bucket".into(),
                    parent: None,
                    object_heal_prob_div: 1,
                },
                &mut into,
            )
            .await
            .expect("concurrent candidate deletion or replacement must not fail the remaining namespace scan");
        assert_eq!(budget.reason(), None);
        assert_eq!(budget.progress().1, 2, "only parent and remaining namespace consume directory quota");
        assert!(
            scanner.new_cache.find(&folder.name).is_none(),
            "vanished candidate cannot retain stale cached totals"
        );
        assert_eq!(
            scanner
                .new_cache
                .find("bucket/remaining")
                .expect("remaining object scanned")
                .objects,
            1
        );
        assert_eq!(
            charged_ops(&root),
            7,
            "vanished open is charged but contributes no entry or metadata read"
        );
        assert!(scanner.new_cache.info.failed_objects.is_empty());
        assert!(
            scanner.new_cache.info.pending_heals.is_empty(),
            "vanished directory is not missing-xl.meta evidence"
        );
        assert_eq!(
            scanner.coverage_gap,
            kind == std::io::ErrorKind::NotADirectory,
            "a replaced directory retains the existing incomplete-coverage behavior"
        );
    }
}
