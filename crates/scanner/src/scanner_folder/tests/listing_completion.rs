// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.

use super::*;

#[test]
#[serial]
fn abandoned_listing_drains_candidate_buffered_before_finished() {
    use std::io::{Read, Seek, SeekFrom};
    use std::process::{Command, Stdio};

    const TEST: &str = "scanner_folder::tests::listing_completion::abandoned_listing_drains_candidate_buffered_before_finished";
    const CHILD: &str = "RUSTFS_ABANDONED_LISTING_OVERFLOW_CHILD";
    const MARKER: &str = "abandoned-listing-overflow-completed";

    if std::env::var_os(CHILD).is_some() {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("fixture runtime")
            .block_on(run_abandoned_listing_overflow_case());
        println!("{MARKER}");
        return;
    }

    let output_dir = tempfile::tempdir().expect("child output directory");
    let mut output = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(output_dir.path().join("output.log"))
        .expect("child output");
    let output_writer = output.try_clone().expect("clone child output");
    let mut child = Command::new(std::env::current_exe().expect("test executable"))
        .args(["--exact", TEST, "--nocapture"])
        .env(CHILD, "1")
        .env("TMPDIR", output_dir.path())
        .env("TMP", output_dir.path())
        .env("TEMP", output_dir.path())
        .stdout(Stdio::from(output_writer.try_clone().expect("clone stdout")))
        .stderr(Stdio::from(output_writer))
        .spawn()
        .expect("start isolated listing fixture");

    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) if std::time::Instant::now() < deadline => std::thread::sleep(Duration::from_millis(20)),
            Ok(None) => {
                let _ = child.kill();
                let reap_deadline = std::time::Instant::now() + Duration::from_secs(5);
                loop {
                    match child.try_wait() {
                        Ok(Some(_)) => {
                            let path = output_dir.keep();
                            panic!("listing fixture timed out and was reaped; evidence={}", path.display());
                        }
                        Ok(None) if std::time::Instant::now() < reap_deadline => {
                            std::thread::sleep(Duration::from_millis(20));
                        }
                        _ => retain_unknown_listing_child(child, output_dir),
                    }
                }
            }
            Err(_) => retain_unknown_listing_child(child, output_dir),
        }
    };
    let log = (|| -> std::io::Result<Vec<u8>> {
        output.seek(SeekFrom::Start(0))?;
        let mut log = Vec::new();
        Read::by_ref(&mut output).take(1_048_577).read_to_end(&mut log)?;
        if log.len() > 1_048_576 {
            return Err(std::io::Error::other("listing fixture output is too large"));
        }
        Ok(log)
    })();
    let log = match log {
        Ok(log) => log,
        Err(_) => {
            let path = output_dir.keep();
            panic!("failed to read listing fixture output; evidence={}", path.display());
        }
    };
    let marker_count = log
        .windows(MARKER.len())
        .filter(|window| *window == MARKER.as_bytes())
        .count();
    if !status.success() || marker_count != 1 {
        let path = output_dir.keep();
        panic!(
            "isolated listing fixture failed or missed its completion marker; evidence={}",
            path.display()
        );
    }
    drop(output);
}

fn retain_unknown_listing_child(child: std::process::Child, output_dir: tempfile::TempDir) -> ! {
    static UNKNOWN_CHILDREN: std::sync::Mutex<Vec<std::process::Child>> = std::sync::Mutex::new(Vec::new());
    let pid = child.id();
    let path = output_dir.keep();
    UNKNOWN_CHILDREN
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .push(child);
    panic!("listing fixture child status unknown; pid={pid}, evidence={}", path.display());
}

async fn run_abandoned_listing_overflow_case() {
    let (mut scanner, root) = build_test_scanner().await;
    let bucket = "bucket";
    let object = "abandoned/object";
    tokio::fs::create_dir_all(root.join(bucket)).await.expect("local bucket");
    let mut disks = vec![scanner.local_disk.clone()];
    let mut original_metadata = Vec::new();
    for (index, version) in [(2, Some(0x1933)), (3, None), (4, Some(0x1935))] {
        let disk_root = root.join(format!("disk-{index}"));
        let mut fi = FileInfo::new(object, 4, 2);
        fi.volume = bucket.to_owned();
        fi.name = object.to_owned();
        fi.version_id = version.map(Uuid::from_u128);
        fi.versioned = fi.version_id.is_some();
        fi.erasure.index = index - 1;
        fi.mod_time = Some(OffsetDateTime::from_unix_timestamp(1_705_312_300).expect("valid timestamp"));
        fi.validate_for_metadata_read().expect("valid per-disk fixture");
        let mut meta = FileMeta::new();
        meta.add_version(fi).expect("fixture version");
        let metadata = meta.marshal_msg().expect("fixture metadata");
        write_test_object_metadata_bytes(&disk_root, bucket, object, &metadata).await;
        original_metadata.push((disk_root.join(bucket).join(object).join(STORAGE_FORMAT_FILE), metadata));
        let endpoint = Endpoint::try_from(disk_root.to_string_lossy().as_ref()).expect("listing fixture endpoint");
        disks.push(
            new_disk(
                &endpoint,
                &DiskOption {
                    cleanup: false,
                    health_check: false,
                },
            )
            .await
            .expect("listing fixture disk"),
        );
    }
    for cache in [&mut scanner.old_cache, &mut scanner.new_cache] {
        cache.info.name = bucket.to_owned();
    }
    scanner.old_cache.replace(
        "bucket/abandoned/object",
        bucket,
        DataUsageEntry {
            objects: 1,
            ..Default::default()
        },
    );
    scanner.heal_object_select = 1;
    scanner.disks = disks;
    scanner.disks_quorum = 2;
    scanner.sleeper = DynamicSleeper::new(rustfs_config::ScannerSpeed::Fastest);
    scanner.partial_heal_candidate_limit = Some(1);
    let (discovery_tx, mut discovery_rx) = mpsc::unbounded_channel();
    scanner.partial_heal_discovery_observer = Some(discovery_tx);
    // Force the valid select ordering that receives terminal errors before the
    // final candidate. One candidate frame fits in the bounded partial channel.
    scanner.defer_abandoned_entries_until_finished = true;
    let (_commands, mut heal_rx) = rustfs_heal_contracts::heal_channel::init_heal_channels().expect("isolated heal channel");
    let (captured_tx, mut captured_rx) = mpsc::unbounded_channel();
    let responder = tokio::spawn(async move {
        while let Some(command) = heal_rx.recv().await {
            let rustfs_heal_contracts::heal_channel::HealReceiptCommand { request, response_tx } = command;
            captured_tx
                .send((request.object_prefix.clone(), request.object_version_id.clone(), request.remove_corrupted))
                .expect("candidate capture remains open");
            let _ = response_tx.send(Ok(rustfs_heal_contracts::heal_channel::HealAdmissionReceipt {
                result: HealAdmissionResult::Accepted,
                task_id: request.id,
            }));
        }
    });
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        scanner.scan_folder(
            CancellationToken::new(),
            CachedFolder {
                name: bucket.to_owned(),
                parent: None,
                object_heal_prob_div: 1,
            },
            &mut DataUsageEntry::default(),
        ),
    )
    .await;
    responder.abort();
    let _ = responder.await;
    result
        .expect("listing completion must close every callback channel")
        .expect("complete folder scan");
    assert_eq!(
        discovery_rx.try_recv().expect("partial frame should be observed"),
        (1, 2),
        "the unversioned candidate must be in the overflow of the same frame"
    );
    let mut candidates = Vec::new();
    while let Ok((object, version, remove_corrupted)) = captured_rx.try_recv() {
        if let Some(object) = object {
            candidates.push((object, version, remove_corrupted));
        }
    }
    assert!(
        candidates.first().is_some_and(|(_, version, _)| version.is_some()),
        "the first candidate must be versioned"
    );
    let candidates: HashSet<_> = candidates.into_iter().collect();
    let object = format!("{object}/");
    let expected = HashSet::from([
        (object.clone(), Some(Uuid::from_u128(0x1933).to_string()), Some(true)),
        (object.clone(), None, Some(false)),
        (object, Some(Uuid::from_u128(0x1935).to_string()), Some(true)),
    ]);
    assert_eq!(
        candidates, expected,
        "buffered overflow candidates must retain their exact version and deletion policy"
    );
    for (path, metadata) in original_metadata {
        assert_eq!(tokio::fs::read(path).await.expect("retained metadata"), metadata);
    }
}
