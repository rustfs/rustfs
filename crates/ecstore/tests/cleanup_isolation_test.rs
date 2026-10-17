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

#![cfg(target_os = "linux")]

use bytes::Bytes;
mod storage_api;

use std::time::Duration;
use storage_api::cleanup_isolation::{DeleteOptions, DiskAPI, DiskError, DiskOption, Endpoint, new_disk};

#[tokio::test]
async fn cleanup_disk_and_periodic_gc_use_affinity_without_changing_foreground() {
    const CHILD: &str = "RUSTFS_CLEANUP_LINUX_TEST_CHILD";
    let foreground = rustix::thread::sched_getaffinity(None).unwrap();
    let cpu = (0..rustix::thread::CpuSet::MAX_CPU)
        .find(|cpu| foreground.is_set(*cpu))
        .expect("the test process must have an allowed CPU");
    if std::env::var_os(CHILD).is_none() {
        for mode in ["custom", "default", "none", "restricted"] {
            if mode == "default" && (!foreground.is_set(1) || !foreground.is_set(2)) {
                continue;
            }
            let output = tokio::task::spawn_blocking(move || {
                let mut command = std::process::Command::new(std::env::current_exe().unwrap());
                command
                    .args([
                        "--exact",
                        "cleanup_disk_and_periodic_gc_use_affinity_without_changing_foreground",
                        "--nocapture",
                    ])
                    .env(CHILD, mode)
                    .env("RUSTFS_CLEANUP_ISOLATE_ENABLE", "true")
                    .env("RUSTFS_PUT_RENAME_TAIL_CLEANUP_MAX_PENDING", "4")
                    .env("RUSTFS_PUT_RENAME_TAIL_CLEANUP_DEFER_WORKERS", "1")
                    .env("RUSTFS_CLEANUP_DISK_MAX_PENDING", "4")
                    .env("RUSTFS_CLEANUP_DISK_WORKERS", "1")
                    .env("RUSTFS_CLEANUP_GC_WORKERS", "1")
                    .env("RUSTFS_CLEANUP_BLOCKING_THREADS", "1")
                    .env("RUSTFS_CLEANUP_ASYNC_THREADS", "1")
                    .env_remove("RUSTFS_CLEANUP_CPUS")
                    .env_remove("RUSTFS_PUT_RENAME_TAIL_CLEANUP_COUNTERFACTUAL_SKIP")
                    .env_remove("RUSTFS_PUT_RENAME_TAIL_CLEANUP_DEFER_HOLD_WORKER")
                    .env_remove("RUSTFS_PUT_RENAME_TAIL_CLEANUP_ZERO_TARGET_TMP_DELETE_SKIP");
                match mode {
                    "custom" => {
                        command.env("RUSTFS_CLEANUP_CPUS", cpu.to_string());
                    }
                    "none" => {
                        command.env("RUSTFS_CLEANUP_CPUS", "none");
                    }
                    _ => {}
                }
                command.output().unwrap()
            })
            .await
            .unwrap();
            assert!(
                output.status.success(),
                "{}\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed"));
        }
        return;
    }

    let mode = std::env::var(CHILD).unwrap();
    if mode == "restricted" {
        let mut restricted = rustix::thread::CpuSet::new();
        restricted.set(cpu);
        rustix::thread::sched_setaffinity(None, &restricted).expect("restrict initialization to one allowed CPU");
    }
    let dir = tempfile::tempdir().unwrap();
    let disk = new_disk(&Endpoint::try_from(dir.path().to_str().unwrap()).unwrap(), &DiskOption::default())
        .await
        .unwrap();
    let bucket = "cleanup-affinity";
    disk.make_volume(bucket).await.unwrap();
    disk.write_all(bucket, "old/part.1", Bytes::from_static(b"garbage"))
        .await
        .unwrap();
    let receipt = disk
        .write_all(bucket, "old/.rustfs-old-data-cleanup-receipt.json", Bytes::from_static(b"receipt"))
        .await;
    if mode == "restricted" {
        assert!(
            receipt
                .expect_err("unavailable default CPUs must reject cleanup")
                .to_string()
                .contains("allowed CPU set")
        );
        assert_eq!(disk.read_all(bucket, "old/part.1").await.unwrap(), Bytes::from_static(b"garbage"));
        return;
    }
    receipt.unwrap();
    let expected_mask = match mode.as_str() {
        "default" => "1-2".to_owned(),
        "none" => std::fs::read_to_string("/proc/thread-self/status")
            .unwrap()
            .lines()
            .find_map(|line| line.strip_prefix("Cpus_allowed_list:"))
            .unwrap()
            .trim()
            .to_owned(),
        _ => cpu.to_string(),
    };
    disk.write_all(bucket, "live/part.1", Bytes::from_static(b"keep"))
        .await
        .unwrap();
    disk.delete(
        bucket,
        "old",
        DeleteOptions {
            recursive: true,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert!(matches!(disk.read_all(bucket, "old/part.1").await, Err(DiskError::FileNotFound)));

    // One async worker and one lazily started blocking worker. Inspect the
    // real process threads, including the threads executing filesystem work.
    let mut cleanup_threads = 0;
    for entry in std::fs::read_dir("/proc/self/task").unwrap() {
        let task = entry.unwrap().path();
        let Ok(name) = std::fs::read_to_string(task.join("comm")) else {
            continue;
        };
        if name.trim() != "rustfs-cleanup" {
            continue;
        }
        cleanup_threads += 1;
        let status = std::fs::read_to_string(task.join("status")).unwrap();
        let mask = status
            .lines()
            .find_map(|line| line.strip_prefix("Cpus_allowed_list:"))
            .unwrap()
            .trim();
        assert_eq!(mask, expected_mask);
    }
    assert_eq!(cleanup_threads, 2, "async and blocking workers must both use the configured runtime");
    assert_eq!(rustix::thread::sched_getaffinity(None).unwrap(), foreground);

    let trash = dir.path().join(".rustfs.sys/tmp/.trash");
    assert!(std::fs::read_dir(&trash).unwrap().next().is_some());
    // Advance the foreground timer that dispatches the five-minute periodic
    // scan; the dedicated runtime then performs the actual physical removal.
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(301)).await;
    tokio::time::resume();
    tokio::time::timeout(Duration::from_secs(20), async {
        while std::fs::read_dir(&trash).unwrap().next().is_some() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("periodic GC must physically remove trash");
    assert_eq!(disk.read_all(bucket, "live/part.1").await.unwrap(), Bytes::from_static(b"keep"));
    assert_eq!(rustix::thread::sched_getaffinity(None).unwrap(), foreground);
}
