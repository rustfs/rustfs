// Copyright 2024 RustFS Team
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

use crate::common::{DEFAULT_ACCESS_KEY, DEFAULT_SECRET_KEY, RustFSTestEnvironment};
use serial_test::serial;
use std::path::Path;
use std::process::Command;
use std::time::Duration;
use tokio::fs;
use tokio::time::timeout;
use tracing::info;

const BUCKET: &str = "ddd";
const OBJECT_COUNT: usize = 484;

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

async fn create_issue_3107_fixture(root: &Path) -> TestResult {
    for idx in 0..OBJECT_COUNT {
        let object_path = root.join("ddd").join("requirements").join(format!("file-{idx:04}.txt"));
        fs::create_dir_all(object_path.parent().expect("object parent should exist")).await?;
        fs::write(object_path, format!("issue-3107 payload {idx}\n").repeat(128)).await?;
    }

    for dir in ["aaa", "ccc", "ccc-package"] {
        let object_path = root.join(dir).join("marker.txt");
        fs::create_dir_all(object_path.parent().expect("object parent should exist")).await?;
        fs::write(object_path, dir.as_bytes()).await?;
    }

    Ok(())
}

fn mc_available() -> bool {
    Command::new("mc")
        .arg("--version")
        .output()
        .is_ok_and(|output| output.status.success())
}

fn run_mc(args: &[&str]) -> TestResult {
    let output = Command::new("mc").args(args).output()?;
    if !output.status.success() {
        return Err(format!(
            "mc {:?} failed: status={:?}, stdout={}, stderr={}",
            args,
            output.status.code(),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }
    Ok(())
}

fn count_files(root: &Path) -> usize {
    walkdir::WalkDir::new(root)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|entry| entry.file_type().is_file())
        .count()
}

#[tokio::test]
#[serial]
async fn test_mc_mirror_small_bucket_completes_without_list_timeout() -> TestResult {
    crate::common::init_logging();
    info!("Starting issue #3107 mc mirror regression test");
    if !mc_available() {
        info!("Skipping issue #3107 mc mirror regression test because mc is not installed");
        return Ok(());
    }

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;
    env.create_test_bucket(BUCKET).await?;

    let alias = format!("rustfs-issue-3107-{}", uuid::Uuid::new_v4());
    run_mc(&["alias", "set", &alias, &env.url, DEFAULT_ACCESS_KEY, DEFAULT_SECRET_KEY])?;

    let fixture = Path::new(&env.temp_dir).join("issue-3107-fixture");
    let backup = Path::new(&env.temp_dir).join("issue-3107-backup");
    fs::create_dir_all(&fixture).await?;
    fs::create_dir_all(&backup).await?;
    create_issue_3107_fixture(&fixture).await?;

    let bucket_target = format!("{alias}/{BUCKET}");
    let source_path = fixture.to_string_lossy().to_string();
    run_mc(&["mirror", "--overwrite", &source_path, &bucket_target])?;

    let backup_path = backup.join("ddd-backup");
    let backup_path_string = backup_path.to_string_lossy().to_string();
    timeout(
        Duration::from_secs(20),
        tokio::task::spawn_blocking({
            let bucket_target = bucket_target.clone();
            let backup_path_string = backup_path_string.clone();
            move || run_mc(&["mirror", "--overwrite", &bucket_target, &backup_path_string])
        }),
    )
    .await
    .map_err(|_| "mc mirror timed out while listing/comparing a small bucket")???;

    let mirrored_count = count_files(&backup_path);
    assert_eq!(
        mirrored_count,
        OBJECT_COUNT + 3,
        "mc mirror should copy every object from the issue #3107 small bucket fixture"
    );

    run_mc(&["alias", "remove", &alias])?;
    Ok(())
}
