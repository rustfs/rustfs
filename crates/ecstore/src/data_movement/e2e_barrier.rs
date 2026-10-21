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

//! Test-only barriers inside the data-movement commit windows.
//!
//! Compiled only with the `e2e-test-hooks` feature and inert unless
//! `RUSTFS_E2E_DATA_MOVEMENT_BARRIER_DIR` names an absolute directory holding
//! an `arm.json`. A mover that matches the arm records one `reached.*.json`
//! observation and then blocks until `release.<nonce>` exists. A release body
//! of `io_error` fails the window instead of continuing, so a fixture can
//! inject a target-commit or source-cleanup failure at a proven point. The
//! observation file is the only evidence that an injection reached its window;
//! a fixture must not count an unreached barrier as a fault test.

use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::time::Duration;

use tokio::fs;
use tokio::time::Instant;

pub(crate) const BARRIER_DIR_ENV: &str = "RUSTFS_E2E_DATA_MOVEMENT_BARRIER_DIR";
/// Every target part is staged and the target version is not yet published.
pub(crate) const STAGE_BEFORE_TARGET_COMMIT: &str = "before_target_commit";
/// The target commit is durable and the source preflight matched; the source
/// delete has not been issued.
pub(crate) const STAGE_BEFORE_SOURCE_CLEANUP: &str = "before_source_cleanup";
/// A decommission start is durable in `pool.bin`; no worker has started and
/// the admin request has not been acknowledged.
pub(crate) const STAGE_AFTER_RESPONSIBILITY_PERSISTED: &str = "after_responsibility_persisted";
const RELEASE_TIMEOUT: Duration = Duration::from_secs(120);
const RELEASE_POLL: Duration = Duration::from_millis(20);
const RELEASE_IO_ERROR: &[u8] = b"io_error";

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct BarrierArm {
    nonce: String,
    bucket: String,
    object: String,
    stage: String,
    /// `decommission` or `rebalance`; absent matches either mover. Movers label
    /// their stages with that word or a `<word>_` prefix (`decommission_object`).
    #[serde(default)]
    operation: Option<String>,
}

fn valid_nonce(nonce: &str) -> bool {
    !nonce.is_empty()
        && nonce.len() <= 64
        && nonce
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
}

async fn read_arm(directory: &Path) -> std::io::Result<Option<BarrierArm>> {
    match fs::read(directory.join("arm.json")).await {
        Ok(bytes) => serde_json::from_slice(&bytes).map(Some).map_err(std::io::Error::other),
        Err(err) if err.kind() == ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err),
    }
}

/// Hold `operation` at `stage` for `bucket/object` while a matching arm exists.
pub(crate) async fn wait(operation: &str, bucket: &str, object: &str, stage: &str) -> std::io::Result<()> {
    let Some(directory) = std::env::var_os(BARRIER_DIR_ENV).map(PathBuf::from) else {
        return Ok(());
    };
    let Some(arm) = read_arm(&directory).await? else {
        return Ok(());
    };
    if arm.bucket != bucket
        || arm.object != object
        || arm.stage != stage
        || arm
            .operation
            .as_deref()
            .is_some_and(|armed| armed != operation && !operation.starts_with(&format!("{armed}_")))
    {
        return Ok(());
    }
    if !directory.is_absolute() || !valid_nonce(&arm.nonce) {
        return Err(std::io::Error::new(
            ErrorKind::InvalidInput,
            "data movement test barrier requires an absolute directory and a safe nonce",
        ));
    }
    let pid = std::process::id();
    let observation = serde_json::json!({
        "operation": operation,
        "bucket": bucket,
        "object": object,
        "stage": stage,
        "pid": pid,
        "nonce": arm.nonce,
        "reached_at_unix_ms": time::OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000,
    });
    let reached = directory.join(format!("reached.{}.{pid}.{}.json", arm.nonce, uuid::Uuid::new_v4().simple()));
    fs::write(&reached, serde_json::to_vec(&observation)?).await?;
    let release = directory.join(format!("release.{}", arm.nonce));
    let deadline = Instant::now() + RELEASE_TIMEOUT;
    loop {
        match fs::read(&release).await {
            Ok(body) if body == RELEASE_IO_ERROR => {
                // The reached observation already names the stage and object.
                return Err(std::io::Error::other("injected data movement barrier failure"));
            }
            Ok(_) => return Ok(()),
            Err(err) if err.kind() == ErrorKind::NotFound => {}
            Err(err) => return Err(err),
        }
        if Instant::now() >= deadline {
            return Err(std::io::Error::new(ErrorKind::TimedOut, "data movement test barrier release timed out"));
        }
        tokio::time::sleep(RELEASE_POLL).await;
    }
}
