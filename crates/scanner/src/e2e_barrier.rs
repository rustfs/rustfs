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

//! Test-cluster observations and bounded pauses at real scanner boundaries.
//! This module is absent unless `e2e-test-hooks` is explicitly enabled.

pub(crate) mod worker_observation;

use serde::Deserialize;
use serde_json::{Value, json};
use std::io::{self, Write};
use std::path::{Component, Path};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

const MAX_BYTES: u64 = 1024 * 1024;
const MAX_EVENTS: u64 = 512;
const MAX_WAIT: Duration = Duration::from_secs(120);
static SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Session {
    nonce: String,
    buckets: Vec<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Arm {
    nonce: String,
    stage: String,
    pid: u32,
    owner_id: Option<String>,
    instance_id: Option<String>,
    epoch: Option<u64>,
    cycle: Option<u64>,
}

fn valid_component(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
}

fn read_control<T: serde::de::DeserializeOwned>(path: &Path) -> io::Result<Option<T>> {
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    if !metadata.is_file() || metadata.len() > MAX_BYTES {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid scanner test control file"));
    }
    let data = std::fs::read(path)?;
    if data.len() > MAX_BYTES as usize {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "scanner test control file grew past its bound",
        ));
    }
    serde_json::from_slice(&data).map(Some).map_err(io::Error::other)
}

fn write_observation(directory: &Path, name: &str, bytes: &[u8]) -> io::Result<()> {
    let temporary = directory.join(format!(".{name}.tmp"));
    let mut output = std::fs::OpenOptions::new().write(true).create_new(true).open(&temporary)?;
    output.write_all(bytes)?;
    output.sync_all()?;
    std::fs::rename(temporary, directory.join(name))
}

/// Observe a real event and, only for an explicitly armed matching session,
/// wait for its release file. No test control can supply scanner state.
pub async fn checkpoint(stage: &str, detail: Value) -> io::Result<()> {
    let Some(directory) = std::env::var_os("RUSTFS_E2E_SCANNER_BARRIER_DIR") else {
        return Ok(());
    };
    let directory = Path::new(&directory);
    if !directory.is_absolute()
        || directory.components().any(|part| matches!(part, Component::ParentDir))
        || !valid_component(stage)
    {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "invalid scanner test barrier path or stage"));
    }
    let Some(session) = read_control::<Session>(&directory.join("session.json"))? else {
        return Ok(());
    };
    if !valid_component(&session.nonce)
        || session.buckets.is_empty()
        || session.buckets.len() > 32
        || session.buckets.iter().any(|bucket| bucket.is_empty() || bucket.len() > 255)
    {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "invalid scanner test session"));
    }
    // Admission may enable the recorder while this actual start gate is held.
    // Begin on release, before any snapshot or worker; other boundaries stay synchronous here.
    if stage != "before_start_snapshot" {
        worker_observation::boundary(stage, &detail);
    }
    if detail
        .get("bucket")
        .and_then(Value::as_str)
        .is_some_and(|bucket| !session.buckets.iter().any(|allowed| allowed == bucket))
    {
        return Ok(());
    }
    let arm = read_control::<Arm>(&directory.join("arm.json"))?;
    let armed = arm.is_some_and(|arm| {
        arm.nonce == session.nonce
            && arm.stage == stage
            && arm.pid == std::process::id()
            && arm
                .owner_id
                .as_ref()
                .is_none_or(|owner| detail.get("owner_id").and_then(Value::as_str) == Some(owner.as_str()))
            && arm
                .instance_id
                .as_ref()
                .is_none_or(|instance| detail.get("instance_id").and_then(Value::as_str) == Some(instance.as_str()))
            && arm
                .epoch
                .is_none_or(|epoch| detail.get("epoch").and_then(Value::as_u64) == Some(epoch))
            && arm
                .cycle
                .is_none_or(|cycle| detail.get("cycle").and_then(Value::as_u64) == Some(cycle))
    });
    let sequence = SEQUENCE.fetch_add(1, Ordering::Relaxed);
    if sequence >= MAX_EVENTS {
        return Err(io::Error::other("scanner test observation limit reached"));
    }
    let pid = std::process::id();
    let event = json!({
        "nonce": session.nonce,
        "pid": pid,
        "sequence": sequence,
        "stage": stage,
        "armed": armed,
        "detail": detail,
    });
    let encoded = serde_json::to_vec(&event).map_err(io::Error::other)?;
    if encoded.len() > MAX_BYTES as usize {
        return Err(io::Error::other("scanner test observation exceeds its byte bound"));
    }
    let suffix = format!("{}.{}.{}.json", session.nonce, pid, sequence);
    write_observation(directory, &format!("observed.{suffix}"), &encoded)?;
    if !armed {
        if stage == "before_start_snapshot" {
            worker_observation::boundary(stage, &event["detail"]);
        }
        return Ok(());
    }
    write_observation(directory, &format!("reached.{suffix}"), &encoded)?;
    let release = directory.join(format!("release.{}.{}.{}", session.nonce, stage, pid));
    let started = Instant::now();
    while !release.try_exists()? {
        if started.elapsed() >= MAX_WAIT {
            return Err(io::Error::new(io::ErrorKind::TimedOut, "scanner test barrier release timed out"));
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    if stage == "before_start_snapshot" {
        worker_observation::boundary(stage, &event["detail"]);
    }
    Ok(())
}

pub(crate) fn hex(bytes: &[u8]) -> String {
    hex_simd::encode_to_string(bytes, hex_simd::AsciiCase::Lower)
}

pub(crate) fn acknowledgement_observations(acknowledgements: &[crate::scanner::ScannerDirtyUsageAcknowledgement]) -> Value {
    Value::Array(
        acknowledgements
            .iter()
            .map(|acknowledgement| {
                use crate::scanner::ScannerDirtyUsageAcknowledgementKind;
                match &acknowledgement.kind {
                    ScannerDirtyUsageAcknowledgementKind::Generation(generation) => json!({
                        "host": acknowledgement.host,
                        "instance_id": acknowledgement.instance_id,
                        "kind": "generation",
                        "generation": generation,
                    }),
                    ScannerDirtyUsageAcknowledgementKind::Scoped { owner_id, entries } => json!({
                        "host": acknowledgement.host,
                        "owner_id": owner_id,
                        "instance_id": acknowledgement.instance_id,
                        "kind": "scoped",
                        "entries": entries.iter().map(|entry| json!({
                            "bucket": entry.bucket,
                            "bucket_incarnation": entry.bucket_incarnation,
                            "generation": entry.generation,
                        })).collect::<Vec<_>>(),
                    }),
                }
            })
            .collect(),
    )
}

pub(crate) fn activity_observations(activity: &crate::scanner::ScannerActivitySnapshot) -> Value {
    Value::Object(
        activity
            .iter()
            .map(|(host, activity)| (host.clone(), activity.e2e_observation()))
            .collect(),
    )
}
