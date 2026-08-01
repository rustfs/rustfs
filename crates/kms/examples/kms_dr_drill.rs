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

//! Operator entry point for the KMS disaster-recovery drill.
//!
//! Runs one rehearsal inside a scratch workspace and writes the evidence
//! bundle. Everything it touches lives under that workspace: it never reads,
//! writes, or restores into a running deployment's key directory. See
//! `docs/operations/kms-disaster-recovery-drill.md` for the procedure and for
//! how to size the dataset so the measured recovery time transfers.
//!
//! Exit status is the verdict: 0 when every check held, 1 otherwise, so a
//! scheduled drill fails its job instead of quietly filing a bad report.

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use rustfs_kms::backup::{BackupKek, DrillDataset, DrillDisaster, DrillRequest, DrillVerdict, run_local_drill};
use std::path::PathBuf;
use std::process::ExitCode;
use zeroize::Zeroizing;

const ENV_WORKSPACE: &str = "RUSTFS_KMS_DRILL_WORKSPACE";
const ENV_DRILL_ID: &str = "RUSTFS_KMS_DRILL_ID";
const ENV_DISASTER: &str = "RUSTFS_KMS_DRILL_DISASTER";
const ENV_DEPLOYMENT: &str = "RUSTFS_KMS_DRILL_DEPLOYMENT";
const ENV_MASTER_KEY: &str = "RUSTFS_KMS_DRILL_MASTER_KEY";
const ENV_KEYS: &str = "RUSTFS_KMS_DRILL_KEYS";
const ENV_OBJECTS_PER_KEY: &str = "RUSTFS_KMS_DRILL_OBJECTS_PER_KEY";
const ENV_OBJECT_BYTES: &str = "RUSTFS_KMS_DRILL_OBJECT_BYTES";
const ENV_EVIDENCE: &str = "RUSTFS_KMS_DRILL_EVIDENCE";
const ENV_FILE_PERMISSIONS: &str = "RUSTFS_KMS_DRILL_FILE_PERMISSIONS";

// Deliberately the same names the admin backup API reads: drilling with the
// KEK the real backups are sealed under is what proves that KEK is still
// retrievable, which is the part of a recovery no bundle can attest to.
const ENV_KEK: &str = "RUSTFS_KMS_BACKUP_KEK";
const ENV_KEK_ID: &str = "RUSTFS_KMS_BACKUP_KEK_ID";
const ENV_KEK_VERSION: &str = "RUSTFS_KMS_BACKUP_KEK_VERSION";

fn usage() -> String {
    format!(
        "KMS disaster-recovery drill.\n\
         \n\
         Required:\n  \
           {ENV_WORKSPACE}      absolute path to a scratch directory the drill owns\n  \
           {ENV_MASTER_KEY}     at-rest master key for the throwaway rehearsal deployment\n  \
           {ENV_KEK}            base64 of the 32-byte backup KEK\n  \
           {ENV_KEK_ID}         identifier recorded in the bundle manifest\n\
         \n\
         Optional:\n  \
           {ENV_KEK_VERSION}    backup KEK version (default 1)\n  \
           {ENV_DRILL_ID}       drill identifier and key-id prefix (default kms-dr-drill)\n  \
           {ENV_DEPLOYMENT}     deployment identity recorded in the bundle (default kms-dr-drill)\n  \
           {ENV_DISASTER}       key-directory-lost | master-key-salt-lost | key-record-corrupted\n  \
           {ENV_KEYS}           master keys to rehearse with (default 4)\n  \
           {ENV_OBJECTS_PER_KEY} objects sealed per key (default 2)\n  \
           {ENV_OBJECT_BYTES}   plaintext bytes per object (default 4096)\n  \
           {ENV_FILE_PERMISSIONS} octal key-file mode (default 600)\n  \
           {ENV_EVIDENCE}       evidence output path (default <workspace>/evidence.json)"
    )
}

fn required(name: &str) -> Result<String, String> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| format!("{name} is required\n\n{}", usage()))
}

fn optional_usize(name: &str, default: usize) -> Result<usize, String> {
    match std::env::var(name) {
        Ok(value) => value
            .trim()
            .parse()
            .map_err(|_| format!("{name} must be an unsigned integer")),
        Err(_) => Ok(default),
    }
}

fn disaster_from_env() -> Result<DrillDisaster, String> {
    match std::env::var(ENV_DISASTER).ok().as_deref().map(str::trim) {
        None | Some("") | Some("key-directory-lost") => Ok(DrillDisaster::KeyDirectoryLost),
        Some("master-key-salt-lost") => Ok(DrillDisaster::MasterKeySaltLost),
        Some("key-record-corrupted") => Ok(DrillDisaster::KeyRecordCorrupted),
        Some(other) => Err(format!(
            "{ENV_DISASTER}={other:?} is not a known disaster; use key-directory-lost, master-key-salt-lost, or key-record-corrupted"
        )),
    }
}

fn kek_from_env() -> Result<BackupKek, String> {
    let raw = Zeroizing::new(required(ENV_KEK)?);
    let decoded = Zeroizing::new(BASE64.decode(raw.trim()).map_err(|_| format!("{ENV_KEK} must be base64"))?);
    if decoded.len() != 32 {
        return Err(format!("{ENV_KEK} must decode to exactly 32 bytes"));
    }
    let mut material = [0u8; 32];
    material.copy_from_slice(&decoded);
    let version = optional_usize(ENV_KEK_VERSION, 1)?;
    let version = u32::try_from(version).map_err(|_| format!("{ENV_KEK_VERSION} is out of range"))?;
    BackupKek::new(required(ENV_KEK_ID)?, version, material).map_err(|error| error.to_string())
}

async fn run() -> Result<DrillVerdict, String> {
    let workspace = PathBuf::from(required(ENV_WORKSPACE)?);
    if !workspace.is_absolute() {
        return Err(format!("{ENV_WORKSPACE} must be an absolute path"));
    }
    let drill_id = std::env::var(ENV_DRILL_ID)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "kms-dr-drill".to_string());
    let file_permissions = match std::env::var(ENV_FILE_PERMISSIONS) {
        Ok(value) => Some(u32::from_str_radix(value.trim(), 8).map_err(|_| format!("{ENV_FILE_PERMISSIONS} must be octal"))?),
        Err(_) => Some(0o600),
    };

    let request = DrillRequest {
        deployment_identity: std::env::var(ENV_DEPLOYMENT)
            .ok()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| drill_id.clone()),
        drill_id,
        workspace: workspace.clone(),
        rustfs_version: env!("CARGO_PKG_VERSION").to_string(),
        // A drill always restores into a target that has observed nothing, so
        // any generation is monotonic; the number is recorded as evidence of
        // which snapshot the rehearsal covered.
        snapshot_generation: 1,
        dataset: DrillDataset {
            keys: optional_usize(ENV_KEYS, 4)?,
            objects_per_key: optional_usize(ENV_OBJECTS_PER_KEY, 2)?,
            object_bytes: optional_usize(ENV_OBJECT_BYTES, 4096)?,
        },
        disaster: disaster_from_env()?,
        master_key: required(ENV_MASTER_KEY)?,
        file_permissions,
    };

    let kek = kek_from_env()?;
    let evidence = run_local_drill(&kek, &request).await.map_err(|error| error.to_string())?;

    let evidence_path = std::env::var(ENV_EVIDENCE)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .map_or_else(|| workspace.join("evidence.json"), PathBuf::from);
    let encoded = evidence.encode().map_err(|error| error.to_string())?;
    std::fs::write(&evidence_path, &encoded).map_err(|error| format!("cannot write {}: {error}", evidence_path.display()))?;

    eprintln!("verdict:  {:?}", evidence.verdict);
    eprintln!("disaster: {:?}", evidence.disaster);
    eprintln!(
        "objects:  {}/{} historical objects decrypted after the restore",
        evidence.envelope_probes.iter().filter(|probe| probe.verified).count(),
        evidence.envelope_probes.len()
    );
    eprintln!("rpo:      {} ms of work past the snapshot fence was lost", evidence.rpo.rpo_window_millis);
    eprintln!("rto:      {} ms of recovery work", evidence.rto_millis);
    for finding in &evidence.findings {
        eprintln!("finding:  {finding}");
    }
    eprintln!("evidence: {}", evidence_path.display());

    Ok(evidence.verdict)
}

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(DrillVerdict::Passed) => ExitCode::SUCCESS,
        Ok(DrillVerdict::Failed) => ExitCode::FAILURE,
        Err(message) => {
            eprintln!("{message}");
            ExitCode::FAILURE
        }
    }
}
