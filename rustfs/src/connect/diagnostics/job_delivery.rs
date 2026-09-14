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

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read as _, Write as _};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[cfg(unix)]
use std::os::unix::fs::{MetadataExt as _, OpenOptionsExt as _, PermissionsExt as _};

use chrono::Utc;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{
    DiagnosticJobEnvelope, DiagnosticJobExecution, DiagnosticJobTarget, ProfileProvenance, TrustedDiagnosticJobSigner,
    execute_diagnostic_job,
};
use crate::connect::config::HeartbeatConfig;
use crate::connect::report_upload::ReportUploadClient;
use crate::connect::telemetry::{TelemetryDelivery, TelemetryTransport};

const PROTOCOL_VERSION: &str = "v1";
const MAX_EXECUTABLE_BYTES: u64 = 2_147_483_648;
const DELIVERY_ATTEMPTS: u8 = 3;
const MAX_STATE_BYTES: u64 = 4 * 1024 * 1024;
const MAX_ARTIFACT_BYTES: usize = 524_288;
const MAX_STATE_FILES: usize = 65_536;

#[derive(Clone)]
pub(crate) struct DiagnosticJobRuntime {
    config: HeartbeatConfig,
    signer: TrustedDiagnosticJobSigner,
    states: Arc<Mutex<BTreeMap<String, JobState>>>,
    store: JobStateStore,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields, tag = "state", content = "result", rename_all = "SCREAMING_SNAKE_CASE")]
enum JobState {
    Active,
    Completed(DiagnosticJobExecution),
    Uploaded(UploadedDiagnosticJobResult),
    Delivered,
}

#[derive(Clone)]
struct JobStateStore {
    directory: PathBuf,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct UploadedDiagnosticJobResult {
    job_id: String,
    outcome: String,
    reason: String,
    artifact_name: Option<String>,
    artifact_sha256: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct DiagnosticJobResultRequest<'a> {
    protocol_version: &'static str,
    request_id: &'a str,
    job_id: &'a str,
    outcome: &'a str,
    reason: &'a str,
    artifact_name: Option<&'a str>,
    artifact_sha256: Option<&'a str>,
}

impl DiagnosticJobRuntime {
    pub(crate) fn from_config(config: &HeartbeatConfig) -> Option<Self> {
        let state_root = config.state_root()?.to_path_buf();
        Some(Self {
            config: config.clone(),
            signer: config.diagnostic_job_signer.clone()?,
            states: Arc::new(Mutex::new(BTreeMap::new())),
            store: JobStateStore {
                directory: state_root.join("diagnostic-jobs"),
            },
        })
    }

    pub(crate) fn offer(&self, envelope: DiagnosticJobEnvelope, shutdown: &CancellationToken) {
        let Ok((target, identity)) = target_and_identity(&self.config) else {
            return;
        };
        let Ok(job) = self.signer.verify(&envelope, &target, Utc::now()) else {
            return;
        };
        let job_id = job.job_id().to_owned();
        let cached = {
            let Ok(mut states) = self.states.lock() else {
                return;
            };
            match states.get(&job_id) {
                Some(JobState::Active) => return,
                Some(JobState::Completed(result)) => Some(result.clone()),
                Some(JobState::Uploaded(result)) => {
                    let result = result.clone();
                    drop(states);
                    let runtime = self.clone();
                    let cancel = shutdown.child_token();
                    tokio::spawn(async move {
                        runtime.deliver_uploaded(result, &cancel).await;
                    });
                    return;
                }
                Some(JobState::Delivered) => return,
                None => match self.store.load(&job_id) {
                    Ok(Some(JobState::Completed(result))) => {
                        if !valid_execution(&result, &job_id) {
                            return;
                        }
                        states.insert(job_id.clone(), JobState::Completed(result.clone()));
                        Some(result)
                    }
                    Ok(Some(JobState::Delivered)) => {
                        states.insert(job_id, JobState::Delivered);
                        return;
                    }
                    Ok(Some(JobState::Uploaded(result))) => {
                        if !valid_uploaded_result(&result, &job_id) {
                            return;
                        }
                        states.insert(job_id.clone(), JobState::Uploaded(result.clone()));
                        drop(states);
                        let runtime = self.clone();
                        let cancel = shutdown.child_token();
                        tokio::spawn(async move {
                            runtime.deliver_uploaded(result, &cancel).await;
                        });
                        return;
                    }
                    Ok(Some(JobState::Active)) => {
                        let result = failed_execution(&job_id, "INTERRUPTED");
                        if self.store.save(&job_id, &JobState::Completed(result.clone())).is_err() {
                            return;
                        }
                        states.insert(job_id.clone(), JobState::Completed(result.clone()));
                        Some(result)
                    }
                    Ok(None) => {
                        if self.store.save(&job_id, &JobState::Active).is_err() {
                            return;
                        }
                        states.insert(job_id.clone(), JobState::Active);
                        None
                    }
                    Err(()) => return,
                },
            }
        };
        let runtime = self.clone();
        let cancel = shutdown.child_token();
        tokio::spawn(async move {
            let result = match cached {
                Some(result) => result,
                None => {
                    let result = match executable_provenance().await {
                        Ok(provenance) => execute_diagnostic_job(job, &identity, provenance, &cancel)
                            .await
                            .unwrap_or_else(|error| failed_execution(&job_id, error.reason())),
                        Err(reason) => failed_execution(&job_id, reason),
                    };
                    let terminal = JobState::Completed(result.clone());
                    if runtime.store.save(&job_id, &terminal).is_ok()
                        && let Ok(mut states) = runtime.states.lock()
                    {
                        states.insert(job_id.clone(), terminal);
                    }
                    result
                }
            };
            if let Some(uploaded) = prepare_result(&runtime, result, &cancel).await {
                runtime.deliver_uploaded(uploaded, &cancel).await;
            }
        });
    }

    async fn deliver_uploaded(&self, result: UploadedDiagnosticJobResult, cancel: &CancellationToken) {
        let job_id = result.job_id.clone();
        if deliver_result(&self.config, &result, cancel).await
            && self.store.save(&job_id, &JobState::Delivered).is_ok()
            && let Ok(mut states) = self.states.lock()
        {
            states.insert(job_id, JobState::Delivered);
        }
    }
}

impl JobStateStore {
    fn path(&self, job_id: &str) -> PathBuf {
        self.directory.join(format!("{job_id}.json"))
    }

    fn load(&self, job_id: &str) -> Result<Option<JobState>, ()> {
        let path = self.path(job_id);
        let initial = match fs::symlink_metadata(&path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(_) => return Err(()),
        };
        if !initial.file_type().is_file() || initial.len() == 0 || initial.len() > MAX_STATE_BYTES {
            return Err(());
        }
        let mut options = OpenOptions::new();
        options.read(true);
        #[cfg(unix)]
        options.custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
        let file = options.open(path).map_err(|_| ())?;
        let opened = file.metadata().map_err(|_| ())?;
        if !secure_state_file(&initial, &opened) {
            return Err(());
        }
        let mut bytes = Vec::with_capacity(usize::try_from(initial.len()).map_err(|_| ())?);
        file.take(MAX_STATE_BYTES + 1).read_to_end(&mut bytes).map_err(|_| ())?;
        if u64::try_from(bytes.len()).map_err(|_| ())? > MAX_STATE_BYTES {
            return Err(());
        }
        serde_json::from_slice(&bytes).map(Some).map_err(|_| ())
    }

    fn save(&self, job_id: &str, state: &JobState) -> Result<(), ()> {
        let bytes = serde_json::to_vec(state).map_err(|_| ())?;
        if bytes.is_empty() || u64::try_from(bytes.len()).map_err(|_| ())? > MAX_STATE_BYTES {
            return Err(());
        }
        self.ensure_directory()?;
        let destination = self.path(job_id);
        if !destination.exists() && self.state_file_count()? >= MAX_STATE_FILES {
            return Err(());
        }
        let temporary = self.directory.join(format!(".{job_id}.{}.tmp", Uuid::new_v4()));
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt as _;
            options.mode(0o600);
        }
        let mut file = options.open(&temporary).map_err(|_| ())?;
        let result = file
            .write_all(&bytes)
            .and_then(|()| file.sync_all())
            .and_then(|()| fs::rename(&temporary, &destination))
            .and_then(|()| sync_directory(&self.directory));
        if result.is_err() {
            let _ = fs::remove_file(temporary);
        }
        result.map_err(|_| ())
    }

    fn ensure_directory(&self) -> Result<(), ()> {
        fs::create_dir_all(&self.directory).map_err(|_| ())?;
        #[cfg(unix)]
        {
            let metadata = fs::symlink_metadata(&self.directory).map_err(|_| ())?;
            if !metadata.file_type().is_dir() || metadata.uid() != rustix::process::geteuid().as_raw() {
                return Err(());
            }
            fs::set_permissions(&self.directory, fs::Permissions::from_mode(0o700)).map_err(|_| ())?;
        }
        Ok(())
    }

    fn state_file_count(&self) -> Result<usize, ()> {
        let mut count = 0_usize;
        for entry in fs::read_dir(&self.directory).map_err(|_| ())? {
            let entry = entry.map_err(|_| ())?;
            if entry.file_name().to_string_lossy().ends_with(".json") {
                count = count.checked_add(1).ok_or(())?;
                if count >= MAX_STATE_FILES {
                    break;
                }
            }
        }
        Ok(count)
    }
}

#[cfg(unix)]
fn secure_state_file(initial: &fs::Metadata, opened: &fs::Metadata) -> bool {
    opened.is_file()
        && opened.uid() == rustix::process::geteuid().as_raw()
        && opened.permissions().mode() & 0o077 == 0
        && opened.dev() == initial.dev()
        && opened.ino() == initial.ino()
}

#[cfg(not(unix))]
fn secure_state_file(_initial: &fs::Metadata, _opened: &fs::Metadata) -> bool {
    false
}

fn sync_directory(path: &Path) -> std::io::Result<()> {
    File::open(path)?.sync_all()
}

fn target_and_identity(config: &HeartbeatConfig) -> Result<(DiagnosticJobTarget, crate::connect::DeviceIdentity), ()> {
    let credential = config.credential_store.load().map_err(|_| ())?.ok_or(())?;
    let parts: Vec<_> = credential.name.split('/').collect();
    if parts.len() != 6
        || parts[0] != "organizations"
        || parts[1].is_empty()
        || parts[2] != "clusters"
        || parts[3].is_empty()
        || parts[4] != "clusterDevices"
        || parts[5] != credential.uid
    {
        return Err(());
    }
    let identity = config.identity_store.load().map_err(|_| ())?.ok_or(())?;
    Ok((
        DiagnosticJobTarget {
            organization_name: format!("organizations/{}", parts[1]),
            cluster_name: format!("organizations/{}/clusters/{}", parts[1], parts[3]),
            device_name: credential.name,
        },
        identity,
    ))
}

async fn executable_provenance() -> Result<ProfileProvenance, &'static str> {
    let digest = tokio::task::spawn_blocking(hash_current_executable)
        .await
        .map_err(|_| "PROVENANCE_FAILED")?
        .map_err(|_| "PROVENANCE_FAILED")?;
    Ok(ProfileProvenance::new(
        crate::version::build::COMMIT_HASH,
        digest,
        env!("CARGO_PKG_VERSION"),
        enabled_build_features(),
    ))
}

fn hash_current_executable() -> Result<String, ()> {
    let path = std::env::current_exe().map_err(|_| ())?;
    let mut file = File::open(path).map_err(|_| ())?;
    let metadata = file.metadata().map_err(|_| ())?;
    if !metadata.is_file() || metadata.len() == 0 || metadata.len() > MAX_EXECUTABLE_BYTES {
        return Err(());
    }
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    let mut total = 0_u64;
    loop {
        let read = file.read(&mut buffer).map_err(|_| ())?;
        if read == 0 {
            break;
        }
        total = total.checked_add(u64::try_from(read).map_err(|_| ())?).ok_or(())?;
        if total > MAX_EXECUTABLE_BYTES {
            return Err(());
        }
        hasher.update(&buffer[..read]);
    }
    if total != metadata.len() {
        return Err(());
    }
    Ok(hex_simd::encode_to_string(hasher.finalize(), hex_simd::AsciiCase::Lower))
}

fn enabled_build_features() -> Vec<String> {
    let mut features = Vec::new();
    for (enabled, name) in [
        (cfg!(feature = "connect-e2e-short-credentials"), "connect-e2e-short-credentials"),
        (cfg!(feature = "dial9"), "dial9"),
        (cfg!(feature = "e2e-test-hooks"), "e2e-test-hooks"),
        (cfg!(feature = "ftps"), "ftps"),
        (cfg!(feature = "full"), "full"),
        (cfg!(feature = "gcs"), "gcs"),
        (cfg!(feature = "hotpath"), "hotpath"),
        (cfg!(feature = "hotpath-alloc"), "hotpath-alloc"),
        (cfg!(feature = "hotpath-cpu"), "hotpath-cpu"),
        (cfg!(feature = "io-scheduler-debug"), "io-scheduler-debug"),
        (cfg!(feature = "license"), "license"),
        (cfg!(feature = "metrics-gpu"), "metrics-gpu"),
        (cfg!(feature = "offline-enrollment-e2e-root"), "offline-enrollment-e2e-root"),
        (cfg!(feature = "pyroscope"), "pyroscope"),
        (cfg!(feature = "rio-v2"), "rio-v2"),
        (cfg!(feature = "sftp"), "sftp"),
        (cfg!(feature = "swift"), "swift"),
        (cfg!(feature = "tracing-chunk-debug"), "tracing-chunk-debug"),
        (cfg!(feature = "webdav"), "webdav"),
    ] {
        if enabled {
            features.push(name.to_owned());
        }
    }
    features
}

fn failed_execution(job_id: &str, reason: &'static str) -> DiagnosticJobExecution {
    let reason = if reason == "CANCELLED" {
        "CANCELLED"
    } else if reason == "LIMIT_EXCEEDED" {
        "LIMIT_EXCEEDED"
    } else {
        "COLLECTION_FAILED"
    };
    DiagnosticJobExecution {
        job_id: job_id.to_owned(),
        outcome: if reason == "CANCELLED" { "CANCELLED" } else { "FAILED" }.to_owned(),
        reason: reason.to_owned(),
        artifact_uid: None,
        artifact_sha256: None,
        artifact_bytes: None,
    }
}

async fn prepare_result(
    runtime: &DiagnosticJobRuntime,
    result: DiagnosticJobExecution,
    cancel: &CancellationToken,
) -> Option<UploadedDiagnosticJobResult> {
    let uploaded = if let (Some(bytes), true) =
        (result.artifact_bytes.as_ref(), matches!(result.outcome.as_str(), "SUCCEEDED" | "PARTIAL"))
    {
        if runtime.store.ensure_directory().is_err() {
            return None;
        }
        let path = runtime
            .store
            .directory
            .join(format!(".{}.{}.artifact", result.job_id, Uuid::new_v4()));
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        options.mode(0o600);
        let mut file = options.open(&path).ok()?;
        if file.write_all(bytes).and_then(|()| file.sync_all()).is_err() {
            let _ = fs::remove_file(&path);
            return None;
        }
        drop(file);
        let receipt = match ReportUploadClient::new(runtime.config.clone(), Duration::from_secs(15 * 60)) {
            Ok(client) => client.upload(&path, cancel).await.ok(),
            Err(_) => None,
        };
        let _ = fs::remove_file(path);
        let receipt = receipt?;
        UploadedDiagnosticJobResult {
            job_id: result.job_id,
            outcome: result.outcome,
            reason: result.reason,
            artifact_name: Some(receipt.name),
            artifact_sha256: Some(receipt.declared_sha256),
        }
    } else {
        UploadedDiagnosticJobResult {
            job_id: result.job_id,
            outcome: result.outcome,
            reason: result.reason,
            artifact_name: None,
            artifact_sha256: None,
        }
    };
    let state = JobState::Uploaded(uploaded.clone());
    if runtime.store.save(&uploaded.job_id, &state).is_err() {
        return None;
    }
    if let Ok(mut states) = runtime.states.lock() {
        states.insert(uploaded.job_id.clone(), state);
    }
    Some(uploaded)
}

async fn deliver_result(config: &HeartbeatConfig, result: &UploadedDiagnosticJobResult, cancel: &CancellationToken) -> bool {
    let Ok(transport) = TelemetryTransport::new(config.clone()) else {
        return false;
    };
    let request_id = Uuid::new_v4().to_string();
    let request = DiagnosticJobResultRequest {
        protocol_version: PROTOCOL_VERSION,
        request_id: &request_id,
        job_id: &result.job_id,
        outcome: &result.outcome,
        reason: &result.reason,
        artifact_name: result.artifact_name.as_deref(),
        artifact_sha256: result.artifact_sha256.as_deref(),
    };
    for attempt in 0..DELIVERY_ATTEMPTS {
        if cancel.is_cancelled() {
            return false;
        }
        if matches!(
            transport.post("diagnosticJobResults", &request).await,
            Ok(TelemetryDelivery::Accepted { .. })
        ) {
            return true;
        }
        if attempt + 1 < DELIVERY_ATTEMPTS {
            tokio::select! {
                () = cancel.cancelled() => return false,
                () = tokio::time::sleep(Duration::from_secs(1_u64 << attempt)) => {}
            }
        }
    }
    false
}

fn valid_uploaded_result(result: &UploadedDiagnosticJobResult, job_id: &str) -> bool {
    result.job_id == job_id
        && valid_terminal_fields(&result.outcome, &result.reason)
        && match (&result.artifact_name, &result.artifact_sha256) {
            (Some(name), Some(digest)) => {
                matches!(result.outcome.as_str(), "SUCCEEDED" | "PARTIAL")
                    && name.len() <= 512
                    && name.starts_with("organizations/")
                    && lower_hex(digest, 64)
            }
            (None, None) => matches!(result.outcome.as_str(), "FAILED" | "UNSUPPORTED" | "CANCELLED"),
            _ => false,
        }
}

fn valid_terminal_fields(outcome: &str, reason: &str) -> bool {
    matches!(outcome, "SUCCEEDED" | "PARTIAL" | "FAILED" | "UNSUPPORTED" | "CANCELLED")
        && !reason.is_empty()
        && reason.len() <= 64
        && reason
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
}

fn lower_hex(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn valid_execution(result: &DiagnosticJobExecution, job_id: &str) -> bool {
    let valid_artifact = match (&result.artifact_uid, &result.artifact_sha256, &result.artifact_bytes) {
        (Some(uid), Some(digest), Some(bytes)) => {
            Uuid::parse_str(uid).is_ok_and(|value| value.get_version_num() == 7 && value.to_string() == *uid)
                && !bytes.is_empty()
                && bytes.len() <= MAX_ARTIFACT_BYTES
                && hex_simd::encode_to_string(Sha256::digest(bytes), hex_simd::AsciiCase::Lower) == *digest
        }
        (None, None, None) => matches!(result.outcome.as_str(), "FAILED" | "CANCELLED") && result.reason != "COMPLETE",
        _ => false,
    };
    result.job_id == job_id && valid_terminal_fields(&result.outcome, &result.reason) && valid_artifact
}

#[cfg(test)]
mod tests {
    use super::*;

    fn execution(job_id: &str, artifact: &[u8]) -> DiagnosticJobExecution {
        DiagnosticJobExecution {
            job_id: job_id.to_owned(),
            outcome: "SUCCEEDED".to_owned(),
            reason: "COMPLETE".to_owned(),
            artifact_uid: Some(Uuid::now_v7().to_string()),
            artifact_sha256: Some(hex_simd::encode_to_string(Sha256::digest(artifact), hex_simd::AsciiCase::Lower)),
            artifact_bytes: Some(artifact.to_vec()),
        }
    }

    #[test]
    fn result_request_has_no_generic_execution_fields() {
        let request = DiagnosticJobResultRequest {
            protocol_version: "v1",
            request_id: "123e4567-e89b-42d3-a456-426614174001",
            job_id: "018cc251-f400-7abc-8def-0123456789ab",
            outcome: "FAILED",
            reason: "CANCELLED",
            artifact_name: None,
            artifact_sha256: None,
        };
        let value = serde_json::to_value(request).expect("result request");
        assert_eq!(value["jobId"], "018cc251-f400-7abc-8def-0123456789ab");
        for forbidden in [
            "command",
            "script",
            "path",
            "sql",
            "arguments",
            "artifact",
            "artifactUid",
            "artifactEncoding",
        ] {
            assert!(value.get(forbidden).is_none());
        }
    }

    #[test]
    fn durable_state_preserves_single_execution_terminal_result() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let store = JobStateStore {
            directory: temporary.path().join("diagnostic-jobs"),
        };
        let job_id = Uuid::now_v7().to_string();
        store.save(&job_id, &JobState::Active).expect("active state");
        assert!(matches!(store.load(&job_id), Ok(Some(JobState::Active))));

        let expected = execution(&job_id, b"bounded artifact");
        store
            .save(&job_id, &JobState::Completed(expected.clone()))
            .expect("completed state");
        let loaded = match store.load(&job_id).expect("load state") {
            Some(JobState::Completed(result)) => result,
            _ => panic!("completed state expected"),
        };
        assert_eq!(loaded, expected);
        assert!(valid_execution(&loaded, &job_id));

        let uploaded = UploadedDiagnosticJobResult {
            job_id: job_id.clone(),
            outcome: "SUCCEEDED".to_owned(),
            reason: "COMPLETE".to_owned(),
            artifact_name: Some(format!(
                "organizations/{}/clusters/{}/supportBundles/{}",
                Uuid::now_v7(),
                Uuid::now_v7(),
                Uuid::now_v7()
            )),
            artifact_sha256: Some("a".repeat(64)),
        };
        store.save(&job_id, &JobState::Uploaded(uploaded)).expect("uploaded state");
        let loaded = match store.load(&job_id).expect("load state") {
            Some(JobState::Uploaded(result)) => result,
            _ => panic!("uploaded state expected"),
        };
        assert!(valid_uploaded_result(&loaded, &job_id));

        store.save(&job_id, &JobState::Delivered).expect("delivered state");
        assert!(matches!(store.load(&job_id), Ok(Some(JobState::Delivered))));
    }

    #[test]
    fn persisted_result_must_match_job_and_artifact_digest() {
        let job_id = Uuid::now_v7().to_string();
        let mut result = execution(&job_id, b"artifact");
        assert!(valid_execution(&result, &job_id));
        assert!(!valid_execution(&result, &Uuid::now_v7().to_string()));

        result.artifact_sha256 = Some("0".repeat(64));
        assert!(!valid_execution(&result, &job_id));
        result.artifact_bytes = Some(vec![0; MAX_ARTIFACT_BYTES + 1]);
        assert!(!valid_execution(&result, &job_id));
    }

    #[cfg(unix)]
    #[test]
    fn state_loader_rejects_group_readable_files() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let store = JobStateStore {
            directory: temporary.path().join("diagnostic-jobs"),
        };
        let job_id = Uuid::now_v7().to_string();
        store.save(&job_id, &JobState::Active).expect("active state");
        fs::set_permissions(store.path(&job_id), fs::Permissions::from_mode(0o640)).expect("permissions");
        assert!(store.load(&job_id).is_err());
    }
}
