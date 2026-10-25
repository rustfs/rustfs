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

//! Authenticated, decoded RPC entry barriers for the distributed MPU tests.
//! No environment variable means no filesystem access. This module is absent
//! from production builds; it never fabricates an RPC or storage error.

use serde::{Deserialize, Serialize};
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::time::Instant;
use uuid::Uuid;

const DIRECTORY_ENV: &str = "RUSTFS_E2E_RPC_DELAY_DIR";
const MAX_ARM_BYTES: u64 = 4096;
const HOLD_LIMIT: Duration = Duration::from_secs(120);
const POLL: Duration = Duration::from_millis(10);

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum Method {
    ReadVersion,
    WriteMetadata,
    RenamePart,
    RenameData,
    RenameMultipartData,
    RecoverMultipartSource,
    Delete,
    Lock,
    LockBatch,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "class", rename_all = "snake_case", deny_unknown_fields)]
enum Scope {
    Multipart { sha256: String },
    Object { bucket: String, key: String },
    Incarnation { bucket: String },
    Lifecycle { bucket: String },
}

#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum Delay {
    Delay { milliseconds: u64 },
    Hold {},
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Arm {
    nonce: Uuid,
    methods: Vec<Method>,
    scope: Scope,
    delay: Delay,
}

fn user_bucket(bucket: &str) -> bool {
    !bucket.is_empty() && bucket.len() <= 63 && !bucket.starts_with('.') && !bucket.contains('/')
}

impl Arm {
    fn validate(&self) -> bool {
        !self.nonce.is_nil()
            && !self.methods.is_empty()
            && self.methods.len() <= 2
            && !matches!(self.delay, Delay::Delay { milliseconds } if milliseconds == 0 || milliseconds > 2000)
            && match &self.scope {
                Scope::Multipart { sha256 } => {
                    sha256.len() == 64
                        && sha256
                            .bytes()
                            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
                        && self.methods.iter().all(|method| {
                            matches!(
                                method,
                                Method::WriteMetadata | Method::RenamePart | Method::Delete | Method::RecoverMultipartSource
                            )
                        })
                }
                Scope::Object { bucket, key } => {
                    user_bucket(bucket)
                        && !key.is_empty()
                        && key.len() <= 1024
                        && self
                            .methods
                            .iter()
                            .all(|method| matches!(method, Method::RenameData | Method::RenameMultipartData))
                }
                Scope::Incarnation { bucket } => {
                    user_bucket(bucket) && self.methods.iter().all(|method| *method == Method::ReadVersion)
                }
                Scope::Lifecycle { bucket } => {
                    user_bucket(bucket)
                        && self
                            .methods
                            .iter()
                            .all(|method| matches!(method, Method::Lock | Method::LockBatch))
                }
            }
    }

    fn matches(&self, method: Method, volume: &str, path: &str) -> bool {
        self.methods.contains(&method)
            && match &self.scope {
                Scope::Multipart { sha256 } => {
                    volume == ".rustfs.sys/multipart" && path.strip_prefix(sha256).is_some_and(|tail| tail.starts_with('/'))
                }
                Scope::Object { bucket, key } => volume == bucket && path == key,
                Scope::Incarnation { bucket } => {
                    volume == ".rustfs.sys" && path == format!("buckets/{bucket}/.bucket-incarnation")
                }
                Scope::Lifecycle { bucket } => volume == bucket && path.is_empty(),
            }
    }
}

pub(super) fn request_id<T>(request: &tonic::Request<T>) -> Option<String> {
    request
        .metadata()
        .get("x-request-id")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
}

async fn write_observation(path: &Path, value: &serde_json::Value) -> std::io::Result<()> {
    let body = serde_json::to_vec(value)?;
    let temporary = path.with_extension(format!("{}.tmp", Uuid::new_v4()));
    tokio::fs::write(&temporary, body).await?;
    tokio::fs::rename(temporary, path).await
}

async fn hook_error(directory: &Path, reason: &'static str) {
    let _ = write_observation(
        &directory.join(format!("hook_error.{}.json", Uuid::new_v4())),
        &serde_json::json!({ "reason": reason, "pid": std::process::id() }),
    )
    .await;
}

struct ExitObservation {
    path: Option<PathBuf>,
    value: serde_json::Value,
    started: Instant,
}

impl ExitObservation {
    async fn finish(self, state: &'static str) {
        self.finish_with(state, |path, value| async move { write_observation(&path, &value).await })
            .await;
    }

    async fn finish_with<F, W>(mut self, state: &'static str, write: F)
    where
        F: FnOnce(PathBuf, serde_json::Value) -> W,
        W: std::future::Future<Output = std::io::Result<()>>,
    {
        let Some(path) = self.path.clone() else { return };
        self.value["state"] = serde_json::json!(state);
        self.value["elapsed_ms"] = serde_json::json!(self.started.elapsed().as_millis());
        // Keep the locator armed throughout the write. Cancellation at either
        // filesystem await must still let Drop publish its observation.
        if write(path.clone(), self.value.clone()).await.is_err()
            && let Some(directory) = path.parent()
        {
            hook_error(directory, "exited_write").await;
        }
        self.path = None;
    }
}

impl Drop for ExitObservation {
    fn drop(&mut self) {
        let Some(path) = self.path.take() else { return };
        let mut value = self.value.clone();
        value["state"] = serde_json::json!("handler_future_dropped");
        value["elapsed_ms"] = serde_json::json!(self.started.elapsed().as_millis());
        // A cancelled filesystem future can leave an already-started rename
        // running. Keep the actual drop separate from its normal exit record
        // so that late completion cannot overwrite cancellation evidence.
        // This says nothing about remote lock release.
        let path = path.with_extension("dropped.json");
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                let _ = tokio::time::timeout(Duration::from_secs(5), write_observation(&path, &value)).await;
            });
        }
    }
}

async fn wait(method: Method, resources: &[(&str, &str)], request_id: Option<&str>, owners: &[(&str, String)]) {
    let Some(directory) = std::env::var_os(DIRECTORY_ENV).map(PathBuf::from) else { return };
    if !directory.is_absolute() {
        return;
    }
    let mut file = match tokio::fs::File::open(directory.join("arm.json")).await {
        Ok(file) => file,
        Err(error) if error.kind() == ErrorKind::NotFound => return,
        Err(_) => return hook_error(&directory, "arm_open").await,
    };
    let mut bytes = Vec::new();
    if (&mut file).take(MAX_ARM_BYTES + 1).read_to_end(&mut bytes).await.is_err() {
        return hook_error(&directory, "arm_read").await;
    }
    let arm = match serde_json::from_slice::<Arm>(&bytes) {
        Ok(arm) if bytes.len() <= usize::try_from(MAX_ARM_BYTES).unwrap_or(0) && arm.validate() => arm,
        _ => return hook_error(&directory, "invalid_arm").await,
    };
    // A batch is never split or reordered. Delaying only some members would
    // change its semantics and delay unrelated requests.
    if resources.is_empty() || !resources.iter().all(|(volume, path)| arm.matches(method, volume, path)) {
        return;
    }
    let event = Uuid::new_v4();
    let started = Instant::now();
    let value = serde_json::json!({
        "nonce": arm.nonce, "event": event, "method": method,
        "pid": std::process::id(), "resources": resources,
        "request_id": request_id, "owners": owners,
    });
    if write_observation(&directory.join(format!("reached.{}.{event}.json", arm.nonce)), &value)
        .await
        .is_err()
    {
        return hook_error(&directory, "reached_write").await;
    }
    let observation = ExitObservation {
        path: Some(directory.join(format!("exited.{}.{event}.json", arm.nonce))),
        value,
        started,
    };
    match arm.delay {
        Delay::Delay { milliseconds } => {
            tokio::time::sleep(Duration::from_millis(milliseconds)).await;
            observation.finish("delay_elapsed").await;
        }
        Delay::Hold {} => {
            let release = directory.join(format!("release.{}", arm.nonce));
            let deadline = started + HOLD_LIMIT;
            loop {
                match tokio::fs::metadata(&release).await {
                    Ok(_) => return observation.finish("released").await,
                    Err(error) if error.kind() == ErrorKind::NotFound => {}
                    Err(_) => {
                        hook_error(&directory, "release_read").await;
                        return observation.finish("release_error").await;
                    }
                }
                if Instant::now() >= deadline {
                    hook_error(&directory, "hold_timeout").await;
                    return observation.finish("hold_timeout").await;
                }
                tokio::time::sleep(POLL).await;
            }
        }
    }
}

pub(super) async fn disk(method: Method, volume: &str, path: &str, request_id: Option<&str>) {
    wait(method, &[(volume, path)], request_id, &[]).await;
}

pub(super) async fn locks(method: Method, requests: &[rustfs_lock::LockRequest], request_id: Option<&str>) {
    // Keep the no-env path free of allocations as well as filesystem I/O.
    if std::env::var_os(DIRECTORY_ENV).is_none() {
        return;
    }
    let resources: Vec<_> = requests
        .iter()
        .map(|request| (request.resource.bucket.as_ref(), request.resource.object.as_ref()))
        .collect();
    let owners: Vec<_> = requests
        .iter()
        .map(|request| (request.owner.as_str(), request.lock_id.to_string()))
        .collect();
    wait(method, &resources, request_id, &owners).await;
}

#[cfg(test)]
mod tests {
    use super::{Arm, ExitObservation, Method, POLL};
    use std::io::ErrorKind;
    use std::time::Duration;
    use tokio::time::Instant;
    use uuid::Uuid;

    #[tokio::test]
    async fn rpc_delay_cancel_during_exit_write_records_the_actual_drop() {
        let directory = tempfile::tempdir().expect("observation directory");
        let path = directory.path().join("exited.json");
        let nonce = Uuid::new_v4();
        let observation = ExitObservation {
            path: Some(path.clone()),
            value: serde_json::json!({ "nonce": nonce }),
            started: Instant::now(),
        };
        let (entered, writing) = tokio::sync::oneshot::channel();
        let worker = tokio::spawn(observation.finish_with("released", move |_, _| async move {
            entered.send(()).expect("report writer entry");
            std::future::pending::<std::io::Result<()>>().await
        }));
        writing.await.expect("writer entered before cancellation");
        worker.abort();
        assert!(worker.await.expect_err("cancelled writer").is_cancelled());
        let bytes = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match tokio::fs::read(path.with_extension("dropped.json")).await {
                    Ok(bytes) => break bytes,
                    Err(error) if error.kind() == ErrorKind::NotFound => tokio::time::sleep(POLL).await,
                    Err(error) => panic!("cannot read drop observation: {error}"),
                }
            }
        })
        .await
        .expect("Drop records an exit after cancellation during the writer await");
        let value: serde_json::Value = serde_json::from_slice(&bytes).expect("drop observation JSON");
        assert_eq!(value["nonce"], serde_json::json!(nonce));
        assert_eq!(value["state"], "handler_future_dropped");
    }

    #[tokio::test]
    async fn rpc_delay_late_exit_rename_cannot_overwrite_drop_evidence() {
        let directory = tempfile::tempdir().expect("observation directory");
        let path = directory.path().join("exited.json");
        let nonce = Uuid::new_v4();
        let observation = ExitObservation {
            path: Some(path.clone()),
            value: serde_json::json!({ "nonce": nonce }),
            started: Instant::now(),
        };
        let (entered, writing) = tokio::sync::oneshot::channel();
        let (release, resume) = std::sync::mpsc::channel();
        let (completed, completion) = tokio::sync::oneshot::channel();
        let worker = tokio::spawn(observation.finish_with("released", move |path, value| async move {
            tokio::task::spawn_blocking(move || {
                let temporary = path.with_extension("late.tmp");
                std::fs::write(&temporary, serde_json::to_vec(&value)?)?;
                entered.send(()).expect("report started writer");
                resume.recv().expect("release already-started rename");
                std::fs::rename(temporary, path)?;
                let _ = completed.send(());
                Ok::<(), std::io::Error>(())
            })
            .await
            .expect("background exit writer")
        }));
        writing.await.expect("writer started before cancellation");
        worker.abort();
        assert!(worker.await.expect_err("cancelled writer").is_cancelled());
        let dropped_path = path.with_extension("dropped.json");
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match tokio::fs::read(&dropped_path).await {
                    Ok(_) => break,
                    Err(error) if error.kind() == ErrorKind::NotFound => tokio::time::sleep(POLL).await,
                    Err(error) => panic!("cannot read drop observation: {error}"),
                }
            }
        })
        .await
        .expect("Drop writes while the old rename is still blocked");
        release.send(()).expect("allow late rename");
        tokio::time::timeout(Duration::from_secs(5), completion)
            .await
            .expect("late rename completes")
            .expect("writer reports completion");
        let normal: serde_json::Value =
            serde_json::from_slice(&tokio::fs::read(path).await.expect("late normal exit")).expect("exit JSON");
        let dropped: serde_json::Value =
            serde_json::from_slice(&tokio::fs::read(dropped_path).await.expect("retained drop exit")).expect("drop JSON");
        assert_eq!(normal["state"], "released");
        assert_eq!(dropped["state"], "handler_future_dropped");
        assert_eq!(dropped["nonce"], serde_json::json!(nonce));
    }

    #[test]
    fn rpc_delay_arm_rejects_unknown_fields_methods_and_unbounded_scope() {
        let valid = serde_json::json!({ "nonce": Uuid::new_v4(), "methods": ["lock", "lock_batch"],
            "scope": { "class": "lifecycle", "bucket": "mpu-scope" }, "delay": { "kind": "hold" } });
        assert!(serde_json::from_value::<Arm>(valid.clone()).expect("strict arm").validate());
        for pointer in ["/path", "/scope/path", "/delay/path"] {
            let mut invalid = valid.clone();
            let (parent, field) = pointer.rsplit_once('/').expect("JSON pointer");
            let object = if parent.is_empty() {
                &mut invalid
            } else {
                invalid.pointer_mut(parent).expect("parent")
            };
            object[field] = serde_json::json!("unrelated");
            assert!(serde_json::from_value::<Arm>(invalid.clone()).is_err(), "accepted {invalid}");
        }
        let mut invalid = valid.clone();
        invalid["methods"] = serde_json::json!(["un_lock"]);
        assert!(serde_json::from_value::<Arm>(invalid).is_err(), "unlock delay must be impossible");
        let mut invalid = valid;
        invalid["methods"] = serde_json::json!(["delete"]);
        assert!(!serde_json::from_value::<Arm>(invalid).expect("known method").validate());
    }

    #[test]
    fn rpc_delay_multipart_recovery_is_limited_to_its_upload_scope() {
        let hash = "a".repeat(64);
        let arm: Arm = serde_json::from_value(serde_json::json!({
            "nonce": Uuid::new_v4(), "methods": ["recover_multipart_source"],
            "scope": { "class": "multipart", "sha256": hash },
            "delay": { "kind": "delay", "milliseconds": 80 }
        }))
        .expect("multipart recovery arm");
        assert!(arm.validate());
        assert!(arm.matches(Method::RecoverMultipartSource, ".rustfs.sys/multipart", &format!("{hash}/upload")));
        assert!(!arm.matches(Method::RecoverMultipartSource, ".rustfs.sys", &format!("{hash}/upload")));
        assert!(!arm.matches(Method::RecoverMultipartSource, ".rustfs.sys/multipart", "other/upload"));
        assert!(!arm.matches(Method::Delete, ".rustfs.sys/multipart", &format!("{hash}/upload")));
    }

    #[test]
    fn rpc_delay_multipart_rename_matches_only_the_complete_object() {
        let mut value = serde_json::json!({ "nonce": Uuid::new_v4(), "methods": ["rename_multipart_data"],
            "scope": { "class": "object", "bucket": "mpu-scope", "key": "complete-object" },
            "delay": { "kind": "delay", "milliseconds": 80 } });
        let arm: Arm = serde_json::from_value(value.clone()).expect("multipart rename arm");
        assert!(arm.validate());
        assert!(arm.matches(Method::RenameMultipartData, "mpu-scope", "complete-object"));
        assert!(!arm.matches(Method::RenameData, "mpu-scope", "complete-object"));
        assert!(!arm.matches(Method::RenameMultipartData, "mpu-scope", "other-object"));
        value["scope"] = serde_json::json!({ "class": "multipart", "sha256": "a".repeat(64) });
        assert!(!serde_json::from_value::<Arm>(value).expect("multipart scope arm").validate());
    }

    #[test]
    fn rpc_delay_matches_only_exact_resource_and_homogeneous_batch() {
        let arm: Arm = serde_json::from_value(serde_json::json!({ "nonce": Uuid::new_v4(), "methods": ["lock_batch"],
            "scope": { "class": "lifecycle", "bucket": "mpu-scope" }, "delay": { "kind": "delay", "milliseconds": 80 } }))
        .expect("arm");
        assert!(arm.matches(Method::LockBatch, "mpu-scope", ""));
        assert!(!arm.matches(Method::LockBatch, "mpu-scope", "unrelated"));
        assert!(!arm.matches(Method::Lock, "mpu-scope", ""));
        let mixed = [("mpu-scope", ""), ("other", "")];
        assert!(
            !mixed
                .iter()
                .all(|(volume, path)| arm.matches(Method::LockBatch, volume, path))
        );
    }
}
