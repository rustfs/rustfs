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

//! Method-selective delay and post-entry peer-loss coverage. Four nodes with
//! EC:2 require three write acknowledgements. The lifecycle shared lock needs
//! two grants, so the three/two-survivor cases straddle the MPU write quorum,
//! not the lifecycle read quorum. Losing peers affects both RPC planes; these
//! cases do not attribute their final error solely to metadata latency.
//!
//! RPC entry owner IDs and real lock-table snapshots are recorded. Full owner
//! lifetime intervals and exported lock counters remain separate acceptance
//! gaps; these tests do not infer either from logs or request completion.

use super::harness::{
    DistCluster, DistLayout, TestResult, assert_object_bytes, put_object, s3_probe_client, unique_bucket, wait_for_ready,
    wait_until,
};
use crate::common::{admin_request, init_logging, rustfs_binary_path_with_features};
use aws_sdk_s3::Client;
use aws_sdk_s3::error::{ProvideErrorMetadata, SdkError};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use http::Method;
use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use uuid::Uuid;

const HOOK_ENV: &str = "RUSTFS_E2E_RPC_DELAY_DIR";
const REQUEST_LIMIT: Duration = Duration::from_secs(100);
const OLD_PART: &[u8] = b"previous acknowledged multipart part";
const NEW_PART: &[u8] = b"replacement acknowledged multipart part";
const OLD_OBJECT: &[u8] = b"previous acknowledged destination";

#[derive(Clone, Copy, Debug)]
enum Operation {
    Create,
    Part,
    Complete,
    Abort,
}

impl Operation {
    const ALL: [Self; 4] = [Self::Create, Self::Part, Self::Complete, Self::Abort];

    fn method(self) -> &'static str {
        match self {
            Self::Create => "write_metadata",
            Self::Part => "rename_part",
            Self::Complete => "rename_data",
            Self::Abort => "delete",
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum Plane {
    Metadata,
    Lock,
}

#[derive(Clone, Copy, Debug)]
enum Scenario {
    BoundedDelay,
    PostEntryThreePeers,
    PostEntryTwoPeers,
}

#[derive(Clone)]
struct Session {
    bucket: String,
    key: String,
    upload_id: Option<String>,
    etag: Option<String>,
}

#[derive(Debug, Serialize)]
struct Ack {
    upload_id: Option<String>,
    etag: Option<String>,
}

#[derive(Debug, Serialize)]
struct Failure {
    status: Option<u16>,
    code: Option<String>,
    cause: Option<String>,
    retry_after: Option<String>,
    content_type: Option<String>,
    request_id: Option<String>,
    amz_request_id: Option<String>,
}

impl Failure {
    fn from_sdk<E: ProvideErrorMetadata>(error: SdkError<E>) -> Self {
        let response = error.raw_response();
        let header = |name: &str| response.and_then(|response| response.headers().get(name)).map(str::to_owned);
        Self {
            status: response.map(|response| response.status().as_u16()),
            code: error
                .as_service_error()
                .and_then(ProvideErrorMetadata::code)
                .map(str::to_owned),
            cause: header("x-rustfs-error-cause"),
            retry_after: header("retry-after"),
            content_type: header("content-type"),
            request_id: header("x-request-id"),
            amz_request_id: header("x-amz-request-id"),
        }
    }

    fn assert_unavailable(&self) {
        assert_eq!(self.status, Some(503), "post-entry two-peer loss must fail closed: {self:?}");
        assert_eq!(self.code.as_deref(), Some("ServiceUnavailable"), "{self:?}");
        assert!(
            self.request_id.as_ref().is_some_and(|id| !id.is_empty()),
            "missing real response ID: {self:?}"
        );
        assert_eq!(self.request_id, self.amz_request_id, "server response ID pair: {self:?}");
        if let Some(cause) = self.cause.as_deref() {
            assert!(
                matches!(cause, "namespace_lock_timeout" | "namespace_lock_unavailable" | "storage_write_quorum"),
                "{self:?}"
            );
            assert_eq!(self.content_type.as_deref(), Some("application/xml"), "{self:?}");
        }
        if self.cause.as_deref() == Some("namespace_lock_timeout") {
            assert_eq!(self.retry_after.as_deref(), Some("1"), "{self:?}");
        } else {
            assert!(self.retry_after.is_none(), "only namespace lock timeout supplies retry advice: {self:?}");
        }
    }
}

impl Session {
    async fn prepare(run: &Run, operation: Operation) -> TestResult<Self> {
        let session = Self {
            bucket: unique_bucket("mpurpc"),
            key: "scoped/object.bin".into(),
            upload_id: None,
            etag: None,
        };
        run.dist.create_bucket(&session.bucket).await?;
        let client = run.client(0)?;
        if matches!(operation, Operation::Complete) {
            put_object(&client, &session.bucket, &session.key, OLD_OBJECT.to_vec()).await?;
        }
        if matches!(operation, Operation::Create) {
            return Ok(session);
        }
        let upload = client
            .create_multipart_upload()
            .bucket(&session.bucket)
            .key(&session.key)
            .send()
            .await?;
        let upload_id = upload.upload_id().ok_or("CreateMultipartUpload omitted ID")?.to_owned();
        let part = client
            .upload_part()
            .bucket(&session.bucket)
            .key(&session.key)
            .upload_id(&upload_id)
            .part_number(1)
            .body(ByteStream::from_static(OLD_PART))
            .send()
            .await?;
        Ok(Self {
            upload_id: Some(upload_id),
            etag: Some(part.e_tag().ok_or("UploadPart omitted ETag")?.to_owned()),
            ..session
        })
    }

    fn completion(&self) -> CompletedMultipartUpload {
        CompletedMultipartUpload::builder()
            .parts(CompletedPart::builder().part_number(1).set_e_tag(self.etag.clone()).build())
            .build()
    }

    async fn execute(&self, client: &Client, operation: Operation) -> Result<Ack, Failure> {
        match operation {
            Operation::Create => client
                .create_multipart_upload()
                .bucket(&self.bucket)
                .key(&self.key)
                .send()
                .await
                .map(|result| Ack {
                    upload_id: result.upload_id().map(str::to_owned),
                    etag: None,
                })
                .map_err(Failure::from_sdk),
            Operation::Part => client
                .upload_part()
                .bucket(&self.bucket)
                .key(&self.key)
                .set_upload_id(self.upload_id.clone())
                .part_number(1)
                .body(ByteStream::from_static(NEW_PART))
                .send()
                .await
                .map(|result| Ack {
                    upload_id: None,
                    etag: result.e_tag().map(str::to_owned),
                })
                .map_err(Failure::from_sdk),
            Operation::Complete => client
                .complete_multipart_upload()
                .bucket(&self.bucket)
                .key(&self.key)
                .set_upload_id(self.upload_id.clone())
                .multipart_upload(self.completion())
                .send()
                .await
                .map(|_| Ack {
                    upload_id: None,
                    etag: None,
                })
                .map_err(Failure::from_sdk),
            Operation::Abort => client
                .abort_multipart_upload()
                .bucket(&self.bucket)
                .key(&self.key)
                .set_upload_id(self.upload_id.clone())
                .send()
                .await
                .map(|_| Ack {
                    upload_id: None,
                    etag: None,
                })
                .map_err(Failure::from_sdk),
        }
    }

    fn scope(&self, operation: Operation, plane: Plane) -> Value {
        if matches!(plane, Plane::Lock) {
            return json!({ "class": "lifecycle", "bucket": self.bucket });
        }
        if matches!(operation, Operation::Complete) {
            return json!({ "class": "object", "bucket": self.bucket, "key": self.key });
        }
        let digest = Sha256::digest(format!("{}/{}", self.bucket, self.key));
        json!({ "class": "multipart", "sha256": rustfs_utils::crypto::hex(digest) })
    }

    async fn verify_and_cleanup(&self, run: &Run, operation: Operation, ack: Option<Ack>) -> TestResult {
        let client = run.client(0)?;
        let mut recovered = self.clone();
        if ack.is_none() {
            match operation {
                Operation::Part => {
                    let parts = client
                        .list_parts()
                        .bucket(&self.bucket)
                        .key(&self.key)
                        .set_upload_id(self.upload_id.clone())
                        .send()
                        .await?;
                    assert_eq!(parts.parts().len(), 1, "failed replacement must preserve acknowledged part");
                    assert_eq!(
                        parts.parts()[0].e_tag(),
                        self.etag.as_deref(),
                        "failed replacement changed acknowledged ETag"
                    );
                    // Re-uploading first could hide damage to the previously
                    // acknowledged payload. Consume the old upload using its
                    // original ETag and verify its bytes before any new write.
                    self.execute(&client, Operation::Complete)
                        .await
                        .map_err(|error| format!("complete acknowledged old part after failed replacement: {error:?}"))?;
                    for node in 0..4 {
                        assert_object_bytes(&run.client(node)?, &self.bucket, &self.key, OLD_PART).await?;
                    }
                    let upload = client
                        .create_multipart_upload()
                        .bucket(&self.bucket)
                        .key(&self.key)
                        .send()
                        .await?;
                    let upload_id = upload.upload_id().ok_or("fresh recovery MPU omitted ID")?.to_owned();
                    assert_ne!(Some(upload_id.as_str()), self.upload_id.as_deref(), "recovery must use a fresh upload");
                    recovered.upload_id = Some(upload_id);
                    recovered.etag = None;
                }
                Operation::Complete => assert_object_bytes(&client, &self.bucket, &self.key, OLD_OBJECT).await?,
                _ => {}
            }
        }
        let ack = match ack {
            Some(ack) => ack,
            None => match recovered.execute(&client, operation).await {
                Ok(ack) => ack,
                Err(failure) if matches!(operation, Operation::Abort) && failure.code.as_deref() == Some("NoSuchUpload") => Ack {
                    upload_id: None,
                    etag: None,
                },
                Err(failure) => return Err(format!("{operation:?} did not recover after peer restart: {failure:?}").into()),
            },
        };
        match operation {
            Operation::Create => {
                let id = ack.upload_id.ok_or("successful CreateMultipartUpload omitted ID")?;
                client
                    .abort_multipart_upload()
                    .bucket(&self.bucket)
                    .key(&self.key)
                    .upload_id(id)
                    .send()
                    .await?;
            }
            Operation::Part => {
                let mut completed = recovered;
                completed.etag = Some(ack.etag.ok_or("successful UploadPart omitted ETag")?);
                completed
                    .execute(&client, Operation::Complete)
                    .await
                    .map_err(|error| format!("complete recovered part: {error:?}"))?;
                for node in 0..4 {
                    assert_object_bytes(&run.client(node)?, &self.bucket, &self.key, NEW_PART).await?;
                }
            }
            Operation::Complete => {
                for node in 0..4 {
                    assert_object_bytes(&run.client(node)?, &self.bucket, &self.key, OLD_PART).await?;
                }
            }
            Operation::Abort => {}
        }
        // An unacknowledged Create can leave a discoverable upload. Clean up
        // every upload for this unique key and prove that none survives.
        let uploads = client
            .list_multipart_uploads()
            .bucket(&self.bucket)
            .prefix(&self.key)
            .send()
            .await?;
        for upload in uploads.uploads() {
            client
                .abort_multipart_upload()
                .bucket(&self.bucket)
                .key(&self.key)
                .upload_id(upload.upload_id().ok_or("listed upload omitted ID")?)
                .send()
                .await?;
        }
        let uploads = client
            .list_multipart_uploads()
            .bucket(&self.bucket)
            .prefix(&self.key)
            .send()
            .await?;
        assert!(uploads.uploads().is_empty(), "upload metadata survived cleanup");
        if matches!(operation, Operation::Create | Operation::Abort) {
            let missing = client
                .head_object()
                .bucket(&self.bucket)
                .key(&self.key)
                .send()
                .await
                .expect_err("unpublished MPU must not expose an object");
            assert_eq!(missing.raw_response().map(|response| response.status().as_u16()), Some(404));
        }
        Ok(())
    }
}

struct Run {
    dist: DistCluster,
    directory: PathBuf,
    keep: bool,
    passed: bool,
}

async fn atomic_json(path: &Path, value: &Value) -> TestResult {
    let temporary = path.with_extension("tmp");
    tokio::fs::write(&temporary, serde_json::to_vec(value)?).await?;
    tokio::fs::rename(temporary, path).await?;
    Ok(())
}

impl Run {
    async fn start(label: &str) -> TestResult<Self> {
        rustfs_binary_path_with_features(Some("e2e-test-hooks"));
        let evidence = std::env::var_os("RUSTFS_E2E_LOG_DIR");
        let keep = evidence.is_some();
        let directory = evidence
            .map(PathBuf::from)
            .unwrap_or_else(std::env::temp_dir)
            .join(format!("multipart-rpc-{label}-{}", Uuid::new_v4()));
        let directory = std::path::absolute(directory)?;
        tokio::fs::create_dir_all(&directory).await?;
        let mut dist =
            DistCluster::new_stopped_with_env(DistLayout::FourNodeFourDisk, &[("RUSTFS_STORAGE_CLASS_STANDARD", "EC:2")]).await?;
        for node in 0..4 {
            let path = directory.join(format!("peer-{node}"));
            tokio::fs::create_dir_all(&path).await?;
            dist.cluster
                .set_node_env(node, HOOK_ENV, path.to_string_lossy().into_owned())?;
        }
        dist.capture_logs_under(&directory.join("logs"))?;
        dist.cluster.start().await?;
        Ok(Self {
            dist,
            directory,
            keep,
            passed: false,
        })
    }

    fn client(&self, node: usize) -> TestResult<Client> {
        Ok(s3_probe_client(&self.dist.client(node)?))
    }
    fn peer_dir(&self, node: usize) -> PathBuf {
        self.directory.join(format!("peer-{node}"))
    }

    async fn arm(&self, nonce: Uuid, methods: &[&str], scope: Value, bounded: bool) -> TestResult {
        let arm = json!({ "nonce": nonce, "methods": methods, "scope": scope,
            "delay": if bounded { json!({ "kind": "delay", "milliseconds": 80 }) } else { json!({ "kind": "hold" }) } });
        for node in 1..4 {
            atomic_json(&self.peer_dir(node).join("arm.json"), &arm).await?;
        }
        Ok(())
    }

    async fn observations(&self, node: usize, prefix: &str, nonce: Uuid) -> TestResult<Vec<Value>> {
        let mut entries = tokio::fs::read_dir(self.peer_dir(node)).await?;
        let mut observations = Vec::new();
        while let Some(entry) = entries.next_entry().await? {
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.starts_with("hook_error.") {
                return Err(format!("RPC hook failure: {}", tokio::fs::read_to_string(entry.path()).await?).into());
            }
            if name.starts_with(&format!("{prefix}.{nonce}.")) && name.ends_with(".json") {
                observations.push(serde_json::from_slice(&tokio::fs::read(entry.path()).await?)?);
            }
        }
        Ok(observations)
    }

    async fn reached(&self, nonce: Uuid, methods: &[&str], minimum_peers: usize) -> TestResult<Vec<usize>> {
        wait_until(
            Duration::from_secs(20),
            || async {
                let mut reached_peers = 0;
                for node in 1..4 {
                    let observations = self.observations(node, "reached", nonce).await?;
                    assert!(
                        observations
                            .iter()
                            .all(|value| methods.contains(&value["method"].as_str().unwrap_or(""))),
                        "wrong method reached: {observations:?}"
                    );
                    for observation in &observations {
                        assert!(
                            observation["request_id"].as_str().is_some_and(|id| !id.is_empty()),
                            "RPC entry must carry its actual propagated request ID: {observation}"
                        );
                        if observation["method"]
                            .as_str()
                            .is_some_and(|method| method == "lock" || method == "lock_batch")
                        {
                            assert!(
                                observation["owners"].as_array().is_some_and(|owners| !owners.is_empty()
                                    && owners
                                        .iter()
                                        .all(|owner| owner[0].as_str().is_some_and(|value| !value.is_empty())
                                            && owner[1].as_str().is_some_and(|value| !value.is_empty()))),
                                "lock entry must identify its actual owner: {observation}"
                            );
                        }
                    }
                    reached_peers += usize::from(!observations.is_empty());
                }
                Ok(reached_peers >= minimum_peers)
            },
            &format!("{minimum_peers} authenticated remote method entries {nonce}"),
        )
        .await?;
        let mut peers = Vec::new();
        for node in 1..4 {
            if !self.observations(node, "reached", nonce).await?.is_empty() {
                peers.push(node);
            }
        }
        Ok(peers)
    }

    async fn release(&self, nonce: Uuid) -> TestResult {
        for node in 1..4 {
            let path = self.peer_dir(node);
            // Disarm first so retries and cleanup cannot enter the old window.
            match tokio::fs::remove_file(path.join("arm.json")).await {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
            tokio::fs::write(path.join(format!("release.{nonce}")), b"release").await?;
        }
        Ok(())
    }

    async fn exited(&self, nonce: Uuid, surviving: &[usize]) -> TestResult {
        for &node in surviving {
            if node == 0 {
                continue;
            }
            wait_until(
                Duration::from_secs(20),
                || async {
                    let reached = self.observations(node, "reached", nonce).await?;
                    let exited = self.observations(node, "exited", nonce).await?;
                    Ok(reached
                        .iter()
                        .all(|entry| exited.iter().any(|exit| exit["event"] == entry["event"])))
                },
                &format!("peer {node} RPC hook exited {nonce}"),
            )
            .await?;
        }
        Ok(())
    }

    async fn lock_snapshot(&self, bucket: &str, label: &str, nonce: Uuid, nodes: &[usize]) -> TestResult<bool> {
        let mut snapshots = Vec::new();
        let mut drained = true;
        for &node in nodes {
            let (status, body) = admin_request(
                &self.dist.cluster.nodes[node].url,
                Method::GET,
                "/rustfs/admin/v3/top/locks?count=1000",
                None,
                &self.dist.cluster.access_key,
                &self.dist.cluster.secret_key,
            )
            .await?;
            assert!(status.is_success(), "lock table query on peer {node}: {status}");
            let locks: Value = serde_json::from_str(&body)?;
            assert_eq!(locks["truncated"], false, "lock evidence must be complete");
            assert!(locks.get("capability_note").is_none(), "lock subsystem is required");
            let scoped: Vec<_> = locks["locks"]
                .as_array()
                .ok_or("lock response omitted inventory")?
                .iter()
                .filter(|entry| entry["bucket"] == bucket)
                .cloned()
                .collect();
            drained &= scoped.is_empty();
            snapshots.push(json!({ "node": node, "locks": scoped }));
        }
        atomic_json(&self.directory.join(format!("locks.{nonce}.{label}.json")), &json!(snapshots)).await?;
        Ok(drained)
    }

    async fn drained(&self, bucket: &str, nonce: Uuid) -> TestResult {
        wait_until(
            Duration::from_secs(60),
            || async { self.lock_snapshot(bucket, "drained", nonce, &[0, 1, 2, 3]).await },
            "real scoped remote lock tables drained",
        )
        .await
    }
}

impl Drop for Run {
    fn drop(&mut self) {
        self.dist.cluster.stop();
        if self.passed && !self.keep {
            let _ = std::fs::remove_dir_all(&self.directory);
        } else {
            eprintln!("multipart RPC evidence: {}", self.directory.display());
        }
    }
}

async fn finish_request(task: JoinHandle<Result<Ack, Failure>>) -> TestResult<Result<Ack, Failure>> {
    Ok(timeout(REQUEST_LIMIT, task)
        .await
        .map_err(|_| "MPU exceeded its unchanged production budgets and E2E observation bound")??)
}

async fn run_matrix(plane: Plane) -> TestResult {
    init_logging();
    let mut run = Run::start(&format!("{plane:?}")).await?;
    for operation in Operation::ALL {
        for scenario in [
            Scenario::BoundedDelay,
            Scenario::PostEntryThreePeers,
            Scenario::PostEntryTwoPeers,
        ] {
            let session = Session::prepare(&run, operation).await?;
            // A previous successful response can precede its commit tail and
            // lifecycle release. Establish the actual owner boundary before
            // testing a new lifecycle acquisition or cold incarnation read.
            run.drained(&session.bucket, Uuid::new_v4()).await?;
            let nonce = Uuid::new_v4();
            let methods = match plane {
                Plane::Metadata => vec![operation.method()],
                Plane::Lock => vec!["lock", "lock_batch"],
            };
            run.arm(
                nonce,
                &methods,
                session.scope(operation, plane),
                matches!(scenario, Scenario::BoundedDelay),
            )
            .await?;
            let client = run.client(0)?;
            let action = session.clone();
            let task = tokio::spawn(async move { action.execute(&client, operation).await });
            // A lifecycle read needs two grants. Its bounded fanout starts
            // quorum + one clients, which can be local zero plus two remotes;
            // requiring the fourth client would wait for an unissued RPC.
            let required_remote_entries = if matches!(plane, Plane::Lock) { 2 } else { 3 };
            let reached = run.reached(nonce, &methods, required_remote_entries).await?;
            let kill_count = match scenario {
                Scenario::BoundedDelay => 0,
                Scenario::PostEntryThreePeers => 1,
                Scenario::PostEntryTwoPeers => 2,
            };
            let killed: Vec<_> = reached.iter().rev().copied().take(kill_count).collect();
            assert_eq!(killed.len(), kill_count, "each killed peer must have entered the selected RPC");
            if !killed.is_empty() {
                assert!(
                    !task.is_finished(),
                    "MPU finished while only one peer could pass the selected RPC entry barrier"
                );
                run.lock_snapshot(&session.bucket, "entered", nonce, &[0, 1, 2, 3]).await?;
                for &node in &killed {
                    run.dist.cluster.stop_node(node)?;
                }
            }
            run.release(nonce).await?;
            let result = finish_request(task).await?;
            atomic_json(
                &run.directory.join(format!("response.{nonce}.json")),
                &json!({ "plane": format!("{plane:?}"), "operation": format!("{operation:?}"),
                "scenario": format!("{scenario:?}"), "reached_remote_peers": reached, "killed_peers": killed,
                "mpu_write_quorum": { "required": 3, "available_peers": 4 - killed.len(), "acknowledged": result.is_ok() },
                "lifecycle_read_quorum": { "required": 2, "available_peers": 4 - killed.len() },
                "ack": result.as_ref().ok(), "failure": result.as_ref().err() }),
            )
            .await?;
            let ack = if matches!(scenario, Scenario::PostEntryTwoPeers) {
                result
                    .as_ref()
                    .expect_err("two writable peers cannot acknowledge an EC:2 mutation")
                    .assert_unavailable();
                None
            } else {
                Some(result.map_err(|failure| format!("{plane:?}/{operation:?}/{scenario:?}: {failure:?}"))?)
            };
            let surviving: Vec<_> = (0..4).filter(|node| !killed.contains(node)).collect();
            run.exited(nonce, &surviving).await?;
            if matches!(scenario, Scenario::BoundedDelay) {
                let mut delayed = 0;
                for node in 1..4 {
                    for exit in run.observations(node, "exited", nonce).await? {
                        if exit["state"] == "delay_elapsed" {
                            assert!(
                                exit["elapsed_ms"].as_u64().is_some_and(|elapsed| elapsed >= 80),
                                "selected RPC did not receive its configured bounded delay: {exit}"
                            );
                            delayed += 1;
                        }
                    }
                }
                let minimum = if matches!(plane, Plane::Metadata) { 2 } else { 1 };
                assert!(
                    delayed >= minimum,
                    "acknowledgement bypassed all delayed RPC peers: {plane:?}/{operation:?}"
                );
            }
            for &node in &killed {
                run.dist.cluster.start_node_process(node).await?;
            }
            if !killed.is_empty() {
                wait_for_ready(&run.dist.cluster).await?;
            }
            session.verify_and_cleanup(&run, operation, ack).await?;
            run.drained(&session.bucket, nonce).await?;
        }
    }
    run.passed = true;
    Ok(())
}

#[tokio::test]
async fn multipart_rpc_delay_metadata_preserves_quorum_and_cleanup() -> TestResult {
    run_matrix(Plane::Metadata).await
}

#[tokio::test]
async fn multipart_rpc_delay_lock_preserves_quorum_and_cleanup() -> TestResult {
    run_matrix(Plane::Lock).await
}

#[tokio::test]
async fn multipart_rpc_delay_cancel_recreate_preserves_incarnation() -> TestResult {
    init_logging();
    let mut run = Run::start("cancel-recreate").await?;
    let session = Session::prepare(&run, Operation::Part).await?;
    // Force no cache or generation state. The real owner boundary prevents a
    // legitimate overlap memo from bypassing this cold incarnation RPC.
    run.drained(&session.bucket, Uuid::new_v4()).await?;
    let nonce = Uuid::new_v4();
    run.arm(
        nonce,
        &["read_version"],
        json!({ "class": "incarnation", "bucket": session.bucket }),
        false,
    )
    .await?;
    let client = run.client(0)?;
    let old = session.clone();
    let task = tokio::spawn(async move { old.execute(&client, Operation::Part).await });
    // The default bounded read fanout starts three drives, including at least
    // two remote peers. Do not require an RPC that the scheduler has not sent.
    run.reached(nonce, &["read_version"], 2).await?;
    run.lock_snapshot(&session.bucket, "before_disconnect", nonce, &[0, 1, 2, 3])
        .await?;
    task.abort();
    assert!(task.await.expect_err("SDK task was aborted").is_cancelled());
    // Disconnecting the client does not establish server cancellation. Release
    // the prewrite window and let the actual lifecycle writer and lock tables
    // establish that the old request no longer owns the bucket.
    run.release(nonce).await?;
    run.exited(nonce, &[1, 2, 3]).await?;
    let writer = run.client(1)?;
    timeout(REQUEST_LIMIT, writer.delete_bucket().bucket(&session.bucket).send()).await??;
    run.drained(&session.bucket, nonce).await?;
    writer.create_bucket().bucket(&session.bucket).send().await?;
    put_object(&writer, &session.bucket, &session.key, b"new incarnation sentinel".to_vec()).await?;
    for operation in [Operation::Part, Operation::Complete, Operation::Abort] {
        let failure = session
            .execute(&writer, operation)
            .await
            .expect_err("old upload ID must not mutate a recreated bucket");
        assert_eq!(failure.status, Some(404), "{operation:?}: {failure:?}");
        assert_eq!(failure.code.as_deref(), Some("NoSuchUpload"), "{operation:?}: {failure:?}");
        assert_object_bytes(&writer, &session.bucket, &session.key, b"new incarnation sentinel").await?;
    }
    let upload = writer
        .create_multipart_upload()
        .bucket(&session.bucket)
        .key("fresh.bin")
        .send()
        .await?;
    let upload_id = upload.upload_id().ok_or("fresh MPU omitted ID")?;
    let part = writer
        .upload_part()
        .bucket(&session.bucket)
        .key("fresh.bin")
        .upload_id(upload_id)
        .part_number(1)
        .body(ByteStream::from_static(NEW_PART))
        .send()
        .await?;
    writer
        .complete_multipart_upload()
        .bucket(&session.bucket)
        .key("fresh.bin")
        .upload_id(upload_id)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .parts(
                    CompletedPart::builder()
                        .part_number(1)
                        .e_tag(part.e_tag().ok_or("fresh part omitted ETag")?)
                        .build(),
                )
                .build(),
        )
        .send()
        .await?;
    for node in 0..4 {
        assert_object_bytes(&run.client(node)?, &session.bucket, "fresh.bin", NEW_PART).await?;
    }
    run.drained(&session.bucket, Uuid::new_v4()).await?;
    run.passed = true;
    Ok(())
}
