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

use crate::common::{
    RustFSTestClusterEnvironment, RustFSTestEnvironment, admin_request, init_logging, replication_fast_env, rustfs_binary_path,
    signal_process, signed_request,
};
use crate::fake_s3_target::{BucketMode, FAKE_ACCESS_KEY, FAKE_SECRET_KEY, FakeS3Target, Operation as FakeTargetOperation};
use crate::on_demand_migration::common::{ODM_SERVER_ENV, OdmTestEnv, SeedObject, fake_source_client};
use crate::replication_extension_test::{
    LOOPBACK_REPLICATION_TARGET_ENV, ReplicationTargetOptions, put_bucket_replication, set_replication_target_with_options,
};
use crate::storage_api::upgrade_compatibility::RUSTFS_META_MULTIPART_BUCKET;
use aws_sdk_s3::Client;
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    BucketLifecycleConfiguration, BucketVersioningStatus, CompletedMultipartUpload, CompletedPart, DefaultRetention, Delete,
    ExpirationStatus, LifecycleExpiration, LifecycleRule, LifecycleRuleFilter, ObjectAttributes, ObjectIdentifier,
    ObjectLockConfiguration, ObjectLockEnabled, ObjectLockRetentionMode, ObjectLockRule, PublicAccessBlockConfiguration,
    ServerSideEncryption, ServerSideEncryptionByDefault, ServerSideEncryptionConfiguration, ServerSideEncryptionRule,
    StorageClass, Tag, Tagging, VersioningConfiguration,
};
use http::{Method, StatusCode};
use serde_json::Value;
use std::future::Future;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::task::JoinSet;
use tokio::time::{Instant, sleep};

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;
type BoxError = Box<dyn std::error::Error + Send + Sync>;

const SOURCE_BINARY_ENV: &str = "RUSTFS_UPGRADE_SOURCE_BINARY";
const G09_EVIDENCE_DIR_ENV: &str = "RUSTFS_SCANNER_HEAL_G09_EVIDENCE_DIR";
const RC5_COMMIT: &str = "40a2470feb567201165a5b809b7598bb4b1f68f5";
const STABLE_1_0_1_COMMIT: &str = "6de965ae3c965a78ff819fbcd7acd4aa44177d92";
const SSE_MASTER_KEY_ENV: &str = "RUSTFS_SSE_S3_MASTER_KEY";
const SSE_MASTER_KEY: &str = "QkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkI=";
const PLAIN_BUCKET: &str = "upgrade-plain-data";
const VERSIONED_BUCKET: &str = "upgrade-versioned-data";
const MIXED_BUCKET: &str = "upgrade-mixed-version-data";
const PENDING_MULTIPART_BUCKET: &str = "upgrade-mpu-continuity";
const PENDING_MULTIPART_KEY: &str = "pending/legacy-multipart.bin";
const MIXED_NODE_COUNT: usize = 4;
const UPGRADE_READINESS_BODY: &[u8] = b"upgrade write readiness";
// Written by the "previous-seed" readiness probe through node 0 before any
// current binary starts; every later phase must still read it.
const PREVIOUS_RELEASE_SEED_KEY: &str = ".upgrade-readiness/previous-seed/node-0";
const MULTIPART_WORKERS: usize = 16;
const MULTIPART_UPLOADS_PER_WORKER: usize = 16;
// Bound each 64 KiB upload, including completion acknowledgement. A peer
// that accepts the request but never responds must fail the compatibility
// check with the writer and object, rather than consume the whole CI job.
const MIXED_MULTIPART_UPLOAD_TIMEOUT: Duration = Duration::from_secs(30);
// Peers keep a restarted node's drive in Suspect/Returning for roughly
// probe_interval (2s) x success_threshold (3) after it comes back; 30s
// comfortably covers that window plus CI scheduling jitter.
const LISTING_CONVERGENCE_TIMEOUT: Duration = Duration::from_secs(30);
// Parity 0 for the reduced-redundancy class only. A parity-0 write on the
// four-drive set needs all four drives (`write_quorum = data_drives`), so a
// PUT with that class succeeds only once the serving node writes through every
// one of its own drive handles. Standard-class objects keep the default layout.
const ALL_DRIVES_STORAGE_CLASS_ENV: (&str, &str) = ("RUSTFS_STORAGE_CLASS_RRS", "EC:0");
// A peer drops a restarted node's drive to Offline after two failed
// operations and only re-admits it through probe_interval (2s) x
// success_threshold (3) once the node answers again. The budget covers that
// for every node after the slowest restart in the sequence, plus CI jitter.
const DRIVE_READMISSION_TIMEOUT: Duration = Duration::from_secs(60);

// Bucket-configuration upgrade/rollback scenarios (rustfs#7172, #7183, #7089).
const CONFIG_PLAIN_BUCKET: &str = "upgrade-config-plain";
const CONFIG_ENCRYPTED_BUCKET: &str = "upgrade-config-encrypted";
const CONFIG_REPLICATED_BUCKET: &str = "upgrade-config-replicated";
const CONFIG_LOCKED_BUCKET: &str = "upgrade-config-locked";
const CONFIG_REPLICA_BUCKET: &str = "upgrade-config-replica";
const ROLLBACK_BUCKET: &str = "rollback-config-data";
const ROLLBACK_REPLICA_BUCKET: &str = "rollback-config-replica";
const BUCKET_QUOTA_BYTES: u64 = 64 * 1024 * 1024;
const QUOTA_USAGE_READINESS_TIMEOUT: Duration = Duration::from_secs(90);
const LIFECYCLE_RULE_ID: &str = "upgrade-expire-logs";
const LIFECYCLE_PREFIX: &str = "logs/";
const LIFECYCLE_DAYS: i32 = 30;
const BUCKET_TAG_KEY: &str = "owner";
const BUCKET_TAG_VALUE: &str = "upgrade-compatibility";
const OBJECT_LOCK_DAYS: i32 = 1;
// `set-bucket-quota` answers 503 until the scanner has made the bucket's usage
// authoritative; the quota test uses the same 30s budget.
const QUOTA_READINESS_TIMEOUT: Duration = Duration::from_secs(30);
// Quota admission fails closed while a freshly started server has neither
// authoritative usage nor a persisted degraded baseline for the bucket
// (rustfs#5716), so a write to a quota-enabled bucket is retryable-503 for that
// window. It is a restart property, not an upgrade property — the same window
// opens on the very first start — so the write assertions ride it out instead
// of treating it as an upgrade failure.
const QUOTA_ADMISSION_WARMUP_TIMEOUT: Duration = Duration::from_secs(90);

struct G09EvidenceContext {
    directory: PathBuf,
    current_revision: String,
    previous_revision: String,
    run_id: String,
    measurement_window_id: String,
}

fn source_binary() -> Result<PathBuf, Box<dyn std::error::Error + Send + Sync>> {
    let path = std::env::var_os(SOURCE_BINARY_ENV)
        .map(PathBuf::from)
        .ok_or("RUSTFS_UPGRADE_SOURCE_BINARY must point to the pinned previous release binary")?;
    if !path.is_file() {
        return Err(format!("upgrade source binary does not exist: {}", path.display()).into());
    }
    Ok(path)
}

fn is_lower_hex_revision(value: &str) -> bool {
    value.len() == 40
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

fn current_source_revision() -> Result<String, BoxError> {
    let revision = env!("RUSTFS_E2E_BUILD_COMMIT");
    if !is_lower_hex_revision(revision) {
        return Err(format!("current test binary has invalid source revision {revision}").into());
    }
    Ok(revision.to_string())
}

async fn binary_source_revision(binary: &Path) -> Result<String, BoxError> {
    let output = tokio::process::Command::new(binary).arg("--version").output().await?;
    if !output.status.success() {
        return Err(format!("{} --version failed with {}", binary.display(), output.status).into());
    }
    let version = String::from_utf8(output.stdout)?;
    source_revision_from_version_output(&version, binary)
}

fn source_revision_from_version_output(version: &str, binary: &Path) -> Result<String, BoxError> {
    version
        .split(|ch: char| !ch.is_ascii_hexdigit())
        .find(|token| is_lower_hex_revision(token))
        .map(str::to_string)
        .ok_or_else(|| format!("{} --version did not expose a 40-byte source revision", binary.display()).into())
}

async fn g09_evidence_context(previous_binary: &Path) -> Result<Option<G09EvidenceContext>, BoxError> {
    let Some(directory) = std::env::var_os(G09_EVIDENCE_DIR_ENV) else {
        return Ok(None);
    };
    let directory = PathBuf::from(directory);
    std::fs::create_dir_all(&directory)?;
    let current_revision = current_source_revision()?;
    let previous_revision = binary_source_revision(previous_binary).await?;
    if current_revision == previous_revision {
        return Err("G09 mixed-version evidence requires distinct current and previous source revisions".into());
    }
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    Ok(Some(G09EvidenceContext {
        directory,
        current_revision,
        previous_revision,
        run_id: format!("g09-upgrade-{}-{now}", std::process::id()),
        measurement_window_id: format!("g09-mixed-version-window-{now}"),
    }))
}

fn write_g09_evidence(
    context: &G09EvidenceContext,
    field: &str,
    role: &str,
    cases: &[&str],
    test: &str,
    details: Value,
) -> TestResult {
    let path = context.directory.join(format!("G09-{field}.json"));
    let mut evidence = serde_json::json!({
        "schema": 1,
        "evidence_type": "measured",
        "artifact_kind": "upgrade-compatibility-e2e",
        "source_revision": context.current_revision,
        "run_id": context.run_id,
        "measurement_window_id": context.measurement_window_id,
        "gate": "G09",
        "field": field,
        "versions": [context.previous_revision, context.current_revision],
        "current_revision": context.current_revision,
        "previous_revision": context.previous_revision,
        "mixed_version_role": role,
        "mixed_version_cases": cases,
        "test": test,
        "details": details,
    });
    if field == "rollback_payload_evidence" {
        evidence["rollback_payload_replayed"] = Value::Bool(true);
    }
    let data = serde_json::to_vec_pretty(&evidence)?;
    if data.len() > 1024 * 1024 {
        return Err("G09 mixed-version evidence exceeds the 1 MiB artifact budget".into());
    }
    let mut output = std::fs::OpenOptions::new().write(true).create_new(true).open(path)?;
    output.write_all(&data)?;
    output.write_all(b"\n")?;
    output.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod g09_evidence_tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn source_revision_parser_requires_lowercase_forty_byte_sha() -> TestResult {
        let binary = Path::new("rustfs");
        assert_eq!(
            source_revision_from_version_output("rustfs 1.0.0 abcdef0123456789abcdef0123456789abcdef01 clean", binary)?,
            "abcdef0123456789abcdef0123456789abcdef01"
        );
        assert!(source_revision_from_version_output("rustfs ABCDEF0123456789ABCDEF0123456789ABCDEF01", binary).is_err());
        assert!(source_revision_from_version_output("rustfs abcdef", binary).is_err());
        Ok(())
    }

    #[test]
    fn g09_evidence_writer_records_measured_role_and_refuses_overwrite() -> TestResult {
        let directory = TempDir::new()?;
        let context = G09EvidenceContext {
            directory: directory.path().to_path_buf(),
            current_revision: "b".repeat(40),
            previous_revision: "a".repeat(40),
            run_id: "g09-upgrade-test-run".to_string(),
            measurement_window_id: "g09-upgrade-test-window".to_string(),
        };
        write_g09_evidence(
            &context,
            "mixed_version_reader_evidence",
            "mixed-version-reader",
            &["old-writer-new-reader", "new-writer-old-reader"],
            "unit",
            serde_json::json!({"assertions": ["reader evidence"]}),
        )?;
        let path = directory.path().join("G09-mixed_version_reader_evidence.json");
        let evidence: Value = serde_json::from_slice(&std::fs::read(path)?)?;
        assert_eq!(evidence["evidence_type"], "measured");
        assert_eq!(evidence["gate"], "G09");
        assert_eq!(evidence["field"], "mixed_version_reader_evidence");
        assert_eq!(evidence["mixed_version_role"], "mixed-version-reader");
        assert_eq!(evidence["versions"], serde_json::json!(["a".repeat(40), "b".repeat(40)]));

        let overwrite = write_g09_evidence(
            &context,
            "mixed_version_reader_evidence",
            "mixed-version-reader",
            &["old-writer-new-reader", "new-writer-old-reader"],
            "unit",
            serde_json::json!({}),
        );
        assert!(overwrite.is_err(), "G09 evidence must not overwrite an existing artifact");
        Ok(())
    }
}

async fn enable_versioning(client: &Client, bucket: &str) -> TestResult {
    let configuration = VersioningConfiguration::builder()
        .status(BucketVersioningStatus::Enabled)
        .build();
    client
        .put_bucket_versioning()
        .bucket(bucket)
        .versioning_configuration(configuration)
        .send()
        .await?;
    Ok(())
}

async fn read_object(
    client: &Client,
    bucket: &str,
    key: &str,
    version_id: Option<&str>,
) -> Result<(Option<ServerSideEncryption>, Vec<u8>), Box<dyn std::error::Error + Send + Sync>> {
    let mut request = client.get_object().bucket(bucket).key(key);
    if let Some(version_id) = version_id {
        request = request.version_id(version_id);
    }
    let response = request.send().await?;
    let encryption = response.server_side_encryption().cloned();
    let body = response.body.collect().await?.into_bytes().to_vec();
    Ok((encryption, body))
}

struct PendingMultipart {
    upload_id: String,
    completed_parts: Vec<CompletedPart>,
    part_sizes: Vec<i64>,
}

async fn stage_multipart(client: &Client, bucket: &str, key: &str, parts: &[Vec<u8>]) -> Result<PendingMultipart, BoxError> {
    let created = client.create_multipart_upload().bucket(bucket).key(key).send().await?;
    let upload_id = created
        .upload_id()
        .ok_or("CreateMultipartUpload omitted upload ID")?
        .to_owned();
    let mut completed_parts = Vec::with_capacity(parts.len());
    let mut part_sizes = Vec::with_capacity(parts.len());
    for (index, part) in parts.iter().enumerate() {
        let part_number = i32::try_from(index + 1)?;
        let uploaded = client
            .upload_part()
            .bucket(bucket)
            .key(key)
            .upload_id(&upload_id)
            .part_number(part_number)
            .body(ByteStream::from(part.clone()))
            .send()
            .await?;
        completed_parts.push(
            CompletedPart::builder()
                .part_number(part_number)
                .e_tag(uploaded.e_tag().ok_or("UploadPart omitted ETag")?)
                .build(),
        );
        part_sizes.push(i64::try_from(part.len())?);
    }
    Ok(PendingMultipart {
        upload_id,
        completed_parts,
        part_sizes,
    })
}

async fn complete_pending_multipart(
    client: &Client,
    bucket: &str,
    key: &str,
    pending: &PendingMultipart,
) -> Result<aws_sdk_s3::operation::complete_multipart_upload::CompleteMultipartUploadOutput, BoxError> {
    Ok(client
        .complete_multipart_upload()
        .bucket(bucket)
        .key(key)
        .upload_id(&pending.upload_id)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .set_parts(Some(pending.completed_parts.clone()))
                .build(),
        )
        .send()
        .await?)
}

async fn write_multipart(client: &Client, bucket: &str, key: &str, parts: &[Vec<u8>]) -> TestResult {
    let pending = stage_multipart(client, bucket, key, parts).await?;
    complete_pending_multipart(client, bucket, key, &pending).await?;
    Ok(())
}

fn configure_cluster_logs(cluster: &mut RustFSTestClusterEnvironment) -> TestResult {
    let Some(log_dir) = std::env::var_os("RUSTFS_E2E_LOG_DIR") else {
        return Ok(());
    };
    std::fs::create_dir_all(&log_dir)?;
    for node_idx in 0..cluster.nodes.len() {
        let path = Path::new(&log_dir).join(format!("mixed-upgrade-node-{node_idx}.log"));
        cluster.set_node_capture_log_path(node_idx, path.to_string_lossy().into_owned())?;
    }
    Ok(())
}

async fn write_multipart_load(clients: &[Client], phase: &str) -> Result<Vec<String>, Box<dyn std::error::Error + Send + Sync>> {
    let mut tasks = JoinSet::new();
    for worker in 0..MULTIPART_WORKERS {
        let node = worker % clients.len();
        let client = clients[node].clone();
        let phase = phase.to_string();
        tasks.spawn(async move {
            let mut keys = Vec::with_capacity(MULTIPART_UPLOADS_PER_WORKER);
            for upload in 0..MULTIPART_UPLOADS_PER_WORKER {
                let key = format!("{phase}/multipart/{worker:02}/{upload:02}");
                let part = vec![u8::try_from(worker)?; 64 * 1024];
                mixed_multipart_before(
                    Instant::now() + MIXED_MULTIPART_UPLOAD_TIMEOUT,
                    node,
                    &key,
                    write_multipart(&client, MIXED_BUCKET, &key, &[part]),
                )
                .await?;
                keys.push(key);
            }
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(keys)
        });
    }

    let mut keys = Vec::with_capacity(MULTIPART_WORKERS * MULTIPART_UPLOADS_PER_WORKER);
    while let Some(result) = tasks.join_next().await {
        keys.extend(result??);
    }
    Ok(keys)
}

async fn write_mixed_put_load(clients: &[Client], phase: &str) -> Result<Vec<String>, BoxError> {
    let mut tasks = JoinSet::new();
    for worker in 0..MULTIPART_WORKERS {
        let node = worker % clients.len();
        let client = clients[node].clone();
        let phase = phase.to_owned();
        tasks.spawn(async move {
            let mut keys = Vec::with_capacity(MULTIPART_UPLOADS_PER_WORKER);
            for upload in 0..MULTIPART_UPLOADS_PER_WORKER {
                let key = format!("{phase}/put/{worker:02}/{upload:02}");
                tokio::time::timeout(
                    MIXED_MULTIPART_UPLOAD_TIMEOUT,
                    client
                        .put_object()
                        .bucket(MIXED_BUCKET)
                        .key(&key)
                        .body(ByteStream::from(vec![u8::try_from(worker)?; 64 * 1024]))
                        .send(),
                )
                .await
                .map_err(|_| format!("node {node}: PUT {key} acknowledgement deadline exceeded"))??;
                keys.push(key);
            }
            Ok::<_, BoxError>(keys)
        });
    }
    let mut keys = Vec::with_capacity(MULTIPART_WORKERS * MULTIPART_UPLOADS_PER_WORKER);
    while let Some(result) = tasks.join_next().await {
        keys.extend(result??);
    }
    Ok(keys)
}

async fn verify_load_bodies(clients: &[Client], phase: &str, operation: &str) -> TestResult {
    for worker in 0..MULTIPART_WORKERS {
        let reader = &clients[(worker + 1) % clients.len()];
        for upload in 0..MULTIPART_UPLOADS_PER_WORKER {
            let key = format!("{phase}/{operation}/{worker:02}/{upload:02}");
            assert_eq!(
                read_object(reader, MIXED_BUCKET, &key, None).await?.1,
                vec![u8::try_from(worker)?; 64 * 1024],
                "{phase}: acknowledged {key} body"
            );
        }
    }
    Ok(())
}

async fn exercise_homogeneous_multipart(cluster: &RustFSTestClusterEnvironment, phase: &str) -> TestResult {
    let clients = cluster.create_all_clients()?;
    wait_for_upgrade_write_readiness(&clients, MIXED_BUCKET, phase, LISTING_CONVERGENCE_TIMEOUT).await?;
    wait_for_drive_readmission(&clients, MIXED_BUCKET, phase, DRIVE_READMISSION_TIMEOUT).await?;
    let keys = match write_multipart_load(&clients, phase).await {
        Ok(keys) => keys,
        Err(error) => {
            let log_dir = std::env::var_os("RUSTFS_E2E_LOG_DIR");
            return observe_mixed_multipart_failure(cluster, phase, error, log_dir.as_deref().map(Path::new)).await;
        }
    };
    assert_eq!(keys.len(), MULTIPART_WORKERS * MULTIPART_UPLOADS_PER_WORKER);
    verify_load_bodies(&clients, phase, "multipart").await?;
    for (reader, client) in clients.iter().enumerate() {
        wait_for_phase_listing(client, phase, keys.len(), &format!("homogeneous reader {reader}")).await?;
    }
    Ok(())
}

struct PendingSourceBytes {
    root: PathBuf,
    files: Vec<(PathBuf, Vec<u8>)>,
}

fn pending_source_bytes(
    cluster: &RustFSTestClusterEnvironment,
    pending: &PendingMultipart,
) -> Result<Vec<PendingSourceBytes>, BoxError> {
    let source_hash =
        rustfs_utils::hex_sha256(format!("{PENDING_MULTIPART_BUCKET}/{PENDING_MULTIPART_KEY}").as_bytes(), str::to_owned);
    let mut sources = Vec::new();
    for volume in cluster.nodes.iter().flat_map(|node| &node.data_dirs) {
        let hash_root = Path::new(volume).join(RUSTFS_META_MULTIPART_BUCKET).join(&source_hash);
        let roots = std::fs::read_dir(&hash_root)?
            .map(|entry| {
                let entry = entry?;
                Ok(entry.file_type()?.is_dir().then(|| entry.path()))
            })
            .collect::<Result<Vec<_>, std::io::Error>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        if roots.is_empty() {
            return Err(std::io::Error::new(std::io::ErrorKind::NotFound, "pending upload source has not settled").into());
        }
        assert_eq!(roots.len(), 1, "the dedicated key has exactly one real pending upload");
        let root = roots.into_iter().next().ok_or("pending upload source missing")?;
        let mut files = vec![(PathBuf::from("xl.meta"), std::fs::read(root.join("xl.meta"))?)];
        let data_dirs = std::fs::read_dir(&root)?
            .map(|entry| {
                let entry = entry?;
                Ok((entry.file_type()?.is_dir() && entry.path().join("part.1").is_file()).then(|| entry.path()))
            })
            .collect::<Result<Vec<_>, std::io::Error>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        if data_dirs.is_empty() {
            return Err(std::io::Error::new(std::io::ErrorKind::NotFound, "selected part publication has not settled").into());
        }
        assert_eq!(data_dirs.len(), 1, "the upload has one actual selected-part directory");
        let data_dir = data_dirs.into_iter().next().ok_or("pending upload parts missing")?;
        for part in &pending.completed_parts {
            let number = part.part_number().ok_or("acknowledged part omitted its number")?;
            for name in [format!("part.{number}"), format!("part.{number}.meta")] {
                let path = data_dir.join(name);
                files.push((path.strip_prefix(&root)?.to_owned(), std::fs::read(path)?));
            }
        }
        sources.push(PendingSourceBytes { root, files });
    }
    assert_eq!(
        sources.len(),
        MIXED_NODE_COUNT,
        "the controlled legacy fixture requires every physical source"
    );
    Ok(sources)
}

async fn wait_for_pending_source_bytes(
    cluster: &RustFSTestClusterEnvironment,
    pending: &PendingMultipart,
) -> Result<Vec<PendingSourceBytes>, BoxError> {
    let deadline = Instant::now() + DRIVE_READMISSION_TIMEOUT;
    loop {
        match pending_source_bytes(cluster, pending) {
            Ok(sources) => return Ok(sources),
            Err(error)
                if error
                    .downcast_ref::<std::io::Error>()
                    .is_some_and(|error| error.kind() == std::io::ErrorKind::NotFound)
                    && Instant::now() < deadline =>
            {
                sleep(Duration::from_millis(500)).await
            }
            Err(error) => return Err(error),
        }
    }
}

async fn assert_pending_parts(client: &Client, pending: &PendingMultipart) -> TestResult {
    let listed = client
        .list_parts()
        .bucket(PENDING_MULTIPART_BUCKET)
        .key(PENDING_MULTIPART_KEY)
        .upload_id(&pending.upload_id)
        .send()
        .await?;
    assert_eq!(listed.upload_id(), Some(pending.upload_id.as_str()));
    assert_ne!(listed.is_truncated(), Some(true));
    assert_eq!(listed.parts().len(), pending.completed_parts.len());
    for ((actual, expected), expected_size) in listed.parts().iter().zip(&pending.completed_parts).zip(&pending.part_sizes) {
        assert_eq!(actual.part_number(), expected.part_number());
        assert_eq!(actual.e_tag(), expected.e_tag());
        assert_eq!(actual.size(), Some(*expected_size));
    }
    Ok(())
}

async fn assert_mixed_pending_refusal(
    cluster: &RustFSTestClusterEnvironment,
    pending: &PendingMultipart,
    sources: &[PendingSourceBytes],
) -> TestResult {
    let clients = cluster.create_all_clients()?;
    assert_pending_parts(&clients[0], pending).await?;
    let error = upgrade_probe_client(&clients[0])
        .complete_multipart_upload()
        .bucket(PENDING_MULTIPART_BUCKET)
        .key(PENDING_MULTIPART_KEY)
        .upload_id(&pending.upload_id)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .set_parts(Some(pending.completed_parts.clone()))
                .build(),
        )
        .send()
        .await
        .expect_err("one native participant cannot authorize a legacy MPU completion");
    assert_eq!(error.raw_response().map(|response| response.status().as_u16()), Some(503), "{error:?}");
    assert_eq!(error.as_service_error().and_then(ProvideErrorMetadata::code), Some("ServiceUnavailable"));
    for client in &clients {
        assert_pending_parts(client, pending).await?;
        let error = client
            .get_object()
            .bucket(PENDING_MULTIPART_BUCKET)
            .key(PENDING_MULTIPART_KEY)
            .send()
            .await
            .expect_err("the rejected Complete must not publish a target");
        assert_eq!(error.as_service_error().and_then(ProvideErrorMetadata::code), Some("NoSuchKey"));
    }
    for source in sources {
        for entry in std::fs::read_dir(&source.root)? {
            assert!(
                !entry?.file_name().to_string_lossy().starts_with(".mpu-"),
                "the failed preflight must not upgrade one physical legacy source"
            );
        }
        for (relative, expected) in &source.files {
            let actual = std::fs::read(source.root.join(relative))?;
            assert!(actual == *expected, "rejected Complete changed actual source {}", relative.display());
        }
    }
    Ok(())
}

struct CompletedPendingMultipart {
    version_id: String,
    etag: String,
    body: Vec<u8>,
}

async fn assert_resumed_multipart(clients: &[Client], completed: &CompletedPendingMultipart) -> TestResult {
    for (node, client) in clients.iter().enumerate() {
        let response = client
            .get_object()
            .bucket(PENDING_MULTIPART_BUCKET)
            .key(PENDING_MULTIPART_KEY)
            .version_id(&completed.version_id)
            .send()
            .await?;
        assert_eq!(response.version_id(), Some(completed.version_id.as_str()), "reader {node}");
        assert_eq!(response.e_tag(), Some(completed.etag.as_str()), "reader {node}");
        assert_eq!(
            response.body.collect().await?.into_bytes().as_ref(),
            completed.body.as_slice(),
            "reader {node}"
        );
        assert_eq!(
            read_object(client, PENDING_MULTIPART_BUCKET, PENDING_MULTIPART_KEY, None)
                .await?
                .1,
            completed.body
        );
        let versions = client
            .list_object_versions()
            .bucket(PENDING_MULTIPART_BUCKET)
            .prefix(PENDING_MULTIPART_KEY)
            .send()
            .await?;
        assert_eq!(versions.versions().len(), 1, "same-upload replay must not add a version");
        assert_eq!(versions.versions()[0].version_id(), Some(completed.version_id.as_str()));
        assert!(versions.delete_markers().is_empty());
    }
    Ok(())
}

async fn finish_pending_multipart(
    clients: &[Client],
    pending: &PendingMultipart,
    body: Vec<u8>,
) -> Result<CompletedPendingMultipart, BoxError> {
    let complete = complete_pending_multipart(&clients[0], PENDING_MULTIPART_BUCKET, PENDING_MULTIPART_KEY, pending).await?;
    let version_id = complete
        .version_id()
        .filter(|version| !version.is_empty() && *version != "null")
        .ok_or("versioned legacy upload completion omitted a concrete version")?
        .to_owned();
    let etag = complete.e_tag().ok_or("CompleteMultipartUpload omitted ETag")?.to_owned();
    let replay = complete_pending_multipart(&clients[0], PENDING_MULTIPART_BUCKET, PENDING_MULTIPART_KEY, pending).await?;
    assert_eq!(replay.version_id(), Some(version_id.as_str()));
    assert_eq!(replay.e_tag(), Some(etag.as_str()));
    let completed = CompletedPendingMultipart { version_id, etag, body };
    assert_resumed_multipart(clients, &completed).await?;
    Ok(completed)
}

async fn wait_for_multipart_quiescence(cluster: &RustFSTestClusterEnvironment, sources: &[PendingSourceBytes]) -> TestResult {
    let deadline = Instant::now() + LISTING_CONVERGENCE_TIMEOUT;
    loop {
        let pending_roots = sources.iter().filter(|source| source.root.exists()).count();
        let mut native_controls = 0;
        let mut remaining_upload_roots = 0;
        for volume in cluster.nodes.iter().flat_map(|node| &node.data_dirs) {
            let root = Path::new(volume).join(RUSTFS_META_MULTIPART_BUCKET);
            if !root.exists() {
                continue;
            }
            for entry in walkdir::WalkDir::new(root) {
                let entry = match entry {
                    Ok(entry) => entry,
                    Err(error)
                        if error
                            .io_error()
                            .is_some_and(|error| error.kind() == std::io::ErrorKind::NotFound) =>
                    {
                        native_controls += 1;
                        continue;
                    }
                    Err(error) => return Err(error.into()),
                };
                // Every fixture upload lives at MPI/<SHA>/<upload>. A born-only
                // source can contain just xl.meta and its data directory, with
                // no .mpu-* record. Only empty SHA parents may survive rollback.
                if entry.depth() == 2 || (entry.depth() == 1 && !entry.file_type().is_dir()) {
                    remaining_upload_roots += 1;
                }
                if entry.file_name().to_string_lossy().starts_with(".mpu-") {
                    native_controls += 1;
                }
            }
        }
        if pending_roots == 0 && remaining_upload_roots == 0 && native_controls == 0 {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "downgrade requires settled native MPU state: {pending_roots} fixed pending roots, {remaining_upload_roots} inventoried upload roots, {native_controls} native controls"
            )
            .into());
        }
        sleep(Duration::from_millis(500)).await;
    }
}

async fn mixed_multipart_before(
    deadline: Instant,
    node: usize,
    key: &str,
    upload: impl Future<Output = TestResult>,
) -> TestResult {
    tokio::time::timeout_at(deadline, upload)
        .await
        .map_err(|_| MultipartAcknowledgementDeadline {
            node,
            key: key.to_owned(),
        })?
}

#[derive(Debug)]
struct MultipartAcknowledgementDeadline {
    node: usize,
    key: String,
}

impl std::fmt::Display for MultipartAcknowledgementDeadline {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "node {}: multipart upload {MIXED_BUCKET}/{} acknowledgement deadline exceeded",
            self.node, self.key
        )
    }
}

impl std::error::Error for MultipartAcknowledgementDeadline {}

// Decode only the public lock snapshot fields. Unknown fields, including
// credential/configuration payloads, never enter the failure artifact.
#[derive(serde::Deserialize, serde::Serialize)]
struct MixedLockSnapshot {
    total: Option<u64>,
    truncated: Option<bool>,
    capability_note: Option<String>,
    locks: Vec<MixedLockEntry>,
}

#[derive(serde::Deserialize, serde::Serialize)]
struct MixedLockEntry {
    resource: Option<String>,
    bucket: Option<String>,
    object: Option<String>,
    version: Option<String>,
    owner: Option<String>,
    #[serde(rename = "type")]
    lock_type: Option<String>,
    priority: Option<String>,
    since: Option<String>,
    elapsed_secs: Option<u64>,
    ttl_secs: Option<u64>,
}

async fn read_mixed_lock_snapshot(response: &mut reqwest::Response) -> Result<MixedLockSnapshot, &'static str> {
    const MAX_BODY_BYTES: usize = 1024 * 1024;
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|_| "body_read_error")? {
        if chunk.len() > MAX_BODY_BYTES - body.len() {
            return Err("body_limit_exceeded");
        }
        body.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&body).map_err(|_| "invalid_lock_snapshot")
}

async fn probe_mixed_multipart_failure(
    cluster: &RustFSTestClusterEnvironment,
    node: usize,
    path: &str,
    head: bool,
    deadline: Instant,
) -> Value {
    let started = Instant::now();
    let method = if head { Method::HEAD } else { Method::GET };
    let mut observation = serde_json::json!({
        "node": node, "method": method.as_str(), "path": path,
        "status": null, "observation": "unknown", "timeout": false,
    });
    // The shared absolute deadline bounds both headers and a slow/infinite
    // response body; a send-only timeout would leave the cluster alive forever.
    let result = tokio::time::timeout_at(deadline, async {
        let mut response = signed_request(
            method,
            &format!("{}{path}", cluster.nodes[node].url.trim_end_matches('/')),
            &cluster.access_key,
            &cluster.secret_key,
            None,
            None,
        )
        .await
        .map_err(|_| "request_error")?;
        let status = response.status();
        observation["status"] = serde_json::json!(status.as_u16());
        if head {
            observation["observation"] = serde_json::json!(match status {
                StatusCode::OK => "visible",
                StatusCode::NOT_FOUND => "not_visible",
                _ => "unknown",
            });
            for (field, header) in [
                ("content_length", "content-length"),
                ("etag", "etag"),
                ("version", "x-amz-version-id"),
            ] {
                observation[field] = serde_json::json!(response.headers().get(header).and_then(|value| value.to_str().ok()));
            }
        } else if status == StatusCode::OK {
            observation["snapshot"] =
                serde_json::to_value(read_mixed_lock_snapshot(&mut response).await?).map_err(|_| "snapshot_encode_error")?;
            observation["observation"] = serde_json::json!("captured");
        }
        Ok::<_, &'static str>(())
    })
    .await;
    match result {
        Ok(Ok(())) => {}
        Ok(Err(reason)) => observation["error"] = serde_json::json!(reason),
        Err(_) => observation["timeout"] = serde_json::json!(true),
    }
    observation["elapsed_secs"] = serde_json::json!(started.elapsed().as_secs_f64());
    observation
}

async fn observe_mixed_multipart_failure(
    cluster: &RustFSTestClusterEnvironment,
    phase: &str,
    error: BoxError,
    log_dir: Option<&Path>,
) -> TestResult {
    if let (Some(log_dir), Some(failure)) = (log_dir, error.downcast_ref::<MultipartAcknowledgementDeadline>()) {
        let started = Instant::now();
        let deadline = started + Duration::from_secs(5);
        // Reserve the last second for serialization and bounded artifact I/O.
        // All probes run directly in this future; no diagnostic task is spawned.
        let network_deadline = started + Duration::from_secs(4);
        let path = format!("/{MIXED_BUCKET}/{}", failure.key);
        let _ = tokio::time::timeout_at(deadline, async {
            // Every node keeps its own lock table, and the writer that stalls is
            // usually a previous-release node; the current node's snapshot alone
            // cannot show which holder blocks the upload.
            let (heads, locks) = tokio::join!(
                futures::future::join_all((0..cluster.nodes.len()).map(|node| probe_mixed_multipart_failure(
                    cluster,
                    node,
                    &path,
                    true,
                    network_deadline
                ))),
                futures::future::join_all((0..cluster.nodes.len()).map(|node| probe_mixed_multipart_failure(
                    cluster,
                    node,
                    "/rustfs/admin/v3/top/locks?count=1000",
                    false,
                    network_deadline
                ))),
            );
            let snapshot = serde_json::json!({
                "phase": phase, "bucket": MIXED_BUCKET, "key": failure.key,
                "writer_node": failure.node, "heads": heads, "top_locks": locks,
            });
            let body = serde_json::to_vec_pretty(&snapshot).map_err(|_| ())?;
            tokio::fs::write(log_dir.join("mixed-multipart-failure.json"), body)
                .await
                .map_err(|_| ())
        })
        .await;
    }
    // Visibility and diagnostic failures cannot acknowledge the original MPU.
    Err(error)
}

/// Assert that `client` eventually lists exactly `expected` objects under
/// `{phase}/`, polling until [`LISTING_CONVERGENCE_TIMEOUT`].
///
/// A single-snapshot assertion here is racy by construction: each phase both
/// writes and lists within seconds of a node restart. While a peer still holds
/// the restarted node's drive in Suspect/Returning, strict-quorum listing
/// consults only the remaining three drives and drops any object that was
/// itself legally written at write quorum (3/4 drives) during an earlier
/// node's identical post-restart window — its xl.meta is then visible on only
/// two of the three consulted drives, below the required object quorum of
/// three. GET still succeeds for such objects; only the listing under-counts
/// until drive health converges. A genuine upgrade data-loss regression still
/// fails after the deadline.
async fn wait_for_phase_listing(client: &Client, phase: &str, expected: usize, context: &str) -> TestResult {
    let deadline = Instant::now() + LISTING_CONVERGENCE_TIMEOUT;
    loop {
        let listed = client
            .list_objects_v2()
            .bucket(MIXED_BUCKET)
            .prefix(format!("{phase}/"))
            .send()
            .await?;
        let count = listed.contents().len();
        if count == expected {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "{context}: listing under {phase}/ returned {count} of {expected} objects even after {}s of post-restart convergence",
                LISTING_CONVERGENCE_TIMEOUT.as_secs()
            )
            .into());
        }
        sleep(Duration::from_millis(500)).await;
    }
}

async fn exercise_mixed_cluster(
    cluster: &RustFSTestClusterEnvironment,
    phase: &str,
    current_node: usize,
    previous_node: usize,
) -> TestResult {
    let clients = cluster.create_all_clients()?;
    wait_for_upgrade_write_readiness(&clients, MIXED_BUCKET, phase, LISTING_CONVERGENCE_TIMEOUT).await?;
    wait_for_drive_readmission(&clients, MIXED_BUCKET, phase, DRIVE_READMISSION_TIMEOUT).await?;
    let current_client = &clients[current_node];
    let previous_client = &clients[previous_node];

    let current_key = format!("{phase}/written-by-current");
    let current_body = format!("{phase}: current RustFS build").into_bytes();
    current_client
        .put_object()
        .bucket(MIXED_BUCKET)
        .key(&current_key)
        .body(ByteStream::from(current_body.clone()))
        .send()
        .await?;
    assert_eq!(read_object(previous_client, MIXED_BUCKET, &current_key, None).await?.1, current_body);

    let previous_key = format!("{phase}/written-by-previous");
    let previous_body = format!("{phase}: previous RustFS release").into_bytes();
    previous_client
        .put_object()
        .bucket(MIXED_BUCKET)
        .key(&previous_key)
        .body(ByteStream::from(previous_body.clone()))
        .send()
        .await?;
    assert_eq!(read_object(current_client, MIXED_BUCKET, &previous_key, None).await?.1, previous_body);

    // Native MPU mutations are operationally paused while RPC generations
    // differ. Keep the same mixed traffic volume with ordinary PUT instead.
    let written_keys = write_mixed_put_load(&clients, phase).await?;
    let expected_count = written_keys.len() + 2;
    verify_load_bodies(&clients, phase, "put").await?;
    for (label, client) in [("current", current_client), ("previous", previous_client)] {
        assert_eq!(
            read_object(client, MIXED_BUCKET, PREVIOUS_RELEASE_SEED_KEY, None).await?.1,
            UPGRADE_READINESS_BODY,
            "{phase}: the {label} node must retain the previous-release seed"
        );
        wait_for_phase_listing(
            client,
            phase,
            expected_count,
            &format!("the {label} RustFS version must stream the complete mixed-version listing"),
        )
        .await?;
    }

    let last_put_key = format!("{phase}/put/{:02}/{:02}", MULTIPART_WORKERS - 1, MULTIPART_UPLOADS_PER_WORKER - 1);
    assert_eq!(
        read_object(previous_client, MIXED_BUCKET, &last_put_key, None).await?.1,
        vec![u8::try_from(MULTIPART_WORKERS - 1)?; 64 * 1024]
    );

    // Every online participant must understand the delete decision protocol.
    // An upgrade phase can serve reads/writes while intentionally refusing a
    // modern DELETE; rejecting it must leave the old writer's seed intact.
    let error = upgrade_probe_client(current_client)
        .delete_object()
        .bucket(MIXED_BUCKET)
        .key(PREVIOUS_RELEASE_SEED_KEY)
        .send()
        .await
        .expect_err("a modern DELETE must fail closed while an old participant is online");
    assert_eq!(error.raw_response().map(|response| response.status().as_u16()), Some(503), "{error:?}");
    assert_eq!(error.as_service_error().and_then(ProvideErrorMetadata::code), Some("ServiceUnavailable"));
    for (node, client) in clients.iter().enumerate() {
        assert_eq!(
            read_object(client, MIXED_BUCKET, PREVIOUS_RELEASE_SEED_KEY, None).await?.1,
            UPGRADE_READINESS_BODY,
            "{phase}: rejected DELETE must preserve the seed through node {node}"
        );
    }

    Ok(())
}

fn upgrade_probe_client(client: &Client) -> Client {
    Client::from_conf(
        client
            .config()
            .to_builder()
            .retry_config(aws_sdk_s3::config::retry::RetryConfig::standard().with_max_attempts(1))
            .build(),
    )
}

pub(crate) async fn wait_for_upgrade_write_readiness(
    clients: &[Client],
    bucket: &str,
    phase: &str,
    budget: Duration,
) -> TestResult {
    // ListBuckets can succeed before peers recover a restarted disk. Cluster
    // health also accepts Returning disks whose write health is still FAULTY.
    // Probe every writer outside the asserted phase prefix; compatibility
    // writes still execute once and retain their original assertions.
    wait_for_upgrade_writes(clients, bucket, phase, "write readiness", None, budget).await
}

/// Block until every node commits a parity-0 write, which needs all of the
/// node's own drive handles.
///
/// Write readiness proves that a node accepts a standard write at quorum; it
/// does not prove that the node has re-admitted the drives it marked Offline
/// while a peer restarted. A writer that still excludes one drive commits at
/// exactly three of four drives and cannot ride out any other slow drive. The
/// cluster must run with [`ALL_DRIVES_STORAGE_CLASS_ENV`]; a write with the
/// reduced-redundancy class then fails with 503 `ServiceUnavailable` until the
/// serving node writes through every drive, on the previous release and on
/// the current build alike.
pub(crate) async fn wait_for_drive_readmission(clients: &[Client], bucket: &str, context: &str, budget: Duration) -> TestResult {
    wait_for_upgrade_writes(
        clients,
        bucket,
        context,
        "drive re-admission",
        Some(StorageClass::ReducedRedundancy),
        budget,
    )
    .await
}

async fn wait_for_upgrade_writes(
    clients: &[Client],
    bucket: &str,
    phase: &str,
    probe: &str,
    storage_class: Option<StorageClass>,
    budget: Duration,
) -> TestResult {
    let deadline = Instant::now() + budget;
    for (node, client) in clients.iter().enumerate() {
        let client = upgrade_probe_client(client);
        let key = match storage_class {
            Some(_) => format!(".upgrade-readiness/{phase}/all-drives/node-{node}"),
            None => format!(".upgrade-readiness/{phase}/node-{node}"),
        };
        let mut last_response = "no response".to_string();
        loop {
            if Instant::now() >= deadline {
                return Err(format!("{phase}: node {node} {probe} deadline exceeded; last response: {last_response}").into());
            }
            let response = tokio::time::timeout_at(
                deadline,
                client
                    .put_object()
                    .bucket(bucket)
                    .key(&key)
                    .set_storage_class(storage_class.clone())
                    .body(ByteStream::from_static(UPGRADE_READINESS_BODY))
                    .send(),
            )
            .await;
            match response {
                Ok(Ok(_)) => break,
                Ok(Err(error)) => {
                    if error.raw_response().map(|response| response.status().as_u16()) != Some(503)
                        || error.as_service_error().and_then(ProvideErrorMetadata::code) != Some("ServiceUnavailable")
                    {
                        return Err(format!("{phase}: node {node} {probe} failed: {error:?}").into());
                    }
                    last_response = format!("{error:?}");
                }
                Err(_) => {
                    return Err(format!(
                        "{phase}: node {node} {probe} deadline exceeded during PutObject; last response: {last_response}"
                    )
                    .into());
                }
            }
            tokio::time::sleep_until(deadline.min(Instant::now() + Duration::from_millis(500))).await;
        }
    }
    Ok(())
}

fn previous_baseline_writers(clients: &[Client], restarted_node: usize) -> &[Client] {
    // Node 0 has persisted the seed. With restart order [1, 2, 3, 0], only
    // this prefix has also cleared rc.5's startup write fence. Every RRS
    // probe still needs all drives, including those of not-yet-restarted peers.
    let ready_end = if restarted_node == 0 {
        clients.len()
    } else {
        restarted_node + 1
    };
    &clients[..ready_end]
}

async fn prepare_previous_release_baseline(cluster: &mut RustFSTestClusterEnvironment, previous_binary: &Path) -> TestResult {
    prepare_release_baseline(cluster, previous_binary, "previous").await
}

async fn prepare_release_baseline(
    cluster: &mut RustFSTestClusterEnvironment,
    previous_binary: &Path,
    prefix: &str,
) -> TestResult {
    let clients: Vec<_> = cluster.create_all_clients()?.iter().map(upgrade_probe_client).collect();
    // rc.5 can latch a non-elected node's write fence while its first startup
    // waits for pool metadata (#7473). First prove the elected writer can
    // persist data, then restart each old process once with three peers still
    // readable. The same preparation is reused only after a quiescent homogeneous rollback.
    wait_for_upgrade_write_readiness(&clients[..1], MIXED_BUCKET, &format!("{prefix}-seed"), LISTING_CONVERGENCE_TIMEOUT).await?;
    // A node that just came back is still replaying its startup traffic against
    // every peer. Killing the next node during that burst gives the fresh node
    // two failures in a row and it drops that peer's drive to Offline, where
    // only the slow recovery probe can bring it back. Let every writable node
    // re-admit every drive before the next kill. Nodes still awaiting their
    // restart may retain the startup write fence and cannot coordinate a PUT.
    for node in [1, 2, 3, 0] {
        cluster.stop_node(node)?;
        cluster.start_node_from_binary(node, previous_binary).await?;
        let context = format!("{prefix}-restart-{node}");
        wait_for_upgrade_write_readiness(
            std::slice::from_ref(&clients[node]),
            MIXED_BUCKET,
            &context,
            LISTING_CONVERGENCE_TIMEOUT,
        )
        .await?;
        wait_for_drive_readmission(
            previous_baseline_writers(&clients, node),
            MIXED_BUCKET,
            &context,
            DRIVE_READMISSION_TIMEOUT,
        )
        .await?;
    }

    wait_for_upgrade_write_readiness(&clients, MIXED_BUCKET, &format!("{prefix}-baseline"), LISTING_CONVERGENCE_TIMEOUT).await?;
    for (reader, client) in clients.iter().enumerate() {
        assert_eq!(
            read_object(client, MIXED_BUCKET, &format!(".upgrade-readiness/{prefix}-seed/node-0"), None)
                .await?
                .1,
            UPGRADE_READINESS_BODY,
            "previous-release node {reader} must retain the seed across its rolling restart"
        );
        for writer in 0..clients.len() {
            let key = format!(".upgrade-readiness/{prefix}-baseline/node-{writer}");
            assert_eq!(
                read_object(client, MIXED_BUCKET, &key, None).await?.1,
                UPGRADE_READINESS_BODY,
                "previous-release node {reader} must read baseline data from writer {writer}"
            );
        }
    }
    Ok(())
}

/// The drive re-admission gate assumes that a parity-0 write needs every
/// drive. Prove it on the workspace build: with one node down a standard
/// write still commits, the reduced-redundancy write fails with 503, and once
/// the node is back the gate converges within its budget.
#[tokio::test]
async fn parity_zero_write_needs_every_drive() -> TestResult {
    init_logging();
    let mut cluster = RustFSTestClusterEnvironment::new(MIXED_NODE_COUNT).await?;
    cluster.set_env("RUST_LOG", "rustfs=warn,rustfs_notify=warn");
    cluster.set_env(ALL_DRIVES_STORAGE_CLASS_ENV.0, ALL_DRIVES_STORAGE_CLASS_ENV.1);
    cluster.start().await?;
    cluster.create_test_bucket(MIXED_BUCKET).await?;
    let clients = cluster.create_all_clients()?;
    wait_for_upgrade_write_readiness(&clients, MIXED_BUCKET, "parity-zero-seed", LISTING_CONVERGENCE_TIMEOUT).await?;
    wait_for_drive_readmission(&clients, MIXED_BUCKET, "parity-zero-healthy", DRIVE_READMISSION_TIMEOUT).await?;

    cluster.stop_node(3)?;
    let writer = upgrade_probe_client(&clients[0]);
    writer
        .put_object()
        .bucket(MIXED_BUCKET)
        .key("parity-zero/standard-with-one-node-down")
        .body(ByteStream::from_static(UPGRADE_READINESS_BODY))
        .send()
        .await?;
    let error = writer
        .put_object()
        .bucket(MIXED_BUCKET)
        .key("parity-zero/all-drives-with-one-node-down")
        .storage_class(StorageClass::ReducedRedundancy)
        .body(ByteStream::from_static(UPGRADE_READINESS_BODY))
        .send()
        .await
        .expect_err("a parity-0 write must not commit at three of four drives");
    assert_eq!(error.raw_response().map(|response| response.status().as_u16()), Some(503), "{error:?}");
    assert_eq!(
        error.as_service_error().and_then(ProvideErrorMetadata::code),
        Some("ServiceUnavailable"),
        "{error:?}"
    );

    cluster.start_node(3).await?;
    wait_for_drive_readmission(&clients, MIXED_BUCKET, "parity-zero-returned", DRIVE_READMISSION_TIMEOUT).await?;
    Ok(())
}

#[cfg(test)]
mod upgrade_write_readiness_tests {
    use super::*;
    use crate::fake_s3_target::FaultAction;
    use aws_sdk_s3::config::interceptors::BeforeTransmitInterceptorContextRef;
    use aws_sdk_s3::config::{ConfigBag, Intercept, RuntimeComponents};

    #[derive(Debug)]
    struct RequireAllDrivesProbe;

    impl Intercept for RequireAllDrivesProbe {
        fn name(&self) -> &'static str {
            "require-all-drives-probe"
        }

        fn read_before_transmit(
            &self,
            context: &BeforeTransmitInterceptorContextRef<'_>,
            _runtime_components: &RuntimeComponents,
            _cfg: &mut ConfigBag,
        ) -> Result<(), BoxError> {
            assert_eq!(context.request().method(), "PUT");
            assert_eq!(
                context.request().headers().get("x-amz-storage-class"),
                Some("REDUCED_REDUNDANCY"),
                "every admitted coordinator must use the all-drive storage class"
            );
            Ok(())
        }
    }

    #[tokio::test]
    async fn previous_baseline_probes_only_writable_coordinators_until_all_restart() -> TestResult {
        let mut targets = Vec::new();
        let mut clients = Vec::new();
        for _ in 0..MIXED_NODE_COUNT {
            let target = FakeS3Target::start().await?;
            target.create_bucket(MIXED_BUCKET);
            clients.push(Client::from_conf(
                fake_source_client(&target)
                    .config()
                    .to_builder()
                    .interceptor(RequireAllDrivesProbe)
                    .build(),
            ));
            targets.push(target);
        }
        for target in &targets[2..] {
            target.inject(FakeTargetOperation::PutObject, FaultAction::Status(StatusCode::INTERNAL_SERVER_ERROR), 10);
        }

        // Reproduce the original all-coordinator gate: node 0 and restarted
        // node 1 can write, but unrestarted node 2 still returns a latched 500.
        let error = wait_for_drive_readmission(&clients, MIXED_BUCKET, "premature", Duration::from_secs(5))
            .await
            .expect_err("an unrestarted coordinator must retain its startup write failure");
        assert!(error.to_string().contains("node 2 drive re-admission failed"), "{error}");
        let failed = targets[2].requests();
        assert_eq!(failed.len(), 1, "the startup 500 must fail without retrying");
        assert_eq!(failed[0].fault, Some(FaultAction::Status(StatusCode::INTERNAL_SERVER_ERROR)));
        for target in &targets {
            target.take_requests();
        }

        for (restarted_node, expected_writers) in [(1, 2), (2, 3), (3, 4), (0, 4)] {
            // A restart clears only this process's startup write fence.
            targets[restarted_node].clear_faults();
            let phase = format!("previous-restart-{restarted_node}");
            wait_for_drive_readmission(
                previous_baseline_writers(&clients, restarted_node),
                MIXED_BUCKET,
                &phase,
                Duration::from_secs(5),
            )
            .await?;
            for (node, target) in targets.iter().enumerate() {
                let key = format!(".upgrade-readiness/{phase}/all-drives/node-{node}");
                let admitted = node < expected_writers;
                assert_eq!(
                    target.count_requests(FakeTargetOperation::PutObject, &key),
                    usize::from(admitted),
                    "{phase}: only writable node {node} may coordinate an all-drive probe"
                );
                assert_eq!(target.has_object(MIXED_BUCKET, &key), admitted);
                assert_eq!(target.take_requests().len(), usize::from(admitted));
            }
        }

        // Once every node is admitted, its permanent errors must still fail
        // the full gate immediately rather than being skipped or retried.
        let phase = "previous-ready-error";
        let key = format!(".upgrade-readiness/{phase}/all-drives/node-3");
        targets[3].inject_for_key(
            FakeTargetOperation::PutObject,
            &key,
            FaultAction::Status(StatusCode::INTERNAL_SERVER_ERROR),
            1,
        );
        let error =
            wait_for_drive_readmission(previous_baseline_writers(&clients, 0), MIXED_BUCKET, phase, Duration::from_secs(5))
                .await
                .expect_err("a recovered coordinator's 500 must still fail the final gate");
        assert!(error.to_string().contains("node 3 drive re-admission failed"), "{error}");
        assert_eq!(targets[3].count_requests(FakeTargetOperation::PutObject, &key), 1);
        assert!(!targets[3].has_object(MIXED_BUCKET, &key));
        for target in targets {
            target.shutdown().await;
        }
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn waits_for_each_writer_after_metadata_is_ready() -> TestResult {
        use aws_sdk_s3::config::{Credentials, Region, RequestChecksumCalculation};
        use aws_smithy_http_client::test_util::{ReplayEvent, StaticReplayClient};
        use aws_smithy_types::body::SdkBody;

        let bucket = "distributed-upgrade-history";
        let phase = "one-previous-node";
        let first_key = format!(".upgrade-readiness/{phase}/node-0");
        let second_key = format!(".upgrade-readiness/{phase}/node-1");
        let endpoint = "http://upgrade-readiness.example.invalid";
        let event = |method: Method, key: &str, status: StatusCode| {
            let (body, query) = if method == Method::PUT {
                (SdkBody::from(UPGRADE_READINESS_BODY), "?x-id=PutObject")
            } else {
                (SdkBody::empty(), "")
            };
            ReplayEvent::new(
                http::Request::builder()
                    .method(method)
                    .uri(format!("{endpoint}/{bucket}/{key}{query}"))
                    .body(body)
                    .unwrap(),
                http::Response::builder()
                    .status(status)
                    .header("content-type", "application/xml")
                    .body(if status == StatusCode::SERVICE_UNAVAILABLE {
                        SdkBody::from("<Error><Code>ServiceUnavailable</Code></Error>")
                    } else {
                        SdkBody::empty()
                    })
                    .unwrap(),
            )
        };
        // Keep transport scheduling outside the probe's time budget. The
        // replay checks every writer's key and body without real network I/O.
        let transport = StaticReplayClient::new(vec![
            event(Method::HEAD, "", StatusCode::OK),
            event(Method::PUT, &first_key, StatusCode::SERVICE_UNAVAILABLE),
            event(Method::PUT, &first_key, StatusCode::SERVICE_UNAVAILABLE),
            event(Method::PUT, &first_key, StatusCode::OK),
            event(Method::PUT, &second_key, StatusCode::SERVICE_UNAVAILABLE),
            event(Method::PUT, &second_key, StatusCode::OK),
        ]);
        let client = Client::from_conf(
            aws_sdk_s3::Config::builder()
                .behavior_version_latest()
                .credentials_provider(Credentials::new(FAKE_ACCESS_KEY, FAKE_SECRET_KEY, None, None, "readiness-test"))
                .region(Region::new("us-east-1"))
                .endpoint_url(endpoint)
                .force_path_style(true)
                .retry_config(aws_sdk_s3::config::retry::RetryConfig::disabled())
                .request_checksum_calculation(RequestChecksumCalculation::WhenRequired)
                .http_client(transport.clone())
                .build(),
        );
        client.head_bucket().bucket(bucket).send().await?;

        // Metadata readiness does not prove that a data write can succeed.
        let premature = client
            .put_object()
            .bucket(bucket)
            .key(&first_key)
            .body(ByteStream::from_static(UPGRADE_READINESS_BODY))
            .send()
            .await
            .expect_err("metadata readiness does not prove write readiness");
        assert_eq!(premature.raw_response().map(|response| response.status().as_u16()), Some(503));

        let started = Instant::now();
        wait_for_upgrade_write_readiness(&[client.clone(), client], bucket, phase, Duration::from_secs(5)).await?;
        assert_eq!(started.elapsed(), Duration::from_secs(1), "each unavailable writer must be polled again");
        transport.assert_requests_match(&[]);
        assert_eq!(
            transport
                .actual_requests()
                .map(|request| request.method().to_string())
                .collect::<Vec<_>>(),
            ["HEAD", "PUT", "PUT", "PUT", "PUT", "PUT"],
        );
        Ok(())
    }

    #[tokio::test]
    async fn rejects_permanent_errors_without_sdk_retries() -> TestResult {
        let target = FakeS3Target::start().await?;
        target.create_bucket(MIXED_BUCKET);
        let client = Client::from_conf(
            fake_source_client(&target)
                .config()
                .to_builder()
                .retry_config(aws_sdk_s3::config::retry::RetryConfig::standard().with_max_attempts(3))
                .build(),
        );
        for status in [
            StatusCode::INTERNAL_SERVER_ERROR,
            StatusCode::FORBIDDEN,
            StatusCode::NOT_FOUND,
        ] {
            let phase = format!("permanent-{}", status.as_u16());
            let key = format!(".upgrade-readiness/{phase}/node-0");
            target.inject_for_key(FakeTargetOperation::PutObject, &key, FaultAction::Status(status), 1);
            let error =
                wait_for_upgrade_write_readiness(std::slice::from_ref(&client), MIXED_BUCKET, &phase, Duration::from_secs(5))
                    .await
                    .expect_err("a permanent error must not be retried into success");
            assert!(error.to_string().contains("node 0 write readiness failed"), "{error}");
            assert_eq!(target.count_requests(FakeTargetOperation::PutObject, &key), 1);
            assert!(!target.has_object(MIXED_BUCKET, &key));
        }
        target.shutdown().await;
        Ok(())
    }

    #[tokio::test]
    async fn transient_errors_stop_at_the_deadline() -> TestResult {
        let target = FakeS3Target::start().await?;
        target.create_bucket(MIXED_BUCKET);
        let client = fake_source_client(&target);
        client.head_bucket().bucket(MIXED_BUCKET).send().await?;
        let phase = "deadline";
        let key = format!(".upgrade-readiness/{phase}/node-0");
        target.inject_for_key(
            FakeTargetOperation::PutObject,
            &key,
            FaultAction::Status(StatusCode::SERVICE_UNAVAILABLE),
            10,
        );
        let error = wait_for_upgrade_write_readiness(&[client], MIXED_BUCKET, phase, Duration::from_secs(1))
            .await
            .expect_err("persistent unavailability must exhaust the shared deadline");
        // Transport scheduling consumes the same budget; do not require a
        // response to reach the fake target before the deadline on a busy host.
        assert!(error.to_string().contains("deadline exceeded"), "{error}");
        target.shutdown().await;
        Ok(())
    }

    #[tokio::test]
    async fn in_flight_requests_are_bounded_by_the_deadline() -> TestResult {
        let target = FakeS3Target::start().await?;
        target.create_bucket(MIXED_BUCKET);
        let client = fake_source_client(&target);
        client.head_bucket().bucket(MIXED_BUCKET).send().await?;
        let phase = "stalled";
        let key = format!(".upgrade-readiness/{phase}/node-0");
        target.inject_for_key(FakeTargetOperation::PutObject, &key, FaultAction::Stall(Duration::from_secs(30)), 1);
        let error = wait_for_upgrade_write_readiness(std::slice::from_ref(&client), MIXED_BUCKET, phase, Duration::from_secs(1))
            .await
            .expect_err("a stalled request must not outlive the readiness deadline");
        assert!(error.to_string().contains("deadline exceeded during PutObject"), "{error}");

        let part = vec![2_u8; 64 * 1024];
        let successful_key = "stalled/multipart/02/00";
        mixed_multipart_before(
            Instant::now() + MIXED_MULTIPART_UPLOAD_TIMEOUT,
            2,
            successful_key,
            write_multipart(&client, MIXED_BUCKET, successful_key, std::slice::from_ref(&part)),
        )
        .await?;
        assert_eq!(read_object(&client, MIXED_BUCKET, successful_key, None).await?.1, part);

        let stalled_key = "stalled/multipart/02/01";
        target.inject_for_key(
            FakeTargetOperation::CompleteMultipartUpload,
            stalled_key,
            FaultAction::Stall(Duration::from_secs(30)),
            1,
        );
        let error = {
            let upload = write_multipart(&client, MIXED_BUCKET, stalled_key, std::slice::from_ref(&part));
            tokio::pin!(upload);
            tokio::select! {
                result = &mut upload => panic!("stalled completion unexpectedly returned: {result:?}"),
                observed = tokio::time::timeout(Duration::from_secs(10), async {
                    while target.count_requests(FakeTargetOperation::CompleteMultipartUpload, stalled_key) == 0
                        || !target.has_object(MIXED_BUCKET, stalled_key)
                    {
                        sleep(Duration::from_millis(10)).await;
                    }
                }) => observed.expect("multipart completion must reach and commit on the fake peer"),
            }
            // An already committed object is not an acknowledged client success.
            // Expire only after observing the real completion request, so host
            // scheduling cannot turn this into a timeout before admission.
            tokio::time::timeout(Duration::from_secs(1), mixed_multipart_before(Instant::now(), 2, stalled_key, upload))
                .await
                .expect("expired deadline must stop the in-flight completion")
                .expect_err("unacknowledged completion must fail even if the peer committed")
        };
        assert!(error.to_string().contains("node 2"), "{error}");
        assert!(error.to_string().contains(stalled_key), "{error}");
        assert!(error.to_string().contains("acknowledgement deadline exceeded"), "{error}");
        assert_eq!(target.count_requests(FakeTargetOperation::CompleteMultipartUpload, stalled_key), 1);

        let failure = error
            .downcast_ref::<MultipartAcknowledgementDeadline>()
            .expect("the timeout must retain typed writer/object context");
        assert_eq!(failure.node, 2);
        assert_eq!(failure.key, stalled_key);
        let original_message = error.to_string();
        // These four probe addresses share an in-process peer. This verifies
        // bounded diagnostic fan-out, not a four-node RustFS cluster.
        let mut probes = RustFSTestClusterEnvironment::new(MIXED_NODE_COUNT).await?;
        probes.access_key = FAKE_ACCESS_KEY.to_owned();
        probes.secret_key = FAKE_SECRET_KEY.to_owned();
        for node in &mut probes.nodes {
            node.url = target.endpoint().to_owned();
        }
        let log_dir = tempfile::tempdir()?;
        target.inject_for_key(
            FakeTargetOperation::HeadObject,
            stalled_key,
            FaultAction::Stall(Duration::from_secs(30)),
            1,
        );
        let error = tokio::time::timeout(
            Duration::from_secs(7),
            observe_mixed_multipart_failure(&probes, phase, error, Some(log_dir.path())),
        )
        .await
        .expect("the shared diagnostic budget must include artifact writing")
        .expect_err("observing committed data cannot acknowledge the stalled upload");
        assert_eq!(error.to_string(), original_message);
        assert!(error.downcast_ref::<MultipartAcknowledgementDeadline>().is_some());
        let snapshot: Value =
            serde_json::from_slice(&tokio::fs::read(log_dir.path().join("mixed-multipart-failure.json")).await?)?;
        assert_eq!(snapshot["key"], stalled_key);
        assert_eq!(snapshot["writer_node"], 2);
        let heads = snapshot["heads"]
            .as_array()
            .expect("failure artifact must retain every probe");
        assert_eq!(heads.len(), MIXED_NODE_COUNT);
        assert_eq!(
            heads
                .iter()
                .filter(|head| head["timeout"] == true && head["observation"] == "unknown")
                .count(),
            1
        );
        assert_eq!(
            heads
                .iter()
                .filter(|head| head["status"] == 200 && head["observation"] == "visible")
                .count(),
            3
        );
        assert_eq!(target.count_requests(FakeTargetOperation::HeadObject, stalled_key), MIXED_NODE_COUNT);
        assert_eq!(target.count_requests(FakeTargetOperation::CompleteMultipartUpload, stalled_key), 1);
        let locks = snapshot["top_locks"]
            .as_array()
            .expect("failure artifact must retain every node's lock probe");
        assert_eq!(locks.len(), MIXED_NODE_COUNT);
        for (node, probe) in locks.iter().enumerate() {
            assert_eq!(probe["node"], node);
            assert_eq!(probe["observation"], "unknown", "the fake peer has no admin lock API");
        }

        let locks: MixedLockSnapshot = serde_json::from_value(serde_json::json!({
            "total": 1, "truncated": true, "capability_note": "partial snapshot",
            "credentials": { "secret_key": "must not persist" },
            "locks": [{
                "resource": format!("{MIXED_BUCKET}/{stalled_key}"), "owner": "writer", "type": "WRITE",
                "elapsed_secs": 30, "ttl_secs": 1, "merged_config": { "secret_key": "must not persist" },
            }],
        }))?;
        let safe_locks = serde_json::to_value(locks)?;
        assert_eq!(safe_locks["total"], 1);
        assert_eq!(safe_locks["truncated"], true);
        assert_eq!(safe_locks["locks"][0]["owner"], "writer");
        assert_eq!(safe_locks["locks"][0]["elapsed_secs"], 30);
        assert!(safe_locks.get("credentials").is_none());
        assert!(safe_locks["locks"][0].get("merged_config").is_none());

        let mut body = br#"{"locks":[]}"#.to_vec();
        body.resize(1024 * 1024, b' ');
        let mut at_limit = reqwest::Response::from(http::Response::new(body.clone()));
        assert!(read_mixed_lock_snapshot(&mut at_limit).await?.locks.is_empty());
        body.push(b' ');
        let mut oversized = reqwest::Response::from(http::Response::new(body));
        assert_eq!(read_mixed_lock_snapshot(&mut oversized).await.err(), Some("body_limit_exceeded"));
        target.shutdown().await;
        Ok(())
    }
}

/// Pins the published old writer's limitation and the supported recovery
/// procedure. This is not a promise that mixed-version ODM is supported.
/// Replace the loss assertion when ODM gains independent persistence;
/// preserving configuration across rc.5 writes is then an improvement.
#[tokio::test]
#[ignore = "requires the pinned 1.0.0-rc.5 release binary"]
async fn rc5_rollback_requires_restoring_odm_configuration() -> TestResult {
    init_logging();
    let previous_binary = source_binary()?;
    let version = tokio::process::Command::new(&previous_binary)
        .arg("--version")
        .output()
        .await?;
    assert!(version.status.success(), "previous binary must report its version");
    assert!(
        String::from_utf8(version.stdout)?.contains(RC5_COMMIT),
        "this compatibility scenario requires the published rc.5 writer"
    );
    let mut env = OdmTestEnv::start().await?;
    let bucket = "odm-rc5-rollback";
    let source_bucket = "odm-rc5-source";
    env.source.create_bucket_with_mode(source_bucket, BucketMode::Unversioned);
    env.seed_source(
        source_bucket,
        &[SeedObject::new(
            "source-only",
            bytes::Bytes::from_static(b"source read after recovery"),
        )],
    );
    env.rustfs.create_test_bucket(bucket).await?;
    let saved_config = env.fake_source_spec(source_bucket);
    assert_eq!(env.configure_source(bucket, &saved_config).await?.status, 200);
    let before = env.get_config(bucket).await?;
    assert_eq!(before.status, 200);
    let expected_config = before
        .json()?
        .get("config")
        .cloned()
        .ok_or("configuration response omitted config")?;
    env.client
        .put_object()
        .bucket(bucket)
        .key("local")
        .body(ByteStream::from_static(b"local data survives rollback"))
        .send()
        .await?;
    env.rustfs.restart_server_preserving_data(vec![], ODM_SERVER_ENV).await?;
    let restarted = env.get_config(bucket).await?;
    assert_eq!(restarted.status, 200, "a current writer preserves ODM across restart");
    assert_eq!(restarted.json()?.get("config"), Some(&expected_config));

    restart_from_binary(&mut env.rustfs, &previous_binary, &[]).await?;
    env.client
        .put_bucket_tagging()
        .bucket(bucket)
        .tagging(
            Tagging::builder()
                .tag_set(Tag::builder().key("writer").value("rc5").build()?)
                .build()?,
        )
        .send()
        .await?;
    env.rustfs.restart_server_preserving_data(vec![], ODM_SERVER_ENV).await?;
    let missing = env.get_config(bucket).await?;
    assert_eq!(missing.status, 404, "rc.5 rewrites metadata without ODM keys");
    assert!(missing.body.contains("NoSuchConfiguration"));
    assert_eq!(read_object(&env.client, bucket, "local", None).await?.1, b"local data survives rollback");
    let tags = env.client.get_bucket_tagging().bucket(bucket).send().await?;
    assert!(tags.tag_set().iter().any(|tag| tag.key() == "writer" && tag.value() == "rc5"));

    assert_eq!(
        env.configure_source(bucket, &saved_config).await?.status,
        200,
        "restore from saved full configuration"
    );
    env.rustfs.restart_server_preserving_data(vec![], ODM_SERVER_ENV).await?;
    let restored = env.get_config(bucket).await?;
    assert_eq!(restored.status, 200, "restored ODM configuration persists");
    assert_eq!(restored.json()?.get("config"), Some(&expected_config));
    env.wait_until_source_consulted(bucket).await?;
    assert_eq!(
        read_object(&env.client, bucket, "source-only", None).await?.1,
        b"source read after recovery"
    );
    Ok(())
}

struct Rc5CheckpointCapture {
    disk: PathBuf,
    path: PathBuf,
    task_id: String,
    bytes: Vec<u8>,
}

async fn capture_rc5_checkpoint(drives: &[PathBuf], writer_pid: u32) -> Result<Rc5CheckpointCapture, BoxError> {
    let has_signed_progress = |value: &Value| {
        value["schema_version"].as_u64() == Some(6)
            && value["successful_objects"].as_u64().is_some_and(|count| count > 0)
            && value["processed_objects"]
                .as_array()
                .is_some_and(|objects| !objects.is_empty())
            && value["integrity_digest"].as_str().is_some_and(|digest| !digest.is_empty())
    };
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            for drive in drives {
                let mut entries = match tokio::fs::read_dir(drive.join(".rustfs.sys/buckets")).await {
                    Ok(entries) => entries,
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                    Err(error) => return Err(error.into()),
                };
                while let Some(entry) = entries.next_entry().await? {
                    if !entry.file_name().to_string_lossy().ends_with("_ahm_checkpoint.json") {
                        continue;
                    }
                    let metadata = entry.metadata().await?;
                    if !metadata.is_file() || metadata.len() > 1024 * 1024 {
                        return Err("the old checkpoint is not a bounded regular file".into());
                    }
                    let bytes = match tokio::fs::read(entry.path()).await {
                        Ok(bytes) => bytes,
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                        Err(error) => return Err(error.into()),
                    };
                    let Ok(value) = serde_json::from_slice::<Value>(&bytes) else {
                        continue;
                    };
                    if !has_signed_progress(&value) {
                        continue;
                    }

                    // Freeze only this fixture's writer before copying its actual
                    // bytes. The environment's normal stop path kills a stopped child.
                    signal_process(writer_pid, "STOP")?;
                    let bytes = tokio::fs::read(entry.path()).await?;
                    let value: Value = serde_json::from_slice(&bytes)?;
                    if !has_signed_progress(&value) {
                        return Err("the stopped rc.5 writer did not retain signed checkpoint progress".into());
                    }
                    let task_id = value["task_id"].as_str().ok_or("the old checkpoint has no task id")?;
                    uuid::Uuid::parse_str(task_id)?;
                    if entry.file_name() != std::ffi::OsStr::new(&format!("{task_id}_ahm_checkpoint.json")) {
                        return Err("the old checkpoint task id does not match its filename".into());
                    }
                    return Ok(Rc5CheckpointCapture {
                        disk: drive.clone(),
                        path: entry.path(),
                        task_id: task_id.to_owned(),
                        bytes,
                    });
                }
            }
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .map_err(|_| "rc.5 did not produce a signed checkpoint with progress within 60 seconds")?
}

fn rc5_checkpoint_object(index: usize) -> (String, Vec<u8>) {
    (
        format!("object-{index:05}.bin"),
        format!("rc.5 checkpoint upgrade object {index:05}\n")
            .repeat(256)
            .into_bytes(),
    )
}

fn checkpoint_binary_sha256(path: &Path) -> Result<String, BoxError> {
    use sha2::Digest;
    use std::io::Read;

    let mut input = std::fs::File::open(path)?;
    let mut digest = sha2::Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let count = input.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        digest.update(&buffer[..count]);
    }
    Ok(hex_simd::encode_to_string(digest.finalize(), hex_simd::AsciiCase::Lower))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires the pinned 1.0.0-rc.5 release binary"]
async fn rc5_checkpoint_upgrade_rewinds_signed_progress_without_losing_objects() -> TestResult {
    use crate::storage_api::upgrade_compatibility::{DiskOption, Endpoint, new_disk};
    use futures::{StreamExt, TryStreamExt, stream};
    use rustfs_heal::heal::CheckpointManager;

    const BUCKET: &str = "upgrade-rc5-checkpoint";
    const OBJECTS: usize = 1600;

    init_logging();
    let previous_binary = source_binary()?;
    assert_eq!(binary_source_revision(&previous_binary).await?, RC5_COMMIT);
    let current_binary = rustfs_binary_path();
    let evidence_dir = std::env::var_os(G09_EVIDENCE_DIR_ENV)
        .map(|root| PathBuf::from(root).join(format!("rc5-checkpoint-upgrade-{}", uuid::Uuid::new_v4())));
    if let Some(directory) = &evidence_dir {
        std::fs::create_dir_all(directory)?;
    }

    // Declare the server after its extra drives so error unwinding stops it
    // before the fixture directories are removed.
    let fixture = tempfile::TempDir::new()?;
    let extra_drives = (0..3)
        .map(|index| {
            let path = fixture.path().join(format!("drive-{index}"));
            std::fs::create_dir(&path)?;
            Ok(path.to_str().ok_or("fixture drive path is not UTF-8")?.to_owned())
        })
        .collect::<Result<Vec<_>, BoxError>>()?;
    let mut env = RustFSTestEnvironment::new().await?;
    let log_path = evidence_dir.as_deref().unwrap_or(fixture.path()).join("server.log");
    env.capture_log_path = Some(log_path.to_str().ok_or("fixture log path is not UTF-8")?.to_owned());
    let server_env = [
        ("RUSTFS_UNSAFE_BYPASS_DISK_CHECK", "true"),
        ("RUSTFS_DURABILITY_MODE", "strict"),
        ("RUSTFS_NEW_BUCKET_DURABILITY_MODE", "strict"),
        ("RUST_LOG", "warn,rustfs::heal=debug"),
    ];
    env.start_rustfs_server_from_binary(&previous_binary, extra_drives.iter().map(String::as_str).collect(), &server_env)
        .await?;
    env.create_test_bucket(BUCKET).await?;
    let (status, durability) = admin_request(
        &env.url,
        Method::GET,
        &format!("/rustfs/admin/v3/bucket-durability/{BUCKET}"),
        None,
        &env.access_key,
        &env.secret_key,
    )
    .await?;
    assert_eq!(status, StatusCode::OK, "old bucket durability: {durability}");
    assert_eq!(serde_json::from_str::<Value>(&durability)?["mode"], "strict");

    let old_client = env.create_s3_client();
    let manifest = stream::iter(0..OBJECTS)
        .map(|index| {
            let client = old_client.clone();
            async move {
                let (key, body) = rc5_checkpoint_object(index);
                let entry = serde_json::json!({
                    "bucket": BUCKET,
                    "key": key,
                    "length": body.len(),
                    "sha256": rustfs_utils::hex_sha256(&body, str::to_owned),
                });
                client
                    .put_object()
                    .bucket(BUCKET)
                    .key(&key)
                    .body(ByteStream::from(body))
                    .send()
                    .await?;
                Ok::<_, BoxError>(entry)
            }
        })
        .buffered(4)
        .try_collect::<Vec<_>>()
        .await?;
    let drives = extra_drives
        .iter()
        .map(PathBuf::from)
        .chain(std::iter::once(PathBuf::from(&env.temp_dir)))
        .collect::<Vec<_>>();
    let writer_pid = env.process.as_ref().ok_or("rc.5 writer is not running")?.id();
    let start_heal = async {
        let (status, body) = tokio::time::timeout(
            Duration::from_secs(15),
            admin_request(
                &env.url,
                Method::POST,
                "/rustfs/admin/v3/heal/",
                Some(
                    serde_json::json!({
                        "recursive": true, "dryRun": false, "remove": false, "recreate": false,
                        "scanMode": 2, "updateParity": false, "nolock": false, "pool": 0, "set": 0,
                    })
                    .to_string(),
                ),
                &env.access_key,
                &env.secret_key,
            ),
        )
        .await??;
        if status != StatusCode::OK {
            return Err(format!("rc.5 root heal was not accepted: {status} {body}").into());
        }
        Ok::<_, BoxError>(body)
    };
    let (capture, heal_start) = tokio::try_join!(capture_rc5_checkpoint(&drives, writer_pid), start_heal)?;
    env.stop_server();
    assert_eq!(
        std::fs::read(&capture.path)?,
        capture.bytes,
        "stopping the old writer must preserve captured bytes"
    );
    let checkpoint_dir = capture.disk.join(".rustfs.sys/buckets");
    let resume_path = checkpoint_dir.join(format!("{}_ahm_resume_state.json", capture.task_id));
    let resume_bytes = std::fs::read(&resume_path)?;
    let old_checkpoint: Value = serde_json::from_slice(&capture.bytes)?;
    if let Some(directory) = &evidence_dir {
        std::fs::write(directory.join("checkpoint.rc5.json"), &capture.bytes)?;
        std::fs::write(directory.join("resume.rc5.json"), &resume_bytes)?;
        std::fs::write(directory.join("manifest.json"), serde_json::to_vec_pretty(&manifest)?)?;
        std::fs::write(
            directory.join("source.json"),
            serde_json::to_vec_pretty(&serde_json::json!({
                "kind": "old-release-checkpoint-reader-upgrade",
                "scope": "schema-6 reader authentication, safe rewind and object reads; not full G09 qualification",
                "writer_revision": RC5_COMMIT,
                "writer_binary_sha256": checkpoint_binary_sha256(&previous_binary)?,
                "candidate_revision": current_source_revision()?,
                "candidate_binary_sha256": checkpoint_binary_sha256(&current_binary)?,
                "layout": "one process and four logical drives; not physical fault qualification",
                "task_id": capture.task_id,
                "checkpoint_sha256": rustfs_utils::hex_sha256(&capture.bytes, str::to_owned),
                "resume_sha256": rustfs_utils::hex_sha256(&resume_bytes, str::to_owned),
                "acknowledged_objects": manifest.len(),
                "bucket_durability": serde_json::from_str::<Value>(&durability)?,
                "heal_start": serde_json::from_str::<Value>(&heal_start)?,
            }))?,
        )?;
    }

    let blocked_path = checkpoint_dir.join(format!("{}_ahm_checkpoint.blocked", capture.task_id));
    assert!(!blocked_path.try_exists()?, "the original checkpoint must not already be blocked");
    let disk_options = DiskOption {
        cleanup: false,
        health_check: false,
    };
    // Reopen with the same member order passed to the old writer.
    let disk_index = drives
        .iter()
        .position(|drive| drive == &capture.disk)
        .ok_or("captured disk is not a fixture member")?;
    let mut endpoint = Endpoint::try_from(capture.disk.to_str().ok_or("captured drive path is not UTF-8")?)?;
    endpoint.set_pool_index(0);
    endpoint.set_set_index(0);
    endpoint.set_disk_index(disk_index);
    let disk = new_disk(&endpoint, &disk_options).await?;
    let manager = CheckpointManager::load_from_disk(disk.clone(), &capture.task_id).await?;
    let rewound = manager.get_checkpoint().await;
    assert_eq!(rewound.schema_version, 7);
    assert_eq!(rewound.task_id, capture.task_id);
    assert_eq!((rewound.current_bucket_index, rewound.current_object_index), (0, 0));
    assert!(rewound.processed_objects.is_empty() && rewound.failed_objects.is_empty() && rewound.skipped_objects.is_empty());
    assert_eq!(rewound.successful_objects, 0);
    assert_eq!(rewound.failed_object_count, 0);
    assert_eq!(rewound.skipped_object_count, 0);
    assert_eq!(rewound.processed_bytes, 0);
    assert_eq!((rewound.total_objects, rewound.total_bytes), (0, 0));
    assert!(rewound.baseline_generation.is_none() && !rewound.baseline_known && !rewound.counter_unknown);
    assert!(!blocked_path.try_exists()?, "an authentic old checkpoint must not be quarantined");
    assert_eq!(
        std::fs::read(&capture.path)?,
        capture.bytes,
        "the reader must not rewrite the old checkpoint"
    );
    assert_eq!(std::fs::read(&resume_path)?, resume_bytes, "the reader must not take over the old task");
    drop(manager);
    drop(disk);

    // A separate copy with one digest byte changed proves that successful
    // rewind above did not bypass authentication of the original wire bytes.
    let corrupt_root = fixture.path().join("corrupt-reader-control");
    let corrupt_metadata = corrupt_root.join(".rustfs.sys/buckets");
    std::fs::create_dir_all(&corrupt_metadata)?;
    let mut corrupted = capture.bytes.clone();
    let digest = old_checkpoint["integrity_digest"]
        .as_str()
        .ok_or("old checkpoint digest is not a string")?;
    let offset = corrupted
        .windows(digest.len())
        .position(|bytes| bytes == digest.as_bytes())
        .ok_or("the digest is absent from the captured bytes")?;
    corrupted[offset] = if corrupted[offset] == b'A' { b'B' } else { b'A' };
    let corrupt_path = corrupt_metadata.join(format!("{}_ahm_checkpoint.json", capture.task_id));
    std::fs::write(&corrupt_path, &corrupted)?;
    let endpoint = Endpoint::try_from(corrupt_root.to_str().ok_or("control disk path is not UTF-8")?)?;
    let corrupt_disk = new_disk(&endpoint, &disk_options).await?;
    let Err(error) = CheckpointManager::load_from_disk(corrupt_disk.clone(), &capture.task_id).await else {
        return Err("the current checkpoint reader accepted a corrupted signature".into());
    };
    assert!(matches!(error, rustfs_heal::Error::InvalidCheckpoint(ref reason) if reason.contains("digest")));
    let corrupt_blocked = corrupt_metadata.join(format!("{}_ahm_checkpoint.blocked", capture.task_id));
    assert!(corrupt_blocked.try_exists()?, "the corrupted copy must retain a blocked marker");
    assert_eq!(std::fs::read(&corrupt_path)?, corrupted, "quarantine must preserve the invalid bytes");
    if let Some(directory) = &evidence_dir {
        std::fs::write(directory.join("checkpoint.corrupted.json"), &corrupted)?;
        std::fs::copy(&corrupt_blocked, directory.join("checkpoint.corrupted.blocked"))?;
    }
    drop(corrupt_disk);

    env.restart_server_preserving_data(extra_drives.iter().map(String::as_str).collect(), &server_env)
        .await?;
    let current_client = env.create_s3_client();
    stream::iter(0..OBJECTS)
        .map(|index| {
            let client = current_client.clone();
            async move {
                let (key, expected) = rc5_checkpoint_object(index);
                let (_, actual) = read_object(&client, BUCKET, &key, None).await?;
                assert_eq!(actual, expected, "old object {key} changed during the upgrade");
                Ok::<_, BoxError>(())
            }
        })
        .buffer_unordered(4)
        .try_collect::<Vec<_>>()
        .await?;
    env.stop_server();
    if let Some(directory) = &evidence_dir {
        std::fs::write(
            directory.join("result.json"),
            serde_json::to_vec_pretty(&serde_json::json!({
                "status": "passed",
                "scope": "old-release checkpoint reader and object-data upgrade only",
                "authenticated_original_bytes": true,
                "original_checkpoint_unchanged_after_read": true,
                "old_successful_objects": old_checkpoint["successful_objects"],
                "rewound_schema": rewound.schema_version,
                "rewound_successful_objects": rewound.successful_objects,
                "corrupt_digest_rejected_and_retained": true,
                "verified_objects": OBJECTS,
            }))?,
        )?;
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires a pinned previous RustFS release binary"]
async fn direct_upgrade_from_previous_release_preserves_object_contracts() -> TestResult {
    init_logging();
    let previous_binary = source_binary()?;
    let mut env = RustFSTestEnvironment::new().await?;
    let server_env = [(SSE_MASTER_KEY_ENV, SSE_MASTER_KEY)];
    env.start_rustfs_server_from_binary(&previous_binary, vec![], &server_env)
        .await?;

    let old_client = env.create_s3_client();
    env.create_test_bucket(PLAIN_BUCKET).await?;
    env.create_test_bucket(VERSIONED_BUCKET).await?;
    enable_versioning(&old_client, VERSIONED_BUCKET).await?;

    let plain_key = "plain-object";
    let plain_bytes = b"written by the previous RustFS release";
    old_client
        .put_object()
        .bucket(PLAIN_BUCKET)
        .key(plain_key)
        .body(ByteStream::from_static(plain_bytes))
        .send()
        .await?;

    let encrypted_key = "sse-s3-object";
    let encrypted_bytes = b"encrypted by the previous RustFS release";
    old_client
        .put_object()
        .bucket(PLAIN_BUCKET)
        .key(encrypted_key)
        .server_side_encryption(ServerSideEncryption::Aes256)
        .body(ByteStream::from_static(encrypted_bytes))
        .send()
        .await?;

    let multipart_key = "multipart-object";
    let multipart_parts = vec![vec![b'a'; 5 * 1024 * 1024], b"final multipart bytes".to_vec()];
    let multipart_bytes = multipart_parts.concat();
    write_multipart(&old_client, PLAIN_BUCKET, multipart_key, &multipart_parts).await?;

    let versioned_key = "versioned-object";
    let version1_bytes = b"version one from the previous release";
    let version1 = old_client
        .put_object()
        .bucket(VERSIONED_BUCKET)
        .key(versioned_key)
        .body(ByteStream::from_static(version1_bytes))
        .send()
        .await?
        .version_id()
        .ok_or("first versioned PUT omitted version ID")?
        .to_string();
    let version2_bytes = b"version two from the previous release";
    let version2 = old_client
        .put_object()
        .bucket(VERSIONED_BUCKET)
        .key(versioned_key)
        .body(ByteStream::from_static(version2_bytes))
        .send()
        .await?
        .version_id()
        .ok_or("second versioned PUT omitted version ID")?
        .to_string();
    let deleted = old_client
        .delete_object()
        .bucket(VERSIONED_BUCKET)
        .key(versioned_key)
        .send()
        .await?;
    assert_eq!(deleted.delete_marker(), Some(true));
    let delete_marker = deleted
        .version_id()
        .ok_or("versioned DELETE omitted delete marker version ID")?
        .to_string();

    env.restart_server_preserving_data(vec![], &server_env).await?;
    let current_client = env.create_s3_client();

    assert_eq!(read_object(&current_client, PLAIN_BUCKET, plain_key, None).await?.1, plain_bytes);

    let (encryption, upgraded_encrypted_bytes) = read_object(&current_client, PLAIN_BUCKET, encrypted_key, None).await?;
    assert_eq!(encryption, Some(ServerSideEncryption::Aes256));
    assert_eq!(upgraded_encrypted_bytes, encrypted_bytes);

    assert_eq!(read_object(&current_client, PLAIN_BUCKET, multipart_key, None).await?.1, multipart_bytes);

    assert_eq!(
        read_object(&current_client, VERSIONED_BUCKET, versioned_key, Some(&version1))
            .await?
            .1,
        version1_bytes
    );
    assert_eq!(
        read_object(&current_client, VERSIONED_BUCKET, versioned_key, Some(&version2))
            .await?
            .1,
        version2_bytes
    );

    let current_read = current_client
        .get_object()
        .bucket(VERSIONED_BUCKET)
        .key(versioned_key)
        .send()
        .await
        .expect_err("the previous release's delete marker must remain current after upgrade");
    assert_eq!(current_read.raw_response().map(|response| response.status().as_u16()), Some(404));
    assert_eq!(current_read.as_service_error().and_then(ProvideErrorMetadata::code), Some("NoSuchKey"));

    let listed = current_client
        .list_object_versions()
        .bucket(VERSIONED_BUCKET)
        .prefix(versioned_key)
        .send()
        .await?;
    assert_eq!(listed.versions().len(), 2);
    assert!(
        listed
            .versions()
            .iter()
            .any(|version| version.version_id() == Some(version1.as_str()))
    );
    assert!(
        listed
            .versions()
            .iter()
            .any(|version| version.version_id() == Some(version2.as_str()))
    );
    assert_eq!(listed.delete_markers().len(), 1);
    assert_eq!(listed.delete_markers()[0].version_id(), Some(delete_marker.as_str()));
    assert_eq!(listed.delete_markers()[0].is_latest(), Some(true));

    let post_upgrade_key = "written-after-upgrade";
    let post_upgrade_bytes = b"written by the current RustFS build";
    current_client
        .put_object()
        .bucket(PLAIN_BUCKET)
        .key(post_upgrade_key)
        .body(ByteStream::from_static(post_upgrade_bytes))
        .send()
        .await?;
    assert_eq!(
        read_object(&current_client, PLAIN_BUCKET, post_upgrade_key, None).await?.1,
        post_upgrade_bytes
    );

    Ok(())
}

#[tokio::test]
#[ignore = "requires a pinned previous RustFS release binary"]
async fn rolling_upgrade_from_previous_release_preserves_mixed_version_contracts() -> TestResult {
    init_logging();
    let previous_binary = source_binary()?;
    let evidence_context = g09_evidence_context(&previous_binary).await?;
    let current_binary = rustfs_binary_path();
    let mut cluster = RustFSTestClusterEnvironment::new(MIXED_NODE_COUNT).await?;
    cluster.set_env("RUST_LOG", "rustfs=warn,rustfs_notify=warn");
    cluster.set_env(ALL_DRIVES_STORAGE_CLASS_ENV.0, ALL_DRIVES_STORAGE_CLASS_ENV.1);
    configure_cluster_logs(&mut cluster)?;
    cluster.start_with_binary(&previous_binary).await?;
    cluster.create_test_bucket(MIXED_BUCKET).await?;
    prepare_previous_release_baseline(&mut cluster, &previous_binary).await?;
    exercise_homogeneous_multipart(&cluster, "homogeneous-previous").await?;
    cluster.create_test_bucket(PENDING_MULTIPART_BUCKET).await?;
    let previous_clients = cluster.create_all_clients()?;
    enable_versioning(&previous_clients[0], PENDING_MULTIPART_BUCKET).await?;
    let pending_parts = vec![vec![0x63; 5 * 1024 * 1024], vec![0x7d; 4096]];
    let pending = stage_multipart(&previous_clients[0], PENDING_MULTIPART_BUCKET, PENDING_MULTIPART_KEY, &pending_parts).await?;
    let sources = wait_for_pending_source_bytes(&cluster, &pending).await?;
    assert_pending_parts(&previous_clients[0], &pending).await?;

    cluster.stop_node(0)?;
    cluster.start_node_from_binary(0, &current_binary).await?;
    exercise_mixed_cluster(&cluster, "one-current-node", 0, 1).await?;
    // One controlled out-of-policy attempt proves fail-closed behavior below
    // native write quorum; positive MPU writers remain paused while mixed.
    assert_mixed_pending_refusal(&cluster, &pending, &sources).await?;

    for (node_idx, phase) in [(1, "two-current-nodes"), (2, "one-previous-node")] {
        cluster.stop_node(node_idx)?;
        cluster.start_node_from_binary(node_idx, &current_binary).await?;
        exercise_mixed_cluster(&cluster, phase, 0, 3).await?;
    }

    cluster.stop_node(3)?;
    cluster.start_node_from_binary(3, &current_binary).await?;
    let clients = cluster.create_all_clients()?;
    wait_for_drive_readmission(&clients, MIXED_BUCKET, "homogeneous-current", DRIVE_READMISSION_TIMEOUT).await?;
    let completed_pending = finish_pending_multipart(&clients, &pending, pending_parts.concat()).await?;
    exercise_homogeneous_multipart(&cluster, "homogeneous-current").await?;
    verify_load_bodies(&clients, "homogeneous-previous", "multipart").await?;

    for (node_idx, client) in clients.iter().enumerate() {
        assert_eq!(
            read_object(client, MIXED_BUCKET, PREVIOUS_RELEASE_SEED_KEY, None).await?.1,
            UPGRADE_READINESS_BODY,
            "current node {node_idx} must retain the previous-release seed"
        );
        for phase in ["one-current-node", "two-current-nodes", "one-previous-node"] {
            wait_for_phase_listing(
                client,
                phase,
                MULTIPART_WORKERS * MULTIPART_UPLOADS_PER_WORKER + 2,
                &format!("node {node_idx}: the homogeneous current cluster must preserve every object"),
            )
            .await?;
        }
    }

    let clients = cluster.create_all_clients()?;
    wait_for_upgrade_write_readiness(&clients, MIXED_BUCKET, "all-current", LISTING_CONVERGENCE_TIMEOUT).await?;
    clients[0]
        .delete_object()
        .bucket(MIXED_BUCKET)
        .key(PREVIOUS_RELEASE_SEED_KEY)
        .send()
        .await?;
    for (node, client) in clients.iter().enumerate() {
        let error = client
            .get_object()
            .bucket(MIXED_BUCKET)
            .key(PREVIOUS_RELEASE_SEED_KEY)
            .send()
            .await
            .expect_err("the homogeneous cluster must apply the previously blocked DELETE");
        assert_eq!(
            error.as_service_error().and_then(ProvideErrorMetadata::code),
            Some("NoSuchKey"),
            "node {node}"
        );
    }

    wait_for_multipart_quiescence(&cluster, &sources).await?;
    // No MPU mutation runs during a heterogeneous downgrade. All current
    // callbacks and source controls settle before old binaries reopen volumes.
    cluster.stop();
    cluster.start_with_binary(&previous_binary).await?;
    prepare_release_baseline(&mut cluster, &previous_binary, "rollback").await?;
    let old_clients = cluster.create_all_clients()?;
    assert_resumed_multipart(&old_clients, &completed_pending).await?;
    for phase in ["homogeneous-previous", "homogeneous-current"] {
        verify_load_bodies(&old_clients, phase, "multipart").await?;
        for client in &old_clients {
            wait_for_phase_listing(
                client,
                phase,
                MULTIPART_WORKERS * MULTIPART_UPLOADS_PER_WORKER,
                "previous reader after rollback",
            )
            .await?;
        }
    }
    for phase in ["one-current-node", "two-current-nodes", "one-previous-node"] {
        verify_load_bodies(&old_clients, phase, "put").await?;
        for client in &old_clients {
            wait_for_phase_listing(
                client,
                phase,
                MULTIPART_WORKERS * MULTIPART_UPLOADS_PER_WORKER + 2,
                "previous reader after rollback",
            )
            .await?;
            for (key, body) in [
                ("written-by-current", format!("{phase}: current RustFS build")),
                ("written-by-previous", format!("{phase}: previous RustFS release")),
            ] {
                assert_eq!(
                    read_object(client, MIXED_BUCKET, &format!("{phase}/{key}"), None).await?.1,
                    body.as_bytes()
                );
            }
        }
    }
    for (node, client) in old_clients.iter().enumerate() {
        let error = client
            .get_object()
            .bucket(MIXED_BUCKET)
            .key(PREVIOUS_RELEASE_SEED_KEY)
            .send()
            .await
            .expect_err("current DELETE must remain visible to previous readers after rollback");
        assert_eq!(
            error.as_service_error().and_then(ProvideErrorMetadata::code),
            Some("NoSuchKey"),
            "reader {node}"
        );
    }
    exercise_homogeneous_multipart(&cluster, "homogeneous-rollback").await?;

    if let Some(context) = evidence_context.as_ref() {
        let phases = ["one-current-node", "two-current-nodes", "one-previous-node"];
        let objects_per_phase = MULTIPART_WORKERS * MULTIPART_UPLOADS_PER_WORKER + 2;
        write_g09_evidence(
            context,
            "mixed_version_reader_evidence",
            "mixed-version-reader",
            &["old-writer-new-reader", "new-writer-old-reader"],
            "upgrade_compatibility_test::rolling_upgrade_from_previous_release_preserves_mixed_version_contracts",
            serde_json::json!({
                "bucket": MIXED_BUCKET,
                "phases": phases,
                "objects_per_phase": objects_per_phase,
                "mixed_load": "plain-put",
                "homogeneous_multipart_phases": ["homogeneous-previous", "homogeneous-current", "homogeneous-rollback"],
                "multipart_objects_per_homogeneous_phase": MULTIPART_WORKERS * MULTIPART_UPLOADS_PER_WORKER,
                "assertions": [
                    "current node reads objects written through previous-release client",
                    "previous-release node reads objects written through current client",
                    "all nodes list every mixed-version object after homogeneous-current convergence",
                    "the previous-release seed survives the baseline restart and every upgrade phase",
                    "previous readers read every current-native MPU body after quiescent rollback",
                    "same legacy upload resumes with identical body, ETag and version after full upgrade"
                ],
            }),
        )?;
        write_g09_evidence(
            context,
            "mixed_version_writer_evidence",
            "mixed-version-writer",
            &["old-reader-new-writer", "new-reader-old-writer"],
            "upgrade_compatibility_test::rolling_upgrade_from_previous_release_preserves_mixed_version_contracts",
            serde_json::json!({
                "bucket": MIXED_BUCKET,
                "phases": phases,
                "objects_per_phase": objects_per_phase,
                "mixed_load": "plain-put",
                "homogeneous_multipart_phases": ["homogeneous-previous", "homogeneous-current", "homogeneous-rollback"],
                "multipart_objects_per_homogeneous_phase": MULTIPART_WORKERS * MULTIPART_UPLOADS_PER_WORKER,
                "assertions": [
                    "current writer publishes objects readable by previous-release node",
                    "previous-release writer publishes objects readable by current node",
                    "mixed phases retain 16x16 plain PUT writers while positive multipart writers are paused",
                    "16x16 multipart load runs in homogeneous previous, current and rollback phases",
                    "one-current-node Complete returns typed 503 and preserves actual source authority and parts",
                    "the same legacy upload completes and replays once fully upgraded without adding a version",
                    "modern DELETE fails closed and preserves every byte while old peers remain"
                ],
            }),
        )?;
    }

    Ok(())
}

struct LegacyDeleteObject {
    bucket: String,
    key: String,
    version: Option<String>,
    etag: String,
    body: Vec<u8>,
    data_dirs: Vec<PathBuf>,
}

fn legacy_payload_directories(object_dir: &Path) -> Result<Vec<PathBuf>, BoxError> {
    let entries = match std::fs::read_dir(object_dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error.into()),
    };
    let mut directories = Vec::new();
    for entry in entries {
        let entry = entry?;
        if entry.file_type()?.is_dir() && uuid::Uuid::parse_str(&entry.file_name().to_string_lossy()).is_ok() {
            for child in std::fs::read_dir(entry.path())? {
                let child = child?;
                if child.file_type()?.is_file() && child.file_name().to_string_lossy().starts_with("part.") {
                    directories.push(entry.path());
                    break;
                }
            }
        }
    }
    Ok(directories)
}

async fn seed_legacy_delete_object(
    env: &RustFSTestEnvironment,
    client: &Client,
    bucket: &str,
    key: &str,
    seed: u64,
) -> Result<LegacyDeleteObject, BoxError> {
    let object_dir = Path::new(&env.temp_dir).join(bucket).join(key);
    let before = legacy_payload_directories(&object_dir)?;
    // Incompressible external shards ensure the regression exercises payload
    // reclamation, rather than only the inline xl.meta path.
    let body = layout_noise(1024 * 1024 + 17, seed);
    let response = client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from(body.clone()))
        .send()
        .await?;
    let data_dirs: Vec<_> = legacy_payload_directories(&object_dir)?
        .into_iter()
        .filter(|path| !before.contains(path))
        .collect();
    assert_eq!(data_dirs.len(), 1, "the old binary must create one external payload for {bucket}/{key}");
    // A suspended PUT can omit the version header while reads identify the
    // stored generation as "null". Preserve the old reader's exact identity.
    let head = client
        .head_object()
        .bucket(bucket)
        .key(key)
        .set_version_id(response.version_id().map(str::to_owned))
        .send()
        .await?;
    assert_eq!(head.e_tag(), response.e_tag(), "old writer and reader agree on the stored bytes");
    if let Some(version) = response.version_id() {
        assert_eq!(head.version_id(), Some(version), "old reader resolves the written version");
    }
    Ok(LegacyDeleteObject {
        bucket: bucket.to_owned(),
        key: key.to_owned(),
        version: head.version_id().map(str::to_owned),
        etag: response.e_tag().ok_or("the old writer omitted its ETag")?.to_owned(),
        body,
        data_dirs,
    })
}

async fn assert_legacy_delete_object(client: &Client, object: &LegacyDeleteObject) -> TestResult {
    let response = client
        .get_object()
        .bucket(&object.bucket)
        .key(&object.key)
        .set_version_id(object.version.clone())
        .send()
        .await
        .map_err(|error| format!("GET {}/{} version {:?}: {error:?}", object.bucket, object.key, object.version))?;
    assert_eq!(
        response.e_tag(),
        Some(object.etag.as_str()),
        "{}/{}: original ETag",
        object.bucket,
        object.key
    );
    assert_eq!(
        response.version_id(),
        object.version.as_deref(),
        "{}/{}: original version",
        object.bucket,
        object.key
    );
    assert_eq!(response.body.collect().await?.into_bytes().as_ref(), object.body);
    Ok(())
}

fn preserve_legacy_upgrade_volume(env: &RustFSTestEnvironment, label: &str) -> TestResult {
    let Some(log_dir) = std::env::var_os("RUSTFS_E2E_LOG_DIR") else { return Ok(()) };
    let root = Path::new(&env.temp_dir);
    let destination = Path::new(&log_dir).join(label);
    for entry in walkdir::WalkDir::new(root).follow_links(false) {
        let entry = entry?;
        let target = destination.join(entry.path().strip_prefix(root)?);
        if entry.file_type().is_dir() {
            std::fs::create_dir_all(target)?;
        } else if entry.file_type().is_file() {
            std::fs::copy(entry.path(), target)?;
        } else {
            return Err("the stopped upgrade fixture contains a non-regular entry".into());
        }
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires the pinned stable 1.0.1 release binary"]
async fn stable_upgrade_deletes_foreign_reservations_without_losing_live_objects() -> TestResult {
    use sha2::Digest;

    init_logging();
    let previous_binary = source_binary()?;
    assert_eq!(binary_source_revision(&previous_binary).await?, STABLE_1_0_1_COMMIT);
    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server_from_binary(&previous_binary, vec![], &[]).await?;
    let old_client = env.create_s3_client();
    let plain_bucket = "legacy-delete-plain";
    let versioned_bucket = "legacy-delete-versioned";
    let suspended_bucket = "legacy-delete-suspended";
    for bucket in [plain_bucket, versioned_bucket, suspended_bucket] {
        env.create_test_bucket(bucket).await?;
    }
    enable_versioning(&old_client, versioned_bucket).await?;
    enable_versioning(&old_client, suspended_bucket).await?;
    old_client
        .put_bucket_versioning()
        .bucket(suspended_bucket)
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Suspended)
                .build(),
        )
        .send()
        .await?;

    let mut deleted = Vec::new();
    for (index, (bucket, key)) in [
        (plain_bucket, "single"),
        (plain_bucket, "batch-one"),
        (plain_bucket, "batch-two"),
        (versioned_bucket, "versioned"),
        (suspended_bucket, "suspended"),
    ]
    .into_iter()
    .enumerate()
    {
        deleted.push(seed_legacy_delete_object(&env, &old_client, bucket, key, index as u64 + 1).await?);
    }
    let successor = seed_legacy_delete_object(&env, &old_client, versioned_bucket, "versioned", 20).await?;
    let child_key = format!("single/{}/child", uuid::Uuid::new_v4());
    let child = seed_legacy_delete_object(&env, &old_client, plain_bucket, &child_key, 21).await?;
    for object in deleted.iter().chain([&successor, &child]) {
        assert_legacy_delete_object(&old_client, object).await?;
    }
    env.stop_server();

    // The stable release produced the complete volume and every xl.meta/part
    // byte above. Only the empty legacy marker is fault-injected after stop;
    // this deterministically models an interrupted reservation, not a claimed
    // reproduction of the old binary's SIGKILL timing window.
    let mut evidence = Vec::new();
    for object in &deleted {
        let metadata_path = Path::new(&env.temp_dir)
            .join(&object.bucket)
            .join(&object.key)
            .join("xl.meta");
        let metadata = std::fs::read(&metadata_path)?;
        for directory in &object.data_dirs {
            let marker = directory.join(format!("reserve-delete-data.{}", uuid::Uuid::new_v4()));
            std::fs::File::create(marker)?.sync_all()?;
        }
        assert_eq!(
            std::fs::read(&metadata_path)?,
            metadata,
            "fault injection must preserve the old xl.meta bytes"
        );
        evidence.push(serde_json::json!({
            "bucket": object.bucket,
            "key": object.key,
            "version": object.version,
            "etag": object.etag,
            "xl_meta_sha256": hex_simd::encode_to_string(sha2::Sha256::digest(&metadata), hex_simd::AsciiCase::Lower),
        }));
    }
    if let Some(directory) = std::env::var_os("RUSTFS_E2E_LOG_DIR") {
        std::fs::create_dir_all(&directory)?;
        std::fs::write(
            Path::new(&directory).join("stable-delete-source.json"),
            serde_json::to_vec_pretty(&serde_json::json!({
                "writer_revision": STABLE_1_0_1_COMMIT,
                "candidate_revision": current_source_revision()?,
                "fault_injection": "empty foreign reservation marker after stopping the genuine old writer",
                "objects": evidence,
            }))?,
        )?;
    }

    preserve_legacy_upgrade_volume(&env, "stable-source-volume")?;
    env.restart_server_preserving_data(vec![], &[]).await?;
    let client = env.create_s3_client();
    for object in deleted.iter().chain([&successor, &child]) {
        if let Err(error) = assert_legacy_delete_object(&client, object).await {
            env.stop_server();
            preserve_legacy_upgrade_volume(&env, "failed-upgrade-volume")?;
            return Err(error);
        }
    }
    client.delete_object().bucket(plain_bucket).key("single").send().await?;
    let response = client
        .delete_objects()
        .bucket(plain_bucket)
        .delete(
            Delete::builder()
                .objects(ObjectIdentifier::builder().key("batch-one").build()?)
                .objects(ObjectIdentifier::builder().key("batch-two").build()?)
                .build()?,
        )
        .send()
        .await?;
    assert!(response.errors().is_empty(), "batch deletion must converge: {:?}", response.errors());
    assert_eq!(response.deleted().len(), 2);
    for object in &deleted[3..] {
        client
            .delete_object()
            .bucket(&object.bucket)
            .key(&object.key)
            .set_version_id(object.version.clone())
            .send()
            .await?;
    }

    // Repeat recovery across process restarts, then retry the same requests.
    // The old committed payload must disappear while the successor and the
    // UUID-shaped prefix child remain intact through every pass.
    for pass in 0..2 {
        env.restart_server_preserving_data(vec![], &[]).await?;
        let client = env.create_s3_client();
        for object in &deleted {
            let error = client
                .get_object()
                .bucket(&object.bucket)
                .key(&object.key)
                .set_version_id(object.version.clone())
                .send()
                .await
                .expect_err("an acknowledged deletion must not resurrect after restart");
            assert_eq!(error.raw_response().map(|response| response.status().as_u16()), Some(404));
            for directory in &object.data_dirs {
                assert!(
                    !directory.exists(),
                    "pass {pass}: deleted payload and foreign reservation must be physically reclaimed"
                );
            }
            client
                .delete_object()
                .bucket(&object.bucket)
                .key(&object.key)
                .set_version_id(object.version.clone())
                .send()
                .await?;
        }
        for object in [&successor, &child] {
            assert_legacy_delete_object(&client, object).await?;
        }
        assert_eq!(
            read_object(&client, versioned_bucket, "versioned", None).await?.1,
            successor.body,
            "deleting the old version must preserve the current successor"
        );
        let versions = client.list_object_versions().bucket(versioned_bucket).send().await?;
        assert_eq!(versions.versions().len(), 1);
        assert_eq!(versions.versions()[0].version_id(), successor.version.as_deref());
        assert!(versions.delete_markers().is_empty());
        let listed = client.list_objects_v2().bucket(plain_bucket).send().await?;
        assert_eq!(listed.contents().len(), 1);
        assert_eq!(listed.contents()[0].key(), Some(child_key.as_str()));
    }

    client.delete_object().bucket(plain_bucket).key(&child_key).send().await?;
    client.delete_bucket().bucket(plain_bucket).send().await?;
    Ok(())
}

/// Child-process environment shared by both bucket-configuration scenarios.
///
/// The replication target is an in-process fake bound to `127.0.0.1`, which
/// `set-remote-target` rejects as an SSRF risk without the loopback opt-in, and
/// the proxy bypass keeps a developer's `HTTP_PROXY` from intercepting the
/// server's outbound health check.
fn bucket_config_server_env() -> Vec<(&'static str, &'static str)> {
    let mut env = vec![
        (SSE_MASTER_KEY_ENV, SSE_MASTER_KEY),
        ("NO_PROXY", "127.0.0.1,localhost"),
        ("HTTP_PROXY", ""),
        ("HTTPS_PROXY", ""),
        // Shorten the scanner cycle so the bucket's usage becomes authoritative
        // in seconds; both `set-bucket-quota` and quota admission block on it.
        ("RUSTFS_SCANNER_CYCLE", "1"),
        ("RUSTFS_SCANNER_START_DELAY_SECS", "0"),
    ];
    env.extend_from_slice(LOOPBACK_REPLICATION_TARGET_ENV);
    env.extend(replication_fast_env());
    env
}

/// Restart `env` in place on the same data directory using an explicit binary.
///
/// [`RustFSTestEnvironment::restart_server_preserving_data`] always relaunches
/// the workspace build, which is the upgrade direction only. The rollback
/// scenario needs the reverse: stop the current build and bring the pinned
/// previous release up on the metadata that build just wrote.
async fn restart_from_binary(env: &mut RustFSTestEnvironment, binary: &Path, server_env: &[(&str, &str)]) -> TestResult {
    env.stop_server();
    env.start_rustfs_server_from_binary(binary, vec![], server_env).await
}

async fn set_bucket_quota(env: &RustFSTestEnvironment, bucket: &str, quota_bytes: u64) -> TestResult {
    let path = format!("/rustfs/admin/v3/quota/{bucket}");
    let body = serde_json::json!({ "quota": quota_bytes, "quota_type": "HARD" }).to_string();
    let deadline = Instant::now() + QUOTA_READINESS_TIMEOUT;
    loop {
        let (status, response) =
            admin_request(&env.url, Method::PUT, &path, Some(body.clone()), &env.access_key, &env.secret_key).await?;
        if status.is_success() {
            return Ok(());
        }
        if status != StatusCode::SERVICE_UNAVAILABLE || Instant::now() >= deadline {
            return Err(format!("setting the quota of {bucket} failed: {status} {response}").into());
        }
        sleep(Duration::from_millis(500)).await;
    }
}

/// PUT into a quota-enabled bucket, riding out the post-start quota-admission
/// warm-up described on [`QUOTA_ADMISSION_WARMUP_TIMEOUT`].
///
/// Only `ServiceUnavailable` is retried: any other failure, and a warm-up that
/// never ends, is a genuine regression and surfaces as an error.
async fn put_object_through_quota_warmup(client: &Client, bucket: &str, key: &str, body: &'static [u8]) -> TestResult {
    let deadline = Instant::now() + QUOTA_ADMISSION_WARMUP_TIMEOUT;
    loop {
        let result = client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from_static(body))
            .send()
            .await;
        let error = match result {
            Ok(_) => return Ok(()),
            Err(error) => error,
        };
        let retryable = error.as_service_error().and_then(ProvideErrorMetadata::code) == Some("ServiceUnavailable");
        if !retryable || Instant::now() >= deadline {
            return Err(format!("PUT {bucket}/{key} failed after the quota warm-up window: {error}").into());
        }
        sleep(Duration::from_millis(500)).await;
    }
}

async fn get_bucket_quota(env: &RustFSTestEnvironment, bucket: &str) -> Result<Option<u64>, BoxError> {
    let path = format!("/rustfs/admin/v3/quota/{bucket}");
    let deadline = Instant::now() + QUOTA_USAGE_READINESS_TIMEOUT;
    loop {
        let (status, response) = tokio::time::timeout_at(
            deadline,
            admin_request(&env.url, Method::GET, &path, None, &env.access_key, &env.secret_key),
        )
        .await
        .map_err(|_| format!("reading the quota of {bucket} exceeded the usage-readiness timeout"))??;
        if status == StatusCode::OK {
            let quota: serde_json::Value = serde_json::from_str(&response)?;
            return Ok(quota.get("quota").and_then(serde_json::Value::as_u64));
        }
        if status != StatusCode::SERVICE_UNAVAILABLE || Instant::now() >= deadline {
            return Err(format!("reading the quota of {bucket} failed: {status} {response}").into());
        }
        sleep(Duration::from_millis(500)).await;
    }
}

/// `GET /rustfs/admin/v3/list-remote-targets?bucket=...`.
///
/// Returns an error for any non-200, because rustfs#7172 made this endpoint
/// fail closed on a `bucket-targets.json` blob the running build cannot parse.
/// An upgrade that misreads a blob written by the previous release therefore
/// shows up here as an error, and a silently dropped target shows up as an
/// empty list — the caller must distinguish the two.
async fn list_remote_targets(env: &RustFSTestEnvironment, bucket: &str) -> Result<Vec<serde_json::Value>, BoxError> {
    let path = format!("/rustfs/admin/v3/list-remote-targets?bucket={}", urlencoding::encode(bucket));
    let (status, response) = admin_request(&env.url, Method::GET, &path, None, &env.access_key, &env.secret_key).await?;
    if status != StatusCode::OK {
        return Err(format!("list-remote-targets for {bucket} failed: {status} {response}").into());
    }
    Ok(serde_json::from_str(&response)?)
}

/// Assert that `bucket` still carries exactly the replication target `arn`.
async fn assert_remote_target_preserved(env: &RustFSTestEnvironment, bucket: &str, arn: &str, context: &str) -> TestResult {
    let targets = list_remote_targets(env, bucket).await?;
    assert_eq!(
        targets.len(),
        1,
        "{context}: list-remote-targets must still report the single configured target, got {targets:?}"
    );
    assert_eq!(
        targets[0].get("arn").and_then(serde_json::Value::as_str),
        Some(arn),
        "{context}: the target ARN changed across the restart: {targets:?}"
    );
    Ok(())
}

/// Configure a replication target on `bucket` pointing at the in-process fake,
/// then attach an enabled replication rule for it. Returns the target ARN.
async fn configure_replication(
    env: &RustFSTestEnvironment,
    bucket: &str,
    target: &FakeS3Target,
    target_bucket: &str,
) -> Result<String, BoxError> {
    let arn = set_replication_target_with_options(
        env,
        bucket,
        ReplicationTargetOptions {
            endpoint: &target.address(),
            access_key: FAKE_ACCESS_KEY,
            secret_key: FAKE_SECRET_KEY,
            target_bucket,
            secure: false,
            skip_tls_verify: false,
            ca_cert_pem: None,
        },
    )
    .await?;
    put_bucket_replication(env, bucket, &arn).await?;
    Ok(arn)
}

async fn put_default_sse_s3_encryption(client: &Client, bucket: &str) -> TestResult {
    let configuration = ServerSideEncryptionConfiguration::builder()
        .rules(
            ServerSideEncryptionRule::builder()
                .apply_server_side_encryption_by_default(
                    ServerSideEncryptionByDefault::builder()
                        .sse_algorithm(ServerSideEncryption::Aes256)
                        .build()?,
                )
                .build(),
        )
        .build()?;
    client
        .put_bucket_encryption()
        .bucket(bucket)
        .server_side_encryption_configuration(configuration)
        .send()
        .await?;
    Ok(())
}

async fn assert_default_sse_s3_encryption(client: &Client, bucket: &str, context: &str) -> TestResult {
    let response = client.get_bucket_encryption().bucket(bucket).send().await?;
    let rules = response
        .server_side_encryption_configuration()
        .ok_or("GetBucketEncryption omitted the configuration")?
        .rules();
    assert_eq!(rules.len(), 1, "{context}: expected exactly one encryption rule, got {rules:?}");
    assert_eq!(
        rules[0]
            .apply_server_side_encryption_by_default()
            .map(ServerSideEncryptionByDefault::sse_algorithm),
        Some(&ServerSideEncryption::Aes256),
        "{context}: the default encryption algorithm changed"
    );
    Ok(())
}

async fn put_bucket_tag(client: &Client, bucket: &str) -> TestResult {
    let tagging = Tagging::builder()
        .tag_set(Tag::builder().key(BUCKET_TAG_KEY).value(BUCKET_TAG_VALUE).build()?)
        .build()?;
    client.put_bucket_tagging().bucket(bucket).tagging(tagging).send().await?;
    Ok(())
}

async fn assert_bucket_tag(client: &Client, bucket: &str, context: &str) -> TestResult {
    let tags = client.get_bucket_tagging().bucket(bucket).send().await?;
    let tag_set = tags.tag_set();
    assert_eq!(tag_set.len(), 1, "{context}: expected exactly one bucket tag, got {tag_set:?}");
    assert_eq!(tag_set[0].key(), BUCKET_TAG_KEY, "{context}: bucket tag key changed");
    assert_eq!(tag_set[0].value(), BUCKET_TAG_VALUE, "{context}: bucket tag value changed");
    Ok(())
}

async fn assert_versioning_enabled(client: &Client, bucket: &str, context: &str) -> TestResult {
    let versioning = client.get_bucket_versioning().bucket(bucket).send().await?;
    assert_eq!(
        versioning.status(),
        Some(&BucketVersioningStatus::Enabled),
        "{context}: versioning is no longer Enabled on {bucket}"
    );
    Ok(())
}

fn bucket_policy_document(bucket: &str) -> serde_json::Value {
    serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Sid": "UpgradePublicRead",
            "Effect": "Allow",
            "Principal": { "AWS": ["*"] },
            "Action": ["s3:GetObject"],
            "Resource": [format!("arn:aws:s3:::{bucket}/public/*")]
        }]
    })
}

/// `GET .../on-demand-migration/{bucket}/status`.
///
/// The migration module defaults on from rustfs#7089, so a bucket that never
/// configured a source must still answer `configured: false` rather than
/// engaging the migration path.
async fn assert_migration_not_configured(env: &RustFSTestEnvironment, bucket: &str) -> TestResult {
    let path = format!("/rustfs/admin/v3/on-demand-migration/{bucket}/status");
    let (status, response) = admin_request(&env.url, Method::GET, &path, None, &env.access_key, &env.secret_key).await?;
    assert_eq!(
        status,
        StatusCode::OK,
        "the migration status endpoint must answer for an unconfigured bucket: {status} {response}"
    );
    let body: serde_json::Value = serde_json::from_str(&response)?;
    assert_eq!(
        body.get("configured"),
        Some(&serde_json::Value::Bool(false)),
        "a bucket upgraded from the previous release must not look migration-configured: {body}"
    );
    Ok(())
}

/// A GET for a key that was never written must be a plain `NoSuchKey`.
///
/// With the migration module on by default this is the cheap proof that an
/// unconfigured bucket never consults a source: any migration engagement would
/// surface as a different status or error code here.
async fn assert_missing_key_is_no_such_key(client: &Client, bucket: &str, key: &str) -> TestResult {
    let error = client
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .expect_err("a key that was never written must not be readable");
    assert_eq!(
        error.raw_response().map(|response| response.status().as_u16()),
        Some(404),
        "a missing key must stay a 404 on a bucket with no migration configuration"
    );
    assert_eq!(
        error.as_service_error().and_then(ProvideErrorMetadata::code),
        Some("NoSuchKey"),
        "a missing key must stay NoSuchKey on a bucket with no migration configuration"
    );
    Ok(())
}

/// Bucket configuration written by the pinned previous release must survive an
/// upgrade to the current build unchanged, and must keep working.
///
/// This pins the three on-disk surfaces the on-demand-migration series moved:
///
/// * `BucketMetadata` grew two msgpack keys (encoded map length 44 -> 46), so
///   every configuration read below decodes a 44-key blob on 46-key code.
/// * rustfs#7172 made an unreadable `bucket-targets.json` / encryption /
///   public-access-block / quota blob "present but unreadable" instead of
///   silently defaulting, and made `list-remote-targets` fail closed on it. A
///   replication target configured by the old release must therefore still be
///   *listed*, not dropped and not an error.
/// * rustfs#7183 made the object write path refuse a PUT when the bucket's
///   encryption configuration cannot be read, so a misparsed SSE config would
///   turn every PUT to that bucket into a 500.
///
/// Not covered on purpose: on-demand-migration configuration itself, which the
/// previous release has no public API for — the reverse direction is asserted
/// instead (an upgraded bucket reports `configured: false`).
#[tokio::test]
#[ignore = "requires a pinned previous RustFS release binary"]
async fn direct_upgrade_from_previous_release_preserves_bucket_configuration() -> TestResult {
    init_logging();
    let previous_binary = source_binary()?;

    // In-process: the fake target outlives both server processes, so the
    // replication target stays reachable across the upgrade.
    let replication_target = FakeS3Target::start().await?;
    replication_target.create_bucket(CONFIG_REPLICA_BUCKET);

    let mut env = RustFSTestEnvironment::new().await?;
    let server_env = bucket_config_server_env();
    env.start_rustfs_server_from_binary(&previous_binary, vec![], &server_env)
        .await?;
    let old_client = env.create_s3_client();

    env.create_test_bucket(CONFIG_PLAIN_BUCKET).await?;
    env.create_test_bucket(CONFIG_ENCRYPTED_BUCKET).await?;
    env.create_test_bucket(CONFIG_REPLICATED_BUCKET).await?;
    old_client
        .create_bucket()
        .bucket(CONFIG_LOCKED_BUCKET)
        .object_lock_enabled_for_bucket(true)
        .send()
        .await?;

    // Plain bucket: policy, tags, lifecycle, quota.
    let policy = bucket_policy_document(CONFIG_PLAIN_BUCKET);
    old_client
        .put_bucket_policy()
        .bucket(CONFIG_PLAIN_BUCKET)
        .policy(policy.to_string())
        .send()
        .await?;
    put_bucket_tag(&old_client, CONFIG_PLAIN_BUCKET).await?;
    old_client
        .put_bucket_lifecycle_configuration()
        .bucket(CONFIG_PLAIN_BUCKET)
        .lifecycle_configuration(
            BucketLifecycleConfiguration::builder()
                .rules(
                    LifecycleRule::builder()
                        .id(LIFECYCLE_RULE_ID)
                        .status(ExpirationStatus::Enabled)
                        .filter(LifecycleRuleFilter::builder().prefix(LIFECYCLE_PREFIX).build())
                        .expiration(LifecycleExpiration::builder().days(LIFECYCLE_DAYS).build())
                        .build()?,
                )
                .build()?,
        )
        .send()
        .await?;
    set_bucket_quota(&env, CONFIG_PLAIN_BUCKET, BUCKET_QUOTA_BYTES).await?;

    // Encrypted bucket: SSE-S3 default encryption plus a fully restrictive
    // public access block, both of which rustfs#7172 now fails closed on.
    put_default_sse_s3_encryption(&old_client, CONFIG_ENCRYPTED_BUCKET).await?;
    old_client
        .put_public_access_block()
        .bucket(CONFIG_ENCRYPTED_BUCKET)
        .public_access_block_configuration(
            PublicAccessBlockConfiguration::builder()
                .block_public_acls(true)
                .ignore_public_acls(true)
                .block_public_policy(true)
                .restrict_public_buckets(true)
                .build(),
        )
        .send()
        .await?;

    // Replicated bucket: versioning, a validated remote target, a rule.
    enable_versioning(&old_client, CONFIG_REPLICATED_BUCKET).await?;
    let target_arn = configure_replication(&env, CONFIG_REPLICATED_BUCKET, &replication_target, CONFIG_REPLICA_BUCKET).await?;
    assert_remote_target_preserved(&env, CONFIG_REPLICATED_BUCKET, &target_arn, "before the upgrade").await?;

    // Object-lock bucket: a default GOVERNANCE retention on a fresh bucket.
    old_client
        .put_object_lock_configuration()
        .bucket(CONFIG_LOCKED_BUCKET)
        .object_lock_configuration(
            ObjectLockConfiguration::builder()
                .object_lock_enabled(ObjectLockEnabled::Enabled)
                .rule(
                    ObjectLockRule::builder()
                        .default_retention(
                            DefaultRetention::builder()
                                .mode(ObjectLockRetentionMode::Governance)
                                .days(OBJECT_LOCK_DAYS)
                                .build(),
                        )
                        .build(),
                )
                .build(),
        )
        .send()
        .await?;

    let plain_key = "plain/written-by-previous";
    let plain_bytes = b"plain object written by the previous RustFS release";
    put_object_through_quota_warmup(&old_client, CONFIG_PLAIN_BUCKET, plain_key, plain_bytes).await?;

    let encrypted_key = "encrypted/written-by-previous";
    let encrypted_bytes = b"default-encrypted object written by the previous RustFS release";
    old_client
        .put_object()
        .bucket(CONFIG_ENCRYPTED_BUCKET)
        .key(encrypted_key)
        .body(ByteStream::from_static(encrypted_bytes))
        .send()
        .await?;
    assert_eq!(
        read_object(&old_client, CONFIG_ENCRYPTED_BUCKET, encrypted_key, None)
            .await?
            .0,
        Some(ServerSideEncryption::Aes256),
        "the previous release must apply the bucket default encryption it just accepted"
    );

    // The multipart object lives in the default-encrypted bucket so the
    // upgraded build has to reassemble parts *and* re-derive the object key.
    let multipart_key = "encrypted/multipart-written-by-previous";
    let multipart_parts = vec![vec![b'm'; 5 * 1024 * 1024], b"final multipart bytes".to_vec()];
    let multipart_bytes = multipart_parts.concat();
    write_multipart(&old_client, CONFIG_ENCRYPTED_BUCKET, multipart_key, &multipart_parts).await?;

    let versioned_key = "versioned/written-by-previous";
    let versioned_bytes = b"versioned object written by the previous RustFS release";
    let versioned_id = old_client
        .put_object()
        .bucket(CONFIG_REPLICATED_BUCKET)
        .key(versioned_key)
        .body(ByteStream::from_static(versioned_bytes))
        .send()
        .await?
        .version_id()
        .ok_or("versioned PUT omitted version ID")?
        .to_string();

    env.restart_server_preserving_data(vec![], &server_env).await?;
    let new_client = env.create_s3_client();

    // Every configuration must read back unchanged on the upgraded build.
    let upgraded_policy = new_client.get_bucket_policy().bucket(CONFIG_PLAIN_BUCKET).send().await?;
    let upgraded_policy: serde_json::Value =
        serde_json::from_str(upgraded_policy.policy().ok_or("GetBucketPolicy omitted the document")?)?;
    assert_eq!(upgraded_policy, policy, "the bucket policy changed across the upgrade");
    assert_bucket_tag(&new_client, CONFIG_PLAIN_BUCKET, "after the upgrade").await?;

    let lifecycle = new_client
        .get_bucket_lifecycle_configuration()
        .bucket(CONFIG_PLAIN_BUCKET)
        .send()
        .await?;
    let rules = lifecycle.rules();
    assert_eq!(rules.len(), 1, "the lifecycle rule count changed across the upgrade: {rules:?}");
    assert_eq!(rules[0].id(), Some(LIFECYCLE_RULE_ID));
    assert_eq!(rules[0].status(), &ExpirationStatus::Enabled);
    assert_eq!(
        rules[0].expiration().and_then(LifecycleExpiration::days),
        Some(LIFECYCLE_DAYS),
        "the lifecycle expiration changed across the upgrade"
    );

    assert_eq!(
        get_bucket_quota(&env, CONFIG_PLAIN_BUCKET).await?,
        Some(BUCKET_QUOTA_BYTES),
        "the bucket quota changed across the upgrade"
    );

    assert_default_sse_s3_encryption(&new_client, CONFIG_ENCRYPTED_BUCKET, "after the upgrade").await?;
    let public_access_block = new_client
        .get_public_access_block()
        .bucket(CONFIG_ENCRYPTED_BUCKET)
        .send()
        .await?;
    let public_access_block = public_access_block
        .public_access_block_configuration()
        .ok_or("GetPublicAccessBlock omitted the configuration")?;
    assert_eq!(public_access_block.block_public_acls(), Some(true));
    assert_eq!(public_access_block.ignore_public_acls(), Some(true));
    assert_eq!(public_access_block.block_public_policy(), Some(true));
    assert_eq!(public_access_block.restrict_public_buckets(), Some(true));

    assert_versioning_enabled(&new_client, CONFIG_REPLICATED_BUCKET, "after the upgrade").await?;
    // rustfs#7172: neither an empty list nor an error is acceptable here.
    assert_remote_target_preserved(&env, CONFIG_REPLICATED_BUCKET, &target_arn, "after the upgrade").await?;
    let replication = new_client
        .get_bucket_replication()
        .bucket(CONFIG_REPLICATED_BUCKET)
        .send()
        .await?;
    let replication_rules = replication
        .replication_configuration()
        .ok_or("GetBucketReplication omitted the configuration")?
        .rules();
    assert_eq!(
        replication_rules.len(),
        1,
        "the replication rule count changed across the upgrade: {replication_rules:?}"
    );
    assert_eq!(
        replication_rules[0].destination().map(|destination| destination.bucket()),
        Some(target_arn.as_str()),
        "the replication rule no longer points at the configured target"
    );

    let object_lock = new_client
        .get_object_lock_configuration()
        .bucket(CONFIG_LOCKED_BUCKET)
        .send()
        .await?;
    let object_lock = object_lock
        .object_lock_configuration()
        .ok_or("GetObjectLockConfiguration omitted the configuration")?;
    assert_eq!(object_lock.object_lock_enabled(), Some(&ObjectLockEnabled::Enabled));
    let retention = object_lock
        .rule()
        .and_then(ObjectLockRule::default_retention)
        .ok_or("the object lock configuration lost its default retention")?;
    assert_eq!(retention.mode(), Some(&ObjectLockRetentionMode::Governance));
    assert_eq!(retention.days(), Some(OBJECT_LOCK_DAYS));

    // rustfs#7183: a PUT into the default-encrypted bucket must still succeed
    // and still come back encrypted.
    let post_upgrade_encrypted_key = "encrypted/written-after-upgrade";
    let post_upgrade_encrypted_bytes = b"default-encrypted object written by the current RustFS build";
    new_client
        .put_object()
        .bucket(CONFIG_ENCRYPTED_BUCKET)
        .key(post_upgrade_encrypted_key)
        .body(ByteStream::from_static(post_upgrade_encrypted_bytes))
        .send()
        .await?;
    let (encryption, body) = read_object(&new_client, CONFIG_ENCRYPTED_BUCKET, post_upgrade_encrypted_key, None).await?;
    assert_eq!(
        encryption,
        Some(ServerSideEncryption::Aes256),
        "a PUT after the upgrade lost the bucket default encryption"
    );
    assert_eq!(body, post_upgrade_encrypted_bytes);

    let post_upgrade_plain_key = "plain/written-after-upgrade";
    let post_upgrade_plain_bytes = b"plain object written by the current RustFS build";
    put_object_through_quota_warmup(&new_client, CONFIG_PLAIN_BUCKET, post_upgrade_plain_key, post_upgrade_plain_bytes).await?;
    let (encryption, body) = read_object(&new_client, CONFIG_PLAIN_BUCKET, post_upgrade_plain_key, None).await?;
    assert_eq!(encryption, None, "a bucket without default encryption must not encrypt a PUT");
    assert_eq!(body, post_upgrade_plain_bytes);

    // Every object written by the previous release reads back byte-identical.
    assert_eq!(read_object(&new_client, CONFIG_PLAIN_BUCKET, plain_key, None).await?.1, plain_bytes);
    let (encryption, body) = read_object(&new_client, CONFIG_ENCRYPTED_BUCKET, encrypted_key, None).await?;
    assert_eq!(encryption, Some(ServerSideEncryption::Aes256));
    assert_eq!(body, encrypted_bytes);
    let (encryption, body) = read_object(&new_client, CONFIG_ENCRYPTED_BUCKET, multipart_key, None).await?;
    assert_eq!(encryption, Some(ServerSideEncryption::Aes256));
    assert_eq!(body, multipart_bytes, "the multipart object did not survive the upgrade");
    assert_eq!(
        read_object(&new_client, CONFIG_REPLICATED_BUCKET, versioned_key, Some(&versioned_id))
            .await?
            .1,
        versioned_bytes
    );

    // rustfs#7089: the migration module is on by default, but a bucket that
    // never configured a source behaves exactly as before.
    assert_migration_not_configured(&env, CONFIG_PLAIN_BUCKET).await?;
    assert_missing_key_is_no_such_key(&new_client, CONFIG_PLAIN_BUCKET, "plain/never-written").await?;

    replication_target.shutdown().await;
    Ok(())
}

/// Rolling back to the pinned previous release must still read the bucket
/// metadata the current build wrote.
///
/// This is the other half of the `BucketMetadata` 44 -> 46 key change: the
/// current build writes a 46-key msgpack map with `OnDemandMigrationConfigJSON`
/// and `OnDemandMigrationConfigUpdatedAt`, and the previous release's decoder
/// has to skip those two unknown keys instead of failing the whole blob. If it
/// did not, every configuration read below would come back empty or error and
/// the rollback would silently discard the bucket's configuration.
#[tokio::test]
#[ignore = "requires a pinned previous RustFS release binary"]
async fn rollback_to_previous_release_reads_current_bucket_metadata() -> TestResult {
    init_logging();
    let previous_binary = source_binary()?;
    let evidence_context = g09_evidence_context(&previous_binary).await?;

    let replication_target = FakeS3Target::start().await?;
    replication_target.create_bucket(ROLLBACK_REPLICA_BUCKET);

    let mut env = RustFSTestEnvironment::new().await?;
    let server_env = bucket_config_server_env();
    env.start_rustfs_server_with_env(vec![], &server_env).await?;
    let new_client = env.create_s3_client();

    env.create_test_bucket(ROLLBACK_BUCKET).await?;
    enable_versioning(&new_client, ROLLBACK_BUCKET).await?;
    put_default_sse_s3_encryption(&new_client, ROLLBACK_BUCKET).await?;
    put_bucket_tag(&new_client, ROLLBACK_BUCKET).await?;
    let target_arn = configure_replication(&env, ROLLBACK_BUCKET, &replication_target, ROLLBACK_REPLICA_BUCKET).await?;
    assert_remote_target_preserved(&env, ROLLBACK_BUCKET, &target_arn, "before the rollback").await?;

    let single_key = "rollback/single";
    let single_bytes = b"single-part object written by the current RustFS build";
    let single_version = new_client
        .put_object()
        .bucket(ROLLBACK_BUCKET)
        .key(single_key)
        .body(ByteStream::from_static(single_bytes))
        .send()
        .await?
        .version_id()
        .ok_or("versioned PUT omitted version ID")?
        .to_string();

    let multipart_key = "rollback/multipart";
    let multipart_parts = vec![vec![b'r'; 5 * 1024 * 1024], b"final rollback bytes".to_vec()];
    let multipart_bytes = multipart_parts.concat();
    write_multipart(&new_client, ROLLBACK_BUCKET, multipart_key, &multipart_parts).await?;

    restart_from_binary(&mut env, &previous_binary, &server_env).await?;
    let old_client = env.create_s3_client();

    assert_versioning_enabled(&old_client, ROLLBACK_BUCKET, "after the rollback").await?;
    assert_default_sse_s3_encryption(&old_client, ROLLBACK_BUCKET, "after the rollback").await?;
    assert_bucket_tag(&old_client, ROLLBACK_BUCKET, "after the rollback").await?;
    assert_remote_target_preserved(&env, ROLLBACK_BUCKET, &target_arn, "after the rollback").await?;

    let (encryption, body) = read_object(&old_client, ROLLBACK_BUCKET, single_key, Some(&single_version)).await?;
    assert_eq!(encryption, Some(ServerSideEncryption::Aes256));
    assert_eq!(body, single_bytes);
    let (encryption, body) = read_object(&old_client, ROLLBACK_BUCKET, multipart_key, None).await?;
    assert_eq!(encryption, Some(ServerSideEncryption::Aes256));
    assert_eq!(body, multipart_bytes, "the multipart object did not survive the rollback");

    // A PUT on the rolled-back release must still honour the encryption
    // configuration it decoded out of the current build's metadata blob.
    let post_rollback_key = "rollback/written-after-rollback";
    let post_rollback_bytes = b"object written by the previous RustFS release after the rollback";
    old_client
        .put_object()
        .bucket(ROLLBACK_BUCKET)
        .key(post_rollback_key)
        .body(ByteStream::from_static(post_rollback_bytes))
        .send()
        .await?;
    let (encryption, body) = read_object(&old_client, ROLLBACK_BUCKET, post_rollback_key, None).await?;
    assert_eq!(
        encryption,
        Some(ServerSideEncryption::Aes256),
        "the rolled-back release lost the bucket default encryption"
    );
    assert_eq!(body, post_rollback_bytes);

    env.restart_server_preserving_data(vec![], &server_env).await?;
    let current_again = env.create_s3_client();
    assert_versioning_enabled(&current_again, ROLLBACK_BUCKET, "after rolling forward again").await?;
    assert_default_sse_s3_encryption(&current_again, ROLLBACK_BUCKET, "after rolling forward again").await?;
    assert_bucket_tag(&current_again, ROLLBACK_BUCKET, "after rolling forward again").await?;
    assert_remote_target_preserved(&env, ROLLBACK_BUCKET, &target_arn, "after rolling forward again").await?;
    assert_eq!(
        read_object(&current_again, ROLLBACK_BUCKET, single_key, Some(&single_version))
            .await?
            .1,
        single_bytes
    );
    assert_eq!(
        read_object(&current_again, ROLLBACK_BUCKET, multipart_key, None).await?.1,
        multipart_bytes
    );
    assert_eq!(
        read_object(&current_again, ROLLBACK_BUCKET, post_rollback_key, None).await?.1,
        post_rollback_bytes
    );

    if let Some(context) = evidence_context.as_ref() {
        write_g09_evidence(
            context,
            "rollback_payload_evidence",
            "rollback-payload",
            &["rollback-to-old", "rollback-to-new", "unknown-field-retained"],
            "upgrade_compatibility_test::rollback_to_previous_release_reads_current_bucket_metadata",
            serde_json::json!({
                "bucket": ROLLBACK_BUCKET,
                "cases": {
                    "rollback-to-old": [
                        "previous-release binary reads current-build versioning, SSE-S3, tags, replication target and objects",
                        "previous-release writer honors the decoded current-build encryption configuration"
                    ],
                    "rollback-to-new": [
                        "current build reads the object written by the rolled-back previous release",
                        "current build reads the current-build single-part and multipart objects after rolling forward again"
                    ],
                    "unknown-field-retained": [
                        "previous release skips current-build bucket metadata extension fields without dropping known bucket configuration",
                        "current build reads the retained bucket configuration after the previous-release round trip"
                    ]
                },
            }),
        )?;
    }

    replication_target.shutdown().await;
    Ok(())
}

// ---------------------------------------------------------------------------
// Previous-release multipart layouts under the current build (backlog#2147 follow-up to
// rustfs#7305)
// ---------------------------------------------------------------------------
//
// rustfs#7305 changed `ObjectInfo::is_multipart` to consult the stored part
// list before the ETag shape. Every earlier check of that change used
// synthetic metadata; this scenario writes the layouts with the published
// previous-release binary and then reads, describes, and replicates them with the
// current build on the same data directory.

const LAYOUT_PLAIN_BUCKET: &str = "upgrade-layout-plain";
const LAYOUT_ENCRYPTED_BUCKET: &str = "upgrade-layout-encrypted";
const LAYOUT_REPLICA_BUCKET: &str = "upgrade-layout-replica";
const LAYOUT_PART_SIZE: usize = 5 * 1024 * 1024;
const LAYOUT_TAIL_SIZE: usize = 1024 * 1024 + 4096;
const LAYOUT_SSEC_KEY: &str = "0123456789abcdef0123456789abcdef";
const LAYOUT_REPLICATION_TIMEOUT: Duration = Duration::from_secs(180);

struct LayoutCase {
    bucket: &'static str,
    key: &'static str,
    /// Empty for a single PUT.
    part_sizes: Vec<usize>,
    body: Vec<u8>,
    ssec: bool,
    /// `false` for layouts whose replication is a known pre-existing failure;
    /// their outcome is logged, not asserted.
    assert_replication: bool,
    /// Recorded from the previous-release writer.
    source_etag: String,
    /// Whether the previous release reported `ObjectParts` for the object.
    source_reported_parts: Option<usize>,
}

impl LayoutCase {
    fn is_multipart_layout(&self) -> bool {
        self.part_sizes.len() > 1
    }

    fn label(&self) -> String {
        format!("{}/{}", self.bucket, self.key)
    }
}

fn layout_noise(len: usize, seed: u64) -> Vec<u8> {
    let mut state = seed ^ 0x9E37_79B9_7F4A_7C15;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state >> 24) as u8
        })
        .collect()
}

fn layout_text(len: usize, seed: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(len + 64);
    let mut line = 0u64;
    while out.len() < len {
        out.extend_from_slice(format!("rc5 legacy layout seed={seed} line={line} lorem ipsum dolor sit amet\n").as_bytes());
        line += 1;
    }
    out.truncate(len);
    out
}

fn layout_ssec_key_md5() -> String {
    use md5::{Digest as _, Md5};
    let mut hasher = Md5::new();
    hasher.update(LAYOUT_SSEC_KEY.as_bytes());
    base64_simd::STANDARD.encode_to_string(hasher.finalize())
}

fn layout_ssec_key() -> String {
    base64_simd::STANDARD.encode_to_string(LAYOUT_SSEC_KEY)
}

async fn layout_head(
    client: &Client,
    case: &LayoutCase,
) -> Result<aws_sdk_s3::operation::head_object::HeadObjectOutput, BoxError> {
    let request = client.head_object().bucket(case.bucket).key(case.key);
    let request = if case.ssec {
        request
            .sse_customer_algorithm("AES256")
            .sse_customer_key(layout_ssec_key())
            .sse_customer_key_md5(layout_ssec_key_md5())
    } else {
        request
    };
    Ok(request.send().await?)
}

async fn layout_get(
    client: &Client,
    case: &LayoutCase,
    range: Option<String>,
    part_number: Option<i32>,
) -> Result<aws_sdk_s3::operation::get_object::GetObjectOutput, BoxError> {
    let request = client.get_object().bucket(case.bucket).key(case.key);
    let request = if case.ssec {
        request
            .sse_customer_algorithm("AES256")
            .sse_customer_key(layout_ssec_key())
            .sse_customer_key_md5(layout_ssec_key_md5())
    } else {
        request
    };
    let request = request.set_range(range).set_part_number(part_number);
    Ok(request.send().await?)
}

async fn layout_attributes(
    client: &Client,
    case: &LayoutCase,
) -> Result<aws_sdk_s3::operation::get_object_attributes::GetObjectAttributesOutput, BoxError> {
    let request = client
        .get_object_attributes()
        .bucket(case.bucket)
        .key(case.key)
        .object_attributes(ObjectAttributes::Etag)
        .object_attributes(ObjectAttributes::ObjectParts)
        .object_attributes(ObjectAttributes::ObjectSize)
        .max_parts(100);
    let request = if case.ssec {
        request
            .sse_customer_algorithm("AES256")
            .sse_customer_key(layout_ssec_key())
            .sse_customer_key_md5(layout_ssec_key_md5())
    } else {
        request
    };
    Ok(request.send().await?)
}

/// Write `case` with the source client; single PUT when `part_sizes` is empty.
async fn layout_write(client: &Client, case: &LayoutCase) -> Result<(), BoxError> {
    let content_type = "text/plain";
    if case.part_sizes.is_empty() {
        let request = client
            .put_object()
            .bucket(case.bucket)
            .key(case.key)
            .content_type(content_type)
            .body(ByteStream::from(case.body.clone()));
        let request = if case.ssec {
            request
                .sse_customer_algorithm("AES256")
                .sse_customer_key(layout_ssec_key())
                .sse_customer_key_md5(layout_ssec_key_md5())
        } else {
            request
        };
        request.send().await?;
        return Ok(());
    }
    let create = client
        .create_multipart_upload()
        .bucket(case.bucket)
        .key(case.key)
        .content_type(content_type);
    let create = if case.ssec {
        create
            .sse_customer_algorithm("AES256")
            .sse_customer_key(layout_ssec_key())
            .sse_customer_key_md5(layout_ssec_key_md5())
    } else {
        create
    };
    let created = create.send().await?;
    let upload_id = created.upload_id().ok_or("CreateMultipartUpload omitted upload ID")?;
    let mut completed = Vec::with_capacity(case.part_sizes.len());
    let mut offset = 0usize;
    for (index, size) in case.part_sizes.iter().enumerate() {
        let part_number = i32::try_from(index + 1)?;
        let chunk = case.body[offset..offset + size].to_vec();
        offset += size;
        let upload = client
            .upload_part()
            .bucket(case.bucket)
            .key(case.key)
            .upload_id(upload_id)
            .part_number(part_number)
            .body(ByteStream::from(chunk));
        let upload = if case.ssec {
            upload
                .sse_customer_algorithm("AES256")
                .sse_customer_key(layout_ssec_key())
                .sse_customer_key_md5(layout_ssec_key_md5())
        } else {
            upload
        };
        let uploaded = upload.send().await?;
        completed.push(
            CompletedPart::builder()
                .part_number(part_number)
                .e_tag(uploaded.e_tag().ok_or("UploadPart omitted ETag")?)
                .build(),
        );
    }
    client
        .complete_multipart_upload()
        .bucket(case.bucket)
        .key(case.key)
        .upload_id(upload_id)
        .multipart_upload(CompletedMultipartUpload::builder().set_parts(Some(completed)).build())
        .send()
        .await?;
    Ok(())
}

fn layout_cases() -> Vec<LayoutCase> {
    let two = vec![LAYOUT_PART_SIZE, LAYOUT_TAIL_SIZE];
    let three = vec![LAYOUT_PART_SIZE, LAYOUT_PART_SIZE, 4096];
    let total = |sizes: &[usize]| sizes.iter().sum::<usize>();
    let case = |bucket, key, part_sizes: Vec<usize>, body: Vec<u8>, ssec| LayoutCase {
        bucket,
        key,
        part_sizes,
        body,
        ssec,
        assert_replication: true,
        source_etag: String::new(),
        source_reported_parts: None,
    };
    vec![
        case(LAYOUT_PLAIN_BUCKET, "plain/single.bin", vec![], layout_noise(1024 * 1024 + 17, 1), false),
        case(
            LAYOUT_PLAIN_BUCKET,
            "plain/multipart-2.bin",
            two.clone(),
            layout_noise(total(&two), 2),
            false,
        ),
        case(
            LAYOUT_PLAIN_BUCKET,
            "plain/multipart-3.bin",
            three.clone(),
            layout_noise(total(&three), 3),
            false,
        ),
        case(
            LAYOUT_PLAIN_BUCKET,
            "plain/compressed-single.txt",
            vec![],
            layout_text(1024 * 1024 + 17, 4),
            false,
        ),
        case(
            LAYOUT_PLAIN_BUCKET,
            "plain/compressed-multipart-2.txt",
            two.clone(),
            layout_text(total(&two), 5),
            false,
        ),
        case(
            LAYOUT_PLAIN_BUCKET,
            "plain/ssec-multipart-2.bin",
            two.clone(),
            layout_noise(total(&two), 6),
            true,
        ),
        // SSE-C passthrough replicates the stored ciphertext part by part; a
        // compressible first part is stored well below 5 MiB, so the sender
        // declares each part's plaintext length and the target validates the
        // 5 MiB minimum against it (rustfs/backlog#2363). rc.5 as the sender
        // still fails this layout (see `previous_release_baseline_replicates_multipart_layouts`).
        case(
            LAYOUT_PLAIN_BUCKET,
            "plain/ssec-compressed-multipart-2.txt",
            two.clone(),
            layout_text(total(&two), 7),
            true,
        ),
        case(
            LAYOUT_ENCRYPTED_BUCKET,
            "encrypted/single.bin",
            vec![],
            layout_noise(1024 * 1024 + 17, 8),
            false,
        ),
        case(
            LAYOUT_ENCRYPTED_BUCKET,
            "encrypted/multipart-2.bin",
            two.clone(),
            layout_noise(total(&two), 9),
            false,
        ),
        case(
            LAYOUT_ENCRYPTED_BUCKET,
            "encrypted/multipart-3.bin",
            three.clone(),
            layout_noise(total(&three), 10),
            false,
        ),
        case(
            LAYOUT_ENCRYPTED_BUCKET,
            "encrypted/compressed-multipart-2.txt",
            two.clone(),
            layout_text(total(&two), 11),
            false,
        ),
    ]
}

fn layout_server_env() -> Vec<(&'static str, &'static str)> {
    let mut env = bucket_config_server_env();
    env.push(("RUSTFS_COMPRESSION_ENABLED", "true"));
    env.push(("RUSTFS_COMPRESSION_MULTIPART_ENABLED", "true"));
    env
}

fn layout_reported_parts(attributes: &aws_sdk_s3::operation::get_object_attributes::GetObjectAttributesOutput) -> Option<usize> {
    attributes.object_parts().map(|parts| parts.parts().len())
}

async fn assert_layout_full_body(client: &Client, case: &LayoutCase, context: &str) -> TestResult {
    let label = case.label();
    let head = layout_head(client, case).await?;
    assert_eq!(
        head.e_tag().map(|etag| etag.trim_matches('"')),
        Some(case.source_etag.as_str()),
        "{context}: {label}: the ETag written by the previous release must be reported unchanged"
    );
    assert_eq!(
        head.content_length(),
        Some(i64::try_from(case.body.len())?),
        "{context}: {label}: HEAD content length"
    );

    let full = layout_get(client, case, None, None).await?.body.collect().await?.into_bytes();
    assert_eq!(full.len(), case.body.len(), "{context}: {label}: full GET length");
    assert!(
        full == case.body,
        "{context}: {label}: full GET body must equal the previous-release upload"
    );
    Ok(())
}

async fn assert_layout_readable(client: &Client, case: &LayoutCase, context: &str) -> TestResult {
    assert_layout_full_body(client, case, context).await?;
    let label = case.label();
    if case.is_multipart_layout() {
        let first = case.part_sizes[0];
        let range = format!("bytes={}-{}", first - 32, first + 31);
        let crossing = layout_get(client, case, Some(range), None)
            .await?
            .body
            .collect()
            .await?
            .into_bytes();
        assert!(
            crossing == case.body[first - 32..first + 32],
            "{context}: {label}: range across the first part boundary"
        );
        let tail_start: usize = case.part_sizes[..case.part_sizes.len() - 1].iter().sum();
        let last_number = i32::try_from(case.part_sizes.len())?;
        let last = layout_get(client, case, None, Some(last_number)).await?;
        assert_eq!(
            last.content_length(),
            Some(i64::try_from(case.part_sizes[case.part_sizes.len() - 1])?),
            "{context}: {label}: partNumber={last_number} length"
        );
        let last_body = last.body.collect().await?.into_bytes();
        assert!(
            last_body == case.body[tail_start..],
            "{context}: {label}: partNumber={last_number} body must be the stored last part"
        );
    }
    Ok(())
}

async fn assert_layout_attributes(client: &Client, case: &LayoutCase, context: &str) -> TestResult {
    let label = case.label();
    let attributes = layout_attributes(client, case).await?;
    assert_eq!(
        attributes.e_tag().map(|etag| etag.trim_matches('"')),
        Some(case.source_etag.as_str()),
        "{context}: {label}: attributes ETag"
    );
    assert_eq!(
        attributes.object_size(),
        Some(i64::try_from(case.body.len())?),
        "{context}: {label}: attributes ObjectSize"
    );
    if case.is_multipart_layout() {
        let parts = attributes
            .object_parts()
            .ok_or_else(|| format!("{context}: {label}: multipart layout must expose ObjectParts"))?;
        assert_eq!(
            parts.total_parts_count(),
            Some(i32::try_from(case.part_sizes.len())?),
            "{context}: {label}: TotalPartsCount"
        );
        let observed: Vec<(Option<i32>, Option<i64>)> =
            parts.parts().iter().map(|part| (part.part_number(), part.size())).collect();
        let expected: Vec<(Option<i32>, Option<i64>)> = case
            .part_sizes
            .iter()
            .enumerate()
            .map(|(index, size)| (Some(index as i32 + 1), Some(*size as i64)))
            .collect();
        assert_eq!(
            observed, expected,
            "{context}: {label}: ObjectParts must report the plaintext part layout"
        );
    } else {
        assert!(
            attributes.object_parts().is_none_or(|parts| parts.parts().is_empty()),
            "{context}: {label}: a single PUT must not report stored parts"
        );
    }
    Ok(())
}

async fn put_layout_replication_rule(env: &RustFSTestEnvironment, bucket: &str, arn: &str) -> TestResult {
    let body = format!(
        r#"<ReplicationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Role></Role>
  <Rule>
    <ID>legacy-layouts</ID>
    <Priority>1</Priority>
    <Status>Enabled</Status>
    <Filter><Prefix></Prefix></Filter>
    <DeleteMarkerReplication><Status>Enabled</Status></DeleteMarkerReplication>
    <DeleteReplication><Status>Enabled</Status></DeleteReplication>
    <ExistingObjectReplication><Status>Enabled</Status></ExistingObjectReplication>
    <Destination><Bucket>{arn}</Bucket></Destination>
  </Rule>
</ReplicationConfiguration>"#
    );
    let url = format!("{}/{bucket}?replication", env.url);
    let response = signed_request(
        Method::PUT,
        &url,
        &env.access_key,
        &env.secret_key,
        Some(body.into_bytes()),
        Some("application/xml"),
    )
    .await?;
    if response.status() != StatusCode::OK {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(format!("put replication rule on {bucket} failed: {status} {body}").into());
    }
    Ok(())
}

/// Wait for the existing-object replication of `case` to reach a terminal
/// status and return it (`COMPLETED` or `FAILED`).
async fn wait_layout_replication_terminal(client: &Client, case: &LayoutCase) -> Result<String, BoxError> {
    let deadline = Instant::now() + LAYOUT_REPLICATION_TIMEOUT;
    loop {
        let head = layout_head(client, case).await?;
        let status = head.replication_status().map(|status| status.as_str().to_string());
        if matches!(status.as_deref(), Some("COMPLETED") | Some("FAILED")) {
            return Ok(status.unwrap_or_default());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "{}: existing-object replication never reached a terminal status; last {status:?}",
                case.label()
            )
            .into());
        }
        sleep(Duration::from_millis(500)).await;
    }
}

#[derive(Debug)]
struct LayoutTransport {
    status: String,
    uploaded_parts: Vec<i32>,
    single_puts: usize,
    completes: usize,
    /// Allowlisted request identity and prepared response metadata. A prepared
    /// response does not prove that the client received it.
    journal: Vec<Value>,
}

/// Configure every layout bucket to replicate its existing objects to a fresh
/// fake target, wait for each case to settle, and report the transport the
/// target observed per case.
async fn replicate_layouts(
    env: &RustFSTestEnvironment,
    client: &Client,
    cases: &[LayoutCase],
) -> Result<(FakeS3Target, Vec<LayoutTransport>), BoxError> {
    let target = FakeS3Target::start().await?;
    target.create_bucket(LAYOUT_REPLICA_BUCKET);
    for bucket in [LAYOUT_PLAIN_BUCKET, LAYOUT_ENCRYPTED_BUCKET] {
        let arn = set_replication_target_with_options(
            env,
            bucket,
            ReplicationTargetOptions {
                endpoint: &target.address(),
                access_key: FAKE_ACCESS_KEY,
                secret_key: FAKE_SECRET_KEY,
                target_bucket: LAYOUT_REPLICA_BUCKET,
                secure: false,
                skip_tls_verify: false,
                ca_cert_pem: None,
            },
        )
        .await?;
        put_layout_replication_rule(env, bucket, &arn).await?;
    }
    let mut statuses = Vec::with_capacity(cases.len());
    for case in cases {
        statuses.push(wait_layout_replication_terminal(client, case).await?);
    }
    let journal = target.requests();
    let mut transports = Vec::with_capacity(cases.len());
    for (case, status) in cases.iter().zip(statuses) {
        let key_requests: Vec<_> = journal
            .iter()
            .filter(|record| record.key.as_deref() == Some(case.key))
            .collect();
        let mut uploaded_parts: Vec<i32> = key_requests
            .iter()
            .filter(|record| record.operation == FakeTargetOperation::UploadPart)
            .filter_map(|record| record.part_number)
            .collect();
        uploaded_parts.sort_unstable();
        uploaded_parts.dedup();
        let transport = LayoutTransport {
            status,
            uploaded_parts,
            single_puts: key_requests
                .iter()
                .filter(|record| record.operation == FakeTargetOperation::PutObject)
                .count(),
            completes: key_requests
                .iter()
                .filter(|record| record.operation == FakeTargetOperation::CompleteMultipartUpload)
                .count(),
            journal: key_requests
                .iter()
                .map(|record| {
                    serde_json::json!({
                        "sequence": record.sequence,
                        "operation": format!("{:?}", record.operation),
                        "part_number": record.part_number,
                        "upload_id": record.upload_id,
                        "requested_version_id": record.version_id,
                        "source_version_id": record.source_version_id,
                        "source_mtime": record.source_mtime,
                        "source_etag": record.source_etag,
                        "source_replication_request": record.source_replication_request,
                        "request_content_length": record.content_length,
                        "journaled_at_unix_millis": record.journaled_at_unix_millis,
                        "prepared_response": record.prepared_response.as_ref().map(|response| serde_json::json!({
                            "status": response.status,
                            "version_id": response.version_id,
                            "etag": response.etag,
                            "last_modified": response.last_modified,
                            "content_length": response.content_length,
                            "prepared_at_unix_millis": response.prepared_at_unix_millis,
                            "elapsed_secs": response.elapsed.as_secs_f64(),
                        })),
                    })
                })
                .collect(),
        };
        tracing::info!(
            target: "e2e_test::upgrade_compatibility_test",
            object = %case.label(),
            ?transport,
            "replication transport observed on the target"
        );
        transports.push(transport);
    }
    Ok((target, transports))
}

/// The previous release writes single-PUT, multipart, compressed, SSE-C and SSE-S3 layouts;
/// the current build must read every byte, expose the stored part layout
/// through GetObjectAttributes and partNumber reads, and replicate the objects
/// with the transport that matches their stored parts.
#[tokio::test]
#[ignore = "requires a pinned previous RustFS release binary"]
async fn direct_upgrade_from_previous_release_preserves_multipart_layouts() -> TestResult {
    init_logging();
    let previous_binary = source_binary()?;
    let server_env = layout_server_env();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server_from_binary(&previous_binary, vec![], &server_env)
        .await?;
    let old_client = env.create_s3_client();
    env.create_test_bucket(LAYOUT_PLAIN_BUCKET).await?;
    env.create_test_bucket(LAYOUT_ENCRYPTED_BUCKET).await?;
    enable_versioning(&old_client, LAYOUT_PLAIN_BUCKET).await?;
    enable_versioning(&old_client, LAYOUT_ENCRYPTED_BUCKET).await?;
    put_default_sse_s3_encryption(&old_client, LAYOUT_ENCRYPTED_BUCKET).await?;
    assert_default_sse_s3_encryption(&old_client, LAYOUT_ENCRYPTED_BUCKET, "previous release").await?;

    let mut cases = layout_cases();
    for case in cases.iter_mut() {
        layout_write(&old_client, case).await?;
        let head = layout_head(&old_client, case).await?;
        case.source_etag = head
            .e_tag()
            .ok_or_else(|| format!("{}: previous-release HEAD omitted the ETag", case.label()))?
            .trim_matches('"')
            .to_string();
        case.source_reported_parts = layout_attributes(&old_client, case)
            .await
            .ok()
            .and_then(|a| layout_reported_parts(&a));
        tracing::info!(
            target: "e2e_test::upgrade_compatibility_test",
            object = %case.label(),
            parts = case.part_sizes.len(),
            etag = %case.source_etag,
            source_reported_parts = ?case.source_reported_parts,
            "previous release wrote a layout"
        );
    }
    // Prove the previous-release writer persisted every original byte before upgrading.
    // Its compressed cross-part range reader has a legacy limitation; the
    // upgraded reader must still satisfy every range and partNumber assertion.
    for case in &cases {
        assert_layout_full_body(&old_client, case, "previous release").await?;
    }

    // Upgrade in place.
    env.restart_server_preserving_data(vec![], &server_env).await?;
    let client = env.create_s3_client();
    for case in &cases {
        assert_layout_readable(&client, case, "upgraded").await?;
        assert_layout_attributes(&client, case, "upgraded").await?;
    }

    // Replicate the pre-existing objects with the current build.
    let (target, transports) = replicate_layouts(&env, &client, &cases).await?;
    let replica_client = fake_source_client(&target);
    for (case, transport) in cases.iter().zip(&transports) {
        let label = case.label();
        if !case.assert_replication {
            continue;
        }
        assert_eq!(transport.status, "COMPLETED", "{label}: existing-object replication must complete");
        if case.is_multipart_layout() {
            let expected: Vec<i32> = (1..=i32::try_from(case.part_sizes.len())?).collect();
            assert_eq!(
                transport.uploaded_parts, expected,
                "{label}: stored parts must replicate as the same multipart layout"
            );
            // An object still PENDING when the next scanner cycle arrives is
            // not driven a second time (rustfs/backlog#2362); the journal is
            // logged so a duplicate round is visible if this ever regresses.
            assert_eq!(
                transport.completes, 1,
                "{label}: exactly one CompleteMultipartUpload; journal {:?}",
                transport.journal
            );
            assert_eq!(
                transport.single_puts, 0,
                "{label}: a multipart layout must not go out as a single PutObject"
            );
        } else {
            assert_eq!(transport.single_puts, 1, "{label}: a single PUT replicates as exactly one PutObject");
            assert!(transport.uploaded_parts.is_empty(), "{label}: a single PUT must not go out as multipart");
        }
        if !case.ssec {
            let replica = replica_client
                .get_object()
                .bucket(LAYOUT_REPLICA_BUCKET)
                .key(case.key)
                .send()
                .await
                .map_err(|err| format!("{label}: replica missing on the target: {err}"))?
                .body
                .collect()
                .await?
                .into_bytes();
            assert_eq!(replica.len(), case.body.len(), "{label}: replica length");
            assert!(replica == case.body, "{label}: replica body must equal the previous-release upload");
        }
    }
    Ok(())
}

/// The same layouts replicated by the previous release itself, without an upgrade. This is the
/// baseline that tells a pre-existing transport failure apart from one the
/// current build introduced; it records the outcome per layout and only fails
/// when the fixture cannot run.
#[tokio::test]
#[ignore = "requires a pinned previous RustFS release binary"]
async fn previous_release_baseline_replicates_multipart_layouts() -> TestResult {
    init_logging();
    let previous_binary = source_binary()?;
    let server_env = layout_server_env();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server_from_binary(&previous_binary, vec![], &server_env)
        .await?;
    let client = env.create_s3_client();
    env.create_test_bucket(LAYOUT_PLAIN_BUCKET).await?;
    env.create_test_bucket(LAYOUT_ENCRYPTED_BUCKET).await?;
    enable_versioning(&client, LAYOUT_PLAIN_BUCKET).await?;
    enable_versioning(&client, LAYOUT_ENCRYPTED_BUCKET).await?;
    put_default_sse_s3_encryption(&client, LAYOUT_ENCRYPTED_BUCKET).await?;

    let mut cases = layout_cases();
    for case in cases.iter_mut() {
        layout_write(&client, case).await?;
        let head = layout_head(&client, case).await?;
        case.source_etag = head
            .e_tag()
            .ok_or_else(|| format!("{}: previous-release HEAD omitted the ETag", case.label()))?
            .trim_matches('"')
            .to_string();
    }
    let (_target, transports) = replicate_layouts(&env, &client, &cases).await?;
    let summary: Vec<String> = cases
        .iter()
        .zip(&transports)
        .map(|(case, transport)| {
            format!(
                "{}: {} parts={:?} puts={}",
                case.label(),
                transport.status,
                transport.uploaded_parts,
                transport.single_puts
            )
        })
        .collect();
    tracing::info!(target: "e2e_test::upgrade_compatibility_test", ?summary, "previous-release baseline replication outcomes");
    Ok(())
}

/// backlog#2362 under the same conditions that reproduced it with the rc.5
/// writer, but with the workspace build on both sides so it runs in the
/// ordinary lane: every pre-existing layout is driven through exactly one
/// upload round even though the scanner re-scans it every second while the
/// first round is still in flight.
#[tokio::test]
async fn existing_object_replication_drives_each_layout_once() -> TestResult {
    init_logging();
    let server_env = layout_server_env();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server_with_env(vec![], &server_env).await?;
    let writer = env.create_s3_client();
    env.create_test_bucket(LAYOUT_PLAIN_BUCKET).await?;
    env.create_test_bucket(LAYOUT_ENCRYPTED_BUCKET).await?;
    enable_versioning(&writer, LAYOUT_PLAIN_BUCKET).await?;
    enable_versioning(&writer, LAYOUT_ENCRYPTED_BUCKET).await?;
    put_default_sse_s3_encryption(&writer, LAYOUT_ENCRYPTED_BUCKET).await?;

    let mut cases = layout_cases();
    for case in cases.iter_mut() {
        layout_write(&writer, case).await?;
        let head = layout_head(&writer, case).await?;
        case.source_etag = head
            .e_tag()
            .ok_or_else(|| format!("{}: HEAD omitted the ETag", case.label()))?
            .trim_matches('"')
            .to_string();
    }

    // The objects come from an earlier process lifetime: the scanner starts
    // cold and every object is a candidate at once.
    env.restart_server_preserving_data(vec![], &server_env).await?;
    let client = env.create_s3_client();
    let (_target, transports) = replicate_layouts(&env, &client, &cases).await?;
    let mut duplicates = Vec::new();
    for (case, transport) in cases.iter().zip(&transports) {
        assert_eq!(
            transport.status,
            "COMPLETED",
            "{}: existing-object replication must complete",
            case.label()
        );
        let rounds = if case.is_multipart_layout() {
            transport.completes
        } else {
            transport.single_puts
        };
        if rounds != 1 {
            duplicates.push(format!("{}: {rounds} upload rounds; journal {:?}", case.label(), transport.journal));
        }
    }
    assert!(duplicates.is_empty(), "each existing object must be driven exactly once: {duplicates:?}");
    Ok(())
}
