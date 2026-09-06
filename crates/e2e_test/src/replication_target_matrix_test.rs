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

//! Outbound target matrix: every object shape RustFS replicates, against
//! every remote-target failure mode the fake target models.
//!
//! The matrix exists because a fix for one target class shipped a regression
//! for another (rustfs#6895 fixed rustfs#6853 and caused rustfs#7082; see
//! `docs/postmortems/2026-09-03-replication-checksum-default-regression.md`).
//! Each row is one target mode with its own RustFS source and fake target;
//! each cell is one object shape. [`expectation`] is the single place that
//! says what a cell must do today:
//!
//! - `Completed` cells must replicate and the target must hold the source
//!   bytes; the journal must also show the wire shape the cell relies on.
//! - `KnownFailing` cells pin an open issue. They must fail for the recorded
//!   reason, and the moment they start passing the test fails with an XPASS
//!   message so the expectation is flipped in the same PR as the fix.
//!
//! Adding a target behavior the fleet has shown: add the mode to the fake
//! target, add a row here, and record any cell that is red before the fix.

use crate::common::{init_logging, replication_fast_env};
use crate::fake_s3_target::{BucketMode, FAKE_ACCESS_KEY, FAKE_SECRET_KEY};
use crate::fake_s3_target::{FakeS3Target, Operation as FakeTargetOperation, RequestRecord};
use crate::on_demand_migration::common::{OdmEnvOptions, OdmTestEnv, fake_source_client};
use crate::replication_extension_test::{
    LOOPBACK_REPLICATION_TARGET_ENV, ReplicationTargetOptions, enable_bucket_versioning, put_bucket_replication,
    set_replication_target_with_options,
};
use aws_sdk_s3::Client;
use aws_sdk_s3::primitives::{ByteStream, DateTime};
use aws_sdk_s3::types::{
    Checksum, CompletedMultipartUpload, CompletedPart, ObjectAttributes, ObjectLockLegalHoldStatus, ObjectLockMode,
};
use bytes::Bytes;
use std::error::Error;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::time::{Duration, sleep, timeout};

type TestResult = Result<(), Box<dyn Error + Send + Sync>>;

/// A remote-target behavior the fleet has shown, as the fake target models it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TargetMode {
    /// RustFS / MinIO-like target: adopts source version ids, decodes any
    /// framing, enforces no checksum rule.
    Baseline,
    /// SeaweedFS 3.97 (rustfs#6853): refuses `aws-chunked` bodies. A sender
    /// that frames its uploads gets a hard failure here instead of a
    /// silently corrupted replica.
    RejectAwsChunked,
    /// AWS S3 / MinIO / Impossible Cloud (rustfs#7082): a PutObject with
    /// Object Lock parameters must carry `Content-MD5` or `x-amz-checksum-*`.
    RequireChecksumWithObjectLock,
    /// AWS S3 / Wasabi / Impossible Cloud: mints its own version ids
    /// (rustfs/backlog#2085). Data must still land.
    MintOwnVersionIds,
}

impl TargetMode {
    const ALL: [TargetMode; 4] = [
        TargetMode::Baseline,
        TargetMode::RejectAwsChunked,
        TargetMode::RequireChecksumWithObjectLock,
        TargetMode::MintOwnVersionIds,
    ];

    fn apply(self, target: &FakeS3Target) {
        match self {
            TargetMode::Baseline => {}
            TargetMode::RejectAwsChunked => target.reject_aws_chunked_uploads(true),
            TargetMode::RequireChecksumWithObjectLock => target.require_checksum_for_object_lock(true),
            TargetMode::MintOwnVersionIds => target.assign_own_version_ids(true),
        }
    }

    fn slug(self) -> &'static str {
        match self {
            TargetMode::Baseline => "baseline",
            TargetMode::RejectAwsChunked => "reject-aws-chunked",
            TargetMode::RequireChecksumWithObjectLock => "require-checksum-object-lock",
            TargetMode::MintOwnVersionIds => "mint-own-version-ids",
        }
    }
}

/// An object shape the replication transport treats differently.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ObjectShape {
    /// The exact rustfs#7082 reproduction: a zero-byte object.
    Empty,
    /// Small single-part object with no Object Lock parameters.
    Plain,
    /// Single-part object with a GOVERNANCE retention period.
    Retention,
    /// Single-part object with legal hold ON.
    LegalHold,
    /// Two-part multipart upload, no Object Lock parameters.
    Multipart,
    /// Two-part multipart upload with a GOVERNANCE retention period; the
    /// lock headers travel on CreateMultipartUpload, which has no body.
    LockedMultipart,
    /// ODM stores two local parts while preserving a single-PUT source's MD5 ETag.
    OdmPreservedMd5Multipart,
}

impl ObjectShape {
    const ALL: [ObjectShape; 7] = [
        ObjectShape::Empty,
        ObjectShape::Plain,
        ObjectShape::Retention,
        ObjectShape::LegalHold,
        ObjectShape::Multipart,
        ObjectShape::LockedMultipart,
        ObjectShape::OdmPreservedMd5Multipart,
    ];

    fn key(self) -> &'static str {
        match self {
            ObjectShape::Empty => "matrix/empty.bin",
            ObjectShape::Plain => "matrix/plain.bin",
            ObjectShape::Retention => "matrix/retention.bin",
            ObjectShape::LegalHold => "matrix/legal-hold.bin",
            ObjectShape::Multipart => "matrix/multipart.bin",
            ObjectShape::LockedMultipart => "matrix/locked-multipart.bin",
            ObjectShape::OdmPreservedMd5Multipart => "matrix/odm-preserved-md5.bin",
        }
    }

    fn carries_object_lock_params(self) -> bool {
        matches!(self, ObjectShape::Retention | ObjectShape::LegalHold | ObjectShape::LockedMultipart)
    }

    /// Upload the shape to the source and return the bytes the target must
    /// end up holding.
    async fn put(self, env: &OdmTestEnv, bucket: &str) -> Result<Bytes, Box<dyn Error + Send + Sync>> {
        let client = &env.client;
        let key = self.key();
        match self {
            ObjectShape::Empty => {
                client
                    .put_object()
                    .bucket(bucket)
                    .key(key)
                    .body(ByteStream::from_static(b""))
                    .send()
                    .await?;
                Ok(Bytes::new())
            }
            ObjectShape::Plain => {
                let body = payload(64 * 1024, 0x11);
                client
                    .put_object()
                    .bucket(bucket)
                    .key(key)
                    .body(ByteStream::from(body.clone()))
                    .send()
                    .await?;
                Ok(body)
            }
            ObjectShape::Retention => {
                let body = payload(48 * 1024, 0x22);
                client
                    .put_object()
                    .bucket(bucket)
                    .key(key)
                    .body(ByteStream::from(body.clone()))
                    .object_lock_mode(ObjectLockMode::Governance)
                    .object_lock_retain_until_date(retain_until())
                    .send()
                    .await?;
                Ok(body)
            }
            ObjectShape::LegalHold => {
                let body = payload(32 * 1024, 0x33);
                client
                    .put_object()
                    .bucket(bucket)
                    .key(key)
                    .body(ByteStream::from(body.clone()))
                    .object_lock_legal_hold_status(ObjectLockLegalHoldStatus::On)
                    .send()
                    .await?;
                Ok(body)
            }
            ObjectShape::Multipart => multipart_put(client, bucket, key, 0x44, false).await,
            ObjectShape::LockedMultipart => multipart_put(client, bucket, key, 0x55, true).await,
            ObjectShape::OdmPreservedMd5Multipart => odm_preserved_md5_multipart(env, bucket, key).await,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Expectation {
    /// Replicates COMPLETED and the target holds the source bytes.
    Completed,
    /// Replicates FAILED today for a recorded reason; pinned to an open issue.
    KnownFailing(&'static str),
}

/// The cells that are red today, each pinned to the open issue that owns it.
/// This is the single source of truth: a fix that turns a cell green must
/// remove its entry in the same PR, and [`check_known_failing_cell`] refuses
/// an unexpected pass so the table cannot go stale silently. rustfs#7082
/// (Retention and LegalHold against the checksum-requiring target) lived
/// here until the replication PUT started carrying a Content-MD5 derived
/// from the source ETag.
const KNOWN_FAILING_CELLS: &[(TargetMode, ObjectShape, &str)] = &[];

fn expectation(mode: TargetMode, shape: ObjectShape) -> Expectation {
    KNOWN_FAILING_CELLS
        .iter()
        .find(|(known_mode, known_shape, _)| *known_mode == mode && *known_shape == shape)
        .map(|(_, _, issue)| Expectation::KnownFailing(issue))
        .unwrap_or(Expectation::Completed)
}

#[tokio::test]
async fn matrix_baseline_target() -> TestResult {
    run_row(TargetMode::Baseline).await
}

#[tokio::test]
async fn matrix_reject_aws_chunked_target() -> TestResult {
    run_row(TargetMode::RejectAwsChunked).await
}

#[tokio::test]
async fn matrix_require_checksum_with_object_lock_target() -> TestResult {
    run_row(TargetMode::RequireChecksumWithObjectLock).await
}

#[tokio::test]
async fn matrix_mint_own_version_ids_target() -> TestResult {
    run_row(TargetMode::MintOwnVersionIds).await
}

/// Every known-red entry must name a real cell and an issue, and the lookup
/// must round-trip, so a stale or mistyped entry cannot silently pin nothing.
#[test]
fn known_failing_table_names_real_cells() {
    for (mode, shape, issue) in KNOWN_FAILING_CELLS {
        assert!(
            TargetMode::ALL.contains(mode) && ObjectShape::ALL.contains(shape),
            "{mode:?}/{shape:?} is not a matrix cell"
        );
        assert!(
            issue.starts_with("rustfs#") || issue.starts_with("rustfs/backlog#"),
            "{issue} must name an open issue"
        );
        assert_eq!(expectation(*mode, *shape), Expectation::KnownFailing(issue));
    }
    let red_cells = TargetMode::ALL
        .iter()
        .flat_map(|mode| ObjectShape::ALL.iter().map(move |shape| (*mode, *shape)))
        .filter(|(mode, shape)| matches!(expectation(*mode, *shape), Expectation::KnownFailing(_)))
        .count();
    assert_eq!(red_cells, KNOWN_FAILING_CELLS.len());
}

async fn run_row(mode: TargetMode) -> TestResult {
    init_logging();

    let target = FakeS3Target::start().await?;
    let target_bucket = format!("matrix-{}-dst", mode.slug());
    target.create_bucket_with_object_lock(target_bucket.clone());
    mode.apply(&target);

    let mut env_vars = replication_fast_env();
    env_vars.extend_from_slice(LOOPBACK_REPLICATION_TARGET_ENV);
    env_vars.extend_from_slice(&[("NO_PROXY", "127.0.0.1,localhost"), ("HTTP_PROXY", ""), ("HTTPS_PROXY", "")]);
    let env = OdmTestEnv::start_with(OdmEnvOptions {
        env: env_vars,
        ..OdmEnvOptions::default()
    })
    .await?;
    let source_env = &env.rustfs;

    let source_bucket = format!("matrix-{}-src", mode.slug());
    let source_client = source_env.create_s3_client();
    source_client
        .create_bucket()
        .bucket(&source_bucket)
        .object_lock_enabled_for_bucket(true)
        .send()
        .await?;
    enable_bucket_versioning(source_env, &source_bucket).await?;
    let target_arn = set_replication_target_with_options(
        source_env,
        &source_bucket,
        ReplicationTargetOptions {
            endpoint: &target.address(),
            access_key: FAKE_ACCESS_KEY,
            secret_key: FAKE_SECRET_KEY,
            target_bucket: &target_bucket,
            secure: false,
            skip_tls_verify: false,
            ca_cert_pem: None,
        },
    )
    .await?;
    put_bucket_replication(source_env, &source_bucket, &target_arn).await?;

    let target_client = fake_source_client(&target);
    let mut failures = Vec::new();
    for shape in ObjectShape::ALL {
        let cell = format!("{}/{:?}", mode.slug(), shape);
        let expected_body = shape.put(&env, &source_bucket).await?;
        let status = wait_for_terminal_replication_status(&source_client, &source_bucket, shape.key()).await?;
        if shape == ObjectShape::OdmPreservedMd5Multipart {
            assert_eq!(
                env.source.count_requests(FakeTargetOperation::GetObject, shape.key()),
                2,
                "one passthrough GET plus one background pull; replication must read the persisted local parts"
            );
        }
        let journal = target.requests();
        let outcome = match expectation(mode, shape) {
            Expectation::Completed => {
                check_completed_cell(&cell, &status, &target_client, &target_bucket, shape, &expected_body, &journal).await
            }
            Expectation::KnownFailing(issue) => check_known_failing_cell(&cell, issue, &status, shape, &journal),
        };
        if let Err(err) = outcome {
            failures.push(format!("{cell}: {err}"));
        }
    }

    target.shutdown().await;
    if failures.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "{} matrix cell(s) violated their expectation:\n  {}",
            failures.len(),
            failures.join("\n  ")
        )
        .into())
    }
}

async fn check_completed_cell(
    cell: &str,
    status: &str,
    target_client: &Client,
    target_bucket: &str,
    shape: ObjectShape,
    expected_body: &Bytes,
    journal: &[RequestRecord],
) -> TestResult {
    if status != "COMPLETED" {
        return Err(format!("expected COMPLETED, source reports {status}").into());
    }
    let stored = target_client
        .get_object()
        .bucket(target_bucket)
        .key(shape.key())
        .send()
        .await
        .map_err(|err| format!("target GET failed after COMPLETED: {err}"))?
        .body
        .collect()
        .await?
        .into_bytes();
    if stored != *expected_body {
        return Err(format!(
            "target holds {} bytes that differ from the {} source bytes (COMPLETED over a corrupted replica)",
            stored.len(),
            expected_body.len()
        )
        .into());
    }
    // The wire shape the cell relies on: plain signed payloads (rustfs#6853)
    // for every upload of this key, and the lock headers present exactly when
    // the shape carries them.
    let uploads: Vec<&RequestRecord> = journal
        .iter()
        .filter(|record| {
            record.key.as_deref() == Some(shape.key())
                && matches!(
                    record.operation,
                    FakeTargetOperation::PutObject | FakeTargetOperation::UploadPart | FakeTargetOperation::CreateMultipartUpload
                )
        })
        .collect();
    if uploads.is_empty() {
        return Err("no upload reached the target although the source reports COMPLETED".into());
    }
    if shape == ObjectShape::OdmPreservedMd5Multipart {
        let key_requests: Vec<_> = journal
            .iter()
            .filter(|record| record.key.as_deref() == Some(shape.key()))
            .collect();
        for operation in [
            FakeTargetOperation::CreateMultipartUpload,
            FakeTargetOperation::CompleteMultipartUpload,
        ] {
            if !key_requests.iter().any(|record| record.operation == operation) {
                return Err(format!("preserved-MD5 multipart object did not use {operation:?}").into());
            }
        }
        if key_requests
            .iter()
            .any(|record| record.operation == FakeTargetOperation::PutObject)
        {
            return Err("preserved-MD5 multipart object used a single PutObject".into());
        }
        let mut part_numbers: Vec<_> = key_requests
            .iter()
            .filter(|record| record.operation == FakeTargetOperation::UploadPart)
            .map(|record| record.part_number)
            .collect();
        part_numbers.sort_unstable();
        part_numbers.dedup();
        if part_numbers != [Some(1), Some(2)] {
            return Err(format!("preserved-MD5 multipart object uploaded unexpected parts: {part_numbers:?}").into());
        }
    }
    if let Some(framed) = uploads.iter().find(|record| record.transport.aws_chunked) {
        return Err(format!("{cell}: an upload went out aws-chunked (rustfs#6853 framing): {framed:?}").into());
    }
    let lock_headers_seen = uploads.iter().any(|record| record.transport.object_lock_params);
    if lock_headers_seen != shape.carries_object_lock_params() {
        return Err(format!(
            "object lock headers on the wire: {lock_headers_seen}, shape carries them: {}",
            shape.carries_object_lock_params()
        )
        .into());
    }
    // rustfs#7082 contract: every PutObject that carries Object Lock
    // parameters also carries Content-MD5 or an x-amz-checksum-* header,
    // whatever the target's own policy is.
    if let Some(bare) = uploads.iter().find(|record| {
        record.operation == FakeTargetOperation::PutObject
            && record.transport.object_lock_params
            && record.transport.content_md5.is_none()
            && record.transport.checksum_headers.is_empty()
    }) {
        return Err(format!("a locked PutObject went out without any integrity header (rustfs#7082): {bare:?}").into());
    }
    Ok(())
}

fn check_known_failing_cell(cell: &str, issue: &str, status: &str, shape: ObjectShape, journal: &[RequestRecord]) -> TestResult {
    if status == "COMPLETED" {
        return Err(format!(
            "XPASS: {cell} reached COMPLETED but the expectation table pins it to {issue}; \
             the fix landed, so flip this cell to Expectation::Completed in the same PR"
        )
        .into());
    }
    if status != "FAILED" {
        return Err(format!("expected FAILED ({issue}), source reports {status}").into());
    }
    // Fail for the recorded reason, not by accident: the PUT carried the lock
    // headers and no integrity header at all.
    let rejected = journal.iter().any(|record| {
        record.operation == FakeTargetOperation::PutObject
            && record.key.as_deref() == Some(shape.key())
            && record.transport.object_lock_params
            && record.transport.content_md5.is_none()
            && record.transport.checksum_headers.is_empty()
    });
    if !rejected {
        return Err(format!(
            "FAILED, but not for the {issue} reason (a locked PUT without Content-MD5 / x-amz-checksum-*); journal: {journal:?}"
        )
        .into());
    }
    Ok(())
}

/// First terminal replication status (`COMPLETED` or `FAILED`) the source
/// reports for the key.
async fn wait_for_terminal_replication_status(
    client: &Client,
    bucket: &str,
    key: &str,
) -> Result<String, Box<dyn Error + Send + Sync>> {
    let wait = async {
        loop {
            let head = client.head_object().bucket(bucket).key(key).send().await?;
            match head.replication_status().map(|status| status.as_str().to_string()) {
                Some(status) if status == "COMPLETED" || status == "FAILED" => return Ok(status),
                _ => sleep(Duration::from_millis(200)).await,
            }
        }
    };
    match timeout(Duration::from_secs(90), wait).await {
        Ok(result) => result,
        Err(_) => Err(format!("{key} reached no terminal replication status within 90 seconds").into()),
    }
}

async fn odm_preserved_md5_multipart(env: &OdmTestEnv, bucket: &str, key: &str) -> Result<Bytes, Box<dyn Error + Send + Sync>> {
    const PART_SIZE: usize = 5 * 1024 * 1024;
    let origin_bucket = format!("{bucket}-origin");
    env.source.create_bucket_with_mode(&origin_bucket, BucketMode::Unversioned);
    let mut spec = env.fake_source_spec(&origin_bucket);
    // Below the 16 MiB inline default the pull is one tee'd PUT with a single
    // part; force the passthrough + background multipart write-back instead.
    spec.policy.inline_max_bytes = 4096;
    spec.policy.multipart_part_size_bytes = PART_SIZE as u64;
    spec.policy.preserve_etag = true;
    env.configure_and_wait(bucket, &spec).await?;

    // A normal source PUT produces the MD5 ETag; only ODM chooses the local parts.
    let body = payload(PART_SIZE + 4096, 0x66);
    let source_put = env
        .source_client()
        .put_object()
        .bucket(&origin_bucket)
        .key(key)
        .body(ByteStream::from(body.clone()))
        .send()
        .await?;
    let source_etag = source_put.e_tag().ok_or("source PUT omitted its ETag")?.trim_matches('"');
    assert_eq!(source_etag.len(), 32, "source fixture must have a single-PUT MD5 ETag");
    assert!(source_etag.bytes().all(|byte| byte.is_ascii_hexdigit()));

    let pulled = env.raw_get(bucket, key).await?;
    assert_eq!(pulled.status, 200, "{}", String::from_utf8_lossy(&pulled.body));
    assert_eq!(pulled.body, body);
    assert!(
        env.wait_local_listed(bucket, key, Duration::from_secs(30)).await?,
        "ODM must persist the object"
    );
    let attributes = env
        .client
        .get_object_attributes()
        .bucket(bucket)
        .key(key)
        .object_attributes(ObjectAttributes::Etag)
        .object_attributes(ObjectAttributes::ObjectParts)
        .object_attributes(ObjectAttributes::Checksum)
        .send()
        .await?;
    assert_eq!(attributes.e_tag().map(|etag| etag.trim_matches('"')), Some(source_etag));
    let parts = attributes
        .object_parts()
        .ok_or("the ODM copy must expose its two local parts")?;
    assert_eq!(parts.total_parts_count(), Some(2));
    assert_eq!(
        parts
            .parts()
            .iter()
            .map(|part| (part.part_number(), part.size()))
            .collect::<Vec<_>>(),
        [(Some(1), Some(PART_SIZE as i64)), (Some(2), Some(4096))]
    );
    assert!(
        attributes
            .checksum()
            .is_none_or(|checksum| checksum == &Checksum::builder().build()),
        "multipart routing must work without an object checksum record"
    );
    Ok(body)
}

async fn multipart_put(
    client: &Client,
    bucket: &str,
    key: &str,
    fill: u8,
    locked: bool,
) -> Result<Bytes, Box<dyn Error + Send + Sync>> {
    let part_one = payload(5 * 1024 * 1024, fill);
    let part_two = payload(256 * 1024, fill.wrapping_add(1));
    let mut create = client.create_multipart_upload().bucket(bucket).key(key);
    if locked {
        create = create
            .object_lock_mode(ObjectLockMode::Governance)
            .object_lock_retain_until_date(retain_until());
    }
    let upload_id = create
        .send()
        .await?
        .upload_id()
        .ok_or("CreateMultipartUpload returned no upload id")?
        .to_string();
    let mut completed = Vec::new();
    for (number, part) in [(1, &part_one), (2, &part_two)] {
        let etag = client
            .upload_part()
            .bucket(bucket)
            .key(key)
            .upload_id(&upload_id)
            .part_number(number)
            .body(ByteStream::from(part.clone()))
            .send()
            .await?
            .e_tag()
            .ok_or("UploadPart returned no ETag")?
            .to_string();
        completed.push(CompletedPart::builder().part_number(number).e_tag(etag).build());
    }
    client
        .complete_multipart_upload()
        .bucket(bucket)
        .key(key)
        .upload_id(&upload_id)
        .multipart_upload(CompletedMultipartUpload::builder().set_parts(Some(completed)).build())
        .send()
        .await?;
    let mut body = Vec::with_capacity(part_one.len() + part_two.len());
    body.extend_from_slice(&part_one);
    body.extend_from_slice(&part_two);
    Ok(Bytes::from(body))
}

fn payload(len: usize, fill: u8) -> Bytes {
    Bytes::from((0..len).map(|i| fill.wrapping_add((i % 251) as u8)).collect::<Vec<u8>>())
}

fn retain_until() -> DateTime {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after epoch")
        .as_secs();
    DateTime::from_secs(now as i64 + 86_400)
}
