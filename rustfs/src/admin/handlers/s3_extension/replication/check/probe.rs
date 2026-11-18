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

//! The probe mutations `replication-check` runs against one target: a single
//! and a multipart probe object, an SSE-C passthrough probe, a delete marker,
//! version deletes, and the cleanup of every probe artifact.
//!
//! Not responsible for: choosing targets or building the report (`super`).

#[cfg(test)]
mod tests;

use super::{
    REPLICATION_CHECK_CODE_SSEC_PASSTHROUGH, REPLICATION_CHECK_CODE_VERSION_MISMATCH, REPLICATION_CHECK_PROBE_PREFIX,
    REPLICATION_CHECK_SSEC_PROBE_KEY_MD5, ReplicationCheckFailureContext, ReplicationCheckPhaseStatus,
    ReplicationCheckTargetStatus, bound_replication_check_error, fail_replication_check_target,
    format_replication_check_client_error,
};
use crate::admin::storage_api::bucket::replication::ReplicationStatusType;
use crate::admin::storage_api::bucket::target::BucketTarget;
use crate::admin::storage_api::bucket::target_sys::{
    BucketTargetSys, PutObjectOptions, RemoveObjectOptions, S3ClientError, TargetClient, append_version_id_query,
    resolve_delete_api_version_id,
};
use aws_sdk_s3::primitives::ByteStream as AwsByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use http::header::HeaderName;
use http::{HeaderMap, HeaderValue};
use rustfs_utils::http::object_encryption_keys::{
    REPLICATION_SSEC_ALGORITHM_HEADER, REPLICATION_SSEC_KEY_MD5_HEADER, REPLICATION_SSEC_ORIGINAL_SIZE_HEADER,
};
use rustfs_utils::http::{
    SUFFIX_SOURCE_DELETEMARKER, SUFFIX_SOURCE_MTIME, SUFFIX_SOURCE_REPLICATION_CHECK, SUFFIX_SOURCE_REPLICATION_REQUEST,
    SUFFIX_SOURCE_VERSION_ID, insert_header,
};
use std::collections::HashSet;
use std::sync::Arc;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use uuid::Uuid;

/// The probe PUT reports both sides of the version-identity contract: the
/// source version id it sent (header + `?versionId=` query, the exact shape
/// live replication uses) and the version id the target answered with.
pub(super) struct ReplicationProbePutOutcome {
    sent_version_id: String,
    response_version_id: Option<String>,
}

/// Outcome of the SSE-C passthrough probe: whether the HEAD-back of the probe
/// replica echoed SSE-C evidence (the customer-algorithm header a RustFS
/// target restores from the passthrough transport headers), plus the version
/// the target assigned so cleanup can address it.
pub(super) struct ReplicationSsecProbeOutcome {
    evidence_present: bool,
    version_id: Option<String>,
}

pub(super) struct ReplicationProbeMultipartError {
    primary: S3ClientError,
    cleanup_error: Option<String>,
}

impl From<S3ClientError> for ReplicationProbeMultipartError {
    fn from(primary: S3ClientError) -> Self {
        Self {
            primary,
            cleanup_error: None,
        }
    }
}

#[async_trait::async_trait]
pub(super) trait ReplicationProbeOperations {
    async fn put(&mut self) -> Result<ReplicationProbePutOutcome, S3ClientError>;
    /// Multipart decides the target version at initiate time and only reports
    /// it on completion, so the identity contract has to be probed separately
    /// there: a target can adopt PutObject version ids and still mint its own
    /// for CreateMultipartUpload.
    async fn multipart_put(&mut self) -> Result<ReplicationProbePutOutcome, ReplicationProbeMultipartError>;
    /// PUT a probe version carrying the SSE-C passthrough transport headers,
    /// HEAD it back through the replication-check channel, and report whether
    /// the SSE-C evidence survived. Cleanup of the created version is the
    /// caller's job (the outcome carries its version id).
    async fn ssec_passthrough_probe(&mut self) -> Result<ReplicationSsecProbeOutcome, S3ClientError>;
    async fn create_delete_marker(&mut self, version_id: Option<&str>) -> Result<Option<String>, S3ClientError>;
    async fn delete_version(&mut self, version_id: Option<&str>) -> Result<(), S3ClientError>;
    async fn cleanup(&mut self, known_version_ids: [Option<&str>; 4]) -> Result<(), String>;
}

pub(super) struct RemoteReplicationProbeOperations<'a> {
    pub(super) client: &'a TargetClient,
    pub(super) bucket: &'a str,
    pub(super) key: &'a str,
    pub(super) time: OffsetDateTime,
}

#[async_trait::async_trait]
impl ReplicationProbeOperations for RemoteReplicationProbeOperations<'_> {
    async fn put(&mut self) -> Result<ReplicationProbePutOutcome, S3ClientError> {
        put_replication_probe_object(self.client, self.bucket, self.key, self.time).await
    }

    async fn multipart_put(&mut self) -> Result<ReplicationProbePutOutcome, ReplicationProbeMultipartError> {
        multipart_put_replication_probe_object(self.client, self.bucket, self.key, self.time).await
    }

    async fn ssec_passthrough_probe(&mut self) -> Result<ReplicationSsecProbeOutcome, S3ClientError> {
        ssec_passthrough_probe_object(self.client, self.bucket, self.key, self.time).await
    }

    async fn create_delete_marker(&mut self, version_id: Option<&str>) -> Result<Option<String>, S3ClientError> {
        delete_replication_probe_object(
            self.client,
            self.bucket,
            self.key,
            version_id,
            build_replication_probe_remove_options(self.time, true),
        )
        .await
    }

    async fn delete_version(&mut self, version_id: Option<&str>) -> Result<(), S3ClientError> {
        delete_replication_probe_object(
            self.client,
            self.bucket,
            self.key,
            version_id,
            build_replication_probe_remove_options(self.time, false),
        )
        .await
        .map(|_| ())
    }

    async fn cleanup(&mut self, known_version_ids: [Option<&str>; 4]) -> Result<(), String> {
        cleanup_replication_probe(self.client, self.bucket, self.key, known_version_ids).await
    }
}

/// `None` when the target adopted the source version id on this path.
fn version_fidelity_error(api: &str, outcome: &ReplicationProbePutOutcome) -> Option<String> {
    if outcome.response_version_id.as_deref() == Some(outcome.sent_version_id.as_str()) {
        return None;
    }
    Some(format!(
        "target assigned version id {} instead of adopting the source version id {} on {api}; \
         version-addressed replication (version deletes, heal) cannot converge on this target",
        outcome.response_version_id.as_deref().unwrap_or("<none>"),
        outcome.sent_version_id,
    ))
}

pub(super) async fn execute_replication_probe(
    result: &mut ReplicationCheckTargetStatus,
    operations: &mut impl ReplicationProbeOperations,
) {
    let mut probe_version_id = None;
    let mut multipart_probe_version_id = None;
    let mut ssec_probe_version_id = None;
    let mut delete_marker_version_id = None;
    let mut cleanup_required = true;
    let mut multipart_cleanup_error = None;

    match operations.put().await {
        Ok(outcome) => {
            result.phases.put = ReplicationCheckPhaseStatus::passed();
            // P1-19 version-identity contract: a target that mints its own
            // version ids never answers to the source id. Judge it from the
            // probe PUT's own response. The mutation phases below still run
            // on such a target: they address the id the target assigned —
            // the same ledger the replication worker records per object
            // (rustfs/backlog#2340) — so they report whether version-
            // addressed deletes can converge there at all.
            match version_fidelity_error("PutObject", &outcome) {
                None => result.phases.version_fidelity = ReplicationCheckPhaseStatus::passed(),
                Some(error) => {
                    result.phases.version_fidelity =
                        ReplicationCheckPhaseStatus::failed_with_code(&error, REPLICATION_CHECK_CODE_VERSION_MISMATCH);
                    fail_replication_check_target(result, error);
                }
            }
            probe_version_id = outcome.response_version_id;
        }
        Err(err) => {
            let error = format_replication_check_client_error(&err, ReplicationCheckFailureContext::ReplicateObject);
            result.phases.put = ReplicationCheckPhaseStatus::failed(&error);
            fail_replication_check_target(result, error);
            // The conditional PUT cannot have created an artifact when the
            // target reports a collision with a concurrently-created key.
            cleanup_required = err.code.as_deref() != Some("PreconditionFailed");
        }
    }

    // The multipart path fixes the target version at initiate and only
    // reports it on completion, so a target can adopt PutObject ids and still
    // mint its own here — probe it before declaring the contract met.
    if result.phases.version_fidelity.status == "OK" {
        match operations.multipart_put().await {
            Ok(outcome) => {
                multipart_probe_version_id = outcome.response_version_id.clone();
                if let Some(error) = version_fidelity_error("CreateMultipartUpload", &outcome) {
                    result.phases.version_fidelity =
                        ReplicationCheckPhaseStatus::failed_with_code(&error, REPLICATION_CHECK_CODE_VERSION_MISMATCH);
                    fail_replication_check_target(result, error);
                }
            }
            Err(err) => {
                let error = format_replication_check_client_error(&err.primary, ReplicationCheckFailureContext::ReplicateObject);
                result.phases.version_fidelity = ReplicationCheckPhaseStatus::failed(&error);
                fail_replication_check_target(result, error);
                multipart_cleanup_error = err.cleanup_error;
            }
        }
    }

    // N2: probe SSE-C passthrough with the same transport headers live
    // replication sends. A target that drops them (MinIO, generic S3) stores
    // the probe as a plain object and echoes no SSE-C evidence on the
    // HEAD-back; SSE-C replicas there would silently lose their decryption
    // material, so the target must be flagged with a machine-readable code.
    // Deliberately unlike VersionFidelity, a failed SsecPassthrough phase
    // does NOT fail the target overall: version-identity drift breaks the
    // replication contract for every object, while dropped SSE-C passthrough
    // headers only limit a capability — a plaintext-only deployment against a
    // MinIO target is perfectly healthy and must not turn red. The phase's
    // own FAILED + machine-readable Code remains for madmin consumers (and
    // the verdict still reaches the runtime capability cache).
    if result.phases.put.status == "OK" && result.phases.version_fidelity.status == "OK" {
        match operations.ssec_passthrough_probe().await {
            Ok(outcome) => {
                ssec_probe_version_id = outcome.version_id;
                if outcome.evidence_present {
                    result.phases.ssec_passthrough = ReplicationCheckPhaseStatus::passed();
                } else {
                    let error = "target drops SSE-C passthrough replication headers; \
                         SSE-C replicas would lose their decryption material on this target";
                    result.phases.ssec_passthrough =
                        ReplicationCheckPhaseStatus::failed_with_code(error, REPLICATION_CHECK_CODE_SSEC_PASSTHROUGH);
                }
            }
            Err(err) => {
                let error = format_replication_check_client_error(&err, ReplicationCheckFailureContext::ReplicateObject);
                result.phases.ssec_passthrough = ReplicationCheckPhaseStatus::failed(&error);
            }
        }
    }

    // DeleteMarker / VersionDelete address `probe_version_id`, the id the
    // target actually assigned, so they run on a drifting target too. What
    // they cannot prove there is the source-id addressing VersionFidelity
    // already failed; what they do prove is that the ledger-addressed purge
    // path the worker uses on such a target works against this endpoint.
    if result.phases.put.status == "OK" && probe_version_id.is_some() {
        match operations.create_delete_marker(probe_version_id.as_deref()).await {
            Ok(version_id) => {
                delete_marker_version_id = version_id;
                result.phases.delete_marker = ReplicationCheckPhaseStatus::passed();
            }
            Err(err) => {
                let error = format_replication_check_client_error(&err, ReplicationCheckFailureContext::ReplicateDeleteMarker);
                result.phases.delete_marker = ReplicationCheckPhaseStatus::failed(&error);
                fail_replication_check_target(result, error);
            }
        }

        match operations.delete_version(probe_version_id.as_deref()).await {
            Ok(()) => result.phases.version_delete = ReplicationCheckPhaseStatus::passed(),
            Err(err) => {
                let error = format_replication_check_client_error(&err, ReplicationCheckFailureContext::DeleteObjectVersion);
                result.phases.version_delete = ReplicationCheckPhaseStatus::failed(&error);
                fail_replication_check_target(result, error);
            }
        }
    }

    let cleanup_result = if cleanup_required {
        operations
            .cleanup([
                probe_version_id.as_deref(),
                multipart_probe_version_id.as_deref(),
                ssec_probe_version_id.as_deref(),
                delete_marker_version_id.as_deref(),
            ])
            .await
    } else {
        Ok(())
    };

    let mut cleanup_errors = Vec::new();
    if let Some(error) = multipart_cleanup_error {
        cleanup_errors.push(error);
    }
    if let Err(error) = cleanup_result {
        cleanup_errors.push(error);
    }
    if cleanup_errors.is_empty() {
        result.phases.cleanup = ReplicationCheckPhaseStatus::passed();
    } else {
        let error = cleanup_errors.join("; ");
        result.phases.cleanup = ReplicationCheckPhaseStatus::failed(&error);
        fail_replication_check_target(result, format!("probe cleanup failed: {error}"));
    }
}

fn new_replication_probe_key() -> String {
    format!("{REPLICATION_CHECK_PROBE_PREFIX}{}/{}", Uuid::new_v4(), Uuid::new_v4())
}

pub(super) async fn allocate_replication_probe_key(target_client: &TargetClient, target_bucket: &str) -> Result<String, String> {
    for _ in 0..4 {
        let probe_key = new_replication_probe_key();
        let output = target_client
            .client
            .list_object_versions()
            .bucket(target_bucket)
            .prefix(&probe_key)
            .max_keys(2)
            .send()
            .await
            .map_err(|err| {
                format_replication_check_client_error(&S3ClientError::from(err), ReplicationCheckFailureContext::ReplicateObject)
            })?;
        let occupied = output
            .versions()
            .iter()
            .any(|version| version.key() == Some(probe_key.as_str()))
            || output
                .delete_markers()
                .iter()
                .any(|marker| marker.key() == Some(probe_key.as_str()));
        if !occupied {
            return Ok(probe_key);
        }
    }
    Err("could not allocate a collision-free replication probe key".to_string())
}

pub(super) async fn resolve_replication_target_client(bucket: &str, target: &BucketTarget) -> Result<Arc<TargetClient>, String> {
    let target_sys = BucketTargetSys::get();
    match target_sys.get_remote_target_client(bucket, &target.arn).await {
        Some(client) => Ok(client),
        None => target_sys
            .get_remote_target_client_internal(target)
            .await
            .map(Arc::new)
            .map_err(|err| err.to_string()),
    }
}

fn build_replication_probe_put_options(now: OffsetDateTime) -> PutObjectOptions {
    PutObjectOptions {
        internal: crate::admin::storage_api::bucket::target_sys::AdvancedPutOptions {
            source_version_id: Uuid::new_v4().to_string(),
            replication_status: ReplicationStatusType::Replica,
            source_mtime: now,
            replication_request: true,
            replication_validity_check: true,
            ..Default::default()
        },
        ..Default::default()
    }
}

fn build_replication_probe_remove_options(now: OffsetDateTime, replication_delete_marker: bool) -> RemoveObjectOptions {
    RemoveObjectOptions {
        force_delete: false,
        governance_bypass: false,
        replication_delete_marker,
        replication_mtime: Some(now),
        replication_status: ReplicationStatusType::Replica,
        replication_request: true,
        replication_validity_check: true,
    }
}

fn build_replication_probe_headers(options: &PutObjectOptions) -> HeaderMap {
    let mut headers = HeaderMap::new();
    insert_header(&mut headers, SUFFIX_SOURCE_VERSION_ID, &options.internal.source_version_id);
    insert_header(
        &mut headers,
        SUFFIX_SOURCE_MTIME,
        options.internal.source_mtime.format(&Rfc3339).unwrap_or_default(),
    );
    insert_header(&mut headers, SUFFIX_SOURCE_REPLICATION_REQUEST, "true");
    insert_header(&mut headers, SUFFIX_SOURCE_REPLICATION_CHECK, "true");
    headers.insert(
        HeaderName::from_static("x-amz-replication-status"),
        HeaderValue::from_static(ReplicationStatusType::Replica.as_str()),
    );
    headers
}

/// Probe the identity contract on the multipart path: initiate carrying the
/// source version as `?versionId=` (where the target fixes the version),
/// upload one small part, and read the version the completion reports.
async fn multipart_put_replication_probe_object(
    target_client: &TargetClient,
    target_bucket: &str,
    probe_key: &str,
    now: OffsetDateTime,
) -> Result<ReplicationProbePutOutcome, ReplicationProbeMultipartError> {
    let options = build_replication_probe_put_options(now);
    let sent_version_id = options.internal.source_version_id.clone();
    let headers = build_replication_probe_headers(&options);

    let initiate_headers = headers.clone();
    let initiate_version_id = sent_version_id.clone();
    let created = target_client
        .client
        .create_multipart_upload()
        .bucket(target_bucket)
        .key(probe_key)
        .customize()
        .map_request(move |mut req| {
            for (key, value) in initiate_headers.clone() {
                req.headers_mut().insert(key.expect("operation should succeed"), value);
            }
            let uri = append_version_id_query(req.uri(), &initiate_version_id);
            req.set_uri(uri).map_err(std::io::Error::other)?;
            Result::<_, std::io::Error>::Ok(req)
        })
        .send()
        .await
        .map_err(S3ClientError::from)
        .map_err(ReplicationProbeMultipartError::from)?;
    let upload_id = created
        .upload_id()
        .ok_or_else(|| S3ClientError::new("target multipart initiate returned no upload id"))
        .map_err(ReplicationProbeMultipartError::from)?
        .to_string();

    let uploaded = match target_client
        .client
        .upload_part()
        .bucket(target_bucket)
        .key(probe_key)
        .upload_id(&upload_id)
        .part_number(1)
        .content_length(8)
        .body(AwsByteStream::from_static(b"aaaaaaaa"))
        .send()
        .await
    {
        Ok(uploaded) => uploaded,
        Err(error) => {
            return Err(abort_failed_replication_probe_multipart(
                target_client,
                target_bucket,
                probe_key,
                &upload_id,
                S3ClientError::from(error),
            )
            .await);
        }
    };

    let completed_part = CompletedPart::builder()
        .part_number(1)
        .set_e_tag(uploaded.e_tag().map(ToOwned::to_owned))
        .build();
    let complete_headers = headers.clone();
    let completed = match target_client
        .client
        .complete_multipart_upload()
        .bucket(target_bucket)
        .key(probe_key)
        .upload_id(&upload_id)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .set_parts(Some(vec![completed_part]))
                .build(),
        )
        .customize()
        .map_request(move |mut req| {
            for (key, value) in complete_headers.clone() {
                req.headers_mut().insert(key.expect("operation should succeed"), value);
            }
            Result::<_, std::io::Error>::Ok(req)
        })
        .send()
        .await
    {
        Ok(completed) => completed,
        Err(error) => {
            return Err(abort_failed_replication_probe_multipart(
                target_client,
                target_bucket,
                probe_key,
                &upload_id,
                S3ClientError::from(error),
            )
            .await);
        }
    };

    Ok(ReplicationProbePutOutcome {
        sent_version_id,
        response_version_id: completed.version_id().map(ToOwned::to_owned),
    })
}

async fn abort_failed_replication_probe_multipart(
    target_client: &TargetClient,
    target_bucket: &str,
    probe_key: &str,
    upload_id: &str,
    primary_error: S3ClientError,
) -> ReplicationProbeMultipartError {
    match target_client
        .client
        .abort_multipart_upload()
        .bucket(target_bucket)
        .key(probe_key)
        .upload_id(upload_id)
        .send()
        .await
    {
        Ok(_) => ReplicationProbeMultipartError::from(primary_error),
        Err(error) => {
            let abort_error = S3ClientError::from(error);
            if abort_error.code.as_deref() == Some("NoSuchUpload") {
                ReplicationProbeMultipartError::from(primary_error)
            } else {
                ReplicationProbeMultipartError {
                    primary: primary_error,
                    cleanup_error: Some("failed to abort multipart replication probe".to_string()),
                }
            }
        }
    }
}

async fn put_replication_probe_object(
    target_client: &TargetClient,
    target_bucket: &str,
    probe_key: &str,
    now: OffsetDateTime,
) -> Result<ReplicationProbePutOutcome, S3ClientError> {
    let options = build_replication_probe_put_options(now);
    let sent_version_id = options.internal.source_version_id.clone();
    let headers = build_replication_probe_headers(&options);

    // Carry the source version as `?versionId=` exactly like a live
    // replication PUT (P0-5 shape): the probe must exercise the query the
    // real data path relies on, and the response tells us whether the target
    // adopts the id. The probe id is always a fresh non-nil UUID, so the
    // null-version mapping in the live path does not apply here.
    let query_version_id = sent_version_id.clone();
    let response = target_client
        .client
        .put_object()
        .bucket(target_bucket)
        .key(probe_key)
        .if_none_match("*")
        .content_length(8)
        .body(AwsByteStream::from_static(b"aaaaaaaa"))
        .customize()
        .map_request(move |mut req| {
            for (key, value) in headers.clone() {
                req.headers_mut().insert(key.expect("operation should succeed"), value);
            }
            let uri = append_version_id_query(req.uri(), &query_version_id);
            req.set_uri(uri).map_err(std::io::Error::other)?;
            Result::<_, std::io::Error>::Ok(req)
        })
        .send()
        .await
        .map_err(S3ClientError::from)?;

    Ok(ReplicationProbePutOutcome {
        sent_version_id,
        response_version_id: response.version_id().map(ToOwned::to_owned),
    })
}

/// PUT a fresh probe version carrying the SSE-C passthrough transport headers
/// (the wire shape live SSE-C replication uses), then HEAD it back through the
/// worker channel (replication-check exemption + proxy suppression). A RustFS
/// target restores the transport headers into stored SSE-C metadata and its
/// HEAD echoes `x-amz-server-side-encryption-customer-algorithm`; a target
/// that dropped the headers echoes nothing. The probe body is never SSE-C
/// encrypted — only the metadata round-trip matters — and the version is
/// deleted by the shared probe cleanup.
async fn ssec_passthrough_probe_object(
    target_client: &TargetClient,
    target_bucket: &str,
    probe_key: &str,
    now: OffsetDateTime,
) -> Result<ReplicationSsecProbeOutcome, S3ClientError> {
    let options = build_replication_probe_put_options(now);
    let sent_version_id = options.internal.source_version_id.clone();
    let mut headers = build_replication_probe_headers(&options);
    // These are full wire names (not x-rustfs/x-minio suffixes), so they must
    // be inserted verbatim — `insert_header` would mangle them.
    for (name, value) in [
        (REPLICATION_SSEC_ALGORITHM_HEADER, "AES256"),
        (REPLICATION_SSEC_KEY_MD5_HEADER, REPLICATION_CHECK_SSEC_PROBE_KEY_MD5),
        (REPLICATION_SSEC_ORIGINAL_SIZE_HEADER, "8"),
    ] {
        let name = name
            .parse::<HeaderName>()
            .map_err(|err| S3ClientError::new(format!("invalid ssec probe header name: {err}")))?;
        let value =
            HeaderValue::from_str(value).map_err(|err| S3ClientError::new(format!("invalid ssec probe header value: {err}")))?;
        headers.insert(name, value);
    }

    let query_version_id = sent_version_id.clone();
    let response = target_client
        .client
        .put_object()
        .bucket(target_bucket)
        .key(probe_key)
        .content_length(8)
        .body(AwsByteStream::from_static(b"aaaaaaaa"))
        .customize()
        .map_request(move |mut req| {
            for (key, value) in headers.clone() {
                req.headers_mut().insert(key.expect("operation should succeed"), value);
            }
            let uri = append_version_id_query(req.uri(), &query_version_id);
            req.set_uri(uri).map_err(std::io::Error::other)?;
            Result::<_, std::io::Error>::Ok(req)
        })
        .send()
        .await
        .map_err(S3ClientError::from)?;
    let version_id = response.version_id().map(ToOwned::to_owned);

    let head_version = version_id.clone().or_else(|| Some(sent_version_id.clone()));
    let head = target_client
        .head_object(target_bucket, probe_key, head_version)
        .await
        .map_err(|err| S3ClientError::from(*err))?;

    Ok(ReplicationSsecProbeOutcome {
        evidence_present: head.sse_customer_algorithm().is_some_and(|algorithm| !algorithm.is_empty()),
        version_id,
    })
}

async fn delete_replication_probe_object(
    target_client: &TargetClient,
    target_bucket: &str,
    probe_key: &str,
    version_id: Option<&str>,
    options: RemoveObjectOptions,
) -> Result<Option<String>, S3ClientError> {
    let mut headers = HeaderMap::new();
    if options.replication_delete_marker {
        insert_header(&mut headers, SUFFIX_SOURCE_DELETEMARKER, "true");
    }
    if let Some(replication_mtime) = options.replication_mtime {
        insert_header(&mut headers, SUFFIX_SOURCE_MTIME, replication_mtime.format(&Rfc3339).unwrap_or_default());
    }
    headers.insert(
        HeaderName::from_static("x-amz-replication-status"),
        HeaderValue::from_static(options.replication_status.as_str()),
    );
    if options.replication_request {
        insert_header(&mut headers, SUFFIX_SOURCE_REPLICATION_REQUEST, "true");
    }
    if options.replication_validity_check {
        insert_header(&mut headers, SUFFIX_SOURCE_REPLICATION_CHECK, "true");
    }

    if let Some(version_id) = version_id {
        insert_header(&mut headers, SUFFIX_SOURCE_VERSION_ID, version_id);
    }

    // Same wire shape as live delete replication: a marker creation carries
    // no `versionId` (the target mints the marker), a version delete does. A
    // generic S3 target handed the version id on the marker step would
    // permanently delete the probe version instead, and the VersionDelete
    // phase would then find nothing (seen on Wasabi).
    let api_version_id = resolve_delete_api_version_id(version_id.map(ToOwned::to_owned), &options);
    target_client
        .client
        .delete_object()
        .bucket(target_bucket)
        .key(probe_key)
        .set_version_id(api_version_id)
        .customize()
        .map_request(move |mut req| {
            for (key, value) in headers.clone() {
                req.headers_mut().insert(key.expect("operation should succeed"), value);
            }
            Result::<_, std::io::Error>::Ok(req)
        })
        .send()
        .await
        .map(|output| output.version_id().map(ToOwned::to_owned))
        .map_err(S3ClientError::from)
}

async fn delete_replication_probe_version(
    target_client: &TargetClient,
    target_bucket: &str,
    probe_key: &str,
    version_id: &str,
) -> Result<(), S3ClientError> {
    target_client
        .client
        .delete_object()
        .bucket(target_bucket)
        .key(probe_key)
        .version_id(version_id)
        .send()
        .await
        .map(|_| ())
        .map_err(S3ClientError::from)
}

/// The VersionDelete phase already removed the probe version the cleanup is
/// handed, and a strict S3 target (Wasabi) answers a second DELETE of that
/// id with `NoSuchVersion` where RustFS/MinIO answer 204: the goal is met
/// either way.
fn probe_version_already_gone(err: &S3ClientError) -> bool {
    matches!(err.code.as_deref(), Some("NoSuchKey" | "NoSuchVersion"))
}

async fn cleanup_replication_probe<'a>(
    target_client: &TargetClient,
    target_bucket: &str,
    probe_key: &str,
    known_version_ids: impl IntoIterator<Item = Option<&'a str>>,
) -> Result<(), String> {
    let mut errors = Vec::new();
    let mut deleted_ids = HashSet::new();
    for version_id in known_version_ids.into_iter().flatten() {
        if deleted_ids.insert(version_id.to_string())
            && let Err(err) = delete_replication_probe_version(target_client, target_bucket, probe_key, version_id).await
            && !probe_version_already_gone(&err)
        {
            errors.push(format_replication_check_client_error(
                &err,
                ReplicationCheckFailureContext::DeleteObjectVersion,
            ));
        }
    }

    let mut key_marker = None;
    let mut version_id_marker = None;
    loop {
        let output = target_client
            .client
            .list_object_versions()
            .bucket(target_bucket)
            .prefix(probe_key)
            .set_key_marker(key_marker.clone())
            .set_version_id_marker(version_id_marker.clone())
            .send()
            .await
            .map_err(|err| {
                format_replication_check_client_error(
                    &S3ClientError::from(err),
                    ReplicationCheckFailureContext::DeleteObjectVersion,
                )
            })?;

        let mut discovered_ids = Vec::new();
        discovered_ids.extend(
            output
                .versions()
                .iter()
                .filter(|version| version.key() == Some(probe_key))
                .filter_map(|version| version.version_id().map(ToOwned::to_owned)),
        );
        discovered_ids.extend(
            output
                .delete_markers()
                .iter()
                .filter(|marker| marker.key() == Some(probe_key))
                .filter_map(|marker| marker.version_id().map(ToOwned::to_owned)),
        );
        for version_id in discovered_ids {
            if deleted_ids.insert(version_id.clone())
                && let Err(err) = delete_replication_probe_version(target_client, target_bucket, probe_key, &version_id).await
                && !probe_version_already_gone(&err)
            {
                errors.push(format_replication_check_client_error(
                    &err,
                    ReplicationCheckFailureContext::DeleteObjectVersion,
                ));
            }
        }

        if output.is_truncated() != Some(true) {
            break;
        }
        let next_key_marker = output.next_key_marker().map(ToOwned::to_owned);
        let next_version_id_marker = output.next_version_id_marker().map(ToOwned::to_owned);
        if next_key_marker.is_none() || next_key_marker == key_marker {
            errors.push("target returned an invalid cleanup pagination marker".to_string());
            break;
        }
        key_marker = next_key_marker;
        version_id_marker = next_version_id_marker;
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(bound_replication_check_error(errors.join("; ")))
    }
}
